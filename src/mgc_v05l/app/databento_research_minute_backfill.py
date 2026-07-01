"""Research-only Databento minute-bar backfill for long-lookback replay.

This module writes offline research artifacts only. It must not be used as
runtime, preflight, dashboard, or broker truth.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence

from mgc_v05l.execution_core.track_b_runtime_candle_capture_cli import _load_databento_api_key
from mgc_v05l.market_data.databento_provider import DatabentoHistoricalHttpClient, UrllibDatabentoTransport
from mgc_v05l.research.trend_participation.storage import build_layout, write_storage_manifest

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RESEARCH_UNIVERSE_PATH = Path("config") / "research_data_universe.json"
FALLBACK_RESEARCH_SYMBOLS = ("MGC", "MNQ", "MES")
DEFAULT_START_DATE = date(2010, 6, 6)
DEFAULT_LOADED_HISTORY_START_DATE = date(2020, 1, 1)
DEFAULT_OUTPUT_ROOT = Path("outputs") / "reports" / "trend_participation_engine"
DEFAULT_DATASET = "GLBX.MDP3"
DEFAULT_SCHEMA = "ohlcv-1m"
DEFAULT_BASE_URL = "https://hist.databento.com/v0"
SOURCE = "DATABENTO_HISTORICAL_RESEARCH_BACKFILL"
TIMEFRAME = "1m"
APPROVED_CHUNKS = {"monthly": 1, "quarterly": 3}
DEFAULT_ROUTINE_MAX_CALENDAR_DAYS = 10
DEFAULT_ESTIMATED_1M_BARS_PER_CALENDAR_DAY = 1380
DEFAULT_ESTIMATED_PARQUET_BYTES_PER_1M_BAR = 220


class ResearchBackfillClient(Protocol):
    def get_range_json_lines(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
        stype_out: str,
        encoding: str,
        compression: str,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch historical Databento records as JSON dictionaries."""


@dataclass(frozen=True)
class BackfillChunk:
    symbol: str
    request_symbol: str
    start: datetime
    end: datetime
    partition_path: Path
    metadata_path: Path
    status: str = "PENDING"


@dataclass(frozen=True)
class ResearchMinuteBackfillConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = DEFAULT_OUTPUT_ROOT
    symbols: tuple[str, ...] = FALLBACK_RESEARCH_SYMBOLS
    start_date: date = DEFAULT_START_DATE
    end_date: date | None = None
    loaded_history_start_date: date | None = DEFAULT_LOADED_HISTORY_START_DATE
    chunk: str = "monthly"
    dry_run: bool = False
    max_chunks: int | None = None
    force: bool = False
    env_file: Path | None = None
    json_output: Path | None = None
    dataset: str = DEFAULT_DATASET
    schema_name: str = DEFAULT_SCHEMA
    base_url: str = DEFAULT_BASE_URL
    stype_in: str = "continuous"
    stype_out: str = "instrument_id"
    limit: int | None = None
    now: datetime | None = None
    research_universe_path: Path | None = DEFAULT_RESEARCH_UNIVERSE_PATH
    research_universe_symbols: tuple[str, ...] | None = None
    operator_approved_large_refresh: bool = False
    end_boundary_mode: str = "closed-day"
    provider_available_end: datetime | None = None


@dataclass(frozen=True)
class ResearchMinuteBackfillResult:
    report: dict[str, Any]
    report_paths: tuple[Path, ...]


def run_research_minute_backfill(
    *,
    config: ResearchMinuteBackfillConfig,
    client: ResearchBackfillClient | None = None,
) -> ResearchMinuteBackfillResult:
    now = _coerce_now(config.now)
    universe = _load_research_universe(config)
    symbols = _normalize_symbols(config.symbols, universe=universe)
    _require_explicit_date_range(config)
    chunk_months = _chunk_months(config.chunk)
    requested_end_date = config.end_date or now.date()
    end_resolution = _resolve_end_boundary(config=config, requested_end_date=requested_end_date, now=now)
    backfill_end = _backfill_end_datetime(config=config, effective_requested_end=end_resolution["effective_requested_end"])
    chunks = _build_chunks(config=config, symbols=symbols, final_end=backfill_end, chunk_months=chunk_months)
    if config.max_chunks is not None:
        chunks = chunks[: max(int(config.max_chunks), 0)]
    scope_estimate = _scope_estimate(
        config=config,
        symbols=symbols,
        chunks=chunks,
        requested_end_date=requested_end_date,
        end_resolution=end_resolution,
        planned_end=backfill_end,
    )
    approval_required = bool(scope_estimate["approval_required"])

    api_key, credential_status, credential_source = _load_databento_api_key(config.env_file)
    rows: list[dict[str, Any]] = []
    partition_reports: list[dict[str, Any]] = []
    provider_error_count = 0
    written_count = 0
    skipped_count = 0
    empty_count = 0
    would_fetch_count = 0

    seed_client: ResearchBackfillClient | None = client
    if seed_client is None and api_key and not config.dry_run:
        seed_client = DatabentoHistoricalHttpClient(
            api_key=api_key,
            base_url=config.base_url,
            transport=UrllibDatabentoTransport(timeout_seconds=120),
        )

    if not config.dry_run and approval_required and not config.operator_approved_large_refresh:
        rows = [
            _symbol_row(
                symbol=symbol,
                chunks=[chunk for chunk in chunks if chunk.symbol == symbol],
                status="BLOCKED",
                block_reason="OPERATOR_APPROVAL_REQUIRED_FOR_LARGE_RESEARCH_REFRESH",
            )
            for symbol in symbols
        ]
        report = _build_report(
            config=config,
            now=now,
            symbols=symbols,
            chunks=chunks,
            rows=rows,
            partition_reports=[],
            credential_status=credential_status,
            credential_source=credential_source,
            provider_error_count=0,
            written_count=0,
            skipped_count=0,
            empty_count=0,
            would_fetch_count=0,
            final_verdict="RESEARCH_MINUTE_BACKFILL_APPROVAL_REQUIRED",
            primary_blocker="OPERATOR_APPROVAL_REQUIRED_FOR_LARGE_RESEARCH_REFRESH",
            universe=universe,
            scope_estimate=scope_estimate,
        )
        return _write_reports(config=config, report=report)

    if not config.dry_run and seed_client is None:
        rows = [_symbol_row(symbol=symbol, chunks=[], status="BLOCKED", block_reason="DATABENTO_API_KEY_MISSING") for symbol in symbols]
        report = _build_report(
            config=config,
            now=now,
            symbols=symbols,
            chunks=chunks,
            rows=rows,
            partition_reports=[],
            credential_status=credential_status,
            credential_source=credential_source,
            provider_error_count=0,
            written_count=0,
            skipped_count=0,
            empty_count=0,
            would_fetch_count=0,
            final_verdict="RESEARCH_MINUTE_BACKFILL_BLOCKED",
            primary_blocker="DATABENTO_API_KEY_MISSING",
            universe=universe,
            scope_estimate=scope_estimate,
        )
        return _write_reports(config=config, report=report)

    chunks_by_symbol: dict[str, list[BackfillChunk]] = {symbol: [] for symbol in symbols}
    for chunk in chunks:
        chunks_by_symbol.setdefault(chunk.symbol, []).append(chunk)

    for symbol in symbols:
        symbol_chunks = chunks_by_symbol.get(symbol, [])
        symbol_written = 0
        symbol_skipped = 0
        symbol_failed = 0
        row_count = 0
        actual_start: str | None = None
        actual_end: str | None = None
        for chunk in symbol_chunks:
            existing = _complete_partition_metadata(chunk)
            if existing and not config.force:
                skipped_count += 1
                symbol_skipped += 1
                partition_reports.append(_partition_report(chunk=chunk, status="SKIPPED_EXISTING_COMPLETE", row_count=int(existing.get("row_count") or 0)))
                continue
            if config.dry_run:
                would_fetch_count += 1
                partition_reports.append(_partition_report(chunk=chunk, status="DRY_RUN_WOULD_FETCH", row_count=0))
                continue
            try:
                records = _fetch_chunk(client=seed_client, config=config, chunk=chunk)
                bars = _normalize_records(records=records, symbol=chunk.symbol, generated_at=now)
                gap_summary = _gap_summary(bars)
                if bars:
                    partition_status = "COMPLETE"
                    _write_partition(chunk=chunk, bars=bars)
                    written_count += 1
                    symbol_written += 1
                    row_count += len(bars)
                    actual_start = actual_start or str(bars[0]["bar_end"])
                    actual_end = str(bars[-1]["bar_end"])
                else:
                    partition_status = "EMPTY_NO_ROWS"
                    chunk.partition_path.unlink(missing_ok=True)
                    empty_count += 1
                    symbol_skipped += 1
                metadata = _partition_metadata(
                    config=config,
                    chunk=chunk,
                    bars=bars,
                    generated_at=now,
                    gap_summary=gap_summary,
                    status=partition_status,
                )
                _write_json_atomic(chunk.metadata_path, metadata)
                partition_reports.append(_partition_report(chunk=chunk, status=partition_status, row_count=len(bars), gaps=gap_summary))
            except Exception as exc:  # noqa: BLE001 - research fetch failures are reported and fail closed.
                provider_error_count += 1
                symbol_failed += 1
                partition_reports.append(_partition_report(chunk=chunk, status="FAILED", row_count=0, error=str(exc)))
        rows.append(
            {
                "symbol": symbol,
                "dataset": config.dataset,
                "vendor": "Databento",
                "timeframe": TIMEFRAME,
                "requested_start": _start_datetime(config.start_date).isoformat(),
                "requested_end": _end_datetime(requested_end_date).isoformat(),
                "planned_fetch_end": backfill_end.isoformat(),
                "effective_requested_end": end_resolution["effective_requested_end"].isoformat(),
                "end_boundary_mode": end_resolution["mode"],
                "end_boundary_capped": end_resolution["capped"],
                "end_boundary_cap_reason": end_resolution["cap_reason"],
                "actual_start": actual_start,
                "actual_end": actual_end,
                "row_count": row_count,
                "partition_count": len(symbol_chunks),
                "partitions_written": symbol_written,
                "partitions_skipped": symbol_skipped,
                "partitions_failed": symbol_failed,
                "status": _symbol_status(
                    dry_run=config.dry_run,
                    written=symbol_written,
                    skipped=symbol_skipped,
                    failed=symbol_failed,
                    total=len(symbol_chunks),
                ),
                "research_artifact": True,
                "runtime_artifact": False,
                "archive_artifact": False,
                "can_submit": False,
                "live_money_eligible": False,
            }
        )

    final_verdict = _final_verdict(
        dry_run=config.dry_run,
        approval_required=approval_required,
        provider_error_count=provider_error_count,
        written_count=written_count,
        skipped_count=skipped_count,
        empty_count=empty_count,
        would_fetch_count=would_fetch_count,
    )
    report = _build_report(
        config=config,
        now=now,
        symbols=symbols,
        chunks=chunks,
        rows=rows,
        partition_reports=partition_reports,
        credential_status=credential_status,
        credential_source=credential_source,
        provider_error_count=provider_error_count,
        written_count=written_count,
        skipped_count=skipped_count,
        empty_count=empty_count,
        would_fetch_count=would_fetch_count,
        final_verdict=final_verdict,
        primary_blocker="PROVIDER_ERRORS" if provider_error_count else None,
        universe=universe,
        scope_estimate=scope_estimate,
    )
    return _write_reports(config=config, report=report)


def _build_chunks(
    *,
    config: ResearchMinuteBackfillConfig,
    symbols: tuple[str, ...],
    final_end: datetime,
    chunk_months: int,
) -> list[BackfillChunk]:
    chunks: list[BackfillChunk] = []
    start = _start_datetime(config.start_date)
    for symbol in symbols:
        cursor = start
        while cursor < final_end:
            chunk_end = min(_add_months(cursor, chunk_months), final_end)
            chunks.append(
                BackfillChunk(
                    symbol=symbol,
                    request_symbol=_continuous_symbol(symbol),
                    start=cursor,
                    end=chunk_end,
                    partition_path=_partition_path(config=config, symbol=symbol, start=cursor),
                    metadata_path=_partition_metadata_path(config=config, symbol=symbol, start=cursor),
                )
            )
            cursor = chunk_end
    return chunks


def _fetch_chunk(
    *,
    client: ResearchBackfillClient | None,
    config: ResearchMinuteBackfillConfig,
    chunk: BackfillChunk,
) -> list[dict[str, Any]]:
    if client is None:
        raise RuntimeError("DATABENTO_CLIENT_NOT_AVAILABLE")
    return client.get_range_json_lines(
        dataset=config.dataset,
        request_symbol=chunk.request_symbol,
        schema_name=config.schema_name,
        start=chunk.start,
        end=chunk.end,
        stype_in=config.stype_in,
        stype_out=config.stype_out,
        encoding="json",
        compression="none",
        pretty_px=True,
        pretty_ts=True,
        map_symbols=True,
        limit=config.limit,
    )


def _normalize_records(*, records: Sequence[dict[str, Any]], symbol: str, generated_at: datetime) -> list[dict[str, Any]]:
    bars: dict[str, dict[str, Any]] = {}
    for record in records:
        ts = _record_timestamp(record)
        if ts is None:
            continue
        try:
            open_px = float(record["open"])
            high_px = float(record["high"])
            low_px = float(record["low"])
            close_px = float(record["close"])
        except (KeyError, TypeError, ValueError):
            continue
        volume = _coerce_volume(record.get("volume"))
        bar_end = ts.astimezone(timezone.utc)
        bar_start = datetime.fromtimestamp(bar_end.timestamp() - 60, tz=timezone.utc)
        row = {
            "symbol": symbol,
            "timeframe": TIMEFRAME,
            "bar_start": bar_start,
            "bar_end": bar_end,
            "bar_ts": bar_end,
            "open": open_px,
            "high": high_px,
            "low": low_px,
            "close": close_px,
            "volume": volume,
            "provider": "databento",
            "dataset": DEFAULT_DATASET,
            "schema": DEFAULT_SCHEMA,
            "request_symbol": _continuous_symbol(symbol),
            "raw_symbol": str(record.get("symbol") or record.get("raw_symbol") or ""),
            "instrument_id": str(record.get("instrument_id") or ""),
            "instrument_identity": str(record.get("symbol") or record.get("raw_symbol") or record.get("instrument_id") or ""),
            "data_source": "historical_1m_research_backfill",
            "ingest_ts": generated_at,
            "coverage_window_start": None,
            "coverage_window_end": None,
            "provenance_tag": SOURCE,
            "source": SOURCE,
            "generated_at": generated_at,
            "research_artifact": True,
            "runtime_artifact": False,
            "archive_artifact": False,
        }
        bars[bar_end.isoformat()] = row
    return [bars[key] for key in sorted(bars)]


def _write_partition(*, chunk: BackfillChunk, bars: list[dict[str, Any]]) -> None:
    pyarrow = _require_pyarrow()
    chunk.partition_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = chunk.partition_path.with_name(f".{chunk.partition_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        table = pyarrow.Table.from_pylist(bars, schema=_bar_schema(pyarrow))
        pyarrow.parquet.write_table(table, tmp_path)
        os.replace(tmp_path, chunk.partition_path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _bar_schema(pyarrow: Any) -> Any:
    return pyarrow.schema(
        [
            ("symbol", pyarrow.string()),
            ("timeframe", pyarrow.string()),
            ("bar_start", pyarrow.timestamp("us", tz="UTC")),
            ("bar_end", pyarrow.timestamp("us", tz="UTC")),
            ("bar_ts", pyarrow.timestamp("us", tz="UTC")),
            ("open", pyarrow.float64()),
            ("high", pyarrow.float64()),
            ("low", pyarrow.float64()),
            ("close", pyarrow.float64()),
            ("volume", pyarrow.float64()),
            ("provider", pyarrow.string()),
            ("dataset", pyarrow.string()),
            ("schema", pyarrow.string()),
            ("request_symbol", pyarrow.string()),
            ("raw_symbol", pyarrow.string()),
            ("instrument_id", pyarrow.string()),
            ("instrument_identity", pyarrow.string()),
            ("data_source", pyarrow.string()),
            ("ingest_ts", pyarrow.timestamp("us", tz="UTC")),
            ("coverage_window_start", pyarrow.timestamp("us", tz="UTC")),
            ("coverage_window_end", pyarrow.timestamp("us", tz="UTC")),
            ("provenance_tag", pyarrow.string()),
            ("source", pyarrow.string()),
            ("generated_at", pyarrow.timestamp("us", tz="UTC")),
            ("research_artifact", pyarrow.bool_()),
            ("runtime_artifact", pyarrow.bool_()),
            ("archive_artifact", pyarrow.bool_()),
        ]
    )


def _partition_metadata(
    *,
    config: ResearchMinuteBackfillConfig,
    chunk: BackfillChunk,
    bars: list[dict[str, Any]],
    generated_at: datetime,
    gap_summary: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    return {
        "schema_version": "databento_research_minute_backfill_partition_v1",
        "generated_at": generated_at.isoformat(),
        "source": SOURCE,
        "vendor": "Databento",
        "dataset": config.dataset,
        "schema": config.schema_name,
        "symbol": chunk.symbol,
        "request_symbol": chunk.request_symbol,
        "timeframe": TIMEFRAME,
        "requested_start": chunk.start.isoformat(),
        "requested_end": chunk.end.isoformat(),
        "actual_start": None if not bars else bars[0]["bar_end"].isoformat(),
        "actual_end": None if not bars else bars[-1]["bar_end"].isoformat(),
        "row_count": len(bars),
        "partition_path": str(chunk.partition_path),
        "missing_sessions_or_gaps": gap_summary,
        "status": status,
        "research_artifact": True,
        "runtime_artifact": False,
        "archive_artifact": False,
        "realtime_feed_confirmed": False,
        "can_submit": False,
        "live_money_eligible": False,
    }


def _build_report(
    *,
    config: ResearchMinuteBackfillConfig,
    now: datetime,
    symbols: tuple[str, ...],
    chunks: list[BackfillChunk],
    rows: list[dict[str, Any]],
    partition_reports: list[dict[str, Any]],
    credential_status: str,
    credential_source: str | None,
    provider_error_count: int,
    written_count: int,
    skipped_count: int,
    empty_count: int,
    would_fetch_count: int,
    final_verdict: str,
    primary_blocker: str | None,
    universe: dict[str, Any],
    scope_estimate: dict[str, Any],
) -> dict[str, Any]:
    end_resolution = scope_estimate.get("end_resolution") or {}
    return {
        "schema_version": "databento_research_minute_backfill_report_v1",
        "generated_at": now.isoformat(),
        "repo_root": str(Path(config.repo_root)),
        "output_root": str(_output_root(config)),
        "storage_architecture": "ATP_RESEARCH_PARQUET_DUCKDB_JSON_MANIFEST",
        "raw_bars_root": str(_layout(config)["raw"] / "databento_minute_backfill"),
        "duckdb_path": str(_layout(config)["duckdb"]),
        "storage_manifest": str(_manifest_path(config)),
        "source": SOURCE,
        "vendor": "Databento",
        "dataset": config.dataset,
        "schema": config.schema_name,
        "timeframe": TIMEFRAME,
        "symbols": list(symbols),
        "symbol_count": len(symbols),
        "research_universe": {
            "source": universe["source"],
            "schema_version": universe.get("schema_version"),
            "allowed_symbols": list(universe["allowed_symbols"]),
            "requested_symbols": list(symbols),
            "unknown_symbols_rejected": True,
        },
        "requested_start": _start_datetime(config.start_date).isoformat(),
        "requested_end": _end_datetime(config.end_date or now.date()).isoformat(),
        "effective_requested_end": end_resolution.get("effective_requested_end"),
        "planned_fetch_end": scope_estimate.get("planned_fetch_end"),
        "end_boundary_mode": end_resolution.get("mode") or config.end_boundary_mode,
        "end_boundary_capped": bool(end_resolution.get("capped")),
        "end_boundary_cap_reason": end_resolution.get("cap_reason"),
        "provider_available_end": end_resolution.get("provider_available_end"),
        "loaded_history_start_date": None if config.loaded_history_start_date is None else config.loaded_history_start_date.isoformat(),
        "loaded_history_overlap_policy": "SKIP_EXISTING_LOADED_HISTORY",
        "loaded_history_overlap_skipped": _loaded_history_overlap_skipped(config=config, requested_end_date=config.end_date or now.date()),
        "loaded_history_skipped_start": None if config.loaded_history_start_date is None else _start_datetime(config.loaded_history_start_date).isoformat(),
        "loaded_history_skipped_end": None
        if config.loaded_history_start_date is None or (config.end_date or now.date()) < config.loaded_history_start_date
        else _end_datetime(config.end_date or now.date()).isoformat(),
        "chunk": config.chunk,
        "chunk_count": len(chunks),
        "scope_estimate": scope_estimate,
        "approval_required": bool(scope_estimate.get("approval_required")),
        "approval_reason": scope_estimate.get("approval_reason"),
        "operator_approved_large_refresh": bool(config.operator_approved_large_refresh),
        "dry_run": bool(config.dry_run),
        "force": bool(config.force),
        "max_chunks": config.max_chunks,
        "credential_status": credential_status,
        "credential_source": credential_source,
        "partitions_written": written_count,
        "partitions_skipped": skipped_count,
        "partitions_empty": empty_count,
        "partitions_would_fetch": would_fetch_count,
        "provider_error_count": provider_error_count,
        "row_count": sum(int(row.get("row_count") or 0) for row in rows),
        "partition_count": sum(int(row.get("partition_count") or 0) for row in rows),
        "rows": rows,
        "partition_reports": partition_reports,
        "research_artifact": True,
        "runtime_artifact": False,
        "archive_artifact": False,
        "realtime_feed_confirmed": False,
        "can_submit": False,
        "live_money_eligible": False,
        "paper_trade_allowed": False,
        "runtime_preflight_dashboard_truth": False,
        "final_verdict": final_verdict,
        "primary_blocker": primary_blocker,
    }


def _write_reports(*, config: ResearchMinuteBackfillConfig, report: dict[str, Any]) -> ResearchMinuteBackfillResult:
    layout = _layout(config)
    report_dir = layout["reports"]
    report_dir.mkdir(parents=True, exist_ok=True)
    latest_json = config.json_output or report_dir / "latest_databento_research_minute_backfill_report.json"
    if not latest_json.is_absolute():
        latest_json = Path(config.repo_root) / latest_json
    latest_json.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(latest_json, report)
    week_label = str(report["requested_end"])[:10]
    markdown = report_dir / f"databento_research_minute_backfill_{week_label}.md"
    markdown.write_text(_render_markdown(report), encoding="utf-8")
    manifest = _write_storage_manifest(config=config, report=report)
    duckdb_status = _refresh_duckdb_view(config=config)
    report["duckdb_registered"] = bool(duckdb_status.get("registered"))
    report["duckdb_registration_status"] = duckdb_status.get("status")
    _write_json_atomic(latest_json, report)
    return ResearchMinuteBackfillResult(report=report, report_paths=(latest_json, markdown, manifest))


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Databento Research Minute Backfill",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- final_verdict: `{report.get('final_verdict')}`",
        f"- source: `{report.get('source')}`",
        f"- requested_start: `{report.get('requested_start')}`",
        f"- requested_end: `{report.get('requested_end')}`",
        f"- chunk_count: `{report.get('chunk_count')}`",
        f"- partitions_written: `{report.get('partitions_written')}`",
        f"- provider_error_count: `{report.get('provider_error_count')}`",
        f"- runtime_artifact: `{report.get('runtime_artifact')}`",
        f"- can_submit: `{report.get('can_submit')}`",
        "",
        "| symbol | status | partitions | written | skipped | failed | rows | actual start | actual end |",
        "|---|---|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in list(report.get("rows") or []):
        lines.append(
            "| {symbol} | {status} | {partition_count} | {partitions_written} | {partitions_skipped} | {partitions_failed} | {row_count} | {actual_start} | {actual_end} |".format(
                **{key: row.get(key, "") for key in ("symbol", "status", "partition_count", "partitions_written", "partitions_skipped", "partitions_failed", "row_count", "actual_start", "actual_end")}
            )
        )
    return "\n".join(lines) + "\n"


def _complete_partition_metadata(chunk: BackfillChunk) -> dict[str, Any] | None:
    if not chunk.metadata_path.exists():
        return None
    try:
        payload = json.loads(chunk.metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if payload.get("status") not in {"COMPLETE", "EMPTY_NO_ROWS"}:
        return None
    if payload.get("status") == "COMPLETE" and not chunk.partition_path.exists():
        return None
    if payload.get("source") != SOURCE or payload.get("runtime_artifact") is not False:
        return None
    if payload.get("requested_start") != chunk.start.isoformat() or payload.get("requested_end") != chunk.end.isoformat():
        return None
    return payload


def _symbol_row(*, symbol: str, chunks: list[BackfillChunk], status: str, block_reason: str | None) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "dataset": DEFAULT_DATASET,
        "vendor": "Databento",
        "timeframe": TIMEFRAME,
        "requested_start": DEFAULT_START_DATE.isoformat(),
        "requested_end": None,
        "actual_start": None,
        "actual_end": None,
        "row_count": 0,
        "partition_count": len(chunks),
        "partitions_written": 0,
        "partitions_skipped": 0,
        "partitions_failed": 0,
        "status": status,
        "block_reason": block_reason,
        "research_artifact": True,
        "runtime_artifact": False,
        "archive_artifact": False,
        "can_submit": False,
        "live_money_eligible": False,
    }


def _partition_report(
    *,
    chunk: BackfillChunk,
    status: str,
    row_count: int,
    gaps: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    return {
        "symbol": chunk.symbol,
        "request_symbol": chunk.request_symbol,
        "requested_start": chunk.start.isoformat(),
        "requested_end": chunk.end.isoformat(),
        "partition_path": str(chunk.partition_path),
        "metadata_path": str(chunk.metadata_path),
        "status": status,
        "row_count": row_count,
        "missing_sessions_or_gaps": gaps or {},
        "error": error,
    }


def _gap_summary(bars: list[dict[str, Any]]) -> dict[str, Any]:
    gaps: list[dict[str, str]] = []
    previous: datetime | None = None
    for row in bars:
        current = row["bar_end"]
        if previous is not None and (current - previous).total_seconds() > 60:
            gaps.append({"from": previous.isoformat(), "to": current.isoformat()})
        previous = current
    return {"gap_count": len(gaps), "sample_gaps": gaps[:20]}


def _partition_path(*, config: ResearchMinuteBackfillConfig, symbol: str, start: datetime) -> Path:
    return _layout(config)["raw"] / "databento_minute_backfill" / f"symbol={symbol}" / f"year={start.year:04d}" / f"month={start.month:02d}" / "bars.parquet"


def _partition_metadata_path(*, config: ResearchMinuteBackfillConfig, symbol: str, start: datetime) -> Path:
    return _partition_path(config=config, symbol=symbol, start=start).with_name("partition_metadata.json")


def _output_root(config: ResearchMinuteBackfillConfig) -> Path:
    root = Path(config.output_root)
    return root if root.is_absolute() else Path(config.repo_root) / root


def _layout(config: ResearchMinuteBackfillConfig) -> dict[str, Path]:
    return build_layout(_output_root(config))


def _manifest_path(config: ResearchMinuteBackfillConfig) -> Path:
    return _layout(config)["manifests"] / "databento_research_minute_backfill_manifest.json"


def _write_storage_manifest(*, config: ResearchMinuteBackfillConfig, report: dict[str, Any]) -> Path:
    partition_rows = [
        {
            "symbol": row.get("symbol"),
            "requested_start": row.get("requested_start"),
            "requested_end": row.get("requested_end"),
            "actual_start": row.get("actual_start"),
            "actual_end": row.get("actual_end"),
            "row_count": row.get("row_count"),
            "partition_count": row.get("partition_count"),
            "status": row.get("status"),
        }
        for row in list(report.get("rows") or [])
    ]
    return write_storage_manifest(
        _manifest_path(config),
        {
            "schema_version": "databento_research_minute_backfill_manifest_v1",
            "generated_at": report.get("generated_at"),
            "storage_architecture": "ATP_RESEARCH_PARQUET_DUCKDB_JSON_MANIFEST",
            "raw_bars_root": str(_layout(config)["raw"] / "databento_minute_backfill"),
            "duckdb_path": str(_layout(config)["duckdb"]),
            "source": SOURCE,
            "dataset": report.get("dataset"),
            "schema": report.get("schema"),
            "timeframe": report.get("timeframe"),
            "symbols": report.get("symbols"),
            "partition_rows": partition_rows,
            "research_artifact": True,
            "runtime_artifact": False,
            "archive_artifact": False,
            "runtime_preflight_dashboard_truth": False,
            "can_submit": False,
            "live_money_eligible": False,
        },
    )


def _refresh_duckdb_view(*, config: ResearchMinuteBackfillConfig) -> dict[str, Any]:
    raw_root = _layout(config)["raw"] / "databento_minute_backfill"
    if not any(raw_root.glob("symbol=*/year=*/month=*/bars.parquet")):
        return {"registered": False, "status": "NO_PARQUET_PARTITIONS"}
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        return {"registered": False, "status": "DUCKDB_NOT_INSTALLED"}
    duckdb_path = _layout(config)["duckdb"]
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    escaped = str(raw_root / "**" / "*.parquet").replace("'", "''")
    connection = duckdb.connect(str(duckdb_path))
    try:
        connection.execute(
            "create or replace view databento_research_raw_bars_1m as "
            f"select * from read_parquet('{escaped}', union_by_name=true, filename=true)"
        )
    finally:
        connection.close()
    return {"registered": True, "status": "REGISTERED"}


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=_json_ready) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _json_ready(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value


def _record_timestamp(record: dict[str, Any]) -> datetime | None:
    for key in ("ts_event", "ts_recv", "timestamp", "bar_end", "time"):
        parsed = _parse_datetime(record.get(key))
        if parsed is not None:
            return parsed
    nested = record.get("hd")
    if isinstance(nested, dict):
        return _parse_datetime(nested.get("ts_event") or nested.get("ts_recv"))
    return None


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_optional_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coerce_volume(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _load_research_universe(config: ResearchMinuteBackfillConfig) -> dict[str, Any]:
    if config.research_universe_symbols is not None:
        symbols = _normalize_symbol_tuple(config.research_universe_symbols)
        return {
            "source": "config.research_universe_symbols",
            "schema_version": "inline_research_data_universe_v1",
            "allowed_symbols": symbols,
            "default_symbols": symbols,
            "routine_max_calendar_days": DEFAULT_ROUTINE_MAX_CALENDAR_DAYS,
            "estimated_1m_bars_per_calendar_day": DEFAULT_ESTIMATED_1M_BARS_PER_CALENDAR_DAY,
            "estimated_parquet_bytes_per_1m_bar": DEFAULT_ESTIMATED_PARQUET_BYTES_PER_1M_BAR,
        }
    universe_path = config.research_universe_path or DEFAULT_RESEARCH_UNIVERSE_PATH
    path = universe_path if universe_path.is_absolute() else Path(config.repo_root) / universe_path
    if not path.exists():
        symbols = FALLBACK_RESEARCH_SYMBOLS
        return {
            "source": f"fallback_missing:{path}",
            "schema_version": "fallback_research_data_universe_v1",
            "allowed_symbols": symbols,
            "default_symbols": symbols,
            "routine_max_calendar_days": DEFAULT_ROUTINE_MAX_CALENDAR_DAYS,
            "estimated_1m_bars_per_calendar_day": DEFAULT_ESTIMATED_1M_BARS_PER_CALENDAR_DAY,
            "estimated_parquet_bytes_per_1m_bar": DEFAULT_ESTIMATED_PARQUET_BYTES_PER_1M_BAR,
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    symbol_rows = payload.get("symbols") or {}
    allowed = tuple(
        sorted(
            str(symbol).strip().upper()
            for symbol, row in symbol_rows.items()
            if str(symbol).strip() and bool((row or {}).get("enabled", True))
        )
    )
    defaults = _normalize_symbol_tuple(payload.get("default_symbols") or allowed)
    policy = payload.get("routine_refresh_policy") or {}
    return {
        "source": str(path),
        "schema_version": payload.get("schema_version"),
        "allowed_symbols": allowed,
        "default_symbols": tuple(symbol for symbol in defaults if symbol in allowed),
        "routine_max_calendar_days": int(policy.get("max_calendar_days_without_operator_approval") or DEFAULT_ROUTINE_MAX_CALENDAR_DAYS),
        "estimated_1m_bars_per_calendar_day": int(policy.get("estimated_1m_bars_per_calendar_day") or DEFAULT_ESTIMATED_1M_BARS_PER_CALENDAR_DAY),
        "estimated_parquet_bytes_per_1m_bar": int(policy.get("estimated_parquet_bytes_per_1m_bar") or DEFAULT_ESTIMATED_PARQUET_BYTES_PER_1M_BAR),
    }


def _normalize_symbol_tuple(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))


def _normalize_symbols(symbols: Sequence[str], *, universe: dict[str, Any]) -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))
    allowed = set(universe["allowed_symbols"])
    invalid = [symbol for symbol in normalized if symbol not in allowed]
    if invalid:
        raise ValueError(f"Unsupported research symbols for this backfill: {invalid}; configured_universe={sorted(allowed)}")
    return normalized


def _require_explicit_date_range(config: ResearchMinuteBackfillConfig) -> None:
    if config.end_date is None:
        raise ValueError("Research minute backfill requires an explicit --end-date for preflight/download scope control.")


def _scope_estimate(
    *,
    config: ResearchMinuteBackfillConfig,
    symbols: tuple[str, ...],
    chunks: Sequence[BackfillChunk],
    requested_end_date: date,
    end_resolution: dict[str, Any],
    planned_end: datetime,
) -> dict[str, Any]:
    start = _start_datetime(config.start_date)
    requested_end = _end_datetime(requested_end_date)
    covered_seconds = max((planned_end - start).total_seconds(), 0)
    calendar_days = int((covered_seconds + 86399) // 86400)
    universe = _load_research_universe(config)
    bars_per_day = int(universe["estimated_1m_bars_per_calendar_day"])
    estimated_bars = calendar_days * bars_per_day * len(symbols)
    estimated_bytes = estimated_bars * int(universe["estimated_parquet_bytes_per_1m_bar"])
    routine_limit = int(universe["routine_max_calendar_days"])
    approval_required = calendar_days > routine_limit
    return {
        "requested_start": start.isoformat(),
        "requested_end": requested_end.isoformat(),
        "effective_requested_end": end_resolution["effective_requested_end"].isoformat(),
        "planned_fetch_end": planned_end.isoformat(),
        "end_resolution": {
            **end_resolution,
            "raw_requested_end": end_resolution["raw_requested_end"].isoformat(),
            "effective_requested_end": end_resolution["effective_requested_end"].isoformat(),
            "latest_closed_day_end": None
            if end_resolution.get("latest_closed_day_end") is None
            else end_resolution["latest_closed_day_end"].isoformat(),
            "provider_available_end": None
            if end_resolution.get("provider_available_end") is None
            else end_resolution["provider_available_end"].isoformat(),
        },
        "symbol_count": len(symbols),
        "chunk_count": len(chunks),
        "calendar_days": calendar_days,
        "routine_max_calendar_days_without_operator_approval": routine_limit,
        "estimated_1m_bars_per_calendar_day": bars_per_day,
        "estimated_1m_bar_count": estimated_bars,
        "estimated_parquet_bytes": estimated_bytes,
        "estimated_parquet_megabytes": round(estimated_bytes / 1_000_000, 3),
        "approval_required": approval_required,
        "approval_reason": "REQUESTED_RANGE_EXCEEDS_ROUTINE_WEEKLY_POLICY" if approval_required else None,
        "routine_weekly_policy": not approval_required,
    }


def _chunk_months(chunk: str) -> int:
    value = str(chunk).strip().lower()
    if value not in APPROVED_CHUNKS:
        raise ValueError(f"--chunk must be one of {sorted(APPROVED_CHUNKS)}")
    return APPROVED_CHUNKS[value]


def _resolve_end_boundary(*, config: ResearchMinuteBackfillConfig, requested_end_date: date, now: datetime) -> dict[str, Any]:
    mode = str(config.end_boundary_mode or "closed-day").strip().lower()
    if mode not in {"closed-day", "intraday-available-end"}:
        raise ValueError("--end-boundary-mode must be one of ['closed-day', 'intraday-available-end']")
    raw_end = _end_datetime(requested_end_date)
    effective_end = raw_end
    cap_reasons: list[str] = []
    latest_closed_day_end: datetime | None = None
    if mode == "closed-day":
        latest_closed_day_end = _end_datetime((_coerce_now(now) - timedelta(days=1)).date())
        if effective_end > latest_closed_day_end:
            effective_end = latest_closed_day_end
            cap_reasons.append("LATEST_FULLY_CLOSED_DAY")
    provider_available_end = _coerce_optional_datetime(config.provider_available_end)
    if provider_available_end is not None and effective_end > provider_available_end:
        effective_end = provider_available_end
        cap_reasons.append("PROVIDER_AVAILABLE_END")
    return {
        "mode": mode,
        "raw_requested_end": raw_end,
        "effective_requested_end": effective_end,
        "latest_closed_day_end": latest_closed_day_end,
        "provider_available_end": provider_available_end,
        "capped": bool(cap_reasons),
        "cap_reason": "+".join(cap_reasons) if cap_reasons else None,
    }


def _backfill_end_datetime(*, config: ResearchMinuteBackfillConfig, effective_requested_end: datetime) -> datetime:
    """Clip the long-lookback pull before already-loaded canonical history."""
    if config.loaded_history_start_date is None:
        return effective_requested_end
    if effective_requested_end < _start_datetime(config.loaded_history_start_date):
        return effective_requested_end
    return _start_datetime(config.loaded_history_start_date)


def _loaded_history_overlap_skipped(*, config: ResearchMinuteBackfillConfig, requested_end_date: date) -> bool:
    return config.loaded_history_start_date is not None and requested_end_date >= config.loaded_history_start_date


def _start_datetime(value: date) -> datetime:
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def _end_datetime(value: date) -> datetime:
    return datetime.combine(value, time.max, tzinfo=timezone.utc).replace(microsecond=0)


def _add_months(value: datetime, months: int) -> datetime:
    month_index = value.year * 12 + (value.month - 1) + months
    year = month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month, day=1, hour=0, minute=0, second=0, microsecond=0)


def _continuous_symbol(symbol: str) -> str:
    return f"{symbol}.v.0"


def _symbol_status(*, dry_run: bool, written: int, skipped: int, failed: int, total: int) -> str:
    if dry_run:
        return "DRY_RUN"
    if failed:
        return "PARTIAL_FAILED" if written or skipped else "FAILED"
    if written or skipped == total:
        return "COMPLETE_OR_RESUMED"
    return "NO_CHUNKS"


def _final_verdict(
    *,
    dry_run: bool,
    approval_required: bool = False,
    provider_error_count: int,
    written_count: int,
    skipped_count: int,
    empty_count: int,
    would_fetch_count: int,
) -> str:
    if dry_run:
        if approval_required:
            return "RESEARCH_MINUTE_BACKFILL_DRY_RUN_APPROVAL_REQUIRED"
        return "RESEARCH_MINUTE_BACKFILL_DRY_RUN_READY"
    if provider_error_count:
        return "RESEARCH_MINUTE_BACKFILL_PARTIAL_OR_BLOCKED"
    if written_count or skipped_count or empty_count:
        return "RESEARCH_MINUTE_BACKFILL_COMPLETE_OR_RESUMED"
    if would_fetch_count:
        return "RESEARCH_MINUTE_BACKFILL_NOT_STARTED"
    return "RESEARCH_MINUTE_BACKFILL_NOOP"


def _require_pyarrow() -> Any:
    try:
        import pyarrow  # type: ignore
        import pyarrow.parquet  # type: ignore  # noqa: F401
    except ModuleNotFoundError as exc:
        raise RuntimeError("Research minute backfill requires pyarrow in the repo environment.") from exc
    return pyarrow


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Research-only Databento historical 1m backfill for configured research data universe.")
    parser.add_argument("--symbols")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--end-date", required=True)
    parser.add_argument(
        "--loaded-history-start-date",
        default=DEFAULT_LOADED_HISTORY_START_DATE.isoformat(),
        help="First date already covered by canonical research data; default skips duplicate 2020+ pulls.",
    )
    parser.add_argument("--chunk", choices=sorted(APPROVED_CHUNKS), default="monthly")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--max-chunks", type=int)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--research-universe", type=Path, default=DEFAULT_RESEARCH_UNIVERSE_PATH)
    parser.add_argument("--operator-approved-large-refresh", action="store_true")
    parser.add_argument(
        "--end-boundary-mode",
        choices=("closed-day", "intraday-available-end"),
        default="closed-day",
        help="closed-day caps current/future requests to the latest fully closed UTC day; intraday-available-end may use --provider-available-end.",
    )
    parser.add_argument("--provider-available-end", help="Optional provider available-end timestamp used to cap intraday/current-day requests.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    universe_path = Path(args.research_universe)
    defaults = _load_research_universe(
        ResearchMinuteBackfillConfig(repo_root=Path(args.repo_root), research_universe_path=universe_path, end_date=date.fromisoformat(str(args.end_date)))
    )["default_symbols"]
    symbols_arg = args.symbols if args.symbols is not None else ",".join(defaults)
    symbols = tuple(symbol.strip().upper() for symbol in str(symbols_arg).split(",") if symbol.strip())
    result = run_research_minute_backfill(
        config=ResearchMinuteBackfillConfig(
            repo_root=Path(args.repo_root),
            output_root=Path(args.output_root),
            symbols=symbols,
            start_date=date.fromisoformat(str(args.start_date)),
            end_date=date.fromisoformat(str(args.end_date)) if args.end_date else None,
            loaded_history_start_date=date.fromisoformat(str(args.loaded_history_start_date)) if args.loaded_history_start_date else None,
            chunk=str(args.chunk),
            dry_run=bool(args.dry_run),
            max_chunks=args.max_chunks,
            force=bool(args.force),
            env_file=args.env_file,
            json_output=args.json_output,
            dataset=str(args.dataset),
            base_url=str(args.base_url),
            limit=args.limit,
            research_universe_path=universe_path,
            operator_approved_large_refresh=bool(args.operator_approved_large_refresh),
            end_boundary_mode=str(args.end_boundary_mode),
            provider_available_end=_parse_datetime(args.provider_available_end),
        )
    )
    print(
        json.dumps(
            {
                "final_verdict": result.report["final_verdict"],
                "symbols": result.report["symbols"],
                "chunk_count": result.report["chunk_count"],
                "planned_fetch_end": result.report["planned_fetch_end"],
                "effective_requested_end": result.report["effective_requested_end"],
                "end_boundary_capped": result.report["end_boundary_capped"],
                "end_boundary_cap_reason": result.report["end_boundary_cap_reason"],
                "loaded_history_overlap_skipped": result.report["loaded_history_overlap_skipped"],
                "partitions_written": result.report["partitions_written"],
                "partitions_would_fetch": result.report["partitions_would_fetch"],
                "provider_error_count": result.report["provider_error_count"],
                "can_submit": result.report["can_submit"],
                "live_money_eligible": result.report["live_money_eligible"],
                "report_paths": [str(path) for path in result.report_paths],
            },
            sort_keys=True,
        )
    )
    return 0 if result.report["final_verdict"] != "RESEARCH_MINUTE_BACKFILL_BLOCKED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
