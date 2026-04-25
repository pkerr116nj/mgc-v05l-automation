"""Research-grade market-data integrity audit, maintenance planning, and health gating."""

from __future__ import annotations

import csv
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Callable, Sequence
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from ..config_models import StrategySettings, load_settings_from_files
from ..research.trend_participation.storage import build_layout as build_report_layout, write_storage_manifest
from ..research.warehouse_historical_evaluator.derived_materializer import materialize_derived_timeframe_partition
from ..research.warehouse_historical_evaluator.layout import build_layout as build_warehouse_layout
from ..research.warehouse_historical_evaluator.multi_symbol_runner import (
    DEFAULT_BASELINE_REPORT_PATH,
    DEFAULT_BASKET,
    run_multi_symbol_warehouse_shard,
)
from ..research.warehouse_historical_evaluator.raw_materializer import export_canonical_1m_partition
from .canonical_maintenance import CanonicalMarketDataMaintenanceService
from .databento_provider import DatabentoHttpError, DatabentoMarketDataProvider
from .provider_config import load_market_data_providers_config, provider_config_path
from .provider_ingest import HistoricalMarketDataIngestionService
from .provider_models import HistoricalBarsRequest
from .timeframes import normalize_timeframe_label

NEW_YORK = ZoneInfo("America/New_York")
RESEARCH_SESSION_EXPR = (
    "case when substr(timestamp,12,5) >= '18:00' then substr(timestamp,1,10) "
    "else date(substr(timestamp,1,10), '-1 day') end"
)
CANONICAL_1M_SOURCE = "historical_1m_canonical"
CANONICAL_5M_SOURCE = "historical_5m_canonical"
CANONICAL_10M_SOURCE = "historical_10m_canonical"
EXTENDED_5M_SOURCES = ("vendor_5m_extended",)
FORBIDDEN_RESEARCH_SOURCES = (
    "schwab_history",
    "schwab_live_poll",
    "databento_live",
)
TRADE_DATASETS = ("lane_entries", "lane_closed_trades")
DEFAULT_AUDIT_PHASE_TIMEOUT_SECONDS = 20.0


@dataclass(frozen=True)
class IntegritySourcePolicy:
    canonical_1m_source: str = CANONICAL_1M_SOURCE
    canonical_5m_source: str = CANONICAL_5M_SOURCE
    canonical_10m_source: str = CANONICAL_10M_SOURCE
    extended_5m_sources: tuple[str, ...] = EXTENDED_5M_SOURCES
    forbidden_research_sources: tuple[str, ...] = FORBIDDEN_RESEARCH_SOURCES
    execution_only_sources: tuple[str, ...] = FORBIDDEN_RESEARCH_SOURCES


@dataclass(frozen=True)
class AuditPhaseTimeout(RuntimeError):
    phase: str
    timeout_seconds: float

    def __str__(self) -> str:
        return f"audit phase '{self.phase}' exceeded timeout={self.timeout_seconds}s"


class EmptyConfiguredScope(RuntimeError):
    """Raised to short-circuit audit execution when the requested scope has no configured symbols."""


def run_research_data_integrity_audit(
    *,
    output_dir: Path,
    replay_db_path: Path,
    warehouse_root: Path,
    settings: StrategySettings | None = None,
    provider_config: str | Path | None = None,
    start_date: str = "2024-01-01",
    end_timestamp: datetime | None = None,
    lane_symbol_map: dict[str, list[str]] | None = None,
    symbols: Sequence[str] | None = None,
    phase_timeout_seconds: float = DEFAULT_AUDIT_PHASE_TIMEOUT_SECONDS,
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    policy = IntegritySourcePolicy()
    latest_target = (end_timestamp or datetime.now(tz=NEW_YORK)).astimezone(NEW_YORK)
    phase_rows: list[dict[str, Any]] = []
    progress_rows: list[dict[str, Any]] = []
    provider_cfg = load_market_data_providers_config(provider_config)
    requested_symbols = [str(symbol).strip().upper() for symbol in (symbols or ())]
    instruments = _resolve_instruments(provider_cfg=provider_cfg, requested_symbols=requested_symbols)
    payload = _build_audit_payload_skeleton(
        replay_db_path=replay_db_path,
        warehouse_root=warehouse_root,
        provider_config=provider_config,
        policy=policy,
        instruments=instruments,
        requested_symbols=requested_symbols,
        start_date=start_date,
        latest_target=latest_target,
    )
    artifacts: dict[str, str] = {}
    lane_map = _scoped_lane_symbol_map(
        lane_symbol_map=lane_symbol_map,
        requested_symbols=requested_symbols,
    )
    canonical_service = CanonicalMarketDataMaintenanceService(
        database_url=_database_url_from_path(replay_db_path),
        provider_config_path=provider_config,
    )

    try:
        _run_audit_phase(
            "registry_load",
            phase_rows=phase_rows,
            progress_rows=progress_rows,
            progress_callback=progress_callback,
            detail={"instrument_count": len(instruments)},
            fn=lambda: None,
        )
        if not instruments:
            payload["audit_runtime"]["status"] = "completed"
            payload["audit_runtime"]["reason"] = "empty_configured_scope"
            payload["audit_runtime"]["phase"] = "registry_load"
            payload["trade_alignment"] = {
                "symbol_rows": [],
                "blocking_issues": ["empty_configured_scope"],
                "analysis_allowed": False,
                "reason": "empty_configured_scope",
            }
            payload["health"] = {
                "overall_status": "skipped",
                "can_assert_complete_and_reliable": False,
                "instrument_rows": [],
                "blocking_issues": ["empty_configured_scope"],
                "reason": "empty_configured_scope",
                "unmapped_symbols": payload["instrument_registry"]["unknown_requested_symbols"],
            }
            payload["repair_plan"] = {
                "missing_ranges": [],
                "repair_commands": [],
                "warehouse_rebuild_commands": [],
                "trade_rematerialization_commands": [],
                "do_not_run_strategy_research_yet": True,
                "reason": "empty_configured_scope",
            }
            payload["daily_maintenance_plan"] = {
                "mode_supported": ["incremental_update", "full_backfill", "dry_run_validation"],
                "per_instrument": [],
                "daily_commands": [],
                "warehouse_commands": [],
                "trade_commands": [],
                "reason": "empty_configured_scope",
            }
            raise EmptyConfiguredScope
        canonical_phase = _run_audit_phase(
            "canonical_1m_coverage",
            phase_rows=phase_rows,
            progress_rows=progress_rows,
            progress_callback=progress_callback,
            detail={"symbols": list(instruments)},
            fn=lambda: _audit_canonical_replay_coverage(
                replay_db_path=replay_db_path,
                instruments=instruments,
                policy=policy,
                canonical_service=canonical_service,
            ),
        )
        payload["replay_audit"].update(canonical_phase)
        payload["sample_coverage"]["actual_replay_latest_timestamp"] = payload["replay_audit"]["global_latest_canonical_1m_ts"]

        overlap_phase = _run_audit_phase(
            "source_overlap_checks",
            phase_rows=phase_rows,
            progress_rows=progress_rows,
            progress_callback=progress_callback,
            detail={"timeout_seconds": phase_timeout_seconds},
            fn=lambda: _audit_source_overlap_checks(
                replay_db_path=replay_db_path,
                instruments=instruments,
                policy=policy,
                phase_timeout_seconds=phase_timeout_seconds,
            ),
        )
        payload["replay_audit"].update(overlap_phase)

        warehouse_audit = _run_audit_phase(
            "warehouse_integrity_checks",
            phase_rows=phase_rows,
            progress_rows=progress_rows,
            progress_callback=progress_callback,
            fn=lambda: _audit_warehouse(warehouse_root=warehouse_root),
        )
        payload["warehouse_audit"] = warehouse_audit

        alignment_bundle = _run_audit_phase(
            "trade_replay_artifact_alignment",
            phase_rows=phase_rows,
            progress_rows=progress_rows,
            progress_callback=progress_callback,
            fn=lambda: _build_alignment_bundle(
                replay_db_path=replay_db_path,
                warehouse_root=warehouse_root,
                provider_config=provider_config,
                instruments=instruments,
                replay_audit=payload["replay_audit"],
                warehouse_audit=payload["warehouse_audit"],
                lane_symbol_map=lane_map,
                start_date=start_date,
                latest_target=latest_target,
                policy=policy,
            ),
        )
        payload["trade_alignment"] = alignment_bundle["trade_alignment"]
        payload["health"] = alignment_bundle["health"]
        payload["repair_plan"] = alignment_bundle["repair_plan"]
        payload["daily_maintenance_plan"] = alignment_bundle["daily_maintenance_plan"]
        payload["audit_runtime"]["status"] = "completed"
    except AuditPhaseTimeout as exc:
        payload["audit_runtime"]["status"] = "blocked"
        payload["audit_runtime"]["reason"] = "audit_phase_timeout"
        payload["audit_runtime"]["phase"] = exc.phase
        payload["health"] = _build_blocked_health(
            replay_audit=payload["replay_audit"],
            phase=exc.phase,
            reason="audit_phase_timeout",
        )
        payload["trade_alignment"]["analysis_allowed"] = False
        payload["trade_alignment"]["blocking_issues"] = [
            f"audit blocked before trade/replay alignment completed (phase={exc.phase})"
        ]
    except EmptyConfiguredScope:
        pass
    except Exception as exc:
        payload["audit_runtime"]["status"] = "failed"
        payload["audit_runtime"]["reason"] = type(exc).__name__
        payload["audit_runtime"]["phase"] = phase_rows[-1]["phase"] if phase_rows else "unknown"
        payload["audit_runtime"]["error"] = str(exc)
        payload["health"] = _build_blocked_health(
            replay_audit=payload["replay_audit"],
            phase=payload["audit_runtime"]["phase"],
            reason=type(exc).__name__,
        )
        payload["trade_alignment"]["analysis_allowed"] = False
        payload["trade_alignment"]["blocking_issues"] = [
            f"audit failed before trade/replay alignment completed (phase={payload['audit_runtime']['phase']}, reason={type(exc).__name__})"
        ]
    finally:
        payload["audit_runtime"]["phase_rows"] = phase_rows
        payload["audit_runtime"]["progress_rows"] = progress_rows

    try:
        artifacts = _run_audit_phase(
            "report_write",
            phase_rows=phase_rows,
            progress_rows=progress_rows,
            progress_callback=progress_callback,
            fn=lambda: _write_artifacts(output_dir=output_dir, payload=payload),
        )
    except Exception as exc:
        payload["audit_runtime"]["status"] = "failed" if payload["audit_runtime"]["status"] == "completed" else payload["audit_runtime"]["status"]
        payload["audit_runtime"]["report_write_error"] = str(exc)
        artifacts = _write_fallback_artifacts(output_dir=output_dir, payload=payload)

    payload["audit_runtime"]["phase_rows"] = phase_rows
    payload["audit_runtime"]["progress_rows"] = progress_rows
    payload["audit_runtime"]["artifact_write_completed"] = bool(artifacts)
    payload["analysis_allowed"] = bool(payload["trade_alignment"]["analysis_allowed"])
    payload["overall_status"] = payload["health"]["overall_status"]
    if artifacts:
        artifacts = _write_artifacts(output_dir=output_dir, payload=payload)
    return {"payload": payload, "artifacts": artifacts}


def _build_audit_payload_skeleton(
    *,
    replay_db_path: Path,
    warehouse_root: Path,
    provider_config: str | Path | None,
    policy: IntegritySourcePolicy,
    instruments: Sequence[str],
    requested_symbols: Sequence[str],
    start_date: str,
    latest_target: datetime,
) -> dict[str, Any]:
    return {
        "module": "Research Market Data Integrity",
        "generated_at": datetime.now(UTC).isoformat(),
        "replay_db_path": str(replay_db_path.resolve()),
        "warehouse_root": str(warehouse_root.resolve()),
        "provider_config_path": str(provider_config_path(provider_config)),
        "policy": asdict(policy),
        "instrument_registry": {
            "instrument_count": len(instruments),
            "instruments": list(instruments),
            "requested_symbols": list(requested_symbols),
            "unknown_requested_symbols": sorted(set(requested_symbols) - set(instruments)),
            "all_instruments_treated_equally": True,
        },
        "sample_coverage": {
            "requested_start_date": start_date,
            "requested_latest_target": latest_target.isoformat(),
            "actual_replay_latest_timestamp": None,
        },
        "replay_audit": {
            "coverage_rows": [],
            "monthly_density_rows": [],
            "canonical_coverage_rows": [],
            "duplicate_rows": [],
            "storage_source_overlap_rows": [],
            "session_audit": {
                "per_month_session_counts": [],
                "session_gap_rows": [],
                "partial_session_rows": [],
            },
            "source_overlap_rows": [],
            "five_minute_surface_rows": [],
            "global_latest_canonical_1m_ts": None,
        },
        "warehouse_audit": {"dataset_reports": _empty_dataset_reports()},
        "trade_alignment": {"symbol_rows": [], "blocking_issues": [], "analysis_allowed": False},
        "health": {"overall_status": "pending", "can_assert_complete_and_reliable": False, "instrument_rows": [], "blocking_issues": []},
        "repair_plan": {"missing_ranges": [], "repair_commands": [], "warehouse_rebuild_commands": [], "trade_rematerialization_commands": [], "do_not_run_strategy_research_yet": True},
        "daily_maintenance_plan": {"mode_supported": ["incremental_update", "full_backfill", "dry_run_validation"], "per_instrument": [], "daily_commands": [], "warehouse_commands": [], "trade_commands": []},
        "audit_runtime": {
            "status": "running",
            "reason": None,
            "phase": None,
            "phase_rows": [],
            "progress_rows": [],
        },
    }


def _empty_dataset_reports() -> dict[str, Any]:
    return {
        "raw_bars_1m": {"root": None, "file_count": 0, "coverage_rows": [], "overall_rows": [], "duplicate_rows": []},
        "derived_bars_5m": {"root": None, "file_count": 0, "coverage_rows": [], "overall_rows": [], "duplicate_rows": []},
        "lane_entries": {"root": None, "file_count": 0, "coverage_rows": [], "overall_rows": [], "duplicate_rows": []},
        "lane_closed_trades": {"root": None, "file_count": 0, "coverage_rows": [], "overall_rows": [], "duplicate_rows": []},
    }


def _resolve_instruments(
    *,
    provider_cfg: Any,
    requested_symbols: Sequence[str],
) -> tuple[str, ...]:
    registry = tuple(sorted(symbol.upper() for symbol in provider_cfg.databento.pilot_symbols))
    if not requested_symbols:
        return registry
    requested = tuple(sorted({symbol.upper() for symbol in requested_symbols}))
    return tuple(symbol for symbol in registry if symbol in requested)


def _run_audit_phase(
    phase: str,
    *,
    phase_rows: list[dict[str, Any]],
    progress_rows: list[dict[str, Any]],
    progress_callback: Callable[[dict[str, Any]], None] | None,
    fn: Callable[[], Any],
    detail: dict[str, Any] | None = None,
) -> Any:
    started_at = datetime.now(UTC).isoformat()
    start_monotonic = time.monotonic()
    start_event = {"phase": phase, "event": "start", "started_at": started_at, "detail": detail or {}}
    progress_rows.append(start_event)
    if progress_callback is not None:
        progress_callback(dict(start_event))
    try:
        result = fn()
    except Exception as exc:
        duration_seconds = round(time.monotonic() - start_monotonic, 3)
        phase_row = {
            "phase": phase,
            "status": "blocked" if isinstance(exc, AuditPhaseTimeout) else "failed",
            "started_at": started_at,
            "finished_at": datetime.now(UTC).isoformat(),
            "duration_seconds": duration_seconds,
            "reason": "audit_phase_timeout" if isinstance(exc, AuditPhaseTimeout) else type(exc).__name__,
        }
        phase_rows.append(phase_row)
        end_event = {**phase_row, "event": "end"}
        progress_rows.append(end_event)
        if progress_callback is not None:
            progress_callback(dict(end_event))
        raise
    duration_seconds = round(time.monotonic() - start_monotonic, 3)
    phase_row = {
        "phase": phase,
        "status": "completed",
        "started_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "duration_seconds": duration_seconds,
    }
    phase_rows.append(phase_row)
    end_event = {**phase_row, "event": "end"}
    progress_rows.append(end_event)
    if progress_callback is not None:
        progress_callback(dict(end_event))
    return result


def _audit_canonical_replay_coverage(
    *,
    replay_db_path: Path,
    instruments: Sequence[str],
    policy: IntegritySourcePolicy,
    canonical_service: CanonicalMarketDataMaintenanceService,
) -> dict[str, Any]:
    conn = sqlite3.connect(replay_db_path)
    try:
        conn.row_factory = sqlite3.Row
        coverage_rows = _coverage_rows(conn=conn, instruments=instruments)
        monthly_density_rows = _monthly_density_rows(conn=conn, instruments=instruments)
        session_audit = _session_audit(conn=conn, instruments=instruments)
    finally:
        conn.close()

    canonical_coverage_rows: list[dict[str, Any]] = []
    for instrument in instruments:
        coverage = canonical_service.audit_coverage(symbol=instrument, timeframe="1m", data_source=policy.canonical_1m_source)
        canonical_coverage_rows.append(
            {
                "instrument": instrument,
                "timeframe": "1m",
                "data_source": policy.canonical_1m_source,
                "bar_count": coverage.bar_count,
                "earliest_ts": coverage.earliest,
                "latest_ts": coverage.latest,
                "gap_count": coverage.gap_count,
                "missing_bar_count": int(sum(gap.missing_minutes for gap in coverage.gaps)),
            }
        )
    latest = max((row["latest_ts"] for row in canonical_coverage_rows if row["latest_ts"]), default=None)
    return {
        "coverage_rows": coverage_rows,
        "monthly_density_rows": monthly_density_rows,
        "canonical_coverage_rows": canonical_coverage_rows,
        "session_audit": session_audit,
        "global_latest_canonical_1m_ts": latest,
    }


def _audit_source_overlap_checks(
    *,
    replay_db_path: Path,
    instruments: Sequence[str],
    policy: IntegritySourcePolicy,
    phase_timeout_seconds: float,
) -> dict[str, Any]:
    conn = sqlite3.connect(replay_db_path)
    try:
        conn.row_factory = sqlite3.Row
        duplicate_rows = _run_sqlite_phase_with_timeout(
            conn,
            phase="source_overlap_checks",
            timeout_seconds=phase_timeout_seconds,
            fn=lambda: _duplicate_rows(conn=conn, instruments=instruments),
        )
        storage_source_overlap_rows = _run_sqlite_phase_with_timeout(
            conn,
            phase="source_overlap_checks",
            timeout_seconds=phase_timeout_seconds,
            fn=lambda: _source_overlap_rows(conn=conn, instruments=instruments),
        )
        source_overlap_rows = _run_sqlite_phase_with_timeout(
            conn,
            phase="source_overlap_checks",
            timeout_seconds=phase_timeout_seconds,
            fn=lambda: _research_surface_overlap_rows(conn=conn, instruments=instruments, policy=policy),
        )
        five_minute_rows = _five_minute_surface_rows(conn=conn, instruments=instruments, policy=policy)
    finally:
        conn.close()
    return {
        "duplicate_rows": duplicate_rows,
        "storage_source_overlap_rows": storage_source_overlap_rows,
        "source_overlap_rows": source_overlap_rows,
        "five_minute_surface_rows": five_minute_rows,
    }


def _build_alignment_bundle(
    *,
    replay_db_path: Path,
    warehouse_root: Path,
    provider_config: str | Path | None,
    instruments: Sequence[str],
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    lane_symbol_map: dict[str, list[str]],
    start_date: str,
    latest_target: datetime,
    policy: IntegritySourcePolicy,
) -> dict[str, Any]:
    trade_alignment = _audit_trade_alignment(
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        lane_symbol_map=lane_symbol_map,
    )
    health = _build_health_report(
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        trade_alignment=trade_alignment,
        policy=policy,
    )
    repair_plan = _build_repair_plan(
        replay_db_path=replay_db_path,
        warehouse_root=warehouse_root,
        provider_config_path=provider_config,
        instruments=instruments,
        replay_audit=replay_audit,
        warehouse_audit=warehouse_audit,
        trade_alignment=trade_alignment,
        start_date=start_date,
        latest_target=latest_target,
    )
    daily_plan = _build_daily_maintenance_plan(
        replay_db_path=replay_db_path,
        warehouse_root=warehouse_root,
        provider_config_path=provider_config,
        instruments=instruments,
        replay_audit=replay_audit,
        latest_target=latest_target,
    )
    return {
        "trade_alignment": trade_alignment,
        "health": health,
        "repair_plan": repair_plan,
        "daily_maintenance_plan": daily_plan,
    }


def _build_blocked_health(
    *,
    replay_audit: dict[str, Any],
    phase: str,
    reason: str,
) -> dict[str, Any]:
    instrument_rows = [
        {
            "instrument": row["instrument"],
            "latest_timestamp": row.get("latest_ts"),
            "missing_bars": row.get("missing_bar_count", 0),
            "missing_sessions": 0,
            "duplicate_bar_count": 0,
            "warehouse_duplicate_bar_count": 0,
            "source_consistency_ok": None,
            "warehouse_1m_aligned": None,
            "warehouse_5m_aligned": None,
            "trade_artifact_fresh": None,
            "status": "blocked",
            "issues": [f"audit_blocked:{phase}:{reason}"],
        }
        for row in replay_audit.get("canonical_coverage_rows", [])
    ]
    return {
        "overall_status": "blocked",
        "can_assert_complete_and_reliable": False,
        "instrument_rows": instrument_rows,
        "blocking_issues": [f"audit blocked in phase={phase} reason={reason}"],
    }


def _run_sqlite_phase_with_timeout(
    conn: sqlite3.Connection,
    *,
    phase: str,
    timeout_seconds: float,
    fn: Callable[[], Any],
) -> Any:
    if timeout_seconds <= 0:
        return fn()
    deadline = time.monotonic() + timeout_seconds

    def _progress_handler() -> int:
        return 1 if time.monotonic() >= deadline else 0

    conn.set_progress_handler(_progress_handler, 10_000)
    try:
        return fn()
    except sqlite3.OperationalError as exc:
        if "interrupted" in str(exc).lower():
            raise AuditPhaseTimeout(phase=phase, timeout_seconds=timeout_seconds) from exc
        raise
    finally:
        conn.set_progress_handler(None, 0)


def execute_research_market_data_backfill(
    *,
    replay_db_path: Path,
    provider_config: str | Path | None = None,
    config_paths: Sequence[str | Path] | None = None,
    symbols: Sequence[str],
    start_ts: datetime,
    end_ts: datetime,
    run_gap_repair: bool = False,
    derive_timeframes: Sequence[str] = (),
    progress_callback: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    settings = load_settings_from_files(config_paths or [Path("config/base.yaml"), Path("config/replay.yaml")])
    provider_cfg = load_market_data_providers_config(provider_config)
    requested_symbols = [str(symbol).strip().upper() for symbol in symbols]
    configured_lookup = {str(symbol).strip().upper() for symbol in provider_cfg.databento.pilot_symbols.keys()}
    configured_symbols = [symbol for symbol in requested_symbols if symbol in configured_lookup]
    unmapped_symbols = [symbol for symbol in requested_symbols if symbol not in configured_lookup]
    provider = None
    ingestion = None
    maintenance = None
    if configured_symbols:
        provider = DatabentoMarketDataProvider(settings, repo_root=Path.cwd(), config_path=provider_config)
        ingestion = HistoricalMarketDataIngestionService(
            database_url=settings.database_url,
            provider_config_path=provider_config,
        )
        maintenance = CanonicalMarketDataMaintenanceService(
            database_url=settings.database_url,
            provider_config_path=provider_config,
        )
    audits: list[dict[str, Any]] = []
    derivations: list[dict[str, Any]] = []
    progress_rows: list[dict[str, Any]] = []
    symbol_results: list[dict[str, Any]] = []
    normalized_derive_timeframes = tuple(
        sorted({normalize_timeframe_label(timeframe) for timeframe in derive_timeframes if str(timeframe).strip()})
    )
    for symbol in unmapped_symbols:
        event = _backfill_progress_event(
            symbol=symbol,
            label="unmapped_symbol",
            status="skipped",
            detail={"reason": "not_configured_in_provider"},
        )
        progress_rows.append(event)
        if progress_callback is not None:
            progress_callback(event)
        symbol_results.append(
            {
                "symbol": symbol,
                "status": "skipped",
                "outcome": "unmapped_symbol",
                "labels": ["unmapped_symbol"],
                "detail": {"reason": "not_configured_in_provider"},
            }
        )

    for symbol in configured_symbols:
        labels: list[str] = []
        request_started = _backfill_progress_event(symbol=symbol, label="request_started", status="running")
        progress_rows.append(request_started)
        if progress_callback is not None:
            progress_callback(request_started)
        labels.append("request_started")

        def _emit_ingest_progress(event: dict[str, Any], *, _symbol: str = symbol) -> None:
            emitted = _backfill_progress_event(
                symbol=_symbol,
                label=str(event.get("label")),
                status=str(event.get("status")),
                detail=dict(event.get("detail") or {}),
            )
            progress_rows.append(emitted)
            if progress_callback is not None:
                progress_callback(emitted)
            labels.append(str(event.get("label")))

        try:
            assert ingestion is not None
            assert provider is not None
            ingest_audit = ingestion.ingest(
                provider=provider,
                request=HistoricalBarsRequest(
                    internal_symbol=symbol,
                    timeframe="1m",
                    start=start_ts,
                    end=end_ts,
                ),
                progress_callback=_emit_ingest_progress,
            )
        except DatabentoHttpError as exc:
            label = "provider_timeout" if "timeout" in str(exc).lower() else "provider_error"
            event = _backfill_progress_event(
                symbol=symbol,
                label=label,
                status="failed",
                detail={"message": str(exc)},
            )
            progress_rows.append(event)
            if progress_callback is not None:
                progress_callback(event)
            labels.append(label)
            symbol_results.append(
                {
                    "symbol": symbol,
                    "status": "failed",
                    "outcome": label,
                    "labels": labels,
                    "detail": {"message": str(exc)},
                }
            )
            continue
        except Exception as exc:
            event = _backfill_progress_event(
                symbol=symbol,
                label="provider_error",
                status="failed",
                detail={"message": str(exc)},
            )
            progress_rows.append(event)
            if progress_callback is not None:
                progress_callback(event)
            labels.append("provider_error")
            symbol_results.append(
                {
                    "symbol": symbol,
                    "status": "failed",
                    "outcome": "provider_error",
                    "labels": labels,
                    "detail": {"message": str(exc)},
                }
            )
            continue

        audits.append(asdict(ingest_audit))
        outcome = "zero_records_no_data" if ingest_audit.fetched_bar_count == 0 else "fetch_completed"
        fetch_completed = _backfill_progress_event(
            symbol=symbol,
            label=outcome,
            status="completed",
            detail={
                "fetched_bar_count": ingest_audit.fetched_bar_count,
                "inserted_bar_count": ingest_audit.inserted_bar_count,
                "skipped_existing_count": ingest_audit.skipped_existing_count,
                "coverage_after_latest": ingest_audit.after.latest,
            },
        )
        progress_rows.append(fetch_completed)
        if progress_callback is not None:
            progress_callback(fetch_completed)
        labels.append(outcome)

        gap_repair_payload: dict[str, Any] | None = None
        if run_gap_repair:
            assert maintenance is not None
            assert provider is not None
            gap_started = _backfill_progress_event(symbol=symbol, label="gap_repair_started", status="running")
            progress_rows.append(gap_started)
            if progress_callback is not None:
                progress_callback(gap_started)
            labels.append("gap_repair_started")
            gap_repair_payload = maintenance.backfill_detected_gaps(provider=provider, symbol=symbol)
            audits.append({"gap_repair": gap_repair_payload})
            gap_completed = _backfill_progress_event(
                symbol=symbol,
                label="gap_repair_completed",
                status="completed",
                detail={"gap_count": gap_repair_payload.get("gap_count", 0)},
            )
            progress_rows.append(gap_completed)
            if progress_callback is not None:
                progress_callback(gap_completed)
            labels.append("gap_repair_completed")

        symbol_derivations: list[dict[str, Any]] = []
        for timeframe in normalized_derive_timeframes:
            assert maintenance is not None
            derive_started_label = f"derive_{timeframe}_started"
            derive_completed_label = f"derive_{timeframe}_completed"
            derive_started = _backfill_progress_event(symbol=symbol, label=derive_started_label, status="running")
            progress_rows.append(derive_started)
            if progress_callback is not None:
                progress_callback(derive_started)
            labels.append(derive_started_label)
            derivation = asdict(maintenance.derive_timeframe(symbol=symbol, target_timeframe=timeframe))
            derivations.append(derivation)
            symbol_derivations.append(derivation)
            derive_completed = _backfill_progress_event(
                symbol=symbol,
                label=derive_completed_label,
                status="completed",
                detail={"derived_bar_count": derivation.get("derived_bar_count")},
            )
            progress_rows.append(derive_completed)
            if progress_callback is not None:
                progress_callback(derive_completed)
            labels.append(derive_completed_label)

        symbol_results.append(
            {
                "symbol": symbol,
                "status": "completed",
                "outcome": outcome,
                "labels": labels,
                "ingest_audit": asdict(ingest_audit),
                "gap_repair": gap_repair_payload,
                "derivations": symbol_derivations,
            }
        )
    return {
        "mode": "executed_backfill",
        "replay_db_path": str(replay_db_path.resolve()),
        "symbols": requested_symbols,
        "configured_symbols": configured_symbols,
        "unmapped_symbols": unmapped_symbols,
        "start_ts": start_ts.isoformat(),
        "end_ts": end_ts.isoformat(),
        "run_gap_repair": bool(run_gap_repair),
        "derive_timeframes": list(normalized_derive_timeframes),
        "ingest_audits": audits,
        "derivations": derivations,
        "progress_rows": progress_rows,
        "symbol_results": symbol_results,
    }


def _backfill_progress_event(
    *,
    symbol: str,
    label: str,
    status: str,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "symbol": str(symbol).strip().upper(),
        "label": str(label).strip(),
        "status": str(status).strip(),
        "detail": detail or {},
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }


def rebuild_canonical_warehouse_surfaces(
    *,
    warehouse_root: Path,
    replay_db_path: Path,
    instruments: Sequence[str],
    start_ts: datetime,
    end_ts: datetime,
) -> dict[str, Any]:
    warehouse_root = warehouse_root.resolve()
    replay_db_path = replay_db_path.resolve()
    results: list[dict[str, Any]] = []
    for shard in _iter_quarter_shards(start_ts=start_ts, end_ts=end_ts):
        for symbol in sorted({str(item).strip().upper() for item in instruments}):
            raw_result = export_canonical_1m_partition(
                root_dir=warehouse_root,
                sqlite_path=replay_db_path,
                symbol=symbol,
                shard_id=shard["shard_id"],
                start_ts=shard["start_ts"],
                end_ts=shard["end_ts"],
            )
            derived_5m = materialize_derived_timeframe_partition(
                root_dir=warehouse_root,
                symbol=symbol,
                shard_id=shard["shard_id"],
                year=shard["year"],
                timeframe="5m",
                raw_partition_path=Path(raw_result["partition_path"]),
                raw_version=str(raw_result["raw_version"]),
            )
            results.append(
                {
                    "symbol": symbol,
                    "shard_id": shard["shard_id"],
                    "raw_row_count": int(raw_result["row_count"]),
                    "derived_5m_row_count": int(derived_5m["row_count"]),
                    "raw_partition_path": str(raw_result["partition_path"]),
                    "derived_5m_partition_path": str(derived_5m["partition_path"]),
                }
            )
    return {
        "mode": "executed_warehouse_rebuild",
        "warehouse_root": str(warehouse_root),
        "replay_db_path": str(replay_db_path),
        "results": results,
    }


def rematerialize_trade_artifacts(
    *,
    warehouse_root: Path,
    replay_db_path: Path,
    lane_symbol_map: dict[str, list[str]] | None = None,
    start_ts: datetime,
    end_ts: datetime,
    baseline_report_path: Path = DEFAULT_BASELINE_REPORT_PATH,
) -> dict[str, Any]:
    lane_map = _scoped_lane_symbol_map(
        lane_symbol_map=lane_symbol_map,
        requested_symbols=(),
    )
    runs: list[dict[str, Any]] = []
    for shard in _iter_quarter_shards(start_ts=start_ts, end_ts=end_ts):
        result = run_multi_symbol_warehouse_shard(
            root_dir=warehouse_root,
            sqlite_path=replay_db_path,
            symbol_lane_map=lane_map,
            shard_id=shard["shard_id"],
            start_ts=shard["start_ts"],
            end_ts=shard["end_ts"],
            baseline_report_path=baseline_report_path,
        )
        runs.append({"shard_id": shard["shard_id"], **result})
    return {
        "mode": "executed_trade_rematerialization",
        "warehouse_root": str(warehouse_root.resolve()),
        "replay_db_path": str(replay_db_path.resolve()),
        "lane_symbol_map": lane_map,
        "runs": runs,
    }


def _audit_replay_database(
    *,
    replay_db_path: Path,
    instruments: Sequence[str],
    policy: IntegritySourcePolicy,
    canonical_service: CanonicalMarketDataMaintenanceService,
) -> dict[str, Any]:
    conn = sqlite3.connect(replay_db_path)
    try:
        conn.row_factory = sqlite3.Row
        coverage_rows = _coverage_rows(conn=conn, instruments=instruments)
        monthly_density_rows = _monthly_density_rows(conn=conn, instruments=instruments)
        duplicate_rows = _duplicate_rows(conn=conn, instruments=instruments)
        session_audit = _session_audit(conn=conn, instruments=instruments)
        source_overlap_rows = _source_overlap_rows(conn=conn, instruments=instruments)
        five_minute_rows = _five_minute_surface_rows(conn=conn, instruments=instruments, policy=policy)
    finally:
        conn.close()

    canonical_coverage_rows: list[dict[str, Any]] = []
    for instrument in instruments:
        coverage = canonical_service.audit_coverage(symbol=instrument, timeframe="1m", data_source=policy.canonical_1m_source)
        canonical_coverage_rows.append(
            {
                "instrument": instrument,
                "timeframe": "1m",
                "data_source": policy.canonical_1m_source,
                "bar_count": coverage.bar_count,
                "earliest_ts": coverage.earliest,
                "latest_ts": coverage.latest,
                "gap_count": coverage.gap_count,
                "missing_bar_count": int(sum(gap.missing_minutes for gap in coverage.gaps)),
            }
        )

    latest = max((row["latest_ts"] for row in canonical_coverage_rows if row["latest_ts"]), default=None)
    return {
        "coverage_rows": coverage_rows,
        "monthly_density_rows": monthly_density_rows,
        "canonical_coverage_rows": canonical_coverage_rows,
        "duplicate_rows": duplicate_rows,
        "session_audit": session_audit,
        "source_overlap_rows": source_overlap_rows,
        "five_minute_surface_rows": five_minute_rows,
        "global_latest_canonical_1m_ts": latest,
    }


def _coverage_rows(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in instruments)
    rows = conn.execute(
        f"""
        select ticker as instrument, timeframe, data_source, min(timestamp) as earliest_ts, max(timestamp) as latest_ts, count(*) as bar_count
        from bars
        where ticker in ({placeholders})
        group by ticker, timeframe, data_source
        order by ticker, timeframe, data_source
        """,
        tuple(instruments),
    ).fetchall()
    return [dict(row) for row in rows]


def _monthly_density_rows(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in instruments)
    rows = conn.execute(
        f"""
        select ticker as instrument, timeframe, data_source, substr(timestamp,1,7) as month, count(*) as bar_count
        from bars
        where ticker in ({placeholders})
        group by ticker, timeframe, data_source, month
        order by ticker, timeframe, data_source, month
        """,
        tuple(instruments),
    ).fetchall()
    return [dict(row) for row in rows]


def _duplicate_rows(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in instruments)
    rows = conn.execute(
        f"""
        with dupes as (
          select ticker as instrument, timeframe, data_source, end_ts, count(*) as row_count
          from bars
          where ticker in ({placeholders})
          group by ticker, timeframe, data_source, end_ts
          having count(*) > 1
        )
        select
          instrument,
          timeframe,
          data_source,
          count(*) as duplicate_timestamp_count,
          sum(row_count - 1) as duplicate_bar_count
        from dupes
        group by instrument, timeframe, data_source
        order by instrument, timeframe, data_source
        """,
        tuple(instruments),
    ).fetchall()
    return [dict(row) for row in rows]


def _session_audit(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> dict[str, Any]:
    per_month_session_counts: list[dict[str, Any]] = []
    partial_session_rows: list[dict[str, Any]] = []
    gap_rows: list[dict[str, Any]] = []
    for instrument in instruments:
        canonical_rows = conn.execute(
            f"""
            with sessions as (
              select {RESEARCH_SESSION_EXPR} as session_date, count(*) as bar_count
              from bars
              where ticker = ?
                and timeframe = '1m'
                and data_source = ?
              group by session_date
            )
            select session_date, substr(session_date,1,7) as month, bar_count
            from sessions
            order by session_date
            """,
            (instrument, CANONICAL_1M_SOURCE),
        ).fetchall()
        canonical_session_dates = {str(row["session_date"]) for row in canonical_rows}
        month_counts = Counter(str(row["month"]) for row in canonical_rows)
        for month, count in sorted(month_counts.items()):
            per_month_session_counts.append(
                {
                    "instrument": instrument,
                    "month": month,
                    "session_count": count,
                }
            )
        if canonical_rows:
            counts = [int(row["bar_count"]) for row in canonical_rows]
            typical = median(counts)
            cutoff = max(1, int(typical * 0.8))
            for row in canonical_rows:
                if int(row["bar_count"]) < cutoff:
                    partial_session_rows.append(
                        {
                            "instrument": instrument,
                            "session_date": str(row["session_date"]),
                            "bar_count": int(row["bar_count"]),
                            "typical_session_bar_count": typical,
                        }
                    )
        all_source_rows = conn.execute(
            f"""
            with sessions as (
              select
                {RESEARCH_SESSION_EXPR} as session_date,
                group_concat(distinct data_source) as data_sources,
                count(*) as bar_count
              from bars
              where ticker = ?
                and timeframe = '1m'
              group by session_date
            )
            select session_date, data_sources, bar_count
            from sessions
            order by session_date
            """,
            (instrument,),
        ).fetchall()
        if not canonical_rows:
            gap_rows.append(
                {
                    "instrument": instrument,
                    "gap_start": None,
                    "gap_end": None,
                    "missing_session_count": 0,
                    "missing_reason": "no_canonical_sessions_loaded",
                    "evidence_data_sources": None,
                }
            )
            continue
        missing_sessions = [
            {
                "session_date": str(row["session_date"]),
                "data_sources": str(row["data_sources"] or ""),
            }
            for row in all_source_rows
            if str(row["session_date"]) not in canonical_session_dates
            and any(
                source.strip() and source.strip() != CANONICAL_1M_SOURCE
                for source in str(row["data_sources"] or "").split(",")
            )
        ]
        gap_rows.extend(
            {
                "instrument": instrument,
                "gap_start": gap["start"],
                "gap_end": gap["end"],
                "missing_session_count": gap["count"],
                "missing_reason": "session_gap",
                "evidence_data_sources": gap["data_sources"],
            }
            for gap in _compress_session_gap_rows(missing_sessions)
        )
    return {
        "per_month_session_counts": per_month_session_counts,
        "session_gap_rows": gap_rows,
        "partial_session_rows": partial_session_rows,
    }


def _compress_session_gap_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted((str(row["session_date"]), str(row.get("data_sources") or "")) for row in rows)
    if not ordered:
        return []
    compressed: list[dict[str, Any]] = []
    current_start = ordered[0][0]
    current_end = ordered[0][0]
    current_sources: set[str] = {ordered[0][1]} if ordered[0][1] else set()
    current_count = 1
    for session_date, data_sources in ordered[1:]:
        previous_date = datetime.fromisoformat(current_end)
        expected_next = (previous_date + timedelta(days=1)).date().isoformat()
        if session_date == expected_next:
            current_end = session_date
            current_count += 1
            if data_sources:
                current_sources.add(data_sources)
            continue
        compressed.append(
            {
                "start": current_start,
                "end": current_end,
                "count": current_count,
                "data_sources": ",".join(sorted(source for source in current_sources if source)) or None,
            }
        )
        current_start = session_date
        current_end = session_date
        current_sources = {data_sources} if data_sources else set()
        current_count = 1
    compressed.append(
        {
            "start": current_start,
            "end": current_end,
            "count": current_count,
            "data_sources": ",".join(sorted(source for source in current_sources if source)) or None,
        }
    )
    return compressed


def _source_overlap_rows(conn: sqlite3.Connection, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument in instruments:
        monthly_rows = conn.execute(
            """
            select
              ticker as instrument,
              timeframe,
              substr(timestamp,1,7) as month,
              count(*) as total_rows,
              count(distinct timestamp) as distinct_timestamps,
              count(*) - count(distinct timestamp) as duplicate_overlap_rows,
              group_concat(distinct data_source) as data_sources
            from bars
            where ticker = ?
            group by ticker, timeframe, month
            order by ticker, timeframe, month
            """,
            (instrument,),
        ).fetchall()
        rows.extend(dict(row) for row in monthly_rows)
    return rows


def _research_surface_overlap_rows(
    conn: sqlite3.Connection,
    *,
    instruments: Sequence[str],
    policy: IntegritySourcePolicy,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument in instruments:
        monthly_rows = conn.execute(
            """
            select
              ticker as instrument,
              timeframe,
              substr(timestamp,1,7) as month,
              count(*) as total_rows,
              count(distinct timestamp) as distinct_timestamps,
              count(*) - count(distinct timestamp) as duplicate_overlap_rows,
              group_concat(distinct data_source) as data_sources
            from bars
            where ticker = ?
              and (
                (timeframe = '1m' and data_source = ?)
                or (timeframe = '5m' and data_source in (?, ?))
                or (timeframe = '10m' and data_source = ?)
              )
            group by ticker, timeframe, month
            order by ticker, timeframe, month
            """,
            (
                instrument,
                policy.canonical_1m_source,
                policy.canonical_5m_source,
                policy.extended_5m_sources[0] if policy.extended_5m_sources else "__none__",
                policy.canonical_10m_source,
            ),
        ).fetchall()
        rows.extend(dict(row) for row in monthly_rows)
    return rows


def _five_minute_surface_rows(
    *,
    conn: sqlite3.Connection,
    instruments: Sequence[str],
    policy: IntegritySourcePolicy,
) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in instruments)
    rows = conn.execute(
        f"""
        select ticker as instrument, data_source, min(timestamp) as earliest_ts, max(timestamp) as latest_ts, count(*) as bar_count
        from bars
        where ticker in ({placeholders}) and timeframe = '5m'
        group by ticker, data_source
        order by ticker, data_source
        """,
        tuple(instruments),
    ).fetchall()
    payload: list[dict[str, Any]] = []
    for row in rows:
        data_source = str(row["data_source"])
        if data_source == policy.canonical_5m_source:
            trust = "canonical"
        elif data_source in policy.extended_5m_sources:
            trust = "extended"
        elif data_source in policy.forbidden_research_sources:
            trust = "forbidden"
        else:
            trust = "unknown"
        payload.append({**dict(row), "trust_classification": trust})
    return payload


def _audit_warehouse(*, warehouse_root: Path) -> dict[str, Any]:
    layout = build_warehouse_layout(warehouse_root)
    dataset_rows = {
        "raw_bars_1m": _warehouse_dataset_report(layout["raw_bars_1m"], parquet_name="bars.parquet", ts_col="bar_ts"),
        "derived_bars_5m": _warehouse_dataset_report(layout["derived_bars_5m"], parquet_name="bars.parquet", ts_col="bar_ts"),
        "lane_entries": _warehouse_dataset_report(layout["lane_entries"], parquet_name="entries.parquet", ts_col="entry_ts"),
        "lane_closed_trades": _warehouse_dataset_report(layout["lane_closed_trades"], parquet_name="closed_trades.parquet", ts_col="entry_ts"),
    }
    return {"dataset_reports": dataset_rows}


def _warehouse_dataset_report(root: Path, *, parquet_name: str, ts_col: str) -> dict[str, Any]:
    files = sorted(root.glob(f"symbol=*/year=*/shard_id=*/{parquet_name}"))
    coverage_rows: list[dict[str, Any]] = []
    overall_by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    duplicate_rows: list[dict[str, Any]] = []
    for parquet_path in files:
        parquet_file = pq.ParquetFile(parquet_path)
        if parquet_file.metadata.num_rows == 0:
            continue
        table = parquet_file.read(columns=["symbol", ts_col])
        symbol = str(table.column("symbol")[0].as_py()).upper()
        timestamps = [item.as_py() for item in table.column(ts_col) if item.as_py() is not None]
        if not timestamps:
            continue
        duplicate_count = len(timestamps) - len({item.isoformat() for item in timestamps})
        row = {
            "symbol": symbol,
            "partition": str(parquet_path.parent.relative_to(root)),
            "earliest_ts": timestamps[0].isoformat(),
            "latest_ts": timestamps[-1].isoformat(),
            "row_count": int(parquet_file.metadata.num_rows),
            "duplicate_timestamp_count": duplicate_count,
        }
        coverage_rows.append(row)
        overall_by_symbol[symbol].append(row)
        if duplicate_count > 0:
            duplicate_rows.append(
                {
                    "symbol": symbol,
                    "partition": row["partition"],
                    "duplicate_timestamp_count": duplicate_count,
                }
            )
    overall_rows = [
        {
            "symbol": symbol,
            "earliest_ts": min(item["earliest_ts"] for item in rows),
            "latest_ts": max(item["latest_ts"] for item in rows),
            "row_count": sum(int(item["row_count"]) for item in rows),
            "partition_count": len(rows),
            "duplicate_timestamp_count": sum(int(item["duplicate_timestamp_count"]) for item in rows),
        }
        for symbol, rows in sorted(overall_by_symbol.items())
    ]
    return {
        "root": str(root.resolve()),
        "file_count": len(files),
        "coverage_rows": coverage_rows,
        "overall_rows": overall_rows,
        "duplicate_rows": duplicate_rows,
    }


def _audit_trade_alignment(
    *,
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    lane_symbol_map: dict[str, list[str]],
) -> dict[str, Any]:
    canonical_by_symbol = {
        row["instrument"]: row
        for row in replay_audit["canonical_coverage_rows"]
    }
    lane_entries_by_symbol = {
        row["symbol"]: row
        for row in warehouse_audit["dataset_reports"]["lane_entries"]["overall_rows"]
    }
    lane_trades_by_symbol = {
        row["symbol"]: row
        for row in warehouse_audit["dataset_reports"]["lane_closed_trades"]["overall_rows"]
    }
    alignment_rows: list[dict[str, Any]] = []
    blocking_issues: list[str] = []
    for symbol, lanes in sorted(lane_symbol_map.items()):
        canonical = canonical_by_symbol.get(symbol)
        entries = lane_entries_by_symbol.get(symbol)
        trades = lane_trades_by_symbol.get(symbol)
        entry_aligned = bool(
            canonical
            and canonical.get("latest_ts")
            and _dataset_has_symbol_shard_for_timestamp(
                warehouse_audit["dataset_reports"]["lane_entries"],
                symbol=symbol,
                latest_ts=str(canonical["latest_ts"]),
            )
        )
        trade_aligned = bool(
            canonical
            and canonical.get("latest_ts")
            and _dataset_has_symbol_shard_for_timestamp(
                warehouse_audit["dataset_reports"]["lane_closed_trades"],
                symbol=symbol,
                latest_ts=str(canonical["latest_ts"]),
            )
        )
        row = {
            "symbol": symbol,
            "lane_count": len(lanes),
            "canonical_latest_ts": canonical["latest_ts"] if canonical else None,
            "lane_entries_latest_ts": entries["latest_ts"] if entries else None,
            "lane_closed_trades_latest_ts": trades["latest_ts"] if trades else None,
            "entries_aligned": entry_aligned,
            "closed_trades_aligned": trade_aligned,
        }
        if not entry_aligned or not trade_aligned:
            blocking_issues.append(
                f"{symbol} trade artifacts are misaligned with canonical market data "
                f"(entries_aligned={entry_aligned}, closed_trades_aligned={trade_aligned})."
            )
        alignment_rows.append(row)
    return {
        "symbol_rows": alignment_rows,
        "blocking_issues": blocking_issues,
        "analysis_allowed": not blocking_issues,
    }


def _scoped_lane_symbol_map(
    *,
    lane_symbol_map: dict[str, list[str]] | None,
    requested_symbols: Sequence[str],
) -> dict[str, list[str]]:
    lane_map = lane_symbol_map or {symbol: list(lanes) for symbol, lanes in DEFAULT_BASKET.items()}
    requested = {str(symbol).strip().upper() for symbol in requested_symbols if str(symbol).strip()}
    if not requested:
        return lane_map
    return {
        symbol: list(lanes)
        for symbol, lanes in lane_map.items()
        if symbol in requested
    }


def _dataset_has_symbol_shard_for_timestamp(
    dataset_report: dict[str, Any],
    *,
    symbol: str,
    latest_ts: str,
) -> bool:
    target = datetime.fromisoformat(str(latest_ts))
    expected_year = target.year
    expected_shard = f"{target.year}Q{((target.month - 1) // 3) + 1}"
    for row in dataset_report.get("coverage_rows", []):
        if str(row.get("symbol") or "").upper() != symbol.upper():
            continue
        partition = str(row.get("partition") or "")
        if f"year={expected_year}" in partition and f"shard_id={expected_shard}" in partition:
            return True
    return False


def _build_health_report(
    *,
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    trade_alignment: dict[str, Any],
    policy: IntegritySourcePolicy,
) -> dict[str, Any]:
    duplicates = {
        (row["instrument"], row["timeframe"], row["data_source"]): row
        for row in replay_audit["duplicate_rows"]
    }
    session_gaps = defaultdict(int)
    for row in replay_audit["session_audit"]["session_gap_rows"]:
        if row.get("gap_start") is None:
            continue
        session_gaps[str(row["instrument"])] += int(row["missing_session_count"])
    raw_warehouse_by_symbol = {
        row["symbol"]: row
        for row in warehouse_audit["dataset_reports"]["raw_bars_1m"]["overall_rows"]
    }
    derived_5m_by_symbol = {
        row["symbol"]: row
        for row in warehouse_audit["dataset_reports"]["derived_bars_5m"]["overall_rows"]
    }

    instrument_rows: list[dict[str, Any]] = []
    blocking_issues: list[str] = []
    for row in replay_audit["canonical_coverage_rows"]:
        instrument = str(row["instrument"])
        duplicate_row = duplicates.get((instrument, "1m", policy.canonical_1m_source))
        mixed_sources = [
            source_row
            for source_row in replay_audit["source_overlap_rows"]
            if source_row["instrument"] == instrument and int(source_row["duplicate_overlap_rows"] or 0) > 0
        ]
        raw_warehouse = raw_warehouse_by_symbol.get(instrument)
        derived_5m = derived_5m_by_symbol.get(instrument)
        latest_ts = row.get("latest_ts")
        raw_aligned = bool(raw_warehouse and latest_ts and raw_warehouse["latest_ts"] >= latest_ts)
        derived_aligned = bool(derived_5m and latest_ts and derived_5m["latest_ts"] >= latest_ts)
        warehouse_duplicate_count = int((raw_warehouse or {}).get("duplicate_timestamp_count") or 0) + int(
            (derived_5m or {}).get("duplicate_timestamp_count") or 0
        )
        missing_bars = int(row["missing_bar_count"])
        duplicate_bars = int(duplicate_row["duplicate_bar_count"]) if duplicate_row else 0
        missing_sessions = int(session_gaps.get(instrument, 0))
        status = "healthy"
        issues: list[str] = []
        if not row["latest_ts"]:
            status = "failed"
            issues.append("canonical_1m_missing")
        if duplicate_bars > 0:
            status = "failed"
            issues.append("duplicate_bars")
        if warehouse_duplicate_count > 0:
            status = "failed"
            issues.append("warehouse_duplicate_bars")
        if missing_sessions > 0:
            status = "failed"
            issues.append("missing_sessions")
        if mixed_sources:
            status = "failed"
            issues.append("mixed_source_overlap")
        if not raw_aligned:
            status = "failed"
            issues.append("warehouse_raw_1m_misaligned")
        if not derived_aligned:
            status = "failed"
            issues.append("warehouse_derived_5m_misaligned")
        instrument_rows.append(
            {
                "instrument": instrument,
                "latest_timestamp": latest_ts,
                "missing_bars": missing_bars,
                "missing_sessions": missing_sessions,
                "duplicate_bar_count": duplicate_bars,
                "warehouse_duplicate_bar_count": warehouse_duplicate_count,
                "source_consistency_ok": not mixed_sources,
                "warehouse_1m_aligned": raw_aligned,
                "warehouse_5m_aligned": derived_aligned,
                "trade_artifact_fresh": next(
                    (
                        symbol_row["closed_trades_aligned"]
                        for symbol_row in trade_alignment["symbol_rows"]
                        if symbol_row["symbol"] == instrument
                    ),
                    None,
                ),
                "status": status,
                "issues": issues,
            }
        )
        if status == "failed":
            blocking_issues.append(f"{instrument}: {', '.join(issues)}")

    if blocking_issues or not trade_alignment["analysis_allowed"]:
        overall = "failed"
    elif any(row["status"] == "warning" for row in instrument_rows):
        overall = "warning"
    else:
        overall = "healthy"
    return {
        "overall_status": overall,
        "can_assert_complete_and_reliable": overall == "healthy",
        "instrument_rows": instrument_rows,
        "blocking_issues": blocking_issues + list(trade_alignment["blocking_issues"]),
    }


def _build_repair_plan(
    *,
    replay_db_path: Path,
    warehouse_root: Path,
    provider_config_path: str | Path | None,
    instruments: Sequence[str],
    replay_audit: dict[str, Any],
    warehouse_audit: dict[str, Any],
    trade_alignment: dict[str, Any],
    start_date: str,
    latest_target: datetime,
) -> dict[str, Any]:
    canonical_rows = {row["instrument"]: row for row in replay_audit["canonical_coverage_rows"]}
    raw_rows = {row["symbol"]: row for row in warehouse_audit["dataset_reports"]["raw_bars_1m"]["overall_rows"]}
    missing_ranges: list[dict[str, Any]] = []
    for instrument in instruments:
        canonical = canonical_rows.get(instrument)
        raw = raw_rows.get(instrument)
        if canonical and canonical.get("latest_ts") and (raw is None or raw["latest_ts"] < canonical["latest_ts"]):
            missing_ranges.append(
                {
                    "dataset": "warehouse_raw_bars_1m",
                    "instrument": instrument,
                    "missing_start": raw["latest_ts"] if raw is not None else f"{start_date}T18:00:00-05:00",
                    "missing_end": canonical["latest_ts"],
                }
            )
    for row in trade_alignment["symbol_rows"]:
        if not row["entries_aligned"] or not row["closed_trades_aligned"]:
            missing_ranges.append(
                {
                    "dataset": "trade_artifacts",
                    "instrument": row["symbol"],
                    "missing_start": row["lane_closed_trades_latest_ts"] or row["lane_entries_latest_ts"],
                    "missing_end": row["canonical_latest_ts"],
                }
            )
    return {
        "missing_ranges": missing_ranges,
        "repair_commands": _build_backfill_commands(
            replay_db_path=replay_db_path,
            provider_config_override=provider_config_path,
            instruments=instruments,
            latest_target=latest_target,
            start_date=start_date,
        ),
        "warehouse_rebuild_commands": _build_warehouse_commands(
            replay_db_path=replay_db_path,
            warehouse_root=warehouse_root,
            latest_target=latest_target,
            mode="repair",
        ),
        "trade_rematerialization_commands": _build_trade_commands(
            replay_db_path=replay_db_path,
            warehouse_root=warehouse_root,
            latest_target=latest_target,
        ),
        "do_not_run_strategy_research_yet": True,
    }


def _build_daily_maintenance_plan(
    *,
    replay_db_path: Path,
    warehouse_root: Path,
    provider_config_path: str | Path | None,
    instruments: Sequence[str],
    replay_audit: dict[str, Any],
    latest_target: datetime,
) -> dict[str, Any]:
    per_instrument = []
    canonical_rows = {row["instrument"]: row for row in replay_audit["canonical_coverage_rows"]}
    for instrument in instruments:
        canonical = canonical_rows.get(instrument)
        if canonical and canonical["latest_ts"]:
            start_ts = (datetime.fromisoformat(str(canonical["latest_ts"])) + timedelta(minutes=1)).isoformat()
        else:
            start_ts = "2024-01-01T18:00:00-05:00"
        per_instrument.append(
            {
                "instrument": instrument,
                "next_incremental_start": start_ts,
                "target_end": latest_target.isoformat(),
            }
        )
    return {
        "mode_supported": ["incremental_update", "full_backfill", "dry_run_validation"],
        "per_instrument": per_instrument,
        "daily_commands": _build_backfill_commands(
            replay_db_path=replay_db_path,
            provider_config_override=provider_config_path,
            instruments=instruments,
            latest_target=latest_target,
            start_date=None,
            per_instrument_starts={row["instrument"]: row["next_incremental_start"] for row in per_instrument},
        ),
        "warehouse_commands": _build_warehouse_commands(
            replay_db_path=replay_db_path,
            warehouse_root=warehouse_root,
            latest_target=latest_target,
            mode="daily",
        ),
        "trade_commands": _build_trade_commands(
            replay_db_path=replay_db_path,
            warehouse_root=warehouse_root,
            latest_target=latest_target,
        ),
    }


def _build_backfill_commands(
    *,
    replay_db_path: Path,
    provider_config_override: str | Path | None,
    instruments: Sequence[str],
    latest_target: datetime,
    start_date: str | None,
    per_instrument_starts: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    commands = []
    for instrument in instruments:
        start_ts = (per_instrument_starts or {}).get(instrument)
        if start_ts is None:
            start_ts = f"{start_date}T18:00:00-05:00" if start_date is not None else "2024-01-01T18:00:00-05:00"
        commands.append(
            {
                "instrument": instrument,
                "command": (
                    "PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.main market-data-backfill "
                    f"--provider databento --symbol {instrument} --start {start_ts} "
                    f"--end {latest_target.isoformat()} --timeframe 1m "
                    f"--provider-config {provider_config_path(provider_config_override)} "
                    "--config config/base.yaml --config config/replay.yaml --skip-replay-preservation"
                ),
            }
        )
    return commands


def _build_warehouse_commands(
    *,
    replay_db_path: Path,
    warehouse_root: Path,
    latest_target: datetime,
    mode: str,
) -> list[dict[str, Any]]:
    return [
        {
            "mode": mode,
            "command": (
                "PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.research_market_data_integrity "
                f"--mode warehouse-rebuild --replay-db {replay_db_path} --warehouse-root {warehouse_root} "
                f"--end {latest_target.isoformat()}"
            ),
        }
    ]


def _build_trade_commands(
    *,
    replay_db_path: Path,
    warehouse_root: Path,
    latest_target: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "command": (
                "PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.research_market_data_integrity "
                f"--mode trade-rematerialize --replay-db {replay_db_path} --warehouse-root {warehouse_root} "
                f"--end {latest_target.isoformat()}"
            ),
        }
    ]


def _write_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    layout = build_report_layout(output_dir)
    summary_json_path = layout["reports"] / "research_market_data_integrity_summary.json"
    summary_md_path = layout["reports"] / "research_market_data_integrity_summary.md"
    coverage_csv = layout["reports"] / "research_market_data_coverage.csv"
    session_gap_csv = layout["reports"] / "research_market_data_session_gaps.csv"
    duplicate_csv = layout["reports"] / "research_market_data_duplicates.csv"
    health_json = layout["reports"] / "research_market_data_health.json"
    runtime_json = layout["reports"] / "research_market_data_audit_runtime.json"
    repair_plan_json = layout["reports"] / "research_market_data_repair_plan.json"
    daily_plan_json = layout["reports"] / "research_market_data_daily_plan.json"
    source_policy_json = layout["reports"] / "research_market_data_source_policy.json"
    alignment_json = layout["reports"] / "research_market_data_trade_alignment.json"

    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_md_path.write_text(_render_markdown(payload), encoding="utf-8")
    _write_csv(coverage_csv, payload["replay_audit"]["coverage_rows"])
    _write_csv(session_gap_csv, payload["replay_audit"]["session_audit"]["session_gap_rows"])
    _write_csv(duplicate_csv, payload["replay_audit"]["duplicate_rows"])
    health_json.write_text(json.dumps(payload["health"], indent=2, sort_keys=True), encoding="utf-8")
    runtime_json.write_text(json.dumps(payload["audit_runtime"], indent=2, sort_keys=True), encoding="utf-8")
    repair_plan_json.write_text(json.dumps(payload["repair_plan"], indent=2, sort_keys=True), encoding="utf-8")
    daily_plan_json.write_text(json.dumps(payload["daily_maintenance_plan"], indent=2, sort_keys=True), encoding="utf-8")
    source_policy_json.write_text(json.dumps(payload["policy"], indent=2, sort_keys=True), encoding="utf-8")
    alignment_json.write_text(json.dumps(payload["trade_alignment"], indent=2, sort_keys=True), encoding="utf-8")
    write_storage_manifest(
        layout["storage_manifest"],
        {
            "module": "research_market_data_integrity",
            "layout": {key: str(value) for key, value in layout.items()},
            "artifact_paths": {
                "summary_json_path": str(summary_json_path),
                "summary_md_path": str(summary_md_path),
                "coverage_csv": str(coverage_csv),
                "session_gap_csv": str(session_gap_csv),
                "duplicate_csv": str(duplicate_csv),
                "health_json": str(health_json),
                "runtime_json": str(runtime_json),
                "repair_plan_json": str(repair_plan_json),
                "daily_plan_json": str(daily_plan_json),
                "source_policy_json": str(source_policy_json),
                "alignment_json": str(alignment_json),
            },
        },
    )
    return {
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
        "coverage_csv": str(coverage_csv),
        "session_gap_csv": str(session_gap_csv),
        "duplicate_csv": str(duplicate_csv),
        "health_json": str(health_json),
        "runtime_json": str(runtime_json),
        "repair_plan_json": str(repair_plan_json),
        "daily_plan_json": str(daily_plan_json),
        "source_policy_json": str(source_policy_json),
        "alignment_json": str(alignment_json),
        "storage_manifest_path": str(layout["storage_manifest"]),
    }


def _write_fallback_artifacts(*, output_dir: Path, payload: dict[str, Any]) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_json_path = output_dir / "research_market_data_integrity_summary.partial.json"
    summary_md_path = output_dir / "research_market_data_integrity_summary.partial.md"
    summary_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    summary_md_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {
        "summary_json_path": str(summary_json_path),
        "summary_md_path": str(summary_md_path),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    health = payload["health"]
    audit_runtime = payload.get("audit_runtime", {})
    phase_rows = list(audit_runtime.get("phase_rows", []))
    slowest_phase = max(phase_rows, key=lambda row: float(row.get("duration_seconds") or 0), default=None)
    lines = [
        "# Research Market Data Integrity",
        "",
        "## Overall Health",
        f"- overall_status={health['overall_status']}",
        f"- can_assert_complete_and_reliable={health['can_assert_complete_and_reliable']}",
        f"- audit_status={audit_runtime.get('status')}",
        f"- audit_phase={audit_runtime.get('phase') or 'none'}",
        f"- audit_reason={audit_runtime.get('reason') or 'none'}",
        f"- instrument_count={payload['instrument_registry']['instrument_count']}",
        f"- requested_symbols={', '.join(payload['instrument_registry'].get('requested_symbols') or []) or 'all'}",
        "",
        "## Audit Runtime",
        f"- phase_count={len(phase_rows)}",
        f"- slowest_phase={slowest_phase['phase'] if slowest_phase else 'none'}",
        f"- slowest_phase_duration_seconds={slowest_phase['duration_seconds'] if slowest_phase else 0}",
        "",
        "## Blocking Issues",
    ]
    if health["blocking_issues"]:
        for issue in health["blocking_issues"]:
            lines.append(f"- {issue}")
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Source Policy",
            f"- canonical_1m_source={payload['policy']['canonical_1m_source']}",
            f"- canonical_5m_source={payload['policy']['canonical_5m_source']}",
            f"- extended_5m_sources={', '.join(payload['policy']['extended_5m_sources']) or 'none'}",
            f"- forbidden_research_sources={', '.join(payload['policy']['forbidden_research_sources'])}",
            "",
            "## Repair Plan",
            "- Do not rerun Asia Drift or cross-asset conclusions until overall health becomes `healthy` and trade alignment becomes `analysis_allowed=true`.",
            f"- repair_command_count={len(payload['repair_plan']['repair_commands'])}",
            f"- daily_command_count={len(payload['daily_maintenance_plan']['daily_commands'])}",
        ]
    )
    return "\n".join(lines) + "\n"


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _compress_date_ranges(values: Sequence[str]) -> list[dict[str, Any]]:
    if not values:
        return []
    compressed: list[dict[str, Any]] = []
    start = end = datetime.fromisoformat(values[0]).date()
    count = 1
    for raw in values[1:]:
        current = datetime.fromisoformat(raw).date()
        if current == end + timedelta(days=1):
            end = current
            count += 1
            continue
        compressed.append({"start": start.isoformat(), "end": end.isoformat(), "count": count})
        start = end = current
        count = 1
    compressed.append({"start": start.isoformat(), "end": end.isoformat(), "count": count})
    return compressed


def _iter_quarter_shards(*, start_ts: datetime, end_ts: datetime) -> list[dict[str, Any]]:
    cursor = datetime(start_ts.year, start_ts.month, 1, tzinfo=start_ts.tzinfo)
    shards: list[dict[str, Any]] = []
    while cursor <= end_ts:
        quarter = ((cursor.month - 1) // 3) + 1
        quarter_start_month = (quarter - 1) * 3 + 1
        quarter_start = datetime(cursor.year, quarter_start_month, 1, tzinfo=start_ts.tzinfo)
        if quarter == 4:
            next_quarter = datetime(cursor.year + 1, 1, 1, tzinfo=start_ts.tzinfo)
        else:
            next_quarter = datetime(cursor.year, quarter_start_month + 3, 1, tzinfo=start_ts.tzinfo)
        shard_start = max(start_ts, quarter_start)
        shard_end = min(end_ts, next_quarter - timedelta(minutes=1))
        if shard_start <= shard_end:
            shards.append(
                {
                    "shard_id": f"{quarter_start.year}Q{quarter}",
                    "start_ts": shard_start,
                    "end_ts": shard_end,
                    "year": quarter_start.year,
                }
            )
        cursor = next_quarter
    return shards


def _database_url_from_path(path: Path) -> str:
    return f"sqlite:///{path.resolve()}"
