"""Sunday-safe Databento historical seed for Phase-1 runtime candles.

This module writes HOT execution-core seed artifacts only. It does not confirm
realtime feed readiness, approve strategies, or enable broker submit paths.
"""

from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol, Sequence

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import (
    PHASE1_RUNTIME_TICKER_ORDER,
    PHASE1_RUNTIME_TIMEFRAMES,
)
from mgc_v05l.execution_core.track_b_runtime_candle_capture_cli import _load_databento_api_key
from mgc_v05l.market_data.databento_provider import DatabentoHistoricalHttpClient, UrllibDatabentoTransport

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_INTRADAY_BACKFILL_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data_intraday_backfill"
DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "phase1_databento_historical_seed"
DEFAULT_DATABENTO_BASE_URL = "https://hist.databento.com/v0"
DEFAULT_DATABENTO_DATASET = "GLBX.MDP3"
DEFAULT_LOOKBACK_DAYS = 30
SOURCE_ID = "DATABENTO_HISTORICAL_SEED"


class HistoricalSeedClient(Protocol):
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
        """Fetch historical Databento rows as decoded JSON records."""


@dataclass(frozen=True)
class Phase1HistoricalSeedConfig:
    repo_root: Path = REPO_ROOT
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    intraday_backfill_root: Path = DEFAULT_INTRADAY_BACKFILL_ROOT
    report_dir: Path = DEFAULT_REPORT_DIR
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    lookback_days: int = DEFAULT_LOOKBACK_DAYS
    dataset: str = DEFAULT_DATABENTO_DATASET
    schema_name: str = "ohlcv-1m"
    stype_in: str = "continuous"
    stype_out: str = "instrument_id"
    base_url: str = DEFAULT_DATABENTO_BASE_URL
    env_file: Path | None = None
    now: datetime | None = None
    limit: int | None = None


@dataclass(frozen=True)
class Phase1HistoricalSeedResult:
    report: dict[str, Any]
    artifacts_written: list[Path]


def build_phase1_databento_historical_seed(
    *,
    config: Phase1HistoricalSeedConfig,
    client: HistoricalSeedClient | None = None,
    write_artifacts: bool = True,
) -> Phase1HistoricalSeedResult:
    now = _coerce_now(config.now)
    end = now.replace(second=0, microsecond=0)
    start = end - timedelta(days=max(int(config.lookback_days), 1))
    api_key, credential_status, credential_source = _load_databento_api_key(config.env_file)
    symbols = tuple(str(symbol).upper() for symbol in config.symbols)
    artifacts_written: list[Path] = []

    if not api_key and client is None:
        report = _base_report(
            config=config,
            now=now,
            start=start,
            end=end,
            symbols=symbols,
            credential_status=credential_status,
            credential_source=credential_source,
            final_classification="DATABENTO_HISTORICAL_SEED_BLOCKED",
            primary_blocker="DATABENTO_API_KEY_MISSING",
        )
        if write_artifacts:
            _write_report(config=config, report=report)
        return Phase1HistoricalSeedResult(report=report, artifacts_written=[])

    seed_client = client or DatabentoHistoricalHttpClient(
        api_key=api_key,
        base_url=config.base_url,
        transport=UrllibDatabentoTransport(timeout_seconds=60),
    )

    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        try:
            records, actual_start, actual_end, provider_available_end, available_end_retry_used = _fetch_seed_records(
                client=seed_client,
                config=config,
                symbol=symbol,
                start=start,
                end=end,
            )
        except Exception as exc:  # noqa: BLE001 - provider failures become explicit fail-closed rows.
            rows.append(
                {
                    "symbol": symbol,
                    "requested_symbol": _continuous_symbol(symbol),
                    "historical_seed_ready": False,
                    "block_reason": "DATABENTO_HISTORICAL_FETCH_FAILED",
                    "detail": str(exc),
                    "artifacts": [],
                    "can_submit": False,
                    "live_money_eligible": False,
                }
            )
            continue
        one_minute = _normalize_1m_records(records)
        timeframe_payloads = _timeframe_payloads(
            config=config,
            symbol=symbol,
            now=now,
            start=actual_start,
            end=actual_end,
            one_minute=one_minute,
        )
        symbol_artifacts: list[str] = []
        if write_artifacts:
            for timeframe, payload in timeframe_payloads.items():
                path = _runtime_candle_path(config=config, symbol=symbol, timeframe=timeframe)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                artifacts_written.append(path)
                symbol_artifacts.append(str(path))
                intraday_payload = {
                    **payload,
                    "source": "RECOVERED_PHASE1_1M",
                    "source_id": f"RECOVERED_PHASE1_1M_{symbol.lower()}",
                    "anchor_recovery_backfill": True,
                    "phase1_current_day_backfill": True,
                    "retention_policy": "HISTORICAL_SEED_REQUEST_WINDOW",
                    "source_historical_seed_root": str(_resolve_path(config.repo_root, config.runtime_candle_root)),
                    "can_submit": False,
                    "paper_trade_allowed": False,
                    "live_money_eligible": False,
                }
                intraday_path = _intraday_backfill_path(config=config, symbol=symbol, timeframe=timeframe)
                intraday_path.parent.mkdir(parents=True, exist_ok=True)
                intraday_path.write_text(json.dumps(intraday_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
                artifacts_written.append(intraday_path)
                symbol_artifacts.append(str(intraday_path))
        rows.append(
            {
                "symbol": symbol,
                "requested_symbol": _continuous_symbol(symbol),
                "historical_seed_ready": bool(one_minute),
                "block_reason": "READY" if one_minute else "DATABENTO_HISTORICAL_BARS_MISSING",
                "timeframes_written": list(timeframe_payloads.keys()) if one_minute else [],
                "bar_counts": {timeframe: payload["bar_count"] for timeframe, payload in timeframe_payloads.items()},
                "last_completed_bar_ts": _last_bar_ts(one_minute),
                "provider_available_end": None if provider_available_end is None else provider_available_end.isoformat(),
                "available_end_retry_used": available_end_retry_used,
                "actual_start_ts": actual_start.isoformat(),
                "actual_end_ts": actual_end.isoformat(),
                "artifacts": symbol_artifacts,
                "can_submit": False,
                "live_money_eligible": False,
            }
        )

    final_classification = (
        "SUNDAY_HISTORICAL_SEED_READY"
        if rows and all(bool(row.get("historical_seed_ready")) for row in rows)
        else "SUNDAY_HISTORICAL_SEED_PARTIAL_OR_BLOCKED"
    )
    report = _base_report(
        config=config,
        now=now,
        start=start,
        end=end,
        symbols=symbols,
        credential_status=credential_status,
        credential_source=credential_source,
        final_classification=final_classification,
        primary_blocker=None if final_classification == "SUNDAY_HISTORICAL_SEED_READY" else "ONE_OR_MORE_SYMBOLS_NOT_SEEDED",
    )
    report.update(
        {
            "row_count": len(rows),
            "historical_seed_ready_count": sum(1 for row in rows if row.get("historical_seed_ready")),
            "realtime_feed_not_available_classification": "REALTIME_FEED_NOT_AVAILABLE_BY_SESSION",
            "paper_watch_classification": "NOT_READY_FOR_PAPER_WATCH_UNTIL_MONDAY_LIVE_PREFLIGHT",
            "can_submit_count": 0,
            "live_money_eligible_count": 0,
            "rows": rows,
        }
    )
    if write_artifacts:
        _write_report(config=config, report=report)
    return Phase1HistoricalSeedResult(report=report, artifacts_written=artifacts_written)


def _fetch_seed_records(
    *,
    client: HistoricalSeedClient,
    config: Phase1HistoricalSeedConfig,
    symbol: str,
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], datetime, datetime, datetime | None, bool]:
    try:
        records = _fetch_range(client=client, config=config, symbol=symbol, start=start, end=end)
        return records, start, end, None, False
    except Exception as exc:
        provider_available_end = _provider_available_end_from_error(exc)
        if provider_available_end is None or provider_available_end <= start:
            raise
        retry_end = provider_available_end.replace(second=0, microsecond=0)
        retry_start = retry_end - timedelta(days=max(int(config.lookback_days), 1))
        records = _fetch_range(client=client, config=config, symbol=symbol, start=retry_start, end=retry_end)
        return records, retry_start, retry_end, retry_end, True


def _fetch_range(
    *,
    client: HistoricalSeedClient,
    config: Phase1HistoricalSeedConfig,
    symbol: str,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    return client.get_range_json_lines(
        dataset=config.dataset,
        request_symbol=_continuous_symbol(symbol),
        schema_name=config.schema_name,
        start=start,
        end=end,
        stype_in=config.stype_in,
        stype_out=config.stype_out,
        encoding="json",
        compression="none",
        pretty_px=True,
        pretty_ts=True,
        map_symbols=True,
        limit=config.limit,
    )


def _timeframe_payloads(
    *,
    config: Phase1HistoricalSeedConfig,
    symbol: str,
    now: datetime,
    start: datetime,
    end: datetime,
    one_minute: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    if not one_minute:
        return {}
    derived: dict[str, list[dict[str, Any]]] = {"1m": one_minute}
    if "3m" in PHASE1_RUNTIME_TIMEFRAMES:
        derived["3m"] = _aggregate_bars(one_minute, minutes=3)
    derived["5m"] = _aggregate_bars(one_minute, minutes=5)
    return {
        timeframe: _seed_payload(
            config=config,
            symbol=symbol,
            timeframe=timeframe,
            now=now,
            start=start,
            end=end,
            bars=bars,
        )
        for timeframe, bars in derived.items()
        if timeframe in PHASE1_RUNTIME_TIMEFRAMES and bars
    }


def _seed_payload(
    *,
    config: Phase1HistoricalSeedConfig,
    symbol: str,
    timeframe: str,
    now: datetime,
    start: datetime,
    end: datetime,
    bars: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "source": SOURCE_ID,
        "source_id": SOURCE_ID,
        "generated_at": now.isoformat(),
        "symbol": symbol,
        "instrument": symbol,
        "root": symbol,
        "timeframe": timeframe,
        "dataset": config.dataset,
        "request_symbol": _continuous_symbol(symbol),
        "schema": config.schema_name,
        "start_ts": start.isoformat(),
        "end_ts": end.isoformat(),
        "bar_count": len(bars),
        "last_completed_bar_ts": _last_bar_ts(bars),
        "historical_seed_ready": bool(bars),
        "realtime_feed_confirmed": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "completed_candles_only": True,
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "bars": bars,
    }


def _normalize_1m_records(records: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    bars: list[dict[str, Any]] = []
    for record in records:
        timestamp = _record_timestamp(record)
        if timestamp is None:
            continue
        try:
            open_px = float(record.get("open"))
            high_px = float(record.get("high"))
            low_px = float(record.get("low"))
            close_px = float(record.get("close"))
        except (TypeError, ValueError):
            continue
        volume = record.get("volume", 0)
        try:
            volume_value: int | float = int(volume)
        except (TypeError, ValueError):
            try:
                volume_value = float(volume)
            except (TypeError, ValueError):
                volume_value = 0
        bars.append(
            {
                "bar_start": (timestamp - timedelta(minutes=1)).isoformat(),
                "bar_end": timestamp.isoformat(),
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": volume_value,
                "completed": True,
            }
        )
    return sorted(bars, key=lambda bar: str(bar["bar_end"]))


def _aggregate_bars(one_minute: list[dict[str, Any]], *, minutes: int) -> list[dict[str, Any]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for bar in one_minute:
        end = _parse_datetime(bar.get("bar_end"))
        if end is None:
            continue
        epoch_minutes = int(end.timestamp() // 60)
        bucket_epoch_minutes = ((epoch_minutes + minutes - 1) // minutes) * minutes
        bucket_end = datetime.fromtimestamp(bucket_epoch_minutes * 60, tz=timezone.utc)
        grouped.setdefault(bucket_end, []).append(bar)
    aggregates: list[dict[str, Any]] = []
    for bucket_end in sorted(grouped):
        bars = sorted(grouped[bucket_end], key=lambda bar: str(bar["bar_end"]))
        if len(bars) != minutes:
            continue
        aggregates.append(
            {
                "bar_start": bars[0]["bar_start"],
                "bar_end": bucket_end.isoformat(),
                "open": bars[0]["open"],
                "high": max(float(bar["high"]) for bar in bars),
                "low": min(float(bar["low"]) for bar in bars),
                "close": bars[-1]["close"],
                "volume": sum(float(bar.get("volume") or 0.0) for bar in bars),
                "completed": True,
                "source_bar_count": len(bars),
            }
        )
    return aggregates


def _record_timestamp(record: dict[str, Any]) -> datetime | None:
    for key in ("ts_event", "ts_recv", "timestamp", "bar_end", "time"):
        parsed = _parse_datetime(record.get(key))
        if parsed is not None:
            return parsed
    nested = record.get("hd")
    if isinstance(nested, dict):
        return _parse_datetime(nested.get("ts_event") or nested.get("ts_recv"))
    return None


def _provider_available_end_from_error(exc: Exception) -> datetime | None:
    match = re.search(r"available up to '([^']+)'", str(exc))
    if not match:
        return None
    return _parse_datetime(match.group(1))


def _base_report(
    *,
    config: Phase1HistoricalSeedConfig,
    now: datetime,
    start: datetime,
    end: datetime,
    symbols: tuple[str, ...],
    credential_status: str,
    credential_source: str | None,
    final_classification: str,
    primary_blocker: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": "phase1_databento_historical_seed_v1",
        "generated_at": now.isoformat(),
        "repo_root": str(Path(config.repo_root)),
        "source": SOURCE_ID,
        "dataset": config.dataset,
        "schema": config.schema_name,
        "lookback_days": int(config.lookback_days),
        "request_window_start": start.isoformat(),
        "request_window_end": end.isoformat(),
        "symbols": list(symbols),
        "phase1_symbol_count": len(symbols),
        "historical_seed_ready": final_classification == "SUNDAY_HISTORICAL_SEED_READY",
        "realtime_feed_confirmed": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "paper_trade_allowed": False,
        "can_submit": False,
        "live_money_eligible": False,
        "credential_status": credential_status,
        "credential_source": credential_source,
        "final_classification": final_classification,
        "primary_blocker": primary_blocker,
    }


def _write_report(*, config: Phase1HistoricalSeedConfig, report: dict[str, Any]) -> None:
    output_dir = Path(config.report_dir)
    if not output_dir.is_absolute():
        output_dir = Path(config.repo_root) / output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_databento_historical_seed_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _runtime_candle_path(*, config: Phase1HistoricalSeedConfig, symbol: str, timeframe: str) -> Path:
    return _resolve_path(config.repo_root, config.runtime_candle_root) / symbol / timeframe / "latest_runtime_candles.json"


def _intraday_backfill_path(*, config: Phase1HistoricalSeedConfig, symbol: str, timeframe: str) -> Path:
    return _resolve_path(config.repo_root, config.intraday_backfill_root) / symbol / timeframe / "latest_runtime_candles.json"


def _resolve_path(repo_root: Path, value: Path) -> Path:
    return value if value.is_absolute() else Path(repo_root) / value


def _continuous_symbol(symbol: str) -> str:
    return f"{symbol}.v.0"


def _last_bar_ts(bars: list[dict[str, Any]]) -> str | None:
    if not bars:
        return None
    return str(bars[-1].get("bar_end") or "")


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed Phase-1 HOT runtime candles from bounded Databento historical 1m bars.")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--runtime-candle-root", default=str(DEFAULT_RUNTIME_CANDLE_ROOT))
    parser.add_argument("--intraday-backfill-root", default=str(DEFAULT_INTRADAY_BACKFILL_ROOT))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--dataset", default=DEFAULT_DATABENTO_DATASET)
    parser.add_argument("--base-url", default=DEFAULT_DATABENTO_BASE_URL)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip())
    result = build_phase1_databento_historical_seed(
        config=Phase1HistoricalSeedConfig(
            repo_root=Path(args.repo_root),
            runtime_candle_root=Path(args.runtime_candle_root),
            intraday_backfill_root=Path(args.intraday_backfill_root),
            report_dir=Path(args.report_dir),
            symbols=symbols,
            lookback_days=args.lookback_days,
            dataset=args.dataset,
            base_url=args.base_url,
            env_file=args.env_file,
            limit=args.limit,
        ),
        write_artifacts=not bool(args.no_write),
    )
    print(
        json.dumps(
            {
                "final_classification": result.report["final_classification"],
                "phase1_symbol_count": result.report.get("phase1_symbol_count"),
                "historical_seed_ready_count": result.report.get("historical_seed_ready_count", 0),
                "realtime_feed_confirmed": result.report["realtime_feed_confirmed"],
                "can_submit": result.report["can_submit"],
                "live_money_eligible": result.report["live_money_eligible"],
            },
            sort_keys=True,
        )
    )
    return 0 if result.report["final_classification"] == "SUNDAY_HISTORICAL_SEED_READY" else 2


if __name__ == "__main__":
    raise SystemExit(main())
