"""Phase-1 Databento Live runtime candle producer.

This module adapts the existing no-broker Databento Live feed boundary into the
Phase-1 HOT runtime candle artifact contract consumed by paper preflight.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.app.session_phase_labels import label_session_phase, session_restriction_matches_timestamp
from mgc_v05l.execution_core.phase1_databento_historical_seed import _aggregate_bars
from mgc_v05l.execution_core.phase1_runtime_ticker_registry import (
    PHASE1_RUNTIME_TICKER_ORDER,
    PHASE1_RUNTIME_TIMEFRAMES,
)
from mgc_v05l.execution_core.track_b_databento_live_runtime_feed import (
    DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT,
    TrackBDatabentoLiveFeedConfig,
    TrackBDatabentoLiveFeedResult,
    run_track_b_databento_live_runtime_feed,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "phase1_databento_live_runtime_candles"
DEFAULT_DATABENTO_DATASET = "GLBX.MDP3"
SOURCE_ID = "DATABENTO_REALTIME_PHASE1"
FRESHNESS_SECONDS_BY_TIMEFRAME = {
    "1m": 180.0,
    "3m": 360.0,
    "5m": 600.0,
}


@dataclass(frozen=True)
class Phase1DatabentoLiveRuntimeCandlesConfig:
    repo_root: Path = REPO_ROOT
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    report_dir: Path = DEFAULT_REPORT_DIR
    legacy_live_output_root: Path = DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    dataset: str = DEFAULT_DATABENTO_DATASET
    schema: str = "ohlcv-1m"
    stype_in: str = "continuous"
    stype_out: str = "instrument_id"
    env_file: Path | None = None
    source_id: str = SOURCE_ID
    max_bars: int = 90
    min_bars: int = 8
    max_records: int = 90
    max_seconds_per_symbol: float = 75.0
    max_workers: int = 10
    max_latest_1m_age_seconds: int = 90
    max_completed_5m_age_seconds: int = 360
    now: datetime | None = None


@dataclass(frozen=True)
class Phase1DatabentoLiveRuntimeCandlesResult:
    report: dict[str, Any]
    artifacts_written: list[Path]


LiveRunner = Callable[[TrackBDatabentoLiveFeedConfig], TrackBDatabentoLiveFeedResult]


def build_phase1_databento_live_runtime_candles(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    live_runner: LiveRunner | None = None,
    write_artifacts: bool = True,
) -> Phase1DatabentoLiveRuntimeCandlesResult:
    now = _coerce_now(config.now)
    runner = live_runner or _default_live_runner
    symbols = tuple(str(symbol).strip().upper() for symbol in config.symbols if str(symbol).strip())
    rows_by_symbol: dict[str, dict[str, Any]] = {}
    artifacts_written: list[Path] = []

    def produce_symbol(symbol: str) -> tuple[str, TrackBDatabentoLiveFeedResult | None, str | None]:
        try:
            return symbol, runner(_live_config_for_symbol(config=config, symbol=symbol)), None
        except Exception as exc:  # noqa: BLE001 - provider/runtime errors become fail-closed rows.
            return symbol, None, str(exc)

    max_workers = max(max(len(symbols), 1), int(config.max_workers))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(produce_symbol, symbol): symbol for symbol in symbols}
        for future in concurrent.futures.as_completed(futures):
            symbol, live_result, error = future.result()
            if error is not None or live_result is None:
                rows_by_symbol[symbol] = _blocked_row(symbol=symbol, reason="DATABENTO_LIVE_PRODUCER_ERROR", detail=error or "")
                continue
            rows_by_symbol[symbol], written = _row_and_artifacts_for_live_result(
                config=config,
                symbol=symbol,
                now=now,
                live_result=live_result,
                write_artifacts=write_artifacts,
            )
            artifacts_written.extend(written)

    rows = [rows_by_symbol[symbol] for symbol in symbols if symbol in rows_by_symbol]
    confirmed_count = sum(1 for row in rows if row.get("realtime_feed_confirmed") is True)
    report = {
        "schema_version": "phase1_databento_live_runtime_candles_v1",
        "generated_at": now.isoformat(),
        "repo_root": str(Path(config.repo_root)),
        "source": SOURCE_ID,
        "source_id": config.source_id,
        "dataset": config.dataset,
        "schema": config.schema,
        "symbols": list(symbols),
        "phase1_symbol_count": len(symbols),
        "realtime_feed_confirmed_count": confirmed_count,
        "historical_seed_ready": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "can_submit": False,
        "live_money_eligible": False,
        "final_classification": "PHASE1_REALTIME_MARKET_DATA_CONFIRMED"
        if rows and confirmed_count == len(rows)
        else "PHASE1_REALTIME_MARKET_DATA_PARTIAL_OR_BLOCKED",
        "rows": rows,
    }
    if write_artifacts:
        _write_report(config=config, report=report)
    return Phase1DatabentoLiveRuntimeCandlesResult(report=report, artifacts_written=artifacts_written)


def _default_live_runner(config: TrackBDatabentoLiveFeedConfig) -> TrackBDatabentoLiveFeedResult:
    return run_track_b_databento_live_runtime_feed(config=config)


def _row_and_artifacts_for_live_result(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    symbol: str,
    now: datetime,
    live_result: TrackBDatabentoLiveFeedResult,
    write_artifacts: bool,
) -> tuple[dict[str, Any], list[Path]]:
    live_event = live_result.live_1m_candles_event or _fallback_legacy_live_event(config=config, symbol=symbol)
    live_connected = live_result.report.get("live_feed_connected") is True or live_event is not None
    if not live_connected:
        return (
            _blocked_row(
                symbol=symbol,
                reason=str(live_result.report.get("primary_blocker") or "DATABENTO_LIVE_NOT_CONNECTED"),
                detail=str(live_result.report.get("live_runtime_feed_verdict") or ""),
                live_report_path=str(live_result.report_json),
            ),
            [],
        )
    candles_1m = _normalize_live_1m_candles(live_event or {})
    timeframe_bars = _timeframe_bars(candles_1m)
    payloads = {
        timeframe: _runtime_payload(
            config=config,
            symbol=symbol,
            timeframe=timeframe,
            generated_at=_parse_datetime(live_result.report.get("generated_at")) or now,
            bars=bars,
            live_result=live_result,
            live_connected=live_connected,
        )
        for timeframe, bars in timeframe_bars.items()
        if timeframe in PHASE1_RUNTIME_TIMEFRAMES
    }
    incomplete = [timeframe for timeframe in PHASE1_RUNTIME_TIMEFRAMES if not payloads.get(timeframe)]
    not_confirmed = [
        timeframe
        for timeframe, payload in payloads.items()
        if payload.get("realtime_feed_confirmed") is not True
    ]
    written: list[Path] = []
    if write_artifacts:
        for timeframe, payload in payloads.items():
            path = _runtime_candle_path(config=config, symbol=symbol, timeframe=timeframe)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            written.append(path)
    return (
        {
            "symbol": symbol,
            "requested_symbol": _continuous_symbol(symbol),
            "live_feed_connected": True,
            "runtime_candles_written": sorted(payloads),
            "bar_counts": {timeframe: payload["bar_count"] for timeframe, payload in payloads.items()},
            "latest_completed_bar_ts": {
                timeframe: payload.get("last_completed_bar_ts") for timeframe, payload in payloads.items()
            },
            "realtime_feed_confirmed": bool(payloads) and not incomplete and not not_confirmed,
            "block_reason": "READY" if bool(payloads) and not incomplete and not not_confirmed else "LIVE_CANDLES_NOT_READY",
            "incomplete_timeframes": incomplete,
            "not_confirmed_timeframes": not_confirmed,
            "live_report_path": str(live_result.report_json),
            "can_submit": False,
            "live_money_eligible": False,
        },
        written,
    )


def _live_config_for_symbol(
    *, config: Phase1DatabentoLiveRuntimeCandlesConfig, symbol: str
) -> TrackBDatabentoLiveFeedConfig:
    return TrackBDatabentoLiveFeedConfig(
        contract_key=f"{symbol}-PHASE1",
        instrument_family=symbol,
        local_symbol=symbol,
        databento_continuous_symbol=_continuous_symbol(symbol),
        dataset=config.dataset,
        schema=config.schema,
        stype_in=config.stype_in,
        stype_out=config.stype_out,
        max_bars=config.max_bars,
        min_bars=config.min_bars,
        max_records=config.max_records,
        max_seconds=config.max_seconds_per_symbol,
        max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        env_file=config.env_file,
        output_root=_resolve_path(config.repo_root, config.legacy_live_output_root),
        source_id=f"{config.source_id}_{symbol.lower()}",
    )


def _normalize_live_1m_candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("candles") or payload.get("candle_history") or []
    bars: list[dict[str, Any]] = []
    if not isinstance(raw, list):
        return bars
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        end = _parse_datetime(row.get("candle_timestamp") or row.get("bar_end"))
        if end is None:
            continue
        try:
            open_px = float(row.get("open"))
            high_px = float(row.get("high"))
            low_px = float(row.get("low"))
            close_px = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        try:
            volume = float(row.get("volume") or 0.0)
        except (TypeError, ValueError):
            volume = 0.0
        bars.append(
            {
                "bar_start": (end - timedelta(minutes=1)).isoformat(),
                "bar_end": end.isoformat(),
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": volume,
                "completed": True,
            }
        )
    return sorted(bars, key=lambda item: str(item["bar_end"]))


def _fallback_legacy_live_event(
    *, config: Phase1DatabentoLiveRuntimeCandlesConfig, symbol: str
) -> dict[str, Any] | None:
    path = _resolve_path(config.repo_root, config.legacy_live_output_root) / f"latest_live_{symbol.lower()}_1m_candles.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("source_id") or "").strip() != f"{config.source_id}_{symbol.lower()}":
        return None
    if str(payload.get("candle_source_mode") or "") != "DATABENTO_LIVE_RUNTIME_FEED":
        return None
    if payload.get("fresh_for_execution") is not True:
        return None
    return payload


def _timeframe_bars(one_minute: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    if not one_minute:
        return {}
    return {
        "1m": one_minute,
        "3m": _aggregate_bars(one_minute, minutes=3),
        "5m": _aggregate_bars(one_minute, minutes=5),
    }


def _runtime_payload(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    symbol: str,
    timeframe: str,
    generated_at: datetime,
    bars: list[dict[str, Any]],
    live_result: TrackBDatabentoLiveFeedResult,
    live_connected: bool,
) -> dict[str, Any]:
    generated_at = _coerce_now(generated_at)
    last_completed = _parse_datetime(bars[-1].get("bar_end")) if bars else None
    freshness_seconds = FRESHNESS_SECONDS_BY_TIMEFRAME[timeframe]
    age_seconds = None if last_completed is None else max(0.0, (generated_at - last_completed).total_seconds())
    min_bars = _min_bars_for_timeframe(config=config, timeframe=timeframe)
    fresh = age_seconds is not None and age_seconds <= freshness_seconds
    complete = len(bars) >= min_bars
    realtime_confirmed = live_connected and fresh and complete
    return {
        "source": SOURCE_ID,
        "source_id": f"{config.source_id}_{symbol.lower()}",
        "generated_at": generated_at.isoformat(),
        "symbol": symbol,
        "instrument": symbol,
        "root": symbol,
        "timeframe": timeframe,
        "dataset": config.dataset,
        "request_symbol": _continuous_symbol(symbol),
        "schema": config.schema,
        "bar_count": len(bars),
        "first_bar_ts": bars[0].get("bar_end") if bars else None,
        "last_completed_bar_ts": None if last_completed is None else last_completed.isoformat(),
        "freshness_seconds": freshness_seconds,
        "latest_bar_age_seconds": age_seconds,
        "minimum_bar_count": min_bars,
        "historical_seed_ready": False,
        "realtime_feed_confirmed": realtime_confirmed,
        "realtime_feed_block_reason": "READY"
        if realtime_confirmed
        else _realtime_block_reason(fresh=fresh, complete=complete, connected=live_connected),
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "completed_candles_only": True,
        "sunday_session_label": label_session_phase(generated_at),
        "sunday_globex_session_supported": session_restriction_matches_timestamp(generated_at, "ASIA"),
        "source_live_report_path": str(live_result.report_json),
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "bars": bars,
    }


def _realtime_block_reason(*, fresh: bool, complete: bool, connected: bool) -> str:
    if not connected:
        return "LIVE_FEED_NOT_CONNECTED"
    if not complete:
        return "INSUFFICIENT_LIVE_BARS"
    if not fresh:
        return "LIVE_BARS_STALE"
    return "REALTIME_FEED_NOT_CONFIRMED"


def _min_bars_for_timeframe(*, config: Phase1DatabentoLiveRuntimeCandlesConfig, timeframe: str) -> int:
    if timeframe == "1m":
        return max(int(config.min_bars), 1)
    if timeframe == "3m":
        return max(int(config.min_bars) // 3, 1)
    if timeframe == "5m":
        return max(int(config.min_bars) // 5, 1)
    return 1


def _blocked_row(*, symbol: str, reason: str, detail: str = "", live_report_path: str | None = None) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "requested_symbol": _continuous_symbol(symbol),
        "live_feed_connected": False,
        "runtime_candles_written": [],
        "realtime_feed_confirmed": False,
        "block_reason": reason,
        "detail": detail,
        "live_report_path": live_report_path,
        "can_submit": False,
        "live_money_eligible": False,
    }


def _write_report(*, config: Phase1DatabentoLiveRuntimeCandlesConfig, report: dict[str, Any]) -> None:
    output_dir = _resolve_path(config.repo_root, config.report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_databento_live_runtime_candles_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _runtime_candle_path(*, config: Phase1DatabentoLiveRuntimeCandlesConfig, symbol: str, timeframe: str) -> Path:
    return _resolve_path(config.repo_root, config.runtime_candle_root) / symbol / timeframe / "latest_runtime_candles.json"


def _resolve_path(repo_root: Path, value: Path) -> Path:
    return value if value.is_absolute() else Path(repo_root) / value


def _continuous_symbol(symbol: str) -> str:
    return f"{symbol}.v.0"


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
    parser = argparse.ArgumentParser(
        description="Write Phase-1 HOT runtime candles from Databento Live. No broker or submit paths are invoked."
    )
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--runtime-candle-root", default=str(DEFAULT_RUNTIME_CANDLE_ROOT))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--legacy-live-output-root", default=str(DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT))
    parser.add_argument("--dataset", default=DEFAULT_DATABENTO_DATASET)
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--max-bars", type=int, default=90)
    parser.add_argument("--min-bars", type=int, default=8)
    parser.add_argument("--max-records", type=int, default=90)
    parser.add_argument("--max-seconds-per-symbol", type=float, default=75.0)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip())
    result = build_phase1_databento_live_runtime_candles(
        config=Phase1DatabentoLiveRuntimeCandlesConfig(
            repo_root=Path(args.repo_root),
            runtime_candle_root=Path(args.runtime_candle_root),
            report_dir=Path(args.report_dir),
            legacy_live_output_root=Path(args.legacy_live_output_root),
            symbols=symbols,
            dataset=args.dataset,
            schema=args.schema,
            env_file=args.env_file,
            max_bars=args.max_bars,
            min_bars=args.min_bars,
            max_records=args.max_records,
            max_seconds_per_symbol=args.max_seconds_per_symbol,
            max_workers=args.max_workers,
        ),
        write_artifacts=not args.no_write,
    )
    print(
        json.dumps(
            {
                "final_classification": result.report["final_classification"],
                "phase1_symbol_count": result.report["phase1_symbol_count"],
                "realtime_feed_confirmed_count": result.report["realtime_feed_confirmed_count"],
                "report_path": str(_resolve_path(Path(args.repo_root), Path(args.report_dir)) / "latest_phase1_databento_live_runtime_candles_report.json"),
                "can_submit": result.report["can_submit"],
                "live_money_eligible": result.report["live_money_eligible"],
            },
            sort_keys=True,
        )
    )
    return 0 if result.report["final_classification"] == "PHASE1_REALTIME_MARKET_DATA_CONFIRMED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
