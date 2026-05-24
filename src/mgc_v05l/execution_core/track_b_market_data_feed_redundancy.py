"""Read-only Track B market-data feed redundancy audit.

This module compares existing market-data artifacts only. It never connects to
Databento, TWS/IBKR, brokers, runtimes, lifecycle stores, or strategy runners.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.paths import PROJECT_ROOT


DATABENTO_SOURCE = "DATABENTO_REALTIME_PHASE1"

FEED_HEALTHY = "HEALTHY"
FEED_STALE = "STALE"
FEED_DOWN = "DOWN"
FEED_NOT_CONFIGURED = "NOT_CONFIGURED"

PRIMARY_FEED_HEALTHY = "PRIMARY_FEED_HEALTHY"
BACKUP_FEED_HEALTHY = "BACKUP_FEED_HEALTHY"
FEEDS_ALIGNED = "FEEDS_ALIGNED"
FEEDS_DIVERGENT = "FEEDS_DIVERGENT"
PRIMARY_STALE_BACKUP_HEALTHY = "PRIMARY_STALE_BACKUP_HEALTHY"
BACKUP_STALE_PRIMARY_HEALTHY = "BACKUP_STALE_PRIMARY_HEALTHY"
NO_TRUSTED_FEED = "NO_TRUSTED_FEED"
IBKR_FEED_NOT_CONFIGURED = "IBKR_FEED_NOT_CONFIGURED"

DEFAULT_SYMBOLS = ("MGC", "MNQ")
DEFAULT_TIMEFRAMES = ("1m", "5m")
DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core" / "market_data_feed_redundancy"
DEFAULT_FRESHNESS_SECONDS = {
    "1m": 180.0,
    "5m": 600.0,
}
DEFAULT_PRICE_TOLERANCE = 0.25


@dataclass(frozen=True)
class TrackBMarketDataFeedRedundancyConfig:
    repo_root: Path = PROJECT_ROOT
    databento_runtime_candle_root: Path = (
        Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
    )
    ibkr_runtime_candle_root: Path = (
        Path("outputs") / "track_b_execution_core" / "ibkr_runtime_market_data"
    )
    ibkr_market_data_diagnostic_path: Path = (
        Path("outputs") / "reports" / "ibkr_market_data_diagnostic" / "ibkr_market_data_diagnostic_report.json"
    )
    output_root: Path = DEFAULT_OUTPUT_ROOT
    symbols: tuple[str, ...] = DEFAULT_SYMBOLS
    timeframes: tuple[str, ...] = DEFAULT_TIMEFRAMES
    now: datetime | None = None
    freshness_seconds_by_timeframe: Mapping[str, float] | None = None
    price_tolerance: float = DEFAULT_PRICE_TOLERANCE

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


@dataclass(frozen=True)
class TrackBMarketDataFeedRedundancyArtifacts:
    feed_health: dict[str, Any]
    feed_comparison: dict[str, Any]


def build_track_b_market_data_feed_redundancy_audit(
    *,
    config: TrackBMarketDataFeedRedundancyConfig,
) -> TrackBMarketDataFeedRedundancyArtifacts:
    now = _coerce_now(config.now)
    pairs = [(symbol.upper(), timeframe) for symbol in config.symbols for timeframe in config.timeframes]
    databento_feed = _build_feed_health(
        feed_name="databento_phase1_runtime",
        provider="DATABENTO",
        root=config.resolve(config.databento_runtime_candle_root),
        pairs=pairs,
        now=now,
        required_source=DATABENTO_SOURCE,
        freshness_seconds_by_timeframe=_freshness(config),
        not_configured_if_missing=False,
    )
    ibkr_diag = _read_json(config.resolve(config.ibkr_market_data_diagnostic_path))
    ibkr_feed = _build_feed_health(
        feed_name="ibkr_readonly_runtime_candidate",
        provider="IBKR",
        root=config.resolve(config.ibkr_runtime_candle_root),
        pairs=pairs,
        now=now,
        required_source=None,
        freshness_seconds_by_timeframe=_freshness(config),
        not_configured_if_missing=True,
        diagnostic=ibkr_diag,
    )
    comparisons = [
        _compare_pair(
            symbol=symbol,
            timeframe=timeframe,
            primary=databento_feed,
            backup=ibkr_feed,
            price_tolerance=float(config.price_tolerance),
        )
        for symbol, timeframe in pairs
    ]
    classifications = _overall_classifications(
        primary=databento_feed,
        backup=ibkr_feed,
        comparisons=comparisons,
    )
    output_root = config.resolve(config.output_root)
    feed_health = {
        "schema_version": "track_b_market_data_feed_health_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "runtime_feed_switching_allowed": False,
        "strategy_input_switching_allowed": False,
        "broker_mutation_allowed": False,
        "order_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "output_path": str(output_root / "latest_feed_health.json"),
        "requested_symbols": list(config.symbols),
        "requested_timeframes": list(config.timeframes),
        "feeds": [databento_feed["summary"], ibkr_feed["summary"]],
        "feed_details": {
            databento_feed["summary"]["feed_name"]: databento_feed["details"],
            ibkr_feed["summary"]["feed_name"]: ibkr_feed["details"],
        },
        "inventory": _inventory(config=config),
    }
    feed_comparison = {
        "schema_version": "track_b_market_data_feed_comparison_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "runtime_feed_switching_allowed": False,
        "strategy_input_switching_allowed": False,
        "classifications": classifications,
        "primary_feed": databento_feed["summary"],
        "backup_feed": ibkr_feed["summary"],
        "comparison_count": len(comparisons),
        "comparisons": comparisons,
        "volume_caveat": (
            "IBKR realtime snapshot/diagnostic data may not provide exchange-equivalent OHLCV bars; "
            "volume is informational until an IBKR 1m/5m candle artifact source exists."
        ),
        "output_path": str(output_root / "latest_feed_comparison.json"),
    }
    return TrackBMarketDataFeedRedundancyArtifacts(
        feed_health=feed_health,
        feed_comparison=feed_comparison,
    )


def write_track_b_market_data_feed_redundancy_artifacts(
    *,
    config: TrackBMarketDataFeedRedundancyConfig,
    artifacts: TrackBMarketDataFeedRedundancyArtifacts,
) -> dict[str, Path]:
    output_root = config.resolve(config.output_root)
    health = write_json_atomic(output_root / "latest_feed_health.json", artifacts.feed_health)
    comparison = write_json_atomic(output_root / "latest_feed_comparison.json", artifacts.feed_comparison)
    return {"feed_health": health, "feed_comparison": comparison}


def _build_feed_health(
    *,
    feed_name: str,
    provider: str,
    root: Path,
    pairs: Sequence[tuple[str, str]],
    now: datetime,
    required_source: str | None,
    freshness_seconds_by_timeframe: Mapping[str, float],
    not_configured_if_missing: bool,
    diagnostic: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    bars_by_pair: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    symbols_available: set[str] = set()
    timeframes_available: set[str] = set()
    latest_timestamp: datetime | None = None
    statuses: list[str] = []
    for symbol, timeframe in pairs:
        pair_key = _pair_key(symbol, timeframe)
        path = root / symbol / timeframe / "latest_runtime_candles.json"
        payload = _read_json(path)
        if not payload:
            status = FEED_NOT_CONFIGURED if not_configured_if_missing else FEED_DOWN
            bars_by_pair[pair_key] = _missing_pair(path=path, symbol=symbol, timeframe=timeframe, status=status)
            errors.append(f"{pair_key}: missing {path}")
            statuses.append(status)
            continue
        pair = _extract_pair_health(
            payload=payload,
            path=path,
            symbol=symbol,
            timeframe=timeframe,
            now=now,
            required_source=required_source,
            freshness_seconds=freshness_seconds_by_timeframe.get(timeframe, 600.0),
        )
        bars_by_pair[pair_key] = pair
        statuses.append(str(pair["status"]))
        if pair["status"] in {FEED_HEALTHY, FEED_STALE}:
            symbols_available.add(symbol)
            timeframes_available.add(timeframe)
        if pair.get("latest_timestamp"):
            parsed = _parse_datetime(pair.get("latest_timestamp"))
            if parsed is not None and (latest_timestamp is None or parsed > latest_timestamp):
                latest_timestamp = parsed
        errors.extend(str(item) for item in pair.get("errors", []))
    status = _rollup_status(statuses=statuses, not_configured_if_missing=not_configured_if_missing)
    diag_connected = bool(_mapping(diagnostic).get("connection_check", {}).get("connected"))
    connected = status in {FEED_HEALTHY, FEED_STALE}
    if diagnostic and status == FEED_NOT_CONFIGURED:
        errors.append(
            "IBKR read-only snapshot diagnostic exists, but no IBKR 1m/5m OHLC artifact source is configured."
        )
    return {
        "summary": {
            "feed_name": feed_name,
            "provider": provider,
            "connected": connected,
            "latest_timestamp": latest_timestamp.isoformat() if latest_timestamp else None,
            "staleness_seconds": None
            if latest_timestamp is None
            else max(0.0, (now - latest_timestamp).total_seconds()),
            "symbols_available": sorted(symbols_available),
            "timeframe_available": sorted(timeframes_available),
            "errors": errors,
            "status": status,
        },
        "details": {
            "root": str(root),
            "required_source": required_source,
            "bars_by_pair": bars_by_pair,
            "diagnostic_classification": _mapping(diagnostic).get("classification"),
            "diagnostic_connection_seen": diag_connected,
            "diagnostic_generated_at": _mapping(diagnostic).get("generated_at"),
            "diagnostic_path_available": bool(diagnostic),
        },
    }


def _extract_pair_health(
    *,
    payload: Mapping[str, Any],
    path: Path,
    symbol: str,
    timeframe: str,
    now: datetime,
    required_source: str | None,
    freshness_seconds: float,
) -> dict[str, Any]:
    errors: list[str] = []
    payload_symbol = str(payload.get("symbol") or "").upper()
    payload_timeframe = str(payload.get("timeframe") or "")
    source = str(payload.get("source") or payload.get("source_id") or "")
    if payload_symbol != symbol:
        errors.append(f"symbol mismatch: expected {symbol}, got {payload_symbol or 'missing'}")
    if payload_timeframe != timeframe:
        errors.append(f"timeframe mismatch: expected {timeframe}, got {payload_timeframe or 'missing'}")
    if required_source and source != required_source:
        errors.append(f"source mismatch: expected {required_source}, got {source or 'missing'}")
    if payload.get("research_artifact_used") is True or payload.get("archive_artifact_used") is True:
        errors.append("research/archive artifact cannot be feed redundancy truth")
    if payload.get("historical_seed_ready") is True or payload.get("databento_live_api_replay") is True:
        errors.append("historical seed/replay artifact cannot be feed redundancy truth")
    if payload.get("completed_candles_only") is not True:
        errors.append("completed_candles_only must be true")
    latest_bar = _latest_bar(payload)
    latest_ts = _parse_datetime(latest_bar.get("timestamp"))
    if latest_ts is None:
        errors.append("latest completed bar timestamp missing")
        staleness_seconds = None
    else:
        staleness_seconds = max(0.0, (now - latest_ts).total_seconds())
    if errors:
        status = FEED_DOWN
    elif staleness_seconds is not None and staleness_seconds > freshness_seconds:
        status = FEED_STALE
    else:
        status = FEED_HEALTHY
    return {
        "path": str(path),
        "symbol": symbol,
        "timeframe": timeframe,
        "status": status,
        "latest_timestamp": None if latest_ts is None else latest_ts.isoformat(),
        "staleness_seconds": staleness_seconds,
        "freshness_threshold_seconds": freshness_seconds,
        "bar": latest_bar,
        "errors": errors,
    }


def _compare_pair(
    *,
    symbol: str,
    timeframe: str,
    primary: Mapping[str, Any],
    backup: Mapping[str, Any],
    price_tolerance: float,
) -> dict[str, Any]:
    pair_key = _pair_key(symbol, timeframe)
    primary_pair = _mapping(_mapping(primary.get("details")).get("bars_by_pair")).get(pair_key, {})
    backup_pair = _mapping(_mapping(backup.get("details")).get("bars_by_pair")).get(pair_key, {})
    primary_status = str(_mapping(primary_pair).get("status") or FEED_DOWN)
    backup_status = str(_mapping(backup_pair).get("status") or FEED_NOT_CONFIGURED)
    primary_healthy = primary_status == FEED_HEALTHY
    backup_healthy = backup_status == FEED_HEALTHY
    if backup_status == FEED_NOT_CONFIGURED:
        classification = IBKR_FEED_NOT_CONFIGURED
    elif primary_healthy and backup_healthy:
        classification = _aligned_or_divergent(
            primary_pair=_mapping(primary_pair),
            backup_pair=_mapping(backup_pair),
            price_tolerance=price_tolerance,
        )
    elif primary_status == FEED_STALE and backup_healthy:
        classification = PRIMARY_STALE_BACKUP_HEALTHY
    elif backup_status == FEED_STALE and primary_healthy:
        classification = BACKUP_STALE_PRIMARY_HEALTHY
    elif not primary_healthy and not backup_healthy:
        classification = NO_TRUSTED_FEED
    else:
        classification = FEEDS_DIVERGENT
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "classification": classification,
        "primary_status": primary_status,
        "backup_status": backup_status,
        "latest_timestamp_alignment": _timestamp_alignment(primary_pair, backup_pair),
        "ohlc_price_difference": _ohlc_diff(primary_pair, backup_pair),
        "missing_bars": _missing_bars(primary_pair, backup_pair),
        "stale_feed_detection": {
            "primary_stale": primary_status == FEED_STALE,
            "backup_stale": backup_status == FEED_STALE,
        },
        "volume_caveat": "IBKR volume may differ or be unavailable.",
    }


def _overall_classifications(
    *,
    primary: Mapping[str, Any],
    backup: Mapping[str, Any],
    comparisons: Sequence[Mapping[str, Any]],
) -> list[str]:
    values: list[str] = []
    primary_status = str(_mapping(primary.get("summary")).get("status") or FEED_DOWN)
    backup_status = str(_mapping(backup.get("summary")).get("status") or FEED_NOT_CONFIGURED)
    if primary_status == FEED_HEALTHY:
        values.append(PRIMARY_FEED_HEALTHY)
    if backup_status == FEED_HEALTHY:
        values.append(BACKUP_FEED_HEALTHY)
    for classification in (
        FEEDS_ALIGNED,
        FEEDS_DIVERGENT,
        PRIMARY_STALE_BACKUP_HEALTHY,
        BACKUP_STALE_PRIMARY_HEALTHY,
        NO_TRUSTED_FEED,
        IBKR_FEED_NOT_CONFIGURED,
    ):
        if any(row.get("classification") == classification for row in comparisons):
            values.append(classification)
    if not values:
        values.append(NO_TRUSTED_FEED)
    return values


def _aligned_or_divergent(
    *,
    primary_pair: Mapping[str, Any],
    backup_pair: Mapping[str, Any],
    price_tolerance: float,
) -> str:
    alignment = _timestamp_alignment(primary_pair, backup_pair)
    diffs = _ohlc_diff(primary_pair, backup_pair)
    if alignment.get("aligned") is not True:
        return FEEDS_DIVERGENT
    numeric_diffs = [abs(float(value)) for value in diffs.values() if value is not None]
    if any(value > price_tolerance for value in numeric_diffs):
        return FEEDS_DIVERGENT
    return FEEDS_ALIGNED


def _latest_bar(payload: Mapping[str, Any]) -> dict[str, Any]:
    bars = payload.get("bars") or payload.get("candles") or []
    latest: Mapping[str, Any] = {}
    if isinstance(bars, list) and bars:
        for row in reversed(bars):
            if isinstance(row, Mapping) and row.get("completed") is not False:
                latest = row
                break
    ts = (
        latest.get("bar_end")
        or latest.get("timestamp")
        or latest.get("end_ts")
        or payload.get("last_completed_bar_ts")
        or payload.get("latest_timestamp")
    )
    return {
        "timestamp": ts,
        "open": _float_or_none(latest.get("open")),
        "high": _float_or_none(latest.get("high")),
        "low": _float_or_none(latest.get("low")),
        "close": _float_or_none(latest.get("close")),
        "volume": _float_or_none(latest.get("volume")),
    }


def _timestamp_alignment(primary_pair: Any, backup_pair: Any) -> dict[str, Any]:
    primary_ts = _parse_datetime(_mapping(primary_pair).get("latest_timestamp"))
    backup_ts = _parse_datetime(_mapping(backup_pair).get("latest_timestamp"))
    if primary_ts is None or backup_ts is None:
        return {"aligned": False, "delta_seconds": None}
    delta = abs((primary_ts - backup_ts).total_seconds())
    return {"aligned": delta == 0.0, "delta_seconds": delta}


def _ohlc_diff(primary_pair: Any, backup_pair: Any) -> dict[str, float | None]:
    primary_bar = _mapping(_mapping(primary_pair).get("bar"))
    backup_bar = _mapping(_mapping(backup_pair).get("bar"))
    diffs: dict[str, float | None] = {}
    for field in ("open", "high", "low", "close"):
        primary_value = _float_or_none(primary_bar.get(field))
        backup_value = _float_or_none(backup_bar.get(field))
        diffs[field] = None if primary_value is None or backup_value is None else backup_value - primary_value
    return diffs


def _missing_bars(primary_pair: Any, backup_pair: Any) -> dict[str, bool]:
    return {
        "primary_missing": not bool(_mapping(primary_pair).get("latest_timestamp")),
        "backup_missing": not bool(_mapping(backup_pair).get("latest_timestamp")),
    }


def _rollup_status(*, statuses: Sequence[str], not_configured_if_missing: bool) -> str:
    if not statuses:
        return FEED_NOT_CONFIGURED if not_configured_if_missing else FEED_DOWN
    if all(status == FEED_NOT_CONFIGURED for status in statuses):
        return FEED_NOT_CONFIGURED
    if any(status == FEED_DOWN for status in statuses):
        return FEED_DOWN
    if any(status == FEED_STALE for status in statuses):
        return FEED_STALE
    if any(status == FEED_HEALTHY for status in statuses):
        return FEED_HEALTHY
    return FEED_NOT_CONFIGURED if not_configured_if_missing else FEED_DOWN


def _missing_pair(*, path: Path, symbol: str, timeframe: str, status: str) -> dict[str, Any]:
    return {
        "path": str(path),
        "symbol": symbol,
        "timeframe": timeframe,
        "status": status,
        "latest_timestamp": None,
        "staleness_seconds": None,
        "freshness_threshold_seconds": None,
        "bar": {},
        "errors": [f"missing artifact: {path}"],
    }


def _inventory(config: TrackBMarketDataFeedRedundancyConfig) -> dict[str, Any]:
    return {
        "databento_phase1_feed_path": str(config.resolve(config.databento_runtime_candle_root)),
        "databento_phase1_artifact_pattern": "{symbol}/{timeframe}/latest_runtime_candles.json",
        "ibkr_readonly_snapshot_diagnostic_path": str(config.resolve(config.ibkr_market_data_diagnostic_path)),
        "ibkr_ohlc_artifact_candidate_path": str(config.resolve(config.ibkr_runtime_candle_root)),
        "ibkr_current_capability": (
            "Read-only IBKR snapshot diagnostics exist; no active IBKR 1m/5m OHLC candle artifact source "
            "is configured by default."
        ),
        "first_scope_symbols": list(config.symbols),
        "first_scope_timeframes": list(config.timeframes),
    }


def _freshness(config: TrackBMarketDataFeedRedundancyConfig) -> Mapping[str, float]:
    return config.freshness_seconds_by_timeframe or DEFAULT_FRESHNESS_SECONDS


def _pair_key(symbol: str, timeframe: str) -> str:
    return f"{symbol.upper()}:{timeframe}"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


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
    return _coerce_now(parsed)


def _coerce_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build read-only Track B market-data feed redundancy artifacts.")
    parser.add_argument("--repo-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBMarketDataFeedRedundancyConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_root=Path(args.output_root),
    )
    artifacts = build_track_b_market_data_feed_redundancy_audit(config=config)
    written: dict[str, Path] = {}
    if not bool(args.no_write):
        written = write_track_b_market_data_feed_redundancy_artifacts(config=config, artifacts=artifacts)
    summary = {
        "classifications": artifacts.feed_comparison.get("classifications"),
        "feed_statuses": [
            {"provider": row.get("provider"), "status": row.get("status")}
            for row in artifacts.feed_health.get("feeds", [])
        ],
        "read_only": True,
        "runtime_feed_switching_allowed": False,
        "written": {key: str(value) for key, value in written.items()},
    }
    print(json.dumps(artifacts.feed_comparison if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
