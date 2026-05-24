"""Normalize Phase-1 runtime candle authority artifacts for P0 envelopes.

The Phase-1 live service publishes bounded runtime market-data authority with a
``bars`` array. Older P0 envelope producers consume the established
``candles``/``candle_history`` contract. This adapter is intentionally pure and
no-submit: it translates schema shape and preserves provenance without creating
any routing authority.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY = "PHASE1_RUNTIME_MARKET_DATA"
PHASE1_OBSERVE_ONLY_QUOTE_VERDICT = "PHASE1_RUNTIME_MARKET_DATA_OBSERVE_ONLY_REALTIMENESS_ACCEPTED"
LEGACY_RUNTIME_CANDLE_PATH_MARKERS = (
    "outputs/track_b_execution_core/databento_live_runtime_feed/",
    "outputs/track_b_execution_core/track_b_runtime_candle_capture/",
)


def is_legacy_p0_runtime_candle_path(path: Path | str | None) -> bool:
    if path is None:
        return False
    normalized = str(path).replace("\\", "/")
    return any(marker in normalized for marker in LEGACY_RUNTIME_CANDLE_PATH_MARKERS)


def legacy_p0_runtime_candle_path_blocker(path: Path | str | None) -> str | None:
    if not is_legacy_p0_runtime_candle_path(path):
        return None
    return (
        "P0 observe-only envelope producers require Phase-1 runtime market-data authority paths by default; "
        f"legacy runtime candle path is diagnostic-only unless explicitly allowed: {path}"
    )


def normalize_phase1_runtime_candle_payload(
    payload: Mapping[str, Any],
    *,
    source_path: Path | str | None = None,
) -> dict[str, Any]:
    """Return a candle-history compatible payload when a Phase-1 ``bars`` array is present."""

    normalized = dict(payload)
    raw_bars = payload.get("bars")
    if not isinstance(raw_bars, Sequence) or isinstance(raw_bars, (str, bytes)):
        return normalized

    timeframe = _text(payload.get("timeframe"))
    symbol = _text(payload.get("symbol") or payload.get("instrument") or payload.get("execution_symbol"))
    candles = [_normalize_bar(bar, timeframe=timeframe) for bar in raw_bars if isinstance(bar, Mapping)]
    latest_bar_timestamp = _latest_bar_timestamp(payload=payload, candles=candles)
    freshness_status = _freshness_status(payload)
    source_path_text = None if source_path is None else str(source_path)

    normalized.update(
        {
            "candles": candles,
            "candle_history": candles,
            "source_path": source_path_text,
            "source_payload_path": source_path_text,
            "source_authority_path": source_path_text,
            "source_category": PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
            "input_source_category": PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
            "source_authority": "execution_core_phase1_runtime_market_data",
            "phase1_runtime_market_data_authority": True,
            "phase1_runtime_market_data_schema": payload.get("schema"),
            "latest_bar_timestamp": latest_bar_timestamp,
            "freshness_status": freshness_status,
            "quote_provider_mode": payload.get("quote_provider_mode") or "REALTIME",
            "realtime_quote_received": payload.get("realtime_quote_received")
            if payload.get("realtime_quote_received") is not None
            else payload.get("realtime_feed_confirmed") is True,
            "current_quote_available": payload.get("current_quote_available")
            if payload.get("current_quote_available") is not None
            else payload.get("realtime_feed_confirmed") is True,
            "quote_freshness_verdict": payload.get("quote_freshness_verdict") or PHASE1_OBSERVE_ONLY_QUOTE_VERDICT,
            "quote_evidence_source": "phase1_runtime_market_data_observe_only",
            "p0_observe_only_quote_mapping": True,
            "not_order_authority": True,
            "not_lifecycle_authority": True,
            "submit_allowed": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
            "live_money_eligible": False,
        }
    )
    normalized.update(_instrument_defaults(symbol=symbol, payload=payload))
    return normalized


def _normalize_bar(bar: Mapping[str, Any], *, timeframe: str | None) -> dict[str, Any]:
    timestamp = (
        bar.get("candle_timestamp")
        or bar.get("timestamp")
        or bar.get("bar_end")
        or bar.get("bar_end_ts")
        or bar.get("end_ts")
        or bar.get("observed_at")
    )
    return {
        "candle_timestamp": timestamp,
        "observed_at": bar.get("observed_at") or timestamp,
        "timeframe": timeframe,
        "open": bar.get("open"),
        "high": bar.get("high"),
        "low": bar.get("low"),
        "close": bar.get("close") if bar.get("close") is not None else bar.get("last"),
        "volume": bar.get("volume"),
        "completed": bar.get("completed") is not False,
        "source_bar_count": bar.get("source_bar_count"),
        "source_start_timestamp": bar.get("bar_start") or bar.get("source_start_timestamp"),
        "source_end_timestamp": timestamp,
        "source_category": PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
    }


def _latest_bar_timestamp(*, payload: Mapping[str, Any], candles: Sequence[Mapping[str, Any]]) -> str | None:
    for key in ("last_completed_bar_ts", "latest_bar_timestamp", "latest_completed_bar_at"):
        value = payload.get(key)
        if value:
            return str(value)
    if candles:
        value = candles[-1].get("candle_timestamp")
        return None if value is None else str(value)
    return None


def _freshness_status(payload: Mapping[str, Any]) -> str:
    if payload.get("realtime_feed_block_reason"):
        return str(payload.get("realtime_feed_block_reason"))
    if payload.get("realtime_feed_confirmed") is True:
        return "READY"
    generated_at = _parse_datetime(payload.get("generated_at"))
    if generated_at is None:
        return "UNKNOWN"
    age_seconds = max((datetime.now(UTC) - generated_at).total_seconds(), 0.0)
    freshness_seconds = _float_or_none(payload.get("freshness_seconds"))
    if freshness_seconds is not None and age_seconds > freshness_seconds:
        return "STALE"
    return "READY"


def _instrument_defaults(*, symbol: str | None, payload: Mapping[str, Any]) -> dict[str, Any]:
    resolved = (symbol or "").upper()
    if resolved == "MNQ":
        return {
            "contract_key": payload.get("contract_key") or "MNQ-202606",
            "instrument_family": payload.get("instrument_family") or "MNQ",
            "local_symbol": payload.get("local_symbol") or "MNQM6",
            "dataset": payload.get("dataset") or "GLBX.MDP3",
        }
    if resolved == "MGC":
        return {
            "contract_key": payload.get("contract_key") or "MGC-202606",
            "instrument_family": payload.get("instrument_family") or "MGC",
            "local_symbol": payload.get("local_symbol") or "MGCM6",
            "dataset": payload.get("dataset") or "GLBX.MDP3",
        }
    return {
        "contract_key": payload.get("contract_key"),
        "instrument_family": payload.get("instrument_family") or resolved or None,
        "local_symbol": payload.get("local_symbol"),
        "dataset": payload.get("dataset"),
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
