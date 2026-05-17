"""Offline Track B regime/session context evaluator.

This module emits advisory regime and session context only. It has no
strategy authority, order authority, lifecycle mutation, or broker dependency.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Mapping, Sequence

from mgc_v05l.session_phase_labels import label_session_phase, phase_coarse_session_group


SCHEMA_VERSION = "track_b_regime_session_context_v1"
PRODUCER_NAME = "TrackBRegimeStateLayer"
DEFAULT_CANDLE_TIMEFRAME = "5m"


class MarketRegimeState(str, Enum):
    REGIME_TRENDING = "REGIME_TRENDING"
    REGIME_CHOP_BALANCED = "REGIME_CHOP_BALANCED"
    REGIME_EXPANSION = "REGIME_EXPANSION"
    REGIME_COMPRESSION = "REGIME_COMPRESSION"
    REGIME_THIN_OR_STALE = "REGIME_THIN_OR_STALE"
    REGIME_LOW_CONFIDENCE = "REGIME_LOW_CONFIDENCE"


class VolatilityRangeState(str, Enum):
    RANGE_COMPRESSED = "RANGE_COMPRESSED"
    RANGE_NORMAL = "RANGE_NORMAL"
    RANGE_EXPANDED = "RANGE_EXPANDED"
    RANGE_THIN_OR_INVALID = "RANGE_THIN_OR_INVALID"


class VolatilityState(str, Enum):
    VOL_LOW = "VOL_LOW"
    VOL_NORMAL = "VOL_NORMAL"
    VOL_HIGH = "VOL_HIGH"


class TrendChopState(str, Enum):
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    TREND_MIXED = "TREND_MIXED"
    CHOP_BALANCED = "CHOP_BALANCED"
    CHOP_DIRECTIONLESS = "CHOP_DIRECTIONLESS"
    COMPRESSION_COILING = "COMPRESSION_COILING"
    EXPANSION_DIRECTIONAL = "EXPANSION_DIRECTIONAL"
    LOW_CONFIDENCE_TREND_CHOP = "LOW_CONFIDENCE_TREND_CHOP"


class LiquidityState(str, Enum):
    LIQUIDITY_NORMAL = "LIQUIDITY_NORMAL"
    LIQUIDITY_THIN_RANGE = "LIQUIDITY_THIN_RANGE"
    LIQUIDITY_THIN_VOLUME = "LIQUIDITY_THIN_VOLUME"
    LIQUIDITY_STALE_OR_INCOMPLETE = "LIQUIDITY_STALE_OR_INCOMPLETE"
    LIQUIDITY_UNKNOWN = "LIQUIDITY_UNKNOWN"


class DirectionalContext(str, Enum):
    DIRECTIONAL_BULLISH = "DIRECTIONAL_BULLISH"
    DIRECTIONAL_BEARISH = "DIRECTIONAL_BEARISH"
    DIRECTIONAL_BALANCED = "DIRECTIONAL_BALANCED"
    DIRECTIONAL_MIXED = "DIRECTIONAL_MIXED"
    DIRECTIONAL_UNKNOWN = "DIRECTIONAL_UNKNOWN"


REGIME_TRENDING = MarketRegimeState.REGIME_TRENDING.value
REGIME_CHOP_BALANCED = MarketRegimeState.REGIME_CHOP_BALANCED.value
REGIME_EXPANSION = MarketRegimeState.REGIME_EXPANSION.value
REGIME_COMPRESSION = MarketRegimeState.REGIME_COMPRESSION.value
REGIME_THIN_OR_STALE = MarketRegimeState.REGIME_THIN_OR_STALE.value
REGIME_LOW_CONFIDENCE = MarketRegimeState.REGIME_LOW_CONFIDENCE.value

RANGE_COMPRESSED = VolatilityRangeState.RANGE_COMPRESSED.value
RANGE_NORMAL = VolatilityRangeState.RANGE_NORMAL.value
RANGE_EXPANDED = VolatilityRangeState.RANGE_EXPANDED.value
RANGE_THIN_OR_INVALID = VolatilityRangeState.RANGE_THIN_OR_INVALID.value

VOL_LOW = VolatilityState.VOL_LOW.value
VOL_NORMAL = VolatilityState.VOL_NORMAL.value
VOL_HIGH = VolatilityState.VOL_HIGH.value

TREND_UP = TrendChopState.TREND_UP.value
TREND_DOWN = TrendChopState.TREND_DOWN.value
TREND_MIXED = TrendChopState.TREND_MIXED.value
CHOP_BALANCED = TrendChopState.CHOP_BALANCED.value
CHOP_DIRECTIONLESS = TrendChopState.CHOP_DIRECTIONLESS.value
COMPRESSION_COILING = TrendChopState.COMPRESSION_COILING.value
EXPANSION_DIRECTIONAL = TrendChopState.EXPANSION_DIRECTIONAL.value
LOW_CONFIDENCE_TREND_CHOP = TrendChopState.LOW_CONFIDENCE_TREND_CHOP.value

LIQUIDITY_NORMAL = LiquidityState.LIQUIDITY_NORMAL.value
LIQUIDITY_THIN_RANGE = LiquidityState.LIQUIDITY_THIN_RANGE.value
LIQUIDITY_THIN_VOLUME = LiquidityState.LIQUIDITY_THIN_VOLUME.value
LIQUIDITY_STALE_OR_INCOMPLETE = LiquidityState.LIQUIDITY_STALE_OR_INCOMPLETE.value
LIQUIDITY_UNKNOWN = LiquidityState.LIQUIDITY_UNKNOWN.value

DIRECTIONAL_BULLISH = DirectionalContext.DIRECTIONAL_BULLISH.value
DIRECTIONAL_BEARISH = DirectionalContext.DIRECTIONAL_BEARISH.value
DIRECTIONAL_BALANCED = DirectionalContext.DIRECTIONAL_BALANCED.value
DIRECTIONAL_MIXED = DirectionalContext.DIRECTIONAL_MIXED.value
DIRECTIONAL_UNKNOWN = DirectionalContext.DIRECTIONAL_UNKNOWN.value

THIN_DATA = "THIN_DATA"
STALE_INPUT = "STALE_INPUT"
INCOMPLETE_CANDLES = "INCOMPLETE_CANDLES"
MIXED_TIMEFRAME = "MIXED_TIMEFRAME"
MISSING_TIMEFRAME_SCHEMA = "MISSING_TIMEFRAME_SCHEMA"
AMBIGUOUS_TIMEFRAME = "AMBIGUOUS_TIMEFRAME"
MALFORMED_CANDLES = "MALFORMED_CANDLES"
LOW_RANGE_QUALITY = "LOW_RANGE_QUALITY"
MISSING_PROVENANCE = "MISSING_PROVENANCE"
RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE = "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"

INPUT_MODE_RUNTIME_DECISION = "RUNTIME_DECISION"
INPUT_MODE_OFFLINE_EVALUATION = "OFFLINE_EVALUATION"
INPUT_MODE_REPLAY_RESEARCH = "REPLAY_RESEARCH"
INPUT_MODE_TEST = "TEST"
INPUT_MODE_UNKNOWN = "UNKNOWN"
SOURCE_CATEGORY_RUNTIME = "RUNTIME"
SOURCE_CATEGORY_RESEARCH = "RESEARCH"
SOURCE_CATEGORY_TEST_FIXTURE = "TEST_FIXTURE"
SOURCE_CATEGORY_UNKNOWN = "UNKNOWN"
SOURCE_PROVENANCE_RUNTIME_DECLARED = "RUNTIME_SOURCE_DECLARED"
SOURCE_PROVENANCE_RESEARCH_NOT_RUNTIME_ELIGIBLE = "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"
SOURCE_PROVENANCE_TEST_NOT_RUNTIME_ELIGIBLE = "TEST_FIXTURE_NOT_RUNTIME_ELIGIBLE"
SOURCE_PROVENANCE_RUNTIME_NOT_DECLARED = "RUNTIME_SOURCE_NOT_DECLARED"
SOURCE_PROVENANCE_UNKNOWN = "SOURCE_PROVENANCE_UNKNOWN"
TIMEFRAME_SOURCE_NATIVE = "NATIVE"
TIMEFRAME_SOURCE_DERIVED = "DERIVED"
TIMEFRAME_ALIGNMENT_ALIGNED = "ALIGNED"
TIMEFRAME_ALIGNMENT_DERIVED_ALIGNED = "DERIVED_ALIGNED"
TIMEFRAME_ALIGNMENT_MIXED_BLOCKED = "MIXED_TIMEFRAME_BLOCKED"


@dataclass(frozen=True)
class RegimeStateThresholds:
    min_completed_candles: int = 8
    preferred_completed_candles: int = 16
    fast_window: int = 3
    stale_after_intervals: float = 3.0
    max_missing_gap_intervals: float = 1.5
    tiny_range_pct: float = 0.00005
    compressed_range_ratio: float = 0.75
    expanded_range_ratio: float = 1.45
    high_volatility_range_ratio: float = 1.65
    trend_slope_ratio: float = 0.55
    directional_bar_ratio: float = 0.65
    chop_alternation_ratio: float = 0.60


@dataclass(frozen=True)
class NormalizedCandle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    timeframe: str

    @property
    def range(self) -> float:
        return self.high - self.low

    @property
    def body(self) -> float:
        return self.close - self.open

    @property
    def range_pct(self) -> float:
        return self.range / self.close


@dataclass(frozen=True)
class _ValidationResult:
    candles: tuple[NormalizedCandle, ...]
    candles_received: int
    latest_candle_timestamp: datetime | None
    freshness_status: str
    failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def build_regime_session_context_state(
    payload: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
    thresholds: RegimeStateThresholds | None = None,
) -> dict[str, Any]:
    """Build deterministic advisory regime/session context from explicit inputs."""

    resolved_now = _coerce_now(now)
    resolved_thresholds = thresholds or RegimeStateThresholds()
    input_context = _input_context(payload)
    timeframe_context = _timeframe_context(payload)
    initial_failures: list[str] = []
    initial_warnings: list[str] = []
    initial_failures.extend(_provenance_failures(input_context))
    initial_failures.extend(_timeframe_failures(timeframe_context))
    raw_candles = _extract_candle_rows(payload)

    if not _is_sequence(raw_candles):
        validation = _ValidationResult(
            candles=(),
            candles_received=0,
            latest_candle_timestamp=None,
            freshness_status="NO_COMPLETED_CANDLES",
            failure_reasons=tuple(_dedupe([*initial_failures, THIN_DATA, MALFORMED_CANDLES])),
            warnings=tuple([*initial_warnings, "candle input must be a sequence under candles, ohlcv_candles, or bars."]),
        )
        return _low_confidence_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            validation=validation,
        )

    validation = _validate_candles(
        raw_candles,
        now=resolved_now,
        expected_timeframe=timeframe_context["primary_timeframe"],
        thresholds=resolved_thresholds,
        initial_failures=initial_failures,
        initial_warnings=initial_warnings,
    )
    if validation.failure_reasons:
        return _low_confidence_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            validation=validation,
        )

    features = _feature_summary(validation.candles, thresholds=resolved_thresholds)
    states = _classify_states(features, validation.candles)
    session_phase = _session_phase(payload, validation.candles[-1].timestamp)
    confidence = _confidence(features=features, completed_count=len(validation.candles), thresholds=resolved_thresholds)
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "ADVISORY_REGIME_CONTEXT_ONLY",
        "instrument": _instrument(payload),
        "source_id": _source_id(payload),
        "generated_at": resolved_now.isoformat(),
        **input_context,
        "source_provenance_status": _source_provenance_status(input_context),
        "freshness_status": validation.freshness_status,
        **timeframe_context,
        "latest_candle_timestamp": validation.latest_candle_timestamp.isoformat()
        if validation.latest_candle_timestamp
        else None,
        "candles_received": validation.candles_received,
        "completed_candles_used": len(validation.candles),
        "session_bucket": f"SESSION_{session_phase}",
        "session_phase": session_phase,
        "coarse_session_group": phase_coarse_session_group(session_phase),
        **states,
        "confidence": _round(confidence),
        "regime_reasons": _regime_reasons(states, features),
        "warning_reasons": list(validation.warnings),
        "failure_reasons": [],
        "feature_summary": features,
        **_safety_flags(),
    }


def _validate_candles(
    rows: Sequence[Any],
    *,
    now: datetime,
    expected_timeframe: str | None,
    thresholds: RegimeStateThresholds,
    initial_failures: Sequence[str],
    initial_warnings: Sequence[str],
) -> _ValidationResult:
    failures = list(initial_failures)
    warnings = list(initial_warnings)
    candles: list[NormalizedCandle] = []
    incomplete_count = 0
    mixed_timeframe_count = 0

    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] must be an object.")
            continue
        if row.get("completed") is not True:
            incomplete_count += 1
            continue
        row_timeframe = _normalize_timeframe(str(row.get("timeframe") or ""))
        if expected_timeframe is not None and row_timeframe != expected_timeframe:
            mixed_timeframe_count += 1
            continue
        missing = [field for field in ("open", "high", "low", "close") if field not in row]
        timestamp_value = row.get("timestamp", row.get("end_ts"))
        if timestamp_value is None:
            missing.append("timestamp")
        if missing:
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] missing required fields: {', '.join(sorted(set(missing)))}.")
            continue
        try:
            timestamp = _parse_timestamp(timestamp_value)
            open_price = _finite_float(row["open"])
            high = _finite_float(row["high"])
            low = _finite_float(row["low"])
            close = _finite_float(row["close"])
            volume = _optional_finite_float(row.get("volume"))
        except ValueError as exc:
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] has invalid field value: {exc}.")
            continue
        if min(open_price, high, low, close) <= 0 or high < low:
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] has invalid OHLC geometry.")
            continue
        if high == low:
            failures.append(LOW_RANGE_QUALITY)
            warnings.append(f"candle[{index}] has zero range.")
            continue
        if high < max(open_price, close) or low > min(open_price, close):
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] OHLC values are internally inconsistent.")
            continue
        candles.append(
            NormalizedCandle(
                timestamp=timestamp,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
                timeframe=row_timeframe,
            )
        )

    if incomplete_count:
        failures.append(INCOMPLETE_CANDLES)
        warnings.append(f"rejected {incomplete_count} incomplete candle(s).")
    if mixed_timeframe_count:
        failures.append(MIXED_TIMEFRAME)
        warnings.append(f"rejected {mixed_timeframe_count} candle(s) outside primary timeframe.")
    for previous, current in zip(candles, candles[1:]):
        if current.timestamp <= previous.timestamp:
            failures.append(MALFORMED_CANDLES)
            warnings.append("candle timestamps are duplicated or out of order.")
            break

    if len(candles) < thresholds.min_completed_candles:
        failures.append(THIN_DATA)
    freshness_status = _freshness_status(
        candles=candles,
        now=now,
        timeframe=expected_timeframe,
        thresholds=thresholds,
        warnings=warnings,
    )
    if freshness_status in {"STALE", "FUTURE_TIMESTAMP", "MISSING_INTERVAL", "UNSUPPORTED_TIMEFRAME"}:
        failures.append(STALE_INPUT)
    if freshness_status in {"THIN_RANGE", "UNCHANGED_PRICES"}:
        failures.append(LOW_RANGE_QUALITY)

    latest = candles[-1].timestamp if candles else None
    return _ValidationResult(
        candles=tuple(candles),
        candles_received=len(rows),
        latest_candle_timestamp=latest,
        freshness_status=freshness_status,
        failure_reasons=tuple(_dedupe(failures)),
        warnings=tuple(warnings),
    )


def _freshness_status(
    *,
    candles: Sequence[NormalizedCandle],
    now: datetime,
    timeframe: str | None,
    thresholds: RegimeStateThresholds,
    warnings: list[str],
) -> str:
    if not candles:
        return "NO_COMPLETED_CANDLES"
    timeframe_delta = _timeframe_delta(timeframe or "")
    if timeframe_delta is None:
        warnings.append(f"unsupported primary timeframe: {timeframe}.")
        return "UNSUPPORTED_TIMEFRAME"
    latest = candles[-1].timestamp
    if latest > now + timeframe_delta:
        warnings.append(f"latest candle timestamp {latest.isoformat()} is too far ahead of generated_at.")
        return "FUTURE_TIMESTAMP"
    max_age = timeframe_delta * thresholds.stale_after_intervals
    if now - latest > max_age:
        warnings.append(f"latest completed candle is stale: age={now - latest}, allowed={max_age}.")
        return "STALE"
    max_gap = timeframe_delta * thresholds.max_missing_gap_intervals
    for previous, current in zip(candles, candles[1:]):
        if current.timestamp - previous.timestamp > max_gap:
            warnings.append(
                f"missing interval detected between {previous.timestamp.isoformat()} and {current.timestamp.isoformat()}."
            )
            return "MISSING_INTERVAL"
    context = candles[-min(len(candles), thresholds.preferred_completed_candles):]
    if _average([candle.range_pct for candle in context]) < thresholds.tiny_range_pct:
        warnings.append("recent candle ranges are tiny relative to price.")
        return "THIN_RANGE"
    unchanged = sum(
        1
        for previous, current in zip(context, context[1:])
        if abs(current.close - previous.close) <= max(current.close * 1e-9, 1e-9)
    )
    if context and unchanged >= max(1, len(context) - 1):
        warnings.append("recent closes are unchanged across the completed horizon.")
        return "UNCHANGED_PRICES"
    return "FRESH"


def _feature_summary(
    candles: Sequence[NormalizedCandle],
    *,
    thresholds: RegimeStateThresholds,
) -> dict[str, Any]:
    context = list(candles[-min(len(candles), thresholds.preferred_completed_candles):])
    fast = context[-min(len(context), thresholds.fast_window):]
    prior = context[: -len(fast)] or context
    ranges = [candle.range for candle in context]
    average_range = _average(ranges)
    fast_average_range = _average([candle.range for candle in fast])
    prior_average_range = _average([candle.range for candle in prior])
    range_expansion_ratio = fast_average_range / max(prior_average_range, 1e-9)
    net_change = context[-1].close - context[0].open
    slope_ratio = net_change / max(average_range, 1e-9)
    directions = [_direction(candle.close - candle.open) for candle in context]
    directional_bars = [direction for direction in directions if direction != 0]
    bullish_ratio = directions.count(1) / len(context)
    bearish_ratio = directions.count(-1) / len(context)
    alternation_ratio = _alternation_ratio(directional_bars)
    volume_values = [candle.volume for candle in context if candle.volume is not None]
    volume_available_ratio = len(volume_values) / len(context)
    average_volume = _average(volume_values) if volume_values else None
    return {
        "average_range": _round(average_range),
        "fast_average_range": _round(fast_average_range),
        "prior_average_range": _round(prior_average_range),
        "range_expansion_ratio": _round(range_expansion_ratio),
        "average_range_pct": _round(_average([candle.range_pct for candle in context])),
        "slope_ratio": _round(slope_ratio),
        "net_change": _round(net_change),
        "bullish_body_ratio": _round(bullish_ratio),
        "bearish_body_ratio": _round(bearish_ratio),
        "alternation_ratio": _round(alternation_ratio),
        "volume_available_ratio": _round(volume_available_ratio),
        "average_volume": None if average_volume is None else _round(average_volume),
    }


def _classify_states(features: Mapping[str, Any], candles: Sequence[NormalizedCandle]) -> dict[str, str]:
    range_ratio = float(features["range_expansion_ratio"])
    slope_ratio = float(features["slope_ratio"])
    bullish_ratio = float(features["bullish_body_ratio"])
    bearish_ratio = float(features["bearish_body_ratio"])
    alternation_ratio = float(features["alternation_ratio"])
    volume_available_ratio = float(features["volume_available_ratio"])

    if range_ratio >= 1.45:
        volatility_range_state = RANGE_EXPANDED
    elif range_ratio <= 0.75:
        volatility_range_state = RANGE_COMPRESSED
    else:
        volatility_range_state = RANGE_NORMAL

    if range_ratio >= 1.65:
        volatility_state = VOL_HIGH
    elif range_ratio <= 0.75:
        volatility_state = VOL_LOW
    else:
        volatility_state = VOL_NORMAL

    if slope_ratio >= 0.55 and bullish_ratio >= 0.65:
        trend_chop_state = EXPANSION_DIRECTIONAL if range_ratio >= 1.45 else TREND_UP
        directional_context = DIRECTIONAL_BULLISH
    elif slope_ratio <= -0.55 and bearish_ratio >= 0.65:
        trend_chop_state = EXPANSION_DIRECTIONAL if range_ratio >= 1.45 else TREND_DOWN
        directional_context = DIRECTIONAL_BEARISH
    elif range_ratio <= 0.75:
        trend_chop_state = COMPRESSION_COILING
        directional_context = DIRECTIONAL_BALANCED
    elif alternation_ratio >= 0.60 and abs(slope_ratio) <= 0.70:
        trend_chop_state = CHOP_BALANCED
        directional_context = DIRECTIONAL_BALANCED
    elif abs(slope_ratio) <= 0.25:
        trend_chop_state = CHOP_DIRECTIONLESS
        directional_context = DIRECTIONAL_MIXED
    else:
        trend_chop_state = TREND_MIXED
        directional_context = DIRECTIONAL_MIXED

    if volume_available_ratio and volume_available_ratio < 0.5:
        liquidity_state = LIQUIDITY_THIN_VOLUME
    elif _average([candle.range_pct for candle in candles]) < RegimeStateThresholds().tiny_range_pct * 2:
        liquidity_state = LIQUIDITY_THIN_RANGE
    else:
        liquidity_state = LIQUIDITY_NORMAL

    if trend_chop_state in {TREND_UP, TREND_DOWN}:
        market_regime_state = REGIME_TRENDING
    elif trend_chop_state == EXPANSION_DIRECTIONAL or volatility_range_state == RANGE_EXPANDED:
        market_regime_state = REGIME_EXPANSION
    elif trend_chop_state == COMPRESSION_COILING or volatility_range_state == RANGE_COMPRESSED:
        market_regime_state = REGIME_COMPRESSION
    elif trend_chop_state in {CHOP_BALANCED, CHOP_DIRECTIONLESS}:
        market_regime_state = REGIME_CHOP_BALANCED
    else:
        market_regime_state = REGIME_LOW_CONFIDENCE

    return {
        "market_regime_state": market_regime_state,
        "volatility_range_state": volatility_range_state,
        "volatility_state": volatility_state,
        "trend_chop_state": trend_chop_state,
        "liquidity_state": liquidity_state,
        "directional_context": directional_context,
    }


def _low_confidence_report(
    *,
    payload: Mapping[str, Any],
    generated_at: datetime,
    input_context: Mapping[str, Any],
    timeframe_context: Mapping[str, Any],
    validation: _ValidationResult,
) -> dict[str, Any]:
    session_phase = _session_phase(payload, validation.latest_candle_timestamp or generated_at)
    failures = _dedupe(validation.failure_reasons)
    liquidity_state = LIQUIDITY_THIN_RANGE if LOW_RANGE_QUALITY in failures else LIQUIDITY_STALE_OR_INCOMPLETE
    if MISSING_PROVENANCE in failures and not validation.candles:
        liquidity_state = LIQUIDITY_UNKNOWN
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "ADVISORY_REGIME_CONTEXT_ONLY",
        "instrument": _instrument(payload),
        "source_id": _source_id(payload),
        "generated_at": generated_at.isoformat(),
        **input_context,
        "source_provenance_status": _source_provenance_status(input_context),
        "freshness_status": validation.freshness_status,
        **timeframe_context,
        "latest_candle_timestamp": validation.latest_candle_timestamp.isoformat()
        if validation.latest_candle_timestamp
        else None,
        "candles_received": validation.candles_received,
        "completed_candles_used": len(validation.candles),
        "session_bucket": f"SESSION_{session_phase}",
        "session_phase": session_phase,
        "coarse_session_group": phase_coarse_session_group(session_phase),
        "market_regime_state": REGIME_THIN_OR_STALE if {STALE_INPUT, LOW_RANGE_QUALITY} & set(failures) else REGIME_LOW_CONFIDENCE,
        "volatility_range_state": RANGE_THIN_OR_INVALID,
        "volatility_state": VOL_LOW,
        "trend_chop_state": LOW_CONFIDENCE_TREND_CHOP,
        "liquidity_state": liquidity_state,
        "directional_context": DIRECTIONAL_UNKNOWN,
        "confidence": 0.0,
        "regime_reasons": _failure_reasons_text(failures),
        "warning_reasons": list(validation.warnings),
        "failure_reasons": failures,
        "feature_summary": _empty_feature_summary(),
        **_safety_flags(),
    }


def _input_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    source_path = _string_or_none(
        payload.get("input_source_path") or payload.get("source_path") or payload.get("source_file")
    )
    input_mode = _normalize_input_mode(_string_or_none(payload.get("input_mode")))
    source_category = _normalize_source_category(
        _string_or_none(payload.get("input_source_category"))
        or _string_or_none(payload.get("source_category"))
        or _infer_source_category(payload=payload, source_path=source_path)
    )
    return {
        "input_source_path": source_path,
        "input_source_category": source_category,
        "input_mode": input_mode,
    }


def _timeframe_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("timeframe_context"))
    primary_timeframe = _normalize_timeframe(
        str(
            raw.get("primary_timeframe")
            or raw.get("regime_evaluation_timeframe")
            or payload.get("primary_timeframe")
            or payload.get("timeframe")
            or payload.get("candle_timeframe")
            or ""
        )
    )
    context_timeframe = _normalize_timeframe(
        str(raw.get("context_timeframe") or payload.get("context_timeframe") or primary_timeframe or "")
    )
    return {
        "primary_timeframe": primary_timeframe or None,
        "context_timeframe": context_timeframe or None,
        "timeframe_source": _string_or_none(raw.get("timeframe_source") or payload.get("timeframe_source")),
        "base_timeframe_if_derived": _string_or_none(
            raw.get("base_timeframe_if_derived") or payload.get("base_timeframe_if_derived")
        ),
        "aggregation_method": _string_or_none(raw.get("aggregation_method") or payload.get("aggregation_method")),
        "anchor_rule": _string_or_none(raw.get("anchor_rule") or payload.get("anchor_rule")),
        "timeframe_alignment_status": _string_or_none(
            raw.get("timeframe_alignment_status") or payload.get("timeframe_alignment_status")
        ),
    }


def _timeframe_failures(context: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    if not context["primary_timeframe"] or not _timeframe_delta(str(context["primary_timeframe"])):
        failures.append(MISSING_TIMEFRAME_SCHEMA)
    alignment = str(context.get("timeframe_alignment_status") or "").upper()
    if alignment in {"MIXED", TIMEFRAME_ALIGNMENT_MIXED_BLOCKED}:
        failures.append(MIXED_TIMEFRAME)
    source = str(context.get("timeframe_source") or "").upper()
    if source == TIMEFRAME_SOURCE_DERIVED and (
        not context.get("base_timeframe_if_derived") or not context.get("aggregation_method")
    ):
        failures.append(AMBIGUOUS_TIMEFRAME)
    return _dedupe(failures)


def _provenance_failures(input_context: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    category = str(input_context.get("input_source_category") or SOURCE_CATEGORY_UNKNOWN)
    mode = str(input_context.get("input_mode") or INPUT_MODE_UNKNOWN)
    path = input_context.get("input_source_path")
    if not path or category == SOURCE_CATEGORY_UNKNOWN:
        failures.append(MISSING_PROVENANCE)
    if mode == INPUT_MODE_RUNTIME_DECISION and category == SOURCE_CATEGORY_RESEARCH:
        failures.append(RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE)
    if mode == INPUT_MODE_RUNTIME_DECISION and category != SOURCE_CATEGORY_RUNTIME:
        failures.append(MISSING_PROVENANCE)
    return _dedupe(failures)


def _source_provenance_status(input_context: Mapping[str, Any]) -> str:
    category = str(input_context.get("input_source_category") or SOURCE_CATEGORY_UNKNOWN)
    mode = str(input_context.get("input_mode") or INPUT_MODE_UNKNOWN)
    if category == SOURCE_CATEGORY_RUNTIME:
        return SOURCE_PROVENANCE_RUNTIME_DECLARED
    if category == SOURCE_CATEGORY_RESEARCH:
        return SOURCE_PROVENANCE_RESEARCH_NOT_RUNTIME_ELIGIBLE
    if category == SOURCE_CATEGORY_TEST_FIXTURE:
        return SOURCE_PROVENANCE_TEST_NOT_RUNTIME_ELIGIBLE
    if mode == INPUT_MODE_RUNTIME_DECISION:
        return SOURCE_PROVENANCE_RUNTIME_NOT_DECLARED
    return SOURCE_PROVENANCE_UNKNOWN


def _session_phase(payload: Mapping[str, Any], timestamp: datetime) -> str:
    declared = _string_or_none(payload.get("session_phase") or payload.get("session_label"))
    if declared:
        return declared.upper()
    return label_session_phase(timestamp)


def _confidence(
    *,
    features: Mapping[str, Any],
    completed_count: int,
    thresholds: RegimeStateThresholds,
) -> float:
    data_score = min(completed_count / thresholds.preferred_completed_candles, 1.0)
    range_score = min(max(float(features["range_expansion_ratio"]), 1.0 / max(float(features["range_expansion_ratio"]), 1e-9)) / 1.75, 1.0)
    direction_score = max(float(features["bullish_body_ratio"]), float(features["bearish_body_ratio"]), float(features["alternation_ratio"]))
    return _clamp((0.45 * data_score) + (0.25 * range_score) + (0.30 * direction_score), 0.0, 1.0)


def _regime_reasons(states: Mapping[str, str], features: Mapping[str, Any]) -> list[str]:
    return [
        (
            "evaluated completed candles using session label, range expansion, close-to-close slope, "
            "body direction persistence, alternation, and volume availability."
        ),
        (
            f"market_regime_state={states['market_regime_state']}; trend_chop_state={states['trend_chop_state']}; "
            f"range_expansion_ratio={features['range_expansion_ratio']}; slope_ratio={features['slope_ratio']}."
        ),
    ]


def _failure_reasons_text(failures: Sequence[str]) -> list[str]:
    text = {
        THIN_DATA: "insufficient completed candles for regime/session classification.",
        STALE_INPUT: "completed candle input is stale, future-dated, or has missing intervals.",
        INCOMPLETE_CANDLES: "input contains incomplete candles.",
        MIXED_TIMEFRAME: "input contains mixed timeframe candles or schema.",
        MISSING_TIMEFRAME_SCHEMA: "timeframe metadata is missing or unsupported.",
        AMBIGUOUS_TIMEFRAME: "derived timeframe metadata is ambiguous.",
        MALFORMED_CANDLES: "input contains malformed candle fields or invalid OHLC geometry.",
        LOW_RANGE_QUALITY: "recent candle ranges are too low quality for regime classification.",
        MISSING_PROVENANCE: "input provenance is missing or not runtime-declared.",
        RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE: "research source was presented as runtime truth and was rejected.",
    }
    return [text.get(reason, f"regime classification failed due to {reason}.") for reason in failures]


def _empty_feature_summary() -> dict[str, Any]:
    return {
        "average_range": 0.0,
        "fast_average_range": 0.0,
        "prior_average_range": 0.0,
        "range_expansion_ratio": 0.0,
        "average_range_pct": 0.0,
        "slope_ratio": 0.0,
        "net_change": 0.0,
        "bullish_body_ratio": 0.0,
        "bearish_body_ratio": 0.0,
        "alternation_ratio": 0.0,
        "volume_available_ratio": 0.0,
        "average_volume": None,
    }


def _safety_flags() -> dict[str, bool]:
    return {
        "strategy_authority": False,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "order_intent_created": False,
        "lifecycle_mutated": False,
        "runtime_trade_eligible": False,
    }


def _extract_candle_rows(payload: Mapping[str, Any]) -> Any:
    for key in ("candles", "ohlcv_candles", "bars", "completed_candles"):
        if key in payload:
            return payload[key]
    return None


def _normalize_timeframe(value: str) -> str:
    text = str(value or "").strip().lower()
    aliases = {
        "1min": "1m",
        "1minute": "1m",
        "5min": "5m",
        "5minute": "5m",
        "15min": "15m",
        "15minute": "15m",
        "60min": "60m",
        "1h": "60m",
    }
    return aliases.get(text, text)


def _timeframe_delta(timeframe: str) -> timedelta | None:
    normalized = _normalize_timeframe(timeframe)
    if not normalized.endswith("m"):
        return None
    try:
        minutes = int(normalized[:-1])
    except ValueError:
        return None
    if minutes <= 0:
        return None
    return timedelta(minutes=minutes)


def _coerce_now(now: datetime | str | None) -> datetime:
    if now is None:
        return datetime.now(UTC)
    if isinstance(now, datetime):
        return now.astimezone(UTC) if now.tzinfo else now.replace(tzinfo=UTC)
    return _parse_timestamp(now)


def _parse_timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid timestamp {value!r}") from exc
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _finite_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"expected finite number, received {value!r}") from exc
    if not math.isfinite(number):
        raise ValueError(f"expected finite number, received {value!r}")
    return number


def _optional_finite_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _finite_float(value)


def _instrument(payload: Mapping[str, Any]) -> str:
    return str(payload.get("instrument") or payload.get("symbol") or "UNKNOWN").strip().upper() or "UNKNOWN"


def _source_id(payload: Mapping[str, Any]) -> str:
    return str(payload.get("source_id") or "track_b_regime_state_offline").strip()


def _normalize_input_mode(value: str | None) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {INPUT_MODE_RUNTIME_DECISION, INPUT_MODE_OFFLINE_EVALUATION, INPUT_MODE_REPLAY_RESEARCH, INPUT_MODE_TEST}:
        return normalized
    return INPUT_MODE_UNKNOWN


def _normalize_source_category(value: str | None) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {SOURCE_CATEGORY_RUNTIME, SOURCE_CATEGORY_RESEARCH, SOURCE_CATEGORY_TEST_FIXTURE}:
        return normalized
    return SOURCE_CATEGORY_UNKNOWN


def _infer_source_category(*, payload: Mapping[str, Any], source_path: str | None) -> str:
    normalized_path = str(source_path or "").replace("\\", "/").lower()
    if bool(payload.get("test_fixture")):
        return SOURCE_CATEGORY_TEST_FIXTURE
    if bool(payload.get("runtime_provenance")) or bool(payload.get("intended_for_runtime_decision")):
        return SOURCE_CATEGORY_RUNTIME
    if "/outputs/track_b_research/" in f"/{normalized_path}" or normalized_path.startswith("outputs/track_b_research/"):
        return SOURCE_CATEGORY_RESEARCH
    return SOURCE_CATEGORY_UNKNOWN


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _average(values: Sequence[float]) -> float:
    numeric = [float(value) for value in values if math.isfinite(float(value))]
    if not numeric:
        return 0.0
    return sum(numeric) / len(numeric)


def _alternation_ratio(directions: Sequence[int]) -> float:
    if len(directions) < 2:
        return 0.0
    alternations = sum(1 for previous, current in zip(directions, directions[1:]) if previous != current)
    return alternations / (len(directions) - 1)


def _direction(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _round(value: float, digits: int = 6) -> float:
    return round(float(value), digits)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))
