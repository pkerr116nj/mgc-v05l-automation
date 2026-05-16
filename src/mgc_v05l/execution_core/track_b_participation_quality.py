"""Offline Track B participation / exit-quality classifier.

The layer consumes completed 5-minute OHLCV candles and emits descriptive
market-quality context only. It has no strategy authority and no broker
authority.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "track_b_participation_quality_state_v1"
PRODUCER_NAME = "TrackBParticipationQualityLayer"
DEFAULT_CANDLE_TIMEFRAME = "5m"
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PARTICIPATION_QUALITY_ARTIFACT_PATH = (
    REPO_ROOT
    / "outputs"
    / "track_b_execution_core"
    / "participation_quality"
    / "latest_participation_quality_state.json"
)

PERSISTENT_BULLISH_PRESSURE = "PERSISTENT_BULLISH_PRESSURE"
PERSISTENT_BEARISH_PRESSURE = "PERSISTENT_BEARISH_PRESSURE"
BULLISH_IMPULSE_ONLY = "BULLISH_IMPULSE_ONLY"
BEARISH_IMPULSE_ONLY = "BEARISH_IMPULSE_ONLY"
BULLISH_IMPULSE_DECAYING = "BULLISH_IMPULSE_DECAYING"
BEARISH_IMPULSE_DECAYING = "BEARISH_IMPULSE_DECAYING"
BULLISH_HEALTHY_PULLBACK = "BULLISH_HEALTHY_PULLBACK"
BEARISH_HEALTHY_PULLBACK = "BEARISH_HEALTHY_PULLBACK"
BULLISH_CONTINUATION_CONFIRMED = "BULLISH_CONTINUATION_CONFIRMED"
BEARISH_CONTINUATION_CONFIRMED = "BEARISH_CONTINUATION_CONFIRMED"
BULLISH_PARTICIPATION_COLLAPSE = "BULLISH_PARTICIPATION_COLLAPSE"
BEARISH_PARTICIPATION_COLLAPSE = "BEARISH_PARTICIPATION_COLLAPSE"
CHOP_BALANCED = "CHOP_BALANCED"
LOW_CONFIDENCE_THIN_DATA = "LOW_CONFIDENCE_THIN_DATA"

THIN_DATA = "THIN_DATA"
STALE_INPUT = "STALE_INPUT"
RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE = "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"
INCOMPLETE_CANDLES = "INCOMPLETE_CANDLES"
MIXED_TIMEFRAME = "MIXED_TIMEFRAME"
MALFORMED_CANDLES = "MALFORMED_CANDLES"
LOW_RANGE_QUALITY = "LOW_RANGE_QUALITY"
MISSING_PROVENANCE = "MISSING_PROVENANCE"

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

CONFIDENCE_FAILURE_REASONS = {
    THIN_DATA,
    STALE_INPUT,
    RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE,
    INCOMPLETE_CANDLES,
    MIXED_TIMEFRAME,
    MALFORMED_CANDLES,
    LOW_RANGE_QUALITY,
    MISSING_PROVENANCE,
}


@dataclass(frozen=True)
class ParticipationQualityThresholds:
    min_completed_candles: int = 12
    preferred_completed_candles: int = 24
    fast_window: int = 3
    impulse_window: int = 6
    context_window: int = 24
    stale_after_intervals: float = 3.0
    max_missing_gap_intervals: float = 1.5
    tiny_range_pct: float = 0.00005
    neutral_pressure: float = 0.14
    persistent_pressure: float = 0.32
    persistent_ratio: float = 0.67
    impulse_pressure: float = 0.62
    pullback_min_depth: float = 0.12
    pullback_max_healthy_depth: float = 0.58
    collapse_depth: float = 0.72
    recovery_confirmed: float = 0.42
    hostile_recent_pressure: float = 0.22


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
    def body_strength(self) -> float:
        return abs(self.body) / self.range

    @property
    def close_location(self) -> float:
        return (self.close - self.low) / self.range

    @property
    def range_pct(self) -> float:
        return self.range / self.close


@dataclass(frozen=True)
class _ValidationResult:
    candles: tuple[NormalizedCandle, ...]
    freshness_status: str
    confidence_failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def build_participation_quality_state(
    payload: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
    candle_timeframe: str | None = None,
    input_mode: str | None = None,
    input_source_path: str | Path | None = None,
    input_source_category: str | None = None,
    thresholds: ParticipationQualityThresholds | None = None,
) -> dict[str, Any]:
    """Build a deterministic offline participation-quality report."""

    resolved_now = _coerce_now(now)
    resolved_thresholds = thresholds or ParticipationQualityThresholds()
    timeframe = _normalize_timeframe(
        candle_timeframe
        or str(payload.get("candle_timeframe") or payload.get("timeframe") or DEFAULT_CANDLE_TIMEFRAME)
    )
    input_context = track_b_input_context(
        payload,
        input_mode=input_mode,
        input_source_path=input_source_path,
        input_source_category=input_source_category,
    )
    instrument = _normalize_instrument(payload)
    raw_candles = _extract_candle_rows(payload)
    candles_received = len(raw_candles) if _is_sequence(raw_candles) else 0
    provenance_failures = _provenance_failure_reasons(input_context)

    if not _is_sequence(raw_candles):
        validation = _ValidationResult(
            candles=(),
            freshness_status="NO_COMPLETED_CANDLES",
            confidence_failure_reasons=tuple([*provenance_failures, THIN_DATA, MALFORMED_CANDLES]),
            warnings=("candle input must be a sequence under candles, ohlcv_candles, or bars.",),
        )
        return _low_confidence_report(
            payload=payload,
            instrument=instrument,
            generated_at=resolved_now,
            timeframe=timeframe,
            candles_received=0,
            validation=validation,
            input_context=input_context,
            state_reasons=("missing explicit completed 5m candle sequence.",),
        )

    validation = _validate_completed_5m_candles(
        raw_candles,
        now=resolved_now,
        expected_timeframe=timeframe,
        thresholds=resolved_thresholds,
        provenance_failure_reasons=provenance_failures,
    )
    if validation.confidence_failure_reasons:
        return _low_confidence_report(
            payload=payload,
            instrument=instrument,
            generated_at=resolved_now,
            timeframe=timeframe,
            candles_received=candles_received,
            validation=validation,
            input_context=input_context,
            state_reasons=_failure_state_reasons(validation.confidence_failure_reasons),
        )

    features = _calculate_features(validation.candles, thresholds=resolved_thresholds)
    state = _classify_participation_state(features, thresholds=resolved_thresholds)
    confidence = _state_confidence(state, features, completed_count=len(validation.candles), thresholds=resolved_thresholds)
    qualities = _quality_context(state)

    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "QUALITY_CONTEXT_ONLY",
        **input_context,
        "source_id": _source_id(payload),
        "instrument": instrument,
        "generated_at": resolved_now.isoformat(),
        "candle_timeframe": timeframe,
        "candles_received": candles_received,
        "completed_candles_used": len(validation.candles),
        "latest_candle_timestamp": validation.candles[-1].timestamp.isoformat(),
        "latest_input_timestamp": validation.candles[-1].timestamp.isoformat(),
        "freshness_status": validation.freshness_status,
        "source_provenance_status": source_provenance_status(input_context),
        "participation_state": state,
        "long_hold_quality": qualities["long_hold_quality"],
        "short_hold_quality": qualities["short_hold_quality"],
        "long_exit_urgency_context": qualities["long_exit_urgency_context"],
        "short_exit_urgency_context": qualities["short_exit_urgency_context"],
        "continuation_confidence": _round(features["continuation_confidence"]),
        "pullback_health": qualities["pullback_health"],
        "confidence": _round(confidence),
        "confidence_state": _confidence_state(confidence),
        "confidence_failure_reasons": [],
        "feature_summary": _round_feature_summary(features),
        "state_reasons": _state_reasons(state, features),
        "warnings": list(validation.warnings),
        **_safety_flags(),
    }


def write_participation_quality_state(
    payload: Mapping[str, Any],
    *,
    output_path: str | Path | None = None,
    now: datetime | str | None = None,
    candle_timeframe: str | None = None,
    input_mode: str | None = None,
    input_source_path: str | Path | None = None,
    input_source_category: str | None = None,
    thresholds: ParticipationQualityThresholds | None = None,
) -> dict[str, Any]:
    """Build and write the latest artifact only when explicitly invoked."""

    resolved_path = Path(output_path or DEFAULT_PARTICIPATION_QUALITY_ARTIFACT_PATH).resolve()
    report = build_participation_quality_state(
        payload,
        now=now,
        candle_timeframe=candle_timeframe,
        input_mode=input_mode,
        input_source_path=input_source_path,
        input_source_category=input_source_category,
        thresholds=thresholds,
    )
    report = {**report, "artifact_path": str(resolved_path)}
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _validate_completed_5m_candles(
    rows: Sequence[Any],
    *,
    now: datetime,
    expected_timeframe: str,
    thresholds: ParticipationQualityThresholds,
    provenance_failure_reasons: Sequence[str],
) -> _ValidationResult:
    candles: list[NormalizedCandle] = []
    warnings: list[str] = []
    failures = list(provenance_failure_reasons)
    skipped_incomplete = 0
    mixed_timeframe_count = 0

    if expected_timeframe != DEFAULT_CANDLE_TIMEFRAME:
        failures.append(MIXED_TIMEFRAME)
        warnings.append(f"V1 lead timeframe must be {DEFAULT_CANDLE_TIMEFRAME}; received {expected_timeframe}.")

    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] must be an object.")
            continue
        if row.get("completed") is False:
            skipped_incomplete += 1
            continue
        row_timeframe = _normalize_timeframe(str(row.get("timeframe") or ""))
        if row_timeframe != DEFAULT_CANDLE_TIMEFRAME:
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

    if skipped_incomplete:
        warnings.append(f"excluded {skipped_incomplete} incomplete candle(s).")
    if mixed_timeframe_count:
        failures.append(MIXED_TIMEFRAME)
        warnings.append(f"rejected input with {mixed_timeframe_count} non-5m completed candle(s).")
    for previous, current in zip(candles, candles[1:]):
        if current.timestamp <= previous.timestamp:
            failures.append(MALFORMED_CANDLES)
            warnings.append("candle timestamps are duplicated or out of order.")
            break

    if skipped_incomplete and len(candles) < thresholds.min_completed_candles:
        failures.append(INCOMPLETE_CANDLES)
    if len(candles) < thresholds.min_completed_candles:
        failures.append(THIN_DATA)
    freshness_status = _freshness_status(candles=candles, now=now, thresholds=thresholds, warnings=warnings)
    if freshness_status in {"STALE", "FUTURE_TIMESTAMP", "MISSING_INTERVAL"}:
        failures.append(STALE_INPUT)
    if freshness_status in {"THIN_RANGE", "UNCHANGED_PRICES"}:
        failures.append(LOW_RANGE_QUALITY)

    return _ValidationResult(
        candles=tuple(candles),
        freshness_status=freshness_status,
        confidence_failure_reasons=tuple(_dedupe(failures)),
        warnings=tuple(warnings),
    )


def _freshness_status(
    *,
    candles: Sequence[NormalizedCandle],
    now: datetime,
    thresholds: ParticipationQualityThresholds,
    warnings: list[str],
) -> str:
    if not candles:
        return "NO_COMPLETED_CANDLES"
    timeframe_delta = _timeframe_delta(DEFAULT_CANDLE_TIMEFRAME)
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
                f"missing 5m interval detected between {previous.timestamp.isoformat()} and {current.timestamp.isoformat()}."
            )
            return "MISSING_INTERVAL"
    context = candles[-min(len(candles), thresholds.context_window):]
    if _average([candle.range_pct for candle in context]) < thresholds.tiny_range_pct:
        warnings.append("recent candle ranges are tiny relative to price.")
        return "THIN_RANGE"
    unchanged_count = sum(
        1
        for previous, current in zip(context, context[1:])
        if abs(current.close - previous.close) <= max(current.close * 1e-9, 1e-9)
    )
    if context and unchanged_count >= max(1, len(context) - 1):
        warnings.append("recent closes are unchanged across the completed horizon.")
        return "UNCHANGED_PRICES"
    return "FRESH"


def _calculate_features(
    candles: Sequence[NormalizedCandle],
    *,
    thresholds: ParticipationQualityThresholds,
) -> dict[str, Any]:
    context = list(candles[-min(len(candles), thresholds.context_window):])
    fast = context[-thresholds.fast_window:]
    prior_context = context[: -thresholds.fast_window] or context
    per_candle = _per_candle_features(context)
    net_pressures = [row["net_pressure"] for row in per_candle]
    fast_pressures = net_pressures[-thresholds.fast_window:]
    prior_pressures = net_pressures[: -thresholds.fast_window] or net_pressures
    bullish_ratio = sum(1 for value in net_pressures if value >= 0.20) / len(net_pressures)
    bearish_ratio = sum(1 for value in net_pressures if value <= -0.20) / len(net_pressures)
    net_pressure = _average(net_pressures)
    fast_net_pressure = _average(fast_pressures)
    prior_net_pressure = _average(prior_pressures)
    alternation_score = _alternation_score(net_pressures)
    average_abs_pressure = _average([abs(value) for value in net_pressures])
    range_expansion_ratio = _average([candle.range for candle in fast]) / max(
        _average([candle.range for candle in prior_context]),
        1e-9,
    )
    impulse = _impulse_profile(context)
    pullback = _pullback_profile(context, impulse["direction"], fast_window=thresholds.fast_window)
    decay = _decay_profile(
        direction=impulse["direction"],
        impulse_strength=impulse["strength"],
        fast_net_pressure=fast_net_pressure,
        prior_net_pressure=prior_net_pressure,
        range_expansion_ratio=range_expansion_ratio,
    )
    continuation_confidence = _continuation_confidence(
        direction=impulse["direction"],
        pullback=pullback,
        fast_net_pressure=fast_net_pressure,
        latest_close_location=context[-1].close_location,
    )
    volume_reliable = _volume_reliable(context)
    return {
        "body_strength": _average([candle.body_strength for candle in context]),
        "close_location_in_range": _average([candle.close_location for candle in context]),
        "close_to_close_pressure": _close_to_close_pressure(context),
        "directional_persistence": max(bullish_ratio, bearish_ratio),
        "bullish_persistence": bullish_ratio,
        "bearish_persistence": bearish_ratio,
        "net_pressure": net_pressure,
        "fast_net_pressure": fast_net_pressure,
        "prior_net_pressure": prior_net_pressure,
        "average_abs_pressure": average_abs_pressure,
        "alternation_score": alternation_score,
        "range_expansion_ratio": range_expansion_ratio,
        "pullback_depth": pullback["depth"],
        "pullback_recovery": pullback["recovery"],
        "pullback_direction": pullback["direction"],
        "impulse_direction": impulse["direction"],
        "impulse_strength": impulse["strength"],
        "impulse_position": impulse["position"],
        "impulse_decay": decay,
        "continuation_confidence": continuation_confidence,
        "volume_confirmation": "RELIABLE" if volume_reliable else "UNAVAILABLE_OR_UNRELIABLE",
        "latest_close_location": context[-1].close_location,
        "per_candle_pressure_tail": per_candle[-thresholds.min_completed_candles:],
    }


def _classify_participation_state(
    features: Mapping[str, Any],
    *,
    thresholds: ParticipationQualityThresholds,
) -> str:
    impulse_direction = str(features["impulse_direction"])
    impulse_strength = float(features["impulse_strength"])
    impulse_position = str(features["impulse_position"])
    fast_net = float(features["fast_net_pressure"])
    net_pressure = float(features["net_pressure"])
    directional_persistence = float(features["directional_persistence"])
    alternation_score = float(features["alternation_score"])
    average_abs_pressure = float(features["average_abs_pressure"])
    pullback_depth = float(features["pullback_depth"])
    pullback_recovery = float(features["pullback_recovery"])
    range_expansion = float(features["range_expansion_ratio"])
    decay = float(features["impulse_decay"])

    if alternation_score >= 0.75 and abs(net_pressure) <= thresholds.neutral_pressure:
        return CHOP_BALANCED
    if (
        net_pressure >= thresholds.persistent_pressure
        and directional_persistence >= thresholds.persistent_ratio
        and fast_net >= thresholds.neutral_pressure
    ):
        return PERSISTENT_BULLISH_PRESSURE
    if (
        net_pressure <= -thresholds.persistent_pressure
        and directional_persistence >= thresholds.persistent_ratio
        and fast_net <= -thresholds.neutral_pressure
    ):
        return PERSISTENT_BEARISH_PRESSURE
    if impulse_direction == "BULLISH" and impulse_strength >= thresholds.impulse_pressure:
        if impulse_position == "LATEST":
            return BULLISH_IMPULSE_ONLY
        if pullback_depth >= thresholds.collapse_depth or (
            fast_net <= -thresholds.hostile_recent_pressure and range_expansion >= 1.05
        ):
            return BULLISH_PARTICIPATION_COLLAPSE
        if pullback_recovery >= thresholds.recovery_confirmed and fast_net >= thresholds.hostile_recent_pressure:
            return BULLISH_CONTINUATION_CONFIRMED
        if (
            thresholds.pullback_min_depth <= pullback_depth <= thresholds.pullback_max_healthy_depth
            and pullback_recovery >= 0.12
            and fast_net > -thresholds.hostile_recent_pressure
        ):
            return BULLISH_HEALTHY_PULLBACK
        if impulse_position != "LATEST" and decay >= 0.35:
            return BULLISH_IMPULSE_DECAYING
        return BULLISH_IMPULSE_ONLY
    if impulse_direction == "BEARISH" and impulse_strength >= thresholds.impulse_pressure:
        if impulse_position == "LATEST":
            return BEARISH_IMPULSE_ONLY
        if pullback_depth >= thresholds.collapse_depth or (
            fast_net >= thresholds.hostile_recent_pressure and range_expansion >= 1.05
        ):
            return BEARISH_PARTICIPATION_COLLAPSE
        if pullback_recovery >= thresholds.recovery_confirmed and fast_net <= -thresholds.hostile_recent_pressure:
            return BEARISH_CONTINUATION_CONFIRMED
        if (
            thresholds.pullback_min_depth <= pullback_depth <= thresholds.pullback_max_healthy_depth
            and pullback_recovery >= 0.12
            and fast_net < thresholds.hostile_recent_pressure
        ):
            return BEARISH_HEALTHY_PULLBACK
        if impulse_position != "LATEST" and decay >= 0.35:
            return BEARISH_IMPULSE_DECAYING
        return BEARISH_IMPULSE_ONLY
    if (
        abs(net_pressure) <= thresholds.neutral_pressure
        and (alternation_score >= 0.55 or average_abs_pressure <= 0.28)
    ):
        return CHOP_BALANCED
    if alternation_score >= 0.45 or abs(net_pressure) <= thresholds.neutral_pressure:
        return CHOP_BALANCED
    if net_pressure > 0:
        return PERSISTENT_BULLISH_PRESSURE
    return PERSISTENT_BEARISH_PRESSURE


def _per_candle_features(candles: Sequence[NormalizedCandle]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, candle in enumerate(candles):
        prior_close = candles[index - 1].close if index > 0 else None
        body_pressure = math.copysign(candle.body_strength, candle.body) if candle.body != 0 else 0.0
        close_location_pressure = (candle.close_location - 0.5) * 2.0
        ctc_pressure = 0.0
        if prior_close is not None:
            ctc_pressure = _clamp((candle.close - prior_close) / candle.range, -1.0, 1.0)
        net_pressure = _clamp(
            (0.45 * body_pressure) + (0.35 * close_location_pressure) + (0.20 * ctc_pressure),
            -1.0,
            1.0,
        )
        rows.append(
            {
                "timestamp": candle.timestamp.isoformat(),
                "body_strength": candle.body_strength,
                "close_location_in_range": candle.close_location,
                "close_to_close_pressure": ctc_pressure,
                "net_pressure": net_pressure,
            }
        )
    return rows


def _impulse_profile(context: Sequence[NormalizedCandle]) -> dict[str, Any]:
    pressures = _per_candle_features(context)
    if not pressures:
        return {"direction": "NONE", "strength": 0.0, "position": "NONE"}
    values = [row["net_pressure"] for row in pressures]
    strongest_value = max(values, key=lambda value: abs(value))
    strength = abs(float(strongest_value))
    direction = "BULLISH" if strongest_value > 0 else "BEARISH"
    if strength < 0.20:
        direction = "NONE"
    strongest_index = values.index(strongest_value)
    position = "LATEST" if strongest_index >= len(values) - 2 else "PRIOR"
    return {"direction": direction, "strength": strength, "position": position}


def _pullback_profile(context: Sequence[NormalizedCandle], direction: str, *, fast_window: int) -> dict[str, Any]:
    closes = [candle.close for candle in context]
    impulse_search_end = max(1, len(context) - fast_window)
    impulse_context = context[:impulse_search_end]
    if direction == "BULLISH":
        high_index = max(range(len(impulse_context)), key=lambda index: impulse_context[index].high)
        prior_low = min(candle.low for candle in context[: high_index + 1])
        impulse_range = max(context[high_index].high - prior_low, 1e-9)
        post = context[high_index + 1:]
        pullback_low = min((candle.low for candle in post), default=context[high_index].low)
        depth = _clamp((context[high_index].high - pullback_low) / impulse_range, 0.0, 2.0)
        recovery = _clamp((closes[-1] - pullback_low) / impulse_range, 0.0, 2.0)
        return {"direction": "BULLISH", "depth": depth, "recovery": recovery}
    if direction == "BEARISH":
        low_index = min(range(len(impulse_context)), key=lambda index: impulse_context[index].low)
        prior_high = max(candle.high for candle in context[: low_index + 1])
        impulse_range = max(prior_high - context[low_index].low, 1e-9)
        post = context[low_index + 1:]
        pullback_high = max((candle.high for candle in post), default=context[low_index].high)
        depth = _clamp((pullback_high - context[low_index].low) / impulse_range, 0.0, 2.0)
        recovery = _clamp((pullback_high - closes[-1]) / impulse_range, 0.0, 2.0)
        return {"direction": "BEARISH", "depth": depth, "recovery": recovery}
    return {"direction": "NONE", "depth": 0.0, "recovery": 0.0}


def _decay_profile(
    *,
    direction: str,
    impulse_strength: float,
    fast_net_pressure: float,
    prior_net_pressure: float,
    range_expansion_ratio: float,
) -> float:
    if direction == "BULLISH":
        pressure_decay = max(0.0, prior_net_pressure - fast_net_pressure)
    elif direction == "BEARISH":
        pressure_decay = max(0.0, fast_net_pressure - prior_net_pressure)
    else:
        pressure_decay = 0.0
    range_decay = max(0.0, 1.0 - min(range_expansion_ratio, 1.0))
    return _clamp((0.55 * pressure_decay) + (0.30 * max(0.0, impulse_strength - abs(fast_net_pressure))) + (0.15 * range_decay), 0.0, 1.0)


def _continuation_confidence(
    *,
    direction: str,
    pullback: Mapping[str, float | str],
    fast_net_pressure: float,
    latest_close_location: float,
) -> float:
    depth = float(pullback["depth"])
    recovery = float(pullback["recovery"])
    if direction == "BULLISH":
        directional_fast = max(fast_net_pressure, 0.0)
        close_score = latest_close_location
    elif direction == "BEARISH":
        directional_fast = max(-fast_net_pressure, 0.0)
        close_score = 1.0 - latest_close_location
    else:
        return 0.0
    depth_score = 1.0 - min(abs(depth - 0.35) / 0.70, 1.0)
    return _clamp((0.35 * directional_fast) + (0.35 * recovery) + (0.20 * close_score) + (0.10 * depth_score), 0.0, 1.0)


def _quality_context(state: str) -> dict[str, str]:
    mapping = {
        PERSISTENT_BULLISH_PRESSURE: ("SUPPORTIVE", "HOSTILE", "LOW", "HIGH", "NONE"),
        PERSISTENT_BEARISH_PRESSURE: ("HOSTILE", "SUPPORTIVE", "HIGH", "LOW", "NONE"),
        BULLISH_IMPULSE_ONLY: ("SUPPORTIVE", "HOSTILE", "LOW", "ELEVATED", "NONE"),
        BEARISH_IMPULSE_ONLY: ("HOSTILE", "SUPPORTIVE", "ELEVATED", "LOW", "NONE"),
        BULLISH_IMPULSE_DECAYING: ("DEGRADING", "NEUTRAL", "ELEVATED", "LOW", "QUESTIONABLE"),
        BEARISH_IMPULSE_DECAYING: ("NEUTRAL", "DEGRADING", "LOW", "ELEVATED", "QUESTIONABLE"),
        BULLISH_HEALTHY_PULLBACK: ("SUPPORTIVE", "HOSTILE", "LOW", "ELEVATED", "HEALTHY"),
        BEARISH_HEALTHY_PULLBACK: ("HOSTILE", "SUPPORTIVE", "ELEVATED", "LOW", "HEALTHY"),
        BULLISH_CONTINUATION_CONFIRMED: ("SUPPORTIVE", "HOSTILE", "LOW", "HIGH", "HEALTHY"),
        BEARISH_CONTINUATION_CONFIRMED: ("HOSTILE", "SUPPORTIVE", "HIGH", "LOW", "HEALTHY"),
        BULLISH_PARTICIPATION_COLLAPSE: ("HOSTILE", "NEUTRAL", "HIGH", "LOW", "FAILED"),
        BEARISH_PARTICIPATION_COLLAPSE: ("NEUTRAL", "HOSTILE", "LOW", "HIGH", "FAILED"),
        CHOP_BALANCED: ("NEUTRAL", "NEUTRAL", "LOW", "LOW", "NONE"),
    }
    long_hold, short_hold, long_urgency, short_urgency, pullback_health = mapping.get(
        state,
        ("UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN", "UNKNOWN"),
    )
    return {
        "long_hold_quality": long_hold,
        "short_hold_quality": short_hold,
        "long_exit_urgency_context": long_urgency,
        "short_exit_urgency_context": short_urgency,
        "pullback_health": pullback_health,
    }


def _low_confidence_report(
    *,
    payload: Mapping[str, Any],
    instrument: str,
    generated_at: datetime,
    timeframe: str,
    candles_received: int,
    validation: _ValidationResult,
    input_context: Mapping[str, Any],
    state_reasons: Sequence[str],
) -> dict[str, Any]:
    latest = validation.candles[-1].timestamp if validation.candles else None
    qualities = _quality_context(LOW_CONFIDENCE_THIN_DATA)
    reasons = _dedupe(validation.confidence_failure_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "QUALITY_CONTEXT_ONLY",
        **input_context,
        "source_id": _source_id(payload),
        "instrument": instrument,
        "generated_at": generated_at.isoformat(),
        "candle_timeframe": timeframe,
        "candles_received": candles_received,
        "completed_candles_used": len(validation.candles),
        "latest_candle_timestamp": latest.isoformat() if latest is not None else None,
        "latest_input_timestamp": latest.isoformat() if latest is not None else None,
        "freshness_status": validation.freshness_status,
        "source_provenance_status": source_provenance_status(input_context),
        "participation_state": LOW_CONFIDENCE_THIN_DATA,
        "long_hold_quality": qualities["long_hold_quality"],
        "short_hold_quality": qualities["short_hold_quality"],
        "long_exit_urgency_context": qualities["long_exit_urgency_context"],
        "short_exit_urgency_context": qualities["short_exit_urgency_context"],
        "continuation_confidence": 0.0,
        "pullback_health": qualities["pullback_health"],
        "confidence": 0.0,
        "confidence_state": LOW_CONFIDENCE_THIN_DATA,
        "confidence_failure_reasons": reasons,
        "feature_summary": _empty_feature_summary(),
        "state_reasons": list(state_reasons),
        "warnings": list(validation.warnings),
        **_safety_flags(),
    }


def _safety_flags() -> dict[str, bool]:
    return {
        "strategy_authority": False,
        "runtime_trade_eligible": False,
        "runtime_eligible": False,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "cancel_attempted": False,
        "close_attempted": False,
        "place_order_attempted": False,
        "direct_trade_command": False,
        "live_money_eligible": False,
        "live_money_readiness": False,
    }


def _provenance_failure_reasons(input_context: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    category = str(input_context.get("input_source_category") or SOURCE_CATEGORY_UNKNOWN)
    mode = str(input_context.get("input_mode") or "")
    path = input_context.get("input_source_path")
    if not path or category == SOURCE_CATEGORY_UNKNOWN:
        failures.append(MISSING_PROVENANCE)
    if mode == INPUT_MODE_RUNTIME_DECISION and category == SOURCE_CATEGORY_RESEARCH:
        failures.append(RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE)
    if mode == INPUT_MODE_RUNTIME_DECISION and category != SOURCE_CATEGORY_RUNTIME:
        failures.append(MISSING_PROVENANCE)
    return _dedupe(failures)


def track_b_input_context(
    payload: Mapping[str, Any],
    *,
    input_mode: str | None = None,
    input_source_path: str | Path | None = None,
    input_source_category: str | None = None,
) -> dict[str, Any]:
    resolved_path = _string_or_none(
        input_source_path
        or payload.get("input_source_path")
        or payload.get("source_path")
        or payload.get("source_file")
    )
    resolved_mode = _normalize_input_mode(input_mode or _string_or_none(payload.get("input_mode")))
    resolved_category = _normalize_source_category(
        input_source_category
        or _string_or_none(payload.get("input_source_category"))
        or _string_or_none(payload.get("source_category"))
        or _infer_source_category(payload=payload, source_path=resolved_path)
    )
    return {
        "input_source_path": resolved_path,
        "input_source_category": resolved_category,
        "input_mode": resolved_mode,
    }


def source_provenance_status(input_context: Mapping[str, Any]) -> str:
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
    path_with_slash = f"/{normalized_path}"
    if "/outputs/track_b_research/" in path_with_slash or normalized_path.startswith("outputs/track_b_research/"):
        return SOURCE_CATEGORY_RESEARCH
    if bool(payload.get("test_fixture")):
        return SOURCE_CATEGORY_TEST_FIXTURE
    if bool(payload.get("runtime_provenance")) or bool(payload.get("intended_for_runtime_decision")):
        return SOURCE_CATEGORY_RUNTIME
    runtime_markers = (
        "outputs/track_b_execution_core/runtime/",
        "outputs/track_b_runtime/",
        "outputs/probationary/runtime/",
    )
    if any(marker in normalized_path for marker in runtime_markers):
        return SOURCE_CATEGORY_RUNTIME
    return SOURCE_CATEGORY_UNKNOWN


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _failure_state_reasons(failures: Sequence[str]) -> tuple[str, ...]:
    text = {
        THIN_DATA: "insufficient completed 5m candles for the minimum lookback.",
        STALE_INPUT: "completed candle input is stale, future-dated, or has missing intervals.",
        RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE: "research source was presented as runtime truth and was rejected.",
        INCOMPLETE_CANDLES: "input contains incomplete candles and lacks enough completed replacements.",
        MIXED_TIMEFRAME: "input contains non-5m completed candles.",
        MALFORMED_CANDLES: "input contains malformed candle fields or invalid OHLC geometry.",
        LOW_RANGE_QUALITY: "recent candle ranges are too low quality for classification.",
        MISSING_PROVENANCE: "input provenance is missing or not runtime-declared.",
    }
    return tuple(text.get(reason, f"classification failed due to {reason}.") for reason in failures)


def _state_reasons(state: str, features: Mapping[str, Any]) -> list[str]:
    reasons = [
        (
            "evaluated completed 5m candles using body strength, close location, close-to-close pressure, "
            "directional persistence, range behavior, pullback depth, pullback recovery, and impulse decay."
        )
    ]
    if state == PERSISTENT_BULLISH_PRESSURE:
        reasons.append("bullish pressure is persistent across the context and remains aligned in the fast window.")
    elif state == PERSISTENT_BEARISH_PRESSURE:
        reasons.append("bearish pressure is persistent across the context and remains aligned in the fast window.")
    elif "HEALTHY_PULLBACK" in state:
        reasons.append("pullback depth is controlled and recent pressure has not become hostile.")
    elif "CONTINUATION_CONFIRMED" in state:
        reasons.append("pullback recovery and fast-window pressure confirm renewed continuation.")
    elif "PARTICIPATION_COLLAPSE" in state:
        reasons.append("pullback depth or adverse fast-window expansion indicates participation collapse.")
    elif "IMPULSE_DECAYING" in state:
        reasons.append("prior impulse remains visible, but fast-window pressure and impulse decay are deteriorating.")
    elif "IMPULSE_ONLY" in state:
        reasons.append("latest pressure is impulse-like, but persistence and accepted pullback evidence are not yet established.")
    elif state == CHOP_BALANCED:
        reasons.append("pressure is balanced or alternating without reliable directional persistence.")
    reasons.append(
        "net_pressure={net}; fast_net_pressure={fast}; pullback_depth={depth}; pullback_recovery={recovery}; impulse_decay={decay}.".format(
            net=_round(float(features["net_pressure"])),
            fast=_round(float(features["fast_net_pressure"])),
            depth=_round(float(features["pullback_depth"])),
            recovery=_round(float(features["pullback_recovery"])),
            decay=_round(float(features["impulse_decay"])),
        )
    )
    return reasons


def _state_confidence(
    state: str,
    features: Mapping[str, Any],
    *,
    completed_count: int,
    thresholds: ParticipationQualityThresholds,
) -> float:
    data_score = min(completed_count / thresholds.preferred_completed_candles, 1.0)
    persistence = float(features["directional_persistence"])
    pressure = min(float(features["average_abs_pressure"]) / 0.70, 1.0)
    continuation = float(features["continuation_confidence"])
    if state == CHOP_BALANCED:
        shape_score = min((float(features["alternation_score"]) + (1.0 - abs(float(features["net_pressure"])))) / 2.0, 1.0)
    elif "PULLBACK" in state or "CONTINUATION" in state:
        shape_score = max(continuation, persistence)
    elif "COLLAPSE" in state:
        shape_score = max(float(features["pullback_depth"]), pressure)
    else:
        shape_score = max(persistence, pressure)
    return _clamp((0.30 * data_score) + (0.35 * shape_score) + (0.35 * pressure), 0.0, 1.0)


def _confidence_state(confidence: float) -> str:
    if confidence >= 0.72:
        return "HIGH"
    if confidence >= 0.45:
        return "MEDIUM"
    return "LOW"


def _round_feature_summary(features: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "body_strength": _round(float(features["body_strength"])),
        "close_location_in_range": _round(float(features["close_location_in_range"])),
        "close_to_close_pressure": _round(float(features["close_to_close_pressure"])),
        "directional_persistence": _round(float(features["directional_persistence"])),
        "range_expansion_ratio": _round(float(features["range_expansion_ratio"])),
        "pullback_depth": _round(float(features["pullback_depth"])),
        "pullback_recovery": _round(float(features["pullback_recovery"])),
        "impulse_decay": _round(float(features["impulse_decay"])),
        "impulse_direction": features["impulse_direction"],
        "impulse_strength": _round(float(features["impulse_strength"])),
        "volume_confirmation": features["volume_confirmation"],
        "per_candle_pressure_tail": [
            {
                "timestamp": str(row["timestamp"]),
                "body_strength": _round(float(row["body_strength"])),
                "close_location_in_range": _round(float(row["close_location_in_range"])),
                "close_to_close_pressure": _round(float(row["close_to_close_pressure"])),
                "net_pressure": _round(float(row["net_pressure"])),
            }
            for row in features["per_candle_pressure_tail"]
        ],
    }


def _empty_feature_summary() -> dict[str, Any]:
    return {
        "body_strength": 0.0,
        "close_location_in_range": 0.0,
        "close_to_close_pressure": 0.0,
        "directional_persistence": 0.0,
        "range_expansion_ratio": 0.0,
        "pullback_depth": 0.0,
        "pullback_recovery": 0.0,
        "impulse_decay": 0.0,
        "impulse_direction": "NONE",
        "impulse_strength": 0.0,
        "volume_confirmation": "UNAVAILABLE_OR_UNRELIABLE",
        "per_candle_pressure_tail": [],
    }


def _extract_candle_rows(payload: Mapping[str, Any]) -> Any:
    for key in ("candles", "ohlcv_candles", "bars"):
        if key in payload:
            return payload.get(key)
    return None


def _normalize_instrument(payload: Mapping[str, Any]) -> str:
    return str(payload.get("instrument") or payload.get("symbol") or "UNKNOWN").strip().upper() or "UNKNOWN"


def _source_id(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("source_id")
    if value is None:
        provenance = payload.get("runtime_provenance")
        if isinstance(provenance, Mapping):
            value = provenance.get("source_id")
    return str(value).strip() if value is not None and str(value).strip() else None


def _normalize_timeframe(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"5", "5m", "5min", "5minute", "5minutes"}:
        return "5m"
    if normalized in {"1", "1m", "1min", "1minute", "1minutes"}:
        return "1m"
    return normalized


def _timeframe_delta(timeframe: str) -> timedelta:
    if timeframe == "5m":
        return timedelta(minutes=5)
    raise ValueError(f"unsupported timeframe: {timeframe}")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return _parse_timestamp(value)


def _parse_timestamp(value: Any) -> datetime:
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid timestamp {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _finite_float(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{value!r} is not numeric") from exc
    if not math.isfinite(number):
        raise ValueError(f"{value!r} is not finite")
    return number


def _optional_finite_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    return _finite_float(value)


def _close_to_close_pressure(candles: Sequence[NormalizedCandle]) -> float:
    values: list[float] = []
    for previous, current in zip(candles, candles[1:]):
        values.append(_clamp((current.close - previous.close) / current.range, -1.0, 1.0))
    return _average(values)


def _alternation_score(values: Sequence[float]) -> float:
    signs = [1 if value > 0.15 else -1 if value < -0.15 else 0 for value in values]
    active = [sign for sign in signs if sign != 0]
    if len(active) < 2:
        return 0.0
    alternations = sum(1 for previous, current in zip(active, active[1:]) if previous != current)
    return alternations / (len(active) - 1)


def _volume_reliable(candles: Sequence[NormalizedCandle]) -> bool:
    volumes = [candle.volume for candle in candles]
    if any(volume is None for volume in volumes):
        return False
    numeric = [float(volume) for volume in volumes if volume is not None]
    if not numeric or any(volume < 0 for volume in numeric):
        return False
    return len(set(numeric)) > 1


def _average(values: Sequence[float]) -> float:
    clean = [float(value) for value in values if math.isfinite(float(value))]
    if not clean:
        return 0.0
    return sum(clean) / len(clean)


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _round(value: float) -> float:
    return round(value, 6)


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
