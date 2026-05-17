"""Offline Track B lifecycle awareness evaluator.

This module emits advisory lifecycle context only. It deliberately avoids
broker APIs, strategy integration, order-intent creation, and lifecycle writes.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "track_b_lifecycle_awareness_state_v1"
PRODUCER_NAME = "TrackBLifecycleAwarenessLayer"
DEFAULT_LIFECYCLE_AWARENESS_ARTIFACT_PATH = (
    "outputs/track_b_execution_core/lifecycle_awareness/latest_lifecycle_awareness_state.json"
)


class LifecycleAwarenessState(str, Enum):
    NOT_IN_POSITION = "NOT_IN_POSITION"
    NEWLY_OPENED = "NEWLY_OPENED"
    WORKING_IN_FAVOR = "WORKING_IN_FAVOR"
    FAVORABLE_EXPANSION = "FAVORABLE_EXPANSION"
    HEALTHY_PULLBACK = "HEALTHY_PULLBACK"
    STALLED = "STALLED"
    DECAYING = "DECAYING"
    ADVERSE_DOMINANCE = "ADVERSE_DOMINANCE"
    DEFENSIVE_MANAGEMENT = "DEFENSIVE_MANAGEMENT"
    EXIT_RECOMMENDED_CONTEXT = "EXIT_RECOMMENDED_CONTEXT"
    EXIT_PENDING_OBSERVED = "EXIT_PENDING_OBSERVED"
    CLOSED_OR_FLAT = "CLOSED_OR_FLAT"
    LOW_CONFIDENCE_STALE_OR_UNRECONCILED = "LOW_CONFIDENCE_STALE_OR_UNRECONCILED"


class HoldQualityContext(str, Enum):
    NO_POSITION = "NO_POSITION"
    EARLY_UNKNOWN = "EARLY_UNKNOWN"
    STRONG_HOLD = "STRONG_HOLD"
    CONSTRUCTIVE_HOLD = "CONSTRUCTIVE_HOLD"
    GUARDED_HOLD = "GUARDED_HOLD"
    WEAK_HOLD = "WEAK_HOLD"
    NO_HOLD_LOW_CONFIDENCE = "NO_HOLD_LOW_CONFIDENCE"


class ExitUrgencyContext(str, Enum):
    NONE = "NONE"
    LOW = "LOW"
    WATCH = "WATCH"
    ELEVATED = "ELEVATED"
    HIGH_ADVISORY = "HIGH_ADVISORY"
    PENDING_OBSERVED = "PENDING_OBSERVED"
    LOW_CONFIDENCE_BLOCKED = "LOW_CONFIDENCE_BLOCKED"


class ReduceSizeContext(str, Enum):
    NO_POSITION = "NO_POSITION"
    NOT_INDICATED = "NOT_INDICATED"
    CONSIDER_ONLY_AFTER_AUTHORIZED_REVIEW = "CONSIDER_ONLY_AFTER_AUTHORIZED_REVIEW"
    ELEVATED_ADVISORY = "ELEVATED_ADVISORY"
    BLOCKED_LOW_CONFIDENCE = "BLOCKED_LOW_CONFIDENCE"


class AddSizeContext(str, Enum):
    NOT_INDICATED = "NOT_INDICATED"
    ONLY_AFTER_CONTINUATION_CONFIRMATION = "ONLY_AFTER_CONTINUATION_CONFIRMATION"
    BLOCKED_DEGRADED_CONTEXT = "BLOCKED_DEGRADED_CONTEXT"
    BLOCKED_LOW_CONFIDENCE = "BLOCKED_LOW_CONFIDENCE"


class PatienceContext(str, Enum):
    NO_POSITION = "NO_POSITION"
    EARLY_PATIENCE = "EARLY_PATIENCE"
    PATIENCE_SUPPORTED = "PATIENCE_SUPPORTED"
    PATIENCE_NEUTRAL = "PATIENCE_NEUTRAL"
    PATIENCE_DECLINING = "PATIENCE_DECLINING"
    PATIENCE_NOT_SUPPORTED = "PATIENCE_NOT_SUPPORTED"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"


NOT_IN_POSITION = LifecycleAwarenessState.NOT_IN_POSITION.value
NEWLY_OPENED = LifecycleAwarenessState.NEWLY_OPENED.value
WORKING_IN_FAVOR = LifecycleAwarenessState.WORKING_IN_FAVOR.value
FAVORABLE_EXPANSION = LifecycleAwarenessState.FAVORABLE_EXPANSION.value
HEALTHY_PULLBACK = LifecycleAwarenessState.HEALTHY_PULLBACK.value
STALLED = LifecycleAwarenessState.STALLED.value
DECAYING = LifecycleAwarenessState.DECAYING.value
ADVERSE_DOMINANCE = LifecycleAwarenessState.ADVERSE_DOMINANCE.value
DEFENSIVE_MANAGEMENT = LifecycleAwarenessState.DEFENSIVE_MANAGEMENT.value
EXIT_RECOMMENDED_CONTEXT = LifecycleAwarenessState.EXIT_RECOMMENDED_CONTEXT.value
EXIT_PENDING_OBSERVED = LifecycleAwarenessState.EXIT_PENDING_OBSERVED.value
CLOSED_OR_FLAT = LifecycleAwarenessState.CLOSED_OR_FLAT.value
LOW_CONFIDENCE_STALE_OR_UNRECONCILED = LifecycleAwarenessState.LOW_CONFIDENCE_STALE_OR_UNRECONCILED.value

MISSING_LIFECYCLE_POSITION = "MISSING_LIFECYCLE_POSITION"
POSITION_NOT_LIFECYCLE_OWNED = "POSITION_NOT_LIFECYCLE_OWNED"
POSITION_CLOSED_OR_FLAT = "POSITION_CLOSED_OR_FLAT"
STALE_INPUT = "STALE_INPUT"
MALFORMED_POSITION_RECORD = "MALFORMED_POSITION_RECORD"
MALFORMED_CANDLES = "MALFORMED_CANDLES"
INCOMPLETE_CANDLES = "INCOMPLETE_CANDLES"
THIN_DATA = "THIN_DATA"
MISSING_PROVENANCE = "MISSING_PROVENANCE"
MISSING_ENTRY_CONTEXT = "MISSING_ENTRY_CONTEXT"
MISSING_MFE_MAE_CONTEXT = "MISSING_MFE_MAE_CONTEXT"
MISSING_PROGRESS_CONTEXT = "MISSING_PROGRESS_CONTEXT"
MIXED_TIMEFRAME = "MIXED_TIMEFRAME"
AMBIGUOUS_TIMEFRAME = "AMBIGUOUS_TIMEFRAME"
UNRECONCILED_POSITION_CONTEXT = "UNRECONCILED_POSITION_CONTEXT"
BROKER_LIFECYCLE_MISMATCH = "BROKER_LIFECYCLE_MISMATCH"
RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE = "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"

INPUT_MODE_RUNTIME_DECISION = "RUNTIME_DECISION"
INPUT_MODE_OFFLINE_EVALUATION = "OFFLINE_EVALUATION"
INPUT_MODE_TEST = "TEST"
SOURCE_CATEGORY_RUNTIME = "RUNTIME"
SOURCE_CATEGORY_RESEARCH = "RESEARCH"
SOURCE_CATEGORY_TEST_FIXTURE = "TEST_FIXTURE"
SOURCE_CATEGORY_UNKNOWN = "UNKNOWN"

TIMEFRAME_SOURCE_NATIVE = "NATIVE"
TIMEFRAME_SOURCE_DERIVED = "DERIVED"
TIMEFRAME_ALIGNMENT_ALIGNED = "ALIGNED"
TIMEFRAME_ALIGNMENT_DERIVED_ALIGNED = "DERIVED_ALIGNED"
TIMEFRAME_ALIGNMENT_MIXED_BLOCKED = "MIXED_TIMEFRAME_BLOCKED"
TIMEFRAME_ALIGNMENT_MISSING_SCHEMA = "MISSING_TIMEFRAME_SCHEMA"

OPEN_LIFECYCLE_STATUSES = {"OPEN", "OPEN_MANAGED", "MANAGED_OPEN"}
EXIT_PENDING_LIFECYCLE_STATUSES = {"EXIT_PENDING", "PENDING_EXIT", "MANAGED_EXIT_PENDING"}
CLOSED_LIFECYCLE_STATUSES = {"CLOSED", "CLOSED_FLAT", "FLAT", "CLOSED_OR_FLAT"}
RECONCILED_STATUSES = {"MATCHED", "RECONCILED", "PAPER_MATCHED", "MATCHED_OPEN"}
EXIT_PENDING_RECONCILIATION_STATUSES = {"EXIT_PENDING", "PENDING_EXIT", "PAPER_EXIT_PENDING"}
FLAT_RECONCILIATION_STATUSES = {"FLAT", "RECONCILED_FLAT", "CLOSED_FLAT"}


@dataclass(frozen=True)
class LifecycleAwarenessThresholds:
    min_completed_candles: int = 8
    stale_after_intervals: float = 3.0
    max_missing_gap_intervals: float = 1.5
    newly_opened_max_bars: int = 2
    favorable_mfe_points: float = 1.50
    favorable_progress_ratio: float = 0.70
    healthy_pullback_min_mfe_points: float = 1.00
    healthy_pullback_min_progress_ratio: float = 0.30
    healthy_pullback_max_drawdown_ratio: float = 0.70
    stalled_min_bars: int = 8
    stalled_max_mfe_points: float = 0.60
    stalled_abs_progress_points: float = 0.35
    decaying_min_mfe_points: float = 1.00
    decaying_drawdown_ratio: float = 0.55
    adverse_mae_points: float = 1.50
    adverse_unrealized_points: float = -0.80
    adverse_progress_ratio: float = 0.75


@dataclass(frozen=True)
class NormalizedCandle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    timeframe: str

    @property
    def range(self) -> float:
        return self.high - self.low


@dataclass(frozen=True)
class _CandleValidation:
    candles: tuple[NormalizedCandle, ...]
    candles_received: int
    latest_candle_timestamp: datetime | None
    freshness_status: str
    failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def build_lifecycle_awareness_state(
    payload: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
    thresholds: LifecycleAwarenessThresholds | None = None,
) -> dict[str, Any]:
    """Build deterministic advisory lifecycle context from explicit inputs."""

    resolved_now = _coerce_now(now)
    resolved_thresholds = thresholds or LifecycleAwarenessThresholds()
    input_context = _input_context(payload)
    timeframe_context = _timeframe_context(payload)
    position = _position_context(payload)
    entry_context = _entry_context(payload)
    reconciliation = _reconciliation_context(payload)
    metrics = _metrics_context(payload, position=position, now=resolved_now)

    failures: list[str] = []
    failures.extend(_provenance_failures(input_context))
    failures.extend(_timeframe_failures(timeframe_context))
    failures.extend(_position_failures(position))
    failures.extend(_entry_failures(entry_context))
    failures.extend(_metrics_failures(metrics))
    failures.extend(_reconciliation_failures(position, reconciliation))

    raw_candles = _extract_candle_rows(payload)
    if not _is_sequence(raw_candles):
        candle_validation = _CandleValidation(
            candles=(),
            candles_received=0,
            latest_candle_timestamp=None,
            freshness_status="NO_COMPLETED_CANDLES",
            failure_reasons=(THIN_DATA, MALFORMED_CANDLES),
            warnings=("candle input must be a sequence under candles, ohlcv_candles, or bars.",),
        )
    else:
        candle_validation = _validate_candles(
            raw_candles,
            now=resolved_now,
            expected_timeframe=str(timeframe_context["lifecycle_evaluation_timeframe"]),
            thresholds=resolved_thresholds,
        )
    failures.extend(candle_validation.failure_reasons)
    failures = _dedupe(failures)

    lifecycle_status = str(position.get("lifecycle_status") or "").upper()
    reconciliation_status = str(reconciliation.get("status") or "").upper()
    if failures:
        return _build_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            position=position,
            entry_context=entry_context,
            reconciliation=reconciliation,
            metrics=metrics,
            candle_validation=candle_validation,
            lifecycle_awareness_state=LOW_CONFIDENCE_STALE_OR_UNRECONCILED,
            hold_quality_context=HoldQualityContext.NO_HOLD_LOW_CONFIDENCE.value,
            exit_urgency_context=ExitUrgencyContext.LOW_CONFIDENCE_BLOCKED.value,
            reduce_size_context=ReduceSizeContext.BLOCKED_LOW_CONFIDENCE.value,
            add_size_context=AddSizeContext.BLOCKED_LOW_CONFIDENCE.value,
            patience_context=PatienceContext.LOW_CONFIDENCE.value,
            state_reasons=[],
            failure_reasons=failures,
            confidence=0.0,
        )
    if lifecycle_status in CLOSED_LIFECYCLE_STATUSES or reconciliation_status in FLAT_RECONCILIATION_STATUSES:
        return _build_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            position=position,
            entry_context=entry_context,
            reconciliation=reconciliation,
            metrics=metrics,
            candle_validation=candle_validation,
            lifecycle_awareness_state=CLOSED_OR_FLAT,
            hold_quality_context=HoldQualityContext.NO_POSITION.value,
            exit_urgency_context=ExitUrgencyContext.NONE.value,
            reduce_size_context=ReduceSizeContext.NO_POSITION.value,
            add_size_context=AddSizeContext.NOT_INDICATED.value,
            patience_context=PatienceContext.NO_POSITION.value,
            state_reasons=["lifecycle or reconciliation context is flat."],
            failure_reasons=[],
            confidence=_confidence(metrics, candle_validation),
        )
    if lifecycle_status in EXIT_PENDING_LIFECYCLE_STATUSES or reconciliation_status in EXIT_PENDING_RECONCILIATION_STATUSES:
        return _build_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            position=position,
            entry_context=entry_context,
            reconciliation=reconciliation,
            metrics=metrics,
            candle_validation=candle_validation,
            lifecycle_awareness_state=EXIT_PENDING_OBSERVED,
            hold_quality_context=HoldQualityContext.GUARDED_HOLD.value,
            exit_urgency_context=ExitUrgencyContext.PENDING_OBSERVED.value,
            reduce_size_context=ReduceSizeContext.NOT_INDICATED.value,
            add_size_context=AddSizeContext.BLOCKED_DEGRADED_CONTEXT.value,
            patience_context=PatienceContext.PATIENCE_NOT_SUPPORTED.value,
            state_reasons=["exit pending state is observed from read-only lifecycle or reconciliation context."],
            failure_reasons=[],
            confidence=_confidence(metrics, candle_validation),
        )

    classification = _classify(metrics, thresholds=resolved_thresholds)
    return _build_report(
        payload=payload,
        generated_at=resolved_now,
        input_context=input_context,
        timeframe_context=timeframe_context,
        position=position,
        entry_context=entry_context,
        reconciliation=reconciliation,
        metrics=metrics,
        candle_validation=candle_validation,
        **classification,
        failure_reasons=[],
        confidence=_confidence(metrics, candle_validation),
    )


def _classify(metrics: Mapping[str, Any], *, thresholds: LifecycleAwarenessThresholds) -> dict[str, Any]:
    bars = int(metrics["bars_since_entry"])
    mfe = float(metrics["mfe_points"])
    mae = float(metrics["mae_points"])
    current = float(metrics["current_unrealized_points"])
    progress_ratio = float(metrics["progress_ratio"])
    drawdown = float(metrics["drawdown_from_mfe_ratio"])
    adverse_ratio = float(metrics["adverse_progress_ratio"])

    if bars <= thresholds.newly_opened_max_bars:
        return {
            "lifecycle_awareness_state": NEWLY_OPENED,
            "hold_quality_context": HoldQualityContext.EARLY_UNKNOWN.value,
            "exit_urgency_context": ExitUrgencyContext.LOW.value,
            "reduce_size_context": ReduceSizeContext.NOT_INDICATED.value,
            "add_size_context": AddSizeContext.NOT_INDICATED.value,
            "patience_context": PatienceContext.EARLY_PATIENCE.value,
            "state_reasons": [f"position is within the first {thresholds.newly_opened_max_bars} completed management bars."],
        }
    if (
        mae >= thresholds.adverse_mae_points
        or current <= thresholds.adverse_unrealized_points
        or adverse_ratio >= thresholds.adverse_progress_ratio
    ):
        return {
            "lifecycle_awareness_state": ADVERSE_DOMINANCE,
            "hold_quality_context": HoldQualityContext.WEAK_HOLD.value,
            "exit_urgency_context": ExitUrgencyContext.HIGH_ADVISORY.value,
            "reduce_size_context": ReduceSizeContext.ELEVATED_ADVISORY.value,
            "add_size_context": AddSizeContext.BLOCKED_DEGRADED_CONTEXT.value,
            "patience_context": PatienceContext.PATIENCE_NOT_SUPPORTED.value,
            "state_reasons": ["adverse excursion or unrealized progress dominates current lifecycle context."],
        }
    if mfe >= thresholds.favorable_mfe_points and progress_ratio >= thresholds.favorable_progress_ratio:
        return {
            "lifecycle_awareness_state": FAVORABLE_EXPANSION,
            "hold_quality_context": HoldQualityContext.STRONG_HOLD.value,
            "exit_urgency_context": ExitUrgencyContext.LOW.value,
            "reduce_size_context": ReduceSizeContext.NOT_INDICATED.value,
            "add_size_context": AddSizeContext.ONLY_AFTER_CONTINUATION_CONFIRMATION.value,
            "patience_context": PatienceContext.PATIENCE_SUPPORTED.value,
            "state_reasons": ["MFE and current progress show favorable expansion."],
        }
    if (
        mfe >= thresholds.healthy_pullback_min_mfe_points
        and progress_ratio >= thresholds.healthy_pullback_min_progress_ratio
        and drawdown <= thresholds.healthy_pullback_max_drawdown_ratio
    ):
        return {
            "lifecycle_awareness_state": HEALTHY_PULLBACK,
            "hold_quality_context": HoldQualityContext.CONSTRUCTIVE_HOLD.value,
            "exit_urgency_context": ExitUrgencyContext.WATCH.value,
            "reduce_size_context": ReduceSizeContext.NOT_INDICATED.value,
            "add_size_context": AddSizeContext.ONLY_AFTER_CONTINUATION_CONFIRMATION.value,
            "patience_context": PatienceContext.PATIENCE_SUPPORTED.value,
            "state_reasons": ["pullback is bounded relative to prior favorable progress."],
        }
    if mfe >= thresholds.decaying_min_mfe_points and drawdown >= thresholds.decaying_drawdown_ratio:
        return {
            "lifecycle_awareness_state": DECAYING,
            "hold_quality_context": HoldQualityContext.GUARDED_HOLD.value,
            "exit_urgency_context": ExitUrgencyContext.ELEVATED.value,
            "reduce_size_context": ReduceSizeContext.CONSIDER_ONLY_AFTER_AUTHORIZED_REVIEW.value,
            "add_size_context": AddSizeContext.BLOCKED_DEGRADED_CONTEXT.value,
            "patience_context": PatienceContext.PATIENCE_DECLINING.value,
            "state_reasons": ["trade has given back a material share of prior favorable progress."],
        }
    if (
        bars >= thresholds.stalled_min_bars
        and mfe <= thresholds.stalled_max_mfe_points
        and abs(current) <= thresholds.stalled_abs_progress_points
    ):
        return {
            "lifecycle_awareness_state": STALLED,
            "hold_quality_context": HoldQualityContext.GUARDED_HOLD.value,
            "exit_urgency_context": ExitUrgencyContext.WATCH.value,
            "reduce_size_context": ReduceSizeContext.NOT_INDICATED.value,
            "add_size_context": AddSizeContext.BLOCKED_DEGRADED_CONTEXT.value,
            "patience_context": PatienceContext.PATIENCE_DECLINING.value,
            "state_reasons": ["position has used enough bars without sufficient favorable progress."],
        }
    if current > 0:
        return {
            "lifecycle_awareness_state": WORKING_IN_FAVOR,
            "hold_quality_context": HoldQualityContext.CONSTRUCTIVE_HOLD.value,
            "exit_urgency_context": ExitUrgencyContext.LOW.value,
            "reduce_size_context": ReduceSizeContext.NOT_INDICATED.value,
            "add_size_context": AddSizeContext.ONLY_AFTER_CONTINUATION_CONFIRMATION.value,
            "patience_context": PatienceContext.PATIENCE_SUPPORTED.value,
            "state_reasons": ["position is favorable but below expansion threshold."],
        }
    return {
        "lifecycle_awareness_state": DEFENSIVE_MANAGEMENT,
        "hold_quality_context": HoldQualityContext.GUARDED_HOLD.value,
        "exit_urgency_context": ExitUrgencyContext.ELEVATED.value,
        "reduce_size_context": ReduceSizeContext.CONSIDER_ONLY_AFTER_AUTHORIZED_REVIEW.value,
        "add_size_context": AddSizeContext.BLOCKED_DEGRADED_CONTEXT.value,
        "patience_context": PatienceContext.PATIENCE_DECLINING.value,
        "state_reasons": ["position context is not favorable and does not qualify as simple stall."],
    }


def _build_report(
    *,
    payload: Mapping[str, Any],
    generated_at: datetime,
    input_context: Mapping[str, Any],
    timeframe_context: Mapping[str, Any],
    position: Mapping[str, Any],
    entry_context: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    metrics: Mapping[str, Any],
    candle_validation: _CandleValidation,
    lifecycle_awareness_state: str,
    hold_quality_context: str,
    exit_urgency_context: str,
    reduce_size_context: str,
    add_size_context: str,
    patience_context: str,
    state_reasons: Sequence[str],
    failure_reasons: Sequence[str],
    confidence: float,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "ADVISORY_CONTEXT_ONLY",
        "artifact_path": DEFAULT_LIFECYCLE_AWARENESS_ARTIFACT_PATH,
        "generated_at": generated_at.isoformat(),
        "source_id": _source_id(payload),
        **input_context,
        "source_provenance_status": _source_provenance_status(input_context),
        "freshness_status": candle_validation.freshness_status,
        "latest_input_timestamp": candle_validation.latest_candle_timestamp.isoformat()
        if candle_validation.latest_candle_timestamp
        else None,
        "candles_received": candle_validation.candles_received,
        "completed_candles_used": len(candle_validation.candles),
        **position,
        "entry_context": dict(entry_context),
        "broker_reconciliation_status": reconciliation["status"],
        "broker_reconciliation_context": dict(reconciliation),
        "lifecycle_awareness_state": lifecycle_awareness_state,
        "hold_quality_context": hold_quality_context,
        "exit_urgency_context": exit_urgency_context,
        "reduce_size_context": reduce_size_context,
        "add_size_context": add_size_context,
        "patience_context": patience_context,
        "state_reasons": list(state_reasons),
        "failure_reasons": list(_dedupe(failure_reasons)),
        "confidence": _round(confidence),
        **{key: value for key, value in metrics.items() if not key.startswith("_")},
        **timeframe_context,
        "warnings": list(candle_validation.warnings),
        **_safety_flags(),
    }


def _validate_candles(
    rows: Sequence[Any],
    *,
    now: datetime,
    expected_timeframe: str,
    thresholds: LifecycleAwarenessThresholds,
) -> _CandleValidation:
    candles: list[NormalizedCandle] = []
    failures: list[str] = []
    warnings: list[str] = []
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
        if row_timeframe != expected_timeframe:
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
            close_price = _finite_float(row["close"])
        except ValueError as exc:
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] has invalid field value: {exc}.")
            continue
        if min(open_price, high, low, close_price) <= 0 or high < low:
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] has invalid OHLC geometry.")
            continue
        if high < max(open_price, close_price) or low > min(open_price, close_price):
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] OHLC values are internally inconsistent.")
            continue
        candles.append(
            NormalizedCandle(
                timestamp=timestamp,
                open=open_price,
                high=high,
                low=low,
                close=close_price,
                timeframe=row_timeframe,
            )
        )

    if incomplete_count:
        failures.append(INCOMPLETE_CANDLES)
        warnings.append(f"rejected {incomplete_count} incomplete candle(s).")
    if mixed_timeframe_count:
        failures.append(MIXED_TIMEFRAME)
        warnings.append(f"rejected {mixed_timeframe_count} candle(s) outside lifecycle evaluation timeframe.")
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
    latest = candles[-1].timestamp if candles else None
    return _CandleValidation(
        candles=tuple(candles),
        candles_received=len(rows),
        latest_candle_timestamp=latest,
        freshness_status=freshness_status,
        failure_reasons=tuple(_dedupe(failures)),
        warnings=tuple(warnings),
    )


def _position_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("lifecycle_position_record") or payload.get("lifecycle_position") or payload.get("position"))
    side = str(raw.get("side") or payload.get("side") or "").strip().upper()
    lifecycle_status = str(raw.get("lifecycle_status") or payload.get("lifecycle_status") or "").strip().upper()
    return {
        "lifecycle_position_id": _string_or_none(raw.get("lifecycle_position_id") or raw.get("position_id") or payload.get("lifecycle_position_id")),
        "strategy_id": _string_or_none(raw.get("strategy_id") or payload.get("strategy_id")),
        "lane_id": _string_or_none(raw.get("lane_id") or payload.get("lane_id")),
        "instrument": _string_or_none(raw.get("instrument") or raw.get("symbol") or payload.get("instrument") or payload.get("symbol")),
        "contract_key": _string_or_none(raw.get("contract_key") or payload.get("contract_key")),
        "side": side,
        "quantity_context": raw.get("quantity_context") or raw.get("quantity") or payload.get("quantity_context"),
        "entry_timestamp": _string_or_none(raw.get("entry_timestamp") or payload.get("entry_timestamp")),
        "fill_timestamp": _string_or_none(raw.get("fill_timestamp") or payload.get("fill_timestamp") or raw.get("entry_timestamp") or payload.get("entry_timestamp")),
        "average_fill_price": _optional_finite_float(raw.get("average_fill_price") or raw.get("avg_fill_price") or payload.get("average_fill_price")),
        "lifecycle_status": lifecycle_status,
        "lifecycle_owned": bool(raw.get("lifecycle_owned", payload.get("lifecycle_owned", True))),
    }


def _position_failures(position: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    if not position["lifecycle_position_id"]:
        failures.append(MISSING_LIFECYCLE_POSITION)
    if position["side"] not in {"LONG", "SHORT"}:
        failures.append(MALFORMED_POSITION_RECORD)
    if not position["lifecycle_status"]:
        failures.append(MALFORMED_POSITION_RECORD)
    if not bool(position["lifecycle_owned"]):
        failures.append(POSITION_NOT_LIFECYCLE_OWNED)
    if not position["fill_timestamp"]:
        failures.append(MALFORMED_POSITION_RECORD)
    return failures


def _entry_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("entry_metadata") or payload.get("entry_context"))
    acceptance = _as_mapping(payload.get("entry_acceptance_context"))
    return {
        "strategy_family": _string_or_none(raw.get("strategy_family") or acceptance.get("candidate_family")),
        "entry_timeframe": _normalize_timeframe(str(raw.get("entry_timeframe") or acceptance.get("entry_timeframe") or payload.get("entry_timeframe") or "")),
        "entry_acceptance_class": _string_or_none(raw.get("entry_acceptance_class") or acceptance.get("acceptance_class")),
        "entry_acceptance_confidence": _optional_finite_float(raw.get("confidence") or acceptance.get("confidence")),
    }


def _entry_failures(entry_context: Mapping[str, Any]) -> list[str]:
    if not entry_context["strategy_family"] or not entry_context["entry_timeframe"]:
        return [MISSING_ENTRY_CONTEXT]
    return []


def _reconciliation_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("broker_reconciliation_context") or payload.get("reconciliation_context"))
    return {
        "status": str(raw.get("status") or payload.get("reconciliation_status") or "").strip().upper(),
        "source": _string_or_none(raw.get("source")),
        "read_only": True,
    }


def _reconciliation_failures(position: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> list[str]:
    status = str(reconciliation.get("status") or "").upper()
    lifecycle_status = str(position.get("lifecycle_status") or "").upper()
    allowed = RECONCILED_STATUSES | EXIT_PENDING_RECONCILIATION_STATUSES | FLAT_RECONCILIATION_STATUSES
    if status not in allowed:
        return [UNRECONCILED_POSITION_CONTEXT]
    if lifecycle_status in OPEN_LIFECYCLE_STATUSES and status in FLAT_RECONCILIATION_STATUSES:
        return [BROKER_LIFECYCLE_MISMATCH]
    if lifecycle_status in CLOSED_LIFECYCLE_STATUSES and status in RECONCILED_STATUSES:
        return [BROKER_LIFECYCLE_MISMATCH]
    return []


def _metrics_context(payload: Mapping[str, Any], *, position: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    mfe_mae = _as_mapping(payload.get("mfe_mae_context") or payload.get("mfe_mae") or payload.get("position_metrics"))
    progress = _as_mapping(payload.get("unrealized_progress_context") or payload.get("progress_context") or payload.get("position_metrics"))
    age = _as_mapping(payload.get("position_age_context") or payload.get("age_context") or payload.get("position_metrics"))

    mfe_points = _optional_finite_float(mfe_mae.get("mfe_points") or payload.get("mfe_points"))
    mae_points = _optional_finite_float(mfe_mae.get("mae_points") or payload.get("mae_points"))
    current_unrealized_points = _optional_finite_float(
        progress.get("current_unrealized_points")
        or progress.get("current_favorable_points")
        or payload.get("current_unrealized_points")
    )
    bars_since_entry = _optional_finite_float(age.get("bars_since_entry") or age.get("bars_since_fill") or payload.get("bars_since_entry"))
    fill_timestamp = _string_or_none(position.get("fill_timestamp"))
    wall_clock_seconds = _optional_finite_float(age.get("wall_clock_seconds_since_entry") or payload.get("wall_clock_seconds_since_entry"))
    if wall_clock_seconds is None and fill_timestamp:
        try:
            wall_clock_seconds = max((now - _parse_timestamp(fill_timestamp)).total_seconds(), 0.0)
        except ValueError:
            wall_clock_seconds = None
    session_seconds = _optional_finite_float(age.get("session_seconds_since_entry") or payload.get("session_seconds_since_entry"))
    if session_seconds is None:
        session_seconds = wall_clock_seconds

    mfe_points = abs(mfe_points) if mfe_points is not None else None
    mae_points = abs(mae_points) if mae_points is not None else None
    current = current_unrealized_points
    progress_ratio = 0.0
    drawdown_ratio = 0.0
    adverse_ratio = 0.0
    if mfe_points and mfe_points > 0 and current is not None:
        progress_ratio = _clamp(current / mfe_points, -1.0, 1.0)
        drawdown_ratio = _clamp((mfe_points - current) / mfe_points, 0.0, 1.0)
    if mae_points and mae_points > 0:
        adverse_ratio = _clamp(mae_points / max((mfe_points or 0.0) + mae_points, 1e-9), 0.0, 1.0)

    return {
        "bars_since_entry": int(bars_since_entry or 0),
        "wall_clock_seconds_since_entry": _round(wall_clock_seconds or 0.0),
        "session_seconds_since_entry": _round(session_seconds or 0.0),
        "mfe_points": _round(mfe_points or 0.0),
        "mae_points": _round(mae_points or 0.0),
        "mfe_ticks": _optional_finite_float(mfe_mae.get("mfe_ticks") or payload.get("mfe_ticks")),
        "mae_ticks": _optional_finite_float(mfe_mae.get("mae_ticks") or payload.get("mae_ticks")),
        "mfe_dollars": _optional_finite_float(mfe_mae.get("mfe_dollars") or payload.get("mfe_dollars")),
        "mae_dollars": _optional_finite_float(mfe_mae.get("mae_dollars") or payload.get("mae_dollars")),
        "current_unrealized_points": _round(current or 0.0),
        "current_unrealized_ticks": _optional_finite_float(progress.get("current_unrealized_ticks") or payload.get("current_unrealized_ticks")),
        "current_unrealized_dollars": _optional_finite_float(progress.get("current_unrealized_dollars") or payload.get("current_unrealized_dollars")),
        "progress_ratio": _round(progress_ratio),
        "drawdown_from_mfe_ratio": _round(drawdown_ratio),
        "adverse_progress_ratio": _round(adverse_ratio),
        "_has_mfe_mae_context": mfe_points is not None and mae_points is not None,
        "_has_progress_context": current is not None,
        "_has_age_context": bars_since_entry is not None,
    }


def _metrics_failures(metrics: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    if not metrics["_has_mfe_mae_context"]:
        failures.append(MISSING_MFE_MAE_CONTEXT)
    if not metrics["_has_progress_context"]:
        failures.append(MISSING_PROGRESS_CONTEXT)
    if not metrics["_has_age_context"]:
        failures.append(MISSING_PROGRESS_CONTEXT)
    return failures


def _timeframe_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("timeframe_context") or payload.get("timeframes"))
    lifecycle_evaluation_timeframe = _normalize_timeframe(
        str(raw.get("lifecycle_evaluation_timeframe") or payload.get("lifecycle_evaluation_timeframe") or "")
    )
    fast_reaction_timeframe = _normalize_timeframe(
        str(raw.get("fast_reaction_timeframe") or payload.get("fast_reaction_timeframe") or "")
    )
    trend_context_timeframe = _normalize_timeframe(
        str(raw.get("trend_context_timeframe") or payload.get("trend_context_timeframe") or "")
    )
    return {
        "lifecycle_evaluation_timeframe": lifecycle_evaluation_timeframe,
        "fast_reaction_timeframe": fast_reaction_timeframe or None,
        "trend_context_timeframe": trend_context_timeframe or None,
        "timeframe_source": str(raw.get("timeframe_source") or payload.get("timeframe_source") or "").strip().upper(),
        "base_timeframe_if_derived": _normalize_timeframe(
            str(raw.get("base_timeframe_if_derived") or payload.get("base_timeframe_if_derived") or "")
        )
        or None,
        "aggregation_method": _string_or_none(raw.get("aggregation_method") or payload.get("aggregation_method")),
        "anchor_rule": _string_or_none(raw.get("anchor_rule") or payload.get("anchor_rule")),
        "timeframe_alignment_status": str(raw.get("timeframe_alignment_status") or payload.get("timeframe_alignment_status") or "").strip().upper(),
    }


def _timeframe_failures(context: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    lifecycle_timeframe = str(context.get("lifecycle_evaluation_timeframe") or "")
    if not lifecycle_timeframe or not context.get("timeframe_source") or not context.get("timeframe_alignment_status"):
        failures.append(AMBIGUOUS_TIMEFRAME)
    if context.get("timeframe_alignment_status") == TIMEFRAME_ALIGNMENT_MISSING_SCHEMA:
        failures.append(AMBIGUOUS_TIMEFRAME)
    if context.get("timeframe_alignment_status") not in {TIMEFRAME_ALIGNMENT_ALIGNED, TIMEFRAME_ALIGNMENT_DERIVED_ALIGNED}:
        failures.append(MIXED_TIMEFRAME)
    if _timeframe_delta(lifecycle_timeframe) is None:
        failures.append(AMBIGUOUS_TIMEFRAME)
    if context.get("timeframe_source") == TIMEFRAME_SOURCE_DERIVED:
        if not context.get("base_timeframe_if_derived") or not context.get("aggregation_method") or not context.get("anchor_rule"):
            failures.append(AMBIGUOUS_TIMEFRAME)
    return _dedupe(failures)


def _input_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    source_path = _string_or_none(payload.get("input_source_path") or payload.get("source_path") or payload.get("source_file"))
    source_category = _normalize_source_category(
        _string_or_none(payload.get("input_source_category") or payload.get("source_category"))
        or _infer_source_category(payload=payload, source_path=source_path)
    )
    input_mode = _normalize_input_mode(_string_or_none(payload.get("input_mode")))
    return {"input_source_path": source_path, "input_source_category": source_category, "input_mode": input_mode}


def _provenance_failures(input_context: Mapping[str, Any]) -> list[str]:
    failures: list[str] = []
    category = str(input_context.get("input_source_category") or SOURCE_CATEGORY_UNKNOWN)
    mode = str(input_context.get("input_mode") or "")
    if not input_context.get("input_source_path") or category == SOURCE_CATEGORY_UNKNOWN:
        failures.append(MISSING_PROVENANCE)
    if mode == INPUT_MODE_RUNTIME_DECISION and category == SOURCE_CATEGORY_RESEARCH:
        failures.append(RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE)
    if mode == INPUT_MODE_RUNTIME_DECISION and category != SOURCE_CATEGORY_RUNTIME:
        failures.append(MISSING_PROVENANCE)
    return _dedupe(failures)


def _source_provenance_status(input_context: Mapping[str, Any]) -> str:
    category = str(input_context.get("input_source_category") or SOURCE_CATEGORY_UNKNOWN)
    mode = str(input_context.get("input_mode") or "")
    if mode == INPUT_MODE_RUNTIME_DECISION and category == SOURCE_CATEGORY_RESEARCH:
        return RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE
    if category == SOURCE_CATEGORY_RUNTIME:
        return "RUNTIME_SOURCE_DECLARED"
    if category == SOURCE_CATEGORY_TEST_FIXTURE:
        return "TEST_FIXTURE_NOT_RUNTIME_ELIGIBLE"
    if category == SOURCE_CATEGORY_RESEARCH:
        return "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"
    return "SOURCE_PROVENANCE_UNKNOWN"


def _freshness_status(
    *,
    candles: Sequence[NormalizedCandle],
    now: datetime,
    timeframe: str,
    thresholds: LifecycleAwarenessThresholds,
    warnings: list[str],
) -> str:
    if not candles:
        return "NO_COMPLETED_CANDLES"
    timeframe_delta = _timeframe_delta(timeframe)
    if timeframe_delta is None:
        warnings.append(f"unsupported lifecycle evaluation timeframe: {timeframe}.")
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
            warnings.append(f"missing candle interval detected between {previous.timestamp.isoformat()} and {current.timestamp.isoformat()}.")
            return "MISSING_INTERVAL"
    return "FRESH"


def _confidence(metrics: Mapping[str, Any], candle_validation: _CandleValidation) -> float:
    candle_score = min(len(candle_validation.candles) / max(LifecycleAwarenessThresholds().min_completed_candles, 1), 1.0)
    metric_score = 0.0
    metric_score += 0.34 if metrics["_has_mfe_mae_context"] else 0.0
    metric_score += 0.33 if metrics["_has_progress_context"] else 0.0
    metric_score += 0.33 if metrics["_has_age_context"] else 0.0
    return _clamp((0.55 * candle_score) + (0.45 * metric_score), 0.0, 1.0)


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
    for key in ("candles", "ohlcv_candles", "bars"):
        if key in payload:
            return payload.get(key)
    return None


def _source_id(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("source_id")
    if value is None:
        provenance = payload.get("runtime_provenance")
        if isinstance(provenance, Mapping):
            value = provenance.get("source_id")
    return _string_or_none(value)


def _normalize_input_mode(value: str | None) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {INPUT_MODE_RUNTIME_DECISION, INPUT_MODE_OFFLINE_EVALUATION, INPUT_MODE_TEST}:
        return normalized
    return "UNKNOWN"


def _normalize_source_category(value: str | None) -> str:
    normalized = str(value or "").strip().upper()
    if normalized in {SOURCE_CATEGORY_RUNTIME, SOURCE_CATEGORY_RESEARCH, SOURCE_CATEGORY_TEST_FIXTURE}:
        return normalized
    return SOURCE_CATEGORY_UNKNOWN


def _infer_source_category(*, payload: Mapping[str, Any], source_path: str | None) -> str:
    normalized_path = str(source_path or "").replace("\\", "/").lower()
    if "outputs/track_b_research/" in normalized_path:
        return SOURCE_CATEGORY_RESEARCH
    if bool(payload.get("test_fixture")):
        return SOURCE_CATEGORY_TEST_FIXTURE
    if bool(payload.get("runtime_provenance")) or "outputs/track_b_execution_core/" in normalized_path:
        return SOURCE_CATEGORY_RUNTIME
    return SOURCE_CATEGORY_UNKNOWN


def _normalize_timeframe(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "1m", "1min", "1minute", "1minutes"}:
        return "1m"
    if normalized in {"5", "5m", "5min", "5minute", "5minutes"}:
        return "5m"
    if normalized in {"15", "15m", "15min", "15minute", "15minutes"}:
        return "15m"
    return normalized


def _timeframe_delta(timeframe: str) -> timedelta | None:
    if timeframe == "1m":
        return timedelta(minutes=1)
    if timeframe == "5m":
        return timedelta(minutes=5)
    if timeframe == "15m":
        return timedelta(minutes=15)
    return None


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


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


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
