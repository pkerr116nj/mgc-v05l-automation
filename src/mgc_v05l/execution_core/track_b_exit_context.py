"""Offline Track B exit context evaluator.

This module emits advisory exit context only. It does not create order
intents, mutate lifecycle state, import broker APIs, or carry strategy
authority.
"""

from __future__ import annotations

import json
import math
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "track_b_exit_context_state_v1"
PRODUCER_NAME = "TrackBExitContextEvaluator"
DEFAULT_EXIT_CONTEXT_ARTIFACT_PATH = (
    Path(__file__).resolve().parents[3]
    / "outputs"
    / "track_b_execution_core"
    / "exit_context"
    / "latest_exit_context_state.json"
)

PATIENT_CONTINUATION = "PATIENT_CONTINUATION"
NORMAL_CONTINUATION = "NORMAL_CONTINUATION"
DEFENSIVE_TIGHT = "DEFENSIVE_TIGHT"
TIME_BOXED = "TIME_BOXED"
PROTECTIVE_HARD_STOP = "PROTECTIVE_HARD_STOP"
PARTICIPATION_COLLAPSE_EXIT = "PARTICIPATION_COLLAPSE_EXIT"
NO_EXIT_LOW_CONFIDENCE = "NO_EXIT_LOW_CONFIDENCE"

THIN_DATA = "THIN_DATA"
STALE_INPUT = "STALE_INPUT"
INCOMPLETE_CANDLES = "INCOMPLETE_CANDLES"
MIXED_TIMEFRAME = "MIXED_TIMEFRAME"
AMBIGUOUS_TIMEFRAME = "AMBIGUOUS_TIMEFRAME"
MALFORMED_CANDLES = "MALFORMED_CANDLES"
LOW_RANGE_QUALITY = "LOW_RANGE_QUALITY"
MISSING_PROVENANCE = "MISSING_PROVENANCE"
UNRECONCILED_POSITION_CONTEXT = "UNRECONCILED_POSITION_CONTEXT"
RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE = "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"
MISSING_LIFECYCLE_POSITION = "MISSING_LIFECYCLE_POSITION"
MISSING_ENTRY_CONTEXT = "MISSING_ENTRY_CONTEXT"
MISSING_PARTICIPATION_CONTEXT = "MISSING_PARTICIPATION_CONTEXT"

INPUT_MODE_RUNTIME_DECISION = "RUNTIME_DECISION"
INPUT_MODE_OFFLINE_EVALUATION = "OFFLINE_EVALUATION"
INPUT_MODE_TEST = "TEST"
SOURCE_CATEGORY_RUNTIME = "RUNTIME"
SOURCE_CATEGORY_RESEARCH = "RESEARCH"
SOURCE_CATEGORY_TEST_FIXTURE = "TEST_FIXTURE"
SOURCE_CATEGORY_UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class ExitContextThresholds:
    min_completed_candles: int = 12
    stale_after_intervals: float = 3.0
    max_missing_gap_intervals: float = 1.5
    tiny_range_pct: float = 0.00005
    default_max_bars_in_trade: int = 12
    protective_mae_points: float = 3.0
    reduce_mae_points: float = 1.75
    reduce_mfe_giveback_ratio: float = 0.60
    low_participation_confidence: float = 0.35


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

    @property
    def range_pct(self) -> float:
        return self.range / self.close


@dataclass(frozen=True)
class _ValidationResult:
    candles: tuple[NormalizedCandle, ...]
    freshness_status: str
    failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def build_exit_context_state(
    payload: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
    thresholds: ExitContextThresholds | None = None,
) -> dict[str, Any]:
    """Build deterministic advisory exit context from offline inputs."""

    resolved_now = _coerce_now(now)
    resolved_thresholds = thresholds or ExitContextThresholds()
    input_context = _input_context(payload)
    timeframe_context = _timeframe_context(payload)
    failures: list[str] = []
    warnings: list[str] = []

    failures.extend(_provenance_failures(input_context))
    failures.extend(_timeframe_failures(timeframe_context))

    position = _position_context(payload)
    entry = _entry_context(payload)
    participation = _participation_context(payload)
    reconciliation = _reconciliation_context(payload)

    if not position["position_id"] or position["side"] not in {"LONG", "SHORT"}:
        failures.append(MISSING_LIFECYCLE_POSITION)
    if not entry["entry_type"] or not entry["entry_quality"]:
        failures.append(MISSING_ENTRY_CONTEXT)
    if not participation["participation_state"]:
        failures.append(MISSING_PARTICIPATION_CONTEXT)
    if reconciliation["status"] not in {"MATCHED", "RECONCILED", "PAPER_MATCHED"}:
        failures.append(UNRECONCILED_POSITION_CONTEXT)

    raw_candles = _extract_candle_rows(payload)
    candles_received = len(raw_candles) if _is_sequence(raw_candles) else 0
    if not _is_sequence(raw_candles):
        validation = _ValidationResult(
            candles=(),
            freshness_status="NO_COMPLETED_CANDLES",
            failure_reasons=tuple(_dedupe([*failures, THIN_DATA, MALFORMED_CANDLES])),
            warnings=tuple([*warnings, "candle input must be a sequence under candles, ohlcv_candles, or bars."]),
        )
        return _low_confidence_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            position=position,
            entry=entry,
            participation=participation,
            reconciliation=reconciliation,
            validation=validation,
            candles_received=0,
        )

    validation = _validate_candles(
        raw_candles,
        now=resolved_now,
        expected_timeframe=timeframe_context["exit_management_timeframe"],
        thresholds=resolved_thresholds,
        initial_failures=failures,
        initial_warnings=warnings,
    )
    if validation.failure_reasons:
        return _low_confidence_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            timeframe_context=timeframe_context,
            position=position,
            entry=entry,
            participation=participation,
            reconciliation=reconciliation,
            validation=validation,
            candles_received=candles_received,
        )

    metrics = _position_metrics(payload)
    module_context = _evaluate_modules(
        position=position,
        participation=participation,
        metrics=metrics,
        thresholds=resolved_thresholds,
    )
    profile = _select_profile(module_context=module_context, participation=participation, metrics=metrics)
    confidence = _confidence(participation=participation, metrics=metrics, completed_count=len(validation.candles), thresholds=resolved_thresholds)

    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "ADVISORY_EXIT_CONTEXT_ONLY",
        **input_context,
        "source_id": _source_id(payload),
        "instrument": str(payload.get("instrument") or payload.get("symbol") or "UNKNOWN").upper(),
        "generated_at": resolved_now.isoformat(),
        **timeframe_context,
        "candles_received": candles_received,
        "completed_candles_used": len(validation.candles),
        "latest_candle_timestamp": validation.candles[-1].timestamp.isoformat(),
        "freshness_status": validation.freshness_status,
        "lifecycle_position_context": position,
        "entry_acceptance_context": entry,
        "participation_quality_context": participation,
        "reconciliation_context": reconciliation,
        "position_metrics_context": metrics,
        "exit_profile_context": profile,
        "exit_urgency_context": module_context["exit_urgency_context"],
        "hold_quality_context": module_context["hold_quality_context"],
        "reduce_size_context": module_context["reduce_size_context"],
        "scale_up_context": module_context["scale_up_context"],
        "protective_exit_context": module_context["protective_exit_context"],
        "exit_reasons": module_context["exit_reasons"],
        "hold_reasons": module_context["hold_reasons"],
        "confidence": _round(confidence),
        "failure_reasons": [],
        "warnings": list(validation.warnings),
        **_safety_flags(),
    }


def write_exit_context_state(
    payload: Mapping[str, Any],
    *,
    output_path: str | Path,
    now: datetime | str | None = None,
    thresholds: ExitContextThresholds | None = None,
) -> dict[str, Any]:
    """Build and write an exit context artifact to a tmp/test path only."""

    resolved_path = Path(output_path).resolve()
    if not _is_tmp_or_test_path(resolved_path):
        raise ValueError("exit context writer is restricted to tmp/test paths in this implementation slice")
    report = build_exit_context_state(payload, now=now, thresholds=thresholds)
    report = {**report, "artifact_path": str(resolved_path)}
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    resolved_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def _validate_candles(
    rows: Sequence[Any],
    *,
    now: datetime,
    expected_timeframe: str,
    thresholds: ExitContextThresholds,
    initial_failures: Sequence[str],
    initial_warnings: Sequence[str],
) -> _ValidationResult:
    candles: list[NormalizedCandle] = []
    failures = list(initial_failures)
    warnings = list(initial_warnings)
    skipped_incomplete = 0
    mixed_timeframe_count = 0

    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            failures.append(MALFORMED_CANDLES)
            warnings.append(f"candle[{index}] must be an object.")
            continue
        if row.get("completed") is False:
            skipped_incomplete += 1
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
            close = _finite_float(row["close"])
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
        candles.append(NormalizedCandle(timestamp=timestamp, open=open_price, high=high, low=low, close=close, timeframe=row_timeframe))

    if skipped_incomplete:
        warnings.append(f"excluded {skipped_incomplete} incomplete candle(s).")
    if mixed_timeframe_count:
        failures.append(MIXED_TIMEFRAME)
        warnings.append(f"rejected input with {mixed_timeframe_count} candle(s) outside the declared exit management timeframe.")
    for previous, current in zip(candles, candles[1:]):
        if current.timestamp <= previous.timestamp:
            failures.append(MALFORMED_CANDLES)
            warnings.append("candle timestamps are duplicated or out of order.")
            break
    if skipped_incomplete and len(candles) < thresholds.min_completed_candles:
        failures.append(INCOMPLETE_CANDLES)
    if len(candles) < thresholds.min_completed_candles:
        failures.append(THIN_DATA)
    freshness_status = _freshness_status(candles=candles, now=now, timeframe=expected_timeframe, thresholds=thresholds, warnings=warnings)
    if freshness_status in {"STALE", "FUTURE_TIMESTAMP", "MISSING_INTERVAL"}:
        failures.append(STALE_INPUT)
    if freshness_status in {"THIN_RANGE", "UNCHANGED_PRICES"}:
        failures.append(LOW_RANGE_QUALITY)
    return _ValidationResult(
        candles=tuple(candles),
        freshness_status=freshness_status,
        failure_reasons=tuple(_dedupe(failures)),
        warnings=tuple(warnings),
    )


def _evaluate_modules(
    *,
    position: Mapping[str, Any],
    participation: Mapping[str, Any],
    metrics: Mapping[str, Any],
    thresholds: ExitContextThresholds,
) -> dict[str, Any]:
    side = str(position["side"])
    state = str(participation["participation_state"])
    hold_quality = str(participation["side_hold_quality"])
    urgency = "LOW"
    hold_context = "SUPPORTIVE" if hold_quality in {"SUPPORTIVE", "HEALTHY"} else "NEUTRAL"
    reduce_context = "HOLD_FULL_SIZE"
    scale_context = "SCALE_ONLY_AFTER_CONTINUATION_CONFIRMATION"
    protective_context = {"active": False, "reason": None, "urgency": "NONE"}
    exit_reasons: list[str] = []
    hold_reasons: list[str] = []

    mae = float(metrics["mae_points"])
    mfe = float(metrics["mfe_points"])
    giveback = float(metrics["mfe_giveback_ratio"])
    bars = int(metrics["bars_since_fill"])
    max_bars = int(metrics["max_bars_in_trade"])

    if bool(metrics["protective_stop_breached"]) or mae >= thresholds.protective_mae_points:
        urgency = "PROTECTIVE"
        hold_context = "HOSTILE"
        reduce_context = "REDUCE_SIZE_CONTEXT"
        scale_context = "NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION"
        protective_context = {"active": True, "reason": "protective risk boundary is active.", "urgency": "PROTECTIVE"}
        exit_reasons.append("hard protective stop context is active.")
    elif "PARTICIPATION_COLLAPSE" in state:
        urgency = "HIGH"
        hold_context = "HOSTILE"
        reduce_context = "REDUCE_SIZE_CONTEXT"
        scale_context = "NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION"
        exit_reasons.append(f"{side.lower()} participation context has collapsed.")
    elif bars >= max_bars:
        urgency = "ELEVATED"
        hold_context = "DEGRADED"
        reduce_context = "REDUCE_SIZE_CONTEXT"
        scale_context = "NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION"
        exit_reasons.append(f"time-box expired after {bars} bars.")
    elif hold_quality in {"DEGRADING", "HOSTILE"} or "IMPULSE_DECAYING" in state:
        urgency = "ELEVATED"
        hold_context = "DEGRADED"
        reduce_context = "REDUCE_SIZE_CONTEXT" if mae >= thresholds.reduce_mae_points or giveback >= thresholds.reduce_mfe_giveback_ratio else "HOLD_FULL_SIZE"
        scale_context = "NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION"
        exit_reasons.append("impulse or participation context is decaying.")
    elif mae >= thresholds.reduce_mae_points or (mfe > 0 and giveback >= thresholds.reduce_mfe_giveback_ratio):
        urgency = "ELEVATED"
        hold_context = "DEGRADED"
        reduce_context = "REDUCE_SIZE_CONTEXT"
        scale_context = "NO_ADD_DEGRADED_ENTRY_OR_WEAK_PARTICIPATION"
        exit_reasons.append("adverse excursion or MFE giveback supports reduce-size context.")
    else:
        hold_context = "SUPPORTIVE" if hold_quality == "SUPPORTIVE" else "NEUTRAL"
        urgency = "LOW"
        reduce_context = "HOLD_FULL_SIZE"
        scale_context = "SCALE_ONLY_AFTER_CONTINUATION_CONFIRMATION"
        hold_reasons.append("participation and pullback context support patience.")

    if not hold_reasons and hold_context != "HOSTILE":
        hold_reasons.append("no hard protective context is active.")

    return {
        "exit_urgency_context": urgency,
        "hold_quality_context": hold_context,
        "reduce_size_context": reduce_context,
        "scale_up_context": scale_context,
        "protective_exit_context": protective_context,
        "exit_reasons": exit_reasons,
        "hold_reasons": hold_reasons,
    }


def _select_profile(
    *,
    module_context: Mapping[str, Any],
    participation: Mapping[str, Any],
    metrics: Mapping[str, Any],
) -> str:
    urgency = str(module_context["exit_urgency_context"])
    state = str(participation["participation_state"])
    if urgency == "PROTECTIVE":
        return PROTECTIVE_HARD_STOP
    if "PARTICIPATION_COLLAPSE" in state:
        return PARTICIPATION_COLLAPSE_EXIT
    if int(metrics["bars_since_fill"]) >= int(metrics["max_bars_in_trade"]):
        return TIME_BOXED
    if urgency == "ELEVATED":
        return DEFENSIVE_TIGHT
    if "HEALTHY_PULLBACK" in state or "CONTINUATION_CONFIRMED" in state:
        return PATIENT_CONTINUATION
    return NORMAL_CONTINUATION


def _low_confidence_report(
    *,
    payload: Mapping[str, Any],
    generated_at: datetime,
    input_context: Mapping[str, Any],
    timeframe_context: Mapping[str, Any],
    position: Mapping[str, Any],
    entry: Mapping[str, Any],
    participation: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    validation: _ValidationResult,
    candles_received: int,
) -> dict[str, Any]:
    latest = validation.candles[-1].timestamp if validation.candles else None
    reasons = _dedupe(validation.failure_reasons)
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "ADVISORY_EXIT_CONTEXT_ONLY",
        **input_context,
        "source_id": _source_id(payload),
        "instrument": str(payload.get("instrument") or payload.get("symbol") or "UNKNOWN").upper(),
        "generated_at": generated_at.isoformat(),
        **timeframe_context,
        "candles_received": candles_received,
        "completed_candles_used": len(validation.candles),
        "latest_candle_timestamp": latest.isoformat() if latest else None,
        "freshness_status": validation.freshness_status,
        "lifecycle_position_context": position,
        "entry_acceptance_context": entry,
        "participation_quality_context": participation,
        "reconciliation_context": reconciliation,
        "position_metrics_context": _position_metrics(payload),
        "exit_profile_context": NO_EXIT_LOW_CONFIDENCE,
        "exit_urgency_context": "NONE",
        "hold_quality_context": "LOW_CONFIDENCE",
        "reduce_size_context": "NO_SIZE_ADJUSTMENT_LOW_CONFIDENCE",
        "scale_up_context": "NO_SIZE_ADJUSTMENT_LOW_CONFIDENCE",
        "protective_exit_context": {"active": False, "reason": None, "urgency": "NONE"},
        "exit_reasons": [],
        "hold_reasons": ["exit context failed closed because input confidence is insufficient."],
        "confidence": 0.0,
        "failure_reasons": reasons,
        "warnings": list(validation.warnings),
        **_safety_flags(),
    }


def _position_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("lifecycle_position") or payload.get("position"))
    return {
        "position_id": _string_or_none(raw.get("position_id") or raw.get("lifecycle_position_id") or payload.get("position_id")),
        "strategy_id": _string_or_none(raw.get("strategy_id") or payload.get("strategy_id")),
        "lane_id": _string_or_none(raw.get("lane_id") or payload.get("lane_id")),
        "symbol": _string_or_none(raw.get("symbol") or payload.get("symbol") or payload.get("instrument")),
        "side": str(raw.get("side") or payload.get("side") or "").strip().upper(),
        "quantity": _optional_finite_float(raw.get("quantity") or raw.get("qty") or payload.get("quantity")),
        "entry_timestamp": _string_or_none(raw.get("entry_timestamp") or payload.get("entry_timestamp")),
    }


def _entry_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("entry_acceptance_context") or payload.get("entry_context"))
    return {
        "entry_type": _string_or_none(raw.get("entry_type")),
        "entry_quality": _string_or_none(raw.get("entry_quality")),
        "entry_timeframe": _normalize_timeframe(str(raw.get("entry_timeframe") or payload.get("entry_timeframe") or "")),
        "acceptance_reason": _string_or_none(raw.get("acceptance_reason")),
    }


def _participation_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("participation_quality_context") or payload.get("participation"))
    side = str(_as_mapping(payload.get("lifecycle_position") or payload.get("position")).get("side") or payload.get("side") or "").strip().upper()
    side_hold_key = "long_hold_quality" if side == "LONG" else "short_hold_quality"
    side_urgency_key = "long_exit_urgency_context" if side == "LONG" else "short_exit_urgency_context"
    return {
        "participation_state": _string_or_none(raw.get("participation_state")),
        "side_hold_quality": _string_or_none(raw.get(side_hold_key) or raw.get("hold_quality")),
        "side_exit_urgency_context": _string_or_none(raw.get(side_urgency_key) or raw.get("exit_urgency_context")),
        "pullback_health": _string_or_none(raw.get("pullback_health")),
        "continuation_confidence": _optional_finite_float(raw.get("continuation_confidence")),
        "confidence": _optional_finite_float(raw.get("confidence")),
        "confidence_failure_reasons": list(raw.get("confidence_failure_reasons") or []),
    }


def _reconciliation_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("reconciliation_context") or payload.get("broker_lifecycle_reconciliation"))
    return {
        "status": str(raw.get("status") or payload.get("reconciliation_status") or "").strip().upper(),
        "source": _string_or_none(raw.get("source")),
        "read_only": True,
    }


def _position_metrics(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("position_metrics") or payload.get("mfe_mae"))
    mfe = _optional_finite_float(raw.get("mfe_points") or payload.get("mfe_points")) or 0.0
    mae = abs(_optional_finite_float(raw.get("mae_points") or payload.get("mae_points")) or 0.0)
    current_favorable = _optional_finite_float(raw.get("current_favorable_points") or payload.get("current_favorable_points"))
    giveback = 0.0
    if mfe > 0 and current_favorable is not None:
        giveback = _clamp((mfe - current_favorable) / mfe, 0.0, 1.0)
    return {
        "mfe_points": _round(mfe),
        "mae_points": _round(mae),
        "current_favorable_points": _round(current_favorable or 0.0),
        "mfe_giveback_ratio": _round(giveback),
        "bars_since_fill": int(_optional_finite_float(raw.get("bars_since_fill") or payload.get("bars_since_fill")) or 0),
        "max_bars_in_trade": int(_optional_finite_float(raw.get("max_bars_in_trade") or payload.get("max_bars_in_trade")) or ExitContextThresholds().default_max_bars_in_trade),
        "protective_stop_breached": bool(raw.get("protective_stop_breached") or payload.get("protective_stop_breached")),
    }


def _timeframe_context(payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = _as_mapping(payload.get("timeframe_context") or payload.get("timeframes"))
    entry_timeframe = _normalize_timeframe(str(raw.get("entry_timeframe") or payload.get("entry_timeframe") or ""))
    exit_management_timeframe = _normalize_timeframe(str(raw.get("exit_management_timeframe") or payload.get("exit_management_timeframe") or ""))
    fast_reaction_timeframe = _normalize_timeframe(str(raw.get("fast_reaction_timeframe") or payload.get("fast_reaction_timeframe") or ""))
    trend_context_timeframe = _normalize_timeframe(str(raw.get("trend_context_timeframe") or payload.get("trend_context_timeframe") or ""))
    return {
        "entry_timeframe": entry_timeframe,
        "exit_management_timeframe": exit_management_timeframe,
        "fast_reaction_timeframe": fast_reaction_timeframe,
        "trend_context_timeframe": trend_context_timeframe,
        "timeframe_source": _string_or_none(raw.get("timeframe_source") or payload.get("timeframe_source")),
        "base_timeframe_if_derived": _normalize_timeframe(str(raw.get("base_timeframe_if_derived") or payload.get("base_timeframe_if_derived") or "")),
        "aggregation_method": _string_or_none(raw.get("aggregation_method") or payload.get("aggregation_method")),
        "anchor_rule": _string_or_none(raw.get("anchor_rule") or payload.get("anchor_rule")),
        "timeframe_alignment_status": str(raw.get("timeframe_alignment_status") or payload.get("timeframe_alignment_status") or "").strip().upper(),
    }


def _timeframe_failures(context: Mapping[str, Any]) -> list[str]:
    required = (
        "entry_timeframe",
        "exit_management_timeframe",
        "fast_reaction_timeframe",
        "trend_context_timeframe",
        "timeframe_source",
        "aggregation_method",
        "anchor_rule",
        "timeframe_alignment_status",
    )
    if any(not context.get(field) for field in required):
        return [AMBIGUOUS_TIMEFRAME]
    if context["timeframe_alignment_status"] != "ALIGNED":
        return [MIXED_TIMEFRAME]
    if not all(_timeframe_delta(str(context[field])) for field in ("entry_timeframe", "exit_management_timeframe", "fast_reaction_timeframe", "trend_context_timeframe")):
        return [AMBIGUOUS_TIMEFRAME]
    return []


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


def _freshness_status(
    *,
    candles: Sequence[NormalizedCandle],
    now: datetime,
    timeframe: str,
    thresholds: ExitContextThresholds,
    warnings: list[str],
) -> str:
    if not candles:
        return "NO_COMPLETED_CANDLES"
    timeframe_delta = _timeframe_delta(timeframe)
    if timeframe_delta is None:
        warnings.append(f"unsupported exit management timeframe: {timeframe}.")
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
    if _average([candle.range_pct for candle in candles]) < thresholds.tiny_range_pct:
        warnings.append("recent candle ranges are tiny relative to price.")
        return "THIN_RANGE"
    unchanged_count = sum(1 for previous, current in zip(candles, candles[1:]) if abs(current.close - previous.close) <= 1e-9)
    if unchanged_count >= max(1, len(candles) - 1):
        warnings.append("recent closes are unchanged across the completed horizon.")
        return "UNCHANGED_PRICES"
    return "FRESH"


def _confidence(
    *,
    participation: Mapping[str, Any],
    metrics: Mapping[str, Any],
    completed_count: int,
    thresholds: ExitContextThresholds,
) -> float:
    participation_confidence = participation.get("confidence")
    if participation_confidence is None:
        participation_confidence = 0.50
    data_score = min(completed_count / max(thresholds.min_completed_candles, 1), 1.0)
    excursion_score = 0.60 if float(metrics["mae_points"]) > 0 or float(metrics["mfe_points"]) > 0 else 0.40
    return _clamp((0.45 * float(participation_confidence)) + (0.35 * data_score) + (0.20 * excursion_score), 0.0, 1.0)


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
    if normalized in {"5", "5m", "5min", "5minute", "5minutes"}:
        return "5m"
    if normalized in {"1", "1m", "1min", "1minute", "1minutes"}:
        return "1m"
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


def _is_tmp_or_test_path(path: Path) -> bool:
    tmp_root = Path(tempfile.gettempdir()).resolve()
    try:
        path.relative_to(tmp_root)
        return True
    except ValueError:
        pass
    return "tests" in path.parts and "fixtures" in path.parts
