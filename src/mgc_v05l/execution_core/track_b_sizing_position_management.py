"""Offline Track B sizing and position-management advisory evaluator.

This module emits advisory sizing context only. It has no broker dependency,
no strategy authority, no order-intent creation, and no lifecycle writes.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "track_b_sizing_position_management_state_v1"
PRODUCER_NAME = "TrackBSizingPositionManagementLayer"


class InitialSizeContext(str, Enum):
    BASE_UNIT = "BASE_UNIT"
    REDUCED_UNIT = "REDUCED_UNIT"
    NO_POSITION = "NO_POSITION"
    UNKNOWN = "UNKNOWN"


class InPositionSizeContext(str, Enum):
    HOLD_FULL = "HOLD_FULL"
    REDUCE_PARTIAL = "REDUCE_PARTIAL"
    DO_NOT_ADD = "DO_NOT_ADD"
    STOP_ADDING = "STOP_ADDING"
    UNKNOWN = "UNKNOWN"


class AddSizeContext(str, Enum):
    NOT_ALLOWED_V1 = "NOT_ALLOWED_V1"


BASE_UNIT = InitialSizeContext.BASE_UNIT.value
REDUCED_UNIT = InitialSizeContext.REDUCED_UNIT.value
NO_POSITION = InitialSizeContext.NO_POSITION.value
UNKNOWN = InitialSizeContext.UNKNOWN.value

HOLD_FULL = InPositionSizeContext.HOLD_FULL.value
REDUCE_PARTIAL = InPositionSizeContext.REDUCE_PARTIAL.value
DO_NOT_ADD = InPositionSizeContext.DO_NOT_ADD.value
STOP_ADDING = InPositionSizeContext.STOP_ADDING.value

NOT_ALLOWED_V1 = AddSizeContext.NOT_ALLOWED_V1.value

LOW_CONFIDENCE_CONTEXT = "LOW_CONFIDENCE_CONTEXT"
SOURCE_FAILURE_REASONS_PRESENT = "SOURCE_FAILURE_REASONS_PRESENT"
STALE_OR_UNRECONCILED_CONTEXT = "STALE_OR_UNRECONCILED_CONTEXT"
ENTRY_ACCEPTANCE_NOT_CLEAN = "ENTRY_ACCEPTANCE_NOT_CLEAN"
DEGRADED_CONTEXT_REDUCED_UNIT = "DEGRADED_CONTEXT_REDUCED_UNIT"
HOSTILE_CONTEXT_REDUCE_PARTIAL = "HOSTILE_CONTEXT_REDUCE_PARTIAL"
CLEAN_CONTEXT_BASE_UNIT = "CLEAN_CONTEXT_BASE_UNIT"
SUPPORTIVE_CONTEXT_HOLD_FULL = "SUPPORTIVE_CONTEXT_HOLD_FULL"
ADD_SIZE_DISABLED_V1 = "ADD_SIZE_DISABLED_V1"

SAFETY_FLAGS = {
    "strategy_authority": False,
    "broker_state_mutated": False,
    "submit_attempted": False,
    "order_intent_created": False,
    "lifecycle_mutated": False,
    "runtime_trade_eligible": False,
}

FAIL_CLOSED_STATES = {
    "LOW_CONFIDENCE_STALE_OR_UNRECONCILED",
    "REGIME_THIN_OR_STALE",
    "REGIME_LOW_CONFIDENCE",
    "LIQUIDITY_STALE_OR_INCOMPLETE",
    "LOW_CONFIDENCE_THIN_DATA",
    "NO_EXIT_LOW_CONFIDENCE",
    "STRUCTURALLY_INVALID",
    "REJECTED",
    "STALE",
    "UNRECONCILED",
}

DEGRADED_STATES = {
    "DECAYING",
    "ADVERSE_DOMINANCE",
    "DEFENSIVE_MANAGEMENT",
    "EXIT_RECOMMENDED_CONTEXT",
    "PARTICIPATION_COLLAPSE",
    "BULLISH_PARTICIPATION_COLLAPSE",
    "BEARISH_PARTICIPATION_COLLAPSE",
    "IMPULSE_DECAYING",
    "BULLISH_IMPULSE_DECAYING",
    "BEARISH_IMPULSE_DECAYING",
    "REGIME_COMPRESSION",
    "REGIME_CHOP_BALANCED",
    "DEFENSIVE_TIGHT",
    "PROTECTIVE_HARD_STOP",
    "PARTICIPATION_COLLAPSE_EXIT",
    "DEGRADED_BUT_VALID_MATCH",
    "NEAR_STRUCTURAL_MATCH",
}

SUPPORTIVE_STATES = {
    "FAVORABLE_EXPANSION",
    "HEALTHY_PULLBACK",
    "WORKING_IN_FAVOR",
    "PERSISTENT_BULLISH_PRESSURE",
    "PERSISTENT_BEARISH_PRESSURE",
    "BULLISH_PERSISTENT_PRESSURE",
    "BEARISH_PERSISTENT_PRESSURE",
    "REGIME_TRENDING",
    "REGIME_EXPANSION",
    "EXPANSION_DIRECTIONAL",
    "PATIENT_CONTINUATION",
    "NORMAL_CONTINUATION",
    "EXACT_STRUCTURAL_MATCH",
}


def build_sizing_position_management_state(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Build deterministic advisory sizing context from explicit inputs."""

    if not isinstance(payload, Mapping):
        payload = {}

    entry_context = _first_mapping(payload, "entry_acceptance_context", "entry_acceptance")
    lifecycle_context = _first_mapping(payload, "lifecycle_awareness_context", "lifecycle_awareness")
    pressure_context = _first_mapping(
        payload,
        "participation_pressure_context_v1",
        "participation_pressure_context",
        "participation_quality_context",
    )
    regime_context = _first_mapping(payload, "regime_session_context_v1", "regime_session_context")
    exit_context = _first_mapping(payload, "exit_context", "exit_profile_context")

    source_contexts = {
        "entry_acceptance_context": entry_context,
        "lifecycle_awareness_context": lifecycle_context,
        "participation_pressure_context_v1": pressure_context,
        "regime_session_context_v1": regime_context,
        "exit_context": exit_context,
    }
    compatibility = _compatibility_fields(payload, source_contexts)
    failures = _failure_reasons(source_contexts)
    low_confidence = bool(failures)
    degraded = _has_any_state(source_contexts, DEGRADED_STATES)
    supportive = _has_any_state(source_contexts, SUPPORTIVE_STATES)

    exact_entry = _is_exact_high_confidence_entry(entry_context)
    in_position = bool(lifecycle_context)

    sizing_reasons: list[str] = [ADD_SIZE_DISABLED_V1]
    management_reasons: list[str] = [ADD_SIZE_DISABLED_V1]
    warning_reasons: list[str] = []

    if low_confidence:
        initial_size_context = NO_POSITION
        in_position_size_context = UNKNOWN
        confidence = 0.0
        sizing_reasons.append(STALE_OR_UNRECONCILED_CONTEXT)
        management_reasons.append(STALE_OR_UNRECONCILED_CONTEXT)
    else:
        if exact_entry and supportive and not degraded:
            initial_size_context = BASE_UNIT
            confidence = _combined_confidence(source_contexts, default=0.82)
            sizing_reasons.append(CLEAN_CONTEXT_BASE_UNIT)
        elif degraded:
            initial_size_context = REDUCED_UNIT
            confidence = min(_combined_confidence(source_contexts, default=0.58), 0.62)
            sizing_reasons.append(DEGRADED_CONTEXT_REDUCED_UNIT)
        elif entry_context:
            initial_size_context = UNKNOWN
            confidence = min(_combined_confidence(source_contexts, default=0.42), 0.50)
            warning_reasons.append(ENTRY_ACCEPTANCE_NOT_CLEAN)
        else:
            initial_size_context = UNKNOWN
            confidence = min(_combined_confidence(source_contexts, default=0.35), 0.45)
            warning_reasons.append("MISSING_ENTRY_ACCEPTANCE_CONTEXT")

        in_position_size_context = _classify_in_position_context(
            in_position=in_position,
            source_contexts=source_contexts,
            degraded=degraded,
            supportive=supportive,
            management_reasons=management_reasons,
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "initial_size_context": initial_size_context,
        "in_position_size_context": in_position_size_context,
        "add_size_context": NOT_ALLOWED_V1,
        "confidence": round(confidence, 4),
        "sizing_reasons": _dedupe(sizing_reasons),
        "management_reasons": _dedupe(management_reasons),
        "warning_reasons": _dedupe(warning_reasons),
        "failure_reasons": _dedupe(failures),
        "strategy_family": compatibility["strategy_family"],
        "exit_profile": compatibility["exit_profile"],
        "instrument": compatibility["instrument"],
        "timeframe": compatibility["timeframe"],
        "source_contexts": source_contexts,
        **SAFETY_FLAGS,
    }


def _classify_in_position_context(
    *,
    in_position: bool,
    source_contexts: Mapping[str, Mapping[str, Any]],
    degraded: bool,
    supportive: bool,
    management_reasons: list[str],
) -> str:
    if not in_position:
        management_reasons.append("NO_LIFECYCLE_AWARENESS_CONTEXT")
        return UNKNOWN
    if _has_any_state(
        source_contexts,
        {
            "ADVERSE_DOMINANCE",
            "DECAYING",
            "DEFENSIVE_MANAGEMENT",
            "EXIT_RECOMMENDED_CONTEXT",
            "PARTICIPATION_COLLAPSE",
            "BULLISH_PARTICIPATION_COLLAPSE",
            "BEARISH_PARTICIPATION_COLLAPSE",
            "DEFENSIVE_TIGHT",
            "PROTECTIVE_HARD_STOP",
            "PARTICIPATION_COLLAPSE_EXIT",
        },
    ):
        management_reasons.append(HOSTILE_CONTEXT_REDUCE_PARTIAL)
        return REDUCE_PARTIAL
    if _has_any_state(source_contexts, {"STALLED", "REGIME_COMPRESSION", "REGIME_CHOP_BALANCED"}):
        management_reasons.append(DEGRADED_CONTEXT_REDUCED_UNIT)
        return DO_NOT_ADD
    if degraded:
        management_reasons.append("DEGRADED_CONTEXT_STOP_ADDING")
        return STOP_ADDING
    if supportive:
        management_reasons.append(SUPPORTIVE_CONTEXT_HOLD_FULL)
        return HOLD_FULL
    management_reasons.append("IN_POSITION_CONTEXT_INDETERMINATE")
    return UNKNOWN


def _compatibility_fields(
    payload: Mapping[str, Any],
    source_contexts: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    entry_context = source_contexts["entry_acceptance_context"]
    lifecycle_context = source_contexts["lifecycle_awareness_context"]
    regime_context = source_contexts["regime_session_context_v1"]
    exit_context = source_contexts["exit_context"]
    strategy_family = _first_text(
        payload,
        entry_context,
        lifecycle_context,
        keys=("strategy_family", "candidate_family", "strategy_id", "family"),
    )
    exit_profile = _first_text(
        payload,
        exit_context,
        keys=("exit_profile", "exit_profile_context", "selected_exit_profile", "profile"),
    )
    instrument = _first_text(
        payload,
        entry_context,
        lifecycle_context,
        regime_context,
        keys=("instrument", "symbol", "root_symbol"),
    )
    timeframe = _first_text(
        payload,
        entry_context,
        lifecycle_context,
        regime_context,
        keys=("timeframe", "candidate_timeframe", "lifecycle_evaluation_timeframe", "candle_timeframe"),
    )
    return {
        "strategy_family": strategy_family or UNKNOWN,
        "exit_profile": exit_profile or UNKNOWN,
        "instrument": instrument or UNKNOWN,
        "timeframe": timeframe or UNKNOWN,
    }


def _failure_reasons(source_contexts: Mapping[str, Mapping[str, Any]]) -> list[str]:
    failures: list[str] = []
    for name, context in source_contexts.items():
        if not context:
            continue
        source_failures = _as_text_list(context.get("failure_reasons"))
        confidence_failures = _as_text_list(context.get("confidence_failure_reasons"))
        if source_failures or confidence_failures:
            failures.append(f"{name}:{SOURCE_FAILURE_REASONS_PRESENT}")
            failures.extend(source_failures)
            failures.extend(confidence_failures)
        if _context_confidence(context) < 0.25:
            failures.append(f"{name}:{LOW_CONFIDENCE_CONTEXT}")
        for value in _state_values(context):
            if value in FAIL_CLOSED_STATES:
                failures.append(f"{name}:{STALE_OR_UNRECONCILED_CONTEXT}")
                failures.append(value)
    return _dedupe(failures)


def _is_exact_high_confidence_entry(entry_context: Mapping[str, Any]) -> bool:
    if not entry_context:
        return False
    entry_state = str(
        entry_context.get("acceptance_class")
        or entry_context.get("entry_acceptance_state")
        or entry_context.get("decision")
        or ""
    ).upper()
    return entry_state == "EXACT_STRUCTURAL_MATCH" and _context_confidence(entry_context) >= 0.75


def _has_any_state(source_contexts: Mapping[str, Mapping[str, Any]], target_states: set[str]) -> bool:
    for context in source_contexts.values():
        if any(value in target_states for value in _state_values(context)):
            return True
    return False


def _state_values(context: Mapping[str, Any]) -> tuple[str, ...]:
    if not context:
        return ()
    values: list[str] = []
    for key, value in context.items():
        if key.endswith("_state") or key.endswith("_context") or key in {
            "acceptance_class",
            "decision",
            "exit_profile",
            "exit_profile_context",
            "selected_exit_profile",
            "liquidity_state",
            "market_regime_state",
            "trend_chop_state",
            "pressure_state",
            "lifecycle_awareness_state",
        }:
            if isinstance(value, str):
                values.append(value.upper())
    return tuple(_dedupe(values))


def _combined_confidence(
    source_contexts: Mapping[str, Mapping[str, Any]],
    *,
    default: float,
) -> float:
    confidences = [
        _context_confidence(context)
        for context in source_contexts.values()
        if context and _context_confidence(context) > 0.0
    ]
    if not confidences:
        return default
    return min(default, sum(confidences) / len(confidences))


def _context_confidence(context: Mapping[str, Any]) -> float:
    for key in ("confidence", "pressure_confidence", "acceptance_score", "score"):
        value = context.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return max(0.0, min(float(value), 1.0))
    return 1.0


def _first_mapping(payload: Mapping[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _first_text(*contexts: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for context in contexts:
        for key in keys:
            value = context.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _as_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result
