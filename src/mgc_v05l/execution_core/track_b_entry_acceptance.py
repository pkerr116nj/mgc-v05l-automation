"""Offline Track B entry-candidate acceptance scorer.

This module emits advisory entry-quality context only. It deliberately avoids
broker, strategy, order-intent, and lifecycle authority.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "track_b_entry_acceptance_state_v1"
PRODUCER_NAME = "TrackBEntryAcceptanceLayer"
DEFAULT_CANDLE_TIMEFRAME = "5m"
DEFAULT_ENTRY_ACCEPTANCE_ARTIFACT_PATH = (
    "outputs/track_b_execution_core/entry_acceptance/latest_entry_acceptance_state.json"
)
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FAMILY = "asiaEarlyNormalBreakoutRetestHoldLong"
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_RULE_ID = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_STATE_KEY = "asia_early_normal_breakout_retest_hold_long_state"
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURES_KEY = "asia_early_normal_breakout_retest_hold_long_features"
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_REQUIRED_PREDICATES = (
    "breakout_breaks_prior_1_high",
    "signal_retests_and_holds_breakout_level",
)
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_CONTEXT_PREDICATES = (
    "asia_early_or_gc_mgc_london_open",
    "allow_asia",
    "no_first_bull_snap_turn",
    "prior_bars_since_long_setup_gt_anti_churn",
    "breakout_bar_slope_is_flat",
    "breakout_bar_expansion_is_normal",
)


class AcceptanceClass(str, Enum):
    EXACT_STRUCTURAL_MATCH = "EXACT_STRUCTURAL_MATCH"
    NEAR_STRUCTURAL_MATCH = "NEAR_STRUCTURAL_MATCH"
    DEGRADED_BUT_VALID_MATCH = "DEGRADED_BUT_VALID_MATCH"
    STRUCTURALLY_INVALID = "STRUCTURALLY_INVALID"
    LOW_CONFIDENCE_INSUFFICIENT_DATA = "LOW_CONFIDENCE_INSUFFICIENT_DATA"


class CandidateSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class EntryAcceptanceLevel(str, Enum):
    EXACT = "EXACT"
    B_PLUS = "B_PLUS"
    NEAR = "NEAR"
    DEGRADED = "DEGRADED"
    INVALID = "INVALID"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"


EXACT_STRUCTURAL_MATCH = AcceptanceClass.EXACT_STRUCTURAL_MATCH.value
NEAR_STRUCTURAL_MATCH = AcceptanceClass.NEAR_STRUCTURAL_MATCH.value
DEGRADED_BUT_VALID_MATCH = AcceptanceClass.DEGRADED_BUT_VALID_MATCH.value
STRUCTURALLY_INVALID = AcceptanceClass.STRUCTURALLY_INVALID.value
LOW_CONFIDENCE_INSUFFICIENT_DATA = AcceptanceClass.LOW_CONFIDENCE_INSUFFICIENT_DATA.value
ENTRY_ACCEPTANCE_LEVEL_EXACT = EntryAcceptanceLevel.EXACT.value
ENTRY_ACCEPTANCE_LEVEL_B_PLUS = EntryAcceptanceLevel.B_PLUS.value
ENTRY_ACCEPTANCE_LEVEL_NEAR = EntryAcceptanceLevel.NEAR.value
ENTRY_ACCEPTANCE_LEVEL_DEGRADED = EntryAcceptanceLevel.DEGRADED.value
ENTRY_ACCEPTANCE_LEVEL_INVALID = EntryAcceptanceLevel.INVALID.value
ENTRY_ACCEPTANCE_LEVEL_LOW_CONFIDENCE = EntryAcceptanceLevel.LOW_CONFIDENCE.value

THIN_DATA = "THIN_DATA"
STALE_INPUT = "STALE_INPUT"
MALFORMED_CANDLES = "MALFORMED_CANDLES"
INCOMPLETE_CANDLES = "INCOMPLETE_CANDLES"
MIXED_TIMEFRAME = "MIXED_TIMEFRAME"
MISSING_TIMEFRAME_SCHEMA = "MISSING_TIMEFRAME_SCHEMA"
MIXED_TIMEFRAME_SCHEMA = "MIXED_TIMEFRAME_SCHEMA"
UNAPPROVED_DERIVED_TIMEFRAME = "UNAPPROVED_DERIVED_TIMEFRAME"
MISSING_CANDLES = "MISSING_CANDLES"
MISSING_CANDIDATE = "MISSING_CANDIDATE"
MISSING_CANDIDATE_ID = "MISSING_CANDIDATE_ID"
INVALID_CANDIDATE_SIDE = "INVALID_CANDIDATE_SIDE"
MISSING_REQUIRED_PREDICATES = "MISSING_REQUIRED_PREDICATES"
WRONG_SIDE_DIRECTIONAL_CONTRADICTION = "WRONG_SIDE_DIRECTIONAL_CONTRADICTION"
RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE = "RESEARCH_SOURCE_NOT_RUNTIME_ELIGIBLE"
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

TIMEFRAME_SOURCE_NATIVE = "NATIVE"
TIMEFRAME_SOURCE_DERIVED = "DERIVED"
TIMEFRAME_ALIGNMENT_ALIGNED = "ALIGNED"
TIMEFRAME_ALIGNMENT_DERIVED_ALIGNED = "DERIVED_ALIGNED"
TIMEFRAME_ALIGNMENT_MIXED_BLOCKED = "MIXED_TIMEFRAME_BLOCKED"
TIMEFRAME_ALIGNMENT_MISSING_SCHEMA = "MISSING_TIMEFRAME_SCHEMA"

DIMENSION_WEIGHTS = {
    "structural_similarity": 0.30,
    "directional_alignment": 0.15,
    "timing_session_fit": 0.10,
    "volatility_range_fit": 0.10,
    "pullback_retest_quality": 0.10,
    "participation_support": 0.10,
    "failure_risk_flags": 0.10,
    "data_provenance_confidence": 0.05,
}


@dataclass(frozen=True)
class EntryAcceptanceThresholds:
    min_completed_candles: int = 8
    preferred_completed_candles: int = 16
    stale_after_intervals: float = 3.0
    max_missing_gap_intervals: float = 1.5
    tiny_range_pct: float = 0.00005
    exact_score: float = 0.85
    near_score: float = 0.70
    degraded_score: float = 0.50
    invalid_score: float = 0.50
    contradiction_alignment: float = 0.25
    b_plus_score: float = 0.80


@dataclass(frozen=True)
class EntryAcceptanceLevelPolicy:
    """Generic strategy-declared acceptance level policy.

    This describes the layer level a strategy is willing to consume. It does
    not itself grant route authority; governance and runtime safety remain
    separate gates.
    """

    level: str
    min_score: float
    min_structural_similarity: float
    accepted_classes: tuple[str, ...]
    advisory_level: bool = True
    route_authority: bool = False


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
    def close_location(self) -> float:
        return (self.close - self.low) / self.range

    @property
    def range_pct(self) -> float:
        return self.range / self.close if self.close else 0.0


@dataclass(frozen=True)
class _CandidateValidation:
    candidate: Mapping[str, Any]
    candidate_id: str | None
    family: str | None
    side: str | None
    failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class _CandleValidation:
    candles: tuple[NormalizedCandle, ...]
    candles_received: int
    freshness_status: str
    failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class _TimeframeContext:
    primary_timeframe: str
    candidate_timeframe: str
    context_timeframe: str
    exit_management_timeframe: str | None
    fast_reaction_timeframe: str | None
    timeframe_source: str
    base_timeframe_if_derived: str | None
    aggregation_method: str | None
    anchor_rule: str | None
    timeframe_alignment_status: str
    failure_reasons: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "primary_timeframe": self.primary_timeframe,
            "candidate_timeframe": self.candidate_timeframe,
            "context_timeframe": self.context_timeframe,
            "exit_management_timeframe": self.exit_management_timeframe,
            "fast_reaction_timeframe": self.fast_reaction_timeframe,
            "timeframe_source": self.timeframe_source,
            "base_timeframe_if_derived": self.base_timeframe_if_derived,
            "aggregation_method": self.aggregation_method,
            "anchor_rule": self.anchor_rule,
            "timeframe_alignment_status": self.timeframe_alignment_status,
        }



def entry_acceptance_level_policy(
    level: str | EntryAcceptanceLevel,
    *,
    thresholds: EntryAcceptanceThresholds | None = None,
) -> EntryAcceptanceLevelPolicy:
    """Return the generic policy for a strategy-declared acceptance level.

    Levels are intentionally strategy-consumption labels. They preserve the
    canonical acceptance-class bands while allowing upstream strategy documents
    or overlays to declare whether a lane consumes exact-only, B+, near, or
    degraded candidates.
    """

    resolved = thresholds or EntryAcceptanceThresholds()
    normalized = str(level.value if isinstance(level, EntryAcceptanceLevel) else level or "").strip().upper()
    if normalized == ENTRY_ACCEPTANCE_LEVEL_EXACT:
        return EntryAcceptanceLevelPolicy(
            level=ENTRY_ACCEPTANCE_LEVEL_EXACT,
            min_score=resolved.exact_score,
            min_structural_similarity=0.92,
            accepted_classes=(EXACT_STRUCTURAL_MATCH,),
        )
    if normalized in {ENTRY_ACCEPTANCE_LEVEL_B_PLUS, "B_PLUS_SETUP_SCORE", "B+"}:
        return EntryAcceptanceLevelPolicy(
            level=ENTRY_ACCEPTANCE_LEVEL_B_PLUS,
            min_score=resolved.b_plus_score,
            min_structural_similarity=0.72,
            accepted_classes=(NEAR_STRUCTURAL_MATCH,),
        )
    if normalized == ENTRY_ACCEPTANCE_LEVEL_NEAR:
        return EntryAcceptanceLevelPolicy(
            level=ENTRY_ACCEPTANCE_LEVEL_NEAR,
            min_score=resolved.near_score,
            min_structural_similarity=0.72,
            accepted_classes=(NEAR_STRUCTURAL_MATCH,),
        )
    if normalized == ENTRY_ACCEPTANCE_LEVEL_DEGRADED:
        return EntryAcceptanceLevelPolicy(
            level=ENTRY_ACCEPTANCE_LEVEL_DEGRADED,
            min_score=resolved.degraded_score,
            min_structural_similarity=0.50,
            accepted_classes=(DEGRADED_BUT_VALID_MATCH,),
        )
    if normalized == ENTRY_ACCEPTANCE_LEVEL_LOW_CONFIDENCE:
        return EntryAcceptanceLevelPolicy(
            level=ENTRY_ACCEPTANCE_LEVEL_LOW_CONFIDENCE,
            min_score=0.0,
            min_structural_similarity=0.0,
            accepted_classes=(LOW_CONFIDENCE_INSUFFICIENT_DATA,),
        )
    return EntryAcceptanceLevelPolicy(
        level=ENTRY_ACCEPTANCE_LEVEL_INVALID,
        min_score=0.0,
        min_structural_similarity=0.0,
        accepted_classes=(STRUCTURALLY_INVALID,),
    )


def entry_acceptance_level_from_report(report: Mapping[str, Any]) -> str:
    """Map an acceptance-layer report to the generic level vocabulary."""

    acceptance_class = str(report.get("acceptance_class") or "")
    score = _optional_float(report.get("acceptance_score")) or 0.0
    dimensions = report.get("dimension_scores") if isinstance(report.get("dimension_scores"), Mapping) else {}
    structural = _optional_float(dimensions.get("structural_similarity")) if isinstance(dimensions, Mapping) else None
    thresholds = EntryAcceptanceThresholds()
    if acceptance_class == EXACT_STRUCTURAL_MATCH and score >= thresholds.exact_score:
        return ENTRY_ACCEPTANCE_LEVEL_EXACT
    if acceptance_class == NEAR_STRUCTURAL_MATCH and score >= thresholds.b_plus_score and (structural is None or structural >= 0.72):
        return ENTRY_ACCEPTANCE_LEVEL_B_PLUS
    if acceptance_class == NEAR_STRUCTURAL_MATCH and score >= thresholds.near_score:
        return ENTRY_ACCEPTANCE_LEVEL_NEAR
    if acceptance_class == DEGRADED_BUT_VALID_MATCH and score >= thresholds.degraded_score:
        return ENTRY_ACCEPTANCE_LEVEL_DEGRADED
    if acceptance_class == LOW_CONFIDENCE_INSUFFICIENT_DATA:
        return ENTRY_ACCEPTANCE_LEVEL_LOW_CONFIDENCE
    return ENTRY_ACCEPTANCE_LEVEL_INVALID


def entry_acceptance_report_satisfies_level(report: Mapping[str, Any], level: str | EntryAcceptanceLevel) -> bool:
    """Return whether an advisory report satisfies a strategy-declared level."""

    policy = entry_acceptance_level_policy(level)
    acceptance_class = str(report.get("acceptance_class") or "")
    score = _optional_float(report.get("acceptance_score")) or 0.0
    dimensions = report.get("dimension_scores") if isinstance(report.get("dimension_scores"), Mapping) else {}
    structural = _optional_float(dimensions.get("structural_similarity")) if isinstance(dimensions, Mapping) else 0.0
    return (
        acceptance_class in policy.accepted_classes
        and score >= policy.min_score
        and (structural or 0.0) >= policy.min_structural_similarity
    )

def build_entry_acceptance_state(
    payload: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
    candle_timeframe: str | None = None,
    input_mode: str | None = None,
    input_source_path: str | Path | None = None,
    input_source_category: str | None = None,
    thresholds: EntryAcceptanceThresholds | None = None,
) -> dict[str, Any]:
    """Build a deterministic, artifact-safe entry acceptance report."""

    resolved_now = _coerce_now(now)
    resolved_thresholds = thresholds or EntryAcceptanceThresholds()
    timeframe_context = _timeframe_context(payload, candle_timeframe=candle_timeframe)
    timeframe = timeframe_context.primary_timeframe
    input_context = track_b_input_context(
        payload,
        input_mode=input_mode,
        input_source_path=input_source_path,
        input_source_category=input_source_category,
    )
    instrument = _normalize_instrument(payload)
    candidate_validation = _validate_candidate(payload)
    candle_validation = _validate_completed_5m_candles(
        _extract_candle_rows(payload),
        now=resolved_now,
        expected_timeframe=timeframe,
        thresholds=resolved_thresholds,
    )
    provenance_failures = tuple(_provenance_failure_reasons(input_context))
    hard_data_failures = _dedupe(
        [
            *provenance_failures,
            *timeframe_context.failure_reasons,
            *candle_validation.failure_reasons,
            *(
                reason
                for reason in candidate_validation.failure_reasons
                if reason in {MISSING_CANDIDATE, MISSING_CANDIDATE_ID, INVALID_CANDIDATE_SIDE}
            ),
        ]
    )
    warnings = _dedupe([*candidate_validation.warnings, *timeframe_context.warnings, *candle_validation.warnings])

    if hard_data_failures:
        return _low_confidence_report(
            payload=payload,
            generated_at=resolved_now,
            input_context=input_context,
            instrument=instrument,
            timeframe=timeframe,
            timeframe_context=timeframe_context,
            candidate_validation=candidate_validation,
            candle_validation=candle_validation,
            failure_reasons=hard_data_failures,
            warnings=warnings,
        )

    assert candidate_validation.side is not None
    assert candidate_validation.candidate_id is not None
    assert candle_validation.candles

    features = _calculate_market_features(candle_validation.candles, side=candidate_validation.side)
    dimensions = _dimension_scores(
        candidate_validation.candidate,
        features=features,
        input_context=input_context,
        side=candidate_validation.side,
        participation_quality=_extract_participation_quality(payload),
    )
    structural_failures = _structural_failure_reasons(
        candidate_validation.candidate,
        dimensions=dimensions,
        thresholds=resolved_thresholds,
    )
    failure_reasons = _dedupe([*candidate_validation.failure_reasons, *structural_failures])
    acceptance_score = _weighted_score(dimensions)
    acceptance_class = _classify_acceptance(
        acceptance_score=acceptance_score,
        failure_reasons=failure_reasons,
        dimensions=dimensions,
        thresholds=resolved_thresholds,
    )
    confidence = _confidence(
        acceptance_class=acceptance_class,
        acceptance_score=acceptance_score,
        completed_count=len(candle_validation.candles),
        dimensions=dimensions,
        thresholds=resolved_thresholds,
    )
    supporting_reasons = _supporting_reasons(
        acceptance_class=acceptance_class,
        dimensions=dimensions,
        features=features,
        participation_quality=_extract_participation_quality(payload),
    )
    failure_reasons = _dedupe([*failure_reasons, *_context_failure_reasons(dimensions)])

    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "QUALITY_CONTEXT_ONLY",
        "artifact_path": DEFAULT_ENTRY_ACCEPTANCE_ARTIFACT_PATH,
        **input_context,
        "source_id": _source_id(payload),
        "source_provenance_status": source_provenance_status(input_context),
        "instrument": instrument,
        "generated_at": resolved_now.isoformat(),
        "candle_timeframe": timeframe,
        **timeframe_context.to_payload(),
        "candles_received": candle_validation.candles_received,
        "completed_candles_used": len(candle_validation.candles),
        "latest_candle_timestamp": candle_validation.candles[-1].timestamp.isoformat(),
        "latest_input_timestamp": candle_validation.candles[-1].timestamp.isoformat(),
        "freshness_status": candle_validation.freshness_status,
        "candidate_id": candidate_validation.candidate_id,
        "candidate_family": candidate_validation.family,
        "candidate_side": candidate_validation.side,
        "acceptance_class": acceptance_class,
        "acceptance_score": _round(acceptance_score),
        "entry_quality_context": _entry_quality_context(
            acceptance_class=acceptance_class,
            dimensions=dimensions,
            features=features,
        ),
        "suggested_exit_profile_context": _exit_profile_context(
            acceptance_class=acceptance_class,
            dimensions=dimensions,
        ),
        "initial_size_context": _initial_size_context(
            acceptance_class=acceptance_class,
            confidence=confidence,
            dimensions=dimensions,
        ),
        "confidence": _round(confidence),
        "confidence_state": _confidence_state(confidence),
        "supporting_reasons": supporting_reasons,
        "failure_reasons": failure_reasons,
        "warnings": warnings,
        "dimension_scores": _round_mapping(dimensions),
        "feature_summary": _round_mapping(features),
        "safety_flags": _safety_flags(),
        **_safety_flags(),
    }


def build_asia_early_normal_breakout_retest_hold_entry_acceptance_payload(
    *,
    event_payload: Mapping[str, Any],
    completed_candles: Sequence[Mapping[str, Any]],
    participation_quality: Mapping[str, Any] | None = None,
    input_source_path: str | Path | None = None,
    input_source_category: str = SOURCE_CATEGORY_RUNTIME,
    input_mode: str = INPUT_MODE_RUNTIME_DECISION,
) -> dict[str, Any]:
    """Map the existing breakout/retest/hold envelope into advisory scorer input.

    The adapter is deliberately offline and behavior-neutral: it does not call
    the strategy runner, emit signals, or create any order/lifecycle authority.
    """

    metadata = event_payload.get("metadata") if isinstance(event_payload.get("metadata"), Mapping) else {}
    state = metadata.get(ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_STATE_KEY)
    features = metadata.get(ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURES_KEY)
    state_payload = state if isinstance(state, Mapping) else {}
    feature_payload = features if isinstance(features, Mapping) else {}
    timeframe = _normalize_timeframe(
        _string_or_none(state_payload.get("timeframe"))
        or _string_or_none(event_payload.get("timeframe"))
        or DEFAULT_CANDLE_TIMEFRAME
    )
    candidate = build_asia_early_normal_breakout_retest_hold_candidate_metadata(event_payload=event_payload)
    payload: dict[str, Any] = {
        "instrument": _string_or_none(event_payload.get("instrument_family")) or "MGC",
        "source_id": _string_or_none(event_payload.get("source_id")),
        "input_source_path": (
            str(input_source_path)
            if input_source_path is not None
            else _string_or_none(metadata.get("source_payload_path"))
        ),
        "input_source_category": input_source_category,
        "input_mode": input_mode,
        "candidate": candidate,
        "candles": list(completed_candles),
        "timeframe_context": {
            "primary_timeframe": timeframe,
            "candidate_timeframe": timeframe,
            "context_timeframe": timeframe,
            "exit_management_timeframe": timeframe,
            "fast_reaction_timeframe": timeframe,
            "timeframe_source": TIMEFRAME_SOURCE_NATIVE,
            "base_timeframe_if_derived": None,
            "aggregation_method": None,
            "anchor_rule": None,
            "timeframe_alignment_status": TIMEFRAME_ALIGNMENT_ALIGNED,
        },
        "runtime_provenance": {
            "source_id": _string_or_none(event_payload.get("source_id")),
            "source_payload_path": _string_or_none(metadata.get("source_payload_path")),
            "source_bar_count": metadata.get("source_bar_count"),
            "event_generated_at": _string_or_none(event_payload.get("generated_at") or event_payload.get("observed_at")),
        },
    }
    if participation_quality is not None:
        payload["participation_quality"] = dict(participation_quality)
    return payload


def build_asia_early_normal_breakout_retest_hold_candidate_metadata(
    *,
    event_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Build scorer candidate metadata from a session-strategy envelope."""

    metadata = event_payload.get("metadata") if isinstance(event_payload.get("metadata"), Mapping) else {}
    state = metadata.get(ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_STATE_KEY)
    features = metadata.get(ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURES_KEY)
    state_payload = state if isinstance(state, Mapping) else {}
    feature_payload = features if isinstance(features, Mapping) else {}
    matched_required, missing_required = _predicate_partition(
        feature_payload,
        ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_REQUIRED_PREDICATES,
    )
    matched_context, missing_context = _predicate_partition(
        {**state_payload, **feature_payload},
        ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_CONTEXT_PREDICATES,
    )
    near_miss_predicates = _near_miss_predicates_for_breakout_retest_hold(
        state=state_payload,
        features=feature_payload,
        missing_context=missing_context,
    )
    failure_risk_flags = _breakout_retest_hold_failure_flags(
        state=state_payload,
        features=feature_payload,
        missing_required=missing_required,
        missing_context=missing_context,
    )
    return {
        "candidate_id": _string_or_none(event_payload.get("lane_id")) or "mgc_asia_early_normal_breakout_retest_hold_long",
        "candidate_family": ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FAMILY,
        "side": CandidateSide.LONG.value,
        "rule_id": _string_or_none(event_payload.get("strategy_id"))
        or ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_RULE_ID,
        "source_event_strategy_id": _string_or_none(event_payload.get("strategy_id")),
        "session_context": {
            "derivative_phase": _string_or_none(state_payload.get("derivative_phase")),
            "session_asia": state_payload.get("session_asia") is True,
            "timing_session_fit": _breakout_retest_hold_timing_score(state_payload),
        },
        "breakout_retest_hold_context": {
            "breakout_breaks_prior_1_high": feature_payload.get("breakout_breaks_prior_1_high") is True,
            "signal_retests_and_holds_breakout_level": (
                feature_payload.get("signal_retests_and_holds_breakout_level") is True
            ),
            "breakout_level": _optional_float(feature_payload.get("breakout_level")),
            "breakout_range_expansion_ratio": _optional_float(feature_payload.get("breakout_range_expansion_ratio")),
            "retest_depth_ticks_or_points": _optional_float(feature_payload.get("retest_depth_ticks_or_points")),
            "retest_depth_normalized": _optional_float(feature_payload.get("retest_depth_normalized")),
            "hold_margin_ticks_or_points": _breakout_retest_hold_margin(event_payload=event_payload, features=feature_payload),
            "hold_margin_normalized": _optional_float(feature_payload.get("hold_margin_normalized")),
            "bars_since_breakout": _optional_float(feature_payload.get("bars_since_breakout")),
            "bars_since_retest": _optional_float(feature_payload.get("bars_since_retest")),
            "range_expansion_ratio": _optional_float(feature_payload.get("range_expansion_ratio")),
            "close_location": _optional_float(feature_payload.get("close_location")),
            "body_to_range_ratio": _optional_float(feature_payload.get("body_to_range_ratio")),
            "churn_score": _optional_float(feature_payload.get("churn_score")),
            "prior_bars_since_long_setup": _optional_float(feature_payload.get("prior_bars_since_long_setup")),
            "anti_churn_bars": _optional_float(feature_payload.get("anti_churn_bars")),
            "anti_churn_margin_bars": _optional_float(feature_payload.get("anti_churn_margin_bars")),
            "snap_turn_conflict_strength": _optional_float(feature_payload.get("snap_turn_conflict_strength")),
        },
        "required_predicates": list(ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_REQUIRED_PREDICATES),
        "matched_predicates": [*matched_required, *matched_context],
        "missing_predicates": missing_required,
        "context_missing_predicates": missing_context,
        "missing_required_predicates": missing_required,
        "near_miss_predicates": near_miss_predicates,
        "structural_similarity": _breakout_retest_hold_structural_score(
            matched_required=matched_required,
            missing_required=missing_required,
            near_miss_predicates=near_miss_predicates,
        ),
        "directional_alignment": _breakout_retest_hold_directional_score(event_payload),
        "timing_session_fit": _breakout_retest_hold_timing_score(state_payload),
        "volatility_range_fit": _breakout_retest_hold_range_score(feature_payload),
        "pullback_retest_quality": _breakout_retest_hold_retest_score(
            event_payload=event_payload,
            features=feature_payload,
        ),
        "failure_risk_score": _breakout_retest_hold_failure_risk_score(failure_risk_flags),
        "failure_risk_flags": failure_risk_flags,
        "candidate_timeframe": _normalize_timeframe(
            _string_or_none(state_payload.get("timeframe"))
            or _string_or_none(event_payload.get("timeframe"))
            or DEFAULT_CANDLE_TIMEFRAME
        ),
        "feature_version": _string_or_none(feature_payload.get("feature_version")),
        "calibration_profile": _string_or_none(feature_payload.get("calibration_profile")),
    }


def write_entry_acceptance_state(
    payload: Mapping[str, Any],
    *,
    output_path: str | Path,
    now: datetime | str | None = None,
    candle_timeframe: str | None = None,
    input_mode: str | None = None,
    input_source_path: str | Path | None = None,
    input_source_category: str | None = None,
    thresholds: EntryAcceptanceThresholds | None = None,
) -> dict[str, Any]:
    """Write a report to an explicit tmp/test path only."""

    resolved_path = Path(output_path).resolve()
    if not _is_tmp_or_test_path(resolved_path):
        raise ValueError("entry acceptance writer is limited to tmp/test output paths in implementation slice 1.")
    report = build_entry_acceptance_state(
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


def _validate_candidate(payload: Mapping[str, Any]) -> _CandidateValidation:
    candidate = _extract_candidate(payload)
    if not isinstance(candidate, Mapping):
        return _CandidateValidation(
            candidate={},
            candidate_id=None,
            family=None,
            side=None,
            failure_reasons=(MISSING_CANDIDATE,),
            warnings=("candidate metadata must be supplied under candidate or candidate_pattern_metadata.",),
        )

    candidate_id = _string_or_none(candidate.get("candidate_id") or candidate.get("id"))
    family = _string_or_none(candidate.get("candidate_family") or candidate.get("family") or candidate.get("strategy_family"))
    raw_side = _string_or_none(candidate.get("candidate_side") or candidate.get("side") or candidate.get("direction"))
    side = str(raw_side or "").strip().upper()
    failures: list[str] = []
    if not candidate_id:
        failures.append(MISSING_CANDIDATE_ID)
    if side not in {CandidateSide.LONG.value, CandidateSide.SHORT.value}:
        failures.append(INVALID_CANDIDATE_SIDE)
        side_value = None
    else:
        side_value = side
    return _CandidateValidation(
        candidate=candidate,
        candidate_id=candidate_id,
        family=family,
        side=side_value,
        failure_reasons=tuple(failures),
        warnings=(),
    )


def _validate_completed_5m_candles(
    raw_rows: Any,
    *,
    now: datetime,
    expected_timeframe: str,
    thresholds: EntryAcceptanceThresholds,
) -> _CandleValidation:
    if not _is_sequence(raw_rows):
        return _CandleValidation(
            candles=(),
            candles_received=0,
            freshness_status="NO_COMPLETED_CANDLES",
            failure_reasons=(MISSING_CANDLES, THIN_DATA, MALFORMED_CANDLES),
            warnings=("candle input must be a sequence under candles, ohlcv_candles, bars, or completed_5m_market_context.candles.",),
        )

    candles: list[NormalizedCandle] = []
    failures: list[str] = []
    warnings: list[str] = []
    incomplete_count = 0
    mixed_timeframe_count = 0

    for row in raw_rows:
        if not isinstance(row, Mapping):
            failures.append(MALFORMED_CANDLES)
            continue
        if row.get("completed") is False or row.get("is_complete") is False:
            incomplete_count += 1
            continue
        timeframe = _normalize_timeframe(str(row.get("timeframe") or row.get("candle_timeframe") or expected_timeframe))
        if timeframe != expected_timeframe:
            mixed_timeframe_count += 1
            continue
        try:
            candle = _normalize_candle(row, timeframe=timeframe)
        except (TypeError, ValueError):
            failures.append(MALFORMED_CANDLES)
            continue
        if candle.range <= 0 or candle.high < max(candle.open, candle.close) or candle.low > min(candle.open, candle.close):
            failures.append(MALFORMED_CANDLES)
            continue
        candles.append(candle)

    candles.sort(key=lambda candle: candle.timestamp)
    if incomplete_count:
        warnings.append(f"excluded {incomplete_count} incomplete candle(s).")
    if mixed_timeframe_count:
        failures.append(MIXED_TIMEFRAME)
    if len(candles) < thresholds.min_completed_candles:
        failures.append(THIN_DATA)
    if candles:
        ranges = [candle.range_pct for candle in candles[-thresholds.min_completed_candles :]]
        if ranges and sum(ranges) / len(ranges) < thresholds.tiny_range_pct:
            failures.append(THIN_DATA)
        freshness_status, freshness_failures = _freshness_status(candles, now=now, thresholds=thresholds)
        failures.extend(freshness_failures)
    else:
        freshness_status = "NO_COMPLETED_CANDLES"
        failures.append(MISSING_CANDLES)

    if incomplete_count and len(candles) < thresholds.min_completed_candles:
        failures.append(INCOMPLETE_CANDLES)

    return _CandleValidation(
        candles=tuple(candles),
        candles_received=len(raw_rows),
        freshness_status=freshness_status,
        failure_reasons=tuple(_dedupe(failures)),
        warnings=tuple(_dedupe(warnings)),
    )


def _timeframe_context(payload: Mapping[str, Any], *, candle_timeframe: str | None) -> _TimeframeContext:
    context = payload.get("timeframe_context")
    timeframe_context = context if isinstance(context, Mapping) else {}
    primary = _normalize_timeframe(
        candle_timeframe
        or _string_or_none(timeframe_context.get("primary_timeframe"))
        or _string_or_none(payload.get("primary_timeframe"))
        or _string_or_none(payload.get("candle_timeframe"))
        or _string_or_none(payload.get("timeframe"))
        or DEFAULT_CANDLE_TIMEFRAME
    )
    candidate = _normalize_timeframe(
        _string_or_none(timeframe_context.get("candidate_timeframe"))
        or _string_or_none(payload.get("candidate_timeframe"))
        or _candidate_timeframe(payload)
        or primary
    )
    context_timeframe = _normalize_timeframe(
        _string_or_none(timeframe_context.get("context_timeframe"))
        or _string_or_none(payload.get("context_timeframe"))
        or _market_context_timeframe(payload)
        or primary
    )
    exit_management = _optional_timeframe(
        _string_or_none(timeframe_context.get("exit_management_timeframe"))
        or _string_or_none(payload.get("exit_management_timeframe"))
    )
    fast_reaction = _optional_timeframe(
        _string_or_none(timeframe_context.get("fast_reaction_timeframe"))
        or _string_or_none(payload.get("fast_reaction_timeframe"))
    )
    source = str(
        _string_or_none(timeframe_context.get("timeframe_source"))
        or _string_or_none(payload.get("timeframe_source"))
        or TIMEFRAME_SOURCE_NATIVE
    ).strip().upper()
    if source not in {TIMEFRAME_SOURCE_NATIVE, TIMEFRAME_SOURCE_DERIVED}:
        source = TIMEFRAME_SOURCE_NATIVE
    base_if_derived = _optional_timeframe(
        _string_or_none(timeframe_context.get("base_timeframe_if_derived"))
        or _string_or_none(payload.get("base_timeframe_if_derived"))
    )
    aggregation_method = _string_or_none(
        timeframe_context.get("aggregation_method") or payload.get("aggregation_method")
    )
    anchor_rule = _string_or_none(timeframe_context.get("anchor_rule") or payload.get("anchor_rule"))
    declared_status = _string_or_none(
        timeframe_context.get("timeframe_alignment_status") or payload.get("timeframe_alignment_status")
    )
    failures: list[str] = []
    warnings: list[str] = []

    if candidate != primary or context_timeframe != primary:
        failures.append(MIXED_TIMEFRAME_SCHEMA)
    if source == TIMEFRAME_SOURCE_DERIVED:
        if not base_if_derived or not aggregation_method or not anchor_rule:
            failures.append(MISSING_TIMEFRAME_SCHEMA)
        if not _approved_derived_timeframe(timeframe_context, payload):
            failures.append(UNAPPROVED_DERIVED_TIMEFRAME)
        elif not failures and declared_status is None:
            declared_status = TIMEFRAME_ALIGNMENT_DERIVED_ALIGNED
    elif base_if_derived or aggregation_method or anchor_rule:
        warnings.append("native timeframe context supplied derivation fields; derivation metadata is advisory only.")

    if failures:
        status = TIMEFRAME_ALIGNMENT_MIXED_BLOCKED
    elif declared_status is not None:
        status = declared_status.strip().upper()
    else:
        status = TIMEFRAME_ALIGNMENT_ALIGNED
    if status in {TIMEFRAME_ALIGNMENT_MIXED_BLOCKED, TIMEFRAME_ALIGNMENT_MISSING_SCHEMA}:
        failures.append(MIXED_TIMEFRAME_SCHEMA if status == TIMEFRAME_ALIGNMENT_MIXED_BLOCKED else MISSING_TIMEFRAME_SCHEMA)

    return _TimeframeContext(
        primary_timeframe=primary,
        candidate_timeframe=candidate,
        context_timeframe=context_timeframe,
        exit_management_timeframe=exit_management,
        fast_reaction_timeframe=fast_reaction,
        timeframe_source=source,
        base_timeframe_if_derived=base_if_derived,
        aggregation_method=aggregation_method,
        anchor_rule=anchor_rule,
        timeframe_alignment_status=status,
        failure_reasons=tuple(_dedupe(failures)),
        warnings=tuple(_dedupe(warnings)),
    )


def _dimension_scores(
    candidate: Mapping[str, Any],
    *,
    features: Mapping[str, float],
    input_context: Mapping[str, Any],
    side: str,
    participation_quality: Mapping[str, Any] | None,
) -> dict[str, float]:
    structural = _structural_similarity(candidate)
    directional = _numeric_score(
        candidate.get("directional_alignment"),
        default=float(features["directional_alignment"]),
    )
    timing = _numeric_score(
        candidate.get("timing_session_fit")
        or candidate.get("session_fit")
        or _nested_score(candidate, "session_context", "timing_session_fit"),
        default=1.0,
    )
    volatility = _numeric_score(
        candidate.get("volatility_range_fit") or candidate.get("range_fit"),
        default=float(features["volatility_range_fit"]),
    )
    pullback = _numeric_score(
        candidate.get("pullback_retest_quality") or candidate.get("retest_quality"),
        default=0.70,
    )
    participation = _participation_support(side=side, participation_quality=participation_quality)
    failure_risk = _failure_risk_score(candidate)
    data_confidence = _data_provenance_confidence(input_context)
    return {
        "structural_similarity": structural,
        "directional_alignment": directional,
        "timing_session_fit": timing,
        "volatility_range_fit": volatility,
        "pullback_retest_quality": pullback,
        "participation_support": participation,
        "failure_risk_flags": failure_risk,
        "data_provenance_confidence": data_confidence,
    }


def _structural_similarity(candidate: Mapping[str, Any]) -> float:
    explicit = _optional_float(candidate.get("structural_similarity") or candidate.get("structural_score"))
    if explicit is not None:
        return _clamp01(explicit)

    required = _strings(candidate.get("required_predicates"))
    matched = set(_strings(candidate.get("matched_predicates") or candidate.get("satisfied_predicates")))
    missing = set(_strings(candidate.get("missing_predicates") or candidate.get("failed_predicates")))
    optional = _strings(candidate.get("optional_predicates"))
    near_misses = _strings(candidate.get("near_miss_predicates"))
    tolerance_distance = _optional_float(candidate.get("tolerance_distance"))

    if required:
        required_score = sum(1 for item in required if item in matched and item not in missing) / len(required)
    elif missing:
        required_score = 0.35
    else:
        required_score = 0.75

    optional_score = 1.0
    if optional:
        optional_score = sum(1 for item in optional if item in matched and item not in missing) / len(optional)

    near_miss_penalty = min(0.12, 0.04 * len(near_misses))
    tolerance_penalty = 0.0 if tolerance_distance is None else min(0.20, max(0.0, tolerance_distance) * 0.20)
    return _clamp01((required_score * 0.80) + (optional_score * 0.20) - near_miss_penalty - tolerance_penalty)


def _failure_risk_score(candidate: Mapping[str, Any]) -> float:
    explicit = _optional_float(candidate.get("failure_risk_score"))
    if explicit is not None:
        return _clamp01(explicit)
    flags = _strings(candidate.get("failure_risk_flags") or candidate.get("risk_flags"))
    severe_flags = {
        "WRONG_SIDE_DIRECTIONAL_CONTRADICTION",
        "FAILED_RETEST",
        "LATE_CHASE",
        "OVEREXTENDED",
        "TRAP_RISK",
    }
    penalty = 0.10 * len(flags) + 0.10 * sum(1 for flag in flags if flag.upper() in severe_flags)
    return _clamp01(1.0 - min(0.70, penalty))


def _participation_support(side: str, participation_quality: Mapping[str, Any] | None) -> float:
    if not participation_quality:
        return 0.65
    hold_key = "long_hold_quality" if side == CandidateSide.LONG.value else "short_hold_quality"
    opposite_hold_key = "short_hold_quality" if side == CandidateSide.LONG.value else "long_hold_quality"
    hold_quality = str(participation_quality.get(hold_key) or "").upper()
    opposite_quality = str(participation_quality.get(opposite_hold_key) or "").upper()
    confidence = _optional_float(participation_quality.get("confidence"))
    continuation = _optional_float(participation_quality.get("continuation_confidence"))

    score = 0.65
    if hold_quality == "SUPPORTIVE":
        score = 0.88
    elif hold_quality == "DEGRADING":
        score = 0.55
    elif hold_quality == "HOSTILE":
        score = 0.22
    elif opposite_quality == "SUPPORTIVE":
        score = 0.35
    if continuation is not None:
        score = (score * 0.75) + (_clamp01(continuation) * 0.25)
    if confidence is not None:
        score *= 0.70 + (_clamp01(confidence) * 0.30)
    return _clamp01(score)


def _structural_failure_reasons(
    candidate: Mapping[str, Any],
    *,
    dimensions: Mapping[str, float],
    thresholds: EntryAcceptanceThresholds,
) -> tuple[str, ...]:
    failures: list[str] = []
    missing_required = _strings(candidate.get("missing_required_predicates") or candidate.get("missing_predicates"))
    if missing_required or dimensions["structural_similarity"] < thresholds.invalid_score:
        failures.append(MISSING_REQUIRED_PREDICATES)
    if (
        dimensions["directional_alignment"] <= thresholds.contradiction_alignment
        or "WRONG_SIDE_DIRECTIONAL_CONTRADICTION" in {flag.upper() for flag in _strings(candidate.get("failure_risk_flags"))}
    ):
        failures.append(WRONG_SIDE_DIRECTIONAL_CONTRADICTION)
    return tuple(_dedupe(failures))


def _classify_acceptance(
    *,
    acceptance_score: float,
    failure_reasons: Sequence[str],
    dimensions: Mapping[str, float],
    thresholds: EntryAcceptanceThresholds,
) -> str:
    if MISSING_REQUIRED_PREDICATES in failure_reasons or WRONG_SIDE_DIRECTIONAL_CONTRADICTION in failure_reasons:
        return STRUCTURALLY_INVALID
    if dimensions["structural_similarity"] < thresholds.invalid_score:
        return STRUCTURALLY_INVALID
    if acceptance_score >= thresholds.exact_score and dimensions["structural_similarity"] >= 0.92:
        return EXACT_STRUCTURAL_MATCH
    if acceptance_score >= thresholds.near_score and dimensions["structural_similarity"] >= 0.72:
        return NEAR_STRUCTURAL_MATCH
    if acceptance_score >= thresholds.degraded_score and dimensions["structural_similarity"] >= 0.50:
        return DEGRADED_BUT_VALID_MATCH
    return STRUCTURALLY_INVALID


def _low_confidence_report(
    *,
    payload: Mapping[str, Any],
    generated_at: datetime,
    input_context: Mapping[str, Any],
    instrument: str,
    timeframe: str,
    timeframe_context: _TimeframeContext,
    candidate_validation: _CandidateValidation,
    candle_validation: _CandleValidation,
    failure_reasons: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    latest = candle_validation.candles[-1].timestamp if candle_validation.candles else None
    dimensions = _low_confidence_dimensions()
    return {
        "schema_version": SCHEMA_VERSION,
        "producer": PRODUCER_NAME,
        "authority_mode": "QUALITY_CONTEXT_ONLY",
        "artifact_path": DEFAULT_ENTRY_ACCEPTANCE_ARTIFACT_PATH,
        **input_context,
        "source_id": _source_id(payload),
        "source_provenance_status": source_provenance_status(input_context),
        "instrument": instrument,
        "generated_at": generated_at.isoformat(),
        "candle_timeframe": timeframe,
        **timeframe_context.to_payload(),
        "candles_received": candle_validation.candles_received,
        "completed_candles_used": len(candle_validation.candles),
        "latest_candle_timestamp": latest.isoformat() if latest is not None else None,
        "latest_input_timestamp": latest.isoformat() if latest is not None else None,
        "freshness_status": candle_validation.freshness_status,
        "candidate_id": candidate_validation.candidate_id,
        "candidate_family": candidate_validation.family,
        "candidate_side": candidate_validation.side,
        "acceptance_class": LOW_CONFIDENCE_INSUFFICIENT_DATA,
        "acceptance_score": 0.0,
        "entry_quality_context": {
            "quality_label": "LOW_CONFIDENCE",
            "structural_summary": "entry structure was not scored because required data or provenance was insufficient.",
            "directional_summary": "directional context unavailable or blocked by input validation.",
            "session_summary": "session context unavailable or blocked by input validation.",
            "volatility_summary": "range context unavailable or blocked by input validation.",
            "pullback_retest_summary": "pullback/retest context unavailable or blocked by input validation.",
            "participation_summary": "participation support is advisory and unavailable or blocked.",
            "risk_summary": "fails closed as advisory low-confidence context.",
        },
        "suggested_exit_profile_context": {
            "profile_hint": "NO_EXIT_PROFILE_RECOMMENDED",
            "reason": "entry acceptance is low confidence.",
            "advisory_only": True,
            "authority_boundary": "exit modules and lifecycle owners remain authoritative.",
        },
        "initial_size_context": {
            "size_quality_label": "NO_SIZE_CONTEXT",
            "size_multiplier_hint": 0.0,
            "reason": "entry acceptance is low confidence.",
            "advisory_only": True,
            "authority_boundary": "governance and exposure gates remain authoritative.",
        },
        "confidence": 0.0,
        "confidence_state": LOW_CONFIDENCE_INSUFFICIENT_DATA,
        "supporting_reasons": [],
        "failure_reasons": list(_dedupe(failure_reasons)),
        "warnings": list(warnings),
        "dimension_scores": _round_mapping(dimensions),
        "feature_summary": _empty_feature_summary(),
        "safety_flags": _safety_flags(),
        **_safety_flags(),
    }


def _entry_quality_context(
    *,
    acceptance_class: str,
    dimensions: Mapping[str, float],
    features: Mapping[str, float],
) -> dict[str, Any]:
    label = {
        EXACT_STRUCTURAL_MATCH: "STRONG",
        NEAR_STRUCTURAL_MATCH: "ACCEPTABLE",
        DEGRADED_BUT_VALID_MATCH: "MARGINAL",
        STRUCTURALLY_INVALID: "INVALID",
        LOW_CONFIDENCE_INSUFFICIENT_DATA: "LOW_CONFIDENCE",
    }[acceptance_class]
    return {
        "quality_label": label,
        "structural_summary": _dimension_summary("structural similarity", dimensions["structural_similarity"]),
        "directional_summary": _dimension_summary("directional alignment", dimensions["directional_alignment"]),
        "session_summary": _dimension_summary("timing/session fit", dimensions["timing_session_fit"]),
        "volatility_summary": _dimension_summary("volatility/range fit", dimensions["volatility_range_fit"]),
        "pullback_retest_summary": _dimension_summary("pullback/retest quality", dimensions["pullback_retest_quality"]),
        "participation_summary": _dimension_summary("participation support", dimensions["participation_support"]),
        "risk_summary": _dimension_summary("failure-risk posture", dimensions["failure_risk_flags"]),
        "recent_directional_pressure": _round(features["recent_directional_pressure"]),
    }


def _exit_profile_context(*, acceptance_class: str, dimensions: Mapping[str, float]) -> dict[str, Any]:
    if acceptance_class == EXACT_STRUCTURAL_MATCH:
        hint = "STANDARD"
        reason = "structure and context are strong enough for normal advisory exit context."
    elif acceptance_class == NEAR_STRUCTURAL_MATCH:
        hint = "TIGHTER_INVALIDATION"
        reason = "near-match structure should be paired with more conservative advisory invalidation."
    elif acceptance_class == DEGRADED_BUT_VALID_MATCH:
        hint = "TIMEBOX_ONLY"
        reason = "degraded context favors conservative advisory timeboxing."
    else:
        hint = "NO_EXIT_PROFILE_RECOMMENDED"
        reason = "entry acceptance is invalid or low confidence."
    if dimensions["failure_risk_flags"] < 0.60 and hint == "STANDARD":
        hint = "TIGHTER_INVALIDATION"
        reason = "failure-risk flags reduce the advisory exit context."
    return {
        "profile_hint": hint,
        "reason": reason,
        "advisory_only": True,
        "authority_boundary": "exit modules and lifecycle owners remain authoritative.",
    }


def _initial_size_context(
    *,
    acceptance_class: str,
    confidence: float,
    dimensions: Mapping[str, float],
) -> dict[str, Any]:
    score = min(confidence, dimensions["failure_risk_flags"], dimensions["data_provenance_confidence"])
    if acceptance_class == EXACT_STRUCTURAL_MATCH and score >= 0.80:
        label = "NORMAL_CONTEXT"
        multiplier = 1.0
        reason = "entry quality context is strong; governance and exposure remain authoritative."
    elif acceptance_class in {NEAR_STRUCTURAL_MATCH, DEGRADED_BUT_VALID_MATCH} and score >= 0.55:
        label = "REDUCED_CONTEXT"
        multiplier = 0.5
        reason = "entry quality context is usable but not strong."
    elif acceptance_class in {NEAR_STRUCTURAL_MATCH, DEGRADED_BUT_VALID_MATCH}:
        label = "MINIMUM_CONTEXT"
        multiplier = 0.25
        reason = "entry quality context is marginal or confidence-limited."
    else:
        label = "NO_SIZE_CONTEXT"
        multiplier = 0.0
        reason = "entry quality context is invalid or low confidence."
    return {
        "size_quality_label": label,
        "size_multiplier_hint": multiplier,
        "reason": reason,
        "advisory_only": True,
        "authority_boundary": "governance and exposure gates remain authoritative.",
    }


def _calculate_market_features(candles: Sequence[NormalizedCandle], *, side: str) -> dict[str, float]:
    recent = candles[-min(4, len(candles)) :]
    all_ranges = [candle.range_pct for candle in candles]
    recent_pressure = sum(_signed_body(candle, side=side) for candle in recent) / len(recent)
    context_pressure = sum(_signed_body(candle, side=side) for candle in candles) / len(candles)
    close_location = sum(
        candle.close_location if side == CandidateSide.LONG.value else 1.0 - candle.close_location
        for candle in recent
    ) / len(recent)
    directional_alignment = _clamp01((recent_pressure + context_pressure + close_location) / 3.0)
    avg_range = sum(all_ranges) / len(all_ranges)
    range_variation = _range_variation([candle.range for candle in candles])
    volatility_range_fit = _clamp01(0.82 - min(0.35, range_variation) + min(0.15, avg_range * 100.0))
    return {
        "recent_directional_pressure": recent_pressure,
        "context_directional_pressure": context_pressure,
        "side_close_location": close_location,
        "directional_alignment": directional_alignment,
        "avg_range_pct": avg_range,
        "range_variation": range_variation,
        "volatility_range_fit": volatility_range_fit,
    }


def _weighted_score(dimensions: Mapping[str, float]) -> float:
    return _clamp01(sum(dimensions[key] * weight for key, weight in DIMENSION_WEIGHTS.items()))


def _confidence(
    *,
    acceptance_class: str,
    acceptance_score: float,
    completed_count: int,
    dimensions: Mapping[str, float],
    thresholds: EntryAcceptanceThresholds,
) -> float:
    if acceptance_class == LOW_CONFIDENCE_INSUFFICIENT_DATA:
        return 0.0
    sample_confidence = min(1.0, completed_count / thresholds.preferred_completed_candles)
    dimension_floor = min(dimensions.values())
    return _clamp01((acceptance_score * 0.55) + (sample_confidence * 0.25) + (dimension_floor * 0.20))


def _supporting_reasons(
    *,
    acceptance_class: str,
    dimensions: Mapping[str, float],
    features: Mapping[str, float],
    participation_quality: Mapping[str, Any] | None,
) -> list[str]:
    reasons = [f"candidate classified as {acceptance_class} with deterministic offline scoring."]
    if dimensions["structural_similarity"] >= 0.90:
        reasons.append("required structural predicates are strongly aligned.")
    elif dimensions["structural_similarity"] >= 0.70:
        reasons.append("core structure is close enough for advisory near-match context.")
    if dimensions["directional_alignment"] >= 0.70:
        reasons.append("completed primary-timeframe directional context supports the candidate side.")
    if dimensions["pullback_retest_quality"] >= 0.70:
        reasons.append("pullback/retest quality supports the candidate context.")
    if participation_quality and dimensions["participation_support"] >= 0.75:
        reasons.append("participation quality is supportive for the candidate side.")
    if features["volatility_range_fit"] >= 0.70:
        reasons.append("recent range quality is adequate for advisory scoring.")
    return reasons


def _context_failure_reasons(dimensions: Mapping[str, float]) -> tuple[str, ...]:
    failures: list[str] = []
    if dimensions["participation_support"] < 0.40:
        failures.append("PARTICIPATION_HOSTILE")
    if dimensions["failure_risk_flags"] < 0.55:
        failures.append("ELEVATED_FAILURE_RISK")
    if dimensions["volatility_range_fit"] < 0.45:
        failures.append("POOR_VOLATILITY_RANGE_FIT")
    if dimensions["pullback_retest_quality"] < 0.45:
        failures.append("WEAK_PULLBACK_RETEST_QUALITY")
    return tuple(failures)


def _predicate_partition(mapping: Mapping[str, Any], predicates: Sequence[str]) -> tuple[list[str], list[str]]:
    matched: list[str] = []
    missing: list[str] = []
    for predicate in predicates:
        if mapping.get(predicate) is True:
            matched.append(predicate)
        else:
            missing.append(predicate)
    return matched, missing


def _near_miss_predicates_for_breakout_retest_hold(
    *,
    state: Mapping[str, Any],
    features: Mapping[str, Any],
    missing_context: Sequence[str],
) -> list[str]:
    near_misses: list[str] = []
    if "breakout_bar_expansion_is_normal" in missing_context and _breakout_retest_hold_range_score(features) >= 0.70:
        near_misses.append("breakout_range_expansion_near_normal")
    if "breakout_bar_slope_is_flat" in missing_context and _breakout_retest_hold_slope_score(features) >= 0.70:
        near_misses.append("breakout_slope_near_flat")
    if "asia_early_or_gc_mgc_london_open" in missing_context and state.get("session_asia") is True:
        near_misses.append("session_window_near_preferred")
    return near_misses


def _breakout_retest_hold_failure_flags(
    *,
    state: Mapping[str, Any],
    features: Mapping[str, Any],
    missing_required: Sequence[str],
    missing_context: Sequence[str],
) -> list[str]:
    flags: list[str] = []
    if missing_required:
        flags.append("FAILED_BREAKOUT_RETEST_HOLD_STRUCTURE")
    if "no_first_bull_snap_turn" in missing_context:
        flags.append("SNAP_TURN_CONFLICT")
    if "prior_bars_since_long_setup_gt_anti_churn" in missing_context:
        flags.append("ANTI_CHURN_CONFLICT")
    if (_optional_float(features.get("churn_score")) or 0.0) >= 0.50:
        flags.append("ANTI_CHURN_PRESSURE")
    if (_optional_float(features.get("snap_turn_conflict_strength")) or 0.0) >= 0.50:
        flags.append("SNAP_TURN_PRESSURE")
    if _breakout_retest_hold_range_score(features) < 0.55:
        flags.append("RANGE_EXPANSION_OUT_OF_BOUNDS")
    if _breakout_retest_hold_slope_score(features) < 0.55:
        flags.append("BREAKOUT_SLOPE_NOT_FLAT")
    if state.get("allow_asia") is False:
        flags.append("SESSION_NOT_ALLOWED")
    return _dedupe(flags)


def _breakout_retest_hold_structural_score(
    *,
    matched_required: Sequence[str],
    missing_required: Sequence[str],
    near_miss_predicates: Sequence[str],
) -> float:
    if missing_required:
        return 0.25 if matched_required else 0.10
    return _clamp01(0.96 - (0.10 * len(near_miss_predicates)))


def _breakout_retest_hold_directional_score(event_payload: Mapping[str, Any]) -> float:
    metadata = event_payload.get("metadata") if isinstance(event_payload.get("metadata"), Mapping) else {}
    feature_payload = metadata.get(ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURES_KEY)
    features = feature_payload if isinstance(feature_payload, Mapping) else {}
    close_location = _optional_float(features.get("close_location"))
    body_to_range = _optional_float(features.get("body_to_range_ratio"))
    if close_location is not None and body_to_range is not None:
        return _clamp01((_clamp01(close_location) * 0.60) + (_clamp01(body_to_range) * 0.40))

    open_price = _optional_float(event_payload.get("open"))
    high = _optional_float(event_payload.get("high"))
    low = _optional_float(event_payload.get("low"))
    close = _optional_float(event_payload.get("close") or event_payload.get("last"))
    if open_price is None or high is None or low is None or close is None or high <= low:
        return 0.70
    body_score = _clamp01(((close - open_price) / (high - low) + 1.0) / 2.0)
    close_location = _clamp01((close - low) / (high - low))
    return _clamp01((body_score * 0.45) + (close_location * 0.55))


def _breakout_retest_hold_timing_score(state: Mapping[str, Any]) -> float:
    if state.get("asia_early_or_gc_mgc_london_open") is True and state.get("allow_asia") is True:
        return 0.96
    if state.get("session_asia") is True and state.get("allow_asia") is True:
        return 0.72
    if state.get("allow_asia") is True:
        return 0.55
    return 0.25


def _breakout_retest_hold_range_score(features: Mapping[str, Any]) -> float:
    if features.get("breakout_bar_expansion_is_normal") is True:
        return 0.92
    ratio = _optional_float(features.get("range_expansion_ratio") or features.get("breakout_range_expansion_ratio"))
    minimum = _optional_float(features.get("breakout_min_range_expansion_ratio"))
    maximum = _optional_float(features.get("breakout_max_range_expansion_ratio"))
    if ratio is None or minimum is None or maximum is None or minimum >= maximum:
        return 0.55
    if minimum <= ratio <= maximum:
        return 0.90
    nearest_bound = minimum if ratio < minimum else maximum
    tolerance = max(0.01, maximum - minimum)
    distance = abs(ratio - nearest_bound) / tolerance
    return _clamp01(0.78 - min(0.40, distance * 0.35))


def _breakout_retest_hold_slope_score(features: Mapping[str, Any]) -> float:
    if features.get("breakout_bar_slope_is_flat") is True:
        return 0.92
    slope = _optional_float(features.get("breakout_normalized_slope"))
    max_abs = _optional_float(features.get("breakout_abs_slope_max"))
    if slope is None or max_abs is None or max_abs <= 0:
        return 0.55
    excess = max(0.0, abs(slope) - max_abs) / max_abs
    return _clamp01(0.78 - min(0.45, excess * 0.35))


def _breakout_retest_hold_retest_score(
    *,
    event_payload: Mapping[str, Any],
    features: Mapping[str, Any],
) -> float:
    if features.get("signal_retests_and_holds_breakout_level") is not True:
        return 0.20
    margin_normalized = _optional_float(features.get("hold_margin_normalized"))
    retest_depth_normalized = _optional_float(features.get("retest_depth_normalized"))
    margin = _breakout_retest_hold_margin(event_payload=event_payload, features=features)
    if margin_normalized is None and retest_depth_normalized is None and margin is None:
        return 0.86
    resolved_margin = margin_normalized if margin_normalized is not None else margin
    resolved_depth = retest_depth_normalized if retest_depth_normalized is not None else 0.0
    assert resolved_margin is not None
    margin_component = 0.62 + min(0.25, max(-0.25, resolved_margin) * 0.35)
    depth_component = 0.24 if resolved_depth > 0 else 0.16
    excessive_depth_penalty = min(0.20, max(0.0, resolved_depth - 0.75) * 0.20)
    return _clamp01(margin_component + depth_component - excessive_depth_penalty)


def _breakout_retest_hold_margin(
    *,
    event_payload: Mapping[str, Any],
    features: Mapping[str, Any],
) -> float | None:
    explicit = _optional_float(features.get("hold_margin_ticks_or_points"))
    if explicit is not None:
        return explicit
    close = _optional_float(event_payload.get("close") or event_payload.get("last"))
    breakout_level = _optional_float(features.get("breakout_level"))
    if close is None or breakout_level is None:
        return None
    return close - breakout_level


def _breakout_retest_hold_failure_risk_score(flags: Sequence[str]) -> float:
    if not flags:
        return 0.95
    severe = {"FAILED_BREAKOUT_RETEST_HOLD_STRUCTURE", "SNAP_TURN_CONFLICT", "SESSION_NOT_ALLOWED"}
    penalty = 0.10 * len(flags) + 0.12 * sum(1 for flag in flags if flag in severe)
    return _clamp01(1.0 - min(0.70, penalty))


def _freshness_status(
    candles: Sequence[NormalizedCandle],
    *,
    now: datetime,
    thresholds: EntryAcceptanceThresholds,
) -> tuple[str, list[str]]:
    failures: list[str] = []
    latest = candles[-1].timestamp
    age = now - latest
    if age < timedelta(0):
        failures.append(STALE_INPUT)
        return "FUTURE_DATED", failures
    max_age = timedelta(minutes=5 * thresholds.stale_after_intervals)
    status = "FRESH" if age <= max_age else "STALE"
    if status == "STALE":
        failures.append(STALE_INPUT)
    max_gap = timedelta(minutes=5 * thresholds.max_missing_gap_intervals)
    for previous, current in zip(candles, candles[1:]):
        if current.timestamp - previous.timestamp > max_gap:
            failures.append(STALE_INPUT)
            status = "GAPPED"
            break
    return status, failures


def _normalize_candle(row: Mapping[str, Any], *, timeframe: str) -> NormalizedCandle:
    timestamp = _coerce_datetime(row.get("timestamp") or row.get("ts") or row.get("datetime"))
    open_price = _finite_float(row.get("open"))
    high = _finite_float(row.get("high"))
    low = _finite_float(row.get("low"))
    close_price = _finite_float(row.get("close"))
    volume = _optional_float(row.get("volume"))
    return NormalizedCandle(
        timestamp=timestamp,
        open=open_price,
        high=high,
        low=low,
        close=close_price,
        volume=volume,
        timeframe=timeframe,
    )


def _extract_candidate(payload: Mapping[str, Any]) -> Any:
    return payload.get("candidate") or payload.get("candidate_pattern_metadata") or payload.get("pattern_candidate")


def _candidate_timeframe(payload: Mapping[str, Any]) -> str | None:
    candidate = _extract_candidate(payload)
    if isinstance(candidate, Mapping):
        return _string_or_none(candidate.get("candidate_timeframe") or candidate.get("timeframe"))
    return None


def _market_context_timeframe(payload: Mapping[str, Any]) -> str | None:
    context = payload.get("market_context") or payload.get("completed_5m_market_context")
    if isinstance(context, Mapping):
        return _string_or_none(context.get("context_timeframe") or context.get("timeframe") or context.get("candle_timeframe"))
    return None


def _extract_candle_rows(payload: Mapping[str, Any]) -> Any:
    for key in ("candles", "ohlcv_candles", "bars"):
        if key in payload:
            return payload.get(key)
    context = payload.get("completed_5m_market_context") or payload.get("market_context")
    if isinstance(context, Mapping):
        for key in ("candles", "ohlcv_candles", "bars"):
            if key in context:
                return context.get(key)
    return None


def _extract_participation_quality(payload: Mapping[str, Any]) -> Mapping[str, Any] | None:
    value = payload.get("participation_quality") or payload.get("participation_quality_context")
    return value if isinstance(value, Mapping) else None


def _provenance_failure_reasons(input_context: Mapping[str, Any]) -> list[str]:
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


def _data_provenance_confidence(input_context: Mapping[str, Any]) -> float:
    category = str(input_context.get("input_source_category") or SOURCE_CATEGORY_UNKNOWN)
    mode = str(input_context.get("input_mode") or INPUT_MODE_UNKNOWN)
    if category == SOURCE_CATEGORY_RUNTIME and mode == INPUT_MODE_RUNTIME_DECISION:
        return 1.0
    if category == SOURCE_CATEGORY_TEST_FIXTURE and mode == INPUT_MODE_TEST:
        return 0.85
    if category == SOURCE_CATEGORY_RESEARCH and mode in {INPUT_MODE_REPLAY_RESEARCH, INPUT_MODE_OFFLINE_EVALUATION}:
        return 0.75
    return 0.35


def _safety_flags() -> dict[str, bool]:
    return {
        "strategy_authority": False,
        "broker_state_mutated": False,
        "submit_attempted": False,
        "order_intent_created": False,
        "lifecycle_mutated": False,
        "runtime_trade_eligible": False,
    }


def _low_confidence_dimensions() -> dict[str, float]:
    return {key: 0.0 for key in DIMENSION_WEIGHTS}


def _empty_feature_summary() -> dict[str, float]:
    return {
        "recent_directional_pressure": 0.0,
        "context_directional_pressure": 0.0,
        "side_close_location": 0.0,
        "directional_alignment": 0.0,
        "avg_range_pct": 0.0,
        "range_variation": 0.0,
        "volatility_range_fit": 0.0,
    }


def _normalize_instrument(payload: Mapping[str, Any]) -> str:
    candidate = _extract_candidate(payload)
    instrument = payload.get("instrument")
    if not instrument and isinstance(candidate, Mapping):
        instrument = candidate.get("instrument") or candidate.get("symbol")
    context = payload.get("session_instrument_context")
    if not instrument and isinstance(context, Mapping):
        instrument = context.get("instrument") or context.get("symbol")
    return str(instrument or "UNKNOWN").strip().upper() or "UNKNOWN"


def _source_id(payload: Mapping[str, Any]) -> str | None:
    runtime_provenance = payload.get("runtime_provenance")
    if isinstance(runtime_provenance, Mapping):
        source_id = _string_or_none(runtime_provenance.get("source_id"))
        if source_id:
            return source_id
    return _string_or_none(payload.get("source_id"))


def _approved_derived_timeframe(timeframe_context: Mapping[str, Any], payload: Mapping[str, Any]) -> bool:
    value = (
        timeframe_context.get("approved_candle_builder")
        or payload.get("approved_candle_builder")
        or timeframe_context.get("derived_timeframe_approved")
        or payload.get("derived_timeframe_approved")
    )
    if bool(value):
        return True
    status = str(
        timeframe_context.get("builder_approval_status")
        or payload.get("builder_approval_status")
        or ""
    ).strip().upper()
    return status == "APPROVED"


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


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    return _coerce_datetime(value)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def _normalize_timeframe(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"5", "5m", "5min", "5minute", "5minutes"}:
        return "5m"
    return normalized or DEFAULT_CANDLE_TIMEFRAME


def _optional_timeframe(value: str | None) -> str | None:
    if value is None:
        return None
    return _normalize_timeframe(value)


def _finite_float(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("value must be finite")
    return result


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return _finite_float(value)
    except (TypeError, ValueError):
        return None


def _numeric_score(value: Any, *, default: float) -> float:
    parsed = _optional_float(value)
    return _clamp01(default if parsed is None else parsed)


def _nested_score(mapping: Mapping[str, Any], key: str, nested_key: str) -> Any:
    value = mapping.get(key)
    if isinstance(value, Mapping):
        return value.get(nested_key)
    return None


def _signed_body(candle: NormalizedCandle, *, side: str) -> float:
    raw = candle.body / candle.range
    return _clamp01((raw + 1.0) / 2.0) if side == CandidateSide.LONG.value else _clamp01((1.0 - raw) / 2.0)


def _range_variation(ranges: Sequence[float]) -> float:
    if not ranges:
        return 0.0
    average = sum(ranges) / len(ranges)
    if average <= 0:
        return 1.0
    return sum(abs(value - average) for value in ranges) / len(ranges) / average


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _dedupe(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def _confidence_state(confidence: float) -> str:
    if confidence >= 0.80:
        return "HIGH"
    if confidence >= 0.55:
        return "MEDIUM"
    if confidence > 0.0:
        return "LOW"
    return LOW_CONFIDENCE_INSUFFICIENT_DATA


def _dimension_summary(name: str, value: float) -> str:
    if value >= 0.80:
        descriptor = "strong"
    elif value >= 0.60:
        descriptor = "acceptable"
    elif value >= 0.40:
        descriptor = "marginal"
    else:
        descriptor = "weak"
    return f"{name} is {descriptor}."


def _round(value: float) -> float:
    return round(float(value), 4)


def _round_mapping(values: Mapping[str, float]) -> dict[str, float]:
    return {key: _round(value) for key, value in values.items()}


def _clamp01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_tmp_or_test_path(path: Path) -> bool:
    text = str(path)
    parts = set(path.parts)
    return text.startswith("/tmp/") or text.startswith("/private/tmp/") or "pytest-" in text or (
        "tests" in parts and "fixtures" in parts
    )
