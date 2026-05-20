"""PAPER-only operational maturation helpers for sparse probationary lanes."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from ..execution_core.track_b_entry_acceptance import (
    DIMENSION_WEIGHTS,
    ENTRY_ACCEPTANCE_LEVEL_B_PLUS,
    NEAR_STRUCTURAL_MATCH,
    EntryAcceptanceThresholds,
    entry_acceptance_level_policy,
)


OPERATIONAL_MATURATION_PROFILE = "PAPER_ONLY_OPERATIONAL_MATURATION_V1"
B_PLUS_SETUP_SCORE_LABELS = (
    "PAPER_ONLY",
    "OPERATIONAL_MATURATION",
    "B_PLUS_SETUP_SCORE",
    "NON_PRODUCTION",
    "NOT_PROMOTION_ELIGIBLE",
)


@dataclass(frozen=True)
class BPlusSetupScore:
    exact_match: bool
    b_plus_match: bool
    score: float
    threshold: float
    passed_components: tuple[str, ...]
    failed_components: tuple[str, ...]
    mandatory_gate_failures: tuple[str, ...]
    reason: str
    component_results: dict[str, bool]
    dimension_scores: dict[str, float]
    acceptance_level: str = ENTRY_ACCEPTANCE_LEVEL_B_PLUS
    acceptance_class: str = NEAR_STRUCTURAL_MATCH
    source_layer: str = "track_b_entry_acceptance_level_policy_v1"
    non_production: bool = True
    operational_maturation: bool = True
    paper_only: bool = True
    live_money_eligible: bool = False
    labels: tuple[str, ...] = B_PLUS_SETUP_SCORE_LABELS

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["labels"] = list(self.labels)
        payload["passed_components"] = list(self.passed_components)
        payload["failed_components"] = list(self.failed_components)
        payload["mandatory_gate_failures"] = list(self.mandatory_gate_failures)
        return payload


def operational_maturation_params(lane_spec: Any) -> dict[str, Any]:
    params = dict(getattr(lane_spec, "runtime_overlay_params", {}) or {})
    if not params.get("operational_maturation_mode"):
        return {}
    if str(params.get("operational_maturation_profile") or "") != OPERATIONAL_MATURATION_PROFILE:
        return {}
    if not bool(getattr(lane_spec, "paper_only", True)):
        return {}
    if params.get("paper_only") is False:
        return {}
    if bool(getattr(lane_spec, "live_money_eligible", False)) or bool(params.get("live_money_eligible", False)):
        return {}
    return params


def operational_maturation_enabled(lane_spec: Any) -> bool:
    return bool(operational_maturation_params(lane_spec))


def operational_entry_reason(
    *,
    lane_spec: Any,
    segment_bars: list[Any],
    current_index: int,
    setup_bar_count: int,
    tick_size: float,
) -> str | None:
    params = operational_maturation_params(lane_spec)
    if not params:
        return None
    if params.get("operational_maturation_timed_entry_enabled") is False:
        return None
    if current_index < setup_bar_count:
        return None

    forced_bar = _coerce_int(params.get("operational_maturation_forced_entry_bar"), default=5)
    forced_bar = max(setup_bar_count + 1, min(forced_bar, 12))
    entry_index = forced_bar - 1
    catchup_bars = max(0, min(_coerce_int(params.get("operational_maturation_entry_catchup_bars"), default=0), 3))
    if current_index < entry_index or current_index > entry_index + catchup_bars:
        return None

    min_setup_range_ticks = max(
        0,
        min(_coerce_int(params.get("operational_maturation_min_setup_range_ticks"), default=2), 20),
    )
    if min_setup_range_ticks > 0 and not _setup_range_passes(
        segment_bars=segment_bars,
        setup_bar_count=setup_bar_count,
        tick_size=tick_size,
        min_setup_range_ticks=min_setup_range_ticks,
    ):
        return None

    return f"operational_maturation_timed_bar{forced_bar}_catchup{catchup_bars}"


def b_plus_setup_score(
    *,
    lane_spec: Any,
    segment_bars: list[Any],
    current_index: int,
    setup_bar_count: int,
    tick_size: float,
    side: str,
    exact_match: bool,
    preferred_or_near_trigger: bool,
    fallback_entry_bar: int | None = None,
) -> BPlusSetupScore:
    params = operational_maturation_params(lane_spec)
    requested_level = str(params.get("operational_maturation_entry_acceptance_level") or ENTRY_ACCEPTANCE_LEVEL_B_PLUS).upper()
    configured_threshold = _coerce_float(params.get("operational_maturation_b_plus_threshold"), default=0.80)
    thresholds = EntryAcceptanceThresholds(b_plus_score=max(0.70, min(configured_threshold, 0.84)))
    policy = entry_acceptance_level_policy(requested_level, thresholds=thresholds)
    threshold = policy.min_score
    if policy.level != ENTRY_ACCEPTANCE_LEVEL_B_PLUS:
        return _b_plus_fail(
            threshold=threshold,
            reason="unsupported_acceptance_level_for_b_plus_runtime",
            gate_failures=("unsupported_acceptance_level_for_b_plus_runtime",),
            exact_match=exact_match,
        )
    if not params.get("operational_maturation_b_plus_enabled"):
        return _b_plus_fail(
            threshold=threshold,
            reason="b_plus_not_enabled",
            gate_failures=("b_plus_not_enabled",),
            exact_match=exact_match,
        )
    if exact_match:
        return BPlusSetupScore(
            exact_match=True,
            b_plus_match=False,
            score=1.0,
            threshold=threshold,
            passed_components=("exact_match",),
            failed_components=(),
            mandatory_gate_failures=(),
            reason="exact_match_preserved",
            component_results={"exact_match": True},
            dimension_scores={key: 1.0 for key in DIMENSION_WEIGHTS},
            acceptance_level="EXACT",
            acceptance_class="EXACT_STRUCTURAL_MATCH",
        )

    mandatory_failures: list[str] = []
    normalized_side = str(side or "").upper()
    if normalized_side not in {"LONG", "SHORT"}:
        mandatory_failures.append("direction_unknown")
    if len(segment_bars) < setup_bar_count:
        mandatory_failures.append("setup_bars_missing")
    if current_index < setup_bar_count or current_index >= len(segment_bars):
        mandatory_failures.append("current_bar_outside_evaluation_window")
    if not _phase1_runtime_source_ok(lane_spec=lane_spec, params=params):
        mandatory_failures.append("wrong_provenance")
    if params.get("operational_maturation_b_plus_phase1_artifact_fresh") is False:
        mandatory_failures.append("stale_phase1_artifact")

    min_setup_range_ticks = max(0, min(_coerce_int(params.get("operational_maturation_min_setup_range_ticks"), default=2), 20))
    range_activity = (
        _setup_range_passes(
            segment_bars=segment_bars,
            setup_bar_count=setup_bar_count,
            tick_size=tick_size,
            min_setup_range_ticks=min_setup_range_ticks,
        )
        if min_setup_range_ticks > 0
        else True
    )
    if not range_activity:
        mandatory_failures.append("range_activity_minimum_failed")
    if mandatory_failures:
        return _b_plus_fail(
            threshold=threshold,
            reason="mandatory_gate_failed",
            gate_failures=tuple(mandatory_failures),
            exact_match=False,
        )

    setup_bars = segment_bars[:setup_bar_count]
    setup_high = max(float(candidate.high) for candidate in setup_bars)
    setup_low = min(float(candidate.low) for candidate in setup_bars)
    midpoint = setup_low + ((setup_high - setup_low) * 0.5)
    previous = segment_bars[current_index - 1]
    current = segment_bars[current_index]
    current_open = float(current.open)
    current_high = float(current.high)
    current_low = float(current.low)
    current_close = float(current.close)
    previous_close = float(previous.close)
    previous_high = float(previous.high)
    previous_low = float(previous.low)

    if normalized_side == "LONG":
        direction_color = current_close >= current_open
        direction_progress = current_close >= previous_close
        near_break = current_high >= previous_high or current_close >= midpoint
        midpoint_support = current_close >= midpoint
        setup_pressure = current_high >= setup_high or current_close >= setup_low
    else:
        direction_color = current_close <= current_open
        direction_progress = current_close <= previous_close
        near_break = current_low <= previous_low or current_close <= midpoint
        midpoint_support = current_close <= midpoint
        setup_pressure = current_low <= setup_low or current_close <= setup_high

    fallback_bar = fallback_entry_bar if fallback_entry_bar is not None else setup_bar_count + 4
    max_b_plus_bar = max(
        setup_bar_count + 1,
        min(_coerce_int(params.get("operational_maturation_b_plus_max_entry_bar"), default=fallback_bar + 2), 14),
    )
    timing_near_rule = current_index <= max_b_plus_bar - 1
    volume_present = int(getattr(current, "volume", 0) or 0) > 0
    directional_confirmation = bool(preferred_or_near_trigger or direction_color or direction_progress or near_break)
    if not directional_confirmation:
        return _b_plus_fail(
            threshold=threshold,
            reason="directional_confirmation_missing",
            gate_failures=("directional_confirmation_missing",),
            exact_match=False,
        )

    components = {
        "range_activity": range_activity,
        "direction_color": direction_color,
        "direction_progress": direction_progress,
        "preferred_or_near_trigger": bool(preferred_or_near_trigger or near_break),
        "midpoint_support": midpoint_support,
        "setup_pressure": setup_pressure,
        "timing_near_source_rule": timing_near_rule,
        "volume_present": volume_present,
    }
    dimensions = _b_plus_dimension_scores(
        components=components,
        preferred_or_near_trigger=bool(preferred_or_near_trigger),
    )
    passed = tuple(key for key, value in components.items() if value)
    failed = tuple(key for key, value in components.items() if not value)
    score = round(sum(dimensions[key] * weight for key, weight in DIMENSION_WEIGHTS.items()), 6)
    b_plus_match = score >= threshold and dimensions["structural_similarity"] >= policy.min_structural_similarity
    return BPlusSetupScore(
        exact_match=False,
        b_plus_match=b_plus_match,
        score=score,
        threshold=threshold,
        passed_components=passed,
        failed_components=failed,
        mandatory_gate_failures=(),
        reason=(
            f"operational_maturation_b_plus_score_{score:.2f}"
            if b_plus_match
            else f"b_plus_score_below_threshold_{score:.2f}"
        ),
        component_results=components,
        dimension_scores=dimensions,
    )


def b_plus_session_key(*, lane_id: str, segment_id: str, bar: Any) -> str:
    return "|".join(
        [str(lane_id or "unknown_lane"), str(segment_id or "unknown_segment"), str(getattr(bar, "end_ts", "unknown_time"))[:10]]
    )


def augment_intent_summary_with_b_plus(summary: dict[str, object], engine: Any, bar: Any) -> dict[str, object]:
    latest = getattr(engine, "_latest_b_plus_setup_score", None)
    if not isinstance(latest, dict):
        return summary
    if latest.get("bar_id") != getattr(bar, "bar_id", None):
        return summary
    return {
        **summary,
        "b_plus_score": latest.get("b_plus_score"),
        "b_plus_threshold": latest.get("b_plus_threshold"),
        "b_plus_components": latest.get("b_plus_components"),
        "b_plus_dimension_scores": latest.get("b_plus_dimension_scores"),
        "b_plus_acceptance_level": latest.get("b_plus_acceptance_level"),
        "b_plus_acceptance_class": latest.get("b_plus_acceptance_class"),
        "b_plus_passed_components": latest.get("b_plus_passed_components"),
        "b_plus_failed_components": latest.get("b_plus_failed_components"),
        "b_plus_mandatory_gate_failures": latest.get("b_plus_mandatory_gate_failures"),
        "b_plus_reason": latest.get("b_plus_reason"),
        "non_production": True,
        "operational_maturation": True,
        "b_plus_setup_score": True,
        "not_promotion_eligible": True,
        "live_money_eligible": False,
    }


def b_plus_diagnostic_payload(*, result: BPlusSetupScore, bar: Any, lane_id: str, source_id: str, side: str) -> dict[str, Any]:
    return {
        "bar_id": getattr(bar, "bar_id", None),
        "bar_end_ts": getattr(getattr(bar, "end_ts", None), "isoformat", lambda: None)(),
        "lane_id": lane_id,
        "source_id": source_id,
        "side": str(side).upper(),
        "b_plus_score": result.score,
        "b_plus_threshold": result.threshold,
        "b_plus_match": result.b_plus_match,
        "b_plus_components": dict(result.component_results),
        "b_plus_dimension_scores": dict(result.dimension_scores),
        "b_plus_acceptance_level": result.acceptance_level,
        "b_plus_acceptance_class": result.acceptance_class,
        "b_plus_source_layer": result.source_layer,
        "b_plus_passed_components": list(result.passed_components),
        "b_plus_failed_components": list(result.failed_components),
        "b_plus_mandatory_gate_failures": list(result.mandatory_gate_failures),
        "b_plus_reason": result.reason,
        "non_production": True,
        "operational_maturation": True,
        "b_plus_setup_score": True,
        "paper_only": True,
        "live_money_eligible": False,
        "labels": list(B_PLUS_SETUP_SCORE_LABELS),
    }


def _b_plus_dimension_scores(*, components: dict[str, bool], preferred_or_near_trigger: bool) -> dict[str, float]:
    structural_passes = sum(
        1
        for key in ("range_activity", "midpoint_support", "setup_pressure", "preferred_or_near_trigger")
        if components.get(key)
    )
    directional_passes = sum(
        1
        for key in ("direction_color", "direction_progress", "preferred_or_near_trigger")
        if components.get(key)
    )
    structural = 0.55 + (0.30 * (structural_passes / 4.0))
    if preferred_or_near_trigger:
        structural += 0.05
    directional = 0.55 + (0.40 * (directional_passes / 3.0))
    return {
        "structural_similarity": round(min(0.90, structural), 6),
        "directional_alignment": round(min(0.95, directional), 6),
        "timing_session_fit": 1.0 if components.get("timing_near_source_rule") else 0.55,
        "volatility_range_fit": 0.82 if components.get("range_activity") else 0.0,
        "pullback_retest_quality": 0.82 if components.get("midpoint_support") else 0.62,
        "participation_support": 0.65,
        "failure_risk_flags": 0.86 if components.get("volume_present") else 0.68,
        "data_provenance_confidence": 1.0,
    }


def _setup_range_passes(
    *,
    segment_bars: list[Any],
    setup_bar_count: int,
    tick_size: float,
    min_setup_range_ticks: int,
) -> bool:
    if len(segment_bars) < setup_bar_count:
        return False
    setup_bars = segment_bars[:setup_bar_count]
    setup_high = max(float(candidate.high) for candidate in setup_bars)
    setup_low = min(float(candidate.low) for candidate in setup_bars)
    return (setup_high - setup_low) >= float(tick_size) * float(min_setup_range_ticks)


def _phase1_runtime_source_ok(*, lane_spec: Any, params: dict[str, Any]) -> bool:
    required_provenance = str(
        params.get("operational_maturation_b_plus_required_market_data_provenance")
        or getattr(lane_spec, "required_market_data_provenance", "DATABENTO_REALTIME_PHASE1")
        or ""
    )
    market_data_source = str(
        params.get("operational_maturation_b_plus_required_market_data_source")
        or getattr(lane_spec, "market_data_source", getattr(lane_spec, "probationary_paper_market_data_source", "phase1_runtime_artifact"))
        or ""
    )
    if required_provenance != "DATABENTO_REALTIME_PHASE1":
        return False
    return market_data_source == "phase1_runtime_artifact"


def _b_plus_fail(
    *,
    threshold: float,
    reason: str,
    gate_failures: tuple[str, ...],
    exact_match: bool,
) -> BPlusSetupScore:
    return BPlusSetupScore(
        exact_match=exact_match,
        b_plus_match=False,
        score=0.0,
        threshold=threshold,
        passed_components=(),
        failed_components=(),
        mandatory_gate_failures=gate_failures,
        reason=reason,
        component_results={},
        dimension_scores={key: 0.0 for key in DIMENSION_WEIGHTS},
    )


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
