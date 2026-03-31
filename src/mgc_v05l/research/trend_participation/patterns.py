"""Bounded pattern families for the Active Trend Participation Engine."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable

from .conflict import resolve_conflict
from .models import FeatureState, HigherPrioritySignal, PatternVariant, SignalDecision


STRICTNESS_LIBRARY_BY_PROFILE = {
    "phase2_baseline": {
        "active": {
            "entry_window_bars_1m": 6,
            "max_hold_bars_1m": 20,
            "stop_atr_multiple": 0.70,
            "target_r_multiple": 1.20,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
        "conservative": {
            "entry_window_bars_1m": 3,
            "max_hold_bars_1m": 18,
            "stop_atr_multiple": 0.95,
            "target_r_multiple": 1.8,
            "local_cooldown_bars_1m": 3,
            "reset_window_bars_5m": 2,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
        "base": {
            "entry_window_bars_1m": 5,
            "max_hold_bars_1m": 24,
            "stop_atr_multiple": 0.85,
            "target_r_multiple": 1.6,
            "local_cooldown_bars_1m": 2,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
        "aggressive": {
            "entry_window_bars_1m": 7,
            "max_hold_bars_1m": 30,
            "stop_atr_multiple": 0.75,
            "target_r_multiple": 1.4,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
    },
    "phase3_window_extension": {
        "active": {
            "entry_window_bars_1m": 9,
            "max_hold_bars_1m": 20,
            "stop_atr_multiple": 0.70,
            "target_r_multiple": 1.20,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
        "conservative": {
            "entry_window_bars_1m": 3,
            "max_hold_bars_1m": 18,
            "stop_atr_multiple": 0.95,
            "target_r_multiple": 1.8,
            "local_cooldown_bars_1m": 3,
            "reset_window_bars_5m": 2,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
        "base": {
            "entry_window_bars_1m": 5,
            "max_hold_bars_1m": 24,
            "stop_atr_multiple": 0.85,
            "target_r_multiple": 1.6,
            "local_cooldown_bars_1m": 2,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
        "aggressive": {
            "entry_window_bars_1m": 7,
            "max_hold_bars_1m": 30,
            "stop_atr_multiple": 0.75,
            "target_r_multiple": 1.4,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.0,
        },
    },
    "phase3_reclaim_band": {
        "active": {
            "entry_window_bars_1m": 9,
            "max_hold_bars_1m": 20,
            "stop_atr_multiple": 0.70,
            "target_r_multiple": 1.20,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.18,
        },
        "conservative": {
            "entry_window_bars_1m": 3,
            "max_hold_bars_1m": 18,
            "stop_atr_multiple": 0.95,
            "target_r_multiple": 1.8,
            "local_cooldown_bars_1m": 3,
            "reset_window_bars_5m": 2,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.05,
        },
        "base": {
            "entry_window_bars_1m": 5,
            "max_hold_bars_1m": 24,
            "stop_atr_multiple": 0.85,
            "target_r_multiple": 1.6,
            "local_cooldown_bars_1m": 2,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.08,
        },
        "aggressive": {
            "entry_window_bars_1m": 7,
            "max_hold_bars_1m": 30,
            "stop_atr_multiple": 0.75,
            "target_r_multiple": 1.4,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "all",
            "trigger_reclaim_band_multiple": 0.12,
        },
    },
    "phase3_reentry_redesign": {
        "active": {
            "entry_window_bars_1m": 9,
            "max_hold_bars_1m": 20,
            "stop_atr_multiple": 0.70,
            "target_r_multiple": 1.20,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.18,
        },
        "conservative": {
            "entry_window_bars_1m": 3,
            "max_hold_bars_1m": 18,
            "stop_atr_multiple": 0.95,
            "target_r_multiple": 1.8,
            "local_cooldown_bars_1m": 3,
            "reset_window_bars_5m": 2,
            "allow_reentry": True,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.05,
        },
        "base": {
            "entry_window_bars_1m": 5,
            "max_hold_bars_1m": 24,
            "stop_atr_multiple": 0.85,
            "target_r_multiple": 1.6,
            "local_cooldown_bars_1m": 2,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.08,
        },
        "aggressive": {
            "entry_window_bars_1m": 7,
            "max_hold_bars_1m": 30,
            "stop_atr_multiple": 0.75,
            "target_r_multiple": 1.4,
            "local_cooldown_bars_1m": 1,
            "reset_window_bars_5m": 1,
            "allow_reentry": True,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.12,
        },
    },
    "phase3_full": {
        "active": {
            "entry_window_bars_1m": 9,
            "max_hold_bars_1m": 20,
            "stop_atr_multiple": 0.70,
            "target_r_multiple": 1.20,
            "local_cooldown_bars_1m": 0,
            "reset_window_bars_5m": 0,
            "allow_reentry": False,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.18,
        },
        "conservative": {
            "entry_window_bars_1m": 3,
            "max_hold_bars_1m": 18,
            "stop_atr_multiple": 0.95,
            "target_r_multiple": 1.8,
            "local_cooldown_bars_1m": 3,
            "reset_window_bars_5m": 2,
            "allow_reentry": False,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.05,
        },
        "base": {
            "entry_window_bars_1m": 5,
            "max_hold_bars_1m": 24,
            "stop_atr_multiple": 0.85,
            "target_r_multiple": 1.6,
            "local_cooldown_bars_1m": 2,
            "reset_window_bars_5m": 1,
            "allow_reentry": False,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.08,
        },
        "aggressive": {
            "entry_window_bars_1m": 8,
            "max_hold_bars_1m": 30,
            "stop_atr_multiple": 0.75,
            "target_r_multiple": 1.4,
            "local_cooldown_bars_1m": 0,
            "reset_window_bars_5m": 0,
            "allow_reentry": False,
            "reentry_policy": "structural_only",
            "trigger_reclaim_band_multiple": 0.14,
        },
    },
}


def default_pattern_variants(*, profile: str = "phase3_full") -> tuple[PatternVariant, ...]:
    strictness_library = STRICTNESS_LIBRARY_BY_PROFILE[profile]
    variants: list[PatternVariant] = []
    for family in (
        "pullback_continuation",
        "breakout_continuation",
        "pause_resume",
        "failed_countertrend_resumption",
    ):
        for side in ("LONG", "SHORT"):
            for strictness, config in strictness_library.items():
                entry_window_bars_1m = config["entry_window_bars_1m"]
                reclaim_band = config["trigger_reclaim_band_multiple"]
                if profile == "phase3_full" and side == "SHORT" and strictness in {"active", "aggressive"}:
                    entry_window_bars_1m += 2
                    reclaim_band += 0.04
                variants.append(
                    PatternVariant(
                        variant_id=f"trend_participation.{family}.{side.lower()}.{strictness}",
                        family=family,
                        side=side,
                        strictness=strictness,
                        description=_variant_description(family=family, side=side, strictness=strictness),
                        entry_window_bars_1m=entry_window_bars_1m,
                        max_hold_bars_1m=config["max_hold_bars_1m"],
                        stop_atr_multiple=config["stop_atr_multiple"],
                        target_r_multiple=config["target_r_multiple"],
                        local_cooldown_bars_1m=config["local_cooldown_bars_1m"],
                        reset_window_bars_5m=config["reset_window_bars_5m"],
                        allow_reentry=config.get("allow_reentry", True),
                        reentry_policy=config["reentry_policy"],
                        trigger_reclaim_band_multiple=reclaim_band,
                        notes=_variant_notes(family=family, strictness=strictness, profile=profile, side=side),
                    )
                )
    return tuple(variants)


def generate_signal_decisions(
    *,
    feature_rows: Iterable[FeatureState],
    variants: Iterable[PatternVariant],
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
) -> list[SignalDecision]:
    decisions: list[SignalDecision] = []
    grouped_variants = tuple(variants)
    higher_priority = tuple(higher_priority_signals)
    for feature in feature_rows:
        for variant in grouped_variants:
            if not variant_matches_feature(variant=variant, feature=feature):
                continue
            setup_quality_score = _setup_quality_score(feature=feature, variant=variant)
            conflict_outcome, block_reason = resolve_conflict(
                instrument=feature.instrument,
                side=variant.side,
                decision_ts=feature.decision_ts,
                entry_window_minutes=variant.entry_window_bars_1m,
                higher_priority_signals=higher_priority,
            )
            live_eligible = conflict_outcome.value == "no_conflict"
            decisions.append(
                SignalDecision(
                    decision_id=_decision_id(feature=feature, variant=variant),
                    instrument=feature.instrument,
                    variant_id=variant.variant_id,
                    family=variant.family,
                    side=variant.side,
                    strictness=variant.strictness,
                    decision_ts=feature.decision_ts,
                    session_date=feature.session_date,
                    session_segment=feature.session_segment,
                    regime_bucket=feature.regime_bucket,
                    volatility_bucket=feature.volatility_bucket,
                    conflict_outcome=conflict_outcome,
                    live_eligible=live_eligible,
                    shadow_only=not live_eligible,
                    block_reason=block_reason,
                    decision_bar_high=feature.high,
                    decision_bar_low=feature.low,
                    decision_bar_close=feature.close,
                    decision_bar_open=feature.open,
                    average_range=feature.average_range,
                    setup_signature=_setup_signature(feature=feature, variant=variant),
                    setup_state_signature=_setup_state_signature(feature=feature),
                    setup_quality_score=setup_quality_score,
                    setup_quality_bucket=_setup_quality_bucket(score=setup_quality_score),
                    feature_snapshot={
                        "trend_state": feature.trend_state,
                        "pullback_state": feature.pullback_state,
                        "expansion_state": feature.expansion_state,
                        "bar_anatomy": feature.bar_anatomy,
                        "momentum_persistence": feature.momentum_persistence,
                        "reference_state": feature.reference_state,
                        "volatility_range_state": feature.volatility_range_state,
                        "mtf_agreement_state": feature.mtf_agreement_state,
                        "atp_bias_state": feature.atp_bias_state,
                        "atp_pullback_state": feature.atp_pullback_state,
                        "atp_pullback_envelope_state": feature.atp_pullback_envelope_state,
                        "atp_pullback_reason": feature.atp_pullback_reason or "",
                    },
                )
            )
    return decisions


def summarize_signal_contexts(decisions: Iterable[SignalDecision]) -> dict[str, list[dict[str, object]]]:
    grouped: dict[tuple[str, str, str, str, str], list[SignalDecision]] = defaultdict(list)
    for decision in decisions:
        grouped[
            (
                decision.variant_id,
                decision.instrument,
                decision.session_segment,
                decision.regime_bucket,
                decision.volatility_bucket,
            )
        ].append(decision)
    rows = []
    for key, bucket in grouped.items():
        rows.append(
            {
                "variant_id": key[0],
                "instrument": key[1],
                "session_segment": key[2],
                "regime_bucket": key[3],
                "volatility_bucket": key[4],
                "signal_count": len(bucket),
                "shadow_signal_count": sum(1 for item in bucket if item.shadow_only),
            }
        )
    rows.sort(key=lambda row: (row["variant_id"], row["instrument"], row["signal_count"]), reverse=False)
    return {"rows": rows}


def variant_matches_feature(*, variant: PatternVariant, feature: FeatureState) -> bool:
    if variant.side == "LONG":
        if not feature_matches_side_context(side=variant.side, feature=feature):
            return False
        if variant.family == "pullback_continuation":
            return (
                feature.pullback_state in {"SHALLOW", "MODERATE", "DEEP"}
                and feature.bar_anatomy in {"LOWER_REJECTION", "BULL_IMPULSE", "BALANCED"}
                and feature.reference_state in {"ABOVE_SESSION_OPEN", "MID_RANGE", "NEAR_RECENT_HIGH"}
                and _strictness_ok(feature=feature, variant=variant)
            )
        if variant.family == "breakout_continuation":
            return (
                feature.expansion_state in {"NORMAL", "EXPANDED"}
                and feature.reference_state in {"NEAR_RECENT_HIGH", "ABOVE_SESSION_OPEN", "MID_RANGE"}
                and feature.bar_anatomy in {"BULL_IMPULSE", "BALANCED"}
                and _strictness_ok(feature=feature, variant=variant)
            )
        if variant.family == "pause_resume":
            return (
                feature.expansion_state in {"COMPRESSED", "NORMAL"}
                and feature.momentum_persistence in {"PERSISTENT_UP", "MIXED"}
                and feature.reference_state in {"MID_RANGE", "ABOVE_SESSION_OPEN", "NEAR_RECENT_HIGH"}
                and _strictness_ok(feature=feature, variant=variant)
            )
        if variant.family == "failed_countertrend_resumption":
            return (
                feature.pullback_state in {"MODERATE", "DEEP"}
                and feature.bar_anatomy in {"LOWER_REJECTION", "BULL_IMPULSE", "BALANCED"}
                and feature.mtf_agreement_state in {"COUNTERTREND_DOWN", "ALIGNED_UP", "MIXED"}
                and _strictness_ok(feature=feature, variant=variant)
            )
        return False

    if not feature_matches_side_context(side=variant.side, feature=feature):
        return False
    if variant.family == "pullback_continuation":
        return (
            feature.pullback_state in {"SHALLOW", "MODERATE", "DEEP"}
            and feature.bar_anatomy in {"UPPER_REJECTION", "BEAR_IMPULSE", "BALANCED"}
            and feature.reference_state in {"BELOW_SESSION_OPEN", "MID_RANGE", "NEAR_RECENT_LOW"}
            and _strictness_ok(feature=feature, variant=variant)
        )
    if variant.family == "breakout_continuation":
        return (
            feature.expansion_state in {"NORMAL", "EXPANDED"}
            and feature.reference_state in {"NEAR_RECENT_LOW", "BELOW_SESSION_OPEN", "MID_RANGE"}
            and feature.bar_anatomy in {"BEAR_IMPULSE", "BALANCED"}
            and _strictness_ok(feature=feature, variant=variant)
        )
    if variant.family == "pause_resume":
        return (
            feature.expansion_state in {"COMPRESSED", "NORMAL"}
            and feature.momentum_persistence in {"PERSISTENT_DOWN", "MIXED"}
            and feature.reference_state in {"MID_RANGE", "BELOW_SESSION_OPEN", "NEAR_RECENT_LOW"}
            and _strictness_ok(feature=feature, variant=variant)
        )
    if variant.family == "failed_countertrend_resumption":
        return (
            feature.pullback_state in {"MODERATE", "DEEP"}
            and feature.bar_anatomy in {"UPPER_REJECTION", "BEAR_IMPULSE", "BALANCED"}
            and feature.mtf_agreement_state in {"COUNTERTREND_UP", "ALIGNED_DOWN", "MIXED"}
            and _strictness_ok(feature=feature, variant=variant)
        )
    return False


def feature_matches_side_context(*, side: str, feature: FeatureState) -> bool:
    if side == "LONG":
        return (
            feature.trend_state in {"UP", "STRONG_UP", "FLAT"}
            and feature.mtf_agreement_state in {"ALIGNED_UP", "COUNTERTREND_DOWN", "MIXED"}
        )
    return (
        feature.trend_state in {"DOWN", "STRONG_DOWN", "FLAT"}
        and feature.mtf_agreement_state in {"ALIGNED_DOWN", "COUNTERTREND_UP", "MIXED"}
    )


def _strictness_ok(*, feature: FeatureState, variant: PatternVariant) -> bool:
    if variant.strictness == "active":
        return feature.direction_bias != "NEUTRAL" or feature.reference_state in {"MID_RANGE", "ABOVE_SESSION_OPEN", "BELOW_SESSION_OPEN"}
    if variant.strictness == "conservative":
        return (
            feature.regime_bucket in {"TREND_UP", "TREND_DOWN"}
            and feature.volatility_bucket != "HOT"
            and feature.direction_bias != "NEUTRAL"
        )
    if variant.strictness == "aggressive":
        return feature.volatility_bucket in {"NORMAL", "HOT"}
    return True


def _variant_description(*, family: str, side: str, strictness: str) -> str:
    side_text = "long" if side == "LONG" else "short"
    if strictness == "active":
        prefix = "active"
    else:
        prefix = strictness
    if family == "pullback_continuation":
        return f"{prefix} {side_text} continuation after a bounded pullback inside an already-established 5m trend."
    if family == "breakout_continuation":
        return f"{prefix} {side_text} continuation when a 5m breakout aligns with 1m follow-through."
    if family == "pause_resume":
        return f"{prefix} {side_text} participation when compression pauses a trend and 1m timing resumes it."
    return f"{prefix} {side_text} participation after a failed countertrend push returns to the primary trend."


def _variant_notes(*, family: str, strictness: str, profile: str, side: str) -> tuple[str, ...]:
    notes = [f"bounded_family={family}", f"strictness={strictness}"]
    if strictness == "active":
        notes.append("favours_participation_over_early_selectivity")
    if strictness == "conservative":
        notes.append("requires_clean_regime_alignment")
    if strictness == "aggressive":
        notes.append("tolerates_hot_volatility")
    if profile != "phase2_baseline":
        notes.append(f"profile={profile}")
    if profile in {"phase3_reclaim_band", "phase3_reentry_redesign", "phase3_full"}:
        notes.append("reclaim_band_entry_logic")
    if profile in {"phase3_reentry_redesign", "phase3_full"}:
        notes.append("reentry_redesign_enabled")
    if profile == "phase3_full" and side == "SHORT" and strictness in {"active", "aggressive"}:
        notes.append("short_trigger_reachability_repair")
    return tuple(notes)


def _setup_signature(*, feature: FeatureState, variant: PatternVariant) -> str:
    return "|".join(
        [
            variant.family,
            variant.side,
            feature.session_segment,
            feature.trend_state,
            feature.pullback_state,
            feature.expansion_state,
            feature.reference_state,
        ]
    )


def _setup_state_signature(*, feature: FeatureState) -> str:
    return "|".join(
        [
            feature.trend_state,
            feature.pullback_state,
            feature.expansion_state,
            feature.bar_anatomy,
            feature.reference_state,
            feature.mtf_agreement_state,
        ]
    )


def _setup_quality_score(*, feature: FeatureState, variant: PatternVariant) -> float:
    score = 0.0
    if variant.side == "LONG":
        if feature.mtf_agreement_state == "ALIGNED_UP":
            score += 1.0
        elif feature.mtf_agreement_state == "COUNTERTREND_DOWN":
            score += 0.6
        if feature.bar_anatomy in {"BULL_IMPULSE", "LOWER_REJECTION"}:
            score += 0.8
    else:
        if feature.mtf_agreement_state == "ALIGNED_DOWN":
            score += 1.0
        elif feature.mtf_agreement_state == "COUNTERTREND_UP":
            score += 0.6
        if feature.bar_anatomy in {"BEAR_IMPULSE", "UPPER_REJECTION"}:
            score += 0.8
    if feature.expansion_state == "EXPANDED":
        score += 0.4
    if feature.pullback_state == "MODERATE":
        score += 0.4
    return round(score, 3)


def _setup_quality_bucket(*, score: float) -> str:
    if score >= 2.0:
        return "HIGH"
    if score >= 1.4:
        return "MEDIUM"
    return "LOW"


def _decision_id(*, feature: FeatureState, variant: PatternVariant) -> str:
    return f"{feature.instrument}|{variant.variant_id}|{feature.decision_ts.isoformat()}"
