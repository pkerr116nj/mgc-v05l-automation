"""Replay-first Asia Drift v1 Phase 1 state machine."""

from __future__ import annotations

from collections import Counter
from typing import Sequence

from .features import (
    ASIA_DRIFT_LONG,
    ASIA_DRIFT_SHORT,
    DISQUALIFYING_PULLBACK,
    DRIFT_MEDIUM,
    DRIFT_STRONG,
    NO_PULLBACK,
    NO_TRADE,
    NORMAL_PULLBACK,
    STRETCHED_BUT_VALID,
    TOO_EXTENDED,
    get_calibration_profile,
)
from .models import AsiaDriftFeatureRow, AsiaDriftSessionSummary, AsiaDriftStateRow


STATE_NO_TRADE = "NO_TRADE"
STATE_DRIFT_LONG_CANDIDATE = "DRIFT_LONG_CANDIDATE"
STATE_DRIFT_SHORT_CANDIDATE = "DRIFT_SHORT_CANDIDATE"
STATE_PULLBACK_PENDING = "PULLBACK_PENDING"
STATE_ENTRY_ARMED = "ENTRY_ARMED"
STATE_IN_TRADE = "IN_TRADE"
STATE_DRIFT_AT_RISK = "DRIFT_AT_RISK"
STATE_RECOVERED_DRIFT = "RECOVERED_DRIFT"
STATE_REQUALIFIED_CANDIDATE = "REQUALIFIED_CANDIDATE"
STATE_CONFIRMED_INVALIDATION = "CONFIRMED_INVALIDATION"
STATE_THESIS_INVALIDATED = STATE_CONFIRMED_INVALIDATION
STATE_SESSION_TIMEOUT = "SESSION_TIMEOUT"

VALID_DRIFT_STATES = {
    STATE_DRIFT_LONG_CANDIDATE,
    STATE_DRIFT_SHORT_CANDIDATE,
    STATE_PULLBACK_PENDING,
    STATE_ENTRY_ARMED,
    STATE_RECOVERED_DRIFT,
    STATE_REQUALIFIED_CANDIDATE,
}


def evaluate_state_machine(feature_rows: Sequence[AsiaDriftFeatureRow]) -> list[AsiaDriftStateRow]:
    sorted_rows = sorted(feature_rows, key=lambda row: (row.instrument, row.decision_ts))
    states: list[AsiaDriftStateRow] = []
    previous_by_session: dict[str, AsiaDriftStateRow] = {}
    context_by_session: dict[str, dict[str, object]] = {}

    for row in sorted_rows:
        previous = previous_by_session.get(row.asia_drift_session_id)
        context = context_by_session.get(row.asia_drift_session_id, {})
        state, reason, at_risk_reason, bars_in_at_risk_state, resolution_tag, next_context = _derive_state(
            row=row,
            previous=previous,
            context=context,
        )
        drift_strength = row.long_drift_strength if row.regime == ASIA_DRIFT_LONG else row.short_drift_strength
        state_row = AsiaDriftStateRow(
            calibration_profile=row.calibration_profile,
            instrument=row.instrument,
            timeframe=row.timeframe,
            decision_ts=row.decision_ts,
            asia_drift_session_id=row.asia_drift_session_id,
            session_bar_index=row.session_bar_index,
            state=state,
            previous_state=previous.state if previous is not None else None,
            transition_reason=reason,
            at_risk_reason=at_risk_reason,
            bars_in_at_risk_state=bars_in_at_risk_state,
            resolution_tag=resolution_tag,
            regime=row.regime,
            dominant_direction=row.dominant_direction,
            drift_strength=drift_strength,
            regime_persistence_score=row.regime_persistence_score,
            pullback_state=row.pullback_state,
            entry_window_open=row.entry_window_open,
            in_scope=row.in_scope,
            thesis_invalidated=state == STATE_CONFIRMED_INVALIDATION,
            hypothetical_entry_ready=row.hypothetical_entry_ready,
        )
        previous_by_session[row.asia_drift_session_id] = state_row
        context_by_session[row.asia_drift_session_id] = next_context
        states.append(state_row)
    return states


def summarize_sessions(
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[AsiaDriftStateRow],
) -> list[AsiaDriftSessionSummary]:
    features_by_session: dict[str, list[AsiaDriftFeatureRow]] = {}
    states_by_session: dict[str, list[AsiaDriftStateRow]] = {}
    for row in feature_rows:
        features_by_session.setdefault(row.asia_drift_session_id, []).append(row)
    for row in state_rows:
        states_by_session.setdefault(row.asia_drift_session_id, []).append(row)

    summaries: list[AsiaDriftSessionSummary] = []
    for session_id in sorted(features_by_session):
        session_features = features_by_session[session_id]
        in_scope_features = [row for row in session_features if row.in_scope]
        summary_features = in_scope_features or session_features
        session_states = states_by_session.get(session_id, [])
        first = session_features[0]
        state_counts = Counter(row.state for row in session_states)
        regime_counts = Counter(row.regime for row in summary_features)
        pullback_counts = Counter(row.pullback_state for row in summary_features if row.regime != NO_TRADE)
        promising_session = state_counts[STATE_ENTRY_ARMED] > 0 or any(
            row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}
            and (row.long_drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG} or row.short_drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG})
            for row in summary_features
        )
        summaries.append(
            AsiaDriftSessionSummary(
                instrument=first.instrument,
                calibration_profile=first.calibration_profile,
                timeframe=first.timeframe,
                asia_drift_session_id=session_id,
                local_session_date=first.local_session_date,
                bar_count=len(session_features),
                in_scope_bar_count=len(in_scope_features),
                candidate_direction=_candidate_direction(summary_features),
                max_long_drift_score=max(row.long_drift_score for row in summary_features),
                max_short_drift_score=max(row.short_drift_score for row in summary_features),
                max_drift_strength=_max_strength(summary_features),
                entry_ready_bar_count=sum(1 for row in summary_features if row.hypothetical_entry_ready),
                at_risk_bar_count=state_counts[STATE_DRIFT_AT_RISK],
                recovered_bar_count=state_counts[STATE_RECOVERED_DRIFT],
                requalified_bar_count=state_counts[STATE_REQUALIFIED_CANDIDATE],
                invalidated_bar_count=state_counts[STATE_CONFIRMED_INVALIDATION],
                state_counts=dict(state_counts),
                regime_counts=dict(regime_counts),
                pullback_counts=dict(pullback_counts),
                chop_veto_bar_count=sum(1 for row in summary_features if row.chop_veto),
                post_spike_bar_count=sum(1 for row in summary_features if row.post_spike_instability),
                promising_session=promising_session,
                first_candidate_ts=next((row.decision_ts for row in summary_features if row.regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}), None),
                first_entry_ready_ts=next((row.decision_ts for row in summary_features if row.hypothetical_entry_ready), None),
                first_invalidated_ts=next((row.decision_ts for row in session_states if row.state == STATE_CONFIRMED_INVALIDATION), None),
            )
        )
    return summaries


def _derive_state(
    *,
    row: AsiaDriftFeatureRow,
    previous: AsiaDriftStateRow | None,
    context: dict[str, object],
) -> tuple[str, str, str | None, int, str | None, dict[str, object]]:
    profile = get_calibration_profile(row.calibration_profile)
    if not row.in_scope:
        if previous is not None and previous.state not in {STATE_NO_TRADE, STATE_SESSION_TIMEOUT}:
            return STATE_SESSION_TIMEOUT, "left_derived_session_scope", None, 0, None, {}
        return STATE_NO_TRADE, "out_of_scope", None, 0, None, {}
    if row.session_timeout:
        return STATE_SESSION_TIMEOUT, "mandatory_handoff_exit_boundary", None, 0, None, {}
    if not row.anchor_observed:
        return STATE_NO_TRADE, "session_anchor_not_observed", None, 0, None, {}
    if row.session_bar_index < 8:
        return STATE_NO_TRADE, "warmup_incomplete", None, 0, None, {}

    base_state, base_reason = _base_state(row)
    had_live_drift = previous is not None and previous.state in VALID_DRIFT_STATES | {STATE_DRIFT_AT_RISK}
    failure_reason, failure_category = _failure_signal(
        row=row,
        previous=previous,
        had_live_drift=had_live_drift,
        profile=profile,
    )
    if previous is not None and previous.state == STATE_DRIFT_AT_RISK:
        recovery_state = _recovery_resolution(
            row=row,
            base_state=base_state,
            previous=previous,
            profile=profile,
            failure_reason=failure_reason,
        )
        if recovery_state is not None:
            state, reason, resolution_tag = recovery_state
            return state, reason, None, 0, resolution_tag, {}
        if failure_reason is None and previous.bars_in_at_risk_state < profile.max_recovery_bars:
            return (
                STATE_DRIFT_AT_RISK,
                previous.at_risk_reason or "awaiting_recovery_confirmation",
                previous.at_risk_reason or "awaiting_recovery_confirmation",
                previous.bars_in_at_risk_state + 1,
                None,
                context,
            )
    if failure_reason is None:
        return base_state, base_reason, None, 0, None, {}

    streak = 1
    if context.get("failure_category") == failure_category:
        streak = int(context.get("failure_streak", 0)) + 1
    confirmation_bars = _confirmation_bars(profile=profile, category=failure_category)
    if failure_category == "VWAP_TAG":
        confirmation_bars = 99
    if failure_category == "DRIFT_COLLAPSE" and row.regime_persistence_score >= profile.at_risk_persistence_threshold:
        confirmation_bars = max(confirmation_bars, 99)

    next_context: dict[str, object] = {
        "failure_reason": failure_reason,
        "failure_category": failure_category,
        "failure_streak": streak,
    }
    if streak >= confirmation_bars:
        return STATE_CONFIRMED_INVALIDATION, failure_reason, failure_reason, streak, "INVALIDATED", next_context
    return STATE_DRIFT_AT_RISK, failure_reason, failure_reason, streak, None, next_context


def _base_state(row: AsiaDriftFeatureRow) -> tuple[str, str]:
    if row.regime == NO_TRADE:
        return STATE_NO_TRADE, "session_filter_not_tradable"
    if row.regime == ASIA_DRIFT_LONG:
        if row.pullback_state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID} and row.entry_window_open:
            return STATE_ENTRY_ARMED, "valid_long_pullback_inside_entry_window"
        if row.pullback_state == TOO_EXTENDED or not row.entry_window_open:
            return STATE_PULLBACK_PENDING, "long_drift_waiting_for_better_reset"
        if row.pullback_state == NO_PULLBACK:
            return STATE_DRIFT_LONG_CANDIDATE, "long_drift_detected_no_pullback_yet"
        return STATE_DRIFT_LONG_CANDIDATE, "long_drift_detected"
    if row.regime == ASIA_DRIFT_SHORT:
        if row.pullback_state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID} and row.entry_window_open:
            return STATE_ENTRY_ARMED, "valid_short_pullback_inside_entry_window"
        if row.pullback_state == TOO_EXTENDED or not row.entry_window_open:
            return STATE_PULLBACK_PENDING, "short_drift_waiting_for_better_reset"
        if row.pullback_state == NO_PULLBACK:
            return STATE_DRIFT_SHORT_CANDIDATE, "short_drift_detected_no_pullback_yet"
        return STATE_DRIFT_SHORT_CANDIDATE, "short_drift_detected"
    return STATE_NO_TRADE, "unclassified"


def _failure_signal(
    *,
    row: AsiaDriftFeatureRow,
    previous: AsiaDriftStateRow | None,
    had_live_drift: bool,
    profile,
) -> tuple[str | None, str | None]:
    if not had_live_drift:
        return None, None
    if row.regime == NO_TRADE:
        if profile.regime_loss_confirmation_bars <= 1 or row.regime_persistence_score < profile.at_risk_persistence_threshold:
            return "regime_lost_after_candidate", "DRIFT_COLLAPSE"
        return "regime_loss_warning", "DRIFT_COLLAPSE"
    if row.pullback_structure_break:
        if row.pullback_reason == "confirmed_vwap_reclaim":
            return "confirmed_vwap_reclaim", "VWAP_RECLAIM"
        if row.pullback_reason == "vwap_and_slow_ema_failure":
            return "vwap_and_slow_ema_failure", "EMA_FAILURE"
        if row.pullback_reason == "protected_swing_break":
            return "protected_swing_break", "STRUCTURE_DAMAGE"
    if row.pullback_state == DISQUALIFYING_PULLBACK or row.thesis_invalidated_flag:
        category = row.pullback_veto_category or "OTHER"
        return row.pullback_reason or "pullback_disqualified", category
    if row.pullback_vwap_interaction == "VWAP_TAG" and profile.vwap_reclaim_confirmation_bars > 1:
        return "vwap_tag_warning", "VWAP_TAG"
    if row.pullback_vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP":
        return "single_close_through_vwap", "VWAP_RECLAIM"
    if row.pullback_warning_flag and profile.disqualifying_confirmation_bars > 1:
        return row.pullback_warning_reason or "soft_pullback_warning", row.pullback_warning_category or "OTHER"
    if row.regime_persistence_score < profile.at_risk_persistence_threshold and profile.regime_loss_confirmation_bars > 1:
        return "drift_persistence_degraded", "DRIFT_COLLAPSE"
    return None, None


def _recovery_resolution(
    *,
    row: AsiaDriftFeatureRow,
    base_state: str,
    previous: AsiaDriftStateRow,
    profile,
    failure_reason: str | None,
) -> tuple[str, str, str] | None:
    if failure_reason is not None:
        return None
    if previous.bars_in_at_risk_state > profile.max_recovery_bars:
        return STATE_CONFIRMED_INVALIDATION, "recovery_window_expired", "INVALIDATED"
    if row.recovery_score < profile.recovery_score_threshold:
        return None
    if base_state == STATE_ENTRY_ARMED and row.recovery_score >= profile.requalification_score_threshold:
        return STATE_REQUALIFIED_CANDIDATE, "requalified_after_at_risk_recovery", "REQUALIFIED"
    if base_state in VALID_DRIFT_STATES:
        return STATE_RECOVERED_DRIFT, "drift_recovered_after_at_risk", "RECOVERED"
    return None


def _confirmation_bars(*, profile, category: str | None) -> int:
    if category == "VWAP_RECLAIM":
        return profile.vwap_reclaim_confirmation_bars
    if category == "EMA_FAILURE":
        return profile.ema_failure_confirmation_bars
    if category == "STRUCTURE_DAMAGE":
        return profile.structure_break_confirmation_bars
    if category == "DRIFT_COLLAPSE":
        return max(profile.regime_loss_confirmation_bars, profile.drift_collapse_confirmation_bars)
    if category in {"DEPTH", "SPEED", "EXPANSION", "VWAP_INTERACTION", "OTHER"}:
        return profile.disqualifying_confirmation_bars
    return profile.disqualifying_confirmation_bars


def _candidate_direction(rows: Sequence[AsiaDriftFeatureRow]) -> str:
    max_long = max(row.long_drift_score for row in rows)
    max_short = max(row.short_drift_score for row in rows)
    if max_long == max_short:
        return "MIXED"
    return "LONG" if max_long > max_short else "SHORT"


def _max_strength(rows: Sequence[AsiaDriftFeatureRow]) -> str:
    strengths = {row.long_drift_strength for row in rows} | {row.short_drift_strength for row in rows}
    if DRIFT_STRONG in strengths:
        return DRIFT_STRONG
    if DRIFT_MEDIUM in strengths:
        return DRIFT_MEDIUM
    return max(strengths) if strengths else "NONE"
