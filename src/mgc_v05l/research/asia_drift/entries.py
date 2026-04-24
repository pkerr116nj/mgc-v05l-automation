"""Replay-only entry setup assembly and bounded entry-model evaluation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from .features import (
    ASIA_DRIFT_LONG,
    ASIA_DRIFT_SHORT,
    DISQUALIFYING_PULLBACK,
    DRIFT_MEDIUM,
    DRIFT_STRONG,
    FAST_SHALLOW_VALID,
    FAST_STRETCHED_WARNING,
)
from .models import AsiaDriftEntryEvaluation, AsiaDriftEntrySetup, AsiaDriftFeatureRow, AsiaDriftStateRow
from .session_scope import derive_session_scope
from .state_machine import STATE_CONFIRMED_INVALIDATION, STATE_ENTRY_ARMED, STATE_REQUALIFIED_CANDIDATE, STATE_THESIS_INVALIDATED


ENTRY_MODEL_LIMIT_PULLBACK = "LIMIT_PULLBACK"
ENTRY_MODEL_LIMIT_LESS_PASSIVE = "LIMIT_LESS_PASSIVE"
ENTRY_MODEL_CONFIRMATION_REACCEL = "CONFIRMATION_REACCEL"
ENTRY_MODEL_SHALLOW_PARTICIPATION = "SHALLOW_PARTICIPATION"
ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED = "SHALLOW_PARTICIPATION_REFINED"

ENTRY_STATUS_ENTERED = "ENTERED"
ENTRY_STATUS_CANCELLED = "CANCELLED"
ENTRY_STATUS_EXPIRED = "EXPIRED"
ENTRY_STATUS_REJECTED = "REJECTED"

PREFILL_PROFILE_RECOVERY_CONFIRMED = "recovery_confirmed"
PREFILL_PROFILE_PERSISTENCE_SHORT = "prefill_persistence_short"
PREFILL_PROFILE_PERSISTENCE_MEDIUM = "prefill_persistence_medium"
PREFILL_PROFILE_LOOSE_DIAGNOSTIC = "loose_diagnostic"


@dataclass(frozen=True)
class RefinedPrefillProfile:
    name: str
    regime_warning_bars: int
    extra_evaluation_bars: int
    recovery_score_threshold: float
    persistence_score_threshold: float
    warning_recovery_bars: int


REFINED_PREFILL_PROFILES: dict[str, RefinedPrefillProfile] = {
    PREFILL_PROFILE_RECOVERY_CONFIRMED: RefinedPrefillProfile(
        name=PREFILL_PROFILE_RECOVERY_CONFIRMED,
        regime_warning_bars=0,
        extra_evaluation_bars=0,
        recovery_score_threshold=0.60,
        persistence_score_threshold=0.52,
        warning_recovery_bars=0,
    ),
    PREFILL_PROFILE_PERSISTENCE_SHORT: RefinedPrefillProfile(
        name=PREFILL_PROFILE_PERSISTENCE_SHORT,
        regime_warning_bars=1,
        extra_evaluation_bars=1,
        recovery_score_threshold=0.58,
        persistence_score_threshold=0.48,
        warning_recovery_bars=1,
    ),
    PREFILL_PROFILE_PERSISTENCE_MEDIUM: RefinedPrefillProfile(
        name=PREFILL_PROFILE_PERSISTENCE_MEDIUM,
        regime_warning_bars=2,
        extra_evaluation_bars=2,
        recovery_score_threshold=0.56,
        persistence_score_threshold=0.45,
        warning_recovery_bars=2,
    ),
    PREFILL_PROFILE_LOOSE_DIAGNOSTIC: RefinedPrefillProfile(
        name=PREFILL_PROFILE_LOOSE_DIAGNOSTIC,
        regime_warning_bars=3,
        extra_evaluation_bars=3,
        recovery_score_threshold=0.52,
        persistence_score_threshold=0.40,
        warning_recovery_bars=3,
    ),
}


def assemble_entry_setups(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[AsiaDriftStateRow],
) -> list[AsiaDriftEntrySetup]:
    features_by_session = _features_by_session(feature_rows)
    state_by_key = _state_by_key(state_rows)
    setups: list[AsiaDriftEntrySetup] = []

    for session_id, session_rows in sorted(features_by_session.items()):
        invalidated_seen = False
        setup_index = 0
        for index, row in enumerate(session_rows):
            state = state_by_key.get((session_id, row.decision_ts))
            if state is not None and state.state == STATE_CONFIRMED_INVALIDATION:
                invalidated_seen = True
            if invalidated_seen:
                continue
            if state is None or state.state not in {STATE_ENTRY_ARMED, STATE_REQUALIFIED_CANDIDATE}:
                continue
            if not row.hypothetical_entry_ready:
                continue
            if index > 0:
                previous_state = state_by_key.get((session_id, session_rows[index - 1].decision_ts))
                if previous_state is not None and previous_state.state == state.state:
                    continue
            setup_index += 1
            if state.state == STATE_REQUALIFIED_CANDIDATE and state.transition_reason.startswith("requalified"):
                armed_reason = state.transition_reason
            else:
                armed_reason = state.transition_reason
            direction = "LONG" if row.regime == ASIA_DRIFT_LONG else "SHORT"
            drift_strength = _drift_strength_for_row(row)
            zone_low, zone_high, limit_price, zone_valid = _build_limit_zone(row)
            scope = derive_session_scope(
                instrument=row.instrument,
                timeframe=row.timeframe,
                bar_end_ts=row.decision_ts,
                anchor_observed=row.anchor_observed,
            )
            setups.append(
                AsiaDriftEntrySetup(
                    setup_id=f"{session_id}__setup_{setup_index}",
                    calibration_profile=row.calibration_profile,
                    instrument=row.instrument,
                    timeframe=row.timeframe,
                    asia_drift_session_id=session_id,
                    local_session_date=row.local_session_date,
                    direction=direction,
                    regime=row.regime,
                    drift_strength=drift_strength,
                    armed_ts=row.decision_ts,
                    armed_subphase=row.subphase,
                    armed_session_bar_index=row.session_bar_index,
                    armed_transition_reason=armed_reason,
                    limit_expiry_ts=_future_ts(session_rows, index, bars_forward=3),
                    confirmation_expiry_ts=_future_ts(session_rows, index, bars_forward=5),
                    session_timeout_ts=scope.mandatory_exit_ts,
                    latest_entry_ts=scope.latest_entry_ts,
                    entry_zone_low=zone_low,
                    entry_zone_high=zone_high,
                    entry_limit_price=limit_price,
                    entry_zone_valid=zone_valid,
                    pullback_pivot_price=row.high if direction == "LONG" else row.low,
                    protected_swing_price=row.protected_swing_price,
                    drift_leg_extreme=row.drift_leg_extreme,
                    atr=row.atr,
                    scope_provenance=row.scope_provenance,
                    feature_version=row.feature_version,
                )
            )
    return setups


def estimate_stop_and_risk(*, setup: AsiaDriftEntrySetup, entry_price: float) -> tuple[float, float]:
    atr = max(setup.atr, 1e-9)
    buffer_points = 0.08 * atr
    if setup.direction == "LONG":
        raw_stop = min(setup.protected_swing_price, setup.entry_zone_low, entry_price) - buffer_points
        max_stop = entry_price - 0.45 * atr
        min_stop = entry_price - 1.25 * atr
        stop_price = min(max(raw_stop, min_stop), max_stop)
        risk_points = max(entry_price - stop_price, 0.45 * atr)
    else:
        raw_stop = max(setup.protected_swing_price, setup.entry_zone_high, entry_price) + buffer_points
        min_stop = entry_price + 0.45 * atr
        max_stop = entry_price + 1.25 * atr
        stop_price = max(min(raw_stop, max_stop), min_stop)
        risk_points = max(stop_price - entry_price, 0.45 * atr)
    return stop_price, risk_points


def evaluate_entry_models(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    state_rows: Sequence[AsiaDriftStateRow],
    setups: Sequence[AsiaDriftEntrySetup],
    refined_prefill_profile_name: str = PREFILL_PROFILE_RECOVERY_CONFIRMED,
) -> list[AsiaDriftEntryEvaluation]:
    features_by_session = _features_by_session(feature_rows)
    state_by_key = _state_by_key(state_rows)
    evaluations: list[AsiaDriftEntryEvaluation] = []
    refined_prefill_profile = get_refined_prefill_profile(refined_prefill_profile_name)

    for setup in setups:
        session_rows = features_by_session.get(setup.asia_drift_session_id, [])
        armed_index = next(
            (index for index, row in enumerate(session_rows) if row.decision_ts == setup.armed_ts),
            None,
        )
        if armed_index is None:
            continue
        evaluations.append(
            _evaluate_limit_pullback(
                setup=setup,
                session_rows=session_rows,
                armed_index=armed_index,
                state_by_key=state_by_key,
            )
        )
        evaluations.append(
            _evaluate_confirmation_reaccel(
                setup=setup,
                session_rows=session_rows,
                armed_index=armed_index,
                state_by_key=state_by_key,
            )
        )
        evaluations.append(
            _evaluate_less_passive_limit(
                setup=setup,
                session_rows=session_rows,
                armed_index=armed_index,
                state_by_key=state_by_key,
            )
        )
        evaluations.append(
            _evaluate_shallow_participation(
                setup=setup,
                session_rows=session_rows,
                armed_index=armed_index,
                state_by_key=state_by_key,
            )
        )
        evaluations.append(
            _evaluate_refined_shallow_participation(
                setup=setup,
                session_rows=session_rows,
                armed_index=armed_index,
                state_by_key=state_by_key,
                prefill_profile=refined_prefill_profile,
            )
        )
    return evaluations


def get_refined_prefill_profile(name: str) -> RefinedPrefillProfile:
    try:
        return REFINED_PREFILL_PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown Asia Drift refined prefill profile: {name}") from exc


def _evaluate_limit_pullback(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    armed_index: int,
    state_by_key: dict[tuple[str, datetime], AsiaDriftStateRow],
) -> AsiaDriftEntryEvaluation:
    tags = ["state_entry_armed", "limit_zone_model"]
    prerequisites_met = setup.entry_zone_valid and setup.drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG}
    if not setup.entry_zone_valid:
        tags.append("empty_entry_zone")
        return _rejected_evaluation(
            setup=setup,
            model=ENTRY_MODEL_LIMIT_PULLBACK,
            reason="empty_entry_zone",
            prerequisites_met=False,
            tags=tuple(tags),
        )
    if setup.drift_strength not in {DRIFT_MEDIUM, DRIFT_STRONG}:
        tags.append("drift_below_medium")
        return _rejected_evaluation(
            setup=setup,
            model=ENTRY_MODEL_LIMIT_PULLBACK,
            reason="drift_below_medium",
            prerequisites_met=False,
            tags=tuple(tags),
        )

    candidate_price = setup.entry_limit_price
    assert candidate_price is not None
    evaluation_rows = list(session_rows[armed_index + 1 : armed_index + 4])
    if not evaluation_rows:
        tags.append("no_future_bars")
        return _expired_evaluation(
            setup=setup,
            model=ENTRY_MODEL_LIMIT_PULLBACK,
            reason="no_future_bars",
            prerequisites_met=prerequisites_met,
            candidate_price=candidate_price,
            tags=tuple(tags),
            reference_index=armed_index,
            session_rows=session_rows,
        )

    for waited_bars, row in enumerate(evaluation_rows, start=1):
        state = state_by_key.get((setup.asia_drift_session_id, row.decision_ts))
        cancel_reason = _entry_cancel_reason(setup=setup, row=row, state=state)
        if cancel_reason is not None:
            tags.append(cancel_reason)
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_LIMIT_PULLBACK,
                reason=cancel_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=candidate_price,
                tags=tuple(tags),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + waited_bars,
                session_rows=session_rows,
            )
        if _limit_fill_allowed(setup=setup, row=row):
            tags.extend(("limit_fill_on_close_inside_zone", "no_chase_enforced"))
            return AsiaDriftEntryEvaluation(
                evaluation_id=f"{setup.setup_id}__{ENTRY_MODEL_LIMIT_PULLBACK.lower()}",
                setup_id=setup.setup_id,
                calibration_profile=setup.calibration_profile,
                instrument=setup.instrument,
                asia_drift_session_id=setup.asia_drift_session_id,
                entry_model=ENTRY_MODEL_LIMIT_PULLBACK,
                direction=setup.direction,
                status=ENTRY_STATUS_ENTERED,
                accepted=True,
                prerequisites_met=prerequisites_met,
                armed_ts=setup.armed_ts,
                evaluation_start_ts=evaluation_rows[0].decision_ts,
                evaluation_end_ts=row.decision_ts,
                entry_ts=row.decision_ts,
                entry_price=row.close,
                candidate_entry_price=candidate_price,
                bars_waited=waited_bars,
                cancellation_reason=None,
                reason_tags=tuple(tags),
                continuation_reasserted_after_reject=False,
                missed_favorable_excursion_points=0.0,
                missed_favorable_excursion_r=0.0,
                scope_provenance=setup.scope_provenance,
                feature_version=setup.feature_version,
            )

    tags.append("limit_window_expired")
    return _expired_evaluation(
        setup=setup,
        model=ENTRY_MODEL_LIMIT_PULLBACK,
        reason="limit_window_expired",
        prerequisites_met=prerequisites_met,
        candidate_price=candidate_price,
        tags=tuple(tags),
        reference_index=min(armed_index + 3, len(session_rows) - 1),
        session_rows=session_rows,
        evaluation_start_ts=evaluation_rows[0].decision_ts,
        evaluation_end_ts=evaluation_rows[-1].decision_ts,
        bars_waited=len(evaluation_rows),
    )


def _evaluate_confirmation_reaccel(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    armed_index: int,
    state_by_key: dict[tuple[str, datetime], AsiaDriftStateRow],
) -> AsiaDriftEntryEvaluation:
    tags = ["state_entry_armed", "confirmation_reaccel_model", "post_limit_window_only"]
    prerequisites_met = setup.entry_zone_valid and setup.drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG}
    if not setup.entry_zone_valid:
        tags.append("empty_entry_zone")
        return _rejected_evaluation(
            setup=setup,
            model=ENTRY_MODEL_CONFIRMATION_REACCEL,
            reason="empty_entry_zone",
            prerequisites_met=False,
            tags=tuple(tags),
        )
    if setup.drift_strength not in {DRIFT_MEDIUM, DRIFT_STRONG}:
        tags.append("drift_below_medium")
        return _rejected_evaluation(
            setup=setup,
            model=ENTRY_MODEL_CONFIRMATION_REACCEL,
            reason="drift_below_medium",
            prerequisites_met=False,
            tags=tuple(tags),
        )

    evaluation_rows = list(session_rows[armed_index + 4 : armed_index + 6])
    if not evaluation_rows:
        tags.append("no_confirmation_window")
        return _expired_evaluation(
            setup=setup,
            model=ENTRY_MODEL_CONFIRMATION_REACCEL,
            reason="no_confirmation_window",
            prerequisites_met=prerequisites_met,
            candidate_price=None,
            tags=tuple(tags),
            reference_index=min(armed_index + 3, len(session_rows) - 1),
            session_rows=session_rows,
        )

    for waited_bars, row in enumerate(evaluation_rows, start=1):
        state = state_by_key.get((setup.asia_drift_session_id, row.decision_ts))
        cancel_reason = _entry_cancel_reason(
            setup=setup,
            row=row,
            state=state,
            allow_reacceleration_break=True,
        )
        if cancel_reason is not None:
            tags.append(cancel_reason)
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_CONFIRMATION_REACCEL,
                reason=cancel_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=None,
                tags=tuple(tags),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + 3 + waited_bars,
                session_rows=session_rows,
            )
        prior_rows = session_rows[max(armed_index + 1, 0) : armed_index + 3 + waited_bars]
        pivot_price = _pivot_price(direction=setup.direction, rows=prior_rows, default=setup.pullback_pivot_price)
        if _confirmation_triggered(setup=setup, row=row, prior_row=session_rows[armed_index + 3 + waited_bars - 1], pivot_price=pivot_price):
            tags.extend(("confirmation_close_through_pivot", "vwap_displacement_improved", "no_chase_enforced"))
            return AsiaDriftEntryEvaluation(
                evaluation_id=f"{setup.setup_id}__{ENTRY_MODEL_CONFIRMATION_REACCEL.lower()}",
                setup_id=setup.setup_id,
                calibration_profile=setup.calibration_profile,
                instrument=setup.instrument,
                asia_drift_session_id=setup.asia_drift_session_id,
                entry_model=ENTRY_MODEL_CONFIRMATION_REACCEL,
                direction=setup.direction,
                status=ENTRY_STATUS_ENTERED,
                accepted=True,
                prerequisites_met=prerequisites_met,
                armed_ts=setup.armed_ts,
                evaluation_start_ts=evaluation_rows[0].decision_ts,
                evaluation_end_ts=row.decision_ts,
                entry_ts=row.decision_ts,
                entry_price=row.close,
                candidate_entry_price=pivot_price,
                bars_waited=waited_bars,
                cancellation_reason=None,
                reason_tags=tuple(tags),
                continuation_reasserted_after_reject=False,
                missed_favorable_excursion_points=0.0,
                missed_favorable_excursion_r=0.0,
                scope_provenance=setup.scope_provenance,
                feature_version=setup.feature_version,
            )

    tags.append("confirmation_not_observed")
    return _expired_evaluation(
        setup=setup,
        model=ENTRY_MODEL_CONFIRMATION_REACCEL,
        reason="confirmation_not_observed",
        prerequisites_met=prerequisites_met,
        candidate_price=None,
        tags=tuple(tags),
        reference_index=min(armed_index + 5, len(session_rows) - 1),
        session_rows=session_rows,
        evaluation_start_ts=evaluation_rows[0].decision_ts,
        evaluation_end_ts=evaluation_rows[-1].decision_ts,
        bars_waited=len(evaluation_rows),
    )


def _evaluate_less_passive_limit(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    armed_index: int,
    state_by_key: dict[tuple[str, datetime], AsiaDriftStateRow],
) -> AsiaDriftEntryEvaluation:
    tags = ["state_entry_armed", "less_passive_limit_model", "touch_fill_diagnostic"]
    prerequisites_met = setup.entry_zone_valid and setup.drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG}
    candidate_price = _less_passive_limit_price(setup)
    evaluation_rows = list(session_rows[armed_index + 1 : armed_index + 4])
    if not evaluation_rows:
        return _expired_evaluation(
            setup=setup,
            model=ENTRY_MODEL_LIMIT_LESS_PASSIVE,
            reason="no_future_bars",
            prerequisites_met=prerequisites_met,
            candidate_price=candidate_price,
            tags=tuple(tags + ["no_future_bars"]),
            reference_index=armed_index,
            session_rows=session_rows,
        )
    for waited_bars, row in enumerate(evaluation_rows, start=1):
        state = state_by_key.get((setup.asia_drift_session_id, row.decision_ts))
        cancel_reason = _entry_cancel_reason(setup=setup, row=row, state=state)
        if cancel_reason is not None:
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_LIMIT_LESS_PASSIVE,
                reason=cancel_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=candidate_price,
                tags=tuple(tags + [cancel_reason]),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + waited_bars,
                session_rows=session_rows,
            )
        if _touch_limit_fill_allowed(direction=setup.direction, candidate_price=candidate_price, row=row):
            return AsiaDriftEntryEvaluation(
                evaluation_id=f"{setup.setup_id}__{ENTRY_MODEL_LIMIT_LESS_PASSIVE.lower()}",
                setup_id=setup.setup_id,
                calibration_profile=setup.calibration_profile,
                instrument=setup.instrument,
                asia_drift_session_id=setup.asia_drift_session_id,
                entry_model=ENTRY_MODEL_LIMIT_LESS_PASSIVE,
                direction=setup.direction,
                status=ENTRY_STATUS_ENTERED,
                accepted=True,
                prerequisites_met=prerequisites_met,
                armed_ts=setup.armed_ts,
                evaluation_start_ts=evaluation_rows[0].decision_ts,
                evaluation_end_ts=row.decision_ts,
                entry_ts=row.decision_ts,
                entry_price=candidate_price,
                candidate_entry_price=candidate_price,
                bars_waited=waited_bars,
                cancellation_reason=None,
                reason_tags=tuple(tags + ["less_passive_touch_fill"]),
                continuation_reasserted_after_reject=False,
                missed_favorable_excursion_points=0.0,
                missed_favorable_excursion_r=0.0,
                scope_provenance=setup.scope_provenance,
                feature_version=setup.feature_version,
            )
    return _expired_evaluation(
        setup=setup,
        model=ENTRY_MODEL_LIMIT_LESS_PASSIVE,
        reason="limit_window_expired",
        prerequisites_met=prerequisites_met,
        candidate_price=candidate_price,
        tags=tuple(tags + ["limit_window_expired"]),
        reference_index=min(armed_index + 3, len(session_rows) - 1),
        session_rows=session_rows,
        evaluation_start_ts=evaluation_rows[0].decision_ts,
        evaluation_end_ts=evaluation_rows[-1].decision_ts,
        bars_waited=len(evaluation_rows),
    )


def _evaluate_shallow_participation(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    armed_index: int,
    state_by_key: dict[tuple[str, datetime], AsiaDriftStateRow],
) -> AsiaDriftEntryEvaluation:
    tags = ["state_entry_armed", "shallow_participation_model", "diagnostic_only_geometry"]
    prerequisites_met = setup.entry_zone_valid and setup.drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG}
    candidate_price = _shallow_participation_price(setup)
    evaluation_rows = list(session_rows[armed_index + 1 : armed_index + 4])
    if not evaluation_rows:
        return _expired_evaluation(
            setup=setup,
            model=ENTRY_MODEL_SHALLOW_PARTICIPATION,
            reason="no_future_bars",
            prerequisites_met=prerequisites_met,
            candidate_price=candidate_price,
            tags=tuple(tags + ["no_future_bars"]),
            reference_index=armed_index,
            session_rows=session_rows,
        )
    for waited_bars, row in enumerate(evaluation_rows, start=1):
        state = state_by_key.get((setup.asia_drift_session_id, row.decision_ts))
        cancel_reason = _entry_cancel_reason(
            setup=setup,
            row=row,
            state=state,
            allow_reacceleration_break=True,
        )
        if cancel_reason is not None:
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_SHALLOW_PARTICIPATION,
                reason=cancel_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=candidate_price,
                tags=tuple(tags + [cancel_reason]),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + waited_bars,
                session_rows=session_rows,
            )
        prior_row = session_rows[max(armed_index, armed_index + waited_bars - 1)]
        if _shallow_participation_triggered(setup=setup, row=row, prior_row=prior_row):
            return AsiaDriftEntryEvaluation(
                evaluation_id=f"{setup.setup_id}__{ENTRY_MODEL_SHALLOW_PARTICIPATION.lower()}",
                setup_id=setup.setup_id,
                calibration_profile=setup.calibration_profile,
                instrument=setup.instrument,
                asia_drift_session_id=setup.asia_drift_session_id,
                entry_model=ENTRY_MODEL_SHALLOW_PARTICIPATION,
                direction=setup.direction,
                status=ENTRY_STATUS_ENTERED,
                accepted=True,
                prerequisites_met=prerequisites_met,
                armed_ts=setup.armed_ts,
                evaluation_start_ts=evaluation_rows[0].decision_ts,
                evaluation_end_ts=row.decision_ts,
                entry_ts=row.decision_ts,
                entry_price=min(row.close, candidate_price) if setup.direction == "LONG" else max(row.close, candidate_price),
                candidate_entry_price=candidate_price,
                bars_waited=waited_bars,
                cancellation_reason=None,
                reason_tags=tuple(tags + ["shallow_pullback_acceptance"]),
                continuation_reasserted_after_reject=False,
                missed_favorable_excursion_points=0.0,
                missed_favorable_excursion_r=0.0,
                scope_provenance=setup.scope_provenance,
                feature_version=setup.feature_version,
            )
    return _expired_evaluation(
        setup=setup,
        model=ENTRY_MODEL_SHALLOW_PARTICIPATION,
        reason="shallow_participation_not_observed",
        prerequisites_met=prerequisites_met,
        candidate_price=candidate_price,
        tags=tuple(tags + ["shallow_participation_not_observed"]),
        reference_index=min(armed_index + 3, len(session_rows) - 1),
        session_rows=session_rows,
        evaluation_start_ts=evaluation_rows[0].decision_ts,
        evaluation_end_ts=evaluation_rows[-1].decision_ts,
        bars_waited=len(evaluation_rows),
    )


def _evaluate_refined_shallow_participation(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    armed_index: int,
    state_by_key: dict[tuple[str, datetime], AsiaDriftStateRow],
    prefill_profile: RefinedPrefillProfile,
) -> AsiaDriftEntryEvaluation:
    tags = [
        "state_entry_armed",
        "shallow_participation_model",
        "fast_pullback_refined_geometry",
        f"prefill_profile:{prefill_profile.name}",
    ]
    prerequisites_met = setup.entry_zone_valid and setup.drift_strength in {DRIFT_MEDIUM, DRIFT_STRONG}
    candidate_price = _refined_shallow_participation_price(setup)
    evaluation_rows = list(session_rows[armed_index + 1 : armed_index + 4 + prefill_profile.extra_evaluation_bars])
    if not evaluation_rows:
        return _expired_evaluation(
            setup=setup,
            model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
            reason="no_future_bars",
            prerequisites_met=prerequisites_met,
            candidate_price=candidate_price,
            tags=tuple(tags + ["no_future_bars"]),
            reference_index=armed_index,
            session_rows=session_rows,
        )
    warning_started_ts: datetime | None = None
    warning_bars = 0
    warning_reason: str | None = None
    warning_reference_row: AsiaDriftFeatureRow | None = None
    for waited_bars, row in enumerate(evaluation_rows, start=1):
        state = state_by_key.get((setup.asia_drift_session_id, row.decision_ts))
        cancel_reason = _entry_cancel_reason(
            setup=setup,
            row=row,
            state=state,
            allow_reacceleration_break=True,
        )
        if cancel_reason == "regime_lost_before_fill":
            if prefill_profile.regime_warning_bars <= 0:
                immediate_source = _prefill_regime_loss_source_category(row=row, state=state)
                return _cancelled_evaluation(
                    setup=setup,
                    model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                    reason="regime_lost_before_fill",
                    prerequisites_met=prerequisites_met,
                    candidate_price=candidate_price,
                    tags=tuple(tags + [f"prefill_warning_source:{immediate_source}"]),
                    cancelled_ts=row.decision_ts,
                    bars_waited=waited_bars,
                    reference_index=armed_index + waited_bars,
                    session_rows=session_rows,
                )
            hard_reason = _prefill_hard_fail_reason(row=row, state=state)
            if hard_reason is not None:
                return _cancelled_evaluation(
                    setup=setup,
                    model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                    reason=hard_reason,
                    prerequisites_met=prerequisites_met,
                    candidate_price=candidate_price,
                    tags=tuple(tags + [hard_reason]),
                    cancelled_ts=row.decision_ts,
                    bars_waited=waited_bars,
                    reference_index=armed_index + waited_bars,
                    session_rows=session_rows,
                )
            warning_allowed = _prefill_regime_warning_allowed(
                setup=setup,
                row=row,
                state=state,
                warning_bars=warning_bars,
                prefill_profile=prefill_profile,
            )
            if warning_allowed:
                warning_bars += 1
                warning_reason = _prefill_regime_loss_source_category(row=row, state=state)
                warning_started_ts = warning_started_ts or row.decision_ts
                warning_reference_row = row
                tags.extend(("prefill_warning_regime_loss", f"prefill_warning_source:{warning_reason}"))
                continue
            if warning_bars > 0:
                return _cancelled_evaluation(
                    setup=setup,
                    model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                    reason="regime_lost_before_fill",
                    prerequisites_met=prerequisites_met,
                    candidate_price=candidate_price,
                    tags=tuple(
                        tags
                        + [
                            "prefill_warning_failed_to_recover",
                            f"prefill_warning_bars:{warning_bars}",
                            f"prefill_warning_source:{warning_reason or _prefill_regime_loss_source_category(row=row, state=state)}",
                        ]
                    ),
                    cancelled_ts=row.decision_ts,
                    bars_waited=waited_bars,
                    reference_index=armed_index + waited_bars,
                    session_rows=session_rows,
                )
        if cancel_reason is not None:
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                reason=cancel_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=candidate_price,
                tags=tuple(tags + [cancel_reason]),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + waited_bars,
                session_rows=session_rows,
            )
        if warning_bars > 0:
            if _prefill_recovery_detected(
                setup=setup,
                row=row,
                reference_row=warning_reference_row or row,
                prefill_profile=prefill_profile,
            ):
                tags.extend(
                    (
                        "prefill_recovered_after_warning",
                        f"prefill_warning_bars:{warning_bars}",
                        f"prefill_warning_source:{warning_reason or 'unknown'}",
                    )
                )
                warning_bars = 0
                warning_reason = None
                warning_started_ts = None
                warning_reference_row = None
            elif warning_started_ts is not None and warning_bars >= prefill_profile.warning_recovery_bars:
                return _cancelled_evaluation(
                    setup=setup,
                    model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                    reason="regime_lost_before_fill",
                    prerequisites_met=prerequisites_met,
                    candidate_price=candidate_price,
                    tags=tuple(
                        tags
                        + [
                            "prefill_warning_failed_to_recover",
                            f"prefill_warning_bars:{warning_bars}",
                            f"prefill_warning_source:{warning_reason or 'unknown'}",
                        ]
                    ),
                    cancelled_ts=row.decision_ts,
                    bars_waited=waited_bars,
                    reference_index=armed_index + waited_bars,
                    session_rows=session_rows,
                )
        chase_reason = _refined_shallow_participation_chase_rejection(
            setup=setup,
            row=row,
            candidate_price=candidate_price,
        )
        if chase_reason is not None:
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                reason=chase_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=candidate_price,
                tags=tuple(tags + [chase_reason]),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + waited_bars,
                session_rows=session_rows,
            )
        participation_reason = _refined_shallow_participation_reason(setup=setup, row=row)
        if participation_reason is not None:
            return _cancelled_evaluation(
                setup=setup,
                model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                reason=participation_reason,
                prerequisites_met=prerequisites_met,
                candidate_price=candidate_price,
                tags=tuple(tags + [participation_reason]),
                cancelled_ts=row.decision_ts,
                bars_waited=waited_bars,
                reference_index=armed_index + waited_bars,
                session_rows=session_rows,
            )
        prior_row = session_rows[max(armed_index, armed_index + waited_bars - 1)]
        trigger_reason = _refined_shallow_participation_trigger_reason(
            setup=setup,
            row=row,
            prior_row=prior_row,
            candidate_price=candidate_price,
        )
        if trigger_reason is not None:
            return AsiaDriftEntryEvaluation(
                evaluation_id=f"{setup.setup_id}__{ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED.lower()}",
                setup_id=setup.setup_id,
                calibration_profile=setup.calibration_profile,
                instrument=setup.instrument,
                asia_drift_session_id=setup.asia_drift_session_id,
                entry_model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
                direction=setup.direction,
                status=ENTRY_STATUS_ENTERED,
                accepted=True,
                prerequisites_met=prerequisites_met,
                armed_ts=setup.armed_ts,
                evaluation_start_ts=evaluation_rows[0].decision_ts,
                evaluation_end_ts=row.decision_ts,
                entry_ts=row.decision_ts,
                entry_price=row.close,
                candidate_entry_price=candidate_price,
                bars_waited=waited_bars,
                cancellation_reason=None,
                reason_tags=tuple(tags + [trigger_reason]),
                continuation_reasserted_after_reject=False,
                missed_favorable_excursion_points=0.0,
                missed_favorable_excursion_r=0.0,
                scope_provenance=setup.scope_provenance,
                feature_version=setup.feature_version,
            )
    return _expired_evaluation(
        setup=setup,
        model=ENTRY_MODEL_SHALLOW_PARTICIPATION_REFINED,
        reason="shallow_participation_not_observed",
        prerequisites_met=prerequisites_met,
        candidate_price=candidate_price,
        tags=tuple(tags + ["shallow_participation_not_observed"]),
        reference_index=min(armed_index + 3, len(session_rows) - 1),
        session_rows=session_rows,
        evaluation_start_ts=evaluation_rows[0].decision_ts,
        evaluation_end_ts=evaluation_rows[-1].decision_ts,
        bars_waited=len(evaluation_rows),
    )


def _build_limit_zone(row: AsiaDriftFeatureRow) -> tuple[float, float, float | None, bool]:
    fib_low = min(row.retracement_38, row.retracement_62)
    fib_high = max(row.retracement_38, row.retracement_62)
    ref_low = min(row.session_vwap, row.fast_ema) - 0.10 * row.atr
    ref_high = max(row.session_vwap, row.fast_ema) + 0.10 * row.atr
    zone_low = max(fib_low, ref_low)
    zone_high = min(fib_high, ref_high)
    if zone_low > zone_high:
        return zone_low, zone_high, None, False
    limit_price = _round_to_tick((zone_low + zone_high) / 2.0, instrument=row.instrument)
    return zone_low, zone_high, limit_price, True


def _less_passive_limit_price(setup: AsiaDriftEntrySetup) -> float:
    zone_range = max(setup.entry_zone_high - setup.entry_zone_low, 1e-9)
    if setup.direction == "LONG":
        price = setup.entry_zone_low + 0.75 * zone_range
    else:
        price = setup.entry_zone_high - 0.75 * zone_range
    return _round_to_tick(price, instrument=setup.instrument)


def _shallow_participation_price(setup: AsiaDriftEntrySetup) -> float:
    if setup.direction == "LONG":
        price = setup.entry_zone_high + 0.10 * setup.atr
    else:
        price = setup.entry_zone_low - 0.10 * setup.atr
    return _round_to_tick(price, instrument=setup.instrument)


def _refined_shallow_participation_price(setup: AsiaDriftEntrySetup) -> float:
    if setup.direction == "LONG":
        price = setup.drift_leg_extreme - 0.18 * setup.atr
    else:
        price = setup.drift_leg_extreme + 0.18 * setup.atr
    return _round_to_tick(price, instrument=setup.instrument)


def _drift_strength_for_row(row: AsiaDriftFeatureRow) -> str:
    return row.long_drift_strength if row.regime == ASIA_DRIFT_LONG else row.short_drift_strength


def _future_ts(rows: Sequence[AsiaDriftFeatureRow], index: int, *, bars_forward: int) -> datetime | None:
    future_index = index + bars_forward
    if future_index >= len(rows):
        return None
    return rows[future_index].decision_ts


def _entry_cancel_reason(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    state: AsiaDriftStateRow | None,
    allow_reacceleration_break: bool = False,
) -> str | None:
    if row.decision_ts > setup.latest_entry_ts or not row.entry_window_open:
        return "latest_entry_time_passed"
    if not row.in_scope or row.session_timeout:
        return "session_timeout_boundary"
    if row.post_spike_instability:
        return "post_spike_instability"
    if row.regime != setup.regime:
        return "regime_lost_before_fill"
    if _drift_strength_for_row(row) not in {DRIFT_MEDIUM, DRIFT_STRONG}:
        return "drift_strength_below_medium"
    if row.pullback_state == DISQUALIFYING_PULLBACK or row.thesis_invalidated_flag:
        return row.pullback_reason or "thesis_invalidated_before_fill"
    if state is not None and state.state == STATE_THESIS_INVALIDATED:
        return state.transition_reason
    if setup.direction == "LONG":
        if row.close < setup.entry_zone_low - 0.10 * row.atr:
            return "zone_broken_through"
        if not allow_reacceleration_break and row.close > setup.drift_leg_extreme + 0.15 * row.atr:
            return "reexpanded_without_fill"
    else:
        if row.close > setup.entry_zone_high + 0.10 * row.atr:
            return "zone_broken_through"
        if not allow_reacceleration_break and row.close < setup.drift_leg_extreme - 0.15 * row.atr:
            return "reexpanded_without_fill"
    return None


def _limit_fill_allowed(*, setup: AsiaDriftEntrySetup, row: AsiaDriftFeatureRow) -> bool:
    if setup.entry_limit_price is None:
        return False
    if not (setup.entry_zone_low <= row.close <= setup.entry_zone_high):
        return False
    return abs(row.close - setup.entry_limit_price) <= 0.20 * row.atr


def _touch_limit_fill_allowed(*, direction: str, candidate_price: float, row: AsiaDriftFeatureRow) -> bool:
    if direction == "LONG":
        return row.low <= candidate_price <= row.high
    return row.low <= candidate_price <= row.high


def _confirmation_triggered(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    prior_row: AsiaDriftFeatureRow,
    pivot_price: float,
) -> bool:
    close_location = row.close_location
    if setup.direction == "LONG":
        return (
            row.close > pivot_price
            and close_location >= 0.65
            and row.signed_vwap_displacement_long > prior_row.signed_vwap_displacement_long
            and row.close <= pivot_price + 0.08 * row.atr
        )
    return (
        row.close < pivot_price
        and close_location <= 0.35
        and row.signed_vwap_displacement_short > prior_row.signed_vwap_displacement_short
        and row.close >= pivot_price - 0.08 * row.atr
    )


def _shallow_participation_triggered(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    prior_row: AsiaDriftFeatureRow,
) -> bool:
    if setup.direction == "LONG":
        return (
            row.pullback_state in {"NO_PULLBACK", "NORMAL_PULLBACK", "STRETCHED_BUT_VALID"}
            and row.close_location >= 0.58
            and row.signed_vwap_displacement_long >= prior_row.signed_vwap_displacement_long
            and row.close <= setup.drift_leg_extreme + 0.08 * row.atr
            and row.recovery_score >= 0.58
        )
    return (
        row.pullback_state in {"NO_PULLBACK", "NORMAL_PULLBACK", "STRETCHED_BUT_VALID"}
        and row.close_location <= 0.42
        and row.signed_vwap_displacement_short >= prior_row.signed_vwap_displacement_short
        and row.close >= setup.drift_leg_extreme - 0.08 * row.atr
        and row.recovery_score >= 0.58
    )


def _refined_shallow_participation_reason(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
) -> str | None:
    if row.pullback_state == DISQUALIFYING_PULLBACK:
        return row.pullback_reason or "pullback_disqualified"
    if row.fast_pullback_class not in {FAST_SHALLOW_VALID, FAST_STRETCHED_WARNING} and row.pullback_state != "NORMAL_PULLBACK":
        return "pullback_not_shallow_enough"
    if row.pullback_depth_fraction > 0.24 or row.pullback_depth_atr > 0.95:
        return "depth_exceeds_shallow_window"
    if row.pullback_vwap_interaction in {"CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}:
        return "vwap_acceptance_against_drift"
    if row.pullback_structure_break:
        return "structure_damage_detected"
    if row.recovery_score < 0.56:
        return "recovery_score_below_shallow_threshold"
    return None


def _prefill_hard_fail_reason(
    *,
    row: AsiaDriftFeatureRow,
    state: AsiaDriftStateRow | None,
) -> str | None:
    if row.post_spike_instability:
        return "post_spike_instability"
    if row.chop_veto:
        return "chop_veto_active"
    if row.pullback_state == DISQUALIFYING_PULLBACK:
        return row.pullback_reason or "pullback_disqualified"
    if row.pullback_depth_atr > 0.95 or row.pullback_depth_fraction > 0.24:
        return "depth_exceeds_shallow_window"
    if row.pullback_structure_break:
        return "structure_damage_detected"
    if row.pullback_vwap_interaction in {"CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}:
        return "vwap_acceptance_against_drift"
    if state is not None and state.state == STATE_THESIS_INVALIDATED:
        return state.transition_reason
    return None


def _prefill_regime_warning_allowed(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    state: AsiaDriftStateRow | None,
    warning_bars: int,
    prefill_profile: RefinedPrefillProfile,
) -> bool:
    if prefill_profile.regime_warning_bars <= 0 or warning_bars >= prefill_profile.regime_warning_bars:
        return False
    if _prefill_hard_fail_reason(row=row, state=state) is not None:
        return False
    if row.fast_pullback_class not in {FAST_SHALLOW_VALID, FAST_STRETCHED_WARNING} and row.pullback_state != "NORMAL_PULLBACK":
        return False
    if setup.direction == "LONG" and row.close < setup.entry_zone_low - 0.10 * row.atr:
        return False
    if setup.direction == "SHORT" and row.close > setup.entry_zone_high + 0.10 * row.atr:
        return False
    return True


def _prefill_recovery_detected(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    reference_row: AsiaDriftFeatureRow,
    prefill_profile: RefinedPrefillProfile,
) -> bool:
    if row.regime != setup.regime:
        return False
    if row.pullback_state == DISQUALIFYING_PULLBACK or row.chop_veto or row.post_spike_instability:
        return False
    if row.pullback_depth_atr > 0.95 or row.pullback_depth_fraction > 0.24:
        return False
    if row.pullback_structure_break:
        return False
    if row.pullback_vwap_interaction in {"CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}:
        return False
    if row.regime_persistence_score < prefill_profile.persistence_score_threshold:
        return False
    if row.recovery_score < prefill_profile.recovery_score_threshold:
        return False
    if setup.direction == "LONG":
        return (
            row.signed_vwap_displacement_long >= reference_row.signed_vwap_displacement_long
            and row.close_location >= 0.52
        )
    return (
        row.signed_vwap_displacement_short >= reference_row.signed_vwap_displacement_short
        and row.close_location <= 0.48
    )


def _prefill_regime_loss_source_category(
    *,
    row: AsiaDriftFeatureRow,
    state: AsiaDriftStateRow | None,
) -> str:
    vwap_damage = row.pullback_vwap_interaction in {"SINGLE_CLOSE_THROUGH_VWAP", "CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}
    structure_damage = row.pullback_structure_break or row.pullback_reason in {"protected_swing_break", "vwap_and_slow_ema_failure"}
    collapse = row.regime_persistence_label == "COLLAPSING" or row.recovery_score < 0.45
    if state is not None and state.at_risk_reason in {"confirmed_vwap_reclaim", "vwap_and_slow_ema_failure", "protected_swing_break"}:
        structure_damage = True
    active = sum(bool(flag) for flag in (vwap_damage, structure_damage, collapse))
    if active >= 2:
        return "combined_damage"
    if vwap_damage:
        return "vwap_failure"
    if structure_damage:
        return "ema_structure_failure"
    if collapse:
        return "drift_score_collapse"
    return "regime_flip_only"


def _refined_shallow_participation_trigger_reason(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    prior_row: AsiaDriftFeatureRow,
    candidate_price: float,
) -> str | None:
    if setup.direction == "LONG":
        if (
            row.close_location >= 0.56
            and row.signed_vwap_displacement_long >= prior_row.signed_vwap_displacement_long
            and row.close >= prior_row.close
            and row.close <= setup.drift_leg_extreme + 0.14 * row.atr
            and (row.fast_pullback_class == FAST_SHALLOW_VALID or row.recovery_score >= 0.60)
        ):
            return "refined_shallow_fast_acceptance"
        return None
    if (
        row.close_location <= 0.44
        and row.signed_vwap_displacement_short >= prior_row.signed_vwap_displacement_short
        and row.close <= prior_row.close
        and row.close >= setup.drift_leg_extreme - 0.14 * row.atr
        and (row.fast_pullback_class == FAST_SHALLOW_VALID or row.recovery_score >= 0.60)
    ):
        return "refined_shallow_fast_acceptance"
    return None


def _refined_shallow_participation_chase_rejection(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    candidate_price: float,
) -> str | None:
    chase_cap = 0.18 * row.atr
    if setup.direction == "LONG" and row.close > candidate_price + chase_cap:
        return "chase_cap_exceeded"
    if setup.direction == "SHORT" and row.close < candidate_price - chase_cap:
        return "chase_cap_exceeded"
    return None


def _pivot_price(*, direction: str, rows: Sequence[AsiaDriftFeatureRow], default: float) -> float:
    if not rows:
        return default
    if direction == "LONG":
        return max(row.high for row in rows)
    return min(row.low for row in rows)


def _rejected_evaluation(
    *,
    setup: AsiaDriftEntrySetup,
    model: str,
    reason: str,
    prerequisites_met: bool,
    tags: tuple[str, ...],
) -> AsiaDriftEntryEvaluation:
    return AsiaDriftEntryEvaluation(
        evaluation_id=f"{setup.setup_id}__{model.lower()}",
        setup_id=setup.setup_id,
        calibration_profile=setup.calibration_profile,
        instrument=setup.instrument,
        asia_drift_session_id=setup.asia_drift_session_id,
        entry_model=model,
        direction=setup.direction,
        status=ENTRY_STATUS_REJECTED,
        accepted=False,
        prerequisites_met=prerequisites_met,
        armed_ts=setup.armed_ts,
        evaluation_start_ts=None,
        evaluation_end_ts=None,
        entry_ts=None,
        entry_price=None,
        candidate_entry_price=setup.entry_limit_price,
        bars_waited=0,
        cancellation_reason=reason,
        reason_tags=tags,
        continuation_reasserted_after_reject=False,
        missed_favorable_excursion_points=0.0,
        missed_favorable_excursion_r=0.0,
        scope_provenance=setup.scope_provenance,
        feature_version=setup.feature_version,
    )


def _cancelled_evaluation(
    *,
    setup: AsiaDriftEntrySetup,
    model: str,
    reason: str,
    prerequisites_met: bool,
    candidate_price: float | None,
    tags: tuple[str, ...],
    cancelled_ts: datetime,
    bars_waited: int,
    reference_index: int,
    session_rows: Sequence[AsiaDriftFeatureRow],
) -> AsiaDriftEntryEvaluation:
    continuation, missed_points, missed_r = _post_reject_continuation(
        setup=setup,
        session_rows=session_rows,
        reference_index=reference_index,
        reference_price=candidate_price or session_rows[reference_index].close,
    )
    return AsiaDriftEntryEvaluation(
        evaluation_id=f"{setup.setup_id}__{model.lower()}",
        setup_id=setup.setup_id,
        calibration_profile=setup.calibration_profile,
        instrument=setup.instrument,
        asia_drift_session_id=setup.asia_drift_session_id,
        entry_model=model,
        direction=setup.direction,
        status=ENTRY_STATUS_CANCELLED,
        accepted=False,
        prerequisites_met=prerequisites_met,
        armed_ts=setup.armed_ts,
        evaluation_start_ts=setup.armed_ts,
        evaluation_end_ts=cancelled_ts,
        entry_ts=None,
        entry_price=None,
        candidate_entry_price=candidate_price,
        bars_waited=bars_waited,
        cancellation_reason=reason,
        reason_tags=tags,
        continuation_reasserted_after_reject=continuation,
        missed_favorable_excursion_points=missed_points,
        missed_favorable_excursion_r=missed_r,
        scope_provenance=setup.scope_provenance,
        feature_version=setup.feature_version,
    )


def _expired_evaluation(
    *,
    setup: AsiaDriftEntrySetup,
    model: str,
    reason: str,
    prerequisites_met: bool,
    candidate_price: float | None,
    tags: tuple[str, ...],
    reference_index: int,
    session_rows: Sequence[AsiaDriftFeatureRow],
    evaluation_start_ts: datetime | None = None,
    evaluation_end_ts: datetime | None = None,
    bars_waited: int = 0,
) -> AsiaDriftEntryEvaluation:
    continuation, missed_points, missed_r = _post_reject_continuation(
        setup=setup,
        session_rows=session_rows,
        reference_index=reference_index,
        reference_price=candidate_price or session_rows[reference_index].close,
    )
    return AsiaDriftEntryEvaluation(
        evaluation_id=f"{setup.setup_id}__{model.lower()}",
        setup_id=setup.setup_id,
        calibration_profile=setup.calibration_profile,
        instrument=setup.instrument,
        asia_drift_session_id=setup.asia_drift_session_id,
        entry_model=model,
        direction=setup.direction,
        status=ENTRY_STATUS_EXPIRED,
        accepted=False,
        prerequisites_met=prerequisites_met,
        armed_ts=setup.armed_ts,
        evaluation_start_ts=evaluation_start_ts,
        evaluation_end_ts=evaluation_end_ts,
        entry_ts=None,
        entry_price=None,
        candidate_entry_price=candidate_price,
        bars_waited=bars_waited,
        cancellation_reason=reason,
        reason_tags=tags,
        continuation_reasserted_after_reject=continuation,
        missed_favorable_excursion_points=missed_points,
        missed_favorable_excursion_r=missed_r,
        scope_provenance=setup.scope_provenance,
        feature_version=setup.feature_version,
    )


def _post_reject_continuation(
    *,
    setup: AsiaDriftEntrySetup,
    session_rows: Sequence[AsiaDriftFeatureRow],
    reference_index: int,
    reference_price: float,
) -> tuple[bool, float, float]:
    future_rows = [row for row in session_rows[reference_index + 1 :] if row.decision_ts <= setup.session_timeout_ts]
    if setup.direction == "LONG":
        continuation = any(row.high >= setup.drift_leg_extreme + 0.10 * setup.atr for row in future_rows)
        missed_points = max((row.high - reference_price for row in future_rows), default=0.0)
    else:
        continuation = any(row.low <= setup.drift_leg_extreme - 0.10 * setup.atr for row in future_rows)
        missed_points = max((reference_price - row.low for row in future_rows), default=0.0)
    _, risk_points = estimate_stop_and_risk(setup=setup, entry_price=reference_price)
    return continuation, max(missed_points, 0.0), max(missed_points, 0.0) / max(risk_points, 1e-9)


def _features_by_session(
    feature_rows: Sequence[AsiaDriftFeatureRow],
) -> dict[str, list[AsiaDriftFeatureRow]]:
    grouped: dict[str, list[AsiaDriftFeatureRow]] = defaultdict(list)
    for row in sorted(feature_rows, key=lambda item: (item.asia_drift_session_id, item.decision_ts)):
        grouped[row.asia_drift_session_id].append(row)
    return grouped


def _state_by_key(
    state_rows: Sequence[AsiaDriftStateRow],
) -> dict[tuple[str, datetime], AsiaDriftStateRow]:
    return {
        (row.asia_drift_session_id, row.decision_ts): row
        for row in state_rows
    }


def _round_to_tick(value: float, *, instrument: str) -> float:
    tick = 0.1 if instrument.upper() in {"MGC", "GC"} else 0.01
    return round(value / tick) * tick
