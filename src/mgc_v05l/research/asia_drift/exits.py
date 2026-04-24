"""Replay-only exit-profile evaluation for Asia Drift v1 Phase 2."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Sequence

from .entries import estimate_stop_and_risk
from .features import ASIA_DRIFT_LONG, DISQUALIFYING_PULLBACK
from .models import AsiaDriftEntryEvaluation, AsiaDriftEntrySetup, AsiaDriftExitProfile, AsiaDriftFeatureRow, AsiaDriftTradeRecord


EXIT_PROFILE_SPEC_BASELINE = "SPEC_BASELINE"
EXIT_PROFILE_EARLY_PROTECTION = "EARLY_PROTECTION"


def default_exit_profiles() -> tuple[AsiaDriftExitProfile, ...]:
    return (
        AsiaDriftExitProfile(
            name=EXIT_PROFILE_SPEC_BASELINE,
            profit_target_r=2.0,
            max_hold_bars=12,
            giveback_activation_r=1.0,
            giveback_fraction=0.45,
            vwap_giveback_activation_r=1.0,
            vwap_giveback_fraction=0.40,
        ),
        AsiaDriftExitProfile(
            name=EXIT_PROFILE_EARLY_PROTECTION,
            profit_target_r=1.5,
            max_hold_bars=8,
            giveback_activation_r=0.8,
            giveback_fraction=0.35,
            vwap_giveback_activation_r=0.8,
            vwap_giveback_fraction=0.35,
        ),
    )


def simulate_trades(
    *,
    feature_rows: Sequence[AsiaDriftFeatureRow],
    setups: Sequence[AsiaDriftEntrySetup],
    entry_evaluations: Sequence[AsiaDriftEntryEvaluation],
    exit_profiles: Sequence[AsiaDriftExitProfile] | None = None,
) -> list[AsiaDriftTradeRecord]:
    profiles = tuple(exit_profiles or default_exit_profiles())
    setup_by_id = {setup.setup_id: setup for setup in setups}
    features_by_session = _features_by_session(feature_rows)
    accepted_evaluations = [row for row in entry_evaluations if row.accepted and row.entry_ts is not None and row.entry_price is not None]

    trades: list[AsiaDriftTradeRecord] = []
    for evaluation in accepted_evaluations:
        setup = setup_by_id.get(evaluation.setup_id)
        if setup is None:
            continue
        session_rows = features_by_session.get(setup.asia_drift_session_id, [])
        entry_index = next((index for index, row in enumerate(session_rows) if row.decision_ts == evaluation.entry_ts), None)
        if entry_index is None:
            continue
        for profile in profiles:
            trades.append(
                _simulate_single_trade(
                    setup=setup,
                    evaluation=evaluation,
                    session_rows=session_rows,
                    entry_index=entry_index,
                    profile=profile,
                )
            )
    return trades


def _simulate_single_trade(
    *,
    setup: AsiaDriftEntrySetup,
    evaluation: AsiaDriftEntryEvaluation,
    session_rows: Sequence[AsiaDriftFeatureRow],
    entry_index: int,
    profile: AsiaDriftExitProfile,
) -> AsiaDriftTradeRecord:
    assert evaluation.entry_price is not None
    assert evaluation.entry_ts is not None
    entry_price = evaluation.entry_price
    stop_price, risk_points = estimate_stop_and_risk(setup=setup, entry_price=entry_price)
    target_price = (
        entry_price + profile.profit_target_r * risk_points
        if setup.direction == "LONG"
        else entry_price - profile.profit_target_r * risk_points
    )

    future_rows = list(session_rows[entry_index + 1 :])
    peak_mfe_points = 0.0
    peak_open_profit_r = 0.0
    mfe_r = 0.0
    mae_r = 0.0
    peak_mfe_r = 0.0
    time_to_follow_through: int | None = None
    time_to_failure: int | None = None
    vwap_warning_count = 0
    consecutive_vwap_failures = 0
    exit_row = session_rows[entry_index]
    exit_reason = "data_end"
    exit_hardness = "hard"
    bars_held = 0

    for bars_held, row in enumerate(future_rows, start=1):
        favorable_points = _favorable_points(setup=setup, entry_price=entry_price, row=row)
        adverse_points = _adverse_points(setup=setup, entry_price=entry_price, row=row)
        open_profit_r = _directional_close_r(setup=setup, entry_price=entry_price, close=row.close, risk_points=risk_points)
        peak_mfe_points = max(peak_mfe_points, favorable_points)
        peak_mfe_r = max(peak_mfe_r, peak_mfe_points / max(risk_points, 1e-9))
        peak_open_profit_r = max(peak_open_profit_r, open_profit_r)
        mfe_r = max(mfe_r, favorable_points / max(risk_points, 1e-9))
        mae_r = max(mae_r, adverse_points / max(risk_points, 1e-9))
        if time_to_follow_through is None and peak_mfe_r >= 0.75:
            time_to_follow_through = bars_held
        if time_to_failure is None and mae_r >= 1.0:
            time_to_failure = bars_held

        invalidation_reason = _invalidation_exit_reason(setup=setup, row=row, entry_price=entry_price, stop_price=stop_price)
        if invalidation_reason is not None:
            exit_row = row
            exit_reason = invalidation_reason
            exit_hardness = "hard"
            break

        vwap_reason, consecutive_vwap_failures, vwap_warning_count = _vwap_failure_exit_reason(
            setup=setup,
            row=row,
            consecutive_vwap_failures=consecutive_vwap_failures,
            vwap_warning_count=vwap_warning_count,
            peak_mfe_r=peak_mfe_r,
            open_profit_r=open_profit_r,
            profile=profile,
        )
        if vwap_reason is not None:
            exit_row = row
            exit_reason = vwap_reason
            exit_hardness = "conditional"
            break

        giveback_reason = _giveback_exit_reason(
            peak_mfe_r=peak_mfe_r,
            open_profit_r=open_profit_r,
            profile=profile,
        )
        if giveback_reason is not None:
            exit_row = row
            exit_reason = giveback_reason
            exit_hardness = "soft"
            break

        if _target_hit(setup=setup, row=row, target_price=target_price):
            exit_row = row
            exit_reason = "profit_target_hit"
            exit_hardness = "hard"
            break

        if bars_held >= profile.max_hold_bars:
            exit_row = row
            exit_reason = "time_stop_no_followthrough"
            exit_hardness = "soft"
            break

        if row.session_timeout or not row.in_scope or row.decision_ts >= setup.session_timeout_ts:
            exit_row = row
            exit_reason = "session_timeout_handoff"
            exit_hardness = "hard"
            break
    else:
        if future_rows:
            exit_row = future_rows[-1]
            exit_reason = "data_end"
            exit_hardness = "hard"

    post_exit_followthrough_r, post_exit_deterioration_r = _post_exit_path(
        setup=setup,
        exit_row=exit_row,
        session_rows=session_rows,
        risk_points=risk_points,
    )
    gross_r = _directional_close_r(
        setup=setup,
        entry_price=entry_price,
        close=exit_row.close,
        risk_points=risk_points,
    )
    return AsiaDriftTradeRecord(
        trade_id=f"{evaluation.evaluation_id}__{profile.name.lower()}",
        setup_id=setup.setup_id,
        evaluation_id=evaluation.evaluation_id,
        calibration_profile=setup.calibration_profile,
        instrument=setup.instrument,
        asia_drift_session_id=setup.asia_drift_session_id,
        entry_model=evaluation.entry_model,
        exit_profile=profile.name,
        direction=setup.direction,
        entry_ts=evaluation.entry_ts,
        entry_price=entry_price,
        stop_price=stop_price,
        target_price=target_price,
        initial_risk_points=risk_points,
        exit_ts=exit_row.decision_ts,
        exit_price=exit_row.close,
        exit_reason=exit_reason,
        exit_hardness=exit_hardness,
        bars_held=bars_held,
        mfe_points=peak_mfe_points,
        mae_points=mae_r * risk_points,
        mfe_r=mfe_r,
        mae_r=mae_r,
        gross_r=gross_r,
        peak_open_profit_r=peak_open_profit_r,
        continuation_achieved=peak_mfe_r >= 0.75,
        continuation_threshold_r=0.75,
        time_to_follow_through_bars=time_to_follow_through,
        time_to_failure_bars=time_to_failure,
        post_exit_followthrough_r=post_exit_followthrough_r,
        post_exit_deterioration_r=post_exit_deterioration_r,
        vwap_warning_count=vwap_warning_count,
        scope_provenance=setup.scope_provenance,
        feature_version=setup.feature_version,
    )


def _invalidation_exit_reason(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    entry_price: float,
    stop_price: float,
) -> str | None:
    if setup.direction == "LONG" and row.close <= stop_price:
        return "hard_stop_close_breach"
    if setup.direction == "SHORT" and row.close >= stop_price:
        return "hard_stop_close_breach"
    if row.pullback_state == DISQUALIFYING_PULLBACK or row.thesis_invalidated_flag:
        return row.pullback_reason or "thesis_invalidated_after_entry"
    if setup.direction == "LONG":
        if row.close < row.protected_swing_price:
            return "protected_swing_break"
        if row.short_drift_score >= 3.1 and (row.short_drift_score - row.long_drift_score) >= 0.75:
            return "opposite_drift_regime_reached"
    else:
        if row.close > row.protected_swing_price:
            return "protected_swing_break"
        if row.long_drift_score >= 3.1 and (row.long_drift_score - row.short_drift_score) >= 0.75:
            return "opposite_drift_regime_reached"
    if row.post_spike_instability:
        return "post_spike_instability"
    return None


def _vwap_failure_exit_reason(
    *,
    setup: AsiaDriftEntrySetup,
    row: AsiaDriftFeatureRow,
    consecutive_vwap_failures: int,
    vwap_warning_count: int,
    peak_mfe_r: float,
    open_profit_r: float,
    profile: AsiaDriftExitProfile,
) -> tuple[str | None, int, int]:
    through_vwap = row.close < row.session_vwap if setup.direction == "LONG" else row.close > row.session_vwap
    through_slow = row.close < row.slow_ema if setup.direction == "LONG" else row.close > row.slow_ema
    if through_vwap:
        consecutive_vwap_failures += 1
        vwap_warning_count += 1
    else:
        consecutive_vwap_failures = 0
    if consecutive_vwap_failures >= 2:
        return "two_closes_through_vwap", consecutive_vwap_failures, vwap_warning_count
    if through_vwap and through_slow:
        return "vwap_and_slow_ema_failure", consecutive_vwap_failures, vwap_warning_count
    if through_vwap and peak_mfe_r >= profile.vwap_giveback_activation_r:
        giveback = max(peak_mfe_r - open_profit_r, 0.0)
        if peak_mfe_r > 0 and (giveback / peak_mfe_r) >= profile.vwap_giveback_fraction:
            return "vwap_failure_after_open_profit_giveback", consecutive_vwap_failures, vwap_warning_count
    return None, consecutive_vwap_failures, vwap_warning_count


def _giveback_exit_reason(
    *,
    peak_mfe_r: float,
    open_profit_r: float,
    profile: AsiaDriftExitProfile,
) -> str | None:
    if peak_mfe_r >= profile.giveback_activation_r:
        giveback = max(peak_mfe_r - open_profit_r, 0.0)
        if peak_mfe_r > 0 and (giveback / peak_mfe_r) >= profile.giveback_fraction:
            return "mfe_giveback_exit"
    if peak_mfe_r >= 0.8 and open_profit_r <= -0.05:
        return "breakeven_floor_failure"
    return None


def _target_hit(*, setup: AsiaDriftEntrySetup, row: AsiaDriftFeatureRow, target_price: float) -> bool:
    if setup.direction == "LONG":
        return row.close >= target_price
    return row.close <= target_price


def _favorable_points(*, setup: AsiaDriftEntrySetup, entry_price: float, row: AsiaDriftFeatureRow) -> float:
    if setup.direction == "LONG":
        return max(row.high - entry_price, 0.0)
    return max(entry_price - row.low, 0.0)


def _adverse_points(*, setup: AsiaDriftEntrySetup, entry_price: float, row: AsiaDriftFeatureRow) -> float:
    if setup.direction == "LONG":
        return max(entry_price - row.low, 0.0)
    return max(row.high - entry_price, 0.0)


def _directional_close_r(
    *,
    setup: AsiaDriftEntrySetup,
    entry_price: float,
    close: float,
    risk_points: float,
) -> float:
    signed = (close - entry_price) if setup.direction == "LONG" else (entry_price - close)
    return signed / max(risk_points, 1e-9)


def _post_exit_path(
    *,
    setup: AsiaDriftEntrySetup,
    exit_row: AsiaDriftFeatureRow,
    session_rows: Sequence[AsiaDriftFeatureRow],
    risk_points: float,
) -> tuple[float, float]:
    exit_index = next((index for index, row in enumerate(session_rows) if row.decision_ts == exit_row.decision_ts), None)
    if exit_index is None:
        return 0.0, 0.0
    future_rows = session_rows[exit_index + 1 : exit_index + 4]
    if not future_rows:
        return 0.0, 0.0
    if setup.direction == "LONG":
        best_follow = max((row.high - exit_row.close) for row in future_rows)
        worst_drop = max((exit_row.close - row.low) for row in future_rows)
    else:
        best_follow = max((exit_row.close - row.low) for row in future_rows)
        worst_drop = max((row.high - exit_row.close) for row in future_rows)
    return max(best_follow, 0.0) / max(risk_points, 1e-9), max(worst_drop, 0.0) / max(risk_points, 1e-9)


def summarize_exit_reasons(trades: Sequence[AsiaDriftTradeRecord]) -> dict[str, dict[str, int]]:
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    for trade in trades:
        grouped[trade.exit_profile][trade.exit_reason] += 1
    return {profile: dict(counter) for profile, counter in grouped.items()}


def _features_by_session(
    rows: Sequence[AsiaDriftFeatureRow],
) -> dict[str, list[AsiaDriftFeatureRow]]:
    grouped: dict[str, list[AsiaDriftFeatureRow]] = defaultdict(list)
    for row in sorted(rows, key=lambda item: (item.asia_drift_session_id, item.decision_ts)):
        grouped[row.asia_drift_session_id].append(row)
    return grouped
