"""ATP v1 Phase 3 replay/paper timing bridge using 1m bars."""

from __future__ import annotations

import json
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .models import AtpEntryState, AtpTimingState, ConflictOutcome, FeatureState, PatternVariant, ResearchBar, TradeRecord
from .phase2_continuation import (
    ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED,
    ATP_V1_LONG_CONTINUATION_FAMILY,
    ATP_V1_LONG_CONTINUATION_VARIANT_ID,
    ATP_V1_SHORT_CONTINUATION_FAMILY,
    ATP_V1_SHORT_CONTINUATION_VARIANT_ID,
    ENTRY_ELIGIBLE,
    atp_phase2_variant,
)
from .state_layers import LONG_BIAS, rolling_ema

ATP_TIMING_WAITING = "ATP_TIMING_WAITING"
ATP_TIMING_CONFIRMED = "ATP_TIMING_CONFIRMED"
ATP_TIMING_EARLY_PARTICIPATION = "ATP_TIMING_EARLY_PARTICIPATION"
ATP_TIMING_CHASE_RISK = "ATP_TIMING_CHASE_RISK"
ATP_TIMING_INVALIDATED = "ATP_TIMING_INVALIDATED"
ATP_TIMING_UNAVAILABLE = "ATP_TIMING_UNAVAILABLE"

ATP_TIMING_ACTIVATION_COMPLETED_5M = "completed_5m_close"
ATP_TIMING_ACTIVATION_ROLLING_5M = "rolling_5m_on_1m"

VWAP_FAVORABLE = "VWAP_FAVORABLE"
VWAP_NEUTRAL = "VWAP_NEUTRAL"
VWAP_CHASE_RISK = "VWAP_CHASE_RISK"

ATP_TIMING_5M_CONTEXT_NOT_READY = "ATP_TIMING_5M_CONTEXT_NOT_READY"
ATP_TIMING_NO_1M_WINDOW = "ATP_TIMING_NO_1M_WINDOW"
ATP_TIMING_CONFIRMATION_NOT_REACHED = "ATP_TIMING_CONFIRMATION_NOT_REACHED"
ATP_TIMING_VWAP_CHASE_RISK = "ATP_TIMING_VWAP_CHASE_RISK"
ATP_TIMING_INVALIDATED_BEFORE_ENTRY = "ATP_TIMING_INVALIDATED_BEFORE_ENTRY"
ATP_TIMING_LONDON_DISABLED = "ATP_TIMING_LONDON_DISABLED"

MINUTE_FAST_EMA_SPAN = 5
TIMING_NEUTRAL_VWAP_BAND = 0.10
ATP_REPLAY_EXIT_POLICY_FIXED_TARGET = "fixed_target_time_stop"
ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT = "target_checkpoint_trail"
ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT_LONG_HOLD = "target_checkpoint_trail_long_hold"
ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT_NO_TRACTION = "target_checkpoint_no_traction_abort"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_15M_2OF3 = "checkpoint075_no_traction_abort_15m_2of3"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3 = "checkpoint100_no_traction_abort_15m_2of3"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_10M_DOUBLE_WEAK = "checkpoint075_no_traction_abort_10m_double_weak_close"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_20M_LOOSE = "checkpoint075_no_traction_abort_20m_loose_trend_hold"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_2OF3 = "checkpoint100_no_traction_abort_7m_2of3"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_STRICT_WEAK = "checkpoint100_no_traction_abort_7m_strict_weak_close"
ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_LOOSE_HOLD = "checkpoint100_no_traction_abort_7m_loose_hold"
ATP_REPLAY_CHECKPOINT_LOCK_R = 0.35
ATP_REPLAY_CHECKPOINT_TRAIL_R = 0.25
ATP_REPLAY_LONG_HOLD_EXTENSION_BARS = 12
ATP_REPLAY_NO_TRACTION_ABORT_BARS = 2
ATP_REPLAY_NO_TRACTION_MIN_FAVORABLE_R = 0.25
ATP_REPLAY_PROMOTION_EXTENSION_R = 0.25
ATP_REPLAY_MAINTENANCE_FAST_EMA_SPAN = 5


def build_phase3_replay_package(
    *,
    entry_states: Sequence[AtpEntryState],
    bars_1m: Sequence[Any],
    point_value: float,
    old_proxy_trade_count: int,
    entry_activation_basis: str = ATP_TIMING_ACTIVATION_COMPLETED_5M,
    allow_pre_5m_context_participation: bool = False,
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    initial_timing_states = classify_timing_states(
        entry_states=entry_states,
        bars_1m=bars_1m,
        entry_activation_basis=entry_activation_basis,
        allow_pre_5m_context_participation=allow_pre_5m_context_participation,
        variant_overrides=variant_overrides,
    )
    initial_trades = simulate_timed_entries(
        timing_states=initial_timing_states,
        bars_1m=bars_1m,
        point_value=point_value,
        variant=atp_phase2_variant(variant_overrides=variant_overrides),
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
    )
    timing_states = overlay_position_blocks(timing_states=initial_timing_states, trades=initial_trades)
    trades = simulate_timed_entries(
        timing_states=timing_states,
        bars_1m=bars_1m,
        point_value=point_value,
        variant=atp_phase2_variant(variant_overrides=variant_overrides),
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
    )
    diagnostics = summarize_phase3_timing_diagnostics(
        timing_states=timing_states,
        trades=trades,
        old_proxy_trade_count=old_proxy_trade_count,
        variant_overrides=variant_overrides,
    )
    return {
        "timing_states": timing_states,
        "shadow_trades": trades,
        "diagnostics": diagnostics,
    }


def classify_timing_states(
    *,
    entry_states: Sequence[AtpEntryState],
    bars_1m: Sequence[Any],
    entry_activation_basis: str = ATP_TIMING_ACTIVATION_COMPLETED_5M,
    allow_pre_5m_context_participation: bool = False,
    variant_overrides: Mapping[str, Any] | None = None,
) -> list[AtpTimingState]:
    minute_bars = sorted(bars_1m, key=lambda bar: (bar.instrument, bar.end_ts))
    bars_by_instrument: dict[str, list[Any]] = {}
    for bar in minute_bars:
        bars_by_instrument.setdefault(bar.instrument, []).append(bar)
    timing_states: list[AtpTimingState] = []
    for state in entry_states:
        instrument_bars = bars_by_instrument.get(state.instrument, [])
        early_participation_allowed = (
            allow_pre_5m_context_participation
            and state.entry_state != ENTRY_ELIGIBLE
            and state.primary_blocker == ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED
            and set(state.blocker_codes) <= {ATP_CONTINUATION_TRIGGER_NOT_CONFIRMED}
        )
        if state.entry_state != ENTRY_ELIGIBLE and not early_participation_allowed:
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_UNAVAILABLE,
                    vwap_price_quality_state=VWAP_NEUTRAL,
                    blocker_codes=(ATP_TIMING_5M_CONTEXT_NOT_READY,),
                    primary_blocker=ATP_TIMING_5M_CONTEXT_NOT_READY,
                    feature_snapshot={"timing_activation": "blocked_by_5m_context"},
                )
            )
            continue
        if state.session_segment == "LONDON":
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_UNAVAILABLE,
                    vwap_price_quality_state=VWAP_NEUTRAL,
                    blocker_codes=(ATP_TIMING_LONDON_DISABLED,),
                    primary_blocker=ATP_TIMING_LONDON_DISABLED,
                    feature_snapshot={"timing_activation": "london_disabled"},
                )
            )
            continue
        candidate_window = _timing_window_bars(
            state=state,
            minute_bars=instrument_bars,
            entry_activation_basis=entry_activation_basis,
            variant_overrides=variant_overrides,
        )
        if not candidate_window:
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=ATP_TIMING_UNAVAILABLE,
                    vwap_price_quality_state=VWAP_NEUTRAL,
                    blocker_codes=(ATP_TIMING_NO_1M_WINDOW,),
                    primary_blocker=ATP_TIMING_NO_1M_WINDOW,
                    feature_snapshot={"timing_activation": "no_1m_window"},
                )
            )
            continue
        minute_ema_by_ts = _minute_fast_ema_map(candidate_window)
        pivot_price = _pivot_price(state)
        invalidation_price = _invalidation_price(state)
        saw_confirmation = False
        saw_chase_risk = False
        last_price_quality = VWAP_NEUTRAL
        last_snapshot: dict[str, Any] = {}
        confirmation_timing_state = (
            ATP_TIMING_EARLY_PARTICIPATION if early_participation_allowed else ATP_TIMING_CONFIRMED
        )

        for index, bar in enumerate(candidate_window):
            previous_bar = candidate_window[index - 1] if index > 0 else None
            bar_vwap = _bar_vwap(bar)
            entry_price = max(float(bar.open), pivot_price) if state.side == "LONG" else min(float(bar.open), pivot_price)
            price_quality = classify_vwap_price_quality(
                side=state.side,
                entry_price=entry_price,
                bar_vwap=bar_vwap,
                band_reference=max(bar.range_points, float(state.feature_snapshot.get("average_range") or 0.0) / 5.0, 1e-9),
            )
            last_price_quality = price_quality
            minute_fast_ema = minute_ema_by_ts[bar.end_ts]
            confirmation_checks = {
                "pivot_price": round(pivot_price, 6),
                "invalidation_price": round(invalidation_price, 6),
                "bar_vwap": round(bar_vwap, 6),
                "entry_price": round(entry_price, 6),
                "price_quality": price_quality,
                "close_crosses_pivot": float(bar.close) >= pivot_price if state.side == "LONG" else float(bar.close) <= pivot_price,
                "breaks_pivot": float(bar.high) >= pivot_price if state.side == "LONG" else float(bar.low) <= pivot_price,
                "close_relative_to_minute_fast_ema": float(bar.close) >= minute_fast_ema if state.side == "LONG" else float(bar.close) <= minute_fast_ema,
                "positive_reacceleration": (
                    previous_bar is not None
                    and (
                        (
                            state.side == "LONG"
                            and float(bar.close) > float(previous_bar.close)
                            and float(bar.high) >= float(previous_bar.high)
                        )
                        or (
                            state.side == "SHORT"
                            and float(bar.close) < float(previous_bar.close)
                            and float(bar.low) <= float(previous_bar.low)
                        )
                    )
                ),
                "non_violent_against_trend": float(bar.low) > invalidation_price if state.side == "LONG" else float(bar.high) < invalidation_price,
            }
            last_snapshot = {
                "timing_bar_ts": bar.end_ts.isoformat(),
                "timing_checks": confirmation_checks,
                "timing_activation": (
                    "pre_5m_context_participation" if early_participation_allowed else "completed_5m_context_ready"
                ),
                "original_entry_state": state.entry_state,
                "original_primary_blocker": state.primary_blocker,
            }
            if (float(bar.low) <= invalidation_price) if state.side == "LONG" else (float(bar.high) >= invalidation_price):
                timing_states.append(
                    _timing_state(
                        state=state,
                        timing_state=ATP_TIMING_INVALIDATED,
                        vwap_price_quality_state=price_quality,
                        blocker_codes=(ATP_TIMING_INVALIDATED_BEFORE_ENTRY,),
                        primary_blocker=ATP_TIMING_INVALIDATED_BEFORE_ENTRY,
                        invalidated_before_entry=True,
                        feature_snapshot=last_snapshot,
                    )
                )
                break
            confirmed = all(
                (
                    confirmation_checks["close_crosses_pivot"],
                    confirmation_checks["breaks_pivot"],
                    confirmation_checks["close_relative_to_minute_fast_ema"],
                    confirmation_checks["positive_reacceleration"],
                    confirmation_checks["non_violent_against_trend"],
                )
            )
            if not confirmed:
                continue
            saw_confirmation = True
            if price_quality == VWAP_CHASE_RISK:
                saw_chase_risk = True
                last_snapshot["timing_checks"]["blocked_for_chase_risk"] = True
                continue
            timing_states.append(
                _timing_state(
                    state=state,
                    timing_state=confirmation_timing_state,
                    vwap_price_quality_state=price_quality,
                    blocker_codes=(),
                    primary_blocker=None,
                    timing_confirmed=True,
                    executable_entry=True,
                    timing_bar_ts=bar.end_ts,
                    entry_ts=bar.end_ts,
                    entry_price=entry_price,
                    feature_snapshot=last_snapshot,
                )
            )
            break
        else:
            if saw_chase_risk and saw_confirmation:
                timing_states.append(
                    _timing_state(
                        state=state,
                        timing_state=ATP_TIMING_CHASE_RISK,
                        vwap_price_quality_state=VWAP_CHASE_RISK,
                        blocker_codes=(ATP_TIMING_VWAP_CHASE_RISK,),
                        primary_blocker=ATP_TIMING_VWAP_CHASE_RISK,
                        timing_confirmed=True,
                        feature_snapshot=last_snapshot,
                    )
                )
            else:
                timing_states.append(
                    _timing_state(
                        state=state,
                        timing_state=ATP_TIMING_WAITING,
                        vwap_price_quality_state=last_price_quality,
                        blocker_codes=(ATP_TIMING_CONFIRMATION_NOT_REACHED,),
                        primary_blocker=ATP_TIMING_CONFIRMATION_NOT_REACHED,
                        feature_snapshot=last_snapshot,
                    )
                )
    return timing_states


def simulate_timed_entries(
    *,
    timing_states: Sequence[AtpTimingState],
    bars_1m: Sequence[Any],
    point_value: float,
    variant: PatternVariant,
    slippage_points: float = 0.25,
    fee_per_trade: float = 1.50,
    feature_rows: Sequence[FeatureState] | None = None,
    exit_policy: str = ATP_REPLAY_EXIT_POLICY_FIXED_TARGET,
    variant_overrides: Mapping[str, Any] | None = None,
) -> list[TradeRecord]:
    minute_bars = sorted(bars_1m, key=lambda bar: (bar.instrument, bar.end_ts))
    bars_by_instrument: dict[str, list[Any]] = {}
    for bar in minute_bars:
        bars_by_instrument.setdefault(bar.instrument, []).append(bar)
    maintenance_cache: dict[tuple[str, int], dict[str, Any]] = {}
    feature_rows_by_instrument: dict[str, list[FeatureState]] = {}
    feature_ts_by_instrument: dict[str, list[Any]] = {}
    for feature in sorted(feature_rows or (), key=lambda item: (item.instrument, item.decision_ts)):
        feature_rows_by_instrument.setdefault(feature.instrument, []).append(feature)
    for instrument, rows in feature_rows_by_instrument.items():
        feature_ts_by_instrument[instrument] = [row.decision_ts for row in rows]

    trades: list[TradeRecord] = []
    policy_profile = _replay_exit_policy_profile(str(exit_policy or ATP_REPLAY_EXIT_POLICY_FIXED_TARGET))
    blocked_until_by_instrument: dict[str, Any] = {}
    last_trade_by_setup: dict[tuple[str, str], dict[str, Any]] = {}
    for state in sorted(timing_states, key=lambda item: (item.instrument, item.decision_ts, item.side, item.family_name)):
        if not state.executable_entry or state.entry_ts is None or state.entry_price is None:
            continue
        instrument = str(state.instrument)
        variant_for_state = atp_phase2_variant(state.side, variant_overrides=variant_overrides)
        blocked_until = blocked_until_by_instrument.get(instrument)
        if blocked_until is not None and state.decision_ts < blocked_until:
            continue
        setup_signature = str(state.feature_snapshot.get("setup_signature") or state.family_name)
        setup_state_signature = str(state.feature_snapshot.get("setup_state_signature") or setup_signature)
        previous_same_setup = last_trade_by_setup.get((instrument, setup_signature))
        if previous_same_setup is not None and not variant_for_state.allow_reentry:
            continue
        if (
            previous_same_setup is not None
            and setup_state_signature == str(previous_same_setup["setup_state_signature"])
            and state.decision_ts
            < previous_same_setup["exit_ts"] + timedelta(minutes=variant_for_state.reset_window_bars_5m * 5)
        ):
            continue
        reentry_type = _classify_timing_reentry_type(
            state=state,
            variant=variant_for_state,
            previous_same_setup=previous_same_setup,
        )
        if reentry_type == "LOCAL_CHURN" and variant_for_state.reentry_policy == "structural_only":
            continue
        if reentry_type != "NONE" and not variant_for_state.allow_reentry:
            continue
        candidate_bars = bars_by_instrument.get(state.instrument, [])
        minute_end_timestamps = [bar.end_ts for bar in candidate_bars]
        entry_index = bisect_right(minute_end_timestamps, state.entry_ts) - 1
        if entry_index < 0 or entry_index >= len(candidate_bars):
            continue
        entry_bar = candidate_bars[entry_index]
        execution_window = _execution_window_for_policy(
            candidate_bars=candidate_bars,
            entry_index=entry_index,
            state=state,
            policy_profile=policy_profile,
            base_max_hold_bars_1m=variant.max_hold_bars_1m,
        )
        if not execution_window:
            continue
        raw_entry_price = float(state.entry_price)
        entry_price = raw_entry_price + slippage_points if state.side == "LONG" else raw_entry_price - slippage_points
        average_range = max(float(state.feature_snapshot.get("average_range") or 0.25), 0.25)
        decision_bar_low = float(state.feature_snapshot.get("decision_bar_low") or raw_entry_price - average_range)
        decision_bar_high = float(state.feature_snapshot.get("decision_bar_high") or raw_entry_price + average_range)
        risk = max(average_range * variant.stop_atr_multiple, 0.25)
        if state.side == "LONG":
            stop_price = decision_bar_low - risk
            target_price = entry_price + risk * variant.target_r_multiple if variant.target_r_multiple is not None else None
        else:
            stop_price = decision_bar_high + risk
            target_price = entry_price - risk * variant.target_r_multiple if variant.target_r_multiple is not None else None

        exit_bar = execution_window[-1]
        raw_exit_price = float(exit_bar.close)
        exit_price = raw_exit_price - slippage_points if state.side == "LONG" else raw_exit_price + slippage_points
        exit_reason = "time_stop"
        dynamic_stop_price = stop_price
        checkpoint_reached = False
        mfe_points = 0.0
        mae_points = 0.0
        bars_held = 0
        promotion_ts = None
        promotion_price = None
        pnl_points_at_promotion = None
        post_promotion_peak_open_profit_points = None
        last_completed_maintenance_bar_end_ts = None
        maintenance_state = None
        if policy_profile.get("use_promoted_maintenance_exit"):
            maintenance_state = maintenance_cache.setdefault(
                (instrument, int(policy_profile["maintenance_timeframe_minutes"])),
                _build_completed_maintenance_context(
                    minute_bars=candidate_bars,
                    timeframe_minutes=int(policy_profile["maintenance_timeframe_minutes"]),
                ),
            )

        for relative_index, bar in enumerate(execution_window, start=1):
            bars_held = relative_index
            latest_feature = _latest_feature_row_for_bar(
                feature_rows_by_instrument.get(state.instrument, ()),
                feature_ts_by_instrument.get(state.instrument, ()),
                bar.end_ts,
            )
            if state.side == "LONG":
                mfe_points = max(mfe_points, float(bar.high) - raw_entry_price)
                mae_points = max(mae_points, raw_entry_price - float(bar.low))
                stop_hit = float(bar.low) <= dynamic_stop_price
                target_hit = not checkpoint_reached and target_price is not None and float(bar.high) >= target_price
            else:
                mfe_points = max(mfe_points, raw_entry_price - float(bar.low))
                mae_points = max(mae_points, float(bar.high) - raw_entry_price)
                stop_hit = float(bar.high) >= dynamic_stop_price
                target_hit = not checkpoint_reached and target_price is not None and float(bar.low) <= target_price
            promotion_hit = (
                not checkpoint_reached
                and policy_profile.get("promotion_r_multiple") is not None
                and mfe_points >= risk * float(policy_profile["promotion_r_multiple"])
            )
            if promotion_hit:
                checkpoint_reached = True
                promotion_ts = bar.end_ts
                promotion_price = float(bar.close)
                pnl_points_at_promotion = (
                    promotion_price - entry_price if state.side == "LONG" else entry_price - promotion_price
                )
                post_promotion_peak_open_profit_points = max(
                    mfe_points,
                    (float(bar.high) - entry_price) if state.side == "LONG" else (entry_price - float(bar.low)),
                )
            if checkpoint_reached:
                if policy_profile.get("use_promoted_maintenance_exit"):
                    dynamic_stop_price = _promoted_maintenance_stop_price(
                        current_stop=dynamic_stop_price,
                        entry_fill_price=entry_price,
                        risk_points=risk,
                        side=state.side,
                    )
                else:
                    dynamic_stop_price = _checkpoint_stop_price(
                        current_stop=dynamic_stop_price,
                        entry_fill_price=entry_price,
                        risk_points=risk,
                        bar=bar,
                        side=state.side,
                    )
            if checkpoint_reached:
                current_open_profit_points = (
                    float(bar.high) - entry_price if state.side == "LONG" else entry_price - float(bar.low)
                )
                post_promotion_peak_open_profit_points = max(
                    float(post_promotion_peak_open_profit_points or 0.0),
                    current_open_profit_points,
                )
            if stop_hit and target_hit:
                exit_bar = bar
                raw_exit_price = dynamic_stop_price
                exit_price = dynamic_stop_price - slippage_points if state.side == "LONG" else dynamic_stop_price + slippage_points
                exit_reason = "stop_first_conflict"
                break
            if stop_hit:
                exit_bar = bar
                raw_exit_price = dynamic_stop_price
                exit_price = dynamic_stop_price - slippage_points if state.side == "LONG" else dynamic_stop_price + slippage_points
                exit_reason = "checkpoint_stop" if checkpoint_reached else "stop"
                break
            if target_hit and target_price is not None and not policy_profile.get("disable_target_exit_after_entry", False):
                if policy_profile["use_target_checkpoint"] and _checkpoint_feature_is_healthy(feature=latest_feature, side=state.side):
                    checkpoint_reached = True
                    promotion_ts = bar.end_ts
                    promotion_price = target_price
                    pnl_points_at_promotion = (
                        target_price - entry_price if state.side == "LONG" else entry_price - target_price
                    )
                    post_promotion_peak_open_profit_points = max(
                        float(post_promotion_peak_open_profit_points or 0.0),
                        pnl_points_at_promotion,
                    )
                    dynamic_stop_price = _checkpoint_stop_price(
                        current_stop=dynamic_stop_price,
                        entry_fill_price=entry_price,
                        risk_points=risk,
                        bar=bar,
                        side=state.side,
                    )
                    target_price = None
                else:
                    exit_bar = bar
                    raw_exit_price = target_price
                    exit_price = target_price - slippage_points if state.side == "LONG" else target_price + slippage_points
                    exit_reason = "target"
                    break
            if (
                checkpoint_reached
                and policy_profile["use_target_checkpoint"]
                and not policy_profile.get("use_promoted_maintenance_exit")
                and not _checkpoint_feature_is_healthy(feature=latest_feature, side=state.side)
            ):
                exit_bar = bar
                raw_exit_price = float(bar.close)
                exit_price = raw_exit_price - slippage_points if state.side == "LONG" else raw_exit_price + slippage_points
                exit_reason = "target_momentum_fade"
                break
            if (
                not checkpoint_reached
                and policy_profile["use_no_traction_abort"]
                and relative_index >= ATP_REPLAY_NO_TRACTION_ABORT_BARS
                and mfe_points < (risk * ATP_REPLAY_NO_TRACTION_MIN_FAVORABLE_R)
            ):
                exit_bar = bar
                raw_exit_price = float(bar.close)
                exit_price = raw_exit_price - slippage_points if state.side == "LONG" else raw_exit_price + slippage_points
                exit_reason = "no_traction_abort"
                break
            if checkpoint_reached and policy_profile.get("use_promoted_maintenance_exit") and maintenance_state is not None:
                maintenance_bar = maintenance_state["bars_by_end_ts"].get(bar.end_ts)
                if (
                    maintenance_bar is not None
                    and bar.end_ts != last_completed_maintenance_bar_end_ts
                    and promotion_ts is not None
                    and maintenance_bar.end_ts >= promotion_ts
                ):
                    if _maintenance_exit_triggered(
                        maintenance_bar=maintenance_bar,
                        maintenance_bars=maintenance_state["bars"],
                        maintenance_ema_by_end_ts=maintenance_state["ema_by_end_ts"],
                        side=state.side,
                        policy_profile=policy_profile,
                        mfe_points=mfe_points,
                        risk_points=risk,
                    ):
                        last_completed_maintenance_bar_end_ts = bar.end_ts
                        exit_bar = bar
                        raw_exit_price = float(bar.close)
                        exit_price = raw_exit_price - slippage_points if state.side == "LONG" else raw_exit_price + slippage_points
                        exit_reason = str(policy_profile["maintenance_exit_reason"])
                        break
                    last_completed_maintenance_bar_end_ts = bar.end_ts

        gross_pnl_points = (raw_exit_price - raw_entry_price) if state.side == "LONG" else (raw_entry_price - raw_exit_price)
        pnl_points = (exit_price - entry_price) if state.side == "LONG" else (entry_price - exit_price)
        gross_pnl_cash = gross_pnl_points * point_value
        pnl_cash = pnl_points * point_value - fee_per_trade
        slippage_cost = max(gross_pnl_cash - pnl_points * point_value, 0.0)
        trades.append(
            TradeRecord(
                instrument=state.instrument,
                variant_id=variant_for_state.variant_id,
                family=variant_for_state.family,
                side=state.side,
                live_eligible=False,
                shadow_only=True,
                conflict_outcome=ConflictOutcome.NO_CONFLICT,
                decision_id=f"{state.instrument}|{variant_for_state.variant_id}|{state.decision_ts.isoformat()}",
                decision_ts=state.decision_ts,
                entry_ts=entry_bar.end_ts,
                exit_ts=exit_bar.end_ts,
                entry_price=entry_price,
                exit_price=exit_price,
                stop_price=dynamic_stop_price,
                target_price=target_price,
                pnl_points=pnl_points,
                gross_pnl_cash=gross_pnl_cash,
                pnl_cash=pnl_cash,
                fees_paid=fee_per_trade,
                slippage_cost=slippage_cost,
                mfe_points=mfe_points,
                mae_points=mae_points,
                bars_held_1m=bars_held,
                hold_minutes=float(bars_held),
                exit_reason=exit_reason,
                is_reentry=reentry_type != "NONE",
                reentry_type=reentry_type,
                stopout=exit_reason in {"stop", "stop_first_conflict"},
                setup_signature=setup_signature,
                setup_quality_bucket=str(state.feature_snapshot.get("setup_quality_bucket") or "MEDIUM"),
                session_segment=state.session_segment,
                regime_bucket=str(state.feature_snapshot.get("regime_bucket") or "UNKNOWN"),
                volatility_bucket=str(state.feature_snapshot.get("volatility_bucket") or "UNKNOWN"),
                participation_promoted=checkpoint_reached and promotion_ts is not None,
                promotion_trigger_r_multiple=(
                    float(policy_profile["promotion_r_multiple"])
                    if checkpoint_reached and policy_profile.get("promotion_r_multiple") is not None
                    else None
                ),
                promotion_ts=promotion_ts,
                promotion_price=promotion_price,
                pnl_points_at_promotion=pnl_points_at_promotion,
                post_promotion_peak_open_profit_points=post_promotion_peak_open_profit_points,
            )
        )
        blocked_until_by_instrument[instrument] = trades[-1].exit_ts + timedelta(
            minutes=variant_for_state.local_cooldown_bars_1m
        )
        last_trade_by_setup[(instrument, setup_signature)] = {
            "exit_ts": trades[-1].exit_ts,
            "setup_state_signature": setup_state_signature,
            "exit_reason": exit_reason,
            "decision_ts": state.decision_ts,
        }
    return trades


def _latest_feature_row_for_bar(
    feature_rows: Sequence[FeatureState],
    feature_timestamps: Sequence[Any],
    bar_ts: Any,
) -> FeatureState | None:
    if not feature_rows or not feature_timestamps:
        return None
    index = bisect_right(feature_timestamps, bar_ts) - 1
    if index < 0:
        return None
    return feature_rows[index]


def _checkpoint_feature_is_healthy(*, feature: FeatureState | None, side: str) -> bool:
    if feature is None:
        return False
    normalized_side = str(side or "LONG").strip().upper()
    if normalized_side == "LONG":
        trend_ok = feature.trend_state in {"UP", "STRONG_UP"}
        momentum_ok = feature.momentum_persistence in {"PERSISTENT_UP", "MIXED"}
        agreement_ok = feature.mtf_agreement_state in {"ALIGNED_UP", "MIXED"}
        anatomy_ok = feature.bar_anatomy in {"BULL_IMPULSE", "BALANCED", "LOWER_REJECTION"}
        reference_ok = feature.reference_state in {"ABOVE_SESSION_OPEN", "MID_RANGE", "NEAR_RECENT_HIGH"}
        expansion_ok = feature.expansion_state in {"NORMAL", "EXPANDED"}
        direction_ok = feature.direction_bias == "LONG_BIAS"
    else:
        trend_ok = feature.trend_state in {"DOWN", "STRONG_DOWN"}
        momentum_ok = feature.momentum_persistence in {"PERSISTENT_DOWN", "MIXED"}
        agreement_ok = feature.mtf_agreement_state in {"ALIGNED_DOWN", "MIXED"}
        anatomy_ok = feature.bar_anatomy in {"BEAR_IMPULSE", "BALANCED", "UPPER_REJECTION"}
        reference_ok = feature.reference_state in {"BELOW_SESSION_OPEN", "MID_RANGE", "NEAR_RECENT_LOW"}
        expansion_ok = feature.expansion_state in {"NORMAL", "EXPANDED"}
        direction_ok = feature.direction_bias == "SHORT_BIAS"
    return direction_ok and trend_ok and agreement_ok and sum(
        1 for value in (momentum_ok, anatomy_ok, reference_ok, expansion_ok) if value
    ) >= 3


def _checkpoint_stop_price(
    *,
    current_stop: float,
    entry_fill_price: float,
    risk_points: float,
    bar: Any,
    side: str,
) -> float:
    normalized_side = str(side or "LONG").strip().upper()
    if normalized_side == "LONG":
        locked_profit_stop = entry_fill_price + risk_points * ATP_REPLAY_CHECKPOINT_LOCK_R
        structure_stop = float(bar.low) - risk_points * ATP_REPLAY_CHECKPOINT_TRAIL_R
        return max(current_stop, locked_profit_stop, structure_stop)
    locked_profit_stop = entry_fill_price - risk_points * ATP_REPLAY_CHECKPOINT_LOCK_R
    structure_stop = float(bar.high) + risk_points * ATP_REPLAY_CHECKPOINT_TRAIL_R
    return min(current_stop, locked_profit_stop, structure_stop)


def _promoted_maintenance_stop_price(
    *,
    current_stop: float,
    entry_fill_price: float,
    risk_points: float,
    side: str,
) -> float:
    normalized_side = str(side or "LONG").strip().upper()
    if normalized_side == "LONG":
        return max(current_stop, entry_fill_price + risk_points * ATP_REPLAY_CHECKPOINT_LOCK_R)
    return min(current_stop, entry_fill_price - risk_points * ATP_REPLAY_CHECKPOINT_LOCK_R)


def _classify_timing_reentry_type(
    *,
    state: AtpTimingState,
    variant: PatternVariant,
    previous_same_setup: dict[str, Any] | None,
) -> str:
    if previous_same_setup is None or not variant.allow_reentry:
        return "NONE"
    previous_exit_ts = previous_same_setup["exit_ts"]
    previous_state_signature = str(previous_same_setup["setup_state_signature"])
    previous_exit_reason = str(previous_same_setup["exit_reason"])
    current_state_signature = str(state.feature_snapshot.get("setup_state_signature") or state.feature_snapshot.get("setup_signature") or "")
    minutes_since_exit = max((state.decision_ts - previous_exit_ts).total_seconds() / 60.0, 0.0)
    if (
        current_state_signature != previous_state_signature
        and minutes_since_exit >= max(variant.local_cooldown_bars_1m, 0)
    ):
        return "STRUCTURAL_RESET"
    if previous_exit_reason in {"stop", "stop_first_conflict"} and minutes_since_exit <= 10.0:
        return "LOCAL_CHURN"
    return "LOCAL_CHURN"


def _execution_window_for_policy(
    *,
    candidate_bars: Sequence[Any],
    entry_index: int,
    state: AtpTimingState,
    policy_profile: Mapping[str, Any],
    base_max_hold_bars_1m: int,
) -> list[Any]:
    if policy_profile.get("use_promoted_maintenance_exit"):
        entry_bar = candidate_bars[entry_index]
        entry_session = str(getattr(entry_bar, "session_segment", None) or state.session_segment or "")
        bounded: list[Any] = []
        for bar in candidate_bars[entry_index:]:
            if bounded and str(getattr(bar, "session_segment", None) or "") != entry_session:
                break
            bounded.append(bar)
        return bounded
    limit = policy_profile["max_hold_bars_1m"](base_max_hold_bars_1m) + 1
    return list(candidate_bars[entry_index : entry_index + limit])


def _build_completed_maintenance_context(
    *,
    minute_bars: Sequence[Any],
    timeframe_minutes: int,
) -> dict[str, Any]:
    grouped: dict[datetime, list[Any]] = {}
    for bar in minute_bars:
        grouped.setdefault(_maintenance_bucket_end(bar.end_ts, timeframe_minutes), []).append(bar)
    completed_bars: list[ResearchBar] = []
    for bucket_end in sorted(grouped):
        bucket = sorted(grouped[bucket_end], key=lambda item: item.end_ts)
        if len(bucket) != timeframe_minutes:
            continue
        if any(
            bucket[index].end_ts - bucket[index - 1].end_ts != timedelta(minutes=1)
            for index in range(1, len(bucket))
        ):
            continue
        first_bar = bucket[0]
        last_bar = bucket[-1]
        completed_bars.append(
            ResearchBar(
                instrument=str(first_bar.instrument),
                timeframe=f"{timeframe_minutes}m",
                start_ts=first_bar.start_ts,
                end_ts=bucket_end,
                open=float(first_bar.open),
                high=max(float(item.high) for item in bucket),
                low=min(float(item.low) for item in bucket),
                close=float(last_bar.close),
                volume=sum(int(item.volume) for item in bucket),
                session_label=str(getattr(last_bar, "session_label", "")),
                session_segment=str(getattr(last_bar, "session_segment", "")),
                source=str(getattr(last_bar, "source", "sqlite")),
            )
        )
    ema_values = rolling_ema([float(bar.close) for bar in completed_bars], span=ATP_REPLAY_MAINTENANCE_FAST_EMA_SPAN)
    return {
        "bars": completed_bars,
        "bars_by_end_ts": {bar.end_ts: bar for bar in completed_bars},
        "ema_by_end_ts": {bar.end_ts: ema_values[index] for index, bar in enumerate(completed_bars)},
    }


def _maintenance_bucket_end(timestamp: datetime, target_minutes: int) -> datetime:
    utc_ts = timestamp.astimezone(UTC)
    epoch_minutes = int(utc_ts.timestamp() // 60)
    bucket = ((epoch_minutes + target_minutes - 1) // target_minutes) * target_minutes
    return datetime.fromtimestamp(bucket * 60, tz=UTC)


def _maintenance_exit_triggered(
    *,
    maintenance_bar: ResearchBar,
    maintenance_bars: Sequence[ResearchBar],
    maintenance_ema_by_end_ts: Mapping[datetime, float],
    side: str,
    policy_profile: Mapping[str, Any],
    mfe_points: float,
    risk_points: float,
) -> bool:
    if str(side or "LONG").upper() != "LONG":
        return False
    previous_bar = next(
        (candidate for candidate in reversed(maintenance_bars) if candidate.end_ts < maintenance_bar.end_ts),
        None,
    )
    if previous_bar is None:
        return False
    maintenance_rule = str(policy_profile.get("maintenance_rule") or "")
    ema_value = float(maintenance_ema_by_end_ts.get(maintenance_bar.end_ts, maintenance_bar.close))
    extension_context_active = mfe_points >= risk_points * (
        float(policy_profile.get("promotion_r_multiple") or 0.0) + ATP_REPLAY_PROMOTION_EXTENSION_R
    )
    lower_half = float(maintenance_bar.close) <= float(maintenance_bar.low) + (maintenance_bar.range_points * 0.5)
    lower_third = float(maintenance_bar.close) <= float(maintenance_bar.low) + (maintenance_bar.range_points / 3.0)
    close_below_ema = float(maintenance_bar.close) < ema_value
    close_below_prior = float(maintenance_bar.close) < float(previous_bar.close)
    if maintenance_rule == "15m_2of3":
        conditions = (
            close_below_ema,
            close_below_prior,
            extension_context_active and lower_half,
        )
        return sum(1 for condition in conditions if condition) >= 2
    if maintenance_rule == "7m_2of3":
        conditions = (
            close_below_ema,
            close_below_prior,
            extension_context_active and lower_half,
        )
        return sum(1 for condition in conditions if condition) >= 2
    if maintenance_rule == "10m_double_weak_close":
        current_weak = close_below_ema and close_below_prior
        decisive_weak = current_weak and lower_third
        previous_ema = float(maintenance_ema_by_end_ts.get(previous_bar.end_ts, previous_bar.close))
        prior_of_previous = next(
            (candidate for candidate in reversed(maintenance_bars) if candidate.end_ts < previous_bar.end_ts),
            None,
        )
        previous_weak = (
            prior_of_previous is not None
            and float(previous_bar.close) < previous_ema
            and float(previous_bar.close) < float(prior_of_previous.close)
        )
        return decisive_weak or (current_weak and previous_weak)
    if maintenance_rule == "7m_strict_weak_close":
        current_weak = close_below_ema and close_below_prior
        decisive_weak = current_weak and lower_third
        previous_ema = float(maintenance_ema_by_end_ts.get(previous_bar.end_ts, previous_bar.close))
        prior_of_previous = next(
            (candidate for candidate in reversed(maintenance_bars) if candidate.end_ts < previous_bar.end_ts),
            None,
        )
        previous_weak = (
            prior_of_previous is not None
            and float(previous_bar.close) < previous_ema
            and float(previous_bar.close) < float(prior_of_previous.close)
        )
        return decisive_weak or (current_weak and previous_weak)
    if maintenance_rule == "7m_loose_hold":
        return close_below_ema and close_below_prior and extension_context_active and lower_half
    if maintenance_rule == "20m_loose_trend_hold":
        return close_below_ema and close_below_prior and lower_third
    return False


def _replay_exit_policy_profile(policy_name: str) -> dict[str, Any]:
    normalized = str(policy_name or ATP_REPLAY_EXIT_POLICY_FIXED_TARGET).strip().lower()
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_15M_2OF3:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 0.75,
            "maintenance_timeframe_minutes": 15,
            "maintenance_rule": "15m_2of3",
            "maintenance_exit_reason": "htf_15m_2of3_deterioration",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_15M_2OF3:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 1.0,
            "maintenance_timeframe_minutes": 15,
            "maintenance_rule": "15m_2of3",
            "maintenance_exit_reason": "htf_15m_2of3_deterioration",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_10M_DOUBLE_WEAK:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 0.75,
            "maintenance_timeframe_minutes": 10,
            "maintenance_rule": "10m_double_weak_close",
            "maintenance_exit_reason": "htf_10m_double_weak_close",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_2OF3:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 1.0,
            "maintenance_timeframe_minutes": 7,
            "maintenance_rule": "7m_2of3",
            "maintenance_exit_reason": "htf_7m_2of3_deterioration",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_STRICT_WEAK:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 1.0,
            "maintenance_timeframe_minutes": 7,
            "maintenance_rule": "7m_strict_weak_close",
            "maintenance_exit_reason": "htf_7m_strict_weak_close",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT100_7M_LOOSE_HOLD:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 1.0,
            "maintenance_timeframe_minutes": 7,
            "maintenance_rule": "7m_loose_hold",
            "maintenance_exit_reason": "htf_7m_loose_hold",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_CHECKPOINT075_20M_LOOSE:
        return {
            "use_target_checkpoint": False,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": True,
            "disable_target_exit_after_entry": True,
            "promotion_r_multiple": 0.75,
            "maintenance_timeframe_minutes": 20,
            "maintenance_rule": "20m_loose_trend_hold",
            "maintenance_exit_reason": "htf_20m_loose_trend_hold",
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT_LONG_HOLD:
        return {
            "use_target_checkpoint": True,
            "use_no_traction_abort": False,
            "use_promoted_maintenance_exit": False,
            "disable_target_exit_after_entry": False,
            "promotion_r_multiple": None,
            "max_hold_bars_1m": lambda base: max(int(base), 1) + ATP_REPLAY_LONG_HOLD_EXTENSION_BARS,
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT_NO_TRACTION:
        return {
            "use_target_checkpoint": True,
            "use_no_traction_abort": True,
            "use_promoted_maintenance_exit": False,
            "disable_target_exit_after_entry": False,
            "promotion_r_multiple": None,
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    if normalized == ATP_REPLAY_EXIT_POLICY_TARGET_CHECKPOINT:
        return {
            "use_target_checkpoint": True,
            "use_no_traction_abort": False,
            "use_promoted_maintenance_exit": False,
            "disable_target_exit_after_entry": False,
            "promotion_r_multiple": None,
            "max_hold_bars_1m": lambda base: max(int(base), 1),
        }
    return {
        "use_target_checkpoint": False,
        "use_no_traction_abort": False,
        "use_promoted_maintenance_exit": False,
        "disable_target_exit_after_entry": False,
        "promotion_r_multiple": None,
        "max_hold_bars_1m": lambda base: max(int(base), 1),
    }


def overlay_position_blocks(
    *,
    timing_states: Sequence[AtpTimingState],
    trades: Sequence[TradeRecord],
) -> list[AtpTimingState]:
    active_windows = sorted((trade.entry_ts, trade.exit_ts) for trade in trades)
    updated: list[AtpTimingState] = []
    for state in timing_states:
        if not state.executable_entry or state.entry_ts is None:
            updated.append(state)
            continue
        overlap = next(
            (
                window
                for window in active_windows
                if window[0] < state.entry_ts < window[1]
            ),
            None,
        )
        if overlap is None:
            updated.append(replace(state, entry_executed=True))
            continue
        blockers = list(state.blocker_codes)
        blockers.extend(code for code in ("ATP_POSITION_NOT_FLAT", "ATP_ONE_POSITION_BASELINE_BLOCK") if code not in blockers)
        updated.append(
            replace(
                state,
                timing_state=ATP_TIMING_UNAVAILABLE,
                blocker_codes=tuple(blockers),
                primary_blocker="ATP_POSITION_NOT_FLAT",
                executable_entry=False,
                entry_executed=False,
                setup_armed_but_not_executable=True,
            )
        )
    return updated


def classify_vwap_price_quality(
    *,
    side: str,
    entry_price: float,
    bar_vwap: float,
    band_reference: float,
) -> str:
    neutral_band = max(float(band_reference), 1e-9) * TIMING_NEUTRAL_VWAP_BAND
    if side == "LONG":
        if entry_price <= bar_vwap:
            return VWAP_FAVORABLE
        if entry_price <= bar_vwap + neutral_band:
            return VWAP_NEUTRAL
        return VWAP_CHASE_RISK
    if entry_price >= bar_vwap:
        return VWAP_FAVORABLE
    if entry_price >= bar_vwap - neutral_band:
        return VWAP_NEUTRAL
    return VWAP_CHASE_RISK


def latest_atp_timing_state_summary(timing_state: AtpTimingState | None) -> dict[str, Any]:
    if timing_state is None:
        return {
            "family_name": ATP_V1_LONG_CONTINUATION_FAMILY,
            "side": "LONG",
            "timing_state": ATP_TIMING_UNAVAILABLE,
            "vwap_price_quality_state": VWAP_NEUTRAL,
            "primary_blocker": ATP_TIMING_5M_CONTEXT_NOT_READY,
            "blocker_codes": [ATP_TIMING_5M_CONTEXT_NOT_READY],
            "entry_executed": False,
        }
    return {
        "family_name": timing_state.family_name,
        "side": timing_state.side,
        "context_entry_state": timing_state.context_entry_state,
        "timing_state": timing_state.timing_state,
        "vwap_price_quality_state": timing_state.vwap_price_quality_state,
        "primary_blocker": timing_state.primary_blocker,
        "blocker_codes": list(timing_state.blocker_codes),
        "setup_armed": timing_state.setup_armed,
        "timing_confirmed": timing_state.timing_confirmed,
        "executable_entry": timing_state.executable_entry,
        "setup_armed_but_not_executable": timing_state.setup_armed_but_not_executable,
        "entry_executed": timing_state.entry_executed,
        "timing_bar_ts": timing_state.timing_bar_ts.isoformat() if timing_state.timing_bar_ts else None,
        "entry_ts": timing_state.entry_ts.isoformat() if timing_state.entry_ts else None,
        "entry_price": timing_state.entry_price,
    }


def summarize_phase3_timing_diagnostics(
    *,
    timing_states: Sequence[AtpTimingState],
    trades: Sequence[TradeRecord],
    old_proxy_trade_count: int,
    variant_overrides: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    total = len(timing_states)
    ready_rows = [state for state in timing_states if state.context_entry_state == ENTRY_ELIGIBLE]
    timing_confirmed_rows = [state for state in ready_rows if state.timing_confirmed]
    executed_rows = [state for state in ready_rows if state.entry_executed]
    vwap_counter = Counter(state.vwap_price_quality_state for state in ready_rows)
    primary_blockers = Counter(state.primary_blocker for state in timing_states if state.primary_blocker)
    session_breakdown: dict[str, Any] = {}
    for session in sorted({state.session_segment for state in timing_states}):
        session_rows = [state for state in timing_states if state.session_segment == session]
        ready_session = [state for state in session_rows if state.context_entry_state == ENTRY_ELIGIBLE]
        confirmed_session = [state for state in ready_session if state.timing_confirmed]
        executed_session = [state for state in ready_session if state.entry_executed]
        session_breakdown[session] = {
            "ready_5m_bars": len(ready_session),
            "timing_confirmed": len(confirmed_session),
            "executed_entries": len(executed_session),
            "ready_to_executed_percent": _percent(len(executed_session), len(ready_session)),
        }
    reference_state = timing_states[0] if timing_states else None
    reference_variant = atp_phase2_variant(
        reference_state.side if reference_state is not None else "LONG",
        variant_overrides=variant_overrides,
    )
    return {
        "family_name": reference_variant.family,
        "timing_bar_count": total,
        "ready_5m_bars_count": len(ready_rows),
        "timing_confirmed_count": len(timing_confirmed_rows),
        "executed_entry_count": len(executed_rows),
        "conversion_rates": {
            "ready_to_timing_confirmed": _percent(len(timing_confirmed_rows), len(ready_rows)),
            "timing_confirmed_to_executed": _percent(len(executed_rows), len(timing_confirmed_rows)),
            "ready_to_executed": _percent(len(executed_rows), len(ready_rows)),
        },
        "timing_state_percent": _percentages(Counter(state.timing_state for state in timing_states), total),
        "vwap_price_quality_percent_on_ready": _percentages(vwap_counter, len(ready_rows)),
        "top_timing_blockers": [
            {"code": code, "count": count, "percent": _percent(count, total)}
            for code, count in primary_blockers.most_common(8)
        ],
        "top_vwap_price_quality_blockers": [
            {"code": code, "count": count}
            for code, count in Counter(
                state.primary_blocker
                for state in timing_states
                if state.primary_blocker == ATP_TIMING_VWAP_CHASE_RISK
            ).most_common(4)
        ],
        "session_breakdown": session_breakdown,
        "old_proxy_comparison": {
            "old_proxy_executed_trade_count": old_proxy_trade_count,
            "phase3_executed_trade_count": len(trades),
            "executed_trade_delta": len(trades) - old_proxy_trade_count,
        },
    }


def render_phase3_timing_diagnostics_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# ATP Phase 3 Timing Diagnostics",
        "",
        f"- Ready 5m bars: `{payload.get('ready_5m_bars_count')}`",
        f"- Timing confirmed: `{payload.get('timing_confirmed_count')}`",
        f"- Executed entries: `{payload.get('executed_entry_count')}`",
        f"- Ready -> executed: `{payload.get('conversion_rates', {}).get('ready_to_executed')}`%",
        "",
        "## Top Timing Blockers",
    ]
    blockers = payload.get("top_timing_blockers") or []
    if not blockers:
        lines.append("- None")
    else:
        for row in blockers:
            lines.append(f"- `{row['code']}` count=`{row['count']}` percent=`{row['percent']}`")
    return "\n".join(lines) + "\n"


def write_phase3_artifacts(
    *,
    reports_dir: Path,
    diagnostics: dict[str, Any],
) -> tuple[Path, Path]:
    json_path = reports_dir / "atp_phase3_timing_diagnostics.json"
    markdown_path = reports_dir / "atp_phase3_timing_diagnostics.md"
    json_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(render_phase3_timing_diagnostics_markdown(diagnostics), encoding="utf-8")
    return json_path, markdown_path


def _timing_window_bars(
    *,
    state: AtpEntryState,
    minute_bars: Sequence[Any],
    entry_activation_basis: str,
    variant_overrides: Mapping[str, Any] | None = None,
) -> list[Any]:
    minute_end_timestamps = [bar.end_ts for bar in minute_bars]
    variant = atp_phase2_variant(state.side, variant_overrides=variant_overrides)
    if entry_activation_basis == ATP_TIMING_ACTIVATION_ROLLING_5M:
        start_index = bisect_left(minute_end_timestamps, state.decision_ts)
        return list(minute_bars[start_index : start_index + variant.entry_window_bars_1m])
    start_index = bisect_right(minute_end_timestamps, state.decision_ts)
    return list(minute_bars[start_index : start_index + variant.entry_window_bars_1m])


def _minute_fast_ema_map(bars: Sequence[Any]) -> dict[Any, float]:
    if not bars:
        return {}
    ema_values = rolling_ema([float(bar.close) for bar in bars], span=MINUTE_FAST_EMA_SPAN)
    return {bar.end_ts: ema_values[index] for index, bar in enumerate(bars)}


def _pivot_price(state: AtpEntryState) -> float:
    setup_high = float(state.feature_snapshot.get("setup_bar_high") or state.feature_snapshot.get("decision_bar_high") or 0.0)
    setup_low = float(state.feature_snapshot.get("setup_bar_low") or state.feature_snapshot.get("decision_bar_low") or 0.0)
    decision_close = float(state.feature_snapshot.get("decision_bar_close") or setup_high)
    if state.side == "SHORT":
        return min(setup_low, decision_close)
    return max(setup_high, decision_close)


def _invalidation_price(state: AtpEntryState) -> float:
    setup_low = float(state.feature_snapshot.get("setup_bar_low") or state.feature_snapshot.get("decision_bar_low") or 0.0)
    setup_high = float(state.feature_snapshot.get("setup_bar_high") or state.feature_snapshot.get("decision_bar_high") or 0.0)
    decision_low = float(state.feature_snapshot.get("decision_bar_low") or setup_low)
    decision_high = float(state.feature_snapshot.get("decision_bar_high") or setup_high)
    if state.side == "SHORT":
        return max(setup_high, decision_high)
    return min(setup_low, decision_low)


def _bar_vwap(bar: Any) -> float:
    return (float(bar.high) + float(bar.low) + float(bar.close)) / 3.0


def _timing_state(
    *,
    state: AtpEntryState,
    timing_state: str,
    vwap_price_quality_state: str,
    blocker_codes: tuple[str, ...],
    primary_blocker: str | None,
    timing_confirmed: bool = False,
    executable_entry: bool = False,
    invalidated_before_entry: bool = False,
    timing_bar_ts=None,
    entry_ts=None,
    entry_price=None,
    feature_snapshot: dict[str, Any],
) -> AtpTimingState:
    return AtpTimingState(
        instrument=state.instrument,
        decision_ts=state.decision_ts,
        session_date=state.session_date,
        session_segment=state.session_segment,
        family_name=state.family_name,
        context_entry_state=state.entry_state,
        timing_state=timing_state,
        vwap_price_quality_state=vwap_price_quality_state,
        blocker_codes=blocker_codes,
        primary_blocker=primary_blocker,
        setup_armed=state.entry_state == ENTRY_ELIGIBLE,
        timing_confirmed=timing_confirmed,
        executable_entry=executable_entry,
        invalidated_before_entry=invalidated_before_entry,
        setup_armed_but_not_executable=state.entry_state == ENTRY_ELIGIBLE and not executable_entry,
        entry_executed=False,
        timing_bar_ts=timing_bar_ts,
        entry_ts=entry_ts,
        entry_price=entry_price,
        feature_snapshot={
            **feature_snapshot,
            "setup_signature": state.setup_signature,
            "setup_state_signature": state.setup_state_signature,
            "setup_quality_bucket": state.setup_quality_bucket,
            "decision_bar_low": state.feature_snapshot.get("decision_bar_low"),
            "decision_bar_high": state.feature_snapshot.get("decision_bar_high"),
            "average_range": state.feature_snapshot.get("average_range"),
            "regime_bucket": state.feature_snapshot.get("regime_bucket"),
            "volatility_bucket": state.feature_snapshot.get("volatility_bucket"),
        },
        side=state.side,
    )


def _percent(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 4)


def _percentages(counter: Counter, total: int) -> dict[str, float]:
    if total <= 0:
        return {}
    return {
        str(key): round((value / total) * 100.0, 4)
        for key, value in sorted(counter.items(), key=lambda item: str(item[0]))
    }
