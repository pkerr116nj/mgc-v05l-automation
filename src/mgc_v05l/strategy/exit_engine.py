"""Exit engine contracts."""

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from ..config_models import StrategySettings
from ..domain.enums import ExitReason, LongEntryFamily, PositionSide, ShortEntryFamily
from ..domain.models import Bar, FeaturePacket, StrategyState
from .risk_engine import RiskContext


@dataclass(frozen=True)
class ExitDecision:
    long_exit: bool
    short_exit: bool
    primary_reason: Optional[ExitReason]
    all_true_reasons: tuple[ExitReason, ...]
    k_long_integrity_lost: bool
    vwap_lost: bool
    vwap_weak_follow_through: bool
    short_integrity_lost: bool
    short_entry_family: ShortEntryFamily
    short_entry_source: Optional[str]
    additive_short_max_favorable_excursion: Decimal
    additive_short_peak_threshold_reached: bool
    additive_short_giveback_from_peak: Decimal


def evaluate_exits(
    history: Sequence[Bar],
    features: FeaturePacket,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
) -> ExitDecision:
    """Evaluate family-specific exit conditions."""
    if not history:
        raise ValueError("history must include at least one finalized bar.")

    current_bar = history[-1]
    previous_bar = history[-2] if len(history) >= 2 else current_bar

    k_long_integrity_lost = (
        current_bar.close <= previous_bar.close
        and current_bar.high <= previous_bar.high + Decimal("0.08") * features.atr
        and not features.bull_close_strong
    )
    vwap_lost = current_bar.close < features.vwap
    vwap_weak_follow_through = (
        state.bars_in_trade >= settings.vwap_weak_close_lookback_bars
        and current_bar.close <= previous_bar.close
        and current_bar.close < features.turn_ema_fast
    )
    short_integrity_lost = (
        current_bar.close >= previous_bar.close
        and current_bar.low >= previous_bar.low - Decimal("0.08") * features.atr
        and not features.bear_close_weak
    )

    if state.position_side == PositionSide.LONG:
        if state.long_entry_family == LongEntryFamily.VWAP:
            reasons = _vwap_long_reasons(
                current_bar,
                features,
                previous_bar,
                state,
                risk_context,
                settings,
                vwap_lost,
                vwap_weak_follow_through,
            )
        else:
            reasons = _k_long_reasons(
                current_bar,
                features,
                previous_bar,
                state,
                risk_context,
                settings,
                k_long_integrity_lost,
            )
        return ExitDecision(
            long_exit=bool(reasons),
            short_exit=False,
            primary_reason=reasons[0] if reasons else None,
            all_true_reasons=tuple(reasons),
            k_long_integrity_lost=k_long_integrity_lost,
            vwap_lost=vwap_lost,
            vwap_weak_follow_through=vwap_weak_follow_through,
            short_integrity_lost=short_integrity_lost,
            short_entry_family=state.short_entry_family,
            short_entry_source=state.short_entry_source,
            additive_short_max_favorable_excursion=state.additive_short_max_favorable_excursion,
            additive_short_peak_threshold_reached=state.additive_short_peak_threshold_reached,
            additive_short_giveback_from_peak=state.additive_short_giveback_from_peak,
        )

    if state.position_side == PositionSide.SHORT:
        reasons = _short_reasons(
            current_bar,
            features,
            previous_bar,
            state,
            risk_context,
            settings,
            short_integrity_lost,
        )
        return ExitDecision(
            long_exit=False,
            short_exit=bool(reasons),
            primary_reason=reasons[0] if reasons else None,
            all_true_reasons=tuple(reasons),
            k_long_integrity_lost=k_long_integrity_lost,
            vwap_lost=vwap_lost,
            vwap_weak_follow_through=vwap_weak_follow_through,
            short_integrity_lost=short_integrity_lost,
            short_entry_family=state.short_entry_family,
            short_entry_source=state.short_entry_source,
            additive_short_max_favorable_excursion=state.additive_short_max_favorable_excursion,
            additive_short_peak_threshold_reached=state.additive_short_peak_threshold_reached,
            additive_short_giveback_from_peak=state.additive_short_giveback_from_peak,
        )

    return ExitDecision(
        long_exit=False,
        short_exit=False,
        primary_reason=None,
        all_true_reasons=tuple(),
        k_long_integrity_lost=k_long_integrity_lost,
        vwap_lost=vwap_lost,
        vwap_weak_follow_through=vwap_weak_follow_through,
        short_integrity_lost=short_integrity_lost,
        short_entry_family=state.short_entry_family,
        short_entry_source=state.short_entry_source,
        additive_short_max_favorable_excursion=state.additive_short_max_favorable_excursion,
        additive_short_peak_threshold_reached=state.additive_short_peak_threshold_reached,
        additive_short_giveback_from_peak=state.additive_short_giveback_from_peak,
    )

def _k_long_reasons(
    current_bar: Bar,
    features: FeaturePacket,
    previous_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
    k_long_integrity_lost: bool,
) -> list[ExitReason]:
    reasons: list[ExitReason] = []
    if risk_context.active_long_stop_ref is not None and current_bar.close < risk_context.active_long_stop_ref:
        reasons.append(ExitReason.LONG_STOP)
    if settings.use_long_swing_exit and state.last_swing_low is not None and current_bar.close < state.last_swing_low:
        reasons.append(ExitReason.LONG_SWING_FAIL)
    if settings.use_long_integrity_exit and k_long_integrity_lost:
        reasons.append(ExitReason.LONG_INTEGRITY_FAIL)
    if settings.use_long_fast_ema_exit and current_bar.close < features.turn_ema_fast:
        reasons.append(ExitReason.LONG_EMA_EXIT)
    if (
        settings.take_profit_at_r > 0
        and state.entry_price is not None
        and risk_context.long_risk is not None
        and current_bar.high >= state.entry_price + settings.take_profit_at_r * risk_context.long_risk
    ):
        reasons.append(ExitReason.LONG_R_TARGET_EXIT)
    if _long_derivative_maturity_exit(current_bar, features, previous_bar, state, risk_context, settings):
        reasons.append(ExitReason.LONG_DERIVATIVE_MATURITY_EXIT)
    if settings.use_long_time_exit and state.bars_in_trade >= settings.max_bars_long:
        reasons.append(ExitReason.LONG_TIME_EXIT)
    return reasons


def _vwap_long_reasons(
    current_bar: Bar,
    features: FeaturePacket,
    previous_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
    vwap_lost: bool,
    vwap_weak_follow_through: bool,
) -> list[ExitReason]:
    reasons: list[ExitReason] = []
    if risk_context.active_long_stop_ref is not None and current_bar.close < risk_context.active_long_stop_ref:
        reasons.append(ExitReason.LONG_STOP)
    if settings.use_vwap_hard_loss_exit and vwap_lost:
        reasons.append(ExitReason.VWAP_LOSS)
    if vwap_weak_follow_through:
        reasons.append(ExitReason.VWAP_WEAK_FOLLOWTHROUGH)
    if settings.use_long_fast_ema_exit and current_bar.close < features.turn_ema_fast:
        reasons.append(ExitReason.LONG_EMA_EXIT)
    if (
        settings.take_profit_at_r > 0
        and state.entry_price is not None
        and risk_context.long_risk is not None
        and current_bar.high >= state.entry_price + settings.take_profit_at_r * risk_context.long_risk
    ):
        reasons.append(ExitReason.LONG_R_TARGET_EXIT)
    if _long_derivative_maturity_exit(current_bar, features, previous_bar, state, risk_context, settings):
        reasons.append(ExitReason.LONG_DERIVATIVE_MATURITY_EXIT)
    if settings.use_long_time_exit and state.bars_in_trade >= settings.vwap_long_max_bars:
        reasons.append(ExitReason.VWAP_TIME_EXIT)
    return reasons


def _short_reasons(
    current_bar: Bar,
    features: FeaturePacket,
    previous_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
    short_integrity_lost: bool,
) -> list[ExitReason]:
    reasons: list[ExitReason] = []
    if risk_context.active_short_stop_ref is not None and current_bar.close > risk_context.active_short_stop_ref:
        reasons.append(ExitReason.SHORT_STOP)
    if settings.use_short_swing_exit and state.last_swing_high is not None and current_bar.close > state.last_swing_high:
        reasons.append(ExitReason.SHORT_SWING_FAIL)
    if _additive_short_giveback_exit(state, settings):
        reasons.append(ExitReason.SHORT_ADDITIVE_GIVEBACK_EXIT)
    if _additive_short_profit_protect_exit(current_bar, previous_bar, state, risk_context, settings):
        reasons.append(ExitReason.SHORT_ADDITIVE_PROFIT_PROTECT_EXIT)
    if _additive_short_stalled_exit(current_bar, previous_bar, state, settings):
        reasons.append(ExitReason.SHORT_ADDITIVE_STALLED_EXIT)
    if settings.use_short_integrity_exit and short_integrity_lost:
        reasons.append(ExitReason.SHORT_INTEGRITY_FAIL)
    if settings.use_short_fast_ema_exit and current_bar.close > features.turn_ema_fast:
        reasons.append(ExitReason.SHORT_EMA_EXIT)
    if (
        settings.take_profit_at_r > 0
        and state.entry_price is not None
        and risk_context.short_risk is not None
        and current_bar.low <= state.entry_price - settings.take_profit_at_r * risk_context.short_risk
    ):
        reasons.append(ExitReason.SHORT_R_TARGET_EXIT)
    if _short_derivative_maturity_exit(current_bar, features, previous_bar, state, risk_context, settings):
        reasons.append(ExitReason.SHORT_DERIVATIVE_MATURITY_EXIT)
    if settings.use_short_time_exit and state.bars_in_trade >= settings.max_bars_short:
        reasons.append(ExitReason.SHORT_TIME_EXIT)
    return reasons


def _additive_short_stalled_exit(
    current_bar: Bar,
    previous_bar: Bar,
    state: StrategyState,
    settings: StrategySettings,
) -> bool:
    if not settings.use_additive_short_stalled_exit:
        return False
    if state.short_entry_family != ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE:
        return False
    if state.bars_in_trade < settings.additive_short_stalled_exit_min_bars:
        return False
    return current_bar.low >= previous_bar.low and current_bar.close >= previous_bar.close


def _additive_short_giveback_exit(
    state: StrategyState,
    settings: StrategySettings,
) -> bool:
    if not settings.use_additive_short_giveback_exit:
        return False
    if state.short_entry_family != ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE:
        return False
    if not state.additive_short_peak_threshold_reached:
        return False
    if state.additive_short_max_favorable_excursion <= 0:
        return False
    required_giveback = state.additive_short_max_favorable_excursion * settings.additive_short_giveback_fraction
    return state.additive_short_giveback_from_peak >= required_giveback


def _additive_short_profit_protect_exit(
    current_bar: Bar,
    previous_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
) -> bool:
    if not settings.use_additive_short_profit_protect_exit:
        return False
    if state.short_entry_family != ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE:
        return False
    if state.entry_price is None or risk_context.short_risk is None:
        return False
    if state.bars_in_trade < settings.additive_short_profit_protect_min_bars:
        return False
    required_profit = settings.additive_short_profit_protect_min_profit_r * risk_context.short_risk
    current_favorable_excursion = state.entry_price - current_bar.low
    if current_favorable_excursion < required_profit:
        return False
    return current_bar.close >= previous_bar.close


def _long_derivative_maturity_exit(
    current_bar: Bar,
    features: FeaturePacket,
    previous_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
) -> bool:
    if not settings.use_long_derivative_maturity_exit:
        return False
    if state.entry_price is None or risk_context.long_risk is None:
        return False
    current_profit = current_bar.close - state.entry_price
    if current_profit < settings.derivative_exit_min_profit_r * risk_context.long_risk:
        return False
    atr = max(features.atr, settings.risk_floor)
    normalized_velocity = features.velocity / atr
    normalized_velocity_delta = features.velocity_delta / atr
    if normalized_velocity < settings.long_derivative_exit_min_normalized_velocity:
        return False
    if normalized_velocity_delta > settings.long_derivative_exit_max_normalized_velocity_delta:
        return False
    if settings.derivative_exit_require_counter_close and current_bar.close > previous_bar.close:
        return False
    return True


def _short_derivative_maturity_exit(
    current_bar: Bar,
    features: FeaturePacket,
    previous_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
) -> bool:
    if not settings.use_short_derivative_maturity_exit:
        return False
    if state.entry_price is None or risk_context.short_risk is None:
        return False
    current_profit = state.entry_price - current_bar.close
    if current_profit < settings.derivative_exit_min_profit_r * risk_context.short_risk:
        return False
    atr = max(features.atr, settings.risk_floor)
    normalized_velocity = features.velocity / atr
    normalized_velocity_delta = features.velocity_delta / atr
    if normalized_velocity > settings.short_derivative_exit_max_normalized_velocity:
        return False
    if normalized_velocity_delta < settings.short_derivative_exit_min_normalized_velocity_delta:
        return False
    if settings.derivative_exit_require_counter_close and current_bar.close < previous_bar.close:
        return False
    return True
