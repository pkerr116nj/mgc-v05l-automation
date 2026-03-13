"""Exit engine contracts."""

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from ..config_models import StrategySettings
from ..domain.enums import ExitReason, LongEntryFamily, PositionSide
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
            reasons = _vwap_long_reasons(current_bar, state, risk_context, settings, vwap_lost, vwap_weak_follow_through)
        else:
            reasons = _k_long_reasons(current_bar, state, risk_context, settings, k_long_integrity_lost)
        return ExitDecision(
            long_exit=bool(reasons),
            short_exit=False,
            primary_reason=reasons[0] if reasons else None,
            all_true_reasons=tuple(reasons),
            k_long_integrity_lost=k_long_integrity_lost,
            vwap_lost=vwap_lost,
            vwap_weak_follow_through=vwap_weak_follow_through,
            short_integrity_lost=short_integrity_lost,
        )

    if state.position_side == PositionSide.SHORT:
        reasons = _short_reasons(current_bar, state, risk_context, settings, short_integrity_lost)
        return ExitDecision(
            long_exit=False,
            short_exit=bool(reasons),
            primary_reason=reasons[0] if reasons else None,
            all_true_reasons=tuple(reasons),
            k_long_integrity_lost=k_long_integrity_lost,
            vwap_lost=vwap_lost,
            vwap_weak_follow_through=vwap_weak_follow_through,
            short_integrity_lost=short_integrity_lost,
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
    )

def _k_long_reasons(
    current_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
    k_long_integrity_lost: bool,
) -> list[ExitReason]:
    reasons: list[ExitReason] = []
    if risk_context.active_long_stop_ref is not None and current_bar.close < risk_context.active_long_stop_ref:
        reasons.append(ExitReason.LONG_STOP)
    if state.last_swing_low is not None and current_bar.close < state.last_swing_low:
        reasons.append(ExitReason.LONG_SWING_FAIL)
    if k_long_integrity_lost:
        reasons.append(ExitReason.LONG_INTEGRITY_FAIL)
    if state.bars_in_trade >= settings.max_bars_long:
        reasons.append(ExitReason.LONG_TIME_EXIT)
    return reasons


def _vwap_long_reasons(
    current_bar: Bar,
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
    if state.bars_in_trade >= settings.vwap_long_max_bars:
        reasons.append(ExitReason.VWAP_TIME_EXIT)
    return reasons


def _short_reasons(
    current_bar: Bar,
    state: StrategyState,
    risk_context: RiskContext,
    settings: StrategySettings,
    short_integrity_lost: bool,
) -> list[ExitReason]:
    reasons: list[ExitReason] = []
    if risk_context.active_short_stop_ref is not None and current_bar.close > risk_context.active_short_stop_ref:
        reasons.append(ExitReason.SHORT_STOP)
    if state.last_swing_high is not None and current_bar.close > state.last_swing_high:
        reasons.append(ExitReason.SHORT_SWING_FAIL)
    if short_integrity_lost:
        reasons.append(ExitReason.SHORT_INTEGRITY_FAIL)
    if state.bars_in_trade >= settings.max_bars_short:
        reasons.append(ExitReason.SHORT_TIME_EXIT)
    return reasons
