"""Risk engine contracts."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional
from collections.abc import Sequence

from ..config_models import StrategySettings
from ..domain.enums import LongEntryFamily, PositionSide
from ..domain.models import Bar, FeaturePacket, StrategyState


@dataclass(frozen=True)
class RiskContext:
    k_long_stop_ref_base: Decimal
    k_short_stop_ref_base: Decimal
    vwap_long_stop_ref_base: Optional[Decimal]
    active_long_stop_ref_base: Optional[Decimal]
    active_long_stop_ref: Optional[Decimal]
    active_short_stop_ref: Optional[Decimal]
    long_risk: Optional[Decimal]
    short_risk: Optional[Decimal]
    long_break_even_armed: bool
    short_break_even_armed: bool


def compute_risk_context(
    history: Sequence[Bar],
    features: FeaturePacket,
    state: StrategyState,
    settings: StrategySettings,
) -> RiskContext:
    """Compute active stop and break-even context."""
    if not history:
        raise ValueError("history must include at least one finalized bar.")

    current_bar = history[-1]
    recent_bars = history[-3:]
    recent_lows = [bar.low for bar in recent_bars]
    recent_highs = [bar.high for bar in recent_bars]

    k_long_stop_ref_base = min(recent_lows) - settings.stop_atr_mult * features.atr
    k_short_stop_ref_base = max(recent_highs) + settings.stop_atr_mult * features.atr
    vwap_long_stop_ref_base = (
        state.asia_reclaim_bar_low - settings.vwap_long_stop_atr_mult * features.atr
        if state.asia_reclaim_bar_low is not None
        else None
    )

    active_long_stop_ref_base: Optional[Decimal]
    if state.long_entry_family == LongEntryFamily.VWAP:
        active_long_stop_ref_base = vwap_long_stop_ref_base
    else:
        active_long_stop_ref_base = k_long_stop_ref_base

    long_risk = _long_risk(state.entry_price, active_long_stop_ref_base, settings.risk_floor)
    short_risk = _short_risk(state.entry_price, k_short_stop_ref_base, settings.risk_floor)

    long_break_even_armed = _long_break_even_armed(current_bar, state, long_risk, settings)
    short_break_even_armed = _short_break_even_armed(current_bar, state, short_risk, settings)

    active_long_stop_ref = (
        max(active_long_stop_ref_base, state.entry_price)
        if long_break_even_armed and active_long_stop_ref_base is not None and state.entry_price is not None
        else active_long_stop_ref_base
    )
    active_short_stop_ref = (
        min(k_short_stop_ref_base, state.entry_price)
        if short_break_even_armed and state.entry_price is not None
        else k_short_stop_ref_base
    )

    return RiskContext(
        k_long_stop_ref_base=k_long_stop_ref_base,
        k_short_stop_ref_base=k_short_stop_ref_base,
        vwap_long_stop_ref_base=vwap_long_stop_ref_base,
        active_long_stop_ref_base=active_long_stop_ref_base,
        active_long_stop_ref=active_long_stop_ref,
        active_short_stop_ref=active_short_stop_ref,
        long_risk=long_risk,
        short_risk=short_risk,
        long_break_even_armed=long_break_even_armed,
        short_break_even_armed=short_break_even_armed,
    )


def _long_risk(
    entry_price: Optional[Decimal],
    active_long_stop_ref_base: Optional[Decimal],
    risk_floor: Decimal,
) -> Optional[Decimal]:
    if entry_price is None or active_long_stop_ref_base is None:
        return None
    return max(risk_floor, entry_price - active_long_stop_ref_base)


def _short_risk(
    entry_price: Optional[Decimal],
    k_short_stop_ref_base: Decimal,
    risk_floor: Decimal,
) -> Optional[Decimal]:
    if entry_price is None:
        return None
    return max(risk_floor, k_short_stop_ref_base - entry_price)


def _long_break_even_armed(
    current_bar: Bar,
    state: StrategyState,
    long_risk: Optional[Decimal],
    settings: StrategySettings,
) -> bool:
    if state.position_side != PositionSide.LONG:
        return False
    if state.long_be_armed:
        return True
    if state.entry_price is None or long_risk is None:
        return False

    if state.long_entry_family == LongEntryFamily.VWAP:
        return current_bar.high >= state.entry_price + settings.vwap_long_breakeven_at_r * long_risk
    if state.long_entry_family == LongEntryFamily.K:
        return current_bar.high >= state.entry_price + settings.breakeven_at_r * long_risk
    return False


def _short_break_even_armed(
    current_bar: Bar,
    state: StrategyState,
    short_risk: Optional[Decimal],
    settings: StrategySettings,
) -> bool:
    if state.position_side != PositionSide.SHORT:
        return False
    if state.short_be_armed:
        return True
    if state.entry_price is None or short_risk is None:
        return False
    return current_bar.low <= state.entry_price - settings.breakeven_at_r * short_risk
