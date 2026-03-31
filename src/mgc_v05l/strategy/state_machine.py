"""Pure state transitions from the Phase 3A blueprint."""

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from ..config_models import StrategySettings
from ..domain.enums import LongEntryFamily, OrderStatus, PositionSide, ShortEntryFamily, StrategyStatus
from ..domain.models import Bar, StrategyState
from ..execution.order_models import FillEvent
from .risk_engine import RiskContext


def transition_on_entry_fill(
    state: StrategyState,
    fill_event: FillEvent,
    trade_size: int,
    signal_bar_id: str,
    long_entry_family: LongEntryFamily,
    short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE,
    short_entry_source: str | None = None,
) -> StrategyState:
    """Apply the documented entry-fill transition rules."""
    if fill_event.order_status is not OrderStatus.FILLED:
        raise ValueError("entry transitions require a confirmed fill.")
    if fill_event.fill_price is None:
        raise ValueError("fill_event.fill_price is required for entry transitions.")

    if fill_event.intent_type.value == "BUY_TO_OPEN":
        next_status = (
            StrategyStatus.IN_LONG_VWAP if long_entry_family == LongEntryFamily.VWAP else StrategyStatus.IN_LONG_K
        )
        next_side = PositionSide.LONG
    elif fill_event.intent_type.value == "SELL_TO_OPEN":
        next_status = StrategyStatus.IN_SHORT_K
        next_side = PositionSide.SHORT
        long_entry_family = LongEntryFamily.NONE
        if short_entry_family == ShortEntryFamily.NONE:
            short_entry_source = None
    else:
        raise ValueError("entry fills must use BUY_TO_OPEN or SELL_TO_OPEN intent types.")

    return replace(
        state,
        strategy_status=next_status,
        position_side=next_side,
        internal_position_qty=trade_size,
        broker_position_qty=trade_size,
        entry_price=fill_event.fill_price,
        entry_timestamp=fill_event.fill_timestamp,
        entry_bar_id=signal_bar_id,
        long_entry_family=long_entry_family,
        short_entry_family=short_entry_family if next_side == PositionSide.SHORT else ShortEntryFamily.NONE,
        short_entry_source=short_entry_source if next_side == PositionSide.SHORT else None,
        additive_short_max_favorable_excursion=Decimal("0"),
        additive_short_peak_threshold_reached=False,
        additive_short_giveback_from_peak=Decimal("0"),
        bars_in_trade=1,
        long_be_armed=False,
        short_be_armed=False,
        open_broker_order_id=None,
        updated_at=fill_event.fill_timestamp,
    )


def transition_on_exit_fill(state: StrategyState, fill_event: FillEvent) -> StrategyState:
    """Apply the documented exit-fill transition rules."""
    if fill_event.order_status is not OrderStatus.FILLED:
        raise ValueError("exit transitions require a confirmed fill.")
    return replace(
        state,
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        entry_price=None,
        entry_timestamp=None,
        entry_bar_id=None,
        long_entry_family=LongEntryFamily.NONE,
        short_entry_family=ShortEntryFamily.NONE,
        short_entry_source=None,
        additive_short_max_favorable_excursion=Decimal("0"),
        additive_short_peak_threshold_reached=False,
        additive_short_giveback_from_peak=Decimal("0"),
        bars_in_trade=0,
        long_be_armed=False,
        short_be_armed=False,
        open_broker_order_id=None,
        updated_at=fill_event.fill_timestamp,
    )


def transition_to_fault(state: StrategyState, occurred_at: datetime, fault_code: str) -> StrategyState:
    """Move the strategy into FAULT."""
    return replace(
        state,
        strategy_status=StrategyStatus.FAULT,
        reconcile_required=True,
        fault_code=fault_code,
        updated_at=occurred_at,
    )


def transition_to_reconciling(state: StrategyState, occurred_at: datetime) -> StrategyState:
    """Move the strategy into RECONCILING."""
    return replace(
        state,
        strategy_status=StrategyStatus.RECONCILING,
        reconcile_required=True,
        updated_at=occurred_at,
    )


def transition_to_ready(state: StrategyState, occurred_at: datetime) -> StrategyState:
    """Move the strategy into READY after successful recovery or flat confirmation."""
    return replace(
        state,
        strategy_status=StrategyStatus.READY,
        reconcile_required=False,
        fault_code=None,
        updated_at=occurred_at,
    )


def increment_bars_in_trade(state: StrategyState, occurred_at: datetime) -> StrategyState:
    """Increment bars_in_trade for an open position on each completed bar."""
    if state.position_side == PositionSide.FLAT:
        return state
    return replace(state, bars_in_trade=state.bars_in_trade + 1, updated_at=occurred_at)


def update_additive_short_peak_state(
    state: StrategyState,
    current_bar: Bar,
    risk_context: RiskContext,
    settings: StrategySettings,
    occurred_at: datetime,
) -> StrategyState:
    """Track additive-short favorable excursion and giveback without changing default exits."""
    if state.position_side != PositionSide.SHORT:
        return state
    if state.short_entry_family != ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE:
        return state
    if state.entry_price is None:
        return state

    current_favorable_excursion = max(Decimal("0"), state.entry_price - current_bar.low)
    max_favorable_excursion = max(state.additive_short_max_favorable_excursion, current_favorable_excursion)
    threshold_reached = state.additive_short_peak_threshold_reached
    if risk_context.short_risk is not None:
        threshold_reached = threshold_reached or (
            max_favorable_excursion >= settings.additive_short_giveback_min_peak_profit_r * risk_context.short_risk
        )
    giveback_from_peak = max(Decimal("0"), max_favorable_excursion - current_favorable_excursion)
    return replace(
        state,
        additive_short_max_favorable_excursion=max_favorable_excursion,
        additive_short_peak_threshold_reached=threshold_reached,
        additive_short_giveback_from_peak=giveback_from_peak,
        updated_at=occurred_at,
    )
