"""Trade-state helpers."""

from dataclasses import replace
from datetime import datetime
from decimal import Decimal

from ..domain.enums import LongEntryFamily, PositionSide, ShortEntryFamily, StrategyStatus
from ..domain.models import StrategyEntryLeg, StrategyState


def build_initial_state(now: datetime) -> StrategyState:
    """Return an explicit flat READY-adjacent state snapshot for persistence initialization."""
    return StrategyState(
        strategy_status=StrategyStatus.DISABLED,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        entry_price=None,
        entry_timestamp=None,
        entry_bar_id=None,
        long_entry_family=LongEntryFamily.NONE,
        bars_in_trade=0,
        long_be_armed=False,
        short_be_armed=False,
        last_swing_low=None,
        last_swing_high=None,
        asia_reclaim_bar_low=None,
        asia_reclaim_bar_high=None,
        asia_reclaim_bar_vwap=None,
        bars_since_bull_snap=None,
        bars_since_bear_snap=None,
        bars_since_asia_reclaim=None,
        bars_since_asia_vwap_signal=None,
        bars_since_long_setup=None,
        bars_since_short_setup=None,
        last_signal_bar_id=None,
        last_order_intent_id=None,
        open_broker_order_id=None,
        entries_enabled=True,
        exits_enabled=True,
        operator_halt=False,
        same_underlying_entry_hold=False,
        same_underlying_hold_reason=None,
        reconcile_required=False,
        fault_code=None,
        updated_at=now,
        short_entry_family=ShortEntryFamily.NONE,
        short_entry_source=None,
        additive_short_max_favorable_excursion=Decimal("0"),
        additive_short_peak_threshold_reached=False,
        additive_short_giveback_from_peak=Decimal("0"),
        open_entry_legs=(),
    )


def normalize_legacy_single_position_state(state: StrategyState) -> StrategyState:
    """Backfill a synthetic single leg for legacy in-position snapshots without staged-leg state."""
    if state.position_side == PositionSide.FLAT or state.open_entry_legs or state.internal_position_qty <= 0:
        return state
    if state.entry_price is None or state.entry_timestamp is None or state.entry_bar_id is None:
        return state
    synthetic_leg = StrategyEntryLeg(
        leg_id=f"legacy::{state.last_order_intent_id or state.entry_bar_id}",
        order_intent_id=str(state.last_order_intent_id or state.entry_bar_id),
        quantity=state.internal_position_qty,
        entry_price=state.entry_price,
        entry_timestamp=state.entry_timestamp,
        signal_bar_id=state.entry_bar_id,
        position_side=state.position_side,
        long_entry_family=state.long_entry_family if state.position_side == PositionSide.LONG else LongEntryFamily.NONE,
        short_entry_family=state.short_entry_family if state.position_side == PositionSide.SHORT else ShortEntryFamily.NONE,
        short_entry_source=state.short_entry_source if state.position_side == PositionSide.SHORT else None,
    )
    return replace(state, open_entry_legs=(synthetic_leg,))
