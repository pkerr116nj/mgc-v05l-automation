"""Trade-state helpers."""

from datetime import datetime

from ..domain.enums import LongEntryFamily, PositionSide, StrategyStatus
from ..domain.models import StrategyState


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
        reconcile_required=False,
        fault_code=None,
        updated_at=now,
    )
