"""Typed strategy state models derived from the formal spec."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from .enums import LongTradeFamily, StrategySide


@dataclass(frozen=True)
class StrategyState:
    in_position: bool
    strategy_side: StrategySide
    long_trade_family: LongTradeFamily
    position_quantity: int
    entry_price: Optional[Decimal]
    bars_in_trade: int
    long_break_even_armed: bool
    short_break_even_armed: bool
    active_long_stop_reference: Optional[Decimal]
    active_short_stop_reference: Optional[Decimal]
    asia_reclaim_bar_low: Optional[Decimal]
    asia_reclaim_bar_high: Optional[Decimal]
    asia_reclaim_bar_vwap: Optional[Decimal]
    bull_snap_cooldown: int
    bear_snap_cooldown: int
    last_long_signal_timestamp: Optional[datetime]
    last_short_signal_timestamp: Optional[datetime]
    last_execution_timestamp: Optional[datetime]
