"""Typed feature models derived from the formal spec."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Optional


@dataclass(frozen=True)
class SwingState:
    swing_low_confirmed: bool
    swing_high_confirmed: bool
    last_swing_low: Optional[Decimal]
    last_swing_high: Optional[Decimal]


@dataclass(frozen=True)
class FeatureSnapshot:
    tr: Decimal
    atr: Decimal
    bar_range: Decimal
    body_size: Decimal
    avg_vol: Decimal
    vol_ratio: Decimal
    turn_ema_fast: Decimal
    turn_ema_slow: Decimal
    velocity: Decimal
    velocity_delta: Decimal
    vwap_val: Decimal
    vwap_buffer: Decimal
    swing_state: SwingState
