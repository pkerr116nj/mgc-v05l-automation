"""Swing tracker contract."""

from collections.abc import Sequence
from decimal import Decimal
from typing import Optional

from ..domain.models import Bar


def update_swing_state(
    history: Sequence[Bar],
    previous_swing_low: Optional[Decimal],
    previous_swing_high: Optional[Decimal],
) -> tuple[bool, bool, Optional[Decimal], Optional[Decimal]]:
    """Return swing confirmation booleans and persisted swing anchors.

    ThinkScript parity:
    - swingLowConfirmed = low[1] < low[2] and low[1] < low
    - swingHighConfirmed = high[1] > high[2] and high[1] > high
    - last swing anchors persist until replaced
    """
    if len(history) < 3:
        return False, False, previous_swing_low, previous_swing_high

    current_bar = history[-1]
    previous_bar = history[-2]
    two_bars_ago = history[-3]

    swing_low_confirmed = previous_bar.low < two_bars_ago.low and previous_bar.low < current_bar.low
    swing_high_confirmed = previous_bar.high > two_bars_ago.high and previous_bar.high > current_bar.high

    last_swing_low = previous_bar.low if swing_low_confirmed else previous_swing_low
    last_swing_high = previous_bar.high if swing_high_confirmed else previous_swing_high

    return swing_low_confirmed, swing_high_confirmed, last_swing_low, last_swing_high
