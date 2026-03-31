"""State invariant checks."""

from ..domain.enums import LongEntryFamily, PositionSide, ShortEntryFamily
from ..domain.models import StrategyState


def validate_state(state: StrategyState) -> list[str]:
    """Return invariant violations for a strategy state snapshot."""
    violations: list[str] = []

    if state.position_side == PositionSide.FLAT and state.internal_position_qty != 0:
        violations.append("internal_position_qty must be 0 while flat")
    if state.position_side == PositionSide.FLAT and state.broker_position_qty != 0:
        violations.append("broker_position_qty must be 0 while flat")
    if state.position_side == PositionSide.FLAT and state.long_entry_family != LongEntryFamily.NONE:
        violations.append("long_entry_family must be NONE while flat")
    if state.position_side == PositionSide.FLAT and state.short_entry_family != ShortEntryFamily.NONE:
        violations.append("short_entry_family must be NONE while flat")
    if state.position_side == PositionSide.FLAT and state.short_entry_source is not None:
        violations.append("short_entry_source must be None while flat")
    if state.position_side == PositionSide.FLAT and state.additive_short_max_favorable_excursion != 0:
        violations.append("additive_short_max_favorable_excursion must be 0 while flat")
    if state.position_side == PositionSide.FLAT and state.additive_short_peak_threshold_reached:
        violations.append("additive_short_peak_threshold_reached must be False while flat")
    if state.position_side == PositionSide.FLAT and state.additive_short_giveback_from_peak != 0:
        violations.append("additive_short_giveback_from_peak must be 0 while flat")
    if state.position_side != PositionSide.FLAT and state.internal_position_qty <= 0:
        violations.append("internal_position_qty must be > 0 while in position")
    if state.position_side != PositionSide.FLAT and state.broker_position_qty < 0:
        violations.append("broker_position_qty must be >= 0 while in position")
    if state.position_side == PositionSide.LONG and state.entry_price is None:
        violations.append("entry_price must not be None while long")
    if state.position_side == PositionSide.LONG and state.long_entry_family == LongEntryFamily.NONE:
        violations.append("long_entry_family must not be NONE while long")
    if state.position_side == PositionSide.SHORT and state.entry_price is None:
        violations.append("entry_price must not be None while short")
    if state.position_side == PositionSide.SHORT and state.internal_position_qty <= 0:
        violations.append("internal_position_qty must be > 0 while short")
    if state.position_side == PositionSide.FLAT and state.bars_in_trade != 0:
        violations.append("bars_in_trade must be 0 while flat")
    if state.position_side != PositionSide.FLAT and state.bars_in_trade < 1:
        violations.append("bars_in_trade must be >= 1 while in position")
    if state.bars_in_trade < 0:
        violations.append("bars_in_trade must be >= 0")
    if state.position_side == PositionSide.SHORT and state.long_entry_family != LongEntryFamily.NONE:
        violations.append("long_entry_family must be NONE while short")
    if state.position_side == PositionSide.LONG and state.short_entry_family != ShortEntryFamily.NONE:
        violations.append("short_entry_family must be NONE while long")
    if state.position_side == PositionSide.LONG and state.short_entry_source is not None:
        violations.append("short_entry_source must be None while long")
    if state.position_side == PositionSide.LONG and state.additive_short_max_favorable_excursion != 0:
        violations.append("additive_short_max_favorable_excursion must be 0 while long")
    if state.position_side == PositionSide.LONG and state.additive_short_peak_threshold_reached:
        violations.append("additive_short_peak_threshold_reached must be False while long")
    if state.position_side == PositionSide.LONG and state.additive_short_giveback_from_peak != 0:
        violations.append("additive_short_giveback_from_peak must be 0 while long")
    if state.additive_short_max_favorable_excursion < 0:
        violations.append("additive_short_max_favorable_excursion must be >= 0")
    if state.additive_short_giveback_from_peak < 0:
        violations.append("additive_short_giveback_from_peak must be >= 0")
    if state.position_side == PositionSide.FLAT and state.entry_price is not None:
        violations.append("entry_price must be None while flat")
    if state.position_side == PositionSide.FLAT and state.entry_timestamp is not None:
        violations.append("entry_timestamp must be None while flat")
    if state.position_side == PositionSide.FLAT and state.entry_bar_id is not None:
        violations.append("entry_bar_id must be None while flat")
    if state.position_side == PositionSide.LONG and state.entry_timestamp is None:
        violations.append("entry_timestamp must not be None while long")
    if state.position_side == PositionSide.SHORT and state.entry_timestamp is None:
        violations.append("entry_timestamp must not be None while short")
    if state.bars_since_bull_snap is not None and state.bars_since_bull_snap < 0:
        violations.append("bars_since_bull_snap must be >= 0")
    if state.bars_since_bear_snap is not None and state.bars_since_bear_snap < 0:
        violations.append("bars_since_bear_snap must be >= 0")
    if state.bars_since_asia_reclaim is not None and state.bars_since_asia_reclaim < 0:
        violations.append("bars_since_asia_reclaim must be >= 0")
    if state.bars_since_asia_vwap_signal is not None and state.bars_since_asia_vwap_signal < 0:
        violations.append("bars_since_asia_vwap_signal must be >= 0")
    if state.bars_since_long_setup is not None and state.bars_since_long_setup < 0:
        violations.append("bars_since_long_setup must be >= 0")
    if state.bars_since_short_setup is not None and state.bars_since_short_setup < 0:
        violations.append("bars_since_short_setup must be >= 0")

    return violations
