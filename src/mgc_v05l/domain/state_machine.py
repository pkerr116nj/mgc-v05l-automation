"""State machine rules from the Phase 2.5 concrete technical design."""

from .enums import StrategyStatus

_IN_POSITION_STATES = (
    StrategyStatus.IN_LONG_K,
    StrategyStatus.IN_LONG_VWAP,
    StrategyStatus.IN_SHORT_K,
)


def is_valid_transition(current_status: StrategyStatus, next_status: StrategyStatus) -> bool:
    """Return whether a state transition is allowed by the documented rules."""
    if current_status == StrategyStatus.IN_LONG_K and next_status == StrategyStatus.IN_SHORT_K:
        return False
    if current_status == StrategyStatus.IN_LONG_VWAP and next_status == StrategyStatus.IN_SHORT_K:
        return False
    if current_status == StrategyStatus.IN_SHORT_K and next_status in (
        StrategyStatus.IN_LONG_K,
        StrategyStatus.IN_LONG_VWAP,
    ):
        return False
    if current_status in _IN_POSITION_STATES and next_status in _IN_POSITION_STATES:
        return False
    return True
