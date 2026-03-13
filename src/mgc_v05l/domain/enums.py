"""Core enums from the Phase 3A and 3B design documents."""

from enum import Enum

try:
    from enum import StrEnum
except ImportError:
    class StrEnum(str, Enum):
        """Compatibility fallback for Python versions without enum.StrEnum."""


class StrategyStatus(StrEnum):
    DISABLED = "DISABLED"
    READY = "READY"
    IN_LONG_K = "IN_LONG_K"
    IN_LONG_VWAP = "IN_LONG_VWAP"
    IN_SHORT_K = "IN_SHORT_K"
    RECONCILING = "RECONCILING"
    FAULT = "FAULT"


class PositionSide(StrEnum):
    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


class LongEntryFamily(StrEnum):
    NONE = "NONE"
    K = "K"
    VWAP = "VWAP"


class OrderIntentType(StrEnum):
    BUY_TO_OPEN = "BUY_TO_OPEN"
    SELL_TO_CLOSE = "SELL_TO_CLOSE"
    SELL_TO_OPEN = "SELL_TO_OPEN"
    BUY_TO_CLOSE = "BUY_TO_CLOSE"


class OrderStatus(StrEnum):
    PENDING = "PENDING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class ExitReason(StrEnum):
    LONG_STOP = "LONG_STOP"
    LONG_SWING_FAIL = "LONG_SWING_FAIL"
    LONG_INTEGRITY_FAIL = "LONG_INTEGRITY_FAIL"
    LONG_TIME_EXIT = "LONG_TIME_EXIT"
    VWAP_LOSS = "VWAP_LOSS"
    VWAP_WEAK_FOLLOWTHROUGH = "VWAP_WEAK_FOLLOWTHROUGH"
    VWAP_TIME_EXIT = "VWAP_TIME_EXIT"
    SHORT_STOP = "SHORT_STOP"
    SHORT_SWING_FAIL = "SHORT_SWING_FAIL"
    SHORT_INTEGRITY_FAIL = "SHORT_INTEGRITY_FAIL"
    SHORT_TIME_EXIT = "SHORT_TIME_EXIT"


class HealthStatus(StrEnum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    FAULT = "FAULT"


class ReplayFillPolicy(StrEnum):
    NEXT_BAR_OPEN = "NEXT_BAR_OPEN"


class VwapPolicy(StrEnum):
    SESSION_RESET = "SESSION_RESET"
