"""Shared enums used by typed models."""

from enum import Enum

try:
    from enum import StrEnum
except ImportError:
    class StrEnum(str, Enum):
        """Compatibility fallback for Python versions without enum.StrEnum."""


class Timeframe(StrEnum):
    FIVE_MINUTES = "5m"


class SessionTimezone(StrEnum):
    AMERICA_NEW_YORK = "America/New_York"


class ReplayFillPolicy(StrEnum):
    NEXT_BAR_OPEN = "NEXT_BAR_OPEN"


class VwapPolicy(StrEnum):
    SESSION_RESET = "SESSION_RESET"


class RunMode(StrEnum):
    REPLAY_FIRST = "REPLAY_FIRST"


class DatabaseBackend(StrEnum):
    SQLITE = "SQLITE"


class SymbolScope(StrEnum):
    SINGLE_SYMBOL_MGC = "SINGLE_SYMBOL_MGC"


class SessionName(StrEnum):
    ASIA = "ASIA"
    LONDON = "LONDON"
    US = "US"
    NONE = "NONE"


class SignalAction(StrEnum):
    ENTER_LONG = "ENTER_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_LONG = "EXIT_LONG"
    EXIT_SHORT = "EXIT_SHORT"
    HOLD = "HOLD"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderIntentType(StrEnum):
    BUY_TO_OPEN = "BUY_TO_OPEN"
    SELL_TO_CLOSE = "SELL_TO_CLOSE"
    SELL_TO_OPEN = "SELL_TO_OPEN"
    BUY_TO_CLOSE = "BUY_TO_CLOSE"


class LongTradeFamily(StrEnum):
    NONE = "0"
    K_LONG = "1"
    VWAP_LONG = "2"


class StrategySide(StrEnum):
    FLAT = "0"
    LONG = "1"
    SHORT = "-1"


class OperatingState(StrEnum):
    DISABLED = "DISABLED"
    READY = "READY"
    IN_LONG_K = "IN_LONG_K"
    IN_LONG_VWAP = "IN_LONG_VWAP"
    IN_SHORT_K = "IN_SHORT_K"
    FAULT = "FAULT"
    RECONCILING = "RECONCILING"


class DeploymentEnvironment(StrEnum):
    RESEARCH = "RESEARCH"
    PAPER = "PAPER"
    PRODUCTION = "PRODUCTION"


class DataHealthStatus(StrEnum):
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    INCOMPLETE = "INCOMPLETE"
    INCONSISTENT = "INCONSISTENT"


class BrokerConnectionStatus(StrEnum):
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"
    DEGRADED = "DEGRADED"


class AckStatus(StrEnum):
    PENDING = "PENDING"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    REJECTED = "REJECTED"
    TIMED_OUT = "TIMED_OUT"


class FillStatus(StrEnum):
    UNFILLED = "UNFILLED"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"


class FaultSeverity(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class ReconciliationTrigger(StrEnum):
    STARTUP = "STARTUP"
    BROKER_RECONNECT = "BROKER_RECONNECT"
    REJECTED_ORDER = "REJECTED_ORDER"
    MISSING_FILL_ACKNOWLEDGEMENT = "MISSING_FILL_ACKNOWLEDGEMENT"
    SCHEDULED_HEARTBEAT = "SCHEDULED_HEARTBEAT"
    POSITION_MISMATCH = "POSITION_MISMATCH"


class MismatchClassification(StrEnum):
    NONE = "NONE"
    QUANTITY = "QUANTITY"
    SIDE = "SIDE"
    AVERAGE_PRICE = "AVERAGE_PRICE"
    OPEN_ORDERS = "OPEN_ORDERS"
    LAST_FILL_TIMESTAMP = "LAST_FILL_TIMESTAMP"
    MULTIPLE = "MULTIPLE"
