"""Execution intent models derived from signals."""

from dataclasses import dataclass
from datetime import datetime

from .enums import OrderIntentType


@dataclass(frozen=True)
class ExecutionIntent:
    created_at: datetime
    bar_timestamp: datetime
    intent: OrderIntentType
    quantity: int
    symbol: str
    source_signal: str
    reason: str

    def __post_init__(self) -> None:
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("ExecutionIntent.created_at must be timezone-aware.")
        if self.bar_timestamp.tzinfo is None or self.bar_timestamp.utcoffset() is None:
            raise ValueError("ExecutionIntent.bar_timestamp must be timezone-aware.")
        if self.quantity <= 0:
            raise ValueError("ExecutionIntent.quantity must be > 0.")
        if not self.symbol:
            raise ValueError("ExecutionIntent.symbol is required.")
        if not self.source_signal:
            raise ValueError("ExecutionIntent.source_signal is required.")
        if not self.reason:
            raise ValueError("ExecutionIntent.reason is required.")
