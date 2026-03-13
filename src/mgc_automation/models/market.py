"""Market data models."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Bar:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int

    def __post_init__(self) -> None:
        if self.timestamp.tzinfo is None or self.timestamp.utcoffset() is None:
            raise ValueError("Bar.timestamp must be timezone-aware.")
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("Bar.high must be >= open, low, and close.")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("Bar.low must be <= open, high, and close.")
        if self.volume < 0:
            raise ValueError("Bar.volume must be >= 0.")
