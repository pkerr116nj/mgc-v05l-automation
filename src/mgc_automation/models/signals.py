"""Signal models that stay separate from execution."""

from dataclasses import dataclass
from datetime import datetime

from .enums import SignalAction


@dataclass(frozen=True)
class SignalDecision:
    signal_name: str
    action: SignalAction
    bar_timestamp: datetime
    reason: str

    def __post_init__(self) -> None:
        if not self.signal_name:
            raise ValueError("SignalDecision.signal_name is required.")
        if self.bar_timestamp.tzinfo is None or self.bar_timestamp.utcoffset() is None:
            raise ValueError("SignalDecision.bar_timestamp must be timezone-aware.")
        if not self.reason:
            raise ValueError("SignalDecision.reason is required.")
