"""Typed session models derived from the formal spec."""

from dataclasses import dataclass
from datetime import datetime, time


@dataclass(frozen=True)
class SessionWindow:
    start: time
    end: time


@dataclass(frozen=True)
class SessionClassification:
    timestamp: datetime
    is_asia: bool
    is_london: bool
    is_us: bool
    asia_allowed: bool
    london_allowed: bool
    us_allowed: bool
    session_allowed: bool
    non_asia_allowed: bool
