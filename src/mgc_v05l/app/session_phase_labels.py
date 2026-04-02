"""Research-only session phase labels for replay diagnostics and exports."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")


def label_session_phase(timestamp: datetime) -> str:
    """Return a research-only session-phase label for the given timestamp."""
    local_dt = timestamp.astimezone(NEW_YORK) if timestamp.tzinfo is not None else timestamp.replace(tzinfo=NEW_YORK)
    local_time = local_dt.timetz().replace(tzinfo=None)

    if local_time == time(18, 0):
        return "SESSION_RESET_1800"
    if time(18, 0) < local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time < time(23, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_OPEN"
    if time(5, 30) <= local_time < time(8, 30):
        return "LONDON_LATE"
    if time(9, 0) <= local_time < time(9, 30):
        return "US_PREOPEN_OPENING"
    if time(9, 30) <= local_time < time(10, 0):
        return "US_CASH_OPEN_IMPULSE"
    if time(10, 0) <= local_time < time(10, 30):
        return "US_OPEN_LATE"
    if time(10, 30) <= local_time < time(14, 0):
        return "US_MIDDAY"
    if time(14, 0) <= local_time < time(17, 0):
        return "US_LATE"
    return "UNCLASSIFIED"
