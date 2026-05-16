"""Shared session phase labels and session matching helpers."""

from __future__ import annotations

from datetime import datetime, time
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")

_SESSION_WINDOWS: dict[str, tuple[time, time]] = {
    "SESSION_OPEN": (time(18, 0), time(19, 0)),
    "ASIA_EARLY": (time(19, 0), time(20, 30)),
    "ASIA_LATE": (time(20, 30), time(3, 0)),
    "ASIA": (time(18, 0), time(3, 0)),
    "LONDON_OPEN": (time(3, 0), time(5, 30)),
    "LONDON_EARLY": (time(3, 0), time(5, 30)),
    "LONDON_LATE": (time(5, 30), time(8, 20)),
    "LONDON": (time(3, 0), time(8, 20)),
    "US_EARLY": (time(8, 20), time(11, 0)),
    "NY_EARLY": (time(8, 20), time(11, 0)),
    "US_PREOPEN_OPENING": (time(9, 0), time(9, 30)),
    "US_CASH_OPEN_IMPULSE": (time(9, 30), time(10, 0)),
    "US_OPEN_LATE": (time(10, 0), time(10, 30)),
    "US_MIDDAY": (time(11, 0), time(13, 30)),
    "NY_LATE": (time(11, 0), time(13, 30)),
    "US_LATE": (time(13, 30), time(16, 0)),
    "US": (time(8, 20), time(16, 0)),
    "NY": (time(8, 20), time(16, 0)),
}


def _is_futures_reopen_day(local_dt: datetime) -> bool:
    """Return true for Sunday through Thursday futures reopen evenings."""
    return local_dt.weekday() in {6, 0, 1, 2, 3}


def label_session_phase(timestamp: datetime) -> str:
    """Return a session-phase label for the given timestamp."""
    local_dt = timestamp.astimezone(NEW_YORK) if timestamp.tzinfo is not None else timestamp.replace(tzinfo=NEW_YORK)
    local_time = local_dt.timetz().replace(tzinfo=None)

    if local_time == time(18, 0):
        return "SESSION_RESET_1800"
    if time(18, 0) < local_time < time(19, 0) and _is_futures_reopen_day(local_dt):
        return "SESSION_OPEN"
    if time(19, 0) <= local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time or local_time < time(3, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_OPEN"
    if time(5, 30) <= local_time < time(8, 20):
        return "LONDON_LATE"
    if time(8, 20) <= local_time < time(9, 0):
        return "US_EARLY"
    if time(9, 0) <= local_time < time(9, 30):
        return "US_PREOPEN_OPENING"
    if time(9, 30) <= local_time < time(10, 0):
        return "US_CASH_OPEN_IMPULSE"
    if time(10, 0) <= local_time < time(10, 30):
        return "US_OPEN_LATE"
    if time(10, 30) <= local_time < time(11, 0):
        return "US_EARLY"
    if time(11, 0) <= local_time < time(13, 30):
        return "US_MIDDAY"
    if time(13, 30) <= local_time < time(16, 0):
        return "US_LATE"
    return "UNCLASSIFIED"


def phase_coarse_session_group(phase: str) -> str:
    """Collapse a fine-grained phase label into a broad session group."""
    normalized = str(phase or "").upper()
    if normalized in {"ASIA", "LONDON", "US", "NY"}:
        return "US" if normalized == "NY" else normalized
    if normalized.startswith("ASIA_") or normalized == "SESSION_OPEN":
        return "ASIA"
    if normalized.startswith("LONDON_"):
        return "LONDON"
    if normalized.startswith("US_"):
        return "US"
    return "UNKNOWN"


def session_restriction_matches_phase(current_phase: str, restriction: str | None) -> bool:
    """Return whether a restriction matches a fine-grained phase label."""
    normalized = str(restriction or "").upper().strip()
    if not normalized or normalized in {"ALL", "ANY"}:
        return True
    if "/" in normalized:
        allowed = {part.strip() for part in normalized.split("/") if part.strip()}
        coarse = phase_coarse_session_group(current_phase)
        return coarse in allowed or str(current_phase or "").upper() in allowed
    if normalized == "US_EARLY_OBSERVATION":
        return str(current_phase or "").upper() in {"US_PREOPEN_OPENING", "US_CASH_OPEN_IMPULSE", "US_OPEN_LATE"}
    if normalized in {"ASIA", "LONDON", "US", "NY"}:
        return phase_coarse_session_group(current_phase) == ("US" if normalized == "NY" else normalized)
    return str(current_phase or "").upper() == normalized


def session_restriction_matches_timestamp(timestamp: datetime, restriction: str | None) -> bool:
    """Return whether a restriction matches a timestamp in New York trading time."""
    normalized = str(restriction or "").upper().strip()
    if not normalized or normalized in {"ALL", "ANY"}:
        return True
    local_dt = timestamp.astimezone(NEW_YORK) if timestamp.tzinfo is not None else timestamp.replace(tzinfo=NEW_YORK)
    local_time = local_dt.timetz().replace(tzinfo=None)
    current_phase = label_session_phase(local_dt)
    if "/" in normalized:
        return any(session_restriction_matches_timestamp(local_dt, part.strip()) for part in normalized.split("/") if part.strip())
    if normalized == "US_EARLY_OBSERVATION":
        return current_phase in {"US_PREOPEN_OPENING", "US_CASH_OPEN_IMPULSE", "US_OPEN_LATE"}
    window = _SESSION_WINDOWS.get(normalized)
    if window is not None:
        if normalized == "SESSION_OPEN" and not _is_futures_reopen_day(local_dt):
            return False
        start, end = window
        if end <= start:
            return local_time >= start or local_time < end
        return start <= local_time < end
    return session_restriction_matches_phase(current_phase, normalized)
