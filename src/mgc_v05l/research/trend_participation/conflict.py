"""Conflict-resolution helpers for lower-priority directional research signals."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable

from .models import ConflictOutcome, HigherPrioritySignal


def resolve_conflict(
    *,
    instrument: str,
    side: str,
    decision_ts: datetime,
    entry_window_minutes: int,
    higher_priority_signals: Iterable[HigherPrioritySignal],
) -> tuple[ConflictOutcome, str | None]:
    normalized_instrument = instrument.strip().upper()
    normalized_side = side.strip().upper()
    window_end = decision_ts + timedelta(minutes=entry_window_minutes)
    active_events = [
        event
        for event in higher_priority_signals
        if event.instrument.strip().upper() == normalized_instrument
        and event.start_ts <= window_end
        and (event.end_ts is None or event.end_ts >= decision_ts)
    ]
    if not active_events:
        return ConflictOutcome.NO_CONFLICT, None

    for event in active_events:
        if event.cooldown:
            return ConflictOutcome.HARD_CONFLICT_COOLDOWN, event.reason

    for event in active_events:
        if event.side.strip().upper() == normalized_side:
            return ConflictOutcome.AGREEMENT, event.reason

    return ConflictOutcome.SOFT_CONFLICT, active_events[0].reason
