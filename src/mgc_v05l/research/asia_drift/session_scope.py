"""Research-only Asia Drift session scope derivation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .models import AsiaDriftSessionScopeTag


NEW_YORK = ZoneInfo("America/New_York")
OUT_OF_SCOPE = "OUT_OF_SCOPE"
ASIA_DRIFT_BUILD = "ASIA_DRIFT_BUILD"
ASIA_DRIFT_MATURE = "ASIA_DRIFT_MATURE"
ASIA_DRIFT_PRE_HANDOFF = "ASIA_DRIFT_PRE_HANDOFF"


@dataclass(frozen=True)
class AsiaDriftSessionConfig:
    timezone: ZoneInfo = NEW_YORK
    session_anchor: time = time(18, 0)
    build_end: time = time(20, 30)
    mature_end: time = time(0, 30)
    latest_entry: time = time(1, 30)
    mandatory_exit: time = time(2, 55)
    scope_provenance: str = "asia_drift_v1_phase1.research_only_derived_scope"


DEFAULT_SESSION_CONFIG = AsiaDriftSessionConfig()


def derive_session_scope(
    *,
    instrument: str,
    timeframe: str,
    bar_end_ts: datetime,
    config: AsiaDriftSessionConfig = DEFAULT_SESSION_CONFIG,
    anchor_observed: bool,
) -> AsiaDriftSessionScopeTag:
    local_ts = bar_end_ts.astimezone(config.timezone)
    session_date = _session_date(local_ts, session_anchor=config.session_anchor)
    anchor_ts = datetime.combine(session_date, config.session_anchor, tzinfo=config.timezone)
    latest_entry_ts = datetime.combine(session_date + timedelta(days=1), config.latest_entry, tzinfo=config.timezone)
    mandatory_exit_ts = datetime.combine(session_date + timedelta(days=1), config.mandatory_exit, tzinfo=config.timezone)

    in_scope = anchor_ts <= local_ts <= mandatory_exit_ts
    session_timeout = local_ts > mandatory_exit_ts
    entry_window_open = anchor_ts <= local_ts <= latest_entry_ts
    subphase = _subphase(local_ts.timetz().replace(tzinfo=None))
    session_time_label = local_ts.strftime("%H:%M")
    session_id = f"{instrument.lower()}__{session_date.isoformat()}__asia_drift_v1"

    return AsiaDriftSessionScopeTag(
        instrument=instrument,
        timeframe=timeframe,
        bar_end_ts=bar_end_ts,
        local_session_date=session_date,
        asia_drift_session_id=session_id,
        session_anchor_ts=anchor_ts.astimezone(bar_end_ts.tzinfo or config.timezone),
        latest_entry_ts=latest_entry_ts.astimezone(bar_end_ts.tzinfo or config.timezone),
        mandatory_exit_ts=mandatory_exit_ts.astimezone(bar_end_ts.tzinfo or config.timezone),
        session_time_label=session_time_label,
        subphase=subphase if in_scope else OUT_OF_SCOPE,
        in_scope=in_scope,
        entry_window_open=in_scope and entry_window_open,
        session_timeout=session_timeout,
        anchor_observed=anchor_observed,
        scope_provenance=config.scope_provenance,
    )


def _session_date(local_ts: datetime, *, session_anchor: time) -> date:
    if local_ts.timetz().replace(tzinfo=None) >= session_anchor:
        return local_ts.date()
    return (local_ts - timedelta(days=1)).date()


def _subphase(local_time: time) -> str:
    if time(18, 0) <= local_time < time(20, 30):
        return ASIA_DRIFT_BUILD
    if local_time >= time(20, 30) or local_time < time(0, 30):
        return ASIA_DRIFT_MATURE
    if time(0, 30) <= local_time <= time(2, 55):
        return ASIA_DRIFT_PRE_HANDOFF
    return OUT_OF_SCOPE
