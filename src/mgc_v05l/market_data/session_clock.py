"""Session classification contract."""

from dataclasses import replace
from datetime import datetime, time
from zoneinfo import ZoneInfo

from ..config_models import StrategySettings
from ..domain.models import Bar


def classify_sessions(bar: Bar, settings: StrategySettings) -> Bar:
    """Return a session-classified bar using exact release-candidate window logic.

    ThinkScript parity:
    - session membership is based on the bar end timestamp
    - window start is inclusive
    - window end is exclusive
    - cross-midnight windows use `>= start or < end`
    """
    local_end = bar.end_ts.astimezone(ZoneInfo(settings.timezone))
    is_asia = _is_within_window(local_end, settings.asia_start, settings.asia_end)
    is_london = _is_within_window(local_end, settings.london_start, settings.london_end)
    is_us = _is_within_window(local_end, settings.us_start, settings.us_end)

    asia_allowed = settings.allow_asia and is_asia
    london_allowed = settings.allow_london and is_london
    us_allowed = settings.allow_us and is_us

    return replace(
        bar,
        session_asia=is_asia,
        session_london=is_london,
        session_us=is_us,
        session_allowed=asia_allowed or london_allowed or us_allowed,
    )


def _is_within_window(ts: datetime, start_time: time, end_time: time) -> bool:
    bar_hhmm = ts.hour * 100 + ts.minute
    start_hhmm = start_time.hour * 100 + start_time.minute
    end_hhmm = end_time.hour * 100 + end_time.minute

    if start_hhmm > end_hhmm:
        return bar_hhmm >= start_hhmm or bar_hhmm < end_hhmm
    return bar_hhmm >= start_hhmm and bar_hhmm < end_hhmm
