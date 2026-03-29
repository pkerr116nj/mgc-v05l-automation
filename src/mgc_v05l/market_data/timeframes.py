"""Shared timeframe normalization utilities for market-data ingestion and research."""

from __future__ import annotations

import re


_HOUR_ALIAS_PATTERN = re.compile(r"^(?P<hours>\d+)h$")
_MINUTE_PATTERN = re.compile(r"^(?P<minutes>\d+)m$")


def normalize_timeframe_label(timeframe: str) -> str:
    value = timeframe.strip().lower()
    minute_match = _MINUTE_PATTERN.fullmatch(value)
    if minute_match is not None:
        minutes = int(minute_match.group("minutes"))
        if minutes <= 0:
            raise ValueError(f"Timeframe must be positive: {timeframe}")
        return f"{minutes}m"

    hour_match = _HOUR_ALIAS_PATTERN.fullmatch(value)
    if hour_match is not None:
        hours = int(hour_match.group("hours"))
        if hours <= 0:
            raise ValueError(f"Timeframe must be positive: {timeframe}")
        return f"{hours * 60}m"

    raise ValueError(f"Unsupported timeframe label: {timeframe}")


def timeframe_minutes(timeframe: str) -> int:
    canonical = normalize_timeframe_label(timeframe)
    minute_match = _MINUTE_PATTERN.fullmatch(canonical)
    assert minute_match is not None
    return int(minute_match.group("minutes"))


def timeframe_aliases(timeframe: str) -> list[str]:
    canonical = normalize_timeframe_label(timeframe)
    minutes = timeframe_minutes(canonical)
    aliases = [canonical]
    if minutes % 60 == 0:
        aliases.append(f"{minutes // 60}h")
    return aliases
