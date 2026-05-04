"""Minimal pytz-compatible shim for local research tooling.

This project uses zoneinfo natively, but some third-party parquet readers still
attempt to import `pytz`. This shim implements only the subset needed by the
research stack.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone as datetime_timezone, tzinfo
from zoneinfo import ZoneInfo


__version__ = "2025.2"


class _CompatTimezone(tzinfo):
    def __init__(self, inner: tzinfo, *, name: str | None = None) -> None:
        self._inner = inner
        self.zone = name or getattr(inner, "key", str(inner))

    def utcoffset(self, dt: datetime | None) -> timedelta | None:
        return self._inner.utcoffset(dt)

    def dst(self, dt: datetime | None) -> timedelta | None:
        return self._inner.dst(dt)

    def tzname(self, dt: datetime | None) -> str | None:
        return self._inner.tzname(dt)

    def fromutc(self, dt: datetime) -> datetime:
        inner_dt = dt.replace(tzinfo=self._inner)
        converted = self._inner.fromutc(inner_dt)
        return converted.replace(tzinfo=self)

    def localize(self, dt: datetime, is_dst: bool | None = None) -> datetime:
        if dt.tzinfo is not None:
            return dt.astimezone(self)
        return dt.replace(tzinfo=self)

    def normalize(self, dt: datetime, is_dst: bool | None = None) -> datetime:
        return dt.astimezone(self)


class BaseTzInfo(_CompatTimezone):
    pass


class _FixedOffset(_CompatTimezone):
    def __init__(self, minutes: int) -> None:
        sign = "+" if minutes >= 0 else "-"
        absolute = abs(minutes)
        hours, remainder = divmod(absolute, 60)
        name = f"{sign}{hours:02d}:{remainder:02d}"
        super().__init__(datetime_timezone(timedelta(minutes=minutes)), name=name)
        self._minutes = minutes


utc = BaseTzInfo(datetime_timezone.utc, name="UTC")
UTC = utc


def timezone(name: str) -> _CompatTimezone:
    if name.upper() == "UTC":
        return utc
    return BaseTzInfo(ZoneInfo(name), name=name)


def FixedOffset(minutes: int) -> _CompatTimezone:
    return _FixedOffset(minutes)
