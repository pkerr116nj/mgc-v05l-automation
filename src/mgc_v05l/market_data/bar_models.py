"""Market-data bar helpers and exports."""

from datetime import datetime, timezone

from ..domain.models import Bar


def build_bar_id(symbol: str, timeframe: str, bar_end_ts: datetime) -> str:
    """Return the recommended persisted bar identifier."""
    if bar_end_ts.tzinfo is None or bar_end_ts.utcoffset() is None:
        raise ValueError("bar_end_ts must be timezone-aware.")
    normalized_end_ts = bar_end_ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return f"{symbol}|{timeframe}|{normalized_end_ts}"


__all__ = ["Bar", "build_bar_id"]
