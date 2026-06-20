"""Session-aware classification for Phase-1 runtime market-data freshness."""

from __future__ import annotations

from datetime import datetime, time
from functools import lru_cache

from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
    MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS,
    MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED,
    SESSION_CALENDAR_CME_CRYPTO_FUTURES,
    SESSION_CALENDAR_GLOBEX_FUTURES,
    TrackBLiveMarketDataSymbol,
    load_track_b_live_market_data_symbols,
)
from mgc_v05l.session_phase_labels import NEW_YORK


MARKET_CLOSED_NO_FRESH_BARS = "MARKET_CLOSED_NO_FRESH_BARS"
MARKET_OPEN_EXPECT_FRESH_BARS = "MARKET_OPEN_EXPECT_FRESH_BARS"


def classify_phase1_futures_market_session(now: datetime, *, symbol: str | None = None) -> dict[str, object]:
    """Classify whether fresh CME Globex futures bars are expected right now."""

    local_dt = now.astimezone(NEW_YORK) if now.tzinfo is not None else now.replace(tzinfo=NEW_YORK)
    local_time = local_dt.timetz().replace(tzinfo=None)
    normalized_symbol = str(symbol or "").strip().upper()
    weekday = local_dt.weekday()
    closed = False
    reason = "GLOBEX_SESSION_OPEN"

    if phase1_symbol_session_calendar(normalized_symbol) == SESSION_CALENDAR_CME_CRYPTO_FUTURES:
        reason = "CRYPTO_FUTURES_SESSION_OPEN"
        if time(17, 0) <= local_time < time(18, 0):
            closed = True
            reason = "CRYPTO_FUTURES_DAILY_MAINTENANCE_HALT"
    elif weekday == 5:
        closed = True
        reason = "WEEKEND_GLOBEX_HALT_SATURDAY"
    elif weekday == 6 and local_time < time(18, 0):
        closed = True
        reason = "WEEKEND_GLOBEX_HALT_BEFORE_SUNDAY_REOPEN"
    elif weekday == 4 and local_time >= time(17, 0):
        closed = True
        reason = "WEEKEND_GLOBEX_HALT_AFTER_FRIDAY_CLOSE"
    elif time(17, 0) <= local_time < time(18, 0):
        closed = True
        reason = "DAILY_GLOBEX_MAINTENANCE_HALT"

    return {
        "classification": MARKET_CLOSED_NO_FRESH_BARS if closed else MARKET_OPEN_EXPECT_FRESH_BARS,
        "market_closed": closed,
        "reason": reason,
        "symbol": normalized_symbol or None,
        "evaluated_at": now.isoformat(),
        "evaluated_at_new_york": local_dt.isoformat(),
    }


def phase1_fresh_bars_expected(now: datetime, *, symbol: str | None = None) -> bool:
    """Return true when Phase-1 should expect new Globex futures OHLCV bars."""

    return not bool(classify_phase1_futures_market_session(now, symbol=symbol).get("market_closed"))


def phase1_latest_bar_freshness_seconds(symbol: str | None, base_seconds: float) -> float:
    """Return the tolerated age for the latest completed trade bar.

    ``base_seconds`` remains the feed-liveness threshold for artifact generation.
    Thin contracts can have a live producer with no new completed trade bar, so
    their last-trade bar can be older without making the feed unusable.
    """

    normalized_symbol = str(symbol or "").strip().upper()
    if phase1_symbol_market_freshness_policy(normalized_symbol) != MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED:
        return float(base_seconds)
    thin_tolerance = phase1_symbol_latest_bar_freshness_seconds(normalized_symbol)
    if thin_tolerance is None:
        return float(base_seconds)
    return max(float(base_seconds), float(thin_tolerance))


def phase1_symbol_allows_stale_trade_bars(symbol: str | None) -> bool:
    """Return true when feed liveness can satisfy readiness despite stale trade bars."""

    return phase1_symbol_market_freshness_policy(symbol) == MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED


def phase1_symbol_session_calendar(symbol: str | None) -> str:
    row = _phase1_symbol_config(symbol)
    if row is None:
        return SESSION_CALENDAR_GLOBEX_FUTURES
    return row.session_calendar


def phase1_symbol_market_freshness_policy(symbol: str | None) -> str:
    row = _phase1_symbol_config(symbol)
    if row is None:
        return MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS
    return row.market_freshness_policy


def phase1_symbol_latest_bar_freshness_seconds(symbol: str | None) -> float | None:
    row = _phase1_symbol_config(symbol)
    if row is None or row.latest_bar_freshness_seconds is None:
        return None
    return float(row.latest_bar_freshness_seconds)


def _phase1_symbol_config(symbol: str | None) -> TrackBLiveMarketDataSymbol | None:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return None
    return _phase1_symbol_config_by_symbol().get(normalized_symbol)


@lru_cache(maxsize=1)
def _phase1_symbol_config_by_symbol() -> dict[str, TrackBLiveMarketDataSymbol]:
    try:
        return load_track_b_live_market_data_symbols().by_symbol()
    except Exception:
        return {}
