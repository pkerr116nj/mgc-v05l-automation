"""Session-aware classification for Phase-1 runtime market-data freshness."""

from __future__ import annotations

from datetime import datetime, time

from mgc_v05l.session_phase_labels import NEW_YORK


MARKET_CLOSED_NO_FRESH_BARS = "MARKET_CLOSED_NO_FRESH_BARS"
MARKET_OPEN_EXPECT_FRESH_BARS = "MARKET_OPEN_EXPECT_FRESH_BARS"
CRYPTO_FUTURES_SYMBOLS = frozenset({"BTC", "MBT", "ETH", "MET", "SOL", "MSL"})
THIN_LAST_TRADE_FRESHNESS_SECONDS_BY_SYMBOL = {
    "BTC": 3600.0,
    "MBT": 3600.0,
    "ETH": 3600.0,
    "MET": 3600.0,
    "SOL": 3600.0,
    "MSL": 3600.0,
    "PL": 1800.0,
}


def classify_phase1_futures_market_session(now: datetime, *, symbol: str | None = None) -> dict[str, object]:
    """Classify whether fresh CME Globex futures bars are expected right now."""

    local_dt = now.astimezone(NEW_YORK) if now.tzinfo is not None else now.replace(tzinfo=NEW_YORK)
    local_time = local_dt.timetz().replace(tzinfo=None)
    normalized_symbol = str(symbol or "").strip().upper()
    weekday = local_dt.weekday()
    closed = False
    reason = "GLOBEX_SESSION_OPEN"

    if normalized_symbol in CRYPTO_FUTURES_SYMBOLS:
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
    thin_tolerance = THIN_LAST_TRADE_FRESHNESS_SECONDS_BY_SYMBOL.get(normalized_symbol)
    if thin_tolerance is None:
        return float(base_seconds)
    return max(float(base_seconds), float(thin_tolerance))
