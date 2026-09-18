"""Independent, fail-closed IV and expected-range analytics for NDXP.

The calculations intentionally use option bid/ask midpoints and never consume
Schwab's supplied volatility or Greeks.  NDXP is a European-style,
cash-settled product, so call/put parity and Black-76 are suitable for the
short-dated review analytics exposed by the terminal.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from statistics import median
from typing import Any
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
SECONDS_PER_YEAR = 365.25 * 24 * 60 * 60
MAX_SURFACE_QUOTE_AGE_MS = 15_000
MIN_SURFACE_POINTS = 6


def derive_expiration_analytics(
    *,
    spot: float | None,
    expiration: str | None,
    chain: dict[str, list[dict[str, Any]]],
    spot_quote_time_ms: int | None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build selected-expiration analytics, or explain why they are unavailable."""

    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    base: dict[str, Any] = {
        "status": "UNAVAILABLE",
        "reason": None,
        "method": "Option-mid IV · parity forward · piecewise-linear smile · Black-76",
        "independent_of_schwab_greeks": True,
        "spreads": {},
    }
    if spot is None or not math.isfinite(float(spot)) or float(spot) <= 0:
        return _unavailable(base, "NDX spot is unavailable.")
    try:
        expiry_at = _expiration_at(expiration)
    except (TypeError, ValueError):
        return _unavailable(base, "The selected expiration is invalid.")
    seconds_remaining = (expiry_at - observed_at).total_seconds()
    if seconds_remaining <= 0:
        return _unavailable(base, "The selected NDXP expiration has passed.")
    time_years = seconds_remaining / SECONDS_PER_YEAR

    calls = _by_strike(chain.get("CALL", []))
    puts = _by_strike(chain.get("PUT", []))
    shared = sorted(set(calls) & set(puts), key=lambda strike: abs(strike - float(spot)))
    parity_rows: list[tuple[float, float, int]] = []
    for strike in shared[:40]:
        call_mid = _quote_mid(calls[strike])
        put_mid = _quote_mid(puts[strike])
        quote_ms = _oldest_timestamp(calls[strike].get("quote_time_ms"), puts[strike].get("quote_time_ms"))
        if call_mid is None or put_mid is None or quote_ms is None:
            continue
        parity_rows.append((strike, strike + call_mid - put_mid, quote_ms))
    if len(parity_rows) < MIN_SURFACE_POINTS:
        return _unavailable(base, "Too few synchronized call/put mids to infer the forward.")

    forward = median(row[1] for row in parity_rows)
    parity_error = math.sqrt(sum((row[1] - forward) ** 2 for row in parity_rows) / len(parity_rows))
    if not (0.95 * float(spot) <= forward <= 1.05 * float(spot)):
        return _unavailable(base, "Call/put parity produced an implausible forward.")
    if parity_error > 5.0:
        return _unavailable(base, "Call/put parity is internally inconsistent across current mids.")

    surface: list[tuple[float, float, int]] = []
    for strike in sorted(set(calls) | set(puts)):
        preferred = puts.get(strike) if strike < forward else calls.get(strike)
        fallback = calls.get(strike) if strike < forward else puts.get(strike)
        contract = preferred or fallback
        if not contract:
            continue
        option_type = "PUT" if contract is puts.get(strike) else "CALL"
        mid = _quote_mid(contract)
        quote_ms = _timestamp(contract.get("quote_time_ms"))
        if mid is None or quote_ms is None:
            continue
        iv = _implied_volatility(
            price=mid,
            forward=forward,
            strike=strike,
            time_years=time_years,
            option_type=option_type,
        )
        if iv is not None:
            surface.append((strike, iv, quote_ms))
    if len(surface) < MIN_SURFACE_POINTS:
        return _unavailable(base, "Too few valid option mids to fit an implied-volatility smile.")

    used_near_atm = sorted(surface, key=lambda row: abs(row[0] - forward))[:8]
    timestamps = [row[2] for row in used_near_atm]
    if spot_quote_time_ms is None:
        return _unavailable(base, "The NDX spot quote timestamp is unavailable.")
    timestamps.append(spot_quote_time_ms)
    oldest_ms = min(timestamps)
    quote_age_ms = max(0.0, observed_at.timestamp() * 1000 - oldest_ms)
    if quote_age_ms > MAX_SURFACE_QUOTE_AGE_MS:
        return _unavailable(base, f"Model inputs are stale ({quote_age_ms / 1000:.1f}s old).")

    atm_iv = _smile_iv(surface, forward)
    if atm_iv is None or not (0.02 <= atm_iv <= 2.0):
        return _unavailable(base, "The independently derived ATM volatility is implausible.")
    expected_move = float(spot) * atm_iv * math.sqrt(time_years)
    if not math.isfinite(expected_move) or expected_move <= 0:
        return _unavailable(base, "The expected move could not be calculated.")

    spreads: dict[str, dict[str, Any]] = {}
    for option_type, side in (("CALL", calls), ("PUT", puts)):
        for short_strike, short in side.items():
            long_strike = short_strike + 10 if option_type == "CALL" else short_strike - 10
            long = side.get(long_strike)
            if long is None:
                continue
            spread = _spread_analytics(
                option_type=option_type,
                short=short,
                long=long,
                spot=float(spot),
                forward=forward,
                time_years=time_years,
                expected_move=expected_move,
                surface=surface,
            )
            if spread is not None:
                spreads[str(short["symbol"])] = spread

    return {
        **base,
        "status": "VALID",
        "reason": None,
        "expiration_at": expiry_at.isoformat(),
        "seconds_remaining": round(seconds_remaining, 3),
        "time_years": time_years,
        "spot": round(float(spot), 4),
        "forward": round(forward, 4),
        "parity_rms_error": round(parity_error, 4),
        "surface_points": len(surface),
        "oldest_model_quote_age_ms": round(quote_age_ms, 1),
        "atm_iv": atm_iv,
        "atm_iv_percent": round(atm_iv * 100, 4),
        "expected_move": round(expected_move, 4),
        "ranges": {
            str(multiplier): {
                "lower": round(float(spot) - multiplier * expected_move, 4),
                "upper": round(float(spot) + multiplier * expected_move, 4),
            }
            for multiplier in (0.5, 1.0, 1.5)
        },
        "spreads": spreads,
    }


def _spread_analytics(
    *,
    option_type: str,
    short: dict[str, Any],
    long: dict[str, Any],
    spot: float,
    forward: float,
    time_years: float,
    expected_move: float,
    surface: list[tuple[float, float, int]],
) -> dict[str, Any] | None:
    short_bid = _number(short.get("bid"))
    short_ask = _number(short.get("ask"))
    long_bid = _number(long.get("bid"))
    long_ask = _number(long.get("ask"))
    if None in {short_bid, short_ask, long_bid, long_ask}:
        return None
    bid = short_bid - long_ask
    ask = short_ask - long_bid
    mid = (bid + ask) / 2
    width = abs(float(short["strike"]) - float(long["strike"]))
    if mid <= 0 or mid >= width or ask < bid:
        return None
    short_strike = float(short["strike"])
    breakeven = short_strike + mid if option_type == "CALL" else short_strike - mid
    directional_distance = breakeven - spot if option_type == "CALL" else spot - breakeven
    short_distance = short_strike - spot if option_type == "CALL" else spot - short_strike
    breakeven_iv = _smile_iv(surface, breakeven)
    short_iv = _smile_iv(surface, short_strike)
    long_strike = float(long["strike"])
    long_iv = _smile_iv(surface, long_strike)
    if breakeven_iv is None or short_iv is None or long_iv is None:
        return None
    short_delta = _black_forward_delta(option_type, forward, short_strike, time_years, short_iv)
    long_delta = _black_forward_delta(option_type, forward, long_strike, time_years, long_iv)
    credit_position_delta = long_delta - short_delta
    return {
        "option_type": option_type,
        "short_strike": short_strike,
        "long_strike": long_strike,
        "opening_bid": round(bid, 4),
        "opening_mid": round(mid, 4),
        "opening_ask": round(ask, 4),
        "market_width": round(ask - bid, 4),
        "short_distance": round(short_distance, 4),
        "breakeven": round(breakeven, 4),
        "breakeven_distance": round(directional_distance, 4),
        "em_multiple": round(directional_distance / expected_move, 6),
        "probability_beyond_breakeven": round(
            _tail_probability(option_type, forward, breakeven, time_years, breakeven_iv), 8
        ),
        "probability_beyond_short": round(
            _tail_probability(option_type, forward, short_strike, time_years, short_iv), 8
        ),
        "credit_to_risk": round(mid / (width - mid), 8),
        "model_iv_at_breakeven": round(breakeven_iv, 8),
        "spread_delta": round(-credit_position_delta, 8),
        "credit_position_delta": round(credit_position_delta, 8),
        "credit_band": "PREFERRED" if 2.0 <= mid <= 2.5 else ("BELOW_PREFERRED" if mid < 2.0 else "ELEVATED"),
    }


def _expiration_at(expiration: str | None) -> datetime:
    expiry_day = date.fromisoformat(str(expiration))
    return datetime.combine(expiry_day, time(16, 0), tzinfo=EASTERN).astimezone(timezone.utc)


def _by_strike(rows: list[dict[str, Any]]) -> dict[float, dict[str, Any]]:
    result: dict[float, dict[str, Any]] = {}
    for row in rows:
        strike = _number(row.get("strike"))
        if strike is not None and row.get("symbol"):
            result[strike] = row
    return result


def _quote_mid(contract: dict[str, Any]) -> float | None:
    bid = _number(contract.get("bid"))
    ask = _number(contract.get("ask"))
    if bid is None or ask is None or bid < 0 or ask <= 0 or ask < bid:
        return None
    return (bid + ask) / 2


def _implied_volatility(
    *, price: float, forward: float, strike: float, time_years: float, option_type: str
) -> float | None:
    intrinsic = max(forward - strike, 0.0) if option_type == "CALL" else max(strike - forward, 0.0)
    upper = forward if option_type == "CALL" else strike
    if price <= intrinsic + 1e-6 or price >= upper:
        return None
    low, high = 0.0001, 5.0
    for _ in range(100):
        mid = (low + high) / 2
        model = _black_price(option_type, forward, strike, time_years, mid)
        if model > price:
            high = mid
        else:
            low = mid
    solved = (low + high) / 2
    return solved if 0.005 <= solved <= 3.0 else None


def _black_price(option_type: str, forward: float, strike: float, time_years: float, volatility: float) -> float:
    root_time = math.sqrt(time_years)
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * time_years) / (volatility * root_time)
    d2 = d1 - volatility * root_time
    if option_type == "CALL":
        return forward * _normal_cdf(d1) - strike * _normal_cdf(d2)
    return strike * _normal_cdf(-d2) - forward * _normal_cdf(-d1)


def _black_forward_delta(option_type: str, forward: float, strike: float, time_years: float, volatility: float) -> float:
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * time_years) / (
        volatility * math.sqrt(time_years)
    )
    return _normal_cdf(d1) if option_type == "CALL" else _normal_cdf(d1) - 1.0


def _tail_probability(option_type: str, forward: float, strike: float, time_years: float, volatility: float) -> float:
    d2 = (math.log(forward / strike) - 0.5 * volatility * volatility * time_years) / (volatility * math.sqrt(time_years))
    return _normal_cdf(d2) if option_type == "CALL" else _normal_cdf(-d2)


def _smile_iv(surface: list[tuple[float, float, int]], target: float) -> float | None:
    ordered = sorted(surface)
    if not ordered:
        return None
    if target <= ordered[0][0]:
        return ordered[0][1]
    if target >= ordered[-1][0]:
        return ordered[-1][1]
    for left, right in zip(ordered, ordered[1:]):
        if left[0] <= target <= right[0]:
            if right[0] == left[0]:
                return left[1]
            weight = (target - left[0]) / (right[0] - left[0])
            return left[1] + weight * (right[1] - left[1])
    return None


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _number(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def _timestamp(value: Any) -> int | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric > 0 else None


def _oldest_timestamp(*values: Any) -> int | None:
    timestamps = [timestamp for value in values if (timestamp := _timestamp(value)) is not None]
    return min(timestamps) if timestamps else None


def _unavailable(base: dict[str, Any], reason: str) -> dict[str, Any]:
    return {**base, "reason": reason}
