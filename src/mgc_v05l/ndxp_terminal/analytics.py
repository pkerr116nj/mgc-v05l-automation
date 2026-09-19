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
MAX_CLOSED_SNAPSHOT_SKEW_MS = 2 * 60 * 60 * 1000
MIN_SURFACE_POINTS = 6


def derive_expiration_analytics(
    *,
    spot: float | None,
    expiration: str | None,
    chain: dict[str, list[dict[str, Any]]],
    spot_quote_time_ms: int | None,
    allow_closed_snapshot: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build selected-expiration analytics, or explain why they are unavailable."""

    requested_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
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

    calls = _by_strike(chain.get("CALL", []))
    puts = _by_strike(chain.get("PUT", []))
    shared = sorted(set(calls) & set(puts), key=lambda strike: abs(strike - float(spot)))
    reference_timestamps = [
        timestamp
        for strike in shared[:8]
        for timestamp in (
            _timestamp(calls[strike].get("quote_time_ms")),
            _timestamp(puts[strike].get("quote_time_ms")),
        )
        if timestamp is not None
    ]
    if spot_quote_time_ms is not None:
        reference_timestamps.append(spot_quote_time_ms)
    model_observed_at = requested_at
    if allow_closed_snapshot:
        if not reference_timestamps:
            return _unavailable(base, "Closed-snapshot timestamps are unavailable.")
        model_observed_at = datetime.fromtimestamp(min(reference_timestamps) / 1000, tz=timezone.utc)
    seconds_remaining = (expiry_at - model_observed_at).total_seconds()
    if seconds_remaining <= 0:
        return _unavailable(base, "The selected NDXP expiration has passed.")
    time_years = seconds_remaining / SECONDS_PER_YEAR

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
    newest_ms = max(timestamps)
    quote_age_ms = max(0.0, requested_at.timestamp() * 1000 - oldest_ms)
    input_skew_ms = newest_ms - oldest_ms
    quote_mode = "LIVE"
    if allow_closed_snapshot:
        eastern_dates = {
            datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).astimezone(EASTERN).date()
            for timestamp in timestamps
        }
        if len(eastern_dates) != 1:
            return _unavailable(base, "Closed-snapshot model inputs do not share one Eastern market date.")
        if input_skew_ms > MAX_CLOSED_SNAPSHOT_SKEW_MS:
            return _unavailable(
                base,
                f"Closed-snapshot model inputs are {input_skew_ms / 1000:.1f}s apart.",
            )
        quote_mode = "CLOSED_SNAPSHOT"
    elif quote_age_ms > MAX_SURFACE_QUOTE_AGE_MS:
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
        "model_input_time_ms": oldest_ms,
        "model_input_skew_ms": input_skew_ms,
        "quote_mode": quote_mode,
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
    short_iv_expected_move = spot * short_iv * math.sqrt(time_years)
    short_delta = _black_forward_delta(option_type, forward, short_strike, time_years, short_iv)
    long_delta = _black_forward_delta(option_type, forward, long_strike, time_years, long_iv)
    credit_position_delta = long_delta - short_delta
    short_gamma = _black_forward_gamma(forward, short_strike, time_years, short_iv)
    long_gamma = _black_forward_gamma(forward, long_strike, time_years, long_iv)
    credit_position_gamma = long_gamma - short_gamma
    gamma_scenarios = _gamma_scenarios(
        option_type=option_type,
        spot=spot,
        forward=forward,
        short_strike=short_strike,
        long_strike=long_strike,
        time_years=time_years,
        short_iv=short_iv,
        long_iv=long_iv,
    )
    gamma_flip_spot = _nearest_gamma_flip_spot(
        spot=spot,
        forward=forward,
        short_strike=short_strike,
        long_strike=long_strike,
        time_years=time_years,
        short_iv=short_iv,
        long_iv=long_iv,
        expected_move=expected_move,
    )
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
        "short_iv": round(short_iv, 8),
        "short_iv_percent": round(short_iv * 100, 4),
        "short_iv_expected_move": round(short_iv_expected_move, 4),
        "short_iv_em_multiple": round(directional_distance / short_iv_expected_move, 6),
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
        "spread_gamma": round(-credit_position_gamma, 8),
        "credit_position_gamma": round(credit_position_gamma, 8),
        "gamma_scenarios": gamma_scenarios,
        "gamma_flip_spot": round(gamma_flip_spot, 4) if gamma_flip_spot is not None else None,
        "gamma_flip_distance": round(gamma_flip_spot - spot, 4) if gamma_flip_spot is not None else None,
        "gamma_method": "Black-76 forward gamma · fitted leg IVs held constant across spot scenarios",
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


def _black_forward_gamma(forward: float, strike: float, time_years: float, volatility: float) -> float:
    root_time = math.sqrt(time_years)
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * time_years) / (volatility * root_time)
    return math.exp(-0.5 * d1 * d1) / (math.sqrt(2.0 * math.pi) * forward * volatility * root_time)


def _credit_position_greeks(
    *,
    option_type: str,
    forward: float,
    short_strike: float,
    long_strike: float,
    time_years: float,
    short_iv: float,
    long_iv: float,
) -> tuple[float, float]:
    short_delta = _black_forward_delta(option_type, forward, short_strike, time_years, short_iv)
    long_delta = _black_forward_delta(option_type, forward, long_strike, time_years, long_iv)
    short_gamma = _black_forward_gamma(forward, short_strike, time_years, short_iv)
    long_gamma = _black_forward_gamma(forward, long_strike, time_years, long_iv)
    return long_delta - short_delta, long_gamma - short_gamma


def _gamma_scenarios(
    *,
    option_type: str,
    spot: float,
    forward: float,
    short_strike: float,
    long_strike: float,
    time_years: float,
    short_iv: float,
    long_iv: float,
) -> list[dict[str, float]]:
    scenarios: list[dict[str, float]] = []
    for spot_move in (-25.0, -10.0, 0.0, 10.0, 25.0):
        scenario_forward = forward + spot_move
        if scenario_forward <= 0:
            continue
        delta, gamma = _credit_position_greeks(
            option_type=option_type,
            forward=scenario_forward,
            short_strike=short_strike,
            long_strike=long_strike,
            time_years=time_years,
            short_iv=short_iv,
            long_iv=long_iv,
        )
        scenarios.append(
            {
                "spot_move": spot_move,
                "spot": round(spot + spot_move, 4),
                "credit_position_delta": round(delta, 8),
                "credit_position_gamma": round(gamma, 8),
            }
        )
    return scenarios


def _nearest_gamma_flip_spot(
    *,
    spot: float,
    forward: float,
    short_strike: float,
    long_strike: float,
    time_years: float,
    short_iv: float,
    long_iv: float,
    expected_move: float,
) -> float | None:
    radius = max(50.0, 2.0 * expected_move, abs(short_strike - long_strike) * 5.0)
    lower_spot = max(0.01, spot - radius)
    upper_spot = spot + radius

    def gamma_at(candidate_spot: float) -> float:
        candidate_forward = max(0.01, forward + candidate_spot - spot)
        return _black_forward_gamma(candidate_forward, long_strike, time_years, long_iv) - _black_forward_gamma(
            candidate_forward, short_strike, time_years, short_iv
        )

    samples = 80
    points = [lower_spot + (upper_spot - lower_spot) * index / samples for index in range(samples + 1)]
    roots: list[float] = []
    left_spot = points[0]
    left_gamma = gamma_at(left_spot)
    for right_spot in points[1:]:
        right_gamma = gamma_at(right_spot)
        if left_gamma * right_gamma < 0:
            low, high = left_spot, right_spot
            low_gamma = left_gamma
            for _ in range(50):
                middle = (low + high) / 2
                middle_gamma = gamma_at(middle)
                if low_gamma * middle_gamma <= 0:
                    high = middle
                else:
                    low = middle
                    low_gamma = middle_gamma
            roots.append((low + high) / 2)
        left_spot, left_gamma = right_spot, right_gamma
    return min(roots, key=lambda value: abs(value - spot)) if roots else None


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
