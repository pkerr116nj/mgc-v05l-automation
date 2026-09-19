"""Start-of-day open-interest gamma concentration analytics.

The signed view is deliberately labeled as an estimate.  Open interest does not
identify the holder, trade direction, or opening/closing status, so it cannot
establish actual dealer positioning.
"""

from __future__ import annotations

import math
from datetime import date, datetime, time, timezone
from typing import Any
from zoneinfo import ZoneInfo

from .products import IndexOptionProduct


EASTERN = ZoneInfo("America/New_York")
SECONDS_PER_YEAR = 365.25 * 24 * 60 * 60
MIN_IV = 0.01
MAX_IV = 3.0


def derive_market_gamma(
    *,
    spot: float | None,
    chains: dict[str, dict[str, list[dict[str, Any]]]],
    product: IndexOptionProduct,
    now: datetime | None = None,
) -> dict[str, Any]:
    observed_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    base: dict[str, Any] = {
        "status": "UNAVAILABLE",
        "reason": None,
        "method": "Schwab start-of-day OI · contract IV · Black-76 gamma · conventional call-plus/put-minus sign",
        "positioning_is_estimated": True,
        "open_interest_basis": "START_OF_DAY",
        "open_interest_source": "SCHWAB_CHAIN",
        "open_interest_source_date": None,
        "product": product.key,
    }
    if spot is None or not math.isfinite(float(spot)) or float(spot) <= 0:
        return {**base, "reason": f"{product.display_symbol} spot is unavailable."}

    contracts: list[dict[str, Any]] = []
    eligible = 0
    with_open_interest = 0
    total_open_interest = 0
    expirations_used: set[str] = set()
    for expiration, sides in chains.items():
        try:
            expiry_day = date.fromisoformat(expiration)
        except ValueError:
            continue
        for option_type in ("CALL", "PUT"):
            for row in sides.get(option_type, []):
                eligible += 1
                open_interest = _nonnegative_int(row.get("open_interest"))
                if open_interest is None:
                    continue
                with_open_interest += 1
                if open_interest == 0:
                    continue
                iv = _normalized_iv(row.get("iv"))
                strike = _positive_float(row.get("strike"))
                if iv is None or strike is None:
                    continue
                root = str(row.get("symbol") or "")[:6].strip().upper()
                settlement_time = time(9, 30) if product.option_roots and root == product.option_roots[0] else time(16, 0)
                expiry_at = datetime.combine(expiry_day, settlement_time, tzinfo=EASTERN).astimezone(timezone.utc)
                time_years = (expiry_at - observed_at).total_seconds() / SECONDS_PER_YEAR
                if time_years <= 0:
                    continue
                total_open_interest += open_interest
                expirations_used.add(expiration)
                contracts.append(
                    {
                        "option_type": option_type,
                        "strike": strike,
                        "iv": iv,
                        "time_years": time_years,
                        "open_interest": open_interest,
                    }
                )

    coverage = with_open_interest / eligible if eligible else 0.0
    if not contracts:
        return {
            **base,
            "reason": "No contracts have usable Schwab open interest, IV, strike, and unexpired time.",
            "eligible_contracts": eligible,
            "contracts_with_open_interest": with_open_interest,
            "coverage_ratio": round(coverage, 4),
        }

    current = _aggregate_at_spot(contracts, float(spot), product.multiplier)
    profile_spots = _profile_spots(float(spot), contracts)
    profile = []
    for candidate in profile_spots:
        aggregate = _aggregate_at_spot(contracts, candidate, product.multiplier)
        profile.append(
            {
                "spot": round(candidate, 4),
                "signed_gex_1pct": round(aggregate["signed"], 2),
                "gross_gex_1pct": round(aggregate["gross"], 2),
                "regime": _regime(aggregate),
            }
        )
    flip = _nearest_flip(profile, float(spot))
    by_strike = _by_strike(contracts, float(spot), product.multiplier)
    call_wall = _largest_strike(by_strike, "call_gex_1pct")
    put_wall = _largest_strike(by_strike, "put_gex_1pct")
    confidence = "HIGH" if coverage >= 0.95 else "MEDIUM" if coverage >= 0.80 else "LOW"

    return {
        **base,
        "status": "VALID",
        "reason": None,
        "generated_at": observed_at.isoformat(),
        "eligible_contracts": eligible,
        "contracts_with_open_interest": with_open_interest,
        "modeled_contracts": len(contracts),
        "expiration_count": len(expirations_used),
        "expirations": sorted(expirations_used),
        "coverage_ratio": round(coverage, 4),
        "confidence": confidence,
        "total_open_interest": total_open_interest,
        "signed_gex_1pct": round(current["signed"], 2),
        "gross_gex_1pct": round(current["gross"], 2),
        "call_gex_1pct": round(current["call"], 2),
        "put_gex_1pct": round(current["put"], 2),
        "regime": _regime(current),
        "estimated_flip": round(flip, 4) if flip is not None else None,
        "estimated_flip_distance": round(flip - float(spot), 4) if flip is not None else None,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "by_strike": by_strike,
        "scenario_profile": profile,
        "interpretation": (
            "Positive estimated gamma implies conventionally stabilizing hedge flows; negative estimated gamma implies "
            "potentially amplifying hedge flows. The sign is an open-interest convention, not observed dealer inventory."
        ),
    }


def _aggregate_at_spot(contracts: list[dict[str, Any]], spot: float, multiplier: int) -> dict[str, float]:
    call = 0.0
    put = 0.0
    for contract in contracts:
        gamma = _black_forward_gamma(spot, contract["strike"], contract["time_years"], contract["iv"])
        exposure = gamma * contract["open_interest"] * multiplier * spot * spot * 0.01
        if contract["option_type"] == "CALL":
            call += exposure
        else:
            put += exposure
    return {"call": call, "put": put, "gross": call + put, "signed": call - put}


def _by_strike(contracts: list[dict[str, Any]], spot: float, multiplier: int) -> list[dict[str, Any]]:
    rows: dict[float, dict[str, float]] = {}
    for contract in contracts:
        gamma = _black_forward_gamma(spot, contract["strike"], contract["time_years"], contract["iv"])
        exposure = gamma * contract["open_interest"] * multiplier * spot * spot * 0.01
        row = rows.setdefault(contract["strike"], {"call_gex_1pct": 0.0, "put_gex_1pct": 0.0})
        row["call_gex_1pct" if contract["option_type"] == "CALL" else "put_gex_1pct"] += exposure
    return [
        {
            "strike": strike,
            "call_gex_1pct": round(values["call_gex_1pct"], 2),
            "put_gex_1pct": round(values["put_gex_1pct"], 2),
            "signed_gex_1pct": round(values["call_gex_1pct"] - values["put_gex_1pct"], 2),
        }
        for strike, values in sorted(rows.items())
    ]


def _profile_spots(spot: float, contracts: list[dict[str, Any]]) -> list[float]:
    strikes = sorted({contract["strike"] for contract in contracts})
    nearby = [strike for strike in strikes if spot * 0.90 <= strike <= spot * 1.10]
    candidates = sorted(set(nearby + [spot]))
    if len(candidates) <= 161:
        return candidates
    step = max(1, math.ceil(len(candidates) / 160))
    sampled = candidates[::step]
    return sorted(set(sampled + [spot]))


def _nearest_flip(profile: list[dict[str, Any]], spot: float) -> float | None:
    if not profile or all(abs(float(row["signed_gex_1pct"])) < 0.01 for row in profile):
        return None
    flips: list[float] = []
    for left, right in zip(profile, profile[1:]):
        left_value = float(left["signed_gex_1pct"])
        right_value = float(right["signed_gex_1pct"])
        if left_value == 0:
            flips.append(float(left["spot"]))
        elif left_value * right_value < 0:
            fraction = abs(left_value) / (abs(left_value) + abs(right_value))
            flips.append(float(left["spot"]) + fraction * (float(right["spot"]) - float(left["spot"])))
    return min(flips, key=lambda value: abs(value - spot)) if flips else None


def _regime(aggregate: dict[str, float]) -> str:
    gross = abs(float(aggregate.get("gross") or 0))
    signed = float(aggregate.get("signed") or 0)
    if gross <= 0 or abs(signed) / gross < 0.005:
        return "NEUTRAL"
    return "POSITIVE" if signed > 0 else "NEGATIVE"


def _largest_strike(rows: list[dict[str, Any]], key: str) -> dict[str, float] | None:
    if not rows:
        return None
    row = max(rows, key=lambda value: float(value.get(key) or 0))
    return {"strike": float(row["strike"]), "gex_1pct": float(row.get(key) or 0)}


def _black_forward_gamma(forward: float, strike: float, time_years: float, volatility: float) -> float:
    root_time = math.sqrt(max(time_years, 1e-12))
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * time_years) / (volatility * root_time)
    return math.exp(-0.5 * d1 * d1) / (math.sqrt(2.0 * math.pi) * forward * volatility * root_time)


def _normalized_iv(value: Any) -> float | None:
    try:
        iv = float(value)
    except (TypeError, ValueError):
        return None
    if iv > 3.0:
        iv /= 100.0
    return iv if math.isfinite(iv) and MIN_IV <= iv <= MAX_IV else None


def _positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _nonnegative_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number >= 0 else None
