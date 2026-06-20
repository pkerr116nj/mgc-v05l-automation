"""Shared Track B futures tick metadata and price rounding."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN
from typing import Any, Literal


RoundingMode = Literal["nearest", "ceil", "floor"]


@dataclass(frozen=True)
class FuturesTickMetadata:
    symbol: str
    min_tick: Decimal
    multiplier: Decimal
    tick_value: Decimal
    exchange: str
    fractional_pricing: bool = False

    def as_dict(self) -> dict[str, str | bool]:
        return {
            "symbol": self.symbol,
            "min_tick": str(self.min_tick),
            "multiplier": str(self.multiplier),
            "tick_value": str(self.tick_value),
            "exchange": self.exchange,
            "fractional_pricing": self.fractional_pricing,
        }


_FUTURES_TICK_METADATA: dict[str, FuturesTickMetadata] = {
    "MGC": FuturesTickMetadata(
        symbol="MGC",
        min_tick=Decimal("0.1"),
        multiplier=Decimal("10"),
        tick_value=Decimal("1.0"),
        exchange="COMEX",
    ),
    "GC": FuturesTickMetadata(
        symbol="GC",
        min_tick=Decimal("0.1"),
        multiplier=Decimal("100"),
        tick_value=Decimal("10.0"),
        exchange="COMEX",
    ),
    "MNQ": FuturesTickMetadata(
        symbol="MNQ",
        min_tick=Decimal("0.25"),
        multiplier=Decimal("2"),
        tick_value=Decimal("0.50"),
        exchange="CME",
    ),
    "NQ": FuturesTickMetadata(
        symbol="NQ",
        min_tick=Decimal("0.25"),
        multiplier=Decimal("20"),
        tick_value=Decimal("5.00"),
        exchange="CME",
    ),
    "MES": FuturesTickMetadata(
        symbol="MES",
        min_tick=Decimal("0.25"),
        multiplier=Decimal("5"),
        tick_value=Decimal("1.25"),
        exchange="CME",
    ),
    "ES": FuturesTickMetadata(
        symbol="ES",
        min_tick=Decimal("0.25"),
        multiplier=Decimal("50"),
        tick_value=Decimal("12.50"),
        exchange="CME",
    ),
    "ZT": FuturesTickMetadata(
        symbol="ZT",
        min_tick=Decimal("0.00390625"),
        multiplier=Decimal("2000"),
        tick_value=Decimal("7.8125"),
        exchange="CBOT",
        fractional_pricing=True,
    ),
    "ZF": FuturesTickMetadata(
        symbol="ZF",
        min_tick=Decimal("0.0078125"),
        multiplier=Decimal("1000"),
        tick_value=Decimal("7.8125"),
        exchange="CBOT",
        fractional_pricing=True,
    ),
    "ZN": FuturesTickMetadata(
        symbol="ZN",
        min_tick=Decimal("0.015625"),
        multiplier=Decimal("1000"),
        tick_value=Decimal("15.625"),
        exchange="CBOT",
        fractional_pricing=True,
    ),
    "ZB": FuturesTickMetadata(
        symbol="ZB",
        min_tick=Decimal("0.03125"),
        multiplier=Decimal("1000"),
        tick_value=Decimal("31.25"),
        exchange="CBOT",
        fractional_pricing=True,
    ),
    "BTC": FuturesTickMetadata(
        symbol="BTC",
        min_tick=Decimal("5"),
        multiplier=Decimal("5"),
        tick_value=Decimal("25"),
        exchange="CME",
    ),
    "MBT": FuturesTickMetadata(
        symbol="MBT",
        min_tick=Decimal("5"),
        multiplier=Decimal("0.1"),
        tick_value=Decimal("0.5"),
        exchange="CME",
    ),
}


def futures_tick_metadata(symbol: str) -> FuturesTickMetadata | None:
    return _FUTURES_TICK_METADATA.get(str(symbol or "").strip().upper())


def futures_tick_metadata_by_symbol() -> dict[str, FuturesTickMetadata]:
    return dict(_FUTURES_TICK_METADATA)


def decimal_price(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def round_price_to_tick(price: Any, min_tick: Any, *, mode: RoundingMode = "nearest") -> Decimal:
    tick = decimal_price(min_tick)
    if tick <= 0:
        return decimal_price(price)
    raw_ticks = decimal_price(price) / tick
    if mode == "ceil":
        rounded_ticks = raw_ticks.to_integral_value(rounding=ROUND_CEILING)
    elif mode == "floor":
        rounded_ticks = raw_ticks.to_integral_value(rounding=ROUND_FLOOR)
    else:
        rounded_ticks = raw_ticks.to_integral_value(rounding=ROUND_HALF_EVEN)
    return rounded_ticks * tick


def round_price_to_tick_float(price: Any, min_tick: Any, *, mode: RoundingMode = "nearest") -> float:
    return float(round_price_to_tick(price, min_tick, mode=mode))


def marketable_price_for_action(
    *,
    reference_price: Any,
    action: str,
    offset_ticks: Any,
    min_tick: Any,
) -> Decimal:
    tick = decimal_price(min_tick)
    offset = decimal_price(offset_ticks) * tick
    normalized_action = str(action or "").strip().upper()
    raw_price = (
        decimal_price(reference_price) + offset
        if normalized_action == "BUY"
        else decimal_price(reference_price) - offset
    )
    mode: RoundingMode = "ceil" if normalized_action == "BUY" else "floor"
    return round_price_to_tick(raw_price, tick, mode=mode)
