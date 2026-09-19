"""Product definitions for the Schwab index-spread terminal."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal


@dataclass(frozen=True)
class IndexOptionProduct:
    key: str
    display_symbol: str
    name: str
    chain_symbol: str
    quote_symbol: str
    option_roots: tuple[str, ...]
    spread_widths: tuple[int, ...]
    default_width: int
    multiplier: int
    exercise_style: str
    settlement: str

    def public(self) -> dict[str, object]:
        return asdict(self)


PRODUCTS: dict[str, IndexOptionProduct] = {
    "NDX": IndexOptionProduct(
        key="NDX",
        display_symbol="NDX",
        name="NASDAQ 100 INDEX",
        chain_symbol="$NDX",
        quote_symbol="$NDX",
        option_roots=("NDX", "NDXP"),
        spread_widths=(5, 10, 20),
        default_width=10,
        multiplier=100,
        exercise_style="EUROPEAN",
        settlement="CASH · AM/PM BY SERIES",
    ),
    "SPX": IndexOptionProduct(
        key="SPX",
        display_symbol="SPX",
        name="S&P 500 INDEX",
        chain_symbol="$SPX",
        quote_symbol="$SPX",
        option_roots=("SPX", "SPXW"),
        spread_widths=(5, 10, 20),
        default_width=5,
        multiplier=100,
        exercise_style="EUROPEAN",
        settlement="CASH · AM/PM BY SERIES",
    ),
    "RUT": IndexOptionProduct(
        key="RUT",
        display_symbol="RUT",
        name="RUSSELL 2000 INDEX",
        chain_symbol="$RUT",
        quote_symbol="$RUT",
        option_roots=("RUT", "RUTW"),
        spread_widths=(5, 10, 20),
        default_width=5,
        multiplier=100,
        exercise_style="EUROPEAN",
        settlement="CASH · AM/PM BY SERIES",
    ),
}


def get_product(value: str | None) -> IndexOptionProduct:
    key = str(value or "NDX").strip().upper().lstrip("$")
    try:
        return PRODUCTS[key]
    except KeyError as exc:
        raise ValueError(f"Unsupported index option product: {value!r}.") from exc


def product_for_root(root: str) -> IndexOptionProduct | None:
    normalized = str(root or "").strip().upper().lstrip("$")
    return next((product for product in PRODUCTS.values() if normalized in product.option_roots), None)


def width_allowed(product: IndexOptionProduct, width: Decimal | int | float) -> bool:
    candidate = Decimal(str(width))
    return any(candidate == Decimal(value) for value in product.spread_widths)
