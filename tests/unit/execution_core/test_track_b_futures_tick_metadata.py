from __future__ import annotations

from decimal import Decimal

import pytest

from mgc_v05l.execution_core.track_b_futures_tick_metadata import (
    futures_tick_metadata,
    marketable_price_for_action,
    round_price_to_tick,
)


@pytest.mark.parametrize(
    ("symbol", "min_tick", "multiplier", "tick_value"),
    (
        ("ZT", Decimal("0.00390625"), Decimal("2000"), Decimal("7.8125")),
        ("ZF", Decimal("0.0078125"), Decimal("1000"), Decimal("7.8125")),
        ("ZN", Decimal("0.015625"), Decimal("1000"), Decimal("15.625")),
        ("ZB", Decimal("0.03125"), Decimal("1000"), Decimal("31.25")),
    ),
)
def test_rates_tick_metadata_uses_fractional_treasury_ticks(
    symbol: str,
    min_tick: Decimal,
    multiplier: Decimal,
    tick_value: Decimal,
) -> None:
    metadata = futures_tick_metadata(symbol)

    assert metadata is not None
    assert metadata.exchange == "CBOT"
    assert metadata.min_tick == min_tick
    assert metadata.multiplier == multiplier
    assert metadata.tick_value == tick_value
    assert metadata.fractional_pricing is True


def test_fractional_treasury_prices_round_to_valid_ticks() -> None:
    assert round_price_to_tick("104.126", "0.00390625") == Decimal("104.12500000")
    assert round_price_to_tick("110.016", "0.015625") == Decimal("110.015625")
    assert round_price_to_tick("121.17", "0.03125") == Decimal("121.15625")


def test_marketable_rates_prices_round_in_risk_direction() -> None:
    assert marketable_price_for_action(
        reference_price="121.15625",
        action="BUY",
        offset_ticks=1,
        min_tick="0.03125",
    ) == Decimal("121.18750")
    assert marketable_price_for_action(
        reference_price="121.15625",
        action="SELL",
        offset_ticks=1,
        min_tick="0.03125",
    ) == Decimal("121.12500")
