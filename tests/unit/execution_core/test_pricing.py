from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mgc_v05l.execution_core.pricing import (
    MarketDataMode,
    MarketDataRole,
    PricingError,
    QuoteObservation,
    create_marketable_limit_decision,
    round_down_to_tick,
    round_up_to_tick,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def quote(**overrides: object) -> QuoteObservation:
    kwargs = {
        "quote_id": "quote-1",
        "run_id": "run-1",
        "contract_key": "MGC-202606",
        "source": "fake_adapter",
        "bid": "2345.0",
        "ask": "2345.1",
        "last": "2345.05",
        "observed_at": aware_now(),
    }
    kwargs.update(overrides)
    return QuoteObservation(**kwargs)


def test_marketable_buy_limit_is_rounded_up_and_bounded() -> None:
    decision = create_marketable_limit_decision(
        pricing_decision_id="pricing-1",
        quote=quote(),
        action="BUY",
        tick_size="0.1",
        fill_offset_ticks=1,
        max_quote_age_seconds=30,
        max_distance_ticks=10,
        max_distance_percent="0.25",
        now=aware_now(),
    )

    assert decision.limit_price == Decimal("2345.2")
    assert decision.mid == Decimal("2345.05")


def test_quote_observation_carries_market_data_reporting_fields() -> None:
    observed = quote(
        market_data_provider="IBKR",
        market_data_mode=MarketDataMode.DELAYED,
        market_data_role=MarketDataRole.BACKUP,
        delayed_data_warning_seen=True,
        tick_size="0.1",
        exchange="COMEX",
        currency="USD",
        provider_warnings=("10167: delayed data",),
    )

    payload = observed.to_json_dict()

    assert payload["market_data_provider"] == "IBKR"
    assert payload["market_data_mode"] == "DELAYED"
    assert payload["market_data_role"] == "BACKUP"
    assert payload["delayed_data_warning_seen"] is True
    assert payload["timestamp"] == "2026-05-02T12:00:00+00:00"
    assert payload["tick_size"] == "0.1"
    assert payload["exchange"] == "COMEX"
    assert payload["currency"] == "USD"
    assert payload["provider_warnings"] == ["10167: delayed data"]


def test_marketable_sell_limit_is_rounded_down_and_bounded() -> None:
    decision = create_marketable_limit_decision(
        pricing_decision_id="pricing-1",
        quote=quote(),
        action="SELL",
        tick_size="0.1",
        fill_offset_ticks=1,
        max_quote_age_seconds=30,
        max_distance_ticks=10,
        max_distance_percent="0.25",
        now=aware_now(),
    )

    assert decision.limit_price == Decimal("2344.9")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"bid": None}, "bid is required"),
        ({"observed_at": aware_now() - timedelta(seconds=31)}, "stale"),
        ({"bid": "2345.0", "ask": "2345.0"}, "locked"),
        ({"bid": "2345.2", "ask": "2345.1"}, "crossed"),
    ],
)
def test_invalid_quotes_fail_closed(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(PricingError, match=message):
        create_marketable_limit_decision(
            pricing_decision_id="pricing-1",
            quote=quote(**overrides),
            action="BUY",
            tick_size="0.1",
            fill_offset_ticks=1,
            max_quote_age_seconds=30,
            max_distance_ticks=10,
            max_distance_percent="0.25",
            now=aware_now(),
        )


def test_tick_rounding_uses_conservative_direction() -> None:
    assert round_up_to_tick("2345.11", "0.1") == Decimal("2345.2")
    assert round_down_to_tick("2345.19", "0.1") == Decimal("2345.1")
