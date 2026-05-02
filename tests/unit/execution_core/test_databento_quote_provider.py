from __future__ import annotations

import inspect
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import pytest

from mgc_v05l.execution_core.databento_quote_provider import (
    DatabentoQuoteProvider,
    DatabentoQuoteProviderConfig,
    DatabentoQuoteProviderError,
)
from mgc_v05l.execution_core.pricing import MarketDataMode
from mgc_v05l.execution_core.quote_provider import validate_quote_for_pricing
import mgc_v05l.execution_core.databento_quote_provider as provider_module


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


class FakeTransport:
    def __init__(
        self,
        *,
        bbo_records: Sequence[Mapping[str, Any]] | None = None,
        trade_records: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        self.bbo_records = tuple(
            (
                {
                    "ts_event": "2026-05-02T12:00:00+00:00",
                    "levels": [{"bid_px": "4626.0", "ask_px": "4626.1"}],
                },
            )
            if bbo_records is None
            else bbo_records
        )
        self.trade_records = tuple(({"ts_event": "2026-05-02T12:00:00+00:00", "price": "4626.0"},) if trade_records is None else trade_records)
        self.requests: list[dict[str, Any]] = []

    def request_records(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]:
        self.requests.append(dict(kwargs))
        return self.trade_records if kwargs["schema"] == "trades" else self.bbo_records


def config(**overrides: object) -> DatabentoQuoteProviderConfig:
    kwargs = {
        "contract_key": "MGC-202606",
        "databento_symbol": "MGCM6",
        "tick_size": "0.1",
        "exchange": "COMEX",
        "currency": "USD",
        "api_key": "test-key",
    }
    kwargs.update(overrides)
    return DatabentoQuoteProviderConfig(**kwargs)


def test_fake_transport_valid_realtime_quote_returns_track_b_snapshot() -> None:
    transport = FakeTransport()
    provider = DatabentoQuoteProvider(config=config(), transport=transport, now=aware_now())

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.provider == "DATABENTO"
    assert snapshot.mode == MarketDataMode.REALTIME
    assert snapshot.role == "PRIMARY"
    assert snapshot.contract_key == "MGC-202606"
    assert snapshot.provider_symbol == "MGCM6"
    assert str(snapshot.bid) == "4626.0"
    assert str(snapshot.ask) == "4626.1"
    assert str(snapshot.last) == "4626.0"
    assert snapshot.exchange == "COMEX"
    assert snapshot.currency == "USD"
    assert snapshot.delayed_data_warning_seen is False
    assert [request["schema"] for request in transport.requests] == ["mbp-1", "trades"]
    validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_explicit_symbol_mapping_is_required() -> None:
    with pytest.raises(DatabentoQuoteProviderError, match="databento_symbol"):
        DatabentoQuoteProvider(config=config(databento_symbol=""), transport=FakeTransport(), now=aware_now())


def test_contract_key_must_match_explicit_mapping() -> None:
    provider = DatabentoQuoteProvider(config=config(), transport=FakeTransport(), now=aware_now())

    with pytest.raises(DatabentoQuoteProviderError, match="contract_key"):
        provider.get_quote("MGC-202608")


def test_stale_quote_is_unknown_and_blocks_live_money_readiness() -> None:
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=FakeTransport(
            bbo_records=({"ts_event": "2026-05-02T11:59:00+00:00", "levels": [{"bid_px": "4626.0", "ask_px": "4626.1"}]},),
            trade_records=({"ts_event": "2026-05-02T11:59:00+00:00", "price": "4626.0"},),
        ),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.mode == MarketDataMode.UNKNOWN
    with pytest.raises(Exception, match="live-money readiness|unknown market data"):
        validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_missing_bid_ask_last_blocks_quote_derived_pricing() -> None:
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=FakeTransport(
            bbo_records=({"ts_event": "2026-05-02T12:00:00+00:00", "bid_px": "4626.0", "ask_px": "4626.1"},),
            trade_records=(),
        ),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.last is None
    with pytest.raises(Exception, match="last is required"):
        validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_provider_source_has_no_track_a_live_feed_or_schwab_imports() -> None:
    source = inspect.getsource(provider_module)

    assert "live_feed" not in source
    assert "schwab" not in source.lower()
    assert "mgc_v05l.execution" not in source
    assert "mgc_v05l.app" not in source
    assert "dashboard" not in source
    assert "persistence" not in source
