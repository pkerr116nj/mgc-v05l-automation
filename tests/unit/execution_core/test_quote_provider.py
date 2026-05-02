from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from mgc_v05l.execution_core.pricing import MarketDataMode, MarketDataRole
from mgc_v05l.execution_core.quote_provider import (
    QuoteProviderError,
    QuoteSnapshot,
    paper_proof_quote_ready,
    production_live_money_quote_ready,
    validate_quote_for_pricing,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def quote(**overrides: object) -> QuoteSnapshot:
    kwargs = {
        "provider": "DATABENTO",
        "mode": MarketDataMode.REALTIME,
        "role": MarketDataRole.PRIMARY,
        "contract_key": "MGC-202606",
        "bid": "4626.0",
        "ask": "4626.1",
        "last": "4626.0",
        "timestamp": aware_now(),
        "tick_size": "0.1",
        "exchange": "COMEX",
        "currency": "USD",
        "source_latency_ms": "12",
    }
    kwargs.update(overrides)
    return QuoteSnapshot(**kwargs)


class FakeQuoteProvider:
    provider_name = "FAKE"

    def __init__(self, snapshot: QuoteSnapshot) -> None:
        self.snapshot = snapshot

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        assert contract_key == self.snapshot.contract_key
        return self.snapshot


def test_fake_provider_returns_quote_snapshot() -> None:
    provider = FakeQuoteProvider(quote(provider="FAKE", role=MarketDataRole.DIAGNOSTIC))

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.provider == "FAKE"
    assert snapshot.contract_key == "MGC-202606"
    assert snapshot.to_json_dict()["source_latency_ms"] == "12"


def test_valid_realtime_quote_passes_live_money_quote_readiness() -> None:
    snapshot = quote()

    assert validate_quote_for_pricing(
        snapshot,
        now=aware_now(),
        max_age_seconds=5,
        allow_delayed_for_paper=False,
        live_money=True,
    ) == snapshot
    assert production_live_money_quote_ready(snapshot) is True


def test_delayed_quote_passes_paper_readiness_only_when_explicitly_allowed() -> None:
    snapshot = quote(provider="IBKR", mode=MarketDataMode.DELAYED, role=MarketDataRole.BACKUP, delayed_data_warning_seen=True)

    validate_quote_for_pricing(
        snapshot,
        now=aware_now(),
        max_age_seconds=5,
        allow_delayed_for_paper=True,
        live_money=False,
    )
    assert paper_proof_quote_ready(snapshot, allow_delayed_for_paper=True) is True

    with pytest.raises(QuoteProviderError, match="explicit paper-proof approval"):
        validate_quote_for_pricing(
            snapshot,
            now=aware_now(),
            max_age_seconds=5,
            allow_delayed_for_paper=False,
            live_money=False,
        )


@pytest.mark.parametrize("mode", [MarketDataMode.DELAYED, MarketDataMode.UNKNOWN])
def test_delayed_or_unknown_quote_blocks_live_money_readiness(mode: str) -> None:
    snapshot = quote(mode=mode)

    assert production_live_money_quote_ready(snapshot) is False
    with pytest.raises(QuoteProviderError, match="live-money readiness|unknown market data"):
        validate_quote_for_pricing(
            snapshot,
            now=aware_now(),
            max_age_seconds=5,
            allow_delayed_for_paper=True,
            live_money=True,
        )


def test_stale_quote_blocks_pricing() -> None:
    with pytest.raises(QuoteProviderError, match="stale"):
        validate_quote_for_pricing(
            quote(timestamp=aware_now() - timedelta(seconds=6)),
            now=aware_now(),
            max_age_seconds=5,
            allow_delayed_for_paper=False,
            live_money=True,
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"bid": "4626.2", "ask": "4626.1"}, "crossed"),
        ({"bid": "4626.1", "ask": "4626.1"}, "locked"),
        ({"bid": None}, "bid is required"),
        ({"ask": None}, "ask is required"),
        ({"last": None}, "last is required"),
        ({"bid": "4626.05"}, "bid is not compatible with tick_size"),
        ({"tick_size": None}, "tick_size is required"),
    ],
)
def test_invalid_quote_pricing_inputs_block(overrides: dict[str, object], message: str) -> None:
    with pytest.raises(QuoteProviderError, match=message):
        validate_quote_for_pricing(
            quote(**overrides),
            now=aware_now(),
            max_age_seconds=5,
            allow_delayed_for_paper=False,
            live_money=True,
        )
