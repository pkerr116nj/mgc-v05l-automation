from __future__ import annotations

from datetime import date

import pytest

from mgc_v05l.brokers.ibkr import (
    IbkrContractResolutionError,
    IbkrContractResolver,
)
from mgc_v05l.execution.broker_requests import BrokerContractRequest


def test_ibkr_contract_resolver_qualifies_futures_request_from_broker_contract_request() -> None:
    resolver = IbkrContractResolver()

    qualified = resolver.qualify_contract(
        BrokerContractRequest(
            asset_class="FUTURE",
            symbol="MGC",
            expiry="202606",
        )
    )

    assert qualified.internal_symbol == "MGC"
    assert qualified.broker_symbol == "MGC"
    assert qualified.security_type == "FUT"
    assert qualified.exchange == "COMEX"
    assert qualified.expiry == "202606"
    assert qualified.local_symbol == "MGCM26"
    assert qualified.multiplier == "10"
    assert qualified.metadata == {
        "contract_month": "202606",
        "month_code": "M",
        "local_symbol_hint": "MGCM26",
    }


def test_ibkr_contract_resolver_rejects_unknown_symbol() -> None:
    resolver = IbkrContractResolver()

    with pytest.raises(IbkrContractResolutionError, match="Unsupported IBKR futures symbol"):
        resolver.qualify_futures(symbol="UNK", expiry="202606")


def test_ibkr_contract_resolver_rejects_disallowed_exchange() -> None:
    resolver = IbkrContractResolver()

    with pytest.raises(IbkrContractResolutionError, match="allowed exchanges"):
        resolver.qualify_futures(symbol="MGC", exchange="CME", expiry="202606")


def test_ibkr_contract_resolver_uses_explicit_currency_multiplier_and_broker_root_overrides() -> None:
    resolver = IbkrContractResolver()

    qualified = resolver.qualify_futures(
        symbol="6E",
        broker_symbol="6E",
        currency="usd",
        exchange="globex",
        expiry="20260915",
        multiplier="125000",
    )

    assert qualified.broker_symbol == "6E"
    assert qualified.exchange == "GLOBEX"
    assert qualified.currency == "USD"
    assert qualified.expiry == "202609"
    assert qualified.local_symbol == "6EU26"
    assert qualified.multiplier == "125000"


def test_ibkr_contract_resolver_requires_calendar_month_expiry() -> None:
    resolver = IbkrContractResolver()

    with pytest.raises(IbkrContractResolutionError, match="real calendar month"):
        resolver.qualify_futures(symbol="MES", expiry="202613")


def test_ibkr_contract_resolver_uses_front_month_fallback_when_expiry_missing() -> None:
    resolver = IbkrContractResolver()

    qualified = resolver.qualify_futures(symbol="ES", now=date(2026, 4, 15))

    assert qualified.expiry == "202604"
    assert qualified.local_symbol == "ESJ26"
