from __future__ import annotations

import inspect

import pytest

from mgc_v05l.execution_core import ibkr_readonly_transport
from mgc_v05l.execution_core.ibkr_paper_adapter import IbkrPaperConfigError
from mgc_v05l.execution_core.ibkr_readonly_transport import (
    IbkrReadOnlyTwsTransport,
    detect_contract_allowlist_ambiguities,
)


def test_read_only_transport_object_exposes_no_order_submission_methods() -> None:
    transport = IbkrReadOnlyTwsTransport()

    assert not hasattr(transport, "placeOrder")
    assert not hasattr(transport, "submit_limit_order")
    assert not hasattr(transport, "submit")


def test_read_only_transport_source_has_no_order_submission_path() -> None:
    source = inspect.getsource(ibkr_readonly_transport)

    assert "placeOrder" not in source
    assert "submit_limit_order" not in source
    assert "submit_enabled=True" not in source
    assert "transmit=True" not in source


def test_transport_rejects_non_readonly_connect_before_loading_ibapi() -> None:
    loaded: list[str] = []

    def loader(name: str) -> object:
        loaded.append(name)
        raise AssertionError("module loader must not be called for invalid readonly config")

    transport = IbkrReadOnlyTwsTransport(module_loader=loader)

    with pytest.raises(IbkrPaperConfigError, match="readonly=True"):
        transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=False)

    assert loaded == []


def test_transport_rejects_live_port_before_loading_ibapi() -> None:
    loaded: list[str] = []

    def loader(name: str) -> object:
        loaded.append(name)
        raise AssertionError("module loader must not be called for invalid live port")

    transport = IbkrReadOnlyTwsTransport(module_loader=loader)

    with pytest.raises(IbkrPaperConfigError, match="port must be 7497"):
        transport.connect(host="127.0.0.1", port=7496, client_id=17077, readonly=True)

    assert loaded == []


def test_contract_allowlist_validation_detects_mgc_local_symbol_month_mismatch() -> None:
    issues = detect_contract_allowlist_ambiguities(
        contract_key="MGC-202606",
        allowlist_entry={
            "symbol": "MGC",
            "security_type": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
            "local_symbol": "MGCJ6",
        },
    )

    assert issues == (
        "contract_key MGC-202606 implies 202606, but local_symbol MGCJ6 implies 202604",
    )


def test_contract_allowlist_validation_accepts_matching_mgc_local_symbol() -> None:
    issues = detect_contract_allowlist_ambiguities(
        contract_key="MGC-202606",
        allowlist_entry={
            "symbol": "MGC",
            "security_type": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
            "local_symbol": "MGCM6",
            "contract_month": "202606",
        },
    )

    assert issues == ()
