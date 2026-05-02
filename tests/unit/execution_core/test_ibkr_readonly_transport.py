from __future__ import annotations

import inspect
import types

import pytest

from mgc_v05l.execution_core import ibkr_readonly_transport
from mgc_v05l.execution_core.ibkr_paper_adapter import IbkrPaperConfigError
from mgc_v05l.execution_core.ibkr_readonly_transport import (
    IbkrReadOnlyTimeoutError,
    IbkrReadOnlyTransportConfig,
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


def test_managed_accounts_callback_is_captured_when_delivered_after_ready() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    bridge = transport.bridge_for_test()
    bridge.nextValidId(1001)

    accounts = transport.managed_accounts()

    assert accounts == ("DUM882026",)
    assert bridge.req_ids_calls == []
    assert bridge.req_managed_accounts_count == 1


def test_managed_accounts_callback_before_next_valid_id_is_preserved() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    bridge = transport.bridge_for_test()
    bridge.managedAccounts("DUM882026")
    bridge.nextValidId(1001)

    accounts = transport.managed_accounts()

    assert accounts == ("DUM882026",)
    assert bridge.req_managed_accounts_count == 0


def test_managed_accounts_times_out_cleanly_when_callback_missing() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader(managed_accounts=""))
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    transport.bridge_for_test().nextValidId(1001)

    with pytest.raises(IbkrReadOnlyTimeoutError, match="missing managedAccounts callback"):
        transport.managed_accounts()


def test_managed_account_exact_match_passes_through_preflight(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from mgc_v05l.execution_core.preflight import PreflightClassification, ReadOnlyPreflightConfig, run_read_only_preflight

    class ReadyTransport:
        def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None:
            return None

        def disconnect(self) -> None:
            return None

        def managed_accounts(self):
            return ("DUM882026",)

        def next_valid_id(self):
            return 1001

        def qualify_contract(self, *, contract_key, allowlist_entry):
            return {"contract_key": contract_key, **dict(allowlist_entry)}

        def snapshot_position(self, *, run_id, account_id, contract_key, observed_at):
            return {"account_id": account_id, "contract_key": contract_key, "signed_quantity": 0}

        def snapshot_open_orders(self, *, account_id, contract_key, observed_at):
            return ()

        def observe_quote(self, *, run_id, contract_key, observed_at):
            return {"run_id": run_id, "contract_key": contract_key, "bid": "2345.0", "ask": "2345.1", "last": "2345.05"}

    result = run_read_only_preflight(
        config=ReadOnlyPreflightConfig(
            account_id="DUM882026",
            output_root=tmp_path / "preflight",
        ),
        transport=ReadyTransport(),
        run_id="preflight-managed-account",
    )

    assert result.classification == PreflightClassification.READY_READ_ONLY
    assert any(check["name"] == "managed_account_exact_match" and check["passed"] for check in result.report["checks"])


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


def fake_ibapi_loader(*, managed_accounts: str = "DUM882026"):
    class FakeWrapper:
        def __init__(self) -> None:
            return None

    class FakeClient:
        def __init__(self, wrapper) -> None:
            self.wrapper = wrapper
            self.req_ids_calls: list[int] = []
            self.req_managed_accounts_count = 0
            self.connected_with: tuple[str, int, int] | None = None
            self.disconnected = False

        def connect(self, host: str, port: int, client_id: int) -> None:
            self.connected_with = (host, port, client_id)

        def run(self) -> None:
            return None

        def disconnect(self) -> None:
            self.disconnected = True

        def reqIds(self, request_id: int) -> None:  # noqa: N802
            self.req_ids_calls.append(request_id)

        def reqManagedAccts(self) -> None:  # noqa: N802
            self.req_managed_accounts_count += 1
            if managed_accounts:
                self.wrapper.managedAccounts(managed_accounts)

    class FakeContract:
        pass

    modules = {
        "ibapi.wrapper": types.SimpleNamespace(EWrapper=FakeWrapper),
        "ibapi.client": types.SimpleNamespace(EClient=FakeClient),
        "ibapi.contract": types.SimpleNamespace(Contract=FakeContract),
    }

    def loader(name: str):
        return modules[name]

    return loader
