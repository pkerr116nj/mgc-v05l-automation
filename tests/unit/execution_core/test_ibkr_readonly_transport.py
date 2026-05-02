from __future__ import annotations

import inspect
import time
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


def test_next_valid_id_callback_is_captured() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)

    transport.bridge_for_test().nextValidId(1001)

    assert transport.next_valid_id() == 1001
    assert transport.diagnostics_report()["next_valid_id_received"] is True


def test_readiness_blocks_account_request_until_next_valid_id_exists() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    bridge = transport.bridge_for_test()

    with pytest.raises(IbkrReadOnlyTimeoutError, match="missing nextValidId callback"):
        transport.managed_accounts()

    assert bridge.req_ids_calls == [-1]
    assert bridge.req_managed_accounts_count == 0
    diagnostics = transport.diagnostics_report()
    assert diagnostics["connected_socket"] is True
    assert diagnostics["event_loop_thread_started"] is True
    assert diagnostics["next_valid_id_received"] is False
    assert diagnostics["client_id"] == 17077
    assert "TWS did not complete API handshake before timeout" in diagnostics["suspected_causes"]


def test_contract_request_is_blocked_until_next_valid_id_exists() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    bridge = transport.bridge_for_test()

    with pytest.raises(IbkrReadOnlyTimeoutError, match="missing nextValidId callback"):
        transport.qualify_contract(
            contract_key="MGC-202606",
            allowlist_entry={
                "symbol": "MGC",
                "security_type": "FUT",
                "exchange": "COMEX",
                "currency": "USD",
                "local_symbol": "MGCM6",
            },
        )

    assert bridge.req_contract_details_count == 0


def test_loop_exit_without_next_valid_id_has_specific_diagnostic() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    time.sleep(0.02)

    with pytest.raises(IbkrReadOnlyTimeoutError, match="missing nextValidId callback"):
        transport.next_valid_id()

    diagnostics = transport.diagnostics_report()
    assert diagnostics["event_loop_started_at"] is not None
    assert diagnostics["event_loop_exited_at"] is not None
    assert diagnostics["is_connected_after_loop_exit"] is True
    assert "IBKR API event loop exited before nextValidId" in diagnostics["suspected_causes"]


def test_connection_closed_callback_is_recorded() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)

    transport.bridge_for_test().connectionClosed()

    diagnostics = transport.diagnostics_report()
    assert diagnostics["connection_closed_at"] is not None
    assert "IBKR connectionClosed callback received" in diagnostics["suspected_causes"]


def test_connect_ack_callback_is_recorded() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)

    transport.bridge_for_test().connectAck()

    diagnostics = transport.diagnostics_report()
    assert diagnostics["connect_ack_received"] is True
    assert diagnostics["connect_ack_at"] is not None


def test_disconnect_is_not_called_before_readiness_timeout() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    bridge = transport.bridge_for_test()

    with pytest.raises(IbkrReadOnlyTimeoutError, match="missing nextValidId callback"):
        transport.managed_accounts()

    assert bridge.disconnected is False
    assert transport.diagnostics_report()["disconnect_called_by_track_b"] is False
    transport.disconnect()
    assert bridge.disconnected is True
    assert transport.diagnostics_report()["disconnect_called_by_track_b"] is True


def test_error_callback_variants_are_captured_with_raw_args() -> None:
    transport = IbkrReadOnlyTwsTransport(config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01), module_loader=fake_ibapi_loader())
    transport.connect(host="127.0.0.1", port=7497, client_id=17077, readonly=True)
    bridge = transport.bridge_for_test()

    bridge.error(-1, 326, "client id in use")
    bridge.error(-1, "20260502 12:00:00", 502, "could not connect", "{}")

    errors = transport.diagnostics_report()["ibkr_errors"]
    assert errors[0]["error_code"] == 326
    assert errors[0]["error_string"] == "client id in use"
    assert errors[0]["raw_args"] == ["-1", "326", "'client id in use'"]
    assert errors[1]["error_code"] == 502
    assert errors[1]["error_string"] == "could not connect"
    assert errors[1]["raw_args"] == ["-1", "'20260502 12:00:00'", "502", "'could not connect'", "'{}'"]


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


def test_ibkr_handshake_error_is_captured_in_preflight_report(tmp_path) -> None:  # type: ignore[no-untyped-def]
    from mgc_v05l.execution_core.preflight import PreflightClassification, ReadOnlyPreflightConfig, run_read_only_preflight

    transport = IbkrReadOnlyTwsTransport(
        config=IbkrReadOnlyTransportConfig(request_timeout_seconds=0.01),
        module_loader=fake_ibapi_loader(handshake_error=(326, "client id is already in use")),
    )

    result = run_read_only_preflight(
        config=ReadOnlyPreflightConfig(
            account_id="DUM882026",
            client_id=17077,
            output_root=tmp_path / "preflight",
        ),
        transport=transport,
        run_id="preflight-next-valid-id-error",
    )

    assert result.classification == PreflightClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    diagnostics = result.report["transport_diagnostics"]
    assert diagnostics["connected_socket"] is True
    assert diagnostics["event_loop_thread_started"] is True
    assert diagnostics["next_valid_id_received"] is False
    assert diagnostics["client_id"] == 17077
    assert diagnostics["handshake_timeout_seconds"] == 0.01
    assert diagnostics["ibkr_errors"] == [
        {
            "error_code": 326,
            "error_string": "client id is already in use",
            "raw_args": ["-1", "326", "'client id is already in use'"],
            "request_id": -1,
        }
    ]
    assert "client_id collision" in diagnostics["suspected_causes"]


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


def fake_ibapi_loader(*, managed_accounts: str = "DUM882026", handshake_error: tuple[int, str] | None = None):
    class FakeWrapper:
        def __init__(self) -> None:
            return None

    class FakeClient:
        def __init__(self, wrapper) -> None:
            self.wrapper = wrapper
            self.req_ids_calls: list[int] = []
            self.req_managed_accounts_count = 0
            self.req_contract_details_count = 0
            self.connected_with: tuple[str, int, int] | None = None
            self.disconnected = False
            self.connected = False

        def connect(self, host: str, port: int, client_id: int) -> None:
            self.connected_with = (host, port, client_id)
            self.connected = True

        def run(self) -> None:
            return None

        def disconnect(self) -> None:
            self.disconnected = True
            self.connected = False

        def isConnected(self) -> bool:  # noqa: N802
            return self.connected

        def reqIds(self, request_id: int) -> None:  # noqa: N802
            self.req_ids_calls.append(request_id)
            if handshake_error is not None:
                code, message = handshake_error
                self.wrapper.error(-1, code, message)

        def reqManagedAccts(self) -> None:  # noqa: N802
            self.req_managed_accounts_count += 1
            if managed_accounts:
                self.wrapper.managedAccounts(managed_accounts)

        def reqContractDetails(self, request_id: int, contract) -> None:  # noqa: N802, ANN001
            self.req_contract_details_count += 1

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
