from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mgc_v05l.execution_core.ibkr_paper_adapter import (
    IbkrPaperAdapter,
    IbkrPaperConfigError,
    IbkrPaperCorrelationError,
    IbkrPaperReadinessError,
    IbkrPaperSubmitDisabledError,
)
from mgc_v05l.execution_core.models import IntentKind, OrderIntent, SubmitAttempt, SubmitAttemptState


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def allowlist() -> dict[str, dict[str, str]]:
    return {
        "MGC-202606": {
            "symbol": "MGC",
            "security_type": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
            "local_symbol": "MGCM6",
            "con_id": "12345",
            "contract_month": "202606",
            "expiry": "20260626",
            "multiplier": "10",
            "tick_size": "0.1",
        }
    }


def adapter(**overrides: object) -> IbkrPaperAdapter:
    kwargs = {
        "mode": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 77,
        "account_id": "DU1234567",
        "contract_allowlist": allowlist(),
    }
    kwargs.update(overrides)
    return IbkrPaperAdapter(**kwargs)


def order_intent(**overrides: object) -> OrderIntent:
    kwargs = {
        "order_intent_id": "intent-1",
        "signal_event_id": "signal-1",
        "run_id": "run-1",
        "intent_kind": IntentKind.OPEN,
        "account_id": "DU1234567",
        "symbol": "MGC",
        "contract_key": "MGC-202606",
        "action": "BUY",
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "2345.2",
        "time_in_force": "DAY",
        "paper_only": True,
        "created_at": aware_now(),
        "reason": "test",
    }
    kwargs.update(overrides)
    return OrderIntent(**kwargs)


def submit_attempt(**overrides: object) -> SubmitAttempt:
    kwargs = {
        "submit_attempt_id": "submit-1",
        "order_intent_id": "intent-1",
        "run_id": "run-1",
        "account_id": "DU1234567",
        "broker": "IBKR",
        "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 77},
        "pre_submit_reconciliation_id": "recon-1",
        "open_order_baseline_event_id": "open-orders-1",
        "request_digest": "digest-1",
        "state": SubmitAttemptState.CREATED,
        "submitted_at": aware_now(),
        "broker_order_id": "1001",
        "perm_id": "9001",
    }
    kwargs.update(overrides)
    return SubmitAttempt(**kwargs)


@pytest.mark.parametrize("mode", ["LIVE", "", "PRODUCTION"])
def test_adapter_rejects_non_paper_mode(mode: str) -> None:
    with pytest.raises(IbkrPaperConfigError, match="mode must be PAPER"):
        adapter(mode=mode)


def test_adapter_rejects_non_localhost_host() -> None:
    with pytest.raises(IbkrPaperConfigError, match="host must be 127.0.0.1"):
        adapter(host="localhost")


@pytest.mark.parametrize("port", [7496, 4001, 4002])
def test_adapter_rejects_non_paper_ports(port: int) -> None:
    with pytest.raises(IbkrPaperConfigError, match="port must be 7497"):
        adapter(port=port)


def test_adapter_rejects_missing_account_id() -> None:
    with pytest.raises(IbkrPaperConfigError, match="account_id is required"):
        adapter(account_id="")


def test_adapter_rejects_implicit_first_account_selection() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU1234567", "DU7654321"))

    with pytest.raises(IbkrPaperReadinessError, match="implicit first-account selection"):
        paper.require_account(None)


def test_managed_account_exact_match_passes() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU1234567", "DU7654321"))

    assert paper.require_configured_account() == "DU1234567"


def test_managed_account_mismatch_fails() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU7654321",))

    with pytest.raises(IbkrPaperReadinessError, match="configured paper account"):
        paper.require_configured_account()


def test_missing_next_valid_id_fails_readiness() -> None:
    paper = adapter()
    paper.record_managed_accounts(("DU1234567",))

    with pytest.raises(IbkrPaperReadinessError, match="missing nextValidId"):
        paper.require_ready()
    assert paper.readiness_report()["next_valid_id_ready"] is False


def test_callback_mapping_creates_broker_order_without_synthetic_fill() -> None:
    paper = adapter()
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    order = paper.map_order_callback(
        submit_attempt_id="submit-1",
        account_id="DU1234567",
        broker_order_id="1001",
        perm_id="9001",
        client_id=77,
        contract_key="MGC-202606",
        action="BUY",
        quantity=1,
        order_type="LMT",
        limit_price="2345.2",
        status="Submitted",
        filled_quantity=0,
        remaining_quantity=1,
        average_fill_price=None,
        observed_at=aware_now(),
        raw={"callback": "openOrder/orderStatus"},
    )

    assert order.broker_order_id == "1001"
    assert order.status == "Submitted"
    assert paper.snapshot_open_orders(contract_key="MGC-202606") == (order,)


def test_exec_details_mapping_creates_fill_only_with_valid_correlation() -> None:
    paper = adapter()
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    fill = paper.map_exec_details(
        submit_attempt_id="submit-1",
        account_id="DU1234567",
        broker_order_id="1001",
        perm_id="9001",
        execution_id="exec-1",
        contract_key="MGC-202606",
        action="BUY",
        quantity=1,
        price="2345.2",
        filled_at=aware_now(),
        raw={"callback": "execDetails"},
    )

    assert fill.fill_event_id == "ibkr_fill_submit-1_exec-1"
    assert fill.order_intent_id == "intent-1"


def test_account_mismatch_marks_submit_context_ambiguous() -> None:
    paper = adapter()
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    with pytest.raises(IbkrPaperCorrelationError, match="account mismatch"):
        paper.map_exec_details(
            submit_attempt_id="submit-1",
            account_id="OTHER",
            broker_order_id="1001",
            perm_id="9001",
            execution_id="exec-1",
            contract_key="MGC-202606",
            action="BUY",
            quantity=1,
            price="2345.2",
            filled_at=aware_now(),
        )
    assert paper.ambiguous_contexts["submit-1"] == "account mismatch"


def test_contract_mismatch_fails_correlation() -> None:
    paper = adapter(contract_allowlist={**allowlist(), "MNQ-202606": {"symbol": "MNQ", "tick_size": "0.25"}})
    paper.register_submit_context(submit_attempt=submit_attempt(), order_intent=order_intent(), created_at=aware_now())

    with pytest.raises(IbkrPaperCorrelationError, match="contract mismatch"):
        paper.map_exec_details(
            submit_attempt_id="submit-1",
            account_id="DU1234567",
            broker_order_id="1001",
            perm_id="9001",
            execution_id="exec-1",
            contract_key="MNQ-202606",
            action="BUY",
            quantity=1,
            price="2345.2",
            filled_at=aware_now(),
        )


def test_position_callback_maps_to_track_b_position_state() -> None:
    paper = adapter()

    position = paper.record_position_callback(
        run_id="run-1",
        account_id="DU1234567",
        contract_key="MGC-202606",
        signed_quantity=1,
        average_price="2345.2",
        observed_at=aware_now(),
        raw={"callback": "position"},
    )

    assert position.signed_quantity == 1
    assert paper.snapshot_position(contract_key="MGC-202606") == position


def test_position_callback_real_mgc_field_shape_maps_by_contract_month_without_local_symbol() -> None:
    paper = adapter(
        contract_allowlist={
            "MGC-202606": {
                "symbol": "MGC",
                "security_type": "FUT",
                "exchange": "COMEX",
                "currency": "USD",
                "local_symbol": "MGCM6",
                "con_id": "712565978",
                "contract_month": "202606",
                "expiry": "20260626",
                "multiplier": "10",
                "tick_size": "0.1",
            }
        },
        module_loader=fake_ibapi_loader(position_contract_kwargs={"localSymbol": "", "conId": 0, "lastTradeDateOrContractMonth": "202606"}),
    )
    paper.connect()

    position = paper.refresh_positions(contract_key="MGC-202606")

    assert position.contract_key == "MGC-202606"
    assert position.raw["contract"]["symbol"] == "MGC"
    assert position.raw["contract"]["lastTradeDateOrContractMonth"] == "202606"
    assert paper.callback_errors == []


@pytest.mark.parametrize(
    "contract_kwargs",
    [
        {"localSymbol": "MNQM6", "conId": 0, "lastTradeDateOrContractMonth": "202606"},
        {"localSymbol": "", "conId": 999999, "lastTradeDateOrContractMonth": "202606"},
    ],
)
def test_position_callback_wrong_exact_identifier_is_captured_not_uncaught(contract_kwargs: dict[str, object]) -> None:
    paper = adapter(module_loader=fake_ibapi_loader(position_contract_kwargs=contract_kwargs))
    paper.request_timeout_seconds = 0.01
    paper.connect()

    with pytest.raises(IbkrPaperReadinessError, match="missing position callback"):
        paper.refresh_positions(contract_key="MGC-202606")

    assert paper.callback_errors
    assert paper.callback_errors[0]["callback"] == "position"
    assert paper.callback_errors[0]["error_type"] == "IbkrPaperCorrelationError"
    assert "allowlist" in str(paper.callback_errors[0]["error_message"])


def test_position_callback_missing_noncritical_fields_still_maps_by_conid() -> None:
    paper = adapter(module_loader=fake_ibapi_loader(position_contract_kwargs={"symbol": "", "localSymbol": "", "conId": 12345, "lastTradeDateOrContractMonth": ""}))
    paper.connect()

    position = paper.refresh_positions(contract_key="MGC-202606")

    assert position.contract_key == "MGC-202606"
    assert paper.callback_errors == []


def test_quote_observation_requires_exact_allowlisted_contract() -> None:
    paper = adapter()

    quote = paper.observe_quote(
        run_id="run-1",
        contract_key="MGC-202606",
        bid="2345.0",
        ask="2345.1",
        last="2345.05",
        observed_at=aware_now(),
    )

    assert quote.source == "ibkr_paper_adapter"
    with pytest.raises(IbkrPaperConfigError, match="allowlisted"):
        paper.observe_quote(
            run_id="run-1",
            contract_key="GC-202606",
            bid="2345.0",
            ask="2345.1",
            last="2345.05",
            observed_at=aware_now(),
        )


def test_submit_limit_order_fails_closed_when_disabled() -> None:
    paper = adapter(submit_enabled=False)

    with pytest.raises(IbkrPaperSubmitDisabledError, match="submit_enabled"):
        paper.submit_limit_order(submit_attempt=submit_attempt(), order_intent=order_intent())


def test_submit_enabled_defaults_to_false() -> None:
    paper = adapter()

    assert paper.submit_enabled is False


def test_submit_limit_order_places_lmt_day_only_after_explicit_enablement() -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader())
    paper.connect()
    submit = submit_attempt(broker_order_id="1001", perm_id=None)
    intent = order_intent()

    local_order_id = paper.submit_limit_order(submit_attempt=submit, order_intent=intent)

    placed = paper.bridge_for_test().placed_orders[0]
    assert local_order_id == 1001
    assert placed["order_id"] == 1001
    assert placed["contract"].localSymbol == "MGCM6"
    assert placed["order"].account == "DU1234567"
    assert placed["order"].orderType == "LMT"
    assert placed["order"].tif == "DAY"
    assert placed["order"].totalQuantity == 1.0
    assert placed["order"].transmit is True
    assert placed["order"].eTradeOnly is False
    assert placed["order"].firmQuoteOnly is False
    assert placed["order"].nbboPriceCap == sys.float_info.max
    diagnostics = paper.submit_diagnostics("submit-1")
    assert diagnostics["place_order_called"] is True
    assert diagnostics["place_order_called_at"] is not None
    assert diagnostics["broker_order_id_allocated"] == "1001"
    assert diagnostics["order_transmit_flag"] is True
    assert diagnostics["order_action"] == "BUY"
    assert diagnostics["order_type"] == "LMT"
    assert diagnostics["limit_price"] == "2345.2"
    assert diagnostics["tif"] == "DAY"
    assert diagnostics["client_id"] == 77
    assert diagnostics["account_id"] == "DU1234567"
    assert diagnostics["contract_key"] == "MGC-202606"
    assert diagnostics["contract_local_symbol"] == "MGCM6"
    assert diagnostics["contract_con_id"] == 12345
    assert diagnostics["isConnected_before_placeOrder"] is True
    assert diagnostics["isConnected_after_placeOrder"] is True


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("order_type", "MKT", "order_type must be LMT"),
        ("time_in_force", "GTC", "time_in_force must be DAY"),
        ("quantity", Decimal("2"), "quantity must be exactly 1"),
        ("extra_fields", {"parentId": 1}, "forbidden"),
    ],
)
def test_submit_limit_order_rejects_unsafe_intent_even_when_enabled(field: str, value: object, message: str) -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader())
    paper.connect()
    intent = order_intent()
    object.__setattr__(intent, field, value)

    with pytest.raises(IbkrPaperConfigError, match=message):
        paper.submit_limit_order(submit_attempt=submit_attempt(broker_order_id="1001"), order_intent=intent)

    assert paper.bridge_for_test().placed_orders == []


def test_submit_limit_order_rejects_unallowlisted_contract_even_when_enabled() -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader())
    paper.connect()
    intent = order_intent(contract_key="MNQ-202606")
    object.__setattr__(intent, "contract_key", "MNQ-202606")

    with pytest.raises(IbkrPaperConfigError, match="allowlisted"):
        paper.submit_limit_order(submit_attempt=submit_attempt(broker_order_id="1001"), order_intent=intent)

    assert paper.bridge_for_test().placed_orders == []


def test_stub_submit_callbacks_capture_broker_order_and_fill_correlation() -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader())
    paper.connect()
    submit = submit_attempt(broker_order_id="1001", perm_id=None)
    intent = order_intent()
    paper.submit_limit_order(submit_attempt=submit, order_intent=intent)
    bridge = paper.bridge_for_test()

    bridge.emit_open_order(order_id=1001, account="DU1234567", local_symbol="MGCM6", perm_id=9001)
    bridge.emit_exec_details(order_id=1001, account="DU1234567", local_symbol="MGCM6", perm_id=9001, exec_id="EXEC-1")

    order = paper.wait_for_broker_order(submit_attempt_id="submit-1")
    fill = paper.wait_for_fill(submit_attempt_id="submit-1")
    assert order.broker_order_id == "1001"
    assert order.perm_id == "9001"
    assert fill.execution_id == "EXEC-1"
    assert fill.broker_order_id == "1001"
    diagnostics = paper.submit_diagnostics("submit-1")
    assert diagnostics["openOrder_seen"] is True
    assert diagnostics["execDetails_seen"] is True


def test_missing_order_truth_after_submit_times_out_cleanly() -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader())
    paper.request_timeout_seconds = 0.01
    paper.connect()
    paper.submit_limit_order(submit_attempt=submit_attempt(broker_order_id="1001"), order_intent=order_intent())

    with pytest.raises(IbkrPaperReadinessError, match="missing openOrder/orderStatus"):
        paper.wait_for_broker_order(submit_attempt_id="submit-1")

    assert "openOrder/orderStatus" in paper.missing_callbacks
    diagnostics = paper.submit_diagnostics("submit-1")
    assert diagnostics["place_order_called"] is True
    assert diagnostics["openOrder_seen"] is False
    assert diagnostics["orderStatus_seen"] is False
    assert diagnostics["callback_wait_timeout_seconds"] == 0.01
    assert diagnostics["isConnected_after_callback_wait"] is True


def test_place_order_exception_is_captured_in_diagnostics() -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader(place_order_error=RuntimeError("rejected")))
    paper.connect()

    with pytest.raises(RuntimeError, match="rejected"):
        paper.submit_limit_order(submit_attempt=submit_attempt(broker_order_id="1001"), order_intent=order_intent())

    diagnostics = paper.submit_diagnostics("submit-1")
    assert diagnostics["place_order_called"] is True
    assert "RuntimeError('rejected')" == diagnostics["place_order_exception"]
    assert diagnostics["broker_order_id_allocated"] == "1001"


@pytest.mark.parametrize(
    ("error_code", "attribute"),
    [
        (10268, "EtradeOnly"),
        (10269, "firmQuoteOnly"),
        (10270, "nbboPriceCap"),
    ],
)
def test_deprecated_order_attribute_error_after_submit_is_included_in_diagnostics(
    error_code: int,
    attribute: str,
) -> None:
    paper = adapter(submit_enabled=True, module_loader=fake_ibapi_loader())
    paper.connect()
    paper.submit_limit_order(submit_attempt=submit_attempt(broker_order_id="1001"), order_intent=order_intent())

    paper.bridge_for_test().error(1001, error_code, f"The '{attribute}' order attribute is not supported.")

    diagnostics = paper.submit_diagnostics("submit-1")
    assert diagnostics["error_callbacks_after_submit"] == [
        {
            "request_id": 1001,
            "error_code": error_code,
            "error_string": f"The '{attribute}' order attribute is not supported.",
            "raw_args": ["1001", str(error_code), f"\"The '{attribute}' order attribute is not supported.\""],
        }
    ]


def fake_ibapi_loader(*, place_order_error: Exception | None = None, position_contract_kwargs: dict[str, object] | None = None):
    class FakeWrapper:
        def __init__(self) -> None:
            return None

    class FakeClient:
        def __init__(self, wrapper) -> None:
            self.wrapper = wrapper
            self.connected = False
            self.placed_orders: list[dict[str, object]] = []
            self.cancelled_orders: list[int] = []

        def connect(self, host: str, port: int, client_id: int) -> None:
            self.connected = True
            self.wrapper.nextValidId(1001)
            self.wrapper.managedAccounts("DU1234567")

        def run(self) -> None:
            return None

        def disconnect(self) -> None:
            self.connected = False

        def isConnected(self) -> bool:  # noqa: N802
            return self.connected

        def placeOrder(self, order_id: int, contract, order) -> None:  # noqa: N802, ANN001
            if place_order_error is not None:
                raise place_order_error
            self.placed_orders.append({"order_id": order_id, "contract": contract, "order": order})

        def cancelOrder(self, order_id: int, *args: object) -> None:  # noqa: N802
            self.cancelled_orders.append(order_id)

        def reqManagedAccts(self) -> None:  # noqa: N802
            self.wrapper.managedAccounts("DU1234567")

        def reqPositions(self) -> None:  # noqa: N802
            kwargs = {"localSymbol": "MGCM6", "conId": 12345}
            kwargs.update(position_contract_kwargs or {})
            self.wrapper.position("DU1234567", FakeContract(**kwargs), 0, 0.0)
            self.wrapper.positionEnd()

        def reqOpenOrders(self) -> None:  # noqa: N802
            self.wrapper.openOrderEnd()

        def emit_open_order(self, *, order_id: int, account: str, local_symbol: str, perm_id: int) -> None:
            contract = FakeContract(localSymbol=local_symbol, conId=12345)
            order = FakeOrder()
            order.account = account
            order.action = "BUY"
            order.totalQuantity = 1
            order.orderType = "LMT"
            order.lmtPrice = 2345.2
            order.tif = "DAY"
            order.permId = perm_id
            order.clientId = 77
            state = types.SimpleNamespace(status="Filled")
            self.wrapper.openOrder(order_id, contract, order, state)

        def emit_exec_details(self, *, order_id: int, account: str, local_symbol: str, perm_id: int, exec_id: str) -> None:
            contract = FakeContract(localSymbol=local_symbol, conId=12345)
            execution = types.SimpleNamespace(
                orderId=order_id,
                acctNumber=account,
                permId=perm_id,
                execId=exec_id,
                side="BOT",
                shares=1,
                price=2345.2,
            )
            self.wrapper.execDetails(1, contract, execution)

    class FakeContract:
        def __init__(self, **kwargs: object) -> None:
            self.symbol = kwargs.get("symbol", "MGC")
            self.secType = kwargs.get("secType", "FUT")
            self.exchange = kwargs.get("exchange", "COMEX")
            self.currency = kwargs.get("currency", "USD")
            self.localSymbol = kwargs.get("localSymbol", "")
            self.conId = kwargs.get("conId", 0)
            self.multiplier = kwargs.get("multiplier", "10")
            self.lastTradeDateOrContractMonth = kwargs.get("lastTradeDateOrContractMonth", "20260626")

    class FakeOrder:
        def __init__(self) -> None:
            self.eTradeOnly = True
            self.firmQuoteOnly = True
            self.nbboPriceCap = 123.45

    modules = {
        "ibapi.wrapper": types.SimpleNamespace(EWrapper=FakeWrapper),
        "ibapi.client": types.SimpleNamespace(EClient=FakeClient),
        "ibapi.contract": types.SimpleNamespace(Contract=FakeContract),
        "ibapi.order": types.SimpleNamespace(Order=FakeOrder),
    }

    def loader(name: str):
        return modules[name]

    return loader
