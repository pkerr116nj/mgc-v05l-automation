from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.brokers.ibkr import (
    IbkrClient,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)


def _client() -> IbkrClient:
    session = IbkrSession(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        account_id="DU1234567",
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False),
    )
    return IbkrClient(session=session)


def test_ibkr_callback_adapter_accumulates_read_only_truth_buffers() -> None:
    now = datetime(2026, 4, 12, 16, 30, tzinfo=timezone.utc)
    client = _client()
    adapter = client.build_read_only_callback_adapter()

    adapter.next_valid_id(7001, occurred_at=now)
    adapter.managed_accounts("DU1234567,DU7654321", occurred_at=now)
    adapter.update_account_value(account_id="DU1234567", key="BuyingPower", value="100000", currency="USD", occurred_at=now)
    adapter.update_account_value(account_id="DU1234567", key="NetLiquidation", value="55000", currency="USD", occurred_at=now)
    adapter.account_download_end(occurred_at=now)
    adapter.position(
        account_id="DU1234567",
        contract={
            "conId": 12345,
            "symbol": "MGC",
            "localSymbol": "MGCM26",
            "securityType": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
            "lastTradeDateOrContractMonth": "202606",
            "multiplier": "10",
        },
        quantity="1",
        average_cost="2450.5",
        occurred_at=now,
    )
    adapter.position_end(occurred_at=now)
    adapter.open_order(
        account_id="DU1234567",
        broker_order_id=7001,
        client_id=7,
        perm_id=1,
        contract={
            "symbol": "MGC",
            "localSymbol": "MGCM26",
            "securityType": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
        },
        status="Submitted",
        quantity="1",
        filled_quantity="0",
        occurred_at=now,
    )
    adapter.open_order_end(occurred_at=now)
    adapter.completed_order(
        account_id="DU1234567",
        broker_order_id=6998,
        client_id=7,
        perm_id=2,
        contract={
            "symbol": "MGC",
            "localSymbol": "MGCM26",
            "securityType": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
        },
        status="Filled",
        quantity="1",
        completed_at=now,
    )
    adapter.completed_orders_end(occurred_at=now)
    adapter.exec_details(
        account_id="DU1234567",
        execution_id="exec-1",
        broker_order_id=6998,
        client_id=7,
        perm_id=2,
        contract={
            "symbol": "MGC",
            "localSymbol": "MGCM26",
            "securityType": "FUT",
            "exchange": "COMEX",
            "currency": "USD",
        },
        side="BOT",
        quantity="1",
        price="2450.7",
        executed_at=now,
    )
    adapter.exec_details_end(occurred_at=now)

    assert client.session.state.next_valid_order_id == 7001
    assert client.connection_state().managed_accounts == ("DU1234567", "DU7654321")
    assert client.balances()[0].buying_power == "100000"
    assert client.positions()[0].contract.symbol == "MGC"
    assert client.open_orders()[0].broker_order_id == 7001
    assert client.completed_orders()[0].broker_order_id == 6998
    assert client.executions()[0].execution_id == "exec-1"


def test_ibkr_callback_adapter_marks_session_disconnected_on_severe_error() -> None:
    now = datetime(2026, 4, 12, 16, 35, tzinfo=timezone.utc)
    client = _client()
    adapter = client.build_read_only_callback_adapter()

    adapter.managed_accounts(("DU1234567",), occurred_at=now)
    adapter.error(code=1100, message="Connectivity between IB and Trader Workstation has been lost", occurred_at=now)

    assert client.connection_state().connected is False
    assert "1100" in str(client.connection_state().last_error or "")
