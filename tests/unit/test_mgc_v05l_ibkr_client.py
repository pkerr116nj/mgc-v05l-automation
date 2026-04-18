from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.brokers.ibkr import (
    IbkrBalanceRecord,
    IbkrClient,
    IbkrCompletedOrderRecord,
    IbkrContractDescriptor,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)


def _session() -> IbkrSession:
    return IbkrSession(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        account_id="DU1234567",
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False),
    )


def _contract() -> IbkrContractDescriptor:
    return IbkrContractDescriptor(
        con_id=12345,
        symbol="MGC",
        local_symbol="MGCM26",
        security_type="FUT",
        exchange="COMEX",
        currency="USD",
        expiry="202606",
        multiplier="10",
    )


def test_ibkr_client_records_request_log_and_replaces_buffers() -> None:
    now = datetime(2026, 4, 12, 16, 0, tzinfo=timezone.utc)
    client = IbkrClient(session=_session())

    client.request_balances()
    client.request_positions()
    client.record_managed_accounts(("DU1234567",), occurred_at=now)
    client.replace_balances((IbkrBalanceRecord(account_id="DU1234567", currency="USD", buying_power="100000", updated_at=now),), occurred_at=now)
    client.replace_positions((IbkrPositionRecord(account_id="DU1234567", contract=_contract(), quantity="1", updated_at=now),), occurred_at=now)
    client.replace_open_orders((IbkrOpenOrderRecord(account_id="DU1234567", broker_order_id=7001, client_id=7, perm_id=1, contract=_contract(), status="Submitted", quantity="1", updated_at=now),), occurred_at=now)
    client.replace_completed_orders((IbkrCompletedOrderRecord(account_id="DU1234567", broker_order_id=6998, client_id=7, perm_id=2, contract=_contract(), status="Filled", quantity="1", completed_at=now),), occurred_at=now)
    client.replace_executions((IbkrExecutionRecord(account_id="DU1234567", execution_id="exec-1", broker_order_id=6998, client_id=7, perm_id=2, contract=_contract(), side="BOT", quantity="1", price="2450.7", executed_at=now),), occurred_at=now)

    assert [row.request_type for row in client.request_log()] == ["balances", "positions"]
    assert client.connection_state().managed_accounts == ("DU1234567",)
    assert client.balances()[0].buying_power == "100000"
    assert client.positions()[0].contract.symbol == "MGC"
    assert client.open_orders()[0].broker_order_id == 7001
    assert client.completed_orders()[0].broker_order_id == 6998
    assert client.executions()[0].execution_id == "exec-1"


def test_ibkr_client_drains_recorded_events() -> None:
    client = IbkrClient(session=_session())

    client.record_event("transport_ready", payload={"host": "127.0.0.1"})
    drained = client.drain_events()

    assert len(drained) == 1
    assert drained[0].event_type == "transport_ready"
    assert client.drain_events() == ()

