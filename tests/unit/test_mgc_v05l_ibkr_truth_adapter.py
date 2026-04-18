from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.brokers.ibkr import (
    IbkrBalanceRecord,
    IbkrCompletedOrderRecord,
    IbkrConnectionState,
    IbkrContractDescriptor,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
    IbkrTruthAdapter,
)


def test_ibkr_truth_adapter_builds_normalized_truth_snapshot() -> None:
    now = datetime(2026, 4, 12, 14, 5, tzinfo=timezone.utc)
    adapter = IbkrTruthAdapter()
    connection_state = IbkrConnectionState(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        account_id="DU1234567",
        connected=True,
        gateway_mode="paper",
        managed_accounts=("DU1234567",),
        next_valid_order_id=7001,
        connected_at=now,
        last_heartbeat_at=now,
    )
    contract = IbkrContractDescriptor(
        con_id=12345,
        symbol="MGC",
        local_symbol="MGCM26",
        security_type="FUT",
        exchange="COMEX",
        currency="USD",
        expiry="202606",
        multiplier="10",
        trading_class="MGC",
    )

    snapshot = adapter.build_truth_snapshot(
        connection_state=connection_state,
        balances=(
            IbkrBalanceRecord(
                account_id="DU1234567",
                currency="USD",
                cash_balance="50000",
                buying_power="100000",
                available_funds="45000",
                net_liquidation="55000",
                maintenance_requirement="5000",
                updated_at=now,
            ),
        ),
        positions=(
            IbkrPositionRecord(
                account_id="DU1234567",
                contract=contract,
                quantity="2",
                average_cost="2450.5",
                market_price="2451.1",
                market_value="49022",
                updated_at=now,
            ),
        ),
        open_orders=(
            IbkrOpenOrderRecord(
                account_id="DU1234567",
                broker_order_id=7001,
                client_id=7,
                perm_id=10001,
                contract=contract,
                status="Submitted",
                quantity="1",
                filled_quantity="0",
                updated_at=now,
            ),
        ),
        completed_orders=(
            IbkrCompletedOrderRecord(
                account_id="DU1234567",
                broker_order_id=6998,
                client_id=7,
                perm_id=9998,
                contract=contract,
                status="Filled",
                quantity="1",
                completed_at=now,
            ),
        ),
        executions=(
            IbkrExecutionRecord(
                account_id="DU1234567",
                execution_id="0001",
                broker_order_id=6998,
                client_id=7,
                perm_id=9998,
                contract=contract,
                side="BOT",
                quantity="1",
                price="2450.7",
                executed_at=now,
            ),
        ),
    )

    assert snapshot.selected_account_id == "DU1234567"
    assert snapshot.health is not None
    assert snapshot.health.connected is True
    assert snapshot.accounts[0].account_id == "DU1234567"
    assert snapshot.balances[0].buying_power == 100000
    assert snapshot.positions[0].symbol == "MGC"
    assert snapshot.open_orders[0].broker_order_id == "7001"
    assert snapshot.completed_orders[0].broker_order_id == "6998"
    assert snapshot.executions[0].execution_id == "0001"


def test_ibkr_truth_adapter_serializes_truth_snapshot_for_provider_state() -> None:
    now = datetime(2026, 4, 12, 14, 5, tzinfo=timezone.utc)
    adapter = IbkrTruthAdapter()
    snapshot = adapter.build_truth_snapshot(
        connection_state=IbkrConnectionState(
            host="127.0.0.1",
            port=7497,
            client_id=7,
            account_id="DU1234567",
            connected=False,
            gateway_mode="paper",
            managed_accounts=("DU1234567", "DU7654321"),
            connected_at=now,
            last_heartbeat_at=now,
            last_error="socket closed",
        ),
    )

    payload = adapter.truth_snapshot_to_dict(snapshot)

    assert payload["provider_id"] == "ibkr_execution"
    assert payload["selected_account_id"] == "DU1234567"
    assert payload["health"]["connected"] is False
    assert payload["health"]["details"]["last_error"] == "socket closed"
    assert payload["accounts"][1]["account_id"] == "DU7654321"
    assert payload["generated_at"] == now.isoformat()

