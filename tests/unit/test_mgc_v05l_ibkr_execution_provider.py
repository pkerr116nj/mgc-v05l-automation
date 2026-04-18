from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.brokers.ibkr import (
    IbkrBalanceRecord,
    IbkrContractDescriptor,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)
from mgc_v05l.execution.ibkr_execution_provider import IbkrExecutionProvider


def test_ibkr_execution_provider_surfaces_normalized_truth_snapshot() -> None:
    now = datetime(2026, 4, 12, 14, 10, tzinfo=timezone.utc)
    session = IbkrSession(
        host="127.0.0.1",
        port=7497,
        client_id=7,
        account_id="DU1234567",
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False),
    )
    session.mark_connected(
        managed_accounts=("DU1234567",),
        connected_at=now,
    )
    session.record_heartbeat(occurred_at=now)
    provider = IbkrExecutionProvider(
        Path.cwd(),
        session=session,
        balances=(
            IbkrBalanceRecord(
                account_id="DU1234567",
                currency="USD",
                buying_power="100000",
                net_liquidation="55000",
                updated_at=now,
            ),
        ),
        positions=(
            IbkrPositionRecord(
                account_id="DU1234567",
                contract=IbkrContractDescriptor(
                    con_id=12345,
                    symbol="MGC",
                    local_symbol="MGCM26",
                    security_type="FUT",
                    exchange="COMEX",
                    currency="USD",
                    expiry="202606",
                    multiplier="10",
                ),
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
                contract=IbkrContractDescriptor(
                    con_id=12345,
                    symbol="MGC",
                    local_symbol="MGCM26",
                    security_type="FUT",
                    exchange="COMEX",
                    currency="USD",
                    expiry="202606",
                    multiplier="10",
                ),
                status="Submitted",
                quantity="1",
                filled_quantity="0",
                updated_at=now,
            ),
        ),
        executions=(
            IbkrExecutionRecord(
                account_id="DU1234567",
                execution_id="exec-1",
                broker_order_id=6998,
                client_id=7,
                perm_id=9998,
                contract=IbkrContractDescriptor(
                    con_id=12345,
                    symbol="MGC",
                    local_symbol="MGCM26",
                    security_type="FUT",
                    exchange="COMEX",
                    currency="USD",
                    expiry="202606",
                    multiplier="10",
                ),
                side="BOT",
                quantity="1",
                price="2450.7",
                executed_at=now,
            ),
        ),
    )

    payload = provider.snapshot_state()

    assert payload["provider_id"] == "ibkr_execution"
    assert payload["selected_account_id"] == "DU1234567"
    assert payload["health"]["connected"] is True
    assert payload["balances"][0]["buying_power"] == "100000"
    assert payload["truth_complete"] is True
    assert payload["position_quantity"] == 2
    assert payload["average_price"] == "2450.5"
    assert payload["open_order_ids"] == ["7001"]
    assert payload["order_status"] == {"7001": "Submitted"}
    assert payload["last_fill_timestamp"] == now.isoformat()
    assert payload["portfolio"]["positions"][0]["symbol"] == "MGC"
    assert payload["orders"]["open_rows"][0]["broker_order_id"] == "7001"
    assert payload["orders"]["recent_fill_rows"][0]["execution_id"] == "exec-1"


def test_ibkr_execution_provider_marks_order_paths_unimplemented_for_now() -> None:
    provider = IbkrExecutionProvider(
        Path.cwd(),
        session=IbkrSession(
            host="127.0.0.1",
            port=7497,
            client_id=7,
            account_id="DU1234567",
            gateway_mode="paper",
            read_only=True,
            order_id_policy=build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False),
        ),
    )

    with pytest.raises(NotImplementedError, match="not implemented yet"):
        provider.submit_order("DU1234567", None)  # type: ignore[arg-type]
