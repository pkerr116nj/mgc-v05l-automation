"""IBKR execution-provider shell focused on normalized broker truth first."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from ..config.ibkr import load_ibkr_config
from ..brokers.ibkr import (
    IbkrBalanceRecord,
    IbkrClient,
    IbkrCompletedOrderRecord,
    IbkrConnectionState,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
    IbkrSession,
    IbkrTruthAdapter,
    build_default_ibkr_order_id_policy,
)
from .broker_requests import BrokerOrderRequest
from .order_models import OrderIntent
from .provider_interfaces import ExecutionProvider


class IbkrExecutionProvider(ExecutionProvider):
    """Truth-first IBKR provider scaffold.

    Order submission remains intentionally unimplemented until the later
    order-mapper and transport passes are complete.
    """

    provider_id = "ibkr_execution"

    def __init__(
        self,
        repo_root: Path,
        *,
        session: IbkrSession | None = None,
        client: IbkrClient | None = None,
        truth_adapter: IbkrTruthAdapter | None = None,
        balances: Iterable[IbkrBalanceRecord] = (),
        positions: Iterable[IbkrPositionRecord] = (),
        open_orders: Iterable[IbkrOpenOrderRecord] = (),
        completed_orders: Iterable[IbkrCompletedOrderRecord] = (),
        executions: Iterable[IbkrExecutionRecord] = (),
    ) -> None:
        self._repo_root = Path(repo_root).resolve(strict=False)
        if session is None:
            ibkr_config = load_ibkr_config()
            session = IbkrSession(
                host=ibkr_config.host,
                port=ibkr_config.port,
                client_id=ibkr_config.client_id,
                account_id=ibkr_config.account_id,
                gateway_mode=ibkr_config.mode,
                read_only=not ibkr_config.allow_live_orders,
                order_id_policy=build_default_ibkr_order_id_policy(
                    client_id=ibkr_config.client_id,
                    live_orders_enabled=ibkr_config.allow_live_orders,
                ),
            )
        self._session = session
        self._client = client or IbkrClient(session=self._session)
        self._truth_adapter = truth_adapter or IbkrTruthAdapter()
        self._balances = tuple(balances)
        self._positions = tuple(positions)
        self._open_orders = tuple(open_orders)
        self._completed_orders = tuple(completed_orders)
        self._executions = tuple(executions)

    def snapshot_truth(self) -> Any:
        return self._truth_adapter.build_truth_snapshot(
            connection_state=self._client.connection_state(),
            balances=self._client.balances() or self._balances,
            positions=self._client.positions() or self._positions,
            open_orders=self._client.open_orders() or self._open_orders,
            completed_orders=self._client.completed_orders() or self._completed_orders,
            executions=self._client.executions() or self._executions,
        )

    def snapshot_state(self, *, force_refresh: bool = False) -> dict[str, Any]:
        del force_refresh
        truth = self.snapshot_truth()
        payload = self._truth_adapter.truth_snapshot_to_dict(truth)
        selected_account_id = self.selected_account_id(payload)
        selected_positions = [
            row
            for row in truth.positions
            if selected_account_id is None or row.account_id == selected_account_id
        ]
        selected_open_orders = [
            row
            for row in truth.open_orders
            if selected_account_id is None or row.account_id == selected_account_id
        ]
        selected_executions = [
            row
            for row in truth.executions
            if selected_account_id is None or row.account_id == selected_account_id
        ]
        signed_position_quantity = sum(
            int(row.quantity) if str(row.side).upper() != "SHORT" else -int(row.quantity)
            for row in selected_positions
        )
        average_price = None
        if selected_positions:
            average_price = (
                str(selected_positions[0].average_cost)
                if selected_positions[0].average_cost is not None
                else None
            )
        open_order_ids = [row.broker_order_id for row in selected_open_orders]
        order_status = {row.broker_order_id: row.status for row in selected_open_orders}
        last_fill_timestamp = _latest_execution_timestamp(selected_executions)
        payload.update(
            {
                "connected": bool(truth.health and truth.health.connected),
                "truth_complete": bool(truth.health and truth.health.connected),
                "position_quantity": signed_position_quantity,
                "average_price": average_price,
                "open_order_ids": open_order_ids,
                "order_status": order_status,
                "last_fill_timestamp": last_fill_timestamp,
                "portfolio": {
                    "positions": [
                        {
                            "symbol": row.symbol,
                            "asset_class": row.asset_class,
                            "side": row.side,
                            "quantity": int(row.quantity),
                            "average_cost": str(row.average_cost) if row.average_cost is not None else None,
                            "mark_price": str(row.mark_price) if row.mark_price is not None else None,
                            "market_value": str(row.market_value) if row.market_value is not None else None,
                            "account_id": row.account_id,
                        }
                        for row in selected_positions
                    ],
                    "balances": [
                        {
                            "account_id": row.account_id,
                            "currency": row.currency,
                            "cash_balance": str(row.cash_balance) if row.cash_balance is not None else None,
                            "buying_power": str(row.buying_power) if row.buying_power is not None else None,
                            "available_funds": str(row.available_funds) if row.available_funds is not None else None,
                            "net_liquidation": str(row.net_liquidation) if row.net_liquidation is not None else None,
                        }
                        for row in truth.balances
                        if selected_account_id is None or row.account_id == selected_account_id
                    ],
                },
                "orders": {
                    "open_rows": [
                        {
                            "broker_order_id": row.broker_order_id,
                            "symbol": row.symbol,
                            "status": row.status,
                            "quantity": str(row.quantity),
                            "filled_quantity": str(row.filled_quantity) if row.filled_quantity is not None else None,
                            "updated_at": row.updated_at.isoformat() if row.updated_at is not None else None,
                            "account_id": row.account_id,
                        }
                        for row in selected_open_orders
                    ],
                    "recent_fill_rows": [
                        {
                            "broker_order_id": row.broker_order_id,
                            "execution_id": row.execution_id,
                            "symbol": row.symbol,
                            "quantity": str(row.quantity),
                            "fill_price": str(row.price) if row.price is not None else None,
                            "updated_at": row.executed_at.isoformat() if row.executed_at is not None else None,
                            "closed_at": row.executed_at.isoformat() if row.executed_at is not None else None,
                            "account_id": row.account_id,
                        }
                        for row in selected_executions
                    ],
                },
            }
        )
        return payload

    def selected_account_id(self, snapshot: dict[str, Any] | None = None) -> str | None:
        payload = snapshot or self.snapshot_state()
        return str(payload.get("selected_account_id") or "").strip() or None

    def selected_account_hash(self, snapshot: dict[str, Any] | None = None) -> str | None:
        return self.selected_account_id(snapshot)

    def build_order_request(
        self,
        *,
        order_intent: OrderIntent,
        quote_snapshot: Any | None = None,
    ) -> BrokerOrderRequest:
        del order_intent, quote_snapshot
        raise NotImplementedError("IBKR order request mapping is not implemented yet.")

    def submit_order(self, account_id: str, order_request: BrokerOrderRequest) -> dict[str, Any]:
        del account_id, order_request
        raise NotImplementedError("IBKR order submission is not implemented yet.")

    def cancel_order(self, account_id: str, broker_order_id: str) -> None:
        del account_id, broker_order_id
        raise NotImplementedError("IBKR order cancellation is not implemented yet.")

    def get_order_status(self, account_id: str, broker_order_id: str) -> dict[str, Any]:
        del account_id, broker_order_id
        raise NotImplementedError("IBKR order status lookup is not implemented yet.")


def _latest_execution_timestamp(executions: Iterable[Any]) -> str | None:
    latest: datetime | None = None
    for row in executions:
        executed_at = getattr(row, "executed_at", None)
        if executed_at is None:
            continue
        if latest is None or executed_at > latest:
            latest = executed_at
    return latest.isoformat() if latest is not None else None
