"""Normalize raw IBKR state into the shared broker-truth model."""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable

from ...execution.broker_truth import (
    BrokerAccountSnapshot,
    BrokerBalanceSnapshot,
    BrokerCompletedOrderSnapshot,
    BrokerExecutionSnapshot,
    BrokerHealthSnapshot,
    BrokerOpenOrderSnapshot,
    BrokerPositionSnapshot,
    BrokerTruthSnapshot,
)
from .ibkr_models import (
    IbkrBalanceRecord,
    IbkrCompletedOrderRecord,
    IbkrConnectionState,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
)


def _as_decimal(value: str | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _serialize_value(value: Any) -> Any:
    if is_dataclass(value):
        return {key: _serialize_value(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class IbkrTruthAdapter:
    provider_id = "ibkr_execution"

    def normalize_accounts(self, connection_state: IbkrConnectionState) -> tuple[BrokerAccountSnapshot, ...]:
        account_ids: list[str] = []
        for account_id in (connection_state.account_id, *connection_state.managed_accounts):
            normalized = str(account_id or "").strip()
            if normalized and normalized not in account_ids:
                account_ids.append(normalized)
        return tuple(
            BrokerAccountSnapshot(
                provider_id=self.provider_id,
                account_id=account_id,
                account_number=account_id,
                display_name=f"IBKR {account_id}",
                account_type=connection_state.gateway_mode.upper(),
                selected=account_id == connection_state.account_id,
                updated_at=connection_state.last_heartbeat_at or connection_state.connected_at,
                raw_payload={
                    "managed_accounts": list(connection_state.managed_accounts),
                    "client_id": connection_state.client_id,
                    "gateway_mode": connection_state.gateway_mode,
                },
            )
            for account_id in account_ids
        )

    def normalize_balances(self, rows: Iterable[IbkrBalanceRecord]) -> tuple[BrokerBalanceSnapshot, ...]:
        return tuple(
            BrokerBalanceSnapshot(
                provider_id=self.provider_id,
                account_id=row.account_id,
                currency=row.currency,
                cash_balance=_as_decimal(row.cash_balance),
                buying_power=_as_decimal(row.buying_power),
                available_funds=_as_decimal(row.available_funds),
                net_liquidation=_as_decimal(row.net_liquidation),
                maintenance_requirement=_as_decimal(row.maintenance_requirement),
                updated_at=row.updated_at,
                raw_payload=dict(row.raw_payload or {}),
            )
            for row in rows
        )

    def normalize_positions(self, rows: Iterable[IbkrPositionRecord]) -> tuple[BrokerPositionSnapshot, ...]:
        normalized_rows: list[BrokerPositionSnapshot] = []
        for row in rows:
            quantity = _as_decimal(row.quantity) or Decimal("0")
            normalized_rows.append(
                BrokerPositionSnapshot(
                    provider_id=self.provider_id,
                    account_id=row.account_id,
                    symbol=row.contract.symbol,
                    asset_class=row.contract.security_type,
                    quantity=abs(quantity),
                    side="LONG" if quantity >= 0 else "SHORT",
                    average_cost=_as_decimal(row.average_cost),
                    mark_price=_as_decimal(row.market_price),
                    market_value=_as_decimal(row.market_value),
                    updated_at=row.updated_at,
                    raw_payload=dict(row.raw_payload or {}),
                )
            )
        return tuple(normalized_rows)

    def normalize_open_orders(self, rows: Iterable[IbkrOpenOrderRecord]) -> tuple[BrokerOpenOrderSnapshot, ...]:
        return tuple(
            BrokerOpenOrderSnapshot(
                provider_id=self.provider_id,
                account_id=row.account_id,
                broker_order_id=str(row.broker_order_id),
                symbol=row.contract.symbol,
                status=row.status,
                quantity=_as_decimal(row.quantity) or Decimal("0"),
                filled_quantity=_as_decimal(row.filled_quantity),
                updated_at=row.updated_at,
                raw_payload=dict(row.raw_payload or {}),
            )
            for row in rows
        )

    def normalize_completed_orders(
        self,
        rows: Iterable[IbkrCompletedOrderRecord],
    ) -> tuple[BrokerCompletedOrderSnapshot, ...]:
        return tuple(
            BrokerCompletedOrderSnapshot(
                provider_id=self.provider_id,
                account_id=row.account_id,
                broker_order_id=str(row.broker_order_id),
                symbol=row.contract.symbol,
                status=row.status,
                quantity=_as_decimal(row.quantity) or Decimal("0"),
                completed_at=row.completed_at,
                raw_payload=dict(row.raw_payload or {}),
            )
            for row in rows
        )

    def normalize_executions(self, rows: Iterable[IbkrExecutionRecord]) -> tuple[BrokerExecutionSnapshot, ...]:
        return tuple(
            BrokerExecutionSnapshot(
                provider_id=self.provider_id,
                account_id=row.account_id,
                broker_order_id=str(row.broker_order_id) if row.broker_order_id is not None else None,
                execution_id=row.execution_id,
                symbol=row.contract.symbol,
                quantity=_as_decimal(row.quantity) or Decimal("0"),
                price=_as_decimal(row.price),
                executed_at=row.executed_at,
                raw_payload=dict(row.raw_payload or {}),
            )
            for row in rows
        )

    def normalize_health(self, connection_state: IbkrConnectionState) -> BrokerHealthSnapshot:
        return BrokerHealthSnapshot(
            provider_id=self.provider_id,
            connected=connection_state.connected,
            checked_at=connection_state.last_heartbeat_at or connection_state.connected_at,
            details={
                "host": connection_state.host,
                "port": connection_state.port,
                "client_id": connection_state.client_id,
                "gateway_mode": connection_state.gateway_mode,
                "managed_accounts": list(connection_state.managed_accounts),
                "next_valid_order_id": connection_state.next_valid_order_id,
                "last_error": connection_state.last_error,
            },
        )

    def build_truth_snapshot(
        self,
        *,
        connection_state: IbkrConnectionState,
        balances: Iterable[IbkrBalanceRecord] = (),
        positions: Iterable[IbkrPositionRecord] = (),
        open_orders: Iterable[IbkrOpenOrderRecord] = (),
        completed_orders: Iterable[IbkrCompletedOrderRecord] = (),
        executions: Iterable[IbkrExecutionRecord] = (),
    ) -> BrokerTruthSnapshot:
        checked_at = connection_state.last_heartbeat_at or connection_state.connected_at
        return BrokerTruthSnapshot(
            provider_id=self.provider_id,
            selected_account_id=connection_state.account_id,
            accounts=self.normalize_accounts(connection_state),
            balances=self.normalize_balances(balances),
            positions=self.normalize_positions(positions),
            open_orders=self.normalize_open_orders(open_orders),
            completed_orders=self.normalize_completed_orders(completed_orders),
            executions=self.normalize_executions(executions),
            health=self.normalize_health(connection_state),
            generated_at=checked_at,
            metadata={
                "visibility_scope": {
                    "selected_account_id": connection_state.account_id,
                    "managed_accounts": list(connection_state.managed_accounts),
                }
            },
        )

    def truth_snapshot_to_dict(self, snapshot: BrokerTruthSnapshot) -> dict[str, Any]:
        return _serialize_value(snapshot)

