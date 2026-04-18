"""Read-only callback adapter for future IBKR EWrapper integration."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .ibkr_client import IbkrClient
from .ibkr_models import (
    IbkrBalanceRecord,
    IbkrCompletedOrderRecord,
    IbkrContractDescriptor,
    IbkrExecutionRecord,
    IbkrOpenOrderRecord,
    IbkrPositionRecord,
)

_SEVERE_CONNECTION_ERROR_CODES = {504, 1100, 1101, 1102, 1300}


@dataclass
class _BalanceAccumulator:
    account_id: str
    currency: str | None
    cash_balance: str | None = None
    buying_power: str | None = None
    available_funds: str | None = None
    net_liquidation: str | None = None
    maintenance_requirement: str | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] | None = None

    def to_record(self) -> IbkrBalanceRecord:
        return IbkrBalanceRecord(
            account_id=self.account_id,
            currency=self.currency,
            cash_balance=self.cash_balance,
            buying_power=self.buying_power,
            available_funds=self.available_funds,
            net_liquidation=self.net_liquidation,
            maintenance_requirement=self.maintenance_requirement,
            updated_at=self.updated_at,
            raw_payload=dict(self.raw_payload or {}),
        )


class IbkrReadOnlyCallbackAdapter:
    """Collect read-only IBKR callback flows into the local client buffers."""

    def __init__(self, client: IbkrClient) -> None:
        self._client = client
        self._balance_accumulators: dict[tuple[str, str | None], _BalanceAccumulator] = {}
        self._position_rows: list[IbkrPositionRecord] = []
        self._open_order_rows: list[IbkrOpenOrderRecord] = []
        self._completed_order_rows: list[IbkrCompletedOrderRecord] = []
        self._execution_rows: list[IbkrExecutionRecord] = []

    def next_valid_id(self, order_id: int, *, occurred_at: datetime | None = None) -> None:
        self._client.session.seed_next_valid_order_id(int(order_id))
        self._client.record_event(
            "next_valid_id",
            payload={"next_valid_order_id": int(order_id)},
            occurred_at=occurred_at,
        )

    def managed_accounts(self, accounts: str | tuple[str, ...], *, occurred_at: datetime | None = None) -> None:
        if isinstance(accounts, str):
            managed_accounts = tuple(
                item.strip() for item in accounts.split(",") if str(item).strip()
            )
        else:
            managed_accounts = tuple(str(item).strip() for item in accounts if str(item).strip())
        self._client.record_managed_accounts(managed_accounts, occurred_at=occurred_at)

    def update_account_value(
        self,
        *,
        account_id: str,
        key: str,
        value: str,
        currency: str | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        normalized_account_id = str(account_id or "").strip()
        normalized_currency = str(currency or "").strip().upper() or None
        if not normalized_account_id:
            return
        accumulator_key = (normalized_account_id, normalized_currency)
        accumulator = self._balance_accumulators.get(accumulator_key)
        if accumulator is None:
            accumulator = _BalanceAccumulator(
                account_id=normalized_account_id,
                currency=normalized_currency,
            )
            self._balance_accumulators[accumulator_key] = accumulator
        normalized_key = str(key or "").strip()
        normalized_value = str(value or "").strip() or None
        accumulator.updated_at = occurred_at or datetime.now(timezone.utc)
        raw_payload = dict(accumulator.raw_payload or {})
        raw_payload[normalized_key] = normalized_value
        accumulator.raw_payload = raw_payload
        if normalized_key == "CashBalance":
            accumulator.cash_balance = normalized_value
        elif normalized_key == "BuyingPower":
            accumulator.buying_power = normalized_value
        elif normalized_key == "AvailableFunds":
            accumulator.available_funds = normalized_value
        elif normalized_key == "NetLiquidation":
            accumulator.net_liquidation = normalized_value
        elif normalized_key in {"MaintMarginReq", "MaintenanceMargin"}:
            accumulator.maintenance_requirement = normalized_value

    def account_download_end(self, *, occurred_at: datetime | None = None) -> None:
        rows = tuple(item.to_record() for item in self._balance_accumulators.values())
        self._client.replace_balances(rows, occurred_at=occurred_at)
        self._balance_accumulators.clear()

    def position(
        self,
        *,
        account_id: str,
        contract: IbkrContractDescriptor | dict[str, Any],
        quantity: str | Decimal | int | float,
        average_cost: str | Decimal | int | float | None = None,
        market_price: str | Decimal | int | float | None = None,
        market_value: str | Decimal | int | float | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._position_rows.append(
            IbkrPositionRecord(
                account_id=str(account_id).strip(),
                contract=_coerce_contract(contract),
                quantity=str(quantity),
                average_cost=_stringify_optional(average_cost),
                market_price=_stringify_optional(market_price),
                market_value=_stringify_optional(market_value),
                updated_at=occurred_at or datetime.now(timezone.utc),
            )
        )

    def position_end(self, *, occurred_at: datetime | None = None) -> None:
        self._client.replace_positions(tuple(self._position_rows), occurred_at=occurred_at)
        self._position_rows.clear()

    def open_order(
        self,
        *,
        account_id: str,
        broker_order_id: int,
        client_id: int,
        perm_id: int | None,
        contract: IbkrContractDescriptor | dict[str, Any],
        status: str,
        quantity: str | Decimal | int | float,
        filled_quantity: str | Decimal | int | float | None = None,
        limit_price: str | Decimal | int | float | None = None,
        stop_price: str | Decimal | int | float | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._open_order_rows.append(
            IbkrOpenOrderRecord(
                account_id=str(account_id).strip(),
                broker_order_id=int(broker_order_id),
                client_id=int(client_id),
                perm_id=None if perm_id is None else int(perm_id),
                contract=_coerce_contract(contract),
                status=str(status).strip(),
                quantity=str(quantity),
                filled_quantity=_stringify_optional(filled_quantity),
                limit_price=_stringify_optional(limit_price),
                stop_price=_stringify_optional(stop_price),
                updated_at=occurred_at or datetime.now(timezone.utc),
            )
        )

    def open_order_end(self, *, occurred_at: datetime | None = None) -> None:
        self._client.replace_open_orders(tuple(self._open_order_rows), occurred_at=occurred_at)
        self._open_order_rows.clear()

    def completed_order(
        self,
        *,
        account_id: str,
        broker_order_id: int,
        client_id: int,
        perm_id: int | None,
        contract: IbkrContractDescriptor | dict[str, Any],
        status: str,
        quantity: str | Decimal | int | float,
        completed_at: datetime | None = None,
    ) -> None:
        self._completed_order_rows.append(
            IbkrCompletedOrderRecord(
                account_id=str(account_id).strip(),
                broker_order_id=int(broker_order_id),
                client_id=int(client_id),
                perm_id=None if perm_id is None else int(perm_id),
                contract=_coerce_contract(contract),
                status=str(status).strip(),
                quantity=str(quantity),
                completed_at=completed_at or datetime.now(timezone.utc),
            )
        )

    def completed_orders_end(self, *, occurred_at: datetime | None = None) -> None:
        self._client.replace_completed_orders(tuple(self._completed_order_rows), occurred_at=occurred_at)
        self._completed_order_rows.clear()

    def exec_details(
        self,
        *,
        account_id: str,
        execution_id: str,
        broker_order_id: int | None,
        client_id: int | None,
        perm_id: int | None,
        contract: IbkrContractDescriptor | dict[str, Any],
        side: str | None,
        quantity: str | Decimal | int | float,
        price: str | Decimal | int | float | None,
        executed_at: datetime | None = None,
    ) -> None:
        self._execution_rows.append(
            IbkrExecutionRecord(
                account_id=str(account_id).strip(),
                execution_id=str(execution_id).strip(),
                broker_order_id=None if broker_order_id is None else int(broker_order_id),
                client_id=None if client_id is None else int(client_id),
                perm_id=None if perm_id is None else int(perm_id),
                contract=_coerce_contract(contract),
                side=str(side).strip() or None if side is not None else None,
                quantity=str(quantity),
                price=_stringify_optional(price),
                executed_at=executed_at or datetime.now(timezone.utc),
            )
        )

    def exec_details_end(self, *, occurred_at: datetime | None = None) -> None:
        self._client.replace_executions(tuple(self._execution_rows), occurred_at=occurred_at)
        self._execution_rows.clear()

    def error(
        self,
        *,
        code: int,
        message: str,
        request_id: int | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        payload = {
            "code": int(code),
            "message": str(message),
            "request_id": request_id,
        }
        self._client.record_event("error", payload=payload, occurred_at=occurred_at)
        if int(code) in _SEVERE_CONNECTION_ERROR_CODES:
            self._client.session.mark_disconnected(
                reason=f"IBKR error {code}: {message}",
                occurred_at=occurred_at,
            )


def _coerce_contract(contract: IbkrContractDescriptor | dict[str, Any]) -> IbkrContractDescriptor:
    if isinstance(contract, IbkrContractDescriptor):
        return contract
    payload = dict(contract or {})
    return IbkrContractDescriptor(
        con_id=_int_or_none(payload.get("con_id") or payload.get("conId")),
        symbol=str(payload.get("symbol") or "").strip(),
        local_symbol=_stringify_optional(payload.get("local_symbol") or payload.get("localSymbol")),
        security_type=str(payload.get("security_type") or payload.get("securityType") or "").strip(),
        exchange=str(payload.get("exchange") or "").strip(),
        currency=str(payload.get("currency") or "").strip(),
        expiry=_stringify_optional(payload.get("expiry") or payload.get("lastTradeDateOrContractMonth")),
        multiplier=_stringify_optional(payload.get("multiplier")),
        trading_class=_stringify_optional(payload.get("trading_class") or payload.get("tradingClass")),
        raw_payload=payload,
    )


def _stringify_optional(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)

