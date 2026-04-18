"""Raw IBKR-facing models used by transport and adapter scaffolding."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class IbkrAccountScope(str, Enum):
    SELECTED_ACCOUNT = "selected_account"
    ALL_OPEN_ORDERS = "all_open_orders"
    COMPLETED_ORDERS = "completed_orders"
    EXECUTIONS = "executions"


@dataclass(frozen=True)
class IbkrConnectionState:
    host: str
    port: int
    client_id: int
    account_id: str | None
    connected: bool = False
    gateway_mode: str = "paper"
    read_only: bool = True
    managed_accounts: tuple[str, ...] = ()
    next_valid_order_id: int | None = None
    connected_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class IbkrRawEvent:
    event_type: str
    occurred_at: datetime | None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrRequestRecord:
    request_type: str
    requested_at: datetime | None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrContractDescriptor:
    con_id: int | None
    symbol: str
    local_symbol: str | None
    security_type: str
    exchange: str
    currency: str
    expiry: str | None = None
    multiplier: str | None = None
    trading_class: str | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrPositionRecord:
    account_id: str
    contract: IbkrContractDescriptor
    quantity: str
    average_cost: str | None = None
    market_price: str | None = None
    market_value: str | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrBalanceRecord:
    account_id: str
    currency: str | None
    cash_balance: str | None = None
    buying_power: str | None = None
    available_funds: str | None = None
    net_liquidation: str | None = None
    maintenance_requirement: str | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrOpenOrderRecord:
    account_id: str
    broker_order_id: int
    client_id: int
    perm_id: int | None
    contract: IbkrContractDescriptor
    status: str
    quantity: str
    filled_quantity: str | None = None
    limit_price: str | None = None
    stop_price: str | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrCompletedOrderRecord:
    account_id: str
    broker_order_id: int
    client_id: int
    perm_id: int | None
    contract: IbkrContractDescriptor
    status: str
    quantity: str
    completed_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IbkrExecutionRecord:
    account_id: str
    execution_id: str
    broker_order_id: int | None
    client_id: int | None
    perm_id: int | None
    contract: IbkrContractDescriptor
    side: str | None
    quantity: str
    price: str | None
    executed_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)
