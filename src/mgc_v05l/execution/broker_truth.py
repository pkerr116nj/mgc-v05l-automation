"""Broker-neutral truth snapshot models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class BrokerHealthSnapshot:
    provider_id: str
    connected: bool
    checked_at: datetime | None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerAccountSnapshot:
    provider_id: str
    account_id: str
    account_number: str | None
    display_name: str
    account_type: str | None
    selected: bool
    updated_at: datetime | None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerBalanceSnapshot:
    provider_id: str
    account_id: str
    currency: str | None
    cash_balance: Decimal | None = None
    buying_power: Decimal | None = None
    available_funds: Decimal | None = None
    net_liquidation: Decimal | None = None
    maintenance_requirement: Decimal | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    provider_id: str
    account_id: str
    symbol: str
    asset_class: str
    quantity: Decimal
    side: str
    average_cost: Decimal | None = None
    mark_price: Decimal | None = None
    market_value: Decimal | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerOpenOrderSnapshot:
    provider_id: str
    account_id: str
    broker_order_id: str
    symbol: str
    status: str
    quantity: Decimal
    filled_quantity: Decimal | None = None
    updated_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerCompletedOrderSnapshot:
    provider_id: str
    account_id: str
    broker_order_id: str
    symbol: str
    status: str
    quantity: Decimal
    completed_at: datetime | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerExecutionSnapshot:
    provider_id: str
    account_id: str
    broker_order_id: str | None
    execution_id: str | None
    symbol: str
    quantity: Decimal
    price: Decimal | None
    executed_at: datetime | None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BrokerTruthSnapshot:
    provider_id: str
    selected_account_id: str | None
    accounts: tuple[BrokerAccountSnapshot, ...] = ()
    balances: tuple[BrokerBalanceSnapshot, ...] = ()
    positions: tuple[BrokerPositionSnapshot, ...] = ()
    open_orders: tuple[BrokerOpenOrderSnapshot, ...] = ()
    completed_orders: tuple[BrokerCompletedOrderSnapshot, ...] = ()
    executions: tuple[BrokerExecutionSnapshot, ...] = ()
    health: BrokerHealthSnapshot | None = None
    generated_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
