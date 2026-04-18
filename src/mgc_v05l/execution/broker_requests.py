"""Broker-neutral order request models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


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


@dataclass(frozen=True)
class BrokerContractRequest:
    asset_class: str
    symbol: str
    broker_symbol: str | None = None
    exchange: str | None = None
    currency: str | None = None
    expiry: str | None = None
    multiplier: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return _serialize_value(self)


@dataclass(frozen=True)
class BrokerOrderRequest:
    account_id: str | None
    contract: BrokerContractRequest
    side: str
    quantity: Decimal
    order_type: str
    time_in_force: str
    session: str | None
    intent_type: str
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    client_order_id: str | None = None
    pricing_source: str = "market_data"
    metadata: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return _serialize_value(self)
