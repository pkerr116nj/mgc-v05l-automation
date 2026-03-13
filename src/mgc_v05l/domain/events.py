"""Event models for the Phase 2.5 runtime topology."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional, Union

from .enums import ExitReason, OrderIntentType


@dataclass(frozen=True)
class ServiceStartupEvent:
    source: str


@dataclass(frozen=True)
class BarClosedEvent:
    bar_id: str
    occurred_at: datetime


@dataclass(frozen=True)
class OrderIntentCreatedEvent:
    order_intent_id: str
    bar_id: str
    intent_type: OrderIntentType
    occurred_at: datetime


@dataclass(frozen=True)
class FillReceivedEvent:
    order_intent_id: str
    broker_order_id: Optional[str]
    fill_timestamp: datetime
    fill_price: Optional[Decimal]


@dataclass(frozen=True)
class FaultRaisedEvent:
    fault_code: str
    occurred_at: datetime


@dataclass(frozen=True)
class ExitEvaluatedEvent:
    bar_id: str
    primary_reason: Optional[ExitReason]
    occurred_at: datetime


DomainEvent = Union[
    BarClosedEvent,
    ExitEvaluatedEvent,
    FillReceivedEvent,
    FaultRaisedEvent,
    OrderIntentCreatedEvent,
    ServiceStartupEvent,
]
