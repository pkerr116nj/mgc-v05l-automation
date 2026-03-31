"""Event models for the Phase 2.5 runtime topology."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional, Union

from .enums import ExitReason, LongEntryFamily, OrderIntentType, ShortEntryFamily


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
    all_true_reasons: tuple[ExitReason, ...] = tuple()
    long_entry_family: LongEntryFamily = LongEntryFamily.NONE
    short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE
    short_entry_source: Optional[str] = None
    long_break_even_armed: bool = False
    short_break_even_armed: bool = False
    active_long_stop_ref: Optional[Decimal] = None
    active_short_stop_ref: Optional[Decimal] = None
    additive_short_max_favorable_excursion: Decimal = Decimal("0")
    additive_short_peak_threshold_reached: bool = False
    additive_short_giveback_from_peak: Decimal = Decimal("0")


DomainEvent = Union[
    BarClosedEvent,
    ExitEvaluatedEvent,
    FillReceivedEvent,
    FaultRaisedEvent,
    OrderIntentCreatedEvent,
    ServiceStartupEvent,
]
