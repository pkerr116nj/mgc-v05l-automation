"""Audit and fault models derived from the Phase 2 architecture."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from .enums import AckStatus, FillStatus, FaultSeverity, OrderIntentType, OperatingState


@dataclass(frozen=True)
class BarDecisionAuditRecord:
    bar_timestamp: datetime
    order_generated: bool
    operating_state: OperatingState
    primary_exit_reason_code: Optional[str]


@dataclass(frozen=True)
class OrderAuditRecord:
    order_intent_id: str
    strategy_bar_id: str
    side: OrderIntentType
    order_type: str
    quantity: int
    submit_timestamp: datetime
    broker_order_id: Optional[str]
    ack_status: AckStatus
    fill_status: FillStatus
    fill_timestamp: Optional[datetime]
    fill_price: Optional[Decimal]
    reason_code: str


@dataclass(frozen=True)
class FaultEvent:
    occurred_at: datetime
    severity: FaultSeverity
    source: str
    message: str
