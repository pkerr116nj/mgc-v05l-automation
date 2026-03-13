"""Reconciliation models derived from the Phase 2 architecture."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from .enums import MismatchClassification, ReconciliationTrigger, StrategySide


@dataclass(frozen=True)
class InternalPositionSnapshot:
    quantity: int
    side: StrategySide
    average_entry_price: Optional[Decimal]
    open_order_count: int
    last_fill_timestamp: Optional[datetime]


@dataclass(frozen=True)
class BrokerPositionSnapshot:
    quantity: int
    side: StrategySide
    average_entry_price: Optional[Decimal]
    open_order_count: int
    last_fill_timestamp: Optional[datetime]


@dataclass(frozen=True)
class ReconciliationEvent:
    trigger: ReconciliationTrigger
    internal_snapshot: InternalPositionSnapshot
    broker_snapshot: BrokerPositionSnapshot
    mismatch_classification: MismatchClassification
    repair_action: str
