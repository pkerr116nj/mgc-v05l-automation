"""Canonical package for the MGC v0.5l external automation engine."""

from .domain.enums import HealthStatus, LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from .domain.models import Bar, FeaturePacket, HealthSnapshot, SignalPacket, StrategyState
from .execution.order_models import FillEvent, OrderIntent

__all__ = [
    "Bar",
    "FeaturePacket",
    "FillEvent",
    "HealthSnapshot",
    "HealthStatus",
    "LongEntryFamily",
    "OrderIntent",
    "OrderIntentType",
    "OrderStatus",
    "PositionSide",
    "SignalPacket",
    "StrategyState",
    "StrategyStatus",
]
