"""Canonical package for the MGC v0.5l external automation engine.

Keep package import side effects minimal.

Entry points such as ``python -m mgc_v05l.app.main`` import this package before
reaching the real module body. Eagerly importing execution/config modules here
forces heavy Pydantic model construction during bootstrap and can stall the
dashboard/service host before it ever binds localhost. Export the same public
symbols lazily instead.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

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

_EXPORT_MAP = {
    "Bar": ("mgc_v05l.domain.models", "Bar"),
    "FeaturePacket": ("mgc_v05l.domain.models", "FeaturePacket"),
    "FillEvent": ("mgc_v05l.execution.order_models", "FillEvent"),
    "HealthSnapshot": ("mgc_v05l.domain.models", "HealthSnapshot"),
    "HealthStatus": ("mgc_v05l.domain.enums", "HealthStatus"),
    "LongEntryFamily": ("mgc_v05l.domain.enums", "LongEntryFamily"),
    "OrderIntent": ("mgc_v05l.execution.order_models", "OrderIntent"),
    "OrderIntentType": ("mgc_v05l.domain.enums", "OrderIntentType"),
    "OrderStatus": ("mgc_v05l.domain.enums", "OrderStatus"),
    "PositionSide": ("mgc_v05l.domain.enums", "PositionSide"),
    "SignalPacket": ("mgc_v05l.domain.models", "SignalPacket"),
    "StrategyState": ("mgc_v05l.domain.models", "StrategyState"),
    "StrategyStatus": ("mgc_v05l.domain.enums", "StrategyStatus"),
}


def __getattr__(name: str) -> Any:
    target = _EXPORT_MAP.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = target
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


if TYPE_CHECKING:
    from .domain.enums import HealthStatus, LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
    from .domain.models import Bar, FeaturePacket, HealthSnapshot, SignalPacket, StrategyState
    from .execution.order_models import FillEvent, OrderIntent
