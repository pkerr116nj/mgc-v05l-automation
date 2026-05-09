"""Execution package exports.

Avoid eager imports here so lightweight consumers can import
``mgc_v05l.execution.order_models`` without dragging the entire execution
engine, settings stack, and broker integrations into startup.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = [
    "ExecutionEngine",
    "PendingExecution",
    "SubmitFailure",
    "FillEvent",
    "OrderIntent",
]

_EXPORT_MAP = {
    "ExecutionEngine": ("mgc_v05l.execution.execution_engine", "ExecutionEngine"),
    "PendingExecution": ("mgc_v05l.execution.execution_engine", "PendingExecution"),
    "SubmitFailure": ("mgc_v05l.execution.execution_engine", "SubmitFailure"),
    "FillEvent": ("mgc_v05l.execution.order_models", "FillEvent"),
    "OrderIntent": ("mgc_v05l.execution.order_models", "OrderIntent"),
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
    from .execution_engine import ExecutionEngine, PendingExecution, SubmitFailure
    from .order_models import FillEvent, OrderIntent
