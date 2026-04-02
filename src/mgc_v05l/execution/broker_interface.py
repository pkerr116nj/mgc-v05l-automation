"""Exact broker interface contract."""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol

from ..domain.models import Bar
from .order_models import FillEvent, OrderIntent


class BrokerInterface(Protocol):
    """Broker abstraction required by the Phase 3A blueprint."""

    def connect(self) -> None:
        """Connect to the broker."""

    def disconnect(self) -> None:
        """Disconnect from the broker."""

    def is_connected(self) -> bool:
        """Return connectivity state."""

    def submit_order(self, order_intent: OrderIntent) -> str:
        """Submit an order intent and return the broker order identifier."""

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel an existing broker order."""

    def get_order_status(self, broker_order_id: str) -> Any:
        """Return the broker order-status payload."""

    def get_open_orders(self) -> Sequence[Any]:
        """Return open order payloads."""

    def get_position(self) -> Any:
        """Return broker position payload."""

    def get_account_health(self) -> Any:
        """Return broker account health payload."""

    def snapshot_state(self) -> Any:
        """Return a normalized broker snapshot used by reconciliation and watchdog logic."""

    def fill_order(self, order_intent: OrderIntent, fill_price: Decimal, fill_timestamp: datetime) -> FillEvent:
        """Paper/replay-only deterministic fill hook."""
