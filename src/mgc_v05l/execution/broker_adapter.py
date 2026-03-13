"""Broker adapter contract."""

from collections.abc import Sequence
from typing import Any, Protocol

from ..domain.models import Bar
from .order_models import OrderIntent


class BrokerAdapter(Protocol):
    """Required broker adapter methods from the concrete technical design."""

    def connect(self) -> None:
        """Connect to the broker API."""

    def disconnect(self) -> None:
        """Disconnect from the broker API."""

    def is_connected(self) -> bool:
        """Return broker connectivity status."""

    def get_latest_bars(self) -> Sequence[Bar]:
        """Return the latest finalized bars."""

    def subscribe_bars(self) -> None:
        """Subscribe to broker bar data."""

    def submit_order(self, order_intent: OrderIntent) -> str:
        """Submit an order and return the broker order identifier."""

    def cancel_order(self, broker_order_id: str) -> None:
        """Cancel a broker order."""

    def get_order_status(self, broker_order_id: str) -> Any:
        """Return the broker order status payload."""

    def get_open_orders(self) -> Sequence[Any]:
        """Return broker open-order payloads."""

    def get_position(self) -> Any:
        """Return the broker-observed position payload."""

    def get_account_health(self) -> Any:
        """Return broker or account health payload."""
