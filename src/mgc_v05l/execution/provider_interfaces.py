"""Execution-provider interface kept separate from market-data providers."""

from __future__ import annotations

from typing import Any, Protocol

from .broker_requests import BrokerOrderRequest
from .order_models import OrderIntent


class ExecutionProvider(Protocol):
    provider_id: str

    def snapshot_state(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Return the authoritative broker/account snapshot."""

    def selected_account_id(self, snapshot: dict[str, Any] | None = None) -> str | None:
        """Return the active broker account identifier, if any."""

    def selected_account_hash(self, snapshot: dict[str, Any] | None = None) -> str | None:
        """Compatibility alias for hash-based brokers."""

    def build_order_request(
        self,
        *,
        order_intent: OrderIntent,
        quote_snapshot: Any | None = None,
    ) -> BrokerOrderRequest:
        """Build a broker-neutral order request from strategy intent plus pricing context."""

    def submit_order(self, account_id: str, order_request: BrokerOrderRequest) -> dict[str, Any]:
        """Submit an order through the broker execution channel."""

    def cancel_order(self, account_id: str, broker_order_id: str) -> None:
        """Cancel an existing broker order."""

    def get_order_status(self, account_id: str, broker_order_id: str) -> dict[str, Any]:
        """Fetch normalized authoritative order status from the broker."""
