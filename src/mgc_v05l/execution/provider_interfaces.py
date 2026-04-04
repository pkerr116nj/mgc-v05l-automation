"""Execution-provider interface kept separate from market-data providers."""

from __future__ import annotations

from typing import Any, Protocol


class ExecutionProvider(Protocol):
    provider_id: str

    def snapshot_state(self, *, force_refresh: bool = False) -> dict[str, Any]:
        """Return the authoritative broker/account snapshot."""

    def selected_account_hash(self, snapshot: dict[str, Any] | None = None) -> str | None:
        """Return the active broker account hash, if any."""

    def submit_order(self, account_hash: str, order_payload: dict[str, Any]) -> dict[str, Any]:
        """Submit an order through the broker execution channel."""

    def cancel_order(self, account_hash: str, broker_order_id: str) -> None:
        """Cancel an existing broker order."""

    def get_order_status(self, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        """Fetch authoritative order status from the broker."""
