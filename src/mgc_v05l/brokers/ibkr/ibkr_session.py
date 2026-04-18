"""IBKR session lifecycle scaffolding for TWS / IB Gateway."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from .ibkr_models import IbkrConnectionState
from .ibkr_order_identity import IbkrOrderIdAllocator, IbkrOrderIdPolicy


class IbkrSession:
    """State holder for IBKR transport connectivity and order-id seeding."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        account_id: str | None,
        gateway_mode: str,
        read_only: bool,
        order_id_policy: IbkrOrderIdPolicy,
        order_id_allocator: IbkrOrderIdAllocator | None = None,
    ) -> None:
        self._state = IbkrConnectionState(
            host=host,
            port=int(port),
            client_id=int(client_id),
            account_id=account_id,
            connected=False,
            gateway_mode=gateway_mode,
            read_only=bool(read_only),
        )
        self._order_id_policy = order_id_policy
        self._order_id_allocator = order_id_allocator or IbkrOrderIdAllocator()

    @property
    def state(self) -> IbkrConnectionState:
        return self._state

    @property
    def order_id_policy(self) -> IbkrOrderIdPolicy:
        return self._order_id_policy

    @property
    def order_id_allocator(self) -> IbkrOrderIdAllocator:
        return self._order_id_allocator

    def mark_connected(self, *, managed_accounts: tuple[str, ...] = (), connected_at: datetime | None = None) -> None:
        now = connected_at or datetime.now(timezone.utc)
        self._state = replace(
            self._state,
            connected=True,
            managed_accounts=tuple(managed_accounts),
            connected_at=now,
            last_heartbeat_at=now,
            last_error=None,
        )

    def mark_disconnected(self, *, reason: str | None = None, occurred_at: datetime | None = None) -> None:
        now = occurred_at or datetime.now(timezone.utc)
        self._state = replace(
            self._state,
            connected=False,
            last_heartbeat_at=now,
            last_error=reason,
        )

    def record_heartbeat(self, *, occurred_at: datetime | None = None) -> None:
        now = occurred_at or datetime.now(timezone.utc)
        self._state = replace(self._state, last_heartbeat_at=now)

    def seed_next_valid_order_id(self, next_valid_order_id: int) -> None:
        self._order_id_allocator.seed(next_valid_order_id)
        self._state = replace(self._state, next_valid_order_id=self._order_id_allocator.next_order_id)

    def allocate_order_id(self) -> int:
        allocated = self._order_id_allocator.allocate()
        self._state = replace(self._state, next_valid_order_id=self._order_id_allocator.next_order_id)
        return allocated
