"""IBKR client-id and order-id policy scaffolding."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IbkrClientIdProfile:
    client_id: int
    label: str
    owns_submitted_orders: bool
    sees_all_open_orders: bool
    binds_manual_tws_orders: bool = False


@dataclass(frozen=True)
class IbkrOrderIdPolicy:
    profile: IbkrClientIdProfile
    manual_tws_binding_enabled: bool
    completed_orders_scope: str
    executions_scope: str


class IbkrOrderIdAllocator:
    """Monotonic allocator seeded from IBKR nextValidId."""

    def __init__(self, *, starting_order_id: int | None = None) -> None:
        self._next_order_id = starting_order_id

    @property
    def next_order_id(self) -> int | None:
        return self._next_order_id

    def seed(self, next_valid_order_id: int) -> None:
        candidate = int(next_valid_order_id)
        if candidate <= 0:
            raise ValueError("next_valid_order_id must be positive.")
        if self._next_order_id is None or candidate > self._next_order_id:
            self._next_order_id = candidate

    def allocate(self) -> int:
        if self._next_order_id is None:
            raise RuntimeError("IBKR order ids must be seeded from nextValidId before allocation.")
        allocated = self._next_order_id
        self._next_order_id += 1
        return allocated


def build_default_ibkr_order_id_policy(*, client_id: int, live_orders_enabled: bool) -> IbkrOrderIdPolicy:
    profile = IbkrClientIdProfile(
        client_id=client_id,
        label="ibkr_app_primary",
        owns_submitted_orders=bool(live_orders_enabled),
        sees_all_open_orders=False,
        binds_manual_tws_orders=False,
    )
    return IbkrOrderIdPolicy(
        profile=profile,
        manual_tws_binding_enabled=False,
        completed_orders_scope="selected_account_and_client_id",
        executions_scope="selected_account_and_client_id",
    )
