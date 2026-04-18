from __future__ import annotations

import pytest

from mgc_v05l.brokers.ibkr import (
    IbkrOrderIdAllocator,
    build_default_ibkr_order_id_policy,
)


def test_ibkr_order_id_allocator_requires_seed_before_allocate() -> None:
    allocator = IbkrOrderIdAllocator()

    with pytest.raises(RuntimeError, match="seeded from nextValidId"):
        allocator.allocate()


def test_ibkr_order_id_allocator_advances_monotonically() -> None:
    allocator = IbkrOrderIdAllocator()
    allocator.seed(7001)

    assert allocator.allocate() == 7001
    assert allocator.allocate() == 7002
    assert allocator.next_order_id == 7003


def test_default_ibkr_order_policy_uses_selected_account_client_scope() -> None:
    policy = build_default_ibkr_order_id_policy(client_id=7, live_orders_enabled=False)

    assert policy.profile.client_id == 7
    assert policy.profile.owns_submitted_orders is False
    assert policy.completed_orders_scope == "selected_account_and_client_id"
    assert policy.executions_scope == "selected_account_and_client_id"
