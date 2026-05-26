from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mgc_v05l.execution_core.models import (
    Action,
    BrokerOrder,
    BrokerOrderLifecycleStatus,
    FillEvent,
    IntentKind,
    OrderIntent,
    SignalEvent,
    TrackBModelError,
    broker_order_blocks_same_account_contract_submit,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def test_signal_event_serializes_json_safe_values() -> None:
    event = SignalEvent(
        signal_event_id="sig-1",
        run_id="run-1",
        source_event_id="operator-1",
        bar_id="bar-1",
        strategy_id="TRACK_B_PROOF",
        symbol="mgc",
        contract_key="MGC-202606",
        decision="TRACK_B_PAPER_PROOF_OPEN_LONG",
        side="BUY",
        quantity=1,
        reason="proof",
        occurred_at=aware_now(),
        input_digest="abc123",
    )

    payload = event.to_json_dict()

    assert payload["side"] == "BUY"
    assert payload["quantity"] == "1"
    assert payload["occurred_at"] == "2026-05-02T12:00:00+00:00"
    assert payload["symbol"] == "MGC"


def test_order_intent_rejects_naive_datetime() -> None:
    with pytest.raises(TrackBModelError, match="timezone-aware"):
        OrderIntent(
            order_intent_id="intent-1",
            signal_event_id="sig-1",
            run_id="run-1",
            intent_kind=IntentKind.OPEN,
            account_id="DUM123",
            symbol="MGC",
            contract_key="MGC-202606",
            action=Action.BUY,
            quantity=1,
            order_type="LMT",
            limit_price=Decimal("2345.1"),
            time_in_force="DAY",
            paper_only=True,
            created_at=datetime(2026, 5, 2, 12, 0),
            reason="proof",
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("quantity", 2, "quantity must be exactly 1"),
        ("order_type", "MKT", "order_type must be LMT"),
        ("time_in_force", "GTC", "time_in_force must be DAY"),
        ("paper_only", False, "paper_only must be true"),
    ],
)
def test_order_intent_rejects_invalid_milestone_one_values(field: str, value: object, message: str) -> None:
    kwargs = {
        "order_intent_id": "intent-1",
        "signal_event_id": "sig-1",
        "run_id": "run-1",
        "intent_kind": IntentKind.OPEN,
        "account_id": "DUM123",
        "symbol": "MGC",
        "contract_key": "MGC-202606",
        "action": Action.BUY,
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": Decimal("2345.1"),
        "time_in_force": "DAY",
        "paper_only": True,
        "created_at": aware_now(),
        "reason": "proof",
    }
    kwargs[field] = value

    with pytest.raises(TrackBModelError, match=message):
        OrderIntent(**kwargs)


def test_close_order_intent_allows_aggregate_managed_exit_quantity() -> None:
    intent = OrderIntent(
        order_intent_id="intent-close-3",
        signal_event_id="sig-1",
        run_id="run-1",
        intent_kind=IntentKind.CLOSE,
        account_id="DUM123",
        symbol="MNQ",
        contract_key="MNQ-202606",
        action=Action.BUY,
        quantity=3,
        order_type="LMT",
        limit_price=Decimal("29919.25"),
        time_in_force="DAY",
        paper_only=True,
        created_at=aware_now(),
        reason="aggregate managed exit",
    )

    assert intent.quantity == Decimal("3")


def test_close_order_intent_rejects_fractional_quantity() -> None:
    with pytest.raises(TrackBModelError, match="positive whole-number"):
        OrderIntent(
            order_intent_id="intent-close-fractional",
            signal_event_id="sig-1",
            run_id="run-1",
            intent_kind=IntentKind.CLOSE,
            account_id="DUM123",
            symbol="MNQ",
            contract_key="MNQ-202606",
            action=Action.BUY,
            quantity=Decimal("1.5"),
            order_type="LMT",
            limit_price=Decimal("29919.25"),
            time_in_force="DAY",
            paper_only=True,
            created_at=aware_now(),
            reason="aggregate managed exit",
        )


def test_fill_event_requires_explicit_ids_and_quantity_one() -> None:
    with pytest.raises(TrackBModelError, match="execution_id is required"):
        FillEvent(
            fill_event_id="fill-1",
            run_id="run-1",
            submit_attempt_id="submit-1",
            order_intent_id="intent-1",
            account_id="DUM123",
            broker_order_id="1001",
            perm_id=None,
            execution_id="",
            contract_key="MGC-202606",
            action="BUY",
            quantity=1,
            price="2345.2",
            filled_at=aware_now(),
        )


def broker_order(*, status: str, remaining_quantity: str = "1") -> BrokerOrder:
    return BrokerOrder(
        broker_order_event_id=f"order-{status}",
        run_id="run-1",
        submit_attempt_id="submit-1",
        account_id="DUM123",
        broker_order_id="1",
        perm_id="736787312",
        client_id=77,
        contract_key="MGC-202606",
        action="BUY",
        quantity=1,
        order_type="LMT",
        limit_price="4626.0",
        status=status,
        filled_quantity="0",
        remaining_quantity=remaining_quantity,
        average_fill_price=None,
        observed_at=aware_now(),
    )


def test_pending_cancel_with_remaining_quantity_is_unresolved_broker_state() -> None:
    order = broker_order(status="PendingCancel", remaining_quantity="1")

    assert order.lifecycle_status == BrokerOrderLifecycleStatus.PENDING_CANCEL
    assert order.blocks_same_account_contract_submit is True
    assert broker_order_blocks_same_account_contract_submit(order) is True


def test_terminal_cancelled_order_does_not_block_future_submit_after_clean_preflight() -> None:
    order = broker_order(status="Cancelled", remaining_quantity="0")

    assert order.lifecycle_status == BrokerOrderLifecycleStatus.CANCELLED
    assert order.blocks_same_account_contract_submit is False
