from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from mgc_v05l.execution_core.models import (
    BrokerOrder,
    FillEvent,
    PositionSource,
    PositionState,
    ReconciliationStage,
    ReconciliationStatus,
)
from mgc_v05l.execution_core.reconcile import (
    ACCOUNT_MISMATCH,
    AMBIGUOUS_BROKER_STATUS,
    BROKER_UNAVAILABLE,
    MISSING_FILL,
    OPEN_ORDER_MISMATCH,
    reconcile_position,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def position(source: PositionSource, quantity: int, *, account_id: str = "DUM123") -> PositionState:
    return PositionState(
        position_state_id=f"{source.value.lower()}-{quantity}",
        run_id="run-1",
        source=source,
        account_id=account_id,
        contract_key="MGC-202606",
        signed_quantity=quantity,
        average_price=None if quantity == 0 else Decimal("2345.1"),
        open_order_ids=(),
        observed_at=aware_now(),
    )


def fill(fill_id: str, action: str) -> FillEvent:
    return FillEvent(
        fill_event_id=fill_id,
        run_id="run-1",
        submit_attempt_id=f"submit-{fill_id}",
        order_intent_id=f"intent-{fill_id}",
        account_id="DUM123",
        broker_order_id=f"order-{fill_id}",
        perm_id=None,
        execution_id=f"exec-{fill_id}",
        contract_key="MGC-202606",
        action=action,
        quantity=1,
        price="2345.1",
        filled_at=aware_now(),
    )


def test_pre_open_reconciliation_clean_when_broker_and_ledger_are_flat() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.PRE_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 0),
        ledger_position=position(PositionSource.LEDGER, 0),
    )

    assert result.status == ReconciliationStatus.CLEAN
    assert result.issues == ()


def test_post_open_reconciliation_clean_with_matching_fill_and_position() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.POST_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 1),
        ledger_position=position(PositionSource.LEDGER, 1),
        fill_events=(fill("open", "BUY"),),
    )

    assert result.status == ReconciliationStatus.CLEAN
    assert result.expected_signed_quantity == 1


def test_final_reconciliation_clean_when_flat_after_two_fills() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.FINAL,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 0),
        ledger_position=position(PositionSource.LEDGER, 0),
        fill_events=(fill("open", "BUY"), fill("close", "SELL")),
    )

    assert result.status == ReconciliationStatus.CLEAN


def test_pre_submit_broker_unavailable_blocks() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.PRE_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=None,
        ledger_position=position(PositionSource.LEDGER, 0),
        broker_connected=False,
    )

    assert result.status == ReconciliationStatus.BLOCKED
    assert BROKER_UNAVAILABLE in result.issues


def test_post_submit_missing_fill_is_ambiguous() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.POST_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 1),
        ledger_position=position(PositionSource.LEDGER, 1),
        fill_events=(),
    )

    assert result.status == ReconciliationStatus.AMBIGUOUS
    assert MISSING_FILL in result.issues


def test_account_mismatch_is_reported() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.PRE_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 0, account_id="OTHER"),
        ledger_position=position(PositionSource.LEDGER, 0),
    )

    assert result.status == ReconciliationStatus.BLOCKED
    assert ACCOUNT_MISMATCH in result.issues


def test_open_order_mismatch_blocks_pre_open() -> None:
    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.PRE_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 0),
        ledger_position=position(PositionSource.LEDGER, 0),
        broker_open_orders=("1001",),
    )

    assert result.status == ReconciliationStatus.BLOCKED
    assert OPEN_ORDER_MISMATCH in result.issues


def test_ambiguous_broker_order_status_is_ambiguous() -> None:
    order = BrokerOrder(
        broker_order_event_id="bo-1",
        run_id="run-1",
        submit_attempt_id="submit-1",
        account_id="DUM123",
        broker_order_id="1001",
        perm_id=None,
        client_id=7,
        contract_key="MGC-202606",
        action="BUY",
        quantity=1,
        order_type="LMT",
        limit_price="2345.1",
        status="UNKNOWN",
        filled_quantity=0,
        remaining_quantity=1,
        average_fill_price=None,
        observed_at=aware_now(),
    )

    result = reconcile_position(
        run_id="run-1",
        stage=ReconciliationStage.POST_OPEN,
        account_id="DUM123",
        contract_key="MGC-202606",
        broker_position=position(PositionSource.BROKER, 1),
        ledger_position=position(PositionSource.LEDGER, 1),
        broker_open_orders=(order,),
        fill_events=(fill("open", "BUY"),),
    )

    assert result.status == ReconciliationStatus.AMBIGUOUS
    assert AMBIGUOUS_BROKER_STATUS in result.issues

