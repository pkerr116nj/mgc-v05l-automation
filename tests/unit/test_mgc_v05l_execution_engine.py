"""Execution-engine tests for pending-order protections."""

from datetime import datetime, timezone

from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.execution_engine import ExecutionEngine
from mgc_v05l.execution.order_models import OrderIntent


def _build_intent(order_intent_id: str, intent_type: OrderIntentType) -> OrderIntent:
    return OrderIntent(
        order_intent_id=order_intent_id,
        bar_id="bar-1",
        symbol="MGC",
        intent_type=intent_type,
        quantity=1,
        created_at=datetime.now(timezone.utc),
        reason_code="test",
    )


def test_execution_engine_blocks_duplicate_order_ids() -> None:
    engine = ExecutionEngine()
    intent = _build_intent("intent-1", OrderIntentType.BUY_TO_OPEN)

    assert engine.register_intent(intent) is True
    assert engine.register_intent(intent) is False


def test_execution_engine_blocks_second_pending_entry() -> None:
    engine = ExecutionEngine()
    first = _build_intent("intent-1", OrderIntentType.BUY_TO_OPEN)
    second = _build_intent("intent-2", OrderIntentType.SELL_TO_OPEN)

    assert engine.register_intent(first) is True
    assert engine.register_intent(second) is False
