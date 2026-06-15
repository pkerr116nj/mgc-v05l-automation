"""Execution-engine tests for pending-order protections."""

from datetime import datetime, timezone
from decimal import Decimal

from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.execution_engine import ExecutionEngine
from mgc_v05l.execution.execution_engine import PendingExecution
from mgc_v05l.execution.order_models import FillEvent
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
        signal_id=f"{order_intent_id}|signal",
    )


class _FailingBroker:
    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def is_connected(self) -> bool:
        return True

    def submit_order(self, order_intent: OrderIntent) -> str:
        raise RuntimeError("synthetic submit failure")

    def cancel_order(self, broker_order_id: str) -> None:
        return None

    def get_order_status(self, broker_order_id: str):
        return {"status": "ACKNOWLEDGED"}

    def get_open_orders(self):
        return []

    def get_position(self):
        return {"quantity": 0}

    def get_account_health(self):
        return {"healthy": True}

    def snapshot_state(self):
        return {}

    def fill_order(self, order_intent: OrderIntent, fill_price: Decimal, fill_timestamp: datetime) -> FillEvent:
        raise NotImplementedError


class _ReconcilingBroker:
    def __init__(self, *, clear_pending: bool) -> None:
        self.clear_pending = clear_pending
        self.reconcile_calls: list[dict[str, object]] = []

    def connect(self) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def is_connected(self) -> bool:
        return True

    def submit_order(self, order_intent: OrderIntent) -> str:
        return f"broker-{order_intent.order_intent_id}"

    def cancel_order(self, broker_order_id: str) -> None:
        return None

    def get_order_status(self, broker_order_id: str):
        return {"status": "ACKNOWLEDGED"}

    def get_open_orders(self):
        return []

    def get_position(self):
        return {"quantity": 0}

    def get_account_health(self):
        return {"healthy": True}

    def snapshot_state(self):
        return {}

    def fill_order(self, order_intent: OrderIntent, fill_price: Decimal, fill_timestamp: datetime) -> FillEvent:
        raise NotImplementedError

    def reconcile_stale_pending_state_for_intent(
        self,
        *,
        intent: OrderIntent,
        pending_executions: list[PendingExecution],
    ) -> dict[str, object]:
        self.reconcile_calls.append(
            {
                "order_intent_id": intent.order_intent_id,
                "pending_count": len(pending_executions),
            }
        )
        if not self.clear_pending:
            return {"clear_pending_intent_ids": []}
        return {
            "clear_pending_intent_ids": [
                pending.intent.order_intent_id
                for pending in pending_executions
            ]
        }


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


def test_execution_engine_clears_stale_pending_entry_when_broker_truth_allows() -> None:
    broker = _ReconcilingBroker(clear_pending=True)
    engine = ExecutionEngine(broker=broker)
    first = _build_intent("intent-1", OrderIntentType.SELL_TO_OPEN)
    second = _build_intent("intent-2", OrderIntentType.BUY_TO_OPEN)

    assert engine.submit_intent(first) is not None
    assert engine.register_intent(second) is True

    assert engine.pending_execution("intent-1") is None
    assert engine.pending_execution("intent-2") is None
    assert broker.reconcile_calls[-1] == {"order_intent_id": "intent-2", "pending_count": 1}


def test_execution_engine_keeps_pending_entry_when_broker_truth_does_not_clear() -> None:
    broker = _ReconcilingBroker(clear_pending=False)
    engine = ExecutionEngine(broker=broker)
    first = _build_intent("intent-1", OrderIntentType.SELL_TO_OPEN)
    second = _build_intent("intent-2", OrderIntentType.BUY_TO_OPEN)

    assert engine.submit_intent(first) is not None
    assert engine.register_intent(second) is False

    assert engine.pending_execution("intent-1") is not None
    assert engine.pending_execution("intent-2") is None
    assert broker.reconcile_calls[-1] == {"order_intent_id": "intent-2", "pending_count": 1}


def test_execution_engine_records_signal_and_submit_attempt_identity_on_submit_failure() -> None:
    engine = ExecutionEngine(broker=_FailingBroker())
    intent = _build_intent("intent-1", OrderIntentType.BUY_TO_OPEN)

    pending = engine.submit_intent(intent)

    assert pending is None
    attempt = engine.last_submit_attempt()
    failure = engine.last_submit_failure()
    assert attempt is not None
    assert attempt["signal_id"] == "intent-1|signal"
    assert isinstance(attempt["submit_attempt_id"], str)
    assert failure is not None
    assert failure.order_intent_id == "intent-1"
    assert failure.submit_attempt_id == attempt["submit_attempt_id"]
