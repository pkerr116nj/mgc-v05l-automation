"""Execution engine for replay and deterministic paper fills."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Optional

from ..config_models import StrategySettings
from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, ReplayFillPolicy, ShortEntryFamily
from ..domain.models import Bar
from .broker_interface import BrokerInterface
from .order_models import FillEvent, OrderIntent
from .paper_broker import PaperBroker


@dataclass(frozen=True)
class PendingExecution:
    intent: OrderIntent
    broker_order_id: str
    submitted_at: datetime
    acknowledged_at: Optional[datetime]
    broker_order_status: Optional[str]
    last_status_checked_at: Optional[datetime]
    retry_count: int
    signal_bar_id: Optional[str]
    long_entry_family: LongEntryFamily
    short_entry_family: ShortEntryFamily
    short_entry_source: Optional[str]


@dataclass(frozen=True)
class SubmitFailure:
    order_intent_id: str
    bar_id: str
    symbol: str
    intent_type: str
    submit_attempted_at: datetime
    failure_stage: str
    error: str


class ExecutionEngine:
    """Deduplicates order intents and drives the deterministic paper execution path."""

    def __init__(self, broker: Optional[BrokerInterface] = None) -> None:
        self._broker = broker or PaperBroker()
        self._pending_order_ids: set[str] = set()
        self._pending_intent_types: set[OrderIntentType] = set()
        self._pending_executions: dict[str, PendingExecution] = {}
        self._last_submit_attempt: dict[str, object] | None = None
        self._last_submit_failure: SubmitFailure | None = None
        if not self._broker.is_connected():
            self._broker.connect()

    def register_intent(self, intent: OrderIntent) -> bool:
        """Return whether the order intent is new and accepted for submission."""
        if intent.order_intent_id in self._pending_order_ids:
            return False
        if intent.is_entry and self._has_pending_entry():
            return False
        if intent.is_exit and self._has_pending_exit():
            return False
        if self._would_create_opposite_side_conflict(intent):
            return False
        self._pending_order_ids.add(intent.order_intent_id)
        self._pending_intent_types.add(intent.intent_type)
        return True

    def submit_intent(
        self,
        intent: OrderIntent,
        signal_bar_id: Optional[str] = None,
        long_entry_family: LongEntryFamily = LongEntryFamily.NONE,
        short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE,
        short_entry_source: Optional[str] = None,
    ) -> Optional[PendingExecution]:
        """Submit an accepted intent to the broker and track it as pending."""
        if not self.register_intent(intent):
            return None
        self._last_submit_failure = None
        self._last_submit_attempt = {
            "order_intent_id": intent.order_intent_id,
            "bar_id": intent.bar_id,
            "symbol": intent.symbol,
            "intent_type": intent.intent_type.value,
            "submit_attempted_at": intent.created_at.isoformat(),
        }
        broker_order_id: str | None = None
        try:
            broker_order_id = self._broker.submit_order(intent)
        except Exception as exc:
            self._clear_registration(intent.order_intent_id, intent.intent_type)
            self._last_submit_failure = SubmitFailure(
                order_intent_id=intent.order_intent_id,
                bar_id=intent.bar_id,
                symbol=intent.symbol,
                intent_type=intent.intent_type.value,
                submit_attempted_at=intent.created_at,
                failure_stage="broker_submit",
                error=str(exc),
            )
            return None
        try:
            initial_status_payload = self._broker.get_order_status(broker_order_id)
        except Exception as exc:
            self._clear_registration(intent.order_intent_id, intent.intent_type)
            self._last_submit_failure = SubmitFailure(
                order_intent_id=intent.order_intent_id,
                bar_id=intent.bar_id,
                symbol=intent.symbol,
                intent_type=intent.intent_type.value,
                submit_attempted_at=intent.created_at,
                failure_stage="broker_status",
                error=str(exc),
            )
            return None
        initial_status = str((initial_status_payload or {}).get("status") or "").strip().upper() or None
        acknowledged_at = intent.created_at if _status_confirms_acknowledgement(initial_status) else None
        self._last_submit_attempt = {
            **dict(self._last_submit_attempt or {}),
            "broker_order_id": broker_order_id,
            "initial_broker_order_status": initial_status,
            "broker_ack_at": acknowledged_at.isoformat() if acknowledged_at is not None else None,
        }
        pending = PendingExecution(
            intent=intent,
            broker_order_id=broker_order_id,
            submitted_at=intent.created_at,
            acknowledged_at=acknowledged_at,
            broker_order_status=initial_status,
            last_status_checked_at=intent.created_at,
            retry_count=0,
            signal_bar_id=signal_bar_id,
            long_entry_family=long_entry_family,
            short_entry_family=short_entry_family,
            short_entry_source=short_entry_source,
        )
        self._pending_executions[intent.order_intent_id] = pending
        return pending

    def pop_due_replay_fills(self, bar: Bar, settings: StrategySettings) -> list[PendingExecution]:
        """Return pending orders due under the baseline-parity next-bar-open replay-fill helper."""
        if settings.replay_fill_policy != ReplayFillPolicy.NEXT_BAR_OPEN:
            raise ValueError(
                "ExecutionEngine baseline-parity replay-fill helper only supports replay_fill_policy=NEXT_BAR_OPEN."
            )
        return [
            pending
            for pending in self._pending_executions.values()
            if pending.intent.bar_id != bar.bar_id
        ]

    def materialize_replay_fill(self, pending: PendingExecution, bar: Bar) -> FillEvent:
        """Fill a pending replay order at the next bar open."""
        fill = self._broker.fill_order(
            order_intent=pending.intent,
            fill_price=bar.open,
            fill_timestamp=bar.start_ts,
        )
        self.clear_intent(pending.intent.order_intent_id)
        return fill

    def clear_intent(self, order_intent_id: str) -> None:
        """Clear a resolved pending intent by identifier."""
        pending = self._pending_executions.pop(order_intent_id, None)
        if pending is None:
            return
        self._clear_registration(order_intent_id, pending.intent.intent_type)

    def pending_execution(self, order_intent_id: str) -> Optional[PendingExecution]:
        """Return the tracked pending execution for an order intent."""
        return self._pending_executions.get(order_intent_id)

    def restore_pending_execution(self, pending: PendingExecution) -> None:
        """Restore a previously persisted pending execution after restart."""
        self._pending_order_ids.add(pending.intent.order_intent_id)
        self._pending_intent_types.add(pending.intent.intent_type)
        self._pending_executions[pending.intent.order_intent_id] = pending

    def pending_executions(self) -> list[PendingExecution]:
        """Return all currently tracked pending executions."""
        return list(self._pending_executions.values())

    def last_submit_attempt(self) -> dict[str, object] | None:
        """Return the latest broker submit-attempt metadata."""
        return dict(self._last_submit_attempt) if self._last_submit_attempt is not None else None

    def last_submit_failure(self) -> SubmitFailure | None:
        """Return the latest broker submit failure, if any."""
        return self._last_submit_failure

    def observe_pending_status(
        self,
        order_intent_id: str,
        *,
        broker_order_status: str | None,
        observed_at: datetime,
        acknowledged: bool | None = None,
        retry_count: int | None = None,
    ) -> Optional[PendingExecution]:
        """Update a tracked pending execution with observed broker lifecycle state."""
        pending = self._pending_executions.get(order_intent_id)
        if pending is None:
            return None
        ack_at = pending.acknowledged_at
        if acknowledged is True and ack_at is None:
            ack_at = observed_at
        updated = replace(
            pending,
            acknowledged_at=ack_at,
            broker_order_status=broker_order_status if broker_order_status is not None else pending.broker_order_status,
            last_status_checked_at=observed_at,
            retry_count=pending.retry_count if retry_count is None else int(retry_count),
        )
        self._pending_executions[order_intent_id] = updated
        return updated

    def _has_pending_entry(self) -> bool:
        return any(
            intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN)
            for intent_type in self._pending_intent_types
        )

    def _has_pending_exit(self) -> bool:
        return any(
            intent_type in (OrderIntentType.SELL_TO_CLOSE, OrderIntentType.BUY_TO_CLOSE)
            for intent_type in self._pending_intent_types
        )

    def _would_create_opposite_side_conflict(self, intent: OrderIntent) -> bool:
        if intent.intent_type == OrderIntentType.BUY_TO_OPEN and OrderIntentType.SELL_TO_OPEN in self._pending_intent_types:
            return True
        if intent.intent_type == OrderIntentType.SELL_TO_OPEN and OrderIntentType.BUY_TO_OPEN in self._pending_intent_types:
            return True
        if intent.intent_type == OrderIntentType.SELL_TO_CLOSE and OrderIntentType.BUY_TO_CLOSE in self._pending_intent_types:
            return True
        if intent.intent_type == OrderIntentType.BUY_TO_CLOSE and OrderIntentType.SELL_TO_CLOSE in self._pending_intent_types:
            return True
        return False

    def _clear_registration(self, order_intent_id: str, intent_type: OrderIntentType) -> None:
        self._pending_order_ids.discard(order_intent_id)
        self._pending_intent_types.discard(intent_type)

    @property
    def broker(self) -> BrokerInterface:
        return self._broker


def _status_confirms_acknowledgement(status: str | None) -> bool:
    normalized = str(status or "").strip().upper()
    return normalized in {
        OrderStatus.ACKNOWLEDGED.value,
        OrderStatus.FILLED.value,
        "WORKING",
        "OPEN",
        "NEW",
        "QUEUED",
        "ACCEPTED",
        "PENDING_ACTIVATION",
        "PARTIALLY_FILLED",
    }
