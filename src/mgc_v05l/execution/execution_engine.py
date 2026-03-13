"""Execution engine for replay and deterministic paper fills."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ..config_models import StrategySettings
from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, ReplayFillPolicy
from ..domain.models import Bar
from .broker_interface import BrokerInterface
from .order_models import FillEvent, OrderIntent
from .paper_broker import PaperBroker


@dataclass(frozen=True)
class PendingExecution:
    intent: OrderIntent
    broker_order_id: str
    signal_bar_id: Optional[str]
    long_entry_family: LongEntryFamily


class ExecutionEngine:
    """Deduplicates order intents and drives the deterministic paper execution path."""

    def __init__(self, broker: Optional[BrokerInterface] = None) -> None:
        self._broker = broker or PaperBroker()
        self._pending_order_ids: set[str] = set()
        self._pending_intent_types: set[OrderIntentType] = set()
        self._pending_executions: dict[str, PendingExecution] = {}
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
    ) -> Optional[PendingExecution]:
        """Submit an accepted intent to the broker and track it as pending."""
        if not self.register_intent(intent):
            return None
        broker_order_id = self._broker.submit_order(intent)
        pending = PendingExecution(
            intent=intent,
            broker_order_id=broker_order_id,
            signal_bar_id=signal_bar_id,
            long_entry_family=long_entry_family,
        )
        self._pending_executions[intent.order_intent_id] = pending
        return pending

    def pop_due_replay_fills(self, bar: Bar, settings: StrategySettings) -> list[PendingExecution]:
        """Return pending orders that should fill at the current bar open."""
        if settings.replay_fill_policy != ReplayFillPolicy.NEXT_BAR_OPEN:
            raise ValueError("ExecutionEngine only supports NEXT_BAR_OPEN for replay fills.")
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
        self._pending_order_ids.discard(order_intent_id)
        self._pending_intent_types.discard(pending.intent.intent_type)

    def pending_execution(self, order_intent_id: str) -> Optional[PendingExecution]:
        """Return the tracked pending execution for an order intent."""
        return self._pending_executions.get(order_intent_id)

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

    @property
    def broker(self) -> BrokerInterface:
        return self._broker
