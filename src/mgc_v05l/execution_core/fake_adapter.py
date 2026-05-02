"""Deterministic fake broker adapter for Track B harness tests.

This module simulates broker callbacks without importing IBKR/TWS libraries and
without submitting any broker orders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from .models import (
    Action,
    BrokerOrder,
    CancelAttempt,
    FillEvent,
    IntentKind,
    OrderIntent,
    PositionSource,
    PositionState,
    SubmitAttempt,
)
from .pricing import QuoteObservation


@dataclass(frozen=True)
class FakeSubmitResult:
    broker_order: BrokerOrder | None
    fill_event: FillEvent | None
    broker_position: PositionState
    open_orders: tuple[BrokerOrder, ...]
    missing_callbacks: tuple[str, ...] = ()
    ambiguous: bool = False
    failure_reason: str | None = None


class FakePaperAdapter:
    """Small stateful fake that exercises the Track B execution spine."""

    def __init__(
        self,
        *,
        account_id: str = "DU1234567",
        contract_key: str = "MGC-202606",
        symbol: str = "MGC",
        scenario: str = "pass",
        quote_observed_at: datetime | None = None,
        quote_bid: str = "2345.0",
        quote_ask: str = "2345.1",
        quote_last: str | None = "2345.05",
        initial_position_quantity: int = 0,
    ) -> None:
        self.account_id = account_id
        self.contract_key = contract_key
        self.symbol = symbol
        self.scenario = scenario
        self.quote_observed_at = quote_observed_at
        self.quote_bid = quote_bid
        self.quote_ask = quote_ask
        self.quote_last = quote_last
        self.connected = False
        self.position_quantity = initial_position_quantity
        self.open_orders: list[BrokerOrder] = []
        self.submit_count = 0
        self.cancel_count = 0
        self.submit_attempt_ids: list[str] = []

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def managed_accounts(self) -> tuple[str, ...]:
        return (self.account_id,) if self.account_id else ()

    def qualify_contract(self, *, run_id: str, contract_key: str, allowlist_entry: dict[str, Any], now: datetime) -> dict[str, Any]:
        if contract_key != self.contract_key:
            raise ValueError("contract not available in fake adapter")
        return {
            "run_id": run_id,
            "contract_key": contract_key,
            "symbol": allowlist_entry.get("symbol", self.symbol),
            "local_symbol": allowlist_entry.get("local_symbol", contract_key),
            "con_id": allowlist_entry.get("con_id", "fake-con-id"),
            "qualified_at": now.isoformat(),
            "source": "fake_adapter",
        }

    def observe_quote(self, *, run_id: str, now: datetime) -> QuoteObservation:
        return QuoteObservation(
            quote_id=f"quote_{run_id}_{len(self.submit_attempt_ids) + 1}",
            run_id=run_id,
            contract_key=self.contract_key,
            source="fake_adapter",
            bid=self.quote_bid,
            ask=self.quote_ask,
            last=self.quote_last,
            observed_at=self.quote_observed_at or now,
            raw={"scenario": self.scenario},
        )

    def observe_position(self, *, run_id: str, now: datetime, stage: str = "observed") -> PositionState:
        quantity = self.position_quantity
        if self.scenario == "final_reconciliation_mismatch" and stage == "FINAL":
            quantity = 1
        return PositionState(
            position_state_id=f"broker_position_{stage}_{run_id}",
            run_id=run_id,
            source=PositionSource.BROKER,
            account_id=self.account_id,
            contract_key=self.contract_key,
            signed_quantity=quantity,
            average_price=None if quantity == 0 else Decimal("2345.1"),
            open_order_ids=tuple(order.broker_order_id for order in self.open_orders),
            observed_at=now,
            raw={"source": "fake_adapter", "stage": stage, "scenario": self.scenario},
        )

    def observe_open_orders(self) -> tuple[BrokerOrder, ...]:
        return tuple(self.open_orders)

    def submit_order(self, *, submit_attempt: SubmitAttempt, order_intent: OrderIntent, now: datetime) -> FakeSubmitResult:
        self.submit_count += 1
        self.submit_attempt_ids.append(submit_attempt.submit_attempt_id)
        is_open = order_intent.intent_kind == IntentKind.OPEN
        if is_open and self.scenario == "submit_missing_order_truth":
            return FakeSubmitResult(
                broker_order=None,
                fill_event=None,
                broker_position=self.observe_position(run_id=order_intent.run_id, now=now, stage="OPEN_AMBIGUOUS"),
                open_orders=tuple(self.open_orders),
                missing_callbacks=("orderStatus", "openOrder"),
                ambiguous=True,
                failure_reason="submit sent but orderStatus/openOrder callbacks were not observed",
            )

        if is_open and self.scenario == "open_rests_cancel_clean":
            order = self._broker_order(submit_attempt, order_intent, now=now, status="Submitted", filled=0, remaining=1)
            self.open_orders.append(order)
            return FakeSubmitResult(
                broker_order=order,
                fill_event=None,
                broker_position=self.observe_position(run_id=order_intent.run_id, now=now, stage="OPEN_RESTING"),
                open_orders=tuple(self.open_orders),
                failure_reason="open order did not fill before timeout",
            )

        if not is_open and self.scenario == "close_submit_ambiguous":
            return FakeSubmitResult(
                broker_order=None,
                fill_event=None,
                broker_position=self.observe_position(run_id=order_intent.run_id, now=now, stage="CLOSE_AMBIGUOUS"),
                open_orders=tuple(self.open_orders),
                missing_callbacks=("orderStatus", "openOrder"),
                ambiguous=True,
                failure_reason="close submit status ambiguous",
            )

        order = self._broker_order(submit_attempt, order_intent, now=now, status="Filled", filled=1, remaining=0)
        fill = self._fill_event(submit_attempt, order_intent, order, now=now)
        delta = 1 if order_intent.action == Action.BUY else -1
        self.position_quantity += delta
        if is_open and self.scenario == "post_open_reconciliation_mismatch":
            self.position_quantity = 0
        return FakeSubmitResult(
            broker_order=order,
            fill_event=fill,
            broker_position=self.observe_position(
                run_id=order_intent.run_id,
                now=now,
                stage="POST_OPEN" if is_open else "POST_CLOSE",
            ),
            open_orders=tuple(self.open_orders),
        )

    def cancel_order(self, *, run_id: str, submit_attempt: SubmitAttempt, broker_order: BrokerOrder, now: datetime) -> CancelAttempt:
        self.cancel_count += 1
        self.open_orders = [order for order in self.open_orders if order.broker_order_id != broker_order.broker_order_id]
        return CancelAttempt(
            cancel_attempt_id=f"cancel_{run_id}_{self.cancel_count}",
            run_id=run_id,
            submit_attempt_id=submit_attempt.submit_attempt_id,
            broker_order_id=broker_order.broker_order_id,
            perm_id=broker_order.perm_id,
            account_id=broker_order.account_id,
            contract_key=broker_order.contract_key,
            requested_at=now,
            observed_cancel_status="Cancelled",
            confirmed_at=now,
            raw={"source": "fake_adapter", "status": "Cancelled"},
        )

    def _broker_order(
        self,
        submit_attempt: SubmitAttempt,
        order_intent: OrderIntent,
        *,
        now: datetime,
        status: str,
        filled: int,
        remaining: int,
    ) -> BrokerOrder:
        broker_order_id = f"FAKE-{self.submit_count}"
        return BrokerOrder(
            broker_order_event_id=f"broker_order_{submit_attempt.submit_attempt_id}",
            run_id=order_intent.run_id,
            submit_attempt_id=submit_attempt.submit_attempt_id,
            account_id=order_intent.account_id,
            broker_order_id=broker_order_id,
            perm_id=f"PERM-{self.submit_count}",
            client_id=int(submit_attempt.environment.get("client_id", 0)),
            contract_key=order_intent.contract_key,
            action=order_intent.action,
            quantity=order_intent.quantity,
            order_type=order_intent.order_type,
            limit_price=order_intent.limit_price,
            status=status,
            filled_quantity=filled,
            remaining_quantity=remaining,
            average_fill_price=order_intent.limit_price if filled else None,
            observed_at=now,
            raw={"source": "fake_adapter"},
        )

    def _fill_event(
        self,
        submit_attempt: SubmitAttempt,
        order_intent: OrderIntent,
        broker_order: BrokerOrder,
        *,
        now: datetime,
    ) -> FillEvent:
        return FillEvent(
            fill_event_id=f"fill_{submit_attempt.submit_attempt_id}",
            run_id=order_intent.run_id,
            submit_attempt_id=submit_attempt.submit_attempt_id,
            order_intent_id=order_intent.order_intent_id,
            account_id=order_intent.account_id,
            broker_order_id=broker_order.broker_order_id,
            perm_id=broker_order.perm_id,
            execution_id=f"EXEC-{self.submit_count}",
            contract_key=order_intent.contract_key,
            action=order_intent.action,
            quantity=order_intent.quantity,
            price=order_intent.limit_price,
            filled_at=now,
            raw={"source": "fake_adapter"},
        )
