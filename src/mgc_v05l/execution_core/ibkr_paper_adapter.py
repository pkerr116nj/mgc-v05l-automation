"""Track B IBKR paper adapter boundary.

This module defines the paper-only IBKR boundary without importing the legacy
Track A execution stack. Optional IBKR transport imports are lazy so unit tests
can exercise callback normalization without TWS or ibapi installed.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping

from .models import (
    Action,
    BrokerOrder,
    FillEvent,
    OrderIntent,
    PositionSource,
    PositionState,
    SubmitAttempt,
    normalize_action,
    normalize_decimal,
    require_aware_datetime,
    require_milestone_quantity,
)
from .pricing import QuoteObservation


class IbkrPaperAdapterError(RuntimeError):
    """Base error for the Track B IBKR paper adapter boundary."""


class IbkrPaperConfigError(IbkrPaperAdapterError):
    """Raised when adapter construction violates paper-only invariants."""


class IbkrPaperReadinessError(IbkrPaperAdapterError):
    """Raised when TWS paper readiness evidence is incomplete."""


class IbkrPaperCorrelationError(IbkrPaperAdapterError):
    """Raised when broker callbacks do not correlate to a Track B submit."""


class IbkrPaperSubmitDisabledError(IbkrPaperAdapterError):
    """Raised when submit is requested while submit_enabled is false."""


@dataclass(frozen=True)
class SubmitContext:
    submit_attempt: SubmitAttempt
    order_intent: OrderIntent
    created_at: datetime


class IbkrPaperAdapter:
    """Paper-only adapter boundary with explicit callback correlation."""

    def __init__(
        self,
        *,
        mode: str,
        host: str,
        port: int,
        client_id: int,
        account_id: str,
        contract_allowlist: Mapping[str, Mapping[str, Any]],
        submit_enabled: bool = False,
    ) -> None:
        self.mode = str(mode or "").strip().upper()
        self.host = str(host or "").strip()
        self.port = int(port)
        self.client_id = int(client_id)
        self.account_id = str(account_id or "").strip()
        self.contract_allowlist = {str(key): dict(value) for key, value in contract_allowlist.items()}
        self.submit_enabled = bool(submit_enabled)
        self._validate_config()

        self._managed_accounts: tuple[str, ...] = ()
        self.next_valid_id: int | None = None
        self._submit_contexts: dict[str, SubmitContext] = {}
        self._broker_orders: dict[str, BrokerOrder] = {}
        self._positions: dict[str, PositionState] = {}
        self._seen_execution_ids: set[str] = set()
        self.ambiguous_contexts: dict[str, str] = {}
        self.missing_callbacks: set[str] = set()

    def record_managed_accounts(self, accounts: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
        if isinstance(accounts, str):
            rows = tuple(account.strip() for account in accounts.split(",") if account.strip())
        else:
            rows = tuple(str(account or "").strip() for account in accounts if str(account or "").strip())
        self._managed_accounts = rows
        return rows

    def require_account(self, account_id: str | None = None) -> str:
        requested = str(account_id or "").strip()
        if not requested:
            raise IbkrPaperReadinessError("implicit first-account selection is forbidden")
        if requested != self.account_id:
            raise IbkrPaperReadinessError("requested account must match configured paper account")
        if requested not in self._managed_accounts:
            raise IbkrPaperReadinessError("configured paper account is not present in managed accounts")
        return requested

    def require_configured_account(self) -> str:
        return self.require_account(self.account_id)

    def record_next_valid_id(self, order_id: int | str | None) -> int:
        if order_id is None or int(order_id) <= 0:
            raise IbkrPaperReadinessError("nextValidId must be a positive integer")
        self.next_valid_id = int(order_id)
        return self.next_valid_id

    def readiness_report(self) -> dict[str, Any]:
        account_ready = self.account_id in self._managed_accounts
        return {
            "mode": self.mode,
            "host": self.host,
            "port": self.port,
            "client_id": self.client_id,
            "account_id": self.account_id,
            "managed_accounts": self._managed_accounts,
            "account_ready": account_ready,
            "next_valid_id": self.next_valid_id,
            "next_valid_id_ready": self.next_valid_id is not None,
            "ready": account_ready and self.next_valid_id is not None,
            "missing_callbacks": tuple(sorted(self.missing_callbacks)),
        }

    def require_ready(self) -> None:
        self.require_configured_account()
        if self.next_valid_id is None:
            self.missing_callbacks.add("nextValidId")
            raise IbkrPaperReadinessError("missing nextValidId callback")

    def qualify_contract(self, *, run_id: str, contract_key: str, now: datetime) -> dict[str, Any]:
        require_aware_datetime(now, "now")
        entry = self.contract_allowlist.get(contract_key)
        if entry is None:
            raise IbkrPaperConfigError("contract_key must be explicitly allowlisted")
        return {
            "run_id": run_id,
            "contract_key": contract_key,
            "symbol": entry.get("symbol"),
            "security_type": entry.get("security_type"),
            "exchange": entry.get("exchange"),
            "currency": entry.get("currency"),
            "local_symbol": entry.get("local_symbol"),
            "con_id": entry.get("con_id"),
            "tick_size": entry.get("tick_size"),
            "qualified_at": now.isoformat(),
            "source": "ibkr_paper_adapter",
        }

    def observe_quote(
        self,
        *,
        run_id: str,
        contract_key: str,
        bid: Decimal | int | float | str | None,
        ask: Decimal | int | float | str | None,
        last: Decimal | int | float | str | None,
        observed_at: datetime,
        raw: Mapping[str, Any] | None = None,
    ) -> QuoteObservation:
        self._require_allowlisted_contract(contract_key)
        return QuoteObservation(
            quote_id=f"ibkr_quote_{run_id}_{contract_key}_{int(observed_at.timestamp())}",
            run_id=run_id,
            contract_key=contract_key,
            source="ibkr_paper_adapter",
            bid=bid,
            ask=ask,
            last=last,
            observed_at=observed_at,
            raw=dict(raw or {}),
        )

    def register_submit_context(
        self,
        *,
        submit_attempt: SubmitAttempt,
        order_intent: OrderIntent,
        created_at: datetime,
    ) -> SubmitContext:
        require_aware_datetime(created_at, "created_at")
        if submit_attempt.run_id != order_intent.run_id:
            raise IbkrPaperCorrelationError("submit_attempt and order_intent run_id mismatch")
        if submit_attempt.account_id != self.account_id or order_intent.account_id != self.account_id:
            raise IbkrPaperCorrelationError("submit context account must match configured paper account")
        self._require_allowlisted_contract(order_intent.contract_key)
        context = SubmitContext(submit_attempt=submit_attempt, order_intent=order_intent, created_at=created_at)
        self._submit_contexts[submit_attempt.submit_attempt_id] = context
        return context

    def map_order_callback(
        self,
        *,
        submit_attempt_id: str,
        account_id: str,
        broker_order_id: str,
        perm_id: str | None,
        client_id: int | None,
        contract_key: str,
        action: Action | str,
        quantity: Decimal | int | str,
        order_type: str,
        limit_price: Decimal | int | float | str,
        status: str,
        filled_quantity: Decimal | int | str,
        remaining_quantity: Decimal | int | str,
        average_fill_price: Decimal | int | float | str | None,
        observed_at: datetime,
        raw: Mapping[str, Any] | None = None,
    ) -> BrokerOrder:
        context = self._context(submit_attempt_id)
        self._validate_callback_correlation(
            context=context,
            account_id=account_id,
            contract_key=contract_key,
            action=action,
            quantity=quantity,
            broker_order_id=broker_order_id,
            perm_id=perm_id,
        )
        order = BrokerOrder(
            broker_order_event_id=f"ibkr_order_{submit_attempt_id}_{broker_order_id}",
            run_id=context.submit_attempt.run_id,
            submit_attempt_id=submit_attempt_id,
            account_id=account_id,
            broker_order_id=broker_order_id,
            perm_id=perm_id,
            client_id=client_id,
            contract_key=contract_key,
            action=action,
            quantity=quantity,
            order_type=order_type,
            limit_price=limit_price,
            status=status,
            filled_quantity=filled_quantity,
            remaining_quantity=remaining_quantity,
            average_fill_price=average_fill_price,
            observed_at=observed_at,
            raw=dict(raw or {}),
        )
        self._broker_orders[broker_order_id] = order
        return order

    def map_exec_details(
        self,
        *,
        submit_attempt_id: str,
        account_id: str,
        broker_order_id: str,
        perm_id: str | None,
        execution_id: str,
        contract_key: str,
        action: Action | str,
        quantity: Decimal | int | str,
        price: Decimal | int | float | str,
        filled_at: datetime,
        raw: Mapping[str, Any] | None = None,
    ) -> FillEvent:
        context = self._context(submit_attempt_id)
        self._validate_callback_correlation(
            context=context,
            account_id=account_id,
            contract_key=contract_key,
            action=action,
            quantity=quantity,
            broker_order_id=broker_order_id,
            perm_id=perm_id,
        )
        normalized_execution_id = str(execution_id or "").strip()
        if not normalized_execution_id:
            self._mark_ambiguous(submit_attempt_id, "execution_id is required")
        if normalized_execution_id in self._seen_execution_ids:
            self._mark_ambiguous(submit_attempt_id, "duplicate execution_id")
        self._seen_execution_ids.add(normalized_execution_id)
        return FillEvent(
            fill_event_id=f"ibkr_fill_{submit_attempt_id}_{normalized_execution_id}",
            run_id=context.submit_attempt.run_id,
            submit_attempt_id=submit_attempt_id,
            order_intent_id=context.order_intent.order_intent_id,
            account_id=account_id,
            broker_order_id=broker_order_id,
            perm_id=perm_id,
            execution_id=normalized_execution_id,
            contract_key=contract_key,
            action=action,
            quantity=quantity,
            price=price,
            filled_at=filled_at,
            raw=dict(raw or {}),
        )

    def record_position_callback(
        self,
        *,
        run_id: str,
        account_id: str,
        contract_key: str,
        signed_quantity: int,
        average_price: Decimal | int | float | str | None,
        observed_at: datetime,
        raw: Mapping[str, Any] | None = None,
    ) -> PositionState:
        if account_id != self.account_id:
            raise IbkrPaperCorrelationError("position account mismatch")
        self._require_allowlisted_contract(contract_key)
        position = PositionState(
            position_state_id=f"ibkr_position_{run_id}_{contract_key}",
            run_id=run_id,
            source=PositionSource.BROKER,
            account_id=account_id,
            contract_key=contract_key,
            signed_quantity=signed_quantity,
            average_price=average_price,
            open_order_ids=(),
            observed_at=observed_at,
            raw=dict(raw or {}),
        )
        self._positions[contract_key] = position
        return position

    def snapshot_position(self, *, contract_key: str) -> PositionState:
        self._require_allowlisted_contract(contract_key)
        try:
            return self._positions[contract_key]
        except KeyError as exc:
            raise IbkrPaperReadinessError("missing position callback for exact contract") from exc

    def snapshot_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
        rows = tuple(
            order
            for order in self._broker_orders.values()
            if _is_working_order(order.status, order.remaining_quantity)
        )
        if contract_key is None:
            return rows
        self._require_allowlisted_contract(contract_key)
        return tuple(order for order in rows if order.contract_key == contract_key)

    def submit_limit_order(self, *, submit_attempt: SubmitAttempt, order_intent: OrderIntent) -> None:
        if not self.submit_enabled:
            raise IbkrPaperSubmitDisabledError("submit_enabled must be true before any broker submit")
        if order_intent.order_type != "LMT":
            raise IbkrPaperConfigError("market orders are forbidden; order_type must be LMT")
        self.require_ready()
        self.register_submit_context(
            submit_attempt=submit_attempt,
            order_intent=order_intent,
            created_at=submit_attempt.submitted_at,
        )
        _load_ibapi()
        raise NotImplementedError("real IBKR submit transport is intentionally not wired in slice 3A")

    def _validate_config(self) -> None:
        if self.mode != "PAPER":
            raise IbkrPaperConfigError("mode must be PAPER")
        if self.host != "127.0.0.1":
            raise IbkrPaperConfigError("host must be 127.0.0.1")
        if self.port != 7497:
            raise IbkrPaperConfigError("port must be 7497 for TWS paper")
        if self.client_id <= 0:
            raise IbkrPaperConfigError("client_id must be an explicit positive integer")
        if not self.account_id:
            raise IbkrPaperConfigError("account_id is required")
        if not self.contract_allowlist:
            raise IbkrPaperConfigError("contract_allowlist is required")

    def _require_allowlisted_contract(self, contract_key: str) -> Mapping[str, Any]:
        entry = self.contract_allowlist.get(contract_key)
        if entry is None:
            raise IbkrPaperConfigError("contract_key must be explicitly allowlisted")
        return entry

    def _context(self, submit_attempt_id: str) -> SubmitContext:
        try:
            return self._submit_contexts[submit_attempt_id]
        except KeyError as exc:
            raise IbkrPaperCorrelationError("missing submit context for callback correlation") from exc

    def _validate_callback_correlation(
        self,
        *,
        context: SubmitContext,
        account_id: str,
        contract_key: str,
        action: Action | str,
        quantity: Decimal | int | str,
        broker_order_id: str,
        perm_id: str | None,
    ) -> None:
        submit_attempt_id = context.submit_attempt.submit_attempt_id
        if account_id != self.account_id or account_id != context.order_intent.account_id:
            self._mark_ambiguous(submit_attempt_id, "account mismatch")
        if contract_key != context.order_intent.contract_key:
            self._mark_ambiguous(submit_attempt_id, "contract mismatch")
        if normalize_action(action) != context.order_intent.action:
            self._mark_ambiguous(submit_attempt_id, "action mismatch")
        if require_milestone_quantity(quantity) != context.order_intent.quantity:
            self._mark_ambiguous(submit_attempt_id, "quantity mismatch")
        if not str(broker_order_id or "").strip():
            self._mark_ambiguous(submit_attempt_id, "broker_order_id is required")
        known_order_id = str(context.submit_attempt.broker_order_id or "").strip()
        if known_order_id and known_order_id != str(broker_order_id):
            self._mark_ambiguous(submit_attempt_id, "broker_order_id mismatch")
        known_perm_id = str(context.submit_attempt.perm_id or "").strip()
        if known_perm_id and known_perm_id != str(perm_id or "").strip():
            self._mark_ambiguous(submit_attempt_id, "perm_id mismatch")

    def _mark_ambiguous(self, submit_attempt_id: str, reason: str) -> None:
        self.ambiguous_contexts[submit_attempt_id] = reason
        raise IbkrPaperCorrelationError(reason)


def _is_working_order(status: str, remaining_quantity: Decimal | int | str) -> bool:
    remaining = normalize_decimal(remaining_quantity, "remaining_quantity")
    return remaining > 0 and str(status or "").strip().upper() not in {"CANCELLED", "CANCELED", "FILLED", "INACTIVE"}


def _load_ibapi() -> Any:
    """Lazily load ibapi only for explicitly enabled live adapter operations."""

    return importlib.import_module("ibapi")
