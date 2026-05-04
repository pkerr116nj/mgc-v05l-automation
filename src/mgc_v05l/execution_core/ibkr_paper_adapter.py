"""Track B IBKR paper adapter boundary.

This module defines the paper-only IBKR boundary without importing the legacy
Track A execution stack. Optional IBKR transport imports are lazy so unit tests
can exercise callback normalization without TWS or ibapi installed.
"""

from __future__ import annotations

import importlib
import sys
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Mapping

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
        module_loader: Callable[[str], Any] | None = None,
        request_timeout_seconds: float = 30.0,
        fill_timeout_seconds: float = 30.0,
        cancel_timeout_seconds: float = 10.0,
    ) -> None:
        self.mode = str(mode or "").strip().upper()
        self.host = str(host or "").strip()
        self.port = int(port)
        self.client_id = int(client_id)
        self.account_id = str(account_id or "").strip()
        self.contract_allowlist = {str(key): dict(value) for key, value in contract_allowlist.items()}
        self.submit_enabled = bool(submit_enabled)
        self._module_loader = module_loader or importlib.import_module
        self.request_timeout_seconds = float(request_timeout_seconds)
        self.fill_timeout_seconds = float(fill_timeout_seconds)
        self.cancel_timeout_seconds = float(cancel_timeout_seconds)
        self._validate_config()

        self._bridge: Any | None = None
        self._thread: threading.Thread | None = None
        self._contract_cls: Any | None = None
        self._order_cls: Any | None = None
        self._managed_accounts: tuple[str, ...] = ()
        self.next_valid_id: int | None = None
        self._submit_contexts: dict[str, SubmitContext] = {}
        self._local_order_to_submit: dict[str, str] = {}
        self._broker_orders: dict[str, BrokerOrder] = {}
        self._fills: dict[str, FillEvent] = {}
        self._positions: dict[str, PositionState] = {}
        self._seen_execution_ids: set[str] = set()
        self._submit_diagnostics: dict[str, dict[str, Any]] = {}
        self.ambiguous_contexts: dict[str, str] = {}
        self.missing_callbacks: set[str] = set()
        self.ibkr_errors: list[dict[str, Any]] = []
        self.callback_errors: list[dict[str, Any]] = []
        self._managed_accounts_ready = threading.Event()
        self._next_valid_id_ready = threading.Event()
        self._order_ready: dict[str, threading.Event] = {}
        self._fill_ready: dict[str, threading.Event] = {}
        self._cancel_ready: dict[str, threading.Event] = {}
        self._positions_ready = threading.Event()
        self._open_orders_ready = threading.Event()

    def connect(self) -> None:
        self._bridge = self._build_bridge()
        self._bridge.connect(self.host, self.port, self.client_id)
        self._thread = threading.Thread(target=self._bridge.run, name="track-b-ibkr-paper-submit", daemon=True)
        self._thread.start()
        self._wait(self._next_valid_id_ready, "nextValidId", self.request_timeout_seconds)

    def disconnect(self) -> None:
        if self._bridge is not None:
            self._bridge.disconnect()

    def bridge_for_test(self) -> Any:
        return self._require_bridge()

    def submit_diagnostics(self, submit_attempt_id: str | None = None) -> dict[str, Any]:
        if submit_attempt_id is not None:
            return dict(self._submit_diagnostics.get(submit_attempt_id, {}))
        return {key: dict(value) for key, value in self._submit_diagnostics.items()}

    def managed_accounts(self) -> tuple[str, ...]:
        bridge = self._require_bridge()
        if not self._managed_accounts:
            bridge.reqManagedAccts()
        self._wait(self._managed_accounts_ready, "managedAccounts", self.request_timeout_seconds)
        return self._managed_accounts

    def record_managed_accounts(self, accounts: str | list[str] | tuple[str, ...]) -> tuple[str, ...]:
        if isinstance(accounts, str):
            rows = tuple(account.strip() for account in accounts.split(",") if account.strip())
        else:
            rows = tuple(str(account or "").strip() for account in accounts if str(account or "").strip())
        self._managed_accounts = rows
        self._managed_accounts_ready.set()
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
        self._next_valid_id_ready.set()
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
        fill = FillEvent(
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
        self._fills[submit_attempt_id] = fill
        self._fill_ready.setdefault(submit_attempt_id, threading.Event()).set()
        return fill

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

    def refresh_positions(self, *, contract_key: str) -> PositionState:
        bridge = self._require_bridge()
        self._positions_ready.clear()
        bridge.reqPositions()
        self._wait(self._positions_ready, "positionEnd", self.request_timeout_seconds)
        return self.snapshot_position(contract_key=contract_key)

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

    def refresh_open_orders(self, *, contract_key: str | None = None) -> tuple[BrokerOrder, ...]:
        bridge = self._require_bridge()
        self._open_orders_ready.clear()
        bridge.reqOpenOrders()
        self._wait(self._open_orders_ready, "openOrderEnd", self.request_timeout_seconds)
        return self.snapshot_open_orders(contract_key=contract_key)

    def submit_limit_order(self, *, submit_attempt: SubmitAttempt, order_intent: OrderIntent) -> int:
        if not self.submit_enabled:
            raise IbkrPaperSubmitDisabledError("submit_enabled must be true before any broker submit")
        if order_intent.order_type != "LMT":
            raise IbkrPaperConfigError("market orders are forbidden; order_type must be LMT")
        if order_intent.time_in_force != "DAY":
            raise IbkrPaperConfigError("time_in_force must be DAY")
        if order_intent.quantity != Decimal("1"):
            raise IbkrPaperConfigError("quantity must be exactly 1")
        if _has_forbidden_order_fields(order_intent.extra_fields):
            raise IbkrPaperConfigError("bracket/OCO/parent/child/algo fields are forbidden")
        self.require_ready()
        self.register_submit_context(
            submit_attempt=submit_attempt,
            order_intent=order_intent,
            created_at=submit_attempt.submitted_at,
        )
        bridge = self._require_bridge()
        local_order_id = self._allocate_local_order_id(submit_attempt)
        self._local_order_to_submit[str(local_order_id)] = submit_attempt.submit_attempt_id
        self._order_ready.setdefault(submit_attempt.submit_attempt_id, threading.Event())
        self._fill_ready.setdefault(submit_attempt.submit_attempt_id, threading.Event())
        contract = self._contract_from_allowlist(order_intent.contract_key)
        order = self._order_from_intent(order_intent)
        diagnostics = self._submit_diagnostics.setdefault(submit_attempt.submit_attempt_id, {})
        diagnostics.update(
            {
                "submit_attempt_id": submit_attempt.submit_attempt_id,
                "place_order_called": False,
                "place_order_called_at": None,
                "broker_order_id_allocated": str(local_order_id),
                "order_transmit_flag": bool(getattr(order, "transmit", False)),
                "order_action": getattr(order, "action", None),
                "order_type": getattr(order, "orderType", None),
                "limit_price": str(order_intent.limit_price),
                "tif": getattr(order, "tif", None),
                "client_id": self.client_id,
                "account_id": order_intent.account_id,
                "contract_key": order_intent.contract_key,
                "contract_local_symbol": getattr(contract, "localSymbol", None),
                "contract_con_id": getattr(contract, "conId", None),
                "callback_wait_timeout_seconds": self.request_timeout_seconds,
                "openOrder_seen": False,
                "orderStatus_seen": False,
                "execDetails_seen": False,
                "completedOrder_seen": False,
                "error_callbacks_after_submit": [],
                "isConnected_before_placeOrder": self._is_connected(),
                "isConnected_after_placeOrder": None,
                "isConnected_after_callback_wait": None,
                "place_order_exception": None,
            }
        )
        try:
            diagnostics["place_order_called"] = True
            diagnostics["place_order_called_at"] = datetime.now(timezone.utc).isoformat()
            bridge.placeOrder(local_order_id, contract, order)
            diagnostics["isConnected_after_placeOrder"] = self._is_connected()
        except Exception as exc:
            diagnostics["isConnected_after_placeOrder"] = self._is_connected()
            diagnostics["place_order_exception"] = repr(exc)
            raise
        return local_order_id

    def wait_for_broker_order(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> BrokerOrder:
        actual_timeout = timeout_seconds or self.request_timeout_seconds
        diagnostics = self._submit_diagnostics.setdefault(submit_attempt_id, {})
        diagnostics["callback_wait_timeout_seconds"] = actual_timeout
        try:
            self._wait(
                self._order_ready.setdefault(submit_attempt_id, threading.Event()),
                "openOrder/orderStatus",
                actual_timeout,
            )
        finally:
            diagnostics["isConnected_after_callback_wait"] = self._is_connected()
        for order in self._broker_orders.values():
            if order.submit_attempt_id == submit_attempt_id:
                return order
        raise IbkrPaperReadinessError("missing broker order observation")

    def wait_for_fill(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> FillEvent:
        try:
            self._wait(
                self._fill_ready.setdefault(submit_attempt_id, threading.Event()),
                "execDetails",
                timeout_seconds or self.fill_timeout_seconds,
            )
        finally:
            self._submit_diagnostics.setdefault(submit_attempt_id, {})["isConnected_after_callback_wait"] = self._is_connected()
        fill = self._fills.get(submit_attempt_id)
        if fill is not None:
            return fill
        raise IbkrPaperReadinessError("missing fill observation")

    def cancel_order(self, *, submit_attempt_id: str, broker_order_id: str) -> None:
        if not self.submit_enabled:
            raise IbkrPaperSubmitDisabledError("submit_enabled must be true before broker cancel")
        bridge = self._require_bridge()
        local_order_id = int(str(broker_order_id))
        self._cancel_ready.setdefault(submit_attempt_id, threading.Event())
        try:
            bridge.cancelOrder(local_order_id, "")
        except TypeError:
            bridge.cancelOrder(local_order_id)

    def wait_for_cancel(self, *, submit_attempt_id: str, timeout_seconds: float | None = None) -> None:
        self._wait(
            self._cancel_ready.setdefault(submit_attempt_id, threading.Event()),
            "cancel orderStatus",
            timeout_seconds or self.cancel_timeout_seconds,
        )

    def _build_bridge(self) -> Any:
        wrapper_module = self._load_module("ibapi.wrapper")
        client_module = self._load_module("ibapi.client")
        contract_module = self._load_module("ibapi.contract")
        order_module = self._load_module("ibapi.order")
        wrapper_cls = getattr(wrapper_module, "EWrapper")
        client_cls = getattr(client_module, "EClient")
        self._contract_cls = getattr(contract_module, "Contract")
        self._order_cls = getattr(order_module, "Order")
        owner = self

        class PaperSubmitBridge(wrapper_cls, client_cls):  # type: ignore[misc, valid-type]
            def __init__(self) -> None:
                wrapper_cls.__init__(self)
                client_cls.__init__(self, self)

            def nextValidId(self, orderId: int) -> None:  # noqa: N802
                owner._safe_callback("nextValidId", lambda: owner.record_next_valid_id(orderId))

            def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
                owner._safe_callback("managedAccounts", lambda: owner.record_managed_accounts(accountsList))

            def openOrder(self, orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
                owner._safe_callback("openOrder", lambda: owner._record_open_order_callback(orderId, contract, order, orderState))

            def orderStatus(self, orderId: int, status: str, filled: float, remaining: float, avgFillPrice: float, *args: Any) -> None:  # noqa: N802, ARG002
                owner._safe_callback("orderStatus", lambda: owner._record_order_status_callback(orderId, status, filled, remaining, avgFillPrice))

            def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802, ARG002
                owner._safe_callback("execDetails", lambda: owner._record_exec_details_callback(contract, execution))

            def completedOrder(self, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
                owner._safe_callback("completedOrder", lambda: owner._record_completed_order_callback(contract, order, orderState))

            def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:
                owner._safe_callback("position", lambda: owner._record_position_callback(account, contract, pos, avgCost))

            def positionEnd(self) -> None:  # noqa: N802
                owner._positions_ready.set()

            def openOrderEnd(self) -> None:  # noqa: N802
                owner._open_orders_ready.set()

            def error(self, *args: Any) -> None:  # noqa: N802
                owner._safe_callback("error", lambda: owner._record_error(args))

        return PaperSubmitBridge()

    def _load_module(self, name: str) -> Any:
        try:
            return self._module_loader(name)
        except ModuleNotFoundError as exc:
            raise IbkrPaperConfigError(f"optional IBKR API module is unavailable: {name}") from exc

    def _require_bridge(self) -> Any:
        if self._bridge is None:
            raise IbkrPaperReadinessError("IBKR paper adapter is not connected")
        return self._bridge

    def _wait(self, event: threading.Event, callback_name: str, timeout_seconds: float) -> None:
        if not event.wait(float(timeout_seconds)):
            self.missing_callbacks.add(callback_name)
            raise IbkrPaperReadinessError(f"missing {callback_name} callback")

    def _allocate_local_order_id(self, submit_attempt: SubmitAttempt) -> int:
        explicit = str(submit_attempt.broker_order_id or "").strip()
        if explicit:
            return int(explicit)
        if self.next_valid_id is None:
            raise IbkrPaperReadinessError("missing nextValidId callback")
        local_order_id = int(self.next_valid_id)
        self.next_valid_id += 1
        return local_order_id

    def _contract_from_allowlist(self, contract_key: str) -> Any:
        entry = self._require_allowlisted_contract(contract_key)
        if self._contract_cls is None:
            self._load_module("ibapi.contract")
        contract = self._contract_cls()
        contract.symbol = str(entry.get("symbol") or "")
        contract.secType = str(entry.get("security_type") or entry.get("secType") or "FUT")
        contract.exchange = str(entry.get("exchange") or "")
        contract.currency = str(entry.get("currency") or "USD")
        contract.lastTradeDateOrContractMonth = str(entry.get("expiry") or entry.get("contract_month") or "")
        if entry.get("local_symbol"):
            contract.localSymbol = str(entry["local_symbol"])
        if entry.get("con_id") is not None:
            contract.conId = int(entry["con_id"])
        if entry.get("multiplier") is not None:
            contract.multiplier = str(entry["multiplier"])
        return contract

    def _order_from_intent(self, order_intent: OrderIntent) -> Any:
        if self._order_cls is None:
            self._load_module("ibapi.order")
        order = self._order_cls()
        order.account = order_intent.account_id
        order.action = order_intent.action.value
        order.totalQuantity = float(order_intent.quantity)
        order.orderType = order_intent.order_type
        order.lmtPrice = float(order_intent.limit_price)
        order.tif = order_intent.time_in_force
        _normalize_unsupported_order_defaults(order)
        order.transmit = True
        return order

    def _record_open_order_callback(self, order_id: int, contract: Any, order: Any, order_state: Any) -> None:
        submit_attempt_id = self._submit_id_for_local_order(order_id)
        if submit_attempt_id is None:
            return
        self._submit_diagnostics.setdefault(submit_attempt_id, {})["openOrder_seen"] = True
        broker_order = self.map_order_callback(
            submit_attempt_id=submit_attempt_id,
            account_id=str(getattr(order, "account", "") or ""),
            broker_order_id=str(order_id),
            perm_id=str(getattr(order, "permId", "") or "") or None,
            client_id=getattr(order, "clientId", None),
            contract_key=self._contract_key_from_contract(contract),
            action=str(getattr(order, "action", "") or ""),
            quantity=getattr(order, "totalQuantity", 1),
            order_type=str(getattr(order, "orderType", "") or ""),
            limit_price=getattr(order, "lmtPrice", 0),
            status=str(getattr(order_state, "status", "") or "Submitted"),
            filled_quantity=0,
            remaining_quantity=getattr(order, "totalQuantity", 1),
            average_fill_price=None,
            observed_at=datetime.now(timezone.utc),
            raw={"callback": "openOrder"},
        )
        self._order_ready.setdefault(submit_attempt_id, threading.Event()).set()
        if _is_cancelled_status(broker_order.status):
            self._cancel_ready.setdefault(submit_attempt_id, threading.Event()).set()

    def _record_order_status_callback(self, order_id: int, status: str, filled: float, remaining: float, avg_fill_price: float) -> None:
        submit_attempt_id = self._submit_id_for_local_order(order_id)
        if submit_attempt_id is None:
            return
        self._submit_diagnostics.setdefault(submit_attempt_id, {})["orderStatus_seen"] = True
        context = self._context(submit_attempt_id)
        broker_order = self.map_order_callback(
            submit_attempt_id=submit_attempt_id,
            account_id=context.order_intent.account_id,
            broker_order_id=str(order_id),
            perm_id=context.submit_attempt.perm_id,
            client_id=self.client_id,
            contract_key=context.order_intent.contract_key,
            action=context.order_intent.action,
            quantity=context.order_intent.quantity,
            order_type=context.order_intent.order_type,
            limit_price=context.order_intent.limit_price,
            status=status,
            filled_quantity=filled,
            remaining_quantity=remaining,
            average_fill_price=avg_fill_price if filled else None,
            observed_at=datetime.now(timezone.utc),
            raw={"callback": "orderStatus"},
        )
        self._order_ready.setdefault(submit_attempt_id, threading.Event()).set()
        if _is_cancelled_status(broker_order.status):
            self._cancel_ready.setdefault(submit_attempt_id, threading.Event()).set()

    def _record_exec_details_callback(self, contract: Any, execution: Any) -> None:
        order_id = getattr(execution, "orderId", None)
        submit_attempt_id = self._submit_id_for_local_order(order_id)
        if submit_attempt_id is None:
            return
        self._submit_diagnostics.setdefault(submit_attempt_id, {})["execDetails_seen"] = True
        side = str(getattr(execution, "side", "") or "").upper()
        action = "BUY" if side in {"BOT", "BUY"} else "SELL"
        self.map_exec_details(
            submit_attempt_id=submit_attempt_id,
            account_id=str(getattr(execution, "acctNumber", "") or self.account_id),
            broker_order_id=str(order_id),
            perm_id=str(getattr(execution, "permId", "") or "") or None,
            execution_id=str(getattr(execution, "execId", "") or ""),
            contract_key=self._contract_key_from_contract(contract),
            action=action,
            quantity=getattr(execution, "shares", 1),
            price=getattr(execution, "price", 0),
            filled_at=datetime.now(timezone.utc),
            raw={"callback": "execDetails"},
        )

    def _record_completed_order_callback(self, contract: Any, order: Any, order_state: Any) -> None:
        order_id = getattr(order, "orderId", None)
        if order_id is not None:
            submit_attempt_id = self._submit_id_for_local_order(order_id)
            if submit_attempt_id is not None:
                self._submit_diagnostics.setdefault(submit_attempt_id, {})["completedOrder_seen"] = True
            self._record_open_order_callback(int(order_id), contract, order, order_state)

    def _record_position_callback(self, account: str, contract: Any, pos: float, avg_cost: float) -> None:
        contract_key = self._contract_key_from_contract(contract)
        self.record_position_callback(
            run_id="ibkr_position_snapshot",
            account_id=account,
            contract_key=contract_key,
            signed_quantity=int(pos),
            average_price=avg_cost,
            observed_at=datetime.now(timezone.utc),
            raw={"callback": "position", "contract": _contract_fields(contract)},
        )

    def _record_error(self, args: tuple[Any, ...]) -> None:
        raw_args = [repr(arg) for arg in args]
        if len(args) >= 4 and isinstance(args[2], int):
            request_id = int(args[0])
            error_code = int(args[2])
            error_string = str(args[3])
        elif len(args) >= 3:
            request_id = int(args[0])
            error_code = int(args[1])
            error_string = str(args[2])
        else:
            request_id = -1
            error_code = -1
            error_string = "unknown IBKR error callback shape"
        self.ibkr_errors.append(
            {"request_id": request_id, "error_code": error_code, "error_string": error_string, "raw_args": raw_args}
        )
        submit_attempt_id = self._submit_id_for_local_order(request_id)
        if submit_attempt_id is not None:
            self._submit_diagnostics.setdefault(submit_attempt_id, {}).setdefault("error_callbacks_after_submit", []).append(
                {"request_id": request_id, "error_code": error_code, "error_string": error_string, "raw_args": raw_args}
            )

    def _is_connected(self) -> bool:
        if self._bridge is None:
            return False
        is_connected = getattr(self._bridge, "isConnected", None)
        return bool(is_connected()) if callable(is_connected) else True

    def _submit_id_for_local_order(self, order_id: Any) -> str | None:
        return self._local_order_to_submit.get(str(order_id))

    def _safe_callback(self, callback_name: str, handler: Callable[[], Any]) -> None:
        try:
            handler()
        except Exception as exc:  # noqa: BLE001 - callback threads must report, not die silently.
            self.callback_errors.append(
                {
                    "callback": callback_name,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "captured_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            if callback_name == "position":
                self._positions_ready.set()
            if callback_name in {"openOrder", "orderStatus", "completedOrder"}:
                self._open_orders_ready.set()

    def _contract_key_from_contract(self, contract: Any) -> str:
        fields = _contract_fields(contract)
        con_id = fields["conId"]
        local_symbol = str(fields["localSymbol"] or "")
        if _positive_int_or_none(con_id) is not None:
            for contract_key, entry in self.contract_allowlist.items():
                if _positive_int_or_none(entry.get("con_id")) == _positive_int_or_none(con_id):
                    return contract_key
            raise IbkrPaperCorrelationError(f"contract callback did not match allowlist conId: {_contract_fields(contract)}")
        if local_symbol:
            for contract_key, entry in self.contract_allowlist.items():
                if _normalize_symbol(entry.get("local_symbol")) == _normalize_symbol(local_symbol):
                    return contract_key
            raise IbkrPaperCorrelationError(f"contract callback did not match allowlist localSymbol: {_contract_fields(contract)}")
        for contract_key, entry in self.contract_allowlist.items():
            if _contract_month_match(fields, entry):
                return contract_key
        raise IbkrPaperCorrelationError(f"contract callback did not match allowlist: {_contract_fields(contract)}")

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


def _is_cancelled_status(status: str) -> bool:
    return str(status or "").strip().upper() in {"CANCELLED", "CANCELED"}


def _contract_fields(contract: Any) -> dict[str, Any]:
    return {
        "symbol": getattr(contract, "symbol", None),
        "secType": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None),
        "currency": getattr(contract, "currency", None),
        "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "conId": getattr(contract, "conId", None),
        "multiplier": getattr(contract, "multiplier", None),
        "tradingClass": getattr(contract, "tradingClass", None),
    }


def _contract_month_match(fields: Mapping[str, Any], entry: Mapping[str, Any]) -> bool:
    symbol_ok = _normalize_symbol(fields.get("symbol")) == _normalize_symbol(entry.get("symbol"))
    sec_type_ok = _normalize_symbol(fields.get("secType")) == _normalize_symbol(entry.get("security_type") or entry.get("secType") or "FUT")
    currency_ok = not fields.get("currency") or _normalize_symbol(fields.get("currency")) == _normalize_symbol(entry.get("currency") or "USD")
    callback_month = _normalize_contract_month(fields.get("lastTradeDateOrContractMonth"))
    allowlist_month = _normalize_contract_month(entry.get("expiry") or entry.get("contract_month"))
    month_ok = bool(callback_month and allowlist_month and callback_month.startswith(allowlist_month[:6]))
    callback_multiplier = str(fields.get("multiplier") or "").strip()
    allowlist_multiplier = str(entry.get("multiplier") or "").strip()
    multiplier_ok = not callback_multiplier or not allowlist_multiplier or callback_multiplier == allowlist_multiplier
    return symbol_ok and sec_type_ok and currency_ok and month_ok and multiplier_ok


def _normalize_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_contract_month(value: Any) -> str:
    raw = "".join(ch for ch in str(value or "").strip() if ch.isdigit())
    return raw[:8] if len(raw) >= 8 else raw[:6]


def _positive_int_or_none(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _has_forbidden_order_fields(extra_fields: Mapping[str, Any]) -> bool:
    forbidden = {"algo", "algostrategy", "bracket", "child", "children", "oca", "ocagroup", "oco", "parent", "parentid"}
    for key in extra_fields:
        normalized = str(key).replace("_", "").replace("-", "").strip().lower()
        if normalized in forbidden:
            return True
    return False


def _normalize_unsupported_order_defaults(order: Any) -> None:
    """Clear legacy IBKR defaults that TWS rejects for futures paper orders."""

    for attr in ("eTradeOnly", "firmQuoteOnly"):
        if hasattr(order, attr):
            setattr(order, attr, False)
    if hasattr(order, "nbboPriceCap"):
        setattr(order, "nbboPriceCap", sys.float_info.max)


def _load_ibapi() -> Any:
    """Lazily load ibapi only for explicitly enabled live adapter operations."""

    return importlib.import_module("ibapi")
