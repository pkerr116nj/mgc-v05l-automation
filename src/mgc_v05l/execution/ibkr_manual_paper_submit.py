"""Manual-only IBKR paper order harness for controlled paper plumbing tests."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import re
import select
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..brokers.ibkr import (
    IbkrClient,
    IbkrContractResolver,
    IbkrQualifiedContract,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)
from .ibkr_phase1_futures_scope import phase1_execution_target_for_symbol
from .ibkr_execution_provider import IbkrExecutionProvider
from .ibkr_paper_order_preview import (
    _FORBIDDEN_CALLER_PREFIXES,
    _FORBIDDEN_CALLER_SUBSTRINGS,
    _coerce_float,
    _guardrail_check,
    _normalize_requested_order,
    _runtime_guardrail_checks,
    build_preview_digest,
    evaluate_manual_preview_caller,
    evaluate_paper_preview_environment_lock,
)
from .ibkr_read_only_verifier import (
    _ACCOUNT_SUMMARY_TAGS,
    _build_account_truth_snapshot,
    _build_open_orders_snapshot,
    _build_positions_snapshot,
    _empty_market_data_row,
    _first_non_empty,
    _qualified_contract_to_dict,
    _resolve_selected_account_id,
    _tws_manual_check_message,
    _wait_for_connection_ready,
    _wait_for_event,
    IbkrReadOnlyApiTransportConfig,
    IbkrReadOnlyProbeCollector,
    IbkrReadOnlyVerificationError,
)

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_EXPECTED_SYMBOL = "MGC"
_EXPECTED_EXPIRY = "202606"
_EXPECTED_ACTION = "BUY"
_EXPECTED_QUANTITY = 1.0
_EXPECTED_ORDER_TYPE = "LMT"
_EXPECTED_TIF = "DAY"
_BID_TICK_IDS = (1, 66)
_ASK_TICK_IDS = (2, 67)
_LAST_TICK_IDS = (4, 68)
_CLOSE_TICK_IDS = (9, 75)
_PERMISSION_ERROR_CODES = {354, 10167, 10168}
_SEVERE_CONNECTION_ERROR_CODES = {502, 504, 1100, 1101, 1102, 1300, 326}
_SUCCESS_ORDER_STATUS = {"Submitted", "PreSubmitted", "ApiPending", "PendingSubmit"}
_CANCELLED_ORDER_STATUS = {"Cancelled", "ApiCancelled"}
_FAILED_ORDER_STATUS = {"Inactive"}
_MANUAL_CONFIRMATION_WAIT_STATE = "SUBMIT_SENT_AWAITING_TWS_MANUAL_CONFIRMATION"
_DEFAULT_DELAYED_QUOTE_MAX_AGE_SECONDS = 30.0
_DEFAULT_NEAR_MARKET_MAX_DISTANCE_TICKS = 50.0
_DEFAULT_FILL_LIMIT_OFFSET_TICKS = 1.0
_DEFAULT_FILL_TIMEOUT_SECONDS = 8.0
_TICK_COMPARISON_EPSILON = 1e-9
_RESTING_TEST_MODE = "PAPER_RESTING_TEST"
_FILL_TEST_MODE = "PAPER_FILL_TEST"
_CLOSE_TEST_MODE = "PAPER_CLOSE_TEST"
_MARKETABLE_LIMIT_LABEL = "MARKETABLE_LIMIT_INTENDED_TO_FILL_IN_PAPER"
_CLOSE_MARKETABLE_LIMIT_LABEL = "MARKETABLE_LIMIT_INTENDED_TO_CLOSE_IN_PAPER"
_NON_MARKETABLE_LIMIT_LABEL = "NEAR_MARKET_NON_MARKETABLE_LIMIT"
_FILLED_ORDER_STATUS = {"Filled"}
_PARTIAL_FILL_STATUS = {"PartiallyFilled"}
_ORDER_REJECTION_ERROR_CODES = {478, 10268, 201, 202}


class IbkrManualPaperSubmitError(RuntimeError):
    """Base error for the manual paper submit/cancel harness."""


def _phase1_target_for_requested_order(requested_order: dict[str, Any]) -> dict[str, Any]:
    symbol = str(requested_order.get("symbol") or "").strip().upper()
    expiry = str(requested_order.get("expiry") or "").strip() or None
    try:
        target = dict(phase1_execution_target_for_symbol(symbol, contract_month=expiry))
    except KeyError:
        return {}
    target.setdefault("symbol", symbol)
    target.setdefault("contract_month", expiry or "")
    return target


@dataclass(frozen=True)
class IbkrManualPaperSubmitConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    account_id: str | None = None
    symbol: str = _EXPECTED_SYMBOL
    expiry: str = _EXPECTED_EXPIRY
    action: str = _EXPECTED_ACTION
    quantity: float = _EXPECTED_QUANTITY
    order_type: str = _EXPECTED_ORDER_TYPE
    limit_price: float | None = None
    time_in_force: str = _EXPECTED_TIF
    test_mode: str = _RESTING_TEST_MODE
    timeout_seconds: float = 15.0
    fill_timeout_seconds: float = _DEFAULT_FILL_TIMEOUT_SECONDS
    manual_confirmation_timeout_seconds: float = 90.0
    delayed_quote_max_age_seconds: float = _DEFAULT_DELAYED_QUOTE_MAX_AGE_SECONDS
    near_market_max_distance_ticks: float = _DEFAULT_NEAR_MARKET_MAX_DISTANCE_TICKS
    fill_limit_offset_ticks: float = _DEFAULT_FILL_LIMIT_OFFSET_TICKS
    caller_path: str = "manual_cli"
    submit: bool = False
    approval_digest: str | None = None
    approval_phrase: str | None = None
    output_dir: Path | None = None
    frozen_preview_path: Path | None = None
    diagnostic_dry_run: bool = False
    post_approval_observation_seconds: float = 15.0


@dataclass(frozen=True)
class IbkrManualPaperSubmitArtifacts:
    classification: str
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]
    open_order_before: dict[str, Any]
    open_order_after_submit: dict[str, Any]
    open_order_after_cancel: dict[str, Any]
    artifact_stem: str = "ibkr_manual_paper_submit"
    extra_artifacts: dict[str, Any] | None = None
    callback_timeline: list[dict[str, Any]] | None = None

    @property
    def exit_code(self) -> int:
        return 0 if not str(self.classification or "").endswith("_BLOCKED") else 1


@dataclass
class _SubmitRuntime:
    session: IbkrSession
    client: IbkrClient
    collector: "IbkrManualPaperSubmitCollector"
    transport: "IbkrManualPaperSubmitTransport"


class IbkrManualPaperSubmitCollector(IbkrReadOnlyProbeCollector):
    """Extends the read-only collector with order-status lifecycle tracking."""

    def __init__(self, client: IbkrClient) -> None:
        super().__init__(client)
        self.order_status_rows: dict[int, list[dict[str, Any]]] = {}
        self._order_status_ready: dict[int, threading.Event] = {}
        self.completed_orders_ready = threading.Event()
        self._executions_ready: dict[int, threading.Event] = {}
        self.callback_timeline: list[dict[str, Any]] = []

    def record_callback(self, callback_name: str, **payload: Any) -> None:
        self.callback_timeline.append(
            {
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "source": "callback",
                "callback_name": str(callback_name),
                **dict(payload),
            }
        )

    def order_status_event(self, order_id: int) -> threading.Event:
        return self._order_status_ready.setdefault(int(order_id), threading.Event())

    def order_status(
        self,
        *,
        order_id: int,
        status: str,
        filled: float,
        remaining: float,
        avg_fill_price: float | None,
        perm_id: int | None,
        parent_id: int | None,
        last_fill_price: float | None,
        client_id: int | None,
        why_held: str | None,
        mkt_cap_price: float | None,
        occurred_at: datetime | None = None,
    ) -> None:
        row = {
            "order_id": int(order_id),
            "status": str(status or "").strip(),
            "filled": float(filled),
            "remaining": float(remaining),
            "avg_fill_price": None if avg_fill_price is None else float(avg_fill_price),
            "perm_id": None if perm_id is None else int(perm_id),
            "parent_id": None if parent_id is None else int(parent_id),
            "last_fill_price": None if last_fill_price is None else float(last_fill_price),
            "client_id": None if client_id is None else int(client_id),
            "why_held": str(why_held or "").strip() or None,
            "mkt_cap_price": None if mkt_cap_price is None else float(mkt_cap_price),
            "updated_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
        }
        self.order_status_rows.setdefault(int(order_id), []).append(row)
        self.order_status_event(int(order_id)).set()

    def open_order(
        self,
        *,
        account_id: str,
        broker_order_id: int,
        client_id: int,
        perm_id: int | None,
        contract: dict[str, Any],
        status: str,
        quantity: str | int | float,
        filled_quantity: str | int | float | None = None,
        limit_price: str | int | float | None = None,
        stop_price: str | int | float | None = None,
        occurred_at: datetime | None = None,
    ) -> None:
        super().open_order(
            account_id=account_id,
            broker_order_id=broker_order_id,
            client_id=client_id,
            perm_id=perm_id,
            contract=contract,
            status=status,
            quantity=quantity,
            filled_quantity=filled_quantity,
            limit_price=limit_price,
            stop_price=stop_price,
            occurred_at=occurred_at,
        )
        self.order_status_event(int(broker_order_id)).set()

    def reset_open_orders_ready(self) -> None:
        self.open_orders_ready.clear()

    def reset_order_status_event(self, order_id: int) -> None:
        self.order_status_event(int(order_id)).clear()

    def latest_order_status(self, order_id: int) -> dict[str, Any] | None:
        rows = self.order_status_rows.get(int(order_id), [])
        return rows[-1] if rows else None

    def executions_event(self, request_id: int) -> threading.Event:
        return self._executions_ready.setdefault(int(request_id), threading.Event())

    def reset_completed_orders_ready(self) -> None:
        self.completed_orders_ready.clear()

    def reset_executions_ready(self, request_id: int) -> None:
        self.executions_event(int(request_id)).clear()

    def completed_order(
        self,
        *,
        account_id: str,
        broker_order_id: int,
        client_id: int,
        perm_id: int | None,
        contract: dict[str, Any],
        status: str,
        quantity: str | int | float,
        occurred_at: datetime | None = None,
    ) -> None:
        self._adapter.completed_order(
            account_id=account_id,
            broker_order_id=broker_order_id,
            client_id=client_id,
            perm_id=perm_id,
            contract=contract,
            status=status,
            quantity=quantity,
            completed_at=occurred_at,
        )

    def completed_orders_end(self, *, occurred_at: datetime | None = None) -> None:
        self._adapter.completed_orders_end(occurred_at=occurred_at)
        self.completed_orders_ready.set()

    def exec_details(
        self,
        *,
        request_id: int,
        account_id: str,
        execution_id: str,
        broker_order_id: int | None,
        client_id: int | None,
        perm_id: int | None,
        contract: dict[str, Any],
        side: str | None,
        quantity: str | int | float,
        price: str | int | float | None,
        occurred_at: datetime | None = None,
    ) -> None:
        self._adapter.exec_details(
            account_id=account_id,
            execution_id=execution_id,
            broker_order_id=broker_order_id,
            client_id=client_id,
            perm_id=perm_id,
            contract=contract,
            side=side,
            quantity=quantity,
            price=price,
            executed_at=occurred_at,
        )

    def exec_details_end(self, *, request_id: int, occurred_at: datetime | None = None) -> None:
        self._adapter.exec_details_end(occurred_at=occurred_at)
        self.executions_event(int(request_id)).set()


class IbkrManualPaperSubmitTransport:
    """Harness-specific IBKR transport with submit/cancel support."""

    def __init__(
        self,
        *,
        client: IbkrClient,
        collector: IbkrManualPaperSubmitCollector,
        config: IbkrReadOnlyApiTransportConfig,
        module_loader: Callable[[str], Any] | None = None,
    ) -> None:
        self._client = client
        self._collector = collector
        self._config = config
        self._module_loader = module_loader or importlib.import_module
        self._bridge: Any | None = None

    def connect(self) -> None:
        bridge = self._ensure_bridge()
        self._client.record_event(
            "transport_connect_requested",
            payload={
                "host": self._config.host,
                "port": self._config.port,
                "client_id": self._config.client_id,
                "read_only": self._config.read_only,
            },
        )
        bridge.connect(self._config.host, int(self._config.port), int(self._config.client_id))

    def disconnect(self) -> None:
        if self._bridge is None:
            return
        self._client.record_event("transport_disconnect_requested", payload={})
        self._bridge.disconnect()

    def run_loop(self) -> None:
        self._ensure_bridge().run()

    def is_connected(self) -> bool:
        bridge = self._ensure_bridge()
        checker = getattr(bridge, "isConnected", None)
        if checker is None:
            return False
        try:
            return bool(checker())
        except Exception:
            return False

    def server_version(self) -> int | None:
        bridge = self._ensure_bridge()
        getter = getattr(bridge, "serverVersion", None)
        if getter is None:
            return None
        try:
            return int(getter())
        except Exception:
            return None

    def tws_connection_time(self) -> str | None:
        bridge = self._ensure_bridge()
        getter = getattr(bridge, "twsConnectionTime", None)
        if getter is None:
            return None
        try:
            value = getter()
        except Exception:
            return None
        text = str(value or "").strip()
        return text or None

    def req_managed_accounts(self) -> None:
        self._ensure_bridge().reqManagedAccts()

    def req_account_summary(self, *, request_id: int, group_name: str, tags: str) -> None:
        self._ensure_bridge().reqAccountSummary(int(request_id), group_name, tags)

    def cancel_account_summary(self, *, request_id: int) -> None:
        self._ensure_bridge().cancelAccountSummary(int(request_id))

    def req_account_updates(self, *, subscribe: bool, account_id: str) -> None:
        self._ensure_bridge().reqAccountUpdates(bool(subscribe), str(account_id))

    def req_positions(self) -> None:
        self._ensure_bridge().reqPositions()

    def req_all_open_orders(self) -> None:
        self._ensure_bridge().reqAllOpenOrders()

    def req_open_orders(self) -> None:
        self._ensure_bridge().reqOpenOrders()

    def req_completed_orders(self, *, api_only: bool) -> None:
        self._ensure_bridge().reqCompletedOrders(bool(api_only))

    def req_executions(self, *, request_id: int) -> None:
        execution_module = self._module_loader("ibapi.execution")
        filter_cls = getattr(execution_module, "ExecutionFilter", None)
        if filter_cls is None:
            raise IbkrManualPaperSubmitError("Installed ibapi package is missing ExecutionFilter.")
        execution_filter = filter_cls()
        self._ensure_bridge().reqExecutions(int(request_id), execution_filter)

    def req_contract_details(self, *, request_id: int, contract: IbkrQualifiedContract) -> None:
        self._ensure_bridge().reqContractDetails(int(request_id), self._raw_contract(contract))

    def req_market_data_type(self, *, market_data_type: int) -> None:
        self._ensure_bridge().reqMarketDataType(int(market_data_type))

    def req_market_data_snapshot(self, *, request_id: int, contract: IbkrQualifiedContract) -> None:
        self._ensure_bridge().reqMktData(int(request_id), self._raw_contract(contract), "", True, False, [])

    def cancel_market_data(self, *, request_id: int) -> None:
        self._ensure_bridge().cancelMktData(int(request_id))

    def place_limit_order(
        self,
        *,
        order_id: int,
        account_id: str,
        contract: IbkrQualifiedContract,
        action: str,
        quantity: float,
        limit_price: float,
        time_in_force: str,
    ) -> None:
        order_cls = getattr(self._module_loader("ibapi.order"), "Order", None)
        if order_cls is None:
            raise IbkrManualPaperSubmitError("Installed ibapi package is missing Order.")
        raw_order = order_cls()
        common_module = self._module_loader("ibapi.common")
        _configure_minimal_futures_limit_order(
            raw_order,
            order_id=order_id,
            account_id=account_id,
            action=action,
            quantity=quantity,
            limit_price=limit_price,
            time_in_force=time_in_force,
            common_module=common_module,
        )
        self._ensure_bridge().placeOrder(int(order_id), self._raw_contract(contract), raw_order)

    def cancel_order(self, *, order_id: int) -> None:
        bridge = self._ensure_bridge()
        cancel_method = getattr(bridge, "cancelOrder", None)
        if cancel_method is None:
            raise IbkrManualPaperSubmitError("Installed ibapi bridge is missing cancelOrder.")
        try:
            cancel_method(int(order_id), "")
        except TypeError:
            cancel_method(int(order_id))

    def _raw_contract(self, contract: IbkrQualifiedContract) -> Any:
        contract_cls = getattr(self._module_loader("ibapi.contract"), "Contract", None)
        if contract_cls is None:
            raise IbkrManualPaperSubmitError("Installed ibapi package is missing Contract.")
        raw_contract = contract_cls()
        raw_contract.symbol = contract.broker_symbol
        raw_contract.secType = contract.security_type
        raw_contract.exchange = contract.exchange
        raw_contract.currency = contract.currency
        raw_contract.lastTradeDateOrContractMonth = contract.expiry
        raw_contract.multiplier = contract.multiplier
        raw_contract.tradingClass = contract.trading_class
        if contract.con_id is not None and getattr(contract, "local_symbol", None):
            raw_contract.localSymbol = contract.local_symbol
        if contract.con_id is not None:
            raw_contract.conId = int(contract.con_id)
        return raw_contract

    def _ensure_bridge(self) -> Any:
        if self._bridge is None:
            wrapper_cls = getattr(self._module_loader("ibapi.wrapper"), "EWrapper", None)
            client_cls = getattr(self._module_loader("ibapi.client"), "EClient", None)
            if wrapper_cls is None or client_cls is None:
                raise IbkrManualPaperSubmitError(
                    "Installed ibapi package is missing EWrapper/EClient and cannot be used for the manual paper submit harness."
                )
            self._bridge = _build_submit_bridge(
                wrapper_cls=wrapper_cls,
                client_cls=client_cls,
                collector=self._collector,
            )
        return self._bridge


def _configure_minimal_futures_limit_order(
    raw_order: Any,
    *,
    order_id: int,
    account_id: str,
    action: str,
    quantity: float,
    limit_price: float,
    time_in_force: str,
    common_module: Any | None,
) -> None:
    unset_double = getattr(common_module, "UNSET_DOUBLE", None) if common_module is not None else None
    unset_integer = getattr(common_module, "UNSET_INTEGER", None) if common_module is not None else None
    raw_order.orderId = int(order_id)
    raw_order.account = str(account_id)
    raw_order.action = str(action)
    raw_order.totalQuantity = float(quantity)
    raw_order.orderType = "LMT"
    raw_order.lmtPrice = float(limit_price)
    raw_order.tif = str(time_in_force)
    raw_order.transmit = True
    if hasattr(raw_order, "eTradeOnly"):
        raw_order.eTradeOnly = False
    if hasattr(raw_order, "firmQuoteOnly"):
        raw_order.firmQuoteOnly = False
    if hasattr(raw_order, "nbboPriceCap") and unset_double is not None:
        raw_order.nbboPriceCap = unset_double
    if hasattr(raw_order, "auctionStrategy") and unset_integer is not None:
        raw_order.auctionStrategy = unset_integer
    if hasattr(raw_order, "discretionaryAmt") and unset_double is not None:
        raw_order.discretionaryAmt = unset_double
    if hasattr(raw_order, "outsideRth"):
        raw_order.outsideRth = False


def run_ibkr_manual_paper_submit_test(
    *,
    config: IbkrManualPaperSubmitConfig,
    transport_factory: Callable[..., Any] = IbkrManualPaperSubmitTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
    manual_confirmation_fn: Callable[..., dict[str, Any]] | None = None,
) -> IbkrManualPaperSubmitArtifacts:
    started_at = datetime.now(timezone.utc)
    audit_events: list[dict[str, Any]] = []
    frozen_preview_bundle: dict[str, Any] | None = None
    frozen_preview_path = frozen_preview_path_for_config(config)
    requested_order = _normalize_requested_order(config)
    if config.submit:
        if frozen_preview_path is None:
            detail = "Submit requires a frozen preview bundle path. Generate a preview first and re-run submit with the saved frozen preview payload."
            return _blocked_artifacts(
                config=config,
                started_at=started_at,
                requested_order=requested_order,
                caller_check=evaluate_manual_preview_caller(caller_path=config.caller_path, stack_provider=stack_provider),
                environment_lock=evaluate_paper_preview_environment_lock(
                    mode=config.mode,
                    host=config.host,
                    port=config.port,
                ),
                guardrail_checks=[],
                audit_events=audit_events,
                detail=detail,
            )
        frozen_preview_bundle = _load_frozen_preview_bundle(frozen_preview_path)
        requested_order = _normalize_requested_order_from_bundle(frozen_preview_bundle)
    caller_check = evaluate_manual_preview_caller(caller_path=config.caller_path, stack_provider=stack_provider)
    environment_lock = evaluate_paper_preview_environment_lock(
        mode=config.mode,
        host=config.host,
        port=config.port,
    )
    input_checks = _submit_input_guardrails(
        requested_order,
        test_mode=config.test_mode,
        require_limit_price=not (
            not config.submit and str(config.test_mode).upper() in {_FILL_TEST_MODE, _CLOSE_TEST_MODE}
        ),
    )
    guardrail_checks = _preflight_guardrail_checks(
        caller_check=caller_check,
        environment_lock=environment_lock,
        input_guardrails=input_checks,
    )
    if frozen_preview_bundle is not None:
        guardrail_checks.extend(_frozen_preview_bundle_guardrails(config=config, frozen_preview_bundle=frozen_preview_bundle))
    _record_audit(
        audit_events,
        event_type="environment_lock_checked",
        config=config,
        classification=None,
        detail=environment_lock["port_policy"],
    )
    if any(check["blocking"] and not check["passed"] for check in guardrail_checks):
        detail = _first_failed_guardrail_detail(guardrail_checks)
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            requested_order=requested_order,
            caller_check=caller_check,
            environment_lock=environment_lock,
            guardrail_checks=guardrail_checks,
            audit_events=audit_events,
            detail=detail,
        )

    runtime: _SubmitRuntime | None = None
    try:
        runtime = _build_runtime(
            config=config,
            transport_factory=transport_factory,
            module_loader=module_loader,
        )
        runtime.transport.connect()
        _start_runtime(runtime)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error(codes=_SEVERE_CONNECTION_ERROR_CODES) or runtime.collector.latest_error()
            raise IbkrManualPaperSubmitError(
                (
                    f"TWS paper submit harness handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"TWS paper submit harness handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        context = _collect_truth_and_preview_context(
            config=config,
            runtime=runtime,
            sleep_fn=sleep_fn,
            started_at=started_at,
            requested_order=requested_order,
            collect_quote=not config.submit,
        )
        audit_events.extend(context["audit_events"])
        guardrail_checks.extend(
            _runtime_guardrail_checks(
                config=_preview_config_from_submit(config),
                runtime_context={
                    "selected_account_id": context["selected_account_id"],
                    "open_orders_snapshot": context["open_orders_before"],
                    "contract_report": context["contract_report"],
                    "quote_context": context["quote_context"],
                },
            )
        )
        if config.submit:
            context, requested_order, preview_payload, preview_digest, expected_phrase, pricing_context = _prepare_frozen_submit_context(
                config=config,
                context=context,
                requested_order=requested_order,
                frozen_preview_bundle=frozen_preview_bundle or {},
            )
        else:
            requested_order = _resolve_requested_order_for_preview(
                config=config,
                requested_order=requested_order,
                contract_report=context["contract_report"],
                quote_context=context["quote_context"],
            )
            pricing_context = _build_delayed_quote_pricing_context(
                config=config,
                requested_order=requested_order,
                context=context,
            )
            guardrail_checks.extend(_delayed_quote_pricing_guardrails(pricing_context))
            if any(check["blocking"] and not check["passed"] for check in guardrail_checks):
                detail = _first_failed_guardrail_detail(guardrail_checks)
                _record_audit(
                    audit_events,
                    event_type="failed_closed",
                    config=config,
                    classification=_blocked_classification_for_mode(config.test_mode),
                    detail=detail,
                    extra={
                        "quote_snapshot": pricing_context.get("quote_snapshot"),
                        "limit_price": requested_order.get("limit_price"),
                        "distance_from_quote": pricing_context.get("distance_from_reference_price"),
                        "distance_ticks": pricing_context.get("distance_ticks"),
                    },
                )
                report = _build_report(
                    config=config,
                    started_at=started_at,
                    classification=_blocked_classification_for_mode(config.test_mode),
                    caller_check=caller_check,
                    environment_lock=environment_lock,
                    context={**context, "pricing_context": pricing_context},
                    requested_order=requested_order,
                    guardrail_checks=guardrail_checks,
                    preview_payload=None,
                    preview_digest="",
                    expected_phrase="",
                    audit_events=audit_events,
                    lifecycle_result={
                        "status": "blocked",
                        "detail": detail,
                    },
                    frozen_preview_bundle=None,
                )
                return IbkrManualPaperSubmitArtifacts(
                    classification=_blocked_classification_for_mode(config.test_mode),
                    report=report,
                    audit_events=audit_events,
                    open_order_before=context["open_orders_before"],
                    open_order_after_submit=_not_run_snapshot("Submit did not run."),
                    open_order_after_cancel=_not_run_snapshot("Cancel did not run."),
                    artifact_stem=artifact_stem_for_test_mode(config.test_mode),
                    callback_timeline=_build_callback_timeline(runtime),
                )
            preview_payload = _build_submit_preview_payload(
                requested_order=requested_order,
                context={**context, "pricing_context": pricing_context},
                guardrail_checks=guardrail_checks,
                test_mode=config.test_mode,
            )
            preview_digest = build_preview_digest(preview_payload)
            expected_phrase = build_submit_approval_phrase(
                selected_account_id=context["selected_account_id"],
                digest=preview_digest,
                requested_order=requested_order,
                delayed_quote_warning=context["quote_context"].get("live_market_data_warning"),
                test_mode=config.test_mode,
            )
            guardrail_checks.append(
                _guardrail_check(
                    "deterministic_preview_digest_generated",
                    passed=bool(preview_digest),
                    blocking=True,
                    detail="The preview digest is a SHA-256 hash of the deterministic preview payload.",
                )
            )
            _record_audit(
                audit_events,
                event_type="preview_generated",
                config=config,
                classification=None,
                detail="Preview digest generated for the single-order manual paper test.",
                extra={
                    "account_id": context["selected_account_id"],
                    "preview_digest": preview_digest,
                    "quote_source_label": context["quote_context"].get("quote_source_label"),
                    "quote_snapshot": pricing_context.get("quote_snapshot"),
                    "limit_price": requested_order.get("limit_price"),
                    "distance_from_quote": pricing_context.get("distance_from_reference_price"),
                    "distance_ticks": pricing_context.get("distance_ticks"),
                    "test_mode": config.test_mode,
                },
            )
            frozen_preview_bundle = _build_frozen_preview_bundle(
                config=config,
                preview_payload=preview_payload,
                preview_digest=preview_digest,
                expected_phrase=expected_phrase,
                requested_order=requested_order,
                context=context,
                pricing_context=pricing_context,
            )

        if not config.submit:
            classification = _preview_only_classification_for_mode(config.test_mode)
            report = _build_report(
                config=config,
                started_at=started_at,
                classification=classification,
                caller_check=caller_check,
                environment_lock=environment_lock,
                context={**context, "pricing_context": pricing_context},
                requested_order=requested_order,
                guardrail_checks=guardrail_checks,
                preview_payload=preview_payload,
                preview_digest=preview_digest,
                expected_phrase=expected_phrase,
                audit_events=audit_events,
                lifecycle_result={
                    "status": "preview_only",
                    "detail": "Preview-only default prevented submit because no explicit submit flags were provided.",
                },
                frozen_preview_bundle=frozen_preview_bundle,
            )
            return IbkrManualPaperSubmitArtifacts(
                classification=classification,
                report=report,
                audit_events=audit_events,
                open_order_before=context["open_orders_before"],
                open_order_after_submit=_not_run_snapshot("Submit was not requested."),
                open_order_after_cancel=_not_run_snapshot("Cancel was not requested because submit did not run."),
                artifact_stem=artifact_stem_for_test_mode(config.test_mode),
                callback_timeline=_build_callback_timeline(runtime),
            )

        approval = _validate_submit_approval(
            config=config,
            expected_digest=preview_digest,
            expected_phrase=expected_phrase,
            selected_account_id=context["selected_account_id"],
        )
        guardrail_checks.extend(approval["guardrail_checks"])
        _record_audit(
            audit_events,
            event_type="approval_attempted",
            config=config,
            classification=None,
            detail=approval["detail"],
            extra={
                "approved": approval["approved"],
                "preview_digest": preview_digest,
                "approval_phrase_hash": approval.get("approval_phrase_hash"),
            },
        )
        if not approval["approved"]:
            report = _build_report(
                config=config,
                started_at=started_at,
                classification=_blocked_classification_for_mode(config.test_mode),
                caller_check=caller_check,
                environment_lock=environment_lock,
                context={**context, "pricing_context": pricing_context},
                requested_order=requested_order,
                guardrail_checks=guardrail_checks,
                preview_payload=preview_payload,
                preview_digest=preview_digest,
                expected_phrase=expected_phrase,
                audit_events=audit_events,
                lifecycle_result={
                    "status": "approval_blocked",
                    "detail": approval["detail"],
                },
                frozen_preview_bundle=frozen_preview_bundle,
            )
            return IbkrManualPaperSubmitArtifacts(
                classification=_blocked_classification_for_mode(config.test_mode),
                report=report,
                audit_events=audit_events,
                open_order_before=context["open_orders_before"],
                open_order_after_submit=_not_run_snapshot("Submit was blocked by approval validation."),
                open_order_after_cancel=_not_run_snapshot("Cancel was not requested because submit was blocked."),
                artifact_stem=artifact_stem_for_test_mode(config.test_mode),
                callback_timeline=_build_callback_timeline(runtime),
            )

        lifecycle_result = _execute_submit_cancel_lifecycle(
            config=config,
            runtime=runtime,
            context=context,
            requested_order=requested_order,
            sleep_fn=sleep_fn,
            audit_events=audit_events,
            preview_payload=preview_payload,
            preview_digest=preview_digest,
            manual_confirmation_fn=manual_confirmation_fn or _prompt_for_tws_manual_confirmation,
        )
        classification = _classify_submit_lifecycle(config.test_mode, lifecycle_result["status"])
        report_context = {
            **context,
            "pricing_context": pricing_context,
            "errors": list(runtime.collector.errors),
        }
        report = _build_report(
            config=config,
            started_at=started_at,
            classification=classification,
            caller_check=caller_check,
            environment_lock=environment_lock,
            context=report_context,
            requested_order=requested_order,
            guardrail_checks=guardrail_checks,
            preview_payload=preview_payload,
            preview_digest=preview_digest,
            expected_phrase=expected_phrase,
            audit_events=audit_events,
            lifecycle_result=lifecycle_result,
            frozen_preview_bundle=frozen_preview_bundle,
        )
        return IbkrManualPaperSubmitArtifacts(
            classification=classification,
            report=report,
            audit_events=audit_events,
            open_order_before=context["open_orders_before"],
            open_order_after_submit=lifecycle_result["open_order_after_submit"],
            open_order_after_cancel=lifecycle_result["open_order_after_cancel"],
            artifact_stem=artifact_stem_for_test_mode(config.test_mode),
            extra_artifacts={
                "positions_before": context["positions"],
                **_extra_artifacts_from_lifecycle(lifecycle_result),
            },
            callback_timeline=_build_callback_timeline(runtime),
        )
    except Exception as exc:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            requested_order=requested_order,
            caller_check=caller_check,
            environment_lock=environment_lock,
            guardrail_checks=guardrail_checks,
            audit_events=audit_events,
            detail=str(exc),
        )
    finally:
        if runtime is not None:
            try:
                runtime.transport.disconnect()
            except Exception:
                pass


def run_ibkr_order_observation_diagnostic(
    *,
    config: IbkrManualPaperSubmitConfig,
    transport_factory: Callable[..., Any] = IbkrManualPaperSubmitTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    started_at = datetime.now(timezone.utc)
    audit_events: list[dict[str, Any]] = []
    requested_order = _normalize_requested_order(config)
    caller_check = evaluate_manual_preview_caller(caller_path=config.caller_path, stack_provider=stack_provider)
    environment_lock = evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port)
    guardrail_checks = _preflight_guardrail_checks(
        caller_check=caller_check,
        environment_lock=environment_lock,
        input_guardrails={},
    )
    runtime: _SubmitRuntime | None = None
    try:
        if any(check["blocking"] and not check["passed"] for check in guardrail_checks):
            detail = _first_failed_guardrail_detail(guardrail_checks)
            report = {
                "classification": "IBKR_ORDER_OBSERVATION_BLOCKED",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "started_at": started_at.isoformat(),
                "mode": config.mode,
                "host": config.host,
                "port": config.port,
                "account_id": config.account_id,
                "manual_caller_check": caller_check,
                "environment_lock_check": environment_lock,
                "guardrail_checks": guardrail_checks,
                "diagnosis": {
                    "likely_root_cause": "preflight_guardrail_block",
                    "conclusion": detail,
                },
            }
            return report, []
        runtime = _build_runtime(config=config, transport_factory=transport_factory, module_loader=module_loader)
        runtime.transport.connect()
        _start_runtime(runtime)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error(codes=_SEVERE_CONNECTION_ERROR_CODES) or runtime.collector.latest_error()
            raise IbkrManualPaperSubmitError(
                (
                    f"TWS paper observation diagnostic handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"TWS paper observation diagnostic handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        context = _collect_truth_and_preview_context(
            config=config,
            runtime=runtime,
            sleep_fn=sleep_fn,
            started_at=started_at,
            requested_order=requested_order,
            collect_quote=False,
        )
        open_orders_snapshot = _refresh_open_orders_snapshot(
            runtime=runtime,
            config=config,
            selected_account_id=context["selected_account_id"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        execution_truth = _refresh_execution_truth(
            runtime=runtime,
            config=config,
            selected_account_id=context["selected_account_id"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        positions_after = _refresh_positions_snapshot(
            runtime=runtime,
            config=config,
            selected_account_id=context["selected_account_id"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        callback_timeline = _build_callback_timeline(runtime)
        callback_summary = _count_timeline_entries(callback_timeline, "callback_name")
        request_summary = _count_timeline_entries(callback_timeline, "request_type")
        diagnosis = _diagnose_order_observation(
            callback_summary=callback_summary,
            request_summary=request_summary,
            callback_timeline=callback_timeline,
            report_errors=list(context.get("errors") or []),
            executions_snapshot=execution_truth["executions"],
            completed_orders_snapshot=execution_truth["completed_orders"],
            positions_snapshot=positions_after,
        )
        classification = diagnosis["classification"]
        report = {
            "classification": classification,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at.isoformat(),
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "account_id": context["selected_account_id"],
            "connection_check": context["connection_check"],
            "next_valid_order_id": runtime.session.state.next_valid_order_id,
            "manual_caller_check": caller_check,
            "environment_lock_check": environment_lock,
            "guardrail_checks": guardrail_checks,
            "open_order_snapshot": open_orders_snapshot,
            "positions_snapshot": positions_after,
            "executions_snapshot": execution_truth["executions"],
            "completed_orders_snapshot": execution_truth["completed_orders"],
            "callback_summary": callback_summary,
            "request_summary": request_summary,
            "callback_timeline_event_count": len(callback_timeline),
            "diagnosis": diagnosis,
            "errors": list(context.get("errors") or []),
        }
        return report, callback_timeline
    except Exception as exc:
        callback_timeline = _build_callback_timeline(runtime) if runtime is not None else []
        report = {
            "classification": "IBKR_ORDER_OBSERVATION_BLOCKED",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at.isoformat(),
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "account_id": config.account_id,
            "connection_check": {
                "connected": False if runtime is None else runtime.session.state.connected,
                "client_id": config.client_id,
                "detail": str(exc),
            },
            "manual_caller_check": caller_check,
            "environment_lock_check": environment_lock,
            "guardrail_checks": guardrail_checks,
            "callback_summary": _count_timeline_entries(callback_timeline, "callback_name"),
            "request_summary": _count_timeline_entries(callback_timeline, "request_type"),
            "callback_timeline_event_count": len(callback_timeline),
            "diagnosis": {
                "classification": "IBKR_ORDER_OBSERVATION_BLOCKED",
                "likely_root_cause": "diagnostic_blocked",
                "conclusion": str(exc),
            },
            "errors": [],
        }
        return report, callback_timeline
    finally:
        if runtime is not None:
            try:
                runtime.transport.disconnect()
            except Exception:
                pass


def build_submit_approval_phrase(
    *,
    selected_account_id: str,
    digest: str,
    requested_order: dict[str, Any],
    delayed_quote_warning: str | None,
    test_mode: str,
) -> str:
    delayed_ack = "yes" if str(delayed_quote_warning or "").strip() else "no"
    return (
        "APPROVE IBKR PAPER SUBMIT "
        f"account={selected_account_id} mode=PAPER host=127.0.0.1 port=7497 "
        f"test_mode={str(test_mode or '').strip().upper()} "
        f"contract={requested_order['symbol']} {requested_order['expiry']} "
        f"action={requested_order['action']} qty=1 type=LMT price={requested_order['limit_price']} "
        f"tif=DAY digest={digest} delayed_quote_ack={delayed_ack}"
    )


def _blocked_classification_for_mode(test_mode: str) -> str:
    if str(test_mode or "").strip().upper() == _CLOSE_TEST_MODE:
        return "IBKR_MANUAL_PAPER_CLOSE_TEST_BLOCKED"
    return "IBKR_MANUAL_PAPER_FILL_TEST_BLOCKED" if str(test_mode or "").strip().upper() == _FILL_TEST_MODE else "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def _preview_only_classification_for_mode(test_mode: str) -> str:
    if str(test_mode or "").strip().upper() == _CLOSE_TEST_MODE:
        return "IBKR_MANUAL_PAPER_CLOSE_TEST_PARTIAL"
    return "IBKR_MANUAL_PAPER_FILL_TEST_PARTIAL" if str(test_mode or "").strip().upper() == _FILL_TEST_MODE else "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PARTIAL"


def _passed_classification_for_mode(test_mode: str) -> str:
    if str(test_mode or "").strip().upper() == _CLOSE_TEST_MODE:
        return "IBKR_MANUAL_PAPER_CLOSE_TEST_PASSED"
    return "IBKR_MANUAL_PAPER_FILL_TEST_PASSED" if str(test_mode or "").strip().upper() == _FILL_TEST_MODE else "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PASSED"


def artifact_stem_for_test_mode(test_mode: str) -> str:
    normalized = str(test_mode or "").strip().upper()
    if normalized == _FILL_TEST_MODE:
        return "ibkr_manual_paper_fill_test"
    if normalized == _CLOSE_TEST_MODE:
        return "ibkr_manual_paper_close_test"
    return "ibkr_manual_paper_submit"


def frozen_preview_path_for_config(config: IbkrManualPaperSubmitConfig) -> Path | None:
    if config.frozen_preview_path is not None:
        return Path(config.frozen_preview_path)
    if config.output_dir is None:
        return None
    return Path(config.output_dir) / f"{artifact_stem_for_test_mode(config.test_mode)}_frozen_preview.json"


def _build_frozen_preview_bundle(
    *,
    config: IbkrManualPaperSubmitConfig,
    preview_payload: dict[str, Any],
    preview_digest: str,
    expected_phrase: str,
    requested_order: dict[str, Any],
    context: dict[str, Any],
    pricing_context: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "test_mode": str(config.test_mode or "").strip().upper(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "account_id": context["selected_account_id"],
        "preview_digest": preview_digest,
        "expected_approval_phrase": expected_phrase,
        "preview_payload": preview_payload,
        "requested_order": requested_order,
        "open_order_baseline_digest": context["open_order_baseline_digest"],
        "open_order_before": context["open_orders_before"],
        "contract_report": {
            "qualified_contract_identifier": context["contract_report"].get("qualified_contract_identifier"),
            "qualified_contract": context["contract_report"].get("qualified_contract"),
            "api_contract_details": context["contract_report"].get("api_contract_details"),
        },
        "pricing_context": pricing_context,
    }


def _normalize_requested_order_from_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    payload = dict(bundle.get("requested_order") or {})
    if not payload:
        raise IbkrManualPaperSubmitError("Frozen preview bundle is missing requested_order.")
    return {
        "symbol": str(payload.get("symbol") or "").strip().upper(),
        "expiry": str(payload.get("expiry") or "").strip(),
        "action": str(payload.get("action") or "").strip().upper(),
        "quantity": float(payload.get("quantity") or 0.0),
        "order_type": str(payload.get("order_type") or "").strip().upper(),
        "limit_price": _coerce_float(payload.get("limit_price")),
        "time_in_force": str(payload.get("time_in_force") or "").strip().upper(),
    }


def _frozen_preview_bundle_guardrails(
    *,
    config: IbkrManualPaperSubmitConfig,
    frozen_preview_bundle: dict[str, Any],
) -> list[dict[str, Any]]:
    bundle_test_mode = str(frozen_preview_bundle.get("test_mode") or "").strip().upper()
    bundle_account_id = str(frozen_preview_bundle.get("account_id") or "").strip() or None
    return [
        _guardrail_check(
            "frozen_preview_bundle_present",
            passed=True,
            blocking=True,
            detail="Submit can proceed only from a previously generated frozen preview bundle.",
        ),
        _guardrail_check(
            "frozen_preview_test_mode_matches_submit_mode",
            passed=bundle_test_mode == str(config.test_mode or "").strip().upper(),
            blocking=True,
            detail="The frozen preview bundle test mode must match the current manual harness mode.",
        ),
        _guardrail_check(
            "expected_account_matches_frozen_preview",
            passed=(config.account_id is None or config.account_id == bundle_account_id),
            blocking=True,
            detail="If an expected account is supplied, it must match the frozen preview account id exactly.",
        ),
    ]


def _load_frozen_preview_bundle(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IbkrManualPaperSubmitError(f"Frozen preview bundle at {path} is not a JSON object.")
    preview_payload = payload.get("preview_payload")
    if not isinstance(preview_payload, dict):
        raise IbkrManualPaperSubmitError(f"Frozen preview bundle at {path} is missing preview_payload.")
    expected_digest = str(payload.get("preview_digest") or "").strip()
    if not expected_digest:
        raise IbkrManualPaperSubmitError(f"Frozen preview bundle at {path} is missing preview_digest.")
    recomputed = build_preview_digest(preview_payload)
    if recomputed != expected_digest:
        raise IbkrManualPaperSubmitError(
            f"Frozen preview bundle at {path} no longer matches its stored digest. Expected {expected_digest}, recomputed {recomputed}."
        )
    return payload


def write_ibkr_manual_paper_submit_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrManualPaperSubmitArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    artifact_stem = str(artifacts.artifact_stem or "ibkr_manual_paper_submit")
    (reports_dir / f"{artifact_stem}_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{artifact_stem}_report.md").write_text(
        render_ibkr_manual_paper_submit_markdown(artifacts.report),
        encoding="utf-8",
    )
    (reports_dir / f"{artifact_stem}_open_order_before.json").write_text(
        json.dumps(artifacts.open_order_before, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{artifact_stem}_open_order_after_submit.json").write_text(
        json.dumps(artifacts.open_order_after_submit, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / f"{artifact_stem}_open_order_after_cancel.json").write_text(
        json.dumps(artifacts.open_order_after_cancel, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    frozen_preview = artifacts.report.get("frozen_preview_bundle")
    if frozen_preview is not None:
        (reports_dir / f"{artifact_stem}_frozen_preview.json").write_text(
            json.dumps(frozen_preview, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    for name, payload in dict(artifacts.extra_artifacts or {}).items():
        (reports_dir / f"{artifact_stem}_{name}.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    if artifacts.callback_timeline is not None:
        with (reports_dir / f"{artifact_stem}_callback_timeline.jsonl").open("w", encoding="utf-8") as handle:
            for row in artifacts.callback_timeline:
                handle.write(json.dumps(row, sort_keys=True))
                handle.write("\n")
    with (reports_dir / f"{artifact_stem}_audit.jsonl").open("a", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def write_ibkr_order_observation_diagnostic_artifacts(
    *,
    output_dir: Path,
    report: dict[str, Any],
    callback_timeline: list[dict[str, Any]],
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_order_observation_diagnostic_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_order_observation_diagnostic_report.md").write_text(
        render_ibkr_order_observation_diagnostic_markdown(report),
        encoding="utf-8",
    )
    with (reports_dir / "ibkr_order_callback_timeline.jsonl").open("w", encoding="utf-8") as handle:
        for row in callback_timeline:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def render_ibkr_manual_paper_submit_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    environment = dict(report.get("environment_lock_check") or {})
    preview = dict(report.get("preview") or {})
    lifecycle = dict(report.get("submit_cancel_lifecycle") or {})
    rejection = dict(lifecycle.get("rejection") or {})
    frozen_preview_bundle = dict(report.get("frozen_preview_bundle") or {})
    contract_report = dict(frozen_preview_bundle.get("contract_report") or {})
    qualified_contract = dict(contract_report.get("qualified_contract") or {})
    latest_order_status = dict(lifecycle.get("latest_order_status") or {})
    fill_verification = dict(lifecycle.get("fill_verification") or {})
    positions_after_submit = dict(fill_verification.get("positions_after_submit") or {})
    test_mode = str(preview.get("test_mode") or "").strip().upper()
    if test_mode == _FILL_TEST_MODE:
        title = "# IBKR Manual Paper Fill Test Report"
    elif test_mode == _CLOSE_TEST_MODE:
        title = "# IBKR Manual Paper Close Test Report"
    else:
        title = "# IBKR Manual Paper Submit Report"
    action_scope = "BUY only" if test_mode != _CLOSE_TEST_MODE else "SELL only"
    lines = [
        title,
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- account_id: `{report.get('account_id')}`",
        f"- mode: `{environment.get('configured_mode')}`",
        f"- host: `{environment.get('configured_host')}`",
        f"- port: `{environment.get('configured_port')}`",
        f"- client_id: `{connection.get('client_id')}`",
        "",
        "## Verification Summary",
        "",
        f"- classification: `{report.get('classification')}`",
        "- manual CLI only",
        "- paper only",
        "- one order maximum",
        "- explicitly approved phase-1 futures targets only",
        "- LMT DAY only",
        "- qty = 1 only",
        f"- {action_scope}",
        "- no market orders",
        "- no bracket/OCO",
        "- no strategy linkage",
        "- no unapproved strategy execution",
        "- no scheduler",
        "- environment lock is PAPER / 127.0.0.1 / 7497 only",
        "- live port 7496 fails closed",
        "- IB Gateway ports 4001 and 4002 fail closed",
        "- unknown ports fail closed",
        f"- test mode: `{test_mode}`",
        f"- preview digest: `{preview.get('preview_digest')}`",
        f"- expected approval phrase: `{preview.get('expected_approval_phrase')}`",
        f"- lifecycle status: `{lifecycle.get('status')}`",
        f"- lifecycle detail: {_markdown_scalar(lifecycle.get('detail'))}",
        f"- manual confirmation state: `{lifecycle.get('manual_confirmation', {}).get('state')}`",
        f"- manual confirmation outcome: `{lifecycle.get('manual_confirmation', {}).get('operator_outcome')}`",
        "",
        "## No Strategy Path",
        "",
        "- this harness lives behind a dedicated manual CLI path only",
        "- strategy-style callers fail closed",
        "- no submit-capable object is exposed to ATP/GC paths or any scheduler",
        "",
        "## Preview",
        "",
        f"- delayed quote label: `{preview.get('quote_source_label')}`",
        f"- delayed quote warning: {preview.get('live_market_data_warning')}",
        f"- quote snapshot: `{preview.get('quote_snapshot')}`",
        f"- reference price source: `{preview.get('reference_price_source')}`",
        f"- reference price: `{preview.get('reference_price')}`",
        f"- chosen limit price: `{preview.get('limit_price')}`",
        f"- distance from quote: `{preview.get('distance_from_quote')}`",
        f"- distance in ticks: `{preview.get('distance_ticks')}`",
        f"- pricing label: `{preview.get('pricing_label')}`",
        f"- intended to fill: `{preview.get('intended_to_fill')}`",
        f"- estimated_notional: `{preview.get('estimated_notional')}`",
        f"- estimated_tick_value: `{preview.get('estimated_tick_value')}`",
        "",
        "## Outcome",
        "",
    ]
    if rejection:
        lines.extend(
            [
                f"- rejection code: `{rejection.get('error_code')}`",
                f"- rejection reason: `{rejection.get('reason')}`",
                f"- rejection message: {rejection.get('error_message')}",
                f"- unsupported attribute: `{rejection.get('unsupported_attribute')}`",
                "- no second order was submitted",
                "- no working order was left open",
            ]
        )
        if any(int(row.get("code", 0)) == 10147 for row in list(report.get("errors") or [])):
            lines.append("- cancel 10147 was expected after rejection because no order was working")
        if not list((lifecycle.get("fill_verification") or {}).get("executions_after_submit") or []):
            lines.append("- no MGC execution or position change was attributed to this run")
    elif str(report.get("classification") or "").strip().upper() == "PAPER_ORDER_FILLED":
        lines.extend(
            [
                f"- environment: `{environment.get('configured_mode')} / {environment.get('configured_host')} / {environment.get('configured_port')}`",
                f"- account: `{report.get('account_id')}`",
                f"- client id: `{connection.get('client_id')}`",
                f"- exact qualified contract was used: `MGC {qualified_contract.get('expiry')}` / `conId={qualified_contract.get('con_id')}` / `localSymbol={qualified_contract.get('local_symbol')}`",
                f"- order was `BUY 1 LMT DAY @ {preview.get('limit_price')}`",
                f"- order id: `{lifecycle.get('submitted_order_id')}`",
                f"- perm id: `{lifecycle.get('submitted_perm_id')}`",
                f"- final status: `{fill_verification.get('final_status') or latest_order_status.get('status')}`",
                f"- fill price: `{latest_order_status.get('last_fill_price')}`",
                f"- filled quantity: `{fill_verification.get('filled_quantity')}`",
                "- no EtradeOnly rejection",
                "- no expiry-conflict rejection",
                "- no second order was submitted",
                "- no retry was attempted",
                "- no strategy path was involved",
                "- this proves app-to-IBKR paper submit-and-fill plumbing works",
            ]
        )
        mgc_positions = [
            row
            for row in list(positions_after_submit.get("positions") or [])
            if str(row.get("symbol") or "").strip().upper() == "MGC"
        ]
        if any(str(row.get("quantity")) == "0.0" for row in mgc_positions):
            lines.extend(
                [
                    "- remaining follow-up: execution truth and orderStatus truth are solid",
                    "- immediate post-submit position snapshot still showed MGC quantity 0.0",
                    "- position refresh/reconciliation after fill must be tightened before position state is treated as authoritative immediately after execution",
                ]
            )
    elif str(report.get("classification") or "").strip().upper() == "PAPER_CLOSE_FILLED_FLAT":
        close_position_verification = dict(lifecycle.get("close_position_verification") or {})
        lines.extend(
            [
                f"- environment: `{environment.get('configured_mode')} / {environment.get('configured_host')} / {environment.get('configured_port')}`",
                f"- account: `{report.get('account_id')}`",
                f"- client id: `{connection.get('client_id')}`",
                f"- exact qualified contract was used: `MGC {qualified_contract.get('expiry')}` / `conId={qualified_contract.get('con_id')}` / `localSymbol={qualified_contract.get('local_symbol')}`",
                f"- order was `SELL 1 LMT DAY @ {preview.get('limit_price')}`",
                f"- order id: `{lifecycle.get('submitted_order_id')}`",
                f"- perm id: `{lifecycle.get('submitted_perm_id')}`",
                f"- final status: `{fill_verification.get('final_status') or latest_order_status.get('status')}`",
                f"- last fill price: `{latest_order_status.get('last_fill_price')}`",
                f"- filled quantity: `{fill_verification.get('filled_quantity')}`",
                f"- post-close MGC quantity: `{close_position_verification.get('exact_position_quantity')}`",
                "- the confirmed long MGC paper position was flattened through broker truth",
                "- no second order was submitted",
                "- no retry was attempted",
                "- no strategy path was involved",
                "- execution list includes prior MGC rows, so close truth is anchored to `orderStatus=Filled`, `permId=490708935`, and the reconciled flat position",
                "- app-path BUY open filled",
                "- long position reconciled",
                "- app-path SELL close filled",
                "- flat position reconciled",
                "- manual paper open-close loop is proven",
            ]
        )
    lines.extend(
        [
            "",
        "## Guardrails",
        "",
        ]
    )
    for check in list(report.get("guardrail_checks") or []):
        lines.append(
            f"- {check.get('name')}: `{'PASS' if check.get('passed') else 'FAIL'}`"
            f" (blocking={check.get('blocking')}) - {check.get('detail')}"
        )
    lines.extend(
        [
            "",
            "## Audit",
            "",
            f"- audit_event_count: `{report.get('audit_event_count')}`",
            "",
        ]
    )
    return "\n".join(lines)


def _markdown_scalar(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        if len(value) == 1:
            return str(value[0])
        return ", ".join(str(item) for item in value)
    return str(value)


def render_ibkr_order_observation_diagnostic_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    diagnosis = dict(report.get("diagnosis") or {})
    callback_summary = dict(report.get("callback_summary") or {})
    request_summary = dict(report.get("request_summary") or {})
    lines = [
        "# IBKR Order Observation Diagnostic Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- account_id: `{report.get('account_id')}`",
        f"- mode/host/port: `{report.get('mode')} / {report.get('host')} / {report.get('port')}`",
        f"- client_id: `{connection.get('client_id')}`",
        f"- next_valid_order_id: `{report.get('next_valid_order_id')}`",
        f"- callback_timeline_events: `{report.get('callback_timeline_event_count')}`",
        "",
        "## Diagnosis",
        "",
        f"- likely_root_cause: `{diagnosis.get('likely_root_cause')}`",
        f"- conclusion: {diagnosis.get('conclusion')}",
        "",
        "## Callback Summary",
        "",
    ]
    for name in sorted(callback_summary):
        lines.append(f"- {name}: `{callback_summary.get(name)}`")
    lines.extend(["", "## Request Summary", ""])
    for name in sorted(request_summary):
        lines.append(f"- {name}: `{request_summary.get(name)}`")
    return "\n".join(lines)


def _submit_input_guardrails(
    requested_order: dict[str, Any],
    *,
    test_mode: str,
    require_limit_price: bool,
) -> dict[str, dict[str, Any]]:
    normalized_mode = str(test_mode or "").strip().upper()
    expected_action = "SELL" if normalized_mode == _CLOSE_TEST_MODE else _EXPECTED_ACTION
    expected_target = _phase1_target_for_requested_order(requested_order)
    expected_symbol = str(expected_target.get("symbol") or "").strip().upper()
    expected_expiry = str(expected_target.get("contract_month") or "").strip()
    action_detail = (
        "Only SELL is allowed in the manual paper close harness."
        if normalized_mode == _CLOSE_TEST_MODE
        else "Only BUY is allowed in the first manual paper submit/cancel harness."
    )
    return {
        "whitelisted_contract": {
            "passed": bool(expected_target)
            and str(requested_order.get("symbol") or "").strip().upper() == expected_symbol
            and str(requested_order.get("expiry") or "").strip() == expected_expiry,
            "detail": (
                f"Only the approved phase-1 execution target {expected_symbol} {expected_expiry} is allowed in the manual paper submit/cancel harness."
                if expected_target
                else "Only explicitly approved phase-1 execution targets are allowed in the manual paper submit/cancel harness."
            ),
        },
        "supported_action": {
            "passed": requested_order.get("action") == expected_action,
            "detail": action_detail,
        },
        "quantity_cap": {
            "passed": float(requested_order.get("quantity") or 0.0) == _EXPECTED_QUANTITY,
            "detail": "Quantity must equal exactly one contract.",
        },
        "limit_only_order_type": {
            "passed": str(requested_order.get("order_type") or "").strip().upper() == _EXPECTED_ORDER_TYPE,
            "detail": "Only LMT orders are allowed in the first manual paper submit/cancel harness.",
        },
        "limit_price_present": {
            "passed": (requested_order.get("limit_price") is not None) if require_limit_price else True,
            "detail": (
                "A limit price is required for the first manual paper submit/cancel harness."
                if str(test_mode).upper() != _FILL_TEST_MODE
                else "A limit price must be derivable from the frozen fill-test preview payload before submit."
            ),
        },
        "limit_price_positive": {
            "passed": (
                requested_order.get("limit_price") is not None and float(requested_order.get("limit_price")) > 0.0
            )
            if require_limit_price
            else True,
            "detail": "The limit price must be positive.",
        },
        "time_in_force_supported": {
            "passed": str(requested_order.get("time_in_force") or "").strip().upper() == _EXPECTED_TIF,
            "detail": "Only DAY time-in-force is allowed in the first manual paper submit/cancel harness.",
        },
    }


def _prepare_frozen_submit_context(
    *,
    config: IbkrManualPaperSubmitConfig,
    context: dict[str, Any],
    requested_order: dict[str, Any],
    frozen_preview_bundle: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str, str, dict[str, Any]]:
    bundle_requested_order = _normalize_requested_order_from_bundle(frozen_preview_bundle)
    preview_payload = dict(frozen_preview_bundle.get("preview_payload") or {})
    preview_digest = str(frozen_preview_bundle.get("preview_digest") or "").strip()
    expected_phrase = str(frozen_preview_bundle.get("expected_approval_phrase") or "").strip()
    pricing_context = dict(frozen_preview_bundle.get("pricing_context") or {})
    frozen_account_id = str(frozen_preview_bundle.get("account_id") or "").strip()
    if context["selected_account_id"] != frozen_account_id:
        raise IbkrManualPaperSubmitError(
            f"Frozen preview bundle account {frozen_account_id} does not match the connected broker account {context['selected_account_id']}."
        )
    live_contract_identifier = context["contract_report"].get("qualified_contract_identifier")
    frozen_contract_identifier = (frozen_preview_bundle.get("contract_report") or {}).get("qualified_contract_identifier")
    if frozen_contract_identifier is not None and live_contract_identifier is not None and frozen_contract_identifier != live_contract_identifier:
        raise IbkrManualPaperSubmitError(
            "The currently qualified MGC contract does not match the frozen preview bundle. Generate a new preview before submitting."
        )
    context = {
        **context,
        "quote_context": dict(preview_payload.get("quote_context") or {}),
        "open_order_baseline_digest": str(frozen_preview_bundle.get("open_order_baseline_digest") or ""),
        "frozen_preview_bundle": frozen_preview_bundle,
    }
    if not context["open_order_baseline_digest"]:
        raise IbkrManualPaperSubmitError("Frozen preview bundle is missing the open-order baseline digest.")
    _ = requested_order
    _ = config
    return context, bundle_requested_order, preview_payload, preview_digest, expected_phrase, pricing_context


def _resolve_requested_order_for_preview(
    *,
    config: IbkrManualPaperSubmitConfig,
    requested_order: dict[str, Any],
    contract_report: dict[str, Any],
    quote_context: dict[str, Any],
) -> dict[str, Any]:
    resolved = dict(requested_order)
    normalized_mode = str(config.test_mode or "").strip().upper()
    if normalized_mode not in {_RESTING_TEST_MODE, _FILL_TEST_MODE, _CLOSE_TEST_MODE}:
        raise IbkrManualPaperSubmitError(f"Unsupported manual paper test mode: {config.test_mode}")
    if normalized_mode in {_FILL_TEST_MODE, _CLOSE_TEST_MODE}:
        if resolved.get("limit_price") is None:
            resolved["limit_price"] = _derive_marketable_limit_price(
                quote_context=quote_context,
                contract_report=contract_report,
                offset_ticks=config.fill_limit_offset_ticks,
                test_mode=normalized_mode,
                action=str(resolved.get("action") or "").strip().upper(),
            )
    return resolved


def _derive_marketable_limit_price(
    *,
    quote_context: dict[str, Any],
    contract_report: dict[str, Any],
    offset_ticks: float,
    test_mode: str,
    action: str,
) -> float:
    normalized_mode = str(test_mode or "").strip().upper()
    normalized_action = str(action or "").strip().upper()
    if normalized_mode == _CLOSE_TEST_MODE or normalized_action == "SELL":
        reference_price, _ = _select_close_reference_price(quote_context)
        missing_detail = "PAPER_CLOSE_TEST requires a delayed bid/last reference price."
    else:
        reference_price, _ = _select_fill_reference_price(quote_context)
        missing_detail = "PAPER_FILL_TEST requires a delayed ask/last reference price."
    if reference_price is None:
        raise IbkrManualPaperSubmitError(missing_detail)
    min_tick = _contract_min_tick(contract_report)
    if min_tick is None or min_tick <= 0.0:
        raise IbkrManualPaperSubmitError("The manual paper marketable-limit tests require a valid contract minTick.")
    if normalized_mode == _CLOSE_TEST_MODE or normalized_action == "SELL":
        limit_price = float(reference_price) - max(1.0, float(offset_ticks)) * float(min_tick)
    else:
        limit_price = float(reference_price) + max(1.0, float(offset_ticks)) * float(min_tick)
    return _round_price_to_tick(limit_price, min_tick)


def _contract_min_tick(contract_report: dict[str, Any]) -> float | None:
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    return _coerce_float(contract_details.get("min_tick"))


def _round_price_to_tick(price: float, min_tick: float) -> float:
    if min_tick <= 0.0:
        return float(price)
    ticks = round(float(price) / float(min_tick))
    return round(ticks * float(min_tick), 8)


def _build_delayed_quote_pricing_context(
    *,
    config: IbkrManualPaperSubmitConfig,
    requested_order: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    quote_context = dict(context.get("quote_context") or {})
    contract_report = dict(context.get("contract_report") or {})
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    min_tick = _coerce_float(contract_details.get("min_tick"))
    limit_price = _coerce_float(requested_order.get("limit_price"))
    normalized_mode = str(config.test_mode).upper()
    normalized_action = str(requested_order.get("action") or "").strip().upper()
    if normalized_mode == _FILL_TEST_MODE:
        reference_price, reference_source = _select_fill_reference_price(quote_context)
        price_relation = "above_reference"
        distance_from_reference_price = (
            None
            if limit_price is None or reference_price is None
            else float(limit_price) - float(reference_price)
        )
        pricing_label = _MARKETABLE_LIMIT_LABEL
        intended_to_fill = True
    elif normalized_mode == _CLOSE_TEST_MODE or normalized_action == "SELL":
        reference_price, reference_source = _select_close_reference_price(quote_context)
        price_relation = "below_reference"
        distance_from_reference_price = (
            None
            if limit_price is None or reference_price is None
            else float(reference_price) - float(limit_price)
        )
        pricing_label = _CLOSE_MARKETABLE_LIMIT_LABEL
        intended_to_fill = True
    else:
        reference_price, reference_source = _select_delayed_reference_price(quote_context)
        price_relation = "below_reference"
        distance_from_reference_price = (
            None
            if limit_price is None or reference_price is None
            else float(reference_price) - float(limit_price)
        )
        pricing_label = _NON_MARKETABLE_LIMIT_LABEL
        intended_to_fill = False
    quote_updated_at = _parse_iso_timestamp(quote_context.get("updated_at"))
    quote_age_seconds = (
        max(0.0, (datetime.now(timezone.utc) - quote_updated_at).total_seconds())
        if quote_updated_at is not None
        else None
    )
    distance_ticks = (
        None
        if distance_from_reference_price is None or min_tick in (None, 0.0)
        else float(distance_from_reference_price) / float(min_tick)
    )
    return {
        "quote_snapshot": {
            "source_label": quote_context.get("quote_source_label"),
            "updated_at": quote_context.get("updated_at"),
            "quote_age_seconds": quote_age_seconds,
            "bid_price": quote_context.get("bid_price"),
            "ask_price": quote_context.get("ask_price"),
            "last_price": quote_context.get("last_price"),
            "close_price": quote_context.get("close_price"),
            "response_indication": quote_context.get("response_indication"),
        },
        "reference_price": reference_price,
        "reference_price_source": reference_source,
        "distance_from_reference_price": distance_from_reference_price,
        "distance_ticks": distance_ticks,
        "price_relation": price_relation,
        "pricing_label": pricing_label,
        "intended_to_fill": intended_to_fill,
        "min_tick": min_tick,
        "max_distance_ticks": float(config.near_market_max_distance_ticks),
        "max_quote_age_seconds": float(config.delayed_quote_max_age_seconds),
    }


def _delayed_quote_pricing_guardrails(pricing_context: dict[str, Any]) -> list[dict[str, Any]]:
    quote_snapshot = dict(pricing_context.get("quote_snapshot") or {})
    quote_source = str(quote_snapshot.get("source_label") or "").strip().upper()
    delayed_quote_available = quote_source in {"DELAYED", "DELAYED_FROZEN"}
    quote_age_seconds = pricing_context.get("quote_snapshot", {}).get("quote_age_seconds")
    distance_from_reference_price = pricing_context.get("distance_from_reference_price")
    distance_ticks = pricing_context.get("distance_ticks")
    max_distance_ticks = pricing_context.get("max_distance_ticks")
    checks = [
        _guardrail_check(
            "delayed_quote_available",
            passed=delayed_quote_available and pricing_context.get("reference_price") is not None,
            blocking=True,
            detail="The first manual paper submit/cancel test requires a delayed bid or last quote snapshot. If delayed quote data is unavailable, fail closed rather than guessing a price.",
        ),
        _guardrail_check(
            "delayed_quote_fresh",
            passed=quote_age_seconds is not None and float(quote_age_seconds) <= float(pricing_context.get("max_quote_age_seconds") or 0.0),
            blocking=True,
            detail=(
                "The delayed quote snapshot must be fresh before preview. If the delayed quote is stale, fail closed rather than guessing a price."
            ),
        ),
    ]
    if pricing_context.get("intended_to_fill"):
        detail = (
            "For PAPER_FILL_TEST BUY 1 MGC 202606 LMT DAY, the chosen limit must be slightly above the delayed ask/last so it is a marketable limit intended to fill in paper, while remaining near-market rather than far away."
            if str(pricing_context.get("pricing_label") or "").strip().upper() != _CLOSE_MARKETABLE_LIMIT_LABEL
            else "For PAPER_CLOSE_TEST SELL 1 MGC 202606 LMT DAY, the chosen limit must be slightly below the delayed bid/last so it is a marketable limit intended to fill in paper while remaining near-market rather than far away."
        )
        checks.append(
            _guardrail_check(
                "marketable_limit_intended_to_fill",
                passed=(
                    distance_from_reference_price is not None
                    and float(distance_from_reference_price) > 0.0
                    and (
                        distance_ticks is None
                        or (
                            float(distance_ticks) >= (1.0 - _TICK_COMPARISON_EPSILON)
                            and float(distance_ticks) <= (float(max_distance_ticks or 0.0) + _TICK_COMPARISON_EPSILON)
                        )
                    )
                ),
                blocking=True,
                detail=detail,
            )
        )
    else:
        checks.append(
            _guardrail_check(
                "near_market_non_marketable_buy_limit",
                passed=(
                    distance_from_reference_price is not None
                    and float(distance_from_reference_price) > 0.0
                    and (
                        distance_ticks is None
                        or (
                            float(distance_ticks) >= (1.0 - _TICK_COMPARISON_EPSILON)
                            and float(distance_ticks) <= (float(max_distance_ticks or 0.0) + _TICK_COMPARISON_EPSILON)
                        )
                    )
                ),
                blocking=True,
                detail=(
                    "For BUY 1 MGC 202606 LMT DAY, the chosen limit must be slightly below the current delayed bid/last, close enough to be accepted by TWS, and not a far-away placeholder limit."
                ),
            )
        )
    return checks


def _select_delayed_reference_price(quote_context: dict[str, Any]) -> tuple[float | None, str | None]:
    bid_price = _coerce_float(quote_context.get("bid_price"))
    if bid_price is not None:
        return bid_price, "bid_price"
    last_price = _coerce_float(quote_context.get("last_price"))
    if last_price is not None:
        return last_price, "last_price"
    return None, None


def _select_fill_reference_price(quote_context: dict[str, Any]) -> tuple[float | None, str | None]:
    ask_price = _coerce_float(quote_context.get("ask_price"))
    if ask_price is not None:
        return ask_price, "ask_price"
    last_price = _coerce_float(quote_context.get("last_price"))
    if last_price is not None:
        return last_price, "last_price"
    return None, None


def _select_close_reference_price(quote_context: dict[str, Any]) -> tuple[float | None, str | None]:
    bid_price = _coerce_float(quote_context.get("bid_price"))
    if bid_price is not None:
        return bid_price, "bid_price"
    last_price = _coerce_float(quote_context.get("last_price"))
    if last_price is not None:
        return last_price, "last_price"
    return None, None


def _exact_contract_position_quantity(*, positions_snapshot: dict[str, Any], contract_report: dict[str, Any]) -> float | None:
    qualified_contract = dict(contract_report.get("qualified_contract") or {})
    expected_symbol = str(qualified_contract.get("broker_symbol") or _EXPECTED_SYMBOL).strip().upper()
    expected_expiry = str(qualified_contract.get("expiry") or "").strip()
    expected_local_symbol = str(qualified_contract.get("local_symbol") or "").strip().upper()
    for row in list(positions_snapshot.get("positions") or []):
        symbol = str(row.get("symbol") or "").strip().upper()
        expiry = str(row.get("expiry") or "").strip()
        local_symbol = str(row.get("local_symbol") or "").strip().upper()
        if symbol != expected_symbol:
            continue
        if expected_expiry and expiry != expected_expiry:
            continue
        if expected_local_symbol and local_symbol != expected_local_symbol:
            continue
        return _coerce_float(row.get("quantity"))
    return None


def _parse_iso_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _preflight_guardrail_checks(
    *,
    caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    input_guardrails: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    checks = [
        _guardrail_check(
            "manual_cli_only",
            passed=caller_check["passed"],
            blocking=True,
            detail=caller_check["detail"],
        ),
        _guardrail_check(
            "paper_tws_environment_lock",
            passed=environment_lock["passed"],
            blocking=True,
            detail=environment_lock["port_policy"],
        ),
    ]
    for name, payload in input_guardrails.items():
        checks.append(
            _guardrail_check(
                name,
                passed=bool(payload["passed"]),
                blocking=True,
                detail=str(payload["detail"]),
            )
        )
    checks.append(
        _guardrail_check(
            "manual_submit_path_is_preview_first_and_approval_gated",
            passed=True,
            blocking=True,
            detail="The only submit path in this harness is manual-only, preview-first, and requires the exact digest plus typed phrase.",
        )
    )
    return checks


def _build_runtime(
    *,
    config: IbkrManualPaperSubmitConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _SubmitRuntime:
    session = IbkrSession(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        gateway_mode="paper",
        read_only=False,
        order_id_policy=build_default_ibkr_order_id_policy(
            client_id=config.client_id,
            live_orders_enabled=False,
        ),
    )
    client = IbkrClient(session=session)
    collector = IbkrManualPaperSubmitCollector(client)
    transport = transport_factory(
        client=client,
        collector=collector,
        config=IbkrReadOnlyApiTransportConfig(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            read_only=False,
        ),
        module_loader=module_loader,
    )
    return _SubmitRuntime(
        session=session,
        client=client,
        collector=collector,
        transport=transport,
    )


def _start_runtime(runtime: _SubmitRuntime) -> None:
    thread = threading.Thread(target=runtime.transport.run_loop, name="ibkr_manual_paper_submit_loop", daemon=True)
    thread.start()


def _collect_truth_and_preview_context(
    *,
    config: IbkrManualPaperSubmitConfig,
    runtime: _SubmitRuntime,
    sleep_fn: Callable[[float], None],
    started_at: datetime,
    requested_order: dict[str, Any],
    collect_quote: bool = True,
) -> dict[str, Any]:
    audit_events: list[dict[str, Any]] = []
    runtime.client.request_managed_accounts()
    runtime.transport.req_managed_accounts()
    if not _wait_for_event(
        runtime.collector.managed_accounts_ready,
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    ):
        latest_error = runtime.collector.latest_error()
        raise IbkrManualPaperSubmitError(
            (
                f"TWS paper API did not expose managed accounts: {latest_error['message']}"
                if latest_error is not None
                else f"TWS paper API did not expose managed accounts within {config.timeout_seconds:.1f}s."
            )
        )
    selected_account_id = _resolve_selected_account_id(
        requested_account_id=config.account_id,
        managed_accounts=runtime.client.connection_state().managed_accounts,
    )
    runtime.transport.req_account_summary(request_id=9301, group_name="All", tags=_ACCOUNT_SUMMARY_TAGS)
    runtime.client.request_balances()
    runtime.transport.req_account_updates(subscribe=True, account_id=selected_account_id)
    runtime.client.request_positions()
    runtime.transport.req_positions()
    runtime.client.request_open_orders()
    runtime.collector.reset_open_orders_ready()
    runtime.transport.req_all_open_orders()
    events_ok = all(
        (
            _wait_for_event(
                runtime.collector.account_summary_event(9301),
                collector=runtime.collector,
                timeout_seconds=config.timeout_seconds,
                sleep_fn=sleep_fn,
            ),
            _wait_for_event(
                runtime.collector.balances_ready,
                collector=runtime.collector,
                timeout_seconds=config.timeout_seconds,
                sleep_fn=sleep_fn,
            ),
            _wait_for_event(
                runtime.collector.positions_ready,
                collector=runtime.collector,
                timeout_seconds=config.timeout_seconds,
                sleep_fn=sleep_fn,
            ),
            _wait_for_event(
                runtime.collector.open_orders_ready,
                collector=runtime.collector,
                timeout_seconds=config.timeout_seconds,
                sleep_fn=sleep_fn,
            ),
        )
    )
    try:
        runtime.transport.req_account_updates(subscribe=False, account_id=selected_account_id)
    except Exception:
        pass
    try:
        runtime.transport.cancel_account_summary(request_id=9301)
    except Exception:
        pass
    if not events_ok:
        latest_error = runtime.collector.latest_error()
        raise IbkrManualPaperSubmitError(
            (
                f"Timed out waiting for account summary / positions / open orders within {config.timeout_seconds:.1f}s."
                if latest_error is None
                else f"IBKR callback error {latest_error['code']}: {latest_error['message']}"
            )
        )
    if runtime.session.state.account_id != selected_account_id:
        runtime.session.select_account(selected_account_id)
    provider = IbkrExecutionProvider(config.repo_root, session=runtime.session, client=runtime.client)
    snapshot = provider.snapshot_state(force_refresh=True)
    account_truth = _build_account_truth_snapshot(
        config=_read_only_config_from_submit(config),
        collector=runtime.collector,
        session=runtime.session,
        snapshot=snapshot,
        selected_account_id=selected_account_id,
    )
    read_only_config = _read_only_config_from_submit(config)
    positions = _build_positions_snapshot(config=read_only_config, client=runtime.client, selected_account_id=selected_account_id)
    open_orders_before = _build_open_orders_snapshot(config=read_only_config, client=runtime.client, selected_account_id=selected_account_id)
    if open_orders_before.get("open_order_count"):
        expected_symbol = str(requested_order.get("symbol") or "").strip().upper()
        mgc_rows = [
            row
            for row in list(open_orders_before.get("open_orders") or [])
            if str(row.get("symbol") or "").strip().upper() == expected_symbol
        ]
        if mgc_rows:
            raise IbkrManualPaperSubmitError(
                f"Open-order baseline contains working {expected_symbol} orders. Cancel them manually in TWS before rerunning the manual paper submit/cancel test."
            )
        raise IbkrManualPaperSubmitError(
            f"Open-order baseline is not empty for the first manual submit test: {open_orders_before['open_order_count']} existing open orders."
        )
    contract_report = _qualify_futures_contract(
        transport=runtime.transport,
        collector=runtime.collector,
        symbol=str(requested_order.get('symbol') or '').strip().upper(),
        expiry=str(requested_order.get('expiry') or '').strip(),
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if not contract_report["ok"]:
        raise IbkrManualPaperSubmitError(str(contract_report["detail"]))
    quote_context = (
        _probe_delayed_quote_context(
            transport=runtime.transport,
            collector=runtime.collector,
            contract=contract_report["qualified_contract_object"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        if collect_quote
        else {}
    )
    audit_events.extend(
        [
            _audit_row("account_truth_read", config=config, detail="Account truth captured.", extra={"account_id": selected_account_id}),
            _audit_row("positions_read", config=config, detail="Current positions captured.", extra={"position_count": positions.get("position_count")}),
            _audit_row("open_order_baseline_read", config=config, detail="Open-order baseline captured.", extra={"open_order_count": open_orders_before.get("open_order_count")}),
            _audit_row(
                "contract_qualified",
                config=config,
                detail=f"{str(requested_order.get('symbol') or '').strip().upper()} {str(requested_order.get('expiry') or '').strip()} qualified successfully.",
                extra={"qualified_contract_identifier": contract_report.get("qualified_contract_identifier")},
            ),
        ]
    )
    if str(config.test_mode or "").strip().upper() == _CLOSE_TEST_MODE:
        exact_quantity = _exact_contract_position_quantity(positions_snapshot=positions, contract_report=contract_report)
        if exact_quantity != 1.0:
            raise IbkrManualPaperSubmitError(
                "Manual paper close test requires an exact current long MGC position quantity of 1.0 before preview."
            )
        audit_events.append(
            _audit_row(
                "exact_long_position_confirmed",
                config=config,
                detail="Exact long MGC position confirmed before close preview.",
                extra={"quantity": exact_quantity},
            )
        )
    return {
        "selected_account_id": selected_account_id,
        "connection_check": {
            "connected": runtime.session.state.connected,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "server_version": runtime.transport.server_version(),
            "tws_connection_time": runtime.transport.tws_connection_time(),
            "connection_timestamp": (
                runtime.session.state.connected_at.isoformat()
                if runtime.session.state.connected_at is not None
                else started_at.isoformat()
            ),
        },
        "account_truth": account_truth,
        "positions": positions,
        "open_orders_before": open_orders_before,
        "contract_report": contract_report,
        "quote_context": quote_context,
        "errors": list(runtime.collector.errors),
        "audit_events": audit_events,
        "open_order_baseline_digest": _snapshot_digest(open_orders_before),
    }


def _qualify_mgc_contract(
    *,
    transport: IbkrManualPaperSubmitTransport,
    collector: IbkrManualPaperSubmitCollector,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    return _qualify_futures_contract(
        transport=transport,
        collector=collector,
        symbol=_EXPECTED_SYMBOL,
        expiry=_EXPECTED_EXPIRY,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )


def _qualify_futures_contract(
    *,
    transport: IbkrManualPaperSubmitTransport,
    collector: IbkrManualPaperSubmitCollector,
    symbol: str,
    expiry: str,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    resolver = IbkrContractResolver()
    qualified = resolver.qualify_futures(symbol=symbol, expiry=expiry)
    request_id = 9501 + (abs(hash((str(symbol).upper(), str(expiry)))) % 200)
    event = collector.contract_details_event(request_id)
    transport.req_contract_details(request_id=request_id, contract=qualified)
    event_ok = _wait_for_event(
        event,
        collector=collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    contract_rows = list(collector.contract_details_rows.get(request_id, []))
    latest_error = next(
        (
            row
            for row in reversed(collector.errors)
            if row.get("request_id") == request_id
        ),
        None,
    )
    ok = bool(event_ok and contract_rows)
    api_details = contract_rows[0] if contract_rows else {}
    if api_details:
        qualified = _qualified_contract_with_api_details(qualified, api_details)
    qualified_dict = _qualified_contract_to_dict(qualified)
    return {
        "ok": ok,
        "detail": (
            "Contract details received from TWS."
            if ok
            else (
                f"IBKR error {latest_error['code']}: {latest_error['message']}"
                if latest_error is not None
                else f"No contract details returned within {timeout_seconds:.1f}s."
            )
        ),
        "qualified_contract": qualified_dict,
        "qualified_contract_object": qualified,
        "qualified_contract_identifier": api_details.get("con_id") or qualified.local_symbol,
        "api_contract_details": contract_rows,
    }


def _qualified_contract_with_api_details(
    qualified: IbkrQualifiedContract,
    api_details: dict[str, Any],
) -> IbkrQualifiedContract:
    exact_expiry = str(api_details.get("expiry") or qualified.expiry or "").strip() or qualified.expiry
    local_symbol = str(api_details.get("local_symbol") or qualified.local_symbol or "").strip() or qualified.local_symbol
    exchange = str(api_details.get("exchange") or qualified.exchange or "").strip() or qualified.exchange
    currency = str(api_details.get("currency") or qualified.currency or "").strip() or qualified.currency
    multiplier = str(api_details.get("multiplier") or qualified.multiplier or "").strip() or qualified.multiplier
    trading_class = str(api_details.get("trading_class") or qualified.trading_class or "").strip() or qualified.trading_class
    con_id = api_details.get("con_id") if api_details.get("con_id") is not None else qualified.con_id
    return IbkrQualifiedContract(
        internal_symbol=qualified.internal_symbol,
        broker_symbol=qualified.broker_symbol,
        local_symbol=local_symbol,
        security_type=qualified.security_type,
        exchange=exchange,
        currency=currency,
        expiry=exact_expiry,
        multiplier=multiplier,
        trading_class=trading_class,
        con_id=con_id,
        metadata=qualified.metadata,
    )


def _probe_delayed_quote_context(
    *,
    transport: IbkrManualPaperSubmitTransport,
    collector: IbkrManualPaperSubmitCollector,
    contract: IbkrQualifiedContract,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    live_probe = _request_market_data_snapshot(
        transport=transport,
        collector=collector,
        contract=contract,
        request_id=9601,
        market_data_type=1,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    delayed_probe = _request_market_data_snapshot(
        transport=transport,
        collector=collector,
        contract=contract,
        request_id=9602,
        market_data_type=3,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    frozen_probe = _request_market_data_snapshot(
        transport=transport,
        collector=collector,
        contract=contract,
        request_id=9603,
        market_data_type=2,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    delayed_frozen_probe = _request_market_data_snapshot(
        transport=transport,
        collector=collector,
        contract=contract,
        request_id=9604,
        market_data_type=4,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    live_market_data_available = bool(
        live_probe.get("any_tick_returned") and live_probe.get("response_indication") == "data_returned"
    )
    active_label = "LIVE"
    active_probe = live_probe
    if delayed_probe.get("any_tick_returned"):
        active_label = "DELAYED"
        active_probe = delayed_probe
    elif delayed_frozen_probe.get("any_tick_returned"):
        active_label = "DELAYED_FROZEN"
        active_probe = delayed_frozen_probe
    elif frozen_probe.get("any_tick_returned"):
        active_label = "FROZEN"
        active_probe = frozen_probe
    warning = None
    if not live_market_data_available:
        if active_label == "DELAYED_FROZEN":
            warning = "Live market data is unavailable in this paper session. The preview uses delayed-frozen data only."
        elif active_label == "FROZEN":
            warning = "Live market data is unavailable in this paper session. The preview uses frozen quote data only."
        else:
            warning = "Live market data is unavailable in this paper session. The preview uses delayed data only."
    return {
        "live_probe": live_probe,
        "delayed_probe": delayed_probe,
        "frozen_probe": frozen_probe,
        "delayed_frozen_probe": delayed_frozen_probe,
        "has_quote": bool(active_probe.get("any_tick_returned")),
        "quote_source_label": active_label if active_probe.get("any_tick_returned") else "UNAVAILABLE",
        "live_market_data_available": live_market_data_available,
        "live_market_data_warning": warning,
        "bid_price": active_probe.get("bid_price"),
        "ask_price": active_probe.get("ask_price"),
        "last_price": active_probe.get("last_price"),
        "close_price": active_probe.get("close_price"),
        "updated_at": active_probe.get("updated_at"),
        "response_indication": active_probe.get("response_indication"),
        "delayed_data_warning_present": bool(warning) or bool(delayed_probe.get("any_tick_returned")) or bool(delayed_frozen_probe.get("any_tick_returned")),
    }


def _request_market_data_snapshot(
    *,
    transport: IbkrManualPaperSubmitTransport,
    collector: IbkrManualPaperSubmitCollector,
    contract: IbkrQualifiedContract,
    request_id: int,
    market_data_type: int,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    transport.req_market_data_type(market_data_type=market_data_type)
    event = collector.market_data_event(request_id)
    transport.req_market_data_snapshot(request_id=request_id, contract=contract)
    event_ok = _wait_for_event(
        event,
        collector=collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    raw_row = dict(collector.market_data_rows.get(request_id, _empty_market_data_row(request_id)))
    errors = [row for row in collector.errors if row.get("request_id") == request_id]
    quote_fields = _extract_quote_fields(raw_row)
    permission_error = next(
        (
            error
            for error in reversed(errors)
            if int(error.get("code", 0)) in _PERMISSION_ERROR_CODES
        ),
        None,
    )
    if quote_fields["any_tick_returned"]:
        indication = "delayed_only" if raw_row.get("market_data_type") in {3, 4} else "data_returned"
        response_code = None
        response_message = None
    elif permission_error is not None:
        indication = "no_permission" if int(permission_error["code"]) != 10167 else "delayed_only"
        response_code = int(permission_error["code"])
        response_message = permission_error["message"]
    elif event_ok:
        indication = "unavailable"
        response_code = None
        response_message = "Snapshot completed without quote ticks."
    else:
        indication = "unavailable"
        response_code = None
        response_message = f"Snapshot did not complete within {timeout_seconds:.1f}s."
    return {
        "request_id": request_id,
        "market_data_type_requested": market_data_type,
        "market_data_type_reported": raw_row.get("market_data_type"),
        "updated_at": raw_row.get("updated_at"),
        "response_code": response_code,
        "response_message": response_message,
        "response_indication": indication,
        "errors": errors,
        **quote_fields,
    }


def _refresh_positions_snapshot(
    *,
    runtime: _SubmitRuntime,
    config: IbkrManualPaperSubmitConfig,
    selected_account_id: str,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    runtime.client.request_positions()
    runtime.client.record_event("req_positions_invoked", payload={"selected_account_id": selected_account_id})
    runtime.collector.positions_ready.clear()
    runtime.transport.req_positions()
    if not _wait_for_event(
        runtime.collector.positions_ready,
        collector=runtime.collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    ):
        latest_error = runtime.collector.latest_error()
        raise IbkrManualPaperSubmitError(
            (
                f"Position refresh did not complete: {latest_error['message']}"
                if latest_error is not None
                else f"Position refresh did not complete within {timeout_seconds:.1f}s."
            )
        )
    return _build_positions_snapshot(
        config=_read_only_config_from_submit(config),
        client=runtime.client,
        selected_account_id=selected_account_id,
    )


def _refresh_execution_truth(
    *,
    runtime: _SubmitRuntime,
    config: IbkrManualPaperSubmitConfig,
    selected_account_id: str,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    execution_request_id = 9701
    runtime.client.request_completed_orders()
    runtime.client.record_event("req_completed_orders_invoked", payload={"api_only": False})
    runtime.collector.reset_completed_orders_ready()
    runtime.transport.req_completed_orders(api_only=False)
    runtime.client.request_executions()
    runtime.client.record_event("req_executions_invoked", payload={"request_id": execution_request_id})
    runtime.collector.reset_executions_ready(execution_request_id)
    runtime.transport.req_executions(request_id=execution_request_id)
    completed_ok = _wait_for_event(
        runtime.collector.completed_orders_ready,
        collector=runtime.collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    executions_ok = _wait_for_event(
        runtime.collector.executions_event(execution_request_id),
        collector=runtime.collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if not completed_ok or not executions_ok:
        latest_error = runtime.collector.latest_error()
        raise IbkrManualPaperSubmitError(
            (
                f"Execution/fill refresh did not complete: {latest_error['message']}"
                if latest_error is not None
                else f"Execution/fill refresh did not complete within {timeout_seconds:.1f}s."
            )
        )
    provider = IbkrExecutionProvider(config.repo_root, session=runtime.session, client=runtime.client)
    snapshot = provider.snapshot_state(force_refresh=True)
    read_only_config = _read_only_config_from_submit(config)
    positions = _build_positions_snapshot(config=read_only_config, client=runtime.client, selected_account_id=selected_account_id)
    open_orders = _build_open_orders_snapshot(config=read_only_config, client=runtime.client, selected_account_id=selected_account_id)
    return {
        "provider_snapshot": snapshot,
        "positions": positions,
        "open_orders": open_orders,
        "executions": _extract_execution_rows(snapshot),
        "completed_orders": _extract_completed_order_rows(snapshot),
    }


def _extract_execution_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("executions")
    if isinstance(rows, list):
        return [dict(row) for row in rows]
    orders = dict(snapshot.get("orders") or {})
    recent_fill_rows = orders.get("recent_fill_rows")
    if isinstance(recent_fill_rows, list):
        return [dict(row) for row in recent_fill_rows]
    return []


def _extract_completed_order_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("completed_orders")
    if isinstance(rows, list):
        return [dict(row) for row in rows]
    return []


def _extract_unsupported_attribute(message: str | None) -> str | None:
    match = re.search(r"'([^']+)' order attribute is not supported", str(message or ""))
    if match is None:
        return None
    return str(match.group(1) or "").strip() or None


def _rejection_reason(error_code: int | None, error_message: str | None) -> str | None:
    if int(error_code or 0) == 478:
        return "contract_expiry_conflict"
    if _extract_unsupported_attribute(error_message) is not None:
        return "unsupported_attribute"
    return None


def _detect_order_rejection(
    *,
    collector: IbkrManualPaperSubmitCollector,
    order_id: int,
) -> dict[str, Any] | None:
    latest_error = next(
        (
            dict(row)
            for row in reversed(list(collector.errors))
            if int(row.get("request_id", -1)) == int(order_id) and int(row.get("code", 0)) in _ORDER_REJECTION_ERROR_CODES
        ),
        None,
    )
    latest_status = collector.latest_order_status(order_id)
    if latest_error is None and str((latest_status or {}).get("status") or "").strip() not in _FAILED_ORDER_STATUS:
        return None
    error_code = int(latest_error.get("code", 0)) if latest_error is not None else None
    error_message = str(latest_error.get("message") or "").strip() if latest_error is not None else None
    unsupported_attribute = _extract_unsupported_attribute(error_message)
    rejection_reason = _rejection_reason(error_code, error_message)
    detail = (
        f"IBKR rejected the paper order before it became broker-visible: {error_message}"
        if error_message
        else "IBKR rejected the paper order before it became broker-visible."
    )
    return {
        "rejected": True,
        "error_code": error_code,
        "error_message": error_message,
        "unsupported_attribute": unsupported_attribute,
        "reason": rejection_reason,
        "latest_order_status": latest_status,
        "detail": detail,
    }


def _matches_order_identity(row: dict[str, Any], *, order_id: int, perm_id: int | None) -> bool:
    broker_order_id = row.get("broker_order_id")
    if broker_order_id is not None and str(broker_order_id) == str(order_id):
        return True
    raw_payload = dict(row.get("raw_payload") or {})
    row_perm_id = row.get("perm_id")
    if row_perm_id is None:
        row_perm_id = raw_payload.get("perm_id")
    return perm_id is not None and row_perm_id is not None and str(row_perm_id) == str(perm_id)


def _matching_execution_rows(rows: list[dict[str, Any]], *, order_id: int, perm_id: int | None) -> list[dict[str, Any]]:
    return [dict(row) for row in rows if _matches_order_identity(dict(row), order_id=order_id, perm_id=perm_id)]


def _filled_quantity_for_order(
    *,
    latest_status: dict[str, Any] | None,
    execution_rows: list[dict[str, Any]],
) -> float:
    if latest_status is not None and latest_status.get("filled") is not None:
        return float(latest_status.get("filled") or 0.0)
    quantity = 0.0
    for row in execution_rows:
        quantity += float(_coerce_float(row.get("quantity")) or 0.0)
    return quantity


def _build_fill_verification_payload(
    *,
    requested_order: dict[str, Any],
    latest_status: dict[str, Any] | None,
    open_orders_after_submit: dict[str, Any],
    positions_after_submit: dict[str, Any],
    execution_rows: list[dict[str, Any]],
    completed_order_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    requested_quantity = float(requested_order.get("quantity") or 0.0)
    submitted_row = _find_open_order(open_orders_after_submit, int(latest_status.get("order_id") or 0)) if latest_status else None
    order_status = str((latest_status or {}).get("status") or "").strip() or None
    completed_status = str((completed_order_rows[-1].get("status") if completed_order_rows else "") or "").strip() or None
    final_status = order_status or completed_status or (submitted_row.get("status") if submitted_row else None)
    filled_quantity = _filled_quantity_for_order(latest_status=latest_status, execution_rows=execution_rows)
    fully_filled = bool(
        (final_status in _FILLED_ORDER_STATUS)
        or (requested_quantity > 0.0 and filled_quantity >= requested_quantity)
    )
    partial_fill = bool(
        not fully_filled
        and (
            final_status in _PARTIAL_FILL_STATUS
            or (0.0 < filled_quantity < requested_quantity)
        )
    )
    detail = (
        "Broker truth verified a full paper fill."
        if fully_filled
        else (
            "Broker truth verified a partial fill."
            if partial_fill
            else "Broker truth has not yet verified a fill."
        )
    )
    return {
        "verified": fully_filled,
        "partial_fill": partial_fill,
        "filled_quantity": filled_quantity,
        "requested_quantity": requested_quantity,
        "final_status": final_status,
        "detail": detail,
        "positions_after_submit": positions_after_submit,
        "executions_after_submit": execution_rows,
        "completed_orders_after_submit": completed_order_rows,
        "open_order_after_submit": open_orders_after_submit,
    }


def _verify_close_position_flat(
    *,
    config: IbkrManualPaperSubmitConfig,
    runtime: _SubmitRuntime,
    selected_account_id: str,
    sleep_fn: Callable[[float], None],
    contract_report: dict[str, Any],
) -> dict[str, Any]:
    deadline = time.monotonic() + max(float(config.post_approval_observation_seconds), 5.0)
    last_positions = _not_run_snapshot("Close verification did not capture broker positions.")
    while time.monotonic() < deadline:
        last_positions = _refresh_positions_snapshot(
            runtime=runtime,
            config=config,
            selected_account_id=selected_account_id,
            timeout_seconds=min(5.0, max(1.0, deadline - time.monotonic())),
            sleep_fn=sleep_fn,
        )
        exact_quantity = _exact_contract_position_quantity(
            positions_snapshot=last_positions,
            contract_report=contract_report,
        )
        if exact_quantity in (None, 0.0):
            return {
                "verified": True,
                "detail": "Broker position truth verified that the exact MGC position is flat after the close fill.",
                "positions_after_close_fill": last_positions,
                "exact_position_quantity": exact_quantity,
            }
        sleep_fn(0.25)
    exact_quantity = _exact_contract_position_quantity(
        positions_snapshot=last_positions,
        contract_report=contract_report,
    )
    return {
        "verified": False,
        "detail": "Broker position truth did not verify the exact MGC position as flat within the allowed close-verification window.",
        "positions_after_close_fill": last_positions,
        "exact_position_quantity": exact_quantity,
    }


def _execute_fill_test_lifecycle(
    *,
    config: IbkrManualPaperSubmitConfig,
    runtime: _SubmitRuntime,
    selected_account_id: str,
    sleep_fn: Callable[[float], None],
    audit_events: list[dict[str, Any]],
    order_id: int,
    requested_order: dict[str, Any],
    perm_id: int | None,
    contract_report: dict[str, Any],
) -> dict[str, Any]:
    observation_window_seconds = max(float(config.fill_timeout_seconds), float(config.post_approval_observation_seconds))
    deadline = time.monotonic() + observation_window_seconds
    last_open_orders = _not_run_snapshot("Submit verification did not capture broker open-order truth.")
    last_positions = _not_run_snapshot("Submit verification did not capture broker positions.")
    last_execution_rows: list[dict[str, Any]] = []
    last_completed_order_rows: list[dict[str, Any]] = []
    latest_status = runtime.collector.latest_order_status(order_id)
    while time.monotonic() < deadline:
        remaining = max(1.0, min(5.0, deadline - time.monotonic()))
        truth = _refresh_execution_truth(
            runtime=runtime,
            config=config,
            selected_account_id=selected_account_id,
            timeout_seconds=remaining,
            sleep_fn=sleep_fn,
        )
        last_open_orders = truth["open_orders"]
        last_positions = truth["positions"]
        latest_status = runtime.collector.latest_order_status(order_id)
        open_row = _find_open_order(last_open_orders, order_id)
        effective_perm_id = perm_id if perm_id is not None else (open_row.get("perm_id") if open_row else (latest_status.get("perm_id") if latest_status else None))
        last_execution_rows = _matching_execution_rows(truth["executions"], order_id=order_id, perm_id=effective_perm_id)
        last_completed_order_rows = _matching_execution_rows(truth["completed_orders"], order_id=order_id, perm_id=effective_perm_id)
        rejection = _detect_order_rejection(collector=runtime.collector, order_id=order_id)
        if rejection is not None:
            _record_audit(
                audit_events,
                event_type="submit_rejected",
                config=config,
                classification=None,
                detail=str(rejection.get("detail") or "IBKR rejected the paper order."),
                extra={
                    "order_id": order_id,
                    "perm_id": effective_perm_id,
                    "error_code": rejection.get("error_code"),
                    "unsupported_attribute": rejection.get("unsupported_attribute"),
                },
            )
            return {
                "status": "rejected",
                "detail": rejection.get("detail"),
                "submitted_order_id": order_id,
                "submitted_perm_id": effective_perm_id,
                "open_order_after_submit": last_open_orders,
                "open_order_after_cancel": _not_run_snapshot(
                    "Cancel was not attempted because IBKR rejected the order before it became broker-visible."
                ),
                "latest_order_status": rejection.get("latest_order_status"),
                "rejection": rejection,
                "fill_verification": {
                    "verified": False,
                    "partial_fill": False,
                    "filled_quantity": 0.0,
                    "requested_quantity": float(requested_order.get("quantity") or 0.0),
                    "final_status": str((rejection.get("latest_order_status") or {}).get("status") or "").strip() or None,
                    "detail": rejection.get("detail"),
                    "positions_after_submit": last_positions,
                    "executions_after_submit": last_execution_rows,
                    "completed_orders_after_submit": last_completed_order_rows,
                    "open_order_after_submit": last_open_orders,
                },
                "positions_after_submit": last_positions,
                "executions_after_submit": last_execution_rows,
                "completed_orders_after_submit": last_completed_order_rows,
            }
        fill_verification = _build_fill_verification_payload(
            requested_order=requested_order,
            latest_status=latest_status,
            open_orders_after_submit=last_open_orders,
            positions_after_submit=last_positions,
            execution_rows=last_execution_rows,
            completed_order_rows=last_completed_order_rows,
        )
        if fill_verification["verified"]:
            normalized_mode = str(config.test_mode or "").strip().upper()
            if normalized_mode == _CLOSE_TEST_MODE:
                close_position_verification = _verify_close_position_flat(
                    config=config,
                    runtime=runtime,
                    selected_account_id=selected_account_id,
                    sleep_fn=sleep_fn,
                    contract_report=contract_report,
                )
                _record_audit(
                    audit_events,
                    event_type="close_position_verification_completed",
                    config=config,
                    classification=None,
                    detail=str(close_position_verification.get("detail") or ""),
                    extra={
                        "order_id": order_id,
                        "perm_id": effective_perm_id,
                        "verified": bool(close_position_verification.get("verified")),
                        "exact_position_quantity": close_position_verification.get("exact_position_quantity"),
                    },
                )
                return {
                    "status": "filled_flat" if close_position_verification.get("verified") else "close_position_not_flat",
                    "detail": (
                        "Submitted one manual paper MGC close order, verified the SELL fill through broker truth, and reconciled the exact position flat."
                        if close_position_verification.get("verified")
                        else str(close_position_verification.get("detail") or "Close fill verified, but exact flat position could not be confirmed.")
                    ),
                    "submitted_order_id": order_id,
                    "submitted_perm_id": effective_perm_id,
                    "open_order_after_submit": last_open_orders,
                    "open_order_after_cancel": _not_run_snapshot("Cancel was not needed because the close order filled."),
                    "latest_order_status": latest_status,
                    "fill_verification": fill_verification,
                    "close_position_verification": close_position_verification,
                    "positions_after_submit": close_position_verification.get("positions_after_close_fill") or last_positions,
                    "executions_after_submit": last_execution_rows,
                    "completed_orders_after_submit": last_completed_order_rows,
                }
            _record_audit(
                audit_events,
                event_type="fill_verified",
                config=config,
                classification=None,
                detail=fill_verification["detail"],
                extra={"order_id": order_id, "perm_id": effective_perm_id, "final_status": fill_verification.get("final_status")},
            )
            return {
                "status": "filled",
                "detail": "Submitted one manual paper MGC limit order and verified the fill through broker truth.",
                "submitted_order_id": order_id,
                "submitted_perm_id": effective_perm_id,
                "open_order_after_submit": last_open_orders,
                "open_order_after_cancel": _not_run_snapshot("Cancel was not needed because the order filled."),
                "latest_order_status": latest_status,
                "fill_verification": fill_verification,
                "positions_after_submit": last_positions,
                "executions_after_submit": last_execution_rows,
                "completed_orders_after_submit": last_completed_order_rows,
            }
        sleep_fn(0.25)
    latest_status = runtime.collector.latest_order_status(order_id)
    open_row = _find_open_order(last_open_orders, order_id)
    effective_perm_id = perm_id if perm_id is not None else (open_row.get("perm_id") if open_row else (latest_status.get("perm_id") if latest_status else None))
    fill_verification = _build_fill_verification_payload(
        requested_order=requested_order,
        latest_status=latest_status,
        open_orders_after_submit=last_open_orders,
        positions_after_submit=last_positions,
        execution_rows=last_execution_rows,
        completed_order_rows=last_completed_order_rows,
    )
    _record_audit(
        audit_events,
        event_type="fill_verification_timeout",
        config=config,
        classification=None,
        detail="The paper fill test did not verify a complete fill within the allowed timeout; the harness is attempting cancel verification.",
        extra={
            "order_id": order_id,
            "perm_id": effective_perm_id,
            "partial_fill": fill_verification.get("partial_fill"),
            "observation_window_seconds": observation_window_seconds,
        },
    )
    cancel_result = _cancel_and_verify_visible_order(
        config=config,
        runtime=runtime,
        selected_account_id=selected_account_id,
        sleep_fn=sleep_fn,
        audit_events=audit_events,
        order_id=order_id,
        perm_id=effective_perm_id,
        after_submit=last_open_orders,
    )
    return {
        "status": "partial_fill_cancelled" if fill_verification.get("partial_fill") else "fill_timeout_cancelled",
        "detail": (
            "The order partially filled and the remaining paper order was cancelled and verified."
            if fill_verification.get("partial_fill")
            else "The order did not verify a fill within the timeout, so the working paper order was cancelled and verified."
        ),
        "submitted_order_id": order_id,
        "submitted_perm_id": effective_perm_id,
        "open_order_after_submit": last_open_orders,
        "open_order_after_cancel": cancel_result["open_order_after_cancel"],
        "latest_order_status": cancel_result["latest_order_status"],
        "cancel_verification": cancel_result["cancel_verification"],
        "fill_verification": fill_verification,
        "positions_after_submit": last_positions,
        "executions_after_submit": last_execution_rows,
        "completed_orders_after_submit": last_completed_order_rows,
    }


def _execute_submit_cancel_lifecycle(
    *,
    config: IbkrManualPaperSubmitConfig,
    runtime: _SubmitRuntime,
    context: dict[str, Any],
    requested_order: dict[str, Any],
    sleep_fn: Callable[[float], None],
    audit_events: list[dict[str, Any]],
    preview_payload: dict[str, Any],
    preview_digest: str,
    manual_confirmation_fn: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    pricing_context = dict(context.get("pricing_context") or {})
    refreshed_before_submit = _refresh_open_orders_snapshot(
        runtime=runtime,
        config=config,
        selected_account_id=context["selected_account_id"],
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if _snapshot_digest(refreshed_before_submit) != context["open_order_baseline_digest"]:
        _record_audit(
            audit_events,
            event_type="approval_invalidated",
            config=config,
            classification=None,
            detail="Open-order baseline changed after preview; approval invalidated.",
        )
        return {
            "status": "approval_invalidated",
            "detail": "Open-order baseline changed after preview; approval was invalidated before submit.",
            "open_order_after_submit": _not_run_snapshot("Submit was blocked because the open-order baseline changed."),
            "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because submit was blocked."),
        }
    order_id = runtime.session.allocate_order_id()
    runtime.collector.reset_order_status_event(order_id)
    runtime.transport.place_limit_order(
        order_id=order_id,
        account_id=context["selected_account_id"],
        contract=context["contract_report"]["qualified_contract_object"],
        action=requested_order["action"],
        quantity=requested_order["quantity"],
        limit_price=requested_order["limit_price"],
        time_in_force=requested_order["time_in_force"],
    )
    _record_audit(
        audit_events,
        event_type="submit_attempted",
        config=config,
        classification=None,
        detail="Submitted one manual paper limit order to TWS.",
        extra={
            "order_id": order_id,
            "preview_digest": preview_digest,
            "quote_snapshot": pricing_context.get("quote_snapshot"),
            "limit_price": requested_order.get("limit_price"),
            "distance_from_quote": pricing_context.get("distance_from_reference_price"),
            "distance_ticks": pricing_context.get("distance_ticks"),
        },
    )
    _record_audit(
        audit_events,
        event_type="manual_confirmation_wait_started",
        config=config,
        classification=None,
        detail=(
            "Check TWS now. If a confirmation dialog is visible, approve or reject it manually. "
            "Press Enter here only after you have handled the TWS dialog."
        ),
        extra={
            "order_id": order_id,
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "timeout_seconds": config.manual_confirmation_timeout_seconds,
        },
    )
    manual_confirmation = manual_confirmation_fn(
        timeout_seconds=config.manual_confirmation_timeout_seconds,
        order_id=order_id,
        preview_digest=preview_digest,
    )
    _record_audit(
        audit_events,
        event_type="manual_confirmation_response_recorded",
        config=config,
        classification=None,
        detail=str(manual_confirmation.get("detail") or "Manual confirmation response recorded."),
        extra={
            "order_id": order_id,
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": manual_confirmation.get("operator_outcome"),
            "response_text": manual_confirmation.get("response_text"),
        },
    )
    if manual_confirmation.get("operator_outcome") == "approved":
        _record_audit(
            audit_events,
            event_type="broker_truth_verification_started",
            config=config,
            classification=None,
            detail="Operator approved the TWS dialog. Broker open-order verification is starting now.",
            extra={"order_id": order_id},
        )
        if str(config.test_mode or "").strip().upper() in {_FILL_TEST_MODE, _CLOSE_TEST_MODE}:
            return {
                **_execute_fill_test_lifecycle(
                    config=config,
                    runtime=runtime,
                    selected_account_id=context["selected_account_id"],
                    sleep_fn=sleep_fn,
                    audit_events=audit_events,
                    order_id=order_id,
                    requested_order=requested_order,
                    perm_id=None,
                    contract_report=context["contract_report"],
                ),
                "manual_confirmation": manual_confirmation,
            }
        after_submit = _wait_for_submitted_order_visibility(
            runtime=runtime,
            config=config,
            selected_account_id=context["selected_account_id"],
            order_id=order_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
    else:
        after_submit = _refresh_open_orders_snapshot(
            runtime=runtime,
            config=config,
            selected_account_id=context["selected_account_id"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
    submitted_row = _find_open_order(after_submit, order_id)
    latest_status = runtime.collector.latest_order_status(order_id)
    perm_id = submitted_row.get("perm_id") if submitted_row else (latest_status.get("perm_id") if latest_status else None)
    if manual_confirmation.get("operator_outcome") == "timeout":
        detail = "Manual TWS confirmation wait expired before the operator confirmed dialog handling."
        if submitted_row is None:
            _record_audit(
                audit_events,
                event_type="open_order_verification_failed",
                config=config,
                classification=None,
                detail=detail,
                extra={"order_id": order_id, "perm_id": perm_id},
            )
            return {
                "status": "manual_confirmation_timeout",
                "detail": detail,
                "submitted_order_id": order_id,
                "submitted_perm_id": perm_id,
                "open_order_after_submit": after_submit,
                "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because no broker order was visible after manual confirmation timed out."),
                "latest_order_status": latest_status,
                "manual_confirmation": manual_confirmation,
            }
        _record_audit(
            audit_events,
            event_type="open_order_verified_after_submit",
            config=config,
            classification=None,
            detail="An order appeared after the manual confirmation timeout, so the harness is attempting an immediate cancel.",
            extra={"order_id": order_id, "perm_id": perm_id, "status": submitted_row.get("status")},
        )
        cancel_result = _cancel_and_verify_visible_order(
            config=config,
            runtime=runtime,
            selected_account_id=context["selected_account_id"],
            sleep_fn=sleep_fn,
            audit_events=audit_events,
            order_id=order_id,
            perm_id=perm_id,
            after_submit=after_submit,
        )
        return {
            "status": "manual_confirmation_timeout_order_cancelled" if cancel_result["cancel_verification"]["verified"] else "cancel_verification_failed",
            "detail": (
                "Manual confirmation timed out, but the unexpected working order was cancelled and verified."
                if cancel_result["cancel_verification"]["verified"]
                else cancel_result["detail"]
            ),
            "submitted_order_id": order_id,
            "submitted_perm_id": perm_id,
            "open_order_after_submit": after_submit,
            "open_order_after_cancel": cancel_result["open_order_after_cancel"],
            "latest_order_status": cancel_result["latest_order_status"],
            "cancel_verification": cancel_result["cancel_verification"],
            "manual_confirmation": manual_confirmation,
        }
    if manual_confirmation.get("operator_outcome") == "unavailable":
        detail = str(manual_confirmation.get("detail") or "Manual TWS confirmation is unavailable in this session.")
        _record_audit(
            audit_events,
            event_type="open_order_verification_failed",
            config=config,
            classification=None,
            detail=detail,
            extra={"order_id": order_id, "perm_id": perm_id},
        )
        return {
            "status": "manual_confirmation_unavailable",
            "detail": detail,
            "submitted_order_id": order_id,
            "submitted_perm_id": perm_id,
            "open_order_after_submit": after_submit,
            "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because manual confirmation was unavailable."),
            "latest_order_status": latest_status,
            "manual_confirmation": manual_confirmation,
        }
    if manual_confirmation.get("operator_outcome") == "rejected":
        detail = "Operator reported that the TWS confirmation dialog was rejected or cancelled."
        if submitted_row is None:
            return {
                "status": "manual_confirmation_rejected_no_order",
                "detail": detail,
                "submitted_order_id": order_id,
                "submitted_perm_id": perm_id,
                "open_order_after_submit": after_submit,
                "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because the operator rejected the TWS dialog and no order was visible."),
                "latest_order_status": latest_status,
                "manual_confirmation": manual_confirmation,
            }
        _record_audit(
            audit_events,
            event_type="open_order_verified_after_submit",
            config=config,
            classification=None,
            detail="An order appeared even though the operator rejected the TWS dialog, so the harness is attempting an immediate cancel.",
            extra={"order_id": order_id, "perm_id": perm_id, "status": submitted_row.get("status")},
        )
        cancel_result = _cancel_and_verify_visible_order(
            config=config,
            runtime=runtime,
            selected_account_id=context["selected_account_id"],
            sleep_fn=sleep_fn,
            audit_events=audit_events,
            order_id=order_id,
            perm_id=perm_id,
            after_submit=after_submit,
        )
        return {
            "status": "manual_confirmation_rejected_order_cancelled" if cancel_result["cancel_verification"]["verified"] else "cancel_verification_failed",
            "detail": (
                "Operator rejected the dialog, but an order still appeared and was cancelled successfully."
                if cancel_result["cancel_verification"]["verified"]
                else cancel_result["detail"]
            ),
            "submitted_order_id": order_id,
            "submitted_perm_id": perm_id,
            "open_order_after_submit": after_submit,
            "open_order_after_cancel": cancel_result["open_order_after_cancel"],
            "latest_order_status": cancel_result["latest_order_status"],
            "cancel_verification": cancel_result["cancel_verification"],
            "manual_confirmation": manual_confirmation,
        }
    submitted_row = _find_open_order(after_submit, order_id)
    latest_status = runtime.collector.latest_order_status(order_id)
    perm_id = submitted_row.get("perm_id") if submitted_row else (latest_status.get("perm_id") if latest_status else None)
    if submitted_row is None:
        rejection = _detect_order_rejection(collector=runtime.collector, order_id=order_id)
        if rejection is not None:
            _record_audit(
                audit_events,
                event_type="submit_rejected",
                config=config,
                classification=None,
                detail=str(rejection.get("detail") or "IBKR rejected the paper order."),
                extra={
                    "order_id": order_id,
                    "perm_id": perm_id,
                    "error_code": rejection.get("error_code"),
                    "unsupported_attribute": rejection.get("unsupported_attribute"),
                },
            )
            return {
                "status": "rejected",
                "detail": rejection.get("detail"),
                "submitted_order_id": order_id,
                "submitted_perm_id": perm_id,
                "open_order_after_submit": after_submit,
                "open_order_after_cancel": _not_run_snapshot(
                    "Cancel was not attempted because IBKR rejected the order before it became broker-visible."
                ),
                "latest_order_status": rejection.get("latest_order_status"),
                "manual_confirmation": manual_confirmation,
                "rejection": rejection,
            }
        detail = "Operator approved the TWS dialog, but broker open-order truth could not verify the expected order by exact order id."
        _record_audit(
            audit_events,
            event_type="open_order_verification_failed",
            config=config,
            classification=None,
            detail=detail,
            extra={"order_id": order_id, "perm_id": perm_id},
        )
        return {
            "status": "submit_verification_failed",
            "detail": detail,
            "submitted_order_id": order_id,
            "submitted_perm_id": perm_id,
            "open_order_after_submit": after_submit,
            "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because submit verification failed."),
            "latest_order_status": latest_status,
            "manual_confirmation": manual_confirmation,
        }
    _record_audit(
        audit_events,
        event_type="open_order_verified_after_submit",
        config=config,
        classification=None,
        detail="Broker open-order truth confirmed the submitted paper order.",
        extra={"order_id": order_id, "perm_id": perm_id, "status": submitted_row.get("status")},
    )
    cancel_result = _cancel_and_verify_visible_order(
        runtime=runtime,
        config=config,
        selected_account_id=context["selected_account_id"],
        sleep_fn=sleep_fn,
        audit_events=audit_events,
        order_id=order_id,
        perm_id=perm_id,
        after_submit=after_submit,
    )
    return {
        "status": "passed" if cancel_result["cancel_verification"]["verified"] else "cancel_verification_failed",
        "detail": (
            "Submitted one manual paper MGC limit order, verified it in broker truth, canceled it, and verified cancellation."
            if cancel_result["cancel_verification"]["verified"]
            else cancel_result["detail"]
        ),
        "submitted_order_id": order_id,
        "submitted_perm_id": perm_id,
        "open_order_after_submit": after_submit,
        "open_order_after_cancel": cancel_result["open_order_after_cancel"],
        "latest_order_status": cancel_result["latest_order_status"],
        "cancel_verification": cancel_result["cancel_verification"],
        "manual_confirmation": manual_confirmation,
    }


def _cancel_and_verify_visible_order(
    *,
    config: IbkrManualPaperSubmitConfig,
    runtime: _SubmitRuntime,
    selected_account_id: str,
    sleep_fn: Callable[[float], None],
    audit_events: list[dict[str, Any]],
    order_id: int,
    perm_id: int | None,
    after_submit: dict[str, Any],
) -> dict[str, Any]:
    runtime.collector.reset_order_status_event(order_id)
    runtime.transport.cancel_order(order_id=order_id)
    _record_audit(
        audit_events,
        event_type="cancel_requested",
        config=config,
        classification=None,
        detail="Requested cancel for the submitted paper order.",
        extra={"order_id": order_id, "perm_id": perm_id},
    )
    after_cancel = _wait_for_order_absence(
        runtime=runtime,
        config=config,
        selected_account_id=selected_account_id,
        order_id=order_id,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    cancel_verification = evaluate_cancel_verification(
        order_id=order_id,
        latest_order_status=runtime.collector.latest_order_status(order_id),
        open_orders_after_cancel=after_cancel,
    )
    event_type = "cancel_verified" if cancel_verification["verified"] else "cancel_verification_failed"
    _record_audit(
        audit_events,
        event_type=event_type,
        config=config,
        classification=None,
        detail=cancel_verification["detail"],
        extra={"order_id": order_id, "perm_id": perm_id, "final_status": cancel_verification.get("final_status")},
    )
    return {
        "detail": cancel_verification["detail"],
        "open_order_after_submit": after_submit,
        "open_order_after_cancel": after_cancel,
        "latest_order_status": runtime.collector.latest_order_status(order_id),
        "cancel_verification": cancel_verification,
    }


def evaluate_cancel_verification(
    *,
    order_id: int,
    latest_order_status: dict[str, Any] | None,
    open_orders_after_cancel: dict[str, Any],
) -> dict[str, Any]:
    still_open = _find_open_order(open_orders_after_cancel, order_id)
    final_status = str((latest_order_status or {}).get("status") or "").strip() or None
    if still_open is None and final_status in _CANCELLED_ORDER_STATUS:
        return {
            "verified": True,
            "final_status": final_status,
            "detail": "Cancel verified by exact order id: the order is no longer open and the latest broker status is canceled.",
        }
    if still_open is None:
        return {
            "verified": False,
            "final_status": final_status,
            "detail": "Cancel could not be verified conclusively because the order disappeared from open orders without an explicit canceled status.",
        }
    return {
        "verified": False,
        "final_status": final_status or still_open.get("status"),
        "detail": "Cancel could not be verified because the order still appears in broker open-order truth.",
    }


def _validate_submit_approval(
    *,
    config: IbkrManualPaperSubmitConfig,
    expected_digest: str,
    expected_phrase: str,
    selected_account_id: str,
) -> dict[str, Any]:
    digest_matches = str(config.approval_digest or "").strip() == str(expected_digest)
    phrase_matches = str(config.approval_phrase or "").strip() == str(expected_phrase)
    guardrail_checks = [
        _guardrail_check(
            "exact_preview_digest_required",
            passed=digest_matches,
            blocking=True,
            detail="Submit requires the exact preview digest from the current preview payload.",
        ),
        _guardrail_check(
            "typed_confirmation_phrase_required",
            passed=phrase_matches,
            blocking=True,
            detail="Submit requires the exact typed confirmation phrase for this preview.",
        ),
    ]
    if not digest_matches:
        detail = "Submit was blocked because the supplied approval digest does not match the current preview digest."
    elif not phrase_matches:
        detail = "Submit was blocked because the supplied typed confirmation phrase does not match the current preview."
    else:
        detail = f"Approval validated for account {selected_account_id} using the exact preview digest and typed confirmation phrase."
    return {
        "approved": digest_matches and phrase_matches,
        "detail": detail,
        "guardrail_checks": guardrail_checks,
        "approval_phrase_hash": hashlib.sha256(str(config.approval_phrase or "").encode("utf-8")).hexdigest()
        if config.approval_phrase
        else None,
    }


def _build_submit_preview_payload(
    *,
    requested_order: dict[str, Any],
    context: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    test_mode: str,
) -> dict[str, Any]:
    contract_report = dict(context["contract_report"])
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    quote_context = dict(context["quote_context"])
    pricing_context = dict(context.get("pricing_context") or {})
    multiplier = _coerce_float(contract_details.get("multiplier")) or _coerce_float(contract_report.get("qualified_contract", {}).get("multiplier"))
    min_tick = _coerce_float(contract_details.get("min_tick"))
    quantity = float(requested_order["quantity"])
    limit_price = _coerce_float(requested_order.get("limit_price"))
    estimated_tick_value = (min_tick * multiplier * quantity) if min_tick is not None and multiplier is not None else None
    estimated_notional = (limit_price * multiplier * quantity) if limit_price is not None and multiplier is not None else None
    normalized_guardrails = [
        {
            "name": check["name"],
            "passed": bool(check["passed"]),
            "blocking": bool(check["blocking"]),
        }
        for check in sorted(guardrail_checks, key=lambda row: str(row.get("name") or ""))
    ]
    return {
        "account_id": context["selected_account_id"],
        "environment": {
            "mode": _EXPECTED_MODE,
            "host": _EXPECTED_HOST,
            "port": _EXPECTED_PORT,
            "client_id": context["connection_check"]["client_id"],
        },
        "contract": {
            "symbol": requested_order["symbol"],
            "expiry": requested_order["expiry"],
            "exchange": contract_details.get("exchange") or contract_report.get("qualified_contract", {}).get("exchange"),
            "local_symbol": contract_details.get("local_symbol") or contract_report.get("qualified_contract", {}).get("local_symbol"),
            "multiplier": contract_details.get("multiplier") or contract_report.get("qualified_contract", {}).get("multiplier"),
            "qualified_contract_identifier": contract_report.get("qualified_contract_identifier"),
        },
        "hypothetical_order": {
            "action": requested_order["action"],
            "quantity": requested_order["quantity"],
            "order_type": requested_order["order_type"],
            "limit_price": requested_order["limit_price"],
            "time_in_force": requested_order["time_in_force"],
            "test_mode": str(test_mode or "").strip().upper(),
            "pricing_label": pricing_context.get("pricing_label"),
            "intended_to_fill": bool(pricing_context.get("intended_to_fill")),
        },
        "quote_context": {
            "quote_source_label": quote_context.get("quote_source_label"),
            "live_market_data_available": quote_context.get("live_market_data_available"),
            "live_market_data_warning": quote_context.get("live_market_data_warning"),
            "quote_snapshot": pricing_context.get("quote_snapshot"),
            "reference_price": pricing_context.get("reference_price"),
            "reference_price_source": pricing_context.get("reference_price_source"),
            "distance_from_reference_price": pricing_context.get("distance_from_reference_price"),
            "distance_ticks": pricing_context.get("distance_ticks"),
            "pricing_label": pricing_context.get("pricing_label"),
            "intended_to_fill": bool(pricing_context.get("intended_to_fill")),
        },
        "open_order_baseline": {
            "open_order_count": context["open_orders_before"].get("open_order_count"),
            "selected_account_id": context["open_orders_before"].get("selected_account_id"),
        },
        "estimated_tick_value": estimated_tick_value,
        "estimated_notional": estimated_notional,
        "guardrails": normalized_guardrails,
        "no_submit_guarantee": {
            "submitted": False,
            "staged": False,
            "transmitted": False,
        },
    }


def _build_report(
    *,
    config: IbkrManualPaperSubmitConfig,
    started_at: datetime,
    classification: str,
    caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    context: dict[str, Any],
    requested_order: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    preview_payload: dict[str, Any],
    preview_digest: str,
    expected_phrase: str,
    audit_events: list[dict[str, Any]],
    lifecycle_result: dict[str, Any],
    frozen_preview_bundle: dict[str, Any] | None,
) -> dict[str, Any]:
    quote_context = dict(context.get("quote_context") or {})
    pricing_context = dict(context.get("pricing_context") or {})
    contract_report = dict(context.get("contract_report") or {})
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    multiplier = _coerce_float(contract_details.get("multiplier")) or _coerce_float(contract_report.get("qualified_contract", {}).get("multiplier"))
    min_tick = _coerce_float(contract_details.get("min_tick"))
    quantity = float(requested_order["quantity"])
    limit_price = _coerce_float(requested_order.get("limit_price"))
    estimated_tick_value = (min_tick * multiplier * quantity) if min_tick is not None and multiplier is not None else None
    estimated_notional = (limit_price * multiplier * quantity) if limit_price is not None and multiplier is not None else None
    return {
        "classification": classification,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "account_id": context["selected_account_id"],
        "connection_check": context["connection_check"],
        "manual_caller_check": caller_check,
        "environment_lock_check": environment_lock,
        "account_truth": context["account_truth"],
        "positions": context["positions"],
        "open_order_before": context["open_orders_before"],
        "preview": {
            "test_mode": str(config.test_mode or "").strip().upper(),
            "symbol": requested_order["symbol"],
            "expiry": requested_order["expiry"],
            "action": requested_order["action"],
            "quantity": requested_order["quantity"],
            "order_type": requested_order["order_type"],
            "limit_price": requested_order["limit_price"],
            "time_in_force": requested_order["time_in_force"],
            "quote_source_label": quote_context.get("quote_source_label"),
            "live_market_data_warning": quote_context.get("live_market_data_warning"),
            "quote_snapshot": pricing_context.get("quote_snapshot"),
            "reference_price": pricing_context.get("reference_price"),
            "reference_price_source": pricing_context.get("reference_price_source"),
            "distance_from_quote": pricing_context.get("distance_from_reference_price"),
            "distance_ticks": pricing_context.get("distance_ticks"),
            "pricing_label": pricing_context.get("pricing_label"),
            "intended_to_fill": bool(pricing_context.get("intended_to_fill")),
            "estimated_notional": estimated_notional,
            "estimated_tick_value": estimated_tick_value,
            "preview_digest": preview_digest,
            "expected_approval_phrase": expected_phrase,
        },
        "preview_payload": preview_payload,
        "frozen_preview_bundle": frozen_preview_bundle,
        "guardrail_checks": guardrail_checks,
        "submit_cancel_lifecycle": lifecycle_result,
        "audit_event_count": len(audit_events),
        "errors": list(context.get("errors") or []),
        "next_manual_check": (
            _tws_manual_check_message(config.host, config.port)
            if classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
            else None
        ),
    }


def _blocked_artifacts(
    *,
    config: IbkrManualPaperSubmitConfig,
    started_at: datetime,
    requested_order: dict[str, Any],
    caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    audit_events: list[dict[str, Any]],
    detail: str,
) -> IbkrManualPaperSubmitArtifacts:
    classification = _blocked_classification_for_mode(config.test_mode)
    _record_audit(
        audit_events,
        event_type="failed_closed",
        config=config,
        classification=classification,
        detail=detail,
    )
    report = {
        "classification": classification,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "account_id": config.account_id,
        "connection_check": {
            "connected": False,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "server_version": None,
            "connection_timestamp": started_at.isoformat(),
            "detail": detail,
        },
        "manual_caller_check": caller_check,
        "environment_lock_check": environment_lock,
        "account_truth": {},
        "positions": {},
        "open_order_before": _not_run_snapshot("Open-order baseline was not captured."),
        "preview": {
            "test_mode": str(config.test_mode or "").strip().upper(),
            "symbol": requested_order.get("symbol"),
            "expiry": requested_order.get("expiry"),
            "action": requested_order.get("action"),
            "quantity": requested_order.get("quantity"),
            "order_type": requested_order.get("order_type"),
            "limit_price": requested_order.get("limit_price"),
            "time_in_force": requested_order.get("time_in_force"),
            "preview_digest": None,
            "expected_approval_phrase": None,
        },
        "preview_payload": None,
        "guardrail_checks": guardrail_checks,
        "submit_cancel_lifecycle": {
            "status": "blocked",
            "detail": detail,
        },
        "audit_event_count": len(audit_events),
        "errors": [],
        "next_manual_check": environment_lock.get("next_manual_check") or _tws_manual_check_message(config.host, config.port),
    }
    return IbkrManualPaperSubmitArtifacts(
        classification=classification,
        report=report,
        audit_events=audit_events,
        open_order_before=_not_run_snapshot("Open-order baseline was not captured."),
        open_order_after_submit=_not_run_snapshot("Submit did not run."),
        open_order_after_cancel=_not_run_snapshot("Cancel did not run."),
        artifact_stem=artifact_stem_for_test_mode(config.test_mode),
    )


def _classify_submit_lifecycle(test_mode: str, status: str) -> str:
    normalized_mode = str(test_mode or "").strip().upper()
    if normalized_mode == _FILL_TEST_MODE:
        if status == "filled":
            return "PAPER_ORDER_FILLED"
        if status in {"passed", "fill_timeout_cancelled", "partial_fill_cancelled", "manual_confirmation_timeout_order_cancelled"}:
            return "PAPER_ORDER_SUBMITTED_NOT_FILLED_CANCELLED"
        if status in {"rejected", "manual_confirmation_rejected_no_order", "manual_confirmation_rejected_order_cancelled"}:
            return "PAPER_ORDER_REJECTED"
        if status in {
            "submit_verification_failed",
            "cancel_verification_failed",
            "approval_invalidated",
            "manual_confirmation_timeout",
            "manual_confirmation_unavailable",
        }:
            return "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW"
        return _blocked_classification_for_mode(test_mode)
    if normalized_mode == _CLOSE_TEST_MODE:
        if status == "filled_flat":
            return "PAPER_CLOSE_FILLED_FLAT"
        if status in {"rejected", "manual_confirmation_rejected_no_order", "manual_confirmation_rejected_order_cancelled"}:
            return "PAPER_CLOSE_REJECTED"
        if status in {"fill_timeout_cancelled", "partial_fill_cancelled", "manual_confirmation_timeout_order_cancelled"}:
            return "PAPER_CLOSE_NOT_FILLED_CANCELLED"
        if status in {
            "close_position_not_flat",
            "submit_verification_failed",
            "cancel_verification_failed",
            "approval_invalidated",
            "manual_confirmation_timeout",
            "manual_confirmation_unavailable",
        }:
            return "PAPER_CLOSE_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW"
        return _blocked_classification_for_mode(test_mode)
    if status == "passed":
        return _passed_classification_for_mode(test_mode)
    if status in {
        "preview_only",
        "submit_verification_failed",
        "cancel_verification_failed",
        "approval_invalidated",
        "manual_confirmation_timeout",
        "manual_confirmation_timeout_order_cancelled",
        "manual_confirmation_rejected_no_order",
        "manual_confirmation_rejected_order_cancelled",
    }:
        return _preview_only_classification_for_mode(test_mode)
    return _blocked_classification_for_mode(test_mode)


def _extra_artifacts_from_lifecycle(lifecycle_result: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if lifecycle_result.get("latest_order_status") is not None:
        payload["order_status_after_submit"] = lifecycle_result.get("latest_order_status")
    if lifecycle_result.get("positions_after_submit") is not None:
        payload["positions_after_submit"] = lifecycle_result.get("positions_after_submit")
    if lifecycle_result.get("executions_after_submit") is not None:
        payload["executions_after_submit"] = lifecycle_result.get("executions_after_submit")
    if lifecycle_result.get("completed_orders_after_submit") is not None:
        payload["completed_orders_after_submit"] = lifecycle_result.get("completed_orders_after_submit")
    if lifecycle_result.get("fill_verification") is not None:
        payload["fill_verification"] = lifecycle_result.get("fill_verification")
    if lifecycle_result.get("close_position_verification") is not None:
        payload["close_position_verification"] = lifecycle_result.get("close_position_verification")
    return payload


def _record_audit(
    audit_events: list[dict[str, Any]],
    *,
    event_type: str,
    config: IbkrManualPaperSubmitConfig,
    classification: str | None,
    detail: str,
    extra: dict[str, Any] | None = None,
) -> None:
    audit_events.append(_audit_row(event_type, config=config, classification=classification, detail=detail, extra=extra))


def _audit_row(
    event_type: str,
    *,
    config: IbkrManualPaperSubmitConfig,
    classification: str | None = None,
    detail: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "event_type": event_type,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "classification": classification,
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "client_id": config.client_id,
        "detail": detail,
        **dict(extra or {}),
    }


def _find_open_order(snapshot: dict[str, Any], order_id: int) -> dict[str, Any] | None:
    for row in list(snapshot.get("open_orders") or []):
        if int(row.get("broker_order_id") or 0) == int(order_id):
            return dict(row)
    return None


def _refresh_open_orders_snapshot(
    *,
    runtime: _SubmitRuntime,
    config: IbkrManualPaperSubmitConfig,
    selected_account_id: str,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    runtime.client.request_open_orders()
    runtime.client.record_event("req_open_orders_invoked", payload={"selected_account_id": selected_account_id})
    runtime.collector.reset_open_orders_ready()
    runtime.transport.req_open_orders()
    runtime.client.record_event("req_all_open_orders_invoked", payload={"selected_account_id": selected_account_id})
    runtime.transport.req_all_open_orders()
    if not _wait_for_event(
        runtime.collector.open_orders_ready,
        collector=runtime.collector,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    ):
        latest_error = runtime.collector.latest_error()
        raise IbkrManualPaperSubmitError(
            (
                f"Open-order refresh did not complete: {latest_error['message']}"
                if latest_error is not None
                else f"Open-order refresh did not complete within {timeout_seconds:.1f}s."
            )
        )
    return _build_open_orders_snapshot(
        config=_read_only_config_from_submit(config),
        client=runtime.client,
        selected_account_id=selected_account_id,
    )


def _build_callback_timeline(runtime: _SubmitRuntime) -> list[dict[str, Any]]:
    timeline: list[dict[str, Any]] = []
    request_log_fn = getattr(runtime.client, "request_log", None)
    requests = request_log_fn() if callable(request_log_fn) else ()
    for request in requests:
        timeline.append(
            {
                "observed_at": request.requested_at.isoformat(),
                "source": "request_log",
                "request_type": request.request_type,
                **dict(request.details or {}),
            }
        )
    drain_events_fn = getattr(runtime.client, "drain_events", None)
    events = drain_events_fn() if callable(drain_events_fn) else ()
    for event in events:
        timeline.append(
            {
                "observed_at": event.occurred_at.isoformat(),
                "source": "client_event",
                "event_type": event.event_type,
                **dict(event.payload or {}),
            }
        )
    timeline.extend(list(getattr(runtime.collector, "callback_timeline", []) or []))
    timeline.sort(key=lambda row: str(row.get("observed_at") or ""))
    return timeline


def _count_timeline_entries(timeline: list[dict[str, Any]], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in timeline:
        key = str(row.get(field_name) or "").strip()
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts


def _diagnose_order_observation(
    *,
    callback_summary: dict[str, int],
    request_summary: dict[str, int],
    callback_timeline: list[dict[str, Any]],
    report_errors: list[dict[str, Any]],
    executions_snapshot: list[dict[str, Any]],
    completed_orders_snapshot: list[dict[str, Any]],
    positions_snapshot: dict[str, Any],
) -> dict[str, Any]:
    _ = callback_timeline
    likely_root_cause = "observation_gap_after_manual_approval"
    conclusion = (
        "The partial paper-fill result was more likely caused by incomplete broker-truth observation after manual TWS approval than by a proven accepted order. "
        "The harness attempted submit and later cancel, but it recorded no openOrder, orderStatus, execDetails, or completedOrder callbacks for the submitted order."
    )
    blocking_gaps: list[str] = []
    if callback_summary.get("nextValidId", 0) == 0:
        blocking_gaps.append("nextValidId callback was not observed.")
    if callback_summary.get("openOrderEnd", 0) == 0:
        blocking_gaps.append("openOrderEnd callback was not observed during order observation requests.")
    if callback_summary.get("execDetailsEnd", 0) == 0:
        blocking_gaps.append("execDetailsEnd callback was not observed during execution requests.")
    if callback_summary.get("completedOrdersEnd", 0) == 0:
        blocking_gaps.append("completedOrdersEnd callback was not observed during completed-order requests.")
    if request_summary.get("open_orders", 0) == 0 or request_summary.get("executions", 0) == 0 or request_summary.get("completed_orders", 0) == 0:
        blocking_gaps.append("Not all observation requests were recorded in the client request log.")
    order_callbacks_seen = any(
        callback_summary.get(name, 0) > 0
        for name in ("openOrder", "orderStatus", "execDetails", "completedOrder")
    )
    mgc_execution_seen = any(str(row.get("symbol") or "").strip().upper() == _EXPECTED_SYMBOL for row in executions_snapshot)
    mgc_completed_seen = any(str(row.get("symbol") or "").strip().upper() == _EXPECTED_SYMBOL for row in completed_orders_snapshot)
    mgc_position_seen = any(
        str(row.get("symbol") or "").strip().upper() == _EXPECTED_SYMBOL
        for row in list(positions_snapshot.get("positions") or [])
    )
    if mgc_execution_seen or mgc_completed_seen or mgc_position_seen:
        likely_root_cause = "harness_failed_to_correlate_fill_truth"
        conclusion = (
            "The first manual paper submit very likely did transmit and fill. The dry-run observation pass found an MGC position plus execDetails/completedOrder truth, "
            "which points to a harness observation problem: the post-approval window was too short and later IBKR truth surfaced under permId-based records with broker_order_id=0 / client_id=0, "
            "so the original run did not correlate that fill back to submitted order id 1."
        )
    elif order_callbacks_seen:
        likely_root_cause = "order_callbacks_present_but_submit_gap_unclear"
        conclusion = (
            "The observation stack is capable of receiving order-related callbacks, so the first partial result may have been caused by timing, transmission, or a very fast order-state transition."
        )
    elif callback_summary.get("openOrderEnd", 0) and callback_summary.get("execDetailsEnd", 0) and callback_summary.get("completedOrdersEnd", 0):
        likely_root_cause = "order_not_transmitted_or_not_broker_visible"
        conclusion = (
            "The observation stack appears healthy for passive requests, but the first submit produced no order-related callbacks or broker-visible order rows. "
            "That points more toward the order not being transmitted, not being accepted into a broker-visible state, or being handled entirely inside the TWS confirmation path."
        )
    if any(int(row.get("code", 0)) in _SEVERE_CONNECTION_ERROR_CODES for row in report_errors):
        blocking_gaps.append("Severe IBKR connection errors were observed during the session.")
    classification = "IBKR_ORDER_OBSERVATION_READY_FOR_RETEST" if not blocking_gaps else "IBKR_ORDER_OBSERVATION_NEEDS_FIX"
    return {
        "classification": classification,
        "likely_root_cause": likely_root_cause,
        "conclusion": conclusion,
        "blocking_gaps": blocking_gaps,
        "recommended_instrumentation": [
            "Capture callback timeline rows for nextValidId, openOrder, openOrderEnd, orderStatus, error, execDetails, execDetailsEnd, completedOrder, completedOrdersEnd.",
            "Keep request-log evidence for reqOpenOrders, reqAllOpenOrders, reqExecutions, reqCompletedOrders, and reqPositions after manual approval.",
            "Extend the post-approval observation window before classifying fill timeout.",
            "Preserve order-id and nextValidId state in the report so future retests can correlate broker responses to the exact submitted order id.",
        ],
    }


def _wait_for_submitted_order_visibility(
    *,
    runtime: _SubmitRuntime,
    config: IbkrManualPaperSubmitConfig,
    selected_account_id: str,
    order_id: int,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest_snapshot = _not_run_snapshot("Broker open-order verification did not run.")
    while time.monotonic() < deadline:
        remaining = max(1.0, min(5.0, deadline - time.monotonic()))
        latest_snapshot = _refresh_open_orders_snapshot(
            runtime=runtime,
            config=config,
            selected_account_id=selected_account_id,
            timeout_seconds=remaining,
            sleep_fn=sleep_fn,
        )
        if _find_open_order(latest_snapshot, order_id) is not None:
            return latest_snapshot
        sleep_fn(0.25)
    return latest_snapshot


def _wait_for_order_absence(
    *,
    runtime: _SubmitRuntime,
    config: IbkrManualPaperSubmitConfig,
    selected_account_id: str,
    order_id: int,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest_snapshot = _not_run_snapshot("Cancel verification did not run.")
    while time.monotonic() < deadline:
        remaining = max(1.0, min(5.0, deadline - time.monotonic()))
        latest_snapshot = _refresh_open_orders_snapshot(
            runtime=runtime,
            config=config,
            selected_account_id=selected_account_id,
            timeout_seconds=remaining,
            sleep_fn=sleep_fn,
        )
        if _find_open_order(latest_snapshot, order_id) is None:
            return latest_snapshot
        sleep_fn(0.25)
    return latest_snapshot


def _prompt_for_tws_manual_confirmation(
    *,
    timeout_seconds: float,
    order_id: int,
    preview_digest: str,
) -> dict[str, Any]:
    del order_id, preview_digest
    if not sys.stdin.isatty():
        return {
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": "unavailable",
            "response_text": None,
            "detail": "Manual TWS confirmation requires an interactive terminal/TTY. Re-run the manual harness from an interactive shell when you are ready to handle the dialog.",
            "timed_out": False,
        }
    deadline = time.monotonic() + float(timeout_seconds)
    message = (
        "Check TWS now. If a confirmation dialog is visible, approve or reject it manually. "
        "Press Enter here only after you have handled the TWS dialog."
    )
    print(message, flush=True)
    print("Type 'approved' if you approved the TWS dialog, or 'rejected' if you rejected/cancelled it, then press Enter.", flush=True)
    while time.monotonic() < deadline:
        remaining = max(0.0, deadline - time.monotonic())
        print(f"[manual-confirmation timeout in {remaining:.0f}s] > ", end="", flush=True)
        ready, _, _ = select.select([sys.stdin], [], [], remaining)
        if not ready:
            break
        response_text = sys.stdin.readline().strip()
        normalized = response_text.lower()
        if normalized in {"approved", "approve"}:
            return {
                "state": _MANUAL_CONFIRMATION_WAIT_STATE,
                "operator_outcome": "approved",
                "response_text": response_text,
                "detail": "Operator confirmed that the TWS dialog was approved manually.",
                "timed_out": False,
            }
        if normalized in {"rejected", "reject", "cancelled", "canceled"}:
            return {
                "state": _MANUAL_CONFIRMATION_WAIT_STATE,
                "operator_outcome": "rejected",
                "response_text": response_text,
                "detail": "Operator confirmed that the TWS dialog was rejected or cancelled manually.",
                "timed_out": False,
            }
        print("Enter 'approved' or 'rejected' after you handle the TWS dialog.", flush=True)
    return {
        "state": _MANUAL_CONFIRMATION_WAIT_STATE,
        "operator_outcome": "timeout",
        "response_text": None,
        "detail": f"Timed out after {timeout_seconds:.1f}s waiting for explicit manual confirmation about the TWS dialog.",
        "timed_out": True,
    }


def _extract_quote_fields(raw_row: dict[str, Any]) -> dict[str, Any]:
    tick_prices = dict(raw_row.get("tick_prices") or {})
    bid_price = _first_tick_price(tick_prices, _BID_TICK_IDS)
    ask_price = _first_tick_price(tick_prices, _ASK_TICK_IDS)
    last_price = _first_tick_price(tick_prices, _LAST_TICK_IDS)
    close_price = _first_tick_price(tick_prices, _CLOSE_TICK_IDS)
    return {
        "bid_price": bid_price,
        "ask_price": ask_price,
        "last_price": last_price,
        "close_price": close_price,
        "any_bid_tick": bid_price is not None,
        "any_ask_tick": ask_price is not None,
        "any_last_tick": last_price is not None,
        "any_close_tick": close_price is not None,
        "any_tick_returned": any(value is not None for value in (bid_price, ask_price, last_price, close_price)),
    }


def _first_tick_price(tick_prices: dict[str, Any], tick_ids: tuple[int, ...]) -> float | None:
    for tick_id in tick_ids:
        value = tick_prices.get(str(tick_id))
        if value is not None:
            return float(value)
    return None


def _snapshot_digest(snapshot: dict[str, Any]) -> str:
    payload = {
        "selected_account_id": snapshot.get("selected_account_id"),
        "open_order_count": snapshot.get("open_order_count"),
        "open_orders": [
            {
                "broker_order_id": row.get("broker_order_id"),
                "symbol": row.get("symbol"),
                "status": row.get("status"),
                "quantity": row.get("quantity"),
                "limit_price": row.get("limit_price"),
                "perm_id": row.get("perm_id"),
            }
            for row in list(snapshot.get("open_orders") or [])
        ],
    }
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _not_run_snapshot(detail: str) -> dict[str, Any]:
    return {
        "status": "not_run",
        "detail": detail,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _first_failed_guardrail_detail(guardrail_checks: list[dict[str, Any]]) -> str:
    for check in guardrail_checks:
        if check.get("blocking") and not check.get("passed"):
            return str(check.get("detail") or f"Blocking guardrail failed: {check.get('name')}")
    return "The manual paper submit harness was blocked by a required guardrail."


def _read_only_config_from_submit(config: IbkrManualPaperSubmitConfig) -> Any:
    from .ibkr_read_only_verifier import IbkrReadOnlyVerificationConfig

    return IbkrReadOnlyVerificationConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        read_only=True,
        account_id=config.account_id,
        timeout_seconds=config.timeout_seconds,
        probe_market_data=False,
        probe_duplicate_client_id=False,
        gc_expiry=_EXPECTED_EXPIRY,
        mgc_expiry=_EXPECTED_EXPIRY,
    )


def _preview_config_from_submit(config: IbkrManualPaperSubmitConfig) -> Any:
    from .ibkr_paper_order_preview import IbkrPaperOrderPreviewConfig

    return IbkrPaperOrderPreviewConfig(
        repo_root=config.repo_root,
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        symbol=config.symbol,
        expiry=config.expiry,
        action=config.action,
        quantity=config.quantity,
        order_type=config.order_type,
        limit_price=config.limit_price,
        time_in_force=config.time_in_force,
        timeout_seconds=config.timeout_seconds,
        caller_path=config.caller_path,
    )


def _build_submit_bridge(*, wrapper_cls: type[Any], client_cls: type[Any], collector: IbkrManualPaperSubmitCollector) -> Any:
    class _Bridge(wrapper_cls, client_cls):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            wrapper_cls.__init__(self)
            client_cls.__init__(self, wrapper=self)

        def nextValidId(self, orderId: int) -> None:  # noqa: N802
            collector.record_callback("nextValidId", order_id=int(orderId))
            collector.next_valid_id(orderId)

        def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
            collector.record_callback("managedAccounts", accounts_list=str(accountsList or ""))
            collector.managed_accounts(accountsList)

        def updateAccountValue(self, key: str, value: str, currency: str, accountName: str) -> None:  # noqa: N802
            collector.record_callback(
                "updateAccountValue",
                key=str(key),
                value=str(value),
                currency=str(currency),
                account_id=str(accountName),
            )
            collector.update_account_value(account_id=accountName, key=key, value=value, currency=currency)

        def accountDownloadEnd(self, accountName: str) -> None:  # noqa: N802, ARG002
            collector.record_callback("accountDownloadEnd", account_id=str(accountName))
            collector.account_download_end()

        def updatePortfolio(  # noqa: N802
            self,
            contract: Any,
            position: float,
            marketPrice: float,
            marketValue: float,
            averageCost: float,
            unrealizedPNL: float,
            realizedPNL: float,
            accountName: str,
        ) -> None:
            collector.record_callback(
                "updatePortfolio",
                account_id=str(accountName),
                contract=_contract_payload(contract),
                quantity=float(position),
                market_price=float(marketPrice),
                market_value=float(marketValue),
                average_cost=float(averageCost),
                unrealized_pnl=float(unrealizedPNL),
                realized_pnl=float(realizedPNL),
            )
            update_portfolio_fn = getattr(collector, "update_portfolio", None)
            if callable(update_portfolio_fn):
                update_portfolio_fn(
                    account_id=accountName,
                    contract=_contract_payload(contract),
                    quantity=position,
                    market_price=marketPrice,
                    market_value=marketValue,
                    average_cost=averageCost,
                    unrealized_pnl=unrealizedPNL,
                    realized_pnl=realizedPNL,
                )

        def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:  # noqa: N802
            collector.record_callback(
                "position",
                account_id=str(account),
                contract=_contract_payload(contract),
                quantity=float(pos),
                average_cost=float(avgCost),
            )
            collector.position(
                account_id=account,
                contract=_contract_payload(contract),
                quantity=pos,
                average_cost=avgCost,
            )

        def positionEnd(self) -> None:  # noqa: N802
            collector.record_callback("positionEnd")
            collector.position_end()

        def openOrder(self, orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
            collector.record_callback(
                "openOrder",
                order_id=int(orderId),
                account_id=str(getattr(order, "account", "") or ""),
                client_id=int(getattr(order, "clientId", 0) or 0),
                perm_id=getattr(order, "permId", None),
                contract=_contract_payload(contract),
                status=str(getattr(orderState, "status", "") or ""),
                total_quantity=getattr(order, "totalQuantity", 0),
                limit_price=getattr(order, "lmtPrice", None),
            )
            collector.open_order(
                account_id=getattr(order, "account", "") or "",
                broker_order_id=orderId,
                client_id=getattr(order, "clientId", 0),
                perm_id=getattr(order, "permId", None),
                contract=_contract_payload(contract),
                status=getattr(orderState, "status", "") or "",
                quantity=getattr(order, "totalQuantity", 0),
                filled_quantity=getattr(order, "filledQuantity", None),
                limit_price=getattr(order, "lmtPrice", None),
                stop_price=getattr(order, "auxPrice", None),
            )

        def openOrderEnd(self) -> None:  # noqa: N802
            collector.record_callback("openOrderEnd")
            collector.open_order_end()

        def completedOrder(self, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
            collector.record_callback(
                "completedOrder",
                order_id=int(getattr(order, "orderId", 0) or 0),
                account_id=str(getattr(order, "account", "") or ""),
                client_id=int(getattr(order, "clientId", 0) or 0),
                perm_id=getattr(order, "permId", None),
                contract=_contract_payload(contract),
                status=str(getattr(orderState, "status", "") or ""),
                total_quantity=getattr(order, "totalQuantity", 0),
            )
            collector.completed_order(
                account_id=getattr(order, "account", "") or "",
                broker_order_id=getattr(order, "orderId", 0),
                client_id=getattr(order, "clientId", 0),
                perm_id=getattr(order, "permId", None),
                contract=_contract_payload(contract),
                status=getattr(orderState, "status", "") or "",
                quantity=getattr(order, "totalQuantity", 0),
            )

        def completedOrdersEnd(self) -> None:  # noqa: N802
            collector.record_callback("completedOrdersEnd")
            collector.completed_orders_end()

        def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str) -> None:  # noqa: N802
            collector.record_callback(
                "accountSummary",
                request_id=int(reqId),
                account_id=str(account),
                tag=str(tag),
                value=str(value),
                currency=str(currency),
            )
            collector.account_summary(request_id=reqId, account_id=account, tag=tag, value=value, currency=currency)

        def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802
            collector.record_callback("accountSummaryEnd", request_id=int(reqId))
            collector.account_summary_end(request_id=reqId)

        def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
            collector.record_callback("contractDetails", request_id=int(reqId))
            collector.contract_details(request_id=reqId, contract_details=contractDetails)

        def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
            collector.record_callback("contractDetailsEnd", request_id=int(reqId))
            collector.contract_details_end(request_id=reqId)

        def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
            collector.record_callback("marketDataType", request_id=int(reqId), market_data_type=int(marketDataType))
            collector.market_data_type(request_id=reqId, market_data_type=marketDataType)

        def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802, ARG002
            collector.record_callback("tickPrice", request_id=int(reqId), tick_type=int(tickType), price=float(price))
            collector.tick_price(request_id=reqId, tick_type=tickType, price=price)

        def tickSize(self, reqId: int, tickType: int, size: int) -> None:  # noqa: N802
            collector.record_callback("tickSize", request_id=int(reqId), tick_type=int(tickType), size=int(size))
            collector.tick_size(request_id=reqId, tick_type=tickType, size=size)

        def tickString(self, reqId: int, tickType: int, value: str) -> None:  # noqa: N802
            collector.record_callback("tickString", request_id=int(reqId), tick_type=int(tickType), value=str(value))
            collector.tick_string(request_id=reqId, tick_type=tickType, value=value)

        def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
            collector.record_callback("tickSnapshotEnd", request_id=int(reqId))
            collector.tick_snapshot_end(request_id=reqId)

        def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802
            collector.record_callback(
                "execDetails",
                request_id=int(reqId),
                account_id=str(getattr(execution, "acctNumber", "") or ""),
                execution_id=str(getattr(execution, "execId", "") or ""),
                broker_order_id=getattr(execution, "orderId", None),
                client_id=getattr(execution, "clientId", None),
                perm_id=getattr(execution, "permId", None),
                side=getattr(execution, "side", None),
                quantity=getattr(execution, "shares", 0),
                price=getattr(execution, "price", None),
                contract=_contract_payload(contract),
            )
            collector.exec_details(
                request_id=reqId,
                account_id=getattr(execution, "acctNumber", "") or "",
                execution_id=getattr(execution, "execId", "") or "",
                broker_order_id=getattr(execution, "orderId", None),
                client_id=getattr(execution, "clientId", None),
                perm_id=getattr(execution, "permId", None),
                contract=_contract_payload(contract),
                side=getattr(execution, "side", None),
                quantity=getattr(execution, "shares", 0),
                price=getattr(execution, "price", None),
            )

        def execDetailsEnd(self, reqId: int) -> None:  # noqa: N802
            collector.record_callback("execDetailsEnd", request_id=int(reqId))
            collector.exec_details_end(request_id=reqId)

        def orderStatus(  # noqa: N802
            self,
            orderId: int,
            status: str,
            filled: float,
            remaining: float,
            avgFillPrice: float,
            permId: int,
            parentId: int,
            lastFillPrice: float,
            clientId: int,
            whyHeld: str,
            mktCapPrice: float,
        ) -> None:
            collector.record_callback(
                "orderStatus",
                order_id=int(orderId),
                status=str(status),
                filled=float(filled),
                remaining=float(remaining),
                avg_fill_price=float(avgFillPrice),
                perm_id=int(permId) if permId is not None else None,
                parent_id=int(parentId) if parentId is not None else None,
                last_fill_price=float(lastFillPrice),
                client_id=int(clientId) if clientId is not None else None,
                why_held=str(whyHeld or ""),
                mkt_cap_price=float(mktCapPrice),
            )
            collector.order_status(
                order_id=orderId,
                status=status,
                filled=filled,
                remaining=remaining,
                avg_fill_price=avgFillPrice,
                perm_id=permId,
                parent_id=parentId,
                last_fill_price=lastFillPrice,
                client_id=clientId,
                why_held=whyHeld,
                mkt_cap_price=mktCapPrice,
            )

        def error(self, reqId: int, errorCode: int, errorString: str, *args: Any) -> None:  # noqa: N802, ARG002
            collector.record_callback(
                "error",
                request_id=int(reqId),
                error_code=int(errorCode),
                error_message=str(errorString),
            )
            collector.error(code=errorCode, message=errorString, request_id=reqId)

    return _Bridge()


def _contract_payload(contract: Any) -> dict[str, Any]:
    return {
        "conId": getattr(contract, "conId", None),
        "symbol": getattr(contract, "symbol", None),
        "localSymbol": getattr(contract, "localSymbol", None),
        "securityType": getattr(contract, "secType", None),
        "exchange": getattr(contract, "exchange", None),
        "currency": getattr(contract, "currency", None),
        "lastTradeDateOrContractMonth": getattr(contract, "lastTradeDateOrContractMonth", None),
        "multiplier": getattr(contract, "multiplier", None),
        "tradingClass": getattr(contract, "tradingClass", None),
    }
