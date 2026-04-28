"""Manual-only IBKR paper submit/cancel test harness for one MGC limit order."""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
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


class IbkrManualPaperSubmitError(RuntimeError):
    """Base error for the manual paper submit/cancel harness."""


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
    timeout_seconds: float = 15.0
    manual_confirmation_timeout_seconds: float = 90.0
    caller_path: str = "manual_cli"
    submit: bool = False
    approval_digest: str | None = None
    approval_phrase: str | None = None


@dataclass(frozen=True)
class IbkrManualPaperSubmitArtifacts:
    classification: str
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]
    open_order_before: dict[str, Any]
    open_order_after_submit: dict[str, Any]
    open_order_after_cancel: dict[str, Any]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED" else 1


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
        raw_order.orderId = int(order_id)
        raw_order.account = str(account_id)
        raw_order.action = str(action)
        raw_order.totalQuantity = float(quantity)
        raw_order.orderType = "LMT"
        raw_order.lmtPrice = float(limit_price)
        raw_order.tif = str(time_in_force)
        raw_order.transmit = True
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
    requested_order = _normalize_requested_order(config)
    caller_check = evaluate_manual_preview_caller(caller_path=config.caller_path, stack_provider=stack_provider)
    environment_lock = evaluate_paper_preview_environment_lock(
        mode=config.mode,
        host=config.host,
        port=config.port,
    )
    input_checks = _submit_input_guardrails(requested_order)
    guardrail_checks = _preflight_guardrail_checks(
        caller_check=caller_check,
        environment_lock=environment_lock,
        input_guardrails=input_checks,
    )
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
        preview_payload = _build_submit_preview_payload(
            requested_order=requested_order,
            context=context,
            guardrail_checks=guardrail_checks,
        )
        preview_digest = build_preview_digest(preview_payload)
        expected_phrase = build_submit_approval_phrase(
            selected_account_id=context["selected_account_id"],
            digest=preview_digest,
            requested_order=requested_order,
            delayed_quote_warning=context["quote_context"].get("live_market_data_warning"),
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
            },
        )

        if not config.submit:
            classification = "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PARTIAL"
            report = _build_report(
                config=config,
                started_at=started_at,
                classification=classification,
                caller_check=caller_check,
                environment_lock=environment_lock,
                context=context,
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
            )
            return IbkrManualPaperSubmitArtifacts(
                classification=classification,
                report=report,
                audit_events=audit_events,
                open_order_before=context["open_orders_before"],
                open_order_after_submit=_not_run_snapshot("Submit was not requested."),
                open_order_after_cancel=_not_run_snapshot("Cancel was not requested because submit did not run."),
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
                classification="IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED",
                caller_check=caller_check,
                environment_lock=environment_lock,
                context=context,
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
            )
            return IbkrManualPaperSubmitArtifacts(
                classification="IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED",
                report=report,
                audit_events=audit_events,
                open_order_before=context["open_orders_before"],
                open_order_after_submit=_not_run_snapshot("Submit was blocked by approval validation."),
                open_order_after_cancel=_not_run_snapshot("Cancel was not requested because submit was blocked."),
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
        classification = _classify_submit_lifecycle(lifecycle_result["status"])
        report = _build_report(
            config=config,
            started_at=started_at,
            classification=classification,
            caller_check=caller_check,
            environment_lock=environment_lock,
            context=context,
            requested_order=requested_order,
            guardrail_checks=guardrail_checks,
            preview_payload=preview_payload,
            preview_digest=preview_digest,
            expected_phrase=expected_phrase,
            audit_events=audit_events,
            lifecycle_result=lifecycle_result,
        )
        return IbkrManualPaperSubmitArtifacts(
            classification=classification,
            report=report,
            audit_events=audit_events,
            open_order_before=context["open_orders_before"],
            open_order_after_submit=lifecycle_result["open_order_after_submit"],
            open_order_after_cancel=lifecycle_result["open_order_after_cancel"],
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


def build_submit_approval_phrase(
    *,
    selected_account_id: str,
    digest: str,
    requested_order: dict[str, Any],
    delayed_quote_warning: str | None,
) -> str:
    delayed_ack = "yes" if str(delayed_quote_warning or "").strip() else "no"
    return (
        "APPROVE IBKR PAPER SUBMIT "
        f"account={selected_account_id} mode=PAPER host=127.0.0.1 port=7497 "
        f"contract={requested_order['symbol']} {requested_order['expiry']} "
        f"action={requested_order['action']} qty=1 type=LMT price={requested_order['limit_price']} "
        f"tif=DAY digest={digest} delayed_quote_ack={delayed_ack}"
    )


def write_ibkr_manual_paper_submit_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrManualPaperSubmitArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_manual_paper_submit_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_manual_paper_submit_report.md").write_text(
        render_ibkr_manual_paper_submit_markdown(artifacts.report),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_manual_paper_submit_open_order_before.json").write_text(
        json.dumps(artifacts.open_order_before, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_manual_paper_submit_open_order_after_submit.json").write_text(
        json.dumps(artifacts.open_order_after_submit, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_manual_paper_submit_open_order_after_cancel.json").write_text(
        json.dumps(artifacts.open_order_after_cancel, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    with (reports_dir / "ibkr_manual_paper_submit_audit.jsonl").open("a", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def render_ibkr_manual_paper_submit_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    environment = dict(report.get("environment_lock_check") or {})
    preview = dict(report.get("preview") or {})
    lifecycle = dict(report.get("submit_cancel_lifecycle") or {})
    lines = [
        "# IBKR Manual Paper Submit Report",
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
        "- MGC 202606 only",
        "- LMT DAY only",
        "- qty = 1 only",
        "- no market orders",
        "- no bracket/OCO",
        "- no strategy linkage",
        "- no ATP/GC execution",
        "- no scheduler",
        "- environment lock is PAPER / 127.0.0.1 / 7497 only",
        "- live port 7496 fails closed",
        "- IB Gateway ports 4001 and 4002 fail closed",
        "- unknown ports fail closed",
        f"- preview digest: `{preview.get('preview_digest')}`",
        f"- expected approval phrase: `{preview.get('expected_approval_phrase')}`",
        f"- lifecycle status: `{lifecycle.get('status')}`",
        f"- lifecycle detail: {lifecycle.get('detail')}",
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
        f"- estimated_notional: `{preview.get('estimated_notional')}`",
        f"- estimated_tick_value: `{preview.get('estimated_tick_value')}`",
        "",
        "## Guardrails",
        "",
    ]
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


def _submit_input_guardrails(requested_order: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "whitelisted_contract": {
            "passed": requested_order.get("symbol") == _EXPECTED_SYMBOL and requested_order.get("expiry") == _EXPECTED_EXPIRY,
            "detail": "Only MGC 202606 is allowed in the first manual paper submit/cancel harness.",
        },
        "supported_action": {
            "passed": requested_order.get("action") == _EXPECTED_ACTION,
            "detail": "Only BUY is allowed in the first manual paper submit/cancel harness.",
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
            "passed": requested_order.get("limit_price") is not None,
            "detail": "A limit price is required for the first manual paper submit/cancel harness.",
        },
        "limit_price_positive": {
            "passed": requested_order.get("limit_price") is not None and float(requested_order.get("limit_price")) > 0.0,
            "detail": "The limit price must be positive.",
        },
        "time_in_force_supported": {
            "passed": str(requested_order.get("time_in_force") or "").strip().upper() == _EXPECTED_TIF,
            "detail": "Only DAY time-in-force is allowed in the first manual paper submit/cancel harness.",
        },
    }


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
    positions = _build_positions_snapshot(client=runtime.client, selected_account_id=selected_account_id)
    open_orders_before = _build_open_orders_snapshot(client=runtime.client, selected_account_id=selected_account_id)
    if open_orders_before.get("open_order_count"):
        mgc_rows = [
            row
            for row in list(open_orders_before.get("open_orders") or [])
            if str(row.get("symbol") or "").strip().upper() == _EXPECTED_SYMBOL
        ]
        if mgc_rows:
            raise IbkrManualPaperSubmitError(
                "Open-order baseline contains working MGC orders. Cancel them manually in TWS before rerunning the manual paper submit/cancel test."
            )
        raise IbkrManualPaperSubmitError(
            f"Open-order baseline is not empty for the first manual submit test: {open_orders_before['open_order_count']} existing open orders."
        )
    contract_report = _qualify_mgc_contract(
        transport=runtime.transport,
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if not contract_report["ok"]:
        raise IbkrManualPaperSubmitError(str(contract_report["detail"]))
    quote_context = _probe_delayed_quote_context(
        transport=runtime.transport,
        collector=runtime.collector,
        contract=contract_report["qualified_contract_object"],
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    audit_events.extend(
        [
            _audit_row("account_truth_read", config=config, detail="Account truth captured.", extra={"account_id": selected_account_id}),
            _audit_row("positions_read", config=config, detail="Current positions captured.", extra={"position_count": positions.get("position_count")}),
            _audit_row("open_order_baseline_read", config=config, detail="Open-order baseline captured.", extra={"open_order_count": open_orders_before.get("open_order_count")}),
            _audit_row("contract_qualified", config=config, detail="MGC 202606 qualified successfully.", extra={"qualified_contract_identifier": contract_report.get("qualified_contract_identifier")}),
        ]
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
    resolver = IbkrContractResolver()
    qualified = resolver.qualify_futures(symbol=_EXPECTED_SYMBOL, expiry=_EXPECTED_EXPIRY)
    request_id = 9501
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
    qualified_dict = _qualified_contract_to_dict(qualified)
    if api_details.get("con_id") is not None:
        qualified_dict["con_id"] = api_details["con_id"]
        qualified = IbkrQualifiedContract(
            internal_symbol=qualified.internal_symbol,
            broker_symbol=qualified.broker_symbol,
            local_symbol=qualified.local_symbol,
            security_type=qualified.security_type,
            exchange=qualified.exchange,
            currency=qualified.currency,
            expiry=qualified.expiry,
            multiplier=qualified.multiplier,
            trading_class=qualified.trading_class,
            con_id=api_details["con_id"],
            metadata=qualified.metadata,
        )
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
    active_probe = delayed_probe if delayed_probe.get("any_tick_returned") else live_probe
    live_market_data_available = bool(
        live_probe.get("any_tick_returned") and live_probe.get("response_indication") == "data_returned"
    )
    warning = (
        "Live market data is unavailable in this paper session. The preview uses delayed data only."
        if not live_market_data_available
        else None
    )
    return {
        "live_probe": live_probe,
        "delayed_probe": delayed_probe,
        "has_quote": bool(active_probe.get("any_tick_returned")),
        "quote_source_label": "DELAYED" if delayed_probe.get("any_tick_returned") else ("LIVE" if live_market_data_available else "UNAVAILABLE"),
        "live_market_data_available": live_market_data_available,
        "live_market_data_warning": warning,
        "bid_price": active_probe.get("bid_price"),
        "ask_price": active_probe.get("ask_price"),
        "last_price": active_probe.get("last_price"),
        "close_price": active_probe.get("close_price"),
        "delayed_data_warning_present": bool(warning) or bool(delayed_probe.get("any_tick_returned")),
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
        "response_code": response_code,
        "response_message": response_message,
        "response_indication": indication,
        "errors": errors,
        **quote_fields,
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
    refreshed_before_submit = _refresh_open_orders_snapshot(
        runtime=runtime,
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
        extra={"order_id": order_id, "preview_digest": preview_digest},
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
        after_submit = _wait_for_submitted_order_visibility(
            runtime=runtime,
            selected_account_id=context["selected_account_id"],
            order_id=order_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
    else:
        after_submit = _refresh_open_orders_snapshot(
            runtime=runtime,
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
) -> dict[str, Any]:
    contract_report = dict(context["contract_report"])
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    quote_context = dict(context["quote_context"])
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
        },
        "quote_context": {
            "quote_source_label": quote_context.get("quote_source_label"),
            "live_market_data_available": quote_context.get("live_market_data_available"),
            "live_market_data_warning": quote_context.get("live_market_data_warning"),
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
) -> dict[str, Any]:
    quote_context = dict(context.get("quote_context") or {})
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
            "symbol": requested_order["symbol"],
            "expiry": requested_order["expiry"],
            "action": requested_order["action"],
            "quantity": requested_order["quantity"],
            "order_type": requested_order["order_type"],
            "limit_price": requested_order["limit_price"],
            "time_in_force": requested_order["time_in_force"],
            "quote_source_label": quote_context.get("quote_source_label"),
            "live_market_data_warning": quote_context.get("live_market_data_warning"),
            "estimated_notional": estimated_notional,
            "estimated_tick_value": estimated_tick_value,
            "preview_digest": preview_digest,
            "expected_approval_phrase": expected_phrase,
        },
        "preview_payload": preview_payload,
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
    _record_audit(
        audit_events,
        event_type="failed_closed",
        config=config,
        classification="IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED",
        detail=detail,
    )
    report = {
        "classification": "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED",
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
        classification="IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED",
        report=report,
        audit_events=audit_events,
        open_order_before=_not_run_snapshot("Open-order baseline was not captured."),
        open_order_after_submit=_not_run_snapshot("Submit did not run."),
        open_order_after_cancel=_not_run_snapshot("Cancel did not run."),
    )


def _classify_submit_lifecycle(status: str) -> str:
    if status == "passed":
        return "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PASSED"
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
        return "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PARTIAL"
    if status == "manual_confirmation_unavailable":
        return "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    return "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


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
    selected_account_id: str,
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    runtime.client.request_open_orders()
    runtime.collector.reset_open_orders_ready()
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
    return _build_open_orders_snapshot(client=runtime.client, selected_account_id=selected_account_id)


def _wait_for_submitted_order_visibility(
    *,
    runtime: _SubmitRuntime,
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
            collector.next_valid_id(orderId)

        def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
            collector.managed_accounts(accountsList)

        def updateAccountValue(self, key: str, value: str, currency: str, accountName: str) -> None:  # noqa: N802
            collector.update_account_value(account_id=accountName, key=key, value=value, currency=currency)

        def accountDownloadEnd(self, accountName: str) -> None:  # noqa: N802, ARG002
            collector.account_download_end()

        def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:  # noqa: N802
            collector.position(
                account_id=account,
                contract=_contract_payload(contract),
                quantity=pos,
                average_cost=avgCost,
            )

        def positionEnd(self) -> None:  # noqa: N802
            collector.position_end()

        def openOrder(self, orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
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
            collector.open_order_end()

        def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str) -> None:  # noqa: N802
            collector.account_summary(request_id=reqId, account_id=account, tag=tag, value=value, currency=currency)

        def accountSummaryEnd(self, reqId: int) -> None:  # noqa: N802
            collector.account_summary_end(request_id=reqId)

        def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
            collector.contract_details(request_id=reqId, contract_details=contractDetails)

        def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
            collector.contract_details_end(request_id=reqId)

        def marketDataType(self, reqId: int, marketDataType: int) -> None:  # noqa: N802
            collector.market_data_type(request_id=reqId, market_data_type=marketDataType)

        def tickPrice(self, reqId: int, tickType: int, price: float, attrib: Any) -> None:  # noqa: N802, ARG002
            collector.tick_price(request_id=reqId, tick_type=tickType, price=price)

        def tickSize(self, reqId: int, tickType: int, size: int) -> None:  # noqa: N802
            collector.tick_size(request_id=reqId, tick_type=tickType, size=size)

        def tickString(self, reqId: int, tickType: int, value: str) -> None:  # noqa: N802
            collector.tick_string(request_id=reqId, tick_type=tickType, value=value)

        def tickSnapshotEnd(self, reqId: int) -> None:  # noqa: N802
            collector.tick_snapshot_end(request_id=reqId)

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
