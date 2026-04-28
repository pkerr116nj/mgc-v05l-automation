"""IBKR manual paper-order preview harness with no submit capability."""

from __future__ import annotations

import hashlib
import inspect
import json
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
from .ibkr_read_only_verifier import (
    IbkrReadOnlyApiTransport,
    IbkrReadOnlyApiTransportConfig,
    IbkrReadOnlyProbeCollector,
    IbkrReadOnlyVerificationError,
    _build_open_orders_snapshot,
    _empty_market_data_row,
    _qualified_contract_to_dict,
    _resolve_selected_account_id,
    _tws_manual_check_message,
    _wait_for_connection_ready,
    _wait_for_event,
)

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_POLL_INTERVAL_SECONDS = 0.05
_ALLOWED_CONTRACTS = {
    ("GC", "202606"),
    ("MGC", "202606"),
}
_ALLOWED_ACTIONS = {"BUY", "SELL"}
_ALLOWED_ORDER_TYPES = {"LMT"}
_ALLOWED_TIFS = {"DAY"}
_BID_TICK_IDS = (1, 66)
_ASK_TICK_IDS = (2, 67)
_LAST_TICK_IDS = (4, 68)
_CLOSE_TICK_IDS = (9, 75)
_PERMISSION_ERROR_CODES = {354, 10167, 10168}
_CONNECTION_ERROR_CODES = {502, 504, 1100, 1101, 1102, 1300, 326}
_FORBIDDEN_CALLER_PREFIXES = (
    "mgc_v05l.strategy",
    "mgc_v05l.app.probationary_runtime",
)
_FORBIDDEN_CALLER_SUBSTRINGS = (
    "scheduler",
    "atp_gc",
    "atp_companion",
)


class IbkrPaperOrderPreviewError(RuntimeError):
    """Base error for the preview-only harness."""


@dataclass(frozen=True)
class IbkrPaperOrderPreviewConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    symbol: str
    expiry: str
    action: str
    quantity: float
    order_type: str
    limit_price: float | None
    time_in_force: str = "DAY"
    account_id: str | None = None
    timeout_seconds: float = 12.0
    caller_path: str = "manual_cli"


@dataclass(frozen=True)
class IbkrPaperOrderPreviewArtifacts:
    classification: str
    report: dict[str, Any]
    audit_entry: dict[str, Any]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_PAPER_ORDER_PREVIEW_BLOCKED" else 1


@dataclass
class _PreviewRuntime:
    session: IbkrSession
    client: IbkrClient
    collector: IbkrReadOnlyProbeCollector
    transport: Any


def run_ibkr_paper_order_preview(
    *,
    config: IbkrPaperOrderPreviewConfig,
    transport_factory: Callable[..., Any] = IbkrReadOnlyApiTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> IbkrPaperOrderPreviewArtifacts:
    started_at = datetime.now(timezone.utc)
    manual_caller_check = evaluate_manual_preview_caller(
        caller_path=config.caller_path,
        stack_provider=stack_provider,
    )
    environment_lock = evaluate_paper_preview_environment_lock(
        mode=config.mode,
        host=config.host,
        port=config.port,
    )
    requested_order = _normalize_requested_order(config)
    input_guardrails = evaluate_preview_input_guardrails(requested_order)
    preflight_checks = _preflight_guardrail_checks(
        manual_caller_check=manual_caller_check,
        environment_lock=environment_lock,
        input_guardrails=input_guardrails,
    )
    if any(check["blocking"] and not check["passed"] for check in preflight_checks):
        detail = _first_failed_guardrail_detail(preflight_checks)
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            requested_order=requested_order,
            manual_caller_check=manual_caller_check,
            environment_lock=environment_lock,
            guardrail_checks=preflight_checks,
            detail=detail,
        )

    try:
        runtime_context = _collect_preview_runtime_context(
            config=config,
            transport_factory=transport_factory,
            module_loader=module_loader,
            sleep_fn=sleep_fn,
            requested_order=requested_order,
            started_at=started_at,
        )
    except Exception as exc:
        return _blocked_artifacts(
            config=config,
            started_at=started_at,
            requested_order=requested_order,
            manual_caller_check=manual_caller_check,
            environment_lock=environment_lock,
            guardrail_checks=preflight_checks,
            detail=str(exc),
        )

    guardrail_checks = list(preflight_checks)
    guardrail_checks.extend(
        _runtime_guardrail_checks(
            config=config,
            runtime_context=runtime_context,
        )
    )
    preview_payload = _build_preview_payload(
        requested_order=requested_order,
        runtime_context=runtime_context,
        guardrail_checks=guardrail_checks,
    )
    preview_digest = build_preview_digest(preview_payload)
    guardrail_checks.append(
        _guardrail_check(
            "deterministic_preview_digest_generated",
            passed=bool(preview_digest),
            blocking=True,
            detail="The preview digest is a SHA-256 hash of the deterministic preview payload.",
        )
    )
    classification = classify_paper_order_preview(guardrail_checks=guardrail_checks)
    report = _build_preview_report(
        config=config,
        started_at=started_at,
        requested_order=requested_order,
        manual_caller_check=manual_caller_check,
        environment_lock=environment_lock,
        runtime_context=runtime_context,
        guardrail_checks=guardrail_checks,
        preview_payload=preview_payload,
        preview_digest=preview_digest,
        classification=classification,
    )
    audit_entry = _build_audit_entry(report=report)
    return IbkrPaperOrderPreviewArtifacts(
        classification=classification,
        report=report,
        audit_entry=audit_entry,
    )


def evaluate_paper_preview_environment_lock(*, mode: str, host: str, port: int) -> dict[str, Any]:
    normalized_mode = str(mode or "").strip().upper()
    normalized_host = str(host or "").strip()
    normalized_port = int(port)
    mismatches: list[str] = []
    if normalized_mode != _EXPECTED_MODE:
        mismatches.append("mode")
    if normalized_host != _EXPECTED_HOST:
        mismatches.append("host")
    if normalized_port != _EXPECTED_PORT:
        mismatches.append("port")
    if normalized_port == 7496:
        port_policy = "Live TWS port 7496 is forbidden for this preview harness."
    elif normalized_port == 4001:
        port_policy = "IB Gateway live port 4001 is forbidden for this preview harness."
    elif normalized_port == 4002:
        port_policy = "IB Gateway paper port 4002 is not allowed in this TWS-only preview phase."
    elif normalized_port != _EXPECTED_PORT:
        port_policy = "Unknown port; this preview harness only allows TWS paper port 7497."
    else:
        port_policy = "TWS paper port lock satisfied."
    return {
        "configured_mode": normalized_mode,
        "configured_host": normalized_host,
        "configured_port": normalized_port,
        "expected_mode": _EXPECTED_MODE,
        "expected_host": _EXPECTED_HOST,
        "expected_port": _EXPECTED_PORT,
        "passed": not mismatches,
        "fail_closed": bool(mismatches),
        "mismatches": mismatches,
        "port_policy": port_policy,
        "next_manual_check": (
            None
            if not mismatches
            else "Set mode=PAPER, host=127.0.0.1, and port=7497 before running preview."
        ),
    }


def evaluate_manual_preview_caller(
    *,
    caller_path: str,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> dict[str, Any]:
    normalized_caller = str(caller_path or "").strip()
    stack_modules = []
    for frame in stack_provider():
        module_name = str(getattr(getattr(frame, "frame", None), "f_globals", {}).get("__name__", "") or "").strip()
        if module_name:
            stack_modules.append(module_name)
    forbidden = [
        module_name
        for module_name in stack_modules
        if module_name.startswith(_FORBIDDEN_CALLER_PREFIXES)
        or any(fragment in module_name for fragment in _FORBIDDEN_CALLER_SUBSTRINGS)
    ]
    caller_allowed = normalized_caller == "manual_cli"
    passed = caller_allowed and not forbidden
    return {
        "caller_path": normalized_caller,
        "passed": passed,
        "fail_closed": not passed,
        "forbidden_callers_detected": forbidden,
        "detail": (
            "Preview harness is restricted to the dedicated manual CLI path."
            if passed
            else (
                "Preview harness rejected a non-manual caller path."
                if not caller_allowed
                else f"Preview harness detected forbidden caller frames: {', '.join(forbidden)}"
            )
        ),
    }


def evaluate_preview_input_guardrails(requested_order: dict[str, Any]) -> dict[str, dict[str, Any]]:
    symbol = str(requested_order.get("symbol") or "").strip().upper()
    expiry = str(requested_order.get("expiry") or "").strip()
    action = str(requested_order.get("action") or "").strip().upper()
    order_type = str(requested_order.get("order_type") or "").strip().upper()
    tif = str(requested_order.get("time_in_force") or "").strip().upper()
    quantity = float(requested_order.get("quantity") or 0.0)
    limit_price = requested_order.get("limit_price")
    return {
        "whitelisted_contract": {
            "passed": (symbol, expiry) in _ALLOWED_CONTRACTS,
            "detail": "Only GC 202606 and MGC 202606 are allowed in the initial preview harness.",
        },
        "supported_action": {
            "passed": action in _ALLOWED_ACTIONS,
            "detail": "Only BUY and SELL actions are allowed.",
        },
        "quantity_cap": {
            "passed": quantity > 0 and quantity <= 1.0,
            "detail": "Quantity must be greater than zero and less than or equal to one contract.",
        },
        "limit_only_order_type": {
            "passed": order_type in _ALLOWED_ORDER_TYPES,
            "detail": "Only LMT orders are allowed in preview.",
        },
        "limit_price_present": {
            "passed": limit_price is not None,
            "detail": "A hypothetical limit price is required for preview.",
        },
        "limit_price_positive": {
            "passed": limit_price is not None and float(limit_price) > 0.0,
            "detail": "The hypothetical limit price must be positive.",
        },
        "time_in_force_supported": {
            "passed": tif in _ALLOWED_TIFS,
            "detail": "Only DAY time-in-force is allowed in the initial preview harness.",
        },
    }


def build_preview_digest(preview_payload: dict[str, Any]) -> str:
    normalized = json.dumps(preview_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def classify_paper_order_preview(*, guardrail_checks: list[dict[str, Any]]) -> str:
    if any(check.get("blocking") and not check.get("passed") for check in guardrail_checks):
        return "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    if any((not check.get("blocking")) and not check.get("passed") for check in guardrail_checks):
        return "IBKR_PAPER_ORDER_PREVIEW_PARTIAL"
    return "IBKR_PAPER_ORDER_PREVIEW_READY"


def write_ibkr_paper_order_preview_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrPaperOrderPreviewArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_paper_order_preview_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_paper_order_preview_report.md").write_text(
        render_ibkr_paper_order_preview_markdown(artifacts.report),
        encoding="utf-8",
    )
    with (reports_dir / "ibkr_paper_order_preview_audit.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(artifacts.audit_entry, sort_keys=True))
        handle.write("\n")


def render_ibkr_paper_order_preview_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    caller = dict(report.get("manual_caller_check") or {})
    environment = dict(report.get("environment_lock_check") or {})
    order_payload = dict(report.get("hypothetical_order") or {})
    contract = dict(report.get("contract_preview") or {})
    quote = dict(report.get("quote_context") or {})
    guarantee = dict(report.get("no_submit_guarantee") or {})
    guardrail_checks = list(report.get("guardrail_checks") or [])
    lines = [
        "# IBKR Paper Order Preview Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- account_id: `{report.get('account_id')}`",
        f"- mode: `{environment.get('configured_mode')}`",
        f"- host: `{environment.get('configured_host')}`",
        f"- port: `{environment.get('configured_port')}`",
        f"- client_id: `{connection.get('client_id')}`",
        "",
        "## Preview",
        "",
        f"- contract: `{contract.get('symbol')} {contract.get('expiry')}`",
        f"- qualified_contract_identifier: `{contract.get('qualified_contract_identifier')}`",
        f"- action: `{order_payload.get('action')}`",
        f"- quantity: `{order_payload.get('quantity')}`",
        f"- order_type: `{order_payload.get('order_type')}`",
        f"- limit_price: `{order_payload.get('limit_price')}`",
        f"- time_in_force: `{order_payload.get('time_in_force')}`",
        f"- delayed_quote_label: `{quote.get('quote_source_label')}`",
        f"- live_market_data_available: `{quote.get('live_market_data_available')}`",
        f"- estimated_notional: `{report.get('estimated_notional')}`",
        f"- estimated_tick_value: `{report.get('estimated_tick_value')}`",
        f"- preview_digest: `{report.get('preview_digest')}`",
        "",
        "## Verification Summary",
        "",
        f"- classification: `{report.get('classification')}`",
        "- this is preview-only by construction",
        "- no submit path was implemented",
        "- no transmit path was implemented",
        "- no staging path was implemented",
        "- no submit-capable object is created",
        "- no paper order was placed",
        "- no strategy execution was connected",
        (
            "- manual CLI caller path only"
            if caller.get("caller_path") == "manual_cli"
            else f"- caller path observed: `{caller.get('caller_path')}`"
        ),
        (
            "- strategy-style callers fail closed"
            if not caller.get("forbidden_callers_detected")
            else f"- strategy-style callers fail closed: detected forbidden callers {', '.join(caller.get('forbidden_callers_detected') or [])}"
        ),
        f"- environment lock is {environment.get('configured_mode')} / {environment.get('configured_host')} / {environment.get('configured_port')} only",
        "- live port 7496 fails closed",
        "- IB Gateway ports 4001 and 4002 fail closed",
        "- unknown ports fail closed",
        "- whitelist is GC/MGC 202606 only",
        "- qty <= 1",
        "- LMT only",
        "- DAY only",
        "- limit price required",
        "- expected-account matching enforced",
        "- fresh open-order baseline required",
        "- deterministic preview digest generated",
        "- audit log written",
        "- delayed quote context captured and labeled",
        "",
        "## Guardrails",
        "",
    ]
    for check in guardrail_checks:
        lines.append(
            f"- {check.get('name')}: `{'PASS' if check.get('passed') else 'FAIL'}`"
            f" (blocking={check.get('blocking')}) - {check.get('detail')}"
        )
    lines.extend(
        [
            "",
            "## No Submit Guarantee",
            "",
            f"- submitted: `{guarantee.get('submitted')}`",
            f"- staged: `{guarantee.get('staged')}`",
            f"- transmitted: `{guarantee.get('transmitted')}`",
            f"- detail: {guarantee.get('detail')}",
            "",
        ]
    )
    warning = str(quote.get("live_market_data_warning") or "").strip()
    if warning:
        lines.extend(
            [
                "## Market Data Warning",
                "",
                f"- {warning}",
                "",
            ]
        )
    return "\n".join(lines)


def _normalize_requested_order(config: IbkrPaperOrderPreviewConfig) -> dict[str, Any]:
    return {
        "symbol": str(config.symbol or "").strip().upper(),
        "expiry": str(config.expiry or "").strip(),
        "action": str(config.action or "").strip().upper(),
        "quantity": float(config.quantity),
        "order_type": str(config.order_type or "").strip().upper(),
        "limit_price": None if config.limit_price is None else float(config.limit_price),
        "time_in_force": str(config.time_in_force or "").strip().upper(),
    }


def _preflight_guardrail_checks(
    *,
    manual_caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    input_guardrails: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    checks = [
        _guardrail_check(
            "manual_cli_only",
            passed=manual_caller_check["passed"],
            blocking=True,
            detail=manual_caller_check["detail"],
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
            "no_submit_path_exists_in_preview_harness",
            passed=True,
            blocking=True,
            detail="This harness constructs preview artifacts only and exits without any submit, stage, or transmit capability.",
        )
    )
    return checks


def _runtime_guardrail_checks(
    *,
    config: IbkrPaperOrderPreviewConfig,
    runtime_context: dict[str, Any],
) -> list[dict[str, Any]]:
    checks = [
        _guardrail_check(
            "managed_account_discovered",
            passed=bool(runtime_context.get("selected_account_id")),
            blocking=True,
            detail="The preview harness must discover and display the selected account id before continuing.",
        ),
        _guardrail_check(
            "expected_account_match",
            passed=(
                not config.account_id
                or str(config.account_id).strip() == str(runtime_context.get("selected_account_id") or "").strip()
            ),
            blocking=True,
            detail="The selected account must match the expected account id when one is provided.",
        ),
        _guardrail_check(
            "open_order_baseline_fresh",
            passed=bool((runtime_context.get("open_orders_snapshot") or {}).get("ok")),
            blocking=True,
            detail="The preview harness must read the current open-order baseline before producing the digest.",
        ),
        _guardrail_check(
            "contract_qualified",
            passed=bool((runtime_context.get("contract_report") or {}).get("ok")),
            blocking=True,
            detail="The requested contract must qualify successfully through TWS before preview is allowed.",
        ),
        _guardrail_check(
            "qualified_contract_identifier_available",
            passed=bool((runtime_context.get("contract_report") or {}).get("qualified_contract_identifier")),
            blocking=False,
            detail="A qualified contract identifier should be present when TWS returns one.",
        ),
        _guardrail_check(
            "delayed_quote_available_if_permissioned",
            passed=bool((runtime_context.get("quote_context") or {}).get("has_quote")),
            blocking=False,
            detail="The preview should include a delayed quote when the TWS paper session exposes one.",
        ),
        _guardrail_check(
            "delayed_data_warning_present",
            passed=bool((runtime_context.get("quote_context") or {}).get("delayed_data_warning_present")),
            blocking=False,
            detail="The preview must explain when live data is unavailable or the quote is delayed.",
        ),
    ]
    return checks


def _collect_preview_runtime_context(
    *,
    config: IbkrPaperOrderPreviewConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
    sleep_fn: Callable[[float], None],
    requested_order: dict[str, Any],
    started_at: datetime,
) -> dict[str, Any]:
    runtime = _build_runtime(config=config, transport_factory=transport_factory, module_loader=module_loader)
    try:
        runtime.transport.connect()
        _start_runtime(runtime=runtime)
        if not _wait_for_connection_ready(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error(codes=_CONNECTION_ERROR_CODES) or runtime.collector.latest_error()
            raise IbkrPaperOrderPreviewError(
                (
                    f"TWS paper preview handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"TWS paper preview handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        runtime.client.request_managed_accounts()
        runtime.transport.req_managed_accounts()
        if not _wait_for_event(
            runtime.collector.managed_accounts_ready,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error()
            raise IbkrPaperOrderPreviewError(
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
        runtime.client.request_open_orders()
        runtime.transport.req_all_open_orders()
        if not _wait_for_event(
            runtime.collector.open_orders_ready,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error()
            raise IbkrPaperOrderPreviewError(
                (
                    f"Open-order baseline refresh failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"Open-order baseline refresh did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        open_orders_snapshot = _build_open_orders_snapshot(
            client=runtime.client,
            selected_account_id=selected_account_id,
        )
        contract_report = _qualify_preview_contract(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            symbol=requested_order["symbol"],
            expiry=requested_order["expiry"],
            sleep_fn=sleep_fn,
        )
        if not contract_report["ok"]:
            raise IbkrPaperOrderPreviewError(str(contract_report["detail"]))
        quote_context = _probe_preview_quote_context(
            transport=runtime.transport,
            collector=runtime.collector,
            contract_report=contract_report,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        return {
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
            "selected_account_id": selected_account_id,
            "open_orders_snapshot": open_orders_snapshot,
            "contract_report": contract_report,
            "quote_context": quote_context,
            "errors": list(runtime.collector.errors),
        }
    finally:
        try:
            runtime.transport.disconnect()
        except Exception:
            pass


def _build_runtime(
    *,
    config: IbkrPaperOrderPreviewConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _PreviewRuntime:
    session = IbkrSession(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        gateway_mode="paper",
        read_only=True,
        order_id_policy=build_default_ibkr_order_id_policy(
            client_id=config.client_id,
            live_orders_enabled=False,
        ),
    )
    client = IbkrClient(session=session)
    collector = IbkrReadOnlyProbeCollector(client)
    transport = transport_factory(
        client=client,
        collector=collector,
        config=IbkrReadOnlyApiTransportConfig(
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            read_only=True,
        ),
        module_loader=module_loader,
    )
    return _PreviewRuntime(
        session=session,
        client=client,
        collector=collector,
        transport=transport,
    )


def _start_runtime(*, runtime: _PreviewRuntime) -> None:
    import threading

    thread = threading.Thread(target=runtime.transport.run_loop, name="ibkr_paper_order_preview_loop", daemon=True)
    thread.start()


def _qualify_preview_contract(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    timeout_seconds: float,
    symbol: str,
    expiry: str,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    resolver = IbkrContractResolver()
    qualified = resolver.qualify_futures(symbol=symbol, expiry=expiry)
    request_id = 6101
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
    return {
        "ok": ok,
        "symbol": symbol,
        "requested_expiry": expiry,
        "qualified_contract": qualified_dict,
        "qualified_contract_identifier": api_details.get("con_id") or qualified.local_symbol,
        "api_contract_details": contract_rows,
        "detail": (
            "Contract details received from TWS."
            if ok
            else (
                f"IBKR error {latest_error['code']}: {latest_error['message']}"
                if latest_error is not None
                else f"No contract details returned within {timeout_seconds:.1f}s."
            )
        ),
    }


def _probe_preview_quote_context(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
    contract_report: dict[str, Any],
    timeout_seconds: float,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    qualified = dict(contract_report.get("qualified_contract") or {})
    contract = IbkrQualifiedContract(
        internal_symbol=qualified["internal_symbol"],
        broker_symbol=qualified["broker_symbol"],
        local_symbol=qualified["local_symbol"],
        security_type=qualified["security_type"],
        exchange=qualified["exchange"],
        currency=qualified["currency"],
        expiry=qualified["expiry"],
        multiplier=qualified.get("multiplier"),
        trading_class=qualified.get("trading_class"),
        con_id=qualified.get("con_id"),
        metadata=qualified.get("metadata"),
    )
    live_probe = _request_market_data_snapshot(
        transport=transport,
        collector=collector,
        contract=contract,
        request_id=6201,
        market_data_type=1,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    delayed_probe = _request_market_data_snapshot(
        transport=transport,
        collector=collector,
        contract=contract,
        request_id=6202,
        market_data_type=3,
        timeout_seconds=timeout_seconds,
        sleep_fn=sleep_fn,
    )
    active_probe = delayed_probe if delayed_probe.get("any_tick_returned") else live_probe
    live_market_data_available = bool(
        live_probe.get("any_tick_returned") and live_probe.get("response_indication") == "data_returned"
    )
    delayed_warning = (
        "Live market data is unavailable in this paper session. The preview uses delayed data only."
        if not live_market_data_available
        else None
    )
    return {
        "live_probe": live_probe,
        "delayed_probe": delayed_probe,
        "has_quote": bool(active_probe.get("any_tick_returned")),
        "quote_source_label": (
            "DELAYED"
            if active_probe is delayed_probe and delayed_probe.get("any_tick_returned")
            else ("LIVE" if live_market_data_available else "UNAVAILABLE")
        ),
        "live_market_data_available": live_market_data_available,
        "live_market_data_warning": delayed_warning,
        "delayed_data_warning_present": bool(delayed_warning) or bool(delayed_probe.get("any_tick_returned")),
        "bid_price": active_probe.get("bid_price"),
        "ask_price": active_probe.get("ask_price"),
        "last_price": active_probe.get("last_price"),
        "close_price": active_probe.get("close_price"),
        "market_data_type_reported": active_probe.get("market_data_type_reported"),
        "detail": (
            "Delayed quote available for preview."
            if delayed_probe.get("any_tick_returned")
            else (
                "Live quote available for preview."
                if live_market_data_available
                else "No preview quote was available from TWS."
            )
        ),
    }


def _request_market_data_snapshot(
    *,
    transport: Any,
    collector: IbkrReadOnlyProbeCollector,
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
    errors = [
        error
        for error in collector.errors
        if error.get("request_id") == request_id
    ]
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


def _build_preview_payload(
    *,
    requested_order: dict[str, Any],
    runtime_context: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
) -> dict[str, Any]:
    contract_report = dict(runtime_context.get("contract_report") or {})
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    quote_context = dict(runtime_context.get("quote_context") or {})
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
        "account_id": runtime_context.get("selected_account_id"),
        "environment": {
            "mode": _EXPECTED_MODE,
            "host": _EXPECTED_HOST,
            "port": _EXPECTED_PORT,
            "client_id": runtime_context.get("connection_check", {}).get("client_id"),
        },
        "contract": {
            "symbol": requested_order["symbol"],
            "expiry": requested_order["expiry"],
            "exchange": contract_details.get("exchange") or contract_report.get("qualified_contract", {}).get("exchange"),
            "local_symbol": contract_details.get("local_symbol") or contract_report.get("qualified_contract", {}).get("local_symbol"),
            "multiplier": contract_details.get("multiplier") or contract_report.get("qualified_contract", {}).get("multiplier"),
            "trading_class": contract_details.get("trading_class") or contract_report.get("qualified_contract", {}).get("trading_class"),
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
            "bid_price": quote_context.get("bid_price"),
            "ask_price": quote_context.get("ask_price"),
            "last_price": quote_context.get("last_price"),
            "close_price": quote_context.get("close_price"),
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


def _build_preview_report(
    *,
    config: IbkrPaperOrderPreviewConfig,
    started_at: datetime,
    requested_order: dict[str, Any],
    manual_caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    runtime_context: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    preview_payload: dict[str, Any],
    preview_digest: str,
    classification: str,
) -> dict[str, Any]:
    contract_report = dict(runtime_context.get("contract_report") or {})
    contract_details = dict(contract_report.get("api_contract_details", [{}])[0] if contract_report.get("api_contract_details") else {})
    quote_context = dict(runtime_context.get("quote_context") or {})
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
        "account_id": runtime_context.get("selected_account_id"),
        "connection_check": dict(runtime_context.get("connection_check") or {}),
        "manual_caller_check": manual_caller_check,
        "environment_lock_check": {
            **environment_lock,
            "required_read_only_path": True,
        },
        "contract_preview": {
            "symbol": requested_order["symbol"],
            "expiry": requested_order["expiry"],
            "exchange": contract_details.get("exchange") or contract_report.get("qualified_contract", {}).get("exchange"),
            "local_symbol": contract_details.get("local_symbol") or contract_report.get("qualified_contract", {}).get("local_symbol"),
            "currency": contract_details.get("currency") or contract_report.get("qualified_contract", {}).get("currency"),
            "multiplier": contract_details.get("multiplier") or contract_report.get("qualified_contract", {}).get("multiplier"),
            "trading_class": contract_details.get("trading_class") or contract_report.get("qualified_contract", {}).get("trading_class"),
            "qualified_contract_identifier": contract_report.get("qualified_contract_identifier"),
            "qualified_contract": contract_report.get("qualified_contract"),
        },
        "hypothetical_order": {
            "action": requested_order["action"],
            "quantity": requested_order["quantity"],
            "order_type": requested_order["order_type"],
            "limit_price": requested_order["limit_price"],
            "time_in_force": requested_order["time_in_force"],
        },
        "quote_context": quote_context,
        "open_order_baseline": dict(runtime_context.get("open_orders_snapshot") or {}),
        "estimated_notional": estimated_notional,
        "estimated_tick_value": estimated_tick_value,
        "guardrail_checks": guardrail_checks,
        "preview_payload": preview_payload,
        "preview_digest": preview_digest,
        "no_submit_guarantee": {
            "submitted": False,
            "staged": False,
            "transmitted": False,
            "detail": "Preview only. No order was submitted, staged, or transmitted, and no submit-capable object was created.",
        },
        "errors": list(runtime_context.get("errors") or []),
        "next_manual_check": None if classification != "IBKR_PAPER_ORDER_PREVIEW_BLOCKED" else _tws_manual_check_message(config.host, config.port),
    }


def _build_audit_entry(*, report: dict[str, Any]) -> dict[str, Any]:
    order_payload = dict(report.get("hypothetical_order") or {})
    contract = dict(report.get("contract_preview") or {})
    return {
        "event_type": "preview_generated",
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "classification": report.get("classification"),
        "preview_digest": report.get("preview_digest"),
        "account_id": report.get("account_id"),
        "mode": (report.get("environment_lock_check") or {}).get("configured_mode"),
        "host": (report.get("environment_lock_check") or {}).get("configured_host"),
        "port": (report.get("environment_lock_check") or {}).get("configured_port"),
        "client_id": (report.get("connection_check") or {}).get("client_id"),
        "symbol": contract.get("symbol"),
        "expiry": contract.get("expiry"),
        "qualified_contract_identifier": contract.get("qualified_contract_identifier"),
        "action": order_payload.get("action"),
        "quantity": order_payload.get("quantity"),
        "order_type": order_payload.get("order_type"),
        "limit_price": order_payload.get("limit_price"),
        "time_in_force": order_payload.get("time_in_force"),
        "submitted": False,
        "staged": False,
        "transmitted": False,
    }


def _blocked_artifacts(
    *,
    config: IbkrPaperOrderPreviewConfig,
    started_at: datetime,
    requested_order: dict[str, Any],
    manual_caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    detail: str,
) -> IbkrPaperOrderPreviewArtifacts:
    generated_at = datetime.now(timezone.utc).isoformat()
    report = {
        "classification": "IBKR_PAPER_ORDER_PREVIEW_BLOCKED",
        "generated_at": generated_at,
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
        "manual_caller_check": manual_caller_check,
        "environment_lock_check": environment_lock,
        "contract_preview": {
            "symbol": requested_order.get("symbol"),
            "expiry": requested_order.get("expiry"),
            "qualified_contract_identifier": None,
        },
        "hypothetical_order": {
            "action": requested_order.get("action"),
            "quantity": requested_order.get("quantity"),
            "order_type": requested_order.get("order_type"),
            "limit_price": requested_order.get("limit_price"),
            "time_in_force": requested_order.get("time_in_force"),
        },
        "quote_context": {
            "has_quote": False,
            "quote_source_label": "UNAVAILABLE",
            "live_market_data_available": False,
            "live_market_data_warning": "Preview blocked before quote capture.",
            "delayed_data_warning_present": True,
        },
        "open_order_baseline": {
            "ok": False,
            "open_order_count": 0,
            "open_orders": [],
        },
        "estimated_notional": None,
        "estimated_tick_value": None,
        "guardrail_checks": guardrail_checks,
        "preview_payload": None,
        "preview_digest": None,
        "no_submit_guarantee": {
            "submitted": False,
            "staged": False,
            "transmitted": False,
            "detail": "Preview blocked before any broker-side action. No order was submitted, staged, or transmitted.",
        },
        "errors": [],
        "next_manual_check": environment_lock.get("next_manual_check") or _tws_manual_check_message(config.host, config.port),
    }
    audit_entry = _build_audit_entry(report=report)
    return IbkrPaperOrderPreviewArtifacts(
        classification="IBKR_PAPER_ORDER_PREVIEW_BLOCKED",
        report=report,
        audit_entry=audit_entry,
    )


def _guardrail_check(name: str, *, passed: bool, blocking: bool, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "passed": bool(passed),
        "blocking": bool(blocking),
        "detail": str(detail),
    }


def _first_failed_guardrail_detail(guardrail_checks: list[dict[str, Any]]) -> str:
    for check in guardrail_checks:
        if check.get("blocking") and not check.get("passed"):
            return str(check.get("detail") or f"Blocking guardrail failed: {check.get('name')}")
    return "Preview was blocked by a required guardrail."


def _first_tick_price(tick_prices: dict[str, Any], tick_ids: tuple[int, ...]) -> float | None:
    for tick_id in tick_ids:
        value = tick_prices.get(str(tick_id))
        if value is not None:
            return float(value)
    return None


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
