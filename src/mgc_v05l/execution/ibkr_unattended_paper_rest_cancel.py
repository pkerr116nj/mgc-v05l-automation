"""Unattended paper-only IBKR MGC rest/cancel test harness."""

from __future__ import annotations

import inspect
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..brokers.ibkr import IbkrClient, IbkrSession, build_default_ibkr_order_id_policy
from ..execution_core.track_b_paper_autonomous_recovery_planner import PLAN_TARGETED_CANCEL_REPLACE
from ..execution_core.track_b_pre_action_snapshot_validator import (
    PRE_ACTION_SNAPSHOT_VALID,
    TrackBPreActionSnapshotValidatorConfig,
    validate_track_b_pre_action_snapshot,
)
from .ibkr_manual_paper_submit import (
    _SEVERE_CONNECTION_ERROR_CODES,
    IbkrManualPaperSubmitCollector,
    IbkrManualPaperSubmitError,
    IbkrManualPaperSubmitTransport,
    _build_callback_timeline,
    _build_delayed_quote_pricing_context,
    _cancel_and_verify_visible_order,
    _contract_min_tick,
    _detect_order_rejection,
    _exact_contract_position_quantity,
    _extract_completed_order_rows,
    _extract_execution_rows,
    _find_open_order,
    _not_run_snapshot,
    _probe_delayed_quote_context,
    _qualify_mgc_contract,
    _record_audit,
    _refresh_execution_truth,
    _refresh_open_orders_snapshot,
    _refresh_positions_snapshot,
    _round_price_to_tick,
    _select_delayed_reference_price,
    _snapshot_digest,
    _wait_for_submitted_order_visibility,
)
from .ibkr_paper_order_preview import (
    _FORBIDDEN_CALLER_PREFIXES,
    _FORBIDDEN_CALLER_SUBSTRINGS,
    _coerce_float,
    _guardrail_check,
    build_preview_digest,
    evaluate_paper_preview_environment_lock,
)
from .ibkr_position_reconciliation import (
    _collect_account_truth,
    _collect_exact_contract_context,
    _collect_managed_account_context,
)
from .ibkr_read_only_verifier import (
    _build_open_orders_snapshot,
    _build_positions_snapshot,
    _wait_for_connection_ready,
    _wait_for_event,
    IbkrReadOnlyApiTransportConfig,
)

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_EXPECTED_ACCOUNT_ID = "DUM882026"
_EXPECTED_SYMBOL = "MGC"
_EXPECTED_CONTRACT_MONTH = "202606"
_EXPECTED_EXACT_EXPIRY = "20260626"
_EXPECTED_CON_ID = 712565978
_EXPECTED_LOCAL_SYMBOL = "MGCM6"
_EXPECTED_EXCHANGE = "COMEX"
_EXPECTED_CURRENCY = "USD"
_EXPECTED_MULTIPLIER = "10"
_EXPECTED_ACTION = "BUY"
_EXPECTED_QUANTITY = 1.0
_EXPECTED_ORDER_TYPE = "LMT"
_EXPECTED_TIF = "DAY"
_DEFAULT_TIMEOUT_SECONDS = 15.0
_DEFAULT_OBSERVATION_SECONDS = 8.0
_DEFAULT_LIMIT_OFFSET_TICKS = 1.0
_DEFAULT_DELAYED_QUOTE_MAX_AGE_SECONDS = 30.0
_DEFAULT_TWS_VISIBILITY_PAUSE_SECONDS = 12.0
_ARTIFACT_STEM = "ibkr_unattended_paper_rest_cancel"


class IbkrUnattendedPaperRestCancelError(RuntimeError):
    """Base error for the unattended paper rest/cancel harness."""


@dataclass(frozen=True)
class IbkrUnattendedPaperRestCancelConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    unattended_paper: bool
    account_id: str | None = _EXPECTED_ACCOUNT_ID
    symbol: str = _EXPECTED_SYMBOL
    contract_month: str = _EXPECTED_CONTRACT_MONTH
    action: str = _EXPECTED_ACTION
    quantity: float = _EXPECTED_QUANTITY
    order_type: str = _EXPECTED_ORDER_TYPE
    time_in_force: str = _EXPECTED_TIF
    exact_expiry: str = _EXPECTED_EXACT_EXPIRY
    con_id: int = _EXPECTED_CON_ID
    local_symbol: str = _EXPECTED_LOCAL_SYMBOL
    exchange: str = _EXPECTED_EXCHANGE
    currency: str = _EXPECTED_CURRENCY
    multiplier: str = _EXPECTED_MULTIPLIER
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    observation_seconds: float = _DEFAULT_OBSERVATION_SECONDS
    limit_offset_ticks: float = _DEFAULT_LIMIT_OFFSET_TICKS
    delayed_quote_max_age_seconds: float = _DEFAULT_DELAYED_QUOTE_MAX_AGE_SECONDS
    tws_visibility_pause_seconds: float = _DEFAULT_TWS_VISIBILITY_PAUSE_SECONDS
    visible_in_tws: bool | None = None
    canceled_in_tws: bool | None = None
    caller_path: str = "unattended_paper_cli"
    emergency_legacy_rest_cancel: bool = False
    pre_action_snapshot_max_age_seconds: int = 300
    expected_broker_order_id: str | None = None
    expected_perm_id: str | None = None


@dataclass(frozen=True)
class IbkrUnattendedPaperRestCancelArtifacts:
    classification: str
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]
    open_order_before: dict[str, Any]
    open_order_after_submit: dict[str, Any]
    open_order_after_cancel: dict[str, Any]
    callback_timeline: list[dict[str, Any]]
    extra_artifacts: dict[str, Any] | None = None

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN" else 1


@dataclass
class _Runtime:
    session: IbkrSession
    client: IbkrClient
    collector: IbkrManualPaperSubmitCollector
    transport: IbkrManualPaperSubmitTransport


def run_ibkr_unattended_paper_rest_cancel(
    *,
    config: IbkrUnattendedPaperRestCancelConfig,
    transport_factory: Callable[..., Any] = IbkrManualPaperSubmitTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> IbkrUnattendedPaperRestCancelArtifacts:
    started_at = datetime.now(timezone.utc)
    audit_events: list[dict[str, Any]] = []
    environment_lock = evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port)
    caller_check = evaluate_unattended_paper_caller(caller_path=config.caller_path, stack_provider=stack_provider)
    runtime: _Runtime | None = None
    requested_order = {
        "symbol": config.symbol,
        "expiry": config.contract_month,
        "action": config.action,
        "quantity": float(config.quantity),
        "order_type": config.order_type,
        "time_in_force": config.time_in_force,
        "limit_price": None,
    }
    input_guardrails = _input_guardrails(config)
    guardrail_checks = _preflight_guardrail_checks(
        caller_check=caller_check,
        environment_lock=environment_lock,
        unattended_paper=config.unattended_paper,
        input_guardrails=input_guardrails,
        emergency_legacy_rest_cancel=config.emergency_legacy_rest_cancel,
    )
    if any(check.get("blocking") and not check.get("passed") for check in guardrail_checks):
        detail = _first_failed_guardrail_detail(guardrail_checks)
        _record_audit(
            audit_events,
            event_type="failed_closed",
            config=_audit_config(config),
            classification="IBKR_UNATTENDED_REST_CANCEL_UNKNOWN",
            detail=detail,
        )
        report = _blocked_report(
            config=config,
            started_at=started_at,
            caller_check=caller_check,
            environment_lock=environment_lock,
            requested_order=requested_order,
            guardrail_checks=guardrail_checks,
            audit_events=audit_events,
            detail=detail,
        )
        return IbkrUnattendedPaperRestCancelArtifacts(
            classification="IBKR_UNATTENDED_REST_CANCEL_UNKNOWN",
            report=report,
            audit_events=audit_events,
            open_order_before=_not_run_snapshot("Open-order baseline was not captured."),
            open_order_after_submit=_not_run_snapshot("Submit did not run."),
            open_order_after_cancel=_not_run_snapshot("Cancel did not run."),
            callback_timeline=[],
        )
    pre_action_validation = _pre_action_snapshot_validation(config=config, now=started_at)
    if pre_action_validation.get("classification") != PRE_ACTION_SNAPSHOT_VALID:
        detail = (
            "Pre-action Control Plane Snapshot validation blocked emergency legacy REST cancel path: "
            f"{pre_action_validation.get('classification')} - {pre_action_validation.get('reason')}"
        )
        _record_audit(
            audit_events,
            event_type="failed_closed",
            config=_audit_config(config),
            classification="IBKR_UNATTENDED_REST_CANCEL_UNKNOWN",
            detail=detail,
            extra={"pre_action_snapshot_validation": pre_action_validation},
        )
        report = _blocked_report(
            config=config,
            started_at=started_at,
            caller_check=caller_check,
            environment_lock=environment_lock,
            requested_order=requested_order,
            guardrail_checks=guardrail_checks,
            audit_events=audit_events,
            detail=detail,
            pre_action_snapshot_validation=pre_action_validation,
        )
        return IbkrUnattendedPaperRestCancelArtifacts(
            classification="IBKR_UNATTENDED_REST_CANCEL_UNKNOWN",
            report=report,
            audit_events=audit_events,
            open_order_before=_not_run_snapshot("Open-order baseline was not captured."),
            open_order_after_submit=_not_run_snapshot("Submit did not run."),
            open_order_after_cancel=_not_run_snapshot("Cancel did not run."),
            callback_timeline=[],
        )
    try:
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
            raise IbkrUnattendedPaperRestCancelError(
                (
                    f"IBKR unattended rest/cancel handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"IBKR unattended rest/cancel handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        selected_account_id = _collect_managed_account_context(
            config=_position_config_from_unattended(config),
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        exact_contract, exact_contract_report = _collect_exact_contract_context(
            config=_position_config_from_unattended(config),
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        account_truth, _provider_snapshot = _collect_account_truth(
            config=_position_config_from_unattended(config),
            runtime=runtime,
            selected_account_id=selected_account_id,
            sleep_fn=sleep_fn,
        )
        positions_before = _refresh_positions_snapshot(
            runtime=runtime,
            config=_manual_like_config(config),
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        open_order_before = _refresh_open_orders_snapshot(
            runtime=runtime,
            config=_manual_like_config(config),
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        if int(open_order_before.get("open_order_count") or 0) > 0:
            matching = [
                row for row in list(open_order_before.get("open_orders") or [])
                if int(row.get("con_id") or 0) == int(config.con_id)
            ]
            if matching:
                raise IbkrUnattendedPaperRestCancelError(
                    "Unattended paper rest/cancel requires a clean baseline with no working MGC order."
                )
            raise IbkrUnattendedPaperRestCancelError(
                f"Unattended paper rest/cancel requires a clean baseline, but {open_order_before.get('open_order_count')} open orders are present."
            )
        contract_report = _qualify_mgc_contract(
            transport=runtime.transport,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        if not contract_report.get("ok"):
            raise IbkrUnattendedPaperRestCancelError(str(contract_report.get("detail") or "MGC contract qualification failed."))
        quote_context = _probe_delayed_quote_context(
            transport=runtime.transport,
            collector=runtime.collector,
            contract=contract_report["qualified_contract_object"],
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        limit_price = _derive_non_marketable_buy_limit(
            quote_context=quote_context,
            contract_report=contract_report,
            delayed_quote_max_age_seconds=config.delayed_quote_max_age_seconds,
            offset_ticks=config.limit_offset_ticks,
        )
        requested_order["limit_price"] = limit_price
        pricing_context = _build_delayed_quote_pricing_context(
            config=_manual_like_config(config),
            requested_order=requested_order,
            context={"quote_context": quote_context, "contract_report": contract_report},
        )
        _validate_pricing_context(pricing_context=pricing_context, config=config)
        preview_payload = {
            "account_id": selected_account_id,
            "environment": {
                "mode": config.mode,
                "host": config.host,
                "port": config.port,
                "client_id": config.client_id,
            },
            "contract": {
                "display_label": f"{config.symbol} {config.contract_month}",
                "exact_expiry": exact_contract.expiry,
                "con_id": exact_contract.con_id,
                "local_symbol": exact_contract.local_symbol,
            },
            "order": {
                "action": requested_order["action"],
                "quantity": requested_order["quantity"],
                "order_type": requested_order["order_type"],
                "limit_price": requested_order["limit_price"],
                "time_in_force": requested_order["time_in_force"],
                "pricing_label": pricing_context.get("pricing_label"),
                "intended_to_fill": False,
                "submitted": False,
            },
            "quote_context": pricing_context,
            "open_order_baseline_digest": _snapshot_digest(open_order_before),
        }
        preview_digest = build_preview_digest(preview_payload)
        _record_audit(
            audit_events,
            event_type="unattended_preflight_ready",
            config=_audit_config(config),
            classification=None,
            detail="Read-only preflight, exact contract qualification, and delayed quote context were captured for unattended rest/cancel.",
            extra={
                "account_id": selected_account_id,
                "open_order_count": open_order_before.get("open_order_count"),
                "preview_digest": preview_digest,
                "quote_snapshot": pricing_context.get("quote_snapshot"),
                "limit_price": requested_order["limit_price"],
                "distance_from_quote": pricing_context.get("distance_from_reference_price"),
                "distance_ticks": pricing_context.get("distance_ticks"),
            },
        )
        lifecycle_result = _execute_unattended_rest_cancel_lifecycle(
            config=config,
            runtime=runtime,
            selected_account_id=selected_account_id,
            requested_order=requested_order,
            audit_events=audit_events,
            open_order_baseline_digest=_snapshot_digest(open_order_before),
            quote_context=quote_context,
            pricing_context=pricing_context,
            contract_report=contract_report,
            sleep_fn=sleep_fn,
        )
        classification = _classify_lifecycle(lifecycle_result)
        report = _build_report(
            config=config,
            started_at=started_at,
            classification=classification,
            caller_check=caller_check,
            environment_lock=environment_lock,
            account_truth=account_truth,
            positions_before=positions_before,
            open_order_before=open_order_before,
            quote_context=quote_context,
            pricing_context=pricing_context,
            preview_digest=preview_digest,
            requested_order=requested_order,
            exact_contract_report=exact_contract_report,
            lifecycle_result=lifecycle_result,
            guardrail_checks=guardrail_checks,
            audit_events=audit_events,
            errors=list(runtime.collector.errors),
            callback_timeline_event_count=len(_build_callback_timeline(runtime)),
            pre_action_snapshot_validation=pre_action_validation,
        )
        return IbkrUnattendedPaperRestCancelArtifacts(
            classification=classification,
            report=report,
            audit_events=audit_events,
            open_order_before=open_order_before,
            open_order_after_submit=lifecycle_result.get("open_order_after_submit") or _not_run_snapshot("Submit snapshot unavailable."),
            open_order_after_cancel=lifecycle_result.get("open_order_after_cancel") or _not_run_snapshot("Cancel snapshot unavailable."),
            callback_timeline=_build_callback_timeline(runtime),
            extra_artifacts=_extra_artifacts_from_lifecycle(lifecycle_result),
        )
    except Exception as exc:
        classification = "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"
        _record_audit(
            audit_events,
            event_type="failed_closed",
            config=_audit_config(config),
            classification=classification,
            detail=str(exc),
        )
        report = _blocked_report(
            config=config,
            started_at=started_at,
            caller_check=caller_check,
            environment_lock=environment_lock,
            requested_order=requested_order,
            guardrail_checks=guardrail_checks,
            audit_events=audit_events,
            detail=str(exc),
            pre_action_snapshot_validation=pre_action_validation,
        )
        if runtime is not None:
            report["errors"] = list(runtime.collector.errors)
            callback_timeline = _build_callback_timeline(runtime)
        else:
            callback_timeline = []
            report["errors"] = []
        return IbkrUnattendedPaperRestCancelArtifacts(
            classification=classification,
            report=report,
            audit_events=audit_events,
            open_order_before=_not_run_snapshot("Open-order baseline was not captured."),
            open_order_after_submit=_not_run_snapshot("Submit did not run."),
            open_order_after_cancel=_not_run_snapshot("Cancel did not run."),
            callback_timeline=callback_timeline,
        )
    finally:
        if runtime is not None:
            try:
                runtime.transport.req_account_updates(subscribe=False, account_id=runtime.session.state.account_id or config.account_id or "")
            except Exception:
                pass
            try:
                runtime.transport.disconnect()
            except Exception:
                pass


def write_ibkr_unattended_paper_rest_cancel_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrUnattendedPaperRestCancelArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / f"{_ARTIFACT_STEM}_report.json").write_text(json.dumps(artifacts.report, indent=2, sort_keys=True), encoding="utf-8")
    (reports_dir / f"{_ARTIFACT_STEM}_report.md").write_text(render_ibkr_unattended_paper_rest_cancel_markdown(artifacts.report), encoding="utf-8")
    (reports_dir / f"{_ARTIFACT_STEM}_open_order_before.json").write_text(json.dumps(artifacts.open_order_before, indent=2, sort_keys=True), encoding="utf-8")
    (reports_dir / f"{_ARTIFACT_STEM}_open_order_after_submit.json").write_text(json.dumps(artifacts.open_order_after_submit, indent=2, sort_keys=True), encoding="utf-8")
    (reports_dir / f"{_ARTIFACT_STEM}_open_order_after_cancel.json").write_text(json.dumps(artifacts.open_order_after_cancel, indent=2, sort_keys=True), encoding="utf-8")
    for name, payload in dict(artifacts.extra_artifacts or {}).items():
        (reports_dir / f"{_ARTIFACT_STEM}_{name}.json").write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    with (reports_dir / f"{_ARTIFACT_STEM}_callback_timeline.jsonl").open("w", encoding="utf-8") as handle:
        for row in artifacts.callback_timeline:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    with (reports_dir / f"{_ARTIFACT_STEM}_audit.jsonl").open("a", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def render_ibkr_unattended_paper_rest_cancel_markdown(report: dict[str, Any]) -> str:
    lifecycle = dict(report.get("lifecycle") or {})
    acceptance = dict(report.get("working_order_acceptance") or {})
    exact_contract = dict(report.get("exact_contract") or {})
    quote = dict(report.get("quote_context") or {})
    pricing = dict(report.get("pricing_context") or {})
    lines = [
        "# IBKR Unattended Paper Rest/Cancel Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- environment: `{report.get('mode')} / {report.get('host')} / {report.get('port')}`",
        f"- account: `{report.get('account_id')}`",
        f"- client id: `{report.get('client_id')}`",
        f"- exact contract: `MGC {exact_contract.get('expiry')}` / `conId={exact_contract.get('con_id')}` / `localSymbol={exact_contract.get('local_symbol')}`",
        f"- order: `BUY 1 LMT DAY @ {report.get('limit_price')}`",
        "",
        "## Summary",
        "",
        f"- lifecycle status: `{lifecycle.get('status')}`",
        f"- detail: {lifecycle.get('detail')}",
        f"- delayed quote label: `{quote.get('quote_source_label')}`",
        f"- quote snapshot: `{pricing.get('quote_snapshot')}`",
        f"- distance from quote: `{pricing.get('distance_from_reference_price')}`",
        f"- distance in ticks: `{pricing.get('distance_ticks')}`",
        f"- open-order baseline count: `{dict(report.get('open_order_before') or {}).get('open_order_count')}`",
        "",
        "## Outcome",
        "",
        f"- submitted order id: `{lifecycle.get('submitted_order_id')}`",
        f"- submitted perm id: `{lifecycle.get('submitted_perm_id')}`",
        f"- final broker status: `{dict(lifecycle.get('latest_order_status') or {}).get('status')}`",
        f"- callback timeline events: `{report.get('callback_timeline_event_count')}`",
        "",
        "## Working Order Acceptance",
        "",
        f"- acceptance classification: `{acceptance.get('classification')}`",
        f"- api working-order truth observed: `{acceptance.get('api_working_order_visible')}`",
        f"- operator visible in tws: `{acceptance.get('visible_in_tws')}`",
        f"- operator cancel/canceled visible in tws: `{acceptance.get('canceled_in_tws')}`",
        f"- tws visibility pause seconds: `{acceptance.get('tws_visibility_pause_seconds')}`",
        "",
        "## Safety",
        "",
        "- no strategy linkage",
        "- no scheduler",
        "- no ATP/GC execution",
        "- one order only",
        "- no retry",
        "- exact qualified contract only",
    ]
    return "\n".join(lines)


def evaluate_unattended_paper_caller(
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
    caller_allowed = normalized_caller == "unattended_paper_cli"
    passed = caller_allowed and not forbidden
    return {
        "caller_path": normalized_caller,
        "passed": passed,
        "fail_closed": not passed,
        "forbidden_callers_detected": forbidden,
        "detail": (
            "Unattended paper harness is restricted to the dedicated unattended paper CLI path."
            if passed
            else (
                "Unattended paper harness rejected a non-unattended caller path."
                if not caller_allowed
                else f"Unattended paper harness detected forbidden caller frames: {', '.join(forbidden)}"
            )
        ),
    }


def _build_runtime(
    *,
    config: IbkrUnattendedPaperRestCancelConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _Runtime:
    session = IbkrSession(
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        gateway_mode="paper",
        read_only=False,
        order_id_policy=build_default_ibkr_order_id_policy(client_id=config.client_id, live_orders_enabled=False),
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
    return _Runtime(session=session, client=client, collector=collector, transport=transport)


def _start_runtime(runtime: _Runtime) -> None:
    thread = threading.Thread(target=runtime.transport.run_loop, name="ibkr_unattended_paper_rest_cancel_loop", daemon=True)
    thread.start()


def _input_guardrails(config: IbkrUnattendedPaperRestCancelConfig) -> dict[str, dict[str, Any]]:
    return {
        "expected_account_id": {
            "passed": str(config.account_id or "").strip() == _EXPECTED_ACCOUNT_ID,
            "detail": f"Only account {_EXPECTED_ACCOUNT_ID} is allowed for the unattended paper rest/cancel test.",
        },
        "whitelisted_contract": {
            "passed": (
                config.symbol == _EXPECTED_SYMBOL
                and config.contract_month == _EXPECTED_CONTRACT_MONTH
                and config.exact_expiry == _EXPECTED_EXACT_EXPIRY
                and int(config.con_id) == _EXPECTED_CON_ID
                and config.local_symbol == _EXPECTED_LOCAL_SYMBOL
            ),
            "detail": "Only exact MGC 20260626 / conId=712565978 / localSymbol=MGCM6 is allowed.",
        },
        "expected_action": {
            "passed": config.action == _EXPECTED_ACTION,
            "detail": "This first unattended mutation test only allows BUY.",
        },
        "expected_quantity": {
            "passed": float(config.quantity) == _EXPECTED_QUANTITY,
            "detail": "Quantity must equal exactly one contract.",
        },
        "expected_order_type": {
            "passed": config.order_type == _EXPECTED_ORDER_TYPE,
            "detail": "Only LMT orders are allowed.",
        },
        "expected_tif": {
            "passed": config.time_in_force == _EXPECTED_TIF,
            "detail": "Only DAY is allowed.",
        },
    }


def _preflight_guardrail_checks(
    *,
    caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    unattended_paper: bool,
    input_guardrails: dict[str, dict[str, Any]],
    emergency_legacy_rest_cancel: bool,
) -> list[dict[str, Any]]:
    checks = [
        _guardrail_check("dedicated_unattended_cli_only", passed=caller_check["passed"], blocking=True, detail=caller_check["detail"]),
        _guardrail_check("paper_tws_environment_lock", passed=environment_lock["passed"], blocking=True, detail=environment_lock["port_policy"]),
        _guardrail_check(
            "explicit_unattended_paper_flag_required",
            passed=bool(unattended_paper),
            blocking=True,
            detail="The unattended paper rest/cancel harness requires the explicit --unattended-paper flag.",
        ),
        _guardrail_check(
            "explicit_emergency_legacy_rest_cancel_required",
            passed=bool(emergency_legacy_rest_cancel),
            blocking=True,
            detail=(
                "The lower-level REST cancel harness is deprecated/emergency-only. "
                "Use track_b_managed_exit_cancel_replace for managed order cancel/replace."
            ),
        ),
    ]
    for name, payload in input_guardrails.items():
        checks.append(_guardrail_check(name, passed=bool(payload["passed"]), blocking=True, detail=str(payload["detail"])))
    return checks


def _derive_non_marketable_buy_limit(
    *,
    quote_context: dict[str, Any],
    contract_report: dict[str, Any],
    delayed_quote_max_age_seconds: float,
    offset_ticks: float,
) -> float:
    if str(quote_context.get("quote_source_label") or "").strip().upper() not in {"DELAYED", "DELAYED_FROZEN"}:
        raise IbkrUnattendedPaperRestCancelError("Unattended paper rest/cancel requires delayed or delayed-frozen quote context. Fail closed if delayed-style quote context is unavailable.")
    updated_at = _parse_iso_timestamp(quote_context.get("updated_at"))
    if updated_at is None:
        raise IbkrUnattendedPaperRestCancelError("Unattended paper rest/cancel requires a timestamped delayed quote.")
    quote_age_seconds = max(0.0, (datetime.now(timezone.utc) - updated_at).total_seconds())
    if quote_age_seconds > float(delayed_quote_max_age_seconds):
        raise IbkrUnattendedPaperRestCancelError("Unattended paper rest/cancel requires a fresh delayed quote. The current delayed quote is stale.")
    reference_price, _reference_source = _select_delayed_reference_price(quote_context)
    if reference_price is None:
        raise IbkrUnattendedPaperRestCancelError("Unattended paper rest/cancel requires a delayed bid/last reference price.")
    min_tick = _contract_min_tick(contract_report)
    if min_tick is None or min_tick <= 0.0:
        raise IbkrUnattendedPaperRestCancelError("The unattended paper rest/cancel harness requires a valid contract minTick.")
    limit_price = float(reference_price) - max(1.0, float(offset_ticks)) * float(min_tick)
    return _round_price_to_tick(limit_price, min_tick)


def _validate_pricing_context(
    *,
    pricing_context: dict[str, Any],
    config: IbkrUnattendedPaperRestCancelConfig,
) -> None:
    quote_snapshot = dict(pricing_context.get("quote_snapshot") or {})
    if str(quote_snapshot.get("source_label") or "").strip().upper() not in {"DELAYED", "DELAYED_FROZEN"}:
        raise IbkrUnattendedPaperRestCancelError("Unattended paper rest/cancel requires delayed or delayed-frozen quote context.")
    if pricing_context.get("reference_price") is None:
        raise IbkrUnattendedPaperRestCancelError("Unattended paper rest/cancel could not derive a delayed reference price.")
    if pricing_context.get("distance_from_reference_price") is None or float(pricing_context.get("distance_from_reference_price") or 0.0) <= 0.0:
        raise IbkrUnattendedPaperRestCancelError("The unattended paper rest/cancel limit must sit below the delayed reference price.")
    distance_ticks = _coerce_float(pricing_context.get("distance_ticks"))
    if distance_ticks is not None and distance_ticks < 1.0:
        raise IbkrUnattendedPaperRestCancelError("The unattended paper rest/cancel limit must be at least one tick below the delayed bid.")
    quote_age = _coerce_float(quote_snapshot.get("quote_age_seconds"))
    if quote_age is None or quote_age > float(config.delayed_quote_max_age_seconds):
        raise IbkrUnattendedPaperRestCancelError("The unattended paper rest/cancel limit requires a fresh delayed quote snapshot.")


def _execute_unattended_rest_cancel_lifecycle(
    *,
    config: IbkrUnattendedPaperRestCancelConfig,
    runtime: _Runtime,
    selected_account_id: str,
    requested_order: dict[str, Any],
    audit_events: list[dict[str, Any]],
    open_order_baseline_digest: str,
    quote_context: dict[str, Any],
    pricing_context: dict[str, Any],
    contract_report: dict[str, Any],
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    refreshed_before_submit = _refresh_open_orders_snapshot(
        runtime=runtime,
        config=_manual_like_config(config),
        selected_account_id=selected_account_id,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if _snapshot_digest(refreshed_before_submit) != open_order_baseline_digest:
        return {
            "status": "baseline_changed",
            "detail": "Open-order baseline changed before unattended submit, so the harness failed closed.",
            "open_order_after_submit": _not_run_snapshot("Submit was blocked because the open-order baseline changed."),
            "open_order_after_cancel": _not_run_snapshot("Cancel did not run."),
        }
    order_id = runtime.session.allocate_order_id()
    runtime.collector.reset_order_status_event(order_id)
    runtime.transport.place_limit_order(
        order_id=order_id,
        account_id=selected_account_id,
        contract=contract_report["qualified_contract_object"],
        action=requested_order["action"],
        quantity=requested_order["quantity"],
        limit_price=requested_order["limit_price"],
        time_in_force=requested_order["time_in_force"],
    )
    _record_audit(
        audit_events,
        event_type="submit_attempted",
        config=_audit_config(config),
        classification=None,
        detail="Submitted one unattended paper MGC limit order.",
        extra={
            "order_id": order_id,
            "quote_snapshot": pricing_context.get("quote_snapshot"),
            "limit_price": requested_order.get("limit_price"),
            "distance_from_quote": pricing_context.get("distance_from_reference_price"),
            "distance_ticks": pricing_context.get("distance_ticks"),
        },
    )
    after_submit = _wait_for_submitted_order_visibility(
        runtime=runtime,
        config=_manual_like_config(config),
        selected_account_id=selected_account_id,
        order_id=order_id,
        timeout_seconds=config.observation_seconds,
        sleep_fn=sleep_fn,
    )
    submitted_row = _find_open_order(after_submit, order_id)
    latest_status = runtime.collector.latest_order_status(order_id)
    perm_id = submitted_row.get("perm_id") if submitted_row else (latest_status.get("perm_id") if latest_status else None)
    if submitted_row is None:
        truth = _refresh_execution_truth(
            runtime=runtime,
            config=_manual_like_config(config),
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        matching_executions = _matching_rows_for_order(truth["executions"], order_id=order_id, perm_id=perm_id)
        matching_completed = _matching_rows_for_order(truth["completed_orders"], order_id=order_id, perm_id=perm_id)
        rejection = _detect_order_rejection(collector=runtime.collector, order_id=order_id)
        if rejection is not None:
            _record_audit(
                audit_events,
                event_type="submit_rejected",
                config=_audit_config(config),
                classification=None,
                detail=str(rejection.get("detail") or "IBKR rejected the unattended paper order."),
                extra={"order_id": order_id, "perm_id": perm_id, "error_code": rejection.get("error_code")},
            )
            return {
                "status": "rejected",
                "detail": rejection.get("detail"),
                "submitted_order_id": order_id,
                "submitted_perm_id": perm_id,
                "latest_order_status": rejection.get("latest_order_status"),
                "rejection": rejection,
                "open_order_after_submit": after_submit,
                "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because the order was rejected."),
                "positions_after_submit": truth["positions"],
                "executions_after_submit": matching_executions,
                "completed_orders_after_submit": matching_completed,
            }
        if _has_any_fill(latest_status=latest_status, execution_rows=matching_executions):
            positions_after_fill = truth["positions"]
            _record_audit(
                audit_events,
                event_type="filled_unexpectedly",
                config=_audit_config(config),
                classification=None,
                detail="The unattended paper rest/cancel test filled unexpectedly before an open order could be cancelled.",
                extra={"order_id": order_id, "perm_id": perm_id},
            )
            return {
                "status": "filled_unexpectedly",
                "detail": "The unattended paper rest/cancel test filled unexpectedly.",
                "submitted_order_id": order_id,
                "submitted_perm_id": perm_id,
                "latest_order_status": latest_status,
                "open_order_after_submit": after_submit,
                "open_order_after_cancel": _not_run_snapshot("Cancel did not run because the order already filled."),
                "positions_after_submit": positions_after_fill,
                "executions_after_submit": matching_executions,
                "completed_orders_after_submit": matching_completed,
            }
        _record_audit(
            audit_events,
            event_type="tws_dialog_or_hidden_block_suspected",
            config=_audit_config(config),
            classification=None,
            detail="No broker-visible order truth appeared after unattended submit. TWS precaution dialog blocking is suspected.",
            extra={"order_id": order_id, "perm_id": perm_id},
        )
        return {
            "status": "dialog_blocked",
            "detail": "No openOrder/orderStatus truth appeared after unattended submit, so a hidden TWS dialog or precaution block is suspected.",
            "submitted_order_id": order_id,
            "submitted_perm_id": perm_id,
            "latest_order_status": latest_status,
            "open_order_after_submit": after_submit,
            "open_order_after_cancel": _not_run_snapshot("Cancel was not attempted because no working order became visible."),
            "positions_after_submit": truth["positions"],
            "executions_after_submit": matching_executions,
            "completed_orders_after_submit": matching_completed,
        }
    sleep_fn(max(0.0, float(config.tws_visibility_pause_seconds)))
    _record_audit(
        audit_events,
        event_type="open_order_verified_after_submit",
        config=_audit_config(config),
        classification=None,
        detail="Broker open-order truth confirmed the unattended paper order.",
        extra={
            "order_id": order_id,
            "perm_id": perm_id,
            "status": submitted_row.get("status"),
            "tws_visibility_pause_seconds": float(config.tws_visibility_pause_seconds),
            "visible_in_tws": config.visible_in_tws,
        },
    )
    cancel_result = _cancel_and_verify_visible_order(
        config=_manual_like_config(config),
        runtime=runtime,
        selected_account_id=selected_account_id,
        sleep_fn=sleep_fn,
        audit_events=audit_events,
        order_id=order_id,
        perm_id=perm_id,
        after_submit=after_submit,
    )
    final_open_orders = cancel_result["open_order_after_cancel"]
    remaining_mgc = [
        row
        for row in list(final_open_orders.get("open_orders") or [])
        if int(row.get("con_id") or 0) == int(config.con_id)
    ]
    if cancel_result["cancel_verification"]["verified"] and not remaining_mgc:
        return {
            "status": "passed",
            "detail": "Submitted one unattended paper MGC order, verified it appeared, cancelled it, and verified no working MGC order remains.",
            "submitted_order_id": order_id,
            "submitted_perm_id": perm_id,
            "latest_order_status": cancel_result["latest_order_status"],
            "open_order_after_submit": after_submit,
            "open_order_after_cancel": final_open_orders,
            "cancel_verification": cancel_result["cancel_verification"],
            "tws_visibility_confirmation": {
                "required_for_resting_order_test": True,
                "visible_in_tws": config.visible_in_tws,
                "canceled_in_tws": config.canceled_in_tws,
                "tws_visibility_pause_seconds": float(config.tws_visibility_pause_seconds),
            },
        }
    return {
        "status": "cancel_unknown",
        "detail": cancel_result["detail"],
        "submitted_order_id": order_id,
        "submitted_perm_id": perm_id,
        "latest_order_status": cancel_result["latest_order_status"],
        "open_order_after_submit": after_submit,
        "open_order_after_cancel": final_open_orders,
        "cancel_verification": cancel_result["cancel_verification"],
        "tws_visibility_confirmation": {
            "required_for_resting_order_test": True,
            "visible_in_tws": config.visible_in_tws,
            "canceled_in_tws": config.canceled_in_tws,
            "tws_visibility_pause_seconds": float(config.tws_visibility_pause_seconds),
        },
    }


def _has_any_fill(*, latest_status: dict[str, Any] | None, execution_rows: list[dict[str, Any]]) -> bool:
    if latest_status is not None and float(_coerce_float(latest_status.get("filled")) or 0.0) > 0.0:
        return True
    return any(float(_coerce_float(row.get("quantity")) or 0.0) > 0.0 for row in execution_rows)


def _matching_rows_for_order(rows: list[dict[str, Any]], *, order_id: int, perm_id: int | None) -> list[dict[str, Any]]:
    matched: list[dict[str, Any]] = []
    for row in rows:
        broker_order_id = row.get("broker_order_id")
        row_perm_id = row.get("perm_id")
        if broker_order_id is not None and str(broker_order_id) == str(order_id):
            matched.append(dict(row))
            continue
        if perm_id is not None and row_perm_id is not None and str(row_perm_id) == str(perm_id):
            matched.append(dict(row))
    return matched


def _classify_lifecycle(lifecycle_result: dict[str, Any]) -> str:
    status = str(lifecycle_result.get("status") or "").strip().lower()
    if status == "passed":
        return "IBKR_UNATTENDED_REST_CANCEL_PASSED"
    if status == "dialog_blocked":
        return "IBKR_UNATTENDED_REST_CANCEL_DIALOG_BLOCKED"
    if status == "rejected":
        return "IBKR_UNATTENDED_REST_CANCEL_REJECTED"
    if status == "filled_unexpectedly":
        return "IBKR_UNATTENDED_REST_CANCEL_FILLED_UNEXPECTEDLY"
    return "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN"


def _build_report(
    *,
    config: IbkrUnattendedPaperRestCancelConfig,
    started_at: datetime,
    classification: str,
    caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    account_truth: dict[str, Any],
    positions_before: dict[str, Any],
    open_order_before: dict[str, Any],
    quote_context: dict[str, Any],
    pricing_context: dict[str, Any],
    preview_digest: str,
    requested_order: dict[str, Any],
    exact_contract_report: dict[str, Any],
    lifecycle_result: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    audit_events: list[dict[str, Any]],
    errors: list[dict[str, Any]],
    callback_timeline_event_count: int,
    pre_action_snapshot_validation: dict[str, Any],
) -> dict[str, Any]:
    exact_contract = dict(exact_contract_report.get("exact_contract") or {})
    return {
        "classification": classification,
        "working_order_acceptance": _build_working_order_acceptance(
            config=config,
            lifecycle_result=lifecycle_result,
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "lower_level_cancel_path": True,
        "preferred_path": "track_b_managed_exit_cancel_replace",
        "emergency_only": True,
        "emergency_legacy_rest_cancel": bool(config.emergency_legacy_rest_cancel),
        "deprecated": True,
        "account_id": account_truth.get("selected_account_id") or config.account_id,
        "client_id": config.client_id,
        "pre_action_snapshot_validation": pre_action_snapshot_validation,
        "caller_gate_check": caller_check,
        "environment_lock_check": environment_lock,
        "account_truth": account_truth,
        "positions_before": positions_before,
        "open_order_before": open_order_before,
        "exact_contract": exact_contract,
        "quote_context": quote_context,
        "pricing_context": pricing_context,
        "limit_price": requested_order.get("limit_price"),
        "preview_digest": preview_digest,
        "lifecycle": lifecycle_result,
        "guardrail_checks": guardrail_checks,
        "audit_event_count": len(audit_events),
        "callback_timeline_event_count": callback_timeline_event_count,
        "errors": errors,
    }


def _build_working_order_acceptance(
    *,
    config: IbkrUnattendedPaperRestCancelConfig,
    lifecycle_result: dict[str, Any],
) -> dict[str, Any]:
    status = str(lifecycle_result.get("status") or "").strip().lower()
    submitted_order_id = lifecycle_result.get("submitted_order_id")
    open_order_after_submit = dict(lifecycle_result.get("open_order_after_submit") or {})
    latest_order_status = dict(lifecycle_result.get("latest_order_status") or {})
    api_working_order_visible = bool(
        submitted_order_id is not None
        and (
            _find_open_order(open_order_after_submit, int(submitted_order_id)) is not None
            or str(latest_order_status.get("status") or "").strip().upper() in {"PRESUBMITTED", "SUBMITTED", "PENDINGCANCEL"}
        )
    )
    classification: str | None = None
    if status == "passed":
        classification = (
            "WORKING_ORDER_API_AND_TWS_VISIBLE"
            if config.visible_in_tws is True
            else "WORKING_ORDER_API_VISIBLE_TWS_NOT_CONFIRMED"
        )
    elif status == "filled_unexpectedly":
        classification = "WORKING_ORDER_FILLED_BEFORE_VISUAL_CONFIRMATION"
    elif status == "rejected":
        classification = "WORKING_ORDER_REJECTED"
    elif status == "cancel_unknown":
        classification = "WORKING_ORDER_CANCEL_FAILED"
    elif api_working_order_visible:
        classification = "WORKING_ORDER_API_VISIBLE_TWS_NOT_CONFIRMED"
    return {
        "classification": classification,
        "api_working_order_visible": api_working_order_visible,
        "visible_in_tws": config.visible_in_tws,
        "canceled_in_tws": config.canceled_in_tws,
        "tws_visibility_pause_seconds": float(config.tws_visibility_pause_seconds),
    }


def _extra_artifacts_from_lifecycle(lifecycle_result: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if lifecycle_result.get("positions_after_submit") is not None:
        payload["positions_after_submit"] = lifecycle_result["positions_after_submit"]
    if lifecycle_result.get("executions_after_submit") is not None:
        payload["executions_after_submit"] = lifecycle_result["executions_after_submit"]
    if lifecycle_result.get("completed_orders_after_submit") is not None:
        payload["completed_orders_after_submit"] = lifecycle_result["completed_orders_after_submit"]
    return payload


def _position_config_from_unattended(config: IbkrUnattendedPaperRestCancelConfig) -> Any:
    return type(
        "UnattendedPositionConfig",
        (),
        {
            "repo_root": config.repo_root,
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "read_only": True,
            "account_id": config.account_id,
            "symbol": config.symbol,
            "contract_month": config.contract_month,
            "exact_expiry": config.exact_expiry,
            "con_id": config.con_id,
            "local_symbol": config.local_symbol,
            "security_type": "FUT",
            "exchange": config.exchange,
            "currency": config.currency,
            "multiplier": config.multiplier,
            "timeout_seconds": config.timeout_seconds,
        },
    )()


def _manual_like_config(config: IbkrUnattendedPaperRestCancelConfig) -> Any:
    return type(
        "ManualLikeConfig",
        (),
        {
            "repo_root": config.repo_root,
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "account_id": config.account_id,
            "timeout_seconds": config.timeout_seconds,
            "test_mode": "PAPER_RESTING_TEST",
            "near_market_max_distance_ticks": 50.0,
            "delayed_quote_max_age_seconds": config.delayed_quote_max_age_seconds,
            "fill_limit_offset_ticks": config.limit_offset_ticks,
        },
    )()


def _pre_action_snapshot_validation(*, config: IbkrUnattendedPaperRestCancelConfig, now: datetime) -> dict[str, Any]:
    return validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=config.repo_root),
        expected_plan_classification=PLAN_TARGETED_CANCEL_REPLACE,
        expected_action_type="TARGETED_CANCEL_REPLACE",
        expected_target_identity=_pre_action_target_identity(config),
        max_snapshot_age_seconds=int(config.pre_action_snapshot_max_age_seconds),
        now=now,
    )


def _pre_action_target_identity(config: IbkrUnattendedPaperRestCancelConfig) -> dict[str, Any]:
    identity = {
        "account_id": config.account_id,
        "symbol": config.symbol,
        "contract": config.local_symbol,
        "con_id": config.con_id,
        "action": config.action,
        "quantity": config.quantity,
    }
    if config.expected_broker_order_id:
        identity["broker_order_id"] = config.expected_broker_order_id
    if config.expected_perm_id:
        identity["perm_id"] = config.expected_perm_id
    return identity


def _audit_config(config: IbkrUnattendedPaperRestCancelConfig) -> Any:
    return type(
        "AuditConfig",
        (),
        {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
        },
    )()


def _blocked_report(
    *,
    config: IbkrUnattendedPaperRestCancelConfig,
    started_at: datetime,
    caller_check: dict[str, Any],
    environment_lock: dict[str, Any],
    requested_order: dict[str, Any],
    guardrail_checks: list[dict[str, Any]],
    audit_events: list[dict[str, Any]],
    detail: str,
    pre_action_snapshot_validation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "classification": "IBKR_UNATTENDED_REST_CANCEL_UNKNOWN",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "lower_level_cancel_path": True,
        "preferred_path": "track_b_managed_exit_cancel_replace",
        "emergency_only": True,
        "emergency_legacy_rest_cancel": bool(config.emergency_legacy_rest_cancel),
        "deprecated": True,
        "account_id": config.account_id,
        "client_id": config.client_id,
        "pre_action_snapshot_validation": pre_action_snapshot_validation or {},
        "caller_gate_check": caller_check,
        "environment_lock_check": environment_lock,
        "quote_context": {},
        "pricing_context": {},
        "open_order_before": _not_run_snapshot("Open-order baseline was not captured."),
        "limit_price": requested_order.get("limit_price"),
        "lifecycle": {"status": "blocked", "detail": detail},
        "guardrail_checks": guardrail_checks,
        "audit_event_count": len(audit_events),
        "callback_timeline_event_count": 0,
        "errors": [],
    }


def _parse_iso_timestamp(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _first_failed_guardrail_detail(guardrail_checks: list[dict[str, Any]]) -> str:
    for check in guardrail_checks:
        if check.get("blocking") and not check.get("passed"):
            return str(check.get("detail") or f"Blocking guardrail failed: {check.get('name')}")
    return "The unattended paper rest/cancel harness was blocked by a required guardrail."
