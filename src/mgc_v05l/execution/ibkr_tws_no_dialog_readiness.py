"""Read-only TWS no-dialog readiness preflight for unattended IBKR paper orders."""

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
from .ibkr_manual_paper_submit import (
    _SEVERE_CONNECTION_ERROR_CODES,
    _build_callback_timeline,
    _qualify_mgc_contract,
    _refresh_open_orders_snapshot,
)
from .ibkr_paper_order_preview import evaluate_manual_preview_caller, evaluate_paper_preview_environment_lock
from .ibkr_position_reconciliation import (
    IbkrPositionReconciliationCollector,
    IbkrPositionReconciliationTransport,
    _collect_account_truth,
    _collect_exact_contract_context,
    _collect_managed_account_context,
    _start_runtime,
)
from .ibkr_read_only_verifier import (
    _build_positions_snapshot,
    _wait_for_event,
    _wait_for_connection_ready,
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


class IbkrTwsNoDialogReadinessError(RuntimeError):
    """Base error for the TWS no-dialog readiness preflight."""


@dataclass(frozen=True)
class IbkrTwsNoDialogReadinessConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    read_only: bool
    account_id: str | None = _EXPECTED_ACCOUNT_ID
    timeout_seconds: float = 15.0
    caller_path: str = "manual_cli"
    symbol: str = _EXPECTED_SYMBOL
    contract_month: str = _EXPECTED_CONTRACT_MONTH
    exact_expiry: str = _EXPECTED_EXACT_EXPIRY
    con_id: int = _EXPECTED_CON_ID
    local_symbol: str = _EXPECTED_LOCAL_SYMBOL
    exchange: str = _EXPECTED_EXCHANGE
    currency: str = _EXPECTED_CURRENCY
    multiplier: str = _EXPECTED_MULTIPLIER


@dataclass(frozen=True)
class IbkrTwsNoDialogReadinessArtifacts:
    classification: str
    report: dict[str, Any]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "TWS_NO_DIALOG_BLOCKED" else 1


@dataclass
class _ReadinessRuntime:
    session: IbkrSession
    client: IbkrClient
    collector: IbkrPositionReconciliationCollector
    transport: IbkrPositionReconciliationTransport


def run_ibkr_tws_no_dialog_readiness_preflight(
    *,
    config: IbkrTwsNoDialogReadinessConfig,
    transport_factory: Callable[..., Any] = IbkrPositionReconciliationTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> IbkrTwsNoDialogReadinessArtifacts:
    started_at = datetime.now(timezone.utc)
    environment_lock = evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port)
    caller_evaluation = evaluate_manual_preview_caller(caller_path=config.caller_path, stack_provider=stack_provider)
    runtime: _ReadinessRuntime | None = None
    try:
        if not bool(config.read_only):
            raise IbkrTwsNoDialogReadinessError("The TWS no-dialog readiness preflight is read-only only. Re-run with read_only=true.")
        if not environment_lock.get("passed"):
            raise IbkrTwsNoDialogReadinessError(str(environment_lock.get("detail") or "Environment lock failed."))
        if not caller_evaluation.get("passed"):
            raise IbkrTwsNoDialogReadinessError(str(caller_evaluation.get("detail") or "Caller path is not allowed."))
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
            raise IbkrTwsNoDialogReadinessError(
                (
                    f"IBKR TWS readiness handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"IBKR TWS readiness handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        selected_account_id = _collect_managed_account_context(
            config=_reconciliation_config_from_readiness(config),
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        exact_contract, contract_report = _collect_exact_contract_context(
            config=_reconciliation_config_from_readiness(config),
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        account_truth, _provider_snapshot = _collect_account_truth(
            config=_reconciliation_config_from_readiness(config),
            runtime=runtime,
            selected_account_id=selected_account_id,
            sleep_fn=sleep_fn,
        )
        runtime.collector.positions_ready.clear()
        runtime.client.request_positions()
        runtime.transport.req_positions()
        if not _wait_for_event(
            runtime.collector.positions_ready,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        ):
            latest_error = runtime.collector.latest_error()
            raise IbkrTwsNoDialogReadinessError(
                (
                    f"IBKR positions refresh did not complete: {latest_error['message']}"
                    if latest_error is not None
                    else f"IBKR positions refresh did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        positions = _build_positions_snapshot(client=runtime.client, selected_account_id=selected_account_id)
        open_orders = _refresh_open_orders_snapshot(
            runtime=runtime,
            selected_account_id=selected_account_id,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        matching_mgc_open_orders = [
            row
            for row in list(open_orders.get("open_orders") or [])
            if int(row.get("con_id") or 0) == int(exact_contract.con_id or 0)
            or (
                str(row.get("symbol") or "").strip().upper() == config.symbol
                and str(row.get("local_symbol") or "").strip().upper() == config.local_symbol
            )
        ]
        manual_setting_rows = _manual_tws_setting_rows(config=config)
        classification = _classify_readiness(
            selected_account_id=selected_account_id,
            contract_report=contract_report,
            open_orders=open_orders,
            matching_mgc_open_orders=matching_mgc_open_orders,
            manual_setting_rows=manual_setting_rows,
        )
        report = {
            "classification": classification,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at.isoformat(),
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "read_only": bool(config.read_only),
            "account_id": selected_account_id,
            "connection_check": {
                "connected": bool(runtime.session.state.connected),
                "client_id": config.client_id,
                "server_version": runtime.transport.server_version(),
                "tws_connection_time": runtime.transport.tws_connection_time(),
                "connection_timestamp": runtime.session.state.connected_at.isoformat() if runtime.session.state.connected_at is not None else None,
            },
            "environment_lock_check": {
                "configured_mode": config.mode,
                "configured_host": config.host,
                "configured_port": config.port,
                "expected_mode": _EXPECTED_MODE,
                "expected_host": _EXPECTED_HOST,
                "expected_port": _EXPECTED_PORT,
                "read_only": bool(config.read_only),
                "passed": True,
            },
            "caller_gate_check": dict(caller_evaluation),
            "account_truth": account_truth,
            "positions": positions,
            "open_orders": open_orders,
            "working_mgc_order_check": {
                "exact_contract_identifier": {
                    "symbol": config.symbol,
                    "contract_month": config.contract_month,
                    "exact_expiry": exact_contract.expiry,
                    "con_id": exact_contract.con_id,
                    "local_symbol": exact_contract.local_symbol,
                },
                "matching_working_order_count": len(matching_mgc_open_orders),
                "matching_working_orders": matching_mgc_open_orders,
                "passed": not bool(matching_mgc_open_orders),
            },
            "contract_qualification": {
                "friendly_label": f"{config.symbol} {config.contract_month}",
                **contract_report,
            },
            "manual_tws_setting_review": {
                "status": "unverified_from_api_only",
                "settings": manual_setting_rows,
                "conclusion": (
                    "Read-only API checks can prove paper connectivity, account truth, open-order baseline, and exact contract qualification, "
                    "but they cannot prove that TWS precaution dialogs are disabled. Manual TWS setting inspection is still required."
                ),
            },
            "readiness_summary": {
                "code_preflight_ready_except_tws_no_dialog_uncertainty": bool(
                    selected_account_id == config.account_id
                    and contract_report.get("ok")
                    and not bool(matching_mgc_open_orders)
                ),
                "unattended_submit_allowed_now": classification == "TWS_NO_DIALOG_READY",
                "next_required_step": (
                    "Inspect TWS API precaution settings and, if they appear correct, prove no-dialog behavior with one separate unattended paper rest/cancel test."
                ),
            },
            "callback_timeline_event_count": len(_build_callback_timeline(runtime)),
            "errors": list(runtime.collector.errors),
        }
        return IbkrTwsNoDialogReadinessArtifacts(classification=classification, report=report)
    except Exception as exc:
        report = {
            "classification": "TWS_NO_DIALOG_BLOCKED",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": started_at.isoformat(),
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "read_only": bool(config.read_only),
            "account_id": config.account_id,
            "environment_lock_check": {
                "configured_mode": config.mode,
                "configured_host": config.host,
                "configured_port": config.port,
                "expected_mode": _EXPECTED_MODE,
                "expected_host": _EXPECTED_HOST,
                "expected_port": _EXPECTED_PORT,
                "read_only": bool(config.read_only),
                "passed": False,
            },
            "manual_tws_setting_review": {
                "status": "not_reached",
                "settings": _manual_tws_setting_rows(config=config),
            },
            "diagnosis": {
                "classification": "TWS_NO_DIALOG_BLOCKED",
                "conclusion": str(exc),
            },
            "errors": [] if runtime is None else list(runtime.collector.errors),
        }
        return IbkrTwsNoDialogReadinessArtifacts(classification="TWS_NO_DIALOG_BLOCKED", report=report)
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


def write_ibkr_tws_no_dialog_readiness_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrTwsNoDialogReadinessArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_tws_no_dialog_readiness_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_tws_no_dialog_readiness_report.md").write_text(
        render_ibkr_tws_no_dialog_readiness_markdown(artifacts.report),
        encoding="utf-8",
    )


def render_ibkr_tws_no_dialog_readiness_markdown(report: dict[str, Any]) -> str:
    connection = dict(report.get("connection_check") or {})
    contract_qualification = dict(report.get("contract_qualification") or {})
    qualified_contract = dict(contract_qualification.get("exact_contract") or contract_qualification.get("qualified_contract") or {})
    readiness = dict(report.get("readiness_summary") or {})
    manual_review = dict(report.get("manual_tws_setting_review") or {})
    setting_rows = list(manual_review.get("settings") or [])
    working_mgc = dict(report.get("working_mgc_order_check") or {})
    lines = [
        "# IBKR TWS No-Dialog Readiness Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- environment: `{report.get('mode')} / {report.get('host')} / {report.get('port')}`",
        f"- account: `{report.get('account_id')}`",
        f"- client id: `{connection.get('client_id')}`",
        f"- exact contract: `MGC {qualified_contract.get('expiry')}` / `conId={qualified_contract.get('con_id')}` / `localSymbol={qualified_contract.get('local_symbol')}`",
        "",
        "## Read-Only Preflight",
        "",
        f"- account truth read: `{bool(report.get('account_truth'))}`",
        f"- open-order baseline count: `{dict(report.get('open_orders') or {}).get('open_order_count')}`",
        f"- working MGC orders detected: `{working_mgc.get('matching_working_order_count')}`",
        f"- exact contract qualification ok: `{contract_qualification.get('ok')}`",
        f"- code preflight ready except no-dialog uncertainty: `{readiness.get('code_preflight_ready_except_tws_no_dialog_uncertainty')}`",
        "",
        "## No-Dialog Status",
        "",
        f"- manual TWS setting review status: `{manual_review.get('status')}`",
        f"- unattended submit allowed now: `{readiness.get('unattended_submit_allowed_now')}`",
        f"- conclusion: {manual_review.get('conclusion') or dict(report.get('diagnosis') or {}).get('conclusion') or readiness.get('next_required_step')}",
        "",
        "## Required TWS Settings To Verify Manually",
        "",
    ]
    for row in setting_rows:
        lines.append(f"- `{row.get('setting')}`: {row.get('status')} - {row.get('detail')}")
    lines.extend(
        [
            "",
            "## Next Step",
            "",
            f"- {readiness.get('next_required_step') or 'Inspect TWS settings before any unattended paper order attempt.'}",
        ]
    )
    return "\n".join(lines)


def _build_runtime(
    *,
    config: IbkrTwsNoDialogReadinessConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _ReadinessRuntime:
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
    collector = IbkrPositionReconciliationCollector(client)
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
    return _ReadinessRuntime(session=session, client=client, collector=collector, transport=transport)


def _reconciliation_config_from_readiness(config: IbkrTwsNoDialogReadinessConfig) -> Any:
    return type(
        "ReadinessPositionConfig",
        (),
        {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "read_only": config.read_only,
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
            "repo_root": config.repo_root,
        },
    )()


def _manual_tws_setting_rows(*, config: IbkrTwsNoDialogReadinessConfig) -> list[dict[str, Any]]:
    return [
        {
            "setting": "Enable ActiveX and Socket Clients",
            "status": "must_verify_manually",
            "detail": "Must be enabled in TWS Global Configuration > API > Settings.",
        },
        {
            "setting": "Socket Port",
            "status": "must_verify_manually",
            "detail": f"Must be set to `{config.port}` for this unattended paper design.",
        },
        {
            "setting": "Read-Only API",
            "status": "must_verify_manually",
            "detail": "Must be disabled for any future unattended submit path. This read-only preflight cannot prove the GUI toggle state.",
        },
        {
            "setting": "Trusted IPs / localhost access",
            "status": "must_verify_manually",
            "detail": "Localhost must be allowed or not restricted in a way that blocks the app path.",
        },
        {
            "setting": "Bypass Order Precautions for API orders",
            "status": "must_verify_manually",
            "detail": "Must be enabled for unattended no-dialog paper orders. If disabled, unattended mode must not be allowed.",
        },
    ]


def _classify_readiness(
    *,
    selected_account_id: str,
    contract_report: dict[str, Any],
    open_orders: dict[str, Any],
    matching_mgc_open_orders: list[dict[str, Any]],
    manual_setting_rows: list[dict[str, Any]],
) -> str:
    if selected_account_id != _EXPECTED_ACCOUNT_ID:
        return "TWS_NO_DIALOG_BLOCKED"
    if not bool(contract_report.get("ok")):
        return "TWS_NO_DIALOG_BLOCKED"
    if int(open_orders.get("open_order_count") or 0) < 0:
        return "TWS_NO_DIALOG_BLOCKED"
    if matching_mgc_open_orders:
        return "TWS_NO_DIALOG_BLOCKED"
    if any(str(row.get("status") or "") == "must_change" for row in manual_setting_rows):
        return "TWS_NO_DIALOG_NEEDS_MANUAL_SETTING"
    return "TWS_NO_DIALOG_UNVERIFIED"
