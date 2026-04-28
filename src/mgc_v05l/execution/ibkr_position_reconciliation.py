"""Read-only IBKR paper execution-to-position reconciliation helpers."""

from __future__ import annotations

import csv
import inspect
import json
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from ..brokers.ibkr import (
    IbkrClient,
    IbkrContractResolver,
    IbkrPositionRecord,
    IbkrQualifiedContract,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)
from .ibkr_execution_provider import IbkrExecutionProvider
from .ibkr_manual_paper_submit import (
    _SEVERE_CONNECTION_ERROR_CODES,
    IbkrManualPaperSubmitCollector,
    IbkrManualPaperSubmitError,
    IbkrManualPaperSubmitTransport,
    _build_callback_timeline,
    _qualified_contract_with_api_details,
)
from .ibkr_paper_order_preview import evaluate_paper_preview_environment_lock
from .ibkr_read_only_verifier import (
    _ACCOUNT_SUMMARY_TAGS,
    _build_account_truth_snapshot,
    _first_non_empty,
    _qualified_contract_to_dict,
    _resolve_selected_account_id,
    _wait_for_connection_ready,
    _wait_for_event,
    IbkrReadOnlyApiTransportConfig,
)

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_EXPECTED_SYMBOL = "MGC"
_EXPECTED_CONTRACT_MONTH = "202606"
_EXPECTED_EXACT_EXPIRY = "20260626"
_EXPECTED_LOCAL_SYMBOL = "MGCM6"
_EXPECTED_CON_ID = 712565978
_EXPECTED_ACCOUNT_ID = "DUM882026"
_EXPECTED_SECURITY_TYPE = "FUT"
_EXPECTED_EXCHANGE = "COMEX"
_EXPECTED_CURRENCY = "USD"
_EXPECTED_MULTIPLIER = "10"
_POSITIONS_REQUEST_ID = 9801
_ACCOUNT_SUMMARY_REQUEST_ID = 9802
_CONTRACT_DETAILS_REQUEST_ID = 9803
_EXECUTIONS_REQUEST_ID = 9804
_DEFAULT_OBSERVATION_WINDOW_SECONDS = 8.0
_DEFAULT_SAMPLE_INTERVAL_SECONDS = 1.0
_DEFAULT_TIMEOUT_SECONDS = 15.0
_DEFAULT_REFERENCE_FILL_REPORT = (
    Path("outputs")
    / "reports"
    / "ibkr_app_paper_fill_retest_contract_fix"
    / "ibkr_manual_paper_fill_test_report.json"
)


class IbkrPositionReconciliationError(RuntimeError):
    """Base error for the IBKR paper position reconciliation pass."""


@dataclass(frozen=True)
class IbkrPositionReconciliationConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    read_only: bool
    account_id: str | None = _EXPECTED_ACCOUNT_ID
    symbol: str = _EXPECTED_SYMBOL
    contract_month: str = _EXPECTED_CONTRACT_MONTH
    exact_expiry: str = _EXPECTED_EXACT_EXPIRY
    con_id: int = _EXPECTED_CON_ID
    local_symbol: str = _EXPECTED_LOCAL_SYMBOL
    security_type: str = _EXPECTED_SECURITY_TYPE
    exchange: str = _EXPECTED_EXCHANGE
    currency: str = _EXPECTED_CURRENCY
    multiplier: str = _EXPECTED_MULTIPLIER
    timeout_seconds: float = _DEFAULT_TIMEOUT_SECONDS
    observation_window_seconds: float = _DEFAULT_OBSERVATION_WINDOW_SECONDS
    sample_interval_seconds: float = _DEFAULT_SAMPLE_INTERVAL_SECONDS
    recent_fill_lookback_minutes: int = 120
    caller_path: str = "manual_cli"
    reference_fill_report_path: Path | None = None


@dataclass(frozen=True)
class IbkrPositionReconciliationArtifacts:
    classification: str
    report: dict[str, Any]
    snapshots: list[dict[str, Any]]
    match_rows: list[dict[str, Any]]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "IBKR_POSITION_RECONCILIATION_BLOCKED" else 1


@dataclass
class _ReconciliationRuntime:
    session: IbkrSession
    client: IbkrClient
    collector: "IbkrPositionReconciliationCollector"
    transport: "IbkrPositionReconciliationTransport"


class IbkrPositionReconciliationCollector(IbkrManualPaperSubmitCollector):
    """Submit-harness collector extended with portfolio-update capture."""

    def __init__(self, client: IbkrClient) -> None:
        super().__init__(client)
        self.portfolio_update_rows: list[dict[str, Any]] = []

    def update_portfolio(
        self,
        *,
        account_id: str,
        contract: dict[str, Any],
        quantity: str | int | float,
        market_price: str | int | float | None,
        market_value: str | int | float | None,
        average_cost: str | int | float | None,
        unrealized_pnl: str | int | float | None,
        realized_pnl: str | int | float | None,
    ) -> None:
        self.portfolio_update_rows.append(
            {
                "account_id": str(account_id or "").strip(),
                "contract": dict(contract or {}),
                "quantity": _coerce_float(quantity),
                "market_price": _coerce_float(market_price),
                "market_value": _coerce_float(market_value),
                "average_cost": _coerce_float(average_cost),
                "unrealized_pnl": _coerce_float(unrealized_pnl),
                "realized_pnl": _coerce_float(realized_pnl),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )


class IbkrPositionReconciliationTransport(IbkrManualPaperSubmitTransport):
    """Read-only transport wrapper for the reconciliation pass."""


def run_ibkr_position_reconciliation(
    *,
    config: IbkrPositionReconciliationConfig,
    transport_factory: Callable[..., Any] = IbkrPositionReconciliationTransport,
    module_loader: Callable[[str], Any] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    stack_provider: Callable[[], list[Any]] = inspect.stack,
) -> IbkrPositionReconciliationArtifacts:
    del stack_provider
    started_at = datetime.now(timezone.utc)
    reference_fill_report_path = _resolve_reference_fill_report_path(config)
    environment_lock = evaluate_paper_preview_environment_lock(mode=config.mode, host=config.host, port=config.port)
    runtime: _ReconciliationRuntime | None = None
    snapshots: list[dict[str, Any]] = []
    match_rows: list[dict[str, Any]] = []
    try:
        if not bool(config.read_only):
            raise IbkrPositionReconciliationError("This reconciliation pass is read-only only. Re-run with read_only=true.")
        if not environment_lock.get("passed"):
            raise IbkrPositionReconciliationError(str(environment_lock.get("detail") or "Environment lock failed."))
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
            raise IbkrPositionReconciliationError(
                (
                    f"IBKR position reconciliation handshake failed: {latest_error['message']}"
                    if latest_error is not None
                    else f"IBKR position reconciliation handshake did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        selected_account_id = _collect_managed_account_context(config=config, runtime=runtime, sleep_fn=sleep_fn)
        exact_contract, contract_report = _collect_exact_contract_context(
            config=config,
            runtime=runtime,
            sleep_fn=sleep_fn,
        )
        account_truth, provider_snapshot = _collect_account_truth(
            config=config,
            runtime=runtime,
            selected_account_id=selected_account_id,
            sleep_fn=sleep_fn,
        )
        snapshots = _collect_position_snapshots(
            config=config,
            runtime=runtime,
            selected_account_id=selected_account_id,
            exact_contract=exact_contract,
            sleep_fn=sleep_fn,
        )
        execution_truth = _collect_execution_truth(
            config=config,
            runtime=runtime,
            selected_account_id=selected_account_id,
            exact_contract=exact_contract,
            sleep_fn=sleep_fn,
        )
        reference_fill_report = _load_reference_fill_report(reference_fill_report_path)
        match_rows = _build_execution_position_match_rows(
            snapshots=snapshots,
            execution_rows=execution_truth["matching_execution_rows"],
            completed_order_rows=execution_truth["matching_completed_order_rows"],
            exact_contract=exact_contract,
            reference_fill_report=reference_fill_report,
        )
        diagnosis = _diagnose_reconciliation(
            snapshots=snapshots,
            execution_truth=execution_truth,
            exact_contract=exact_contract,
            reference_fill_report=reference_fill_report,
        )
        report = {
            "classification": diagnosis["classification"],
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
                "connected_at": runtime.session.state.connected_at.isoformat() if runtime.session.state.connected_at is not None else None,
            },
            "environment_lock_check": {
                "configured_mode": config.mode,
                "configured_host": config.host,
                "configured_port": config.port,
                "expected_mode": _EXPECTED_MODE,
                "expected_host": _EXPECTED_HOST,
                "expected_port": _EXPECTED_PORT,
                "read_only": bool(config.read_only),
                "ok": True,
                "passed": True,
            },
            "account_truth": account_truth,
            "provider_snapshot": provider_snapshot,
            "contract_report": contract_report,
            "portfolio_update_summary": _build_portfolio_update_summary(
                collector=runtime.collector,
                selected_account_id=selected_account_id,
                exact_contract=exact_contract,
            ),
            "execution_truth": execution_truth,
            "reference_fill_report_path": str(reference_fill_report_path) if reference_fill_report_path is not None else None,
            "reference_fill_report_loaded": reference_fill_report is not None,
            "snapshots_observed": len(snapshots),
            "diagnosis": diagnosis,
            "callback_timeline_event_count": len(_build_callback_timeline(runtime)),
            "request_log": [
                {
                    "request_type": row.request_type,
                    "requested_at": row.requested_at.isoformat() if row.requested_at is not None else None,
                    "details": dict(row.details or {}),
                }
                for row in runtime.client.request_log()
            ],
            "errors": list(runtime.collector.errors),
        }
        return IbkrPositionReconciliationArtifacts(
            classification=str(diagnosis["classification"]),
            report=report,
            snapshots=snapshots,
            match_rows=match_rows,
        )
    except Exception as exc:
        report = {
            "classification": "IBKR_POSITION_RECONCILIATION_BLOCKED",
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
                "ok": False,
                "passed": False,
            },
            "diagnosis": {
                "classification": "IBKR_POSITION_RECONCILIATION_BLOCKED",
                "likely_root_cause": "reconciliation_blocked",
                "conclusion": str(exc),
            },
            "errors": [] if runtime is None else list(runtime.collector.errors),
        }
        return IbkrPositionReconciliationArtifacts(
            classification="IBKR_POSITION_RECONCILIATION_BLOCKED",
            report=report,
            snapshots=snapshots,
            match_rows=match_rows,
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


def write_ibkr_position_reconciliation_artifacts(
    *,
    output_dir: Path,
    artifacts: IbkrPositionReconciliationArtifacts,
) -> None:
    reports_dir = Path(output_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "ibkr_position_reconciliation_report.json").write_text(
        json.dumps(artifacts.report, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (reports_dir / "ibkr_position_reconciliation_report.md").write_text(
        render_ibkr_position_reconciliation_markdown(artifacts.report),
        encoding="utf-8",
    )
    with (reports_dir / "ibkr_position_reconciliation_snapshots.jsonl").open("w", encoding="utf-8") as handle:
        for row in artifacts.snapshots:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    fieldnames = [
        "snapshot_index",
        "snapshot_generated_at",
        "position_match_status",
        "position_quantity",
        "portfolio_update_quantity",
        "execution_id",
        "execution_side",
        "execution_quantity",
        "execution_price",
        "execution_time",
        "completed_order_status",
        "perm_id",
        "broker_order_id",
        "note",
    ]
    with (reports_dir / "ibkr_execution_position_match_table.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in artifacts.match_rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def render_ibkr_position_reconciliation_markdown(report: dict[str, Any]) -> str:
    diagnosis = dict(report.get("diagnosis") or {})
    connection = dict(report.get("connection_check") or {})
    execution_truth = dict(report.get("execution_truth") or {})
    contract_report = dict(report.get("contract_report") or {})
    exact_contract = dict(contract_report.get("exact_contract") or {})
    portfolio_update_summary = dict(report.get("portfolio_update_summary") or {})
    lines = [
        "# IBKR Position Reconciliation Report",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- environment: `{report.get('mode')} / {report.get('host')} / {report.get('port')}`",
        f"- account: `{report.get('account_id')}`",
        f"- client id: `{connection.get('client_id')}`",
        f"- exact contract: `MGC {exact_contract.get('expiry')}` / `conId={exact_contract.get('con_id')}` / `localSymbol={exact_contract.get('local_symbol')}`",
        "",
        "## Summary",
        "",
        f"- conclusion: {diagnosis.get('conclusion')}",
        f"- likely cause of prior `0.0` position snapshot: `{diagnosis.get('likely_root_cause')}`",
        f"- matching BUY executions observed: `{execution_truth.get('matching_execution_count')}`",
        f"- matching completed orders observed: `{execution_truth.get('matching_completed_order_count')}`",
        f"- exact position snapshots observed: `{report.get('snapshots_observed')}`",
        f"- portfolio updates observed for exact contract: `{portfolio_update_summary.get('matching_row_count')}`",
        "",
        "## Current Truth",
        "",
        f"- latest exact position quantity: `{diagnosis.get('latest_exact_position_quantity')}`",
        f"- exact position quantity ever observed during window: `{diagnosis.get('max_exact_position_quantity')}`",
        f"- latest matching execution side: `{diagnosis.get('latest_matching_execution_side')}`",
        f"- latest matching execution time: `{diagnosis.get('latest_matching_execution_time')}`",
        f"- latest matching execution quantity: `{diagnosis.get('latest_matching_execution_quantity')}`",
        f"- latest matching perm id: `{diagnosis.get('latest_matching_perm_id')}`",
        "",
        "## Follow-up",
        "",
    ]
    classification = str(report.get("classification") or "")
    if classification == "IBKR_POSITION_RECONCILED_LONG_MGC":
        lines.append("- current broker position truth confirms the paper account is long MGC; use a separate manual app-path close test if you want to flatten it.")
    elif classification == "IBKR_POSITION_RECONCILED_FLAT":
        lines.append("- current broker position truth is flat; review matching SELL execution/completed-order evidence to determine whether the position was manually flattened.")
    else:
        lines.append("- position truth is still not fully authoritative against the confirmed fill; tighten post-fill position refresh sequencing before relying on an immediate one-shot snapshot.")
    if not bool(portfolio_update_summary.get("portfolio_callback_supported")):
        lines.append("- the current bridge does not treat `updatePortfolio` as authoritative position state; this pass sampled it only as supplementary evidence.")
    return "\n".join(lines)


def _build_runtime(
    *,
    config: IbkrPositionReconciliationConfig,
    transport_factory: Callable[..., Any],
    module_loader: Callable[[str], Any] | None,
) -> _ReconciliationRuntime:
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
    return _ReconciliationRuntime(session=session, client=client, collector=collector, transport=transport)


def _start_runtime(runtime: _ReconciliationRuntime) -> None:
    thread = threading.Thread(target=runtime.transport.run_loop, name="ibkr_position_reconciliation_loop", daemon=True)
    thread.start()


def _collect_managed_account_context(
    *,
    config: IbkrPositionReconciliationConfig,
    runtime: _ReconciliationRuntime,
    sleep_fn: Callable[[float], None],
) -> str:
    runtime.client.request_managed_accounts()
    runtime.transport.req_managed_accounts()
    if not _wait_for_event(
        runtime.collector.managed_accounts_ready,
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    ):
        latest_error = runtime.collector.latest_error()
        raise IbkrPositionReconciliationError(
            (
                f"IBKR managed accounts were not exposed: {latest_error['message']}"
                if latest_error is not None
                else f"IBKR managed accounts were not exposed within {config.timeout_seconds:.1f}s."
            )
        )
    selected_account_id = _resolve_selected_account_id(
        requested_account_id=config.account_id,
        managed_accounts=runtime.client.connection_state().managed_accounts,
    )
    return selected_account_id


def _collect_exact_contract_context(
    *,
    config: IbkrPositionReconciliationConfig,
    runtime: _ReconciliationRuntime,
    sleep_fn: Callable[[float], None],
) -> tuple[IbkrQualifiedContract, dict[str, Any]]:
    contract = IbkrQualifiedContract(
        internal_symbol=config.symbol,
        broker_symbol=config.symbol,
        local_symbol=config.local_symbol,
        security_type=config.security_type,
        exchange=config.exchange,
        currency=config.currency,
        expiry=config.exact_expiry,
        multiplier=config.multiplier,
        trading_class=config.symbol,
        con_id=config.con_id,
        metadata={"contract_month": config.contract_month},
    )
    runtime.client.record_event("req_contract_details_invoked", payload={"request_id": _CONTRACT_DETAILS_REQUEST_ID})
    event = runtime.collector.contract_details_event(_CONTRACT_DETAILS_REQUEST_ID)
    runtime.transport.req_contract_details(request_id=_CONTRACT_DETAILS_REQUEST_ID, contract=contract)
    event_ok = _wait_for_event(
        event,
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    contract_rows = list(runtime.collector.contract_details_rows.get(_CONTRACT_DETAILS_REQUEST_ID, []))
    exact_contract = contract
    if contract_rows:
        exact_contract = _qualified_contract_with_api_details(contract, contract_rows[0])
    latest_error = next(
        (
            row
            for row in reversed(runtime.collector.errors)
            if row.get("request_id") == _CONTRACT_DETAILS_REQUEST_ID
        ),
        None,
    )
    return exact_contract, {
        "request_id": _CONTRACT_DETAILS_REQUEST_ID,
        "ok": bool(event_ok and contract_rows),
        "detail": (
            "Exact MGC contract details received from TWS."
            if event_ok and contract_rows
            else (
                f"IBKR error {latest_error['code']}: {latest_error['message']}"
                if latest_error is not None
                else f"No exact MGC contract details returned within {config.timeout_seconds:.1f}s."
            )
        ),
        "exact_contract": _qualified_contract_to_dict(exact_contract),
        "api_contract_details": contract_rows,
    }


def _collect_account_truth(
    *,
    config: IbkrPositionReconciliationConfig,
    runtime: _ReconciliationRuntime,
    selected_account_id: str,
    sleep_fn: Callable[[float], None],
) -> tuple[dict[str, Any], dict[str, Any]]:
    runtime.transport.req_account_summary(request_id=_ACCOUNT_SUMMARY_REQUEST_ID, group_name="All", tags=_ACCOUNT_SUMMARY_TAGS)
    runtime.client.request_balances()
    runtime.transport.req_account_updates(subscribe=True, account_id=selected_account_id)
    summary_ok = _wait_for_event(
        runtime.collector.account_summary_event(_ACCOUNT_SUMMARY_REQUEST_ID),
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    balances_ok = _wait_for_event(
        runtime.collector.balances_ready,
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if not summary_ok or not balances_ok:
        latest_error = runtime.collector.latest_error()
        raise IbkrPositionReconciliationError(
            (
                f"IBKR account truth refresh did not complete: {latest_error['message']}"
                if latest_error is not None
                else f"IBKR account truth refresh did not complete within {config.timeout_seconds:.1f}s."
            )
        )
    provider = IbkrExecutionProvider(config.repo_root, session=runtime.session, client=runtime.client)
    snapshot = provider.snapshot_state(force_refresh=True)
    return _build_account_truth_snapshot(
        config=type("ReadOnlyConfig", (), {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "read_only": config.read_only,
        })(),
        collector=runtime.collector,
        session=runtime.session,
        snapshot=snapshot,
        selected_account_id=selected_account_id,
    ), snapshot


def _collect_position_snapshots(
    *,
    config: IbkrPositionReconciliationConfig,
    runtime: _ReconciliationRuntime,
    selected_account_id: str,
    exact_contract: IbkrQualifiedContract,
    sleep_fn: Callable[[float], None],
) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    deadline = time.monotonic() + max(float(config.observation_window_seconds), float(config.sample_interval_seconds))
    index = 0
    while True:
        runtime.collector.positions_ready.clear()
        runtime.client.request_positions()
        runtime.client.record_event("req_positions_invoked", payload={"sample_index": index, "request_id": _POSITIONS_REQUEST_ID})
        runtime.transport.req_positions()
        positions_ok = _wait_for_event(
            runtime.collector.positions_ready,
            collector=runtime.collector,
            timeout_seconds=config.timeout_seconds,
            sleep_fn=sleep_fn,
        )
        if not positions_ok:
            latest_error = runtime.collector.latest_error()
            raise IbkrPositionReconciliationError(
                (
                    f"IBKR position refresh did not complete: {latest_error['message']}"
                    if latest_error is not None
                    else f"IBKR position refresh did not complete within {config.timeout_seconds:.1f}s."
                )
            )
        rows = _build_exact_position_rows(
            positions=runtime.client.positions(),
            selected_account_id=selected_account_id,
        )
        exact_rows = [row for row in rows if _matches_exact_contract(row, exact_contract)]
        near_rows = [
            row
            for row in rows
            if str(row.get("symbol") or "").strip().upper() == config.symbol and not _matches_exact_contract(row, exact_contract)
        ]
        portfolio_exact_rows = _matching_portfolio_update_rows(
            collector=runtime.collector,
            selected_account_id=selected_account_id,
            exact_contract=exact_contract,
        )
        snapshots.append(
            {
                "snapshot_index": index,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "selected_account_id": selected_account_id,
                "position_rows": rows,
                "exact_position_rows": exact_rows,
                "near_match_position_rows": near_rows,
                "latest_exact_position_quantity": _latest_quantity(exact_rows),
                "latest_portfolio_update_quantity": _latest_portfolio_quantity(portfolio_exact_rows),
                "portfolio_exact_rows": portfolio_exact_rows,
            }
        )
        index += 1
        if time.monotonic() >= deadline:
            break
        sleep_fn(float(config.sample_interval_seconds))
    return snapshots


def _collect_execution_truth(
    *,
    config: IbkrPositionReconciliationConfig,
    runtime: _ReconciliationRuntime,
    selected_account_id: str,
    exact_contract: IbkrQualifiedContract,
    sleep_fn: Callable[[float], None],
) -> dict[str, Any]:
    runtime.client.request_completed_orders()
    runtime.client.record_event("req_completed_orders_invoked", payload={"api_only": False})
    runtime.collector.reset_completed_orders_ready()
    runtime.transport.req_completed_orders(api_only=False)
    runtime.client.request_executions()
    runtime.client.record_event("req_executions_invoked", payload={"request_id": _EXECUTIONS_REQUEST_ID})
    runtime.collector.reset_executions_ready(_EXECUTIONS_REQUEST_ID)
    runtime.transport.req_executions(request_id=_EXECUTIONS_REQUEST_ID)
    completed_ok = _wait_for_event(
        runtime.collector.completed_orders_ready,
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    executions_ok = _wait_for_event(
        runtime.collector.executions_event(_EXECUTIONS_REQUEST_ID),
        collector=runtime.collector,
        timeout_seconds=config.timeout_seconds,
        sleep_fn=sleep_fn,
    )
    if not completed_ok or not executions_ok:
        latest_error = runtime.collector.latest_error()
        raise IbkrPositionReconciliationError(
            (
                f"IBKR execution/completed-order refresh did not complete: {latest_error['message']}"
                if latest_error is not None
                else f"IBKR execution/completed-order refresh did not complete within {config.timeout_seconds:.1f}s."
            )
        )
    matching_execution_rows = [
        row
        for row in _build_execution_rows(runtime.client.executions())
        if str(row.get("account_id") or "") == selected_account_id and _matches_exact_contract(row, exact_contract)
    ]
    matching_completed_order_rows = [
        row
        for row in _build_completed_order_rows(runtime.client.completed_orders())
        if str(row.get("account_id") or "") == selected_account_id and _matches_exact_contract(row, exact_contract)
    ]
    since = datetime.now(timezone.utc) - timedelta(minutes=int(config.recent_fill_lookback_minutes))
    recent_matching_execution_rows = [
        row
        for row in matching_execution_rows
        if _parse_datetime(row.get("executed_at")) is None or _parse_datetime(row.get("executed_at")) >= since
    ]
    return {
        "matching_execution_count": len(matching_execution_rows),
        "matching_completed_order_count": len(matching_completed_order_rows),
        "matching_execution_rows": matching_execution_rows,
        "matching_completed_order_rows": matching_completed_order_rows,
        "recent_matching_execution_rows": recent_matching_execution_rows,
    }


def _build_exact_position_rows(
    *,
    positions: tuple[IbkrPositionRecord, ...],
    selected_account_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in positions:
        if position.account_id != selected_account_id:
            continue
        rows.append(
            {
                "account_id": position.account_id,
                "con_id": position.contract.con_id,
                "symbol": position.contract.symbol,
                "local_symbol": position.contract.local_symbol,
                "security_type": position.contract.security_type,
                "exchange": position.contract.exchange,
                "currency": position.contract.currency,
                "expiry": position.contract.expiry,
                "multiplier": position.contract.multiplier,
                "trading_class": position.contract.trading_class,
                "quantity": _coerce_float(position.quantity),
                "average_cost": _coerce_float(position.average_cost),
                "market_price": _coerce_float(position.market_price),
                "market_value": _coerce_float(position.market_value),
                "updated_at": position.updated_at.isoformat() if position.updated_at is not None else None,
                "raw_payload": dict(position.raw_payload or {}),
            }
        )
    return rows


def _build_execution_rows(rows: tuple[Any, ...]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "account_id": row.account_id,
                "execution_id": row.execution_id,
                "broker_order_id": row.broker_order_id,
                "client_id": row.client_id,
                "perm_id": row.perm_id,
                "symbol": row.contract.symbol,
                "local_symbol": row.contract.local_symbol,
                "security_type": row.contract.security_type,
                "exchange": row.contract.exchange,
                "currency": row.contract.currency,
                "expiry": row.contract.expiry,
                "multiplier": row.contract.multiplier,
                "trading_class": row.contract.trading_class,
                "con_id": row.contract.con_id,
                "side": row.side,
                "quantity": _coerce_float(row.quantity),
                "price": _coerce_float(row.price),
                "executed_at": row.executed_at.isoformat() if row.executed_at is not None else None,
                "raw_payload": dict(row.raw_payload or {}),
            }
        )
    payload.sort(key=lambda item: str(item.get("executed_at") or ""))
    return payload


def _build_completed_order_rows(rows: tuple[Any, ...]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "account_id": row.account_id,
                "broker_order_id": row.broker_order_id,
                "client_id": row.client_id,
                "perm_id": row.perm_id,
                "symbol": row.contract.symbol,
                "local_symbol": row.contract.local_symbol,
                "security_type": row.contract.security_type,
                "exchange": row.contract.exchange,
                "currency": row.contract.currency,
                "expiry": row.contract.expiry,
                "multiplier": row.contract.multiplier,
                "trading_class": row.contract.trading_class,
                "con_id": row.contract.con_id,
                "status": row.status,
                "quantity": _coerce_float(row.quantity),
                "completed_at": row.completed_at.isoformat() if row.completed_at is not None else None,
                "raw_payload": dict(row.raw_payload or {}),
            }
        )
    payload.sort(key=lambda item: str(item.get("completed_at") or ""))
    return payload


def _build_portfolio_update_summary(
    *,
    collector: IbkrPositionReconciliationCollector,
    selected_account_id: str,
    exact_contract: IbkrQualifiedContract,
) -> dict[str, Any]:
    exact_rows = _matching_portfolio_update_rows(
        collector=collector,
        selected_account_id=selected_account_id,
        exact_contract=exact_contract,
    )
    return {
        "portfolio_callback_supported": True,
        "matching_row_count": len(exact_rows),
        "latest_matching_quantity": _latest_portfolio_quantity(exact_rows),
        "rows": exact_rows,
    }


def _matching_portfolio_update_rows(
    *,
    collector: IbkrPositionReconciliationCollector,
    selected_account_id: str,
    exact_contract: IbkrQualifiedContract,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in collector.portfolio_update_rows:
        contract = dict(row.get("contract") or {})
        candidate = {
            "account_id": row.get("account_id"),
            "con_id": contract.get("conId"),
            "symbol": contract.get("symbol"),
            "local_symbol": contract.get("localSymbol"),
            "security_type": contract.get("securityType"),
            "exchange": contract.get("exchange"),
            "currency": contract.get("currency"),
            "expiry": contract.get("lastTradeDateOrContractMonth"),
            "multiplier": contract.get("multiplier"),
            "trading_class": contract.get("tradingClass"),
            "quantity": row.get("quantity"),
            "market_price": row.get("market_price"),
            "market_value": row.get("market_value"),
            "average_cost": row.get("average_cost"),
            "unrealized_pnl": row.get("unrealized_pnl"),
            "realized_pnl": row.get("realized_pnl"),
            "updated_at": row.get("updated_at"),
        }
        if str(candidate.get("account_id") or "") != selected_account_id:
            continue
        if _matches_exact_contract(candidate, exact_contract):
            rows.append(candidate)
    rows.sort(key=lambda item: str(item.get("updated_at") or ""))
    return rows


def _matches_exact_contract(row: dict[str, Any], exact_contract: IbkrQualifiedContract) -> bool:
    row_con_id = _coerce_int(row.get("con_id"))
    row_symbol = str(row.get("symbol") or "").strip().upper()
    row_local_symbol = str(row.get("local_symbol") or "").strip().upper()
    row_expiry = str(row.get("expiry") or "").strip()
    if row_con_id is not None and exact_contract.con_id is not None and row_con_id != int(exact_contract.con_id):
        return False
    if row_symbol != str(exact_contract.broker_symbol or "").strip().upper():
        return False
    if row_local_symbol and row_local_symbol != str(exact_contract.local_symbol or "").strip().upper():
        return False
    if row_expiry and row_expiry != str(exact_contract.expiry or "").strip():
        return False
    return True


def _build_execution_position_match_rows(
    *,
    snapshots: list[dict[str, Any]],
    execution_rows: list[dict[str, Any]],
    completed_order_rows: list[dict[str, Any]],
    exact_contract: IbkrQualifiedContract,
    reference_fill_report: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    del exact_contract
    latest_execution = execution_rows[-1] if execution_rows else {}
    latest_completed = completed_order_rows[-1] if completed_order_rows else {}
    reference_order_id = _reference_submitted_order_id(reference_fill_report)
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        exact_rows = list(snapshot.get("exact_position_rows") or [])
        quantity = _latest_quantity(exact_rows)
        portfolio_quantity = snapshot.get("latest_portfolio_update_quantity")
        rows.append(
            {
                "snapshot_index": snapshot.get("snapshot_index"),
                "snapshot_generated_at": snapshot.get("generated_at"),
                "position_match_status": "exact_quantity_match"
                if quantity not in (None, 0.0)
                else ("exact_zero_quantity_row" if exact_rows else "no_exact_position_row"),
                "position_quantity": quantity,
                "portfolio_update_quantity": portfolio_quantity,
                "execution_id": latest_execution.get("execution_id"),
                "execution_side": latest_execution.get("side"),
                "execution_quantity": latest_execution.get("quantity"),
                "execution_price": latest_execution.get("price"),
                "execution_time": latest_execution.get("executed_at"),
                "completed_order_status": latest_completed.get("status"),
                "perm_id": latest_execution.get("perm_id") or latest_completed.get("perm_id"),
                "broker_order_id": latest_execution.get("broker_order_id") or latest_completed.get("broker_order_id") or reference_order_id,
                "note": "Snapshot sampled after confirmed paper fill.",
            }
        )
    return rows


def _diagnose_reconciliation(
    *,
    snapshots: list[dict[str, Any]],
    execution_truth: dict[str, Any],
    exact_contract: IbkrQualifiedContract,
    reference_fill_report: dict[str, Any] | None,
) -> dict[str, Any]:
    del exact_contract
    matching_executions = list(execution_truth.get("matching_execution_rows") or [])
    matching_completed_orders = list(execution_truth.get("matching_completed_order_rows") or [])
    latest_execution = matching_executions[-1] if matching_executions else {}
    latest_completed = matching_completed_orders[-1] if matching_completed_orders else {}
    exact_quantities = [snapshot.get("latest_exact_position_quantity") for snapshot in snapshots]
    non_null_quantities = [float(value) for value in exact_quantities if value is not None]
    latest_exact_quantity = non_null_quantities[-1] if non_null_quantities else None
    max_exact_quantity = max(non_null_quantities) if non_null_quantities else None
    prior_report_zero = _reference_report_showed_zero_position(reference_fill_report)
    latest_side = str(latest_execution.get("side") or "").strip().upper() or None
    latest_quantity = _coerce_float(latest_execution.get("quantity"))
    latest_execution_time = latest_execution.get("executed_at")
    latest_perm_id = latest_execution.get("perm_id") or latest_completed.get("perm_id")
    matching_sell = [
        row for row in matching_executions if str(row.get("side") or "").strip().upper() == "SLD"
    ]
    if max_exact_quantity is not None and max_exact_quantity > 0.0:
        likely_root_cause = "timing_or_request_sequencing_latency" if prior_report_zero else "position_reconciled"
        conclusion = (
            "The confirmed paper BUY execution now reconciles to a live long MGC position. "
            "The earlier 0.0 snapshot was most likely timing/latency or request sequencing rather than a contract-key mismatch."
            if prior_report_zero
            else "The confirmed paper BUY execution reconciles cleanly to the current long MGC position."
        )
        classification = "IBKR_POSITION_RECONCILED_LONG_MGC"
    elif matching_sell:
        classification = "IBKR_POSITION_RECONCILED_FLAT"
        likely_root_cause = "position_flat_after_sell_execution"
        conclusion = (
            "Current broker position truth is flat, and the matching execution history includes a SELL on the same exact MGC contract. "
            "That indicates the long was flattened after the confirmed BUY fill."
        )
    elif matching_executions:
        classification = "IBKR_POSITION_RECONCILIATION_PARTIAL"
        likely_root_cause = "execution_truth_without_position_confirmation"
        conclusion = (
            "Execution truth confirms the MGC fill, but repeated exact-contract position snapshots did not confirm the expected long position. "
            "That points to a remaining position refresh or reconciliation gap rather than an order-routing problem."
        )
    else:
        classification = "IBKR_POSITION_RECONCILIATION_PARTIAL"
        likely_root_cause = "no_matching_execution_rows"
        conclusion = (
            "The reconciliation pass did not capture matching MGC execution truth in the recent lookback window, so current position state cannot be tied back to the prior confirmed fill conclusively."
        )
    return {
        "classification": classification,
        "likely_root_cause": likely_root_cause,
        "conclusion": conclusion,
        "latest_exact_position_quantity": latest_exact_quantity,
        "max_exact_position_quantity": max_exact_quantity,
        "latest_matching_execution_side": latest_side,
        "latest_matching_execution_quantity": latest_quantity,
        "latest_matching_execution_time": latest_execution_time,
        "latest_matching_perm_id": latest_perm_id,
        "reference_fill_report_showed_zero_position": prior_report_zero,
    }


def _resolve_reference_fill_report_path(config: IbkrPositionReconciliationConfig) -> Path | None:
    if config.reference_fill_report_path is not None:
        return Path(config.reference_fill_report_path)
    candidate = Path(config.repo_root) / _DEFAULT_REFERENCE_FILL_REPORT
    return candidate if candidate.exists() else None


def _load_reference_fill_report(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _reference_report_showed_zero_position(reference_fill_report: dict[str, Any] | None) -> bool:
    if not reference_fill_report:
        return False
    lifecycle = dict(reference_fill_report.get("submit_cancel_lifecycle") or {})
    positions = dict((lifecycle.get("fill_verification") or {}).get("positions_after_submit") or {})
    for row in list(positions.get("positions") or []):
        if str(row.get("symbol") or "").strip().upper() != _EXPECTED_SYMBOL:
            continue
        if str(row.get("local_symbol") or "").strip().upper() != _EXPECTED_LOCAL_SYMBOL:
            continue
        if str(row.get("expiry") or "").strip() != _EXPECTED_EXACT_EXPIRY:
            continue
        if _coerce_float(row.get("quantity")) == 0.0:
            return True
    return False


def _reference_submitted_order_id(reference_fill_report: dict[str, Any] | None) -> int | None:
    if not reference_fill_report:
        return None
    lifecycle = dict(reference_fill_report.get("submit_cancel_lifecycle") or {})
    return _coerce_int(lifecycle.get("submitted_order_id"))


def _latest_quantity(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    return _coerce_float(rows[-1].get("quantity"))


def _latest_portfolio_quantity(rows: list[dict[str, Any]]) -> float | None:
    if not rows:
        return None
    return _coerce_float(rows[-1].get("quantity"))


def _coerce_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
