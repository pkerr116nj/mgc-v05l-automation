"""Supervised IBKR paper strategy executor for the first ATP lane."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
import time

from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
from .ibkr_paper_strategy_monitor import (
    IbkrPaperStrategyMonitorConfig,
    run_ibkr_paper_strategy_monitor,
    write_ibkr_paper_strategy_monitor_artifacts,
)
from .ibkr_paper_strategy_exposure import (
    IbkrPaperStrategyExposureConfig,
    run_ibkr_paper_strategy_exposure,
    write_ibkr_paper_strategy_exposure_artifacts,
)
from .ibkr_paper_strategy_governance import (
    IbkrPaperStrategyGovernanceConfig,
    run_ibkr_paper_strategy_governance,
    write_ibkr_paper_strategy_governance_artifacts,
)
from .ibkr_unattended_paper_close import (
    IbkrUnattendedPaperCloseConfig,
    run_ibkr_unattended_paper_close,
)

_EXPECTED_MODE = "PAPER"
_EXPECTED_HOST = "127.0.0.1"
_EXPECTED_PORT = 7497
_EXPECTED_ACCOUNT_ID = "DUM882026"
_EXPECTED_STRATEGY_ID = "ATP_COMPANION_V1_ASIA_US"
_EXPECTED_SYMBOL = "MGC"
_EXPECTED_CONTRACT_MONTH = "202606"
_EXPECTED_EXACT_EXPIRY = "20260626"
_EXPECTED_CON_ID = 712565978
_EXPECTED_LOCAL_SYMBOL = "MGCM6"
_EXPECTED_QUANTITY = 1.0
_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "ibkr_paper_strategy_executor"
_DEFAULT_LEDGER_PATH = Path("var") / "paper_strategy_position_ledger.json"
_DEFAULT_MONITOR_STATUS_PATH = Path("var") / "paper_strategy_monitor_runtime_status.json"
_DEFAULT_DASHBOARD_SNAPSHOT_PATH = Path("outputs") / "operator_dashboard" / "dashboard_api_snapshot.json"
_DEFAULT_DASHBOARD_FRESHNESS_SECONDS = 120.0
_DEFAULT_MONITOR_FRESHNESS_SECONDS = 45.0
_REPORT_BASENAME = "ibkr_paper_strategy_executor_report"
_AUDIT_BASENAME = "ibkr_paper_strategy_executor_audit.jsonl"
_STATUS_SUMMARY_BASENAME = "per_strategy_paper_status_summary.csv"
_LOOP_STATUS_BASENAME = "paper_strategy_executor_loop_status.json"
_LOOP_AUDIT_BASENAME = "paper_strategy_executor_loop_audit.jsonl"
_LOOP_REPORT_BASENAME = "paper_strategy_executor_loop_report.md"
_VAR_LOOP_STATUS_PATH = Path("var") / "paper_strategy_executor_loop_status.json"
_VAR_LOOP_AUDIT_PATH = Path("var") / "paper_strategy_executor_loop_audit.jsonl"


class IbkrPaperStrategyExecutorError(RuntimeError):
    """Raised when the supervised paper strategy executor must fail closed."""


@dataclass(frozen=True)
class IbkrPaperStrategyExecutorConfig:
    repo_root: Path
    mode: str
    host: str
    port: int
    client_id: int
    account_id: str = _EXPECTED_ACCOUNT_ID
    strategy_id: str = _EXPECTED_STRATEGY_ID
    symbol: str = _EXPECTED_SYMBOL
    contract_month: str = _EXPECTED_CONTRACT_MONTH
    exact_expiry: str = _EXPECTED_EXACT_EXPIRY
    con_id: int = _EXPECTED_CON_ID
    local_symbol: str = _EXPECTED_LOCAL_SYMBOL
    quantity: float = _EXPECTED_QUANTITY
    output_dir: Path = _DEFAULT_OUTPUT_DIR
    ledger_path: Path = _DEFAULT_LEDGER_PATH
    monitor_status_path: Path = _DEFAULT_MONITOR_STATUS_PATH
    dashboard_snapshot_path: Path = _DEFAULT_DASHBOARD_SNAPSHOT_PATH
    dashboard_freshness_seconds: float = _DEFAULT_DASHBOARD_FRESHNESS_SECONDS
    monitor_freshness_seconds: float = _DEFAULT_MONITOR_FRESHNESS_SECONDS
    supervised_submit_enabled: bool = True
    force_exit_long: bool = False
    allow_direct_reconciliation_close: bool = False


@dataclass(frozen=True)
class IbkrPaperStrategyExecutorArtifacts:
    classification: str
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification not in {
            "PAPER_STRATEGY_EXECUTOR_BLOCKED",
            "PAPER_STRATEGY_EXECUTOR_RECONCILIATION_FAILED",
        } else 1


@dataclass(frozen=True)
class IbkrPaperStrategyExecutorLoopConfig:
    executor_config: IbkrPaperStrategyExecutorConfig
    poll_interval_seconds: float = 45.0
    max_cycles: int = 0
    stop_on_blocked: bool = False
    loop_status_path: Path = _VAR_LOOP_STATUS_PATH
    loop_audit_path: Path = _VAR_LOOP_AUDIT_PATH


@dataclass(frozen=True)
class IbkrPaperStrategyExecutorLoopArtifacts:
    classification: str
    runtime_status: dict[str, Any]
    report: dict[str, Any]
    audit_events: list[dict[str, Any]]
    latest_cycle: IbkrPaperStrategyExecutorArtifacts | None

    @property
    def exit_code(self) -> int:
        return 0 if self.classification not in {"PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED"} else 1


def run_ibkr_paper_strategy_executor(
    *,
    config: IbkrPaperStrategyExecutorConfig,
    close_runner: Callable[..., Any] = run_ibkr_unattended_paper_close,
    monitor_refresh_runner: Callable[..., Any] = run_ibkr_paper_strategy_monitor,
) -> IbkrPaperStrategyExecutorArtifacts:
    _validate_environment_lock(config)
    audit_events: list[dict[str, Any]] = []
    started_at = _utc_now()
    _record_audit(
        audit_events,
        event_type="executor_started",
        detail="Supervised IBKR paper strategy executor started.",
        config=config,
    )

    monitor_status = load_paper_strategy_monitor_status(repo_root=config.repo_root)
    ledger = _load_json(config.repo_root / config.ledger_path)
    dashboard_snapshot = _load_json(config.repo_root / config.dashboard_snapshot_path)
    dashboard_gate = _extract_dashboard_gate(dashboard_snapshot)
    strategy_position = _load_strategy_position(ledger, config.strategy_id)
    runtime_wrapper_stale = _runtime_wrapper_stale(monitor_status)
    direct_monitor_status: dict[str, Any] | None = None
    preflight_note: str | None = None

    if (
        bool(config.force_exit_long)
        and bool(config.allow_direct_reconciliation_close)
        and _strategy_position_is_long(strategy_position)
        and runtime_wrapper_stale
    ):
        refreshed_monitor = monitor_refresh_runner(
            config=IbkrPaperStrategyMonitorConfig(
                repo_root=config.repo_root,
                mode=config.mode,
                host=config.host,
                port=config.port,
                client_id=int(config.client_id),
                account_id=config.account_id,
                strategy_id=config.strategy_id,
                symbol=config.symbol,
                contract_month=config.contract_month,
                exact_expiry=config.exact_expiry,
                con_id=int(config.con_id),
                local_symbol=config.local_symbol,
            )
        )
        write_ibkr_paper_strategy_monitor_artifacts(
            config=IbkrPaperStrategyMonitorConfig(
                repo_root=config.repo_root,
                mode=config.mode,
                host=config.host,
                port=config.port,
                client_id=int(config.client_id),
                account_id=config.account_id,
                strategy_id=config.strategy_id,
                symbol=config.symbol,
                contract_month=config.contract_month,
                exact_expiry=config.exact_expiry,
                con_id=int(config.con_id),
                local_symbol=config.local_symbol,
            ),
            artifacts=refreshed_monitor,
        )
        direct_monitor_status = dict(refreshed_monitor.status)
        monitor_status = _runtime_like_status_from_direct_monitor(direct_monitor_status)
        ledger = dict(refreshed_monitor.ledger)
        strategy_position = _load_strategy_position(ledger, config.strategy_id)
        dashboard_gate = _dashboard_gate_from_direct_monitor_status(direct_monitor_status, fallback=dashboard_gate)
        preflight_note = (
            "Monitor runtime wrapper was stale/disconnected, so this supervised close used a fresh direct broker/ledger reconciliation snapshot."
        )

    preflight_checks = _build_preflight_checks(
        config=config,
        monitor_status=monitor_status,
        dashboard_gate=dashboard_gate,
        strategy_position=strategy_position,
    )
    _record_audit(
        audit_events,
        event_type="preflight_evaluated",
        detail="Evaluated executor monitor, dashboard, and ledger gates.",
        config=config,
        extra={"preflight_checks": preflight_checks},
    )
    blocking_checks = [row for row in preflight_checks if row.get("blocking") and not row.get("passed")]
    if blocking_checks:
        detail = str(blocking_checks[0].get("detail") or "Supervised paper strategy executor preflight failed closed.")
        classification = "PAPER_STRATEGY_EXECUTOR_BLOCKED"
        decision = "BLOCKED_NEEDS_REVIEW"
        _record_audit(
            audit_events,
            event_type="executor_blocked",
            detail=detail,
            config=config,
            extra={"classification": classification, "decision": decision},
        )
        report = _build_report(
            config=config,
            classification=classification,
            decision=decision,
            decision_reason=detail,
        monitor_status=monitor_status,
        dashboard_gate=dashboard_gate,
        strategy_position=strategy_position,
        preflight_checks=preflight_checks,
        delegated_result=None,
        preflight_note=preflight_note,
        direct_monitor_status=direct_monitor_status,
    )
        return IbkrPaperStrategyExecutorArtifacts(classification=classification, report=report, audit_events=audit_events)

    decision, decision_reason = _derive_strategy_decision(
        config=config,
        dashboard_snapshot=dashboard_snapshot,
        dashboard_gate=dashboard_gate,
        strategy_position=strategy_position,
    )
    _record_audit(
        audit_events,
        event_type="decision_derived",
        detail="Derived the next supervised paper strategy action from current runtime state.",
        config=config,
        extra={"decision": decision, "decision_reason": decision_reason},
    )

    delegated_result: dict[str, Any] | None = None
    classification = "PAPER_STRATEGY_EXECUTOR_BLOCKED"
    if decision == "HOLD_LONG":
        classification = "PAPER_STRATEGY_EXECUTOR_HOLDING_LONG"
    elif decision == "NO_ACTION":
        classification = "PAPER_STRATEGY_EXECUTOR_NO_ACTION"
    elif decision == "BLOCKED_NEEDS_REVIEW":
        classification = "PAPER_STRATEGY_EXECUTOR_BLOCKED"
    elif decision == "EXIT_LONG":
        if not config.supervised_submit_enabled:
            classification = "PAPER_STRATEGY_EXECUTOR_BLOCKED"
            decision = "BLOCKED_NEEDS_REVIEW"
            decision_reason = "Supervised submit is disabled in executor configuration."
        else:
            exit_quantity = _resolved_exit_quantity(strategy_position=strategy_position, max_quantity=float(config.quantity))
            delegated_artifacts = close_runner(
                config=IbkrUnattendedPaperCloseConfig(
                    repo_root=config.repo_root,
                    mode=config.mode,
                    host=config.host,
                    port=config.port,
                    client_id=int(config.client_id),
                    unattended_paper=True,
                    account_id=config.account_id,
                    symbol=config.symbol,
                    contract_month=config.contract_month,
                    action="SELL",
                    quantity=float(exit_quantity),
                    exact_expiry=config.exact_expiry,
                    con_id=int(config.con_id),
                    local_symbol=config.local_symbol,
                    caller_path="ibkr_paper_strategy_executor",
                )
            )
            delegated_result = {
                "classification": delegated_artifacts.classification,
                "report": delegated_artifacts.report,
            }
            if delegated_artifacts.classification == "PAPER_CLOSE_FILLED_FLAT":
                post_close_monitor = monitor_refresh_runner(
                    config=IbkrPaperStrategyMonitorConfig(
                        repo_root=config.repo_root,
                        mode=config.mode,
                        host=config.host,
                        port=config.port,
                        client_id=int(config.client_id),
                        account_id=config.account_id,
                        strategy_id=config.strategy_id,
                        symbol=config.symbol,
                        contract_month=config.contract_month,
                        exact_expiry=config.exact_expiry,
                        con_id=int(config.con_id),
                        local_symbol=config.local_symbol,
                    )
                )
                write_ibkr_paper_strategy_monitor_artifacts(
                    config=IbkrPaperStrategyMonitorConfig(
                        repo_root=config.repo_root,
                        mode=config.mode,
                        host=config.host,
                        port=config.port,
                        client_id=int(config.client_id),
                        account_id=config.account_id,
                        strategy_id=config.strategy_id,
                        symbol=config.symbol,
                        contract_month=config.contract_month,
                        exact_expiry=config.exact_expiry,
                        con_id=int(config.con_id),
                        local_symbol=config.local_symbol,
                    ),
                    artifacts=post_close_monitor,
                )
                monitor_status = _runtime_like_status_from_direct_monitor(dict(post_close_monitor.status))
                direct_monitor_status = dict(post_close_monitor.status)
                ledger = dict(post_close_monitor.ledger)
                strategy_position = _load_strategy_position(ledger, config.strategy_id)
                dashboard_gate = _dashboard_gate_from_direct_monitor_status(dict(post_close_monitor.status), fallback=dashboard_gate)
                exposure_artifacts = run_ibkr_paper_strategy_exposure(
                    config=IbkrPaperStrategyExposureConfig(
                        repo_root=config.repo_root,
                        strategy_id=config.strategy_id,
                        bridge_strategy_id=config.strategy_id,
                    )
                )
                write_ibkr_paper_strategy_exposure_artifacts(
                    config=IbkrPaperStrategyExposureConfig(
                        repo_root=config.repo_root,
                        strategy_id=config.strategy_id,
                        bridge_strategy_id=config.strategy_id,
                    ),
                    artifacts=exposure_artifacts,
                )
                governance_artifacts = run_ibkr_paper_strategy_governance(
                    config=IbkrPaperStrategyGovernanceConfig(repo_root=config.repo_root)
                )
                write_ibkr_paper_strategy_governance_artifacts(
                    config=IbkrPaperStrategyGovernanceConfig(repo_root=config.repo_root),
                    artifacts=governance_artifacts,
                )
                classification = "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT"
                decision_reason = "Strategy exit intent was submitted through the supervised paper close path and reconciled flat."
            else:
                classification = "PAPER_STRATEGY_EXECUTOR_RECONCILIATION_FAILED"
                decision_reason = str(
                    delegated_artifacts.report.get("detail")
                    or delegated_artifacts.report.get("summary")
                    or "Supervised exit did not reconcile cleanly."
                )
            _record_audit(
                audit_events,
                event_type="delegated_close_completed",
                detail="Delegated the strategy exit to the exact-contract unattended paper close path.",
                config=config,
                extra={
                    "delegated_classification": delegated_artifacts.classification,
                    "delegated_summary": delegated_artifacts.report.get("summary"),
                },
            )
    else:
        classification = "PAPER_STRATEGY_EXECUTOR_BLOCKED"
        decision = "BLOCKED_NEEDS_REVIEW"
        decision_reason = f"Unhandled executor decision state: {decision}"

    report = _build_report(
        config=config,
        classification=classification,
        decision=decision,
        decision_reason=decision_reason,
        monitor_status=monitor_status,
        dashboard_gate=dashboard_gate,
        strategy_position=strategy_position,
        preflight_checks=preflight_checks,
        delegated_result=delegated_result,
        preflight_note=preflight_note,
        direct_monitor_status=direct_monitor_status,
    )
    return IbkrPaperStrategyExecutorArtifacts(classification=classification, report=report, audit_events=audit_events)


def write_ibkr_paper_strategy_executor_artifacts(
    *,
    config: IbkrPaperStrategyExecutorConfig,
    artifacts: IbkrPaperStrategyExecutorArtifacts,
) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{_REPORT_BASENAME}.json"
    report_path.write_text(json.dumps(artifacts.report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = output_dir / f"{_REPORT_BASENAME}.md"
    markdown_path.write_text(render_ibkr_paper_strategy_executor_markdown(artifacts.report) + "\n", encoding="utf-8")
    audit_path = output_dir / _AUDIT_BASENAME
    with audit_path.open("w", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    _write_status_summary_csv(output_dir / _STATUS_SUMMARY_BASENAME, report=artifacts.report)


def run_ibkr_paper_strategy_executor_loop(
    *,
    config: IbkrPaperStrategyExecutorLoopConfig,
    close_runner: Callable[..., Any] = run_ibkr_unattended_paper_close,
    sleep_fn: Callable[[float], None] = time.sleep,
    should_stop: Callable[[], bool] | None = None,
) -> IbkrPaperStrategyExecutorLoopArtifacts:
    audit_events: list[dict[str, Any]] = []
    cycles: list[dict[str, Any]] = []
    latest_cycle: IbkrPaperStrategyExecutorArtifacts | None = None
    stop_requested = should_stop or (lambda: False)
    started_at = _utc_now()
    _record_loop_audit(
        audit_events,
        event_type="loop_started",
        detail="Supervised IBKR paper strategy executor loop started.",
        config=config,
    )
    cycle_index = 0
    while True:
        if stop_requested():
            classification = "PAPER_STRATEGY_EXECUTOR_LOOP_STOPPED"
            reason = "Stop requested before the next supervised executor cycle began."
            break
        if config.max_cycles > 0 and cycle_index >= int(config.max_cycles):
            classification = _final_loop_classification(latest_cycle=latest_cycle)
            reason = f"Reached configured max cycles ({config.max_cycles})."
            break
        cycle_index += 1
        cycle_started_at = _utc_now()
        latest_cycle = run_ibkr_paper_strategy_executor(config=config.executor_config, close_runner=close_runner)
        cycle_row = {
            "cycle_index": cycle_index,
            "started_at": cycle_started_at,
            "finished_at": _utc_now(),
            "classification": latest_cycle.classification,
            "decision": latest_cycle.report.get("decision"),
            "decision_reason": latest_cycle.report.get("decision_reason"),
            "position_quantity": dict(latest_cycle.report.get("strategy_position") or {}).get("quantity"),
            "open_order_count": dict(latest_cycle.report.get("paper_strategy_monitor_status") or {}).get("open_order_count"),
        }
        cycles.append(cycle_row)
        _record_loop_audit(
            audit_events,
            event_type="cycle_completed",
            detail="Completed one supervised paper strategy executor cycle.",
            config=config,
            extra=cycle_row,
        )
        runtime_status = _build_loop_runtime_status(
            config=config,
            classification=_loop_runtime_classification_from_cycle(latest_cycle=latest_cycle),
            latest_cycle=latest_cycle,
            cycles=cycles,
            loop_running=True,
        )
        _write_loop_runtime_files(config=config, runtime_status=runtime_status, audit_events=audit_events)
        if latest_cycle.classification == "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT":
            classification = "PAPER_STRATEGY_EXECUTOR_LOOP_EXITED_FLAT"
            reason = "Strategy executor produced EXIT_LONG and reconciled the paper position flat."
            break
        if latest_cycle.classification == "PAPER_STRATEGY_EXECUTOR_BLOCKED" and config.stop_on_blocked:
            classification = "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED"
            reason = "Loop is configured to stop immediately when a blocking supervised executor state is encountered."
            break
        if stop_requested():
            classification = "PAPER_STRATEGY_EXECUTOR_LOOP_STOPPED"
            reason = "Stop requested after the latest supervised executor cycle completed."
            break
        sleep_fn(float(config.poll_interval_seconds))

    runtime_status = _build_loop_runtime_status(
        config=config,
        classification=classification,
        latest_cycle=latest_cycle,
        cycles=cycles,
        stop_reason=reason,
        loop_running=False,
    )
    report = {
        "generated_at": _utc_now(),
        "classification": classification,
        "started_at": started_at,
        "finished_at": _utc_now(),
        "stop_reason": reason,
        "strategy_id": config.executor_config.strategy_id,
        "account_id": config.executor_config.account_id,
        "mode": config.executor_config.mode,
        "host": config.executor_config.host,
        "port": config.executor_config.port,
        "poll_interval_seconds": config.poll_interval_seconds,
        "max_cycles": config.max_cycles,
        "cycles_completed": len(cycles),
        "latest_cycle_classification": None if latest_cycle is None else latest_cycle.classification,
        "latest_cycle_decision": None if latest_cycle is None else latest_cycle.report.get("decision"),
        "latest_cycle_report": None if latest_cycle is None else latest_cycle.report,
        "runtime_status": runtime_status,
    }
    _record_loop_audit(
        audit_events,
        event_type="loop_finished",
        detail="Supervised IBKR paper strategy executor loop finished.",
        config=config,
        extra={"classification": classification, "stop_reason": reason, "cycles_completed": len(cycles)},
    )
    _write_loop_runtime_files(config=config, runtime_status=runtime_status, audit_events=audit_events)
    return IbkrPaperStrategyExecutorLoopArtifacts(
        classification=classification,
        runtime_status=runtime_status,
        report=report,
        audit_events=audit_events,
        latest_cycle=latest_cycle,
    )


def write_ibkr_paper_strategy_executor_loop_artifacts(
    *,
    config: IbkrPaperStrategyExecutorLoopConfig,
    artifacts: IbkrPaperStrategyExecutorLoopArtifacts,
) -> None:
    output_dir = config.executor_config.repo_root / config.executor_config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / _LOOP_STATUS_BASENAME).write_text(
        json.dumps(artifacts.runtime_status, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / _LOOP_REPORT_BASENAME).write_text(
        render_ibkr_paper_strategy_executor_loop_markdown(artifacts.report) + "\n",
        encoding="utf-8",
    )
    with (output_dir / _LOOP_AUDIT_BASENAME).open("w", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True) + "\n")
    if artifacts.latest_cycle is not None:
        _write_status_summary_csv(output_dir / _STATUS_SUMMARY_BASENAME, report=artifacts.latest_cycle.report)


def render_ibkr_paper_strategy_executor_markdown(report: dict[str, Any]) -> str:
    monitor = dict(report.get("paper_strategy_monitor_status") or {})
    position = dict(report.get("strategy_position") or {})
    dashboard = dict(report.get("dashboard_gate") or {})
    lines = [
        "# IBKR Paper Strategy Executor",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- strategy: `{report.get('strategy_id')}`",
        f"- decision: `{report.get('decision')}`",
        f"- decision reason: `{report.get('decision_reason')}`",
        f"- account: `{report.get('account_id')}`",
        f"- environment: `{report.get('mode')} / {report.get('host')} / {report.get('port')}`",
        f"- exact contract: `MGC {dict(report.get('exact_contract') or {}).get('expiry')}` / `conId={dict(report.get('exact_contract') or {}).get('con_id')}` / `localSymbol={dict(report.get('exact_contract') or {}).get('local_symbol')}`",
        f"- current quantity: `{position.get('quantity')}`",
        f"- side: `{position.get('side')}`",
        f"- average entry price: `{position.get('average_entry_price')}`",
        f"- unrealized P&L: `{monitor.get('unrealized_pnl')}`",
        f"- realized P&L: `{monitor.get('realized_pnl')}`",
        f"- open orders: `{monitor.get('open_order_count')}`",
        f"- monitor classification: `{monitor.get('classification')}`",
        f"- monitor health: `{monitor.get('health_classification') or monitor.get('monitor_health')}`",
        f"- monitor stale: `{monitor.get('stale')}`",
        f"- dashboard source mode: `{dashboard.get('source_mode')}`",
        f"- supervised launch allowed: `{dashboard.get('launch_allowed')}`",
        f"- session classification: `{dashboard.get('session_classification')}`",
        "- scope: `single-lane supervised paper executor for ATP_COMPANION_V1_ASIA_US only`",
        "- execution mode: `exit-only while the owned strategy position is long; flat state is monitor-only until entry support is implemented later`",
        "- generalization: `decisioning and exit sizing are derived from current ledger plus broker truth, not from one hard-coded order/permId/execution history`",
    ]
    delegated = dict(report.get("delegated_result") or {})
    if delegated:
        lines.append(f"- delegated classification: `{delegated.get('classification')}`")
    for row in list(report.get("preflight_checks") or []):
        if not row.get("passed"):
            lines.append(f"- blocked check: `{row.get('name')}` -> `{row.get('detail')}`")
    return "\n".join(lines)


def render_ibkr_paper_strategy_executor_loop_markdown(report: dict[str, Any]) -> str:
    runtime = dict(report.get("runtime_status") or {})
    latest = dict(report.get("latest_cycle_report") or {})
    lines = [
        "# IBKR Paper Strategy Executor Loop",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- strategy: `{report.get('strategy_id')}`",
        f"- account: `{report.get('account_id')}`",
        f"- environment: `{report.get('mode')} / {report.get('host')} / {report.get('port')}`",
        f"- poll interval seconds: `{report.get('poll_interval_seconds')}`",
        f"- max cycles: `{report.get('max_cycles')}`",
        f"- cycles completed: `{report.get('cycles_completed')}`",
        f"- stop reason: `{report.get('stop_reason')}`",
        f"- latest cycle classification: `{report.get('latest_cycle_classification')}`",
        f"- latest cycle decision: `{report.get('latest_cycle_decision')}`",
        f"- loop running: `{runtime.get('loop_running')}`",
        f"- monitor health: `{dict(runtime.get('paper_strategy_monitor_status') or {}).get('health_classification')}`",
        f"- stale: `{dict(runtime.get('paper_strategy_monitor_status') or {}).get('stale')}`",
        f"- open orders: `{dict(runtime.get('paper_strategy_monitor_status') or {}).get('open_order_count')}`",
        f"- current quantity: `{dict(runtime.get('strategy_position') or {}).get('quantity')}`",
        f"- current side: `{dict(runtime.get('strategy_position') or {}).get('side')}`",
        "- scope: `single-lane supervised paper loop`",
        "- execution mode: `exit-only for the currently owned strategy position`",
        "- engine boundary: `not a full entry/exit autonomous strategy engine yet`",
        "- state model: `generalizes from reconciled ledger/broker state rather than from one specific adopted trade`",
    ]
    if latest:
        lines.append(f"- latest decision reason: `{latest.get('decision_reason')}`")
    return "\n".join(lines)


def _validate_environment_lock(config: IbkrPaperStrategyExecutorConfig) -> None:
    if str(config.mode or "").strip().upper() != _EXPECTED_MODE:
        raise IbkrPaperStrategyExecutorError("Supervised paper strategy executor is PAPER-only.")
    if str(config.host or "").strip() != _EXPECTED_HOST or int(config.port) != _EXPECTED_PORT:
        raise IbkrPaperStrategyExecutorError("Supervised paper strategy executor requires 127.0.0.1:7497.")
    if str(config.account_id or "").strip() != _EXPECTED_ACCOUNT_ID:
        raise IbkrPaperStrategyExecutorError("Supervised paper strategy executor requires account DUM882026.")
    if str(config.strategy_id or "").strip() != _EXPECTED_STRATEGY_ID:
        raise IbkrPaperStrategyExecutorError("Only ATP_COMPANION_V1_ASIA_US is enabled in the first supervised paper executor lane.")
    if str(config.symbol or "").strip().upper() != _EXPECTED_SYMBOL:
        raise IbkrPaperStrategyExecutorError("Only MGC is enabled in the first supervised paper executor lane.")


def _build_preflight_checks(
    *,
    config: IbkrPaperStrategyExecutorConfig,
    monitor_status: dict[str, Any],
    dashboard_gate: dict[str, Any],
    strategy_position: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    monitor_age = _safe_float(monitor_status.get("age_seconds"))
    monitor_health = str(monitor_status.get("health_classification") or monitor_status.get("monitor_health") or "").strip().upper()
    checks = [
        _check(
            "paper_environment_lock",
            str(config.mode).upper() == _EXPECTED_MODE and config.host == _EXPECTED_HOST and int(config.port) == _EXPECTED_PORT,
            True,
            "Supervised executor is locked to PAPER / 127.0.0.1 / 7497.",
        ),
        _check(
            "monitor_runtime_present",
            bool(monitor_status),
            True,
            "Live paper monitor runtime status must exist before supervised paper execution.",
        ),
        _check(
            "monitor_running",
            bool(monitor_status.get("monitor_running")),
            True,
            "Live paper monitor must be actively running.",
        ),
        _check(
            "monitor_fresh",
            monitor_age is not None and monitor_age <= float(config.monitor_freshness_seconds) and not bool(monitor_status.get("stale")),
            True,
            f"Monitor runtime status must be fresh within {config.monitor_freshness_seconds:.0f}s and not stale.",
        ),
        _check(
            "monitor_health",
            monitor_health == "HEALTHY" and str(monitor_status.get("classification") or "").strip().upper() == "PAPER_STRATEGY_MONITOR_ACTIVE",
            True,
            "Monitor health must be HEALTHY and classification must remain PAPER_STRATEGY_MONITOR_ACTIVE.",
        ),
        _check(
            "dashboard_live_ready",
            bool(dashboard_gate.get("backend_healthy")) and bool(dashboard_gate.get("live_source_ready")) and bool(dashboard_gate.get("launch_allowed")),
            True,
            "Dashboard/backend state must be live, attached, and launch-allowed for supervised paper execution.",
        ),
        _check(
            "strategy_position_owned",
            strategy_position is not None and str(strategy_position.get("strategy_id") or "") == config.strategy_id,
            True,
            "Strategy ledger must own the current MGC paper position before supervised decisioning.",
        ),
        _check(
            "position_side_long_or_flat",
            strategy_position is None or str(strategy_position.get("side") or "").upper() in {"LONG", "FLAT"},
            True,
            "First supervised lane only supports FLAT or LONG paper state; direct short or flip states are blocked.",
        ),
        _check(
            "no_conflicting_open_order",
            int(monitor_status.get("open_order_count") or 0) == 0,
            True,
            "Supervised executor requires zero working MGC orders before any new decision.",
        ),
    ]
    return checks


def _derive_strategy_decision(
    *,
    config: IbkrPaperStrategyExecutorConfig,
    dashboard_snapshot: dict[str, Any],
    dashboard_gate: dict[str, Any],
    strategy_position: dict[str, Any] | None,
) -> tuple[str, str]:
    hint = _extract_strategy_decision_hint(dashboard_snapshot=dashboard_snapshot, strategy_id=config.strategy_id)
    if hint == "EXIT_LONG":
        return "EXIT_LONG", "A strategy-level exit hint was surfaced for ATP_COMPANION_V1_ASIA_US."
    quantity = 0.0 if strategy_position is None else float(strategy_position.get("quantity") or 0.0)
    side = "" if strategy_position is None else str(strategy_position.get("side") or "").strip().upper()
    if quantity >= 1.0 and side == "LONG":
        if bool(config.force_exit_long):
            return (
                "EXIT_LONG",
                "Operator forced one supervised ATP sell-to-close from the reconciled ledger state.",
            )
        session = dashboard_gate.get("session_classification")
        return (
            "HOLD_LONG",
            "The strategy currently owns long 1.0 MGC and no authoritative exit signal is surfaced in the live monitored state, so the supervised executor continues holding the paper position."
            + (f" Current detected session is {session}." if session else ""),
        )
    if quantity == 0.0:
        return "NO_ACTION", "The strategy is flat and this first supervised executor pass is not allowed to open a new paper position autonomously."
    return "BLOCKED_NEEDS_REVIEW", "The supervised executor could not map the current ledger position into a safe FLAT/LONG state."


def _resolved_exit_quantity(*, strategy_position: dict[str, Any] | None, max_quantity: float) -> float:
    if strategy_position is None:
        raise IbkrPaperStrategyExecutorError("EXIT_LONG requires a current reconciled strategy position.")
    quantity = float(strategy_position.get("quantity") or 0.0)
    if quantity <= 0.0:
        raise IbkrPaperStrategyExecutorError("EXIT_LONG requires a positive reconciled strategy quantity.")
    if quantity > float(max_quantity):
        raise IbkrPaperStrategyExecutorError(
            f"EXIT_LONG requires reconciled strategy quantity <= configured max quantity ({max_quantity})."
        )
    return quantity


def _strategy_position_is_long(strategy_position: dict[str, Any] | None) -> bool:
    if strategy_position is None:
        return False
    return float(strategy_position.get("quantity") or 0.0) > 0.0 and str(strategy_position.get("side") or "").strip().upper() == "LONG"


def _runtime_wrapper_stale(monitor_status: dict[str, Any]) -> bool:
    return (
        not bool(monitor_status.get("monitor_running"))
        or str(monitor_status.get("health_classification") or monitor_status.get("monitor_health") or "").strip().upper() == "DISCONNECTED"
        or "monitor_disconnected" in list(monitor_status.get("block_reasons") or [])
    )


def _runtime_like_status_from_direct_monitor(status: dict[str, Any]) -> dict[str, Any]:
    backend_gate = dict(status.get("backend_gate") or {})
    block_reasons = list(status.get("block_reasons") or [])
    synthetic_block_reasons = [reason for reason in block_reasons if reason != "paper_strategy_monitor_not_running"]
    return {
        "classification": "PAPER_STRATEGY_MONITOR_ACTIVE" if status.get("ownership_proven") else status.get("classification"),
        "monitor_running": True,
        "health_classification": "HEALTHY" if status.get("ownership_proven") and not synthetic_block_reasons else "DEGRADED",
        "stale": False,
        "submit_allowed": bool(status.get("ownership_proven")) and not synthetic_block_reasons,
        "strategy_id": status.get("strategy_id"),
        "account_id": status.get("account_id"),
        "exact_contract": status.get("exact_contract"),
        "broker_position_quantity": status.get("broker_position_quantity"),
        "ledger_position_quantity": status.get("ledger_position_quantity"),
        "average_entry_price": status.get("average_entry_price"),
        "unrealized_pnl": status.get("unrealized_pnl"),
        "realized_pnl": status.get("realized_pnl"),
        "open_order_count": status.get("open_order_count"),
        "age_seconds": 0.0,
        "block_reasons": synthetic_block_reasons,
        "backend_gate": backend_gate,
        "detail": status.get("detail"),
    }


def _dashboard_gate_from_direct_monitor_status(status: dict[str, Any], *, fallback: dict[str, Any]) -> dict[str, Any]:
    backend_gate = dict(status.get("backend_gate") or {})
    if not backend_gate:
        return fallback
    return {
        "backend_healthy": bool(backend_gate.get("backend_healthy")),
        "live_source_ready": bool(backend_gate.get("live_source_ready")),
        "launch_allowed": bool(backend_gate.get("launch_allowed")),
        "source_mode": "LIVE_API" if bool(backend_gate.get("live_source_ready")) else "SNAPSHOT_FALLBACK",
        "session_classification": backend_gate.get("session_classification"),
        "paper_runtime_stale": bool(backend_gate.get("paper_runtime_stale")),
        "generated_at": status.get("generated_at"),
        "age_seconds": 0.0,
    }


def _extract_strategy_decision_hint(*, dashboard_snapshot: dict[str, Any], strategy_id: str) -> str | None:
    paper = dict(dashboard_snapshot.get("paper") or {})
    tracked = dict(paper.get("tracked_strategies") or {})
    details = dict(tracked.get("details_by_strategy_id") or {})
    detail = dict(details.get(strategy_id.lower()) or details.get(strategy_id) or {})
    for container in (
        detail,
        dict(detail.get("runtime_state") or {}),
        dict(detail.get("gating_state") or {}),
        dict(detail.get("latest_signal") or {}),
        dict(detail.get("latest_order_intent") or {}),
    ):
        hint = str(container.get("executor_decision") or container.get("decision") or "").strip().upper()
        if hint in {"EXIT_LONG", "HOLD_LONG", "NO_ACTION", "BLOCKED_NEEDS_REVIEW"}:
            return hint
    return None


def _load_strategy_position(ledger: dict[str, Any], strategy_id: str) -> dict[str, Any] | None:
    for row in list(ledger.get("positions") or []):
        if str(row.get("strategy_id") or "") == strategy_id:
            return dict(row)
    return None


def _extract_dashboard_gate(payload: dict[str, Any]) -> dict[str, Any]:
    dashboard_meta = dict(payload.get("dashboard_meta") or {})
    supervised = dict(payload.get("supervised_paper_operability") or {})
    startup = dict(payload.get("startup_control_plane") or {})
    paper = dict(payload.get("paper") or {})
    paper_status = dict(paper.get("status") or {})
    readiness = dict(paper.get("readiness") or {})
    generated_at = (
        payload.get("generated_at")
        or supervised.get("generated_at")
        or dict(startup.get("convergence") or {}).get("generated_at")
        or paper.get("generated_at")
    )
    generated_dt = _parse_datetime(generated_at)
    age_seconds = None
    if generated_dt is not None:
        age_seconds = max((_parse_datetime(_utc_now()) - generated_dt).total_seconds(), 0.0)
    return {
        "backend_healthy": not bool(dashboard_meta.get("degraded")) and bool(supervised.get("dashboard_attached")),
        "live_source_ready": bool(supervised.get("dashboard_attached")) and str(startup.get("overall_state") or "").upper() == "READY",
        "launch_allowed": bool(supervised.get("launch_allowed")),
        "source_mode": "LIVE_API" if bool(supervised.get("dashboard_attached")) else "SNAPSHOT_FALLBACK",
        "session_classification": readiness.get("current_detected_session"),
        "paper_runtime_stale": bool(paper_status.get("stale")),
        "generated_at": generated_at,
        "age_seconds": age_seconds,
    }


def _build_report(
    *,
    config: IbkrPaperStrategyExecutorConfig,
    classification: str,
    decision: str,
    decision_reason: str,
    monitor_status: dict[str, Any],
    dashboard_gate: dict[str, Any],
    strategy_position: dict[str, Any] | None,
    preflight_checks: list[dict[str, Any]],
    delegated_result: dict[str, Any] | None,
    preflight_note: str | None,
    direct_monitor_status: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "generated_at": _utc_now(),
        "classification": classification,
        "decision": decision,
        "decision_reason": decision_reason,
        "strategy_id": config.strategy_id,
        "account_id": config.account_id,
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "client_id": config.client_id,
        "exact_contract": {
            "symbol": config.symbol,
            "expiry": config.exact_expiry,
            "con_id": config.con_id,
            "local_symbol": config.local_symbol,
        },
        "scope": {
            "single_lane": True,
            "strategy_scope": [config.strategy_id],
            "contract_scope": [f"{config.symbol} {config.exact_expiry} / conId={config.con_id} / localSymbol={config.local_symbol}"],
            "paper_only": True,
            "exit_only_while_long": True,
            "entry_support_implemented": False,
            "generalized_from_ledger_broker_state": True,
        },
        "paper_strategy_monitor_status": monitor_status,
        "direct_monitor_reconciliation_status": direct_monitor_status,
        "dashboard_gate": dashboard_gate,
        "strategy_position": strategy_position,
        "preflight_checks": preflight_checks,
        "delegated_result": delegated_result,
        "preflight_note": preflight_note,
    }


def _build_loop_runtime_status(
    *,
    config: IbkrPaperStrategyExecutorLoopConfig,
    classification: str,
    latest_cycle: IbkrPaperStrategyExecutorArtifacts | None,
    cycles: list[dict[str, Any]],
    stop_reason: str | None = None,
    loop_running: bool,
) -> dict[str, Any]:
    latest_report = {} if latest_cycle is None else dict(latest_cycle.report)
    monitor = dict(latest_report.get("paper_strategy_monitor_status") or {})
    position = dict(latest_report.get("strategy_position") or {})
    return {
        "generated_at": _utc_now(),
        "classification": classification,
        "loop_running": bool(loop_running),
        "strategy_id": config.executor_config.strategy_id,
        "account_id": config.executor_config.account_id,
        "mode": config.executor_config.mode,
        "host": config.executor_config.host,
        "port": config.executor_config.port,
        "poll_interval_seconds": float(config.poll_interval_seconds),
        "max_cycles": int(config.max_cycles),
        "cycles_completed": len(cycles),
        "last_cycle": None if not cycles else cycles[-1],
        "paper_strategy_monitor_status": monitor,
        "strategy_position": position,
        "last_executor_classification": None if latest_cycle is None else latest_cycle.classification,
        "last_executor_decision": latest_report.get("decision"),
        "last_executor_decision_reason": latest_report.get("decision_reason"),
        "stop_reason": stop_reason,
        "last_refresh_timestamp": _utc_now(),
    }


def _write_loop_runtime_files(
    *,
    config: IbkrPaperStrategyExecutorLoopConfig,
    runtime_status: dict[str, Any],
    audit_events: list[dict[str, Any]],
) -> None:
    status_path = config.executor_config.repo_root / config.loop_status_path
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(json.dumps(runtime_status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    audit_path = config.executor_config.repo_root / config.loop_audit_path
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("w", encoding="utf-8") as handle:
        for row in audit_events:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _loop_runtime_classification_from_cycle(*, latest_cycle: IbkrPaperStrategyExecutorArtifacts | None) -> str:
    if latest_cycle is None:
        return "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED"
    mapping = {
        "PAPER_STRATEGY_EXECUTOR_HOLDING_LONG": "PAPER_STRATEGY_EXECUTOR_LOOP_HOLDING",
        "PAPER_STRATEGY_EXECUTOR_NO_ACTION": "PAPER_STRATEGY_EXECUTOR_LOOP_ACTIVE",
        "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT": "PAPER_STRATEGY_EXECUTOR_LOOP_EXITED_FLAT",
        "PAPER_STRATEGY_EXECUTOR_BLOCKED": "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED",
        "PAPER_STRATEGY_EXECUTOR_RECONCILIATION_FAILED": "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED",
    }
    return mapping.get(latest_cycle.classification, "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED")


def _final_loop_classification(*, latest_cycle: IbkrPaperStrategyExecutorArtifacts | None) -> str:
    if latest_cycle is None:
        return "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED"
    if latest_cycle.classification == "PAPER_STRATEGY_EXECUTOR_EXIT_FILLED_FLAT":
        return "PAPER_STRATEGY_EXECUTOR_LOOP_EXITED_FLAT"
    if latest_cycle.classification == "PAPER_STRATEGY_EXECUTOR_HOLDING_LONG":
        return "PAPER_STRATEGY_EXECUTOR_LOOP_HOLDING"
    if latest_cycle.classification == "PAPER_STRATEGY_EXECUTOR_NO_ACTION":
        return "PAPER_STRATEGY_EXECUTOR_LOOP_ACTIVE"
    return "PAPER_STRATEGY_EXECUTOR_LOOP_BLOCKED"


def _write_status_summary_csv(path: Path, *, report: dict[str, Any]) -> None:
    row = {
        "generated_at": report.get("generated_at"),
        "strategy_id": report.get("strategy_id"),
        "classification": report.get("classification"),
        "decision": report.get("decision"),
        "account_id": report.get("account_id"),
        "symbol": dict(report.get("exact_contract") or {}).get("symbol"),
        "expiry": dict(report.get("exact_contract") or {}).get("expiry"),
        "quantity": dict(report.get("strategy_position") or {}).get("quantity"),
        "side": dict(report.get("strategy_position") or {}).get("side"),
        "average_entry_price": dict(report.get("strategy_position") or {}).get("average_entry_price"),
        "unrealized_pnl": dict(report.get("paper_strategy_monitor_status") or {}).get("unrealized_pnl"),
        "realized_pnl": dict(report.get("paper_strategy_monitor_status") or {}).get("realized_pnl"),
        "monitor_health": dict(report.get("paper_strategy_monitor_status") or {}).get("health_classification"),
    }
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)


def _record_audit(
    audit_events: list[dict[str, Any]],
    *,
    event_type: str,
    detail: str,
    config: IbkrPaperStrategyExecutorConfig,
    extra: dict[str, Any] | None = None,
) -> None:
    audit_events.append(
        {
            "event_type": event_type,
            "observed_at": _utc_now(),
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
            "client_id": config.client_id,
            "detail": detail,
            **dict(extra or {}),
        }
    )


def _record_loop_audit(
    audit_events: list[dict[str, Any]],
    *,
    event_type: str,
    detail: str,
    config: IbkrPaperStrategyExecutorLoopConfig,
    extra: dict[str, Any] | None = None,
) -> None:
    audit_events.append(
        {
            "event_type": event_type,
            "observed_at": _utc_now(),
            "mode": config.executor_config.mode,
            "host": config.executor_config.host,
            "port": config.executor_config.port,
            "client_id": config.executor_config.client_id,
            "detail": detail,
            **dict(extra or {}),
        }
    )


def _check(name: str, passed: bool, blocking: bool, detail: str) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed), "blocking": bool(blocking), "detail": detail}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
