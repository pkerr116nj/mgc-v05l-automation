"""Persistent IBKR paper strategy position and P&L monitor."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .ibkr_position_reconciliation import (
    IbkrPositionReconciliationConfig,
    run_ibkr_position_reconciliation,
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
_DEFAULT_DASHBOARD_URL = "http://127.0.0.1:8790/api/dashboard"
_DEFAULT_LEDGER_PATH = Path("var") / "paper_strategy_position_ledger.json"
_DEFAULT_OUTPUT_DIR = Path("outputs") / "reports" / "paper_strategy_monitor"
_DEFAULT_VAR_RUNTIME_STATUS_PATH = Path("var") / "paper_strategy_monitor_runtime_status.json"
_DEFAULT_VAR_PNL_SNAPSHOT_PATH = Path("var") / "paper_strategy_pnl_snapshot.json"
_DEFAULT_VAR_HEARTBEAT_PATH = Path("var") / "paper_strategy_monitor_heartbeat.json"
_DEFAULT_VAR_AUDIT_PATH = Path("var") / "paper_strategy_monitor_audit.jsonl"
_DEFAULT_BRIDGE_REPORT_PATH = Path("outputs") / "reports" / "ibkr_paper_strategy_bridge" / "ibkr_paper_strategy_bridge_report.json"
_DEFAULT_PREPARED_BUNDLE_PATH = (
    Path("outputs")
    / "reports"
    / "ibkr_paper_strategy_bridge"
    / "prepared_manual_harness"
    / "ibkr_manual_paper_fill_test_frozen_preview.json"
)
_DEFAULT_STRATEGY_TRACKING_SNAPSHOT_PATH = (
    Path("outputs") / "reports" / "ibkr_paper_strategy_tracking_snapshot" / "strategy_position_snapshot.json"
)
_DEFAULT_EXECUTOR_REPORT_PATH = (
    Path("outputs") / "reports" / "ibkr_paper_strategy_executor" / "ibkr_paper_strategy_executor_report.json"
)
_DEFAULT_EXECUTOR_LOOP_STATUS_PATH = Path("var") / "paper_strategy_executor_loop_status.json"
_DEFAULT_MONITOR_SERVICE_PID_PATH = Path("var") / "paper_strategy_monitor_service.pid"
_RUNTIME_STATUS_FILENAME = "paper_strategy_monitor_runtime_status.json"
_DAEMON_REPORT_FILENAME = "paper_strategy_monitor_daemon_report.json"
_RUNTIME_AUDIT_FILENAME = "paper_strategy_monitor_runtime_audit.jsonl"


class IbkrPaperStrategyMonitorError(RuntimeError):
    """Raised when the paper strategy monitor must fail closed."""


@dataclass(frozen=True)
class IbkrPaperStrategyMonitorConfig:
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
    dashboard_url: str = _DEFAULT_DASHBOARD_URL
    ledger_path: Path = _DEFAULT_LEDGER_PATH
    runtime_status_path: Path = _DEFAULT_VAR_RUNTIME_STATUS_PATH
    pnl_snapshot_path: Path = _DEFAULT_VAR_PNL_SNAPSHOT_PATH
    heartbeat_path: Path = _DEFAULT_VAR_HEARTBEAT_PATH
    runtime_audit_path: Path = _DEFAULT_VAR_AUDIT_PATH
    output_dir: Path = _DEFAULT_OUTPUT_DIR
    recent_fill_lookback_minutes: int = 240
    bridge_report_path: Path = _DEFAULT_BRIDGE_REPORT_PATH
    prepared_bundle_path: Path = _DEFAULT_PREPARED_BUNDLE_PATH
    strategy_tracking_snapshot_path: Path = _DEFAULT_STRATEGY_TRACKING_SNAPSHOT_PATH


@dataclass(frozen=True)
class IbkrPaperStrategyMonitorArtifacts:
    classification: str
    ledger: dict[str, Any]
    pnl_snapshot: dict[str, Any]
    status: dict[str, Any]
    audit_events: list[dict[str, Any]]
    broker_report: dict[str, Any]

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "PAPER_STRATEGY_MONITOR_BLOCKED" else 1


@dataclass(frozen=True)
class IbkrPaperStrategyMonitorDaemonConfig:
    monitor_config: IbkrPaperStrategyMonitorConfig
    poll_interval_seconds: float = 10.0
    max_cycles: int = 3
    freshness_window_seconds: float = 45.0
    reconnect_backoff_seconds: float = 10.0


@dataclass(frozen=True)
class IbkrPaperStrategyMonitorDaemonArtifacts:
    classification: str
    daemon_report: dict[str, Any]
    runtime_status: dict[str, Any]
    runtime_audit_events: list[dict[str, Any]]
    latest_cycle: IbkrPaperStrategyMonitorArtifacts | None

    @property
    def exit_code(self) -> int:
        return 0 if self.classification != "PAPER_STRATEGY_MONITOR_BLOCKED" else 1


def run_ibkr_paper_strategy_monitor(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    reconciliation_runner: Callable[..., Any] = run_ibkr_position_reconciliation,
    dashboard_fetcher: Callable[[str], dict[str, Any]] | None = None,
) -> IbkrPaperStrategyMonitorArtifacts:
    _validate_environment_lock(config)
    audit_events: list[dict[str, Any]] = []
    now = _utc_now()
    _record_audit(
        audit_events,
        "monitor_started",
        "Persistent IBKR paper strategy monitor started.",
        config=config,
    )

    dashboard_payload: dict[str, Any]
    backend_gate: dict[str, Any]
    dashboard_error: str | None = None
    try:
        dashboard_payload = (dashboard_fetcher or _fetch_dashboard_payload)(config.dashboard_url)
        backend_gate = _build_backend_gate(dashboard_payload)
        _record_audit(
            audit_events,
            "backend_gate_loaded",
            "Loaded live operator backend state for paper strategy gating.",
            config=config,
            extra={"backend_gate": backend_gate},
        )
    except IbkrPaperStrategyMonitorError as exc:
        dashboard_payload = {}
        dashboard_error = str(exc)
        backend_gate = {
            "backend_healthy": False,
            "live_source_ready": False,
            "startup_control_plane_ready": False,
            "paper_runtime_stale": None,
            "market_data_semantics": None,
            "temp_paper_blocked": None,
            "temp_paper_mismatch_status": None,
            "session_classification": None,
            "launch_allowed": False,
            "state": "UNAVAILABLE",
            "summary_line": dashboard_error,
            "source_mode": "UNAVAILABLE",
        }
        _record_audit(
            audit_events,
            "backend_gate_unavailable",
            "Live operator backend payload could not be loaded; submit gate remains blocked.",
            config=config,
            extra={"error": dashboard_error},
        )

    reconciliation = reconciliation_runner(
        config=IbkrPositionReconciliationConfig(
            repo_root=config.repo_root,
            mode=config.mode,
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            read_only=True,
            account_id=config.account_id,
            symbol=config.symbol,
            contract_month=config.contract_month,
            exact_expiry=config.exact_expiry,
            con_id=config.con_id,
            local_symbol=config.local_symbol,
            recent_fill_lookback_minutes=int(config.recent_fill_lookback_minutes),
        )
    )
    broker_report = dict(reconciliation.report)
    broker_quantity = float((broker_report.get("diagnosis") or {}).get("latest_exact_position_quantity") or 0.0)
    _record_audit(
        audit_events,
        "broker_reconciliation_loaded",
        "Loaded current exact-contract broker truth for the paper strategy monitor.",
        config=config,
        extra={
            "reconciliation_classification": reconciliation.classification,
            "broker_quantity": broker_quantity,
        },
    )

    existing_ledger = _load_json(config.repo_root / config.ledger_path)
    ownership = _determine_strategy_ownership(config=config, reconciliation_report=broker_report)
    _record_audit(
        audit_events,
        "strategy_ownership_resolved",
        "Resolved whether the current broker position can be adopted into the paper strategy ledger.",
        config=config,
        extra={"ownership": ownership},
    )
    if ownership.get("restored_from_prior_evidence"):
        _record_audit(
            audit_events,
            "paper_orphan_reconciliation_adoption",
            "Restored lost strategy attribution for known bridge-created paper position.",
            config=config,
            extra={
                "strategy_id": ownership.get("strategy_id"),
                "quantity": broker_quantity,
                "side": "LONG" if broker_quantity > 0 else ("SHORT" if broker_quantity < 0 else "FLAT"),
                "average_entry_price": ownership.get("average_entry_price"),
                "perm_id": ownership.get("perm_id"),
                "execution_id": ownership.get("execution_id"),
                "source": "paper_orphan_reconciliation_adoption",
                "reason": "restoring lost strategy attribution for known bridge-created paper position",
                "source_intent_id": ownership.get("source_intent_id"),
            },
        )
    ledger = _build_updated_ledger(
        config=config,
        now=now,
        existing_ledger=existing_ledger,
        reconciliation_report=broker_report,
        ownership=ownership,
    )
    pnl_snapshot = _build_pnl_snapshot(config=config, now=now, reconciliation_report=broker_report, ledger=ledger)
    status = _build_monitor_status(
        config=config,
        now=now,
        reconciliation_report=broker_report,
        ledger=ledger,
        ownership=ownership,
        backend_gate=backend_gate,
        dashboard_error=dashboard_error,
    )
    _record_audit(
        audit_events,
        "monitor_status_evaluated",
        "Evaluated ledger ownership, broker truth, and future submit gate status.",
        config=config,
        extra={
            "classification": status.get("classification"),
            "submit_allowed": status.get("submit_allowed"),
            "block_reasons": status.get("block_reasons"),
        },
    )
    return IbkrPaperStrategyMonitorArtifacts(
        classification=str(status.get("classification") or "PAPER_STRATEGY_MONITOR_BLOCKED"),
        ledger=ledger,
        pnl_snapshot=pnl_snapshot,
        status=status,
        audit_events=audit_events,
        broker_report=broker_report,
    )


def write_ibkr_paper_strategy_monitor_artifacts(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    artifacts: IbkrPaperStrategyMonitorArtifacts,
) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    persistent_ledger_path = config.repo_root / config.ledger_path
    persistent_ledger_path.parent.mkdir(parents=True, exist_ok=True)
    persistent_ledger_path.write_text(json.dumps(artifacts.ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    persistent_pnl_path = config.repo_root / config.pnl_snapshot_path
    persistent_pnl_path.parent.mkdir(parents=True, exist_ok=True)
    persistent_pnl_path.write_text(json.dumps(artifacts.pnl_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    (output_dir / "paper_strategy_position_ledger.json").write_text(
        json.dumps(artifacts.ledger, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "paper_strategy_pnl_snapshot.json").write_text(
        json.dumps(artifacts.pnl_snapshot, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "paper_strategy_monitor_status.json").write_text(
        json.dumps(artifacts.status, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / "paper_strategy_monitor_audit.jsonl").open("a", encoding="utf-8") as handle:
        for row in artifacts.audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def run_ibkr_paper_strategy_monitor_daemon(
    *,
    config: IbkrPaperStrategyMonitorDaemonConfig,
    sleep_fn: Callable[[float], None] = time.sleep,
    cycle_runner: Callable[..., IbkrPaperStrategyMonitorArtifacts] = run_ibkr_paper_strategy_monitor,
    should_stop: Callable[[], bool] | None = None,
) -> IbkrPaperStrategyMonitorDaemonArtifacts:
    runtime_audit_events: list[dict[str, Any]] = []
    written_runtime_audit_count = 0
    started_at = _utc_now()
    _record_runtime_audit(
        runtime_audit_events,
        "daemon_started",
        "Continuous paper strategy monitor polling started.",
        config=config,
        extra={"max_cycles": config.max_cycles, "poll_interval_seconds": config.poll_interval_seconds},
    )
    _write_runtime_heartbeat(
        config=config.monitor_config,
        payload={
            "generated_at": started_at,
            "monitor_running": True,
            "last_poll_time": started_at,
            "last_successful_broker_refresh": None,
            "poll_interval_seconds": float(config.poll_interval_seconds),
            "freshness_window_seconds": float(config.freshness_window_seconds),
            "health": "STARTING",
        },
    )
    written_runtime_audit_count = _flush_runtime_audit_events(
        config=config.monitor_config,
        runtime_audit_events=runtime_audit_events,
        written_count=written_runtime_audit_count,
    )
    cycles: list[dict[str, Any]] = []
    latest_cycle: IbkrPaperStrategyMonitorArtifacts | None = None
    latest_error: str | None = None
    max_cycles = int(config.max_cycles)
    cycle_index = 0

    while max_cycles <= 0 or cycle_index < max_cycles:
        if should_stop is not None and should_stop():
            _record_runtime_audit(
                runtime_audit_events,
                "daemon_stop_requested",
                "Continuous paper strategy monitor received a stop request.",
                config=config,
            )
            break
        cycle_index += 1
        cycle_started_at = _utc_now()
        try:
            latest_cycle = cycle_runner(config=config.monitor_config)
            write_ibkr_paper_strategy_monitor_artifacts(config=config.monitor_config, artifacts=latest_cycle)
            cycles.append(
                {
                    "cycle_index": cycle_index,
                    "started_at": cycle_started_at,
                    "finished_at": _utc_now(),
                    "classification": latest_cycle.classification,
                    "broker_position_quantity": latest_cycle.status.get("broker_position_quantity"),
                    "open_order_count": latest_cycle.status.get("open_order_count"),
                    "submit_allowed": latest_cycle.status.get("submit_allowed"),
                }
            )
            current_runtime_status = _build_runtime_status(
                config=config,
                started_at=started_at,
                cycles=cycles,
                latest_cycle=latest_cycle,
                latest_error=None,
                monitor_running=True,
            )
            _write_live_runtime_files(
                config=config.monitor_config,
                runtime_status=current_runtime_status,
                latest_cycle=latest_cycle,
            )
            _record_runtime_audit(
                runtime_audit_events,
                "cycle_completed",
                "Paper strategy monitor cycle completed.",
                config=config,
                extra=cycles[-1],
            )
            written_runtime_audit_count = _flush_runtime_audit_events(
                config=config.monitor_config,
                runtime_audit_events=runtime_audit_events,
                written_count=written_runtime_audit_count,
            )
        except Exception as exc:
            latest_error = str(exc)
            cycles.append(
                {
                    "cycle_index": cycle_index,
                    "started_at": cycle_started_at,
                    "finished_at": _utc_now(),
                    "classification": "PAPER_STRATEGY_MONITOR_DISCONNECTED",
                    "error": latest_error,
                }
            )
            current_runtime_status = _build_runtime_status(
                config=config,
                started_at=started_at,
                cycles=cycles,
                latest_cycle=latest_cycle,
                latest_error=latest_error,
                monitor_running=True,
            )
            _write_live_runtime_files(
                config=config.monitor_config,
                runtime_status=current_runtime_status,
                latest_cycle=latest_cycle,
            )
            _record_runtime_audit(
                runtime_audit_events,
                "cycle_failed",
                "Paper strategy monitor cycle failed closed.",
                config=config,
                extra=cycles[-1],
            )
            written_runtime_audit_count = _flush_runtime_audit_events(
                config=config.monitor_config,
                runtime_audit_events=runtime_audit_events,
                written_count=written_runtime_audit_count,
            )
            if max_cycles > 0 and cycle_index >= max_cycles:
                break
            sleep_fn(float(config.reconnect_backoff_seconds))
            continue
        if should_stop is not None and should_stop():
            break
        if max_cycles <= 0 or cycle_index < max_cycles:
            sleep_fn(float(config.poll_interval_seconds))

    runtime_status = _build_runtime_status(
        config=config,
        started_at=started_at,
        cycles=cycles,
        latest_cycle=latest_cycle,
        latest_error=latest_error,
        monitor_running=False,
    )
    _write_live_runtime_files(
        config=config.monitor_config,
        runtime_status=runtime_status,
        latest_cycle=latest_cycle,
    )
    written_runtime_audit_count = _flush_runtime_audit_events(
        config=config.monitor_config,
        runtime_audit_events=runtime_audit_events,
        written_count=written_runtime_audit_count,
    )
    daemon_report = {
        "classification": runtime_status.get("classification"),
        "generated_at": _utc_now(),
        "runtime_status_path": str((config.monitor_config.repo_root / config.monitor_config.output_dir / _RUNTIME_STATUS_FILENAME).resolve()),
        "persistent_runtime_status_path": str((config.monitor_config.repo_root / config.monitor_config.runtime_status_path).resolve()),
        "persistent_heartbeat_path": str((config.monitor_config.repo_root / config.monitor_config.heartbeat_path).resolve()),
        "persistent_runtime_audit_path": str((config.monitor_config.repo_root / config.monitor_config.runtime_audit_path).resolve()),
        "poll_interval_seconds": float(config.poll_interval_seconds),
        "freshness_window_seconds": float(config.freshness_window_seconds),
        "cycles_completed": len(cycles),
        "latest_cycle_classification": None if latest_cycle is None else latest_cycle.classification,
        "latest_runtime_status": runtime_status,
    }
    _record_runtime_audit(
        runtime_audit_events,
        "daemon_finished",
        "Continuous paper strategy monitor polling finished.",
        config=config,
        extra={"classification": runtime_status.get("classification"), "cycles_completed": len(cycles)},
    )
    return IbkrPaperStrategyMonitorDaemonArtifacts(
        classification=str(runtime_status.get("classification") or "PAPER_STRATEGY_MONITOR_BLOCKED"),
        daemon_report=daemon_report,
        runtime_status=runtime_status,
        runtime_audit_events=runtime_audit_events,
        latest_cycle=latest_cycle,
    )


def write_ibkr_paper_strategy_monitor_daemon_artifacts(
    *,
    config: IbkrPaperStrategyMonitorDaemonConfig,
    artifacts: IbkrPaperStrategyMonitorDaemonArtifacts,
) -> None:
    output_dir = config.monitor_config.repo_root / config.monitor_config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / _DAEMON_REPORT_FILENAME).write_text(
        json.dumps(artifacts.daemon_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "paper_strategy_monitor_daemon_report.md").write_text(
        render_ibkr_paper_strategy_monitor_daemon_markdown(artifacts.daemon_report),
        encoding="utf-8",
    )
    (output_dir / _RUNTIME_STATUS_FILENAME).write_text(
        json.dumps(artifacts.runtime_status, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    persistent_runtime_status_path = config.monitor_config.repo_root / config.monitor_config.runtime_status_path
    persistent_runtime_status_path.parent.mkdir(parents=True, exist_ok=True)
    persistent_runtime_status_path.write_text(
        json.dumps(artifacts.runtime_status, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with (output_dir / _RUNTIME_AUDIT_FILENAME).open("a", encoding="utf-8") as handle:
        for row in artifacts.runtime_audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    persistent_runtime_audit_path = config.monitor_config.repo_root / config.monitor_config.runtime_audit_path
    persistent_runtime_audit_path.parent.mkdir(parents=True, exist_ok=True)
    with persistent_runtime_audit_path.open("a", encoding="utf-8") as handle:
        for row in artifacts.runtime_audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def load_paper_strategy_monitor_status(*, repo_root: Path) -> dict[str, Any]:
    runtime_path = repo_root / _DEFAULT_VAR_RUNTIME_STATUS_PATH
    if not runtime_path.exists():
        runtime_path = repo_root / _DEFAULT_OUTPUT_DIR / _RUNTIME_STATUS_FILENAME
    runtime_status_fallback_reason: str | None = None
    if runtime_path.exists():
        try:
            runtime_status = dict(json.loads(runtime_path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            runtime_status = {
                "classification": "PAPER_STRATEGY_MONITOR_BLOCKED",
                "submit_allowed": False,
                "block_reasons": ["paper_strategy_monitor_runtime_status_invalid"],
                "detail": "Paper strategy monitor runtime status could not be decoded.",
            }
        service_running = _monitor_service_process_running(repo_root)
        if bool(runtime_status.get("monitor_running")) and service_running is False:
            runtime_status_fallback_reason = "paper_strategy_monitor_not_running"
        else:
            reasons = list(runtime_status.get("block_reasons") or [])
            runtime_status["submit_allowed"] = bool(runtime_status.get("submit_allowed"))
            if not bool(runtime_status.get("monitor_running")):
                runtime_status["submit_allowed"] = False
                if "paper_strategy_monitor_not_running" not in reasons:
                    reasons.append("paper_strategy_monitor_not_running")
            freshness_window = float(runtime_status.get("freshness_window_seconds") or 0.0)
            refreshed_at = _parse_datetime(
                runtime_status.get("last_successful_broker_refresh") or runtime_status.get("last_broker_refresh_timestamp")
            )
            stale = True
            if freshness_window > 0.0 and refreshed_at is not None:
                age_seconds = max(0.0, (datetime.now(timezone.utc) - refreshed_at).total_seconds())
                runtime_status["age_seconds"] = age_seconds
                stale = age_seconds > freshness_window
                runtime_status["stale"] = stale
                if stale:
                    runtime_status["submit_allowed"] = False
                    if "paper_strategy_monitor_runtime_stale" not in reasons:
                        reasons.append("paper_strategy_monitor_runtime_stale")
            elif freshness_window > 0.0:
                stale = True
                runtime_status["stale"] = True
                runtime_status["submit_allowed"] = False
                if "paper_strategy_monitor_runtime_refresh_missing" not in reasons:
                    reasons.append("paper_strategy_monitor_runtime_refresh_missing")
            else:
                stale = bool(runtime_status.get("stale"))
                runtime_status["stale"] = stale
            health = str(runtime_status.get("health_classification") or runtime_status.get("monitor_health") or "").strip().upper()
            if health and health != "HEALTHY":
                runtime_status["submit_allowed"] = False
                normalized = f"paper_strategy_monitor_health_{health.lower()}"
                if normalized not in reasons:
                    reasons.append(normalized)
            runtime_status["block_reasons"] = reasons
            runtime_status["submit_allowed"] = bool(runtime_status.get("submit_allowed")) and bool(runtime_status.get("monitor_running")) and not stale and health == "HEALTHY"
            return runtime_status

    path = repo_root / _DEFAULT_OUTPUT_DIR / "paper_strategy_monitor_status.json"
    if not path.exists():
        return {
            "classification": "PAPER_STRATEGY_MONITOR_BLOCKED",
            "submit_allowed": False,
            "block_reasons": ["paper_strategy_monitor_runtime_status_missing"],
            "detail": "Paper strategy monitor runtime status has not been generated yet.",
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {
            "classification": "PAPER_STRATEGY_MONITOR_BLOCKED",
            "submit_allowed": False,
            "block_reasons": ["paper_strategy_monitor_status_invalid"],
            "detail": "Paper strategy monitor status could not be decoded.",
        }
    snapshot_payload = dict(payload)
    snapshot_status = dict(snapshot_payload.get("snapshot_status") or snapshot_payload)
    reasons = list(snapshot_status.get("block_reasons") or [])
    if runtime_status_fallback_reason and runtime_status_fallback_reason not in reasons:
        reasons.append(runtime_status_fallback_reason)
    return {
        "classification": snapshot_status.get("classification") or "PAPER_STRATEGY_MONITOR_BLOCKED",
        "submit_allowed": False,
        "block_reasons": reasons or ["paper_strategy_monitor_runtime_status_missing", "paper_strategy_monitor_snapshot_only"],
        "detail": snapshot_status.get("detail")
        or "Paper strategy monitor daemon runtime status is missing; snapshot-only monitor output cannot authorize new paper orders.",
        "snapshot_status": snapshot_payload,
        "strategy_id": snapshot_status.get("strategy_id"),
        "account_id": snapshot_status.get("account_id"),
        "exact_contract": snapshot_status.get("exact_contract"),
        "broker_position_quantity": snapshot_status.get("broker_position_quantity"),
        "ledger_position_quantity": snapshot_status.get("ledger_position_quantity"),
        "ownership_proven": snapshot_status.get("ownership_proven"),
        "monitor_running": False,
    }


def _monitor_service_process_running(repo_root: Path) -> bool | None:
    pid_path = repo_root / _DEFAULT_MONITOR_SERVICE_PID_PATH
    if not pid_path.exists():
        return None
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip() or "0")
    except ValueError:
        return False
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def render_ibkr_paper_strategy_monitor_markdown(
    *,
    status: dict[str, Any],
    ledger: dict[str, Any],
    pnl_snapshot: dict[str, Any],
) -> str:
    active_position = dict((ledger.get("positions") or [None])[0] or {})
    lines = [
        "# IBKR Paper Strategy Monitor",
        "",
        f"- classification: `{status.get('classification')}`",
        f"- account: `{status.get('account_id')}`",
        f"- strategy id: `{active_position.get('strategy_id')}`",
        f"- exact contract: `MGC {active_position.get('expiry')}` / `conId={active_position.get('con_id')}` / `localSymbol={active_position.get('local_symbol')}`",
        f"- broker quantity: `{status.get('broker_position_quantity')}`",
        f"- ledger quantity: `{status.get('ledger_position_quantity')}`",
        f"- ledger state: `{active_position.get('state')}`",
        f"- ownership proven: `{status.get('ownership_proven')}`",
        f"- submit allowed: `{status.get('submit_allowed')}`",
        "",
        "## P&L Snapshot",
        "",
        f"- unrealized pnl: `{pnl_snapshot.get('unrealized_pnl')}`",
        f"- realized pnl: `{pnl_snapshot.get('realized_pnl')}`",
        f"- market price: `{pnl_snapshot.get('market_price')}`",
        f"- pnl source: `{pnl_snapshot.get('pnl_source')}`",
        "",
        "## Submit Gate",
        "",
    ]
    for reason in list(status.get("block_reasons") or []):
        lines.append(f"- `{reason}`")
    return "\n".join(lines)


def render_ibkr_paper_strategy_monitor_daemon_markdown(report: dict[str, Any]) -> str:
    status = dict(report.get("latest_runtime_status") or {})
    lines = [
        "# IBKR Paper Strategy Monitor Daemon",
        "",
        f"- classification: `{report.get('classification')}`",
        f"- poll interval seconds: `{report.get('poll_interval_seconds')}`",
        f"- freshness window seconds: `{report.get('freshness_window_seconds')}`",
        f"- cycles completed: `{report.get('cycles_completed')}`",
        f"- strategy id: `{status.get('strategy_id')}`",
        f"- account: `{status.get('account_id')}`",
        f"- contract: `MGC {dict(status.get('exact_contract') or {}).get('expiry')}` / `conId={dict(status.get('exact_contract') or {}).get('con_id')}` / `localSymbol={dict(status.get('exact_contract') or {}).get('local_symbol')}`",
        f"- position quantity: `{status.get('broker_position_quantity')}`",
        f"- average entry price: `{status.get('average_entry_price')}`",
        f"- unrealized pnl: `{status.get('unrealized_pnl')}`",
        f"- realized pnl: `{status.get('realized_pnl')}`",
        f"- open orders: `{status.get('open_order_count')}`",
        f"- last broker refresh timestamp: `{status.get('last_broker_refresh_timestamp')}`",
        f"- monitor health: `{status.get('monitor_health')}`",
        f"- stale: `{status.get('stale')}`",
        f"- pnl source: `{status.get('pnl_source')}`",
    ]
    for reason in list(status.get("block_reasons") or []):
        lines.append(f"- block reason: `{reason}`")
    return "\n".join(lines)


def _validate_environment_lock(config: IbkrPaperStrategyMonitorConfig) -> None:
    if str(config.mode or "").strip().upper() != _EXPECTED_MODE:
        raise IbkrPaperStrategyMonitorError("Paper strategy monitor is PAPER-only.")
    if str(config.host or "").strip() != _EXPECTED_HOST or int(config.port) != _EXPECTED_PORT:
        raise IbkrPaperStrategyMonitorError("Paper strategy monitor environment lock requires 127.0.0.1:7497.")
    if str(config.account_id or "").strip() != _EXPECTED_ACCOUNT_ID:
        raise IbkrPaperStrategyMonitorError("Paper strategy monitor account lock requires DUM882026.")


def _fetch_dashboard_payload(url: str) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise IbkrPaperStrategyMonitorError(f"Live dashboard payload could not be loaded from {url}: {exc}") from exc


def _build_backend_gate(payload: dict[str, Any]) -> dict[str, Any]:
    dashboard_meta = dict(payload.get("dashboard_meta") or {})
    supervised = dict(payload.get("supervised_paper_operability") or {})
    startup = dict(payload.get("startup_control_plane") or {})
    paper = dict(payload.get("paper") or {})
    paper_status = dict(paper.get("status") or {})
    temp_integrity = dict(paper.get("temporary_paper_runtime_integrity") or {})
    readiness = dict(paper.get("readiness") or {})
    return {
        "backend_healthy": not bool(dashboard_meta.get("degraded")) and bool(supervised.get("dashboard_attached")),
        "live_source_ready": bool(supervised.get("dashboard_attached")) and str(startup.get("overall_state") or "").upper() == "READY",
        "startup_control_plane_ready": str(startup.get("overall_state") or "").upper() == "READY",
        "paper_runtime_stale": bool(paper_status.get("stale")),
        "market_data_semantics": paper_status.get("market_data_semantics"),
        "temp_paper_blocked": bool(temp_integrity.get("temp_paper_blocked")),
        "temp_paper_mismatch_status": temp_integrity.get("mismatch_status"),
        "session_classification": readiness.get("current_detected_session"),
        "launch_allowed": bool(supervised.get("launch_allowed")),
        "state": supervised.get("state"),
        "summary_line": supervised.get("summary_line"),
        "source_mode": "LIVE_API" if bool(supervised.get("dashboard_attached")) else "SNAPSHOT_FALLBACK",
    }


def _determine_strategy_ownership(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    reconciliation_report: dict[str, Any],
) -> dict[str, Any]:
    latest_exec = _latest_matching_execution_row(reconciliation_report)
    latest_perm_id = (reconciliation_report.get("diagnosis") or {}).get("latest_matching_perm_id")
    broker_quantity = float((reconciliation_report.get("diagnosis") or {}).get("latest_exact_position_quantity") or 0.0)

    strategy_snapshot = _load_json(config.repo_root / config.strategy_tracking_snapshot_path)
    bridge_report = _load_json(config.repo_root / config.bridge_report_path)
    prepared_bundle = _load_json(config.repo_root / config.prepared_bundle_path)

    bridge_intent = dict(bridge_report.get("intent") or {})
    preview_payload = dict(prepared_bundle.get("preview_payload") or {})
    preview_environment = dict(preview_payload.get("environment") or {})
    prior_adopted = _load_prior_adopted_position_evidence(config)
    bridge_strategy_matches = bridge_intent.get("strategy_id") == config.strategy_id
    bridge_buy_one_matches = bridge_intent.get("action") == "BUY" and float(bridge_intent.get("quantity") or 0.0) == 1.0
    snapshot_quantity_matches = (
        strategy_snapshot.get("strategy_id") == config.strategy_id
        and float(strategy_snapshot.get("current_reconciled_quantity") or 0.0) == broker_quantity
    )
    snapshot_matches = (
        snapshot_quantity_matches
        and latest_perm_id is not None
        and strategy_snapshot.get("latest_matching_perm_id") == latest_perm_id
    )
    exec_matches = (
        latest_exec is not None
        and int(latest_exec.get("con_id") or 0) == int(config.con_id)
        and str(latest_exec.get("local_symbol") or "").strip().upper() == config.local_symbol
        and str(latest_exec.get("side") or "").strip().upper() == "BOT"
        and float(latest_exec.get("quantity") or 0.0) == 1.0
    )
    prepared_client_matches = latest_exec is not None and int(preview_environment.get("client_id") or 0) == int(latest_exec.get("client_id") or 0)
    ownership_proven = broker_quantity == 1.0 and snapshot_matches and bridge_strategy_matches and bridge_buy_one_matches and exec_matches and prepared_client_matches

    if ownership_proven and latest_exec is not None:
        return {
            "classification": "adopted",
            "ownership_proven": True,
            "strategy_id": config.strategy_id,
            "detail": "Bridge intent, prepared frozen preview clientId, prior strategy snapshot, and current broker execution permId all align to ATP_COMPANION_V1_ASIA_US.",
            "source_intent_id": bridge_intent.get("intent_id"),
            "perm_id": latest_perm_id,
            "execution_id": latest_exec.get("execution_id"),
            "client_id": latest_exec.get("client_id"),
            "sources": [
                str((config.repo_root / config.strategy_tracking_snapshot_path).resolve()),
                str((config.repo_root / config.bridge_report_path).resolve()),
                str((config.repo_root / config.prepared_bundle_path).resolve()),
            ],
            "restored_from_prior_evidence": False,
            "restoration_source": None,
            "average_entry_price": latest_exec.get("price"),
            "order_id": latest_exec.get("broker_order_id"),
            "entry_timestamp": latest_exec.get("executed_at"),
        }

    prior_perm_id = prior_adopted.get("perm_id")
    prior_contract_matches = (
        prior_adopted.get("strategy_id") == config.strategy_id
        and int(prior_adopted.get("con_id") or config.con_id) == int(config.con_id)
        and str(prior_adopted.get("local_symbol") or config.local_symbol).strip().upper() == config.local_symbol
        and str(prior_adopted.get("side") or "LONG").strip().upper() == "LONG"
    )
    prior_quantity_matches = float(prior_adopted.get("quantity") or 0.0) == broker_quantity == 1.0
    prior_perm_matches = latest_perm_id is None or prior_perm_id is None or int(prior_perm_id) == int(latest_perm_id)
    snapshot_perm_matches_prior = (
        strategy_snapshot.get("latest_matching_perm_id") is None
        or prior_perm_id is None
        or int(strategy_snapshot.get("latest_matching_perm_id") or 0) == int(prior_perm_id)
    )
    restoration_proven = (
        snapshot_quantity_matches
        and prior_contract_matches
        and prior_quantity_matches
        and prior_perm_matches
        and snapshot_perm_matches_prior
    )
    if restoration_proven:
        return {
            "classification": "adopted",
            "ownership_proven": True,
            "strategy_id": config.strategy_id,
            "detail": "Restored lost strategy attribution for known bridge-created paper position using prior adopted ATP evidence.",
            "source_intent_id": prior_adopted.get("source_intent_id") or bridge_intent.get("intent_id"),
            "perm_id": prior_perm_id,
            "execution_id": prior_adopted.get("execution_id"),
            "client_id": prior_adopted.get("client_id"),
            "sources": [
                str((config.repo_root / config.strategy_tracking_snapshot_path).resolve()),
                str((config.repo_root / config.bridge_report_path).resolve()),
                str((config.repo_root / config.prepared_bundle_path).resolve()),
                *list(prior_adopted.get("sources") or []),
            ],
            "restored_from_prior_evidence": True,
            "restoration_source": "paper_orphan_reconciliation_adoption",
            "average_entry_price": prior_adopted.get("average_entry_price"),
            "order_id": prior_adopted.get("order_id"),
            "entry_timestamp": prior_adopted.get("entry_timestamp"),
        }
    if broker_quantity == 0.0 and prior_contract_matches:
        return {
            "classification": "adopted",
            "ownership_proven": True,
            "strategy_id": config.strategy_id,
            "detail": "Preserved ATP ownership on the reconciled flat paper position using prior adopted evidence from the opened broker lot.",
            "source_intent_id": prior_adopted.get("source_intent_id") or bridge_intent.get("intent_id"),
            "perm_id": prior_perm_id,
            "execution_id": prior_adopted.get("execution_id"),
            "client_id": prior_adopted.get("client_id"),
            "sources": [
                str((config.repo_root / config.strategy_tracking_snapshot_path).resolve()),
                str((config.repo_root / config.bridge_report_path).resolve()),
                str((config.repo_root / config.prepared_bundle_path).resolve()),
                *list(prior_adopted.get("sources") or []),
            ],
            "restored_from_prior_evidence": True,
            "restoration_source": "paper_orphan_reconciliation_adoption",
            "average_entry_price": prior_adopted.get("average_entry_price"),
            "order_id": prior_adopted.get("order_id"),
            "entry_timestamp": prior_adopted.get("entry_timestamp"),
        }
    return {
        "classification": "orphan",
        "ownership_proven": False,
        "strategy_id": None,
        "detail": "The current broker position could not be proven to belong to the expected paper strategy lane from existing bridge evidence.",
        "source_intent_id": None,
        "perm_id": latest_perm_id,
        "execution_id": None if latest_exec is None else latest_exec.get("execution_id"),
        "client_id": None if latest_exec is None else latest_exec.get("client_id"),
        "sources": [
            str((config.repo_root / config.strategy_tracking_snapshot_path).resolve()),
            str((config.repo_root / config.bridge_report_path).resolve()),
            str((config.repo_root / config.prepared_bundle_path).resolve()),
        ],
        "restored_from_prior_evidence": False,
        "restoration_source": None,
        "average_entry_price": None,
        "order_id": None,
        "entry_timestamp": None,
    }


def _build_updated_ledger(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    now: str,
    existing_ledger: dict[str, Any],
    reconciliation_report: dict[str, Any],
    ownership: dict[str, Any],
) -> dict[str, Any]:
    latest_exec = _latest_matching_execution_row(reconciliation_report)
    portfolio_row = _latest_portfolio_row(reconciliation_report)
    broker_quantity = float((reconciliation_report.get("diagnosis") or {}).get("latest_exact_position_quantity") or 0.0)
    existing_positions = list(existing_ledger.get("positions") or [])
    previously_adopted = (bool(existing_positions) and bool((existing_positions[0] or {}).get("strategy_id"))) or bool(
        ownership.get("restored_from_prior_evidence")
    )

    side = "FLAT"
    state = "FLAT"
    if broker_quantity > 0:
        side = "LONG"
        state = "OPEN"
    elif broker_quantity < 0:
        side = "SHORT"
        state = "NEEDS_REVIEW"

    preserved_strategy_id = ownership.get("strategy_id") or (
        existing_positions[0].get("strategy_id")
        if broker_quantity == 0.0 and previously_adopted and existing_positions
        else None
    )
    preserved_source_intent_id = ownership.get("source_intent_id") or (
        existing_positions[0].get("source_intent_id")
        if broker_quantity == 0.0 and previously_adopted and existing_positions
        else None
    )
    preserved_perm_id = ownership.get("perm_id") or (
        existing_positions[0].get("perm_id")
        if broker_quantity == 0.0 and previously_adopted and existing_positions
        else None
    )
    preserved_execution_id = ownership.get("execution_id") or (
        existing_positions[0].get("execution_id")
        if broker_quantity == 0.0 and previously_adopted and existing_positions
        else None
    )
    preserved_order_id = ownership.get("order_id") if latest_exec is None else latest_exec.get("broker_order_id")
    if preserved_order_id is None and broker_quantity == 0.0 and previously_adopted and existing_positions:
        preserved_order_id = existing_positions[0].get("order_id")
    preserved_entry_timestamp = ownership.get("entry_timestamp") if latest_exec is None else latest_exec.get("executed_at")
    if preserved_entry_timestamp is None and broker_quantity == 0.0 and previously_adopted and existing_positions:
        preserved_entry_timestamp = existing_positions[0].get("entry_timestamp")
    preserved_average_entry_price = ownership.get("average_entry_price") if latest_exec is None else latest_exec.get("price")
    if preserved_average_entry_price is None and broker_quantity == 0.0 and previously_adopted and existing_positions:
        preserved_average_entry_price = existing_positions[0].get("average_entry_price")

    position_row = {
        "strategy_id": preserved_strategy_id,
        "account_id": config.account_id,
        "environment": {
            "mode": config.mode,
            "host": config.host,
            "port": config.port,
        },
        "symbol": config.symbol,
        "contract_month": config.contract_month,
        "expiry": config.exact_expiry,
        "con_id": config.con_id,
        "local_symbol": config.local_symbol,
        "side": side,
        "quantity": broker_quantity,
        "average_entry_price": preserved_average_entry_price,
        "average_cost_basis": None if portfolio_row is None else portfolio_row.get("average_cost"),
        "order_id": preserved_order_id,
        "perm_id": preserved_perm_id,
        "execution_id": preserved_execution_id,
        "entry_timestamp": preserved_entry_timestamp,
        "source_intent_id": preserved_source_intent_id,
        "state": state if (ownership.get("ownership_proven") or (broker_quantity == 0.0 and previously_adopted)) else ("FLAT" if broker_quantity == 0.0 else "NEEDS_REVIEW"),
        "realized_pnl": None if portfolio_row is None else portfolio_row.get("realized_pnl"),
        "unrealized_pnl": None if portfolio_row is None else portfolio_row.get("unrealized_pnl"),
        "last_reconciliation_timestamp": now,
        "pnl_source": "ibkr_updatePortfolio" if portfolio_row is not None else "ibkr_account_snapshot",
        "ownership_detail": ownership.get("detail"),
        "adopted_from_broker_truth": bool(ownership.get("ownership_proven")) or (broker_quantity == 0.0 and previously_adopted),
        "previously_adopted": previously_adopted,
    }

    positions: list[dict[str, Any]]
    orphan_positions: list[dict[str, Any]]
    if broker_quantity == 0.0:
        positions = [position_row] if previously_adopted else []
        orphan_positions = []
    elif ownership.get("ownership_proven"):
        positions = [position_row]
        orphan_positions = []
    else:
        positions = []
        orphan_positions = [position_row]

    return {
        "schema_version": 1,
        "generated_at": now,
        "persistent_path": str((config.repo_root / config.ledger_path).resolve()),
        "positions": positions,
        "orphan_positions": orphan_positions,
        "previous_positions": existing_positions,
    }


def _build_pnl_snapshot(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    now: str,
    reconciliation_report: dict[str, Any],
    ledger: dict[str, Any],
) -> dict[str, Any]:
    portfolio_row = _latest_portfolio_row(reconciliation_report)
    account_truth = dict(reconciliation_report.get("account_truth") or {})
    active_position = dict((ledger.get("positions") or [{}])[0] or {})
    latest_exec = _latest_matching_execution_row(reconciliation_report)
    return {
        "generated_at": now,
        "strategy_id": active_position.get("strategy_id"),
        "account_id": config.account_id,
        "exact_contract": {
            "symbol": config.symbol,
            "expiry": config.exact_expiry,
            "con_id": config.con_id,
            "local_symbol": config.local_symbol,
        },
        "position_quantity": active_position.get("quantity", 0.0),
        "average_entry_price": active_position.get("average_entry_price"),
        "average_cost_basis": active_position.get("average_cost_basis"),
        "latest_execution_price": None if latest_exec is None else latest_exec.get("price"),
        "unrealized_pnl": None if portfolio_row is None else portfolio_row.get("unrealized_pnl"),
        "realized_pnl": None if portfolio_row is None else portfolio_row.get("realized_pnl"),
        "market_price": None if portfolio_row is None else portfolio_row.get("market_price"),
        "market_value": None if portfolio_row is None else portfolio_row.get("market_value"),
        "pnl_source": "ibkr_updatePortfolio" if portfolio_row is not None else "ibkr_account_snapshot",
        "broker_account_pnl_fields": {
            "net_liquidation": account_truth.get("net_liquidation"),
            "buying_power": account_truth.get("buying_power"),
            "currency": account_truth.get("currency"),
        },
    }


def _build_monitor_status(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    now: str,
    reconciliation_report: dict[str, Any],
    ledger: dict[str, Any],
    ownership: dict[str, Any],
    backend_gate: dict[str, Any],
    dashboard_error: str | None,
) -> dict[str, Any]:
    broker_quantity = float((reconciliation_report.get("diagnosis") or {}).get("latest_exact_position_quantity") or 0.0)
    open_orders = list((((reconciliation_report.get("provider_snapshot") or {}).get("open_orders")) or []))
    active_position = dict((ledger.get("positions") or [None])[0] or {})
    ledger_quantity = float(active_position.get("quantity") or 0.0)
    previous_positions = list(ledger.get("previous_positions") or [])
    previous_active = dict((previous_positions or [None])[0] or {})
    previous_quantity_raw = previous_active.get("quantity")
    previous_quantity = broker_quantity if previous_quantity_raw is None else float(previous_quantity_raw)
    mismatch = bool(previous_positions) and previous_quantity != broker_quantity and not (
        broker_quantity == 0.0 and bool(previous_active.get("adopted_from_broker_truth"))
    )
    orphan = broker_quantity > 0.0 and not ownership.get("ownership_proven")
    newly_adopted = bool(active_position) and bool(active_position.get("adopted_from_broker_truth")) and not bool(
        previous_positions and (previous_positions[0] or {}).get("adopted_from_broker_truth")
    )

    block_reasons: list[str] = []
    if not backend_gate.get("backend_healthy"):
        block_reasons.append("backend_down")
    if not backend_gate.get("live_source_ready"):
        block_reasons.append("source_snapshot_fallback")
    if backend_gate.get("paper_runtime_stale"):
        block_reasons.append("paper_runtime_stale")
    if backend_gate.get("temp_paper_blocked"):
        block_reasons.append("temp_paper_blocked")
    if mismatch:
        block_reasons.append("ledger_broker_mismatch")
    if orphan:
        block_reasons.append("orphan_broker_position")
    if open_orders:
        block_reasons.append("working_open_order_present")

    classification = "PAPER_STRATEGY_MONITOR_ACTIVE"
    if dashboard_error is not None:
        classification = "PAPER_STRATEGY_MONITOR_BLOCKED"
    elif mismatch:
        classification = "PAPER_STRATEGY_LEDGER_BROKER_MISMATCH"
    elif orphan:
        classification = "PAPER_STRATEGY_ORPHAN_POSITION"
    elif newly_adopted:
        classification = "PAPER_STRATEGY_POSITION_ADOPTED"

    return {
        "classification": classification,
        "generated_at": now,
        "strategy_id": ownership.get("strategy_id"),
        "account_id": config.account_id,
        "exact_contract": {
            "symbol": config.symbol,
            "expiry": config.exact_expiry,
            "con_id": config.con_id,
            "local_symbol": config.local_symbol,
        },
        "ownership_proven": ownership.get("ownership_proven"),
        "broker_position_quantity": broker_quantity,
        "ledger_position_quantity": ledger_quantity,
        "open_order_count": len(open_orders),
        "backend_gate": backend_gate,
        "block_reasons": block_reasons,
        "submit_allowed": not block_reasons,
        "continuous_monitor_active": False,
        "persistent_ledger_exists": bool(active_position),
        "persistent_strategy_position_ledger": True,
        "continuous_unrealized_pnl_tracking_active": False,
        "continuous_realized_pnl_tracking_active": False,
        "detail": ownership.get("detail") if dashboard_error is None else dashboard_error,
        "monitoring_scope": "snapshot_reconciliation_only",
    }


def _load_prior_adopted_position_evidence(config: IbkrPaperStrategyMonitorConfig) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    audit_path = config.repo_root / config.output_dir / "paper_strategy_monitor_audit.jsonl"
    if audit_path.exists():
        try:
            for line in audit_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                row = json.loads(line)
                if not isinstance(row, dict):
                    continue
                if row.get("event_type") != "strategy_ownership_resolved":
                    continue
                ownership = dict(row.get("ownership") or {})
                if (
                    ownership.get("classification") == "adopted"
                    and ownership.get("strategy_id") == config.strategy_id
                ):
                    evidence.update(
                        {
                            "strategy_id": ownership.get("strategy_id"),
                            "perm_id": ownership.get("perm_id"),
                            "execution_id": ownership.get("execution_id"),
                            "client_id": ownership.get("client_id"),
                            "source_intent_id": ownership.get("source_intent_id"),
                            "ownership_detail": ownership.get("detail"),
                            "sources": [str(audit_path.resolve()), *list(ownership.get("sources") or [])],
                        }
                    )
        except Exception:
            pass

    for candidate_path in (
        config.repo_root / _DEFAULT_EXECUTOR_LOOP_STATUS_PATH,
        config.repo_root / _DEFAULT_EXECUTOR_REPORT_PATH,
    ):
        payload = _load_json(candidate_path)
        position = dict(payload.get("strategy_position") or {})
        if (
            position.get("strategy_id") == config.strategy_id
            and bool(position.get("adopted_from_broker_truth"))
            and int(position.get("con_id") or 0) == int(config.con_id)
            and str(position.get("local_symbol") or "").strip().upper() == config.local_symbol
        ):
            evidence.update(
                {
                    "strategy_id": position.get("strategy_id"),
                    "con_id": position.get("con_id"),
                    "local_symbol": position.get("local_symbol"),
                    "quantity": position.get("quantity"),
                    "side": position.get("side"),
                    "average_entry_price": position.get("average_entry_price"),
                    "order_id": position.get("order_id"),
                    "entry_timestamp": position.get("entry_timestamp"),
                    "source_intent_id": position.get("source_intent_id") or evidence.get("source_intent_id"),
                    "perm_id": position.get("perm_id") or evidence.get("perm_id"),
                    "execution_id": position.get("execution_id") or evidence.get("execution_id"),
                    "sources": [str(candidate_path.resolve()), *list(evidence.get("sources") or [])],
                }
            )
            break
    return evidence


def _build_runtime_status(
    *,
    config: IbkrPaperStrategyMonitorDaemonConfig,
    started_at: str,
    cycles: list[dict[str, Any]],
    latest_cycle: IbkrPaperStrategyMonitorArtifacts | None,
    latest_error: str | None,
    monitor_running: bool,
) -> dict[str, Any]:
    latest_cycle_status = {} if latest_cycle is None else dict(latest_cycle.status)
    latest_cycle_pnl = {} if latest_cycle is None else dict(latest_cycle.pnl_snapshot)
    latest_cycle_report = {} if latest_cycle is None else dict(latest_cycle.broker_report)
    generated_at = _utc_now()
    last_refresh_timestamp = None
    if latest_cycle is not None:
        last_refresh_timestamp = latest_cycle_status.get("generated_at") or latest_cycle_pnl.get("generated_at")
    block_reasons = list(latest_cycle_status.get("block_reasons") or [])
    classification = "PAPER_STRATEGY_MONITOR_BLOCKED"
    monitor_health = "ERROR"
    health_classification = "ERROR"

    provider_snapshot = dict(latest_cycle_report.get("provider_snapshot") or {})
    provider_health = dict(provider_snapshot.get("health") or {})
    latest_errors = list(latest_cycle_report.get("errors") or [])
    last_error = latest_error or (latest_errors[-1] if latest_errors else None)
    last_refresh = _parse_datetime(last_refresh_timestamp)
    stale = True
    age_seconds: float | None = None
    if last_refresh is not None:
        age_seconds = max(0.0, (datetime.now(timezone.utc) - last_refresh).total_seconds())
        stale = age_seconds > float(config.freshness_window_seconds)
    if latest_error is not None:
        classification = "PAPER_STRATEGY_MONITOR_DISCONNECTED"
        monitor_health = "DISCONNECTED"
        health_classification = "DISCONNECTED"
        if "monitor_disconnected" not in block_reasons:
            block_reasons.append("monitor_disconnected")
    elif stale:
        classification = "PAPER_STRATEGY_MONITOR_PARTIAL" if latest_cycle is not None else "PAPER_STRATEGY_MONITOR_BLOCKED"
        monitor_health = "STALE"
        health_classification = "STALE"
        if "paper_strategy_monitor_runtime_stale" not in block_reasons:
            block_reasons.append("paper_strategy_monitor_runtime_stale")
    elif "ledger_broker_mismatch" in block_reasons:
        classification = "PAPER_STRATEGY_MONITOR_PARTIAL"
        monitor_health = "DEGRADED"
        health_classification = "LEDGER_BROKER_MISMATCH"
    elif "orphan_broker_position" in block_reasons:
        classification = "PAPER_STRATEGY_MONITOR_PARTIAL"
        monitor_health = "DEGRADED"
        health_classification = "ORPHAN_BROKER_POSITION"
    elif "working_open_order_present" in block_reasons:
        classification = "PAPER_STRATEGY_MONITOR_PARTIAL"
        monitor_health = "DEGRADED"
        health_classification = "OPEN_ORDER_PRESENT"
    elif latest_cycle is not None and bool(latest_cycle_status.get("ownership_proven")):
        classification = "PAPER_STRATEGY_MONITOR_ACTIVE"
        monitor_health = "HEALTHY"
        health_classification = "HEALTHY"
    elif latest_cycle is not None:
        classification = "PAPER_STRATEGY_MONITOR_PARTIAL"
        monitor_health = "DEGRADED"
        health_classification = "ERROR"

    if not monitor_running:
        if "paper_strategy_monitor_not_running" not in block_reasons:
            block_reasons.append("paper_strategy_monitor_not_running")

    return {
        "classification": classification,
        "generated_at": generated_at,
        "runtime_started_at": started_at,
        "runtime_finished_at": None if monitor_running else generated_at,
        "monitor_running": monitor_running,
        "poll_interval_seconds": float(config.poll_interval_seconds),
        "freshness_window_seconds": float(config.freshness_window_seconds),
        "cycles_completed": len(cycles),
        "strategy_id": latest_cycle_status.get("strategy_id"),
        "account_id": latest_cycle_status.get("account_id"),
        "exact_contract": latest_cycle_status.get("exact_contract"),
        "broker_position_quantity": latest_cycle_status.get("broker_position_quantity"),
        "ledger_position_quantity": latest_cycle_status.get("ledger_position_quantity"),
        "average_entry_price": latest_cycle_pnl.get("average_entry_price"),
        "unrealized_pnl": latest_cycle_pnl.get("unrealized_pnl"),
        "realized_pnl": latest_cycle_pnl.get("realized_pnl"),
        "open_order_count": latest_cycle_status.get("open_order_count"),
        "last_poll_time": generated_at,
        "last_successful_broker_refresh": last_refresh_timestamp,
        "last_broker_refresh_timestamp": last_refresh_timestamp,
        "ibkr_connection_state": "CONNECTED" if bool(provider_health.get("connected")) else ("DISCONNECTED" if latest_error else "UNKNOWN"),
        "monitor_health": monitor_health,
        "health_classification": health_classification,
        "stale": stale,
        "age_seconds": age_seconds,
        "pnl_source": latest_cycle_pnl.get("pnl_source"),
        "submit_allowed": bool(monitor_running) and not stale and classification == "PAPER_STRATEGY_MONITOR_ACTIVE" and health_classification == "HEALTHY",
        "block_reasons": block_reasons,
        "continuous_monitor_active": bool(monitor_running),
        "continuous_unrealized_pnl_tracking_active": bool(monitor_running),
        "continuous_realized_pnl_tracking_active": bool(monitor_running),
        "persistent_strategy_position_ledger": bool(latest_cycle_status.get("persistent_strategy_position_ledger")),
        "monitoring_scope": "polling_runtime",
        "cycles": cycles,
        "broker_ledger_match": "MATCH" if "ledger_broker_mismatch" not in block_reasons else "MISMATCH",
        "error_count": len(latest_errors) + (1 if latest_error else 0),
        "last_error": last_error,
        "detail": latest_error or latest_cycle_status.get("detail"),
    }


def _record_audit(
    audit_events: list[dict[str, Any]],
    event_type: str,
    detail: str,
    *,
    config: IbkrPaperStrategyMonitorConfig,
    extra: dict[str, Any] | None = None,
) -> None:
    row = {
        "event_type": event_type,
        "detail": detail,
        "recorded_at": _utc_now(),
        "mode": config.mode,
        "host": config.host,
        "port": config.port,
        "account_id": config.account_id,
    }
    if extra:
        row.update(extra)
    audit_events.append(row)


def _record_runtime_audit(
    audit_events: list[dict[str, Any]],
    event_type: str,
    detail: str,
    *,
    config: IbkrPaperStrategyMonitorDaemonConfig,
    extra: dict[str, Any] | None = None,
) -> None:
    row = {
        "event_type": event_type,
        "detail": detail,
        "recorded_at": _utc_now(),
        "mode": config.monitor_config.mode,
        "host": config.monitor_config.host,
        "port": config.monitor_config.port,
        "account_id": config.monitor_config.account_id,
        "poll_interval_seconds": float(config.poll_interval_seconds),
    }
    if extra:
        row.update(extra)
    audit_events.append(row)


def _write_live_runtime_files(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    runtime_status: dict[str, Any],
    latest_cycle: IbkrPaperStrategyMonitorArtifacts | None,
) -> None:
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_status_path = config.repo_root / config.runtime_status_path
    runtime_status_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_status_path.write_text(json.dumps(runtime_status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output_dir / _RUNTIME_STATUS_FILENAME).write_text(json.dumps(runtime_status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if latest_cycle is not None:
        ledger_path = config.repo_root / config.ledger_path
        pnl_path = config.repo_root / config.pnl_snapshot_path
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        pnl_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(json.dumps(latest_cycle.ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        pnl_path.write_text(json.dumps(latest_cycle.pnl_snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    heartbeat_payload = {
        "generated_at": _utc_now(),
        "monitor_running": bool(runtime_status.get("monitor_running")),
        "last_poll_time": runtime_status.get("last_poll_time"),
        "last_successful_broker_refresh": runtime_status.get("last_successful_broker_refresh"),
        "health_classification": runtime_status.get("health_classification"),
        "monitor_health": runtime_status.get("monitor_health"),
        "stale": runtime_status.get("stale"),
        "error_count": runtime_status.get("error_count"),
        "last_error": runtime_status.get("last_error"),
    }
    _write_runtime_heartbeat(config=config, payload=heartbeat_payload)


def _flush_runtime_audit_events(
    *,
    config: IbkrPaperStrategyMonitorConfig,
    runtime_audit_events: list[dict[str, Any]],
    written_count: int,
) -> int:
    new_rows = runtime_audit_events[written_count:]
    if not new_rows:
        return written_count
    runtime_audit_path = config.repo_root / config.runtime_audit_path
    runtime_audit_path.parent.mkdir(parents=True, exist_ok=True)
    with runtime_audit_path.open("a", encoding="utf-8") as handle:
        for row in new_rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / _RUNTIME_AUDIT_FILENAME).open("a", encoding="utf-8") as handle:
        for row in new_rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    return len(runtime_audit_events)


def _write_runtime_heartbeat(*, config: IbkrPaperStrategyMonitorConfig, payload: dict[str, Any]) -> None:
    heartbeat_path = config.repo_root / config.heartbeat_path
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    heartbeat_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    output_dir = config.repo_root / config.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "paper_strategy_monitor_heartbeat.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _latest_matching_execution_row(report: dict[str, Any]) -> dict[str, Any] | None:
    rows = list(((report.get("execution_truth") or {}).get("recent_matching_execution_rows")) or [])
    if not rows:
        rows = list(((report.get("execution_truth") or {}).get("matching_execution_rows")) or [])
    if not rows:
        return None
    rows.sort(key=lambda row: str(row.get("executed_at") or ""))
    return dict(rows[-1])


def _latest_portfolio_row(report: dict[str, Any]) -> dict[str, Any] | None:
    rows = list(((report.get("portfolio_update_summary") or {}).get("rows")) or [])
    if not rows:
        return None
    rows.sort(key=lambda row: str(row.get("updated_at") or ""))
    return dict(rows[-1])


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None
