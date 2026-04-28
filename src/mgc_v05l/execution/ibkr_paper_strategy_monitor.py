"""Persistent IBKR paper strategy position and P&L monitor."""

from __future__ import annotations

import json
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
) -> IbkrPaperStrategyMonitorDaemonArtifacts:
    runtime_audit_events: list[dict[str, Any]] = []
    started_at = _utc_now()
    _record_runtime_audit(
        runtime_audit_events,
        "daemon_started",
        "Continuous paper strategy monitor polling started.",
        config=config,
        extra={"max_cycles": config.max_cycles, "poll_interval_seconds": config.poll_interval_seconds},
    )
    cycles: list[dict[str, Any]] = []
    latest_cycle: IbkrPaperStrategyMonitorArtifacts | None = None
    latest_error: str | None = None

    for index in range(max(1, int(config.max_cycles))):
        cycle_started_at = _utc_now()
        try:
            latest_cycle = cycle_runner(config=config.monitor_config)
            write_ibkr_paper_strategy_monitor_artifacts(config=config.monitor_config, artifacts=latest_cycle)
            cycles.append(
                {
                    "cycle_index": index + 1,
                    "started_at": cycle_started_at,
                    "finished_at": _utc_now(),
                    "classification": latest_cycle.classification,
                    "broker_position_quantity": latest_cycle.status.get("broker_position_quantity"),
                    "open_order_count": latest_cycle.status.get("open_order_count"),
                    "submit_allowed": latest_cycle.status.get("submit_allowed"),
                }
            )
            _record_runtime_audit(
                runtime_audit_events,
                "cycle_completed",
                "Paper strategy monitor cycle completed.",
                config=config,
                extra=cycles[-1],
            )
        except Exception as exc:
            latest_error = str(exc)
            cycles.append(
                {
                    "cycle_index": index + 1,
                    "started_at": cycle_started_at,
                    "finished_at": _utc_now(),
                    "classification": "PAPER_STRATEGY_MONITOR_DISCONNECTED",
                    "error": latest_error,
                }
            )
            _record_runtime_audit(
                runtime_audit_events,
                "cycle_failed",
                "Paper strategy monitor cycle failed closed.",
                config=config,
                extra=cycles[-1],
            )
            break
        if index + 1 < max(1, int(config.max_cycles)):
            sleep_fn(float(config.poll_interval_seconds))

    runtime_status = _build_runtime_status(
        config=config,
        started_at=started_at,
        cycles=cycles,
        latest_cycle=latest_cycle,
        latest_error=latest_error,
    )
    daemon_report = {
        "classification": runtime_status.get("classification"),
        "generated_at": _utc_now(),
        "runtime_status_path": str((config.monitor_config.repo_root / config.monitor_config.output_dir / _RUNTIME_STATUS_FILENAME).resolve()),
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
    with (output_dir / _RUNTIME_AUDIT_FILENAME).open("a", encoding="utf-8") as handle:
        for row in artifacts.runtime_audit_events:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")


def load_paper_strategy_monitor_status(*, repo_root: Path) -> dict[str, Any]:
    runtime_path = repo_root / _DEFAULT_OUTPUT_DIR / _RUNTIME_STATUS_FILENAME
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
        freshness_window = float(runtime_status.get("freshness_window_seconds") or 0.0)
        refreshed_at = _parse_datetime(runtime_status.get("last_broker_refresh_timestamp"))
        if freshness_window > 0.0 and refreshed_at is not None:
            age_seconds = max(0.0, (datetime.now(timezone.utc) - refreshed_at).total_seconds())
            runtime_status["age_seconds"] = age_seconds
            runtime_status["stale"] = age_seconds > freshness_window
            if runtime_status["stale"]:
                runtime_status["submit_allowed"] = False
                reasons = list(runtime_status.get("block_reasons") or [])
                if "paper_strategy_monitor_runtime_stale" not in reasons:
                    reasons.append("paper_strategy_monitor_runtime_stale")
                runtime_status["block_reasons"] = reasons
        elif freshness_window > 0.0:
            runtime_status["submit_allowed"] = False
            reasons = list(runtime_status.get("block_reasons") or [])
            if "paper_strategy_monitor_runtime_refresh_missing" not in reasons:
                reasons.append("paper_strategy_monitor_runtime_refresh_missing")
            runtime_status["block_reasons"] = reasons
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
    return {
        "classification": "PAPER_STRATEGY_MONITOR_BLOCKED",
        "submit_allowed": False,
        "block_reasons": ["paper_strategy_monitor_runtime_status_missing", "paper_strategy_monitor_snapshot_only"],
        "detail": "Paper strategy monitor daemon runtime status is missing; snapshot-only monitor output cannot authorize new paper orders.",
        "snapshot_status": snapshot_payload,
    }


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
    bridge_strategy_matches = bridge_intent.get("strategy_id") == config.strategy_id
    bridge_buy_one_matches = bridge_intent.get("action") == "BUY" and float(bridge_intent.get("quantity") or 0.0) == 1.0
    snapshot_matches = (
        strategy_snapshot.get("strategy_id") == config.strategy_id
        and strategy_snapshot.get("latest_matching_perm_id") == latest_perm_id
        and float(strategy_snapshot.get("current_reconciled_quantity") or 0.0) == broker_quantity
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
    previously_adopted = bool(existing_positions) and bool((existing_positions[0] or {}).get("strategy_id"))

    side = "FLAT"
    state = "FLAT"
    if broker_quantity > 0:
        side = "LONG"
        state = "OPEN"
    elif broker_quantity < 0:
        side = "SHORT"
        state = "NEEDS_REVIEW"

    position_row = {
        "strategy_id": ownership.get("strategy_id"),
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
        "average_entry_price": None if latest_exec is None else latest_exec.get("price"),
        "average_cost_basis": None if portfolio_row is None else portfolio_row.get("average_cost"),
        "order_id": None if latest_exec is None else latest_exec.get("broker_order_id"),
        "perm_id": ownership.get("perm_id"),
        "execution_id": ownership.get("execution_id"),
        "entry_timestamp": None if latest_exec is None else latest_exec.get("executed_at"),
        "source_intent_id": ownership.get("source_intent_id"),
        "state": state if ownership.get("ownership_proven") else ("FLAT" if broker_quantity == 0.0 else "NEEDS_REVIEW"),
        "realized_pnl": None if portfolio_row is None else portfolio_row.get("realized_pnl"),
        "unrealized_pnl": None if portfolio_row is None else portfolio_row.get("unrealized_pnl"),
        "last_reconciliation_timestamp": now,
        "pnl_source": "ibkr_updatePortfolio" if portfolio_row is not None else "ibkr_account_snapshot",
        "ownership_detail": ownership.get("detail"),
        "adopted_from_broker_truth": bool(ownership.get("ownership_proven")),
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
    mismatch = bool(previous_positions) and previous_quantity != broker_quantity
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


def _build_runtime_status(
    *,
    config: IbkrPaperStrategyMonitorDaemonConfig,
    started_at: str,
    cycles: list[dict[str, Any]],
    latest_cycle: IbkrPaperStrategyMonitorArtifacts | None,
    latest_error: str | None,
) -> dict[str, Any]:
    latest_cycle_status = {} if latest_cycle is None else dict(latest_cycle.status)
    latest_cycle_pnl = {} if latest_cycle is None else dict(latest_cycle.pnl_snapshot)
    last_refresh_timestamp = None
    if latest_cycle is not None:
        last_refresh_timestamp = latest_cycle_status.get("generated_at") or latest_cycle_pnl.get("generated_at")
    block_reasons = list(latest_cycle_status.get("block_reasons") or [])
    classification = "PAPER_STRATEGY_MONITOR_BLOCKED"
    monitor_health = "UNAVAILABLE"
    if latest_error is not None:
        classification = "PAPER_STRATEGY_MONITOR_DISCONNECTED"
        monitor_health = "DISCONNECTED"
        if "monitor_disconnected" not in block_reasons:
            block_reasons.append("monitor_disconnected")
    elif latest_cycle is not None:
        if latest_cycle_status.get("ownership_proven") and not block_reasons:
            classification = "PAPER_STRATEGY_MONITOR_ACTIVE"
            monitor_health = "HEALTHY"
        elif block_reasons:
            classification = "PAPER_STRATEGY_MONITOR_PARTIAL"
            monitor_health = "DEGRADED"
        else:
            classification = "PAPER_STRATEGY_MONITOR_PARTIAL"
            monitor_health = "DEGRADED"

    return {
        "classification": classification,
        "generated_at": _utc_now(),
        "runtime_started_at": started_at,
        "runtime_finished_at": _utc_now(),
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
        "last_broker_refresh_timestamp": last_refresh_timestamp,
        "monitor_health": monitor_health,
        "stale": False,
        "pnl_source": latest_cycle_pnl.get("pnl_source"),
        "submit_allowed": bool(latest_cycle_status.get("submit_allowed")) and classification == "PAPER_STRATEGY_MONITOR_ACTIVE",
        "block_reasons": block_reasons,
        "continuous_monitor_active": True,
        "continuous_unrealized_pnl_tracking_active": classification == "PAPER_STRATEGY_MONITOR_ACTIVE",
        "continuous_realized_pnl_tracking_active": classification == "PAPER_STRATEGY_MONITOR_ACTIVE",
        "persistent_strategy_position_ledger": bool(latest_cycle_status.get("persistent_strategy_position_ledger")),
        "monitoring_scope": "polling_runtime",
        "cycles": cycles,
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
