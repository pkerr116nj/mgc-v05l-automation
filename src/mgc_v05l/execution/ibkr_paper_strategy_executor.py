"""Supervised IBKR paper strategy executor for the first ATP lane."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .ibkr_paper_strategy_monitor import load_paper_strategy_monitor_status
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


def run_ibkr_paper_strategy_executor(
    *,
    config: IbkrPaperStrategyExecutorConfig,
    close_runner: Callable[..., Any] = run_ibkr_unattended_paper_close,
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
                    quantity=float(config.quantity),
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
    ]
    delegated = dict(report.get("delegated_result") or {})
    if delegated:
        lines.append(f"- delegated classification: `{delegated.get('classification')}`")
    for row in list(report.get("preflight_checks") or []):
        if not row.get("passed"):
            lines.append(f"- blocked check: `{row.get('name')}` -> `{row.get('detail')}`")
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
        session = dashboard_gate.get("session_classification")
        return (
            "HOLD_LONG",
            "The strategy currently owns long 1.0 MGC and no authoritative exit signal is surfaced in the live monitored state, so the supervised executor continues holding the paper position."
            + (f" Current detected session is {session}." if session else ""),
        )
    if quantity == 0.0:
        return "NO_ACTION", "The strategy is flat and this first supervised executor pass is not allowed to open a new paper position autonomously."
    return "BLOCKED_NEEDS_REVIEW", "The supervised executor could not map the current ledger position into a safe FLAT/LONG state."


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
        "paper_strategy_monitor_status": monitor_status,
        "dashboard_gate": dashboard_gate,
        "strategy_position": strategy_position,
        "preflight_checks": preflight_checks,
        "delegated_result": delegated_result,
    }


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
