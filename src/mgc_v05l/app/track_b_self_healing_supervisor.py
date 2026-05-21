"""CLI/status writer for Track B PAPER self-healing health.

The command is read-only with respect to broker/order/lifecycle state unless
``--apply`` is explicitly selected, and even then it only restarts approved
sidecar/support services.  It never restarts the PAPER runtime and never calls
broker/order/lifecycle mutation APIs.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT,
    REPO_ROOT,
)
from mgc_v05l.execution_core.track_b_self_healing_supervisor import (
    DEFAULT_SELF_HEALING_HEALTH_ARTIFACT,
    build_track_b_self_healing_health,
    write_track_b_self_healing_health,
)

DEFAULT_RESTART_AUDIT_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "self_healing_recovery_audit.jsonl"
)
DEFAULT_RESTART_COOLDOWN_SECONDS = 300.0
DEFAULT_RESTART_MAX_ATTEMPTS = 3
DEFAULT_RESTART_WINDOW_SECONDS = 900.0
DEFAULT_RESTART_COMMAND_TIMEOUT_SECONDS = 180.0
AUTO_RESTART_ELIGIBLE = "AUTO_RESTART_ELIGIBLE"
PAPER_RUNTIME_AGENT_ID = "paper_runtime"
ACTIVE_BROKER_LEASE_STATES = {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
PAPER_RUNTIME_CONFIG_STACK = (
    "config/base.yaml",
    "config/live.yaml",
    "config/probationary_pattern_engine.yaml",
    "config/headless_supervised_paper_runtime.yaml",
    "config/probationary_pattern_engine_paper.yaml",
    "config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml",
)
RETRYABLE_PAPER_LAUNCH_FAILURES = {
    "LAUNCHCTL_SUBMIT_FAILED",
    "RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT",
    "WRAPPER_PRE_EXEC_FAILURE",
}
IMMEDIATE_RETRY_PAPER_LAUNCH_FAILURES = {"LAUNCHCTL_SUBMIT_FAILED"}
DEFAULT_PAPER_LAUNCH_STATUS_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper_launch_status.json"
)

RECOVERY_RESTART_COMMANDS: dict[str, tuple[tuple[str, ...], ...]] = {
    "broker_truth_refresher": (
        ("bash", "scripts/stop-track-b-broker-truth-refresh"),
        ("bash", "scripts/start-track-b-broker-truth-refresh"),
    ),
    "operator_readiness_refresher": (
        ("bash", "scripts/stop-track-b-operator-readiness-refresh"),
        ("bash", "scripts/start-track-b-operator-readiness-refresh"),
    ),
    "phase1_candle_supervisor": (
        ("bash", "scripts/stop-phase1-databento-live-candles"),
        ("bash", "scripts/start-phase1-databento-live-candles"),
    ),
    "paper_runtime": (
        ("bash", "scripts/stop_probationary_paper_soak.sh"),
        (
            "env",
            "MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS="
            + ":".join(str(REPO_ROOT / item) for item in PAPER_RUNTIME_CONFIG_STACK),
            "MGC_HEADLESS_REQUIRED_PAPER_CONFIGS="
            + ":".join(str(REPO_ROOT / item) for item in PAPER_RUNTIME_CONFIG_STACK),
            "bash",
            "scripts/run_headless_supervised_paper_service.sh",
            "--no-start-dashboard",
            "--wait-timeout-seconds",
            "120",
            "--post-start-pid-wait-timeout-seconds",
            "45",
        ),
    ),
}


def run_track_b_self_healing_status(
    *,
    repo_root: Path = REPO_ROOT,
    expected_root: Path | None = None,
    output_path: Path | None = None,
    audit_path: Path | None = None,
    write: bool = True,
    mode: str = "dry-run",
    process_probe=None,
    now: datetime | None = None,
    command_runner: Callable[[Sequence[str], Path], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.expanduser().resolve()
    expected_root = (expected_root or DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT).expanduser().resolve()
    current = _ensure_utc(now or datetime.now(timezone.utc))
    health = build_track_b_self_healing_health(
        repo_root=repo_root,
        expected_root=expected_root,
        process_probe=process_probe,
        now=current,
    )
    target = output_path or (repo_root / DEFAULT_SELF_HEALING_HEALTH_ARTIFACT)
    audit_target = audit_path or (repo_root / DEFAULT_RESTART_AUDIT_ARTIFACT)
    action = evaluate_track_b_self_healing_restart_action(
        health=health,
        repo_root=repo_root,
        mode=mode,
        audit_path=audit_target,
        now=current,
        command_runner=command_runner,
    )
    runtime_restart = _runtime_restart_summary(action["plan"], action.get("attempt"))
    health = {
        **health,
        "health_contract_artifact_path": str(target),
        "restart_audit_artifact_path": str(audit_target),
        "recovery_audit_artifact_path": str(audit_target),
        "last_restart_plan": action["plan"],
        "recovery_classification": action["plan"].get("classification"),
        "runtime_restart_eligible": runtime_restart["eligible"],
        "runtime_restart_blockers": runtime_restart["blockers"],
        "runtime_restart_cooldown_state": runtime_restart["cooldown_state"],
        "retry_policy_classification": action["plan"].get("retry_policy_classification"),
        "retry_policy": action["plan"].get("retry_policy"),
    }
    if action.get("attempt"):
        health = {**health, "last_restart_attempt": action["attempt"]}
    if runtime_restart.get("last_attempt"):
        health = {**health, "last_runtime_restart_attempt": runtime_restart["last_attempt"]}
    if write:
        write_track_b_self_healing_health(output_path=target, health=health)
    return health


def evaluate_track_b_self_healing_restart_action(
    *,
    health: Mapping[str, Any],
    repo_root: Path,
    mode: str,
    audit_path: Path,
    now: datetime | None = None,
    command_runner: Callable[[Sequence[str], Path], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    current = _ensure_utc(now or datetime.now(timezone.utc))
    restart_mode = _normalize_mode(mode)
    audit_entries = _read_audit_entries(audit_path)
    plan = plan_track_b_self_healing_restarts(
        health=health,
        audit_entries=audit_entries,
        now=current,
    )
    if restart_mode == "dry-run":
        return {"mode": restart_mode, "plan": plan, "attempt": None}
    attempt = _apply_restart_plan(
        plan=plan,
        repo_root=repo_root,
        audit_path=audit_path,
        now=current,
        command_runner=command_runner or _run_restart_command,
    )
    return {"mode": restart_mode, "plan": plan, "attempt": attempt}


def plan_track_b_self_healing_restarts(
    *,
    health: Mapping[str, Any],
    audit_entries: Sequence[Mapping[str, Any]] = (),
    now: datetime | None = None,
    cooldown_seconds: float = DEFAULT_RESTART_COOLDOWN_SECONDS,
    max_attempts: int = DEFAULT_RESTART_MAX_ATTEMPTS,
    window_seconds: float = DEFAULT_RESTART_WINDOW_SECONDS,
) -> dict[str, Any]:
    current = _ensure_utc(now or datetime.now(timezone.utc))
    classification = str(health.get("classification") or "")
    agents = health.get("agents") if isinstance(health.get("agents"), Mapping) else {}
    restart_candidates = list(health.get("restart_candidates") or [])
    health_blockers = list(health.get("blockers") or [])
    actions: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    retry_policy_rows: dict[str, Any] = {}

    global_blockers = _global_restart_blockers(health)
    if classification != AUTO_RESTART_ELIGIBLE:
        global_blockers.append(f"classification_not_{AUTO_RESTART_ELIGIBLE}")
    candidate_blockers = _candidate_agent_blockers(agents=agents, restart_candidates=restart_candidates)
    if health_blockers:
        global_blockers.extend(
            str(item) for item in health_blockers if str(item) not in candidate_blockers
        )

    for agent_id in restart_candidates:
        row = agents.get(agent_id) if isinstance(agents.get(agent_id), Mapping) else {}
        agent_blockers = _agent_restart_blockers(
            agent_id=agent_id,
            agent=row,
            audit_entries=audit_entries,
            now=current,
            cooldown_seconds=cooldown_seconds,
            max_attempts=max_attempts,
            window_seconds=window_seconds,
        )
        retry_policy = _agent_retry_policy(
            agent_id=agent_id,
            audit_entries=audit_entries,
            now=current,
            cooldown_seconds=cooldown_seconds,
            max_attempts=max_attempts,
            window_seconds=window_seconds,
        )
        retry_policy_rows[agent_id] = retry_policy
        if agent_id == PAPER_RUNTIME_AGENT_ID:
            agent_blockers.extend(_runtime_restart_blockers(health=health, agent=row))
            agent_blockers.extend(str(item) for item in retry_policy.get("blockers") or [])
        all_blockers = _dedupe([*global_blockers, *agent_blockers])
        commands = RECOVERY_RESTART_COMMANDS.get(agent_id)
        if all_blockers or not commands:
            if not commands:
                all_blockers.append("restart_command_not_supported")
            blocked.append({"agent_id": agent_id, "blockers": tuple(_dedupe(all_blockers))})
            continue
        actions.append(
            {
                "agent_id": agent_id,
                "display_name": row.get("display_name") or agent_id,
                "reason": tuple(row.get("blockers") or ("sidecar_unhealthy",)),
                "commands": tuple(tuple(command) for command in commands),
                "retry_policy": retry_policy,
            }
        )

    plan_classification = "RESTART_PLAN_READY" if actions and not blocked else "RESTART_PLAN_BLOCKED"
    if not restart_candidates:
        plan_classification = "RESTART_PLAN_NO_ACTION"
    return {
        "schema_version": "track_b_self_healing_restart_plan_v1",
        "generated_at": current.isoformat(),
        "mode_supported": ("dry-run", "apply"),
        "classification": plan_classification,
        "health_classification": classification,
        "restart_allowed": bool(actions and not blocked and not global_blockers),
        "actions": tuple(actions),
        "blocked": tuple(blocked),
        "global_blockers": tuple(_dedupe(global_blockers)),
        "paper_runtime_auto_restart_allowed": any(row.get("agent_id") == PAPER_RUNTIME_AGENT_ID for row in actions),
        "retry_policy": retry_policy_rows,
        "retry_policy_classification": _plan_retry_policy_classification(retry_policy_rows),
        "cooldown_seconds": cooldown_seconds,
        "max_attempts": max_attempts,
        "window_seconds": window_seconds,
    }


def render_track_b_self_healing_status(health: Mapping[str, Any]) -> str:
    agents = health.get("agents") if isinstance(health.get("agents"), Mapping) else {}
    plan = health.get("last_restart_plan") if isinstance(health.get("last_restart_plan"), Mapping) else {}
    attempt = health.get("last_restart_attempt") if isinstance(health.get("last_restart_attempt"), Mapping) else {}
    lines = [
        f"classification={health.get('classification') or 'UNKNOWN'}",
        f"auto_restart_allowed={str(health.get('auto_restart_allowed') is True).lower()}",
        f"restart_candidates={_csv(health.get('restart_candidates'))}",
        f"operator_required_agents={_csv(health.get('operator_required_agents'))}",
        f"blockers={_csv(health.get('blockers'))}",
        f"warnings={_csv(health.get('warnings'))}",
        f"live_money_eligible={str(health.get('live_money_eligible') is True).lower()}",
        f"artifact_path={health.get('health_contract_artifact_path') or DEFAULT_SELF_HEALING_HEALTH_ARTIFACT}",
        f"audit_path={health.get('restart_audit_artifact_path') or DEFAULT_RESTART_AUDIT_ARTIFACT}",
        f"recovery_classification={health.get('recovery_classification') or plan.get('classification') or 'RESTART_PLAN_NOT_EVALUATED'}",
        f"runtime_restart_eligible={str(health.get('runtime_restart_eligible') is True).lower()}",
        f"runtime_restart_blockers={_csv(health.get('runtime_restart_blockers'))}",
        f"runtime_restart_cooldown={health.get('runtime_restart_cooldown_state') or '-'}",
        f"restart_plan={plan.get('classification') or 'RESTART_PLAN_NOT_EVALUATED'}",
        f"restart_plan_actions={_csv([row.get('agent_id') for row in plan.get('actions', [])])}",
        f"restart_plan_blocked={_csv([row.get('agent_id') for row in plan.get('blocked', [])])}",
        f"last_restart_attempt={attempt.get('classification') or 'none'}",
        "agents:",
    ]
    for agent_id in sorted(agents):
        row = agents.get(agent_id) if isinstance(agents.get(agent_id), Mapping) else {}
        lines.append(
            "  "
            + str(agent_id)
            + f": health={row.get('health_state') or '-'}"
            + f" running={str(row.get('process_running') is True).lower()}"
            + f" restart_eligible={str(row.get('restart_eligible') is True).lower()}"
            + f" restart_candidate={str(row.get('restart_candidate') is True).lower()}"
            + f" blockers={_csv(row.get('blockers'))}"
            + f" restart_blockers={_csv(row.get('restart_blockers'))}"
        )
    return "\n".join(lines) + "\n"


def _apply_restart_plan(
    *,
    plan: Mapping[str, Any],
    repo_root: Path,
    audit_path: Path,
    now: datetime,
    command_runner: Callable[[Sequence[str], Path], Mapping[str, Any]],
) -> dict[str, Any]:
    if plan.get("restart_allowed") is not True:
        attempt = {
            "schema_version": "track_b_self_healing_restart_attempt_v1",
            "generated_at": now.isoformat(),
            "classification": "RESTART_NOT_ALLOWED",
            "plan_classification": plan.get("classification"),
            "global_blockers": list(plan.get("global_blockers") or []),
            "results": [],
        }
        _append_audit(audit_path, attempt)
        return attempt
    results: list[dict[str, Any]] = []
    for action in plan.get("actions") or []:
        agent_id = str(action.get("agent_id") or "")
        command_results = []
        agent_ok = True
        for command in action.get("commands") or []:
            result = dict(command_runner(tuple(command), repo_root))
            command_results.append(result)
            if int(result.get("returncode") or 0) != 0 and not _nonfatal_restart_command_result(agent_id, tuple(command), result):
                agent_ok = False
                break
        results.append(
            {
                "agent_id": agent_id,
                "classification": "RESTART_SUCCEEDED" if agent_ok else "RESTART_FAILED",
                "failure_class": _command_failure_class(command_results),
                "commands": command_results,
            }
        )
    attempt_classification = "RESTART_APPLIED" if all(row["classification"] == "RESTART_SUCCEEDED" for row in results) else "RESTART_PARTIAL_FAILURE"
    attempt = {
        "schema_version": "track_b_self_healing_restart_attempt_v1",
        "generated_at": now.isoformat(),
        "classification": attempt_classification,
        "plan_classification": plan.get("classification"),
        "results": results,
    }
    _append_audit(audit_path, attempt)
    return attempt


def _global_restart_blockers(health: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    broker_safety = health.get("broker_safety") if isinstance(health.get("broker_safety"), Mapping) else {}
    if _truthy(health.get("live_money_eligible")) or _truthy(broker_safety.get("live_money_eligible")):
        blockers.append("live_money_eligible_true")
    if int(broker_safety.get("unknown_open_order_count") or 0):
        blockers.append("unknown_open_orders")
    if int(broker_safety.get("track_b_broker_open_order_count") or 0):
        blockers.append("open_orders_present")
    if int(broker_safety.get("unknown_broker_open_order_count") or 0):
        blockers.append("unknown_open_orders")
    if int(broker_safety.get("review_required_count") or 0):
        blockers.append("lifecycle_review_required")
    if int(broker_safety.get("unresolved_submit_intent_ownership_count") or 0):
        blockers.append("unresolved_submit_ownership")
    if broker_safety.get("broker_reconciled") is False:
        blockers.append("broker_reconciliation_mismatch")
    if broker_safety.get("duplicate_conflicting_runtime_count"):
        blockers.append("duplicate_conflicting_runtimes")
    agents = health.get("agents") if isinstance(health.get("agents"), Mapping) else {}
    for row in agents.values():
        if isinstance(row, Mapping) and row.get("root_ok") is False:
            blockers.append("wrong_root")
    return _dedupe(blockers)


def _runtime_restart_blockers(*, health: Mapping[str, Any], agent: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    broker_safety = health.get("broker_safety") if isinstance(health.get("broker_safety"), Mapping) else {}
    lease_state = str(broker_safety.get("broker_truth_lease_state") or "").strip().upper()
    if lease_state and lease_state not in ACTIVE_BROKER_LEASE_STATES:
        blockers.append("broker_truth_lease_not_active")
    if not lease_state:
        blockers.append("broker_truth_lease_state_missing")
    if str(broker_safety.get("classification") or "").strip().upper() != "TRACK_B_PAPER_BROKER_RECONCILED":
        blockers.append("broker_reconciliation_mismatch")
    if broker_safety.get("broker_reconciled") is not True:
        blockers.append("broker_reconciliation_mismatch")
    agents = health.get("agents") if isinstance(health.get("agents"), Mapping) else {}
    phase1 = agents.get("phase1_candle_supervisor") if isinstance(agents.get("phase1_candle_supervisor"), Mapping) else {}
    if phase1.get("health_state") != "HEALTHY":
        blockers.append("phase1_not_healthy_for_runtime_restart")
    if agent.get("process_running") is True and agent.get("health_state") == "HEALTHY":
        blockers.append("paper_runtime_not_dead_or_stale")
    return _dedupe(blockers)


def _candidate_agent_blockers(*, agents: Mapping[str, Any], restart_candidates: Sequence[str]) -> set[str]:
    blockers: set[str] = set()
    for agent_id in restart_candidates:
        row = agents.get(agent_id) if isinstance(agents.get(agent_id), Mapping) else {}
        blockers.update(str(item) for item in row.get("blockers") or [])
    return blockers


def _runtime_restart_summary(plan: Mapping[str, Any], attempt: Mapping[str, Any] | None) -> dict[str, Any]:
    eligible = any(row.get("agent_id") == PAPER_RUNTIME_AGENT_ID for row in plan.get("actions") or [])
    blockers: list[str] = []
    for row in plan.get("blocked") or []:
        if isinstance(row, Mapping) and row.get("agent_id") == PAPER_RUNTIME_AGENT_ID:
            blockers.extend(str(item) for item in row.get("blockers") or [])
    if not eligible and not blockers and plan.get("health_classification") == AUTO_RESTART_ELIGIBLE:
        blockers.append("paper_runtime_not_restart_candidate")
    cooldown_state = "ACTIVE" if "restart_cooldown_active" in blockers else "CLEAR"
    last_attempt = None
    if isinstance(attempt, Mapping):
        for row in attempt.get("results") or []:
            if isinstance(row, Mapping) and row.get("agent_id") == PAPER_RUNTIME_AGENT_ID:
                last_attempt = row
                break
    return {
        "eligible": eligible,
        "blockers": tuple(_dedupe(blockers)),
        "cooldown_state": cooldown_state,
        "last_attempt": last_attempt,
    }


def _agent_restart_blockers(
    *,
    agent_id: str,
    agent: Mapping[str, Any],
    audit_entries: Sequence[Mapping[str, Any]],
    now: datetime,
    cooldown_seconds: float,
    max_attempts: int,
    window_seconds: float,
) -> list[str]:
    blockers: list[str] = []
    if agent.get("restart_eligible") is not True:
        blockers.append("agent_restart_not_eligible")
    if agent.get("restart_candidate") is not True:
        blockers.append("agent_not_restart_candidate")
    if agent.get("operator_required") is True:
        blockers.append("operator_required")
    recent = _recent_agent_attempts(audit_entries=audit_entries, agent_id=agent_id, now=now, window_seconds=window_seconds)
    if agent_id == PAPER_RUNTIME_AGENT_ID:
        # Paper runtime launch failures have their own class-aware progressive
        # retry policy. Generic cooldown still applies to other agents.
        return _dedupe(blockers)
    if recent:
        last_ts = _parse_datetime(recent[-1].get("generated_at"))
        if last_ts is not None and (now - last_ts).total_seconds() < cooldown_seconds:
            blockers.append("restart_cooldown_active")
    if len(recent) >= max_attempts:
        blockers.append("restart_max_attempts_exceeded")
    return _dedupe(blockers)


def _agent_retry_policy(
    *,
    agent_id: str,
    audit_entries: Sequence[Mapping[str, Any]],
    now: datetime,
    cooldown_seconds: float,
    max_attempts: int,
    window_seconds: float,
) -> dict[str, Any]:
    if agent_id != PAPER_RUNTIME_AGENT_ID:
        return {
            "classification": "RESTART_ALLOWED",
            "retry_attempt_count": len(
                _recent_agent_attempts(
                    audit_entries=audit_entries,
                    agent_id=agent_id,
                    now=now,
                    window_seconds=window_seconds,
                )
            ),
            "cooldown_state": "CLEAR",
            "next_retry_allowed_at": None,
            "budget_exhausted": False,
            "last_attempt_result": None,
            "blockers": (),
        }
    recent_results = _recent_agent_results(
        audit_entries=audit_entries,
        agent_id=agent_id,
        now=now,
        window_seconds=window_seconds,
    )
    last_result = recent_results[-1] if recent_results else None
    failure_class = _restart_result_failure_class(last_result) if last_result else None
    same_class_count = sum(1 for row in recent_results if _restart_result_failure_class(row) == failure_class) if failure_class else 0
    cooldown_until = None
    cooldown_active = False
    blockers: list[str] = []

    if not last_result or str(last_result.get("classification") or "") == "RESTART_SUCCEEDED":
        classification = "RESTART_ALLOWED"
    elif failure_class not in RETRYABLE_PAPER_LAUNCH_FAILURES:
        classification = "RESTART_BLOCKED_NON_RETRYABLE_FAILURE"
        blockers.append("paper_runtime_last_failure_not_retryable")
    elif same_class_count >= max_attempts:
        classification = "RESTART_BUDGET_EXHAUSTED"
        blockers.append("restart_max_attempts_exceeded")
    else:
        last_ts = _parse_datetime(last_result.get("generated_at"))
        if failure_class in IMMEDIATE_RETRY_PAPER_LAUNCH_FAILURES and same_class_count == 1:
            classification = "RESTART_RETRY_IMMEDIATE"
        elif last_ts is not None and (now - last_ts).total_seconds() < cooldown_seconds:
            cooldown_until = last_ts + timedelta(seconds=cooldown_seconds)
            cooldown_active = True
            classification = "RESTART_COOLDOWN_ACTIVE"
            blockers.append("restart_cooldown_active")
        else:
            classification = "RESTART_RETRY_AFTER_COOLDOWN"

    return {
        "classification": classification,
        "retry_attempt_count": len(recent_results),
        "failure_class": failure_class,
        "failure_class_attempt_count": same_class_count,
        "restart_window_seconds": window_seconds,
        "max_attempts_per_class": max_attempts,
        "cooldown_state": "ACTIVE" if cooldown_active else "CLEAR",
        "cooldown_until": cooldown_until.isoformat() if cooldown_until else None,
        "next_retry_allowed_at": cooldown_until.isoformat() if cooldown_until else now.isoformat(),
        "budget_exhausted": classification == "RESTART_BUDGET_EXHAUSTED",
        "last_attempt_result": dict(last_result) if isinstance(last_result, Mapping) else None,
        "blockers": tuple(_dedupe(blockers)),
    }


def _recent_agent_results(
    *,
    audit_entries: Sequence[Mapping[str, Any]],
    agent_id: str,
    now: datetime,
    window_seconds: float,
) -> list[dict[str, Any]]:
    cutoff = now - timedelta(seconds=window_seconds)
    recent: list[dict[str, Any]] = []
    for entry in audit_entries:
        ts = _parse_datetime(entry.get("generated_at"))
        if ts is None or ts < cutoff:
            continue
        for result in entry.get("results") or []:
            if isinstance(result, Mapping) and result.get("agent_id") == agent_id:
                if _audit_result_is_nonfatal_stop_only(agent_id, result):
                    continue
                recent.append({"generated_at": entry.get("generated_at"), **dict(result)})
                break
    return recent


def _restart_result_failure_class(result: Mapping[str, Any] | None) -> str | None:
    if not isinstance(result, Mapping):
        return None
    if str(result.get("classification") or "") == "RESTART_SUCCEEDED":
        return None
    explicit = str(result.get("failure_class") or "").strip()
    if explicit:
        return explicit
    for command in result.get("commands") or []:
        if not isinstance(command, Mapping):
            continue
        classification = str(command.get("launch_status_classification") or "").strip()
        if classification:
            return classification
    return str(result.get("classification") or "").strip() or None


def _command_failure_class(command_results: Sequence[Mapping[str, Any]]) -> str | None:
    for result in reversed(list(command_results)):
        classification = str(result.get("launch_status_classification") or "").strip()
        if classification:
            return classification
    for result in reversed(list(command_results)):
        if int(result.get("returncode") or 0) != 0:
            return str(result.get("classification") or "COMMAND_FAILED")
    return None


def _nonfatal_restart_command_result(agent_id: str, command: Sequence[str], result: Mapping[str, Any]) -> bool:
    if agent_id != PAPER_RUNTIME_AGENT_ID:
        return False
    if not any(str(part).endswith("stop_probationary_paper_soak.sh") for part in command):
        return False
    text = f"{result.get('stdout_tail') or ''}\n{result.get('stderr_tail') or ''}"
    return "No probationary paper PID file found" in text


def _audit_result_is_nonfatal_stop_only(agent_id: str, result: Mapping[str, Any]) -> bool:
    if agent_id != PAPER_RUNTIME_AGENT_ID:
        return False
    commands = [row for row in result.get("commands") or [] if isinstance(row, Mapping)]
    if len(commands) != 1:
        return False
    command = commands[0].get("command") if isinstance(commands[0].get("command"), Sequence) else ()
    return _nonfatal_restart_command_result(agent_id, tuple(str(part) for part in command), commands[0])


def _plan_retry_policy_classification(rows: Mapping[str, Any]) -> str:
    if not rows:
        return "RESTART_ALLOWED"
    if any(_truthy(row.get("budget_exhausted")) for row in rows.values() if isinstance(row, Mapping)):
        return "RESTART_BUDGET_EXHAUSTED"
    if any(str(row.get("classification") or "") == "RESTART_COOLDOWN_ACTIVE" for row in rows.values() if isinstance(row, Mapping)):
        return "RESTART_COOLDOWN_ACTIVE"
    if any(str(row.get("classification") or "").startswith("RESTART_BLOCKED") for row in rows.values() if isinstance(row, Mapping)):
        return "RESTART_BLOCKED"
    if any(str(row.get("classification") or "") == "RESTART_RETRY_IMMEDIATE" for row in rows.values() if isinstance(row, Mapping)):
        return "RESTART_RETRY_IMMEDIATE"
    if any(str(row.get("classification") or "") == "RESTART_RETRY_AFTER_COOLDOWN" for row in rows.values() if isinstance(row, Mapping)):
        return "RESTART_RETRY_AFTER_COOLDOWN"
    return "RESTART_ALLOWED"


def _recent_agent_attempts(
    *,
    audit_entries: Sequence[Mapping[str, Any]],
    agent_id: str,
    now: datetime,
    window_seconds: float,
) -> list[Mapping[str, Any]]:
    cutoff = now - timedelta(seconds=window_seconds)
    recent = []
    for entry in audit_entries:
        ts = _parse_datetime(entry.get("generated_at"))
        if ts is None or ts < cutoff:
            continue
        for result in entry.get("results") or []:
            if isinstance(result, Mapping) and result.get("agent_id") == agent_id:
                recent.append(entry)
                break
    return recent


def _read_audit_entries(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    entries = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            entries.append(payload)
    return entries


def _append_audit(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")


def _run_restart_command(command: Sequence[str], repo_root: Path) -> dict[str, Any]:
    try:
        proc = subprocess.run(
            list(command),
            cwd=str(repo_root),
            check=False,
            capture_output=True,
            text=True,
            timeout=DEFAULT_RESTART_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        result = {
            "command": list(command),
            "returncode": 124,
            "classification": "RESTART_COMMAND_TIMEOUT",
            "stdout_tail": str(exc.stdout or "")[-2000:],
            "stderr_tail": str(exc.stderr or "")[-2000:],
            "timeout_seconds": DEFAULT_RESTART_COMMAND_TIMEOUT_SECONDS,
        }
        if _is_paper_runtime_start_command(command):
            launch_status = _read_json(repo_root / DEFAULT_PAPER_LAUNCH_STATUS_ARTIFACT)
            if launch_status:
                result["launch_status_classification"] = launch_status.get("classification")
                result["launch_status_artifact"] = str(repo_root / DEFAULT_PAPER_LAUNCH_STATUS_ARTIFACT)
                result["launchctl_exit_code"] = launch_status.get("launchctl_exit_code")
        return result
    result = {
        "command": list(command),
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-2000:],
        "stderr_tail": proc.stderr[-2000:],
    }
    if _is_paper_runtime_start_command(command):
        launch_status = _read_json(repo_root / DEFAULT_PAPER_LAUNCH_STATUS_ARTIFACT)
        if launch_status:
            result["launch_status_classification"] = launch_status.get("classification")
            result["launch_status_artifact"] = str(repo_root / DEFAULT_PAPER_LAUNCH_STATUS_ARTIFACT)
            result["launchctl_exit_code"] = launch_status.get("launchctl_exit_code")
    return result


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _is_paper_runtime_start_command(command: Sequence[str]) -> bool:
    return any(str(part).endswith("run_headless_supervised_paper_service.sh") for part in command)


def _normalize_mode(value: str) -> str:
    mode = str(value or "dry-run").strip().lower()
    if mode in {"dryrun", "dry_run"}:
        return "dry-run"
    if mode not in {"dry-run", "apply"}:
        raise ValueError(f"Unsupported self-healing restart mode: {value}")
    return mode


def _csv(value: object) -> str:
    if isinstance(value, (list, tuple)):
        return ",".join(str(item) for item in value) if value else "none"
    if value in (None, ""):
        return "none"
    return str(value)


def _truthy(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Write/read Track B self-healing health status.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--expected-root", type=Path, default=None)
    parser.add_argument("--output-path", type=Path, default=None)
    parser.add_argument("--audit-path", type=Path, default=None)
    parser.add_argument("--write", action="store_true", default=False)
    parser.add_argument("--no-write", action="store_false", dest="write")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_const", const="dry-run", dest="mode")
    mode.add_argument("--apply", action="store_const", const="apply", dest="mode")
    parser.set_defaults(mode="dry-run")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    health = run_track_b_self_healing_status(
        repo_root=args.repo_root,
        expected_root=args.expected_root,
        output_path=args.output_path,
        audit_path=args.audit_path,
        write=args.write,
        mode=args.mode,
    )
    if args.json:
        print(json.dumps(health, indent=2, sort_keys=True))
    else:
        print(render_track_b_self_healing_status(health), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
