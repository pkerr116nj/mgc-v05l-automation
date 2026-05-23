"""Read-only Track B PAPER self-healing health contract.

This first slice defines the supervised agents and classifies whether a future
self-healing supervisor may even consider restart actions.  It never restarts a
process, touches broker/order APIs, mutates lifecycle, or grants live-money
eligibility.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_readiness_authority import (
    PHASE1_RECONCILIATION_ARTIFACT,
    RECONCILED_CLASSIFICATION,
)
from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT,
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
    DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT,
)
from mgc_v05l.execution_core.track_b_shared_truth_refresh_cli import (
    TrackBSharedTruthRefreshConfig,
    build_runtime_start_preflight_summary,
    refresh_track_b_shared_truth,
)

DEFAULT_SELF_HEALING_HEALTH_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_self_healing_health.json"
)

SELF_HEALING_CLASSIFICATIONS = {
    "SELF_HEALING_READY",
    "DEGRADED_RECOVERABLE",
    "AUTO_RESTART_ELIGIBLE",
    "AUTO_RESTART_BLOCKED",
    "OPERATOR_REQUIRED",
    "UNSAFE_BROKER_STATE",
}

RESTART_ALLOWED = "RESTART_ALLOWED"
RESTART_COOLDOWN_ACTIVE = "RESTART_COOLDOWN_ACTIVE"
RESTART_BUDGET_EXHAUSTED = "RESTART_BUDGET_EXHAUSTED"
RESTART_BLOCKED_DUPLICATE_WRITER = "RESTART_BLOCKED_DUPLICATE_WRITER"
RESTART_BLOCKED_RECONCILIATION = "RESTART_BLOCKED_RECONCILIATION"
RESTART_NOT_NEEDED_HEALTHY = "RESTART_NOT_NEEDED_HEALTHY"
RESTART_ALLOWED_CLEAN = "RESTART_ALLOWED_CLEAN"
RESTART_BLOCKED_POSITION_TRUTH = "RESTART_BLOCKED_POSITION_TRUTH"
RESTART_BLOCKED_OPEN_ORDER_TRUTH = "RESTART_BLOCKED_OPEN_ORDER_TRUTH"
RESTART_BLOCKED_MANAGED_ORDER_TRUTH = "RESTART_BLOCKED_MANAGED_ORDER_TRUTH"
RESTART_BLOCKED_MANAGED_POSITION_TRUTH = "RESTART_BLOCKED_MANAGED_POSITION_TRUTH"
RESTART_BLOCKED_RUNTIME_TRUTH = "RESTART_BLOCKED_RUNTIME_TRUTH"

DEFAULT_RESTART_WINDOW_SECONDS = 900
DEFAULT_MAX_RESTART_ATTEMPTS = 3


@dataclass(frozen=True)
class ArtifactContract:
    label: str
    path: str
    freshness_ttl_seconds: float
    required: bool = True
    timestamp_fields: tuple[str, ...] = ("generated_at",)

    def as_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "path": self.path,
            "freshness_ttl_seconds": self.freshness_ttl_seconds,
            "required": self.required,
            "timestamp_fields": self.timestamp_fields,
        }


@dataclass(frozen=True)
class AgentContract:
    agent_id: str
    display_name: str
    expected_command_fragments: tuple[str, ...]
    pid_paths: tuple[str, ...]
    heartbeat_artifacts: tuple[ArtifactContract, ...]
    status_artifacts: tuple[ArtifactContract, ...]
    required: bool
    restart_eligible: bool
    restart_command: str | None
    restart_blockers: tuple[str, ...]
    operator_required_states: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "display_name": self.display_name,
            "expected_command_fragments": self.expected_command_fragments,
            "pid_paths": self.pid_paths,
            "heartbeat_artifacts": tuple(item.as_dict() for item in self.heartbeat_artifacts),
            "status_artifacts": tuple(item.as_dict() for item in self.status_artifacts),
            "required": self.required,
            "restart_eligible": self.restart_eligible,
            "restart_command": self.restart_command,
            "restart_blockers": self.restart_blockers,
            "operator_required_states": self.operator_required_states,
        }


def build_track_b_self_healing_agent_registry(*, repo_root: Path | None = None) -> tuple[dict[str, Any], ...]:
    """Return the static Track B PAPER health contract registry."""

    root = (repo_root or DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT).expanduser()
    return tuple(contract.as_dict() for contract in _agent_contracts(root))


def build_track_b_self_healing_health(
    *,
    repo_root: Path,
    expected_root: Path | None = None,
    now: datetime | None = None,
    process_probe: Callable[[int], Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Read local artifacts/PID files and classify self-healing readiness."""

    repo_root = repo_root.expanduser().resolve()
    expected_root = (expected_root or DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT).expanduser().resolve()
    current = _ensure_utc(now or datetime.now(timezone.utc))
    contracts = _agent_contracts(repo_root)
    agent_inputs: dict[str, Any] = {}
    for contract in contracts:
        agent_inputs[contract.agent_id] = _read_agent_state(
            contract=contract,
            repo_root=repo_root,
            expected_root=expected_root,
            now=current,
            process_probe=process_probe,
        )
    safety = _read_safety_state(repo_root)
    shared_truth = _read_shared_truth_restart_evidence(
        repo_root=repo_root,
        now=current,
        process_probe=process_probe,
    )
    return classify_track_b_self_healing_health(
        {
            "generated_at": current.isoformat(),
            "expected_root": str(expected_root),
            "agents": agent_inputs,
            "broker_safety": safety,
            "shared_truth_restart_evidence": shared_truth,
        }
    )


def classify_track_b_self_healing_health(inputs: Mapping[str, Any]) -> dict[str, Any]:
    """Pure health classifier for Track B self-healing supervision."""

    generated_at = str(inputs.get("generated_at") or datetime.now(timezone.utc).isoformat())
    expected_root = str(inputs.get("expected_root") or DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT)
    registry_by_id = {contract.agent_id: contract for contract in _agent_contracts(Path(expected_root))}
    agents = _mapping(inputs.get("agents"))
    broker_safety = _mapping(inputs.get("broker_safety"))
    restart_policy = _mapping(inputs.get("restart_policy"))
    shared_truth_restart_evidence = _mapping(inputs.get("shared_truth_restart_evidence"))
    if shared_truth_restart_evidence and not _mapping(shared_truth_restart_evidence.get("restart_eligibility")):
        shared_truth_restart_evidence = {
            **shared_truth_restart_evidence,
            "restart_eligibility": classify_shared_truth_restart_evidence(shared_truth_restart_evidence),
        }

    blockers: list[str] = []
    warnings: list[str] = []
    restart_candidates: list[str] = []
    operator_required_agents: list[str] = []
    agent_results: dict[str, Any] = {}

    unsafe_broker_blockers = _broker_safety_blockers(broker_safety)
    for blocker in unsafe_broker_blockers:
        blockers.append(blocker)

    for agent_id, contract in registry_by_id.items():
        supplied = _mapping(agents.get(agent_id))
        result = _classify_agent(contract=contract, supplied=supplied, expected_root=expected_root)
        agent_results[agent_id] = result
        blockers.extend(result["blockers"])
        warnings.extend(result["warnings"])
        if result["operator_required"]:
            operator_required_agents.append(agent_id)
        if result["restart_candidate"]:
            restart_candidates.append(agent_id)

    shared_truth_blockers = (
        _shared_truth_restart_blockers(shared_truth_restart_evidence)
        if "paper_runtime" in restart_candidates
        else []
    )
    blockers.extend(shared_truth_blockers)

    duplicate_submitters = _int(broker_safety.get("duplicate_conflicting_runtime_count"))
    if duplicate_submitters:
        blockers.append("duplicate_conflicting_runtimes")
    stale_launchctl_jobs = _int(broker_safety.get("stale_launchctl_runtime_job_count"))
    if stale_launchctl_jobs:
        blockers.append("stale_launchctl_runtime_job_loaded")

    live_money_eligible = _bool(broker_safety.get("live_money_eligible")) or any(
        _bool(_mapping(agent).get("live_money_eligible")) for agent in agents.values()
    )
    if live_money_eligible:
        blockers.append("live_money_eligible_true")

    wrong_root = any("wrong_root" in result["blockers"] for result in agent_results.values())
    restart_blocked = bool(
        unsafe_broker_blockers
        or shared_truth_blockers
        or live_money_eligible
        or wrong_root
        or duplicate_submitters
        or stale_launchctl_jobs
    )
    required_unhealthy = any(
        result["required"] and result["health_state"] != "HEALTHY" for result in agent_results.values()
    )

    if unsafe_broker_blockers:
        classification = "UNSAFE_BROKER_STATE"
    elif operator_required_agents:
        classification = "OPERATOR_REQUIRED"
    elif restart_candidates and not restart_blocked:
        classification = "AUTO_RESTART_ELIGIBLE"
    elif restart_candidates and restart_blocked:
        classification = "AUTO_RESTART_BLOCKED"
    elif required_unhealthy:
        classification = "DEGRADED_RECOVERABLE"
    elif blockers:
        classification = "AUTO_RESTART_BLOCKED"
    else:
        classification = "SELF_HEALING_READY"

    restart_control = classify_restart_budget_state(
        restart_candidates=restart_candidates,
        broker_reconciliation_clean=not unsafe_broker_blockers,
        duplicate_writer_detected=bool(duplicate_submitters or stale_launchctl_jobs),
        now=_parse_datetime(generated_at) or datetime.now(timezone.utc),
        restart_attempt_count=_int(restart_policy.get("restart_attempt_count")),
        restart_window_seconds=_int_or_default(
            restart_policy.get("restart_window_seconds"),
            DEFAULT_RESTART_WINDOW_SECONDS,
        ),
        max_restart_attempts=_int_or_default(
            restart_policy.get("max_restart_attempts"),
            DEFAULT_MAX_RESTART_ATTEMPTS,
        ),
        cooldown_until=str(restart_policy.get("cooldown_until") or ""),
        healthy_runtime=not required_unhealthy and not blockers,
    )
    if restart_control["classification"] == RESTART_COOLDOWN_ACTIVE:
        blockers.append("restart_cooldown_active")
    elif restart_control["classification"] == RESTART_BUDGET_EXHAUSTED:
        blockers.append("restart_max_attempts_exceeded")
    elif restart_control["classification"] == RESTART_BLOCKED_DUPLICATE_WRITER:
        blockers.append("duplicate_conflicting_runtimes")
    elif restart_control["classification"] == RESTART_BLOCKED_RECONCILIATION:
        blockers.append("broker_reconciliation_mismatch")
    auto_restart_allowed = classification == "AUTO_RESTART_ELIGIBLE" and restart_control["classification"] == RESTART_ALLOWED
    if classification == "AUTO_RESTART_ELIGIBLE" and not auto_restart_allowed:
        classification = "AUTO_RESTART_BLOCKED"
    return {
        "schema_version": "track_b_self_healing_health_v1",
        "generated_at": generated_at,
        "classification": classification,
        "auto_restart_allowed": auto_restart_allowed,
        "restart_control": restart_control,
        "restart_candidates": tuple(restart_candidates),
        "operator_required_agents": tuple(operator_required_agents),
        "blockers": tuple(_dedupe(blockers)),
        "warnings": tuple(_dedupe(warnings)),
        "live_money_eligible": live_money_eligible,
        "expected_root": expected_root,
        "health_contract_artifact_path": str(DEFAULT_SELF_HEALING_HEALTH_ARTIFACT),
        "registry": tuple(contract.as_dict() for contract in registry_by_id.values()),
        "agents": agent_results,
        "broker_safety": dict(broker_safety),
        "shared_truth_restart_evidence": dict(shared_truth_restart_evidence),
        "stale_launchctl_runtime_job_count": stale_launchctl_jobs,
    }


def classify_shared_truth_restart_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Classify execution_core shared-truth evidence for restart supervision."""

    if not evidence:
        return {
            "classification": "RESTART_BLOCKED_SHARED_TRUTH_MISSING",
            "restart_allowed": False,
            "blockers": ("shared_truth_missing",),
            "classifications": {},
            "artifact_paths": {},
        }
    classifications = _mapping(evidence.get("classifications"))
    preflight = _mapping(evidence.get("runtime_start_preflight"))
    unsafe_blockers = tuple(str(_mapping(row).get("code") or "") for row in _list(evidence.get("unsafe_blockers")))
    preflight_blockers = tuple(_mapping(row) for row in _list(preflight.get("blockers")))
    if unsafe_blockers:
        return _shared_truth_restart_row(
            "RESTART_BLOCKED_SHARED_TRUTH_UNSAFE",
            classifications=classifications,
            evidence=evidence,
            blockers=unsafe_blockers,
            preflight_blockers=preflight_blockers,
        )

    service_blocker = _first_shared_truth_service_blocker(classifications)
    if service_blocker:
        return _shared_truth_restart_row(
            service_blocker,
            classifications=classifications,
            evidence=evidence,
            blockers=tuple(blocker.get("code") or service_blocker.lower() for blocker in preflight_blockers)
            or (service_blocker.lower(),),
            preflight_blockers=preflight_blockers,
        )

    if preflight.get("clean_for_runtime_start") is False:
        return _shared_truth_restart_row(
            "RESTART_BLOCKED_SHARED_TRUTH_PREFLIGHT",
            classifications=classifications,
            evidence=evidence,
            blockers=tuple(blocker.get("code") or "shared_truth_preflight_blocked" for blocker in preflight_blockers)
            or ("shared_truth_preflight_blocked",),
            preflight_blockers=preflight_blockers,
        )

    return _shared_truth_restart_row(
        RESTART_ALLOWED_CLEAN,
        classifications=classifications,
        evidence=evidence,
        blockers=(),
        preflight_blockers=preflight_blockers,
        restart_allowed=True,
    )


def classify_restart_budget_state(
    *,
    restart_candidates: Sequence[str],
    broker_reconciliation_clean: bool,
    duplicate_writer_detected: bool,
    now: datetime,
    restart_attempt_count: int = 0,
    restart_window_seconds: int = DEFAULT_RESTART_WINDOW_SECONDS,
    max_restart_attempts: int = DEFAULT_MAX_RESTART_ATTEMPTS,
    cooldown_until: str | None = None,
    healthy_runtime: bool = False,
) -> dict[str, Any]:
    """Classify restart budget/cooldown state without performing a restart."""

    current = _ensure_utc(now)
    cooldown_ts = _parse_datetime(cooldown_until)
    cooldown_active = bool(cooldown_ts and cooldown_ts > current)
    budget_exhausted = bool(restart_attempt_count >= max_restart_attempts > 0)
    if healthy_runtime:
        classification = RESTART_NOT_NEEDED_HEALTHY
        crash_loop_state = "HEALTHY_RUNTIME_BUDGET_RESET"
        restart_attempt_count = 0
        budget_exhausted = False
        cooldown_active = False
    elif duplicate_writer_detected:
        classification = RESTART_BLOCKED_DUPLICATE_WRITER
        crash_loop_state = "DUPLICATE_WRITER_BLOCK"
    elif not broker_reconciliation_clean:
        classification = RESTART_BLOCKED_RECONCILIATION
        crash_loop_state = "BROKER_RECONCILIATION_BLOCK"
    elif not restart_candidates:
        classification = RESTART_NOT_NEEDED_HEALTHY
        crash_loop_state = "NO_RESTART_CANDIDATE"
    elif cooldown_active:
        classification = RESTART_COOLDOWN_ACTIVE
        crash_loop_state = "COOLDOWN_ACTIVE"
    elif budget_exhausted:
        classification = RESTART_BUDGET_EXHAUSTED
        crash_loop_state = "BUDGET_EXHAUSTED"
    else:
        classification = RESTART_ALLOWED
        crash_loop_state = "RESTART_CANDIDATE_WITHIN_BUDGET"
    return {
        "classification": classification,
        "restart_allowed": classification == RESTART_ALLOWED,
        "restart_attempt_count": restart_attempt_count,
        "restart_window_seconds": int(restart_window_seconds),
        "max_restart_attempts": int(max_restart_attempts),
        "cooldown_until": cooldown_ts.isoformat() if cooldown_ts else None,
        "cooldown_active": cooldown_active,
        "crash_loop_state": crash_loop_state,
        "max_restart_budget_exhausted": budget_exhausted,
        "restart_candidates": tuple(restart_candidates),
        "broker_reconciliation_clean": bool(broker_reconciliation_clean),
        "duplicate_writer_detected": bool(duplicate_writer_detected),
    }


def write_track_b_self_healing_health(*, output_path: Path, health: Mapping[str, Any]) -> None:
    """Write a read-only self-healing health artifact."""

    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(health), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _agent_contracts(repo_root: Path) -> tuple[AgentContract, ...]:
    root = repo_root.expanduser()
    return (
        AgentContract(
            agent_id="phase1_candle_supervisor",
            display_name="Phase-1 candle supervisor/listener",
            expected_command_fragments=("mgc_v05l.execution_core.phase1_databento_live_runtime_candles", "--mode", "service"),
            pid_paths=(
                str(root / "var" / "phase1_databento_live_candles_service.pid"),
                str(root / "var" / "phase1_databento_live_candles_child.pid"),
            ),
            heartbeat_artifacts=(),
            status_artifacts=(
                ArtifactContract(
                    "phase1_listener_status",
                    str(
                        root
                        / "outputs"
                        / "reports"
                        / "phase1_databento_live_runtime_candles"
                        / "latest_phase1_databento_live_listener_status.json"
                    ),
                    180.0,
                    True,
                    ("latest_record_at", "generated_at"),
                ),
                ArtifactContract(
                    "phase1_supervisor_status",
                    str(
                        root
                        / "outputs"
                        / "reports"
                        / "phase1_databento_live_runtime_candles"
                        / "latest_phase1_databento_live_supervisor_status.json"
                    ),
                    180.0,
                    True,
                ),
            ),
            required=True,
            restart_eligible=True,
            restart_command="bash scripts/start-phase1-databento-live-candles",
            restart_blockers=_global_restart_blockers(),
            operator_required_states=("PHASE1_DATABENTO_ENV_MISSING", "PHASE1_DATABENTO_AUTH_FAILED"),
        ),
        AgentContract(
            agent_id="broker_truth_refresher",
            display_name="Broker truth refresher",
            expected_command_fragments=("mgc_v05l.app.ibkr_broker_truth_refresher", "--service", "--read-only"),
            pid_paths=(str(root / "var" / "track_b_broker_truth_refresh_service.pid"),),
            heartbeat_artifacts=(
                ArtifactContract(
                    "broker_truth_heartbeat",
                    str(root / "var" / "track_b_broker_truth_refresh_heartbeat.json"),
                    150.0,
                    True,
                ),
            ),
            status_artifacts=(
                ArtifactContract(
                    "broker_truth_status",
                    str(
                        root
                        / "outputs"
                        / "reports"
                        / "ibkr_read_only_verification"
                        / "ibkr_broker_truth_refresh_status.json"
                    ),
                    150.0,
                    True,
                    ("generated_at", "last_success_at", "completed_at"),
                ),
                ArtifactContract("broker_truth_lease", str(root / DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT), 150.0, True),
            ),
            required=True,
            restart_eligible=True,
            restart_command="bash scripts/start-track-b-broker-truth-refresh",
            restart_blockers=_global_restart_blockers(),
            operator_required_states=("BROKER_TRUTH_REFRESH_OPERATOR_REQUIRED", "BROKER_TRUTH_REFRESH_TWS_REVIEW_REQUIRED"),
        ),
        AgentContract(
            agent_id="operator_readiness_refresher",
            display_name="Operator readiness refresher",
            expected_command_fragments=("mgc_v05l.app.track_b_operator_readiness_refresher", "--service"),
            pid_paths=(
                str(root / "var" / "track_b_operator_readiness_refresh_service.pid"),
                str(root / "var" / "track_b_operator_readiness_refresh_child.pid"),
            ),
            heartbeat_artifacts=(
                ArtifactContract(
                    "operator_readiness_heartbeat",
                    str(root / "var" / "track_b_operator_readiness_refresh_heartbeat.json"),
                    180.0,
                    True,
                ),
            ),
            status_artifacts=(
                ArtifactContract(
                    "operator_readiness_status",
                    str(
                        root
                        / "outputs"
                        / "reports"
                        / "track_b_operator_readiness_refresher"
                        / "latest_track_b_operator_readiness_refresher_status.json"
                    ),
                    180.0,
                    True,
                    ("generated_at", "last_refresh_finished_at"),
                ),
                ArtifactContract("canonical_readiness", str(root / DEFAULT_CANONICAL_READINESS_ARTIFACT), 180.0, True),
            ),
            required=True,
            restart_eligible=True,
            restart_command="bash scripts/start-track-b-operator-readiness-refresh",
            restart_blockers=_global_restart_blockers(),
            operator_required_states=("TRACK_B_OPERATOR_READINESS_REFRESH_OPERATOR_REQUIRED",),
        ),
        AgentContract(
            agent_id="paper_runtime",
            display_name="Track B PAPER runtime",
            expected_command_fragments=("mgc_v05l.app.main", "probationary-paper-soak"),
            pid_paths=(str(root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper.pid"),),
            heartbeat_artifacts=(),
            status_artifacts=(
                ArtifactContract(
                    "operator_status",
                    str(root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"),
                    180.0,
                    True,
                    ("generated_at", "updated_at"),
                ),
                ArtifactContract(
                    "paper_runtime_truth",
                    str(
                        root
                        / "outputs"
                        / "probationary_pattern_engine"
                        / "paper_session"
                        / "runtime"
                        / "paper_runtime_truth.json"
                    ),
                    180.0,
                    False,
                    ("last_success_at", "generated_at"),
                ),
                ArtifactContract(
                    "paper_runtime_pid_metadata",
                    str(
                        root
                        / "outputs"
                        / "probationary_pattern_engine"
                        / "paper_session"
                        / "runtime"
                        / "probationary_paper.pid.json"
                    ),
                    180.0,
                    False,
                    ("generated_at", "launch_started_at"),
                ),
                ArtifactContract("canonical_readiness", str(root / DEFAULT_CANONICAL_READINESS_ARTIFACT), 180.0, True),
            ),
            required=True,
            restart_eligible=True,
            restart_command="bash scripts/run_headless_supervised_paper_service.sh --no-start-dashboard",
            restart_blockers=(
                *_global_restart_blockers(),
                "broker_truth_lease_not_active",
                "unresolved_submit_ownership",
                "phase1_not_healthy_for_runtime_restart",
                "restart_cooldown_active",
                "restart_max_attempts_exceeded",
            ),
            operator_required_states=("OPEN_MANAGED_POSITION_REVIEW_REQUIRED", "RUNTIME_RESTART_BROKER_STATE_AMBIGUOUS"),
        ),
        AgentContract(
            agent_id="operator_dashboard_backend",
            display_name="Operator dashboard/backend",
            expected_command_fragments=("mgc_v05l.app.operator_dashboard",),
            pid_paths=(str(root / "outputs" / "operator_dashboard" / "runtime" / "operator_dashboard.pid"),),
            heartbeat_artifacts=(),
            status_artifacts=(
                ArtifactContract(
                    "operator_dashboard_info",
                    str(root / "outputs" / "operator_dashboard" / "runtime" / "operator_dashboard.json"),
                    180.0,
                    False,
                    ("generated_at", "started_at"),
                ),
                ArtifactContract(
                    "operator_dashboard_readiness",
                    str(root / "outputs" / "operator_dashboard" / "runtime" / "operator_dashboard_readiness.json"),
                    180.0,
                    False,
                ),
            ),
            required=False,
            restart_eligible=True,
            restart_command="bash scripts/run_operator_dashboard.sh",
            restart_blockers=("wrong_root", "duplicate_conflicting_runtimes", "live_money_eligible_true"),
            operator_required_states=("DASHBOARD_PORT_CONFLICT_OPERATOR_REQUIRED",),
        ),
    )


def _global_restart_blockers() -> tuple[str, ...]:
    return (
        "unknown_open_orders",
        "lifecycle_review_required",
        "broker_reconciliation_mismatch",
        "live_money_eligible_true",
        "wrong_root",
        "duplicate_conflicting_runtimes",
    )


def _read_agent_state(
    *,
    contract: AgentContract,
    repo_root: Path,
    expected_root: Path,
    now: datetime,
    process_probe: Callable[[int], Mapping[str, Any]] | None,
) -> dict[str, Any]:
    pid_rows = []
    running = False
    root_ok = True
    command_ok = True
    probe_process = process_probe or _default_process_probe
    for pid_path in contract.pid_paths:
        pid = _read_pid(Path(pid_path))
        probe = _mapping(probe_process(pid)) if pid else {}
        if pid and probe:
            running = running or _bool(probe.get("running"))
            cwd = str(probe.get("cwd") or "")
            command = str(probe.get("command") or "")
            if cwd and Path(cwd).expanduser().resolve() != expected_root:
                root_ok = False
            if command and not all(fragment in command for fragment in contract.expected_command_fragments):
                command_ok = False
        pid_rows.append({"path": pid_path, "pid": pid, "probe": dict(probe)})
    artifact_rows = [
        _artifact_state(artifact, now=now) for artifact in (*contract.heartbeat_artifacts, *contract.status_artifacts)
    ]
    return {
        "process_running": running,
        "root_ok": root_ok,
        "command_ok": command_ok,
        "pid_files": pid_rows,
        "artifacts": artifact_rows,
        "classification": _first_payload_classification(artifact_rows),
        "live_money_eligible": any(_bool(row.get("payload", {}).get("live_money_eligible")) for row in artifact_rows),
    }


def _read_safety_state(repo_root: Path) -> dict[str, Any]:
    reconciliation = _read_json(repo_root / PHASE1_RECONCILIATION_ARTIFACT)
    canonical = _read_json(repo_root / DEFAULT_CANONICAL_READINESS_ARTIFACT)
    broker_lease = _read_json(repo_root / DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT)
    launch_guard = _read_json(
        repo_root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "probationary_paper.pid.json.launch_guard.json"
    )
    ownership = reconciliation.get("submit_intent_ownership_reconciliation")
    ownership = ownership if isinstance(ownership, Mapping) else {}
    unresolved_ownership_count = (
        reconciliation.get("unresolved_submit_intent_ownership_count")
        or reconciliation.get("unresolved_submit_ownership_count")
        or ownership.get("unresolved_count")
        or ownership.get("unresolved_submit_intent_ownership_count")
        or 0
    )
    return {
        "classification": reconciliation.get("classification"),
        "broker_reconciled": reconciliation.get("broker_reconciled"),
        "broker_truth_lease_state": broker_lease.get("lease_state") or broker_lease.get("state"),
        "unknown_open_order_count": reconciliation.get("unknown_broker_open_order_count"),
        "track_b_broker_open_order_count": reconciliation.get("track_b_broker_open_order_count"),
        "track_b_broker_position_count": reconciliation.get("track_b_broker_position_count"),
        "review_required_count": reconciliation.get("review_required_count"),
        "lifecycle_open_position_count": reconciliation.get("lifecycle_open_position_count"),
        "unresolved_submit_intent_ownership_count": unresolved_ownership_count,
        "live_money_eligible": _bool(reconciliation.get("live_money_eligible")) or _bool(canonical.get("live_money_eligible")),
        "stale_launchctl_runtime_job_count": _int(launch_guard.get("stale_launchctl_runtime_job_count")),
    }


def _read_shared_truth_restart_evidence(
    *,
    repo_root: Path,
    now: datetime,
    process_probe: Callable[[int], Mapping[str, Any]] | None,
) -> dict[str, Any]:
    def pid_running(pid: int) -> bool:
        if process_probe is None:
            return False
        return _bool(_mapping(process_probe(pid)).get("running"))

    def process_root(pid: int) -> Path | None:
        if process_probe is None:
            return None
        cwd = str(_mapping(process_probe(pid)).get("cwd") or "")
        if not cwd:
            return None
        try:
            return Path(cwd).expanduser().resolve()
        except OSError:
            return None

    shared_truth = refresh_track_b_shared_truth(
        config=TrackBSharedTruthRefreshConfig(
            repo_root=repo_root,
            broker_lease_history_path=None,
        ),
        now=now,
        pid_running=pid_running if process_probe is not None else None,
        process_root_resolver=process_root if process_probe is not None else None,
    )
    preflight = build_runtime_start_preflight_summary(shared_truth)
    classification = classify_shared_truth_restart_evidence(
        {
            **shared_truth,
            "runtime_start_preflight": preflight,
        }
    )
    return {
        "source": "execution_core_shared_truth_refresh",
        "read_only": True,
        "projection_consumed": False,
        "classifications": _mapping(shared_truth.get("classifications")),
        "runtime_start_preflight": preflight,
        "unsafe_blockers": tuple(_mapping(row) for row in _list(shared_truth.get("unsafe_blockers"))),
        "warnings": tuple(_mapping(row) for row in _list(shared_truth.get("warnings"))),
        "artifact_paths": _mapping(shared_truth.get("artifact_paths")),
        "restart_eligibility": classification,
    }


def _classify_agent(*, contract: AgentContract, supplied: Mapping[str, Any], expected_root: str) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    artifacts = tuple(_mapping(item) for item in supplied.get("artifacts") or ())
    running = _bool(supplied.get("process_running") or supplied.get("running"))
    root_ok = supplied.get("root_ok", supplied.get("root_match", True)) is not False
    command_ok = supplied.get("command_ok", True) is not False
    classification = str(supplied.get("classification") or supplied.get("state") or "").strip().upper()

    if classification in contract.operator_required_states:
        blockers.append(f"{contract.agent_id}_operator_required")
    if not root_ok:
        blockers.append("wrong_root")
    if not command_ok:
        blockers.append(f"{contract.agent_id}_command_mismatch")
    if contract.required and not running:
        blockers.append(f"{contract.agent_id}_not_running")
    elif not contract.required and not running:
        warnings.append(f"{contract.agent_id}_not_running_optional")

    required_artifacts = [item for item in artifacts if item.get("required", True)]
    stale_required = [str(item.get("label") or item.get("path")) for item in required_artifacts if item.get("fresh") is False]
    missing_required = [str(item.get("label") or item.get("path")) for item in required_artifacts if item.get("present") is False]
    for label in missing_required:
        blockers.append(f"{contract.agent_id}_{label}_missing")
    for label in stale_required:
        blockers.append(f"{contract.agent_id}_{label}_stale")
    for item in artifacts:
        if not item.get("required", True) and item.get("present") is False:
            warnings.append(f"{contract.agent_id}_{item.get('label')}_missing_optional")
        elif not item.get("required", True) and item.get("fresh") is False:
            warnings.append(f"{contract.agent_id}_{item.get('label')}_stale_optional")

    unhealthy = bool(blockers)
    restart_candidate = bool(unhealthy and contract.restart_eligible and contract.required)
    operator_required = bool(classification in contract.operator_required_states or supplied.get("operator_required") is True)
    if unhealthy and not contract.restart_eligible and contract.required:
        blockers.append(f"{contract.agent_id}_restart_not_eligible")

    return {
        "agent_id": contract.agent_id,
        "display_name": contract.display_name,
        "required": contract.required,
        "restart_eligible": contract.restart_eligible,
        "restart_command": contract.restart_command,
        "restart_blockers": contract.restart_blockers,
        "health_state": "HEALTHY" if not unhealthy else "UNHEALTHY",
        "process_running": running,
        "root_ok": root_ok,
        "expected_root": expected_root,
        "command_ok": command_ok,
        "classification": classification or None,
        "restart_candidate": restart_candidate,
        "operator_required": operator_required,
        "blockers": tuple(_dedupe(blockers)),
        "warnings": tuple(_dedupe(warnings)),
        "artifacts": artifacts,
    }


def _broker_safety_blockers(safety: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    classification = str(safety.get("classification") or "").strip().upper()
    if _int(safety.get("unknown_open_order_count")):
        blockers.append("unknown_open_orders")
    if _int(safety.get("review_required_count")):
        blockers.append("lifecycle_review_required")
    if _int(safety.get("unresolved_submit_intent_ownership_count")):
        blockers.append("unresolved_submit_ownership")
    if classification and classification != RECONCILED_CLASSIFICATION:
        blockers.append("broker_reconciliation_mismatch")
    if safety.get("broker_reconciled") is False:
        blockers.append("broker_reconciliation_mismatch")
    if _int(safety.get("track_b_broker_open_order_count")) and classification != RECONCILED_CLASSIFICATION:
        blockers.append("unknown_open_orders")
    return _dedupe(blockers)


def _shared_truth_restart_blockers(evidence: Mapping[str, Any]) -> list[str]:
    if not evidence:
        return []
    eligibility = _mapping(evidence.get("restart_eligibility")) or classify_shared_truth_restart_evidence(evidence)
    classification = str(eligibility.get("classification") or "")
    if classification == RESTART_ALLOWED_CLEAN:
        return []
    return [f"shared_truth_{classification.lower()}"] or ["shared_truth_restart_blocked"]


def _first_shared_truth_service_blocker(classifications: Mapping[str, Any]) -> str | None:
    checks = (
        ("Open Order Truth", {"NO_OPEN_ORDERS"}, RESTART_BLOCKED_OPEN_ORDER_TRUTH),
        ("Managed Order Registry", {"NO_MANAGED_ORDERS"}, RESTART_BLOCKED_MANAGED_ORDER_TRUTH),
        ("Position Truth", {"CLEAN_FLAT_READY"}, RESTART_BLOCKED_POSITION_TRUTH),
        ("Runtime Environment Truth", {"RUNTIME_DOWN_CLEAN"}, RESTART_BLOCKED_RUNTIME_TRUTH),
        ("Managed Position Registry", {"NO_MANAGED_POSITIONS"}, RESTART_BLOCKED_MANAGED_POSITION_TRUTH),
        ("Reconciliation", {RECONCILED_CLASSIFICATION}, RESTART_BLOCKED_RECONCILIATION),
        ("Broker Truth Lease", {"ACTIVE"}, RESTART_BLOCKED_RECONCILIATION),
    )
    for service, allowed, restart_classification in checks:
        observed = str(classifications.get(service) or "MISSING")
        if observed not in allowed:
            return restart_classification
    return None


def _shared_truth_restart_row(
    classification: str,
    *,
    classifications: Mapping[str, Any],
    evidence: Mapping[str, Any],
    blockers: Sequence[str],
    preflight_blockers: Sequence[Mapping[str, Any]],
    restart_allowed: bool = False,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "restart_allowed": restart_allowed,
        "blockers": tuple(_dedupe([str(blocker) for blocker in blockers])),
        "classifications": dict(classifications),
        "preflight_blockers": tuple(dict(blocker) for blocker in preflight_blockers),
        "artifact_paths": _mapping(evidence.get("artifact_paths")),
    }


def _artifact_state(contract: ArtifactContract, *, now: datetime) -> dict[str, Any]:
    path = Path(contract.path)
    payload = _read_json(path)
    timestamp = None
    timestamp_source = None
    for field in contract.timestamp_fields:
        timestamp = _parse_datetime(payload.get(field))
        if timestamp is not None:
            timestamp_source = field
            break
    age_seconds = None if timestamp is None else max((now - timestamp).total_seconds(), 0.0)
    return {
        "label": contract.label,
        "path": str(path),
        "present": bool(payload),
        "required": contract.required,
        "generated_at": timestamp.isoformat() if timestamp else None,
        "timestamp_source": timestamp_source,
        "age_seconds": age_seconds,
        "freshness_ttl_seconds": contract.freshness_ttl_seconds,
        "fresh": bool(payload and age_seconds is not None and age_seconds <= contract.freshness_ttl_seconds),
        "payload": payload,
    }


def _first_payload_classification(artifact_rows: Sequence[Mapping[str, Any]]) -> str | None:
    for row in artifact_rows:
        payload = _mapping(row.get("payload"))
        value = payload.get("classification") or payload.get("state")
        if value:
            return str(value)
    return None


def _read_pid(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _default_process_probe(pid: int) -> dict[str, Any]:
    running = False
    try:
        os.kill(pid, 0)
        running = True
    except PermissionError:
        running = True
    except ProcessLookupError:
        running = False
    except OSError:
        running = False
    command = ""
    cwd = ""
    if running:
        try:
            proc = subprocess.run(
                ["ps", "-p", str(pid), "-o", "command="],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            command = proc.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            command = ""
        try:
            proc = subprocess.run(
                ["lsof", "-a", "-p", str(pid), "-d", "cwd", "-Fn"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            for line in proc.stdout.splitlines():
                if line.startswith("n"):
                    cwd = line[1:]
                    break
        except (OSError, subprocess.SubprocessError):
            cwd = ""
    return {"pid": pid, "running": running, "command": command, "cwd": cwd}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


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


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _bool(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


def _int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _int_or_default(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))
