"""Read-only Track B PAPER agent health contract authority.

Agent Health authority lives in execution_core. Dashboard artifacts are
projections and must not be used as runtime, readiness, restart, broker, or
routing authority. This v1 contract classifies registered agents only; it never
starts, stops, restarts, submits, cancels, replaces, closes, or flattens
anything.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_agent_registry import (
    DEFAULT_AGENT_REGISTRY_ARTIFACT,
    TrackBAgentRegistryConfig,
    build_track_b_agent_registry,
    write_track_b_agent_registry,
)
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_process_surface_hygiene import (
    DEFAULT_PROCESS_HYGIENE_ARTIFACT,
    TrackBProcessSurfaceHygieneConfig,
    build_track_b_process_surface_hygiene,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


AGENT_HEALTH_READY = "AGENT_HEALTH_READY"
AGENT_HEALTH_DEGRADED = "AGENT_HEALTH_DEGRADED"
AGENT_HEALTH_BLOCKING = "AGENT_HEALTH_BLOCKING"
AGENT_HEALTH_UNKNOWN = "AGENT_HEALTH_UNKNOWN"

HEALTHY = "HEALTHY"
DEGRADED = "DEGRADED"
STALE = "STALE"
MISSING = "MISSING"
MISSING_ARTIFACT = "MISSING_ARTIFACT"
STOPPED_EXPECTED = "STOPPED_EXPECTED"
STOPPED_UNEXPECTED = "STOPPED_UNEXPECTED"
NOT_APPLICABLE = "NOT_APPLICABLE"
DUPLICATE_PROCESS = "DUPLICATE_PROCESS"
ROOT_MISMATCH = "ROOT_MISMATCH"
SOURCE_COMMIT_MISMATCH = "SOURCE_COMMIT_MISMATCH"

REQUIRED_RUNNING = "required_running"
REQUIRED_FRESH_ARTIFACT = "required_fresh_artifact"
ON_DEMAND = "on_demand"
DIAGNOSTIC_OPTIONAL = "diagnostic_optional"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_AGENT_HEALTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "agent_health" / "latest_agent_health.json"
)
DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_agent_health.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_PHASE1_READINESS_ARTIFACT = (
    Path("outputs") / "reports" / "phase1_runtime_data_readiness" / "latest_phase1_runtime_data_readiness.json"
)
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
)
DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json"
)
DEFAULT_BROKER_TRUTH_STATUS_ARTIFACT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
)
DEFAULT_ARTIFACT_MAX_AGE_SECONDS = 300.0


@dataclass(frozen=True)
class TrackBAgentHealthConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION
    agent_registry_path: Path = DEFAULT_AGENT_REGISTRY_ARTIFACT
    runtime_environment_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    phase1_readiness_path: Path = DEFAULT_PHASE1_READINESS_ARTIFACT
    process_hygiene_path: Path = DEFAULT_PROCESS_HYGIENE_ARTIFACT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    shared_truth_refresh_path: Path = DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT
    broker_truth_status_path: Path = DEFAULT_BROKER_TRUTH_STATUS_ARTIFACT
    broker_lease_path: Path = DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT
    artifact_max_age_seconds: float = DEFAULT_ARTIFACT_MAX_AGE_SECONDS

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_agent_health(
    *,
    config: TrackBAgentHealthConfig,
    now: datetime | None = None,
    process_rows: Sequence[Mapping[str, Any]] | None = None,
    pid_running: Callable[[int], bool] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    current_head = (source_commit_resolver or _git_head)(config.repo_root)
    registry_path = config.resolve(config.agent_registry_path)
    registry = _read_json(registry_path)
    if not registry or not _list(registry.get("agents")):
        registry_config = TrackBAgentRegistryConfig(repo_root=config.repo_root, dashboard_projection_path=None)
        registry = build_track_b_agent_registry(config=registry_config, now=actual_now)
        write_track_b_agent_registry(config=registry_config, payload=registry)

    runtime_environment_truth = _read_json(config.resolve(config.runtime_environment_truth_path))
    phase1_readiness = _read_json(config.resolve(config.phase1_readiness_path))
    process_hygiene = _load_or_build_process_hygiene(
        config=config,
        now=actual_now,
        process_rows=process_rows,
        pid_running=pid_running,
    )
    control_plane_snapshot = _read_json(config.resolve(config.control_plane_snapshot_path))
    shared_truth_refresh = _read_json(config.resolve(config.shared_truth_refresh_path))
    agents = _list(registry.get("agents"))
    health_rows = [
        _agent_health_row(
            agent=agent,
            config=config,
            now=actual_now,
            runtime_environment_truth=runtime_environment_truth,
            phase1_readiness=phase1_readiness,
            process_hygiene=process_hygiene,
            control_plane_snapshot=control_plane_snapshot,
            shared_truth_refresh=shared_truth_refresh,
            current_head=current_head,
        )
        for agent in agents
    ]
    classification = _overall_classification(health_rows)
    return {
        "schema_version": "track_b_agent_health_v2",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "authority_owner": "execution_core",
        "projection_only": False,
        "classification": classification,
        "agents": health_rows,
        "agent_registry": {
            "classification": registry.get("classification"),
            "generated_at": registry.get("generated_at"),
            "artifact_path": str(registry_path),
        },
        "summary": {
            "classification": classification,
            "agent_count": len(health_rows),
            "healthy_count": sum(1 for row in health_rows if row.get("status") == HEALTHY),
            "degraded_count": sum(1 for row in health_rows if row.get("status") == DEGRADED),
            "stale_count": sum(1 for row in health_rows if row.get("status") == STALE),
            "missing_count": sum(1 for row in health_rows if row.get("status") in {MISSING, MISSING_ARTIFACT}),
            "missing_artifact_count": sum(1 for row in health_rows if row.get("status") == MISSING_ARTIFACT),
            "stopped_expected_count": sum(1 for row in health_rows if row.get("status") == STOPPED_EXPECTED),
            "stopped_unexpected_count": sum(1 for row in health_rows if row.get("status") == STOPPED_UNEXPECTED),
            "duplicate_process_count": sum(1 for row in health_rows if row.get("status") == DUPLICATE_PROCESS),
            "root_mismatch_count": sum(1 for row in health_rows if row.get("status") == ROOT_MISMATCH),
            "source_commit_mismatch_count": sum(
                1 for row in health_rows if row.get("status") == SOURCE_COMMIT_MISMATCH
            ),
            "blocking_for_recovery_count": sum(1 for row in health_rows if row.get("blocking_for_recovery") is True),
            "diagnostic_only_count": sum(1 for row in health_rows if row.get("diagnostic_only") is True),
            "blocking_for_proof_count": sum(1 for row in health_rows if row.get("blocking_for_proof") is True),
            "blocking_for_runtime_submit_count": sum(
                1 for row in health_rows if row.get("blocking_for_runtime_submit") is True
            ),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "agent_registry": str(registry_path),
            "runtime_environment_truth": str(config.resolve(config.runtime_environment_truth_path)),
            "phase1_readiness": str(config.resolve(config.phase1_readiness_path)),
            "process_hygiene": str(config.resolve(config.process_hygiene_path)),
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "shared_truth_refresh": str(config.resolve(config.shared_truth_refresh_path)),
        },
    }


def write_track_b_agent_health(
    *,
    config: TrackBAgentHealthConfig,
    payload: Mapping[str, Any],
) -> Path:
    authority_path = config.resolve(config.output_path)
    _write_json_atomic(authority_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_agent_health_projection(authority_payload=payload, authority_path=authority_path),
        )
    return authority_path


def build_dashboard_agent_health_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_agent_health_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write the read-only Track B PAPER agent health authority artifact.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_AGENT_HEALTH_ARTIFACT)
    parser.add_argument("--dashboard-projection-path", type=Path, default=DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION)
    parser.add_argument("--no-dashboard-projection", action="store_true")
    parser.add_argument("--artifact-max-age-seconds", type=float, default=DEFAULT_ARTIFACT_MAX_AGE_SECONDS)
    parser.add_argument("--json", action="store_true", help="Print the full health artifact as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBAgentHealthConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        dashboard_projection_path=None if bool(args.no_dashboard_projection) else Path(args.dashboard_projection_path),
        artifact_max_age_seconds=float(args.artifact_max_age_seconds),
    )
    payload = build_track_b_agent_health(config=config)
    authority_path = write_track_b_agent_health(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "summary": payload.get("summary"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "paper_proof_invoked": False,
                    "live_money_eligible": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0 if payload.get("classification") in {AGENT_HEALTH_READY, AGENT_HEALTH_DEGRADED} else 2


def _agent_health_row(
    *,
    agent: Mapping[str, Any],
    config: TrackBAgentHealthConfig,
    now: datetime,
    runtime_environment_truth: Mapping[str, Any],
    phase1_readiness: Mapping[str, Any],
    process_hygiene: Mapping[str, Any],
    control_plane_snapshot: Mapping[str, Any],
    shared_truth_refresh: Mapping[str, Any],
    current_head: str | None,
) -> dict[str, Any]:
    agent_id = str(agent.get("agent_id") or "")
    expected_state = _expected_state(agent)
    if agent_id == "track_b_paper_runtime":
        status, reason = _runtime_status(runtime_environment_truth)
        primary_path = str(config.resolve(config.runtime_environment_truth_path))
        artifact_status = _artifact_status(
            config.resolve(config.runtime_environment_truth_path),
            now=now,
            max_age_seconds=config.artifact_max_age_seconds,
            repo_root=config.repo_root,
            current_head=current_head,
        )
    elif agent_id == "phase1_databento_live_candles":
        status, reason = _phase1_status(phase1_readiness)
        primary_path = str(config.resolve(config.phase1_readiness_path))
        artifact_status = _artifact_status(
            config.resolve(config.phase1_readiness_path),
            now=now,
            max_age_seconds=config.artifact_max_age_seconds,
            repo_root=config.repo_root,
            current_head=current_head,
        )
        if status == HEALTHY and artifact_status["status"] == STALE and _phase1_market_closed(phase1_readiness):
            artifact_status = {**artifact_status, "status": HEALTHY}
    elif agent_id == "broker_truth_lease_refresher":
        primary_path = str(config.resolve(config.broker_truth_status_path))
        artifact_status = _broker_truth_artifact_status(
            config=config,
            now=now,
            current_head=current_head,
        )
        status = str(artifact_status.get("status") or MISSING_ARTIFACT)
        reason = str(artifact_status.get("reason") or "broker_truth_artifact_status")
    elif agent_id == "canonical_readiness_refresher":
        primary_path = _canonical_readiness_primary_artifact(agent, config=config)
        artifact_status = _canonical_readiness_artifact_status(
            agent=agent,
            config=config,
            now=now,
            current_head=current_head,
            runtime_environment_truth=runtime_environment_truth,
        )
        status = str(artifact_status.get("status") or MISSING_ARTIFACT)
        reason = str(artifact_status.get("reason") or "canonical_readiness_artifact_status")
    elif expected_state == ON_DEMAND:
        status, reason = HEALTHY, "on_demand_agent_invoked_by_callers"
        primary_path = None
        artifact_status = {"status": NOT_APPLICABLE, "last_seen_at": None, "freshness_age_seconds": None}
    elif expected_state == DIAGNOSTIC_OPTIONAL:
        primary_path = _primary_path(agent)
        artifact_status = _optional_artifact_status(
            primary_path,
            now=now,
            max_age_seconds=config.artifact_max_age_seconds,
            repo_root=config.repo_root,
            current_head=current_head,
        )
        status = artifact_status["status"]
        reason = "diagnostic_optional"
    else:
        primary_path = _primary_path(agent)
        artifact_status = _required_artifact_status(
            primary_path,
            now=now,
            max_age_seconds=config.artifact_max_age_seconds,
            repo_root=config.repo_root,
            current_head=current_head,
        )
        status = artifact_status["status"]
        reason = artifact_status["reason"]

    process_probe = _process_probe(agent_id=agent_id, process_hygiene=process_hygiene)
    generation_evidence = _generation_evidence(
        agent_id=agent_id,
        runtime_environment_truth=runtime_environment_truth,
        control_plane_snapshot=control_plane_snapshot,
        shared_truth_refresh=shared_truth_refresh,
        artifact_status=artifact_status,
    )
    status, reason = _apply_probe_overrides(
        agent_id=agent_id,
        status=status,
        reason=reason,
        runtime_environment_truth=runtime_environment_truth,
        process_probe=process_probe,
        artifact_status=artifact_status,
    )
    if agent_id == "track_b_paper_runtime" and status == STOPPED_UNEXPECTED and _shared_truth_active_hold_pending(
        shared_truth_refresh
    ):
        status = DEGRADED
        reason = "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
    required_for_proof = agent.get("required_for_proof") is True
    required_for_runtime_submit = agent.get("required_for_runtime_submit") is True
    diagnostic_only = agent.get("diagnostic_only") is True or expected_state == DIAGNOSTIC_OPTIONAL
    hard_blocking_statuses = {
        MISSING,
        MISSING_ARTIFACT,
        STALE,
        STOPPED_UNEXPECTED,
        DUPLICATE_PROCESS,
        ROOT_MISMATCH,
        SOURCE_COMMIT_MISMATCH,
    }
    blocking_for_proof = required_for_proof and status in hard_blocking_statuses
    blocking_for_runtime_submit = required_for_runtime_submit and status in hard_blocking_statuses
    blocking_for_recovery = status in {DUPLICATE_PROCESS, ROOT_MISMATCH, SOURCE_COMMIT_MISMATCH, MISSING_ARTIFACT}
    if status == DEGRADED and agent_id == "track_b_paper_runtime" and _runtime_down_clean(runtime_environment_truth):
        blocking_for_proof = False
        blocking_for_runtime_submit = False
        blocking_for_recovery = False
    if expected_state == DIAGNOSTIC_OPTIONAL:
        blocking_for_proof = False
        blocking_for_runtime_submit = False
        blocking_for_recovery = False
    runtime_probe = _runtime_probe(runtime_environment_truth) if agent_id == "track_b_paper_runtime" else {}
    heartbeat_fresh = str(artifact_status.get("heartbeat_status") or artifact_status.get("status") or "") == HEALTHY
    artifact_fresh = artifact_status.get("status") == HEALTHY
    return {
        "agent_id": agent_id,
        "display_name": agent.get("display_name"),
        "category": agent.get("category"),
        "health_contract_id": agent.get("health_contract_id"),
        "expected_state": expected_state,
        "status": status,
        "probe_status": status,
        "probe_reason": reason,
        "heartbeat_path": agent.get("heartbeat_artifact_path"),
        "primary_artifact_path": primary_path,
        "max_age_seconds": None if expected_state in {ON_DEMAND, DIAGNOSTIC_OPTIONAL} else config.artifact_max_age_seconds,
        "last_seen_at": artifact_status.get("last_seen_at"),
        "freshness_age_seconds": artifact_status.get("freshness_age_seconds"),
        "heartbeat_fresh": heartbeat_fresh,
        "artifact_fresh": artifact_fresh,
        "process_alive": runtime_probe.get("process_alive")
        if agent_id == "track_b_paper_runtime"
        else process_probe.get("process_alive"),
        "root_matches": runtime_probe.get("root_matches")
        if agent_id == "track_b_paper_runtime"
        else artifact_status.get("root_matches"),
        "source_commit_matches": runtime_probe.get("source_commit_matches")
        if agent_id == "track_b_paper_runtime"
        else artifact_status.get("source_commit_matches"),
        "stale_pid_detected": process_probe.get("stale_pid_detected"),
        "duplicate_process_detected": process_probe.get("duplicate_process_detected"),
        "expected_support_process_running": process_probe.get("expected_support_process_running"),
        "runtime_generation_id": generation_evidence.get("runtime_generation_id"),
        "shared_truth_generation_id": generation_evidence.get("shared_truth_generation_id"),
        "control_plane_snapshot_id": generation_evidence.get("control_plane_snapshot_id"),
        "generation_evidence": generation_evidence,
        "required_for_proof": required_for_proof,
        "required_for_runtime_submit": required_for_runtime_submit,
        "blocking_for_proof": blocking_for_proof,
        "blocking_for_runtime_submit": blocking_for_runtime_submit,
        "blocking_for_recovery": blocking_for_recovery,
        "diagnostic_only": diagnostic_only,
        "warnings": _list(artifact_status.get("warnings")),
        "evidence_paths": _evidence_paths(agent=agent, primary_path=primary_path),
        "reason": reason,
    }


def _expected_state(agent: Mapping[str, Any]) -> str:
    agent_id = str(agent.get("agent_id") or "")
    category = str(agent.get("category") or "")
    if agent_id == "track_b_paper_runtime":
        return REQUIRED_RUNNING
    if category == "diagnostic" or agent_id in {"position_truth_notifier", "self_healing_supervisor"}:
        return DIAGNOSTIC_OPTIONAL
    if agent_id in {"shared_truth_refresh"}:
        return ON_DEMAND
    return REQUIRED_FRESH_ARTIFACT


def _runtime_status(runtime_environment_truth: Mapping[str, Any]) -> tuple[str, str]:
    classification = str(runtime_environment_truth.get("classification") or "")
    if classification in {RUNTIME_ACTIVE_TRADE_CAPABLE, RUNTIME_ACTIVE_OBSERVATION_ONLY}:
        return HEALTHY, classification
    if classification == RUNTIME_DOWN_CLEAN:
        return STOPPED_EXPECTED, classification
    if classification == RUNTIME_DOWN_WITH_BROKER_EXPOSURE:
        return STOPPED_UNEXPECTED, classification
    if not runtime_environment_truth:
        return MISSING, "runtime_environment_truth_missing"
    return DEGRADED, classification or "runtime_environment_truth_not_clean"


def _shared_truth_active_hold_pending(shared_truth_refresh: Mapping[str, Any]) -> bool:
    classifications = _mapping(shared_truth_refresh.get("classifications"))
    return (
        classifications.get("Open Order Truth") == "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
        and classifications.get("Managed Order Registry") == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
        and classifications.get("Position Truth") == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
        and classifications.get("Managed Position Registry") == "OPEN_MANAGED_MATCHED"
        and classifications.get("Reconciliation") == "TRACK_B_PAPER_BROKER_RECONCILED"
    )


def _runtime_probe(runtime_environment_truth: Mapping[str, Any]) -> dict[str, Any]:
    runtime = _mapping(runtime_environment_truth.get("runtime"))
    return {
        "process_alive": runtime.get("pid_alive"),
        "root_matches": runtime.get("root_match"),
        "source_commit_matches": runtime.get("commit_matches_head"),
    }


def _runtime_down_clean(runtime_environment_truth: Mapping[str, Any]) -> bool:
    return runtime_environment_truth.get("classification") == RUNTIME_DOWN_CLEAN


def _phase1_status(phase1_readiness: Mapping[str, Any]) -> tuple[str, str]:
    if not phase1_readiness:
        return MISSING, "phase1_readiness_missing"
    if _phase1_market_closed(phase1_readiness):
        return HEALTHY, MARKET_CLOSED_NO_FRESH_BARS
    rows = _list(phase1_readiness.get("rows"))
    if rows and all(row.get("runtime_candles_ready") is True for row in rows):
        return HEALTHY, "phase1_runtime_candles_ready"
    return DEGRADED, _first_phase1_reason(phase1_readiness)


def _phase1_market_closed(phase1_readiness: Mapping[str, Any]) -> bool:
    market_session = _mapping(phase1_readiness.get("market_session"))
    if market_session.get("classification") == MARKET_CLOSED_NO_FRESH_BARS:
        return True
    for row in _list(phase1_readiness.get("rows")):
        for check_group_name in ("candle_checks", "feature_checks"):
            for check in _mapping(row.get(check_group_name)).values():
                if isinstance(check, Mapping) and check.get("reason") == MARKET_CLOSED_NO_FRESH_BARS:
                    return True
    return False


def _first_phase1_reason(phase1_readiness: Mapping[str, Any]) -> str:
    for row in _list(phase1_readiness.get("rows")):
        for key in ("runtime_candles_block_reason", "derived_features_block_reason"):
            reason = str(row.get(key) or "")
            if reason and reason != "READY":
                return reason
    return "phase1_runtime_data_not_ready"


def _primary_path(agent: Mapping[str, Any]) -> str | None:
    heartbeat = agent.get("heartbeat_artifact_path")
    if heartbeat:
        return str(heartbeat)
    paths = _list(agent.get("expected_artifact_paths"))
    return str(paths[0]) if paths else None


def _required_artifact_status(
    path_text: str | None,
    *,
    now: datetime,
    max_age_seconds: float,
    repo_root: Path,
    current_head: str | None,
) -> dict[str, Any]:
    if not path_text:
        return {
            "status": MISSING_ARTIFACT,
            "reason": "primary_artifact_path_missing",
            "last_seen_at": None,
            "freshness_age_seconds": None,
        }
    return _artifact_status(
        Path(path_text),
        now=now,
        max_age_seconds=max_age_seconds,
        repo_root=repo_root,
        current_head=current_head,
    )


def _broker_truth_artifact_status(
    *,
    config: TrackBAgentHealthConfig,
    now: datetime,
    current_head: str | None,
) -> dict[str, Any]:
    status_path = config.resolve(config.broker_truth_status_path)
    lease_path = config.resolve(config.broker_lease_path)
    status_payload = _read_json(status_path)
    lease_payload = _read_json(lease_path)
    status_artifact = _artifact_status(
        status_path,
        now=now,
        max_age_seconds=config.artifact_max_age_seconds,
        repo_root=config.repo_root,
        current_head=current_head,
    )
    lease_artifact = _artifact_status(
        lease_path,
        now=now,
        max_age_seconds=config.artifact_max_age_seconds,
        repo_root=config.repo_root,
        current_head=current_head,
    )
    if status_artifact.get("status") in {MISSING_ARTIFACT, ROOT_MISMATCH, SOURCE_COMMIT_MISMATCH}:
        return status_artifact
    if lease_artifact.get("status") in {MISSING_ARTIFACT, ROOT_MISMATCH, SOURCE_COMMIT_MISMATCH}:
        return lease_artifact
    lease_state = str(lease_payload.get("lease_state") or "")
    operator_required = lease_payload.get("operator_action_required") is True
    if not lease_state and str(status_payload.get("classification") or "") == "READY":
        return status_artifact
    if status_artifact.get("status") == STALE or lease_artifact.get("status") == STALE:
        return {
            **status_artifact,
            "status": STALE,
            "reason": "broker_truth_or_lease_stale",
            "lease_state": lease_state,
        }
    status_fresh = str(status_payload.get("classification") or "") == "BROKER_TRUTH_REFRESH_READY"
    lease_valid = lease_state in {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"} and operator_required is False
    if status_fresh and lease_valid:
        return {
            **status_artifact,
            "status": HEALTHY,
            "reason": "broker_truth_and_lease_fresh",
            "lease_state": lease_state,
        }
    if lease_valid:
        return {
            **status_artifact,
            "status": DEGRADED,
            "reason": "broker_lease_active_degraded_refresh_failing",
            "lease_state": lease_state,
        }
    return {
        **status_artifact,
        "status": STALE,
        "reason": "broker_truth_lease_not_active",
        "lease_state": lease_state,
    }


def _canonical_readiness_primary_artifact(agent: Mapping[str, Any], *, config: TrackBAgentHealthConfig) -> str | None:
    for path_text in _list(agent.get("expected_artifact_paths")):
        text = str(path_text)
        if text.endswith("latest_canonical_readiness.json"):
            return str(config.resolve(Path(text)))
    primary = _primary_path(agent)
    return str(config.resolve(Path(primary))) if primary else None


def _canonical_readiness_artifact_status(
    *,
    agent: Mapping[str, Any],
    config: TrackBAgentHealthConfig,
    now: datetime,
    current_head: str | None,
    runtime_environment_truth: Mapping[str, Any],
) -> dict[str, Any]:
    heartbeat_status = _artifact_status(
        config.resolve(Path(str(agent.get("heartbeat_artifact_path") or ""))),
        now=now,
        max_age_seconds=config.artifact_max_age_seconds,
        repo_root=config.repo_root,
        current_head=current_head,
    ) if agent.get("heartbeat_artifact_path") else {
        "status": MISSING_ARTIFACT,
        "reason": "heartbeat_artifact_path_missing",
        "last_seen_at": None,
        "freshness_age_seconds": None,
    }
    primary_path = _canonical_readiness_primary_artifact(agent, config=config)
    if primary_path is None:
        return {
            "status": MISSING_ARTIFACT,
            "reason": "canonical_readiness_artifact_path_missing",
            "last_seen_at": None,
            "freshness_age_seconds": None,
            **_canonical_readiness_heartbeat_evidence(heartbeat_status),
        }
    path = Path(primary_path)
    payload = _read_json(path)
    artifact = _artifact_status(
        path,
        now=now,
        max_age_seconds=config.artifact_max_age_seconds,
        repo_root=config.repo_root,
        current_head=current_head,
    )
    if artifact.get("status") in {MISSING_ARTIFACT, ROOT_MISMATCH, SOURCE_COMMIT_MISMATCH, STALE}:
        return {**artifact, **_canonical_readiness_heartbeat_evidence(heartbeat_status)}
    if not payload:
        return {
            **artifact,
            "status": MISSING_ARTIFACT,
            "reason": "canonical_readiness_artifact_missing_or_invalid",
            **_canonical_readiness_heartbeat_evidence(heartbeat_status),
        }
    if _canonical_readiness_startup_preflight_evidence_clean(
        payload=payload,
        runtime_environment_truth=runtime_environment_truth,
    ):
        return {
            **artifact,
            "status": HEALTHY,
            "reason": "canonical_readiness_artifact_fresh_startup_preflight",
            "startup_preflight_compatible": True,
            **_canonical_readiness_heartbeat_evidence(heartbeat_status, warn_if_unhealthy=True),
        }
    if str(payload.get("classification") or "") == "READY":
        return {
            **artifact,
            "status": HEALTHY,
            "reason": "canonical_readiness_artifact_fresh",
            **_canonical_readiness_heartbeat_evidence(heartbeat_status, warn_if_unhealthy=True),
        }
    if (
        _canonical_readiness_submit_capable(payload)
    ):
        return {
            **artifact,
            "status": HEALTHY,
            "reason": "canonical_readiness_artifact_fresh_submit_capable",
            **_canonical_readiness_heartbeat_evidence(heartbeat_status, warn_if_unhealthy=True),
        }
    runtime_down = runtime_environment_truth.get("classification") == RUNTIME_DOWN_CLEAN
    return {
        **artifact,
        "status": STALE,
        "reason": "canonical_readiness_artifact_not_startup_preflight_compatible"
        if runtime_down
        else "canonical_readiness_artifact_not_submit_capable",
        "startup_preflight_compatible": False if runtime_down else None,
        **_canonical_readiness_heartbeat_evidence(heartbeat_status),
    }


def _canonical_readiness_submit_capable(payload: Mapping[str, Any]) -> bool:
    if str(payload.get("canonical_readiness") or "") != "READY_SUBMIT_CAPABLE":
        return False
    if payload.get("submit_allowed") is not True:
        return False
    if _list(payload.get("blockers")) or _list(payload.get("readiness_blockers")):
        return False
    if payload.get("live_money_eligible") is True or payload.get("paper_proof_invoked") is True:
        return False
    if payload.get("broker_mutation_allowed") is True:
        return False
    return True


def _canonical_readiness_heartbeat_evidence(
    heartbeat_status: Mapping[str, Any],
    *,
    warn_if_unhealthy: bool = False,
) -> dict[str, Any]:
    status = str(heartbeat_status.get("status") or "")
    reason = str(heartbeat_status.get("reason") or "")
    warnings: list[dict[str, Any]] = []
    if warn_if_unhealthy and status != HEALTHY:
        code = (
            "canonical_readiness_refresher_heartbeat_missing"
            if status in {MISSING, MISSING_ARTIFACT}
            else "canonical_readiness_refresher_heartbeat_stale"
        )
        warnings.append(
            {
                "code": code,
                "status": status,
                "reason": reason,
                "last_seen_at": heartbeat_status.get("last_seen_at"),
                "freshness_age_seconds": heartbeat_status.get("freshness_age_seconds"),
            }
        )
    return {
        "heartbeat_status": status,
        "heartbeat_reason": reason,
        "heartbeat_last_seen_at": heartbeat_status.get("last_seen_at"),
        "heartbeat_freshness_age_seconds": heartbeat_status.get("freshness_age_seconds"),
        "warnings": warnings,
    }


def _canonical_readiness_startup_preflight_evidence_clean(
    *,
    payload: Mapping[str, Any],
    runtime_environment_truth: Mapping[str, Any],
) -> bool:
    runtime = _mapping(payload.get("runtime"))
    broker_truth = _mapping(payload.get("broker_truth"))
    reconciliation = _mapping(payload.get("phase1_reconciliation"))
    broker_session = _mapping(payload.get("broker_session_authority"))
    safety_live_money = payload.get("live_money_eligible") is True or broker_truth.get("live_money_eligible") is True
    paper_proof = payload.get("paper_proof_invoked") is True or broker_truth.get("paper_proof_invoked") is True
    if safety_live_money or paper_proof:
        return False
    runtime_down = runtime_environment_truth.get("classification") == RUNTIME_DOWN_CLEAN or runtime.get("running") is False
    if not runtime_down:
        return False
    if broker_truth.get("available") is not True or broker_truth.get("fresh") is not True:
        return False
    if broker_truth.get("positions_complete") is not True or broker_truth.get("open_orders_complete") is not True:
        return False
    if _as_int(broker_truth.get("open_order_count")) != 0:
        return False
    if "RECONCILED" not in str(reconciliation.get("classification") or ""):
        return False
    if reconciliation.get("broker_reconciled") is False:
        return False
    if _as_int(reconciliation.get("track_b_broker_open_order_count")) != 0:
        return False
    if _as_int(reconciliation.get("lifecycle_open_order_count")) != 0:
        return False
    if _as_int(reconciliation.get("lifecycle_open_position_count")) != 0:
        return False
    if _as_int(reconciliation.get("review_required_count")) != 0:
        return False
    if reconciliation.get("live_money_eligible") is True:
        return False
    if broker_session and broker_session.get("available") is not True:
        return False
    return True


def _optional_artifact_status(
    path_text: str | None,
    *,
    now: datetime,
    max_age_seconds: float,
    repo_root: Path,
    current_head: str | None,
) -> dict[str, Any]:
    if not path_text:
        return {"status": NOT_APPLICABLE, "reason": "diagnostic_optional_no_primary_artifact", "last_seen_at": None, "freshness_age_seconds": None}
    status = _artifact_status(
        Path(path_text),
        now=now,
        max_age_seconds=max_age_seconds,
        repo_root=repo_root,
        current_head=current_head,
    )
    if status["status"] in {MISSING, MISSING_ARTIFACT}:
        return {**status, "status": NOT_APPLICABLE, "reason": "diagnostic_optional_artifact_missing"}
    return status


def _artifact_status(
    path: Path,
    *,
    now: datetime,
    max_age_seconds: float,
    repo_root: Path,
    current_head: str | None,
) -> dict[str, Any]:
    payload = _read_json(path)
    if not payload:
        if path.exists():
            return _file_artifact_status(path, now=now, max_age_seconds=max_age_seconds)
        return {
            "status": MISSING_ARTIFACT,
            "reason": "artifact_missing_or_invalid",
            "last_seen_at": None,
            "freshness_age_seconds": None,
            "root_matches": None,
            "source_commit_matches": None,
        }
    identity = _artifact_identity_probe(payload=payload, repo_root=repo_root, current_head=current_head)
    if identity.get("root_matches") is False:
        return {
            "status": ROOT_MISMATCH,
            "reason": "artifact_root_mismatch",
            "last_seen_at": _time_text(payload),
            "freshness_age_seconds": None,
            **identity,
        }
    if identity.get("source_commit_matches") is False:
        return {
            "status": SOURCE_COMMIT_MISMATCH,
            "reason": "artifact_source_commit_mismatch",
            "last_seen_at": _time_text(payload),
            "freshness_age_seconds": None,
            **identity,
        }
    last_seen = _parse_time(payload.get("generated_at") or payload.get("updated_at"))
    if last_seen is None:
        return {
            "status": DEGRADED,
            "reason": "artifact_timestamp_missing",
            "last_seen_at": None,
            "freshness_age_seconds": None,
            **identity,
        }
    age = max(0.0, (now - last_seen).total_seconds())
    if age > max_age_seconds:
        return {
            "status": STALE,
            "reason": "artifact_stale",
            "last_seen_at": last_seen.isoformat(),
            "freshness_age_seconds": age,
            **identity,
        }
    return {
        "status": HEALTHY,
        "reason": "artifact_fresh",
        "last_seen_at": last_seen.isoformat(),
        "freshness_age_seconds": age,
        **identity,
    }


def _file_artifact_status(path: Path, *, now: datetime, max_age_seconds: float) -> dict[str, Any]:
    try:
        last_seen = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    except OSError:
        return {
            "status": MISSING_ARTIFACT,
            "reason": "artifact_missing_or_invalid",
            "last_seen_at": None,
            "freshness_age_seconds": None,
            "root_matches": None,
            "source_commit_matches": None,
        }
    age = max(0.0, (now - last_seen).total_seconds())
    status = HEALTHY if age <= max_age_seconds else STALE
    return {
        "status": status,
        "reason": "file_artifact_fresh" if status == HEALTHY else "file_artifact_stale",
        "last_seen_at": last_seen.isoformat(),
        "freshness_age_seconds": age,
        "root_matches": None,
        "source_commit_matches": None,
    }


def _load_or_build_process_hygiene(
    *,
    config: TrackBAgentHealthConfig,
    now: datetime,
    process_rows: Sequence[Mapping[str, Any]] | None,
    pid_running: Callable[[int], bool] | None,
) -> Mapping[str, Any]:
    if process_rows is not None or pid_running is not None:
        return build_track_b_process_surface_hygiene(
            config=TrackBProcessSurfaceHygieneConfig(
                repo_root=config.repo_root,
                output_path=config.process_hygiene_path,
            ),
            now=now,
            process_rows=process_rows,
            pid_running=pid_running,
        )
    payload = _read_json(config.resolve(config.process_hygiene_path))
    if payload:
        return payload
    return build_track_b_process_surface_hygiene(
        config=TrackBProcessSurfaceHygieneConfig(
            repo_root=config.repo_root,
            output_path=config.process_hygiene_path,
        ),
        now=now,
    )


def _process_probe(*, agent_id: str, process_hygiene: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(process_hygiene.get("summary"))
    stale_labels = {
        str(item.get("label") or "")
        for item in _list(process_hygiene.get("stale_pid_files"))
        if isinstance(item, Mapping)
    }
    if agent_id == "track_b_paper_runtime":
        count = int(summary.get("active_runtime_writer_count") or 0)
        return {
            "process_alive": count > 0,
            "stale_pid_detected": "track_b_paper_runtime" in stale_labels
            or "track_b_paper_runtime_wrapper" in stale_labels,
            "duplicate_process_detected": count > 1,
            "expected_support_process_running": None,
            "process_count": count,
        }
    if agent_id == "phase1_databento_live_candles":
        count = int(summary.get("phase1_databento_process_count") or 0)
        return _support_probe(count=count, stale=bool({"phase1_databento_service", "phase1_databento_child"} & stale_labels))
    if agent_id == "broker_truth_lease_refresher":
        count = int(summary.get("broker_truth_refresher_count") or 0)
        return _support_probe(count=count, stale="broker_truth_refresher" in stale_labels)
    if agent_id in {"canonical_readiness_refresher"}:
        count = int(summary.get("operator_dashboard_readiness_process_count") or 0)
        return _support_probe(
            count=count,
            stale=bool({"operator_readiness_refresher_service", "operator_readiness_refresher_child"} & stale_labels),
        )
    return {
        "process_alive": None,
        "stale_pid_detected": False,
        "duplicate_process_detected": False,
        "expected_support_process_running": None,
        "process_count": None,
    }


def _support_probe(*, count: int, stale: bool) -> dict[str, Any]:
    return {
        "process_alive": count > 0,
        "stale_pid_detected": stale,
        "duplicate_process_detected": False,
        "expected_support_process_running": count > 0,
        "process_count": count,
    }


def _generation_evidence(
    *,
    agent_id: str,
    runtime_environment_truth: Mapping[str, Any],
    control_plane_snapshot: Mapping[str, Any],
    shared_truth_refresh: Mapping[str, Any],
    artifact_status: Mapping[str, Any],
) -> dict[str, Any]:
    runtime = _mapping(runtime_environment_truth.get("runtime"))
    runtime_generation_id = runtime.get("runtime_generation_id") or runtime.get("restart_generation")
    return {
        "agent_id": agent_id,
        "runtime_generation_id": runtime_generation_id,
        "runtime_instance_id": runtime.get("runtime_instance_id"),
        "shared_truth_generation_id": control_plane_snapshot.get("shared_truth_refresh_generation_id")
        or shared_truth_refresh.get("refresh_generation_id"),
        "shared_truth_generated_at": control_plane_snapshot.get("shared_truth_refresh_generated_at")
        or shared_truth_refresh.get("generated_at"),
        "control_plane_snapshot_id": control_plane_snapshot.get("control_plane_snapshot_id"),
        "control_plane_snapshot_generated_at": control_plane_snapshot.get("generated_at"),
        "artifact_source_commit": artifact_status.get("artifact_source_commit"),
        "expected_source_commit": artifact_status.get("expected_source_commit"),
        "artifact_root": artifact_status.get("artifact_root"),
        "expected_root": artifact_status.get("expected_root"),
    }


def _apply_probe_overrides(
    *,
    agent_id: str,
    status: str,
    reason: str,
    runtime_environment_truth: Mapping[str, Any],
    process_probe: Mapping[str, Any],
    artifact_status: Mapping[str, Any],
) -> tuple[str, str]:
    if process_probe.get("duplicate_process_detected") is True:
        return DUPLICATE_PROCESS, "duplicate_process_detected"
    if agent_id == "track_b_paper_runtime" and process_probe.get("stale_pid_detected") is True and _runtime_down_clean(runtime_environment_truth):
        return DEGRADED, "stale_runtime_pid_detected_runtime_down_clean"
    if artifact_status.get("status") in {ROOT_MISMATCH, SOURCE_COMMIT_MISMATCH}:
        return str(artifact_status.get("status")), str(artifact_status.get("reason") or reason)
    return status, reason


def _artifact_identity_probe(
    *,
    payload: Mapping[str, Any],
    repo_root: Path,
    current_head: str | None,
) -> dict[str, Any]:
    artifact_root = _first_text(
        payload.get("repo_root"),
        payload.get("root"),
        payload.get("producer_root"),
        payload.get("expected_project_root"),
    )
    source_commit = _first_text(payload.get("source_commit"), payload.get("current_head"))
    expected_root = str(repo_root.resolve())
    root_matches = None if not artifact_root else str(Path(artifact_root).expanduser().resolve()) == expected_root
    source_commit_matches = None
    if source_commit and current_head:
        source_commit_matches = source_commit == current_head
    return {
        "artifact_root": artifact_root,
        "expected_root": expected_root,
        "root_matches": root_matches,
        "artifact_source_commit": source_commit,
        "expected_source_commit": current_head,
        "source_commit_matches": source_commit_matches,
    }


def _time_text(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("generated_at") or payload.get("updated_at")
    return str(value) if value else None


def _evidence_paths(*, agent: Mapping[str, Any], primary_path: str | None) -> list[str]:
    paths = []
    if primary_path:
        paths.append(primary_path)
    for path in _list(agent.get("expected_artifact_paths")):
        text = str(path)
        if text not in paths:
            paths.append(text)
    return paths


def _overall_classification(rows: Sequence[Mapping[str, Any]]) -> str:
    if not rows:
        return AGENT_HEALTH_UNKNOWN
    if any(row.get("blocking_for_proof") is True or row.get("blocking_for_runtime_submit") is True for row in rows):
        return AGENT_HEALTH_BLOCKING
    statuses = {str(row.get("status") or "") for row in rows}
    if statuses & {
        DEGRADED,
        STALE,
        MISSING,
        MISSING_ARTIFACT,
        STOPPED_UNEXPECTED,
        ROOT_MISMATCH,
        SOURCE_COMMIT_MISMATCH,
    }:
        return AGENT_HEALTH_DEGRADED
    return AGENT_HEALTH_READY


def _git_head(repo_root: Path) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _first_text(*values: Any) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


if __name__ == "__main__":
    raise SystemExit(main())
