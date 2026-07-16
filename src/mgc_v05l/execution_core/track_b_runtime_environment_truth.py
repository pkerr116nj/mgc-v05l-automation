"""Read-only Track B PAPER runtime environment truth.

Runtime Environment Truth authority lives in execution_core. Dashboard
artifacts are projections and must not be used as runtime authority.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_fresh_truth_contract import build_authority_freshness_metadata
from mgc_v05l.execution_core.track_b_projection_metadata import build_projection_metadata
from mgc_v05l.execution_core.track_b_current_state_authority import evaluate_exit_capability


RUNTIME_ACTIVE_TRADE_CAPABLE = "RUNTIME_ACTIVE_TRADE_CAPABLE"
RUNTIME_ACTIVE_OBSERVATION_ONLY = "RUNTIME_ACTIVE_OBSERVATION_ONLY"
RUNTIME_DOWN_CLEAN = "RUNTIME_DOWN_CLEAN"
RUNTIME_DOWN_WITH_BROKER_EXPOSURE = "RUNTIME_DOWN_WITH_BROKER_EXPOSURE"
DUPLICATE_RUNTIME_WRITERS = "DUPLICATE_RUNTIME_WRITERS"
STALE_PID_METADATA = "STALE_PID_METADATA"
STALE_RUNTIME_TRUTH = "STALE_RUNTIME_TRUTH"
WRONG_ROOT_RUNTIME = "WRONG_ROOT_RUNTIME"
COMMIT_MISMATCH = "COMMIT_MISMATCH"
CONFIG_MISMATCH = "CONFIG_MISMATCH"
UNKNOWN_RUNTIME_STATE = "UNKNOWN_RUNTIME_STATE"

DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "runtime_environment_events.jsonl"
)
DEFAULT_DASHBOARD_RUNTIME_ENVIRONMENT_PROJECTION = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_runtime_environment_truth.json"
)
DEFAULT_RUNTIME_TRUTH_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_runtime_truth.json"
)
DEFAULT_PID_METADATA_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper.pid.json"
)
DEFAULT_LAUNCH_STATUS_ARTIFACT = (
    Path("outputs") / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper_launch_status.json"
)
DEFAULT_OPERATOR_STATUS_ARTIFACT = Path("outputs") / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
DEFAULT_CANONICAL_READINESS_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_SELF_HEALING_HEALTH_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_DETACHED_CHILD_STATUS_ARTIFACT = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "probationary_paper_detached_child_status.json"
)
DEFAULT_RUNTIME_STARTUP_PROGRESS_EVENTS = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_post_truth_startup_progress_events.jsonl"
)
DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "broker_position_guardian" / "latest_broker_position_guardian.json"
)
DEFAULT_SAFE_STATE_ENVELOPE_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"
)
DEFAULT_MANAGED_EXIT_SERVICE_STATUS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_exit_service" / "latest_managed_exit_service_status.json"
)


@dataclass(frozen=True)
class TrackBRuntimeEnvironmentTruthConfig:
    repo_root: Path
    output_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    event_log_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_EVENTS
    dashboard_projection_path: Path | None = DEFAULT_DASHBOARD_RUNTIME_ENVIRONMENT_PROJECTION
    runtime_truth_path: Path = DEFAULT_RUNTIME_TRUTH_ARTIFACT
    pid_metadata_path: Path = DEFAULT_PID_METADATA_ARTIFACT
    launch_status_path: Path = DEFAULT_LAUNCH_STATUS_ARTIFACT
    operator_status_path: Path = DEFAULT_OPERATOR_STATUS_ARTIFACT
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_ARTIFACT
    self_healing_health_path: Path = DEFAULT_SELF_HEALING_HEALTH_ARTIFACT
    position_truth_path: Path = DEFAULT_POSITION_TRUTH_ARTIFACT
    reconciliation_path: Path = DEFAULT_RECONCILIATION_ARTIFACT
    detached_child_status_path: Path = DEFAULT_DETACHED_CHILD_STATUS_ARTIFACT
    runtime_startup_progress_events_path: Path = DEFAULT_RUNTIME_STARTUP_PROGRESS_EVENTS
    broker_truth_lease_path: Path = DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT
    open_order_truth_path: Path = DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT
    broker_position_guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    safe_state_envelope_path: Path = DEFAULT_SAFE_STATE_ENVELOPE_ARTIFACT
    managed_exit_service_status_path: Path = DEFAULT_MANAGED_EXIT_SERVICE_STATUS_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_runtime_environment_truth(
    *,
    config: TrackBRuntimeEnvironmentTruthConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    process_root_resolver: Callable[[int], Path | None] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    pid_running = pid_running or _pid_running
    process_root_resolver = process_root_resolver or _process_root
    source_commit_resolver = source_commit_resolver or _git_head

    runtime_truth = _read_json(config.resolve(config.runtime_truth_path))
    pid_metadata = _read_json(config.resolve(config.pid_metadata_path))
    launch_status = _read_json(config.resolve(config.launch_status_path))
    operator_status = _read_json(config.resolve(config.operator_status_path))
    canonical_readiness = _read_json(config.resolve(config.canonical_readiness_path))
    self_healing_health = _read_json(config.resolve(config.self_healing_health_path))
    position_truth = _read_json(config.resolve(config.position_truth_path))
    reconciliation = _read_json(config.resolve(config.reconciliation_path))
    detached_child_status = _read_json(config.resolve(config.detached_child_status_path))
    runtime_startup_progress_events = _current_runtime_progress_events(
        _read_jsonl_tail(config.resolve(config.runtime_startup_progress_events_path)),
        runtime_truth=runtime_truth,
    )
    current_runtime_status = {
        "detached_child_status": detached_child_status,
        "runtime_startup_progress_events": runtime_startup_progress_events,
    }
    broker_truth_lease = _read_json(config.resolve(config.broker_truth_lease_path))
    open_order_truth = _read_json(config.resolve(config.open_order_truth_path))
    broker_position_guardian = _read_json(config.resolve(config.broker_position_guardian_path))
    safe_state_envelope = _read_json(config.resolve(config.safe_state_envelope_path))
    managed_exit_status = _read_json(config.resolve(config.managed_exit_service_status_path))

    runtime_pid = _first_int(runtime_truth.get("producer_pid"), pid_metadata.get("pid"), operator_status.get("source_runtime_pid"))
    runtime_pid_alive = bool(runtime_pid is not None and pid_running(runtime_pid))
    process_root = process_root_resolver(runtime_pid) if runtime_pid_alive and runtime_pid is not None else None
    expected_root = config.repo_root.resolve()
    producer_root = _path_or_none(runtime_truth.get("producer_root") or pid_metadata.get("root") or pid_metadata.get("expected_project_root"))
    effective_root = process_root or producer_root
    root_match = _path_matches_root(effective_root, expected_root) if effective_root is not None else None

    runtime_truth_fresh = _runtime_truth_fresh(runtime_truth, now=actual_now)
    source_commit = str(runtime_truth.get("source_commit") or pid_metadata.get("source_commit") or "").strip()
    current_head = source_commit_resolver(config.repo_root)
    commit_matches = bool(source_commit and current_head and source_commit == current_head)
    config_matches = _config_matches(runtime_truth=runtime_truth, pid_metadata=pid_metadata, operator_status=operator_status)
    duplicate_writer = _duplicate_writer_detected(runtime_truth=runtime_truth, launch_status=launch_status)
    position_classification = _position_truth_classification(position_truth=position_truth, reconciliation=reconciliation)
    broker_exposure = _broker_exposure_present(position_truth=position_truth, reconciliation=reconciliation)
    canonical_state = str(canonical_readiness.get("canonical_readiness") or canonical_readiness.get("state") or "").strip()
    trade_capability = _build_current_state_trade_capability(
        runtime_truth=runtime_truth,
        detached_child_status=current_runtime_status,
        broker_truth_lease=broker_truth_lease,
        open_order_truth=open_order_truth,
        broker_position_guardian=broker_position_guardian,
        safe_state_envelope=safe_state_envelope,
        managed_exit_status=managed_exit_status,
        position_truth=position_truth,
        reconciliation=reconciliation,
        now=actual_now,
    )
    paper_trade_allowed = bool(trade_capability.get("trade_capable"))

    classification, blockers, warnings = _classify(
        runtime_pid=runtime_pid,
        runtime_pid_alive=runtime_pid_alive,
        runtime_truth=runtime_truth,
        runtime_truth_fresh=runtime_truth_fresh,
        root_match=root_match,
        duplicate_writer=duplicate_writer,
        commit_matches=commit_matches,
        source_commit=source_commit,
        current_head=current_head,
        config_matches=config_matches,
        position_classification=position_classification,
        broker_exposure=broker_exposure,
        paper_trade_allowed=paper_trade_allowed,
        paper_trade_blocking_reasons=_list(trade_capability.get("blocking_reasons")),
    )
    payload = {
        "schema_version": "track_b_runtime_environment_truth_v1",
        "generated_at": actual_now.isoformat(),
        **build_authority_freshness_metadata(
            generated_at=actual_now,
            observed_at=actual_now,
            source_pid=os.getpid(),
            ttl_seconds=180.0,
            authority_scope="runtime_environment_truth",
        ),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": classification,
        "blockers": blockers,
        "warnings": warnings,
        "runtime": {
            "pid": runtime_pid,
            "pid_alive": runtime_pid_alive,
            "process_root": str(process_root) if process_root is not None else None,
            "producer_root": str(producer_root) if producer_root is not None else None,
            "root_match": root_match,
            "runtime_instance_id": runtime_truth.get("runtime_instance_id") or pid_metadata.get("runtime_instance_id"),
            "restart_generation": runtime_truth.get("restart_generation") or pid_metadata.get("restart_generation"),
            "source_commit": source_commit or None,
            "current_head": current_head,
            "commit_matches_head": commit_matches,
            "config_matches": config_matches,
            "lane_count": runtime_truth.get("lane_count"),
            "test_mule_enabled": runtime_truth.get("test_mule_enabled"),
            "heartbeat_state": runtime_truth.get("heartbeat_state"),
            "freshness_state": runtime_truth.get("freshness_state"),
            "writer_authority": runtime_truth.get("writer_authority"),
            "runtime_truth_fresh": runtime_truth_fresh,
            "runtime_truth_generated_at": runtime_truth.get("generated_at"),
            "runtime_truth_age_seconds": _age_seconds(runtime_truth.get("generated_at"), actual_now),
        },
        "canonical_readiness": {
            "classification": canonical_state or None,
            "ready_submit_capable": canonical_state == "READY_SUBMIT_CAPABLE",
            "diagnostic_only_for_runtime_environment_truth": True,
            "blockers": canonical_readiness.get("readiness_blockers") or canonical_readiness.get("blockers") or [],
            "warnings": canonical_readiness.get("readiness_warnings") or canonical_readiness.get("warnings") or [],
            "generated_at": canonical_readiness.get("generated_at"),
        },
        "current_state_trade_capability": trade_capability,
        "runtime_startup_progress": {
            "event_count": len(runtime_startup_progress_events),
            "latest_event_generated_at": runtime_startup_progress_events[-1].get("generated_at")
            if runtime_startup_progress_events
            else None,
        },
        "position_truth": {
            "classification": position_classification,
            "broker_exposure_present": broker_exposure,
            "generated_at": position_truth.get("generated_at"),
            "authority_generation_id": position_truth.get("authority_generation_id"),
            "authority_cycle_generated_at": position_truth.get("authority_cycle_generated_at"),
            "summary": position_truth.get("summary") or {},
        },
        "source_generation_references": {
            "position_truth_generated_at": position_truth.get("generated_at"),
            "position_truth_authority_generation_id": position_truth.get("authority_generation_id"),
            "broker_reconciliation_generated_at": reconciliation.get("generated_at"),
        },
        "reconciliation": {
            "classification": reconciliation.get("classification"),
            "broker_reconciled": reconciliation.get("broker_reconciled"),
            "track_b_broker_position_count": reconciliation.get("track_b_broker_position_count"),
            "track_b_broker_open_order_count": reconciliation.get("track_b_broker_open_order_count"),
            "review_required_count": reconciliation.get("review_required_count"),
            "unresolved_submit_intent_ownership_count": reconciliation.get("unresolved_submit_intent_ownership_count"),
            "generated_at": reconciliation.get("generated_at"),
        },
        "launch_status": launch_status,
        "pid_metadata": pid_metadata,
        "self_healing_health": self_healing_health,
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None
            if config.dashboard_projection_path is None
            else str(config.resolve(config.dashboard_projection_path)),
            "runtime_truth": str(config.resolve(config.runtime_truth_path)),
            "pid_metadata": str(config.resolve(config.pid_metadata_path)),
            "launch_status": str(config.resolve(config.launch_status_path)),
            "operator_status": str(config.resolve(config.operator_status_path)),
            "canonical_readiness": str(config.resolve(config.canonical_readiness_path)),
            "self_healing_health": str(config.resolve(config.self_healing_health_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "detached_child_status": str(config.resolve(config.detached_child_status_path)),
            "broker_truth_lease": str(config.resolve(config.broker_truth_lease_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "broker_position_guardian": str(config.resolve(config.broker_position_guardian_path)),
            "safe_state_envelope": str(config.resolve(config.safe_state_envelope_path)),
            "managed_exit_service_status": str(config.resolve(config.managed_exit_service_status_path)),
        },
    }
    return payload


def write_track_b_runtime_environment_truth(
    *,
    config: TrackBRuntimeEnvironmentTruthConfig,
    payload: Mapping[str, Any],
    now: datetime | None = None,
) -> tuple[Path, list[dict[str, Any]]]:
    output_path = config.resolve(config.output_path)
    event_log_path = config.resolve(config.event_log_path)
    previous = _read_json(output_path)
    events = build_runtime_environment_events(previous=previous, current=payload, now=now)
    _write_json_atomic(output_path, dict(payload))
    if config.dashboard_projection_path is not None:
        _write_json_atomic(
            config.resolve(config.dashboard_projection_path),
            build_dashboard_runtime_environment_projection(authority_payload=payload, authority_path=output_path),
        )
    if events:
        for event in events:
            append_bounded_jsonl(event_log_path, event)
    return output_path, events


def build_dashboard_runtime_environment_projection(*, authority_payload: Mapping[str, Any], authority_path: Path) -> dict[str, Any]:
    return {
        **dict(authority_payload),
        "schema_version": "track_b_runtime_environment_truth_dashboard_projection_v1",
        **build_projection_metadata(source_authority_path=authority_path),
    }


def build_runtime_environment_events(
    *,
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    previous_class = (previous or {}).get("classification")
    current_class = current.get("classification")
    if current_class == previous_class:
        return []
    actual_now = _ensure_utc(now or datetime.now(UTC))
    return [
        {
            "schema_version": "track_b_runtime_environment_event_v1",
            "event_type": "RUNTIME_ENVIRONMENT_CLASSIFICATION_CHANGED",
            "generated_at": actual_now.isoformat(),
            "previous_classification": previous_class,
            "classification": current_class,
            "runtime_instance_id": _mapping(current.get("runtime")).get("runtime_instance_id"),
            "runtime_pid": _mapping(current.get("runtime")).get("pid"),
            "read_only": True,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
    ]


def _classify(
    *,
    runtime_pid: int | None,
    runtime_pid_alive: bool,
    runtime_truth: Mapping[str, Any],
    runtime_truth_fresh: bool,
    root_match: bool | None,
    duplicate_writer: bool,
    commit_matches: bool,
    source_commit: str,
    current_head: str | None,
    config_matches: bool,
    position_classification: str,
    broker_exposure: bool,
    paper_trade_allowed: bool,
    paper_trade_blocking_reasons: list[Any],
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    def block(code: str, detail: str) -> None:
        blockers.append({"code": code, "detail": detail})

    def warn(code: str, detail: str) -> None:
        warnings.append({"code": code, "detail": detail})

    if duplicate_writer:
        block("duplicate_runtime_writers", "Duplicate Track B PAPER runtime writer evidence is present.")
        return DUPLICATE_RUNTIME_WRITERS, blockers, warnings
    if runtime_pid_alive and root_match is False:
        block("wrong_root_runtime", "Live Track B PAPER runtime is not running from the expected Dev root.")
        return WRONG_ROOT_RUNTIME, blockers, warnings
    if runtime_pid_alive and source_commit and current_head and not commit_matches:
        block("commit_mismatch", "Live Track B PAPER runtime source commit differs from current HEAD.")
        return COMMIT_MISMATCH, blockers, warnings
    if runtime_pid_alive and not config_matches:
        block("config_mismatch", "Runtime generation/config evidence is inconsistent across artifacts.")
        return CONFIG_MISMATCH, blockers, warnings
    if runtime_pid_alive and runtime_truth and not runtime_truth_fresh:
        block("stale_runtime_truth", "Runtime process is present but runtime truth heartbeat is stale.")
        return STALE_RUNTIME_TRUTH, blockers, warnings
    if runtime_pid is not None and not runtime_pid_alive and runtime_truth:
        warn("stale_pid_metadata", "Runtime/PID metadata exists but the process is not alive.")
        if broker_exposure:
            block("broker_exposure_without_runtime", "Broker exposure exists while runtime is down.")
            return RUNTIME_DOWN_WITH_BROKER_EXPOSURE, blockers, warnings
        return RUNTIME_DOWN_CLEAN, blockers, warnings
    if not runtime_pid_alive:
        if broker_exposure:
            block("broker_exposure_without_runtime", "Broker exposure exists while runtime is down.")
            return RUNTIME_DOWN_WITH_BROKER_EXPOSURE, blockers, warnings
        return RUNTIME_DOWN_CLEAN, blockers, warnings
    if not runtime_truth:
        block("runtime_truth_missing", "Runtime process is alive but runtime truth artifact is missing.")
        return UNKNOWN_RUNTIME_STATE, blockers, warnings
    if paper_trade_allowed:
        return RUNTIME_ACTIVE_TRADE_CAPABLE, blockers, warnings
    reason_text = ", ".join(str(reason) for reason in paper_trade_blocking_reasons) or position_classification
    warn(
        "runtime_observation_only",
        f"Runtime is alive but current-state submit capability is not clean: {reason_text}.",
    )
    return RUNTIME_ACTIVE_OBSERVATION_ONLY, blockers, warnings


def _runtime_truth_fresh(payload: Mapping[str, Any], *, now: datetime) -> bool:
    if not payload:
        return False
    generated_at = payload.get("last_success_at") or payload.get("generated_at")
    age_seconds = _age_seconds(generated_at, now)
    ttl = _float(payload.get("freshness_ttl_seconds"), 180.0)
    return bool(age_seconds is not None and age_seconds <= ttl and payload.get("heartbeat_state") == "HEALTHY")


def _duplicate_writer_detected(*, runtime_truth: Mapping[str, Any], launch_status: Mapping[str, Any]) -> bool:
    duplicate = _mapping(runtime_truth.get("duplicate_writer_detection"))
    if runtime_truth.get("writer_authority") not in {None, "", "SINGLE_WRITER"}:
        return True
    return any(
        value is True
        for value in (
            duplicate.get("duplicate_writer_detected"),
            duplicate.get("duplicate_writers_detected"),
            launch_status.get("duplicate_writer_detected"),
        )
    )


def _config_matches(*, runtime_truth: Mapping[str, Any], pid_metadata: Mapping[str, Any], operator_status: Mapping[str, Any]) -> bool:
    fingerprints = [
        str(value).strip()
        for value in (
            runtime_truth.get("config_fingerprint"),
            pid_metadata.get("config_fingerprint"),
            operator_status.get("config_fingerprint"),
        )
        if str(value or "").strip()
    ]
    return len(set(fingerprints)) <= 1


def _position_truth_classification(*, position_truth: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> str:
    summary = _mapping(position_truth.get("summary"))
    return str(
        summary.get("overall_classification")
        or position_truth.get("classification")
        or reconciliation.get("classification")
        or "UNKNOWN"
    )


def _broker_exposure_present(*, position_truth: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> bool:
    summary = _mapping(position_truth.get("summary"))
    if summary.get("broker_exposure_present") is True:
        return True
    if _list(position_truth.get("broker_positions")) or _list(position_truth.get("open_broker_orders")):
        return True
    return bool(
        int(reconciliation.get("track_b_broker_position_count") or 0) > 0
        or int(reconciliation.get("track_b_broker_open_order_count") or 0) > 0
        or int(reconciliation.get("unknown_broker_open_order_count") or 0) > 0
    )



def _build_current_state_trade_capability(
    *,
    runtime_truth: Mapping[str, Any],
    detached_child_status: Mapping[str, Any],
    broker_truth_lease: Mapping[str, Any],
    open_order_truth: Mapping[str, Any],
    broker_position_guardian: Mapping[str, Any],
    safe_state_envelope: Mapping[str, Any],
    managed_exit_status: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    reasons: list[str] = []

    trading_loop_entered = _current_state_has_value(
        detached_child_status, "TRADING_LOOP_ENTERED"
    ) or _current_state_has_value(runtime_truth, "TRADING_LOOP_ENTERED")
    submit_authority = _current_state_has_key_value(
        detached_child_status, "submit_authority", True
    ) or _current_state_has_key_value(runtime_truth, "submit_authority", True)
    submit_authority_source = _first_text_recursive(
        detached_child_status,
        "submit_authority_source",
        fallback=_first_text_recursive(runtime_truth, "submit_authority_source"),
    )
    if not trading_loop_entered:
        reasons.append("trading_loop_not_entered")
    if not submit_authority:
        reasons.append("current_state_submit_authority_false")
    if submit_authority and submit_authority_source and submit_authority_source != "current_state_authority":
        reasons.append(f"submit_authority_source_not_current_state:{submit_authority_source}")

    lease_state = str(broker_truth_lease.get("lease_state") or broker_truth_lease.get("classification") or "").strip()
    if lease_state != "ACTIVE":
        reasons.append(f"broker_truth_lease_not_active:{lease_state or 'missing'}")
    if broker_truth_lease.get("submit_entry_allowed") is False:
        reasons.append("broker_truth_lease_submit_entry_not_allowed")
    if broker_truth_lease.get("broker_reconciled") is False:
        reasons.append("broker_truth_lease_not_reconciled")
    if _count_gt_zero(broker_truth_lease, "unknown_broker_open_order_count"):
        reasons.append("broker_truth_lease_unknown_orders_present")
    if _count_gt_zero(broker_truth_lease, "track_b_broker_open_order_count"):
        reasons.append("broker_truth_lease_open_orders_present")
    if _list(broker_truth_lease.get("blockers")):
        reasons.append("broker_truth_lease_blockers_present")

    open_order_class = str(open_order_truth.get("classification") or open_order_truth.get("state") or "").strip()
    if open_order_class and open_order_class != "NO_OPEN_ORDERS":
        reasons.append(f"open_order_truth_not_clean:{open_order_class}")
    if _count_gt_zero(open_order_truth, "unknown_order_count") or _count_gt_zero(open_order_truth, "unknown_broker_open_order_count"):
        reasons.append("open_order_truth_unknown_orders_present")
    if _count_gt_zero(open_order_truth, "open_order_count") or _count_gt_zero(open_order_truth, "track_b_broker_open_order_count"):
        reasons.append("open_order_truth_open_orders_present")
    if _list(open_order_truth.get("blockers")):
        reasons.append("open_order_truth_blockers_present")

    guardian_class = str(
        broker_position_guardian.get("classification")
        or broker_position_guardian.get("guardian_state")
        or broker_position_guardian.get("state")
        or ""
    ).strip()
    if guardian_class and guardian_class != "BROKER_POSITION_GUARDIAN_READY":
        reasons.append(f"guardian_not_ready:{guardian_class}")
    if _list(broker_position_guardian.get("blockers")):
        reasons.append("guardian_blockers_present")

    safe_state_class = str(
        safe_state_envelope.get("classification")
        or safe_state_envelope.get("safe_state")
        or safe_state_envelope.get("safe_state_classification")
        or ""
    ).strip()
    if safe_state_class and safe_state_class != "SAFE_STATE_NORMAL":
        reasons.append(f"safe_state_not_normal:{safe_state_class}")
    if safe_state_envelope.get("submit_allowed") is False or safe_state_envelope.get("entry_submit_allowed") is False:
        reasons.append("safe_state_submit_not_allowed")
    if _list(safe_state_envelope.get("blockers")):
        reasons.append("safe_state_blockers_present")

    exit_capability = evaluate_exit_capability(managed_exit_status, now=now)
    if exit_capability.get("ready") is not True:
        reasons.extend(str(reason) for reason in list(exit_capability.get("block_reasons") or []))

    position_classification = _position_truth_classification(position_truth=position_truth, reconciliation=reconciliation)
    if position_classification != "CLEAN_FLAT_READY":
        reasons.append(f"position_truth_not_clean:{position_classification}")
    if _broker_exposure_present(position_truth=position_truth, reconciliation=reconciliation):
        reasons.append("broker_exposure_present")

    reconciliation_class = str(reconciliation.get("classification") or "").strip()
    if reconciliation_class not in {"TRACK_B_PAPER_BROKER_RECONCILED", "BROKER_LIFECYCLE_RECONCILED"}:
        reasons.append(f"reconciliation_not_clean:{reconciliation_class or 'missing'}")
    if reconciliation.get("broker_reconciled") is False:
        reasons.append("reconciliation_broker_reconciled_false")
    for key in (
        "track_b_broker_position_count",
        "track_b_broker_open_order_count",
        "unknown_broker_open_order_count",
        "review_required_count",
        "unresolved_submit_intent_ownership_count",
    ):
        if _count_gt_zero(reconciliation, key):
            reasons.append(f"reconciliation_{key}_nonzero")

    return {
        "trade_capable": not reasons,
        "authority_source": "lower_level_current_state_authority",
        "canonical_readiness_dependency": False,
        "blocking_reasons": reasons,
        "trading_loop_entered": trading_loop_entered,
        "submit_authority": submit_authority,
        "submit_authority_source": submit_authority_source,
        "broker_truth_lease_state": lease_state or None,
        "open_order_truth_classification": open_order_class or None,
        "guardian_classification": guardian_class or None,
        "safe_state_classification": safe_state_class or None,
        "exit_capability": exit_capability,
        "position_truth_classification": position_classification,
        "reconciliation_classification": reconciliation_class or None,
        "generated_at": now.isoformat(),
    }


def _current_state_has_value(payload: Any, expected: Any) -> bool:
    if isinstance(payload, Mapping):
        return any(_current_state_has_value(value, expected) for value in payload.values())
    if isinstance(payload, list):
        return any(_current_state_has_value(value, expected) for value in payload)
    return payload == expected


def _current_state_has_key_value(payload: Any, key: str, expected: Any) -> bool:
    if isinstance(payload, Mapping):
        if payload.get(key) == expected:
            return True
        return any(_current_state_has_key_value(value, key, expected) for value in payload.values())
    if isinstance(payload, list):
        return any(_current_state_has_key_value(value, key, expected) for value in payload)
    return False


def _first_text_recursive(payload: Any, key: str, *, fallback: str | None = None) -> str | None:
    if isinstance(payload, Mapping):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        for child in payload.values():
            found = _first_text_recursive(child, key)
            if found:
                return found
    elif isinstance(payload, list):
        for child in payload:
            found = _first_text_recursive(child, key)
            if found:
                return found
    return fallback


def _current_runtime_progress_events(
    events: list[dict[str, Any]], *, runtime_truth: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if not events:
        return []
    runtime_pid = _first_int(runtime_truth.get("producer_pid"))
    runtime_instance_id = str(runtime_truth.get("runtime_instance_id") or "").strip()
    scoped: list[dict[str, Any]] = []
    for event in events:
        event_pid = _first_int(event.get("producer_pid"))
        event_instance_id = str(event.get("runtime_instance_id") or "").strip()
        pid_matches = runtime_pid is not None and event_pid == runtime_pid
        instance_matches = bool(runtime_instance_id and event_instance_id == runtime_instance_id)
        if pid_matches or instance_matches:
            scoped.append(event)
    return scoped


def _count_gt_zero(payload: Mapping[str, Any], key: str) -> bool:
    try:
        return int(payload.get(key) or 0) > 0
    except (TypeError, ValueError):
        return False


def _git_head(repo_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _process_root(pid: int) -> Path | None:
    try:
        result = subprocess.run(
            ["lsof", "-a", "-d", "cwd", "-p", str(pid), "-Fn"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        if line.startswith("n") and len(line) > 1:
            return Path(line[1:]).resolve()
    return None


def _path_matches_root(path: Path | None, root: Path) -> bool | None:
    if path is None:
        return None
    try:
        return os.path.commonpath([str(path.resolve()), str(root.resolve())]) == str(root.resolve())
    except (OSError, ValueError):
        return False


def _path_or_none(value: Any) -> Path | None:
    if not value:
        return None
    try:
        return Path(str(value)).expanduser().resolve()
    except OSError:
        return None


def _first_int(*values: Any) -> int | None:
    for value in values:
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_time(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds())


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
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


def _float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl_tail(path: Path, *, max_lines: int = 200) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    events: list[dict[str, Any]] = []
    for line in lines[-max_lines:]:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            events.append(payload)
    return events


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)
