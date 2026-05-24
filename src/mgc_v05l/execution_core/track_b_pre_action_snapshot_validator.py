"""Pure pre-action Control Plane Snapshot validator for Track B PAPER.

Future autonomous recovery executors must call this boundary immediately before
any runtime restart or broker/lifecycle mutation. The validator reads
execution_core authority artifacts only, never dashboard projections, and never
executes recovery.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
)


PRE_ACTION_SNAPSHOT_VALID = "PRE_ACTION_SNAPSHOT_VALID"
PRE_ACTION_BLOCKED_SNAPSHOT_MISSING = "PRE_ACTION_BLOCKED_SNAPSHOT_MISSING"
PRE_ACTION_BLOCKED_SNAPSHOT_STALE = "PRE_ACTION_BLOCKED_SNAPSHOT_STALE"
PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT = "PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT"
PRE_ACTION_BLOCKED_PLAN_MISMATCH = "PRE_ACTION_BLOCKED_PLAN_MISMATCH"
PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH = "PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH"
PRE_ACTION_BLOCKED_HARD_INVARIANT = "PRE_ACTION_BLOCKED_HARD_INVARIANT"
PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH = "PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH"

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class TrackBPreActionSnapshotValidatorConfig:
    repo_root: Path = REPO_ROOT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def validate_track_b_pre_action_snapshot(
    *,
    config: TrackBPreActionSnapshotValidatorConfig,
    expected_plan_classification: str,
    expected_action_type: str,
    expected_target_identity: Mapping[str, Any] | None = None,
    max_snapshot_age_seconds: int = 300,
    expected_snapshot_id: str | None = None,
    expected_shared_truth_generation_id: str | None = None,
    now: datetime | None = None,
    explicit_future_executor_mode: bool = False,
) -> dict[str, Any]:
    """Validate one coherent pre-action evidence packet without mutating state."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    snapshot_path = config.resolve(config.control_plane_snapshot_path)
    plan_path = config.resolve(config.autonomous_recovery_plan_path)
    supervisor_path = config.resolve(config.runtime_supervisor_authority_path)
    snapshot = _read_json(snapshot_path)
    plan = _read_json(plan_path)
    supervisor = _read_json(supervisor_path)

    base = {
        "schema_version": "track_b_pre_action_snapshot_validation_v1",
        "validated_at": actual_now.isoformat(),
        "paper_only": True,
        "read_only": True,
        "execution_enabled": False,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "expected_plan_classification": expected_plan_classification,
        "expected_action_type": expected_action_type,
        "expected_target_identity": _normalize_identity(expected_target_identity or {}),
        "control_plane_snapshot_id": str(snapshot.get("control_plane_snapshot_id") or ""),
        "shared_truth_refresh_generation_id": str(snapshot.get("shared_truth_refresh_generation_id") or ""),
        "snapshot_coherence_status": str(snapshot.get("shared_truth_coherence_status") or ""),
        "snapshot_safe_to_start_runtime": snapshot.get("safe_to_start_runtime") is True,
        "supervisor_decision_id": str(snapshot.get("runtime_supervisor_decision_id") or ""),
        "supervisor_classification": str(snapshot.get("runtime_supervisor_classification") or ""),
        "runtime_resume_semantics_version": str(snapshot.get("runtime_resume_semantics_version") or ""),
        "runtime_resume_action_policy": str(snapshot.get("runtime_resume_action_policy") or ""),
        "runtime_resume_previous_runtime_generation_id": str(
            snapshot.get("runtime_resume_previous_runtime_generation_id") or ""
        ),
        "runtime_resume_proposed_next_runtime_generation_id": str(
            snapshot.get("runtime_resume_proposed_next_runtime_generation_id") or ""
        ),
        "runtime_resume_bounded_retry_budget_key": str(snapshot.get("runtime_resume_bounded_retry_budget_key") or ""),
        "runtime_resume_attempts_remaining": snapshot.get("runtime_resume_attempts_remaining"),
        "runtime_resume_cooldown_until": snapshot.get("runtime_resume_cooldown_until"),
        "runtime_resume_generation_reuse_allowed": snapshot.get("runtime_resume_generation_reuse_allowed") is True,
        "runtime_resume_must_start_new_generation": snapshot.get("runtime_resume_must_start_new_generation") is True,
        "self_recover_schema_version": str(snapshot.get("self_recover_schema_version") or ""),
        "recommended_recovery_action": str(snapshot.get("recommended_recovery_action") or ""),
        "self_recover_paper_action_policy": str(snapshot.get("paper_action_policy") or ""),
        "self_recover_autonomous_recovery_plan_classification": str(
            snapshot.get("self_recover_autonomous_recovery_plan_classification") or ""
        ),
        "self_recover_recovery_budget_key": str(snapshot.get("recovery_budget_key") or ""),
        "self_recover_attempts_remaining": snapshot.get("attempts_remaining"),
        "self_recover_cooldown_until": snapshot.get("cooldown_until"),
        "self_recover_quarantine_required": snapshot.get("quarantine_required") is True,
        "agent_health_top_blockers": _agent_health_blockers(snapshot),
        "agent_health_blocks_proof": snapshot.get("agent_health_blocks_proof") is True,
        "agent_health_blocks_runtime_submit": snapshot.get("agent_health_blocks_runtime_submit") is True,
        "agent_health_blocks_recovery": snapshot.get("agent_health_blocks_recovery") is True,
        "agent_health_has_duplicate_writer": snapshot.get("agent_health_has_duplicate_writer") is True,
        "planner_prioritized_blockers": list(plan.get("prioritized_blockers") or []),
        "planner_primary_blocking_agent_id": str(plan.get("primary_blocking_agent_id") or ""),
        "planner_primary_blocking_reason": str(plan.get("primary_blocking_reason") or ""),
        "planner_operator_explanation": str(plan.get("operator_explanation") or ""),
        "planner_recommended_observation_step": str(plan.get("recommended_observation_step") or ""),
        "planner_classification": str(plan.get("classification") or ""),
        "planner_action_type": "",
        "source_artifact_paths": {
            "control_plane_snapshot": str(snapshot_path),
            "autonomous_recovery_plan": str(plan_path),
            "runtime_supervisor_authority": str(supervisor_path),
        },
        "dashboard_projection_authority": False,
        "not_routing_authority": True,
    }

    if not snapshot:
        return _result(base, PRE_ACTION_BLOCKED_SNAPSHOT_MISSING, "Control Plane Snapshot authority artifact is missing.")

    snapshot_generated_at = _parse_datetime(snapshot.get("generated_at"))
    if snapshot_generated_at is None:
        return _result(base, PRE_ACTION_BLOCKED_SNAPSHOT_STALE, "Control Plane Snapshot generated_at is missing or invalid.")
    snapshot_age_seconds = max(0.0, (actual_now - snapshot_generated_at).total_seconds())
    base["snapshot_age_seconds"] = snapshot_age_seconds
    base["max_snapshot_age_seconds"] = max_snapshot_age_seconds
    if snapshot_age_seconds > max_snapshot_age_seconds:
        return _result(base, PRE_ACTION_BLOCKED_SNAPSHOT_STALE, "Control Plane Snapshot is stale.")

    if base["snapshot_coherence_status"] != "COHERENT":
        return _result(base, PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT, "Control Plane Snapshot is not coherent.")

    if _any_live_money(snapshot, plan, supervisor):
        return _result(base, PRE_ACTION_BLOCKED_HARD_INVARIANT, "live_money_eligible=true is a hard invariant block.")
    if _duplicate_writer(snapshot, supervisor):
        return _result(base, PRE_ACTION_BLOCKED_HARD_INVARIANT, "Duplicate runtime writer evidence is a hard invariant block.")

    if expected_snapshot_id and expected_snapshot_id != base["control_plane_snapshot_id"]:
        return _result(base, PRE_ACTION_BLOCKED_PLAN_MISMATCH, "Control Plane Snapshot id does not match caller expectation.")
    if (
        expected_shared_truth_generation_id
        and expected_shared_truth_generation_id != base["shared_truth_refresh_generation_id"]
    ):
        return _result(
            base,
            PRE_ACTION_BLOCKED_PLAN_MISMATCH,
            "Shared Truth generation id does not match caller expectation.",
        )

    if supervisor and supervisor.get("supervisor_decision_id") != base["supervisor_decision_id"]:
        return _result(base, PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH, "Runtime Supervisor decision id differs.")
    if supervisor and supervisor.get("classification") != base["supervisor_classification"]:
        return _result(base, PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH, "Runtime Supervisor classification differs.")

    plan_snapshot_id = str(plan.get("control_plane_snapshot_id") or "")
    plan_generation_id = str(plan.get("shared_truth_refresh_generation_id") or "")
    if not plan or plan_snapshot_id != base["control_plane_snapshot_id"]:
        return _result(base, PRE_ACTION_BLOCKED_PLAN_MISMATCH, "Planner does not reference the active snapshot id.")
    if plan_generation_id != base["shared_truth_refresh_generation_id"]:
        return _result(base, PRE_ACTION_BLOCKED_PLAN_MISMATCH, "Planner does not reference the active generation id.")
    if plan.get("classification") != expected_plan_classification:
        return _result(base, PRE_ACTION_BLOCKED_PLAN_MISMATCH, "Planner classification does not match expected action.")
    if plan.get("execution_enabled") is True and not explicit_future_executor_mode:
        return _result(base, PRE_ACTION_BLOCKED_HARD_INVARIANT, "Planner execution_enabled=true without executor mode.")

    action = _find_action(plan, expected_action_type)
    if not action:
        return _result(base, PRE_ACTION_BLOCKED_PLAN_MISMATCH, "Expected action type is not present in the plan.")
    base["planner_action_type"] = str(action.get("action_type") or "")
    base["planner_action_id"] = str(action.get("action_id") or "")
    base["planner_target_identity"] = _normalize_identity(_mapping(action.get("target_identity")))

    expected_identity = _normalize_identity(expected_target_identity or {})
    if expected_identity and expected_identity != base["planner_target_identity"]:
        return _result(
            base,
            PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH,
            "Expected target identity does not exactly match the planner target.",
        )

    return _result(base, PRE_ACTION_SNAPSHOT_VALID, "Pre-action Control Plane Snapshot evidence is valid.")


def _result(base: Mapping[str, Any], classification: str, reason: str) -> dict[str, Any]:
    payload = dict(base)
    payload.update(
        {
            "classification": classification,
            "valid": classification == PRE_ACTION_SNAPSHOT_VALID,
            "reason": reason,
        }
    )
    return payload


def _find_action(plan: Mapping[str, Any], expected_action_type: str) -> dict[str, Any]:
    for action in list(plan.get("proposed_actions") or []) + list(plan.get("blocked_actions") or []):
        if isinstance(action, Mapping) and action.get("action_type") == expected_action_type:
            return dict(action)
    return {}


def _duplicate_writer(*payloads: Mapping[str, Any]) -> bool:
    for payload in payloads:
        if payload.get("agent_health_has_duplicate_writer") is True:
            return True
        if _positive_int(payload.get("duplicate_writer_count")):
            return True
        if _positive_int(payload.get("duplicate_process_count")):
            return True
        if payload.get("runtime_environment_truth_classification") == "DUPLICATE_RUNTIME_WRITERS":
            return True
        evidence = _mapping(payload.get("evidence_summary"))
        if evidence.get("agent_health_has_duplicate_writer") is True:
            return True
        if _positive_int(evidence.get("duplicate_writer_count")):
            return True
        if _positive_int(evidence.get("duplicate_process_count")):
            return True
        if evidence.get("runtime_environment_truth_classification") == "DUPLICATE_RUNTIME_WRITERS":
            return True
    return False


def _any_live_money(*payloads: Mapping[str, Any]) -> bool:
    return any(payload.get("live_money_eligible") is True for payload in payloads)


def _normalize_identity(identity: Mapping[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in identity.items():
        if value is None or value == "":
            continue
        normalized[str(key)] = str(value)
    return normalized


def _agent_health_blockers(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    for row in list(snapshot.get("agent_health_top_blockers") or []):
        if not isinstance(row, Mapping):
            continue
        blockers.append(
            {
                "agent_id": str(row.get("agent_id") or ""),
                "display_name": str(row.get("display_name") or row.get("agent_id") or ""),
                "status": str(row.get("status") or ""),
                "reason": str(row.get("reason") or ""),
                "blocking_for_proof": row.get("blocking_for_proof") is True,
                "blocking_for_runtime_submit": row.get("blocking_for_runtime_submit") is True,
                "blocking_for_recovery": row.get("blocking_for_recovery") is True,
                "diagnostic_only": row.get("diagnostic_only") is True,
            }
        )
    return blockers


def _positive_int(value: Any) -> bool:
    try:
        return int(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
