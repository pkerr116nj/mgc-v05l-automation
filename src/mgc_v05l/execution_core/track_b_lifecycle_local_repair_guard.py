"""Pure guard for Track B lifecycle/local artifact repair boundaries.

This guard validates local lifecycle/manifest/ledger repair intent against the
Lifecycle State Matrix and, when an apply path can affect active runtime or
reconciliation state, requires a fresh coherent Control Plane Snapshot.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    MANUAL_OR_MALFORMED_CLEANUP,
    REVIEW_REQUIRED,
    classify_managed_position_transition,
    lifecycle_state_matrix,
)


LIFECYCLE_LOCAL_REPAIR_VALID = "LIFECYCLE_LOCAL_REPAIR_VALID"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_INVALID_TRANSITION = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_INVALID_TRANSITION"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_EVIDENCE_INCOMPLETE = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_EVIDENCE_INCOMPLETE"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_MISSING = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_MISSING"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_STALE = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_STALE"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_HARD_INVARIANT = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_HARD_INVARIANT"
LIFECYCLE_LOCAL_REPAIR_BLOCKED_TARGET_IDENTITY = "LIFECYCLE_LOCAL_REPAIR_BLOCKED_TARGET_IDENTITY"

REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class TrackBLifecycleLocalRepairGuardConfig:
    repo_root: Path = REPO_ROOT
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    max_snapshot_age_seconds: int = 300
    require_snapshot_for_apply: bool = True

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def validate_lifecycle_local_artifact_repair(
    *,
    config: TrackBLifecycleLocalRepairGuardConfig,
    current_state: str,
    target_state: str,
    evidence: Mapping[str, Any],
    apply: bool,
    active_state_affecting: bool,
    target_identity: Mapping[str, Any] | None = None,
    expected_snapshot_id: str | None = None,
    expected_shared_truth_generation_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate a local lifecycle repair intent without mutating artifacts."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    matrix = lifecycle_state_matrix()
    normalized_current = _normalize_state(current_state)
    normalized_target = _normalize_state(target_state)
    base = {
        "schema_version": "track_b_lifecycle_local_repair_guard_v1",
        "validated_at": actual_now.isoformat(),
        "paper_only": True,
        "read_only": True,
        "apply_requested": bool(apply),
        "active_state_affecting": bool(active_state_affecting),
        "current_state": normalized_current,
        "target_state": normalized_target,
        "target_identity": _normalize_identity(target_identity or {}),
        "dashboard_projection_consumed": False,
        "source_authority": "execution_core_authority",
        "lifecycle_state_matrix_source": "mgc_v05l.execution_core.track_b_lifecycle_state_transition",
        "control_plane_snapshot_required": bool(
            apply and active_state_affecting and config.require_snapshot_for_apply
        ),
        "control_plane_snapshot_id": None,
        "shared_truth_refresh_generation_id": None,
        "snapshot_coherence_status": None,
        "snapshot_age_seconds": None,
        "matrix_rule": matrix.get(normalized_target),
        "transition": {},
        "clean_trade_stats_allowed": False,
        "managed_position_registry_allowed": False,
        "reconciliation_clean_eligible": False,
        "blockers": [],
    }

    if normalized_current not in matrix:
        return _result(base, LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE, "Current lifecycle state is not in the Lifecycle State Matrix.", [normalized_current])
    if normalized_target not in matrix:
        return _result(base, LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE, "Target lifecycle state is not in the Lifecycle State Matrix.", [normalized_target])

    allowed_from = tuple(matrix[normalized_target].get("allowed_from") or ())
    if "*" not in allowed_from and normalized_current not in allowed_from:
        return _result(
            base,
            LIFECYCLE_LOCAL_REPAIR_BLOCKED_INVALID_TRANSITION,
            "Lifecycle transition is not allowed by the Lifecycle State Matrix.",
            [f"{normalized_current}->{normalized_target}"],
        )

    evidence_with_target = dict(evidence)
    evidence_with_target["requested_lifecycle_status"] = normalized_target
    transition = classify_managed_position_transition(evidence_with_target)
    base["transition"] = _transition_payload(transition)
    base["clean_trade_stats_allowed"] = transition.clean_trade_stats_allowed
    base["managed_position_registry_allowed"] = transition.managed_position_registry_allowed
    base["reconciliation_clean_eligible"] = transition.reconciliation_clean_eligible
    if transition.classification != normalized_target or transition.blockers:
        return _result(
            base,
            LIFECYCLE_LOCAL_REPAIR_BLOCKED_EVIDENCE_INCOMPLETE,
            "Required lifecycle evidence is incomplete for the requested target state.",
            list(transition.blockers or (transition.classification,)),
        )

    if base["control_plane_snapshot_required"]:
        snapshot_check = _snapshot_check(
            config=config,
            expected_snapshot_id=expected_snapshot_id,
            expected_shared_truth_generation_id=expected_shared_truth_generation_id,
            target_identity=target_identity or {},
            now=actual_now,
        )
        base.update(snapshot_check["evidence"])
        if not snapshot_check["valid"]:
            return _result(base, snapshot_check["classification"], snapshot_check["reason"], snapshot_check["blockers"])

    return _result(base, LIFECYCLE_LOCAL_REPAIR_VALID, "Lifecycle local artifact repair evidence is matrix-aligned.", [])


def _snapshot_check(
    *,
    config: TrackBLifecycleLocalRepairGuardConfig,
    expected_snapshot_id: str | None,
    expected_shared_truth_generation_id: str | None,
    target_identity: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    path = config.resolve(config.control_plane_snapshot_path)
    snapshot = _read_json(path)
    evidence = {
        "control_plane_snapshot_path": str(path),
        "control_plane_snapshot_id": snapshot.get("control_plane_snapshot_id"),
        "shared_truth_refresh_generation_id": snapshot.get("shared_truth_refresh_generation_id"),
        "snapshot_coherence_status": snapshot.get("shared_truth_coherence_status"),
        "snapshot_age_seconds": None,
    }
    if not snapshot:
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_MISSING, "Control Plane Snapshot is missing.", ["control_plane_snapshot_missing"])
    generated_at = _parse_datetime(snapshot.get("generated_at"))
    if generated_at is None:
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_STALE, "Control Plane Snapshot generated_at is missing or invalid.", ["control_plane_snapshot_generated_at"])
    age_seconds = max(0.0, (now - generated_at).total_seconds())
    evidence["snapshot_age_seconds"] = age_seconds
    if age_seconds > config.max_snapshot_age_seconds:
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_STALE, "Control Plane Snapshot is stale.", ["control_plane_snapshot_stale"])
    if snapshot.get("shared_truth_coherence_status") != "COHERENT":
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT, "Control Plane Snapshot is not coherent.", ["control_plane_snapshot_incoherent"])
    if snapshot.get("live_money_eligible") is True:
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_HARD_INVARIANT, "live_money_eligible=true is a hard invariant block.", ["live_money_eligible"])
    if _duplicate_writer(snapshot):
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_HARD_INVARIANT, "Duplicate runtime writer evidence is a hard invariant block.", ["duplicate_runtime_writer"])
    if expected_snapshot_id and expected_snapshot_id != str(snapshot.get("control_plane_snapshot_id") or ""):
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT, "Control Plane Snapshot id does not match caller expectation.", ["control_plane_snapshot_id"])
    if (
        expected_shared_truth_generation_id
        and expected_shared_truth_generation_id != str(snapshot.get("shared_truth_refresh_generation_id") or "")
    ):
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT, "Shared Truth generation id does not match caller expectation.", ["shared_truth_generation_id"])
    expected_identity = _normalize_identity(target_identity)
    snapshot_identity = _normalize_identity(_mapping(snapshot.get("target_identity")))
    if snapshot_identity and expected_identity and snapshot_identity != expected_identity:
        return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_BLOCKED_TARGET_IDENTITY, "Control Plane Snapshot target identity does not match repair target.", ["target_identity"])
    return _snapshot_result(evidence, LIFECYCLE_LOCAL_REPAIR_VALID, "Control Plane Snapshot is fresh and coherent.", [])


def _snapshot_result(evidence: Mapping[str, Any], classification: str, reason: str, blockers: list[str]) -> dict[str, Any]:
    return {
        "valid": classification == LIFECYCLE_LOCAL_REPAIR_VALID,
        "classification": classification,
        "reason": reason,
        "blockers": blockers,
        "evidence": dict(evidence),
    }


def _result(base: Mapping[str, Any], classification: str, reason: str, blockers: list[str]) -> dict[str, Any]:
    payload = dict(base)
    payload.update(
        {
            "classification": classification,
            "valid": classification == LIFECYCLE_LOCAL_REPAIR_VALID,
            "reason": reason,
            "blockers": blockers,
        }
    )
    return payload


def _normalize_state(value: str) -> str:
    state = str(value or "").strip().upper()
    if state in {"TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"}:
        return "OPEN_MANAGED"
    if state in {"TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"}:
        return "CLOSED_FLAT"
    if state in {
        "MANUALLY_FLATTENED_REVIEWED",
        "APP_ONLY_UNFILLED_REVIEWED",
        "IBKR_CONTRACT_REJECTED_REVIEWED",
        "LEAK_TEST_ADOPTED_ENTRY_SETTLED_FLAT_REVIEWED",
        "VOID_MALFORMED_STALE_ARTIFACT",
        "MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT",
        "OPPOSITE_ENTRY_OFFSET_EXISTING_POSITION_RECLASSIFIED",
    }:
        return MANUAL_OR_MALFORMED_CLEANUP
    return state


def _transition_payload(transition: Any) -> dict[str, Any]:
    return {
        "classification": transition.classification,
        "blockers": list(transition.blockers),
        "terminal": transition.terminal,
        "broker_backed": transition.broker_backed,
        "no_broker_effect": transition.no_broker_effect,
        "managed_position_registry_allowed": transition.managed_position_registry_allowed,
        "reconciliation_clean_eligible": transition.reconciliation_clean_eligible,
        "operator_action_required": transition.operator_action_required,
        "clean_trade_stats_allowed": transition.clean_trade_stats_allowed,
    }


def _normalize_identity(identity: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in identity.items() if value not in {None, ""}}


def _duplicate_writer(payload: Mapping[str, Any]) -> bool:
    if _positive_int(payload.get("duplicate_writer_count")):
        return True
    if payload.get("runtime_environment_truth_classification") == "DUPLICATE_RUNTIME_WRITERS":
        return True
    evidence = _mapping(payload.get("evidence_summary"))
    if _positive_int(evidence.get("duplicate_writer_count")):
        return True
    return evidence.get("runtime_environment_truth_classification") == "DUPLICATE_RUNTIME_WRITERS"


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


__all__ = [
    "LIFECYCLE_LOCAL_REPAIR_VALID",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_UNKNOWN_STATE",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_INVALID_TRANSITION",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_EVIDENCE_INCOMPLETE",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_MISSING",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_STALE",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_SNAPSHOT_INCOHERENT",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_HARD_INVARIANT",
    "LIFECYCLE_LOCAL_REPAIR_BLOCKED_TARGET_IDENTITY",
    "TrackBLifecycleLocalRepairGuardConfig",
    "validate_lifecycle_local_artifact_repair",
]
