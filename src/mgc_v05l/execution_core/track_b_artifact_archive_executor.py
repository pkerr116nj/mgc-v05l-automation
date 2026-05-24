"""Disabled Track B artifact archive execution boundary.

This boundary validates a dry-run archive plan and writes an execution envelope.
It never deletes, moves, bundles, compresses, archives, restarts, submits,
cancels, replaces, closes, flattens, or mutates broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_artifact_archive_planner import (
    ARCHIVE_PLAN_EMPTY,
    ARCHIVE_PLAN_READY,
    DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH,
)
from mgc_v05l.execution_core.track_b_artifact_retention_inventory import (
    HOT_ACTIVE_LIFECYCLE_PROTECTED,
    HOT_AUTHORITY_PROTECTED,
)
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_managed_position_registry import DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT


ARCHIVE_EXECUTOR_DRY_RUN_READY = "ARCHIVE_EXECUTOR_DRY_RUN_READY"
ARCHIVE_EXECUTOR_APPLY_DISABLED = "ARCHIVE_EXECUTOR_APPLY_DISABLED"
ARCHIVE_EXECUTOR_BLOCKED_PLAN_NOT_READY = "ARCHIVE_EXECUTOR_BLOCKED_PLAN_NOT_READY"
ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_AUTHORITY = "ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_AUTHORITY"
ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE = "ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE"
ARCHIVE_EXECUTOR_BLOCKED_PROTECTION_CHECK = "ARCHIVE_EXECUTOR_BLOCKED_PROTECTION_CHECK"
ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN = "ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ARTIFACT_ARCHIVE_EXECUTION_PLAN_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "artifact_retention"
    / "latest_artifact_archive_execution_plan.json"
)
DEFAULT_AGENT_HEALTH_ARTIFACT = Path("outputs") / "track_b_execution_core" / "agent_health" / "latest_agent_health.json"
NO_MANAGED_POSITIONS = "NO_MANAGED_POSITIONS"


@dataclass(frozen=True)
class TrackBArtifactArchiveExecutorConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_ARTIFACT_ARCHIVE_EXECUTION_PLAN_PATH
    archive_plan_path: Path = DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    max_plan_age_seconds: int = 900
    apply_requested: bool = False
    sample_limit: int = 20

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_artifact_archive_execution_plan(
    *,
    config: TrackBArtifactArchiveExecutorConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    archive_plan_path = config.resolve(config.archive_plan_path)
    control_plane_path = config.resolve(config.control_plane_snapshot_path)
    agent_health_path = config.resolve(config.agent_health_path)
    managed_position_path = config.resolve(config.managed_position_registry_path)
    archive_plan = _read_json(archive_plan_path)
    control_plane = _read_json(control_plane_path)
    agent_health = _read_json(agent_health_path)
    managed_positions = _read_json(managed_position_path)
    archive_execution_id = f"track_b_artifact_archive_execution_{uuid.uuid4().hex}"
    source_plan_id = str(
        archive_plan.get("archive_plan_id")
        or archive_plan.get("artifact_archive_plan_id")
        or archive_plan.get("plan_id")
        or ""
    )
    candidates = _candidate_records(archive_plan)
    blocked_candidates: list[dict[str, Any]] = []
    blocked_candidates.extend(_authority_candidate_blocks(candidates))
    blocked_candidates.extend(_active_lifecycle_candidate_blocks(candidates))
    blocked_candidates.extend(_protection_candidate_blocks(candidates))
    if _managed_positions_active(managed_positions):
        blocked_candidates.append(
            {
                "relative_path": str(managed_position_path.relative_to(config.repo_root))
                if managed_position_path.is_relative_to(config.repo_root)
                else str(managed_position_path),
                "blocked_reason": "managed position registry reports active/review lifecycle state",
            }
        )

    validation = _validate_plan(
        archive_plan=archive_plan,
        blocked_candidates=blocked_candidates,
        now=actual_now,
        max_plan_age_seconds=config.max_plan_age_seconds,
    )
    candidate_count = int(archive_plan.get("cold_archive_candidate_count") or len(candidates))
    protected_count = int(archive_plan.get("hot_authority_protected_count") or 0) + int(
        archive_plan.get("active_lifecycle_protected_count") or 0
    )
    classification = validation["classification"]
    if validation["valid"]:
        classification = (
            ARCHIVE_EXECUTOR_APPLY_DISABLED
            if candidate_count > 0 or config.apply_requested
            else ARCHIVE_EXECUTOR_DRY_RUN_READY
        )
    blocked_reason = validation["blocked_reason"]
    if validation["valid"] and classification == ARCHIVE_EXECUTOR_APPLY_DISABLED:
        blocked_reason = "APPLY_DISABLED"
    return {
        "schema_version": "track_b_artifact_archive_execution_boundary_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "archive_execution_id": archive_execution_id,
        "source_plan_id": source_plan_id,
        "classification": classification,
        "valid": validation["valid"],
        "reason": validation["reason"],
        "blocked_reason": blocked_reason,
        "candidate_count": candidate_count,
        "proposed_batches": _list(archive_plan.get("proposed_archive_batches")),
        "protected_count": protected_count,
        "blocked_candidates": blocked_candidates[: config.sample_limit],
        "would_move_files": False,
        "would_delete_files": False,
        "would_compress": False,
        "execution_enabled": False,
        "apply_enabled": False,
        "apply_requested": bool(config.apply_requested),
        "delete_enabled": False,
        "file_move_enabled": False,
        "cold_storage_write_enabled": False,
        "compression_enabled": False,
        "dashboard_projection_consumed": False,
        "projection_only": False,
        "not_routing_authority": True,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "source_artifact_paths": {
            "artifact_archive_plan": str(archive_plan_path),
            "control_plane_snapshot": str(control_plane_path),
            "agent_health": str(agent_health_path),
            "managed_position_registry": str(managed_position_path),
            "artifact_archive_execution_plan": str(config.resolve(config.output_path)),
        },
        "evidence_summary": {
            "archive_plan_classification": archive_plan.get("classification"),
            "archive_plan_generated_at": archive_plan.get("generated_at"),
            "archive_plan_age_seconds": validation.get("plan_age_seconds"),
            "archive_plan_dry_run_only": archive_plan.get("dry_run_only") is True,
            "archive_plan_execution_enabled": archive_plan.get("execution_enabled") is True,
            "control_plane_snapshot_id": control_plane.get("control_plane_snapshot_id") or "",
            "control_plane_classification": control_plane.get("classification") or "",
            "agent_health_classification": agent_health.get("classification") or "",
            "managed_position_classification": managed_positions.get("classification") or "",
        },
    }


def write_track_b_artifact_archive_execution_plan(
    *,
    config: TrackBArtifactArchiveExecutorConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), dict(payload))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write a disabled Track B artifact archive execution envelope.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--archive-plan-path", type=Path, default=DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_ARTIFACT_ARCHIVE_EXECUTION_PLAN_PATH)
    parser.add_argument("--max-plan-age-seconds", type=int, default=900)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Records apply_requested only; execution remains disabled.",
    )
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBArtifactArchiveExecutorConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        archive_plan_path=Path(args.archive_plan_path),
        output_path=Path(args.output_path),
        max_plan_age_seconds=int(args.max_plan_age_seconds),
        apply_requested=bool(args.apply),
    )
    payload = build_track_b_artifact_archive_execution_plan(config=config)
    output_path: Path | None = None
    if not bool(args.no_write):
        output_path = write_track_b_artifact_archive_execution_plan(config=config, payload=payload)
    summary = {
        "classification": payload.get("classification"),
        "candidate_count": payload.get("candidate_count"),
        "blocked_reason": payload.get("blocked_reason"),
        "would_move_files": False,
        "would_delete_files": False,
        "would_compress": False,
        "execution_enabled": False,
        "apply_enabled": False,
        "output_path": str(output_path if output_path is not None else config.resolve(config.output_path)),
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    if payload.get("classification") in {ARCHIVE_EXECUTOR_DRY_RUN_READY, ARCHIVE_EXECUTOR_APPLY_DISABLED}:
        return 0
    return 2


def _validate_plan(
    *,
    archive_plan: Mapping[str, Any],
    blocked_candidates: Sequence[Mapping[str, Any]],
    now: datetime,
    max_plan_age_seconds: int,
) -> dict[str, Any]:
    if not archive_plan:
        return _validation(
            ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN,
            False,
            "Archive plan authority artifact is missing.",
            "PLAN_MISSING",
        )
    generated_at = _parse_datetime(archive_plan.get("generated_at"))
    if generated_at is None:
        return _validation(
            ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN,
            False,
            "Archive plan generated_at is missing or invalid.",
            "PLAN_STALE",
        )
    age_seconds = max(0.0, (now - generated_at).total_seconds())
    if age_seconds > max_plan_age_seconds:
        payload = _validation(
            ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN,
            False,
            "Archive plan is stale.",
            "PLAN_STALE",
        )
        payload["plan_age_seconds"] = age_seconds
        return payload
    plan_classification = str(archive_plan.get("classification") or "")
    if plan_classification not in {ARCHIVE_PLAN_READY, ARCHIVE_PLAN_EMPTY}:
        return _validation(
            ARCHIVE_EXECUTOR_BLOCKED_PLAN_NOT_READY,
            False,
            f"Archive plan is not ready: {plan_classification}.",
            "PLAN_NOT_READY",
            plan_age_seconds=age_seconds,
        )
    if int(archive_plan.get("active_lifecycle_protected_count") or 0) > 0:
        return _validation(
            ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE,
            False,
            "Archive plan reports active lifecycle artifacts protected from archive.",
            "ACTIVE_LIFECYCLE_PROTECTED",
            plan_age_seconds=age_seconds,
        )
    if archive_plan.get("dry_run_only") is not True or archive_plan.get("execution_enabled") is True:
        return _validation(
            ARCHIVE_EXECUTOR_BLOCKED_PROTECTION_CHECK,
            False,
            "Archive plan must be dry_run_only=true and execution_enabled=false.",
            "PLAN_EXECUTION_FLAGS_UNSAFE",
            plan_age_seconds=age_seconds,
        )
    for candidate in blocked_candidates:
        reason = str(candidate.get("blocked_reason") or "")
        if "authority" in reason:
            return _validation(
                ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_AUTHORITY,
                False,
                "Archive candidates include a current/latest authority artifact.",
                "ACTIVE_AUTHORITY_CANDIDATE",
                plan_age_seconds=age_seconds,
            )
        if "lifecycle" in reason or "managed position" in reason:
            return _validation(
                ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE,
                False,
                "Archive candidates include active lifecycle or managed position evidence.",
                "ACTIVE_LIFECYCLE_CANDIDATE",
                plan_age_seconds=age_seconds,
            )
    if blocked_candidates:
        return _validation(
            ARCHIVE_EXECUTOR_BLOCKED_PROTECTION_CHECK,
            False,
            "Archive candidates failed protection validation.",
            "PROTECTION_CHECK_FAILED",
            plan_age_seconds=age_seconds,
        )
    return _validation(
        ARCHIVE_EXECUTOR_DRY_RUN_READY,
        True,
        "Archive plan is valid; execution remains disabled.",
        "",
        plan_age_seconds=age_seconds,
    )


def _validation(
    classification: str,
    valid: bool,
    reason: str,
    blocked_reason: str,
    *,
    plan_age_seconds: float | None = None,
) -> dict[str, Any]:
    return {
        "classification": classification,
        "valid": valid,
        "reason": reason,
        "blocked_reason": blocked_reason,
        "plan_age_seconds": plan_age_seconds,
    }


def _candidate_records(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = [dict(item) for item in _list(plan.get("archive_candidates_sample")) if isinstance(item, Mapping)]
    seen = {str(item.get("relative_path") or "") for item in candidates}
    for batch in _list(plan.get("proposed_archive_batches")):
        for relative_path in _list(_mapping(batch).get("sample_paths")):
            text = str(relative_path or "")
            if text and text not in seen:
                candidates.append({"relative_path": text, "plan_role": "archive_batch_sample"})
                seen.add(text)
    return candidates


def _authority_candidate_blocks(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for candidate in candidates:
        relative_path = str(candidate.get("relative_path") or "")
        if _is_current_authority_path(relative_path):
            blocked.append({**dict(candidate), "blocked_reason": "candidate is a current/latest authority artifact"})
    return blocked


def _active_lifecycle_candidate_blocks(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for candidate in candidates:
        relative_path = str(candidate.get("relative_path") or "")
        tier = str(candidate.get("retention_tier") or "")
        role = str(candidate.get("plan_role") or "")
        if tier == HOT_ACTIVE_LIFECYCLE_PROTECTED or role == "active_lifecycle":
            blocked.append({**dict(candidate), "blocked_reason": "candidate is protected active lifecycle evidence"})
            continue
        if (
            "track_b_strategy_managed_paper_lifecycle" in relative_path
            and "track_b_strategy_managed_paper_lifecycle_report.json" in relative_path
        ):
            blocked.append({**dict(candidate), "blocked_reason": "candidate may be active lifecycle evidence"})
    return blocked


def _protection_candidate_blocks(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    blocked: list[dict[str, Any]] = []
    for candidate in candidates:
        relative_path = str(candidate.get("relative_path") or "")
        if _is_unresolved_truth_path(relative_path):
            blocked.append(
                {
                    **dict(candidate),
                    "blocked_reason": "candidate may contain unresolved ownership/order/broker truth evidence",
                }
            )
            continue
        if relative_path.startswith("outputs/track_b_research/") and not _research_candidate_labeled(candidate):
            blocked.append({**dict(candidate), "blocked_reason": "research/offline candidate lacks non-runtime labels"})
            continue
        if candidate.get("protected") is True or str(candidate.get("retention_tier") or "") == HOT_AUTHORITY_PROTECTED:
            blocked.append({**dict(candidate), "blocked_reason": "candidate is protected authority evidence"})
    return blocked


def _is_current_authority_path(relative_path: str) -> bool:
    path = Path(relative_path)
    return (
        path.name.startswith("latest_")
        and (
            relative_path.startswith("outputs/track_b_execution_core/")
            or relative_path.startswith("outputs/reports/track_b_paper_broker_reconciliation/")
        )
    )


def _is_unresolved_truth_path(relative_path: str) -> bool:
    lowered = relative_path.lower()
    markers = (
        "unresolved_submit_intent_ownership",
        "ownership",
        "open_order_truth",
        "managed_orders",
        "broker_truth",
        "broker_reconciliation",
        "paper_broker_reconciliation",
    )
    return any(marker in lowered for marker in markers)


def _research_candidate_labeled(candidate: Mapping[str, Any]) -> bool:
    if candidate.get("research_only") is True and candidate.get("offline_diagnostic") is True:
        return candidate.get("not_runtime_authority") is True and candidate.get("not_routing_authority") is True
    role = str(candidate.get("plan_role") or "")
    reason = str(candidate.get("candidate_reason") or "").lower()
    return role == "research_offline" and "non-runtime" in reason


def _managed_positions_active(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or "")
    if classification and classification != NO_MANAGED_POSITIONS:
        return True
    return bool(_list(payload.get("managed_positions")) or _list(payload.get("review_required_positions")))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


if __name__ == "__main__":
    raise SystemExit(main())
