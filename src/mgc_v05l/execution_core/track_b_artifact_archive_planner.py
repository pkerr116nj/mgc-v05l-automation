"""Dry-run Track B artifact archive planner.

The planner consumes retention inventory and execution_core authority context
to identify cold-storage candidates. It never deletes, moves, bundles,
compresses, restarts, submits, cancels, replaces, closes, flattens, or mutates
broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_artifact_retention_inventory import (
    COLD_ARCHIVE_CANDIDATE,
    HOT_ACTIVE_LIFECYCLE_PROTECTED,
    HOT_AUTHORITY_PROTECTED,
    TrackBArtifactRetentionInventoryConfig,
    build_track_b_artifact_retention_inventory,
)
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


ARCHIVE_PLAN_READY = "ARCHIVE_PLAN_READY"
ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY = "ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY"
ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE = "ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE"
ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT = "ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT"
ARCHIVE_PLAN_EMPTY = "ARCHIVE_PLAN_EMPTY"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ARTIFACT_RETENTION_INVENTORY_PATH = (
    Path("outputs") / "track_b_execution_core" / "artifact_retention" / "latest_artifact_retention_inventory.json"
)
DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH = (
    Path("outputs") / "track_b_execution_core" / "artifact_retention" / "latest_artifact_archive_plan.json"
)
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
)
DEFAULT_AGENT_HEALTH_ARTIFACT = Path("outputs") / "track_b_execution_core" / "agent_health" / "latest_agent_health.json"
DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_recovery_attempt_history.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_RESEARCH_ROOTS = (Path("outputs") / "track_b_research",)
DEFAULT_COLD_CANDIDATE_DAYS = 30
NO_MANAGED_POSITIONS = "NO_MANAGED_POSITIONS"

ADDITIONAL_HOT_AUTHORITY_PATHS: tuple[Path, ...] = (
    DEFAULT_ARTIFACT_RETENTION_INVENTORY_PATH,
    DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH,
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
    DEFAULT_AGENT_HEALTH_ARTIFACT,
    DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT,
    DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT,
    Path("outputs/track_b_execution_core/paper_recovery_policy/latest_paper_recovery_policy.json"),
    Path("outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json"),
    Path("outputs/track_b_execution_core/recovery_budget/latest_recovery_budget_ledger.json"),
)


@dataclass(frozen=True)
class TrackBArtifactArchivePlannerConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH
    inventory_path: Path = DEFAULT_ARTIFACT_RETENTION_INVENTORY_PATH
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    agent_health_path: Path = DEFAULT_AGENT_HEALTH_ARTIFACT
    recovery_attempt_history_path: Path = DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT
    managed_position_registry_path: Path = DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT
    research_roots: tuple[Path, ...] = DEFAULT_RESEARCH_ROOTS
    cold_candidate_days: int = DEFAULT_COLD_CANDIDATE_DAYS
    max_research_scanned_files: int = 5000
    sample_limit: int = 10

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_artifact_archive_plan(
    *,
    config: TrackBArtifactArchivePlannerConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    inventory = _read_json(config.resolve(config.inventory_path))
    if not inventory:
        inventory = build_track_b_artifact_retention_inventory(
            config=TrackBArtifactRetentionInventoryConfig(
                repo_root=config.repo_root,
                output_path=config.inventory_path,
                cold_candidate_days=config.cold_candidate_days,
            ),
            now=actual_now,
        )
    control_plane_snapshot = _read_json(config.resolve(config.control_plane_snapshot_path))
    agent_health = _read_json(config.resolve(config.agent_health_path))
    recovery_attempt_history = _read_json(config.resolve(config.recovery_attempt_history_path))
    managed_positions = _read_json(config.resolve(config.managed_position_registry_path))

    hot_protected, active_lifecycle_protected = _protected_records(config=config, inventory=inventory, now=actual_now)
    inventory_candidates = _list(inventory.get("archive_candidates"))
    warm_diagnostics = list(_list(inventory.get("warm_diagnostics")))
    research_candidates, research_warm, research_blocked, research_warnings = _research_candidates(
        config=config,
        now=actual_now,
    )
    cold_candidates: list[dict[str, Any]] = []
    blocked_candidates: list[dict[str, Any]] = list(research_blocked)
    protected_relative_paths = {str(item.get("relative_path") or "") for item in [*hot_protected, *active_lifecycle_protected]}

    for item in inventory_candidates:
        candidate = _candidate_record(item=item, source="retention_inventory", reason="older than retention window")
        relative_path = str(candidate.get("relative_path") or "")
        if relative_path in protected_relative_paths or _is_latest_authority_path(relative_path):
            blocked_candidates.append(
                {
                    **candidate,
                    "blocked": True,
                    "blocked_reason": "current/latest authority artifacts are protected from cold archive",
                }
            )
            continue
        cold_candidates.append(candidate)

    cold_candidates.extend(research_candidates)
    warm_diagnostics.extend(research_warm)
    scan_warnings = [*_list(inventory.get("warnings")), *research_warnings]
    active_lifecycle_blocked = _has_unresolved_lifecycle(
        active_lifecycle_protected=active_lifecycle_protected,
        managed_positions=managed_positions,
    )
    active_authority_blocked = any(
        str(item.get("blocked_reason") or "").startswith("current/latest authority") for item in blocked_candidates
    )
    classification = _classification(
        scan_warnings=scan_warnings,
        active_lifecycle_blocked=active_lifecycle_blocked,
        active_authority_blocked=active_authority_blocked,
        cold_candidate_count=len(cold_candidates),
    )
    estimated_bytes = sum(int(item.get("size_bytes") or 0) for item in cold_candidates)
    return {
        "schema_version": "track_b_artifact_archive_plan_v2",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "dry_run_only": True,
        "execution_enabled": False,
        "delete_enabled": False,
        "file_move_enabled": False,
        "cold_storage_write_enabled": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "dashboard_projection_consumed": False,
        "classification": classification,
        "hot_authority_protected_count": len(hot_protected),
        "active_lifecycle_protected_count": len(active_lifecycle_protected),
        "warm_diagnostic_count": len(warm_diagnostics),
        "cold_archive_candidate_count": len(cold_candidates),
        "blocked_candidate_count": len(blocked_candidates),
        "archive_candidates_sample": cold_candidates[: config.sample_limit],
        "blocked_candidates_sample": blocked_candidates[: config.sample_limit],
        "proposed_archive_batches": _archive_batches(cold_candidates),
        "estimated_bytes": estimated_bytes,
        "hot_authority_protected_sample": hot_protected[: config.sample_limit],
        "active_lifecycle_protected_sample": active_lifecycle_protected[: config.sample_limit],
        "warnings": scan_warnings,
        "evidence_summary": {
            "inventory_classification": inventory.get("classification"),
            "control_plane_snapshot_id": control_plane_snapshot.get("control_plane_snapshot_id"),
            "control_plane_classification": control_plane_snapshot.get("classification"),
            "agent_health_classification": agent_health.get("classification"),
            "latest_recovery_attempt_id": recovery_attempt_history.get("latest_recovery_attempt_id") or "",
            "managed_position_classification": managed_positions.get("classification"),
            "managed_position_count": _summary_int(managed_positions, "managed_position_count"),
            "review_required_count": _summary_int(managed_positions, "review_required_count"),
        },
        "source_artifact_paths": {
            "artifact_retention_inventory": str(config.resolve(config.inventory_path)),
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "agent_health": str(config.resolve(config.agent_health_path)),
            "recovery_attempt_history": str(config.resolve(config.recovery_attempt_history_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "archive_plan": str(config.resolve(config.output_path)),
        },
    }


def write_track_b_artifact_archive_plan(
    *,
    config: TrackBArtifactArchivePlannerConfig,
    payload: Mapping[str, Any],
) -> Path:
    return write_json_atomic(config.resolve(config.output_path), dict(payload))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write a dry-run Track B PAPER artifact archive plan.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_ARTIFACT_ARCHIVE_PLAN_PATH)
    parser.add_argument("--inventory-path", type=Path, default=DEFAULT_ARTIFACT_RETENTION_INVENTORY_PATH)
    parser.add_argument("--cold-candidate-days", type=int, default=DEFAULT_COLD_CANDIDATE_DAYS)
    parser.add_argument("--max-research-scanned-files", type=int, default=5000)
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBArtifactArchivePlannerConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        inventory_path=Path(args.inventory_path),
        cold_candidate_days=int(args.cold_candidate_days),
        max_research_scanned_files=int(args.max_research_scanned_files),
    )
    payload = build_track_b_artifact_archive_plan(config=config)
    output_path: Path | None = None
    if not bool(args.no_write):
        output_path = write_track_b_artifact_archive_plan(config=config, payload=payload)
    summary = {
        "classification": payload.get("classification"),
        "hot_authority_protected_count": payload.get("hot_authority_protected_count"),
        "active_lifecycle_protected_count": payload.get("active_lifecycle_protected_count"),
        "warm_diagnostic_count": payload.get("warm_diagnostic_count"),
        "cold_archive_candidate_count": payload.get("cold_archive_candidate_count"),
        "blocked_candidate_count": payload.get("blocked_candidate_count"),
        "estimated_bytes": payload.get("estimated_bytes"),
        "output_path": str(output_path or config.resolve(config.output_path)),
        "dry_run_only": True,
        "execution_enabled": False,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0 if payload.get("classification") in {ARCHIVE_PLAN_READY, ARCHIVE_PLAN_EMPTY} else 2


def _protected_records(
    *,
    config: TrackBArtifactArchivePlannerConfig,
    inventory: Mapping[str, Any],
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    hot = [_normalize_plan_record(item, plan_role="hot_authority") for item in _list(inventory.get("hot_authority_artifacts"))]
    hot_by_relative = {str(item.get("relative_path") or ""): item for item in hot}
    for path in ADDITIONAL_HOT_AUTHORITY_PATHS:
        resolved = config.resolve(path)
        relative = _relative(path=resolved, repo_root=config.repo_root)
        hot_by_relative.setdefault(
            relative,
            _path_record(
                path=resolved,
                repo_root=config.repo_root,
                now=now,
                retention_tier=HOT_AUTHORITY_PROTECTED,
                plan_role="hot_authority",
                protected=True,
                reason="current/latest execution_core authority artifact",
            ),
        )
    active = [
        _normalize_plan_record(item, plan_role="active_lifecycle")
        for item in _list(inventory.get("active_lifecycle_artifacts"))
    ]
    return sorted(hot_by_relative.values(), key=lambda item: str(item.get("relative_path") or "")), active


def _research_candidates(
    *,
    config: TrackBArtifactArchivePlannerConfig,
    now: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    cold: list[dict[str, Any]] = []
    warm: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    scanned = 0
    for root in config.research_roots:
        resolved_root = config.resolve(root)
        if not resolved_root.exists():
            continue
        for path in sorted(resolved_root.rglob("*")):
            if not path.is_file():
                continue
            scanned += 1
            if scanned > config.max_research_scanned_files:
                warnings.append(
                    {
                        "code": "research_scan_file_cap_reached",
                        "max_research_scanned_files": config.max_research_scanned_files,
                        "last_root": str(resolved_root),
                    }
                )
                return cold, warm, blocked, warnings
            if path.suffix.lower() != ".json":
                continue
            payload = _read_json(path)
            record = _path_record(
                path=path,
                repo_root=config.repo_root,
                now=now,
                retention_tier=COLD_ARCHIVE_CANDIDATE,
                plan_role="research_offline",
                protected=False,
                reason="research/offline metadata marks this artifact non-runtime",
            )
            if not _is_research_offline_payload(payload):
                blocked.append(
                    {
                        **record,
                        "blocked": True,
                        "blocked_reason": "research artifact lacks explicit non-runtime/offline authority metadata",
                    }
                )
                continue
            if _age_days(path=path, now=now) >= config.cold_candidate_days:
                cold.append(record)
            else:
                warm.append({**record, "retention_tier": "WARM_DIAGNOSTIC_RETAIN", "archive_candidate": False})
    return cold, warm, blocked, warnings


def _classification(
    *,
    scan_warnings: Sequence[Mapping[str, Any]],
    active_lifecycle_blocked: bool,
    active_authority_blocked: bool,
    cold_candidate_count: int,
) -> str:
    if scan_warnings:
        return ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT
    if active_lifecycle_blocked:
        return ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE
    if active_authority_blocked:
        return ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY
    if cold_candidate_count > 0:
        return ARCHIVE_PLAN_READY
    return ARCHIVE_PLAN_EMPTY


def _has_unresolved_lifecycle(
    *,
    active_lifecycle_protected: Sequence[Mapping[str, Any]],
    managed_positions: Mapping[str, Any],
) -> bool:
    if active_lifecycle_protected:
        return True
    classification = str(managed_positions.get("classification") or "")
    if classification and classification != NO_MANAGED_POSITIONS:
        return True
    return bool(_list(managed_positions.get("managed_positions")) or _list(managed_positions.get("review_required_positions")))


def _candidate_record(*, item: Mapping[str, Any], source: str, reason: str) -> dict[str, Any]:
    return {
        **_normalize_plan_record(item, plan_role="cold_archive_candidate"),
        "source": source,
        "candidate_reason": reason,
        "blocked": False,
    }


def _archive_batches(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if not candidates:
        return []
    total_bytes = sum(int(item.get("size_bytes") or 0) for item in candidates)
    return [
        {
            "batch_id": "track_b_cold_archive_batch_001",
            "candidate_count": len(candidates),
            "estimated_bytes": total_bytes,
            "dry_run_only": True,
            "execution_enabled": False,
            "sample_paths": [str(item.get("relative_path") or "") for item in candidates[:10]],
        }
    ]


def _is_latest_authority_path(relative_path: str) -> bool:
    path = Path(relative_path)
    if path.name.startswith("latest_") and (
        relative_path.startswith("outputs/track_b_execution_core/")
        or relative_path.startswith("outputs/reports/track_b_paper_broker_reconciliation/")
        or relative_path.startswith("outputs/operator_dashboard/runtime/")
    ):
        return True
    return relative_path in {str(path) for path in ADDITIONAL_HOT_AUTHORITY_PATHS}


def _is_research_offline_payload(payload: Mapping[str, Any]) -> bool:
    metadata = _mapping(payload.get("research_offline_metadata"))
    source_category = str(payload.get("source_category") or metadata.get("source_category") or "").lower()
    return (
        payload.get("research_only") is True
        and payload.get("offline_diagnostic") is True
        and (payload.get("not_runtime_authority") is True or metadata.get("not_runtime_authority") is True)
        and (payload.get("not_routing_authority") is True or metadata.get("not_routing_authority") is True)
        and (source_category in {"research/offline", "research", "offline"} or source_category.startswith("research/"))
    )


def _normalize_plan_record(item: Mapping[str, Any], *, plan_role: str) -> dict[str, Any]:
    return {
        "relative_path": item.get("relative_path") or "",
        "absolute_path": item.get("absolute_path") or "",
        "exists": item.get("exists") is True,
        "size_bytes": item.get("size_bytes"),
        "last_modified_at": item.get("last_modified_at"),
        "age_seconds": item.get("age_seconds"),
        "age_days": item.get("age_days"),
        "retention_tier": item.get("retention_tier"),
        "protected": item.get("protected") is True,
        "archive_candidate": item.get("archive_candidate") is True,
        "protection_reason": item.get("protection_reason"),
        "plan_role": plan_role,
    }


def _path_record(
    *,
    path: Path,
    repo_root: Path,
    now: datetime,
    retention_tier: str,
    plan_role: str,
    protected: bool,
    reason: str | None,
) -> dict[str, Any]:
    exists = path.exists()
    age_seconds = _age_seconds(path=path, now=now) if exists else None
    return {
        "relative_path": _relative(path=path, repo_root=repo_root),
        "absolute_path": str(path),
        "exists": exists,
        "size_bytes": path.stat().st_size if exists and path.is_file() else None,
        "last_modified_at": _mtime_iso(path) if exists else None,
        "age_seconds": age_seconds,
        "age_days": None if age_seconds is None else round(age_seconds / 86400, 3),
        "retention_tier": retention_tier,
        "protected": protected,
        "archive_candidate": retention_tier == COLD_ARCHIVE_CANDIDATE,
        "protection_reason": reason,
        "plan_role": plan_role,
    }


def _summary_int(payload: Mapping[str, Any], key: str) -> int:
    summary = _mapping(payload.get("summary"))
    try:
        return int(summary.get(key) or payload.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()


def _age_seconds(*, path: Path, now: datetime) -> float:
    return max(0.0, (now - datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)).total_seconds())


def _age_days(*, path: Path, now: datetime) -> float:
    return _age_seconds(path=path, now=now) / 86400


def _relative(*, path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
