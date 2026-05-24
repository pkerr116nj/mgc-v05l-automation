"""Read-only Track B artifact retention inventory authority.

Artifact retention authority lives in execution_core. This inventory separates
hot authority artifacts from warm diagnostics and cold archive candidates; it
never deletes, moves, compresses, archives, submits, cancels, replaces, closes,
or mutates broker/lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    is_registry_eligible,
    is_terminal_state,
    normalize_lifecycle_state,
    requires_operator_action,
)


ARTIFACT_RETENTION_INVENTORY_READY = "ARTIFACT_RETENTION_INVENTORY_READY"
ARTIFACT_RETENTION_INVENTORY_PARTIAL = "ARTIFACT_RETENTION_INVENTORY_PARTIAL"

HOT_AUTHORITY_PROTECTED = "HOT_AUTHORITY_PROTECTED"
HOT_ACTIVE_LIFECYCLE_PROTECTED = "HOT_ACTIVE_LIFECYCLE_PROTECTED"
WARM_DIAGNOSTIC_RETAIN = "WARM_DIAGNOSTIC_RETAIN"
COLD_ARCHIVE_CANDIDATE = "COLD_ARCHIVE_CANDIDATE"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs") / "track_b_execution_core" / "artifact_retention" / "latest_artifact_retention_inventory.json"
)
DEFAULT_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)

HOT_AUTHORITY_PATHS: tuple[Path, ...] = (
    Path("outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json"),
    Path("outputs/track_b_execution_core/managed_orders/latest_managed_orders.json"),
    Path("outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json"),
    Path("outputs/track_b_execution_core/position_truth/latest_position_truth.json"),
    Path("outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json"),
    Path("outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json"),
    Path("outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"),
    Path("outputs/track_b_execution_core/agent_registry/latest_agent_registry.json"),
    Path("outputs/track_b_execution_core/agent_health/latest_agent_health.json"),
    Path("outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json"),
    Path("outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json"),
    Path("outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json"),
    Path("outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json"),
    Path("outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"),
    Path("outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"),
    Path("outputs/operator_dashboard/runtime/latest_canonical_readiness.json"),
    Path("outputs/track_b_execution_core/shared_truth/latest_track_b_shared_truth_refresh.json"),
)

WARM_DIAGNOSTIC_ROOTS: tuple[Path, ...] = (
    Path("outputs/reports"),
    Path("outputs/probationary_pattern_engine/paper_session/runtime"),
    Path("outputs/operator_dashboard/runtime"),
    Path("outputs/track_b_execution_core"),
)

@dataclass(frozen=True)
class TrackBArtifactRetentionInventoryConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    hot_authority_paths: tuple[Path, ...] = HOT_AUTHORITY_PATHS
    warm_diagnostic_roots: tuple[Path, ...] = WARM_DIAGNOSTIC_ROOTS
    lifecycle_root: Path = DEFAULT_LIFECYCLE_ROOT
    recent_diagnostics_days: int = 30
    cold_candidate_days: int = 30
    max_scanned_files: int = 25000

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_artifact_retention_inventory(
    *,
    config: TrackBArtifactRetentionInventoryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    scanned_paths, scan_warnings = _scan_candidate_paths(config)
    hot_paths = {config.resolve(path) for path in config.hot_authority_paths}
    hot_authority_artifacts = [
        _artifact_record(
            path=config.resolve(path),
            repo_root=config.repo_root,
            now=actual_now,
            retention_tier=HOT_AUTHORITY_PROTECTED,
            protected=True,
            protection_reason="current/latest execution_core authority or legacy hot decision artifact",
        )
        for path in config.hot_authority_paths
    ]
    protected_by_path = {Path(item["absolute_path"]): item for item in hot_authority_artifacts}
    active_lifecycle_artifacts: list[dict[str, Any]] = []
    warm_diagnostics: list[dict[str, Any]] = []
    archive_candidates: list[dict[str, Any]] = []

    for path in sorted(scanned_paths):
        if path in hot_paths:
            continue
        lifecycle_protection = _lifecycle_protection_reason(path=path, lifecycle_root=config.resolve(config.lifecycle_root))
        if lifecycle_protection is not None:
            record = _artifact_record(
                path=path,
                repo_root=config.repo_root,
                now=actual_now,
                retention_tier=HOT_ACTIVE_LIFECYCLE_PROTECTED,
                protected=True,
                protection_reason=lifecycle_protection,
            )
            active_lifecycle_artifacts.append(record)
            protected_by_path[path] = record
            continue
        if not path.exists() or not path.is_file():
            continue
        age_days = _age_days(path=path, now=actual_now)
        if age_days is not None and age_days >= config.cold_candidate_days:
            archive_candidates.append(
                _artifact_record(
                    path=path,
                    repo_root=config.repo_root,
                    now=actual_now,
                    retention_tier=COLD_ARCHIVE_CANDIDATE,
                    protected=False,
                    protection_reason=None,
                )
            )
        else:
            warm_diagnostics.append(
                _artifact_record(
                    path=path,
                    repo_root=config.repo_root,
                    now=actual_now,
                    retention_tier=WARM_DIAGNOSTIC_RETAIN,
                    protected=False,
                    protection_reason=f"within {config.recent_diagnostics_days} day warm diagnostic window",
                )
            )

    classification = ARTIFACT_RETENTION_INVENTORY_READY
    return {
        "schema_version": "track_b_artifact_retention_inventory_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "dry_run_only": True,
        "deletion_performed": False,
        "archive_performed": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "dashboard_projection_consumed": False,
        "authority_owner": "execution_core",
        "classification": classification,
        "policy": {
            "hot_latest_keep": "always keep current/latest authority artifacts",
            "recent_diagnostics_days": config.recent_diagnostics_days,
            "cold_candidate_days": config.cold_candidate_days,
            "archive_action": "future dry-run-gated compressed cold storage only",
            "runtime_reads_cold_archive": False,
        },
        "summary": {
            "classification": classification,
            "hot_authority_count": len(hot_authority_artifacts),
            "protected_artifact_count": len(protected_by_path),
            "active_lifecycle_protected_count": len(active_lifecycle_artifacts),
            "warm_diagnostic_count": len(warm_diagnostics),
            "cold_archive_candidate_count": len(archive_candidates),
            "scan_warning_count": len(scan_warnings),
        },
        "hot_authority_artifacts": hot_authority_artifacts,
        "active_lifecycle_artifacts": active_lifecycle_artifacts,
        "warm_diagnostics": warm_diagnostics,
        "archive_candidates": archive_candidates,
        "protected_artifacts": sorted(protected_by_path.values(), key=lambda item: item["relative_path"]),
        "warnings": scan_warnings,
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
            "scanned_roots": [str(config.resolve(path)) for path in config.warm_diagnostic_roots],
        },
    }


def write_track_b_artifact_retention_inventory(
    *,
    config: TrackBArtifactRetentionInventoryConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    _write_json_atomic(output_path, dict(payload))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Write the read-only Track B PAPER artifact retention inventory authority artifact."
    )
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--cold-candidate-days", type=int, default=30)
    parser.add_argument("--recent-diagnostics-days", type=int, default=30)
    parser.add_argument("--max-scanned-files", type=int, default=25000)
    parser.add_argument("--no-write", action="store_true", help="Build and print without writing the authority artifact.")
    parser.add_argument("--json", action="store_true", help="Print the full inventory artifact as JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBArtifactRetentionInventoryConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        cold_candidate_days=int(args.cold_candidate_days),
        recent_diagnostics_days=int(args.recent_diagnostics_days),
        max_scanned_files=int(args.max_scanned_files),
    )
    payload = build_track_b_artifact_retention_inventory(config=config)
    authority_path: Path | None = None
    if not bool(args.no_write):
        authority_path = write_track_b_artifact_retention_inventory(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "hot_authority_count": payload.get("summary", {}).get("hot_authority_count"),
                    "protected_artifact_count": payload.get("summary", {}).get("protected_artifact_count"),
                    "cold_archive_candidate_count": payload.get("summary", {}).get("cold_archive_candidate_count"),
                    "authority_path": str(authority_path or config.resolve(config.output_path)),
                    "read_only": True,
                    "dry_run_only": True,
                    "deletion_performed": False,
                    "archive_performed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0


def _scan_candidate_paths(config: TrackBArtifactRetentionInventoryConfig) -> tuple[set[Path], list[dict[str, Any]]]:
    paths: set[Path] = set()
    warnings: list[dict[str, Any]] = []
    scanned = 0
    lifecycle_root = config.resolve(config.lifecycle_root)
    if lifecycle_root.exists():
        for path in lifecycle_root.rglob("*"):
            if path.is_file():
                paths.add(path)
    for root in config.warm_diagnostic_roots:
        resolved = config.resolve(root)
        if not resolved.exists():
            continue
        for path in resolved.rglob("*"):
            if not path.is_file():
                continue
            paths.add(path)
            scanned += 1
            if scanned >= config.max_scanned_files:
                warnings.append(
                    {
                        "code": "scan_file_cap_reached",
                        "max_scanned_files": config.max_scanned_files,
                        "last_root": str(resolved),
                    }
                )
                return paths, warnings
    return paths, warnings


def _artifact_record(
    *,
    path: Path,
    repo_root: Path,
    now: datetime,
    retention_tier: str,
    protected: bool,
    protection_reason: str | None,
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
        "protection_reason": protection_reason,
    }


def _lifecycle_protection_reason(*, path: Path, lifecycle_root: Path) -> str | None:
    try:
        path.relative_to(lifecycle_root)
    except ValueError:
        return None
    if path.name != "track_b_strategy_managed_paper_lifecycle_report.json":
        return None
    payload = _read_json(path)
    if not payload:
        return "lifecycle report is unreadable or malformed; protect for operator review"
    if payload.get("review_required") is True:
        return "lifecycle report has review_required=true"
    values = [
        normalize_lifecycle_state(payload.get(key))
        for key in (
            "status",
            "classification",
            "lifecycle_status",
            "paper_lifecycle_status",
            "paper_lifecycle_classification",
            "final_position_status",
            "position_status",
            "close_status",
        )
    ]
    if any(_active_or_review_lifecycle_state(value) for value in values):
        return "active or review-required lifecycle state"
    return None


def _active_or_review_lifecycle_state(state: str) -> bool:
    if not state:
        return False
    if "REVIEW" in state:
        return True
    if requires_operator_action(state):
        return True
    if is_registry_eligible(state):
        return True
    return bool(not is_terminal_state(state) and state not in {"INTENT_CREATED", "SUBMIT_ATTEMPTED"})


def _read_json(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()


def _age_seconds(*, path: Path, now: datetime) -> float:
    return max(0.0, (now - datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)).total_seconds())


def _age_days(*, path: Path, now: datetime) -> float | None:
    if not path.exists():
        return None
    return _age_seconds(path=path, now=now) / 86400


def _relative(*, path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


if __name__ == "__main__":
    raise SystemExit(main())
