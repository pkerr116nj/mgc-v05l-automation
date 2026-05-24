from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_artifact_archive_executor import (
    ARCHIVE_EXECUTOR_APPLY_DISABLED,
    ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_AUTHORITY,
    ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE,
    ARCHIVE_EXECUTOR_BLOCKED_PLAN_NOT_READY,
    ARCHIVE_EXECUTOR_BLOCKED_PROTECTION_CHECK,
    ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN,
    ARCHIVE_EXECUTOR_DRY_RUN_READY,
    TrackBArtifactArchiveExecutorConfig,
    build_track_b_artifact_archive_execution_plan,
    write_track_b_artifact_archive_execution_plan,
)
from mgc_v05l.execution_core.track_b_artifact_archive_planner import (
    ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT,
    ARCHIVE_PLAN_EMPTY,
    ARCHIVE_PLAN_READY,
)


NOW = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)


def test_valid_ready_plan_reaches_apply_disabled_boundary(tmp_path: Path) -> None:
    candidate = tmp_path / "outputs/reports/old_runtime_report.json"
    _write_json(candidate, {"old": True})
    _seed_context(tmp_path)
    _write_plan(
        tmp_path,
        classification=ARCHIVE_PLAN_READY,
        candidates=[
            {
                "relative_path": "outputs/reports/old_runtime_report.json",
                "absolute_path": str(candidate),
                "size_bytes": 12,
                "retention_tier": "COLD_ARCHIVE_CANDIDATE",
                "archive_candidate": True,
                "protected": False,
            }
        ],
    )
    before = candidate.read_text(encoding="utf-8")

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )
    output_path = write_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        payload=payload,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_APPLY_DISABLED
    assert payload["candidate_count"] == 1
    assert payload["blocked_reason"] == "APPLY_DISABLED"
    assert payload["would_move_files"] is False
    assert payload["would_delete_files"] is False
    assert payload["would_compress"] is False
    assert payload["execution_enabled"] is False
    assert payload["apply_enabled"] is False
    assert candidate.read_text(encoding="utf-8") == before
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["classification"] == ARCHIVE_EXECUTOR_APPLY_DISABLED


def test_empty_safe_plan_is_calm(tmp_path: Path) -> None:
    _seed_context(tmp_path)
    _write_plan(tmp_path, classification=ARCHIVE_PLAN_EMPTY, candidates=[])

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_DRY_RUN_READY
    assert payload["candidate_count"] == 0
    assert payload["reason"] == "Archive plan is valid; execution remains disabled."


def test_active_authority_candidate_blocks(tmp_path: Path) -> None:
    _seed_context(tmp_path)
    _write_plan(
        tmp_path,
        classification=ARCHIVE_PLAN_READY,
        candidates=[
            {
                "relative_path": "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
                "retention_tier": "COLD_ARCHIVE_CANDIDATE",
                "archive_candidate": True,
                "protected": False,
            }
        ],
    )

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_AUTHORITY
    assert "authority" in payload["blocked_candidates"][0]["blocked_reason"]


def test_active_lifecycle_candidate_blocks(tmp_path: Path) -> None:
    _seed_context(tmp_path)
    _write_plan(
        tmp_path,
        classification=ARCHIVE_PLAN_READY,
        candidates=[
            {
                "relative_path": (
                    "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/"
                    "lifecycle-1/track_b_strategy_managed_paper_lifecycle_report.json"
                ),
                "retention_tier": "COLD_ARCHIVE_CANDIDATE",
                "archive_candidate": True,
                "protected": False,
            }
        ],
    )

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE


def test_active_lifecycle_protected_count_blocks(tmp_path: Path) -> None:
    _seed_context(tmp_path)
    _write_plan(tmp_path, classification=ARCHIVE_PLAN_READY, candidates=[], active_lifecycle_protected_count=1)

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_BLOCKED_ACTIVE_LIFECYCLE
    assert payload["blocked_reason"] == "ACTIVE_LIFECYCLE_PROTECTED"


def test_missing_and_stale_plan_block(tmp_path: Path) -> None:
    _seed_context(tmp_path)

    missing = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )
    _write_plan(tmp_path, classification=ARCHIVE_PLAN_READY, candidates=[], generated_at=NOW - timedelta(hours=2))
    stale = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path, max_plan_age_seconds=60),
        now=NOW,
    )

    assert missing["classification"] == ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN
    assert missing["blocked_reason"] == "PLAN_MISSING"
    assert stale["classification"] == ARCHIVE_EXECUTOR_BLOCKED_STALE_PLAN
    assert stale["blocked_reason"] == "PLAN_STALE"


def test_plan_not_ready_blocks(tmp_path: Path) -> None:
    _seed_context(tmp_path)
    _write_plan(tmp_path, classification=ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT, candidates=[])

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_BLOCKED_PLAN_NOT_READY


def test_unlabeled_research_candidate_blocks(tmp_path: Path) -> None:
    _seed_context(tmp_path)
    _write_plan(
        tmp_path,
        classification=ARCHIVE_PLAN_READY,
        candidates=[
            {
                "relative_path": "outputs/track_b_research/old_report.json",
                "retention_tier": "COLD_ARCHIVE_CANDIDATE",
                "archive_candidate": True,
                "protected": False,
                "plan_role": "cold_archive_candidate",
            }
        ],
    )

    payload = build_track_b_artifact_archive_execution_plan(
        config=TrackBArtifactArchiveExecutorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_EXECUTOR_BLOCKED_PROTECTION_CHECK
    assert "research/offline" in payload["blocked_candidates"][0]["blocked_reason"]


def test_dashboard_projection_is_not_consumed() -> None:
    module = Path("src/mgc_v05l/execution_core/track_b_artifact_archive_executor.py")

    assert "outputs/operator_dashboard/runtime" not in module.read_text(encoding="utf-8")


def _seed_context(root: Path) -> None:
    _write_json(
        root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_coherence_status": "COHERENT",
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/agent_health/latest_agent_health.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "AGENT_HEALTH_READY",
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_POSITIONS",
            "managed_positions": [],
            "review_required_positions": [],
        },
    )


def _write_plan(
    root: Path,
    *,
    classification: str,
    candidates: list[dict],
    generated_at: datetime = NOW,
    active_lifecycle_protected_count: int = 0,
) -> None:
    _write_json(
        root / "outputs/track_b_execution_core/artifact_retention/latest_artifact_archive_plan.json",
        {
            "schema_version": "track_b_artifact_archive_plan_v2",
            "generated_at": generated_at.isoformat(),
            "archive_plan_id": "archive-plan-1",
            "classification": classification,
            "dry_run_only": True,
            "execution_enabled": False,
            "hot_authority_protected_count": 1,
            "active_lifecycle_protected_count": active_lifecycle_protected_count,
            "cold_archive_candidate_count": len(candidates),
            "archive_candidates_sample": candidates,
            "blocked_candidates_sample": [],
            "proposed_archive_batches": [
                {
                    "batch_id": "batch-1",
                    "candidate_count": len(candidates),
                    "sample_paths": [item["relative_path"] for item in candidates],
                    "execution_enabled": False,
                }
            ]
            if candidates
            else [],
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
