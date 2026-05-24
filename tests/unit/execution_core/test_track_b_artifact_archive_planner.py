from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_artifact_archive_planner import (
    ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY,
    ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT,
    ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE,
    ARCHIVE_PLAN_EMPTY,
    ARCHIVE_PLAN_READY,
    TrackBArtifactArchivePlannerConfig,
    build_track_b_artifact_archive_plan,
    write_track_b_artifact_archive_plan,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_latest_authority_files_are_protected(tmp_path: Path) -> None:
    latest = tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
    _write_json(latest, {"classification": "CONTROL_PLANE_SNAPSHOT_READY"})
    _set_mtime(latest, NOW - timedelta(days=90))

    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path, cold_candidate_days=30),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY
    assert payload["dry_run_only"] is True
    assert payload["execution_enabled"] is False
    assert payload["hot_authority_protected_count"] >= 1
    assert payload["cold_archive_candidate_count"] == 0
    blocked = _by_relative(payload["blocked_candidates_sample"])
    assert "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json" in blocked
    assert blocked["outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"][
        "blocked_reason"
    ].startswith("current/latest authority")


def test_active_lifecycle_files_are_protected(tmp_path: Path) -> None:
    lifecycle = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/lifecycle-1/"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    _write_json(lifecycle, {"final_position_status": "OPEN_MANAGED"})
    _set_mtime(lifecycle, NOW - timedelta(days=90))

    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path, cold_candidate_days=30),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE
    assert payload["active_lifecycle_protected_count"] == 1
    protected = _by_relative(payload["active_lifecycle_protected_sample"])
    assert (
        "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/lifecycle-1/"
        "track_b_strategy_managed_paper_lifecycle_report.json"
    ) in protected
    assert payload["cold_archive_candidate_count"] == 0


def test_research_offline_metadata_candidates_classified_cold(tmp_path: Path) -> None:
    research = tmp_path / "outputs/track_b_research/old_research_report.json"
    _write_json(
        research,
        {
            "research_only": True,
            "offline_diagnostic": True,
            "not_runtime_authority": True,
            "not_routing_authority": True,
            "source_category": "research/offline",
        },
    )
    _set_mtime(research, NOW - timedelta(days=60))

    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path, cold_candidate_days=30),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_READY
    assert payload["cold_archive_candidate_count"] == 1
    candidates = _by_relative(payload["archive_candidates_sample"])
    assert candidates["outputs/track_b_research/old_research_report.json"]["plan_role"] == "research_offline"
    assert payload["proposed_archive_batches"][0]["execution_enabled"] is False


def test_scan_limit_warning_blocks_ready_classification(tmp_path: Path) -> None:
    inventory_path = (
        tmp_path / "outputs/track_b_execution_core/artifact_retention/latest_artifact_retention_inventory.json"
    )
    _write_json(
        inventory_path,
        {
            "classification": "ARTIFACT_RETENTION_INVENTORY_PARTIAL",
            "hot_authority_artifacts": [],
            "active_lifecycle_artifacts": [],
            "warm_diagnostics": [],
            "archive_candidates": [
                {
                    "relative_path": "outputs/reports/old_report.json",
                    "absolute_path": str(tmp_path / "outputs/reports/old_report.json"),
                    "exists": True,
                    "size_bytes": 10,
                    "retention_tier": "COLD_ARCHIVE_CANDIDATE",
                    "archive_candidate": True,
                    "protected": False,
                }
            ],
            "warnings": [{"code": "scan_file_cap_reached"}],
        },
    )

    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT
    assert payload["cold_archive_candidate_count"] == 1
    assert payload["warnings"][0]["code"] == "scan_file_cap_reached"


def test_no_history_no_candidates_is_empty(tmp_path: Path) -> None:
    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_EMPTY
    assert payload["cold_archive_candidate_count"] == 0
    assert payload["blocked_candidate_count"] == 0
    assert payload["archive_candidates_sample"] == []
    assert payload["proposed_archive_batches"] == []


def test_write_plan_is_dry_run_only_and_does_not_mutate_candidates(tmp_path: Path) -> None:
    old_report = tmp_path / "outputs/reports/old_runtime_report.json"
    _write_json(old_report, {"report": "old"})
    _set_mtime(old_report, NOW - timedelta(days=45))
    before = old_report.read_text(encoding="utf-8")
    config = TrackBArtifactArchivePlannerConfig(repo_root=tmp_path)
    payload = build_track_b_artifact_archive_plan(config=config, now=NOW)

    output_path = write_track_b_artifact_archive_plan(config=config, payload=payload)

    assert output_path.exists()
    assert old_report.exists()
    assert old_report.read_text(encoding="utf-8") == before
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["dry_run_only"] is True
    assert written["execution_enabled"] is False
    assert written["delete_enabled"] is False
    assert written["file_move_enabled"] is False
    assert written["cold_storage_write_enabled"] is False


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _set_mtime(path: Path, when: datetime) -> None:
    timestamp = when.timestamp()
    os.utime(path, (timestamp, timestamp))


def _by_relative(items: list[dict]) -> dict[str, dict]:
    return {item["relative_path"]: item for item in items}
