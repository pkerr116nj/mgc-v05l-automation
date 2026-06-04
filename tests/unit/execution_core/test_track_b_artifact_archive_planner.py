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
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path, cold_candidate_days=30, sample_limit=50),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_EMPTY
    assert payload["dry_run_only"] is True
    assert payload["execution_enabled"] is False
    assert payload["hot_authority_protected_count"] >= 1
    assert payload["cold_archive_candidate_count"] == 0
    assert payload["blocked_candidate_count"] == 0
    protected = _by_relative(payload["hot_authority_protected_sample"])
    assert "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json" in protected
    assert protected["outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"][
        "retention_tier"
    ] == "HOT_AUTHORITY_PROTECTED"
    assert payload["dry_run_safety_report"]["protected_count"] == payload["protected_count"]
    assert payload["dry_run_safety_report"]["cleanup_candidate_count"] == 0
    assert payload["dry_run_safety_report"]["blocked_candidate_count"] == 0


def test_hot_authority_guardrail_protects_restart_submit_close_artifacts(tmp_path: Path) -> None:
    protected_paths = [
        "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        "outputs/track_b_execution_core/runtime_truth/latest_runtime_environment_truth.json",
        "outputs/track_b_execution_core/shared_truth/latest_track_b_shared_truth_refresh.json",
        "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        "outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json",
        "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        "outputs/track_b_execution_core/diagnostics/latest_track_b_registry_truth_diagnostics.json",
        "outputs/track_b_execution_core/paper_stack/latest_paper_stack_status.json",
        "outputs/track_b_execution_core/paper_stack/latest_paper_stack_startup.json",
        "outputs/reports/phase1_runtime_data_readiness/latest_phase1_runtime_data_readiness.json",
        "outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json",
        "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
    ]
    for relative_path in protected_paths:
        path = tmp_path / relative_path
        _write_json(path, {"classification": "CURRENT_AUTHORITY"})
        _set_mtime(path, NOW - timedelta(days=90))

    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path, cold_candidate_days=30, sample_limit=50),
        now=NOW,
    )

    protected = _by_relative(payload["hot_authority_protected_sample"])
    candidates = _by_relative(payload["archive_candidates_sample"])
    blocked = _by_relative(payload["blocked_candidates_sample"])
    for relative_path in protected_paths:
        assert protected[relative_path]["retention_tier"] == "HOT_AUTHORITY_PROTECTED"
        assert protected[relative_path]["protected"] is True
        assert relative_path not in candidates
        assert relative_path not in blocked
    assert payload["cleanup_candidate_count"] == 0
    assert payload["blocked_candidate_count"] == 0
    report = payload["dry_run_safety_report"]
    assert report["protected_count"] == payload["protected_count"]
    assert report["cleanup_candidate_count"] == 0
    assert report["future_destructive_cleanup_allowed"] is True


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
    assert payload["cleanup_candidate_count"] == 1
    candidates = _by_relative(payload["archive_candidates_sample"])
    assert candidates["outputs/track_b_research/old_research_report.json"]["plan_role"] == "research_offline"
    assert payload["proposed_archive_batches"][0]["execution_enabled"] is False
    report = payload["dry_run_safety_report"]
    assert report["blocked_candidate_count"] == 0
    assert report["cleanup_candidate_count"] == 1
    classifications = _by_relative(report["path_classifications"])
    assert classifications["outputs/track_b_research/old_research_report.json"]["classification_reason"]


def test_stale_inventory_candidate_for_hot_authority_is_blocked_and_reported(tmp_path: Path) -> None:
    inventory_path = (
        tmp_path / "outputs/track_b_execution_core/artifact_retention/latest_artifact_retention_inventory.json"
    )
    hot_relative = "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json"
    hot_path = tmp_path / hot_relative
    _write_json(hot_path, {"classification": "OPEN_MANAGED_CLOSE_WORKING"})
    _set_mtime(hot_path, NOW - timedelta(days=90))
    _write_json(
        inventory_path,
        {
            "classification": "ARTIFACT_RETENTION_INVENTORY_READY",
            "hot_authority_artifacts": [],
            "active_lifecycle_artifacts": [],
            "warm_diagnostics": [],
            "archive_candidates": [
                {
                    "relative_path": hot_relative,
                    "absolute_path": str(hot_path),
                    "exists": True,
                    "size_bytes": 10,
                    "retention_tier": "COLD_ARCHIVE_CANDIDATE",
                    "archive_candidate": True,
                    "protected": False,
                }
            ],
            "warnings": [],
        },
    )

    payload = build_track_b_artifact_archive_plan(
        config=TrackBArtifactArchivePlannerConfig(repo_root=tmp_path, sample_limit=50),
        now=NOW,
    )

    assert payload["classification"] == ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY
    assert payload["cleanup_candidate_count"] == 0
    assert payload["blocked_candidate_count"] == 1
    assert payload["blocked_candidate_paths"] == [hot_relative]
    blocked = _by_relative(payload["blocked_candidates_sample"])
    assert blocked[hot_relative]["blocked_reason"] == (
        "current/latest hot-authority artifact is protected from cleanup/archive/quarantine"
    )
    report = payload["dry_run_safety_report"]
    assert report["blocked_candidate_count"] == 1
    assert report["blocked_candidate_paths"] == [hot_relative]
    assert report["future_destructive_cleanup_allowed"] is False
    classifications = _by_relative(report["path_classifications"])
    assert classifications[hot_relative]["blocked"] is True
    assert classifications[hot_relative]["classification_reason"] == blocked[hot_relative]["blocked_reason"]


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
