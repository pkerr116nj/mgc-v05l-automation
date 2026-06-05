from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_artifact_retention_inventory import (
    ARTIFACT_RETENTION_INVENTORY_READY,
    COLD_ARCHIVE_CANDIDATE,
    HOT_ACTIVE_LIFECYCLE_PROTECTED,
    HOT_AUTHORITY_PROTECTED,
    TrackBArtifactRetentionInventoryConfig,
    build_track_b_artifact_retention_inventory,
    write_track_b_artifact_retention_inventory,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_identifies_hot_authority_artifacts(tmp_path: Path) -> None:
    hot_path = tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
    _write_json(hot_path, {"classification": "CLEAN_FLAT_READY"})

    payload = build_track_b_artifact_retention_inventory(
        config=TrackBArtifactRetentionInventoryConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == ARTIFACT_RETENTION_INVENTORY_READY
    hot = _by_relative(payload["hot_authority_artifacts"])
    item = hot["outputs/track_b_execution_core/position_truth/latest_position_truth.json"]
    assert item["retention_tier"] == HOT_AUTHORITY_PROTECTED
    assert item["protected"] is True
    assert item["exists"] is True
    assert payload["read_only"] is True
    assert payload["dry_run_only"] is True
    assert payload["scan_truncated"] is False
    assert payload["summary"]["scan_truncated"] is False
    assert payload["scan_summaries"]
    assert payload["deletion_performed"] is False
    assert payload["archive_performed"] is False
    assert payload["dashboard_projection_consumed"] is False


def test_protects_startup_restart_authority_latest_patterns(tmp_path: Path) -> None:
    protected_paths = [
        "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json",
        "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        "outputs/track_b_execution_core/diagnostics/latest_track_b_registry_truth_diagnostics.json",
        "outputs/track_b_execution_core/paper_stack/latest_paper_stack_status.json",
        "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
    ]
    for relative_path in protected_paths:
        path = tmp_path / relative_path
        _write_json(path, {"classification": "CURRENT_AUTHORITY"})
        _set_mtime(path, NOW - timedelta(days=90))

    payload = build_track_b_artifact_retention_inventory(
        config=TrackBArtifactRetentionInventoryConfig(repo_root=tmp_path, cold_candidate_days=30),
        now=NOW,
    )

    hot = _by_relative(payload["hot_authority_artifacts"])
    candidates = _by_relative(payload["archive_candidates"])
    for relative_path in protected_paths:
        assert hot[relative_path]["retention_tier"] == HOT_AUTHORITY_PROTECTED
        assert hot[relative_path]["protected"] is True
        assert relative_path not in candidates


def test_identifies_old_diagnostics_as_archive_candidates(tmp_path: Path) -> None:
    old_report = tmp_path / "outputs" / "reports" / "old_runtime_report.json"
    _write_json(old_report, {"report": "old"})
    _set_mtime(old_report, NOW - timedelta(days=45))

    payload = build_track_b_artifact_retention_inventory(
        config=TrackBArtifactRetentionInventoryConfig(repo_root=tmp_path, cold_candidate_days=30),
        now=NOW,
    )

    candidates = _by_relative(payload["archive_candidates"])
    item = candidates["outputs/reports/old_runtime_report.json"]
    assert item["retention_tier"] == COLD_ARCHIVE_CANDIDATE
    assert item["archive_candidate"] is True
    assert item["protected"] is False
    reports_summary = _scan_summary(payload, "outputs/reports")
    assert reports_summary["scan_truncated"] is False
    assert reports_summary["scanned_file_count"] == 1


def test_inventory_reports_truncated_scan_with_per_root_summary(tmp_path: Path) -> None:
    for index in range(3):
        old_report = tmp_path / "outputs" / "reports" / f"old_runtime_report_{index}.json"
        _write_json(old_report, {"report": index})
        _set_mtime(old_report, NOW - timedelta(days=45))

    payload = build_track_b_artifact_retention_inventory(
        config=TrackBArtifactRetentionInventoryConfig(
            repo_root=tmp_path,
            cold_candidate_days=30,
            max_scanned_files=2,
        ),
        now=NOW,
    )

    assert payload["classification"] == "ARTIFACT_RETENTION_INVENTORY_PARTIAL"
    assert payload["scan_truncated"] is True
    assert payload["summary"]["scan_truncated"] is True
    assert payload["warnings"][0]["code"] == "scan_file_cap_reached"
    reports_summary = _scan_summary(payload, "outputs/reports")
    assert reports_summary["scan_truncated"] is True
    assert reports_summary["scanned_file_count"] == 2


def test_protects_active_lifecycle_and_reconciliation_latest(tmp_path: Path) -> None:
    lifecycle = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / "lifecycle_1"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    reconciliation = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    _write_json(lifecycle, {"final_position_status": "OPEN_MANAGED"})
    _write_json(reconciliation, {"classification": "TRACK_B_PAPER_BROKER_RECONCILED"})
    _set_mtime(lifecycle, NOW - timedelta(days=60))
    _set_mtime(reconciliation, NOW - timedelta(days=60))

    payload = build_track_b_artifact_retention_inventory(
        config=TrackBArtifactRetentionInventoryConfig(repo_root=tmp_path, cold_candidate_days=30),
        now=NOW,
    )

    active = _by_relative(payload["active_lifecycle_artifacts"])
    lifecycle_item = active[
        "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/lifecycle_1/"
        "track_b_strategy_managed_paper_lifecycle_report.json"
    ]
    assert lifecycle_item["retention_tier"] == HOT_ACTIVE_LIFECYCLE_PROTECTED
    assert lifecycle_item["protected"] is True
    hot = _by_relative(payload["hot_authority_artifacts"])
    assert hot[
        "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json"
    ]["protected"] is True
    candidates = _by_relative(payload["archive_candidates"])
    assert lifecycle_item["relative_path"] not in candidates
    assert "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json" not in candidates


def test_inventory_write_is_dry_run_only_and_deletes_nothing(tmp_path: Path) -> None:
    old_report = tmp_path / "outputs" / "reports" / "old_runtime_report.json"
    _write_json(old_report, {"report": "old"})
    _set_mtime(old_report, NOW - timedelta(days=45))
    config = TrackBArtifactRetentionInventoryConfig(repo_root=tmp_path)
    payload = build_track_b_artifact_retention_inventory(config=config, now=NOW)

    output_path = write_track_b_artifact_retention_inventory(config=config, payload=payload)

    assert old_report.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["dry_run_only"] is True
    assert written["deletion_performed"] is False
    assert written["archive_performed"] is False
    assert output_path == tmp_path / "outputs" / "track_b_execution_core" / "artifact_retention" / "latest_artifact_retention_inventory.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _set_mtime(path: Path, when: datetime) -> None:
    timestamp = when.timestamp()
    os.utime(path, (timestamp, timestamp))


def _by_relative(items: list[dict]) -> dict[str, dict]:
    return {item["relative_path"]: item for item in items}


def _scan_summary(payload: dict, suffix: str) -> dict:
    for item in payload["scan_summaries"]:
        if str(item["root"]).endswith(suffix):
            return item
    raise AssertionError(f"missing scan summary for {suffix}")
