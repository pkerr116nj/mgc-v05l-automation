from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core import track_b_weekly_maintenance_orchestrator as orchestrator
from mgc_v05l.execution_core.track_b_weekly_maintenance_orchestrator import (
    LANE_DIAGNOSTIC_WARNING,
    LANE_FAILED,
    LANE_PROOF_BLOCKED,
    LANE_READY,
    WEEKLY_MAINTENANCE_FAILED,
    WEEKLY_MAINTENANCE_PROOF_BLOCKED,
    WEEKLY_MAINTENANCE_READY,
    WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS,
    TrackBWeeklyMaintenanceOrchestratorConfig,
    build_track_b_weekly_maintenance_orchestrator,
    write_track_b_weekly_maintenance_orchestrator,
)


NOW = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)


def test_all_lanes_run_successfully_is_ready(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
    )
    written = write_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        payload=payload,
    )

    assert payload["overall_classification"] in {
        WEEKLY_MAINTENANCE_READY,
        WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS,
    }
    assert set(payload["lanes_run"]) == set(orchestrator.LANE_IDS)
    assert payload["proof_blocking_findings"] == []
    assert payload["dry_run_only"] is True
    assert payload["broker_mutation_allowed"] is False
    assert payload["archive_delete_enabled"] is False
    assert payload["archive_compression_enabled"] is False
    assert json.loads(written["json"].read_text(encoding="utf-8"))["overall_classification"] in {
        WEEKLY_MAINTENANCE_READY,
        WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS,
    }
    assert written["markdown"].read_text(encoding="utf-8").startswith("# Track B Weekly Maintenance")


def test_historical_critical_lane_stale_blocks_only_when_configured_proof_critical(tmp_path: Path) -> None:
    _write_stale_data_maintenance_report(tmp_path)

    diagnostic = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(
            repo_root=tmp_path,
            historical_data_proof_critical=False,
        ),
        now=NOW,
    )
    proof_critical = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(
            repo_root=tmp_path,
            historical_data_proof_critical=True,
        ),
        now=NOW,
    )

    assert diagnostic["overall_classification"] == WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS
    assert diagnostic["proof_blocking_findings"] == []
    assert proof_critical["overall_classification"] == WEEKLY_MAINTENANCE_PROOF_BLOCKED
    assert proof_critical["proof_blocking_findings"][0]["lane_id"] == "historical_data_maintenance"


def test_artifact_hygiene_warning_is_diagnostic_not_proof_blocking(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
        lane_overrides={
            "artifact_hygiene": {
                "classification": LANE_DIAGNOSTIC_WARNING,
                "status": "diagnostic",
                "reason": "Weekly dry-run found disposable build metadata.",
                "proof_blocking": False,
                "diagnostic_only": True,
            }
        },
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS
    assert payload["proof_blocking_findings"] == []
    assert payload["diagnostic_findings"][0]["lane_id"] == "artifact_hygiene"


def test_archive_planner_scan_block_is_diagnostic_but_active_authority_risk_blocks(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    diagnostic = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
        lane_overrides={
            "artifact_archive_planner": {
                "classification": LANE_DIAGNOSTIC_WARNING,
                "status": "attention",
                "reason": "Archive planner scan limit reached.",
                "proof_blocking": False,
                "diagnostic_only": True,
            }
        },
    )
    active_authority = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
        lane_overrides={
            "artifact_archive_planner": {
                "classification": LANE_PROOF_BLOCKED,
                "status": "blocked",
                "reason": "Archive planner saw current/latest authority in candidates.",
                "proof_blocking": True,
                "diagnostic_only": False,
            }
        },
    )

    assert diagnostic["overall_classification"] == WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS
    assert active_authority["overall_classification"] == WEEKLY_MAINTENANCE_PROOF_BLOCKED
    assert active_authority["proof_blocking_findings"][0]["lane_id"] == "artifact_archive_planner"


def test_lane_failure_is_failed(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
        lane_overrides={
            "research_offline_labeling_check": {
                "classification": LANE_FAILED,
                "status": "failed",
                "reason": "Injected failure.",
                "proof_blocking": False,
                "diagnostic_only": True,
            }
        },
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_FAILED
    assert payload["lanes_failed"] == ["research_offline_labeling_check"]


def test_old_root_contamination_blocks_proof(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    bad_script = tmp_path / "scripts" / "bad.sh"
    bad_script.parent.mkdir(parents=True, exist_ok=True)
    bad_script.write_text(
        "/Users/patrick/Documents/MGC-v05l-automation/scripts/run_probationary_paper_soak.sh\n",
        encoding="utf-8",
    )

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_PROOF_BLOCKED
    assert payload["old_root_hits"] == ["scripts/bad.sh:1"]
    assert any(item["lane_id"] == "old_root_contamination_check" for item in payload["proof_blocking_findings"])


def test_research_offline_unlabeled_is_diagnostic(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    _write_json(tmp_path / "outputs/track_b_research/latest_old_probe.json", {"not_runtime_authority": True})

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS
    assert any(item["lane_id"] == "research_offline_labeling_check" for item in payload["diagnostic_findings"])


def test_no_broker_order_lifecycle_mutation_or_archive_execution_terms() -> None:
    source = inspect.getsource(orchestrator)

    assert "paper_proof" not in source.lower().replace("paper_proof_invoked", "")
    assert "live_money_eligible\": True" not in source
    assert "sub" + "process" not in source
    assert "shutil" not in source
    assert ".un" + "link(" not in source
    assert ".re" + "name(" not in source
    assert "tar" + "file" not in source
    assert "zip" + "file" not in source
    assert "g" + "zip" not in source


def test_dashboard_projection_is_not_consumed() -> None:
    source = inspect.getsource(orchestrator)

    assert "outputs/operator_dashboard/runtime" not in source
    assert "dashboard_projection_consumed" in source


def _write_good_data_maintenance_report(root: Path) -> None:
    _write_data_maintenance_report(
        root,
        {
            "data_maintenance_verdict": "TRACK_B_DATA_MAINTENANCE_UPDATED_HISTORY_READY",
            "complete_through_cutoff": True,
            "latest_bar_timestamp": "2026-05-22T21:00:00+00:00",
            "history_ready": True,
        },
    )


def _write_stale_data_maintenance_report(root: Path) -> None:
    _write_data_maintenance_report(
        root,
        {
            "data_maintenance_verdict": "TRACK_B_DATA_MAINTENANCE_STALE_OR_INSUFFICIENT",
            "complete_through_cutoff": False,
            "latest_bar_timestamp": (NOW - timedelta(days=10)).isoformat(),
            "history_ready": False,
        },
    )


def _write_data_maintenance_report(root: Path, payload: dict) -> None:
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "track_b_data_maintenance"
        / "latest_track_b_data_maintenance_report.json",
        payload,
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
