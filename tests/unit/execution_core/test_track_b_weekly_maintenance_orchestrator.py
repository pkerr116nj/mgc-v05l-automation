from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core import track_b_weekly_maintenance_orchestrator as orchestrator
from mgc_v05l.execution_core import weekly_maintenance
from mgc_v05l.execution_core.track_b_weekly_maintenance_orchestrator import (
    COMPLETION_COMPLETE,
    COMPLETION_COMPLETE_WITH_DIAGNOSTICS,
    ACTIVE_PROOF_PATH_BLOCKER,
    GENERATED_CACHE_DIAGNOSTIC,
    LANE_DIAGNOSTIC_WARNING,
    LANE_FAILED,
    LANE_INCOMPLETE,
    LANE_PROOF_BLOCKED,
    WEEKLY_MAINTENANCE_ALERT_REQUIRED,
    WEEKLY_MAINTENANCE_ALREADY_COMPLETE,
    WEEKLY_MAINTENANCE_READY,
    WEEKLY_MAINTENANCE_RETRY_SCHEDULED,
    WEEKLY_MAINTENANCE_WINDOW_EXPIRED,
    TrackBWeeklyMaintenanceOrchestratorConfig,
    build_track_b_weekly_maintenance_orchestrator,
    write_track_b_weekly_maintenance_orchestrator,
)


NOW = datetime(2026, 5, 24, 12, 0, tzinfo=UTC)
SATURDAY_0000_ET = datetime(2026, 5, 23, 4, 0, tzinfo=UTC)
SATURDAY_0100_ET = datetime(2026, 5, 23, 5, 0, tzinfo=UTC)
SUNDAY_0000_ET = datetime(2026, 5, 24, 4, 0, tzinfo=UTC)
SUNDAY_0100_ET = datetime(2026, 5, 24, 5, 0, tzinfo=UTC)
SUNDAY_1600_ET = datetime(2026, 5, 24, 20, 0, tzinfo=UTC)


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

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert payload["completion_status"] in {COMPLETION_COMPLETE, COMPLETION_COMPLETE_WITH_DIAGNOSTICS}
    assert payload["maintenance_window_id"] == "track_b_weekly_maintenance_2026-05-23_saturday_et"
    assert payload["window_start"] == "2026-05-23T04:00:00+00:00"
    assert payload["alert_start"] == "2026-05-24T05:00:00+00:00"
    assert payload["window_end"] == "2026-05-24T20:00:00+00:00"
    assert payload["attempts"] == 1
    assert payload["next_retry_at"] is None
    assert payload["alert_required"] is False
    assert set(payload["lanes_run"]) == set(orchestrator.LANE_IDS)
    assert set(payload["lanes_complete"]) == set(orchestrator.LANE_IDS)
    assert payload["proof_blocking_findings"] == []
    assert payload["dry_run_only"] is True
    assert payload["broker_mutation_allowed"] is False
    assert payload["archive_delete_enabled"] is False
    assert payload["archive_compression_enabled"] is False
    assert json.loads(written["json"].read_text(encoding="utf-8"))["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert written["markdown"].read_text(encoding="utf-8").startswith("# Track B Weekly Maintenance")


def test_stale_mgc_historical_data_is_optional_runner_blocking_not_canonical(tmp_path: Path) -> None:
    _write_stale_data_maintenance_report(tmp_path)

    diagnostic = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(
            repo_root=tmp_path,
            maintained_history_runner_enabled=False,
        ),
        now=NOW,
    )
    optional_runner = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(
            repo_root=tmp_path,
            maintained_history_runner_enabled=True,
        ),
        now=NOW,
    )

    assert diagnostic["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert diagnostic["completion_status"] == COMPLETION_COMPLETE_WITH_DIAGNOSTICS
    assert diagnostic["canonical_proof_blocking_findings"] == []
    assert diagnostic["optional_strategy_blocking_findings"] == []
    assert optional_runner["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert optional_runner["canonical_proof_blocking_findings"] == []
    assert optional_runner["optional_strategy_blocking_findings"][0]["lane_id"] == "historical_data_maintenance"


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

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert payload["completion_status"] == COMPLETION_COMPLETE_WITH_DIAGNOSTICS
    assert payload["proof_blocking_findings"] == []
    assert payload["diagnostic_findings"][0]["lane_id"] == "artifact_hygiene"


def test_archive_planner_scan_block_is_maintenance_incomplete_not_canonical(tmp_path: Path) -> None:
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
                "maintenance_incomplete": True,
            }
        },
    )

    assert diagnostic["overall_classification"] == WEEKLY_MAINTENANCE_ALERT_REQUIRED
    assert diagnostic["canonical_proof_blocking_findings"] == []
    assert diagnostic["maintenance_incomplete_findings"][0]["lane_id"] == "artifact_archive_planner"


def test_active_authority_archive_risk_remains_canonical_proof_blocker(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    active_authority = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
        lane_overrides={
            "artifact_archive_planner": {
                "classification": LANE_PROOF_BLOCKED,
                "status": "blocked",
                "reason": "Archive planner saw current/latest authority in candidates.",
                "canonical_proof_blocking": True,
                "diagnostic_only": False,
            }
        },
    )

    assert active_authority["overall_classification"] == orchestrator.WEEKLY_MAINTENANCE_PROOF_BLOCKED
    assert active_authority["canonical_proof_blocking_findings"][0]["lane_id"] == "artifact_archive_planner"


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

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_ALERT_REQUIRED
    assert payload["base_lane_classification"] == orchestrator.WEEKLY_MAINTENANCE_FAILED
    assert payload["lanes_failed"] == ["research_offline_labeling_check"]


def test_pycache_old_root_hits_are_diagnostic_only(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    cache = tmp_path / "src" / "validation_layer" / "__pycache__" / "models.cpython-311.pyc"
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(
        "/Users/patrick/Documents/MGC-v05l-automation/src/validation_layer/models.py\n",
        encoding="utf-8",
    )

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert payload["canonical_proof_blocking_findings"] == []
    details = [
        lane for lane in payload["lanes"] if lane["lane_id"] == "old_root_contamination_check"
    ][0]["summary"]["old_root_hit_details"]
    assert details[0]["severity"] == GENERATED_CACHE_DIAGNOSTIC


def test_active_launch_preflight_old_root_hit_is_canonical_proof_blocker(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    preflight = tmp_path / "scripts" / "track_b_paper_preflight.sh"
    preflight.parent.mkdir(parents=True, exist_ok=True)
    preflight.write_text(
        "/Users/patrick/Documents/MGC-v05l-automation/scripts/run_probationary_paper_soak.sh\n",
        encoding="utf-8",
    )

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["overall_classification"] == orchestrator.WEEKLY_MAINTENANCE_PROOF_BLOCKED
    assert payload["old_root_hits"] == ["scripts/track_b_paper_preflight.sh:1"]
    assert payload["old_root_active_path_blockers"][0]["severity"] == ACTIVE_PROOF_PATH_BLOCKER
    assert payload["canonical_proof_blocking_findings"][0]["lane_id"] == "old_root_contamination_check"


def test_research_offline_unlabeled_is_diagnostic(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    _write_json(tmp_path / "outputs/track_b_research/latest_old_probe.json", {"not_runtime_authority": True})

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert payload["completion_status"] == COMPLETION_COMPLETE_WITH_DIAGNOSTICS
    assert any(item["lane_id"] == "research_offline_labeling_check" for item in payload["diagnostic_findings"])


def test_saturday_0000_first_attempt_is_allowed(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=SATURDAY_0000_ET,
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_READY
    assert payload["last_attempt_at"] == "2026-05-23T04:00:00+00:00"
    assert payload["attempts"] == 1
    assert payload["alert_required"] is False


def test_saturday_hourly_retry_when_incomplete(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=SATURDAY_0100_ET,
        lane_overrides=_incomplete_lane_override(),
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_RETRY_SCHEDULED
    assert payload["completion_status"] == "INCOMPLETE"
    assert payload["next_retry_at"] == "2026-05-23T06:00:00+00:00"
    assert payload["alert_required"] is False


def test_sunday_0000_incomplete_has_no_alert_yet(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=SUNDAY_0000_ET,
        lane_overrides=_incomplete_lane_override(),
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_RETRY_SCHEDULED
    assert payload["next_retry_at"] == "2026-05-24T05:00:00+00:00"
    assert payload["alert_required"] is False


def test_sunday_0100_incomplete_requires_alert(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=SUNDAY_0100_ET,
        lane_overrides=_incomplete_lane_override(),
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_ALERT_REQUIRED
    assert payload["alert_required"] is True
    assert payload["next_retry_at"] == "2026-05-24T06:00:00+00:00"


def test_sunday_1600_incomplete_expires_and_blocks_proof(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=SUNDAY_1600_ET,
        lane_overrides=_incomplete_lane_override(),
    )

    assert payload["overall_classification"] == WEEKLY_MAINTENANCE_WINDOW_EXPIRED
    assert payload["completion_status"] == "WINDOW_EXPIRED"
    assert payload["next_retry_at"] is None
    assert payload["alert_required"] is True
    assert payload["canonical_proof_blocking_findings"] == []
    assert payload["maintenance_incomplete_findings"][0]["lane_id"] == "artifact_hygiene"


def test_final_readiness_context_separates_canonical_from_diagnostics(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path),
        now=NOW,
        lane_overrides={
            "artifact_hygiene": {
                "classification": LANE_DIAGNOSTIC_WARNING,
                "status": "diagnostic",
                "reason": "Diagnostic warning.",
                "diagnostic_only": True,
                "proof_blocking": False,
            },
            "artifact_archive_planner": {
                "classification": LANE_DIAGNOSTIC_WARNING,
                "status": "attention",
                "reason": "Archive planner scan limit reached.",
                "diagnostic_only": True,
                "maintenance_incomplete": True,
            },
        },
    )

    final_lane = [lane for lane in payload["lanes"] if lane["lane_id"] == "final_control_plane_readiness_context"][0]
    assert payload["canonical_proof_blocking_findings"] == []
    assert payload["maintenance_incomplete_findings"][0]["lane_id"] == "artifact_archive_planner"
    assert final_lane["summary"]["canonical_proof_blocking_count"] == 0
    assert final_lane["summary"]["maintenance_incomplete_count"] == 1


def test_completed_week_is_already_complete_noop(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    config = TrackBWeeklyMaintenanceOrchestratorConfig(repo_root=tmp_path)
    first = build_track_b_weekly_maintenance_orchestrator(config=config, now=SATURDAY_0000_ET)
    write_track_b_weekly_maintenance_orchestrator(config=config, payload=first)

    second = build_track_b_weekly_maintenance_orchestrator(config=config, now=SATURDAY_0100_ET)

    assert second["overall_classification"] == WEEKLY_MAINTENANCE_ALREADY_COMPLETE
    assert second["lanes_run"] == []
    assert second["attempts"] == 1
    assert second["next_retry_at"] is None
    assert second["alert_required"] is False


def test_generated_artifact_retention_check_is_weekly_lane(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    _write_retention_policy(tmp_path)

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(
            repo_root=tmp_path,
            retention_policy_path=Path("config/generated_artifact_retention.json"),
        ),
        now=NOW,
    )

    assert "generated_artifact_retention_check" in payload["lanes_run"]
    assert payload["generated_artifact_retention_status"]["classification"] == "GENERATED_ARTIFACT_RETENTION_CHECK_OK"
    assert any(item["utility_id"] == "generated_artifact_retention" for item in payload["utility_statuses"])


def test_missed_week_backfill_range_detection(tmp_path: Path) -> None:
    _write_retention_policy(tmp_path)
    _write_data_maintenance_report(
        tmp_path,
        {
            "data_maintenance_verdict": "TRACK_B_DATA_MAINTENANCE_STALE_OR_INSUFFICIENT",
            "complete_through_cutoff": False,
            "latest_bar_timestamp": "2026-05-16T21:00:00+00:00",
            "history_ready": False,
        },
    )
    _write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_data_maintenance"
        / "track_b_data_maintenance_success"
        / "track_b_data_maintenance_report.json",
        {
            "generated_at": "2026-05-17T05:00:00+00:00",
            "data_maintenance_verdict": "TRACK_B_DATA_MAINTENANCE_UPDATED_HISTORY_READY",
            "complete_through_cutoff": True,
            "latest_bar_timestamp": "2026-05-16T21:00:00+00:00",
            "history_ready": True,
        },
    )

    payload = build_track_b_weekly_maintenance_orchestrator(
        config=TrackBWeeklyMaintenanceOrchestratorConfig(
            repo_root=tmp_path,
            retention_policy_path=Path("config/generated_artifact_retention.json"),
            maintained_history_runner_enabled=True,
        ),
        now=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
    )

    tracking = payload["backfill_tracking"]
    assert tracking["last_two_weekly_runs_missed"] is True
    assert [item["week_id"] for item in tracking["missed_weekly_windows"]] == [
        "2026-05-23_saturday_et",
        "2026-05-30_saturday_et",
    ]
    assert tracking["recommended_backfill_date_ranges"] == [
        {
            "from": "2026-05-16T21:01:00+00:00",
            "to": "2026-05-29T21:00:00+00:00",
            "reason": "latest_historical_bar_before_expected_prior_friday_close",
        }
    ]


def test_weekly_maintenance_dry_run_writes_report_and_includes_retention(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    _write_retention_policy(tmp_path)

    rc = weekly_maintenance.main(
        [
            "--repo-root",
            str(tmp_path),
            "--retention-policy",
            "config/generated_artifact_retention.json",
            "dry-run",
        ]
    )

    report = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "weekly_maintenance"
        / "latest_weekly_maintenance_orchestrator.json"
    )
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert rc == 0
    assert report.exists()
    assert payload["generated_artifact_retention_status"]["retention_maintenance_dry_run_included"] is True
    assert payload["broker_mutation_allowed"] is False


def test_weekly_maintenance_apply_requires_approved(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    _write_retention_policy(tmp_path)

    rc = weekly_maintenance.main(
        [
            "--repo-root",
            str(tmp_path),
            "--retention-policy",
            "config/generated_artifact_retention.json",
            "apply",
        ]
    )

    assert rc == 2
    assert not (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "weekly_maintenance"
        / "latest_weekly_maintenance_orchestrator.json"
    ).exists()


def test_weekly_maintenance_backfill_is_plan_only(tmp_path: Path) -> None:
    _write_good_data_maintenance_report(tmp_path)
    _write_retention_policy(tmp_path)

    rc = weekly_maintenance.main(
        [
            "--repo-root",
            str(tmp_path),
            "--retention-policy",
            "config/generated_artifact_retention.json",
            "backfill",
            "--from",
            "2026-05-16",
            "--to",
            "2026-05-29",
            "--dry-run",
        ]
    )

    report = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "weekly_maintenance"
        / "latest_weekly_maintenance_orchestrator.json"
    )
    payload = json.loads(report.read_text(encoding="utf-8"))
    assert rc == 0
    assert payload["requested_backfill"]["execute_backfill"] is False
    assert payload["requested_backfill"]["from"] == "2026-05-16T00:00:00+00:00"
    assert payload["backfill_execution_invoked"] is False


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
    if not (root / "config" / "generated_artifact_retention.json").exists():
        _write_retention_policy(root)
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


def _write_retention_policy(root: Path) -> None:
    _write_json(
        root / "config" / "generated_artifact_retention.json",
        {
            "schema_version": "generated_artifact_retention_policy_v1",
            "compression_enabled": True,
            "max_archives_per_stream": 2,
            "max_file_size_mb": 1,
            "keep_latest_tail_mb": 1,
            "generated_roots": ["outputs"],
            "forbidden_roots": ["src", "tests", "docs", "scripts", "config", "var"],
            "protected_globs": [
                "outputs/track_b_execution_core/trade_registry/**",
                "outputs/track_b_execution_core/paper_trade_ledger/**",
                "outputs/track_b_execution_core/lifecycle_stress/latest*",
            ],
            "streams": [
                {
                    "stream_id": "test_stream",
                    "patterns": ["outputs/test/*.jsonl"],
                    "max_file_size_mb": 1,
                    "keep_latest_tail_mb": 1,
                    "max_archives_per_stream": 2,
                    "compression_enabled": True,
                    "recreate_live_file": True,
                }
            ],
        },
    )


def _incomplete_lane_override() -> dict[str, dict[str, object]]:
    return {
        "artifact_hygiene": {
            "classification": LANE_INCOMPLETE,
            "status": "incomplete",
            "reason": "External platform maintenance still in progress.",
            "proof_blocking": False,
            "diagnostic_only": False,
        }
    }
