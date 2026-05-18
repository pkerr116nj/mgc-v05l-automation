from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_readiness_state import (
    _broker_truth_input,
    build_root_process_guard,
    classify_canonical_readiness,
    write_canonical_readiness_artifact,
)


def _clean_inputs() -> dict:
    return {
        "generated_at": "2026-05-18T12:00:00+00:00",
        "paper_only": True,
        "live_money_eligible": False,
        "root_guard_summary": {
            "expected_root": "/Users/patrick/Dev/MGC-v05l-automation",
            "active_root": "/Users/patrick/Dev/MGC-v05l-automation",
            "root_match": True,
            "wrong_root_processes": [],
            "unknown_root_processes": [],
        },
        "backend": {"healthy": True, "running": True},
        "runtime": {
            "running": True,
            "healthy": True,
            "loaded_lane_count": 3,
            "eligible_lane_count": 3,
            "entries_enabled": True,
            "operator_halt": False,
        },
        "broker_truth": {
            "available": True,
            "fresh": True,
            "positions_complete": True,
            "open_orders_complete": True,
            "open_order_count": 0,
            "live_money_eligible": False,
            "latest_attempt_status": {"classification": "BROKER_TRUTH_REFRESH_READY", "last_failure": False},
        },
        "phase1_reconciliation": {
            "available": True,
            "fresh": True,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "live_money_eligible": False,
        },
        "market_data": {"fresh": True, "market_data_ok": True},
        "lane_quarantine": {
            "classification": "PAPER_LANE_QUARANTINE_CLEAR",
            "quarantine_count": 0,
            "quarantined_lane_ids": [],
            "live_money_eligible": False,
        },
        "submit_bridge": {
            "submit_route_ready": True,
            "submit_authority_explicit": True,
            "eligible_lane_count": 3,
            "live_money_eligible": False,
        },
    }


def test_clean_submit_capable_state_returns_ready_submit_capable() -> None:
    result = classify_canonical_readiness(_clean_inputs())

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["ready_submit_capable"] is True
    assert result["readiness_blockers"] == []


def test_wrong_root_blocks() -> None:
    inputs = _clean_inputs()
    inputs["root_guard_summary"] = {
        "expected_root": "/Users/patrick/Dev/MGC-v05l-automation",
        "active_root": "/Users/patrick/Dev/MGC-v05l-automation",
        "root_match": False,
        "wrong_root_processes": [{"name": "paper_runtime", "pid": 123, "detected_root": "/tmp/wrong"}],
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_WRONG_ROOT"
    assert result["operator_action_required"] is True
    assert result["readiness_blockers"][0]["code"] == "wrong_root_process"


def test_healthy_runtime_without_eligible_lanes_is_observation_only() -> None:
    inputs = _clean_inputs()
    inputs["runtime"]["eligible_lane_count"] = 0
    inputs["submit_bridge"]["eligible_lane_count"] = 0
    inputs["submit_bridge"]["submit_authority_explicit"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_OBSERVATION_ONLY"
    assert result["ready_submit_capable"] is False


def test_stale_broker_truth_blocks_submit() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth"]["fresh"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["code"] == "broker_truth_not_fresh_or_complete"


def test_broker_truth_uses_fresh_complete_last_good_when_latest_attempt_failed() -> None:
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    result = _broker_truth_input(
        {
            "classification": "BROKER_TRUTH_REFRESH_FAILED",
            "generated_at": "2026-05-18T11:59:50+00:00",
            "last_success": False,
            "positions_complete": False,
            "open_orders_complete": False,
            "latest_attempt_status": {
                "classification": "BROKER_TRUTH_REFRESH_FAILED",
                "last_failure": True,
            },
            "last_successful_broker_truth": {
                "classification": "BROKER_TRUTH_REFRESH_READY",
                "generated_at": "2026-05-18T11:59:00+00:00",
                "positions_complete": True,
                "open_orders_complete": True,
                "open_order_count": 0,
                "position_count": 0,
                "live_money_eligible": False,
            },
        },
        now=now,
    )

    assert result["fresh"] is True
    assert result["using_last_successful_broker_truth"] is True
    assert result["latest_attempt_status"]["last_failure"] is True


def test_missing_reconciliation_blocks() -> None:
    inputs = _clean_inputs()
    inputs["phase1_reconciliation"]["available"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_RECONCILIATION"
    assert result["readiness_blockers"][0]["code"] == "phase1_reconciliation_not_clean"


def test_live_money_eligible_true_blocks_paper_readiness() -> None:
    inputs = _clean_inputs()
    inputs["live_money_eligible"] = True

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_CONFIG"
    assert result["readiness_blockers"][0]["code"] == "live_money_eligible_true"


def test_lane_quarantine_with_other_eligible_lanes_keeps_submit_ready_with_warning() -> None:
    inputs = _clean_inputs()
    inputs["runtime"]["loaded_lane_count"] = 3
    inputs["runtime"]["eligible_lane_count"] = 2
    inputs["lane_quarantine"] = {
        "classification": "PAPER_LANE_QUARANTINE_ACTIVE",
        "quarantine_count": 1,
        "quarantined_lane_ids": ["lane-a"],
        "healthy_lane_ids": ["lane-b", "lane-c"],
        "live_money_eligible": False,
    }

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert result["readiness_warnings"][0]["code"] == "lane_quarantine_active"


def test_broker_wide_ambiguity_remains_fatal() -> None:
    inputs = _clean_inputs()
    inputs["broker_truth"]["positions_complete"] = False

    result = classify_canonical_readiness(inputs)

    assert result["canonical_readiness"] == "NOT_READY_DEPENDENCY"
    assert result["readiness_blockers"][0]["source"] == "broker_truth"


def test_root_guard_classifies_wrong_root_process(monkeypatch) -> None:
    repo_root = Path("/Users/patrick/Dev/MGC-v05l-automation")
    expected_root = repo_root
    artifacts = {
        "operator_status": {
            "source_runtime_pid": 777,
            "source_runtime_repo_root": str(repo_root),
        }
    }
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 777)

    result = build_root_process_guard(
        repo_root=repo_root,
        expected_root=expected_root,
        artifacts=artifacts,
        process_cwd_resolver=lambda pid: Path("/Users/patrick/Documents/MGC-v05l-automation"),
        now=datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc),
    )

    assert result["root_match"] is False
    assert result["wrong_root_processes"][0]["name"] == "paper_runtime"


def test_write_canonical_readiness_artifact_without_dashboard(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path
    runtime_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime"
    report_dir = repo_root / "outputs" / "reports"
    lanes_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mnq"
    stale_lane_dir = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes" / "mgc_stale"
    runtime_dir.mkdir(parents=True)
    (report_dir / "ibkr_read_only_verification").mkdir(parents=True)
    (report_dir / "track_b_paper_broker_reconciliation").mkdir(parents=True)
    lanes_dir.mkdir(parents=True)
    stale_lane_dir.mkdir(parents=True)
    now = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
    (repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json").write_text(
        """
        {
          "source_runtime_pid": 100,
          "source_runtime_repo_root": "%s",
          "active_lane_ids": ["mnq"],
          "usable_lane_count": 1,
          "entries_enabled": true,
          "operator_halt": false,
          "last_processed_bar_end_ts": "2026-05-18T11:59:00+00:00",
          "health": {"market_data_ok": true}
        }
        """
        % repo_root,
        encoding="utf-8",
    )
    (runtime_dir / "paper_config_in_force.json").write_text('{"lanes": [{"lane_id": "mnq"}]}', encoding="utf-8")
    (runtime_dir / "paper_lane_quarantine_status.json").write_text(
        '{"classification": "PAPER_LANE_QUARANTINE_CLEAR", "quarantine_count": 0, "live_money_eligible": false}',
        encoding="utf-8",
    )
    (runtime_dir / "market_data_transport_probe.json").write_text('{"status": "ok", "runtime_ready": true}', encoding="utf-8")
    (report_dir / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json").write_text(
        """
        {
          "classification": "BROKER_TRUTH_REFRESH_READY",
          "generated_at": "2026-05-18T11:59:10+00:00",
          "last_success": true,
          "positions_complete": true,
          "open_orders_complete": true,
          "live_money_eligible": false
        }
        """,
        encoding="utf-8",
    )
    (
        report_dir
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    ).write_text(
        """
        {
          "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
          "generated_at": "2026-05-18T11:59:20+00:00",
          "broker_reconciled": true,
          "review_required_count": 0,
          "lifecycle_open_position_count": 0,
          "live_money_eligible": false
        }
        """,
        encoding="utf-8",
    )
    (lanes_dir / "live_timing_summary_latest.json").write_text(
        """
        {
          "lane_id": "mnq",
          "broker_truth": {
            "account_health": {
              "status": "HEALTHY",
              "route_destination": "ibkr_paper_bridge_submit_capable"
            }
          }
        }
        """,
        encoding="utf-8",
    )
    (stale_lane_dir / "live_timing_summary_latest.json").write_text(
        """
        {
          "lane_id": "mgc_stale",
          "broker_truth": {
            "account_health": {
              "status": "HEALTHY",
              "route_destination": "ibkr_paper_bridge_submit_capable"
            }
          }
        }
        """,
        encoding="utf-8",
    )
    monkeypatch.setattr("mgc_v05l.execution_core.track_b_readiness_state._pid_running", lambda pid: pid == 100)

    output = repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    result = write_canonical_readiness_artifact(
        repo_root=repo_root,
        expected_root=repo_root,
        output_path=output,
        now=now,
    )

    assert output.exists()
    assert result["canonical_readiness"] == "READY_SUBMIT_CAPABLE"
    assert [row["lane_id"] for row in result["submit_bridge"]["route_rows"]] == ["mnq"]
