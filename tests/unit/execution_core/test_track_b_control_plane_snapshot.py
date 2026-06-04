from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core import track_b_control_plane_snapshot as cp_module
from mgc_v05l.execution_core.track_b_control_plane_snapshot import (
    CONTROL_PLANE_SNAPSHOT_READY,
    CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED,
    TrackBControlPlaneSnapshotConfig,
    build_dashboard_control_plane_snapshot_projection,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_snapshot_ties_supervisor_to_shared_truth_generation(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["shared_truth_refresh_generation_id"] == "track-b-shared-truth-20260523T120000000000Z"
    assert payload["shared_truth_coherence_status"] == "COHERENT"
    assert payload["shared_truth_generation_matches_supervisor"] is True
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert payload["supervisor_mode"] == "READY_FOR_OPERATOR_START"
    assert payload["safe_to_start_runtime"] is True
    assert payload["top_line_classification"] == "READY_FOR_OPERATOR_START"
    assert "Ready for supervised Track B PAPER runtime start" in payload["top_line_status"]
    assert payload["runtime_resume_action_policy"] == "NEW_RUNTIME_GENERATION_ALLOWED"
    assert payload["runtime_resume_proposed_next_runtime_generation_id"] == "track-b-paper-runtime-generation-20260523T120000Z"
    assert payload["runtime_resume_attempts_remaining"] == 2
    assert payload["recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert payload["paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["recovery_budget_key"].startswith("track_b_paper_runtime|RUNTIME_RETRY|")
    assert payload["attempts_remaining"] == 2
    assert payload["quarantine_required"] is False
    assert payload["recovery_attempt_history_no_history"] is True
    assert payload["latest_recovery_attempt_id"] == ""
    assert payload["artifact_archive_plan_classification"] == "ARCHIVE_PLAN_EMPTY"
    assert payload["artifact_archive_cold_archive_candidate_count"] == 0
    assert payload["artifact_archive_dry_run_only"] is True
    assert payload["artifact_archive_execution_enabled"] is False
    assert payload["artifact_archive_diagnostic_only"] is True
    assert payload["artifact_archive_not_routing_authority"] is True
    assert payload["safe_state_classification"] == "SAFE_STATE_NORMAL"
    assert payload["safe_state_runtime_start_allowed"] is True
    assert payload["safe_state_submit_allowed"] is True
    assert payload["safe_state_broker_mutation_allowed"] is True
    assert payload["broker_order_position_summary"]["open_order_truth"] == "NO_OPEN_ORDERS"
    assert payload["source_artifact_paths"]["shared_truth_refresh"].endswith(
        "outputs/track_b_execution_core/shared_truth/latest_track_b_shared_truth_refresh.json"
    )


def test_snapshot_refreshes_stale_proof_readiness_before_supervisor(monkeypatch, tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path, proof_classification="PHASE1_DATA_UNHEALTHY")

    proof_calls = []

    def fake_build_proof(**kwargs):
        proof_calls.append(kwargs["config"])
        return {
            "generated_at": NOW.isoformat(),
            "classification": "READY_FOR_PROOF",
            "phase1_session_reason": "GLOBEX_SESSION_OPEN",
            "phase1_market_session": {"market_closed": False, "reason": "GLOBEX_SESSION_OPEN"},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }

    def fake_write_proof(*, config, payload):
        path = config.resolve(config.output_path)
        _write(path, payload)
        return path

    monkeypatch.setattr(cp_module, "build_track_b_paper_proof_readiness", fake_build_proof)
    monkeypatch.setattr(cp_module, "write_track_b_paper_proof_readiness", fake_write_proof)

    payload = build_track_b_control_plane_snapshot(
        config=TrackBControlPlaneSnapshotConfig(repo_root=tmp_path),
        now=NOW,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )

    assert proof_calls
    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert payload["proof_window_status"] == "ready"


def test_snapshot_preserves_real_stale_phase1_proof_blocker(monkeypatch, tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path, proof_classification="READY_FOR_PROOF")

    def fake_build_proof(**_kwargs):
        return {
            "generated_at": NOW.isoformat(),
            "classification": "PHASE1_DATA_UNHEALTHY",
            "primary_blocker": {"code": "phase1_mgc_5m_not_ready"},
            "phase1_session_reason": "RUNTIME_CANDLES_STALE",
            "phase1_market_session": {"market_closed": False, "reason": "GLOBEX_SESSION_OPEN"},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }

    def fake_write_proof(*, config, payload):
        path = config.resolve(config.output_path)
        _write(path, payload)
        return path

    monkeypatch.setattr(cp_module, "build_track_b_paper_proof_readiness", fake_build_proof)
    monkeypatch.setattr(cp_module, "write_track_b_paper_proof_readiness", fake_write_proof)

    payload = build_track_b_control_plane_snapshot(
        config=TrackBControlPlaneSnapshotConfig(repo_root=tmp_path),
        now=NOW,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )

    assert payload["classification"] != CONTROL_PLANE_SNAPSHOT_READY
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_SHARED_TRUTH_STALE"
    assert payload["proof_window_status"] == "data_stale"


def test_market_closed_snapshot_waits_without_alarm(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(
        tmp_path,
        proof_classification=MARKET_CLOSED_NO_FRESH_BARS,
        supervisor_classification="SUPERVISOR_WAIT_MARKET_CLOSED",
        supervisor_mode="MARKET_CLOSED_WAIT",
        resume_classification="RESUME_BLOCKED_MARKET_CLOSED",
        self_recover_recommendation="WAIT_MARKET_CLOSED",
    )

    payload = _snapshot(tmp_path)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert payload["supervisor_mode"] == "READY_FOR_OPERATOR_START"
    assert payload["proof_window_status"] == "market_closed"
    assert payload["top_line_classification"] == "MARKET_CLOSED_WAIT"
    assert "Market closed/no fresh bars expected" in payload["top_line_status"]
    assert payload["runtime_resume_action_policy"] == "HOLD_MARKET_CLOSED"
    assert payload["recommended_recovery_action"] == "WAIT_MARKET_CLOSED"
    assert payload["recommended_next_command"] == "operator may start Track B PAPER runtime using the repaired direct supervisor launcher"
    assert payload["safe_to_start_runtime"] is True


def test_stale_mixed_generation_blocks_snapshot(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def corrupt_open_order_truth(_shared_truth: dict) -> None:
        _write(
            tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "UNKNOWN_OPEN_ORDER",
                "live_money_eligible": False,
            },
        )

    payload = _snapshot(tmp_path, post_hook=corrupt_open_order_truth)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_SHARED_TRUTH_STALE"
    assert payload["shared_truth_coherence_status"] == "STALE_OR_MIXED"
    assert any(source["service"] == "Open Order Truth" for source in payload["stale_or_mixed_sources"])


def test_snapshot_converges_stale_order_adjustment_after_clean_shared_truth(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    stale_plan_path = tmp_path / "outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json"
    _write(
        stale_plan_path,
        {
            "generated_at": "2026-05-23T11:55:00+00:00",
            "classification": "REVIEW_REQUIRED_SUSPICIOUS_STATE",
            "plans": [{"classification": "REVIEW_REQUIRED_SUSPICIOUS_STATE"}],
            "summary": {"classification": "REVIEW_REQUIRED_SUSPICIOUS_STATE", "plan_count": 1},
            "live_money_eligible": False,
        },
    )

    payload = _snapshot(tmp_path)
    refreshed_plan = json.loads(stale_plan_path.read_text(encoding="utf-8"))

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["shared_truth_coherence_status"] == "COHERENT"
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert refreshed_plan["classification"] == "NO_ACTION_NEEDED"
    assert refreshed_plan["summary"]["plan_count"] == 0


def test_snapshot_includes_recovery_and_planner_fields(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["paper_recovery_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert payload["autonomous_recovery_plan_classification"]
    assert payload["autonomous_recovery_next_action"]
    assert payload["autonomous_recovery_execution_enabled"] is False


def test_snapshot_surfaces_recovery_attempt_history(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    _seed_recovery_attempt(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["latest_recovery_attempt_id"] == "attempt-1"
    assert payload["latest_recovery_attempt_action_type"] == "RUNTIME_RETRY"
    assert payload["latest_recovery_attempt_classification"] == "EXECUTOR_DRY_RUN_READY"
    assert payload["recovery_attempt_recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert payload["recovery_attempt_recovery_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert payload["recovery_attempt_attempts_remaining"] == 1
    assert payload["recovery_attempt_quarantine_required"] is False
    assert payload["recovery_attempt_last_success_at"] == "2026-05-23T12:01:00+00:00"
    assert payload["recovery_attempt_history_no_history"] is False
    assert payload["source_artifact_paths"]["recovery_attempt_history"].endswith(
        "outputs/track_b_execution_core/paper_autonomous_recovery/latest_recovery_attempt_history.json"
    )


def test_snapshot_surfaces_blocked_artifact_archive_plan(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    lifecycle = (
        tmp_path
        / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/lifecycle-1/"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    _write(lifecycle, {"final_position_status": "OPEN_MANAGED"})

    payload = _snapshot(tmp_path)

    assert payload["artifact_archive_plan_classification"] == "ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE"
    assert payload["artifact_archive_active_lifecycle_protected_count"] == 1
    assert payload["artifact_archive_dry_run_only"] is True
    assert payload["artifact_archive_execution_enabled"] is False
    assert payload["source_artifact_paths"]["artifact_archive_plan"].endswith(
        "outputs/track_b_execution_core/artifact_retention/latest_artifact_archive_plan.json"
    )


def test_snapshot_surfaces_continuation_aware_exit_preview_as_diagnostic_only(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    _seed_continuation_aware_exit_preview(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["continuation_aware_exit_strategy_id"] == "asian_drift_v1"
    assert payload["continuation_aware_exit_symbol"] == "MGC"
    assert payload["continuation_aware_exit_policy_id"] == "TIME_PLUS_CONTINUATION_EXIT_V1"
    assert payload["continuation_aware_exit_profile_id"] == "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1"
    assert payload["continuation_aware_exit_state"] == "HOLD_CONTINUATION_CONFIRMED"
    assert payload["continuation_aware_exit_quality_state"] == "STRONG_ALIGNED_CONTINUATION"
    assert payload["continuation_aware_exit_should_request_close"] is False
    assert payload["continuation_aware_exit_dry_run_only"] is True
    assert payload["continuation_aware_exit_not_order_authority"] is True
    assert payload["continuation_aware_exit_not_lifecycle_authority"] is True
    assert payload["continuation_aware_exit_missing_inputs"] == []
    assert payload["continuation_aware_exit_source_report_path"].endswith("asian_drift_rule_report.json")
    assert payload["continuation_aware_exit_no_preview"] is False
    assert payload["continuation_aware_exit_diagnostic_only"] is True
    assert payload["continuation_aware_exit_not_routing_authority"] is True
    assert payload["continuation_aware_exit_history_classification"] == "CONTINUATION_EXIT_HISTORY_READY"
    assert payload["continuation_aware_exit_history_total_events"] == 1
    assert payload["continuation_aware_exit_history_strategy_count"] == 1
    assert payload["continuation_aware_exit_history_latest_strategy_id"] == "asian_drift_v1"
    assert payload["continuation_aware_exit_history_latest_exit_profile_id"] == (
        "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1"
    )
    assert payload["continuation_aware_exit_history_latest_exit_state"] == "HOLD_CONTINUATION_CONFIRMED"
    assert payload["continuation_aware_exit_history_not_order_authority"] is True
    assert payload["continuation_aware_exit_history_not_lifecycle_authority"] is True
    assert payload["continuation_aware_exit_history_not_routing_authority"] is True
    assert payload["continuation_aware_exit_history_top_strategies"][0]["strategy_id"] == "asian_drift_v1"
    assert payload["source_artifact_paths"]["continuation_aware_exit_preview"].endswith(
        "outputs/track_b_execution_core/continuation_aware_exit/latest_continuation_aware_exit_preview.json"
    )
    assert payload["source_artifact_paths"]["continuation_aware_exit_history"].endswith(
        "outputs/track_b_execution_core/continuation_aware_exit/latest_continuation_aware_exit_history.json"
    )


def test_snapshot_no_continuation_aware_exit_preview_is_calm(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["continuation_aware_exit_state"] == "NO_CONTINUATION_AWARE_EXIT_PREVIEW"
    assert payload["continuation_aware_exit_no_preview"] is True
    assert payload["continuation_aware_exit_diagnostic_only"] is True
    assert payload["continuation_aware_exit_should_request_close"] is False
    assert payload["continuation_aware_exit_history_classification"] == "CONTINUATION_EXIT_HISTORY_EMPTY"
    assert payload["continuation_aware_exit_history_total_events"] == 0
    assert payload["continuation_aware_exit_history_top_strategies"] == []


def test_malformed_continuation_preview_cannot_influence_authority_fields(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    _seed_continuation_aware_exit_preview(
        tmp_path,
        should_request_close=True,
        exit_state="EXIT_REVERSAL_DETECTED",
    )

    payload = _snapshot(tmp_path)

    assert payload["continuation_aware_exit_should_request_close"] is True
    assert payload["continuation_aware_exit_not_order_authority"] is True
    assert payload["continuation_aware_exit_not_lifecycle_authority"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["safe_state_classification"] == "SAFE_STATE_NORMAL"
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert not any(blocker["code"].startswith("continuation_aware_exit") for blocker in payload["blockers"])
    assert payload["continuation_aware_exit_history_total_events"] == 1
    assert payload["continuation_aware_exit_history_not_order_authority"] is True
    assert payload["continuation_aware_exit_history_not_lifecycle_authority"] is True


def test_snapshot_surfaces_planner_operator_explanation_for_missing_open_order_truth(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def write_blocked_plan(_shared_truth: dict) -> None:
        _write(
            tmp_path / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "PLAN_BLOCKED_STALE_EVIDENCE",
                "primary_blocking_agent_id": "open_order_truth",
                "primary_blocking_reason": "authority artifact missing",
                "operator_explanation": "Refresh evidence before recovery planning: Open Order Truth reports authority artifact missing.",
                "recommended_observation_step": "Run the Control Plane Snapshot refresh path so shared truth, Agent Health, planner, and supervisor are rebuilt from one generation.",
                "prioritized_blockers": [
                    {
                        "agent_id": "open_order_truth",
                        "display_name": "Open Order Truth",
                        "status": "MISSING_ARTIFACT",
                        "reason": "authority artifact missing",
                        "blocking_for_proof": True,
                        "blocking_for_runtime_submit": True,
                        "blocking_for_recovery": True,
                        "diagnostic_only": False,
                        "source": "agent_health",
                        "priority": 2,
                    }
                ],
            },
        )

    payload = _snapshot(tmp_path, post_hook=write_blocked_plan)

    assert payload["primary_blocking_agent_id"] == "open_order_truth"
    assert payload["primary_blocking_reason"] == "authority artifact missing"
    assert "Open Order Truth" in payload["operator_explanation"]
    assert payload["top_line_classification"] == "MISSING_AUTHORITY_ARTIFACT"
    assert "Open Order Truth" in payload["top_line_status"]
    assert payload["prioritized_blockers"][0]["status"] == "MISSING_ARTIFACT"


def test_snapshot_includes_agent_health_v2_fields(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(tmp_path)

    assert payload["agent_health_schema_version"] == "track_b_agent_health_v2"
    assert payload["agent_health_classification"] == "AGENT_HEALTH_READY"
    assert payload["agent_health_summary"]["agent_count"] == 14
    assert payload["blocking_for_proof_count"] == 0
    assert payload["blocking_for_runtime_submit_count"] == 0
    assert payload["blocking_for_recovery_count"] == 0
    assert payload["duplicate_process_count"] == 0
    assert payload["missing_artifact_count"] == 0
    assert payload["source_commit_mismatch_count"] == 0
    assert payload["root_mismatch_count"] == 0
    assert payload["agent_health_blocks_proof"] is False
    assert payload["agent_health_blocks_runtime_submit"] is False
    assert payload["agent_health_blocks_recovery"] is False
    assert payload["agent_health_has_duplicate_writer"] is False


def test_duplicate_writer_blocks_snapshot_start_posture(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    payload = _snapshot(
        tmp_path,
        process_rows=[
            {"pid": 10, "command": "bash scripts/run_probationary_paper_soak.sh"},
            {"pid": 11, "command": "python -m mgc_v05l.app.main probationary-paper-soak"},
        ],
    )

    assert payload["agent_health_has_duplicate_writer"] is True
    assert payload["duplicate_process_count"] == 1
    assert payload["agent_health_blocks_proof"] is True
    assert payload["classification"] == "CONTROL_PLANE_SNAPSHOT_BLOCKED"
    assert payload["top_line_classification"] == "HARD_UNSAFE_DUPLICATE_WRITER"
    assert "duplicate" in payload["top_line_status"].lower()
    assert any(blocker["code"] == "agent_health_duplicate_writer" for blocker in payload["blockers"])
    assert payload["safe_to_start_runtime"] is False


def test_owned_exit_due_exposure_tolerates_runtime_down_agent_health_for_start(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def fake_agent_health(**_kwargs):
        return {
            "schema_version": "track_b_agent_health_v2",
            "classification": "AGENT_HEALTH_BLOCKING",
            "summary": {
                "agent_count": 1,
                "blocking_for_proof_count": 1,
                "blocking_for_runtime_submit_count": 1,
                "blocking_for_recovery_count": 0,
                "duplicate_process_count": 0,
            },
            "agents": [
                {
                    "agent_id": "track_b_paper_runtime",
                    "display_name": "Track B PAPER runtime",
                    "status": "STOPPED_UNEXPECTED",
                    "reason": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE",
                    "blocking_for_proof": True,
                    "blocking_for_runtime_submit": True,
                    "blocking_for_recovery": False,
                    "diagnostic_only": False,
                }
            ],
        }

    def fake_supervisor(**_kwargs):
        return {
            "generated_at": NOW.isoformat(),
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_mode": "READY_FOR_OPERATOR_START",
            "proof_window_status": "ready",
            "safe_to_start_runtime": True,
            "shared_truth_refresh_generation_id": "track-b-shared-truth-20260523T120000000000Z",
            "shared_truth_coherence_status": "COHERENT",
            "live_money_eligible": False,
            "evidence_summary": {
                "pre_restart_exposure_resolution_classification": "MANAGED_EXPOSURE_RESOLVED",
                "restart_with_owned_exposure_allowed": True,
            },
        }

    monkeypatch.setattr(cp_module, "build_track_b_agent_health", fake_agent_health)
    monkeypatch.setattr(cp_module, "build_track_b_runtime_supervisor_authority", fake_supervisor)

    payload = _snapshot(tmp_path)

    assert payload["classification"] == CONTROL_PLANE_SNAPSHOT_READY
    assert payload["safe_to_start_runtime"] is True
    assert payload["agent_health_blocks_runtime_submit"] is True
    assert payload["runtime_authority_exposure_classification"] == "RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE"
    assert payload["fresh_broker_exposure_visible_when_runtime_stale"] is True
    assert payload["runtime_authority_stale_submit_blocked"] is True
    assert payload["runtime_authority_stale_maintenance_needed"] is True
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_RUNTIME_START_ALLOWED"
    assert not any(blocker["code"] == "agent_health_blocks_runtime_submit" for blocker in payload["blockers"])
    assert payload["top_line_classification"] == "READY_FOR_OPERATOR_START"


def test_control_plane_reports_stale_runtime_close_path_without_entry_submit(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def fake_agent_health(**_kwargs):
        return {
            "schema_version": "track_b_agent_health_v2",
            "classification": "AGENT_HEALTH_BLOCKING",
            "summary": {
                "agent_count": 1,
                "blocking_for_proof_count": 1,
                "blocking_for_runtime_submit_count": 1,
                "blocking_for_recovery_count": 0,
                "duplicate_process_count": 0,
            },
            "agents": [
                {
                    "agent_id": "track_b_paper_runtime",
                    "display_name": "Track B PAPER runtime",
                    "status": "STOPPED_UNEXPECTED",
                    "reason": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE",
                    "blocking_for_proof": True,
                    "blocking_for_runtime_submit": True,
                    "blocking_for_recovery": False,
                    "diagnostic_only": False,
                }
            ],
        }

    def fake_supervisor(**_kwargs):
        return {
            "generated_at": NOW.isoformat(),
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_mode": "READY_FOR_OPERATOR_START",
            "proof_window_status": "ready",
            "safe_to_start_runtime": True,
            "shared_truth_refresh_generation_id": "track-b-shared-truth-20260523T120000000000Z",
            "shared_truth_coherence_status": "COHERENT",
            "live_money_eligible": False,
            "evidence_summary": {
                "runtime_environment_truth_classification": "RUNTIME_DOWN_WITH_BROKER_EXPOSURE",
                "pre_restart_exposure_resolution_classification": "MANAGED_EXPOSURE_RESOLVED",
                "restart_with_owned_exposure_allowed": True,
            },
        }

    monkeypatch.setattr(cp_module, "build_track_b_agent_health", fake_agent_health)
    monkeypatch.setattr(cp_module, "build_track_b_runtime_supervisor_authority", fake_supervisor)

    payload = _snapshot(tmp_path, post_hook=lambda _shared_truth: _seed_exit_due_mnq_authority(tmp_path))

    assert payload["submit_authority"] is False
    assert payload["broker_mutation"] is False
    assert payload["runtime_authority_exposure_classification"] == "RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE"
    assert payload["fresh_broker_exposure_visible_when_runtime_stale"] is True
    assert payload["safe_state_submit_allowed"] is False
    assert payload["safe_state_entry_mutation_allowed"] is False
    assert payload["risk_reducing_close_classification"] == (
        "RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE"
    )
    assert payload["risk_reducing_close_allowed_runtime_stale"] is True
    assert payload["risk_reducing_close_candidate"]["action"] == "BUY"
    assert payload["risk_reducing_close_candidate"]["local_symbol"] == "MNQM6"


def test_safe_state_broker_mutation_limit_blocks_snapshot_status(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    _write(
        tmp_path / "outputs/track_b_execution_core/strategy_bridge/latest_strategy_bridge_submit_report.json",
        {
            "generated_at": NOW.isoformat(),
            "submits_per_symbol_window": {"MGC": 4},
            "broker_mutation_attempts_per_window": 4,
            "failed_broker_mutations_per_window": 0,
            "live_money_eligible": False,
        },
    )

    payload = _snapshot(tmp_path)

    assert payload["safe_state_classification"] == "SAFE_STATE_BROKER_MUTATION_LIMIT_HIT"
    assert payload["safe_state_recovery_only"] is True
    assert payload["classification"] == "CONTROL_PLANE_SNAPSHOT_BLOCKED"
    assert payload["safe_to_start_runtime"] is False
    assert any(blocker["code"] == "runtime_safe_state_envelope" for blocker in payload["blockers"])
    assert payload["source_artifact_paths"]["runtime_safe_state_envelope"].endswith(
        "outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json"
    )


def test_stale_noncritical_agent_health_artifact_is_warning(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def stale_runtime_pid(_shared_truth: dict) -> None:
        _write(
            tmp_path / "outputs/track_b_execution_core/process_hygiene/latest_track_b_process_surface_hygiene.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "PROCESS_SURFACE_CLEAR",
                "proof_blocking_processes": [],
                "diagnostic_processes": [],
                "expected_support_processes": [],
                "stale_pid_files": [
                    {
                        "label": "track_b_paper_runtime",
                        "path": str(tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"),
                        "pid": 123,
                        "reason": "pid_not_running",
                    }
                ],
                "summary": {
                    "active_runtime_writer_count": 0,
                    "phase1_databento_process_count": 1,
                    "broker_truth_refresher_count": 1,
                    "operator_dashboard_readiness_process_count": 1,
                },
            },
        )

    payload = _snapshot(tmp_path, post_hook=stale_runtime_pid)

    assert payload["stale_pid_count"] == 1
    assert payload["agent_health_blocks_proof"] is False
    assert payload["agent_health_blocks_recovery"] is False
    assert any(warning["code"] == "agent_health_stale_pid_detected" for warning in payload["warnings"])


def test_missing_required_agent_health_artifact_blocks(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)

    def remove_open_order_truth(_shared_truth: dict) -> None:
        (tmp_path / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json").unlink()

    payload = _snapshot(tmp_path, post_hook=remove_open_order_truth)

    assert payload["missing_artifact_count"] == 1
    assert payload["agent_health_blocks_proof"] is True
    assert payload["agent_health_blocks_runtime_submit"] is True
    assert payload["runtime_supervisor_classification"] == "SUPERVISOR_SHARED_TRUTH_STALE"
    open_order_blocker = next(
        blocker for blocker in payload["agent_health_top_blockers"] if blocker["agent_id"] == "open_order_truth"
    )
    assert open_order_blocker["display_name"] == "Open Order Truth"
    assert open_order_blocker["status"] == "MISSING_ARTIFACT"
    assert open_order_blocker["blocking_for_proof"] is True
    assert open_order_blocker["blocking_for_runtime_submit"] is True
    assert open_order_blocker["blocking_for_recovery"] is True
    assert open_order_blocker["diagnostic_only"] is False


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    config = TrackBControlPlaneSnapshotConfig(repo_root=tmp_path)
    payload = build_track_b_control_plane_snapshot(
        config=config,
        now=NOW,
        pid_running=lambda _pid: False,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
    )

    authority_path = write_track_b_control_plane_snapshot(config=config, payload=payload)
    projection = json.loads(config.resolve(config.dashboard_projection_path).read_text(encoding="utf-8"))  # type: ignore[arg-type]
    direct_projection = build_dashboard_control_plane_snapshot_projection(
        authority_payload=payload,
        authority_path=authority_path,
    )

    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True


def test_dashboard_projection_not_consumed(tmp_path: Path) -> None:
    _seed_clean_stack(tmp_path)
    _seed_control_plane(tmp_path)
    projection_path = tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json"
    _write(
        projection_path,
        {
            "projection_only": True,
            "not_routing_authority": True,
            "classification": "POISONED_DASHBOARD_PROJECTION",
        },
    )

    payload = _snapshot(tmp_path)

    assert payload["classification"] != "POISONED_DASHBOARD_PROJECTION"
    assert payload["source_artifact_paths"]["control_plane_snapshot"].endswith(
        "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json"
    )


def _snapshot(
    root: Path,
    *,
    post_hook=None,
    process_rows=None,
) -> dict:
    return build_track_b_control_plane_snapshot(
        config=TrackBControlPlaneSnapshotConfig(repo_root=root, refresh_proof_readiness_before_snapshot=False),
        now=NOW,
        pid_running=(lambda _pid: False) if process_rows is not None else None,
        process_rows=process_rows,
        process_root_resolver=lambda _pid: None,
        source_commit_resolver=lambda _root: "test-head",
        post_shared_truth_refresh_hook=post_hook,
    )


def _seed_clean_stack(root: Path) -> None:
    _write_reconciliation(root)
    _write_broker_status(root)
    _write_live_position_status(root)
    _write_trade_summary(root)


def _seed_control_plane(
    root: Path,
    *,
    proof_classification: str = "READY_FOR_PROOF",
    supervisor_classification: str = "SUPERVISOR_RUNTIME_START_ALLOWED",
    supervisor_mode: str = "READY_FOR_OPERATOR_START",
    resume_classification: str = "RESUME_ALLOWED_CLEAN",
    self_recover_recommendation: str = "RESTART_RUNTIME_ALLOWED",
) -> None:
    _write(
        root / "outputs/track_b_execution_core/proof_readiness/latest_track_b_paper_proof_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "phase1_session_reason": proof_classification,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "safe_to_start_runtime": supervisor_classification == "SUPERVISOR_RUNTIME_START_ALLOWED",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/runtime_resume/latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": resume_classification,
            "resume_semantics_version": "v2",
            "resume_action_policy": "HOLD_MARKET_CLOSED"
            if resume_classification == "RESUME_BLOCKED_MARKET_CLOSED"
            else "NEW_RUNTIME_GENERATION_ALLOWED",
            "previous_runtime_generation_id": "runtime-generation-previous",
            "proposed_next_runtime_generation_id": "runtime-generation-next",
            "bounded_retry_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "attempts_remaining": 2,
            "cooldown_until": None,
            "generation_reuse_allowed": False,
            "must_start_new_generation": True,
            "allowed": resume_classification == "RESUME_ALLOWED_CLEAN",
            "safe_to_start_runtime": resume_classification == "RESUME_ALLOWED_CLEAN",
            "previous_broker_safe_at_stop": True,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/self_recover/latest_self_recover_rules.json",
        {
            "self_recover_schema_version": "v2",
            "generated_at": NOW.isoformat(),
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "recovery_plan_id": "self-recover-plan-1",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_generation_id": "track-b-shared-truth-20260523T120000000000Z",
            "paper_action_policy": "OBSERVE"
            if self_recover_recommendation == "WAIT_MARKET_CLOSED"
            else "AUTONOMOUS_RETRY_ELIGIBLE",
            "autonomous_recovery_plan_classification": "WAIT_MARKET_CLOSED"
            if self_recover_recommendation == "WAIT_MARKET_CLOSED"
            else "PLAN_RUNTIME_RETRY",
            "recommended_recovery_action": "WAIT_MARKET_CLOSED"
            if self_recover_recommendation == "WAIT_MARKET_CLOSED"
            else "RUNTIME_RETRY_DRY_RUN",
            "recovery_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "attempts_remaining": 2,
            "cooldown_until": None,
            "quarantine_required": False,
            "agent_health_top_blockers": [],
            "operator_explanation": "self recover structured plan",
            "execution_enabled": False,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/crash_loop_protection/latest_crash_loop_protection.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "NO_CRASH_LOOP",
            "restart_blocked": False,
            "operator_ack_required": False,
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/agent_health/latest_agent_health.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "AGENT_HEALTH_READY",
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/agent_registry/latest_agent_registry.json",
        {"generated_at": NOW.isoformat(), "classification": "AGENT_REGISTRY_READY"},
    )
    _write(
        root / "outputs/operator_dashboard/runtime/latest_canonical_readiness.json",
        {"generated_at": NOW.isoformat(), "canonical_readiness": "READY_SUBMIT_CAPABLE"},
    )
    _write(
        root / "var/track_b_operator_readiness_refresh_heartbeat.json",
        {"generated_at": NOW.isoformat(), "repo_root": str(root)},
    )
    broker_pid_path = root / "var/track_b_broker_truth_refresh_service.pid"
    broker_pid_path.parent.mkdir(parents=True, exist_ok=True)
    broker_pid_path.write_text("123\n", encoding="utf-8")
    _write(
        root / "outputs/reports/phase1_runtime_data_readiness/latest_phase1_runtime_data_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "market_session": {"classification": proof_classification},
            "rows": [{"symbol": "MGC", "runtime_candles_ready": True, "derived_features_ready": True}],
        },
    )
    _write(
        root / "outputs/probationary_pattern_engine/paper_session/runtime/latest_runtime_stop_provenance.json",
        {
            "generated_at": NOW.isoformat(),
            "stop_source": "launcher",
            "stop_reason": "expected_clean_down",
            "live_money_eligible": False,
        },
    )


def _seed_recovery_attempt(root: Path) -> None:
    attempt = {
        "generated_at": NOW.isoformat(),
        "recovery_attempt_id": "attempt-1",
        "action_type": "RUNTIME_RETRY",
        "classification": "EXECUTOR_DRY_RUN_READY",
        "control_plane_snapshot_id": "snapshot-1",
        "shared_truth_generation_id": "track-b-shared-truth-20260523T120000000000Z",
        "recommended_recovery_action": "RUNTIME_RETRY_DRY_RUN",
        "self_recover_paper_action_policy": "AUTONOMOUS_RETRY_ELIGIBLE",
        "autonomous_recovery_plan_classification": "PLAN_RUNTIME_RETRY",
        "recovery_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
        "self_recover_attempts_remaining": 1,
        "quarantine_required": False,
        "operator_explanation": "Runtime retry dry-run remains disabled and auditable.",
        "execution_enabled": False,
    }
    _write(
        root
        / "outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/latest_paper_autonomous_recovery_executor_attempt.json",
        attempt,
    )
    path = (
        root
        / "outputs/track_b_execution_core/paper_autonomous_recovery/executor_attempts/paper_autonomous_recovery_executor_events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(attempt, sort_keys=True) + "\n", encoding="utf-8")
    budget_event_path = root / "outputs/track_b_execution_core/recovery_budget/recovery_budget_events.jsonl"
    budget_event_path.parent.mkdir(parents=True, exist_ok=True)
    budget_event_path.write_text(
        json.dumps(
            {
                "event_type": "BUDGET_ATTEMPT_CONSUMED_SUCCESS",
                "reservation_id": "reservation-1",
                "recovery_attempt_id": "attempt-1",
                "consumed_at": "2026-05-23T12:01:00+00:00",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _seed_continuation_aware_exit_preview(
    root: Path,
    *,
    should_request_close: bool = False,
    exit_state: str = "HOLD_CONTINUATION_CONFIRMED",
) -> None:
    _write(
        root
        / "outputs/track_b_execution_core/continuation_aware_exit/latest_continuation_aware_exit_preview.json",
        {
            "generated_at": NOW.isoformat(),
            "strategy_id": "asian_drift_v1",
            "symbol": "MGC",
            "exit_policy_id": "TIME_PLUS_CONTINUATION_EXIT_V1",
            "exit_profile_id": "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1",
            "exit_state": exit_state,
            "continuation_quality_state": "STRONG_ALIGNED_CONTINUATION",
            "should_request_close": should_request_close,
            "dry_run_only": True,
            "not_order_authority": True,
            "not_lifecycle_authority": True,
            "missing_inputs": [],
            "source_strategy_report_path": str(root / "outputs/strategy/asian_drift_rule_report.json"),
            "close_intent_preview": {"would_submit": False},
        },
    )


def _write_reconciliation(root: Path) -> None:
    _write(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "live_money_eligible": False,
            "track_b_broker_positions": [],
            "track_b_broker_open_orders": [],
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": True},
            "blockers": [],
        },
    )


def _write_broker_status(root: Path) -> None:
    payload = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": NOW.isoformat(),
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": [],
        "open_orders": [],
        "live_money_eligible": False,
    }
    _write(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {**payload, "last_successful_broker_truth": payload, "latest_attempt_status": payload},
    )
    _write(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", payload)


def _write_live_position_status(root: Path) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {
            "generated_at": NOW.isoformat(),
            "open_position_count": 0,
            "open_order_count": 0,
            "open_positions": [],
            "review_required_positions": [],
            "live_money_eligible": False,
        },
    )


def _write_trade_summary(root: Path) -> None:
    _write(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {
            "generated_at": NOW.isoformat(),
            "review_required_count": 0,
            "unknown_open_order_count": 0,
            "unresolved_intent_count": 0,
            "live_money_eligible": False,
        },
    )


def _seed_exit_due_mnq_authority(root: Path) -> None:
    position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "trade_id": "trade-1",
        "lifecycle_id": "lifecycle-1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": "MNQ",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "quantity": "-1",
        },
        "lifecycle_position": {
            "entry_exec_id": "0000e1a7.test.01.01",
            "entry_perm_id": 1421894440,
            "entry_broker_identity": {"exec_id": "0000e1a7.test.01.01", "perm_id": 1421894440},
        },
    }
    _write(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [position],
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [
                {
                    "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                    "trade_id": "trade-1",
                    "lifecycle_id": "lifecycle-1",
                    "action": "BUY",
                    "quantity": "1",
                    "working": False,
                }
            ],
            "summary": {"managed_order_count": 1},
            "live_money_eligible": False,
        },
    )
    _write(
        root / "outputs/track_b_execution_core/broker_position_guardian/latest_broker_position_guardian.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "managed_close_authority": {
                "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                "allowed": True,
                "reason_codes": [],
                "candidates": [
                    {
                        "trade_id": "trade-1",
                        "lifecycle_id": "lifecycle-1",
                        "account_id": "DUM882026",
                        "symbol": "MNQ",
                        "local_symbol": "MNQM6",
                        "con_id": 770561201,
                        "action": "BUY",
                        "quantity": "1",
                    }
                ],
            },
            "live_money_eligible": False,
        },
    )


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
