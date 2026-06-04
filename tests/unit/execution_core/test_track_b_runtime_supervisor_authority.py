from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mgc_v05l.execution_core import track_b_runtime_supervisor_authority as supervisor_authority
from mgc_v05l.execution_core.track_b_agent_health import HEALTHY, STOPPED_EXPECTED
from mgc_v05l.execution_core.track_b_crash_loop_protection import NO_CRASH_LOOP, RESTART_COOLDOWN_ACTIVE
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
    NO_MANAGED_ORDERS,
)
from mgc_v05l.execution_core.track_b_managed_position_registry import NO_MANAGED_POSITIONS
from mgc_v05l.execution_core.track_b_open_order_truth import NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import (
    RESUME_POLICY_HOLD_MARKET_CLOSED,
    RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED,
    RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
    RESUME_ALLOWED_CLEAN,
    RESUME_BLOCKED_MARKET_CLOSED,
    RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    CLEANUP_REQUIRED,
    HARD_UNSAFE_HOLD,
    MANUAL_REVIEW_REQUIRED,
    MARKET_CLOSED_WAIT,
    PAPER_QUARANTINE_OBSERVE_ONLY,
    READY_FOR_OPERATOR_START,
    RUNTIME_ACTIVE_MONITOR,
    STALE_EVIDENCE_HOLD,
    SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME,
    SUPERVISOR_HARD_UNSAFE_HOLD,
    SUPERVISOR_MANUAL_REVIEW_REQUIRED,
    SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY,
    SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP,
    SUPERVISOR_RUNTIME_ALREADY_HEALTHY,
    SUPERVISOR_RUNTIME_START_ALLOWED,
    SUPERVISOR_SHARED_TRUTH_STALE,
    SUPERVISOR_WAIT_MARKET_CLOSED,
    TrackBRuntimeSupervisorAuthorityConfig,
    build_dashboard_runtime_supervisor_projection,
    build_track_b_runtime_supervisor_authority,
    write_track_b_runtime_supervisor_authority,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_market_closed_waits_without_alarm(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        proof_classification=MARKET_CLOSED_NO_FRESH_BARS,
        resume_classification=RESUME_BLOCKED_MARKET_CLOSED,
        resume_action_policy=RESUME_POLICY_HOLD_MARKET_CLOSED,
        resume_reason=MARKET_CLOSED_NO_FRESH_BARS,
        self_recover_recommendation="WAIT_MARKET_CLOSED",
        autonomous_plan_classification="WAIT_MARKET_CLOSED",
        autonomous_plan_next_action="WAIT_MARKET_CLOSED",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["supervisor_mode"] == READY_FOR_OPERATOR_START
    assert payload["recommended_action"] == "START_RUNTIME_DIAGNOSTIC_ONLY"
    assert payload["recommended_next_command"] == "operator may start Track B PAPER runtime using the repaired direct supervisor launcher"
    assert payload["proof_window_status"] == "market_closed"
    assert payload["action_allowed"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["decision_precedence"][0]["service"] == "Proof Readiness / Runtime Resume Semantics"
    assert payload["autonomous_recovery_plan_classification"] == "WAIT_MARKET_CLOSED"
    assert payload["autonomous_recovery_next_action"] == "WAIT_MARKET_CLOSED"
    assert payload["autonomous_recovery_execution_enabled"] is False
    assert payload["shared_truth_refresh_generation_id"] == "test-shared-truth-generation"
    assert payload["shared_truth_coherence_status"] == "COHERENT"
    assert payload["runtime_resume_action_policy"] == RESUME_POLICY_HOLD_MARKET_CLOSED
    assert payload["self_recover_recommended_recovery_action"] == "WAIT_MARKET_CLOSED"


def test_open_proof_window_ignores_stale_market_closed_resume_and_self_recover(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        proof_classification=READY_FOR_PROOF,
        resume_classification=RESUME_BLOCKED_MARKET_CLOSED,
        resume_action_policy=RESUME_POLICY_HOLD_MARKET_CLOSED,
        resume_reason=MARKET_CLOSED_NO_FRESH_BARS,
        self_recover_recommendation="WAIT_MARKET_CLOSED",
        autonomous_plan_classification="WAIT_MARKET_CLOSED",
        autonomous_plan_next_action="WAIT_MARKET_CLOSED",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] != SUPERVISOR_WAIT_MARKET_CLOSED
    assert payload["supervisor_mode"] != MARKET_CLOSED_WAIT
    assert payload["proof_window_status"] == "ready"


def test_clean_proof_ready_allows_runtime_start(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["supervisor_mode"] == READY_FOR_OPERATOR_START
    assert payload["recommended_action"] == "START_RUNTIME_ALLOWED"
    assert payload["recommended_next_command"] == (
        "operator may start Track B PAPER runtime using the repaired direct supervisor launcher"
    )
    assert payload["proof_window_status"] == "ready"
    assert payload["action_allowed"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["runtime_restart_authority"] is False
    assert payload["broker_mutation"] is False
    assert payload["autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert payload["autonomous_recovery_next_action"] == "RUNTIME_RETRY"
    assert payload["autonomous_recovery_execution_enabled"] is False
    assert payload["shared_truth_refresh_generation_id"] == "test-shared-truth-generation"
    assert payload["shared_truth_refresh_generated_at"] == NOW.isoformat()
    assert payload["shared_truth_coherence_status"] == "COHERENT"
    assert payload["stale_or_mixed_sources"] == []
    assert payload["runtime_resume_semantics_version"] == "v2"
    assert payload["runtime_resume_action_policy"] == RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED
    assert payload["runtime_resume_previous_runtime_generation_id"] == "runtime-generation-previous"
    assert payload["runtime_resume_proposed_next_runtime_generation_id"] == "runtime-generation-next"
    assert payload["runtime_resume_bounded_retry_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert payload["runtime_resume_attempts_remaining"] == 2
    assert payload["runtime_resume_cooldown_until"] is None
    assert payload["runtime_resume_generation_reuse_allowed"] is False
    assert payload["runtime_resume_must_start_new_generation"] is True
    assert payload["self_recover_schema_version"] == "v2"
    assert payload["self_recover_recommended_recovery_action"] == "RUNTIME_RETRY_DRY_RUN"
    assert payload["self_recover_paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["self_recover_autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert payload["self_recover_recovery_budget_key"] == "track_b_paper_runtime|RUNTIME_RETRY|test"
    assert payload["self_recover_attempts_remaining"] == 2
    assert payload["self_recover_quarantine_required"] is False


def test_active_healthy_runtime_is_left_running(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_ACTIVE_TRADE_CAPABLE)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_ALREADY_HEALTHY
    assert payload["supervisor_mode"] == RUNTIME_ACTIVE_MONITOR
    assert payload["recommended_action"] == "LEAVE_RUNTIME_RUNNING"
    assert payload["recommended_next_command"] == "monitor active runtime through execution_core shared truth artifacts"
    assert payload["safe_to_leave_runtime_running"] is True
    assert payload["safe_to_start_runtime"] is False


def test_active_observation_only_runtime_is_left_running(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_ACTIVE_OBSERVATION_ONLY)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_ALREADY_HEALTHY
    assert payload["supervisor_mode"] == RUNTIME_ACTIVE_MONITOR
    assert payload["recommended_action"] == "LEAVE_RUNTIME_RUNNING"
    assert payload["safe_to_leave_runtime_running"] is True
    assert payload["safe_to_start_runtime"] is False
    assert payload["blockers"] == []


def test_broker_exposure_requires_cleanup(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE, position_classification="ATTENTION_REQUIRED")

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME
    assert payload["supervisor_mode"] == CLEANUP_REQUIRED
    assert payload["recommended_action"] == "CLEANUP_REQUIRED_BEFORE_RUNTIME"
    assert payload["recommended_next_command"] == "perform exact scoped cleanup only after operator authorization"
    assert payload["safe_to_start_runtime"] is False


def test_owned_exit_due_exposure_allows_runtime_start_for_managed_close_maintenance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_base(
        tmp_path,
        runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
        position_classification="ATTENTION_REQUIRED",
        open_order_classification="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_classification="POSITION_WITHOUT_CLOSE_ORDER",
        managed_position_classification="OPEN_MANAGED_EXIT_DUE",
    )
    monkeypatch.setattr(
        supervisor_authority,
        "resolve_pre_restart_exposure_reconciliation",
        lambda **_: {
            "classification": "MANAGED_EXPOSURE_RESOLVED",
            "restart_with_owned_exposure_allowed": True,
            "broker_open_order_count": 0,
            "review_required_exposure_count": 0,
        },
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["recommended_action"] == "START_RUNTIME_WITH_OWNED_MANAGED_EXPOSURE"
    assert payload["safe_to_start_runtime"] is True
    assert payload["broker_mutation"] is False
    assert payload["evidence_summary"]["restart_with_owned_exposure_allowed"] is True


def test_market_closed_owned_exit_due_with_canonical_no_open_orders_allows_maintenance_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed_base(
        tmp_path,
        proof_classification=MARKET_CLOSED_NO_FRESH_BARS,
        resume_classification=RESUME_BLOCKED_MARKET_CLOSED,
        resume_action_policy=RESUME_POLICY_HOLD_MARKET_CLOSED,
        resume_reason=MARKET_CLOSED_NO_FRESH_BARS,
        runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
        position_classification="ATTENTION_REQUIRED",
        open_order_classification=NO_OPEN_ORDERS,
        managed_order_classification="POSITION_WITHOUT_CLOSE_ORDER",
        managed_position_classification="OPEN_MANAGED_EXIT_DUE",
        self_recover_recommendation="WAIT_MARKET_CLOSED",
        autonomous_plan_classification="WAIT_MARKET_CLOSED",
        autonomous_plan_next_action="WAIT_MARKET_CLOSED",
    )
    monkeypatch.setattr(
        supervisor_authority,
        "resolve_pre_restart_exposure_reconciliation",
        lambda **_: {
            "classification": "MANAGED_EXPOSURE_RESOLVED",
            "restart_with_owned_exposure_allowed": True,
            "broker_open_order_count": 0,
            "review_required_exposure_count": 0,
        },
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["supervisor_mode"] == READY_FOR_OPERATOR_START
    assert payload["recommended_action"] == "START_RUNTIME_WITH_OWNED_MANAGED_EXPOSURE"
    assert payload["proof_window_status"] == "market_closed"
    assert payload["safe_to_start_runtime"] is True
    assert payload["broker_mutation"] is False


def test_reconciled_managed_timed_hold_allows_nonconflicting_runtime_posture(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
        position_classification=ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
        open_order_classification="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_classification=ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
        managed_position_classification="OPEN_MANAGED_MATCHED",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["supervisor_mode"] == READY_FOR_OPERATOR_START
    assert payload["recommended_action"] == "MANAGED_ACTIVE_HOLD"
    assert payload["safe_to_start_runtime"] is True


def test_suspicious_order_manual_review_is_paper_advisory_not_ack_gate(tmp_path: Path) -> None:
    _seed_base(tmp_path, open_order_classification="SUSPICIOUS_ORDER_STATE", managed_order_classification="CLOSE_ORDER_SUSPICIOUS")

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_MANUAL_REVIEW_REQUIRED
    assert payload["supervisor_mode"] == MANUAL_REVIEW_REQUIRED
    assert payload["recommended_action"] == "MANUAL_REVIEW_REQUIRED"
    assert payload["recommended_next_command"] == (
        "PAPER advisory manual review: quarantine/observe, refresh evidence, and preserve artifacts before any mutation"
    )
    assert payload["operator_ack"]["required"] is False
    assert payload["operator_ack"]["reason"] == (
        "Manual review is advisory in PAPER unless a hard invariant or ambiguous mutation identity is present."
    )
    assert payload["safe_to_start_runtime"] is False


def test_crash_loop_budget_exhausted_uses_paper_quarantine_not_operator_ack(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        crash_loop_classification=RESTART_COOLDOWN_ACTIVE,
        crash_loop_restart_blocked=True,
        resume_action_policy=RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
        resume_attempts_remaining=0,
        resume_cooldown_until="2026-05-23T12:15:00+00:00",
        paper_action_policy="QUARANTINE_OBSERVE_ONLY",
        paper_autonomous_recovery_allowed=False,
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY
    assert payload["supervisor_mode"] == PAPER_QUARANTINE_OBSERVE_ONLY
    assert payload["recommended_action"] == "PAPER_QUARANTINE_OBSERVE_ONLY"
    assert payload["action_allowed"] is False
    assert payload["operator_ack"]["required"] is False
    assert payload["evidence_summary"]["paper_action_policy"] == "QUARANTINE_OBSERVE_ONLY"
    assert payload["runtime_resume_action_policy"] == RESUME_POLICY_QUARANTINE_OBSERVE_ONLY


def test_prior_unsafe_stop_with_paper_bounded_retry_allows_start_without_ack(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        resume_classification=RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
        resume_required_operator_ack=True,
        resume_reason="Prior unsafe stop requires acknowledgement.",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["supervisor_mode"] == READY_FOR_OPERATOR_START
    assert payload["operator_ack_required"] is False
    assert payload["operator_ack"]["required"] is False
    assert payload["safe_to_start_runtime"] is True
    assert payload["evidence_summary"]["paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"


def test_legacy_operator_ack_supervisor_hold_is_not_paper_ack_dependency(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        crash_loop_classification="OPERATOR_ACK_REQUIRED",
        crash_loop_restart_blocked=True,
        resume_classification=RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
        resume_required_operator_ack=True,
        paper_action_policy="OBSERVE",
        paper_autonomous_recovery_allowed=False,
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_PAPER_QUARANTINE_OBSERVE_ONLY
    assert payload["operator_ack_required"] is False
    assert payload["operator_ack"]["required"] is False
    assert "quarantine-observe" in payload["reason"]


def test_live_money_policy_hard_unsafe_blocks_supervisor(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True, paper_action_policy="HARD_UNSAFE_HOLD")

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_HARD_UNSAFE_HOLD
    assert payload["supervisor_mode"] == HARD_UNSAFE_HOLD
    assert payload["safe_to_start_runtime"] is False


def test_duplicate_writer_hard_unsafe_blocks_supervisor(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification="DUPLICATE_RUNTIME_WRITERS", duplicate_writer_count=1)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_HARD_UNSAFE_HOLD
    assert payload["supervisor_mode"] == HARD_UNSAFE_HOLD
    assert payload["blockers"][0]["code"] == "runtime_environment_truth"


def test_stale_evidence_blocks_supervisor_action(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        include_agent_health=False,
        autonomous_plan_classification="PLAN_BLOCKED_STALE_EVIDENCE",
        autonomous_plan_next_action="REFRESH_EVIDENCE",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_SHARED_TRUTH_STALE
    assert payload["supervisor_mode"] == STALE_EVIDENCE_HOLD
    assert payload["recommended_action"] == "REFRESH_SHARED_TRUTH"
    assert payload["recommended_next_command"] == "run shared truth refresh and proof readiness"
    assert any("agent_health" in blocker["detail"] for blocker in payload["blockers"])
    assert payload["autonomous_recovery_plan_classification"] == "PLAN_BLOCKED_STALE_EVIDENCE"
    assert payload["autonomous_recovery_next_action"] == "REFRESH_EVIDENCE"


def test_mixed_shared_truth_generation_blocks_supervisor(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": NOW.isoformat(), "classification": "UNKNOWN_OPEN_ORDER", "live_money_eligible": False},
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_SHARED_TRUTH_STALE
    assert payload["supervisor_mode"] == STALE_EVIDENCE_HOLD
    assert payload["shared_truth_coherence_status"] == "STALE_OR_MIXED"
    assert any(source["service"] == "Open Order Truth" for source in payload["stale_or_mixed_sources"])


def test_shared_truth_coherence_prefers_authority_generation_over_timestamp(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    shared_truth_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "shared_truth"
        / "latest_track_b_shared_truth_refresh.json"
    )
    shared_truth = json.loads(shared_truth_path.read_text(encoding="utf-8"))
    for row in shared_truth["services"]:
        if row["service"] == "Position Truth":
            row["authority_generation_id"] = "test-shared-truth-generation"
            row["authority_cycle_generated_at"] = NOW.isoformat()
    _write_json(shared_truth_path, shared_truth)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": "2026-05-23T12:00:05+00:00",
            "authority_generation_id": "test-shared-truth-generation",
            "authority_cycle_generated_at": NOW.isoformat(),
            "classification": "CLEAN_FLAT_READY",
            "summary": {"overall_classification": "CLEAN_FLAT_READY"},
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["shared_truth_coherence_status"] == "COHERENT"
    assert payload["stale_or_mixed_sources"] == []


def test_shared_truth_coherence_blocks_authority_generation_mismatch(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    shared_truth_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "shared_truth"
        / "latest_track_b_shared_truth_refresh.json"
    )
    shared_truth = json.loads(shared_truth_path.read_text(encoding="utf-8"))
    for row in shared_truth["services"]:
        if row["service"] == "Position Truth":
            row["authority_generation_id"] = "test-shared-truth-generation"
            row["authority_cycle_generated_at"] = NOW.isoformat()
    _write_json(shared_truth_path, shared_truth)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "authority_generation_id": "different-generation",
            "authority_cycle_generated_at": NOW.isoformat(),
            "classification": "CLEAN_FLAT_READY",
            "summary": {"overall_classification": "CLEAN_FLAT_READY"},
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_SHARED_TRUTH_STALE
    assert payload["shared_truth_coherence_status"] == "STALE_OR_MIXED"
    assert any(
        source["service"] == "Position Truth" and source["reason"] == "authority_generation_id_mismatch"
        for source in payload["stale_or_mixed_sources"]
    )


def test_shared_truth_coherence_legacy_generated_at_fallback_still_blocks(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": "2026-05-23T12:00:05+00:00",
            "classification": "CLEAN_FLAT_READY",
            "summary": {"overall_classification": "CLEAN_FLAT_READY"},
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_SHARED_TRUTH_STALE
    assert payload["shared_truth_coherence_status"] == "STALE_OR_MIXED"
    assert any(
        source["service"] == "Position Truth" and source["reason"] == "generated_at_mismatch"
        for source in payload["stale_or_mixed_sources"]
    )


def test_missing_shared_truth_generation_blocks_supervisor(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_shared_truth=False)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_SHARED_TRUTH_STALE
    assert payload["supervisor_mode"] == STALE_EVIDENCE_HOLD
    assert payload["shared_truth_coherence_status"] == "MISSING"
    assert payload["shared_truth_refresh_generation_id"] is None


def test_shared_truth_generation_has_no_supervisor_recursion(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    shared_truth = json.loads(
        (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "shared_truth"
            / "latest_track_b_shared_truth_refresh.json"
        ).read_text(encoding="utf-8")
    )

    services = {row["service"] for row in shared_truth["services"]}
    assert "PAPER Recovery Policy" in services
    assert "PAPER Autonomous Recovery Planner" in services
    assert "Runtime Supervisor Authority" not in services


def test_hard_unsafe_supervisor_dominates_autonomous_plan(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        live_money_eligible=True,
        paper_action_policy="HARD_UNSAFE_HOLD",
        autonomous_plan_classification="PLAN_RUNTIME_RETRY",
        autonomous_plan_next_action="RUNTIME_RETRY",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_HARD_UNSAFE_HOLD
    assert payload["supervisor_mode"] == HARD_UNSAFE_HOLD
    assert payload["autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert payload["safe_to_start_runtime"] is False


def test_safe_state_hard_hold_blocks_supervisor(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json",
        {
            "generated_at": NOW.isoformat(),
            "safe_state_classification": "SAFE_STATE_HARD_HOLD",
            "classification": "SAFE_STATE_HARD_HOLD",
            "runtime_start_allowed": False,
            "submit_allowed": False,
            "broker_mutation_allowed": False,
            "observe_only": True,
            "recovery_only": False,
            "tripped_limits": [{"limit_id": "duplicate_runtime_writer", "classification": "SAFE_STATE_HARD_HOLD"}],
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_HARD_UNSAFE_HOLD
    assert payload["safe_state_classification"] == "SAFE_STATE_HARD_HOLD"
    assert payload["safe_state_observe_only"] is True
    assert payload["safe_to_start_runtime"] is False
    assert payload["blockers"][0]["code"] == "runtime_safe_state_envelope"


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path)
    payload = build_track_b_runtime_supervisor_authority(config=config, now=NOW)

    authority_path = write_track_b_runtime_supervisor_authority(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    direct_projection = build_dashboard_runtime_supervisor_projection(authority_payload=payload, authority_path=authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True
    assert "latest_track_b_paper_autonomous_recovery" not in json.dumps(projection)


def _seed_base(
    root: Path,
    *,
    proof_classification: str = READY_FOR_PROOF,
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    resume_classification: str = RESUME_ALLOWED_CLEAN,
    resume_reason: str = "Clean flat shared truth and proof readiness is READY_FOR_PROOF.",
    resume_action_policy: str = RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED,
    resume_attempts_remaining: int = 2,
    resume_cooldown_until: str | None = None,
    resume_required_operator_ack: bool = False,
    position_classification: str = "CLEAN_FLAT_READY",
    open_order_classification: str = NO_OPEN_ORDERS,
    managed_order_classification: str = NO_MANAGED_ORDERS,
    order_adjustment_plan_classification: str = "NO_ACTION_NEEDED",
    managed_position_classification: str = NO_MANAGED_POSITIONS,
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_lease_classification: str = "ACTIVE",
    crash_loop_classification: str = NO_CRASH_LOOP,
    crash_loop_restart_blocked: bool = False,
    self_recover_recommendation: str = "RESTART_RUNTIME_ALLOWED",
    paper_action_policy: str = "AUTONOMOUS_RETRY_ELIGIBLE",
    paper_autonomous_recovery_allowed: bool = True,
    autonomous_plan_classification: str = "PLAN_RUNTIME_RETRY",
    autonomous_plan_next_action: str = "RUNTIME_RETRY",
    autonomous_plan_budget_exhausted: bool = False,
    duplicate_writer_count: int = 0,
    live_money_eligible: bool = False,
    include_agent_health: bool = True,
    include_shared_truth: bool = True,
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": runtime_classification,
            "writer_authority": "DUPLICATE_WRITER" if duplicate_writer_count else "NO_ACTIVE_WRITER",
            "duplicate_writer_count": duplicate_writer_count,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": resume_classification,
            "resume_semantics_version": "v2",
            "resume_action_policy": resume_action_policy,
            "previous_runtime_generation_id": "runtime-generation-previous",
            "proposed_next_runtime_generation_id": "runtime-generation-next",
            "bounded_retry_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "attempts_remaining": resume_attempts_remaining,
            "cooldown_until": resume_cooldown_until,
            "generation_reuse_allowed": False,
            "must_start_new_generation": True,
            "allowed": resume_classification == RESUME_ALLOWED_CLEAN,
            "safe_to_start_runtime": resume_classification == RESUME_ALLOWED_CLEAN,
            "required_operator_ack": resume_required_operator_ack,
            "reason": resume_reason,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {
            "self_recover_schema_version": "v2",
            "generated_at": NOW.isoformat(),
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "recovery_plan_id": "self-recover-plan-1",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_generation_id": "test-shared-truth-generation",
            "paper_action_policy": paper_action_policy,
            "autonomous_recovery_plan_classification": autonomous_plan_classification,
            "recommended_recovery_action": "WAIT_MARKET_CLOSED"
            if self_recover_recommendation == "WAIT_MARKET_CLOSED"
            else (
                "QUARANTINE_OBSERVE_ONLY"
                if paper_action_policy == "QUARANTINE_OBSERVE_ONLY"
                else (
                    "HARD_UNSAFE_HOLD"
                    if paper_action_policy == "HARD_UNSAFE_HOLD"
                    else "RUNTIME_RETRY_DRY_RUN"
                )
            ),
            "recovery_budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
            "attempts_remaining": resume_attempts_remaining,
            "cooldown_until": resume_cooldown_until,
            "quarantine_required": paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
            "agent_health_top_blockers": [],
            "operator_explanation": "self recover structured plan",
            "execution_enabled": False,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": crash_loop_classification,
            "restart_blocked": crash_loop_restart_blocked,
            "operator_ack_required": False,
            "live_money_eligible": live_money_eligible,
        },
    )
    if include_agent_health:
        _write_json(
            root / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "AGENT_HEALTH_READY",
                "live_money_eligible": live_money_eligible,
                "agents": [
                    {
                        "agent_id": "track_b_paper_runtime",
                        "category": "runtime",
                        "status": STOPPED_EXPECTED,
                        "reason": runtime_classification,
                        "blocking_for_proof": False,
                        "blocking_for_runtime_submit": False,
                    },
                    {
                        "agent_id": "phase1_databento_live_candles",
                        "category": "market_data",
                        "status": HEALTHY,
                        "reason": "phase1_runtime_candles_ready",
                        "blocking_for_proof": False,
                        "blocking_for_runtime_submit": False,
                    },
                ],
            },
        )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "agent_registry" / "latest_agent_registry.json",
        {"generated_at": NOW.isoformat(), "classification": "AGENT_REGISTRY_READY"},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "phase1_session_reason": proof_classification,
            "phase1_market_session": {
                "classification": "MARKET_CLOSED_NO_FRESH_BARS"
                if proof_classification == MARKET_CLOSED_NO_FRESH_BARS
                else "MARKET_OPEN_EXPECT_FRESH_BARS",
                "market_closed": proof_classification == MARKET_CLOSED_NO_FRESH_BARS,
                "reason": "MARKET_CLOSED_NO_FRESH_BARS"
                if proof_classification == MARKET_CLOSED_NO_FRESH_BARS
                else "GLOBEX_SESSION_OPEN",
            },
            "live_money_eligible": live_money_eligible,
        },
    )
    if include_shared_truth:
        _write_json(
            root / "outputs" / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json",
            _shared_truth_payload(
                root,
                runtime_classification=runtime_classification,
                position_classification=position_classification,
                open_order_classification=open_order_classification,
                managed_order_classification=managed_order_classification,
                managed_position_classification=managed_position_classification,
                reconciliation_classification=reconciliation_classification,
                broker_lease_classification=broker_lease_classification,
                paper_action_policy=paper_action_policy,
                autonomous_plan_classification=autonomous_plan_classification,
            ),
        )
    _write_json(
        root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        {"generated_at": NOW.isoformat(), "canonical_readiness": "READY_SUBMIT_CAPABLE"},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": position_classification,
            "summary": {"overall_classification": position_classification},
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": NOW.isoformat(), "classification": open_order_classification, "live_money_eligible": live_money_eligible},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"generated_at": NOW.isoformat(), "classification": managed_order_classification, "live_money_eligible": live_money_eligible},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_order_adjustment_plan.json",
        {"generated_at": NOW.isoformat(), "classification": order_adjustment_plan_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {"generated_at": NOW.isoformat(), "classification": managed_position_classification, "live_money_eligible": live_money_eligible},
    )
    _write_json(
        root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": NOW.isoformat(), "classification": reconciliation_classification, "live_money_eligible": live_money_eligible},
    )
    _write_json(
        root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": NOW.isoformat(), "classification": broker_lease_classification, "live_money_eligible": live_money_eligible},
    )
    _write_json(
        root / "outputs" / "reports" / "phase1_runtime_data_readiness" / "latest_phase1_runtime_data_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "market_session": {"classification": proof_classification},
            "rows": [],
        },
    )
    _write_json(
        root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "latest_runtime_stop_provenance.json",
        {
            "generated_at": NOW.isoformat(),
            "stop_source": "launcher",
            "stop_reason": "expected_clean_down",
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json",
        {
            "generated_at": NOW.isoformat(),
            "severity": "UNSAFE" if paper_action_policy == "HARD_UNSAFE_HOLD" else "INFO",
            "paper_action_policy": paper_action_policy,
            "live_action_policy": "REQUIRE_ACK",
            "autonomous_recovery_allowed": paper_autonomous_recovery_allowed,
            "requires_operator_ack_for_paper": False,
            "bounded_recovery_budget": {
                "max_attempts_per_target": 1,
                "max_attempts_per_window": 2,
                "cooldown_seconds": 300,
                "budget_exhausted": paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
            },
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "paper_autonomous_recovery"
        / "latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": autonomous_plan_classification,
            "execution_enabled": False,
            "proposed_actions": [
                {
                    "action_id": autonomous_plan_next_action.lower(),
                    "action_type": autonomous_plan_next_action,
                    "execution_enabled": False,
                    "would_mutate_broker": False,
                    "would_mutate_lifecycle": False,
                    "would_restart_runtime": autonomous_plan_next_action == "RUNTIME_RETRY",
                }
            ]
            if autonomous_plan_next_action
            else [],
            "blockers": [],
            "evidence_summary": {
                "bounded_recovery_budget": {
                    "budget_exhausted": autonomous_plan_budget_exhausted,
                    "max_attempts_per_target": 1,
                    "max_attempts_per_window": 2,
                    "cooldown_seconds": 300,
                }
            },
            "live_money_eligible": live_money_eligible,
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _shared_truth_payload(
    root: Path,
    *,
    runtime_classification: str,
    position_classification: str,
    open_order_classification: str,
    managed_order_classification: str,
    managed_position_classification: str,
    reconciliation_classification: str,
    broker_lease_classification: str,
    paper_action_policy: str,
    autonomous_plan_classification: str,
) -> dict:
    rows = [
        (
            "Open Order Truth",
            open_order_classification,
            root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        ),
        (
            "Managed Order Registry",
            managed_order_classification,
            root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        ),
        (
            "Position Truth",
            position_classification,
            root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        ),
        (
            "Runtime Environment Truth",
            runtime_classification,
            root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        ),
        (
            "Managed Position Registry",
            managed_position_classification,
            root / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        ),
        (
            "Reconciliation",
            reconciliation_classification,
            root
            / "outputs"
            / "reports"
            / "track_b_paper_broker_reconciliation"
            / "latest_track_b_paper_broker_reconciliation.json",
        ),
        (
            "Broker Truth Lease",
            broker_lease_classification,
            root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        ),
        (
            "PAPER Recovery Policy",
            paper_action_policy,
            root / "outputs" / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json",
        ),
        (
            "PAPER Autonomous Recovery Planner",
            autonomous_plan_classification,
            root
            / "outputs"
            / "track_b_execution_core"
            / "paper_autonomous_recovery"
            / "latest_paper_autonomous_recovery_plan.json",
        ),
    ]
    return {
        "schema_version": "track_b_shared_truth_refresh_v1",
        "generated_at": NOW.isoformat(),
        "refresh_generation_id": "test-shared-truth-generation",
        "refresh_phase": "pre_supervisor_refresh",
        "mode": "PAPER",
        "read_only": True,
        "services": [
            {
                "service": service,
                "classification": classification,
                "generated_at": NOW.isoformat(),
                "artifact_path": str(path),
            }
            for service, classification, path in rows
        ],
        "classifications": {service: classification for service, classification, _path in rows},
        "artifact_paths": {service: str(path) for service, _classification, path in rows},
    }
