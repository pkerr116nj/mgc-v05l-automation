from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import HEALTHY, STOPPED_EXPECTED
from mgc_v05l.execution_core.track_b_crash_loop_protection import (
    NO_CRASH_LOOP,
    REPEATED_UNSAFE_STOP_QUARANTINE,
    RESTART_COOLDOWN_ACTIVE,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import NO_MANAGED_ORDERS
from mgc_v05l.execution_core.track_b_managed_position_registry import NO_MANAGED_POSITIONS
from mgc_v05l.execution_core.track_b_open_order_truth import NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_paper_recovery_policy import (
    ATTENTION,
    AUTONOMOUS_RETRY_ELIGIBLE,
    HARD_UNSAFE_HOLD,
    INFO,
    OBSERVE,
    QUARANTINE_OBSERVE_ONLY,
    REFRESH_EVIDENCE,
    SCOPED_RECOVERY_ELIGIBLE,
    TrackBPaperRecoveryPolicyConfig,
    WARNING,
    build_dashboard_paper_recovery_policy_projection,
    build_track_b_paper_recovery_policy,
    write_track_b_paper_recovery_policy,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import RUNTIME_DOWN_CLEAN, RUNTIME_DOWN_WITH_BROKER_EXPOSURE
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import RESUME_ALLOWED_CLEAN
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    READY_FOR_OPERATOR_START,
    SUPERVISOR_RUNTIME_START_ALLOWED,
    SUPERVISOR_WAIT_MARKET_CLOSED,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import RESTART_RUNTIME_ALLOWED, WAIT_MARKET_CLOSED
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_market_closed_observes_without_paper_operator_ack(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        supervisor_classification=SUPERVISOR_WAIT_MARKET_CLOSED,
        proof_classification=MARKET_CLOSED_NO_FRESH_BARS,
        phase1_reason=MARKET_CLOSED_NO_FRESH_BARS,
        self_recover_recommendation=WAIT_MARKET_CLOSED,
    )

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == INFO
    assert payload["paper_action_policy"] == OBSERVE
    assert payload["requires_operator_ack_for_paper"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["runtime_restart_allowed"] is False
    assert payload["reason"] == MARKET_CLOSED_NO_FRESH_BARS


def test_clean_proof_ready_is_autonomous_retry_eligible_without_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == INFO
    assert payload["paper_action_policy"] == AUTONOMOUS_RETRY_ELIGIBLE
    assert payload["autonomous_recovery_allowed"] is True
    assert payload["requires_operator_ack_for_paper"] is False
    assert payload["bounded_recovery_budget"]["budget_exhausted"] is False


def test_runtime_preflight_failure_within_budget_is_retry_eligible(tmp_path: Path) -> None:
    _seed_base(tmp_path, stop_reason="runtime_exited_after_preflight")

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == WARNING
    assert payload["paper_action_policy"] == AUTONOMOUS_RETRY_ELIGIBLE
    assert payload["autonomous_recovery_allowed"] is True
    assert payload["requires_operator_ack_for_paper"] is False
    assert any(warning["code"] == "runtime_stop_or_launch" for warning in payload["warnings"])


def test_crash_loop_budget_exhausted_quarantines_without_paper_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path, crash_loop_classification=RESTART_COOLDOWN_ACTIVE, crash_loop_restart_blocked=True)

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == ATTENTION
    assert payload["paper_action_policy"] == QUARANTINE_OBSERVE_ONLY
    assert payload["bounded_recovery_budget"]["budget_exhausted"] is True
    assert payload["requires_operator_ack_for_paper"] is False
    assert payload["live_action_policy"] == "REQUIRE_ACK"


def test_repeated_unsafe_stop_quarantine_keeps_future_live_ack_only(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        crash_loop_classification=REPEATED_UNSAFE_STOP_QUARANTINE,
        crash_loop_restart_blocked=True,
    )

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["paper_action_policy"] == QUARANTINE_OBSERVE_ONLY
    assert payload["requires_operator_ack_for_paper"] is False
    assert payload["live_action_policy"] == "REQUIRE_ACK"
    assert payload["bounded_recovery_budget"]["budget_exhausted"] is True


def test_prior_unsafe_stop_current_clean_truth_uses_enhanced_observation_not_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path, previous_broker_safe_at_stop=False)

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == WARNING
    assert payload["paper_action_policy"] == AUTONOMOUS_RETRY_ELIGIBLE
    assert payload["autonomous_recovery_allowed"] is True
    assert payload["bounded_recovery_budget"]["max_attempts_per_target"] == 1
    assert payload["requires_operator_ack_for_paper"] is False
    assert "enhanced observation" in payload["reason"]


def test_broker_exposure_with_exact_target_is_scoped_recovery_eligible(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
        position_classification="ATTENTION_REQUIRED",
        managed_position_classification="BROKER_BACKED_ADOPTION_REQUIRED",
        exact_position=True,
    )

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == ATTENTION
    assert payload["paper_action_policy"] == SCOPED_RECOVERY_ELIGIBLE
    assert payload["autonomous_recovery_allowed"] is True
    assert payload["broker_mutation_allowed"] is False
    assert payload["requires_operator_ack_for_paper"] is False


def test_suspicious_order_quarantines_observe_only(tmp_path: Path) -> None:
    _seed_base(tmp_path, open_order_classification="SUSPICIOUS_ORDER_STATE", managed_order_classification="CLOSE_ORDER_SUSPICIOUS")

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == ATTENTION
    assert payload["paper_action_policy"] == QUARANTINE_OBSERVE_ONLY
    assert payload["autonomous_recovery_allowed"] is False
    assert payload["requires_operator_ack_for_paper"] is False


def test_duplicate_writer_is_hard_unsafe_hold(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification="DUPLICATE_RUNTIME_WRITERS", duplicate_writer_count=1)

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == "UNSAFE"
    assert payload["paper_action_policy"] == HARD_UNSAFE_HOLD
    assert payload["requires_operator_ack_for_paper"] is False
    assert payload["blockers"][0]["code"] == "runtime_environment_truth"


def test_live_money_possible_is_hard_unsafe_hold(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True)

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == "UNSAFE"
    assert payload["paper_action_policy"] == HARD_UNSAFE_HOLD
    assert payload["autonomous_recovery_allowed"] is False
    assert payload["blockers"][0]["code"] == "live_money_eligible"


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path)
    payload = build_track_b_paper_recovery_policy(config=config, now=NOW)

    authority_path = write_track_b_paper_recovery_policy(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    direct_projection = build_dashboard_paper_recovery_policy_projection(authority_payload=payload, authority_path=authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True


def test_missing_authority_refreshes_evidence_without_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_agent_health=False)

    payload = build_track_b_paper_recovery_policy(config=TrackBPaperRecoveryPolicyConfig(repo_root=tmp_path), now=NOW)

    assert payload["severity"] == WARNING
    assert payload["paper_action_policy"] == REFRESH_EVIDENCE
    assert payload["requires_operator_ack_for_paper"] is False
    assert any(warning["code"] == "stale_or_missing_evidence" for warning in payload["warnings"])


def _seed_base(
    root: Path,
    *,
    supervisor_classification: str = SUPERVISOR_RUNTIME_START_ALLOWED,
    supervisor_mode: str = READY_FOR_OPERATOR_START,
    proof_classification: str = READY_FOR_PROOF,
    phase1_reason: str = "phase1_runtime_candles_ready",
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    position_classification: str = "CLEAN_FLAT_READY",
    open_order_classification: str = NO_OPEN_ORDERS,
    managed_order_classification: str = NO_MANAGED_ORDERS,
    order_adjustment_plan_classification: str = "NO_ACTION_NEEDED",
    managed_position_classification: str = NO_MANAGED_POSITIONS,
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_lease_classification: str = "ACTIVE",
    crash_loop_classification: str = NO_CRASH_LOOP,
    crash_loop_restart_blocked: bool = False,
    self_recover_recommendation: str = RESTART_RUNTIME_ALLOWED,
    previous_broker_safe_at_stop: bool = True,
    stop_reason: str = "expected_clean_down",
    duplicate_writer_count: int = 0,
    live_money_eligible: bool = False,
    exact_position: bool = False,
    include_agent_health: bool = True,
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "safe_to_start_runtime": supervisor_classification == SUPERVISOR_RUNTIME_START_ALLOWED,
            "recommended_next_command": "operator may start runtime",
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": crash_loop_classification,
            "restart_blocked": crash_loop_restart_blocked,
            "operator_ack_required": crash_loop_classification == "OPERATOR_ACK_REQUIRED",
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": RESUME_ALLOWED_CLEAN,
            "previous_broker_safe_at_stop": previous_broker_safe_at_stop,
            "live_money_eligible": live_money_eligible,
        },
    )
    if include_agent_health:
        _write_json(
            root / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json",
            {
                "generated_at": NOW.isoformat(),
                "classification": "AGENT_HEALTH_READY",
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
                        "reason": phase1_reason,
                        "blocking_for_proof": False,
                        "blocking_for_runtime_submit": False,
                    },
                ],
            },
        )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": proof_classification,
            "phase1_session_reason": phase1_reason,
            "live_money_eligible": live_money_eligible,
        },
    )
    position_payload = {
        "generated_at": NOW.isoformat(),
        "classification": position_classification,
        "summary": {"overall_classification": position_classification},
        "live_money_eligible": live_money_eligible,
    }
    managed_position_payload = {
        "generated_at": NOW.isoformat(),
        "classification": managed_position_classification,
        "live_money_eligible": live_money_eligible,
    }
    if exact_position:
        exact = {
            "symbol": "MNQ",
            "contract": "MNQM6",
            "quantity": 1,
            "ownership_id": "submit_owner_test",
            "lifecycle_id": "lifecycle_test",
            "exact_target_identity_known": True,
        }
        position_payload["positions"] = [exact]
        managed_position_payload["managed_positions"] = [exact]
    _write_json(root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json", position_payload)
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
        managed_position_payload,
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
        root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "latest_runtime_stop_provenance.json",
        {
            "generated_at": NOW.isoformat(),
            "stop_source": "launcher",
            "stop_reason": stop_reason,
            "runtime_instance_id": "track-b-paper-runtime-test",
            "broker_safe_at_stop": previous_broker_safe_at_stop,
            "live_money_eligible": live_money_eligible,
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
