from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    NO_ACTION_NEEDED,
    PLAN_BLOCKED_BUDGET_EXHAUSTED,
    PLAN_BLOCKED_IDENTITY_AMBIGUITY,
    PLAN_BLOCKED_STALE_EVIDENCE,
    PLAN_HARD_UNSAFE_HOLD,
    PLAN_MANAGED_ORDER_MODIFY,
    PLAN_MARKET_DATA_RESTART,
    PLAN_QUARANTINE_OBSERVE_ONLY,
    PLAN_RUNTIME_RETRY,
    PLAN_SCOPED_POSITION_CLEANUP,
    PLAN_TARGETED_CANCEL_REPLACE,
    WAIT_MARKET_CLOSED,
    TrackBPaperAutonomousRecoveryPlannerConfig,
    build_track_b_paper_autonomous_recovery_plan,
    write_track_b_paper_autonomous_recovery_plan,
)
from mgc_v05l.execution_core.track_b_paper_recovery_policy import (
    AUTONOMOUS_RETRY_ELIGIBLE,
    OBSERVE,
    QUARANTINE_OBSERVE_ONLY,
    REFRESH_EVIDENCE,
    SCOPED_RECOVERY_ELIGIBLE,
)
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_runtime_environment_truth import RUNTIME_DOWN_CLEAN, RUNTIME_DOWN_WITH_BROKER_EXPOSURE
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    READY_FOR_OPERATOR_START,
    SUPERVISOR_RUNTIME_START_ALLOWED,
    SUPERVISOR_WAIT_MARKET_CLOSED,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_market_closed_plans_wait_without_execution(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        paper_action_policy=OBSERVE,
        supervisor_classification=SUPERVISOR_WAIT_MARKET_CLOSED,
        proof_classification=MARKET_CLOSED_NO_FRESH_BARS,
        phase1_reason=MARKET_CLOSED_NO_FRESH_BARS,
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == WAIT_MARKET_CLOSED
    assert payload["execution_enabled"] is False
    assert payload["proposed_actions"][0]["execution_enabled"] is False
    assert payload["proposed_actions"][0]["would_restart_runtime"] is False


def test_clean_proof_ready_plans_runtime_retry_dry_run(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_RUNTIME_RETRY
    assert payload["control_plane_snapshot_id"] == "snapshot-test"
    assert payload["shared_truth_refresh_generation_id"] == "shared-truth-generation-test"
    assert payload["snapshot_coherence_status"] == "COHERENT"
    assert payload["supervisor_decision_id"] == "supervisor-decision-test"
    assert payload["supervisor_classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["runtime_restart_allowed"] is False
    assert payload["proposed_actions"][0]["action_type"] == "RUNTIME_RETRY"
    assert "coherent_control_plane_snapshot_captured_immediately_before_action" in payload["proposed_actions"][0][
        "required_preconditions"
    ]
    assert payload["proposed_actions"][0]["would_restart_runtime"] is True
    assert payload["proposed_actions"][0]["execution_enabled"] is False


def test_missing_control_plane_snapshot_blocks_as_stale_evidence(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_control_plane_snapshot=False)

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_BLOCKED_STALE_EVIDENCE
    assert payload["control_plane_snapshot_id"] == ""
    assert "control_plane_snapshot_missing" in payload["evidence_summary"]["stale_or_missing_evidence"]


def test_incoherent_control_plane_snapshot_blocks_as_stale_evidence(tmp_path: Path) -> None:
    _seed_base(tmp_path, snapshot_coherence_status="STALE_OR_MIXED")

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_BLOCKED_STALE_EVIDENCE
    assert payload["snapshot_coherence_status"] == "STALE_OR_MIXED"
    assert "control_plane_snapshot_not_coherent" in payload["evidence_summary"]["stale_or_missing_evidence"]


def test_stale_control_plane_snapshot_blocks_as_stale_evidence(tmp_path: Path) -> None:
    _seed_base(tmp_path, snapshot_generated_at=datetime(2026, 5, 23, 11, 0, tzinfo=UTC))

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(
            repo_root=tmp_path,
            control_plane_snapshot_max_age_seconds=60,
        ),
        now=NOW,
    )

    assert payload["classification"] == PLAN_BLOCKED_STALE_EVIDENCE
    assert payload["evidence_summary"]["control_plane_snapshot_age_seconds"] == 3600.0
    assert "control_plane_snapshot_stale" in payload["evidence_summary"]["stale_or_missing_evidence"]


def test_stale_evidence_plans_evidence_refresh(tmp_path: Path) -> None:
    _seed_base(tmp_path, paper_action_policy=REFRESH_EVIDENCE)

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_BLOCKED_STALE_EVIDENCE
    assert payload["proposed_actions"][0]["action_type"] == "REFRESH_EVIDENCE"


def test_producer_down_open_market_plans_market_data_restart_dry_run(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        paper_action_policy=OBSERVE,
        phase1_agent_status="STOPPED_UNEXPECTED",
        phase1_agent_reason="producer_not_running",
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_MARKET_DATA_RESTART
    assert payload["proposed_actions"][0]["action_type"] == "MARKET_DATA_RESTART"
    assert payload["proposed_actions"][0]["execution_enabled"] is False
    assert payload["proposed_actions"][0]["would_restart_runtime"] is False


def test_exact_exposure_plans_scoped_position_cleanup(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        paper_action_policy=SCOPED_RECOVERY_ELIGIBLE,
        runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
        position_classification="ATTENTION_REQUIRED",
        managed_position_classification="BROKER_BACKED_ADOPTION_REQUIRED",
        exact_position=True,
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_SCOPED_POSITION_CLEANUP
    action = payload["proposed_actions"][0]
    assert action["target_identity"]["symbol"] == "MNQ"
    assert action["would_mutate_broker"] is True
    assert action["would_mutate_lifecycle"] is True
    assert action["execution_enabled"] is False


def test_modifiable_order_plans_modify_in_place(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        open_order_classification="OPEN_CLOSE_ORDER_WORKING",
        managed_order_classification="CLOSE_ORDER_MODIFIABLE",
        order_adjustment_plan_classification="MODIFY_IN_PLACE_ELIGIBLE",
        include_order_plan=True,
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_MANAGED_ORDER_MODIFY
    assert payload["proposed_actions"][0]["target_identity"]["perm_id"] == "perm-1"
    assert payload["proposed_actions"][0]["would_mutate_broker"] is True
    assert payload["proposed_actions"][0]["execution_enabled"] is False


def test_terminal_old_order_plans_targeted_cancel_replace(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        open_order_classification="BROKER_POSITION_WITHOUT_CLOSE_ORDER",
        managed_order_classification="ORDER_TERMINAL_CANCELLED",
        order_adjustment_plan_classification="TARGETED_CANCEL_REPLACE_REQUIRED",
        include_order_plan=True,
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_TARGETED_CANCEL_REPLACE
    assert payload["proposed_actions"][0]["action_type"] == "TARGETED_CANCEL_REPLACE"
    assert payload["proposed_actions"][0]["execution_enabled"] is False


def test_suspicious_order_quarantines_or_blocks_identity_ambiguity(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        open_order_classification="SUSPICIOUS_ORDER_STATE",
        managed_order_classification="CLOSE_ORDER_SUSPICIOUS",
        order_adjustment_plan_classification="REVIEW_REQUIRED_SUSPICIOUS_STATE",
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_QUARANTINE_OBSERVE_ONLY
    assert payload["proposed_actions"][0]["action_type"] == "QUARANTINE_OBSERVE_ONLY"


def test_budget_exhausted_blocks_autonomous_retry(tmp_path: Path) -> None:
    _seed_base(tmp_path, budget_exhausted=True)

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_BLOCKED_BUDGET_EXHAUSTED
    assert payload["blocked_actions"][0]["execution_enabled"] is False


def test_live_money_and_duplicate_writer_are_hard_unsafe(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True)
    live_money = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )
    assert live_money["classification"] == PLAN_HARD_UNSAFE_HOLD

    duplicate_root = tmp_path / "duplicate"
    _seed_base(duplicate_root, runtime_classification="DUPLICATE_RUNTIME_WRITERS", duplicate_writer_count=1)
    duplicate = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=duplicate_root),
        now=NOW,
    )
    assert duplicate["classification"] == PLAN_HARD_UNSAFE_HOLD


def test_identity_ambiguous_duplicate_order_blocks(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        open_order_classification="DUPLICATE_CLOSE_ORDER",
        managed_order_classification="DUPLICATE_CLOSE_ORDER_BLOCKED",
        order_adjustment_plan_classification="DO_NOT_REPLACE_DUPLICATE_RISK",
    )

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == PLAN_BLOCKED_IDENTITY_AMBIGUITY


def test_no_action_when_no_recovery_policy_requires_it(tmp_path: Path) -> None:
    _seed_base(tmp_path, paper_action_policy=OBSERVE)

    payload = build_track_b_paper_autonomous_recovery_plan(
        config=TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path),
        now=NOW,
    )

    assert payload["classification"] == NO_ACTION_NEEDED


def test_writes_authority_artifact_without_dashboard_projection(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=tmp_path)
    payload = build_track_b_paper_autonomous_recovery_plan(config=config, now=NOW)

    authority_path = write_track_b_paper_autonomous_recovery_plan(config=config, payload=payload)
    written = json.loads(authority_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "paper_autonomous_recovery" / "latest_paper_autonomous_recovery_plan.json"
    assert written["schema_version"] == "track_b_paper_autonomous_recovery_plan_v1"
    assert "dashboard_projection" not in written["artifact_paths"]
    assert "operator_dashboard/runtime/latest_track_b_paper_autonomous_recovery" not in json.dumps(written)


def _seed_base(
    root: Path,
    *,
    paper_action_policy: str = AUTONOMOUS_RETRY_ELIGIBLE,
    supervisor_classification: str = SUPERVISOR_RUNTIME_START_ALLOWED,
    supervisor_mode: str = READY_FOR_OPERATOR_START,
    proof_classification: str = READY_FOR_PROOF,
    phase1_reason: str = "phase1_runtime_candles_ready",
    phase1_agent_status: str = "HEALTHY",
    phase1_agent_reason: str = "phase1_runtime_candles_ready",
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    position_classification: str = "CLEAN_FLAT_READY",
    open_order_classification: str = "NO_OPEN_ORDERS",
    managed_order_classification: str = "NO_MANAGED_ORDERS",
    order_adjustment_plan_classification: str = "NO_ACTION_NEEDED",
    managed_position_classification: str = "NO_MANAGED_POSITIONS",
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_lease_classification: str = "ACTIVE",
    budget_exhausted: bool = False,
    duplicate_writer_count: int = 0,
    live_money_eligible: bool = False,
    exact_position: bool = False,
    include_order_plan: bool = False,
    include_control_plane_snapshot: bool = True,
    snapshot_coherence_status: str = "COHERENT",
    snapshot_generated_at: datetime = NOW,
) -> None:
    if include_control_plane_snapshot:
        _write_json(
            root / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json",
            {
                "generated_at": snapshot_generated_at.isoformat(),
                "classification": "CONTROL_PLANE_SNAPSHOT_READY",
                "control_plane_snapshot_id": "snapshot-test",
                "shared_truth_refresh_generation_id": "shared-truth-generation-test",
                "shared_truth_coherence_status": snapshot_coherence_status,
                "runtime_supervisor_decision_id": "supervisor-decision-test",
                "runtime_supervisor_classification": supervisor_classification,
                "supervisor_mode": supervisor_mode,
                "safe_to_start_runtime": supervisor_classification == SUPERVISOR_RUNTIME_START_ALLOWED,
                "live_money_eligible": live_money_eligible,
            },
        )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": paper_action_policy,
            "severity": "INFO" if paper_action_policy in {OBSERVE, AUTONOMOUS_RETRY_ELIGIBLE} else "ATTENTION",
            "paper_action_policy": paper_action_policy,
            "autonomous_recovery_allowed": paper_action_policy
            in {AUTONOMOUS_RETRY_ELIGIBLE, SCOPED_RECOVERY_ELIGIBLE},
            "requires_operator_ack_for_paper": False,
            "live_action_policy": "REQUIRE_ACK",
            "runtime_restart_allowed": False,
            "broker_mutation_allowed": False,
            "bounded_recovery_budget": {"budget_exhausted": budget_exhausted, "max_attempts_per_target": 1},
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "recovery_budget" / "latest_recovery_budget_ledger.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "RECOVERY_BUDGET_EXHAUSTED" if budget_exhausted else "RECOVERY_BUDGET_AVAILABLE",
            "budget_exhausted": budget_exhausted,
            "quarantine_required": budget_exhausted,
            "entries": [
                {
                    "budget_key": "track_b_paper_runtime|RUNTIME_RETRY|empty|*|runtime_retry",
                    "agent_id": "track_b_paper_runtime",
                    "action_type": "RUNTIME_RETRY",
                    "attempts_remaining": 0 if budget_exhausted else 1,
                    "budget_exhausted": budget_exhausted,
                }
            ],
            "summary": {
                "budget_exhausted": budget_exhausted,
                "quarantine_required": budget_exhausted,
                "minimum_attempts_remaining": 0 if budget_exhausted else 1,
            },
            "artifact_paths": {
                "authority": str(
                    root / "outputs" / "track_b_execution_core" / "recovery_budget" / "latest_recovery_budget_ledger.json"
                )
            },
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_supervisor" / "latest_runtime_supervisor_authority.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": supervisor_classification,
            "supervisor_mode": supervisor_mode,
            "safe_to_start_runtime": supervisor_classification == SUPERVISOR_RUNTIME_START_ALLOWED,
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json",
        {"generated_at": NOW.isoformat(), "classification": "RESUME_ALLOWED_CLEAN", "live_money_eligible": live_money_eligible},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {"generated_at": NOW.isoformat(), "classification": "RESTART_RUNTIME_ALLOWED", "recommendation": "RESTART_RUNTIME_ALLOWED"},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json",
        {"generated_at": NOW.isoformat(), "classification": "NO_CRASH_LOOP", "restart_blocked": False},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "AGENT_HEALTH_READY",
            "agents": [
                {"agent_id": "phase1_databento_live_candles", "status": phase1_agent_status, "reason": phase1_agent_reason}
            ],
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json",
        {"generated_at": NOW.isoformat(), "classification": proof_classification, "phase1_session_reason": phase1_reason},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": NOW.isoformat(), "classification": open_order_classification, "live_money_eligible": live_money_eligible},
    )
    managed_order_payload = {
        "generated_at": NOW.isoformat(),
        "classification": managed_order_classification,
        "live_money_eligible": live_money_eligible,
    }
    if include_order_plan:
        managed_order_payload["managed_orders"] = [_order_identity()]
    _write_json(root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json", managed_order_payload)
    order_plan_payload = {"generated_at": NOW.isoformat(), "classification": order_adjustment_plan_classification}
    if include_order_plan:
        order_plan_payload["plans"] = [{"classification": order_adjustment_plan_classification, "identity": _order_identity()}]
    _write_json(root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_order_adjustment_plan.json", order_plan_payload)
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
        position = {
            "symbol": "MNQ",
            "contract": "MNQM6",
            "quantity": 1,
            "side": "LONG",
            "ownership_id": "submit_owner_test",
            "lifecycle_id": "lifecycle_test",
            "manifest_id": "manifest_test",
        }
        position_payload["positions"] = [position]
        managed_position_payload["managed_positions"] = [position]
    _write_json(root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json", position_payload)
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


def _order_identity() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MGC",
        "contract": "MGCM6",
        "con_id": 123,
        "broker_order_id": "32",
        "perm_id": "perm-1",
        "action": "SELL",
        "quantity": 1,
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
