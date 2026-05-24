from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import HEALTHY, STOPPED_EXPECTED
from mgc_v05l.execution_core.track_b_crash_loop_protection import NO_CRASH_LOOP, RESTART_COOLDOWN_ACTIVE
from mgc_v05l.execution_core.track_b_managed_order_registry import NO_MANAGED_ORDERS
from mgc_v05l.execution_core.track_b_managed_position_registry import NO_MANAGED_POSITIONS
from mgc_v05l.execution_core.track_b_open_order_truth import NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_runtime_resume_semantics import (
    RESUME_POLICY_HOLD_DUPLICATE_WRITER,
    RESUME_POLICY_HOLD_LIVE_MONEY,
    RESUME_POLICY_HOLD_MARKET_CLOSED,
    RESUME_POLICY_HOLD_STALE_EVIDENCE,
    RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED,
    RESUME_POLICY_QUARANTINE_OBSERVE_ONLY,
    RESUME_ALLOWED_CLEAN,
    RESUME_ALLOWED_PAPER_BOUNDED_RETRY,
    RESUME_BLOCKED_BROKER_EXPOSURE,
    RESUME_BLOCKED_CRASH_LOOP,
    RESUME_BLOCKED_HARD_UNSAFE,
    RESUME_BLOCKED_MANAGED_POSITION,
    RESUME_BLOCKED_MARKET_CLOSED,
    RESUME_BLOCKED_OPEN_ORDER,
    RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY,
    RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE,
    RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE,
    TrackBRuntimeResumeSemanticsConfig,
    build_dashboard_runtime_resume_projection,
    build_track_b_runtime_resume_semantics,
    write_track_b_runtime_resume_semantics,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_clean_proof_ready_allows_clean_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_ALLOWED_CLEAN
    assert payload["allowed"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["safe_to_reuse_previous_runtime_state"] is False
    assert payload["must_start_new_runtime_generation"] is True
    assert payload["resume_semantics_version"] == "v2"
    assert payload["resume_action_policy"] == RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED
    assert payload["generation_reuse_allowed"] is False
    assert payload["must_start_new_generation"] is True
    assert payload["previous_runtime_generation_id"] == "runtime-generation-previous"
    assert payload["proposed_next_runtime_generation_id"] == "track-b-paper-runtime-generation-20260523T120000Z"
    assert payload["previous_runtime_instance_id"] == "track-b-paper-runtime-test"
    assert payload["previous_source_commit"] == "test-previous-source-commit"
    assert payload["previous_control_plane_snapshot_id"] == "test-control-plane-snapshot"
    assert payload["bounded_retry_budget_key"]
    assert payload["attempts_remaining"] == 2
    assert payload["read_only"] is True
    assert payload["runtime_restart_authority"] is False
    assert payload["broker_mutation"] is False


def test_market_closed_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, proof_classification=MARKET_CLOSED_NO_FRESH_BARS, phase1_reason=MARKET_CLOSED_NO_FRESH_BARS)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_MARKET_CLOSED
    assert payload["allowed"] is False
    assert payload["reason"] == MARKET_CLOSED_NO_FRESH_BARS
    assert payload["resume_action_policy"] == RESUME_POLICY_HOLD_MARKET_CLOSED
    assert payload["required_operator_ack"] is False


def test_active_runtime_blocks_new_resume_but_marks_reuse_safe(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_ACTIVE_TRADE_CAPABLE)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_RUNTIME_ALREADY_ACTIVE
    assert payload["allowed"] is False
    assert payload["safe_to_start_runtime"] is False
    assert payload["safe_to_reuse_previous_runtime_state"] is True
    assert payload["must_start_new_runtime_generation"] is False
    assert payload["resume_action_policy"] == "RESUME_EXISTING_RUNTIME"
    assert payload["generation_reuse_allowed"] is True


def test_broker_exposure_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE, position_classification="ATTENTION_REQUIRED")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_BROKER_EXPOSURE
    assert payload["allowed"] is False
    assert payload["safe_to_start_runtime"] is False


def test_open_order_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, open_order_classification="SUSPICIOUS_ORDER_STATE")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_OPEN_ORDER
    assert payload["blockers"][0]["code"] == "open_order_truth"


def test_managed_position_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, managed_position_classification="OPEN_MANAGED_MATCHED")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_MANAGED_POSITION
    assert payload["blockers"][0]["code"] == "managed_position_registry"


def test_crash_loop_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, crash_loop_classification=RESTART_COOLDOWN_ACTIVE, crash_loop_restart_blocked=True)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_CRASH_LOOP
    assert payload["allowed"] is False
    assert payload["safe_to_start_runtime"] is False


def test_prior_unsafe_stop_clean_truth_uses_paper_bounded_retry_not_operator_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path, previous_broker_safe_at_stop=False)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_ALLOWED_PAPER_BOUNDED_RETRY
    assert payload["required_operator_ack"] is False
    assert payload["allowed"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["previous_broker_safe_at_stop"] is False
    assert payload["evidence"]["paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["resume_action_policy"] == RESUME_POLICY_NEW_RUNTIME_GENERATION_ALLOWED
    assert payload["enhanced_observation_required"] is True
    assert payload["must_start_new_generation"] is True


def test_crash_loop_with_paper_quarantine_blocks_without_operator_ack(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        crash_loop_classification=RESTART_COOLDOWN_ACTIVE,
        crash_loop_restart_blocked=True,
        paper_action_policy="QUARANTINE_OBSERVE_ONLY",
        paper_autonomous_recovery_allowed=False,
    )

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY
    assert payload["required_operator_ack"] is False
    assert payload["allowed"] is False
    assert payload["resume_mode"] == "paper_quarantine_observe_only"
    assert payload["resume_action_policy"] == RESUME_POLICY_QUARANTINE_OBSERVE_ONLY


def test_legacy_operator_ack_classification_is_not_paper_ack_dependency(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        crash_loop_classification="OPERATOR_ACK_REQUIRED",
        crash_loop_restart_blocked=True,
        paper_action_policy="OBSERVE",
        paper_autonomous_recovery_allowed=False,
    )

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY
    assert payload["required_operator_ack"] is False
    assert payload["resume_mode"] == "paper_quarantine_observe_only"


def test_live_money_policy_hard_unsafe_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True, paper_action_policy="HARD_UNSAFE_HOLD")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_HARD_UNSAFE
    assert payload["required_operator_ack"] is False
    assert payload["safe_to_start_runtime"] is False
    assert payload["resume_action_policy"] == RESUME_POLICY_HOLD_LIVE_MONEY


def test_duplicate_writer_hard_unsafe_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification="DUPLICATE_RUNTIME_WRITERS", duplicate_writer_count=1)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_HARD_UNSAFE
    assert payload["blockers"][0]["code"] == "runtime_environment_truth"
    assert payload["resume_action_policy"] == RESUME_POLICY_HOLD_DUPLICATE_WRITER


def test_budget_exhausted_quarantines_observe_only(tmp_path: Path) -> None:
    _seed_base(tmp_path, budget_exhausted=True)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_PAPER_QUARANTINE_OBSERVE_ONLY
    assert payload["resume_action_policy"] == RESUME_POLICY_QUARANTINE_OBSERVE_ONLY
    assert payload["attempts_remaining"] == 0
    assert payload["cooldown_until"]
    assert payload["required_operator_ack"] is False


def test_stale_control_plane_snapshot_holds_stale_evidence(tmp_path: Path) -> None:
    _seed_base(tmp_path, control_plane_generated_at="2026-05-23T11:50:00+00:00")

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE
    assert payload["resume_action_policy"] == RESUME_POLICY_HOLD_STALE_EVIDENCE
    assert payload["blockers"][0]["code"] == "control_plane_snapshot"


def test_missing_evidence_blocks_resume(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_agent_health=False)

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE
    assert payload["allowed"] is False
    assert any("agent_health" in blocker["detail"] for blocker in payload["blockers"])


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path)
    payload = build_track_b_runtime_resume_semantics(config=config, now=NOW)

    authority_path = write_track_b_runtime_resume_semantics(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    direct_projection = build_dashboard_runtime_resume_projection(authority_payload=payload, authority_path=authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True


def test_control_plane_dashboard_projection_is_not_consumed_as_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    (tmp_path / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json").unlink()
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_track_b_control_plane_snapshot.json",
        {
            "projection_only": True,
            "not_routing_authority": True,
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "safe_to_start_runtime": True,
        },
    )

    payload = build_track_b_runtime_resume_semantics(config=TrackBRuntimeResumeSemanticsConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESUME_BLOCKED_STALE_OR_MISSING_EVIDENCE
    assert payload["resume_action_policy"] == RESUME_POLICY_HOLD_STALE_EVIDENCE
    assert payload["safe_to_start_runtime"] is False


def _seed_base(
    root: Path,
    *,
    proof_classification: str = READY_FOR_PROOF,
    phase1_reason: str = "phase1_runtime_candles_ready",
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    position_classification: str = "CLEAN_FLAT_READY",
    open_order_classification: str = NO_OPEN_ORDERS,
    managed_order_classification: str = NO_MANAGED_ORDERS,
    managed_position_classification: str = NO_MANAGED_POSITIONS,
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_lease_classification: str = "ACTIVE",
    crash_loop_classification: str = NO_CRASH_LOOP,
    crash_loop_restart_blocked: bool = False,
    previous_broker_safe_at_stop: bool = True,
    paper_action_policy: str = "AUTONOMOUS_RETRY_ELIGIBLE",
    paper_autonomous_recovery_allowed: bool = True,
    duplicate_writer_count: int = 0,
    live_money_eligible: bool = False,
    include_agent_health: bool = True,
    budget_exhausted: bool = False,
    control_plane_generated_at: str | None = None,
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json",
        {"generated_at": NOW.isoformat(), "classification": proof_classification, "phase1_session_reason": phase1_reason},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json",
        {
            "generated_at": NOW.isoformat(),
            "classifications": {
                "Open Order Truth": open_order_classification,
                "Managed Order Registry": managed_order_classification,
                "Position Truth": position_classification,
                "Runtime Environment Truth": runtime_classification,
                "Managed Position Registry": managed_position_classification,
                "Reconciliation": reconciliation_classification,
                "Broker Truth Lease": broker_lease_classification,
            },
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": runtime_classification,
            "writer_authority": "DUPLICATE_WRITER" if duplicate_writer_count else "NO_ACTIVE_WRITER",
            "duplicate_writer_count": duplicate_writer_count,
            "live_money_eligible": live_money_eligible,
            "runtime_generation_id": "runtime-generation-previous",
            "source_commit": "test-previous-source-commit",
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
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {"generated_at": NOW.isoformat(), "classification": "RESTART_RUNTIME_ALLOWED", "recommendation": "RESTART_RUNTIME_ALLOWED"},
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
            "runtime_instance_id": "track-b-paper-runtime-test",
            "runtime_generation_id": "runtime-generation-previous",
            "source_commit": "test-previous-source-commit",
            "control_plane_snapshot_id": "test-control-plane-snapshot",
            "broker_safe_at_stop": previous_broker_safe_at_stop,
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
                "budget_exhausted": budget_exhausted or paper_action_policy == "QUARANTINE_OBSERVE_ONLY",
            },
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "recovery_budget" / "latest_recovery_budget_ledger.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "RECOVERY_BUDGET_EXHAUSTED" if budget_exhausted else "RECOVERY_BUDGET_AVAILABLE",
            "max_attempts_per_target": 2,
            "budget_exhausted": budget_exhausted,
            "quarantine_required": budget_exhausted,
            "entries": [
                {
                    "budget_key": "track_b_paper_runtime|RUNTIME_RETRY|44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a|*|runtime_retry",
                    "agent_id": "track_b_paper_runtime",
                    "action_type": "RUNTIME_RETRY",
                    "target_identity": {},
                    "target_identity_hash": "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                    "failure_classification": "runtime_retry",
                    "attempts_used": 2 if budget_exhausted else 0,
                    "attempts_remaining": 0 if budget_exhausted else 2,
                    "budget_exhausted": budget_exhausted,
                    "quarantine_required": budget_exhausted,
                    "cooldown_until": "2026-05-23T12:15:00+00:00" if budget_exhausted else None,
                }
            ],
            "summary": {
                "budget_exhausted": budget_exhausted,
                "quarantine_required": budget_exhausted,
                "minimum_attempts_remaining": 0 if budget_exhausted else 2,
            },
            "live_money_eligible": live_money_eligible,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json",
        {
            "generated_at": control_plane_generated_at or NOW.isoformat(),
            "control_plane_snapshot_id": "test-control-plane-snapshot",
            "classification": "CONTROL_PLANE_SNAPSHOT_READY",
            "shared_truth_coherence_status": "COHERENT",
            "shared_truth_refresh_generation_id": "test-shared-truth-generation",
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED"
            if proof_classification == READY_FOR_PROOF and not duplicate_writer_count and not live_money_eligible
            else "SUPERVISOR_WAIT_MARKET_CLOSED"
            if proof_classification == MARKET_CLOSED_NO_FRESH_BARS
            else "SUPERVISOR_RUNTIME_START_BLOCKED",
            "supervisor_mode": "READY_FOR_OPERATOR_START"
            if proof_classification == READY_FOR_PROOF and not duplicate_writer_count and not live_money_eligible
            else "MARKET_CLOSED_WAIT"
            if proof_classification == MARKET_CLOSED_NO_FRESH_BARS
            else "MANUAL_REVIEW_REQUIRED",
            "proof_window_status": "ready" if proof_classification == READY_FOR_PROOF else "market_closed",
            "safe_to_start_runtime": proof_classification == READY_FOR_PROOF and not duplicate_writer_count and not live_money_eligible,
            "blockers": [],
            "live_money_eligible": live_money_eligible,
        },
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
