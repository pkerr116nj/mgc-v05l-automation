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
    RESUME_ALLOWED_CLEAN,
    RESUME_BLOCKED_MARKET_CLOSED,
    RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME,
    SUPERVISOR_MANUAL_REVIEW_REQUIRED,
    SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP,
    SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK,
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
        resume_reason=MARKET_CLOSED_NO_FRESH_BARS,
        self_recover_recommendation="WAIT_MARKET_CLOSED",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_WAIT_MARKET_CLOSED
    assert payload["recommended_action"] == "WAIT_MARKET_CLOSED"
    assert payload["action_allowed"] is True
    assert payload["safe_to_start_runtime"] is False


def test_clean_proof_ready_allows_runtime_start(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_START_ALLOWED
    assert payload["recommended_action"] == "START_RUNTIME_ALLOWED"
    assert payload["action_allowed"] is True
    assert payload["safe_to_start_runtime"] is True
    assert payload["runtime_restart_authority"] is False
    assert payload["broker_mutation"] is False


def test_active_healthy_runtime_is_left_running(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_ACTIVE_TRADE_CAPABLE)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RUNTIME_ALREADY_HEALTHY
    assert payload["recommended_action"] == "LEAVE_RUNTIME_RUNNING"
    assert payload["safe_to_leave_runtime_running"] is True
    assert payload["safe_to_start_runtime"] is False


def test_broker_exposure_requires_cleanup(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE, position_classification="ATTENTION_REQUIRED")

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_CLEANUP_REQUIRED_BEFORE_RUNTIME
    assert payload["recommended_action"] == "CLEANUP_REQUIRED_BEFORE_RUNTIME"
    assert payload["safe_to_start_runtime"] is False


def test_suspicious_order_requires_manual_review(tmp_path: Path) -> None:
    _seed_base(tmp_path, open_order_classification="SUSPICIOUS_ORDER_STATE", managed_order_classification="CLOSE_ORDER_SUSPICIOUS")

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_MANUAL_REVIEW_REQUIRED
    assert payload["recommended_action"] == "MANUAL_REVIEW_REQUIRED"
    assert payload["safe_to_start_runtime"] is False


def test_crash_loop_blocks_restart(tmp_path: Path) -> None:
    _seed_base(tmp_path, crash_loop_classification=RESTART_COOLDOWN_ACTIVE, crash_loop_restart_blocked=True)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RESTART_BLOCKED_CRASH_LOOP
    assert payload["recommended_action"] == "HOLD_DOWN_CRASH_LOOP"
    assert payload["action_allowed"] is False


def test_operator_ack_required_blocks_restart(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        resume_classification=RESUME_BLOCKED_OPERATOR_ACK_REQUIRED,
        resume_required_operator_ack=True,
        resume_reason="Prior unsafe stop requires acknowledgement.",
    )

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_RESTART_BLOCKED_OPERATOR_ACK
    assert payload["operator_ack_required"] is True
    assert payload["recommended_action"] == "HOLD_DOWN_OPERATOR_ACK_REQUIRED"


def test_stale_evidence_blocks_supervisor_action(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_agent_health=False)

    payload = build_track_b_runtime_supervisor_authority(config=TrackBRuntimeSupervisorAuthorityConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == SUPERVISOR_SHARED_TRUTH_STALE
    assert payload["recommended_action"] == "REFRESH_SHARED_TRUTH"
    assert any("agent_health" in blocker["detail"] for blocker in payload["blockers"])


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


def _seed_base(
    root: Path,
    *,
    proof_classification: str = READY_FOR_PROOF,
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    resume_classification: str = RESUME_ALLOWED_CLEAN,
    resume_reason: str = "Clean flat shared truth and proof readiness is READY_FOR_PROOF.",
    resume_required_operator_ack: bool = False,
    position_classification: str = "CLEAN_FLAT_READY",
    open_order_classification: str = NO_OPEN_ORDERS,
    managed_order_classification: str = NO_MANAGED_ORDERS,
    managed_position_classification: str = NO_MANAGED_POSITIONS,
    reconciliation_classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_lease_classification: str = "ACTIVE",
    crash_loop_classification: str = NO_CRASH_LOOP,
    crash_loop_restart_blocked: bool = False,
    self_recover_recommendation: str = "RESTART_RUNTIME_ALLOWED",
    include_agent_health: bool = True,
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {"generated_at": NOW.isoformat(), "classification": runtime_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_resume" / "latest_runtime_resume_semantics.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": resume_classification,
            "allowed": resume_classification == RESUME_ALLOWED_CLEAN,
            "safe_to_start_runtime": resume_classification == RESUME_ALLOWED_CLEAN,
            "required_operator_ack": resume_required_operator_ack,
            "reason": resume_reason,
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {"generated_at": NOW.isoformat(), "classification": self_recover_recommendation, "recommendation": self_recover_recommendation},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": crash_loop_classification,
            "restart_blocked": crash_loop_restart_blocked,
            "operator_ack_required": False,
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
        {"generated_at": NOW.isoformat(), "classification": proof_classification, "phase1_session_reason": proof_classification},
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
        root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json",
        {"generated_at": NOW.isoformat(), "canonical_readiness": "READY_SUBMIT_CAPABLE"},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": position_classification,
            "summary": {"overall_classification": position_classification},
        },
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json",
        {"generated_at": NOW.isoformat(), "classification": open_order_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json",
        {"generated_at": NOW.isoformat(), "classification": managed_order_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json",
        {"generated_at": NOW.isoformat(), "classification": managed_position_classification},
    )
    _write_json(
        root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": NOW.isoformat(), "classification": reconciliation_classification},
    )
    _write_json(
        root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": NOW.isoformat(), "classification": broker_lease_classification},
    )
    _write_json(
        root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "latest_runtime_stop_provenance.json",
        {"generated_at": NOW.isoformat(), "stop_source": "launcher", "stop_reason": "expected_clean_down"},
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
