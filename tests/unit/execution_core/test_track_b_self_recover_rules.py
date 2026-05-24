from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import HEALTHY, MISSING, STALE, STOPPED_EXPECTED
from mgc_v05l.execution_core.track_b_managed_order_registry import NO_MANAGED_ORDERS
from mgc_v05l.execution_core.track_b_managed_position_registry import NO_MANAGED_POSITIONS
from mgc_v05l.execution_core.track_b_open_order_truth import NO_OPEN_ORDERS
from mgc_v05l.execution_core.track_b_paper_proof_readiness import READY_FOR_PROOF
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
)
from mgc_v05l.execution_core.track_b_self_recover_rules import (
    CLEANUP_REQUIRED_BEFORE_RESTART,
    MANUAL_TWS_REVIEW_REQUIRED,
    REFRESH_SHARED_TRUTH,
    RESTART_MARKET_DATA_PRODUCER_ALLOWED,
    RESTART_RUNTIME_ALLOWED,
    STRUCTURED_HARD_UNSAFE_HOLD,
    STRUCTURED_MARKET_DATA_RESTART_DRY_RUN,
    STRUCTURED_QUARANTINE_OBSERVE_ONLY,
    STRUCTURED_REFRESH_EVIDENCE,
    STRUCTURED_RUNTIME_RETRY_DRY_RUN,
    STRUCTURED_WAIT_MARKET_CLOSED,
    WAIT_MARKET_CLOSED,
    CLEAN_FLAT_READY,
    TrackBSelfRecoverRulesConfig,
    build_track_b_self_recover_rules,
    write_track_b_self_recover_rules,
)
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_clean_runtime_down_proof_ready_allows_runtime_restart(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == RESTART_RUNTIME_ALLOWED
    assert payload["allowed"] is True
    assert payload["read_only"] is True
    assert payload["runtime_restart_authority"] is False
    assert payload["broker_mutation"] is False
    assert payload["self_recover_schema_version"] == "v2"
    assert payload["control_plane_snapshot_id"] == "snapshot-1"
    assert payload["shared_truth_generation_id"] == "generation-1"
    assert payload["paper_action_policy"] == "AUTONOMOUS_RETRY_ELIGIBLE"
    assert payload["autonomous_recovery_plan_classification"] == "PLAN_RUNTIME_RETRY"
    assert payload["recommended_recovery_action"] == STRUCTURED_RUNTIME_RETRY_DRY_RUN
    assert payload["attempts_remaining"] == 2
    assert payload["execution_enabled"] is False


def test_market_closed_waits_instead_of_restarting(tmp_path: Path) -> None:
    _seed_base(tmp_path, proof_classification=MARKET_CLOSED_NO_FRESH_BARS, phase1_reason=MARKET_CLOSED_NO_FRESH_BARS)

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == WAIT_MARKET_CLOSED
    assert payload["allowed"] is True
    assert payload["recommended_recovery_action"] == STRUCTURED_WAIT_MARKET_CLOSED
    assert payload["paper_action_policy"] == "OBSERVE"


def test_broker_exposure_requires_cleanup_before_restart(tmp_path: Path) -> None:
    _seed_base(tmp_path, runtime_classification=RUNTIME_DOWN_WITH_BROKER_EXPOSURE, position_classification="ATTENTION_REQUIRED")

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == CLEANUP_REQUIRED_BEFORE_RESTART
    assert payload["allowed"] is False


def test_suspicious_order_requires_manual_tws_review(tmp_path: Path) -> None:
    _seed_base(tmp_path, open_order_classification="SUSPICIOUS_ORDER_STATE")

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == MANUAL_TWS_REVIEW_REQUIRED
    assert payload["allowed"] is False


def test_stale_truth_services_recommend_shared_truth_refresh(tmp_path: Path) -> None:
    _seed_base(tmp_path, stale_truth_agent="open_order_truth")

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == REFRESH_SHARED_TRUTH
    assert payload["allowed"] is True
    assert payload["evidence"]["stale_truth_agents"] == ["open_order_truth"]
    assert payload["recommended_recovery_action"] == STRUCTURED_REFRESH_EVIDENCE
    assert payload["agent_health_top_blockers"][0]["agent_id"] == "open_order_truth"


def test_phase1_down_open_market_allows_market_data_restart(tmp_path: Path) -> None:
    _seed_base(tmp_path, proof_classification="PHASE1_DATA_UNHEALTHY", phase1_status=MISSING, phase1_reason="phase1_readiness_missing")

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == RESTART_MARKET_DATA_PRODUCER_ALLOWED
    assert payload["allowed"] is True
    assert payload["recommended_recovery_action"] == STRUCTURED_MARKET_DATA_RESTART_DRY_RUN


def test_budget_exhausted_quarantines_without_paper_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path, paper_action_policy="QUARANTINE_OBSERVE_ONLY", budget_attempts_remaining=0, budget_exhausted=True)

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == RESTART_RUNTIME_ALLOWED
    assert payload["recommended_recovery_action"] == STRUCTURED_QUARANTINE_OBSERVE_ONLY
    assert payload["quarantine_required"] is True
    assert "operator acknowledgement" in payload["operator_explanation"]


def test_duplicate_writer_is_hard_unsafe_hold(tmp_path: Path) -> None:
    _seed_base(tmp_path, duplicate_writer=True)

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommended_recovery_action"] == STRUCTURED_HARD_UNSAFE_HOLD
    assert payload["paper_action_policy"] == "HARD_UNSAFE_HOLD"
    assert payload["agent_health_top_blockers"][0]["agent_id"] == "track_b_paper_runtime"


def test_live_money_is_hard_unsafe_hold(tmp_path: Path) -> None:
    _seed_base(tmp_path, live_money_eligible=True)

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommended_recovery_action"] == STRUCTURED_HARD_UNSAFE_HOLD
    assert payload["paper_action_policy"] == "HARD_UNSAFE_HOLD"


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path, include_control_plane_snapshot=False)
    _write_json(
        tmp_path / "outputs" / "operator_dashboard" / "runtime" / "latest_track_b_control_plane_snapshot.json",
        {
            "projection_only": True,
            "not_routing_authority": True,
            "control_plane_snapshot_id": "dashboard-projection-only",
            "shared_truth_refresh_generation_id": "dashboard-generation",
        },
    )
    config = TrackBSelfRecoverRulesConfig(repo_root=tmp_path)
    payload = build_track_b_self_recover_rules(config=config, now=NOW)

    authority_path = write_track_b_self_recover_rules(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert payload["control_plane_snapshot_id"] == ""
    assert payload["shared_truth_generation_id"] == ""


def _seed_base(
    root: Path,
    *,
    runtime_classification: str = RUNTIME_DOWN_CLEAN,
    proof_classification: str = READY_FOR_PROOF,
    position_classification: str = CLEAN_FLAT_READY,
    open_order_classification: str = NO_OPEN_ORDERS,
    managed_order_classification: str = NO_MANAGED_ORDERS,
    managed_position_classification: str = NO_MANAGED_POSITIONS,
    stale_truth_agent: str | None = None,
    phase1_status: str = HEALTHY,
    phase1_reason: str = "phase1_runtime_candles_ready",
    paper_action_policy: str = "AUTONOMOUS_RETRY_ELIGIBLE",
    autonomous_plan_classification: str = "PLAN_RUNTIME_RETRY",
    budget_attempts_remaining: int = 2,
    budget_exhausted: bool = False,
    duplicate_writer: bool = False,
    live_money_eligible: bool = False,
    include_control_plane_snapshot: bool = True,
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "agent_registry" / "latest_agent_registry.json",
        {"generated_at": NOW.isoformat(), "classification": "AGENT_REGISTRY_READY"},
    )
    agents = [
        _health_agent("open_order_truth", "truth_service", HEALTHY),
        _health_agent("managed_order_registry", "truth_service", HEALTHY),
        _health_agent("position_truth", "truth_service", HEALTHY),
        _health_agent("runtime_environment_truth", "truth_service", HEALTHY),
        _health_agent("managed_position_registry", "truth_service", HEALTHY),
        _health_agent("phase1_databento_live_candles", "market_data", phase1_status, reason=phase1_reason),
        _health_agent("track_b_paper_runtime", "runtime", STOPPED_EXPECTED, reason=runtime_classification),
    ]
    if duplicate_writer:
        agents[-1]["status"] = "DUPLICATE_PROCESS"
        agents[-1]["reason"] = "duplicate runtime writer detected"
        agents[-1]["blocking_for_proof"] = True
        agents[-1]["blocking_for_runtime_submit"] = True
        agents[-1]["blocking_for_recovery"] = True
    if stale_truth_agent:
        for agent in agents:
            if agent["agent_id"] == stale_truth_agent:
                agent["status"] = STALE
                agent["blocking_for_proof"] = True
                agent["blocking_for_runtime_submit"] = True
                agent["blocking_for_recovery"] = True
    _write_json(
        root / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json",
        {"generated_at": NOW.isoformat(), "classification": "AGENT_HEALTH_READY", "agents": agents},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "proof_readiness" / "latest_track_b_paper_proof_readiness.json",
        {"generated_at": NOW.isoformat(), "classification": proof_classification, "phase1_session_reason": phase1_reason},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {"generated_at": NOW.isoformat(), "classification": runtime_classification},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json",
        {"generated_at": NOW.isoformat(), "classification": position_classification, "summary": {"overall_classification": position_classification}},
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
        root / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json",
        {"generated_at": NOW.isoformat(), "classification": "TRACK_B_PAPER_BROKER_RECONCILED"},
    )
    _write_json(root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json", {"generated_at": NOW.isoformat(), "classification": "ACTIVE"})
    if include_control_plane_snapshot:
        _write_json(
            root / "outputs" / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json",
            {
                "generated_at": NOW.isoformat(),
                "control_plane_snapshot_id": "snapshot-1",
                "shared_truth_refresh_generation_id": "generation-1",
                "paper_action_policy": "HARD_UNSAFE_HOLD"
                if (duplicate_writer or live_money_eligible)
                else paper_action_policy,
                "agent_health_has_duplicate_writer": duplicate_writer,
                "duplicate_writer_count": 1 if duplicate_writer else 0,
                "live_money_eligible": live_money_eligible,
                "agent_health_top_blockers": [agents[-1]] if duplicate_writer else [],
            },
        )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "paper_recovery_policy" / "latest_paper_recovery_policy.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": "HARD_UNSAFE_HOLD" if live_money_eligible else paper_action_policy,
            "paper_action_policy": "HARD_UNSAFE_HOLD" if live_money_eligible else paper_action_policy,
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
            "operator_explanation": "",
            "execution_enabled": False,
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
                    "budget_key": "track_b_paper_runtime|RUNTIME_RETRY|test",
                    "agent_id": "track_b_paper_runtime",
                    "action_type": "RUNTIME_RETRY",
                    "attempts_remaining": budget_attempts_remaining,
                    "cooldown_until": None,
                    "quarantine_required": budget_exhausted,
                    "budget_exhausted": budget_exhausted,
                }
            ],
            "summary": {
                "minimum_attempts_remaining": budget_attempts_remaining,
                "budget_exhausted": budget_exhausted,
                "quarantine_required": budget_exhausted,
            },
        },
    )


def _health_agent(agent_id: str, category: str, status: str, *, reason: str = "healthy") -> dict:
    return {
        "agent_id": agent_id,
        "category": category,
        "status": status,
        "reason": reason,
        "blocking_for_proof": False,
        "blocking_for_runtime_submit": False,
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
