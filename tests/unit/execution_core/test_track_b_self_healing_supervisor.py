from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.execution_core.track_b_self_healing_supervisor import (
    DEFAULT_SELF_HEALING_HEALTH_ARTIFACT,
    build_track_b_self_healing_agent_registry,
    classify_track_b_self_healing_health,
)


NOW = datetime(2026, 5, 21, 3, 45, tzinfo=timezone.utc)
DEV_ROOT = "/Users/patrick/Dev/MGC-v05l-automation"
RECONCILED = "TRACK_B_PAPER_BROKER_RECONCILED"


def test_registry_defines_expected_track_b_agents() -> None:
    registry = build_track_b_self_healing_agent_registry()
    agent_ids = {row["agent_id"] for row in registry}

    assert {
        "phase1_candle_supervisor",
        "broker_truth_refresher",
        "operator_readiness_refresher",
        "paper_runtime",
        "operator_dashboard_backend",
    } <= agent_ids
    paper_runtime = next(row for row in registry if row["agent_id"] == "paper_runtime")
    assert paper_runtime["required"] is True
    assert paper_runtime["restart_eligible"] is False
    assert "runtime_restart_requires_explicit_operator_approval" in paper_runtime["restart_blockers"]


def test_healthy_required_agents_are_self_healing_ready() -> None:
    result = classify_track_b_self_healing_health(_inputs())

    assert result["classification"] == "SELF_HEALING_READY"
    assert result["auto_restart_allowed"] is False
    assert result["blockers"] == ()
    assert result["restart_candidates"] == ()
    assert result["health_contract_artifact_path"] == str(DEFAULT_SELF_HEALING_HEALTH_ARTIFACT)


def test_dead_required_sidecar_is_auto_restart_eligible_when_broker_state_is_safe() -> None:
    inputs = _inputs()
    inputs["agents"]["broker_truth_refresher"]["process_running"] = False

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "AUTO_RESTART_ELIGIBLE"
    assert result["auto_restart_allowed"] is True
    assert result["restart_candidates"] == ("broker_truth_refresher",)
    assert "broker_truth_refresher_not_running" in result["agents"]["broker_truth_refresher"]["blockers"]


def test_unknown_open_orders_block_auto_restart() -> None:
    inputs = _inputs()
    inputs["agents"]["broker_truth_refresher"]["process_running"] = False
    inputs["broker_safety"]["unknown_open_order_count"] = 1
    inputs["broker_safety"]["classification"] = "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    inputs["broker_safety"]["broker_reconciled"] = False

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "UNSAFE_BROKER_STATE"
    assert result["auto_restart_allowed"] is False
    assert "unknown_open_orders" in result["blockers"]
    assert "broker_reconciliation_mismatch" in result["blockers"]
    assert result["restart_candidates"] == ("broker_truth_refresher",)


def test_live_money_blocks_restart_even_with_sidecar_candidate() -> None:
    inputs = _inputs()
    inputs["agents"]["operator_readiness_refresher"]["process_running"] = False
    inputs["broker_safety"]["live_money_eligible"] = True

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "AUTO_RESTART_BLOCKED"
    assert result["auto_restart_allowed"] is False
    assert "live_money_eligible_true" in result["blockers"]


def test_wrong_root_blocks_restart() -> None:
    inputs = _inputs()
    inputs["agents"]["phase1_candle_supervisor"]["process_running"] = False
    inputs["agents"]["phase1_candle_supervisor"]["root_ok"] = False

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "AUTO_RESTART_BLOCKED"
    assert result["auto_restart_allowed"] is False
    assert "wrong_root" in result["blockers"]


def test_runtime_degraded_requires_operator_approval_not_auto_restart() -> None:
    inputs = _inputs()
    inputs["agents"]["paper_runtime"]["process_running"] = False

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "DEGRADED_RECOVERABLE"
    assert result["auto_restart_allowed"] is False
    assert result["restart_candidates"] == ()
    assert "paper_runtime_restart_not_eligible" in result["agents"]["paper_runtime"]["blockers"]


def test_operator_required_state_wins_over_restart_candidate() -> None:
    inputs = _inputs()
    inputs["agents"]["broker_truth_refresher"]["classification"] = "BROKER_TRUTH_REFRESH_OPERATOR_REQUIRED"

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "OPERATOR_REQUIRED"
    assert result["operator_required_agents"] == ("broker_truth_refresher",)
    assert result["auto_restart_allowed"] is False


def _inputs() -> dict:
    return {
        "generated_at": NOW.isoformat(),
        "expected_root": DEV_ROOT,
        "agents": {
            "phase1_candle_supervisor": _agent(),
            "broker_truth_refresher": _agent(),
            "operator_readiness_refresher": _agent(),
            "paper_runtime": _agent(),
            "operator_dashboard_backend": _agent(required=False),
        },
        "broker_safety": {
            "classification": RECONCILED,
            "broker_reconciled": True,
            "unknown_open_order_count": 0,
            "track_b_broker_open_order_count": 0,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "live_money_eligible": False,
            "duplicate_conflicting_runtime_count": 0,
        },
    }


def _agent(*, required: bool = True) -> dict:
    return {
        "process_running": True,
        "root_ok": True,
        "command_ok": True,
        "classification": "HEALTHY",
        "operator_required": False,
        "artifacts": (
            {
                "label": "status",
                "present": True,
                "required": required,
                "fresh": True,
                "age_seconds": 10.0,
                "freshness_ttl_seconds": 180.0,
            },
        ),
        "live_money_eligible": False,
    }
