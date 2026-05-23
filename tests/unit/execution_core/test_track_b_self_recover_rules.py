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


def test_market_closed_waits_instead_of_restarting(tmp_path: Path) -> None:
    _seed_base(tmp_path, proof_classification=MARKET_CLOSED_NO_FRESH_BARS, phase1_reason=MARKET_CLOSED_NO_FRESH_BARS)

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == WAIT_MARKET_CLOSED
    assert payload["allowed"] is True


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


def test_phase1_down_open_market_allows_market_data_restart(tmp_path: Path) -> None:
    _seed_base(tmp_path, proof_classification="PHASE1_DATA_UNHEALTHY", phase1_status=MISSING, phase1_reason="phase1_readiness_missing")

    payload = build_track_b_self_recover_rules(config=TrackBSelfRecoverRulesConfig(repo_root=tmp_path), now=NOW)

    assert payload["recommendation"] == RESTART_MARKET_DATA_PRODUCER_ALLOWED
    assert payload["allowed"] is True


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBSelfRecoverRulesConfig(repo_root=tmp_path)
    payload = build_track_b_self_recover_rules(config=config, now=NOW)

    authority_path = write_track_b_self_recover_rules(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)


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
    if stale_truth_agent:
        for agent in agents:
            if agent["agent_id"] == stale_truth_agent:
                agent["status"] = STALE
                agent["blocking_for_proof"] = True
                agent["blocking_for_runtime_submit"] = True
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
