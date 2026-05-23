from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_registry import (
    AGENT_REGISTRY_INCOMPLETE,
    AGENT_REGISTRY_READY,
    DEFAULT_DASHBOARD_AGENT_REGISTRY_PROJECTION,
    REQUIRED_PROOF_AGENT_IDS,
    TrackBAgentRegistryConfig,
    build_track_b_agent_registry,
    write_track_b_agent_registry,
)


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_registry_includes_all_required_proof_agents(tmp_path: Path) -> None:
    payload = build_track_b_agent_registry(config=TrackBAgentRegistryConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == AGENT_REGISTRY_READY
    agent_ids = {agent["agent_id"] for agent in payload["agents"]}
    assert REQUIRED_PROOF_AGENT_IDS <= agent_ids
    assert payload["read_only"] is True
    assert payload["submit_authority"] is False
    assert payload["broker_mutation"] is False
    assert payload["lifecycle_mutation"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False


def test_runtime_agent_marked_required_for_proof(tmp_path: Path) -> None:
    payload = build_track_b_agent_registry(config=TrackBAgentRegistryConfig(repo_root=tmp_path), now=NOW)

    runtime = _agent(payload, "track_b_paper_runtime")
    assert runtime["category"] == "runtime"
    assert runtime["required_for_proof"] is True
    assert runtime["required_for_runtime_submit"] is True
    assert runtime["projection_only"] is False
    assert runtime["owner_root_expectation"] == "Dev root"
    assert "phase1_databento_live_candles" in runtime["dependencies"]


def test_phase1_producer_marked_required_for_proof(tmp_path: Path) -> None:
    payload = build_track_b_agent_registry(config=TrackBAgentRegistryConfig(repo_root=tmp_path), now=NOW)

    phase1 = _agent(payload, "phase1_databento_live_candles")
    assert phase1["category"] == "market_data"
    assert phase1["required_for_proof"] is True
    assert phase1["required_for_runtime_submit"] is True
    assert phase1["projection_only"] is False
    assert any("phase1_runtime_market_data/MGC/1m" in path for path in phase1["expected_artifact_paths"])
    assert any("phase1_runtime_market_data/MNQ/5m" in path for path in phase1["expected_artifact_paths"])


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    config = TrackBAgentRegistryConfig(repo_root=tmp_path)
    payload = build_track_b_agent_registry(config=config, now=NOW)

    authority_path = write_track_b_agent_registry(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "agent_registry" / "latest_agent_registry.json"
    assert authority_path.exists()
    assert projection_path == tmp_path / DEFAULT_DASHBOARD_AGENT_REGISTRY_PROJECTION
    assert projection_path.exists()
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert projection["authority_owner"] == "execution_core"


def test_missing_expected_agent_marks_registry_incomplete(tmp_path: Path) -> None:
    config = TrackBAgentRegistryConfig(repo_root=tmp_path, expected_proof_agent_ids=frozenset({"missing_agent"}))

    payload = build_track_b_agent_registry(config=config, now=NOW)

    assert payload["classification"] == AGENT_REGISTRY_INCOMPLETE
    assert payload["blockers"] == [{"code": "missing_required_proof_agents", "agent_ids": ["missing_agent"]}]


def test_no_dashboard_projection_consumed_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_agent_registry.json"
    critical_paths = [
        repo_root / "src/mgc_v05l/app/probationary_runtime.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_runtime_environment_truth.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_position_truth_monitor.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_paper_broker_reconciliation.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_readiness_state.py",
        repo_root / "src/mgc_v05l/execution_core/track_b_self_healing_supervisor.py",
    ]

    offenders = [str(path) for path in critical_paths if forbidden in path.read_text(encoding="utf-8")]

    assert offenders == []


def _agent(payload: dict, agent_id: str) -> dict:
    for agent in payload["agents"]:
        if agent["agent_id"] == agent_id:
            return agent
    raise AssertionError(f"Missing agent {agent_id}")
