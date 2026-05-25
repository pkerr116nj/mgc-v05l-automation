from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import (
    AGENT_HEALTH_BLOCKING,
    AGENT_HEALTH_DEGRADED,
    AGENT_HEALTH_READY,
    DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION,
    DUPLICATE_PROCESS,
    DEGRADED,
    HEALTHY,
    MISSING_ARTIFACT,
    ROOT_MISMATCH,
    SOURCE_COMMIT_MISMATCH,
    STALE,
    STOPPED_EXPECTED,
    TrackBAgentHealthConfig,
    build_track_b_agent_health,
    write_track_b_agent_health,
)
from mgc_v05l.execution_core.track_b_agent_registry import (
    TrackBAgentRegistryConfig,
    build_track_b_agent_registry,
    write_track_b_agent_registry,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import RUNTIME_DOWN_CLEAN, RUNTIME_DOWN_WITH_BROKER_EXPOSURE
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_clean_flat_runtime_down_is_expected_and_not_blocking(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)

    payload = build_track_b_agent_health(config=TrackBAgentHealthConfig(repo_root=tmp_path), now=NOW, process_rows=[])

    runtime = _agent(payload, "track_b_paper_runtime")
    assert payload["classification"] == AGENT_HEALTH_READY
    assert runtime["status"] == STOPPED_EXPECTED
    assert runtime["probe_status"] == STOPPED_EXPECTED
    assert runtime["probe_reason"] == RUNTIME_DOWN_CLEAN
    assert runtime["reason"] == RUNTIME_DOWN_CLEAN
    assert runtime["artifact_fresh"] is True
    assert runtime["heartbeat_fresh"] is True
    assert runtime["process_alive"] is None
    assert runtime["root_matches"] is None
    assert runtime["source_commit_matches"] is None
    assert runtime["runtime_generation_id"] is None
    assert runtime["shared_truth_generation_id"] is None
    assert runtime["control_plane_snapshot_id"] is None
    assert runtime["blocking_for_proof"] is False
    assert runtime["blocking_for_runtime_submit"] is False
    assert runtime["blocking_for_recovery"] is False


def test_missing_required_artifact_blocks(tmp_path: Path) -> None:
    registry = _seed_healthy_artifacts(tmp_path)
    open_order_agent = _agent(registry, "open_order_truth")
    Path(open_order_agent["heartbeat_artifact_path"]).unlink()

    payload = build_track_b_agent_health(config=TrackBAgentHealthConfig(repo_root=tmp_path), now=NOW, process_rows=[])

    open_order = _agent(payload, "open_order_truth")
    assert payload["classification"] == AGENT_HEALTH_BLOCKING
    assert open_order["status"] == MISSING_ARTIFACT
    assert open_order["blocking_for_proof"] is True
    assert open_order["blocking_for_runtime_submit"] is True


def test_stale_truth_artifact_is_stale_and_blocking(tmp_path: Path) -> None:
    registry = _seed_healthy_artifacts(tmp_path)
    open_order_agent = _agent(registry, "open_order_truth")
    _write_json(Path(open_order_agent["heartbeat_artifact_path"]), {"generated_at": (NOW - timedelta(minutes=20)).isoformat()})

    payload = build_track_b_agent_health(config=TrackBAgentHealthConfig(repo_root=tmp_path), now=NOW, process_rows=[])

    open_order = _agent(payload, "open_order_truth")
    assert payload["classification"] == AGENT_HEALTH_BLOCKING
    assert open_order["status"] == STALE
    assert open_order["freshness_age_seconds"] == 1200.0


def test_phase1_market_closed_is_healthy_not_producer_down(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path, phase1_market_closed=True)

    payload = build_track_b_agent_health(
        config=TrackBAgentHealthConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[
            {
                "pid": 101,
                "command": "python -m mgc_v05l.execution_core.phase1_databento_live_runtime_candles --mode service",
            }
        ],
    )

    phase1 = _agent(payload, "phase1_databento_live_candles")
    assert phase1["status"] == HEALTHY
    assert phase1["reason"] == MARKET_CLOSED_NO_FRESH_BARS
    assert phase1["expected_support_process_running"] is True
    assert phase1["blocking_for_proof"] is False


def test_stale_runtime_pid_is_degraded_not_proof_blocking_when_runtime_down_clean(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)
    pid_file = tmp_path / "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text("123456\n", encoding="utf-8")

    payload = build_track_b_agent_health(
        config=TrackBAgentHealthConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[],
        pid_running=lambda _pid: False,
    )

    runtime = _agent(payload, "track_b_paper_runtime")
    assert payload["classification"] == AGENT_HEALTH_DEGRADED
    assert runtime["status"] == DEGRADED
    assert runtime["stale_pid_detected"] is True
    assert runtime["blocking_for_proof"] is False
    assert runtime["blocking_for_runtime_submit"] is False


def test_managed_active_hold_runtime_down_is_degraded_not_blocking(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {"generated_at": NOW.isoformat(), "classification": RUNTIME_DOWN_WITH_BROKER_EXPOSURE},
    )
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json",
        {
            "generated_at": NOW.isoformat(),
            "refresh_generation_id": "track-b-shared-truth-test",
            "classifications": {
                "Open Order Truth": "BROKER_POSITION_WITHOUT_CLOSE_ORDER",
                "Managed Order Registry": "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
                "Position Truth": "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
                "Managed Position Registry": "OPEN_MANAGED_MATCHED",
                "Reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
            },
        },
    )

    payload = build_track_b_agent_health(config=TrackBAgentHealthConfig(repo_root=tmp_path), now=NOW, process_rows=[])

    runtime = _agent(payload, "track_b_paper_runtime")
    assert payload["classification"] == AGENT_HEALTH_DEGRADED
    assert runtime["status"] == DEGRADED
    assert runtime["reason"] == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
    assert runtime["blocking_for_proof"] is False
    assert runtime["blocking_for_runtime_submit"] is False


def test_fresh_broker_truth_with_stale_refresher_pid_is_not_proof_blocking(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {"generated_at": NOW.isoformat(), "classification": "BROKER_TRUTH_REFRESH_READY"},
    )
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        {
            "generated_at": NOW.isoformat(),
            "lease_state": "ACTIVE",
            "operator_action_required": False,
            "live_money_eligible": False,
        },
    )
    (tmp_path / "var/track_b_broker_truth_refresh_service.pid").write_text("123456\n", encoding="utf-8")

    payload = build_track_b_agent_health(
        config=TrackBAgentHealthConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[],
        pid_running=lambda _pid: False,
    )

    broker = _agent(payload, "broker_truth_lease_refresher")
    assert broker["status"] == HEALTHY
    assert broker["reason"] == "broker_truth_and_lease_fresh"
    assert broker["stale_pid_detected"] is True
    assert broker["blocking_for_proof"] is False
    assert broker["blocking_for_runtime_submit"] is False


def test_stale_broker_truth_remains_proof_blocking(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)
    _write_json(
        tmp_path / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {"generated_at": (NOW - timedelta(minutes=20)).isoformat(), "classification": "BROKER_TRUTH_REFRESH_READY"},
    )
    _write_json(
        tmp_path / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json",
        {
            "generated_at": (NOW - timedelta(minutes=20)).isoformat(),
            "lease_state": "ACTIVE",
            "operator_action_required": False,
            "live_money_eligible": False,
        },
    )

    payload = build_track_b_agent_health(config=TrackBAgentHealthConfig(repo_root=tmp_path), now=NOW, process_rows=[])

    broker = _agent(payload, "broker_truth_lease_refresher")
    assert broker["status"] == STALE
    assert broker["blocking_for_proof"] is True


def test_duplicate_runtime_writer_blocks(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)

    payload = build_track_b_agent_health(
        config=TrackBAgentHealthConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[
            {"pid": 10, "command": "bash scripts/run_probationary_paper_soak.sh"},
            {"pid": 11, "command": "python -m mgc_v05l.app.main probationary-paper-soak"},
        ],
    )

    runtime = _agent(payload, "track_b_paper_runtime")
    assert payload["classification"] == AGENT_HEALTH_BLOCKING
    assert runtime["status"] == DUPLICATE_PROCESS
    assert runtime["duplicate_process_detected"] is True
    assert runtime["blocking_for_proof"] is True
    assert runtime["blocking_for_runtime_submit"] is True


def test_source_commit_mismatch_blocks_required_authority_agent(tmp_path: Path) -> None:
    registry = _seed_healthy_artifacts(tmp_path)
    open_order_agent = _agent(registry, "open_order_truth")
    _write_json(
        Path(open_order_agent["heartbeat_artifact_path"]),
        {"generated_at": NOW.isoformat(), "source_commit": "old-head"},
    )

    payload = build_track_b_agent_health(
        config=TrackBAgentHealthConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[],
        source_commit_resolver=lambda _root: "new-head",
    )

    open_order = _agent(payload, "open_order_truth")
    assert payload["classification"] == AGENT_HEALTH_BLOCKING
    assert open_order["status"] == SOURCE_COMMIT_MISMATCH
    assert open_order["source_commit_matches"] is False


def test_root_mismatch_blocks_required_authority_agent(tmp_path: Path) -> None:
    registry = _seed_healthy_artifacts(tmp_path)
    open_order_agent = _agent(registry, "open_order_truth")
    _write_json(
        Path(open_order_agent["heartbeat_artifact_path"]),
        {"generated_at": NOW.isoformat(), "repo_root": str(tmp_path / "wrong-root")},
    )

    payload = build_track_b_agent_health(
        config=TrackBAgentHealthConfig(repo_root=tmp_path),
        now=NOW,
        process_rows=[],
    )

    open_order = _agent(payload, "open_order_truth")
    assert payload["classification"] == AGENT_HEALTH_BLOCKING
    assert open_order["status"] == ROOT_MISMATCH
    assert open_order["root_matches"] is False


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_healthy_artifacts(tmp_path)
    config = TrackBAgentHealthConfig(repo_root=tmp_path)
    payload = build_track_b_agent_health(config=config, now=NOW, process_rows=[])

    authority_path = write_track_b_agent_health(config=config, payload=payload)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json"
    assert projection_path == tmp_path / DEFAULT_DASHBOARD_AGENT_HEALTH_PROJECTION
    projection = json.loads(projection_path.read_text(encoding="utf-8"))
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert projection["authority_owner"] == "execution_core"


def test_no_dashboard_projection_consumed_as_authority() -> None:
    repo_root = Path(__file__).resolve().parents[3]
    forbidden = "outputs/operator_dashboard/runtime/latest_track_b_agent_health.json"
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


def _seed_healthy_artifacts(root: Path, *, phase1_market_closed: bool = False) -> dict:
    registry_config = TrackBAgentRegistryConfig(repo_root=root, dashboard_projection_path=None)
    registry = build_track_b_agent_registry(config=registry_config, now=NOW)
    write_track_b_agent_registry(config=registry_config, payload=registry)
    for agent in registry["agents"]:
        for path_text in agent.get("expected_artifact_paths") or []:
            _write_json(Path(path_text), {"generated_at": NOW.isoformat(), "classification": "READY"})
        heartbeat = agent.get("heartbeat_artifact_path")
        if heartbeat:
            _write_json(Path(heartbeat), {"generated_at": NOW.isoformat(), "classification": "READY"})
    _write_json(
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {"generated_at": NOW.isoformat(), "classification": RUNTIME_DOWN_CLEAN},
    )
    phase1_payload = {
        "generated_at": NOW.isoformat(),
        "market_session": {
            "classification": MARKET_CLOSED_NO_FRESH_BARS if phase1_market_closed else "MARKET_OPEN",
            "reason": "weekend_globex_halt" if phase1_market_closed else "market_open",
        },
        "rows": [
            {
                "symbol": "MGC",
                "runtime_candles_ready": not phase1_market_closed,
                "runtime_candles_block_reason": MARKET_CLOSED_NO_FRESH_BARS if phase1_market_closed else "READY",
                "derived_features_ready": True,
            }
        ],
    }
    _write_json(
        root / "outputs" / "reports" / "phase1_runtime_data_readiness" / "latest_phase1_runtime_data_readiness.json",
        phase1_payload,
    )
    return registry


def _agent(payload: dict, agent_id: str) -> dict:
    for agent in payload["agents"]:
        if agent["agent_id"] == agent_id:
            return agent
    raise AssertionError(f"Missing agent {agent_id}")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
