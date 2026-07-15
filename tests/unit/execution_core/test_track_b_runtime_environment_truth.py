from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    DUPLICATE_RUNTIME_WRITERS,
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    RUNTIME_DOWN_WITH_BROKER_EXPOSURE,
    STALE_RUNTIME_TRUTH,
    WRONG_ROOT_RUNTIME,
    TrackBRuntimeEnvironmentTruthConfig,
    build_dashboard_runtime_environment_projection,
    build_track_b_runtime_environment_truth,
    write_track_b_runtime_environment_truth,
)


NOW = datetime(2026, 5, 22, 12, 0, tzinfo=UTC)


def test_active_healthy_single_writer_is_trade_capable(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)

    payload = _build(tmp_path)

    assert payload["classification"] == RUNTIME_ACTIVE_TRADE_CAPABLE
    assert payload["runtime"]["pid"] == 123
    assert payload["runtime"]["runtime_truth_fresh"] is True
    assert payload["runtime"]["writer_authority"] == "SINGLE_WRITER"
    assert payload["live_money_eligible"] is False
    assert payload["blockers"] == []


def test_active_runtime_trade_capable_ignores_stale_not_ready_canonical(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    _write_json(
        _canonical_path(tmp_path),
        {
            "generated_at": NOW.isoformat(),
            "canonical_readiness": "NOT_READY_DEPENDENCY",
            "readiness_blockers": [{"code": "runtime_not_healthy"}],
            "live_money_eligible": False,
        },
    )

    payload = _build(tmp_path)

    assert payload["classification"] == RUNTIME_ACTIVE_TRADE_CAPABLE
    assert payload["canonical_readiness"]["diagnostic_only_for_runtime_environment_truth"] is True
    assert payload["current_state_trade_capability"]["canonical_readiness_dependency"] is False
    assert payload["current_state_trade_capability"]["blocking_reasons"] == []


def test_false_current_submit_authority_remains_observation_only(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    child = json.loads(_detached_child_status_path(tmp_path).read_text(encoding="utf-8"))
    child["submit_authority"] = False
    _write_json(_detached_child_status_path(tmp_path), child)

    payload = _build(tmp_path)

    assert payload["classification"] == RUNTIME_ACTIVE_OBSERVATION_ONLY
    assert "current_state_submit_authority_false" in payload["current_state_trade_capability"]["blocking_reasons"]


def test_stale_or_invalid_broker_lease_prevents_trade_capable(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    lease = json.loads(_broker_truth_lease_path(tmp_path).read_text(encoding="utf-8"))
    lease["lease_state"] = "INVALIDATED_CONTRADICTION"
    lease["blockers"] = [{"code": "lifecycle_broker_position_mismatch"}]
    _write_json(_broker_truth_lease_path(tmp_path), lease)

    payload = _build(tmp_path)

    assert payload["classification"] == RUNTIME_ACTIVE_OBSERVATION_ONLY
    reasons = set(payload["current_state_trade_capability"]["blocking_reasons"])
    assert "broker_truth_lease_not_active:INVALIDATED_CONTRADICTION" in reasons
    assert "broker_truth_lease_blockers_present" in reasons


def test_unsafe_safe_state_prevents_trade_capable(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    safe_state = json.loads(_safe_state_path(tmp_path).read_text(encoding="utf-8"))
    safe_state["classification"] = "SAFE_STATE_HARD_HALT"
    safe_state["submit_allowed"] = False
    _write_json(_safe_state_path(tmp_path), safe_state)

    payload = _build(tmp_path)

    assert payload["classification"] == RUNTIME_ACTIVE_OBSERVATION_ONLY
    reasons = set(payload["current_state_trade_capability"]["blocking_reasons"])
    assert "safe_state_not_normal:SAFE_STATE_HARD_HALT" in reasons
    assert "safe_state_submit_not_allowed" in reasons


def test_reconciliation_blocker_prevents_trade_capable(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    reconciliation = json.loads(_reconciliation_path(tmp_path).read_text(encoding="utf-8"))
    reconciliation["classification"] = "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    reconciliation["broker_reconciled"] = False
    _write_json(_reconciliation_path(tmp_path), reconciliation)

    payload = _build(tmp_path)

    assert payload["classification"] == RUNTIME_ACTIVE_OBSERVATION_ONLY
    reasons = set(payload["current_state_trade_capability"]["blocking_reasons"])
    assert "reconciliation_not_clean:WAITING_FOR_BROKER_TRUTH_SETTLEMENT" in reasons
    assert "reconciliation_broker_reconciled_false" in reasons


def test_no_runtime_clean_flat_is_down_clean(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path, pid_alive=False)
    _runtime_truth_path(tmp_path).unlink()
    _pid_metadata_path(tmp_path).unlink()

    payload = _build(tmp_path, pid_alive=False)

    assert payload["classification"] == RUNTIME_DOWN_CLEAN
    assert payload["runtime"]["pid_alive"] is False


def test_dead_stale_pid_metadata_clean_flat_is_down_clean_with_warning(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path, pid_alive=False)

    payload = _build(tmp_path, pid_alive=False)

    assert payload["classification"] == RUNTIME_DOWN_CLEAN
    assert payload["runtime"]["pid_alive"] is False
    assert payload["warnings"][0]["code"] == "stale_pid_metadata"
    assert payload["blockers"] == []


def test_no_runtime_with_broker_exposure_is_loud(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path, pid_alive=False)
    position_truth = json.loads(_position_truth_path(tmp_path).read_text(encoding="utf-8"))
    position_truth["summary"] = {"overall_classification": "ATTENTION_REQUIRED", "broker_exposure_present": True}
    position_truth["broker_positions"] = [{"symbol": "MGC", "quantity": "1"}]
    _write_json(_position_truth_path(tmp_path), position_truth)

    payload = _build(tmp_path, pid_alive=False)

    assert payload["classification"] == RUNTIME_DOWN_WITH_BROKER_EXPOSURE
    assert payload["blockers"][0]["code"] == "broker_exposure_without_runtime"


def test_duplicate_writer_evidence_blocks(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    runtime_truth = json.loads(_runtime_truth_path(tmp_path).read_text(encoding="utf-8"))
    runtime_truth["writer_authority"] = "DUPLICATE_WRITERS"
    _write_json(_runtime_truth_path(tmp_path), runtime_truth)

    payload = _build(tmp_path)

    assert payload["classification"] == DUPLICATE_RUNTIME_WRITERS


def test_stale_runtime_truth_detected(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path, runtime_generated_at="2026-05-22T11:00:00+00:00")

    payload = _build(tmp_path)

    assert payload["classification"] == STALE_RUNTIME_TRUTH
    assert payload["blockers"][0]["code"] == "stale_runtime_truth"


def test_wrong_root_detected(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)

    payload = _build(tmp_path, process_root=Path("/tmp/wrong-root"))

    assert payload["classification"] == WRONG_ROOT_RUNTIME
    assert payload["blockers"][0]["code"] == "wrong_root_runtime"


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_trade_capable(tmp_path)
    config = TrackBRuntimeEnvironmentTruthConfig(repo_root=tmp_path)
    payload = _build(tmp_path)

    authority_path, _ = write_track_b_runtime_environment_truth(config=config, payload=payload, now=NOW)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    assert build_dashboard_runtime_environment_projection(authority_payload=payload, authority_path=authority_path)[
        "authority_owner"
    ] == "execution_core"


def _build(
    root: Path,
    *,
    pid_alive: bool = True,
    process_root: Path | None = None,
    head: str = "abc123",
) -> dict:
    return build_track_b_runtime_environment_truth(
        config=TrackBRuntimeEnvironmentTruthConfig(repo_root=root),
        now=NOW,
        pid_running=lambda pid: pid_alive,
        process_root_resolver=lambda pid: process_root or root,
        source_commit_resolver=lambda repo_root: head,
    )


def _seed_trade_capable(root: Path, *, pid_alive: bool = True, runtime_generated_at: str | None = None) -> None:
    generated_at = runtime_generated_at or NOW.isoformat()
    _write_json(
        _runtime_truth_path(root),
        {
            "runtime_instance_id": "runtime-a",
            "restart_generation": 3,
            "producer_pid": 123 if pid_alive else 987,
            "producer_root": str(root),
            "generated_at": generated_at,
            "last_success_at": generated_at,
            "freshness_ttl_seconds": 180,
            "heartbeat_state": "HEALTHY",
            "freshness_state": "FRESH",
            "writer_authority": "SINGLE_WRITER",
            "source_commit": "abc123",
            "profile": "mnq_mes_full_session_active_evidence",
            "config_fingerprint": "cfg-1",
            "lane_count": 17,
            "test_mule_enabled": True,
            "live_money_eligible": False,
        },
    )
    _write_json(
        _pid_metadata_path(root),
        {
            "pid": 123 if pid_alive else 987,
            "runtime_instance_id": "runtime-a",
            "restart_generation": 3,
            "root": str(root),
            "source_commit": "abc123",
            "config_fingerprint": "cfg-1",
            "generated_at": generated_at,
        },
    )
    _write_json(
        _operator_status_path(root),
        {
            "source_runtime_pid": 123 if pid_alive else 987,
            "config_fingerprint": "cfg-1",
            "generated_at": generated_at,
            "live_money_eligible": False,
        },
    )
    _write_json(
        _canonical_path(root),
        {
            "generated_at": generated_at,
            "canonical_readiness": "READY_SUBMIT_CAPABLE",
            "readiness_blockers": [],
            "readiness_warnings": [],
            "live_money_eligible": False,
        },
    )
    _write_json(_launch_status_path(root), {"classification": "RUNTIME_PID_AVAILABLE", "generated_at": generated_at})
    _write_json(_self_healing_path(root), {"classification": "SELF_HEALING_READY", "generated_at": generated_at})
    _write_json(
        _detached_child_status_path(root),
        {
            "generated_at": generated_at,
            "pid": 123 if pid_alive else 987,
            "state": "TRADING_LOOP_ENTERED",
            "submit_authority": True,
            "submit_authority_source": "current_state_authority",
            "live_money_eligible": False,
        },
    )
    _write_json(
        _broker_truth_lease_path(root),
        {
            "generated_at": generated_at,
            "lease_state": "ACTIVE",
            "submit_entry_allowed": True,
            "submit_exit_allowed": True,
            "broker_reconciled": True,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "blockers": [],
            "live_money_eligible": False,
        },
    )
    _write_json(
        _open_order_truth_path(root),
        {
            "generated_at": generated_at,
            "classification": "NO_OPEN_ORDERS",
            "open_order_count": 0,
            "unknown_order_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "blockers": [],
            "live_money_eligible": False,
        },
    )
    _write_json(
        _guardian_path(root),
        {
            "generated_at": generated_at,
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "blockers": [],
            "live_money_eligible": False,
        },
    )
    _write_json(
        _safe_state_path(root),
        {
            "generated_at": generated_at,
            "classification": "SAFE_STATE_NORMAL",
            "submit_allowed": True,
            "entry_submit_allowed": True,
            "blockers": [],
            "live_money_eligible": False,
        },
    )
    _write_json(
        _position_truth_path(root),
        {
            "generated_at": generated_at,
            "summary": {"overall_classification": "CLEAN_FLAT_READY", "broker_exposure_present": False},
            "broker_positions": [],
            "open_broker_orders": [],
            "live_money_eligible": False,
        },
    )
    _write_json(
        _reconciliation_path(root),
        {
            "generated_at": generated_at,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "live_money_eligible": False,
        },
    )


def _runtime_truth_path(root: Path) -> Path:
    return root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "paper_runtime_truth.json"


def _pid_metadata_path(root: Path) -> Path:
    return root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper.pid.json"


def _launch_status_path(root: Path) -> Path:
    return root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper_launch_status.json"


def _operator_status_path(root: Path) -> Path:
    return root / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"


def _canonical_path(root: Path) -> Path:
    return root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"


def _self_healing_path(root: Path) -> Path:
    return root / "outputs" / "operator_dashboard" / "runtime" / "latest_maintenance_supervisor_decision.json"


def _position_truth_path(root: Path) -> Path:
    return root / "outputs" / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"


def _reconciliation_path(root: Path) -> Path:
    return root / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest_track_b_paper_broker_reconciliation.json"


def _detached_child_status_path(root: Path) -> Path:
    return root / "outputs" / "probationary_pattern_engine" / "paper_session" / "runtime" / "probationary_paper_detached_child_status.json"


def _broker_truth_lease_path(root: Path) -> Path:
    return root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"


def _open_order_truth_path(root: Path) -> Path:
    return root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"


def _guardian_path(root: Path) -> Path:
    return root / "outputs" / "track_b_execution_core" / "broker_position_guardian" / "latest_broker_position_guardian.json"


def _safe_state_path(root: Path) -> Path:
    return root / "outputs" / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
