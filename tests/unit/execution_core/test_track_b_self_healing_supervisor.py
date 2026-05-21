from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_self_healing_supervisor import (
    DEFAULT_SELF_HEALING_HEALTH_ARTIFACT,
    build_track_b_self_healing_agent_registry,
    build_track_b_self_healing_health,
    classify_track_b_self_healing_health,
    write_track_b_self_healing_health,
)


NOW = datetime(2026, 5, 21, 3, 45, tzinfo=timezone.utc)
DEV_ROOT = "/Users/patrick/Dev/MGC-v05l-automation"
RECONCILED = "TRACK_B_PAPER_BROKER_RECONCILED"


def test_writer_builds_and_writes_self_healing_artifact(tmp_path: Path) -> None:
    _write_runtime_artifacts(tmp_path)
    health = build_track_b_self_healing_health(
        repo_root=tmp_path,
        expected_root=tmp_path,
        now=NOW,
        process_probe=_process_probe(tmp_path),
    )
    output = tmp_path / DEFAULT_SELF_HEALING_HEALTH_ARTIFACT
    write_track_b_self_healing_health(output_path=output, health=health)

    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["classification"] == "SELF_HEALING_READY"
    assert written["auto_restart_allowed"] is False
    assert written["live_money_eligible"] is False
    assert written["agents"]["paper_runtime"]["restart_eligible"] is True
    assert written["agents"]["broker_truth_refresher"]["health_state"] == "HEALTHY"


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
    assert paper_runtime["restart_eligible"] is True
    assert "broker_truth_lease_not_active" in paper_runtime["restart_blockers"]
    assert "unresolved_submit_ownership" in paper_runtime["restart_blockers"]


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


def test_runtime_degraded_becomes_restart_candidate_for_clean_broker_state() -> None:
    inputs = _inputs()
    inputs["agents"]["paper_runtime"]["process_running"] = False

    result = classify_track_b_self_healing_health(inputs)

    assert result["classification"] == "AUTO_RESTART_ELIGIBLE"
    assert result["auto_restart_allowed"] is True
    assert result["restart_candidates"] == ("paper_runtime",)
    assert "paper_runtime_not_running" in result["agents"]["paper_runtime"]["blockers"]


def test_paper_runtime_truth_is_optional_evidence_not_restart_authority(tmp_path: Path) -> None:
    _write_runtime_artifacts(tmp_path)
    truth_path = (
        tmp_path
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_runtime_truth.json"
    )
    truth_path.write_text(
        json.dumps(
            {
                "schema_version": "track_b_runtime_truth_contract_v1",
                "runtime_instance_id": "track-b-paper-runtime-test",
                "service_name": "track_b_paper_runtime",
                "producer_pid": 1006,
                "producer_root": str(tmp_path),
                "generated_at": NOW.isoformat(),
                "last_success_at": NOW.isoformat(),
                "freshness_ttl_seconds": 180.0,
                "freshness_state": "FRESH",
                "heartbeat_state": "HEALTHY",
                "writer_authority": "SINGLE_WRITER",
                "source_commit": "abc123",
                "config_fingerprint": "sha256:deadbeef",
                "runtime_mode": "PAPER",
                "restart_generation": 0,
                "duplicate_writer_detection": {"duplicate_writer_detected": False},
                "stale_reason": None,
                "recovery_state": "OBSERVE_ONLY",
                "lane_count": 17,
                "b_plus_threshold": 0.775,
                "test_mule_enabled": True,
            }
        ),
        encoding="utf-8",
    )

    health = build_track_b_self_healing_health(
        repo_root=tmp_path,
        expected_root=tmp_path,
        now=NOW,
        process_probe=_process_probe(tmp_path),
    )

    paper_runtime = health["agents"]["paper_runtime"]
    truth_row = next(row for row in paper_runtime["artifacts"] if row["label"] == "paper_runtime_truth")
    assert health["classification"] == "SELF_HEALING_READY"
    assert health["auto_restart_allowed"] is False
    assert truth_row["required"] is False
    assert truth_row["fresh"] is True
    assert truth_row["payload"]["lane_count"] == 17


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
            "track_b_broker_position_count": 0,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "broker_truth_lease_state": "ACTIVE",
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


def _write_runtime_artifacts(repo_root: Path) -> None:
    def write(rel: str, payload: dict) -> None:
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    fresh = {"generated_at": NOW.isoformat(), "classification": "HEALTHY", "live_money_eligible": False}
    write("outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_listener_status.json", {**fresh, "latest_record_at": NOW.isoformat()})
    write("outputs/reports/phase1_databento_live_runtime_candles/latest_phase1_databento_live_supervisor_status.json", fresh)
    write("var/track_b_broker_truth_refresh_heartbeat.json", fresh)
    write("outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json", {**fresh, "last_success_at": NOW.isoformat()})
    write("outputs/operator_dashboard/runtime/latest_broker_truth_lease.json", {**fresh, "lease_state": "ACTIVE"})
    write("var/track_b_operator_readiness_refresh_heartbeat.json", fresh)
    write("outputs/reports/track_b_operator_readiness_refresher/latest_track_b_operator_readiness_refresher_status.json", {**fresh, "last_refresh_finished_at": NOW.isoformat()})
    write("outputs/operator_dashboard/runtime/latest_canonical_readiness.json", {**fresh, "canonical_readiness": "READY_SUBMIT_CAPABLE"})
    write("outputs/probationary_pattern_engine/paper_session/operator_status.json", fresh)
    write(
        "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            **fresh,
            "classification": RECONCILED,
            "broker_reconciled": True,
            "unknown_broker_open_order_count": 0,
            "track_b_broker_open_order_count": 0,
            "review_required_count": 0,
            "lifecycle_open_position_count": 0,
        },
    )
    pid_paths = (
        "var/phase1_databento_live_candles_service.pid",
        "var/phase1_databento_live_candles_child.pid",
        "var/track_b_broker_truth_refresh_service.pid",
        "var/track_b_operator_readiness_refresh_service.pid",
        "var/track_b_operator_readiness_refresh_child.pid",
        "outputs/probationary_pattern_engine/paper_session/runtime/probationary_paper.pid",
    )
    for idx, rel in enumerate(pid_paths, start=1001):
        path = repo_root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(idx), encoding="utf-8")


def _process_probe(repo_root: Path):
    commands = {
        1001: "python -m mgc_v05l.execution_core.phase1_databento_live_runtime_candles --mode service",
        1002: "python -m mgc_v05l.execution_core.phase1_databento_live_runtime_candles --mode service",
        1003: "python -m mgc_v05l.app.ibkr_broker_truth_refresher --service --read-only",
        1004: "python -m mgc_v05l.app.track_b_operator_readiness_refresher --service",
        1005: "python -m mgc_v05l.app.track_b_operator_readiness_refresher --service",
        1006: "python -m mgc_v05l.app.main probationary-paper-soak",
    }

    def probe(pid: int) -> dict:
        return {"running": pid in commands, "cwd": str(repo_root), "command": commands.get(pid, "")}

    return probe
