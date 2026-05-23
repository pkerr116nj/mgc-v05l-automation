from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_agent_health import HEALTHY, STOPPED_EXPECTED
from mgc_v05l.execution_core.track_b_crash_loop_protection import (
    INSUFFICIENT_HISTORY,
    NO_CRASH_LOOP,
    OPERATOR_ACK_REQUIRED,
    REPEATED_BROKER_LEASE_FAILURE,
    RESTART_COOLDOWN_ACTIVE,
    TrackBCrashLoopProtectionConfig,
    build_dashboard_crash_loop_projection,
    build_track_b_crash_loop_protection,
    write_track_b_crash_loop_protection,
)
from mgc_v05l.execution_core.track_b_runtime_environment_truth import RUNTIME_DOWN_CLEAN
from mgc_v05l.execution_core.track_b_self_recover_rules import RESTART_RUNTIME_ALLOWED, WAIT_MARKET_CLOSED
from mgc_v05l.market_data.phase1_market_session import MARKET_CLOSED_NO_FRESH_BARS


NOW = datetime(2026, 5, 23, 12, 0, tzinfo=UTC)


def test_no_history_is_nonblocking_insufficient_history(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = build_track_b_crash_loop_protection(config=TrackBCrashLoopProtectionConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == INSUFFICIENT_HISTORY
    assert payload["restart_blocked"] is False
    assert payload["operator_ack_required"] is False
    assert payload["read_only"] is True
    assert payload["runtime_restart_authority"] is False
    assert payload["broker_mutation"] is False


def test_market_closed_stop_is_not_a_crash_loop(tmp_path: Path) -> None:
    _seed_base(tmp_path, self_recover_recommendation=WAIT_MARKET_CLOSED, phase1_reason=MARKET_CLOSED_NO_FRESH_BARS)
    _write_launch_history(
        tmp_path,
        [
            _launch_event(
                seconds_ago=60,
                classification="RUNTIME_EXITED_AFTER_PREFLIGHT",
                stop_reason="runtime_exited_after_preflight",
                expected_cleanup=True,
                broker_safe_at_stop=True,
            )
        ],
    )

    payload = build_track_b_crash_loop_protection(config=TrackBCrashLoopProtectionConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == NO_CRASH_LOOP
    assert payload["restart_blocked"] is False
    assert payload["reason"] == "Market is closed; no fresh bars expected."


def test_repeated_runtime_preflight_exits_trigger_cooldown(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_launch_history(
        tmp_path,
        [
            _launch_event(seconds_ago=120, classification="RUNTIME_EXITED_AFTER_PREFLIGHT", stop_reason="runtime_exited_after_preflight"),
            _launch_event(seconds_ago=30, classification="RUNTIME_EXITED_AFTER_PREFLIGHT", stop_reason="runtime_exited_after_preflight"),
        ],
    )

    payload = build_track_b_crash_loop_protection(config=TrackBCrashLoopProtectionConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == RESTART_COOLDOWN_ACTIVE
    assert payload["restart_blocked"] is True
    assert payload["cooldown_until"] is not None
    assert payload["repeated_same_stop_reason_count"] == 2


def test_repeated_unsafe_stops_require_operator_ack(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_launch_history(
        tmp_path,
        [
            _launch_event(
                seconds_ago=120,
                classification="paper_reconciliation_mismatch",
                stop_reason="paper_reconciliation_mismatch",
                stop_source="runtime_internal",
                broker_safe_at_stop=False,
            ),
            _launch_event(
                seconds_ago=30,
                classification="paper_reconciliation_mismatch",
                stop_reason="paper_reconciliation_mismatch",
                stop_source="runtime_internal",
                broker_safe_at_stop=False,
            ),
        ],
    )

    payload = build_track_b_crash_loop_protection(config=TrackBCrashLoopProtectionConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == OPERATOR_ACK_REQUIRED
    assert payload["restart_blocked"] is True
    assert payload["operator_ack_required"] is True


def test_repeated_broker_lease_failure_blocks_restart(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    _write_launch_history(
        tmp_path,
        [
            _launch_event(seconds_ago=120, classification="ACTIVE_DEGRADED_REFRESH_FAILING", stop_reason="ACTIVE_DEGRADED_REFRESH_FAILING"),
            _launch_event(seconds_ago=30, classification="ACTIVE_DEGRADED_REFRESH_FAILING", stop_reason="ACTIVE_DEGRADED_REFRESH_FAILING"),
        ],
    )

    payload = build_track_b_crash_loop_protection(config=TrackBCrashLoopProtectionConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == REPEATED_BROKER_LEASE_FAILURE
    assert payload["restart_blocked"] is True
    assert payload["operator_ack_required"] is False


def test_dashboard_projection_is_not_authority(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBCrashLoopProtectionConfig(repo_root=tmp_path)
    payload = build_track_b_crash_loop_protection(config=config, now=NOW)

    authority_path, events = write_track_b_crash_loop_protection(config=config, payload=payload, now=NOW)
    projection_path = config.resolve(config.dashboard_projection_path)  # type: ignore[arg-type]
    projection = json.loads(projection_path.read_text(encoding="utf-8"))

    assert authority_path == tmp_path / "outputs" / "track_b_execution_core" / "crash_loop_protection" / "latest_crash_loop_protection.json"
    assert events
    assert projection["projection_only"] is True
    assert projection["not_routing_authority"] is True
    assert projection["source_authority_path"] == str(authority_path)
    direct_projection = build_dashboard_crash_loop_projection(authority_payload=payload, authority_path=authority_path)
    assert direct_projection["operator_dashboard_display_only"] is True


def _seed_base(
    root: Path,
    *,
    self_recover_recommendation: str = RESTART_RUNTIME_ALLOWED,
    phase1_reason: str = "phase1_runtime_candles_ready",
    broker_lease_classification: str = "ACTIVE",
) -> None:
    _write_json(
        root / "outputs" / "track_b_execution_core" / "agent_registry" / "latest_agent_registry.json",
        {"generated_at": NOW.isoformat(), "classification": "AGENT_REGISTRY_READY"},
    )
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
                    "reason": RUNTIME_DOWN_CLEAN,
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
        root / "outputs" / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json",
        {"generated_at": NOW.isoformat(), "classification": RUNTIME_DOWN_CLEAN},
    )
    _write_json(
        root / "outputs" / "track_b_execution_core" / "self_recover" / "latest_self_recover_rules.json",
        {
            "generated_at": NOW.isoformat(),
            "classification": self_recover_recommendation,
            "recommendation": self_recover_recommendation,
            "reason": phase1_reason,
        },
    )
    _write_json(
        root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json",
        {"generated_at": NOW.isoformat(), "classification": broker_lease_classification},
    )


def _launch_event(
    *,
    seconds_ago: int,
    classification: str,
    stop_reason: str,
    stop_source: str = "launcher",
    expected_cleanup: bool = False,
    broker_safe_at_stop: bool = True,
) -> dict:
    observed_at = (NOW - timedelta(seconds=seconds_ago)).isoformat()
    return {
        "generated_at": observed_at,
        "classification": classification,
        "stop_provenance": {
            "stop_source": stop_source,
            "stop_reason": stop_reason,
            "runtime_instance_id": f"track-b-paper-runtime-test-{seconds_ago}",
            "restart_generation": 1,
            "source_commit": "test",
            "observed_at": observed_at,
            "expected_cleanup": expected_cleanup,
            "broker_safe_at_stop": broker_safe_at_stop,
        },
    }


def _write_launch_history(root: Path, rows: list[dict]) -> None:
    path = (
        root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "probationary_paper_launch_status_history.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
