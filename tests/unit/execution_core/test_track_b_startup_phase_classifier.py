from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_startup_phase_classifier import (
    AUTHORITY_REFRESHED,
    CONTROL_PLANE_SNAPSHOT_START_BLOCKED,
    LANES_LOADED,
    MARKET_DATA_FEED_OBSERVED,
    PRECHECK_ACCEPTED,
    PROCESS_IDENTIFIED,
    PROCESS_SPAWNED,
    PROCESS_STARTED_LANES_PENDING,
    PROCESS_STARTED_PROFILE_PENDING,
    PROFILE_LOADED,
    READINESS_EVALUATED,
    RUNTIME_INGESTION_ADVANCING,
    STARTED_DEGRADED_RUNTIME_INGESTION_STALE,
    STARTED_DIAGNOSTIC_ONLY_MARKET_CLOSED,
    STARTED_NOT_SUBMIT_CAPABLE_AUTHORITY_PENDING,
    SUBMIT_CAPABLE,
    TrackBStartupPhaseClassifierConfig,
    classify_track_b_startup_phase,
)


NOW = datetime(2026, 6, 4, 15, 0, tzinfo=UTC)


def test_process_alive_with_profile_missing_is_profile_pending() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(config_in_force={}),
        now=NOW,
    )

    assert result["classification"] == PROCESS_STARTED_PROFILE_PENDING
    assert result["phase"] == PROCESS_IDENTIFIED
    assert result["next_expected_phase"] == PROFILE_LOADED
    assert _phase_state(result, PROCESS_SPAWNED) == "PASSED"
    assert _phase_state(result, PROFILE_LOADED) == "WAITING"
    _assert_no_submit_authority(result)


def test_control_plane_snapshot_start_blocked_surfaces_exact_launch_blocker() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(
            runtime_truth={
                "generated_at": "2026-06-04T14:59:00+00:00",
                "process_alive": False,
                "producer_pid": 1234,
                "runtime_instance_id": "track-b-runtime-fixture",
                "source_commit": "abc123",
                "producer_root": "/tmp/mgc",
                "restart_generation": 7,
            },
            config_in_force={},
            launch_status={
                "generated_at": NOW.isoformat(),
                "classification": CONTROL_PLANE_SNAPSHOT_START_BLOCKED,
                "child_exit_code": 2,
                "final_pid_alive": False,
                "first_truth_generated_at": None,
                "second_truth_generated_at": None,
                "detail": (
                    "Control Plane Snapshot blocks runtime start. "
                    "primary_blocking_agent_id=canonical_readiness_refresher "
                    "primary_blocking_reason=\"artifact_stale\""
                ),
                "control_plane_snapshot": {
                    "classification": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
                    "runtime_supervisor_classification": "SUPERVISOR_MANUAL_REVIEW_REQUIRED",
                    "proof_window_status": "data_stale",
                },
            },
        ),
        now=NOW,
    )

    assert result["classification"] == CONTROL_PLANE_SNAPSHOT_START_BLOCKED
    assert result["current_blockers"][0]["code"] == CONTROL_PLANE_SNAPSHOT_START_BLOCKED
    assert result["current_blockers"][0]["source"] == "control_plane_snapshot_start_preflight"
    assert result["current_blockers"][0]["primary_blocking_agent_id"] == "canonical_readiness_refresher"
    assert result["current_blockers"][0]["primary_blocking_reason"] == "artifact_stale"
    assert result["evidence"]["launch_status_classification"] == CONTROL_PLANE_SNAPSHOT_START_BLOCKED
    _assert_no_submit_authority(result)


def test_stale_control_plane_launch_failure_does_not_override_new_runtime_truth() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(
            launch_status={
                "generated_at": "2026-06-04T14:59:00+00:00",
                "classification": CONTROL_PLANE_SNAPSHOT_START_BLOCKED,
                "child_exit_code": 2,
                "final_pid_alive": False,
            }
        ),
        now=NOW,
    )

    assert result["classification"] == SUBMIT_CAPABLE
    _assert_no_submit_authority(result)


def test_profile_loaded_with_lanes_missing_is_lanes_pending() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(operator_status={"generated_at": NOW.isoformat(), "lanes": []}),
        now=NOW,
    )

    assert result["classification"] == PROCESS_STARTED_LANES_PENDING
    assert result["phase"] == PROFILE_LOADED
    assert result["next_expected_phase"] == LANES_LOADED
    _assert_no_submit_authority(result)


def test_feed_fresh_but_runtime_ingestion_stale_is_degraded_started() -> None:
    operator_status = _operator_status(runtime_ingestion_fresh=False)
    operator_status["runtime_ingestion_stale"] = True

    result = classify_track_b_startup_phase(
        artifacts=_artifacts(operator_status=operator_status),
        now=NOW,
    )

    assert result["classification"] == STARTED_DEGRADED_RUNTIME_INGESTION_STALE
    assert result["phase"] == MARKET_DATA_FEED_OBSERVED
    assert result["next_expected_phase"] == RUNTIME_INGESTION_ADVANCING
    assert result["evidence"]["market_data_feed_observed"] is True
    assert result["evidence"]["runtime_ingestion_advancing"] is False
    _assert_no_submit_authority(result)


def test_authority_stale_after_ingestion_advances_is_authority_pending() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(authority_refresh={"classification": "AUTHORITY_REFRESH_STALE", "fresh": False}),
        now=NOW,
    )

    assert result["classification"] == STARTED_NOT_SUBMIT_CAPABLE_AUTHORITY_PENDING
    assert result["phase"] == RUNTIME_INGESTION_ADVANCING
    assert result["next_expected_phase"] == AUTHORITY_REFRESHED
    assert result["current_blockers"][0]["authority_refresh_classification"] == "AUTHORITY_REFRESH_STALE"
    _assert_no_submit_authority(result)


def test_scheduled_halt_is_diagnostic_only_market_closed() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(
            canonical_readiness={
                "generated_at": NOW.isoformat(),
                "canonical_readiness": "READY_TO_START_DIAGNOSTIC_ONLY",
                "readiness_block_is_scheduled_halt": True,
                "submit_allowed": False,
                "market_schedule_state": "SCHEDULED_MARKET_HALT",
            },
            phase1_listener_status={"classification": "MARKET_CLOSED_NO_FRESH_BARS"},
        ),
        now=NOW,
    )

    assert result["classification"] == STARTED_DIAGNOSTIC_ONLY_MARKET_CLOSED
    assert result["phase"] == READINESS_EVALUATED
    assert result["evidence"]["market_closed_or_scheduled_halt"] is True
    assert result["canonical_readiness_submit_capable"] is False
    _assert_no_submit_authority(result)


def test_ready_submit_capable_observed_from_canonical_readiness_only() -> None:
    result = classify_track_b_startup_phase(artifacts=_artifacts(), now=NOW)

    assert result["classification"] == SUBMIT_CAPABLE
    assert result["phase"] == SUBMIT_CAPABLE
    assert result["completed_phases"] == [
        PRECHECK_ACCEPTED,
        PROCESS_SPAWNED,
        PROCESS_IDENTIFIED,
        PROFILE_LOADED,
        LANES_LOADED,
        MARKET_DATA_FEED_OBSERVED,
        RUNTIME_INGESTION_ADVANCING,
        AUTHORITY_REFRESHED,
        READINESS_EVALUATED,
        SUBMIT_CAPABLE,
    ]
    assert result["canonical_readiness_submit_capable"] is True
    assert result["submit_authority_source"] == "canonical_readiness_only"
    _assert_no_submit_authority(result)


def test_complete_startup_evidence_without_canonical_readiness_cannot_submit() -> None:
    result = classify_track_b_startup_phase(
        artifacts=_artifacts(canonical_readiness={}),
        now=NOW,
    )

    assert result["classification"] == AUTHORITY_REFRESHED
    assert result["phase"] == AUTHORITY_REFRESHED
    assert result["next_expected_phase"] == READINESS_EVALUATED
    assert result["canonical_readiness_submit_capable"] is False
    _assert_no_submit_authority(result)


def test_classifier_reads_existing_artifacts_from_tmp_path_without_writing(tmp_path: Path) -> None:
    config = TrackBStartupPhaseClassifierConfig(repo_root=tmp_path)
    artifacts = _artifacts()
    _write(config.resolve(config.runtime_truth_path), artifacts["runtime_truth"])
    _write(config.resolve(config.pid_metadata_path), artifacts["pid_metadata"])
    _write(config.resolve(config.config_in_force_path), artifacts["config_in_force"])
    _write(config.resolve(config.launch_status_path), artifacts["launch_status"])
    _write(config.resolve(config.operator_status_path), artifacts["operator_status"])
    _write(config.resolve(config.phase1_listener_status_path), artifacts["phase1_listener_status"])
    _write(config.resolve(config.authority_refresh_path), artifacts["authority_refresh"])
    _write(config.resolve(config.canonical_readiness_path), artifacts["canonical_readiness"])
    _write(config.resolve(config.broker_reconciliation_path), artifacts["broker_reconciliation"])

    before = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file())
    result = classify_track_b_startup_phase(config=config, now=NOW)
    after = sorted(path.relative_to(tmp_path) for path in tmp_path.rglob("*") if path.is_file())

    assert result["classification"] == SUBMIT_CAPABLE
    assert after == before
    _assert_no_submit_authority(result)


def _artifacts(**overrides: dict) -> dict[str, dict]:
    payload = {
        "runtime_truth": {
            "generated_at": NOW.isoformat(),
            "process_alive": True,
            "producer_pid": 1234,
            "runtime_instance_id": "track-b-runtime-fixture",
            "source_commit": "abc123",
            "producer_root": "/tmp/mgc",
            "restart_generation": 7,
            "config_fingerprint": "sha256:fixture",
            "profile_id": "mnq_mes_full_session_active_evidence",
            "lane_count": 2,
            "heartbeat_state": "HEALTHY",
        },
        "pid_metadata": {
            "generated_at": NOW.isoformat(),
            "running": True,
            "pid": 1234,
            "runtime_instance_id": "track-b-runtime-fixture",
            "source_commit": "abc123",
            "producer_root": "/tmp/mgc",
            "restart_generation": 7,
        },
        "config_in_force": {
            "generated_at": NOW.isoformat(),
            "profile_id": "mnq_mes_full_session_active_evidence",
            "config_fingerprint": "sha256:fixture",
            "probationary_paper_runtime_exclusive_config": True,
        },
        "launch_status": {},
        "operator_status": _operator_status(),
        "phase1_listener_status": {
            "generated_at": NOW.isoformat(),
            "classification": "READY_FOR_PROOF",
            "fresh": True,
            "latest_record_at": NOW.isoformat(),
        },
        "authority_refresh": {
            "generated_at": NOW.isoformat(),
            "classification": "AUTHORITY_REFRESHED",
            "fresh": True,
        },
        "canonical_readiness": {
            "generated_at": NOW.isoformat(),
            "canonical_readiness": "READY_SUBMIT_CAPABLE",
            "submit_allowed": True,
            "market_schedule_state": "MARKET_OPEN_EXPECT_FRESH_BARS",
        },
        "broker_reconciliation": {
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        },
    }
    for key, value in overrides.items():
        payload[key] = value
    return payload


def _operator_status(*, runtime_ingestion_fresh: bool = True) -> dict:
    return {
        "generated_at": NOW.isoformat(),
        "expected_lane_count": 2,
        "runtime_ingestion_fresh": runtime_ingestion_fresh,
        "lanes": [
            {
                "lane_id": "mnq_lane",
                "last_processed_bar_end_ts": NOW.isoformat(),
                "last_execution_bar_evaluated_at": NOW.isoformat() if runtime_ingestion_fresh else "2026-06-04T14:50:00+00:00",
            },
            {
                "lane_id": "mes_lane",
                "last_processed_bar_end_ts": NOW.isoformat(),
                "last_execution_bar_evaluated_at": NOW.isoformat() if runtime_ingestion_fresh else "2026-06-04T14:50:00+00:00",
            },
        ],
    }


def _phase_state(result: dict, phase: str) -> str:
    for row in result["phases"]:
        if row["phase"] == phase:
            return row["state"]
    raise AssertionError(f"missing phase {phase}")


def _assert_no_submit_authority(result: dict) -> None:
    assert result["submit_authority"] is False
    assert result["startup_grants_submit_authority"] is False
    assert result["broker_mutation_allowed"] is False
    assert result["paper_proof_invoked"] is False
    assert result["live_money_eligible"] is False
    assert result["broad_flatten_allowed"] is False
    assert result["global_flatten_allowed"] is False


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
