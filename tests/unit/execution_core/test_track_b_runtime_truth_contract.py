from __future__ import annotations

from datetime import datetime, timezone

import pytest

from mgc_v05l.execution_core.track_b_runtime_truth_contract import (
    FRESHNESS_FRESH,
    FRESHNESS_MISSING_ARTIFACT,
    FRESHNESS_STALE,
    HEARTBEAT_ARTIFACT_MISSING,
    HEARTBEAT_ARTIFACT_STALE,
    HEARTBEAT_HEALTHY,
    HEARTBEAT_PROCESS_DOWN,
    HEARTBEAT_WRONG_ROOT,
    CONFIG_STACK_SAFE,
    CONFIG_STACK_UNSAFE,
    LATE_RUNTIME_CONVERGED,
    LAUNCH_CONFLICTING_WRITER_BLOCKED,
    LAUNCHCTL_NO_RUNTIME_JOB_EVIDENCE,
    LAUNCHCTL_RUNTIME_JOB_ACCEPTED,
    LAUNCHCTL_SUBMIT_FAILED,
    LAUNCHCTL_STALE_RUNTIME_JOB_BLOCKED,
    LAUNCH_PID_ACCEPTED,
    LAUNCH_STALE_PID_CLEANUP_ALLOWED,
    LAUNCH_STALE_PID_CLEANUP_BLOCKED,
    LAUNCH_WRONG_ROOT_BLOCKED,
    LAUNCH_ZOMBIE_PID_REJECTED,
    PID_WRITE_PATH_MISMATCH,
    PID_METADATA_MISSING,
    PID_METADATA_OK,
    PID_METADATA_STALE,
    PID_PROCESS_DEAD,
    PID_PROCESS_ZOMBIE,
    PID_WRONG_ROOT,
    RUNTIME_PID_AVAILABLE,
    RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT,
    SCHEMA_VERSION,
    WRAPPER_PRE_EXEC_FAILURE,
    WRITER_DUPLICATE,
    WRITER_MISSING,
    WRITER_SINGLE,
    build_runtime_truth_contract,
    classify_freshness,
    classify_heartbeat,
    classify_launchctl_start_attempt,
    classify_launchctl_runtime_jobs,
    classify_launch_status_convergence,
    classify_paper_config_stack_safety,
    classify_pid_metadata,
    classify_runtime_launch_guard,
    classify_writer_authority,
    runtime_generation_mismatches,
    validate_runtime_truth_contract,
)


NOW = datetime(2026, 5, 21, 12, 0, tzinfo=timezone.utc)


def test_classify_freshness_uses_one_ttl_clock() -> None:
    fresh = classify_freshness(generated_at="2026-05-21T11:58:31+00:00", freshness_ttl_seconds=90, now=NOW)
    stale = classify_freshness(generated_at="2026-05-21T11:58:29+00:00", freshness_ttl_seconds=90, now=NOW)

    assert fresh.freshness_state == FRESHNESS_FRESH
    assert fresh.stale_reason is None
    assert stale.freshness_state == FRESHNESS_STALE
    assert stale.stale_reason == "age_seconds>90"


def test_missing_artifact_and_missing_timestamp_are_distinct() -> None:
    missing_artifact = classify_freshness(generated_at=None, freshness_ttl_seconds=90, now=NOW, artifact_present=False)
    missing_timestamp = classify_freshness(generated_at=None, freshness_ttl_seconds=90, now=NOW, artifact_present=True)

    assert missing_artifact.freshness_state == FRESHNESS_MISSING_ARTIFACT
    assert missing_artifact.stale_reason == "artifact_missing"
    assert missing_timestamp.freshness_state == "MISSING_TIMESTAMP"
    assert missing_timestamp.stale_reason == "timestamp_missing"


def test_heartbeat_prioritizes_root_and_process_before_artifact_freshness() -> None:
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=False,
            command_ok=True,
            freshness_state=FRESHNESS_FRESH,
        )
        == HEARTBEAT_WRONG_ROOT
    )
    assert (
        classify_heartbeat(
            process_running=False,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_FRESH,
        )
        == HEARTBEAT_PROCESS_DOWN
    )
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_STALE,
        )
        == HEARTBEAT_ARTIFACT_STALE
    )
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_MISSING_ARTIFACT,
        )
        == HEARTBEAT_ARTIFACT_MISSING
    )
    assert (
        classify_heartbeat(
            process_running=True,
            root_ok=True,
            command_ok=True,
            freshness_state=FRESHNESS_FRESH,
        )
        == HEARTBEAT_HEALTHY
    )


def test_writer_authority_detects_missing_single_and_duplicate_writers() -> None:
    assert classify_writer_authority([]) == WRITER_MISSING
    assert (
        classify_writer_authority(
            [
                {"process_running": True, "heartbeat_state": HEARTBEAT_HEALTHY},
                {"process_running": False, "heartbeat_state": HEARTBEAT_PROCESS_DOWN},
            ]
        )
        == WRITER_SINGLE
    )
    assert (
        classify_writer_authority(
            [
                {"process_running": True, "heartbeat_state": HEARTBEAT_HEALTHY},
                {"process_running": True, "heartbeat_state": HEARTBEAT_HEALTHY},
            ]
        )
        == WRITER_DUPLICATE
    )


def test_build_runtime_truth_contract_is_paper_only_and_schema_valid() -> None:
    payload = build_runtime_truth_contract(
        runtime_instance_id="track-b-paper-20260521-120000-123",
        service_name="paper_runtime",
        producer_pid=123,
        producer_root="/Users/patrick/Dev/MGC-v05l-automation",
        generated_at=NOW,
        last_success_at=NOW,
        freshness_ttl_seconds=180,
        heartbeat_state=HEARTBEAT_HEALTHY,
        writer_authority=WRITER_SINGLE,
        source_commit="abc123",
        config_fingerprint="sha256:deadbeef",
        restart_generation=2,
    )

    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["runtime_mode"] == "PAPER"
    assert payload["freshness_state"] == FRESHNESS_FRESH
    assert payload["heartbeat_state"] == HEARTBEAT_HEALTHY
    assert validate_runtime_truth_contract(payload) == ()


def test_runtime_truth_contract_can_represent_stale_heartbeat_artifact() -> None:
    payload = build_runtime_truth_contract(
        runtime_instance_id="track-b-paper-20260521-120000-123",
        service_name="paper_runtime",
        producer_pid=123,
        producer_root="/Users/patrick/Dev/MGC-v05l-automation",
        generated_at=NOW,
        last_success_at="2026-05-21T11:56:59+00:00",
        freshness_ttl_seconds=180,
        heartbeat_state=HEARTBEAT_ARTIFACT_STALE,
        writer_authority=WRITER_SINGLE,
    )

    assert payload["freshness_state"] == FRESHNESS_STALE
    assert payload["stale_reason"] == "age_seconds>180"
    assert payload["heartbeat_state"] == HEARTBEAT_ARTIFACT_STALE


def test_runtime_truth_contract_can_represent_wrong_root_writer() -> None:
    payload = build_runtime_truth_contract(
        runtime_instance_id="track-b-paper-20260521-120000-123",
        service_name="paper_runtime",
        producer_pid=123,
        producer_root="/Users/patrick/Documents/MGC-v05l-automation",
        generated_at=NOW,
        last_success_at=NOW,
        freshness_ttl_seconds=180,
        heartbeat_state=HEARTBEAT_WRONG_ROOT,
        writer_authority=WRITER_SINGLE,
    )

    assert payload["producer_root"] == "/Users/patrick/Documents/MGC-v05l-automation"
    assert payload["heartbeat_state"] == HEARTBEAT_WRONG_ROOT
    assert validate_runtime_truth_contract(payload) == ()


def test_validate_runtime_truth_contract_rejects_live_mode() -> None:
    payload = build_runtime_truth_contract(
        runtime_instance_id="track-b-paper-20260521-120000-123",
        service_name="paper_runtime",
        producer_pid=123,
        producer_root="/Users/patrick/Dev/MGC-v05l-automation",
        generated_at=NOW,
        freshness_ttl_seconds=180,
        heartbeat_state=HEARTBEAT_HEALTHY,
        writer_authority=WRITER_SINGLE,
    )
    payload["runtime_mode"] = "LIVE"

    with pytest.raises(ValueError, match="runtime_mode_not_paper"):
        validate_runtime_truth_contract(payload)


def test_pid_metadata_classifier_rejects_missing_stale_dead_zombie_and_wrong_root() -> None:
    metadata = {
        "generated_at": NOW.isoformat(),
        "pid": 123,
        "runtime_instance_id": "runtime-a",
        "restart_generation": 7,
        "root": "/Users/patrick/Dev/MGC-v05l-automation",
    }

    assert classify_pid_metadata(None, now=NOW) == PID_METADATA_MISSING
    assert (
        classify_pid_metadata({**metadata, "generated_at": "2026-05-21T11:55:00+00:00"}, now=NOW)
        == PID_METADATA_STALE
    )
    assert classify_pid_metadata(metadata, now=NOW, process_probe={"running": False}) == PID_PROCESS_DEAD
    assert (
        classify_pid_metadata(metadata, now=NOW, process_probe={"running": True, "zombie": True})
        == PID_PROCESS_ZOMBIE
    )
    assert (
        classify_pid_metadata(
            metadata,
            now=NOW,
            process_probe={"running": True, "zombie": False, "cwd": "/Users/patrick/Documents/MGC-v05l-automation"},
            expected_root="/Users/patrick/Dev/MGC-v05l-automation",
        )
        == PID_WRONG_ROOT
    )
    assert (
        classify_pid_metadata(
            metadata,
            now=NOW,
            process_probe={"running": True, "zombie": False, "cwd": "/Users/patrick/Dev/MGC-v05l-automation"},
            expected_root="/Users/patrick/Dev/MGC-v05l-automation",
        )
        == PID_METADATA_OK
    )


def test_runtime_generation_mismatches_detect_artifact_identity_split() -> None:
    truth = {"runtime_instance_id": "runtime-a", "restart_generation": 2}
    matching = {"runtime_instance_id": "runtime-a", "restart_generation": 2}
    stale_config = {"runtime_instance_id": "runtime-old", "restart_generation": 1}
    stale_operator = {"runtime_instance_id": "runtime-a", "restart_generation": 1}

    assert runtime_generation_mismatches(pid_metadata=matching, runtime_truth=truth, config_in_force=matching) == ()
    assert runtime_generation_mismatches(
        pid_metadata=matching,
        runtime_truth=truth,
        config_in_force=stale_config,
        operator_status=stale_operator,
    ) == ("config_in_force_runtime_truth_mismatch", "operator_status_runtime_truth_mismatch")


def test_launch_guard_accepts_active_matching_runtime() -> None:
    truth = {"runtime_instance_id": "runtime-a", "restart_generation": 2}
    result = classify_runtime_launch_guard(
        pid_metadata_state=PID_METADATA_OK,
        pid_metadata=truth,
        runtime_truth=truth,
        config_in_force=truth,
        operator_status=truth,
        broker_clean=True,
        process_running=True,
    )

    assert result["classification"] == LAUNCH_PID_ACCEPTED
    assert result["active_runtime_accepted"] is True
    assert result["launch_allowed"] is False
    assert result["broker_mutation"] is False


def test_launch_guard_allows_dead_stale_pid_cleanup_only_when_broker_clean() -> None:
    stale = classify_runtime_launch_guard(
        pid_metadata_state=PID_PROCESS_DEAD,
        broker_clean=True,
        process_running=False,
    )
    blocked = classify_runtime_launch_guard(
        pid_metadata_state=PID_PROCESS_DEAD,
        broker_clean=False,
        process_running=False,
    )

    assert stale["classification"] == LAUNCH_STALE_PID_CLEANUP_ALLOWED
    assert stale["cleanup_allowed"] is True
    assert stale["launch_allowed"] is True
    assert blocked["classification"] == LAUNCH_STALE_PID_CLEANUP_BLOCKED
    assert "broker_state_not_clean_for_pid_cleanup" in blocked["blockers"]


def test_launch_guard_blocks_live_wrong_root_runtime() -> None:
    result = classify_runtime_launch_guard(
        pid_metadata_state=PID_WRONG_ROOT,
        broker_clean=True,
        process_running=True,
    )

    assert result["classification"] == LAUNCH_WRONG_ROOT_BLOCKED
    assert result["launch_allowed"] is False
    assert "live_runtime_wrong_root" in result["blockers"]


def test_launch_guard_rejects_zombie_pid() -> None:
    result = classify_runtime_launch_guard(
        pid_metadata_state=PID_PROCESS_ZOMBIE,
        broker_clean=True,
        process_running=False,
    )

    assert result["classification"] == LAUNCH_ZOMBIE_PID_REJECTED
    assert result["cleanup_allowed"] is False
    assert "zombie_runtime_pid" in result["blockers"]


def test_launch_guard_detects_duplicate_and_mismatched_runtime_generation() -> None:
    truth = {"runtime_instance_id": "runtime-a", "restart_generation": 2}
    stale_pid = {"runtime_instance_id": "runtime-old", "restart_generation": 1}
    result = classify_runtime_launch_guard(
        pid_metadata_state=PID_METADATA_OK,
        pid_metadata=stale_pid,
        runtime_truth=truth,
        config_in_force=truth,
        operator_status=truth,
        broker_clean=True,
        process_running=True,
        duplicate_writer_detected=True,
    )

    assert result["classification"] == LAUNCH_CONFLICTING_WRITER_BLOCKED
    assert result["launch_allowed"] is False
    assert result["duplicate_writer_detected"] is True
    assert result["generation_mismatches"] == ["pid_metadata_runtime_truth_mismatch"]
    assert "duplicate_runtime_writer_detected" in result["blockers"]


def test_launchctl_runtime_job_classifier_blocks_stale_loaded_label() -> None:
    result = classify_launchctl_runtime_jobs(
        (
            {
                "label": "com.mgc-v05l.headless-supervised-paper.runtime.20260521120000.111",
                "pid": "-",
                "status": "0",
            },
        ),
        expected_label="com.mgc-v05l.headless-supervised-paper.runtime.20260521120500.222",
    )

    assert result["classification"] == LAUNCHCTL_STALE_RUNTIME_JOB_BLOCKED
    assert result["launch_allowed"] is False
    assert result["stale_runtime_labels"] == [
        "com.mgc-v05l.headless-supervised-paper.runtime.20260521120000.111"
    ]
    assert "stale_launchctl_runtime_job_loaded" in result["blockers"]


def test_launchctl_runtime_job_classifier_accepts_current_loaded_label() -> None:
    label = "com.mgc-v05l.headless-supervised-paper.runtime.20260521120500.222"

    result = classify_launchctl_runtime_jobs(({"label": label, "pid": "1234", "status": "0"},), expected_label=label)

    assert result["classification"] == LAUNCHCTL_RUNTIME_JOB_ACCEPTED
    assert result["launch_allowed"] is True
    assert result["stale_runtime_labels"] == []


def test_launchctl_runtime_job_classifier_ignores_unrelated_jobs() -> None:
    result = classify_launchctl_runtime_jobs(
        (
            {"label": "com.mgc_v05l.track_b_sunday_preflight", "pid": "-", "status": "0"},
            {"label": "com.apple.example", "pid": "1", "status": "0"},
        )
    )

    assert result["classification"] == LAUNCHCTL_NO_RUNTIME_JOB_EVIDENCE
    assert result["launch_allowed"] is True


def test_launchctl_start_attempt_surfaces_submit_failure() -> None:
    result = classify_launchctl_start_attempt(
        launchctl_exit_code=125,
        pid_available=False,
        expected_pid_file="/tmp/runtime/probationary_paper.pid",
    )

    assert result["classification"] == LAUNCHCTL_SUBMIT_FAILED
    assert result["blockers"] == ["launchctl_submit_failed"]
    assert result["broker_mutation"] is False
    assert result["live_money_eligible"] is False


def test_launchctl_start_attempt_surfaces_wrapper_pre_exec_failure() -> None:
    result = classify_launchctl_start_attempt(
        launchctl_exit_code=0,
        pid_available=False,
        wrapper_status={"classification": WRAPPER_PRE_EXEC_FAILURE, "reason": "source_commit_mismatch"},
        expected_pid_file="/tmp/runtime/probationary_paper.pid",
    )

    assert result["classification"] == WRAPPER_PRE_EXEC_FAILURE
    assert result["blockers"] == ["source_commit_mismatch"]
    assert result["submit_authority"] is False


def test_launchctl_start_attempt_detects_pid_write_path_mismatch() -> None:
    result = classify_launchctl_start_attempt(
        launchctl_exit_code=0,
        pid_available=False,
        wrapper_status={
            "classification": "WRAPPER_STARTED",
            "pid_file": "/tmp/wrong/probationary_paper.pid",
        },
        expected_pid_file="/tmp/runtime/probationary_paper.pid",
    )

    assert result["classification"] == PID_WRITE_PATH_MISMATCH
    assert result["observed_pid_file"] == "/tmp/wrong/probationary_paper.pid"
    assert result["expected_pid_file"] == "/tmp/runtime/probationary_paper.pid"


def test_launchctl_start_attempt_accepts_available_runtime_pid() -> None:
    result = classify_launchctl_start_attempt(
        launchctl_exit_code=0,
        pid_available=True,
        wrapper_status={
            "classification": "WRAPPER_EXECING_RUNTIME",
            "pid_file": "/tmp/runtime/probationary_paper.pid",
        },
        expected_pid_file="/tmp/runtime/probationary_paper.pid",
    )

    assert result["classification"] == RUNTIME_PID_AVAILABLE
    assert result["blockers"] == []


def test_launchctl_start_attempt_keeps_pid_timeout_explicit() -> None:
    result = classify_launchctl_start_attempt(
        launchctl_exit_code=0,
        pid_available=False,
        expected_pid_file="/tmp/runtime/probationary_paper.pid",
    )

    assert result["classification"] == RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT
    assert result["blockers"] == ["runtime_pid_unavailable_after_launchctl_submit"]


def test_late_runtime_truth_clears_stale_launch_failure() -> None:
    result = classify_launch_status_convergence(
        launch_status={"classification": RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT},
        pid_metadata={
            "pid": 58328,
            "runtime_instance_id": "runtime-1",
            "restart_generation": 2,
            "root": "/repo",
        },
        runtime_truth={
            "producer_pid": 58328,
            "runtime_instance_id": "runtime-1",
            "restart_generation": 2,
            "producer_root": "/repo",
            "freshness_state": FRESHNESS_FRESH,
            "heartbeat_state": HEARTBEAT_HEALTHY,
            "writer_authority": WRITER_SINGLE,
            "duplicate_writer_detection": {"duplicate_writer_detected": False},
        },
        process_probe={"running": True, "zombie": False, "cwd": "/repo"},
        expected_root="/repo",
    )

    assert result["classification"] == LATE_RUNTIME_CONVERGED
    assert result["original_classification"] == RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT
    assert result["runtime_converged"] is True
    assert result["stale_failure_superseded"] is True
    assert result["blockers"] == []


def test_real_launch_failure_remains_failed_without_runtime_truth() -> None:
    result = classify_launch_status_convergence(
        launch_status={"classification": LAUNCHCTL_SUBMIT_FAILED},
        pid_metadata={},
        runtime_truth={},
        process_probe={"running": False, "zombie": False, "cwd": None},
        expected_root="/repo",
    )

    assert result["classification"] == LAUNCHCTL_SUBMIT_FAILED
    assert result["runtime_converged"] is False
    assert result["stale_failure_superseded"] is False
    assert "runtime_process_not_running" in result["blockers"]


def test_wrong_root_late_pid_does_not_clear_launch_failure() -> None:
    result = classify_launch_status_convergence(
        launch_status={"classification": RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT},
        pid_metadata={"pid": 58328, "runtime_instance_id": "runtime-1", "root": "/wrong"},
        runtime_truth={
            "producer_pid": 58328,
            "runtime_instance_id": "runtime-1",
            "producer_root": "/wrong",
            "freshness_state": FRESHNESS_FRESH,
            "heartbeat_state": HEARTBEAT_HEALTHY,
            "writer_authority": WRITER_SINGLE,
        },
        process_probe={"running": True, "zombie": False, "cwd": "/wrong"},
        expected_root="/repo",
    )

    assert result["classification"] == RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT
    assert result["runtime_converged"] is False
    assert result["stale_failure_superseded"] is False
    assert "runtime_pid_wrong_root" in result["blockers"]


def test_duplicate_writer_late_pid_does_not_clear_launch_failure() -> None:
    result = classify_launch_status_convergence(
        launch_status={"classification": RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT},
        pid_metadata={"pid": 58328, "runtime_instance_id": "runtime-1", "root": "/repo"},
        runtime_truth={
            "producer_pid": 58328,
            "runtime_instance_id": "runtime-1",
            "producer_root": "/repo",
            "freshness_state": FRESHNESS_FRESH,
            "heartbeat_state": HEARTBEAT_HEALTHY,
            "writer_authority": WRITER_DUPLICATE,
            "duplicate_writer_detection": {"duplicate_writer_detected": True},
        },
        process_probe={"running": True, "zombie": False, "cwd": "/repo"},
        expected_root="/repo",
    )

    assert result["classification"] == RUNTIME_PID_UNAVAILABLE_AFTER_LAUNCHCTL_SUBMIT
    assert result["runtime_converged"] is False
    assert result["stale_failure_superseded"] is False
    assert "duplicate_runtime_writer_detected" in result["blockers"]


def test_paper_config_stack_safety_rejects_documents_root_and_no_mule_temp_overlay() -> None:
    result = classify_paper_config_stack_safety(
        (
            "/Users/patrick/Dev/MGC-v05l-automation/config/base.yaml",
            "/Users/patrick/Documents/MGC-v05l-automation/config/no_mule_temp_overlay.yaml",
        ),
        expected_root="/Users/patrick/Dev/MGC-v05l-automation",
    )

    assert result["classification"] == CONFIG_STACK_UNSAFE
    assert result["launch_allowed"] is False
    assert "config_path_deprecated_documents_root" in result["blockers"]
    assert "config_path_outside_expected_root" in result["blockers"]
    assert "config_path_temp_or_no_mule_overlay" in result["blockers"]


def test_paper_config_stack_safety_accepts_dev_root_stack() -> None:
    result = classify_paper_config_stack_safety(
        (
            "/Users/patrick/Dev/MGC-v05l-automation/config/base.yaml",
            "/Users/patrick/Dev/MGC-v05l-automation/config/probationary_pattern_engine_paper_mnq_mgc_plus_mnq_us_intraday_review.yaml",
        ),
        expected_root="/Users/patrick/Dev/MGC-v05l-automation",
    )

    assert result["classification"] == CONFIG_STACK_SAFE
    assert result["launch_allowed"] is True
    assert result["blockers"] == []
