import json
import os
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_detached_runtime_monitor import (
    build_detached_runtime_child_status,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")


def test_clean_bounded_exit_after_runtime_cycle_is_not_ready_service(tmp_path: Path) -> None:
    pid = 999991
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    log = tmp_path / "runtime.log"
    status = tmp_path / "child_status.json"
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:00+00:00"})
    _write_json(
        progress,
        {
            "producer_pid": pid,
            "generated_at": "2026-06-16T12:00:05+00:00",
            "stage": "runtime_cycle",
            "state": "COMPLETED",
        },
    )
    log.write_text('{"reconciliation_clean": true, "stop_reason": null}\n', encoding="utf-8")

    payload = build_detached_runtime_child_status(
        event="exited",
        status_path=status,
        pid=pid,
        exit_code=0,
        log_file=log,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
        observed_at=datetime(2026, 6, 16, 12, 0, tzinfo=UTC),
    )

    assert payload["classification"] == "RUNTIME_CLEAN_EXIT_AFTER_CYCLE"
    assert payload["child_final_status"] == "EXITED"
    assert payload["child_exit_code"] == 0
    assert payload["child_exit_signal"] is None
    assert payload["runtime_cycle_completed"] is True
    assert payload["submit_authority"] is False
    assert json.loads(status.read_text(encoding="utf-8"))["classification"] == payload["classification"]


def test_exit_before_runtime_cycle_is_blocked_with_exit_code(tmp_path: Path) -> None:
    pid = 999992
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    status = tmp_path / "child_status.json"
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:00+00:00"})
    _write_json(progress, {"producer_pid": pid, "stage": "lane_restore", "state": "COMPLETED"})

    payload = build_detached_runtime_child_status(
        event="exited",
        status_path=status,
        pid=pid,
        exit_code=1,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
    )

    assert payload["classification"] == "RUNTIME_EXITED_BEFORE_DURABLE_READY"
    assert payload["child_exit_code"] == 1
    assert payload["last_runtime_cycle_marker"] is None
    assert payload["termination_reason"] == "runtime_exited_after_initial_truth"


def test_running_child_with_runtime_cycle_marker_is_observable(tmp_path: Path) -> None:
    pid = os.getpid()
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    status = tmp_path / "child_status.json"
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:00+00:00"})
    _write_json(progress, {"producer_pid": pid, "stage": "runtime_cycle", "state": "STARTED"})

    payload = build_detached_runtime_child_status(
        event="heartbeat",
        status_path=status,
        pid=pid,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
    )

    assert payload["classification"] == "RUNTIME_CHILD_RUNNING_CYCLE_OBSERVED"
    assert payload["process_alive"] is True
    assert payload["runtime_cycle_marker_observed"] is True
    assert payload["last_runtime_cycle_marker"]["submit_authority"] is False


def test_running_child_records_trading_loop_submit_authority_marker(tmp_path: Path) -> None:
    pid = os.getpid()
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    status = tmp_path / "child_status.json"
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:00+00:00"})
    _write_json(
        progress,
        {
            "producer_pid": pid,
            "stage": "runtime_cycle",
            "state": "TRADING_LOOP_ENTERED",
            "submit_authority": True,
            "broker_mutation_allowed": True,
        },
    )

    payload = build_detached_runtime_child_status(
        event="heartbeat",
        status_path=status,
        pid=pid,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
    )

    assert payload["classification"] == "RUNTIME_CHILD_RUNNING_CYCLE_OBSERVED"
    assert payload["last_runtime_cycle_marker"]["state"] == "TRADING_LOOP_ENTERED"
    assert payload["last_runtime_cycle_marker"]["submit_authority"] is True
    assert payload["last_runtime_cycle_marker"]["broker_mutation_allowed"] is True


def test_signal_exit_records_signal_without_ready_classification(tmp_path: Path) -> None:
    pid = 999993
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    status = tmp_path / "child_status.json"
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:00+00:00"})
    _write_json(
        progress,
        {
            "producer_pid": pid,
            "generated_at": "2026-06-16T12:00:05+00:00",
            "stage": "runtime_cycle",
            "state": "STARTED",
        },
    )

    payload = build_detached_runtime_child_status(
        event="exited",
        status_path=status,
        pid=pid,
        exit_code=143,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
        child_command="python -m mgc_v05l.app.main probationary-paper-soak",
        parent_pid=12345,
    )

    assert payload["classification"] == "RUNTIME_CHILD_SIGNALED_AFTER_CYCLE_MARKER"
    assert payload["child_final_status"] == "SIGNALED"
    assert payload["child_exit_code"] == 143
    assert payload["child_exit_signal"] == 15
    assert payload["child_command"] == "python -m mgc_v05l.app.main probationary-paper-soak"
    assert payload["supervisor_parent_pid"] == 12345
    assert payload["termination_reason"] == "signal_15"


def test_completed_cycle_without_newer_truth_is_not_durable_ready(tmp_path: Path) -> None:
    pid = os.getpid()
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    status = tmp_path / "child_status.json"
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:00+00:00"})
    _write_json(
        progress,
        {
            "producer_pid": pid,
            "generated_at": "2026-06-16T12:00:01+00:00",
            "stage": "runtime_cycle",
            "state": "COMPLETED",
        },
    )

    payload = build_detached_runtime_child_status(
        event="heartbeat",
        status_path=status,
        pid=pid,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
    )

    assert payload["classification"] == "RUNTIME_CHILD_CYCLE_COMPLETED_WAITING_FOR_NEXT_TRUTH"
    assert payload["child_final_status"] == "RUNNING"
    assert payload["runtime_cycle_completed"] is True


def test_heartbeat_preserves_parent_owned_metadata(tmp_path: Path) -> None:
    pid = os.getpid()
    truth = tmp_path / "truth.json"
    progress = tmp_path / "progress.json"
    status = tmp_path / "child_status.json"
    _write_json(
        status,
        {
            "child_started_at": "2026-06-16T12:00:00Z",
            "child_command": "bash scripts/run_probationary_paper_soak.sh",
            "supervisor_parent_pid": 24680,
        },
    )
    _write_json(truth, {"producer_pid": pid, "generated_at": "2026-06-16T12:00:01+00:00"})
    _write_json(progress, {"producer_pid": pid, "stage": "runtime_cycle", "state": "STARTED"})

    payload = build_detached_runtime_child_status(
        event="heartbeat",
        status_path=status,
        pid=pid,
        runtime_truth_file=truth,
        post_truth_progress_file=progress,
    )

    assert payload["child_started_at"] == "2026-06-16T12:00:00Z"
    assert payload["child_command"] == "bash scripts/run_probationary_paper_soak.sh"
    assert payload["supervisor_parent_pid"] == 24680
