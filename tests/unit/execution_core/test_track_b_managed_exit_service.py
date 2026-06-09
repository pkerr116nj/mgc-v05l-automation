from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_BLOCKED,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    MANAGED_EXIT_ACTUATOR_NOOP,
)
from mgc_v05l.execution_core.track_b_managed_exit_service import (
    MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING,
    MANAGED_EXIT_SERVICE_DRY_RUN_READY,
    MANAGED_EXIT_SERVICE_REFRESH_FAILED,
    TrackBManagedExitServiceConfig,
    read_track_b_managed_exit_service_status,
    run_track_b_managed_exit_service,
    run_track_b_managed_exit_service_once,
)


NOW = datetime(2026, 6, 8, 15, 5, tzinfo=UTC)


def test_service_dry_run_detects_exit_due_without_apply(tmp_path: Path) -> None:
    actuator_calls = []

    def _actuator(config, now):
        actuator_calls.append(config)
        return {
            "classification": MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
            "exit_due_count": 1,
            "eligible_count": 1,
            "submitted_count": 0,
            "submit_attempted": False,
            "attempted_closes": [],
        }

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh_ok,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["entry_allowed"] is False
    assert payload["apply_requested"] is False
    assert payload["authority_inputs"] == [
        "managed_positions",
        "managed_orders",
        "reconciliation",
        "open_order_truth",
        "broker_session_authority",
        "safe_state",
        "guardian",
    ]
    assert actuator_calls[0].apply is False
    assert actuator_calls[0].max_closes_per_run == 1


def test_apply_service_processes_multiple_positions_one_at_a_time_with_refresh_between(tmp_path: Path) -> None:
    classifications = [
        MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
        MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
        MANAGED_EXIT_ACTUATOR_NOOP,
    ]
    local_symbols = ["MESM6", "MNQM6", None]
    refresh_phases = []

    def _actuator(config, now):
        index = len(actuator_calls)
        actuator_calls.append(config)
        submitted = 1 if classifications[index] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING else 0
        return {
            "classification": classifications[index],
            "exit_due_count": max(0, 2 - index),
            "eligible_count": max(0, 2 - index),
            "submitted_count": submitted,
            "submit_attempted": submitted > 0,
            "broker_state_mutated": submitted > 0,
            "attempted_closes": []
            if local_symbols[index] is None
            else [{"identity": {"local_symbol": local_symbols[index]}, "submit_attempted": True}],
        }

    def _refresh(config, phase):
        refresh_phases.append(phase)
        return _refresh_ok(config, phase)

    actuator_calls = []
    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=True,
            max_cycles_per_tick=3,
        ),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING
    assert payload["submitted_count"] == 2
    assert [call.max_closes_per_run for call in actuator_calls] == [1, 1, 1]
    assert refresh_phases == ["before_actuator", "after_actuator_attempt", "after_actuator_attempt"]


def test_service_preserves_full_position_quantity_inside_single_lifecycle_close(tmp_path: Path) -> None:
    quantities = []

    def _actuator(config, now):
        quantities.append(3)
        return {
            "classification": MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            "exit_due_count": 1,
            "eligible_count": 1,
            "submitted_count": 1,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "attempted_closes": [
                {
                    "identity": {"local_symbol": "MESM6", "lifecycle_id": "life-mes-3"},
                    "close_candidate": {"action": "SELL", "quantity": 3, "local_symbol": "MESM6"},
                    "submit_attempted": True,
                }
            ],
        }

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=True,
            max_cycles_per_tick=1,
        ),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh_ok,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING
    assert payload["attempted_closes"][0]["close_candidate"]["quantity"] == 3
    assert quantities == [3]


def test_pre_apply_authority_refresh_failure_blocks_actuator(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        actuator_runner=lambda config, now: calls.append(config) or {},
        authority_refresher=lambda config, phase: {
            "phase": phase,
            "succeeded": False,
            "classification": "AUTHORITY_REFRESH_FAILED",
        },
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_REFRESH_FAILED
    assert payload["submit_attempted"] is False
    assert payload["entry_allowed"] is False
    assert calls == []


def test_service_loop_writes_status_and_heartbeat_without_starting_entries(tmp_path: Path) -> None:
    status_path = tmp_path / "latest_service_status.json"
    heartbeat_path = tmp_path / "heartbeat.json"
    sleeps = []

    payloads = [
        {
            "classification": MANAGED_EXIT_ACTUATOR_BLOCKED,
            "exit_due_count": 1,
            "eligible_count": 0,
            "submitted_count": 0,
            "submit_attempted": False,
            "attempted_closes": [],
        }
    ]

    run_track_b_managed_exit_service(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            heartbeat_path=heartbeat_path,
            cadence_seconds=30,
            apply=True,
            operator_authorized_managed_exit=True,
        ),
        actuator_runner=lambda config, now: payloads[0],
        authority_refresher=_refresh_ok,
        sleep_func=lambda seconds: sleeps.append(seconds),
        max_iterations=1,
    )

    status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
    assert status["classification"] == "TRACK_B_MANAGED_EXIT_SERVICE_BLOCKED"
    assert status["close_only"] is True
    assert status["entry_allowed"] is False
    assert status["live_money_eligible"] is False
    assert status["paper_proof_invoked"] is False
    assert heartbeat_path.exists()
    assert sleeps == []


def _refresh_ok(config, phase):
    return {
        "phase": phase,
        "succeeded": True,
        "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
        "generated_at": NOW.isoformat(),
        "dependency_refresh_failures": [],
    }
