from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_BLOCKED,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    MANAGED_EXIT_ACTUATOR_NOOP,
)
from mgc_v05l.execution_core.track_b_managed_exit_service import (
    MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT,
    MANAGED_EXIT_SERVICE_APPLY_BLOCKED,
    MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED,
    MANAGED_EXIT_SERVICE_BLOCKED,
    MANAGED_EXIT_SERVICE_CYCLE_STARTED,
    MANAGED_EXIT_SERVICE_DRY_RUN_READY,
    MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS,
    MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE,
    MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED,
    MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED,
    TrackBManagedExitServiceConfig,
    _build_pipeline_execution_plan,
    _modify_config_from_order_plan,
    read_track_b_managed_exit_service_status,
    run_track_b_managed_exit_service,
    run_track_b_managed_exit_service_once,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT


NOW = datetime(2026, 6, 8, 15, 5, tzinfo=UTC)


def test_pipeline_execution_plan_splits_v1_allowed_degraded_and_blocked(tmp_path: Path) -> None:
    plan = _build_pipeline_execution_plan(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED", "DEGRADED_ALLOWED", "BLOCKED")
        ),
    )

    assert plan["classification"] == "EXIT_INTENT_ALLOWED"
    assert [row["decision"] for row in plan["executable_intents"]] == ["ALLOWED", "DEGRADED_ALLOWED"]
    assert [row["decision"] for row in plan["blocked_intents"]] == ["BLOCKED"]
    assert len(plan["exit_intents"]) == 3
    assert len(plan["authority_decisions"]) == 3


def test_service_dry_run_uses_v1_allowed_plan_without_apply(tmp_path: Path) -> None:
    actuator_calls = []
    call_order = []

    def _actuator(config, now, timeout):
        call_order.append("actuator")
        actuator_calls.append(config)
        return _actuator_report(MANAGED_EXIT_ACTUATOR_DRY_RUN_READY, eligible=1)

    def _refresh(config, phase):
        call_order.append(f"refresh:{phase}")
        return _refresh_ok(config, phase)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["pipeline_classification"] == "EXIT_INTENT_ALLOWED"
    assert payload["executable_exit_intent_ids"] == ["exit-mes"]
    assert payload["entry_allowed"] is False
    assert payload["apply_requested"] is False
    assert actuator_calls[0].apply is False
    assert actuator_calls[0].max_closes_per_run == 1
    assert call_order == ["actuator"]


def test_apply_service_processes_v1_executable_intents_one_at_a_time_with_refresh_between(tmp_path: Path) -> None:
    classifications = [
        MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
        MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
        MANAGED_EXIT_ACTUATOR_NOOP,
    ]
    refresh_phases = []
    actuator_calls = []

    def _actuator(config, now, timeout):
        index = len(actuator_calls)
        actuator_calls.append(config)
        submitted = 1 if classifications[index] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING else 0
        return _actuator_report(
            classifications[index],
            eligible=max(0, 2 - index),
            submitted=submitted,
            local_symbol=("MESM6", "MNQM6", None)[index],
        )

    def _refresh(config, phase):
        refresh_phases.append(phase)
        return _refresh_ok(config, phase)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            apply=True,
            operator_authorized_managed_exit=False,
            max_cycles_per_tick=3,
        ),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED", "ALLOWED", "ALLOWED")),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["submitted_count"] == 2
    assert payload["operator_authorized_managed_exit"] is False
    assert [call.apply for call in actuator_calls] == [True, True, True]
    assert [call.operator_authorized_managed_exit for call in actuator_calls] == [True, True, True]
    assert [call.max_closes_per_run for call in actuator_calls] == [1, 1, 1]
    assert refresh_phases == []


def test_apply_service_treats_stale_publication_as_diagnostic_when_v11_broker_risk_is_clear(tmp_path: Path) -> None:
    actuator_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: actuator_calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, eligible=1, submitted=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "open_order_truth": "ORDER_TRUTH_STALE",
                "managed_positions": "LIFECYCLE_WITHOUT_BROKER",
                "managed_orders": "POSITION_WITHOUT_CLOSE_ORDER",
                "reconciliation": "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
            },
        ),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["submitted_count"] == 1
    assert len(actuator_calls) == 1
    diagnostics = payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["diagnostics"]
    assert {row["kind"] for row in diagnostics} >= {
        "diagnostic_open_order_truth_classification",
        "diagnostic_managed_position_classification",
    }


def test_service_preserves_actuator_close_quantity_for_v1_plan(tmp_path: Path) -> None:
    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {
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
        },
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            intents=[{"exit_intent_id": "exit-mes", "localSymbol": "MESM6", "close_action": "SELL", "close_qty": 3}],
        ),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["attempted_closes"][0]["close_candidate"]["quantity"] == 3
    assert payload["execution_plan"]["executable_intents"][0]["close_qty"] == 3


def test_refresh_failure_is_diagnostic_for_v1_allowed_paper_risk_reducing_close(tmp_path: Path) -> None:
    calls = []
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, eligible=2, submitted=1, local_symbol="MESM6"),
        authority_refresher=lambda config, phase: refresh_calls.append(phase)
        or {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED",
            "dependency_refresh_failures": [
                {"step": "control_plane_snapshot", "code": "control_plane_snapshot_refresh_failed", "returncode": 124}
            ],
        },
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "control_plane": "CONTROL_PLANE_SNAPSHOT_BLOCKED",
                "broker_session_authority": "BROKER_SESSION_AUTHORITY_DEGRADED_RECOVERED",
            },
        ),
        write=False,
    )

    assert calls
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert refresh_calls == []
    assert payload["authority_refresh_failed"] is False
    assert payload["authority_refresh_degraded_actuator_attempted"] is False
    assert payload["service_diagnostics"][0]["code"] == "LEGACY_OPERATOR_READINESS_REFRESH_DIAGNOSTIC_ONLY"
    assert (
        payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["classification"]
        == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED
    )
    assert payload["pipeline_diagnostics"][0]["kind"] == "legacy_source_classifications"
    assert payload["submitted_count"] == 1
    assert payload["submit_attempted"] is True


def test_managed_close_fill_triggers_post_broker_mutation_refresh(tmp_path: Path) -> None:
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: _actuator_report(
            MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
            eligible=1,
            submitted=1,
            local_symbol="MESM6",
        ),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        post_mutation_refresher=lambda **kwargs: refresh_calls.append(kwargs)
        or {"classification": "POST_BROKER_MUTATION_REFRESH_SUCCEEDED", "trigger": kwargs["trigger"]},
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["broker_state_mutated"] is True
    assert payload["post_broker_mutation_refresh"]["classification"] == "POST_BROKER_MUTATION_REFRESH_SUCCEEDED"
    assert payload["post_broker_mutation_refresh"]["trigger"] == "managed_exit_service_actuator"
    assert refresh_calls[0]["mutation_report"]["broker_state_mutated"] is True


def test_v1_allowed_close_with_real_broker_risk_blocker_stops_before_actuator(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True, max_cycles_per_tick=1),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, eligible=1, submitted=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            failed_hard_checks=("same_contract_working_close_does_not_over_close",),
        ),
        write=False,
    )

    assert calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert payload["service_diagnostics"][0]["classification"] == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED
    assert "same_contract_working_close_over_close_risk" in payload["service_diagnostics"][0]["blockers"]


def test_v1_blocked_plan_does_not_invoke_actuator(tmp_path: Path) -> None:
    calls = []
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=lambda config, phase: refresh_calls.append(phase) or _refresh_ok(config, phase),
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("BLOCKED",), block_reasons=("over_close_risk",)),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_BLOCKED
    assert payload["executable_intent_count"] == 0
    assert payload["blocked_exit_intent_ids"] == ["exit-mes"]
    assert payload["required_next_action"] == "OPERATOR_REVIEW_REQUIRED"
    assert calls == []
    assert refresh_calls == []


def test_pipeline_unavailable_does_not_invoke_actuator(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(pipeline_errors=({"code": "position_state_unavailable"},)),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE
    assert payload["required_next_action"] == "REPAIR_MANAGED_EXIT_PIPELINE"
    assert payload["pipeline_diagnostics"][0]["kind"] == "pipeline_errors"
    assert calls == []


def test_legacy_bsa_false_is_diagnostic_when_v1_allows_broker_scoped_exit(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: calls.append(config)
        or _actuator_report(MANAGED_EXIT_ACTUATOR_DRY_RUN_READY, eligible=1),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(
            decisions=("ALLOWED",),
            source_classifications={
                "broker_session_authority": "managed_risk_reducing_close=false",
                "control_plane": "CONTROL_PLANE_SNAPSHOT_STALE_OR_MIXED",
                "managed_registry": "historical_review_debris",
            },
        ),
        write=False,
    )

    assert calls
    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["pipeline_diagnostics"][0]["kind"] == "legacy_source_classifications"
    assert "close_authority_snapshots" not in payload


def test_service_loop_writes_status_and_heartbeat_without_starting_entries(tmp_path: Path) -> None:
    status_path = tmp_path / "latest_service_status.json"
    heartbeat_path = tmp_path / "heartbeat.json"
    sleeps = []

    run_track_b_managed_exit_service(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            heartbeat_path=heartbeat_path,
            cadence_seconds=30,
            apply=True,
        ),
        actuator_runner=lambda config, now, timeout: _actuator_report(MANAGED_EXIT_ACTUATOR_BLOCKED, eligible=0),
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        sleep_func=lambda seconds: sleeps.append(seconds),
        max_iterations=1,
    )

    status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
    assert status["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert status["close_only"] is True
    assert status["entry_allowed"] is False
    assert status["live_money_eligible"] is False
    assert status["paper_proof_invoked"] is False
    assert heartbeat_path.exists()
    assert sleeps == []


def test_service_writes_cycle_started_status_before_actuator_call(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"

    def _actuator(config, now, timeout):
        status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
        assert status["classification"] == MANAGED_EXIT_SERVICE_CYCLE_STARTED
        assert status["pid"] > 0
        assert status["service_label"] == "unit-test-service"
        assert status["apply_mode"] == "GUARDED_CLOSE_ONLY_APPLY"
        assert status["max_closes_per_run"] == 1
        assert status["detected_candidates_count"] is None
        assert status["candidate_detection"] == "deferred_to_v1_pipeline_execution_plan"
        return _actuator_report(MANAGED_EXIT_ACTUATOR_NOOP, eligible=0)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            apply=True,
            service_label="unit-test-service",
        ),
        now=NOW,
        actuator_runner=_actuator,
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED",)),
        write=True,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    assert payload["phase_timings"][0]["phase"] == "pipeline_candidate_discovery"


def test_actuator_timeout_produces_terminal_status_without_further_apply(tmp_path: Path) -> None:
    status_path = tmp_path / "status.json"
    calls = []

    def _timeout_command(command, repo_root, timeout):
        calls.append(command)
        return subprocess.CompletedProcess(command, 124, stdout="", stderr="timed out")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(
            repo_root=tmp_path,
            status_path=status_path,
            apply=True,
            max_cycles_per_tick=3,
            actuator_timeout_seconds=0.01,
        ),
        now=NOW,
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=("ALLOWED", "ALLOWED", "ALLOWED")),
        command_runner=_timeout_command,
        write=True,
    )

    status = read_track_b_managed_exit_service_status(repo_root=tmp_path, status_path=status_path)
    assert payload["classification"] == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT
    assert status["classification"] == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT
    assert payload["submit_attempted"] is False
    assert payload["submitted_count"] == 0
    assert len(calls) == 1
    assert "--apply" in calls[0]
    assert "--operator-authorized-managed-exit" in calls[0]


def test_no_candidates_produces_no_eligible_exits(tmp_path: Path) -> None:
    def _refresh_should_not_run(config, phase):
        raise AssertionError("HOLD/no-candidate cycles must not refresh operator authority")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=lambda config, now, timeout: _actuator_report(MANAGED_EXIT_ACTUATOR_NOOP, eligible=0),
        authority_refresher=_refresh_should_not_run,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="NO_POSITIONS"),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    assert payload["required_next_action"] == "NO_ACTION"
    assert payload["actuator_invocation_count"] == 0
    assert payload["authority_refreshes"] == []


def test_hold_only_cycle_skips_slow_authority_refresh_and_actuator(tmp_path: Path) -> None:
    def _refresh_should_not_run(config, phase):
        raise AssertionError("HOLD_ONLY cycle reached slow authority refresh")

    def _actuator_should_not_run(config, now, timeout):
        raise AssertionError("HOLD_ONLY cycle reached actuator")

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=_actuator_should_not_run,
        authority_refresher=_refresh_should_not_run,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    assert payload["pipeline_classification"] == "HOLD_ONLY"
    assert payload["authority_refreshes"] == []
    assert payload["actuator_invocation_count"] == 0
    assert payload["managed_order_maintenance_invocation_count"] == 0


def test_no_exit_intents_invokes_working_close_order_maintenance(tmp_path: Path) -> None:
    maintenance_calls = []
    actuator_calls = []
    _write_managed_close_order_registry(tmp_path)

    def _maintenance(config, now, timeout):
        maintenance_calls.append((config, timeout))
        return {
            "classification": "MANAGED_ORDER_MAINTENANCE_DRY_RUN_READY",
            "broker_mutation_attempted": False,
            "broker_mutation_performed": False,
            "results": [{"broker_order_id": "91", "classification": "MODIFY_IN_PLACE_DRY_RUN_READY"}],
        }

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path),
        now=NOW,
        actuator_runner=lambda config, now, timeout: actuator_calls.append(config) or {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=_maintenance,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_DRY_RUN_READY
    assert payload["actuator_invocation_count"] == 0
    assert len(maintenance_calls) == 1
    assert payload["managed_order_maintenance_invocation_count"] == 1
    assert payload["latest_managed_order_maintenance_classification"] == "MANAGED_ORDER_MAINTENANCE_DRY_RUN_READY"
    assert payload["broker_state_mutated"] is False


def test_apply_service_reports_working_close_order_maintenance_mutation(tmp_path: Path) -> None:
    _write_managed_close_order_registry(tmp_path)

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
            "results": [
                {
                    "broker_order_id": "91",
                    "classification": "MODIFY_IN_PLACE_APPLIED",
                    "current_known_limit": "7388.0",
                    "new_limit": "7378.0",
                }
            ],
        },
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["actuator_invocation_count"] == 0
    assert payload["broker_state_mutated"] is True
    assert payload["managed_order_maintenance_mutation_attempted"] is True
    assert payload["managed_order_maintenance_mutation_performed"] is True


def test_working_close_order_maintenance_refresh_timeout_blocks_before_mutation(tmp_path: Path) -> None:
    _write_managed_close_order_registry(tmp_path)
    path = tmp_path / DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["managed_orders"][0]["account_id"] = "WRONG"
    path.write_text(json.dumps(payload), encoding="utf-8")
    maintenance_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=lambda config, phase: {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_TIMEOUT",
        },
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: maintenance_calls.append(config) or {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
        },
        write=False,
    )

    assert maintenance_calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    assert payload["service_diagnostics"][0]["classification"] == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED
    assert "wrong_account" in payload["service_diagnostics"][0]["blockers"]
    assert payload["managed_order_maintenance_invocation_count"] == 0
    assert payload["managed_order_maintenance_mutation_attempted"] is False


def test_working_close_order_maintenance_refresh_timeout_is_diagnostic_when_close_order_is_risk_reducing(
    tmp_path: Path,
) -> None:
    _write_managed_close_order_registry(tmp_path)
    maintenance_calls = []
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=lambda config, phase: refresh_calls.append(phase) or {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_TIMEOUT",
        },
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: maintenance_calls.append(config) or {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
        },
        write=False,
    )

    assert len(maintenance_calls) == 1
    assert refresh_calls == []
    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["service_diagnostics"][0]["managed_paper_risk_reducing_exit_authority"]["classification"] == (
        MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED
    )
    assert payload["managed_order_maintenance_mutation_performed"] is True


def test_managed_order_maintenance_mutation_triggers_post_broker_mutation_refresh(tmp_path: Path) -> None:
    _write_managed_close_order_registry(tmp_path)
    refresh_calls = []

    payload = run_track_b_managed_exit_service_once(
        config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        now=NOW,
        actuator_runner=lambda config, now, timeout: {},
        authority_refresher=_refresh_ok,
        pipeline_builder=lambda config, now: _pipeline_report(decisions=(), classification="HOLD_ONLY"),
        order_maintenance_runner=lambda config, now, timeout: {
            "classification": "MANAGED_ORDER_MAINTENANCE_APPLIED",
            "broker_mutation_attempted": True,
            "broker_mutation_performed": True,
        },
        post_mutation_refresher=lambda **kwargs: refresh_calls.append(kwargs)
        or {"classification": "POST_BROKER_MUTATION_REFRESH_DEGRADED", "trigger": kwargs["trigger"]},
    )

    assert payload["classification"] == MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    assert payload["managed_order_maintenance_mutation_performed"] is True
    assert payload["post_broker_mutation_refresh"]["classification"] == "POST_BROKER_MUTATION_REFRESH_DEGRADED"
    assert payload["post_broker_mutation_refresh"]["trigger"] == "managed_exit_service_order_maintenance"
    assert len(refresh_calls) == 1


def test_modify_config_uses_known_order_owner_client_id(tmp_path: Path) -> None:
    config = _modify_config_from_order_plan(
        service_config=TrackBManagedExitServiceConfig(repo_root=tmp_path, apply=True),
        order={
            "broker_order_id": "91",
            "perm_id": 68652733,
            "client_id": 17086,
            "account_id": "DUM882026",
            "symbol": "MES",
            "contract": "MESM6",
            "con_id": 770561194,
            "action": "SELL",
            "quantity": "1",
            "limit_price": "7388.0",
        },
        plan={
            "classification": "MODIFY_IN_PLACE_ELIGIBLE",
            "managed_close_reprice_policy": {"limit_price": "7374.25"},
        },
        timeout_seconds=30.0,
    )

    assert config is not None
    assert config.tws_client_id == 17086
    assert config.broker_order_id == "91"
    assert config.new_limit == "7374.25"


def _refresh_ok(config, phase):
    return {
        "phase": phase,
        "succeeded": True,
        "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_READY",
        "generated_at": NOW.isoformat(),
        "dependency_refresh_failures": [],
    }


def _actuator_report(classification: str, *, eligible: int, submitted: int = 0, local_symbol: str | None = None) -> dict:
    return {
        "classification": classification,
        "exit_due_count": eligible,
        "eligible_count": eligible,
        "submitted_count": submitted,
        "submit_attempted": submitted > 0,
        "broker_state_mutated": submitted > 0,
        "attempted_closes": []
        if local_symbol is None
        else [{"identity": {"local_symbol": local_symbol}, "submit_attempted": submitted > 0}],
    }


def _write_managed_close_order_registry(repo_root: Path) -> None:
    path = repo_root / DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
                "managed_orders": [
                    {
                        "classification": "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
                        "is_close_order": True,
                        "broker_order_id": "91",
                        "perm_id": 68652733,
                        "client_id": 17086,
                        "account_id": "DUM882026",
                        "symbol": "MES",
                        "contract": "MESM6",
                        "con_id": 770561194,
                        "action": "SELL",
                        "quantity": "1",
                        "limit_price": "7388.0",
                        "lifecycle_id": "life-mes",
                        "broker_position": {"quantity": "1", "local_symbol": "MESM6"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def _pipeline_report(
    *,
    decisions: tuple[str, ...] = ("ALLOWED",),
    classification: str = "EXIT_INTENT_ALLOWED",
    intents: list[dict[str, Any]] | None = None,
    block_reasons: tuple[str, ...] = (),
    source_classifications: dict[str, str] | None = None,
    pipeline_errors: tuple[dict[str, Any], ...] = (),
    pipeline_blockers: tuple[dict[str, Any], ...] = (),
    failed_hard_checks: tuple[str, ...] = (),
    failed_conditional_checks: tuple[str, ...] = (),
) -> dict[str, Any]:
    default_intents = [
        {
            "exit_intent_id": "exit-mes",
            "localSymbol": "MESM6",
            "close_action": "BUY",
            "close_qty": 1,
            "execution_domain": "TRACK_B_PAPER",
            "account": "DUM882026",
            "account_id": "DUM882026",
            "position_side": "SHORT",
            "owned_qty": "1",
            "remaining_qty_after": "0",
            "source_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_reason": "timebox_exit_due",
            "lifecycle_id": "life-mes",
            "trade_id": "trade-mes",
            "strategy_id": "strategy-mes",
            "lane_id": "lane-mes",
            "live_money_eligible": False,
            "live_money_allowed": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "attribution": {
                "lifecycle_id": "life-mes",
                "trade_id": "trade-mes",
                "strategy_id": "strategy-mes",
                "lane_id": "lane-mes",
            },
        },
        {
            "exit_intent_id": "exit-mnq",
            "localSymbol": "MNQM6",
            "close_action": "BUY",
            "close_qty": 1,
            "execution_domain": "TRACK_B_PAPER",
            "account": "DUM882026",
            "account_id": "DUM882026",
            "position_side": "SHORT",
            "owned_qty": "1",
            "remaining_qty_after": "0",
            "source_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_reason": "timebox_exit_due",
            "lifecycle_id": "life-mnq",
            "trade_id": "trade-mnq",
            "live_money_eligible": False,
            "live_money_allowed": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "attribution": {"lifecycle_id": "life-mnq", "trade_id": "trade-mnq"},
        },
        {
            "exit_intent_id": "exit-mgc",
            "localSymbol": "MGCM6",
            "close_action": "SELL",
            "close_qty": 1,
            "execution_domain": "TRACK_B_PAPER",
            "account": "DUM882026",
            "account_id": "DUM882026",
            "position_side": "LONG",
            "owned_qty": "1",
            "remaining_qty_after": "0",
            "source_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
            "exit_reason": "timebox_exit_due",
            "lifecycle_id": "life-mgc",
            "trade_id": "trade-mgc",
            "live_money_eligible": False,
            "live_money_allowed": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "attribution": {"lifecycle_id": "life-mgc", "trade_id": "trade-mgc"},
        },
    ]
    if intents is not None:
        selected_intents = [{**default_intents[index], **intent} for index, intent in enumerate(intents)]
    else:
        selected_intents = default_intents
    selected_intents = selected_intents[: len(decisions)]
    authority_decisions = []
    for intent, decision in zip(selected_intents, decisions):
        authority_decisions.append(
            {
                "exit_intent_id": intent["exit_intent_id"],
                "decision": decision,
                "block_reasons": list(block_reasons if decision == "BLOCKED" else ()),
                "diagnostics": [],
                "authority_decision": {
                    "decision": decision,
                    "block_reasons": list(block_reasons if decision == "BLOCKED" else ()),
                    "account_id": intent.get("account_id") or intent.get("account"),
                    "execution_domain": intent.get("execution_domain"),
                    "live_money_eligible": intent.get("live_money_eligible") is True,
                    "paper_proof_invoked": intent.get("paper_proof_invoked") is True,
                    "hard_required_checks": _authority_hard_checks(failed_hard_checks),
                    "conditional_risk_checks": _authority_conditional_checks(failed_conditional_checks),
                },
            }
        )
    return {
        "classification": classification,
        "generated_exit_intents": selected_intents,
        "exit_authority_decisions": authority_decisions,
        "pipeline_errors": list(pipeline_errors),
        "pipeline_blockers": list(pipeline_blockers),
        "source_classifications": source_classifications
        or {
            "open_order_truth": "NO_OPEN_ORDERS",
            "managed_positions": "OPEN_MANAGED_EXIT_DUE" if decisions else "OPEN_MANAGED_MATCHED",
            "managed_orders": "POSITION_WITHOUT_CLOSE_ORDER" if decisions else "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
            "reconciliation": "TRACK_B_PAPER_BROKER_RECONCILED",
        },
    }


def _authority_hard_checks(failed: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    names = (
        "known_current_broker_position",
        "account_matches",
        "execution_domain_matches",
        "contract_matches",
        "close_qty_within_broker_position",
        "risk_reducing_action",
        "same_contract_working_close_does_not_over_close",
        "safe_state_no_hard_halt",
        "live_money_domain_allowed",
        "paper_proof_not_invoked",
        "broad_or_global_flatten_not_requested",
    )
    return {name: {"passed": name not in failed} for name in names}


def _authority_conditional_checks(failed: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    names = ("same_contract_unknown_order_risk",)
    return {name: {"passed": name not in failed} for name in names}
