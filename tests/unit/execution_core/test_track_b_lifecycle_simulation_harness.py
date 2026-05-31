from __future__ import annotations

from pathlib import Path

from mgc_v05l.execution_core.track_b_lifecycle_simulation_harness import (
    LIFECYCLE_SIMULATION_BLOCKED,
    LIFECYCLE_SIMULATION_CANCELLED,
    LIFECYCLE_SIMULATION_PASSED,
    REASON_AGGREGATE_ACCOUNT_DIAGNOSTIC_ONLY,
    REASON_BROKER_BACKED_CLOSE_FILL,
    REASON_CONTRACT_CLOSE_ONLY_ENTRY_BLOCKED,
    REASON_CONTROL_PLANE_SNAPSHOT_STALE,
    REASON_ENTRY_FILL_NOT_ADOPTED,
    REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED,
    REASON_EXIT_CONTRACT_MISMATCH,
    REASON_LANE_THESIS_STRATEGY_MISMATCH,
    REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED,
    REASON_MANAGED_EXIT_BAR_COUNT_MISMATCH,
    REASON_MISSING_LIFECYCLE_ID,
    REASON_PASSIVE_ENTRY_CANCELLED,
    REASON_PLANNER_SNAPSHOT_MISMATCH,
    REASON_RECONCILED_FLAT,
    REASON_REQUIRED_FIELD_MISSING,
    REASON_SAFE_STATE_SUBMIT_BLOCKED,
    REASON_SCOPED_CLEANUP_DIAGNOSTIC_FIELDS_IGNORED,
    SCENARIO_IDS,
    SimulationValidationMode,
    build_lifecycle_simulation_scenario,
    run_lifecycle_simulation_scenario,
    simulation_result_to_dict,
    validate_lifecycle_simulation_scenario,
    write_lifecycle_simulation_authority_artifacts,
    write_lifecycle_simulation_report,
)


def test_clean_full_lifecycle_reconciles_flat(tmp_path: Path) -> None:
    result = _run(tmp_path, "clean_full_lifecycle")

    assert result.classification == LIFECYCLE_SIMULATION_PASSED
    assert result.passed is True
    assert result.terminal_state == "RECONCILED_FLAT"
    assert REASON_BROKER_BACKED_CLOSE_FILL in result.reason_codes
    assert REASON_RECONCILED_FLAT in result.reason_codes
    assert result.broker_mutation_allowed is False
    assert result.ibkr_mutation_allowed is False


def test_passive_entry_cancel_is_terminal_without_fill(tmp_path: Path) -> None:
    result = _run(tmp_path, "passive_entry_cancel")

    assert result.classification == LIFECYCLE_SIMULATION_CANCELLED
    assert result.terminal_state == "ENTRY_ORDER_CANCELLED_BEFORE_FILL"
    assert REASON_PASSIVE_ENTRY_CANCELLED in result.reason_codes


def test_entry_fill_not_adopted_blocks_lifecycle(tmp_path: Path) -> None:
    result = _run(tmp_path, "entry_fill_not_adopted")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "BROKER_FILL_NOT_ADOPTED_BY_LIFECYCLE"
    assert REASON_ENTRY_FILL_NOT_ADOPTED in result.reason_codes


def test_mes_managed_exit_policy_wrong_bar_count_blocks(tmp_path: Path) -> None:
    result = _run(tmp_path, "managed_exit_policy_wrong_bar_count")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "MANAGED_EXIT_POLICY_CONTRACT_FAILED"
    assert REASON_MANAGED_EXIT_BAR_COUNT_MISMATCH in result.reason_codes


def test_managed_exit_due_missing_lifecycle_id_fails_closed(tmp_path: Path) -> None:
    result = _run(tmp_path, "managed_exit_due_missing_lifecycle_id")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "IDENTITY_CONTRACT_FAILED"
    assert REASON_REQUIRED_FIELD_MISSING in result.reason_codes
    assert REASON_MISSING_LIFECYCLE_ID in result.reason_codes
    assert "MISSING_managed_position.lifecycle_id" in result.reason_codes


def test_lane_id_vs_thesis_strategy_id_mismatch_blocks(tmp_path: Path) -> None:
    result = _run(tmp_path, "lane_id_vs_thesis_strategy_id_mismatch")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "LANE_THESIS_MISMATCH_BLOCKED"
    assert REASON_LANE_THESIS_STRATEGY_MISMATCH in result.reason_codes


def test_mnq_close_requires_exact_lifecycle_contract_identity(tmp_path: Path) -> None:
    result = _run(tmp_path, "managed_exit_close_identity_contract_mismatch")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "EXACT_LIFECYCLE_EXIT_IDENTITY_FAILED"
    assert REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED in result.reason_codes
    assert REASON_EXIT_CONTRACT_MISMATCH in result.reason_codes


def test_aggregate_account_multiple_does_not_block_exact_lifecycle_owner(tmp_path: Path) -> None:
    result = _run(tmp_path, "aggregate_account_multiple_exact_row_valid")

    assert result.classification == LIFECYCLE_SIMULATION_PASSED
    assert REASON_AGGREGATE_ACCOUNT_DIAGNOSTIC_ONLY in result.reason_codes
    assert REASON_RECONCILED_FLAT in result.reason_codes


def test_stale_control_plane_snapshot_blocks_before_lifecycle(tmp_path: Path) -> None:
    result = _run(tmp_path, "stale_control_plane_snapshot")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "PRE_ACTION_BLOCKED_SNAPSHOT_STALE"
    assert REASON_CONTROL_PLANE_SNAPSHOT_STALE in result.reason_codes


def test_safe_state_submit_blocked_fails_closed(tmp_path: Path) -> None:
    result = _run(tmp_path, "safe_state_submit_blocked")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "SAFE_STATE_SUBMIT_BLOCKED"
    assert REASON_SAFE_STATE_SUBMIT_BLOCKED in result.reason_codes


def test_planner_snapshot_mismatch_blocks(tmp_path: Path) -> None:
    result = _run(tmp_path, "planner_snapshot_mismatch")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "PRE_ACTION_BLOCKED_PLAN_MISMATCH"
    assert REASON_PLANNER_SNAPSHOT_MISMATCH in result.reason_codes


def test_scoped_cleanup_extra_diagnostic_fields_still_allows_exact_identity(tmp_path: Path) -> None:
    result = _run(tmp_path, "scoped_cleanup_extra_diagnostic_fields")

    assert result.classification == LIFECYCLE_SIMULATION_PASSED
    assert REASON_SCOPED_CLEANUP_DIAGNOSTIC_FIELDS_IGNORED in result.reason_codes
    assert REASON_RECONCILED_FLAT in result.reason_codes


def test_local_paper_artifact_without_perm_or_exec_is_not_broker_backed(tmp_path: Path) -> None:
    result = _run(tmp_path, "local_paper_artifact_without_broker_ids")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "ENTRY_FILL_NOT_BROKER_BACKED"
    assert REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED in result.reason_codes


def test_close_only_contract_blocks_new_entry_but_allows_exit_identity(tmp_path: Path) -> None:
    result = _run(tmp_path, "contract_close_only_new_entry_blocked_exit_allowed")

    assert result.classification == LIFECYCLE_SIMULATION_BLOCKED
    assert result.terminal_state == "ENTRY_BLOCKED_EXIT_ALLOWED_CLOSE_ONLY_CONTRACT"
    assert REASON_CONTRACT_CLOSE_ONLY_ENTRY_BLOCKED in result.reason_codes
    assert "EXIT_ALLOWED_ON_ORIGINAL_FILLED_CONTRACT" in result.reason_codes


def test_dry_run_and_simulated_live_path_use_same_validation_function(tmp_path: Path) -> None:
    scenario = build_lifecycle_simulation_scenario("aggregate_account_multiple_exact_row_valid")
    write_lifecycle_simulation_authority_artifacts(tmp_path, scenario)

    dry_run = validate_lifecycle_simulation_scenario(
        scenario=scenario,
        repo_root=tmp_path,
        validation_mode=SimulationValidationMode.DRY_RUN,
    )
    live_path = validate_lifecycle_simulation_scenario(
        scenario=scenario,
        repo_root=tmp_path,
        validation_mode=SimulationValidationMode.SIMULATED_LIVE_PATH,
    )

    assert dry_run.reason_codes == live_path.reason_codes
    assert dry_run.terminal_state == live_path.terminal_state
    assert dry_run.validation_mode == "DRY_RUN"
    assert live_path.validation_mode == "SIMULATED_LIVE_PATH"


def test_report_payload_is_json_ready_and_contains_no_mutation_flags(tmp_path: Path) -> None:
    result = _run(tmp_path, "clean_full_lifecycle")
    payload = simulation_result_to_dict(result)

    assert payload["scenario_id"] == "clean_full_lifecycle"
    assert payload["broker_mutation_allowed"] is False
    assert payload["lifecycle_mutation_allowed"] is False
    assert payload["ibkr_mutation_allowed"] is False
    assert payload["simulated_only"] is True
    assert payload["stage_results"]


def test_report_writer_creates_artifact_without_runtime_or_broker_side_effects(tmp_path: Path) -> None:
    result = _run(tmp_path / "scenario", "clean_full_lifecycle")
    report_path = tmp_path / "report.json"

    write_lifecycle_simulation_report(report_path, [result])

    report = report_path.read_text(encoding="utf-8")
    assert '"ibkr_mutation_allowed": false' in report
    assert '"simulated_only": true' in report
    assert '"scenario_id": "clean_full_lifecycle"' in report


def test_every_declared_scenario_is_runnable(tmp_path: Path) -> None:
    classifications = {}

    for scenario_id in SCENARIO_IDS:
        result = _run(tmp_path / scenario_id, scenario_id)
        classifications[scenario_id] = result.classification

    assert classifications["clean_full_lifecycle"] == LIFECYCLE_SIMULATION_PASSED
    assert classifications["passive_entry_cancel"] == LIFECYCLE_SIMULATION_CANCELLED
    assert set(classifications) == set(SCENARIO_IDS)


def _run(tmp_path: Path, scenario_id: str):
    scenario = build_lifecycle_simulation_scenario(scenario_id)
    write_lifecycle_simulation_authority_artifacts(tmp_path, scenario)
    return run_lifecycle_simulation_scenario(repo_root=tmp_path, scenario_id=scenario_id)
