from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    BROKER_LIFECYCLE_RECONCILIATION_DIRTY,
    BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED,
    CONTRACT_ENTRY_CLOSE_ONLY,
    CONTROL_PLANE_STALE,
    EXACT_LIFECYCLE_IDENTITY_MISMATCH,
    FILL_NOT_BROKER_BACKED,
    LANE_THESIS_STRATEGY_MISMATCH,
    MANAGED_EXIT_POLICY_CONFLICT,
    PLANNER_SNAPSHOT_MISMATCH,
    SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT,
    SAFE_STATE_SUBMIT_BLOCKED,
    TRUTH_CONFLICT_REVIEW_REQUIRED,
    TRUTH_SNAPSHOT_OK,
)
from mgc_v05l.execution_core.track_b_lifecycle_simulation_harness import (
    NOW,
    SCENARIO_IDS,
    SimulationValidationMode,
    build_canonical_truth_simulation_report_row,
    build_lifecycle_simulation_scenario,
    build_lifecycle_simulation_truth_snapshot,
    validate_lifecycle_simulation_scenario,
    write_canonical_truth_simulation_report,
    write_lifecycle_simulation_authority_artifacts,
)


EXPECTATIONS = {
    "clean_full_lifecycle": {
        "classification": TRUTH_SNAPSHOT_OK,
        "reasons": (),
        "conflicts": (),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "passive_entry_cancel": {
        "classification": TRUTH_SNAPSHOT_OK,
        "reasons": (),
        "conflicts": (),
        "broker_backed": False,
        "submit_allowed": True,
    },
    "entry_fill_not_adopted": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (BROKER_LIFECYCLE_RECONCILIATION_DIRTY,),
        "conflicts": (BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "managed_exit_policy_wrong_bar_count": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (MANAGED_EXIT_POLICY_CONFLICT,),
        "conflicts": (MANAGED_EXIT_POLICY_CONFLICT,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "managed_exit_due_missing_lifecycle_id": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (EXACT_LIFECYCLE_IDENTITY_MISMATCH,),
        "conflicts": (EXACT_LIFECYCLE_IDENTITY_MISMATCH,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "lane_id_vs_thesis_strategy_id_mismatch": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (LANE_THESIS_STRATEGY_MISMATCH,),
        "conflicts": (LANE_THESIS_STRATEGY_MISMATCH,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "managed_exit_close_identity_contract_mismatch": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (EXACT_LIFECYCLE_IDENTITY_MISMATCH,),
        "conflicts": (EXACT_LIFECYCLE_IDENTITY_MISMATCH,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "aggregate_account_multiple_exact_row_valid": {
        "classification": TRUTH_SNAPSHOT_OK,
        "reasons": ("EXACT_LIFECYCLE_OWNER_RESOLVED", "AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY"),
        "conflicts": (),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "stale_control_plane_snapshot": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (CONTROL_PLANE_STALE,),
        "conflicts": (CONTROL_PLANE_STALE,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "safe_state_submit_blocked": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (SAFE_STATE_SUBMIT_BLOCKED, SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT),
        "conflicts": (SAFE_STATE_SUBMIT_BLOCKED,),
        "broker_backed": True,
        "submit_allowed": False,
    },
    "planner_snapshot_mismatch": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (PLANNER_SNAPSHOT_MISMATCH,),
        "conflicts": (PLANNER_SNAPSHOT_MISMATCH,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "scoped_cleanup_extra_diagnostic_fields": {
        "classification": TRUTH_SNAPSHOT_OK,
        "reasons": (),
        "conflicts": (),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "local_paper_artifact_without_broker_ids": {
        "classification": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (FILL_NOT_BROKER_BACKED,),
        "conflicts": (FILL_NOT_BROKER_BACKED,),
        "broker_backed": False,
        "submit_allowed": True,
    },
    "contract_close_only_new_entry_blocked_exit_allowed": {
        "classification": TRUTH_SNAPSHOT_OK,
        "reasons": (CONTRACT_ENTRY_CLOSE_ONLY,),
        "conflicts": (),
        "broker_backed": True,
        "submit_allowed": True,
    },
}


@pytest.mark.parametrize("scenario_id", SCENARIO_IDS)
def test_lifecycle_scenarios_build_expected_canonical_truth_snapshot(tmp_path: Path, scenario_id: str) -> None:
    expected = EXPECTATIONS[scenario_id]

    snapshot = build_lifecycle_simulation_truth_snapshot(
        repo_root=tmp_path / scenario_id,
        scenario_id=scenario_id,
        now=NOW,
    )
    actual_conflicts = tuple(conflict.classification for conflict in snapshot.conflicts)

    assert snapshot.classification == expected["classification"]
    assert snapshot.broker_backed_evidence.broker_backed is expected["broker_backed"]
    assert snapshot.safe_state.submit_allowed is expected["submit_allowed"]
    for reason in expected["reasons"]:
        assert reason in snapshot.reason_codes
    for conflict in expected["conflicts"]:
        assert conflict in actual_conflicts


def test_canonical_truth_simulation_report_records_pass_fail(tmp_path: Path) -> None:
    rows = []
    for scenario_id, expected in EXPECTATIONS.items():
        rows.append(
            build_canonical_truth_simulation_report_row(
                repo_root=tmp_path / scenario_id,
                scenario_id=scenario_id,
                expected_classification=expected["classification"],
                expected_reason_codes=expected["reasons"],
                expected_conflicts=expected["conflicts"],
                expected_broker_backed=expected["broker_backed"],
                expected_submit_allowed=expected["submit_allowed"],
                now=NOW,
            )
        )

    report_path = tmp_path / "canonical_truth_simulation_report.json"
    write_canonical_truth_simulation_report(report_path, rows, now=NOW)
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "track_b_canonical_truth_simulation_report_v1"
    assert payload["broker_mutation_allowed"] is False
    assert payload["runtime_restart_allowed"] is False
    assert all(row["passed"] is True for row in payload["results"])


def test_dry_run_and_simulated_live_still_share_validator_path(tmp_path: Path) -> None:
    scenario = build_lifecycle_simulation_scenario("aggregate_account_multiple_exact_row_valid")
    write_lifecycle_simulation_authority_artifacts(tmp_path, scenario)

    dry_run = validate_lifecycle_simulation_scenario(
        scenario=scenario,
        repo_root=tmp_path,
        validation_mode=SimulationValidationMode.DRY_RUN,
    )
    simulated_live = validate_lifecycle_simulation_scenario(
        scenario=scenario,
        repo_root=tmp_path,
        validation_mode=SimulationValidationMode.SIMULATED_LIVE_PATH,
    )

    assert dry_run.reason_codes == simulated_live.reason_codes
    assert dry_run.terminal_state == simulated_live.terminal_state
