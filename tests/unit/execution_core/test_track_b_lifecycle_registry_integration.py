from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    CONTRACT_ENTRY_CLOSE_ONLY,
    CONTROL_PLANE_STALE,
    FILL_NOT_BROKER_BACKED,
    PLANNER_SNAPSHOT_MISMATCH,
    SAFE_STATE_SUBMIT_BLOCKED,
    TRUTH_CONFLICT_REVIEW_REQUIRED,
    TRUTH_SNAPSHOT_OK,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState
from mgc_v05l.execution_core.track_b_lifecycle_simulation_harness import (
    NOW,
    build_canonical_truth_simulation_report_row,
    build_lifecycle_simulation_scenario,
    build_lifecycle_simulation_truth_snapshot,
    build_lifecycle_simulation_trade_events,
    reduce_lifecycle_simulation_trade_registry,
    write_canonical_truth_simulation_report,
)


EXPECTATIONS = {
    "clean_full_lifecycle": {
        "state": TradeCurrentState.CLOSED_FLAT,
        "truth": TRUTH_SNAPSHOT_OK,
        "reasons": (),
        "conflicts": (),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "passive_entry_cancel": {
        "state": TradeCurrentState.CANCELLED,
        "truth": TRUTH_SNAPSHOT_OK,
        "reasons": (),
        "conflicts": (),
        "broker_backed": False,
        "submit_allowed": True,
    },
    "entry_fill_not_adopted": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": ("BROKER_LIFECYCLE_RECONCILIATION_DIRTY",),
        "conflicts": ("BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED",),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "managed_exit_close_identity_contract_mismatch": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": ("EXACT_LIFECYCLE_IDENTITY_MISMATCH",),
        "conflicts": ("EXACT_LIFECYCLE_IDENTITY_MISMATCH",),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "stale_control_plane_snapshot": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (CONTROL_PLANE_STALE,),
        "conflicts": (CONTROL_PLANE_STALE,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "safe_state_submit_blocked": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (SAFE_STATE_SUBMIT_BLOCKED,),
        "conflicts": (SAFE_STATE_SUBMIT_BLOCKED,),
        "broker_backed": True,
        "submit_allowed": False,
    },
    "planner_snapshot_mismatch": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (PLANNER_SNAPSHOT_MISMATCH,),
        "conflicts": (PLANNER_SNAPSHOT_MISMATCH,),
        "broker_backed": True,
        "submit_allowed": True,
    },
    "local_paper_artifact_without_broker_ids": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_CONFLICT_REVIEW_REQUIRED,
        "reasons": (FILL_NOT_BROKER_BACKED,),
        "conflicts": (FILL_NOT_BROKER_BACKED,),
        "broker_backed": False,
        "submit_allowed": True,
    },
    "contract_close_only_new_entry_blocked_exit_allowed": {
        "state": TradeCurrentState.REVIEW_REQUIRED,
        "truth": TRUTH_SNAPSHOT_OK,
        "reasons": (CONTRACT_ENTRY_CLOSE_ONLY,),
        "conflicts": (),
        "broker_backed": True,
        "submit_allowed": True,
    },
}


@pytest.mark.parametrize("scenario_id", sorted(EXPECTATIONS))
def test_registry_reduction_and_truth_snapshot_for_core_scenarios(tmp_path: Path, scenario_id: str) -> None:
    expected = EXPECTATIONS[scenario_id]
    scenario = build_lifecycle_simulation_scenario(scenario_id)

    events = build_lifecycle_simulation_trade_events(scenario, now=NOW)
    record = reduce_lifecycle_simulation_trade_registry(scenario, now=NOW)
    snapshot = build_lifecycle_simulation_truth_snapshot(repo_root=tmp_path / scenario_id, scenario_id=scenario_id, now=NOW)
    actual_conflicts = tuple(conflict.classification for conflict in snapshot.conflicts)

    assert events
    assert record.current_state == expected["state"]
    assert snapshot.classification == expected["truth"]
    assert snapshot.broker_backed_evidence.broker_backed is expected["broker_backed"]
    assert snapshot.safe_state.submit_allowed is expected["submit_allowed"]
    for reason in expected["reasons"]:
        assert reason in snapshot.reason_codes
    for conflict in expected["conflicts"]:
        assert conflict in actual_conflicts


def test_registry_backed_simulation_report_includes_registry_and_truth_results(tmp_path: Path) -> None:
    rows = []
    for scenario_id, expected in EXPECTATIONS.items():
        rows.append(
            build_canonical_truth_simulation_report_row(
                repo_root=tmp_path / scenario_id,
                scenario_id=scenario_id,
                expected_registry_state=expected["state"],
                expected_classification=expected["truth"],
                expected_reason_codes=expected["reasons"],
                expected_conflicts=expected["conflicts"],
                expected_broker_backed=expected["broker_backed"],
                expected_submit_allowed=expected["submit_allowed"],
                now=NOW,
            )
        )

    report_path = tmp_path / "registry_backed_simulation_report.json"
    write_canonical_truth_simulation_report(report_path, rows, now=NOW)
    payload = json.loads(report_path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "track_b_canonical_truth_simulation_report_v1"
    assert all(row["passed"] for row in payload["results"])
    clean = next(row for row in payload["results"] if row["scenario_id"] == "clean_full_lifecycle")
    assert clean["actual_registry_state"] == "CLOSED_FLAT"
    assert clean["registry_broker_backed_entry"] is True
    assert clean["registry_broker_backed_exit"] is True
