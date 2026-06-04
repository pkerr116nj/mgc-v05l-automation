from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_fault_injection_harness import (
    FAULT_INJECTION_SCENARIOS,
    FAULT_INJECTION_REPORT_SCHEMA_VERSION,
    SAFETY_INVARIANTS_CHECKED,
    SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE,
    SCENARIO_DEAD_PID_STALE_HEARTBEAT,
    SCENARIO_DUPLICATE_LIFECYCLE_ROWS,
    SCENARIO_HISTORICAL_REGISTRY_DEBRIS,
    SCENARIO_NEAR_EXPIRY_CONTRACT,
    SCENARIO_PHASE1_FRESH_RUNTIME_STALE,
    SCENARIO_METADATA,
    SCENARIO_STALE_OWNER_FRESH_BROKER,
    SCENARIO_STALE_RUNTIME_EXIT_DUE,
    list_track_b_fault_injection_scenarios,
    run_track_b_fault_injection_harness,
    run_track_b_fault_injection_scenario,
    write_track_b_fault_injection_json_report,
)
from mgc_v05l.execution_core.track_b_futures_contract_resolver import CONTRACT_NEAR_EXPIRY
from mgc_v05l.execution_core.track_b_live_runtime_environment_watchdog import (
    DEGRADED_LANES_NOT_EVALUATING,
    READY_SUBMIT_CAPABLE,
    RECOVERY_REQUIRED,
)
from mgc_v05l.execution_core.track_b_risk_reducing_close_authority import (
    RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE,
    RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE,
)


NOW = datetime(2026, 6, 4, 14, 0, tzinfo=UTC)
EXPECTED_SCENARIO_IDS = (
    "stale_runtime_with_exit_due_position",
    "stale_owner_candidates_with_fresh_broker_owner",
    "historical_registry_debris_clean_current_scope",
    "duplicate_lifecycle_rows_same_contract",
    "dead_pid_with_stale_heartbeat",
    "near_expiry_contract_submit_attempt",
    "phase1_fresh_runtime_ingestion_stale",
    "broker_fill_without_lifecycle_close",
)
REQUIRED_METADATA_FIELDS = {
    "bug_class",
    "retired_invariant",
    "authority_helpers_exercised",
    "expected_primary_classification",
    "safety_invariants_checked",
    "retirement_status",
    "notes",
    "evidence",
}


def test_scenario_ids_remain_stable() -> None:
    assert FAULT_INJECTION_SCENARIOS == EXPECTED_SCENARIO_IDS
    assert list_track_b_fault_injection_scenarios() == EXPECTED_SCENARIO_IDS
    assert tuple(SCENARIO_METADATA) == EXPECTED_SCENARIO_IDS


def test_fault_injection_harness_runs_all_named_scenarios(tmp_path: Path) -> None:
    report = run_track_b_fault_injection_harness(artifact_root=tmp_path, now=NOW)

    assert report["schema_version"] == "track_b_fault_injection_harness_v1"
    assert report["read_only"] is True
    assert report["broker_mutation_allowed"] is False
    assert report["passed"] is True
    assert [item["scenario"] for item in report["scenarios"]] == list(FAULT_INJECTION_SCENARIOS)
    assert list_track_b_fault_injection_scenarios() == FAULT_INJECTION_SCENARIOS
    for scenario in report["scenarios"]:
        assert scenario["passed"] is True
        assert scenario["safety"] == {
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "broker_mutation_allowed": False,
        }
        assert {item["name"] for item in scenario["assertions"]} == {
            "submit_authority_classified",
            "broker_exposure_classified",
            "ownership_classified",
            "registry_classified",
            "risk_reducing_close_authority_classified_when_applicable",
            "no_live_money_eligibility",
            "no_paper_proof",
            "no_broad_flatten",
            "no_unguarded_broker_mutation",
        }


def test_every_scenario_includes_complete_metadata(tmp_path: Path) -> None:
    report = run_track_b_fault_injection_harness(artifact_root=tmp_path, now=NOW)

    for scenario in report["scenarios"]:
        metadata = scenario["metadata"]
        assert set(metadata) == REQUIRED_METADATA_FIELDS
        assert metadata["bug_class"]
        assert metadata["retired_invariant"]
        assert metadata["authority_helpers_exercised"]
        assert all(isinstance(item, str) and item for item in metadata["authority_helpers_exercised"])
        assert metadata["expected_primary_classification"] == _primary_classification(scenario)
        assert metadata["safety_invariants_checked"] == list(SAFETY_INVARIANTS_CHECKED)
        assert metadata["retirement_status"] == "FAULT_INJECTION_V1_COVERED"
        assert metadata["notes"].startswith("Evidence placeholder:")
        assert metadata["evidence"] == {"placeholder": True}


def test_json_report_writer_requires_explicit_tmp_path_output(tmp_path: Path) -> None:
    harness_report = run_track_b_fault_injection_harness(artifact_root=tmp_path / "artifacts", now=NOW)
    output_path = tmp_path / "reports" / "fault_injection_report.json"

    written = write_track_b_fault_injection_json_report(
        harness_report=harness_report,
        output_path=output_path,
        repo_root=Path.cwd(),
        now=NOW,
    )

    assert written == output_path.resolve()
    written.relative_to(tmp_path)
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["schema_version"] == FAULT_INJECTION_REPORT_SCHEMA_VERSION
    assert payload["generated_at"] == NOW.isoformat()
    assert payload["scenario_count"] == len(FAULT_INJECTION_SCENARIOS)
    assert payload["passed"] is True
    assert payload["broker_mutation_allowed"] is False
    assert [scenario["scenario_id"] for scenario in payload["scenarios"]] == list(FAULT_INJECTION_SCENARIOS)
    for scenario in payload["scenarios"]:
        assert set(scenario) == {"scenario_id", "timestamp", "verdict", "metadata", "safety_checks"}
        assert scenario["timestamp"]
        assert scenario["metadata"]["bug_class"]
        assert scenario["verdict"]["passed"] is True
        assert {item["name"] for item in scenario["safety_checks"]} == set(SAFETY_INVARIANTS_CHECKED)
        assert all(item["passed"] is True for item in scenario["safety_checks"])


def test_json_report_writer_rejects_live_repo_output_and_var_paths(tmp_path: Path) -> None:
    harness_report = run_track_b_fault_injection_harness(artifact_root=tmp_path / "artifacts", now=NOW)
    repo_root = tmp_path / "repo"

    for rejected in (
        repo_root / "outputs" / "track_b_execution_core" / "fault_injection_report.json",
        repo_root / "var" / "fault_injection_report.json",
    ):
        with pytest.raises(ValueError, match="Refusing to write fault-injection report"):
            write_track_b_fault_injection_json_report(
                harness_report=harness_report,
                output_path=rejected,
                repo_root=repo_root,
                now=NOW,
            )
        assert not rejected.exists()


def test_stale_runtime_exit_due_blocks_submit_but_allows_exact_risk_reducing_close(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_STALE_RUNTIME_EXIT_DUE)

    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["ownership"]["classification"] == "OWNED_MANAGED_EXIT_DUE"
    assert (
        scenario["risk_reducing_close_authority"]["classification"]
        == RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE
    )
    assert scenario["risk_reducing_close_authority"]["allowed"] is True
    assert scenario["risk_reducing_close_authority"]["close_candidate"]["action"] == "BUY"
    assert scenario["risk_reducing_close_authority"]["broad_flatten_allowed"] is False


def test_stale_owner_candidate_is_diagnostic_only_when_fresh_broker_owner_exists(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_STALE_OWNER_FRESH_BROKER)

    assert scenario["ownership"]["classification"] == "OWNED_MANAGED_EXIT_DUE"
    assert scenario["ownership"]["owned_exposures"][0]["trade_id"] == "trade_fresh_owner"
    assert scenario["ownership"]["review_required_exposure_count"] == 0
    stale_rows = scenario["ownership"]["stale_superseded_full_audit_only"]
    assert stale_rows[0]["classification"] == "EXPIRED_DIAGNOSTIC_ONLY"
    assert stale_rows[0]["diagnostic_only"] is True


def test_historical_registry_debris_warns_but_current_scope_stays_clean(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_HISTORICAL_REGISTRY_DEBRIS)

    assert scenario["submit_authority"]["classification"] == READY_SUBMIT_CAPABLE
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["registry"]["clean_current_scope"] is True
    assert scenario["registry"]["warning_code"] == "REGISTRY_DIAGNOSTICS_STALE_OR_HISTORICAL_DIAGNOSTIC_ONLY"
    assert "REGISTRY_RECONCILIATION_NOT_MATCHED" not in scenario["submit_authority"]["reason_codes"]


def test_duplicate_lifecycle_rows_do_not_double_count_current_scope(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_DUPLICATE_LIFECYCLE_ROWS)

    assert scenario["ownership"]["classification"] == "OWNED_MANAGED_EXPOSURE"
    assert scenario["ownership"]["owned_exposures"][0]["trade_id"] == "trade_current_mes_short"
    current_rows = scenario["registry"]["current_scope_lifecycle_rows"]
    diagnostic_rows = scenario["registry"]["diagnostic_only_rows"]
    assert len(current_rows) == 1
    assert current_rows[0]["aggregate_qty"] == "-1"
    assert len(diagnostic_rows) == 1
    assert diagnostic_rows[0]["classification"] == "STALE_DUPLICATE_LIFECYCLE_AGGREGATION_FULL_AUDIT_ONLY"


def test_dead_pid_with_stale_heartbeat_is_not_runtime_ready(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_DEAD_PID_STALE_HEARTBEAT)
    watchdog = scenario["observability"]["watchdog"]

    assert scenario["submit_authority"]["classification"] == RECOVERY_REQUIRED
    assert scenario["submit_authority"]["allowed"] is False
    assert watchdog["liveness_contract"]["process_alive"] is False
    assert "RUNTIME_PROCESS_NOT_ALIVE" in scenario["submit_authority"]["reason_codes"]
    assert watchdog["classification"] != READY_SUBMIT_CAPABLE


def test_near_expiry_contract_submit_attempt_fails_closed(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_NEAR_EXPIRY_CONTRACT)
    contract = scenario["observability"]["contract_authority"]

    assert scenario["submit_authority"]["classification"] == CONTRACT_NEAR_EXPIRY
    assert scenario["submit_authority"]["allowed"] is False
    assert contract["submit_allowed"] is False
    assert contract["broker_mutation_allowed"] is False
    assert contract["days_to_expiry"] == 3


def test_phase1_fresh_but_runtime_ingestion_stale_blocks_submit(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_PHASE1_FRESH_RUNTIME_STALE)
    watchdog = scenario["observability"]["watchdog"]

    assert scenario["submit_authority"]["classification"] == DEGRADED_LANES_NOT_EVALUATING
    assert scenario["submit_authority"]["allowed"] is False
    assert watchdog["market_data"]["fresh"] is True
    assert watchdog["liveness_contract"]["process_alive"] is True
    assert watchdog["liveness_contract"]["lane_evaluation_advancing_when_in_window"] is False


def test_broker_fill_without_lifecycle_close_keeps_owner_and_reconciliation_path(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE)

    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["ownership"]["classification"] == "OWNED_MANAGED_EXPOSURE"
    assert scenario["ownership"]["owned_exposures"][0]["trade_id"] == "trade_open_no_close"
    assert scenario["registry"]["reconciliation_path_available"] is True
    assert scenario["observability"]["ownership_loss_detected"] is False
    assert scenario["risk_reducing_close_authority"]["classification"] == RISK_REDUCING_CLOSE_BLOCKED_NOT_EXIT_DUE


def _run(tmp_path: Path, name: str) -> dict:
    return run_track_b_fault_injection_scenario(name=name, artifact_root=tmp_path / name, now=NOW)


def _primary_classification(scenario: dict) -> str:
    scenario_id = scenario["scenario"]
    if scenario_id == SCENARIO_STALE_RUNTIME_EXIT_DUE:
        return scenario["risk_reducing_close_authority"]["classification"]
    if scenario_id in {SCENARIO_STALE_OWNER_FRESH_BROKER, SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE}:
        return scenario["ownership"]["classification"]
    if scenario_id == SCENARIO_DUPLICATE_LIFECYCLE_ROWS:
        return scenario["registry"]["diagnostic_only_rows"][0]["classification"]
    return scenario["submit_authority"]["classification"]
