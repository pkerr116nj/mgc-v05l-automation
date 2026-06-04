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
    SCENARIO_BROKER_OBSERVED_CONFLICTING_CANDIDATE,
    SCENARIO_BROKER_OBSERVED_RESERVED_LIFECYCLE_ADOPTION,
    SCENARIO_BROKER_OBSERVED_STALE_INTENT,
    SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE,
    SCENARIO_CONFLICTING_AUTHORITY_GENERATION,
    SCENARIO_DEAD_PID_STALE_HEARTBEAT,
    SCENARIO_DUPLICATE_LIFECYCLE_ROWS,
    SCENARIO_HISTORICAL_REGISTRY_DEBRIS,
    SCENARIO_MALFORMED_BROKER_POSITION_TRUTH,
    SCENARIO_MANAGED_CLOSE_DISAPPEARS_BROKER_FLAT,
    SCENARIO_MANAGED_CLOSE_ORDER_BROKER_ZERO,
    SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE,
    SCENARIO_MISSING_LIFECYCLE_RECONCILIATION,
    SCENARIO_NEAR_EXPIRY_CONTRACT,
    SCENARIO_PHASE1_FRESH_RUNTIME_STALE,
    SCENARIO_REGISTRY_DIAGNOSTIC_MISSING_CURRENT_SCOPE,
    SCENARIO_REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_EXIT,
    SCENARIO_METADATA,
    SCENARIO_RAW_STALE_LIFECYCLE_COUNT_CURRENT_SCOPE_FLAT,
    SCENARIO_SIMULTANEOUS_MNQ_MES_BROKER_OBSERVED_SHORT_ADOPTION,
    SCENARIO_STALE_OWNER_FRESH_BROKER,
    SCENARIO_STALE_RUNTIME_EXIT_DUE,
    SCENARIO_STALE_RUNTIME_ENV_FRESH_BROKER_TRUTH,
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
    "malformed_broker_position_truth_artifact",
    "missing_lifecycle_reconciliation_artifact",
    "registry_diagnostic_missing_current_scope_fields",
    "stale_runtime_environment_truth_with_fresh_broker_truth",
    "conflicting_authority_generation_id",
    "managed_close_order_artifact_broker_open_orders_zero",
    "broker_open_order_exists_lifecycle_registry_none",
    "missing_guardian_artifact_with_broker_exposure",
    "managed_close_order_disappears_broker_flat_without_fill_callback",
    "registry_review_required_null_lifecycle_blocks_valid_exit",
    "broker_observed_fill_reserved_lifecycle_adoption_required",
    "broker_observed_fill_conflicting_candidate_fails_closed",
    "broker_observed_fill_stale_intent_fails_closed",
    "simultaneous_mnq_mes_broker_observed_short_adoption",
    "raw_stale_lifecycle_count_current_scope_flat",
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
    "live_incident_source",
}
COVERAGE_SUMMARY_FIELDS = {
    "scenario_id",
    "bug_class",
    "protected_invariant",
    "live_incident_source",
    "authority_layers_exercised",
    "expected_fail_closed_or_repair_path",
    "safety_invariants",
    "regression_status",
}
MALFORMED_AUTHORITY_SCENARIOS = (
    (
        SCENARIO_MALFORMED_BROKER_POSITION_TRUTH,
        "MALFORMED_BROKER_POSITION_TRUTH_BLOCKED",
        False,
        "malformed_blocking",
    ),
    (
        SCENARIO_MISSING_LIFECYCLE_RECONCILIATION,
        "MISSING_LIFECYCLE_RECONCILIATION_BLOCKED",
        True,
        "missing_blocking",
    ),
    (
        SCENARIO_REGISTRY_DIAGNOSTIC_MISSING_CURRENT_SCOPE,
        "REGISTRY_DIAGNOSTIC_CURRENT_SCOPE_MISSING_BLOCKED",
        False,
        "malformed_blocking",
    ),
    (
        SCENARIO_STALE_RUNTIME_ENV_FRESH_BROKER_TRUTH,
        "STALE_RUNTIME_ENVIRONMENT_TRUTH_BROKER_TRUTH_FRESH_BLOCKED",
        True,
        "stale_blocking",
    ),
    (
        SCENARIO_CONFLICTING_AUTHORITY_GENERATION,
        "CONFLICTING_AUTHORITY_GENERATION_ID_BLOCKED",
        False,
        "conflicting_blocking",
    ),
    (
        SCENARIO_MANAGED_CLOSE_ORDER_BROKER_ZERO,
        "MANAGED_CLOSE_ORDER_PHANTOM_BROKER_ZERO_BLOCKED",
        True,
        "conflicting_blocking",
    ),
    (
        SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE,
        "BROKER_OPEN_ORDER_WITHOUT_LIFECYCLE_REGISTRY_BLOCKED",
        False,
        "fresh_broker_truth_blocking",
    ),
    (
        SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE,
        "MISSING_GUARDIAN_WITH_BROKER_EXPOSURE_BLOCKED",
        True,
        "missing_blocking",
    ),
)


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
    assert [item["scenario_id"] for item in report["coverage_summary"]] == list(FAULT_INJECTION_SCENARIOS)
    assert list_track_b_fault_injection_scenarios() == FAULT_INJECTION_SCENARIOS
    for scenario in report["scenarios"]:
        assert scenario["passed"] is True
        assert scenario["safety"] == {
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "broker_mutation_allowed": False,
            "runtime_restart_invoked": False,
            "live_outputs_or_var_writes": False,
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
            "no_runtime_restart",
            "no_live_outputs_or_var_writes",
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
        assert "live_incident_source" in metadata


def test_coverage_summary_includes_all_scenarios_by_bug_class_and_invariant(tmp_path: Path) -> None:
    report = run_track_b_fault_injection_harness(artifact_root=tmp_path, now=NOW)
    summary = report["coverage_summary"]

    assert len(summary) == 23
    assert [row["scenario_id"] for row in summary] == list(FAULT_INJECTION_SCENARIOS)
    for row in summary:
        assert set(row) == COVERAGE_SUMMARY_FIELDS
        assert row["bug_class"]
        assert row["protected_invariant"]
        assert row["authority_layers_exercised"]
        assert row["expected_fail_closed_or_repair_path"]
        assert row["safety_invariants"] == list(SAFETY_INVARIANTS_CHECKED)
        assert row["regression_status"] == "FAULT_INJECTION_V1_COVERED"


def test_coverage_summary_marks_live_incident_sources_where_applicable(tmp_path: Path) -> None:
    report = run_track_b_fault_injection_harness(artifact_root=tmp_path, now=NOW)
    by_id = {row["scenario_id"]: row for row in report["coverage_summary"]}

    assert by_id["broker_observed_fill_reserved_lifecycle_adoption_required"]["live_incident_source"]
    assert by_id["simultaneous_mnq_mes_broker_observed_short_adoption"]["live_incident_source"]
    assert by_id["raw_stale_lifecycle_count_current_scope_flat"]["live_incident_source"]
    assert by_id["stale_runtime_with_exit_due_position"]["live_incident_source"] is None


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
    assert set(payload) == {
        "schema_version",
        "generated_at",
        "source_schema_version",
        "read_only",
        "broker_mutation_allowed",
        "scenario_count",
        "passed",
        "coverage_summary",
        "scenarios",
    }
    assert payload["schema_version"] == FAULT_INJECTION_REPORT_SCHEMA_VERSION
    assert payload["generated_at"] == NOW.isoformat()
    assert payload["scenario_count"] == len(FAULT_INJECTION_SCENARIOS)
    assert payload["passed"] is True
    assert payload["broker_mutation_allowed"] is False
    assert [row["scenario_id"] for row in payload["coverage_summary"]] == list(FAULT_INJECTION_SCENARIOS)
    assert all(set(row) == COVERAGE_SUMMARY_FIELDS for row in payload["coverage_summary"])
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


@pytest.mark.parametrize(
    ("scenario_id", "expected_classification", "broker_exposure_visible", "artifact_status"),
    MALFORMED_AUTHORITY_SCENARIOS,
)
def test_malformed_authority_artifact_scenarios_fail_closed(
    tmp_path: Path,
    scenario_id: str,
    expected_classification: str,
    broker_exposure_visible: bool,
    artifact_status: str,
) -> None:
    scenario = _run(tmp_path, scenario_id)

    assert scenario["submit_authority"]["classification"] == expected_classification
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is broker_exposure_visible
    assert scenario["registry"]["artifact_integrity"]["status"] == artifact_status
    assert scenario["registry"]["artifact_integrity"]["blocking"] is True
    assert scenario["observability"]["stale_or_malformed_artifact_blocked"] is True
    assert scenario["observability"]["stale_or_malformed_artifact_silently_accepted"] is False
    assert scenario["safety"] == {
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "broker_mutation_allowed": False,
        "runtime_restart_invoked": False,
        "live_outputs_or_var_writes": False,
    }
    assert scenario["passed"] is True
    if scenario_id == SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE:
        assert scenario["broker_exposure"]["broker_open_order_visible"] is True
        assert scenario["registry"]["artifact_integrity"]["broker_open_order_count"] == 1
    if scenario_id == SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE:
        assert scenario["risk_reducing_close_authority"]["classification"] == "RISK_REDUCING_CLOSE_BLOCKED_MISSING_GUARDIAN"
        assert scenario["risk_reducing_close_authority"]["allowed"] is False
        assert scenario["risk_reducing_close_authority"]["broad_flatten_allowed"] is False


def test_managed_close_order_disappears_broker_flat_requires_local_reconciliation(tmp_path: Path) -> None:
    scenario = _run(tmp_path, SCENARIO_MANAGED_CLOSE_DISAPPEARS_BROKER_FLAT)

    assert (
        scenario["submit_authority"]["classification"]
        == "MANAGED_CLOSE_DISAPPEARED_BROKER_FLAT_RECONCILIATION_REQUIRED"
    )
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["broker_flat"] is True
    assert scenario["broker_exposure"]["broker_open_order_count"] == 0
    assert scenario["ownership"]["classification"] == "LIFECYCLE_WITHOUT_BROKER_LOCAL_CLOSE_RECONCILIATION_REQUIRED"
    assert scenario["registry"]["reconciliation_path_available"] is True
    assert scenario["registry"]["broker_flat_without_fill_callback"] is True
    assert scenario["registry"]["prior_scoped_managed_close_order"]["order_id"] == "69"
    assert scenario["risk_reducing_close_authority"]["classification"] == "RISK_REDUCING_CLOSE_BLOCKED_BROKER_ALREADY_FLAT"
    assert scenario["risk_reducing_close_authority"]["allowed"] is False
    assert scenario["observability"]["local_reconciliation_allowed"] is True
    assert scenario["safety"] == {
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "broker_mutation_allowed": False,
        "runtime_restart_invoked": False,
        "live_outputs_or_var_writes": False,
    }
    assert scenario["passed"] is True


def test_registry_review_required_null_lifecycle_blocks_valid_exit_until_exact_repair(
    tmp_path: Path,
) -> None:
    scenario = _run(tmp_path, SCENARIO_REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_EXIT)

    assert (
        scenario["submit_authority"]["classification"]
        == "REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_BLOCKED_MANAGED_EXIT"
    )
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["broker_exposure"]["local_symbol"] == "MESM6"
    assert scenario["ownership"]["classification"] == "OWNED_MANAGED_EXPOSURE_REGISTRY_IDENTITY_REPAIR_REQUIRED"
    assert scenario["ownership"]["exact_broker_lifecycle_projection"] is True
    assert scenario["registry"]["classification"] == "REGISTRY_IDENTITY_NORMALIZATION_REPAIR_NEEDED_EXACT_EVIDENCE"
    assert scenario["registry"]["registry_current_state_before"] == "REVIEW_REQUIRED"
    assert scenario["registry"]["registry_lifecycle_id_before"] is None
    assert scenario["registry"]["target_state_after_repair"] == "OPEN_MANAGED"
    assert scenario["registry"]["exact_fill_identity_recoverable"] is True
    assert scenario["registry"]["conflicting_trade_or_lifecycle_identity"] is False
    assert (
        scenario["risk_reducing_close_authority"]["classification"]
        == "MANAGED_EXIT_BLOCKED_PENDING_REGISTRY_IDENTITY_NORMALIZATION"
    )
    assert scenario["risk_reducing_close_authority"]["close_candidate"] == {
        "action": "SELL",
        "quantity": "1",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "trade_id": "trade_mes_valid_exit",
        "lifecycle_id": "reserved_submit_mes_valid_exit",
    }
    assert scenario["observability"]["registry_identity_normalization_required"] is True
    assert scenario["observability"]["registry_identity_normalization_broker_mutation_allowed"] is False
    assert scenario["passed"] is True


def test_broker_observed_reserved_lifecycle_adoption_is_artifact_only_eligible(
    tmp_path: Path,
) -> None:
    scenario = _run(tmp_path, SCENARIO_BROKER_OBSERVED_RESERVED_LIFECYCLE_ADOPTION)

    assert scenario["submit_authority"]["classification"] == "BROKER_OBSERVED_FILL_ADOPTION_ELIGIBLE"
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["broker_exposure"]["count"] == 2
    assert scenario["broker_exposure"]["broker_open_order_count"] == 0
    assert scenario["ownership"]["classification"] == "BROKER_OBSERVED_ADOPTION_REQUIRED_EXACT_IDENTITY"
    assert scenario["registry"]["classification"] == "BROKER_OBSERVED_FILL_ADOPTION_ELIGIBLE"
    assert scenario["registry"]["current_blockers"] == []
    assert scenario["registry"]["adoption"]["eligible"] is True
    assert scenario["registry"]["adoption"]["artifact_only"] is True
    assert scenario["registry"]["adoption"]["apply_allowed_by_harness"] is False
    assert scenario["registry"]["adoption"]["managed_position_created"] is False
    assert scenario["registry"]["adoption"]["managed_position_would_be_created"] is True
    assert scenario["registry"]["adoption"]["exec_details_missing"] is True
    assert scenario["registry"]["adoption"]["completed_order_missing"] is True
    assert {candidate["local_symbol"] for candidate in scenario["registry"]["candidates"]} == {"MNQM6", "MESM6"}
    assert scenario["observability"]["broker_state_mutated"] is False
    assert scenario["observability"]["runtime_restart_invoked"] is False
    assert scenario["passed"] is True


def test_broker_observed_conflicting_candidate_fails_closed(
    tmp_path: Path,
) -> None:
    scenario = _run(tmp_path, SCENARIO_BROKER_OBSERVED_CONFLICTING_CANDIDATE)

    assert scenario["submit_authority"]["classification"] == "BROKER_OBSERVED_FILL_ADOPTION_BLOCKED_CONFLICTING_CANDIDATES"
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["ownership"]["classification"] == "AMBIGUOUS_EXPOSURE_OWNERSHIP"
    assert scenario["ownership"]["owned_exposure_count"] == 0
    assert scenario["registry"]["adoption"]["eligible"] is False
    assert scenario["registry"]["adoption"]["managed_position_created"] is False
    assert scenario["registry"]["adoption"]["competing_candidate_count"] == 2
    assert scenario["registry"]["current_blockers"] == ["conflicting_lifecycle_candidates", "conflicting_trade_ids"]
    assert scenario["observability"]["conflicting_evidence_fails_closed"] is True
    assert scenario["passed"] is True


def test_broker_observed_stale_intent_fails_closed(
    tmp_path: Path,
) -> None:
    scenario = _run(tmp_path, SCENARIO_BROKER_OBSERVED_STALE_INTENT)

    assert scenario["submit_authority"]["classification"] == "BROKER_OBSERVED_FILL_ADOPTION_BLOCKED_STALE_INTENT"
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["registry"]["adoption"]["eligible"] is False
    assert scenario["registry"]["adoption"]["managed_position_created"] is False
    assert scenario["registry"]["current_blockers"] == ["stale_submit_intent"]
    assert "intent_outside_adoption_window" in scenario["registry"]["adoption"]["reason_codes"]
    assert scenario["passed"] is True


def test_simultaneous_mnq_mes_broker_observed_shorts_adopt_without_ambiguity(
    tmp_path: Path,
) -> None:
    scenario = _run(tmp_path, SCENARIO_SIMULTANEOUS_MNQ_MES_BROKER_OBSERVED_SHORT_ADOPTION)

    assert scenario["submit_authority"]["classification"] == "BROKER_OBSERVED_SIMULTANEOUS_SHORTS_ADOPTABLE"
    assert scenario["submit_authority"]["allowed"] is False
    assert scenario["broker_exposure"]["visible"] is True
    assert scenario["broker_exposure"]["count"] == 2
    assert scenario["ownership"]["classification"] == "OWNED_MANAGED_EXPOSURE"
    assert scenario["ownership"]["owned_exposure_count"] == 2
    assert scenario["ownership"]["review_required_exposure_count"] == 0
    assert scenario["registry"]["adoption"]["eligible"] is True
    assert scenario["registry"]["adoption"]["competing_candidate_count"] == 0
    assert scenario["registry"]["adoption"]["managed_position_would_be_created"] is True
    owned = scenario["ownership"]["owned_exposures_after_adoption"]
    assert {item["local_symbol"] for item in owned} == {"MNQM6", "MESM6"}
    assert len({item["lifecycle_id"] for item in owned}) == 2
    assert scenario["passed"] is True


def test_raw_stale_lifecycle_count_is_diagnostic_only_when_current_scope_flat(
    tmp_path: Path,
) -> None:
    scenario = _run(tmp_path, SCENARIO_RAW_STALE_LIFECYCLE_COUNT_CURRENT_SCOPE_FLAT)

    assert scenario["submit_authority"]["classification"] == "CURRENT_SCOPE_FLAT_STALE_LIFECYCLE_COUNT_DIAGNOSTIC_ONLY"
    assert scenario["submit_authority"]["allowed"] is True
    assert scenario["submit_authority"]["blocked_by_raw_lifecycle_open_position_count"] is False
    assert scenario["broker_exposure"]["broker_flat"] is True
    assert scenario["ownership"]["classification"] == "NO_OPEN_EXPOSURE"
    assert scenario["registry"]["raw_lifecycle_open_position_count"] == 1
    assert scenario["registry"]["current_scope_lifecycle_open_position_count"] == 0
    assert scenario["registry"]["stale_superseded_lifecycle_projection_count"] == 1
    assert scenario["registry"]["current_scope_lifecycle_positions"] == []
    assert scenario["registry"]["diagnostic_only_rows"][0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"
    assert scenario["registry"]["diagnostic_only_rows"][0]["diagnostic_only"] is True
    assert scenario["registry"]["restart_precheck_classification"] == "RESTART_ALLOWED_FLAT_RECONCILED"
    assert scenario["registry"]["submit_blocked_by_raw_lifecycle_count"] is False
    assert scenario["registry"]["submit_blocked_by_current_scope_lifecycle_count"] is False
    assert scenario["observability"]["stale_lifecycle_debris_diagnostic_only"] is True
    assert scenario["passed"] is True


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
