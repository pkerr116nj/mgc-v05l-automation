from __future__ import annotations

import json
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState
from mgc_v05l.execution_core.track_b_lifecycle_stress_framework import (
    CONTRACT_ENTRY_CLOSE_ONLY,
    LifecycleStressConfig,
    LifecycleStressRunMode,
    LifecycleStressScenario,
    STRESS_EXPECTED_REVIEW_REQUIRED,
    STRESS_INVARIANT_FAILED,
    STRESS_PASSED,
    run_lifecycle_stress,
    write_lifecycle_stress_report,
)


def test_seeded_stress_run_is_deterministic() -> None:
    config = LifecycleStressConfig(count=40, seed=42)

    first = run_lifecycle_stress(config)
    second = run_lifecycle_stress(config)

    assert first.to_dict()["summary"] == second.to_dict()["summary"]
    assert [result.trade_id for result in first.results] == [result.trade_id for result in second.results]
    assert first.trade_id_collisions == ()
    assert first.read_only is True
    assert first.broker_mutation_allowed is False
    assert first.runtime_restart_allowed is False


def test_staged_run_ladder_counts_and_goal_metrics() -> None:
    smoke = run_lifecycle_stress(LifecycleStressConfig(mode=LifecycleStressRunMode.SMOKE, seed=5))
    known = run_lifecycle_stress(LifecycleStressConfig(mode=LifecycleStressRunMode.KNOWN_SCENARIOS, seed=5))

    assert smoke.requested_count == 20
    assert smoke.summary["total_trades"] == 20
    assert smoke.summary["stage_goal_zero_failures"]["zero_crashes"] is True
    assert smoke.summary["stage_goal_zero_failures"]["zero_silent_merges"] is True
    assert smoke.summary["stage_goal_zero_failures"]["zero_trade_id_collisions"] is True
    assert smoke.summary["stage_goal_zero_failures"]["zero_impossible_states"] is True
    assert smoke.summary["stage_goal_zero_failures"]["every_bad_lifecycle_has_reason_codes"] is True
    assert known.requested_count == 260
    assert known.summary["expected_review_required"] > 0
    assert known.summary["gate_shadow_total_checks"] == known.summary["total_trades"] * 6
    assert known.summary["gate_shadow_mismatches"] == 0
    assert known.summary["gate_shadow_safety_regressions"] == 0


def test_expected_counts_and_classifications_for_small_mix() -> None:
    config = LifecycleStressConfig(
        count=8,
        seed=7,
        scenario_mix=(
            LifecycleStressScenario.CLEAN_FULL_LIFECYCLE,
            LifecycleStressScenario.PASSIVE_ENTRY_CANCEL,
            LifecycleStressScenario.MISSING_PERM_OR_EXEC,
            LifecycleStressScenario.CONTRACT_CLOSE_ONLY,
        ),
    )

    report = run_lifecycle_stress(config)
    states = {result.registry_state for result in report.results}

    assert report.summary["total_trades"] == 8
    assert report.summary["trade_id_collisions"] == 0
    assert TradeCurrentState.CLOSED_FLAT.value in states
    assert TradeCurrentState.CANCELLED.value in states
    assert TradeCurrentState.REVIEW_REQUIRED.value in states
    assert report.summary["scenario_breakdown"]
    assert report.summary["gate_shadow_total_checks"] == 48
    assert report.summary["gate_shadow_mismatches"] == 0


def test_induced_invalid_scenarios_produce_review_or_invariant_failures() -> None:
    reports = [
        run_lifecycle_stress(LifecycleStressConfig(count=1, seed=1, scenario_mix=(scenario,)))
        for scenario in (
            LifecycleStressScenario.MISSING_PERM_OR_EXEC,
            LifecycleStressScenario.WRONG_CONTRACT_IDENTITY,
            LifecycleStressScenario.WRONG_LIFECYCLE_ID,
            LifecycleStressScenario.DUPLICATE_FILL,
            LifecycleStressScenario.DUPLICATE_CLOSE,
            LifecycleStressScenario.RECONCILIATION_AMBIGUITY,
            LifecycleStressScenario.STALE_ARTIFACT_RESURRECTION,
            LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE,
            LifecycleStressScenario.RECOVERY_AMBIGUOUS_TRADE_IDS,
            LifecycleStressScenario.RECOVERY_LIFECYCLE_OWNER_CONFLICT,
            LifecycleStressScenario.RECOVERY_STALE_HISTORY_NO_ADOPT,
        )
    ]
    results = [report.results[0] for report in reports]

    assert all(result.classification in {STRESS_EXPECTED_REVIEW_REQUIRED, STRESS_INVARIANT_FAILED} for result in results)
    assert any("BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC" in result.invariant_failures for result in results)
    assert any("DUPLICATE_ENTRY_FILL" in result.invariant_failures for result in results)
    assert any("DUPLICATE_CLOSE_FILL" in result.invariant_failures for result in results)
    reconciliation_cases = {
        result.scenario: result for result in results if result.scenario in {
            LifecycleStressScenario.RECONCILIATION_AMBIGUITY.value,
            LifecycleStressScenario.STALE_ARTIFACT_RESURRECTION.value,
        }
    }
    assert reconciliation_cases[LifecycleStressScenario.RECONCILIATION_AMBIGUITY.value].registry_state == "REVIEW_REQUIRED"
    assert reconciliation_cases[LifecycleStressScenario.STALE_ARTIFACT_RESURRECTION.value].registry_state == "REVIEW_REQUIRED"
    recovery_cases = {
        result.scenario: result for result in results if result.scenario.startswith("recovery_")
    }
    assert recovery_cases[LifecycleStressScenario.RECOVERY_MISSING_BROKER_EVIDENCE.value].registry_state == "REVIEW_REQUIRED"
    assert recovery_cases[LifecycleStressScenario.RECOVERY_AMBIGUOUS_TRADE_IDS.value].registry_state == "REVIEW_REQUIRED"
    assert recovery_cases[LifecycleStressScenario.RECOVERY_LIFECYCLE_OWNER_CONFLICT.value].registry_state == "REVIEW_REQUIRED"
    assert recovery_cases[LifecycleStressScenario.RECOVERY_STALE_HISTORY_NO_ADOPT.value].registry_state == "REVIEW_REQUIRED"


def test_contract_close_only_blocks_entry_and_preserves_exit_management() -> None:
    report = run_lifecycle_stress(
        LifecycleStressConfig(
            count=1,
            seed=99,
            scenario_mix=(LifecycleStressScenario.CONTRACT_CLOSE_ONLY,),
        )
    )
    result = report.results[0]

    assert result.registry_state == TradeCurrentState.REVIEW_REQUIRED.value
    assert result.classification == STRESS_EXPECTED_REVIEW_REQUIRED
    assert CONTRACT_ENTRY_CLOSE_ONLY in result.truth_reason_codes
    assert result.shadow_row is not None
    assert result.shadow_row["symbol"] in {"MGC", "GC"}


def test_passed_scenarios_have_shadow_report_style_rows() -> None:
    report = run_lifecycle_stress(
        LifecycleStressConfig(
            count=3,
            seed=3,
            scenario_mix=(
                LifecycleStressScenario.CLEAN_FULL_LIFECYCLE,
                LifecycleStressScenario.RECOVERY_ADOPTION,
                LifecycleStressScenario.RECOVERY_REGISTRY_BACKED_RESUME,
                LifecycleStressScenario.MANUAL_OPERATOR_CLOSE,
            ),
        )
    )

    assert all(result.classification == STRESS_PASSED for result in report.results)
    for result in report.results:
        assert result.shadow_row is not None
        assert result.shadow_row["trade_id"] == result.trade_id
        assert "event_chain_summary" in result.shadow_row


def test_stress_report_json_roundtrip(tmp_path: Path) -> None:
    report = run_lifecycle_stress(
        LifecycleStressConfig(count=12, seed=11, scenario_mix=(LifecycleStressScenario.CLEAN_FULL_LIFECYCLE,))
    )
    path = write_lifecycle_stress_report(tmp_path / "stress_report.json", report)
    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "track_b_lifecycle_stress_framework_v1"
    assert payload["summary"]["total_trades"] == 12
    assert payload["summary"]["passed"] == 12
    assert payload["results"][0]["shadow_row"]["current_derived_state"] == TradeCurrentState.CLOSED_FLAT.value
    assert payload["summary"]["gate_shadow_total_checks"] == 72
    assert payload["summary"]["gate_shadow_mismatches"] == 0
