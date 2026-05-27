from __future__ import annotations

from mgc_v05l.execution_core.track_b_shadow_promotion_contract import (
    ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID,
    ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID,
    ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND,
    ATP_MGC_ASIA_EDGE_V1_PROMOTED_ID,
    ATP_MGC_ASIA_PROMOTION_1_075R_5M_PROMOTED_ID,
    ATP_MGC_ASIA_PROMOTION_1_075R_PROMOTED_ID,
    LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID,
    PROMOTION_CANDIDATE_GUARDED_PAPER_READY,
    PROMOTION_CANDIDATE_SHADOW_ONLY,
    PROMOTION_CONTRACT_READY,
    TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
    US_DERIVATIVE_BEAR_TURN_PROMOTED_ID,
    build_shadow_promotion_contract_report,
    promoted_probationary_paper_lane_rows,
)


def test_shadow_candidate_remains_non_authoritative_until_roster_enabled() -> None:
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": ["asian_drift_v1"]})

    assert report["classification"] == PROMOTION_CONTRACT_READY
    for row in report["promotion_candidates"]:
        assert row["classification"] == PROMOTION_CANDIDATE_SHADOW_ONLY
        assert row["submit_allowed"] is False
        assert row["live_money_eligible"] is False
        assert row["paper_proof_invoked"] is False
        assert row["broker_mutation_allowed"] is False


def test_explicit_roster_enablement_makes_candidate_guarded_paper_ready_not_live_money() -> None:
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": [ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID]})
    row = next(item for item in report["promotion_candidates"] if item["promoted_strategy_id"] == ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID)

    assert row["classification"] == PROMOTION_CANDIDATE_GUARDED_PAPER_READY
    assert row["submit_allowed"] is True
    assert row["lifecycle_authority"] is True
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID in report["broker_authoritative_promoted_strategy_ids"]


def test_mapped_mgc_atp_candidates_promote_to_guarded_paper_without_live_money() -> None:
    enabled = [
        ATP_MGC_ASIA_EDGE_V1_PROMOTED_ID,
        ATP_MGC_ASIA_PROMOTION_1_075R_PROMOTED_ID,
        ATP_MGC_ASIA_PROMOTION_1_075R_5M_PROMOTED_ID,
    ]
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": enabled})

    rows = {
        row["promoted_strategy_id"]: row
        for row in report["promotion_candidates"]
        if row["promoted_strategy_id"] in enabled
    }
    assert set(rows) == set(enabled)
    for row in rows.values():
        assert row["classification"] == PROMOTION_CANDIDATE_GUARDED_PAPER_READY
        assert row["submit_allowed"] is True
        assert row["lifecycle_authority"] is True
        assert row["live_money_eligible"] is False
        assert row["paper_proof_invoked"] is False
        assert row["probationary_paper_lane_row"]["runtime_kind"] == ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND
        assert row["probationary_paper_lane_row"]["non_approved"] is False
        assert row["probationary_paper_lane_row"]["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


def test_guarded_paper_ready_candidate_exports_probationary_paper_lane_row() -> None:
    rows = promoted_probationary_paper_lane_rows({"enabled_strategy_ids": [ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID]})

    assert len(rows) == 1
    row = rows[0]
    assert row["runtime_kind"] == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
    assert row["standalone_strategy_id"] == ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID
    assert row["long_sources"] == [ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID]
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert row["broad_cancel_flatten_allowed"] is False
    assert row["runtime_overlay_params"]["require_timestamp_coherence"] is True


def test_approved_short_side_candidates_export_rule_runner_rows_with_specific_event_artifacts() -> None:
    enabled = [
        ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID,
        LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID,
        US_DERIVATIVE_BEAR_TURN_PROMOTED_ID,
    ]
    rows = promoted_probationary_paper_lane_rows({"enabled_strategy_ids": enabled})

    by_strategy = {row["standalone_strategy_id"]: row for row in rows}
    assert set(by_strategy) == set(enabled)
    assert by_strategy[ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID]["short_sources"] == [
        ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID
    ]
    assert by_strategy[LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID]["session_restriction"] == "LONDON_LATE"
    assert by_strategy[US_DERIVATIVE_BEAR_TURN_PROMOTED_ID]["session_restriction"] == "US"
    for strategy_id, row in by_strategy.items():
        assert row["runtime_kind"] == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
        assert row["live_money_eligible"] is False
        assert row["paper_proof_invoked"] is False
        assert row["submit_capable_without_promotion_contract"] is False
        assert row["runtime_overlay_params"]["strategy_id"] == strategy_id
        assert row["runtime_overlay_params"]["input_event_path"].startswith(
            "outputs/track_b_execution_core/session_strategy_state/latest_"
        )


def test_promotion_contract_reports_remaining_shadow_only_exception_groups() -> None:
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": [ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID]})

    exceptions = report["remaining_shadow_only_exceptions"]
    assert {row["candidate_group"] for row in exceptions} >= {
        "ATP_GC_CANDIDATES",
        "ATP_PL_CANDIDATES",
        "GENERAL_NEAR_MISS_C_GRADE",
    }
    assert all(row["submit_allowed"] is False for row in exceptions)
    assert all(row["live_money_eligible"] is False for row in exceptions)
