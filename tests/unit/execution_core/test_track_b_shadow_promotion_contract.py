from __future__ import annotations

from mgc_v05l.execution_core.track_b_shadow_promotion_contract import (
    ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID,
    ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID,
    ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND,
    ATP_MGC_ASIA_EDGE_V1_PROMOTED_ID,
    ATP_MGC_ASIA_PROMOTION_1_075R_5M_PROMOTED_ID,
    ATP_MGC_ASIA_PROMOTION_1_075R_PROMOTED_ID,
    LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID,
    MNQ_US_DERIVATIVE_BEAR_TURN_PROMOTED_ID,
    MNQ_US_MIDDAY_PAUSE_RESUME_SHORT_TURN_PROMOTED_ID,
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID,
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID,
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_PROMOTED_ID,
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID,
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID,
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_PROMOTED_ID,
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
        PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID,
        PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID,
        PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID,
        PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID,
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


def test_mnq_us_derivative_bear_turn_exports_reusable_guarded_paper_lane() -> None:
    report = build_shadow_promotion_contract_report(
        {"enabled_strategy_ids": [MNQ_US_DERIVATIVE_BEAR_TURN_PROMOTED_ID]}
    )
    candidate = next(
        row
        for row in report["promotion_candidates"]
        if row["promoted_strategy_id"] == MNQ_US_DERIVATIVE_BEAR_TURN_PROMOTED_ID
    )
    rows = promoted_probationary_paper_lane_rows(
        {"enabled_strategy_ids": [MNQ_US_DERIVATIVE_BEAR_TURN_PROMOTED_ID]}
    )

    assert candidate["classification"] == PROMOTION_CANDIDATE_GUARDED_PAPER_READY
    assert candidate["blockers"] == []
    assert candidate["submit_allowed"] is True
    assert len(rows) == 1
    row = rows[0]
    assert row["lane_id"] == "mnq_us_derivative_bear_turn"
    assert row["symbol"] == "MNQ"
    assert row["runtime_kind"] == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
    assert row["short_sources"] == [MNQ_US_DERIVATIVE_BEAR_TURN_PROMOTED_ID]
    assert row["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert row["runtime_overlay_params"]["input_event_path"].endswith(
        "latest_mnq_us_derivative_bear_turn_event_envelope.json"
    )
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert row["broad_cancel_flatten_allowed"] is False
    assert row["unguarded_broker_mutation_allowed"] is False


def test_mnq_us_midday_pause_resume_short_turn_is_future_promotion_shadow_candidate() -> None:
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": []})
    candidate = next(
        row
        for row in report["promotion_candidates"]
        if row["promoted_strategy_id"] == MNQ_US_MIDDAY_PAUSE_RESUME_SHORT_TURN_PROMOTED_ID
    )

    assert candidate["classification"] == PROMOTION_CANDIDATE_SHADOW_ONLY
    assert candidate["submit_allowed"] is False
    assert candidate["live_money_eligible"] is False
    assert candidate["paper_proof_invoked"] is False
    assert candidate["probationary_paper_lane_row"]["lane_id"] == "mnq_us_midday_pause_resume_short_turn"
    assert candidate["probationary_paper_lane_row"]["symbol"] == "MNQ"
    assert candidate["probationary_paper_lane_row"]["runtime_overlay_params"]["input_event_path"].endswith(
        "latest_mnq_us_midday_pause_resume_short_turn_event_envelope.json"
    )


def test_london_open_active_evidence_cohort_exports_canonical_contract_paper_rows() -> None:
    enabled = [
        PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID,
        PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID,
        PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID,
        PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID,
    ]
    rows = promoted_probationary_paper_lane_rows({"enabled_strategy_ids": enabled})

    by_strategy = {row["standalone_strategy_id"]: row for row in rows}
    assert set(by_strategy) == set(enabled)
    assert by_strategy[PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID]["lane_id"] == (
        "mnq_london_open_active_participation_long"
    )
    assert by_strategy[PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID]["short_sources"] == [
        PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID
    ]
    assert by_strategy[PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID]["symbol"] == "MES"
    assert by_strategy[PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID]["symbol"] == "MES"
    for row in by_strategy.values():
        assert row["lane_mode"] == "PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE"
        assert row["runtime_kind"] == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
        assert row["session_restriction"] == "LONDON_OPEN"
        assert row["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
        assert row["structural_signal_timeframe"] == "1m"
        assert row["artifact_timeframe"] == "1m"
        assert row["context_timeframes"] == ["1m"]
        assert row["participation_policy"] == "SINGLE_ENTRY_ONLY"
        assert row["max_position_quantity"] == 1
        assert row["max_concurrent_entries"] == 1
        assert row["live_money_eligible"] is False
        assert row["paper_proof_invoked"] is False
        assert row["broad_cancel_flatten_allowed"] is False
        assert row["unguarded_broker_mutation_allowed"] is False


def test_london_late_mnq_short_active_evidence_cohort_exports_single_guarded_row() -> None:
    rows = promoted_probationary_paper_lane_rows(
        {"enabled_strategy_ids": [PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_PROMOTED_ID]}
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["standalone_strategy_id"] == PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_PROMOTED_ID
    assert row["lane_id"] == "mnq_london_late_active_participation_short"
    assert row["symbol"] == "MNQ"
    assert row["local_symbol"] == "MNQM6"
    assert row["con_id"] == 770561201
    assert row["lane_mode"] == "PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE"
    assert row["runtime_kind"] == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
    assert row["session_restriction"] == "LONDON_LATE"
    assert row["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert row["participation_policy"] == "SINGLE_ENTRY_ONLY"
    assert row["max_position_quantity"] == 1
    assert row["max_concurrent_entries"] == 1
    assert row["conflict_group"] == "equity_index_mnq_mes_london_late_active_evidence"
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert row["broad_cancel_flatten_allowed"] is False
    assert row["unguarded_broker_mutation_allowed"] is False


def test_london_late_mes_short_active_evidence_cohort_exports_guarded_row() -> None:
    rows = promoted_probationary_paper_lane_rows(
        {"enabled_strategy_ids": [PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_PROMOTED_ID]}
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["standalone_strategy_id"] == PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_PROMOTED_ID
    assert row["lane_id"] == "mes_london_late_active_participation_short"
    assert row["symbol"] == "MES"
    assert row["local_symbol"] == "MESM6"
    assert row["con_id"] == 770561194
    assert row["lane_mode"] == "PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE"
    assert row["runtime_kind"] == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
    assert row["session_restriction"] == "LONDON_LATE"
    assert row["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    assert row["participation_policy"] == "SINGLE_ENTRY_ONLY"
    assert row["max_position_quantity"] == 1
    assert row["max_concurrent_entries"] == 1
    assert row["conflict_group"] == "equity_index_mnq_mes_london_late_active_evidence"
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert row["broad_cancel_flatten_allowed"] is False
    assert row["unguarded_broker_mutation_allowed"] is False


def test_batch2_rates_active_evidence_cohort_exports_validated_contract_rows() -> None:
    enabled = [
        "PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_ZF_GLOBEX_PARTICIPATION_SHORT_V1",
        "PAPER_ACTIVE_EVIDENCE_ZN_LONDON_OPEN_PARTICIPATION_LONG_V1",
        "PAPER_ACTIVE_EVIDENCE_ZB_LONDON_LATE_PARTICIPATION_SHORT_V1",
    ]
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": enabled})
    rows = promoted_probationary_paper_lane_rows({"enabled_strategy_ids": enabled})

    expected = {
        "PAPER_ACTIVE_EVIDENCE_ZT_US_PARTICIPATION_LONG_V1": ("zt_us_active_participation_long", "ZT", "ZTU6", 842590391),
        "PAPER_ACTIVE_EVIDENCE_ZF_GLOBEX_PARTICIPATION_SHORT_V1": (
            "zf_globex_active_participation_short",
            "ZF",
            "ZFU6",
            842590380,
        ),
        "PAPER_ACTIVE_EVIDENCE_ZN_LONDON_OPEN_PARTICIPATION_LONG_V1": (
            "zn_london_open_active_participation_long",
            "ZN",
            "ZNU6",
            840227361,
        ),
        "PAPER_ACTIVE_EVIDENCE_ZB_LONDON_LATE_PARTICIPATION_SHORT_V1": (
            "zb_london_late_active_participation_short",
            "ZB",
            "ZBU6",
            840227357,
        ),
    }

    by_strategy = {row["standalone_strategy_id"]: row for row in rows}
    candidates = {row["promoted_strategy_id"]: row for row in report["promotion_candidates"]}
    assert set(by_strategy) == set(enabled)
    for strategy_id, (lane_id, symbol, local_symbol, con_id) in expected.items():
        row = by_strategy[strategy_id]
        candidate = candidates[strategy_id]
        assert row["lane_id"] == lane_id
        assert row["symbol"] == symbol
        assert row["local_symbol"] == local_symbol
        assert row["con_id"] == con_id
        assert row["conflict_group"] == "rates_treasury_active_evidence"
        assert candidate["evidence_summary"]["activation_batch"] == "PAPER_NOISEMAKER_CHAOS_BATCH_2"
        assert row["max_position_quantity"] == 1
        assert row["live_money_eligible"] is False
        assert row["paper_proof_invoked"] is False
        assert row["broad_cancel_flatten_allowed"] is False
        assert row["unguarded_broker_mutation_allowed"] is False


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
