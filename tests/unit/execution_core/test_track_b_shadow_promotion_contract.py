from __future__ import annotations

from mgc_v05l.execution_core.track_b_shadow_promotion_contract import (
    ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID,
    PROMOTION_CANDIDATE_GUARDED_PAPER_READY,
    PROMOTION_CANDIDATE_SHADOW_ONLY,
    PROMOTION_CONTRACT_READY,
    TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
    build_shadow_promotion_contract_report,
    promoted_probationary_paper_lane_rows,
)


def test_shadow_candidate_remains_non_authoritative_until_roster_enabled() -> None:
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": ["asian_drift_v1"]})
    row = report["promotion_candidates"][0]

    assert report["classification"] == PROMOTION_CONTRACT_READY
    assert row["classification"] == PROMOTION_CANDIDATE_SHADOW_ONLY
    assert row["submit_allowed"] is False
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert row["broker_mutation_allowed"] is False


def test_explicit_roster_enablement_makes_candidate_guarded_paper_ready_not_live_money() -> None:
    report = build_shadow_promotion_contract_report({"enabled_strategy_ids": [ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID]})
    row = report["promotion_candidates"][0]

    assert row["classification"] == PROMOTION_CANDIDATE_GUARDED_PAPER_READY
    assert row["submit_allowed"] is True
    assert row["lifecycle_authority"] is True
    assert row["live_money_eligible"] is False
    assert row["paper_proof_invoked"] is False
    assert ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID in report["broker_authoritative_promoted_strategy_ids"]


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
