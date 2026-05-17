from __future__ import annotations

from mgc_v05l.research.track_b_entry_exit_candidates import (
    EXACT_BASELINE_ADAPTIVE_24_TO_36_EXIT,
    EXACT_BASELINE_FIXED_36B_EXIT,
    get_track_b_entry_exit_research_candidate,
    track_b_entry_exit_research_candidate_payloads,
    track_b_entry_exit_research_candidates,
)


def test_formal_track_b_entry_exit_candidates_are_registered() -> None:
    candidates = track_b_entry_exit_research_candidates()

    assert candidates == (
        EXACT_BASELINE_FIXED_36B_EXIT,
        EXACT_BASELINE_ADAPTIVE_24_TO_36_EXIT,
    )
    assert {candidate.candidate_id for candidate in candidates} == {
        "track_b_exact_baseline_fixed_36b_exit_v1",
        "track_b_exact_baseline_adaptive_24_to_36_exit_v1",
    }
    assert all(candidate.status == "ACTIVE_RESEARCH_CANDIDATE" for candidate in candidates)
    assert all(
        candidate.candidate_family == "asiaEarlyNormalBreakoutRetestHoldLong"
        for candidate in candidates
    )


def test_candidates_remain_research_only_without_runtime_authority() -> None:
    for candidate in track_b_entry_exit_research_candidates():
        boundary = candidate.authority_boundary

        assert boundary.research_offline_only is True
        assert boundary.paper_eligible is False
        assert boundary.live_eligible is False
        assert boundary.runtime_wired is False
        assert boundary.strategy_behavior_changes is False
        assert boundary.broker_state_mutated is False
        assert boundary.order_intent_created is False
        assert boundary.lifecycle_mutated is False


def test_candidate_provenance_references_full_history_gc_mgc_evidence() -> None:
    for candidate in track_b_entry_exit_research_candidates():
        provenance = candidate.provenance

        assert provenance.date_start == "2020-01-01"
        assert provenance.date_end == "2026-04-22"
        assert provenance.instruments == ("GC", "MGC")
        assert "GC/MGC combined" in provenance.evidence_scope
        assert "full_history_batch" in provenance.study_root
        assert any(path.endswith(".md") for path in provenance.report_paths)
        assert any(path.endswith(".json") for path in provenance.report_paths)


def test_fixed_36_candidate_carries_raw_return_winner_evidence() -> None:
    metrics = EXACT_BASELINE_FIXED_36B_EXIT.evidence_metrics

    assert EXACT_BASELINE_FIXED_36B_EXIT.exit_definition["exit_after_completed_bars"] == 36
    assert metrics["episodes"] == 856
    assert metrics["average_return_points"] == 0.306308
    assert metrics["profit_factor_proxy"] == 1.107389
    assert "Best average return" in metrics["comparison_note"]


def test_adaptive_24_to_36_candidate_is_risk_managed_not_raw_return_winner() -> None:
    fixed_metrics = EXACT_BASELINE_FIXED_36B_EXIT.evidence_metrics
    adaptive = EXACT_BASELINE_ADAPTIVE_24_TO_36_EXIT
    adaptive_metrics = adaptive.evidence_metrics
    extension_conditions = adaptive.exit_definition["extension_conditions"]

    assert adaptive.exit_definition["default_exit_after_completed_bars"] == 24
    assert adaptive.exit_definition["extension_exit_after_completed_bars"] == 36
    assert extension_conditions["net_progress_at_24b_points_gt"] == 0.0
    assert extension_conditions["mfe_mae_ratio_at_24b_gte"] == 1.1
    assert extension_conditions["giveback_pct_at_24b_lte"] == 0.4
    assert adaptive_metrics["profit_factor_proxy"] > fixed_metrics["profit_factor_proxy"]
    assert (
        adaptive_metrics["max_drawdown_proxy_points"]
        < fixed_metrics["max_drawdown_proxy_points"]
    )
    assert adaptive_metrics["average_return_points"] < fixed_metrics["average_return_points"]
    assert adaptive_metrics["branch_counts"] == {"exit_24b": 646, "extend_to_36b": 210}
    assert "not the raw-return winner" in adaptive_metrics["comparison_note"]


def test_candidate_payloads_and_lookup_are_artifact_friendly() -> None:
    payloads = track_b_entry_exit_research_candidate_payloads()

    assert payloads[0]["candidate_id"] == "track_b_exact_baseline_fixed_36b_exit_v1"
    assert payloads[0]["authority_boundary"]["runtime_wired"] is False
    assert (
        get_track_b_entry_exit_research_candidate(
            "track_b_exact_baseline_adaptive_24_to_36_exit_v1"
        )
        is EXACT_BASELINE_ADAPTIVE_24_TO_36_EXIT
    )
