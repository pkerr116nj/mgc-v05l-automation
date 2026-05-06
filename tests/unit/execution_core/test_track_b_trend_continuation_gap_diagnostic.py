from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_strategy_registry import get_track_b_strategy_registry
from mgc_v05l.execution_core.track_b_trend_continuation_gap_diagnostic import (
    OVERLAY_ID,
    build_track_b_trend_continuation_gap_diagnostic,
)


NOW = datetime(2026, 5, 6, 21, 0, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_postmortem(tmp_path: Path) -> Path:
    return write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "diagnostics" / "latest_track_b_same_day_postmortem.json",
        {
            "classifications": [
                "EXECUTION_PATH_WORKED_BUT_STRATEGY_FAILED_TO_PARTICIPATE",
                "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION",
                "STRATEGY_GATES_MUTED_NEAR_MISSES",
                "TRADE_LIFECYCLE_TOO_SHORT_OR_OVER_FLATTENED",
            ],
            "trade_lifecycle_reconstruction": [
                {
                    "strategy": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "instrument": "MNQ",
                    "closed_by_guarded_paper_proof_lifecycle": True,
                    "closed_by_normal_strategy_logic": False,
                    "hold_duration_seconds": 0.07884,
                    "realized_pnl": "-1",
                }
            ],
            "missed_rally_window_review": {
                "MGC": {
                    "main_rally_window": {
                        "start_timestamp": "2026-05-05T22:50:00+00:00",
                        "end_timestamp": "2026-05-06T10:50:00+00:00",
                        "start_price": "4562.1",
                        "end_price": "4733.3",
                        "point_change": "171.2",
                    },
                    "near_misses": {"one_predicate_away": 9, "two_predicates_away": 25},
                    "top_failed_predicates": [{"reason": "bull_snap_close_strong", "count": 201}],
                    "dominant_blocker": "COVERAGE_GAP",
                    "long_side_trend_continuation_coverage_exists": False,
                },
                "MNQ": {
                    "main_rally_window": {
                        "start_timestamp": "2026-05-06T00:30:00+00:00",
                        "end_timestamp": "2026-05-06T20:10:00+00:00",
                        "start_price": "28250.5",
                        "end_price": "28793.25",
                        "point_change": "542.75",
                    },
                    "near_misses": {"one_predicate_away": 0, "two_predicates_away": 5},
                    "top_failed_predicates": [{"reason": "normalized_curvature_below_threshold", "count": 189}],
                    "dominant_blocker": "COVERAGE_GAP",
                    "long_side_trend_continuation_coverage_exists": False,
                },
            },
            "simple_baseline_comparison": {
                "track_b_realized_pnl_today": "-1",
                "missed_opportunity_magnitude": {
                    "instrument": "MGC",
                    "baseline": "buy_at_session_start_exit_latest",
                    "baseline_pnl": "937.0",
                    "track_b_realized_pnl": "-1",
                    "missed_vs_best_available_baseline": "938.0",
                },
            },
        },
    )


def test_proof_style_trade_is_excluded_from_meaningful_participation(tmp_path: Path) -> None:
    postmortem = write_postmortem(tmp_path)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=tmp_path,
        postmortem_path=postmortem,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    fixture = result.report["missed_rally_failure_fixture"]
    assert fixture["actual_track_b_meaningful_managed_trades"] == 0
    assert fixture["proof_style_lifecycle_trades"] == 1
    assert fixture["proof_style_trade_excluded_from_meaningful_participation"] is True


def test_major_rally_fixture_classifies_as_trend_continuation_gap(tmp_path: Path) -> None:
    postmortem = write_postmortem(tmp_path)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=tmp_path,
        postmortem_path=postmortem,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert "TREND_CONTINUATION_COVERAGE_GAP" in result.report["classifications"]
    assert result.report["missed_rally_failure_fixture"]["expected_classification"] == "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION"
    assert result.report["trend_continuation_gap_assessment"]["today_contained_long_trend_continuation_regime"] is True


def test_near_misses_are_preserved_but_not_counted_as_participation(tmp_path: Path) -> None:
    postmortem = write_postmortem(tmp_path)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=tmp_path,
        postmortem_path=postmortem,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    attempts = result.report["trend_continuation_gap_assessment"]["existing_strategy_family_attempted_coverage"]
    assert attempts["MGC"]["near_misses"]["one_predicate_away"] == 9
    assert result.report["missed_rally_failure_fixture"]["actual_track_b_meaningful_managed_trades"] == 0
    assert "EXISTING_STRATEGY_NEAR_MISS" in result.report["classifications"]


def test_research_overlay_cannot_submit_and_is_not_paper_eligible(tmp_path: Path) -> None:
    postmortem = write_postmortem(tmp_path)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=tmp_path,
        postmortem_path=postmortem,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    overlay = result.report["research_overlay_scaffold"]
    assert overlay["strategy_id"] == OVERLAY_ID
    assert overlay["research_only"] is True
    assert overlay["paper_eligible"] is False
    assert overlay["live_money_eligible"] is False
    assert overlay["can_submit"] is False
    assert overlay["can_handoff_to_paper_lifecycle"] is False
    assert result.report["safety"]["broker_commands_invoked"] is False
    assert result.report["safety"]["paper_proof_cli_invoked"] is False


def test_existing_track_b_registry_is_unchanged(tmp_path: Path) -> None:
    postmortem = write_postmortem(tmp_path)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=tmp_path,
        postmortem_path=postmortem,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert result.report["research_overlay_scaffold"]["registered_in_track_b_strategy_registry"] is False
    assert OVERLAY_ID not in {entry.strategy_id for entry in get_track_b_strategy_registry()}


def test_existing_track1_candidates_are_reported_not_invented(tmp_path: Path) -> None:
    postmortem = write_postmortem(tmp_path)
    result = build_track_b_trend_continuation_gap_diagnostic(
        repo_root=Path("/Users/patrick/Dev/MGC-v05l-automation"),
        postmortem_path=postmortem,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    names = {candidate["candidate_name"] for candidate in result.report["candidate_strategy_search"]}
    assert "NDX UP opening-drive same-day long-bias signal extraction" in names
    assert "ATP_COMPANION_V1_ASIA_US / ATP v1 pullback continuation" in names
    assert "RESEARCH_CANDIDATE_FOUND_IN_TRACK_1" in result.report["classifications"]
