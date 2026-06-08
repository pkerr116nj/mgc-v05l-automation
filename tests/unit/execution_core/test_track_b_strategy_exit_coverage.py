from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_strategy_exit_coverage import (
    EXIT_COVERAGE_COMPLETE,
    EXIT_POLICY_MISSING,
    EXIT_POLICY_PRESENT_BUT_NOT_AUTO_ACTUATED,
    TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND,
    TRACK_B_STRATEGY_EXIT_COVERAGE_READY,
    TrackBStrategyExitCoverageConfig,
    build_track_b_strategy_exit_coverage_report,
    lane_exit_coverage_for,
)


def test_active_evidence_lane_with_complete_exit_coverage_is_ready(tmp_path: Path) -> None:
    report = build_track_b_strategy_exit_coverage_report(
        config=TrackBStrategyExitCoverageConfig(repo_root=tmp_path),
        now=datetime(2026, 6, 8, 12, 0, tzinfo=UTC),
        config_in_force={
            "profile": "test",
            "lanes": [
                {
                    "lane_id": "mnq_us_active_participation_long",
                    "lane_mode": "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
                    "symbol": "MNQ",
                }
            ],
        },
    )

    assert report["classification"] == TRACK_B_STRATEGY_EXIT_COVERAGE_READY
    row = lane_exit_coverage_for("mnq_us_active_participation_long", report)
    assert row["classification"] == EXIT_COVERAGE_COMPLETE
    assert row["exit_policy_name"] == "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert row["managed_exit_actuator_coverage"] == "track_b_managed_exit_actuator"


def test_entry_lane_without_exit_policy_is_blocked(tmp_path: Path) -> None:
    report = build_track_b_strategy_exit_coverage_report(
        config=TrackBStrategyExitCoverageConfig(repo_root=tmp_path),
        config_in_force={
            "lanes": [
                {
                    "lane_id": "uncovered_active_lane",
                    "lane_mode": "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
                    "symbol": "MNQ",
                }
            ],
        },
    )

    assert report["classification"] == TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND
    row = lane_exit_coverage_for("uncovered_active_lane", report)
    assert row["classification"] == EXIT_POLICY_MISSING
    assert "explicit_exit_policy" in row["missing_or_weak_pieces"]


def test_exit_policy_without_lifecycle_template_is_not_auto_actuated(tmp_path: Path) -> None:
    report = build_track_b_strategy_exit_coverage_report(
        config=TrackBStrategyExitCoverageConfig(repo_root=tmp_path),
        config_in_force={
            "lanes": [
                {
                    "lane_id": "custom_active_lane",
                    "lane_mode": "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
                    "symbol": "MNQ",
                    "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
                }
            ],
        },
    )

    row = lane_exit_coverage_for("custom_active_lane", report)
    assert row["classification"] == EXIT_POLICY_PRESENT_BUT_NOT_AUTO_ACTUATED
    assert "managed_lifecycle_registration" in row["missing_or_weak_pieces"]


def test_multiple_active_profile_lanes_report_all_blocked_lanes(tmp_path: Path) -> None:
    report = build_track_b_strategy_exit_coverage_report(
        config=TrackBStrategyExitCoverageConfig(repo_root=tmp_path),
        config_in_force={
            "lanes": [
                {
                    "lane_id": "mnq_us_active_participation_long",
                    "lane_mode": "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
                    "symbol": "MNQ",
                },
                {
                    "lane_id": "uncovered_active_lane",
                    "lane_mode": "PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
                    "symbol": "MNQ",
                },
            ],
        },
    )

    assert report["classification"] == TRACK_B_STRATEGY_EXIT_COVERAGE_GAPS_FOUND
    assert report["complete_strategy_count"] == 1
    assert report["blocked_lanes"] == ["uncovered_active_lane"]
