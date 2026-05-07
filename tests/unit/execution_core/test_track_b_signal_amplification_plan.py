from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_signal_amplification_plan import (
    TrackBSignalAmplificationPlanConfig,
    create_track_b_signal_amplification_plan,
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_signal_amplification_plan_declares_workstreams_and_first_candidates(tmp_path: Path) -> None:
    activity = tmp_path / "activity.json"
    missed = tmp_path / "missed.json"
    trend = tmp_path / "trend.json"
    _write_json(
        activity,
        {
            "strategies": [
                {
                    "strategy_id": "asian_drift_v1",
                    "instrument": "MGC",
                    "session": "ASIA",
                    "side": "LONG_OR_STATE_EXPLICIT",
                    "strategy_evaluations": 2,
                    "hard_signals": 0,
                    "meaningful_managed_trade_count": 0,
                    "classification": "QUIET_DUE_TO_SESSION_FILTER",
                },
                {
                    "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
                    "instrument": "MNQ",
                    "session": "REGISTRY_SESSION_ALLOWED",
                    "side": "LONG",
                    "strategy_evaluations": 204,
                    "hard_signals": 2,
                    "meaningful_managed_trade_count": 1,
                    "one_predicate_away": 0,
                    "two_predicates_away": 0,
                    "top_failed_predicates": [
                        {"reason": "bull_snap_turn_candidate", "count": 202},
                        {"reason": "bull_snap_raw", "count": 198},
                    ],
                },
            ]
        },
    )
    _write_json(missed, {"classification": "STRATEGY_COVERAGE_GAP_TREND_CONTINUATION"})
    _write_json(trend, {"classification": "TREND_CONTINUATION_COVERAGE_GAP"})

    result = create_track_b_signal_amplification_plan(
        config=TrackBSignalAmplificationPlanConfig(
            repo_root=tmp_path,
            activity_calibration_json=activity,
            missed_move_json=missed,
            trend_gap_json=trend,
            output_json=tmp_path / "plan.json",
            output_md=tmp_path / "plan.md",
        ),
        now=datetime(2026, 5, 7, 21, 30, tzinfo=UTC),
    )

    assert result.report_json.exists()
    assert result.report_md.exists()
    assert result.report["phase"] == "PAPER_STAGE_TRADING_ENGINE"
    assert [row["id"] for row in result.report["workstreams"]] == [
        "repair_parity_amplification",
        "snap_turn_near_miss_amplification",
        "trend_continuation_coverage_amplification",
    ]
    assert result.report["first_repair_candidate"]["strategy_id"] == "asian_drift_v1"
    assert result.report["first_variant_candidate"]["strategy_family"].startswith("FIRST_*_SNAP_TURN")
    assert result.report["first_coverage_candidate"]["managed_paper_eligible"] is False


def test_signal_amplification_plan_expected_frequency_marks_quiet_strategies(tmp_path: Path) -> None:
    activity = tmp_path / "activity.json"
    _write_json(
        activity,
        {
            "strategies": [
                {
                    "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "instrument": "MNQ",
                    "session": "REGISTRY_SESSION_ALLOWED",
                    "side": "SHORT",
                    "strategy_evaluations": 204,
                    "hard_signals": 0,
                    "meaningful_managed_trade_count": 0,
                    "top_failed_predicates": [{"reason": "bear_snap_turn_candidate", "count": 204}],
                }
            ]
        },
    )

    result = create_track_b_signal_amplification_plan(
        config=TrackBSignalAmplificationPlanConfig(
            repo_root=tmp_path,
            activity_calibration_json=activity,
            missed_move_json=tmp_path / "missing_missed.json",
            trend_gap_json=tmp_path / "missing_trend.json",
            output_json=tmp_path / "plan.json",
            output_md=tmp_path / "plan.md",
        ),
        now=datetime(2026, 5, 7, 21, 30, tzinfo=UTC),
    )

    row = result.report["expected_frequency_ranges"][0]
    assert row["strategy_id"] == "MNQ_FIRST_BEAR_SNAP_TURN_V1"
    assert row["activity_classification"] == "too_quiet"
    table = result.report["workstreams"][1]["ranked_failed_predicate_table"]
    assert table[0]["classification"] == "VARIANT_CANDIDATE"
    assert table[0]["numeric_distance_status"] == "NOT_AVAILABLE_IN_CURRENT_ROLLUP"


def test_signal_amplification_plan_keeps_zero_meaningful_trades_when_broker_backed_count_exists(
    tmp_path: Path,
) -> None:
    activity = tmp_path / "activity.json"
    _write_json(
        activity,
        {
            "strategies": [
                {
                    "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    "instrument": "MNQ",
                    "session": "REGISTRY_SESSION_ALLOWED",
                    "side": "SHORT",
                    "strategy_evaluations": 204,
                    "hard_signals": 0,
                    "meaningful_managed_trade_count": 0,
                    "broker_backed_trade_count": 1,
                }
            ]
        },
    )

    result = create_track_b_signal_amplification_plan(
        config=TrackBSignalAmplificationPlanConfig(
            repo_root=tmp_path,
            activity_calibration_json=activity,
            missed_move_json=tmp_path / "missing_missed.json",
            trend_gap_json=tmp_path / "missing_trend.json",
            output_json=tmp_path / "plan.json",
            output_md=tmp_path / "plan.md",
        ),
        now=datetime(2026, 5, 7, 21, 30, tzinfo=UTC),
    )

    row = result.report["expected_frequency_ranges"][0]
    assert row["observed_managed_paper_trades_day"] == 0
