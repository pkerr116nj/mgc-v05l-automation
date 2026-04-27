from __future__ import annotations

import csv
from pathlib import Path

from mgc_v05l.research.us_open_follow_through.probabilistic_pass2 import (
    DAY_TYPE_NON_TREND,
    DAY_TYPE_TREND_DOWN,
    DAY_TYPE_TREND_UP,
    _classify_day_type,
    run_probabilistic_pass2,
)


def test_classify_day_type_uses_1530_and_close_persistence() -> None:
    assert _classify_day_type(5.0, 8.0) == DAY_TYPE_TREND_UP
    assert _classify_day_type(-4.0, -2.0) == DAY_TYPE_TREND_DOWN
    assert _classify_day_type(3.0, -1.0) == DAY_TYPE_NON_TREND
    assert _classify_day_type(None, 2.0) == DAY_TYPE_NON_TREND


def test_run_probabilistic_pass2_writes_expected_artifacts(tmp_path: Path) -> None:
    pass1_root = tmp_path / "pass1"
    (pass1_root / "features").mkdir(parents=True)
    (pass1_root / "signals").mkdir(parents=True)
    output_dir = tmp_path / "pass2"

    candidate_rows = [
        {
            "candidate_id": "ES|dev|up",
            "instrument": "ES",
            "direction": "UP",
            "opening_drive_signed_return_points": 5.0,
            "overnight_direction": "UP",
            "direction_60m_state": "UP",
            "cross_index_confirmation": True,
        },
        {
            "candidate_id": "ES|dev|down",
            "instrument": "ES",
            "direction": "DOWN",
            "opening_drive_signed_return_points": 15.0,
            "overnight_direction": "DOWN",
            "direction_60m_state": "DOWN",
            "cross_index_confirmation": False,
        },
        {
            "candidate_id": "NQ|dev|up",
            "instrument": "NQ",
            "direction": "UP",
            "opening_drive_signed_return_points": 40.0,
            "overnight_direction": "DOWN",
            "direction_60m_state": "UP",
            "cross_index_confirmation": True,
        },
        {
            "candidate_id": "ES|hold|up",
            "instrument": "ES",
            "direction": "UP",
            "opening_drive_signed_return_points": 7.0,
            "overnight_direction": "UP",
            "direction_60m_state": "UP",
            "cross_index_confirmation": True,
        },
        {
            "candidate_id": "NQ|hold|down",
            "instrument": "NQ",
            "direction": "DOWN",
            "opening_drive_signed_return_points": 60.0,
            "overnight_direction": "DOWN",
            "direction_60m_state": "DOWN",
            "cross_index_confirmation": True,
        },
        {
            "candidate_id": "NQ|hold|mixed",
            "instrument": "NQ",
            "direction": "UP",
            "opening_drive_signed_return_points": 20.0,
            "overnight_direction": "FLAT",
            "direction_60m_state": "FLAT",
            "cross_index_confirmation": False,
        },
    ]
    outcome_rows = [
        {
            "candidate_id": "ES|dev|up",
            "instrument": "ES",
            "sample_split": "development",
            "direction": "UP",
            "opening_drive_signed_return_points": 5.0,
            "vix_level_bucket": "LOW",
            "vix_change_bucket": "DOWN",
            "forward_return_60m": 1.0,
            "continuation_60m": True,
            "reversal_60m": False,
            "forward_return_120m": 2.0,
            "continuation_120m": True,
            "reversal_120m": False,
            "forward_return_1530": 3.0,
            "continuation_1530": True,
            "reversal_1530": False,
            "forward_return_close": 4.0,
            "continuation_close": True,
            "reversal_close": False,
        },
        {
            "candidate_id": "ES|dev|down",
            "instrument": "ES",
            "sample_split": "development",
            "direction": "DOWN",
            "opening_drive_signed_return_points": 15.0,
            "vix_level_bucket": "MID",
            "vix_change_bucket": "UP",
            "forward_return_60m": 1.0,
            "continuation_60m": True,
            "reversal_60m": False,
            "forward_return_120m": 1.5,
            "continuation_120m": True,
            "reversal_120m": False,
            "forward_return_1530": 2.0,
            "continuation_1530": True,
            "reversal_1530": False,
            "forward_return_close": 2.5,
            "continuation_close": True,
            "reversal_close": False,
        },
        {
            "candidate_id": "NQ|dev|up",
            "instrument": "NQ",
            "sample_split": "development",
            "direction": "UP",
            "opening_drive_signed_return_points": 40.0,
            "vix_level_bucket": "HIGH",
            "vix_change_bucket": "DOWN",
            "forward_return_60m": -2.0,
            "continuation_60m": False,
            "reversal_60m": True,
            "forward_return_120m": -3.0,
            "continuation_120m": False,
            "reversal_120m": True,
            "forward_return_1530": 2.0,
            "continuation_1530": True,
            "reversal_1530": False,
            "forward_return_close": -1.0,
            "continuation_close": False,
            "reversal_close": True,
        },
        {
            "candidate_id": "ES|hold|up",
            "instrument": "ES",
            "sample_split": "holdout",
            "direction": "UP",
            "opening_drive_signed_return_points": 7.0,
            "vix_level_bucket": "LOW",
            "vix_change_bucket": "FLAT",
            "forward_return_60m": 0.5,
            "continuation_60m": True,
            "reversal_60m": False,
            "forward_return_120m": 1.0,
            "continuation_120m": True,
            "reversal_120m": False,
            "forward_return_1530": 1.0,
            "continuation_1530": True,
            "reversal_1530": False,
            "forward_return_close": 2.0,
            "continuation_close": True,
            "reversal_close": False,
        },
        {
            "candidate_id": "NQ|hold|down",
            "instrument": "NQ",
            "sample_split": "holdout",
            "direction": "DOWN",
            "opening_drive_signed_return_points": 60.0,
            "vix_level_bucket": "HIGH",
            "vix_change_bucket": "UP",
            "forward_return_60m": 1.0,
            "continuation_60m": True,
            "reversal_60m": False,
            "forward_return_120m": 1.5,
            "continuation_120m": True,
            "reversal_120m": False,
            "forward_return_1530": 2.0,
            "continuation_1530": True,
            "reversal_1530": False,
            "forward_return_close": 3.0,
            "continuation_close": True,
            "reversal_close": False,
        },
        {
            "candidate_id": "NQ|hold|mixed",
            "instrument": "NQ",
            "sample_split": "holdout",
            "direction": "UP",
            "opening_drive_signed_return_points": 20.0,
            "vix_level_bucket": "MID",
            "vix_change_bucket": "DOWN",
            "forward_return_60m": -0.5,
            "continuation_60m": False,
            "reversal_60m": True,
            "forward_return_120m": 0.25,
            "continuation_120m": True,
            "reversal_120m": False,
            "forward_return_1530": 1.0,
            "continuation_1530": True,
            "reversal_1530": False,
            "forward_return_close": -0.75,
            "continuation_close": False,
            "reversal_close": True,
        },
    ]

    _write_csv(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv", candidate_rows)
    _write_csv(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv", outcome_rows)

    result = run_probabilistic_pass2(pass1_root=pass1_root, output_dir=output_dir)

    assert result["summary"]["row_counts"]["merged_rows"] == 6
    for artifact in (
        "pooled_summary_csv",
        "opening_drive_size_csv",
        "vix_conditioned_csv",
        "overnight_alignment_csv",
        "cross_index_csv",
        "trend_agreement_csv",
        "extreme_exclusion_csv",
        "dev_holdout_stability_csv",
        "day_type_summary_csv",
        "summary_markdown",
    ):
        assert Path(result["artifacts"][artifact]).exists()

    with Path(result["artifacts"]["day_type_summary_csv"]).open(newline="", encoding="utf-8") as handle:
        day_type_rows = list(csv.DictReader(handle))

    up_row = next(
        row for row in day_type_rows
        if row["cluster"] == "ALL"
        and row["feature_name"] == "opening_drive_direction"
        and row["feature_value"] == "UP"
    )
    down_row = next(
        row for row in day_type_rows
        if row["cluster"] == "ALL"
        and row["feature_name"] == "opening_drive_direction"
        and row["feature_value"] == "DOWN"
    )
    assert up_row["holdout_trend_up_probability"] == "0.5"
    assert up_row["holdout_non_trend_probability"] == "0.5"
    assert down_row["holdout_trend_down_probability"] == "1.0"


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
