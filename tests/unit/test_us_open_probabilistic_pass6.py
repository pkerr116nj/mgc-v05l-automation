from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.research.us_open_follow_through.probabilistic_pass6 import (
    RISK_CONTROL_INEFFECTIVE,
    RISK_CONTROL_MIXED,
    RISK_CONTROL_OVERFITTING_RISK,
    RISK_CONTROL_PROMISING,
    run_probabilistic_pass6,
)


def test_run_probabilistic_pass6_writes_feasibility_outputs(tmp_path: Path, monkeypatch) -> None:
    pass1_root = tmp_path / "pass1"
    (pass1_root / "features").mkdir(parents=True)
    (pass1_root / "signals").mkdir(parents=True)
    warehouse_root = tmp_path / "warehouse"
    output_dir = tmp_path / "pass6"

    candidate_rows = [
        {
            "candidate_id": "NQ|dev",
            "instrument": "NQ",
            "decision_ts": "2024-06-03T10:00:00-04:00",
            "decision_close": 100.0,
            "sample_split": "development",
            "direction": "UP",
            "opening_drive_signed_return_points": 10.0,
            "cross_index_confirmation": True,
            "overnight_direction": "DOWN",
            "direction_60m_state": "UP",
        },
        {
            "candidate_id": "MNQ|hold",
            "instrument": "MNQ",
            "decision_ts": "2025-06-02T10:00:00-04:00",
            "decision_close": 200.0,
            "sample_split": "holdout",
            "direction": "UP",
            "opening_drive_signed_return_points": 20.0,
            "cross_index_confirmation": False,
            "overnight_direction": "DOWN",
            "direction_60m_state": "UP",
        },
    ]
    outcome_rows = [
        {
            "candidate_id": "NQ|dev",
            "instrument": "NQ",
            "sample_split": "development",
            "direction": "UP",
            "decision_ts": "2024-06-03T10:00:00-04:00",
            "opening_drive_signed_return_points": 10.0,
            "cross_index_confirmation": True,
            "trend_60m_agreement": True,
            "forward_return_1530": 4.0,
            "forward_return_close": 5.0,
            "mfe_close_points": 8.0,
            "mae_close_points": 3.0,
        },
        {
            "candidate_id": "MNQ|hold",
            "instrument": "MNQ",
            "sample_split": "holdout",
            "direction": "UP",
            "decision_ts": "2025-06-02T10:00:00-04:00",
            "opening_drive_signed_return_points": 20.0,
            "cross_index_confirmation": False,
            "trend_60m_agreement": True,
            "forward_return_1530": 10.0,
            "forward_return_close": 12.0,
            "mfe_close_points": 18.0,
            "mae_close_points": 6.0,
        },
    ]

    _write_csv(pass1_root / "features" / "us_open_probabilistic_pass1_candidates.csv", candidate_rows)
    _write_csv(pass1_root / "signals" / "us_open_probabilistic_pass1_outcomes.csv", outcome_rows)

    def fake_load_raw_series_for_signal_rows(*, warehouse_root: Path, rows):
        return {
            "NQ": _mock_series(datetime.fromisoformat("2024-06-03T09:20:00-04:00"), 95.0, 100.0, dip_size=80.0),
            "MNQ": _mock_series(datetime.fromisoformat("2025-06-02T09:20:00-04:00"), 195.0, 200.0, dip_size=90.0),
        }

    monkeypatch.setattr(
        "mgc_v05l.research.us_open_follow_through.probabilistic_pass6._load_raw_series_for_signal_rows",
        fake_load_raw_series_for_signal_rows,
    )

    result = run_probabilistic_pass6(pass1_root=pass1_root, warehouse_root=warehouse_root, output_dir=output_dir)

    assert result["summary"]["classification"]["classification"] in {
        RISK_CONTROL_PROMISING,
        RISK_CONTROL_MIXED,
        RISK_CONTROL_INEFFECTIVE,
        RISK_CONTROL_OVERFITTING_RISK,
    }
    for artifact in (
        "simulation_csv",
        "stop_horizon_summary_table",
        "tail_risk_reduction_table",
        "stop_damage_table",
        "dev_holdout_stability_table",
        "summary_markdown",
    ):
        assert Path(result["artifacts"][artifact]).exists()

    with Path(result["artifacts"]["stop_horizon_summary_table"]).open(newline="", encoding="utf-8") as handle:
        summary_rows = list(csv.DictReader(handle))
    fixed_60 = next(
        row
        for row in summary_rows
        if row["sample_split"] == "holdout" and row["instrument"] == "ALL" and row["stop_family"] == "FIXED" and row["stop_label"] == "50pt" and row["horizon"] == "60m"
    )
    assert float(fixed_60["stopped_out_rate"]) >= 0.0


def _mock_series(start_ts: datetime, pre_open_price: float, entry: float, *, dip_size: float) -> dict[str, list]:
    timestamps = []
    highs = []
    lows = []
    closes = []
    current = start_ts.astimezone(UTC)
    value = pre_open_price
    for idx in range(500):
        current = current + timedelta(minutes=1)
        if idx < 40:
            value += 0.2
        else:
            value += 0.8
        if idx == 55:
            value -= dip_size
        timestamps.append(current)
        highs.append(value + 1.0)
        lows.append(value - (60.0 if idx == 55 else 1.5))
        closes.append(value)
    return {
        "timestamps": timestamps,
        "highs": highs,
        "lows": lows,
        "closes": closes,
    }


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
