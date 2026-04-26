from __future__ import annotations

from datetime import datetime

from mgc_v05l.research.asia_drift.probabilistic_pass4 import (
    _build_holding_window_rows,
    _simulate_stop_target,
    _time_to_first_positive,
)


def test_holding_window_chooses_earliest_positive_horizon() -> None:
    rows = [
        {
            "sample_split": "holdout",
            "focus": "INDEX_POOLED",
            "timing_within_asia": "EARLY_ASIA",
            "horizon": "15m",
            "row_count": 200,
            "avg_return": -0.2,
            "positive_return_probability": 0.49,
            "do_not_trust": False,
        },
        {
            "sample_split": "holdout",
            "focus": "INDEX_POOLED",
            "timing_within_asia": "EARLY_ASIA",
            "horizon": "30m",
            "row_count": 200,
            "avg_return": 0.1,
            "positive_return_probability": 0.52,
            "do_not_trust": False,
        },
        {
            "sample_split": "holdout",
            "focus": "INDEX_POOLED",
            "timing_within_asia": "EARLY_ASIA",
            "horizon": "60m",
            "row_count": 200,
            "avg_return": 0.3,
            "positive_return_probability": 0.55,
            "do_not_trust": False,
        },
    ]

    holding = _build_holding_window_rows(rows)

    assert holding[0]["minimum_viable_holding_window"] == "30m"


def test_simulate_stop_target_uses_conservative_stop_first_tie_break() -> None:
    decision_ts = datetime.fromisoformat("2026-04-01T20:00:00-04:00")
    row = {
        "direction": "LONG",
        "decision_ts": decision_ts,
        "decision_close": 100.0,
    }
    raw_series = {
        "timestamps": [decision_ts.replace(minute=1)],
        "highs": [105.0],
        "lows": [95.0],
        "closes": [101.0],
    }

    result = _simulate_stop_target(
        row=row,
        raw_series=raw_series,
        stop_points=4.0,
        target_points=4.0,
    )

    assert result["exit_type"] == "STOP"
    assert result["scenario_return"] == -4.0


def test_time_to_first_positive_finds_first_adverse_minute() -> None:
    decision_ts = datetime.fromisoformat("2026-04-01T20:00:00-04:00")
    timestamps = [
        datetime.fromisoformat("2026-04-01T20:01:00-04:00"),
        datetime.fromisoformat("2026-04-01T20:02:00-04:00"),
    ]

    assert _time_to_first_positive(timestamps, decision_ts, [0.0, 1.5]) == 2
