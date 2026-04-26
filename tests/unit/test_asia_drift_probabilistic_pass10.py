from __future__ import annotations

from datetime import datetime, time

from mgc_v05l.research.asia_drift.probabilistic_pass10 import _checkpoint_on_trade_date, _simulate_stop_target


def test_checkpoint_on_trade_date_uses_trade_day_for_rth_and_session_end() -> None:
    decision = datetime.fromisoformat("2026-04-01T21:30:00-04:00")
    assert _checkpoint_on_trade_date(decision, time(9, 30)).isoformat() == "2026-04-02T09:30:00-04:00"
    assert _checkpoint_on_trade_date(decision, time(16, 0)).isoformat() == "2026-04-02T16:00:00-04:00"


def test_simulate_stop_target_prefers_stop_on_same_bar_dual_touch() -> None:
    row = {
        "direction": "LONG",
        "decision_ts": "2026-04-01T18:00:00-04:00",
        "decision_close": 100.0,
    }
    raw_series = {
        "timestamps": [datetime.fromisoformat("2026-04-01T18:01:00-04:00")],
        "closes": [101.0],
        "highs": [109.0],
        "lows": [95.0],
    }
    result = _simulate_stop_target(
        row=row,
        raw_series=raw_series,
        stop_points=4.0,
        target_points=8.0,
        max_minutes=120,
    )
    assert result["exit_type"] == "STOP"
    assert result["scenario_return"] == -4.0
