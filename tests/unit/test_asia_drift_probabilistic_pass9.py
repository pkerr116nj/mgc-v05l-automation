from __future__ import annotations

from datetime import datetime, time

from mgc_v05l.research.asia_drift.probabilistic_pass9 import _checkpoint_on_trade_date, _window_metrics


def test_checkpoint_on_trade_date_uses_next_trade_day_for_evening_session() -> None:
    decision = datetime.fromisoformat("2026-04-05T19:00:00-04:00")
    assert _checkpoint_on_trade_date(decision, time(3, 0)).isoformat() == "2026-04-06T03:00:00-04:00"
    assert _checkpoint_on_trade_date(decision, time(9, 30)).isoformat() == "2026-04-06T09:30:00-04:00"


def test_window_metrics_reports_extended_path_statistics() -> None:
    decision = datetime.fromisoformat("2026-04-01T18:00:00-04:00")
    timestamps = [
        datetime.fromisoformat("2026-04-01T18:01:00-04:00"),
        datetime.fromisoformat("2026-04-01T18:02:00-04:00"),
        datetime.fromisoformat("2026-04-01T18:03:00-04:00"),
    ]
    metrics = _window_metrics(
        decision_ts=decision,
        close_value=104.0,
        direction="LONG",
        entry_price=100.0,
        timestamps=timestamps,
        favorable_values=[1.0, 5.0, 4.0],
        adverse_values=[0.5, 2.0, 1.0],
    )
    assert metrics["forward_return"] == 4.0
    assert metrics["mfe_points"] == 5.0
    assert metrics["mae_points"] == 2.0
    assert metrics["time_to_peak_favorable_minute"] == 2
    assert metrics["time_to_peak_adverse_minute"] == 2
    assert metrics["time_to_first_profit_minute"] == 1
    assert metrics["time_to_first_adverse_minute"] == 1
