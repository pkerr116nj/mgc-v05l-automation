from __future__ import annotations

from datetime import datetime

from mgc_v05l.research.asia_drift.probabilistic_pass8 import _payoff_ratio, _time_to_first_positive


def test_time_to_first_positive_returns_minutes_from_decision() -> None:
    decision = datetime.fromisoformat("2026-04-01T18:00:00-04:00")
    timestamps = [
        datetime.fromisoformat("2026-04-01T18:01:00-04:00"),
        datetime.fromisoformat("2026-04-01T18:05:00-04:00"),
    ]
    assert _time_to_first_positive(timestamps, decision, [-1.0, 2.0]) == 5


def test_payoff_ratio_uses_average_win_over_average_loss_abs() -> None:
    assert _payoff_ratio([5.0, -2.0, 3.0, -1.0]) == 2.666667
