from __future__ import annotations

from mgc_v05l.app.best_inclusion_candidate_review import (
    _candidate_priority,
    _compute_trade_metrics,
    _verdict_bucket,
)


def test_compute_trade_metrics_tracks_drawdown_and_concentration() -> None:
    metrics = _compute_trade_metrics(
        [
            {"entry_ts": "2026-03-01T14:00:00-04:00", "exit_ts": "2026-03-01T14:10:00-04:00", "net_pnl": 10.0},
            {"entry_ts": "2026-03-02T14:00:00-04:00", "exit_ts": "2026-03-02T14:10:00-04:00", "net_pnl": -4.0},
            {"entry_ts": "2026-03-03T14:00:00-04:00", "exit_ts": "2026-03-03T14:10:00-04:00", "net_pnl": 3.0},
        ]
    )
    assert metrics["realized_pnl"] == 9.0
    assert metrics["max_drawdown"] == 4.0
    assert metrics["survives_without_top_1"] is False
    assert metrics["survives_without_top_3"] is False
    assert metrics["top_1_contribution"] is not None


def test_candidate_priority_penalizes_thin_and_overconcentrated_results() -> None:
    broader = {
        "realized_pnl": 200.0,
        "trades": 18,
        "profit_factor": 2.0,
        "top_1_contribution": 60.0,
        "top_3_contribution": 95.0,
    }
    thin = {
        "realized_pnl": 250.0,
        "trades": 5,
        "profit_factor": 2.0,
        "top_1_contribution": 300.0,
        "top_3_contribution": 350.0,
    }
    assert _candidate_priority(broader) > _candidate_priority(thin)


def test_verdict_bucket_requires_more_than_thin_positive_sample() -> None:
    include_now = {
        "trades": 28,
        "realized_pnl": 300.0,
        "profit_factor": 2.1,
        "survives_without_top_1": True,
        "survives_without_top_3": True,
    }
    serious_next = {
        "trades": 18,
        "realized_pnl": 150.0,
        "profit_factor": 1.7,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    thin = {
        "trades": 6,
        "realized_pnl": 120.0,
        "profit_factor": 1.8,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    assert _verdict_bucket(include_now) == "INCLUDE_NOW_RESEARCH_PRIORITY"
    assert _verdict_bucket(serious_next) == "SERIOUS_NEXT_CANDIDATE"
    assert _verdict_bucket(thin) == "INTERESTING_BUT_NOT_YET"
