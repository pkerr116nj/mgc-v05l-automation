from __future__ import annotations

from mgc_v05l.app.mnq_usDerivativeBearTurn_validation import (
    PairMetrics,
    _compute_metrics,
    _verdict_bucket,
)


def test_compute_metrics_counts_large_winners_from_median_loser_proxy() -> None:
    summary = {"source_first_bar_ts": "2026-01-01T00:00:00+00:00", "source_last_bar_ts": "2026-01-02T00:00:00+00:00"}
    rows = [
        {"net_pnl": "100"},
        {"net_pnl": "90"},
        {"net_pnl": "-20"},
        {"net_pnl": "-30"},
        {"net_pnl": "170"},
    ]

    metrics = _compute_metrics(summary=summary, rows=rows, sample_start=summary["source_first_bar_ts"], sample_end=summary["source_last_bar_ts"])

    assert metrics.trades == 5
    assert metrics.realized_pnl == 310.0
    assert metrics.median_loser == 25.0
    assert metrics.large_winner_count == 3
    assert metrics.very_large_winner_count == 1


def test_verdict_bucket_marks_strong_clean_case_as_prioritize_now() -> None:
    metrics = PairMetrics(
        sample_start="a",
        sample_end="b",
        trades=9,
        realized_pnl=800.0,
        avg_trade=88.0,
        median_trade=35.0,
        profit_factor=2.2,
        max_drawdown=120.0,
        win_rate=0.55,
        average_loser=30.0,
        median_loser=24.0,
        p95_loser=60.0,
        worst_loser=70.0,
        average_winner=95.0,
        avg_winner_over_avg_loser=3.1,
        top_1_contribution=18.0,
        top_3_contribution=42.0,
        survives_without_top_1=True,
        survives_without_top_3=True,
        large_winner_count=3,
        very_large_winner_count=1,
    )

    assert _verdict_bucket(metrics) == "PRIORITIZE_NOW"


def test_verdict_bucket_marks_thin_positive_case_as_serious_next_candidate() -> None:
    metrics = PairMetrics(
        sample_start="a",
        sample_end="b",
        trades=6,
        realized_pnl=180.0,
        avg_trade=30.0,
        median_trade=-2.0,
        profit_factor=1.35,
        max_drawdown=90.0,
        win_rate=0.5,
        average_loser=22.0,
        median_loser=20.0,
        p95_loser=34.0,
        worst_loser=40.0,
        average_winner=45.0,
        avg_winner_over_avg_loser=2.0,
        top_1_contribution=40.0,
        top_3_contribution=85.0,
        survives_without_top_1=True,
        survives_without_top_3=False,
        large_winner_count=1,
        very_large_winner_count=0,
    )

    assert _verdict_bucket(metrics) == "SERIOUS_NEXT_CANDIDATE"
