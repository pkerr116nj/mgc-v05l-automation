from __future__ import annotations

from mgc_v05l.app.mgc_usDerivativeBearTurn_validation import _direct_answers, _verdict_bucket
from mgc_v05l.app.mnq_usDerivativeBearTurn_validation import PairMetrics


def _metrics(*, trades: int, profit_factor: float, median_trade: float, survives_without_top_3: bool) -> PairMetrics:
    return PairMetrics(
        sample_start="a",
        sample_end="b",
        trades=trades,
        realized_pnl=500.0,
        avg_trade=50.0,
        median_trade=median_trade,
        profit_factor=profit_factor,
        max_drawdown=100.0,
        win_rate=0.6,
        average_loser=20.0,
        median_loser=18.0,
        p95_loser=40.0,
        worst_loser=45.0,
        average_winner=80.0,
        avg_winner_over_avg_loser=4.0,
        top_1_contribution=30.0,
        top_3_contribution=60.0 if survives_without_top_3 else 120.0,
        survives_without_top_1=True,
        survives_without_top_3=survives_without_top_3,
        large_winner_count=3,
        very_large_winner_count=1,
    )


def test_verdict_bucket_marks_clean_home_lane_as_prioritize_now() -> None:
    metrics = _metrics(trades=7, profit_factor=3.2, median_trade=25.0, survives_without_top_3=True)
    assert _verdict_bucket(metrics) == "PRIORITIZE_NOW"


def test_direct_answers_keep_us_late_as_more_paper_suitable_when_branch_is_thin() -> None:
    target = _metrics(trades=7, profit_factor=3.2, median_trade=25.0, survives_without_top_3=True)
    us_late = _metrics(trades=20, profit_factor=5.0, median_trade=1.0, survives_without_top_3=False)
    prior_cached = _metrics(trades=7, profit_factor=13.0, median_trade=77.0, survives_without_top_3=True)

    answers = _direct_answers(target_metrics=target, us_late_metrics=us_late, prior_cached_reference=prior_cached)

    assert "Yes." in answers["should_become_next_immediate_active_thread_1_focus"]
    assert answers["stronger_or_weaker_than_usLate_as_paper_candidate_path"].startswith("Weaker.")


def test_direct_answers_call_out_sample_breadth_as_main_blocker() -> None:
    target = _metrics(trades=7, profit_factor=3.2, median_trade=25.0, survives_without_top_3=True)
    us_late = _metrics(trades=20, profit_factor=5.0, median_trade=1.0, survives_without_top_3=False)
    prior_cached = _metrics(trades=9, profit_factor=13.0, median_trade=77.0, survives_without_top_3=True)

    answers = _direct_answers(target_metrics=target, us_late_metrics=us_late, prior_cached_reference=prior_cached)

    assert "Sample breadth" in answers["single_biggest_remaining_blocker"]
