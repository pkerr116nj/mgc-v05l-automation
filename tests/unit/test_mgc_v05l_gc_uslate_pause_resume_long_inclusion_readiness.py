from __future__ import annotations

from mgc_v05l.app.gc_uslate_pause_resume_long_inclusion_readiness import (
    _ready_for_narrow_paper_design_pass,
    _verdict_bucket,
)
from mgc_v05l.app.mnq_usDerivativeBearTurn_validation import PairMetrics


def _metrics(*, trades: int, pnl: float, profit_factor: float | None) -> PairMetrics:
    return PairMetrics(
        sample_start="2025-08-21T16:25:00-04:00",
        sample_end="2026-03-11T15:45:00-04:00",
        trades=trades,
        realized_pnl=pnl,
        avg_trade=20.0 if trades else None,
        median_trade=-1.0 if trades else None,
        profit_factor=profit_factor,
        max_drawdown=100.0,
        win_rate=0.45 if trades else None,
        average_loser=20.0 if trades else None,
        median_loser=15.0 if trades else None,
        p95_loser=50.0 if trades else None,
        worst_loser=60.0 if trades else None,
        average_winner=70.0 if trades else None,
        avg_winner_over_avg_loser=3.5 if trades else None,
        top_1_contribution=100.0 if trades else None,
        top_3_contribution=130.0 if trades else None,
        survives_without_top_1=False,
        survives_without_top_3=False,
        large_winner_count=2,
        very_large_winner_count=1,
    )


def test_ready_for_narrow_paper_design_pass_requires_breadth_positive_economics_and_architecture() -> None:
    assert _ready_for_narrow_paper_design_pass(_metrics(trades=22, pnl=456.0, profit_factor=2.8), True) is True
    assert _ready_for_narrow_paper_design_pass(_metrics(trades=7, pnl=456.0, profit_factor=2.8), True) is False
    assert _ready_for_narrow_paper_design_pass(_metrics(trades=22, pnl=456.0, profit_factor=2.8), False) is False


def test_verdict_bucket_downgrades_when_design_pass_not_yet_supported() -> None:
    assert _verdict_bucket(_metrics(trades=22, pnl=456.0, profit_factor=2.8), True) == "READY_FOR_NARROW_PAPER_DESIGN_PASS"
    assert _verdict_bucket(_metrics(trades=8, pnl=100.0, profit_factor=1.3), True) == "SERIOUS_NEXT_CANDIDATE"
    assert _verdict_bucket(_metrics(trades=8, pnl=10.0, profit_factor=0.9), True) == "LATER_REVIEW"
    assert _verdict_bucket(_metrics(trades=8, pnl=-5.0, profit_factor=0.9), True) == "DEPRIORITIZE"
