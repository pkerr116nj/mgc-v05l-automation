from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_breakout_asia_early_long_refinement_separator import _separator_quality


def test_breakout_refinement_separator_quality_marks_recent_resilient_candidate() -> None:
    assert (
        _separator_quality(
            match_count=18,
            favorable_rate=Decimal("0.62"),
            favorable_rate_lift=Decimal("1.05"),
            mfe_mae_lift=Decimal("1.18"),
            avg_move_10bar=Decimal("3.10"),
            recent_avg_move_10bar=Decimal("1.20"),
        )
        == "candidate"
    )


def test_breakout_refinement_separator_quality_rejects_negative_recent_subset() -> None:
    assert (
        _separator_quality(
            match_count=22,
            favorable_rate=Decimal("0.63"),
            favorable_rate_lift=Decimal("1.06"),
            mfe_mae_lift=Decimal("1.21"),
            avg_move_10bar=Decimal("3.30"),
            recent_avg_move_10bar=Decimal("-0.40"),
        )
        == "broad_or_noisy"
    )
