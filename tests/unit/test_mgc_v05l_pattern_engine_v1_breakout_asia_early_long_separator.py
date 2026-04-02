from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_breakout_asia_early_long_separator import _separator_quality


def test_breakout_separator_quality_marks_candidate_for_repeatable_improvement() -> None:
    assert (
        _separator_quality(
            match_count=18,
            favorable_rate=Decimal("0.67"),
            favorable_rate_lift=Decimal("1.08"),
            mfe_mae_lift=Decimal("1.18"),
            avg_move_10bar=Decimal("2.4"),
        )
        == "candidate"
    )


def test_breakout_separator_quality_rejects_thin_or_weak_subsets() -> None:
    assert (
        _separator_quality(
            match_count=9,
            favorable_rate=Decimal("0.80"),
            favorable_rate_lift=Decimal("1.20"),
            mfe_mae_lift=Decimal("1.40"),
            avg_move_10bar=Decimal("5.0"),
        )
        == "broad_or_noisy"
    )
