from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_failed_move_reversal_us_late_long_separator import _separator_quality


def test_failed_move_reversal_us_late_long_separator_quality_marks_repeatable_improvement() -> None:
    assert (
        _separator_quality(
            match_count=18,
            favorable_rate=Decimal("0.61"),
            favorable_rate_lift=Decimal("1.07"),
            mfe_mae_lift=Decimal("1.11"),
            avg_move_10bar=Decimal("1.6"),
        )
        == "candidate"
    )


def test_failed_move_reversal_us_late_long_separator_quality_rejects_thin_or_weak_subset() -> None:
    assert (
        _separator_quality(
            match_count=9,
            favorable_rate=Decimal("0.74"),
            favorable_rate_lift=Decimal("1.18"),
            mfe_mae_lift=Decimal("1.30"),
            avg_move_10bar=Decimal("3.4"),
        )
        == "broad_or_noisy"
    )
