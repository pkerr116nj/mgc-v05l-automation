from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_failed_move_reversal_us_midday_short_separator import _separator_quality


def test_failed_move_reversal_short_separator_quality_marks_candidate_for_repeatable_improvement() -> None:
    assert (
        _separator_quality(
            match_count=18,
            aligned_rate=Decimal("0.61"),
            aligned_rate_lift=Decimal("1.08"),
            mfe_mae_lift=Decimal("1.18"),
            directional_avg_move_10bar=Decimal("1.7"),
        )
        == "candidate"
    )


def test_failed_move_reversal_short_separator_quality_rejects_thin_or_weak_subsets() -> None:
    assert (
        _separator_quality(
            match_count=9,
            aligned_rate=Decimal("0.74"),
            aligned_rate_lift=Decimal("1.20"),
            mfe_mae_lift=Decimal("1.35"),
            directional_avg_move_10bar=Decimal("4.6"),
        )
        == "broad_or_noisy"
    )
