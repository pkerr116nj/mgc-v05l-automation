from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_failed_move_reversal_london_open_short_separator import _separator_quality


def test_failed_move_reversal_london_open_short_separator_quality_marks_repeatable_improvement() -> None:
    assert (
        _separator_quality(
            match_count=26,
            aligned_rate=Decimal("0.61"),
            aligned_rate_lift=Decimal("1.08"),
            mfe_mae_lift=Decimal("1.24"),
            directional_avg_move_10bar=Decimal("1.85"),
        )
        == "candidate"
    )


def test_failed_move_reversal_london_open_short_separator_quality_rejects_thin_or_weak_subset() -> None:
    assert (
        _separator_quality(
            match_count=9,
            aligned_rate=Decimal("0.63"),
            aligned_rate_lift=Decimal("1.12"),
            mfe_mae_lift=Decimal("1.31"),
            directional_avg_move_10bar=Decimal("2.40"),
        )
        == "broad_or_noisy"
    )
