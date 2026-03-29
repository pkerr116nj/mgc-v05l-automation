from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_failed_move_reversal_us_midday_short_refinement_separator import (
    _separator_quality,
)


def test_failed_move_reversal_short_refinement_quality_marks_candidate_for_balanced_improvement() -> None:
    assert (
        _separator_quality(
            match_count=22,
            aligned_rate=Decimal("0.61"),
            aligned_rate_lift=Decimal("1.04"),
            mfe_mae_lift=Decimal("1.08"),
            directional_avg_move_10bar=Decimal("1.4"),
            mfe_mae_ratio=Decimal("1.06"),
        )
        == "candidate"
    )


def test_failed_move_reversal_short_refinement_quality_rejects_direction_only_improvement() -> None:
    assert (
        _separator_quality(
            match_count=42,
            aligned_rate=Decimal("0.63"),
            aligned_rate_lift=Decimal("1.11"),
            mfe_mae_lift=Decimal("0.88"),
            directional_avg_move_10bar=Decimal("2.8"),
            mfe_mae_ratio=Decimal("0.95"),
        )
        == "broad_or_noisy"
    )
