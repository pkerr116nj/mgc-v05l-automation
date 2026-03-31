from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_breakout_asia_early_short_separator import _separator_quality


def test_breakout_short_separator_quality_marks_candidate_for_repeatable_improvement() -> None:
    assert (
        _separator_quality(
            match_count=18,
            favorable_rate=Decimal("0.63"),
            favorable_rate_lift=Decimal("1.06"),
            mfe_mae_lift=Decimal("1.16"),
            avg_move_10bar=Decimal("1.9"),
        )
        == "candidate"
    )


def test_breakout_short_separator_quality_rejects_thin_or_weak_subsets() -> None:
    assert (
        _separator_quality(
            match_count=9,
            favorable_rate=Decimal("0.82"),
            favorable_rate_lift=Decimal("1.20"),
            mfe_mae_lift=Decimal("1.32"),
            avg_move_10bar=Decimal("4.2"),
        )
        == "broad_or_noisy"
    )
