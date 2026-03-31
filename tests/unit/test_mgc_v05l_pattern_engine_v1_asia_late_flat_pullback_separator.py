from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_asia_late_flat_pullback_separator import (
    _asia_late_time_bucket,
    _separator_quality,
)


def test_separator_quality_marks_candidate_with_nonnegative_recent_behavior() -> None:
    assert (
        _separator_quality(
            match_count=6,
            favorable_rate=Decimal("0.75"),
            favorable_rate_lift=Decimal("1.10"),
            mfe_mae_lift=Decimal("1.20"),
            avg_move_10bar=Decimal("3.0"),
            recent_avg_move_10bar=Decimal("0.1"),
        )
        == "candidate"
    )


def test_asia_late_time_bucket_assigns_expected_ranges() -> None:
    import datetime as dt

    assert _asia_late_time_bucket(dt.datetime.fromisoformat("2025-10-30T20:45:00-04:00")) == "ASIA_LATE_OPEN"
    assert _asia_late_time_bucket(dt.datetime.fromisoformat("2025-10-30T21:45:00-04:00")) == "ASIA_LATE_MID"
    assert _asia_late_time_bucket(dt.datetime.fromisoformat("2025-10-30T22:40:00-04:00")) == "ASIA_LATE_CLOSE"
