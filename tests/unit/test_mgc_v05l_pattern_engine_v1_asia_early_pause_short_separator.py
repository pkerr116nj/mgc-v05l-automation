from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_asia_early_pause_short_separator import (
    _candidate_row,
    _separator_quality,
)


def test_separator_quality_marks_repeatable_candidate() -> None:
    assert (
        _separator_quality(
            match_count=8,
            favorable_rate=Decimal("0.625"),
            favorable_rate_lift=Decimal("1.15"),
            mfe_mae_lift=Decimal("1.30"),
            avg_move_10bar=Decimal("5.0"),
        )
        == "candidate"
    )


def test_candidate_row_tracks_repeatability_and_score() -> None:
    baseline = {
        "match_count": Decimal("20"),
        "avg_mfe_20bar": Decimal("10"),
        "favorable_move10_rate": Decimal("0.50"),
        "mfe_mae_ratio": Decimal("1.00"),
    }
    rows = []
    for _ in range(6):
        rows.append(
            type(
                "Row",
                (),
                {
                    "move_10bar": Decimal("4"),
                    "mfe_20bar": Decimal("12"),
                    "mae_20bar": Decimal("6"),
                },
            )()
        )
    candidate = _candidate_row(
        rows=rows,
        baseline=baseline,
        candidate_type="single_phase",
        phase_path="rebound",
        primitive_field="ema_location_state",
        primitive_value="REBOUND_BELOW_SLOW",
    )
    assert candidate["repeatability"] == "repeatable"
    assert candidate["separator_quality"] == "candidate"
    assert Decimal(candidate["usefulness_score"]) > Decimal("0")
