from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_us_late_pause_long_separator import (
    _candidate_row,
    _separator_quality,
)


def test_separator_quality_marks_repeatable_candidate_for_long_lane() -> None:
    assert (
        _separator_quality(
            match_count=8,
            favorable_rate=Decimal("0.625"),
            favorable_rate_lift=Decimal("1.10"),
            mfe_mae_lift=Decimal("1.25"),
            avg_move_10bar=Decimal("3.0"),
        )
        == "candidate"
    )


def test_candidate_row_tracks_repeatability_and_score_for_long_lane() -> None:
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
                    "move_10bar": Decimal("5"),
                    "mfe_20bar": Decimal("13"),
                    "mae_20bar": Decimal("6"),
                },
            )()
        )
    candidate = _candidate_row(
        rows=rows,
        baseline=baseline,
        candidate_type="single_phase",
        phase_path="setup",
        primitive_field="curvature_state",
        primitive_value="CURVATURE_POS",
    )
    assert candidate["repeatability"] == "repeatable"
    assert candidate["separator_quality"] == "candidate"
    assert Decimal(candidate["usefulness_score"]) > Decimal("0")
