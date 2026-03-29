from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_asia_early_pause_short_refinement_separator import (
    _candidate_row,
    _separator_quality,
)


def test_separator_quality_marks_repeatable_candidate() -> None:
    assert (
        _separator_quality(
            match_count=9,
            favorable_rate=Decimal("0.7777"),
            favorable_rate_lift=Decimal("1.08"),
            mfe_mae_lift=Decimal("1.22"),
            avg_move_10bar=Decimal("6.0"),
        )
        == "candidate"
    )


def test_candidate_row_tracks_repeatability_and_score() -> None:
    baseline = {
        "match_count": Decimal("25"),
        "avg_mfe_20bar": Decimal("32"),
        "favorable_move10_rate": Decimal("0.72"),
        "mfe_mae_ratio": Decimal("3.36"),
    }
    rows = []
    for _ in range(9):
        rows.append(
            type(
                "Row",
                (),
                {
                    "move_10bar": Decimal("5"),
                    "mfe_20bar": Decimal("34"),
                    "mae_20bar": Decimal("8"),
                },
            )()
        )
    candidate = _candidate_row(
        rows=rows,
        baseline=baseline,
        candidate_type="phase_transition",
        phase_path="rebound->resumption",
        primitive_field="expansion_state",
        primitive_value="COMPRESSED->NORMAL",
    )
    assert candidate["repeatability"] == "repeatable"
    assert candidate["separator_quality"] == "candidate"
    assert Decimal(candidate["usefulness_score"]) > Decimal("0")
