from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_asia_late_pause_long_separator import (
    _candidate_row,
    _separator_quality,
)


def test_separator_quality_marks_repeatable_candidate() -> None:
    assert (
        _separator_quality(
            match_count=27,
            favorable_rate=Decimal("0.6667"),
            favorable_rate_lift=Decimal("1.08"),
            mfe_mae_lift=Decimal("1.21"),
            avg_move_10bar=Decimal("2.6"),
        )
        == "candidate"
    )


def test_candidate_row_tracks_repeatability_and_score() -> None:
    baseline = {
        "match_count": Decimal("60"),
        "avg_mfe_20bar": Decimal("10.4"),
        "favorable_move10_rate": Decimal("0.55"),
        "mfe_mae_ratio": Decimal("1.31"),
    }
    rows = []
    for _ in range(27):
        rows.append(
            type(
                "Row",
                (),
                {
                    "move_10bar": Decimal("3"),
                    "mfe_20bar": Decimal("11"),
                    "mae_20bar": Decimal("5.8"),
                },
            )()
        )
    candidate = _candidate_row(
        rows=rows,
        baseline=baseline,
        candidate_type="phase_transition",
        phase_path="pullback->resumption",
        primitive_field="expansion_state",
        primitive_value="COMPRESSED->NORMAL",
    )
    assert candidate["repeatability"] == "repeatable"
    assert candidate["separator_quality"] == "candidate"
    assert Decimal(candidate["usefulness_score"]) > Decimal("0")
