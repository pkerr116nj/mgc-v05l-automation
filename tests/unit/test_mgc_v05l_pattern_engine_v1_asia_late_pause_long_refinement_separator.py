from decimal import Decimal

from mgc_v05l.app.pattern_engine_v1_asia_late_pause_long_refinement_separator import (
    _candidate_row,
    _separator_quality,
)


def test_separator_quality_marks_candidate_when_recent_slice_holds_up() -> None:
    assert (
        _separator_quality(
            match_count=12,
            favorable_rate=Decimal("0.66"),
            favorable_rate_lift=Decimal("1.10"),
            mfe_mae_lift=Decimal("1.20"),
            avg_move_10bar=Decimal("2.4"),
            recent_avg_move_10bar=Decimal("0.4"),
        )
        == "candidate"
    )


def test_candidate_row_flags_recent_drag() -> None:
    baseline = {
        "match_count": Decimal("27"),
        "avg_move_10bar": Decimal("2.69"),
        "avg_mfe_20bar": Decimal("11.0"),
        "avg_mae_20bar": Decimal("5.9"),
        "favorable_move10_rate": Decimal("0.66"),
        "mfe_mae_ratio": Decimal("1.87"),
        "recent_avg_move_10bar": Decimal("0.1"),
        "recent_favorable_move10_rate": Decimal("0.50"),
    }
    rows = []
    for idx in range(6):
        rows.append(
            type(
                "Row",
                (),
                {
                    "anchor_dt": __import__("datetime").datetime.fromisoformat(
                        "2026-02-01T21:00:00-05:00" if idx < 3 else "2025-11-01T21:00:00-04:00"
                    ),
                    "move_10bar": Decimal("-1") if idx < 3 else Decimal("3"),
                    "mfe_20bar": Decimal("10"),
                    "mae_20bar": Decimal("6"),
                },
            )()
        )
    candidate = _candidate_row(
        rows=rows,
        baseline=baseline,
        candidate_type="single_phase",
        phase_path="setup",
        primitive_field="ema_location_state",
        primitive_value="ABOVE_BOTH_FAST_GT_SLOW",
    )
    assert candidate["repeatability"] == "repeatable"
    assert candidate["recent_drag_flag"] is True
