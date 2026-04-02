from datetime import datetime
from zoneinfo import ZoneInfo

from mgc_v05l.app.us_derivative_bear_regime_analysis import assign_slice_name, best_next_gating_hypothesis


def test_assign_slice_name_uses_chronological_thirds() -> None:
    ny = ZoneInfo("America/New_York")
    boundaries = {
        "middle_start": datetime(2025, 10, 27, 0, 0, tzinfo=ny),
        "recent_start": datetime(2026, 1, 6, 3, 10, tzinfo=ny),
    }

    assert assign_slice_name(datetime(2025, 10, 1, 11, 15, tzinfo=ny), boundaries) == "early"
    assert assign_slice_name(datetime(2025, 12, 10, 14, 25, tzinfo=ny), boundaries) == "middle"
    assert assign_slice_name(datetime(2026, 2, 5, 10, 0, tzinfo=ny), boundaries) == "recent"


def test_best_next_gating_hypothesis_returns_concrete_gate() -> None:
    hypothesis = best_next_gating_hypothesis(
        {
            "best_next_gating_hypothesis": "Allow widened thresholds only in the US open window with a VWAP-extension cap."
        }
    )

    assert "US open window" in hypothesis
