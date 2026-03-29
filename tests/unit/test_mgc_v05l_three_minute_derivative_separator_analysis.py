from decimal import Decimal

from mgc_v05l.app.three_minute_derivative_separator_analysis import (
    _bucket_three_way,
    _find_anchor_row,
)


def test_bucket_three_way_supports_flat_pos_and_neg_states() -> None:
    assert _bucket_three_way(Decimal("0.30"), flat_threshold=Decimal("0.20")) == "SLOPE_POS"
    assert _bucket_three_way(Decimal("0.05"), flat_threshold=Decimal("0.20")) == "SLOPE_FLAT"
    assert _bucket_three_way(Decimal("-0.25"), flat_threshold=Decimal("0.20")) == "SLOPE_NEG"


def test_find_anchor_row_uses_latest_completed_3m_bar_at_or_before_event() -> None:
    rows = [
        {"end_ts": "2026-03-03T11:36:00-05:00"},
        {"end_ts": "2026-03-03T11:39:00-05:00"},
        {"end_ts": "2026-03-03T11:42:00-05:00"},
    ]

    assert _find_anchor_row(rows, "2026-03-03T11:40:00-05:00") == {"end_ts": "2026-03-03T11:39:00-05:00"}
    assert _find_anchor_row(rows, "2026-03-03T11:42:00-05:00") == {"end_ts": "2026-03-03T11:42:00-05:00"}
