from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from mgc_v05l.app.index_futures_forced_session_research import (
    label_stock_index_segment,
    stock_index_trade_date_for_timestamp,
)


NEW_YORK = ZoneInfo("America/New_York")


def test_stock_index_session_labels_follow_requested_windows() -> None:
    assert label_stock_index_segment(datetime(2026, 4, 21, 18, 30, tzinfo=NEW_YORK)) == "SESSION_OPEN"
    assert label_stock_index_segment(datetime(2026, 4, 21, 19, 0, tzinfo=NEW_YORK)) == "ASIA_EARLY"
    assert label_stock_index_segment(datetime(2026, 4, 21, 20, 45, tzinfo=NEW_YORK)) == "ASIA_LATE"
    assert label_stock_index_segment(datetime(2026, 4, 22, 1, 15, tzinfo=NEW_YORK)) == "ASIA_LATE"
    assert label_stock_index_segment(datetime(2026, 4, 22, 3, 15, tzinfo=NEW_YORK)) == "LONDON_EARLY"
    assert label_stock_index_segment(datetime(2026, 4, 22, 6, 0, tzinfo=NEW_YORK)) == "LONDON_LATE"
    assert label_stock_index_segment(datetime(2026, 4, 22, 8, 30, tzinfo=NEW_YORK)) == "US_EARLY"
    assert label_stock_index_segment(datetime(2026, 4, 22, 11, 30, tzinfo=NEW_YORK)) == "US_MIDDAY"
    assert label_stock_index_segment(datetime(2026, 4, 22, 13, 35, tzinfo=NEW_YORK)) == "US_LATE"


def test_stock_index_trade_date_rolls_after_session_reset() -> None:
    assert stock_index_trade_date_for_timestamp(datetime(2026, 4, 21, 17, 59, tzinfo=NEW_YORK)).isoformat() == "2026-04-21"
    assert stock_index_trade_date_for_timestamp(datetime(2026, 4, 21, 18, 0, tzinfo=NEW_YORK)).isoformat() == "2026-04-22"
