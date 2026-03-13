"""Bar-store tests for completed-bar determinism rules."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from mgc_v05l.domain.exceptions import DeterminismError
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.bar_models import build_bar_id
from mgc_v05l.market_data.bar_store import BarStore


def _build_bar(end_ts: datetime) -> Bar:
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=build_bar_id("MGC", "5m", end_ts),
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal("3000.0"),
        high=Decimal("3001.0"),
        low=Decimal("2999.5"),
        close=Decimal("3000.5"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_build_bar_id_uses_recommended_format() -> None:
    end_ts = datetime(2026, 3, 13, 15, 0, tzinfo=timezone.utc)
    assert build_bar_id("MGC", "5m", end_ts) == "MGC|5m|2026-03-13T15:00:00Z"


def test_bar_store_ignores_duplicate_bar_ids() -> None:
    store = BarStore()
    bar = _build_bar(datetime(2026, 3, 13, 15, 0, tzinfo=timezone.utc))

    assert store.validate_next_bar(bar) is True
    store.mark_processed(bar)
    assert store.validate_next_bar(bar) is False


def test_bar_store_rejects_out_of_order_bars() -> None:
    store = BarStore()
    later_bar = _build_bar(datetime(2026, 3, 13, 15, 5, tzinfo=timezone.utc))
    earlier_bar = _build_bar(datetime(2026, 3, 13, 15, 0, tzinfo=timezone.utc))

    store.mark_processed(later_bar)

    with pytest.raises(DeterminismError):
        store.validate_next_bar(earlier_bar)
