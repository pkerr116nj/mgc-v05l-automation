from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.app.gc_mgc_ny_early_short_forced_session_research import (
    _evaluate_forced_session,
    build_variant_specs,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(*, end_ts: datetime, open_: float, high: float, low: float, close: float, timeframe: str = "3m") -> ResearchBar:
    minutes = int(timeframe.removesuffix("m"))
    return ResearchBar(
        instrument="GC",
        timeframe=timeframe,
        start_ts=end_ts - timedelta(minutes=minutes),
        end_ts=end_ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label="TEST",
        session_segment="TEST",
        source="test",
    )


def _segment_bars() -> list[ResearchBar]:
    current = datetime.fromisoformat("2026-04-21T08:23:00-04:00")
    rows = []
    for open_, high, low, close in [
        (3300.0, 3301.5, 3299.8, 3301.2),
        (3301.2, 3302.0, 3301.0, 3301.8),
        (3301.8, 3302.5, 3301.6, 3302.1),
        (3302.1, 3302.3, 3301.7, 3301.9),
        (3301.9, 3302.0, 3299.9, 3300.2),
        (3300.2, 3300.5, 3298.7, 3299.0),
        (3299.0, 3299.3, 3298.1, 3298.5),
    ]:
        rows.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    return rows


def test_forced_session_v1_prefers_breakdown_before_fallback() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "ny_forced_short_v1_breakdown_or_bar6")
    trade = _evaluate_forced_session(symbol="GC", trade_day=date(2026, 4, 21), segment_bars=_segment_bars(), spec=spec)
    assert trade.entered is True
    assert trade.entry_reason == "preferred_breakdown"
    assert trade.pnl_points is not None


def test_forced_session_v3_always_enters_timed_bar() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "ny_forced_short_v3_bar5_always")
    trade = _evaluate_forced_session(symbol="GC", trade_day=date(2026, 4, 21), segment_bars=_segment_bars(), spec=spec)
    assert trade.entered is True
    assert trade.entry_reason == "timed_bar5"
    assert trade.entry_bar_number == 5
