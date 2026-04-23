from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.app.gc_mgc_segment_forced_session_short_research import (
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
    current = datetime.fromisoformat("2026-04-21T19:03:00-04:00")
    rows = []
    for open_, high, low, close in [
        (3300.0, 3301.2, 3299.8, 3300.2),
        (3300.2, 3301.6, 3300.0, 3301.3),
        (3301.3, 3302.0, 3301.0, 3301.8),
        (3301.8, 3302.1, 3301.4, 3301.9),
        (3301.9, 3302.2, 3300.7, 3300.8),
        (3300.8, 3301.0, 3299.5, 3299.9),
        (3299.9, 3300.1, 3298.8, 3299.1),
    ]:
        rows.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    return rows


def test_forced_session_v1_prefers_breakdown_before_fallback() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_short_v1_breakdown_or_bar6")
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="ASIA_EARLY",
        segment_bars=_segment_bars(),
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "preferred_breakdown"
    assert trade.pnl_points is not None


def test_forced_session_v3_always_enters_timed_bar() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_short_v3_bar5_always")
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="ASIA_EARLY",
        segment_bars=_segment_bars(),
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "timed_bar5"
    assert trade.entry_bar_number == 5


def test_forced_session_v4_falls_back_to_bar7_when_breakdown_missing() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_short_v4_breakdown_or_bar7")
    current = datetime.fromisoformat("2026-04-21T19:03:00-04:00")
    rows = []
    for open_, high, low, close in [
        (3300.0, 3301.2, 3299.8, 3300.2),
        (3300.2, 3301.6, 3300.0, 3301.3),
        (3301.3, 3302.0, 3301.0, 3301.8),
        (3301.8, 3302.1, 3301.4, 3301.9),
        (3301.9, 3302.0, 3301.2, 3301.3),
        (3301.3, 3301.5, 3300.9, 3301.1),
        (3301.1, 3301.4, 3300.7, 3300.9),
        (3300.9, 3301.2, 3300.5, 3300.8),
    ]:
        rows.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="US_MIDDAY",
        segment_bars=rows,
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "fallback_bar7"
    assert trade.entry_bar_number == 7


def test_forced_session_v5_can_use_contextual_bar7_for_us_midday() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_short_v5_contextual_breakdown")
    current = datetime.fromisoformat("2026-04-21T11:03:00-04:00")
    rows = []
    for open_, high, low, close in [
        (3300.0, 3301.8, 3299.8, 3300.8),
        (3300.8, 3302.1, 3300.6, 3301.2),
        (3301.2, 3302.4, 3301.0, 3301.4),
        (3301.4, 3302.6, 3301.2, 3301.5),
        (3301.5, 3301.8, 3301.3, 3301.4),
        (3301.4, 3301.6, 3301.0, 3301.2),
        (3301.2, 3301.4, 3300.8, 3301.0),
        (3301.0, 3301.2, 3300.7, 3300.9),
    ]:
        rows.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="US_MIDDAY",
        segment_bars=rows,
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "context_bar7_positive_setup"
    assert trade.entry_bar_number == 7
