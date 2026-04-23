from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.app.gc_mgc_segment_forced_session_long_research import (
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
        (3300.0, 3301.0, 3299.8, 3300.7),
        (3300.7, 3301.4, 3300.5, 3301.1),
        (3301.1, 3301.8, 3300.9, 3301.5),
        (3301.5, 3301.7, 3300.8, 3301.0),
        (3301.0, 3302.2, 3300.9, 3302.0),
        (3302.0, 3303.0, 3301.8, 3302.7),
        (3302.7, 3303.2, 3302.3, 3302.9),
    ]:
        rows.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    return rows


def test_forced_session_v1_prefers_breakout_before_fallback() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_long_v1_breakout_or_bar6")
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="ASIA_EARLY",
        segment_bars=_segment_bars(),
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "preferred_breakout"
    assert trade.pnl_points is not None


def test_forced_session_v3_always_enters_timed_bar() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_long_v3_bar5_always")
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


def test_forced_session_v4_falls_back_to_bar7_when_breakout_missing() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_long_v4_breakout_or_bar7")
    current = datetime.fromisoformat("2026-04-21T03:03:00-04:00")
    bars = []
    for open_, high, low, close in [
        (3300.0, 3301.0, 3299.8, 3300.6),
        (3300.6, 3301.3, 3300.4, 3301.0),
        (3301.0, 3301.8, 3300.9, 3301.4),
        (3301.4, 3301.7, 3301.0, 3301.2),
        (3301.2, 3301.7, 3301.0, 3301.4),
        (3301.4, 3301.8, 3301.1, 3301.6),
        (3301.6, 3301.8, 3301.2, 3301.7),
        (3301.7, 3301.8, 3301.4, 3301.7),
    ]:
        bars.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="LONDON_EARLY",
        segment_bars=bars,
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "fallback_bar7"
    assert trade.entry_bar_number == 7


def test_forced_session_v6_uses_contextual_asia_bar8_fallback() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "segment_forced_long_v6_contextual_fallback")
    current = datetime.fromisoformat("2026-04-21T19:03:00-04:00")
    rows = []
    for open_, high, low, close in [
        (3300.0, 3300.6, 3299.8, 3300.1),
        (3300.1, 3300.5, 3299.7, 3299.9),
        (3299.9, 3300.2, 3299.5, 3299.8),
        (3299.8, 3300.1, 3299.4, 3299.7),
        (3299.7, 3299.85, 3299.55, 3299.75),
        (3299.75, 3299.9, 3299.6, 3299.8),
        (3299.8, 3299.95, 3299.65, 3299.82),
        (3299.82, 3299.98, 3299.7, 3299.85),
        (3299.85, 3300.0, 3299.72, 3299.88),
    ]:
        rows.append(_bar(end_ts=current, open_=open_, high=high, low=low, close=close))
        current += timedelta(minutes=3)
    trade = _evaluate_forced_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        segment_id="ASIA_EARLY",
        segment_bars=rows,
        spec=spec,
    )
    assert trade.entered is True
    assert trade.entry_reason == "context_bar8_weak_setup"
    assert trade.entry_bar_number == 8
