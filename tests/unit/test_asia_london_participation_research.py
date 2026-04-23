from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from mgc_v05l.app.asia_london_participation_research import (
    _build_overnight_session_contexts,
    _evaluate_long_participation,
)
from mgc_v05l.app.gc_mgc_segment_forced_session_long_research import ForcedSegmentLongSpec
from mgc_v05l.research.trend_participation.models import ResearchBar


NEW_YORK = ZoneInfo("America/New_York")


def test_build_overnight_session_contexts_groups_asia_and_london_bars_by_trade_day() -> None:
    bars = [
        _bar("ES", "2026-01-04T19:03:00-05:00", 100.0, 101.0, 99.5, 100.8, 1000),
        _bar("ES", "2026-01-04T20:33:00-05:00", 100.8, 101.2, 100.5, 101.0, 900),
        _bar("ES", "2026-01-05T03:03:00-05:00", 101.0, 101.4, 100.9, 101.3, 800),
        _bar("ES", "2026-01-05T05:33:00-05:00", 101.3, 101.5, 101.0, 101.1, 700),
    ]

    contexts = _build_overnight_session_contexts(
        decision_bars=bars,
        symbol="ES",
        start_day=None,
        end_day=None,
    )

    assert len(contexts) == 1
    context = contexts[0]
    assert context.trade_date.isoformat() == "2026-01-05"
    assert len(context.entry_segment_bars) == 1
    assert len(context.hold_bars) == 4


def test_evaluate_long_participation_holds_until_london_late_close_when_no_earlier_exit() -> None:
    context = _build_context(
        [
            _bar("ES", "2026-01-04T19:03:00-05:00", 100.0, 100.4, 99.8, 100.2, 1000),
            _bar("ES", "2026-01-04T19:06:00-05:00", 100.2, 100.5, 100.0, 100.3, 1000),
            _bar("ES", "2026-01-04T19:09:00-05:00", 100.3, 100.6, 100.2, 100.4, 1000),
            _bar("ES", "2026-01-04T19:12:00-05:00", 100.4, 100.7, 100.3, 100.5, 1000),
            _bar("ES", "2026-01-04T19:15:00-05:00", 100.8, 101.0, 100.7, 100.9, 1000),
            _bar("ES", "2026-01-04T19:18:00-05:00", 100.9, 101.1, 100.8, 101.0, 1000),
            _bar("ES", "2026-01-04T19:21:00-05:00", 101.0, 101.2, 100.9, 101.1, 1000),
            _bar("ES", "2026-01-04T20:27:00-05:00", 101.1, 101.3, 101.0, 101.2, 1000),
            _bar("ES", "2026-01-04T20:33:00-05:00", 101.1, 101.3, 101.0, 101.2, 900),
            _bar("ES", "2026-01-05T03:03:00-05:00", 101.1, 101.3, 101.0, 101.2, 800),
            _bar("ES", "2026-01-05T05:33:00-05:00", 101.2, 101.4, 101.1, 101.3, 700),
        ]
    )
    spec = ForcedSegmentLongSpec(
        variant_id="segment_forced_long_v4_breakout_or_bar7",
        description="test",
        tick_size=0.25,
        fallback_entry_bar=7,
    )

    trade = _evaluate_long_participation(symbol="ES", context=context, spec=spec)

    assert trade.entered is True
    assert trade.entry_reason == "preferred_breakout"
    assert trade.exit_reason == "segment_close"
    assert trade.exit_session_phase == "LONDON_LATE"
    assert trade.entry_end_ts == "2026-01-04T19:18:00-05:00"
    assert trade.exit_end_ts == "2026-01-05T05:33:00-05:00"


def _build_context(bars: list[ResearchBar]):
    return _build_overnight_session_contexts(
        decision_bars=bars,
        symbol=str(bars[0].instrument),
        start_day=None,
        end_day=None,
    )[0]


def _bar(symbol: str, end_ts: str, open_: float, high: float, low: float, close: float, volume: int) -> ResearchBar:
    end_dt = datetime.fromisoformat(end_ts).astimezone(NEW_YORK)
    return ResearchBar(
        instrument=symbol,
        timeframe="3m",
        start_ts=end_dt,
        end_ts=end_dt,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        session_label="synthetic",
        session_segment="synthetic",
    )
