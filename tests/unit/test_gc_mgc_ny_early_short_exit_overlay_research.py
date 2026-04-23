from __future__ import annotations

from datetime import datetime, timedelta

from mgc_v05l.app.gc_mgc_ny_early_short_exit_overlay_research import ExitOverlaySpec, _overlay_exit_index
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(*, end_ts: datetime, open_: float, high: float, low: float, close: float) -> ResearchBar:
    return ResearchBar(
        instrument="GC",
        timeframe="3m",
        start_ts=end_ts - timedelta(minutes=3),
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


def test_overlay_exits_on_first_bar_adverse_close_without_progress() -> None:
    bars = [
        _bar(end_ts=datetime.fromisoformat("2026-04-21T08:32:00-04:00"), open_=100.0, high=100.6, low=99.9, close=100.2),
        _bar(end_ts=datetime.fromisoformat("2026-04-21T08:35:00-04:00"), open_=100.2, high=100.4, low=99.7, close=99.8),
    ]
    overlay = ExitOverlaySpec(
        overlay_id="fast_fail_first_bar_adverse",
        description="test",
        first_bar_max_close_r=0.0,
        first_bar_min_mfe_r=0.20,
    )

    result = _overlay_exit_index(
        segment_bars=bars,
        entry_index=0,
        entry_price=100.0,
        risk_points=2.0,
        overlay=overlay,
    )

    assert result == (0, "fast_fail_first_bar_adverse")


def test_overlay_waits_when_two_bar_progress_threshold_is_met() -> None:
    bars = [
        _bar(end_ts=datetime.fromisoformat("2026-04-21T08:32:00-04:00"), open_=100.0, high=100.2, low=99.1, close=99.4),
        _bar(end_ts=datetime.fromisoformat("2026-04-21T08:35:00-04:00"), open_=99.4, high=99.6, low=98.8, close=99.0),
    ]
    overlay = ExitOverlaySpec(
        overlay_id="fast_fail_two_bar_no_extension",
        description="test",
        second_bar_min_mfe_r=0.50,
    )

    result = _overlay_exit_index(
        segment_bars=bars,
        entry_index=0,
        entry_price=100.0,
        risk_points=2.0,
        overlay=overlay,
    )

    assert result is None
