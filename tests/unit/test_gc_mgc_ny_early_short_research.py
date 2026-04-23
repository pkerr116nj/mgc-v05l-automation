from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.app.gc_mgc_ny_early_short_research import (
    _evaluate_ny_early_session,
    build_variant_specs,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(
    *,
    end_ts: datetime,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int,
    timeframe: str,
) -> ResearchBar:
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
        volume=volume,
        session_label="TEST",
        session_segment="TEST",
        source="test",
    )


def _pre_context() -> list[ResearchBar]:
    current = datetime.fromisoformat("2026-04-21T07:51:00-04:00")
    rows: list[ResearchBar] = []
    price = 3300.0
    for _ in range(30):
        rows.append(
            _bar(
                end_ts=current,
                open_=price,
                high=price + 0.10,
                low=price - 0.12,
                close=price + 0.01,
                volume=120,
                timeframe="1m",
            )
        )
        price += 0.01
        current += timedelta(minutes=1)
    return rows


def _failed_pop_segment_5m() -> list[ResearchBar]:
    current = datetime.fromisoformat("2026-04-21T08:25:00-04:00")
    rows: list[ResearchBar] = []
    for open_, high, low, close, volume in [
        (3300.4, 3302.4, 3300.3, 3302.2, 250),
        (3302.2, 3304.2, 3302.1, 3304.0, 280),
        (3304.0, 3305.0, 3303.8, 3304.6, 310),
        (3304.6, 3304.8, 3300.0, 3300.2, 330),
        (3300.2, 3300.5, 3298.2, 3298.6, 320),
        (3298.6, 3299.7, 3298.4, 3299.0, 210),
    ]:
        rows.append(
            _bar(
                end_ts=current,
                open_=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                timeframe="5m",
            )
        )
        current += timedelta(minutes=5)
    return rows


def _downside_resumption_segment_5m() -> list[ResearchBar]:
    current = datetime.fromisoformat("2026-04-21T08:25:00-04:00")
    rows: list[ResearchBar] = []
    for open_, high, low, close, volume in [
        (3300.5, 3300.7, 3298.8, 3299.0, 220),
        (3299.0, 3299.2, 3297.5, 3297.8, 240),
        (3297.8, 3297.9, 3296.4, 3296.7, 260),
        (3296.7, 3296.9, 3294.4, 3294.9, 280),
        (3294.9, 3295.0, 3292.8, 3293.0, 290),
        (3293.0, 3293.4, 3291.6, 3292.0, 230),
    ]:
        rows.append(
            _bar(
                end_ts=current,
                open_=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                timeframe="5m",
            )
        )
        current += timedelta(minutes=5)
    return rows


def test_ny_early_failed_pop_v1_qualifies_and_enters() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "ny_early_short_v1_failed_pop_5m")

    trade = _evaluate_ny_early_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_pre_context(),
        segment_bars=_failed_pop_segment_5m(),
        spec=spec,
    )

    assert trade.qualified is True
    assert trade.entered is True
    assert trade.entry_bar_number == 5
    assert trade.exit_bar_number is not None
    assert trade.pnl_points is not None and trade.pnl_points > 0.0


def test_ny_early_downside_resumption_v3_qualifies_and_enters() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "ny_early_short_v3_downside_resume_5m")

    trade = _evaluate_ny_early_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_pre_context(),
        segment_bars=_downside_resumption_segment_5m(),
        spec=spec,
    )

    assert trade.qualified is True
    assert trade.entered is True
    assert trade.pnl_points is not None and trade.pnl_points > 0.0


def test_ny_early_failed_pop_rejects_weak_green_extension() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "ny_early_short_v1_failed_pop_5m")
    weak_segment = _failed_pop_segment_5m()
    weak_segment[2] = _bar(
        end_ts=weak_segment[2].end_ts,
        open_=3304.0,
        high=3304.4,
        low=3303.6,
        close=3303.9,
        volume=160,
        timeframe="5m",
    )

    trade = _evaluate_ny_early_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_pre_context(),
        segment_bars=weak_segment,
        spec=spec,
    )

    assert trade.qualified is False
    assert trade.entered is False
    assert "setup_green_share_ok" in trade.notes or "setup_close_location_ok" in trade.notes
