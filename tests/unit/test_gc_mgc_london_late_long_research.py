from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.app.gc_mgc_london_late_long_research import (
    _evaluate_london_late_session,
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
    minutes = 5 if timeframe == "5m" else 1
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
    current = datetime.fromisoformat("2026-04-21T05:01:00-04:00")
    rows: list[ResearchBar] = []
    price = 3300.0
    for _ in range(30):
        rows.append(
            _bar(
                end_ts=current,
                open_=price,
                high=price + 0.12,
                low=price - 0.10,
                close=price + 0.03,
                volume=100,
                timeframe="1m",
            )
        )
        price += 0.01
        current += timedelta(minutes=1)
    return rows


def _segment_bars() -> list[ResearchBar]:
    current = datetime.fromisoformat("2026-04-21T05:35:00-04:00")
    rows: list[ResearchBar] = []
    for open_, high, low, close, volume in [
        (3300.2, 3301.0, 3300.1, 3300.9, 180),
        (3300.9, 3302.0, 3300.8, 3301.8, 190),
        (3301.8, 3303.2, 3301.7, 3303.0, 210),
        (3303.0, 3303.8, 3302.9, 3303.5, 220),
        (3303.5, 3305.2, 3303.4, 3304.9, 240),
        (3304.9, 3305.0, 3304.2, 3304.6, 200),
        (3304.6, 3304.7, 3303.9, 3304.1, 190),
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


def _noisy_pre_context() -> list[ResearchBar]:
    rows = _pre_context()
    rows[-5] = _bar(
        end_ts=rows[-5].end_ts,
        open_=3300.10,
        high=3301.10,
        low=3299.90,
        close=3300.95,
        volume=105,
        timeframe="1m",
    )
    rows[-2] = _bar(
        end_ts=rows[-2].end_ts,
        open_=3300.20,
        high=3301.05,
        low=3300.05,
        close=3300.90,
        volume=100,
        timeframe="1m",
    )
    return rows


def test_london_late_v1_qualifies_and_enters() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "london_late_long_v1")

    trade = _evaluate_london_late_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_pre_context(),
        segment_bars=_segment_bars(),
        spec=spec,
    )

    assert trade.qualified is True
    assert trade.entered is True
    assert trade.entry_bar_number == 5
    assert trade.exit_bar_number == 7
    assert trade.pnl_points is not None and trade.pnl_points > 0.0
    assert trade.net_pnl_points is not None and trade.net_pnl_points < trade.pnl_points


def test_london_late_rejects_weak_setup_close_location() -> None:
    spec = next(spec for spec in build_variant_specs() if spec.variant_id == "london_late_long_v1")
    weak_segment = _segment_bars()
    weak_segment[2] = _bar(
        end_ts=weak_segment[2].end_ts,
        open_=weak_segment[2].open,
        high=weak_segment[2].high,
        low=3300.8,
        close=3301.4,
        volume=weak_segment[2].volume,
        timeframe="5m",
    )

    trade = _evaluate_london_late_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_pre_context(),
        segment_bars=weak_segment,
        spec=spec,
    )

    assert trade.qualified is False
    assert trade.entered is False
    assert "setup_close_location_ok" in trade.notes


def test_score_based_variant_admits_borderline_setup_that_strict_variant_rejects() -> None:
    strict_spec = next(spec for spec in build_variant_specs() if spec.variant_id == "london_late_long_v2")
    soft_spec = next(spec for spec in build_variant_specs() if spec.variant_id == "london_late_long_v2b_5m_soft")
    softer_segment = _segment_bars()
    softer_segment[2] = _bar(
        end_ts=softer_segment[2].end_ts,
        open_=3301.8,
        high=3302.7,
        low=3301.7,
        close=3301.75,
        volume=125,
        timeframe="5m",
    )

    strict_trade = _evaluate_london_late_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_noisy_pre_context(),
        segment_bars=softer_segment,
        spec=strict_spec,
    )
    soft_trade = _evaluate_london_late_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_noisy_pre_context(),
        segment_bars=softer_segment,
        spec=soft_spec,
    )

    assert strict_trade.qualified is False
    assert soft_trade.qualified is True
    assert soft_trade.setup_score is not None and soft_trade.setup_score >= soft_spec.min_setup_score


def test_meta_filter_variant_rejects_soft_candidate_with_weak_context_quality() -> None:
    soft_spec = next(spec for spec in build_variant_specs() if spec.variant_id == "london_late_long_v5_3m_soft")
    meta_spec = next(spec for spec in build_variant_specs() if spec.variant_id == "london_late_long_v7_3m_meta")
    segment = _segment_bars()
    segment[3] = _bar(
        end_ts=segment[3].end_ts,
        open_=3303.0,
        high=3303.4,
        low=3302.7,
        close=3302.35,
        volume=150,
        timeframe="5m",
    )

    soft_trade = _evaluate_london_late_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_noisy_pre_context(),
        segment_bars=segment,
        spec=soft_spec,
    )
    meta_trade = _evaluate_london_late_session(
        symbol="GC",
        trade_day=date(2026, 4, 21),
        pre_context=_noisy_pre_context(),
        segment_bars=segment,
        spec=meta_spec,
    )

    assert soft_trade.qualified is True
    assert meta_trade.qualified is False
    assert meta_trade.meta_score is not None and meta_trade.meta_score < meta_spec.min_meta_score
