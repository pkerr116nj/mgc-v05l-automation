from __future__ import annotations

from datetime import date, datetime, time, timedelta

from mgc_v05l.app.gc_mgc_segment_regime_research import (
    GoldSegmentRegimeRow,
    _apply_empirical_regime_scores,
    build_segment_regime_rows,
    label_gold_segment,
    trade_date_for_timestamp,
)
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(
    end_ts: str,
    open_: float,
    high: float,
    low: float,
    close: float,
    volume: int = 100,
) -> ResearchBar:
    end_dt = datetime.fromisoformat(end_ts)
    return ResearchBar(
        instrument="GC",
        timeframe="1m",
        start_ts=end_dt - timedelta(minutes=1),
        end_ts=end_dt,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
        session_label="TEST",
        session_segment="TEST",
        source="test",
    )


def test_trade_date_and_segment_label_follow_gold_session_boundaries() -> None:
    asia_dt = datetime.fromisoformat("2026-04-20T18:05:00-04:00")
    ny_dt = datetime.fromisoformat("2026-04-21T08:25:00-04:00")

    assert trade_date_for_timestamp(asia_dt) == date(2026, 4, 21)
    assert trade_date_for_timestamp(ny_dt) == date(2026, 4, 21)
    assert label_gold_segment(asia_dt) == "SESSION_OPEN"
    assert label_gold_segment(datetime.fromisoformat("2026-04-21T01:15:00-04:00")) == "ASIA_LATE"
    assert label_gold_segment(ny_dt) == "US_EARLY"
    assert label_gold_segment(datetime.fromisoformat("2026-04-21T13:35:00-04:00")) == "US_LATE"


def test_build_segment_regime_rows_emits_gold_native_long_and_short_labels() -> None:
    bars: list[ResearchBar] = []
    current = datetime.fromisoformat("2026-04-21T07:50:00-04:00")
    price = 3300.0
    for _ in range(30):
        bars.append(_bar(current.isoformat(), price, price + 0.2, price - 0.2, price + 0.1, 80))
        price += 0.05
        current += timedelta(minutes=1)

    current = datetime.fromisoformat("2026-04-21T08:20:00-04:00")
    minute_specs = [
        (3301.5, 3302.4, 3301.4, 3302.2),
        (3302.2, 3303.0, 3302.0, 3302.9),
        (3302.9, 3303.6, 3302.8, 3303.5),
        (3303.5, 3304.0, 3303.3, 3303.8),
        (3303.8, 3304.4, 3303.7, 3304.2),
        (3304.2, 3304.8, 3304.0, 3304.7),
        (3304.7, 3305.4, 3304.6, 3305.2),
        (3305.2, 3305.9, 3305.0, 3305.7),
        (3305.7, 3306.1, 3305.5, 3306.0),
        (3306.0, 3306.4, 3305.8, 3306.2),
    ]
    for open_, high, low, close in minute_specs:
        bars.append(_bar(current.isoformat(), open_, high, low, close, 180))
        current += timedelta(minutes=1)

    rows = build_segment_regime_rows(
        symbol="GC",
        bars=bars,
        start_day=date(2026, 4, 21),
        end_day=date(2026, 4, 21),
        tick_size=0.1,
        setup_minutes=5,
        pre_context_minutes=30,
    )

    assert len(rows) == 2
    row = next(item for item in rows if item.segment_id == "US_EARLY")
    assert row.segment_id == "US_EARLY"
    assert row.trade_date == "2026-04-21"
    assert row.pre_context_bar_count == 30
    assert row.long_triggered is True
    assert row.long_close_pnl_points is not None and row.long_close_pnl_points > 0.0
    assert row.long_mfe_points is not None and row.long_mfe_points > 0.0
    assert row.short_triggered is False
    assert row.long_regime_score is not None
    assert row.short_regime_score is not None


def test_empirical_scorecard_rewards_stronger_positive_rows() -> None:
    rows = []
    for index in range(8):
        positive = index >= 4
        rows.append(
            GoldSegmentRegimeRow(
                symbol="GC",
                trade_date=f"2026-04-{index + 1:02d}",
                segment_id="LONDON_EARLY",
                segment_start_ts=f"2026-04-{index + 1:02d}T03:00:00-04:00",
                segment_end_ts=f"2026-04-{index + 1:02d}T05:30:00-04:00",
                segment_bar_count=30,
                setup_bar_count=15,
                continuation_bar_count=15,
                pre_context_bar_count=30,
                baseline_context_bar_count=30,
                pre_context_range_points=1.0 + index * 0.1,
                pre_context_drift_points=0.1 * index,
                pre_context_compression_ratio=0.5 + index * 0.05,
                pre_context_mean_abs_delta=0.2 + index * 0.02,
                setup_range_points=1.5 + index * 0.3,
                setup_return_points=0.2 + index * 0.4,
                setup_abs_efficiency=0.2 + index * 0.08,
                setup_close_location=0.2 + index * 0.08,
                setup_vwap_displacement=0.1 + index * 0.05,
                setup_volume_ratio=0.8 + index * 0.15,
                setup_green_share=0.2 + index * 0.08,
                setup_red_share=0.8 - index * 0.08,
                setup_pressure=-0.6 + index * 0.2,
                segment_close_price=3300.0 + index,
                long_trigger_price=3300.5,
                long_stop_price=3298.5,
                long_risk_points=2.0,
                long_triggered=True,
                long_entry_ts=f"2026-04-{index + 1:02d}T03:20:00-04:00",
                long_close_pnl_points=2.5 if positive else -1.0,
                long_mfe_points=2.6 if positive else 0.6,
                long_mae_points=0.5 if positive else 1.4,
                long_one_r_reached=positive,
                long_positive_label=positive,
                short_trigger_price=3299.5,
                short_stop_price=3301.5,
                short_risk_points=2.0,
                short_triggered=False,
                short_entry_ts=None,
                short_close_pnl_points=None,
                short_mfe_points=None,
                short_mae_points=None,
                short_one_r_reached=False,
                short_positive_label=False,
            )
        )

    _apply_empirical_regime_scores(rows)

    assert rows[0].long_regime_score is not None
    assert rows[-1].long_regime_score is not None
    assert rows[-1].long_regime_score > rows[0].long_regime_score
