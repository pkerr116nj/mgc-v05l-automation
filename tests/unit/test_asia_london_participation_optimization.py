from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.app.asia_london_participation_optimization import (
    _apply_gate_to_trade,
    _summarize_trades_with_drawdown,
)
from mgc_v05l.app.asia_london_participation_research import OvernightParticipationTrade, OvernightSessionContext
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(*, minute: int, close: float, low: float | None = None, high: float | None = None) -> ResearchBar:
    price_low = close if low is None else low
    price_high = close if high is None else high
    ts = datetime(2026, 1, 5, 0, minute, tzinfo=UTC)
    return ResearchBar(
        instrument="MES",
        timeframe="3m",
        start_ts=ts,
        end_ts=ts,
        open=close - 0.25,
        high=price_high,
        low=price_low,
        close=close,
        volume=1000,
        session_label="overnight",
        session_segment="ASIA_EARLY",
    )


def test_apply_gate_to_trade_clears_entered_trade_when_bias_gate_fails() -> None:
    context = OvernightSessionContext(
        trade_date=datetime(2026, 1, 5, tzinfo=UTC).date(),
        entry_segment_bars=[
            _bar(minute=0, close=100.0),
            _bar(minute=3, close=99.5),
            _bar(minute=6, close=99.0),
            _bar(minute=9, close=98.5),
        ],
        hold_bars=[],
    )
    trade = OvernightParticipationTrade(
        symbol="MES",
        trade_date="2026-01-05",
        side="LONG",
        variant_id="segment_forced_long_v5_dip_reclaim_or_bar8",
        entered=True,
        entry_reason="preferred_dip_reclaim",
        entry_bar_number=4,
        entry_end_ts="2026-01-05T00:09:00+00:00",
        entry_price=99.0,
        stop_price=98.0,
        exit_bar_number=10,
        exit_end_ts="2026-01-05T05:30:00+00:00",
        exit_price=101.0,
        exit_reason="segment_close",
        exit_session_phase="LONDON_LATE",
        pnl_points=2.0,
        net_pnl_points=1.5,
        gross_r_multiple=2.0,
        net_r_multiple=1.5,
        mae_points=0.5,
        mfe_points=2.5,
        setup_return_points=0.0,
        setup_range_points=1.0,
        setup_close_location=0.5,
        setup_vwap_displacement=0.0,
        notes=(),
    )

    gated = _apply_gate_to_trade(trade=trade, context=context, side="LONG", gate_mode="atp_bias_gate")

    assert gated.entered is False
    assert gated.net_pnl_points is None
    assert any(str(note).startswith("bias_gate_failed:") for note in gated.notes)


def test_summarize_trades_with_drawdown_reports_max_drawdown() -> None:
    trades = [
        OvernightParticipationTrade(
            symbol="GC",
            trade_date="2026-01-05",
            side="LONG",
            variant_id="v",
            entered=True,
            entry_reason="x",
            entry_bar_number=1,
            entry_end_ts="2026-01-05T00:00:00+00:00",
            entry_price=1.0,
            stop_price=0.0,
            exit_bar_number=2,
            exit_end_ts="2026-01-05T00:03:00+00:00",
            exit_price=2.0,
            exit_reason="x",
            exit_session_phase="ASIA_LATE",
            pnl_points=3.0,
            net_pnl_points=3.0,
            gross_r_multiple=3.0,
            net_r_multiple=3.0,
            mae_points=0.0,
            mfe_points=3.0,
            setup_return_points=1.0,
            setup_range_points=1.0,
            setup_close_location=0.8,
            setup_vwap_displacement=0.2,
            notes=(),
        ),
        OvernightParticipationTrade(
            symbol="GC",
            trade_date="2026-01-06",
            side="LONG",
            variant_id="v",
            entered=True,
            entry_reason="x",
            entry_bar_number=1,
            entry_end_ts="2026-01-06T00:00:00+00:00",
            entry_price=1.0,
            stop_price=0.0,
            exit_bar_number=2,
            exit_end_ts="2026-01-06T00:03:00+00:00",
            exit_price=2.0,
            exit_reason="x",
            exit_session_phase="ASIA_LATE",
            pnl_points=-2.0,
            net_pnl_points=-2.0,
            gross_r_multiple=-2.0,
            net_r_multiple=-2.0,
            mae_points=2.0,
            mfe_points=0.0,
            setup_return_points=-1.0,
            setup_range_points=1.0,
            setup_close_location=0.2,
            setup_vwap_displacement=-0.2,
            notes=(),
        ),
    ]

    summary = _summarize_trades_with_drawdown(trades)

    assert summary["entered_trade_count"] == 2
    assert summary["total_net_pnl_points"] == 1.0
    assert summary["max_drawdown_points"] == 2.0
