from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.app.asia_london_participation_research import OvernightParticipationTrade, OvernightSessionContext
from mgc_v05l.app.es_mes_asia_london_volatility_floor_research import _apply_volatility_floor, _pair_rankings
from mgc_v05l.research.trend_participation.models import ResearchBar


def _bar(*, minute: int, close: float, low: float, high: float) -> ResearchBar:
    ts = datetime(2026, 1, 5, 0, minute, tzinfo=UTC)
    return ResearchBar(
        instrument="MES",
        timeframe="3m",
        start_ts=ts,
        end_ts=ts,
        open=close - 0.25,
        high=high,
        low=low,
        close=close,
        volume=1000,
        session_label="overnight",
        session_segment="ASIA_EARLY",
    )


def _trade() -> OvernightParticipationTrade:
    ts = datetime(2026, 1, 5, 0, 9, tzinfo=UTC).isoformat()
    return OvernightParticipationTrade(
        symbol="MES",
        trade_date="2026-01-05",
        side="LONG",
        variant_id="segment_forced_long_v6_contextual_fallback",
        entered=True,
        entry_reason="contextual_fallback",
        entry_bar_number=4,
        entry_end_ts=ts,
        entry_price=100.0,
        stop_price=99.0,
        exit_bar_number=10,
        exit_end_ts=ts,
        exit_price=101.0,
        exit_reason="segment_close",
        exit_session_phase="LONDON_LATE",
        pnl_points=1.0,
        net_pnl_points=0.75,
        gross_r_multiple=1.0,
        net_r_multiple=0.75,
        mae_points=0.5,
        mfe_points=1.5,
        setup_return_points=0.5,
        setup_range_points=1.0,
        setup_close_location=0.8,
        setup_vwap_displacement=0.3,
        notes=(),
    )


def test_apply_volatility_floor_blocks_quiet_setup() -> None:
    context = OvernightSessionContext(
        trade_date=datetime(2026, 1, 5, tzinfo=UTC).date(),
        entry_segment_bars=[
            _bar(minute=0, close=100.0, low=99.95, high=100.05),
            _bar(minute=3, close=100.02, low=99.97, high=100.06),
            _bar(minute=6, close=100.01, low=99.98, high=100.04),
            _bar(minute=9, close=100.0, low=99.97, high=100.03),
        ],
        hold_bars=[],
    )

    gated = _apply_volatility_floor(trade=_trade(), context=context, setup_bar_count=4, floor_ratio=1.5)

    assert gated.entered is False
    assert gated.net_pnl_points is None
    assert any(str(note).startswith("volatility_floor_failed:") for note in gated.notes)


def test_pair_rankings_orders_by_minimum_pair_score() -> None:
    symbol_reports = {
        "ES": {
            "variants": {
                "LONG__segment_forced_long_v6_contextual_fallback__vol_floor_0p75": {
                    "side": "LONG",
                    "variant_id": "segment_forced_long_v6_contextual_fallback",
                    "gate_mode": "vol_floor_0p75",
                    "volatility_floor_ratio": 0.75,
                    "trade_summary": {"average_net_pnl_points": 0.4, "net_profit_factor": 1.3, "max_drawdown_points": 5.0, "entered_trade_count": 100},
                },
                "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__vol_floor_0p75": {
                    "side": "SHORT",
                    "variant_id": "segment_forced_short_v2_reclaim_fail_or_bar7",
                    "gate_mode": "vol_floor_0p75",
                    "volatility_floor_ratio": 0.75,
                    "trade_summary": {"average_net_pnl_points": -0.1, "net_profit_factor": 0.9, "max_drawdown_points": 6.0, "entered_trade_count": 80},
                },
            }
        },
        "MES": {
            "variants": {
                "LONG__segment_forced_long_v6_contextual_fallback__vol_floor_0p75": {
                    "side": "LONG",
                    "variant_id": "segment_forced_long_v6_contextual_fallback",
                    "gate_mode": "vol_floor_0p75",
                    "volatility_floor_ratio": 0.75,
                    "trade_summary": {"average_net_pnl_points": 0.2, "net_profit_factor": 1.1, "max_drawdown_points": 4.0, "entered_trade_count": 90},
                },
                "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7__vol_floor_0p75": {
                    "side": "SHORT",
                    "variant_id": "segment_forced_short_v2_reclaim_fail_or_bar7",
                    "gate_mode": "vol_floor_0p75",
                    "volatility_floor_ratio": 0.75,
                    "trade_summary": {"average_net_pnl_points": -0.2, "net_profit_factor": 0.8, "max_drawdown_points": 8.0, "entered_trade_count": 75},
                },
            }
        },
    }

    rankings = _pair_rankings(symbol_reports=symbol_reports)

    assert rankings["ES_MES"][0]["variant_key"] == "LONG__segment_forced_long_v6_contextual_fallback__vol_floor_0p75"
    assert rankings["ES_MES"][0]["min_average_net_pnl_points"] == 0.2
