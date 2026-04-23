from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.app.es_mes_nq_led_asia_london_research import _pair_rankings, _summarize_follower_trades, FollowerTrade


def _trade(*, symbol: str, side: str, variant_id: str, net_pnl: float, max_drawdown: float) -> FollowerTrade:
    ts = datetime(2026, 1, 5, 0, 0, tzinfo=UTC).isoformat()
    return FollowerTrade(
        symbol=symbol,
        leader_symbol="NQ" if symbol == "ES" else "MNQ",
        trade_date="2026-01-05",
        side=side,
        leader_variant_id=variant_id,
        entered=True,
        entry_reason="followed_leader_entry",
        leader_entry_reason="leader_entered",
        leader_entry_end_ts=ts,
        follower_entry_end_ts=ts,
        follower_entry_price=100.0,
        stop_price=99.0,
        exit_end_ts=ts,
        exit_price=101.0,
        exit_reason="segment_close",
        pnl_points=net_pnl,
        net_pnl_points=net_pnl,
        gross_r_multiple=net_pnl,
        max_drawdown_points=max_drawdown,
        notes=(),
    )


def test_summarize_follower_trades_reports_profit_factor_and_drawdown() -> None:
    summary = _summarize_follower_trades(
        [
            _trade(symbol="ES", side="LONG", variant_id="segment_forced_long_v6_contextual_fallback", net_pnl=2.5, max_drawdown=1.0),
            _trade(symbol="ES", side="LONG", variant_id="segment_forced_long_v6_contextual_fallback", net_pnl=-1.0, max_drawdown=2.0),
        ]
    )

    assert summary["entered_trade_count"] == 2
    assert summary["avg_net_pnl_points"] == 0.75
    assert summary["profit_factor"] == 2.5
    assert summary["max_drawdown_points"] == 2.0


def test_pair_rankings_uses_minimum_pair_score() -> None:
    symbol_reports = {
        "ES": {
            "variants": {
                "LONG__segment_forced_long_v6_contextual_fallback": {
                    "trade_summary": {"avg_net_pnl_points": 1.5, "profit_factor": 1.8},
                },
                "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7": {
                    "trade_summary": {"avg_net_pnl_points": 0.5, "profit_factor": 1.1},
                },
            }
        },
        "MES": {
            "variants": {
                "LONG__segment_forced_long_v6_contextual_fallback": {
                    "trade_summary": {"avg_net_pnl_points": 1.2, "profit_factor": 1.6},
                },
                "SHORT__segment_forced_short_v2_reclaim_fail_or_bar7": {
                    "trade_summary": {"avg_net_pnl_points": -0.2, "profit_factor": 0.8},
                },
            }
        },
    }

    rankings = _pair_rankings(symbol_reports=symbol_reports)

    assert rankings["ES_MES"][0]["variant_key"] == "LONG__segment_forced_long_v6_contextual_fallback"
    assert rankings["ES_MES"][0]["min_avg_net_pnl_points"] == 1.2
    assert rankings["ES_MES"][0]["min_profit_factor"] == 1.6
