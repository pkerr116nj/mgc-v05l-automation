from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.research.trend_participation import (
    annotate_trade_sequence_features,
    build_cross_run_summary,
)


def _trade(
    *,
    minutes: int,
    pnl_cash: float,
    session_segment: str,
    vwap_state: str,
) -> dict[str, object]:
    decision_ts = datetime(2026, 1, 5, 14, 0, tzinfo=UTC) + timedelta(minutes=minutes)
    return {
        "decision_ts": decision_ts,
        "entry_ts": decision_ts + timedelta(minutes=1),
        "exit_ts": decision_ts + timedelta(minutes=11),
        "session_segment": session_segment,
        "pnl_cash": pnl_cash,
        "mfe_points": 1.5 if pnl_cash > 0 else 0.7,
        "mae_points": 0.5 if pnl_cash > 0 else 1.2,
        "bars_held_1m": 10,
        "hold_minutes": 10.0,
        "side": "LONG",
        "family": "atp_v1_long_pullback_continuation",
        "bias_state": "LONG_BIAS",
        "pullback_state": "NORMAL_PULLBACK",
        "timing_state": "ATP_TIMING_CONFIRMED",
        "vwap_price_quality_state": vwap_state,
    }


def test_annotate_trade_sequence_features_marks_first_later_density_and_loss_streaks() -> None:
    rows = annotate_trade_sequence_features(
        [
            _trade(minutes=0, pnl_cash=15.0, session_segment="US", vwap_state="VWAP_FAVORABLE"),
            _trade(minutes=5, pnl_cash=-10.0, session_segment="US", vwap_state="VWAP_NEUTRAL"),
            _trade(minutes=70, pnl_cash=-8.0, session_segment="LONDON", vwap_state="VWAP_NEUTRAL"),
            _trade(minutes=24 * 60 + 10, pnl_cash=12.0, session_segment="US", vwap_state="VWAP_FAVORABLE"),
        ]
    )

    assert rows[0]["trade_sequence_position"] == "FIRST_IN_SESSION_BUCKET"
    assert rows[1]["trade_sequence_position"] == "LATER_IN_SESSION_BUCKET"
    assert rows[0]["session_bucket_density"] == "PAIR"
    assert rows[2]["session_bucket_density"] == "ISOLATED"
    assert rows[2]["prior_loss_streak_bucket"] == "AFTER_1_LOSS"
    assert rows[3]["prior_loss_streak_bucket"] == "AFTER_2_PLUS_LOSSES"
    assert rows[1]["time_since_prior_trade_minutes"] == 5.0


def test_build_cross_run_summary_aggregates_medians_and_hypotheses() -> None:
    combined_validation = {
        "atp_phase3_performance": {
            "total_trades": 4,
            "net_pnl_cash": 120.0,
            "profit_factor": 1.2,
            "win_rate": 50.0,
            "average_trade_pnl_cash": 30.0,
            "max_drawdown": 80.0,
            "entries_per_100_bars": 2.0,
            "average_favorable_excursion_points": 1.8,
            "average_adverse_excursion_points": 0.9,
        },
        "same_window_comparison": {
            "atp_phase3": {
                "trade_count": 4,
                "net_pnl_cash": 120.0,
                "profit_factor": 1.2,
                "max_drawdown": 80.0,
                "win_rate": 50.0,
                "average_trade_pnl_cash": 30.0,
                "entries_per_100_bars": 2.0,
            },
            "legacy_replay_proxy": {
                "trade_count": 2,
                "net_pnl_cash": 90.0,
                "profit_factor": 1.5,
                "max_drawdown": 30.0,
                "win_rate": 55.0,
                "average_trade_pnl_cash": 45.0,
                "entries_per_100_bars": 1.0,
            },
            "delta": {
                "trade_count_delta": 2,
                "net_pnl_cash_delta": 30.0,
                "profit_factor_delta": -0.3,
                "max_drawdown_delta": 50.0,
                "win_rate_delta": -5.0,
                "average_trade_pnl_cash_delta": -15.0,
                "entries_per_100_bars_delta": 1.0,
            },
        },
        "segment_breakdowns": {
            "by_vwap_price_quality_state": [
                {
                    "segment": "VWAP_FAVORABLE",
                    "total_trades": 2,
                    "net_pnl_cash": 180.0,
                    "profit_factor": 1.7,
                    "average_trade_pnl_cash": 90.0,
                },
                {
                    "segment": "VWAP_NEUTRAL",
                    "total_trades": 2,
                    "net_pnl_cash": -60.0,
                    "profit_factor": 0.7,
                    "average_trade_pnl_cash": -30.0,
                },
            ],
            "by_session_segment": [
                {
                    "segment": "ASIA",
                    "total_trades": 1,
                    "net_pnl_cash": 75.0,
                    "profit_factor": 1.6,
                    "average_trade_pnl_cash": 75.0,
                },
                {
                    "segment": "LONDON",
                    "total_trades": 2,
                    "net_pnl_cash": -40.0,
                    "profit_factor": 0.8,
                    "average_trade_pnl_cash": -20.0,
                },
                {
                    "segment": "US",
                    "total_trades": 1,
                    "net_pnl_cash": 85.0,
                    "profit_factor": 1.4,
                    "average_trade_pnl_cash": 85.0,
                },
            ],
            "by_pullback_state": [],
            "by_bias_state": [],
            "by_timing_state": [],
            "by_entry_family": [],
        },
    }
    run_rows = [
        {
            "label": "run_a",
            "bars_processed": 100,
            "tags": {"tape_direction_tag": "UP_TAPE", "regime_tag": "TREND_HEAVY", "dominant_session_tag": "US"},
            "atp_phase3_performance": {
                "total_trades": 2,
                "net_pnl_cash": 90.0,
                "profit_factor": 1.3,
                "win_rate": 50.0,
                "average_trade_pnl_cash": 45.0,
                "max_drawdown": 40.0,
                "entries_per_100_bars": 2.0,
                "average_favorable_excursion_points": 1.9,
                "average_adverse_excursion_points": 0.8,
            },
        },
        {
            "label": "run_b",
            "bars_processed": 120,
            "tags": {"tape_direction_tag": "DOWN_TAPE", "regime_tag": "CHOP_HEAVY", "dominant_session_tag": "LONDON"},
            "atp_phase3_performance": {
                "total_trades": 4,
                "net_pnl_cash": 30.0,
                "profit_factor": 1.1,
                "win_rate": 45.0,
                "average_trade_pnl_cash": 7.5,
                "max_drawdown": 120.0,
                "entries_per_100_bars": 3.3333,
                "average_favorable_excursion_points": 1.7,
                "average_adverse_excursion_points": 1.0,
            },
        },
    ]

    summary = build_cross_run_summary(
        run_rows=run_rows,
        combined_validation=combined_validation,
        combined_enriched_trades=annotate_trade_sequence_features(
            [
                _trade(minutes=0, pnl_cash=60.0, session_segment="US", vwap_state="VWAP_FAVORABLE"),
                _trade(minutes=6, pnl_cash=-20.0, session_segment="US", vwap_state="VWAP_NEUTRAL"),
                _trade(minutes=70, pnl_cash=-20.0, session_segment="LONDON", vwap_state="VWAP_NEUTRAL"),
                _trade(minutes=24 * 60 + 5, pnl_cash=100.0, session_segment="ASIA", vwap_state="VWAP_FAVORABLE"),
            ]
        ),
        total_bars=220,
    )

    assert summary["run_count"] == 2
    assert summary["run_medians"]["trade_count"] == 3
    assert summary["run_medians"]["net_pnl_cash"] == 60.0
    assert summary["hypothesis_checks"]["vwap_neutral_vs_favorable"]["neutral_is_quality_leak"] is True
    assert summary["hypothesis_checks"]["session_strength"]["london_is_structurally_weaker"] is True
    assert "by_trade_sequence_position" in summary["segment_breakdowns"]
    assert "by_session_bucket_density" in summary["segment_breakdowns"]
