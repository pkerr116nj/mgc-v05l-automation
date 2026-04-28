from __future__ import annotations

from datetime import datetime

from mgc_v05l.app import atp_companion_gc_asia_only_directional_counterfactual_v2 as subject


def _trade(
    *,
    trade_id: str,
    entry_ts: str,
    exit_ts: str,
    baseline_pnl_cash: float,
    minute_path: list[dict[str, object]],
    exit_reason: str = "time_stop",
) -> dict[str, object]:
    entry_dt = datetime.fromisoformat(entry_ts)
    return {
        "trade_id": trade_id,
        "posture": "LONG",
        "entry_ts": entry_dt,
        "exit_ts": datetime.fromisoformat(exit_ts),
        "entry_date": entry_dt.date().isoformat(),
        "entry_year": entry_dt.year,
        "entry_month": entry_dt.strftime("%Y-%m"),
        "entry_quarter": f"{entry_dt.year}-Q{((entry_dt.month - 1) // 3) + 1}",
        "entry_hour": entry_dt.hour,
        "direction": "LONG",
        "exit_reason": exit_reason,
        "gross_pnl_cash": baseline_pnl_cash + 51.5,
        "fees_paid": 1.5,
        "slippage_cost": 50.0,
        "trade_pnl_cash": baseline_pnl_cash,
        "entry_price": 100.0,
        "exit_price": 101.0,
        "stop_price": 98.0,
        "target_price": 102.0,
        "initial_risk_points": 2.0,
        "point_value": 100.0,
        "minute_path": minute_path,
    }


def test_mirrored_short_hits_target_when_path_sells_off() -> None:
    trade = _trade(
        trade_id="a",
        entry_ts="2024-01-01T19:00:00-05:00",
        exit_ts="2024-01-01T19:02:00-05:00",
        baseline_pnl_cash=-251.5,
        minute_path=[
            {
                "timestamp": "2024-01-01 19:01:00-05:00",
                "open": 100.0,
                "high": 100.5,
                "low": 97.5,
                "close": 98.0,
            }
        ],
        exit_reason="stop",
    )

    short_trade = subject._simulate_mirrored_short_trade(trade)

    assert short_trade["exit_reason"] == "target"
    assert short_trade["trade_pnl_cash"] == 148.5


def test_mirrored_short_uses_conservative_stop_first_when_both_levels_print() -> None:
    trade = _trade(
        trade_id="b",
        entry_ts="2024-01-02T19:00:00-05:00",
        exit_ts="2024-01-02T19:02:00-05:00",
        baseline_pnl_cash=148.5,
        minute_path=[
            {
                "timestamp": "2024-01-02 19:01:00-05:00",
                "open": 100.0,
                "high": 102.5,
                "low": 97.5,
                "close": 99.0,
            }
        ],
        exit_reason="target",
    )

    short_trade = subject._simulate_mirrored_short_trade(trade)

    assert short_trade["exit_reason"] == "stop"
    assert short_trade["trade_pnl_cash"] == -251.5


def test_classification_prefers_flat_filter_when_short_is_bad_and_recovery_is_long_only() -> None:
    rows = [
        {"posture": "LONG", "period_id": "PEAK_TO_TROUGH", "net_pnl_cash": -1000.0},
        {"posture": "SHORT", "period_id": "PEAK_TO_TROUGH", "net_pnl_cash": -2000.0},
        {"posture": "FLAT", "period_id": "PEAK_TO_TROUGH", "net_pnl_cash": 0.0},
        {"posture": "LONG", "period_id": "TROUGH_TO_RECOVERY", "net_pnl_cash": 1500.0},
        {"posture": "SHORT", "period_id": "TROUGH_TO_RECOVERY", "net_pnl_cash": -500.0},
        {"posture": "FLAT", "period_id": "TROUGH_TO_RECOVERY", "net_pnl_cash": 0.0},
        {"posture": "LONG", "period_id": "FULL_EPISODE", "net_pnl_cash": 500.0},
        {"posture": "SHORT", "period_id": "FULL_EPISODE", "net_pnl_cash": -2500.0},
        {"posture": "FLAT", "period_id": "FULL_EPISODE", "net_pnl_cash": 0.0},
        {"posture": "LONG", "period_id": "OUTSIDE_EPISODE", "net_pnl_cash": 5000.0},
        {"posture": "SHORT", "period_id": "OUTSIDE_EPISODE", "net_pnl_cash": -200.0},
        {"posture": "FLAT", "period_id": "OUTSIDE_EPISODE", "net_pnl_cash": 0.0},
    ]

    assert subject._classify(rows) == "FLAT_REGIME_FILTER_PROMISING"
