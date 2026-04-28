from __future__ import annotations

from datetime import datetime

from mgc_v05l.app import atp_companion_gc_asia_only_flat_regime_filter_feasibility as subject


def _trade(*, ts: str, pnl: float, gross: float, cost: float, regime: str = "TREND_UP", vol: str = "HOT") -> dict[str, object]:
    entry_ts = datetime.fromisoformat(ts)
    return {
        "trade_id": ts,
        "entry_ts": entry_ts,
        "entry_date": entry_ts.date().isoformat(),
        "entry_year": entry_ts.year,
        "entry_month": entry_ts.strftime("%Y-%m"),
        "entry_quarter": f"{entry_ts.year}-Q{((entry_ts.month - 1) // 3) + 1}",
        "session": "ASIA",
        "direction": "LONG",
        "exit_reason": "time_stop",
        "baseline_pnl_cash": pnl,
        "gross_pnl_cash": gross,
        "fees_paid": 1.5,
        "slippage_cost": cost - 1.5,
        "cost_cash": cost,
        "regime_bucket": regime,
        "volatility_bucket": vol,
        "quality_bucket": "HIGH",
    }


def test_rolling_net_filter_uses_only_prior_history() -> None:
    trades = [
        _trade(ts="2024-01-01T19:00:00-05:00", pnl=-100.0, gross=0.0, cost=10.0),
        _trade(ts="2024-01-02T19:00:00-05:00", pnl=-50.0, gross=0.0, cost=10.0),
        _trade(ts="2024-01-03T19:00:00-05:00", pnl=200.0, gross=260.0, cost=60.0),
    ]
    spec = {
        "predicate": subject._rolling_health_predicate("ROLL_NET_GT_0", 2),
    }

    taken, skipped, mask = subject._evaluate_active_filter(spec=spec, trades=trades)

    assert mask == (True, False, False)
    assert len(taken) == 1
    assert len(skipped) == 2


def test_known_bad_window_flag_requires_concentrated_skips() -> None:
    assert subject._effectively_skips_known_bad_window(
        {
            "skipped_peak_to_trough_pct": 80.0,
            "skipped_trough_to_recovery_pct": 10.0,
            "skipped_outside_episode_pct": 10.0,
        }
    )
    assert not subject._effectively_skips_known_bad_window(
        {
            "skipped_peak_to_trough_pct": 55.0,
            "skipped_trough_to_recovery_pct": 20.0,
            "skipped_outside_episode_pct": 25.0,
        }
    )


def test_classification_marks_promising_when_drawdown_falls_and_edge_is_retained() -> None:
    rows = [
        {
            "filter_id": "BASELINE_LONG_ONLY",
            "status": "ACTIVE",
            "drawdown_reduction_pct": 0.0,
            "net_pnl_retained_pct_vs_baseline": 100.0,
            "works_only_by_skipping_known_bad_window": "NO",
        },
        {
            "filter_id": "ROLL_NET_GT_0_40",
            "status": "ACTIVE",
            "drawdown_reduction_pct": 30.0,
            "net_pnl_retained_pct_vs_baseline": 95.0,
            "works_only_by_skipping_known_bad_window": "NO",
        },
    ]

    assert subject._classify(rows) == "FLAT_FILTER_PROMISING"
