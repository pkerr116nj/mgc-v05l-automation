from __future__ import annotations

from datetime import datetime

from mgc_v05l.app import atp_companion_gc_asia_only_directional_counterfactual as subject


def _trade(
    *,
    trade_id: str,
    entry_ts: str,
    pnl: float,
    exit_reason: str = "target",
    regime_bucket: str = "TREND_UP",
    volatility_bucket: str = "NORMAL",
) -> dict[str, object]:
    entry = datetime.fromisoformat(entry_ts)
    return {
        "trade_id": trade_id,
        "entry_ts": entry,
        "exit_ts": entry,
        "entry_date": entry.date().isoformat(),
        "entry_year": entry.year,
        "entry_month": entry.month,
        "entry_quarter": f"{entry.year}-Q{((entry.month - 1) // 3) + 1}",
        "entry_hour": entry.hour,
        "direction": "LONG",
        "exit_reason": exit_reason,
        "gross_pnl_cash": pnl + 51.5,
        "fees_paid": 1.5,
        "slippage_cost": 50.0,
        "trade_pnl_cash": pnl,
        "regime_bucket": regime_bucket,
        "volatility_bucket": volatility_bucket,
        "quality_bucket": "HIGH",
        "minute_path": [],
        "entry_price": 100.0,
        "exit_price": 99.0,
        "stop_price": 98.0,
        "target_price": 102.0,
        "initial_risk_points": 2.0,
        "r_unit_points": 2.0,
    }


def test_directional_counterfactual_blocks_when_minute_path_is_empty() -> None:
    trades = [
        _trade(trade_id="a", entry_ts="2024-01-01T19:00:00-05:00", pnl=-100.0, exit_reason="stop"),
        _trade(trade_id="b", entry_ts="2024-01-02T19:00:00-05:00", pnl=200.0, exit_reason="target"),
    ]

    rows = subject._directional_counterfactual_rows(trades)

    assert rows[0]["status"] == "BLOCKED"
    assert "minute_path" in rows[0]["block_reason"]


def test_flat_comparison_and_classification_reflect_blocked_short_path() -> None:
    trades = [
        _trade(trade_id="a", entry_ts="2024-01-01T19:00:00-05:00", pnl=-100.0, exit_reason="stop"),
        _trade(trade_id="b", entry_ts="2024-01-02T19:00:00-05:00", pnl=200.0, exit_reason="target"),
        _trade(trade_id="c", entry_ts="2025-04-22T19:00:00-04:00", pnl=300.0, exit_reason="target"),
    ]

    flat_rows = subject._flat_comparison_rows(trades)
    short_rows = subject._directional_counterfactual_rows(trades)
    classification = subject._classify(short_rows, flat_rows)

    by_id = {row["comparison_id"]: row for row in flat_rows}
    assert by_id["LONG_BASELINE_EPISODE_TOTAL"]["net_pnl_cash"] == 100.0
    assert by_id["FLAT_NO_TRADE_DURING_EPISODE"]["max_drawdown"] == 0.0
    assert by_id["LONG_OUTSIDE_EPISODE_ONLY"]["net_pnl_cash"] == 300.0
    assert classification == "DIRECTIONAL_COUNTERFACTUAL_BLOCKED"
