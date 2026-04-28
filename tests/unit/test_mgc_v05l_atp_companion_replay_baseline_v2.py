from __future__ import annotations

from datetime import datetime

from mgc_v05l.app.atp_companion_replay_baseline_v2 import (
    _classification,
    _summary_from_trade_rows,
    _validation_rows,
)
from mgc_v05l.research.trend_participation.models import ConflictOutcome, TradeRecord


def _trade(*, ts: str, pnl_cash: float, session: str, side: str = "LONG") -> TradeRecord:
    decision_ts = datetime.fromisoformat(ts)
    return TradeRecord(
        instrument="MGC",
        variant_id="variant",
        family="family",
        side=side,
        live_eligible=False,
        shadow_only=True,
        conflict_outcome=ConflictOutcome.NO_CONFLICT,
        decision_id=f"id-{ts}",
        decision_ts=decision_ts,
        entry_ts=decision_ts,
        exit_ts=decision_ts,
        entry_price=100.0,
        exit_price=101.0,
        stop_price=99.0,
        target_price=102.0,
        pnl_points=pnl_cash / 10.0,
        gross_pnl_cash=pnl_cash + 1.5,
        pnl_cash=pnl_cash,
        fees_paid=1.5,
        slippage_cost=0.0,
        mfe_points=1.0,
        mae_points=0.5,
        bars_held_1m=5,
        hold_minutes=5.0,
        exit_reason="target",
        is_reentry=False,
        reentry_type="NONE",
        stopout=False,
        setup_signature="sig",
        setup_quality_bucket="HIGH",
        session_segment=session,
        regime_bucket="ROTATION",
        volatility_bucket="NORMAL",
    )


def test_summary_from_trade_rows_includes_session_contributions_and_streaks() -> None:
    rows = [
        {"trade_id": "a", "trade_record": _trade(ts="2024-01-01T19:00:00-05:00", pnl_cash=10.0, session="ASIA")},
        {"trade_id": "b", "trade_record": _trade(ts="2024-01-01T20:00:00-05:00", pnl_cash=-5.0, session="ASIA")},
        {"trade_id": "c", "trade_record": _trade(ts="2024-01-02T09:35:00-05:00", pnl_cash=20.0, session="US")},
    ]
    summary = _summary_from_trade_rows(rows, bar_count=100)

    assert summary["trade_count"] == 3
    assert summary["net_pnl_cash"] == 25.0
    assert summary["asia_net_pnl_cash"] == 5.0
    assert summary["us_net_pnl_cash"] == 20.0
    assert summary["largest_win"] == 20.0
    assert summary["largest_loss"] == -5.0
    assert summary["max_consecutive_losers"] == 1


def test_validation_rows_classifies_built_when_expected_matches_observed() -> None:
    expected = {
        "trade_count": 10,
        "net_pnl_cash": 100.0,
        "average_trade_pnl_cash": 10.0,
        "median_trade_pnl_cash": 8.0,
        "win_rate": 60.0,
        "profit_factor": 1.5,
        "max_drawdown": 20.0,
        "largest_win": 30.0,
        "largest_loss": -10.0,
        "max_consecutive_losers": 2,
        "asia_net_pnl_cash": 40.0,
        "us_net_pnl_cash": 60.0,
    }
    rows = _validation_rows(expected, dict(expected))

    assert all(row["passed"] for row in rows)
    assert _classification(rows) == "REPLAY_BASELINE_V2_BUILT"
