from __future__ import annotations

from datetime import datetime, timedelta

from mgc_v05l.app.atp_companion_asia_drift_enhancement_review import (
    _build_add_metrics,
    _build_core_metrics,
    _build_delta_rows,
    _classify_candidate,
    _overlay_asia_only_candidate_rows,
)
from mgc_v05l.research.trend_participation import ConflictOutcome
from mgc_v05l.research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
)
from mgc_v05l.research.trend_participation.models import ResearchBar, TradeRecord


def _trade(*, trade_id: str, session: str, entry_price: float, exit_price: float, pnl_cash: float) -> dict[str, object]:
    decision_ts = datetime(2026, 3, 10, 1, 0) if session == "ASIA" else datetime(2026, 3, 10, 14, 0)
    entry_ts = decision_ts + timedelta(minutes=1)
    exit_ts = entry_ts + timedelta(minutes=4)
    return {
        "trade_id": trade_id,
        "trade_record": TradeRecord(
            instrument="MGC",
            variant_id="trend_participation.atp_v1_long_pullback_continuation.long.base",
            family="atp_v1_long_pullback_continuation",
            side="LONG",
            live_eligible=True,
            shadow_only=False,
            conflict_outcome=ConflictOutcome.NO_CONFLICT,
            decision_id=f"decision-{trade_id}",
            decision_ts=decision_ts,
            entry_ts=entry_ts,
            exit_ts=exit_ts,
            entry_price=entry_price,
            exit_price=exit_price,
            stop_price=entry_price - 2.0,
            target_price=None,
            pnl_points=exit_price - entry_price,
            gross_pnl_cash=pnl_cash,
            pnl_cash=pnl_cash,
            fees_paid=1.5,
            slippage_cost=0.0,
            mfe_points=max(exit_price - entry_price, 0.0),
            mae_points=1.0,
            bars_held_1m=4,
            hold_minutes=4.0,
            exit_reason="trend_failure",
            is_reentry=False,
            reentry_type="NONE",
            stopout=False,
            setup_signature=f"setup-{trade_id}",
            setup_quality_bucket="HIGH",
            session_segment=session,
            regime_bucket="TREND",
            volatility_bucket="MEDIUM",
        ),
    }


def _bar(base_ts: datetime, minute_offset: int, *, open_: float, high: float, low: float, close: float, session: str) -> ResearchBar:
    ts = base_ts + timedelta(minutes=minute_offset)
    return ResearchBar(
        instrument="MGC",
        timeframe="1m",
        start_ts=ts - timedelta(minutes=1),
        end_ts=ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label=session,
        session_segment=session,
    )


def test_overlay_adds_only_on_asia_trades() -> None:
    candidate = next(
        item
        for item in default_atp_promotion_add_candidates()
        if item.candidate_id == "promotion_1_075r_favorable_only"
    )
    asia_row = _trade(trade_id="asia-1", session="ASIA", entry_price=100.0, exit_price=106.0, pnl_cash=58.5)
    us_row = _trade(trade_id="us-1", session="US", entry_price=100.0, exit_price=106.0, pnl_cash=58.5)
    asia_entry_ts = asia_row["trade_record"].entry_ts
    us_entry_ts = us_row["trade_record"].entry_ts
    rows = _overlay_asia_only_candidate_rows(
        trade_rows=[asia_row, us_row],
        trade_windows_by_id={
            "asia-1": [
                _bar(asia_entry_ts, 1, open_=100.9, high=101.4, low=100.4, close=101.0, session="ASIA"),
                _bar(asia_entry_ts, 2, open_=101.2, high=102.0, low=100.6, close=101.9, session="ASIA"),
                _bar(asia_entry_ts, 3, open_=102.0, high=102.4, low=101.8, close=102.2, session="ASIA"),
            ],
            "us-1": [
                _bar(us_entry_ts, 1, open_=100.9, high=101.4, low=100.4, close=101.0, session="US"),
                _bar(us_entry_ts, 2, open_=101.2, high=102.0, low=100.6, close=101.9, session="US"),
                _bar(us_entry_ts, 3, open_=102.0, high=102.4, low=101.8, close=102.2, session="US"),
            ],
        },
        candidate=candidate,
        point_value=10.0,
        eligible_sessions=("ASIA",),
    )

    asia_result = next(row for row in rows if row["trade_id"] == "asia-1")
    us_result = next(row for row in rows if row["trade_id"] == "us-1")
    assert asia_result["added"] is True
    assert asia_result["session_segment"] == "ASIA"
    assert us_result["added"] is False
    assert us_result["session_segment"] == "US"
    assert us_result["add_reason"] == "SESSION_NOT_ELIGIBLE"
    assert us_result["pnl_cash"] == us_result["trade_pnl_cash"]


def test_add_metrics_capture_incremental_effect_and_path_volatility() -> None:
    baseline_rows = [
        {"entry_ts": datetime(2026, 1, 1, 1), "decision_ts": datetime(2026, 1, 1, 1), "exit_ts": datetime(2026, 1, 1, 2), "pnl_cash": 10.0, "trade_pnl_cash": 10.0, "mfe_points": 2.0, "mae_points": 1.0, "hold_minutes": 10.0, "bars_held_1m": 10, "side": "LONG", "session_segment": "ASIA", "added": False},
        {"entry_ts": datetime(2026, 1, 2, 1), "decision_ts": datetime(2026, 1, 2, 1), "exit_ts": datetime(2026, 1, 2, 2), "pnl_cash": -5.0, "trade_pnl_cash": -5.0, "mfe_points": 1.0, "mae_points": 2.0, "hold_minutes": 10.0, "bars_held_1m": 10, "side": "LONG", "session_segment": "ASIA", "added": False},
    ]
    candidate_rows = [
        {**baseline_rows[0], "pnl_cash": 15.0, "added": True, "add_pnl_cash": 5.0},
        {**baseline_rows[1], "pnl_cash": -5.0, "added": False, "add_pnl_cash": 0.0},
    ]
    baseline_metrics = _build_core_metrics(baseline_rows, bar_count=100)
    candidate_metrics = _build_core_metrics(candidate_rows, bar_count=100)
    add_metrics = _build_add_metrics(
        rows=candidate_rows,
        baseline_rows=baseline_rows,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
    )

    assert add_metrics["add_count"] == 1
    assert add_metrics["add_success_rate_percent"] == 100.0
    assert add_metrics["incremental_pnl_from_adds"] == 5.0
    assert add_metrics["percent_adds_improved_trade"] == 100.0
    assert add_metrics["percent_adds_worsened_trade"] == 0.0


def test_classification_requires_more_than_raw_pnl_improvement() -> None:
    baseline_metrics = {
        "net_pnl_cash": 100.0,
        "average_trade_pnl_cash": 10.0,
        "profit_factor": 1.5,
        "max_drawdown": 20.0,
        "drawdown_to_profit_ratio": 0.2,
        "max_consecutive_losers": 3,
        "daily_path_volatility_cash": 12.0,
    }
    candidate_metrics = {
        "net_pnl_cash": 130.0,
        "average_trade_pnl_cash": 13.0,
        "profit_factor": 1.6,
        "max_drawdown": 18.0,
        "drawdown_to_profit_ratio": 0.1385,
        "max_consecutive_losers": 3,
        "daily_path_volatility_cash": 10.0,
    }
    add_metrics = {
        "add_count": 12,
        "incremental_pnl_from_adds": 30.0,
        "path_volatility_delta_cash": -2.0,
    }
    delta = _build_delta_rows(
        evaluation_id="mgc",
        label="MGC",
        baseline_metrics=baseline_metrics,
        candidate_metrics={**candidate_metrics, "median_trade_pnl_cash": 11.0, "largest_win_pnl_cash": 30.0, "largest_loss_pnl_cash": -10.0},
        add_metrics=add_metrics,
    )
    classification = _classify_candidate(
        trade_count=120,
        add_count=12,
        delta=delta,
        candidate_metrics=candidate_metrics,
        baseline_metrics=baseline_metrics,
        add_metrics=add_metrics,
    )

    assert classification == "ENHANCEMENT_PROMISING"
