from __future__ import annotations

from datetime import datetime

from mgc_v05l.app.atp_companion_gc_drawdown_governance_feasibility import (
    GovernancePolicy,
    _annotate_summary_rows,
    _baseline_metrics,
    _classification,
    _policy_allows_resume,
    _simulate_policy,
)


def _trade(*, trade_id: str, entry_ts: str, pnl: float, session: str = "ASIA") -> dict[str, object]:
    entry = datetime.fromisoformat(entry_ts)
    exit_ts = entry
    return {
        "trade_id": trade_id,
        "instrument": "GC",
        "session": session,
        "direction": "LONG",
        "entry_ts": entry,
        "exit_ts": exit_ts,
        "entry_date": entry.date().isoformat(),
        "entry_year": entry.year,
        "entry_month": entry.month,
        "entry_iso_week": entry.isocalendar().week,
        "exit_reason": "target" if pnl > 0 else "stop",
        "trade_pnl_cash": pnl,
    }


def test_policy_allows_resume_respects_day_week_month_and_manual() -> None:
    breach = _trade(trade_id="a", entry_ts="2024-01-05T10:00:00-05:00", pnl=-3000.0)
    same_day = _trade(trade_id="b", entry_ts="2024-01-05T12:00:00-05:00", pnl=1000.0)
    next_day = _trade(trade_id="c", entry_ts="2024-01-06T10:00:00-05:00", pnl=1000.0)
    next_week = _trade(trade_id="d", entry_ts="2024-01-08T10:00:00-05:00", pnl=1000.0)
    next_month = _trade(trade_id="e", entry_ts="2024-02-01T10:00:00-05:00", pnl=1000.0)

    assert not _policy_allows_resume(policy="REST_OF_DAY", breach_trade=breach, candidate_trade=same_day)
    assert _policy_allows_resume(policy="REST_OF_DAY", breach_trade=breach, candidate_trade=next_day)
    assert not _policy_allows_resume(policy="REST_OF_WEEK", breach_trade=breach, candidate_trade=next_day)
    assert _policy_allows_resume(policy="REST_OF_WEEK", breach_trade=breach, candidate_trade=next_week)
    assert not _policy_allows_resume(policy="REST_OF_MONTH", breach_trade=breach, candidate_trade=next_week)
    assert _policy_allows_resume(policy="REST_OF_MONTH", breach_trade=breach, candidate_trade=next_month)
    assert not _policy_allows_resume(policy="MANUAL_REVIEW_ONLY", breach_trade=breach, candidate_trade=next_month)


def test_simulate_policy_skips_future_trades_after_breach_and_restarts_next_day() -> None:
    trades = [
        _trade(trade_id="t1", entry_ts="2024-01-02T09:30:00-05:00", pnl=1000.0),
        _trade(trade_id="t2", entry_ts="2024-01-02T10:00:00-05:00", pnl=-4000.0),
        _trade(trade_id="t3", entry_ts="2024-01-02T11:00:00-05:00", pnl=500.0),
        _trade(trade_id="t4", entry_ts="2024-01-03T09:30:00-05:00", pnl=700.0, session="US"),
    ]

    ledger, events, payload = _simulate_policy(trades, GovernancePolicy(cap_cash=2500.0, reset_policy="REST_OF_DAY"))

    assert len(events) == 2
    assert events[0]["skipped_trade_count"] == 1
    assert events[0]["skipped_net_pnl_cash"] == 500.0
    assert events[1]["skipped_trade_count"] == 0
    retained = [row for row in ledger if row["retained_or_skipped"] == "RETAINED"]
    skipped = [row for row in ledger if row["retained_or_skipped"] == "SKIPPED"]
    assert [row["trade_id"] for row in retained] == ["t1", "t2", "t4"]
    assert [row["trade_id"] for row in skipped] == ["t3"]
    assert payload["summary"]["trade_count_retained"] == 3
    assert payload["summary"]["asia_contribution_after_governance"] == -3000.0
    assert payload["summary"]["us_contribution_after_governance"] == 700.0


def test_classification_marks_current_use_too_large_when_only_high_caps_preserve_edge() -> None:
    summary_rows = [
        {
            "cap_cash": 5000.0,
            "retained_net_pnl_cash": 1000.0,
            "retained_net_pnl_vs_ungated_gc_pct": 20.0,
            "trade_count_retained_pct": 30.0,
            "drawdown_reduction_cash": 15000.0,
        },
        {
            "cap_cash": 10000.0,
            "retained_net_pnl_cash": 4000.0,
            "retained_net_pnl_vs_ungated_gc_pct": 30.0,
            "trade_count_retained_pct": 35.0,
            "drawdown_reduction_cash": 12000.0,
        },
        {
            "cap_cash": 15000.0,
            "retained_net_pnl_cash": 9000.0,
            "retained_net_pnl_vs_ungated_gc_pct": 60.0,
            "trade_count_retained_pct": 70.0,
            "drawdown_reduction_cash": 8000.0,
        },
    ]

    assert _classification(summary_rows) == "GC_DRAWDOWN_TOO_LARGE_FOR_CURRENT_USE"


def test_annotate_summary_rows_adds_retained_pct_and_survivability() -> None:
    baseline = _baseline_metrics(
        [
            _trade(trade_id="a", entry_ts="2024-01-01T09:30:00-05:00", pnl=10000.0),
            _trade(trade_id="b", entry_ts="2024-01-02T09:30:00-05:00", pnl=-2000.0),
        ]
    )
    rows = [
        {
            "retained_net_pnl_cash": 6000.0,
            "max_drawdown_after_governance": 4500.0,
            "trade_count_retained": 1,
        }
    ]

    _annotate_summary_rows(rows, baseline=baseline)

    assert rows[0]["retained_net_pnl_vs_ungated_gc_pct"] == 75.0
    assert rows[0]["psychological_survivability"] == "PRACTICALLY_TOLERABLE"
    assert rows[0]["expectancy_effect"] == "PRESERVED"
