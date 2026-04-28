from __future__ import annotations

from datetime import datetime

from mgc_v05l.app import atp_companion_gc_asia_flat_filter_state_machine_validation as subject


def _trade(*, date: str, pnl: float, gross: float | None = None, cost: float = 50.0) -> dict[str, object]:
    entry_ts = datetime.fromisoformat(f"{date}T19:00:00-05:00")
    exit_ts = datetime.fromisoformat(f"{date}T20:00:00-05:00")
    gross_value = pnl + cost if gross is None else gross
    return {
        "trade_id": date,
        "entry_ts": entry_ts,
        "exit_ts": exit_ts,
        "entry_date": entry_ts.date().isoformat(),
        "entry_year": entry_ts.year,
        "entry_month": entry_ts.strftime("%Y-%m"),
        "session": "ASIA",
        "direction": "LONG",
        "exit_reason": "time_stop",
        "baseline_pnl_cash": pnl,
        "gross_pnl_cash": gross_value,
        "fees_paid": 1.5,
        "slippage_cost": cost - 1.5,
        "cost_cash": cost,
    }


def test_shadow_ledger_reactivates_from_skipped_trade_outcomes() -> None:
    trades = [
        _trade(date="2024-01-01", pnl=-100.0, gross=0.0),
        _trade(date="2024-01-02", pnl=-100.0, gross=0.0),
        _trade(date="2024-01-03", pnl=400.0, gross=450.0),
        _trade(date="2024-01-04", pnl=400.0, gross=450.0),
    ]
    baseline = subject._summary_metrics(trades)

    summary, transitions, shadow_rows = subject._simulate_shadow_machine(
        filter_id="ROLL_NET_GT_0_40",
        trades=trades,
        baseline_metrics=baseline,
    )

    assert summary["trades_executed"] == 2
    assert summary["trades_skipped"] == 2
    assert any(row["prior_state"] == "OFF" and row["next_state"] == "ON" for row in transitions)
    assert any(row["executed_trade"] == "NO" for row in shadow_rows)


def test_executed_only_gets_stuck_off_after_first_off_trigger() -> None:
    trades = [
        _trade(date="2024-01-01", pnl=-100.0, gross=0.0),
        _trade(date="2024-01-02", pnl=400.0, gross=450.0),
        _trade(date="2024-01-03", pnl=400.0, gross=450.0),
    ]
    baseline = subject._summary_metrics(trades)

    summary, transitions = subject._simulate_executed_only_machine(
        filter_id="ROLL_NET_GT_0_40",
        trades=trades,
        baseline_metrics=baseline,
    )

    assert summary["reactivation_live_safe"] == "NO"
    assert summary["stuck_off"] == "YES"
    assert summary["trades_skipped"] == 2
    assert len(transitions) == 1


def test_cooldown_reactivates_after_fixed_number_of_sessions() -> None:
    trades = [
        _trade(date="2024-01-01", pnl=-100.0, gross=0.0),
        _trade(date="2024-01-02", pnl=200.0, gross=250.0),
        _trade(date="2024-01-03", pnl=200.0, gross=250.0),
        _trade(date="2024-01-04", pnl=200.0, gross=250.0),
    ]
    baseline = subject._summary_metrics(trades)

    summary, transitions = subject._simulate_cooldown_machine(
        filter_id="ROLL_NET_GT_0_40",
        cooldown_sessions=2,
        trades=trades,
        baseline_metrics=baseline,
    )

    assert summary["reactivation_live_safe"] == "YES"
    assert any(row["prior_state"] == "OFF" and row["next_state"] == "ON" for row in transitions)
