from __future__ import annotations

import csv
import json
from pathlib import Path

from mgc_v05l.app import atp_companion_gc_asia_only_feasibility as subject


def _write_gc_trades(path: Path) -> None:
    rows = [
        {
            "trade_id": "asia-1",
            "instrument": "GC",
            "session": "ASIA",
            "direction": "LONG",
            "exit_reason": "target",
            "baseline_pnl_cash": 1200.0,
            "trade_record": {
                "instrument": "GC",
                "session_segment": "ASIA",
                "side": "LONG",
                "entry_ts": "2024-01-02 19:00:00-05:00",
                "exit_ts": "2024-01-02 19:30:00-05:00",
                "gross_pnl_cash": 1251.5,
                "fees_paid": 1.5,
                "slippage_cost": 50.0,
                "pnl_cash": 1200.0,
                "exit_reason": "target",
            },
        },
        {
            "trade_id": "asia-2",
            "instrument": "GC",
            "session": "ASIA",
            "direction": "LONG",
            "exit_reason": "stop",
            "baseline_pnl_cash": -400.0,
            "trade_record": {
                "instrument": "GC",
                "session_segment": "ASIA",
                "side": "LONG",
                "entry_ts": "2025-01-03 19:00:00-05:00",
                "exit_ts": "2025-01-03 19:20:00-05:00",
                "gross_pnl_cash": -348.5,
                "fees_paid": 1.5,
                "slippage_cost": 50.0,
                "pnl_cash": -400.0,
                "exit_reason": "stop",
            },
        },
        {
            "trade_id": "us-1",
            "instrument": "GC",
            "session": "US",
            "direction": "LONG",
            "exit_reason": "target",
            "baseline_pnl_cash": 100.0,
            "trade_record": {
                "instrument": "GC",
                "session_segment": "US",
                "side": "LONG",
                "entry_ts": "2025-01-04 09:45:00-05:00",
                "exit_ts": "2025-01-04 10:15:00-05:00",
                "gross_pnl_cash": 151.5,
                "fees_paid": 1.5,
                "slippage_cost": 50.0,
                "pnl_cash": 100.0,
                "exit_reason": "target",
            },
        },
    ]
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def _write_mgc_context(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "scenario_id",
                "trade_count",
                "total_gross_pnl_cash",
                "total_fees_cash",
                "total_slippage_cash",
                "total_net_pnl_cash",
                "average_net_trade_cash",
                "median_net_trade_cash",
                "win_rate",
                "profit_factor",
                "max_drawdown",
                "largest_win",
                "largest_loss",
                "max_consecutive_losers",
                "asia_contribution_cash",
                "us_contribution_cash",
                "cost_consumed_pct_of_gross_edge",
                "drawdown_to_profit_ratio",
                "net_pnl_per_unit_of_drawdown",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "scenario_id": "CURRENT_V2_COST_MODEL",
                "trade_count": 10,
                "total_gross_pnl_cash": 5000.0,
                "total_fees_cash": 15.0,
                "total_slippage_cash": 50.0,
                "total_net_pnl_cash": 4935.0,
                "average_net_trade_cash": 493.5,
                "median_net_trade_cash": 200.0,
                "win_rate": 60.0,
                "profit_factor": 1.25,
                "max_drawdown": 1000.0,
                "largest_win": 800.0,
                "largest_loss": -400.0,
                "max_consecutive_losers": 2,
                "asia_contribution_cash": 3500.0,
                "us_contribution_cash": 1435.0,
                "cost_consumed_pct_of_gross_edge": 1.3,
                "drawdown_to_profit_ratio": 0.2026,
                "net_pnl_per_unit_of_drawdown": 4.935,
            }
        )


def test_build_gc_asia_only_feasibility_filters_us_and_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    trades_path = tmp_path / "gc_trades.jsonl"
    mgc_path = tmp_path / "mgc_summary.csv"
    output_dir = tmp_path / "out"
    _write_gc_trades(trades_path)
    _write_mgc_context(mgc_path)

    monkeypatch.setattr(subject, "CAP_GRID", (2500.0,))
    monkeypatch.setattr(subject, "RESET_POLICIES", ("REST_OF_DAY",))

    artifacts = subject.build_gc_asia_only_feasibility(
        gc_trades_jsonl=trades_path,
        mgc_cost_summary_csv=mgc_path,
        output_dir=output_dir,
    )

    assert artifacts["feasibility_summary_csv"].exists()
    assert artifacts["comparison_csv"].exists()
    assert artifacts["year_csv"].exists()
    assert artifacts["governance_csv"].exists()
    assert artifacts["summary_md_path"].exists()

    with artifacts["feasibility_summary_csv"].open() as handle:
        summary_rows = list(csv.DictReader(handle))
    assert summary_rows[0]["scope_id"] == "GC_ASIA_ONLY"
    assert summary_rows[0]["trade_count"] == "2"
    assert summary_rows[0]["net_pnl_cash"] == "800.0"
    assert summary_rows[0]["classification"] == "GC_ASIA_ONLY_REDUCES_EDGE_TOO_MUCH"

    with artifacts["comparison_csv"].open() as handle:
        comparison_rows = list(csv.DictReader(handle))
    asia_plus_us = next(row for row in comparison_rows if row["scope_id"] == "GC_ASIA_PLUS_US")
    asia_only = next(row for row in comparison_rows if row["scope_id"] == "GC_ASIA_ONLY")
    assert asia_plus_us["net_pnl_cash"] == "900.0"
    assert asia_only["us_contribution_cash"] in {"0", "0.0"}
    assert asia_only["net_pnl_pct_vs_gc_asia_us"] == "88.8889"


def test_classify_marks_improves_risk_but_still_too_large_when_fivek_is_not_tolerable() -> None:
    asia_only_summary = {
        "net_pnl_cash": 50000.0,
        "max_drawdown": 9000.0,
        "drawdown_to_profit_ratio": 0.18,
    }
    asia_us_summary = {
        "net_pnl_cash": 60000.0,
        "max_drawdown": 22000.0,
    }
    governance_rows = [
        {
            "cap_cash": 5000.0,
            "retained_net_pnl_cash": 15000.0,
            "retained_net_pnl_vs_ungated_gc_pct": 30.0,
            "max_drawdown_after_governance": 12000.0,
            "drawdown_reduction_cash": 6000.0,
        },
        {
            "cap_cash": 10000.0,
            "retained_net_pnl_cash": 35000.0,
            "retained_net_pnl_vs_ungated_gc_pct": 70.0,
            "max_drawdown_after_governance": 13000.0,
            "drawdown_reduction_cash": 7000.0,
        },
    ]

    assert (
        subject._classify(
            asia_only_summary=asia_only_summary,
            asia_us_summary=asia_us_summary,
            governance_rows=governance_rows,
        )
        == "GC_ASIA_ONLY_IMPROVES_RISK_BUT_STILL_TOO_LARGE"
    )
