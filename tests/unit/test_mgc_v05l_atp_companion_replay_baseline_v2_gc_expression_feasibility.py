from __future__ import annotations

from mgc_v05l.app.atp_companion_replay_baseline_v2_gc_expression_feasibility import (
    GC_REPO_PROXY_SCENARIOS,
    MGC_SCENARIOS,
    _classify,
    _comparison_rows,
    _naive_10x_row,
    _scenario_summary_row,
    _scenario_trade_rows,
)


def _trade(*, instrument: str, trade_id: str, session: str, year: str, gross: float, fee: float, slippage: float) -> dict[str, object]:
    return {
        "trade_id": trade_id,
        "instrument": instrument,
        "session": session,
        "direction": "LONG",
        "year": year,
        "entry_ts": f"{year}-01-02T09:35:00-05:00",
        "exit_ts": f"{year}-01-02T10:05:00-05:00",
        "exit_reason": "target",
        "gross_pnl_cash": gross,
        "base_fee_cash": fee,
        "base_slippage_cash": slippage,
        "baseline_net_pnl_cash": gross - fee - slippage,
    }


def test_gc_repo_proxy_uses_gc_specific_fixed_fee_and_slippage() -> None:
    scenario = next(item for item in GC_REPO_PROXY_SCENARIOS if item.scenario_id == "GC_CURRENT_REPO_CONTRACT_COST_PROXY")
    trades = [_trade(instrument="GC", trade_id="gc-1", session="ASIA", year="2024", gross=200.0, fee=1.5, slippage=50.0)]

    scenario_rows = _scenario_trade_rows(trades, scenario)
    summary = _scenario_summary_row(scenario, scenario_rows)

    assert scenario_rows[0]["fees_cash"] == 4.5
    assert scenario_rows[0]["slippage_cash"] == 20.0
    assert scenario_rows[0]["net_pnl_cash"] == 175.5
    assert summary["cost_consumed_pct_of_gross_edge"] == 12.25


def test_naive_10x_reference_scales_mgc_current_row_only_as_reference() -> None:
    mgc_current = {
        "scenario_id": "MGC_CURRENT_V2_COST_MODEL",
        "scenario_label": "MGC current v2 cost model",
        "instrument_expression": "MGC",
        "scenario_note": "",
        "trade_count": 10,
        "total_gross_pnl_cash": 100.0,
        "total_fees_cash": 15.0,
        "total_slippage_cash": 25.0,
        "total_net_pnl_cash": 60.0,
        "average_net_trade_cash": 6.0,
        "median_net_trade_cash": 5.0,
        "win_rate": 60.0,
        "profit_factor": 1.2,
        "max_drawdown": 20.0,
        "largest_win": 15.0,
        "largest_loss": -8.0,
        "max_consecutive_losers": 2,
        "asia_contribution_cash": 35.0,
        "us_contribution_cash": 25.0,
        "cost_consumed_pct_of_gross_edge": 40.0,
        "breakeven_cost_per_trade": 10.0,
        "drawdown_to_profit_ratio": 0.3333,
        "net_pnl_per_unit_of_drawdown": 3.0,
        "edge_economically_meaningful": "YES",
    }

    scaled = _naive_10x_row(mgc_current)

    assert scaled["instrument_expression"] == "NAIVE_10X_MGC_PROXY"
    assert scaled["total_net_pnl_cash"] == 600.0
    assert scaled["max_drawdown"] == 200.0
    assert scaled["scenario_id"] == "NAIVE_10X_MGC_SCALING_REFERENCE_ONLY"


def test_classification_flags_cost_improvement_with_excessive_drawdown() -> None:
    mgc_current = {
        "scenario_id": "MGC_CURRENT_V2_COST_MODEL",
        "total_net_pnl_cash": 1000.0,
        "cost_consumed_pct_of_gross_edge": 80.0,
        "max_drawdown": 500.0,
        "drawdown_to_profit_ratio": 0.5,
        "profit_factor": 1.1,
    }
    gc_framework = {
        "scenario_id": "GC_CURRENT_FRAMEWORK_REPLAY_COST_MODEL",
        "total_net_pnl_cash": 100.0,
    }
    gc_current = {
        "scenario_id": "GC_CURRENT_REPO_CONTRACT_COST_PROXY",
        "total_net_pnl_cash": 1200.0,
        "cost_consumed_pct_of_gross_edge": 50.0,
        "max_drawdown": 5000.0,
        "drawdown_to_profit_ratio": 4.1667,
        "profit_factor": 1.12,
    }
    gc_conservative = {
        "scenario_id": "GC_CONSERVATIVE_LIVE_PROXY",
        "total_net_pnl_cash": 300.0,
    }

    classification = _classify([mgc_current, gc_framework, gc_current, gc_conservative])  # type: ignore[arg-type]

    assert classification == "GC_EXPRESSION_COST_IMPROVED_BUT_RISK_TOO_LARGE"


def test_comparison_rows_include_gc_vs_mgc_pairs() -> None:
    summary_rows = []
    for scenario in MGC_SCENARIOS:
        summary_rows.append(
            {
                "scenario_id": scenario.scenario_id,
                "scenario_label": scenario.label,
                "instrument_expression": "MGC",
                "total_net_pnl_cash": 10.0,
                "profit_factor": 1.1,
                "cost_consumed_pct_of_gross_edge": 80.0,
                "max_drawdown": 5.0,
                "drawdown_to_profit_ratio": 0.5,
                "net_pnl_per_unit_of_drawdown": 2.0,
                "breakeven_cost_per_trade": 7.0,
                "trade_count": 2,
                "total_gross_pnl_cash": 20.0,
                "total_fees_cash": 3.0,
                "total_slippage_cash": 7.0,
                "average_net_trade_cash": 5.0,
                "median_net_trade_cash": 5.0,
                "win_rate": 50.0,
                "largest_win": 10.0,
                "largest_loss": -5.0,
                "max_consecutive_losers": 1,
                "asia_contribution_cash": 6.0,
                "us_contribution_cash": 4.0,
                "edge_economically_meaningful": "YES",
                "scenario_note": "",
            }
        )
    summary_rows.extend(
        [
            {
                "scenario_id": "GC_GROSS_ZERO_COST",
                "scenario_label": "GC gross / zero cost",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 100.0,
                "profit_factor": 1.2,
                "cost_consumed_pct_of_gross_edge": 0.0,
                "max_drawdown": 20.0,
                "drawdown_to_profit_ratio": 0.2,
                "net_pnl_per_unit_of_drawdown": 5.0,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_CURRENT_FRAMEWORK_REPLAY_COST_MODEL",
                "scenario_label": "GC current framework replay cost model",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 30.0,
                "profit_factor": 1.05,
                "cost_consumed_pct_of_gross_edge": 70.0,
                "max_drawdown": 25.0,
                "drawdown_to_profit_ratio": 0.8333,
                "net_pnl_per_unit_of_drawdown": 1.2,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_CURRENT_REPO_CONTRACT_COST_PROXY",
                "scenario_label": "GC current repo contract-cost proxy",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 60.0,
                "profit_factor": 1.1,
                "cost_consumed_pct_of_gross_edge": 40.0,
                "max_drawdown": 50.0,
                "drawdown_to_profit_ratio": 0.8333,
                "net_pnl_per_unit_of_drawdown": 1.2,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_HALF_SLIPPAGE",
                "scenario_label": "GC half slippage",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 70.0,
                "profit_factor": 1.15,
                "cost_consumed_pct_of_gross_edge": 30.0,
                "max_drawdown": 45.0,
                "drawdown_to_profit_ratio": 0.6429,
                "net_pnl_per_unit_of_drawdown": 1.5556,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_DOUBLE_SLIPPAGE",
                "scenario_label": "GC double slippage",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 20.0,
                "profit_factor": 1.01,
                "cost_consumed_pct_of_gross_edge": 80.0,
                "max_drawdown": 60.0,
                "drawdown_to_profit_ratio": 3.0,
                "net_pnl_per_unit_of_drawdown": 0.3333,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_FEE_ONLY",
                "scenario_label": "GC fee-only",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 90.0,
                "profit_factor": 1.18,
                "cost_consumed_pct_of_gross_edge": 10.0,
                "max_drawdown": 22.0,
                "drawdown_to_profit_ratio": 0.2444,
                "net_pnl_per_unit_of_drawdown": 4.0909,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_SLIPPAGE_ONLY",
                "scenario_label": "GC slippage-only",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 80.0,
                "profit_factor": 1.16,
                "cost_consumed_pct_of_gross_edge": 20.0,
                "max_drawdown": 30.0,
                "drawdown_to_profit_ratio": 0.375,
                "net_pnl_per_unit_of_drawdown": 2.6667,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
            {
                "scenario_id": "GC_CONSERVATIVE_LIVE_PROXY",
                "scenario_label": "GC conservative live proxy",
                "instrument_expression": "GC",
                "total_net_pnl_cash": 10.0,
                "profit_factor": 1.0,
                "cost_consumed_pct_of_gross_edge": 90.0,
                "max_drawdown": 70.0,
                "drawdown_to_profit_ratio": 7.0,
                "net_pnl_per_unit_of_drawdown": 0.1429,
                "breakeven_cost_per_trade": 20.0,
                "trade_count": 2,
            },
        ]
    )
    summary_rows.append(_naive_10x_row(next(row for row in summary_rows if row["scenario_id"] == "MGC_CURRENT_V2_COST_MODEL")))

    comparison_rows = _comparison_rows(summary_rows)

    assert any(row["comparison_id"] == "gc_proxy_current_vs_mgc_current" for row in comparison_rows)
    assert any(row["comparison_id"] == "naive_10x_reference" for row in comparison_rows)
