from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from validation_layer.config.defaults import build_debug_config
from validation_layer.data.contracts import EquityPoint, PositionPoint, PriceBar, StrategyBacktest, TradeRecord
from validation_layer.layer1.producer_contract import build_trade_validation_metadata
from validation_layer.orchestration.pipeline import run_validation_pipeline
from validation_layer.reporting.json_report import render_json_report
from validation_layer.reporting.markdown_report import render_markdown_report
from validation_layer.robustness.bootstrap import run_bootstrap_module
from validation_layer.robustness.market_permutation import run_market_permutation_module
from validation_layer.robustness.monte_carlo import run_monte_carlo_module


def _ts(index: int) -> datetime:
    base = datetime(2026, 4, 6, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    return base + timedelta(minutes=5 * index)


def _build_strategy(
    *,
    name: str,
    net_pnls: list[float],
    gross_offset: float,
    slippage_cost: float,
    fee_cost: float,
    bar_capture_values: list[float] | None = None,
    active_market_moves: list[float] | None = None,
) -> StrategyBacktest:
    equity = 100_000.0
    trades = []
    curve = [EquityPoint(timestamp=_ts(0), equity=equity)]
    positions = [PositionPoint(timestamp=_ts(0), position=0.0)]
    close = 100.0
    bars = [
        PriceBar(
            timestamp=_ts(0),
            open=close,
            high=close + 0.1,
            low=close - 0.1,
            close=close,
            volume=100.0,
            session_label="us",
        )
    ]
    bar_capture_values = bar_capture_values or [abs(value) for value in net_pnls]
    for index, net_pnl in enumerate(net_pnls, start=1):
        entry = _ts(index * 2 - 1)
        exit_time = _ts(index * 2)
        gross_pnl = net_pnl + gross_offset
        direction = "long" if index % 2 else "short"
        inactive_move = 0.05 if index % 2 else -0.05
        close += inactive_move
        bars.append(
            PriceBar(
                timestamp=entry,
                open=close - inactive_move,
                high=max(close - inactive_move, close) + 0.1,
                low=min(close - inactive_move, close) - 0.1,
                close=close,
                volume=100.0 + index,
                session_label="us",
            )
        )
        if active_market_moves is None:
            active_move = float(bar_capture_values[index - 1])
            if direction == "short":
                active_move = -active_move
        else:
            active_move = float(active_market_moves[index - 1])
        prior_close = close
        close += active_move
        bars.append(
            PriceBar(
                timestamp=exit_time,
                open=prior_close,
                high=max(prior_close, close) + 0.1,
                low=min(prior_close, close) - 0.1,
                close=close,
                volume=120.0 + index,
                session_label="us",
            )
        )
        trades.append(
            TradeRecord(
                entry_time=entry,
                exit_time=exit_time,
                direction=direction,
                qty=1.0,
                gross_pnl=gross_pnl,
                net_pnl=net_pnl,
                mae=min(net_pnl - 0.5, -0.5),
                mfe=max(net_pnl + 0.5, 0.5),
                holding_bars=2,
                holding_minutes=10.0,
                session_label="us",
                validation_metadata=build_trade_validation_metadata(
                    trade_id=f"{name}:{index}",
                    signal_id=f"{name}:signal:{index}",
                    setup_family="synthetic_phase4",
                    setup_variant="base",
                    decision_time=entry - timedelta(minutes=1),
                    exit_reason="target" if net_pnl > 0 else "stop",
                    execution_model="paper_runtime",
                    fill_policy="current_candle_vwap",
                    slippage_cost=slippage_cost,
                    fee_cost=fee_cost,
                    tags={"phase4_test": True},
                ),
            )
        )
        positions.append(PositionPoint(timestamp=entry, position=1.0 if direction == "long" else -1.0))
        equity += net_pnl
        curve.append(EquityPoint(timestamp=exit_time, equity=equity))
        positions.append(PositionPoint(timestamp=exit_time, position=0.0))

    return StrategyBacktest(
        strategy_name=name,
        symbol="MGC",
        timeframe="5m",
        parameters={"variant": name},
        bar_data=tuple(bars),
        trades=tuple(trades),
        equity_curve=tuple(curve),
        position_series=tuple(positions),
        metadata={"margin_per_contract": 5000.0},
    )


def test_bootstrap_is_reproducible_and_intervals_are_ordered() -> None:
    config = build_debug_config()
    strategy = _build_strategy(
        name="bootstrap_case",
        net_pnls=[8.0, 9.0, 7.0, 10.0, 6.5, 8.5, 9.5, 7.5],
        gross_offset=0.5,
        slippage_cost=0.2,
        fee_cost=0.1,
    )

    first = run_bootstrap_module(strategy, config)
    second = run_bootstrap_module(strategy, config)

    assert first.metrics == second.metrics
    assert first.artifacts == second.artifacts
    assert first.diagnostics["reported_interval_methods"] == ["percentile", "basic"]
    expectancy = first.artifacts["bootstrap_percentile_intervals"]["expectancy"]
    drawdown = first.artifacts["bootstrap_basic_intervals"]["drawdown"]
    assert expectancy["lower"] <= expectancy["median"] <= expectancy["upper"]
    assert drawdown["lower"] <= drawdown["median"] <= drawdown["upper"]


def test_monte_carlo_penalizes_execution_fragility() -> None:
    config = build_debug_config()
    resilient = _build_strategy(
        name="resilient_case",
        net_pnls=[6.0, 5.5, 7.0, 4.5, 6.5, 5.0, 7.5, 6.0],
        gross_offset=1.0,
        slippage_cost=0.15,
        fee_cost=0.05,
    )
    fragile = _build_strategy(
        name="fragile_case",
        net_pnls=[0.45, 0.40, 0.35, 0.50, 0.30, 0.55, 0.25, 0.40],
        gross_offset=0.35,
        slippage_cost=0.20,
        fee_cost=0.10,
    )

    resilient_result = run_monte_carlo_module(resilient, config)
    fragile_result = run_monte_carlo_module(fragile, config)

    assert resilient_result.metrics["execution_sensitivity_score"] > fragile_result.metrics["execution_sensitivity_score"]


def test_market_permutation_prefers_real_edge_over_no_edge() -> None:
    config = build_debug_config()
    strong = _build_strategy(
        name="strong_edge",
        net_pnls=[7.0, 6.5, 8.0, 5.5, 7.5, 6.0, 8.5, 7.0, 6.8, 7.4],
        gross_offset=0.5,
        slippage_cost=0.1,
        fee_cost=0.05,
        bar_capture_values=[6.5, 6.0, 7.5, 5.2, 7.1, 5.8, 8.0, 6.6, 6.2, 7.0],
    )
    no_edge = _build_strategy(
        name="no_edge",
        net_pnls=[2.0, -2.0, 1.5, -1.5, 2.5, -2.5, 1.0, -1.0, 1.2, -1.2],
        gross_offset=0.2,
        slippage_cost=0.1,
        fee_cost=0.05,
        bar_capture_values=[1.5, 1.5, 1.2, 1.2, 1.0, 1.0, 0.8, 0.8, 0.7, 0.7],
        active_market_moves=[1.2, -1.2, -1.0, 1.0, 0.8, -0.8, -0.6, 0.6, 0.5, -0.5],
    )

    strong_result = run_market_permutation_module(strong, config)
    no_edge_result = run_market_permutation_module(no_edge, config)

    assert strong_result.metrics["market_permutation_score"] > no_edge_result.metrics["market_permutation_score"]
    assert strong_result.metrics["observed_capture_percentile"] > no_edge_result.metrics["observed_capture_percentile"]


def test_market_permutation_is_distinct_from_execution_path_fragility() -> None:
    config = build_debug_config()
    structurally_aligned_but_cost_fragile = _build_strategy(
        name="aligned_but_cost_fragile",
        net_pnls=[0.35, 0.30, 0.40, 0.25, 0.32, 0.28, 0.36, 0.31, 0.34, 0.29],
        gross_offset=0.12,
        slippage_cost=0.18,
        fee_cost=0.12,
        bar_capture_values=[4.0, 3.8, 4.2, 3.7, 4.1, 3.9, 4.3, 3.8, 4.0, 3.7],
    )

    market_result = run_market_permutation_module(structurally_aligned_but_cost_fragile, config)
    execution_result = run_monte_carlo_module(structurally_aligned_but_cost_fragile, config)

    assert market_result.metrics["market_permutation_score"] > 0.6
    assert execution_result.metrics["execution_sensitivity_score"] < 0.6


def test_phase4_modules_return_honest_insufficient_evidence_for_small_samples() -> None:
    config = build_debug_config()
    strategy = _build_strategy(
        name="thin_sample",
        net_pnls=[3.0, -1.0, 2.0],
        gross_offset=0.2,
        slippage_cost=0.1,
        fee_cost=0.05,
    )

    bootstrap = run_bootstrap_module(strategy, config)
    monte_carlo = run_monte_carlo_module(strategy, config)
    confidence = run_market_permutation_module(strategy, config)

    for result in (bootstrap, monte_carlo, confidence):
        assert result.status == "warn"
        assert result.metrics["insufficient_evidence"] is True


def test_pipeline_and_reports_include_phase4_modules() -> None:
    config = build_debug_config()
    strategy = _build_strategy(
        name="pipeline_phase4",
        net_pnls=[5.0, 4.5, 6.0, 5.5, 4.0, 6.5, 5.0, 4.5],
        gross_offset=0.4,
        slippage_cost=0.1,
        fee_cost=0.05,
    )

    report = run_validation_pipeline(strategy, config)
    module_names = {result.module_name for result in report.module_results}
    payload = json.loads(render_json_report(report))
    markdown = render_markdown_report(report)

    assert {"bootstrap", "monte_carlo", "market_permutation"}.issubset(module_names)
    json_module_names = {row["module_name"] for row in payload["module_results"]}
    assert {"bootstrap", "monte_carlo", "market_permutation"}.issubset(json_module_names)
    assert "### bootstrap" in markdown
    assert "### monte_carlo" in markdown
    assert "### market_permutation" in markdown
    assert "`reported_interval_methods`:" in markdown
