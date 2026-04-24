"""Monte Carlo perturbation tests for path and execution fragility."""

from __future__ import annotations

from dataclasses import replace

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest, TradeRecord
from ..performance.metrics import calculate_canonical_metrics
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, percentile, safe_mean
from ._helpers import (
    build_pnl_override_trade,
    ensure_robustness_prerequisites,
    evaluate_trade_variant,
    make_rng,
    retention_score,
    worst_case_retention,
)


def run_monte_carlo_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    insufficient = ensure_robustness_prerequisites(
        strategy,
        config,
        module_name="monte_carlo",
        score_fields=("path_fragility_score", "execution_sensitivity_score"),
    )
    if insufficient is not None:
        return insufficient

    observed = calculate_canonical_metrics(strategy, config)
    trades = tuple(strategy.trades)
    rng = make_rng(config, salt=211)

    shuffled_drawdowns: list[float] = []
    slippage_expectancies: list[float] = []
    cost_expectancies: list[float] = []
    skipped_expectancies: list[float] = []
    degraded_expectancies: list[float] = []

    for index in range(config.robustness.monte_carlo_iterations):
        shuffled = list(trades)
        rng.shuffle(shuffled)
        shuffled_snapshot = evaluate_trade_variant(strategy, tuple(shuffled), suffix=f"mc_shuffle_{index}", config=config)
        shuffled_drawdowns.append(shuffled_snapshot.max_drawdown)

        slippage_snapshot = evaluate_trade_variant(
            strategy,
            _apply_slippage_stress(trades, rng=rng),
            suffix=f"mc_slippage_{index}",
            config=config,
        )
        slippage_expectancies.append(slippage_snapshot.expectancy)

        cost_snapshot = evaluate_trade_variant(
            strategy,
            _apply_fee_stress(trades, rng=rng),
            suffix=f"mc_cost_{index}",
            config=config,
        )
        cost_expectancies.append(cost_snapshot.expectancy)

        skipped_snapshot = evaluate_trade_variant(
            strategy,
            _apply_skipped_trade_stress(trades, rng=rng),
            suffix=f"mc_skipped_{index}",
            config=config,
        )
        skipped_expectancies.append(skipped_snapshot.expectancy)

        degraded_snapshot = evaluate_trade_variant(
            strategy,
            _apply_degraded_fill_stress(trades, rng=rng),
            suffix=f"mc_degraded_{index}",
            config=config,
        )
        degraded_expectancies.append(degraded_snapshot.expectancy)

    shuffled_median_drawdown = percentile(shuffled_drawdowns, 0.50)
    shuffled_tail_drawdown = percentile(shuffled_drawdowns, 0.95)
    drawdown_worsening = max(0.0, shuffled_tail_drawdown - observed.max_drawdown)
    path_fragility_score = clamp01(1.0 - drawdown_worsening / (observed.max_drawdown + 1.0))

    scenario_retentions = [
        retention_score(observed.expectancy, slippage_expectancies),
        retention_score(observed.expectancy, cost_expectancies),
        retention_score(observed.expectancy, skipped_expectancies),
        retention_score(observed.expectancy, degraded_expectancies),
    ]
    worst_case_expectancy_retention = min(
        worst_case_retention(observed.expectancy, slippage_expectancies),
        worst_case_retention(observed.expectancy, cost_expectancies),
        worst_case_retention(observed.expectancy, skipped_expectancies),
        worst_case_retention(observed.expectancy, degraded_expectancies),
    )
    execution_sensitivity_score = clamp01(safe_mean(scenario_retentions + [worst_case_expectancy_retention]))

    status = "pass"
    if path_fragility_score < 0.35 or execution_sensitivity_score < 0.35:
        status = "fail"
    elif path_fragility_score < 0.60 or execution_sensitivity_score < 0.60:
        status = "warn"

    summary = (
        f"Monte Carlo path fragility score {path_fragility_score:.2f}; execution sensitivity score "
        f"{execution_sensitivity_score:.2f} under slippage, cost, skipped-trade, and degraded-fill stress."
    )
    return ValidationModuleResult(
        module_name="monte_carlo",
        status=status,
        summary=summary,
        metrics={
            "path_fragility_score": round(path_fragility_score, config.report_precision),
            "execution_sensitivity_score": round(execution_sensitivity_score, config.report_precision),
            "worst_case_expectancy_retention": round(worst_case_expectancy_retention, config.report_precision),
            "monte_carlo_iteration_count": config.robustness.monte_carlo_iterations,
        },
        diagnostics={
            "observed_expectancy": observed.expectancy,
            "observed_max_drawdown": observed.max_drawdown,
            "scenario_expectancy_retentions": {
                "slippage": scenario_retentions[0],
                "cost": scenario_retentions[1],
                "skipped_trades": scenario_retentions[2],
                "degraded_fill": scenario_retentions[3],
            },
        },
        artifacts={
            "randomized_trade_order_stress": {
                "median_drawdown": shuffled_median_drawdown,
                "tail_drawdown": shuffled_tail_drawdown,
            },
            "slippage_perturbation_stress": _stress_summary(slippage_expectancies),
            "cost_perturbation_stress": _stress_summary(cost_expectancies),
            "skipped_trade_stress": _stress_summary(skipped_expectancies),
            "degraded_fill_stress": _stress_summary(degraded_expectancies),
        },
        recommendations=[
            "Treat a strategy as fragile if small execution insults erase the apparent edge."
        ],
    )


def _apply_slippage_stress(trades: tuple[TradeRecord, ...], *, rng) -> tuple[TradeRecord, ...]:
    stressed: list[TradeRecord] = []
    for trade in trades:
        metadata = trade.validation_metadata
        explicit = abs(float(metadata.slippage_cost)) if metadata and metadata.slippage_cost is not None else 0.0
        implicit = max(0.0, trade.gross_pnl - trade.net_pnl)
        baseline = explicit or implicit or max(0.05, abs(trade.gross_pnl) * 0.02)
        extra = baseline * (1.0 + rng.uniform(0.25, 1.50))
        stressed.append(build_pnl_override_trade(trade, net_pnl=trade.net_pnl - extra, gross_pnl=trade.gross_pnl))
    return tuple(stressed)


def _apply_fee_stress(trades: tuple[TradeRecord, ...], *, rng) -> tuple[TradeRecord, ...]:
    stressed: list[TradeRecord] = []
    for trade in trades:
        metadata = trade.validation_metadata
        explicit = abs(float(metadata.fee_cost)) if metadata and metadata.fee_cost is not None else 0.0
        baseline = explicit or 0.25
        extra = baseline * (1.0 + rng.uniform(0.10, 1.00))
        stressed.append(build_pnl_override_trade(trade, net_pnl=trade.net_pnl - extra, gross_pnl=trade.gross_pnl))
    return tuple(stressed)


def _apply_skipped_trade_stress(trades: tuple[TradeRecord, ...], *, rng) -> tuple[TradeRecord, ...]:
    kept: list[TradeRecord] = []
    for trade in trades:
        if rng.random() < 0.15:
            continue
        kept.append(trade)
    if not kept:
        kept.append(min(trades, key=lambda item: item.net_pnl))
    return tuple(kept)


def _apply_degraded_fill_stress(trades: tuple[TradeRecord, ...], *, rng) -> tuple[TradeRecord, ...]:
    stressed: list[TradeRecord] = []
    for trade in trades:
        if trade.net_pnl >= 0:
            degraded_net = trade.net_pnl * (1.0 - rng.uniform(0.05, 0.25))
        else:
            degraded_net = trade.net_pnl * (1.0 + rng.uniform(0.05, 0.25))
        stressed.append(
            replace(
                build_pnl_override_trade(trade, net_pnl=degraded_net, gross_pnl=trade.gross_pnl),
                metadata={**trade.metadata, "degraded_fill_applied": True},
            )
        )
    return tuple(stressed)


def _stress_summary(values: list[float]) -> dict[str, float]:
    return {
        "lower_expectancy": percentile(values, 0.05),
        "median_expectancy": percentile(values, 0.50),
        "upper_expectancy": percentile(values, 0.95),
    }
