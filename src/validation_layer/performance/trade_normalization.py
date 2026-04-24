"""Trade normalization metrics to prevent misleading comparisons."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..performance.metrics import calculate_canonical_metrics
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_divide, safe_mean


def run_trade_normalization_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    trade_count = len(strategy.trades)
    total_net = sum(trade.net_pnl for trade in strategy.trades)
    total_bars = sum(max(0, trade.holding_bars) for trade in strategy.trades)
    total_minutes = sum(max(0.0, trade.holding_minutes) for trade in strategy.trades)
    total_hours = total_minutes / 60.0
    total_days = total_hours / 24.0
    total_qty = sum(abs(trade.qty) for trade in strategy.trades)
    margin = float(strategy.metadata.get("margin_per_contract", config.default_margin_per_contract))
    capital_committed = max(total_qty * margin, margin)
    canonical = calculate_canonical_metrics(strategy, config)

    pnl_per_trade = safe_divide(total_net, trade_count, default=0.0)
    pnl_per_bar = safe_divide(total_net, total_bars, default=0.0)
    pnl_per_hour = safe_divide(total_net, total_hours, default=0.0)
    pnl_per_day = safe_divide(total_net, total_days, default=0.0)
    pnl_per_margin_dollar = safe_divide(total_net, capital_committed, default=0.0)
    pnl_per_drawdown_unit = safe_divide(total_net, canonical.max_drawdown, default=0.0)
    expectancy_per_time_at_risk = safe_divide(canonical.expectancy, safe_divide(total_minutes, trade_count, default=0.0), default=0.0)
    turnover_adjusted_return = safe_divide(total_net, total_qty, default=0.0)
    capital_efficiency = clamp01(safe_mean([min(1.0, pnl_per_margin_dollar * 50.0), min(1.0, pnl_per_drawdown_unit / 2.0)]))
    time_efficiency = clamp01(safe_mean([min(1.0, max(0.0, pnl_per_hour)), min(1.0, max(0.0, expectancy_per_time_at_risk * 60.0))]))

    status = "pass"
    if capital_efficiency < config.thresholds.capital_efficiency_min:
        status = "fail"
    elif time_efficiency < config.thresholds.time_efficiency_min:
        status = "warn"

    normalization_summary = {
        "pnl_per_trade": pnl_per_trade,
        "pnl_per_bar": pnl_per_bar,
        "pnl_per_hour": pnl_per_hour,
        "pnl_per_day": pnl_per_day,
        "pnl_per_margin_dollar": pnl_per_margin_dollar,
        "pnl_per_drawdown_unit": pnl_per_drawdown_unit,
        "expectancy_per_trade": canonical.expectancy,
        "expectancy_per_time_at_risk": expectancy_per_time_at_risk,
        "turnover_adjusted_return": turnover_adjusted_return,
        "capital_efficiency": capital_efficiency,
    }
    summary = (
        f"Normalization shows {pnl_per_trade:.2f} per trade, {pnl_per_hour:.2f} per hour at risk, "
        f"capital efficiency {capital_efficiency:.2f}."
    )
    recommendations: list[str] = []
    if status != "pass":
        recommendations.append("Improve capital and time efficiency before moving toward probation.")
    return ValidationModuleResult(
        module_name="trade_normalization",
        status=status,
        summary=summary,
        metrics={
            **{key: round(value, config.report_precision) for key, value in normalization_summary.items()},
            "capital_efficiency_score": round(capital_efficiency, config.report_precision),
            "time_efficiency_score": round(time_efficiency, config.report_precision),
        },
        diagnostics={"normalization_summary": normalization_summary},
        artifacts={},
        recommendations=recommendations,
    )
