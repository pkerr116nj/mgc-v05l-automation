"""Canonical performance metrics for strategy validation."""

from __future__ import annotations

from dataclasses import dataclass

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..data.normalization import normalize_strategy_backtest
from ..reporting.models import ValidationModuleResult
from ..robustness.drawdown import calculate_drawdown_profile
from ..utils.stats import safe_divide, safe_mean, safe_stdev


@dataclass(frozen=True)
class CanonicalPerformanceSnapshot:
    net_pnl: float
    profit_factor: float
    expectancy: float
    sharpe_like_ratio: float
    sortino_like_ratio: float
    hit_rate: float
    average_win: float
    average_loss: float
    max_drawdown: float
    ulcer_like_pain_metric: float
    recovery_factor: float
    exposure_time_in_market: float

    def to_dict(self) -> dict[str, float]:
        return {
            "net_pnl": self.net_pnl,
            "profit_factor": self.profit_factor,
            "expectancy": self.expectancy,
            "sharpe_like_ratio": self.sharpe_like_ratio,
            "sortino_like_ratio": self.sortino_like_ratio,
            "hit_rate": self.hit_rate,
            "average_win": self.average_win,
            "average_loss": self.average_loss,
            "max_drawdown": self.max_drawdown,
            "ulcer_like_pain_metric": self.ulcer_like_pain_metric,
            "recovery_factor": self.recovery_factor,
            "exposure_time_in_market": self.exposure_time_in_market,
        }


def calculate_canonical_metrics(strategy: StrategyBacktest, config: ValidationConfig) -> CanonicalPerformanceSnapshot:
    normalized = normalize_strategy_backtest(strategy)
    pnls = [trade.net_pnl for trade in normalized.trades]
    positives = [value for value in pnls if value > 0]
    negatives = [value for value in pnls if value < 0]
    total_positive = sum(positives)
    total_negative = abs(sum(negatives))
    expectancy = safe_mean(pnls)
    pnl_stdev = safe_stdev(pnls)
    sharpe = safe_divide(expectancy, pnl_stdev, default=0.0) * (len(pnls) ** 0.5 if pnls else 0.0)
    downside = [value for value in pnls if value < 0]
    downside_stdev = safe_stdev(downside)
    sortino = safe_divide(expectancy, downside_stdev, default=0.0) * (len(pnls) ** 0.5 if pnls else 0.0)
    drawdown = calculate_drawdown_profile(normalized, config)

    exposure = 0.0
    if len(normalized.position_series) > 1:
        active = sum(1 for point in normalized.position_series if abs(point.position) > 0)
        exposure = active / len(normalized.position_series)
    elif normalized.trades:
        total_holding = sum(max(0.0, trade.holding_minutes) for trade in normalized.trades)
        total_window = (
            normalized.equity_curve[-1].timestamp - normalized.equity_curve[0].timestamp
        ).total_seconds() / 60.0
        exposure = safe_divide(total_holding, total_window, default=0.0)

    net_pnl = sum(pnls)
    return CanonicalPerformanceSnapshot(
        net_pnl=net_pnl,
        profit_factor=float("inf") if total_positive > 0 and total_negative == 0 else safe_divide(total_positive, total_negative),
        expectancy=expectancy,
        sharpe_like_ratio=sharpe,
        sortino_like_ratio=sortino,
        hit_rate=safe_divide(len(positives), len(pnls), default=0.0),
        average_win=safe_mean(positives),
        average_loss=safe_mean(negatives),
        max_drawdown=drawdown.max_drawdown,
        ulcer_like_pain_metric=drawdown.ulcer_like_pain,
        recovery_factor=safe_divide(net_pnl, drawdown.max_drawdown, default=0.0),
        exposure_time_in_market=exposure,
    )


def run_canonical_performance_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    metrics = calculate_canonical_metrics(strategy, config)
    status = "pass"
    if metrics.expectancy <= 0 or metrics.profit_factor < 1.0:
        status = "warn"
    summary = (
        f"Canonical metrics show net PnL {metrics.net_pnl:.2f}, expectancy {metrics.expectancy:.2f}, "
        f"profit factor {metrics.profit_factor:.2f}."
    )
    recommendations: list[str] = []
    if status == "warn":
        recommendations.append("Improve expectancy quality before trusting headline returns.")
    return ValidationModuleResult(
        module_name="canonical_performance",
        status=status,
        summary=summary,
        metrics={key: round(value, config.report_precision) for key, value in metrics.to_dict().items()},
        diagnostics=metrics.to_dict(),
        artifacts={},
        recommendations=recommendations,
    )
