"""Alternative split validation diagnostics."""

from __future__ import annotations

from dataclasses import dataclass

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..data.normalization import normalize_strategy_backtest, slice_strategy_backtest
from ..performance.metrics import calculate_canonical_metrics
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_mean, safe_stdev


@dataclass(frozen=True)
class FoldSplit:
    split_id: str
    start_index: int
    end_index: int


def generate_cross_validation_splits(total_points: int, config: ValidationConfig) -> tuple[FoldSplit, ...]:
    folds = max(1, config.split_config.cross_validation_folds)
    base = max(config.split_config.min_fold_size, total_points // folds if folds else total_points)
    splits: list[FoldSplit] = []
    start = 0
    for fold in range(folds):
        remaining = total_points - start
        if remaining < config.split_config.min_fold_size:
            break
        end = total_points - 1 if fold == folds - 1 else min(total_points - 1, start + base - 1)
        splits.append(FoldSplit(split_id=f"cv_{fold + 1}", start_index=start, end_index=end))
        start = end + 1
    return tuple(splits)


def run_cross_validation_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    normalized = normalize_strategy_backtest(strategy)
    points = normalized.equity_curve
    splits = generate_cross_validation_splits(len(points), config)
    fold_metrics: list[dict[str, float | int | str]] = []
    expectancies: list[float] = []
    profit_factors: list[float] = []

    for split in splits:
        start = points[split.start_index].timestamp
        end = points[split.end_index].timestamp
        sliced = slice_strategy_backtest(normalized, start, end, split.split_id)
        metrics = calculate_canonical_metrics(sliced, config)
        fold_metrics.append(
            {
                "split_id": split.split_id,
                "trade_count": len(sliced.trades),
                "expectancy": metrics.expectancy,
                "profit_factor": metrics.profit_factor,
                "net_pnl": metrics.net_pnl,
                "parameter_consistency": 1.0,
            }
        )
        expectancies.append(metrics.expectancy)
        profit_factors.append(metrics.profit_factor if metrics.profit_factor != float("inf") else 10.0)

    fold_dispersion_score = clamp01(1.0 / (1.0 + safe_stdev(expectancies)))
    score = clamp01(safe_mean([fold_dispersion_score, 1.0 / (1.0 + safe_stdev(profit_factors))]))
    status = "pass"
    if score < config.thresholds.cross_validation_min:
        status = "warn"
    summary = f"Cross-validation score {score:.2f} with expectancy dispersion {safe_stdev(expectancies):.3f}."
    return ValidationModuleResult(
        module_name="cross_validation",
        status=status,
        summary=summary,
        metrics={
            "cross_validation_score": round(score, config.report_precision),
            "fold_dispersion_score": round(fold_dispersion_score, config.report_precision),
        },
        diagnostics={
            "split_level_summary": fold_metrics,
            "fold_variance": safe_stdev(expectancies) ** 2 if len(expectancies) > 1 else 0.0,
            "metric_dispersion": {"expectancy": safe_stdev(expectancies), "profit_factor": safe_stdev(profit_factors)},
            "parameter_consistency_across_folds": 1.0,
        },
        artifacts={},
        recommendations=["Check whether edge persists under different split families, not just walk-forward."],
    )
