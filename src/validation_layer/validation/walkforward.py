"""Sequential walk-forward validation diagnostics."""

from __future__ import annotations

from dataclasses import dataclass

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..data.normalization import normalize_strategy_backtest, slice_strategy_backtest
from ..performance.metrics import calculate_canonical_metrics
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, linear_slope, safe_mean, safe_stdev


@dataclass(frozen=True)
class TimeSplit:
    split_id: str
    start_index: int
    end_index: int


def generate_walkforward_splits(total_points: int, config: ValidationConfig) -> tuple[TimeSplit, ...]:
    folds = max(1, config.split_config.walkforward_folds)
    fold_size = max(config.split_config.min_fold_size, total_points // folds if folds else total_points)
    splits: list[TimeSplit] = []
    start = 0
    for fold in range(folds):
        remaining = total_points - start
        if remaining < config.split_config.min_fold_size:
            break
        end = total_points - 1 if fold == folds - 1 else min(total_points - 1, start + fold_size - 1)
        splits.append(TimeSplit(split_id=f"wf_{fold + 1}", start_index=start, end_index=end))
        start = end + 1
        if start >= total_points:
            break
    return tuple(splits)


def run_walkforward_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    normalized = normalize_strategy_backtest(strategy)
    points = normalized.equity_curve
    splits = generate_walkforward_splits(len(points), config)
    fold_metrics: list[dict[str, float | int | str]] = []
    expectancies: list[float] = []
    fail_count = 0

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
            }
        )
        expectancies.append(metrics.expectancy)
        if metrics.expectancy <= 0 or metrics.profit_factor < 1.0:
            fail_count += 1

    fold_consistency_score = clamp01(1.0 / (1.0 + safe_stdev(expectancies)))
    degradation_slope = linear_slope(expectancies)
    temporal_degradation_score = clamp01(1.0 / (1.0 + max(0.0, -degradation_slope)))
    walkforward_score = clamp01(safe_mean([fold_consistency_score, temporal_degradation_score, 1.0 - fail_count / max(1, len(splits))]))

    status = "pass"
    if fail_count > config.thresholds.max_fold_failures:
        status = "fail"
    elif walkforward_score < config.thresholds.walkforward_min:
        status = "warn"

    summary = (
        f"Walk-forward score {walkforward_score:.2f} across {len(splits)} folds with {fail_count} fold failures "
        f"and degradation slope {degradation_slope:.3f}."
    )
    return ValidationModuleResult(
        module_name="walkforward",
        status=status,
        summary=summary,
        metrics={
            "walkforward_score": round(walkforward_score, config.report_precision),
            "fold_consistency_score": round(fold_consistency_score, config.report_precision),
            "temporal_degradation_score": round(temporal_degradation_score, config.report_precision),
            "fail_count": fail_count,
        },
        diagnostics={
            "fold_by_fold_metrics": fold_metrics,
            "degradation_slope": degradation_slope,
            "edge_stability_across_regimes": safe_mean([1.0 if metric["expectancy"] > 0 else 0.0 for metric in fold_metrics]),
            "fail_count_across_windows": fail_count,
        },
        artifacts={},
        recommendations=["Prefer strategies whose edge survives time progression instead of one lucky era."],
    )
