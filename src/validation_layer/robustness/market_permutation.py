"""Market-level permutation significance using bar-sequence randomization."""

from __future__ import annotations

from bisect import bisect_right

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..data.normalization import normalize_strategy_backtest
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, percentile, safe_mean, safe_stdev
from ._helpers import make_rng, percentile_rank, robustness_insufficient_evidence_result


def run_market_permutation_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    normalized = normalize_strategy_backtest(strategy)
    if len(normalized.bar_data) < max(config.thresholds.minimum_feature_length, 20):
        return robustness_insufficient_evidence_result(
            module_name="market_permutation",
            summary="Insufficient evidence: market-level permutation needs a broader bar history window.",
            score_fields=("market_permutation_score", "observed_capture_percentile"),
            diagnostics={"bar_count": len(normalized.bar_data)},
            recommendations=["Provide enough bar coverage to evaluate the strategy against randomized market sequences."],
        )
    if len(normalized.position_series) < 2:
        return robustness_insufficient_evidence_result(
            module_name="market_permutation",
            summary="Insufficient evidence: market-level permutation needs position lineage aligned to bars.",
            score_fields=("market_permutation_score", "observed_capture_percentile"),
        )

    exposures, deltas = _bar_exposures_and_deltas(normalized)
    active_count = sum(1 for exposure in exposures if abs(exposure) > 0)
    if active_count < config.thresholds.minimum_trade_count:
        return robustness_insufficient_evidence_result(
            module_name="market_permutation",
            summary="Insufficient evidence: too few active exposure bars overlap the available market window.",
            score_fields=("market_permutation_score", "observed_capture_percentile"),
            diagnostics={
                "active_exposure_bars": active_count,
                "bar_window_length": len(deltas),
            },
        )

    observed_series = [exposure * delta for exposure, delta in zip(exposures, deltas)]
    observed_total = sum(observed_series)
    observed_mean = safe_mean(observed_series)
    observed_sharpe = _series_sharpe(observed_series)

    rng = make_rng(config, salt=409)
    null_totals: list[float] = []
    null_means: list[float] = []
    null_sharpes: list[float] = []
    permutable = list(deltas)
    for _ in range(config.robustness.monte_carlo_iterations):
        shuffled = list(permutable)
        rng.shuffle(shuffled)
        simulated = [exposure * delta for exposure, delta in zip(exposures, shuffled)]
        null_totals.append(sum(simulated))
        null_means.append(safe_mean(simulated))
        null_sharpes.append(_series_sharpe(simulated))

    total_percentile = percentile_rank(null_totals, observed_total)
    mean_percentile = percentile_rank(null_means, observed_mean)
    sharpe_percentile = percentile_rank(null_sharpes, observed_sharpe)
    market_permutation_score = clamp01(
        safe_mean(
            [
                max(0.0, (total_percentile - 0.5) * 2.0),
                max(0.0, (mean_percentile - 0.5) * 2.0),
                max(0.0, (sharpe_percentile - 0.5) * 2.0),
            ]
        )
    )
    caution_flag = total_percentile < 0.9 or mean_percentile < 0.9

    status = "pass"
    if market_permutation_score < 0.35:
        status = "fail"
    elif market_permutation_score < 0.60 or caution_flag:
        status = "warn"

    summary = (
        f"Market-level permutation score {market_permutation_score:.2f}; observed exposure-weighted capture sits at the "
        f"{total_percentile:.1%} percentile of randomized bar sequences."
    )
    return ValidationModuleResult(
        module_name="market_permutation",
        status=status,
        summary=summary,
        metrics={
            "market_permutation_score": round(market_permutation_score, config.report_precision),
            "observed_capture_percentile": round(total_percentile, config.report_precision),
            "observed_mean_capture_percentile": round(mean_percentile, config.report_precision),
            "observed_sharpe_capture_percentile": round(sharpe_percentile, config.report_precision),
            "significance_style_caution_flag": caution_flag,
        },
        diagnostics={
            "method": "bar_delta_permutation_with_fixed_exposure_schedule",
            "bar_window_length": len(deltas),
            "active_exposure_bars": active_count,
            "observed_capture_summary": {
                "total_capture": observed_total,
                "mean_capture": observed_mean,
                "sharpe_capture": observed_sharpe,
            },
        },
        artifacts={
            "permutation_distribution_summary": {
                "total_capture": {
                    "lower": percentile(null_totals, 0.05),
                    "median": percentile(null_totals, 0.50),
                    "upper": percentile(null_totals, 0.95),
                },
                "mean_capture": {
                    "lower": percentile(null_means, 0.05),
                    "median": percentile(null_means, 0.50),
                    "upper": percentile(null_means, 0.95),
                },
                "sharpe_capture": {
                    "lower": percentile(null_sharpes, 0.05),
                    "median": percentile(null_sharpes, 0.50),
                    "upper": percentile(null_sharpes, 0.95),
                },
            },
            "market_randomization_basis": {
                "preserved": "observed exposure schedule and bar-return marginal distribution",
                "randomized": "bar return ordering",
            },
        },
        recommendations=[
            "Treat this as market-level randomization significance, separate from execution perturbation stress."
        ],
    )


def _bar_exposures_and_deltas(strategy: StrategyBacktest) -> tuple[list[float], list[float]]:
    position_times = [point.timestamp for point in strategy.position_series]
    positions = [point.position for point in strategy.position_series]
    exposures: list[float] = []
    deltas: list[float] = []
    for left, right in zip(strategy.bar_data, strategy.bar_data[1:]):
        index = bisect_right(position_times, left.timestamp) - 1
        exposure = positions[index] if index >= 0 else 0.0
        exposures.append(float(exposure))
        deltas.append(float(right.close - left.close))
    return exposures, deltas


def _series_sharpe(values: list[float]) -> float:
    stdev = safe_stdev(values)
    if stdev <= 1e-12:
        return 0.0
    return safe_mean(values) / stdev * (len(values) ** 0.5)
