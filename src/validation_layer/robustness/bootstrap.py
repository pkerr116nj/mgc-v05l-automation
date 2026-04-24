"""Bootstrap confidence intervals for strategy robustness."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import StrategyBacktest
from ..performance.metrics import calculate_canonical_metrics
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_mean
from ._helpers import (
    bounded_width_score,
    ensure_robustness_prerequisites,
    evaluate_trade_variant,
    finite_ratio,
    make_rng,
    metric_interval,
    positive_fraction,
)


def run_bootstrap_module(strategy: StrategyBacktest, config: ValidationConfig) -> ValidationModuleResult:
    insufficient = ensure_robustness_prerequisites(
        strategy,
        config,
        module_name="bootstrap",
        score_fields=("bootstrap_confidence_score",),
    )
    if insufficient is not None:
        return insufficient

    observed = calculate_canonical_metrics(strategy, config)
    trades = tuple(strategy.trades)
    trade_count = len(trades)
    rng = make_rng(config, salt=101)

    expectancies: list[float] = []
    drawdowns: list[float] = []
    sharpes: list[float] = []
    profit_factors: list[float] = []

    for index in range(config.robustness.bootstrap_samples):
        sample = tuple(trades[rng.randrange(trade_count)] for _ in range(trade_count))
        snapshot = evaluate_trade_variant(strategy, sample, suffix=f"bootstrap_{index}", config=config)
        expectancies.append(snapshot.expectancy)
        drawdowns.append(snapshot.max_drawdown)
        sharpes.append(snapshot.sharpe_like_ratio)
        profit_factors.append(finite_ratio(snapshot.profit_factor))

    expectancy_interval = metric_interval(expectancies)
    drawdown_interval = metric_interval(drawdowns)
    sharpe_interval = metric_interval(sharpes)
    profit_factor_interval = metric_interval(profit_factors)
    percentile_methods = {
        "expectancy": expectancy_interval,
        "drawdown": drawdown_interval,
        "sharpe_like_ratio": sharpe_interval,
        "profit_factor": profit_factor_interval,
    }
    basic_methods = {
        "expectancy": _basic_interval(observed.expectancy, expectancy_interval),
        "drawdown": _basic_interval(observed.max_drawdown, drawdown_interval),
        "sharpe_like_ratio": _basic_interval(observed.sharpe_like_ratio, sharpe_interval),
        "profit_factor": _basic_interval(finite_ratio(observed.profit_factor), profit_factor_interval),
    }
    sign_stability = positive_fraction(expectancies)
    magnitude_stability = safe_mean(
        [
            bounded_width_score(expectancy_interval["width"], observed.expectancy),
            bounded_width_score(drawdown_interval["width"], observed.max_drawdown),
            bounded_width_score(sharpe_interval["width"], observed.sharpe_like_ratio),
            bounded_width_score(profit_factor_interval["width"], finite_ratio(observed.profit_factor)),
        ]
    )
    bootstrap_confidence_score = clamp01(safe_mean([sign_stability, magnitude_stability]))

    status = "pass"
    if bootstrap_confidence_score < 0.35 or sign_stability < 0.55:
        status = "fail"
    elif bootstrap_confidence_score < 0.60:
        status = "warn"

    summary = (
        f"Bootstrap confidence score {bootstrap_confidence_score:.2f} with expectancy interval "
        f"[{expectancy_interval['lower']:.2f}, {expectancy_interval['upper']:.2f}] using percentile bounds; "
        f"basic bounds are reported separately in diagnostics."
    )
    return ValidationModuleResult(
        module_name="bootstrap",
        status=status,
        summary=summary,
        metrics={
            "bootstrap_confidence_score": round(bootstrap_confidence_score, config.report_precision),
            "expectancy_sign_stability": round(sign_stability, config.report_precision),
            "bootstrap_interval_tightness": round(magnitude_stability, config.report_precision),
            "bootstrap_sample_count": config.robustness.bootstrap_samples,
        },
        diagnostics={
            "reported_interval_methods": ["percentile", "basic"],
            "observed_metrics": {
                "expectancy": observed.expectancy,
                "max_drawdown": observed.max_drawdown,
                "sharpe_like_ratio": observed.sharpe_like_ratio,
                "profit_factor": finite_ratio(observed.profit_factor),
            },
            "stability_of_sign_and_magnitude": {
                "sign_stability": sign_stability,
                "magnitude_stability": magnitude_stability,
            },
            "bootstrap_method_interpretation": {
                "percentile": "Direct empirical quantiles of bootstrap estimates.",
                "basic": "Pivot/basic interval reflected around the observed estimate.",
            },
        },
        artifacts={
            "bootstrap_percentile_intervals": percentile_methods,
            "bootstrap_basic_intervals": basic_methods,
        },
        recommendations=[
            "Treat wide bootstrap intervals as a warning that the backtest edge may not be transportable."
        ],
    )


def _basic_interval(observed: float, percentile_interval: dict[str, float]) -> dict[str, float]:
    return {
        "lower": 2.0 * observed - percentile_interval["upper"],
        "median": observed,
        "upper": 2.0 * observed - percentile_interval["lower"],
        "width": percentile_interval["width"],
    }
