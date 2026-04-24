"""Feature stationarity and stability diagnostics."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import FeatureSeries
from ..data.normalization import normalize_feature_series
from ..data.sessioning import build_regime_labels, build_session_labels
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, rolling_windows, run_lengths, safe_divide, safe_mean, safe_stdev


def run_stationarity_module(feature: FeatureSeries, config: ValidationConfig) -> ValidationModuleResult:
    normalized = normalize_feature_series(feature)
    values = normalized.values
    timestamps = normalized.timestamps
    mean_scores: list[float] = []
    variance_scores: list[float] = []
    rolling_mean_stability: dict[str, float] = {}
    rolling_variance_stability: dict[str, float] = {}

    for window in config.stationarity_windows:
        windows = rolling_windows(values, window)
        means = [safe_mean(window_values) for window_values in windows]
        variances = [safe_stdev(window_values) ** 2 for window_values in windows]
        mean_instability = safe_divide(safe_stdev(means), abs(safe_mean(values)) + safe_stdev(values) + 1e-9, default=0.0)
        variance_instability = safe_divide(safe_stdev(variances), safe_mean(variances) + 1e-9, default=0.0)
        mean_score = clamp01(1.0 / (1.0 + mean_instability))
        variance_score = clamp01(1.0 / (1.0 + variance_instability))
        mean_scores.append(mean_score)
        variance_scores.append(variance_score)
        rolling_mean_stability[f"window_{window}"] = mean_score
        rolling_variance_stability[f"window_{window}"] = variance_score

    sessions = build_session_labels(timestamps, config)
    regimes = build_regime_labels(len(values), config)
    groups: dict[str, list[float]] = {}
    for label, value in zip(regimes, values):
        groups.setdefault(label, []).append(value)
    regime_means = [safe_mean(group) for group in groups.values()]
    regime_sensitivity = safe_divide(max(regime_means) - min(regime_means), safe_stdev(values) + 1e-9, default=0.0) if regime_means else 0.0

    threshold = safe_mean(values)
    lengths = run_lengths([value >= threshold for value in values])
    max_run = max(lengths) if lengths else 0
    run_persistence_score = clamp01(1.0 - safe_divide(max_run, len(values), default=1.0))

    left = values[: len(values) // 2]
    right = values[len(values) // 2 :]
    mean_delta = abs(safe_mean(left) - safe_mean(right))
    variance_delta = abs((safe_stdev(left) ** 2) - (safe_stdev(right) ** 2))
    window_sensitivity_score = clamp01(
        1.0 / (1.0 + safe_divide(mean_delta + variance_delta, safe_stdev(values) + 1e-9, default=0.0))
    )

    stationarity_score = clamp01(
        safe_mean(
            [
                safe_mean(mean_scores),
                safe_mean(variance_scores),
                1.0 / (1.0 + regime_sensitivity),
            ]
        )
    )

    status = "pass"
    if len(values) < config.thresholds.minimum_feature_length:
        status = "warn"
    if stationarity_score < config.thresholds.feature_stability_min:
        status = "fail"

    summary = (
        f"Stationarity score {stationarity_score:.2f}; longest run {max_run} observations; "
        f"window sensitivity {window_sensitivity_score:.2f}."
    )
    return ValidationModuleResult(
        module_name="stationarity",
        status=status,
        summary=summary,
        metrics={
            "stationarity_score": round(stationarity_score, config.report_precision),
            "run_persistence_score": round(run_persistence_score, config.report_precision),
            "window_sensitivity_score": round(window_sensitivity_score, config.report_precision),
            "regime_sensitivity": round(regime_sensitivity, config.report_precision),
            "observation_count": len(values),
        },
        diagnostics={
            "rolling_mean_stability": rolling_mean_stability,
            "rolling_variance_stability": rolling_variance_stability,
            "sessions": list(sessions),
            "regimes": list(regimes),
            "run_lengths": list(lengths),
            "difference_of_window_diagnostics": {
                "mean_delta": mean_delta,
                "variance_delta": variance_delta,
            },
            "optional_tests_available": False,
        },
        artifacts={},
        recommendations=["Treat this as a feature quality filter, not proof of alpha."],
    )
