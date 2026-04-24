"""Centralized promotion logic for validation results."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..reporting.models import OverallStatus, ValidationModuleResult, ValidationScorecard

DIAGNOSTIC_ONLY_MODULES = frozenset(
    {
        "parameter_surface",
        "train_bias",
        "selection_bias",
        "cscv_pbo",
        "bootstrap",
        "monte_carlo",
        "market_permutation",
        "confidence_tests",
    }
)


def collect_blocking_issues(
    subject_type: str,
    module_results: list[ValidationModuleResult],
    config: ValidationConfig,
) -> tuple[list[str], list[str]]:
    blocking: list[str] = []
    warnings: list[str] = []
    for result in module_results:
        if result.status == "fail":
            if result.module_name in DIAGNOSTIC_ONLY_MODULES:
                warnings.append(f"{result.module_name} diagnosed a serious concern: {result.summary}")
            else:
                blocking.append(f"{result.module_name} failed: {result.summary}")
        elif result.status == "error":
            blocking.append(f"{result.module_name} errored: {result.summary}")
        elif result.status == "warn":
            warnings.append(f"{result.module_name} warned: {result.summary}")

    thresholds = config.thresholds
    for result in module_results:
        metrics = result.metrics
        if subject_type == "feature" and float(metrics.get("stationarity_score", 1.0) or 1.0) < thresholds.feature_stability_min:
            blocking.append("Feature stability score is below the configured minimum.")
        if float(metrics.get("walkforward_score", 1.0) or 1.0) < thresholds.walkforward_min:
            blocking.append("Walk-forward score is below the configured minimum.")
        if float(metrics.get("cross_validation_score", 1.0) or 1.0) < thresholds.cross_validation_min:
            warnings.append("Cross-validation score is below the configured minimum.")
        if float(metrics.get("split_dependence_score", 1.0) or 1.0) < thresholds.split_dependence_min:
            blocking.append("Split dependence is too high for promotion.")
        if int(metrics.get("fail_count", 0) or 0) > thresholds.max_fold_failures:
            blocking.append("Too many fold failures for promotion.")
        if float(metrics.get("drawdown_score", 1.0) or 1.0) < thresholds.drawdown_min:
            blocking.append("Drawdown geometry is too weak for promotion.")
        if float(metrics.get("pain_profile_score", 1.0) or 1.0) < thresholds.pain_profile_min:
            warnings.append("Pain profile is weak and should be improved.")
    return blocking, warnings


def determine_overall_status(
    subject_type: str,
    module_results: list[ValidationModuleResult],
    scorecard: ValidationScorecard,
    config: ValidationConfig,
) -> OverallStatus:
    if any(
        result.module_name == "layer1_prerequisites" and bool(result.metrics.get("insufficient_evidence"))
        for result in module_results
    ):
        return "insufficient_evidence"
    blocking, warnings = collect_blocking_issues(subject_type, module_results, config)
    if blocking:
        return "reject"
    if scorecard.error_count > 0:
        return "reject"
    if scorecard.evidence_coverage < 0.75:
        return "insufficient_evidence"
    if scorecard.composite_score >= config.thresholds.promote_composite_min and not warnings:
        return "promote"
    if scorecard.composite_score >= config.thresholds.probation_composite_min:
        return "probation"
    return "insufficient_evidence"
