"""Unified orchestration entry point for research validation."""

from __future__ import annotations

from ..config.defaults import build_default_config
from ..config.schemas import ValidationConfig
from ..data.contracts import FeatureSeries, OptimizationRun, StrategyBacktest, ValidationSubject
from ..data.normalization import normalize_feature_series, normalize_strategy_backtest
from ..features.feature_report import summarize_feature_quality
from ..orchestration.promotion import collect_blocking_issues, determine_overall_status
from ..orchestration.registry import feature_runners, optimization_runners, strategy_runners
from ..reporting.models import ValidationModuleResult, ValidationReport
from ..reporting.scorecard import build_scorecard
from ..utils.logging import get_logger, log_event
from ..validation.split_comparison import run_split_comparison_module


def _coerce_subject(subject: FeatureSeries | StrategyBacktest | OptimizationRun | ValidationSubject) -> ValidationSubject:
    if isinstance(subject, ValidationSubject):
        return subject
    if isinstance(subject, FeatureSeries):
        return ValidationSubject(feature_series=normalize_feature_series(subject))
    if isinstance(subject, StrategyBacktest):
        return ValidationSubject(strategy_backtest=normalize_strategy_backtest(subject))
    if isinstance(subject, OptimizationRun):
        return ValidationSubject(optimization_run=subject)
    raise TypeError(f"Unsupported validation subject: {type(subject)!r}")


def run_validation_pipeline(
    subject: FeatureSeries | StrategyBacktest | OptimizationRun | ValidationSubject,
    config: ValidationConfig | None = None,
) -> ValidationReport:
    resolved_config = config or build_default_config()
    resolved_subject = _coerce_subject(subject)
    logger = get_logger(__name__)
    log_event(logger, "validation_pipeline_started", subject_type=resolved_subject.subject_type, subject_name=resolved_subject.subject_name)

    module_results: list[ValidationModuleResult] = []
    if resolved_subject.feature_series is not None:
        for runner in feature_runners(resolved_config):
            module_results.append(runner.runner(resolved_subject.feature_series, resolved_config))

    if resolved_subject.strategy_backtest is not None:
        for runner in strategy_runners(resolved_config):
            module_results.append(runner.runner(resolved_subject.strategy_backtest, resolved_config))
        walkforward_result = next((result for result in module_results if result.module_name == "walkforward"), None)
        cross_validation_result = next((result for result in module_results if result.module_name == "cross_validation"), None)
        if (
            resolved_config.module_flags.split_comparison
            and walkforward_result is not None
            and cross_validation_result is not None
        ):
            module_results.append(run_split_comparison_module(walkforward_result, cross_validation_result, resolved_config))

    if resolved_subject.optimization_run is not None:
        for runner in optimization_runners(resolved_config):
            module_results.append(runner.runner(resolved_subject.optimization_run, resolved_config))

    scorecard = build_scorecard(module_results)
    blocking_issues, warnings = collect_blocking_issues(resolved_subject.subject_type, module_results, resolved_config)
    overall_status = determine_overall_status(resolved_subject.subject_type, module_results, scorecard, resolved_config)

    next_actions: list[str] = []
    if resolved_subject.subject_type == "feature":
        next_actions.extend(summarize_feature_quality(module_results))
    elif resolved_subject.strategy_backtest is None and resolved_subject.optimization_run is not None:
        next_actions.append("Optimization diagnostics alone are not enough for promotion; add strategy validation evidence next.")
    elif overall_status == "reject":
        next_actions.append("Reject promotion and investigate the blocking diagnostics before re-running.")
    elif overall_status == "insufficient_evidence":
        next_actions.append("Gather more samples or broader splits before considering probation.")
    elif overall_status == "probation":
        next_actions.append("Eligible for shadow or tightly constrained paper probation, not full deployment.")
    else:
        next_actions.append("Promotion is acceptable only to the next approved research stage.")

    optimization_module_results = [
        result
        for result in module_results
        if result.module_name in {"parameter_surface", "train_bias", "selection_bias", "cscv_pbo"}
    ]
    if optimization_module_results:
        optimization_statuses = {result.status for result in optimization_module_results}
        if "fail" in optimization_statuses or "warn" in optimization_statuses:
            next_actions.append("Optimization result is not yet clearly credible enough to advance to robustness testing.")
        else:
            next_actions.append("Optimization result looks credible enough to advance to robustness testing.")

    log_event(
        logger,
        "validation_pipeline_completed",
        subject_type=resolved_subject.subject_type,
        subject_name=resolved_subject.subject_name,
        overall_status=overall_status,
        module_count=len(module_results),
    )
    return ValidationReport(
        subject_type=resolved_subject.subject_type,
        subject_name=resolved_subject.subject_name,
        overall_status=overall_status,
        module_results=tuple(module_results),
        scorecard=scorecard,
        blocking_issues=tuple(blocking_issues),
        warnings=tuple(warnings),
        next_actions=tuple(next_actions),
    )
