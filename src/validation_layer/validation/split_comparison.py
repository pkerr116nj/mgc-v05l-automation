"""Compare validation outcomes across split families."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_mean


def run_split_comparison_module(
    walkforward_result: ValidationModuleResult,
    cross_validation_result: ValidationModuleResult,
    config: ValidationConfig,
) -> ValidationModuleResult:
    wf_score = float(walkforward_result.metrics.get("walkforward_score", 0.0) or 0.0)
    cv_score = float(cross_validation_result.metrics.get("cross_validation_score", 0.0) or 0.0)
    status_agreement = 1.0 if walkforward_result.status == cross_validation_result.status else 0.0
    score_agreement = 1.0 - min(1.0, abs(wf_score - cv_score))
    confidence_penalty = 1.0 - safe_mean([status_agreement, score_agreement])
    split_dependence_score = clamp01(1.0 - confidence_penalty)
    instability_flag = (
        walkforward_result.status == "pass" and cross_validation_result.status in {"warn", "fail"}
    ) or (
        cross_validation_result.status == "pass" and walkforward_result.status in {"warn", "fail"}
    )
    status = "pass"
    if split_dependence_score < config.thresholds.split_dependence_min:
        status = "fail"
    elif instability_flag:
        status = "warn"
    summary = (
        f"Split comparison score {split_dependence_score:.2f}; walk-forward {wf_score:.2f} vs "
        f"cross-validation {cv_score:.2f}."
    )
    return ValidationModuleResult(
        module_name="split_comparison",
        status=status,
        summary=summary,
        metrics={"split_dependence_score": round(split_dependence_score, config.report_precision)},
        diagnostics={
            "agreement_disagreement_matrix": {
                "status_agreement": status_agreement,
                "score_agreement": score_agreement,
            },
            "confidence_penalty": confidence_penalty,
            "instability_flag": instability_flag,
        },
        artifacts={},
        recommendations=["Do not trust a strategy whose edge exists only under one flattering split method."],
    )
