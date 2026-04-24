"""Score aggregation for validation results."""

from __future__ import annotations

from ..reporting.models import ScorecardEntry, ValidationModuleResult, ValidationScorecard
from ..utils.stats import clamp01, safe_mean

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


def build_scorecard(module_results: list[ValidationModuleResult]) -> ValidationScorecard:
    entries: list[ScorecardEntry] = []
    weighted_scores: list[float] = []
    for result in module_results:
        numeric_scores = {
            name: float(value)
            for name, value in result.metrics.items()
            if name.endswith("_score") and isinstance(value, (int, float))
        }
        primary_name = next(iter(sorted(numeric_scores)), None)
        primary_value = numeric_scores.get(primary_name) if primary_name else None
        if primary_value is not None and result.module_name not in DIAGNOSTIC_ONLY_MODULES:
            weighted_scores.append(primary_value)
        entries.append(
            ScorecardEntry(
                module_name=result.module_name,
                status=result.status,
                primary_score_name=primary_name,
                primary_score_value=primary_value,
                secondary_scores=numeric_scores,
            )
        )

    total = len(module_results) or 1
    pass_count = sum(1 for result in module_results if result.status == "pass")
    warn_count = sum(1 for result in module_results if result.status == "warn")
    fail_count = sum(1 for result in module_results if result.status == "fail")
    error_count = sum(1 for result in module_results if result.status == "error")
    evidence_coverage = clamp01((pass_count + warn_count) / total)
    composite_score = clamp01(safe_mean(weighted_scores))
    return ValidationScorecard(
        composite_score=composite_score,
        entries=tuple(entries),
        pass_count=pass_count,
        warn_count=warn_count,
        fail_count=fail_count,
        error_count=error_count,
        evidence_coverage=evidence_coverage,
    )
