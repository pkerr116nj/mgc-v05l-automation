"""Feature-report helpers."""

from __future__ import annotations

from ..reporting.models import ValidationModuleResult


def summarize_feature_quality(module_results: list[ValidationModuleResult]) -> list[str]:
    recommendations: list[str] = []
    for result in module_results:
        if result.status in {"fail", "error"}:
            recommendations.extend(result.recommendations)
    if not recommendations:
        recommendations.append("Feature looks viable enough for later strategy construction, not yet tradability.")
    return recommendations
