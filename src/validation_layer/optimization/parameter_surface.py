"""Parameter-surface diagnostics for optimization skepticism."""

from __future__ import annotations

from dataclasses import dataclass

from ..config.schemas import ValidationConfig
from ..data.contracts import OptimizationRun
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_divide, safe_mean, safe_median, safe_stdev
from ._helpers import (
    ParameterPoint,
    insufficient_evidence_result,
    parameter_distance,
    parameter_spans,
    validate_full_history,
)


@dataclass(frozen=True)
class ParameterSurfaceAnalysis:
    parameter_surface_score: float
    neighborhood_robustness_score: float
    plateau_score: float
    winner_isolation_index: float
    local_support_count: int
    similar_neighbor_fraction: float
    winner_vs_median_gap: float
    winner_vs_top_decile_gap: float
    local_neighborhood_mean: float
    local_neighborhood_median: float
    surface_classification: str
    nearest_neighbors: tuple[dict[str, object], ...]
    axis_slices: dict[str, list[dict[str, object]]]


def analyze_parameter_surface(run: OptimizationRun) -> ParameterSurfaceAnalysis | None:
    valid, _, points, chosen = validate_full_history(
        run,
        minimum_trials=8,
        minimum_unique_parameter_sets=3,
        require_split_history=False,
    )
    if not valid or chosen is None:
        return None

    spans = parameter_spans(points)
    pack_medians = sorted(point.median_objective for point in points)
    winner_median = chosen.median_objective
    neighborhood: list[tuple[ParameterPoint, float]] = []
    for point in points:
        if point.parameter_key == chosen.parameter_key:
            continue
        distance = parameter_distance(chosen.parameter_values, point.parameter_values, spans)
        neighborhood.append((point, distance))
    neighborhood.sort(key=lambda item: (item[1], -item[0].median_objective))

    local_neighbors = [(point, distance) for point, distance in neighborhood if distance <= 0.34]
    if len(local_neighbors) < 3:
        local_neighbors = neighborhood[: min(5, len(neighborhood))]

    local_medians = [point.median_objective for point, _ in local_neighbors]
    local_support_count = len(local_neighbors)
    local_mean = safe_mean(local_medians)
    local_median = safe_median(local_medians)
    similar_neighbor_fraction = safe_divide(
        sum(1 for value in local_medians if value >= winner_median * 0.85),
        local_support_count,
        default=0.0,
    )

    winner_vs_median_gap = winner_median - safe_median(pack_medians)
    top_decile_index = max(0, int(0.9 * (len(pack_medians) - 1)))
    top_decile_reference = pack_medians[top_decile_index]
    winner_vs_top_decile_gap = winner_median - top_decile_reference
    isolation_ratio = safe_divide(max(0.0, winner_median - local_mean), abs(winner_median) + 1e-9, default=0.0)
    plateau_score = clamp01(safe_mean([similar_neighbor_fraction, 1.0 - min(1.0, safe_stdev(local_medians))]))
    neighborhood_robustness_score = clamp01(
        safe_mean(
            [
                plateau_score,
                min(1.0, safe_divide(local_mean, winner_median, default=0.0)),
                min(1.0, local_support_count / 5.0),
            ]
        )
    )
    winner_isolation_index = clamp01(isolation_ratio)
    parameter_surface_score = clamp01(safe_mean([neighborhood_robustness_score, 1.0 - winner_isolation_index]))

    if winner_isolation_index > 0.55 or similar_neighbor_fraction < 0.25:
        surface_classification = "narrow_spike"
    elif plateau_score > 0.65 and local_support_count >= 3:
        surface_classification = "stable_plateau"
    else:
        surface_classification = "mixed_ridge"

    axis_slices: dict[str, list[dict[str, object]]] = {}
    for name in sorted(chosen.parameter_values):
        grouped: dict[object, list[float]] = {}
        for point in points:
            grouped.setdefault(point.parameter_values.get(name), []).append(point.median_objective)
        ranked = sorted(grouped.items(), key=lambda item: safe_mean(item[1]), reverse=True)
        axis_slices[name] = [
            {
                "value": value,
                "median_objective": round(safe_median(values), 6),
                "mean_objective": round(safe_mean(values), 6),
                "sample_count": len(values),
            }
            for value, values in ranked[:5]
        ]

    nearest_neighbors = tuple(
        {
            "parameter_values": point.parameter_values,
            "distance": round(distance, 6),
            "median_objective": round(point.median_objective, 6),
            "trial_count": point.trial_count,
            "split_count": point.split_count,
        }
        for point, distance in local_neighbors[:8]
    )
    return ParameterSurfaceAnalysis(
        parameter_surface_score=parameter_surface_score,
        neighborhood_robustness_score=neighborhood_robustness_score,
        plateau_score=plateau_score,
        winner_isolation_index=winner_isolation_index,
        local_support_count=local_support_count,
        similar_neighbor_fraction=similar_neighbor_fraction,
        winner_vs_median_gap=winner_vs_median_gap,
        winner_vs_top_decile_gap=winner_vs_top_decile_gap,
        local_neighborhood_mean=local_mean,
        local_neighborhood_median=local_median,
        surface_classification=surface_classification,
        nearest_neighbors=nearest_neighbors,
        axis_slices=axis_slices,
    )


def run_parameter_surface_module(run: OptimizationRun, config: ValidationConfig) -> ValidationModuleResult:
    valid, reason, points, chosen = validate_full_history(
        run,
        minimum_trials=8,
        minimum_unique_parameter_sets=3,
        require_split_history=False,
    )
    if not valid or chosen is None:
        return insufficient_evidence_result(
            "parameter_surface",
            reason,
            ("parameter_surface_score", "neighborhood_robustness_score", "plateau_score"),
            diagnostics={
                "trial_count": len(run.trials),
                "unique_parameter_sets": len(points),
                "chosen_parameters_present": chosen is not None,
            },
            recommendations=["Parameter-surface analysis needs full trial-level history with the chosen parameters present."],
        )

    analysis = analyze_parameter_surface(run)
    if analysis is None:
        return insufficient_evidence_result(
            "parameter_surface",
            "Insufficient evidence: parameter surface could not be analyzed from the provided history.",
            ("parameter_surface_score", "neighborhood_robustness_score", "plateau_score"),
        )

    status = "pass"
    if analysis.surface_classification == "narrow_spike":
        status = "fail"
    elif analysis.parameter_surface_score < 0.55:
        status = "warn"

    summary = (
        f"Winner median objective beat the pack median by {analysis.winner_vs_median_gap:.3f}; "
        f"surface reads as {analysis.surface_classification} with neighborhood robustness {analysis.neighborhood_robustness_score:.2f}."
    )
    recommendations = []
    if status == "fail":
        recommendations.append("Do not trust an isolated winner; broaden the search and look for a plateau before robustness testing.")
    elif status == "warn":
        recommendations.append("Nearby parameter choices are only partially supportive; treat the winner as fragile.")
    else:
        recommendations.append("Surface shape is credible enough to carry forward into robustness testing, pending later phases.")
    return ValidationModuleResult(
        module_name="parameter_surface",
        status=status,
        summary=summary,
        metrics={
            "parameter_surface_score": round(analysis.parameter_surface_score, config.report_precision),
            "neighborhood_robustness_score": round(analysis.neighborhood_robustness_score, config.report_precision),
            "plateau_score": round(analysis.plateau_score, config.report_precision),
            "winner_isolation_index": round(analysis.winner_isolation_index, config.report_precision),
            "local_support_count": analysis.local_support_count,
            "similar_neighbor_fraction": round(analysis.similar_neighbor_fraction, config.report_precision),
        },
        diagnostics={
            "winner_vs_pack": {
                "winner_vs_median_gap": round(analysis.winner_vs_median_gap, config.report_precision),
                "winner_vs_top_decile_gap": round(analysis.winner_vs_top_decile_gap, config.report_precision),
            },
            "local_neighborhood": {
                "mean_objective": round(analysis.local_neighborhood_mean, config.report_precision),
                "median_objective": round(analysis.local_neighborhood_median, config.report_precision),
                "support_count": analysis.local_support_count,
            },
            "surface_classification": analysis.surface_classification,
        },
        artifacts={
            "surface_classification": analysis.surface_classification,
            "nearest_neighbors": analysis.nearest_neighbors,
            "parameter_axis_slices": analysis.axis_slices,
        },
        recommendations=recommendations,
    )
