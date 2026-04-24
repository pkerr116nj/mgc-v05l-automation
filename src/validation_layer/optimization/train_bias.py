"""Training-bias diagnostics using full optimization history."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import OptimizationRun
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_divide, safe_mean, safe_median, safe_stdev
from ._helpers import (
    derive_split_ranks,
    insufficient_evidence_result,
    objective_values,
    parameter_key,
    validate_full_history,
)
from .parameter_surface import analyze_parameter_surface


def run_training_bias_module(run: OptimizationRun, config: ValidationConfig) -> ValidationModuleResult:
    valid, reason, points, chosen = validate_full_history(
        run,
        minimum_trials=8,
        minimum_unique_parameter_sets=3,
        require_split_history=False,
    )
    if not valid or chosen is None:
        return insufficient_evidence_result(
            "train_bias",
            reason,
            ("training_bias_score", "winner_fragility_score", "parameter_stability_score"),
            diagnostics={
                "trial_count": len(run.trials),
                "unique_parameter_sets": len(points),
                "chosen_parameters_present": chosen is not None,
            },
            recommendations=["Training-bias analysis requires full optimization history, not a summary of winners."],
        )

    raw_objectives = sorted(objective_values(run))
    best_objective = raw_objectives[-1]
    median_objective = safe_median(raw_objectives)
    best_trial_inflation_estimate = safe_divide(best_objective - median_objective, abs(median_objective) + 1e-9, default=0.0)
    median_vs_best_gap = best_objective - median_objective

    surface = analyze_parameter_surface(run)
    if surface is None:
        return insufficient_evidence_result(
            "train_bias",
            "Insufficient evidence: unable to analyze winner fragility from the optimization history.",
            ("training_bias_score", "winner_fragility_score", "parameter_stability_score"),
        )

    split_ranks = derive_split_ranks(run.trials)
    split_evidence: list[float] = []
    chosen_key = parameter_key(run.chosen_parameters)
    for (split_id, key), rank in split_ranks.items():
        if key == chosen_key:
            split_size = sum(1 for trial in run.trials if trial.split_id == split_id)
            split_evidence.append(1.0 - safe_divide(rank - 1, max(1, split_size - 1), default=0.0))
    split_support = safe_mean(split_evidence) if split_evidence else 0.5

    winner_fragility_score = clamp01(
        safe_mean(
            [
                surface.neighborhood_robustness_score,
                1.0 - surface.winner_isolation_index,
                split_support,
            ]
        )
    )
    parameter_stability_score = clamp01(safe_mean([surface.plateau_score, surface.neighborhood_robustness_score]))
    training_bias_score = clamp01(
        safe_mean(
            [
                1.0 / (1.0 + max(0.0, best_trial_inflation_estimate)),
                winner_fragility_score,
                parameter_stability_score,
            ]
        )
    )

    status = "pass"
    if training_bias_score < 0.45:
        status = "fail"
    elif best_trial_inflation_estimate > 1.0 or winner_fragility_score < 0.55:
        status = "warn"

    summary = (
        f"Winner beat the median trial by {median_vs_best_gap:.3f} ({best_trial_inflation_estimate:.2f} inflation), "
        f"with fragility score {winner_fragility_score:.2f}."
    )
    recommendations = []
    if status == "fail":
        recommendations.append("Optimization looks materially inflated relative to the pack; do not treat the winner as durable.")
    elif status == "warn":
        recommendations.append("Winner advantage may be exaggerated by training bias; require robustness testing before trust.")
    else:
        recommendations.append("Training-bias evidence is good enough to proceed to robustness testing, still with skepticism.")
    return ValidationModuleResult(
        module_name="train_bias",
        status=status,
        summary=summary,
        metrics={
            "training_bias_score": round(training_bias_score, config.report_precision),
            "winner_fragility_score": round(winner_fragility_score, config.report_precision),
            "parameter_stability_score": round(parameter_stability_score, config.report_precision),
            "best_trial_inflation_estimate": round(best_trial_inflation_estimate, config.report_precision),
            "median_vs_best_gap": round(median_vs_best_gap, config.report_precision),
        },
        diagnostics={
            "best_trial_inflation_estimate": best_trial_inflation_estimate,
            "median_vs_best_gap": median_vs_best_gap,
            "winner_fragility_under_nearby_perturbations": {
                "neighbor_robustness": surface.neighborhood_robustness_score,
                "winner_isolation_index": surface.winner_isolation_index,
            },
            "split_aware_support": {
                "available": bool(split_evidence),
                "mean_split_rank_support": split_support,
                "split_rank_support_values": split_evidence,
            },
            "objective_dispersion": safe_stdev(raw_objectives),
        },
        artifacts={
            "surface_classification": surface.surface_classification,
            "nearest_neighbors": surface.nearest_neighbors,
        },
        recommendations=recommendations,
    )
