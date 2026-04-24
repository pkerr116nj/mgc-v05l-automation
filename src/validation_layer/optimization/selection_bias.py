"""Selection-bias diagnostics using split-aware optimization history."""

from __future__ import annotations

from ..config.schemas import ValidationConfig
from ..data.contracts import OptimizationRun
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_divide, safe_mean, safe_stdev
from ._helpers import derive_split_ranks, insufficient_evidence_result, parameter_key, validate_full_history


def run_selection_bias_module(run: OptimizationRun, config: ValidationConfig) -> ValidationModuleResult:
    valid, reason, points, chosen = validate_full_history(
        run,
        minimum_trials=8,
        minimum_unique_parameter_sets=3,
        require_split_history=True,
    )
    if not valid or chosen is None:
        return insufficient_evidence_result(
            "selection_bias",
            reason,
            ("selection_bias_score", "winner_luck_index", "cross_split_rank_stability"),
            diagnostics={
                "trial_count": len(run.trials),
                "unique_parameter_sets": len(points),
                "chosen_parameters_present": chosen is not None,
            },
            recommendations=["Selection-bias analysis requires full split-aware optimization history."],
        )

    chosen_key = parameter_key(run.chosen_parameters)
    split_ranks = derive_split_ranks(run.trials)
    split_sizes: dict[str, int] = {}
    for trial in run.trials:
        if trial.split_id is not None:
            split_sizes[trial.split_id] = split_sizes.get(trial.split_id, 0) + 1

    chosen_split_ranks: list[int] = []
    normalized_rank_scores: list[float] = []
    missing_splits: list[str] = []
    for split_id, split_size in sorted(split_sizes.items()):
        rank = split_ranks.get((split_id, chosen_key))
        if rank is None:
            missing_splits.append(split_id)
            continue
        chosen_split_ranks.append(rank)
        normalized_rank_scores.append(1.0 - safe_divide(rank - 1, max(1, split_size - 1), default=0.0))

    if len(chosen_split_ranks) < 2:
        return insufficient_evidence_result(
            "selection_bias",
            "Insufficient evidence: chosen parameters do not appear across enough splits for rank-persistence analysis.",
            ("selection_bias_score", "winner_luck_index", "cross_split_rank_stability"),
            diagnostics={
                "observed_split_ranks": chosen_split_ranks,
                "missing_splits": missing_splits,
            },
        )

    cross_split_rank_stability = clamp01(1.0 / (1.0 + safe_stdev([float(rank) for rank in chosen_split_ranks])))
    mean_rank_support = safe_mean(normalized_rank_scores)
    top_decile_presence = safe_divide(sum(1 for score in normalized_rank_scores if score >= 0.9), len(normalized_rank_scores), default=0.0)
    winner_luck_index = clamp01(
        safe_mean(
            [
                1.0 - mean_rank_support,
                1.0 - cross_split_rank_stability,
                1.0 - top_decile_presence,
            ]
        )
    )
    selection_bias_score = clamp01(1.0 - winner_luck_index)

    status = "pass"
    if selection_bias_score < 0.45:
        status = "fail"
    elif cross_split_rank_stability < 0.55 or top_decile_presence < 0.5:
        status = "warn"

    summary = (
        f"Winner rank persistence across splits is {cross_split_rank_stability:.2f}; "
        f"luck index {winner_luck_index:.2f} based on ranks {chosen_split_ranks}."
    )
    recommendations = []
    if status == "fail":
        recommendations.append("Chosen winner looks too dependent on lucky split outcomes to trust yet.")
    elif status == "warn":
        recommendations.append("Winner rank drifts across splits; treat selection bias as unresolved.")
    else:
        recommendations.append("Winner rank persistence is credible enough to move into robustness testing.")
    return ValidationModuleResult(
        module_name="selection_bias",
        status=status,
        summary=summary,
        metrics={
            "selection_bias_score": round(selection_bias_score, config.report_precision),
            "winner_luck_index": round(winner_luck_index, config.report_precision),
            "cross_split_rank_stability": round(cross_split_rank_stability, config.report_precision),
            "top_decile_presence": round(top_decile_presence, config.report_precision),
        },
        diagnostics={
            "winner_rank_persistence_across_splits": chosen_split_ranks,
            "normalized_rank_scores": normalized_rank_scores,
            "false_discovery_risk_proxy": winner_luck_index,
            "top_decile_cluster_robustness": top_decile_presence,
            "missing_splits": missing_splits,
        },
        artifacts={
            "split_rank_table": [
                {"split_id": split_id, "rank": split_ranks.get((split_id, chosen_key)), "split_size": split_sizes[split_id]}
                for split_id in sorted(split_sizes)
            ]
        },
        recommendations=recommendations,
    )
