"""CSCV/PBO-style overfit-probability adjunct for optimization history."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from math import log

from ..config.schemas import ValidationConfig
from ..data.contracts import OptimizationRun
from ..reporting.models import ValidationModuleResult
from ..utils.stats import clamp01, safe_mean, safe_median
from ._helpers import insufficient_evidence_result, parameter_key, validate_full_history


@dataclass(frozen=True)
class CscvPboAnalysis:
    pbo_probability: float
    rank_collapse_rate: float
    median_oos_relative_rank: float
    median_logit: float
    combination_count: int
    split_ids_used: tuple[str, ...]
    combo_rows: tuple[dict[str, object], ...]

    @property
    def cscv_pbo_score(self) -> float:
        return clamp01(safe_mean([1.0 - self.pbo_probability, self.median_oos_relative_rank]))


def analyze_cscv_pbo(run: OptimizationRun) -> CscvPboAnalysis | None:
    valid, _, _, _ = validate_full_history(
        run,
        minimum_trials=12,
        minimum_unique_parameter_sets=3,
        require_split_history=True,
    )
    if not valid:
        return None

    split_ids = sorted({str(trial.split_id) for trial in run.trials if trial.split_id is not None})
    if len(split_ids) < 4:
        return None
    if len(split_ids) % 2 == 1:
        split_ids = split_ids[:-1]
    if len(split_ids) < 4:
        return None

    by_param_split: dict[tuple[tuple[str, object], ...], dict[str, list[float]]] = {}
    for trial in run.trials:
        if trial.split_id is None or str(trial.split_id) not in split_ids:
            continue
        key = parameter_key(trial.parameter_values)
        by_param_split.setdefault(key, {}).setdefault(str(trial.split_id), []).append(float(trial.objective_value))

    complete_params = {
        key: {split_id: safe_mean(values[split_id]) for split_id in split_ids}
        for key, values in by_param_split.items()
        if all(split_id in values for split_id in split_ids)
    }
    if len(complete_params) < 3:
        return None

    half = len(split_ids) // 2
    combo_rows: list[dict[str, object]] = []
    rel_ranks: list[float] = []
    logits: list[float] = []
    collapses = 0
    for combo_id, train_splits in enumerate(combinations(split_ids, half), start=1):
        train_set = set(train_splits)
        test_splits = tuple(split_id for split_id in split_ids if split_id not in train_set)
        is_scores = {
            key: safe_mean([complete_params[key][split_id] for split_id in train_splits])
            for key in complete_params
        }
        oos_scores = {
            key: safe_mean([complete_params[key][split_id] for split_id in test_splits])
            for key in complete_params
        }
        best_key = max(is_scores, key=is_scores.get)
        oos_value = oos_scores[best_key]
        rank_count = sum(1 for value in oos_scores.values() if oos_value >= value)
        rel_rank = rank_count / (len(oos_scores) + 1.0)
        rel_ranks.append(rel_rank)
        collapse = rel_rank <= 0.5
        if collapse:
            collapses += 1
        clipped = min(0.999999, max(0.000001, rel_rank))
        logits.append(log(clipped / (1.0 - clipped)))
        combo_rows.append(
            {
                "combo_id": combo_id,
                "train_splits": train_splits,
                "test_splits": test_splits,
                "is_best_parameters": dict(best_key),
                "is_best_score": round(is_scores[best_key], 6),
                "oos_score": round(oos_value, 6),
                "oos_relative_rank": round(rel_rank, 6),
                "rank_collapse": collapse,
            }
        )

    combination_count = len(combo_rows)
    return CscvPboAnalysis(
        pbo_probability=collapses / max(1, combination_count),
        rank_collapse_rate=collapses / max(1, combination_count),
        median_oos_relative_rank=safe_median(rel_ranks),
        median_logit=safe_median(logits),
        combination_count=combination_count,
        split_ids_used=tuple(split_ids),
        combo_rows=tuple(combo_rows),
    )


def run_cscv_pbo_module(run: OptimizationRun, config: ValidationConfig) -> ValidationModuleResult:
    valid, reason, points, _ = validate_full_history(
        run,
        minimum_trials=12,
        minimum_unique_parameter_sets=3,
        require_split_history=True,
    )
    split_ids = sorted({str(trial.split_id) for trial in run.trials if trial.split_id is not None})
    if not valid:
        return insufficient_evidence_result(
            "cscv_pbo",
            reason,
            ("cscv_pbo_score", "pbo_probability", "rank_collapse_rate"),
            diagnostics={
                "trial_count": len(run.trials),
                "unique_parameter_sets": len(points),
                "split_count": len(split_ids),
            },
            recommendations=["CSCV/PBO analysis needs broad split-aware optimization history with repeated candidates."],
        )

    analysis = analyze_cscv_pbo(run)
    if analysis is None:
        return insufficient_evidence_result(
            "cscv_pbo",
            "Insufficient evidence: CSCV/PBO analysis needs at least four usable splits and enough complete parameter candidates.",
            ("cscv_pbo_score", "pbo_probability", "rank_collapse_rate"),
            diagnostics={
                "split_count": len(split_ids),
                "usable_even_split_count": len(split_ids) - (len(split_ids) % 2),
            },
        )

    status = "pass"
    if analysis.pbo_probability > 0.5:
        status = "fail"
    elif analysis.pbo_probability > 0.3 or analysis.median_oos_relative_rank < 0.6:
        status = "warn"

    summary = (
        f"CSCV/PBO-style analysis shows overfit probability {analysis.pbo_probability:.2f} across "
        f"{analysis.combination_count} split recombinations; median OOS relative rank {analysis.median_oos_relative_rank:.2f}."
    )
    recommendations: list[str] = []
    if status == "fail":
        recommendations.append("IS winners collapse too often under recombined splits; treat the optimization as materially overfit.")
    elif status == "warn":
        recommendations.append("Overfit probability is elevated; demand more robust support before trusting the winner.")
    else:
        recommendations.append("Rank-collapse evidence is acceptable enough to continue, while keeping it separate from selection bias.")

    return ValidationModuleResult(
        module_name="cscv_pbo",
        status=status,
        summary=summary,
        metrics={
            "cscv_pbo_score": round(analysis.cscv_pbo_score, config.report_precision),
            "pbo_probability": round(analysis.pbo_probability, config.report_precision),
            "rank_collapse_rate": round(analysis.rank_collapse_rate, config.report_precision),
            "median_oos_relative_rank": round(analysis.median_oos_relative_rank, config.report_precision),
            "combination_count": analysis.combination_count,
        },
        diagnostics={
            "split_ids_used": list(analysis.split_ids_used),
            "median_logit": round(analysis.median_logit, config.report_precision),
            "interpretation": "Probability that IS winners land at or below median OOS rank across split recombinations.",
        },
        artifacts={
            "rank_collapse_table": list(analysis.combo_rows[:32]),
        },
        recommendations=recommendations,
    )
