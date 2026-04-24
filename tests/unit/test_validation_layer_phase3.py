from __future__ import annotations

from validation_layer.config.defaults import build_debug_config
from validation_layer.data.contracts import OptimizationRun, OptimizationTrial
from validation_layer.optimization.cscv_pbo import run_cscv_pbo_module
from validation_layer.optimization.parameter_surface import run_parameter_surface_module
from validation_layer.optimization.selection_bias import run_selection_bias_module
from validation_layer.optimization.train_bias import run_training_bias_module
from validation_layer.orchestration.pipeline import run_validation_pipeline


def _ranked_trials(
    split_id: str,
    objective_map: dict[tuple[int, float], float],
) -> list[OptimizationTrial]:
    ordered = sorted(objective_map.items(), key=lambda item: item[1], reverse=True)
    trials: list[OptimizationTrial] = []
    for rank, ((lookback, threshold), objective_value) in enumerate(ordered, start=1):
        trials.append(
            OptimizationTrial(
                parameter_values={"lookback": lookback, "threshold": threshold},
                in_sample_metrics={"objective": objective_value},
                out_of_sample_metrics={},
                split_id=split_id,
                rank=rank,
                objective_value=objective_value,
            )
        )
    return trials


def _build_plateau_run() -> OptimizationRun:
    splits = {
        "split_a": {
            (9, 0.20): 8.6,
            (9, 0.25): 9.0,
            (9, 0.30): 8.5,
            (10, 0.20): 9.1,
            (10, 0.25): 9.6,
            (10, 0.30): 9.0,
            (11, 0.20): 8.7,
            (11, 0.25): 9.1,
            (11, 0.30): 8.6,
        },
        "split_b": {
            (9, 0.20): 8.4,
            (9, 0.25): 8.9,
            (9, 0.30): 8.3,
            (10, 0.20): 8.9,
            (10, 0.25): 9.4,
            (10, 0.30): 8.8,
            (11, 0.20): 8.5,
            (11, 0.25): 8.9,
            (11, 0.30): 8.4,
        },
        "split_c": {
            (9, 0.20): 8.8,
            (9, 0.25): 9.2,
            (9, 0.30): 8.7,
            (10, 0.20): 9.2,
            (10, 0.25): 9.7,
            (10, 0.30): 9.1,
            (11, 0.20): 8.8,
            (11, 0.25): 9.2,
            (11, 0.30): 8.7,
        },
    }
    trials = []
    for split_id, objective_map in splits.items():
        trials.extend(_ranked_trials(split_id, objective_map))
    return OptimizationRun(
        strategy_name="plateau_strategy",
        search_space={"lookback": [9, 10, 11], "threshold": [0.20, 0.25, 0.30]},
        trials=tuple(trials),
        chosen_parameters={"lookback": 10, "threshold": 0.25},
        objective_name="net_pnl",
    )


def _build_spike_run() -> OptimizationRun:
    splits = {
        "split_a": {
            (9, 0.20): 2.0,
            (9, 0.25): 2.1,
            (9, 0.30): 1.9,
            (10, 0.20): 2.4,
            (10, 0.25): 9.8,
            (10, 0.30): 2.2,
            (11, 0.20): 1.8,
            (11, 0.25): 2.0,
            (11, 0.30): 1.7,
        },
        "split_b": {
            (9, 0.20): 2.2,
            (9, 0.25): 2.0,
            (9, 0.30): 1.8,
            (10, 0.20): 2.5,
            (10, 0.25): 10.2,
            (10, 0.30): 2.3,
            (11, 0.20): 1.9,
            (11, 0.25): 2.1,
            (11, 0.30): 1.8,
        },
        "split_c": {
            (9, 0.20): 1.9,
            (9, 0.25): 1.8,
            (9, 0.30): 1.7,
            (10, 0.20): 2.1,
            (10, 0.25): 9.6,
            (10, 0.30): 2.0,
            (11, 0.20): 1.7,
            (11, 0.25): 1.9,
            (11, 0.30): 1.6,
        },
    }
    trials = []
    for split_id, objective_map in splits.items():
        trials.extend(_ranked_trials(split_id, objective_map))
    return OptimizationRun(
        strategy_name="spike_strategy",
        search_space={"lookback": [9, 10, 11], "threshold": [0.20, 0.25, 0.30]},
        trials=tuple(trials),
        chosen_parameters={"lookback": 10, "threshold": 0.25},
        objective_name="net_pnl",
    )


def _build_lucky_winner_run() -> OptimizationRun:
    splits = {
        "split_a": {
            (8, 0.20): 9.0,
            (10, 0.25): 14.0,
            (12, 0.30): 8.0,
            (14, 0.35): 7.0,
        },
        "split_b": {
            (8, 0.20): 8.5,
            (10, 0.25): 4.0,
            (12, 0.30): 8.9,
            (14, 0.35): 8.2,
        },
        "split_c": {
            (8, 0.20): 8.2,
            (10, 0.25): 5.0,
            (12, 0.30): 8.8,
            (14, 0.35): 8.0,
        },
    }
    trials = []
    for split_id, objective_map in splits.items():
        trials.extend(_ranked_trials(split_id, objective_map))
    return OptimizationRun(
        strategy_name="lucky_strategy",
        search_space={"lookback": [8, 10, 12, 14], "threshold": [0.20, 0.25, 0.30, 0.35]},
        trials=tuple(trials),
        chosen_parameters={"lookback": 10, "threshold": 0.25},
        objective_name="net_pnl",
    )


def _build_cscv_plateau_run() -> OptimizationRun:
    splits = {
        "split_a": {(9, 0.20): 8.8, (10, 0.25): 9.4, (11, 0.30): 8.9, (12, 0.35): 8.6},
        "split_b": {(9, 0.20): 8.7, (10, 0.25): 9.3, (11, 0.30): 8.8, (12, 0.35): 8.5},
        "split_c": {(9, 0.20): 8.6, (10, 0.25): 9.2, (11, 0.30): 8.7, (12, 0.35): 8.4},
        "split_d": {(9, 0.20): 8.9, (10, 0.25): 9.5, (11, 0.30): 9.0, (12, 0.35): 8.7},
    }
    trials = []
    for split_id, objective_map in splits.items():
        trials.extend(_ranked_trials(split_id, objective_map))
    return OptimizationRun(
        strategy_name="cscv_plateau",
        search_space={"lookback": [9, 10, 11, 12], "threshold": [0.20, 0.25, 0.30, 0.35]},
        trials=tuple(trials),
        chosen_parameters={"lookback": 10, "threshold": 0.25},
        objective_name="net_pnl",
    )


def _build_cscv_overfit_run() -> OptimizationRun:
    splits = {
        "split_a": {(9, 0.20): 7.5, (10, 0.25): 12.5, (11, 0.30): 8.4, (12, 0.35): 8.2},
        "split_b": {(9, 0.20): 7.6, (10, 0.25): 12.2, (11, 0.30): 8.5, (12, 0.35): 8.3},
        "split_c": {(9, 0.20): 8.9, (10, 0.25): 4.0, (11, 0.30): 8.8, (12, 0.35): 8.7},
        "split_d": {(9, 0.20): 9.0, (10, 0.25): 4.2, (11, 0.30): 8.9, (12, 0.35): 8.8},
    }
    trials = []
    for split_id, objective_map in splits.items():
        trials.extend(_ranked_trials(split_id, objective_map))
    return OptimizationRun(
        strategy_name="cscv_overfit",
        search_space={"lookback": [9, 10, 11, 12], "threshold": [0.20, 0.25, 0.30, 0.35]},
        trials=tuple(trials),
        chosen_parameters={"lookback": 10, "threshold": 0.25},
        objective_name="net_pnl",
    )


def test_parameter_surface_distinguishes_plateau_from_spike() -> None:
    config = build_debug_config()
    plateau = run_parameter_surface_module(_build_plateau_run(), config)
    spike = run_parameter_surface_module(_build_spike_run(), config)

    assert plateau.metrics["parameter_surface_score"] > spike.metrics["parameter_surface_score"]
    assert plateau.artifacts["surface_classification"] == "stable_plateau"
    assert spike.artifacts["surface_classification"] == "narrow_spike"


def test_training_bias_penalizes_spiky_winner_more_than_plateau() -> None:
    config = build_debug_config()
    plateau = run_training_bias_module(_build_plateau_run(), config)
    spike = run_training_bias_module(_build_spike_run(), config)

    assert plateau.metrics["training_bias_score"] > spike.metrics["training_bias_score"]
    assert plateau.metrics["winner_fragility_score"] > spike.metrics["winner_fragility_score"]


def test_selection_bias_penalizes_lucky_winner_with_rank_drift() -> None:
    config = build_debug_config()
    stable = run_selection_bias_module(_build_plateau_run(), config)
    lucky = run_selection_bias_module(_build_lucky_winner_run(), config)

    assert stable.metrics["selection_bias_score"] > lucky.metrics["selection_bias_score"]
    assert lucky.metrics["winner_luck_index"] > stable.metrics["winner_luck_index"]


def test_cscv_pbo_penalizes_rank_collapse_more_than_plateau() -> None:
    config = build_debug_config()
    plateau = run_cscv_pbo_module(_build_cscv_plateau_run(), config)
    overfit = run_cscv_pbo_module(_build_cscv_overfit_run(), config)

    assert plateau.metrics["cscv_pbo_score"] > overfit.metrics["cscv_pbo_score"]
    assert overfit.metrics["pbo_probability"] > plateau.metrics["pbo_probability"]


def test_selection_bias_returns_honest_insufficient_evidence_without_split_history() -> None:
    config = build_debug_config()
    run = OptimizationRun(
        strategy_name="summary_only",
        search_space={"lookback": [5, 10, 15]},
        trials=(
            OptimizationTrial(
                parameter_values={"lookback": 5},
                in_sample_metrics={"objective": 1.0},
                out_of_sample_metrics={},
                split_id=None,
                rank=1,
                objective_value=1.0,
            ),
            OptimizationTrial(
                parameter_values={"lookback": 10},
                in_sample_metrics={"objective": 2.0},
                out_of_sample_metrics={},
                split_id=None,
                rank=1,
                objective_value=2.0,
            ),
            OptimizationTrial(
                parameter_values={"lookback": 15},
                in_sample_metrics={"objective": 1.5},
                out_of_sample_metrics={},
                split_id=None,
                rank=1,
                objective_value=1.5,
            ),
        ),
        chosen_parameters={"lookback": 10},
        objective_name="net_pnl",
    )

    result = run_selection_bias_module(run, config)

    assert result.status == "warn"
    assert result.metrics["insufficient_evidence"] is True
    assert result.metrics["selection_bias_score"] is None
    assert "Insufficient evidence" in result.summary


def test_cscv_pbo_returns_honest_insufficient_evidence_with_too_few_splits() -> None:
    config = build_debug_config()
    result = run_cscv_pbo_module(_build_plateau_run(), config)

    assert result.status == "warn"
    assert result.metrics["insufficient_evidence"] is True
    assert result.metrics["cscv_pbo_score"] is None


def test_pipeline_accepts_optimization_run_directly_as_strategy_validation_input() -> None:
    report = run_validation_pipeline(_build_plateau_run(), build_debug_config())
    module_names = {result.module_name for result in report.module_results}

    assert report.subject_type == "strategy"
    assert report.subject_name == "plateau_strategy"
    assert {"parameter_surface", "train_bias", "selection_bias", "cscv_pbo"}.issubset(module_names)
    assert report.overall_status == "insufficient_evidence"
