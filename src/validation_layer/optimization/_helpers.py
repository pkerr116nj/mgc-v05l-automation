"""Internal helpers for optimization-history diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..data.contracts import OptimizationRun, OptimizationTrial
from ..reporting.models import ValidationModuleResult
from ..utils.stats import safe_divide, safe_mean, safe_median

ParameterKey = tuple[tuple[str, Any], ...]


def _freeze_value(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((str(key), _freeze_value(sub_value)) for key, sub_value in sorted(value.items()))
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)
    return value


def parameter_key(parameter_values: dict[str, Any]) -> ParameterKey:
    return tuple((str(name), _freeze_value(value)) for name, value in sorted(parameter_values.items()))


def thaw_parameter_key(key: ParameterKey) -> dict[str, Any]:
    return {name: value for name, value in key}


def is_numeric_parameter(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


@dataclass(frozen=True)
class ParameterPoint:
    parameter_key: ParameterKey
    parameter_values: dict[str, Any]
    objective_values: tuple[float, ...]
    split_ids: tuple[str, ...]
    ranks: tuple[int, ...]

    @property
    def mean_objective(self) -> float:
        return safe_mean(self.objective_values)

    @property
    def median_objective(self) -> float:
        return safe_median(self.objective_values)

    @property
    def trial_count(self) -> int:
        return len(self.objective_values)

    @property
    def split_count(self) -> int:
        return len(set(self.split_ids))


def build_parameter_points(run: OptimizationRun) -> tuple[ParameterPoint, ...]:
    grouped: dict[ParameterKey, dict[str, Any]] = {}
    for trial in run.trials:
        key = parameter_key(trial.parameter_values)
        bucket = grouped.setdefault(
            key,
            {
                "parameter_values": dict(trial.parameter_values),
                "objective_values": [],
                "split_ids": [],
                "ranks": [],
            },
        )
        bucket["objective_values"].append(float(trial.objective_value))
        if trial.split_id is not None:
            bucket["split_ids"].append(trial.split_id)
        if trial.rank is not None:
            bucket["ranks"].append(int(trial.rank))
    return tuple(
        ParameterPoint(
            parameter_key=key,
            parameter_values=bucket["parameter_values"],
            objective_values=tuple(bucket["objective_values"]),
            split_ids=tuple(bucket["split_ids"]),
            ranks=tuple(bucket["ranks"]),
        )
        for key, bucket in grouped.items()
    )


def find_chosen_point(run: OptimizationRun, points: tuple[ParameterPoint, ...]) -> ParameterPoint | None:
    chosen_key = parameter_key(run.chosen_parameters)
    for point in points:
        if point.parameter_key == chosen_key:
            return point
    return None


def objective_values(run: OptimizationRun) -> tuple[float, ...]:
    return tuple(float(trial.objective_value) for trial in run.trials)


def parameter_spans(points: tuple[ParameterPoint, ...]) -> dict[str, tuple[float, float]]:
    numeric_values: dict[str, list[float]] = {}
    for point in points:
        for name, value in point.parameter_values.items():
            if is_numeric_parameter(value):
                numeric_values.setdefault(name, []).append(float(value))
    return {
        name: (min(values), max(values))
        for name, values in numeric_values.items()
        if values
    }


def parameter_distance(left: dict[str, Any], right: dict[str, Any], spans: dict[str, tuple[float, float]]) -> float:
    names = sorted(set(left) | set(right))
    if not names:
        return 0.0
    contributions: list[float] = []
    for name in names:
        left_value = left.get(name)
        right_value = right.get(name)
        if is_numeric_parameter(left_value) and is_numeric_parameter(right_value):
            low, high = spans.get(name, (float(left_value), float(right_value)))
            span = high - low
            if abs(span) < 1e-12:
                contributions.append(0.0 if float(left_value) == float(right_value) else 1.0)
            else:
                contributions.append(min(1.0, abs(float(left_value) - float(right_value)) / span))
        else:
            contributions.append(0.0 if left_value == right_value else 1.0)
    return safe_mean(contributions)


def derive_split_ranks(trials: tuple[OptimizationTrial, ...]) -> dict[tuple[str, ParameterKey], int]:
    grouped: dict[str, list[OptimizationTrial]] = {}
    for trial in trials:
        if trial.split_id is None:
            continue
        grouped.setdefault(trial.split_id, []).append(trial)
    derived: dict[tuple[str, ParameterKey], int] = {}
    for split_id, split_trials in grouped.items():
        ordered = sorted(split_trials, key=lambda item: item.objective_value, reverse=True)
        for index, trial in enumerate(ordered, start=1):
            derived[(split_id, parameter_key(trial.parameter_values))] = int(trial.rank or index)
    return derived


def insufficient_evidence_result(
    module_name: str,
    summary: str,
    score_names: tuple[str, ...],
    diagnostics: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    recommendations: list[str] | None = None,
) -> ValidationModuleResult:
    metrics: dict[str, float | int | str | bool | None] = {name: None for name in score_names}
    metrics["insufficient_evidence"] = True
    return ValidationModuleResult(
        module_name=module_name,
        status="warn",
        summary=summary,
        metrics=metrics,
        diagnostics=diagnostics or {},
        artifacts=artifacts or {},
        recommendations=recommendations or ["Collect complete optimization history before trusting this diagnostic."],
    )


def validate_full_history(
    run: OptimizationRun,
    *,
    minimum_trials: int,
    minimum_unique_parameter_sets: int,
    require_split_history: bool,
) -> tuple[bool, str, tuple[ParameterPoint, ...], ParameterPoint | None]:
    points = build_parameter_points(run)
    chosen = find_chosen_point(run, points)
    if len(run.trials) < minimum_trials:
        return False, f"Insufficient evidence: only {len(run.trials)} trials are available.", points, chosen
    if len(points) < minimum_unique_parameter_sets:
        return (
            False,
            f"Insufficient evidence: only {len(points)} unique parameter sets are available.",
            points,
            chosen,
        )
    if chosen is None:
        return False, "Insufficient evidence: chosen_parameters are not present in the optimization history.", points, chosen
    if require_split_history:
        split_ids = {trial.split_id for trial in run.trials if trial.split_id is not None}
        if len(split_ids) < 2:
            return False, "Insufficient evidence: split-aware history requires at least two splits.", points, chosen
    return True, "", points, chosen
