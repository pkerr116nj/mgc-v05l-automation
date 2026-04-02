"""Research-only evaluation layer for EMA momentum tracks.

This module labels two parallel experiment tracks:
- filter track
- math-trigger track

It does not alter production strategy behavior, execution, or entry gating.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from ..domain.models import Bar
from ..persistence.repositories import RepositorySet
from ..persistence.research_models import SignalEvaluationRecord


@dataclass(frozen=True)
class ResearchEvaluationPoint:
    bar_id: str
    baseline_long_context_present: bool
    baseline_short_context_present: bool
    filter_pass_long: bool
    filter_pass_short: bool
    trigger_long_math: bool
    trigger_short_math: bool
    quality_score_long: Decimal
    quality_score_short: Decimal
    size_recommendation_long: Decimal
    size_recommendation_short: Decimal
    warmup_complete: bool


class EMAMomentumResearchEvaluator:
    """Evaluate filter-track and math-trigger-track labels from persisted research features."""

    def __init__(
        self,
        repositories: RepositorySet,
        smoothing_length: int = 3,
        impulse_smoothing_length: int = 3,
        volume_window: int = 20,
    ) -> None:
        self._repositories = repositories
        self._smoothing_length = smoothing_length
        self._impulse_smoothing_length = impulse_smoothing_length
        self._volume_window = volume_window

    def evaluate_and_persist(
        self,
        bars: Sequence[Bar],
        experiment_run_id: int,
    ) -> list[ResearchEvaluationPoint]:
        results: list[ResearchEvaluationPoint] = []
        warmup_bars = max(self._smoothing_length, self._impulse_smoothing_length, self._volume_window, 3)

        for index, bar in enumerate(bars):
            feature_row = self._repositories.derived_features.get_by_bar_id(bar.bar_id, experiment_run_id)
            if feature_row is None:
                raise ValueError(
                    f"Derived features must exist before evaluation for bar {bar.bar_id} and run {experiment_run_id}."
                )

            signal_row = self._repositories.signal_evaluations.get_by_bar_id(bar.bar_id, experiment_run_id)
            if signal_row is None:
                signal_row = SignalEvaluationRecord(
                    bar_id=bar.bar_id,
                    experiment_run_id=experiment_run_id,
                    bull_snap_raw=False,
                    bear_snap_raw=False,
                    asia_vwap_reclaim_raw=False,
                    momentum_compressing_up=False,
                    momentum_turning_positive=False,
                    momentum_compressing_down=False,
                    momentum_turning_negative=False,
                    filter_pass_long=False,
                    filter_pass_short=False,
                    trigger_long_math=False,
                    trigger_short_math=False,
                    warmup_complete=False,
                    created_at=bar.end_ts,
                )

            baseline_long_context_present = bool(signal_row.bull_snap_raw or signal_row.asia_vwap_reclaim_raw)
            baseline_short_context_present = bool(signal_row.bear_snap_raw)
            long_flag_count = int(bool(signal_row.momentum_compressing_up)) + int(bool(signal_row.momentum_turning_positive))
            short_flag_count = int(bool(signal_row.momentum_compressing_down)) + int(bool(signal_row.momentum_turning_negative))

            quality_score_long = Decimal(long_flag_count) / Decimal("2")
            quality_score_short = Decimal(short_flag_count) / Decimal("2")
            size_recommendation_long = quality_score_long
            size_recommendation_short = quality_score_short

            filter_pass_long = baseline_long_context_present and long_flag_count == 2
            filter_pass_short = baseline_short_context_present and short_flag_count == 2
            trigger_long_math = long_flag_count >= 1
            trigger_short_math = short_flag_count >= 1
            warmup_complete = index + 1 >= warmup_bars

            updated_signal_row = SignalEvaluationRecord(
                signal_eval_id=signal_row.signal_eval_id,
                bar_id=signal_row.bar_id,
                experiment_run_id=signal_row.experiment_run_id,
                bull_snap_raw=signal_row.bull_snap_raw,
                bear_snap_raw=signal_row.bear_snap_raw,
                asia_vwap_reclaim_raw=signal_row.asia_vwap_reclaim_raw,
                momentum_compressing_up=signal_row.momentum_compressing_up,
                momentum_turning_positive=signal_row.momentum_turning_positive,
                momentum_compressing_down=signal_row.momentum_compressing_down,
                momentum_turning_negative=signal_row.momentum_turning_negative,
                filter_pass_long=filter_pass_long,
                filter_pass_short=filter_pass_short,
                trigger_long_math=trigger_long_math,
                trigger_short_math=trigger_short_math,
                warmup_complete=warmup_complete,
                compression_long=signal_row.compression_long,
                reclaim_long=signal_row.reclaim_long,
                separation_long=signal_row.separation_long,
                structure_long_candidate=signal_row.structure_long_candidate,
                compression_short=signal_row.compression_short,
                failure_short=signal_row.failure_short,
                separation_short=signal_row.separation_short,
                structure_short_candidate=signal_row.structure_short_candidate,
                quality_score_long=quality_score_long,
                quality_score_short=quality_score_short,
                size_recommendation_long=size_recommendation_long,
                size_recommendation_short=size_recommendation_short,
                created_at=signal_row.created_at,
            )
            self._repositories.signal_evaluations.save(updated_signal_row)

            results.append(
                ResearchEvaluationPoint(
                    bar_id=bar.bar_id,
                    baseline_long_context_present=baseline_long_context_present,
                    baseline_short_context_present=baseline_short_context_present,
                    filter_pass_long=filter_pass_long,
                    filter_pass_short=filter_pass_short,
                    trigger_long_math=trigger_long_math,
                    trigger_short_math=trigger_short_math,
                    quality_score_long=quality_score_long,
                    quality_score_short=quality_score_short,
                    size_recommendation_long=size_recommendation_long,
                    size_recommendation_short=size_recommendation_short,
                    warmup_complete=warmup_complete,
                )
            )

        return results
