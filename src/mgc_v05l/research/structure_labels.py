"""Research-only first-pass structure labels for EMA momentum analysis."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence

from ..domain.models import Bar
from ..persistence.repositories import RepositorySet
from ..persistence.research_models import DerivedFeatureRecord, SignalEvaluationRecord

MICRO_RANGE_LOOKBACK = 3
SEPARATION_ATR_MULTIPLIER = Decimal("0.10")
MIN_SEPARATION_FLOOR = Decimal("0.01")
SEPARATION_LOOKAHEAD_BARS = 1
COMPRESSION_LOOKBACK_BARS = 1


@dataclass(frozen=True)
class StructureLabelPoint:
    bar_id: str
    compression_long: bool
    reclaim_long: bool
    separation_long: bool
    structure_long_candidate: bool
    compression_short: bool
    failure_short: bool
    separation_short: bool
    structure_short_candidate: bool


class EMAStructureResearchLabeler:
    """Assign first-pass causal structure labels from persisted EMA research features."""

    def __init__(
        self,
        repositories: RepositorySet,
        micro_range_lookback: int = MICRO_RANGE_LOOKBACK,
        separation_atr_multiplier: Decimal = SEPARATION_ATR_MULTIPLIER,
        min_separation_floor: Decimal = MIN_SEPARATION_FLOOR,
        separation_lookahead_bars: int = SEPARATION_LOOKAHEAD_BARS,
        compression_lookback_bars: int = COMPRESSION_LOOKBACK_BARS,
    ) -> None:
        self._repositories = repositories
        self._micro_range_lookback = micro_range_lookback
        self._separation_atr_multiplier = separation_atr_multiplier
        self._min_separation_floor = min_separation_floor
        self._separation_lookahead_bars = separation_lookahead_bars
        self._compression_lookback_bars = compression_lookback_bars

    def label_and_persist(
        self,
        bars: Sequence[Bar],
        experiment_run_id: int,
    ) -> list[StructureLabelPoint]:
        results: list[StructureLabelPoint] = []
        last_long_reclaim_level: Decimal | None = None
        last_long_reclaim_index: int | None = None
        last_short_failure_level: Decimal | None = None
        last_short_failure_index: int | None = None

        for index, bar in enumerate(bars):
            feature_row = self._repositories.derived_features.get_by_bar_id(bar.bar_id, experiment_run_id)
            if feature_row is None:
                raise ValueError(
                    f"Derived features must exist before structure labeling for bar {bar.bar_id} and run {experiment_run_id}."
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
                    created_at=bar.end_ts,
                )

            prior_feature = self._feature_at(bars, experiment_run_id, index - 1)
            prior_bar = bars[index - 1] if index > 0 else bar

            compression_long = self._is_compression_long(feature_row, prior_feature)
            compression_short = self._is_compression_short(feature_row, prior_feature)

            recent_high = self._recent_high(bars, index)
            recent_low = self._recent_low(bars, index)

            reclaim_long, reclaim_level = self._is_reclaim_long(bar, prior_bar, feature_row, prior_feature, recent_high)
            failure_short, failure_level = self._is_failure_short(bar, prior_bar, feature_row, prior_feature, recent_low)

            if reclaim_long and reclaim_level is not None:
                last_long_reclaim_level = reclaim_level
                last_long_reclaim_index = index
            if failure_short and failure_level is not None:
                last_short_failure_level = failure_level
                last_short_failure_index = index

            separation_long = self._is_separation_long(
                bar=bar,
                prior_bar=prior_bar,
                feature_row=feature_row,
                prior_feature=prior_feature,
                reclaim_level=last_long_reclaim_level,
                reclaim_index=last_long_reclaim_index,
                index=index,
            )
            separation_short = self._is_separation_short(
                bar=bar,
                prior_bar=prior_bar,
                feature_row=feature_row,
                prior_feature=prior_feature,
                failure_level=last_short_failure_level,
                failure_index=last_short_failure_index,
                index=index,
            )

            recent_long_compression = self._has_recent_true(
                results,
                index=index,
                lookback=self._compression_lookback_bars,
                accessor=lambda row: row.compression_long,
            ) or compression_long
            recent_short_compression = self._has_recent_true(
                results,
                index=index,
                lookback=self._compression_lookback_bars,
                accessor=lambda row: row.compression_short,
            ) or compression_short
            recent_long_reclaim = (
                last_long_reclaim_index is not None and index - last_long_reclaim_index <= self._separation_lookahead_bars
            )
            recent_short_failure = (
                last_short_failure_index is not None and index - last_short_failure_index <= self._separation_lookahead_bars
            )

            structure_long_candidate = recent_long_compression and recent_long_reclaim and separation_long
            structure_short_candidate = recent_short_compression and recent_short_failure and separation_short

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
                filter_pass_long=signal_row.filter_pass_long,
                filter_pass_short=signal_row.filter_pass_short,
                trigger_long_math=signal_row.trigger_long_math,
                trigger_short_math=signal_row.trigger_short_math,
                warmup_complete=signal_row.warmup_complete,
                compression_long=compression_long,
                reclaim_long=reclaim_long,
                separation_long=separation_long,
                structure_long_candidate=structure_long_candidate,
                compression_short=compression_short,
                failure_short=failure_short,
                separation_short=separation_short,
                structure_short_candidate=structure_short_candidate,
                quality_score_long=signal_row.quality_score_long,
                quality_score_short=signal_row.quality_score_short,
                size_recommendation_long=signal_row.size_recommendation_long,
                size_recommendation_short=signal_row.size_recommendation_short,
                created_at=signal_row.created_at,
            )
            self._repositories.signal_evaluations.save(updated_signal_row)

            results.append(
                StructureLabelPoint(
                    bar_id=bar.bar_id,
                    compression_long=compression_long,
                    reclaim_long=reclaim_long,
                    separation_long=separation_long,
                    structure_long_candidate=structure_long_candidate,
                    compression_short=compression_short,
                    failure_short=failure_short,
                    separation_short=separation_short,
                    structure_short_candidate=structure_short_candidate,
                )
            )

        return results

    def _feature_at(
        self,
        bars: Sequence[Bar],
        experiment_run_id: int,
        index: int,
    ) -> DerivedFeatureRecord | None:
        if index < 0:
            return None
        return self._repositories.derived_features.get_by_bar_id(bars[index].bar_id, experiment_run_id)

    def _is_compression_long(
        self,
        feature_row: DerivedFeatureRecord,
        prior_feature: DerivedFeatureRecord | None,
    ) -> bool:
        prior_momentum_norm = prior_feature.momentum_norm if prior_feature is not None else feature_row.momentum_norm
        prior_signed_impulse = prior_feature.signed_impulse if prior_feature is not None else feature_row.signed_impulse
        return (
            feature_row.momentum_norm is not None
            and prior_momentum_norm is not None
            and feature_row.momentum_acceleration is not None
            and feature_row.signed_impulse is not None
            and prior_signed_impulse is not None
            and feature_row.momentum_norm < 0
            and feature_row.momentum_norm > prior_momentum_norm
            and feature_row.momentum_acceleration > 0
            and feature_row.signed_impulse > prior_signed_impulse
        )

    def _is_compression_short(
        self,
        feature_row: DerivedFeatureRecord,
        prior_feature: DerivedFeatureRecord | None,
    ) -> bool:
        prior_momentum_norm = prior_feature.momentum_norm if prior_feature is not None else feature_row.momentum_norm
        prior_signed_impulse = prior_feature.signed_impulse if prior_feature is not None else feature_row.signed_impulse
        return (
            feature_row.momentum_norm is not None
            and prior_momentum_norm is not None
            and feature_row.momentum_acceleration is not None
            and feature_row.signed_impulse is not None
            and prior_signed_impulse is not None
            and feature_row.momentum_norm > 0
            and feature_row.momentum_norm < prior_momentum_norm
            and feature_row.momentum_acceleration < 0
            and feature_row.signed_impulse < prior_signed_impulse
        )

    def _is_reclaim_long(
        self,
        bar: Bar,
        prior_bar: Bar,
        feature_row: DerivedFeatureRecord,
        prior_feature: DerivedFeatureRecord | None,
        recent_high: Decimal | None,
    ) -> tuple[bool, Decimal | None]:
        vwap_reclaim = (
            feature_row.vwap is not None
            and bar.close >= feature_row.vwap
            and prior_feature is not None
            and prior_feature.vwap is not None
            and prior_bar.close < prior_feature.vwap
        )
        micro_range_reclaim = recent_high is not None and bar.close > recent_high
        reclaim_level: Decimal | None = None
        if vwap_reclaim:
            reclaim_level = feature_row.vwap
        elif micro_range_reclaim:
            reclaim_level = recent_high
        return vwap_reclaim or micro_range_reclaim, reclaim_level

    def _is_failure_short(
        self,
        bar: Bar,
        prior_bar: Bar,
        feature_row: DerivedFeatureRecord,
        prior_feature: DerivedFeatureRecord | None,
        recent_low: Decimal | None,
    ) -> tuple[bool, Decimal | None]:
        vwap_failure = (
            feature_row.vwap is not None
            and bar.close <= feature_row.vwap
            and prior_feature is not None
            and prior_feature.vwap is not None
            and prior_bar.close > prior_feature.vwap
        )
        micro_range_failure = recent_low is not None and bar.close < recent_low
        failure_level: Decimal | None = None
        if vwap_failure:
            failure_level = feature_row.vwap
        elif micro_range_failure:
            failure_level = recent_low
        return vwap_failure or micro_range_failure, failure_level

    def _is_separation_long(
        self,
        bar: Bar,
        prior_bar: Bar,
        feature_row: DerivedFeatureRecord,
        prior_feature: DerivedFeatureRecord | None,
        reclaim_level: Decimal | None,
        reclaim_index: int | None,
        index: int,
    ) -> bool:
        if reclaim_level is None or reclaim_index is None or index - reclaim_index > self._separation_lookahead_bars:
            return False
        threshold = self._separation_threshold(feature_row)
        prior_impulse = prior_feature.signed_impulse if prior_feature is not None else feature_row.signed_impulse
        return (
            bar.close >= reclaim_level + threshold
            and bar.low >= reclaim_level
            and bar.close >= prior_bar.close
            and feature_row.signed_impulse is not None
            and prior_impulse is not None
            and feature_row.signed_impulse >= prior_impulse
        )

    def _is_separation_short(
        self,
        bar: Bar,
        prior_bar: Bar,
        feature_row: DerivedFeatureRecord,
        prior_feature: DerivedFeatureRecord | None,
        failure_level: Decimal | None,
        failure_index: int | None,
        index: int,
    ) -> bool:
        if failure_level is None or failure_index is None or index - failure_index > self._separation_lookahead_bars:
            return False
        threshold = self._separation_threshold(feature_row)
        prior_impulse = prior_feature.signed_impulse if prior_feature is not None else feature_row.signed_impulse
        return (
            bar.close <= failure_level - threshold
            and bar.high <= failure_level
            and bar.close <= prior_bar.close
            and feature_row.signed_impulse is not None
            and prior_impulse is not None
            and feature_row.signed_impulse <= prior_impulse
        )

    def _recent_high(self, bars: Sequence[Bar], index: int) -> Decimal | None:
        window = bars[max(0, index - self._micro_range_lookback) : index]
        if not window:
            return None
        return max(bar.high for bar in window)

    def _recent_low(self, bars: Sequence[Bar], index: int) -> Decimal | None:
        window = bars[max(0, index - self._micro_range_lookback) : index]
        if not window:
            return None
        return min(bar.low for bar in window)

    def _separation_threshold(self, feature_row: DerivedFeatureRecord) -> Decimal:
        atr = feature_row.atr if feature_row.atr is not None else Decimal("0")
        return max(self._min_separation_floor, atr * self._separation_atr_multiplier)

    def _has_recent_true(
        self,
        rows: Sequence[StructureLabelPoint],
        index: int,
        lookback: int,
        accessor,
    ) -> bool:
        start = max(0, index - lookback)
        return any(accessor(row) for row in rows[start:index])
