"""EMA-based momentum and impulse research features.

This module is additive and research-only. It does not alter production
strategy triggers, entry gating, or execution behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping, Sequence

from ..config_models import StrategySettings
from ..domain.models import Bar, SignalPacket
from ..persistence.repositories import RepositorySet
from ..persistence.research_models import DerivedFeatureRecord, SignalEvaluationRecord


@dataclass(frozen=True)
class EMAMomentumCorePoint:
    index: int
    smoothed_close: Decimal
    momentum_raw: Decimal
    momentum_norm: Decimal
    momentum_delta: Decimal
    momentum_acceleration: Decimal
    volume_ratio: Decimal
    signed_impulse: Decimal
    smoothed_signed_impulse: Decimal
    impulse_delta: Decimal
    momentum_compressing_up: bool
    momentum_turning_positive: bool
    momentum_compressing_down: bool
    momentum_turning_negative: bool
    filter_pass_long: bool
    filter_pass_short: bool
    trigger_long_math: bool
    trigger_short_math: bool


@dataclass(frozen=True)
class EMAMomentumFeaturePoint:
    bar_id: str
    atr: Decimal
    vwap: Decimal
    ema_fast: Decimal
    ema_slow: Decimal
    velocity: Decimal
    velocity_delta: Decimal
    stretch_down: Decimal
    stretch_up: Decimal
    smoothed_close: Decimal
    momentum_raw: Decimal
    momentum_norm: Decimal
    momentum_delta: Decimal
    momentum_acceleration: Decimal
    volume_ratio: Decimal
    signed_impulse: Decimal
    smoothed_signed_impulse: Decimal
    impulse_delta: Decimal
    momentum_compressing_up: bool
    momentum_turning_positive: bool
    momentum_compressing_down: bool
    momentum_turning_negative: bool
    filter_pass_long: bool
    filter_pass_short: bool
    trigger_long_math: bool
    trigger_short_math: bool


@dataclass(frozen=True)
class _BaseFeaturePoint:
    atr: Decimal
    vwap: Decimal
    turn_ema_fast: Decimal
    turn_ema_slow: Decimal
    velocity: Decimal
    velocity_delta: Decimal
    downside_stretch: Decimal
    upside_stretch: Decimal


def compute_ema_momentum_core(
    closes: Sequence[Decimal],
    atr_values: Sequence[Decimal],
    volumes: Sequence[int],
    smoothing_length: int = 3,
    volume_window: int = 20,
    normalization_floor: Decimal = Decimal("0.01"),
    impulse_smoothing_length: int = 3,
) -> list[EMAMomentumCorePoint]:
    """Compute causal EMA momentum and volume-aware impulse features."""
    if not closes:
        return []
    if len(closes) != len(atr_values) or len(closes) != len(volumes):
        raise ValueError("closes, atr_values, and volumes must be aligned.")
    if smoothing_length <= 0 or impulse_smoothing_length <= 0:
        raise ValueError("EMA lengths must be > 0.")
    if volume_window <= 0:
        raise ValueError("volume_window must be > 0.")
    if normalization_floor <= 0:
        raise ValueError("normalization_floor must be > 0.")

    smoothed_closes = _ema_series(closes, smoothing_length)
    momentum_raw_series: list[Decimal] = []
    momentum_norm_series: list[Decimal] = []
    momentum_delta_series: list[Decimal] = []
    momentum_acceleration_series: list[Decimal] = []
    volume_ratio_series: list[Decimal] = []
    signed_impulse_series: list[Decimal] = []

    for index, close in enumerate(closes):
        prior_smoothed_close = smoothed_closes[index - 1] if index > 0 else smoothed_closes[index]
        momentum_raw = smoothed_closes[index] - prior_smoothed_close if index > 0 else Decimal("0")
        momentum_raw_series.append(momentum_raw)

        normalizer = max(atr_values[index], normalization_floor)
        momentum_norm = momentum_raw / normalizer
        momentum_norm_series.append(momentum_norm)

        prior_momentum_raw = momentum_raw_series[index - 1] if index > 0 else Decimal("0")
        momentum_delta = momentum_raw - prior_momentum_raw if index > 0 else Decimal("0")
        momentum_delta_series.append(momentum_delta)

        prior_momentum_delta = momentum_delta_series[index - 1] if index > 0 else Decimal("0")
        momentum_acceleration = momentum_delta - prior_momentum_delta if index > 0 else Decimal("0")
        momentum_acceleration_series.append(momentum_acceleration)

        trailing_volumes = volumes[max(0, index - volume_window + 1) : index + 1]
        avg_volume = sum(Decimal(volume) for volume in trailing_volumes) / Decimal(len(trailing_volumes))
        volume_ratio = Decimal("1") if avg_volume == 0 else Decimal(volumes[index]) / avg_volume
        volume_ratio_series.append(volume_ratio)

        prior_close = closes[index - 1] if index > 0 else close
        signed_impulse = (close - prior_close) * volume_ratio if index > 0 else Decimal("0")
        signed_impulse_series.append(signed_impulse)

    smoothed_signed_impulse_series = _ema_series(signed_impulse_series, impulse_smoothing_length)

    results: list[EMAMomentumCorePoint] = []
    for index in range(len(closes)):
        prior_momentum_norm = momentum_norm_series[index - 1] if index > 0 else momentum_norm_series[index]
        prior_smoothed_impulse = (
            smoothed_signed_impulse_series[index - 1]
            if index > 0
            else smoothed_signed_impulse_series[index]
        )
        impulse_delta = (
            smoothed_signed_impulse_series[index] - prior_smoothed_impulse if index > 0 else Decimal("0")
        )

        momentum_compressing_up = (
            momentum_norm_series[index] < 0
            and momentum_norm_series[index] > prior_momentum_norm
            and momentum_acceleration_series[index] > 0
        )
        momentum_turning_positive = momentum_norm_series[index] >= 0
        momentum_compressing_down = (
            momentum_norm_series[index] > 0
            and momentum_norm_series[index] < prior_momentum_norm
            and momentum_acceleration_series[index] < 0
        )
        momentum_turning_negative = momentum_norm_series[index] <= 0
        filter_pass_long = momentum_compressing_up or momentum_turning_positive
        filter_pass_short = momentum_compressing_down or momentum_turning_negative
        trigger_long_math = momentum_compressing_up and momentum_turning_positive
        trigger_short_math = momentum_compressing_down and momentum_turning_negative

        results.append(
            EMAMomentumCorePoint(
                index=index,
                smoothed_close=smoothed_closes[index],
                momentum_raw=momentum_raw_series[index],
                momentum_norm=momentum_norm_series[index],
                momentum_delta=momentum_delta_series[index],
                momentum_acceleration=momentum_acceleration_series[index],
                volume_ratio=volume_ratio_series[index],
                signed_impulse=signed_impulse_series[index],
                smoothed_signed_impulse=smoothed_signed_impulse_series[index],
                impulse_delta=impulse_delta,
                momentum_compressing_up=momentum_compressing_up,
                momentum_turning_positive=momentum_turning_positive,
                momentum_compressing_down=momentum_compressing_down,
                momentum_turning_negative=momentum_turning_negative,
                filter_pass_long=filter_pass_long,
                filter_pass_short=filter_pass_short,
                trigger_long_math=trigger_long_math,
                trigger_short_math=trigger_short_math,
            )
        )

    return results


class EMAMomentumResearchService:
    """Compute and persist EMA-based momentum research features."""

    def __init__(
        self,
        repositories: RepositorySet,
        settings: StrategySettings,
        smoothing_length: int = 3,
        volume_window: int | None = None,
        normalization_floor: Decimal = Decimal("0.01"),
        impulse_smoothing_length: int = 3,
    ) -> None:
        self._repositories = repositories
        self._settings = settings
        self._smoothing_length = smoothing_length
        self._volume_window = volume_window if volume_window is not None else settings.vol_len
        self._normalization_floor = normalization_floor
        self._impulse_smoothing_length = impulse_smoothing_length

    @property
    def volume_window(self) -> int:
        return self._volume_window

    def compute(self, bars: Sequence[Bar]) -> list[EMAMomentumFeaturePoint]:
        if not bars:
            return []

        base_features = _compute_base_feature_points_incremental(bars, self._settings)

        core_points = compute_ema_momentum_core(
            closes=[bar.close for bar in bars],
            atr_values=[features.atr for features in base_features],
            volumes=[bar.volume for bar in bars],
            smoothing_length=self._smoothing_length,
            volume_window=self._volume_window,
            normalization_floor=self._normalization_floor,
            impulse_smoothing_length=self._impulse_smoothing_length,
        )

        return [
            EMAMomentumFeaturePoint(
                bar_id=bar.bar_id,
                atr=features.atr,
                vwap=features.vwap,
                ema_fast=features.turn_ema_fast,
                ema_slow=features.turn_ema_slow,
                velocity=features.velocity,
                velocity_delta=features.velocity_delta,
                stretch_down=features.downside_stretch,
                stretch_up=features.upside_stretch,
                smoothed_close=core.smoothed_close,
                momentum_raw=core.momentum_raw,
                momentum_norm=core.momentum_norm,
                momentum_delta=core.momentum_delta,
                momentum_acceleration=core.momentum_acceleration,
                volume_ratio=core.volume_ratio,
                signed_impulse=core.signed_impulse,
                smoothed_signed_impulse=core.smoothed_signed_impulse,
                impulse_delta=core.impulse_delta,
                momentum_compressing_up=core.momentum_compressing_up,
                momentum_turning_positive=core.momentum_turning_positive,
                momentum_compressing_down=core.momentum_compressing_down,
                momentum_turning_negative=core.momentum_turning_negative,
                filter_pass_long=core.filter_pass_long,
                filter_pass_short=core.filter_pass_short,
                trigger_long_math=core.trigger_long_math,
                trigger_short_math=core.trigger_short_math,
            )
            for bar, features, core in zip(bars, base_features, core_points)
        ]

    def compute_and_persist(
        self,
        bars: Sequence[Bar],
        experiment_run_id: int,
        signal_packets: Mapping[str, SignalPacket] | None = None,
    ) -> list[EMAMomentumFeaturePoint]:
        points = self.compute(bars)
        packet_lookup = signal_packets or {}

        for bar, point in zip(bars, points):
            raw_signals = packet_lookup.get(bar.bar_id)
            self._repositories.derived_features.save(
                DerivedFeatureRecord(
                    bar_id=bar.bar_id,
                    experiment_run_id=experiment_run_id,
                    atr=point.atr,
                    vwap=point.vwap,
                    ema_fast=point.ema_fast,
                    ema_slow=point.ema_slow,
                    velocity=point.velocity,
                    velocity_delta=point.velocity_delta,
                    stretch_down=point.stretch_down,
                    stretch_up=point.stretch_up,
                    smoothed_close=point.smoothed_close,
                    momentum_raw=point.momentum_raw,
                    momentum_norm=point.momentum_norm,
                    momentum_delta=point.momentum_delta,
                    momentum_acceleration=point.momentum_acceleration,
                    volume_ratio=point.volume_ratio,
                    signed_impulse=point.signed_impulse,
                    smoothed_signed_impulse=point.smoothed_signed_impulse,
                    impulse_delta=point.impulse_delta,
                    created_at=bar.end_ts,
                )
            )
            self._repositories.signal_evaluations.save(
                SignalEvaluationRecord(
                    bar_id=bar.bar_id,
                    experiment_run_id=experiment_run_id,
                    bull_snap_raw=raw_signals.bull_snap_raw if raw_signals is not None else False,
                    bear_snap_raw=raw_signals.bear_snap_raw if raw_signals is not None else False,
                    asia_vwap_reclaim_raw=raw_signals.asia_reclaim_bar_raw if raw_signals is not None else False,
                    momentum_compressing_up=point.momentum_compressing_up,
                    momentum_turning_positive=point.momentum_turning_positive,
                    momentum_compressing_down=point.momentum_compressing_down,
                    momentum_turning_negative=point.momentum_turning_negative,
                    filter_pass_long=point.filter_pass_long,
                    filter_pass_short=point.filter_pass_short,
                    trigger_long_math=point.trigger_long_math,
                    trigger_short_math=point.trigger_short_math,
                    warmup_complete=False,
                    created_at=bar.end_ts,
                )
            )

        return points


def _ema_series(values: Sequence[Decimal], length: int) -> list[Decimal]:
    alpha = Decimal("2") / Decimal(length + 1)
    ema_values: list[Decimal] = []
    for index, value in enumerate(values):
        if index == 0:
            ema_values.append(value)
        else:
            ema_values.append(alpha * value + (Decimal("1") - alpha) * ema_values[index - 1])
    return ema_values


def _compute_base_feature_points_incremental(
    bars: Sequence[Bar],
    settings: StrategySettings,
) -> list[_BaseFeaturePoint]:
    tr_values: list[Decimal] = []
    closes: list[Decimal] = []
    volumes: list[Decimal] = []
    results: list[_BaseFeaturePoint] = []

    session_date = None
    cumulative_volume = Decimal("0")
    cumulative_price_volume = Decimal("0")

    previous_velocity = Decimal("0")

    for index, bar in enumerate(bars):
        closes.append(bar.close)
        volumes.append(Decimal(bar.volume))

        previous_close = bars[index - 1].close if index > 0 else None
        if previous_close is None:
            tr = bar.high - bar.low
        else:
            tr = max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close))
        tr_values.append(tr)

        atr = _rolling_wilders_average(tr_values, settings.atr_len)
        turn_ema_fast = _rolling_exp_average(closes, settings.turn_fast_len)
        turn_ema_slow = _rolling_exp_average(closes, settings.turn_slow_len)
        velocity = turn_ema_fast - turn_ema_slow

        if index == 0:
            previous_velocity = velocity
        velocity_delta = velocity - previous_velocity if index > 0 else Decimal("0")
        previous_velocity = velocity

        local_date = bar.end_ts.astimezone(settings.timezone_info).date()
        if session_date != local_date:
            session_date = local_date
            cumulative_volume = Decimal("0")
            cumulative_price_volume = Decimal("0")
        typical_price = (bar.high + bar.low + bar.close) / Decimal("3")
        cumulative_price_volume += typical_price * Decimal(bar.volume)
        cumulative_volume += Decimal(bar.volume)
        vwap = bar.close if cumulative_volume == 0 else cumulative_price_volume / cumulative_volume

        prior_highs = [prior_bar.high for prior_bar in bars[max(0, index - settings.turn_stretch_lookback) : index]]
        prior_lows = [prior_bar.low for prior_bar in bars[max(0, index - settings.turn_stretch_lookback) : index]]
        downside_stretch = (max(prior_highs) - bar.close) if prior_highs else Decimal("0")
        upside_stretch = (bar.close - min(prior_lows)) if prior_lows else Decimal("0")

        results.append(
            _BaseFeaturePoint(
                atr=atr,
                vwap=vwap,
                turn_ema_fast=turn_ema_fast,
                turn_ema_slow=turn_ema_slow,
                velocity=velocity,
                velocity_delta=velocity_delta,
                downside_stretch=downside_stretch,
                upside_stretch=upside_stretch,
            )
        )

    return results


def _rolling_wilders_average(values: Sequence[Decimal], length: int) -> Decimal:
    relevant_values = values[-max(length, 1) :]
    average = relevant_values[0]
    for value in relevant_values[1:]:
        average = average + (value - average) / Decimal(length)
    return average


def _rolling_exp_average(values: Sequence[Decimal], length: int) -> Decimal:
    relevant_values = values[-max(length, 1) :]
    multiplier = Decimal("2") / Decimal(length + 1)
    ema = relevant_values[0]
    for value in relevant_values[1:]:
        ema = (value - ema) * multiplier + ema
    return ema
