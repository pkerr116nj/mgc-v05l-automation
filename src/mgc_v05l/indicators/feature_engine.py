"""Feature engine contract."""

from collections.abc import Sequence
from decimal import Decimal
from typing import Optional

from ..config_models import StrategySettings
from ..domain.models import Bar, FeaturePacket, StrategyState
from .swing_tracker import update_swing_state
from .vwap_engine import compute_session_vwap


def compute_features(
    history: Sequence[Bar],
    state: StrategyState,
    settings: StrategySettings,
) -> FeaturePacket:
    """Compute the feature packet for the latest completed bar."""
    if not history:
        raise ValueError("history must include at least one finalized bar.")

    current_bar = history[-1]
    closes = [bar.close for bar in history]
    volumes = [Decimal(bar.volume) for bar in history]

    tr_values = _true_range_series(history)
    atr = _wilders_average(tr_values, settings.atr_len)
    bar_range = current_bar.high - current_bar.low
    body_size = abs(current_bar.close - current_bar.open)
    avg_vol = _simple_average(volumes[-settings.vol_len :])
    vol_ratio = Decimal("1") if avg_vol == 0 else Decimal(current_bar.volume) / avg_vol
    turn_ema_fast = _exp_average(closes, settings.turn_fast_len)
    turn_ema_slow = _exp_average(closes, settings.turn_slow_len)
    velocity = turn_ema_fast - turn_ema_slow
    previous_velocity = _previous_velocity(closes, settings.turn_fast_len, settings.turn_slow_len)
    velocity_delta = velocity - previous_velocity
    vwap = compute_session_vwap(history, settings)
    vwap_buffer = settings.reclaim_close_buffer_atr * atr
    swing_low_confirmed, swing_high_confirmed, last_swing_low, last_swing_high = update_swing_state(
        history,
        state.last_swing_low,
        state.last_swing_high,
    )
    downside_stretch = _downside_stretch(history, settings.turn_stretch_lookback, current_bar.close)
    upside_stretch = _upside_stretch(history, settings.turn_stretch_lookback, current_bar.close)
    bull_close_strong = _close_location_above_threshold(current_bar.low, current_bar.close, bar_range, Decimal("0.65"))
    bear_close_weak = _close_location_below_threshold(current_bar.low, current_bar.close, bar_range, Decimal("0.28"))

    return FeaturePacket(
        bar_id=current_bar.bar_id,
        tr=tr_values[-1],
        atr=atr,
        bar_range=bar_range,
        body_size=body_size,
        avg_vol=avg_vol,
        vol_ratio=vol_ratio,
        turn_ema_fast=turn_ema_fast,
        turn_ema_slow=turn_ema_slow,
        velocity=velocity,
        velocity_delta=velocity_delta,
        vwap=vwap,
        vwap_buffer=vwap_buffer,
        swing_low_confirmed=swing_low_confirmed,
        swing_high_confirmed=swing_high_confirmed,
        last_swing_low=last_swing_low,
        last_swing_high=last_swing_high,
        downside_stretch=downside_stretch,
        upside_stretch=upside_stretch,
        bull_close_strong=bull_close_strong,
        bear_close_weak=bear_close_weak,
    )


def _true_range_series(history: Sequence[Bar]) -> list[Decimal]:
    tr_values: list[Decimal] = []
    previous_close: Optional[Decimal] = None

    for bar in history:
        if previous_close is None:
            tr = bar.high - bar.low
        else:
            tr = max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close))
        tr_values.append(tr)
        previous_close = bar.close

    return tr_values


def _wilders_average(values: Sequence[Decimal], length: int) -> Decimal:
    relevant_values = values[-max(length, 1) :]
    average = relevant_values[0]
    for value in relevant_values[1:]:
        average = average + (value - average) / Decimal(length)
    return average


def _exp_average(values: Sequence[Decimal], length: int) -> Decimal:
    relevant_values = values[-max(length, 1) :]
    multiplier = Decimal("2") / Decimal(length + 1)
    ema = relevant_values[0]
    for value in relevant_values[1:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _previous_velocity(values: Sequence[Decimal], fast_len: int, slow_len: int) -> Decimal:
    if len(values) < 2:
        current_fast = _exp_average(values, fast_len)
        current_slow = _exp_average(values, slow_len)
        return current_fast - current_slow

    prior_values = values[:-1]
    return _exp_average(prior_values, fast_len) - _exp_average(prior_values, slow_len)


def _downside_stretch(history: Sequence[Bar], lookback: int, current_close: Decimal) -> Decimal:
    prior_highs = [bar.high for bar in history[-(lookback + 1) : -1]]
    if not prior_highs:
        return Decimal("0")
    return max(prior_highs) - current_close


def _upside_stretch(history: Sequence[Bar], lookback: int, current_close: Decimal) -> Decimal:
    prior_lows = [bar.low for bar in history[-(lookback + 1) : -1]]
    if not prior_lows:
        return Decimal("0")
    return current_close - min(prior_lows)


def _simple_average(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _close_location_above_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    if bar_range <= 0:
        return False
    return close > low + threshold * bar_range


def _close_location_below_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    if bar_range <= 0:
        return False
    return close < low + threshold * bar_range
