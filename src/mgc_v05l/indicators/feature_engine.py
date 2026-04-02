"""Feature engine contract."""

from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from ..config_models import StrategySettings
from ..domain.models import Bar, FeaturePacket, StrategyState
from .swing_tracker import update_swing_state
from .vwap_engine import compute_session_vwap


@dataclass
class IncrementalFeatureComputer:
    """Incremental feature calculator preserving current feature math."""

    settings: StrategySettings

    def __post_init__(self) -> None:
        close_window = max(self.settings.turn_fast_len, self.settings.turn_slow_len) + 1
        bar_window = max(self.settings.turn_stretch_lookback + 1, 3)
        self._closes: deque[Decimal] = deque(maxlen=close_window)
        self._volumes: deque[Decimal] = deque(maxlen=self.settings.vol_len)
        self._tr_values: deque[Decimal] = deque(maxlen=self.settings.atr_len)
        self._recent_bars: deque[Bar] = deque(maxlen=bar_window)
        self._session_date = None
        self._session_cumulative_volume = Decimal("0")
        self._session_cumulative_price_volume = Decimal("0")

    def compute_next(self, bar: Bar, state: StrategyState) -> FeaturePacket:
        previous_close = self._recent_bars[-1].close if self._recent_bars else None
        tr = (
            bar.high - bar.low
            if previous_close is None
            else max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close))
        )
        self._tr_values.append(tr)
        atr = _wilders_average(list(self._tr_values), self.settings.atr_len)

        bar_range = bar.high - bar.low
        body_size = abs(bar.close - bar.open)

        self._volumes.append(Decimal(bar.volume))
        avg_vol = _simple_average(list(self._volumes))
        vol_ratio = Decimal("1") if avg_vol == 0 else Decimal(bar.volume) / avg_vol

        self._closes.append(bar.close)
        close_values = list(self._closes)
        turn_ema_fast = _exp_average(close_values, self.settings.turn_fast_len)
        turn_ema_slow = _exp_average(close_values, self.settings.turn_slow_len)
        velocity = turn_ema_fast - turn_ema_slow
        if len(close_values) < 2:
            previous_velocity = velocity
        else:
            prior_close_values = close_values[:-1]
            previous_velocity = _exp_average(prior_close_values, self.settings.turn_fast_len) - _exp_average(
                prior_close_values,
                self.settings.turn_slow_len,
            )
        velocity_delta = velocity - previous_velocity

        local_session_date = bar.end_ts.astimezone(self.settings.timezone_info).date()
        if self._session_date != local_session_date:
            self._session_date = local_session_date
            self._session_cumulative_volume = Decimal("0")
            self._session_cumulative_price_volume = Decimal("0")
        typical_price = (bar.high + bar.low + bar.close) / Decimal("3")
        volume_decimal = Decimal(bar.volume)
        self._session_cumulative_price_volume += typical_price * volume_decimal
        self._session_cumulative_volume += volume_decimal
        vwap = (
            bar.close
            if self._session_cumulative_volume == 0
            else self._session_cumulative_price_volume / self._session_cumulative_volume
        )
        vwap_buffer = self.settings.reclaim_close_buffer_atr * atr

        recent_history = [*self._recent_bars, bar]
        swing_low_confirmed, swing_high_confirmed, last_swing_low, last_swing_high = update_swing_state(
            recent_history,
            state.last_swing_low,
            state.last_swing_high,
        )
        downside_stretch = _downside_stretch(recent_history, self.settings.turn_stretch_lookback, bar.close)
        upside_stretch = _upside_stretch(recent_history, self.settings.turn_stretch_lookback, bar.close)
        bull_close_strong = _close_location_above_threshold(bar.low, bar.close, bar_range, Decimal("0.65"))
        bear_close_weak = _close_location_below_threshold(bar.low, bar.close, bar_range, Decimal("0.28"))

        self._recent_bars.append(bar)

        return FeaturePacket(
            bar_id=bar.bar_id,
            tr=tr,
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
