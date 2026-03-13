"""Asia VWAP reclaim contract."""

from collections.abc import Sequence

from ..config_models import StrategySettings
from ..domain.models import Bar, FeaturePacket, StrategyState


def evaluate_asia_vwap_reclaim(
    bar_history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket],
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool]:
    """Return Asia reclaim, hold, and acceptance boolean predicates."""
    if not bar_history:
        raise ValueError("bar_history must include the current completed bar.")
    if len(bar_history) != len(feature_history):
        raise ValueError("bar_history and feature_history must have the same length.")

    current_bar = bar_history[-1]
    current_features = feature_history[-1]
    previous_close = bar_history[-2].close if len(bar_history) >= 2 else current_bar.close

    below_vwap_recently = _below_vwap_recently(bar_history, feature_history, settings.below_vwap_lookback)
    reclaim_range_ok = current_features.bar_range >= settings.min_vwap_bar_range_atr * current_features.atr
    reclaim_vol_ok = (not settings.use_vwap_volume_filter) or current_features.vol_ratio >= settings.min_vwap_vol_ratio
    reclaim_color_ok = (not settings.require_green_reclaim_bar) or current_bar.close > current_bar.open
    reclaim_close_ok = current_bar.close > current_features.vwap + current_features.vwap_buffer

    asia_reclaim_bar_raw = (
        (settings.allow_asia and current_bar.session_asia)
        and settings.enable_asia_vwap_longs
        and below_vwap_recently
        and reclaim_close_ok
        and reclaim_color_ok
        and reclaim_range_ok
        and reclaim_vol_ok
        and current_bar.close > previous_close
    )

    prior_bars_since_asia_reclaim = state.bars_since_asia_reclaim if state.bars_since_asia_reclaim is not None else 1000
    asia_hold_bar = prior_bars_since_asia_reclaim == 0
    asia_hold_close_vwap_ok = (not settings.require_hold_close_above_vwap) or (
        state.asia_reclaim_bar_vwap is not None and current_bar.close >= state.asia_reclaim_bar_vwap
    )
    asia_hold_low_ok = (not settings.require_hold_not_break_reclaim_low) or (
        state.asia_reclaim_bar_low is not None and current_bar.low >= state.asia_reclaim_bar_low
    )
    asia_hold_bar_ok = asia_hold_bar and asia_hold_close_vwap_ok and asia_hold_low_ok

    asia_acceptance_bar = prior_bars_since_asia_reclaim == 1
    asia_acceptance_close_high_ok = (not settings.require_acceptance_close_above_reclaim_high) or (
        state.asia_reclaim_bar_high is not None and current_bar.close > state.asia_reclaim_bar_high
    )
    asia_acceptance_close_vwap_ok = (not settings.require_acceptance_close_above_vwap) or (
        state.asia_reclaim_bar_vwap is not None and current_bar.close > state.asia_reclaim_bar_vwap
    )
    asia_acceptance_bar_ok = (
        asia_acceptance_bar
        and _previous_hold_bar_ok(bar_history, state, settings)
        and asia_acceptance_close_high_ok
        and asia_acceptance_close_vwap_ok
    )

    prior_bars_since_asia_vwap_signal = (
        state.bars_since_asia_vwap_signal if state.bars_since_asia_vwap_signal is not None else 1000
    )
    asia_vwap_long_signal = asia_acceptance_bar_ok and prior_bars_since_asia_vwap_signal > settings.anti_churn_bars

    return {
        "below_vwap_recently": below_vwap_recently,
        "reclaim_range_ok": reclaim_range_ok,
        "reclaim_vol_ok": reclaim_vol_ok,
        "reclaim_color_ok": reclaim_color_ok,
        "reclaim_close_ok": reclaim_close_ok,
        "asia_reclaim_bar_raw": asia_reclaim_bar_raw,
        "asia_hold_bar": asia_hold_bar,
        "asia_hold_close_vwap_ok": asia_hold_close_vwap_ok,
        "asia_hold_low_ok": asia_hold_low_ok,
        "asia_hold_bar_ok": asia_hold_bar_ok,
        "asia_acceptance_bar": asia_acceptance_bar,
        "asia_acceptance_close_high_ok": asia_acceptance_close_high_ok,
        "asia_acceptance_close_vwap_ok": asia_acceptance_close_vwap_ok,
        "asia_acceptance_bar_ok": asia_acceptance_bar_ok,
        "asia_vwap_long_signal": asia_vwap_long_signal,
    }


def _below_vwap_recently(
    bar_history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket],
    lookback: int,
) -> bool:
    recent_bars = bar_history[-lookback:]
    recent_features = feature_history[-lookback:]
    return any(bar.close < feature.vwap for bar, feature in zip(recent_bars, recent_features))


def _previous_hold_bar_ok(bar_history: Sequence[Bar], state: StrategyState, settings: StrategySettings) -> bool:
    if len(bar_history) < 2:
        return False

    hold_bar = bar_history[-2]
    hold_close_vwap_ok = (not settings.require_hold_close_above_vwap) or (
        state.asia_reclaim_bar_vwap is not None and hold_bar.close >= state.asia_reclaim_bar_vwap
    )
    hold_low_ok = (not settings.require_hold_not_break_reclaim_low) or (
        state.asia_reclaim_bar_low is not None and hold_bar.low >= state.asia_reclaim_bar_low
    )
    return hold_close_vwap_ok and hold_low_ok
