"""Bear Snap signal contract."""

from collections.abc import Sequence
from decimal import Decimal

from ..config_models import StrategySettings
from ..domain.models import Bar, FeaturePacket, StrategyState


def evaluate_bear_snap(
    history: Sequence[Bar],
    features: FeaturePacket,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool]:
    """Return Bear Snap boolean predicates for the current completed bar."""
    if not history:
        raise ValueError("history must include the current completed bar.")

    current_bar = history[-1]
    previous_close = history[-2].close if len(history) >= 2 else current_bar.close

    session_allowed = (
        (settings.allow_asia and current_bar.session_asia)
        or (settings.allow_london and current_bar.session_london)
        or (settings.allow_us and current_bar.session_us)
    )

    bear_snap_up_stretch_ok = features.upside_stretch >= settings.min_bear_snap_up_stretch_atr * features.atr
    bear_snap_range_ok = features.bar_range >= settings.min_bear_snap_bar_range_atr * features.atr
    bear_snap_body_ok = features.body_size >= settings.min_bear_snap_body_atr * features.atr
    bear_snap_close_weak = _close_location_below_threshold(
        current_bar.low,
        current_bar.close,
        features.bar_range,
        settings.max_bear_snap_close_location,
    )
    bear_snap_velocity_ok = features.velocity_delta <= -settings.min_bear_snap_velocity_delta_atr * features.atr
    bear_snap_reversal_bar = (
        current_bar.close < current_bar.open
        and bear_snap_range_ok
        and bear_snap_body_ok
        and bear_snap_close_weak
    )
    bear_snap_location_ok = (
        True
        if not settings.use_bear_snap_location_filter
        else (
            current_bar.close >= features.turn_ema_slow + settings.bear_snap_min_close_vs_slow_ema_atr * features.atr
            and (
                current_bar.close >= features.turn_ema_slow
                if settings.bear_snap_require_close_above_slow_ema
                else True
            )
        )
    )
    bear_snap_raw = (
        settings.use_turn_family
        and bear_snap_up_stretch_ok
        and bear_snap_reversal_bar
        and bear_snap_velocity_ok
        and current_bar.close < previous_close
    )
    bear_snap_turn_candidate = (
        session_allowed
        and settings.enable_bear_snap_shorts
        and bear_snap_raw
        and bear_snap_location_ok
    )
    prior_bars_since_bear_snap = state.bars_since_bear_snap if state.bars_since_bear_snap is not None else 1000
    first_bear_snap_turn = bear_snap_turn_candidate and prior_bars_since_bear_snap > settings.bear_snap_cooldown_bars

    return {
        "bear_snap_up_stretch_ok": bear_snap_up_stretch_ok,
        "bear_snap_range_ok": bear_snap_range_ok,
        "bear_snap_body_ok": bear_snap_body_ok,
        "bear_snap_close_weak": bear_snap_close_weak,
        "bear_snap_velocity_ok": bear_snap_velocity_ok,
        "bear_snap_reversal_bar": bear_snap_reversal_bar,
        "bear_snap_location_ok": bear_snap_location_ok,
        "bear_snap_raw": bear_snap_raw,
        "bear_snap_turn_candidate": bear_snap_turn_candidate,
        "first_bear_snap_turn": first_bear_snap_turn,
    }


def _close_location_below_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    if bar_range <= 0:
        return False
    return close < low + threshold * bar_range
