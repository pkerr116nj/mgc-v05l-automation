"""Bull Snap signal contract."""

from collections.abc import Sequence
from decimal import Decimal

from ..config_models import StrategySettings
from ..domain.models import Bar, FeaturePacket, StrategyState


def evaluate_bull_snap(
    history: Sequence[Bar],
    features: FeaturePacket,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool]:
    """Return Bull Snap boolean predicates for the current completed bar."""
    if not history:
        raise ValueError("history must include the current completed bar.")

    current_bar = history[-1]
    previous_close = history[-2].close if len(history) >= 2 else current_bar.close

    is_asia = current_bar.session_asia
    asia_allowed = settings.allow_asia and is_asia
    london_allowed = settings.allow_london and current_bar.session_london
    us_allowed = settings.allow_us and current_bar.session_us
    non_asia_allowed = london_allowed or us_allowed

    bull_snap_range_threshold = (
        settings.asia_min_snap_bar_range_atr
        if is_asia and settings.use_asia_bull_snap_thresholds
        else settings.min_snap_bar_range_atr
    )
    bull_snap_body_threshold = (
        settings.asia_min_snap_body_atr
        if is_asia and settings.use_asia_bull_snap_thresholds
        else settings.min_snap_body_atr
    )
    bull_snap_velocity_threshold = (
        settings.asia_min_snap_velocity_delta_atr
        if is_asia and settings.use_asia_bull_snap_thresholds
        else settings.min_snap_velocity_delta_atr
    )

    bull_snap_downside_stretch_ok = features.downside_stretch >= settings.min_snap_down_stretch_atr * features.atr
    bull_snap_range_ok = features.bar_range >= bull_snap_range_threshold * features.atr
    bull_snap_body_ok = features.body_size >= bull_snap_body_threshold * features.atr
    bull_snap_close_strong = _close_location_above_threshold(
        current_bar.low,
        current_bar.close,
        features.bar_range,
        settings.min_snap_close_location,
    )
    bull_snap_velocity_ok = features.velocity_delta >= bull_snap_velocity_threshold * features.atr
    bull_snap_reversal_bar = (
        current_bar.close > current_bar.open
        and bull_snap_range_ok
        and bull_snap_body_ok
        and bull_snap_close_strong
    )
    bull_snap_location_ok = (
        True
        if not settings.use_bull_snap_location_filter
        else (
            current_bar.close <= features.turn_ema_slow + settings.bull_snap_max_close_vs_slow_ema_atr * features.atr
            and (
                current_bar.close <= features.turn_ema_slow
                if settings.bull_snap_require_close_below_slow_ema
                else True
            )
        )
    )
    bull_snap_raw = (
        settings.use_turn_family
        and bull_snap_downside_stretch_ok
        and bull_snap_reversal_bar
        and bull_snap_velocity_ok
        and current_bar.close > previous_close
    )
    bull_snap_turn_candidate = (
        (asia_allowed or non_asia_allowed)
        and settings.enable_bull_snap_longs
        and bull_snap_raw
        and bull_snap_location_ok
    )
    prior_bars_since_bull_snap = state.bars_since_bull_snap if state.bars_since_bull_snap is not None else 1000
    first_bull_snap_turn = bull_snap_turn_candidate and prior_bars_since_bull_snap > settings.snap_cooldown_bars

    return {
        "bull_snap_downside_stretch_ok": bull_snap_downside_stretch_ok,
        "bull_snap_range_ok": bull_snap_range_ok,
        "bull_snap_body_ok": bull_snap_body_ok,
        "bull_snap_close_strong": bull_snap_close_strong,
        "bull_snap_velocity_ok": bull_snap_velocity_ok,
        "bull_snap_reversal_bar": bull_snap_reversal_bar,
        "bull_snap_location_ok": bull_snap_location_ok,
        "bull_snap_raw": bull_snap_raw,
        "bull_snap_turn_candidate": bull_snap_turn_candidate,
        "first_bull_snap_turn": first_bull_snap_turn,
    }


def _close_location_above_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    if bar_range <= 0:
        return False
    return close > low + threshold * bar_range
