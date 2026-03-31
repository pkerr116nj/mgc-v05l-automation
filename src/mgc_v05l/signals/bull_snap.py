"""Bull Snap signal contract."""

from collections.abc import Sequence
from datetime import time
from decimal import Decimal

from ..app.session_phase_labels import label_session_phase
from ..config_models import StrategySettings
from ..indicators.feature_engine import compute_features
from ..domain.models import Bar, FeaturePacket, StrategyState


def evaluate_bull_snap(
    history: Sequence[Bar],
    features: FeaturePacket,
    state: StrategyState,
    settings: StrategySettings,
    feature_history: Sequence[FeaturePacket] | None = None,
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
    if not is_asia and not settings.enable_first_bull_snap_us_london:
        non_asia_allowed = False

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
    normalizer = max(features.atr, settings.risk_floor)
    normalized_slope = features.velocity / normalizer
    normalized_curvature = features.velocity_delta / normalizer
    derivative_phase = label_session_phase(current_bar.end_ts)
    current_bar_time = current_bar.end_ts.astimezone(settings.timezone_info).time()
    gc_mgc_london_open_extension = (
        settings.allow_asia
        and str(current_bar.symbol or "").upper() in {"GC", "MGC"}
        and derivative_phase == "LONDON_OPEN"
        and current_bar_time in {time(3, 5), time(3, 10), time(3, 15)}
    )
    asia_early_or_gc_mgc_london_open = (
        (current_bar.session_asia and derivative_phase == "ASIA_EARLY" and is_asia)
        or gc_mgc_london_open_extension
    )
    prior_bars_since_long_setup = state.bars_since_long_setup if state.bars_since_long_setup is not None else 1000
    midday_long_context = _compute_midday_long_recent_context(history, feature_history, state, settings)
    us_late_long_context = _compute_us_late_long_recent_context(history, feature_history, state, settings)
    us_late_failed_move_reversal_long_context = _compute_us_late_failed_move_reversal_long_context(
        history,
        feature_history,
        state,
        settings,
    )
    us_late_breakout_long_context = _compute_us_late_breakout_retest_hold_context(
        history,
        feature_history,
        state,
        settings,
    )
    asia_early_breakout_long_context = _compute_asia_early_breakout_retest_hold_context(
        history,
        feature_history,
        state,
        settings,
    )
    asia_late_long_context = _compute_asia_late_long_recent_context(history, feature_history, state, settings)
    midday_pause_resume_long_slope_ok = (
        settings.us_midday_pause_resume_long_min_normalized_slope
        <= normalized_slope
        <= settings.us_midday_pause_resume_long_max_normalized_slope
    )
    midday_pause_resume_long_curvature_ok = (
        settings.us_midday_pause_resume_long_min_normalized_curvature
        <= normalized_curvature
        <= settings.us_midday_pause_resume_long_max_normalized_curvature
    )
    midday_pause_resume_long_range_expansion_ok = (
        midday_long_context["signal_range_expansion_ratio"]
        < settings.us_midday_pause_resume_long_max_range_expansion_ratio
    )
    midday_pause_resume_long_rebound_below_slow_ok = (
        True
        if not settings.us_midday_pause_resume_long_require_rebound_below_slow_ema
        else midday_long_context["signal_is_rebound_below_slow"]
    )
    midday_pause_resume_long_pullback_ok = (
        True
        if not settings.us_midday_pause_resume_long_require_one_bar_pullback
        else midday_long_context["one_bar_pullback_before_signal"]
    )
    midday_pause_resume_long_break_ok = (
        True
        if not settings.us_midday_pause_resume_long_require_break_above_prior_1_high
        else midday_long_context["signal_breaks_prior_1_high"]
    )
    midday_pause_resume_long_turn_candidate = (
        settings.enable_us_midday_pause_resume_longs
        and not first_bull_snap_turn
        and current_bar.session_us
        and derivative_phase == "US_MIDDAY"
        and not is_asia
        and settings.allow_us
        and current_bar.close > current_bar.open
        and current_bar.close > previous_close
        and bull_snap_close_strong
        and midday_pause_resume_long_slope_ok
        and midday_pause_resume_long_curvature_ok
        and midday_pause_resume_long_range_expansion_ok
        and midday_pause_resume_long_rebound_below_slow_ok
        and midday_pause_resume_long_pullback_ok
        and midday_pause_resume_long_break_ok
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    us_late_pause_resume_long_turn_candidate = (
        settings.enable_us_late_pause_resume_longs
        and not first_bull_snap_turn
        and current_bar.session_us
        and derivative_phase == "US_LATE"
        and not is_asia
        and settings.allow_us
        and current_bar.close > current_bar.open
        and current_bar.close > previous_close
        and bull_snap_close_strong
        and normalized_curvature >= settings.us_late_pause_resume_long_min_resumption_curvature
        and us_late_long_context["setup_bar_curvature_is_positive"]
        and (
            us_late_long_context["signal_range_expansion_ratio"]
            < settings.us_late_pause_resume_long_max_range_expansion_ratio
        )
        and us_late_long_context["one_bar_pullback_before_signal"]
        and us_late_long_context["signal_breaks_prior_1_high"]
        and us_late_long_context["signal_ema_location_ok"]
        and not (
            settings.us_late_pause_resume_long_exclude_1755_carryover
            and current_bar_time == time(16, 55)
        )
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    us_late_failed_move_reversal_long_turn_candidate = (
        settings.enable_us_late_failed_move_reversal_longs
        and not first_bull_snap_turn
        and current_bar.session_us
        and derivative_phase == "US_LATE"
        and not is_asia
        and settings.allow_us
        and current_bar.close > current_bar.open
        and current_bar.close > previous_close
        and bull_snap_close_strong
        and normalized_curvature >= settings.us_late_failed_move_reversal_long_failed_move_curvature_min
        and us_late_failed_move_reversal_long_context["failed_move_breaks_prior_1_low"]
        and us_late_failed_move_reversal_long_context["reversal_closes_back_above_failed_move_low"]
        and us_late_failed_move_reversal_long_context["reversal_closes_above_failed_move_close"]
        and us_late_failed_move_reversal_long_context["failed_move_curvature_is_positive"]
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    us_late_breakout_retest_hold_long_turn_candidate = (
        settings.enable_us_late_breakout_retest_hold_longs
        and not first_bull_snap_turn
        and current_bar.session_us
        and derivative_phase == "US_LATE"
        and not is_asia
        and settings.allow_us
        and us_late_breakout_long_context["breakout_bar_expansion_is_normal"]
        and us_late_breakout_long_context["signal_bar_expansion_is_expanded"]
        and us_late_breakout_long_context["breakout_breaks_prior_1_high"]
        and us_late_breakout_long_context["signal_retests_and_holds_breakout_level"]
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    asia_early_breakout_retest_hold_long_turn_candidate = (
        settings.enable_asia_early_breakout_retest_hold_longs
        and not first_bull_snap_turn
        and asia_early_or_gc_mgc_london_open
        and settings.allow_asia
        and asia_early_breakout_long_context["breakout_bar_slope_is_flat"]
        and asia_early_breakout_long_context["breakout_breaks_prior_1_high"]
        and asia_early_breakout_long_context["signal_retests_and_holds_breakout_level"]
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    asia_early_normal_breakout_retest_hold_long_turn_candidate = (
        settings.enable_asia_early_normal_breakout_retest_hold_longs
        and not first_bull_snap_turn
        and asia_early_or_gc_mgc_london_open
        and settings.allow_asia
        and asia_early_breakout_long_context["breakout_bar_slope_is_flat"]
        and asia_early_breakout_long_context["breakout_bar_expansion_is_normal"]
        and asia_early_breakout_long_context["breakout_breaks_prior_1_high"]
        and asia_early_breakout_long_context["signal_retests_and_holds_breakout_level"]
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    asia_late_pause_resume_long_turn_candidate = (
        settings.enable_asia_late_pause_resume_longs
        and not first_bull_snap_turn
        and current_bar.session_asia
        and derivative_phase == "ASIA_LATE"
        and is_asia
        and settings.allow_asia
        and current_bar.close > current_bar.open
        and current_bar.close > previous_close
        and bull_snap_close_strong
        and asia_late_long_context["one_bar_pullback_before_signal"]
        and asia_late_long_context["signal_breaks_prior_1_high"]
        and (
            asia_late_long_context["pullback_range_expansion_ratio"]
            < settings.asia_late_pause_resume_long_pullback_max_range_expansion_ratio
        )
        and (
            asia_late_long_context["signal_range_expansion_ratio"]
            > settings.asia_late_pause_resume_long_signal_min_range_expansion_ratio
        )
        and (
            asia_late_long_context["signal_range_expansion_ratio"]
            < settings.asia_late_pause_resume_long_signal_max_range_expansion_ratio
        )
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    asia_late_flat_pullback_pause_resume_long_turn_candidate = (
        settings.enable_asia_late_flat_pullback_pause_resume_longs
        and not first_bull_snap_turn
        and current_bar.session_asia
        and derivative_phase == "ASIA_LATE"
        and is_asia
        and settings.allow_asia
        and current_bar.close > current_bar.open
        and current_bar.close > previous_close
        and bull_snap_close_strong
        and asia_late_long_context["one_bar_pullback_before_signal"]
        and asia_late_long_context["signal_breaks_prior_1_high"]
        and (
            asia_late_long_context["pullback_range_expansion_ratio"]
            < settings.asia_late_pause_resume_long_pullback_max_range_expansion_ratio
        )
        and (
            asia_late_long_context["signal_range_expansion_ratio"]
            > settings.asia_late_pause_resume_long_signal_min_range_expansion_ratio
        )
        and (
            asia_late_long_context["signal_range_expansion_ratio"]
            < settings.asia_late_pause_resume_long_signal_max_range_expansion_ratio
        )
        and (
            abs(asia_late_long_context["pullback_normalized_curvature"])
            <= settings.asia_late_pause_resume_long_pullback_curvature_flat_threshold
        )
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )
    asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate = (
        settings.enable_asia_late_compressed_flat_pullback_pause_resume_longs
        and not first_bull_snap_turn
        and current_bar.session_asia
        and derivative_phase == "ASIA_LATE"
        and is_asia
        and settings.allow_asia
        and current_bar.close > current_bar.open
        and current_bar.close > previous_close
        and bull_snap_close_strong
        and asia_late_long_context["one_bar_pullback_before_signal"]
        and asia_late_long_context["signal_breaks_prior_1_high"]
        and (
            asia_late_long_context["setup_range_expansion_ratio"]
            <= settings.asia_late_pause_resume_long_setup_max_range_expansion_ratio
        )
        and (
            asia_late_long_context["pullback_range_expansion_ratio"]
            < settings.asia_late_pause_resume_long_pullback_max_range_expansion_ratio
        )
        and (
            asia_late_long_context["signal_range_expansion_ratio"]
            > settings.asia_late_pause_resume_long_signal_min_range_expansion_ratio
        )
        and (
            asia_late_long_context["signal_range_expansion_ratio"]
            < settings.asia_late_pause_resume_long_signal_max_range_expansion_ratio
        )
        and (
            abs(asia_late_long_context["pullback_normalized_curvature"])
            <= settings.asia_late_pause_resume_long_pullback_curvature_flat_threshold
        )
        and prior_bars_since_long_setup > settings.anti_churn_bars
    )

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
        "midday_pause_resume_long_turn_candidate": midday_pause_resume_long_turn_candidate,
        "us_late_pause_resume_long_turn_candidate": us_late_pause_resume_long_turn_candidate,
        "us_late_failed_move_reversal_long_turn_candidate": us_late_failed_move_reversal_long_turn_candidate,
        "us_late_breakout_retest_hold_long_turn_candidate": us_late_breakout_retest_hold_long_turn_candidate,
        "asia_early_breakout_retest_hold_long_turn_candidate": asia_early_breakout_retest_hold_long_turn_candidate,
        "asia_early_normal_breakout_retest_hold_long_turn_candidate": (
            asia_early_normal_breakout_retest_hold_long_turn_candidate
        ),
        "asia_late_pause_resume_long_turn_candidate": asia_late_pause_resume_long_turn_candidate,
        "asia_late_flat_pullback_pause_resume_long_turn_candidate": asia_late_flat_pullback_pause_resume_long_turn_candidate,
        "asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate": (
            asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate
        ),
    }


def _close_location_above_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    if bar_range <= 0:
        return False
    return close > low + threshold * bar_range


def _compute_asia_early_breakout_retest_hold_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool]:
    if len(history) < 3:
        return {
            "breakout_bar_slope_is_flat": False,
            "breakout_bar_expansion_is_normal": False,
            "breakout_breaks_prior_1_high": False,
            "signal_retests_and_holds_breakout_level": False,
        }

    prior_bar = history[-3]
    breakout_bar = history[-2]
    signal_bar = history[-1]

    breakout_feature: FeaturePacket | None = None
    if feature_history is not None and len(feature_history) == len(history):
        breakout_feature = feature_history[-2]
    else:
        breakout_history = history[:-1]
        if breakout_history:
            breakout_feature = compute_features(breakout_history, state, settings)

    breakout_normalized_slope = Decimal("0")
    breakout_range_expansion_ratio = Decimal("0")
    if breakout_feature is not None:
        breakout_normalized_slope = breakout_feature.velocity / max(breakout_feature.atr, settings.risk_floor)
        breakout_range_expansion_ratio = (
            (breakout_bar.high - breakout_bar.low) / breakout_feature.atr if breakout_feature.atr > 0 else Decimal("0")
        )

    breakout_level = breakout_bar.high
    return {
        "breakout_bar_slope_is_flat": (
            abs(breakout_normalized_slope) <= settings.asia_early_breakout_retest_hold_breakout_abs_slope_max
        ),
        "breakout_bar_expansion_is_normal": (
            breakout_range_expansion_ratio > settings.asia_early_breakout_retest_hold_breakout_min_range_expansion_ratio
            and breakout_range_expansion_ratio < settings.asia_early_breakout_retest_hold_breakout_max_range_expansion_ratio
        ),
        "breakout_breaks_prior_1_high": breakout_bar.high > prior_bar.high and breakout_bar.close >= prior_bar.close,
        "signal_retests_and_holds_breakout_level": signal_bar.low <= breakout_level and signal_bar.close >= breakout_level,
    }


def _compute_us_late_breakout_retest_hold_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool]:
    if len(history) < 3:
        return {
            "breakout_bar_expansion_is_normal": False,
            "signal_bar_expansion_is_expanded": False,
            "breakout_breaks_prior_1_high": False,
            "signal_retests_and_holds_breakout_level": False,
        }

    prior_bar = history[-3]
    breakout_bar = history[-2]
    signal_bar = history[-1]

    breakout_feature: FeaturePacket | None = None
    signal_feature: FeaturePacket | None = None
    if feature_history is not None and len(feature_history) == len(history):
        breakout_feature = feature_history[-2]
        signal_feature = feature_history[-1]
    else:
        breakout_history = history[:-1]
        if breakout_history:
            breakout_feature = compute_features(breakout_history, state, settings)
        signal_feature = compute_features(history, state, settings)

    breakout_range_expansion_ratio = Decimal("0")
    signal_range_expansion_ratio = Decimal("0")
    if breakout_feature is not None and breakout_feature.atr > 0:
        breakout_range_expansion_ratio = (breakout_bar.high - breakout_bar.low) / breakout_feature.atr
    if signal_feature is not None and signal_feature.atr > 0:
        signal_range_expansion_ratio = (signal_bar.high - signal_bar.low) / signal_feature.atr

    breakout_level = breakout_bar.high
    return {
        "breakout_bar_expansion_is_normal": (
            breakout_range_expansion_ratio > settings.us_late_breakout_retest_hold_breakout_min_range_expansion_ratio
            and breakout_range_expansion_ratio < settings.us_late_breakout_retest_hold_breakout_max_range_expansion_ratio
        ),
        "signal_bar_expansion_is_expanded": (
            signal_range_expansion_ratio >= settings.us_late_breakout_retest_hold_retest_min_range_expansion_ratio
        ),
        "breakout_breaks_prior_1_high": breakout_bar.high > prior_bar.high and breakout_bar.close >= prior_bar.close,
        "signal_retests_and_holds_breakout_level": signal_bar.low <= breakout_level and signal_bar.close >= breakout_level,
    }


def _compute_midday_long_recent_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, Decimal | bool]:
    if len(history) < 2:
        return {
            "signal_range_expansion_ratio": Decimal("0"),
            "signal_is_rebound_below_slow": False,
            "one_bar_pullback_before_signal": False,
            "signal_breaks_prior_1_high": False,
        }

    if feature_history is not None and len(feature_history) == len(history):
        current_feature = feature_history[-1] if feature_history else None
        current_bar = history[-1]
        return {
            "signal_range_expansion_ratio": (
                (current_bar.high - current_bar.low) / current_feature.atr
                if current_feature is not None and current_feature.atr > 0
                else Decimal("0")
            ),
            "signal_is_rebound_below_slow": (
                current_feature is not None
                and current_feature.turn_ema_fast < current_feature.turn_ema_slow
                and current_bar.close > current_feature.turn_ema_fast
                and current_bar.close <= current_feature.turn_ema_slow
            ),
            "one_bar_pullback_before_signal": len(history) >= 3 and history[-2].close < history[-3].close,
            "signal_breaks_prior_1_high": history[-1].high > history[-2].high,
        }

    current_features = compute_features(history, state, settings)
    current_bar = history[-1]
    return {
        "signal_range_expansion_ratio": (
            (current_bar.high - current_bar.low) / current_features.atr
            if current_features.atr > 0
            else Decimal("0")
        ),
        "signal_is_rebound_below_slow": (
            current_features.turn_ema_fast < current_features.turn_ema_slow
            and current_bar.close > current_features.turn_ema_fast
            and current_bar.close <= current_features.turn_ema_slow
        ),
        "one_bar_pullback_before_signal": len(history) >= 3 and history[-2].close < history[-3].close,
        "signal_breaks_prior_1_high": history[-1].high > history[-2].high,
    }


def _compute_us_late_long_recent_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, Decimal | bool]:
    if len(history) < 3:
        return {
            "signal_range_expansion_ratio": Decimal("0"),
            "one_bar_pullback_before_signal": False,
            "signal_breaks_prior_1_high": False,
            "signal_ema_location_ok": False,
            "setup_bar_curvature_is_positive": False,
        }

    current_bar = history[-1]
    if feature_history is not None and len(feature_history) == len(history):
        current_feature = feature_history[-1]
        setup_feature = feature_history[-3]
        normalizer = max(setup_feature.atr, settings.risk_floor)
        setup_curvature = setup_feature.velocity_delta / normalizer
        return {
            "signal_range_expansion_ratio": (
                (current_bar.high - current_bar.low) / current_feature.atr
                if current_feature.atr > 0
                else Decimal("0")
            ),
            "one_bar_pullback_before_signal": history[-2].close < history[-3].close,
            "signal_breaks_prior_1_high": history[-1].high > history[-2].high,
            "signal_ema_location_ok": current_feature.turn_ema_fast < current_feature.turn_ema_slow
            and current_bar.close > current_feature.turn_ema_fast
            and current_bar.close <= current_feature.turn_ema_slow
            or (
                current_feature.turn_ema_fast > current_feature.turn_ema_slow
                and current_bar.close >= current_feature.turn_ema_fast
                and current_bar.close >= current_feature.turn_ema_slow
            ),
            "setup_bar_curvature_is_positive": setup_curvature >= settings.us_late_pause_resume_long_setup_curvature_min,
        }

    current_features = compute_features(history, state, settings)
    setup_history = history[:-2]
    setup_features = compute_features(setup_history, state, settings) if setup_history else None
    setup_curvature = Decimal("0")
    if setup_features is not None:
        setup_normalizer = max(setup_features.atr, settings.risk_floor)
        setup_curvature = setup_features.velocity_delta / setup_normalizer

    return {
        "signal_range_expansion_ratio": (
            (current_bar.high - current_bar.low) / current_features.atr
            if current_features.atr > 0
            else Decimal("0")
        ),
        "one_bar_pullback_before_signal": history[-2].close < history[-3].close,
        "signal_breaks_prior_1_high": history[-1].high > history[-2].high,
        "signal_ema_location_ok": (
            (
                current_features.turn_ema_fast < current_features.turn_ema_slow
                and current_bar.close > current_features.turn_ema_fast
                and current_bar.close <= current_features.turn_ema_slow
            )
            or (
                current_features.turn_ema_fast > current_features.turn_ema_slow
                and current_bar.close >= current_features.turn_ema_fast
                and current_bar.close >= current_features.turn_ema_slow
            )
        ),
        "setup_bar_curvature_is_positive": setup_curvature >= settings.us_late_pause_resume_long_setup_curvature_min,
    }


def _compute_us_late_failed_move_reversal_long_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, Decimal | bool]:
    if len(history) < 3:
        return {
            "failed_move_breaks_prior_1_low": False,
            "reversal_closes_back_above_failed_move_low": False,
            "reversal_closes_above_failed_move_close": False,
            "failed_move_curvature_is_positive": False,
        }

    prior_bar = history[-3]
    failed_move_bar = history[-2]
    reversal_bar = history[-1]

    if feature_history is not None and len(feature_history) == len(history):
        failed_move_feature = feature_history[-2]
    else:
        failed_move_feature = compute_features(history[:-1], state, settings)

    failed_move_curvature = Decimal("0")
    if failed_move_feature.atr > 0:
        failed_move_curvature = failed_move_feature.velocity_delta / max(failed_move_feature.atr, settings.risk_floor)

    return {
        "failed_move_breaks_prior_1_low": failed_move_bar.low < prior_bar.low,
        "reversal_closes_back_above_failed_move_low": reversal_bar.close > failed_move_bar.low,
        "reversal_closes_above_failed_move_close": reversal_bar.close > failed_move_bar.close,
        "failed_move_curvature_is_positive": (
            failed_move_curvature >= settings.us_late_failed_move_reversal_long_failed_move_curvature_min
        ),
    }


def _compute_asia_late_long_recent_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, Decimal | bool]:
    if len(history) < 3:
        return {
            "setup_range_expansion_ratio": Decimal("0"),
            "pullback_range_expansion_ratio": Decimal("0"),
            "signal_range_expansion_ratio": Decimal("0"),
            "pullback_normalized_curvature": Decimal("0"),
            "one_bar_pullback_before_signal": False,
            "signal_breaks_prior_1_high": False,
        }

    setup_bar = history[-3]
    pullback_bar = history[-2]
    signal_bar = history[-1]
    if feature_history is not None and len(feature_history) == len(history):
        setup_feature = feature_history[-3]
        pullback_feature = feature_history[-2]
        signal_feature = feature_history[-1]
        return {
            "setup_range_expansion_ratio": (
                (setup_bar.high - setup_bar.low) / setup_feature.atr
                if setup_feature.atr > 0
                else Decimal("0")
            ),
            "pullback_range_expansion_ratio": (
                (pullback_bar.high - pullback_bar.low) / pullback_feature.atr
                if pullback_feature.atr > 0
                else Decimal("0")
            ),
            "signal_range_expansion_ratio": (
                (signal_bar.high - signal_bar.low) / signal_feature.atr
                if signal_feature.atr > 0
                else Decimal("0")
            ),
            "pullback_normalized_curvature": (
                pullback_feature.velocity_delta / max(pullback_feature.atr, settings.risk_floor)
                if pullback_feature.atr > 0
                else Decimal("0")
            ),
            "one_bar_pullback_before_signal": history[-2].close < history[-3].close,
            "signal_breaks_prior_1_high": history[-1].high > history[-2].high,
        }

    setup_features = compute_features(history[:-2], state, settings)
    pullback_features = compute_features(history[:-1], state, settings)
    signal_features = compute_features(history, state, settings)
    return {
        "setup_range_expansion_ratio": (
            (setup_bar.high - setup_bar.low) / setup_features.atr
            if setup_features.atr > 0
            else Decimal("0")
        ),
        "pullback_range_expansion_ratio": (
            (pullback_bar.high - pullback_bar.low) / pullback_features.atr
            if pullback_features.atr > 0
            else Decimal("0")
        ),
        "signal_range_expansion_ratio": (
            (signal_bar.high - signal_bar.low) / signal_features.atr
            if signal_features.atr > 0
            else Decimal("0")
        ),
        "pullback_normalized_curvature": (
            pullback_features.velocity_delta / max(pullback_features.atr, settings.risk_floor)
            if pullback_features.atr > 0
            else Decimal("0")
        ),
        "one_bar_pullback_before_signal": history[-2].close < history[-3].close,
        "signal_breaks_prior_1_high": history[-1].high > history[-2].high,
    }
