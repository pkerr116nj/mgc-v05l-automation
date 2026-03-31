"""Bear Snap signal contract."""

from collections.abc import Sequence
from decimal import Decimal

from ..indicators.feature_engine import compute_features
from ..config_models import StrategySettings
from ..domain.models import Bar, FeaturePacket, StrategyState
from ..app.session_phase_labels import label_session_phase


def evaluate_bear_snap(
    history: Sequence[Bar],
    features: FeaturePacket,
    state: StrategyState,
    settings: StrategySettings,
    feature_history: Sequence[FeaturePacket] | None = None,
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
    if not settings.use_bear_snap_location_filter:
        bear_snap_location_ok = True
    elif settings.bear_snap_require_close_above_slow_ema:
        bear_snap_location_ok = (
            current_bar.close >= features.turn_ema_slow + settings.bear_snap_min_close_vs_slow_ema_atr * features.atr
        )
    elif settings.bear_snap_min_close_vs_slow_ema_atr > Decimal("0"):
        bear_snap_location_ok = (
            current_bar.close >= features.turn_ema_slow - settings.bear_snap_min_close_vs_slow_ema_atr * features.atr
        )
    else:
        bear_snap_location_ok = True
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
    normalizer = max(features.atr, settings.risk_floor)
    normalized_slope = features.velocity / normalizer
    normalized_curvature = features.velocity_delta / normalizer
    prior_bars_since_short_setup = state.bars_since_short_setup if state.bars_since_short_setup is not None else 1000
    derivative_bear_slope_ok = (
        settings.us_derivative_bear_min_normalized_slope
        <= normalized_slope
        <= settings.us_derivative_bear_max_normalized_slope
    )
    derivative_bear_curvature_ok = normalized_curvature <= settings.us_derivative_bear_max_normalized_curvature
    derivative_bear_close_weak = _close_location_below_threshold(
        current_bar.low,
        current_bar.close,
        features.bar_range,
        settings.us_derivative_bear_max_close_location,
    )
    derivative_bear_range_ok = features.bar_range >= settings.us_derivative_bear_min_bar_range_atr * features.atr
    derivative_bear_body_ok = features.body_size >= settings.us_derivative_bear_min_body_atr * features.atr
    derivative_bear_stretch_ok = features.upside_stretch >= settings.us_derivative_bear_min_up_stretch_atr * features.atr
    derivative_bear_fast_ema_ok = (
        current_bar.close <= features.turn_ema_fast if settings.us_derivative_bear_require_below_fast_ema else True
    )
    derivative_bear_vwap_ok = current_bar.close <= features.vwap if settings.us_derivative_bear_require_below_vwap else True
    derivative_bear_slow_ema_ok = (
        current_bar.close
        <= features.turn_ema_slow - settings.us_derivative_bear_min_close_below_slow_ema_atr * features.atr
        if settings.us_derivative_bear_require_below_slow_ema
        else True
    )
    derivative_phase = label_session_phase(current_bar.end_ts)
    derivative_bear_phase_ok = (
        (derivative_phase == "US_PREOPEN_OPENING" and settings.us_derivative_bear_allow_phase_preopen_opening)
        or (derivative_phase == "US_CASH_OPEN_IMPULSE" and settings.us_derivative_bear_allow_phase_cash_open_impulse)
        or (derivative_phase == "US_OPEN_LATE" and settings.us_derivative_bear_allow_phase_open_late)
    )
    derivative_bear_structure_ok = (
        features.turn_ema_fast < features.turn_ema_slow if settings.us_derivative_bear_require_fast_below_slow else True
    )
    derivative_bar_time = current_bar.end_ts.astimezone(settings.timezone_info).time()
    derivative_bear_window_ok = (
        settings.us_derivative_bear_window_start <= derivative_bar_time < settings.us_derivative_bear_window_end
    )
    derivative_bear_vwap_extension_ok = (
        current_bar.close
        >= features.vwap - settings.us_derivative_bear_max_distance_below_vwap_atr * features.atr
    )
    derivative_bear_open_late_extension_floor_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            current_bar.close
            <= features.vwap - settings.us_derivative_bear_open_late_min_distance_below_vwap_atr * features.atr
        )
    )
    derivative_bear_open_late_body_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else features.body_size >= settings.us_derivative_bear_open_late_min_body_atr * features.atr
    )
    derivative_bear_open_late_close_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else _close_location_below_threshold(
            current_bar.low,
            current_bar.close,
            features.bar_range,
            settings.us_derivative_bear_open_late_max_close_location,
        )
    )
    derivative_bear_open_late_fast_ema_extension_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            current_bar.close
            >= features.turn_ema_fast - settings.us_derivative_bear_open_late_max_distance_below_fast_ema_atr * features.atr
        )
    )
    derivative_bear_cooldown_ok = prior_bars_since_short_setup > settings.us_derivative_bear_cooldown_bars
    derivative_bear_turn_candidate = (
        settings.enable_us_derivative_bear_shorts
        and settings.allow_us
        and current_bar.session_us
        and derivative_bear_window_ok
        and derivative_bear_phase_ok
        and derivative_bear_slope_ok
        and derivative_bear_curvature_ok
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and derivative_bear_fast_ema_ok
        and derivative_bear_vwap_ok
        and derivative_bear_vwap_extension_ok
        and derivative_bear_open_late_extension_floor_ok
        and derivative_bear_open_late_body_ok
        and derivative_bear_open_late_close_ok
        and derivative_bear_open_late_fast_ema_extension_ok
        and derivative_bear_slow_ema_ok
        and derivative_bear_structure_ok
        and derivative_bear_cooldown_ok
    )
    derivative_bear_additive_phase_ok = (
        (derivative_phase == "US_CASH_OPEN_IMPULSE" and settings.us_derivative_bear_additive_allow_phase_cash_open_impulse)
        or (derivative_phase == "US_OPEN_LATE" and settings.us_derivative_bear_additive_allow_phase_open_late)
    )
    derivative_bear_additive_slope_ok = (
        settings.us_derivative_bear_additive_min_normalized_slope
        <= normalized_slope
        <= settings.us_derivative_bear_additive_max_normalized_slope
    )
    derivative_bear_additive_curvature_ok = (
        settings.us_derivative_bear_additive_min_normalized_curvature
        <= normalized_curvature
        <= settings.us_derivative_bear_additive_max_normalized_curvature
    )
    derivative_bear_additive_open_late_extension_floor_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            current_bar.close
            <= features.vwap - settings.us_derivative_bear_additive_open_late_min_distance_below_vwap_atr * features.atr
        )
    )
    additive_recent_context = _compute_additive_recent_context(history, feature_history, state, settings)
    derivative_bear_additive_open_late_prior_vwap_extension_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            additive_recent_context["prior_3_bar_avg_vwap_extension_atr"]
            <= settings.us_derivative_bear_additive_open_late_max_prior_3_bar_avg_vwap_extension_atr
        )
    )
    derivative_bear_additive_open_late_prior_curvature_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            additive_recent_context["prior_3_bar_avg_curvature"]
            >= settings.us_derivative_bear_additive_open_late_min_prior_3_bar_avg_curvature
        )
    )
    derivative_bear_additive_open_late_prior_1_bar_vwap_extension_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            additive_recent_context["prior_1_bar_vwap_extension_atr"]
            <= settings.us_derivative_bear_additive_open_late_max_prior_1_bar_vwap_extension_atr
        )
    )
    derivative_bear_additive_open_late_prior_5_bar_avg_slope_ok = (
        True
        if derivative_phase != "US_OPEN_LATE"
        else (
            additive_recent_context["prior_5_bar_avg_slope"]
            <= settings.us_derivative_bear_additive_open_late_max_prior_5_bar_avg_slope
        )
    )
    derivative_bear_additive_open_late_prior_3_any_below_vwap_ok = (
        True
        if derivative_phase != "US_OPEN_LATE" or not settings.us_derivative_bear_additive_open_late_require_prior_3_any_below_vwap
        else additive_recent_context["prior_3_any_below_vwap"]
    )
    derivative_bear_additive_open_late_not_two_bar_rebound_ok = (
        True
        if derivative_phase != "US_OPEN_LATE" or not settings.us_derivative_bear_additive_open_late_require_not_two_bar_rebound
        else not additive_recent_context["two_bar_rebound_before_signal"]
    )
    derivative_bear_additive_open_late_break_below_prior_1_low_ok = (
        True
        if derivative_phase != "US_OPEN_LATE" or not settings.us_derivative_bear_additive_open_late_require_break_below_prior_1_low
        else additive_recent_context["signal_breaks_prior_1_low"]
    )
    derivative_bear_additive_open_late_break_below_prior_2_low_ok = (
        True
        if derivative_phase != "US_OPEN_LATE" or not settings.us_derivative_bear_additive_open_late_require_break_below_prior_2_low
        else additive_recent_context["signal_breaks_prior_2_low"]
    )
    derivative_bear_additive_open_late_downside_expansion_ok = (
        True
        if derivative_phase != "US_OPEN_LATE" or not settings.us_derivative_bear_additive_open_late_require_downside_expansion
        else additive_recent_context["signal_is_downside_expansion"]
    )
    asia_early_breakout_short_context = _compute_asia_early_breakout_retest_hold_short_context(
        history,
        feature_history,
        state,
        settings,
    )
    failed_move_reversal_short_context = _compute_failed_move_reversal_short_context(
        history,
        feature_history,
        state,
        settings,
    )
    midday_pause_resume_short_slope_ok = (
        settings.us_midday_pause_resume_short_min_normalized_slope
        <= normalized_slope
        <= settings.us_midday_pause_resume_short_max_normalized_slope
    )
    midday_pause_resume_short_curvature_ok = (
        settings.us_midday_pause_resume_short_min_normalized_curvature
        <= normalized_curvature
        <= settings.us_midday_pause_resume_short_max_normalized_curvature
    )
    midday_pause_resume_short_range_expansion_ok = (
        additive_recent_context["signal_range_expansion_ratio"]
        < settings.us_midday_pause_resume_short_max_range_expansion_ratio
    )
    midday_expanded_pause_resume_short_setup_expanded_ok = (
        additive_recent_context["setup_bar_range_expansion_ratio"]
        >= settings.us_midday_expanded_pause_resume_short_setup_min_range_expansion_ratio
    )
    midday_compressed_pause_resume_short_setup_compressed_ok = (
        additive_recent_context["setup_bar_range_expansion_ratio"]
        <= settings.us_midday_compressed_pause_resume_short_setup_max_range_expansion_ratio
    )
    midday_compressed_pause_resume_short_signal_normal_ok = (
        additive_recent_context["signal_range_expansion_ratio"]
        > settings.us_midday_compressed_pause_resume_short_signal_min_range_expansion_ratio
        and additive_recent_context["signal_range_expansion_ratio"]
        < settings.us_midday_compressed_pause_resume_short_signal_max_range_expansion_ratio
    )
    midday_expanded_pause_resume_short_setup_slope_ok = (
        additive_recent_context["setup_bar_normalized_slope"] <= Decimal("0.20")
    )
    midday_expanded_pause_resume_short_curvature_ok = normalized_curvature <= Decimal("-0.15")
    midday_expanded_pause_resume_short_ema_ok = (
        (
            current_bar.close <= features.turn_ema_fast
            and current_bar.close <= features.turn_ema_slow
            and features.turn_ema_fast < features.turn_ema_slow
        )
        or (
            features.turn_ema_fast > features.turn_ema_slow
            and current_bar.close < features.turn_ema_fast
            and current_bar.close >= features.turn_ema_slow
        )
    )
    midday_pause_resume_short_rebound_ok = (
        True
        if not settings.us_midday_pause_resume_short_require_one_bar_rebound
        else additive_recent_context["one_bar_rebound_before_signal"]
    )
    midday_pause_resume_short_prior_curvature_ok = (
        True
        if not settings.us_midday_pause_resume_short_require_prior_3_any_positive_curvature
        else additive_recent_context["prior_3_any_positive_curvature"]
    )
    midday_pause_resume_short_break_ok = (
        True
        if not settings.us_midday_pause_resume_short_require_break_below_prior_1_low
        else additive_recent_context["signal_breaks_prior_1_low"]
    )
    london_late_pause_resume_short_slope_ok = (
        settings.london_late_pause_resume_short_min_normalized_slope
        <= normalized_slope
        <= settings.london_late_pause_resume_short_max_normalized_slope
    )
    london_late_pause_resume_short_curvature_ok = (
        settings.london_late_pause_resume_short_min_normalized_curvature
        <= normalized_curvature
        <= settings.london_late_pause_resume_short_max_normalized_curvature
    )
    london_late_pause_resume_short_range_expansion_ok = (
        additive_recent_context["signal_range_expansion_ratio"]
        < settings.london_late_pause_resume_short_max_range_expansion_ratio
    )
    london_late_pause_resume_short_slow_ema_ok = (
        current_bar.close >= features.turn_ema_slow if settings.london_late_pause_resume_short_require_above_slow_ema else True
    )
    london_late_pause_resume_short_rebound_ok = (
        True
        if not settings.london_late_pause_resume_short_require_one_bar_rebound
        else additive_recent_context["one_bar_rebound_before_signal"]
    )
    london_late_pause_resume_short_prior_curvature_ok = (
        True
        if not settings.london_late_pause_resume_short_require_prior_3_any_positive_curvature
        else additive_recent_context["prior_3_any_positive_curvature"]
    )
    london_late_pause_resume_short_break_ok = (
        True
        if not settings.london_late_pause_resume_short_require_break_below_prior_1_low
        else additive_recent_context["signal_breaks_prior_1_low"]
    )
    asia_early_pause_resume_short_curvature_ok = (
        normalized_curvature <= settings.asia_early_pause_resume_short_max_normalized_curvature
    )
    asia_early_pause_resume_short_setup_curvature_ok = additive_recent_context["setup_bar_curvature_is_flat"]
    asia_early_pause_resume_short_range_expansion_ok = (
        additive_recent_context["signal_range_expansion_ratio"]
        < settings.asia_early_pause_resume_short_max_range_expansion_ratio
    )
    asia_early_compressed_pause_resume_short_rebound_compressed_ok = (
        additive_recent_context["rebound_bar_range_expansion_ratio"]
        <= settings.asia_early_compressed_pause_resume_short_rebound_max_range_expansion_ratio
    )
    asia_early_compressed_pause_resume_short_signal_normal_ok = (
        additive_recent_context["signal_range_expansion_ratio"]
        > settings.asia_early_compressed_pause_resume_short_signal_min_range_expansion_ratio
        and additive_recent_context["signal_range_expansion_ratio"]
        < settings.asia_early_compressed_pause_resume_short_signal_max_range_expansion_ratio
    )
    asia_early_pause_resume_short_rebound_ok = (
        True
        if not settings.asia_early_pause_resume_short_require_one_bar_rebound
        else additive_recent_context["one_bar_rebound_before_signal"]
    )
    asia_early_pause_resume_short_break_ok = (
        True
        if not settings.asia_early_pause_resume_short_require_break_below_prior_1_low
        else additive_recent_context["signal_breaks_prior_1_low"]
    )
    asia_early_pause_resume_short_fast_ema_ok = (
        True
        if not settings.asia_early_pause_resume_short_require_close_below_fast_ema
        else current_bar.close <= features.turn_ema_fast
    )
    midday_compressed_failed_move_reversal_short_reversal_compressed_ok = (
        failed_move_reversal_short_context["reversal_range_expansion_ratio"]
        <= settings.us_midday_failed_move_reversal_short_reversal_max_range_expansion_ratio
    )
    midday_compressed_failed_move_reversal_short_failed_move_shape_ok = (
        failed_move_reversal_short_context["failed_move_breaks_prior_1_high"]
        and failed_move_reversal_short_context["reversal_closes_back_below_failed_move_high"]
        and failed_move_reversal_short_context["reversal_closes_below_failed_move_close"]
    )
    midday_compressed_failed_move_reversal_short_curvature_ok = normalized_curvature <= Decimal("-0.15")
    midday_compressed_failed_move_reversal_short_ema_transition_ok = (
        failed_move_reversal_short_context["failed_move_ema_above_both"]
        and failed_move_reversal_short_context["reversal_ema_rebound_above_slow"]
    )
    midday_compressed_failed_move_reversal_short_turn_candidate = (
        settings.enable_us_midday_compressed_failed_move_reversal_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and settings.allow_us
        and current_bar.session_us
        and derivative_phase == "US_MIDDAY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and midday_compressed_failed_move_reversal_short_failed_move_shape_ok
        and midday_compressed_failed_move_reversal_short_curvature_ok
        and midday_compressed_failed_move_reversal_short_reversal_compressed_ok
        and derivative_bear_cooldown_ok
    )
    midday_compressed_rebound_failed_move_reversal_short_turn_candidate = (
        settings.enable_us_midday_compressed_rebound_failed_move_reversal_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and settings.allow_us
        and current_bar.session_us
        and derivative_phase == "US_MIDDAY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and midday_compressed_failed_move_reversal_short_failed_move_shape_ok
        and midday_compressed_failed_move_reversal_short_curvature_ok
        and midday_compressed_failed_move_reversal_short_reversal_compressed_ok
        and midday_compressed_failed_move_reversal_short_ema_transition_ok
        and derivative_bear_cooldown_ok
    )
    midday_expanded_pause_resume_short_turn_candidate = (
        settings.enable_us_midday_expanded_pause_resume_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_compressed_rebound_failed_move_reversal_short_turn_candidate
        and not midday_compressed_failed_move_reversal_short_turn_candidate
        and settings.allow_us
        and current_bar.session_us
        and derivative_phase == "US_MIDDAY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and midday_expanded_pause_resume_short_setup_slope_ok
        and midday_expanded_pause_resume_short_curvature_ok
        and midday_pause_resume_short_range_expansion_ok
        and midday_pause_resume_short_rebound_ok
        and midday_pause_resume_short_break_ok
        and midday_expanded_pause_resume_short_setup_expanded_ok
        and midday_expanded_pause_resume_short_ema_ok
        and derivative_bear_cooldown_ok
    )
    midday_compressed_pause_resume_short_turn_candidate = (
        settings.enable_us_midday_compressed_pause_resume_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_expanded_pause_resume_short_turn_candidate
        and settings.allow_us
        and current_bar.session_us
        and derivative_phase == "US_MIDDAY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and midday_expanded_pause_resume_short_setup_slope_ok
        and midday_expanded_pause_resume_short_curvature_ok
        and midday_pause_resume_short_rebound_ok
        and midday_pause_resume_short_break_ok
        and midday_compressed_pause_resume_short_setup_compressed_ok
        and midday_compressed_pause_resume_short_signal_normal_ok
        and midday_expanded_pause_resume_short_ema_ok
        and derivative_bear_cooldown_ok
    )
    midday_pause_resume_short_turn_candidate = (
        settings.enable_us_midday_pause_resume_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_expanded_pause_resume_short_turn_candidate
        and not midday_compressed_pause_resume_short_turn_candidate
        and settings.allow_us
        and current_bar.session_us
        and derivative_phase == "US_MIDDAY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and midday_pause_resume_short_slope_ok
        and midday_pause_resume_short_curvature_ok
        and midday_pause_resume_short_range_expansion_ok
        and midday_pause_resume_short_rebound_ok
        and midday_pause_resume_short_prior_curvature_ok
        and midday_pause_resume_short_break_ok
        and derivative_bear_cooldown_ok
    )
    london_late_pause_resume_short_turn_candidate = (
        settings.enable_london_late_pause_resume_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_pause_resume_short_turn_candidate
        and settings.allow_london
        and current_bar.session_london
        and derivative_phase == "LONDON_LATE"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and london_late_pause_resume_short_slope_ok
        and london_late_pause_resume_short_curvature_ok
        and london_late_pause_resume_short_range_expansion_ok
        and london_late_pause_resume_short_slow_ema_ok
        and london_late_pause_resume_short_rebound_ok
        and london_late_pause_resume_short_prior_curvature_ok
        and london_late_pause_resume_short_break_ok
        and derivative_bear_cooldown_ok
    )
    asia_early_expanded_breakout_retest_hold_short_turn_candidate = (
        settings.enable_asia_early_expanded_breakout_retest_hold_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_pause_resume_short_turn_candidate
        and not london_late_pause_resume_short_turn_candidate
        and settings.allow_asia
        and current_bar.session_asia
        and derivative_phase == "ASIA_EARLY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and asia_early_breakout_short_context["breakout_bar_is_expanded"]
        and asia_early_breakout_short_context["signal_bar_is_expanded"]
        and asia_early_breakout_short_context["breakout_breaks_prior_1_low"]
        and asia_early_breakout_short_context["signal_retests_and_holds_breakout_level"]
        and derivative_bear_cooldown_ok
    )
    asia_early_compressed_pause_resume_short_turn_candidate = (
        settings.enable_asia_early_compressed_pause_resume_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_pause_resume_short_turn_candidate
        and not london_late_pause_resume_short_turn_candidate
        and not asia_early_expanded_breakout_retest_hold_short_turn_candidate
        and settings.allow_asia
        and current_bar.session_asia
        and derivative_phase == "ASIA_EARLY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and asia_early_pause_resume_short_curvature_ok
        and asia_early_pause_resume_short_setup_curvature_ok
        and asia_early_pause_resume_short_range_expansion_ok
        and asia_early_pause_resume_short_rebound_ok
        and asia_early_pause_resume_short_break_ok
        and asia_early_pause_resume_short_fast_ema_ok
        and asia_early_compressed_pause_resume_short_rebound_compressed_ok
        and asia_early_compressed_pause_resume_short_signal_normal_ok
        and derivative_bear_cooldown_ok
    )
    asia_early_pause_resume_short_turn_candidate = (
        settings.enable_asia_early_pause_resume_shorts
        and not derivative_bear_turn_candidate
        and not first_bear_snap_turn
        and not midday_pause_resume_short_turn_candidate
        and not london_late_pause_resume_short_turn_candidate
        and not asia_early_expanded_breakout_retest_hold_short_turn_candidate
        and not asia_early_compressed_pause_resume_short_turn_candidate
        and settings.allow_asia
        and current_bar.session_asia
        and derivative_phase == "ASIA_EARLY"
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and asia_early_pause_resume_short_curvature_ok
        and asia_early_pause_resume_short_setup_curvature_ok
        and asia_early_pause_resume_short_range_expansion_ok
        and asia_early_pause_resume_short_rebound_ok
        and asia_early_pause_resume_short_break_ok
        and asia_early_pause_resume_short_fast_ema_ok
        and derivative_bear_cooldown_ok
    )
    derivative_bear_additive_turn_candidate = (
        settings.enable_us_derivative_bear_additive_shorts
        and not derivative_bear_turn_candidate
        and not midday_pause_resume_short_turn_candidate
        and not london_late_pause_resume_short_turn_candidate
        and not asia_early_compressed_pause_resume_short_turn_candidate
        and not asia_early_pause_resume_short_turn_candidate
        and not first_bear_snap_turn
        and settings.allow_us
        and current_bar.session_us
        and derivative_bear_window_ok
        and derivative_bear_additive_phase_ok
        and derivative_bear_additive_slope_ok
        and derivative_bear_additive_curvature_ok
        and current_bar.close < current_bar.open
        and current_bar.close < previous_close
        and derivative_bear_close_weak
        and derivative_bear_range_ok
        and derivative_bear_body_ok
        and derivative_bear_stretch_ok
        and derivative_bear_fast_ema_ok
        and derivative_bear_vwap_ok
        and derivative_bear_vwap_extension_ok
        and derivative_bear_additive_open_late_extension_floor_ok
        and derivative_bear_additive_open_late_prior_vwap_extension_ok
        and derivative_bear_additive_open_late_prior_curvature_ok
        and derivative_bear_additive_open_late_prior_1_bar_vwap_extension_ok
        and derivative_bear_additive_open_late_prior_5_bar_avg_slope_ok
        and derivative_bear_additive_open_late_prior_3_any_below_vwap_ok
        and derivative_bear_additive_open_late_not_two_bar_rebound_ok
        and derivative_bear_additive_open_late_break_below_prior_1_low_ok
        and derivative_bear_additive_open_late_break_below_prior_2_low_ok
        and derivative_bear_additive_open_late_downside_expansion_ok
        and derivative_bear_open_late_body_ok
        and derivative_bear_open_late_close_ok
        and derivative_bear_open_late_fast_ema_extension_ok
        and derivative_bear_slow_ema_ok
        and derivative_bear_cooldown_ok
    )

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
        "derivative_bear_slope_ok": derivative_bear_slope_ok,
        "derivative_bear_curvature_ok": derivative_bear_curvature_ok,
        "derivative_bear_turn_candidate": derivative_bear_turn_candidate,
        "derivative_bear_additive_turn_candidate": derivative_bear_additive_turn_candidate,
        "midday_compressed_failed_move_reversal_short_turn_candidate": (
            midday_compressed_failed_move_reversal_short_turn_candidate
        ),
        "midday_compressed_rebound_failed_move_reversal_short_turn_candidate": (
            midday_compressed_rebound_failed_move_reversal_short_turn_candidate
        ),
        "midday_expanded_pause_resume_short_turn_candidate": midday_expanded_pause_resume_short_turn_candidate,
        "midday_compressed_pause_resume_short_turn_candidate": midday_compressed_pause_resume_short_turn_candidate,
        "midday_pause_resume_short_turn_candidate": midday_pause_resume_short_turn_candidate,
        "london_late_pause_resume_short_turn_candidate": london_late_pause_resume_short_turn_candidate,
        "asia_early_expanded_breakout_retest_hold_short_turn_candidate": (
            asia_early_expanded_breakout_retest_hold_short_turn_candidate
        ),
        "asia_early_compressed_pause_resume_short_turn_candidate": asia_early_compressed_pause_resume_short_turn_candidate,
        "asia_early_pause_resume_short_turn_candidate": asia_early_pause_resume_short_turn_candidate,
    }


def _close_location_below_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    if bar_range <= 0:
        return False
    return close < low + threshold * bar_range


def _compute_asia_early_breakout_retest_hold_short_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool]:
    if len(history) < 3:
        return {
            "breakout_bar_is_expanded": False,
            "signal_bar_is_expanded": False,
            "breakout_breaks_prior_1_low": False,
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

    breakout_level = breakout_bar.low
    return {
        "breakout_bar_is_expanded": (
            breakout_range_expansion_ratio
            >= settings.asia_early_breakout_retest_hold_short_breakout_min_range_expansion_ratio
        ),
        "signal_bar_is_expanded": (
            signal_range_expansion_ratio
            >= settings.asia_early_breakout_retest_hold_short_retest_min_range_expansion_ratio
        ),
        "breakout_breaks_prior_1_low": breakout_bar.low < prior_bar.low and breakout_bar.close <= prior_bar.close,
        "signal_retests_and_holds_breakout_level": signal_bar.high >= breakout_level and signal_bar.close <= breakout_level,
    }


def _compute_failed_move_reversal_short_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, bool | Decimal]:
    if len(history) < 3:
        return {
            "failed_move_breaks_prior_1_high": False,
            "reversal_closes_back_below_failed_move_high": False,
            "reversal_closes_below_failed_move_close": False,
            "failed_move_ema_above_both": False,
            "reversal_ema_rebound_above_slow": False,
            "reversal_range_expansion_ratio": Decimal("0"),
        }

    prior_bar = history[-3]
    failed_move_bar = history[-2]
    reversal_bar = history[-1]

    if feature_history is not None and len(feature_history) == len(history):
        failed_move_feature = feature_history[-2]
        reversal_feature = feature_history[-1]
    else:
        failed_move_feature = compute_features(history[:-1], state, settings)
        reversal_feature = compute_features(history, state, settings)

    reversal_range_expansion_ratio = Decimal("0")
    if reversal_feature.atr > 0:
        reversal_range_expansion_ratio = (reversal_bar.high - reversal_bar.low) / reversal_feature.atr

    return {
        "failed_move_breaks_prior_1_high": failed_move_bar.high > prior_bar.high,
        "reversal_closes_back_below_failed_move_high": reversal_bar.close < failed_move_bar.high,
        "reversal_closes_below_failed_move_close": reversal_bar.close < failed_move_bar.close,
        "failed_move_ema_above_both": (
            failed_move_bar.close >= failed_move_feature.turn_ema_fast
            and failed_move_bar.close >= failed_move_feature.turn_ema_slow
            and failed_move_feature.turn_ema_fast > failed_move_feature.turn_ema_slow
        ),
        "reversal_ema_rebound_above_slow": (
            reversal_feature.turn_ema_fast > reversal_feature.turn_ema_slow
            and reversal_bar.close < reversal_feature.turn_ema_fast
            and reversal_bar.close >= reversal_feature.turn_ema_slow
        ),
        "reversal_range_expansion_ratio": reversal_range_expansion_ratio,
    }


def _compute_additive_recent_context(
    history: Sequence[Bar],
    feature_history: Sequence[FeaturePacket] | None,
    state: StrategyState,
    settings: StrategySettings,
) -> dict[str, Decimal]:
    if feature_history is not None and len(feature_history) == len(history):
        prior_features = feature_history[-4:-1]
        prior_last_feature = feature_history[-2] if len(feature_history) >= 2 else None
        current_feature = feature_history[-1] if feature_history else None
        prior_slope_features = feature_history[-6:-1]
        prior_curvatures = [
            feature.velocity_delta / feature.atr
            for feature in prior_features
            if feature.atr > 0
        ]
        prior_vwap_extensions = [
            (bar.close - feature.vwap) / feature.atr
            for bar, feature in zip(history[-4:-1], prior_features)
            if feature.atr > 0
        ]
        prior_slopes = [
            feature.velocity / feature.atr
            for feature in prior_slope_features
            if feature.atr > 0
        ]
        return {
            "prior_3_bar_avg_curvature": _average_decimal(prior_curvatures),
            "prior_3_bar_avg_vwap_extension_atr": _average_decimal(prior_vwap_extensions),
            "prior_1_bar_vwap_extension_atr": (
                (history[-2].close - prior_last_feature.vwap) / prior_last_feature.atr
                if prior_last_feature is not None and prior_last_feature.atr > 0 and len(history) >= 2
                else Decimal("0")
            ),
            "prior_5_bar_avg_slope": _average_decimal(prior_slopes),
            "setup_bar_normalized_curvature": (
                feature_history[-3].velocity_delta / feature_history[-3].atr
                if len(feature_history) >= 3 and feature_history[-3].atr > 0
                else Decimal("0")
            ),
            "setup_bar_normalized_slope": (
                feature_history[-3].velocity / feature_history[-3].atr
                if len(feature_history) >= 3 and feature_history[-3].atr > 0
                else Decimal("0")
            ),
            "setup_bar_curvature_is_flat": (
                abs(feature_history[-3].velocity_delta / feature_history[-3].atr)
                <= settings.asia_early_pause_resume_short_setup_curvature_flat_threshold
                if len(feature_history) >= 3 and feature_history[-3].atr > 0
                else False
            ),
            "setup_bar_range_expansion_ratio": (
                (history[-3].high - history[-3].low) / feature_history[-3].atr
                if len(history) >= 3 and len(feature_history) >= 3 and feature_history[-3].atr > 0
                else Decimal("0")
            ),
            "rebound_bar_range_expansion_ratio": (
                (history[-2].high - history[-2].low) / feature_history[-2].atr
                if len(history) >= 2 and len(feature_history) >= 2 and feature_history[-2].atr > 0
                else Decimal("0")
            ),
            "prior_3_any_below_vwap": any(value < 0 for value in prior_vwap_extensions),
            "prior_3_any_positive_curvature": any(value > 0 for value in prior_curvatures),
            "one_bar_rebound_before_signal": len(history) >= 3 and history[-2].close > history[-3].close,
            "two_bar_rebound_before_signal": (
                len(history) >= 4
                and history[-2].close > history[-3].close
                and history[-3].close > history[-4].close
            ),
            "signal_range_expansion_ratio": (
                (history[-1].high - history[-1].low) / current_feature.atr
                if current_feature is not None and current_feature.atr > 0
                else Decimal("0")
            ),
            "signal_breaks_prior_1_low": len(history) >= 2 and history[-1].low < history[-2].low,
            "signal_breaks_prior_2_low": (
                len(history) >= 3 and history[-1].low < min(history[-2].low, history[-3].low)
            ),
            "signal_is_downside_expansion": (
                len(history) >= 4
                and history[-1].close < history[-1].open
                and (history[-1].high - history[-1].low)
                > _average_decimal([bar.high - bar.low for bar in history[-4:-1]])
            ),
        }

    prior_curvatures: list[Decimal] = []
    prior_vwap_extensions: list[Decimal] = []
    prior_slopes: list[Decimal] = []
    if len(history) < 2:
        return {
            "prior_3_bar_avg_curvature": Decimal("0"),
            "prior_3_bar_avg_vwap_extension_atr": Decimal("0"),
            "prior_1_bar_vwap_extension_atr": Decimal("0"),
            "prior_5_bar_avg_slope": Decimal("0"),
            "setup_bar_normalized_curvature": Decimal("0"),
            "setup_bar_normalized_slope": Decimal("0"),
            "setup_bar_curvature_is_flat": False,
            "setup_bar_range_expansion_ratio": Decimal("0"),
            "rebound_bar_range_expansion_ratio": Decimal("0"),
            "prior_3_any_below_vwap": False,
            "prior_3_any_positive_curvature": False,
            "one_bar_rebound_before_signal": False,
            "two_bar_rebound_before_signal": False,
            "signal_range_expansion_ratio": Decimal("0"),
            "signal_breaks_prior_1_low": False,
            "signal_breaks_prior_2_low": False,
            "signal_is_downside_expansion": False,
        }

    start_index = max(0, len(history) - 4)
    for end_index in range(start_index, len(history) - 1):
        prefix = history[: end_index + 1]
        prior_features = compute_features(prefix, state, settings)
        if prior_features.atr > 0:
            prior_curvatures.append(prior_features.velocity_delta / prior_features.atr)
            prior_vwap_extensions.append((prefix[-1].close - prior_features.vwap) / prior_features.atr)
    slope_start_index = max(0, len(history) - 6)
    for end_index in range(slope_start_index, len(history) - 1):
        prefix = history[: end_index + 1]
        prior_features = compute_features(prefix, state, settings)
        if prior_features.atr > 0:
            prior_slopes.append(prior_features.velocity / prior_features.atr)

    current_features = compute_features(history, state, settings)
    setup_curvature = Decimal("0")
    setup_slope = Decimal("0")
    setup_curvature_is_flat = False
    setup_range_expansion_ratio = Decimal("0")
    rebound_range_expansion_ratio = Decimal("0")
    if len(history) >= 3:
        setup_features = compute_features(history[:-2], state, settings)
        if setup_features.atr > 0:
            setup_slope = setup_features.velocity / setup_features.atr
            setup_curvature = setup_features.velocity_delta / setup_features.atr
            setup_curvature_is_flat = (
                abs(setup_curvature) <= settings.asia_early_pause_resume_short_setup_curvature_flat_threshold
            )
            setup_range_expansion_ratio = (history[-3].high - history[-3].low) / setup_features.atr
    if len(history) >= 2:
        rebound_features = compute_features(history[:-1], state, settings)
        if rebound_features.atr > 0:
            rebound_range_expansion_ratio = (history[-2].high - history[-2].low) / rebound_features.atr
    return {
        "prior_3_bar_avg_curvature": _average_decimal(prior_curvatures),
        "prior_3_bar_avg_vwap_extension_atr": _average_decimal(prior_vwap_extensions),
        "prior_1_bar_vwap_extension_atr": prior_vwap_extensions[-1] if prior_vwap_extensions else Decimal("0"),
        "prior_5_bar_avg_slope": _average_decimal(prior_slopes),
        "setup_bar_normalized_curvature": setup_curvature,
        "setup_bar_normalized_slope": setup_slope,
        "setup_bar_curvature_is_flat": setup_curvature_is_flat,
        "setup_bar_range_expansion_ratio": setup_range_expansion_ratio,
        "rebound_bar_range_expansion_ratio": rebound_range_expansion_ratio,
        "prior_3_any_below_vwap": any(value < 0 for value in prior_vwap_extensions),
        "prior_3_any_positive_curvature": any(value > 0 for value in prior_curvatures),
        "one_bar_rebound_before_signal": len(history) >= 3 and history[-2].close > history[-3].close,
        "two_bar_rebound_before_signal": (
            len(history) >= 4
            and history[-2].close > history[-3].close
            and history[-3].close > history[-4].close
        ),
        "signal_range_expansion_ratio": (
            (history[-1].high - history[-1].low) / current_features.atr
            if current_features.atr > 0
            else Decimal("0")
        ),
        "signal_breaks_prior_1_low": len(history) >= 2 and history[-1].low < history[-2].low,
        "signal_breaks_prior_2_low": len(history) >= 3 and history[-1].low < min(history[-2].low, history[-3].low),
        "signal_is_downside_expansion": (
            len(history) >= 4
            and history[-1].close < history[-1].open
            and (history[-1].high - history[-1].low) > _average_decimal([bar.high - bar.low for bar in history[-4:-1]])
        ),
    }


def _average_decimal(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))
