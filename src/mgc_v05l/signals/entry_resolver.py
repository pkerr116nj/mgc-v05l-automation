"""Entry resolver contract."""

from ..config_models import StrategySettings
from ..domain.models import SignalPacket, StrategyState


def resolve_entries(signal_packet: SignalPacket, state: StrategyState, settings: StrategySettings) -> SignalPacket:
    """Resolve raw entries into final entry booleans and source tagging."""
    recent_long_setup = (state.bars_since_long_setup if state.bars_since_long_setup is not None else 1000) <= (
        settings.anti_churn_bars
    )
    recent_short_setup = (state.bars_since_short_setup if state.bars_since_short_setup is not None else 1000) <= (
        settings.anti_churn_bars
    )

    long_entry_raw = (
        signal_packet.first_bull_snap_turn
        or signal_packet.asia_vwap_long_signal
        or signal_packet.midday_pause_resume_long_turn_candidate
        or signal_packet.us_late_breakout_retest_hold_long_turn_candidate
        or signal_packet.us_late_failed_move_reversal_long_turn_candidate
        or signal_packet.us_late_pause_resume_long_turn_candidate
        or signal_packet.asia_early_breakout_retest_hold_long_turn_candidate
        or signal_packet.asia_early_normal_breakout_retest_hold_long_turn_candidate
        or signal_packet.asia_late_pause_resume_long_turn_candidate
        or signal_packet.asia_late_flat_pullback_pause_resume_long_turn_candidate
        or signal_packet.asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate
    )
    short_entry_raw = (
        signal_packet.first_bear_snap_turn
        or signal_packet.derivative_bear_turn_candidate
        or signal_packet.derivative_bear_additive_turn_candidate
        or signal_packet.midday_compressed_rebound_failed_move_reversal_short_turn_candidate
        or signal_packet.midday_compressed_failed_move_reversal_short_turn_candidate
        or signal_packet.midday_expanded_pause_resume_short_turn_candidate
        or signal_packet.midday_compressed_pause_resume_short_turn_candidate
        or signal_packet.midday_pause_resume_short_turn_candidate
        or signal_packet.london_late_pause_resume_short_turn_candidate
        or signal_packet.asia_early_expanded_breakout_retest_hold_short_turn_candidate
        or signal_packet.asia_early_compressed_pause_resume_short_turn_candidate
        or signal_packet.asia_early_pause_resume_short_turn_candidate
    )

    long_entry = long_entry_raw and (
        (not recent_long_setup) or signal_packet.first_bull_snap_turn or signal_packet.asia_vwap_long_signal
    )
    short_entry = short_entry_raw and ((not recent_short_setup) or signal_packet.first_bear_snap_turn)

    long_entry_source = None
    if long_entry:
        if signal_packet.asia_vwap_long_signal:
            long_entry_source = "asiaVWAPLongSignal"
        elif signal_packet.first_bull_snap_turn:
            long_entry_source = "firstBullSnapTurn"
        elif signal_packet.midday_pause_resume_long_turn_candidate:
            long_entry_source = "usMiddayPauseResumeLongTurn"
        elif signal_packet.us_late_breakout_retest_hold_long_turn_candidate:
            long_entry_source = "usLateBreakoutRetestHoldTurn"
        elif signal_packet.us_late_failed_move_reversal_long_turn_candidate:
            long_entry_source = "usLateFailedMoveReversalLongTurn"
        elif signal_packet.us_late_pause_resume_long_turn_candidate:
            long_entry_source = "usLatePauseResumeLongTurn"
        elif signal_packet.asia_early_normal_breakout_retest_hold_long_turn_candidate:
            long_entry_source = "asiaEarlyNormalBreakoutRetestHoldTurn"
        elif signal_packet.asia_early_breakout_retest_hold_long_turn_candidate:
            long_entry_source = "asiaEarlyBreakoutRetestHoldTurn"
        elif signal_packet.asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate:
            long_entry_source = "asiaLateCompressedFlatPullbackPauseResumeLongTurn"
        elif signal_packet.asia_late_flat_pullback_pause_resume_long_turn_candidate:
            long_entry_source = "asiaLateFlatPullbackPauseResumeLongTurn"
        elif signal_packet.asia_late_pause_resume_long_turn_candidate:
            long_entry_source = "asiaLatePauseResumeLongTurn"

    short_entry_source = None
    if short_entry:
        if signal_packet.first_bear_snap_turn:
            short_entry_source = "firstBearSnapTurn"
        elif signal_packet.derivative_bear_turn_candidate:
            short_entry_source = "usDerivativeBearTurn"
        elif signal_packet.derivative_bear_additive_turn_candidate:
            short_entry_source = "usDerivativeBearAdditiveTurn"
        elif signal_packet.midday_compressed_rebound_failed_move_reversal_short_turn_candidate:
            short_entry_source = "usMiddayCompressedReboundFailedMoveReversalShortTurn"
        elif signal_packet.midday_compressed_failed_move_reversal_short_turn_candidate:
            short_entry_source = "usMiddayCompressedFailedMoveReversalShortTurn"
        elif signal_packet.midday_expanded_pause_resume_short_turn_candidate:
            short_entry_source = "usMiddayExpandedPauseResumeShortTurn"
        elif signal_packet.midday_compressed_pause_resume_short_turn_candidate:
            short_entry_source = "usMiddayCompressedPauseResumeShortTurn"
        elif signal_packet.midday_pause_resume_short_turn_candidate:
            short_entry_source = "usMiddayPauseResumeShortTurn"
        elif signal_packet.london_late_pause_resume_short_turn_candidate:
            short_entry_source = "londonLatePauseResumeShortTurn"
        elif signal_packet.asia_early_expanded_breakout_retest_hold_short_turn_candidate:
            short_entry_source = "asiaEarlyExpandedBreakoutRetestHoldShortTurn"
        elif signal_packet.asia_early_compressed_pause_resume_short_turn_candidate:
            short_entry_source = "asiaEarlyCompressedPauseResumeShortTurn"
        elif signal_packet.asia_early_pause_resume_short_turn_candidate:
            short_entry_source = "asiaEarlyPauseResumeShortTurn"

    return SignalPacket(
        bar_id=signal_packet.bar_id,
        bull_snap_downside_stretch_ok=signal_packet.bull_snap_downside_stretch_ok,
        bull_snap_range_ok=signal_packet.bull_snap_range_ok,
        bull_snap_body_ok=signal_packet.bull_snap_body_ok,
        bull_snap_close_strong=signal_packet.bull_snap_close_strong,
        bull_snap_velocity_ok=signal_packet.bull_snap_velocity_ok,
        bull_snap_reversal_bar=signal_packet.bull_snap_reversal_bar,
        bull_snap_location_ok=signal_packet.bull_snap_location_ok,
        bull_snap_raw=signal_packet.bull_snap_raw,
        bull_snap_turn_candidate=signal_packet.bull_snap_turn_candidate,
        first_bull_snap_turn=signal_packet.first_bull_snap_turn,
        below_vwap_recently=signal_packet.below_vwap_recently,
        reclaim_range_ok=signal_packet.reclaim_range_ok,
        reclaim_vol_ok=signal_packet.reclaim_vol_ok,
        reclaim_color_ok=signal_packet.reclaim_color_ok,
        reclaim_close_ok=signal_packet.reclaim_close_ok,
        asia_reclaim_bar_raw=signal_packet.asia_reclaim_bar_raw,
        asia_hold_bar=signal_packet.asia_hold_bar,
        asia_hold_close_vwap_ok=signal_packet.asia_hold_close_vwap_ok,
        asia_hold_low_ok=signal_packet.asia_hold_low_ok,
        asia_hold_bar_ok=signal_packet.asia_hold_bar_ok,
        asia_acceptance_bar=signal_packet.asia_acceptance_bar,
        asia_acceptance_close_high_ok=signal_packet.asia_acceptance_close_high_ok,
        asia_acceptance_close_vwap_ok=signal_packet.asia_acceptance_close_vwap_ok,
        asia_acceptance_bar_ok=signal_packet.asia_acceptance_bar_ok,
        asia_vwap_long_signal=signal_packet.asia_vwap_long_signal,
        midday_pause_resume_long_turn_candidate=signal_packet.midday_pause_resume_long_turn_candidate,
        us_late_breakout_retest_hold_long_turn_candidate=signal_packet.us_late_breakout_retest_hold_long_turn_candidate,
        us_late_failed_move_reversal_long_turn_candidate=signal_packet.us_late_failed_move_reversal_long_turn_candidate,
        us_late_pause_resume_long_turn_candidate=signal_packet.us_late_pause_resume_long_turn_candidate,
        asia_early_breakout_retest_hold_long_turn_candidate=signal_packet.asia_early_breakout_retest_hold_long_turn_candidate,
        asia_early_normal_breakout_retest_hold_long_turn_candidate=signal_packet.asia_early_normal_breakout_retest_hold_long_turn_candidate,
        asia_late_pause_resume_long_turn_candidate=signal_packet.asia_late_pause_resume_long_turn_candidate,
        asia_late_flat_pullback_pause_resume_long_turn_candidate=signal_packet.asia_late_flat_pullback_pause_resume_long_turn_candidate,
        asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate=signal_packet.asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate,
        bear_snap_up_stretch_ok=signal_packet.bear_snap_up_stretch_ok,
        bear_snap_range_ok=signal_packet.bear_snap_range_ok,
        bear_snap_body_ok=signal_packet.bear_snap_body_ok,
        bear_snap_close_weak=signal_packet.bear_snap_close_weak,
        bear_snap_velocity_ok=signal_packet.bear_snap_velocity_ok,
        bear_snap_reversal_bar=signal_packet.bear_snap_reversal_bar,
        bear_snap_location_ok=signal_packet.bear_snap_location_ok,
        bear_snap_raw=signal_packet.bear_snap_raw,
        bear_snap_turn_candidate=signal_packet.bear_snap_turn_candidate,
        first_bear_snap_turn=signal_packet.first_bear_snap_turn,
        derivative_bear_slope_ok=signal_packet.derivative_bear_slope_ok,
        derivative_bear_curvature_ok=signal_packet.derivative_bear_curvature_ok,
        derivative_bear_turn_candidate=signal_packet.derivative_bear_turn_candidate,
        derivative_bear_additive_turn_candidate=signal_packet.derivative_bear_additive_turn_candidate,
        midday_compressed_failed_move_reversal_short_turn_candidate=(
            signal_packet.midday_compressed_failed_move_reversal_short_turn_candidate
        ),
        midday_compressed_rebound_failed_move_reversal_short_turn_candidate=(
            signal_packet.midday_compressed_rebound_failed_move_reversal_short_turn_candidate
        ),
        midday_expanded_pause_resume_short_turn_candidate=signal_packet.midday_expanded_pause_resume_short_turn_candidate,
        midday_compressed_pause_resume_short_turn_candidate=signal_packet.midday_compressed_pause_resume_short_turn_candidate,
        midday_pause_resume_short_turn_candidate=signal_packet.midday_pause_resume_short_turn_candidate,
        london_late_pause_resume_short_turn_candidate=signal_packet.london_late_pause_resume_short_turn_candidate,
        asia_early_expanded_breakout_retest_hold_short_turn_candidate=signal_packet.asia_early_expanded_breakout_retest_hold_short_turn_candidate,
        asia_early_compressed_pause_resume_short_turn_candidate=signal_packet.asia_early_compressed_pause_resume_short_turn_candidate,
        asia_early_pause_resume_short_turn_candidate=signal_packet.asia_early_pause_resume_short_turn_candidate,
        long_entry_raw=long_entry_raw,
        short_entry_raw=short_entry_raw,
        recent_long_setup=recent_long_setup,
        recent_short_setup=recent_short_setup,
        long_entry=long_entry,
        short_entry=short_entry,
        long_entry_source=long_entry_source,
        short_entry_source=short_entry_source,
    )
