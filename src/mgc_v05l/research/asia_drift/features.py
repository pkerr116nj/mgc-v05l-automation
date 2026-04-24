"""Feature computation and explicit classifiers for Asia Drift v1 Phase 1."""

from __future__ import annotations

from collections import defaultdict, deque
from statistics import fmean, median
from typing import Sequence

from ..trend_participation.models import ResearchBar
from ..trend_participation.state_layers import rolling_atr, rolling_ema
from .models import AsiaDriftCalibrationProfile, AsiaDriftFeatureRow, DriftAssessment, PullbackAssessment
from .session_scope import DEFAULT_SESSION_CONFIG, derive_session_scope


EPS = 1e-9
FAST_EMA_SPAN = 4
SLOW_EMA_SPAN = 9
ATR_WINDOW = 8
WARMUP_BARS = 8

NO_TRADE = "NO_TRADE"
ASIA_DRIFT_LONG = "ASIA_DRIFT_LONG"
ASIA_DRIFT_SHORT = "ASIA_DRIFT_SHORT"

DRIFT_NONE = "NONE"
DRIFT_WEAK = "WEAK"
DRIFT_MEDIUM = "MEDIUM"
DRIFT_STRONG = "STRONG"

NO_PULLBACK = "NO_PULLBACK"
TOO_EXTENDED = "TOO_EXTENDED"
NORMAL_PULLBACK = "NORMAL_PULLBACK"
STRETCHED_BUT_VALID = "STRETCHED_BUT_VALID"
DISQUALIFYING_PULLBACK = "DISQUALIFYING_PULLBACK"

SLOW_OR_STANDARD = "SLOW_OR_STANDARD"
FAST_SHALLOW_VALID = "FAST_SHALLOW_VALID"
FAST_STRETCHED_WARNING = "FAST_STRETCHED_WARNING"
FAST_DEEP_DISQUALIFYING = "FAST_DEEP_DISQUALIFYING"

STRICT_CURRENT = "strict_current"
BALANCED_REVISION = "balanced_revision"
PERSISTENCE_CONFIRMED = "persistence_confirmed"
RECOVERY_CONFIRMED = "recovery_confirmed"
LOOSE_DIAGNOSTIC = "loose_diagnostic"


CALIBRATION_PROFILES: dict[str, AsiaDriftCalibrationProfile] = {
    STRICT_CURRENT: AsiaDriftCalibrationProfile(
        name=STRICT_CURRENT,
        description="Current hard pullback disqualification and immediate invalidation behavior.",
        pullback_speed_warning=9.0,
        pullback_speed_disqualify=0.55,
        pullback_depth_fraction_warning=9.0,
        pullback_depth_fraction_disqualify=0.62,
        pullback_depth_atr_warning=9.0,
        pullback_depth_atr_disqualify=1.25,
        pullback_expansion_warning=9.0,
        pullback_expansion_disqualify=1.35,
        normal_depth_fraction_max=0.45,
        normal_depth_atr_max=0.90,
        normal_speed_max=0.35,
        disqualifying_confirmation_bars=1,
        regime_loss_confirmation_bars=1,
        at_risk_persistence_threshold=0.46,
        invalidation_persistence_threshold=0.32,
        drift_collapse_confirmation_bars=1,
        vwap_reclaim_confirmation_bars=1,
        ema_failure_confirmation_bars=1,
        structure_break_confirmation_bars=1,
        recovery_score_threshold=0.78,
        requalification_score_threshold=0.84,
        max_recovery_bars=1,
    ),
    BALANCED_REVISION: AsiaDriftCalibrationProfile(
        name=BALANCED_REVISION,
        description="Allows normal Asia slop via warning-before-invalidation and wider stretched-but-valid tolerance.",
        pullback_speed_warning=0.55,
        pullback_speed_disqualify=0.78,
        pullback_depth_fraction_warning=0.58,
        pullback_depth_fraction_disqualify=0.72,
        pullback_depth_atr_warning=1.10,
        pullback_depth_atr_disqualify=1.45,
        pullback_expansion_warning=1.30,
        pullback_expansion_disqualify=1.55,
        normal_depth_fraction_max=0.50,
        normal_depth_atr_max=1.00,
        normal_speed_max=0.42,
        disqualifying_confirmation_bars=2,
        regime_loss_confirmation_bars=2,
        at_risk_persistence_threshold=0.50,
        invalidation_persistence_threshold=0.34,
        drift_collapse_confirmation_bars=2,
        vwap_reclaim_confirmation_bars=2,
        ema_failure_confirmation_bars=2,
        structure_break_confirmation_bars=1,
        recovery_score_threshold=0.72,
        requalification_score_threshold=0.80,
        max_recovery_bars=2,
    ),
    PERSISTENCE_CONFIRMED: AsiaDriftCalibrationProfile(
        name=PERSISTENCE_CONFIRMED,
        description=(
            "Treats single adverse events as at-risk first, requiring persistence and follow-through before "
            "hard invalidation while preserving chop and post-spike vetoes."
        ),
        pullback_speed_warning=0.58,
        pullback_speed_disqualify=0.82,
        pullback_depth_fraction_warning=0.60,
        pullback_depth_fraction_disqualify=0.74,
        pullback_depth_atr_warning=1.15,
        pullback_depth_atr_disqualify=1.48,
        pullback_expansion_warning=1.32,
        pullback_expansion_disqualify=1.58,
        normal_depth_fraction_max=0.50,
        normal_depth_atr_max=1.00,
        normal_speed_max=0.42,
        disqualifying_confirmation_bars=2,
        regime_loss_confirmation_bars=3,
        at_risk_persistence_threshold=0.52,
        invalidation_persistence_threshold=0.30,
        drift_collapse_confirmation_bars=3,
        vwap_reclaim_confirmation_bars=2,
        ema_failure_confirmation_bars=2,
        structure_break_confirmation_bars=2,
        recovery_score_threshold=0.66,
        requalification_score_threshold=0.74,
        max_recovery_bars=3,
    ),
    RECOVERY_CONFIRMED: AsiaDriftCalibrationProfile(
        name=RECOVERY_CONFIRMED,
        description=(
            "Requires explicit post-risk recovery evidence before requalification, so recoverable Asia drift can "
            "resume without granting blanket tolerance to weak drift."
        ),
        pullback_speed_warning=0.58,
        pullback_speed_disqualify=0.82,
        pullback_depth_fraction_warning=0.60,
        pullback_depth_fraction_disqualify=0.74,
        pullback_depth_atr_warning=1.15,
        pullback_depth_atr_disqualify=1.48,
        pullback_expansion_warning=1.32,
        pullback_expansion_disqualify=1.58,
        normal_depth_fraction_max=0.50,
        normal_depth_atr_max=1.00,
        normal_speed_max=0.42,
        disqualifying_confirmation_bars=2,
        regime_loss_confirmation_bars=3,
        at_risk_persistence_threshold=0.50,
        invalidation_persistence_threshold=0.29,
        drift_collapse_confirmation_bars=4,
        vwap_reclaim_confirmation_bars=2,
        ema_failure_confirmation_bars=2,
        structure_break_confirmation_bars=2,
        recovery_score_threshold=0.62,
        requalification_score_threshold=0.72,
        max_recovery_bars=4,
    ),
    LOOSE_DIAGNOSTIC: AsiaDriftCalibrationProfile(
        name=LOOSE_DIAGNOSTIC,
        description="Diagnostic-only loose tolerance to expose whether false vetoes dominate the sample.",
        pullback_speed_warning=0.65,
        pullback_speed_disqualify=0.95,
        pullback_depth_fraction_warning=0.66,
        pullback_depth_fraction_disqualify=0.82,
        pullback_depth_atr_warning=1.25,
        pullback_depth_atr_disqualify=1.65,
        pullback_expansion_warning=1.40,
        pullback_expansion_disqualify=1.75,
        normal_depth_fraction_max=0.55,
        normal_depth_atr_max=1.15,
        normal_speed_max=0.50,
        disqualifying_confirmation_bars=2,
        regime_loss_confirmation_bars=2,
        at_risk_persistence_threshold=0.44,
        invalidation_persistence_threshold=0.24,
        drift_collapse_confirmation_bars=3,
        vwap_reclaim_confirmation_bars=3,
        ema_failure_confirmation_bars=3,
        structure_break_confirmation_bars=2,
        recovery_score_threshold=0.58,
        requalification_score_threshold=0.66,
        max_recovery_bars=5,
    ),
}


def build_feature_rows(
    *,
    bars_5m: Sequence[ResearchBar],
    calibration_profile_name: str = STRICT_CURRENT,
) -> list[AsiaDriftFeatureRow]:
    sorted_bars = sorted(bars_5m, key=lambda bar: (bar.instrument, bar.end_ts))
    if not sorted_bars:
        return []
    profile = get_calibration_profile(calibration_profile_name)

    grouped: dict[str, list[ResearchBar]] = defaultdict(list)
    for bar in sorted_bars:
        grouped[bar.instrument].append(bar)

    feature_rows: list[AsiaDriftFeatureRow] = []
    for instrument in sorted(grouped):
        instrument_rows = _build_instrument_feature_rows(grouped[instrument], profile=profile)
        feature_rows.extend(instrument_rows)
    return feature_rows


def get_calibration_profile(name: str) -> AsiaDriftCalibrationProfile:
    try:
        return CALIBRATION_PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown Asia Drift calibration profile: {name}") from exc


def classify_drift(features: AsiaDriftFeatureRow) -> DriftAssessment:
    return DriftAssessment(
        calibration_profile=features.calibration_profile,
        dominant_direction=features.dominant_direction,
        regime=features.regime,
        long_score=features.long_drift_score,
        short_score=features.short_drift_score,
        long_strength=features.long_drift_strength,
        short_strength=features.short_drift_strength,
        score_gap=features.score_gap,
        warmup_complete=features.session_bar_index >= WARMUP_BARS and features.anchor_observed,
        chop_veto=features.chop_veto,
        post_spike_instability=features.post_spike_instability,
        extension_elevated=features.extension_elevated,
        long_reasons=features.drift_long_reasons,
        short_reasons=features.drift_short_reasons,
    )


def classify_pullback(features: AsiaDriftFeatureRow) -> PullbackAssessment:
    return PullbackAssessment(
        calibration_profile=features.calibration_profile,
        direction=features.pullback_direction,
        state=features.pullback_state,
        reason=features.pullback_reason,
        veto_category=features.pullback_veto_category,
        depth_points=features.pullback_depth_points,
        depth_atr=features.pullback_depth_atr,
        depth_fraction=features.pullback_depth_fraction,
        duration_bars=features.pullback_duration_bars,
        speed=features.pullback_speed,
        severity=features.pullback_severity,
        expansion_ratio=features.pullback_expansion_ratio,
        vwap_interaction=features.pullback_vwap_interaction,
        structure_preserved=features.pullback_structure_preserved,
        structure_break=features.pullback_structure_break,
        warning_flag=features.pullback_warning_flag,
        warning_reason=features.pullback_warning_reason,
        warning_category=features.pullback_warning_category,
        hard_invalidation_candidate=features.pullback_hard_invalidation_candidate,
        too_extended=features.pullback_too_extended,
        drift_leg_start=features.drift_leg_start,
        drift_leg_extreme=features.drift_leg_extreme,
        protected_swing_price=features.protected_swing_price,
        retracement_38=features.retracement_38,
        retracement_50=features.retracement_50,
        retracement_62=features.retracement_62,
        envelope_low=features.envelope_low,
        envelope_high=features.envelope_high,
    )


def _build_instrument_feature_rows(
    bars: Sequence[ResearchBar],
    *,
    profile: AsiaDriftCalibrationProfile,
) -> list[AsiaDriftFeatureRow]:
    close_values = [bar.close for bar in bars]
    fast_ema_values = rolling_ema(close_values, span=FAST_EMA_SPAN)
    slow_ema_values = rolling_ema(close_values, span=SLOW_EMA_SPAN)
    atr_values = rolling_atr(bars, window=ATR_WINDOW)

    feature_rows: list[AsiaDriftFeatureRow] = []
    session_bars: dict[str, list[ResearchBar]] = defaultdict(list)
    session_ranges: dict[str, list[float]] = defaultdict(list)
    session_vwap_state: dict[str, tuple[float, float]] = {}
    anchor_observed_by_session: dict[str, bool] = {}
    last_impulse_by_session_direction: dict[tuple[str, str], int] = {}

    for index, bar in enumerate(bars):
        local_time = bar.end_ts.astimezone(DEFAULT_SESSION_CONFIG.timezone).timetz().replace(tzinfo=None)
        session_date = (
            bar.end_ts.astimezone(DEFAULT_SESSION_CONFIG.timezone).date()
            if local_time >= DEFAULT_SESSION_CONFIG.session_anchor
            else (bar.end_ts.astimezone(DEFAULT_SESSION_CONFIG.timezone).date())
        )
        provisional_session_id = f"{bar.instrument.lower()}__{session_date.isoformat()}__asia_drift_v1"
        if provisional_session_id not in anchor_observed_by_session:
            anchor_observed_by_session[provisional_session_id] = local_time == DEFAULT_SESSION_CONFIG.session_anchor

        scope = derive_session_scope(
            instrument=bar.instrument,
            timeframe=bar.timeframe,
            bar_end_ts=bar.end_ts,
            anchor_observed=anchor_observed_by_session[provisional_session_id],
        )
        anchor_observed_by_session[scope.asia_drift_session_id] = (
            anchor_observed_by_session.get(scope.asia_drift_session_id, False) or local_time == DEFAULT_SESSION_CONFIG.session_anchor
        )
        scope = derive_session_scope(
            instrument=bar.instrument,
            timeframe=bar.timeframe,
            bar_end_ts=bar.end_ts,
            anchor_observed=anchor_observed_by_session[scope.asia_drift_session_id],
        )

        session_id = scope.asia_drift_session_id
        session_bars[session_id].append(bar)
        bars_in_session = session_bars[session_id]
        session_ranges[session_id].append(bar.range_points)
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        price_volume, total_volume = session_vwap_state.get(session_id, (0.0, 0.0))
        price_volume += typical_price * max(float(bar.volume), 1.0)
        total_volume += max(float(bar.volume), 1.0)
        session_vwap_state[session_id] = (price_volume, total_volume)
        session_vwap = price_volume / max(total_volume, 1.0)
        atr = max(float(atr_values[index]), EPS)
        fast_ema = float(fast_ema_values[index])
        slow_ema = float(slow_ema_values[index])
        previous_slow = float(slow_ema_values[index - 1]) if index > 0 else slow_ema
        slow_slope = (slow_ema - previous_slow) / atr

        session_open = bars_in_session[0].open
        close_location = (bar.close - bar.low) / max(bar.high - bar.low, EPS)
        signed_session_disp_long = (bar.close - session_open) / atr
        signed_session_disp_short = (session_open - bar.close) / atr
        signed_vwap_disp_long = (bar.close - session_vwap) / atr
        signed_vwap_disp_short = (session_vwap - bar.close) / atr
        slope_3_long = _directional_slope(bars_in_session, direction="LONG", window=3, atr=atr)
        slope_6_long = _directional_slope(bars_in_session, direction="LONG", window=6, atr=atr)
        slope_12_long = _directional_slope(bars_in_session, direction="LONG", window=12, atr=atr)
        slope_3_short = _directional_slope(bars_in_session, direction="SHORT", window=3, atr=atr)
        slope_6_short = _directional_slope(bars_in_session, direction="SHORT", window=6, atr=atr)
        slope_12_short = _directional_slope(bars_in_session, direction="SHORT", window=12, atr=atr)
        slope_combo_long = 0.5 * slope_3_long + 0.3 * slope_6_long + 0.2 * slope_12_long
        slope_combo_short = 0.5 * slope_3_short + 0.3 * slope_6_short + 0.2 * slope_12_short
        efficiency_ratio_12 = _efficiency_ratio(bars_in_session, window=12)
        close_location_persistence_long = _close_location_persistence(bars_in_session, direction="LONG", window=8)
        close_location_persistence_short = _close_location_persistence(bars_in_session, direction="SHORT", window=8)
        bar_overlap_ratio_8 = _bar_overlap_ratio(bars_in_session, window=8)
        reversal_frequency_12 = _reversal_frequency(bars_in_session, window=12)
        directional_persistence_8 = _directional_persistence(bars_in_session, window=8)
        local_realized_volatility = _realized_volatility(bars_in_session, window=6)
        realized_volatility_ratio = _realized_volatility_ratio(bars_in_session, window=6, baseline_window=18)
        upside_extension_atr = max(bar.close - max(fast_ema, session_vwap), 0.0) / atr
        downside_extension_atr = max(min(fast_ema, session_vwap) - bar.close, 0.0) / atr
        chop_veto = (
            abs(signed_session_disp_long) < 0.6
            or efficiency_ratio_12 < 0.30
            or (bar_overlap_ratio_8 > 0.65 and reversal_frequency_12 > 0.30)
            or reversal_frequency_12 > 0.45
        )
        post_spike_instability = _post_spike_instability(
            bars_in_session=bars_in_session,
            atr=atr,
            realized_volatility_ratio=realized_volatility_ratio,
            efficiency_ratio=efficiency_ratio_12,
        )
        long_score, long_reasons = _drift_score(
            direction="LONG",
            signed_session_disp=signed_session_disp_long,
            signed_vwap_disp=signed_vwap_disp_long,
            slope_combo=slope_combo_long,
            efficiency_ratio=efficiency_ratio_12,
            close_location_persistence=close_location_persistence_long,
            bar_overlap_ratio=bar_overlap_ratio_8,
            reversal_frequency=reversal_frequency_12,
            extension_atr=upside_extension_atr,
            realized_volatility_ratio=realized_volatility_ratio,
            post_spike_instability=post_spike_instability,
            directional_persistence=directional_persistence_8,
        )
        short_score, short_reasons = _drift_score(
            direction="SHORT",
            signed_session_disp=signed_session_disp_short,
            signed_vwap_disp=signed_vwap_disp_short,
            slope_combo=slope_combo_short,
            efficiency_ratio=efficiency_ratio_12,
            close_location_persistence=close_location_persistence_short,
            bar_overlap_ratio=bar_overlap_ratio_8,
            reversal_frequency=reversal_frequency_12,
            extension_atr=downside_extension_atr,
            realized_volatility_ratio=realized_volatility_ratio,
            post_spike_instability=post_spike_instability,
            directional_persistence=directional_persistence_8,
        )
        long_strength = _drift_strength(long_score)
        short_strength = _drift_strength(short_score)
        score_gap = abs(long_score - short_score)
        dominant_direction = "LONG" if long_score >= short_score else "SHORT"
        warmup_complete = len(bars_in_session) >= WARMUP_BARS and scope.anchor_observed
        regime = NO_TRADE
        if (
            warmup_complete
            and not chop_veto
            and not post_spike_instability
            and long_score >= 3.1
            and long_score - short_score >= 0.75
        ):
            regime = ASIA_DRIFT_LONG
        elif (
            warmup_complete
            and not chop_veto
            and not post_spike_instability
            and short_score >= 3.1
            and short_score - long_score >= 0.75
        ):
            regime = ASIA_DRIFT_SHORT

        pullback = _pullback_assessment(
            bars_in_session=bars_in_session,
            profile=profile,
            regime=regime,
            close=bar.close,
            high=bar.high,
            low=bar.low,
            session_open=session_open,
            session_vwap=session_vwap,
            fast_ema=fast_ema,
            slow_ema=slow_ema,
            atr=atr,
            session_ranges=session_ranges[session_id],
        )
        relevant_direction = _relevant_direction(regime=regime, dominant_direction=dominant_direction)
        if _is_drift_impulse(
            direction=relevant_direction,
            close=bar.close,
            session_vwap=session_vwap,
            fast_ema=fast_ema,
            signed_vwap_disp_long=signed_vwap_disp_long,
            signed_vwap_disp_short=signed_vwap_disp_short,
            slope_combo_long=slope_combo_long,
            slope_combo_short=slope_combo_short,
            close_location=close_location,
        ):
            last_impulse_by_session_direction[(session_id, relevant_direction)] = len(bars_in_session)
        last_impulse_index = last_impulse_by_session_direction.get((session_id, relevant_direction), len(bars_in_session))
        bars_since_last_drift_impulse = max(len(bars_in_session) - last_impulse_index, 0)
        regime_persistence_score = _regime_persistence_score(
            direction=relevant_direction,
            drift_score=long_score if relevant_direction == "LONG" else short_score,
            signed_vwap_disp=signed_vwap_disp_long if relevant_direction == "LONG" else signed_vwap_disp_short,
            slope_combo=slope_combo_long if relevant_direction == "LONG" else slope_combo_short,
            close_location_persistence=(
                close_location_persistence_long if relevant_direction == "LONG" else close_location_persistence_short
            ),
            pullback=pullback,
            bars_since_last_drift_impulse=bars_since_last_drift_impulse,
            realized_volatility_ratio=realized_volatility_ratio,
            regime=regime,
        )
        regime_persistence_label = _regime_persistence_label(
            score=regime_persistence_score,
            profile=profile,
        )
        renewed_signed_vwap_displacement = signed_vwap_disp_long if relevant_direction == "LONG" else signed_vwap_disp_short
        slope_recovery_score = slope_combo_long if relevant_direction == "LONG" else slope_combo_short
        close_location_recovery_score = (
            close_location_persistence_long if relevant_direction == "LONG" else close_location_persistence_short
        )
        fresh_drift_impulse = bars_since_last_drift_impulse == 0
        countertrend_extension_failed = _countertrend_extension_failed(
            direction=relevant_direction,
            signed_vwap_disp=renewed_signed_vwap_displacement,
            pullback=pullback,
            realized_volatility_ratio=realized_volatility_ratio,
        )
        compression_followed_by_drift_expansion = _compression_followed_by_drift_expansion(
            signed_vwap_disp=renewed_signed_vwap_displacement,
            slope_combo=slope_recovery_score,
            pullback=pullback,
        )
        recovery_score = _recovery_score(
            renewed_signed_vwap_displacement=renewed_signed_vwap_displacement,
            slope_recovery_score=slope_recovery_score,
            close_location_recovery_score=close_location_recovery_score,
            fresh_drift_impulse=fresh_drift_impulse,
            countertrend_extension_failed=countertrend_extension_failed,
            compression_followed_by_drift_expansion=compression_followed_by_drift_expansion,
            regime_persistence_score=regime_persistence_score,
        )
        recovery_label = _recovery_label(score=recovery_score, profile=profile)
        hypothetical_entry_ready = (
            regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}
            and pullback.state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID}
            and scope.entry_window_open
        )
        thesis_invalidated_flag = (
            regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}
            and pullback.hard_invalidation_candidate
        )

        feature_rows.append(
            AsiaDriftFeatureRow(
                calibration_profile=profile.name,
                instrument=bar.instrument,
                timeframe=bar.timeframe,
                decision_ts=bar.end_ts,
                local_session_date=scope.local_session_date,
                asia_drift_session_id=session_id,
                session_bar_index=len(bars_in_session),
                session_time_label=scope.session_time_label,
                subphase=scope.subphase,
                in_scope=scope.in_scope,
                entry_window_open=scope.entry_window_open,
                session_timeout=scope.session_timeout,
                scope_provenance=scope.scope_provenance,
                anchor_observed=scope.anchor_observed,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                range_points=bar.range_points,
                session_open=session_open,
                session_vwap=session_vwap,
                atr=atr,
                fast_ema=fast_ema,
                slow_ema=slow_ema,
                slow_ema_slope=slow_slope,
                session_displacement_atr=(bar.close - session_open) / atr,
                signed_session_displacement_long=signed_session_disp_long,
                signed_session_displacement_short=signed_session_disp_short,
                signed_vwap_displacement_long=signed_vwap_disp_long,
                signed_vwap_displacement_short=signed_vwap_disp_short,
                bars_since_last_drift_impulse=bars_since_last_drift_impulse,
                regime_persistence_score=regime_persistence_score,
                regime_persistence_label=regime_persistence_label,
                recovery_score=recovery_score,
                recovery_label=recovery_label,
                renewed_signed_vwap_displacement=renewed_signed_vwap_displacement,
                slope_recovery_score=slope_recovery_score,
                close_location_recovery_score=close_location_recovery_score,
                fresh_drift_impulse=fresh_drift_impulse,
                countertrend_extension_failed=countertrend_extension_failed,
                compression_followed_by_drift_expansion=compression_followed_by_drift_expansion,
                slope_3_long=slope_3_long,
                slope_6_long=slope_6_long,
                slope_12_long=slope_12_long,
                slope_3_short=slope_3_short,
                slope_6_short=slope_6_short,
                slope_12_short=slope_12_short,
                slope_combo_long=slope_combo_long,
                slope_combo_short=slope_combo_short,
                efficiency_ratio_12=efficiency_ratio_12,
                close_location=close_location,
                close_location_persistence_long=close_location_persistence_long,
                close_location_persistence_short=close_location_persistence_short,
                bar_overlap_ratio_8=bar_overlap_ratio_8,
                reversal_frequency_12=reversal_frequency_12,
                directional_persistence_8=directional_persistence_8,
                local_realized_volatility=local_realized_volatility,
                realized_volatility_ratio=realized_volatility_ratio,
                upside_extension_atr=upside_extension_atr,
                downside_extension_atr=downside_extension_atr,
                long_drift_score=long_score,
                short_drift_score=short_score,
                long_drift_strength=long_strength,
                short_drift_strength=short_strength,
                dominant_direction=dominant_direction,
                regime=regime,
                score_gap=score_gap,
                chop_veto=chop_veto,
                post_spike_instability=post_spike_instability,
                extension_elevated=max(upside_extension_atr, downside_extension_atr) >= 2.2,
                drift_long_reasons=long_reasons,
                drift_short_reasons=short_reasons,
                pullback_direction=pullback.direction,
                pullback_state=pullback.state,
                pullback_reason=pullback.reason,
                pullback_veto_category=pullback.veto_category,
                pullback_depth_points=pullback.depth_points,
                pullback_depth_atr=pullback.depth_atr,
                pullback_depth_fraction=pullback.depth_fraction,
                pullback_duration_bars=pullback.duration_bars,
                pullback_speed=pullback.speed,
                pullback_severity=pullback.severity,
                pullback_expansion_ratio=pullback.expansion_ratio,
                fast_pullback_class=pullback.fast_pullback_class,
                pullback_vwap_interaction=pullback.vwap_interaction,
                pullback_structure_preserved=pullback.structure_preserved,
                pullback_structure_break=pullback.structure_break,
                pullback_warning_flag=pullback.warning_flag,
                pullback_warning_reason=pullback.warning_reason,
                pullback_warning_category=pullback.warning_category,
                pullback_hard_invalidation_candidate=pullback.hard_invalidation_candidate,
                pullback_too_extended=pullback.too_extended,
                drift_leg_start=pullback.drift_leg_start,
                drift_leg_extreme=pullback.drift_leg_extreme,
                protected_swing_price=pullback.protected_swing_price,
                retracement_38=pullback.retracement_38,
                retracement_50=pullback.retracement_50,
                retracement_62=pullback.retracement_62,
                envelope_low=pullback.envelope_low,
                envelope_high=pullback.envelope_high,
                hypothetical_entry_ready=hypothetical_entry_ready,
                thesis_invalidated_flag=thesis_invalidated_flag,
                feature_version=f"asia_drift_v1_phase1:{profile.name}",
            )
        )
    return feature_rows


def _directional_slope(bars: Sequence[ResearchBar], *, direction: str, window: int, atr: float) -> float:
    if len(bars) <= window:
        return 0.0
    raw = (bars[-1].close - bars[-1 - window].close) / max(window * atr, EPS)
    return raw if direction == "LONG" else -raw


def _efficiency_ratio(bars: Sequence[ResearchBar], *, window: int) -> float:
    if len(bars) < 3:
        return 0.0
    effective_window = min(window, len(bars) - 1)
    recent = bars[-(effective_window + 1) :]
    net = abs(recent[-1].close - recent[0].close)
    travel = sum(abs(current.close - previous.close) for previous, current in zip(recent, recent[1:], strict=False))
    return net / max(travel, EPS)


def _close_location_persistence(bars: Sequence[ResearchBar], *, direction: str, window: int) -> float:
    recent = list(bars[-window:])
    if not recent:
        return 0.0
    if direction == "LONG":
        hits = [1.0 for bar in recent if ((bar.close - bar.low) / max(bar.high - bar.low, EPS)) >= 0.60]
    else:
        hits = [1.0 for bar in recent if ((bar.close - bar.low) / max(bar.high - bar.low, EPS)) <= 0.40]
    return sum(hits) / max(len(recent), 1)


def _bar_overlap_ratio(bars: Sequence[ResearchBar], *, window: int) -> float:
    recent = list(bars[-window:])
    if len(recent) < 2:
        return 0.0
    overlaps: list[float] = []
    for previous, current in zip(recent, recent[1:], strict=False):
        numerator = max(0.0, min(previous.high, current.high) - max(previous.low, current.low))
        denominator = max(previous.high - previous.low, current.high - current.low, EPS)
        overlaps.append(numerator / denominator)
    return fmean(overlaps) if overlaps else 0.0


def _reversal_frequency(bars: Sequence[ResearchBar], *, window: int) -> float:
    recent = list(bars[-(window + 1) :])
    if len(recent) < 3:
        return 0.0
    signs: list[int] = []
    for previous, current in zip(recent, recent[1:], strict=False):
        delta = current.close - previous.close
        if delta > 0:
            signs.append(1)
        elif delta < 0:
            signs.append(-1)
        else:
            signs.append(0)
    reversals = 0
    previous_sign = 0
    for sign in signs:
        if sign == 0:
            continue
        if previous_sign != 0 and sign == -previous_sign:
            reversals += 1
        previous_sign = sign
    return reversals / max(len(signs) - 1, 1)


def _directional_persistence(bars: Sequence[ResearchBar], *, window: int) -> float:
    recent = list(bars[-(window + 1) :])
    if len(recent) < 2:
        return 0.0
    signed_steps = [current.close - previous.close for previous, current in zip(recent, recent[1:], strict=False)]
    return abs(sum(signed_steps)) / max(sum(abs(step) for step in signed_steps), EPS)


def _realized_volatility(bars: Sequence[ResearchBar], *, window: int) -> float:
    recent = list(bars[-(window + 1) :])
    if len(recent) < 2:
        return 0.0
    returns = [current.close - previous.close for previous, current in zip(recent, recent[1:], strict=False)]
    return (sum(value * value for value in returns) / max(len(returns), 1)) ** 0.5


def _realized_volatility_ratio(bars: Sequence[ResearchBar], *, window: int, baseline_window: int) -> float:
    recent = list(bars[-(baseline_window + 1) :])
    if len(recent) < 3:
        return 1.0
    absolute_returns = [abs(current.close - previous.close) for previous, current in zip(recent, recent[1:], strict=False)]
    local = fmean(absolute_returns[-window:]) if absolute_returns[-window:] else 0.0
    baseline = median(absolute_returns) if absolute_returns else 0.0
    return local / max(baseline, EPS)


def _post_spike_instability(
    *,
    bars_in_session: Sequence[ResearchBar],
    atr: float,
    realized_volatility_ratio: float,
    efficiency_ratio: float,
) -> bool:
    recent = list(bars_in_session[-3:])
    if not recent:
        return False
    extreme_range = max((bar.high - bar.low) / max(atr, EPS) for bar in recent)
    return extreme_range >= 2.0 and (realized_volatility_ratio >= 1.8 or efficiency_ratio < 0.45)


def _drift_score(
    *,
    direction: str,
    signed_session_disp: float,
    signed_vwap_disp: float,
    slope_combo: float,
    efficiency_ratio: float,
    close_location_persistence: float,
    bar_overlap_ratio: float,
    reversal_frequency: float,
    extension_atr: float,
    realized_volatility_ratio: float,
    post_spike_instability: bool,
    directional_persistence: float,
) -> tuple[float, tuple[str, ...]]:
    x1 = _clip(signed_session_disp / 2.0, 0.0, 1.5)
    x2 = _clip(signed_vwap_disp / 1.0, 0.0, 1.0)
    x3 = _clip(slope_combo / 0.35, 0.0, 1.25)
    x4 = _clip((efficiency_ratio - 0.30) / 0.40, 0.0, 1.0)
    x5 = _clip((close_location_persistence - 0.50) / 0.40, 0.0, 1.0)
    x6 = _clip((0.60 - bar_overlap_ratio) / 0.35, 0.0, 1.0)
    x7 = _clip((0.55 - reversal_frequency) / 0.35, 0.0, 1.0)

    penalties = 0.0
    reasons: list[str] = []
    if signed_session_disp >= 1.0:
        reasons.append("session_displacement_material")
    if signed_vwap_disp >= 0.15:
        reasons.append("vwap_displacement_positive")
    if slope_combo >= 0.20:
        reasons.append("multi_window_slope_aligned")
    if efficiency_ratio >= 0.38:
        reasons.append("efficiency_supportive")
    if close_location_persistence >= 0.55:
        reasons.append("close_location_persistent")
    if bar_overlap_ratio <= 0.55:
        reasons.append("overlap_not_choppy")
    if reversal_frequency <= 0.35:
        reasons.append("reversal_frequency_controlled")
    if directional_persistence >= 0.40:
        reasons.append("directional_persistence_supportive")

    if extension_atr > 2.2:
        penalties += 0.6
        reasons.append("extension_elevated_penalty")
    if post_spike_instability:
        penalties += 0.8
        reasons.append("post_spike_instability_penalty")
    if realized_volatility_ratio > 1.8 and signed_session_disp < 1.2:
        penalties += 0.4
        reasons.append("volatility_surge_penalty")
    if reversal_frequency > 0.35 and bar_overlap_ratio > 0.50:
        penalties += 0.5
        reasons.append("countertrend_quality_penalty")

    score = 1.25 * x1 + 1.0 * x2 + 1.0 * x3 + 0.75 * x4 + 0.50 * x5 + 0.50 * x6 + 0.50 * x7 - penalties
    return score, tuple(reasons)


def _drift_strength(score: float) -> str:
    if score >= 3.9:
        return DRIFT_STRONG
    if score >= 3.1:
        return DRIFT_MEDIUM
    if score >= 2.4:
        return DRIFT_WEAK
    return DRIFT_NONE


def _pullback_assessment(
    *,
    bars_in_session: Sequence[ResearchBar],
    profile: AsiaDriftCalibrationProfile,
    regime: str,
    close: float,
    high: float,
    low: float,
    session_open: float,
    session_vwap: float,
    fast_ema: float,
    slow_ema: float,
    atr: float,
    session_ranges: Sequence[float],
) -> PullbackAssessment:
    if regime == ASIA_DRIFT_SHORT:
        direction = "SHORT"
        session_extreme = min(bar.low for bar in bars_in_session)
        extreme_index = max(index for index, bar in enumerate(bars_in_session) if bar.low == session_extreme)
        leg_start = session_open
        leg_size = max(session_open - session_extreme, EPS)
        depth_points = max(close - session_extreme, 0.0)
        protected_swing = max(bar.high for bar in bars_in_session[max(0, len(bars_in_session) - 4) :])
        vwap_interaction = _pullback_vwap_interaction(bars_in_session, direction="SHORT", session_vwap=session_vwap, slow_ema=slow_ema)
        structure_break_reason = _structure_break_reason(
            direction=direction,
            close=close,
            protected_swing=protected_swing,
            vwap_interaction=vwap_interaction,
        )
        structure_break = structure_break_reason is not None
    else:
        direction = "LONG"
        session_extreme = max(bar.high for bar in bars_in_session)
        extreme_index = max(index for index, bar in enumerate(bars_in_session) if bar.high == session_extreme)
        leg_start = session_open
        leg_size = max(session_extreme - session_open, EPS)
        depth_points = max(session_extreme - close, 0.0)
        protected_swing = min(bar.low for bar in bars_in_session[max(0, len(bars_in_session) - 4) :])
        vwap_interaction = _pullback_vwap_interaction(bars_in_session, direction="LONG", session_vwap=session_vwap, slow_ema=slow_ema)
        structure_break_reason = _structure_break_reason(
            direction=direction,
            close=close,
            protected_swing=protected_swing,
            vwap_interaction=vwap_interaction,
        )
        structure_break = structure_break_reason is not None

    depth_atr = depth_points / max(atr, EPS)
    depth_fraction = depth_points / max(leg_size, EPS)
    duration_bars = max(len(bars_in_session) - 1 - extreme_index, 0)
    speed = depth_atr / max(duration_bars, 1)
    pullback_ranges = list(session_ranges[extreme_index + 1 :]) if duration_bars > 0 else []
    baseline_ranges = list(session_ranges[max(0, extreme_index - 12) : extreme_index + 1])
    expansion_ratio = (
        fmean(pullback_ranges) / max(median(baseline_ranges), EPS)
        if pullback_ranges and baseline_ranges
        else 1.0
    )
    too_extended = duration_bars == 0 and (
        (close - max(fast_ema, session_vwap)) / max(atr, EPS) > 2.2
        if direction == "LONG"
        else (min(fast_ema, session_vwap) - close) / max(atr, EPS) > 2.2
    )
    structure_preserved = not structure_break
    severity = max(depth_atr, speed, max(expansion_ratio - 1.0, 0.0))
    veto_category: str | None = None
    warning_flag = False
    warning_reason: str | None = None
    warning_category: str | None = None
    hard_invalidation_candidate = False
    fast_pullback_class = _fast_pullback_class(
        profile=profile,
        depth_fraction=depth_fraction,
        depth_atr=depth_atr,
        speed=speed,
        expansion_ratio=expansion_ratio,
        vwap_interaction=vwap_interaction,
        structure_break=structure_break,
    )

    retracement_38 = session_extreme - 0.382 * leg_size if direction == "LONG" else session_extreme + 0.382 * leg_size
    retracement_50 = session_extreme - 0.500 * leg_size if direction == "LONG" else session_extreme + 0.500 * leg_size
    retracement_62 = session_extreme - 0.618 * leg_size if direction == "LONG" else session_extreme + 0.618 * leg_size
    envelope_low = min(retracement_62, min(session_vwap, fast_ema) - 0.10 * atr)
    envelope_high = max(retracement_38, max(session_vwap, fast_ema) + 0.10 * atr)

    if too_extended:
        state = TOO_EXTENDED
        reason = "too_extended_without_reset"
    elif structure_break:
        state = DISQUALIFYING_PULLBACK
        reason = structure_break_reason
        veto_category = _reason_category(reason)
        hard_invalidation_candidate = True
    elif depth_fraction > profile.pullback_depth_fraction_disqualify or depth_atr > profile.pullback_depth_atr_disqualify:
        state = DISQUALIFYING_PULLBACK
        reason = "depth_exceeds_limit"
        veto_category = "DEPTH"
    elif speed > profile.pullback_speed_disqualify:
        if fast_pullback_class == FAST_SHALLOW_VALID:
            state = NORMAL_PULLBACK
            reason = "fast_shallow_valid"
        elif fast_pullback_class == FAST_STRETCHED_WARNING:
            state = STRETCHED_BUT_VALID
            reason = "fast_stretched_warning"
        else:
            state = DISQUALIFYING_PULLBACK
            reason = "fast_deep_disqualifying"
            veto_category = "SPEED"
    elif expansion_ratio > profile.pullback_expansion_disqualify:
        state = DISQUALIFYING_PULLBACK
        reason = "violent_countertrend_expansion"
        veto_category = "EXPANSION"
    elif depth_fraction < 0.18 and depth_atr < 0.30:
        state = NO_PULLBACK
        reason = "too_shallow"
    elif depth_fraction <= profile.normal_depth_fraction_max and depth_atr <= profile.normal_depth_atr_max and speed <= profile.normal_speed_max:
        state = NORMAL_PULLBACK
        reason = None
    else:
        state = STRETCHED_BUT_VALID
        reason = "deep_but_structure_intact"

    if state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID}:
        warning_reason, warning_category = _pullback_warning_reason(
            profile=profile,
            depth_fraction=depth_fraction,
            depth_atr=depth_atr,
            speed=speed,
            expansion_ratio=expansion_ratio,
            vwap_interaction=vwap_interaction,
            fast_pullback_class=fast_pullback_class,
        )
        warning_flag = warning_reason is not None

    return PullbackAssessment(
        calibration_profile=profile.name,
        direction=direction,
        state=state,
        reason=reason,
        veto_category=veto_category,
        depth_points=depth_points,
        depth_atr=depth_atr,
        depth_fraction=depth_fraction,
        duration_bars=duration_bars,
        speed=speed,
        severity=severity,
        expansion_ratio=expansion_ratio,
        fast_pullback_class=fast_pullback_class,
        vwap_interaction=vwap_interaction,
        structure_preserved=structure_preserved,
        structure_break=structure_break,
        warning_flag=warning_flag,
        warning_reason=warning_reason,
        warning_category=warning_category,
        hard_invalidation_candidate=hard_invalidation_candidate,
        too_extended=too_extended,
        drift_leg_start=leg_start,
        drift_leg_extreme=session_extreme,
        protected_swing_price=protected_swing,
        retracement_38=retracement_38,
        retracement_50=retracement_50,
        retracement_62=retracement_62,
        envelope_low=envelope_low,
        envelope_high=envelope_high,
    )


def _pullback_vwap_interaction(
    bars_in_session: Sequence[ResearchBar],
    *,
    direction: str,
    session_vwap: float,
    slow_ema: float,
) -> str:
    recent = list(bars_in_session[-2:])
    if direction == "LONG":
        closes_through = [bar.close < session_vwap for bar in recent]
        if len(closes_through) >= 2 and all(closes_through):
            return "CONFIRMED_CLOSES_THROUGH_VWAP"
        if recent and recent[-1].close < session_vwap and recent[-1].close < slow_ema:
            return "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"
        if recent and recent[-1].close < session_vwap:
            return "SINGLE_CLOSE_THROUGH_VWAP"
        if recent and recent[-1].low <= session_vwap <= recent[-1].high:
            return "VWAP_TAG"
        return "FAVORABLE_SIDE_OF_VWAP"
    closes_through = [bar.close > session_vwap for bar in recent]
    if len(closes_through) >= 2 and all(closes_through):
        return "CONFIRMED_CLOSES_THROUGH_VWAP"
    if recent and recent[-1].close > session_vwap and recent[-1].close > slow_ema:
        return "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"
    if recent and recent[-1].close > session_vwap:
        return "SINGLE_CLOSE_THROUGH_VWAP"
    if recent and recent[-1].low <= session_vwap <= recent[-1].high:
        return "VWAP_TAG"
    return "FAVORABLE_SIDE_OF_VWAP"


def _structure_break_reason(
    *,
    direction: str,
    close: float,
    protected_swing: float,
    vwap_interaction: str,
) -> str | None:
    if direction == "LONG" and close < protected_swing:
        return "protected_swing_break"
    if direction == "SHORT" and close > protected_swing:
        return "protected_swing_break"
    if vwap_interaction == "CONFIRMED_CLOSES_THROUGH_VWAP":
        return "confirmed_vwap_reclaim"
    if vwap_interaction == "CLOSE_THROUGH_VWAP_AND_SLOW_EMA":
        return "vwap_and_slow_ema_failure"
    return None


def _pullback_warning_reason(
    *,
    profile: AsiaDriftCalibrationProfile,
    depth_fraction: float,
    depth_atr: float,
    speed: float,
    expansion_ratio: float,
    vwap_interaction: str,
    fast_pullback_class: str,
) -> tuple[str | None, str | None]:
    if fast_pullback_class == FAST_STRETCHED_WARNING:
        return "fast_stretched_warning", "SPEED"
    if fast_pullback_class == FAST_SHALLOW_VALID:
        return None, None
    if vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP":
        return "single_close_through_vwap", "VWAP_INTERACTION"
    if depth_fraction > profile.pullback_depth_fraction_warning or depth_atr > profile.pullback_depth_atr_warning:
        return "depth_warning", "DEPTH"
    if speed > profile.pullback_speed_warning:
        return "speed_warning", "SPEED"
    if expansion_ratio > profile.pullback_expansion_warning:
        return "expansion_warning", "EXPANSION"
    return None, None


def _fast_pullback_class(
    *,
    profile: AsiaDriftCalibrationProfile,
    depth_fraction: float,
    depth_atr: float,
    speed: float,
    expansion_ratio: float,
    vwap_interaction: str,
    structure_break: bool,
) -> str:
    if speed <= profile.pullback_speed_warning:
        return SLOW_OR_STANDARD
    if structure_break:
        return FAST_DEEP_DISQUALIFYING
    if vwap_interaction in {"CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}:
        return FAST_DEEP_DISQUALIFYING
    if depth_fraction > profile.pullback_depth_fraction_disqualify or depth_atr > profile.pullback_depth_atr_disqualify:
        return FAST_DEEP_DISQUALIFYING
    if expansion_ratio > profile.pullback_expansion_disqualify:
        return FAST_DEEP_DISQUALIFYING

    shallow_depth = depth_fraction <= profile.normal_depth_fraction_max and depth_atr <= profile.normal_depth_atr_max
    warning_depth = depth_fraction <= profile.pullback_depth_fraction_warning and depth_atr <= profile.pullback_depth_atr_warning
    favorable_vwap = vwap_interaction in {"FAVORABLE_SIDE_OF_VWAP", "VWAP_TAG"}
    recoverable_vwap = favorable_vwap or vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP"
    compressed = expansion_ratio <= profile.pullback_expansion_warning

    if shallow_depth and favorable_vwap and compressed:
        return FAST_SHALLOW_VALID
    if warning_depth and recoverable_vwap and compressed:
        return FAST_STRETCHED_WARNING
    return FAST_DEEP_DISQUALIFYING


def _relevant_direction(*, regime: str, dominant_direction: str) -> str:
    if regime == ASIA_DRIFT_SHORT:
        return "SHORT"
    return "LONG" if regime == ASIA_DRIFT_LONG else dominant_direction


def _is_drift_impulse(
    *,
    direction: str,
    close: float,
    session_vwap: float,
    fast_ema: float,
    signed_vwap_disp_long: float,
    signed_vwap_disp_short: float,
    slope_combo_long: float,
    slope_combo_short: float,
    close_location: float,
) -> bool:
    if direction == "LONG":
        return (
            close >= max(session_vwap, fast_ema)
            and signed_vwap_disp_long >= 0.05
            and slope_combo_long >= 0.12
            and close_location >= 0.55
        )
    return (
        close <= min(session_vwap, fast_ema)
        and signed_vwap_disp_short >= 0.05
        and slope_combo_short >= 0.12
        and close_location <= 0.45
    )


def _regime_persistence_score(
    *,
    direction: str,
    drift_score: float,
    signed_vwap_disp: float,
    slope_combo: float,
    close_location_persistence: float,
    pullback: PullbackAssessment,
    bars_since_last_drift_impulse: int,
    realized_volatility_ratio: float,
    regime: str,
) -> float:
    drift_component = _clip(drift_score / 4.1, 0.0, 1.0)
    vwap_component = _clip((signed_vwap_disp + 0.10) / 0.70, 0.0, 1.0)
    slope_component = _clip((slope_combo + 0.05) / 0.35, 0.0, 1.0)
    close_component = _clip((close_location_persistence - 0.40) / 0.40, 0.0, 1.0)
    compression_component = _clip((1.55 - pullback.expansion_ratio) / 0.70, 0.0, 1.0)
    freshness_component = _clip((6.0 - float(bars_since_last_drift_impulse)) / 6.0, 0.0, 1.0)
    score = (
        0.28 * drift_component
        + 0.18 * vwap_component
        + 0.18 * slope_component
        + 0.12 * close_component
        + 0.12 * compression_component
        + 0.12 * freshness_component
    )

    penalties = 0.0
    if regime == NO_TRADE:
        penalties += 0.10
    if pullback.vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP":
        penalties += 0.06
    if pullback.vwap_interaction == "CONFIRMED_CLOSES_THROUGH_VWAP":
        penalties += 0.15
    if pullback.vwap_interaction == "CLOSE_THROUGH_VWAP_AND_SLOW_EMA":
        penalties += 0.22
    if pullback.structure_break:
        penalties += 0.28
    if pullback.reason == "violent_countertrend_expansion":
        penalties += 0.16
    if realized_volatility_ratio > 1.8 and pullback.expansion_ratio > 1.2:
        penalties += 0.08
    return _clip(score - penalties, 0.0, 1.0)


def _regime_persistence_label(*, score: float, profile: AsiaDriftCalibrationProfile) -> str:
    if score >= profile.at_risk_persistence_threshold + 0.10:
        return "PERSISTENT"
    if score >= profile.invalidation_persistence_threshold:
        return "AT_RISK"
    return "COLLAPSING"


def _countertrend_extension_failed(
    *,
    direction: str,
    signed_vwap_disp: float,
    pullback: PullbackAssessment,
    realized_volatility_ratio: float,
) -> bool:
    return (
        signed_vwap_disp >= 0.05
        and pullback.expansion_ratio <= 1.15
        and pullback.depth_atr <= 1.10
        and not pullback.structure_break
        and realized_volatility_ratio <= 1.75
        and direction in {"LONG", "SHORT"}
    )


def _compression_followed_by_drift_expansion(
    *,
    signed_vwap_disp: float,
    slope_combo: float,
    pullback: PullbackAssessment,
) -> bool:
    return (
        signed_vwap_disp >= 0.08
        and slope_combo >= 0.14
        and pullback.expansion_ratio <= 1.05
        and pullback.speed <= 0.42
    )


def _recovery_score(
    *,
    renewed_signed_vwap_displacement: float,
    slope_recovery_score: float,
    close_location_recovery_score: float,
    fresh_drift_impulse: bool,
    countertrend_extension_failed: bool,
    compression_followed_by_drift_expansion: bool,
    regime_persistence_score: float,
) -> float:
    score = (
        0.22 * _clip((renewed_signed_vwap_displacement + 0.02) / 0.45, 0.0, 1.0)
        + 0.20 * _clip((slope_recovery_score + 0.02) / 0.28, 0.0, 1.0)
        + 0.16 * _clip((close_location_recovery_score - 0.42) / 0.40, 0.0, 1.0)
        + 0.18 * regime_persistence_score
        + (0.12 if fresh_drift_impulse else 0.0)
        + (0.07 if countertrend_extension_failed else 0.0)
        + (0.05 if compression_followed_by_drift_expansion else 0.0)
    )
    return _clip(score, 0.0, 1.0)


def _recovery_label(*, score: float, profile: AsiaDriftCalibrationProfile) -> str:
    if score >= profile.requalification_score_threshold:
        return "REQUALIFY"
    if score >= profile.recovery_score_threshold:
        return "RECOVERING"
    return "UNRECOVERED"


def _reason_category(reason: str | None) -> str | None:
    mapping = {
        "protected_swing_break": "STRUCTURE_DAMAGE",
        "confirmed_vwap_reclaim": "VWAP_RECLAIM",
        "vwap_and_slow_ema_failure": "EMA_FAILURE",
        "depth_exceeds_limit": "DEPTH",
        "pullback_too_fast": "SPEED",
        "violent_countertrend_expansion": "EXPANSION",
    }
    if reason is None:
        return None
    return mapping.get(reason, "OTHER")


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))
