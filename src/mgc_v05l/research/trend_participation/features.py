"""Interpretable multi-timeframe feature generation for trend participation research."""

from __future__ import annotations

from collections import deque
from datetime import timedelta
from statistics import fmean
from typing import Iterable

from ...app.session_phase_labels import label_session_phase
from .models import FeatureState, ResearchBar
from .state_layers import (
    EMA_FAST_SPAN,
    EMA_SLOW_SPAN,
    classify_bias,
    classify_pullback,
    rolling_atr,
    rolling_ema,
)


def build_feature_states(
    *,
    bars_5m: list[ResearchBar],
    bars_1m: list[ResearchBar],
) -> list[FeatureState]:
    """Build semi-discrete feature states keyed off the 5m decision bar."""
    sorted_5m = sorted(bars_5m, key=lambda bar: bar.end_ts)
    sorted_1m = sorted(bars_1m, key=lambda bar: bar.end_ts)
    if not sorted_5m:
        return []

    one_minute_cursor = 0
    recent_1m_window: deque[ResearchBar] = deque()
    feature_rows: list[FeatureState] = []
    first_open_by_segment: dict[tuple[str, str], float] = {}
    session_vwap_state: dict[tuple[str, str], tuple[float, float]] = {}
    close_values = [bar.close for bar in sorted_5m]
    fast_ema_values = rolling_ema(close_values, span=EMA_FAST_SPAN)
    slow_ema_values = rolling_ema(close_values, span=EMA_SLOW_SPAN)
    atr_values = rolling_atr(sorted_5m)

    for index, bar in enumerate(sorted_5m):
        recent_1m_cutoff = bar.end_ts - timedelta(minutes=5)
        while one_minute_cursor < len(sorted_1m) and sorted_1m[one_minute_cursor].end_ts <= bar.end_ts:
            recent_1m_window.append(sorted_1m[one_minute_cursor])
            one_minute_cursor += 1
        while recent_1m_window and recent_1m_window[0].end_ts <= recent_1m_cutoff:
            recent_1m_window.popleft()

        trailing_5m = sorted_5m[max(0, index - 11) : index + 1]
        recent_1m = list(recent_1m_window)[-5:]
        average_range = max(fmean(item.range_points for item in trailing_5m), 1e-9)
        slope_norm = _normalized_slope(sorted_5m, index, window=3, average_range=average_range)
        one_minute_slope = _normalized_embedded_slope(recent_1m, average_range=average_range)
        trend_state = _classify_slope_state(slope_norm)
        pullback_depth = _pullback_depth_norm(sorted_5m, index, average_range=average_range, trend_state=trend_state)
        expansion_ratio = bar.range_points / average_range
        expansion_state = _classify_expansion_state(expansion_ratio)
        bar_anatomy = _classify_bar_anatomy(bar)
        momentum_persistence = _classify_persistence(sorted_5m, index)
        distance_high = _distance_from_recent_extreme(sorted_5m, index, average_range=average_range, use_high=True)
        distance_low = _distance_from_recent_extreme(sorted_5m, index, average_range=average_range, use_high=False)
        session_label = label_session_phase(bar.end_ts)
        session_segment = _base_session_segment(session_label)
        first_open_by_segment.setdefault((bar.instrument, session_segment), bar.open)
        session_key = (bar.instrument, session_segment)
        vwap_price_volume, vwap_volume = session_vwap_state.get(session_key, (0.0, 0.0))
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        vwap_price_volume += typical_price * max(float(bar.volume), 1.0)
        vwap_volume += max(float(bar.volume), 1.0)
        session_vwap_state[session_key] = (vwap_price_volume, vwap_volume)
        session_vwap = vwap_price_volume / max(vwap_volume, 1.0)
        distance_session_open = _distance_from_session_open_fast(
            bar=bar,
            average_range=average_range,
            session_segment=session_segment,
            first_open_by_segment=first_open_by_segment,
        )
        reference_state = _classify_reference_state(
            bar=bar,
            distance_high=distance_high,
            distance_low=distance_low,
            distance_session_open=distance_session_open,
        )
        mtf_agreement = _classify_mtf_agreement(slope_norm=slope_norm, one_minute_slope=one_minute_slope)
        volatility_range_state = _classify_volatility_range_state(expansion_ratio)
        regime_bucket = _classify_regime(trend_state=trend_state, momentum_persistence=momentum_persistence)
        volatility_bucket = _classify_volatility_bucket(expansion_ratio)
        direction_bias = _classify_direction_bias(trend_state=trend_state, mtf_agreement=mtf_agreement)
        bias_assessment = classify_bias(
            bars=sorted_5m,
            index=index,
            fast_ema=fast_ema_values[index],
            slow_ema=slow_ema_values[index],
            prev_slow_ema=slow_ema_values[index - 1] if index > 0 else None,
            session_vwap=session_vwap,
            atr=atr_values[index],
        )
        pullback_assessment = classify_pullback(
            bars=sorted_5m,
            index=index,
            bias=bias_assessment,
            atr=atr_values[index],
        )
        local_dt = bar.end_ts.astimezone().replace(tzinfo=bar.end_ts.tzinfo)

        feature_rows.append(
            FeatureState(
                instrument=bar.instrument,
                timeframe=bar.timeframe,
                decision_ts=bar.end_ts,
                session_date=local_dt.date(),
                session_label=session_label,
                session_segment=session_segment,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                range_points=bar.range_points,
                average_range=average_range,
                slope_norm=slope_norm,
                pullback_depth_norm=pullback_depth,
                expansion_ratio=expansion_ratio,
                one_minute_slope_norm=one_minute_slope,
                distance_from_recent_high_norm=distance_high,
                distance_from_recent_low_norm=distance_low,
                distance_from_session_open_norm=distance_session_open,
                trend_state=trend_state,
                pullback_state=_classify_pullback_state(pullback_depth),
                expansion_state=expansion_state,
                bar_anatomy=bar_anatomy,
                momentum_persistence=momentum_persistence,
                reference_state=reference_state,
                volatility_range_state=volatility_range_state,
                mtf_agreement_state=mtf_agreement,
                regime_bucket=regime_bucket,
                volatility_bucket=volatility_bucket,
                direction_bias=direction_bias,
                atp_bias_state=bias_assessment.state,
                atp_bias_score=bias_assessment.score,
                atp_bias_reasons=bias_assessment.reasons,
                atp_long_bias_blockers=bias_assessment.long_blockers,
                atp_short_bias_blockers=bias_assessment.short_blockers,
                atp_fast_ema=bias_assessment.fast_ema,
                atp_slow_ema=bias_assessment.slow_ema,
                atp_slow_ema_slope_norm=bias_assessment.slow_ema_slope_norm,
                atp_session_vwap=bias_assessment.session_vwap,
                atp_directional_persistence_score=bias_assessment.directional_persistence_score,
                atp_trend_extension_norm=bias_assessment.trend_extension_norm,
                atp_pullback_state=pullback_assessment.state,
                atp_pullback_envelope_state=pullback_assessment.envelope_state,
                atp_pullback_reason=pullback_assessment.reason,
                atp_pullback_depth_points=pullback_assessment.depth_points,
                atp_pullback_depth_score=pullback_assessment.depth_score,
                atp_pullback_violence_score=pullback_assessment.violence_score,
                atp_pullback_min_reset_depth=pullback_assessment.min_reset_depth,
                atp_pullback_standard_depth=pullback_assessment.standard_depth,
                atp_pullback_stretched_depth=pullback_assessment.stretched_depth,
                atp_pullback_disqualify_depth=pullback_assessment.disqualify_depth,
                atp_pullback_retracement_ratio=pullback_assessment.retracement_ratio,
                atp_countertrend_velocity_norm=pullback_assessment.countertrend_velocity_norm,
                atp_countertrend_range_expansion=pullback_assessment.countertrend_range_expansion,
                atp_structure_damage=pullback_assessment.structure_damage,
                atp_reference_displacement=pullback_assessment.reference_displacement,
            )
        )

    return feature_rows


def _normalized_slope(bars: list[ResearchBar], index: int, *, window: int, average_range: float) -> float:
    start_index = index - window
    if start_index < 0:
        return 0.0
    return (bars[index].close - bars[start_index].close) / max(average_range, 1e-9)


def _normalized_embedded_slope(bars: Iterable[ResearchBar], *, average_range: float) -> float:
    items = list(bars)
    if len(items) < 2:
        return 0.0
    return (items[-1].close - items[0].close) / max(average_range, 1e-9)


def _pullback_depth_norm(bars: list[ResearchBar], index: int, *, average_range: float, trend_state: str) -> float:
    start_index = max(0, index - 6)
    window = bars[start_index : index + 1]
    if not window:
        return 0.0
    if trend_state in {"UP", "STRONG_UP"}:
        recent_high = max(item.high for item in window)
        return max(recent_high - bars[index].close, 0.0) / max(average_range, 1e-9)
    if trend_state in {"DOWN", "STRONG_DOWN"}:
        recent_low = min(item.low for item in window)
        return max(bars[index].close - recent_low, 0.0) / max(average_range, 1e-9)
    recent_mid = (max(item.high for item in window) + min(item.low for item in window)) / 2.0
    return abs(bars[index].close - recent_mid) / max(average_range, 1e-9)


def _distance_from_recent_extreme(
    bars: list[ResearchBar],
    index: int,
    *,
    average_range: float,
    use_high: bool,
) -> float:
    window = bars[max(0, index - 10) : index + 1]
    if not window:
        return 0.0
    extreme = max(item.high for item in window) if use_high else min(item.low for item in window)
    raw_distance = extreme - bars[index].close if use_high else bars[index].close - extreme
    return raw_distance / max(average_range, 1e-9)


def _distance_from_session_open_fast(
    *,
    bar: ResearchBar,
    average_range: float,
    session_segment: str,
    first_open_by_segment: dict[tuple[str, str], float],
) -> float:
    session_open = first_open_by_segment.get((bar.instrument, session_segment))
    if session_open is None:
        return 0.0
    return (bar.close - session_open) / max(average_range, 1e-9)


def _classify_slope_state(value: float) -> str:
    if value >= 1.2:
        return "STRONG_UP"
    if value >= 0.35:
        return "UP"
    if value <= -1.2:
        return "STRONG_DOWN"
    if value <= -0.35:
        return "DOWN"
    return "FLAT"


def _classify_pullback_state(value: float) -> str:
    if value <= 0.5:
        return "SHALLOW"
    if value <= 1.25:
        return "MODERATE"
    return "DEEP"


def _classify_expansion_state(value: float) -> str:
    if value <= 0.75:
        return "COMPRESSED"
    if value >= 1.6:
        return "EXPANDED"
    return "NORMAL"


def _classify_bar_anatomy(bar: ResearchBar) -> str:
    range_points = max(bar.range_points, 1e-9)
    body_ratio = bar.body_points / range_points
    close_location = (bar.close - bar.low) / range_points
    if body_ratio >= 0.6 and close_location >= 0.7:
        return "BULL_IMPULSE"
    if body_ratio >= 0.6 and close_location <= 0.3:
        return "BEAR_IMPULSE"
    upper_wick = max(bar.high - max(bar.open, bar.close), 0.0) / range_points
    lower_wick = max(min(bar.open, bar.close) - bar.low, 0.0) / range_points
    if upper_wick >= 0.4 and close_location <= 0.45:
        return "UPPER_REJECTION"
    if lower_wick >= 0.4 and close_location >= 0.55:
        return "LOWER_REJECTION"
    return "BALANCED"


def _classify_persistence(bars: list[ResearchBar], index: int) -> str:
    if index < 3:
        return "MIXED"
    window = bars[index - 3 : index + 1]
    up_closes = sum(1 for item in window if item.close >= item.open)
    down_closes = len(window) - up_closes
    if up_closes >= 3:
        return "PERSISTENT_UP"
    if down_closes >= 3:
        return "PERSISTENT_DOWN"
    return "MIXED"


def _classify_reference_state(
    *,
    bar: ResearchBar,
    distance_high: float,
    distance_low: float,
    distance_session_open: float,
) -> str:
    if distance_high <= 0.25:
        return "NEAR_RECENT_HIGH"
    if distance_low <= 0.25:
        return "NEAR_RECENT_LOW"
    if distance_session_open >= 0.35:
        return "ABOVE_SESSION_OPEN"
    if distance_session_open <= -0.35:
        return "BELOW_SESSION_OPEN"
    return "MID_RANGE"


def _classify_mtf_agreement(*, slope_norm: float, one_minute_slope: float) -> str:
    if slope_norm >= 0.35 and one_minute_slope >= 0.15:
        return "ALIGNED_UP"
    if slope_norm <= -0.35 and one_minute_slope <= -0.15:
        return "ALIGNED_DOWN"
    if slope_norm >= 0.35 and one_minute_slope <= -0.15:
        return "COUNTERTREND_DOWN"
    if slope_norm <= -0.35 and one_minute_slope >= 0.15:
        return "COUNTERTREND_UP"
    return "MIXED"


def _classify_volatility_range_state(expansion_ratio: float) -> str:
    if expansion_ratio <= 0.75:
        return "LOW_VOL_RANGE"
    if expansion_ratio >= 1.6:
        return "HIGH_VOL_RANGE"
    return "NORMAL_VOL_RANGE"


def _classify_regime(*, trend_state: str, momentum_persistence: str) -> str:
    if trend_state in {"UP", "STRONG_UP"} and momentum_persistence == "PERSISTENT_UP":
        return "TREND_UP"
    if trend_state in {"DOWN", "STRONG_DOWN"} and momentum_persistence == "PERSISTENT_DOWN":
        return "TREND_DOWN"
    return "ROTATION"


def _classify_volatility_bucket(expansion_ratio: float) -> str:
    if expansion_ratio <= 0.75:
        return "QUIET"
    if expansion_ratio >= 1.6:
        return "HOT"
    return "NORMAL"


def _classify_direction_bias(*, trend_state: str, mtf_agreement: str) -> str:
    if trend_state in {"UP", "STRONG_UP"} and mtf_agreement == "ALIGNED_UP":
        return "LONG_BIAS"
    if trend_state in {"DOWN", "STRONG_DOWN"} and mtf_agreement == "ALIGNED_DOWN":
        return "SHORT_BIAS"
    return "NEUTRAL"


def _base_session_segment(label: str) -> str:
    if label.startswith("ASIA"):
        return "ASIA"
    if label.startswith("LONDON"):
        return "LONDON"
    if label.startswith("US"):
        return "US"
    return "UNKNOWN"
