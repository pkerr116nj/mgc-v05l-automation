"""Pattern Engine v1 primitive vocabulary."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .dataset import PatternEngineContext

SLOPE_POSITIVE = Decimal("0.20")
SLOPE_NEGATIVE = Decimal("-0.20")
CURVATURE_POSITIVE = Decimal("0.15")
CURVATURE_NEGATIVE = Decimal("-0.15")
EXPANDED_RATIO = Decimal("1.25")
COMPRESSED_RATIO = Decimal("0.85")
HIGH_VOLUME_RATIO = Decimal("1.20")
LOW_VOLUME_RATIO = Decimal("0.80")
NEAR_EXTREMA_ATR = Decimal("0.50")


@dataclass(frozen=True)
class PatternPrimitivePoint:
    timestamp: str
    slope_state: str
    curvature_state: str
    expansion_state: str
    range_context: str
    pullback_state: str
    breakout_state: str
    failure_state: str
    ema_location_state: str
    extrema_distance_state: str
    volume_context: str
    close_strength_state: str


def build_pattern_primitive_points(contexts: list[PatternEngineContext]) -> list[PatternPrimitivePoint]:
    primitives: list[PatternPrimitivePoint] = []
    for index, current in enumerate(contexts):
        previous = contexts[index - 1] if index > 0 else None
        prior_two = contexts[index - 2] if index > 1 else None
        primitives.append(
            PatternPrimitivePoint(
                timestamp=current.timestamp.isoformat(),
                slope_state=_slope_state(current.normalized_slope),
                curvature_state=_curvature_state(current.normalized_curvature),
                expansion_state=_expansion_state(current.range_expansion_ratio),
                range_context=_expansion_state(current.range_expansion_ratio),
                pullback_state=_pullback_state(current, previous),
                breakout_state=_breakout_state(current, previous),
                failure_state=_failure_state(current, previous, prior_two),
                ema_location_state=_ema_location_state(current),
                extrema_distance_state=_extrema_distance_state(current),
                volume_context=_volume_context(current.vol_ratio),
                close_strength_state=_close_strength_state(current.close_location),
            )
        )
    return primitives


def _slope_state(value: Decimal) -> str:
    if value > SLOPE_POSITIVE:
        return "SLOPE_POS"
    if value < SLOPE_NEGATIVE:
        return "SLOPE_NEG"
    return "SLOPE_FLAT"


def _curvature_state(value: Decimal) -> str:
    if value > CURVATURE_POSITIVE:
        return "CURVATURE_POS"
    if value < CURVATURE_NEGATIVE:
        return "CURVATURE_NEG"
    return "CURVATURE_FLAT"


def _expansion_state(value: Decimal) -> str:
    if value >= EXPANDED_RATIO:
        return "EXPANDED"
    if value <= COMPRESSED_RATIO:
        return "COMPRESSED"
    return "NORMAL"


def _pullback_state(current: PatternEngineContext, previous: PatternEngineContext | None) -> str:
    if previous is None:
        return "NONE"
    if current.close < previous.close:
        return "ONE_BAR_PULLBACK"
    if current.close > previous.close:
        return "ONE_BAR_REBOUND"
    return "NONE"


def _breakout_state(current: PatternEngineContext, previous: PatternEngineContext | None) -> str:
    if previous is None:
        return "NONE"
    if current.high > previous.high and current.close >= previous.close:
        return "BREAK_ABOVE_PRIOR_HIGH"
    if current.low < previous.low and current.close <= previous.close:
        return "BREAK_BELOW_PRIOR_LOW"
    return "NONE"


def _failure_state(
    current: PatternEngineContext,
    previous: PatternEngineContext | None,
    prior_two: PatternEngineContext | None,
) -> str:
    if previous is None or prior_two is None:
        return "NONE"
    if previous.high > prior_two.high and current.close < previous.high and current.close < previous.close:
        return "FAILED_UP_BREAK"
    if previous.low < prior_two.low and current.close > previous.low and current.close > previous.close:
        return "FAILED_DOWN_BREAK"
    return "NONE"


def _ema_location_state(current: PatternEngineContext) -> str:
    fast = current.turn_ema_fast
    slow = current.turn_ema_slow
    if fast is None or slow is None:
        return "UNKNOWN"
    if current.close <= fast and current.close <= slow and fast < slow:
        return "BELOW_BOTH_FAST_LT_SLOW"
    if current.close >= fast and current.close >= slow and fast > slow:
        return "ABOVE_BOTH_FAST_GT_SLOW"
    if fast < slow and current.close > fast and current.close <= slow:
        return "REBOUND_BELOW_SLOW"
    if fast > slow and current.close < fast and current.close >= slow:
        return "REBOUND_ABOVE_SLOW"
    return "MIXED"


def _extrema_distance_state(current: PatternEngineContext) -> str:
    if current.distance_from_low_10_atr is not None and current.distance_from_low_10_atr <= NEAR_EXTREMA_ATR:
        return "NEAR_RECENT_LOW"
    if current.distance_from_high_10_atr is not None and current.distance_from_high_10_atr <= NEAR_EXTREMA_ATR:
        return "NEAR_RECENT_HIGH"
    return "MID_RANGE"


def _volume_context(value: Decimal) -> str:
    if value >= HIGH_VOLUME_RATIO:
        return "HIGH_VOLUME"
    if value <= LOW_VOLUME_RATIO:
        return "LOW_VOLUME"
    return "NORMAL_VOLUME"


def _close_strength_state(value: Decimal) -> str:
    if value >= Decimal("0.70"):
        return "STRONG_CLOSE"
    if value <= Decimal("0.30"):
        return "WEAK_CLOSE"
    return "NEUTRAL_CLOSE"
