"""Experimental causal momentum-shape features.

This module is intentionally isolated from the production v0.5l signal path.
All calculations are trailing-only and use no future bars.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Sequence


@dataclass(frozen=True)
class CausalMomentumFeature:
    index: int
    raw_price: Decimal
    smoothed_price: Decimal
    first_derivative: Decimal
    second_derivative: Decimal
    normalized_slope: Decimal
    normalized_curvature: Decimal
    momentum_compressing_up: bool
    momentum_compressing_down: bool
    momentum_turning_positive: bool
    momentum_turning_negative: bool


def compute_causal_momentum_features(
    prices: Sequence[Decimal],
    volatility_scale: Sequence[Decimal],
    smoothing_length: int,
    normalization_floor: Decimal,
) -> list[CausalMomentumFeature]:
    """Compute trailing-only smoothed-price derivatives and simple shape flags."""
    if len(prices) != len(volatility_scale):
        raise ValueError("prices and volatility_scale must have the same length.")
    if smoothing_length <= 0:
        raise ValueError("smoothing_length must be > 0.")
    if normalization_floor <= 0:
        raise ValueError("normalization_floor must be > 0.")
    if not prices:
        return []

    smoothed_prices = _causal_exponential_smoother(prices, smoothing_length)
    first_derivatives: list[Decimal] = []
    second_derivatives: list[Decimal] = []
    features: list[CausalMomentumFeature] = []

    for index, smoothed_price in enumerate(smoothed_prices):
        previous_smoothed = smoothed_prices[index - 1] if index > 0 else smoothed_price
        first_derivative = smoothed_price - previous_smoothed if index > 0 else Decimal("0")
        first_derivatives.append(first_derivative)

        previous_first_derivative = first_derivatives[index - 1] if index > 0 else Decimal("0")
        second_derivative = first_derivative - previous_first_derivative if index > 0 else Decimal("0")
        second_derivatives.append(second_derivative)

        normalizer = max(volatility_scale[index], normalization_floor)
        normalized_slope = first_derivative / normalizer
        normalized_curvature = second_derivative / normalizer
        previous_slope = first_derivatives[index - 1] if index > 0 else Decimal("0")

        features.append(
            CausalMomentumFeature(
                index=index,
                raw_price=prices[index],
                smoothed_price=smoothed_price,
                first_derivative=first_derivative,
                second_derivative=second_derivative,
                normalized_slope=normalized_slope,
                normalized_curvature=normalized_curvature,
                momentum_compressing_up=normalized_slope > 0 and normalized_curvature < 0,
                momentum_compressing_down=normalized_slope < 0 and normalized_curvature > 0,
                momentum_turning_positive=first_derivative > 0 and previous_slope <= 0,
                momentum_turning_negative=first_derivative < 0 and previous_slope >= 0,
            )
        )

    return features


def _causal_exponential_smoother(prices: Sequence[Decimal], smoothing_length: int) -> list[Decimal]:
    alpha = Decimal("2") / Decimal(smoothing_length + 1)
    smoothed: list[Decimal] = []
    for index, price in enumerate(prices):
        if index == 0:
            smoothed.append(price)
        else:
            smoothed.append(alpha * price + (Decimal("1") - alpha) * smoothed[index - 1])
    return smoothed
