"""Tests for the experimental causal momentum-shape features."""

from decimal import Decimal

from mgc_v05l.research.causal_momentum import compute_causal_momentum_features


def test_causal_momentum_features_do_not_change_when_future_data_is_appended() -> None:
    base_prices = [Decimal("1"), Decimal("2"), Decimal("3"), Decimal("2")]
    base_scale = [Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1")]
    base_features = compute_causal_momentum_features(
        prices=base_prices,
        volatility_scale=base_scale,
        smoothing_length=2,
        normalization_floor=Decimal("0.01"),
    )

    extended_prices = [*base_prices, Decimal("50"), Decimal("10")]
    extended_scale = [*base_scale, Decimal("1"), Decimal("1")]
    extended_features = compute_causal_momentum_features(
        prices=extended_prices,
        volatility_scale=extended_scale,
        smoothing_length=2,
        normalization_floor=Decimal("0.01"),
    )

    assert extended_features[: len(base_features)] == base_features


def test_derivative_feature_flags_behave_on_simple_synthetic_sequences() -> None:
    prices = [Decimal("1"), Decimal("3"), Decimal("4"), Decimal("4.5"), Decimal("3.5"), Decimal("3.0")]
    atr_scale = [Decimal("2")] * len(prices)
    features = compute_causal_momentum_features(
        prices=prices,
        volatility_scale=atr_scale,
        smoothing_length=1,
        normalization_floor=Decimal("0.01"),
    )

    assert features[1].momentum_turning_positive is True
    assert features[2].momentum_compressing_up is True
    assert features[3].momentum_compressing_up is True
    assert features[4].momentum_turning_negative is True
    assert features[5].momentum_compressing_down is True


def test_normalized_slope_and_curvature_use_supplied_volatility_scale() -> None:
    features = compute_causal_momentum_features(
        prices=[Decimal("10"), Decimal("12"), Decimal("13")],
        volatility_scale=[Decimal("2"), Decimal("2"), Decimal("2")],
        smoothing_length=1,
        normalization_floor=Decimal("0.01"),
    )

    assert features[1].normalized_slope == Decimal("1")
    assert features[1].normalized_curvature == Decimal("1")
    assert features[2].normalized_slope == Decimal("0.5")
