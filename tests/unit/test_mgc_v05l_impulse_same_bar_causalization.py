from mgc_v05l.app.mgc_impulse_same_bar_causalization import (
    CausalProxyVariant,
    _causal_quality_score,
    _decision_bucket,
)


def test_causal_quality_score_rewards_clean_broad_impulse_shape() -> None:
    clean = _causal_quality_score(
        same_direction_share=0.875,
        body_dominance=0.82,
        path_efficiency=0.66,
        contributing_breadth=0.625,
        normalized_move=1.7,
        acceleration_ratio=1.12,
        largest_bar_share=0.31,
        body_to_range_quality=0.64,
        wickiness_metric=0.36,
        prior_20_norm=0.55,
        late_extension_share=0.28,
        materially_contributing_bars=4.0,
    )
    noisy = _causal_quality_score(
        same_direction_share=0.75,
        body_dominance=0.70,
        path_efficiency=0.50,
        contributing_breadth=0.375,
        normalized_move=1.4,
        acceleration_ratio=0.95,
        largest_bar_share=0.48,
        body_to_range_quality=0.49,
        wickiness_metric=0.51,
        prior_20_norm=1.35,
        late_extension_share=0.52,
        materially_contributing_bars=3.0,
    )
    assert clean > noisy


def test_decision_bucket_marks_recovered_variant_when_quality_is_kept() -> None:
    bucket = _decision_bucket(
        metrics={
            "trades": 70,
            "profit_factor": 2.3,
            "median_trade": 6.0,
            "top_3_contribution": 42.0,
            "survives_without_top_3": True,
            "realized_pnl": 2200.0,
        },
        raw_control_metrics={"realized_pnl": 1113.0, "top_3_contribution": 156.33},
        benchmark_metrics={"trades": 115, "realized_pnl": 4667.0},
    )
    assert bucket == "CAUSAL_PROXY_RECOVERS_ENOUGH"


def test_rule_summary_dataclass_is_constructible() -> None:
    variant = CausalProxyVariant(
        variant_name="example",
        description="example",
        min_acceleration_ratio=1.0,
        require_micro_breakout=True,
    )
    assert variant.require_micro_breakout is True
