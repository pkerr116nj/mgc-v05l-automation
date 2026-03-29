from datetime import datetime

from mgc_v05l.app.mgc_impulse_quant_rescue_pass import (
    QuantFeatureRow,
    RescueOverlay,
    _decision_bucket,
    _passes_overlay,
    _separator_row,
)


def _row(**overrides):
    base = dict(
        event=object(),
        subclass_bucket="SPIKE_DOMINATED_OTHER",
        diagnostic_subtype="GOOD_IGNITION_SPIKE",
        prior_20_norm=0.8,
        compression_ratio=0.9,
        micro_breakout=True,
        largest_bar_share=0.3,
        materially_contributing_bars=4.0,
        contributing_breadth=0.5,
        body_dominance=0.93,
        path_efficiency=0.88,
        normalized_move=1.5,
        acceleration_ratio=1.6,
        late_extension_share=0.45,
        body_to_range_quality=0.56,
        wickiness_metric=0.44,
        breadth_concentration_score=0.35,
        force_exhaustion_score=0.72,
        volatility_normalized_shape_score=0.31,
    )
    base.update(overrides)
    return QuantFeatureRow(**base)


def test_separator_row_reports_expected_direction() -> None:
    good = [_row(largest_bar_share=0.28), _row(largest_bar_share=0.30)]
    bad = [_row(diagnostic_subtype="BAD_SPIKE_TRAP", largest_bar_share=0.40), _row(diagnostic_subtype="BAD_SPIKE_TRAP", largest_bar_share=0.42)]
    row = _separator_row(feature_name="largest_bar_share", good=good, bad=bad)
    assert row["preferred_direction"] == "LOWER_IS_BETTER"
    assert row["separation_score"] > 0


def test_overlay_applies_only_to_spike_bucket() -> None:
    overlay = RescueOverlay(
        variant_name="combo",
        description="combo",
        min_breadth_concentration_score=0.34,
        max_force_exhaustion_score=0.8,
    )
    assert _passes_overlay(_row(subclass_bucket="FRESH_LAUNCH_FROM_COMPRESSION"), overlay) is True
    assert _passes_overlay(_row(), overlay) is True
    assert _passes_overlay(_row(breadth_concentration_score=0.2), overlay) is False


def test_decision_bucket_marks_material_rescue_when_quality_is_restored() -> None:
    bucket = _decision_bucket(
        metrics={
            "profit_factor": 2.4,
            "median_trade": 7.0,
            "top_3_contribution": 58.0,
            "survives_without_top_3": True,
            "realized_pnl": 1900.0,
        },
        raw_control_metrics={"realized_pnl": 1113.0},
        benchmark_metrics={"realized_pnl": 4667.0},
    )
    assert bucket == "QUANT_OVERLAY_MATERIALLY_RESCUES_FAMILY"
