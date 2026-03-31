from mgc_v05l.app.mgc_impulse_spike_confirmation_pass import (
    ConfirmationVariant,
    _decision_bucket,
    _passes_confirmation_variant,
)
from mgc_v05l.app.mgc_impulse_spike_subtypes import SpikeFeatureRow


def _row(**overrides):
    base = dict(
        pnl=10.0,
        subtype="GOOD_IGNITION_SPIKE",
        time_of_day_bucket="US_LATE",
        prior_10_bar_net_move_normalized=1.0,
        prior_20_bar_net_move_normalized=0.8,
        pre_burst_range_compression_or_expansion=0.8,
        local_micro_range_breakout_flag=1.0,
        largest_bar_concentration_metric=0.33,
        materially_contributing_bar_count=4.0,
        contributing_bar_breadth_metric=0.5,
        same_direction_share=0.75,
        body_dominance=0.9,
        path_efficiency=0.85,
        normalized_move=1.5,
        acceleration_ratio=1.2,
        late_extension_share=0.4,
        body_to_range_quality=0.55,
        wickiness_metric=0.45,
        first_1_bar_continuation_amount=20.0,
        first_2_bars_continuation_amount=60.0,
        first_1_bar_retrace=10.0,
        first_2_bars_max_retrace=20.0,
        new_extension_within_2_bars=1.0,
        confirmation_bar_count_first_3=2.0,
    )
    base.update(overrides)
    return SpikeFeatureRow(**base)


def test_passes_confirmation_variant_requires_new_extension_and_confirmation() -> None:
    row = _row(new_extension_within_2_bars=0.0, confirmation_bar_count_first_3=1.0)
    variant = ConfirmationVariant(
        variant_name="x",
        description="x",
        require_new_extension=True,
        min_confirmation_bar_count=2,
    )
    assert not _passes_confirmation_variant(row, variant)


def test_passes_confirmation_variant_rejects_bad_retrace_quality() -> None:
    row = _row(first_2_bars_continuation_amount=30.0, first_2_bars_max_retrace=50.0)
    variant = ConfirmationVariant(
        variant_name="x",
        description="x",
        max_first_2_bar_retrace=45.0,
        require_continuation_over_retrace=True,
    )
    assert not _passes_confirmation_variant(row, variant)


def test_decision_bucket_marks_cleaner_when_good_kept_and_bad_removed() -> None:
    bucket = _decision_bucket(
        variant=ConfirmationVariant(variant_name="x", description="x"),
        metrics={"trades": 130, "profit_factor": 1.18},
        control_count=171,
        good_preserved=0.75,
        bad_removed=0.56,
    )
    assert bucket == "CLEANER_AND_STILL_REAL"
