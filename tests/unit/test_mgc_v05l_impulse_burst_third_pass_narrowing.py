from mgc_v05l.app.mgc_impulse_burst_third_pass_narrowing import (
    NarrowingVariant,
    _decision_bucket,
    _passes_variant,
)


class _Snapshot:
    def __init__(self) -> None:
        self.event = type(
            "E",
            (),
            {
                "impulse": {
                    "largest_bar_share": 0.53,
                    "materially_contributing_bars": 3,
                    "late_extension_share": 0.52,
                    "signal_phase": "ASIA_EARLY",
                }
            },
        )()
        self.prior_10_norm = 1.35
        self.prior_20_norm = 1.02


def test_passes_variant_rejects_spike_and_chase() -> None:
    snapshot = _Snapshot()
    variant = NarrowingVariant(
        variant_name="x",
        description="x",
        largest_bar_share_max=0.50,
        min_material_bars=4,
        chase_prior_10_trigger=1.32,
        chase_prior_20_trigger=1.00,
        chase_late_extension_trigger=0.50,
    )
    assert not _passes_variant(snapshot, variant)


def test_passes_variant_respects_session_exclusion() -> None:
    snapshot = _Snapshot()
    variant = NarrowingVariant(
        variant_name="x",
        description="x",
        excluded_phases=("ASIA_EARLY", "ASIA_LATE"),
    )
    assert not _passes_variant(snapshot, variant)


def test_decision_bucket_marks_cleaner_when_quality_holds() -> None:
    bucket = _decision_bucket(
        variant=NarrowingVariant(variant_name="x", description="x"),
        metrics={
            "trades": 180,
            "profit_factor": 1.25,
            "average_loser": 30.0,
            "top_3_contribution": 140.0,
            "large_winner_count": 22,
        },
        composition={},
        control_metrics={
            "trades": 255,
            "profit_factor": 1.19,
            "average_loser": 38.0,
            "top_3_contribution": 156.0,
            "large_winner_count": 27,
        },
    )
    assert bucket == "CLEANER_AND_STILL_REAL"
