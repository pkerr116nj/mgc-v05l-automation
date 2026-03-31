from mgc_v05l.app.mgc_impulse_burst_subclass_diagnostics import (
    _build_diagnosis,
    _classify_subclass,
)


def test_classify_subclass_marks_reversal_burst_on_negative_prior_run() -> None:
    assert (
        _classify_subclass(
            prior_run_10_norm=-0.7,
            prior_run_20_norm=-0.2,
            compression_ratio=0.5,
            micro_breakout=False,
            largest_bar_share=0.2,
        )
        == "REVERSAL_BURST"
    )


def test_classify_subclass_marks_late_extension_on_large_prior_run() -> None:
    assert (
        _classify_subclass(
            prior_run_10_norm=0.9,
            prior_run_20_norm=1.5,
            compression_ratio=0.9,
            micro_breakout=True,
            largest_bar_share=0.25,
        )
        == "LATE_EXTENSION_CHASE"
    )


def test_classify_subclass_marks_fresh_launch_from_compression() -> None:
    assert (
        _classify_subclass(
            prior_run_10_norm=0.3,
            prior_run_20_norm=0.5,
            compression_ratio=0.6,
            micro_breakout=True,
            largest_bar_share=0.3,
        )
        == "FRESH_LAUNCH_FROM_COMPRESSION"
    )


def test_build_diagnosis_prefers_clean_subclass_candidate() -> None:
    diagnosis = _build_diagnosis(
        [
            {
                "subclass_bucket": "FRESH_LAUNCH_FROM_COMPRESSION",
                "trades": 50,
                "realized_pnl": 500.0,
                "median_trade": 3.0,
                "profit_factor": 1.22,
                "top_3_contribution": 140.0,
                "survives_without_top_1": True,
            },
            {
                "subclass_bucket": "LATE_EXTENSION_CHASE",
                "trades": 60,
                "realized_pnl": -120.0,
                "median_trade": -5.0,
                "profit_factor": 0.85,
                "top_3_contribution": 260.0,
                "survives_without_top_1": False,
            },
        ]
    )
    assert diagnosis["carrier_subclass"] == "FRESH_LAUNCH_FROM_COMPRESSION"
    assert diagnosis["poison_subclass"] == "LATE_EXTENSION_CHASE"
    assert diagnosis["clean_subclass_candidate"] == "FRESH_LAUNCH_FROM_COMPRESSION"
    assert diagnosis["worth_narrowing_to_one_subclass"] is True
    assert diagnosis["family_verdict"] == "NARROW_TO_CLEAN_SUBCLASS"
