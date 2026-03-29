from mgc_v05l.app.mgc_impulse_confirmation_validation import _validation_bucket


def test_validation_bucket_marks_validated_candidate_when_clean_and_stable() -> None:
    bucket = _validation_bucket(
        metrics={
            "profit_factor": 3.0,
            "median_trade": 10.0,
            "top_3_contribution": 40.0,
            "survives_without_top_3": True,
        },
        subtype={
            "percent_GOOD_IGNITION_SPIKE_preserved": 0.75,
            "percent_BAD_SPIKE_TRAP_removed": 0.95,
        },
        trade_count=80,
        control_trade_count=100,
    )
    assert bucket == "VALIDATED_MGC_PAPER_CANDIDATE"


def test_validation_bucket_marks_promising_when_good_but_not_full_bar() -> None:
    bucket = _validation_bucket(
        metrics={
            "profit_factor": 2.0,
            "median_trade": 8.0,
            "survives_without_top_3": True,
        },
        subtype={
            "percent_GOOD_IGNITION_SPIKE_preserved": 0.6,
            "percent_BAD_SPIKE_TRAP_removed": 0.8,
        },
        trade_count=50,
        control_trade_count=100,
    )
    assert bucket == "PROMISING_BUT_ONE_MORE_MGC_PASS_NEEDED"
