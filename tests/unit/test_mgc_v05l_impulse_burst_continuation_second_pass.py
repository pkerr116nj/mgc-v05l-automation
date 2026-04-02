from mgc_v05l.app.mgc_impulse_burst_continuation_second_pass import (
    _decision_bucket_second_pass,
    _fast_rejection_practical,
)


def test_fast_rejection_practical_requires_multiple_improvements() -> None:
    assert _fast_rejection_practical(
        overlay={
            "realized_pnl": 120.0,
            "median_trade": -1.0,
            "profit_factor": 1.25,
            "max_drawdown": 80.0,
        },
        baseline_metrics={
            "realized_pnl": 100.0,
            "median_trade": -3.0,
            "profit_factor": 1.05,
            "max_drawdown": 120.0,
        },
    )


def test_decision_bucket_second_pass_marks_too_noisy_when_pf_sub_one() -> None:
    bucket = _decision_bucket_second_pass(
        raw_events=100,
        post_filter_events=60,
        trade_count=60,
        metrics={
            "realized_pnl": -50.0,
            "profit_factor": 0.92,
            "median_trade": -4.0,
            "false_start_rate": 0.4,
            "top_3_trade_contribution": 120.0,
        },
    )
    assert bucket == "TOO_NOISY"


def test_decision_bucket_second_pass_marks_promising_when_cleaner() -> None:
    bucket = _decision_bucket_second_pass(
        raw_events=80,
        post_filter_events=40,
        trade_count=40,
        metrics={
            "realized_pnl": 220.0,
            "profit_factor": 1.3,
            "median_trade": -2.0,
            "false_start_rate": 0.2,
            "top_3_trade_contribution": 140.0,
        },
    )
    assert bucket == "PROMISING_NEW_FAMILY"
