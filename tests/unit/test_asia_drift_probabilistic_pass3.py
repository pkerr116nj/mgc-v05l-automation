from __future__ import annotations

from mgc_v05l.research.asia_drift.probabilistic_pass3 import _metric_summary, _with_stack_flags


def test_with_stack_flags_marks_primary_stack_only() -> None:
    rows = [
        {
            "agreement_60m": "AGREE",
            "cross_asset_confirmation": True,
            "compression_bucket": "NOT_COMPRESSED",
            "timing_within_asia": "EARLY_ASIA",
        },
        {
            "agreement_60m": "NOT_AGREE",
            "cross_asset_confirmation": True,
            "compression_bucket": "NOT_COMPRESSED",
            "timing_within_asia": "EARLY_ASIA",
        },
    ]

    flagged = _with_stack_flags(rows)

    assert flagged[0]["primary_stack_only"] is True
    assert flagged[1]["primary_stack_only"] is False


def test_metric_summary_adds_tail_and_trust_fields() -> None:
    bucket = [
        {
            "continuation_60m": True,
            "forward_return_15m": 1.0,
            "forward_return_60m": 2.0,
            "session_end_return": 3.0,
            "mfe_60m_points": 4.0,
            "mae_60m_points": 1.0,
        },
        {
            "continuation_60m": False,
            "forward_return_15m": -1.0,
            "forward_return_60m": -2.0,
            "session_end_return": -3.0,
            "mfe_60m_points": 5.0,
            "mae_60m_points": 2.0,
        },
    ]

    summary = _metric_summary(bucket)

    assert summary["row_count"] == 2
    assert summary["continuation_probability_60m"] == 0.5
    assert "forward_return_60m_p10" in summary
    assert "mae_p90" in summary
    assert summary["do_not_trust"] is True
