from __future__ import annotations

from mgc_v05l.research.asia_drift.probabilistic_pass2 import (
    _agreement_bucket,
    _compression_bucket,
    _disorder_bucket,
    _instrument_cluster,
    _merge_rows,
    _timing_bucket,
)


def test_slice_bucket_helpers_classify_expected_states() -> None:
    assert _instrument_cluster("GC") == "METALS"
    assert _instrument_cluster("NQ") == "INDEX"
    assert _agreement_bucket("UP", "LONG") == "AGREE"
    assert _agreement_bucket("FLAT", "LONG") == "NOT_AGREE"
    assert _agreement_bucket("DOWN", "SHORT") == "AGREE"
    assert _compression_bucket("COMPRESSED_DRIFT_EXPANSION") == "COMPRESSED"
    assert _compression_bucket("NORMAL") == "NOT_COMPRESSED"
    assert _disorder_bucket("ORDERLY") == "STABLE"
    assert _disorder_bucket("POST_SPIKE") == "POST_SPIKE_DISORDER"
    assert _disorder_bucket("CHOPPY") == "OTHER_DISORDER"
    assert _timing_bucket("ASIA_DRIFT_BUILD") == "EARLY_ASIA"
    assert _timing_bucket("ASIA_DRIFT_MATURE") == "LATE_ASIA"
    assert _timing_bucket("ASIA_DRIFT_PRE_HANDOFF") == "PRE_LONDON"


def test_merge_rows_adds_slice_dimensions() -> None:
    candidate_rows = [
        {
            "candidate_id": "GC|1|LONG",
            "instrument": "GC",
            "direction": "LONG",
            "direction_60m_state": "UP",
            "direction_240m_state": "FLAT",
            "compression_state": "COMPRESSED",
            "disorder_state": "POST_SPIKE",
            "subphase": "ASIA_DRIFT_PRE_HANDOFF",
        }
    ]
    outcome_rows = [
        {
            "candidate_id": "GC|1|LONG",
            "sample_split": "holdout",
            "continuation_60m": True,
            "forward_return_15m": 1.0,
            "forward_return_60m": 2.0,
            "session_end_return": 3.0,
            "mfe_60m_points": 4.0,
            "mae_60m_points": 1.5,
            "daily_regime_bucket": "UP_NORMAL",
            "cross_asset_confirmation": True,
        }
    ]

    merged = _merge_rows(candidate_rows, outcome_rows)

    assert len(merged) == 1
    row = merged[0]
    assert row["instrument_cluster"] == "METALS"
    assert row["agreement_60m"] == "AGREE"
    assert row["agreement_240m"] == "NOT_AGREE"
    assert row["compression_bucket"] == "COMPRESSED"
    assert row["disorder_bucket"] == "POST_SPIKE_DISORDER"
    assert row["timing_within_asia"] == "PRE_LONDON"
