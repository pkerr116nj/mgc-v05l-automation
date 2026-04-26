from __future__ import annotations

from mgc_v05l.research.asia_drift.probabilistic_pass6 import (
    _classify_environment,
    _enrich_environment_row,
    _instrument_consistency,
)


def test_enrich_environment_row_collapses_vix_buckets() -> None:
    row = _enrich_environment_row(
        {
            "timing_within_asia": "LATE_ASIA",
            "volatility_regime": "LOW_VOL",
            "vix_level_bucket": "MID",
            "vix_change_bucket": "FLAT",
        }
    )

    assert row["vix_level_simple"] == "NOT_LOW"
    assert row["vix_change_simple"] == "NOT_UP"
    assert row["environment_key"] == "LATE_ASIA|LOW_VOL|NOT_LOW|NOT_UP"


def test_classification_requires_stable_positive_behavior_for_edge_on() -> None:
    development = {"avg_return_60m": 1.0, "win_rate_60m": 0.53, "row_count": 150, "do_not_trust": False}
    holdout = {"avg_return_60m": 2.0, "win_rate_60m": 0.56, "row_count": 140, "do_not_trust": False}
    assert _instrument_consistency(
        {"avg_return_60m": 0.2, "row_count": 80},
        {"avg_return_60m": 0.5, "row_count": 90},
    ) == "BOTH_POSITIVE"
    assert _classify_environment(
        development=development,
        holdout=holdout,
        do_not_trust=False,
        instrument_consistency="BOTH_POSITIVE",
    ) == "EDGE_ON"
    assert _classify_environment(
        development={"avg_return_60m": -0.2, "win_rate_60m": 0.47, "row_count": 140, "do_not_trust": False},
        holdout={"avg_return_60m": -0.5, "win_rate_60m": 0.44, "row_count": 140, "do_not_trust": False},
        do_not_trust=False,
        instrument_consistency="MIXED",
    ) == "EDGE_OFF"
