from __future__ import annotations

from datetime import datetime
from pathlib import Path

from mgc_v05l.research.asia_drift.probabilistic_pass5 import (
    _build_edge_classification_rows,
    _attach_vix_regimes,
    _build_index_alignment_lookup,
    _prior_session_behavior_bucket,
    _prior_session_range_bucket,
    _regime_dimensions,
    _trend_regime,
    _volatility_regime,
)
from mgc_v05l.research.trend_participation.storage import materialize_parquet_dataset
from mgc_v05l.research.warehouse_historical_evaluator.layout import build_layout


def test_volatility_and_trend_regimes_are_bucketed_consistently() -> None:
    assert _volatility_regime("UP_EXPANDED") == "HIGH_VOL"
    assert _volatility_regime("DOWN_NORMAL") == "LOW_VOL"
    assert _trend_regime("UP_EXPANDED", "UP", "AGREE") == "TREND_SUPPORTIVE"
    assert _trend_regime("FLAT_NORMAL", "FLAT", "NOT_AGREE") == "RANGE_BOUND_OR_UNCLEAR"
    assert _trend_regime("DOWN_NORMAL", "UP", "NOT_AGREE") == "COUNTERTREND_OR_MIXED"


def test_prior_session_buckets_use_descriptive_cutoffs() -> None:
    assert _prior_session_behavior_bucket(0.7) == "STRONG_PRIOR_TREND"
    assert _prior_session_behavior_bucket(0.2) == "PRIOR_CHOP"
    assert _prior_session_behavior_bucket(0.45) == "PRIOR_BALANCED"
    assert _prior_session_range_bucket(1.3) == "PRIOR_LARGE_RANGE"
    assert _prior_session_range_bucket(0.7) == "PRIOR_SMALL_RANGE"
    assert _prior_session_range_bucket(1.0) == "PRIOR_NORMAL_RANGE"


def test_index_alignment_requires_both_spx_and_ndx_roots() -> None:
    decision_ts = datetime.fromisoformat("2026-04-01T20:00:00-04:00")
    rows = [
        {"decision_ts": decision_ts, "direction": "LONG", "instrument": "ES"},
        {"decision_ts": decision_ts, "direction": "LONG", "instrument": "NQ"},
        {"decision_ts": decision_ts, "direction": "SHORT", "instrument": "MES"},
    ]

    lookup = _build_index_alignment_lookup(rows)

    assert lookup[(decision_ts, "LONG")] == "INDEX_ALIGNED"
    assert lookup[(decision_ts, "SHORT")] == "INDEX_DIVERGENT"


def test_edge_classification_marks_edge_on_only_when_both_splits_support_it() -> None:
    regime_rows = [
        {
            "sample_split": "development",
            "timing_within_asia": "LATE_ASIA",
            "regime_dimension": "volatility_regime",
            "regime_value": "HIGH_VOL",
            "row_count": 200,
            "avg_return_60m": 1.0,
            "win_rate_60m": 0.53,
            "do_not_trust": False,
        },
        {
            "sample_split": "holdout",
            "timing_within_asia": "LATE_ASIA",
            "regime_dimension": "volatility_regime",
            "regime_value": "HIGH_VOL",
            "row_count": 180,
            "avg_return_60m": 2.0,
            "win_rate_60m": 0.55,
            "do_not_trust": False,
        },
        {
            "sample_split": "development",
            "timing_within_asia": "EARLY_ASIA",
            "regime_dimension": "volatility_regime",
            "regime_value": "LOW_VOL",
            "row_count": 200,
            "avg_return_60m": 0.3,
            "win_rate_60m": 0.51,
            "do_not_trust": False,
        },
        {
            "sample_split": "holdout",
            "timing_within_asia": "EARLY_ASIA",
            "regime_dimension": "volatility_regime",
            "regime_value": "LOW_VOL",
            "row_count": 180,
            "avg_return_60m": -0.1,
            "win_rate_60m": 0.49,
            "do_not_trust": False,
        },
    ]

    rows = _build_edge_classification_rows(regime_rows)

    late = next(row for row in rows if row["timing_within_asia"] == "LATE_ASIA")
    early = next(row for row in rows if row["timing_within_asia"] == "EARLY_ASIA")
    assert late["edge_classification_candidate"] == "EDGE_ON_CANDIDATE"
    assert early["edge_classification_candidate"] == "EDGE_OFF_CANDIDATE"


def test_regime_dimensions_expand_by_mode() -> None:
    assert "volatility_regime" in _regime_dimensions("realized_only")
    assert "vix_level_bucket" in _regime_dimensions("vix_only")
    assert "realized_plus_vix_bucket" in _regime_dimensions("realized_plus_vix")


def test_attach_vix_regimes_uses_asof_join_and_combined_bucket(tmp_path: Path) -> None:
    warehouse_root = tmp_path / "warehouse"
    layout = build_layout(warehouse_root)
    materialize_parquet_dataset(
        layout["vol_regime_daily"] / "vol_regime_daily.parquet",
        [
            {
                "vix_trade_date": "2026-03-13",
                "vix_asof_ts": "2026-03-13T20:15:00+00:00",
                "vix_close": 21.0,
                "vix_change_abs": 1.0,
                "vix_change_pct": 0.05,
                "vix_level_bucket": "MID",
                "vix_change_bucket": "UP",
                "vix_combined_bucket": "MID_UP",
            },
            {
                "vix_trade_date": "2026-03-16",
                "vix_asof_ts": "2026-03-16T20:15:00+00:00",
                "vix_close": 18.0,
                "vix_change_abs": -3.0,
                "vix_change_pct": -0.142857,
                "vix_level_bucket": "MID",
                "vix_change_bucket": "DOWN",
                "vix_combined_bucket": "MID_DOWN",
            },
        ],
    )

    rows = _attach_vix_regimes(
        [
            {
                "decision_ts": "2026-03-16T15:00:00-04:00",
                "volatility_regime": "LOW_VOL",
            },
            {
                "decision_ts": "2026-03-16T18:00:00-04:00",
                "volatility_regime": "HIGH_VOL",
            },
        ],
        warehouse_root=warehouse_root,
        regime_mode="vix_only",
    )

    assert rows[0]["vix_trade_date"] == "2026-03-13"
    assert rows[1]["vix_trade_date"] == "2026-03-16"
    assert rows[1]["realized_plus_vix_bucket"] == "HIGH_VOL|MID_DOWN"
