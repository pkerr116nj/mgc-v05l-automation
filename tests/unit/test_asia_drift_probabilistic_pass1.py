from __future__ import annotations

from datetime import datetime

from mgc_v05l.research.asia_drift.probabilistic_pass1 import _build_cross_asset_confirmation_map, _label_outcome_row


def test_cross_asset_confirmation_requires_different_root_groups() -> None:
    decision_ts = datetime.fromisoformat("2026-03-03T00:30:00+00:00")
    confirmation = _build_cross_asset_confirmation_map(
        [
            {
                "candidate_id": "GC|a",
                "decision_ts": decision_ts,
                "direction": "LONG",
                "root_group": "GOLD",
                "symbol": "GC",
            },
            {
                "candidate_id": "MGC|b",
                "decision_ts": decision_ts,
                "direction": "LONG",
                "root_group": "GOLD",
                "symbol": "MGC",
            },
            {
                "candidate_id": "ES|c",
                "decision_ts": decision_ts,
                "direction": "LONG",
                "root_group": "SPX",
                "symbol": "ES",
            },
        ]
    )

    assert confirmation["GC|a"]["cross_asset_confirmation"] is True
    assert confirmation["GC|a"]["cross_asset_confirming_root_count"] == 1
    assert confirmation["MGC|b"]["cross_asset_confirmation"] is True
    assert confirmation["MGC|b"]["cross_asset_confirming_root_count"] == 1
    assert confirmation["ES|c"]["cross_asset_confirmation"] is True
    assert confirmation["ES|c"]["cross_asset_confirming_root_count"] == 1


def test_label_outcome_row_computes_directional_forward_returns_and_excursions() -> None:
    decision_ts = datetime.fromisoformat("2026-03-03T00:00:00+00:00")
    candidate = {
        "candidate_id": "GC|2026-03-03T00:00:00+00:00|LONG",
        "instrument": "GC",
        "sample_split": "development",
        "direction": "LONG",
        "decision_ts": decision_ts,
        "decision_close": 100.0,
        "session_end_ts": datetime.fromisoformat("2026-03-03T01:10:00+00:00"),
        "cross_asset_confirmation": True,
        "cross_asset_confirming_root_count": 2,
        "daily_regime_bucket": "UP_EXPANDED",
        "higher_timeframe_agreement_state": "FULL",
        "compression_state": "NORMAL",
        "disorder_state": "ORDERLY",
    }
    raw_series = {
        "timestamps": [
            datetime.fromisoformat("2026-03-03T00:05:00+00:00"),
            datetime.fromisoformat("2026-03-03T00:10:00+00:00"),
            datetime.fromisoformat("2026-03-03T00:15:00+00:00"),
            datetime.fromisoformat("2026-03-03T00:30:00+00:00"),
            datetime.fromisoformat("2026-03-03T01:00:00+00:00"),
        ],
        "highs": [100.4, 100.8, 101.3, 101.8, 102.0],
        "lows": [99.7, 100.1, 100.5, 100.9, 101.4],
        "closes": [100.2, 100.6, 101.0, 101.6, 101.9],
    }

    row = _label_outcome_row(candidate=candidate, raw_series=raw_series)

    assert row["forward_return_15m"] == 1.0
    assert row["forward_return_60m"] == 1.9
    assert row["session_end_return"] == 1.9
    assert row["continuation_60m"] is True
    assert row["mfe_60m_points"] == 2.0
    assert row["mae_60m_points"] == 0.3
    assert row["resolution_proxy"] == "ADVERSE_FIRST"
    assert row["time_to_resolution_proxy_minutes"] == 5
