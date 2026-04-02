from __future__ import annotations

from mgc_v05l.app.uslate_pause_resume_long_cross_metal_anatomy import (
    _bucket_rows,
    _economic_metrics,
    _separator_summary,
)


def test_economic_metrics_tracks_loser_stats_and_concentration() -> None:
    rows = [
        {"entry_ts": "2026-03-01T14:00:00-05:00", "exit_ts": "2026-03-01T14:10:00-05:00", "net_pnl": 50.0},
        {"entry_ts": "2026-03-02T14:00:00-05:00", "exit_ts": "2026-03-02T14:10:00-05:00", "net_pnl": -10.0},
        {"entry_ts": "2026-03-03T14:00:00-05:00", "exit_ts": "2026-03-03T14:10:00-05:00", "net_pnl": 20.0},
        {"entry_ts": "2026-03-04T14:00:00-05:00", "exit_ts": "2026-03-04T14:10:00-05:00", "net_pnl": -5.0},
    ]
    metrics = _economic_metrics(rows)
    assert metrics["trades"] == 4
    assert metrics["average_loser"] == 7.5
    assert metrics["worst_loser"] == 10.0
    assert metrics["survives_without_top_1"] is True
    assert metrics["survives_without_top_3"] is False


def test_bucket_rows_creates_strong_and_fragile_groups() -> None:
    rows = [
        {"net_pnl": 100.0},
        {"net_pnl": 40.0},
        {"net_pnl": 10.0},
        {"net_pnl": -5.0},
        {"net_pnl": -15.0},
        {"net_pnl": -30.0},
    ]
    buckets = _bucket_rows(rows)
    assert len(buckets["strongest_winners"]) == 1
    assert buckets["strongest_winners"][0]["net_pnl"] == 100.0
    assert len(buckets["fragile_losers"]) == 1
    assert buckets["fragile_losers"][0]["net_pnl"] == -30.0


def test_separator_summary_orders_features_by_group_gap() -> None:
    positive = [
        {"entry_efficiency_5": 90.0, "entry_efficiency_10": 91.0, "initial_favorable_3bar": 10.0, "initial_adverse_3bar": 1.0, "mfe": 12.0, "mae": 1.0, "bars_held": 7.0, "mfe_capture_pct": 80.0, "entry_distance_fast_ema_atr": 0.2, "entry_distance_slow_ema_atr": 0.5, "entry_distance_vwap_atr": 3.0},
        {"entry_efficiency_5": 88.0, "entry_efficiency_10": 89.0, "initial_favorable_3bar": 9.0, "initial_adverse_3bar": 1.2, "mfe": 11.0, "mae": 1.1, "bars_held": 7.0, "mfe_capture_pct": 82.0, "entry_distance_fast_ema_atr": 0.3, "entry_distance_slow_ema_atr": 0.4, "entry_distance_vwap_atr": 2.5},
    ]
    baseline = [
        {"entry_efficiency_5": 45.0, "entry_efficiency_10": 50.0, "initial_favorable_3bar": 2.0, "initial_adverse_3bar": 3.0, "mfe": 3.0, "mae": 2.5, "bars_held": 2.0, "mfe_capture_pct": 35.0, "entry_distance_fast_ema_atr": 0.1, "entry_distance_slow_ema_atr": 0.2, "entry_distance_vwap_atr": 0.5},
        {"entry_efficiency_5": 40.0, "entry_efficiency_10": 48.0, "initial_favorable_3bar": 1.5, "initial_adverse_3bar": 3.4, "mfe": 2.5, "mae": 2.7, "bars_held": 2.0, "mfe_capture_pct": 30.0, "entry_distance_fast_ema_atr": 0.1, "entry_distance_slow_ema_atr": 0.2, "entry_distance_vwap_atr": 0.7},
    ]
    summary = _separator_summary(positive, baseline)
    assert summary
    assert any(item["feature"] == "entry_efficiency_5" for item in summary)
