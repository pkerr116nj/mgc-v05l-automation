from __future__ import annotations

from mgc_v05l.app.uslate_pause_resume_long_shared_metals_refinement import (
    _delta_vs_raw,
    _material_improvement,
    _verdict_bucket,
)


def test_delta_vs_raw_tracks_middle_and_fragile_loser_changes() -> None:
    row = {
        "metrics": {"trades": 10, "realized_pnl": 120.0, "median_trade": 3.0, "profit_factor": 2.0, "max_drawdown": 20.0, "top_1_contribution": 40.0, "top_3_contribution": 80.0},
        "anatomy": {"middle_pnl_ex_top3": 25.0, "fragile_loser_realized_pnl": -20.0, "fragile_loser_mean_initial_adverse_3bar": 2.0},
    }
    baseline = {
        "metrics": {"trades": 12, "realized_pnl": 100.0, "median_trade": 1.0, "profit_factor": 1.8, "max_drawdown": 25.0, "top_1_contribution": 55.0, "top_3_contribution": 110.0},
        "anatomy": {"middle_pnl_ex_top3": 10.0, "fragile_loser_realized_pnl": -35.0, "fragile_loser_mean_initial_adverse_3bar": 3.5},
    }
    delta = _delta_vs_raw(row, baseline)
    assert delta["middle_pnl_ex_top3_delta"] == 15.0
    assert delta["fragile_loser_pnl_delta"] == 15.0
    assert delta["top_3_contribution_delta"] == -30.0


def test_material_improvement_requires_retention_and_quality_gain() -> None:
    improved = {
        "metrics": {"realized_pnl": 90.0, "profit_factor": 1.9, "top_3_contribution": 90.0},
        "anatomy": {"middle_pnl_ex_top3": 5.0, "fragile_loser_realized_pnl": -15.0},
    }
    baseline = {
        "metrics": {"realized_pnl": 100.0, "profit_factor": 1.8, "top_3_contribution": 115.0},
        "anatomy": {"middle_pnl_ex_top3": 0.0, "fragile_loser_realized_pnl": -25.0},
    }
    assert _material_improvement(improved, baseline) is True


def test_verdict_prefers_raw_when_best_variant_is_baseline() -> None:
    results = {
        "MGC": [{"slug": "raw_baseline", "metrics": {"realized_pnl": 100.0, "profit_factor": 2.0}, "anatomy": {"middle_pnl_ex_top3": 0.0, "fragile_loser_realized_pnl": -20.0}}],
        "GC": [{"slug": "raw_baseline", "metrics": {"realized_pnl": 60.0, "profit_factor": 1.5}, "anatomy": {"middle_pnl_ex_top3": -5.0, "fragile_loser_realized_pnl": -25.0}}],
    }
    best = {
        "slug": "raw_baseline",
        "per_instrument": {
            "MGC": results["MGC"][0],
            "GC": results["GC"][0],
        },
    }
    assert _verdict_bucket(results, best) == "RAW_BASELINE_REMAINS_BEST"
