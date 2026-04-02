from __future__ import annotations

from mgc_v05l.app.gc_uslate_pause_resume_long_refinement_pass import (
    _material_improvement,
    _refinement_score,
    _verdict_bucket,
)


def test_material_improvement_requires_better_concentration_without_destroying_economics() -> None:
    baseline = {
        "realized_pnl": 456.0,
        "profit_factor": 2.8,
        "top_1_contribution": 106.0,
        "top_3_contribution": 136.0,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    improved = {
        "realized_pnl": 410.0,
        "profit_factor": 2.7,
        "top_1_contribution": 84.0,
        "top_3_contribution": 112.0,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    weak = {
        "realized_pnl": 250.0,
        "profit_factor": 1.4,
        "top_1_contribution": 90.0,
        "top_3_contribution": 115.0,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    assert _material_improvement(improved, baseline) is True
    assert _material_improvement(weak, baseline) is False


def test_refinement_score_can_prefer_real_concentration_improvement() -> None:
    baseline = {
        "realized_pnl": 456.0,
        "profit_factor": 2.8,
        "top_1_contribution": 106.0,
        "top_3_contribution": 136.0,
    }
    raw = {
        "realized_pnl": 456.0,
        "profit_factor": 2.8,
        "top_1_contribution": 106.0,
        "top_3_contribution": 136.0,
        "max_drawdown": 116.0,
        "trades": 22,
    }
    refined = {
        "realized_pnl": 420.0,
        "profit_factor": 2.7,
        "top_1_contribution": 80.0,
        "top_3_contribution": 105.0,
        "max_drawdown": 100.0,
        "trades": 18,
    }
    assert _refinement_score(refined, baseline, "tight_curvature") > _refinement_score(raw, baseline, "raw_baseline")


def test_verdict_bucket_distinguishes_raw_vs_unjustified_vs_improved() -> None:
    baseline = {
        "realized_pnl": 456.0,
        "profit_factor": 2.8,
        "top_1_contribution": 106.0,
        "top_3_contribution": 136.0,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    same = dict(baseline)
    improved = {
        "realized_pnl": 430.0,
        "profit_factor": 2.8,
        "top_1_contribution": 85.0,
        "top_3_contribution": 110.0,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    stronger = {
        "realized_pnl": 430.0,
        "profit_factor": 2.8,
        "top_1_contribution": 70.0,
        "top_3_contribution": 90.0,
        "survives_without_top_1": True,
        "survives_without_top_3": False,
    }
    weak = {
        "realized_pnl": 260.0,
        "profit_factor": 1.4,
        "top_1_contribution": 92.0,
        "top_3_contribution": 118.0,
        "survives_without_top_1": False,
        "survives_without_top_3": False,
    }
    assert _verdict_bucket("raw_baseline", same, baseline) == "RAW_BASELINE_REMAINS_BEST"
    assert _verdict_bucket("tight_curvature", weak, baseline) == "REFINEMENT_NOT_JUSTIFIED"
    assert _verdict_bucket("tight_curvature", improved, baseline) == "IMPROVED_BUT_STILL_DESIGN_STAGE"
    assert _verdict_bucket("tight_curvature", stronger, baseline) == "STRONGER_NARROW_PAPER_DESIGN_CANDIDATE"
