from __future__ import annotations

from mgc_v05l.research.quant_futures_phase2a import (
    _approaching_shortlist_quality,
    _continuation_score,
    _reversal_score,
)


def test_continuation_score_prefers_stronger_trend_context() -> None:
    strong = _continuation_score(
        direction="LONG",
        slope_240=1.2,
        slope_720=0.7,
        slope_1440=0.9,
        eff_240=0.65,
        eff_720=0.58,
        dist_240=0.45,
        vol_ratio_60_240=1.05,
        vol_ratio_240_1440=1.0,
    )
    weak = _continuation_score(
        direction="LONG",
        slope_240=-0.2,
        slope_720=0.0,
        slope_1440=-0.1,
        eff_240=0.32,
        eff_720=0.28,
        dist_240=1.9,
        vol_ratio_60_240=2.1,
        vol_ratio_240_1440=0.2,
    )
    assert strong > weak


def test_reversal_score_short_prefers_extension_and_mature_context() -> None:
    strong = _reversal_score(
        direction="SHORT",
        slope_240=0.8,
        slope_720=0.5,
        eff_240=0.48,
        dist_240=1.4,
        dist_1440=1.0,
        vol_ratio_60_240=1.35,
    )
    weak = _reversal_score(
        direction="SHORT",
        slope_240=-0.4,
        slope_720=-0.3,
        eff_240=0.82,
        dist_240=-0.2,
        dist_1440=-0.1,
        vol_ratio_60_240=0.7,
    )
    assert strong > weak


def test_approaching_shortlist_quality_requires_breadth_stability_and_cost_survival() -> None:
    assert _approaching_shortlist_quality(
        {
            "expectancy_r": 0.06,
            "positive_symbol_share": 0.60,
            "walk_forward_positive_ratio": 0.70,
            "cost_expectancy_r_010": 0.01,
            "concentration": {"top_3_share_of_total_r": 0.55},
        }
    )
    assert not _approaching_shortlist_quality(
        {
            "expectancy_r": 0.06,
            "positive_symbol_share": 0.40,
            "walk_forward_positive_ratio": 0.70,
            "cost_expectancy_r_010": 0.01,
            "concentration": {"top_3_share_of_total_r": 0.55},
        }
    )
