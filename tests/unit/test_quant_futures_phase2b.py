from __future__ import annotations

from mgc_v05l.research.quant_futures_phase2b import _family_recommendation, _phase2b_rank_score


def test_phase2b_rank_score_favors_post_cost_viability_and_robustness() -> None:
    strong = _phase2b_rank_score(
        {
            "cost_expectancy_r_010": 0.03,
            "cost_expectancy_r_005": 0.07,
            "positive_symbol_share_cost_010": 0.67,
            "walk_forward_positive_ratio": 0.75,
            "trade_count": 120,
            "max_drawdown_r": 7.5,
            "concentration": {"top_3_share_of_total_r": 0.42},
            "dominant_session_share": 0.44,
            "threshold_sensitivity_penalty": 0.15,
            "gross_edge_penalty": 0.10,
        }
    )
    weak = _phase2b_rank_score(
        {
            "cost_expectancy_r_010": -0.03,
            "cost_expectancy_r_005": 0.00,
            "positive_symbol_share_cost_010": 0.17,
            "walk_forward_positive_ratio": 0.35,
            "trade_count": 24,
            "max_drawdown_r": 18.0,
            "concentration": {"top_3_share_of_total_r": 0.84},
            "dominant_session_share": 0.86,
            "threshold_sensitivity_penalty": 0.85,
            "gross_edge_penalty": 0.90,
        }
    )
    assert strong > weak


def test_family_recommendation_marks_small_gross_edge_as_split() -> None:
    row = {
        "variant": {"lane": "specialized_breakout"},
        "summary": {
            "viable_post_cost": False,
            "cost_expectancy_r_010": -0.01,
            "cost_expectancy_r_005": 0.02,
            "expectancy_r": 0.05,
        },
    }
    recommendation = _family_recommendation(row)
    assert recommendation["action"] == "split"


def test_family_recommendation_keeps_narrow_mean_reversion_lane_when_gross_edge_positive() -> None:
    row = {
        "variant": {"lane": "short_horizon_mean_reversion"},
        "summary": {
            "viable_post_cost": False,
            "cost_expectancy_r_010": -0.02,
            "cost_expectancy_r_005": -0.01,
            "expectancy_r": 0.03,
        },
    }
    recommendation = _family_recommendation(row)
    assert recommendation["action"] == "keep_narrow_lane"
