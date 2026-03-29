from __future__ import annotations

from mgc_v05l.research.quant_futures_phase2c import _phase2c_rank_score, _promotion_shortlist_status


def test_phase2c_rank_score_rewards_cost_stress_and_leave_one_out_strength() -> None:
    strong = _phase2c_rank_score(
        summary={
            "cost_expectancy_r_010": 0.10,
            "cost_expectancy_r_015": 0.06,
            "cost_expectancy_r_020": 0.02,
            "positive_symbol_share_cost_015": 0.75,
            "walk_forward_positive_ratio": 0.80,
            "trade_count": 220,
            "concentration": {"dominant_symbol_share_of_total_r": 0.32},
            "dominant_session_share": 0.42,
            "max_drawdown_r": 8.0,
        },
        robustness={
            "leave_one_symbol_out_positive_ratio": 1.0,
            "leave_one_session_out_positive_ratio": 0.67,
            "subperiod_stability": {"positive_ratio_cost_015": 0.67, "worst_cost_expectancy_r_015": -0.01},
            "perturbation": {"positive_ratio_cost_015": 0.56},
        },
        variant={"complexity_points": 4},
    )
    weak = _phase2c_rank_score(
        summary={
            "cost_expectancy_r_010": 0.01,
            "cost_expectancy_r_015": -0.03,
            "cost_expectancy_r_020": -0.07,
            "positive_symbol_share_cost_015": 0.25,
            "walk_forward_positive_ratio": 0.40,
            "trade_count": 60,
            "concentration": {"dominant_symbol_share_of_total_r": 0.78},
            "dominant_session_share": 0.88,
            "max_drawdown_r": 22.0,
        },
        robustness={
            "leave_one_symbol_out_positive_ratio": 0.25,
            "leave_one_session_out_positive_ratio": 0.0,
            "subperiod_stability": {"positive_ratio_cost_015": 0.17, "worst_cost_expectancy_r_015": -0.18},
            "perturbation": {"positive_ratio_cost_015": 0.11},
        },
        variant={"complexity_points": 5},
    )
    assert strong > weak


def test_promotion_shortlist_status_requires_stressed_cost_and_robustness() -> None:
    assert _promotion_shortlist_status(
        summary={
            "cost_expectancy_r_015": 0.03,
            "cost_expectancy_r_020": -0.01,
            "positive_symbol_share_cost_015": 0.50,
            "concentration": {"dominant_symbol_share_of_total_r": 0.45},
            "trade_count": 120,
        },
        robustness={
            "leave_one_symbol_out_positive_ratio": 0.75,
            "leave_one_session_out_positive_ratio": 0.50,
            "subperiod_stability": {"positive_ratio_cost_015": 0.50},
            "perturbation": {"positive_ratio_cost_015": 0.40},
        },
        variant={"role": "lane"},
    )
    assert not _promotion_shortlist_status(
        summary={
            "cost_expectancy_r_015": 0.03,
            "cost_expectancy_r_020": -0.01,
            "positive_symbol_share_cost_015": 0.50,
            "concentration": {"dominant_symbol_share_of_total_r": 0.45},
            "trade_count": 80,
        },
        robustness={
            "leave_one_symbol_out_positive_ratio": 0.75,
            "leave_one_session_out_positive_ratio": 0.50,
            "subperiod_stability": {"positive_ratio_cost_015": 0.50},
            "perturbation": {"positive_ratio_cost_015": 0.40},
        },
        variant={"role": "lane"},
    )
