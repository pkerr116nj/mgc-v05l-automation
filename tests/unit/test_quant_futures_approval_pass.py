from __future__ import annotations

from mgc_v05l.research.quant_futures_approval_pass import _approval_score, _approval_verdict


def test_approval_score_rewards_tough_cost_survival_and_slice_robustness() -> None:
    strong = _approval_score(
        summary={},
        approval={
            "cost_expectancy_r_020": 0.08,
            "cost_expectancy_r_025": 0.04,
            "positive_symbol_share_cost_020": 0.75,
            "positive_symbol_share_cost_025": 0.50,
            "leave_one_symbol_out_positive_ratio_020": 1.0,
            "leave_one_session_out_positive_ratio_020": 0.67,
            "subperiod": {"positive_ratio_cost_020": 0.67, "worst_cost_expectancy_r_025": -0.01},
            "perturbation": {"positive_ratio_cost_020": 0.67},
            "walk_forward_positive_ratio_cost_020": 0.75,
            "walk_forward_positive_ratio_cost_025": 0.50,
            "concentration": {"dominant_symbol_share_of_total_r": 0.35},
        },
        variant={"complexity_points": 4},
    )
    weak = _approval_score(
        summary={},
        approval={
            "cost_expectancy_r_020": -0.01,
            "cost_expectancy_r_025": -0.05,
            "positive_symbol_share_cost_020": 0.25,
            "positive_symbol_share_cost_025": 0.0,
            "leave_one_symbol_out_positive_ratio_020": 0.25,
            "leave_one_session_out_positive_ratio_020": 0.0,
            "subperiod": {"positive_ratio_cost_020": 0.17, "worst_cost_expectancy_r_025": -0.12},
            "perturbation": {"positive_ratio_cost_020": 0.11},
            "walk_forward_positive_ratio_cost_020": 0.25,
            "walk_forward_positive_ratio_cost_025": 0.0,
            "concentration": {"dominant_symbol_share_of_total_r": 0.82},
        },
        variant={"complexity_points": 5},
    )
    assert strong > weak


def test_approval_verdict_requires_positive_cost_025_and_strict_robustness() -> None:
    approved = _approval_verdict(
        row={
            "approval": {
                "cost_expectancy_r_020": 0.06,
                "cost_expectancy_r_025": 0.02,
                "positive_symbol_share_cost_020": 0.50,
                "leave_one_symbol_out_positive_ratio_020": 0.75,
                "leave_one_session_out_positive_ratio_020": 0.50,
                "subperiod": {"positive_ratio_cost_020": 0.50},
                "perturbation": {"positive_ratio_cost_020": 0.50},
                "concentration": {"dominant_symbol_share_of_total_r": 0.50},
            }
        }
    )
    rejected = _approval_verdict(
        row={
            "approval": {
                "cost_expectancy_r_020": 0.06,
                "cost_expectancy_r_025": -0.01,
                "positive_symbol_share_cost_020": 0.50,
                "leave_one_symbol_out_positive_ratio_020": 0.75,
                "leave_one_session_out_positive_ratio_020": 0.50,
                "subperiod": {"positive_ratio_cost_020": 0.50},
                "perturbation": {"positive_ratio_cost_020": 0.50},
                "concentration": {"dominant_symbol_share_of_total_r": 0.50},
            }
        }
    )
    assert approved["approved"] is True
    assert rejected["approved"] is False
