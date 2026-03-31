from mgc_v05l.app.mgc_impulse_burst_asymmetry_report import (
    _candidate_verdict,
    _losses_naturally_small_enough,
    _right_tail_repeatability,
)


def test_right_tail_repeatability_marks_recurring_when_large_winners_repeat() -> None:
    assert _right_tail_repeatability([1.0, 2.0, 6.5, 7.0, 8.0, 10.5, 12.0, 13.0], 2.0) == "RECURRING"


def test_losses_naturally_small_enough_respects_proxy_bounds() -> None:
    assert _losses_naturally_small_enough(r_loss_proxy=10.0, losers=[8.0, 10.0, 12.0, 18.0, 22.0])
    assert not _losses_naturally_small_enough(r_loss_proxy=10.0, losers=[8.0, 10.0, 12.0, 28.0, 45.0])


def test_candidate_verdict_marks_right_tail_isolated_when_tail_is_thin() -> None:
    verdict = _candidate_verdict(
        {
            "core_performance": {
                "profit_factor": 1.15,
                "realized_pnl": 100.0,
            },
            "payoff_structure": {
                "avg_winner_over_avg_loser": 3.0,
                "r_equivalent_proxy": 10.0,
            },
            "right_tail_repeatability": {
                "large_winner_count": 2,
                "outsized_winners_recur": "LIMITED",
                "top_3_contribution": 180.0,
            },
            "loss_containment": {
                "p95_loser": 18.0,
            },
        }
    )
    assert verdict == "STRUCTURALLY_REAL_BUT_RIGHT_TAIL_TOO_ISOLATED"
