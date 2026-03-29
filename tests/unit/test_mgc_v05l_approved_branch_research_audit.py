from mgc_v05l.app.approved_branch_research_audit import (
    _classify_discovery_failure,
    _classify_transfer_verdict,
    _direct_promotion_readiness,
    _robustness_funnel_bucket,
    _recommendation_score,
    _recommendation_bucket,
    classify_branch_portability,
)


def test_classify_branch_portability_reference_lane() -> None:
    assert classify_branch_portability(
        branch_metrics={"signals": 10, "closed_trades": 8, "realized_pnl": 100, "average_realized_per_trade": 12.5},
        baseline_metrics=None,
        instrument_symbol="MGC",
    ) == "REFERENCE_LANE"


def test_classify_branch_portability_portable_candidate() -> None:
    assert classify_branch_portability(
        branch_metrics={"signals": 12, "closed_trades": 8, "realized_pnl": 150, "average_realized_per_trade": 18.75},
        baseline_metrics={"signals": 20},
        instrument_symbol="MES",
    ) == "PORTABLE_CANDIDATE"


def test_classify_branch_portability_thin_sample() -> None:
    assert classify_branch_portability(
        branch_metrics={"signals": 3, "closed_trades": 1, "realized_pnl": 20, "average_realized_per_trade": 20},
        baseline_metrics={"signals": 10},
        instrument_symbol="CL",
    ) == "THIN_SAMPLE"


def test_classify_branch_portability_degraded() -> None:
    assert classify_branch_portability(
        branch_metrics={"signals": 12, "closed_trades": 8, "realized_pnl": -45, "average_realized_per_trade": -5.625},
        baseline_metrics={"signals": 16},
        instrument_symbol="NQ",
    ) == "DEGRADED_OUTSIDE_ORIGINAL_LANE"


def test_direct_promotion_readiness_marks_close_but_not_ready() -> None:
    assert _direct_promotion_readiness(
        branch_row={
            "branch": "asiaEarlyNormalBreakoutRetestHoldTurn",
            "closed_trades": 20,
            "realized_pnl": 0.05,
            "average_realized_per_trade": 0.0025,
        },
        dominant_phase="ASIA_EARLY",
        dominant_share=0.85,
    ) == "CLOSE_BUT_NOT_READY"


def test_recommendation_bucket_promotes_direct_candidate() -> None:
    assert _recommendation_bucket(
        {
            "promotion_readiness": "DIRECT_PROMOTION_CANDIDATE",
            "closed_trades": 18,
            "realized_pnl": 120.0,
            "average_realized_per_trade": 6.6667,
            "largest_loss": -15.0,
            "largest_win": 22.0,
            "win_rate": 0.44,
            "variant": "direct",
            "variant_overrides": {},
        }
    ) == "PROMOTE_TO_PROBATIONARY_RESEARCH_READY"


def test_recommendation_score_rewards_better_economics() -> None:
    strong = _recommendation_score(
        {
            "closed_trades": 20,
            "realized_pnl": 100.0,
            "average_realized_per_trade": 5.0,
            "win_rate": 0.45,
            "variant": "direct",
        }
    )
    weak = _recommendation_score(
        {
            "closed_trades": 8,
            "realized_pnl": 5.0,
            "average_realized_per_trade": 0.2,
            "win_rate": 0.35,
            "variant": "direct",
        }
    )
    assert strong > weak


def test_classify_discovery_failure_identifies_no_structural_fit() -> None:
    primary, secondary = _classify_discovery_failure(
        {
            "branch": "asiaEarlyPauseResumeShortTurn",
            "symbol": "PL",
            "raw_setup_count": 0,
            "blocked_count": 0,
            "intent_count": 0,
            "fill_count": 0,
            "realized_pnl": 0.0,
            "avg_trade": None,
            "profit_factor": None,
            "max_drawdown": 0.0,
            "pocket_concentration_ratio": None,
            "top_1_trade_contribution": None,
            "top_3_trade_contribution": None,
            "survives_without_top_1": None,
            "survives_without_top_3": None,
        }
    )
    assert primary == "NO_STRUCTURAL_FIT"
    assert secondary is None


def test_classify_transfer_verdict_marks_narrow_adaptation_when_structure_exists() -> None:
    verdict, reason = _classify_transfer_verdict(
        {
            "symbol": "6B",
            "branch": "asiaEarlyNormalBreakoutRetestHoldTurn",
            "raw_setup_count": 20,
            "fill_count": 8,
            "realized_pnl": -0.02,
            "avg_trade": -0.001,
            "failure_primary_cause": "STRUCTURALLY_PRESENT_BUT_RIGID_REPLAY_FORM",
            "portability_assessment": "MIXED_NEEDS_MORE_RESEARCH",
        }
    )
    assert verdict == "NARROW_ADAPTATION_WORTH_TESTING"
    assert "rigid" in reason.lower()


def test_robustness_funnel_bucket_advances_strong_direct_transfer() -> None:
    bucket = _robustness_funnel_bucket(
        row={
            "symbol": "PL",
            "branch": "usLatePauseResumeLongTurn",
            "transfer_verdict": "DIRECT_TRANSFER_CREDIBLE",
            "fill_count": 24,
            "realized_pnl": 250.0,
            "max_drawdown": 100.0,
        },
        adaptation_pairs=set(),
    )
    assert bucket == "ADVANCE_TO_ROBUSTNESS_TESTING"
