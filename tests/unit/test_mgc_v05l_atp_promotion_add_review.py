from __future__ import annotations

from datetime import date, datetime, timedelta

from mgc_v05l.research.trend_participation import ConflictOutcome
from mgc_v05l.research.trend_participation.atp_promotion_add_review import (
    _candidate_summary,
    _candidate_lane_readiness,
    _candidate_session_acceptability,
    build_candidate_branch_registry,
    build_atp_candidate_lane_identity,
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from mgc_v05l.research.trend_participation.models import ResearchBar, TradeRecord


def _trade() -> TradeRecord:
    return TradeRecord(
        instrument="MGC",
        variant_id="trend_participation.atp_v1_long_pullback_continuation.long.base",
        family="atp_v1_long_pullback_continuation",
        side="LONG",
        live_eligible=True,
        shadow_only=False,
        conflict_outcome=ConflictOutcome.NO_CONFLICT,
        decision_id="decision-1",
        decision_ts=datetime(2026, 3, 10, 10, 0),
        entry_ts=datetime(2026, 3, 10, 10, 1),
        exit_ts=datetime(2026, 3, 10, 10, 5),
        entry_price=100.0,
        exit_price=106.0,
        stop_price=98.0,
        target_price=None,
        pnl_points=6.0,
        gross_pnl_cash=30.0,
        pnl_cash=28.5,
        fees_paid=1.5,
        slippage_cost=0.0,
        mfe_points=7.0,
        mae_points=1.0,
        bars_held_1m=4,
        hold_minutes=4.0,
        exit_reason="trend_failure",
        is_reentry=False,
        reentry_type="NONE",
        stopout=False,
        setup_signature="setup-1",
        setup_quality_bucket="HIGH",
        session_segment="US",
        regime_bucket="TREND",
        volatility_bucket="MEDIUM",
    )


def _bar(minute_offset: int, *, open_: float, high: float, low: float, close: float) -> ResearchBar:
    ts = datetime(2026, 3, 10, 10, 1) + timedelta(minutes=minute_offset)
    return ResearchBar(
        instrument="MGC",
        timeframe="1m",
        start_ts=ts - timedelta(minutes=1),
        end_ts=ts,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=100,
        session_label="US",
        session_segment="US",
    )


def test_evaluate_promotion_add_candidate_emits_earned_add_on_strong_follow_through() -> None:
    favorable_only = next(
        candidate
        for candidate in default_atp_promotion_add_candidates()
        if candidate.candidate_id == "promotion_1_075r_favorable_only"
    )
    result = evaluate_promotion_add_candidate(
        trade=_trade(),
        minute_bars=[
            _bar(1, open_=100.9, high=101.4, low=100.4, close=101.0),
            _bar(2, open_=101.2, high=102.0, low=100.6, close=101.9),
            _bar(3, open_=102.0, high=102.4, low=101.8, close=102.2),
        ],
        candidate=favorable_only,
        point_value=5.0,
    )

    assert result["added"] is True
    assert result["candidate_id"] == "promotion_1_075r_favorable_only"
    assert result["add_entry_ts"] == datetime(2026, 3, 10, 10, 3)
    assert result["add_price_quality_state"] == "VWAP_FAVORABLE"
    assert result["depends_on_weak_evidence"] is False
    assert result["pnl_cash"] > result["trade_pnl_cash"]


def test_favorable_only_candidate_blocks_when_only_neutral_quality_is_available() -> None:
    neutral_plus = next(
        candidate
        for candidate in default_atp_promotion_add_candidates()
        if candidate.candidate_id == "promotion_1_075r_neutral_plus"
    )
    favorable_only = next(
        candidate
        for candidate in default_atp_promotion_add_candidates()
        if candidate.candidate_id == "promotion_1_075r_favorable_only"
    )
    bars = [
        _bar(1, open_=100.9, high=101.4, low=100.4, close=101.0),
        _bar(2, open_=101.6, high=102.0, low=100.6, close=101.7),
    ]

    neutral_result = evaluate_promotion_add_candidate(
        trade=_trade(),
        minute_bars=bars,
        candidate=neutral_plus,
        point_value=5.0,
    )
    favorable_result = evaluate_promotion_add_candidate(
        trade=_trade(),
        minute_bars=bars,
        candidate=favorable_only,
        point_value=5.0,
    )

    assert neutral_result["added"] is True
    assert neutral_result["add_price_quality_state"] == "VWAP_NEUTRAL"
    assert favorable_result["added"] is False
    assert favorable_result["add_reason"] == "PROMOTION_NOT_EARNED"
    assert favorable_result["pnl_cash"] == favorable_result["trade_pnl_cash"]


def test_no_add_path_preserves_frozen_baseline_trade_outcome() -> None:
    candidate = default_atp_promotion_add_candidates()[0]
    result = evaluate_promotion_add_candidate(
        trade=_trade(),
        minute_bars=[_bar(1, open_=100.2, high=100.8, low=99.9, close=100.3)],
        candidate=candidate,
        point_value=5.0,
    )

    assert result["added"] is False
    assert result["add_reason"] == "PROMOTION_NOT_EARNED"
    assert result["pnl_cash"] == 28.5
    assert result["modeled_exit_dependency"] == "inherits_frozen_baseline_exit"
    assert result["session_segment"] == "US"


def test_candidate_lane_identity_is_explicit_and_paper_disabled_in_this_pass() -> None:
    identity = build_atp_candidate_lane_identity(
        candidate_payload={
            "candidate_id": "promotion_1_075r_favorable_only",
            "label": "Promotion 1 at +0.75R, VWAP favorable only",
        }
    )

    assert identity["candidate_status"] == "RESEARCH_CANDIDATE_ONLY"
    assert identity["replay_strategy_id"] == "atp_companion_v1_asia_us__promotion_1_075r_favorable_only"
    assert identity["study_candidate_id"] == "ATP_COMPANION_V1_ASIA_US__promotion_1_075r_favorable_only"
    assert identity["paper_lane_enabled"] is False
    assert identity["paper_lane_status"] == "IDENTITY_ONLY_NOT_ENABLED"


def test_candidate_lane_readiness_stays_research_only_when_us_add_contribution_is_negative() -> None:
    candidate_payload = {
        "quality_verdict": "QUALITY_IMPROVED",
        "session_breakdown": {
            "ASIA": {"add_count": 3, "add_contribution_net_pnl_cash": 64.45, "net_pnl_cash": 524.42, "trade_count": 47},
            "US": {"add_count": 2, "add_contribution_net_pnl_cash": -30.40, "net_pnl_cash": 314.01, "trade_count": 64},
        },
    }

    session_acceptability = _candidate_session_acceptability(candidate_payload)
    readiness = _candidate_lane_readiness(
        candidate_payload=candidate_payload,
        session_acceptability=session_acceptability,
    )

    assert session_acceptability["overall_status"] == "LIMITED_BY_SESSION_WEAKNESS"
    assert session_acceptability["sessions"]["US"]["status"] == "NEGATIVE"
    assert readiness["verdict"] == "RESEARCH_WORTHY_NOT_PROMOTION_READY"


def test_candidate_branch_registry_retains_all_candidates_and_advances_only_strongest() -> None:
    registry = build_candidate_branch_registry(
        [
            {"candidate_id": "promotion_1_050r_neutral_plus", "label": "A", "quality_verdict": "QUANTITY_UP_QUALITY_MIXED", "baseline_delta": {"net_pnl_cash_delta": 1.0, "profit_factor_delta": 0.1}},
            {"candidate_id": "promotion_1_075r_neutral_plus", "label": "B", "quality_verdict": "QUALITY_IMPROVED", "baseline_delta": {"net_pnl_cash_delta": 2.0, "profit_factor_delta": 0.2}},
            {"candidate_id": "promotion_1_075r_favorable_only", "label": "C", "quality_verdict": "QUALITY_IMPROVED", "baseline_delta": {"net_pnl_cash_delta": 3.0, "profit_factor_delta": 0.3}},
        ]
    )

    assert registry["advanced_candidate_id"] == "promotion_1_075r_favorable_only"
    assert len(registry["items"]) == 3
    statuses = {item["candidate_id"]: item["status"] for item in registry["items"]}
    assert statuses["promotion_1_050r_neutral_plus"] == "RETAINED_RESEARCH_CANDIDATE"
    assert statuses["promotion_1_075r_neutral_plus"] == "RETAINED_RESEARCH_CANDIDATE"
    assert statuses["promotion_1_075r_favorable_only"] == "ACTIVE_RESEARCH_CANDIDATE"


def test_candidate_summary_reports_add_only_metrics_from_incremental_add_pnl() -> None:
    candidate = next(
        item
        for item in default_atp_promotion_add_candidates()
        if item.candidate_id == "promotion_1_075r_favorable_only"
    )
    summary = _candidate_summary(
        candidate=candidate,
        baseline_summary={
            "total_trades": 2,
            "net_pnl_cash": 150.0,
            "average_trade_pnl_cash": 75.0,
            "profit_factor": 2.0,
            "max_drawdown": 10.0,
            "win_rate": 50.0,
        },
        position_rows=[
            {
                "entry_ts": datetime(2026, 3, 10, 10, 1),
                "decision_ts": datetime(2026, 3, 10, 10, 0),
                "pnl_cash": 120.0,
                "add_pnl_cash": 20.0,
                "added": True,
                "add_entry_ts": datetime(2026, 3, 10, 10, 3),
                "add_hold_minutes": 2.0,
                "bars_held_1m": 4,
                "side": "LONG",
                "session_segment": "ASIA",
                "mfe_points": 0.0,
                "mae_points": 0.0,
            },
            {
                "entry_ts": datetime(2026, 3, 10, 11, 1),
                "decision_ts": datetime(2026, 3, 10, 11, 0),
                "pnl_cash": 40.0,
                "add_pnl_cash": 0.0,
                "added": False,
                "add_entry_ts": None,
                "add_hold_minutes": 0.0,
                "bars_held_1m": 3,
                "side": "LONG",
                "session_segment": "US",
                "mfe_points": 0.0,
                "mae_points": 0.0,
            },
        ],
        bar_count=100,
    )

    assert summary["add_count"] == 1
    assert summary["add_contribution_net_pnl_cash"] == 20.0
    assert summary["add_only_metrics"]["total_trades"] == 1
    assert summary["add_only_metrics"]["net_pnl_cash"] == 20.0
