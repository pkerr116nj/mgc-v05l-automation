from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_exit_attribution_policy_v2 import ALPHA_EXIT, BUG_FIX_EXIT
from mgc_v05l.execution_core.track_b_hold_exit_shadow_engine import (
    EXIT_DECAY,
    EXIT_DECAY_STATE,
    EXIT_THESIS_FAILURE,
    EXIT_THESIS_FAILURE_STATE,
    EXTEND_HOLD,
    HARVEST,
    HARVEST_PROFIT_AVAILABLE,
    HOLD_EXIT_SHADOW_READY,
    HOLD_EXTEND_PARTICIPATION_STRONG,
    INSUFFICIENT_EVIDENCE_STATE,
    NO_RECOMMENDATION,
    TIMEBOX_EXIT,
    TIMEBOX_EXIT_DUE_STATE,
    HoldExitShadowEngineConfig,
    evaluate_hold_exit_shadow,
    run_hold_exit_shadow_engine,
)
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES,
    PositionIntentContractAuditConfig,
    build_position_intent_contract_audit,
    position_intent_from_template,
)
from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import strategy_hold_exit_policy_for


NOW = datetime(2026, 5, 27, 14, 0, tzinfo=UTC)


def test_strong_participation_with_favorable_mfe_extends_hold() -> None:
    result = evaluate_hold_exit_shadow(
        position_intent=_intent_payload("asian_drift_v1"),
        evidence={
            "completed_5m_bars_since_entry": 2,
            "mfe_points": "4.5",
            "mae_points": "0.5",
            "current_net_points": "3.8",
            "giveback_from_mfe_points": "0.7",
            "participation_state": "STRONG_ALIGNED_CONTINUATION",
        },
        generated_at=NOW,
    )

    assert result["hold_state"] == HOLD_EXTEND_PARTICIPATION_STRONG
    assert result["recommendation"] == EXTEND_HOLD
    assert result["hold_policy_id"] == "ASIAN_DRIFT_PARTICIPATION_HOLD_SHADOW_V1"
    assert result["exit_policy_family"] == "TIME_BOX_PLUS_PARTICIPATION_DECAY_SHADOW"
    assert result["submit_allowed"] is False
    assert result["broker_mutation_allowed"] is False
    assert result["lifecycle_authority"] is False


def test_high_giveback_and_participation_decay_harvests() -> None:
    result = evaluate_hold_exit_shadow(
        position_intent=_intent_payload("asian_drift_v1"),
        evidence={
            "completed_5m_bars_since_entry": 2,
            "mfe_points": "8",
            "mae_points": "1",
            "current_net_points": "3",
            "giveback_from_mfe_points": "5",
            "participation_state": "PARTICIPATION_DECAY",
        },
        generated_at=NOW,
    )

    assert result["hold_state"] == HARVEST_PROFIT_AVAILABLE
    assert result["recommendation"] == HARVEST


def test_decay_without_material_giveback_recommends_exit_decay() -> None:
    result = evaluate_hold_exit_shadow(
        position_intent=_intent_payload("MNQ_FIRST_BEAR_SNAP_TURN_V1"),
        evidence={
            "completed_5m_bars_since_entry": 1,
            "mfe_points": "3",
            "mae_points": "2",
            "current_net_points": "1",
            "giveback_from_mfe_points": "2",
            "microtrend_state": "WEAK_OR_STAGNANT",
        },
        generated_at=NOW,
    )

    assert result["hold_state"] == EXIT_DECAY_STATE
    assert result["recommendation"] == EXIT_DECAY


def test_thesis_invalidation_recommends_thesis_failure_exit() -> None:
    result = evaluate_hold_exit_shadow(
        position_intent=_intent_payload("MNQ_FIRST_BULL_SNAP_TURN_V1"),
        evidence={
            "completed_5m_bars_since_entry": 1,
            "mfe_points": "1",
            "mae_points": "25",
            "current_net_points": "-20",
            "giveback_from_mfe_points": "21",
            "thesis_invalidated": True,
        },
        generated_at=NOW,
    )

    assert result["hold_state"] == EXIT_THESIS_FAILURE_STATE
    assert result["recommendation"] == EXIT_THESIS_FAILURE


def test_max_hold_exceeded_recommends_timebox_exit() -> None:
    result = evaluate_hold_exit_shadow(
        position_intent=_intent_payload("asian_drift_v1"),
        evidence={
            "completed_5m_bars_since_entry": 3,
            "mfe_points": "2.2",
            "mae_points": "0.6",
            "current_net_points": "1.1",
            "giveback_from_mfe_points": "1.1",
            "participation_state": "NEUTRAL",
        },
        generated_at=NOW,
    )

    assert result["hold_state"] == TIMEBOX_EXIT_DUE_STATE
    assert result["recommendation"] == TIMEBOX_EXIT


def test_missing_data_returns_insufficient_evidence() -> None:
    result = evaluate_hold_exit_shadow(
        position_intent=_intent_payload("asian_drift_v1"),
        evidence={"completed_5m_bars_since_entry": None},
        generated_at=NOW,
    )

    assert result["hold_state"] == INSUFFICIENT_EVIDENCE_STATE
    assert result["recommendation"] == NO_RECOMMENDATION


def test_bug_fix_exits_are_excluded_from_alpha_learning(tmp_path: Path) -> None:
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_position_intent_contract_audit.json",
        build_position_intent_contract_audit(config=PositionIntentContractAuditConfig(repo_root=tmp_path), now=NOW),
    )
    _write_json(
        tmp_path
        / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_broker_reconciled_live_position_status.json",
        {"broker_track_b_positions": []},
    )
    _write_json(
        tmp_path / "outputs/track_b_execution_core/diagnostics/latest_exit_attribution_review.json",
        {
            "trades": [
                {
                    "trade_id": "bugfix",
                    "strategy_id": "asian_drift_v1",
                    "lane_id": "mgc_example_long_lmt_day",
                    "exit_intent_category": BUG_FIX_EXIT,
                    "eligible_for_alpha_exit_analysis": False,
                },
                {
                    "trade_id": "alpha",
                    "strategy_id": "asian_drift_v1",
                    "lane_id": "mgc_example_long_lmt_day",
                    "exit_intent_category": ALPHA_EXIT,
                    "eligible_for_alpha_exit_analysis": True,
                    "instrument": "MGC",
                    "side": "LONG",
                    "quantity": "1",
                    "bars_held": 2,
                    "mfe_points": "5",
                    "mae_points": "1",
                    "realized_points": "4",
                    "giveback_from_mfe_points": "1",
                    "participation_state": "STRONG",
                },
            ]
        },
    )

    payload = run_hold_exit_shadow_engine(config=HoldExitShadowEngineConfig(repo_root=tmp_path), now=NOW)

    assert payload["classification"] == HOLD_EXIT_SHADOW_READY
    assert payload["summary"]["skipped_bug_fix_exit_count"] == 1
    assert payload["summary"]["closed_alpha_recommendation_count"] == 1
    assert payload["closed_alpha_exit_recommendations"][0]["trade_id"] == "alpha"
    assert payload["closed_alpha_exit_recommendations"][0]["hold_policy_id"] == "ASIAN_DRIFT_PARTICIPATION_HOLD_SHADOW_V1"
    assert payload["submit_allowed"] is False
    assert payload["broker_mutation_allowed"] is False
    assert payload["lifecycle_authority"] is False


def _intent_payload(strategy_id: str) -> dict:
    intent = position_intent_from_template(APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES[strategy_id])
    payload = json.loads(json.dumps(intent, default=lambda value: getattr(value, "__dict__", str(value))))
    payload["strategy_hold_exit_policy"] = strategy_hold_exit_policy_for(strategy_id)
    return payload


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
