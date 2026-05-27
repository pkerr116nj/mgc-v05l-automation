from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_hold_exit_shadow_engine import (
    EXTEND_HOLD,
    HOLD_EXTEND_PARTICIPATION_STRONG,
    INSUFFICIENT_EVIDENCE_STATE,
    NO_RECOMMENDATION,
)
from mgc_v05l.execution_core.track_b_managed_position_hold_exit_shadow import (
    MANAGED_POSITION_HOLD_EXIT_SHADOW_NO_OPEN_POSITIONS,
    MANAGED_POSITION_HOLD_EXIT_SHADOW_READY,
    ManagedPositionHoldExitShadowConfig,
    build_managed_position_hold_exit_shadow,
    write_managed_position_hold_exit_shadow,
)


NOW = datetime(2026, 5, 27, 14, 15, tzinfo=UTC)


def test_open_managed_position_receives_shadow_recommendation(tmp_path: Path) -> None:
    _write_intent_audit(tmp_path)
    registry = _managed_registry(
        [
            {
                **_managed_position(),
                "completed_5m_bars_since_entry": 2,
                "mfe_points": "4.5",
                "current_net_points": "3.8",
                "participation_state": "STRONG_FOLLOW_THROUGH",
            }
        ]
    )

    payload = build_managed_position_hold_exit_shadow(
        config=ManagedPositionHoldExitShadowConfig(repo_root=tmp_path),
        managed_position_registry=registry,
        now=NOW,
    )

    row = payload["managed_position_recommendations"][0]
    assert payload["classification"] == MANAGED_POSITION_HOLD_EXIT_SHADOW_READY
    assert row["hold_state"] == HOLD_EXTEND_PARTICIPATION_STRONG
    assert row["shadow_exit_recommendation"] == EXTEND_HOLD
    assert row["position_intent_summary"]["thesis_type"] == "DRIFT"
    assert row["assigned_live_exit_profile"] == "MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1"
    assert row["submit_authority"] is False
    assert row["broker_mutation_allowed"] is False
    assert row["lifecycle_authority"] is False


def test_no_open_position_yields_ready_noop_status(tmp_path: Path) -> None:
    _write_intent_audit(tmp_path)

    payload = build_managed_position_hold_exit_shadow(
        config=ManagedPositionHoldExitShadowConfig(repo_root=tmp_path),
        managed_position_registry=_managed_registry([]),
        now=NOW,
    )

    assert payload["classification"] == MANAGED_POSITION_HOLD_EXIT_SHADOW_NO_OPEN_POSITIONS
    assert payload["managed_position_recommendations"] == []
    assert payload["summary"]["recommendation_count"] == 0


def test_missing_market_data_yields_insufficient_evidence(tmp_path: Path) -> None:
    _write_intent_audit(tmp_path)
    registry = _managed_registry([{**_managed_position(), "completed_5m_bars_since_entry": 2}])

    payload = build_managed_position_hold_exit_shadow(
        config=ManagedPositionHoldExitShadowConfig(repo_root=tmp_path),
        managed_position_registry=registry,
        now=NOW,
    )

    row = payload["managed_position_recommendations"][0]
    assert row["hold_state"] == INSUFFICIENT_EVIDENCE_STATE
    assert row["shadow_exit_recommendation"] == NO_RECOMMENDATION
    assert "mfe_or_current_net" in row["evidence_gaps"]


def test_bug_fix_remediation_position_excluded_from_alpha_learning(tmp_path: Path) -> None:
    _write_intent_audit(tmp_path)
    registry = _managed_registry(
        [
            {
                **_managed_position(),
                "completed_5m_bars_since_entry": 3,
                "mfe_points": "5",
                "current_net_points": "1",
                "remediation_trade": True,
            }
        ]
    )

    payload = build_managed_position_hold_exit_shadow(
        config=ManagedPositionHoldExitShadowConfig(repo_root=tmp_path),
        managed_position_registry=registry,
        now=NOW,
    )

    row = payload["managed_position_recommendations"][0]
    assert row["eligible_for_alpha_exit_analysis"] is False
    assert row["exclusion_reason"] == "contamination_flags_present"
    assert "REMEDIATION_TRADE" in row["contamination_flags"]


def test_write_emits_latest_and_jsonl_without_authority(tmp_path: Path) -> None:
    _write_intent_audit(tmp_path)
    payload = build_managed_position_hold_exit_shadow(
        config=ManagedPositionHoldExitShadowConfig(repo_root=tmp_path),
        managed_position_registry=_managed_registry([]),
        now=NOW,
    )

    output_path = write_managed_position_hold_exit_shadow(
        config=ManagedPositionHoldExitShadowConfig(repo_root=tmp_path),
        payload=payload,
    )

    assert output_path.exists()
    written = json.loads(output_path.read_text(encoding="utf-8"))
    assert written["submit_authority"] is False
    assert written["broker_mutation_allowed"] is False
    assert written["lifecycle_authority"] is False
    events = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "research_shadow"
        / "managed_position_hold_exit_shadow_events.jsonl"
    )
    assert events.exists()


def _managed_registry(positions: list[dict]) -> dict:
    return {
        "schema_version": "track_b_managed_position_registry_v1",
        "classification": "NO_MANAGED_POSITIONS" if not positions else "OPEN_MANAGED_MATCHED",
        "managed_positions": positions,
    }


def _managed_position() -> dict:
    return {
        "classification": "OPEN_MANAGED_MATCHED",
        "strategy_id": "asian_drift_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "lifecycle_id": "lifecycle-mgc-1",
        "symbol": "MGC",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "side": "LONG",
        "quantity": "1",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    }


def _write_intent_audit(root: Path) -> None:
    path = (
        root
        / "outputs"
        / "track_b_execution_core"
        / "diagnostics"
        / "latest_position_intent_contract_audit.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "POSITION_INTENT_CONTRACT_READY",
                "strategies": [
                    {
                        "strategy_id": "asian_drift_v1",
                        "lane_id": "mgc_example_long_lmt_day",
                        "position_intent": {
                            "strategy_id": "asian_drift_v1",
                            "lane_id": "mgc_example_long_lmt_day",
                            "instrument_family": "MGC",
                            "local_symbol": "MGCM6",
                            "con_id": 712565978,
                            "expiry": "20260626",
                            "side": "LONG",
                            "quantity": 1,
                            "trade_thesis": {
                                "thesis_type": "DRIFT",
                                "thesis_summary": "Asia-session drift continuation.",
                                "invalidation_conditions": ["drift_context_fails"],
                            },
                            "hold_policy": {
                                "expected_hold_type": "PARTICIPATION_HOLD",
                                "max_hold_policy": "3_COMPLETED_5M_BARS",
                                "expected_hold_bars_5m": 3,
                            },
                            "exit_policy": {
                                "intended_exit_family": "TIME_BOX_EXIT",
                                "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                                "exit_profile_id": "MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1",
                            },
                            "pyramiding_policy": "PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED",
                            "conflict_group": "gold_mgc_gc",
                            "attribution_tags": {
                                "eligible_for_alpha_exit_analysis": True,
                                "contamination_flags": [],
                            },
                        },
                        "strategy_hold_exit_policy": {
                            "strategy_id": "asian_drift_v1",
                            "lane_id": "mgc_example_long_lmt_day",
                            "hold_policy_id": "ASIAN_DRIFT_PARTICIPATION_HOLD_SHADOW_V1",
                            "exit_policy_family": "TIME_BOX_PLUS_PARTICIPATION_DECAY_SHADOW",
                            "profit_harvest_policy": "MGC_DRIFT_PROFIT_HARVEST_SHADOW_V1",
                            "thesis_failure_conditions": ["drift_context_fails"],
                            "participation_decay_inputs": ["phase1_5m_closes"],
                            "max_hold_policy": "3_COMPLETED_5M_BARS",
                        },
                    }
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
