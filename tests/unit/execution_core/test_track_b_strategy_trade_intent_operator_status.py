from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.operator_status import OperatorStatusInputs, create_operator_status_summary


def aware_now() -> datetime:
    return datetime(2026, 5, 7, 15, 0, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def test_operator_status_exposes_strategy_trade_intent_from_paper_runner(tmp_path: Path) -> None:
    runner_path = write_json(
        tmp_path / "runner" / "latest_track_b_strategy_paper_runner_report.json",
        {
            "schema_version": "track_b_strategy_paper_runner_v1",
            "strategy_paper_runner_verdict": "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_OPEN_MANAGED",
            "strategy_trade_intent_created": True,
            "strategy_trade_intent_classification": "STRATEGY_TRADE_INTENT_CREATED",
            "strategy_trade_intent_id": "intent-001",
            "strategy_trade_intent_report_path": "outputs/track_b_execution_core/strategy_trade_intents/latest_track_b_strategy_trade_intent.json",
            "intent_blocked_reason": None,
            "lifecycle_mode": "STRATEGY_MANAGED",
            "paper_proof_invoked": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
            "report_json_path": "track_b_strategy_paper_runner_report.json",
        },
    )

    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_strategy_paper_runner_report_json=runner_path,
            track_b_paper_trade_summary_json=None,
            track_b_live_position_status_json=None,
            track_b_pnl_summary_json=None,
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-intent-runner",
        now=aware_now(),
    )

    assert result.report["strategy_trade_intent_created"] is True
    assert result.report["strategy_trade_intent_classification"] == "STRATEGY_TRADE_INTENT_CREATED"
    assert result.report["strategy_trade_intent_id"] == "intent-001"
    assert result.report["strategy_trade_intent_lifecycle_mode"] == "STRATEGY_MANAGED"
    assert result.report["strategy_paper_proof_invoked"] is False


def test_operator_status_exposes_strategy_trade_intent_from_shadow_monitor(tmp_path: Path) -> None:
    monitor_path = write_json(
        tmp_path / "monitor" / "latest_track_b_shadow_monitor_report.json",
        {
            "schema_version": "track_b_shadow_monitor_v2",
            "monitor_verdict": "TRACK_B_SHADOW_MONITOR_OK_NO_SIGNAL",
            "monitor_mode": "PAPER",
            "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
            "latest_strategy_trade_intent_created": False,
            "latest_strategy_trade_intent_classification": "INTENT_BLOCKED_MISSING_EXIT_POLICY",
            "latest_strategy_trade_intent_path": "outputs/track_b_execution_core/strategy_trade_intents/latest_track_b_strategy_trade_intent.json",
            "latest_strategy_trade_intent_blocked_reason": "missing managed exit policy",
            "paper_trades_attempted_count": 0,
            "candidate_signals": [],
            "suppressed_signals": [],
            "arbitration_result": {},
            "decision_journal_tier_counts": {},
            "submit_allowed": False,
            "submit_attempted": False,
            "paper_proof_invoked": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
            "report_json_path": "track_b_shadow_monitor_report.json",
        },
    )

    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_shadow_monitor_report_json=monitor_path,
            track_b_paper_trade_summary_json=None,
            track_b_live_position_status_json=None,
            track_b_pnl_summary_json=None,
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-intent-monitor",
        now=aware_now(),
    )

    assert result.report["shadow_monitor_strategy_trade_intent_created"] is False
    assert result.report["shadow_monitor_strategy_trade_intent_classification"] == "INTENT_BLOCKED_MISSING_EXIT_POLICY"
    assert result.report["shadow_monitor_strategy_trade_intent_blocked_reason"] == "missing managed exit policy"
    assert result.report["shadow_monitor_paper_proof_invoked"] is False
