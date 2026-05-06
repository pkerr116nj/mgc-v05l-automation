from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_same_day_postmortem import build_track_b_same_day_postmortem


NOW = datetime(2026, 5, 6, 20, 30, tzinfo=UTC)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def write_runtime_report(
    tmp_path: Path,
    *,
    generated_at: str = "2026-05-06T20:26:00+00:00",
    instrument: str = "MNQ",
    strategy_id: str = "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    decision: str = "SIGNAL",
) -> None:
    write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_multi_strategy_runtime_cycle"
        / "cycle"
        / "track_b_multi_strategy_runtime_cycle_report.json",
        {
            "generated_at": generated_at,
            "candidate_signals": [{"strategy_id": strategy_id, "signal_source": strategy_id}],
            "chosen_strategy_id": strategy_id if decision == "SIGNAL" else None,
            "paper_runner_report_path": "outputs/track_b_execution_core/track_b_strategy_paper_runner/runner/report.json"
            if decision == "SIGNAL"
            else None,
            "evaluated_strategies": [
                {
                    "strategy_id": strategy_id,
                    "signal_source": strategy_id,
                    "decision": decision,
                    "signal_emitted": decision == "SIGNAL",
                    "decision_reason": f"{strategy_id} explicit feature/state snapshot is entry-ready for SHORT."
                    if decision == "SIGNAL"
                    else "no signal",
                    "rule_conditions": {
                        "bear_snap_body_ok": True,
                        "bear_snap_close_weak": True,
                        "bear_snap_turn_candidate": decision == "SIGNAL",
                    },
                    "registry_metadata": {
                        "strategy_registry_instrument_family": instrument,
                        "strategy_registry_paper_eligible": True,
                        "strategy_registry_live_money_eligible": False,
                    },
                    "report_json_path": "outputs/rule.json",
                }
            ],
            "paper_proof_invoked": decision == "SIGNAL",
            "submit_attempted": decision == "SIGNAL",
            "broker_state_mutated": decision == "SIGNAL",
            "live_money_readiness": False,
        },
    )


def write_monitor_report(tmp_path: Path) -> None:
    write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_shadow_monitor"
        / "cycle"
        / "track_b_shadow_monitor_report.json",
        {
            "completed_at": "2026-05-06T20:26:00+00:00",
            "mode": "PAPER",
            "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
            "instrument_reports": [
                {
                    "instrument_family": "MNQ",
                    "enabled_strategies": [
                        "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
                        "MNQ_FIRST_BEAR_SNAP_TURN_V1",
                    ],
                    "feature_context_ready": True,
                    "live_execution_approved": True,
                    "paper_evaluation_allowed": True,
                    "latest_completed_5m_timestamp": "2026-05-06T20:25:00+00:00",
                }
            ],
        },
    )


def write_trade_artifacts(tmp_path: Path) -> None:
    proof_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "paper_proof"
        / "paper_proof_test"
        / "paper_proof_report.json"
    )
    write_json(
        proof_path,
        {
            "classification": "TRACK_B_PAPER_PROOF_PASSED",
            "proof_payload": {
                "proof_lifecycle_status": "PROOF_COMPLETE_FLAT",
                "open_fill": {"broker_order_id": "5", "price": "28774.5"},
                "close_fill": {"broker_order_id": "6", "price": "28775"},
                "open_intent": {"reason": "ibkr paper proof open"},
                "close_intent": {"reason": "ibkr paper proof close"},
                "final_reconciliation": {"status": "CLEAN"},
            },
        },
    )
    recent_trade = {
        "trade_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1:paper_proof_test",
        "lifecycle_id": "paper_proof_test",
        "signal_id": "signal_test",
        "strategy_id": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "SHORT",
        "order_action": "SELL",
        "quantity": "1",
        "entry_timestamp": "2026-05-06T20:26:03+00:00",
        "entry_fill_price": "28774.5",
        "exit_timestamp": "2026-05-06T20:26:03.100000+00:00",
        "exit_fill_price": "28775",
        "realized_pnl": "-1",
        "paper_lifecycle_classification": "PROOF_COMPLETE_FLAT",
        "final_position_status": "CLEAN",
        "final_broker_state_classification": "TRACK_B_PAPER_PROOF_PASSED",
        "review_required": False,
        "paper_lifecycle_report_path": str(proof_path.relative_to(tmp_path)),
    }
    ledger_root = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    write_json(
        ledger_root / "latest_track_b_paper_trade_summary.json",
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "paper_trades_attempted_count": 1,
            "completed_trade_count": 1,
            "open_position_count": 0,
            "last_trade_strategy": "MNQ_FIRST_BEAR_SNAP_TURN_V1",
            "last_trade_pnl": "-1",
            "review_required_count": 0,
            "recent_trades": [recent_trade],
        },
    )
    write_json(
        ledger_root / "latest_track_b_pnl_summary.json",
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "total_realized_pnl_today": "-1",
            "total_unrealized_pnl": "0",
        },
    )
    write_json(
        ledger_root / "latest_track_b_live_position_status.json",
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "open_position_count": 0,
            "open_order_count": 0,
            "total_unrealized_pnl": "0",
        },
    )


def write_candles(tmp_path: Path, instrument: str = "MNQ") -> None:
    candles = []
    for minute, price in enumerate([100, 102, 104, 106, 108, 110, 116, 120, 124, 130]):
        candles.append(
            {
                "candle_timestamp": f"2026-05-06T20:{minute:02d}:00+00:00",
                "open": str(price),
                "high": str(price + 1),
                "low": str(price - 1),
                "close": str(price + 1),
                "volume": "10",
            }
        )
    write_json(
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_runtime_candle_capture"
        / "context"
        / f"runtime_{instrument.lower()}_1m_candles.json",
        {"candle_history": candles},
    )


def test_postmortem_reconstructs_immediate_paper_proof_trade(tmp_path: Path) -> None:
    write_runtime_report(tmp_path)
    write_monitor_report(tmp_path)
    write_trade_artifacts(tmp_path)
    write_candles(tmp_path)

    result = build_track_b_same_day_postmortem(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    trade = result.report["trade_lifecycle_reconstruction"][0]
    assert trade["strategy"] == "MNQ_FIRST_BEAR_SNAP_TURN_V1"
    assert trade["exit_reason"] == "lifecycle_guardrail"
    assert trade["closed_by_normal_strategy_logic"] is False
    assert trade["closed_by_guarded_paper_proof_lifecycle"] is True
    assert trade["hold_duration_seconds"] == 0.1
    assert result.report["classifications"]
    assert "TRADE_LIFECYCLE_TOO_SHORT_OR_OVER_FLATTENED" in result.report["classifications"]


def test_postmortem_reports_trend_coverage_gap_and_baseline(tmp_path: Path) -> None:
    write_runtime_report(tmp_path, decision="NO_SIGNAL")
    write_monitor_report(tmp_path)
    write_trade_artifacts(tmp_path)
    write_candles(tmp_path)

    result = build_track_b_same_day_postmortem(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    review = result.report["missed_rally_window_review"]["MNQ"]
    assert review["long_side_trend_continuation_coverage_exists"] is False
    assert review["dominant_blocker"] == "COVERAGE_GAP"
    baseline = result.report["simple_baseline_comparison"]["by_day"]["today"]["MNQ"]
    assert baseline["buy_at_session_start_exit_latest"]["available"] is True
    assert result.report["simple_baseline_comparison"]["source_tag"] == "DIAGNOSTIC_BASELINE_ONLY_NOT_TRADING_INSTRUCTIONS"


def test_postmortem_is_read_only_and_writes_json_and_markdown(tmp_path: Path) -> None:
    write_runtime_report(tmp_path)
    write_monitor_report(tmp_path)
    write_trade_artifacts(tmp_path)
    write_candles(tmp_path)

    result = build_track_b_same_day_postmortem(
        repo_root=tmp_path,
        output_root=tmp_path / "outputs" / "track_b_execution_core" / "diagnostics",
        now=NOW,
    )

    assert result.report["broker_commands_invoked"] is False
    assert result.report["paper_proof_cli_invoked_by_postmortem"] is False
    assert result.report["manual_submit_cancel_place_order_invoked"] is False
    assert result.report_json.exists()
    assert result.report_markdown.exists()
    assert "not meaningful participation" in result.markdown
