from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.operator_status import OperatorStatusInputs, create_operator_status_summary
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    update_track_b_paper_trade_ledger_from_runner_report,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 22, 30, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def runner_report(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    proof_path = write_json(
        tmp_path / "proof" / "paper_proof_report.json",
        {
            "classification": "TRACK_B_PAPER_PROOF_PASSED",
            "account_id": "DUM882026",
            "contract_key": "MNQ-202606",
            "proof_payload": {
                "run_id": "paper-proof-mnq-001",
                "proof_lifecycle_status": "PROOF_COMPLETE_FLAT",
                "open_intent": {
                    "account_id": "DUM882026",
                    "action": "SELL",
                    "contract_key": "MNQ-202606",
                    "created_at": "2026-05-05T22:00:00+00:00",
                    "limit_price": "18800.00",
                    "quantity": "1",
                    "signal_event_id": "signal-001",
                },
                "open_submit_attempt": {"broker_order_id": "101", "submitted_at": "2026-05-05T22:00:01+00:00"},
                "open_fill": {
                    "broker_order_id": "101",
                    "filled_at": "2026-05-05T22:00:02+00:00",
                    "price": "18799.50",
                    "quantity": "1",
                },
                "close_intent": {
                    "action": "BUY",
                    "created_at": "2026-05-05T22:01:00+00:00",
                    "limit_price": "18795.00",
                    "quantity": "1",
                },
                "close_submit_attempt": {"broker_order_id": "102", "submitted_at": "2026-05-05T22:01:01+00:00"},
                "close_fill": {
                    "broker_order_id": "102",
                    "filled_at": "2026-05-05T22:01:02+00:00",
                    "price": "18795.25",
                    "quantity": "1",
                },
                "final_reconciliation": {"status": "CLEAN"},
            },
        },
    )
    payload: dict[str, object] = {
        "strategy_id": "MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        "signal_direction": "SHORT",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": 1,
        "paper_proof_invoked": True,
        "paper_proof_classification": "TRACK_B_PAPER_PROOF_PASSED",
        "paper_proof_lifecycle_status": "PROOF_COMPLETE_FLAT",
        "paper_proof_report_path": str(proof_path),
        "final_position_status": "CLEAN",
        "strategy_paper_runner_verdict": "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED",
    }
    report_path = write_json(tmp_path / "runner" / "track_b_strategy_paper_runner_report.json", payload)
    return report_path, payload


def test_trade_ledger_writes_compact_pnl_and_summaries(tmp_path: Path) -> None:
    report_path, payload = runner_report(tmp_path)

    result = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    assert result.trade_record_written is True
    row = json.loads(result.ledger_jsonl.read_text(encoding="utf-8").strip())
    assert row["strategy_id"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert row["side"] == "SHORT"
    assert row["entry_fill_price"] == "18799.5"
    assert row["exit_fill_price"] == "18795.25"
    assert row["points_pnl"] == "4.25"
    assert row["ticks_pnl"] == "17"
    assert row["realized_pnl"] == "8.5"
    assert row["review_required"] is False

    pnl = json.loads(result.pnl_summary_json.read_text(encoding="utf-8"))
    assert pnl["total_realized_pnl_today"] == "8.5"
    assert pnl["total_realized_pnl_session"] == "8.5"
    assert pnl["total_realized_pnl_month"] == "8.5"
    assert pnl["total_realized_pnl_ytd"] == "8.5"
    assert pnl["trades_today"] == 1
    assert pnl["trades_session"] == 1
    assert pnl["trades_month"] == 1
    assert pnl["trades_ytd"] == 1
    summary = json.loads(result.trade_summary_json.read_text(encoding="utf-8"))
    assert summary["completed_trade_count"] == 1
    assert summary["recent_trades"][0]["strategy_id"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert summary["recent_trades"][0]["realized_pnl"] == "8.5"
    assert pnl["by_strategy"]["MNQ_US_DERIVATIVE_BEAR_TURN_V1"]["realized_pnl"] == "8.5"
    assert pnl["by_strategy"]["MNQ_US_DERIVATIVE_BEAR_TURN_V1"]["realized_pnl_ytd"] == "8.5"
    status = json.loads(result.live_position_status_json.read_text(encoding="utf-8"))
    assert status["source"] == "TRACK_B_LIFECYCLE_ARTIFACTS"
    assert status["broker_reconciled"] is False
    assert status["open_position_count"] == 0
    assert status["total_unrealized_pnl"] == "0"


def test_trade_ledger_update_is_idempotent(tmp_path: Path) -> None:
    report_path, payload = runner_report(tmp_path)

    first = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )
    second = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    assert first.trade_record_written is True
    assert second.trade_record_written is False
    assert len(second.ledger_jsonl.read_text(encoding="utf-8").splitlines()) == 1


def test_no_paper_lifecycle_writes_zero_summaries_only(tmp_path: Path) -> None:
    result = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report={"paper_proof_invoked": False},
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    assert result.trade_record_written is False
    summary = json.loads(result.trade_summary_json.read_text(encoding="utf-8"))
    assert summary["trade_count"] == 0
    assert summary["open_position_count"] == 0
    assert summary["paper_trades_attempted_count"] == 0


def test_strategy_managed_lifecycle_trade_is_separated_from_proof(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-001",
            "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "account_id": "DUM882026",
            "managed_exit_policy_id": "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            "entry_intent": {
                "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "LONG",
                "order_action": "BUY",
                "quantity": 1,
                "entry_limit_price": "4704.6",
                "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            },
            "entry_submit_attempt": {"broker_order_id": "201", "submitted_at": "2026-05-05T22:00:01+00:00"},
            "entry_fill": {"broker_order_id": "201", "filled_at": "2026-05-05T22:00:02+00:00", "price": "4704.6", "quantity": 1},
            "close_intent": {"order_action": "SELL", "quantity": 1, "close_limit_price": "4705.1"},
            "close_submit_attempt": {"broker_order_id": "202", "submitted_at": "2026-05-05T22:05:01+00:00"},
            "close_fill": {"broker_order_id": "202", "filled_at": "2026-05-05T22:05:02+00:00", "price": "4705.1", "quantity": 1},
            "final_position_status": "CLOSED_FLAT",
            "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
            "review_required": False,
            "broker_reconciled": False,
        },
    )
    runner = {
        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": 1,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "strategy_paper_runner_verdict": "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_CLOSED_FLAT",
        "paper_proof_invoked": False,
    }

    result = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    assert result.trade_record_written is True
    row = json.loads(result.ledger_jsonl.read_text(encoding="utf-8").strip())
    assert row["paper_lifecycle_type"] == "STRATEGY_MANAGED"
    assert row["paper_proof_classification"] is None
    assert row["paper_lifecycle_classification"] == "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"
    assert row["realized_pnl"] == "5"
    summary = json.loads(result.trade_summary_json.read_text(encoding="utf-8"))
    assert summary["completed_trade_count"] == 1


def test_operator_status_exposes_compact_paper_results(tmp_path: Path) -> None:
    report_path, payload = runner_report(tmp_path)
    ledger = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_paper_trade_summary_json=ledger.trade_summary_json,
            track_b_live_position_status_json=ledger.live_position_status_json,
            track_b_pnl_summary_json=ledger.pnl_summary_json,
            output_root=tmp_path / "operator_status",
        ),
        status_id="paper-results-status",
        now=aware_now(),
    )

    assert result.report["track_b_paper_results_source"] == "TRACK_B_LIFECYCLE_ARTIFACTS"
    assert result.report["track_b_paper_results_broker_reconciled"] is False
    assert result.report["paper_trades_attempted_count"] == 1
    assert result.report["open_position_count"] == 0
    assert result.report["realized_pnl_today"] == "8.5"
    assert result.report["realized_pnl_week"] == "8.5"
    assert result.report["realized_pnl_month"] == "8.5"
    assert result.report["realized_pnl_ytd"] == "8.5"
    assert result.report["unrealized_pnl"] == "0"
    assert result.report["completed_trade_count"] == 1
    assert result.report["track_b_recent_trades"][0]["realized_pnl"] == "8.5"
    assert result.report["last_trade_strategy"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert result.report["last_trade_pnl"] == "8.5"
    assert result.report["review_required_count"] == 0
    assert result.report["latest_trade_ledger_path"] == str(ledger.ledger_jsonl)
    assert result.report["latest_live_position_status_path"] == str(ledger.live_position_status_json)
    assert result.report["latest_pnl_summary_path"] == str(ledger.pnl_summary_json)
