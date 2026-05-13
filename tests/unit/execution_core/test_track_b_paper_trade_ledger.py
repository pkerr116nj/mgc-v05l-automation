from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.operator_status import OperatorStatusInputs, create_operator_status_summary
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    reconcile_app_only_unfilled_managed_lifecycles,
    reconcile_ibkr_contract_rejected_managed_lifecycles,
    reconcile_manually_flattened_proof_lifecycle,
    update_track_b_paper_trade_ledger_from_filled_bridge_result,
    update_track_b_paper_trade_ledger_from_runner_report,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 22, 30, tzinfo=timezone.utc)


def test_updates_open_position_from_direct_bridge_fill_artifact(tmp_path: Path) -> None:
    artifact_path = tmp_path / "filled_bridge_result_latest.json"
    payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
        "lane_id": "mnq_1x_ny_early_core__us_late_long",
        "instrument": "MNQ",
        "symbol": "MNQ",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-12T17:34:00+00:00",
        "broker_order_id": "1",
        "account_id": "DUM882026",
        "perm_id": 1984099439,
        "client_id": 10905,
        "exec_id": "0000e1a7.6a05a265.01.01",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "expiry": "202606", "multiplier": "2"},
        "fill_price": "28981.25",
        "fill_timestamp": "2026-05-12T19:05:26.191844+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }
    write_json(artifact_path, payload)

    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=payload,
        filled_bridge_result_json=artifact_path,
        output_root=tmp_path / "ledger",
        now=aware_now(),
    )

    assert result.trade_record_written is True
    assert result.trade_record is not None
    assert result.trade_record["source"] == "TRACK_B_DIRECT_BRIDGE_FILL_ARTIFACT"
    assert result.trade_record["paper_lifecycle_type"] == "STRATEGY_MANAGED"
    assert result.live_position_status["open_position_count"] == 1
    position = result.live_position_status["positions_by_instrument"]["MNQ-202606"]
    assert position["strategy_id"] == payload["strategy_id"]
    assert position["side"] == "LONG"
    assert position["quantity"] == "1"
    assert position["local_symbol"] == "MNQM6"
    assert position["con_id"] == 770561201
    assert position["entry_perm_id"] == 1984099439
    assert position["entry_client_id"] == 10905
    assert position["entry_broker_identity"]["broker_order_id"] == "1"


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


def write_clean_mgc_preflight(tmp_path: Path, *, signed_quantity: int = 0, open_orders: list[dict[str, object]] | None = None) -> Path:
    return write_json(
        tmp_path / "preflight" / "preflight_report.json",
        {
            "classification": "READY_READ_ONLY",
            "account_id": "DUM882026",
            "contract_key": "MGC-202606",
            "safety": {"submit_attempted": False},
            "contract": {"con_id": 712565978, "contract_key": "MGC-202606", "local_symbol": "MGCM6"},
            "position": {
                "account_id": "DUM882026",
                "contract_key": "MGC-202606",
                "signed_quantity": signed_quantity,
                "raw": {
                    "rows": [
                        {
                            "account_id": "DUM882026",
                            "con_id": 712565978,
                            "local_symbol": "MGCM6",
                            "signed_quantity": signed_quantity,
                        }
                    ]
                },
            },
            "open_orders": [] if open_orders is None else open_orders,
            "checks": [
                {"name": "proof_position_flat", "passed": signed_quantity == 0},
                {"name": "proof_open_orders_clean", "passed": not open_orders},
            ],
        },
    )


def write_clean_recovery(tmp_path: Path) -> Path:
    return write_json(
        tmp_path / "recovery" / "recovery_status_report.json",
        {
            "classification": "RECOVERY_READY_CLEAN",
            "account_id": "DUM882026",
            "contract_key": "MGC-202606",
            "submit_attempted": False,
            "primary_blocker": None,
        },
    )


def stale_mgc_proof_runner_report(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    proof_path = write_json(
        tmp_path / "proof" / "paper_proof_report.json",
        {
            "classification": "TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED",
            "account_id": "DUM882026",
            "contract_key": "MGC-202606",
            "proof_payload": {
                "run_id": "paper_proof_f9d713f6cf94415c93663b9085e05b06",
                "proof_lifecycle_status": "OPEN_FILLED",
                "open_intent": {
                    "account_id": "DUM882026",
                    "action": "BUY",
                    "contract_key": "MGC-202606",
                    "created_at": "2026-05-06T23:11:11+00:00",
                    "limit_price": "4705.5",
                    "quantity": "1",
                },
                "open_fill": {
                    "broker_order_id": "7",
                    "filled_at": "2026-05-06T23:11:12+00:00",
                    "price": "4704.6",
                    "quantity": "1",
                },
            },
        },
    )
    payload: dict[str, object] = {
        "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "signal_direction": "LONG",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": 1,
        "paper_proof_invoked": True,
        "paper_proof_classification": "TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED",
        "paper_proof_lifecycle_status": "OPEN_FILLED",
        "paper_proof_report_path": str(proof_path),
        "strategy_paper_runner_verdict": "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED",
    }
    report_path = write_json(tmp_path / "runner" / "stale_mgc_runner_report.json", payload)
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


def test_strategy_managed_lifecycle_update_appends_close_for_same_trade_id(tmp_path: Path) -> None:
    lifecycle_path = tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json"
    open_payload = {
        "lifecycle_id": "managed-update-001",
        "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "account_id": "DUM882026",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "entry_intent": {
            "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "side": "LONG",
            "order_action": "BUY",
            "quantity": 1,
            "entry_limit_price": "28729",
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
        },
        "entry_submit_attempt": {"broker_order_id": "11", "submitted_at": "2026-05-07T16:26:05+00:00"},
        "entry_fill": {"broker_order_id": "11", "filled_at": "2026-05-07T16:26:07+00:00", "price": "28729", "quantity": 1},
        "final_position_status": "OPEN_MANAGED",
        "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "review_required": False,
    }
    write_json(lifecycle_path, open_payload)
    runner = {
        "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": 1,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "paper_proof_invoked": False,
    }

    first = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=datetime(2026, 5, 7, 16, 26, tzinfo=timezone.utc),
    )
    closed_payload = {
        **open_payload,
        "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
        "close_intent": {"order_action": "SELL", "quantity": 1, "close_limit_price": "28668.75"},
        "close_submit_attempt": {"broker_order_id": "12", "submitted_at": "2026-05-07T18:11:45+00:00"},
        "close_fill": {"broker_order_id": "12", "filled_at": "2026-05-07T18:11:45+00:00", "price": "28682.5", "quantity": 1},
        "final_position_status": "CLOSED_FLAT",
        "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT",
        "review_required": False,
    }
    write_json(lifecycle_path, closed_payload)
    runner = {**runner, "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"}

    second = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=datetime(2026, 5, 7, 18, 11, tzinfo=timezone.utc),
    )

    assert first.trade_record_written is True
    assert second.trade_record_written is True
    assert len(second.ledger_jsonl.read_text(encoding="utf-8").splitlines()) == 2
    assert second.trade_summary["open_position_count"] == 0
    assert second.trade_summary["completed_trade_count"] == 1
    assert second.pnl_summary["total_realized_pnl_today"] == "-93"
    assert second.live_position_status["open_position_count"] == 0


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
    assert summary["managed_strategy_trade_count"] == 1
    assert summary["meaningful_strategy_trade_count"] == 1


def test_strategy_managed_lifecycle_without_submit_does_not_create_open_position(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-no-submit-001",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "account_id": "DUM882026",
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "entry_intent": {
                "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "SHORT",
                "order_action": "SELL",
                "quantity": 1,
                "entry_limit_price": "4756.6",
                "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
                "created_at": "2026-05-07T12:56:05+00:00",
            },
            "entry_submit_attempt": None,
            "entry_fill": None,
            "close_intent": None,
            "close_submit_attempt": None,
            "close_fill": None,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "final_position_status": "REVIEW_REQUIRED",
            "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "review_required": True,
            "broker_reconciled": False,
        },
    )
    runner = {
        "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": 1,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "strategy_paper_runner_verdict": "TRACK_B_STRATEGY_PAPER_RUNNER_REVIEW_REQUIRED",
        "paper_proof_invoked": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
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
    assert row["entry_submit_attempted"] is False
    assert row["entry_fill_confirmed"] is False
    assert row["broker_backed_position_confirmed"] is False
    assert row["app_only_no_broker_transmission"] is True
    assert row["transmission_classification"] == "LIFECYCLE_CREATED_NO_SUBMIT"
    assert row["entry_timestamp"] is None

    summary = json.loads(result.trade_summary_json.read_text(encoding="utf-8"))
    assert summary["trade_count"] == 1
    assert summary["broker_backed_trade_count"] == 0
    assert summary["managed_strategy_trade_count"] == 0
    assert summary["meaningful_strategy_trade_count"] == 0
    assert summary["paper_trades_attempted_count"] == 0
    assert summary["open_position_count"] == 0
    assert summary["review_required_count"] == 1
    assert summary["app_only_position_from_unfilled_entry_count"] == 1
    assert summary["recent_trades"][0]["broker_backed_position_confirmed"] is False
    assert summary["recent_trades"][0]["app_only_no_broker_transmission"] is True

    status = json.loads(result.live_position_status_json.read_text(encoding="utf-8"))
    assert status["open_position_count"] == 0
    assert status["positions_by_instrument"] == {}
    assert status["review_required_positions"] == []

    pnl = json.loads(result.pnl_summary_json.read_text(encoding="utf-8"))
    assert pnl["trades_today"] == 0
    assert pnl["total_realized_pnl_today"] == "0"
    assert pnl["by_strategy"] == {}


def test_strategy_managed_lifecycle_submit_attempt_without_fill_does_not_create_open_position(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-submit-no-fill-001",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "account_id": "DUM882026",
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "entry_intent": {
                "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "SHORT",
                "order_action": "SELL",
                "quantity": 1,
                "entry_limit_price": "4756.6",
                "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            },
            "entry_submit_attempt": {
                "broker_order_id": "301",
                "submitted_at": "2026-05-07T12:56:06+00:00",
            },
            "entry_fill": None,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "final_position_status": "REVIEW_REQUIRED",
            "review_required": True,
            "broker_reconciled": False,
        },
    )
    runner = {
        "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": 1,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "paper_proof_invoked": False,
    }

    result = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    row = json.loads(result.ledger_jsonl.read_text(encoding="utf-8").strip())
    assert row["entry_submit_attempted"] is True
    assert row["entry_order_id"] == "301"
    assert row["entry_fill_confirmed"] is False
    assert row["transmission_classification"] == "FILL_MISSING"
    assert result.trade_summary["open_position_count"] == 0
    assert result.trade_summary["managed_strategy_trade_count"] == 0
    assert result.live_position_status["open_position_count"] == 0


def test_app_only_unfilled_managed_lifecycle_archives_review_without_trade_counts(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-app-only-001",
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "account_id": "DUM882026",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "entry_intent": {
                "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "LONG",
                "order_action": "BUY",
                "quantity": 1,
                "entry_limit_price": "4750",
            },
            "entry_submit_attempt": None,
            "entry_fill": None,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "final_position_status": "REVIEW_REQUIRED",
            "review_required": True,
            "broker_reconciled": False,
        },
    )
    runner = {
        "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": 1,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "paper_proof_invoked": False,
    }
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )
    assert initial.trade_summary["review_required_count"] == 1
    assert initial.trade_summary["open_position_count"] == 0

    result = reconcile_app_only_unfilled_managed_lifecycles(
        lifecycle_ids=["managed-app-only-001"],
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is True
    assert result.reconciliation_report["reconciliation_record_count"] == 1
    assert result.reconciliation_report["targets"][0]["app_only_unfilled_evidence"]["app_only_unfilled_confirmed"] is True
    assert result.trade_summary["open_position_count"] == 0
    assert result.trade_summary["review_required_count"] == 0
    assert result.trade_summary["managed_strategy_trade_count"] == 0
    assert result.trade_summary["meaningful_strategy_trade_count"] == 0
    assert result.trade_summary["app_only_position_from_unfilled_entry_count"] == 0
    assert result.trade_summary["recent_trades"][0]["artifact_reconciliation_classification"] == "APP_ONLY_UNFILLED_REVIEWED"
    assert result.live_position_status["open_position_count"] == 0
    assert result.live_position_status["review_required_positions"] == []
    assert result.pnl_summary["trades_today"] == 0


def test_app_only_unfilled_reconciliation_does_not_archive_submit_attempt(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-submit-attempt-001",
            "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "entry_intent": {"strategy_id": "FIRST_BULL_SNAP_TURN_V1"},
            "entry_submit_attempt": {"broker_order_id": "123"},
            "entry_fill": None,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "review_required": True,
        },
    )
    runner = {
        "strategy_id": "FIRST_BULL_SNAP_TURN_V1",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "paper_proof_invoked": False,
    }
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    result = reconcile_app_only_unfilled_managed_lifecycles(
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is False
    assert result.reconciliation_report["targets"][0]["remaining_blocker"] == "ENTRY_ORDER_ID_PRESENT"
    assert result.trade_summary["review_required_count"] == 1


def test_ibkr_contract_rejected_lifecycle_archives_review_without_trade_counts(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-contract-reject-001",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "con_id": 712565978,
            "account_id": "DUM882026",
            "strategy_managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
            "entry_intent": {
                "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "side": "SHORT",
                "order_action": "SELL",
                "quantity": 1,
                "entry_limit_price": "4768.2",
            },
            "entry_submit_attempt": {
                "broker_order_id": "10",
                "submit_attempted": True,
                "broker_state_mutated": True,
                "primary_blocker": "IBKR_CONTRACT_REJECTED: Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
                "submit_diagnostics": {
                    "place_order_called": True,
                    "order_transmit_flag": True,
                    "broker_order_id_allocated": "10",
                    "openOrder_seen": False,
                    "orderStatus_seen": False,
                    "execDetails_seen": False,
                    "contract_fields_submitted_to_ibkr": {"lastTradeDateOrContractMonth": "202606"},
                    "canonical_broker_contract_fields": {"lastTradeDateOrContractMonth": "20260626"},
                    "error_callbacks_after_submit": [
                        {
                            "error_code": 478,
                            "error_string": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
                        }
                    ],
                },
            },
            "entry_fill": None,
            "submit_attempted": True,
            "broker_state_mutated": True,
            "final_position_status": "REVIEW_REQUIRED",
            "review_required": True,
            "broker_reconciled": False,
            "primary_blocker": "IBKR_CONTRACT_REJECTED: Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
        },
    )
    runner = {
        "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        "mode": "PAPER",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "quantity": 1,
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "paper_proof_invoked": False,
    }
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )
    assert initial.trade_summary["review_required_count"] == 1
    assert initial.trade_summary["open_position_count"] == 0
    assert initial.trade_summary["managed_strategy_trade_count"] == 0

    result = reconcile_ibkr_contract_rejected_managed_lifecycles(
        lifecycle_ids=["managed-contract-reject-001"],
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is True
    assert result.reconciliation_report["reconciliation_record_count"] == 1
    evidence = result.reconciliation_report["targets"][0]["ibkr_contract_rejection_evidence"]
    assert evidence["ibkr_contract_rejection_confirmed"] is True
    assert evidence["ibkr_error_code"] == 478
    assert result.trade_summary["open_position_count"] == 0
    assert result.trade_summary["review_required_count"] == 0
    assert result.trade_summary["managed_strategy_trade_count"] == 0
    assert result.trade_summary["meaningful_strategy_trade_count"] == 0
    assert result.trade_summary["app_only_position_from_unfilled_entry_count"] == 0
    assert result.trade_summary["recent_trades"][0]["artifact_reconciliation_classification"] == "IBKR_CONTRACT_REJECTED_REVIEWED"
    assert result.live_position_status["open_position_count"] == 0
    assert result.live_position_status["review_required_positions"] == []
    assert result.pnl_summary["trades_today"] == 0


def test_ibkr_contract_rejected_reconciliation_does_not_archive_fill(tmp_path: Path) -> None:
    lifecycle_path = write_json(
        tmp_path / "managed" / "track_b_strategy_managed_paper_lifecycle_report.json",
        {
            "lifecycle_id": "managed-contract-reject-fill-001",
            "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
            "instrument_family": "MGC",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "entry_submit_attempt": {
                "broker_order_id": "10",
                "submit_diagnostics": {
                    "place_order_called": True,
                    "error_callbacks_after_submit": [
                        {
                            "error_code": 478,
                            "error_string": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
                        }
                    ],
                },
            },
            "entry_fill": {"price": "4768.2", "quantity": 1},
            "submit_attempted": True,
            "broker_state_mutated": True,
            "review_required": True,
        },
    )
    runner = {
        "strategy_id": "FIRST_BEAR_SNAP_TURN_V1",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "managed_lifecycle_invoked": True,
        "managed_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "managed_lifecycle_report_path": str(lifecycle_path),
        "paper_proof_invoked": False,
    }
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=runner,
        runner_report_json=tmp_path / "runner.json",
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    result = reconcile_ibkr_contract_rejected_managed_lifecycles(
        lifecycle_ids=["managed-contract-reject-fill-001"],
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is False
    assert result.reconciliation_report["target_count"] == 0
    assert result.reconciliation_report["reconciliation_record_count"] == 0


def test_stale_proof_lifecycle_broker_flat_archives_manual_review_and_clears_compact_open_position(tmp_path: Path) -> None:
    report_path, payload = stale_mgc_proof_runner_report(tmp_path)
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )
    assert initial.trade_summary["open_position_count"] == 1
    assert initial.trade_summary["review_required_count"] == 1

    result = reconcile_manually_flattened_proof_lifecycle(
        lifecycle_id="paper_proof_f9d713f6cf94415c93663b9085e05b06",
        preflight_report_json=write_clean_mgc_preflight(tmp_path),
        recovery_report_json=write_clean_recovery(tmp_path),
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        expected_account_id="DUM882026",
        expected_contract_key="MGC-202606",
        expected_local_symbol="MGCM6",
        expected_con_id=712565978,
        now=aware_now(),
    )

    assert result.reconciliation_record_written is True
    assert result.reconciliation_report["reconciliation_action"] == "MANUALLY_FLATTENED_REVIEWED"
    assert result.reconciliation_report["broker_flat_confirmation"]["broker_flat_confirmed"] is True
    assert result.trade_summary["open_position_count"] == 0
    assert result.trade_summary["review_required_count"] == 0
    assert result.trade_summary["proof_canary_excluded_from_meaningful_strategy_counts"] is True
    assert result.trade_summary["managed_strategy_trade_count"] == 0
    assert result.trade_summary["meaningful_strategy_trade_count"] == 0
    assert result.trade_summary["proof_canary_trade_count"] == 1
    assert result.trade_summary["archived_manual_flat_count"] == 1
    assert result.trade_summary["recent_trades"][0]["paper_lifecycle_classification"] == "MANUALLY_FLATTENED_REVIEWED"
    assert result.trade_summary["recent_trades"][0]["artifact_reconciliation_classification"] == "MANUALLY_FLATTENED_REVIEWED"
    assert result.live_position_status["open_position_count"] == 0
    assert result.live_position_status["positions_by_instrument"] == {}
    report = json.loads(result.reconciliation_report_json.read_text(encoding="utf-8"))
    assert report["broker_mutation_attempted"] is False
    assert report["paper_proof_cli_invoked"] is False


def test_stale_proof_lifecycle_broker_non_flat_remains_review_required(tmp_path: Path) -> None:
    report_path, payload = stale_mgc_proof_runner_report(tmp_path)
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    result = reconcile_manually_flattened_proof_lifecycle(
        lifecycle_id="paper_proof_f9d713f6cf94415c93663b9085e05b06",
        preflight_report_json=write_clean_mgc_preflight(tmp_path, signed_quantity=1),
        recovery_report_json=write_clean_recovery(tmp_path),
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is False
    assert result.reconciliation_report["reconciliation_action"] == "NO_ARCHIVE_REVIEW_REQUIRED"
    assert result.reconciliation_report["remaining_blocker"] == "BROKER_FLAT_CONFIRMATION_FAILED"
    assert result.trade_summary["open_position_count"] == 1
    assert result.trade_summary["review_required_count"] == 1


def test_stale_proof_lifecycle_open_orders_remain_review_required(tmp_path: Path) -> None:
    report_path, payload = stale_mgc_proof_runner_report(tmp_path)
    initial = update_track_b_paper_trade_ledger_from_runner_report(
        runner_report=payload,
        runner_report_json=report_path,
        output_root=tmp_path / "paper_trade_ledger",
        now=aware_now(),
    )

    result = reconcile_manually_flattened_proof_lifecycle(
        lifecycle_id="paper_proof_f9d713f6cf94415c93663b9085e05b06",
        preflight_report_json=write_clean_mgc_preflight(tmp_path, open_orders=[{"order_id": 7}]),
        recovery_report_json=write_clean_recovery(tmp_path),
        ledger_jsonl=initial.ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is False
    assert result.reconciliation_report["open_orders_confirmation"]["open_orders_none"] is False
    assert result.trade_summary["open_position_count"] == 1
    assert result.trade_summary["review_required_count"] == 1


def test_strategy_managed_lifecycle_is_not_archived_as_proof_canary(tmp_path: Path) -> None:
    ledger_jsonl = tmp_path / "paper_trade_ledger" / "track_b_paper_trade_ledger.jsonl"
    ledger_jsonl.parent.mkdir(parents=True, exist_ok=True)
    ledger_jsonl.write_text(
        json.dumps(
            {
                "ledger_schema_version": "track_b_paper_trade_ledger_v1",
                "paper_lifecycle_type": "STRATEGY_MANAGED",
                "lifecycle_id": "managed-open-001",
                "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "con_id": 712565978,
                "account_id": "DUM882026",
                "quantity": "1",
                "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
                "entry_fill_price": "4704.6",
                "review_required": True,
                "created_at": "2026-05-06T23:11:12+00:00",
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    result = reconcile_manually_flattened_proof_lifecycle(
        lifecycle_id="managed-open-001",
        preflight_report_json=write_clean_mgc_preflight(tmp_path),
        recovery_report_json=write_clean_recovery(tmp_path),
        ledger_jsonl=ledger_jsonl,
        output_root=tmp_path / "paper_trade_ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is False
    assert result.reconciliation_report["remaining_blocker"] == "LIFECYCLE_IS_NOT_PROOF_CANARY"
    assert result.trade_summary["open_position_count"] == 1
    assert result.trade_summary["review_required_count"] == 1


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
