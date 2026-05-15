from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
    reconcile_track_b_paper_broker_truth,
)


NOW = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)


def test_reconciles_flat_lifecycle_with_fresh_broker_truth_and_unrelated_positions(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "AAPL",
                "local_symbol": "AAPL",
                "security_type": "STK",
                "quantity": "500",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["track_b_broker_position_count"] == 0
    assert report["track_b_broker_open_order_count"] == 0
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["source"] == "BROKER_RECONCILED"
    assert reconciled_position["broker_reconciled"] is True
    assert reconciled_position["broker_reconciled_state"] == "BROKER_AND_LIFECYCLE_FLAT"
    assert reconciled_position["open_position_count"] == 0
    assert reconciled_position["open_order_count"] == 0
    assert reconciled_position["live_money_eligible"] is False
    assert reconciled_position["paper_proof_invoked"] is False


def test_blocks_when_track_b_broker_position_exists_but_lifecycle_is_flat(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["broker_reconciled"] is False
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])
    assert not config.reconciled_live_position_status_path.exists()


def test_count_mismatch_still_reports_cost_basis_for_matched_positions(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
            "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "28981.25",
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "PL",
                "local_symbol": "PLN6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "107257.52",
                "multiplier": "50",
            },
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "57963.12",
                "multiplier": "2",
            },
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["position_match_report"]["state"] == "BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH"
    assert report["position_match_report"]["matches"][0]["root"] == "MNQ"
    assert report["broker_cost_basis_adjustments"][0]["broker_minus_lifecycle_points_per_contract"] == "0.31"
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])


def test_reconciles_matching_track_b_broker_and_lifecycle_open_position(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
            "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "28981.25",
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "57963.12",
                "multiplier": "2",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["position_match_report"]["state"] == "BROKER_AND_LIFECYCLE_OPEN_MATCHED"
    assert report["broker_cost_basis_adjustments"] == [
        {
            "source": "IBKR_AVERAGE_PRICE_MINUS_TRACK_B_LIFECYCLE_ENTRY_PRICE",
            "root": "MNQ",
            "broker_local_symbol": "MNQM6",
            "lifecycle_local_symbol": "MNQM6",
            "quantity": "1",
            "lifecycle_average_entry_price": "28981.25",
            "broker_average_price": "28981.56",
            "broker_minus_lifecycle_points_per_contract": "0.31",
            "broker_minus_lifecycle_points_total": "0.31",
            "absolute_points_per_contract": "0.31",
            "absolute_points_total": "0.31",
            "note": "Captured for broker fee/cost-basis tracking only; IBKR broker truth remains authoritative for live PAPER position state.",
        }
    ]
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["broker_reconciled_state"] == "BROKER_AND_LIFECYCLE_OPEN_MATCHED"
    assert reconciled_position["open_position_count"] == 1
    assert reconciled_position["broker_track_b_position_count"] == 1
    assert reconciled_position["broker_cost_basis_adjustments"] == report["broker_cost_basis_adjustments"]
    assert reconciled_position["live_money_eligible"] is False
    assert reconciled_position["paper_proof_invoked"] is False


def test_blocks_when_track_b_open_order_exists(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        open_orders=[
            {
                "order_id": 42,
                "action": "BUY",
                "total_quantity": "1",
                "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "security_type": "FUT"},
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert any(blocker["code"] == "UNKNOWN_BROKER_OPEN_ORDER" for blocker in report["blockers"])


def test_known_managed_exit_order_is_not_unknown_open_order_blocker(tmp_path: Path) -> None:
    open_position = {
        "strategy_id": "gc_1x_all_lanes__london_early_long",
        "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "instrument_family": "GC",
        "contract_key": "GC-202606",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "side": "LONG",
        "quantity": "1",
        "avg_entry_price": "4574.6",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    live_status = json.loads(config.live_position_status_path.read_text(encoding="utf-8"))
    live_status["known_managed_exit_orders"] = [
        {
            "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
            "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
            "strategy_id": "gc_1x_all_lanes__london_early_long",
            "lane_id": "gc_1x_all_lanes__london_early_long",
            "order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
            "broker_order_id": "1",
            "client_id": 10815,
            "perm_id": 614029377,
            "symbol": "GC",
            "local_symbol": "GCM6",
            "con_id": 430360630,
            "action": "SELL",
            "quantity": "1",
        }
    ]
    _write_json(config.live_position_status_path, live_status)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "457460.0",
                "multiplier": "100",
                "con_id": 430360630,
            }
        ],
        open_orders=[
            {
                "broker_order_id": "1",
                "client_id": 10815,
                "perm_id": 614029377,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "order_type": "LMT",
                "limit_price": "4574.7",
                "quantity": "1",
                "remaining_quantity": "1",
                "status": "Submitted",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER"
    assert report["broker_reconciled"] is True
    assert report["known_managed_exit_order_count"] == 1
    assert report["unknown_broker_open_order_count"] == 0
    assert report["blockers"] == []
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["known_managed_exit_orders"][0]["broker_order_id"] == "1"


def test_known_leak_test_entry_order_is_not_unknown_open_order_blocker(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    state_path = tmp_path / "outputs" / "track_b_execution_core" / "leak_test_entry_orders" / "latest_known_leak_test_entry_orders.json"
    _write_json(
        state_path,
        {
            "schema_version": "track_b_known_leak_test_entry_orders_v1",
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "known_leak_test_entry_orders": [
                {
                    "managed_order_status": "KNOWN_LEAK_TEST_ENTRY_ORDER_WORKING",
                    "source": "IBKR_PAPER_STRATEGY_BRIDGE_DELEGATED_LEAK_TEST_ENTRY_SUBMIT",
                    "broker_order_id": "12",
                    "client_id": 11940,
                    "perm_id": 614043263,
                    "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                    "lane_id": "gc_1x_all_lanes__london_early_long",
                    "account_id": "DUM882026",
                    "symbol": "GC",
                    "local_symbol": "GCM6",
                    "con_id": 430360630,
                    "action": "BUY",
                    "quantity": "1",
                    "limit_price": "4556.0",
                }
            ],
        },
    )
    _write_broker_truth(
        config,
        open_orders=[
            {
                "broker_order_id": "12",
                "client_id": 11940,
                "perm_id": 614043263,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "BUY",
                "order_type": "LMT",
                "limit_price": "4556.0",
                "quantity": "1",
                "remaining_quantity": "1",
                "status": "Submitted",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_LEAK_TEST_ENTRY_ORDER"
    assert report["broker_reconciled"] is True
    assert report["known_leak_test_entry_order_count"] == 1
    assert report["unknown_broker_open_order_count"] == 0
    assert report["blockers"] == []
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["known_leak_test_entry_orders"][0]["broker_order_id"] == "12"


def test_runtime_restore_known_managed_exit_order_is_not_unknown_open_order_blocker(tmp_path: Path) -> None:
    open_position = {
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "instrument_family": "GC",
        "contract_key": "GC-202606",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "side": "LONG",
        "quantity": "1",
        "avg_entry_price": "4574.6",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    restore_path = (
        config.repo_root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "lanes"
        / "gc_1x_all_lanes__london_early_long"
        / "restore_validation_latest.json"
    )
    _write_json(
        restore_path,
        {
            "lane_id": "gc_1x_all_lanes__london_early_long",
            "symbol": "GC",
            "pre_restore_state_summary": {
                "latest_order_intent": {
                    "order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
                    "intent_type": "SELL_TO_CLOSE",
                    "standalone_strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                    "lane_id": "gc_1x_all_lanes__london_early_long",
                    "instrument": "GC",
                    "symbol": "GC",
                    "quantity": 1,
                    "reason_code": "forced_session_initial_stop",
                    "submitted_at": "2026-05-15T08:50:35.730818+00:00",
                }
            },
            "restored_state_summary": {
                "latest_order_intent_state": "ACKNOWLEDGED",
                "last_order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
                "open_broker_order_id": "1",
                "pending_broker_order_ids": ["1"],
                "pending_execution_count": 1,
                "broker_snapshot": {
                    "broker_truth_position": {
                        "symbol": "GC",
                        "local_symbol": "GCM6",
                        "expiry": "20260626",
                        "con_id": 430360630,
                    }
                },
            },
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "457460.0",
                "multiplier": "100",
                "con_id": 430360630,
            }
        ],
        open_orders=[
            {
                "broker_order_id": "1",
                "client_id": 10815,
                "perm_id": 614029377,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "order_type": "LMT",
                "limit_price": "4574.7",
                "quantity": "1",
                "remaining_quantity": "1",
                "status": "Submitted",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER"
    assert report["broker_reconciled"] is True
    assert report["known_managed_exit_order_count"] == 1
    assert report["unknown_broker_open_order_count"] == 0
    assert report["blockers"] == []
    known_order = report["known_managed_exit_orders"][0]
    assert known_order["source"] == "TRACK_B_RUNTIME_RESTORE_PENDING_EXIT_ORDER"
    assert known_order["source_artifact_path"] == str(restore_path)


def test_known_hard_managed_exit_order_reports_stale_non_marketable_policy(tmp_path: Path) -> None:
    open_position = {
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "instrument_family": "GC",
        "contract_key": "GC-202606",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "side": "LONG",
        "quantity": "1",
        "avg_entry_price": "4574.6",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_market_price(config, "GC", close=4556.9)
    live_status = json.loads(config.live_position_status_path.read_text(encoding="utf-8"))
    live_status["known_managed_exit_orders"] = [
        {
            "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
            "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
            "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
            "lane_id": "gc_1x_all_lanes__london_early_long",
            "order_intent_id": "GC|1m|2026-05-15T08:06:00Z|SELL_TO_CLOSE",
            "broker_order_id": "1",
            "client_id": 10815,
            "perm_id": 614029377,
            "symbol": "GC",
            "local_symbol": "GCM6",
            "con_id": 430360630,
            "action": "SELL",
            "order_type": "LMT",
            "limit_price": "4574.7",
            "quantity": "1",
            "submitted_at": "2026-05-11T11:50:00+00:00",
            "exit_reason": "forced_session_initial_stop",
        }
    ]
    _write_json(config.live_position_status_path, live_status)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "457460.0",
                "multiplier": "100",
                "con_id": 430360630,
            }
        ],
        open_orders=[
            {
                "broker_order_id": "1",
                "client_id": 10815,
                "perm_id": 614029377,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "order_type": "LMT",
                "quantity": "1",
                "remaining_quantity": "1",
                "status": "Submitted",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    assert report["stale_managed_exit_order_count"] == 1
    assert report["hard_exit_order_not_marketable_count"] == 1
    order = report["known_managed_exit_orders"][0]
    assert order["managed_order_status"] == "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED"
    assert order["managed_order_policy"]["exit_urgency"] == "HARD_PROTECTIVE"
    assert order["order_limit_price"] == 4574.7
    assert order["runtime_market_reference"] == 4556.9
    assert order["marketable_by_runtime_context"] is False
    proposal = order["guarded_cancel_replace_proposal"]
    assert proposal["enabled"] is False
    assert proposal["requires_explicit_operator_authorization"] is True
    assert proposal["cancel_identity"]["broker_order_id"] == "1"
    assert proposal["replacement_order"]["limit_price"] == 4556.8
    assert proposal["replacement_order"]["price_source"] == "RUNTIME_MARKET_REFERENCE_PLUS_HARD_EXIT_ONE_TICK"
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["stale_managed_exit_order_count"] == 1


def test_persisted_cancel_replace_working_order_remains_known_managed(tmp_path: Path) -> None:
    open_position = {
        "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
        "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
        "instrument_family": "GC",
        "contract_key": "GC-202606",
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "side": "LONG",
        "quantity": "1",
        "avg_entry_price": "4574.6",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_market_price(config, "GC", close=4556.9)
    _write_json(
        tmp_path / "outputs" / "track_b_execution_core" / "managed_exit_orders" / "latest_known_managed_exit_orders.json",
        {
            "schema_version": "track_b_known_managed_exit_orders_v1",
            "generated_at": "2026-05-11T11:59:00+00:00",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "known_managed_exit_orders": [
                {
                    "managed_order_status": "KNOWN_MANAGED_EXIT_ORDER_WORKING",
                    "source": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
                    "lifecycle_id": "bridge_fill_GC|1m|2026-05-15T07:06:00Z|BUY_TO_OPEN",
                    "strategy_id": "gc_mgc_forced_session_baseline_v2__gc_1x_all_lanes__london_early_long",
                    "lane_id": "gc_1x_all_lanes__london_early_long",
                    "order_intent_id": "replacement-exit",
                    "broker_order_id": "2",
                    "client_id": 10941,
                    "perm_id": 614029400,
                    "symbol": "GC",
                    "local_symbol": "GCM6",
                    "expiry": "20260626",
                    "con_id": 430360630,
                    "action": "SELL",
                    "order_type": "LMT",
                    "limit_price": "4556.8",
                    "tif": "DAY",
                    "quantity": "1",
                    "submitted_at": "2026-05-11T11:58:00+00:00",
                    "exit_reason": "forced_session_initial_stop",
                }
            ],
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "457460.0",
                "multiplier": "100",
                "con_id": 430360630,
            }
        ],
        open_orders=[
            {
                "broker_order_id": "2",
                "client_id": 10941,
                "perm_id": 614029400,
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "expiry": "20260626",
                "con_id": 430360630,
                "action": "SELL",
                "order_type": "LMT",
                "quantity": "1",
                "remaining_quantity": "1",
                "status": "Submitted",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER"
    assert report["unknown_broker_open_order_count"] == 0
    assert report["known_managed_exit_order_count"] == 1
    known_order = report["known_managed_exit_orders"][0]
    assert known_order["source"] == "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY"
    assert known_order["broker_order_id"] == "2"


def test_blocks_when_broker_truth_is_stale_or_incomplete(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(config, generated_at="2026-05-11T11:55:00+00:00", positions_complete=False)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    codes = {blocker["code"] for blocker in report["blockers"]}
    assert "BROKER_TRUTH_STATUS_STALE" in codes
    assert "BROKER_TRUTH_STATUS_FLAG_MISMATCH" in codes
    assert "BROKER_TRUTH_SNAPSHOT_INCOMPLETE" in codes
    assert report["broker_reconciled"] is False


def test_bridge_terminal_event_grace_prevents_short_broker_truth_lag_block(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        recent_trades=[
            {
                "account_id": "DUM882026",
                "contract_key": "GC-202606",
                "local_symbol": "GCM6",
                "con_id": 470332,
                "exit_timestamp": "2026-05-11T11:59:15+00:00",
                "exit_order_id": "1234",
            }
        ],
    )
    _write_broker_truth(config, generated_at="2026-05-11T11:57:00+00:00")

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["bridge_terminal_event_grace"]["applied"] is True
    assert report["bridge_terminal_event_grace"]["downgraded_stale_blockers"]
    assert not any(blocker["code"] == "BROKER_TRUTH_STATUS_STALE" for blocker in report["blockers"])
    assert report["live_money_eligible"] is False


def test_bridge_terminal_event_grace_expires_and_fails_closed(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        recent_trades=[
            {
                "account_id": "DUM882026",
                "contract_key": "GC-202606",
                "local_symbol": "GCM6",
                "con_id": 470332,
                "exit_timestamp": "2026-05-11T11:40:00+00:00",
                "exit_order_id": "1234",
            }
        ],
    )
    _write_broker_truth(config, generated_at="2026-05-11T11:57:00+00:00")

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["bridge_terminal_event_grace"]["applied"] is False
    assert any(blocker["code"] == "BROKER_TRUTH_STATUS_STALE" for blocker in report["blockers"])


def test_known_entry_fill_waits_for_broker_truth_settlement(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "atp_companion_v1__production_track_gc_asia_us",
            "lifecycle_id": "bridge_fill_gc_leak_test",
            "instrument_family": "GC",
            "contract_key": "GC-202606",
            "local_symbol": "GCM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "4550.0",
            "entry_order_id": "8",
            "entry_timestamp": "2026-05-11T11:59:00+00:00",
            "con_id": 430360630,
        },
    )
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    assert report["broker_reconciled"] is False
    assert report["broker_truth_settlement"]["classification"] == "WAITING_FOR_BROKER_TRUTH_SETTLEMENT"
    assert report["broker_truth_settlement"]["event"]["broker_order_id"] == "8"
    assert not any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])


def test_broker_truth_settlement_resolves_when_position_matches_within_window(tmp_path: Path) -> None:
    open_position = {
        "strategy_id": "atp_companion_v1__production_track_gc_asia_us",
        "lifecycle_id": "bridge_fill_gc_leak_test",
        "instrument_family": "GC",
        "contract_key": "GC-202606",
        "local_symbol": "GCM6",
        "side": "LONG",
        "quantity": "1",
        "avg_entry_price": "4550.0",
        "entry_order_id": "8",
        "entry_timestamp": "2026-05-11T11:59:00+00:00",
        "con_id": 430360630,
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    live_position_status = json.loads(config.live_position_status_path.read_text(encoding="utf-8"))
    live_position_status["broker_truth_settlement"] = {
        "classification": "WAITING_FOR_BROKER_TRUTH_SETTLEMENT",
        "event": {
            "event_type": "ENTRY_FILL_EXPECTING_BROKER_POSITION",
            "broker_order_id": "8",
            "contract_key": "GC-202606",
            "local_symbol": "GCM6",
            "quantity": "1",
            "side": "LONG",
            "event_time": "2026-05-11T11:59:00+00:00",
        },
    }
    _write_json(config.live_position_status_path, live_position_status)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "455000",
                "multiplier": "100",
                "con_id": 430360630,
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "BROKER_TRUTH_SETTLEMENT_RESOLVED"
    assert report["broker_reconciled"] is True
    assert report["broker_truth_settlement"]["classification"] == "BROKER_TRUTH_SETTLEMENT_RESOLVED"


def test_broker_truth_settlement_timeout_blocks(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "atp_companion_v1__production_track_gc_asia_us",
            "lifecycle_id": "bridge_fill_gc_leak_test",
            "instrument_family": "GC",
            "contract_key": "GC-202606",
            "local_symbol": "GCM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "4550.0",
            "entry_order_id": "8",
            "entry_timestamp": "2026-05-11T11:50:00+00:00",
            "con_id": 430360630,
        },
    )
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "BROKER_TRUTH_SETTLEMENT_TIMEOUT"
    assert report["broker_reconciled"] is False
    assert any(blocker["code"] == "BROKER_TRUTH_SETTLEMENT_TIMEOUT" for blocker in report["blockers"])


def test_unknown_open_order_does_not_enter_settlement_wait(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "atp_companion_v1__production_track_gc_asia_us",
            "lifecycle_id": "bridge_fill_gc_leak_test",
            "instrument_family": "GC",
            "contract_key": "GC-202606",
            "local_symbol": "GCM6",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "4550.0",
            "entry_order_id": "8",
            "entry_timestamp": "2026-05-11T11:59:00+00:00",
            "con_id": 430360630,
        },
    )
    _write_broker_truth(
        config,
        positions=[],
        open_orders=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "order_id": "99",
                "action": "BUY",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE"
    assert report["broker_truth_settlement"]["classification"] == "BROKER_TRUTH_SETTLEMENT_CONTRADICTORY_STATE"
    assert any(blocker["code"] == "UNKNOWN_BROKER_OPEN_ORDER" for blocker in report["blockers"])


def test_blocks_when_lifecycle_reports_review_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path, review_required_count=1)
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert any(blocker["code"] == "LIFECYCLE_REVIEW_REQUIRED_PRESENT" for blocker in report["blockers"])
    assert report["broker_reconciled"] is False


def test_blocks_when_bridge_fill_persistence_is_review_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path, review_required_count=1)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "PL",
                "local_symbol": "PLN6",
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "107257.52",
                "multiplier": "50",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    codes = {blocker["code"] for blocker in report["blockers"]}
    assert "LIFECYCLE_REVIEW_REQUIRED_PRESENT" in codes
    assert "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" in codes
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["broker_reconciled"] is False
    assert not config.reconciled_live_position_status_path.exists()


def _write_base_artifacts(
    tmp_path: Path,
    *,
    review_required_count: int = 0,
    open_position: dict[str, object] | None = None,
    recent_trades: list[dict[str, object]] | None = None,
) -> ReconciliationConfig:
    ledger_root = tmp_path / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    broker_root = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification"
    report_path = tmp_path / "outputs" / "reports" / "track_b_paper_broker_reconciliation" / "latest.json"
    ledger_root.mkdir(parents=True)
    broker_root.mkdir(parents=True)
    config = ReconciliationConfig(
        repo_root=tmp_path,
        ledger_root=ledger_root,
        broker_truth_root=broker_root,
        market_data_root=tmp_path / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data",
        report_path=report_path,
        max_age_seconds=120.0,
    )
    _write_json(
        config.trade_summary_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "paper_trades_attempted_count": 2,
            "open_position_count": 1 if open_position else 0,
            "review_required_count": review_required_count,
            "recent_trades": recent_trades or [],
        },
    )
    _write_json(
        config.live_position_status_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "open_position_count": 1 if open_position else 0,
            "open_order_count": 0,
            "positions_by_instrument": {str(open_position["contract_key"]): open_position} if open_position else {},
            "positions_by_strategy": {str(open_position["strategy_id"]): open_position} if open_position else {},
            "review_required_positions": [],
        },
    )
    _write_json(
        config.pnl_summary_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "total_realized_pnl_today": "0",
            "total_unrealized_pnl": "0",
            "review_required_count": review_required_count,
            "by_strategy": {},
            "by_instrument": {},
        },
    )
    return config


def _write_broker_truth(
    config: ReconciliationConfig,
    *,
    generated_at: str = "2026-05-11T11:59:30+00:00",
    positions_complete: bool = True,
    open_orders_complete: bool = True,
    positions: list[dict[str, object]] | None = None,
    open_orders: list[dict[str, object]] | None = None,
) -> None:
    positions_path = config.broker_truth_root / "ibkr_positions_snapshot.json"
    open_orders_path = config.broker_truth_root / "ibkr_open_orders_snapshot.json"
    positions_payload = positions or []
    open_orders_payload = open_orders or []
    _write_json(
        config.broker_status_path,
        {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "generated_at": generated_at,
            "latest_refresh_time": generated_at,
            "last_success": True,
            "read_only": True,
            "account": "DUM882026",
            "positions_complete": positions_complete,
            "open_orders_complete": open_orders_complete,
            "position_count": len(positions_payload),
            "open_order_count": len(open_orders_payload),
            "positions_snapshot_path": str(positions_path),
            "open_orders_snapshot_path": str(open_orders_path),
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        positions_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "positions_complete": positions_complete,
            "request_method": "reqPositions",
            "positions": positions_payload,
        },
    )
    _write_json(
        open_orders_path,
        {
            "generated_at": generated_at,
            "account": "DUM882026",
            "selected_account_id": "DUM882026",
            "read_only": True,
            "open_orders_complete": open_orders_complete,
            "request_method": "reqAllOpenOrders",
            "auto_open_orders_requested": False,
            "order_binding_requested": False,
            "open_orders": open_orders_payload,
        },
    )


def _write_market_price(config: ReconciliationConfig, root: str, *, close: float) -> None:
    path = config.market_data_root / root / "1m" / "latest_runtime_candles.json"
    _write_json(
        path,
        {
            "generated_at": "2026-05-11T11:59:30+00:00",
            "candles": [
                {
                    "bar_start": "2026-05-11T11:58:00+00:00",
                    "bar_end": "2026-05-11T11:59:00+00:00",
                    "close": close,
                    "completed": True,
                }
            ],
        },
    )


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
