from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.operator_status import OperatorStatusInputs, create_operator_status_summary
from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
    DUPLICATE_EXIT_OVERFILL_SCOPED_REMEDIATION_REVIEWED,
    MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
    _positions_by,
    build_track_b_paper_trade_summaries,
    reconcile_duplicate_exit_overfill_scoped_remediation,
    reconcile_app_only_unfilled_managed_lifecycles,
    reconcile_ibkr_contract_rejected_managed_lifecycles,
    reconcile_leak_test_adopted_entry_settled_flat_lifecycles,
    reconcile_manually_flattened_proof_lifecycle,
    update_track_b_paper_trade_ledger_from_filled_bridge_result,
    update_track_b_paper_trade_ledger_from_runner_report,
)
from mgc_v05l.execution_core.track_b_position_management_manifest import (
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    create_or_update_position_management_manifest,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 22, 30, tzinfo=timezone.utc)


def test_positions_by_preserves_same_lane_units_and_aggregates_quantity() -> None:
    records = [
        _open_unit("lifecycle-46", "46", "1306861269", "29898.5", "2026-05-26T10:36:07+00:00"),
        _open_unit("lifecycle-47", "47", "1306861276", "29901.5", "2026-05-26T10:37:09+00:00"),
        _open_unit("lifecycle-48", "48", "1306861287", "29904", "2026-05-26T10:38:09+00:00"),
    ]

    positions = _positions_by(records, "contract_key", aware_now())
    position = positions["MNQ-202606"]

    assert position["quantity"] == "3"
    assert position["aggregate_qty"] == "-3"
    assert position["side"] == "SHORT"
    assert position["lifecycle_unit_count"] == 3
    assert position["lifecycle_ids"] == ["lifecycle-46", "lifecycle-47", "lifecycle-48"]
    assert position["entry_order_ids"] == ["46", "47", "48"]
    assert position["duplicate_same_lane_exposure"] is True
    assert position["pyramiding_allowed"] is False
    assert position["pyramiding_policy"] == "PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED"
    assert [unit["signed_qty"] for unit in position["lifecycle_units"]] == ["-1", "-1", "-1"]


def _open_unit(
    lifecycle_id: str,
    order_id: str,
    perm_id: str,
    price: str,
    timestamp: str,
    *,
    strategy_id: str = "MNQ_FIRST_BEAR_SNAP_TURN_V1",
    lane_id: str = "mnq_first_bear_snap_turn",
    contract_key: str = "MNQ-202606",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
) -> dict:
    return {
        "final_position_status": "OPEN_MANAGED",
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "lifecycle_id": lifecycle_id,
        "signal_id": f"intent-{order_id}",
        "instrument_family": contract_key.split("-")[0],
        "contract_key": contract_key,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "expiry": "20260618",
        "account_id": "DUM882026",
        "side": "SHORT",
        "quantity": "1",
        "entry_order_id": order_id,
        "entry_perm_id": perm_id,
        "entry_fill_price": price,
        "entry_timestamp": timestamp,
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_lifecycle_report_path": f"outputs/lifecycle/{lifecycle_id}.json",
    }


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


def test_direct_bridge_fill_prefers_reserved_lifecycle_id(tmp_path: Path) -> None:
    artifact_path = tmp_path / "filled_bridge_result_latest.json"
    reserved_lifecycle_id = "reserved_submit_mgc_1x_all_lanes_asia_early_long_20260525T101343188485Z_bf0fbb7201f7"
    payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_long",
        "lane_id": "mgc_1x_all_lanes__asia_early_long",
        "instrument": "MGC",
        "symbol": "MGC",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "submit_owner_d03f3ef67fe7421d4f17101d",
        "reserved_lifecycle_id": reserved_lifecycle_id,
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-25T10:13:43.188485+00:00",
        "broker_order_id": "31",
        "account_id": "DUM882026",
        "perm_id": None,
        "client_id": 11940,
        "exec_id": None,
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "contract": {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "20260626", "multiplier": "10"},
        "fill_price": "4572.897",
        "fill_timestamp": "2026-05-25T10:32:09.473292+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "entry_source": "LEAK_TEST_ENTRY",
        "source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "managed_exit_policy_id": "FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
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
    assert result.trade_record["lifecycle_id"] == reserved_lifecycle_id
    assert result.trade_record["trade_id"].endswith(f":{reserved_lifecycle_id}")
    assert result.live_position_status["open_position_count"] == 1
    position = next(iter(result.live_position_status["positions_by_instrument"].values()))
    assert position["lifecycle_id"] == reserved_lifecycle_id


def test_duplicate_exit_overfill_scoped_remediation_clears_review_without_broker_mutation(tmp_path: Path) -> None:
    lifecycle_id = "reserved_submit_mnq_1x_asia_london_participation_asia_london_long_v6_20260525T111329090382Z_42f9e3f5707e"
    ledger = tmp_path / "ledger" / "track_b_paper_trade_ledger.jsonl"
    ledger.parent.mkdir(parents=True)
    review_row = {
        "ledger_schema_version": "track_b_paper_trade_ledger_v1",
        "trade_id": f"strategy:{lifecycle_id}",
        "lifecycle_id": lifecycle_id,
        "strategy_id": "asia_london_participation_core_v1__mnq_1x_asia_london_participation__asia_london_long_v6",
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "broker_backed_position_confirmed": True,
        "entry_fill_confirmed": True,
        "entry_order_id": "32",
        "entry_fill_price": "29976.81",
        "entry_timestamp": "2026-05-25T11:13:41.191614+00:00",
        "exit_order_id": "36",
        "exit_timestamp": "2026-05-25T11:53:55.018266+00:00",
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "final_position_status": "REVIEW_REQUIRED",
        "final_broker_state_classification": "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED",
        "review_required": True,
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "account_id": "DUM882026",
        "side": "LONG",
        "quantity": "1",
    }
    ledger.write_text(json.dumps(review_row, sort_keys=True) + "\n", encoding="utf-8")
    remediation = {
        "classification": "SCOPED_GUARDIAN_REMEDIATION_FILLED",
        "guardian_hard_classifications": ["UNAUTHORIZED_REVERSE_EXPOSURE", "BROKER_LIFECYCLE_POSITION_MISMATCH"],
        "target_identity": {"account_id": "DUM882026", "local_symbol": "MNQM6", "con_id": 770561201},
        "apply_result": {
            "broker_order_id": "5",
            "fill": {
                "account_id": "DUM882026",
                "action": "BUY",
                "contract_key": "MNQ-202606",
                "quantity": "1.0",
                "price": "29952.0",
                "perm_id": "917755089",
                "execution_id": "0000e1a7.6a1d9c47.01.01",
                "filled_at": "2026-05-25T12:41:47.257831+00:00",
            },
        },
    }
    remediation_path = write_json(tmp_path / "remediation.json", remediation)
    positions_path = write_json(
        tmp_path / "positions.json",
        {"positions": [{"account_id": "DUM882026", "symbol": "MNQ", "local_symbol": "MNQM6", "con_id": 770561201, "quantity": "0.0"}]},
    )
    orders_path = write_json(tmp_path / "orders.json", {"open_orders": []})

    result = reconcile_duplicate_exit_overfill_scoped_remediation(
        lifecycle_id=lifecycle_id,
        guardian_remediation_json=remediation_path,
        broker_positions_snapshot_json=positions_path,
        broker_open_orders_snapshot_json=orders_path,
        ledger_jsonl=ledger,
        output_root=tmp_path / "ledger",
        diagnostics_root=tmp_path / "diagnostics",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is True
    assert result.reconciliation_report["reconciliation_action"] == DUPLICATE_EXIT_OVERFILL_SCOPED_REMEDIATION_REVIEWED
    assert result.trade_summary["review_required_count"] == 0
    assert result.trade_summary["open_position_count"] == 0
    latest = result.trade_summary["recent_trades"][0]
    assert latest["review_required"] is False
    assert latest["artifact_reconciliation_classification"] == DUPLICATE_EXIT_OVERFILL_SCOPED_REMEDIATION_REVIEWED
    assert result.reconciliation_report["broker_mutation_attempted"] is False


def test_direct_bridge_fill_inherits_exit_policy_from_manifest(tmp_path: Path) -> None:
    manifest_root = tmp_path / "manifests"
    intent_id = "MNQ|1m|2026-05-21T15:07:00Z|BUY_TO_OPEN"
    create_or_update_position_management_manifest(
        entry_intent_id=intent_id,
        lane_id="mnq_1x_ny_early_core__us_midday_long",
        strategy_id="index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_midday_long",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        side="LONG",
        quantity=1,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        lifecycle_status="INTENT_CREATED",
        output_root=manifest_root,
        now=aware_now(),
    )
    payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_midday_long",
        "lane_id": "mnq_1x_ny_early_core__us_midday_long",
        "instrument": "MNQ",
        "symbol": "MNQ",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": intent_id,
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-21T15:07:00+00:00",
        "broker_order_id": "1",
        "account_id": "DUM882026",
        "perm_id": 1948367784,
        "client_id": 11194,
        "exec_id": "0000e1a7.6a19013e.01.01",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "expiry": "202606", "multiplier": "2"},
        "fill_price": "29150.0",
        "fill_timestamp": "2026-05-21T15:08:57.079226+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }

    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=payload,
        filled_bridge_result_json=write_json(tmp_path / "fill.json", payload),
        output_root=tmp_path / "ledger",
        position_management_manifest_root=manifest_root,
        managed_lifecycle_output_root=tmp_path / "managed",
        lane_registry_paths=(),
        now=aware_now(),
    )

    assert result.trade_record is not None
    assert result.trade_record["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.trade_record["position_management_metadata_source"] == "manifest"
    assert result.trade_record["review_required"] is False
    position = result.live_position_status["positions_by_instrument"]["MNQ-202606"]
    assert position["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert position["position_management_manifest_path"].endswith(".json")
    lifecycle_path = Path(position["paper_lifecycle_report_path"])
    assert lifecycle_path.exists()
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
    assert lifecycle["paper_lifecycle_classification"] == "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
    assert lifecycle["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


def test_direct_bridge_fill_missing_policy_is_review_required_incomplete(tmp_path: Path) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "unknown_strategy",
        "lane_id": "unknown_lane",
        "instrument": "MNQ",
        "symbol": "MNQ",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "MNQ|1m|2026-05-21T15:07:00Z|BUY_TO_OPEN",
        "intent_type": "BUY_TO_OPEN",
        "broker_order_id": "1",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "expiry": "202606", "multiplier": "2"},
        "fill_price": "29150.0",
        "fill_timestamp": "2026-05-21T15:08:57.079226+00:00",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
    }

    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=payload,
        filled_bridge_result_json=write_json(tmp_path / "fill.json", payload),
        output_root=tmp_path / "ledger",
        position_management_manifest_root=tmp_path / "manifests",
        lane_registry_paths=(tmp_path / "missing.json",),
        now=aware_now(),
    )

    assert result.trade_record is not None
    assert result.trade_record["managed_exit_policy_id"] is None
    assert result.trade_record["paper_lifecycle_classification"] == OPEN_MANAGED_METADATA_INCOMPLETE
    assert result.trade_record["review_required"] is True


def test_manifest_writer_rejects_unknown_lifecycle_state(tmp_path: Path) -> None:
    result = create_or_update_position_management_manifest(
        entry_intent_id="MGC|unknown",
        lane_id="track_b_paper_execution_test_mule_v1__mgc",
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        side="LONG",
        quantity=1,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        lifecycle_status="SOME_NEW_LOCAL_STATE",
        broker_ownership_identity={"broker_order_id": "1", "fill_price": "4528.8", "fill_timestamp": "2026-05-22T08:12:07+00:00"},
        lifecycle_id="bridge_fill_unknown",
        output_root=tmp_path / "manifests",
        now=aware_now(),
    )

    assert result.manifest["lifecycle_status"] == "REVIEW_REQUIRED"
    assert result.manifest["lifecycle_status_blockers"] == []


def test_direct_bridge_entry_without_broker_fill_identity_does_not_open_managed(tmp_path: Path) -> None:
    payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
        "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
        "instrument": "MGC",
        "symbol": "MGC",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "MGC|1m|2026-05-22T01:39:00Z|BUY_TO_OPEN",
        "intent_type": "BUY_TO_OPEN",
        "broker_order_id": None,
        "perm_id": None,
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "contract": {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "202606", "multiplier": "10"},
        "fill_price": None,
        "fill_timestamp": None,
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
    }

    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=payload,
        filled_bridge_result_json=write_json(tmp_path / "fill.json", payload),
        output_root=tmp_path / "ledger",
        position_management_manifest_root=tmp_path / "manifests",
        lane_registry_paths=(),
        now=aware_now(),
    )

    assert result.trade_record is not None
    assert result.trade_record["paper_lifecycle_classification"] == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE
    assert result.trade_record["final_position_status"] == "REVIEW_REQUIRED"
    assert result.trade_record["review_required"] is True
    assert result.live_position_status["open_position_count"] == 0


def test_direct_bridge_close_fill_persists_closed_flat_record(tmp_path: Path) -> None:
    output_root = tmp_path / "ledger"
    open_payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_early_long",
        "lane_id": "mnq_1x_ny_early_core__us_early_long",
        "instrument": "MNQ",
        "symbol": "MNQ",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "MNQ|1m|2026-05-14T12:26:00Z|BUY_TO_OPEN",
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-14T12:26:00+00:00",
        "broker_order_id": "1",
        "account_id": "DUM882026",
        "perm_id": 984265750,
        "client_id": 11099,
        "exec_id": "0000e1a7.6a09f918.01.01",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "expiry": "202606", "multiplier": "2"},
        "fill_price": "29561.5",
        "fill_timestamp": "2026-05-14T12:26:53.964662+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }
    close_payload = {
        **open_payload,
        "action": "SELL",
        "order_intent_id": "MNQ|1m|2026-05-14T13:03:00Z|SELL_TO_CLOSE",
        "intent_type": "SELL_TO_CLOSE",
        "broker_order_id": "11",
        "perm_id": 984270669,
        "client_id": 10864,
        "exec_id": "0000e1a7.6a0a0b1e.01.01",
        "fill_price": "29492.75",
        "fill_timestamp": "2026-05-14T13:04:48.414655+00:00",
    }

    update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=open_payload,
        filled_bridge_result_json=write_json(tmp_path / "open.json", open_payload),
        output_root=output_root,
        now=aware_now(),
    )
    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=close_payload,
        filled_bridge_result_json=write_json(tmp_path / "close.json", close_payload),
        output_root=output_root,
        now=aware_now(),
    )

    assert result.trade_record_written is True
    assert result.trade_record is not None
    assert result.trade_record["final_position_status"] == "CLOSED_FLAT"
    assert result.trade_record["exit_order_id"] == "11"
    assert result.trade_record["exit_fill_price"] == "29492.75"
    assert result.trade_record["realized_pnl"] == "-137.5"
    assert result.live_position_status["open_position_count"] == 0
    assert result.pnl_summary["completed_trades"] == 1


def test_flat_confirmed_direct_close_counts_as_closed_when_price_unknown(tmp_path: Path) -> None:
    output_root = tmp_path / "ledger"
    open_payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "atp_companion_v1__production_track_gc_asia_us_selective_v1",
        "lane_id": "atp_companion_v1_gc_asia_us_production_track_selective_v1",
        "instrument": "GC",
        "symbol": "GC",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "GC|leak_test|BUY_TO_OPEN",
        "intent_type": "BUY_TO_OPEN",
        "broker_order_id": "26",
        "account_id": None,
        "perm_id": None,
        "client_id": 11940,
        "exec_id": None,
        "local_symbol": "GCM6",
        "con_id": 430360630,
        "contract": {"symbol": "GC", "local_symbol": "GCM6", "expiry": "202606", "multiplier": "100"},
        "fill_price": "4566.0",
        "fill_timestamp": "2026-05-15T15:40:00+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }
    close_payload = {
        **open_payload,
        "action": "SELL",
        "order_intent_id": "GC|managed_exit_disappeared|27",
        "intent_type": "SELL_TO_CLOSE",
        "broker_order_id": "27",
        "account_id": "DUM882026",
        "fill_price": None,
        "fill_price_source": "UNKNOWN_BROKER_POSITION_FLAT",
        "realized_pnl_unknown": True,
        "fill_timestamp": "2026-05-15T15:45:00+00:00",
    }

    update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=open_payload,
        filled_bridge_result_json=write_json(tmp_path / "open.json", open_payload),
        output_root=output_root,
        now=aware_now(),
    )
    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=close_payload,
        filled_bridge_result_json=write_json(tmp_path / "close.json", close_payload),
        output_root=output_root,
        now=aware_now(),
    )

    assert result.trade_record_written is True
    assert result.trade_record is not None
    assert result.trade_record["final_position_status"] == "CLOSED_FLAT"
    assert result.trade_record["lane_id"] == open_payload["lane_id"]
    assert result.trade_record["exit_fill_confirmed"] is True
    assert result.trade_record["exit_fill_price"] is None
    assert result.live_position_status["open_position_count"] == 0
    assert result.pnl_summary["completed_trades"] == 1


def test_known_close_matches_strategy_managed_lifecycle_source_by_lifecycle_id(tmp_path: Path) -> None:
    output_root = tmp_path / "ledger"
    output_root.mkdir(parents=True)
    lifecycle_id = "strategy_managed_track_b_multi_strategy_runtime_cycle_04098e91aa94408e834cb36e3e404445_asia_late_flat_pullback_pause_resume_long_v1"
    open_row = {
        "ledger_schema_version": "track_b_paper_trade_ledger_v1",
        "trade_id": f"ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1:{lifecycle_id}",
        "lifecycle_id": lifecycle_id,
        "source": "TRACK_B_STRATEGY_MANAGED_LIFECYCLE",
        "strategy_id": "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        "lane_id": "mgc_asia_late_flat_pullback_pause_resume_long",
        "entry_intent_id": lifecycle_id,
        "paper_lifecycle_type": "STRATEGY_MANAGED",
        "broker_backed_position_confirmed": True,
        "instrument_family": "MGC",
        "contract_key": "MGC-202606",
        "contract": {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "20260626"},
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "account_id": "DUM882026",
        "side": "LONG",
        "quantity": "1",
        "entry_order_id": "44",
        "entry_fill_price": "4534.9",
        "entry_timestamp": "2026-05-26T02:56:52.969540+00:00",
        "entry_fill_confirmed": True,
        "final_position_status": "OPEN_MANAGED",
        "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    }
    ledger_jsonl = output_root / "track_b_paper_trade_ledger.jsonl"
    ledger_jsonl.write_text(json.dumps(open_row, sort_keys=True) + "\n", encoding="utf-8")
    close_payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "bridge_classification": "KNOWN_MANAGED_EXIT_ORDER_FILLED_CLOSE_PERSISTENCE_GAP",
        "intent_type": "SELL_TO_CLOSE",
        "action": "SELL",
        "instrument": "MGC",
        "symbol": "MGC",
        "strategy_id": "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        "lane_id": "mgc_asia_late_flat_pullback_pause_resume_long",
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "broker_order_id": "45",
        "client_id": 17086,
        "perm_id": 1306860537,
        "quantity": "1",
        "fill_price": None,
        "fill_price_source": "UNKNOWN_BROKER_POSITION_FLAT",
        "realized_pnl_unknown": True,
        "fill_timestamp": "2026-05-26T07:29:05.882775+00:00",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "contract": {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "20260626"},
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }

    result = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=close_payload,
        filled_bridge_result_json=write_json(tmp_path / "close.json", close_payload),
        output_root=output_root,
        now=aware_now(),
    )

    assert result.trade_record_written is True
    assert result.trade_record is not None
    assert result.trade_record["lifecycle_id"] == lifecycle_id
    assert result.trade_record["final_position_status"] == "CLOSED_FLAT"
    assert result.trade_record["source"] == "TRACK_B_STRATEGY_MANAGED_LIFECYCLE"
    assert result.trade_record["exit_order_id"] == "45"
    assert result.live_position_status["open_position_count"] == 0


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


def test_malformed_manual_cleanup_is_excluded_from_clean_trade_stats(tmp_path: Path) -> None:
    summaries = build_track_b_paper_trade_summaries(
        ledger_records=[
            {
                "ledger_schema_version": "track_b_paper_trade_ledger_v1",
                "trade_id": "malformed:1",
                "lifecycle_id": "malformed-1",
                "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
                "instrument_family": "MGC",
                "contract_key": "MGC-202606",
                "paper_lifecycle_type": "STRATEGY_MANAGED",
                "paper_lifecycle_classification": MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
                "final_position_status": MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT,
                "entry_fill_confirmed": True,
                "entry_fill_price": "4500.0",
                "entry_timestamp": "2026-05-22T08:12:07+00:00",
                "exit_fill_confirmed": True,
                "exit_fill_price": "4501.0",
                "exit_timestamp": "2026-05-22T08:17:07+00:00",
                "quantity": "1",
                "side": "LONG",
                "malformed_broker_backed_manually_reconciled": True,
                "realized_pnl": "10",
                "created_at": "2026-05-22T08:17:08+00:00",
            }
        ],
        ledger_jsonl=tmp_path / "ledger.jsonl",
        trade_summary_json=tmp_path / "summary.json",
        live_position_status_json=tmp_path / "positions.json",
        pnl_summary_json=tmp_path / "pnl.json",
        now=aware_now(),
    )

    assert summaries["trade_summary"]["completed_trade_count"] == 0
    assert summaries["trade_summary"]["archived_manual_flat_count"] == 1
    assert summaries["pnl_summary"]["total_realized_pnl_today"] == "0"


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
                        "signal_id": "managed-open-001",
                    "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                    "lane_id": "asia_early_long",
                    "contract_key": "MGC-202606",
                    "local_symbol": "MGCM6",
                    "con_id": 712565978,
                    "account_id": "DUM882026",
                    "quantity": "1",
                    "side": "LONG",
                    "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
                    "paper_lifecycle_classification": "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED",
                    "entry_order_id": "301",
                    "entry_perm_id": "1948384301",
                    "entry_fill_price": "4704.6",
                    "entry_timestamp": "2026-05-06T23:11:12+00:00",
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


def test_leak_test_adopted_entry_settled_flat_archives_lifecycle_only_row(tmp_path: Path) -> None:
    output_root = tmp_path / "paper_trade_ledger"
    open_payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "pl_us_late_pause_resume_long",
        "lane_id": "pl_us_late_pause_resume_long",
        "instrument": "PL",
        "symbol": "PL",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "d89d2455-10c4-4ae8-bf30-6a5dd3cd88d7",
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-15T15:49:42.830385+00:00",
        "broker_order_id": "25",
        "account_id": "DUM882026",
        "perm_id": None,
        "client_id": None,
        "exec_id": None,
        "local_symbol": "PLN6",
        "con_id": 644855286,
        "contract": {"symbol": "PL", "local_symbol": "PLN6", "expiry": "20260729", "multiplier": "50"},
        "fill_price": "1989.2504",
        "fill_timestamp": "2026-05-15T15:50:24.150574+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "entry_source": "LEAK_TEST_ENTRY",
        "source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        "source_artifact_paths": ["outputs/reports/track_b_paper_lifecycle_adoption/adopt.json"],
        "route_destination": "ibkr_paper_bridge_submit_capable",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }
    ledger = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=open_payload,
        filled_bridge_result_json=write_json(tmp_path / "adopt.json", open_payload),
        output_root=output_root,
        now=aware_now(),
    )
    write_json(
        tmp_path / "positions.json",
        {
            "positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "PL",
                    "local_symbol": "PLN6",
                    "con_id": 644855286,
                    "quantity": "0.0",
                }
            ]
        },
    )
    write_json(tmp_path / "orders.json", {"open_orders": []})

    result = reconcile_leak_test_adopted_entry_settled_flat_lifecycles(
        lifecycle_ids=["bridge_fill_d89d2455-10c4-4ae8-bf30-6a5dd3cd88d7"],
        ledger_jsonl=ledger.ledger_jsonl,
        output_root=output_root,
        diagnostics_root=tmp_path / "diagnostics",
        broker_positions_snapshot_json=tmp_path / "positions.json",
        broker_open_orders_snapshot_json=tmp_path / "orders.json",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is True
    assert result.live_position_status["open_position_count"] == 0
    rows = [json.loads(line) for line in ledger.ledger_jsonl.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["new_artifact_classification"] == "LEAK_TEST_ADOPTED_ENTRY_SETTLED_FLAT_REVIEWED"
    recent = result.trade_summary["recent_trades"][0]
    assert recent["artifact_reconciliation_classification"] == "LEAK_TEST_ADOPTED_ENTRY_SETTLED_FLAT_REVIEWED"
    assert recent["excluded_from_strategy_managed_pnl"] is True


def test_leak_test_adopted_entry_settled_flat_refuses_when_broker_position_remains(tmp_path: Path) -> None:
    output_root = tmp_path / "paper_trade_ledger"
    open_payload = {
        "classification": "PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
        "strategy_id": "pl_us_late_pause_resume_long",
        "lane_id": "pl_us_late_pause_resume_long",
        "instrument": "PL",
        "symbol": "PL",
        "action": "BUY",
        "quantity": 1,
        "order_intent_id": "d89d2455-10c4-4ae8-bf30-6a5dd3cd88d7",
        "intent_type": "BUY_TO_OPEN",
        "decision_bar_timestamp": "2026-05-15T15:49:42.830385+00:00",
        "broker_order_id": "25",
        "account_id": "DUM882026",
        "local_symbol": "PLN6",
        "con_id": 644855286,
        "contract": {"symbol": "PL", "local_symbol": "PLN6", "expiry": "20260729", "multiplier": "50"},
        "fill_price": "1989.2504",
        "fill_timestamp": "2026-05-15T15:50:24.150574+00:00",
        "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
        "entry_source": "LEAK_TEST_ENTRY",
        "source": "TRACK_B_PAPER_LIFECYCLE_ADOPTION",
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_proof_invoked": False,
        "live_money_readiness": False,
        "review_required": False,
    }
    ledger = update_track_b_paper_trade_ledger_from_filled_bridge_result(
        filled_bridge_result=open_payload,
        filled_bridge_result_json=write_json(tmp_path / "adopt.json", open_payload),
        output_root=output_root,
        now=aware_now(),
    )
    write_json(
        tmp_path / "positions.json",
        {
            "positions": [
                {
                    "account_id": "DUM882026",
                    "symbol": "PL",
                    "local_symbol": "PLN6",
                    "con_id": 644855286,
                    "quantity": "1.0",
                }
            ]
        },
    )
    write_json(tmp_path / "orders.json", {"open_orders": []})

    result = reconcile_leak_test_adopted_entry_settled_flat_lifecycles(
        lifecycle_ids=["bridge_fill_d89d2455-10c4-4ae8-bf30-6a5dd3cd88d7"],
        ledger_jsonl=ledger.ledger_jsonl,
        output_root=output_root,
        diagnostics_root=tmp_path / "diagnostics",
        broker_positions_snapshot_json=tmp_path / "positions.json",
        broker_open_orders_snapshot_json=tmp_path / "orders.json",
        now=aware_now(),
    )

    assert result.reconciliation_record_written is False
    assert result.reconciliation_report["targets"][0]["remaining_blocker"] == "BROKER_POSITION_STILL_OPEN"
    assert result.live_position_status["open_position_count"] == 1
