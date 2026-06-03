from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import (
    ReconciliationConfig,
    _broker_lifecycle_position_match,
    reconcile_track_b_paper_broker_truth,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    make_live_trade_registry_event,
    validate_registry_managed_exit_identity,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    SubmitIntentOwnershipRecord,
    SubmitIntentOwnershipState,
    append_submit_intent_ownership_record,
)


NOW = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc)


def test_broker_lifecycle_match_accepts_aggregate_same_lane_units() -> None:
    report = _broker_lifecycle_position_match(
        broker_positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "quantity": "-3",
            }
        ],
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "instrument_family": "MNQ",
                "contract_key": "MNQ-202606",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "side": "SHORT",
                "quantity": "3",
                "aggregate_qty": "-3",
                "lifecycle_unit_count": 3,
                "lifecycle_ids": ["lifecycle-46", "lifecycle-47", "lifecycle-48"],
                "duplicate_same_lane_exposure": True,
                "pyramiding_allowed": False,
            }
        ],
        symbols=["MNQ"],
    )

    assert report["matched"] is True
    assert report["state"] == "BROKER_AND_LIFECYCLE_OPEN_MATCHED"
    assert report["matches"][0]["lifecycle_unit_count"] == 3
    assert report["matches"][0]["duplicate_same_lane_exposure"] is True


def test_broker_lifecycle_match_blocks_collapsed_same_lane_quantity() -> None:
    report = _broker_lifecycle_position_match(
        broker_positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "quantity": "-3",
            }
        ],
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "instrument_family": "MNQ",
                "contract_key": "MNQ-202606",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "side": "SHORT",
                "quantity": "1",
                "lifecycle_id": "lifecycle-48",
            }
        ],
        symbols=["MNQ"],
    )

    assert report["matched"] is False
    assert report["state"] == "BROKER_LIFECYCLE_POSITION_DETAIL_MISMATCH"


def test_broker_lifecycle_match_scopes_weaker_duplicate_lifecycle_to_full_audit() -> None:
    report = _broker_lifecycle_position_match(
        broker_positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MES",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "expiry": "20260618",
                "quantity": "1",
                "average_cost": "38089.37",
                "multiplier": "5",
            }
        ],
        lifecycle_positions=[
            {
                "account_id": "MULTIPLE",
                "trade_id": "trade_older_globex_lifecycle",
                "lifecycle_id": "reserved_submit_mes_globex_active_participation_long_older",
                "instrument_family": "MES",
                "contract_key": "MES-M6",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "expiry": "20260618",
                "side": "LONG",
                "quantity": "1",
                "entry_price": "7601.5",
                "entry_exec_id": "0000e1a7.older.01.01",
            },
            {
                "account_id": "DUM882026",
                "trade_id": "trade_current_us_long",
                "lifecycle_id": "reserved_submit_mes_us_active_participation_long_current",
                "instrument_family": "MES",
                "contract_key": "MES-202606",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "expiry": "20260618",
                "side": "LONG",
                "quantity": "1",
                "entry_price": "7617.75",
                "entry_exec_id": "0000e1a7.6a2e7d49.01.01",
                "entry_perm_id": "665735688",
                "entry_order_id": "2",
            },
        ],
        symbols=["MES"],
    )

    assert report["matched"] is True
    assert report["matches"][0]["lifecycle_position"]["trade_id"] == "trade_current_us_long"
    assert report["superseded_unmatched_lifecycle_positions"][0]["trade_id"] == "trade_older_globex_lifecycle"
    assert report["superseded_unmatched_lifecycle_positions"][0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"


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
    assert report["open_order_truth_classification"] == "NO_OPEN_ORDERS"
    assert report["open_order_truth"]["source"] == "OPEN_ORDER_TRUTH_BUILDER_DIRECT"
    assert report["managed_order_registry_classification"] == "NO_MANAGED_ORDERS"
    assert report["managed_order_registry"]["source"] == "MANAGED_ORDER_REGISTRY_AUTHORITY_ARTIFACT"
    assert "outputs/operator_dashboard/runtime/latest_track_b_managed_orders.json" not in report["managed_order_registry"]["artifact_path"]
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["source"] == "BROKER_RECONCILED"
    assert reconciled_position["broker_reconciled"] is True
    assert reconciled_position["broker_reconciled_state"] == "BROKER_AND_LIFECYCLE_FLAT"
    assert reconciled_position["open_position_count"] == 0
    assert reconciled_position["open_order_count"] == 0
    assert reconciled_position["live_money_eligible"] is False
    assert reconciled_position["paper_proof_invoked"] is False


def test_reconciliation_reports_last_successful_truth_and_latest_attempt(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(config)
    status = json.loads(config.broker_status_path.read_text(encoding="utf-8"))
    status.update(
        {
            "classification": "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED",
            "last_failure": True,
            "last_failure_at": "2026-05-11T11:59:50+00:00",
            "latest_attempt_status": {
                "classification": "BROKER_TRUTH_REFRESH_FAILED",
                "generated_at": "2026-05-11T11:59:50+00:00",
                "positions_complete": False,
                "open_orders_complete": False,
                "position_count": 0,
                "open_order_count": 0,
                "submit_authority": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            },
            "last_successful_broker_truth": {
                "classification": "BROKER_TRUTH_REFRESH_LAST_SUCCESS_PRESERVED",
                "generated_at": "2026-05-11T11:59:30+00:00",
                "positions_complete": True,
                "open_orders_complete": True,
                "position_count": 0,
                "open_order_count": 0,
                "positions_snapshot_path": str(config.broker_truth_root / "ibkr_positions_snapshot.json"),
                "open_orders_snapshot_path": str(config.broker_truth_root / "ibkr_open_orders_snapshot.json"),
                "submit_authority": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            },
        }
    )
    _write_json(config.broker_status_path, status)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["last_successful_broker_truth"]["positions_complete"] is True
    assert report["latest_attempt_status"]["classification"] == "BROKER_TRUTH_REFRESH_FAILED"
    assert report["latest_attempt_status"]["positions_complete"] is False


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


def test_broker_only_position_with_matching_submit_intent_is_adoption_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["broker_reconciled"] is False
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED"
    remediation = report["broker_backed_entry_adoption"]
    assert remediation["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REQUIRED"
    assert remediation["ownership_intent_id"].startswith("submit_owner_")
    assert remediation["broker_order_id"] == "28"
    assert remediation["perm_id"] == 614044377
    assert remediation["exec_id"] == "exec-1"
    assert remediation["contract"] == {
        "symbol": "MGC",
        "local_symbol": "MGCM6",
        "expiry": "20260626",
        "con_id": 712565978,
    }
    assert remediation["qty"] == 1
    assert any(blocker["code"] == "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED" for blocker in report["blockers"])
    blocker = next(blocker for blocker in report["blockers"] if blocker["code"] == "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED")
    assert blocker["broker_backed_entry_adoption"]["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REQUIRED"
    assert not any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])
    events = _read_registry_events(config)
    assert events[-1]["event_type"] == "RECOVERY_ADOPTION_RECORDED"
    assert events[-1]["trade_id"] == remediation["trade_id"]


def test_multiple_mule_broker_only_positions_get_per_position_adoption_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(
        config,
        lane_id="track_b_paper_execution_test_mule_v1__mgc",
        strategy_id="track_b_paper_execution_test_mule_v1__mgc",
        broker_order_id="1",
        ownership_intent_id="submit_owner_test_mgc_1",
        client_id=10869,
        perm_id=1948358479,
        symbol="MGC",
        local_symbol="MGCM6",
        expiry="20260626",
        con_id=712565978,
        created_at="2026-05-11T11:59:00+00:00",
    )
    _write_submit_intent_ownership(
        config,
        lane_id="track_b_paper_execution_test_mule_v1__mnq",
        strategy_id="track_b_paper_execution_test_mule_v1__mnq",
        broker_order_id="1",
        ownership_intent_id="submit_owner_test_mnq_1",
        client_id=10968,
        perm_id=1948358488,
        symbol="MNQ",
        local_symbol="MNQM6",
        expiry="20260618",
        con_id=770561201,
        created_at="2026-05-11T11:59:05+00:00",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "security_type": "FUT",
                "quantity": "1",
            },
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            },
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["broker_reconciled"] is False
    ownership = report["submit_intent_ownership_reconciliation"]
    assert ownership["classification"] == "SUBMIT_INTENT_BROKER_POSITIONS_ADOPTION_REQUIRED"
    assert len(ownership["matching_adoptions"]) == 2
    remediation = report["broker_backed_entry_adoption"]
    assert remediation["classification"] == "BROKER_BACKED_ENTRIES_ADOPTION_REQUIRED"
    assert remediation["adoption_count"] == 2
    assert {item["contract"]["symbol"] for item in remediation["adoptions"]} == {"MGC", "MNQ"}
    assert {item["ownership_intent_id"] for item in remediation["adoptions"]} == {
        "submit_owner_test_mgc_1",
        "submit_owner_test_mnq_1",
    }
    assert all(item["broker_backed_evidence_valid"] is True for item in remediation["adoptions"])
    assert any(blocker["code"] == "SUBMIT_INTENT_BROKER_POSITIONS_ADOPTION_REQUIRED" for blocker in report["blockers"])
    assert not any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])


def test_restart_with_registry_backed_open_position_resumes_same_trade_id(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    trade_id = "trade_registry_restart_resume"
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id="life_restart_resume",
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    adoption = report["broker_backed_entry_adoption"]
    assert adoption["classification"] == "BROKER_BACKED_ENTRY_REGISTRY_RESUME_REQUIRED"
    assert adoption["adoptions"][0]["trade_id"] == trade_id
    events = _read_registry_events(config)
    assert events[-1]["event_type"] == "RECOVERY_ADOPTION_RECORDED"
    assert events[-1]["trade_id"] == trade_id


def test_broker_position_with_fill_evidence_but_no_registry_creates_recovery_adoption(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    record = _write_submit_intent_ownership(config, ownership_intent_id="submit_owner_mnq_recovery", symbol="MNQ", local_symbol="MNQM6", expiry="20260618", con_id=770561201)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    adoption = report["broker_backed_entry_adoption"]
    assert adoption["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REQUIRED"
    assert adoption["broker_backed_evidence_valid"] is True
    assert adoption["trade_id"] == record["extra"]["trade_id"]
    events = _read_registry_events(config)
    assert events[-1]["event_type"] == "RECOVERY_ADOPTION_RECORDED"
    assert events[-1]["trade_id"] == record["extra"]["trade_id"]


def test_broker_position_without_fill_evidence_is_recovery_review_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, exec_id=None)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    adoption = report["broker_backed_entry_adoption"]
    assert adoption["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REVIEW_REQUIRED"
    assert adoption["broker_backed_evidence_valid"] is False
    assert any(blocker["code"] == "REVIEW_REQUIRED_UNMANAGED_BROKER_EXPOSURE" for blocker in report["blockers"])
    assert any(event["event_type"] == "REVIEW_REQUIRED" for event in _read_registry_events(config))


def test_missing_submit_exec_id_resolves_from_bridge_execution_report_for_adoption(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    record = _write_submit_intent_ownership(
        config,
        ownership_intent_id="submit_owner_mnq_exec_late",
        symbol="MNQ",
        local_symbol="MNQM6",
        expiry="20260618",
        con_id=770561201,
        broker_order_id="1",
        client_id=11127,
        perm_id=1955790757,
        exec_id=None,
    )
    _write_bridge_execution_report(
        config,
        lane_id="atp_companion_v1_asia_us",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="1",
        client_id=11127,
        perm_id=1955790757,
        exec_id="0000e1a7.current.01.01",
        account_id="DUM882026",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    adoption = report["broker_backed_entry_adoption"]
    assert adoption["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REQUIRED"
    assert adoption["broker_backed_evidence_valid"] is True
    assert adoption["exec_id"] == "0000e1a7.current.01.01"
    assert adoption["trade_id"] == record["extra"]["trade_id"]
    assert adoption["fill_evidence_resolver"]["classification"] == "BROKER_BACKED_FILL_EVIDENCE_RESOLVED"
    events = _read_registry_events(config)
    assert [event["event_type"] for event in events[-2:]] == ["ENTRY_FILL_BROKER_BACKED", "RECOVERY_ADOPTION_RECORDED"]
    assert events[-2]["exec_id"] == "0000e1a7.current.01.01"
    assert report["post_fill_lifecycle_adoption"]["classification"] == "POST_FILL_LIFECYCLE_ADOPTION_APPLIED"
    live_status = json.loads(config.live_position_status_path.read_text(encoding="utf-8"))
    assert live_status["open_position_count"] == 1
    open_position = next(iter(live_status["positions_by_instrument"].values()))
    assert open_position["trade_id"] == record["extra"]["trade_id"]
    assert open_position["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"


def test_simultaneous_reused_order_id_fills_auto_adopt_by_exact_bridge_fill_price(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    mnq_record = _write_submit_intent_ownership(
        config,
        ownership_intent_id="submit_owner_mnq_globex_short",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        action="SELL",
        symbol="MNQ",
        local_symbol="MNQM6",
        expiry="20260618",
        con_id=770561201,
        broker_order_id="1",
        client_id=10851,
        perm_id=1955790772,
        exec_id=None,
        trade_id="trade_original_mnq_globex_short",
        created_at="2026-05-11T11:50:10+00:00",
        broker_effect_confirmed=True,
    )
    mes_record = _write_submit_intent_ownership(
        config,
        ownership_intent_id="submit_owner_mes_globex_short",
        lane_id="mes_globex_active_participation_short",
        strategy_id="mes_globex_active_participation_short",
        action="SELL",
        symbol="MES",
        local_symbol="MESM6",
        expiry="20260618",
        con_id=770561194,
        broker_order_id="1",
        client_id=10922,
        perm_id=1955790779,
        exec_id=None,
        trade_id="trade_original_mes_globex_short",
        created_at="2026-05-11T11:50:12+00:00",
        broker_effect_confirmed=True,
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_submit_owner_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_submit_owner_mes_globex_short",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_1",
        lane_id="mes_globex_active_participation_short",
        strategy_id="mes_globex_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
    )
    _write_bridge_execution_report(
        config,
        lane_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="1",
        client_id=10851,
        perm_id=1955790772,
        exec_id="0000e1a7.mnq.current.01.01",
        account_id="DUM882026",
        fill_price="30480.5",
        extra_executions=[
            {
                "execution_id": "0000e1a7.mnq.old.01.01",
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "quantity": 1,
                "side": "SLD",
                "price": "30436.75",
                "executed_at": "2026-05-11T11:59:13+00:00",
            },
            {
                "execution_id": "0000e1a7.mes.other.01.01",
                "account_id": "DUM882026",
                "symbol": "MES",
                "quantity": 1,
                "side": "SLD",
                "price": "7598.75",
                "executed_at": "2026-05-11T11:59:13+00:00",
            },
        ],
    )
    _write_bridge_execution_report(
        config,
        lane_id="mes_globex_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        order_id="1",
        client_id=10922,
        perm_id=1955790779,
        exec_id="0000e1a7.mes.current.01.01",
        account_id="DUM882026",
        fill_price="7598.75",
        extra_executions=[
            {
                "execution_id": "0000e1a7.mnq.other.01.01",
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "quantity": 1,
                "side": "SLD",
                "price": "30480.5",
                "executed_at": "2026-05-11T11:59:14+00:00",
            },
            {
                "execution_id": "0000e1a7.mes.old.01.01",
                "account_id": "DUM882026",
                "symbol": "MES",
                "quantity": 1,
                "side": "SLD",
                "price": "7590.75",
                "executed_at": "2026-05-11T11:59:14+00:00",
            },
        ],
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "-1",
            },
            {
                "account_id": "DUM882026",
                "symbol": "MES",
                "local_symbol": "MESM6",
                "expiry": "20260618",
                "con_id": 770561194,
                "security_type": "FUT",
                "quantity": "-1",
            },
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    adoption = report["broker_backed_entry_adoption"]
    assert adoption["classification"] == "BROKER_BACKED_ENTRIES_ADOPTION_REQUIRED"
    assert adoption["adoption_allowed"] is True
    assert {item["exec_id"] for item in adoption["adoptions"]} == {
        "0000e1a7.mnq.current.01.01",
        "0000e1a7.mes.current.01.01",
    }
    assert report["post_fill_lifecycle_adoption"]["classification"] == "POST_FILL_LIFECYCLE_ADOPTION_APPLIED"
    live_status = json.loads(config.live_position_status_path.read_text(encoding="utf-8"))
    assert live_status["open_position_count"] == 2
    trade_ids = {row["trade_id"] for row in live_status["positions_by_instrument"].values()}
    assert trade_ids == {mnq_record["extra"]["trade_id"], mes_record["extra"]["trade_id"]}

    followup = reconcile_track_b_paper_broker_truth(config=config, now=NOW)
    assert followup["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED", followup[
        "registry_reconciliation"
    ]["blockers"]
    assert set(followup["registry_reconciliation"]["mapped_trade_ids"]) == {
        mnq_record["extra"]["trade_id"],
        mes_record["extra"]["trade_id"],
    }


def test_ambiguous_registry_trade_ids_block_recovery_adoption(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    for trade_id in ("trade_recovery_ambiguous_a", "trade_recovery_ambiguous_b"):
        _write_registry_open_managed_trade(
            config,
            trade_id=trade_id,
            lifecycle_id=f"life_{trade_id}",
            lane_id="mnq_us_active_participation_long",
            strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            symbol="MNQ",
            local_symbol="MNQM6",
            con_id=770561201,
            expiry="20260618",
        )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_backed_entry_adoption"]["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REVIEW_REQUIRED"
    assert any(
        blocker["code"] == "REGISTRY_AMBIGUOUS_BROKER_POSITION"
        for blocker in report["registry_reconciliation"]["blockers"]
    )


def test_lifecycle_owner_conflict_with_registry_blocks_recovery_adoption(tmp_path: Path) -> None:
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            "lane_id": "mnq_us_active_participation_long",
            "trade_id": "trade_lifecycle_conflict",
            "lifecycle_id": "life_actual",
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "expiry": "20260618",
            "side": "LONG",
            "quantity": "1",
        },
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_lifecycle_conflict",
        lifecycle_id="life_other",
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_backed_entry_adoption"]["classification"] == "BROKER_BACKED_ENTRY_ADOPTION_REVIEW_REQUIRED"
    assert any(
        blocker["code"] == "REGISTRY_LIFECYCLE_OPEN_WITHOUT_TRADE_ID_REVIEW_REQUIRED"
        for blocker in report["registry_reconciliation"]["blockers"]
    )


def test_closed_historical_registry_record_does_not_adopt_current_broker_position(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_closed_history_no_adopt",
        lifecycle_id="life_closed_history_no_adopt",
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10851",
        entry_perm_id="1955790772",
        entry_exec_id="0000e1a7.6a2d1b84.01.01",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_backed_entry_adoption"] is None
    assert any(
        blocker["code"] == "REGISTRY_ORPHAN_BROKER_POSITION_REVIEW_REQUIRED"
        for blocker in report["registry_reconciliation"]["blockers"]
    )


def test_closed_flat_registry_supersedes_stale_open_lifecycle_projection(tmp_path: Path) -> None:
    trade_id = "trade_closed_flat_supersedes_stale_open"
    lifecycle_id = "life_closed_flat_supersedes_stale_open"
    open_position = {
        "strategy_id": "mes_globex_active_participation_long",
        "lane_id": "mes_globex_active_participation_long",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "instrument_family": "MES",
        "contract_key": "MES-202606",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "final_position_status": "OPEN_MANAGED",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mes_globex_active_participation_long",
        strategy_id="mes_globex_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10922",
        entry_perm_id="1955790779",
        entry_exec_id="0000e1a7.6a2d1b87.01.01",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["track_b_broker_position_count"] == 0
    assert report["track_b_broker_open_order_count"] == 0
    assert report["track_b_lifecycle_positions"] == []
    assert report["current_scope_review_required_count"] == 0
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    superseded = report["superseded_lifecycle_projections"]
    assert len(superseded) == 1
    assert superseded[0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"
    assert superseded[0]["trade_id"] == trade_id


def test_evidence_gated_broker_flat_cleanup_supersedes_stale_open_lifecycle_projection(tmp_path: Path) -> None:
    trade_id = "trade_696f40f3-5a3a-4166-b11e-fb59401c50ed"
    lifecycle_id = "reserved_submit_mes_us_active_participation_short_20260602T185428677911Z_0c5caf5f40f7"
    open_position = {
        "strategy_id": "mes_us_active_participation_short",
        "lane_id": "mes_us_active_participation_short",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "instrument_family": "MES",
        "contract_key": "MES-M6",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "SHORT",
        "quantity": "1",
        "final_position_status": "OPEN_MANAGED",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mes_us_active_participation_short",
        strategy_id="mes_us_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        include_close_fill=False,
        order_id="2",
        client_id="10973",
        entry_perm_id="665735662",
        entry_exec_id="0000e1a7.6a2e7d30.01.01",
    )
    append_live_trade_registry_event(
        repo_root=config.repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id="mes_us_active_participation_short",
            thesis_strategy_id="mes_us_active_participation_short",
            account_id="DUM882026",
            symbol="MES",
            con_id=770561194,
            local_symbol="MESM6",
            expiry="20260618",
            side="SHORT",
            action="HISTORICAL_FLAT_CLEANUP",
            qty="1",
            source_artifact_path=str(config.report_path),
            generated_at=NOW + timedelta(seconds=5),
            reason_codes=(
                "HISTORICAL_SUBMIT_INTENT_RESOLVED_FLAT",
                "BROKER_FLAT_PROOF_CONFIRMED",
                "NO_OPEN_ORDER_PROOF_CONFIRMED",
                "NOT_CURRENT_EXPOSURE",
                "NOT_CURRENT_OPEN_ORDER",
            ),
            metadata={
                "historical_only": True,
                "not_current_exposure": True,
                "not_current_open_order": True,
                "broker_flat_proof_path": str(config.broker_truth_root / "ibkr_positions_snapshot.json"),
                "open_orders_proof_path": str(config.broker_truth_root / "ibkr_open_orders_snapshot.json"),
            },
        ),
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW + timedelta(seconds=6))

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["track_b_lifecycle_positions"] == []
    superseded = report["superseded_lifecycle_projections"]
    assert superseded[0]["classification"] == "STALE_SUPERSEDED_LIFECYCLE_PROJECTION"
    assert superseded[0]["terminal_registry_truth"]["classification"] == "BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL"
    assert superseded[0]["terminal_registry_truth"]["broker_backed_exit"] is False


def test_terminal_registry_truth_supersedes_post_close_review_noise(tmp_path: Path) -> None:
    trade_id = "trade_terminal_review_noise"
    lifecycle_id = "life_terminal_review_noise"
    open_position = {
        "strategy_id": "mes_globex_active_participation_long",
        "lane_id": "mes_globex_active_participation_long",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "instrument_family": "MES",
        "contract_key": "MES-202606",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "final_position_status": "OPEN_MANAGED",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mes_globex_active_participation_long",
        strategy_id="mes_globex_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="11192",
        entry_perm_id="665735640",
        entry_exec_id="0000e1a7.6a2d8658.01.01",
        exit_order_id="63",
        exit_perm_id="665735642",
        exit_exec_id="0000e1a7.6a2d8f85.01.01",
    )
    append_live_trade_registry_event(
        repo_root=config.repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.REVIEW_REQUIRED,
            generated_at=NOW + timedelta(seconds=10),
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id="mes_globex_active_participation_long",
            thesis_strategy_id="mes_globex_active_participation_long",
            account_id="DUM882026",
            symbol="MES",
            con_id=770561194,
            local_symbol="MESM6",
            expiry="20260618",
            side="LONG",
            action="RECONCILE",
            qty="1",
            source_artifact_path=str(config.report_path),
            reason_codes=("REGISTRY_RECONCILIATION_REVIEW_REQUIRED",),
        ),
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["track_b_lifecycle_positions"] == []
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    superseded = report["superseded_lifecycle_projections"]
    assert superseded[0]["trade_id"] == trade_id
    assert "BROKER_BACKED_EXIT_EVIDENCE_CONFIRMED" in superseded[0]["reason_codes"]


def test_stale_open_lifecycle_projection_without_close_evidence_still_blocks(tmp_path: Path) -> None:
    trade_id = "trade_missing_close_evidence_still_blocks"
    lifecycle_id = "life_missing_close_evidence_still_blocks"
    open_position = {
        "strategy_id": "mes_globex_active_participation_long",
        "lane_id": "mes_globex_active_participation_long",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "instrument_family": "MES",
        "contract_key": "MES-202606",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "final_position_status": "OPEN_MANAGED",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mes_globex_active_participation_long",
        strategy_id="mes_globex_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is False
    assert report["track_b_lifecycle_positions"]
    assert report["superseded_lifecycle_projections"] == []
    assert any(
        blocker["code"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
        for blocker in report["blockers"]
    )


def test_closed_flat_registry_does_not_supersede_current_broker_exposure(tmp_path: Path) -> None:
    trade_id = "trade_closed_registry_broker_still_open"
    lifecycle_id = "life_closed_registry_broker_still_open"
    open_position = {
        "strategy_id": "mes_globex_active_participation_long",
        "lane_id": "mes_globex_active_participation_long",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "instrument_family": "MES",
        "contract_key": "MES-202606",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "final_position_status": "OPEN_MANAGED",
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mes_globex_active_participation_long",
        strategy_id="mes_globex_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10922",
        entry_perm_id="1955790779",
        entry_exec_id="0000e1a7.6a2d1b87.01.01",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MES",
                "local_symbol": "MESM6",
                "expiry": "20260618",
                "con_id": 770561194,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is False
    assert report["track_b_broker_position_count"] == 1
    assert report["track_b_lifecycle_positions"]
    assert report["superseded_lifecycle_projections"] == []
    assert any(
        blocker["code"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
        for blocker in report["blockers"]
    )


def test_duplicate_lifecycle_only_mnq_chain_superseded_by_broker_backed_closed_flat_chain(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_lifecycle_only_open_trade(
        config,
        trade_id="trade_original_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="202606",
        order_id="1",
        client_id="10851",
        perm_id="1955790772",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_def5fb2c-ddce-4428-83b7-c906119225d5",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260601T220616753074Z_420c66bf3c42",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10851",
        entry_perm_id="1955790772",
        entry_exec_id="0000e1a7.6a2d1b84.01.01",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert report["current_scope_review_required_count"] == 0
    superseded = report["registry_reconciliation"]["superseded_lifecycle_only_records"]
    assert len(superseded) == 1
    assert superseded[0]["classification"] == "DUPLICATE_SUPERSEDED_FULL_AUDIT_ONLY"
    assert superseded[0]["trade_id"] == "trade_original_mnq_globex_short"
    assert superseded[0]["superseding_trade_id"] == "trade_def5fb2c-ddce-4428-83b7-c906119225d5"


def test_duplicate_lifecycle_only_mes_chain_superseded_by_broker_backed_closed_flat_chain(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_lifecycle_only_open_trade(
        config,
        trade_id="trade_original_mes_globex_short",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_1",
        lane_id="mes_globex_active_participation_short",
        strategy_id="mes_globex_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="202606",
        order_id="1",
        client_id="10922",
        perm_id="1955790779",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_3018ab96-7608-4e40-877a-ed0d7b995184",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_20260601T220620676808Z_e3785d1f3061",
        lane_id="mes_globex_active_participation_short",
        strategy_id="mes_globex_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10922",
        entry_perm_id="1955790779",
        entry_exec_id="0000e1a7.6a2d1b87.01.01",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    superseded = report["registry_reconciliation"]["superseded_lifecycle_only_records"]
    assert len(superseded) == 1
    assert superseded[0]["classification"] == "DUPLICATE_SUPERSEDED_FULL_AUDIT_ONLY"
    assert superseded[0]["trade_id"] == "trade_original_mes_globex_short"
    assert superseded[0]["superseding_trade_id"] == "trade_3018ab96-7608-4e40-877a-ed0d7b995184"


def test_broker_backed_duplicate_submit_owner_chain_superseded_by_closed_flat_chain(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_submit_owner_mes_globex_short",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_1",
        lane_id="mes_globex_active_participation_short",
        strategy_id="mes_globex_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="202606",
        order_id="1",
        client_id="10922",
        entry_perm_id="1955790779",
        entry_exec_id="0000e1a7.mes.current.01.01",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_3018ab96-7608-4e40-877a-ed0d7b995184",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_20260601T220620676808Z_e3785d1f3061",
        lane_id="mes_globex_active_participation_short",
        strategy_id="mes_globex_active_participation_short",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10922",
        entry_perm_id="1955790779",
        entry_exec_id="0000e1a7.6a2d1b87.01.01",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert report["current_scope_review_required_count"] == 0
    superseded = report["registry_reconciliation"]["superseded_lifecycle_only_records"]
    assert len(superseded) == 1
    assert superseded[0]["classification"] == "DUPLICATE_SUPERSEDED_FULL_AUDIT_ONLY"
    assert superseded[0]["trade_id"] == "trade_submit_owner_mes_globex_short"
    assert superseded[0]["broker_backed_entry"] is True
    assert superseded[0]["superseding_trade_id"] == "trade_3018ab96-7608-4e40-877a-ed0d7b995184"


def test_open_registry_row_suppressed_by_scoped_remediation_terminal_proof(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    trade_id = "trade_bb50eab2-ca16-460d-98ff-21a9ada8083d"
    lifecycle_id = "reserved_submit_mes_us_active_participation_long_20260602T185515869214Z_feff416a9245"
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mes_us_active_participation_long",
        strategy_id="mes_us_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        order_id="2",
        client_id="10844",
        entry_perm_id="665735688",
        entry_exec_id="0000e1a7.6a2e7d49.01.01",
    )
    _write_jsonl(
        config.ledger_root / "track_b_paper_trade_ledger.jsonl",
        [
            {
                "ledger_schema_version": "track_b_paper_trade_ledger_v1",
                "record_type": "ARTIFACT_RECONCILIATION",
                "trade_id": f"{trade_id}:duplicate_exit_overfill_scoped_remediation_review",
                "lifecycle_id": lifecycle_id,
                "strategy_id": "mes_us_active_participation_long",
                "instrument_family": "MES",
                "contract_key": "MES-202606",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "account_id": "DUM882026",
                "reconciliation_action": "DUPLICATE_EXIT_OVERFILL_SCOPED_REMEDIATION_REVIEWED",
                "new_artifact_classification": "DUPLICATE_EXIT_OVERFILL_SCOPED_REMEDIATION_REVIEWED",
                "final_position_status": "DUPLICATE_EXIT_OVERFILL_SCOPED_REMEDIATION_REVIEWED",
                "broker_reconciled": True,
                "historical_broker_backed_exposure_confirmed": True,
                "remediation_broker_order_id": "7",
                "remediation_perm_id": "665807058",
                "remediation_execution_id": "0000e1a7.6a2eb979.01.01",
                "remediation_fill_time": "2026-06-03T00:41:49.494075+00:00",
                "remediation_fill_price": "7625.0",
                "broker_flat": True,
                "open_orders_zero": True,
                "guardian_remediation_artifact_path": str(
                    config.repo_root
                    / "outputs/track_b_execution_core/broker_position_guardian/latest_scoped_guardian_remediation_filled_normalized.json"
                ),
                "broker_positions_snapshot_path": str(config.broker_truth_root / "ibkr_positions_snapshot.json"),
                "broker_open_orders_snapshot_path": str(config.broker_truth_root / "ibkr_open_orders_snapshot.json"),
                "source": "BROKER_POSITION_GUARDIAN_SCOPED_REMEDIATION_FILLED_AND_BROKER_FLAT_TRUTH",
                "created_at": NOW.isoformat(),
            }
        ],
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert report["current_scope_review_required_count"] == 0
    superseded = report["registry_reconciliation"]["superseded_lifecycle_only_records"]
    assert len(superseded) == 1
    assert superseded[0]["classification"] == "BROKER_FLAT_EVIDENCE_GATED_CLEANUP_TERMINAL_FULL_AUDIT_ONLY"
    assert superseded[0]["trade_id"] == trade_id
    assert superseded[0]["broker_backed_entry"] is True
    assert superseded[0]["broker_backed_exit"] is False
    assert superseded[0]["remediation_terminal"]["remediation_execution_id"] == "0000e1a7.6a2eb979.01.01"


def test_stale_derived_registry_chain_without_current_broker_linkage_is_full_audit_only(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    source_path = config.repo_root / "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/reserved_submit_atp_companion_v1_asia_us_1/track_b_strategy_managed_paper_lifecycle_report.json"
    _write_json(
        source_path,
        {
            "generated_at": "2026-05-11T12:00:00+00:00",
            "trade_id": "trade_submit_owner_mnq_exec_late",
            "lifecycle_id": "reserved_submit_atp_companion_v1_asia_us_1",
            "final_position_status": "OPEN_MANAGED",
            "entry_fill": {
                "broker_order_id": "1",
                "execution_id": "0000e1a7.current.01.01",
                "filled_at": "2026-06-01T13:36:09+00:00",
                "perm_id": 1955790757,
                "price": "30437",
                "quantity": "1",
            },
            "close_fill": None,
        },
    )
    _write_registry_lifecycle_only_open_trade(
        config,
        trade_id="trade_submit_owner_mnq_exec_late",
        lifecycle_id="reserved_submit_atp_companion_v1_asia_us_1",
        lane_id="atp_companion_v1_asia_us",
        strategy_id="atp_companion_v1__benchmark_mgc_asia_us",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="202606",
        order_id="1",
        client_id="11127",
        perm_id="1955790757",
        exec_id="0000e1a7.current.01.01",
        source_artifact_path=str(source_path),
        generated_at=NOW,
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    superseded = report["registry_reconciliation"]["superseded_lifecycle_only_records"]
    assert len(superseded) == 1
    assert superseded[0]["classification"] == "STALE_DERIVED_REGISTRY_CHAIN_FULL_AUDIT_ONLY"
    assert superseded[0]["trade_id"] == "trade_submit_owner_mnq_exec_late"
    assert superseded[0]["stale_source"]["source_reports"][0]["impossible_timestamps"] is True


def test_current_broker_exposure_prevents_lifecycle_only_supersession(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_lifecycle_only_open_trade(
        config,
        trade_id="trade_original_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="202606",
        order_id="1",
        client_id="10851",
        perm_id="1955790772",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_def5fb2c-ddce-4428-83b7-c906119225d5",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260601T220616753074Z_420c66bf3c42",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        include_close_fill=True,
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "-1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["track_b_broker_position_count"] == 1
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert "trade_original_mnq_globex_short" in report["registry_reconciliation"]["mapped_trade_ids"]
    assert report["registry_reconciliation"]["superseded_lifecycle_only_records"] == []


def test_missing_broker_backed_exit_prevents_lifecycle_only_supersession(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_lifecycle_only_open_trade(
        config,
        trade_id="trade_original_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="202606",
        order_id="1",
        client_id="10851",
        perm_id="1955790772",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_def5fb2c-ddce-4428-83b7-c906119225d5",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260601T220616753074Z_420c66bf3c42",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        order_id="1",
        client_id="10851",
        entry_perm_id="1955790772",
        entry_exec_id="0000e1a7.6a2d1b84.01.01",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
    assert report["registry_reconciliation"]["superseded_lifecycle_only_records"] == []
    assert "trade_original_mnq_globex_short" in report["registry_reconciliation"]["review_required_trade_ids"]


def test_identity_mismatch_prevents_lifecycle_only_supersession(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_lifecycle_only_open_trade(
        config,
        trade_id="trade_original_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="202606",
        order_id="1",
        client_id="10851",
        perm_id="1955790772",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_def5fb2c-ddce-4428-83b7-c906119225d5",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260601T220616753074Z_420c66bf3c42",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQZ6",
        con_id=770561202,
        expiry="20260618",
        include_close_fill=True,
        order_id="1",
        client_id="10851",
        entry_perm_id="1955790772",
        entry_exec_id="0000e1a7.6a2d1b84.01.01",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
    assert report["registry_reconciliation"]["superseded_lifecycle_only_records"] == []
    assert "trade_original_mnq_globex_short" in report["registry_reconciliation"]["review_required_trade_ids"]


def test_current_broker_position_maps_to_stronger_broker_backed_lifecycle_row(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    old_lifecycle = {
        "account_id": "MULTIPLE",
        "trade_id": "trade_older_mes_globex_long",
        "lifecycle_id": "reserved_submit_mes_globex_active_participation_long_older",
        "strategy_id": "mes_globex_active_participation_long",
        "instrument_family": "MES",
        "contract_key": "MES-M6",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "entry_price": "7601.5",
        "entry_exec_id": "0000e1a7.older.01.01",
    }
    current_lifecycle = {
        "account_id": "DUM882026",
        "trade_id": "trade_current_mes_us_long",
        "lifecycle_id": "reserved_submit_mes_us_active_participation_long_current",
        "strategy_id": "mes_us_active_participation_long",
        "instrument_family": "MES",
        "contract_key": "MES-202606",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "entry_price": "7617.75",
        "entry_exec_id": "0000e1a7.6a2e7d49.01.01",
        "entry_perm_id": "665735688",
        "entry_order_id": "2",
    }
    _write_json(
        config.live_position_status_path,
        {
            "source": "TRACK_B_LIFECYCLE_ARTIFACTS",
            "broker_reconciled": False,
            "open_position_count": 2,
            "open_order_count": 0,
            "positions_by_instrument": {
                old_lifecycle["contract_key"]: old_lifecycle,
                current_lifecycle["contract_key"]: current_lifecycle,
            },
            "positions_by_strategy": {
                old_lifecycle["strategy_id"]: old_lifecycle,
                current_lifecycle["strategy_id"]: current_lifecycle,
            },
            "review_required_positions": [],
        },
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_older_mes_globex_long",
        lifecycle_id="reserved_submit_mes_globex_active_participation_long_older",
        lane_id="mes_globex_active_participation_long",
        strategy_id="mes_globex_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        order_id="1",
        client_id="11192",
        entry_perm_id="665735640",
        entry_exec_id="0000e1a7.older.01.01",
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_current_mes_us_long",
        lifecycle_id="reserved_submit_mes_us_active_participation_long_current",
        lane_id="mes_us_active_participation_long",
        strategy_id="mes_us_active_participation_long",
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        order_id="2",
        client_id="10844",
        entry_perm_id="665735688",
        entry_exec_id="0000e1a7.6a2e7d49.01.01",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MES",
                "local_symbol": "MESM6",
                "expiry": "20260618",
                "con_id": 770561194,
                "security_type": "FUT",
                "quantity": "1",
                "average_cost": "38089.37",
                "multiplier": "5",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["position_match_report"]["matched"] is True
    assert report["position_match_report"]["matches"][0]["lifecycle_position"]["trade_id"] == "trade_current_mes_us_long"
    assert report["position_match_report"]["superseded_unmatched_lifecycle_positions"][0]["trade_id"] == "trade_older_mes_globex_long"
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert report["registry_reconciliation"]["mapped_trade_ids"] == ["trade_current_mes_us_long"]


def test_managed_exit_after_recovery_uses_trade_id(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    record = _write_submit_intent_ownership(config, ownership_intent_id="submit_owner_mnq_exit_after_recovery", symbol="MNQ", local_symbol="MNQM6", expiry="20260618", con_id=770561201)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )
    reconcile_track_b_paper_broker_truth(config=config, now=NOW)
    phase1 = {
        "ready": True,
        "track_b_lifecycle_positions": [
            {
                "lifecycle_id": record["lifecycle_id"],
                "account_id": "MULTIPLE",
                "lane_id": record["lane_id"],
                "strategy_id": record["strategy_id"],
                "track_b_root": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "quantity": "1",
                "side": "LONG",
            }
        ],
        "track_b_broker_positions": [
            {"account_id": "DUM882026", "symbol": "MNQ", "local_symbol": "MNQM6", "con_id": 770561201, "quantity": "1"}
        ],
    }

    validation = validate_registry_managed_exit_identity(
        repo_root=config.repo_root,
        trade_id=record["extra"]["trade_id"],
        lifecycle_id=record["lifecycle_id"],
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        quantity=1,
        action="SELL",
        phase1_reconciliation_gate=phase1,
    )

    assert validation["allowed"] is True


def test_competing_submit_intents_block_review_required(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, broker_order_id="28")
    _write_submit_intent_ownership(config, broker_order_id="29")
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_COMPETING_UNRESOLVED_REVIEW_REQUIRED"
    assert any(blocker["code"] == "SUBMIT_INTENT_COMPETING_UNRESOLVED_REVIEW_REQUIRED" for blocker in report["blockers"])


def test_mismatched_submit_intent_does_not_anonymously_match_broker_position(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, action="SELL")
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED"
    assert any(blocker["code"] == "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED" for blocker in report["blockers"])


def test_stale_submit_intent_does_not_adopt_broker_only_position(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, created_at="2026-05-11T11:50:00+00:00")
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED"
    assert any(blocker["code"] == "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED" for blocker in report["blockers"])
    assert not any(blocker["code"] == "SUBMIT_INTENT_BROKER_POSITION_ADOPTION_REQUIRED" for blocker in report["blockers"])


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


def test_registry_aware_reconciliation_appends_reconciled_open_for_matching_trade_id(tmp_path: Path) -> None:
    trade_id = "trade_registry_open_mnq"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            "lane_id": "mnq_us_active_participation_long",
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "expiry": "20260618",
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "28981.25",
        },
    )
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert report["registry_reconciliation"]["mapped_trade_ids"] == [trade_id]
    events = _read_registry_events(config)
    assert events[-1]["event_type"] == "RECONCILED_OPEN"
    assert events[-1]["trade_id"] == trade_id


def test_registry_aware_reconciliation_appends_reconciled_flat_for_manual_close(tmp_path: Path) -> None:
    trade_id = "trade_registry_manual_flat_mnq"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        include_manual_close=True,
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    events = _read_registry_events(config)
    assert events[-1]["event_type"] == "RECONCILED_FLAT"
    assert events[-1]["trade_id"] == trade_id


def test_registry_aware_reconciliation_blocks_ambiguous_broker_position(tmp_path: Path) -> None:
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            "lane_id": "mnq_us_active_participation_long",
            "lifecycle_id": lifecycle_id,
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "expiry": "20260618",
            "side": "LONG",
            "quantity": "1",
        },
    )
    for trade_id in ("trade_registry_ambiguous_a", "trade_registry_ambiguous_b"):
        _write_registry_open_managed_trade(
            config,
            trade_id=trade_id,
            lifecycle_id=f"{lifecycle_id}_{trade_id}",
            lane_id="mnq_us_active_participation_long",
            strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            symbol="MNQ",
            local_symbol="MNQM6",
            con_id=770561201,
            expiry="20260618",
        )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is False
    registry = report["registry_reconciliation"]
    assert registry["classification"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
    assert any(blocker["code"] == "REGISTRY_AMBIGUOUS_BROKER_POSITION" for blocker in registry["blockers"])


def test_registry_aware_reconciliation_prefers_newest_exact_broker_backed_entry_when_lifecycle_is_missing(
    tmp_path: Path,
) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_stale_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        order_id="1",
        client_id="10851",
        entry_perm_id="1955790772",
        entry_exec_id="0000e1a7.mnq.current.01.01",
        generated_at=NOW,
    )
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_current_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_20260603T024821054865Z_6e2aaf2cc5da",
        lane_id="mnq_globex_active_participation_short",
        strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        order_id="2",
        client_id="10898",
        entry_perm_id="665807059",
        entry_exec_id="0000e1a7.6a2ecf0d.01.01",
        generated_at=NOW + timedelta(days=1),
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "security_type": "FUT",
                "quantity": "-1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW + timedelta(days=1, minutes=5))

    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    assert report["registry_reconciliation"]["mapped_trade_ids"] == ["trade_current_mnq_globex_short"]


def test_registry_aware_reconciliation_blocks_lifecycle_open_when_broker_flat(tmp_path: Path) -> None:
    trade_id = "trade_registry_lifecycle_only"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
    config = _write_base_artifacts(
        tmp_path,
        open_position={
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            "lane_id": "mnq_us_active_participation_long",
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
            "instrument_family": "MNQ",
            "contract_key": "MNQ-202606",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "expiry": "20260618",
            "side": "LONG",
            "quantity": "1",
        },
    )
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is False
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
    assert any(blocker["code"] == "TRACK_B_BROKER_LIFECYCLE_POSITION_COUNT_MISMATCH" for blocker in report["blockers"])
    assert any(event["event_type"] == "REVIEW_REQUIRED" for event in _read_registry_events(config))


def test_registry_aware_reconciliation_does_not_resurrect_stale_closed_trade(tmp_path: Path) -> None:
    trade_id = "trade_registry_closed_history"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        include_close_fill=True,
    )
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    assert report["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    events = _read_registry_events(config)
    assert events[-1]["event_type"] == "RECONCILED_FLAT"
    assert events[-1]["trade_id"] == trade_id
    assert not any(event["event_type"] == "RECONCILED_OPEN" for event in events if event["trade_id"] == trade_id)


def test_registry_stale_open_trade_can_be_resolved_only_with_explicit_historical_flat_cleanup(
    tmp_path: Path,
) -> None:
    trade_id = "trade_registry_historical_flat_cleanup"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN"
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_broker_truth(config)

    blocked = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert blocked["broker_reconciled"] is False
    assert any(
        blocker["code"] == "REGISTRY_RECONCILIATION_REVIEW_REQUIRED"
        for blocker in blocked["blockers"]
    )

    cleanup_payload = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": "mnq_us_active_participation_long",
        "thesis_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "20260618",
        "side": "LONG",
        "action": "HISTORICAL_FLAT_CLEANUP",
        "qty": "1",
        "source_artifact_path": str(config.report_path),
        "generated_at": NOW + timedelta(seconds=30),
        "reason_codes": (
            "REGISTRY_OPEN_TRADE_WITH_FLAT_BROKER_LIFECYCLE_REVIEW_REQUIRED",
            "BROKER_LIFECYCLE_FLAT_CONFIRMED",
            "NOT_CURRENT_EXPOSURE",
            "NOT_CURRENT_OPEN_ORDER",
        ),
        "metadata": {
            "not_current_exposure": True,
            "not_current_open_order": True,
            "broker_position_count": 0,
            "broker_open_order_count": 0,
            "lifecycle_position_count": 0,
            "cleanup_authority": "READ_ONLY_BROKER_LIFECYCLE_FLAT_EVIDENCE",
        },
    }
    append_live_trade_registry_event(
        repo_root=config.repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.RECONCILED_FLAT_HISTORICAL_CLEANUP,
            **cleanup_payload,
        ),
    )

    clean = reconcile_track_b_paper_broker_truth(config=config, now=NOW + timedelta(seconds=31))

    assert clean["broker_reconciled"] is True
    assert clean["registry_reconciliation"]["classification"] == "REGISTRY_RECONCILIATION_MATCHED"
    events = _read_registry_events(config)
    assert any(event["event_type"] == "RECONCILED_FLAT_HISTORICAL_CLEANUP" for event in events)
    assert events[-1]["event_type"] == "RECONCILED_FLAT"


def test_registry_review_event_is_not_broker_backed_without_perm_and_exec_id(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_registry_open_managed_trade(
        config,
        trade_id="trade_registry_orphan",
        lifecycle_id="bridge_fill_MNQ|1m|2026-05-12T17:34:00Z|BUY_TO_OPEN",
        lane_id="mnq_us_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
    )
    _write_broker_truth(config)

    reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    review_events = [event for event in _read_registry_events(config) if event["event_type"] == "REVIEW_REQUIRED"]
    assert review_events
    assert all(not event.get("perm_id") and not event.get("exec_id") for event in review_events)


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
    assert report["open_order_truth"]["classification"] == "SUSPICIOUS_ORDER_STATE"
    assert report["open_order_truth"]["summary"]["suspicious_order_count"] == 1


def test_duplicate_close_order_uses_open_order_truth_evidence(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
        open_orders=[
            {
                "order_id": 27,
                "perm_id": 347068546,
                "action": "SELL",
                "quantity": "1",
                "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "security_type": "FUT"},
            },
            {
                "order_id": 28,
                "perm_id": 347068547,
                "action": "SELL",
                "quantity": "1",
                "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "security_type": "FUT"},
            },
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["open_order_truth_classification"] == "DUPLICATE_CLOSE_ORDER"
    assert report["open_order_truth"]["summary"]["duplicate_close_order_group_count"] == 1
    assert any(blocker["code"] == "DUPLICATE_CLOSE_ORDER" for blocker in report["blockers"])


def test_suspicious_sentinel_order_is_visible_from_open_order_truth(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "security_type": "FUT",
                "quantity": "1",
            }
        ],
        open_orders=[
            {
                "order_id": 27,
                "perm_id": 347068546,
                "action": "SELL",
                "quantity": "1",
                "filled_quantity": "1.7976931348623157e+308",
                "remaining_quantity": None,
                "contract": {"symbol": "MNQ", "local_symbol": "MNQM6", "security_type": "FUT"},
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["open_order_truth_classification"] == "SUSPICIOUS_ORDER_STATE"
    order_state = report["open_order_truth"]["order_states"][0]
    assert "sentinel_filled_quantity" in order_state["suspicious_reasons"]
    assert "missing_remaining_quantity" in order_state["suspicious_reasons"]
    assert any(blocker["code"] == "SUSPICIOUS_ORDER_STATE" for blocker in report["blockers"])


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
    assert report["open_order_truth_classification"] == "OPEN_CLOSE_ORDER_WORKING"
    assert report["open_order_truth"]["summary"]["working_close_order_count"] == 1
    assert report["blockers"] == []
    reconciled_position = json.loads(config.reconciled_live_position_status_path.read_text(encoding="utf-8"))
    assert reconciled_position["known_managed_exit_orders"][0]["broker_order_id"] == "1"


def test_lifecycle_report_known_managed_exit_order_is_not_unknown_open_order_blocker(tmp_path: Path) -> None:
    lifecycle_path = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "track_b_strategy_managed_paper_lifecycle"
        / "reserved_submit_mgc"
        / "track_b_strategy_managed_paper_lifecycle_report.json"
    )
    open_position = {
        "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_short",
        "lifecycle_id": "reserved_submit_mgc",
        "instrument_family": "MGC",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "side": "SHORT",
        "quantity": "-1",
        "avg_entry_price": "4564.3",
        "paper_lifecycle_report_path": str(lifecycle_path),
    }
    config = _write_base_artifacts(tmp_path, open_position=open_position)
    _write_json(
        lifecycle_path,
        {
            "known_managed_exit_orders": [
                {
                    "managed_order_status": "WORKING",
                    "lifecycle_id": "reserved_submit_mgc",
                    "strategy_id": "gc_mgc_forced_session_baseline_v2__mgc_1x_all_lanes__asia_early_short",
                    "lane_id": "mgc_1x_all_lanes__asia_early_short",
                    "broker_order_id": "38",
                    "client_id": 17086,
                    "symbol": "MGC",
                    "local_symbol": "MGCM6",
                    "con_id": 712565978,
                    "action": "BUY",
                    "quantity": "1",
                }
            ]
        },
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "security_type": "FUT",
                "quantity": "-1",
                "average_cost": "45643.0",
                "multiplier": "10",
                "con_id": 712565978,
            }
        ],
        open_orders=[
            {
                "broker_order_id": "38",
                "client_id": 17086,
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "security_type": "FUT",
                "expiry": "20260626",
                "con_id": 712565978,
                "action": "BUY",
                "order_type": "LMT",
                "limit_price": "4568.6",
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
    assert report["open_order_truth_classification"] == "OPEN_CLOSE_ORDER_WORKING"
    assert report["known_managed_exit_orders"][0]["source"] == "TRACK_B_LIFECYCLE_REPORT_KNOWN_MANAGED_EXIT_ORDER"


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


def test_unresolved_submit_intent_without_broker_effect_waits_inside_window(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, created_at="2026-05-11T11:59:00+00:00")
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "SUBMIT_INTENT_NO_BROKER_EFFECT_PENDING_SETTLEMENT"
    assert report["broker_reconciled"] is False
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_NO_BROKER_EFFECT_PENDING_SETTLEMENT"
    assert report["blockers"] == []


def test_unresolved_submit_intent_without_broker_effect_times_out_resolves_as_historical_flat(
    tmp_path: Path,
) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, created_at="2026-05-11T11:50:00+00:00")
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_OWNERSHIP_NOT_APPLICABLE"
    assert report["historical_reconciliation_debris_resolution"]["classification"] == "HISTORICAL_RECONCILIATION_DEBRIS_RESOLVED"
    assert report["blockers"] == []
    events = _read_registry_events(config)
    assert any(event["event_type"] == "RECONCILED_FLAT_HISTORICAL_CLEANUP" for event in events)
    latest_ownership = json.loads(
        (config.repo_root / "outputs/track_b_execution_core/submit_intent_ownership/latest_track_b_submit_intent_ownership.json").read_text(
            encoding="utf-8"
        )
    )
    assert latest_ownership["unresolved_count"] == 0


def test_unsafe_unresolved_submit_intent_blocks_reconciliation(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    record = _write_submit_intent_ownership(config, created_at="2026-05-11T11:59:00+00:00")
    path = config.repo_root / "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl"
    tampered = dict(record)
    tampered["live_money_eligible"] = True
    path.write_text(json.dumps(tampered, sort_keys=True) + "\n", encoding="utf-8")
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED"
    assert any(blocker["code"] == "SUBMIT_INTENT_IDENTITY_MISMATCH_REVIEW_REQUIRED" for blocker in report["blockers"])


def test_historical_debris_resolver_never_cleans_current_exposure(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, created_at="2026-05-11T11:50:00+00:00", symbol="MNQ", local_symbol="MNQM6", con_id=770561201)
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "expiry": "20260618",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is False
    assert report["historical_reconciliation_debris_resolution"]["classification"] == "HISTORICAL_RECONCILIATION_DEBRIS_BLOCKED"
    assert "CURRENT_EXPOSURE_OR_OPEN_ORDER_PRESENT" in report["historical_reconciliation_debris_resolution"]["reason_codes"]
    latest_ownership = json.loads(
        (config.repo_root / "outputs/track_b_execution_core/submit_intent_ownership/latest_track_b_submit_intent_ownership.json").read_text(
            encoding="utf-8"
        )
    )
    assert latest_ownership["unresolved_count"] == 1


def test_historical_debris_resolver_never_cleans_open_order(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, created_at="2026-05-11T11:50:00+00:00", symbol="MNQ", local_symbol="MNQM6", con_id=770561201)
    _write_broker_truth(
        config,
        positions=[],
        open_orders=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "order_id": "28",
                "action": "BUY",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is False
    assert report["historical_reconciliation_debris_resolution"]["classification"] == "HISTORICAL_RECONCILIATION_DEBRIS_BLOCKED"
    assert "CURRENT_EXPOSURE_OR_OPEN_ORDER_PRESENT" in report["historical_reconciliation_debris_resolution"]["reason_codes"]


def test_historical_debris_resolver_prefers_exact_execution_evidence(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    record = _write_submit_intent_ownership(
        config,
        created_at="2026-05-11T11:50:00+00:00",
        symbol="MNQ",
        local_symbol="MNQM6",
        expiry="20260618",
        con_id=770561201,
        broker_order_id="101",
        client_id=17086,
        perm_id=2047276405,
        exec_id=None,
        trade_id="trade_exact_exec_historical",
        ownership_intent_id="submit_owner_exact_exec_historical",
    )
    _write_bridge_execution_report(
        config,
        lane_id="atp_companion_v1_asia_us",
        symbol="MNQ",
        local_symbol="MNQM6",
        con_id=770561201,
        order_id="101",
        client_id=17086,
        perm_id=2047276405,
        exec_id="exec-historical-1",
        account_id="DUM882026",
    )
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    item = report["historical_reconciliation_debris_resolution"]["resolved_items"][0]
    assert item["trade_id"] == record["extra"]["trade_id"]
    assert item["broker_fill_evidence_classification"] == "BROKER_BACKED_FILL_EVIDENCE_RESOLVED"
    events = _read_registry_events(config)
    event_types = [event["event_type"] for event in events]
    assert "ENTRY_FILL_BROKER_BACKED" in event_types
    assert "RECONCILED_FLAT_HISTORICAL_CLEANUP" in event_types


def test_historical_lifecycle_review_debris_clears_current_hot_path_reconciliation(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path, review_required_count=1)
    managed_path = config.repo_root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    _write_json(
        managed_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "REVIEW_REQUIRED",
            "managed_positions": [
                {
                    "classification": "REVIEW_REQUIRED",
                    "review_required_position": {
                        "trade_id": "trade_old_review",
                        "lifecycle_id": "old_lifecycle",
                        "account_id": "DUM882026",
                        "instrument_family": "MGC",
                        "symbol": "MGC",
                        "local_symbol": "MGCM6",
                        "con_id": 712565978,
                        "quantity": 1,
                        "side": "LONG",
                        "strategy_id": "old_strategy",
                        "generated_at": "2026-05-01T12:00:00+00:00",
                        "final_position_status": "REVIEW_REQUIRED",
                    },
                }
            ],
        },
    )
    _write_broker_truth(config, positions=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["review_required_count"] == 0
    assert report["current_scope_review_required_count"] == 0
    assert report["historical_review_required_count"] == 1
    assert report["raw_review_required_count"] == 1
    assert report["historical_reconciliation_debris_resolution"]["classification"] == "HISTORICAL_RECONCILIATION_DEBRIS_RESOLVED"
    assert not any(blocker["code"] == "LIFECYCLE_REVIEW_REQUIRED_PRESENT" for blocker in report["blockers"])


def test_unknown_open_order_blocks_even_with_matching_submit_intent(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_submit_intent_ownership(config, created_at="2026-05-11T11:59:00+00:00")
    _write_broker_truth(
        config,
        positions=[],
        open_orders=[
            {
                "account_id": "DUM882026",
                "symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
                "con_id": 712565978,
                "order_id": "99",
                "action": "BUY",
                "quantity": "1",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    assert any(blocker["code"] == "UNKNOWN_BROKER_OPEN_ORDER" for blocker in report["blockers"])
    assert report["submit_intent_ownership_reconciliation"]["classification"] == "SUBMIT_INTENT_OPEN_ORDER_AMBIGUITY"


def test_cancelled_known_close_order_absent_from_open_orders_reconciles_flat(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path)
    _write_json(
        config.repo_root / "outputs/track_b_execution_core/managed_exit_orders/latest_known_managed_exit_orders.json",
        {
            "schema_version": "track_b_known_managed_exit_orders_v1",
            "known_managed_exit_orders": [
                {
                    "source": "GUARDED_TRACK_B_PAPER_CANCEL_REPLACE_ONLY",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "order_id": "13",
                    "broker_order_id": "13",
                    "perm_id": "1784491840",
                    "action": "SELL",
                    "quantity": "1",
                    "managed_order_status": "Cancelled",
                    "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-20T12:25:00Z|BUY_TO_OPEN",
                }
            ],
            "resolved_known_managed_exit_orders": [],
        },
    )
    _write_broker_truth(config, positions=[], open_orders=[])

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["broker_reconciled"] is True
    assert report["track_b_broker_open_order_count"] == 0
    assert report["known_managed_exit_order_count"] == 0
    assert not any(blocker.get("code") == "UNKNOWN_BROKER_OPEN_ORDER" for blocker in report["blockers"])


def test_reconciliation_module_has_no_broker_mutation_symbols() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_paper_broker_reconciliation.py").read_text(encoding="utf-8")
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "IbkrClient" not in source
    assert "IbkrSession" not in source


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


def test_lifecycle_review_summary_debris_resolves_when_broker_lifecycle_flat(tmp_path: Path) -> None:
    config = _write_base_artifacts(tmp_path, review_required_count=1)
    _write_broker_truth(config)

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["blockers"] == []
    assert report["broker_reconciled"] is True
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["review_required_count"] == 0
    assert report["current_scope_review_required_count"] == 0
    assert report["historical_review_required_count"] == 1
    assert report["historical_reconciliation_debris_resolution"]["classification"] == "HISTORICAL_RECONCILIATION_DEBRIS_RESOLVED"


def test_old_review_trade_does_not_block_matched_current_open_position(tmp_path: Path) -> None:
    old_review = {
        "trade_id": "mnq_us_active_participation_long:old_lifecycle",
        "lifecycle_id": "old_lifecycle",
        "strategy_id": "mnq_us_active_participation_long",
        "instrument_family": "MNQ",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "LONG",
        "quantity": "1",
        "review_required": True,
        "final_position_status": "REVIEW_REQUIRED",
    }
    current_short = {
        "strategy_id": "mnq_globex_active_participation_short",
        "lane_id": "mnq_globex_active_participation_short",
        "trade_id": "trade_current_short",
        "trade_ids": ["trade_current_short"],
        "lifecycle_id": "current_short_lifecycle",
        "lifecycle_ids": ["current_short_lifecycle"],
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "SHORT",
        "quantity": "1",
        "avg_entry_price": "30480.5",
    }
    config = _write_base_artifacts(
        tmp_path,
        review_required_count=1,
        open_position=current_short,
        recent_trades=[old_review],
    )
    _write_broker_truth(
        config,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "con_id": 770561201,
                "security_type": "FUT",
                "quantity": "-1",
                "average_cost": "60960.38",
                "multiplier": "2",
            }
        ],
    )

    report = reconcile_track_b_paper_broker_truth(config=config, now=NOW)

    assert report["broker_reconciled"] is True
    assert report["classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert report["review_required_count"] == 0
    assert report["current_scope_review_required_count"] == 0
    assert report["historical_review_required_count"] == 1
    assert not any(blocker["code"] == "LIFECYCLE_REVIEW_REQUIRED_PRESENT" for blocker in report["blockers"])


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
        managed_order_registry_path=tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "managed_orders"
        / "latest_managed_orders.json",
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
    _write_json(
        config.managed_order_registry_path,
        {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS",
            "summary": {
                "managed_order_count": 0,
                "working_close_order_count": 0,
                "suspicious_order_count": 0,
                "duplicate_close_order_count": 0,
            },
            "projection_only": False,
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


def _write_submit_intent_ownership(
    config: ReconciliationConfig,
    *,
    broker_order_id: str = "28",
    action: str = "BUY",
    created_at: str = "2026-05-11T11:59:00+00:00",
    live_money_eligible: bool = False,
    paper_proof_invoked: bool = False,
    lane_id: str = "atp_companion_v1_asia_us",
    strategy_id: str = "atp_companion_v1__benchmark_mgc_asia_us",
    symbol: str = "MGC",
    local_symbol: str = "MGCM6",
    expiry: str = "20260626",
    con_id: int = 712565978,
    client_id: int = 11940,
    perm_id: int = 614044377,
    exec_id: str | None = "exec-1",
    trade_id: str | None = None,
    ownership_intent_id: str | None = None,
    broker_effect_confirmed: bool = False,
) -> dict[str, object]:
    parsed_created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    resolved_ownership_intent_id = ownership_intent_id or f"submit_owner_test_{broker_order_id}"
    resolved_trade_id = trade_id or f"trade_{resolved_ownership_intent_id}"
    extra = {
        "reason": "LEAK_TEST_ENTRY",
        "delegated_classification": "PAPER_ORDER_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW",
        "bridge_classification": "PAPER_STRATEGY_NEEDS_MANUAL_REVIEW",
        "trade_id": resolved_trade_id,
        "caller_metadata": {
            "trade_id": resolved_trade_id,
            "lane_id": lane_id,
            "strategy_id": strategy_id,
            "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        },
    }
    if broker_effect_confirmed:
        extra.update(
            {
                "broker_effect_classification": "BROKER_EFFECT_CONFIRMED",
                "bridge_classification": "PAPER_STRATEGY_ORDER_FILLED",
                "delegated_status": "filled",
            }
        )
    if exec_id is not None:
        extra["exec_id"] = exec_id
    state = (
        SubmitIntentOwnershipState.BROKER_POSITION_OBSERVED_ADOPTION_REQUIRED
        if broker_effect_confirmed
        else SubmitIntentOwnershipState.BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED
    )
    record = SubmitIntentOwnershipRecord(
        mode="PAPER",
        account_id="DUM882026",
        lane_id=lane_id,
        strategy_id=strategy_id,
        intent_type="BUY_TO_OPEN" if action == "BUY" else "SELL_TO_OPEN",
        action=action,
        symbol=symbol,
        local_symbol=local_symbol,
        expiry=expiry,
        con_id=con_id,
        qty=1,
        order_type="LMT",
        limit_price="4543.2",
        time_in_force="DAY",
        repo_root=str(config.repo_root),
        git_head="abc123",
        created_at=parsed_created_at,
        state=state,
        ownership_intent_id=resolved_ownership_intent_id,
        lifecycle_id=f"reserved_submit_{lane_id}_{broker_order_id}",
        lifecycle_id_reserved_only=True,
        lifecycle_position_open=False,
        caller_path="track_b_paper_leak_test_apply",
        caller_type="track_b_paper_leak_test",
        execution_price_source="RUNTIME_DATABENTO_1M_CLOSE",
        runtime_reference_price="4543.1",
        pre_submit_reconciliation_classification="PHASE1_BROKER_RECONCILIATION_CLEAR",
        governance_classification="PAPER_STRATEGY_GOVERNANCE_READY",
        exposure_classification="PAPER_EXPOSURE_ENTRY_ALLOWED",
        open_order_count=0,
        unknown_open_order_count=0,
        review_required_count=0,
        live_money_eligible=live_money_eligible,
        paper_proof_invoked=paper_proof_invoked,
        broker_order_id=broker_order_id,
        client_id=client_id,
        perm_id=perm_id,
        extra=extra,
    )
    result = append_submit_intent_ownership_record(
        record,
        jsonl_path=config.repo_root / "outputs/track_b_execution_core/submit_intent_ownership/track_b_submit_intent_ownership.jsonl",
        latest_path=config.repo_root / "outputs/track_b_execution_core/submit_intent_ownership/latest_track_b_submit_intent_ownership.json",
    )
    return result.record


def _write_registry_open_managed_trade(
    config: ReconciliationConfig,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    strategy_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    expiry: str,
    include_close_fill: bool = False,
    include_manual_close: bool = False,
    order_id: str = "101",
    client_id: str = "17086",
    entry_perm_id: str = "2047276405",
    entry_exec_id: str = "exec-1",
    exit_order_id: str = "102",
    exit_perm_id: str = "2047276406",
    exit_exec_id: str = "exec-2",
    generated_at: datetime = NOW,
) -> None:
    side = "SHORT" if "short" in lane_id else "LONG"
    action = "SELL" if side == "SHORT" else "BUY"
    base = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": lane_id,
        "thesis_strategy_id": strategy_id,
        "account_id": "DUM882026",
        "symbol": symbol,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "expiry": expiry,
        "side": side,
        "action": action,
        "qty": "1",
        "source_artifact_path": str(config.report_path),
        "generated_at": generated_at,
    }
    events: list[tuple[TradeEventType, dict[str, object]]] = [
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": order_id, "client_id": client_id}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": order_id, "client_id": client_id, "perm_id": entry_perm_id, "exec_id": entry_exec_id, "price": "28981.25"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": order_id, "client_id": client_id, "perm_id": entry_perm_id, "exec_id": entry_exec_id, "price": "28981.25"},
        ),
    ]
    if include_close_fill:
        events.extend(
            [
                (TradeEventType.EXIT_INTENT_CREATED, {"action": "SELL"}),
                (TradeEventType.EXIT_ORDER_SUBMITTED, {"action": "SELL", "order_id": exit_order_id, "client_id": client_id}),
                (
                    TradeEventType.EXIT_FILL_BROKER_BACKED,
                    {
                        "action": "SELL",
                        "order_id": exit_order_id,
                        "client_id": client_id,
                        "perm_id": exit_perm_id,
                        "exec_id": exit_exec_id,
                        "price": "28985.00",
                    },
                ),
            ]
        )
    if include_manual_close:
        events.append((TradeEventType.MANUAL_OPERATOR_CLOSE_RECORDED, {"action": "SELL", "price": "28985.00"}))
    for index, (event_type, extra) in enumerate(events):
        payload = {**base, **extra, "generated_at": generated_at + timedelta(seconds=index)}
        append_live_trade_registry_event(
            repo_root=config.repo_root,
            event=make_live_trade_registry_event(event_type=event_type, **payload),
        )


def _write_registry_lifecycle_only_open_trade(
    config: ReconciliationConfig,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    strategy_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    expiry: str,
    account_id: str = "DUM882026",
    order_id: str = "101",
    client_id: str = "17086",
    perm_id: str = "2047276405",
    exec_id: str = "exec-lifecycle-only",
    source_artifact_path: str | None = None,
    generated_at: datetime = NOW,
) -> None:
    side = "SHORT" if "short" in lane_id else "LONG"
    action = "SELL" if side == "SHORT" else "BUY"
    append_live_trade_registry_event(
        repo_root=config.repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            lane_id=lane_id,
            thesis_strategy_id=strategy_id,
            account_id=account_id,
            symbol=symbol,
            con_id=con_id,
            local_symbol=local_symbol,
            expiry=expiry,
            side=side,
            action=action,
            qty="1",
            order_id=order_id,
            client_id=client_id,
            perm_id=perm_id,
            exec_id=exec_id,
            price="28981.25",
            source_artifact_path=source_artifact_path or str(config.report_path),
            generated_at=generated_at,
            reason_codes=["LIFECYCLE_OPEN_MANAGED"],
        ),
    )


def _write_bridge_execution_report(
    config: ReconciliationConfig,
    *,
    lane_id: str,
    symbol: str,
    local_symbol: str,
    con_id: int,
    order_id: str,
    client_id: int,
    perm_id: int,
    exec_id: str,
    account_id: str,
    fill_price: str = "30437.00",
    extra_executions: list[dict[str, object]] | None = None,
) -> Path:
    path = config.repo_root / "outputs/reports/ibkr_runtime_route_dispatch" / lane_id / "ibkr_paper_strategy_bridge_report.json"
    action = "SELL" if "short" in lane_id else "BUY"
    side = "SLD" if action == "SELL" else "BOT"
    _write_json(
        path,
        {
            "selected_account_id": account_id,
            "intent": {"strategy_id": lane_id, "symbol": symbol, "quantity": 1, "action": action},
            "qualified_contract_report": {
                "qualified_contract": {
                    "symbol": symbol,
                    "local_symbol": local_symbol,
                    "con_id": con_id,
                    "expiry": "20260618",
                }
            },
            "delegated_result": {
                "classification": "PAPER_ORDER_FILLED",
                "report": {
                    "submit_cancel_lifecycle": {
                        "latest_order_status": {
                            "status": "Filled",
                            "order_id": order_id,
                            "client_id": client_id,
                            "perm_id": perm_id,
                            "filled": 1,
                            "last_fill_price": fill_price,
                            "avg_fill_price": fill_price,
                        },
                        "executions_after_submit": [
                            {
                                "execution_id": exec_id,
                                "account_id": account_id,
                                "symbol": symbol,
                                "quantity": 1,
                                "side": side,
                                "price": fill_price,
                                "executed_at": "2026-06-01T13:36:09+00:00",
                            },
                            *(extra_executions or []),
                        ],
                    }
                },
            },
        },
    )
    return path


def _read_registry_events(config: ReconciliationConfig) -> list[dict[str, object]]:
    path = config.repo_root / "outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


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


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
