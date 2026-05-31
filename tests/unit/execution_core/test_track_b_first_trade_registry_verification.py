from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    TRUTH_SNAPSHOT_OK,
    TrackBTruthSnapshotConfig,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_first_trade_registry_verification import (
    FirstTradeRegistryVerificationConfig,
    build_first_trade_registry_verification_report,
    write_first_trade_registry_verification_report,
)
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    make_live_trade_registry_event,
)


NOW = datetime(2026, 5, 31, 22, 15, tzinfo=UTC)


def test_first_trade_verification_reports_no_active_trades(tmp_path: Path) -> None:
    truth_config = _seed_clean_truth(tmp_path)

    report = build_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, truth_config=truth_config),
        now=NOW,
    )

    assert report["classification"] == "FIRST_TRADE_REGISTRY_NO_TRADES"
    assert report["latest_or_active_trade_id"] is None
    assert report["expected_next_lifecycle_action"] == "WAIT_FOR_FIRST_ENTRY_INTENT"
    assert report["canonical_truth_classification"] == TRUTH_SNAPSHOT_OK


def test_first_trade_verification_reports_entry_submitted_not_filled(tmp_path: Path) -> None:
    truth_config = _seed_clean_truth(tmp_path)
    _write_trade_events(tmp_path, [TradeEventType.ENTRY_INTENT_CREATED, TradeEventType.ENTRY_ORDER_SUBMITTED])

    report = build_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, truth_config=truth_config),
        now=NOW,
    )

    assert report["classification"] == "FIRST_TRADE_REGISTRY_VERIFIED"
    assert report["current_registry_state"] == "WORKING_ENTRY"
    assert report["entry_intent_event_present"] is True
    assert report["entry_order_event_present"] is True
    assert report["entry_fill_broker_backed_event_present"] is False
    assert report["expected_next_lifecycle_action"] == "AWAIT_BROKER_BACKED_ENTRY_FILL"


def test_first_trade_verification_reports_broker_backed_open_managed(tmp_path: Path) -> None:
    truth_config = _seed_clean_truth(tmp_path)
    _write_trade_events(
        tmp_path,
        [
            TradeEventType.ENTRY_INTENT_CREATED,
            TradeEventType.ENTRY_ORDER_SUBMITTED,
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
        ],
    )

    report = build_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, truth_config=truth_config),
        now=NOW,
    )

    assert report["classification"] == "FIRST_TRADE_REGISTRY_VERIFIED"
    assert report["current_registry_state"] == "OPEN_MANAGED"
    assert report["broker_backed_entry"] is True
    assert report["entry_perm_id"] == "2047276405"
    assert report["entry_exec_id"] == "0000e1a7.6a29f525.01.01"
    assert report["lifecycle_open_managed_event_present"] is True
    assert report["expected_next_lifecycle_action"] == "AWAIT_MANAGED_HOLD_OR_EXIT_POLICY"


def test_first_trade_verification_reports_closed_flat(tmp_path: Path) -> None:
    truth_config = _seed_clean_truth(tmp_path)
    _write_trade_events(
        tmp_path,
        [
            TradeEventType.ENTRY_INTENT_CREATED,
            TradeEventType.ENTRY_ORDER_SUBMITTED,
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            TradeEventType.EXIT_INTENT_CREATED,
            TradeEventType.EXIT_ORDER_SUBMITTED,
            TradeEventType.EXIT_FILL_BROKER_BACKED,
            TradeEventType.RECONCILED_FLAT,
        ],
    )

    report = build_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, truth_config=truth_config),
        now=NOW,
    )

    assert report["classification"] == "FIRST_TRADE_REGISTRY_CLOSED_FLAT"
    assert report["current_registry_state"] == "CLOSED_FLAT"
    assert report["broker_backed_exit"] is True
    assert report["expected_next_lifecycle_action"] == "NO_ACTION_CLOSED_FLAT"


def test_first_trade_verification_reports_review_required(tmp_path: Path) -> None:
    truth_config = _seed_clean_truth(tmp_path)
    _write_trade_events(tmp_path, [TradeEventType.ENTRY_INTENT_CREATED, TradeEventType.REVIEW_REQUIRED])

    report = build_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, truth_config=truth_config),
        now=NOW,
    )

    assert report["classification"] == "FIRST_TRADE_REGISTRY_REVIEW_REQUIRED"
    assert report["current_registry_state"] == "REVIEW_REQUIRED"
    assert "REGISTRY_TRADE_REVIEW_REQUIRED" in report["blockers"]
    assert report["expected_next_lifecycle_action"] == "OPERATOR_REVIEW_REQUIRED"


def test_first_trade_verification_writes_report_artifact(tmp_path: Path) -> None:
    truth_config = _seed_clean_truth(tmp_path)
    report = build_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, truth_config=truth_config),
        now=NOW,
    )
    path = write_first_trade_registry_verification_report(
        config=FirstTradeRegistryVerificationConfig(repo_root=tmp_path, output_path=Path("out/report.json")),
        report=report,
    )

    assert json.loads(path.read_text(encoding="utf-8"))["schema_version"] == "track_b_first_trade_registry_verification_v1"


def _write_trade_events(tmp_path: Path, event_types: list[TradeEventType]) -> None:
    base = {
        "trade_id": "trade_first_mnq",
        "lifecycle_id": "life-first-mnq",
        "lane_id": "mnq_us_active_participation_long",
        "thesis_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": "1",
        "source_artifact_path": str(tmp_path / "fixture.json"),
    }
    for index, event_type in enumerate(event_types):
        extra = _event_extra(event_type)
        payload = {
            **base,
            **extra,
            "generated_at": NOW + timedelta(seconds=index),
        }
        append_live_trade_registry_event(
            repo_root=tmp_path,
            event=make_live_trade_registry_event(event_type=event_type, **payload),
        )


def _event_extra(event_type: TradeEventType) -> dict[str, object]:
    if event_type == TradeEventType.ENTRY_ORDER_SUBMITTED:
        return {"order_id": "101", "client_id": "17086"}
    if event_type == TradeEventType.ENTRY_FILL_BROKER_BACKED:
        return {
            "order_id": "101",
            "client_id": "17086",
            "perm_id": "2047276405",
            "exec_id": "0000e1a7.6a29f525.01.01",
            "price": "28981.25",
        }
    if event_type == TradeEventType.LIFECYCLE_OPEN_MANAGED:
        return {
            "order_id": "101",
            "client_id": "17086",
            "perm_id": "2047276405",
            "exec_id": "0000e1a7.6a29f525.01.01",
            "price": "28981.25",
        }
    if event_type == TradeEventType.EXIT_INTENT_CREATED:
        return {"action": "SELL"}
    if event_type == TradeEventType.EXIT_ORDER_SUBMITTED:
        return {"action": "SELL", "order_id": "102", "client_id": "17086"}
    if event_type == TradeEventType.EXIT_FILL_BROKER_BACKED:
        return {
            "action": "SELL",
            "order_id": "102",
            "client_id": "17086",
            "perm_id": "2047276406",
            "exec_id": "0000e1a7.6a29f526.01.01",
            "price": "28985.00",
        }
    if event_type == TradeEventType.RECONCILED_FLAT:
        return {"action": "SELL"}
    if event_type == TradeEventType.REVIEW_REQUIRED:
        return {"reason_codes": ("SYNTHETIC_REVIEW_REQUIRED",)}
    return {}


def _seed_clean_truth(tmp_path: Path) -> TrackBTruthSnapshotConfig:
    config = TrackBTruthSnapshotConfig(repo_root=tmp_path)
    _write_json(
        tmp_path / config.runtime_truth_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RUNTIME_ACTIVE_TRADE_CAPABLE",
            "runtime": {"pid": 1234, "pid_alive": True, "runtime_instance_id": "generation-1", "lane_count": 8},
            "canonical_readiness": {"classification": "READY_SUBMIT_CAPABLE", "ready_submit_capable": True},
        },
    )
    _write_json(
        tmp_path / config.recovery_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "RECOVERY_ACTIVE",
            "launchd_loaded": True,
            "launchd_enabled": True,
            "last_tick": NOW.isoformat(),
        },
    )
    _write_json(
        tmp_path / config.recovery_audit_path,
        {"generated_at": NOW.isoformat(), "classification": "RUNTIME_HEALTHY_NO_ACTION"},
    )
    _write_json(
        tmp_path / config.broker_status_path,
        {
            "generated_at": NOW.isoformat(),
            "positions_snapshot_path": str(tmp_path / config.broker_positions_path),
            "open_orders_snapshot_path": str(tmp_path / config.broker_open_orders_path),
        },
    )
    _write_json(tmp_path / config.broker_positions_path, {"generated_at": NOW.isoformat(), "positions": []})
    _write_json(tmp_path / config.broker_open_orders_path, {"generated_at": NOW.isoformat(), "open_orders": []})
    _write_json(tmp_path / config.lifecycle_live_position_path, {"generated_at": NOW.isoformat(), "open_positions": []})
    _write_json(tmp_path / config.managed_position_registry_path, {"generated_at": NOW.isoformat(), "managed_positions": []})
    _write_json(tmp_path / config.managed_order_registry_path, {"generated_at": NOW.isoformat(), "managed_orders": []})
    _write_json(
        tmp_path / config.reconciliation_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_LIFECYCLE_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
        },
    )
    _write_json(
        tmp_path / config.safe_state_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "SAFE_STATE_NORMAL",
            "submit_allowed": True,
            "runtime_start_allowed": True,
        },
    )
    _write_json(
        tmp_path / config.control_plane_path,
        {
            "generated_at": NOW.isoformat(),
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
            "shared_truth_coherence_status": "COHERENT",
        },
    )
    _write_json(
        tmp_path / config.planner_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "PLAN_SCOPED_POSITION_CLEANUP",
            "control_plane_snapshot_id": "snapshot-1",
            "shared_truth_refresh_generation_id": "generation-1",
        },
    )
    _write_json(
        tmp_path / config.supervisor_path,
        {"generated_at": NOW.isoformat(), "classification": "SUPERVISOR_RUNTIME_START_ALLOWED"},
    )
    _write_json(
        tmp_path / config.contract_status_path,
        {
            "generated_at": NOW.isoformat(),
            "classification": "CONTRACT_ALLOWED",
            "submit_allowed": True,
            "symbol": "MNQ",
            "selected_contract": {"localSymbol": "MNQM6", "conId": 770561201, "expiry": "202606"},
        },
    )
    _write_json(
        tmp_path / config.broker_backed_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "fills": [
                {
                    "order_id": "101",
                    "client_id": "17086",
                    "perm_id": "2047276405",
                    "exec_id": "0000e1a7.6a29f525.01.01",
                }
            ],
        },
    )
    _write_json(tmp_path / config.local_paper_artifact_path, {"generated_at": NOW.isoformat(), "local_rows": []})
    if config.dashboard_runtime_path is not None:
        _write_json(tmp_path / config.dashboard_runtime_path, {"generated_at": NOW.isoformat(), "diagnostic": True})
    return config


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
