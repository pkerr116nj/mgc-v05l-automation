from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState, TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    broker_backed_fill_has_required_ids,
    load_live_trade_registry_records,
    load_live_trade_registry_record,
    make_live_trade_registry_event,
    repair_registry_identity_for_broker_backed_managed_position,
    resolve_live_trade_id_for_lifecycle_id,
    validate_registry_managed_exit_identity,
)
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    TradeRegistryReconstructionConfig,
    reconstruct_trade_registry_from_artifacts,
)


NOW = datetime(2026, 5, 31, 21, 55, tzinfo=UTC)


def test_live_trade_registry_appends_and_reconstructs_one_trade_chain(tmp_path):
    repo_root = tmp_path
    source_path = str(repo_root / "outputs/track_b_execution_core/strategy_bridge/bridge_report.json")
    base = {
        "trade_id": "trade_active_evidence_1",
        "lifecycle_id": "life_active_evidence_1",
        "lane_id": "mnq_globex_active_participation_long",
        "thesis_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": source_path,
        "generated_at": NOW,
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "101", "client_id": "17086"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "101", "client_id": "17086", "perm_id": "2047", "exec_id": "exec-1", "price": "30380.25"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "101", "client_id": "17086", "perm_id": "2047", "exec_id": "exec-1", "price": "30380.25"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(event_type=event_type, **base, **extra),
        )

    report = reconstruct_trade_registry_from_artifacts(
        config=TradeRegistryReconstructionConfig(
            repo_root=repo_root,
            reconciliation_path=Path("outputs/reports/track_b_paper_broker_reconciliation/missing.json"),
        ),
        now=NOW,
    )

    assert len(report.records) == 1
    record = report.records[0]
    assert record.trade_id == "trade_active_evidence_1"
    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id == "life_active_evidence_1"


def test_broker_backed_fill_requires_perm_and_exec_id():
    assert broker_backed_fill_has_required_ids(perm_id="2047", exec_id="exec-1") is True
    assert broker_backed_fill_has_required_ids(perm_id="2047", exec_id=None) is False
    assert broker_backed_fill_has_required_ids(perm_id=None, exec_id="exec-1") is False


def test_live_registry_rejects_out_of_repo_temp_source_artifacts(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    event = make_live_trade_registry_event(
        event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
        trade_id="trade_pytest_pollution",
        lifecycle_id="life_pytest_pollution",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        account_id="DUM882026",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
        expiry="202606",
        side="LONG",
        action="BUY",
        qty=Decimal("1"),
        order_id="101",
        client_id="17086",
        perm_id="2047",
        exec_id="exec-1",
        source_artifact_path=str(tmp_path / "pytest-of-patrick/test_case/report.json"),
        generated_at=NOW,
    )

    result = append_live_trade_registry_event(repo_root=repo_root, event=event)

    assert result["persisted"] is False
    assert result["classification"] == "LIVE_REGISTRY_EVENT_REJECTED_OUT_OF_REPO_SOURCE"
    assert load_live_trade_registry_records(repo_root=repo_root) == ()


def test_registry_managed_exit_validator_allows_dry_run_and_live_from_same_snapshot(tmp_path):
    repo_root = tmp_path
    trade_id = "trade_mnq_managed_exit"
    lifecycle_id = "life_mnq_managed_exit"
    source_path = str(repo_root / "outputs/track_b_execution_core/strategy_bridge/bridge_report.json")
    base = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": "mnq_us_active_participation_long",
        "thesis_strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": source_path,
        "generated_at": NOW,
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "101", "client_id": "17086"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "101", "client_id": "17086", "perm_id": "2047", "exec_id": "exec-1", "price": "30380.25"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "101", "client_id": "17086", "perm_id": "2047", "exec_id": "exec-1", "price": "30380.25"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(event_type=event_type, **base, **extra),
        )
    phase1 = {
        "ready": True,
        "track_b_lifecycle_positions": [
            {
                "lifecycle_id": lifecycle_id,
                "account_id": "MULTIPLE",
                "lane_id": "mnq_us_active_participation_long",
                "strategy_id": "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
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

    dry_run = validate_registry_managed_exit_identity(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        quantity=1,
        action="SELL",
        phase1_reconciliation_gate=phase1,
    )
    live = validate_registry_managed_exit_identity(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        quantity=1,
        action="SELL",
        phase1_reconciliation_gate=phase1,
    )

    assert dry_run["allowed"] is True
    assert live["allowed"] is True
    assert dry_run["owner_identity"] == live["owner_identity"]


def test_registry_managed_exit_validator_uses_matched_registry_reconciliation_when_legacy_lifecycle_is_stale(tmp_path):
    repo_root = tmp_path
    trade_id = "trade_mnq_registry_reconciled"
    lifecycle_id = "life_mnq_registry_reconciled"
    source_path = str(repo_root / "outputs/track_b_execution_core/strategy_bridge/bridge_report.json")
    base = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": "mnq_us_active_participation_long",
        "thesis_strategy_id": "mnq_us_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": source_path,
        "generated_at": NOW,
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "1", "client_id": "11127"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "1", "client_id": "11127", "perm_id": "1955790757", "exec_id": "exec-mnq", "price": "30436.75"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "1", "client_id": "11127", "perm_id": "1955790757", "exec_id": "exec-mnq", "price": "30436.75"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(event_type=event_type, **base, **extra),
        )
    phase1 = {
        "ready": False,
        "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
        "track_b_lifecycle_positions": [],
        "track_b_broker_positions": [
            {"account_id": "DUM882026", "symbol": "MNQ", "local_symbol": "MNQM6", "con_id": 770561201, "quantity": "1"}
        ],
        "registry_reconciliation": {
            "classification": "REGISTRY_RECONCILIATION_MATCHED",
            "blocking": False,
            "mapped_records": [
                {
                    "trade_id": trade_id,
                    "lifecycle_id": lifecycle_id,
                    "account_id": "DUM882026",
                    "instrument_family": "MNQ",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "quantity": "1",
                    "side": "LONG",
                    "lane_id": "mnq_us_active_participation_long",
                    "strategy_id": "mnq_us_active_participation_long",
                    "entry_perm_id": "1955790757",
                    "entry_exec_id": "exec-mnq",
                }
            ],
        },
    }

    result = validate_registry_managed_exit_identity(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        quantity=1,
        action="SELL",
        phase1_reconciliation_gate=phase1,
    )

    assert result["allowed"] is True
    assert result["lifecycle_row"]["source"] == "CENTRAL_TRADE_REGISTRY_RECONCILIATION"
    assert result["broker_position"]["con_id"] == 770561201


def test_resolve_live_trade_id_for_lifecycle_id_requires_unique_broker_backed_open_owner(tmp_path):
    repo_root = tmp_path
    source_path = str(repo_root / "outputs/track_b_execution_core/strategy_bridge/bridge_report.json")
    base = {
        "lifecycle_id": "life_shared",
        "lane_id": "mnq_us_active_participation_long",
        "thesis_strategy_id": "mnq_us_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": source_path,
        "generated_at": NOW,
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "1", "client_id": "17086"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "1", "client_id": "17086", "perm_id": "perm-1", "exec_id": "exec-1", "price": "30380.25"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "1", "client_id": "17086", "perm_id": "perm-1", "exec_id": "exec-1", "price": "30380.25"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(event_type=event_type, trade_id="trade_current", **base, **extra),
        )

    assert resolve_live_trade_id_for_lifecycle_id(repo_root=repo_root, lifecycle_id="life_shared") == "trade_current"

    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "2", "client_id": "17086"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "2", "client_id": "17086", "perm_id": "perm-2", "exec_id": "exec-2", "price": "30381.25"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "2", "client_id": "17086", "perm_id": "perm-2", "exec_id": "exec-2", "price": "30381.25"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(event_type=event_type, trade_id="trade_ambiguous", **base, **extra),
        )

    assert resolve_live_trade_id_for_lifecycle_id(repo_root=repo_root, lifecycle_id="life_shared") is None


def test_registry_managed_exit_validator_fails_closed_without_trade_id(tmp_path):
    result = validate_registry_managed_exit_identity(
        repo_root=tmp_path,
        trade_id=None,
        lifecycle_id="life",
        account_id="DUM882026",
        con_id=1,
        local_symbol="ANY",
        quantity=1,
        action="SELL",
        phase1_reconciliation_gate={"ready": True},
    )

    assert result["allowed"] is False
    assert "missing_trade_id" in result["block_reasons"]


def test_registry_identity_repair_recovers_review_required_null_lifecycle_owner_with_exact_evidence(tmp_path):
    repo_root = tmp_path
    trade_id = "trade_mes_review_required"
    lifecycle_id = "life_mes_managed"
    _write_review_required_null_lifecycle_registry_chain(repo_root, trade_id=trade_id, lifecycle_id=lifecycle_id)

    result = repair_registry_identity_for_broker_backed_managed_position(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        phase1_reconciliation_gate=_phase1_repair_gate(trade_id=trade_id, lifecycle_id=lifecycle_id),
        lifecycle_report=_lifecycle_repair_report(trade_id=trade_id, lifecycle_id=lifecycle_id),
        generated_at=NOW + timedelta(minutes=1),
    )

    assert result["classification"] == "REGISTRY_IDENTITY_REPAIR_APPLIED"
    assert result["broker_mutation_performed"] is False
    assert result["live_money_eligible"] is False
    assert result["paper_proof_invoked"] is False
    assert [event["event_type"] for event in result["persisted_events"]] == [
        "ENTRY_FILL_BROKER_BACKED",
        "LIFECYCLE_OPEN_MANAGED",
    ]
    record = load_live_trade_registry_record(repo_root=repo_root, trade_id=trade_id)
    assert record is not None
    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    assert record.broker_backed_entry is True
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id == lifecycle_id

    validation = validate_registry_managed_exit_identity(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        account_id="DUM882026",
        con_id=770561194,
        local_symbol="MESM6",
        quantity=1,
        action="SELL",
        phase1_reconciliation_gate=_phase1_repair_gate(trade_id=trade_id, lifecycle_id=lifecycle_id),
    )
    assert validation["allowed"] is True
    assert validation["owner_identity"]["entry_exec_id"] == "exec-mes-entry"


def test_registry_identity_repair_fails_closed_without_exact_fill_evidence(tmp_path):
    repo_root = tmp_path
    trade_id = "trade_mes_missing_exec"
    lifecycle_id = "life_mes_missing_exec"
    _write_review_required_null_lifecycle_registry_chain(repo_root, trade_id=trade_id, lifecycle_id=lifecycle_id)
    lifecycle_report = _lifecycle_repair_report(trade_id=trade_id, lifecycle_id=lifecycle_id)
    lifecycle_report["entry_fill"].pop("exec_id")
    phase1 = _phase1_repair_gate(trade_id=trade_id, lifecycle_id=lifecycle_id)
    phase1["track_b_lifecycle_positions"][0].pop("entry_exec_ids")
    phase1["registry_reconciliation"]["mapped_records"][0]["entry_exec_id"] = None

    result = repair_registry_identity_for_broker_backed_managed_position(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        phase1_reconciliation_gate=phase1,
        lifecycle_report=lifecycle_report,
        generated_at=NOW + timedelta(minutes=1),
    )

    assert result["classification"] == "REGISTRY_IDENTITY_REPAIR_BLOCKED"
    assert "entry_fill_exec_id_missing" in result["block_reasons"]
    assert result["persisted_events"] == []
    record = load_live_trade_registry_record(repo_root=repo_root, trade_id=trade_id)
    assert record is not None
    assert record.current_state == TradeCurrentState.REVIEW_REQUIRED
    assert record.ownership_identity is not None
    assert record.ownership_identity.lifecycle_id is None


def test_registry_identity_repair_fails_closed_with_conflicting_current_owner(tmp_path):
    repo_root = tmp_path
    trade_id = "trade_mes_review_conflict"
    lifecycle_id = "life_mes_conflict"
    _write_review_required_null_lifecycle_registry_chain(repo_root, trade_id=trade_id, lifecycle_id=lifecycle_id)
    _write_open_managed_registry_chain(
        repo_root,
        trade_id="trade_mes_conflicting_owner",
        lifecycle_id="life_mes_conflicting_owner",
        order_id="2",
        perm_id="perm-conflict",
        exec_id="exec-conflict",
    )
    phase1 = _phase1_repair_gate(trade_id=trade_id, lifecycle_id=lifecycle_id)
    phase1["registry_reconciliation"]["mapped_records"].append(
        {
            "trade_id": "trade_mes_conflicting_owner",
            "lifecycle_id": "life_mes_conflicting_owner",
            "account_id": "DUM882026",
            "instrument_family": "MES",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "quantity": "1",
            "side": "LONG",
            "lane_id": "mes_us_active_participation_long",
            "strategy_id": "mes_us_active_participation_long",
        }
    )

    result = repair_registry_identity_for_broker_backed_managed_position(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        phase1_reconciliation_gate=phase1,
        lifecycle_report=_lifecycle_repair_report(trade_id=trade_id, lifecycle_id=lifecycle_id),
        generated_at=NOW + timedelta(minutes=1),
    )

    assert result["classification"] == "REGISTRY_IDENTITY_REPAIR_BLOCKED"
    assert "conflicting_current_registry_owner" in result["block_reasons"]
    assert result["persisted_events"] == []


def test_registry_identity_repair_ignores_stale_owner_and_failed_close_attempt(tmp_path):
    repo_root = tmp_path
    trade_id = "trade_mes_review_stale_owner"
    lifecycle_id = "life_mes_current"
    _write_review_required_null_lifecycle_registry_chain(repo_root, trade_id=trade_id, lifecycle_id=lifecycle_id)
    _write_open_managed_registry_chain(
        repo_root,
        trade_id="trade_mes_stale_owner",
        lifecycle_id="life_mes_stale_owner",
        order_id="2",
        perm_id="perm-stale",
        exec_id="exec-stale",
    )
    lifecycle_report = _lifecycle_repair_report(trade_id=trade_id, lifecycle_id=lifecycle_id)
    lifecycle_report["close_submit_attempt"] = {
        "classification": "STRATEGY_SUBMIT_BLOCKED_ENTRY_EXPOSURE",
        "broker_state_mutated": False,
        "submitted": False,
        "strategy_submit_authorization": {
            "broker_mutation_allowed": False,
        },
    }

    result = repair_registry_identity_for_broker_backed_managed_position(
        repo_root=repo_root,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        phase1_reconciliation_gate=_phase1_repair_gate(trade_id=trade_id, lifecycle_id=lifecycle_id),
        lifecycle_report=lifecycle_report,
        generated_at=NOW + timedelta(minutes=1),
    )

    assert result["classification"] == "REGISTRY_IDENTITY_REPAIR_APPLIED"
    assert result["broker_mutation_performed"] is False
    assert result["live_money_eligible"] is False
    assert result["paper_proof_invoked"] is False
    assert "lifecycle_already_has_close_evidence" not in result["block_reasons"]
    assert "conflicting_current_registry_owner" not in result["block_reasons"]


def _write_review_required_null_lifecycle_registry_chain(repo_root, *, trade_id: str, lifecycle_id: str) -> None:
    base = _mes_event_base(repo_root, trade_id=trade_id, lifecycle_id=None, generated_at=NOW)
    append_live_trade_registry_event(
        repo_root=repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.ENTRY_INTENT_CREATED,
            **base,
        ),
    )
    append_live_trade_registry_event(
        repo_root=repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.ENTRY_ORDER_SUBMITTED,
            lifecycle_id=lifecycle_id,
            order_id="1",
            client_id="11113",
            generated_at=NOW.replace(second=NOW.second + 1),
            **{key: value for key, value in base.items() if key not in {"lifecycle_id", "generated_at"}},
        ),
    )
    append_live_trade_registry_event(
        repo_root=repo_root,
        event=make_live_trade_registry_event(
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            lifecycle_id=lifecycle_id,
            order_id="1",
            client_id="11113",
            perm_id="1092553522",
            exec_id=None,
            price="7565.75",
            reason_codes=("BROKER_BACKED_FILL_MISSING_PERM_OR_EXEC",),
            generated_at=NOW.replace(second=NOW.second + 2),
            **{key: value for key, value in base.items() if key not in {"lifecycle_id", "generated_at"}},
        ),
    )


def _write_open_managed_registry_chain(
    repo_root,
    *,
    trade_id: str,
    lifecycle_id: str,
    order_id: str,
    perm_id: str,
    exec_id: str,
) -> None:
    base = _mes_event_base(repo_root, trade_id=trade_id, lifecycle_id=lifecycle_id, generated_at=NOW)
    base_without_generated_at = {key: value for key, value in base.items() if key != "generated_at"}
    for offset, event_type, extra in (
        (0, TradeEventType.ENTRY_INTENT_CREATED, {}),
        (1, TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": order_id, "client_id": "11113"}),
        (
            2,
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": order_id, "client_id": "11113", "perm_id": perm_id, "exec_id": exec_id, "price": "7565.75"},
        ),
        (3, TradeEventType.LIFECYCLE_OPEN_MANAGED, {}),
    ):
        append_live_trade_registry_event(
            repo_root=repo_root,
            event=make_live_trade_registry_event(
                event_type=event_type,
                generated_at=NOW.replace(second=NOW.second + offset),
                **base_without_generated_at,
                **extra,
            ),
        )


def _mes_event_base(repo_root, *, trade_id: str, lifecycle_id: str | None, generated_at: datetime) -> dict:
    return {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": "mes_us_active_participation_long",
        "thesis_strategy_id": "mes_us_active_participation_long",
        "account_id": "DUM882026",
        "symbol": "MES",
        "con_id": 770561194,
        "local_symbol": "MESM6",
        "expiry": "20260618",
        "side": "LONG",
        "action": "BUY",
        "qty": Decimal("1"),
        "source_artifact_path": str(repo_root / "outputs/track_b_execution_core/strategy_bridge/mes_bridge_report.json"),
        "generated_at": generated_at,
    }


def _phase1_repair_gate(*, trade_id: str, lifecycle_id: str) -> dict:
    lifecycle_row = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "lane_id": "mes_us_active_participation_long",
        "strategy_id": "mes_us_active_participation_long",
        "instrument_family": "MES",
        "track_b_root": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "expiry": "20260618",
        "quantity": "1",
        "side": "LONG",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "entry_client_id": "11113",
        "entry_order_ids": ["1"],
        "entry_perm_ids": [1092553522],
        "entry_exec_ids": ["exec-mes-entry"],
        "avg_entry_price": "7565.75",
    }
    return {
        "ready": True,
        "broker_reconciled": True,
        "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        "current_scope_review_required_count": 0,
        "track_b_broker_open_order_count": 0,
        "track_b_broker_open_orders": [],
        "track_b_lifecycle_positions": [lifecycle_row],
        "track_b_broker_positions": [
            {
                "account_id": "DUM882026",
                "symbol": "MES",
                "track_b_root": "MES",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "expiry": "20260618",
                "quantity": "1.0",
            }
        ],
        "registry_reconciliation": {
            "classification": "REGISTRY_RECONCILIATION_MATCHED",
            "blocking": False,
            "mapped_records": [
                {
                    **lifecycle_row,
                    "current_state": "REVIEW_REQUIRED",
                    "entry_order_id": "1",
                    "entry_client_id": "11113",
                    "entry_perm_id": "1092553522",
                    "entry_exec_id": None,
                }
            ],
        },
    }


def _lifecycle_repair_report(*, trade_id: str, lifecycle_id: str) -> dict:
    return {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "lane_id": "mes_us_active_participation_long",
        "strategy_id": "mes_us_active_participation_long",
        "instrument_family": "MES",
        "symbol": "MES",
        "con_id": 770561194,
        "local_symbol": "MESM6",
        "expiry": "20260618",
        "side": "LONG",
        "quantity": "1",
        "managed_exit_policy_id": "US_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "report_json_path": "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle/life_mes_managed/track_b_strategy_managed_paper_lifecycle_report.json",
        "entry_fill": {
            "order_id": "1",
            "client_id": "11113",
            "perm_id": "1092553522",
            "exec_id": "exec-mes-entry",
            "price": "7565.75",
            "quantity": "1",
        },
    }
