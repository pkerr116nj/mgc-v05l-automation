from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from mgc_v05l.execution.ibkr_paper_strategy_exposure import (
    IbkrPaperStrategyExposureConfig,
    evaluate_paper_strategy_exposure_gate,
    run_ibkr_paper_strategy_exposure,
)
from mgc_v05l.execution_core.track_b_submit_intent_ownership import (
    SubmitIntentOwnershipRecord,
    SubmitIntentOwnershipState,
    append_submit_intent_ownership_record,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEventType
from mgc_v05l.execution_core.track_b_live_trade_registry import (
    append_live_trade_registry_event,
    make_live_trade_registry_event,
)


def _write_monitor(tmp_path: Path, *, broker_quantity: float = 1.0, orphan_positions: list[dict[str, object]] | None = None) -> None:
    path = tmp_path / "var" / "paper_strategy_monitor_runtime_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_MONITOR_ACTIVE",
                "monitor_running": True,
                "health_classification": "HEALTHY",
                "stale": False,
                "submit_allowed": True,
                "account_id": "DUM882026",
                "broker_position_quantity": broker_quantity,
                "ledger_position_quantity": broker_quantity,
                "open_order_count": 0,
                "last_successful_broker_refresh": "2999-01-01T00:00:00+00:00",
                "freshness_window_seconds": 60.0,
                "block_reasons": [],
                "orphan_positions": orphan_positions or [],
            }
        ),
        encoding="utf-8",
    )


def _write_ledger(tmp_path: Path, positions: list[dict[str, object]]) -> None:
    path = tmp_path / "var" / "paper_strategy_position_ledger.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"positions": positions, "orphan_positions": []}), encoding="utf-8")


def _write_governance(tmp_path: Path, rows: list[dict[str, object]]) -> None:
    path = tmp_path / "var" / "per_strategy_paper_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "classification": "PAPER_STRATEGY_GOVERNANCE_READY",
                "strategies": rows,
                "summary": {
                    "strategy_count": len(rows),
                    "submit_capable_count": len([row for row in rows if row.get("submit_allowed")]),
                },
            }
        ),
        encoding="utf-8",
    )


def _write_phase1_reconciliation(
    tmp_path: Path,
    *,
    classification: str = "TRACK_B_PAPER_BROKER_RECONCILED",
    broker_reconciled: bool = True,
    review_required_count: int = 0,
    open_order_count: int = 0,
    block_reasons: list[str] | None = None,
    lifecycle_positions: list[dict[str, object]] | None = None,
    broker_positions: list[dict[str, object]] | None = None,
) -> None:
    path = (
        tmp_path
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": classification,
                "broker_reconciled": broker_reconciled,
                "review_required_count": review_required_count,
                "current_scope_review_required_count": review_required_count,
                "track_b_broker_open_order_count": open_order_count,
                "track_b_broker_position_count": len(broker_positions or []),
                "track_b_broker_positions": broker_positions or [],
                "track_b_lifecycle_positions": lifecycle_positions or [],
                "live_money_eligible": False,
                "blockers": [],
                "block_reasons": block_reasons or [],
            }
        ),
        encoding="utf-8",
    )


def _write_canonical_current_scope(
    tmp_path: Path,
    *,
    registry_classification: str = "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
    registry_generated_at: str = "2999-01-01T00:00:00+00:00",
    managed_position_count: int = 0,
    managed_position_review_count: int = 0,
    managed_order_count: int = 0,
    open_order_count: int = 0,
) -> None:
    registry_path = tmp_path / "outputs" / "track_b_execution_core" / "diagnostics" / "latest_track_b_registry_truth_diagnostics.json"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        json.dumps(
            {
                "generated_at": registry_generated_at,
                "classification": registry_classification,
                "current_scope_review_required_count": 0 if registry_classification == "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE" else 1,
            }
        ),
        encoding="utf-8",
    )

    managed_positions_path = tmp_path / "outputs" / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
    managed_positions_path.parent.mkdir(parents=True, exist_ok=True)
    managed_positions_path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "NO_MANAGED_POSITIONS" if managed_position_count == 0 else "OPEN_MANAGED_MATCHED",
                "summary": {
                    "managed_position_count": managed_position_count,
                    "lifecycle_position_count": managed_position_count,
                    "review_required_count": managed_position_review_count,
                    "attention_required_count": managed_position_review_count,
                },
            }
        ),
        encoding="utf-8",
    )

    managed_orders_path = tmp_path / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
    managed_orders_path.parent.mkdir(parents=True, exist_ok=True)
    managed_orders_path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "NO_MANAGED_ORDERS" if managed_order_count == 0 else "MANAGED_ORDERS_PRESENT",
                "summary": {
                    "managed_order_count": managed_order_count,
                    "working_entry_order_count": managed_order_count,
                    "working_close_order_count": 0,
                    "position_without_close_order_count": 0,
                },
            }
        ),
        encoding="utf-8",
    )

    open_order_truth_path = tmp_path / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
    open_order_truth_path.parent.mkdir(parents=True, exist_ok=True)
    open_order_truth_path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "NO_OPEN_ORDERS" if open_order_count == 0 else "OPEN_ORDERS_PRESENT",
                "summary": {"open_order_count": open_order_count},
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture(autouse=True)
def _default_phase1_reconciliation(tmp_path: Path) -> None:
    _write_phase1_reconciliation(tmp_path)
    _write_contract_status(tmp_path)
    _write_canonical_current_scope(tmp_path)


def _write_broker_positions_snapshot(
    tmp_path: Path,
    *,
    generated_at: str = "2999-01-01T00:00:00+00:00",
    positions: list[dict[str, object]] | None = None,
) -> None:
    path = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "source": "IBKR_TWS_API_REQ_POSITIONS",
                "request_method": "reqPositions",
                "positions_complete": True,
                "ok": True,
                "selected_account_id": "DUM882026",
                "positions": positions or [],
            }
        ),
        encoding="utf-8",
    )


def _write_broker_open_orders_snapshot(
    tmp_path: Path,
    *,
    generated_at: str = "2999-01-01T00:00:00+00:00",
    open_orders: list[dict[str, object]] | None = None,
) -> None:
    path = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": generated_at,
                "source": "IBKR_TWS_API_REQ_ALL_OPEN_ORDERS",
                "request_method": "reqAllOpenOrders",
                "open_orders_complete": True,
                "ok": True,
                "selected_account_id": "DUM882026",
                "open_order_count": len(open_orders or []),
                "has_open_orders": bool(open_orders),
                "open_orders": open_orders or [],
            }
        ),
        encoding="utf-8",
    )


def _write_contract_status(tmp_path: Path, *, entry_status: str = "CONTRACT_ENTRY_ELIGIBLE") -> None:
    path = tmp_path / "outputs" / "track_b_execution_core" / "contract_resolver" / "latest_contract_resolver_status.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00+00:00",
                "classification": "CONTRACT_ALLOWED" if entry_status == "CONTRACT_ENTRY_ELIGIBLE" else "CONTRACT_BLOCKED",
                "entry_status": entry_status,
                "exit_status": "EXIT_ORIGINAL_CONTRACT_ALLOWED",
                "symbol": "MGC",
                "selected_contract": {"localSymbol": "MGCM6", "conId": 712565978, "expiry": "202606"},
            }
        ),
        encoding="utf-8",
    )


def _position(strategy_id: str, *, qty: float = 1.0, side: str = "LONG", symbol: str = "MGC") -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "account_id": "DUM882026",
        "symbol": symbol,
        "contract_month": "202606",
        "expiry": "20260626",
        "con_id": 712565978,
        "local_symbol": "MGCM6",
        "quantity": qty,
        "side": side,
        "average_entry_price": 4586.7,
        "realized_pnl": 14.18,
        "unrealized_pnl": 223.03,
        "order_id": 1,
        "perm_id": 490708968,
        "execution_id": "exec-1",
        "entry_timestamp": "2026-04-28T17:44:00+00:00",
        "source_intent_id": "intent-1",
        "state": "SHORT" if side == "SHORT" else "OPEN",
    }


def _governance_row(strategy_id: str, bridge_strategy_id: str, *, status: str = "PROBATION_ACTIVE") -> dict[str, object]:
    return {
        "strategy_id": strategy_id,
        "bridge_strategy_id": bridge_strategy_id,
        "strategy_status": status,
        "submit_allowed": True,
        "submit_block_reasons": [],
        "open_order_ambiguity_count": 0,
    }


def _write_broker_backed_managed_position(
    tmp_path: Path,
    *,
    symbol: str,
    lane_id: str,
    thesis_strategy_id: str,
    lifecycle_id: str,
    con_id: int,
    local_symbol: str,
    account_id: str = "MULTIPLE",
    broker_account_id: str = "DUM882026",
    quantity: str = "1",
) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row(lane_id, thesis_strategy_id)])
    broker_position = {
        "account_id": broker_account_id,
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": quantity,
    }
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "account_id": account_id,
                "strategy_id": thesis_strategy_id,
                "lane_id": lane_id,
                "track_b_root": symbol,
                "instrument_family": symbol,
                "contract_key": f"{symbol}-202606",
                "local_symbol": local_symbol,
                "con_id": con_id,
                "quantity": quantity,
                "side": "LONG",
                "avg_entry_price": "100",
                "entry_order_id": "1",
                "entry_perm_id": "2047276405",
                "entry_exec_id": "0000e1a7.6a29f525.01.01",
                "lifecycle_id": lifecycle_id,
            }
        ],
        broker_positions=[broker_position],
    )
    _write_broker_positions_snapshot(tmp_path, positions=[broker_position])
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])


def _write_registry_open_managed_trade(
    tmp_path: Path,
    *,
    trade_id: str,
    lifecycle_id: str,
    lane_id: str,
    thesis_strategy_id: str,
    symbol: str,
    con_id: int,
    local_symbol: str,
    account_id: str = "DUM882026",
    qty: str = "1",
) -> None:
    base = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": lane_id,
        "thesis_strategy_id": thesis_strategy_id,
        "account_id": account_id,
        "symbol": symbol,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": qty,
        "source_artifact_path": str(tmp_path / "registry_fixture.json"),
        "generated_at": datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
    }
    for event_type, extra in (
        (TradeEventType.ENTRY_INTENT_CREATED, {}),
        (TradeEventType.ENTRY_ORDER_SUBMITTED, {"order_id": "101", "client_id": "9801"}),
        (
            TradeEventType.ENTRY_FILL_BROKER_BACKED,
            {"order_id": "101", "client_id": "9801", "perm_id": "2047276405", "exec_id": "0000e1a7.6a29f525.01.01", "price": "100"},
        ),
        (
            TradeEventType.LIFECYCLE_OPEN_MANAGED,
            {"order_id": "101", "client_id": "9801", "perm_id": "2047276405", "exec_id": "0000e1a7.6a29f525.01.01", "price": "100"},
        ),
    ):
        append_live_trade_registry_event(
            repo_root=tmp_path,
            event=make_live_trade_registry_event(event_type=event_type, **base, **extra),
        )


def _write_submit_intent_ownership(
    tmp_path: Path,
    *,
    state: SubmitIntentOwnershipState = SubmitIntentOwnershipState.BROKER_RESULT_UNKNOWN_REFRESH_REQUIRED,
    lane_id: str = "gc_1x_all_lanes__asia_early_long",
    strategy_id: str = "gold_forced_session_baseline_v2__GC",
    symbol: str = "GC",
    local_symbol: str = "GCM6",
    con_id: int = 430360630,
) -> None:
    record = SubmitIntentOwnershipRecord(
        mode="PAPER",
        account_id="DUM882026",
        lane_id=lane_id,
        strategy_id=strategy_id,
        intent_type="BUY_TO_OPEN",
        action="BUY",
        symbol=symbol,
        local_symbol=local_symbol,
        expiry="20260626",
        con_id=con_id,
        qty=1,
        order_type="LMT",
        limit_price="4556.0",
        time_in_force="DAY",
        repo_root=str(tmp_path),
        git_head="abc123",
        created_at=datetime(2026, 5, 15, 12, 0, tzinfo=UTC),
        state=state,
        ownership_intent_id=f"submit_owner_{lane_id}_{con_id}_{state.value}".replace("/", "_"),
        lifecycle_id=f"reserved_submit_{lane_id}_{con_id}",
        lifecycle_id_reserved_only=True,
        lifecycle_position_open=False,
        live_money_eligible=False,
        paper_proof_invoked=False,
    )
    append_submit_intent_ownership_record(
        record,
        jsonl_path=tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "submit_intent_ownership"
        / "track_b_submit_intent_ownership.jsonl",
        latest_path=tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "submit_intent_ownership"
        / "latest_track_b_submit_intent_ownership.json",
    )


def test_allows_second_strategy_buy_when_another_strategy_is_already_long(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
        allow_stacking=True,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["submit_allowed"] is False
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]
    assert gate["max_total_mgc_contracts"] == 20.0


def test_blocks_new_entry_by_unresolved_same_account_contract_submit_intent(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mgc_1x_asia_london_participation__asia_london_long_v5", "asia_london_participation_core_v1__MGC")])
    _write_submit_intent_ownership(
        tmp_path,
        lane_id="other_mgc_lane",
        strategy_id="other_mgc_strategy",
        symbol="MGC",
        local_symbol="MGCM6",
        con_id=712565978,
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        bridge_strategy_id="asia_london_participation_core_v1__MGC",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MGC",
        con_id=712565978,
        local_symbol="MGCM6",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_ENTRY_ALLOWED_REGISTRY_TRUTH_LEGACY_DIAGNOSTIC"
    assert gate["submit_allowed"] is True
    assert gate["authoritative_source"] == "REGISTRY_TRUTH"
    assert gate["registry_truth_result"]["allowed"] is True
    assert gate["legacy_result"]["allowed"] is False
    assert gate["parity_status"] == "REGISTRY_TRUTH_ALLOWED_LEGACY_BLOCKED_DIAGNOSTIC"
    assert gate["blocker_classification"] is None
    assert "TRACK_B_UNRESOLVED_SUBMIT_INTENT_BLOCKS_NEW_ENTRY" in gate["diagnostic_reason_codes"]
    blocker = gate["unresolved_submit_intent_ownership_blocker"]
    assert blocker["matching_records"][0]["match_reason"] == "same_account_contract"


def test_stale_raw_open_registry_rows_do_not_block_when_canonical_current_scope_is_clean(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_broker_positions_snapshot(tmp_path, positions=[])
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id="trade_submit_owner_mnq_exec_late",
        lifecycle_id="reserved_submit_atp_companion_v1_asia_us_1",
        lane_id="atp_companion_v1_asia_us",
        thesis_strategy_id="atp_companion_v1__benchmark_mgc_asia_us",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id="trade_original_mnq_globex_short",
        lifecycle_id="reserved_submit_mnq_globex_active_participation_short_1",
        lane_id="mnq_globex_active_participation_short",
        thesis_strategy_id="mnq_globex_active_participation_short",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id="trade_original_mes_globex_short",
        lifecycle_id="reserved_submit_mes_globex_active_participation_short_1",
        lane_id="mes_globex_active_participation_short",
        thesis_strategy_id="mes_globex_active_participation_short",
        symbol="MES",
        con_id=770561194,
        local_symbol="MESM6",
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
    )

    assert gate["submit_allowed"] is True
    assert gate["classification"] == "PAPER_EXPOSURE_ENTRY_ALLOWED"
    assert gate["registry_truth_result"]["raw_active_registry_trade_count"] == 3
    assert gate["registry_truth_result"]["active_registry_trade_count"] == 0
    assert gate["registry_truth_result"]["canonical_current_scope_result"]["allowed"] is True
    assert "strategy_stacking_disabled" not in gate["block_reasons"]


def test_canonical_current_open_managed_position_blocks_entry(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_canonical_current_scope(tmp_path, managed_position_count=1)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "CANONICAL_CURRENT_EXPOSURE_PRESENT" in gate["block_reasons"]
    assert gate["registry_truth_result"]["canonical_current_scope_result"]["current_exposure_present"] is True


def test_canonical_open_order_blocks_entry(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_phase1_reconciliation(tmp_path, open_order_count=1)
    _write_canonical_current_scope(tmp_path, open_order_count=1)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert "current_open_order_conflict" in gate["block_reasons"]
    assert "CANONICAL_CURRENT_EXPOSURE_PRESENT" in gate["block_reasons"]


def test_stale_canonical_registry_diagnostics_fail_closed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_canonical_current_scope(tmp_path, registry_generated_at="2026-01-01T00:00:00+00:00")

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert "CANONICAL_REGISTRY_TRUTH_UNAVAILABLE" in gate["block_reasons"]
    assert gate["registry_truth_result"]["canonical_current_scope_result"]["usable"] is False


def test_ambiguous_canonical_registry_diagnostics_fail_closed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_canonical_current_scope(tmp_path, registry_classification="TRACK_B_DIAGNOSTICS_CONFLICT_CURRENT_SCOPE")

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert "CANONICAL_REGISTRY_TRUTH_AMBIGUOUS" in gate["block_reasons"]
    assert gate["registry_truth_result"]["canonical_current_scope_result"]["usable"] is False


def test_blocks_new_entry_by_unresolved_same_lane_submit_intent(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mgc_1x_asia_london_participation__asia_london_long_v5", "asia_london_participation_core_v1__MGC")])
    _write_submit_intent_ownership(
        tmp_path,
        lane_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        strategy_id="unrelated_bridge_strategy",
        symbol="NQ",
        local_symbol="NQM6",
        con_id=12345,
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        bridge_strategy_id="asia_london_participation_core_v1__MGC",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MGC",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_ENTRY_ALLOWED_REGISTRY_TRUTH_LEGACY_DIAGNOSTIC"
    assert gate["submit_allowed"] is True
    assert "TRACK_B_UNRESOLVED_SUBMIT_INTENT_BLOCKS_NEW_ENTRY" in gate["diagnostic_reason_codes"]
    assert gate["unresolved_submit_intent_ownership_blocker"]["matching_records"][0]["match_reason"] == "same_lane_or_strategy"


def test_terminal_submit_intent_does_not_block_new_entry(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mgc_1x_asia_london_participation__asia_london_long_v5", "asia_london_participation_core_v1__MGC")])
    _write_submit_intent_ownership(
        tmp_path,
        state=SubmitIntentOwnershipState.NO_BROKER_EFFECT_CONFIRMED,
        lane_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        strategy_id="asia_london_participation_core_v1__MGC",
        symbol="MGC",
        local_symbol="MGCM6",
        con_id=712565978,
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        bridge_strategy_id="asia_london_participation_core_v1__MGC",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MGC",
        con_id=712565978,
        local_symbol="MGCM6",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_ENTRY_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["unresolved_submit_intent_ownership_blocker"] is None


def test_review_required_submit_intent_blocks_same_instrument_new_entry(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mgc_1x_asia_london_participation__asia_london_long_v5", "asia_london_participation_core_v1__MGC")])
    _write_submit_intent_ownership(
        tmp_path,
        state=SubmitIntentOwnershipState.REVIEW_REQUIRED,
        lane_id="other_mgc_lane",
        strategy_id="other_mgc_strategy",
        symbol="MGC",
        local_symbol="MGCM6",
        con_id=712565978,
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_1x_asia_london_participation__asia_london_long_v5",
        bridge_strategy_id="asia_london_participation_core_v1__MGC",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MGC",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_ENTRY_ALLOWED_REGISTRY_TRUTH_LEGACY_DIAGNOSTIC"
    assert gate["submit_allowed"] is True
    assert "TRACK_B_UNRESOLVED_SUBMIT_INTENT_BLOCKS_NEW_ENTRY" in gate["diagnostic_reason_codes"]
    assert gate["unresolved_submit_intent_ownership_blocker"]["matching_records"][0]["state"] == "REVIEW_REQUIRED"


def test_unresolved_submit_intent_does_not_block_managed_close(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])
    _write_submit_intent_ownership(
        tmp_path,
        lane_id="atp_companion_v1_asia_us",
        strategy_id="ATP_COMPANION_V1_ASIA_US",
        symbol="MGC",
        local_symbol="MGCM6",
        con_id=712565978,
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="atp_companion_v1_asia_us",
        bridge_strategy_id="ATP_COMPANION_V1_ASIA_US",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        executable_symbol="MGC",
        con_id=712565978,
        local_symbol="MGCM6",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert "TRACK_B_UNRESOLVED_SUBMIT_INTENT_BLOCKS_NEW_ENTRY" not in gate["block_reasons"]


def test_phase1_reconciliation_blocked_overrides_legacy_monitor_submit_allowed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_phase1_reconciliation(
        tmp_path,
        classification="TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED",
        broker_reconciled=True,
        review_required_count=1,
        block_reasons=["review_required_present"],
    )
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
        allow_stacking=True,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["submit_allowed"] is False
    assert gate["blocker_classification"] == "PHASE1_BROKER_RECONCILIATION_NOT_CLEAR"
    assert gate["review_required"] is True
    assert "phase1_broker_reconciliation_not_clear" in gate["block_reasons"]


def test_blocks_duplicate_buy_from_same_strategy_while_already_long(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="atp_companion_v1_asia_us",
        bridge_strategy_id="ATP_COMPANION_V1_ASIA_US",
        action="BUY",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]


def test_allows_owning_strategy_exit(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="atp_companion_v1_asia_us",
        bridge_strategy_id="ATP_COMPANION_V1_ASIA_US",
        action="EXIT",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True


@pytest.mark.parametrize(
    ("symbol", "strategy_id", "lane_id", "bridge_strategy_id", "local_symbol"),
    [
        (
            "MNQ",
            "index_futures_ny_intraday_forced_core_v2__mnq_1x_ny_early_core__us_late_long",
            "mnq_1x_ny_early_core__us_late_long",
            "index_futures_ny_intraday_forced_core_v2__MNQ",
            "MNQM6",
        ),
        (
            "PL",
            "atp_companion_v1__paper_pl_asia_us",
            "atp_companion_v1_pl_asia_us",
            "active_trend_participation_engine__PL",
            "PLN6",
        ),
    ],
)
def test_allows_ticker_agnostic_owning_strategy_exit_from_phase1_reconciliation(
    tmp_path: Path,
    symbol: str,
    strategy_id: str,
    lane_id: str,
    bridge_strategy_id: str,
    local_symbol: str,
) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row(lane_id, strategy_id)])
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "strategy_id": strategy_id,
                "track_b_root": symbol,
                "instrument_family": symbol,
                "contract_key": f"{symbol}-202606",
                "local_symbol": local_symbol,
                "quantity": "1",
                "side": "LONG",
                "avg_entry_price": "100",
                "entry_order_id": "1",
                "lifecycle_id": f"bridge_fill_{symbol}",
            }
        ],
    )
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[{"symbol": symbol, "local_symbol": local_symbol, "quantity": "1.0"}],
    )
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id=lane_id,
        bridge_strategy_id=bridge_strategy_id,
        executable_symbol=symbol,
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["owned_strategy_quantity"] == 1.0
    assert gate["aggregate_broker_position"] == 1.0


def test_flat_mnq_sell_to_open_short_entry_is_allowed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(
        tmp_path,
        [_governance_row("mnq_1x_ny_early_core__us_early_short_reclaim_fail", "index_futures_ny_intraday_forced_core_v2__MNQ")],
    )
    _write_broker_positions_snapshot(tmp_path)
    _write_broker_open_orders_snapshot(tmp_path)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_short_reclaim_fail",
        bridge_strategy_id="index_futures_ny_intraday_forced_core_v2__MNQ",
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_OPEN",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_ENTRY_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["intent_operation"] == "OPEN"
    assert gate["intent_direction"] == "SHORT"
    assert gate["broker_action"] == "SELL"
    assert gate["block_reasons"] == []


def test_registry_truth_blocks_entry_when_contract_is_not_entry_allowed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_broker_positions_snapshot(tmp_path)
    _write_broker_open_orders_snapshot(tmp_path)
    _write_contract_status(tmp_path, entry_status="CONTRACT_ENTRY_CLOSE_ONLY")

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        executable_symbol="MNQ",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        trade_id="trade_entry_contract_close_only",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["submit_allowed"] is False
    assert gate["authoritative_source"] == "REGISTRY_TRUTH"
    assert "CONTRACT_ENTRY_CLOSE_ONLY" in gate["block_reasons"]


def test_registry_truth_blocks_duplicate_same_lane_entry(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_us_active_participation_long", "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1")])
    _write_broker_positions_snapshot(tmp_path)
    _write_broker_open_orders_snapshot(tmp_path)
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id="trade_existing_mnq_long",
        lifecycle_id="lifecycle_existing_mnq_long",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_canonical_current_scope(tmp_path, managed_position_count=1)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        executable_symbol="MNQ",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        trade_id="trade_new_mnq_long",
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["submit_allowed"] is False
    assert "duplicate_same_lane_entry" in gate["block_reasons"]
    assert gate["registry_truth_result"]["matching_registry_trade_ids"] == ["trade_existing_mnq_long"]


def test_registry_truth_blocks_entry_without_birth_identity(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("", "")])
    _write_broker_positions_snapshot(tmp_path)
    _write_broker_open_orders_snapshot(tmp_path)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="",
        bridge_strategy_id="",
        executable_symbol="",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["submit_allowed"] is False
    assert "missing_or_invalid_trade_id_birth_path" in gate["block_reasons"]


def test_blocks_opposite_direction_open_against_existing_phase1_position(tmp_path: Path) -> None:
    existing_lifecycle_id = "bridge_fill_3c23e0f6-b19f-42e5-9582-28dee7b600b7"
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(
        tmp_path,
        [
            _governance_row(
                "atp_companion_v1_gc_asia_promotion_1_075r_favorable_only_5m",
                "atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_5m",
            ),
            _governance_row(
                "atp_companion_v1_gc_asia_us_production_track_selective_v1",
                "atp_companion_v1__production_track_gc_asia_us_selective_v1",
            ),
        ],
    )
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "strategy_id": "atp_companion_v1__production_track_gc_asia_us_selective_v1",
                "track_b_root": "GC",
                "instrument_family": "GC",
                "contract_key": "GC-202606",
                "local_symbol": "GCM6",
                "con_id": 430360630,
                "quantity": "1",
                "side": "LONG",
                "avg_entry_price": "4578.5",
                "entry_order_id": "3",
                "lifecycle_id": existing_lifecycle_id,
            }
        ],
    )
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "GC",
                "local_symbol": "GCM6",
                "con_id": 430360630,
                "quantity": "1.0",
            }
        ],
    )
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="atp_companion_v1_gc_asia_promotion_1_075r_favorable_only_5m",
        bridge_strategy_id="atp_companion_v1__paper_gc_asia__promotion_1_075r_favorable_only_5m",
        executable_symbol="GC",
        action="SELL",
        intent_type="SELL_TO_OPEN",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]
    assert gate["aggregate_strategy_position_sum"] == 1.0
    assert gate["aggregate_broker_position"] == 1.0


def test_mgc_plus_one_blocks_session_coverage_review_long_without_pyramiding(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(
        tmp_path,
        [
            _governance_row(
                "mgc_asia_late_flat_pullback_pause_resume_long",
                "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
            ),
        ],
    )
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                "track_b_root": "MGC",
                "instrument_family": "MGC",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "con_id": 712565978,
                "quantity": "1",
                "side": "LONG",
                "avg_entry_price": "4586.7",
                "entry_order_id": "7",
                "lifecycle_id": "bridge_fill_existing_mgc_plus_one",
            }
        ],
    )
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[{"account_id": "DUM882026", "symbol": "MGC", "local_symbol": "MGCM6", "con_id": 712565978, "quantity": "1.0"}],
    )
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_asia_late_flat_pullback_pause_resume_long",
        bridge_strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        executable_symbol="MGC",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]
    assert gate["aggregate_strategy_position_sum"] == 1.0
    assert gate["aggregate_broker_position"] == 1.0

    explicit_pyramid_gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_asia_late_flat_pullback_pause_resume_long",
        bridge_strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        executable_symbol="MGC",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        allow_stacking=True,
    )

    assert explicit_pyramid_gate["submit_allowed"] is False
    assert "current_broker_position_without_registry_trade" in explicit_pyramid_gate["block_reasons"]


def test_mgc_plus_one_blocks_session_coverage_review_short_without_reversal_policy(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(
        tmp_path,
        [
            _governance_row(
                "mgc_london_late_pause_resume_short",
                "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
            ),
        ],
    )
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
                "track_b_root": "MGC",
                "instrument_family": "MGC",
                "contract_key": "MGC-202606",
                "local_symbol": "MGCM6",
                "con_id": 712565978,
                "quantity": "1",
                "side": "LONG",
                "avg_entry_price": "4586.7",
                "entry_order_id": "7",
                "lifecycle_id": "bridge_fill_existing_mgc_plus_one",
            }
        ],
    )
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[{"account_id": "DUM882026", "symbol": "MGC", "local_symbol": "MGCM6", "con_id": 712565978, "quantity": "1.0"}],
    )
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mgc_london_late_pause_resume_short",
        bridge_strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        executable_symbol="MGC",
        action="SELL",
        intent_type="SELL_TO_OPEN",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]
    assert gate["aggregate_strategy_position_sum"] == 1.0
    assert gate["aggregate_broker_position"] == 1.0


def test_pl_lane_can_exit_turn_owner_from_clean_phase1_reconciliation(tmp_path: Path) -> None:
    lifecycle_id = "bridge_fill_PL|1m|2026-05-14T17:52:00Z|BUY_TO_OPEN"
    trade_id = "trade_pl_turn_registry"
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(
        tmp_path,
        [_governance_row("pl_us_late_pause_resume_long", "pl_us_late_pause_resume_long__PL")],
    )
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "strategy_id": "pl_us_late_pause_resume_long_turn__PL",
                "track_b_root": "PL",
                "instrument_family": "PL",
                "contract_key": "PL-202607",
                "local_symbol": "PLN6",
                "con_id": 644855286,
                "quantity": "1",
                "side": "LONG",
                "avg_entry_price": "2086.2",
                "entry_order_id": "1",
                "lifecycle_id": lifecycle_id,
            }
        ],
        broker_positions=[
            {
                "account_id": "DUM882026",
                "symbol": "PL",
                "local_symbol": "PLN6",
                "con_id": 644855286,
                "quantity": "1.0",
            }
        ],
    )
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[{"symbol": "PL", "local_symbol": "PLN6", "con_id": 644855286, "quantity": "1.0"}],
    )
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="pl_us_late_pause_resume_long",
        thesis_strategy_id="pl_us_late_pause_resume_long_turn__PL",
        symbol="PL",
        con_id=644855286,
        local_symbol="PLN6",
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="pl_us_late_pause_resume_long",
        bridge_strategy_id="pl_us_late_pause_resume_long__PL",
        executable_symbol="PL",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=644855286,
        local_symbol="PLN6",
        lifecycle_id=lifecycle_id,
        trade_id=trade_id,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["owned_strategy_quantity"] == 1.0
    assert gate["exit_identity_requested"] is True


def test_pl_exit_blocks_when_exact_identity_does_not_match_phase1_owner(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(
        tmp_path,
        [_governance_row("pl_us_late_pause_resume_long", "pl_us_late_pause_resume_long__PL")],
    )
    _write_phase1_reconciliation(
        tmp_path,
        lifecycle_positions=[
            {
                "account_id": "DUM882026",
                "strategy_id": "pl_us_late_pause_resume_long_turn__PL",
                "track_b_root": "PL",
                "instrument_family": "PL",
                "contract_key": "PL-202607",
                "local_symbol": "PLN6",
                "con_id": 644855286,
                "quantity": "1",
                "side": "LONG",
                "avg_entry_price": "2086.2",
                "lifecycle_id": "bridge_fill_PL|1m|2026-05-14T17:52:00Z|BUY_TO_OPEN",
            }
        ],
    )
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[{"symbol": "PL", "local_symbol": "PLN6", "con_id": 644855286, "quantity": "1.0"}],
    )
    _write_broker_open_orders_snapshot(tmp_path, open_orders=[])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="pl_us_late_pause_resume_long",
        bridge_strategy_id="pl_us_late_pause_resume_long__PL",
        executable_symbol="PL",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=999999,
        local_symbol="PLN6",
    )

    assert gate["submit_allowed"] is False
    assert "exit_identity_mismatch" in gate["block_reasons"]
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


@pytest.mark.parametrize(
    ("symbol", "lane_id", "thesis_strategy_id", "lifecycle_id", "con_id", "local_symbol"),
    [
        (
            "MNQ",
            "mnq_us_active_participation_long",
            "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
            "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN",
            770561201,
            "MNQM6",
        ),
        (
            "MES",
            "mes_globex_active_participation_long",
            "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
            "bridge_fill_MES|1m|2026-05-29T05:54:00Z|BUY_TO_OPEN",
            770561194,
            "MESM6",
        ),
        (
            "MGC",
            "mgc_asia_late_flat_pullback_pause_resume_long",
            "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
            "bridge_fill_MGC|1m|2026-05-29T01:05:00Z|BUY_TO_OPEN",
            712565978,
            "MGCM6",
        ),
        (
            "GC",
            "gc_1x_asia_london_participation_long",
            "GC_ASIA_LONDON_PARTICIPATION_LONG_V1",
            "bridge_fill_GC|1m|2026-05-29T01:05:00Z|BUY_TO_OPEN",
            430360630,
            "GCM6",
        ),
        (
            "ZC",
            "zc_ag_paper_managed_long",
            "AGRICULTURE_MANAGED_PAPER_ZC_LONG_V1",
            "bridge_fill_ZC|1m|2026-05-29T14:05:00Z|BUY_TO_OPEN",
            800100101,
            "ZCN6",
        ),
        (
            "BTC",
            "btc_crypto_paper_managed_long",
            "CRYPTO_MANAGED_PAPER_BTC_LONG_V1",
            "bridge_fill_BTC|1m|2026-05-29T14:05:00Z|BUY_TO_OPEN",
            900200202,
            "BTCUSD",
        ),
    ],
)
def test_exact_lifecycle_identity_allows_managed_close_when_lane_differs_from_thesis_and_account_is_aggregate(
    tmp_path: Path,
    symbol: str,
    lane_id: str,
    thesis_strategy_id: str,
    lifecycle_id: str,
    con_id: int,
    local_symbol: str,
) -> None:
    _write_broker_backed_managed_position(
        tmp_path,
        symbol=symbol,
        lane_id=lane_id,
        thesis_strategy_id=thesis_strategy_id,
        lifecycle_id=lifecycle_id,
        con_id=con_id,
        local_symbol=local_symbol,
    )
    trade_id = f"trade_{symbol.lower()}_registry_backed"
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=lane_id,
        thesis_strategy_id=thesis_strategy_id,
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id=lane_id,
        bridge_strategy_id=thesis_strategy_id,
        executable_symbol=symbol,
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=con_id,
        local_symbol=local_symbol,
        lifecycle_id=lifecycle_id,
        trade_id=trade_id,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["owned_strategy_quantity"] == 1.0
    assert "exit_identity_mismatch" not in gate["block_reasons"]
    assert gate["registry_exit_validation"]["allowed"] is True
    assert gate["authoritative_source"] == "REGISTRY_TRUTH"
    assert gate["registry_truth_result"]["allowed"] is True
    assert gate["legacy_result"]["diagnostic_only"] is True
    assert gate["parity_status"] == "MATCH_ALLOWED"


def test_registry_backed_managed_close_requires_trade_id(tmp_path: Path) -> None:
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MNQ",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        lifecycle_id=lifecycle_id,
        con_id=770561201,
        local_symbol="MNQM6",
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        lifecycle_id=lifecycle_id,
    )

    assert gate["submit_allowed"] is False
    assert "missing_trade_id" in gate["block_reasons"]


def test_registry_truth_allows_managed_close_when_legacy_snapshot_is_stale_aggregate_mismatch(
    tmp_path: Path,
) -> None:
    trade_id = "trade_mnq_registry_authoritative"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MNQ",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        lifecycle_id=lifecycle_id,
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_broker_positions_snapshot(tmp_path, positions=[])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        lifecycle_id=lifecycle_id,
        trade_id=trade_id,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["authoritative_source"] == "REGISTRY_TRUTH"
    assert gate["registry_truth_result"]["allowed"] is True
    assert gate["legacy_result"]["allowed"] is False
    assert gate["parity_status"] == "REGISTRY_TRUTH_ALLOWED_LEGACY_BLOCKED_DIAGNOSTIC"
    assert "broker_position_does_not_support_requested_exit" in gate["diagnostic_reason_codes"]


@pytest.mark.parametrize(
    ("override", "expected_blocker"),
    [
        ({"con_id": 999999999}, "con_id_mismatch"),
        ({"local_symbol": "MNQU6"}, "local_symbol_mismatch"),
        ({"quantity": 2.0}, "quantity_mismatch"),
    ],
)
def test_registry_backed_managed_close_identity_mismatches_fail_closed(
    tmp_path: Path,
    override: dict[str, object],
    expected_blocker: str,
) -> None:
    trade_id = "trade_mnq_assisted_registry"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MNQ",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        lifecycle_id=lifecycle_id,
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
    )
    params = {"con_id": 770561201, "local_symbol": "MNQM6", "quantity": 1.0}
    params.update(override)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=float(params["quantity"]),
        account_id="DUM882026",
        con_id=params["con_id"],  # type: ignore[arg-type]
        local_symbol=str(params["local_symbol"]),
        lifecycle_id=lifecycle_id,
        trade_id=trade_id,
    )

    assert gate["submit_allowed"] is False
    assert expected_blocker in gate["block_reasons"]


def test_exact_lifecycle_identity_blocks_true_non_owner_managed_close(tmp_path: Path) -> None:
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MNQ",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        lifecycle_id=lifecycle_id,
        con_id=770561201,
        local_symbol="MNQM6",
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_short",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        lifecycle_id=lifecycle_id,
    )

    assert gate["submit_allowed"] is False
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_registry_truth_blocks_true_non_owner_even_with_valid_trade_id(tmp_path: Path) -> None:
    trade_id = "trade_mnq_true_owner"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MNQ",
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        lifecycle_id=lifecycle_id,
        con_id=770561201,
        local_symbol="MNQM6",
    )
    _write_registry_open_managed_trade(
        tmp_path,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_us_active_participation_short",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1",
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        lifecycle_id=lifecycle_id,
        trade_id=trade_id,
    )

    assert gate["submit_allowed"] is False
    assert gate["authoritative_source"] == "REGISTRY_TRUTH"
    assert gate["registry_truth_result"]["allowed"] is False
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_registry_truth_blocks_managed_close_without_broker_backed_entry_evidence(tmp_path: Path) -> None:
    trade_id = "trade_mnq_local_only"
    lifecycle_id = "bridge_fill_MNQ|1m|2026-05-29T18:33:00Z|BUY_TO_OPEN"
    lane_id = "mnq_us_active_participation_long"
    thesis_strategy_id = "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MNQ",
        lane_id=lane_id,
        thesis_strategy_id=thesis_strategy_id,
        lifecycle_id=lifecycle_id,
        con_id=770561201,
        local_symbol="MNQM6",
    )
    base = {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": lane_id,
        "thesis_strategy_id": thesis_strategy_id,
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "expiry": "202606",
        "side": "LONG",
        "action": "BUY",
        "qty": "1",
        "source_artifact_path": str(tmp_path / "local_paper_only.json"),
        "generated_at": datetime(2026, 5, 31, 12, 0, tzinfo=UTC),
    }
    append_live_trade_registry_event(
        repo_root=tmp_path,
        event=make_live_trade_registry_event(event_type=TradeEventType.ENTRY_INTENT_CREATED, **base),
    )
    append_live_trade_registry_event(
        repo_root=tmp_path,
        event=make_live_trade_registry_event(event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED, **base),
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id=lane_id,
        bridge_strategy_id=thesis_strategy_id,
        executable_symbol="MNQ",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        lifecycle_id=lifecycle_id,
        trade_id=trade_id,
    )

    assert gate["submit_allowed"] is False
    assert gate["authoritative_source"] == "REGISTRY_TRUTH"
    assert gate["registry_truth_result"]["allowed"] is False
    assert any(
        reason in gate["block_reasons"]
        for reason in {"trade_registry_entry_not_broker_backed", "trade_registry_state_not_open_managed"}
    )


@pytest.mark.parametrize(
    ("override", "expected_blocker"),
    [
        ({"quantity": 2.0}, "exit_quantity_exceeds_owned_strategy_position"),
        ({"account_id": "OTHER123"}, "exit_identity_mismatch"),
        ({"con_id": 999999999}, "exit_identity_mismatch"),
        ({"lifecycle_id": None}, "missing_lifecycle_identity"),
    ],
)
def test_managed_close_exact_identity_mismatches_fail_closed(
    tmp_path: Path,
    override: dict[str, object],
    expected_blocker: str,
) -> None:
    lifecycle_id = "bridge_fill_MES|1m|2026-05-29T05:54:00Z|BUY_TO_OPEN"
    _write_broker_backed_managed_position(
        tmp_path,
        symbol="MES",
        lane_id="mes_globex_active_participation_long",
        thesis_strategy_id="PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
        lifecycle_id=lifecycle_id,
        con_id=770561194,
        local_symbol="MESM6",
    )
    params = {
        "quantity": 1.0,
        "account_id": "DUM882026",
        "con_id": 770561194,
        "local_symbol": "MESM6",
        "lifecycle_id": lifecycle_id,
    }
    params.update(override)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mes_globex_active_participation_long",
        bridge_strategy_id="PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
        executable_symbol="MES",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=float(params["quantity"]),
        account_id=str(params["account_id"]),
        con_id=params["con_id"],  # type: ignore[arg-type]
        local_symbol=str(params["local_symbol"]),
        lifecycle_id=params["lifecycle_id"],  # type: ignore[arg-type]
    )

    assert gate["submit_allowed"] is False
    assert expected_blocker in gate["block_reasons"]


def test_flat_sell_to_close_blocks(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("strategy_long", "strategy_long")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="strategy_long",
        bridge_strategy_id="strategy_long",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_flat_buy_to_close_blocks(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("strategy_short", "strategy_short")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="strategy_short",
        bridge_strategy_id="strategy_short",
        action="BUY",
        intent_type="BUY_TO_CLOSE",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_long_sell_to_close_is_allowed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("strategy_long", side="LONG")])
    _write_governance(tmp_path, [_governance_row("strategy_long", "strategy_long")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="strategy_long",
        bridge_strategy_id="strategy_long",
        action="SELL",
        intent_type="SELL_TO_CLOSE",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["intent_operation"] == "CLOSE"
    assert gate["intent_direction"] == "LONG"


def test_short_buy_to_close_is_allowed(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=-1.0)
    _write_ledger(tmp_path, [_position("strategy_short", side="SHORT")])
    _write_governance(tmp_path, [_governance_row("strategy_short", "strategy_short")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="strategy_short",
        bridge_strategy_id="strategy_short",
        action="BUY",
        intent_type="BUY_TO_CLOSE",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_EXIT_ALLOWED"
    assert gate["submit_allowed"] is True
    assert gate["intent_operation"] == "CLOSE"
    assert gate["intent_direction"] == "SHORT"


def test_flat_ambiguous_sell_without_short_entry_semantics_blocks(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("strategy_unknown", "strategy_unknown")])

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="strategy_unknown",
        bridge_strategy_id="strategy_unknown",
        action="SELL",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert gate["intent_operation"] == "CLOSE"
    assert gate["intent_direction"] == "LONG"
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_blocks_non_owning_strategy_exit(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="EXIT",
        quantity=1.0,
    )

    assert gate["submit_allowed"] is False
    assert "non_owning_strategy_exit_forbidden" in gate["block_reasons"]


def test_detects_broker_net_vs_strategy_ledger_mismatch(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=2.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US", qty=1.0)])
    _write_governance(tmp_path, [_governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US")])

    artifacts = run_ibkr_paper_strategy_exposure(config=IbkrPaperStrategyExposureConfig(repo_root=tmp_path))

    assert artifacts.classification == "PAPER_EXPOSURE_BLOCKED_LEDGER_BROKER_MISMATCH"
    assert artifacts.aggregate_exposure_state["discrepancy_classification"] == "LEDGER_BROKER_MISMATCH"


def test_detects_orphan_broker_position(tmp_path: Path) -> None:
    _write_monitor(
        tmp_path,
        broker_quantity=1.0,
        orphan_positions=[{"symbol": "MGC", "quantity": 1.0, "detail": "unmatched"}],
    )
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [])

    artifacts = run_ibkr_paper_strategy_exposure(config=IbkrPaperStrategyExposureConfig(repo_root=tmp_path))

    assert artifacts.classification == "PAPER_EXPOSURE_BLOCKED_ORPHAN_POSITION"
    assert artifacts.aggregate_exposure_state["discrepancy_classification"] == "ORPHAN_BROKER_POSITION"


def test_blocks_mnq_entry_when_broker_position_is_not_strategy_owned(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_1x_ny_early_core__us_early_long", "mnq_1x_ny_early_core__us_early_long")])
    _write_broker_positions_snapshot(
        tmp_path,
        positions=[
            {
                "account_id": "DUM882026",
                "symbol": "MNQ",
                "local_symbol": "MNQM6",
                "expiry": "20260618",
                "quantity": "1.0",
                "average_cost": "58616.12",
            }
        ],
    )
    _write_broker_open_orders_snapshot(tmp_path)

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_long",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert gate["blocker_classification"] == "BROKER_LEDGER_POSITION_MISMATCH"
    assert gate["review_required"] is True
    assert "ledger_broker_mismatch" in gate["block_reasons"]
    assert gate["aggregate_broker_position"] == 1.0
    assert gate["aggregate_strategy_position_sum"] == 0


def test_blocks_mnq_entry_when_non_mgc_broker_truth_is_stale(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_1x_ny_early_core__us_early_long", "mnq_1x_ny_early_core__us_early_long")])
    _write_broker_positions_snapshot(tmp_path, generated_at="2026-01-01T00:00:00+00:00")

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_long",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["blocker_classification"] == "BROKER_TRUTH_STALE_OR_MISSING"
    assert gate["review_required"] is True
    assert "broker_position_truth_stale_or_missing" in gate["block_reasons"]
    assert gate["detail"] == "Registry/truth current-hot-path authority blocked this new entry."
    broker_truth = gate["broker_truth"]
    assert broker_truth["symbol"] == "MNQ"
    assert broker_truth["positions_snapshot"]["path"].endswith("ibkr_positions_snapshot.json")
    assert broker_truth["positions_snapshot"]["generated_at"] == "2026-01-01T00:00:00+00:00"
    assert broker_truth["open_orders_snapshot"]["path"].endswith("ibkr_open_orders_snapshot.json")
    assert broker_truth["required_freshness_threshold_seconds"] == 300.0
    assert broker_truth["account"] == "DUM882026"


def test_blocks_non_mgc_entry_when_broker_truth_refresh_is_incomplete(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=0.0)
    _write_ledger(tmp_path, [])
    _write_governance(tmp_path, [_governance_row("mnq_1x_ny_early_core__us_early_long", "mnq_1x_ny_early_core__us_early_long")])
    _write_broker_positions_snapshot(tmp_path)
    _write_broker_open_orders_snapshot(tmp_path)
    positions_path = tmp_path / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
    positions_payload = json.loads(positions_path.read_text(encoding="utf-8"))
    positions_payload["positions_complete"] = False
    positions_path.write_text(json.dumps(positions_payload), encoding="utf-8")

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="mnq_1x_ny_early_core__us_early_long",
        action="BUY",
        intent_type="BUY_TO_OPEN",
        quantity=1.0,
        executable_symbol="MNQ",
    )

    assert gate["submit_allowed"] is False
    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert gate["broker_truth"]["positions_snapshot"]["reason"] == "positions_complete_false_or_missing"


def test_honors_optional_aggregate_cap_when_configured(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
        max_total_mgc_contracts=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]


def test_does_not_impose_aggregate_cap_of_one_by_default(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=1.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US")])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
        allow_stacking=True,
    )

    assert gate["submit_allowed"] is False
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]
    assert gate["max_total_mgc_contracts"] == 20.0


def test_blocks_when_default_twenty_mgc_cap_would_be_exceeded(tmp_path: Path) -> None:
    _write_monitor(tmp_path, broker_quantity=20.0)
    _write_ledger(tmp_path, [_position("ATP_COMPANION_V1_ASIA_US", qty=20.0)])
    _write_governance(
        tmp_path,
        [
            _governance_row("gc_1x_all_lanes__asia_early_long", "gold_forced_session_baseline_v2__GC"),
            _governance_row("atp_companion_v1_asia_us", "ATP_COMPANION_V1_ASIA_US"),
        ],
    )

    gate = evaluate_paper_strategy_exposure_gate(
        repo_root=tmp_path,
        strategy_id="gc_1x_all_lanes__asia_early_long",
        bridge_strategy_id="gold_forced_session_baseline_v2__GC",
        action="BUY",
        quantity=1.0,
    )

    assert gate["classification"] == "PAPER_EXPOSURE_BLOCKED_REGISTRY_TRUTH_ENTRY"
    assert "current_broker_position_without_registry_trade" in gate["block_reasons"]
