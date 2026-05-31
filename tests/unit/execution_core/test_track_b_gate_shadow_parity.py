from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    CONTRACT_ENTRY_ELIGIBLE,
    CONTROL_PLANE_STALE,
    SAFE_STATE_SUBMIT_BLOCKED,
    SourceAuthorityLevel,
    SourceClassification,
    TrackBTruthSnapshot,
    TrackBTruthSource,
    RuntimeTruthSection,
    RecoveryTruthSection,
    BrokerTruthSection,
    LifecycleTruthSection,
    ReconciliationTruthSection,
    SafeStateTruthSection,
    ControlPlaneTruthSection,
    PlannerSupervisorTruthSection,
    ContractTruthSection,
    BrokerBackedEvidenceSection,
    TruthConflict,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeCurrentState,
    TradeEvent,
    TradeEventType,
    reduce_trade_events,
)
from mgc_v05l.execution_core.track_b_gate_shadow_parity import (
    GATE_SHADOW_PARITY_OK,
    GATE_SHADOW_REVIEW_REQUIRED,
    TrackBGateName,
    TrackBGateShadowContext,
    evaluate_track_b_gate_shadow_parity,
)


NOW = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)
LANE_ID = "mnq_us_active_participation_long"
STRATEGY_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1"


def test_clean_flat_entry_allowed_parity(tmp_path: Path) -> None:
    report = evaluate_track_b_gate_shadow_parity(
        _context(tmp_path, action="BUY_TO_OPEN", truth_snapshot=_truth(), existing_allowed=True)
    )

    assert report.classification == GATE_SHADOW_PARITY_OK
    assert report.parity is True
    assert _row(report, TrackBGateName.ENTRY_EXPOSURE).registry_truth_gate_result.allowed is True
    assert report.behavior_change_allowed is False
    assert report.broker_mutation_allowed is False


def test_open_managed_trade_blocks_duplicate_entry_parity(tmp_path: Path) -> None:
    record = _open_record(trade_id="trade_open_duplicate")
    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="BUY_TO_OPEN",
            truth_snapshot=_truth(broker_positions=1, lifecycle_positions=1),
            registry_records=(record,),
            existing_overrides={
                TrackBGateName.ENTRY_EXPOSURE.value: {
                    "submit_allowed": False,
                    "block_reasons": ["duplicate_strategy_entry_while_position_open"],
                }
            },
        )
    )

    row = _row(report, TrackBGateName.ENTRY_EXPOSURE)
    assert row.parity is True
    assert row.registry_truth_gate_result.allowed is False
    assert "DUPLICATE_STRATEGY_ENTRY_WHILE_POSITION_OPEN" in row.registry_truth_gate_result.reason_codes


def test_managed_exit_allowed_parity(tmp_path: Path) -> None:
    record = _open_record(trade_id="trade_exit_ready")
    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="SELL_TO_CLOSE",
            trade_id="trade_exit_ready",
            lifecycle_id="life_trade_exit_ready",
            truth_snapshot=_truth(broker_positions=1, lifecycle_positions=1),
            registry_records=(record,),
            existing_allowed=True,
        )
    )

    row = _row(report, TrackBGateName.MANAGED_EXIT_EXPOSURE)
    assert row.parity is True
    assert row.registry_truth_gate_result.allowed is True
    assert row.registry_truth_gate_result.trade_id == "trade_exit_ready"


def test_stale_control_plane_blocks_parity(tmp_path: Path) -> None:
    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="BUY_TO_OPEN",
            truth_snapshot=_truth(control_plane_fresh=False),
            existing_overrides={
                TrackBGateName.GOVERNANCE_SUBMIT.value: {
                    "submit_allowed": False,
                    "block_reasons": ["control_plane_not_ready"],
                }
            },
        )
    )

    row = _row(report, TrackBGateName.GOVERNANCE_SUBMIT)
    assert row.parity is True
    assert row.registry_truth_gate_result.allowed is False
    assert CONTROL_PLANE_STALE in row.registry_truth_gate_result.reason_codes


def test_safe_state_blocked_parity(tmp_path: Path) -> None:
    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="BUY_TO_OPEN",
            truth_snapshot=_truth(safe_submit=False),
            existing_overrides={
                TrackBGateName.SAFE_STATE_SUBMIT.value: {
                    "submit_allowed": False,
                    "block_reasons": ["safe_state_submit_not_allowed"],
                },
                TrackBGateName.GOVERNANCE_SUBMIT.value: {
                    "submit_allowed": False,
                    "block_reasons": ["safe_state_submit_not_allowed"],
                },
            },
        )
    )

    safe_row = _row(report, TrackBGateName.SAFE_STATE_SUBMIT)
    governance_row = _row(report, TrackBGateName.GOVERNANCE_SUBMIT)
    assert safe_row.parity is True
    assert governance_row.parity is True
    assert SAFE_STATE_SUBMIT_BLOCKED in governance_row.registry_truth_gate_result.reason_codes


def test_open_order_blocks_parity(tmp_path: Path) -> None:
    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="BUY_TO_OPEN",
            truth_snapshot=_truth(open_orders=1),
            existing_overrides={
                TrackBGateName.NO_WORKING_ORDER.value: {
                    "passed": False,
                    "block_reasons": ["open_working_order_present"],
                },
                TrackBGateName.GOVERNANCE_SUBMIT.value: {
                    "submit_allowed": False,
                    "block_reasons": ["open_working_order_present"],
                },
                TrackBGateName.ENTRY_EXPOSURE.value: {
                    "submit_allowed": False,
                    "block_reasons": ["open_working_order_present"],
                },
            },
        )
    )

    assert _row(report, TrackBGateName.NO_WORKING_ORDER).parity is True
    assert _row(report, TrackBGateName.GOVERNANCE_SUBMIT).parity is True
    assert _row(report, TrackBGateName.ENTRY_EXPOSURE).parity is True


def test_registry_conflict_blocks_parity(tmp_path: Path) -> None:
    conflict = TruthConflict(
        classification="TRUTH_CONFLICT_REVIEW_REQUIRED",
        question="Are registry and broker/lifecycle facts coherent?",
        authoritative_source="central_trade_registry",
        conflicting_sources=("broker_truth",),
        reason_codes=("REGISTRY_CONFLICT_REVIEW_REQUIRED",),
    )
    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="BUY_TO_OPEN",
            truth_snapshot=_truth(conflicts=(conflict,)),
            existing_overrides={
                TrackBGateName.ENTRY_EXPOSURE.value: {
                    "submit_allowed": False,
                    "block_reasons": ["registry_conflict_review_required"],
                },
                TrackBGateName.GOVERNANCE_SUBMIT.value: {
                    "submit_allowed": False,
                    "block_reasons": ["registry_conflict_review_required"],
                },
            },
        )
    )

    assert report.classification == GATE_SHADOW_REVIEW_REQUIRED
    assert report.parity is True
    assert "REGISTRY_CONFLICT_REVIEW_REQUIRED" in report.reason_codes


def test_historical_registry_artifacts_quarantined_do_not_block_current_hot_path(tmp_path: Path) -> None:
    historical_review = _review_record(trade_id="trade_historical_review")
    historical_closed = _closed_record(trade_id="trade_historical_closed")

    report = evaluate_track_b_gate_shadow_parity(
        _context(
            tmp_path,
            action="BUY_TO_OPEN",
            truth_snapshot=_truth(),
            registry_records=(historical_review, historical_closed),
            existing_allowed=True,
        )
    )

    assert report.classification == GATE_SHADOW_PARITY_OK
    assert report.parity is True
    assert _row(report, TrackBGateName.ENTRY_EXPOSURE).registry_truth_gate_result.allowed is True


def _context(
    tmp_path: Path,
    *,
    action: str,
    truth_snapshot: TrackBTruthSnapshot,
    registry_records=(),
    trade_id: str | None = None,
    lifecycle_id: str | None = None,
    existing_allowed: bool | None = None,
    existing_overrides: dict[str, dict] | None = None,
) -> TrackBGateShadowContext:
    existing = _existing_results(allowed=True if existing_allowed is None else existing_allowed)
    for key, value in (existing_overrides or {}).items():
        existing[key] = value
    return TrackBGateShadowContext(
        repo_root=tmp_path,
        lane_id=LANE_ID,
        thesis_strategy_id=STRATEGY_ID,
        action=action,
        quantity=Decimal("1"),
        symbol="MNQ",
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        existing_gate_results=existing,
        truth_snapshot=truth_snapshot,
        registry_records=registry_records,
        generated_at=NOW,
    )


def _existing_results(*, allowed: bool) -> dict[str, dict]:
    return {
        TrackBGateName.ENTRY_EXPOSURE.value: {"submit_allowed": allowed, "block_reasons": [] if allowed else ["existing_block"]},
        TrackBGateName.MANAGED_EXIT_EXPOSURE.value: {"submit_allowed": allowed, "block_reasons": [] if allowed else ["existing_block"]},
        TrackBGateName.GOVERNANCE_SUBMIT.value: {"submit_allowed": allowed, "block_reasons": [] if allowed else ["existing_block"]},
        TrackBGateName.SAFE_STATE_SUBMIT.value: {"submit_allowed": allowed, "block_reasons": [] if allowed else ["existing_block"]},
        TrackBGateName.NO_WORKING_ORDER.value: {"passed": allowed, "block_reasons": [] if allowed else ["existing_block"]},
        TrackBGateName.RUNTIME_AUTHORITY.value: {"ready": allowed, "block_reasons": [] if allowed else ["existing_block"]},
    }


def _row(report, gate: TrackBGateName):
    return next(row for row in report.rows if row.gate_name == gate.value)


def _truth(
    *,
    broker_positions: int = 0,
    lifecycle_positions: int = 0,
    open_orders: int = 0,
    safe_submit: bool = True,
    control_plane_fresh: bool = True,
    conflicts: tuple[TruthConflict, ...] = (),
) -> TrackBTruthSnapshot:
    fresh = _source("fresh", fresh=True)
    stale = _source("stale", fresh=False)
    cp_source = fresh if control_plane_fresh else stale
    return TrackBTruthSnapshot(
        schema_version="track_b_truth_snapshot_v1",
        generated_at=NOW,
        snapshot_id="truth-test",
        paper_only=True,
        live_money_eligible=False,
        paper_proof_invoked=False,
        runtime=RuntimeTruthSection(
            runtime_alive=True,
            submit_capable=True,
            runtime_generation_id="generation-1",
            pid=123,
            lane_count=8,
            duplicate_writer_detected=False,
            source=fresh,
        ),
        recovery=RecoveryTruthSection(
            active=True,
            paused=False,
            classification="RECOVERY_ACTIVE",
            launchd_loaded=True,
            last_tick=NOW.isoformat(),
            source=fresh,
        ),
        broker_truth=BrokerTruthSection(
            fresh=True,
            open_order_count=open_orders,
            broker_position_count=broker_positions,
            positions=tuple({"symbol": "MNQ", "quantity": "1"} for _ in range(broker_positions)),
            open_orders=tuple({"symbol": "MNQ", "order_id": str(index)} for index in range(open_orders)),
            source=fresh,
        ),
        lifecycle=LifecycleTruthSection(
            fresh=True,
            open_position_count=lifecycle_positions,
            managed_order_count=0,
            open_positions=tuple({"lifecycle_id": f"life-{index}", "quantity": "1"} for index in range(lifecycle_positions)),
            exact_owner_resolved=bool(lifecycle_positions),
            aggregate_placeholder_accounts=(),
            source=fresh,
        ),
        reconciliation=ReconciliationTruthSection(
            fresh=True,
            reconciled=True,
            classification="BROKER_LIFECYCLE_RECONCILED",
            review_required_count=0,
            source=fresh,
        ),
        safe_state=SafeStateTruthSection(
            fresh=True,
            classification="SAFE_STATE_NORMAL" if safe_submit else "SAFE_STATE_BLOCKED",
            submit_allowed=safe_submit,
            runtime_start_allowed=True,
            source=fresh,
        ),
        control_plane=ControlPlaneTruthSection(
            fresh=control_plane_fresh,
            coherent=control_plane_fresh,
            snapshot_id="snapshot-1",
            shared_truth_generation_id="generation-1",
            classification="COHERENT" if control_plane_fresh else CONTROL_PLANE_STALE,
            source=cp_source,
            reason_codes=() if control_plane_fresh else (CONTROL_PLANE_STALE,),
        ),
        planner_supervisor=PlannerSupervisorTruthSection(
            planner_classification="PLAN_OK",
            supervisor_classification="SUPERVISOR_OK",
            planner_snapshot_id="snapshot-1",
            supervisor_decision_id="supervisor-1",
            snapshot_match=True,
            planner_source=fresh,
            supervisor_source=fresh,
        ),
        contract_status=ContractTruthSection(
            symbol="MNQ",
            selected_local_symbol="MNQM6",
            resolved_local_symbol="MNQM6",
            con_id=770561201,
            expiry="202606",
            entry_status=CONTRACT_ENTRY_ELIGIBLE,
            exit_status="EXIT_ORIGINAL_CONTRACT_ALLOWED",
            source=fresh,
        ),
        broker_backed_evidence=BrokerBackedEvidenceSection(
            broker_backed=True,
            order_id="1",
            client_id="17",
            perm_id="perm-1",
            exec_id="exec-1",
            source=fresh,
        ),
        conflicts=conflicts,
        reason_codes=tuple(code for conflict in conflicts for code in conflict.reason_codes),
        source_paths={},
    )


def _source(name: str, *, fresh: bool) -> TrackBTruthSource:
    return TrackBTruthSource(
        source_name=name,
        artifact_path=f"synthetic://{name}",
        generated_at=NOW if fresh else NOW,
        freshness_seconds=0.0 if fresh else 999.0,
        max_age_seconds=120.0,
        fresh=fresh,
        classification=SourceClassification.FRESH.value if fresh else SourceClassification.STALE.value,
        authority_level=SourceAuthorityLevel.AUTHORITATIVE.value,
    )


def _open_record(*, trade_id: str):
    events = [
        _event(trade_id, TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=0, order_id="1", client_id="17", perm_id="perm-1", exec_id="exec-1"),
        _event(trade_id, TradeEventType.LIFECYCLE_OPEN_MANAGED, seconds=1),
    ]
    record = reduce_trade_events(events)
    assert record.current_state == TradeCurrentState.OPEN_MANAGED
    return record


def _closed_record(*, trade_id: str):
    return reduce_trade_events(
        [
            _event(trade_id, TradeEventType.ENTRY_FILL_BROKER_BACKED, seconds=0, order_id="1", client_id="17", perm_id="perm-1", exec_id="exec-1"),
            _event(trade_id, TradeEventType.LIFECYCLE_OPEN_MANAGED, seconds=1),
            _event(trade_id, TradeEventType.EXIT_FILL_BROKER_BACKED, seconds=2, action="SELL_TO_CLOSE", order_id="2", client_id="17", perm_id="perm-2", exec_id="exec-2"),
            _event(trade_id, TradeEventType.RECONCILED_FLAT, seconds=3, action="SELL_TO_CLOSE"),
        ]
    )


def _review_record(*, trade_id: str):
    return reduce_trade_events(
        [
            _event(trade_id, TradeEventType.REVIEW_REQUIRED, seconds=0, reason_codes=("HISTORICAL_REVIEW_REQUIRED",)),
        ]
    )


def _event(
    trade_id: str,
    event_type: TradeEventType,
    *,
    seconds: int,
    action: str = "BUY_TO_OPEN",
    order_id: str | None = None,
    client_id: str | None = None,
    perm_id: str | None = None,
    exec_id: str | None = None,
    reason_codes=(),
) -> TradeEvent:
    return TradeEvent(
        event_id=f"{trade_id}_{seconds}_{event_type.value}",
        event_type=event_type,
        generated_at=NOW,
        trade_id=trade_id,
        lifecycle_id=f"life_{trade_id}",
        lane_id=LANE_ID,
        thesis_strategy_id=STRATEGY_ID,
        account_id="DUM882026",
        symbol="MNQ",
        con_id=770561201,
        local_symbol="MNQM6",
        expiry="202606",
        side="LONG",
        action=action,
        qty=Decimal("1"),
        source_artifact_path="synthetic://gate-shadow",
        order_id=order_id,
        client_id=client_id,
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal("100"),
        reason_codes=tuple(reason_codes),
    )
