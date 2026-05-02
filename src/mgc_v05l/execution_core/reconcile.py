"""Track B proof-specific broker-vs-ledger reconciliation."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Sequence

from .models import (
    BrokerOrder,
    FillEvent,
    PositionSource,
    PositionState,
    ReconciliationResult,
    ReconciliationStage,
    ReconciliationStatus,
)

BROKER_UNAVAILABLE = "BROKER_UNAVAILABLE"
ACCOUNT_MISMATCH = "ACCOUNT_MISMATCH"
CONTRACT_MISMATCH = "CONTRACT_MISMATCH"
MISSING_FILL = "MISSING_FILL"
OPEN_ORDER_MISMATCH = "OPEN_ORDER_MISMATCH"
UNOWNED_POSITION = "UNOWNED_POSITION"
AMBIGUOUS_BROKER_STATUS = "AMBIGUOUS_BROKER_STATUS"
BROKER_LEDGER_DISAGREEMENT = "BROKER_LEDGER_DISAGREEMENT"
LEDGER_CORRUPTION = "LEDGER_CORRUPTION"

_POST_SUBMIT_STAGES = {
    ReconciliationStage.POST_OPEN,
    ReconciliationStage.PRE_CLOSE,
    ReconciliationStage.POST_CLOSE,
    ReconciliationStage.FINAL,
}


def reconcile_position(
    *,
    run_id: str,
    stage: ReconciliationStage | str,
    account_id: str,
    contract_key: str,
    broker_position: PositionState | None,
    ledger_position: PositionState | None,
    broker_open_orders: Sequence[str | BrokerOrder] = (),
    ledger_open_order_ids: Sequence[str] = (),
    fill_events: Sequence[FillEvent] = (),
    broker_connected: bool = True,
    ambiguous_broker_status: bool = False,
    expected_signed_quantity: int | None = None,
    created_at: datetime | None = None,
) -> ReconciliationResult:
    """Compare one account/contract broker truth to ledger-owned state."""

    normalized_stage = stage if isinstance(stage, ReconciliationStage) else ReconciliationStage(str(stage).strip().upper())
    expected_quantity = _expected_quantity(
        stage=normalized_stage,
        expected_signed_quantity=expected_signed_quantity,
        ledger_position=ledger_position,
    )
    issues: list[str] = []

    if not broker_connected or broker_position is None:
        issues.append(BROKER_UNAVAILABLE)
    if ledger_position is None:
        issues.append(LEDGER_CORRUPTION)

    if broker_position is not None:
        if broker_position.source != PositionSource.BROKER:
            issues.append(LEDGER_CORRUPTION)
        if broker_position.account_id != account_id:
            issues.append(ACCOUNT_MISMATCH)
        if broker_position.contract_key != contract_key:
            issues.append(CONTRACT_MISMATCH)

    if ledger_position is not None:
        if ledger_position.source != PositionSource.LEDGER:
            issues.append(LEDGER_CORRUPTION)
        if ledger_position.account_id != account_id:
            issues.append(ACCOUNT_MISMATCH)
        if ledger_position.contract_key != contract_key:
            issues.append(CONTRACT_MISMATCH)

    if _duplicate_execution_ids(fill_events):
        issues.append(LEDGER_CORRUPTION)

    matching_fills = [
        fill
        for fill in fill_events
        if fill.account_id == account_id and fill.contract_key == contract_key
    ]
    required_fill_count = _required_fill_count(normalized_stage)
    if len(matching_fills) < required_fill_count:
        issues.append(MISSING_FILL)

    broker_order_ids = _broker_open_order_ids(broker_open_orders)
    ledger_order_ids = tuple(str(item).strip() for item in ledger_open_order_ids if str(item).strip())
    if set(broker_order_ids) != set(ledger_order_ids):
        issues.append(OPEN_ORDER_MISMATCH)
    if broker_order_ids or ledger_order_ids:
        issues.append(OPEN_ORDER_MISMATCH)

    if ambiguous_broker_status or _has_ambiguous_order_status(broker_open_orders):
        issues.append(AMBIGUOUS_BROKER_STATUS)

    if broker_position is not None and ledger_position is not None:
        if broker_position.signed_quantity != ledger_position.signed_quantity:
            issues.append(BROKER_LEDGER_DISAGREEMENT)
        if ledger_position.signed_quantity != expected_quantity:
            issues.append(BROKER_LEDGER_DISAGREEMENT)
        if broker_position.signed_quantity != expected_quantity:
            if ledger_position.signed_quantity == 0 and broker_position.signed_quantity != 0:
                issues.append(UNOWNED_POSITION)
            else:
                issues.append(BROKER_LEDGER_DISAGREEMENT)

    unique_issues = tuple(dict.fromkeys(issues))
    status = _status_for_issues(unique_issues, normalized_stage)
    return ReconciliationResult(
        reconciliation_id=f"recon_{uuid.uuid4().hex}",
        run_id=run_id,
        stage=normalized_stage,
        status=status,
        account_id=account_id,
        contract_key=contract_key,
        expected_signed_quantity=expected_quantity,
        broker_position_state_id=broker_position.position_state_id if broker_position is not None else None,
        ledger_position_state_id=ledger_position.position_state_id if ledger_position is not None else None,
        broker_open_order_ids=broker_order_ids,
        ledger_open_order_ids=ledger_order_ids,
        fill_event_ids=tuple(fill.fill_event_id for fill in matching_fills),
        issues=unique_issues,
        required_action=_required_action(status, unique_issues),
        created_at=created_at or datetime.now(timezone.utc),
    )


def _expected_quantity(
    *,
    stage: ReconciliationStage,
    expected_signed_quantity: int | None,
    ledger_position: PositionState | None,
) -> int:
    if expected_signed_quantity is not None:
        return int(expected_signed_quantity)
    if stage in {ReconciliationStage.PRE_OPEN, ReconciliationStage.POST_CLOSE, ReconciliationStage.FINAL}:
        return 0
    if ledger_position is not None and ledger_position.signed_quantity != 0:
        return int(ledger_position.signed_quantity)
    return 1


def _required_fill_count(stage: ReconciliationStage) -> int:
    if stage == ReconciliationStage.POST_OPEN:
        return 1
    if stage == ReconciliationStage.PRE_CLOSE:
        return 1
    if stage in {ReconciliationStage.POST_CLOSE, ReconciliationStage.FINAL}:
        return 2
    return 0


def _status_for_issues(issues: tuple[str, ...], stage: ReconciliationStage) -> ReconciliationStatus:
    if not issues:
        return ReconciliationStatus.CLEAN
    if stage in _POST_SUBMIT_STAGES:
        return ReconciliationStatus.AMBIGUOUS
    if AMBIGUOUS_BROKER_STATUS in issues:
        return ReconciliationStatus.AMBIGUOUS
    return ReconciliationStatus.BLOCKED


def _required_action(status: ReconciliationStatus, issues: tuple[str, ...]) -> str:
    if status == ReconciliationStatus.CLEAN:
        return "No action required."
    if status == ReconciliationStatus.BLOCKED:
        return "Stop before broker submit, fix the blocking precondition, and start a new run."
    if AMBIGUOUS_BROKER_STATUS in issues:
        return "Manual TWS review required before any further broker action."
    return "Manual broker and ledger review required before any further broker action."


def _broker_open_order_ids(open_orders: Sequence[str | BrokerOrder]) -> tuple[str, ...]:
    ids: list[str] = []
    for item in open_orders:
        if isinstance(item, str):
            value = item
        else:
            value = item.broker_order_id
        normalized = str(value or "").strip()
        if normalized:
            ids.append(normalized)
    return tuple(sorted(ids))


def _has_ambiguous_order_status(open_orders: Sequence[str | BrokerOrder]) -> bool:
    ambiguous_statuses = {"UNKNOWN", "PENDING", "PENDINGSUBMIT", "PRESUBMITTED", "SUBMITTED", "PENDINGCANCEL"}
    for item in open_orders:
        if isinstance(item, BrokerOrder) and str(item.status or "").strip().upper() in ambiguous_statuses:
            return True
    return False


def _duplicate_execution_ids(fill_events: Sequence[FillEvent]) -> bool:
    seen: set[str] = set()
    for fill in fill_events:
        execution_id = fill.execution_id
        if execution_id in seen:
            return True
        seen.add(execution_id)
    return False
