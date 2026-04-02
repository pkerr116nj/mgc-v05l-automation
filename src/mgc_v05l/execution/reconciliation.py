"""Execution-side reconciliation models and comparison logic."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from ..domain.enums import PositionSide

RECONCILIATION_CLASS_CLEAN = "clean"
RECONCILIATION_CLASS_SAFE_REPAIR = "safe_repair"
RECONCILIATION_CLASS_UNSAFE_AMBIGUITY = "unsafe_ambiguity"
RECONCILIATION_CLASS_BROKER_UNAVAILABLE = "broker_unavailable_incomplete_truth"
RECONCILIATION_CLASS_PERSISTENCE_CORRUPTION = "persistence_state_corruption"
RECONCILIATION_CLASS_OPEN_ORDER_UNCERTAINTY = "open_order_uncertainty"
RECONCILIATION_CLASS_FILL_ACK_UNCERTAINTY = "fill_ack_uncertainty"

RECONCILIATION_SAFE_CLASSES = {
    RECONCILIATION_CLASS_CLEAN,
    RECONCILIATION_CLASS_SAFE_REPAIR,
}

RECONCILIATION_REPAIR_CLEAR_STALE_OPEN_ORDER = "clear_stale_open_order_markers"
RECONCILIATION_REPAIR_CONFIRM_FLAT = "confirm_flat_from_broker_fill"
RECONCILIATION_REPAIR_SYNC_BROKER_QTY = "sync_internal_broker_position_qty"


@dataclass(frozen=True)
class InternalReconciliationSnapshot:
    strategy_status: str
    position_side: str
    expected_signed_quantity: int
    internal_position_qty: int
    broker_position_qty: int
    average_price: str | None
    open_broker_order_id: str | None
    persisted_open_order_ids: tuple[str, ...]
    pending_execution_open_order_ids: tuple[str, ...]
    last_fill_timestamp: str | None
    last_order_intent_id: str | None
    entries_enabled: bool
    exits_enabled: bool
    operator_halt: bool
    reconcile_required: bool
    fault_code: str | None
    open_entry_leg_count: int = 0
    open_entry_leg_quantities: tuple[int, ...] = ()

    def to_payload(self) -> dict[str, Any]:
        return {
            "strategy_status": self.strategy_status,
            "position_side": self.position_side,
            "expected_signed_quantity": self.expected_signed_quantity,
            "internal_position_qty": self.internal_position_qty,
            "broker_position_qty": self.broker_position_qty,
            "average_price": self.average_price,
            "open_broker_order_id": self.open_broker_order_id,
            "persisted_open_order_ids": list(self.persisted_open_order_ids),
            "pending_execution_open_order_ids": list(self.pending_execution_open_order_ids),
            "last_fill_timestamp": self.last_fill_timestamp,
            "last_order_intent_id": self.last_order_intent_id,
            "entries_enabled": self.entries_enabled,
            "exits_enabled": self.exits_enabled,
            "operator_halt": self.operator_halt,
            "reconcile_required": self.reconcile_required,
            "fault_code": self.fault_code,
            "open_entry_leg_count": self.open_entry_leg_count,
            "open_entry_leg_quantities": list(self.open_entry_leg_quantities),
        }


@dataclass(frozen=True)
class BrokerReconciliationSnapshot:
    connected: bool
    truth_complete: bool
    position_quantity: int
    side: str
    average_price: str | None
    open_order_ids: tuple[str, ...]
    order_status: dict[str, str]
    last_fill_timestamp: str | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "connected": self.connected,
            "truth_complete": self.truth_complete,
            "position_quantity": self.position_quantity,
            "side": self.side,
            "average_price": self.average_price,
            "open_order_ids": list(self.open_order_ids),
            "order_status": dict(self.order_status),
            "last_fill_timestamp": self.last_fill_timestamp,
        }


@dataclass(frozen=True)
class ReconciliationOutcome:
    trigger: str
    classification: str
    mismatches: tuple[str, ...]
    repair_actions: tuple[str, ...]
    recommended_action: str
    notes: tuple[str, ...]
    freeze_new_entries: bool
    requires_review: bool
    requires_fault: bool
    clean: bool
    internal_snapshot: InternalReconciliationSnapshot
    broker_snapshot: BrokerReconciliationSnapshot
    fault_code: str | None = None
    state_hint: str = "unchanged"

    def to_payload(self, *, occurred_at: str) -> dict[str, Any]:
        return {
            "occurred_at": occurred_at,
            "trigger": self.trigger,
            "classification": self.classification,
            "clean": self.clean,
            "mismatches": list(self.mismatches),
            "issues": list(self.mismatches),
            "repair_actions": list(self.repair_actions),
            "recommended_action": self.recommended_action,
            "notes": list(self.notes),
            "freeze_new_entries": self.freeze_new_entries,
            "requires_review": self.requires_review,
            "requires_fault": self.requires_fault,
            "fault_code": self.fault_code,
            "state_hint": self.state_hint,
            "internal_snapshot": self.internal_snapshot.to_payload(),
            "broker_snapshot": self.broker_snapshot.to_payload(),
            "strategy_position_side": self.internal_snapshot.position_side,
            "strategy_internal_position_qty": self.internal_snapshot.internal_position_qty,
            "strategy_broker_position_qty": self.internal_snapshot.broker_position_qty,
            "strategy_open_broker_order_id": self.internal_snapshot.open_broker_order_id,
            "broker_position_quantity": self.broker_snapshot.position_quantity,
            "broker_average_price": self.broker_snapshot.average_price,
            "broker_open_order_ids": list(self.broker_snapshot.open_order_ids),
            "persisted_open_order_ids": list(self.internal_snapshot.persisted_open_order_ids),
            "pending_execution_open_order_ids": list(self.internal_snapshot.pending_execution_open_order_ids),
        }


class ReconciliationCoordinator:
    """Compares internal state, persisted orders/fills, and broker truth."""

    def evaluate(
        self,
        *,
        trigger: str,
        internal: InternalReconciliationSnapshot,
        broker: BrokerReconciliationSnapshot,
    ) -> ReconciliationOutcome:
        mismatches: list[str] = []
        repair_actions: list[str] = []
        notes: list[str] = []

        if self._internal_state_corrupt(internal):
            mismatches.extend(self._internal_state_corruption_reasons(internal))
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_PERSISTENCE_CORRUPTION,
                mismatches=tuple(mismatches),
                repair_actions=(),
                recommended_action="Inspect persisted state and broker truth before resuming entries.",
                notes=("Internal strategy state violates required invariants.",),
                freeze_new_entries=True,
                requires_review=True,
                requires_fault=True,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                fault_code="reconciliation_persistence_state_corruption",
                state_hint="fault",
            )

        if not broker.connected or not broker.truth_complete:
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_BROKER_UNAVAILABLE,
                mismatches=("broker_truth_unavailable",),
                repair_actions=(),
                recommended_action="Wait for broker truth to recover, then rerun reconciliation.",
                notes=("Broker state is disconnected or incomplete, so new entries must remain frozen.",),
                freeze_new_entries=True,
                requires_review=True,
                requires_fault=False,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                fault_code="reconciliation_broker_unavailable",
                state_hint="reconciling",
            )

        broker_open_order_ids = set(broker.open_order_ids)
        persisted_open_order_ids = set(internal.persisted_open_order_ids)
        pending_execution_open_order_ids = set(internal.pending_execution_open_order_ids)

        qty_mismatch = broker.position_quantity != internal.expected_signed_quantity
        side_mismatch = broker.side != internal.position_side
        open_orders_match_persisted = broker_open_order_ids == persisted_open_order_ids
        open_orders_match_pending = broker_open_order_ids == pending_execution_open_order_ids
        state_open_order_matches = (
            (internal.open_broker_order_id is None and not broker_open_order_ids)
            or (internal.open_broker_order_id is not None and internal.open_broker_order_id in broker_open_order_ids)
        )
        open_order_uncertainty = not (
            open_orders_match_persisted and open_orders_match_pending and state_open_order_matches
        )

        avg_price_mismatch = self._average_price_mismatch(
            internal.average_price,
            broker.average_price,
            internal.expected_signed_quantity,
            broker.position_quantity,
        )
        if qty_mismatch:
            mismatches.append("broker_position_quantity_mismatch")
        if side_mismatch:
            mismatches.append("broker_position_side_mismatch")
        if avg_price_mismatch:
            mismatches.append("broker_average_price_mismatch")
        if not open_orders_match_persisted:
            mismatches.append("persisted_open_orders_mismatch")
        if not open_orders_match_pending:
            mismatches.append("pending_execution_open_orders_mismatch")
        if not state_open_order_matches:
            mismatches.append("strategy_state_open_order_id_mismatch")

        broker_fill_newer = self._is_newer_timestamp(
            broker.last_fill_timestamp,
            internal.last_fill_timestamp,
        )

        if not mismatches:
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_CLEAN,
                mismatches=(),
                repair_actions=(),
                recommended_action="No operator action required.",
                notes=("Internal and broker state are aligned.",),
                freeze_new_entries=False,
                requires_review=False,
                requires_fault=False,
                clean=True,
                internal_snapshot=internal,
                broker_snapshot=broker,
                state_hint="ready",
            )

        if (
            broker.position_quantity == 0
            and not broker_open_order_ids
            and internal.expected_signed_quantity == 0
            and (
                internal.open_broker_order_id is not None
                or persisted_open_order_ids
                or pending_execution_open_order_ids
            )
        ):
            repair_actions.append(RECONCILIATION_REPAIR_CLEAR_STALE_OPEN_ORDER)
            notes.append("Broker is flat with no live open orders, so stale internal order markers can be cleared safely.")
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_SAFE_REPAIR,
                mismatches=tuple(mismatches),
                repair_actions=tuple(repair_actions),
                recommended_action="Safe cleanup will clear stale internal open-order markers.",
                notes=tuple(notes),
                freeze_new_entries=False,
                requires_review=False,
                requires_fault=False,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                state_hint="ready",
            )

        if (
            broker.position_quantity == 0
            and not broker_open_order_ids
            and internal.expected_signed_quantity != 0
            and broker_fill_newer
            and not open_order_uncertainty
        ):
            repair_actions.append(RECONCILIATION_REPAIR_CONFIRM_FLAT)
            notes.append("Broker confirms the position is already closed and no open orders remain.")
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_SAFE_REPAIR,
                mismatches=tuple(mismatches),
                repair_actions=tuple(repair_actions),
                recommended_action="Safe flat repair will clear the internal position and return to READY if nothing else is blocking.",
                notes=tuple(notes),
                freeze_new_entries=False,
                requires_review=False,
                requires_fault=False,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                state_hint="ready",
            )

        if (
            not qty_mismatch
            and not side_mismatch
            and not open_order_uncertainty
            and internal.broker_position_qty != abs(broker.position_quantity)
        ):
            repair_actions.append(RECONCILIATION_REPAIR_SYNC_BROKER_QTY)
            notes.append("Only the mirrored broker_position_qty field is stale; position exposure itself is aligned.")
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_SAFE_REPAIR,
                mismatches=tuple(mismatches),
                repair_actions=tuple(repair_actions),
                recommended_action="Safe cleanup will sync the mirrored broker position quantity field.",
                notes=tuple(notes),
                freeze_new_entries=False,
                requires_review=False,
                requires_fault=False,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                state_hint="ready",
            )

        if open_order_uncertainty:
            notes.append("Open-order truth differs across internal state, persisted intents, or broker state.")
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_OPEN_ORDER_UNCERTAINTY,
                mismatches=tuple(mismatches),
                repair_actions=(),
                recommended_action="Freeze new entries, inspect open orders, and rerun reconciliation after broker truth is confirmed.",
                notes=tuple(notes),
                freeze_new_entries=True,
                requires_review=True,
                requires_fault=False,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                fault_code="reconciliation_open_order_uncertainty",
                state_hint="reconciling",
            )

        if broker_fill_newer:
            notes.append("Broker shows a newer fill timestamp than the internal ledger.")
            return ReconciliationOutcome(
                trigger=trigger,
                classification=RECONCILIATION_CLASS_FILL_ACK_UNCERTAINTY,
                mismatches=tuple(mismatches),
                repair_actions=(),
                recommended_action="Freeze new entries and inspect missing fill acknowledgement before resuming.",
                notes=tuple(notes),
                freeze_new_entries=True,
                requires_review=True,
                requires_fault=False,
                clean=False,
                internal_snapshot=internal,
                broker_snapshot=broker,
                fault_code="reconciliation_fill_ack_uncertainty",
                state_hint="reconciling",
            )

        fault_required = side_mismatch and broker.side != PositionSide.FLAT.value and internal.position_side != PositionSide.FLAT.value
        notes.append("Broker exposure differs from internal strategy exposure in a way that cannot be repaired automatically.")
        return ReconciliationOutcome(
            trigger=trigger,
            classification=RECONCILIATION_CLASS_UNSAFE_AMBIGUITY,
            mismatches=tuple(mismatches),
            repair_actions=(),
            recommended_action=(
                "Freeze new entries and inspect broker/internal exposure before clearing the fault."
                if fault_required
                else "Freeze new entries, investigate the mismatch, and rerun reconciliation after review."
            ),
            notes=tuple(notes),
            freeze_new_entries=True,
            requires_review=True,
            requires_fault=fault_required or avg_price_mismatch,
            clean=False,
            internal_snapshot=internal,
            broker_snapshot=broker,
            fault_code=(
                "reconciliation_unsafe_opposite_side_exposure"
                if fault_required
                else "reconciliation_unsafe_ambiguity"
            ),
            state_hint="fault" if (fault_required or avg_price_mismatch) else "reconciling",
        )

    def _internal_state_corrupt(self, internal: InternalReconciliationSnapshot) -> bool:
        return bool(self._internal_state_corruption_reasons(internal))

    def _internal_state_corruption_reasons(self, internal: InternalReconciliationSnapshot) -> list[str]:
        reasons: list[str] = []
        if internal.position_side == PositionSide.FLAT.value and internal.internal_position_qty != 0:
            reasons.append("flat_state_nonzero_internal_position")
        if internal.position_side == PositionSide.LONG.value and internal.internal_position_qty <= 0:
            reasons.append("long_state_nonpositive_internal_position")
        if internal.position_side == PositionSide.SHORT.value and internal.internal_position_qty <= 0:
            reasons.append("short_state_nonpositive_internal_position")
        if internal.position_side != PositionSide.FLAT.value and internal.average_price is None:
            reasons.append("open_position_missing_entry_price")
        return reasons

    def _average_price_mismatch(
        self,
        internal_average_price: str | None,
        broker_average_price: str | None,
        internal_signed_quantity: int,
        broker_quantity: int,
    ) -> bool:
        if internal_signed_quantity == 0 or broker_quantity == 0:
            return False
        if internal_average_price is None or broker_average_price is None:
            return False
        return Decimal(internal_average_price) != Decimal(broker_average_price)

    def _is_newer_timestamp(self, broker_timestamp: str | None, internal_timestamp: str | None) -> bool:
        if broker_timestamp is None:
            return False
        if internal_timestamp is None:
            return True
        return broker_timestamp > internal_timestamp
