"""Strategy-side reconciliation workflow."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from typing import Any

from sqlalchemy.exc import OperationalError

from ..domain.enums import PositionSide, StrategyStatus
from ..domain.models import StrategyState
from ..execution.execution_engine import ExecutionEngine
from ..execution.reconciliation import (
    BrokerReconciliationSnapshot,
    InternalReconciliationSnapshot,
    RECONCILIATION_CLASS_CLEAN,
    RECONCILIATION_CLASS_FILL_ACK_UNCERTAINTY,
    RECONCILIATION_CLASS_OPEN_ORDER_UNCERTAINTY,
    RECONCILIATION_CLASS_SAFE_REPAIR,
    RECONCILIATION_REPAIR_CLEAR_STALE_OPEN_ORDER,
    RECONCILIATION_REPAIR_CONFIRM_FLAT,
    RECONCILIATION_REPAIR_SYNC_BROKER_QTY,
    ReconciliationCoordinator,
    ReconciliationOutcome,
)
from ..monitoring.alerts import AlertDispatcher
from ..monitoring.logger import StructuredLogger
from ..persistence.repositories import RepositorySet, decode_fill
from .state_machine import transition_to_fault, transition_to_ready, transition_to_reconciling


class StrategyReconciler:
    """Coordinates startup and periodic reconciliation from the strategy layer."""

    def __init__(
        self,
        *,
        repositories: RepositorySet | None = None,
        structured_logger: StructuredLogger | None = None,
        alert_dispatcher: AlertDispatcher | None = None,
        runtime_identity: dict[str, object] | None = None,
    ) -> None:
        self._repositories = repositories
        self._structured_logger = structured_logger
        self._alert_dispatcher = alert_dispatcher
        self._runtime_identity = dict(runtime_identity or {})
        self._coordinator = ReconciliationCoordinator()

    def inspect(
        self,
        *,
        state: StrategyState,
        occurred_at: datetime,
        trigger: str,
        execution_engine: ExecutionEngine,
    ) -> dict[str, Any]:
        outcome = self._evaluate(
            state=state,
            occurred_at=occurred_at,
            trigger=trigger,
            execution_engine=execution_engine,
        )
        return outcome.to_payload(occurred_at=occurred_at.isoformat())

    def reconcile(
        self,
        *,
        state: StrategyState,
        occurred_at: datetime,
        trigger: str,
        execution_engine: ExecutionEngine,
    ) -> tuple[StrategyState, dict[str, Any]]:
        outcome = self._evaluate(
            state=state,
            occurred_at=occurred_at,
            trigger=trigger,
            execution_engine=execution_engine,
        )
        next_state = self._apply_outcome(state=state, outcome=outcome, occurred_at=occurred_at)
        payload = outcome.to_payload(occurred_at=occurred_at.isoformat())
        payload.update(
            {
                "event_type": "strategy_reconciliation",
                "repair_applied": outcome.classification == RECONCILIATION_CLASS_SAFE_REPAIR,
                "state_transition": next_state.strategy_status.value if next_state != state else "unchanged",
                "resulting_strategy_status": next_state.strategy_status.value,
                "resulting_fault_code": next_state.fault_code,
                "resulting_entries_enabled": next_state.entries_enabled,
                "resulting_operator_halt": next_state.operator_halt,
                "reconcile_required": next_state.reconcile_required,
                "runtime_identity": dict(self._runtime_identity),
            }
        )
        self._persist_reconciliation_event(payload, occurred_at=occurred_at)
        if next_state.fault_code is not None and next_state.strategy_status is StrategyStatus.FAULT:
            self._persist_fault_event(payload, fault_code=next_state.fault_code, occurred_at=occurred_at)
        return next_state, payload

    def force_reconcile(
        self,
        state: StrategyState,
        occurred_at: datetime,
        execution_engine: ExecutionEngine,
    ) -> tuple[StrategyState, dict[str, Any]]:
        return self.reconcile(
            state=state,
            occurred_at=occurred_at,
            trigger="manual_force_reconcile",
            execution_engine=execution_engine,
        )

    def _evaluate(
        self,
        *,
        state: StrategyState,
        occurred_at: datetime,
        trigger: str,
        execution_engine: ExecutionEngine,
    ) -> ReconciliationOutcome:
        persisted_open_order_ids = self._load_persisted_open_order_ids()
        pending_execution_open_order_ids = tuple(
            sorted(pending.broker_order_id for pending in execution_engine.pending_executions())
        )
        broker_snapshot = execution_engine.broker.snapshot_state()
        internal_snapshot = InternalReconciliationSnapshot(
            strategy_status=state.strategy_status.value,
            position_side=state.position_side.value,
            expected_signed_quantity=self._signed_internal_quantity(state),
            internal_position_qty=int(state.internal_position_qty),
            broker_position_qty=int(state.broker_position_qty),
            average_price=str(state.entry_price) if state.entry_price is not None else None,
            open_broker_order_id=state.open_broker_order_id,
            persisted_open_order_ids=persisted_open_order_ids,
            pending_execution_open_order_ids=pending_execution_open_order_ids,
            last_fill_timestamp=self._load_latest_fill_timestamp(),
            last_order_intent_id=state.last_order_intent_id,
            entries_enabled=state.entries_enabled,
            exits_enabled=state.exits_enabled,
            operator_halt=state.operator_halt,
            reconcile_required=state.reconcile_required,
            fault_code=state.fault_code,
            open_entry_leg_count=len(state.open_entry_legs),
            open_entry_leg_quantities=tuple(int(leg.quantity) for leg in state.open_entry_legs),
        )
        broker = BrokerReconciliationSnapshot(
            connected=bool(broker_snapshot.get("connected", False)),
            truth_complete="position_quantity" in broker_snapshot and "open_order_ids" in broker_snapshot,
            position_quantity=int(broker_snapshot.get("position_quantity", 0)),
            side=self._signed_quantity_to_side(int(broker_snapshot.get("position_quantity", 0))),
            average_price=self._normalize_optional_text(broker_snapshot.get("average_price")),
            open_order_ids=tuple(sorted(str(order_id) for order_id in broker_snapshot.get("open_order_ids", []))),
            order_status={
                str(key): str(value)
                for key, value in dict(broker_snapshot.get("order_status") or {}).items()
            },
            last_fill_timestamp=self._normalize_optional_text(broker_snapshot.get("last_fill_timestamp")),
        )
        del occurred_at
        return self._coordinator.evaluate(trigger=trigger, internal=internal_snapshot, broker=broker)

    def _apply_outcome(
        self,
        *,
        state: StrategyState,
        outcome: ReconciliationOutcome,
        occurred_at: datetime,
    ) -> StrategyState:
        if outcome.classification == RECONCILIATION_CLASS_CLEAN:
            if state.position_side == PositionSide.FLAT and state.strategy_status in {
                StrategyStatus.RECONCILING,
                StrategyStatus.FAULT,
                StrategyStatus.DISABLED,
                StrategyStatus.READY,
            }:
                base = replace(
                    state,
                    broker_position_qty=abs(outcome.broker_snapshot.position_quantity),
                    entries_enabled=not state.operator_halt,
                    reconcile_required=False,
                    fault_code=None,
                )
                return transition_to_ready(base, occurred_at)
            return replace(
                state,
                broker_position_qty=abs(outcome.broker_snapshot.position_quantity),
                reconcile_required=False,
                fault_code=None if state.strategy_status is not StrategyStatus.FAULT else state.fault_code,
                updated_at=occurred_at,
            )

        if outcome.classification == RECONCILIATION_CLASS_SAFE_REPAIR:
            repaired = state
            for repair in outcome.repair_actions:
                if repair == RECONCILIATION_REPAIR_CLEAR_STALE_OPEN_ORDER:
                    repaired = replace(repaired, open_broker_order_id=None)
                elif repair == RECONCILIATION_REPAIR_CONFIRM_FLAT:
                    repaired = replace(
                        repaired,
                        strategy_status=StrategyStatus.READY,
                        position_side=PositionSide.FLAT,
                        internal_position_qty=0,
                        broker_position_qty=0,
                        entry_price=None,
                        entry_timestamp=None,
                        entry_bar_id=None,
                        open_broker_order_id=None,
                        open_entry_legs=(),
                    )
                elif repair == RECONCILIATION_REPAIR_SYNC_BROKER_QTY:
                    repaired = replace(repaired, broker_position_qty=abs(outcome.broker_snapshot.position_quantity))
            repaired = replace(
                repaired,
                entries_enabled=not repaired.operator_halt,
                reconcile_required=False,
                fault_code=None,
            )
            if repaired.position_side == PositionSide.FLAT or repaired.strategy_status in {
                StrategyStatus.RECONCILING,
                StrategyStatus.FAULT,
                StrategyStatus.DISABLED,
            }:
                return transition_to_ready(repaired, occurred_at)
            return replace(repaired, updated_at=occurred_at)

        degraded = replace(
            state,
            entries_enabled=False,
            reconcile_required=True,
            updated_at=occurred_at,
        )
        if outcome.requires_fault:
            return transition_to_fault(degraded, occurred_at, outcome.fault_code or "reconciliation_fault")
        reconciling = transition_to_reconciling(degraded, occurred_at)
        return replace(
            reconciling,
            entries_enabled=False,
            fault_code=outcome.fault_code,
            updated_at=occurred_at,
        )

    def _persist_reconciliation_event(self, payload: dict[str, Any], *, occurred_at: datetime) -> None:
        enriched_payload = dict(payload)
        if self._repositories is not None:
            try:
                self._repositories.reconciliation_events.save(enriched_payload, created_at=occurred_at)
            except OperationalError as exc:
                if not _is_transient_sqlite_lock_error(exc):
                    raise
                enriched_payload["repository_persistence_degraded"] = True
                enriched_payload["repository_persistence_error"] = str(exc)
        if self._structured_logger is not None:
            self._structured_logger.log_reconciliation_event(enriched_payload)
        if self._alert_dispatcher is None:
            return
        mismatch_codes = [str(item) for item in enriched_payload.get("mismatches") or []]
        classification = str(enriched_payload.get("classification") or "")
        category = (
            "missing_fill_ack"
            if classification == RECONCILIATION_CLASS_FILL_ACK_UNCERTAINTY
            else "open_order_uncertainty"
            if classification == RECONCILIATION_CLASS_OPEN_ORDER_UNCERTAINTY
            else "reconciliation_mismatch"
        )
        title = (
            "Missing Fill Acknowledgement"
            if category == "missing_fill_ack"
            else "Open Order Uncertainty"
            if category == "open_order_uncertainty"
            else "Reconciliation Mismatch"
        )
        dedup_key = "|".join(
            [
                str(self._runtime_identity.get("standalone_strategy_id") or ""),
                str(self._runtime_identity.get("lane_id") or ""),
                str(self._runtime_identity.get("instrument") or payload.get("instrument") or ""),
                "reconciliation",
            ]
        )
        if payload.get("classification") == RECONCILIATION_CLASS_SAFE_REPAIR:
            self._alert_dispatcher.sync_condition(
                code="strategy_reconciliation_mismatch",
                active=False,
                severity="RECOVERY",
                category=category,
                title="Reconciliation Recovered",
                message=enriched_payload.get("recommended_action") or "Reconciliation mismatch resolved with a safe repair.",
                payload={**enriched_payload, **self._runtime_identity},
                dedup_key=dedup_key,
                recommended_action="No manual action required unless the mismatch reappears.",
                occurred_at=occurred_at,
            )
            self._alert_dispatcher.emit(
                severity="RECOVERY",
                code="safe_repair_performed",
                message=enriched_payload.get("recommended_action") or "Safe reconciliation repair performed.",
                payload={**enriched_payload, **self._runtime_identity},
                category="safe_repair_performed",
                title="Safe Repair Performed",
                dedup_key=f"{dedup_key}|safe_repair|{'-'.join(str(item) for item in enriched_payload.get('repair_actions') or [])}",
                active=False,
                coalesce=False,
                occurred_at=occurred_at,
            )
            return
        if enriched_payload.get("clean", False):
            self._alert_dispatcher.sync_condition(
                code="strategy_reconciliation_mismatch",
                active=False,
                severity="RECOVERY",
                category=category,
                title="Reconciliation Recovered",
                message="Reconciliation is clean again.",
                payload={**enriched_payload, **self._runtime_identity},
                dedup_key=dedup_key,
                occurred_at=occurred_at,
            )
            return
        self._alert_dispatcher.sync_condition(
            code="strategy_reconciliation_mismatch",
            active=True,
            severity="BLOCKING" if enriched_payload.get("requires_fault") is True else "ACTION",
            category=category,
            title=title,
            message=enriched_payload.get("recommended_action") or "Reconciliation mismatch detected.",
            payload={**enriched_payload, **self._runtime_identity, "mismatch_codes": mismatch_codes},
            dedup_key=dedup_key,
            recommended_action=enriched_payload.get("recommended_action") or "Inspect broker/internal mismatch details before resuming entries.",
            occurred_at=occurred_at,
        )

    def _persist_fault_event(self, payload: dict[str, Any], *, fault_code: str, occurred_at: datetime) -> None:
        fault_payload = dict(payload)
        fault_payload["fault_code"] = fault_code
        if self._repositories is not None:
            try:
                self._repositories.fault_events.save(
                    fault_code=fault_code,
                    payload=fault_payload,
                    created_at=occurred_at,
                    bar_id=None,
                )
            except OperationalError as exc:
                if not _is_transient_sqlite_lock_error(exc):
                    raise

    def _load_persisted_open_order_ids(self) -> tuple[str, ...]:
        if self._repositories is None:
            return ()
        intent_rows = self._repositories.order_intents.list_all()
        fill_intent_ids = {row["order_intent_id"] for row in self._repositories.fills.list_all()}
        open_ids = [
            row.get("broker_order_id") or f"paper-{row['order_intent_id']}"
            for row in intent_rows
            if row["order_intent_id"] not in fill_intent_ids
            and row.get("order_status") not in {"CANCELLED", "REJECTED", "FILLED"}
        ]
        return tuple(sorted(str(order_id) for order_id in open_ids))

    def _load_latest_fill_timestamp(self) -> str | None:
        if self._repositories is None:
            return None
        latest_timestamp: str | None = None
        for row in self._repositories.fills.list_all():
            fill = decode_fill(dict(row))
            timestamp = fill.fill_timestamp.isoformat()
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
        return latest_timestamp

    def _signed_internal_quantity(self, state: StrategyState) -> int:
        if state.position_side == PositionSide.LONG:
            return int(state.internal_position_qty)
        if state.position_side == PositionSide.SHORT:
            return -int(state.internal_position_qty)
        return 0

    def _signed_quantity_to_side(self, quantity: int) -> str:
        if quantity > 0:
            return PositionSide.LONG.value
        if quantity < 0:
            return PositionSide.SHORT.value
        return PositionSide.FLAT.value

    def _normalize_optional_text(self, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None


def _is_transient_sqlite_lock_error(error: OperationalError) -> bool:
    return "database is locked" in str(error).lower()
