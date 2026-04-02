"""Strategy engine orchestration for deterministic bar-close execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, time, timezone
from typing import Callable, Optional

from ..config_models import StrategySettings
from ..domain.enums import (
    AddDirectionPolicy,
    LongEntryFamily,
    OrderIntentType,
    OrderStatus,
    ParticipationPolicy,
    PositionSide,
    ShortEntryFamily,
    StrategyStatus,
)
from ..domain.events import (
    BarClosedEvent,
    DomainEvent,
    ExitEvaluatedEvent,
    FaultRaisedEvent,
    FillReceivedEvent,
    OrderIntentCreatedEvent,
)
from ..domain.models import Bar, FeaturePacket, SignalPacket, StrategyState
from ..execution.execution_engine import ExecutionEngine
from ..execution.order_models import FillEvent, OrderIntent
from ..execution.paper_broker import PaperBroker
from ..indicators.feature_engine import IncrementalFeatureComputer, compute_features
from ..market_data.bar_store import BarStore
from ..market_data.session_clock import classify_sessions
from ..monitoring.alerts import AlertDispatcher
from ..monitoring.logger import StructuredLogger
from ..persistence.repositories import RepositorySet
from ..persistence.state_repository import StateRepository
from ..signals.asia_vwap_reclaim import evaluate_asia_vwap_reclaim
from ..signals.bear_snap import evaluate_bear_snap
from ..signals.bull_snap import evaluate_bull_snap
from ..signals.entry_resolver import resolve_entries
from .exit_engine import ExitDecision, evaluate_exits
from .invariants import validate_state
from .risk_engine import compute_risk_context
from .state_machine import (
    increment_bars_in_trade,
    transition_on_entry_fill,
    transition_on_exit_fill,
    transition_to_fault,
    transition_to_ready,
    update_additive_short_peak_state,
)
from .reconcile import StrategyReconciler
from .trade_state import build_initial_state, normalize_legacy_single_position_state


class StrategyEngine:
    """Orchestrates deterministic bar-close processing."""

    def __init__(
        self,
        settings: StrategySettings,
        initial_state: Optional[StrategyState] = None,
        repositories: Optional[RepositorySet] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        use_incremental_features: bool = True,
        structured_logger: Optional[StructuredLogger] = None,
        alert_dispatcher: Optional[AlertDispatcher] = None,
        runtime_identity: Optional[dict[str, object]] = None,
        shadow_mode_no_submit: bool = False,
        submit_gate_evaluator: Optional[Callable[[Bar, StrategyState, OrderIntent], str | None]] = None,
    ) -> None:
        self._settings = settings
        self._repositories = repositories
        self._runtime_identity = dict(runtime_identity or getattr(repositories, "runtime_identity", {}) or {})
        if not self._runtime_identity:
            self._runtime_identity = {
                "standalone_strategy_id": f"legacy_runtime__{settings.symbol}",
                "strategy_family": "LEGACY_RUNTIME",
                "instrument": settings.symbol,
                "lane_id": settings.probationary_paper_lane_id or "",
            }
        self._state_repository = (
            StateRepository(repositories.engine, runtime_identity=self._runtime_identity)
            if repositories is not None
            else None
        )
        self._bar_store = BarStore(repositories.processed_bars if repositories is not None else None)
        self._execution_engine = execution_engine or ExecutionEngine()
        self._structured_logger = structured_logger
        self._alert_dispatcher = alert_dispatcher
        self._shadow_mode_no_submit = shadow_mode_no_submit
        self._submit_gate_evaluator = submit_gate_evaluator
        self._use_incremental_features = use_incremental_features
        self._incremental_feature_computer = (
            IncrementalFeatureComputer(settings) if use_incremental_features else None
        )
        self._state = self._load_initial_state(initial_state)
        self._bar_history: list[Bar] = []
        self._feature_history: list[FeaturePacket] = []
        self._last_signal_packet: Optional[SignalPacket] = None
        self._last_feature_packet: Optional[FeaturePacket] = None
        self._last_exit_decision_summary: dict[str, object] = {}
        self._latest_shadow_intent_summary: dict[str, object] = {}
        self._latest_live_intent_summary: dict[str, object] = {}
        self._restore_processing_context()

    def process_bar(self, bar: Bar) -> list[DomainEvent]:
        """Process a single completed bar and return emitted domain events."""
        if not bar.is_final:
            return []

        events: list[DomainEvent] = []
        events.extend(self._apply_due_replay_fills(bar))

        session_bar = classify_sessions(bar, self._settings)
        if not self._bar_store.validate_next_bar(session_bar):
            return events

        events.append(BarClosedEvent(bar_id=session_bar.bar_id, occurred_at=session_bar.end_ts))
        self._bar_history.append(session_bar)

        feature_packet = self._compute_feature_packet(session_bar)
        self._feature_history.append(feature_packet)
        signal_packet = self._evaluate_signals(feature_packet, self._feature_history)
        signal_packet = self._apply_runtime_entry_controls(session_bar, signal_packet)

        working_state = self._advance_state_for_bar(feature_packet, signal_packet, session_bar.end_ts)
        if working_state.position_side != PositionSide.FLAT:
            working_state = increment_bars_in_trade(working_state, session_bar.end_ts)

        risk_context = compute_risk_context(self._bar_history, feature_packet, working_state, self._settings)
        working_state = replace(
            working_state,
            long_be_armed=risk_context.long_break_even_armed,
            short_be_armed=risk_context.short_break_even_armed,
            updated_at=session_bar.end_ts,
        )
        working_state = update_additive_short_peak_state(
            working_state,
            session_bar,
            risk_context,
            self._settings,
            session_bar.end_ts,
        )

        exit_decision = evaluate_exits(self._bar_history, feature_packet, working_state, risk_context, self._settings)
        if working_state.position_side != PositionSide.FLAT:
            self._last_exit_decision_summary = self._build_exit_decision_summary(
                bar=session_bar,
                state=working_state,
                exit_decision=exit_decision,
                risk_context=risk_context,
                exit_fill_pending=False,
                exit_fill_confirmed=False,
            )
            events.append(
                ExitEvaluatedEvent(
                    bar_id=session_bar.bar_id,
                    primary_reason=exit_decision.primary_reason,
                    occurred_at=session_bar.end_ts,
                    all_true_reasons=exit_decision.all_true_reasons,
                    long_entry_family=working_state.long_entry_family,
                    short_entry_family=exit_decision.short_entry_family,
                    short_entry_source=exit_decision.short_entry_source,
                    long_break_even_armed=risk_context.long_break_even_armed,
                    short_break_even_armed=risk_context.short_break_even_armed,
                    active_long_stop_ref=risk_context.active_long_stop_ref,
                    active_short_stop_ref=risk_context.active_short_stop_ref,
                    additive_short_max_favorable_excursion=exit_decision.additive_short_max_favorable_excursion,
                    additive_short_peak_threshold_reached=exit_decision.additive_short_peak_threshold_reached,
                    additive_short_giveback_from_peak=exit_decision.additive_short_giveback_from_peak,
                )
            )

        violations = validate_state(working_state)
        if violations:
            fault_code = "; ".join(violations)
            working_state = transition_to_fault(working_state, session_bar.end_ts, fault_code)
            events.append(FaultRaisedEvent(fault_code=fault_code, occurred_at=session_bar.end_ts))
            if self._alert_dispatcher is not None:
                self._alert_dispatcher.emit(
                    "error",
                    "strategy_invariant_fault",
                    fault_code,
                    {"bar_id": session_bar.bar_id},
                )
            self._persist_state(working_state, transition_label="fault")
        else:
            maybe_intent = self._maybe_create_order_intent(session_bar, signal_packet, working_state, exit_decision)
            if maybe_intent is not None:
                long_entry_family = self._resolve_long_entry_family(signal_packet)
                short_entry_family = (
                    self._resolve_short_entry_family(signal_packet)
                    if maybe_intent.intent_type == OrderIntentType.SELL_TO_OPEN
                    else ShortEntryFamily.NONE
                )
                short_entry_source = (
                    signal_packet.short_entry_source
                    if maybe_intent.intent_type == OrderIntentType.SELL_TO_OPEN
                    else None
                )
                if self._shadow_mode_no_submit:
                    self._latest_shadow_intent_summary = self._build_shadow_intent_summary(
                        bar=session_bar,
                        state=working_state,
                        signal_packet=signal_packet,
                        exit_decision=exit_decision,
                        risk_context=risk_context,
                        intent=maybe_intent,
                        long_entry_family=long_entry_family,
                        short_entry_family=short_entry_family,
                        short_entry_source=short_entry_source,
                    )
                    events.append(
                        OrderIntentCreatedEvent(
                            order_intent_id=maybe_intent.order_intent_id,
                            bar_id=session_bar.bar_id,
                            intent_type=maybe_intent.intent_type,
                            occurred_at=session_bar.end_ts,
                        )
                    )
                    self._emit_shadow_submit_suppressed_alert(maybe_intent, session_bar.end_ts)
                else:
                    live_intent_summary = self._build_live_intent_summary(
                        bar=session_bar,
                        state=working_state,
                        signal_packet=signal_packet,
                        exit_decision=exit_decision,
                        risk_context=risk_context,
                        intent=maybe_intent,
                        long_entry_family=long_entry_family,
                        short_entry_family=short_entry_family,
                        short_entry_source=short_entry_source,
                    )
                    submit_blocker = (
                        self._submit_gate_evaluator(session_bar, working_state, maybe_intent)
                        if self._submit_gate_evaluator is not None
                        else None
                    )
                    if submit_blocker is not None:
                        self._latest_live_intent_summary = {
                            **live_intent_summary,
                            "submit_gate_blocker": submit_blocker,
                            "submit_attempted": False,
                            "submit_suppressed": True,
                        }
                    else:
                        self._latest_live_intent_summary = {
                            **live_intent_summary,
                            "submit_gate_blocker": None,
                            "submit_attempted": True,
                            "submit_suppressed": False,
                        }
                    if submit_blocker is not None:
                        self._emit_order_rejection_alert(maybe_intent, session_bar.end_ts, reason=submit_blocker)
                    else:
                        pending = self._execution_engine.submit_intent(
                            maybe_intent,
                            signal_bar_id=session_bar.bar_id if maybe_intent.is_entry else None,
                            long_entry_family=long_entry_family,
                            short_entry_family=short_entry_family,
                            short_entry_source=short_entry_source,
                        )
                        if pending is not None:
                            working_state = replace(
                                working_state,
                                last_order_intent_id=maybe_intent.order_intent_id,
                                open_broker_order_id=pending.broker_order_id,
                                updated_at=session_bar.end_ts,
                            )
                            self._latest_live_intent_summary = {
                                **self._latest_live_intent_summary,
                                "submit_attempted_at": pending.submitted_at.isoformat(),
                                "broker_order_id": pending.broker_order_id,
                                "broker_ack_at": pending.acknowledged_at.isoformat() if pending.acknowledged_at is not None else None,
                                "broker_order_status": pending.broker_order_status,
                            }
                            events.append(
                                OrderIntentCreatedEvent(
                                    order_intent_id=maybe_intent.order_intent_id,
                                    bar_id=session_bar.bar_id,
                                    intent_type=maybe_intent.intent_type,
                                    occurred_at=session_bar.end_ts,
                                )
                            )
                            self._persist_order_intent(
                                maybe_intent,
                                pending.broker_order_id,
                                submitted_at=pending.submitted_at,
                                acknowledged_at=pending.acknowledged_at,
                                broker_order_status=pending.broker_order_status,
                                last_status_checked_at=pending.last_status_checked_at,
                                retry_count=pending.retry_count,
                            )
                            self._persist_state(working_state, transition_label="intent_created")
                            if maybe_intent.intent_type in (OrderIntentType.SELL_TO_CLOSE, OrderIntentType.BUY_TO_CLOSE):
                                self._last_exit_decision_summary = {
                                    **self._last_exit_decision_summary,
                                    "exit_order_intent_id": maybe_intent.order_intent_id,
                                    "exit_intent_type": maybe_intent.intent_type.value,
                                    "exit_fill_pending": True,
                                    "exit_fill_confirmed": False,
                                    "pending_broker_order_id": pending.broker_order_id,
                                    "intent_created_at": maybe_intent.created_at.isoformat(),
                                    "latest_order_status": pending.broker_order_status,
                                }
                            self._emit_order_lifecycle_alert("created", maybe_intent, session_bar.end_ts, pending_broker_order_id=pending.broker_order_id)
                            self._emit_order_lifecycle_alert("submitted", maybe_intent, session_bar.end_ts, pending_broker_order_id=pending.broker_order_id)
                        else:
                            failure = self._execution_engine.last_submit_failure()
                            self._latest_live_intent_summary = {
                                **self._latest_live_intent_summary,
                                "submit_failure": {
                                    "failure_stage": failure.failure_stage,
                                    "error": failure.error,
                                    "submit_attempted_at": failure.submit_attempted_at.isoformat(),
                                }
                                if failure is not None and failure.order_intent_id == maybe_intent.order_intent_id
                                else None,
                            }
                            working_state = self._handle_submit_failure_or_rejection(
                                state=working_state,
                                intent=maybe_intent,
                                occurred_at=session_bar.end_ts,
                                default_reason="Execution engine rejected the intent due to an existing pending or opposite-side conflict.",
                            )

        self._bar_store.mark_processed(session_bar)
        self._persist_bar_artifacts(session_bar, feature_packet, signal_packet)
        self._last_feature_packet = feature_packet
        self._last_signal_packet = signal_packet
        self._state = working_state
        self._persist_state(self._state, transition_label="bar_close")
        return events

    def apply_fill(
        self,
        fill_event: FillEvent,
        signal_bar_id: Optional[str] = None,
        long_entry_family: LongEntryFamily = LongEntryFamily.NONE,
        short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE,
        short_entry_source: Optional[str] = None,
    ) -> StrategyState:
        """Apply a confirmed fill to strategy state."""
        if fill_event.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            if signal_bar_id is None:
                raise ValueError("signal_bar_id is required for entry fills.")
            self._state = transition_on_entry_fill(
                state=self._state,
                fill_event=fill_event,
                signal_bar_id=signal_bar_id,
                long_entry_family=long_entry_family,
                short_entry_family=short_entry_family,
                short_entry_source=short_entry_source,
            )
        else:
            self._state = transition_on_exit_fill(self._state, fill_event)
            self._last_exit_decision_summary = {
                **self._last_exit_decision_summary,
                "exit_fill_pending": False,
                "exit_fill_confirmed": True,
                "fill_confirmed_at": fill_event.fill_timestamp.isoformat(),
                "fill_price": str(fill_event.fill_price) if fill_event.fill_price is not None else None,
                "fill_broker_order_id": fill_event.broker_order_id,
                "resulting_strategy_status": self._state.strategy_status.value,
                "resulting_position_side": self._state.position_side.value,
                "resulting_internal_qty": self._state.internal_position_qty,
                "resulting_broker_qty": self._state.broker_position_qty,
            }
        if self._latest_live_intent_summary.get("order_intent_id") == fill_event.order_intent_id:
            self._latest_live_intent_summary = {
                **self._latest_live_intent_summary,
                "broker_fill_at": fill_event.fill_timestamp.isoformat(),
                "fill_price": str(fill_event.fill_price) if fill_event.fill_price is not None else None,
                "fill_broker_order_id": fill_event.broker_order_id,
                "resulting_strategy_status": self._state.strategy_status.value,
                "resulting_position_side": self._state.position_side.value,
                "resulting_internal_qty": self._state.internal_position_qty,
                "resulting_broker_qty": self._state.broker_position_qty,
                "fill_confirmed": True,
            }
        self._persist_fill(fill_event)
        self._persist_state(self._state, transition_label="fill")
        self._emit_fill_alert(fill_event)
        return self._state

    @property
    def state(self) -> StrategyState:
        return self._state

    def latest_exit_decision_summary(self) -> dict[str, object]:
        return dict(self._last_exit_decision_summary)

    def latest_shadow_intent_summary(self) -> dict[str, object]:
        return dict(self._latest_shadow_intent_summary)

    def latest_live_intent_summary(self) -> dict[str, object]:
        return dict(self._latest_live_intent_summary)

    def force_fault(self, occurred_at: datetime, fault_code: str) -> StrategyState:
        """Fail closed and persist an explicit runtime fault."""
        self._state = transition_to_fault(self._state, occurred_at, fault_code)
        if self._alert_dispatcher is not None:
            self._alert_dispatcher.emit(
                severity="BLOCKING",
                code="strategy_forced_fault",
                message=fault_code,
                payload={"occurred_at": occurred_at.isoformat(), **self._runtime_identity, "fault_code": fault_code},
                category="persistent_fault",
                title="Strategy Fault Raised",
                dedup_key=self._runtime_alert_dedup_key("persistent_fault", "strategy_forced_fault", fault_code),
                recommended_action="Review the fault detail before clearing or resuming entries.",
                active=True,
            )
        self._persist_state(self._state, transition_label="forced_fault")
        return self._state

    def set_operator_halt(self, occurred_at: datetime, halted: bool) -> StrategyState:
        """Toggle operator entry-halt state and persist it."""
        self._state = replace(
            self._state,
            operator_halt=halted,
            entries_enabled=not halted,
            updated_at=occurred_at,
        )
        self._persist_state(self._state, transition_label="operator_halt" if halted else "operator_resume")
        return self._state

    def set_same_underlying_entry_hold(
        self,
        occurred_at: datetime,
        held: bool,
        *,
        reason: str | None = None,
    ) -> StrategyState:
        """Toggle explicit same-underlying entry gating without altering exits or generic operator-halt state."""
        resolved_reason = str(reason or "").strip() or None
        if (
            self._state.same_underlying_entry_hold is held
            and (self._state.same_underlying_hold_reason or None) == resolved_reason
        ):
            return self._state
        self._state = replace(
            self._state,
            same_underlying_entry_hold=held,
            same_underlying_hold_reason=resolved_reason if held else None,
            updated_at=occurred_at,
        )
        self._persist_state(
            self._state,
            transition_label="same_underlying_entry_hold" if held else "same_underlying_entry_hold_cleared",
        )
        return self._state

    def clear_fault(self, occurred_at: datetime) -> StrategyState:
        """Clear a recoverable fault back to READY."""
        recovered = replace(
            self._state,
            operator_halt=False,
            entries_enabled=True,
        )
        self._state = transition_to_ready(recovered, occurred_at)
        self._persist_state(self._state, transition_label="operator_clear_fault")
        return self._state

    def inspect_reconciliation(self, *, occurred_at: datetime, trigger: str, execution_engine: ExecutionEngine) -> dict[str, object]:
        """Inspect internal state vs broker state without mutating strategy state."""
        reconciler = self._build_reconciler()
        return reconciler.inspect(
            state=self._state,
            occurred_at=occurred_at,
            trigger=trigger,
            execution_engine=execution_engine,
        )

    def apply_reconciliation(self, *, occurred_at: datetime, trigger: str, execution_engine: ExecutionEngine) -> dict[str, object]:
        """Apply safe reconciliation repairs or transition into RECONCILING / FAULT."""
        reconciler = self._build_reconciler()
        previous_state = self._state
        next_state, payload = reconciler.reconcile(
            state=self._state,
            occurred_at=occurred_at,
            trigger=trigger,
            execution_engine=execution_engine,
        )
        self._state = next_state
        if next_state != previous_state:
            transition_label = (
                "reconciliation_safe_repair"
                if payload.get("classification") == "safe_repair"
                else "reconciliation_fault"
                if next_state.strategy_status is StrategyStatus.FAULT
                else "reconciliation_reconciling"
                if next_state.strategy_status is StrategyStatus.RECONCILING
                else "reconciliation_ready"
            )
            self._persist_state(self._state, transition_label=transition_label)
        return payload

    def force_reconcile(self, *, occurred_at: datetime, execution_engine: ExecutionEngine) -> dict[str, object]:
        """Run a manual operator-triggered reconciliation pass."""
        return self.apply_reconciliation(
            occurred_at=occurred_at,
            trigger="manual_force_reconcile",
            execution_engine=execution_engine,
        )

    def submit_operator_flatten_intent(
        self,
        occurred_at: datetime,
        reason_code: str = "operator_flatten_and_halt",
    ) -> OrderIntent | None:
        """Create and submit a paper-safe operator flatten intent against the current position."""
        try:
            return self.submit_runtime_exit_intent(
                occurred_at,
                quantity=self._state.internal_position_qty,
                reason_code=reason_code,
                signal_source="operatorFlatten",
            )
        except ValueError as exc:
            if "runtime-managed exit intent" in str(exc):
                raise ValueError("Execution engine rejected the operator flatten intent.") from exc
            raise

    def submit_runtime_exit_intent(
        self,
        occurred_at: datetime,
        *,
        quantity: int | None = None,
        reason_code: str,
        signal_source: str = "runtimeManagedExit",
        symbol: str | None = None,
    ) -> OrderIntent | None:
        """Create a runtime-managed exit intent, including staged partial exits."""
        if self._repositories is None:
            raise ValueError("Runtime-managed exit intent requires persistence repositories.")
        if self._state.position_side == PositionSide.FLAT or self._state.internal_position_qty <= 0:
            return None
        if self._state.open_broker_order_id is not None:
            raise ValueError("Cannot submit a runtime-managed exit intent while another broker order is open.")
        if not self._state.exits_enabled or self._state.fault_code is not None:
            return None

        resolved_quantity = int(quantity or self._state.internal_position_qty)
        if resolved_quantity <= 0:
            raise ValueError("Runtime-managed exit quantity must be > 0.")
        if resolved_quantity > self._state.internal_position_qty:
            raise ValueError("Runtime-managed exit quantity cannot exceed the current internal position quantity.")

        bar_id = f"runtime-exit|{int(occurred_at.timestamp() * 1000)}"
        if self._state.position_side == PositionSide.LONG:
            intent_type = OrderIntentType.SELL_TO_CLOSE
        else:
            intent_type = OrderIntentType.BUY_TO_CLOSE
        intent = OrderIntent(
            order_intent_id=f"{bar_id}|{intent_type.value}",
            bar_id=bar_id,
            symbol=self._settings.symbol if symbol is None else str(symbol),
            intent_type=intent_type,
            quantity=resolved_quantity,
            created_at=occurred_at,
            reason_code=reason_code,
        )
        pending = self._execution_engine.submit_intent(intent)
        if pending is None:
            self._state = self._handle_submit_failure_or_rejection(
                state=self._state,
                intent=intent,
                occurred_at=occurred_at,
                default_reason="Execution engine rejected the runtime-managed exit intent.",
            )
            self._persist_state(self._state, transition_label="runtime_exit_intent_rejected")
            raise ValueError("Execution engine rejected the runtime-managed exit intent.")
        self._state = replace(
            self._state,
            last_order_intent_id=intent.order_intent_id,
            open_broker_order_id=pending.broker_order_id,
            updated_at=occurred_at,
        )
        self._persist_order_intent(
            intent,
            pending.broker_order_id,
            submitted_at=pending.submitted_at,
            acknowledged_at=pending.acknowledged_at,
            broker_order_status=pending.broker_order_status,
            last_status_checked_at=pending.last_status_checked_at,
            retry_count=pending.retry_count,
        )
        self._persist_state(self._state, transition_label="runtime_exit_intent")
        self._latest_live_intent_summary = {
            "order_intent_id": intent.order_intent_id,
            "intent_type": intent.intent_type.value,
            "quantity": intent.quantity,
            "reason_code": intent.reason_code,
            "signal_source": signal_source,
            "submit_attempted_at": occurred_at.isoformat(),
            "submit_attempted": True,
            "submit_suppressed": False,
            "submit_gate_blocker": None,
            "resulting_position_side": self._state.position_side.value,
            "resulting_internal_qty": self._state.internal_position_qty,
        }
        self._emit_order_lifecycle_alert("created", intent, occurred_at, pending_broker_order_id=pending.broker_order_id)
        self._emit_order_lifecycle_alert("submitted", intent, occurred_at, pending_broker_order_id=pending.broker_order_id)
        return intent

    def submit_paper_canary_entry_intent(
        self,
        bar: Bar,
        *,
        signal_source: str = "paperExecutionCanary",
        reason_code: str = "paperExecutionCanaryEntryLateWindow",
    ) -> OrderIntent | None:
        """Create a paper-only synthetic long entry for the execution canary."""
        return self.submit_runtime_entry_intent(
            bar,
            side="LONG",
            signal_source=signal_source,
            reason_code=reason_code,
            long_entry_family=LongEntryFamily.K,
        )

    def submit_runtime_entry_intent(
        self,
        bar: Bar,
        *,
        side: str,
        signal_source: str,
        reason_code: str,
        symbol: str | None = None,
        long_entry_family: LongEntryFamily = LongEntryFamily.NONE,
        short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE,
    ) -> OrderIntent | None:
        """Create a synthetic paper entry intent for runtime-managed temporary lanes."""
        if self._repositories is None:
            raise ValueError("Runtime-managed paper entry requires persistence repositories.")
        normalized_side = str(side or "").strip().upper()
        if normalized_side not in {"LONG", "SHORT"}:
            raise ValueError(f"Unsupported runtime entry side: {side}")
        if self._state.open_broker_order_id is not None:
            return None
        if not self._entry_side_is_currently_allowed(normalized_side, self._state):
            return None
        if (
            not self._state.entries_enabled
            or self._state.operator_halt
            or self._state.same_underlying_entry_hold
        ):
            if self._state.same_underlying_entry_hold:
                intent_type = (
                    OrderIntentType.BUY_TO_OPEN
                    if normalized_side == "LONG"
                    else OrderIntentType.SELL_TO_OPEN
                )
                self._log_same_underlying_entry_block(
                    bar=bar,
                    intent_type=intent_type,
                    source=signal_source,
                    reason=(
                        str(self._state.same_underlying_hold_reason or "").strip()
                        or f"New entries held by operator for same-underlying conflict review on {self._settings.symbol}."
                    ),
                )
            return None

        payload = _empty_signal_packet_payload(bar.bar_id)
        payload.update(
            {
                "bar_id": bar.bar_id,
                "long_entry_raw": normalized_side == "LONG",
                "long_entry": normalized_side == "LONG",
                "long_entry_source": signal_source if normalized_side == "LONG" else None,
                "short_entry_raw": normalized_side == "SHORT",
                "short_entry": normalized_side == "SHORT",
                "short_entry_source": signal_source if normalized_side == "SHORT" else None,
            }
        )
        signal_packet = SignalPacket(**payload)
        self._repositories.signals.save(signal_packet, created_at=bar.end_ts)
        self._last_signal_packet = signal_packet
        self._log_branch_source(
            bar,
            "long" if normalized_side == "LONG" else "short",
            signal_source,
            True,
            None,
        )

        intent_type = (
            OrderIntentType.BUY_TO_OPEN
            if normalized_side == "LONG"
            else OrderIntentType.SELL_TO_OPEN
        )
        intent = OrderIntent(
            order_intent_id=f"{bar.bar_id}|{intent_type.value}",
            bar_id=bar.bar_id,
            symbol=str(symbol or self._settings.symbol),
            intent_type=intent_type,
            quantity=self._runtime_entry_quantity(),
            created_at=bar.end_ts,
            reason_code=reason_code,
        )
        pending = self._execution_engine.submit_intent(
            intent,
            signal_bar_id=bar.bar_id,
            long_entry_family=long_entry_family if normalized_side == "LONG" else LongEntryFamily.NONE,
            short_entry_family=short_entry_family if normalized_side == "SHORT" else ShortEntryFamily.NONE,
            short_entry_source=signal_source if normalized_side == "SHORT" else None,
        )
        if pending is None:
            self._state = self._handle_submit_failure_or_rejection(
                state=self._state,
                intent=intent,
                occurred_at=bar.end_ts,
                default_reason="Execution engine rejected the runtime entry intent.",
            )
            self._persist_state(
                self._state,
                transition_label="runtime_entry_intent_rejected_long" if normalized_side == "LONG" else "runtime_entry_intent_rejected_short",
            )
            return None
        self._state = replace(
            self._state,
            last_order_intent_id=intent.order_intent_id,
            open_broker_order_id=pending.broker_order_id,
            updated_at=bar.end_ts,
        )
        self._persist_order_intent(
            intent,
            pending.broker_order_id,
            submitted_at=pending.submitted_at,
            acknowledged_at=pending.acknowledged_at,
            broker_order_status=pending.broker_order_status,
            last_status_checked_at=pending.last_status_checked_at,
            retry_count=pending.retry_count,
        )
        self._persist_state(
            self._state,
            transition_label="runtime_entry_intent_long" if normalized_side == "LONG" else "runtime_entry_intent_short",
        )
        self._emit_order_lifecycle_alert("created", intent, bar.end_ts, pending_broker_order_id=pending.broker_order_id)
        self._emit_order_lifecycle_alert("submitted", intent, bar.end_ts, pending_broker_order_id=pending.broker_order_id)
        return intent

    def _load_initial_state(self, initial_state: Optional[StrategyState]) -> StrategyState:
        if initial_state is not None:
            return normalize_legacy_single_position_state(initial_state)
        if self._state_repository is not None:
            persisted_state = self._state_repository.load_latest()
            if persisted_state is not None:
                return normalize_legacy_single_position_state(persisted_state)
        now = datetime.now(timezone.utc)
        return transition_to_ready(build_initial_state(now), now)

    def _entry_side_is_currently_allowed(self, desired_side: str, state: StrategyState) -> bool:
        normalized_side = str(desired_side or "").strip().upper()
        if normalized_side not in {"LONG", "SHORT"}:
            return False
        if state.open_broker_order_id is not None:
            return False
        if state.strategy_status not in {
            StrategyStatus.READY,
            StrategyStatus.IN_LONG_K,
            StrategyStatus.IN_LONG_VWAP,
            StrategyStatus.IN_SHORT_K,
        }:
            return False
        if normalized_side == "LONG" and state.position_side == PositionSide.SHORT:
            return False
        if normalized_side == "SHORT" and state.position_side == PositionSide.LONG:
            return False
        if state.position_side == PositionSide.FLAT:
            return True
        if self._settings.add_direction_policy is not AddDirectionPolicy.SAME_DIRECTION_ONLY:
            return False
        if normalized_side != state.position_side.value:
            return False
        return self._can_add_to_existing_position(state)

    def _can_add_to_existing_position(self, state: StrategyState) -> bool:
        if state.position_side == PositionSide.FLAT:
            return True
        if self._settings.participation_policy is ParticipationPolicy.SINGLE_ENTRY_ONLY:
            return False
        entry_leg_count = len(state.open_entry_legs)
        if entry_leg_count <= 0:
            return False
        if entry_leg_count >= self._settings.max_concurrent_entries:
            return False
        if (entry_leg_count - 1) >= self._settings.max_adds_after_entry:
            return False
        next_quantity = state.internal_position_qty + self._runtime_entry_quantity()
        max_position_quantity = self._settings.max_position_quantity or (
            self._settings.trade_size * self._settings.max_concurrent_entries
        )
        if next_quantity > max_position_quantity:
            return False
        return True

    def _runtime_entry_quantity(self) -> int:
        return int(self._settings.trade_size)

    def _restore_processing_context(self) -> None:
        if self._repositories is None:
            return
        restore_limit = self._settings.warmup_bars_required()
        recent_bars = self._repositories.bars.list_recent_processed(
            symbol=self._settings.symbol,
            timeframe=self._settings.timeframe,
            limit=restore_limit,
        )
        if not recent_bars:
            return
        recent_features = self._repositories.features.load_by_bar_ids([bar.bar_id for bar in recent_bars])
        self._bar_history = recent_bars
        self._feature_history = recent_features
        if recent_features:
            self._last_feature_packet = recent_features[-1]

    def _apply_due_replay_fills(self, bar: Bar) -> list[DomainEvent]:
        events: list[DomainEvent] = []
        if not isinstance(self._execution_engine.broker, PaperBroker):
            return events
        for pending in self._execution_engine.pop_due_replay_fills(bar, self._settings):
            fill = self._execution_engine.materialize_replay_fill(pending, bar)
            self._persist_order_intent(
                pending.intent,
                fill.broker_order_id or pending.broker_order_id,
                order_status=OrderStatus.FILLED,
                submitted_at=pending.submitted_at,
                acknowledged_at=pending.acknowledged_at or bar.start_ts,
                broker_order_status=OrderStatus.FILLED.value,
                last_status_checked_at=bar.start_ts,
                retry_count=pending.retry_count,
            )
            self.apply_fill(
                fill_event=fill,
                signal_bar_id=pending.signal_bar_id,
                long_entry_family=pending.long_entry_family,
                short_entry_family=pending.short_entry_family,
                short_entry_source=pending.short_entry_source,
            )
            events.append(
                FillReceivedEvent(
                    order_intent_id=fill.order_intent_id,
                    broker_order_id=fill.broker_order_id,
                    fill_timestamp=fill.fill_timestamp,
                    fill_price=fill.fill_price,
                )
            )
        return events

    def _evaluate_signals(self, feature_packet: FeaturePacket, feature_history: list[FeaturePacket]) -> SignalPacket:
        bull = evaluate_bull_snap(self._bar_history, feature_packet, self._state, self._settings, feature_history)
        bear = evaluate_bear_snap(self._bar_history, feature_packet, self._state, self._settings, feature_history)
        asia = evaluate_asia_vwap_reclaim(self._bar_history, feature_history, self._state, self._settings)

        signal_packet = SignalPacket(
            bar_id=feature_packet.bar_id,
            bull_snap_downside_stretch_ok=bull["bull_snap_downside_stretch_ok"],
            bull_snap_range_ok=bull["bull_snap_range_ok"],
            bull_snap_body_ok=bull["bull_snap_body_ok"],
            bull_snap_close_strong=bull["bull_snap_close_strong"],
            bull_snap_velocity_ok=bull["bull_snap_velocity_ok"],
            bull_snap_reversal_bar=bull["bull_snap_reversal_bar"],
            bull_snap_location_ok=bull["bull_snap_location_ok"],
            bull_snap_raw=bull["bull_snap_raw"],
            bull_snap_turn_candidate=bull["bull_snap_turn_candidate"],
            first_bull_snap_turn=bull["first_bull_snap_turn"],
            below_vwap_recently=asia["below_vwap_recently"],
            reclaim_range_ok=asia["reclaim_range_ok"],
            reclaim_vol_ok=asia["reclaim_vol_ok"],
            reclaim_color_ok=asia["reclaim_color_ok"],
            reclaim_close_ok=asia["reclaim_close_ok"],
            asia_reclaim_bar_raw=asia["asia_reclaim_bar_raw"],
            asia_hold_bar=asia["asia_hold_bar"],
            asia_hold_close_vwap_ok=asia["asia_hold_close_vwap_ok"],
            asia_hold_low_ok=asia["asia_hold_low_ok"],
            asia_hold_bar_ok=asia["asia_hold_bar_ok"],
            asia_acceptance_bar=asia["asia_acceptance_bar"],
            asia_acceptance_close_high_ok=asia["asia_acceptance_close_high_ok"],
            asia_acceptance_close_vwap_ok=asia["asia_acceptance_close_vwap_ok"],
            asia_acceptance_bar_ok=asia["asia_acceptance_bar_ok"],
            asia_vwap_long_signal=asia["asia_vwap_long_signal"],
            midday_pause_resume_long_turn_candidate=bull["midday_pause_resume_long_turn_candidate"],
            us_late_breakout_retest_hold_long_turn_candidate=bull["us_late_breakout_retest_hold_long_turn_candidate"],
            us_late_failed_move_reversal_long_turn_candidate=bull["us_late_failed_move_reversal_long_turn_candidate"],
            us_late_pause_resume_long_turn_candidate=bull["us_late_pause_resume_long_turn_candidate"],
            asia_early_breakout_retest_hold_long_turn_candidate=bull[
                "asia_early_breakout_retest_hold_long_turn_candidate"
            ],
            asia_early_normal_breakout_retest_hold_long_turn_candidate=bull[
                "asia_early_normal_breakout_retest_hold_long_turn_candidate"
            ],
            asia_late_pause_resume_long_turn_candidate=bull["asia_late_pause_resume_long_turn_candidate"],
            asia_late_flat_pullback_pause_resume_long_turn_candidate=bull["asia_late_flat_pullback_pause_resume_long_turn_candidate"],
            asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate=bull[
                "asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate"
            ],
            bear_snap_up_stretch_ok=bear["bear_snap_up_stretch_ok"],
            bear_snap_range_ok=bear["bear_snap_range_ok"],
            bear_snap_body_ok=bear["bear_snap_body_ok"],
            bear_snap_close_weak=bear["bear_snap_close_weak"],
            bear_snap_velocity_ok=bear["bear_snap_velocity_ok"],
            bear_snap_reversal_bar=bear["bear_snap_reversal_bar"],
            bear_snap_location_ok=bear["bear_snap_location_ok"],
            bear_snap_raw=bear["bear_snap_raw"],
            bear_snap_turn_candidate=bear["bear_snap_turn_candidate"],
            first_bear_snap_turn=bear["first_bear_snap_turn"],
            derivative_bear_slope_ok=bear["derivative_bear_slope_ok"],
            derivative_bear_curvature_ok=bear["derivative_bear_curvature_ok"],
            derivative_bear_turn_candidate=bear["derivative_bear_turn_candidate"],
            derivative_bear_additive_turn_candidate=bear["derivative_bear_additive_turn_candidate"],
            midday_compressed_failed_move_reversal_short_turn_candidate=bear[
                "midday_compressed_failed_move_reversal_short_turn_candidate"
            ],
            midday_compressed_rebound_failed_move_reversal_short_turn_candidate=bear[
                "midday_compressed_rebound_failed_move_reversal_short_turn_candidate"
            ],
            midday_expanded_pause_resume_short_turn_candidate=bear["midday_expanded_pause_resume_short_turn_candidate"],
            midday_compressed_pause_resume_short_turn_candidate=bear["midday_compressed_pause_resume_short_turn_candidate"],
            midday_pause_resume_short_turn_candidate=bear["midday_pause_resume_short_turn_candidate"],
            london_late_pause_resume_short_turn_candidate=bear["london_late_pause_resume_short_turn_candidate"],
            asia_early_expanded_breakout_retest_hold_short_turn_candidate=bear[
                "asia_early_expanded_breakout_retest_hold_short_turn_candidate"
            ],
            asia_early_compressed_pause_resume_short_turn_candidate=bear["asia_early_compressed_pause_resume_short_turn_candidate"],
            asia_early_pause_resume_short_turn_candidate=bear["asia_early_pause_resume_short_turn_candidate"],
            long_entry_raw=False,
            short_entry_raw=False,
            recent_long_setup=False,
            recent_short_setup=False,
            long_entry=False,
            short_entry=False,
            long_entry_source=None,
            short_entry_source=None,
        )
        return resolve_entries(signal_packet, self._state, self._settings)

    def _apply_runtime_entry_controls(self, bar: Bar, signal_packet: SignalPacket) -> SignalPacket:
        packet = signal_packet

        if signal_packet.long_entry and signal_packet.long_entry_source is not None:
            block_reason = self._blocked_long_entry_reason(bar, signal_packet.long_entry_source)
            self._log_branch_source(bar, "long", signal_packet.long_entry_source, block_reason is None, block_reason)
            if block_reason is not None:
                packet = replace(packet, long_entry=False, long_entry_source=None)

        if signal_packet.short_entry and signal_packet.short_entry_source is not None:
            block_reason = self._blocked_short_entry_reason(bar, signal_packet.short_entry_source)
            self._log_branch_source(bar, "short", signal_packet.short_entry_source, block_reason is None, block_reason)
            if block_reason is not None:
                packet = replace(packet, short_entry=False, short_entry_source=None)

        return packet

    def _blocked_long_entry_reason(self, bar: Bar, source: str) -> Optional[str]:
        if (
            self._settings.us_late_pause_resume_long_exclude_1755_carryover
            and source == "usLatePauseResumeLongTurn"
            and bar.end_ts.astimezone(self._settings.timezone_info).time() == time(16, 55)
        ):
            return "us_late_1755_carryover_exclusion"
        if (
            self._settings.probationary_paper_lane_session_restriction
            and not _gc_mgc_asia_retest_hold_london_open_extension_matches(
                bar=bar,
                source=source,
                timezone_info=self._settings.timezone_info,
            )
            and not _bar_matches_probationary_session_restriction(
                bar,
                self._settings.probationary_paper_lane_session_restriction,
                self._settings.timezone_info,
            )
        ):
            return (
                "probationary_session_restriction_"
                f"{self._settings.probationary_paper_lane_session_restriction.lower()}"
            )
        if (
            self._settings.probationary_enforce_approved_branches
            and source not in self._settings.approved_long_entry_sources
        ):
            return "probationary_long_source_not_allowlisted"
        return None

    def _blocked_short_entry_reason(self, bar: Bar, source: str) -> Optional[str]:
        if (
            self._settings.probationary_paper_lane_session_restriction
            and not _bar_matches_probationary_session_restriction(
                bar,
                self._settings.probationary_paper_lane_session_restriction,
                self._settings.timezone_info,
            )
        ):
            return (
                "probationary_session_restriction_"
                f"{self._settings.probationary_paper_lane_session_restriction.lower()}"
            )
        if (
            self._settings.probationary_enforce_approved_branches
            and source not in self._settings.approved_short_entry_sources
        ):
            return "probationary_short_source_not_allowlisted"
        return None

    def _log_branch_source(
        self,
        bar: Bar,
        side: str,
        source: str,
        allowed: bool,
        block_reason: Optional[str],
    ) -> None:
        if self._structured_logger is None:
            return
        payload = {
            "bar_id": bar.bar_id,
            "bar_end_ts": bar.end_ts.isoformat(),
            "side": side,
            "source": source,
            "decision": "allowed" if allowed else "blocked",
            "block_reason": block_reason,
        }
        self._structured_logger.log_branch_source(payload)
        if not allowed:
            self._structured_logger.log_rule_block(payload)
            if self._alert_dispatcher is not None:
                self._alert_dispatcher.emit(
                    severity="ACTION",
                    code="branch_rule_blocked",
                    message=f"Blocked {side} entry from {source}",
                    payload={**payload, **self._runtime_identity},
                    category="order_rejection",
                    title="Branch Rule Blocked",
                    dedup_key=self._runtime_alert_dedup_key("branch_rule_blocked", side, source, bar.bar_id),
                    active=False,
                    coalesce=False,
                )

    def _log_same_underlying_entry_block(
        self,
        *,
        bar: Bar,
        intent_type: OrderIntentType,
        source: str | None,
        reason: str,
    ) -> None:
        payload = {
            "event_type": "entry_blocked_by_same_underlying_hold",
            "action": "same_underlying_entry_hold_blocked",
            "occurred_at": bar.end_ts.isoformat(),
            "bar_id": bar.bar_id,
            "instrument": self._settings.symbol,
            "standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
            "strategy_family": self._runtime_identity.get("strategy_family"),
            "lane_id": self._runtime_identity.get("lane_id"),
            "blocked_standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
            "blocked_reason": reason,
            "hold_new_entries": True,
            "entry_hold_effective": True,
            "review_state_status": "HOLDING",
            "operator_label": "automatic runtime control",
            "automatic": True,
            "operator_triggered": False,
            "intent_type": intent_type.value,
            "signal_source": source,
            "conflict_kind": "multiple_runtime_instances_same_instrument",
            "severity": "BLOCKING",
            "message": reason,
        }
        payload["event_id"] = hashlib.sha256(
            json.dumps(
                {
                    "event_type": payload["event_type"],
                    "occurred_at": payload["occurred_at"],
                    "instrument": payload["instrument"],
                    "standalone_strategy_id": payload["standalone_strategy_id"],
                    "bar_id": payload["bar_id"],
                    "intent_type": payload["intent_type"],
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        if self._structured_logger is not None:
            self._structured_logger.log_operator_control(payload)
        if self._alert_dispatcher is not None:
            self._alert_dispatcher.emit(
                severity="ACTION",
                code="same_underlying_entry_blocked",
                message=reason,
                payload={**payload, **self._runtime_identity},
                category="same_underlying_conflict",
                title="Same-Underlying Entry Blocked",
                dedup_key=self._runtime_alert_dedup_key("same_underlying_entry_blocked", bar.bar_id, intent_type.value),
                recommended_action="Review the same-underlying conflict only if live exposure or pending-order ambiguity is present.",
                active=True,
            )

    def _compute_feature_packet(self, bar: Bar) -> FeaturePacket:
        if self._use_incremental_features and self._incremental_feature_computer is not None:
            return self._incremental_feature_computer.compute_next(bar, self._state)
        return compute_features(self._bar_history, self._state, self._settings)

    def _advance_state_for_bar(
        self,
        feature_packet: FeaturePacket,
        signal_packet: SignalPacket,
        occurred_at: datetime,
    ) -> StrategyState:
        return replace(
            self._state,
            last_swing_low=feature_packet.last_swing_low,
            last_swing_high=feature_packet.last_swing_high,
            asia_reclaim_bar_low=self._bar_history[-1].low if signal_packet.asia_reclaim_bar_raw else self._state.asia_reclaim_bar_low,
            asia_reclaim_bar_high=self._bar_history[-1].high if signal_packet.asia_reclaim_bar_raw else self._state.asia_reclaim_bar_high,
            asia_reclaim_bar_vwap=feature_packet.vwap if signal_packet.asia_reclaim_bar_raw else self._state.asia_reclaim_bar_vwap,
            bars_since_bull_snap=_next_counter(self._state.bars_since_bull_snap, signal_packet.bull_snap_turn_candidate),
            bars_since_bear_snap=_next_counter(self._state.bars_since_bear_snap, signal_packet.bear_snap_turn_candidate),
            bars_since_asia_reclaim=_next_counter(self._state.bars_since_asia_reclaim, signal_packet.asia_reclaim_bar_raw),
            bars_since_asia_vwap_signal=_next_counter(self._state.bars_since_asia_vwap_signal, signal_packet.asia_vwap_long_signal),
            bars_since_long_setup=_next_counter(self._state.bars_since_long_setup, signal_packet.long_entry_raw),
            bars_since_short_setup=_next_counter(self._state.bars_since_short_setup, signal_packet.short_entry_raw),
            last_signal_bar_id=signal_packet.bar_id if _signal_present(signal_packet) else self._state.last_signal_bar_id,
            updated_at=occurred_at,
        )

    def _maybe_create_order_intent(
        self,
        bar: Bar,
        signal_packet: SignalPacket,
        state: StrategyState,
        exit_decision: ExitDecision,
    ) -> Optional[OrderIntent]:
        warmup_complete = len(self._bar_history) >= self._settings.warmup_bars_required()
        if self._entry_side_is_currently_allowed("LONG", state) or self._entry_side_is_currently_allowed("SHORT", state):
            if (
                state.entries_enabled
                and not state.operator_halt
                and state.same_underlying_entry_hold
            ):
                hold_reason = (
                    str(state.same_underlying_hold_reason or "").strip()
                    or f"New entries held by operator for same-underlying conflict review on {self._settings.symbol}."
                )
                if signal_packet.long_entry and self._entry_side_is_currently_allowed("LONG", state):
                    self._log_same_underlying_entry_block(
                        bar=bar,
                        intent_type=OrderIntentType.BUY_TO_OPEN,
                        source=signal_packet.long_entry_source,
                        reason=hold_reason,
                    )
                    return None
                if signal_packet.short_entry and self._entry_side_is_currently_allowed("SHORT", state):
                    self._log_same_underlying_entry_block(
                        bar=bar,
                        intent_type=OrderIntentType.SELL_TO_OPEN,
                        source=signal_packet.short_entry_source,
                        reason=hold_reason,
                    )
                    return None
            if (
                warmup_complete
                and state.entries_enabled
                and not state.operator_halt
                and not state.same_underlying_entry_hold
            ):
                if signal_packet.long_entry and self._entry_side_is_currently_allowed("LONG", state):
                    return OrderIntent(
                        order_intent_id=f"{bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
                        bar_id=bar.bar_id,
                        symbol=self._settings.symbol,
                        intent_type=OrderIntentType.BUY_TO_OPEN,
                        quantity=self._runtime_entry_quantity(),
                        created_at=bar.end_ts,
                        reason_code=signal_packet.long_entry_source or "longEntry",
                    )
                if signal_packet.short_entry and self._entry_side_is_currently_allowed("SHORT", state):
                    return OrderIntent(
                        order_intent_id=f"{bar.bar_id}|{OrderIntentType.SELL_TO_OPEN.value}",
                        bar_id=bar.bar_id,
                        symbol=self._settings.symbol,
                        intent_type=OrderIntentType.SELL_TO_OPEN,
                        quantity=self._runtime_entry_quantity(),
                        created_at=bar.end_ts,
                        reason_code=signal_packet.short_entry_source or "shortEntry",
                    )
            return None

        if state.position_side == PositionSide.LONG and state.exits_enabled and exit_decision.long_exit:
            return OrderIntent(
                order_intent_id=f"{bar.bar_id}|{OrderIntentType.SELL_TO_CLOSE.value}",
                bar_id=bar.bar_id,
                symbol=self._settings.symbol,
                intent_type=OrderIntentType.SELL_TO_CLOSE,
                quantity=state.internal_position_qty,
                created_at=bar.end_ts,
                reason_code=exit_decision.primary_reason.value if exit_decision.primary_reason else "longExit",
            )

        if state.position_side == PositionSide.SHORT and state.exits_enabled and exit_decision.short_exit:
            return OrderIntent(
                order_intent_id=f"{bar.bar_id}|{OrderIntentType.BUY_TO_CLOSE.value}",
                bar_id=bar.bar_id,
                symbol=self._settings.symbol,
                intent_type=OrderIntentType.BUY_TO_CLOSE,
                quantity=state.internal_position_qty,
                created_at=bar.end_ts,
                reason_code=exit_decision.primary_reason.value if exit_decision.primary_reason else "shortExit",
            )
        return None

    def _resolve_long_entry_family(self, signal_packet: SignalPacket) -> LongEntryFamily:
        if signal_packet.long_entry_source == "asiaVWAPLongSignal":
            return LongEntryFamily.VWAP
        if signal_packet.long_entry_source == "firstBullSnapTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "usMiddayPauseResumeLongTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "usLateBreakoutRetestHoldTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "usLateFailedMoveReversalLongTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "usLatePauseResumeLongTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "asiaEarlyNormalBreakoutRetestHoldTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "asiaEarlyBreakoutRetestHoldTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "asiaLateCompressedFlatPullbackPauseResumeLongTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "asiaLateFlatPullbackPauseResumeLongTurn":
            return LongEntryFamily.K
        if signal_packet.long_entry_source == "asiaLatePauseResumeLongTurn":
            return LongEntryFamily.K
        return LongEntryFamily.NONE

    def _resolve_short_entry_family(self, signal_packet: SignalPacket) -> ShortEntryFamily:
        if signal_packet.short_entry_source == "firstBearSnapTurn":
            return ShortEntryFamily.BEAR_SNAP
        if signal_packet.short_entry_source == "usDerivativeBearTurn":
            return ShortEntryFamily.DERIVATIVE_BEAR
        if signal_packet.short_entry_source == "usDerivativeBearAdditiveTurn":
            return ShortEntryFamily.DERIVATIVE_BEAR_ADDITIVE
        if signal_packet.short_entry_source == "usMiddayCompressedReboundFailedMoveReversalShortTurn":
            return ShortEntryFamily.FAILED_MOVE_REVERSAL_SHORT
        if signal_packet.short_entry_source == "usMiddayCompressedFailedMoveReversalShortTurn":
            return ShortEntryFamily.FAILED_MOVE_REVERSAL_SHORT
        if signal_packet.short_entry_source == "usMiddayExpandedPauseResumeShortTurn":
            return ShortEntryFamily.MIDDAY_PAUSE_RESUME_SHORT
        if signal_packet.short_entry_source == "usMiddayCompressedPauseResumeShortTurn":
            return ShortEntryFamily.MIDDAY_PAUSE_RESUME_SHORT
        if signal_packet.short_entry_source == "usMiddayPauseResumeShortTurn":
            return ShortEntryFamily.MIDDAY_PAUSE_RESUME_SHORT
        if signal_packet.short_entry_source == "londonLatePauseResumeShortTurn":
            return ShortEntryFamily.LONDON_LATE_PAUSE_RESUME_SHORT
        if signal_packet.short_entry_source == "asiaEarlyCompressedPauseResumeShortTurn":
            return ShortEntryFamily.ASIA_EARLY_PAUSE_RESUME_SHORT
        if signal_packet.short_entry_source == "asiaEarlyPauseResumeShortTurn":
            return ShortEntryFamily.ASIA_EARLY_PAUSE_RESUME_SHORT
        return ShortEntryFamily.NONE

    def _build_exit_decision_summary(
        self,
        *,
        bar: Bar,
        state: StrategyState,
        exit_decision: ExitDecision,
        risk_context: object,
        exit_fill_pending: bool,
        exit_fill_confirmed: bool,
    ) -> dict[str, object]:
        current_position_family = (
            state.long_entry_family.value
            if state.position_side is PositionSide.LONG
            else state.short_entry_family.value
            if state.position_side is PositionSide.SHORT
            else "NONE"
        )
        return {
            "evaluated_at": bar.end_ts.isoformat(),
            "bar_id": bar.bar_id,
            "position_side": state.position_side.value,
            "current_position_family": current_position_family,
            "long_entry_family": state.long_entry_family.value,
            "short_entry_family": state.short_entry_family.value,
            "short_entry_source": state.short_entry_source,
            "bars_in_trade": state.bars_in_trade,
            "primary_reason": exit_decision.primary_reason.value if exit_decision.primary_reason is not None else None,
            "all_true_reasons": [reason.value for reason in exit_decision.all_true_reasons],
            "long_break_even_armed": state.long_be_armed,
            "short_break_even_armed": state.short_be_armed,
            "active_long_stop_ref": getattr(risk_context, "active_long_stop_ref", None),
            "active_short_stop_ref": getattr(risk_context, "active_short_stop_ref", None),
            "active_long_stop_ref_base": getattr(risk_context, "active_long_stop_ref_base", None),
            "k_long_stop_ref_base": getattr(risk_context, "k_long_stop_ref_base", None),
            "vwap_long_stop_ref_base": getattr(risk_context, "vwap_long_stop_ref_base", None),
            "long_risk": getattr(risk_context, "long_risk", None),
            "short_risk": getattr(risk_context, "short_risk", None),
            "k_long_integrity_lost": exit_decision.k_long_integrity_lost,
            "vwap_lost": exit_decision.vwap_lost,
            "vwap_weak_follow_through": exit_decision.vwap_weak_follow_through,
            "short_integrity_lost": exit_decision.short_integrity_lost,
            "exit_requested": exit_decision.long_exit or exit_decision.short_exit,
            "exit_fill_pending": exit_fill_pending,
            "exit_fill_confirmed": exit_fill_confirmed,
        }

    def _emit_fill_alert(self, fill_event: FillEvent) -> None:
        if self._alert_dispatcher is None:
            return
        lifecycle = "entry_filled" if fill_event.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN) else "exit_filled"
        title = "Entry Filled" if lifecycle == "entry_filled" else "Exit Filled"
        self._alert_dispatcher.emit(
            severity="AUDIT_ONLY",
            code=lifecycle,
            message=f"{title} for {self._settings.symbol}.",
            payload={
                **self._runtime_identity,
                "instrument": self._settings.symbol,
                "order_intent_id": fill_event.order_intent_id,
                "intent_type": fill_event.intent_type.value,
                "broker_order_id": fill_event.broker_order_id,
                "fill_timestamp": fill_event.fill_timestamp.isoformat(),
                "fill_price": str(fill_event.fill_price) if fill_event.fill_price is not None else None,
            },
            category=lifecycle,
            title=title,
            dedup_key=self._runtime_alert_dedup_key(lifecycle, fill_event.order_intent_id),
            active=False,
            coalesce=False,
        )

    def _emit_order_lifecycle_alert(
        self,
        stage: str,
        intent: OrderIntent,
        occurred_at: datetime,
        *,
        pending_broker_order_id: str | None = None,
    ) -> None:
        if self._alert_dispatcher is None:
            return
        is_entry = intent.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN)
        lifecycle = f"{'entry' if is_entry else 'exit'}_{stage}"
        title = f"{'Entry' if is_entry else 'Exit'} {stage.replace('_', ' ').title()}"
        self._alert_dispatcher.emit(
            severity="AUDIT_ONLY",
            code=lifecycle,
            message=f"{title} for {intent.symbol}.",
            payload={
                **self._runtime_identity,
                "instrument": intent.symbol,
                "order_intent_id": intent.order_intent_id,
                "intent_type": intent.intent_type.value,
                "quantity": intent.quantity,
                "reason_code": intent.reason_code,
                "broker_order_id": pending_broker_order_id,
                "occurred_at": occurred_at.isoformat(),
            },
            category=lifecycle,
            title=title,
            dedup_key=self._runtime_alert_dedup_key(lifecycle, intent.order_intent_id),
            active=False,
            coalesce=False,
            occurred_at=occurred_at,
        )

    def _emit_order_rejection_alert(self, intent: OrderIntent, occurred_at: datetime, *, reason: str) -> None:
        if self._alert_dispatcher is None:
            return
        self._alert_dispatcher.emit(
            severity="ACTION",
            code="order_rejection",
            message=f"Order intent for {intent.symbol} was rejected before broker submission.",
            payload={
                **self._runtime_identity,
                "instrument": intent.symbol,
                "order_intent_id": intent.order_intent_id,
                "intent_type": intent.intent_type.value,
                "quantity": intent.quantity,
                "reason_code": intent.reason_code,
                "rejection_reason": reason,
                "occurred_at": occurred_at.isoformat(),
            },
            category="order_rejection",
            title="Order Rejected",
            dedup_key=self._runtime_alert_dedup_key("order_rejection", intent.order_intent_id),
            recommended_action="Review pending-order and opposite-side exposure state before retrying.",
            active=False,
            coalesce=False,
            occurred_at=occurred_at,
        )

    def _emit_shadow_submit_suppressed_alert(self, intent: OrderIntent, occurred_at: datetime) -> None:
        if self._alert_dispatcher is None:
            return
        self._alert_dispatcher.emit(
            severity="AUDIT_ONLY",
            code="shadow_submit_suppressed",
            message=f"Shadow mode suppressed broker submit for {intent.symbol}.",
            payload={
                **self._runtime_identity,
                "instrument": intent.symbol,
                "order_intent_id": intent.order_intent_id,
                "intent_type": intent.intent_type.value,
                "reason_code": intent.reason_code,
                "occurred_at": occurred_at.isoformat(),
                "shadow_mode_no_submit": True,
            },
            category="shadow_submit_suppressed",
            title="Shadow Submit Suppressed",
            dedup_key=self._runtime_alert_dedup_key("shadow_submit_suppressed", intent.order_intent_id),
            active=False,
            coalesce=False,
            occurred_at=occurred_at,
        )

    def _handle_submit_failure_or_rejection(
        self,
        *,
        state: StrategyState,
        intent: OrderIntent,
        occurred_at: datetime,
        default_reason: str,
    ) -> StrategyState:
        failure = self._execution_engine.last_submit_failure()
        reason = default_reason
        if failure is not None and failure.order_intent_id == intent.order_intent_id:
            reason = f"{default_reason} Broker stage={failure.failure_stage}: {failure.error}"
        self._emit_order_rejection_alert(intent, occurred_at, reason=reason)
        if failure is None or failure.order_intent_id != intent.order_intent_id:
            return state
        reconciler = self._build_reconciler()
        next_state, _ = reconciler.reconcile(
            state=replace(
                state,
                entries_enabled=False,
                updated_at=occurred_at,
            ),
            occurred_at=occurred_at,
            trigger="broker_submit_failed",
            execution_engine=self._execution_engine,
        )
        return next_state

    def _runtime_alert_dedup_key(self, *parts: object) -> str:
        identity = {
            "standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
            "lane_id": self._runtime_identity.get("lane_id"),
            "instrument": self._runtime_identity.get("instrument") or self._settings.symbol,
            "parts": [str(part) for part in parts],
        }
        return hashlib.sha256(json.dumps(identity, sort_keys=True).encode("utf-8")).hexdigest()[:24]

    def _build_shadow_intent_summary(
        self,
        *,
        bar: Bar,
        state: StrategyState,
        signal_packet: SignalPacket,
        exit_decision: ExitDecision,
        risk_context: object,
        intent: OrderIntent,
        long_entry_family: LongEntryFamily,
        short_entry_family: ShortEntryFamily,
        short_entry_source: str | None,
    ) -> dict[str, object]:
        return {
            "bar_id": bar.bar_id,
            "bar_end_ts": bar.end_ts.isoformat(),
            "order_intent_id": intent.order_intent_id,
            "symbol": intent.symbol,
            "intent_type": intent.intent_type.value,
            "quantity": intent.quantity,
            "reason_code": intent.reason_code,
            "created_at": intent.created_at.isoformat(),
            "shadow_submit_suppressed": True,
            "position_side_before_submit": state.position_side.value,
            "long_entry_family": long_entry_family.value,
            "short_entry_family": short_entry_family.value,
            "short_entry_source": short_entry_source,
            "signal_long_entry": signal_packet.long_entry,
            "signal_short_entry": signal_packet.short_entry,
            "signal_long_entry_source": signal_packet.long_entry_source,
            "signal_short_entry_source": signal_packet.short_entry_source,
            "exit_primary_reason": exit_decision.primary_reason.value if exit_decision.primary_reason is not None else None,
            "exit_all_true_reasons": [reason.value for reason in exit_decision.all_true_reasons],
            "long_break_even_armed": getattr(risk_context, "long_break_even_armed", state.long_be_armed),
            "short_break_even_armed": getattr(risk_context, "short_break_even_armed", state.short_be_armed),
            "active_long_stop_ref": getattr(risk_context, "active_long_stop_ref", None),
            "active_short_stop_ref": getattr(risk_context, "active_short_stop_ref", None),
        }

    def _build_live_intent_summary(
        self,
        *,
        bar: Bar,
        state: StrategyState,
        signal_packet: SignalPacket,
        exit_decision: ExitDecision,
        risk_context: object,
        intent: OrderIntent,
        long_entry_family: LongEntryFamily,
        short_entry_family: ShortEntryFamily,
        short_entry_source: str | None,
    ) -> dict[str, object]:
        return {
            "bar_id": bar.bar_id,
            "bar_end_ts": bar.end_ts.isoformat(),
            "order_intent_id": intent.order_intent_id,
            "symbol": intent.symbol,
            "intent_type": intent.intent_type.value,
            "quantity": intent.quantity,
            "reason_code": intent.reason_code,
            "created_at": intent.created_at.isoformat(),
            "position_side_before_submit": state.position_side.value,
            "long_entry_family": long_entry_family.value,
            "short_entry_family": short_entry_family.value,
            "short_entry_source": short_entry_source,
            "signal_long_entry": signal_packet.long_entry,
            "signal_short_entry": signal_packet.short_entry,
            "signal_long_entry_source": signal_packet.long_entry_source,
            "signal_short_entry_source": signal_packet.short_entry_source,
            "exit_primary_reason": exit_decision.primary_reason.value if exit_decision.primary_reason is not None else None,
            "exit_all_true_reasons": [reason.value for reason in exit_decision.all_true_reasons],
            "long_break_even_armed": getattr(risk_context, "long_break_even_armed", state.long_be_armed),
            "short_break_even_armed": getattr(risk_context, "short_break_even_armed", state.short_be_armed),
            "active_long_stop_ref": getattr(risk_context, "active_long_stop_ref", None),
            "active_short_stop_ref": getattr(risk_context, "active_short_stop_ref", None),
        }

    def _persist_bar_artifacts(self, bar: Bar, features: FeaturePacket, signals: SignalPacket) -> None:
        if self._repositories is None:
            return
        self._repositories.bars.save(bar)
        self._repositories.features.save(features, created_at=bar.end_ts)
        self._repositories.signals.save(signals, created_at=bar.end_ts)

    def _persist_order_intent(
        self,
        intent: OrderIntent,
        broker_order_id: str,
        order_status: OrderStatus = OrderStatus.ACKNOWLEDGED,
        *,
        submitted_at: datetime | None = None,
        acknowledged_at: datetime | None = None,
        broker_order_status: str | None = None,
        last_status_checked_at: datetime | None = None,
        timeout_classification: str | None = None,
        retry_count: int | None = None,
    ) -> None:
        if self._repositories is None:
            return
        self._repositories.order_intents.save(
            intent,
            broker_order_id=broker_order_id,
            order_status=order_status,
            submitted_at=submitted_at,
            acknowledged_at=acknowledged_at,
            broker_order_status=broker_order_status,
            last_status_checked_at=last_status_checked_at,
            timeout_classification=timeout_classification,
            retry_count=retry_count,
        )

    def _persist_fill(self, fill: FillEvent) -> None:
        if self._repositories is None:
            return
        self._repositories.fills.save(fill)

    def _persist_state(self, state: StrategyState, transition_label: str) -> None:
        if self._state_repository is None:
            return
        self._state_repository.save_snapshot(state, transition_label=transition_label)

    def _build_reconciler(self) -> StrategyReconciler:
        return StrategyReconciler(
            repositories=self._repositories,
            structured_logger=self._structured_logger,
            alert_dispatcher=self._alert_dispatcher,
            runtime_identity=self._runtime_identity,
        )


def _next_counter(current: Optional[int], reset: bool) -> int:
    if reset:
        return 0
    return (current if current is not None else 1000) + 1


def _signal_present(signal_packet: SignalPacket) -> bool:
    return (
        signal_packet.long_entry_raw
        or signal_packet.short_entry_raw
        or signal_packet.asia_reclaim_bar_raw
        or signal_packet.bull_snap_turn_candidate
        or signal_packet.bear_snap_turn_candidate
        or signal_packet.derivative_bear_turn_candidate
    )


def _bar_matches_probationary_session_restriction(bar: Bar, restriction: str, timezone_info) -> bool:
    local_time = bar.end_ts.astimezone(timezone_info).time()
    normalized = restriction.upper()
    if "/" in normalized:
        allowed = {part.strip() for part in normalized.split("/") if part.strip()}
        coarse = _phase_coarse_session_group(label_session_phase_for_bar(bar, timezone_info))
        return coarse in allowed or label_session_phase_for_bar(bar, timezone_info) in allowed
    if normalized == "ASIA_EARLY":
        return time(18, 0) < local_time < time(20, 30)
    if normalized == "US_LATE":
        return time(14, 0) <= local_time < time(17, 0)
    return True


def _gc_mgc_asia_retest_hold_london_open_extension_matches(bar: Bar, source: str, timezone_info) -> bool:
    if str(source or "") != "asiaEarlyNormalBreakoutRetestHoldTurn":
        return False
    if str(bar.symbol or "").upper() not in {"GC", "MGC"}:
        return False
    if label_session_phase_for_bar(bar, timezone_info) != "LONDON_OPEN":
        return False
    local_time = bar.end_ts.astimezone(timezone_info).time()
    return local_time in {time(3, 5), time(3, 10), time(3, 15)}


def label_session_phase_for_bar(bar: Bar, timezone_info) -> str:
    local_time = bar.end_ts.astimezone(timezone_info).time()
    if local_time == time(18, 0):
        return "SESSION_RESET_1800"
    if time(18, 0) < local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time < time(23, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_OPEN"
    if time(5, 30) <= local_time < time(8, 30):
        return "LONDON_LATE"
    if time(9, 0) <= local_time < time(9, 30):
        return "US_PREOPEN_OPENING"
    if time(9, 30) <= local_time < time(10, 0):
        return "US_CASH_OPEN_IMPULSE"
    if time(10, 0) <= local_time < time(10, 30):
        return "US_OPEN_LATE"
    if time(10, 30) <= local_time < time(14, 0):
        return "US_MIDDAY"
    if time(14, 0) <= local_time < time(17, 0):
        return "US_LATE"
    return "UNCLASSIFIED"


def _phase_coarse_session_group(phase: str) -> str:
    normalized = str(phase or "").upper()
    if normalized.startswith("ASIA_"):
        return "ASIA"
    if normalized.startswith("LONDON_"):
        return "LONDON"
    if normalized.startswith("US_"):
        return "US"
    return "UNKNOWN"


def _empty_signal_packet_payload(bar_id: str) -> dict[str, bool | str | None]:
    return {
        "bar_id": bar_id,
        "bull_snap_downside_stretch_ok": False,
        "bull_snap_range_ok": False,
        "bull_snap_body_ok": False,
        "bull_snap_close_strong": False,
        "bull_snap_velocity_ok": False,
        "bull_snap_reversal_bar": False,
        "bull_snap_location_ok": False,
        "bull_snap_raw": False,
        "bull_snap_turn_candidate": False,
        "first_bull_snap_turn": False,
        "below_vwap_recently": False,
        "reclaim_range_ok": False,
        "reclaim_vol_ok": False,
        "reclaim_color_ok": False,
        "reclaim_close_ok": False,
        "asia_reclaim_bar_raw": False,
        "asia_hold_bar": False,
        "asia_hold_close_vwap_ok": False,
        "asia_hold_low_ok": False,
        "asia_hold_bar_ok": False,
        "asia_acceptance_bar": False,
        "asia_acceptance_close_high_ok": False,
        "asia_acceptance_close_vwap_ok": False,
        "asia_acceptance_bar_ok": False,
        "asia_vwap_long_signal": False,
        "midday_pause_resume_long_turn_candidate": False,
        "us_late_pause_resume_long_turn_candidate": False,
        "us_late_failed_move_reversal_long_turn_candidate": False,
        "us_late_breakout_retest_hold_long_turn_candidate": False,
        "asia_early_breakout_retest_hold_long_turn_candidate": False,
        "asia_early_normal_breakout_retest_hold_long_turn_candidate": False,
        "asia_late_pause_resume_long_turn_candidate": False,
        "asia_late_flat_pullback_pause_resume_long_turn_candidate": False,
        "asia_late_compressed_flat_pullback_pause_resume_long_turn_candidate": False,
        "bear_snap_up_stretch_ok": False,
        "bear_snap_range_ok": False,
        "bear_snap_body_ok": False,
        "bear_snap_close_weak": False,
        "bear_snap_velocity_ok": False,
        "bear_snap_reversal_bar": False,
        "bear_snap_location_ok": False,
        "bear_snap_raw": False,
        "bear_snap_turn_candidate": False,
        "first_bear_snap_turn": False,
        "derivative_bear_slope_ok": False,
        "derivative_bear_curvature_ok": False,
        "derivative_bear_turn_candidate": False,
        "derivative_bear_additive_turn_candidate": False,
        "midday_compressed_failed_move_reversal_short_turn_candidate": False,
        "midday_compressed_rebound_failed_move_reversal_short_turn_candidate": False,
        "midday_expanded_pause_resume_short_turn_candidate": False,
        "midday_compressed_pause_resume_short_turn_candidate": False,
        "midday_pause_resume_short_turn_candidate": False,
        "london_late_pause_resume_short_turn_candidate": False,
        "asia_early_expanded_breakout_retest_hold_short_turn_candidate": False,
        "asia_early_compressed_pause_resume_short_turn_candidate": False,
        "asia_early_pause_resume_short_turn_candidate": False,
        "long_entry_raw": False,
        "short_entry_raw": False,
        "recent_long_setup": False,
        "recent_short_setup": False,
        "long_entry": False,
        "short_entry": False,
        "long_entry_source": None,
        "short_entry_source": None,
    }
