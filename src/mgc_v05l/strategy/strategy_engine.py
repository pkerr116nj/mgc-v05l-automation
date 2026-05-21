"""Strategy engine orchestration for deterministic bar-close execution."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime, time, timezone
from decimal import Decimal
from typing import Callable, Optional

from ..config_models import ExecutionTimeframeRole, StrategySettings
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
from ..execution_core.track_b_no_trade_diagnostics import (
    NoTradeFinalDecision,
    build_no_trade_diagnostic,
    diagnostics_root_from_artifact_dir,
    write_no_trade_diagnostic,
)
from ..execution_core.track_b_position_management_manifest import (
    DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    create_manifest_from_order_intent,
    resolve_management_metadata,
    update_manifest_from_filled_bridge_result,
)
from ..indicators.feature_engine import IncrementalFeatureComputer, compute_features
from ..market_data.bar_builder import BarBuilder
from ..market_data.bar_store import BarStore
from ..market_data.timeframes import timeframe_minutes
from ..market_data.session_clock import classify_sessions
from ..monitoring.alerts import AlertDispatcher
from ..monitoring.logger import StructuredLogger
from ..persistence.repositories import RepositorySet
from ..persistence.state_repository import StateRepository
from ..research.bar_resampling import build_resampled_bars
from ..signals.asia_vwap_reclaim import evaluate_asia_vwap_reclaim
from ..signals.bear_snap import evaluate_bear_snap
from ..signals.bull_snap import evaluate_bull_snap
from ..signals.entry_resolver import resolve_entries
from ..app.session_phase_labels import label_session_phase, phase_coarse_session_group, session_restriction_matches_timestamp
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


def _intent_side(intent: OrderIntent) -> str:
    if intent.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.BUY_TO_CLOSE):
        return "BUY"
    return "SELL"


def _broker_status_is_filled(status: object) -> bool:
    return str(status or "").strip().upper() in {OrderStatus.FILLED.value, "FILLED"}


def _parse_fill_price(value: object) -> Decimal | None:
    if value in (None, ""):
        return None
    return Decimal(str(value))


def _parse_fill_timestamp(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _blocked_intent_classification(reason: str, submit_attempt: dict[str, object]) -> str:
    gate_trace = [dict(row) for row in list(submit_attempt.get("bridge_gate_trace") or []) if isinstance(row, dict)]
    failed_gate_text = " ".join(
        [
            json.dumps(
                {
                    "name": row.get("name"),
                    "detail": row.get("detail"),
                    "passed": row.get("passed"),
                },
                sort_keys=True,
                default=str,
            )
            for row in gate_trace
            if row.get("passed") is False
        ]
    ).lower()
    text = " ".join(
        [
            str(reason or ""),
            str(submit_attempt.get("bridge_detail") or ""),
            failed_gate_text,
        ]
    ).lower()
    if (
        "broker_position_truth_stale_or_missing" in text
        or "broker truth" in text
        or ("broker position" in text and "truth" in text)
    ):
        return "BROKER_TRUTH_STALE_OR_MISSING"
    if "paper_strategy_monitor_running" in failed_gate_text or "monitor_not_running" in text or "not running" in text or "health_stopped" in text or "stopped" in text:
        return "PAPER_MONITOR_NOT_HEALTHY"
    if (
        "stale" in failed_gate_text
        or "broker_refresh" in failed_gate_text
        or "last_successful_broker_refresh" in failed_gate_text
        or (not failed_gate_text and ("stale" in text or "broker_refresh" in text or "last_successful_broker_refresh" in text))
    ):
        return "ROUTE_HEALTH_STALE"
    if "bridge_allowed=false" in text or "bridge_allowed" in failed_gate_text:
        return "BRIDGE_AUTHORITY_BLOCKED"
    if "paper_strategy_exposure_gate" in failed_gate_text or "owning strategy" in text or "attributed exposure" in text:
        return "BRIDGE_EXPOSURE_GATE_BLOCKED"
    if "account" in failed_gate_text:
        return "ACCOUNT_MISMATCH"
    if "contract" in failed_gate_text or "local_symbol" in failed_gate_text or "expiry" in failed_gate_text or "execution target" in failed_gate_text:
        return "EXECUTION_TARGET_MISMATCH"
    if "adapter" in failed_gate_text or "allowlist" in failed_gate_text or "not enabled" in text or "unsupported" in text:
        return "EXECUTION_TARGET_NOT_ENABLED"
    return "PRE_SUBMIT_GATE_BLOCKED"


def _blocked_intent_route_target(submit_attempt: dict[str, object], intent: OrderIntent) -> dict[str, object]:
    metadata = dict(submit_attempt.get("caller_metadata") or {})
    return {
        "source_instrument": metadata.get("source_instrument") or submit_attempt.get("source_symbol") or intent.symbol,
        "execution_symbol": submit_attempt.get("bridge_symbol") or metadata.get("executable_proxy") or intent.symbol,
        "contract_month": submit_attempt.get("bridge_contract_month"),
        "action": submit_attempt.get("bridge_action"),
        "route_destination": submit_attempt.get("route_destination"),
        "bridge_proxy_mode": submit_attempt.get("bridge_proxy_mode"),
        "caller_path": submit_attempt.get("caller_path"),
    }


def _blocked_intent_monitor_snapshot(submit_attempt: dict[str, object], reason: str) -> dict[str, object]:
    checks = [dict(row) for row in list(submit_attempt.get("bridge_gate_trace") or []) if isinstance(row, dict)]
    by_name = {str(row.get("name") or row.get("gate") or ""): row for row in checks}

    def _detail(*names: str) -> str | None:
        for name in names:
            row = by_name.get(name)
            if row:
                return str(row.get("detail") or row.get("message") or "")
        return None

    text = f"{reason} {json.dumps(checks, sort_keys=True, default=str)}"
    return {
        "monitor_running": _gate_passed(by_name.get("paper_strategy_monitor_running"), text, "monitor_running"),
        "health_classification": _extract_token(text, "health_classification")
        or _extract_health_from_detail(_detail("paper_strategy_monitor_health")),
        "bridge_allowed": _extract_bool_token(text, "bridge_allowed"),
        "broker_refresh_timestamp": _extract_token(text, "last_successful_broker_refresh")
        or _extract_token(text, "broker_refresh_timestamp"),
        "broker_refresh_freshness": "STALE" if "stale" in text.lower() else None,
        "account": _extract_token(text, "account_id") or _extract_token(text, "account"),
        "contract": {
            "symbol": submit_attempt.get("bridge_symbol"),
            "contract_month": submit_attempt.get("bridge_contract_month"),
            "detail": _detail("paper_strategy_monitor_contract_match", "executable_contract_whitelist"),
        },
    }


def _gate_passed(row: dict[str, object] | None, text: str, field_name: str) -> bool | None:
    if row is not None and "passed" in row:
        return bool(row.get("passed"))
    return _extract_bool_token(text, field_name)


def _extract_bool_token(text: str, key: str) -> bool | None:
    lowered = text.lower()
    key_lower = key.lower()
    for separator in ("=", ":"):
        token = f"{key_lower}{separator}"
        if token in lowered:
            tail = lowered.split(token, 1)[1].strip()
            if tail.startswith("true"):
                return True
            if tail.startswith("false"):
                return False
    return None


def _extract_token(text: str, key: str) -> str | None:
    for separator in ("=", ":"):
        marker = f"{key}{separator}"
        if marker in text:
            raw = text.split(marker, 1)[1].strip()
            token = raw.split()[0].strip(" ,.;'\"{}[]")
            return token or None
    return None


def _extract_health_from_detail(detail: str | None) -> str | None:
    if not detail:
        return None
    upper = detail.upper()
    for value in ("HEALTHY", "STOPPED", "STALE", "UNKNOWN", "DEGRADED"):
        if value in upper:
            return value
    return None


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
        route_hold_evaluator: Optional[Callable[[Bar, StrategyState, OrderIntent], str | None]] = None,
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
        self._route_hold_evaluator = route_hold_evaluator
        self._use_incremental_features = use_incremental_features
        self._incremental_feature_computer = (
            IncrementalFeatureComputer(settings) if use_incremental_features else None
        )
        self._execution_timeframe = settings.resolved_execution_timeframe
        self._context_timeframes = settings.resolved_context_timeframes
        self._primary_context_timeframe = settings.primary_context_timeframe
        self._uses_multi_timescale_execution = settings.uses_multi_timescale_execution
        self._bar_builder = BarBuilder(settings)
        self._state = self._load_initial_state(initial_state)
        self._execution_bar_history: list[Bar] = []
        self._context_bar_histories: dict[str, list[Bar]] = {
            timeframe: [] for timeframe in self._context_timeframes
        }
        self._context_feature_histories: dict[str, list[FeaturePacket]] = {
            timeframe: [] for timeframe in self._context_timeframes
        }
        self._latest_context_signal_packet: Optional[SignalPacket] = None
        self._last_completed_context_bar_end_by_timeframe: dict[str, datetime | None] = {
            timeframe: None for timeframe in self._context_timeframes
        }
        self._last_execution_bar_evaluated_at: Optional[datetime] = None
        self._last_execution_bar_id: Optional[str] = None
        self._bar_history: list[Bar] = []
        self._feature_history: list[FeaturePacket] = []
        self._last_signal_packet: Optional[SignalPacket] = None
        self._last_feature_packet: Optional[FeaturePacket] = None
        self._last_exit_decision_summary: dict[str, object] = {}
        self._latest_shadow_intent_summary: dict[str, object] = {}
        self._latest_live_intent_summary: dict[str, object] = {}
        self._no_trade_diagnostics_root = diagnostics_root_from_artifact_dir(
            structured_logger.artifact_dir if structured_logger is not None else None
        )
        self._restore_processing_context()

    def set_route_hold_evaluator(
        self,
        evaluator: Callable[[Bar, StrategyState, OrderIntent], str | None] | None,
    ) -> None:
        self._route_hold_evaluator = evaluator

    def process_bar(self, bar: Bar) -> list[DomainEvent]:
        """Process a single completed bar and return emitted domain events."""
        if not bar.is_final:
            return []

        events: list[DomainEvent] = []
        events.extend(self._apply_due_replay_fills(bar))

        execution_bar = classify_sessions(bar, self._settings)
        if not self._bar_store.validate_next_bar(execution_bar):
            return events

        events.append(BarClosedEvent(bar_id=execution_bar.bar_id, occurred_at=execution_bar.end_ts))
        self._execution_bar_history.append(execution_bar)
        self._trim_execution_history()

        context_advanced = self._refresh_context_histories()
        current_context_bar = self._bar_history[-1] if self._bar_history else None
        current_context_feature = self._feature_history[-1] if self._feature_history else None

        if current_context_feature is not None:
            if context_advanced or self._latest_context_signal_packet is None:
                self._latest_context_signal_packet = self._evaluate_signals(
                    current_context_feature,
                    self._feature_history,
                )
            signal_packet = self._signal_packet_for_execution_bar(execution_bar)
            signal_packet = self._apply_runtime_entry_controls(execution_bar, signal_packet)
        else:
            signal_packet = self._empty_execution_signal_packet(execution_bar.bar_id)

        working_state = self._state
        if context_advanced and current_context_feature is not None and self._latest_context_signal_packet is not None:
            working_state = self._advance_state_for_bar(
                current_context_feature,
                self._latest_context_signal_packet,
                current_context_bar.end_ts if current_context_bar is not None else execution_bar.end_ts,
            )
            if working_state.position_side != PositionSide.FLAT and current_context_bar is not None:
                working_state = increment_bars_in_trade(working_state, current_context_bar.end_ts)
        else:
            working_state = replace(working_state, updated_at=execution_bar.end_ts)

        feature_packet = self._feature_packet_for_execution_bar(execution_bar, current_context_feature)
        if current_context_feature is None or not self._bar_history:
            self._last_execution_bar_id = execution_bar.bar_id
            self._last_execution_bar_evaluated_at = execution_bar.end_ts
            self._emit_no_trade_diagnostic(
                bar=execution_bar,
                signal_packet=signal_packet,
                strategy_evaluated=False,
                setup_detected=False,
                blocker_reason="context_feature_history_not_ready",
                final_decision=NoTradeFinalDecision.NO_SETUP,
            )
            self._bar_store.mark_processed(execution_bar)
            self._persist_bar_artifacts(execution_bar, feature_packet, signal_packet)
            self._last_feature_packet = feature_packet
            self._last_signal_packet = signal_packet
            self._state = replace(working_state, updated_at=execution_bar.end_ts)
            self._persist_state(self._state, transition_label="bar_close")
            return events
        risk_context = compute_risk_context(self._bar_history, feature_packet, working_state, self._settings)
        working_state = replace(
            working_state,
            long_be_armed=risk_context.long_break_even_armed,
            short_be_armed=risk_context.short_break_even_armed,
            updated_at=execution_bar.end_ts,
        )
        working_state = update_additive_short_peak_state(
            working_state,
            execution_bar,
            risk_context,
            self._settings,
            execution_bar.end_ts,
        )

        exit_decision = evaluate_exits(self._bar_history, feature_packet, working_state, risk_context, self._settings)
        if working_state.position_side != PositionSide.FLAT:
            self._last_exit_decision_summary = self._build_exit_decision_summary(
                bar=execution_bar,
                state=working_state,
                exit_decision=exit_decision,
                risk_context=risk_context,
                exit_fill_pending=False,
                exit_fill_confirmed=False,
            )
            events.append(
                ExitEvaluatedEvent(
                    bar_id=execution_bar.bar_id,
                    primary_reason=exit_decision.primary_reason,
                    occurred_at=execution_bar.end_ts,
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

        diagnostic_blocker_reason: str | None = None
        diagnostic_final_decision: NoTradeFinalDecision | None = None
        diagnostic_order_intent_created = False
        diagnostic_would_route = False
        diagnostic_order_intent_id: str | None = None
        diagnostic_submit_blocker: str | None = None
        diagnostic_setup_detected_override: bool | None = None
        diagnostic_extra: dict[str, object] = {}

        violations = validate_state(working_state)
        if violations:
            fault_code = "; ".join(violations)
            diagnostic_blocker_reason = f"strategy_invariant_fault: {fault_code}"
            diagnostic_final_decision = NoTradeFinalDecision.FILTER_REJECTED
            working_state = transition_to_fault(working_state, execution_bar.end_ts, fault_code)
            events.append(FaultRaisedEvent(fault_code=fault_code, occurred_at=execution_bar.end_ts))
            if self._alert_dispatcher is not None:
                self._alert_dispatcher.emit(
                    "error",
                    "strategy_invariant_fault",
                    fault_code,
                    {"bar_id": execution_bar.bar_id},
                )
            self._persist_state(working_state, transition_label="fault")
        else:
            maybe_intent = self._maybe_create_order_intent(execution_bar, signal_packet, working_state, exit_decision)
            if maybe_intent is not None:
                diagnostic_order_intent_id = maybe_intent.order_intent_id
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
                        bar=execution_bar,
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
                            bar_id=execution_bar.bar_id,
                            intent_type=maybe_intent.intent_type,
                            occurred_at=execution_bar.end_ts,
                        )
                    )
                    self._emit_shadow_submit_suppressed_alert(maybe_intent, execution_bar.end_ts)
                    diagnostic_order_intent_created = True
                else:
                    live_intent_summary = self._build_live_intent_summary(
                        bar=execution_bar,
                        state=working_state,
                        signal_packet=signal_packet,
                        exit_decision=exit_decision,
                        risk_context=risk_context,
                        intent=maybe_intent,
                        long_entry_family=long_entry_family,
                        short_entry_family=short_entry_family,
                        short_entry_source=short_entry_source,
                    )
                    position_manifest_blocker: str | None = None
                    position_manifest = None
                    if maybe_intent.is_entry:
                        position_manifest = create_manifest_from_order_intent(
                            order_intent=maybe_intent,
                            runtime_identity=self._runtime_identity,
                            output_root=DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
                            now=execution_bar.end_ts,
                        )
                        live_intent_summary = {
                            **live_intent_summary,
                            "position_management_manifest_path": str(position_manifest.manifest_path)
                            if position_manifest is not None
                            else None,
                            "managed_exit_policy_id": None
                            if position_manifest is None
                            else position_manifest.manifest.get("managed_exit_policy_id"),
                        }
                        if not (position_manifest and position_manifest.manifest.get("managed_exit_policy_id")):
                            position_manifest_blocker = (
                                f"{OPEN_MANAGED_METADATA_INCOMPLETE}: missing managed_exit_policy_id"
                            )
                    submit_attempt_was_executed = False
                    pending = None
                    route_hold_blocker = (
                        self._route_hold_evaluator(execution_bar, working_state, maybe_intent)
                        if self._route_hold_evaluator is not None
                        else None
                    )
                    if route_hold_blocker is not None:
                        diagnostic_blocker_reason = route_hold_blocker
                        diagnostic_setup_detected_override = True
                        diagnostic_final_decision = (
                            NoTradeFinalDecision.STARTUP_CATCHUP_DIAGNOSTIC_ONLY
                            if str(route_hold_blocker).startswith("STARTUP_CATCHUP_DIAGNOSTIC_ONLY")
                            else NoTradeFinalDecision.STARTUP_CATCHUP_NOT_ROUTABLE
                            if str(route_hold_blocker).startswith("STARTUP_CATCHUP_NOT_ROUTABLE")
                            else NoTradeFinalDecision.ROUTE_HELD_UNTIL_READINESS_CONVERGED
                        )
                        diagnostic_extra.update(
                            {
                                "startup_route_hold_blocker": route_hold_blocker,
                                "route_held_until_readiness_converged": True,
                                "intended_intent_type": maybe_intent.intent_type.value,
                                "intended_action": maybe_intent.intent_type.value,
                                "intended_quantity": maybe_intent.quantity,
                                "intended_reason_code": maybe_intent.reason_code,
                                "intended_signal_id": maybe_intent.signal_id,
                            }
                        )
                        self._latest_live_intent_summary = {
                            **live_intent_summary,
                            "submit_attempt_id": self._pre_submit_attempt_id(maybe_intent, execution_bar.end_ts),
                            "startup_route_hold_blocker": route_hold_blocker,
                            "submit_gate_blocker": None,
                            "submit_attempted": False,
                            "submit_suppressed": True,
                            "route_held_until_readiness_converged": True,
                        }
                    else:
                        submit_blocker = (
                            self._submit_gate_evaluator(execution_bar, working_state, maybe_intent)
                            if self._submit_gate_evaluator is not None
                            else None
                        )
                        submit_blocker = position_manifest_blocker or submit_blocker
                        if submit_blocker is not None:
                            diagnostic_submit_blocker = submit_blocker
                            diagnostic_blocker_reason = submit_blocker
                            diagnostic_final_decision = NoTradeFinalDecision.GOVERNANCE_BLOCKED
                            self._latest_live_intent_summary = {
                                **live_intent_summary,
                                "submit_attempt_id": self._pre_submit_attempt_id(maybe_intent, execution_bar.end_ts),
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
                            blocked_payload = self._persist_blocked_strategy_intent(
                                maybe_intent,
                                occurred_at=execution_bar.end_ts,
                                reason=submit_blocker,
                                submit_attempt_id=self._pre_submit_attempt_id(maybe_intent, execution_bar.end_ts),
                            )
                            self._emit_order_rejection_alert(
                                maybe_intent,
                                execution_bar.end_ts,
                                reason=submit_blocker,
                                submit_attempt_id=self._pre_submit_attempt_id(maybe_intent, execution_bar.end_ts),
                            )
                            self._latest_live_intent_summary = {
                                **self._latest_live_intent_summary,
                                "blocked_strategy_intent": blocked_payload,
                            }
                        else:
                            pending = self._execution_engine.submit_intent(
                                maybe_intent,
                                signal_bar_id=execution_bar.bar_id if maybe_intent.is_entry else None,
                                long_entry_family=long_entry_family,
                                short_entry_family=short_entry_family,
                                short_entry_source=short_entry_source,
                            )
                            submit_attempt_was_executed = True
                        if submit_attempt_was_executed and pending is not None:
                            diagnostic_order_intent_created = True
                            self._latest_live_intent_summary = {
                                **self._latest_live_intent_summary,
                                "submit_attempt_id": pending.submit_attempt_id,
                                "submit_attempted_at": pending.submitted_at.isoformat(),
                                "broker_order_id": pending.broker_order_id,
                                "broker_ack_at": pending.acknowledged_at.isoformat() if pending.acknowledged_at is not None else None,
                                "broker_order_status": pending.broker_order_status,
                            }
                            events.append(
                                OrderIntentCreatedEvent(
                                    order_intent_id=maybe_intent.order_intent_id,
                                    bar_id=execution_bar.bar_id,
                                    intent_type=maybe_intent.intent_type,
                                    occurred_at=execution_bar.end_ts,
                                )
                            )
                            if _broker_status_is_filled(pending.broker_order_status):
                                try:
                                    fill_event = self._broker_fill_event_from_pending(pending)
                                    self._persist_order_intent(
                                        maybe_intent,
                                        pending.broker_order_id,
                                        order_status=OrderStatus.FILLED,
                                        submitted_at=pending.submitted_at,
                                        acknowledged_at=pending.acknowledged_at,
                                        broker_order_status=OrderStatus.FILLED.value,
                                        last_status_checked_at=pending.last_status_checked_at,
                                        retry_count=pending.retry_count,
                                    )
                                    self.apply_fill(
                                        fill_event=fill_event,
                                        signal_bar_id=pending.signal_bar_id,
                                        long_entry_family=pending.long_entry_family,
                                        short_entry_family=pending.short_entry_family,
                                        short_entry_source=pending.short_entry_source,
                                    )
                                    self._execution_engine.clear_intent(pending.intent.order_intent_id)
                                    working_state = self._state
                                    self._persist_filled_bridge_result(
                                        pending=pending,
                                        fill_event=fill_event,
                                        classification="PAPER_STRATEGY_ORDER_FILLED_PERSISTED",
                                        review_required=False,
                                    )
                                    events.append(
                                        FillReceivedEvent(
                                            order_intent_id=fill_event.order_intent_id,
                                            broker_order_id=fill_event.broker_order_id,
                                            fill_timestamp=fill_event.fill_timestamp,
                                            fill_price=fill_event.fill_price,
                                        )
                                    )
                                except Exception as exc:
                                    self._persist_order_intent(
                                        maybe_intent,
                                        pending.broker_order_id,
                                        order_status=OrderStatus.FILLED,
                                        submitted_at=pending.submitted_at,
                                        acknowledged_at=pending.acknowledged_at,
                                        broker_order_status=OrderStatus.FILLED.value,
                                        last_status_checked_at=pending.last_status_checked_at,
                                        timeout_classification="REVIEW_REQUIRED_FILLED_BUT_PERSISTENCE_INCOMPLETE",
                                        timeout_status_updated_at=execution_bar.end_ts,
                                        retry_count=pending.retry_count,
                                    )
                                    working_state = transition_to_fault(
                                        replace(working_state, entries_enabled=False, updated_at=execution_bar.end_ts),
                                        execution_bar.end_ts,
                                        f"REVIEW_REQUIRED_FILLED_BUT_PERSISTENCE_INCOMPLETE: {exc}",
                                    )
                                    self._state = working_state
                                    self._execution_engine.clear_intent(pending.intent.order_intent_id)
                                    self._persist_state(working_state, transition_label="filled_bridge_persistence_review_required")
                                    self._persist_filled_bridge_result(
                                        pending=pending,
                                        fill_event=None,
                                        classification="REVIEW_REQUIRED_FILLED_BUT_PERSISTENCE_INCOMPLETE",
                                        review_required=True,
                                        error=str(exc),
                                    )
                            else:
                                working_state = replace(
                                    working_state,
                                    last_order_intent_id=maybe_intent.order_intent_id,
                                    open_broker_order_id=pending.broker_order_id,
                                    updated_at=execution_bar.end_ts,
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
                                self._emit_order_lifecycle_alert(
                                    "created",
                                    maybe_intent,
                                    execution_bar.end_ts,
                                    pending_broker_order_id=pending.broker_order_id,
                                    submit_attempt_id=pending.submit_attempt_id,
                                )
                                self._emit_order_lifecycle_alert(
                                    "submitted",
                                    maybe_intent,
                                    execution_bar.end_ts,
                                    pending_broker_order_id=pending.broker_order_id,
                                    submit_attempt_id=pending.submit_attempt_id,
                                )
                        elif submit_attempt_was_executed:
                            failure = self._execution_engine.last_submit_failure()
                            diagnostic_blocker_reason = (
                                failure.error
                                if failure is not None and failure.order_intent_id == maybe_intent.order_intent_id
                                else "Execution engine rejected the intent due to an existing pending or opposite-side conflict."
                            )
                            diagnostic_final_decision = NoTradeFinalDecision.EXPOSURE_BLOCKED
                            self._latest_live_intent_summary = {
                                **self._latest_live_intent_summary,
                                "submit_failure": {
                                    "submit_attempt_id": failure.submit_attempt_id,
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
                                occurred_at=execution_bar.end_ts,
                                default_reason="Execution engine rejected the intent due to an existing pending or opposite-side conflict.",
                            )
            elif diagnostic_blocker_reason is None:
                diagnostic_blocker_reason = self._infer_no_trade_blocker_reason(
                    bar=execution_bar,
                    signal_packet=signal_packet,
                    state=working_state,
                    exit_decision=exit_decision,
                )

        self._last_execution_bar_id = execution_bar.bar_id
        self._last_execution_bar_evaluated_at = execution_bar.end_ts
        self._emit_no_trade_diagnostic(
            bar=execution_bar,
            signal_packet=signal_packet,
            strategy_evaluated=True,
            setup_detected=(
                diagnostic_setup_detected_override
                if diagnostic_setup_detected_override is not None
                else _signal_present(signal_packet)
            ),
            blocker_reason=diagnostic_blocker_reason,
            submit_blocker=diagnostic_submit_blocker,
            final_decision=diagnostic_final_decision,
            order_intent_created=diagnostic_order_intent_created,
            would_route=diagnostic_would_route,
            order_intent_id=diagnostic_order_intent_id,
            extra={
                "position_side": working_state.position_side.value,
                "entries_enabled": working_state.entries_enabled,
                "exits_enabled": working_state.exits_enabled,
                "operator_halt": working_state.operator_halt,
                "same_underlying_entry_hold": working_state.same_underlying_entry_hold,
                "long_entry_source": signal_packet.long_entry_source,
                "short_entry_source": signal_packet.short_entry_source,
                **diagnostic_extra,
            },
        )
        self._bar_store.mark_processed(execution_bar)
        self._persist_bar_artifacts(execution_bar, feature_packet, signal_packet)
        self._last_feature_packet = feature_packet
        self._last_signal_packet = signal_packet
        self._state = working_state
        self._persist_state(self._state, transition_label="bar_close")
        return events

    def _emit_no_trade_diagnostic(
        self,
        *,
        bar: Bar,
        signal_packet: SignalPacket,
        strategy_evaluated: bool,
        setup_detected: bool,
        blocker_reason: str | None = None,
        submit_blocker: str | None = None,
        final_decision: NoTradeFinalDecision | None = None,
        order_intent_created: bool = False,
        would_route: bool = False,
        order_intent_id: str | None = None,
        extra: dict[str, object] | None = None,
    ) -> None:
        try:
            payload = build_no_trade_diagnostic(
                lane_id=str(self._runtime_identity.get("lane_id") or self._settings.probationary_paper_lane_id or ""),
                symbol=self._settings.symbol,
                session=label_session_phase(bar.end_ts),
                bar_timestamp=bar.end_ts,
                bar_id=bar.bar_id,
                session_allowed=bar.session_allowed,
                market_data_fresh=True,
                strategy_evaluated=strategy_evaluated,
                signal_packet=signal_packet,
                setup_detected=setup_detected,
                blocker_reason=blocker_reason,
                submit_blocker=submit_blocker,
                final_decision=final_decision,
                order_intent_created=order_intent_created,
                would_route=would_route,
                order_intent_id=order_intent_id,
                runtime_identity=self._runtime_identity,
                extra=extra,
            )
            write_no_trade_diagnostic(payload, diagnostics_root=self._no_trade_diagnostics_root)
        except Exception as exc:
            if self._alert_dispatcher is not None:
                self._alert_dispatcher.emit(
                    "warning",
                    "no_trade_diagnostic_write_failed",
                    str(exc),
                    {"bar_id": bar.bar_id, "lane_id": self._runtime_identity.get("lane_id")},
                )

    def _infer_no_trade_blocker_reason(
        self,
        *,
        bar: Bar,
        signal_packet: SignalPacket,
        state: StrategyState,
        exit_decision: ExitDecision,
    ) -> str | None:
        if not bar.session_allowed:
            return "session_not_allowed"
        if state.operator_halt:
            return "operator_halt"
        if state.position_side == PositionSide.FLAT:
            if not state.entries_enabled:
                return "entries_disabled"
            if state.same_underlying_entry_hold and (signal_packet.long_entry or signal_packet.short_entry):
                return str(state.same_underlying_hold_reason or "same_underlying_entry_hold")
            if len(self._bar_history) < self._settings.warmup_bars_required():
                return "warmup_incomplete"
            if signal_packet.long_entry and not self._entry_side_is_currently_allowed("LONG", state):
                return "long_entry_side_not_allowed"
            if signal_packet.short_entry and not self._entry_side_is_currently_allowed("SHORT", state):
                return "short_entry_side_not_allowed"
            if _signal_present(signal_packet):
                return "entry_signal_filtered_or_controls_not_satisfied"
            return "no_setup_detected"
        if not state.exits_enabled:
            return "exits_disabled"
        if state.position_side == PositionSide.LONG and not exit_decision.long_exit:
            return "long_position_exit_not_signaled"
        if state.position_side == PositionSide.SHORT and not exit_decision.short_exit:
            return "short_position_exit_not_signaled"
        return None

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
        self._state = transition_to_fault(
            replace(self._state, entries_enabled=False, updated_at=occurred_at),
            occurred_at,
            fault_code,
        )
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
            signal_id=f"{bar_id}|signal|RUNTIME_EXIT|{reason_code}",
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
            "signal_id": intent.signal_id,
            "order_intent_id": intent.order_intent_id,
            "strategy_trade_id": intent.order_intent_id,
            "intent_type": intent.intent_type.value,
            "quantity": intent.quantity,
            "reason_code": intent.reason_code,
            "signal_source": signal_source,
            "submit_attempted_at": occurred_at.isoformat(),
            "submit_attempted": True,
            "submit_suppressed": False,
            "submit_gate_blocker": None,
            "submit_attempt_id": pending.submit_attempt_id,
            "resulting_position_side": self._state.position_side.value,
            "resulting_internal_qty": self._state.internal_position_qty,
        }
        self._emit_order_lifecycle_alert(
            "created",
            intent,
            occurred_at,
            pending_broker_order_id=pending.broker_order_id,
            submit_attempt_id=pending.submit_attempt_id,
        )
        self._emit_order_lifecycle_alert(
            "submitted",
            intent,
            occurred_at,
            pending_broker_order_id=pending.broker_order_id,
            submit_attempt_id=pending.submit_attempt_id,
        )
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
            signal_id=self._signal_id_for_actionable_signal(bar, normalized_side, signal_source),
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
        self._emit_order_lifecycle_alert(
            "created",
            intent,
            bar.end_ts,
            pending_broker_order_id=pending.broker_order_id,
            submit_attempt_id=pending.submit_attempt_id,
        )
        self._emit_order_lifecycle_alert(
            "submitted",
            intent,
            bar.end_ts,
            pending_broker_order_id=pending.broker_order_id,
            submit_attempt_id=pending.submit_attempt_id,
        )
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

    def runtime_cadence_snapshot(self) -> dict[str, object]:
        return {
            "execution_timeframe": self._execution_timeframe,
            "context_timeframes": list(self._context_timeframes),
            "last_execution_bar_id": self._last_execution_bar_id,
            "last_execution_bar_evaluated_at": (
                self._last_execution_bar_evaluated_at.isoformat()
                if self._last_execution_bar_evaluated_at is not None
                else None
            ),
            "last_completed_context_bars_at": {
                timeframe: value.isoformat() if value is not None else None
                for timeframe, value in self._last_completed_context_bar_end_by_timeframe.items()
            },
            "primary_context_timeframe": self._primary_context_timeframe,
            "uses_multi_timescale_execution": self._uses_multi_timescale_execution,
        }

    def _trim_execution_history(self) -> None:
        max_context_minutes = max(timeframe_minutes(timeframe) for timeframe in self._context_timeframes)
        keep_count = max(
            self._settings.warmup_bars_required() * max(1, max_context_minutes // timeframe_minutes(self._execution_timeframe)) + 8,
            120,
        )
        if len(self._execution_bar_history) > keep_count:
            self._execution_bar_history = self._execution_bar_history[-keep_count:]

    def _settings_for_timeframe(self, timeframe: str) -> StrategySettings:
        return self._settings.model_copy(
            update={
                "timeframe": timeframe,
                "structural_signal_timeframe": timeframe,
                "execution_timeframe": timeframe,
                "artifact_timeframe": timeframe,
                "context_timeframes": (timeframe,),
                "execution_timeframe_role": ExecutionTimeframeRole.MATCHES_SIGNAL_EVALUATION,
            }
        )

    def _rebuild_feature_history(self, bars: list[Bar], timeframe: str) -> list[FeaturePacket]:
        if not bars:
            return []
        feature_settings = self._settings_for_timeframe(timeframe)
        feature_computer = IncrementalFeatureComputer(feature_settings)
        swing_state = replace(self._state)
        packets: list[FeaturePacket] = []
        for bar in bars:
            packet = feature_computer.compute_next(bar, swing_state)
            packets.append(packet)
            swing_state = replace(
                swing_state,
                last_swing_low=packet.last_swing_low,
                last_swing_high=packet.last_swing_high,
            )
        return packets

    def _persist_context_artifacts(self, timeframe: str, bars: list[Bar], feature_history: list[FeaturePacket]) -> None:
        if self._repositories is None:
            return
        data_source = f"runtime_resampled_{self._execution_timeframe}_to_{timeframe}"
        known_feature_ids = {packet.bar_id for packet in feature_history}
        for bar in bars:
            self._repositories.bars.save(bar, data_source=data_source)
            if bar.bar_id in known_feature_ids:
                packet = next(packet for packet in feature_history if packet.bar_id == bar.bar_id)
                self._repositories.features.save(packet, created_at=bar.end_ts)

    def _refresh_context_histories(self) -> bool:
        if not self._execution_bar_history:
            return False
        context_advanced = False
        for timeframe in self._context_timeframes:
            if timeframe == self._execution_timeframe:
                bars = list(self._execution_bar_history)
            else:
                resampled = build_resampled_bars(
                    self._execution_bar_history,
                    target_timeframe=timeframe,
                    bar_builder=self._bar_builder,
                    alignment=(
                        "rolling"
                        if self._settings.execution_timeframe_role is ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY
                        else "bucket"
                    ),
                )
                bars = list(resampled.bars)
            previous_latest = self._context_bar_histories.get(timeframe, [])[-1].bar_id if self._context_bar_histories.get(timeframe) else None
            self._context_bar_histories[timeframe] = bars
            self._context_feature_histories[timeframe] = self._rebuild_feature_history(bars, timeframe)
            if bars:
                self._last_completed_context_bar_end_by_timeframe[timeframe] = bars[-1].end_ts
                if bars[-1].bar_id != previous_latest:
                    context_advanced = context_advanced or timeframe == self._primary_context_timeframe
                self._persist_context_artifacts(timeframe, bars, self._context_feature_histories[timeframe])
            else:
                self._last_completed_context_bar_end_by_timeframe[timeframe] = None
        self._bar_history = list(self._context_bar_histories.get(self._primary_context_timeframe, []))
        self._feature_history = list(self._context_feature_histories.get(self._primary_context_timeframe, []))
        if self._feature_history:
            self._last_feature_packet = self._feature_history[-1]
        return context_advanced

    def _feature_packet_for_execution_bar(
        self,
        execution_bar: Bar,
        current_context_feature: FeaturePacket | None,
    ) -> FeaturePacket:
        if current_context_feature is None:
            return compute_features([execution_bar], self._state, self._settings_for_timeframe(self._execution_timeframe))
        return replace(current_context_feature, bar_id=execution_bar.bar_id)

    def _signal_packet_for_execution_bar(self, execution_bar: Bar) -> SignalPacket:
        if self._latest_context_signal_packet is None:
            return self._empty_execution_signal_packet(execution_bar.bar_id)
        return replace(self._latest_context_signal_packet, bar_id=execution_bar.bar_id)

    def _empty_execution_signal_packet(self, bar_id: str) -> SignalPacket:
        return SignalPacket(**_empty_signal_packet_payload(bar_id))

    def _restore_processing_context(self) -> None:
        if self._repositories is None:
            return
        restore_limit = self._settings.warmup_bars_required()
        execution_restore_limit = max(
            restore_limit * max(1, timeframe_minutes(self._primary_context_timeframe) // timeframe_minutes(self._execution_timeframe)) + 8,
            120,
        )
        self._execution_bar_history = self._repositories.bars.list_recent_processed(
            symbol=self._settings.symbol,
            timeframe=self._execution_timeframe,
            limit=execution_restore_limit,
        )
        if not self._execution_bar_history:
            return
        for timeframe in self._context_timeframes:
            if timeframe == self._execution_timeframe:
                bars = list(self._execution_bar_history)
            else:
                bars = self._repositories.bars.list_recent(
                    symbol=self._settings.symbol,
                    timeframe=timeframe,
                    limit=restore_limit,
                )
                if not bars:
                    resampled = build_resampled_bars(
                        self._execution_bar_history,
                        target_timeframe=timeframe,
                        bar_builder=self._bar_builder,
                        alignment=(
                            "rolling"
                            if self._settings.execution_timeframe_role is ExecutionTimeframeRole.EXECUTION_DETAIL_ONLY
                            else "bucket"
                        ),
                    )
                    bars = list(resampled.bars)
            self._context_bar_histories[timeframe] = bars
            self._context_feature_histories[timeframe] = self._rebuild_feature_history(bars, timeframe)
            self._last_completed_context_bar_end_by_timeframe[timeframe] = bars[-1].end_ts if bars else None
        self._bar_history = list(self._context_bar_histories.get(self._primary_context_timeframe, []))
        self._feature_history = list(self._context_feature_histories.get(self._primary_context_timeframe, []))
        if self._feature_history:
            self._last_feature_packet = self._feature_history[-1]
            self._startup_restore_in_progress = True
            try:
                self._latest_context_signal_packet = self._evaluate_signals(self._feature_history[-1], self._feature_history)
            finally:
                self._startup_restore_in_progress = False
        if self._execution_bar_history:
            self._last_execution_bar_id = self._execution_bar_history[-1].bar_id
            self._last_execution_bar_evaluated_at = self._execution_bar_history[-1].end_ts

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
                        signal_id=self._signal_id_for_actionable_signal(bar, "LONG", signal_packet.long_entry_source),
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
                        signal_id=self._signal_id_for_actionable_signal(bar, "SHORT", signal_packet.short_entry_source),
                    )
            return None

        if (
            state.position_side == PositionSide.LONG
            and state.open_broker_order_id is None
            and state.exits_enabled
            and exit_decision.long_exit
        ):
            return OrderIntent(
                order_intent_id=f"{bar.bar_id}|{OrderIntentType.SELL_TO_CLOSE.value}",
                bar_id=bar.bar_id,
                symbol=self._settings.symbol,
                intent_type=OrderIntentType.SELL_TO_CLOSE,
                quantity=state.internal_position_qty,
                created_at=bar.end_ts,
                reason_code=exit_decision.primary_reason.value if exit_decision.primary_reason else "longExit",
                signal_id=self._signal_id_for_actionable_signal(
                    bar,
                    "EXIT_LONG",
                    exit_decision.primary_reason.value if exit_decision.primary_reason else "longExit",
                ),
            )

        if (
            state.position_side == PositionSide.SHORT
            and state.open_broker_order_id is None
            and state.exits_enabled
            and exit_decision.short_exit
        ):
            return OrderIntent(
                order_intent_id=f"{bar.bar_id}|{OrderIntentType.BUY_TO_CLOSE.value}",
                bar_id=bar.bar_id,
                symbol=self._settings.symbol,
                intent_type=OrderIntentType.BUY_TO_CLOSE,
                quantity=state.internal_position_qty,
                created_at=bar.end_ts,
                reason_code=exit_decision.primary_reason.value if exit_decision.primary_reason else "shortExit",
                signal_id=self._signal_id_for_actionable_signal(
                    bar,
                    "EXIT_SHORT",
                    exit_decision.primary_reason.value if exit_decision.primary_reason else "shortExit",
                ),
            )
        return None

    def _signal_id_for_actionable_signal(self, bar: Bar, side: str, source: str | None) -> str:
        normalized_source = str(source or "unknown").strip() or "unknown"
        return f"{bar.bar_id}|signal|{side}|{normalized_source}"

    def _pre_submit_attempt_id(self, intent: OrderIntent, occurred_at: datetime) -> str:
        return f"{intent.order_intent_id}|pre_submit|{occurred_at.isoformat()}"

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
                "strategy_trade_id": fill_event.order_intent_id,
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
        submit_attempt_id: str | None = None,
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
                "signal_id": intent.signal_id,
                "order_intent_id": intent.order_intent_id,
                "strategy_trade_id": intent.order_intent_id,
                "intent_type": intent.intent_type.value,
                "quantity": intent.quantity,
                "reason_code": intent.reason_code,
                "submit_attempt_id": submit_attempt_id,
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

    def _emit_order_rejection_alert(
        self,
        intent: OrderIntent,
        occurred_at: datetime,
        *,
        reason: str,
        submit_attempt_id: str | None = None,
    ) -> None:
        if self._alert_dispatcher is None:
            return
        self._alert_dispatcher.emit(
            severity="ACTION",
            code="order_rejection",
            message=f"Order intent for {intent.symbol} was rejected before broker submission.",
            payload={
                **self._runtime_identity,
                "instrument": intent.symbol,
                "signal_id": intent.signal_id,
                "order_intent_id": intent.order_intent_id,
                "strategy_trade_id": intent.order_intent_id,
                "submit_attempt_id": submit_attempt_id,
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
                "signal_id": intent.signal_id,
                "order_intent_id": intent.order_intent_id,
                "strategy_trade_id": intent.order_intent_id,
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
        blocked_payload = self._persist_blocked_strategy_intent(
            intent,
            occurred_at=occurred_at,
            reason=reason,
            submit_attempt_id=failure.submit_attempt_id if failure is not None else None,
        )
        self._latest_live_intent_summary = {
            **self._latest_live_intent_summary,
            "blocked_strategy_intent": blocked_payload,
        }
        self._emit_order_rejection_alert(
            intent,
            occurred_at,
            reason=reason,
            submit_attempt_id=failure.submit_attempt_id if failure is not None else None,
        )
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

    def _broker_fill_event_from_pending(self, pending) -> FillEvent:
        status_payload = self._execution_engine.broker.get_order_status(pending.broker_order_id) or {}
        fill_timestamp = _parse_fill_timestamp(status_payload.get("fill_timestamp")) or pending.acknowledged_at or pending.submitted_at
        fill_price = _parse_fill_price(status_payload.get("fill_price"))
        return FillEvent(
            order_intent_id=pending.intent.order_intent_id,
            intent_type=pending.intent.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=fill_timestamp,
            fill_price=fill_price,
            broker_order_id=pending.broker_order_id,
            quantity=pending.intent.quantity,
        )

    def _persist_filled_bridge_result(
        self,
        *,
        pending,
        fill_event: FillEvent | None,
        classification: str,
        review_required: bool,
        error: str | None = None,
    ) -> dict[str, object]:
        submit_attempt = self._execution_engine.last_submit_attempt() or {}
        status_payload = self._execution_engine.broker.get_order_status(pending.broker_order_id) or {}
        manifest_update = update_manifest_from_filled_bridge_result(
            filled_bridge_result={
                "order_intent_id": pending.intent.order_intent_id,
                "strategy_id": self._runtime_identity.get("standalone_strategy_id") or self._runtime_identity.get("strategy_id"),
                "lane_id": self._runtime_identity.get("lane_id"),
                "instrument": self._runtime_identity.get("instrument") or pending.intent.symbol,
                "symbol": pending.intent.symbol,
                "action": _intent_side(pending.intent),
                "quantity": pending.intent.quantity,
                "broker_order_id": pending.broker_order_id,
                "account_id": status_payload.get("account_id") or submit_attempt.get("account_id"),
                "perm_id": status_payload.get("perm_id") or submit_attempt.get("perm_id"),
                "client_id": status_payload.get("client_id") or submit_attempt.get("client_id"),
                "exec_id": status_payload.get("execution_id") or submit_attempt.get("execution_id"),
                "local_symbol": status_payload.get("local_symbol") or submit_attempt.get("local_symbol"),
                "con_id": status_payload.get("con_id") or submit_attempt.get("con_id"),
                "contract": status_payload.get("contract") or dict(submit_attempt.get("bridge_order_metadata") or {}).get("contract"),
                "managed_exit_policy_id": self._runtime_identity.get("managed_exit_policy_id"),
            },
            output_root=DEFAULT_TRACK_B_POSITION_MANAGEMENT_MANIFEST_ROOT,
        )
        manifest = manifest_update.manifest if manifest_update is not None else {}
        metadata = resolve_management_metadata(
            source={
                "order_intent_id": pending.intent.order_intent_id,
                "lane_id": self._runtime_identity.get("lane_id"),
                "strategy_id": self._runtime_identity.get("standalone_strategy_id") or self._runtime_identity.get("strategy_id"),
                "managed_exit_policy_id": self._runtime_identity.get("managed_exit_policy_id"),
                "position_management_manifest_path": str(manifest_update.manifest_path)
                if manifest_update is not None
                else None,
            }
        )
        payload: dict[str, object] = {
            "schema_version": "strategy_managed_filled_bridge_result_v1",
            "artifact_type": "filled_bridge_result",
            "classification": classification,
            "review_required": bool(review_required),
            "strategy_id": self._runtime_identity.get("standalone_strategy_id") or self._runtime_identity.get("strategy_id"),
            "lane_id": self._runtime_identity.get("lane_id"),
            "strategy_family": self._runtime_identity.get("strategy_family"),
            "instrument": self._runtime_identity.get("instrument") or pending.intent.symbol,
            "symbol": pending.intent.symbol,
            "side": _intent_side(pending.intent),
            "action": _intent_side(pending.intent),
            "quantity": pending.intent.quantity,
            "order_intent_id": pending.intent.order_intent_id,
            "intent_type": pending.intent.intent_type.value,
            "decision_bar_timestamp": pending.intent.created_at.isoformat(),
            "bar_id": pending.intent.bar_id,
            "broker_order_id": pending.broker_order_id,
            "account_id": status_payload.get("account_id") or submit_attempt.get("account_id"),
            "perm_id": status_payload.get("perm_id") or submit_attempt.get("perm_id"),
            "client_id": status_payload.get("client_id") or submit_attempt.get("client_id"),
            "exec_id": status_payload.get("execution_id") or submit_attempt.get("execution_id"),
            "local_symbol": status_payload.get("local_symbol") or submit_attempt.get("local_symbol"),
            "con_id": status_payload.get("con_id") or submit_attempt.get("con_id"),
            "contract": status_payload.get("contract") or dict(submit_attempt.get("bridge_order_metadata") or {}).get("contract"),
            "fill_price": str(fill_event.fill_price) if fill_event is not None and fill_event.fill_price is not None else status_payload.get("fill_price") or submit_attempt.get("fill_price"),
            "fill_timestamp": fill_event.fill_timestamp.isoformat() if fill_event is not None else status_payload.get("fill_timestamp") or submit_attempt.get("fill_timestamp"),
            "bridge_classification": submit_attempt.get("bridge_classification"),
            "bridge_order_status": submit_attempt.get("bridge_order_status"),
            "route_destination": submit_attempt.get("route_destination"),
            "managed_exit_policy_id": metadata.managed_exit_policy_id,
            "position_management_manifest_path": str(manifest_update.manifest_path)
            if manifest_update is not None
            else None,
            "position_management_manifest_status": manifest.get("lifecycle_status"),
            "position_management_metadata_source": metadata.source,
            "intended_lifecycle_mode": "STRATEGY_MANAGED",
            "lifecycle_state": self._state.strategy_status.value,
            "position_side": self._state.position_side.value,
            "internal_position_qty": self._state.internal_position_qty,
            "broker_position_qty": self._state.broker_position_qty,
            "paper_proof_invoked": False,
            "live_money_readiness": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        if error:
            payload["error"] = error
        if self._structured_logger is not None:
            if hasattr(self._structured_logger, "log_filled_bridge_result"):
                self._structured_logger.log_filled_bridge_result(payload)
            if hasattr(self._structured_logger, "write_filled_bridge_result_state"):
                self._structured_logger.write_filled_bridge_result_state(payload)
        try:
            from mgc_v05l.execution_core.track_b_paper_trade_ledger import (
                update_track_b_paper_trade_ledger_from_filled_bridge_result,
            )

            update_track_b_paper_trade_ledger_from_filled_bridge_result(filled_bridge_result=payload)
            payload["paper_trade_ledger_update_attempted"] = True
        except Exception as exc:  # noqa: BLE001 - compact ledger update must not become submit authority.
            payload["paper_trade_ledger_update_attempted"] = True
            payload["paper_trade_ledger_update_error"] = str(exc)
        self._latest_live_intent_summary = {
            **self._latest_live_intent_summary,
            "filled_bridge_result": payload,
            "review_required": bool(review_required),
        }
        return payload

    def _persist_blocked_strategy_intent(
        self,
        intent: OrderIntent,
        *,
        occurred_at: datetime,
        reason: str,
        submit_attempt_id: str | None,
    ) -> dict[str, object]:
        submit_attempt = self._execution_engine.last_submit_attempt() or {}
        classification = _blocked_intent_classification(reason, submit_attempt)
        monitor_snapshot = _blocked_intent_monitor_snapshot(submit_attempt, reason)
        route_target = _blocked_intent_route_target(submit_attempt, intent)
        payload: dict[str, object] = {
            "schema_version": "strategy_managed_blocked_intent_v1",
            "artifact_type": "blocked_strategy_intent",
            "strategy_id": self._runtime_identity.get("standalone_strategy_id") or self._runtime_identity.get("strategy_id"),
            "lane_id": self._runtime_identity.get("lane_id"),
            "strategy_family": self._runtime_identity.get("strategy_family"),
            "instrument": self._runtime_identity.get("instrument") or intent.symbol,
            "symbol": intent.symbol,
            "side": _intent_side(intent),
            "order_intent_id": intent.order_intent_id,
            "intent_type": intent.intent_type.value,
            "signal_id": intent.signal_id,
            "signal_timestamp": intent.created_at.isoformat(),
            "decision_bar_timestamp": occurred_at.isoformat(),
            "bar_id": intent.bar_id,
            "source_artifact": "runtime_completed_decision_bar",
            "source_artifact_bar_id": intent.bar_id,
            "route_target": route_target,
            "intended_lifecycle_mode": "STRATEGY_MANAGED",
            "submit_allowed": False,
            "submit_attempt_id": submit_attempt_id,
            "submit_attempted": bool(submit_attempt),
            "blocker_classification": classification,
            "exact_blocker_reason": reason,
            "monitor_running": monitor_snapshot.get("monitor_running"),
            "health_classification": monitor_snapshot.get("health_classification"),
            "bridge_allowed": monitor_snapshot.get("bridge_allowed"),
            "broker_refresh_timestamp": monitor_snapshot.get("broker_refresh_timestamp"),
            "broker_refresh_freshness": monitor_snapshot.get("broker_refresh_freshness"),
            "account": monitor_snapshot.get("account"),
            "contract": monitor_snapshot.get("contract"),
            "bridge_classification": submit_attempt.get("bridge_classification"),
            "bridge_detail": submit_attempt.get("bridge_detail"),
            "bridge_gate_trace": list(submit_attempt.get("bridge_gate_trace") or []),
            "paper_proof_invoked": False,
            "live_money_readiness": False,
            "created_at": occurred_at.isoformat(),
        }
        self._persist_order_intent(
            intent,
            broker_order_id=None,
            order_status=OrderStatus.REJECTED,
            broker_order_status="PRE_SUBMIT_BLOCKED",
            last_status_checked_at=occurred_at,
            timeout_classification=classification,
            timeout_status_updated_at=occurred_at,
            retry_count=0,
        )
        if self._structured_logger is not None:
            self._structured_logger.log_blocked_strategy_intent(payload)
            self._structured_logger.write_blocked_strategy_intent_state(payload)
        return payload

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
            "signal_id": intent.signal_id,
            "order_intent_id": intent.order_intent_id,
            "strategy_trade_id": intent.order_intent_id,
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
            "signal_id": intent.signal_id,
            "order_intent_id": intent.order_intent_id,
            "strategy_trade_id": intent.order_intent_id,
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
        broker_order_id: str | None,
        order_status: OrderStatus = OrderStatus.ACKNOWLEDGED,
        *,
        submitted_at: datetime | None = None,
        acknowledged_at: datetime | None = None,
        broker_order_status: str | None = None,
        last_status_checked_at: datetime | None = None,
        timeout_classification: str | None = None,
        timeout_status_updated_at: datetime | None = None,
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
            timeout_status_updated_at=timeout_status_updated_at,
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
    observed_ts = bar.end_ts.astimezone(timezone_info) if bar.end_ts.tzinfo is not None else bar.end_ts.replace(tzinfo=timezone_info)
    return session_restriction_matches_timestamp(observed_ts, restriction)


def _gold_probationary_session_matches_time(local_time: time, restriction: str) -> bool:
    windows = {
        "SESSION_OPEN": (time(18, 0), time(19, 0)),
        "ASIA_EARLY": (time(19, 0), time(22, 0)),
        "ASIA_LATE": (time(22, 0), time(3, 0)),
        "LONDON_EARLY": (time(3, 0), time(5, 30)),
        "LONDON_LATE": (time(5, 30), time(8, 20)),
        "US_EARLY": (time(8, 20), time(11, 0)),
        "NY_EARLY": (time(8, 20), time(11, 0)),
        "US_PREOPEN_OPENING": (time(9, 0), time(9, 30)),
        "US_CASH_OPEN_IMPULSE": (time(9, 30), time(10, 0)),
        "US_OPEN_LATE": (time(10, 0), time(10, 30)),
        "US_MIDDAY": (time(11, 0), time(13, 30)),
        "US_LATE": (time(13, 30), time(16, 0)),
        "NY_LATE": (time(11, 0), time(13, 30)),
    }
    window = windows.get(str(restriction or "").upper())
    if window is None:
        return False
    start, end = window
    if end <= start:
        return local_time >= start or local_time < end
    return start <= local_time < end


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
    observed_ts = bar.end_ts.astimezone(timezone_info) if bar.end_ts.tzinfo is not None else bar.end_ts.replace(tzinfo=timezone_info)
    return label_session_phase(observed_ts)


def _phase_coarse_session_group(phase: str) -> str:
    return phase_coarse_session_group(phase)


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
