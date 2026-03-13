"""Strategy engine orchestration for replay-first execution."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from typing import Optional

from ..config_models import StrategySettings
from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
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
from ..indicators.feature_engine import compute_features
from ..market_data.bar_store import BarStore
from ..market_data.session_clock import classify_sessions
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
)
from .trade_state import build_initial_state


class StrategyEngine:
    """Orchestrates deterministic bar-close processing."""

    def __init__(
        self,
        settings: StrategySettings,
        initial_state: Optional[StrategyState] = None,
        repositories: Optional[RepositorySet] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ) -> None:
        self._settings = settings
        self._repositories = repositories
        self._state_repository = StateRepository(repositories.engine) if repositories is not None else None
        self._bar_store = BarStore(repositories.processed_bars if repositories is not None else None)
        self._execution_engine = execution_engine or ExecutionEngine()
        self._state = self._load_initial_state(initial_state)
        self._bar_history: list[Bar] = []
        self._feature_history: list[FeaturePacket] = []
        self._last_signal_packet: Optional[SignalPacket] = None
        self._last_feature_packet: Optional[FeaturePacket] = None

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

        feature_packet = compute_features(self._bar_history, self._state, self._settings)
        feature_history = [*self._feature_history, feature_packet]
        signal_packet = self._evaluate_signals(feature_packet, feature_history)

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

        exit_decision = evaluate_exits(self._bar_history, feature_packet, working_state, risk_context, self._settings)
        if working_state.position_side != PositionSide.FLAT:
            events.append(
                ExitEvaluatedEvent(
                    bar_id=session_bar.bar_id,
                    primary_reason=exit_decision.primary_reason,
                    occurred_at=session_bar.end_ts,
                )
            )

        violations = validate_state(working_state)
        if violations:
            fault_code = "; ".join(violations)
            working_state = transition_to_fault(working_state, session_bar.end_ts, fault_code)
            events.append(FaultRaisedEvent(fault_code=fault_code, occurred_at=session_bar.end_ts))
            self._persist_state(working_state, transition_label="fault")
        else:
            maybe_intent = self._maybe_create_order_intent(session_bar, signal_packet, working_state, exit_decision)
            if maybe_intent is not None:
                long_entry_family = self._resolve_long_entry_family(signal_packet)
                pending = self._execution_engine.submit_intent(
                    maybe_intent,
                    signal_bar_id=session_bar.bar_id if maybe_intent.is_entry else None,
                    long_entry_family=long_entry_family,
                )
                if pending is not None:
                    working_state = replace(
                        working_state,
                        last_order_intent_id=maybe_intent.order_intent_id,
                        open_broker_order_id=pending.broker_order_id,
                        updated_at=session_bar.end_ts,
                    )
                    events.append(
                        OrderIntentCreatedEvent(
                            order_intent_id=maybe_intent.order_intent_id,
                            bar_id=session_bar.bar_id,
                            intent_type=maybe_intent.intent_type,
                            occurred_at=session_bar.end_ts,
                        )
                    )
                    self._persist_order_intent(maybe_intent, pending.broker_order_id)
                    self._persist_state(working_state, transition_label="intent_created")

        self._bar_store.mark_processed(session_bar)
        self._persist_bar_artifacts(session_bar, feature_packet, signal_packet)
        self._feature_history.append(feature_packet)
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
    ) -> StrategyState:
        """Apply a confirmed fill to strategy state."""
        if fill_event.intent_type in (OrderIntentType.BUY_TO_OPEN, OrderIntentType.SELL_TO_OPEN):
            if signal_bar_id is None:
                raise ValueError("signal_bar_id is required for entry fills.")
            self._state = transition_on_entry_fill(
                state=self._state,
                fill_event=fill_event,
                trade_size=self._settings.trade_size,
                signal_bar_id=signal_bar_id,
                long_entry_family=long_entry_family,
            )
        else:
            self._state = transition_on_exit_fill(self._state, fill_event)
        self._persist_fill(fill_event)
        self._persist_state(self._state, transition_label="fill")
        return self._state

    @property
    def state(self) -> StrategyState:
        return self._state

    def _load_initial_state(self, initial_state: Optional[StrategyState]) -> StrategyState:
        if initial_state is not None:
            return initial_state
        if self._state_repository is not None:
            persisted_state = self._state_repository.load_latest()
            if persisted_state is not None:
                return persisted_state
        now = datetime.now(timezone.utc)
        return transition_to_ready(build_initial_state(now), now)

    def _apply_due_replay_fills(self, bar: Bar) -> list[DomainEvent]:
        events: list[DomainEvent] = []
        for pending in self._execution_engine.pop_due_replay_fills(bar, self._settings):
            fill = self._execution_engine.materialize_replay_fill(pending, bar)
            self.apply_fill(
                fill_event=fill,
                signal_bar_id=pending.signal_bar_id,
                long_entry_family=pending.long_entry_family,
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
        bull = evaluate_bull_snap(self._bar_history, feature_packet, self._state, self._settings)
        bear = evaluate_bear_snap(self._bar_history, feature_packet, self._state, self._settings)
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
        if state.position_side == PositionSide.FLAT:
            if (
                state.strategy_status is StrategyStatus.READY
                and warmup_complete
                and state.entries_enabled
                and not state.operator_halt
            ):
                if signal_packet.long_entry:
                    return OrderIntent(
                        order_intent_id=f"{bar.bar_id}|{OrderIntentType.BUY_TO_OPEN.value}",
                        bar_id=bar.bar_id,
                        symbol=self._settings.symbol,
                        intent_type=OrderIntentType.BUY_TO_OPEN,
                        quantity=self._settings.trade_size,
                        created_at=bar.end_ts,
                        reason_code=signal_packet.long_entry_source or "longEntry",
                    )
                if signal_packet.short_entry:
                    return OrderIntent(
                        order_intent_id=f"{bar.bar_id}|{OrderIntentType.SELL_TO_OPEN.value}",
                        bar_id=bar.bar_id,
                        symbol=self._settings.symbol,
                        intent_type=OrderIntentType.SELL_TO_OPEN,
                        quantity=self._settings.trade_size,
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
        return LongEntryFamily.NONE

    def _persist_bar_artifacts(self, bar: Bar, features: FeaturePacket, signals: SignalPacket) -> None:
        if self._repositories is None:
            return
        self._repositories.bars.save(bar)
        self._repositories.features.save(features, created_at=bar.end_ts)
        self._repositories.signals.save(signals, created_at=bar.end_ts)

    def _persist_order_intent(self, intent: OrderIntent, broker_order_id: str) -> None:
        if self._repositories is None:
            return
        self._repositories.order_intents.save(
            intent,
            broker_order_id=broker_order_id,
            order_status=OrderStatus.ACKNOWLEDGED,
        )

    def _persist_fill(self, fill: FillEvent) -> None:
        if self._repositories is None:
            return
        self._repositories.fills.save(fill)

    def _persist_state(self, state: StrategyState, transition_label: str) -> None:
        if self._state_repository is None:
            return
        self._state_repository.save_snapshot(state, transition_label=transition_label)


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
    )
