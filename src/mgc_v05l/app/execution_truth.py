"""Shared execution-truth emitters for comparative study generation."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any, Protocol, Sequence

from ..config_models import StrategySettings
from ..domain.models import Bar
from ..research.trend_participation.models import ResearchBar
from ..research.trend_participation.phase3_timing import build_phase3_replay_package

BASELINE_NEXT_BAR_OPEN = "BASELINE_NEXT_BAR_OPEN"
CURRENT_CANDLE_VWAP = "CURRENT_CANDLE_VWAP"
BAR_CONTEXT_DEFAULT = "BAR_CONTEXT_DEFAULT"

BASELINE_FILL_TRUTH = "BASELINE_FILL_TRUTH"
ENRICHED_EXECUTION_TRUTH = "ENRICHED_EXECUTION_TRUTH"
HYBRID_ENTRY_BASELINE_EXIT_TRUTH = "HYBRID_ENTRY_BASELINE_EXIT_TRUTH"
PAPER_RUNTIME_LEDGER = "PAPER_RUNTIME_LEDGER"
UNSUPPORTED_ENTRY_MODEL = "UNSUPPORTED_ENTRY_MODEL"

FULL_AUTHORITATIVE_LIFECYCLE = "FULL_AUTHORITATIVE_LIFECYCLE"
AUTHORITATIVE_INTRABAR_ENTRY_ONLY = "AUTHORITATIVE_INTRABAR_ENTRY_ONLY"
HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT = "HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT"
# Retain the machine-readable legacy token for backward-compatible artifacts.
BASELINE_PARITY_ONLY = "BASELINE_PARITY_ONLY"


@dataclass(frozen=True)
class ExecutionTruthEmitterContext:
    settings: StrategySettings
    bars: Sequence[Bar]
    source_bars: Sequence[Bar]
    rows: Sequence[dict[str, Any]]
    signal_by_bar_id: dict[str, dict[str, Any]]
    feature_by_bar_id: dict[str, dict[str, Any]]
    point_value: Decimal | None
    strategy_family: str | None
    standalone_strategy_id: str | None
    instrument: str
    requested_entry_model: str


@dataclass(frozen=True)
class ExecutionTruthEmitterResult:
    execution_truth_emitter: str
    supported_entry_models: tuple[str, ...]
    active_entry_model: str
    entry_model_supported: bool
    authoritative_intrabar_available: bool
    authoritative_entry_truth_available: bool
    authoritative_exit_truth_available: bool
    authoritative_trade_lifecycle_available: bool
    pnl_truth_basis: str
    lifecycle_truth_class: str
    unsupported_reason: str | None = None
    authoritative_execution_events: tuple[dict[str, Any], ...] = ()
    authoritative_trade_lifecycle_records: tuple[dict[str, Any], ...] = ()
    capability_rows: tuple[dict[str, Any], ...] = ()
    meta: dict[str, Any] = field(default_factory=dict)


class ExecutionTruthEmitter(Protocol):
    name: str

    def supports(self, context: ExecutionTruthEmitterContext) -> bool: ...

    def capability_rows(self, context: ExecutionTruthEmitterContext) -> list[dict[str, Any]]: ...

    def emit(self, context: ExecutionTruthEmitterContext) -> ExecutionTruthEmitterResult: ...


class AtpExecutionTruthEmitter:
    name = "atp_phase3_timing_emitter"

    def supports(self, context: ExecutionTruthEmitterContext) -> bool:
        family = str(context.strategy_family or "").upper()
        strategy_id = str(context.standalone_strategy_id or "").upper()
        return family == "ACTIVE_TREND_PARTICIPATION" or strategy_id.startswith("ATP") or "ATP" in strategy_id

    def capability_rows(self, context: ExecutionTruthEmitterContext) -> list[dict[str, Any]]:
        if not self.supports(context):
            return []
        return [
            {
                "subject": context.strategy_family or context.standalone_strategy_id or "ACTIVE_TREND_PARTICIPATION",
                "supported_entry_models": [BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP],
                "execution_truth_emitter": self.name,
                "authoritative_intrabar_available": _has_minute_execution_detail(context.source_bars),
                "unsupported_reason": None,
            }
        ]

    def emit(self, context: ExecutionTruthEmitterContext) -> ExecutionTruthEmitterResult:
        capability_rows = self.capability_rows(context)
        if context.requested_entry_model != CURRENT_CANDLE_VWAP:
            return _baseline_result(context, capability_rows=capability_rows, execution_truth_emitter=self.name)

        entry_states = []
        bar_ids: list[str] = []
        for row in context.rows:
            signal = context.signal_by_bar_id.get(str(row.get("bar_id") or "")) or {}
            if not signal.get("atp_entry_state") and not row.get("atp_entry_state"):
                continue
        minute_bars = [_to_research_bar(bar) for bar in context.source_bars if str(bar.timeframe or "").lower() == "1m"]
        serialized_timing_states = list(
            context.feature_by_bar_id.get("__atp_serialized_timing_states__", [])
        )
        serialized_shadow_trades = list(
            context.feature_by_bar_id.get("__atp_serialized_shadow_trades__", [])
        )
        if not serialized_timing_states and not serialized_shadow_trades:
            atp_entry_states = context.feature_by_bar_id.get("__atp_entry_states_by_bar_id__", {})
            ordered_pairs = [
                (bar.bar_id, atp_entry_states[bar.bar_id])
                for bar in context.bars
                if bar.bar_id in atp_entry_states
            ]
            if minute_bars and ordered_pairs:
                replay_package = build_phase3_replay_package(
                    entry_states=[entry_state for _, entry_state in ordered_pairs],
                    bars_1m=minute_bars,
                    point_value=float(context.point_value) if context.point_value is not None else 1.0,
                    old_proxy_trade_count=0,
                )
                serialized_timing_states = [
                    _serialize_timing_state(bar_id=bar_id, timing_state=timing_state)
                    for (bar_id, _), timing_state in zip(ordered_pairs, replay_package.get("timing_states") or [])
                ]
                serialized_shadow_trades = [
                    _serialize_shadow_trade(trade)
                    for trade in list(replay_package.get("shadow_trades") or [])
                ]

        authoritative_events = _build_atp_execution_events(
            rows=context.rows,
            serialized_timing_states=serialized_timing_states,
            serialized_shadow_trades=serialized_shadow_trades,
            entry_model=CURRENT_CANDLE_VWAP,
        )
        authoritative_entry_truth_available = bool(authoritative_events)
        authoritative_exit_truth_available = any(
            str(event.get("execution_event_type") or "") == "EXIT_TRIGGERED"
            for event in authoritative_events
        )
        authoritative_trade_lifecycle_available = bool(serialized_shadow_trades)
        lifecycle_truth_class = (
            FULL_AUTHORITATIVE_LIFECYCLE
            if authoritative_entry_truth_available and authoritative_exit_truth_available and authoritative_trade_lifecycle_available
            else AUTHORITATIVE_INTRABAR_ENTRY_ONLY
            if authoritative_entry_truth_available
            else UNSUPPORTED_ENTRY_MODEL
        )
        return ExecutionTruthEmitterResult(
            execution_truth_emitter=self.name,
            supported_entry_models=(BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP),
            active_entry_model=CURRENT_CANDLE_VWAP,
            entry_model_supported=True,
            authoritative_intrabar_available=bool(authoritative_events or serialized_shadow_trades),
            authoritative_entry_truth_available=authoritative_entry_truth_available,
            authoritative_exit_truth_available=authoritative_exit_truth_available,
            authoritative_trade_lifecycle_available=authoritative_trade_lifecycle_available,
            pnl_truth_basis=ENRICHED_EXECUTION_TRUTH,
            lifecycle_truth_class=lifecycle_truth_class,
            authoritative_execution_events=tuple(authoritative_events),
            authoritative_trade_lifecycle_records=tuple(serialized_shadow_trades),
            capability_rows=tuple(capability_rows),
            meta={
                "authoritative_execution_timing_records": serialized_timing_states,
                "authoritative_intrabar_timing_states": serialized_timing_states,
                "authoritative_intrabar_trades": serialized_shadow_trades,
            },
        )


class AsiaVwapExecutionTruthEmitter:
    name = "asia_vwap_reclaim_emitter"

    def supports(self, context: ExecutionTruthEmitterContext) -> bool:
        if not context.settings.enable_asia_vwap_longs:
            return False
        return True

    def capability_rows(self, context: ExecutionTruthEmitterContext) -> list[dict[str, Any]]:
        if not context.settings.enable_asia_vwap_longs:
            return []
        return [
            {
                "subject": "asiaVWAPLongSignal",
                "supported_entry_models": [BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP],
                "execution_truth_emitter": self.name,
                "authoritative_intrabar_available": _has_minute_execution_detail(context.source_bars),
                "unsupported_reason": None,
            }
        ]

    def emit(self, context: ExecutionTruthEmitterContext) -> ExecutionTruthEmitterResult:
        capability_rows = self.capability_rows(context)
        if context.requested_entry_model != CURRENT_CANDLE_VWAP:
            return _baseline_result(context, capability_rows=capability_rows, execution_truth_emitter=self.name)

        execution_events, lifecycle_records = _build_asia_vwap_execution_truth(context)
        authoritative_entry_truth_available = bool(execution_events)
        authoritative_exit_truth_available = any(
            str(event.get("execution_event_type") or "") == "EXIT_TRIGGERED"
            for event in execution_events
        )
        authoritative_trade_lifecycle_available = bool(lifecycle_records)
        lifecycle_truth_class = (
            HYBRID_AUTHORITATIVE_ENTRY_BASELINE_EXIT
            if authoritative_entry_truth_available and authoritative_trade_lifecycle_available
            else UNSUPPORTED_ENTRY_MODEL
        )
        return ExecutionTruthEmitterResult(
            execution_truth_emitter=self.name,
            supported_entry_models=(BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP),
            active_entry_model=CURRENT_CANDLE_VWAP,
            entry_model_supported=True,
            authoritative_intrabar_available=bool(execution_events),
            authoritative_entry_truth_available=authoritative_entry_truth_available,
            authoritative_exit_truth_available=authoritative_exit_truth_available,
            authoritative_trade_lifecycle_available=authoritative_trade_lifecycle_available,
            pnl_truth_basis=HYBRID_ENTRY_BASELINE_EXIT_TRUTH if execution_events else UNSUPPORTED_ENTRY_MODEL,
            lifecycle_truth_class=lifecycle_truth_class,
            unsupported_reason=None if execution_events else "No persisted asiaVWAPLongSignal entry context was observed for this study.",
            authoritative_execution_events=tuple(execution_events),
            authoritative_trade_lifecycle_records=tuple(lifecycle_records),
            capability_rows=tuple(capability_rows),
        )


def resolve_execution_truth(context: ExecutionTruthEmitterContext) -> ExecutionTruthEmitterResult:
    capability_rows = _capability_rows_for_context(context)
    for emitter in _emitters():
        if not emitter.supports(context):
            continue
        if context.requested_entry_model == CURRENT_CANDLE_VWAP:
            result = emitter.emit(context)
            if result.entry_model_supported:
                return result
    if context.requested_entry_model == CURRENT_CANDLE_VWAP:
        supported_union = sorted(
            {
                model
                for row in capability_rows
                for model in list(row.get("supported_entry_models") or [])
            }
            or {BASELINE_NEXT_BAR_OPEN}
        )
        return ExecutionTruthEmitterResult(
            execution_truth_emitter="unsupported",
            supported_entry_models=tuple(supported_union),
            active_entry_model=CURRENT_CANDLE_VWAP,
            entry_model_supported=False,
            authoritative_intrabar_available=False,
            authoritative_entry_truth_available=False,
            authoritative_exit_truth_available=False,
            authoritative_trade_lifecycle_available=False,
            pnl_truth_basis=UNSUPPORTED_ENTRY_MODEL,
            lifecycle_truth_class=UNSUPPORTED_ENTRY_MODEL,
            unsupported_reason="CURRENT_CANDLE_VWAP is not implemented for the observed strategy family/source set in this study.",
            capability_rows=tuple(capability_rows),
        )
    return _baseline_result(context, capability_rows=capability_rows, execution_truth_emitter="baseline_parity_emitter")


def _baseline_result(
    context: ExecutionTruthEmitterContext,
    *,
    capability_rows: Sequence[dict[str, Any]],
    execution_truth_emitter: str,
) -> ExecutionTruthEmitterResult:
    supported_union = sorted(
        {
            model
            for row in capability_rows
            for model in list(row.get("supported_entry_models") or [])
        }
        or {BASELINE_NEXT_BAR_OPEN}
    )
    return ExecutionTruthEmitterResult(
        execution_truth_emitter=execution_truth_emitter,
        supported_entry_models=tuple(supported_union),
        active_entry_model=BASELINE_NEXT_BAR_OPEN if context.requested_entry_model == BASELINE_NEXT_BAR_OPEN else context.requested_entry_model,
        entry_model_supported=context.requested_entry_model == BASELINE_NEXT_BAR_OPEN,
        authoritative_intrabar_available=False,
        authoritative_entry_truth_available=False,
        authoritative_exit_truth_available=False,
        authoritative_trade_lifecycle_available=False,
        pnl_truth_basis=BASELINE_FILL_TRUTH,
        lifecycle_truth_class=BASELINE_PARITY_ONLY,
        capability_rows=tuple(capability_rows),
    )


def _capability_rows_for_context(context: ExecutionTruthEmitterContext) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    observed_sources = _observed_entry_sources(context)
    for emitter in _emitters():
        rows.extend(emitter.capability_rows(context))
    supported_subjects = {str(row.get("subject") or "") for row in rows}
    for source in observed_sources:
        if source in supported_subjects:
            continue
        rows.append(
            {
                "subject": source,
                "supported_entry_models": [BASELINE_NEXT_BAR_OPEN],
                "execution_truth_emitter": "baseline_parity_emitter",
                "authoritative_intrabar_available": False,
                "unsupported_reason": f"No shared execution-truth emitter is implemented yet for {source}.",
            }
        )
    if not rows:
        rows.append(
            {
                "subject": context.strategy_family or context.standalone_strategy_id or context.instrument,
                "supported_entry_models": [BASELINE_NEXT_BAR_OPEN],
                "execution_truth_emitter": "baseline_parity_emitter",
                "authoritative_intrabar_available": False,
                "unsupported_reason": None,
            }
        )
    return rows


def _observed_entry_sources(context: ExecutionTruthEmitterContext) -> list[str]:
    observed: set[str] = set()
    for row in context.rows:
        source = str(row.get("entry_source_family") or row.get("latest_signal_source") or "").strip()
        if source:
            observed.add(source)
    for payload in context.signal_by_bar_id.values():
        for key in ("long_entry_source", "short_entry_source"):
            source = str(payload.get(key) or "").strip()
            if source:
                observed.add(source)
        if payload.get("asia_vwap_long_signal"):
            observed.add("asiaVWAPLongSignal")
    return sorted(observed)


def _emitters() -> tuple[ExecutionTruthEmitter, ...]:
    return (AtpExecutionTruthEmitter(), AsiaVwapExecutionTruthEmitter())


def _build_atp_execution_events(
    *,
    rows: Sequence[dict[str, Any]],
    serialized_timing_states: Sequence[dict[str, Any]],
    serialized_shadow_trades: Sequence[dict[str, Any]],
    entry_model: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for timing_state in serialized_timing_states:
        linked_bar_id = str(
            timing_state.get("bar_id")
            or _linked_bar_id_for_timestamp(rows=rows, timestamp=_maybe_iso_text(timing_state.get("decision_ts")))
            or ""
        )
        if not linked_bar_id:
            continue
        decision_timestamp = _maybe_iso_text(timing_state.get("decision_ts"))
        event_timestamp = _maybe_iso_text(timing_state.get("timing_bar_ts")) or _maybe_iso_text(timing_state.get("entry_ts")) or decision_timestamp
        linked_subbar_id = _maybe_iso_text(timing_state.get("timing_bar_ts")) or event_timestamp
        vwap_at_event = _feature_snapshot_value(timing_state.get("feature_snapshot"), "timing_checks", "bar_vwap")
        if timing_state.get("setup_armed"):
            events.append(
                _trade_event(
                    event_id=f"{linked_bar_id}:intrabar_armed:{decision_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_ARMED",
                    execution_event_type="ENTRY_ARMED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("context_entry_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=bool(timing_state.get("timing_confirmed")),
                    invalidation_reason=timing_state.get("primary_blocker") if timing_state.get("invalidated_before_entry") else None,
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if timing_state.get("timing_confirmed"):
            events.append(
                _trade_event(
                    event_id=f"{linked_bar_id}:intrabar_confirmed:{event_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_TIMING_CONFIRMED",
                    execution_event_type="ENTRY_CONFIRMED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("timing_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=True,
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if timing_state.get("invalidated_before_entry"):
            events.append(
                _trade_event(
                    event_id=f"{linked_bar_id}:intrabar_invalidated:{event_timestamp}",
                    linked_bar_id=linked_bar_id,
                    linked_subbar_id=linked_subbar_id,
                    event_type="ATP_ENTRY_INVALIDATED",
                    execution_event_type="ENTRY_INVALIDATED",
                    side=timing_state.get("side") or "LONG",
                    family=timing_state.get("family_name"),
                    reason=timing_state.get("primary_blocker") or timing_state.get("timing_state"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=event_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=timing_state.get("entry_price"),
                    vwap_at_event=vwap_at_event,
                    acceptance_state=timing_state.get("vwap_price_quality_state"),
                    confirmation_flag=False,
                    invalidation_reason=timing_state.get("primary_blocker"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
    for trade in serialized_shadow_trades:
        entry_timestamp = _maybe_iso_text(trade.get("entry_ts"))
        exit_timestamp = _maybe_iso_text(trade.get("exit_ts"))
        decision_timestamp = _maybe_iso_text(trade.get("decision_ts"))
        entry_bar_id = _linked_bar_id_for_timestamp(rows=rows, timestamp=entry_timestamp)
        exit_bar_id = _linked_bar_id_for_timestamp(rows=rows, timestamp=exit_timestamp)
        if entry_bar_id and entry_timestamp:
            events.append(
                _trade_event(
                    event_id=f"{entry_bar_id}:intrabar_entry_executed:{entry_timestamp}",
                    linked_bar_id=entry_bar_id,
                    linked_subbar_id=entry_timestamp,
                    event_type="ATP_ENTRY_EXECUTED",
                    execution_event_type="ENTRY_EXECUTED",
                    side=trade.get("side"),
                    family=trade.get("family"),
                    reason=trade.get("decision_id"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=entry_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=trade.get("entry_price"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
        if exit_bar_id and exit_timestamp:
            events.append(
                _trade_event(
                    event_id=f"{exit_bar_id}:intrabar_exit_executed:{exit_timestamp}",
                    linked_bar_id=exit_bar_id,
                    linked_subbar_id=exit_timestamp,
                    event_type="ATP_EXIT_EXECUTED",
                    execution_event_type="EXIT_TRIGGERED",
                    side=trade.get("side"),
                    family=trade.get("family"),
                    reason=trade.get("exit_reason"),
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=exit_timestamp,
                    source_resolution="INTRABAR",
                    entry_model=entry_model,
                    event_price=trade.get("exit_price"),
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
    return events


def _build_asia_vwap_execution_truth(
    context: ExecutionTruthEmitterContext,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ordered_rows = sorted(context.rows, key=lambda row: str(row.get("timestamp") or row.get("end_timestamp") or ""))
    context_bars_by_id = {bar.bar_id: bar for bar in context.bars}
    source_bars_by_parent = _group_source_bars_by_parent(context.source_bars, context.bars)
    last_reclaim: dict[str, Any] | None = None
    entry_records: list[dict[str, Any]] = []
    for row in ordered_rows:
        bar_id = str(row.get("bar_id") or "")
        signal = context.signal_by_bar_id.get(bar_id) or {}
        feature = context.feature_by_bar_id.get(bar_id) or {}
        current_bar = context_bars_by_id.get(bar_id)
        if signal.get("asia_reclaim_bar_raw") and current_bar is not None:
            last_reclaim = {
                "reclaim_bar_id": bar_id,
                "reclaim_high": float(current_bar.high),
                "reclaim_low": float(current_bar.low),
                "reclaim_vwap": float(feature.get("vwap")) if feature.get("vwap") is not None else None,
            }
            continue
        if not signal.get("asia_vwap_long_signal") or last_reclaim is None:
            continue
        parent_bar = context_bars_by_id.get(bar_id)
        if parent_bar is None:
            continue
        decision_timestamp = parent_bar.start_ts.isoformat()
        entry_records.append(
            _trade_event(
                event_id=f"{bar_id}:asia_vwap_armed:{decision_timestamp}",
                linked_bar_id=bar_id,
                linked_subbar_id=decision_timestamp,
                event_type="ASIA_VWAP_ENTRY_ARMED",
                execution_event_type="ENTRY_ARMED",
                side="LONG",
                family="asiaVWAPLongSignal",
                reason="asia_acceptance_bar",
                decision_context_timestamp=decision_timestamp,
                event_timestamp=decision_timestamp,
                source_resolution="INTRABAR",
                entry_model=CURRENT_CANDLE_VWAP,
                event_price=None,
                vwap_at_event=last_reclaim.get("reclaim_vwap"),
                acceptance_state="ASIA_ACCEPTANCE_MONITORING",
                truth_authority="AUTHORITATIVE_INTRABAR",
            )
        )
        qualifying_slice = _first_asia_vwap_qualifying_slice(
            source_bars_by_parent.get(bar_id, []),
            reclaim_high=last_reclaim.get("reclaim_high"),
            reclaim_vwap=last_reclaim.get("reclaim_vwap"),
            require_vwap_close=bool(context.settings.require_acceptance_close_above_vwap),
        )
        if qualifying_slice is None:
            entry_records.append(
                _trade_event(
                    event_id=f"{bar_id}:asia_vwap_unsupported:{decision_timestamp}",
                    linked_bar_id=bar_id,
                    linked_subbar_id=decision_timestamp,
                    event_type="ASIA_VWAP_ENTRY_UNSUPPORTED",
                    execution_event_type="ENTRY_INVALIDATED",
                    side="LONG",
                    family="asiaVWAPLongSignal",
                    reason="persisted_signal_without_intrabar_acceptance_slice",
                    decision_context_timestamp=decision_timestamp,
                    event_timestamp=parent_bar.end_ts.isoformat(),
                    source_resolution="INTRABAR",
                    entry_model=CURRENT_CANDLE_VWAP,
                    event_price=None,
                    vwap_at_event=last_reclaim.get("reclaim_vwap"),
                    acceptance_state="ASIA_ACCEPTANCE_UNRESOLVED",
                    invalidation_reason="persisted_signal_without_intrabar_acceptance_slice",
                    truth_authority="AUTHORITATIVE_INTRABAR",
                )
            )
            continue
        event_price = max(
            float(qualifying_slice.open),
            float(last_reclaim.get("reclaim_high") or qualifying_slice.open),
            float(last_reclaim.get("reclaim_vwap") or qualifying_slice.open),
        )
        entry_ts = qualifying_slice.end_ts.isoformat()
        entry_records.append(
            _trade_event(
                event_id=f"{bar_id}:asia_vwap_confirmed:{entry_ts}",
                linked_bar_id=bar_id,
                linked_subbar_id=entry_ts,
                event_type="ASIA_VWAP_ENTRY_CONFIRMED",
                execution_event_type="ENTRY_CONFIRMED",
                side="LONG",
                family="asiaVWAPLongSignal",
                reason="asia_vwap_long_signal",
                decision_context_timestamp=decision_timestamp,
                event_timestamp=entry_ts,
                source_resolution="INTRABAR",
                entry_model=CURRENT_CANDLE_VWAP,
                event_price=event_price,
                vwap_at_event=last_reclaim.get("reclaim_vwap"),
                acceptance_state="ASIA_ACCEPTANCE_CONFIRMED",
                confirmation_flag=True,
                truth_authority="AUTHORITATIVE_INTRABAR",
            )
        )
        entry_records.append(
            _trade_event(
                event_id=f"{bar_id}:asia_vwap_executed:{entry_ts}",
                linked_bar_id=bar_id,
                linked_subbar_id=entry_ts,
                event_type="ASIA_VWAP_ENTRY_EXECUTED",
                execution_event_type="ENTRY_EXECUTED",
                side="LONG",
                family="asiaVWAPLongSignal",
                reason="asia_vwap_long_signal",
                decision_context_timestamp=decision_timestamp,
                event_timestamp=entry_ts,
                source_resolution="INTRABAR",
                entry_model=CURRENT_CANDLE_VWAP,
                event_price=event_price,
                vwap_at_event=last_reclaim.get("reclaim_vwap"),
                acceptance_state="ASIA_ACCEPTANCE_CONFIRMED",
                confirmation_flag=True,
                truth_authority="AUTHORITATIVE_INTRABAR",
            )
        )

    lifecycle_records = _build_hybrid_lifecycle_records(rows=ordered_rows, authoritative_events=entry_records, family="asiaVWAPLongSignal")
    for record in lifecycle_records:
        exit_timestamp = _maybe_iso_text(record.get("exit_ts"))
        exit_bar_id = _linked_bar_id_for_timestamp(rows=ordered_rows, timestamp=exit_timestamp)
        if exit_bar_id and exit_timestamp:
            entry_records.append(
                _trade_event(
                    event_id=f"{exit_bar_id}:asia_vwap_exit:{exit_timestamp}",
                    linked_bar_id=exit_bar_id,
                    linked_subbar_id=exit_timestamp,
                    event_type="ASIA_VWAP_EXIT_EXECUTED",
                    execution_event_type="EXIT_TRIGGERED",
                    side=record.get("side"),
                    family=record.get("family"),
                    reason=record.get("exit_reason"),
                    decision_context_timestamp=_maybe_iso_text(record.get("decision_ts")),
                    event_timestamp=exit_timestamp,
                    source_resolution="BAR_CONTEXT",
                    entry_model=CURRENT_CANDLE_VWAP,
                    event_price=record.get("exit_price"),
                    truth_authority="HYBRID_BASELINE_EXIT",
                )
            )
    return entry_records, lifecycle_records


def _group_source_bars_by_parent(source_bars: Sequence[Bar], context_bars: Sequence[Bar]) -> dict[str, list[Bar]]:
    ordered_source = sorted(source_bars, key=lambda bar: bar.end_ts)
    ordered_context = sorted(context_bars, key=lambda bar: bar.end_ts)
    grouped: dict[str, list[Bar]] = {}
    context_index = 0
    for source_bar in ordered_source:
        while context_index < len(ordered_context) and source_bar.end_ts > ordered_context[context_index].end_ts:
            context_index += 1
        if context_index >= len(ordered_context):
            break
        parent_bar = ordered_context[context_index]
        if source_bar.start_ts < parent_bar.start_ts or source_bar.end_ts > parent_bar.end_ts:
            continue
        grouped.setdefault(parent_bar.bar_id, []).append(source_bar)
    return grouped


def _first_asia_vwap_qualifying_slice(
    source_bars: Sequence[Bar],
    *,
    reclaim_high: float | None,
    reclaim_vwap: float | None,
    require_vwap_close: bool,
) -> Bar | None:
    if reclaim_high is None:
        return None
    for bar in sorted(source_bars, key=lambda item: item.end_ts):
        close_price = float(bar.close)
        if close_price <= reclaim_high:
            continue
        if require_vwap_close and reclaim_vwap is not None and close_price <= reclaim_vwap:
            continue
        return bar
    return None


def _build_hybrid_lifecycle_records(
    *,
    rows: Sequence[dict[str, Any]],
    authoritative_events: Sequence[dict[str, Any]],
    family: str,
) -> list[dict[str, Any]]:
    entry_events = [
        event
        for event in authoritative_events
        if str(event.get("execution_event_type") or "") == "ENTRY_EXECUTED"
        and str(event.get("family") or "") == family
    ]
    exit_markers = []
    for row in rows:
        for marker in list(row.get("fill_markers") or []):
            if not marker.get("is_exit"):
                continue
            exit_markers.append(
                {
                    "timestamp": _maybe_iso_text(marker.get("timestamp")),
                    "price": marker.get("price"),
                    "reason": marker.get("intent_type"),
                }
            )
    used_exit_indices: set[int] = set()
    lifecycle_records: list[dict[str, Any]] = []
    for index, event in enumerate(sorted(entry_events, key=lambda item: str(item.get("event_timestamp") or "")), start=1):
        event_ts = _maybe_iso_text(event.get("event_timestamp"))
        exit_marker = None
        for exit_index, marker in enumerate(exit_markers):
            if exit_index in used_exit_indices:
                continue
            if marker["timestamp"] and event_ts and marker["timestamp"] > event_ts:
                exit_marker = marker
                used_exit_indices.add(exit_index)
                break
        if exit_marker is None:
            continue
        entry_price = Decimal(str(event.get("event_price")))
        exit_price = Decimal(str(exit_marker.get("price")))
        lifecycle_records.append(
            {
                "decision_id": f"{family}:{index}:{event.get('decision_context_timestamp')}",
                "decision_ts": event.get("decision_context_timestamp"),
                "entry_ts": event_ts,
                "exit_ts": exit_marker.get("timestamp"),
                "entry_price": str(entry_price),
                "exit_price": str(exit_price),
                "pnl_points": str(exit_price - entry_price),
                "exit_reason": exit_marker.get("reason"),
                "family": family,
                "side": "LONG",
            }
        )
    return lifecycle_records


def _serialize_timing_state(*, bar_id: str, timing_state: Any) -> dict[str, Any]:
    return {
        "bar_id": bar_id,
        "instrument": getattr(timing_state, "instrument", None),
        "decision_ts": getattr(timing_state, "decision_ts", None).isoformat()
        if getattr(timing_state, "decision_ts", None) is not None
        else None,
        "session_segment": getattr(timing_state, "session_segment", None),
        "family_name": getattr(timing_state, "family_name", None),
        "context_entry_state": getattr(timing_state, "context_entry_state", None),
        "timing_state": getattr(timing_state, "timing_state", None),
        "vwap_price_quality_state": getattr(timing_state, "vwap_price_quality_state", None),
        "blocker_codes": list(getattr(timing_state, "blocker_codes", ()) or ()),
        "primary_blocker": getattr(timing_state, "primary_blocker", None),
        "setup_armed": bool(getattr(timing_state, "setup_armed", False)),
        "timing_confirmed": bool(getattr(timing_state, "timing_confirmed", False)),
        "executable_entry": bool(getattr(timing_state, "executable_entry", False)),
        "invalidated_before_entry": bool(getattr(timing_state, "invalidated_before_entry", False)),
        "setup_armed_but_not_executable": bool(getattr(timing_state, "setup_armed_but_not_executable", False)),
        "entry_executed": bool(getattr(timing_state, "entry_executed", False)),
        "timing_bar_ts": getattr(timing_state, "timing_bar_ts", None).isoformat()
        if getattr(timing_state, "timing_bar_ts", None) is not None
        else None,
        "entry_ts": getattr(timing_state, "entry_ts", None).isoformat()
        if getattr(timing_state, "entry_ts", None) is not None
        else None,
        "entry_price": getattr(timing_state, "entry_price", None),
        "feature_snapshot": dict(getattr(timing_state, "feature_snapshot", {}) or {}),
        "side": "LONG",
    }


def _serialize_shadow_trade(trade: Any) -> dict[str, Any]:
    return {
        "decision_id": getattr(trade, "decision_id", None),
        "decision_ts": getattr(trade, "decision_ts", None).isoformat()
        if getattr(trade, "decision_ts", None) is not None
        else None,
        "entry_ts": getattr(trade, "entry_ts", None).isoformat()
        if getattr(trade, "entry_ts", None) is not None
        else None,
        "exit_ts": getattr(trade, "exit_ts", None).isoformat()
        if getattr(trade, "exit_ts", None) is not None
        else None,
        "entry_price": getattr(trade, "entry_price", None),
        "exit_price": getattr(trade, "exit_price", None),
        "pnl_points": getattr(trade, "pnl_points", None),
        "pnl_cash": getattr(trade, "pnl_cash", None),
        "exit_reason": getattr(trade, "exit_reason", None),
        "family": getattr(trade, "family", None),
        "side": getattr(trade, "side", None),
        "bars_held_1m": getattr(trade, "bars_held_1m", None),
    }


def normalize_trade_lifecycle_records(
    records: Sequence[dict[str, Any]] | Sequence[Any],
    *,
    entry_model: str,
    pnl_truth_basis: str,
    lifecycle_truth_class: str,
    truth_provenance: dict[str, Any] | None,
    record_source: str,
    source_resolution: str = "INTRABAR",
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    provenance = dict(truth_provenance or {})
    for index, raw_record in enumerate(records):
        record = dict(raw_record)
        trade_id = (
            record.get("trade_id")
            or record.get("decision_id")
            or f"{record_source}:{index + 1}"
        )
        exit_reason = record.get("exit_reason") or record.get("primary_exit_reason")
        realized_pnl = record.get("pnl_cash")
        if realized_pnl is None:
            realized_pnl = record.get("realized_pnl")
        normalized.append(
            {
                "trade_id": trade_id,
                "decision_id": record.get("decision_id"),
                "decision_ts": _maybe_iso_text(
                    record.get("decision_ts")
                    or record.get("decision_timestamp")
                    or record.get("decision_context_timestamp")
                ),
                "entry_ts": _maybe_iso_text(record.get("entry_ts") or record.get("entry_timestamp")),
                "exit_ts": _maybe_iso_text(record.get("exit_ts") or record.get("exit_timestamp")),
                "entry_price": record.get("entry_price"),
                "exit_price": record.get("exit_price"),
                "pnl_points": record.get("pnl_points"),
                "pnl_cash": record.get("pnl_cash"),
                "realized_pnl": realized_pnl,
                "primary_exit_reason": exit_reason,
                "exit_reason": exit_reason,
                "family": record.get("family") or record.get("setup_family"),
                "entry_source_family": record.get("entry_source_family") or record.get("signal_source"),
                "setup_signature": record.get("setup_signature"),
                "setup_state_signature": record.get("setup_state_signature"),
                "side": record.get("side") or record.get("direction"),
                "bars_held_1m": record.get("bars_held_1m"),
                "quantity": record.get("quantity") or record.get("qty"),
                "decision_context_linkage_available": bool(
                    record.get("decision_id")
                    or record.get("decision_ts")
                    or record.get("setup_signature")
                    or record.get("setup_state_signature")
                ),
                "decision_context_linkage_status": (
                    "AVAILABLE"
                    if (
                        record.get("decision_id")
                        or record.get("decision_ts")
                        or record.get("setup_signature")
                        or record.get("setup_state_signature")
                    )
                    else "UNAVAILABLE"
                ),
                "source_resolution": source_resolution,
                "entry_model": entry_model,
                "pnl_truth_basis": pnl_truth_basis,
                "lifecycle_truth_class": lifecycle_truth_class,
                "truth_provenance": dict(provenance),
                "record_source": record_source,
            }
        )
    return normalized


def _trade_event(
    *,
    event_id: str,
    linked_bar_id: str,
    linked_subbar_id: Any,
    event_type: str,
    execution_event_type: str | None,
    side: Any,
    family: Any,
    reason: Any,
    decision_context_timestamp: str | None,
    event_timestamp: str | None,
    source_resolution: str,
    entry_model: str,
    event_price: Any = None,
    vwap_at_event: Any = None,
    acceptance_state: Any = None,
    confirmation_flag: bool | None = None,
    invalidation_reason: Any = None,
    truth_authority: str | None = None,
) -> dict[str, Any]:
    return {
        "event_id": event_id,
        "linked_bar_id": linked_bar_id or None,
        "linked_subbar_id": linked_subbar_id,
        "event_type": event_type,
        "execution_event_type": execution_event_type,
        "side": side,
        "family": family,
        "reason": reason,
        "source_resolution": source_resolution,
        "decision_context_timestamp": decision_context_timestamp,
        "event_timestamp": event_timestamp,
        "entry_model": entry_model,
        "event_price": event_price,
        "vwap_at_event": vwap_at_event,
        "acceptance_state": acceptance_state,
        "confirmation_flag": confirmation_flag,
        "invalidation_reason": invalidation_reason,
        "truth_authority": truth_authority,
    }


def _linked_bar_id_for_timestamp(*, rows: Sequence[dict[str, Any]], timestamp: str | None) -> str | None:
    event_timestamp = _parse_iso_timestamp(timestamp)
    if event_timestamp is None:
        return None
    for row in rows:
        row_start = _parse_iso_timestamp(row.get("start_timestamp"))
        row_end = _parse_iso_timestamp(row.get("end_timestamp") or row.get("timestamp"))
        if row_start is None or row_end is None:
            continue
        if row_start < event_timestamp <= row_end:
            return str(row.get("bar_id") or "") or None
    return None


def _feature_snapshot_value(snapshot: Any, *path: str) -> Any:
    current = snapshot
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _parse_iso_timestamp(value: Any) -> datetime | None:
    text = _maybe_iso_text(value)
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _maybe_iso_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _has_minute_execution_detail(source_bars: Sequence[Bar]) -> bool:
    return any(str(bar.timeframe or "").lower() == "1m" for bar in source_bars)


def _to_research_bar(bar: Bar) -> ResearchBar:
    return ResearchBar(
        instrument=bar.symbol,
        timeframe=bar.timeframe,
        start_ts=bar.start_ts,
        end_ts=bar.end_ts,
        open=float(bar.open),
        high=float(bar.high),
        low=float(bar.low),
        close=float(bar.close),
        volume=int(bar.volume),
        session_label="UNKNOWN",
        session_segment="UNKNOWN",
        source="execution_truth",
        provenance="persisted_runtime_truth",
    )
