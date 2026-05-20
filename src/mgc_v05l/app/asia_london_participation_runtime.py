"""Executable paper-runtime adapter for promoted Asia-to-London participation lanes."""

from __future__ import annotations

import csv
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from ..domain.enums import LongEntryFamily, OrderIntentType, PositionSide, ShortEntryFamily
from ..domain.models import Bar, FeaturePacket, SignalPacket, StrategyState
from ..execution.order_models import OrderIntent
from ..strategy.exit_engine import ExitDecision
from ..strategy.strategy_engine import StrategyEngine, _empty_signal_packet_payload
from .gc_mgc_london_late_long_research import _ema as _long_ema
from .gc_mgc_london_late_long_research import _find_exit as _find_long_exit
from .gc_mgc_ny_early_short_research import NyEarlyShortSpec
from .gc_mgc_ny_early_short_research import _find_exit as _find_short_exit
from .gc_mgc_segment_forced_session_long_research import (
    ForcedSegmentLongSpec,
    _select_entry_index as _select_long_entry_index,
)
from .gc_mgc_segment_forced_session_short_research import (
    ForcedSegmentShortSpec,
    _select_entry_index as _select_short_entry_index,
)
from .gc_mgc_segment_regime_research import label_gold_segment, trade_date_for_timestamp
from .index_futures_forced_session_research import label_stock_index_segment, stock_index_trade_date_for_timestamp
from .operational_maturation_runtime import (
    augment_intent_summary_with_b_plus,
    b_plus_can_promote_to_order_intent,
    b_plus_diagnostic_payload,
    b_plus_session_key,
    b_plus_setup_score,
    emit_b_plus_diagnostic,
    operational_entry_reason,
)


ASIA_LONDON_PARTICIPATION_RUNTIME_KIND = "asia_london_participation_candidate_runtime"
ASIA_LONDON_PARTICIPATION_FAMILY = "asia_london_participation_core_v1"

GC_ASIA_LONDON_LONG_V5_SOURCE = "gcAsiaLondonLongV5"
GC_ASIA_LONDON_SHORT_V2_SOURCE = "gcAsiaLondonShortV2"
NQ_ASIA_LONDON_LONG_V6_SOURCE = "nqAsiaLondonLongV6"
NQ_ASIA_LONDON_LONG_V5_SOURCE = "nqAsiaLondonLongV5"
NQ_ASIA_LONDON_SHORT_V2_SOURCE = "nqAsiaLondonShortV2"
ES_ASIA_LONDON_LONG_V6_VOL_FLOOR_125_SOURCE = "esAsiaLondonLongV6VolFloor125"

ENTRY_SEGMENT = "ASIA_EARLY"
HOLD_SEGMENTS: tuple[str, ...] = ("ASIA_EARLY", "ASIA_LATE", "LONDON_EARLY", "LONDON_LATE")
ASIA_LONDON_INSTRUMENTATION_DIRNAME = "strategy_activity_instrumentation"
ASIA_LONDON_LIVE_PREDICATE_TRACE = "asia_london_live_predicate_trace.jsonl"
ASIA_LONDON_LIVE_NEAR_MISS_TRACE = "asia_london_live_near_miss_trace.csv"
ASIA_LONDON_SCORE_BUCKET_TRACE = "asia_london_score_bucket_live_observation.csv"


@dataclass(frozen=True)
class AsiaLondonParticipationRuntimeDefinition:
    source_id: str
    lane_id: str
    side: str
    symbol_group: str
    variant_id: str
    tick_size: float
    setup_bar_count: int = 4
    fallback_entry_bar: int = 7
    exit_ema_length: int = 5
    min_setup_range_atr_ratio: float | None = None


ASIA_LONDON_PARTICIPATION_RUNTIME_DEFINITIONS: tuple[AsiaLondonParticipationRuntimeDefinition, ...] = (
    AsiaLondonParticipationRuntimeDefinition(
        source_id=GC_ASIA_LONDON_LONG_V5_SOURCE,
        lane_id="ASIA_LONDON_LONG_V5",
        side="LONG",
        symbol_group="GC_MGC",
        variant_id="segment_forced_long_v5_dip_reclaim_or_bar8",
        tick_size=0.1,
        fallback_entry_bar=8,
    ),
    AsiaLondonParticipationRuntimeDefinition(
        source_id=GC_ASIA_LONDON_SHORT_V2_SOURCE,
        lane_id="ASIA_LONDON_SHORT_V2",
        side="SHORT",
        symbol_group="GC_MGC",
        variant_id="segment_forced_short_v2_reclaim_fail_or_bar7",
        tick_size=0.1,
        fallback_entry_bar=7,
    ),
    AsiaLondonParticipationRuntimeDefinition(
        source_id=NQ_ASIA_LONDON_LONG_V6_SOURCE,
        lane_id="ASIA_LONDON_LONG_V6",
        side="LONG",
        symbol_group="NQ_MNQ",
        variant_id="segment_forced_long_v6_contextual_fallback",
        tick_size=0.25,
        fallback_entry_bar=8,
    ),
    AsiaLondonParticipationRuntimeDefinition(
        source_id=NQ_ASIA_LONDON_LONG_V5_SOURCE,
        lane_id="ASIA_LONDON_LONG_V5",
        side="LONG",
        symbol_group="NQ_MNQ",
        variant_id="segment_forced_long_v5_dip_reclaim_or_bar8",
        tick_size=0.25,
        fallback_entry_bar=8,
    ),
    AsiaLondonParticipationRuntimeDefinition(
        source_id=NQ_ASIA_LONDON_SHORT_V2_SOURCE,
        lane_id="ASIA_LONDON_SHORT_V2",
        side="SHORT",
        symbol_group="NQ_MNQ",
        variant_id="segment_forced_short_v2_reclaim_fail_or_bar7",
        tick_size=0.25,
        fallback_entry_bar=7,
    ),
    AsiaLondonParticipationRuntimeDefinition(
        source_id=ES_ASIA_LONDON_LONG_V6_VOL_FLOOR_125_SOURCE,
        lane_id="ASIA_LONDON_LONG_V6_VOL_FLOOR_125",
        side="LONG",
        symbol_group="ES_ONLY",
        variant_id="segment_forced_long_v6_contextual_fallback",
        tick_size=0.25,
        fallback_entry_bar=8,
        min_setup_range_atr_ratio=1.25,
    ),
)

ASIA_LONDON_RUNTIME_BY_SOURCE = {
    definition.source_id: definition for definition in ASIA_LONDON_PARTICIPATION_RUNTIME_DEFINITIONS
}


class AsiaLondonParticipationStrategyEngine(StrategyEngine):
    """StrategyEngine-backed executable adapter for Asia-to-London participation lanes."""

    def __init__(self, *, lane_spec: Any, **kwargs: Any) -> None:
        self._lane_spec = lane_spec
        self._runtime_definition = _resolve_runtime_definition(lane_spec)
        super().__init__(**kwargs)

    def _evaluate_signals(self, feature_packet: FeaturePacket, feature_history: list[FeaturePacket]) -> SignalPacket:
        del feature_history
        payload = _empty_signal_packet_payload(feature_packet.bar_id)
        definition = self._runtime_definition
        if definition is None or not self._bar_history:
            return SignalPacket(**payload)

        current_bar = self._bar_history[-1]
        if not _symbol_matches_definition(symbol=str(current_bar.symbol or ""), definition=definition):
            return SignalPacket(**payload)
        lane_id = str(getattr(self._lane_spec, "lane_id", "") or definition.lane_id)
        session_label = _label_segment_for_symbol(symbol=str(current_bar.symbol or ""), timestamp=current_bar.end_ts)
        if session_label != ENTRY_SEGMENT:
            _record_asia_london_live_observation(
                _build_asia_london_live_observation(
                    lane_id=lane_id,
                    definition=definition,
                    current_bar=current_bar,
                    session_label=session_label,
                    segment_bars=[],
                    strict_gate_pass=False,
                    strict_gate_fail_reason="outside_entry_segment",
                    entry_index=None,
                    entry_reason="outside_entry_segment",
                    floor_reason=None,
                )
            )
            return SignalPacket(**payload)

        segment_bars = self._entry_segment_bars_for_timestamp(current_bar.end_ts)
        current_index = len(segment_bars) - 1
        operational_reason = operational_entry_reason(
            lane_spec=self._lane_spec,
            segment_bars=segment_bars,
            current_index=current_index,
            setup_bar_count=definition.setup_bar_count,
            tick_size=definition.tick_size,
        )
        if operational_reason is not None:
            _record_asia_london_live_observation(
                _build_asia_london_live_observation(
                    lane_id=lane_id,
                    definition=definition,
                    current_bar=current_bar,
                    session_label=session_label,
                    segment_bars=segment_bars,
                    strict_gate_pass=True,
                    strict_gate_fail_reason=None,
                    entry_index=current_index,
                    entry_reason=operational_reason,
                    floor_reason=None,
                )
            )
            payload.update(
                {
                    "long_entry_raw": definition.side == "LONG",
                    "short_entry_raw": definition.side == "SHORT",
                    "recent_long_setup": definition.side == "LONG",
                    "recent_short_setup": definition.side == "SHORT",
                    "long_entry": definition.side == "LONG",
                    "short_entry": definition.side == "SHORT",
                    "long_entry_source": definition.source_id if definition.side == "LONG" else None,
                    "short_entry_source": definition.source_id if definition.side == "SHORT" else None,
                }
            )
            return SignalPacket(**payload)
        if len(segment_bars) <= definition.fallback_entry_bar:
            _record_asia_london_live_observation(
                _build_asia_london_live_observation(
                    lane_id=lane_id,
                    definition=definition,
                    current_bar=current_bar,
                    session_label=session_label,
                    segment_bars=segment_bars,
                    strict_gate_pass=False,
                    strict_gate_fail_reason="insufficient_setup_bars",
                    entry_index=None,
                    entry_reason="insufficient_setup_bars",
                    floor_reason=None,
                )
            )
            return SignalPacket(**payload)

        entry_index, entry_reason = _entry_selection(
            definition=definition,
            segment_bars=segment_bars,
        )
        floor_reason = _volatility_floor_reason(definition=definition, segment_bars=segment_bars)
        strict_gate_pass = entry_index == len(segment_bars) - 1 and floor_reason is None
        strict_gate_fail_reason = None
        if entry_index != len(segment_bars) - 1:
            strict_gate_fail_reason = "entry_not_latest_bar"
        elif floor_reason is not None:
            strict_gate_fail_reason = floor_reason
        _record_asia_london_live_observation(
            _build_asia_london_live_observation(
                lane_id=lane_id,
                definition=definition,
                current_bar=current_bar,
                session_label=session_label,
                segment_bars=segment_bars,
                strict_gate_pass=strict_gate_pass,
                strict_gate_fail_reason=strict_gate_fail_reason,
                entry_index=entry_index,
                entry_reason=entry_reason,
                floor_reason=floor_reason,
            )
        )
        if not strict_gate_pass:
            b_plus_key = b_plus_session_key(
                lane_id=str(getattr(self._lane_spec, "lane_id", "") or definition.lane_id),
                segment_id=ENTRY_SEGMENT,
                bar=current_bar,
            )
            used_b_plus_keys = getattr(self, "_b_plus_session_keys", set())
            if b_plus_key not in used_b_plus_keys:
                b_plus_result = b_plus_setup_score(
                    lane_spec=self._lane_spec,
                    segment_bars=segment_bars,
                    current_index=current_index,
                    setup_bar_count=definition.setup_bar_count,
                    tick_size=definition.tick_size,
                    side=definition.side,
                    exact_match=False,
                    preferred_or_near_trigger=entry_index is not None and abs(entry_index - current_index) <= 2,
                    fallback_entry_bar=definition.fallback_entry_bar,
                )
                self._latest_b_plus_setup_score = b_plus_diagnostic_payload(
                    result=b_plus_result,
                    bar=current_bar,
                    lane_id=str(getattr(self._lane_spec, "lane_id", "") or definition.lane_id),
                    source_id=definition.source_id,
                    side=definition.side,
                )
                emit_b_plus_diagnostic(lane_spec=self._lane_spec, payload=self._latest_b_plus_setup_score)
                if b_plus_result.b_plus_match and b_plus_can_promote_to_order_intent(self):
                    used_b_plus_keys = set(used_b_plus_keys)
                    used_b_plus_keys.add(b_plus_key)
                    self._b_plus_session_keys = used_b_plus_keys
                    payload.update(
                        {
                            "long_entry_raw": definition.side == "LONG",
                            "short_entry_raw": definition.side == "SHORT",
                            "recent_long_setup": definition.side == "LONG",
                            "recent_short_setup": definition.side == "SHORT",
                            "long_entry": definition.side == "LONG",
                            "short_entry": definition.side == "SHORT",
                            "long_entry_source": definition.source_id if definition.side == "LONG" else None,
                            "short_entry_source": definition.source_id if definition.side == "SHORT" else None,
                        }
                    )
                    return SignalPacket(**payload)
        if entry_index != len(segment_bars) - 1:
            return SignalPacket(**payload)
        if floor_reason is not None:
            return SignalPacket(**payload)

        payload.update(
            {
                "long_entry_raw": definition.side == "LONG",
                "short_entry_raw": definition.side == "SHORT",
                "recent_long_setup": definition.side == "LONG",
                "recent_short_setup": definition.side == "SHORT",
                "long_entry": definition.side == "LONG",
                "short_entry": definition.side == "SHORT",
                "long_entry_source": definition.source_id if definition.side == "LONG" else None,
                "short_entry_source": definition.source_id if definition.side == "SHORT" else None,
            }
        )
        return SignalPacket(**payload)

    def _build_live_intent_summary(self, **kwargs: Any) -> dict[str, object]:
        return augment_intent_summary_with_b_plus(super()._build_live_intent_summary(**kwargs), self, kwargs["bar"])

    def _build_shadow_intent_summary(self, **kwargs: Any) -> dict[str, object]:
        return augment_intent_summary_with_b_plus(super()._build_shadow_intent_summary(**kwargs), self, kwargs["bar"])

    def _resolve_long_entry_family(self, signal_packet: SignalPacket) -> LongEntryFamily:
        if signal_packet.long_entry_source in {
            GC_ASIA_LONDON_LONG_V5_SOURCE,
            NQ_ASIA_LONDON_LONG_V6_SOURCE,
            NQ_ASIA_LONDON_LONG_V5_SOURCE,
        }:
            return LongEntryFamily.K
        return super()._resolve_long_entry_family(signal_packet)

    def _resolve_short_entry_family(self, signal_packet: SignalPacket) -> ShortEntryFamily:
        if signal_packet.short_entry_source in {GC_ASIA_LONDON_SHORT_V2_SOURCE, NQ_ASIA_LONDON_SHORT_V2_SOURCE}:
            return ShortEntryFamily.FAILED_MOVE_REVERSAL_SHORT
        return super()._resolve_short_entry_family(signal_packet)

    def _maybe_create_order_intent(
        self,
        bar: Bar,
        signal_packet: SignalPacket,
        state: StrategyState,
        exit_decision: ExitDecision,
    ):
        if state.position_side != PositionSide.FLAT:
            custom_exit = self._participation_exit_intent(bar=bar, state=state)
            if custom_exit is not None:
                return custom_exit
            return None
        return super()._maybe_create_order_intent(bar, signal_packet, state, exit_decision)

    def _participation_exit_intent(self, *, bar: Bar, state: StrategyState) -> OrderIntent | None:
        definition = self._runtime_definition
        if definition is None or state.open_broker_order_id is not None or not state.exits_enabled:
            return None
        quantity = state.internal_position_qty
        if quantity <= 0:
            return None

        current_segment = _label_segment_for_symbol(symbol=str(bar.symbol or ""), timestamp=bar.end_ts)
        hold_bars = self._hold_bars_for_timestamp(bar.end_ts)
        entry_segment_bars = self._entry_segment_bars_for_timestamp(bar.end_ts)
        if not hold_bars or len(entry_segment_bars) < definition.setup_bar_count:
            return None
        entry_index = _entry_index_for_state(segment_bars=hold_bars, state=state)
        if entry_index is None:
            return None

        setup_bars = entry_segment_bars[: definition.setup_bar_count]
        setup_high = max(float(candidate.high) for candidate in setup_bars)
        setup_low = min(float(candidate.low) for candidate in setup_bars)
        closes = [float(candidate.close) for candidate in hold_bars]
        ema_values = _long_ema(closes, length=definition.exit_ema_length)
        current_index = len(hold_bars) - 1
        current = hold_bars[current_index]
        previous = hold_bars[current_index - 1] if current_index > 0 else current

        if definition.side == "LONG":
            stop_price = round(float(setup_low - definition.tick_size), 4)
            spec = ForcedSegmentLongSpec(
                variant_id=definition.variant_id,
                description=definition.variant_id,
                tick_size=definition.tick_size,
                fallback_entry_bar=definition.fallback_entry_bar,
                exit_ema_length=definition.exit_ema_length,
            )
            historical_exit_index, _exit_price, historical_exit_reason = _find_long_exit(
                segment_bars=hold_bars,
                entry_index=entry_index,
                stop_price=stop_price,
                ema_values=ema_values,
                spec=spec,
            )
            exit_reason = _current_long_exit_reason(
                current=current,
                previous=previous,
                current_index=current_index,
                entry_index=entry_index,
                stop_price=stop_price,
                ema_values=ema_values,
                spec=spec,
            )
        else:
            stop_price = round(float(setup_high + definition.tick_size), 4)
            spec = NyEarlyShortSpec(
                variant_id=definition.variant_id,
                description=definition.variant_id,
                setup_family="asia_london_participation",
                tick_size=definition.tick_size,
                exit_ema_length=definition.exit_ema_length,
            )
            historical_exit_index, _exit_price, historical_exit_reason = _find_short_exit(
                segment_bars=hold_bars,
                entry_index=entry_index,
                stop_price=stop_price,
                ema_values=ema_values,
                spec=spec,
            )
            exit_reason = _current_short_exit_reason(
                current=current,
                previous=previous,
                current_index=current_index,
                entry_index=entry_index,
                stop_price=stop_price,
                ema_values=ema_values,
                spec=spec,
            )

        if exit_reason is None and _is_adopted_broker_truth_entry(state=state, repositories=getattr(self, "_repositories", None)):
            if historical_exit_index < current_index:
                exit_reason = f"adopted_{historical_exit_reason}_catchup"
            elif current_segment not in HOLD_SEGMENTS:
                exit_reason = f"adopted_{historical_exit_reason}_catchup"

        if exit_reason is None and current_segment not in HOLD_SEGMENTS:
            exit_reason = "segment_overrun"

        if exit_reason is None and _is_last_bar_of_hold_window(
            bar=current,
            symbol=str(current.symbol or ""),
            timeframe=getattr(self, "_primary_context_timeframe", current.timeframe),
        ):
            exit_reason = "segment_close"

        if exit_reason is None:
            return None

        return _build_exit_intent(
            bar=bar,
            quantity=quantity,
            side=state.position_side,
            reason_code=f"asia_london_{exit_reason}",
        )

    def _entry_segment_bars_for_timestamp(self, timestamp: datetime) -> list[Bar]:
        trade_day = _trade_day_for_symbol(symbol=self._lane_spec.symbol, timestamp=timestamp)
        return [
            candidate
            for candidate in self._bar_history
            if _trade_day_for_symbol(symbol=candidate.symbol, timestamp=candidate.end_ts) == trade_day
            and _label_segment_for_symbol(symbol=str(candidate.symbol or ""), timestamp=candidate.end_ts) == ENTRY_SEGMENT
        ]

    def _hold_bars_for_timestamp(self, timestamp: datetime) -> list[Bar]:
        trade_day = _trade_day_for_symbol(symbol=self._lane_spec.symbol, timestamp=timestamp)
        return [
            candidate
            for candidate in self._bar_history
            if _trade_day_for_symbol(symbol=candidate.symbol, timestamp=candidate.end_ts) == trade_day
            and _label_segment_for_symbol(symbol=str(candidate.symbol or ""), timestamp=candidate.end_ts) in HOLD_SEGMENTS
        ]


def _resolve_runtime_definition(lane_spec: Any) -> AsiaLondonParticipationRuntimeDefinition | None:
    long_sources = [str(value) for value in getattr(lane_spec, "long_sources", ()) if str(value).strip()]
    short_sources = [str(value) for value in getattr(lane_spec, "short_sources", ()) if str(value).strip()]
    source = long_sources[0] if long_sources else short_sources[0] if short_sources else None
    if source is None:
        return None
    return ASIA_LONDON_RUNTIME_BY_SOURCE.get(str(source))


def _symbol_matches_definition(*, symbol: str, definition: AsiaLondonParticipationRuntimeDefinition) -> bool:
    normalized = str(symbol or "").upper()
    if definition.symbol_group == "GC_MGC":
        return normalized in {"GC", "MGC"}
    if definition.symbol_group == "NQ_MNQ":
        return normalized in {"NQ", "MNQ"}
    if definition.symbol_group == "ES_ONLY":
        return normalized == "ES"
    return False


def _label_segment_for_symbol(*, symbol: str, timestamp: datetime) -> str:
    normalized = str(symbol or "").upper()
    if normalized in {"GC", "MGC"}:
        return label_gold_segment(timestamp)
    return label_stock_index_segment(timestamp)


def _trade_day_for_symbol(*, symbol: str, timestamp: datetime):
    normalized = str(symbol or "").upper()
    if normalized in {"GC", "MGC"}:
        return trade_date_for_timestamp(timestamp)
    return stock_index_trade_date_for_timestamp(timestamp)


def _entry_selection(
    *,
    definition: AsiaLondonParticipationRuntimeDefinition,
    segment_bars: list[Bar],
) -> tuple[int, str]:
    setup_bars = segment_bars[: definition.setup_bar_count]
    setup_high = max(float(bar.high) for bar in setup_bars)
    setup_low = min(float(bar.low) for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 1e-9)
    setup_return = float(setup_bars[-1].close) - float(setup_bars[0].open)
    setup_close_location = (float(setup_bars[-1].close) - setup_low) / setup_range
    setup_vwap = _bars_vwap(setup_bars)
    setup_vwap_displacement = (float(setup_bars[-1].close) - setup_vwap) / setup_range
    if definition.side == "LONG":
        spec = ForcedSegmentLongSpec(
            variant_id=definition.variant_id,
            description=definition.variant_id,
            tick_size=definition.tick_size,
            fallback_entry_bar=definition.fallback_entry_bar,
            exit_ema_length=definition.exit_ema_length,
        )
        return _select_long_entry_index(
            segment_bars=segment_bars,
            segment_id=ENTRY_SEGMENT,
            spec=spec,
            setup_low=setup_low,
            setup_high=setup_high,
            setup_close_location=setup_close_location,
            setup_vwap_displacement=setup_vwap_displacement,
            setup_return=setup_return,
            setup_range=setup_range,
        )
    spec = ForcedSegmentShortSpec(
        variant_id=definition.variant_id,
        description=definition.variant_id,
        tick_size=definition.tick_size,
        fallback_entry_bar=definition.fallback_entry_bar,
        exit_ema_length=definition.exit_ema_length,
    )
    return _select_short_entry_index(
        segment_bars=segment_bars,
        segment_id=ENTRY_SEGMENT,
        spec=spec,
        setup_low=setup_low,
        setup_high=setup_high,
        setup_close_location=setup_close_location,
        setup_vwap_displacement=setup_vwap_displacement,
        setup_return=setup_return,
        setup_range=setup_range,
    )


def _bars_vwap(bars: list[Bar]) -> float:
    total_volume = sum(max(float(getattr(bar, "volume", 0.0) or 0.0), 1.0) for bar in bars)
    total_pv = sum(float(bar.close) * max(float(getattr(bar, "volume", 0.0) or 0.0), 1.0) for bar in bars)
    return total_pv / max(total_volume, 1.0)


def _entry_index_for_state(*, segment_bars: list[Bar], state: StrategyState) -> int | None:
    if state.entry_bar_id:
        for index, candidate in enumerate(segment_bars):
            if candidate.bar_id == state.entry_bar_id:
                return index
    if state.entry_timestamp is None:
        return None
    for index, candidate in enumerate(segment_bars):
        if candidate.end_ts >= state.entry_timestamp:
            return index
    return None


def _current_long_exit_reason(
    *,
    current: Bar,
    previous: Bar,
    current_index: int,
    entry_index: int,
    stop_price: float,
    ema_values: list[float | None],
    spec: ForcedSegmentLongSpec,
) -> str | None:
    if float(current.low) <= stop_price:
        return "initial_stop"
    if spec.exit_mode == "ema_structure" and current_index > entry_index:
        ema_value = ema_values[current_index]
        if ema_value is not None and float(current.close) < ema_value and float(current.close) < float(previous.low):
            return "ema_structure_break"
    return None


def _current_short_exit_reason(
    *,
    current: Bar,
    previous: Bar,
    current_index: int,
    entry_index: int,
    stop_price: float,
    ema_values: list[float | None],
    spec: NyEarlyShortSpec,
) -> str | None:
    if float(current.high) >= stop_price:
        return "initial_stop"
    if spec.exit_mode == "ema_structure" and current_index > entry_index:
        ema_value = ema_values[current_index]
        if ema_value is not None and float(current.close) > ema_value and float(current.close) > float(previous.high):
            return "ema_structure_break"
    return None


def _is_last_bar_of_hold_window(*, bar: Bar, symbol: str, timeframe: str) -> bool:
    from ..market_data.timeframes import timeframe_minutes

    next_end = bar.end_ts + timedelta(minutes=timeframe_minutes(timeframe))
    return _label_segment_for_symbol(symbol=symbol, timestamp=next_end) not in HOLD_SEGMENTS


def _is_adopted_broker_truth_entry(*, state: StrategyState, repositories: Any | None) -> bool:
    if repositories is None or state.entry_bar_id is None:
        return False
    try:
        fills = repositories.fills.list_all()
    except Exception:
        return False
    entry_prefix = f"{state.entry_bar_id}|"
    for row in reversed(fills):
        broker_order_id = str(row.get("broker_order_id") or "")
        if not broker_order_id.startswith("adopted-broker-truth-"):
            continue
        intent_type = str(row.get("intent_type") or "").upper()
        if intent_type not in {"BUY_TO_OPEN", "SELL_TO_OPEN"}:
            continue
        order_intent_id = str(row.get("order_intent_id") or "")
        if order_intent_id == state.entry_bar_id or order_intent_id.startswith(entry_prefix):
            return True
    return False


def _build_exit_intent(
    *,
    bar: Bar,
    quantity: int,
    side: PositionSide,
    reason_code: str,
) -> OrderIntent:
    intent_type = OrderIntentType.SELL_TO_CLOSE if side == PositionSide.LONG else OrderIntentType.BUY_TO_CLOSE
    return OrderIntent(
        order_intent_id=f"{bar.bar_id}|{intent_type.value}",
        bar_id=bar.bar_id,
        symbol=bar.symbol,
        intent_type=intent_type,
        quantity=quantity,
        created_at=bar.end_ts,
        reason_code=reason_code,
    )


def _volatility_floor_reason(
    *,
    definition: AsiaLondonParticipationRuntimeDefinition,
    segment_bars: list[Bar],
) -> str | None:
    floor = definition.min_setup_range_atr_ratio
    if floor is None or floor <= 0:
        return None
    ratio = _setup_range_atr_ratio(segment_bars=segment_bars, setup_bar_count=definition.setup_bar_count)
    if ratio is None:
        return "volatility_floor_insufficient_bars"
    if ratio >= floor:
        return None
    return f"volatility_floor_failed:{round(ratio,4)}<{floor}"


def _setup_range_atr_ratio(*, segment_bars: list[Bar], setup_bar_count: int) -> float | None:
    setup_bars = list(segment_bars[:setup_bar_count])
    if len(setup_bars) < setup_bar_count:
        return None
    true_ranges: list[float] = []
    previous_close: float | None = None
    for bar in segment_bars:
        high = float(bar.high)
        low = float(bar.low)
        if previous_close is None:
            true_range = high - low
        else:
            true_range = max(high - low, abs(high - previous_close), abs(low - previous_close))
        true_ranges.append(true_range)
        previous_close = float(bar.close)
    atr_window = sorted(true_ranges[max(0, setup_bar_count - 8) : setup_bar_count])
    if not atr_window:
        return None
    mid = len(atr_window) // 2
    atr_value = atr_window[mid] if len(atr_window) % 2 == 1 else (atr_window[mid - 1] + atr_window[mid]) / 2.0
    atr_value = max(float(atr_value), 1e-9)
    setup_high = max(float(bar.high) for bar in setup_bars)
    setup_low = min(float(bar.low) for bar in setup_bars)
    setup_range = max(setup_high - setup_low, 0.0)
    return setup_range / atr_value


def _asia_london_instrumentation_root() -> Path:
    override_raw = str(os.environ.get("MGC_ASIA_LONDON_INSTRUMENTATION_DIR") or "").strip()
    if override_raw:
        return Path(override_raw).expanduser().resolve()
    return (Path.cwd() / "outputs" / "reports" / ASIA_LONDON_INSTRUMENTATION_DIRNAME).resolve()


def _asia_london_instrumentation_enabled() -> bool:
    raw_enabled = str(os.environ.get("MGC_ASIA_LONDON_INSTRUMENTATION_ENABLED") or "").strip().lower()
    if raw_enabled in {"1", "true", "yes", "on"}:
        return True
    return bool(str(os.environ.get("MGC_ASIA_LONDON_INSTRUMENTATION_DIR") or "").strip())


def _append_jsonl_record(path: Path, row: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True))
        handle.write("\n")
    return path


def _append_csv_record(path: Path, *, fieldnames: list[str], row: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow({name: row.get(name) for name in fieldnames})
    return path


def _write_observation_artifact_best_effort(writer: Any, *args: Any, **kwargs: Any) -> None:
    try:
        writer(*args, **kwargs)
    except (OSError, TimeoutError):
        # Instrumentation must never block or abort live paper runtime restore/startup.
        return


def _score_bucket_for_ratio(score_ratio: float, *, strict_gate_pass: bool) -> str:
    if strict_gate_pass:
        return "A+"
    if score_ratio >= 0.8:
        return "A"
    if score_ratio >= 0.6:
        return "B+"
    if score_ratio >= 0.4:
        return "B"
    return "rejected"


def _rounded_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _build_asia_london_live_observation(
    *,
    lane_id: str,
    definition: AsiaLondonParticipationRuntimeDefinition,
    current_bar: Bar,
    session_label: str,
    segment_bars: list[Bar],
    strict_gate_pass: bool,
    strict_gate_fail_reason: str | None,
    entry_index: int | None,
    entry_reason: str,
    floor_reason: str | None,
) -> dict[str, Any]:
    setup_ready = len(segment_bars) > definition.fallback_entry_bar
    latest_index = len(segment_bars) - 1 if segment_bars else None
    setup_high = None
    setup_low = None
    setup_range = None
    setup_return = None
    setup_close_location = None
    setup_vwap_displacement = None
    midpoint = None
    current_close = None
    previous_high = None
    previous_low = None
    breakout_confirmed = False
    breakdown_confirmed = False
    dip_reclaim_confirmed = False
    reclaim_fail_confirmed = False
    contextual_tight_close = False
    contextual_wide_range = False
    contextual_positive_setup = False
    contextual_weak_reclaim = False
    floor_ratio = None
    if setup_ready:
        setup_bars = segment_bars[: definition.setup_bar_count]
        setup_high = max(float(bar.high) for bar in setup_bars)
        setup_low = min(float(bar.low) for bar in setup_bars)
        setup_range = max(setup_high - setup_low, 1e-9)
        setup_return = float(setup_bars[-1].close) - float(setup_bars[0].open)
        setup_close_location = (float(setup_bars[-1].close) - setup_low) / setup_range
        setup_vwap = _bars_vwap(setup_bars)
        setup_vwap_displacement = (float(setup_bars[-1].close) - setup_vwap) / setup_range
        midpoint = setup_low + (setup_range / 2.0)
        current_close = float(segment_bars[-1].close)
        if len(segment_bars) >= 2:
            previous_bar = segment_bars[-2]
            previous_high = float(previous_bar.high)
            previous_low = float(previous_bar.low)
            dip_reclaim_confirmed = previous_low <= midpoint and current_close > previous_high
            reclaim_fail_confirmed = previous_high >= midpoint and current_close < previous_low
        breakout_confirmed = current_close >= setup_high + definition.tick_size
        breakdown_confirmed = current_close <= setup_low - definition.tick_size
        contextual_tight_close = (
            setup_close_location is not None
            and setup_vwap_displacement is not None
            and setup_close_location <= 0.72
            and setup_vwap_displacement <= 0.15
        )
        contextual_wide_range = setup_range >= 3.0
        contextual_positive_setup = (
            setup_vwap_displacement is not None
            and setup_return is not None
            and setup_vwap_displacement <= 0.15
            and setup_return >= 1.0
        )
        contextual_weak_reclaim = (
            setup_close_location is not None
            and setup_range is not None
            and setup_close_location <= 0.35
            and setup_range >= 4.0
        )
        floor_ratio = _setup_range_atr_ratio(segment_bars=segment_bars, setup_bar_count=definition.setup_bar_count)

    predicate_results: dict[str, bool] = {
        "in_entry_segment": session_label == ENTRY_SEGMENT,
        "setup_bars_ready": setup_ready,
        "selected_entry_matches_latest_bar": entry_index == latest_index if latest_index is not None and entry_index is not None else False,
        "volatility_floor_pass": floor_reason is None,
        "breakout_confirmed": breakout_confirmed,
        "breakdown_confirmed": breakdown_confirmed,
        "dip_reclaim_confirmed": dip_reclaim_confirmed,
        "reclaim_fail_confirmed": reclaim_fail_confirmed,
        "contextual_tight_close": contextual_tight_close,
        "contextual_wide_range": contextual_wide_range,
        "contextual_positive_setup": contextual_positive_setup,
        "contextual_weak_reclaim": contextual_weak_reclaim,
    }
    predicate_values = {
        "entry_reason": entry_reason,
        "strict_gate_fail_reason": strict_gate_fail_reason,
        "setup_high": _rounded_or_none(setup_high),
        "setup_low": _rounded_or_none(setup_low),
        "setup_range": _rounded_or_none(setup_range),
        "setup_return": _rounded_or_none(setup_return),
        "setup_close_location": _rounded_or_none(setup_close_location),
        "setup_vwap_displacement": _rounded_or_none(setup_vwap_displacement),
        "setup_midpoint": _rounded_or_none(midpoint),
        "current_close": _rounded_or_none(current_close),
        "previous_high": _rounded_or_none(previous_high),
        "previous_low": _rounded_or_none(previous_low),
        "entry_index": entry_index,
        "latest_segment_index": latest_index,
        "fallback_entry_bar": definition.fallback_entry_bar,
        "volatility_floor_ratio": _rounded_or_none(floor_ratio),
        "volatility_floor_min_ratio": definition.min_setup_range_atr_ratio,
    }
    passed_predicates = sum(1 for value in predicate_results.values() if value)
    total_predicates = len(predicate_results)
    score_ratio = passed_predicates / max(total_predicates, 1)
    bucket = _score_bucket_for_ratio(score_ratio, strict_gate_pass=strict_gate_pass)
    research_candidate = bucket in {"A+", "A", "B+"}
    return {
        "observed_at": current_bar.end_ts.isoformat(),
        "timestamp": current_bar.end_ts.isoformat(),
        "lane_id": lane_id,
        "instrument": str(current_bar.symbol or "").upper(),
        "session_label": session_label,
        "strict_gate_pass": strict_gate_pass,
        "strict_gate_fail_reason": strict_gate_fail_reason,
        "near_miss_score": _rounded_or_none(score_ratio),
        "predicates_passed": passed_predicates,
        "predicate_count": total_predicates,
        "candidate_score_bucket": bucket,
        "current_strict_candidate": strict_gate_pass,
        "research_score_candidate": research_candidate,
        "entry_reason": entry_reason,
        "floor_reason": floor_reason,
        "predicate_results": predicate_results,
        "predicate_values": predicate_values,
        "forward_return_label_available": False,
    }


def _record_asia_london_live_observation(row: dict[str, Any]) -> None:
    if not _asia_london_instrumentation_enabled():
        return
    root = _asia_london_instrumentation_root()
    _write_observation_artifact_best_effort(_append_jsonl_record, root / ASIA_LONDON_LIVE_PREDICATE_TRACE, row)
    failed_predicates = [
        name for name, passed in dict(row.get("predicate_results") or {}).items() if not bool(passed)
    ]
    _write_observation_artifact_best_effort(
        _append_csv_record,
        root / ASIA_LONDON_LIVE_NEAR_MISS_TRACE,
        fieldnames=[
            "timestamp",
            "lane_id",
            "instrument",
            "session_label",
            "strict_gate_pass",
            "strict_gate_fail_reason",
            "predicates_passed",
            "predicate_count",
            "near_miss_score",
            "candidate_score_bucket",
            "current_strict_candidate",
            "research_score_candidate",
            "entry_reason",
            "floor_reason",
            "failed_predicates",
        ],
        row={
            "timestamp": row.get("timestamp"),
            "lane_id": row.get("lane_id"),
            "instrument": row.get("instrument"),
            "session_label": row.get("session_label"),
            "strict_gate_pass": row.get("strict_gate_pass"),
            "strict_gate_fail_reason": row.get("strict_gate_fail_reason"),
            "predicates_passed": row.get("predicates_passed"),
            "predicate_count": row.get("predicate_count"),
            "near_miss_score": row.get("near_miss_score"),
            "candidate_score_bucket": row.get("candidate_score_bucket"),
            "current_strict_candidate": row.get("current_strict_candidate"),
            "research_score_candidate": row.get("research_score_candidate"),
            "entry_reason": row.get("entry_reason"),
            "floor_reason": row.get("floor_reason"),
            "failed_predicates": "|".join(failed_predicates),
        },
    )
    _write_observation_artifact_best_effort(
        _append_csv_record,
        root / ASIA_LONDON_SCORE_BUCKET_TRACE,
        fieldnames=[
            "timestamp",
            "lane_id",
            "instrument",
            "candidate_score_bucket",
            "near_miss_score",
            "predicates_passed",
            "predicate_count",
            "current_strict_candidate",
            "research_score_candidate",
        ],
        row={
            "timestamp": row.get("timestamp"),
            "lane_id": row.get("lane_id"),
            "instrument": row.get("instrument"),
            "candidate_score_bucket": row.get("candidate_score_bucket"),
            "near_miss_score": row.get("near_miss_score"),
            "predicates_passed": row.get("predicates_passed"),
            "predicate_count": row.get("predicate_count"),
            "current_strict_candidate": row.get("current_strict_candidate"),
            "research_score_candidate": row.get("research_score_candidate"),
        },
    )
