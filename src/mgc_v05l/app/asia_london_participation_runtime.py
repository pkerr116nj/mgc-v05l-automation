"""Executable paper-runtime adapter for promoted Asia-to-London participation lanes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
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
        payload = _empty_signal_packet_payload(feature_packet.bar_id)
        definition = self._runtime_definition
        if definition is None or not self._bar_history:
            return SignalPacket(**payload)

        current_bar = self._bar_history[-1]
        if not _symbol_matches_definition(symbol=str(current_bar.symbol or ""), definition=definition):
            return SignalPacket(**payload)
        if _label_segment_for_symbol(symbol=str(current_bar.symbol or ""), timestamp=current_bar.end_ts) != ENTRY_SEGMENT:
            return SignalPacket(**payload)

        segment_bars = self._entry_segment_bars_for_timestamp(current_bar.end_ts)
        if len(segment_bars) <= definition.fallback_entry_bar:
            return SignalPacket(**payload)

        entry_index, entry_reason = _entry_selection(
            definition=definition,
            segment_bars=segment_bars,
        )
        if entry_index != len(segment_bars) - 1:
            return SignalPacket(**payload)
        floor_reason = _volatility_floor_reason(definition=definition, segment_bars=segment_bars)
        if floor_reason is not None:
            payload["analysis_notes"] = (entry_reason, floor_reason)
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
                "analysis_notes": (entry_reason,),
            }
        )
        return SignalPacket(**payload)

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

        if definition.side == "LONG":
            stop_price = round(float(setup_low - definition.tick_size), 4)
            spec = ForcedSegmentLongSpec(
                variant_id=definition.variant_id,
                description=definition.variant_id,
                tick_size=definition.tick_size,
                fallback_entry_bar=definition.fallback_entry_bar,
                exit_ema_length=definition.exit_ema_length,
            )
            exit_index, _exit_price, exit_reason = _find_long_exit(
                segment_bars=hold_bars,
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
            exit_index, _exit_price, exit_reason = _find_short_exit(
                segment_bars=hold_bars,
                entry_index=entry_index,
                stop_price=stop_price,
                ema_values=ema_values,
                spec=spec,
            )

        if exit_index != current_index:
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
