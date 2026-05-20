"""Executable paper-runtime adapter for promoted stock-index forced-session lanes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from ..domain.enums import LongEntryFamily, OrderIntentType, PositionSide, ShortEntryFamily
from ..domain.models import Bar, FeaturePacket, SignalPacket, StrategyState
from ..execution.order_models import OrderIntent
from ..strategy.exit_engine import ExitDecision
from ..strategy.strategy_engine import StrategyEngine, _empty_signal_packet_payload
from .index_futures_forced_session_research import label_stock_index_segment, stock_index_trade_date_for_timestamp
from .operational_maturation_runtime import (
    augment_intent_summary_with_b_plus,
    b_plus_diagnostic_payload,
    b_plus_session_key,
    b_plus_setup_score,
    operational_entry_reason,
)


INDEX_FUTURES_FORCED_SESSION_RUNTIME_KIND = "index_futures_forced_session_candidate_runtime"
INDEX_FUTURES_FORCED_SESSION_FAMILY = "index_futures_ny_intraday_forced_core_v2"

INDEX_NY_EARLY_LONG_SOURCE = "indexNyEarlyLongV5"
INDEX_NY_EARLY_SHORT_BREAKDOWN_SOURCE = "indexNyEarlyShortV4"
INDEX_NY_EARLY_SHORT_RECLAIM_FAIL_SOURCE = "indexNyEarlyShortV2"
INDEX_NY_LATE_LONG_SOURCE = "indexNyLateLongV5"
INDEX_NY_LATE_SHORT_BREAKDOWN_SOURCE = "indexNyLateShortV4"
INDEX_US_LATE_LONG_SOURCE = "indexUsLateLongV5"
INDEX_US_LATE_SHORT_RECLAIM_FAIL_SOURCE = "indexUsLateShortV2"


@dataclass(frozen=True)
class IndexForcedSessionRuntimeDefinition:
    source_id: str
    lane_id: str
    segment_id: str
    side: str
    setup_bar_count: int = 4
    fallback_entry_bar: int = 7
    tick_size: float = 0.25
    exit_ema_length: int = 5


INDEX_FORCED_SESSION_RUNTIME_DEFINITIONS: tuple[IndexForcedSessionRuntimeDefinition, ...] = (
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_NY_EARLY_LONG_SOURCE,
        lane_id="US_EARLY_LONG",
        segment_id="US_EARLY",
        side="LONG",
        fallback_entry_bar=8,
    ),
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_NY_EARLY_SHORT_BREAKDOWN_SOURCE,
        lane_id="US_EARLY_SHORT_BREAKDOWN",
        segment_id="US_EARLY",
        side="SHORT",
    ),
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_NY_EARLY_SHORT_RECLAIM_FAIL_SOURCE,
        lane_id="US_EARLY_SHORT_RECLAIM_FAIL",
        segment_id="US_EARLY",
        side="SHORT",
    ),
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_NY_LATE_LONG_SOURCE,
        lane_id="US_MIDDAY_LONG",
        segment_id="US_MIDDAY",
        side="LONG",
        fallback_entry_bar=8,
    ),
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_NY_LATE_SHORT_BREAKDOWN_SOURCE,
        lane_id="US_MIDDAY_SHORT_BREAKDOWN",
        segment_id="US_MIDDAY",
        side="SHORT",
    ),
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_US_LATE_LONG_SOURCE,
        lane_id="US_LATE_LONG",
        segment_id="US_LATE",
        side="LONG",
        fallback_entry_bar=8,
    ),
    IndexForcedSessionRuntimeDefinition(
        source_id=INDEX_US_LATE_SHORT_RECLAIM_FAIL_SOURCE,
        lane_id="US_LATE_SHORT_RECLAIM_FAIL",
        segment_id="US_LATE",
        side="SHORT",
    ),
)

INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE = {
    definition.source_id: definition for definition in INDEX_FORCED_SESSION_RUNTIME_DEFINITIONS
}


class IndexFuturesForcedSessionStrategyEngine(StrategyEngine):
    """StrategyEngine-backed executable adapter for stock-index forced-session lanes."""

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
        if str(current_bar.symbol or "").upper() not in {"ES", "MES", "NQ", "MNQ"}:
            return SignalPacket(**payload)
        if label_stock_index_segment(current_bar.end_ts) != definition.segment_id:
            return SignalPacket(**payload)

        segment_bars = self._segment_bars_for_timestamp(current_bar.end_ts)
        if len(segment_bars) < definition.setup_bar_count:
            return SignalPacket(**payload)

        current_index = len(segment_bars) - 1
        if current_index < definition.setup_bar_count:
            return SignalPacket(**payload)

        operational_reason = operational_entry_reason(
            lane_spec=self._lane_spec,
            segment_bars=segment_bars,
            current_index=current_index,
            setup_bar_count=definition.setup_bar_count,
            tick_size=definition.tick_size,
        )
        if operational_reason is not None:
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

        preferred_trigger = False
        if current_index < definition.fallback_entry_bar - 1:
            preferred_trigger = _preferred_entry_trigger(
                side=definition.side,
                segment_bars=segment_bars,
                current_index=current_index,
                setup_bar_count=definition.setup_bar_count,
            )

        timed_fallback = (
            current_index == definition.fallback_entry_bar - 2
            and not preferred_trigger
            and not _preferred_trigger_seen(
                side=definition.side,
                segment_bars=segment_bars,
                setup_bar_count=definition.setup_bar_count,
                stop_index=current_index,
            )
        )

        if not preferred_trigger and not timed_fallback:
            b_plus_key = b_plus_session_key(
                lane_id=str(getattr(self._lane_spec, "lane_id", "") or definition.lane_id),
                segment_id=definition.segment_id,
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
                    preferred_or_near_trigger=False,
                    fallback_entry_bar=definition.fallback_entry_bar,
                )
                self._latest_b_plus_setup_score = b_plus_diagnostic_payload(
                    result=b_plus_result,
                    bar=current_bar,
                    lane_id=str(getattr(self._lane_spec, "lane_id", "") or definition.lane_id),
                    source_id=definition.source_id,
                    side=definition.side,
                )
                if b_plus_result.b_plus_match:
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
            INDEX_NY_EARLY_LONG_SOURCE,
            INDEX_NY_LATE_LONG_SOURCE,
            INDEX_US_LATE_LONG_SOURCE,
        }:
            return LongEntryFamily.K
        return super()._resolve_long_entry_family(signal_packet)

    def _resolve_short_entry_family(self, signal_packet: SignalPacket) -> ShortEntryFamily:
        if signal_packet.short_entry_source in {
            INDEX_NY_EARLY_SHORT_BREAKDOWN_SOURCE,
            INDEX_NY_EARLY_SHORT_RECLAIM_FAIL_SOURCE,
            INDEX_NY_LATE_SHORT_BREAKDOWN_SOURCE,
            INDEX_US_LATE_SHORT_RECLAIM_FAIL_SOURCE,
        }:
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
            custom_exit = self._forced_session_exit_intent(bar=bar, state=state)
            if custom_exit is not None:
                return custom_exit
            return None
        return super()._maybe_create_order_intent(bar, signal_packet, state, exit_decision)

    def _forced_session_exit_intent(self, *, bar: Bar, state: StrategyState) -> OrderIntent | None:
        definition = self._runtime_definition
        if definition is None or state.open_broker_order_id is not None or not state.exits_enabled:
            return None

        quantity = state.internal_position_qty
        if quantity <= 0:
            return None

        current_segment = label_stock_index_segment(bar.end_ts)
        if current_segment != definition.segment_id:
            return _build_forced_exit_intent(bar=bar, quantity=quantity, side=state.position_side, reason_code="segment_overrun")

        segment_bars = self._segment_bars_for_timestamp(bar.end_ts)
        entry_index = _entry_index_for_state(segment_bars=segment_bars, state=state)
        if entry_index is None or len(segment_bars) < definition.setup_bar_count:
            return None

        current_index = len(segment_bars) - 1
        setup_bars = segment_bars[: definition.setup_bar_count]
        setup_high = max(float(candidate.high) for candidate in setup_bars)
        setup_low = min(float(candidate.low) for candidate in setup_bars)
        closes = [float(candidate.close) for candidate in segment_bars]
        ema_values = _ema_values(closes=closes, length=definition.exit_ema_length)
        current = segment_bars[current_index]
        previous = segment_bars[current_index - 1] if current_index > 0 else current

        should_exit = False
        reason_code = None
        if definition.side == "LONG":
            stop_price = setup_low - definition.tick_size
            if float(current.low) <= stop_price:
                should_exit = True
                reason_code = "forced_session_initial_stop"
            elif current_index > entry_index:
                ema_value = ema_values[current_index]
                if ema_value is not None and float(current.close) < ema_value and float(current.close) < float(previous.low):
                    should_exit = True
                    reason_code = "forced_session_ema_structure_break"
        else:
            stop_price = setup_high + definition.tick_size
            if float(current.high) >= stop_price:
                should_exit = True
                reason_code = "forced_session_initial_stop"
            elif current_index > entry_index:
                ema_value = ema_values[current_index]
                if ema_value is not None and float(current.close) > ema_value and float(current.close) > float(previous.high):
                    should_exit = True
                    reason_code = "forced_session_ema_structure_break"

        if not should_exit and _is_last_bar_of_segment(bar=current, segment_id=definition.segment_id, timeframe=self._primary_context_timeframe):
            should_exit = True
            reason_code = "forced_session_segment_close"

        if not should_exit:
            return None

        return _build_forced_exit_intent(
            bar=bar,
            quantity=quantity,
            side=state.position_side,
            reason_code=reason_code or "forced_session_exit",
        )

    def _segment_bars_for_timestamp(self, timestamp) -> list[Bar]:
        definition = self._runtime_definition
        if definition is None:
            return []
        trade_day = stock_index_trade_date_for_timestamp(timestamp)
        return [
            candidate
            for candidate in self._bar_history
            if stock_index_trade_date_for_timestamp(candidate.end_ts) == trade_day
            and label_stock_index_segment(candidate.end_ts) == definition.segment_id
        ]


def _resolve_runtime_definition(lane_spec: Any) -> IndexForcedSessionRuntimeDefinition | None:
    long_sources = [str(value) for value in getattr(lane_spec, "long_sources", ()) if str(value).strip()]
    short_sources = [str(value) for value in getattr(lane_spec, "short_sources", ()) if str(value).strip()]
    source = long_sources[0] if long_sources else short_sources[0] if short_sources else None
    if source is None:
        return None
    return INDEX_FORCED_SESSION_RUNTIME_BY_SOURCE.get(str(source))


def _preferred_trigger_seen(
    *,
    side: str,
    segment_bars: list[Bar],
    setup_bar_count: int,
    stop_index: int,
) -> bool:
    for index in range(setup_bar_count, max(setup_bar_count, stop_index + 1)):
        if _preferred_entry_trigger(
            side=side,
            segment_bars=segment_bars,
            current_index=index,
            setup_bar_count=setup_bar_count,
        ):
            return True
    return False


def _preferred_entry_trigger(
    *,
    side: str,
    segment_bars: list[Bar],
    current_index: int,
    setup_bar_count: int,
) -> bool:
    if current_index < setup_bar_count or current_index >= len(segment_bars):
        return False
    setup_bars = segment_bars[:setup_bar_count]
    setup_high = max(float(candidate.high) for candidate in setup_bars)
    setup_low = min(float(candidate.low) for candidate in setup_bars)
    midpoint = setup_low + ((setup_high - setup_low) * 0.5)
    previous = segment_bars[current_index - 1]
    current = segment_bars[current_index]
    if str(side).upper() == "LONG":
        return float(previous.low) <= midpoint and float(current.close) > float(previous.high)
    return float(previous.high) >= midpoint and float(current.close) < float(previous.low)


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


def _ema_values(*, closes: list[float], length: int) -> list[float | None]:
    if not closes or length <= 0:
        return [None for _ in closes]
    alpha = 2.0 / (float(length) + 1.0)
    ema: float | None = None
    output: list[float | None] = []
    for close in closes:
        ema = float(close) if ema is None else ((float(close) - ema) * alpha) + ema
        output.append(round(ema, 6))
    return output


def _is_last_bar_of_segment(*, bar: Bar, segment_id: str, timeframe: str) -> bool:
    from ..market_data.timeframes import timeframe_minutes

    local_end = bar.end_ts
    next_end = local_end + timedelta(minutes=timeframe_minutes(timeframe))
    return label_stock_index_segment(next_end) != segment_id


def _build_forced_exit_intent(
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
