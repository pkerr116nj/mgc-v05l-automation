"""Runtime adapter for the GC/MGC London-open acceptance continuation temp paper branch."""

from __future__ import annotations

from datetime import time
from decimal import Decimal
from typing import Any

from ..domain.enums import OrderIntentType
from ..domain.models import Bar, FeaturePacket, SignalPacket, StrategyState
from ..strategy.exit_engine import ExitDecision
from ..strategy.strategy_engine import StrategyEngine, _empty_signal_packet_payload
from .session_phase_labels import label_session_phase


GC_MGC_LONDON_OPEN_ACCEPTANCE_SOURCE = "gc_mgc_london_open_acceptance_continuation_long"
GC_MGC_LONDON_OPEN_ACCEPTANCE_FIRST_THREE_BARS = (time(3, 5), time(3, 10), time(3, 15))
GC_MGC_LONDON_OPEN_ACCEPTANCE_SLOPE_MIN = Decimal("0.25")
GC_MGC_LONDON_OPEN_ACCEPTANCE_SLOPE_MAX = Decimal("1.10")
GC_MGC_LONDON_OPEN_ACCEPTANCE_EXPANSION_MIN = Decimal("0.10")
GC_MGC_LONDON_OPEN_ACCEPTANCE_EXPANSION_MAX = Decimal("0.50")
GC_MGC_LONDON_OPEN_ACCEPTANCE_CLOSE_BUFFER_ATR = Decimal("0.35")


class GcMgcLondonOpenAcceptanceContinuationStrategyEngine(StrategyEngine):
    """Persist real runtime bars/features/signals while staying non-execution-authority."""

    def _evaluate_signals(self, feature_packet: FeaturePacket, feature_history: list[FeaturePacket]) -> SignalPacket:
        payload = _empty_signal_packet_payload(feature_packet.bar_id)
        if len(self._bar_history) < 3 or len(feature_history) < 3:
            return SignalPacket(**payload)

        signal_bar = self._bar_history[-1]
        breakout_bar = self._bar_history[-2]
        prior_bar = self._bar_history[-3]
        breakout_feature = feature_history[-2]

        if str(signal_bar.symbol or "").upper() not in {"GC", "MGC"}:
            return SignalPacket(**payload)

        local_signal_dt = signal_bar.end_ts.astimezone(self._settings.timezone_info)
        if label_session_phase(signal_bar.end_ts) != "LONDON_OPEN":
            return SignalPacket(**payload)
        if local_signal_dt.time() not in GC_MGC_LONDON_OPEN_ACCEPTANCE_FIRST_THREE_BARS:
            return SignalPacket(**payload)

        breakout_atr = max(breakout_feature.atr, self._settings.risk_floor)
        breakout_level = breakout_bar.high
        breakout_normalized_slope = breakout_feature.velocity / breakout_atr
        breakout_range_expansion_ratio = (
            breakout_feature.bar_range / breakout_atr if breakout_atr > 0 else Decimal("0")
        )

        if breakout_bar.high <= prior_bar.high:
            return SignalPacket(**payload)
        if breakout_bar.close < prior_bar.close:
            return SignalPacket(**payload)
        if not (GC_MGC_LONDON_OPEN_ACCEPTANCE_SLOPE_MIN <= breakout_normalized_slope <= GC_MGC_LONDON_OPEN_ACCEPTANCE_SLOPE_MAX):
            return SignalPacket(**payload)
        if not (
            GC_MGC_LONDON_OPEN_ACCEPTANCE_EXPANSION_MIN
            <= breakout_range_expansion_ratio
            <= GC_MGC_LONDON_OPEN_ACCEPTANCE_EXPANSION_MAX
        ):
            return SignalPacket(**payload)
        if signal_bar.low < breakout_level:
            return SignalPacket(**payload)
        if signal_bar.close < breakout_level + (GC_MGC_LONDON_OPEN_ACCEPTANCE_CLOSE_BUFFER_ATR * breakout_feature.atr):
            return SignalPacket(**payload)

        payload.update(
            {
                "long_entry_raw": True,
                "recent_long_setup": True,
                "long_entry": True,
                "long_entry_source": GC_MGC_LONDON_OPEN_ACCEPTANCE_SOURCE,
            }
        )
        return SignalPacket(**payload)

    def _maybe_create_order_intent(
        self,
        bar: Bar,
        signal_packet: SignalPacket,
        state: StrategyState,
        exit_decision: ExitDecision,
    ):
        if signal_packet.long_entry and state.same_underlying_entry_hold:
            hold_reason = (
                str(state.same_underlying_hold_reason or "").strip()
                or f"New entries held by operator for same-underlying conflict review on {self._settings.symbol}."
            )
            self._log_same_underlying_entry_block(
                bar=bar,
                intent_type=OrderIntentType.BUY_TO_OPEN,
                source=signal_packet.long_entry_source,
                reason=hold_reason,
            )
        return None


def gc_mgc_london_open_acceptance_window_matches(*, end_ts, timezone_info) -> bool:
    local_dt = end_ts.astimezone(timezone_info)
    return label_session_phase(end_ts) == "LONDON_OPEN" and local_dt.time() in GC_MGC_LONDON_OPEN_ACCEPTANCE_FIRST_THREE_BARS
