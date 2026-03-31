"""StrategyEngine-backed approved quant runtime adapter."""

from __future__ import annotations

from typing import Any

from ...domain.enums import LongEntryFamily, ShortEntryFamily
from ...domain.models import FeaturePacket, SignalPacket
from ...market_data.bar_builder import BarBuilder
from ...research.bar_resampling import build_resampled_bars
from ...research.quant_futures import HIGHER_TIMEFRAMES, _FrameSeries, _align_timestamps, _build_feature_rows
from ...strategy.strategy_engine import StrategyEngine, _empty_signal_packet_payload
from .evaluator import lane_rejection_reason
from .specs import ApprovedQuantLaneSpec

APPROVED_QUANT_HISTORY_RESTORE_BARS = 8000


class ApprovedQuantStrategyEngine(StrategyEngine):
    """Run approved quant families inside the standard completed-bar engine shell."""

    def __init__(self, *, quant_spec: ApprovedQuantLaneSpec, **kwargs: Any) -> None:
        self._quant_spec = quant_spec
        settings = kwargs["settings"]
        self._bar_builder = BarBuilder(settings)
        super().__init__(**kwargs)

    def _restore_processing_context(self) -> None:
        if self._repositories is None:
            return
        restore_limit = max(self._settings.warmup_bars_required(), APPROVED_QUANT_HISTORY_RESTORE_BARS)
        recent_bars = self._repositories.bars.list_recent(
            symbol=self._settings.symbol,
            timeframe=self._settings.timeframe,
            limit=restore_limit,
        )
        if not recent_bars:
            return
        self._bar_history = recent_bars
        recent_features = self._repositories.features.load_by_bar_ids([bar.bar_id for bar in recent_bars])
        self._feature_history = recent_features
        if recent_features:
            self._last_feature_packet = recent_features[-1]

    def _evaluate_signals(self, feature_packet: FeaturePacket, feature_history: list[FeaturePacket]) -> SignalPacket:
        payload = _empty_signal_packet_payload(feature_packet.bar_id)
        quant_feature = self._latest_quant_feature()
        if not quant_feature or not bool(quant_feature.get("ready")):
            return SignalPacket(**payload)

        rejection_reason = lane_rejection_reason(
            spec=self._quant_spec,
            session_label=str(quant_feature.get("session_label") or "UNKNOWN"),
            feature=quant_feature,
        )
        if rejection_reason is not None:
            return SignalPacket(**payload)

        source = self._quant_spec.family
        if str(self._quant_spec.direction or "").upper() == "LONG":
            payload.update(
                {
                    "long_entry_raw": True,
                    "recent_long_setup": True,
                    "long_entry": True,
                    "long_entry_source": source,
                }
            )
        else:
            payload.update(
                {
                    "short_entry_raw": True,
                    "recent_short_setup": True,
                    "short_entry": True,
                    "short_entry_source": source,
                }
            )
        return SignalPacket(**payload)

    def _resolve_long_entry_family(self, signal_packet: SignalPacket) -> LongEntryFamily:
        if signal_packet.long_entry_source == self._quant_spec.family:
            return LongEntryFamily.K
        return super()._resolve_long_entry_family(signal_packet)

    def _resolve_short_entry_family(self, signal_packet: SignalPacket) -> ShortEntryFamily:
        if signal_packet.short_entry_source == self._quant_spec.family:
            return ShortEntryFamily.FAILED_MOVE_REVERSAL_SHORT
        return super()._resolve_short_entry_family(signal_packet)

    def _latest_quant_feature(self) -> dict[str, Any] | None:
        if not self._bar_history:
            return None
        execution = _FrameSeries.from_bars(self._bar_history)
        higher: dict[str, _FrameSeries] = {}
        for timeframe in HIGHER_TIMEFRAMES:
            resampled = build_resampled_bars(
                self._bar_history,
                target_timeframe=timeframe,
                bar_builder=self._bar_builder,
            ).bars
            if not resampled:
                return None
            higher[timeframe] = _FrameSeries.from_bars(resampled)
        alignments = {
            timeframe: _align_timestamps(execution.timestamps, higher[timeframe].timestamps)
            for timeframe in HIGHER_TIMEFRAMES
        }
        feature_rows = _build_feature_rows(
            execution=execution,
            higher=higher,
            alignments=alignments,
        )
        if not feature_rows:
            return None
        return dict(feature_rows[-1])
