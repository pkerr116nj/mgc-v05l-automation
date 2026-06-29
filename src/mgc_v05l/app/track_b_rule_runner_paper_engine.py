"""Track B rule-runner adapter for guarded PAPER probationary lanes.

This adapter gives a promoted Track B strategy an explicit bridge into the
existing PAPER StrategyEngine lifecycle without changing live rules or bypassing
the normal broker/lifecycle gates. It reads a configured Track B state artifact,
evaluates the configured rule runner, and emits a standard StrategyEngine
SignalPacket only when the rule emits a timestamp-coherent PAPER signal.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from ..domain.enums import LongEntryFamily, OrderIntentType, ShortEntryFamily
from ..domain.models import FeaturePacket, SignalPacket
from ..execution.order_models import OrderIntent
from ..strategy.strategy_engine import StrategyEngine, _empty_signal_packet_payload
from ..execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)
from ..execution_core.track_b_session_anchor_resolver import (
    SessionAnchorStatus,
    SessionAnchorType,
    TrackBSessionAnchorConfig,
    resolve_session_anchor,
)
from ..execution_core.track_b_broker_event_envelope import (
    BrokerEventEnvelopeConfig,
    BrokerEventEnvelopeLaneContext,
    broker_event_report_fields,
    build_broker_event_envelope,
    write_broker_event_envelope,
)


TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND = "track_b_rule_runner_paper_strategy_engine"
DEFAULT_TRACK_B_RULE_RUNNER_SIGNAL_FRESHNESS_SECONDS = 360.0
CHANGEOVER_0300_LONG_CONTINUATION_ID = "CHANGEOVER_0300_LONG_CONTINUATION_ABOVE_SESSION_OPEN_SHADOW_V1"
CHANGEOVER_0300_MES_LONG_CONTINUATION_ID = "CHANGEOVER_0300_MES_LONG_CONTINUATION_ABOVE_SESSION_OPEN_SHADOW_V1"
CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID = "CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ABOVE_REFERENCE_SHADOW_V1"
CHANGEOVER_0700_MES_LONG_CONTINUATION_ID = "CHANGEOVER_0700_MES_LONG_CONTINUATION_ABOVE_REFERENCE_SHADOW_V1"
US_SESSION_MNQ_LONG_CONTINUATION_ID = "US_SESSION_MNQ_LONG_CONTINUATION_ABOVE_OPEN_SHADOW_V1"
US_SESSION_MES_LONG_CONTINUATION_ID = "US_SESSION_MES_LONG_CONTINUATION_ABOVE_OPEN_SHADOW_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_US_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MES_US_LONG_ID = "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MES_US_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MES_US_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_LONG_ID = "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_ID = "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_ID = "PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_PARTICIPATION_SHORT_V1"
NEW_YORK_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class _ChangeoverContinuationSpec:
    strategy_id: str
    overlay_label: str
    start_hour_et: int
    end_hour_et: int
    benchmark_hold_bars_5m: int
    condition_label: str
    use_0300_reference: bool = False


@dataclass(frozen=True)
class _UsSessionContinuationSpec:
    strategy_id: str
    overlay_label: str
    start_time_et: time
    end_time_et: time
    benchmark_hold_bars_5m: int
    condition_label: str


@dataclass(frozen=True)
class _PaperActiveEvidenceSpec:
    strategy_id: str
    direction: str
    overlay_label: str
    start_time_et: time
    end_time_et: time
    benchmark_hold_bars_5m: int
    condition_label: str
    reference_time_et: time = time(9, 30)
    reference_label: str = "09_30_us_session_open"
    require_reference_bar: bool = True


CHANGEOVER_CONTINUATION_SPECS: dict[str, _ChangeoverContinuationSpec] = {
    CHANGEOVER_0300_LONG_CONTINUATION_ID: _ChangeoverContinuationSpec(
        strategy_id=CHANGEOVER_0300_LONG_CONTINUATION_ID,
        overlay_label="ASIA_TO_EUROPE_CHANGEOVER",
        start_hour_et=3,
        end_hour_et=4,
        benchmark_hold_bars_5m=72,
        condition_label="03:00-04:00_ET_close_above_18:00_ET_session_open",
    ),
    CHANGEOVER_0300_MES_LONG_CONTINUATION_ID: _ChangeoverContinuationSpec(
        strategy_id=CHANGEOVER_0300_MES_LONG_CONTINUATION_ID,
        overlay_label="ASIA_TO_EUROPE_CHANGEOVER",
        start_hour_et=3,
        end_hour_et=4,
        benchmark_hold_bars_5m=72,
        condition_label="03:00-04:00_ET_close_above_18:00_ET_session_open",
    ),
    CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID: _ChangeoverContinuationSpec(
        strategy_id=CHANGEOVER_0700_MNQ_LONG_CONTINUATION_ID,
        overlay_label="EUROPE_TO_US_CHANGEOVER",
        start_hour_et=7,
        end_hour_et=8,
        benchmark_hold_bars_5m=48,
        condition_label="07:00-08:00_ET_close_above_18:00_ET_session_open_or_03:00_reference",
        use_0300_reference=True,
    ),
    CHANGEOVER_0700_MES_LONG_CONTINUATION_ID: _ChangeoverContinuationSpec(
        strategy_id=CHANGEOVER_0700_MES_LONG_CONTINUATION_ID,
        overlay_label="EUROPE_TO_US_CHANGEOVER",
        start_hour_et=7,
        end_hour_et=8,
        benchmark_hold_bars_5m=48,
        condition_label="07:00-08:00_ET_close_above_18:00_ET_session_open_or_03:00_reference",
        use_0300_reference=True,
    ),
}

US_SESSION_CONTINUATION_SPECS: dict[str, _UsSessionContinuationSpec] = {
    US_SESSION_MNQ_LONG_CONTINUATION_ID: _UsSessionContinuationSpec(
        strategy_id=US_SESSION_MNQ_LONG_CONTINUATION_ID,
        overlay_label="US_SESSION_CONTINUATION",
        start_time_et=time(9, 30),
        end_time_et=time(12, 0),
        benchmark_hold_bars_5m=24,
        condition_label="09:30-12:00_ET_close_above_09:30_ET_us_session_open",
    ),
    US_SESSION_MES_LONG_CONTINUATION_ID: _UsSessionContinuationSpec(
        strategy_id=US_SESSION_MES_LONG_CONTINUATION_ID,
        overlay_label="US_SESSION_CONTINUATION",
        start_time_et=time(9, 30),
        end_time_et=time(12, 0),
        benchmark_hold_bars_5m=24,
        condition_label="09:30-12:00_ET_close_above_09:30_ET_us_session_open",
    ),
}

PAPER_ACTIVE_EVIDENCE_SPECS: dict[str, _PaperActiveEvidenceSpec] = {
    PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_US_LONG_ID,
        direction="LONG",
        overlay_label="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(9, 35),
        end_time_et=time(15, 30),
        benchmark_hold_bars_5m=12,
        condition_label="09:35-15:30_ET_close_above_vwap_or_us_session_open",
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_US_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(9, 35),
        end_time_et=time(15, 30),
        benchmark_hold_bars_5m=12,
        condition_label="09:35-15:30_ET_close_below_vwap_or_us_session_open",
    ),
    PAPER_ACTIVE_EVIDENCE_MES_US_LONG_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_US_LONG_ID,
        direction="LONG",
        overlay_label="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(9, 35),
        end_time_et=time(15, 30),
        benchmark_hold_bars_5m=12,
        condition_label="09:35-15:30_ET_close_above_vwap_or_us_session_open",
    ),
    PAPER_ACTIVE_EVIDENCE_MES_US_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_US_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(9, 35),
        end_time_et=time(15, 30),
        benchmark_hold_bars_5m=12,
        condition_label="09:35-15:30_ET_close_below_vwap_or_us_session_open",
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_LONG_ID,
        direction="LONG",
        overlay_label="PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(18, 5),
        end_time_et=time(3, 0),
        benchmark_hold_bars_5m=12,
        condition_label="18:05-03:00_ET_close_above_vwap_or_globex_session_open",
        reference_time_et=time(18, 0),
        reference_label="18_00_globex_session_open",
        require_reference_bar=False,
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(18, 5),
        end_time_et=time(3, 0),
        benchmark_hold_bars_5m=12,
        condition_label="18:05-03:00_ET_close_below_vwap_or_globex_session_open",
        reference_time_et=time(18, 0),
        reference_label="18_00_globex_session_open",
        require_reference_bar=False,
    ),
    PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_LONG_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_LONG_ID,
        direction="LONG",
        overlay_label="PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(18, 5),
        end_time_et=time(3, 0),
        benchmark_hold_bars_5m=12,
        condition_label="18:05-03:00_ET_close_above_vwap_or_globex_session_open",
        reference_time_et=time(18, 0),
        reference_label="18_00_globex_session_open",
        require_reference_bar=False,
    ),
    PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(18, 5),
        end_time_et=time(3, 0),
        benchmark_hold_bars_5m=12,
        condition_label="18:05-03:00_ET_close_below_vwap_or_globex_session_open",
        reference_time_et=time(18, 0),
        reference_label="18_00_globex_session_open",
        require_reference_bar=False,
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_ID,
        direction="LONG",
        overlay_label="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(3, 5),
        end_time_et=time(5, 30),
        benchmark_hold_bars_5m=12,
        condition_label="03:05-05:30_ET_close_above_vwap_or_london_open",
        reference_time_et=time(3, 0),
        reference_label="03_00_london_open",
        require_reference_bar=True,
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(3, 5),
        end_time_et=time(5, 30),
        benchmark_hold_bars_5m=12,
        condition_label="03:05-05:30_ET_close_below_vwap_or_london_open",
        reference_time_et=time(3, 0),
        reference_label="03_00_london_open",
        require_reference_bar=True,
    ),
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_ID,
        direction="LONG",
        overlay_label="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(3, 5),
        end_time_et=time(5, 30),
        benchmark_hold_bars_5m=12,
        condition_label="03:05-05:30_ET_close_above_vwap_or_london_open",
        reference_time_et=time(3, 0),
        reference_label="03_00_london_open",
        require_reference_bar=True,
    ),
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(3, 5),
        end_time_et=time(5, 30),
        benchmark_hold_bars_5m=12,
        condition_label="03:05-05:30_ET_close_below_vwap_or_london_open",
        reference_time_et=time(3, 0),
        reference_label="03_00_london_open",
        require_reference_bar=True,
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(5, 30),
        end_time_et=time(8, 20),
        benchmark_hold_bars_5m=12,
        condition_label="05:30-08:20_ET_close_below_vwap_or_london_late_reference",
        reference_time_et=time(5, 30),
        reference_label="05_30_london_late_reference",
        require_reference_bar=True,
    ),
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_ID: _PaperActiveEvidenceSpec(
        strategy_id=PAPER_ACTIVE_EVIDENCE_MES_LONDON_LATE_SHORT_ID,
        direction="SHORT",
        overlay_label="PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE",
        start_time_et=time(5, 30),
        end_time_et=time(8, 20),
        benchmark_hold_bars_5m=12,
        condition_label="05:30-08:20_ET_close_below_vwap_or_london_late_reference",
        reference_time_et=time(5, 30),
        reference_label="05_30_london_late_reference",
        require_reference_bar=True,
    ),
}


_BATCH1_ACTIVE_EVIDENCE_SYMBOLS = (
    "MGC",
    "GC",
    "NQ",
    "ES",
    "ZT",
    "ZF",
    "ZN",
    "ZB",
)


def _batch1_active_evidence_specs() -> dict[str, _PaperActiveEvidenceSpec]:
    specs: dict[str, _PaperActiveEvidenceSpec] = {}
    for symbol in _BATCH1_ACTIVE_EVIDENCE_SYMBOLS:
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_US_PARTICIPATION_LONG_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_US_PARTICIPATION_LONG_V1",
            direction="LONG",
            overlay_label="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(9, 35),
            end_time_et=time(15, 30),
            benchmark_hold_bars_5m=12,
            condition_label="09:35-15:30_ET_close_above_vwap_or_us_session_open",
        )
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_US_PARTICIPATION_SHORT_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_US_PARTICIPATION_SHORT_V1",
            direction="SHORT",
            overlay_label="PAPER_ONLY_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(9, 35),
            end_time_et=time(15, 30),
            benchmark_hold_bars_5m=12,
            condition_label="09:35-15:30_ET_close_below_vwap_or_us_session_open",
        )
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_GLOBEX_PARTICIPATION_LONG_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_GLOBEX_PARTICIPATION_LONG_V1",
            direction="LONG",
            overlay_label="PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(18, 5),
            end_time_et=time(3, 0),
            benchmark_hold_bars_5m=12,
            condition_label="18:05-03:00_ET_close_above_vwap_or_globex_session_open",
            reference_time_et=time(18, 0),
            reference_label="18_00_globex_session_open",
            require_reference_bar=False,
        )
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_GLOBEX_PARTICIPATION_SHORT_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_GLOBEX_PARTICIPATION_SHORT_V1",
            direction="SHORT",
            overlay_label="PAPER_ONLY_GLOBEX_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(18, 5),
            end_time_et=time(3, 0),
            benchmark_hold_bars_5m=12,
            condition_label="18:05-03:00_ET_close_below_vwap_or_globex_session_open",
            reference_time_et=time(18, 0),
            reference_label="18_00_globex_session_open",
            require_reference_bar=False,
        )
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_LONDON_OPEN_PARTICIPATION_LONG_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_LONDON_OPEN_PARTICIPATION_LONG_V1",
            direction="LONG",
            overlay_label="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(3, 5),
            end_time_et=time(5, 30),
            benchmark_hold_bars_5m=12,
            condition_label="03:05-05:30_ET_close_above_vwap_or_london_open",
            reference_time_et=time(3, 0),
            reference_label="03_00_london_open",
            require_reference_bar=True,
        )
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_LONDON_OPEN_PARTICIPATION_SHORT_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_LONDON_OPEN_PARTICIPATION_SHORT_V1",
            direction="SHORT",
            overlay_label="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(3, 5),
            end_time_et=time(5, 30),
            benchmark_hold_bars_5m=12,
            condition_label="03:05-05:30_ET_close_below_vwap_or_london_open",
            reference_time_et=time(3, 0),
            reference_label="03_00_london_open",
            require_reference_bar=True,
        )
        specs[f"PAPER_ACTIVE_EVIDENCE_{symbol}_LONDON_LATE_PARTICIPATION_SHORT_V1"] = _PaperActiveEvidenceSpec(
            strategy_id=f"PAPER_ACTIVE_EVIDENCE_{symbol}_LONDON_LATE_PARTICIPATION_SHORT_V1",
            direction="SHORT",
            overlay_label="PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE",
            start_time_et=time(5, 30),
            end_time_et=time(8, 20),
            benchmark_hold_bars_5m=12,
            condition_label="05:30-08:20_ET_close_below_vwap_or_london_late_reference",
            reference_time_et=time(5, 30),
            reference_label="05_30_london_late_reference",
            require_reference_bar=True,
        )
    return specs


PAPER_ACTIVE_EVIDENCE_SPECS.update(_batch1_active_evidence_specs())


@dataclass(frozen=True)
class _NativeRuntimeFallbackSpec:
    promoted_source: str
    direction: str
    native_sources: tuple[str, ...]
    short_family: ShortEntryFamily | None = None


_NATIVE_RUNTIME_FALLBACKS: dict[str, _NativeRuntimeFallbackSpec] = {
    "FIRST_BULL_SNAP_TURN_V1": _NativeRuntimeFallbackSpec(
        promoted_source="FIRST_BULL_SNAP_TURN_V1",
        direction="LONG",
        native_sources=("firstBullSnapTurn",),
    ),
    "FIRST_BEAR_SNAP_TURN_V1": _NativeRuntimeFallbackSpec(
        promoted_source="FIRST_BEAR_SNAP_TURN_V1",
        direction="SHORT",
        native_sources=("firstBearSnapTurn",),
        short_family=ShortEntryFamily.BEAR_SNAP,
    ),
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": _NativeRuntimeFallbackSpec(
        promoted_source="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        direction="LONG",
        native_sources=("asiaEarlyNormalBreakoutRetestHoldTurn",),
    ),
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": _NativeRuntimeFallbackSpec(
        promoted_source="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        direction="SHORT",
        native_sources=(
            "asiaEarlyPauseResumeShortTurn",
            "asiaEarlyCompressedPauseResumeShortTurn",
        ),
        short_family=ShortEntryFamily.ASIA_EARLY_PAUSE_RESUME_SHORT,
    ),
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1": _NativeRuntimeFallbackSpec(
        promoted_source="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        direction="SHORT",
        native_sources=("londonLatePauseResumeShortTurn",),
        short_family=ShortEntryFamily.LONDON_LATE_PAUSE_RESUME_SHORT,
    ),
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1": _NativeRuntimeFallbackSpec(
        promoted_source="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        direction="LONG",
        native_sources=("asiaLateFlatPullbackPauseResumeLongTurn",),
    ),
    "US_DERIVATIVE_BEAR_TURN_V1": _NativeRuntimeFallbackSpec(
        promoted_source="US_DERIVATIVE_BEAR_TURN_V1",
        direction="SHORT",
        native_sources=("usDerivativeBearTurn",),
        short_family=ShortEntryFamily.DERIVATIVE_BEAR,
    ),
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1": _NativeRuntimeFallbackSpec(
        promoted_source="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        direction="SHORT",
        native_sources=("usDerivativeBearTurn",),
        short_family=ShortEntryFamily.DERIVATIVE_BEAR,
    ),
    "MNQ_US_MIDDAY_PAUSE_RESUME_SHORT_TURN_V1": _NativeRuntimeFallbackSpec(
        promoted_source="MNQ_US_MIDDAY_PAUSE_RESUME_SHORT_TURN_V1",
        direction="SHORT",
        native_sources=(
            "usMiddayPauseResumeShortTurn",
            "usMiddayExpandedPauseResumeShortTurn",
            "usMiddayCompressedPauseResumeShortTurn",
        ),
        short_family=ShortEntryFamily.MIDDAY_PAUSE_RESUME_SHORT,
    ),
    "US_LATE_PAUSE_RESUME_LONG_V1": _NativeRuntimeFallbackSpec(
        promoted_source="US_LATE_PAUSE_RESUME_LONG_V1",
        direction="LONG",
        native_sources=("usLatePauseResumeLongTurn",),
    ),
}


class TrackBRuleRunnerPaperStrategyEngine(StrategyEngine):
    """Run a promoted Track B rule inside the standard PAPER StrategyEngine shell."""

    def __init__(self, *, lane_spec: Any, repo_root: Path | None = None, **kwargs: Any) -> None:
        self._track_b_lane_spec = lane_spec
        self._track_b_repo_root = (repo_root or Path(__file__).resolve().parents[3]).resolve()
        self._latest_track_b_rule_report: dict[str, Any] = {}
        super().__init__(**kwargs)

    def latest_track_b_rule_runner_report(self) -> dict[str, Any]:
        return dict(self._latest_track_b_rule_report)

    def _evaluate_signals(self, feature_packet: FeaturePacket, feature_history: list[FeaturePacket]) -> SignalPacket:
        payload = _empty_signal_packet_payload(feature_packet.bar_id)
        config = self._track_b_rule_runner_config()
        changeover_packet = self._evaluate_changeover_runtime_rule(feature_packet)
        if changeover_packet is not None:
            return changeover_packet
        us_continuation_packet = self._evaluate_us_session_continuation_runtime_rule(feature_packet)
        if us_continuation_packet is not None:
            return us_continuation_packet
        active_evidence_packet = self._evaluate_paper_active_evidence_runtime_rule(feature_packet)
        if active_evidence_packet is not None:
            return active_evidence_packet
        event_path = self._resolve_repo_path(config.get("input_event_path") or config.get("state_artifact_path"))
        event_payload = _read_json(event_path)
        current_bar = self._bar_history[-1] if self._bar_history else None
        freshness_blocker = _state_freshness_blocker(event_payload, current_bar_end=getattr(current_bar, "end_ts", None), config=config)
        if freshness_blocker is not None:
            native_fallback = self._evaluate_native_runtime_fallback(
                feature_packet,
                feature_history,
                stale_input_blocker=freshness_blocker,
                input_event_path=event_path,
            )
            if native_fallback is not None:
                return native_fallback
            self._latest_track_b_rule_report = {
                "classification": "TRACK_B_RULE_RUNNER_PAPER_NO_SIGNAL",
                "primary_blocker": freshness_blocker,
                "input_event_path": str(event_path),
                "submit_allowed": False,
                "live_money_eligible": False,
                "paper_proof_invoked": False,
            }
            return SignalPacket(**payload)

        result = run_track_b_strategy_rule(
            input_event_payload=event_payload,
            input_event_path=event_path,
            inbox_dir=self._resolve_repo_path(config.get("inbox_dir") or "outputs/track_b_execution_core/listener_inbox"),
            expected_account_id=str(config.get("expected_account_id") or "DUM882026"),
            source_id=str(config.get("source_id") or self._track_b_lane_spec.lane_id),
            strategy_id=str(config.get("strategy_id") or self._track_b_lane_spec.standalone_strategy_id or self._track_b_lane_spec.lane_id),
            lane_id=str(config.get("lane_id") or self._track_b_lane_spec.lane_id),
            rule_id=str(config.get("rule_id") or self._track_b_lane_spec.strategy_family),
            rule_mode=str(config.get("rule_mode") or self._track_b_lane_spec.strategy_family),
            emit_signal=False,
            allow_fixture_input=False,
            output_root=self._resolve_repo_path(config.get("output_root") or "outputs/track_b_execution_core/track_b_strategy_rule_runner"),
            runner_id=f"{self._track_b_lane_spec.lane_id}_{feature_packet.bar_id}",
        )
        report = dict(result.report)
        self._latest_track_b_rule_report = {
            **report,
            "paper_runtime_adapter": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
            "submit_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        if result.verdict != TrackBStrategyRuleRunnerVerdict.NO_SIGNAL:
            return SignalPacket(**payload)
        if report.get("decision_reason") != "Rule conditions passed, but --emit-signal was not supplied.":
            return SignalPacket(**payload)
        rule_evaluation = dict(report.get("rule_evaluation") or {})
        direction = str(report.get("signal_direction") or rule_evaluation.get("decision") or report.get("decision") or "").upper()
        source = str(config.get("entry_source") or config.get("rule_mode") or self._track_b_lane_spec.strategy_family)
        if direction == "LONG":
            payload.update(
                {
                    "long_entry_raw": True,
                    "recent_long_setup": True,
                    "long_entry": True,
                    "long_entry_source": source,
                }
            )
        elif direction == "SHORT":
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
        if signal_packet.long_entry_source in self._promoted_sources():
            return LongEntryFamily.K
        return super()._resolve_long_entry_family(signal_packet)

    def _resolve_short_entry_family(self, signal_packet: SignalPacket) -> ShortEntryFamily:
        if signal_packet.short_entry_source in self._promoted_sources():
            fallback = _native_runtime_fallback_spec(self._track_b_rule_runner_config(), self._track_b_lane_spec)
            if fallback is not None and fallback.short_family is not None:
                return fallback.short_family
            return ShortEntryFamily.BEAR_SNAP
        return super()._resolve_short_entry_family(signal_packet)

    def _maybe_create_order_intent(
        self,
        bar: Any,
        signal_packet: SignalPacket,
        state: Any,
        exit_decision: Any,
    ) -> OrderIntent | None:
        paper_entry = _paper_only_direct_entry_source(signal_packet)
        if paper_entry is None:
            return super()._maybe_create_order_intent(bar, signal_packet, state, exit_decision)

        side, source = paper_entry
        decision = self._paper_only_direct_entry_decision_for_source(source)
        if not decision["accepted"]:
            return super()._maybe_create_order_intent(bar, signal_packet, state, exit_decision)

        intent_type = OrderIntentType.BUY_TO_OPEN if side == "LONG" else OrderIntentType.SELL_TO_OPEN
        if (
            state.entries_enabled
            and not state.operator_halt
            and state.same_underlying_entry_hold
            and self._entry_side_is_currently_allowed(side, state)
        ):
            hold_reason = (
                str(state.same_underlying_hold_reason or "").strip()
                or f"New entries held by operator for same-underlying conflict review on {self._settings.symbol}."
            )
            self._log_same_underlying_entry_block(
                bar=bar,
                intent_type=intent_type,
                source=source,
                reason=hold_reason,
            )
            return None

        if (
            state.entries_enabled
            and not state.operator_halt
            and not state.same_underlying_entry_hold
            and self._entry_side_is_currently_allowed(side, state)
        ):
            intent = OrderIntent(
                order_intent_id=f"{bar.bar_id}|{intent_type.value}",
                bar_id=bar.bar_id,
                symbol=self._settings.symbol,
                intent_type=intent_type,
                quantity=self._runtime_entry_quantity(),
                created_at=bar.end_ts,
                reason_code=source,
                signal_id=self._signal_id_for_actionable_signal(bar, side, source),
            )
            self._enforce_broker_event_envelope_contract(bar=bar, intent=intent)
            return intent

        return super()._maybe_create_order_intent(bar, signal_packet, state, exit_decision)

    def _enforce_broker_event_envelope_contract(self, *, bar: Any, intent: OrderIntent) -> None:
        lane_spec = getattr(self, "_track_b_lane_spec", None)
        if lane_spec is None:
            return
        lane_id = str(getattr(lane_spec, "lane_id", "") or "")
        strategy_id = str(intent.reason_code or self._track_b_rule_runner_config().get("strategy_id") or "")
        config = dict(getattr(lane_spec, "runtime_overlay_params", None) or {})
        latest_report = dict(getattr(self, "_latest_track_b_rule_report", {}) or {})
        bridge_adapter_present = _lane_has_bridge_submit_adapter(lane_id)
        result = build_broker_event_envelope(
            context=BrokerEventEnvelopeLaneContext(
                lane_id=lane_id,
                strategy_id=strategy_id,
                lane_classification=_broker_event_lane_classification(lane_spec=lane_spec, config=config),
                session=str(latest_report.get("session_label") or latest_report.get("session") or ""),
                window=str(latest_report.get("condition") or ""),
                artifact_family=_broker_event_artifact_family(lane_id),
                anchor_type=_broker_event_anchor_type(latest_report),
                input_artifact_path=str(config.get("input_event_path") or ""),
                runtime_profile=_broker_event_runtime_profile(config),
                runtime_commit=_broker_event_runtime_commit(config),
                bridge_submit_adapter_present=bridge_adapter_present,
                promotion_ready=not bridge_adapter_present,
                broker_authoritative=bridge_adapter_present,
            ),
            order_intent=intent,
            rule_report=latest_report,
            source_candle_timestamp=getattr(bar, "end_ts", None),
            config=BrokerEventEnvelopeConfig(repo_root=getattr(self, "_track_b_repo_root", Path(__file__).resolve().parents[3])),
        )
        if result.envelope is not None:
            write_broker_event_envelope(result=result)
        latest_report.update(broker_event_report_fields(result))
        self._latest_track_b_rule_report = latest_report

    def _evaluate_changeover_runtime_rule(self, feature_packet: FeaturePacket) -> SignalPacket | None:
        config = self._track_b_rule_runner_config()
        spec = _changeover_continuation_spec(config, self._track_b_lane_spec)
        if spec is None:
            return None
        decision = _changeover_long_continuation_decision(self._bar_history, spec=spec)
        self._latest_track_b_rule_report = {
            "classification": (
                "TRACK_B_CHANGEOVER_LONG_CONTINUATION_ACCEPTED"
                if decision["accepted"]
                else "TRACK_B_CHANGEOVER_LONG_CONTINUATION_NO_SIGNAL"
            ),
            "primary_blocker": decision["primary_blocker"],
            "changeover_overlay_label": spec.overlay_label,
            "condition": spec.condition_label,
            "benchmark_hold_bars_5m": spec.benchmark_hold_bars_5m,
            "session_open_price": decision["session_open_price"],
            "changeover_reference_price": decision["changeover_reference_price"],
            "current_close": decision["current_close"],
            "current_bar_end_et": decision["current_bar_end_et"],
            "paper_runtime_adapter": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
            "submit_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        signal_payload = _empty_signal_packet_payload(feature_packet.bar_id)
        if decision["accepted"]:
            signal_payload.update(
                {
                    "long_entry_raw": True,
                    "recent_long_setup": True,
                    "long_entry": True,
                    "long_entry_source": spec.strategy_id,
                }
            )
        return SignalPacket(**signal_payload)

    def _evaluate_us_session_continuation_runtime_rule(self, feature_packet: FeaturePacket) -> SignalPacket | None:
        config = self._track_b_rule_runner_config()
        spec = _us_session_continuation_spec(config, self._track_b_lane_spec)
        if spec is None:
            return None
        decision = self._paper_only_long_continuation_decision_for_source(spec.strategy_id)
        self._latest_track_b_rule_report = {
            "classification": (
                "TRACK_B_US_SESSION_LONG_CONTINUATION_ACCEPTED"
                if decision["accepted"]
                else "TRACK_B_US_SESSION_LONG_CONTINUATION_NO_SIGNAL"
            ),
            "primary_blocker": decision["primary_blocker"],
            "session_overlay_label": spec.overlay_label,
            "condition": spec.condition_label,
            "benchmark_hold_bars_5m": spec.benchmark_hold_bars_5m,
            "session_open_price": decision["session_open_price"],
            "current_close": decision["current_close"],
            "current_bar_end_et": decision["current_bar_end_et"],
            "paper_runtime_adapter": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
            "submit_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        signal_payload = _empty_signal_packet_payload(feature_packet.bar_id)
        if decision["accepted"]:
            signal_payload.update(
                {
                    "long_entry_raw": True,
                    "recent_long_setup": True,
                    "long_entry": True,
                    "long_entry_source": spec.strategy_id,
                }
            )
        return SignalPacket(**signal_payload)

    def _evaluate_paper_active_evidence_runtime_rule(self, feature_packet: FeaturePacket) -> SignalPacket | None:
        config = self._track_b_rule_runner_config()
        spec = _paper_active_evidence_spec(config, self._track_b_lane_spec)
        if spec is None:
            return None
        decision = self._paper_active_evidence_decision_for_source(
            spec.strategy_id,
            vwap=getattr(feature_packet, "vwap", None),
        )
        accepted = bool(decision["accepted"])
        self._latest_track_b_rule_report = {
            "classification": (
                "TRACK_B_PAPER_ACTIVE_EVIDENCE_ACCEPTED"
                if accepted
                else "TRACK_B_PAPER_ACTIVE_EVIDENCE_NO_SIGNAL"
            ),
            "primary_blocker": decision["primary_blocker"],
            "active_evidence_label": spec.overlay_label,
            "entry_source": spec.strategy_id,
            "condition": spec.condition_label,
            "direction": spec.direction,
            "benchmark_hold_bars_5m": spec.benchmark_hold_bars_5m,
            "session_open_price": decision["session_open_price"],
            "session_anchor_status": decision.get("session_anchor_status"),
            "session_anchor_reason_code": decision.get("session_anchor_reason_code"),
            "session_anchor_source": decision.get("session_anchor_source"),
            "session_anchor_source_artifact_path": decision.get("session_anchor_source_artifact_path"),
            "vwap_price": decision.get("vwap_price"),
            "current_close": decision["current_close"],
            "current_bar_end_et": decision["current_bar_end_et"],
            "paper_runtime_adapter": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
            "submit_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        signal_payload = _empty_signal_packet_payload(feature_packet.bar_id)
        if accepted and spec.direction == "LONG":
            signal_payload.update(
                {
                    "long_entry_raw": True,
                    "recent_long_setup": True,
                    "long_entry": True,
                    "long_entry_source": spec.strategy_id,
                }
            )
        elif accepted and spec.direction == "SHORT":
            signal_payload.update(
                {
                    "short_entry_raw": True,
                    "recent_short_setup": True,
                    "short_entry": True,
                    "short_entry_source": spec.strategy_id,
                }
            )
        return SignalPacket(**signal_payload)

    def _paper_only_long_continuation_decision_for_source(self, source: str) -> dict[str, Any]:
        decision = _paper_only_long_continuation_decision(self._bar_history, source=source)
        if source not in US_SESSION_CONTINUATION_SPECS:
            return decision
        current_end = getattr(self._bar_history[-1], "end_ts", None) if self._bar_history else None
        if decision["primary_blocker"] not in {"bar_history_missing", "us_session_open_bar_missing"} and len(self._bar_history) >= 2:
            return decision
        artifact_history = _phase1_runtime_bar_history_for_symbol(
            self._track_b_repo_root,
            symbol=str(getattr(self._settings, "symbol", "") or getattr(self._track_b_lane_spec, "symbol", "")),
            current_end=current_end if isinstance(current_end, datetime) else None,
        )
        if not artifact_history:
            return decision
        return _paper_only_long_continuation_decision(artifact_history, source=source)

    def _paper_only_direct_entry_decision_for_source(self, source: str) -> dict[str, Any]:
        if source in PAPER_ACTIVE_EVIDENCE_SPECS:
            latest_report = dict(getattr(self, "_latest_track_b_rule_report", {}) or {})
            if (
                latest_report.get("classification") == "TRACK_B_PAPER_ACTIVE_EVIDENCE_ACCEPTED"
                and latest_report.get("primary_blocker") is None
                and latest_report.get("entry_source") == source
                and latest_report.get("direction") == PAPER_ACTIVE_EVIDENCE_SPECS[source].direction
            ):
                return _changeover_decision(True, None)
            return self._paper_active_evidence_decision_for_source(source, vwap=None)
        return self._paper_only_long_continuation_decision_for_source(source)

    def _paper_active_evidence_decision_for_source(self, source: str, *, vwap: object = None) -> dict[str, Any]:
        current_end = getattr(self._bar_history[-1], "end_ts", None) if self._bar_history else None
        anchor_reference = None
        anchor_type = _session_anchor_type_for_active_evidence_source(source)
        if isinstance(current_end, datetime) and anchor_type is not None:
            anchor_reference = _resolve_active_evidence_session_anchor_reference(
                repo_root=self._track_b_repo_root,
                symbol=str(getattr(self._settings, "symbol", "") or getattr(self._track_b_lane_spec, "symbol", "")),
                current_end=current_end,
                anchor_type=anchor_type,
            )
        decision = _paper_active_evidence_decision(
            self._bar_history,
            source=source,
            vwap=vwap,
            session_anchor_reference=anchor_reference,
        )
        if anchor_reference is not None:
            return decision
        current_end = getattr(self._bar_history[-1], "end_ts", None) if self._bar_history else None
        if decision["primary_blocker"] not in {"bar_history_missing", "us_session_open_bar_missing"} and len(self._bar_history) >= 2:
            return decision
        artifact_history = _phase1_runtime_bar_history_for_symbol(
            self._track_b_repo_root,
            symbol=str(getattr(self._settings, "symbol", "") or getattr(self._track_b_lane_spec, "symbol", "")),
            current_end=current_end if isinstance(current_end, datetime) else None,
        )
        if not artifact_history:
            return decision
        return _paper_active_evidence_decision(
            artifact_history,
            source=source,
            vwap=vwap,
            session_anchor_reference=anchor_reference,
        )

    def _track_b_rule_runner_config(self) -> dict[str, Any]:
        return dict(getattr(self._track_b_lane_spec, "runtime_overlay_params", None) or {})

    def _promoted_sources(self) -> set[str]:
        config = self._track_b_rule_runner_config()
        return {
            str(item)
            for item in (
                config.get("entry_source"),
                config.get("rule_mode"),
                config.get("rule_id"),
                self._track_b_lane_spec.strategy_family,
            )
            if item
        }

    def _resolve_repo_path(self, value: object) -> Path:
        path = Path(str(value))
        if path.is_absolute():
            return path
        return self._track_b_repo_root / path

    def _evaluate_native_runtime_fallback(
        self,
        feature_packet: FeaturePacket,
        feature_history: list[FeaturePacket],
        *,
        stale_input_blocker: str,
        input_event_path: Path,
    ) -> SignalPacket | None:
        fallback = _native_runtime_fallback_spec(self._track_b_rule_runner_config(), self._track_b_lane_spec)
        if fallback is None:
            return None
        native_packet = super()._evaluate_signals(feature_packet, feature_history)
        promoted_packet = _promoted_signal_packet_from_native(
            feature_packet.bar_id,
            native_packet,
            fallback,
        )
        native_source = _matching_native_source(native_packet, fallback.native_sources, direction=fallback.direction)
        accepted = native_source is not None
        self._latest_track_b_rule_report = {
            "classification": (
                "TRACK_B_RULE_RUNNER_PAPER_NATIVE_SIGNAL_FALLBACK_ACCEPTED"
                if accepted
                else "TRACK_B_RULE_RUNNER_PAPER_NATIVE_SIGNAL_FALLBACK_NO_SIGNAL"
            ),
            "primary_blocker": None if accepted else "native_runtime_signal_not_present",
            "stale_input_blocker": stale_input_blocker,
            "input_event_path": str(input_event_path),
            "native_runtime_fallback": True,
            "native_sources_checked": list(fallback.native_sources),
            "native_source_matched": native_source,
            "promoted_entry_source": fallback.promoted_source,
            "signal_direction": fallback.direction if accepted else None,
            "paper_runtime_adapter": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
            "submit_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        return promoted_packet


def _lane_has_bridge_submit_adapter(lane_id: str) -> bool:
    try:
        from ..execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
    except Exception:
        return False
    return lane_submit_bridge_adapter(lane_id=lane_id) is not None


def _broker_event_lane_classification(*, lane_spec: Any, config: Mapping[str, Any]) -> str:
    values = (
        getattr(lane_spec, "lane_mode", None),
        getattr(lane_spec, "source_family", None),
        getattr(lane_spec, "status", None),
        config.get("lane_mode"),
        config.get("experimental_reason"),
        config.get("promotion_status"),
        config.get("entry_source"),
    )
    return "|".join(str(value) for value in values if value)


def _broker_event_artifact_family(lane_id: str) -> str | None:
    lowered = str(lane_id or "").lower()
    if "london_open_active_participation" in lowered:
        return "london_open_active_evidence"
    if "london_late_active_participation" in lowered:
        return "london_late_active_evidence"
    if "_us_active_participation_" in lowered:
        return "us_active_evidence"
    if "globex_active_participation" in lowered:
        return "globex_active_evidence"
    return None


def _broker_event_anchor_type(rule_report: Mapping[str, Any]) -> str | None:
    condition = str(rule_report.get("condition") or "").lower()
    if "london_late_reference" in condition or "05:30" in condition:
        return "LONDON_LATE_0530_REFERENCE"
    if "london_open" in condition or "03:00" in condition:
        return "LONDON_0300_OPEN"
    if "us_session_open" in condition or "09:30" in condition:
        return "US_0930_OPEN"
    if "globex" in condition or "18:00" in condition:
        return "GLOBEX_1800_REOPEN"
    return None


def _broker_event_runtime_profile(config: Mapping[str, Any]) -> str:
    for value in (
        config.get("profile_name"),
        config.get("track_b_paper_stack_profile"),
        os.environ.get("TRACK_B_PAPER_STACK_PROFILE"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    config_paths = str(
        os.environ.get("MGC_PROBATIONARY_PAPER_CONFIG_PATHS")
        or os.environ.get("MGC_HEADLESS_SUPERVISED_PAPER_CONFIG_PATHS")
        or ""
    )
    for item in config_paths.split(":"):
        name = Path(item).name
        if name.startswith("paper_stack_") and name.endswith(".yaml"):
            return name.removeprefix("paper_stack_").removesuffix(".yaml")
    return "UNKNOWN_RUNTIME_PROFILE"


def _broker_event_runtime_commit(config: Mapping[str, Any]) -> str:
    for value in (
        config.get("runtime_git_head"),
        config.get("source_runtime_git_head"),
        os.environ.get("MGC_TRACK_B_EXPECTED_SOURCE_COMMIT"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return "UNKNOWN_RUNTIME_COMMIT"


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _state_freshness_blocker(
    payload: Mapping[str, Any],
    *,
    current_bar_end: datetime | None,
    config: Mapping[str, Any],
) -> str | None:
    if not payload:
        return "track_b_rule_runner_input_event_missing"
    if bool(config.get("require_timestamp_coherence", True)) is False:
        return None
    if current_bar_end is None:
        return "track_b_rule_runner_current_bar_missing"
    candidate_ts = _parse_ts(
        payload.get("candle_timestamp")
        or payload.get("bar_end_ts")
        or payload.get("timestamp")
        or payload.get("observed_at")
    )
    if candidate_ts is None:
        return "track_b_rule_runner_input_event_timestamp_missing"
    tolerance = float(config.get("timestamp_tolerance_seconds") or DEFAULT_TRACK_B_RULE_RUNNER_SIGNAL_FRESHNESS_SECONDS)
    if abs((current_bar_end.astimezone(UTC) - candidate_ts.astimezone(UTC)).total_seconds()) > tolerance:
        return "track_b_rule_runner_input_event_not_timestamp_coherent"
    return None


def _native_runtime_fallback_spec(config: Mapping[str, Any], lane_spec: Any) -> _NativeRuntimeFallbackSpec | None:
    candidates: Sequence[object] = (
        config.get("entry_source"),
        config.get("rule_mode"),
        config.get("rule_id"),
        getattr(lane_spec, "strategy_family", None),
    )
    for candidate in candidates:
        value = str(candidate or "")
        if value in _NATIVE_RUNTIME_FALLBACKS:
            return _NATIVE_RUNTIME_FALLBACKS[value]
    return None


def _is_changeover_0300_long_rule(config: Mapping[str, Any], lane_spec: Any) -> bool:
    return _changeover_continuation_spec(config, lane_spec) is not None


def _changeover_continuation_spec(
    config: Mapping[str, Any],
    lane_spec: Any,
) -> _ChangeoverContinuationSpec | None:
    candidates: Sequence[object] = (
        config.get("entry_source"),
        config.get("rule_mode"),
        config.get("rule_id"),
        config.get("strategy_id"),
        getattr(lane_spec, "strategy_family", None),
        getattr(lane_spec, "standalone_strategy_id", None),
    )
    for candidate in candidates:
        spec = CHANGEOVER_CONTINUATION_SPECS.get(str(candidate or ""))
        if spec is not None:
            return spec
    return None


def _us_session_continuation_spec(
    config: Mapping[str, Any],
    lane_spec: Any,
) -> _UsSessionContinuationSpec | None:
    candidates: Sequence[object] = (
        config.get("entry_source"),
        config.get("rule_mode"),
        config.get("rule_id"),
        config.get("strategy_id"),
        getattr(lane_spec, "strategy_family", None),
        getattr(lane_spec, "standalone_strategy_id", None),
    )
    for candidate in candidates:
        spec = US_SESSION_CONTINUATION_SPECS.get(str(candidate or ""))
        if spec is not None:
            return spec
    return None


def _paper_active_evidence_spec(
    config: Mapping[str, Any],
    lane_spec: Any,
) -> _PaperActiveEvidenceSpec | None:
    candidates: Sequence[object] = (
        config.get("entry_source"),
        config.get("rule_mode"),
        config.get("rule_id"),
        config.get("strategy_id"),
        getattr(lane_spec, "strategy_family", None),
        getattr(lane_spec, "standalone_strategy_id", None),
    )
    for candidate in candidates:
        spec = PAPER_ACTIVE_EVIDENCE_SPECS.get(str(candidate or ""))
        if spec is not None:
            return spec
    return None


def _paper_only_long_continuation_entry_source(signal_packet: SignalPacket) -> str | None:
    source = str(getattr(signal_packet, "long_entry_source", "") or "")
    if bool(getattr(signal_packet, "long_entry", False)) and (
        source in CHANGEOVER_CONTINUATION_SPECS or source in US_SESSION_CONTINUATION_SPECS
    ):
        return source
    return None


def _paper_only_direct_entry_source(signal_packet: SignalPacket) -> tuple[str, str] | None:
    long_source = str(getattr(signal_packet, "long_entry_source", "") or "")
    short_source = str(getattr(signal_packet, "short_entry_source", "") or "")
    if bool(getattr(signal_packet, "long_entry", False)) and (
        long_source in CHANGEOVER_CONTINUATION_SPECS
        or long_source in US_SESSION_CONTINUATION_SPECS
        or long_source in PAPER_ACTIVE_EVIDENCE_SPECS
    ):
        return ("LONG", long_source)
    if bool(getattr(signal_packet, "short_entry", False)) and short_source in PAPER_ACTIVE_EVIDENCE_SPECS:
        return ("SHORT", short_source)
    return None


def _changeover_long_entry_source(signal_packet: SignalPacket) -> str | None:
    source = str(getattr(signal_packet, "long_entry_source", "") or "")
    if bool(getattr(signal_packet, "long_entry", False)) and source in CHANGEOVER_CONTINUATION_SPECS:
        return source
    return None


def _paper_only_long_continuation_decision(bar_history: Sequence[Any], *, source: str) -> dict[str, Any]:
    changeover_spec = CHANGEOVER_CONTINUATION_SPECS.get(source)
    if changeover_spec is not None:
        return _changeover_long_continuation_decision(bar_history, spec=changeover_spec)
    us_spec = US_SESSION_CONTINUATION_SPECS.get(source)
    if us_spec is not None:
        return _us_session_long_continuation_decision(bar_history, spec=us_spec)
    return _changeover_decision(False, "unknown_paper_only_continuation_source")


def _paper_active_evidence_decision(
    bar_history: Sequence[Any],
    *,
    source: str,
    vwap: object = None,
    session_anchor_reference: Any | None = None,
) -> dict[str, Any]:
    spec = PAPER_ACTIVE_EVIDENCE_SPECS.get(source)
    if spec is None:
        return _changeover_decision(False, "unknown_paper_active_evidence_source")
    if not bar_history:
        return _changeover_decision(False, "bar_history_missing")
    current = bar_history[-1]
    current_end = getattr(current, "end_ts", None)
    if not isinstance(current_end, datetime):
        return _changeover_decision(False, "current_bar_end_missing")
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    local_time = current_end_et.timetz().replace(tzinfo=None)
    if not _time_inside_active_evidence_window(local_time, spec=spec):
        return _changeover_decision(
            False,
            "not_in_paper_active_evidence_entry_window",
            current_close=getattr(current, "close", None),
            current_bar_end_et=current_end_et.isoformat(),
        )
    session_open_bar = session_anchor_reference
    if session_open_bar is None:
        session_open_bar = _active_evidence_reference_bar(bar_history, current_end=current_end, spec=spec)
    current_close = getattr(current, "close", None)
    anchor_status = getattr(session_anchor_reference, "session_anchor_status", None)
    anchor_reason = getattr(session_anchor_reference, "session_anchor_reason_code", None)
    anchor_source_path = getattr(session_anchor_reference, "source_artifact_path", None)
    reference_source = str(getattr(session_open_bar, "reference_source", None) or "BAR_HISTORY") if session_open_bar is not None else None
    if session_anchor_reference is not None and anchor_status != SessionAnchorStatus.READY.value:
        return _changeover_decision(
            False,
            "SESSION_ANCHOR_NOT_READY",
            current_close=current_close,
            current_bar_end_et=current_end_et.isoformat(),
            reference_source=reference_source,
            reference_recovery_blocker=str(anchor_reason or "SESSION_ANCHOR_NOT_READY"),
            session_anchor_status=anchor_status,
            session_anchor_reason_code=anchor_reason,
            session_anchor_source=reference_source,
            session_anchor_source_artifact_path=anchor_source_path,
        )
    session_open_price = getattr(session_open_bar, "open", None) if session_open_bar is not None else None
    vwap_price = _decimal_or_none(vwap)
    if vwap_price is None and spec.reference_time_et == time(18, 0):
        vwap_price = _rolling_vwap_reference(bar_history)
        if vwap_price is not None and reference_source is None:
            reference_source = "ROLLING_VWAP_REFERENCE"
    if session_open_bar is None and (spec.require_reference_bar or vwap_price is None):
        missing_blocker = "SESSION_ANCHOR_NOT_READY" if spec.reference_time_et == time(9, 30) else f"{spec.reference_label}_bar_missing"
        recovery_blocker = (
            "SESSION_ANCHOR_NOT_READY"
            if spec.reference_time_et == time(9, 30)
            else f"phase1_1m_{spec.reference_label}_reference_missing"
        )
        return _changeover_decision(
            False,
            missing_blocker,
            current_close=current_close,
            current_bar_end_et=current_end_et.isoformat(),
            reference_recovery_blocker=recovery_blocker,
            session_anchor_status=anchor_status,
            session_anchor_reason_code=anchor_reason,
            session_anchor_source=reference_source,
            session_anchor_source_artifact_path=anchor_source_path,
        )
    close_value = _decimal_or_none(current_close)
    session_open_value = _decimal_or_none(session_open_price)
    if spec.direction == "SHORT":
        reference_ok = close_value is not None and (
            (session_open_value is not None and close_value < session_open_value)
            or (vwap_price is not None and close_value < vwap_price)
        )
        continuation_ok = _simple_short_continuation_ok(bar_history)
        if not reference_ok:
            blocker = (
                "close_not_below_vwap_or_09_30_us_session_open"
                if spec.reference_time_et == time(9, 30)
                else f"close_not_below_vwap_or_{spec.reference_label}"
            )
        else:
            blocker = None
        soft_warning = None if continuation_ok else "simple_short_continuation_not_confirmed"
    else:
        reference_ok = close_value is not None and (
            (session_open_value is not None and close_value > session_open_value)
            or (vwap_price is not None and close_value > vwap_price)
        )
        continuation_ok = _simple_long_continuation_ok(bar_history)
        if not reference_ok:
            blocker = (
                "close_not_above_vwap_or_09_30_us_session_open"
                if spec.reference_time_et == time(9, 30)
                else f"close_not_above_vwap_or_{spec.reference_label}"
            )
        else:
            blocker = None
        soft_warning = None if continuation_ok else "simple_long_continuation_not_confirmed"
    decision = _changeover_decision(
        blocker is None,
        blocker,
        current_close=current_close,
        session_open_price=session_open_price,
        current_bar_end_et=current_end_et.isoformat(),
        reference_source=reference_source,
        session_anchor_status=anchor_status,
        session_anchor_reason_code=anchor_reason,
        session_anchor_source=reference_source,
        session_anchor_source_artifact_path=anchor_source_path,
    )
    decision["vwap_price"] = None if vwap_price is None else str(vwap_price)
    decision["continuation_confirmed"] = continuation_ok
    decision["recent_close_direction_tag"] = _recent_close_direction_tag(bar_history)
    decision["paper_only_soft_warnings"] = [] if soft_warning is None else [soft_warning]
    return decision


def _time_inside_active_evidence_window(local_time: time, *, spec: _PaperActiveEvidenceSpec) -> bool:
    if spec.start_time_et <= spec.end_time_et:
        return spec.start_time_et <= local_time < spec.end_time_et
    return local_time >= spec.start_time_et or local_time < spec.end_time_et


def _session_anchor_type_for_active_evidence_source(source: str) -> SessionAnchorType | None:
    spec = PAPER_ACTIVE_EVIDENCE_SPECS.get(source)
    if spec is None:
        return None
    if spec.reference_time_et == time(9, 30):
        return SessionAnchorType.US_0930_OPEN
    if spec.reference_time_et == time(3, 0):
        return SessionAnchorType.LONDON_0300_OPEN
    if spec.reference_time_et == time(5, 30):
        return SessionAnchorType.LONDON_LATE_0530_REFERENCE
    return None


def _uses_us_session_anchor(source: str) -> bool:
    return _session_anchor_type_for_active_evidence_source(source) == SessionAnchorType.US_0930_OPEN


def _uses_london_session_anchor(source: str) -> bool:
    return _session_anchor_type_for_active_evidence_source(source) == SessionAnchorType.LONDON_0300_OPEN


def _resolve_active_evidence_session_anchor_reference(
    *,
    repo_root: Path,
    symbol: str,
    current_end: datetime,
    anchor_type: SessionAnchorType,
) -> Any | None:
    result = resolve_session_anchor(
        symbol,
        anchor_type,
        current_end,
        timeframe="1m",
        config=TrackBSessionAnchorConfig(repo_root=repo_root),
    )
    if result.status != SessionAnchorStatus.READY or result.bar is None:
        return SimpleNamespace(
            session_anchor_status=result.status.value,
            session_anchor_reason_code=result.reason_code.value,
            source_artifact_path=result.source_artifact_path,
            reference_source=result.source,
            open=None,
        )
    bar = result.bar
    return SimpleNamespace(
        start_ts=_parse_ts(bar.get("bar_start")),
        end_ts=_parse_ts(bar.get("bar_end")),
        open=result.reference_price,
        high=bar.get("high"),
        low=bar.get("low"),
        close=bar.get("close"),
        reference_source=result.source or "SESSION_ANCHOR_RESOLVER",
        source_artifact_path=result.source_artifact_path,
        session_anchor_status=result.status.value,
        session_anchor_reason_code=result.reason_code.value,
    )


def _active_evidence_reference_bar(
    bar_history: Sequence[Any],
    *,
    current_end: datetime,
    spec: _PaperActiveEvidenceSpec,
) -> Any | None:
    if spec.reference_time_et == time(18, 0):
        return _globex_session_open_reference_bar(bar_history, current_end=current_end)
    return _reference_bar_for_time(bar_history, current_end=current_end, reference_time=spec.reference_time_et)


def _globex_session_open_reference_bar(bar_history: Sequence[Any], *, current_end: datetime) -> Any | None:
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    session_open_date = current_end_et.date()
    if current_end_et.timetz().replace(tzinfo=None) < time(18, 0):
        session_open_date = session_open_date - timedelta(days=1)
    session_open_et = datetime.combine(session_open_date, time(18, 0), tzinfo=NEW_YORK_TZ)
    session_open_utc = session_open_et.astimezone(UTC)
    containing = []
    for bar in bar_history:
        start = getattr(bar, "start_ts", None)
        end = getattr(bar, "end_ts", None)
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        if start.astimezone(UTC) <= session_open_utc < end.astimezone(UTC):
            containing.append(bar)
    if containing:
        return min(containing, key=lambda bar: getattr(bar, "end_ts").astimezone(UTC))
    return None


def _rolling_vwap_reference(bar_history: Sequence[Any], *, max_bars: int = 30) -> Decimal | None:
    recent = list(bar_history)[-max_bars:]
    weighted_sum = Decimal("0")
    volume_sum = Decimal("0")
    close_values: list[Decimal] = []
    for bar in recent:
        close_value = _decimal_or_none(getattr(bar, "close", None))
        if close_value is None:
            continue
        close_values.append(close_value)
        volume_value = _decimal_or_none(getattr(bar, "volume", None))
        if volume_value is not None and volume_value > 0:
            weighted_sum += close_value * volume_value
            volume_sum += volume_value
    if volume_sum > 0:
        return weighted_sum / volume_sum
    if close_values:
        return sum(close_values, Decimal("0")) / Decimal(len(close_values))
    return None


def _changeover_0300_long_continuation_decision(bar_history: Sequence[Any]) -> dict[str, Any]:
    return _changeover_long_continuation_decision(
        bar_history,
        spec=CHANGEOVER_CONTINUATION_SPECS[CHANGEOVER_0300_LONG_CONTINUATION_ID],
    )


def _us_session_long_continuation_decision(
    bar_history: Sequence[Any],
    *,
    spec: _UsSessionContinuationSpec,
) -> dict[str, Any]:
    if not bar_history:
        return _changeover_decision(False, "bar_history_missing")
    current = bar_history[-1]
    current_end = getattr(current, "end_ts", None)
    if not isinstance(current_end, datetime):
        return _changeover_decision(False, "current_bar_end_missing")
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    local_time = current_end_et.timetz().replace(tzinfo=None)
    if not (spec.start_time_et <= local_time < spec.end_time_et):
        return _changeover_decision(
            False,
            "not_in_us_session_continuation_entry_window",
            current_close=getattr(current, "close", None),
            current_bar_end_et=current_end_et.isoformat(),
        )
    session_open_bar = _reference_bar_for_time(bar_history, current_end=current_end, reference_time=time(9, 30))
    if session_open_bar is None:
        return _changeover_decision(
            False,
            "us_session_open_bar_missing",
            current_close=getattr(current, "close", None),
            current_bar_end_et=current_end_et.isoformat(),
        )
    current_close = getattr(current, "close", None)
    session_open_price = getattr(session_open_bar, "open", None)
    price_reference_ok = current_close is not None and session_open_price is not None and current_close > session_open_price
    continuation_ok = _simple_long_continuation_ok(bar_history)
    if not price_reference_ok:
        blocker = "close_not_above_09_30_us_session_open"
    elif not continuation_ok:
        blocker = "simple_continuation_not_confirmed"
    else:
        blocker = None
    return _changeover_decision(
        blocker is None,
        blocker,
        current_close=current_close,
        session_open_price=session_open_price,
        current_bar_end_et=current_end_et.isoformat(),
    )


def _changeover_long_continuation_decision(
    bar_history: Sequence[Any],
    *,
    spec: _ChangeoverContinuationSpec,
) -> dict[str, Any]:
    if not bar_history:
        return _changeover_decision(False, "bar_history_missing")
    current = bar_history[-1]
    current_end = getattr(current, "end_ts", None)
    if not isinstance(current_end, datetime):
        return _changeover_decision(False, "current_bar_end_missing")
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    if not _bar_end_inside_changeover_window(current_end_et, spec=spec):
        return _changeover_decision(
            False,
            "not_in_changeover_entry_window",
            current_close=getattr(current, "close", None),
            current_bar_end_et=current_end_et.isoformat(),
        )
    session_open_bar = _session_open_bar_for_changeover(bar_history, current_end=current_end)
    if session_open_bar is None:
        return _changeover_decision(
            False,
            "session_open_bar_missing",
            current_close=getattr(current, "close", None),
            current_bar_end_et=current_end_et.isoformat(),
        )
    current_close = getattr(current, "close", None)
    session_open_price = getattr(session_open_bar, "open", None)
    reference_bar = _reference_bar_for_time(bar_history, current_end=current_end, reference_time=time(3, 0))
    changeover_reference_price = getattr(reference_bar, "close", None) if reference_bar is not None else None
    price_reference_ok = current_close is not None and session_open_price is not None and current_close > session_open_price
    if spec.use_0300_reference and current_close is not None and changeover_reference_price is not None:
        price_reference_ok = price_reference_ok or current_close > changeover_reference_price
    continuation_ok = _simple_long_continuation_ok(bar_history)
    if not price_reference_ok:
        blocker = "close_not_above_18_00_session_open_or_changeover_reference" if spec.use_0300_reference else "close_not_above_18_00_session_open"
    elif not continuation_ok:
        blocker = "simple_continuation_not_confirmed"
    else:
        blocker = None
    return _changeover_decision(
        blocker is None,
        blocker,
        current_close=current_close,
        session_open_price=session_open_price,
        changeover_reference_price=changeover_reference_price,
        current_bar_end_et=current_end_et.isoformat(),
    )


def _bar_end_inside_changeover_window(current_end_et: datetime, *, spec: _ChangeoverContinuationSpec) -> bool:
    local_time = current_end_et.timetz().replace(tzinfo=None)
    return time(spec.start_hour_et, 0) <= local_time < time(spec.end_hour_et, 0)


def _session_open_bar_for_changeover(bar_history: Sequence[Any], *, current_end: datetime) -> Any | None:
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    session_open_date = current_end_et.date()
    if current_end_et.timetz().replace(tzinfo=None) < time(18, 0):
        session_open_date = session_open_date - timedelta(days=1)
    session_open_et = datetime.combine(session_open_date, time(18, 0), tzinfo=NEW_YORK_TZ)
    session_open_utc = session_open_et.astimezone(UTC)
    containing = []
    after_open = []
    for bar in bar_history:
        start = getattr(bar, "start_ts", None)
        end = getattr(bar, "end_ts", None)
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        start_utc = start.astimezone(UTC)
        end_utc = end.astimezone(UTC)
        if start_utc <= session_open_utc < end_utc:
            containing.append(bar)
        elif session_open_utc <= end_utc <= current_end.astimezone(UTC):
            after_open.append(bar)
    if containing:
        return min(containing, key=lambda bar: getattr(bar, "end_ts").astimezone(UTC))
    if after_open:
        return min(after_open, key=lambda bar: getattr(bar, "end_ts").astimezone(UTC))
    return None


def _reference_bar_for_time(
    bar_history: Sequence[Any],
    *,
    current_end: datetime,
    reference_time: time,
) -> Any | None:
    current_end_et = current_end.astimezone(NEW_YORK_TZ)
    reference_et = datetime.combine(current_end_et.date(), reference_time, tzinfo=NEW_YORK_TZ)
    if reference_et > current_end_et:
        reference_et = reference_et - timedelta(days=1)
    reference_utc = reference_et.astimezone(UTC)
    candidates = []
    for bar in bar_history:
        start = getattr(bar, "start_ts", None)
        end = getattr(bar, "end_ts", None)
        if not isinstance(start, datetime) or not isinstance(end, datetime):
            continue
        if start.astimezone(UTC) < reference_utc <= end.astimezone(UTC):
            candidates.append(bar)
    return max(candidates, key=lambda bar: getattr(bar, "end_ts").astimezone(UTC)) if candidates else None


def _simple_long_continuation_ok(bar_history: Sequence[Any]) -> bool:
    if len(bar_history) < 2:
        return True
    current_close = getattr(bar_history[-1], "close", None)
    previous_close = getattr(bar_history[-2], "close", None)
    current_open = getattr(bar_history[-1], "open", None)
    if current_close is None:
        return False
    if previous_close is not None and current_close >= previous_close:
        return True
    return current_open is not None and current_close >= current_open


def _simple_short_continuation_ok(bar_history: Sequence[Any]) -> bool:
    if len(bar_history) < 2:
        return True
    current_close = getattr(bar_history[-1], "close", None)
    previous_close = getattr(bar_history[-2], "close", None)
    current_open = getattr(bar_history[-1], "open", None)
    if current_close is None:
        return False
    if previous_close is not None and current_close <= previous_close:
        return True
    return current_open is not None and current_close <= current_open


def _recent_close_direction_tag(bar_history: Sequence[Any]) -> str:
    if len(bar_history) < 2:
        return "INSUFFICIENT_RECENT_CLOSE_HISTORY"
    current_close = _decimal_or_none(getattr(bar_history[-1], "close", None))
    previous_close = _decimal_or_none(getattr(bar_history[-2], "close", None))
    if current_close is None or previous_close is None:
        return "RECENT_CLOSE_UNAVAILABLE"
    if current_close > previous_close:
        return "RECENT_CLOSE_UP"
    if current_close < previous_close:
        return "RECENT_CLOSE_DOWN"
    return "RECENT_CLOSE_FLAT"


def _decimal_or_none(value: object) -> Any | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 - reference evidence should fail closed, not crash the lane.
        return None


def _changeover_decision(
    accepted: bool,
    primary_blocker: str | None,
    *,
    current_close: object = None,
    session_open_price: object = None,
    changeover_reference_price: object = None,
    current_bar_end_et: str | None = None,
    reference_source: str | None = None,
    reference_recovery_blocker: str | None = None,
    session_anchor_status: str | None = None,
    session_anchor_reason_code: str | None = None,
    session_anchor_source: str | None = None,
    session_anchor_source_artifact_path: str | None = None,
) -> dict[str, Any]:
    return {
        "accepted": accepted,
        "primary_blocker": primary_blocker,
        "current_close": None if current_close is None else str(current_close),
        "session_open_price": None if session_open_price is None else str(session_open_price),
        "changeover_reference_price": (
            None if changeover_reference_price is None else str(changeover_reference_price)
        ),
        "current_bar_end_et": current_bar_end_et,
        "reference_source": reference_source,
        "reference_recovery_blocker": reference_recovery_blocker,
        "session_anchor_status": session_anchor_status,
        "session_anchor_reason_code": session_anchor_reason_code,
        "session_anchor_source": session_anchor_source,
        "session_anchor_source_artifact_path": session_anchor_source_artifact_path,
    }


def _phase1_runtime_bar_history_for_symbol(
    repo_root: Path,
    *,
    symbol: str,
    current_end: datetime | None = None,
) -> list[Any]:
    normalized_symbol = str(symbol or "").strip().upper()
    if not normalized_symbol:
        return []
    paths = [
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data_gap_backfill"
        / "us_session_reference"
        / normalized_symbol
        / "1m"
        / "latest_runtime_candles.json"
    ]
    paths.append(
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / normalized_symbol
        / "1m"
        / "latest_runtime_candles.json"
    )
    merged: dict[datetime, Any] = {}
    for path in paths:
        for bar in _phase1_runtime_bar_history_from_path(path, current_end=current_end):
            merged[bar.end_ts.astimezone(UTC)] = bar
    return [merged[key] for key in sorted(merged)]


def _phase1_runtime_bar_history_from_path(path: Path, *, current_end: datetime | None = None) -> list[Any]:
    payload = _read_json(path)
    bars = payload.get("bars")
    if not isinstance(bars, list):
        return []
    history: list[Any] = []
    current_end_utc = current_end.astimezone(UTC) if isinstance(current_end, datetime) else None
    source = str(payload.get("source") or payload.get("source_id") or "")
    reference_source = "RECOVERED_PHASE1_1M" if source == "DATABENTO_HISTORICAL_SEED" else "PHASE1_RUNTIME_1M"
    for row in bars:
        if not isinstance(row, Mapping):
            continue
        start = _parse_ts(row.get("bar_start") or row.get("start_ts") or row.get("start"))
        end = _parse_ts(row.get("bar_end") or row.get("end_ts") or row.get("timestamp"))
        if start is None or end is None:
            continue
        if current_end_utc is not None and end.astimezone(UTC) > current_end_utc:
            continue
        history.append(
            SimpleNamespace(
                start_ts=start,
                end_ts=end,
                open=row.get("open"),
                high=row.get("high"),
                low=row.get("low"),
                close=row.get("close"),
                reference_source=reference_source,
                source_artifact_path=str(path),
            )
        )
    return history


def _matching_native_source(packet: SignalPacket, native_sources: Sequence[str], *, direction: str) -> str | None:
    if direction == "SHORT" and packet.short_entry and packet.short_entry_source in native_sources:
        return packet.short_entry_source
    if direction == "LONG" and packet.long_entry and packet.long_entry_source in native_sources:
        return packet.long_entry_source
    return None


def _promoted_signal_packet_from_native(
    bar_id: str,
    native_packet: SignalPacket,
    fallback: _NativeRuntimeFallbackSpec,
) -> SignalPacket:
    payload = _empty_signal_packet_payload(bar_id)
    if _matching_native_source(native_packet, fallback.native_sources, direction=fallback.direction) is None:
        return SignalPacket(**payload)
    if fallback.direction == "SHORT":
        payload.update(
            {
                "short_entry_raw": True,
                "recent_short_setup": True,
                "short_entry": True,
                "short_entry_source": fallback.promoted_source,
            }
        )
    elif fallback.direction == "LONG":
        payload.update(
            {
                "long_entry_raw": True,
                "recent_long_setup": True,
                "long_entry": True,
                "long_entry_source": fallback.promoted_source,
            }
        )
    return SignalPacket(**payload)


def _parse_ts(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed
