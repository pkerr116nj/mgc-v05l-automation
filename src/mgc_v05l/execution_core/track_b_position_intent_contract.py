"""Read-only PositionIntent / TradeThesis contract for Track B PAPER.

This module defines metadata that every Track B entry should be able to carry
before the system uses it for lifecycle, exit attribution, or future exit-policy
decisions.  It is deliberately diagnostic only: it does not submit, cancel,
close, modify, flatten, or mutate lifecycle state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_exit_strategy_roster import (
    MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
    MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1,
    MNQ_SNAP_TURN_TIMEBOX_3X5M_V1,
    MNQ_GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_60M_SHADOW_V1,
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
)
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
)
from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import (
    strategy_hold_exit_policy_for,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROSTER_PATH = Path("config/track_b_guarded_paper_roster.json")
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_position_intent_contract_audit.json"
)

POSITION_INTENT_CONTRACT_READY = "POSITION_INTENT_CONTRACT_READY"
POSITION_INTENT_CONTRACT_GAPS_FOUND = "POSITION_INTENT_CONTRACT_GAPS_FOUND"
POSITION_INTENT_VALID = "POSITION_INTENT_VALID"
POSITION_INTENT_INVALID = "POSITION_INTENT_INVALID"


class ThesisType(str, Enum):
    SCALP = "SCALP"
    DRIFT = "DRIFT"
    TREND_PARTICIPATION = "TREND_PARTICIPATION"
    SNAP_TURN = "SNAP_TURN"
    BREAKOUT_RETEST = "BREAKOUT_RETEST"
    OTHER = "OTHER"


class ExpectedHoldType(str, Enum):
    QUICK_SCALP = "QUICK_SCALP"
    TIMEBOXED = "TIMEBOXED"
    PARTICIPATION_HOLD = "PARTICIPATION_HOLD"
    SESSION_HOLD = "SESSION_HOLD"


@dataclass(frozen=True)
class TradeThesis:
    thesis_type: str
    thesis_summary: str
    invalidation_conditions: tuple[str, ...]
    regime_tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class HoldPolicy:
    expected_hold_type: str
    max_hold_policy: str
    expected_hold_bars_5m: int | None = None


@dataclass(frozen=True)
class ExitPolicy:
    intended_exit_family: str
    managed_exit_policy_id: str
    exit_profile_id: str
    exit_profile_source: str = "track_b_exit_strategy_roster"


@dataclass(frozen=True)
class OrderPolicy:
    order_type: str = "LMT"
    time_in_force: str = "DAY"
    pricing_policy: str = "GUARDED_LIMIT"
    broker_mutation_allowed: bool = False
    submit_authority: bool = False


@dataclass(frozen=True)
class AttributionTags:
    eligible_for_alpha_exit_analysis: bool = True
    strategy_family: str = ""
    session_tags: tuple[str, ...] = ()
    regime_tags: tuple[str, ...] = ()
    contamination_flags: tuple[str, ...] = ()


@dataclass(frozen=True)
class PositionIntent:
    strategy_id: str
    lane_id: str
    instrument_family: str
    contract_key: str
    local_symbol: str
    con_id: int | None
    expiry: str
    side: str
    quantity: int
    trade_thesis: TradeThesis
    hold_policy: HoldPolicy
    exit_policy: ExitPolicy
    order_policy: OrderPolicy
    pyramiding_policy: str
    conflict_group: str
    attribution_tags: AttributionTags
    schema_version: str = "track_b_position_intent_contract_v1"
    read_only: bool = True
    lifecycle_authority: bool = False
    submit_authority: bool = False
    broker_mutation_allowed: bool = False
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False


@dataclass(frozen=True)
class PositionIntentContractAuditConfig:
    repo_root: Path = REPO_ROOT
    roster_path: Path = DEFAULT_ROSTER_PATH
    output_path: Path = DEFAULT_OUTPUT_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


@dataclass(frozen=True)
class _StrategyContractTemplate:
    strategy_id: str
    lane_id: str
    instrument_family: str
    contract_key: str
    local_symbol: str
    con_id: int
    expiry: str
    side: str
    quantity: int
    thesis_type: ThesisType
    thesis_summary: str
    expected_hold_type: ExpectedHoldType
    intended_exit_family: str
    managed_exit_policy_id: str
    exit_profile_id: str
    invalidation_conditions: tuple[str, ...]
    max_hold_policy: str = "3_COMPLETED_5M_BARS"
    expected_hold_bars_5m: int = 3
    pyramiding_policy: str = PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED
    conflict_group: str = ""
    strategy_family: str = ""
    session_tags: tuple[str, ...] = ()
    regime_tags: tuple[str, ...] = ()


def _mgc_template(
    *,
    strategy_id: str,
    lane_id: str,
    side: str,
    thesis_type: ThesisType,
    thesis_summary: str,
    managed_exit_policy_id: str = PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    exit_profile_id: str = MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    session_tags: tuple[str, ...],
    regime_tags: tuple[str, ...],
) -> _StrategyContractTemplate:
    return _StrategyContractTemplate(
        strategy_id=strategy_id,
        lane_id=lane_id,
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        expiry="20260626",
        side=side,
        quantity=1,
        thesis_type=thesis_type,
        thesis_summary=thesis_summary,
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="TIME_BOX_EXIT",
        managed_exit_policy_id=managed_exit_policy_id,
        exit_profile_id=exit_profile_id,
        invalidation_conditions=("opposite_signal_confirms", "thesis_context_invalidates"),
        conflict_group="gold_mgc_gc",
        strategy_family="gold_track_b_runtime",
        session_tags=session_tags,
        regime_tags=regime_tags,
    )


def _mnq_template(
    *,
    strategy_id: str,
    lane_id: str,
    side: str,
    thesis_type: ThesisType,
    thesis_summary: str,
    session_tags: tuple[str, ...],
    regime_tags: tuple[str, ...],
) -> _StrategyContractTemplate:
    return _StrategyContractTemplate(
        strategy_id=strategy_id,
        lane_id=lane_id,
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        side=side,
        quantity=1,
        thesis_type=thesis_type,
        thesis_summary=thesis_summary,
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="TIME_BOX_EXIT",
        managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MNQ_SNAP_TURN_TIMEBOX_3X5M_V1,
        invalidation_conditions=("opposite_snap_turn_confirms", "snap_turn_context_invalidates"),
        conflict_group="equity_index_nasdaq_mnq_nq",
        strategy_family="mnq_snap_turn",
        session_tags=session_tags,
        regime_tags=regime_tags,
    )


APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES: Mapping[str, _StrategyContractTemplate] = {
    "asian_drift_v1": _StrategyContractTemplate(
        strategy_id="asian_drift_v1",
        lane_id="mgc_example_long_lmt_day",
        instrument_family="MGC",
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        expiry="20260626",
        side="LONG",
        quantity=1,
        thesis_type=ThesisType.DRIFT,
        thesis_summary="Asia-session drift continuation with strict A-grade anchor context.",
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="TIME_BOX_EXIT",
        managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        invalidation_conditions=("drift_context_fails", "opposite_participation_accelerates"),
        conflict_group="gold_mgc_gc",
        strategy_family="asian_drift",
        session_tags=("ASIA",),
        regime_tags=("QUIET_DRIFT", "TREND_PARTICIPATION"),
    ),
    "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": _mgc_template(
        strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_asia_early_pause_resume_short",
        side="SHORT",
        thesis_type=ThesisType.OTHER,
        thesis_summary="Asia-early pause/resume short continuation after local weakness resumes.",
        session_tags=("ASIA",),
        regime_tags=("TREND_PARTICIPATION",),
    ),
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": _mgc_template(
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
        side="LONG",
        thesis_type=ThesisType.BREAKOUT_RETEST,
        thesis_summary="Asia-early breakout/retest/hold long continuation.",
        session_tags=("ASIA",),
        regime_tags=("BREAKOUT_RETEST", "TREND_PARTICIPATION"),
    ),
    "FIRST_BULL_SNAP_TURN_V1": _mgc_template(
        strategy_id="FIRST_BULL_SNAP_TURN_V1",
        lane_id="mgc_first_bull_snap_turn",
        side="LONG",
        thesis_type=ThesisType.SNAP_TURN,
        thesis_summary="First bullish snap-turn after local reversal evidence.",
        session_tags=("GLOBAL",),
        regime_tags=("SNAP_TURN_REVERSAL",),
    ),
    "FIRST_BEAR_SNAP_TURN_V1": _mgc_template(
        strategy_id="FIRST_BEAR_SNAP_TURN_V1",
        lane_id="mgc_first_bear_snap_turn",
        side="SHORT",
        thesis_type=ThesisType.SNAP_TURN,
        thesis_summary="First bearish snap-turn after local reversal evidence.",
        session_tags=("GLOBAL",),
        regime_tags=("SNAP_TURN_REVERSAL",),
    ),
    "LONDON_LATE_PAUSE_RESUME_SHORT_V1": _mgc_template(
        strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_london_late_pause_resume_short",
        side="SHORT",
        thesis_type=ThesisType.OTHER,
        thesis_summary="London-late pause/resume short after continuation resumes.",
        session_tags=("LONDON",),
        regime_tags=("TREND_PARTICIPATION",),
    ),
    "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1": _mgc_template(
        strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_asia_late_flat_pullback_pause_resume_long",
        side="LONG",
        thesis_type=ThesisType.DRIFT,
        thesis_summary="Asia-late flat pullback pause/resume long continuation.",
        managed_exit_policy_id="FORCED_SESSION_SEGMENT_LOCAL_EXIT_V1",
        exit_profile_id=MGC_FORCED_SESSION_SEGMENT_TIMEBOX_3X5M_V1,
        session_tags=("ASIA",),
        regime_tags=("QUIET_DRIFT", "TREND_PARTICIPATION"),
    ),
    "US_DERIVATIVE_BEAR_TURN_V1": _mgc_template(
        strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
        lane_id="mgc_us_derivative_bear_turn",
        side="SHORT",
        thesis_type=ThesisType.SNAP_TURN,
        thesis_summary="US derivative bearish turn on MGC.",
        session_tags=("US",),
        regime_tags=("SNAP_TURN_REVERSAL",),
    ),
    "US_LATE_PAUSE_RESUME_LONG_V1": _mgc_template(
        strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_us_late_pause_resume_long",
        side="LONG",
        thesis_type=ThesisType.OTHER,
        thesis_summary="US-late pause/resume long continuation.",
        session_tags=("US_LATE",),
        regime_tags=("TREND_PARTICIPATION",),
    ),
    "MNQ_US_DERIVATIVE_BEAR_TURN_V1": _mnq_template(
        strategy_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        lane_id="mnq_us_derivative_bear_turn",
        side="SHORT",
        thesis_type=ThesisType.SNAP_TURN,
        thesis_summary="US derivative bearish turn on MNQ.",
        session_tags=("US",),
        regime_tags=("SNAP_TURN_REVERSAL",),
    ),
    "MNQ_FIRST_BEAR_SNAP_TURN_V1": _mnq_template(
        strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        lane_id="mnq_first_bear_snap_turn",
        side="SHORT",
        thesis_type=ThesisType.SNAP_TURN,
        thesis_summary="First bearish MNQ snap-turn.",
        session_tags=("GLOBAL",),
        regime_tags=("SNAP_TURN_REVERSAL",),
    ),
    "MNQ_FIRST_BULL_SNAP_TURN_V1": _mnq_template(
        strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
        lane_id="mnq_first_bull_snap_turn",
        side="LONG",
        thesis_type=ThesisType.SNAP_TURN,
        thesis_summary="First bullish MNQ snap-turn.",
        session_tags=("GLOBAL",),
        regime_tags=("SNAP_TURN_REVERSAL",),
    ),
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1": _StrategyContractTemplate(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
        lane_id="mnq_london_open_active_participation_long",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        side="LONG",
        quantity=1,
        thesis_type=ThesisType.TREND_PARTICIPATION,
        thesis_summary=(
            "PAPER-only London Open active participation long evidence lane: enter when MNQ is above "
            "VWAP or the 03:00 ET London open and the latest completed runtime bar confirms upward participation."
        ),
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_EXIT",
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        invalidation_conditions=("price_loses_london_open_reference", "opposite_recent_close_confirms"),
        max_hold_policy="3_COMPLETED_5M_BARS",
        expected_hold_bars_5m=3,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        strategy_family="paper_active_evidence",
        session_tags=("LONDON_OPEN",),
        regime_tags=("PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE", "TREND_PARTICIPATION"),
    ),
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1": _StrategyContractTemplate(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        lane_id="mnq_london_open_active_participation_short",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        side="SHORT",
        quantity=1,
        thesis_type=ThesisType.TREND_PARTICIPATION,
        thesis_summary=(
            "PAPER-only London Open active participation short evidence lane: enter when MNQ is below "
            "VWAP or the 03:00 ET London open and the latest completed runtime bar confirms downward participation."
        ),
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_EXIT",
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        invalidation_conditions=("price_recovers_london_open_reference", "opposite_recent_close_confirms"),
        max_hold_policy="3_COMPLETED_5M_BARS",
        expected_hold_bars_5m=3,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        strategy_family="paper_active_evidence",
        session_tags=("LONDON_OPEN",),
        regime_tags=("PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE", "TREND_PARTICIPATION"),
    ),
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1": _StrategyContractTemplate(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
        lane_id="mes_london_open_active_participation_long",
        instrument_family="MES",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        side="LONG",
        quantity=1,
        thesis_type=ThesisType.TREND_PARTICIPATION,
        thesis_summary=(
            "PAPER-only London Open active participation long evidence lane: enter when MES is above "
            "VWAP or the 03:00 ET London open and the latest completed runtime bar confirms upward participation."
        ),
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_EXIT",
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        exit_profile_id=MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        invalidation_conditions=("price_loses_london_open_reference", "opposite_recent_close_confirms"),
        max_hold_policy="3_COMPLETED_5M_BARS",
        expected_hold_bars_5m=3,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        strategy_family="paper_active_evidence",
        session_tags=("LONDON_OPEN",),
        regime_tags=("PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE", "TREND_PARTICIPATION"),
    ),
    "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1": _StrategyContractTemplate(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        lane_id="mes_london_open_active_participation_short",
        instrument_family="MES",
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        expiry="20260618",
        side="SHORT",
        quantity=1,
        thesis_type=ThesisType.TREND_PARTICIPATION,
        thesis_summary=(
            "PAPER-only London Open active participation short evidence lane: enter when MES is below "
            "VWAP or the 03:00 ET London open and the latest completed runtime bar confirms downward participation."
        ),
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="LONDON_OPEN_ACTIVE_EVIDENCE_TIMEBOX_EXIT",
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        exit_profile_id=MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        invalidation_conditions=("price_recovers_london_open_reference", "opposite_recent_close_confirms"),
        max_hold_policy="3_COMPLETED_5M_BARS",
        expected_hold_bars_5m=3,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        strategy_family="paper_active_evidence",
        session_tags=("LONDON_OPEN",),
        regime_tags=("PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE", "TREND_PARTICIPATION"),
    ),
    "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1": _StrategyContractTemplate(
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
        lane_id="mnq_london_late_active_participation_short",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        side="SHORT",
        quantity=1,
        thesis_type=ThesisType.TREND_PARTICIPATION,
        thesis_summary=(
            "PAPER-only London Late active participation short evidence lane: enter when MNQ is below "
            "VWAP or the canonical 05:30 ET London Late reference and the latest completed runtime bar "
            "confirms downward participation."
        ),
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="LONDON_LATE_ACTIVE_EVIDENCE_TIMEBOX_EXIT",
        managed_exit_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1",
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_V1,
        invalidation_conditions=("price_recovers_london_late_reference", "opposite_recent_close_confirms"),
        max_hold_policy="3_COMPLETED_5M_BARS",
        expected_hold_bars_5m=3,
        conflict_group="equity_index_mnq_mes_london_late_active_evidence",
        strategy_family="paper_active_evidence",
        session_tags=("LONDON_LATE",),
        regime_tags=("PAPER_ONLY_LONDON_LATE_ACTIVE_EVIDENCE_LANE", "TREND_PARTICIPATION"),
    ),
    "GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1": _StrategyContractTemplate(
        strategy_id="GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1",
        lane_id="globex_reopen_mnq_1m_strong_green_second_candle_confirm_shadow",
        instrument_family="MNQ",
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        expiry="20260618",
        side="LONG",
        quantity=1,
        thesis_type=ThesisType.TREND_PARTICIPATION,
        thesis_summary=(
            "Shadow-only Globex reopen continuation thesis: after the 18:00 ET futures reopen, "
            "observe MNQ when the first completed 1m candle is strong green and the second candle "
            "confirms without immediate rejection, using a 60m continuation benchmark."
        ),
        expected_hold_type=ExpectedHoldType.TIMEBOXED,
        intended_exit_family="GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_EXIT_SHADOW",
        managed_exit_policy_id="GLOBEX_REOPEN_FIRST_CANDLE_60M_TIMEBOX_SHADOW_EXIT_V1",
        exit_profile_id=MNQ_GLOBEX_REOPEN_FIRST_CANDLE_TIMEBOX_60M_SHADOW_V1,
        invalidation_conditions=("second_candle_rejects_first_candle", "first_candle_not_strong_green"),
        max_hold_policy="12_COMPLETED_5M_BARS",
        expected_hold_bars_5m=12,
        conflict_group="equity_index_nasdaq_mnq_nq",
        strategy_family="globex_reopen_first_candle_continuation_shadow",
        session_tags=("GLOBEX_REOPEN", "ASIA_EARLY"),
        regime_tags=("FIRST_CANDLE_CONTINUATION", "TREND_PARTICIPATION", "SHADOW_CANDIDATE"),
    ),
}


REQUIRED_TOP_LEVEL_FIELDS = (
    "strategy_id",
    "lane_id",
    "instrument_family",
    "contract_key",
    "local_symbol",
    "con_id",
    "expiry",
    "side",
    "quantity",
    "pyramiding_policy",
    "conflict_group",
)
REQUIRED_NESTED_FIELDS = {
    "trade_thesis": ("thesis_type", "invalidation_conditions"),
    "hold_policy": ("expected_hold_type", "max_hold_policy"),
    "exit_policy": ("intended_exit_family", "managed_exit_policy_id", "exit_profile_id"),
    "attribution_tags": ("eligible_for_alpha_exit_analysis", "strategy_family"),
}


def position_intent_from_template(template: _StrategyContractTemplate) -> PositionIntent:
    return PositionIntent(
        strategy_id=template.strategy_id,
        lane_id=template.lane_id,
        instrument_family=template.instrument_family,
        contract_key=template.contract_key,
        local_symbol=template.local_symbol,
        con_id=template.con_id,
        expiry=template.expiry,
        side=template.side,
        quantity=template.quantity,
        trade_thesis=TradeThesis(
            thesis_type=template.thesis_type.value,
            thesis_summary=template.thesis_summary,
            invalidation_conditions=template.invalidation_conditions,
            regime_tags=template.regime_tags,
        ),
        hold_policy=HoldPolicy(
            expected_hold_type=template.expected_hold_type.value,
            max_hold_policy=template.max_hold_policy,
            expected_hold_bars_5m=template.expected_hold_bars_5m,
        ),
        exit_policy=ExitPolicy(
            intended_exit_family=template.intended_exit_family,
            managed_exit_policy_id=template.managed_exit_policy_id,
            exit_profile_id=template.exit_profile_id,
        ),
        order_policy=OrderPolicy(),
        pyramiding_policy=template.pyramiding_policy,
        conflict_group=template.conflict_group,
        attribution_tags=AttributionTags(
            eligible_for_alpha_exit_analysis=True,
            strategy_family=template.strategy_family,
            session_tags=template.session_tags,
            regime_tags=template.regime_tags,
        ),
    )


def validate_position_intent(intent: PositionIntent | Mapping[str, Any]) -> dict[str, Any]:
    payload = _to_payload(intent)
    missing: list[str] = []
    invalid: list[str] = []
    for field_name in REQUIRED_TOP_LEVEL_FIELDS:
        if _empty(payload.get(field_name)):
            missing.append(field_name)
    for parent, field_names in REQUIRED_NESTED_FIELDS.items():
        nested = payload.get(parent)
        if not isinstance(nested, Mapping):
            missing.append(parent)
            continue
        for field_name in field_names:
            if _empty(nested.get(field_name)):
                missing.append(f"{parent}.{field_name}")

    thesis_type = _nested_text(payload, "trade_thesis", "thesis_type")
    if thesis_type and thesis_type not in {item.value for item in ThesisType}:
        invalid.append("trade_thesis.thesis_type")
    hold_type = _nested_text(payload, "hold_policy", "expected_hold_type")
    if hold_type and hold_type not in {item.value for item in ExpectedHoldType}:
        invalid.append("hold_policy.expected_hold_type")
    if payload.get("live_money_eligible") is not False:
        invalid.append("live_money_eligible")
    if payload.get("paper_proof_invoked") is not False:
        invalid.append("paper_proof_invoked")
    if payload.get("submit_authority") is not False or payload.get("broker_mutation_allowed") is not False:
        invalid.append("read_only_authority_flags")

    classification = POSITION_INTENT_VALID if not missing and not invalid else POSITION_INTENT_INVALID
    return {
        "classification": classification,
        "valid": classification == POSITION_INTENT_VALID,
        "strategy_id": payload.get("strategy_id"),
        "lane_id": payload.get("lane_id"),
        "missing_metadata": missing,
        "invalid_metadata": invalid,
        "read_only": True,
        "submit_authority": False,
        "broker_mutation_allowed": False,
        "lifecycle_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def build_position_intent_contract_audit(
    *,
    config: PositionIntentContractAuditConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    roster = _read_json(config.resolve(config.roster_path))
    enabled_strategy_ids = tuple(str(item) for item in roster.get("enabled_strategy_ids") or [])
    strategy_ids = enabled_strategy_ids or tuple(APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES)
    rows: list[dict[str, Any]] = []
    for strategy_id in strategy_ids:
        template = APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES.get(strategy_id)
        if template is None:
            rows.append(
                {
                    "classification": POSITION_INTENT_INVALID,
                    "valid": False,
                    "strategy_id": strategy_id,
                    "lane_id": None,
                    "missing_metadata": ["strategy_contract_template"],
                    "invalid_metadata": [],
                    "read_only": True,
                    "submit_authority": False,
                    "broker_mutation_allowed": False,
                    "lifecycle_authority": False,
                    "live_money_eligible": False,
                    "paper_proof_invoked": False,
                }
            )
            continue
        intent = position_intent_from_template(template)
        validation = validate_position_intent(intent)
        rows.append(
            {
                **validation,
                "position_intent": _to_payload(intent),
                "strategy_hold_exit_policy": strategy_hold_exit_policy_for(strategy_id),
            }
        )

    missing_count = sum(1 for row in rows if row.get("valid") is not True)
    classification = POSITION_INTENT_CONTRACT_READY if missing_count == 0 else POSITION_INTENT_CONTRACT_GAPS_FOUND
    return {
        "schema_version": "track_b_position_intent_contract_audit_v1",
        "generated_at": actual_now.isoformat(),
        "classification": classification,
        "read_only": True,
        "submit_authority": False,
        "broker_mutation_allowed": False,
        "lifecycle_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "roster_path": str(config.resolve(config.roster_path)),
        "strategy_count": len(rows),
        "valid_strategy_count": len(rows) - missing_count,
        "gap_strategy_count": missing_count,
        "strategies": rows,
        "integration_targets": {
            "exit_attribution_framework": {
                "mode": "read_only_context",
                "fields": ["eligible_for_alpha_exit_analysis", "thesis_type", "intended_exit_family"],
            },
            "managed_lifecycle_reports": {
                "mode": "read_only_context",
                "fields": ["position_intent", "trade_thesis", "hold_policy", "exit_policy"],
            },
            "exit_policy_v2_shadow_recommendations": {
                "mode": "read_only_context",
                "fields": ["expected_hold_type", "invalidation_conditions", "attribution_tags"],
            },
            "hold_exit_shadow_engine": {
                "mode": "read_only_context",
                "fields": [
                    "hold_policy_id",
                    "exit_policy_family",
                    "thesis_type",
                    "expected_hold_type",
                    "expected_hold_bars_5m",
                    "invalidation_conditions",
                    "intended_exit_family",
                ],
            },
        },
        "artifact_paths": {
            "audit": str(config.resolve(config.output_path)),
            "docs": str(config.repo_root / "docs/track_b_position_intent_contract.md"),
        },
    }


def write_position_intent_contract_audit(
    *,
    config: PositionIntentContractAuditConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, payload)
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit read-only Track B PositionIntent contract coverage.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--roster-path", default=str(DEFAULT_ROSTER_PATH))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = PositionIntentContractAuditConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        roster_path=Path(args.roster_path),
        output_path=Path(args.output_path),
    )
    payload = build_position_intent_contract_audit(config=config)
    write_position_intent_contract_audit(config=config, payload=payload)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']}: {payload['valid_strategy_count']}/{payload['strategy_count']} strategies valid")
    return 0 if payload["classification"] == POSITION_INTENT_CONTRACT_READY else 2


def _to_payload(intent: PositionIntent | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(intent, Mapping):
        return dict(intent)
    return asdict(intent)


def _nested_text(payload: Mapping[str, Any], parent: str, field_name: str) -> str:
    nested = payload.get(parent)
    if not isinstance(nested, Mapping):
        return ""
    return str(nested.get(field_name) or "")


def _empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return len(value) == 0
    return False


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
