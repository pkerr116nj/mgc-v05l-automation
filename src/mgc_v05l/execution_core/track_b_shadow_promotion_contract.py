"""Track B shadow-to-PAPER promotion contract.

This module is declarative.  It does not submit, cancel, close, modify,
flatten, invoke paper_proof, or create a live-money route.  A promoted row only
becomes broker-authoritative when the guarded PAPER runtime roster explicitly
includes the promoted strategy id and all normal runtime gates pass.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_exit_strategy_roster import (
    MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
    MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
    MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
)
from mgc_v05l.execution_core.track_b_position_intent_contract import APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES
from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import (
    APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES,
)
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
)
from mgc_v05l.execution_core.track_b_strategy_registry import (
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    resolve_track_b_strategy_registry_entry,
)


PROMOTION_CONTRACT_READY = "PROMOTION_CONTRACT_READY"
PROMOTION_CONTRACT_BLOCKED = "PROMOTION_CONTRACT_BLOCKED"
PROMOTION_CANDIDATE_GUARDED_PAPER_READY = "PROMOTION_CANDIDATE_GUARDED_PAPER_READY"
PROMOTION_CANDIDATE_SHADOW_ONLY = "PROMOTION_CANDIDATE_SHADOW_ONLY"
PROMOTION_CANDIDATE_BLOCKED = "PROMOTION_CANDIDATE_BLOCKED"

ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID = "ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1"
ATP_MGC_ASIA_EDGE_V1_PROMOTED_ID = "atp_companion_v1__paper_mgc_asia__edge_v1"
ATP_MGC_ASIA_PROMOTION_1_075R_PROMOTED_ID = "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only"
ATP_MGC_ASIA_PROMOTION_1_075R_5M_PROMOTED_ID = "atp_companion_v1__paper_mgc_asia__promotion_1_075r_favorable_only_5m"
ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID = "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1"
PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID = "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1"
PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID = "PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1"
US_DERIVATIVE_BEAR_TURN_PROMOTED_ID = "US_DERIVATIVE_BEAR_TURN_V1"
TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND = "track_b_rule_runner_paper_strategy_engine"
ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND = "atp_companion_benchmark_paper"
DEFAULT_TIMESTAMP_COHERENCE_SECONDS = 360


@dataclass(frozen=True)
class ShadowPromotionCandidate:
    shadow_candidate_family: str
    promoted_strategy_id: str
    lane_id: str
    instrument_family: str
    side: str
    session_eligibility: tuple[str, ...]
    contract_key: str
    local_symbol: str
    con_id: int
    lifecycle_policy_id: str
    exit_profile_id: str
    pyramiding_policy: str
    conflict_group: str
    evidence_summary: Mapping[str, Any]
    experimental_reason: str
    runtime_kind: str = TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND
    display_name: str | None = None
    identity_components: tuple[str, ...] = ()
    long_sources: tuple[str, ...] = ()
    short_sources: tuple[str, ...] = ()
    lane_mode: str = "TRACK_B_SHADOW_PROMOTED_PAPER_CANDIDATE"
    strategy_family: str | None = None
    strategy_identity_root: str | None = None
    observed_instruments: tuple[str, ...] = ()
    quality_bucket_policy: str | None = None
    experimental_status: str = "paper_candidate"
    observer_variant_id: str | None = None
    observer_side: str | None = None
    allow_pre_5m_context_participation: bool | None = None
    atp_context_timeframe: str | None = None
    candidate_id: str | None = None
    candidate_origin: str | None = None
    input_event_path: str | None = None
    max_position_quantity: int = 1
    max_concurrent_entries: int = 1
    max_adds_after_entry: int = 0
    participation_policy: str = "SINGLE_ENTRY_ONLY"
    catastrophic_open_loss: str = "-500"
    point_value: str = "10"
    recommended: bool = True
    broker_authority_requested: bool = False
    submit_allowed: bool = False
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False


PROMOTION_CANDIDATES: Mapping[str, ShadowPromotionCandidate] = {
    ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1",
        promoted_strategy_id=ASIAN_DRIFT_LATE_JOIN_PROMOTED_ID,
        lane_id="mgc_asian_drift_late_join_missing_anchor_long",
        instrument_family="MGC",
        side="LONG",
        session_eligibility=("ASIA",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "timestamp_locked_valid_outcomes": 5,
            "missed_winner_count": 4,
            "avoided_loser_count": 1,
            "near_miss_score": 0.88,
        },
        experimental_reason="missing_18_00_et_session_anchor_context",
        display_name="MGC / Asian Drift late-join missing-anchor long / PAPER experiment",
        identity_components=("paper", "mgc", "asian_drift", "late_join_missing_anchor"),
    ),
    ATP_MGC_ASIA_EDGE_V1_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="ATP_COMPANION_MGC_ASIA_EDGE_V1",
        promoted_strategy_id=ATP_MGC_ASIA_EDGE_V1_PROMOTED_ID,
        lane_id="atp_companion_v1_mgc_asia_promotion_edge_v1",
        instrument_family="MGC",
        side="SHORT",
        session_eligibility=("ASIA",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "atp_lifecycle_mapping_status": "ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY",
            "source_shadow_classification": "ATP_SHADOW_CONFIRMS_LIVE_SIGNAL",
            "score": 0.765909,
            "confidence": "MEDIUM",
        },
        experimental_reason="atp_mgc_asia_edge_v1_paper_leak_test_cohort",
        runtime_kind=ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND,
        display_name="ATP Companion Candidate / MGC / Asia / Edge v1 / PAPER",
        identity_components=("paper", "mgc", "asia", "edge_v1"),
        short_sources=("trend_participation.atp_v1_short_pullback_continuation.short.base",),
        lane_mode="TRACK_B_PROMOTED_ATP_PAPER_CANDIDATE",
        strategy_family="active_trend_participation_engine",
        strategy_identity_root="ATP_COMPANION_V1",
        observed_instruments=("MGC",),
        quality_bucket_policy="HIGH_ONLY",
        observer_variant_id="trend_participation.pullback_continuation.long.conservative",
        observer_side="SHORT",
        allow_pre_5m_context_participation=True,
        candidate_id="edge_v1",
        candidate_origin="atp_candidate_refinement",
        catastrophic_open_loss="-400",
    ),
    ATP_MGC_ASIA_PROMOTION_1_075R_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="ATP_COMPANION_MGC_ASIA_PROMOTION_1_075R_FAVORABLE_ONLY",
        promoted_strategy_id=ATP_MGC_ASIA_PROMOTION_1_075R_PROMOTED_ID,
        lane_id="atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only_prodline_v1",
        instrument_family="MGC",
        side="SHORT",
        session_eligibility=("ASIA",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "atp_lifecycle_mapping_status": "ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY",
            "source_shadow_classification": "ATP_SHADOW_CONFIRMS_LIVE_SIGNAL",
            "score": 0.765909,
            "confidence": "MEDIUM",
        },
        experimental_reason="atp_mgc_asia_promotion_1_075r_favorable_only_paper_leak_test_cohort_clean_lane_state",
        runtime_kind=ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND,
        display_name="ATP Companion Candidate / MGC / Asia / Promotion 1 +0.75R Favorable Only [3m] / PAPER",
        identity_components=("paper", "mgc", "asia", "promotion_1_075r_favorable_only", "prodline_v1"),
        short_sources=("trend_participation.atp_v1_short_pullback_continuation.short.base",),
        lane_mode="TRACK_B_PROMOTED_ATP_PAPER_CANDIDATE",
        strategy_family="active_trend_participation_engine",
        strategy_identity_root="ATP_COMPANION_V1",
        observed_instruments=("MGC",),
        quality_bucket_policy="VWAP_FAVORABLE_ONLY",
        observer_variant_id="trend_participation.pullback_continuation.long.conservative",
        observer_side="SHORT",
        allow_pre_5m_context_participation=True,
        atp_context_timeframe="3m",
        candidate_id="promotion_1_075r_favorable_only",
        candidate_origin="atp_promotion_add_review",
    ),
    ATP_MGC_ASIA_PROMOTION_1_075R_5M_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="ATP_COMPANION_MGC_ASIA_PROMOTION_1_075R_FAVORABLE_ONLY_5M",
        promoted_strategy_id=ATP_MGC_ASIA_PROMOTION_1_075R_5M_PROMOTED_ID,
        lane_id="atp_companion_v1_mgc_asia_promotion_1_075r_favorable_only_5m_prodline_v1",
        instrument_family="MGC",
        side="SHORT",
        session_eligibility=("ASIA",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "atp_lifecycle_mapping_status": "ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY",
            "source_shadow_classification": "ATP_SHADOW_CONFIRMS_LIVE_SIGNAL",
            "score": 0.765909,
            "confidence": "MEDIUM",
        },
        experimental_reason="atp_mgc_asia_promotion_1_075r_favorable_only_5m_paper_leak_test_cohort_clean_lane_state",
        runtime_kind=ATP_COMPANION_BENCHMARK_PAPER_RUNTIME_KIND,
        display_name="ATP Companion Candidate / MGC / Asia / Promotion 1 +0.75R Favorable Only [5m] / PAPER",
        identity_components=("paper", "mgc", "asia", "promotion_1_075r_favorable_only", "context_5m", "prodline_v1"),
        short_sources=("trend_participation.atp_v1_short_pullback_continuation.short.base",),
        lane_mode="TRACK_B_PROMOTED_ATP_PAPER_CANDIDATE",
        strategy_family="active_trend_participation_engine",
        strategy_identity_root="ATP_COMPANION_V1",
        observed_instruments=("MGC",),
        quality_bucket_policy="VWAP_FAVORABLE_ONLY",
        observer_variant_id="trend_participation.pullback_continuation.long.conservative",
        observer_side="SHORT",
        allow_pre_5m_context_participation=True,
        atp_context_timeframe="5m",
        candidate_id="promotion_1_075r_favorable_only",
        candidate_origin="atp_promotion_add_review",
    ),
    ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="TRACK_B_APPROVED_ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        promoted_strategy_id=ASIA_EARLY_PAUSE_RESUME_SHORT_PROMOTED_ID,
        lane_id="mgc_asia_early_pause_resume_short",
        instrument_family="MGC",
        side="SHORT",
        session_eligibility=("ASIA",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "source": "approved_track_b_strategy_registry",
            "coverage": "asia_early_short_side",
            "runtime_event_artifact": "latest_asia_early_pause_resume_short_event_envelope",
        },
        experimental_reason="production_line_leak_test_short_side_asia_early",
        display_name="MGC / Asia early pause-resume short / PAPER",
        identity_components=("paper", "mgc", "asia_early", "pause_resume_short"),
        input_event_path="outputs/track_b_execution_core/session_strategy_state/latest_asia_early_pause_resume_short_event_envelope.json",
    ),
    LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="TRACK_B_APPROVED_LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        promoted_strategy_id=LONDON_LATE_PAUSE_RESUME_SHORT_PROMOTED_ID,
        lane_id="mgc_london_late_pause_resume_short",
        instrument_family="MGC",
        side="SHORT",
        session_eligibility=("LONDON_LATE",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "source": "approved_track_b_strategy_registry",
            "coverage": "london_late_short_side",
            "runtime_event_artifact": "latest_london_late_pause_resume_short_event_envelope",
        },
        experimental_reason="production_line_leak_test_london_late_short_coverage",
        display_name="MGC / London-late pause-resume short / PAPER",
        identity_components=("paper", "mgc", "london_late", "pause_resume_short"),
        input_event_path="outputs/track_b_execution_core/session_strategy_state/latest_london_late_pause_resume_short_event_envelope.json",
    ),
    US_DERIVATIVE_BEAR_TURN_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="TRACK_B_APPROVED_US_DERIVATIVE_BEAR_TURN_V1",
        promoted_strategy_id=US_DERIVATIVE_BEAR_TURN_PROMOTED_ID,
        lane_id="mgc_us_derivative_bear_turn",
        instrument_family="MGC",
        side="SHORT",
        session_eligibility=("US",),
        contract_key="MGC-202606",
        local_symbol="MGCM6",
        con_id=712565978,
        lifecycle_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        exit_profile_id=MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="gold_mgc_gc",
        evidence_summary={
            "source": "approved_track_b_strategy_registry",
            "coverage": "us_short_side",
            "runtime_event_artifact": "latest_us_derivative_bear_turn_event_envelope",
        },
        experimental_reason="production_line_leak_test_us_derivative_bear_turn_short",
        display_name="MGC / US derivative bear turn short / PAPER",
        identity_components=("paper", "mgc", "us", "derivative_bear_turn"),
        input_event_path="outputs/track_b_execution_core/session_strategy_state/latest_us_derivative_bear_turn_event_envelope.json",
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        promoted_strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_LONG_PROMOTED_ID,
        lane_id="mnq_london_open_active_participation_long",
        instrument_family="MNQ",
        side="LONG",
        session_eligibility=("LONDON_OPEN",),
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        lifecycle_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        evidence_summary={
            "source": "paper_london_open_active_evidence_cohort",
            "condition": "03:05-05:30_ET_close_above_vwap_or_03_00_london_open",
            "benchmark_exit": "60m_timebox",
            "classification": "PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
            "session_anchor": "LONDON_0300_OPEN",
            "purpose": "controlled_real_paper_lifecycle_evidence_generation_london_open",
        },
        experimental_reason="paper_only_london_open_active_evidence_generation_mnq_long",
        display_name="MNQ / London Open active participation long / PAPER evidence",
        identity_components=("paper", "mnq", "london_open", "active_participation_long"),
        lane_mode="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        strategy_family="paper_active_evidence",
        observed_instruments=("MNQ",),
        experimental_status="paper_evidence_generation",
        input_event_path="outputs/track_b_execution_core/london_open_active_evidence/latest_mnq_london_open_active_participation_long_event_envelope.json",
        point_value="2",
        catastrophic_open_loss="-250",
    ),
    PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        promoted_strategy_id=PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_SHORT_PROMOTED_ID,
        lane_id="mnq_london_open_active_participation_short",
        instrument_family="MNQ",
        side="SHORT",
        session_eligibility=("LONDON_OPEN",),
        contract_key="MNQ-202606",
        local_symbol="MNQM6",
        con_id=770561201,
        lifecycle_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        exit_profile_id=MNQ_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        evidence_summary={
            "source": "paper_london_open_active_evidence_cohort",
            "condition": "03:05-05:30_ET_close_below_vwap_or_03_00_london_open",
            "benchmark_exit": "60m_timebox",
            "classification": "PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
            "session_anchor": "LONDON_0300_OPEN",
            "purpose": "controlled_real_paper_lifecycle_evidence_generation_london_open",
        },
        experimental_reason="paper_only_london_open_active_evidence_generation_mnq_short",
        display_name="MNQ / London Open active participation short / PAPER evidence",
        identity_components=("paper", "mnq", "london_open", "active_participation_short"),
        lane_mode="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        strategy_family="paper_active_evidence",
        observed_instruments=("MNQ",),
        experimental_status="paper_evidence_generation",
        input_event_path="outputs/track_b_execution_core/london_open_active_evidence/latest_mnq_london_open_active_participation_short_event_envelope.json",
        point_value="2",
        catastrophic_open_loss="-250",
    ),
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        promoted_strategy_id=PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_LONG_PROMOTED_ID,
        lane_id="mes_london_open_active_participation_long",
        instrument_family="MES",
        side="LONG",
        session_eligibility=("LONDON_OPEN",),
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        lifecycle_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        exit_profile_id=MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        evidence_summary={
            "source": "paper_london_open_active_evidence_cohort",
            "condition": "03:05-05:30_ET_close_above_vwap_or_03_00_london_open",
            "benchmark_exit": "60m_timebox",
            "classification": "PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
            "session_anchor": "LONDON_0300_OPEN",
            "purpose": "controlled_real_paper_lifecycle_evidence_generation_london_open",
        },
        experimental_reason="paper_only_london_open_active_evidence_generation_mes_long",
        display_name="MES / London Open active participation long / PAPER evidence",
        identity_components=("paper", "mes", "london_open", "active_participation_long"),
        lane_mode="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        strategy_family="paper_active_evidence",
        observed_instruments=("MES",),
        experimental_status="paper_evidence_generation",
        input_event_path="outputs/track_b_execution_core/london_open_active_evidence/latest_mes_london_open_active_participation_long_event_envelope.json",
        point_value="5",
        catastrophic_open_loss="-300",
    ),
    PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID: ShadowPromotionCandidate(
        shadow_candidate_family="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        promoted_strategy_id=PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_SHORT_PROMOTED_ID,
        lane_id="mes_london_open_active_participation_short",
        instrument_family="MES",
        side="SHORT",
        session_eligibility=("LONDON_OPEN",),
        contract_key="MES-202606",
        local_symbol="MESM6",
        con_id=770561194,
        lifecycle_policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        exit_profile_id=MES_GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_V1,
        pyramiding_policy=PYRAMIDING_NOT_ALLOWED_REVIEW_REQUIRED,
        conflict_group="equity_index_mnq_mes_london_open_active_evidence",
        evidence_summary={
            "source": "paper_london_open_active_evidence_cohort",
            "condition": "03:05-05:30_ET_close_below_vwap_or_03_00_london_open",
            "benchmark_exit": "60m_timebox",
            "classification": "PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
            "session_anchor": "LONDON_0300_OPEN",
            "purpose": "controlled_real_paper_lifecycle_evidence_generation_london_open",
        },
        experimental_reason="paper_only_london_open_active_evidence_generation_mes_short",
        display_name="MES / London Open active participation short / PAPER evidence",
        identity_components=("paper", "mes", "london_open", "active_participation_short"),
        lane_mode="PAPER_ONLY_LONDON_OPEN_ACTIVE_EVIDENCE_LANE",
        strategy_family="paper_active_evidence",
        observed_instruments=("MES",),
        experimental_status="paper_evidence_generation",
        input_event_path="outputs/track_b_execution_core/london_open_active_evidence/latest_mes_london_open_active_participation_short_event_envelope.json",
        point_value="5",
        catastrophic_open_loss="-300",
    ),
}

REMAINING_SHADOW_ONLY_EXCEPTIONS: tuple[dict[str, Any], ...] = (
    {
        "candidate_group": "ATP_GC_CANDIDATES",
        "instrument_family": "GC",
        "classification": PROMOTION_CANDIDATE_BLOCKED,
        "shadow_only_reason": "missing_atp_compatible_managed_exit_profile",
        "candidate_count": 7,
        "submit_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    },
    {
        "candidate_group": "ATP_PL_CANDIDATES",
        "instrument_family": "PL",
        "classification": PROMOTION_CANDIDATE_BLOCKED,
        "shadow_only_reason": "missing_exit_profile_or_current_actionable_shadow_signal",
        "candidate_count": 3,
        "submit_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    },
    {
        "candidate_group": "GENERAL_NEAR_MISS_C_GRADE",
        "instrument_family": "MGC",
        "classification": PROMOTION_CANDIDATE_BLOCKED,
        "shadow_only_reason": "c_grade_or_missing_timestamp_locked_forward_evidence",
        "candidate_count": 47,
        "submit_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    },
)


def build_shadow_promotion_contract_report(roster: Mapping[str, Any] | None = None) -> dict[str, Any]:
    roster = roster or {}
    enabled = {str(item) for item in roster.get("enabled_strategy_ids") or []}
    rows = [_candidate_report(candidate, enabled) for candidate in PROMOTION_CANDIDATES.values()]
    blocked = [row for row in rows if row["classification"] == PROMOTION_CANDIDATE_BLOCKED]
    return {
        "schema_version": "track_b_shadow_promotion_contract_v1",
        "classification": PROMOTION_CONTRACT_BLOCKED if blocked else PROMOTION_CONTRACT_READY,
        "read_only_contract": True,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_cancel_flatten_allowed": False,
        "unguarded_broker_mutation_allowed": False,
        "promotion_candidates": rows,
        "broker_authoritative_promoted_strategy_ids": [
            row["promoted_strategy_id"]
            for row in rows
            if row["classification"] == PROMOTION_CANDIDATE_GUARDED_PAPER_READY
        ],
        "shadow_only_strategy_ids": [
            row["promoted_strategy_id"]
            for row in rows
            if row["classification"] == PROMOTION_CANDIDATE_SHADOW_ONLY
        ],
        "remaining_shadow_only_exceptions": [dict(row) for row in REMAINING_SHADOW_ONLY_EXCEPTIONS],
    }


def promotion_contract_for_strategy(strategy_id: str) -> dict[str, Any] | None:
    candidate = PROMOTION_CANDIDATES.get(strategy_id)
    return _candidate_report(candidate, {strategy_id}) if candidate else None


def promoted_probationary_paper_lane_rows(roster: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    roster = roster or {}
    enabled = {str(item) for item in roster.get("enabled_strategy_ids") or []}
    rows: list[dict[str, Any]] = []
    for candidate in PROMOTION_CANDIDATES.values():
        report = _candidate_report(candidate, enabled)
        if report["classification"] == PROMOTION_CANDIDATE_GUARDED_PAPER_READY:
            rows.append(_probationary_paper_lane_row(candidate))
    return rows


def write_shadow_promotion_contract_report(payload: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)
    return path


def _candidate_report(candidate: ShadowPromotionCandidate, enabled: set[str]) -> dict[str, Any]:
    payload = asdict(candidate)
    registry = resolve_track_b_strategy_registry_entry(
        rule_mode=candidate.promoted_strategy_id,
        rule_id=candidate.promoted_strategy_id,
        strategy_id=candidate.promoted_strategy_id,
    )
    blockers: list[str] = []
    if registry is None:
        blockers.append("missing_strategy_registry_entry")
    elif registry.live_money_eligible is not False or registry.paper_eligible is not True:
        blockers.append("strategy_registry_not_paper_only")
    if candidate.promoted_strategy_id not in APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES:
        blockers.append("missing_position_intent_contract")
    if candidate.promoted_strategy_id not in APPROVED_TRACK_B_STRATEGY_HOLD_EXIT_POLICIES:
        blockers.append("missing_hold_exit_policy_mapping")
    if candidate.live_money_eligible is not False:
        blockers.append("promotion_candidate_live_money_not_false")
    if candidate.paper_proof_invoked is not False:
        blockers.append("promotion_candidate_paper_proof_not_false")

    if blockers:
        classification = PROMOTION_CANDIDATE_BLOCKED
    elif candidate.promoted_strategy_id in enabled:
        classification = PROMOTION_CANDIDATE_GUARDED_PAPER_READY
    else:
        classification = PROMOTION_CANDIDATE_SHADOW_ONLY

    return {
        **payload,
        "classification": classification,
        "blockers": blockers,
        "guarded_roster_enabled": candidate.promoted_strategy_id in enabled,
        "submit_allowed": classification == PROMOTION_CANDIDATE_GUARDED_PAPER_READY,
        "broker_mutation_allowed": False,
        "lifecycle_authority": classification == PROMOTION_CANDIDATE_GUARDED_PAPER_READY,
        "not_live_money_authority": True,
        "paper_proof_invoked": False,
        "probationary_paper_lane_row": _probationary_paper_lane_row(candidate),
    }


def _probationary_paper_lane_row(candidate: ShadowPromotionCandidate) -> dict[str, Any]:
    source = candidate.promoted_strategy_id
    long_sources = list(candidate.long_sources) or ([source] if candidate.side == "LONG" else [])
    short_sources = list(candidate.short_sources) or ([source] if candidate.side == "SHORT" else [])
    runtime_overlay_params: dict[str, Any] = {
        "rule_id": candidate.promoted_strategy_id,
        "rule_mode": candidate.promoted_strategy_id,
        "strategy_id": candidate.promoted_strategy_id,
        "lane_id": candidate.lane_id,
        "entry_source": candidate.promoted_strategy_id,
        "output_root": f"outputs/track_b_execution_core/track_b_strategy_rule_runner/{candidate.lane_id}",
        "expected_account_id": "DUM882026",
        "experimental_reason": candidate.experimental_reason,
    }
    if candidate.runtime_kind == TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND:
        runtime_overlay_params.update(
            {
                "input_event_path": candidate.input_event_path
                or "outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json",
                "require_timestamp_coherence": True,
                "timestamp_tolerance_seconds": DEFAULT_TIMESTAMP_COHERENCE_SECONDS,
            }
        )
    row = {
        "lane_id": candidate.lane_id,
        "display_name": candidate.display_name or f"{candidate.instrument_family} / {candidate.promoted_strategy_id} / PAPER",
        "symbol": candidate.instrument_family,
        "standalone_strategy_id": candidate.promoted_strategy_id,
        "identity_components": list(candidate.identity_components)
        or ["paper", candidate.instrument_family.lower(), candidate.promoted_strategy_id.lower()],
        "long_sources": long_sources,
        "short_sources": short_sources,
        "session_restriction": "/".join(candidate.session_eligibility),
        "allowed_sessions": list(candidate.session_eligibility),
        "point_value": candidate.point_value,
        "trade_size": 1,
        "participation_policy": candidate.participation_policy,
        "max_concurrent_entries": candidate.max_concurrent_entries,
        "max_position_quantity": candidate.max_position_quantity,
        "max_adds_after_entry": candidate.max_adds_after_entry,
        "add_direction_policy": "SAME_DIRECTION_ONLY",
        "catastrophic_open_loss": candidate.catastrophic_open_loss,
        "lane_mode": candidate.lane_mode,
        "strategy_family": candidate.strategy_family or candidate.promoted_strategy_id,
        "strategy_identity_root": candidate.strategy_identity_root or candidate.promoted_strategy_id,
        "runtime_kind": candidate.runtime_kind,
        "structural_signal_timeframe": "5m",
        "execution_timeframe": "1m",
        "artifact_timeframe": "5m",
        "context_timeframes": ["5m"],
        "live_poll_lookback_minutes": 1440,
        "observed_instruments": list(candidate.observed_instruments) or [candidate.instrument_family],
        "experimental_status": candidate.experimental_status,
        "paper_only": True,
        "non_approved": False,
        "managed_exit_policy_id": candidate.lifecycle_policy_id,
        "artifacts_dir": f"./outputs/probationary_pattern_engine/paper_session/lanes/{candidate.lane_id}",
        "database_url": f"sqlite:///./mgc_v05l.probationary.paper__{candidate.lane_id}.sqlite3",
        "runtime_overlay_params": runtime_overlay_params,
        "submit_authority": "PAPER_ONLY_GUARDED_RUNTIME_AFTER_PROMOTION_CONTRACT",
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "submit_capable_without_promotion_contract": False,
        "broad_cancel_flatten_allowed": False,
        "unguarded_broker_mutation_allowed": False,
    }
    if candidate.quality_bucket_policy:
        row["quality_bucket_policy"] = candidate.quality_bucket_policy
    if candidate.observer_variant_id:
        row["observer_variant_id"] = candidate.observer_variant_id
    if candidate.observer_side:
        row["observer_side"] = candidate.observer_side
    if candidate.allow_pre_5m_context_participation is not None:
        row["allow_pre_5m_context_participation"] = candidate.allow_pre_5m_context_participation
    if candidate.atp_context_timeframe:
        row["atp_context_timeframe"] = candidate.atp_context_timeframe
    if candidate.candidate_id:
        row["candidate_id"] = candidate.candidate_id
    if candidate.candidate_origin:
        row["candidate_origin"] = candidate.candidate_origin
    return row
