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

from mgc_v05l.execution_core.track_b_exit_strategy_roster import MGC_DIAGNOSTIC_TIMEBOX_3X5M_V1
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
TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND = "track_b_rule_runner_paper_strategy_engine"
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
    )
}


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
    return {
        "lane_id": candidate.lane_id,
        "display_name": "MGC / Asian Drift late-join missing-anchor long / PAPER experiment",
        "symbol": candidate.instrument_family,
        "standalone_strategy_id": candidate.promoted_strategy_id,
        "identity_components": ["paper", "mgc", "asian_drift", "late_join_missing_anchor"],
        "long_sources": [source] if candidate.side == "LONG" else [],
        "short_sources": [source] if candidate.side == "SHORT" else [],
        "session_restriction": "/".join(candidate.session_eligibility),
        "allowed_sessions": list(candidate.session_eligibility),
        "point_value": "10",
        "trade_size": 1,
        "participation_policy": "SINGLE_ENTRY_ONLY",
        "max_concurrent_entries": 1,
        "max_position_quantity": 1,
        "max_adds_after_entry": 0,
        "add_direction_policy": "SAME_DIRECTION_ONLY",
        "catastrophic_open_loss": "-500",
        "lane_mode": "TRACK_B_SHADOW_PROMOTED_PAPER_CANDIDATE",
        "strategy_family": candidate.promoted_strategy_id,
        "strategy_identity_root": candidate.promoted_strategy_id,
        "runtime_kind": TRACK_B_RULE_RUNNER_PAPER_RUNTIME_KIND,
        "structural_signal_timeframe": "5m",
        "execution_timeframe": "1m",
        "artifact_timeframe": "5m",
        "context_timeframes": ["5m"],
        "live_poll_lookback_minutes": 1440,
        "observed_instruments": [candidate.instrument_family],
        "experimental_status": "paper_candidate_missing_anchor_experiment",
        "paper_only": True,
        "non_approved": False,
        "managed_exit_policy_id": candidate.lifecycle_policy_id,
        "artifacts_dir": f"./outputs/probationary_pattern_engine/paper_session/lanes/{candidate.lane_id}",
        "database_url": f"sqlite:///./mgc_v05l.probationary.paper__{candidate.lane_id}.sqlite3",
        "runtime_overlay_params": {
            "rule_id": candidate.promoted_strategy_id,
            "rule_mode": candidate.promoted_strategy_id,
            "strategy_id": candidate.promoted_strategy_id,
            "lane_id": candidate.lane_id,
            "entry_source": candidate.promoted_strategy_id,
            "input_event_path": "outputs/track_b_execution_core/asian_drift_state/latest_asian_drift_5m_state_snapshot.json",
            "output_root": f"outputs/track_b_execution_core/track_b_strategy_rule_runner/{candidate.lane_id}",
            "expected_account_id": "DUM882026",
            "require_timestamp_coherence": True,
            "timestamp_tolerance_seconds": DEFAULT_TIMESTAMP_COHERENCE_SECONDS,
            "experimental_reason": candidate.experimental_reason,
        },
        "submit_authority": "PAPER_ONLY_GUARDED_RUNTIME_AFTER_PROMOTION_CONTRACT",
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "submit_capable_without_promotion_contract": False,
        "broad_cancel_flatten_allowed": False,
        "unguarded_broker_mutation_allowed": False,
    }
