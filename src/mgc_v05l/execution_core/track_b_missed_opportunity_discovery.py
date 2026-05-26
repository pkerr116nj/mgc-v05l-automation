"""Track B missed-opportunity discovery shadows.

This module migrates ATP/trend participation configs into Track B research
shadow diagnostics and turns strict-strategy near misses from any Track B
strategy family into scored B-grade shadow candidates. The near-miss scoring
model is intentionally general and is not coupled to ATP lanes. It is
non-authoritative: no strategy rule, broker, lifecycle, order, or runtime
authority is changed here.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime
from .track_b_atomic_io import write_json_atomic
from .track_b_atp_paper_readiness import build_atp_lifecycle_mapping_shadow
from .track_b_phase1_runtime_candle_adapter import normalize_phase1_runtime_candle_payload


DEFAULT_RESEARCH_SHADOW_ROOT = Path("outputs") / "track_b_execution_core" / "research_shadow"
DEFAULT_DIAGNOSTICS_ROOT = Path("outputs") / "track_b_execution_core" / "diagnostics"
DEFAULT_ATP_LATEST = DEFAULT_RESEARCH_SHADOW_ROOT / "latest_atp_trend_participation_shadow.json"
DEFAULT_ATP_EVENTS = DEFAULT_RESEARCH_SHADOW_ROOT / "atp_trend_participation_shadow_events.jsonl"
DEFAULT_ATP_LIFECYCLE_MAPPING_LATEST = DEFAULT_RESEARCH_SHADOW_ROOT / "latest_atp_lifecycle_mapping_shadow.json"
DEFAULT_ATP_LIFECYCLE_MAPPING_EVENTS = DEFAULT_RESEARCH_SHADOW_ROOT / "atp_lifecycle_mapping_shadow_events.jsonl"
DEFAULT_NEAR_MISS_LATEST = DEFAULT_RESEARCH_SHADOW_ROOT / "latest_near_miss_scored_shadow.json"
DEFAULT_NEAR_MISS_EVENTS = DEFAULT_RESEARCH_SHADOW_ROOT / "near_miss_scored_shadow_events.jsonl"
DEFAULT_FORWARD_OUTCOMES_LATEST = DEFAULT_RESEARCH_SHADOW_ROOT / "latest_missed_opportunity_forward_outcomes.json"
DEFAULT_FORWARD_OUTCOMES_EVENTS = DEFAULT_RESEARCH_SHADOW_ROOT / "missed_opportunity_forward_outcomes.jsonl"
DEFAULT_TIMESTAMP_LOCKED_EVIDENCE_LATEST = (
    DEFAULT_RESEARCH_SHADOW_ROOT / "latest_timestamp_locked_forward_evidence.json"
)
DEFAULT_TIMESTAMP_LOCKED_EVIDENCE_EVENTS = DEFAULT_RESEARCH_SHADOW_ROOT / "timestamp_locked_forward_evidence.jsonl"
DEFAULT_SHADOW_CANDIDATE_MATURATION_LATEST = (
    DEFAULT_RESEARCH_SHADOW_ROOT / "latest_shadow_candidate_maturation.json"
)
DEFAULT_SHADOW_CANDIDATE_MATURATION_EVENTS = DEFAULT_RESEARCH_SHADOW_ROOT / "shadow_candidate_maturation_events.jsonl"
DEFAULT_LATE_JOIN_MISSING_ANCHOR_LATEST = (
    DEFAULT_RESEARCH_SHADOW_ROOT / "latest_asian_drift_late_join_missing_anchor_long_shadow.json"
)
DEFAULT_LATE_JOIN_MISSING_ANCHOR_EVENTS = (
    DEFAULT_RESEARCH_SHADOW_ROOT / "asian_drift_late_join_missing_anchor_long_shadow_events.jsonl"
)
DEFAULT_REPORT_JSON = DEFAULT_DIAGNOSTICS_ROOT / "latest_missed_opportunity_discovery_layer.json"
DEFAULT_REPORT_MD = Path("docs") / "track_b_missed_opportunity_discovery_layer.md"
DEFAULT_UNDERPERFORMANCE_REVIEW = DEFAULT_DIAGNOSTICS_ROOT / "latest_strategy_underperformance_review.json"
DEFAULT_OVERFILTERING_AUDIT = DEFAULT_DIAGNOSTICS_ROOT / "latest_overfiltering_missing_regime_audit.json"
DEFAULT_ROSTER_CONFIG = Path("config") / "track_b_guarded_paper_roster.json"
DEFAULT_CONTROL_PLANE = (
    Path("outputs") / "track_b_execution_core" / "control_plane" / "latest_control_plane_snapshot.json"
)
DEFAULT_SAFE_STATE = Path("outputs") / "track_b_execution_core" / "safe_state" / "latest_runtime_safe_state_envelope.json"
DEFAULT_RUNTIME_AUTHORITY = (
    Path("outputs") / "track_b_execution_core" / "runtime_authority" / "latest_runtime_authority.json"
)
DEFAULT_PHASE1_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_HISTORICAL_REPLAY_ROOT = (
    Path("outputs")
    / "reports"
    / "entry_acceptance_research"
    / "full_history_batch"
    / "warehouse_historical_evaluator_partitions"
)

ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT = "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT"
ATP_SHADOW_CONFIRMS_LIVE_SIGNAL = "ATP_SHADOW_CONFIRMS_LIVE_SIGNAL"
ATP_SHADOW_NO_CANDIDATE = "ATP_SHADOW_NO_CANDIDATE"
ATP_SHADOW_LOW_CONFIDENCE = "ATP_SHADOW_LOW_CONFIDENCE"

NEAR_MISS_SCORED_SHADOW_READY = "NEAR_MISS_SCORED_SHADOW_READY"
NEAR_MISS_SCORED_SHADOW_EMPTY = "NEAR_MISS_SCORED_SHADOW_EMPTY"
MISSED_OPPORTUNITY_FORWARD_OUTCOMES_READY = "MISSED_OPPORTUNITY_FORWARD_OUTCOMES_READY"
MISSED_OPPORTUNITY_FORWARD_OUTCOMES_EMPTY = "MISSED_OPPORTUNITY_FORWARD_OUTCOMES_EMPTY"
TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY = "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY"
TIMESTAMP_LOCKED_FORWARD_EVIDENCE_EMPTY = "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_EMPTY"
SHADOW_CANDIDATE_MATURATION_READY = "SHADOW_CANDIDATE_MATURATION_READY"
SHADOW_CANDIDATE_MATURATION_EMPTY = "SHADOW_CANDIDATE_MATURATION_EMPTY"
MISSED_OPPORTUNITY_DISCOVERY_READY = "MISSED_OPPORTUNITY_DISCOVERY_READY"
ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1 = (
    "ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1"
)
ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_READY = (
    "ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_READY"
)
ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_EMPTY = (
    "ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_EMPTY"
)

CRITICAL_GATE_TOKENS = (
    "broker",
    "control_plane",
    "safe_state",
    "guardian",
    "live_money",
    "paper_proof",
    "snapshot",
    "runtime_generation",
    "duplicate",
    "authorization",
    "reconciliation",
)


@dataclass(frozen=True)
class MissedOpportunityDiscoveryConfig:
    repo_root: Path = Path(".")
    underperformance_review_json: Path = DEFAULT_UNDERPERFORMANCE_REVIEW
    overfiltering_audit_json: Path = DEFAULT_OVERFILTERING_AUDIT
    roster_config_json: Path = DEFAULT_ROSTER_CONFIG
    control_plane_json: Path = DEFAULT_CONTROL_PLANE
    safe_state_json: Path = DEFAULT_SAFE_STATE
    runtime_authority_json: Path = DEFAULT_RUNTIME_AUTHORITY
    phase1_root: Path = DEFAULT_PHASE1_ROOT
    historical_replay_root: Path = DEFAULT_HISTORICAL_REPLAY_ROOT
    atp_latest_json: Path = DEFAULT_ATP_LATEST
    atp_events_jsonl: Path = DEFAULT_ATP_EVENTS
    atp_lifecycle_mapping_latest_json: Path = DEFAULT_ATP_LIFECYCLE_MAPPING_LATEST
    atp_lifecycle_mapping_events_jsonl: Path = DEFAULT_ATP_LIFECYCLE_MAPPING_EVENTS
    near_miss_latest_json: Path = DEFAULT_NEAR_MISS_LATEST
    near_miss_events_jsonl: Path = DEFAULT_NEAR_MISS_EVENTS
    forward_outcomes_latest_json: Path = DEFAULT_FORWARD_OUTCOMES_LATEST
    forward_outcomes_events_jsonl: Path = DEFAULT_FORWARD_OUTCOMES_EVENTS
    timestamp_locked_evidence_latest_json: Path = DEFAULT_TIMESTAMP_LOCKED_EVIDENCE_LATEST
    timestamp_locked_evidence_events_jsonl: Path = DEFAULT_TIMESTAMP_LOCKED_EVIDENCE_EVENTS
    shadow_candidate_maturation_latest_json: Path = DEFAULT_SHADOW_CANDIDATE_MATURATION_LATEST
    shadow_candidate_maturation_events_jsonl: Path = DEFAULT_SHADOW_CANDIDATE_MATURATION_EVENTS
    late_join_missing_anchor_latest_json: Path = DEFAULT_LATE_JOIN_MISSING_ANCHOR_LATEST
    late_join_missing_anchor_events_jsonl: Path = DEFAULT_LATE_JOIN_MISSING_ANCHOR_EVENTS
    report_json: Path = DEFAULT_REPORT_JSON
    report_md: Path = DEFAULT_REPORT_MD
    min_atp_score: float = 0.62
    min_near_miss_b_score: float = 0.75


def create_missed_opportunity_discovery_layer(
    *,
    config: MissedOpportunityDiscoveryConfig | None = None,
    now: datetime | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    actual_config = config or MissedOpportunityDiscoveryConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)

    review_path = _resolve(repo_root, actual_config.underperformance_review_json)
    overfiltering_path = _resolve(repo_root, actual_config.overfiltering_audit_json)
    roster_path = _resolve(repo_root, actual_config.roster_config_json)
    control_plane_path = _resolve(repo_root, actual_config.control_plane_json)
    safe_state_path = _resolve(repo_root, actual_config.safe_state_json)
    runtime_authority_path = _resolve(repo_root, actual_config.runtime_authority_json)
    maturation_latest_path = _resolve(repo_root, actual_config.shadow_candidate_maturation_latest_json)

    review = _load_json(review_path)
    overfiltering = _load_json(overfiltering_path)
    roster = _load_json(roster_path)
    control_plane = _load_json(control_plane_path)
    safe_state = _load_json(safe_state_path)
    runtime_authority = _load_json(runtime_authority_path)
    prior_maturation = _load_json(maturation_latest_path)
    active_roster = _active_roster_ids(roster, review.get("strategy_inventory"))

    shared_context = _shared_context(
        control_plane=control_plane,
        safe_state=safe_state,
        runtime_authority=runtime_authority,
        source_paths={
            "control_plane": control_plane_path,
            "safe_state": safe_state_path,
            "runtime_authority": runtime_authority_path,
        },
    )
    atp_shadow = build_atp_trend_participation_shadow(
        repo_root=repo_root,
        phase1_root=_resolve(repo_root, actual_config.phase1_root),
        active_roster=active_roster,
        review=review,
        shared_context=shared_context,
        now=actual_now,
        min_score=actual_config.min_atp_score,
    )
    atp_lifecycle_mapping_shadow = build_atp_lifecycle_mapping_shadow(atp_shadow=atp_shadow, now=actual_now)
    near_miss_shadow = build_near_miss_scored_shadow(
        review=review,
        overfiltering=overfiltering,
        now=actual_now,
        min_b_score=actual_config.min_near_miss_b_score,
    )
    shadow_candidate_maturation = build_shadow_candidate_maturation(
        atp_shadow=atp_shadow,
        near_miss_shadow=near_miss_shadow,
        phase1_root=_resolve(repo_root, actual_config.phase1_root),
        historical_replay_root=_resolve(repo_root, actual_config.historical_replay_root),
        now=actual_now,
        prior_maturation=prior_maturation,
    )
    timestamp_locked_evidence = _timestamp_locked_evidence_from_maturation(
        shadow_candidate_maturation,
        now=actual_now,
    )
    forward_outcomes = build_missed_opportunity_forward_outcomes(
        atp_shadow=atp_shadow,
        near_miss_shadow=near_miss_shadow,
        phase1_root=_resolve(repo_root, actual_config.phase1_root),
        now=actual_now,
        timestamp_locked_evidence=timestamp_locked_evidence,
    )
    late_join_missing_anchor_shadow = build_asian_drift_late_join_missing_anchor_shadow_family(
        near_miss_shadow=near_miss_shadow,
        timestamp_locked_evidence=timestamp_locked_evidence,
        forward_outcomes=forward_outcomes,
        now=actual_now,
    )
    report = _build_combined_report(
        atp_shadow=atp_shadow,
        atp_lifecycle_mapping_shadow=atp_lifecycle_mapping_shadow,
        near_miss_shadow=near_miss_shadow,
        shadow_candidate_maturation=shadow_candidate_maturation,
        timestamp_locked_evidence=timestamp_locked_evidence,
        forward_outcomes=forward_outcomes,
        late_join_missing_anchor_shadow=late_join_missing_anchor_shadow,
        review=review,
        overfiltering=overfiltering,
        now=actual_now,
        source_paths={
            "underperformance_review": review_path,
            "overfiltering_audit": overfiltering_path,
            "roster_config": roster_path,
            "atp_lifecycle_mapping_shadow": _resolve(repo_root, actual_config.atp_lifecycle_mapping_latest_json),
            "shadow_candidate_maturation": maturation_latest_path,
        },
    )

    atp_latest = _resolve(repo_root, actual_config.atp_latest_json)
    atp_events = _resolve(repo_root, actual_config.atp_events_jsonl)
    atp_lifecycle_mapping_latest = _resolve(repo_root, actual_config.atp_lifecycle_mapping_latest_json)
    atp_lifecycle_mapping_events = _resolve(repo_root, actual_config.atp_lifecycle_mapping_events_jsonl)
    near_latest = _resolve(repo_root, actual_config.near_miss_latest_json)
    near_events = _resolve(repo_root, actual_config.near_miss_events_jsonl)
    forward_latest = _resolve(repo_root, actual_config.forward_outcomes_latest_json)
    forward_events = _resolve(repo_root, actual_config.forward_outcomes_events_jsonl)
    timestamp_locked_latest = _resolve(repo_root, actual_config.timestamp_locked_evidence_latest_json)
    timestamp_locked_events = _resolve(repo_root, actual_config.timestamp_locked_evidence_events_jsonl)
    maturation_latest = _resolve(repo_root, actual_config.shadow_candidate_maturation_latest_json)
    maturation_events = _resolve(repo_root, actual_config.shadow_candidate_maturation_events_jsonl)
    late_join_missing_anchor_latest = _resolve(repo_root, actual_config.late_join_missing_anchor_latest_json)
    late_join_missing_anchor_events = _resolve(repo_root, actual_config.late_join_missing_anchor_events_jsonl)
    report_json = _resolve(repo_root, actual_config.report_json)
    report_md = _resolve(repo_root, actual_config.report_md)

    write_json_atomic(atp_latest, atp_shadow)
    _append_jsonl(atp_events, _event_projection(atp_shadow))
    write_json_atomic(atp_lifecycle_mapping_latest, atp_lifecycle_mapping_shadow)
    _append_jsonl(atp_lifecycle_mapping_events, _event_projection(atp_lifecycle_mapping_shadow))
    write_json_atomic(near_latest, near_miss_shadow)
    _append_jsonl(near_events, _event_projection(near_miss_shadow))
    write_json_atomic(maturation_latest, shadow_candidate_maturation)
    _append_jsonl(maturation_events, _event_projection(shadow_candidate_maturation))
    write_json_atomic(timestamp_locked_latest, timestamp_locked_evidence)
    _append_jsonl(timestamp_locked_events, _event_projection(timestamp_locked_evidence))
    write_json_atomic(forward_latest, forward_outcomes)
    _append_jsonl(forward_events, _event_projection(forward_outcomes))
    write_json_atomic(late_join_missing_anchor_latest, late_join_missing_anchor_shadow)
    _append_jsonl(late_join_missing_anchor_events, _event_projection(late_join_missing_anchor_shadow))
    write_json_atomic(report_json, report)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.write_text(_render_markdown(report), encoding="utf-8")
    return report_json, report_md, report


def build_atp_trend_participation_shadow(
    *,
    repo_root: Path,
    phase1_root: Path,
    active_roster: set[str],
    review: Mapping[str, Any],
    shared_context: Mapping[str, Any],
    now: datetime,
    min_score: float = 0.62,
) -> dict[str, Any]:
    candidates = _discover_atp_candidates(repo_root)
    live_signal_index = _live_signal_index(review)
    rows = []
    class_counts: Counter[str] = Counter()
    for candidate in candidates:
        symbol = str(candidate.get("instrument") or "").upper()
        candles, source_path = _load_phase1_candles(phase1_root, symbol)
        evaluation = _evaluate_atp_candidate(candidate, candles=candles, min_score=min_score)
        live_signals = int(live_signal_index.get(symbol, 0))
        if evaluation["candidate_ready"] and live_signals <= 0:
            classification = ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT
        elif evaluation["candidate_ready"]:
            classification = ATP_SHADOW_CONFIRMS_LIVE_SIGNAL
        elif evaluation["score"] >= max(min_score * 0.75, 0.4):
            classification = ATP_SHADOW_LOW_CONFIDENCE
        else:
            classification = ATP_SHADOW_NO_CANDIDATE
        class_counts[classification] += 1
        rows.append(
            {
                **candidate,
                "shadow_classification": classification,
                "direction": evaluation["direction"],
                "score": evaluation["score"],
                "confidence": evaluation["confidence"],
                "failed_gates": evaluation["failed_gates"],
                "session_regime": candidate.get("session_regime"),
                "source_authority_path": str(source_path) if source_path else None,
                "source_candle_timestamp": evaluation["latest_candle_timestamp"],
                "live_strategy_signal_count_for_instrument": live_signals,
                **_non_authority_flags(),
            }
        )
    return {
        "schema_version": "track_b_atp_trend_participation_shadow_v1",
        "generated_at": now.isoformat(),
        "classification": "ATP_TREND_PARTICIPATION_SHADOW_READY" if rows else "ATP_TREND_PARTICIPATION_SHADOW_EMPTY",
        "candidate_count": len(rows),
        "shadow_candidate_count": class_counts.get(ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT, 0)
        + class_counts.get(ATP_SHADOW_CONFIRMS_LIVE_SIGNAL, 0),
        "classification_counts": dict(class_counts),
        "shared_services_context": dict(shared_context),
        "candidates": rows,
        **_non_authority_flags(),
    }


def build_near_miss_scored_shadow(
    *,
    review: Mapping[str, Any],
    overfiltering: Mapping[str, Any],
    now: datetime,
    min_b_score: float = 0.75,
) -> dict[str, Any]:
    rejection = review.get("rejection_attribution") if isinstance(review.get("rejection_attribution"), Mapping) else {}
    forward = review.get("forward_outcome_simulation") if isinstance(review.get("forward_outcome_simulation"), Mapping) else {}
    outcome_by_strategy: dict[str, Counter[str]] = defaultdict(Counter)
    for row in _as_list(forward.get("simulations")):
        if isinstance(row, Mapping):
            outcome_by_strategy[str(row.get("strategy_id") or "UNKNOWN")][
                str(row.get("outcome_classification") or "UNCLEAR")
            ] += 1

    rows = []
    for item in _as_list(rejection.get("near_miss_examples")):
        if not isinstance(item, Mapping):
            continue
        rows.append(_score_near_miss(item, outcome_by_strategy, min_b_score=min_b_score))
    for strategy in _as_list(overfiltering.get("overfiltering_audit", {}).get("per_strategy") if isinstance(overfiltering.get("overfiltering_audit"), Mapping) else []):
        if not isinstance(strategy, Mapping) or _int(strategy.get("near_miss_count"), 0) <= 0:
            continue
        if any(row.get("strategy_id") == strategy.get("strategy_id") for row in rows):
            continue
        synthetic = {
            "strategy_id": strategy.get("strategy_id"),
            "failed_predicates": [
                row.get("predicate")
                for row in _as_list(strategy.get("ranked_failed_predicates"))[:2]
                if isinstance(row, Mapping)
            ],
            "gate_distance": 1 if _int(strategy.get("one_gate_away_count"), 0) else 2,
            "source": "overfiltering_strategy_rollup",
        }
        rows.append(_score_near_miss(synthetic, outcome_by_strategy, min_b_score=min_b_score))

    grade_counts = Counter(str(row.get("approval_grade")) for row in rows)
    strategy_summary = _near_miss_strategy_summary(rows)
    return {
        "schema_version": "track_b_near_miss_scored_shadow_v1",
        "scoring_scope": "ALL_TRACK_B_STRATEGY_FAMILIES",
        "scoring_model_note": "General near-miss scorer over strict strategy rejects; not ATP-specific.",
        "generated_at": now.isoformat(),
        "classification": NEAR_MISS_SCORED_SHADOW_READY if rows else NEAR_MISS_SCORED_SHADOW_EMPTY,
        "candidate_count": len(rows),
        "b_grade_shadow_candidate_count": grade_counts.get("B", 0),
        "grade_counts": dict(grade_counts),
        "per_strategy_summary": strategy_summary,
        "scored_candidates": rows[-200:],
        **_non_authority_flags(),
    }


def build_missed_opportunity_forward_outcomes(
    *,
    atp_shadow: Mapping[str, Any],
    near_miss_shadow: Mapping[str, Any],
    phase1_root: Path,
    now: datetime,
    timestamp_locked_evidence: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if timestamp_locked_evidence is None:
        timestamp_locked_evidence = build_timestamp_locked_forward_evidence(
            atp_shadow=atp_shadow,
            near_miss_shadow=near_miss_shadow,
            phase1_root=phase1_root,
            now=now,
        )
    outcomes = []
    for evidence in _as_list(timestamp_locked_evidence.get("evidence")):
        if not isinstance(evidence, Mapping):
            continue
        candidate = evidence.get("candidate") if isinstance(evidence.get("candidate"), Mapping) else evidence
        outcome = _outcome_from_evidence(evidence)
        outcomes.append(
            {
                **candidate,
                **outcome,
                "timestamp_lock_classification": evidence.get("timestamp_lock_classification"),
                "maturation_status": evidence.get("maturation_status"),
                "outcome_status": evidence.get("outcome_status"),
                "candle_window_start": evidence.get("candle_window_start"),
                "candle_window_end": evidence.get("candle_window_end"),
                "source_authority_path": evidence.get("source_authority_path") or candidate.get("source_authority_path"),
                **_non_authority_flags(),
            }
        )
    classification_counts = Counter(str(row.get("outcome_classification") or "UNCLEAR") for row in outcomes)
    return {
        "schema_version": "track_b_missed_opportunity_forward_outcomes_v1",
        "generated_at": now.isoformat(),
        "classification": MISSED_OPPORTUNITY_FORWARD_OUTCOMES_READY
        if outcomes
        else MISSED_OPPORTUNITY_FORWARD_OUTCOMES_EMPTY,
        "candidate_count": len(outcomes),
        "classification_counts": dict(classification_counts),
        "windows": ["5m", "15m", "30m", "60m"],
        "outcomes": outcomes,
        **_non_authority_flags(),
    }


def build_timestamp_locked_forward_evidence(
    *,
    atp_shadow: Mapping[str, Any],
    near_miss_shadow: Mapping[str, Any],
    phase1_root: Path,
    historical_replay_root: Path | None = None,
    now: datetime,
) -> dict[str, Any]:
    candidates = _forward_candidate_rows(atp_shadow=atp_shadow, near_miss_shadow=near_miss_shadow)
    evidence_rows = []
    for candidate in candidates:
        instrument = str(candidate.get("instrument") or _symbol_for_strategy(str(candidate.get("strategy_id") or "")))
        candles, source_path = _load_phase1_candles(phase1_root, instrument)
        evidence_rows.append(
            build_timestamp_locked_candidate_evidence(
                candidate=candidate,
                candles=candles,
                source_path=source_path,
                replay_candles_loader=(
                    (lambda symbol, timestamp: _load_replay_candles_for_candidate(
                        historical_replay_root,
                        symbol,
                        timestamp,
                    ))
                    if historical_replay_root is not None
                    else None
                ),
            )
        )
    classification_counts = Counter(str(row.get("timestamp_lock_classification") or "UNKNOWN") for row in evidence_rows)
    return {
        "schema_version": "track_b_timestamp_locked_forward_evidence_v1",
        "generated_at": now.isoformat(),
        "classification": TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY
        if evidence_rows
        else TIMESTAMP_LOCKED_FORWARD_EVIDENCE_EMPTY,
        "candidate_count": len(evidence_rows),
        "classification_counts": dict(classification_counts),
        "windows": ["5m", "15m", "30m", "60m"],
        "evidence": evidence_rows,
        **_non_authority_flags(),
    }


def build_shadow_candidate_maturation(
    *,
    atp_shadow: Mapping[str, Any],
    near_miss_shadow: Mapping[str, Any],
    phase1_root: Path,
    historical_replay_root: Path | None = None,
    now: datetime,
    prior_maturation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    current_candidates = _forward_candidate_rows(atp_shadow=atp_shadow, near_miss_shadow=near_miss_shadow)
    merged = _merge_shadow_maturation_candidates(prior_maturation or {}, current_candidates, now=now)
    records = []
    for record in merged:
        candidate = record.get("candidate") if isinstance(record.get("candidate"), Mapping) else record
        instrument = str(candidate.get("instrument") or _symbol_for_strategy(str(candidate.get("strategy_id") or "")))
        candles, source_path = _load_phase1_candles(phase1_root, instrument)
        evidence = build_timestamp_locked_candidate_evidence(
            candidate=candidate,
            candles=candles,
            source_path=source_path,
            replay_candles_loader=(
                (lambda symbol, timestamp: _load_replay_candles_for_candidate(
                    historical_replay_root,
                    symbol,
                    timestamp,
                ))
                if historical_replay_root is not None
                else None
            ),
        )
        outcome = _outcome_from_evidence(evidence)
        status = _maturation_status(evidence)
        records.append(
            {
                **record,
                "candidate": dict(candidate),
                "candidate_key": _shadow_candidate_key(candidate),
                "last_evaluated_at": now.isoformat(),
                "required_forward_windows": ["5m", "15m", "30m", "60m"],
                "matured_forward_windows": [
                    key
                    for key, value in (evidence.get("forward_window_availability") or {}).items()
                    if bool(value)
                ]
                if isinstance(evidence.get("forward_window_availability"), Mapping)
                else [],
                "maturation_status": status,
                "outcome_status": "FINAL" if status == "FINAL" else status,
                "outcome_classification": outcome.get("outcome_classification"),
                "outcome_reason": outcome.get("outcome_reason"),
                "timestamp_locked_evidence": evidence,
                "forward_outcome": outcome,
                **_non_authority_flags(),
            }
        )
    status_counts = Counter(str(row.get("maturation_status") or "UNKNOWN") for row in records)
    return {
        "schema_version": "track_b_shadow_candidate_maturation_v1",
        "generated_at": now.isoformat(),
        "classification": SHADOW_CANDIDATE_MATURATION_READY if records else SHADOW_CANDIDATE_MATURATION_EMPTY,
        "candidate_count": len(records),
        "new_candidate_count": len(current_candidates),
        "pending_count": status_counts.get("PENDING", 0),
        "partially_matured_count": status_counts.get("PARTIALLY_MATURED", 0),
        "final_count": status_counts.get("FINAL", 0),
        "status_counts": dict(status_counts),
        "records": records,
        **_non_authority_flags(),
    }


def _timestamp_locked_evidence_from_maturation(
    maturation: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    evidence_rows = []
    for record in _as_list(maturation.get("records")):
        if not isinstance(record, Mapping):
            continue
        evidence = record.get("timestamp_locked_evidence")
        if not isinstance(evidence, Mapping):
            continue
        evidence_rows.append(
            {
                **evidence,
                "maturation_status": record.get("maturation_status"),
                "outcome_status": record.get("outcome_status"),
            }
        )
    classification_counts = Counter(str(row.get("timestamp_lock_classification") or "UNKNOWN") for row in evidence_rows)
    maturity_counts = Counter(str(row.get("maturation_status") or "UNKNOWN") for row in evidence_rows)
    return {
        "schema_version": "track_b_timestamp_locked_forward_evidence_v1",
        "generated_at": now.isoformat(),
        "classification": TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY
        if evidence_rows
        else TIMESTAMP_LOCKED_FORWARD_EVIDENCE_EMPTY,
        "candidate_count": len(evidence_rows),
        "classification_counts": dict(classification_counts),
        "maturation_status_counts": dict(maturity_counts),
        "windows": ["5m", "15m", "30m", "60m"],
        "evidence": evidence_rows,
        **_non_authority_flags(),
    }


def _merge_shadow_maturation_candidates(
    prior_maturation: Mapping[str, Any],
    current_candidates: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for record in _as_list(prior_maturation.get("records")):
        if not isinstance(record, Mapping):
            continue
        candidate = record.get("candidate") if isinstance(record.get("candidate"), Mapping) else record
        key = str(record.get("candidate_key") or _shadow_candidate_key(candidate))
        if not key:
            continue
        by_key[key] = dict(record)
    for candidate in current_candidates:
        key = _shadow_candidate_key(candidate)
        if not key:
            continue
        existing = by_key.get(key, {})
        first_seen = existing.get("first_seen_at") or now.isoformat()
        seen_count = _int(existing.get("seen_count"), 0) + 1
        by_key[key] = {
            **existing,
            "candidate_key": key,
            "candidate": dict(candidate),
            "first_seen_at": first_seen,
            "last_seen_at": now.isoformat(),
            "seen_count": seen_count,
            "source_family": candidate.get("source") or candidate.get("strategy_family"),
            "candidate_id": candidate.get("candidate_id"),
            "strategy_id": candidate.get("strategy_id"),
            "instrument": candidate.get("instrument"),
            "direction": candidate.get("direction"),
            "candidate_timestamp": candidate.get("timestamp"),
            "reference_price": candidate.get("reference_price"),
            "score": candidate.get("score"),
            "confidence": candidate.get("confidence"),
            "regime_session": candidate.get("regime_session"),
        }
    return sorted(by_key.values(), key=lambda row: str(row.get("candidate_key") or ""))


def _shadow_candidate_key(candidate: Mapping[str, Any]) -> str:
    parts = [
        str(candidate.get("source") or ""),
        str(candidate.get("candidate_id") or ""),
        str(candidate.get("strategy_id") or ""),
        str(candidate.get("instrument") or ""),
        str(candidate.get("direction") or ""),
        str(candidate.get("timestamp") or candidate.get("candidate_timestamp") or ""),
    ]
    return "|".join(parts)


def _outcome_from_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    candidate = evidence.get("candidate") if isinstance(evidence.get("candidate"), Mapping) else evidence
    candles = _as_list(evidence.get("timestamp_locked_candle_window"))
    if evidence.get("timestamp_lock_classification") != "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY":
        return {
            "outcome_classification": "UNCLEAR",
            "outcome_reason": evidence.get("timestamp_lock_classification")
            or "timestamp_locked_forward_evidence_unavailable",
            "reference_price": candidate.get("reference_price"),
            "forward_windows": {},
        }
    return compute_shadow_forward_outcome(candidate=candidate, candles=candles)


def _maturation_status(evidence: Mapping[str, Any]) -> str:
    classification = str(evidence.get("timestamp_lock_classification") or "")
    availability = evidence.get("forward_window_availability")
    if classification == "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY" and isinstance(availability, Mapping):
        required = ["5m", "15m", "30m", "60m"]
        if all(bool(availability.get(window)) for window in required):
            return "FINAL"
        if any(bool(availability.get(window)) for window in required):
            return "PARTIALLY_MATURED"
        return "PENDING"
    if classification in {"UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE", ""}:
        return "PENDING"
    return "FINAL"


def build_timestamp_locked_candidate_evidence(
    *,
    candidate: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    source_path: Path | None = None,
    replay_candles_loader: Any | None = None,
) -> dict[str, Any]:
    ordered = sorted((_normalise_candle(row) for row in candles), key=lambda row: str(row.get("timestamp") or ""))
    ordered = [row for row in ordered if row.get("timestamp") and row.get("close") is not None]
    candidate_timestamp = str(candidate.get("timestamp") or "")
    replay_source_path: Path | None = None
    replay_checked_paths: list[str] = []
    replay_lookup_status = "REPLAY_LOOKUP_NOT_REQUESTED"
    window_start = str(ordered[0].get("timestamp") or "") if ordered else None
    window_end = str(ordered[-1].get("timestamp") or "") if ordered else None
    start_index = _first_candle_index_at_or_after(ordered, candidate_timestamp)
    if start_index is None:
        if replay_candles_loader is not None:
            replay_result = replay_candles_loader(str(candidate.get("instrument") or ""), candidate_timestamp)
            replay_candles = _as_list(replay_result.get("candles")) if isinstance(replay_result, Mapping) else []
            replay_source_path_value = replay_result.get("source_path") if isinstance(replay_result, Mapping) else None
            replay_checked_paths = _as_list(replay_result.get("checked_paths")) if isinstance(replay_result, Mapping) else []
            replay_lookup_status = (
                replay_result.get("lookup_status") if isinstance(replay_result, Mapping) else "REPLAY_LOOKUP_UNAVAILABLE"
            )
            replay_source_path = Path(str(replay_source_path_value)) if replay_source_path_value else None
            replay_ordered = sorted(
                (_normalise_candle(row) for row in replay_candles), key=lambda row: str(row.get("timestamp") or "")
            )
            replay_ordered = [row for row in replay_ordered if row.get("timestamp") and row.get("close") is not None]
            replay_index = _first_candle_index_at_or_after(replay_ordered, candidate_timestamp)
            if replay_index is not None:
                ordered = replay_ordered
                start_index = replay_index
                source_path = replay_source_path or source_path
                window_start = str(ordered[0].get("timestamp") or "") if ordered else None
                window_end = str(ordered[-1].get("timestamp") or "") if ordered else None
            elif replay_ordered:
                ordered = replay_ordered
                source_path = replay_source_path or source_path
                window_start = str(ordered[0].get("timestamp") or "") if ordered else None
                window_end = str(ordered[-1].get("timestamp") or "") if ordered else None
        if start_index is None:
            classification = (
                "UNCLEAR_CANDIDATE_OUTSIDE_REPLAY_WINDOW"
                if ordered
                else "UNCLEAR_NO_REPLAY_OR_PHASE1_CANDLES_AVAILABLE"
            )
            timestamp_locked_window = []
            forward_bars = []
        else:
            classification = "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY"
            timestamp_locked_window = ordered[start_index : start_index + 13]
            forward_bars = ordered[start_index + 1 : start_index + 13]
            if not forward_bars:
                classification = "UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE"
    else:
        replay_lookup_status = "PHASE1_WINDOW_USED"
        classification = "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY"
        timestamp_locked_window = ordered[start_index : start_index + 13]
        forward_bars = ordered[start_index + 1 : start_index + 13]
        if not forward_bars:
            classification = "UNCLEAR_FUTURE_BARS_NOT_YET_AVAILABLE"
    if classification == "TIMESTAMP_LOCKED_FORWARD_EVIDENCE_READY" and not _has_five_minute_continuity(
        timestamp_locked_window
    ):
        classification = "UNCLEAR_REPLAY_CANDLE_CONTINUITY_GAP"
    return {
        "candidate": dict(candidate),
        "candidate_id": candidate.get("candidate_id"),
        "strategy_id": candidate.get("strategy_id"),
        "strategy_family": candidate.get("strategy_family"),
        "instrument": candidate.get("instrument"),
        "direction": candidate.get("direction"),
        "candidate_timestamp": candidate_timestamp,
        "reference_price": candidate.get("reference_price"),
        "timestamp_lock_classification": classification,
        "source_authority_path": str(source_path) if source_path else candidate.get("source_authority_path"),
        "source_category": "PHASE1_RUNTIME_MARKET_DATA_OR_REPLAY",
        "replay_source_used": replay_source_path is not None and source_path == replay_source_path,
        "replay_source_path": str(replay_source_path) if replay_source_path else None,
        "replay_checked_paths": [str(path) for path in replay_checked_paths],
        "replay_lookup_status": replay_lookup_status,
        "candle_window_start": window_start,
        "candle_window_end": window_end,
        "timestamp_locked_candle_window": timestamp_locked_window,
        "forward_candles_available": len(forward_bars),
        "forward_window_availability": {
            "5m": len(forward_bars) >= 1,
            "15m": len(forward_bars) >= 3,
            "30m": len(forward_bars) >= 6,
            "60m": len(forward_bars) >= 12,
        },
        **_non_authority_flags(),
    }


def compute_shadow_forward_outcome(
    *,
    candidate: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    ordered = sorted((_normalise_candle(row) for row in candles), key=lambda row: str(row.get("timestamp") or ""))
    ordered = [row for row in ordered if row.get("timestamp") and row.get("close") is not None]
    if not ordered:
        return {
            "outcome_classification": "UNCLEAR",
            "outcome_reason": "no_forward_candles_available",
            "reference_price": candidate.get("reference_price"),
            "forward_windows": {},
        }
    start_index = _first_candle_index_at_or_after(ordered, str(candidate.get("timestamp") or ""))
    if start_index is None:
        return {
            "outcome_classification": "UNCLEAR",
            "outcome_reason": "candidate_timestamp_outside_candle_window",
            "reference_price": candidate.get("reference_price"),
            "forward_windows": {},
        }
    reference = _float(candidate.get("reference_price"))
    if reference is None:
        reference = _float(ordered[start_index].get("close"))
    if reference is None:
        return {
            "outcome_classification": "UNCLEAR",
            "outcome_reason": "missing_reference_price",
            "reference_price": None,
            "forward_windows": {},
        }
    direction = str(candidate.get("direction") or "LONG").upper()
    long_side = direction not in {"SHORT", "SELL"}
    forward = ordered[start_index + 1 : start_index + 13]
    if len(forward) < 1:
        return {
            "outcome_classification": "UNCLEAR",
            "outcome_reason": "future_bars_not_yet_available",
            "reference_price": reference,
            "forward_windows": {},
        }
    windows = {5: 1, 15: 3, 30: 6, 60: 12}
    forward_windows: dict[str, dict[str, Any]] = {}
    best_mfe = 0.0
    worst_mae = 0.0
    terminal = 0.0
    for minutes, count in windows.items():
        rows = forward[:count]
        if len(rows) < count:
            forward_windows[f"{minutes}m"] = {"complete": False, "bars_observed": len(rows)}
            continue
        highs = [float(row["high"]) for row in rows if row.get("high") is not None]
        lows = [float(row["low"]) for row in rows if row.get("low") is not None]
        closes = [float(row["close"]) for row in rows if row.get("close") is not None]
        if not highs or not lows or not closes:
            forward_windows[f"{minutes}m"] = {"complete": False, "bars_observed": len(rows)}
            continue
        if long_side:
            mfe = max(highs) - reference
            mae = min(lows) - reference
            net = closes[-1] - reference
        else:
            mfe = reference - min(lows)
            mae = reference - max(highs)
            net = reference - closes[-1]
        best_mfe = max(best_mfe, mfe)
        worst_mae = min(worst_mae, mae)
        terminal = net
        forward_windows[f"{minutes}m"] = {
            "complete": True,
            "mfe": round(mfe, 6),
            "mae": round(mae, 6),
            "net_movement": round(net, 6),
            "adverse_excursion": round(abs(min(0.0, mae)), 6),
            "bars_observed": len(rows),
        }
    complete_windows = [row for row in forward_windows.values() if isinstance(row, Mapping) and row.get("complete")]
    profit_harvest_helped = best_mfe > max(abs(worst_mae), 0.5)
    classification = _classify_shadow_forward(
        best_mfe,
        worst_mae,
        terminal,
        complete_count=len(complete_windows),
        profit_harvest_helped=profit_harvest_helped,
    )
    return {
        "outcome_classification": classification,
        "outcome_reason": _shadow_forward_reason(classification, best_mfe, worst_mae, terminal, len(complete_windows)),
        "reference_price": reference,
        "mfe": round(best_mfe, 6),
        "mae": round(worst_mae, 6),
        "net_movement": round(terminal, 6),
        "adverse_excursion": round(abs(min(0.0, worst_mae)), 6),
        "simple_timebox_exit_helped": terminal > 0,
        "profit_harvest_exit_helped": profit_harvest_helped,
        "forward_windows": forward_windows,
    }


def rank_b_grade_missed_winner_families(
    *,
    near_miss_shadow: Mapping[str, Any],
    forward_outcomes: Mapping[str, Any],
    atp_shadow: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Group B-grade missed winners into explainable shadow-family candidates."""

    near_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in _as_list(near_miss_shadow.get("scored_candidates")):
        if not isinstance(row, Mapping):
            continue
        key = (str(row.get("strategy_id") or ""), str(row.get("generated_at") or ""))
        near_by_key[key] = row

    atp_overlap_counts: Counter[tuple[str, str]] = Counter()
    for row in _as_list(atp_shadow.get("candidates")):
        if not isinstance(row, Mapping):
            continue
        if str(row.get("shadow_classification") or "") not in {
            ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT,
            ATP_SHADOW_CONFIRMS_LIVE_SIGNAL,
        }:
            continue
        atp_overlap_counts[(str(row.get("instrument") or ""), str(row.get("direction") or "").upper())] += 1

    grouped: dict[tuple[str, str, str, tuple[str, ...]], list[dict[str, Any]]] = defaultdict(list)
    for outcome in _as_list(forward_outcomes.get("outcomes")):
        if not isinstance(outcome, Mapping):
            continue
        if outcome.get("outcome_classification") != "MISSED_WINNER" or outcome.get("approval_grade") != "B":
            continue
        strategy_id = str(outcome.get("strategy_id") or "")
        timestamp = str(outcome.get("timestamp") or "")
        near = near_by_key.get((strategy_id, timestamp), {})
        failed_predicates = tuple(sorted(str(value) for value in _as_list(near.get("failed_predicates")) if str(value)))
        grouped[
            (
                str(outcome.get("strategy_family") or _strategy_family_for_id(strategy_id)),
                str(outcome.get("instrument") or ""),
                str(outcome.get("direction") or "").upper(),
                failed_predicates,
            )
        ].append(_b_grade_candidate_trait_row(outcome, near))

    ranked = []
    for (family, instrument, direction, failed_predicates), rows in grouped.items():
        count = len(rows)
        avg_mfe = _avg(row.get("mfe") for row in rows)
        avg_mae = _avg(row.get("mae") for row in rows)
        avg_score = _avg(row.get("near_miss_score") for row in rows)
        best_window_counts = Counter(str(row.get("best_forward_window") or "UNKNOWN") for row in rows)
        exit_counts = Counter(str(row.get("harvest_exit_style") or "UNKNOWN") for row in rows)
        overlap_count = atp_overlap_counts[(instrument, direction)]
        family_hypothesis = _family_hypothesis(family, failed_predicates, overlap_count)
        ranked.append(
            {
                "rank": 0,
                "shadow_family_id": _slug(
                    f"{family}_{instrument}_{direction}_{'_'.join(failed_predicates) or 'no_failed_predicates'}"
                ),
                "classification": _ranked_family_classification(count, family_hypothesis),
                "strategy_family": family,
                "instrument": instrument,
                "direction": direction,
                "candidate_count": count,
                "common_failed_predicates": list(failed_predicates),
                "common_passed_traits": _common_passed_traits(rows),
                "average_near_miss_score": round(avg_score, 6),
                "average_mfe": round(avg_mfe, 6),
                "average_mae": round(avg_mae, 6),
                "best_forward_window_counts": dict(best_window_counts),
                "harvest_exit_style_counts": dict(exit_counts),
                "atp_overlap_candidate_count": overlap_count,
                "behavioral_hypothesis": family_hypothesis,
                "recommended_shadow_instrumentation": _shadow_instrumentation_recommendation(family_hypothesis),
                "promotion_allowed": False,
                "live_rule_change_allowed": False,
                "candidate_examples": rows,
                **_non_authority_flags(),
            }
        )
    ranked.sort(
        key=lambda row: (
            {"B_GRADE_PROMISING_SHADOW": 0, "NEEDS_MORE_FORWARD_EVIDENCE": 1, "LIKELY_NOISE": 2}.get(
                str(row.get("classification")), 9
            ),
            -_int(row.get("candidate_count")),
            -float(row.get("average_mfe") or 0.0),
        )
    )
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
    return ranked


def build_asian_drift_late_join_missing_anchor_shadow_family(
    *,
    near_miss_shadow: Mapping[str, Any],
    timestamp_locked_evidence: Mapping[str, Any],
    forward_outcomes: Mapping[str, Any],
    now: datetime,
    min_near_miss_score: float = 0.75,
) -> dict[str, Any]:
    """Persist the replay-backed late-join Asian Drift family as shadow only."""

    require_aware_datetime(now, "now")
    evidence_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in _as_list(timestamp_locked_evidence.get("evidence")):
        if not isinstance(row, Mapping):
            continue
        candidate = row.get("candidate") if isinstance(row.get("candidate"), Mapping) else row
        evidence_by_key[(str(candidate.get("strategy_id") or ""), str(candidate.get("timestamp") or ""))] = row

    outcome_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in _as_list(forward_outcomes.get("outcomes")):
        if not isinstance(row, Mapping):
            continue
        outcome_by_key[(str(row.get("strategy_id") or ""), str(row.get("timestamp") or ""))] = row

    candidates = []
    for row in _as_list(near_miss_shadow.get("scored_candidates")):
        if not isinstance(row, Mapping):
            continue
        strategy_id = str(row.get("strategy_id") or "")
        timestamp = str(row.get("generated_at") or "")
        failed_predicates = [str(value) for value in _as_list(row.get("failed_predicates")) if str(value)]
        direction = str(row.get("hypothetical_direction") or _default_direction(strategy_id)).upper()
        near_miss_score = _float(row.get("near_miss_score")) or 0.0
        if strategy_id != "asian_drift_v1":
            continue
        if _symbol_for_strategy(strategy_id) != "MGC" or direction != "LONG":
            continue
        if "missing_18_00_et_session_anchor_context" not in failed_predicates:
            continue
        if _as_list(row.get("failed_critical_gates")):
            continue
        if near_miss_score < min_near_miss_score:
            continue
        evidence = evidence_by_key.get((strategy_id, timestamp), {})
        outcome = outcome_by_key.get((strategy_id, timestamp), {})
        candidates.append(
            {
                "shadow_strategy_id": ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1,
                "source_strategy_id": strategy_id,
                "candidate_id": f"{strategy_id}:{timestamp}",
                "candidate_timestamp": timestamp,
                "instrument": "MGC",
                "direction": "LONG",
                "entry_grade": row.get("approval_grade"),
                "near_miss_score": row.get("near_miss_score"),
                "hypothetical_score": row.get("hypothetical_score"),
                "failed_predicates": failed_predicates,
                "failed_critical_gates": _as_list(row.get("failed_critical_gates")),
                "failed_noncritical_gates": _as_list(row.get("failed_noncritical_gates")),
                "missing_anchor_allowed_only_in_shadow": True,
                "safety_infrastructure_gates_passed": True,
                "timestamp_lock_classification": evidence.get("timestamp_lock_classification"),
                "source_authority_path": evidence.get("source_authority_path") or outcome.get("source_authority_path"),
                "forward_outcome_classification": outcome.get("outcome_classification"),
                "mfe": outcome.get("mfe"),
                "mae": outcome.get("mae"),
                "net_movement": outcome.get("net_movement"),
                "simple_timebox_exit_helped": outcome.get("simple_timebox_exit_helped"),
                "profit_harvest_exit_helped": outcome.get("profit_harvest_exit_helped"),
                "forward_windows": outcome.get("forward_windows") if isinstance(outcome.get("forward_windows"), Mapping) else {},
                **_non_authority_flags(),
            }
        )

    outcome_counts = Counter(str(row.get("forward_outcome_classification") or "UNCLEAR") for row in candidates)
    valid_forward_count = sum(
        outcome_counts.get(name, 0) for name in ("MISSED_WINNER", "GOOD_REJECT", "AVOIDED_LOSER")
    )
    missed_winner_count = outcome_counts.get("MISSED_WINNER", 0)
    avoided_loser_count = outcome_counts.get("AVOIDED_LOSER", 0)
    missed_winner_rate = missed_winner_count / valid_forward_count if valid_forward_count else 0.0
    avoided_loser_rate = avoided_loser_count / valid_forward_count if valid_forward_count else 0.0
    avg_mae = _avg(row.get("mae") for row in candidates)
    profit_harvest_count = sum(1 for row in candidates if row.get("profit_harvest_exit_helped") is True)
    promotion_gates = [
        _promotion_gate("minimum_forward_sample_size", valid_forward_count, 30, valid_forward_count >= 30),
        _promotion_gate("missed_winner_rate", round(missed_winner_rate, 6), 0.6, missed_winner_rate >= 0.6),
        _promotion_gate("max_avoided_loser_rate", round(avoided_loser_rate, 6), 0.25, avoided_loser_rate <= 0.25),
        _promotion_gate("max_average_mae", round(avg_mae, 6), -6.0, avg_mae >= -6.0 if valid_forward_count else False),
        _promotion_gate("exit_profile_suitability", profit_harvest_count, 3, profit_harvest_count >= 3),
        _promotion_gate("lifecycle_order_management_failures", 0, 0, True),
    ]
    classification = (
        ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_READY
        if candidates
        else ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_EMPTY
    )
    return {
        "schema_version": "track_b_asian_drift_late_join_missing_anchor_long_shadow_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "shadow_strategy_id": ASIAN_DRIFT_LATE_JOIN_MISSING_ANCHOR_LONG_SHADOW_V1,
        "source_strategy_id": "asian_drift_v1",
        "strategy_family": "asian_drift_late_join_missing_anchor_continuation",
        "instrument": "MGC",
        "direction": "LONG",
        "candidate_count": len(candidates),
        "valid_forward_outcome_count": valid_forward_count,
        "forward_outcome_counts": dict(outcome_counts),
        "explainable_rule_basis": {
            "instrument": "MGC",
            "direction": "LONG",
            "regime": "late_join_drift_continuation",
            "requires_strong_drift_score": True,
            "requires_entry_window_context": True,
            "allowed_failed_predicate_shadow_only": "missing_18_00_et_session_anchor_context",
            "missing_anchor_allowed_only_in_shadow": True,
            "safety_infrastructure_gates_must_pass": True,
            "a_grade_asian_drift_rules_unchanged": True,
        },
        "promotion_gates": promotion_gates,
        "promotion_gate_status": "PROMOTION_PAUSED_COLLECT_FORWARD_EVIDENCE"
        if any(not gate["passed"] for gate in promotion_gates)
        else "PROMOTION_REVIEW_READY",
        "why_not_live_authority": (
            "This is a separate PAPER shadow family for forward collection. It does not weaken asian_drift_v1, "
            "does not create broker or lifecycle authority, and requires promotion review before any PAPER authority."
        ),
        "candidates": candidates[-100:],
        **_non_authority_flags(),
    }


def _b_grade_candidate_trait_row(outcome: Mapping[str, Any], near: Mapping[str, Any]) -> dict[str, Any]:
    forward_windows = outcome.get("forward_windows") if isinstance(outcome.get("forward_windows"), Mapping) else {}
    best_window = _best_forward_window(forward_windows)
    failed_predicates = [str(value) for value in _as_list(near.get("failed_predicates")) if str(value)]
    return {
        "candidate_id": outcome.get("candidate_id"),
        "strategy_id": outcome.get("strategy_id"),
        "timestamp": outcome.get("timestamp"),
        "session": outcome.get("regime_session"),
        "direction": outcome.get("direction"),
        "instrument": outcome.get("instrument"),
        "near_miss_score": outcome.get("score") or near.get("near_miss_score"),
        "hypothetical_score": near.get("hypothetical_score"),
        "failed_predicates": failed_predicates,
        "failed_critical_gates": _as_list(near.get("failed_critical_gates")),
        "failed_noncritical_gates": _as_list(near.get("failed_noncritical_gates")),
        "passed_predicate_traits": _passed_predicate_traits(near, outcome),
        "mfe": outcome.get("mfe"),
        "mae": outcome.get("mae"),
        "net_movement": outcome.get("net_movement"),
        "best_forward_window": best_window.get("window"),
        "best_forward_net_movement": best_window.get("net_movement"),
        "best_forward_mfe": best_window.get("mfe"),
        "harvest_exit_style": "PROFIT_IMPULSE_HARVEST"
        if outcome.get("profit_harvest_exit_helped")
        else "SIMPLE_TIMEBOX"
        if outcome.get("simple_timebox_exit_helped")
        else "UNPROVEN_EXIT",
        "source": outcome.get("source"),
        "source_authority_path": outcome.get("source_authority_path"),
    }


def _best_forward_window(forward_windows: Mapping[str, Any]) -> dict[str, Any]:
    best: dict[str, Any] = {"window": None, "net_movement": None, "mfe": None}
    for window, payload in forward_windows.items():
        if not isinstance(payload, Mapping) or not payload.get("complete"):
            continue
        mfe = _float(payload.get("mfe"))
        if mfe is None:
            continue
        if best["mfe"] is None or mfe > float(best["mfe"]):
            best = {
                "window": str(window),
                "net_movement": payload.get("net_movement"),
                "mfe": payload.get("mfe"),
            }
    return best


def _passed_predicate_traits(near: Mapping[str, Any], outcome: Mapping[str, Any]) -> list[str]:
    traits = []
    if near.get("threshold_passed"):
        traits.append("near_miss_score_threshold_passed")
    if not _as_list(near.get("failed_critical_gates")):
        traits.append("no_critical_safety_or_infrastructure_gate_failed")
    if outcome.get("profit_harvest_exit_helped"):
        traits.append("profit_harvest_forward_move_observed")
    if str(outcome.get("direction") or "").upper() in {"LONG", "SHORT"}:
        traits.append("directional_hypothesis_present")
    if _float(outcome.get("mfe")) is not None and float(outcome.get("mfe") or 0.0) >= 2.0:
        traits.append("material_forward_mfe_observed")
    return traits


def _common_passed_traits(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    if not rows:
        return []
    common = set(str(value) for value in _as_list(rows[0].get("passed_predicate_traits")))
    for row in rows[1:]:
        common &= set(str(value) for value in _as_list(row.get("passed_predicate_traits")))
    return sorted(common)


def _family_hypothesis(family: str, failed_predicates: Sequence[str], atp_overlap_count: int) -> str:
    failed = " ".join(failed_predicates).lower()
    if "missing_18_00_et_session_anchor_context" in failed and family == "asian_drift":
        return "LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR"
    if atp_overlap_count > 0:
        return "EXISTING_ATP_TREND_PARTICIPATION_OVERLAP"
    if "breakout" in failed or "retest" in failed:
        return "RELAXED_BREAKOUT_RETEST_BEHAVIOR"
    return "UNRESOLVED_B_GRADE_NEAR_MISS_BEHAVIOR"


def _ranked_family_classification(count: int, hypothesis: str) -> str:
    if count >= 3 and hypothesis in {
        "LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR",
        "EXISTING_ATP_TREND_PARTICIPATION_OVERLAP",
        "RELAXED_BREAKOUT_RETEST_BEHAVIOR",
    }:
        return "B_GRADE_PROMISING_SHADOW"
    if count >= 2:
        return "NEEDS_MORE_FORWARD_EVIDENCE"
    return "LIKELY_NOISE"


def _shadow_instrumentation_recommendation(hypothesis: str) -> str:
    if hypothesis == "LATE_JOIN_DRIFT_CONTINUATION_BEHAVIOR":
        return (
            "Add shadow-only late-join Asian Drift family with explicit missing-anchor reason, "
            "post-anchor drift score, profit-harvest MFE/MAE tracking, and no submit authority."
        )
    if hypothesis == "EXISTING_ATP_TREND_PARTICIPATION_OVERLAP":
        return "Link ATP shadow candidates to strict-strategy near misses and track shared forward outcomes."
    if hypothesis == "RELAXED_BREAKOUT_RETEST_BEHAVIOR":
        return "Create relaxed breakout/retest shadow diagnostics with critical-gate preservation."
    return "Collect more B-grade forward evidence before designing a specific shadow family."


def _avg(values: Any) -> float:
    numeric = [_float(value) for value in values]
    numeric = [value for value in numeric if value is not None]
    return sum(numeric) / len(numeric) if numeric else 0.0


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def _build_combined_report(
    *,
    atp_shadow: Mapping[str, Any],
    atp_lifecycle_mapping_shadow: Mapping[str, Any],
    near_miss_shadow: Mapping[str, Any],
    shadow_candidate_maturation: Mapping[str, Any],
    timestamp_locked_evidence: Mapping[str, Any],
    forward_outcomes: Mapping[str, Any],
    late_join_missing_anchor_shadow: Mapping[str, Any],
    review: Mapping[str, Any],
    overfiltering: Mapping[str, Any],
    now: datetime,
    source_paths: Mapping[str, Path],
) -> dict[str, Any]:
    ranked_b_grade_families = rank_b_grade_missed_winner_families(
        near_miss_shadow=near_miss_shadow,
        forward_outcomes=forward_outcomes,
        atp_shadow=atp_shadow,
    )
    recommendations = _discovery_recommendations(atp_shadow, near_miss_shadow, review, overfiltering)
    return {
        "schema_version": "track_b_missed_opportunity_discovery_layer_v1",
        "generated_at": now.isoformat(),
        "classification": MISSED_OPPORTUNITY_DISCOVERY_READY,
        "atp_shadow_summary": {
            "classification": atp_shadow.get("classification"),
            "candidate_count": atp_shadow.get("candidate_count"),
            "shadow_candidate_count": atp_shadow.get("shadow_candidate_count"),
            "classification_counts": atp_shadow.get("classification_counts"),
        },
        "atp_lifecycle_mapping_shadow_summary": {
            "classification": atp_lifecycle_mapping_shadow.get("classification"),
            "candidate_count": atp_lifecycle_mapping_shadow.get("candidate_count"),
            "mapped_shadow_only_count": atp_lifecycle_mapping_shadow.get("mapped_shadow_only_count"),
            "status_counts": atp_lifecycle_mapping_shadow.get("status_counts"),
            "lifecycle_authority": False,
            "submit_allowed": False,
            "broker_mutation_allowed": False,
        },
        "near_miss_scoring_summary": {
            "classification": near_miss_shadow.get("classification"),
            "candidate_count": near_miss_shadow.get("candidate_count"),
            "b_grade_shadow_candidate_count": near_miss_shadow.get("b_grade_shadow_candidate_count"),
            "grade_counts": near_miss_shadow.get("grade_counts"),
        },
        "forward_outcome_summary": {
            "classification": forward_outcomes.get("classification"),
            "candidate_count": forward_outcomes.get("candidate_count"),
            "classification_counts": forward_outcomes.get("classification_counts"),
        },
        "shadow_candidate_maturation_summary": {
            "classification": shadow_candidate_maturation.get("classification"),
            "candidate_count": shadow_candidate_maturation.get("candidate_count"),
            "pending_count": shadow_candidate_maturation.get("pending_count"),
            "partially_matured_count": shadow_candidate_maturation.get("partially_matured_count"),
            "final_count": shadow_candidate_maturation.get("final_count"),
            "status_counts": shadow_candidate_maturation.get("status_counts"),
        },
        "timestamp_locked_forward_evidence_summary": {
            "classification": timestamp_locked_evidence.get("classification"),
            "candidate_count": timestamp_locked_evidence.get("candidate_count"),
            "classification_counts": timestamp_locked_evidence.get("classification_counts"),
        },
        "b_grade_missed_winner_family_rankings": ranked_b_grade_families,
        "persistent_shadow_families": [
            {
                "shadow_strategy_id": late_join_missing_anchor_shadow.get("shadow_strategy_id"),
                "classification": late_join_missing_anchor_shadow.get("classification"),
                "candidate_count": late_join_missing_anchor_shadow.get("candidate_count"),
                "valid_forward_outcome_count": late_join_missing_anchor_shadow.get("valid_forward_outcome_count"),
                "forward_outcome_counts": late_join_missing_anchor_shadow.get("forward_outcome_counts"),
                "promotion_gate_status": late_join_missing_anchor_shadow.get("promotion_gate_status"),
                "promotion_allowed": False,
                **_non_authority_flags(),
            }
        ],
        "promotion_gate_recommendations": recommendations,
        "sub80_scope": "DEFER_UNTIL_ATP_AND_NEAR_MISS_SHADOWS_HAVE_FORWARD_EVIDENCE",
        "source_artifact_paths": {key: str(value) for key, value in source_paths.items()},
        **_non_authority_flags(),
    }


def _discover_atp_candidates(repo_root: Path) -> list[dict[str, Any]]:
    rows = []
    seen: set[tuple[str, str | None]] = set()
    for path in sorted((repo_root / "config").glob("*atp_companion*.yaml")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        lane = _extract_lane_payload(text)
        if not lane:
            continue
        lane_id = str(lane.get("lane_id") or path.stem)
        strategy_id = str(lane.get("standalone_strategy_id") or lane_id)
        key = (strategy_id, str(lane.get("symbol") or ""))
        if key in seen:
            continue
        seen.add(key)
        direction = _candidate_direction(lane)
        rows.append(
            {
                "candidate_id": lane.get("candidate_id") or lane_id,
                "lane_id": lane_id,
                "strategy_id": strategy_id,
                "strategy_family": lane.get("strategy_family") or "active_trend_participation_engine",
                "strategy_identity_root": lane.get("strategy_identity_root") or "ATP_COMPANION_V1",
                "instrument": lane.get("symbol") or _instrument_from_text(strategy_id),
                "session_regime": lane.get("session_restriction") or "/".join(_as_list(lane.get("allowed_sessions"))),
                "direction_hint": direction,
                "quality_bucket_policy": lane.get("quality_bucket_policy"),
                "experimental_status": lane.get("experimental_status"),
                "paper_only": lane.get("paper_only"),
                "non_approved": lane.get("non_approved"),
                "research_lineage": lane.get("candidate_origin") or "atp_companion_config",
                "config_path": str(path),
                "implementation_state": _candidate_implementation_state(lane),
            }
        )
    return rows


def _evaluate_atp_candidate(
    candidate: Mapping[str, Any],
    *,
    candles: Sequence[Mapping[str, Any]],
    min_score: float,
) -> dict[str, Any]:
    failed = []
    if not candles:
        failed.append("missing_phase1_runtime_market_data")
        return {
            "candidate_ready": False,
            "direction": candidate.get("direction_hint") or "UNKNOWN",
            "score": 0.0,
            "confidence": "LOW",
            "failed_gates": failed,
            "latest_candle_timestamp": None,
        }
    rows = [_normalise_candle(row) for row in candles if isinstance(row, Mapping)]
    rows = [row for row in rows if row.get("close") is not None]
    if len(rows) < 6:
        failed.append("insufficient_completed_candles")
    window = rows[-min(len(rows), 12) :]
    first = float(window[0]["close"])
    last = float(window[-1]["close"])
    highs = [float(row["high"]) for row in window if row.get("high") is not None]
    lows = [float(row["low"]) for row in window if row.get("low") is not None]
    total_range = max(highs) - min(lows) if highs and lows else abs(last - first)
    net = last - first
    direction_hint = str(candidate.get("direction_hint") or "BOTH").upper()
    direction = "LONG" if net >= 0 else "SHORT"
    if direction_hint == "LONG" and net <= 0:
        failed.append("long_direction_not_confirmed")
    if direction_hint == "SHORT" and net >= 0:
        failed.append("short_direction_not_confirmed")
    if total_range <= 0:
        failed.append("zero_range_context")
    directionality = abs(net) / total_range if total_range else 0.0
    close_location = (last - min(lows)) / total_range if total_range and lows else 0.5
    aligned = (
        (direction == "LONG" and close_location >= 0.55)
        or (direction == "SHORT" and close_location <= 0.45)
        or direction_hint == "BOTH"
    )
    if not aligned:
        failed.append("close_location_not_directionally_aligned")
    score = max(0.0, min(1.0, directionality * 0.75 + (0.25 if aligned else 0.0)))
    candidate_ready = score >= min_score and not failed
    confidence = "HIGH" if score >= 0.78 and not failed else "MEDIUM" if score >= min_score else "LOW"
    return {
        "candidate_ready": candidate_ready,
        "direction": direction if direction_hint == "BOTH" else direction_hint,
        "score": round(score, 6),
        "confidence": confidence,
        "failed_gates": failed,
        "latest_candle_timestamp": window[-1].get("timestamp"),
    }


def _score_near_miss(
    item: Mapping[str, Any],
    outcome_by_strategy: Mapping[str, Counter[str]],
    *,
    min_b_score: float,
) -> dict[str, Any]:
    strategy_id = str(item.get("strategy_id") or "UNKNOWN")
    failed = [str(value) for value in _as_list(item.get("failed_predicates")) if str(value)]
    critical = [gate for gate in failed if _is_critical_gate(gate)]
    noncritical = [gate for gate in failed if gate not in critical]
    gate_distance = _int(item.get("gate_distance"), len(failed) if failed else 0)
    base = 1.0 - 0.12 * max(gate_distance, len(failed), 1)
    base -= 0.22 * len(critical)
    base -= 0.04 * max(0, len(noncritical) - 1)
    score = max(0.0, min(0.99, base))
    threshold_passed = score >= min_b_score and not critical
    if not failed and threshold_passed:
        grade = "A"
    elif threshold_passed:
        grade = "B"
    elif score >= 0.55 and not critical:
        grade = "C"
    else:
        grade = "REJECT"
    outcomes = outcome_by_strategy.get(strategy_id, Counter())
    outcome_classification = _dominant_outcome(outcomes)
    return {
        "strategy_id": strategy_id,
        "generated_at": item.get("generated_at"),
        "near_miss_score": round(score, 6),
        "threshold_passed": threshold_passed,
        "failed_critical_gates": critical,
        "failed_noncritical_gates": noncritical,
        "failed_predicates": failed,
        "approval_grade": grade,
        "hypothetical_direction": item.get("hypothetical_direction"),
        "hypothetical_score": item.get("hypothetical_score"),
        "forward_outcome_counts": dict(outcomes),
        "forward_outcome_classification": outcome_classification,
        "recommendation": _near_miss_recommendation(grade, outcome_classification),
        **_non_authority_flags(),
    }


def _near_miss_strategy_summary(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_strategy: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_strategy[str(row.get("strategy_id") or "UNKNOWN")].append(row)
    summary = []
    for strategy_id, items in sorted(by_strategy.items()):
        grades = Counter(str(item.get("approval_grade")) for item in items)
        outcomes = Counter(str(item.get("forward_outcome_classification")) for item in items)
        one_gate = sum(1 for item in items if len(_as_list(item.get("failed_predicates"))) == 1)
        two_gate = sum(1 for item in items if len(_as_list(item.get("failed_predicates"))) == 2)
        summary.append(
            {
                "strategy_id": strategy_id,
                "candidate_count": len(items),
                "one_gate_away_count": one_gate,
                "two_gate_away_count": two_gate,
                "b_grade_shadow_candidates": grades.get("B", 0),
                "grade_counts": dict(grades),
                "forward_outcome_counts": dict(outcomes),
            }
        )
    return summary


def _discovery_recommendations(
    atp_shadow: Mapping[str, Any],
    near_miss_shadow: Mapping[str, Any],
    review: Mapping[str, Any],
    overfiltering: Mapping[str, Any],
) -> list[dict[str, Any]]:
    recs = []
    if _int(atp_shadow.get("shadow_candidate_count")) > 0 or _int(atp_shadow.get("candidate_count")) > 0:
        recs.append(
            _recommendation(
                "ADD_SHADOW_ONLY",
                "Run ATP/trend participation candidates as a Track B shadow cohort",
                "HIGH",
                "ATP configs are implemented but absent from guarded PAPER authority; shadow cohort can measure missed trend flow.",
                "No submit authority; requires shared-services diagnostic-only wiring.",
            )
        )
    if _int(near_miss_shadow.get("b_grade_shadow_candidate_count")) > 0:
        recs.append(
            _recommendation(
                "ADD_SHADOW_ONLY",
                "Persist scored B-grade near-miss shadow candidates",
                "HIGH",
                f"{near_miss_shadow.get('b_grade_shadow_candidate_count')} B-grade candidates scored from live strict-strategy rejects.",
                "B-grade remains non-authoritative until forward evidence clears tollgate.",
            )
        )
    root_causes = json.dumps(review.get("top_underperformance_root_causes") or [])
    if "REGIME_COVERAGE_GAP" in root_causes or "ANCHOR_POLICY" in root_causes:
        recs.append(
            _recommendation(
                "COLLECT_MORE_FORWARD_EVIDENCE",
                "Prioritize late-join drift and gap/continuation forward evidence",
                "HIGH",
                "Existing reviews point to missing regime coverage before raw threshold mining.",
                "Continuation shadows can chase; require MFE/MAE by regime.",
            )
        )
    recs.append(
        _recommendation(
            "KEEP_LIVE_RULES_UNCHANGED",
            "Keep A-grade live predicates unchanged",
            "HIGH",
            "Current evidence supports discovery shadows, not live loosening.",
            "Opportunity cost only; safety and attribution remain clean.",
        )
    )
    recs.append(
        _recommendation(
            "DO_NOT_PROMOTE",
            "Do not promote raw sub-80 score buckets",
            "MEDIUM",
            "The broad >=0.80 universe is already negative after cost; sub-80 needs explainable subsets first.",
            "Raw mining likely increases false positives.",
        )
    )
    return recs[:5]


def _forward_candidate_rows(
    *,
    atp_shadow: Mapping[str, Any],
    near_miss_shadow: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _as_list(atp_shadow.get("candidates")):
        if not isinstance(item, Mapping):
            continue
        if item.get("shadow_classification") not in {
            ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT,
            ATP_SHADOW_CONFIRMS_LIVE_SIGNAL,
        }:
            continue
        rows.append(
            {
                "candidate_id": item.get("candidate_id") or item.get("lane_id"),
                "strategy_id": item.get("strategy_id"),
                "strategy_family": item.get("strategy_family"),
                "source": "ATP_TREND_PARTICIPATION_SHADOW",
                "instrument": item.get("instrument"),
                "direction": item.get("direction"),
                "timestamp": item.get("source_candle_timestamp"),
                "reference_price": item.get("reference_price"),
                "score": item.get("score"),
                "confidence": item.get("confidence"),
                "regime_session": item.get("session_regime"),
                "approval_grade": None,
                "source_authority_path": item.get("source_authority_path"),
            }
        )
    for item in _as_list(near_miss_shadow.get("scored_candidates")):
        if not isinstance(item, Mapping) or item.get("approval_grade") != "B":
            continue
        rows.append(
            {
                "candidate_id": f"{item.get('strategy_id')}:{item.get('generated_at') or len(rows)}",
                "strategy_id": item.get("strategy_id"),
                "strategy_family": _strategy_family_for_id(str(item.get("strategy_id") or "")),
                "source": "GENERAL_NEAR_MISS_SCORED_SHADOW",
                "instrument": _symbol_for_strategy(str(item.get("strategy_id") or "")),
                "direction": item.get("hypothetical_direction") or _default_direction(str(item.get("strategy_id") or "")),
                "timestamp": item.get("generated_at"),
                "reference_price": item.get("reference_price"),
                "score": item.get("near_miss_score"),
                "confidence": "B_GRADE",
                "regime_session": None,
                "approval_grade": item.get("approval_grade"),
                "source_authority_path": None,
            }
        )
    return rows


def _classify_shadow_forward(
    best_mfe: float,
    worst_mae: float,
    terminal: float,
    *,
    complete_count: int,
    profit_harvest_helped: bool = False,
) -> str:
    if complete_count <= 0:
        return "UNCLEAR"
    if best_mfe >= 2.0 and (terminal > 0 or profit_harvest_helped):
        return "MISSED_WINNER"
    if terminal < 0 and abs(worst_mae) >= max(best_mfe, 1.0):
        return "AVOIDED_LOSER"
    if best_mfe <= 0.5 and terminal <= 0:
        return "GOOD_REJECT"
    return "UNCLEAR"


def _shadow_forward_reason(
    classification: str,
    best_mfe: float,
    worst_mae: float,
    terminal: float,
    complete_count: int,
) -> str:
    if complete_count <= 0:
        return "insufficient_completed_forward_windows"
    if classification == "MISSED_WINNER":
        return (
            f"favorable MFE {round(best_mfe, 4)} with tradable forward movement; "
            f"terminal movement {round(terminal, 4)}"
        )
    if classification == "AVOIDED_LOSER":
        return f"adverse excursion {round(abs(worst_mae), 4)} dominated favorable MFE {round(best_mfe, 4)}"
    if classification == "GOOD_REJECT":
        return "little favorable movement and nonpositive terminal movement"
    return "mixed_or_incomplete_forward_evidence"


def _shared_context(
    *,
    control_plane: Mapping[str, Any],
    safe_state: Mapping[str, Any],
    runtime_authority: Mapping[str, Any],
    source_paths: Mapping[str, Path],
) -> dict[str, Any]:
    return {
        "control_plane_classification": control_plane.get("classification"),
        "shared_truth_coherence_status": control_plane.get("shared_truth_coherence_status"),
        "safe_state_classification": safe_state.get("safe_state_classification")
        or safe_state.get("classification"),
        "runtime_authority_classification": runtime_authority.get("classification"),
        "live_money_eligible": bool(control_plane.get("live_money_eligible") or safe_state.get("live_money_eligible")),
        "paper_proof_invoked": bool(control_plane.get("paper_proof_invoked") or safe_state.get("paper_proof_invoked")),
        "read_only_context": True,
        "source_artifact_paths": {key: str(value) for key, value in source_paths.items()},
    }


def _load_phase1_candles(phase1_root: Path, symbol: str) -> tuple[list[dict[str, Any]], Path | None]:
    if not symbol:
        return [], None
    path = phase1_root / symbol / "5m" / "latest_runtime_candles.json"
    payload = _load_json(path)
    if not payload and symbol == "GC":
        path = phase1_root / "MGC" / "5m" / "latest_runtime_candles.json"
        payload = _load_json(path)
    normalized = normalize_phase1_runtime_candle_payload(payload, source_path=path) if payload else {}
    candles = normalized.get("candles") or normalized.get("candle_history") or []
    return [row for row in candles if isinstance(row, Mapping)], path if payload else None


def _load_replay_candles_for_candidate(root: Path, symbol: str, timestamp: str) -> dict[str, Any]:
    symbol = str(symbol or "").upper()
    candidate_dt = _parse_dt(timestamp)
    checked_paths: list[str] = []
    if not symbol or candidate_dt is None:
        return {"candles": [], "source_path": None, "checked_paths": checked_paths, "lookup_status": "REPLAY_INVALID_INPUT"}
    parquet_path = _historical_replay_parquet_path(root, symbol, candidate_dt)
    checked_paths.append(str(parquet_path))
    parquet_candles = _load_historical_replay_parquet_candles(parquet_path, symbol=symbol)
    if parquet_candles:
        return {
            "candles": parquet_candles,
            "source_path": parquet_path,
            "checked_paths": checked_paths,
            "lookup_status": "REPLAY_PARQUET_FOUND",
        }
    databento = _load_databento_backfill_candles(symbol=symbol, candidate_dt=candidate_dt)
    checked_paths.extend(_as_list(databento.get("checked_paths")))
    if _as_list(databento.get("candles")):
        return {
            "candles": _as_list(databento.get("candles")),
            "source_path": databento.get("source_path"),
            "checked_paths": checked_paths,
            "lookup_status": databento.get("lookup_status") or "DATABENTO_BACKFILL_FOUND",
        }
    return {
        "candles": [],
        "source_path": None,
        "checked_paths": checked_paths,
        "lookup_status": databento.get("lookup_status") or "REPLAY_DATA_NOT_FOUND",
    }


def _historical_replay_parquet_path(root: Path, symbol: str, timestamp: datetime) -> Path:
    quarter = (timestamp.month - 1) // 3 + 1
    shard = f"{timestamp.year}Q{quarter}"
    return (
        root
        / symbol
        / shard
        / "datasets"
        / "derived_bars_5m"
        / f"symbol={symbol}"
        / f"year={timestamp.year}"
        / f"shard_id={shard}"
        / "bars.parquet"
    )


def _load_historical_replay_parquet_candles(path: Path, *, symbol: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        import pandas as pd  # type: ignore[import-not-found]
    except ImportError:
        return []
    try:
        frame = pd.read_parquet(path)
    except Exception:
        return []
    if not {"symbol", "timeframe", "bar_ts", "high", "low", "close"}.issubset(set(frame.columns)):
        return []
    frame = frame[(frame["symbol"] == symbol) & (frame["timeframe"] == "5m")].copy()
    if frame.empty:
        return []
    frame = frame.sort_values("bar_ts")
    rows = []
    for row in frame.to_dict("records"):
        rows.append(
            {
                "timestamp": _iso_from_any_dt(row.get("bar_ts")),
                "high": _float(row.get("high")),
                "low": _float(row.get("low")),
                "close": _float(row.get("close")),
                "symbol": row.get("symbol"),
                "timeframe": row.get("timeframe"),
                "source": "historical_replay_parquet",
            }
        )
    return [row for row in rows if row.get("timestamp") and row.get("high") is not None and row.get("low") is not None]


def _load_databento_backfill_candles(*, symbol: str, candidate_dt: datetime) -> dict[str, Any]:
    api_key = os.environ.get("DATABENTO_API_KEY")
    checked = [f"databento:{symbol}:ohlcv-1m:{candidate_dt.isoformat()}"]
    if not api_key:
        return {"candles": [], "source_path": None, "checked_paths": checked, "lookup_status": "DATABENTO_API_KEY_MISSING"}
    try:
        import databento as db  # type: ignore[import-not-found]
        import pandas as pd  # type: ignore[import-not-found]
    except ImportError:
        return {"candles": [], "source_path": None, "checked_paths": checked, "lookup_status": "DATABENTO_PACKAGE_MISSING"}
    dataset = os.environ.get("DATABENTO_FUTURES_DATASET", "GLBX.MDP3")
    db_symbol = os.environ.get(f"DATABENTO_{symbol}_SYMBOL") or _databento_symbol(symbol)
    start = candidate_dt - timedelta(minutes=10)
    end = candidate_dt + timedelta(minutes=75)
    try:
        client = db.Historical(api_key)
        data = client.timeseries.get_range(
            dataset=dataset,
            symbols=[db_symbol],
            schema="ohlcv-1m",
            start=start.isoformat(),
            end=end.isoformat(),
        )
        frame = data.to_df()
    except Exception as exc:
        return {
            "candles": [],
            "source_path": None,
            "checked_paths": checked,
            "lookup_status": f"DATABENTO_BACKFILL_FAILED:{type(exc).__name__}",
        }
    candles = _resample_databento_ohlcv_to_5m(frame, symbol=symbol, pd=pd)
    return {
        "candles": candles,
        "source_path": f"databento:{dataset}:{db_symbol}:ohlcv-1m",
        "checked_paths": checked,
        "lookup_status": "DATABENTO_BACKFILL_FOUND" if candles else "DATABENTO_BACKFILL_EMPTY",
    }


def _databento_symbol(symbol: str) -> str:
    mapping = {"MGC": "MGC.c.0", "GC": "GC.c.0", "MNQ": "MNQ.c.0", "NQ": "NQ.c.0", "MES": "MES.c.0", "ES": "ES.c.0"}
    return mapping.get(symbol.upper(), f"{symbol.upper()}.c.0")


def _resample_databento_ohlcv_to_5m(frame: Any, *, symbol: str, pd: Any) -> list[dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return []
    data = frame.copy()
    if "ts_event" in data.columns:
        data["timestamp"] = pd.to_datetime(data["ts_event"], utc=True)
    elif data.index.name:
        data["timestamp"] = pd.to_datetime(data.index, utc=True)
    else:
        return []
    rename = {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume"}
    if not {"timestamp", "open", "high", "low", "close"}.issubset(set(data.columns)):
        return []
    data = data.sort_values("timestamp").set_index("timestamp")
    resampled = data.resample("5min", label="right", closed="right").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"}
    )
    resampled = resampled.dropna(subset=["open", "high", "low", "close"])
    rows = []
    for timestamp, row in resampled.iterrows():
        rows.append(
            {
                "timestamp": _iso_from_any_dt(timestamp),
                "high": _float(row.get("high")),
                "low": _float(row.get("low")),
                "close": _float(row.get("close")),
                "symbol": symbol,
                "timeframe": "5m",
                "source": "databento_historical_backfill",
            }
        )
    return rows


def _extract_lane_payload(text: str) -> dict[str, Any]:
    match = re.search(r"probationary_paper_lanes_json:\s*'(?P<payload>\[.*?\])'", text, flags=re.DOTALL)
    if not match:
        return {}
    try:
        payload = json.loads(match.group("payload"))
    except json.JSONDecodeError:
        return {}
    if isinstance(payload, list) and payload and isinstance(payload[0], Mapping):
        return dict(payload[0])
    return {}


def _candidate_direction(lane: Mapping[str, Any]) -> str:
    has_long = bool(_as_list(lane.get("long_sources")))
    has_short = bool(_as_list(lane.get("short_sources")))
    if has_long and has_short:
        return "BOTH"
    if has_short:
        return "SHORT"
    return "LONG"


def _candidate_implementation_state(lane: Mapping[str, Any]) -> str:
    if str(lane.get("non_approved")).lower() == "false":
        return "IMPLEMENTED_PRODUCTION_TRACK_OR_APPROVED_CONFIG"
    if lane.get("standalone_strategy_id") or lane.get("lane_id"):
        return "IMPLEMENTED_RESEARCH_OR_PROBATIONARY_CONFIG"
    return "RESEARCH_LINEAGE_ONLY"


def _live_signal_index(review: Mapping[str, Any]) -> Counter[str]:
    index: Counter[str] = Counter()
    rejection = review.get("rejection_attribution") if isinstance(review.get("rejection_attribution"), Mapping) else {}
    for row in _as_list(rejection.get("per_strategy")):
        if not isinstance(row, Mapping):
            continue
        signals = _int(row.get("signals"))
        if signals:
            index[_symbol_for_strategy(str(row.get("strategy_id") or ""))] += signals
    return index


def _normalise_candle(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "timestamp": row.get("candle_timestamp") or row.get("bar_end") or row.get("timestamp"),
        "high": _float(row.get("high")),
        "low": _float(row.get("low")),
        "close": _float(row.get("close")),
    }


def _first_candle_index_at_or_after(candles: Sequence[Mapping[str, Any]], timestamp: str) -> int | None:
    if not timestamp:
        return None
    first_timestamp = str(candles[0].get("timestamp") or "") if candles else ""
    last_timestamp = str(candles[-1].get("timestamp") or "") if candles else ""
    if not first_timestamp or not last_timestamp:
        return None
    requested_dt = _parse_dt(timestamp)
    first_dt = _parse_dt(first_timestamp)
    last_dt = _parse_dt(last_timestamp)
    if requested_dt is None or first_dt is None or last_dt is None:
        return None
    if requested_dt < first_dt:
        return 0 if timedelta(0) <= first_dt - requested_dt <= timedelta(minutes=5) else None
    if requested_dt > last_dt:
        return None
    for index, candle in enumerate(candles):
        if str(candle.get("timestamp") or "") >= timestamp:
            return index
    return None


def _has_five_minute_continuity(candles: Sequence[Mapping[str, Any]]) -> bool:
    if len(candles) <= 1:
        return bool(candles)
    timestamps = [_parse_dt(str(row.get("timestamp") or "")) for row in candles]
    timestamps = [item for item in timestamps if item is not None]
    if len(timestamps) != len(candles):
        return False
    return all((right - left) == timedelta(minutes=5) for left, right in zip(timestamps, timestamps[1:]))


def _parse_dt(value: str) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso_from_any_dt(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            try:
                parsed = value.to_pydatetime()
            except AttributeError:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat()


def _is_critical_gate(gate: str) -> bool:
    lowered = gate.lower()
    return any(token in lowered for token in CRITICAL_GATE_TOKENS)


def _dominant_outcome(counter: Mapping[str, int]) -> str:
    if not counter:
        return "UNCLEAR"
    return Counter(counter).most_common(1)[0][0]


def _near_miss_recommendation(grade: str, outcome: str) -> str:
    if grade == "B" and outcome == "MISSED_WINNER":
        return "PROMOTION_CANDIDATE_LATER"
    if grade == "B":
        return "COLLECT_MORE_FORWARD_EVIDENCE"
    if grade == "C":
        return "ADD_SHADOW_ONLY"
    return "DO_NOT_PROMOTE"


def _recommendation(action: str, title: str, impact: str, evidence: str, risk: str) -> dict[str, Any]:
    return {
        "recommendation": action,
        "title": title,
        "expected_impact": impact,
        "evidence": evidence,
        "risk": risk,
        "live_rule_change_allowed": False,
    }


def _promotion_gate(name: str, observed: Any, required: Any, passed: bool) -> dict[str, Any]:
    return {
        "gate": name,
        "observed": observed,
        "required": required,
        "passed": bool(passed),
    }


def _event_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at"),
        "schema_version": payload.get("schema_version"),
        "classification": payload.get("classification"),
        "candidate_count": payload.get("candidate_count"),
        "shadow_candidate_count": payload.get("shadow_candidate_count")
        or payload.get("b_grade_shadow_candidate_count"),
        "shadow_strategy_id": payload.get("shadow_strategy_id"),
        "pending_count": payload.get("pending_count"),
        "partially_matured_count": payload.get("partially_matured_count"),
        "final_count": payload.get("final_count"),
        **_non_authority_flags(),
    }


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True) + "\n")


def _render_markdown(report: Mapping[str, Any]) -> str:
    atp = report.get("atp_shadow_summary") if isinstance(report.get("atp_shadow_summary"), Mapping) else {}
    atp_mapping = (
        report.get("atp_lifecycle_mapping_shadow_summary")
        if isinstance(report.get("atp_lifecycle_mapping_shadow_summary"), Mapping)
        else {}
    )
    near = report.get("near_miss_scoring_summary") if isinstance(report.get("near_miss_scoring_summary"), Mapping) else {}
    forward = report.get("forward_outcome_summary") if isinstance(report.get("forward_outcome_summary"), Mapping) else {}
    maturation = (
        report.get("shadow_candidate_maturation_summary")
        if isinstance(report.get("shadow_candidate_maturation_summary"), Mapping)
        else {}
    )
    timestamp_locked = (
        report.get("timestamp_locked_forward_evidence_summary")
        if isinstance(report.get("timestamp_locked_forward_evidence_summary"), Mapping)
        else {}
    )
    ranked_families = _as_list(report.get("b_grade_missed_winner_family_rankings"))
    persistent_families = _as_list(report.get("persistent_shadow_families"))
    lines = [
        "# Track B Missed-Opportunity Discovery Layer",
        "",
        f"Generated: `{report.get('generated_at')}`",
        "",
        "Shadow/diagnostic only. No live rules were loosened, no broker/lifecycle authority was created, and no paper_proof/live-money route was introduced.",
        "",
        "## ATP / Trend Participation Shadow",
        "",
        f"- Classification: `{atp.get('classification')}`",
        f"- Candidate count: `{atp.get('candidate_count')}`",
        f"- Shadow candidate count: `{atp.get('shadow_candidate_count')}`",
        f"- Counts: `{atp.get('classification_counts')}`",
        f"- Lifecycle mapping shadow: `{atp_mapping}`",
        "",
        "## Near-Miss Scoring",
        "",
        f"- Classification: `{near.get('classification')}`",
        f"- Candidate count: `{near.get('candidate_count')}`",
        f"- B-grade candidate count: `{near.get('b_grade_shadow_candidate_count')}`",
        f"- Grade counts: `{near.get('grade_counts')}`",
        "",
        "## Forward Outcomes",
        "",
        f"- Shadow candidate maturation: `{maturation}`",
        f"- Timestamp-locked evidence: `{timestamp_locked}`",
        f"- Classification: `{forward.get('classification')}`",
        f"- Candidate count: `{forward.get('candidate_count')}`",
        f"- Outcome counts: `{forward.get('classification_counts')}`",
        "",
        "## Persistent PAPER Shadow Families",
        "",
        "| Shadow Family | Classification | Candidates | Valid Forward Outcomes | Outcome Counts | Promotion Gate |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in persistent_families:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            f"| `{row.get('shadow_strategy_id')}` | `{row.get('classification')}` | {row.get('candidate_count')} | {row.get('valid_forward_outcome_count')} | `{row.get('forward_outcome_counts')}` | `{row.get('promotion_gate_status')}` |"
        )
    lines.extend(
        [
            "",
        "## B-Grade Missed-Winner Family Rankings",
        "",
        "| Rank | Classification | Family | Instrument | Direction | Count | Failed Traits | Behavior | Exit Style |",
        "| ---: | --- | --- | --- | --- | ---: | --- | --- | --- |",
        ]
    )
    for row in ranked_families:
        if not isinstance(row, Mapping):
            continue
        lines.append(
            f"| {row.get('rank')} | `{row.get('classification')}` | `{row.get('strategy_family')}` | `{row.get('instrument')}` | `{row.get('direction')}` | {row.get('candidate_count')} | `{row.get('common_failed_predicates')}` | `{row.get('behavioral_hypothesis')}` | `{row.get('harvest_exit_style_counts')}` |"
        )
    lines.extend(
        [
            "",
        "## Promotion Gate Recommendations",
        "",
        "| Recommendation | Title | Impact | Risk |",
        "| --- | --- | --- | --- |",
        ]
    )
    for row in _as_list(report.get("promotion_gate_recommendations")):
        if not isinstance(row, Mapping):
            continue
        lines.append(
            f"| `{row.get('recommendation')}` | {row.get('title')} | `{row.get('expected_impact')}` | {row.get('risk')} |"
        )
    lines.extend(
        [
            "",
            "## Tollgate",
            "",
            "- Keep live A-grade predicates unchanged.",
            "- Require forward MFE/MAE and exit attribution before promotion.",
            "- Promote explainable shadows only; do not promote raw lower-score buckets.",
            "",
        ]
    )
    return "\n".join(lines)


def _active_roster_ids(roster: Mapping[str, Any], fallback: Any) -> set[str]:
    ids = {str(item) for item in _as_list(fallback) if str(item)}
    for key in ("enabled_strategy_ids", "strategies", "enabled_strategies", "roster", "strategy_ids"):
        for item in _as_list(roster.get(key)):
            if isinstance(item, Mapping):
                value = item.get("strategy_id") or item.get("standalone_strategy_id") or item.get("id")
            else:
                value = item
            if value:
                ids.add(str(value))
    return ids


def _symbol_for_strategy(strategy_id: str) -> str:
    upper = strategy_id.upper()
    if "MNQ" in upper or "NQ" in upper:
        return "MNQ"
    if "GC" in upper and "MGC" not in upper:
        return "GC"
    if "PL" in upper:
        return "PL"
    return "MGC"


def _strategy_family_for_id(strategy_id: str) -> str:
    upper = strategy_id.upper()
    if "DRIFT" in upper:
        return "asian_drift"
    if "SNAP_TURN" in upper:
        return "snap_turn"
    if "BREAKOUT" in upper or "RETEST" in upper:
        return "breakout_retest"
    if "PAUSE_RESUME" in upper:
        return "pause_resume"
    return "track_b_strategy"


def _default_direction(strategy_id: str) -> str:
    upper = strategy_id.upper()
    if "SHORT" in upper or "BEAR" in upper:
        return "SHORT"
    return "LONG"


def _instrument_from_text(value: str) -> str | None:
    upper = value.upper()
    for symbol in ("MNQ", "MGC", "GC", "PL", "NQ", "ES", "MES"):
        if symbol in upper:
            return symbol
    return None


def _non_authority_flags() -> dict[str, Any]:
    return {
        "shadow_only": True,
        "dry_run_only": True,
        "research_only": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "live_money_route_allowed": False,
        "paper_proof_allowed": False,
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--json", action="store_true", help="Print generated discovery report JSON.")
    args = parser.parse_args(argv)
    config = MissedOpportunityDiscoveryConfig(repo_root=args.repo_root)
    json_path, md_path, report = create_missed_opportunity_discovery_layer(config=config)
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"Wrote {json_path}")
        print(f"Wrote {md_path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
