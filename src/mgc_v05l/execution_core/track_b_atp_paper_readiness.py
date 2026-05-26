"""Track B ATP/trend participation PAPER readiness diagnostics.

This module ranks ATP/trend participation shadows for future guarded PAPER
promotion. It is analysis-only: it does not alter strategy rules, grant broker
authority, create lifecycle authority, or mutate runtime/broker state.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime
from .track_b_atomic_io import write_json_atomic
from .track_b_exit_strategy_roster import (
    PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
    resolve_track_b_exit_profile_for_position,
)


DEFAULT_ATP_SHADOW = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_atp_trend_participation_shadow.json"
)
DEFAULT_FORWARD_OUTCOMES = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_missed_opportunity_forward_outcomes.json"
)
DEFAULT_OUTPUT_JSON = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_atp_paper_readiness_report.json"
)
DEFAULT_OUTPUT_MD = Path("docs") / "track_b_atp_paper_readiness_report.md"
DEFAULT_LIFECYCLE_MAPPING_LATEST = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_atp_lifecycle_mapping_shadow.json"
)
DEFAULT_LIFECYCLE_MAPPING_EVENTS = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "atp_lifecycle_mapping_shadow_events.jsonl"
)

ATP_READINESS_READY = "ATP_PAPER_READINESS_READY"
SHADOW_ONLY = "SHADOW_ONLY"
PAPER_CANDIDATE_NEEDS_EXIT_PROFILE = "PAPER_CANDIDATE_NEEDS_EXIT_PROFILE"
PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING = "PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING"
PAPER_READY_AFTER_PRECHECK = "PAPER_READY_AFTER_PRECHECK"
REJECT_OR_DEFER = "REJECT_OR_DEFER"

ATP_LIFECYCLE_MAPPING_SHADOW_READY = "ATP_LIFECYCLE_MAPPING_SHADOW_READY"
ATP_LIFECYCLE_MAPPING_SHADOW_EMPTY = "ATP_LIFECYCLE_MAPPING_SHADOW_EMPTY"
ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY = "ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY"
ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE = "ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE"
ATP_LIFECYCLE_MAPPING_NEEDS_LIFECYCLE_MAPPING = "ATP_LIFECYCLE_MAPPING_NEEDS_LIFECYCLE_MAPPING"
ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE = "ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE"


@dataclass(frozen=True)
class AtpPaperReadinessConfig:
    repo_root: Path = Path(".")
    atp_shadow_json: Path = DEFAULT_ATP_SHADOW
    forward_outcomes_json: Path = DEFAULT_FORWARD_OUTCOMES
    output_json: Path = DEFAULT_OUTPUT_JSON
    output_md: Path = DEFAULT_OUTPUT_MD
    lifecycle_mapping_latest_json: Path = DEFAULT_LIFECYCLE_MAPPING_LATEST
    lifecycle_mapping_events_jsonl: Path = DEFAULT_LIFECYCLE_MAPPING_EVENTS
    min_forward_sample_size: int = 30
    min_missed_winner_rate: float = 0.55
    max_avoided_loser_rate: float = 0.25
    min_positive_expectancy: float = 0.0
    max_average_mae: float = -8.0


def create_atp_paper_readiness_report(
    *,
    config: AtpPaperReadinessConfig | None = None,
    now: datetime | None = None,
) -> tuple[Path, Path, dict[str, Any]]:
    actual_config = config or AtpPaperReadinessConfig()
    report = build_atp_paper_readiness_report(config=actual_config, now=now)
    repo_root = Path(actual_config.repo_root)
    json_path = _resolve(repo_root, actual_config.output_json)
    md_path = _resolve(repo_root, actual_config.output_md)
    mapping_latest = _resolve(repo_root, actual_config.lifecycle_mapping_latest_json)
    mapping_events = _resolve(repo_root, actual_config.lifecycle_mapping_events_jsonl)
    write_json_atomic(json_path, report)
    mapping_shadow = report.get("atp_lifecycle_mapping_shadow")
    if isinstance(mapping_shadow, Mapping):
        write_json_atomic(mapping_latest, dict(mapping_shadow))
        _append_jsonl(mapping_events, _event_projection(mapping_shadow))
    md_path.parent.mkdir(parents=True, exist_ok=True)
    md_path.write_text(_render_markdown(report), encoding="utf-8")
    return json_path, md_path, report


def build_atp_paper_readiness_report(
    *,
    config: AtpPaperReadinessConfig | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_config = config or AtpPaperReadinessConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    repo_root = Path(actual_config.repo_root)
    atp_path = _resolve(repo_root, actual_config.atp_shadow_json)
    forward_path = _resolve(repo_root, actual_config.forward_outcomes_json)
    atp_shadow = _load_json(atp_path)
    forward_outcomes = _load_json(forward_path)
    outcomes_by_candidate = _atp_forward_outcomes_by_candidate(forward_outcomes)
    lifecycle_mapping_shadow = build_atp_lifecycle_mapping_shadow(atp_shadow=atp_shadow, now=actual_now)

    rows = [
        _candidate_readiness_row(candidate, outcomes_by_candidate, config=actual_config)
        for candidate in _as_list(atp_shadow.get("candidates"))
        if isinstance(candidate, Mapping)
    ]
    rows.sort(key=_candidate_sort_key)
    classifications = Counter(str(row.get("readiness_classification")) for row in rows)
    instrument_counts = Counter(str(row.get("instrument") or "UNKNOWN") for row in rows)
    first_candidate = _recommended_first_candidate(rows)
    return {
        "schema_version": "track_b_atp_paper_readiness_v1",
        "generated_at": actual_now.isoformat(),
        "classification": ATP_READINESS_READY,
        "mode": "PAPER_SHADOW_READINESS_ONLY",
        "candidate_count": len(rows),
        "classification_counts": dict(classifications),
        "instrument_counts": dict(instrument_counts),
        "source_artifact_paths": {
            "atp_shadow": str(atp_path),
            "missed_opportunity_forward_outcomes": str(forward_path),
            "atp_lifecycle_mapping_shadow": str(_resolve(repo_root, actual_config.lifecycle_mapping_latest_json)),
        },
        "atp_lifecycle_mapping_shadow_summary": _lifecycle_mapping_summary(lifecycle_mapping_shadow),
        "atp_lifecycle_mapping_shadow": lifecycle_mapping_shadow,
        "promotion_gates": _promotion_gate_policy(actual_config),
        "ranked_candidates": rows,
        "recommended_first_atp_candidate_for_guarded_paper": first_candidate,
        "missing_implementation_gaps": _missing_gap_summary(rows),
        "exact_next_implementation_slice": _next_slice(rows),
        **_non_authority_flags(),
    }


def build_atp_lifecycle_mapping_shadow(
    *,
    atp_shadow: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    rows = [
        _candidate_lifecycle_mapping_row(candidate)
        for candidate in _as_list(atp_shadow.get("candidates"))
        if isinstance(candidate, Mapping)
    ]
    rows.sort(key=_lifecycle_mapping_sort_key)
    status_counts = Counter(str(row.get("lifecycle_mapping_status")) for row in rows)
    mapped = [row for row in rows if row.get("lifecycle_mapping_status") == ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY]
    return {
        "schema_version": "track_b_atp_lifecycle_mapping_shadow_v1",
        "generated_at": actual_now.isoformat(),
        "classification": ATP_LIFECYCLE_MAPPING_SHADOW_READY if rows else ATP_LIFECYCLE_MAPPING_SHADOW_EMPTY,
        "mode": "ATP_SHADOW_LIFECYCLE_MAPPING_ONLY",
        "candidate_count": len(rows),
        "mapped_shadow_only_count": len(mapped),
        "status_counts": dict(status_counts),
        "mapped_candidates": rows,
        "recommended_first_structural_candidate": dict(mapped[0]) if mapped else None,
        **_non_authority_flags(),
    }


def _candidate_lifecycle_mapping_row(candidate: Mapping[str, Any]) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or candidate.get("lane_id") or "")
    strategy_id = str(candidate.get("strategy_id") or candidate.get("standalone_strategy_id") or "")
    instrument = str(candidate.get("instrument") or "").upper()
    direction = str(candidate.get("direction") or candidate.get("direction_hint") or "").upper()
    exit_profile = _exit_profile_for_instrument(instrument)
    is_paper_shadow = _is_shadow_candidate(candidate) and bool(candidate.get("paper_only")) is True
    status = _lifecycle_mapping_status(candidate=candidate, exit_profile=exit_profile, is_paper_shadow=is_paper_shadow)
    return {
        "atp_candidate_id": candidate_id,
        "candidate_id": candidate_id,
        "strategy_id": strategy_id,
        "strategy_family": candidate.get("strategy_family") or "active_trend_participation_engine",
        "instrument": instrument,
        "contract_family": _contract_family_for_instrument(instrument),
        "direction": direction,
        "session_regime": candidate.get("session_regime"),
        "intended_exit_policy": PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1 if exit_profile else None,
        "mapped_exit_profile_id": exit_profile.get("exit_profile_id") if exit_profile else None,
        "managed_exit_policy_id": exit_profile.get("managed_exit_policy_id") if exit_profile else None,
        "lifecycle_mapping_id": _shadow_lifecycle_mapping_id(candidate_id=candidate_id, strategy_id=strategy_id)
        if status == ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY
        else None,
        "lifecycle_mapping_status": status,
        "missing_authority_reason": _mapping_missing_reason(status, candidate),
        "why_not_live_authority": "ATP lifecycle mapping is shadow metadata only; forward evidence, operator approval, guarded roster inclusion, and lifecycle authority remain absent.",
        "lifecycle_authority": False,
        "source_shadow_classification": candidate.get("shadow_classification"),
        "score": candidate.get("score"),
        "confidence": candidate.get("confidence"),
        "source_authority_path": candidate.get("source_authority_path"),
        "source_candle_timestamp": candidate.get("source_candle_timestamp"),
        **_non_authority_flags(),
    }


def _candidate_readiness_row(
    candidate: Mapping[str, Any],
    outcomes_by_candidate: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    config: AtpPaperReadinessConfig,
) -> dict[str, Any]:
    candidate_id = str(candidate.get("candidate_id") or candidate.get("lane_id") or "")
    strategy_id = str(candidate.get("strategy_id") or candidate.get("standalone_strategy_id") or "")
    instrument = str(candidate.get("instrument") or "").upper()
    direction = str(candidate.get("direction") or candidate.get("direction_hint") or "").upper()
    outcome_rows = list(outcomes_by_candidate.get(candidate_id) or [])
    outcome_counts = Counter(str(row.get("outcome_classification") or "UNCLEAR") for row in outcome_rows)
    valid_outcomes = [
        row
        for row in outcome_rows
        if str(row.get("outcome_classification") or "") in {"MISSED_WINNER", "GOOD_REJECT", "AVOIDED_LOSER"}
    ]
    mfe_values = [_float(row.get("mfe")) for row in valid_outcomes]
    mae_values = [_float(row.get("mae")) for row in valid_outcomes]
    net_values = [_float(row.get("net_movement")) for row in valid_outcomes]
    avg_mfe = _avg(mfe_values)
    avg_mae = _avg(mae_values)
    avg_net = _avg(net_values)
    valid_count = len(valid_outcomes)
    missed_winner_rate = outcome_counts.get("MISSED_WINNER", 0) / valid_count if valid_count else 0.0
    avoided_loser_rate = outcome_counts.get("AVOIDED_LOSER", 0) / valid_count if valid_count else 0.0
    exit_profile = _exit_profile_for_instrument(instrument)
    exit_profile_exists = exit_profile is not None
    lifecycle_mapping_exists = _lifecycle_mapping_exists(candidate, exit_profile_exists=exit_profile_exists)
    order_management_exists = exit_profile_exists and lifecycle_mapping_exists
    evidence_ready = (
        valid_count >= config.min_forward_sample_size
        and missed_winner_rate >= config.min_missed_winner_rate
        and avoided_loser_rate <= config.max_avoided_loser_rate
        and avg_net > config.min_positive_expectancy
        and (avg_mae >= config.max_average_mae if valid_count else False)
    )
    structural_ready = (
        _is_shadow_candidate(candidate)
        and bool(candidate.get("paper_only")) is True
        and exit_profile_exists
        and lifecycle_mapping_exists
        and order_management_exists
    )
    missing_gates = _missing_gates(
        candidate=candidate,
        exit_profile_exists=exit_profile_exists,
        lifecycle_mapping_exists=lifecycle_mapping_exists,
        order_management_exists=order_management_exists,
        evidence_ready=evidence_ready,
        valid_count=valid_count,
        config=config,
    )
    readiness = _readiness_classification(
        candidate=candidate,
        exit_profile_exists=exit_profile_exists,
        lifecycle_mapping_exists=lifecycle_mapping_exists,
        structural_ready=structural_ready,
        evidence_ready=evidence_ready,
    )
    return {
        "candidate_id": candidate_id,
        "strategy_id": strategy_id,
        "instrument": instrument,
        "session": candidate.get("session_regime"),
        "direction": direction,
        "regime_assumptions": {
            "strategy_family": candidate.get("strategy_family"),
            "quality_bucket_policy": candidate.get("quality_bucket_policy"),
            "implementation_state": candidate.get("implementation_state"),
            "experimental_status": candidate.get("experimental_status"),
        },
        "data_dependencies": {
            "source_authority_path": candidate.get("source_authority_path"),
            "source_candle_timestamp": candidate.get("source_candle_timestamp"),
            "phase1_runtime_market_data_required": True,
        },
        "current_shadow": {
            "shadow_classification": candidate.get("shadow_classification"),
            "score": candidate.get("score"),
            "confidence": candidate.get("confidence"),
            "failed_gates": _as_list(candidate.get("failed_gates")),
        },
        "forward_outcome_tracking": {
            "tracked_candidate_count": len(outcome_rows),
            "valid_forward_outcome_count": valid_count,
            "outcome_counts": dict(outcome_counts),
            "average_mfe": round(avg_mfe, 6),
            "average_mae": round(avg_mae, 6),
            "average_net_movement": round(avg_net, 6),
            "missed_winner_rate": round(missed_winner_rate, 6),
            "avoided_loser_rate": round(avoided_loser_rate, 6),
            "windows": ["5m", "15m", "30m", "60m"],
        },
        "structural_gates": {
            "paper_only_config": bool(candidate.get("paper_only")) is True,
            "non_approved_research_candidate": bool(candidate.get("non_approved")) is True,
            "exit_profile_exists": exit_profile_exists,
            "exit_profile_id": exit_profile.get("exit_profile_id") if exit_profile else None,
            "managed_exit_policy_id": exit_profile.get("managed_exit_policy_id") if exit_profile else None,
            "lifecycle_mapping_exists": lifecycle_mapping_exists,
            "order_management_path_exists": order_management_exists,
            "guardian_control_plane_safe_state_compatible": True,
        },
        "promotion_gate_status": {
            "structural_ready": structural_ready,
            "evidence_ready": evidence_ready,
            "broker_authority_allowed": False,
        },
        "missing_gates": missing_gates,
        "readiness_classification": readiness,
        "recommended_action": _candidate_recommendation(readiness, missing_gates),
        **_non_authority_flags(),
    }


def _atp_forward_outcomes_by_candidate(payload: Mapping[str, Any]) -> dict[str, list[Mapping[str, Any]]]:
    rows: dict[str, list[Mapping[str, Any]]] = {}
    for row in _as_list(payload.get("outcomes")):
        if not isinstance(row, Mapping) or row.get("source") != "ATP_TREND_PARTICIPATION_SHADOW":
            continue
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            continue
        rows.setdefault(candidate_id, []).append(row)
    return rows


def _exit_profile_for_instrument(instrument: str) -> dict[str, Any] | None:
    try:
        profile = resolve_track_b_exit_profile_for_position(
            instrument_family=instrument,
            managed_exit_policy_id=PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1,
        )
    except ValueError:
        return None
    return profile.to_json_dict()


def _lifecycle_mapping_exists(candidate: Mapping[str, Any], *, exit_profile_exists: bool) -> bool:
    if not exit_profile_exists:
        return False
    strategy_id = str(candidate.get("strategy_id") or "")
    # ATP has shadow diagnostics today, but no registered Track B lifecycle
    # ownership mapping. Keeping this explicit prevents accidental authority.
    return bool(candidate.get("track_b_lifecycle_mapping_id")) and strategy_id.startswith("atp_companion")


def _missing_gates(
    *,
    candidate: Mapping[str, Any],
    exit_profile_exists: bool,
    lifecycle_mapping_exists: bool,
    order_management_exists: bool,
    evidence_ready: bool,
    valid_count: int,
    config: AtpPaperReadinessConfig,
) -> list[str]:
    missing = []
    if not _is_shadow_candidate(candidate):
        missing.append("shadow_candidate_not_currently_ready")
    if bool(candidate.get("paper_only")) is not True:
        missing.append("paper_only_candidate_config_required")
    if not exit_profile_exists:
        missing.append("managed_exit_profile_required")
    if not lifecycle_mapping_exists:
        missing.append("track_b_lifecycle_mapping_required")
    if not order_management_exists:
        missing.append("managed_order_management_path_required")
    if not evidence_ready:
        missing.append(f"forward_outcome_sample_size_below_{config.min_forward_sample_size}")
        if valid_count <= 0:
            missing.append("timestamp_locked_forward_outcomes_missing_or_incomplete")
    return missing


def _readiness_classification(
    *,
    candidate: Mapping[str, Any],
    exit_profile_exists: bool,
    lifecycle_mapping_exists: bool,
    structural_ready: bool,
    evidence_ready: bool,
) -> str:
    if not _is_shadow_candidate(candidate):
        return REJECT_OR_DEFER if candidate.get("shadow_classification") == "ATP_SHADOW_NO_CANDIDATE" else SHADOW_ONLY
    if bool(candidate.get("paper_only")) is not True:
        return SHADOW_ONLY
    if not exit_profile_exists:
        return PAPER_CANDIDATE_NEEDS_EXIT_PROFILE
    if not lifecycle_mapping_exists:
        return PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING
    if structural_ready and evidence_ready:
        return PAPER_READY_AFTER_PRECHECK
    return SHADOW_ONLY


def _candidate_recommendation(classification: str, missing_gates: Sequence[str]) -> str:
    if classification == PAPER_READY_AFTER_PRECHECK:
        return "Prepare guarded PAPER candidate spec and require explicit operator approval before roster inclusion."
    if classification == PAPER_CANDIDATE_NEEDS_EXIT_PROFILE:
        return "Add generic ATP-compatible exit profile mapping before lifecycle or broker authority."
    if classification == PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING:
        return "Add Track B managed lifecycle ownership mapping for ATP candidate family, then continue shadow evidence."
    if "timestamp_locked_forward_outcomes_missing_or_incomplete" in missing_gates:
        return "Keep shadow-only and collect timestamp-locked forward evidence."
    return "Keep shadow-only until confidence, structural gates, and forward evidence improve."


def _lifecycle_mapping_status(
    *,
    candidate: Mapping[str, Any],
    exit_profile: Mapping[str, Any] | None,
    is_paper_shadow: bool,
) -> str:
    if not _is_shadow_candidate(candidate):
        return ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE
    if bool(candidate.get("paper_only")) is not True:
        return ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE
    if exit_profile is None:
        return ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE
    if not _shadow_lifecycle_mapping_id(
        candidate_id=str(candidate.get("candidate_id") or candidate.get("lane_id") or ""),
        strategy_id=str(candidate.get("strategy_id") or candidate.get("standalone_strategy_id") or ""),
    ):
        return ATP_LIFECYCLE_MAPPING_NEEDS_LIFECYCLE_MAPPING
    return ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY if is_paper_shadow else ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE


def _contract_family_for_instrument(instrument: str) -> str | None:
    if instrument in {"MGC", "GC"}:
        return "GOLD"
    if instrument in {"MNQ", "NQ", "MES", "ES", "YM"}:
        return "EQUITY_INDEX"
    if instrument:
        return instrument
    return None


def _shadow_lifecycle_mapping_id(*, candidate_id: str, strategy_id: str) -> str | None:
    basis = candidate_id or strategy_id
    if not basis:
        return None
    return f"atp_shadow_lifecycle_mapping__{_slug(basis)}"


def _mapping_missing_reason(status: str, candidate: Mapping[str, Any]) -> str | None:
    if status == ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY:
        return None
    if status == ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE:
        return "No ATP-compatible managed exit profile is mapped for this instrument."
    if status == ATP_LIFECYCLE_MAPPING_NEEDS_LIFECYCLE_MAPPING:
        return "ATP candidate identity is incomplete for shadow lifecycle mapping."
    if bool(candidate.get("paper_only")) is not True:
        return "Candidate is not configured as PAPER-only and remains non-actionable."
    return "Candidate did not produce a current actionable ATP shadow signal."


def _lifecycle_mapping_sort_key(row: Mapping[str, Any]) -> tuple[int, float, str]:
    order = {
        ATP_LIFECYCLE_MAPPING_MAPPED_SHADOW_ONLY: 0,
        ATP_LIFECYCLE_MAPPING_NEEDS_LIFECYCLE_MAPPING: 1,
        ATP_LIFECYCLE_MAPPING_NEEDS_EXIT_PROFILE: 2,
        ATP_LIFECYCLE_MAPPING_NON_ACTIONABLE: 3,
    }
    return (
        order.get(str(row.get("lifecycle_mapping_status")), 9),
        -(_float(row.get("score")) or 0.0),
        str(row.get("atp_candidate_id") or ""),
    )


def _lifecycle_mapping_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": payload.get("classification"),
        "candidate_count": payload.get("candidate_count"),
        "mapped_shadow_only_count": payload.get("mapped_shadow_only_count"),
        "status_counts": payload.get("status_counts"),
        "lifecycle_authority": False,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
    }


def _recommended_first_candidate(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    for row in rows:
        if row.get("readiness_classification") == PAPER_READY_AFTER_PRECHECK:
            return dict(row)
    for row in rows:
        if row.get("readiness_classification") == PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING:
            return dict(row)
    for row in rows:
        if row.get("readiness_classification") == PAPER_CANDIDATE_NEEDS_EXIT_PROFILE:
            return dict(row)
    return dict(rows[0]) if rows else None


def _missing_gap_summary(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[str] = Counter()
    for row in rows:
        for gate in _as_list(row.get("missing_gates")):
            counts[str(gate)] += 1
    return [{"missing_gate": gate, "candidate_count": count} for gate, count in counts.most_common()]


def _next_slice(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if any(row.get("readiness_classification") == PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING for row in rows):
        return {
            "classification": "IMPLEMENT_ATP_LIFECYCLE_MAPPING_SHADOW_FIRST",
            "scope": "Define generic ATP Track B lifecycle ownership metadata and map MGC/MNQ-compatible ATP candidates to existing diagnostic time-boxed exit profiles, still without broker authority.",
        }
    if any(row.get("readiness_classification") == PAPER_CANDIDATE_NEEDS_EXIT_PROFILE for row in rows):
        return {
            "classification": "ADD_ATP_EXIT_PROFILE_MAPPING",
            "scope": "Create reusable ATP exit profiles for instruments lacking managed exit coverage, then rerun readiness.",
        }
    return {
        "classification": "COLLECT_ATP_FORWARD_EVIDENCE",
        "scope": "Continue shadow collection until timestamp-locked forward samples clear promotion gates.",
    }


def _promotion_gate_policy(config: AtpPaperReadinessConfig) -> dict[str, Any]:
    return {
        "minimum_forward_sample_size": config.min_forward_sample_size,
        "minimum_missed_winner_rate": config.min_missed_winner_rate,
        "maximum_avoided_loser_rate": config.max_avoided_loser_rate,
        "minimum_positive_expectancy": config.min_positive_expectancy,
        "maximum_average_mae": config.max_average_mae,
        "requires_exit_profile": True,
        "requires_lifecycle_mapping": True,
        "requires_order_management_path": True,
        "requires_guardian_control_plane_safe_state_compatibility": True,
    }


def _candidate_sort_key(row: Mapping[str, Any]) -> tuple[int, float, str]:
    order = {
        PAPER_READY_AFTER_PRECHECK: 0,
        PAPER_CANDIDATE_NEEDS_LIFECYCLE_MAPPING: 1,
        PAPER_CANDIDATE_NEEDS_EXIT_PROFILE: 2,
        SHADOW_ONLY: 3,
        REJECT_OR_DEFER: 4,
    }
    score = _float((row.get("current_shadow") or {}).get("score") if isinstance(row.get("current_shadow"), Mapping) else None)
    return (order.get(str(row.get("readiness_classification")), 9), -(score or 0.0), str(row.get("candidate_id") or ""))


def _is_shadow_candidate(candidate: Mapping[str, Any]) -> bool:
    return str(candidate.get("shadow_classification") or "") in {
        "ATP_SHADOW_CANDIDATE_LIVE_STRATEGIES_SILENT",
        "ATP_SHADOW_CONFIRMS_LIVE_SIGNAL",
    }


def _render_markdown(report: Mapping[str, Any]) -> str:
    mapping_summary = (
        report.get("atp_lifecycle_mapping_shadow_summary")
        if isinstance(report.get("atp_lifecycle_mapping_shadow_summary"), Mapping)
        else {}
    )
    lines = [
        "# Track B ATP PAPER Readiness Report",
        "",
        f"Generated: `{report.get('generated_at')}`",
        "",
        "Research/shadow only. No broker authority, lifecycle authority, live-money route, or paper_proof path is created.",
        "",
        "## Summary",
        "",
        f"- Classification: `{report.get('classification')}`",
        f"- Candidate count: `{report.get('candidate_count')}`",
        f"- Readiness counts: `{report.get('classification_counts')}`",
        f"- Instrument counts: `{report.get('instrument_counts')}`",
        f"- Lifecycle mapping shadow: `{mapping_summary}`",
        "",
        "## Ranked Candidates",
        "",
        "| Rank | Candidate | Instrument | Direction | Readiness | Score | Outcomes | Missing Gates |",
        "| ---: | --- | --- | --- | --- | ---: | --- | --- |",
    ]
    for index, row in enumerate(_as_list(report.get("ranked_candidates")), start=1):
        if not isinstance(row, Mapping):
            continue
        shadow = row.get("current_shadow") if isinstance(row.get("current_shadow"), Mapping) else {}
        outcomes = row.get("forward_outcome_tracking") if isinstance(row.get("forward_outcome_tracking"), Mapping) else {}
        lines.append(
            f"| {index} | `{row.get('candidate_id')}` | `{row.get('instrument')}` | `{row.get('direction')}` | `{row.get('readiness_classification')}` | {shadow.get('score')} | `{outcomes.get('outcome_counts')}` | `{row.get('missing_gates')}` |"
        )
    first = report.get("recommended_first_atp_candidate_for_guarded_paper")
    lines.extend(["", "## Recommendation", ""])
    if isinstance(first, Mapping):
        lines.append(
            f"- First candidate to work next: `{first.get('candidate_id')}` with `{first.get('readiness_classification')}`."
        )
        lines.append(f"- Recommended action: {first.get('recommended_action')}")
    else:
        lines.append("- No ATP candidate is ready for guarded PAPER preparation.")
    lines.extend(
        [
            "",
            "## Next Slice",
            "",
            f"- `{(report.get('exact_next_implementation_slice') or {}).get('classification') if isinstance(report.get('exact_next_implementation_slice'), Mapping) else None}`",
            f"- {(report.get('exact_next_implementation_slice') or {}).get('scope') if isinstance(report.get('exact_next_implementation_slice'), Mapping) else None}",
            "",
        ]
    )
    return "\n".join(lines)


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _event_projection(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at"),
        "classification": payload.get("classification"),
        "candidate_count": payload.get("candidate_count"),
        "mapped_shadow_only_count": payload.get("mapped_shadow_only_count"),
        "status_counts": payload.get("status_counts"),
        **_non_authority_flags(),
    }


def _resolve(root: Path, path: Path) -> Path:
    return path if path.is_absolute() else root / path


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _avg(values: Sequence[float | None]) -> float:
    numeric = [value for value in values if value is not None]
    return sum(numeric) / len(numeric) if numeric else 0.0


def _slug(value: str) -> str:
    return "".join(char if char.isalnum() else "_" for char in value.lower()).strip("_")


def _non_authority_flags() -> dict[str, bool]:
    return {
        "analysis_only": True,
        "dry_run_only": True,
        "research_only": True,
        "shadow_only": True,
        "not_order_authority": True,
        "not_lifecycle_authority": True,
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "live_money_route_allowed": False,
        "paper_proof_allowed": False,
        "dashboard_projection_consumed": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--json", action="store_true", help="Print generated readiness JSON.")
    args = parser.parse_args(argv)
    _, _, report = create_atp_paper_readiness_report(config=AtpPaperReadinessConfig(repo_root=args.repo_root))
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
