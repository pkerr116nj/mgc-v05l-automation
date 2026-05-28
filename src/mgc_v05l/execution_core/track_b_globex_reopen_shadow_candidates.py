"""Read-only Track B Globex reopen first-candle shadow candidate registry.

This module registers the MNQ 18:00 ET reopen first-candle continuation idea as
a formal shadow candidate. It creates no broker authority, no lifecycle
authority, and no PAPER submit path.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES,
    position_intent_from_template,
    validate_position_intent,
)
from mgc_v05l.execution_core.track_b_strategy_hold_exit_policy_registry import (
    strategy_hold_exit_policy_for,
    validate_strategy_hold_exit_policy,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1 = (
    "GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1"
)
GLOBEX_REOPEN_SHADOW_CANDIDATES_READY = "GLOBEX_REOPEN_SHADOW_CANDIDATES_READY"
GLOBEX_REOPEN_SHADOW_CANDIDATES_GAPS_FOUND = "GLOBEX_REOPEN_SHADOW_CANDIDATES_GAPS_FOUND"
GLOBEX_REOPEN_ENTRY_CANDIDATE = "GLOBEX_REOPEN_ENTRY_CANDIDATE"
NO_FRESH_RUNTIME_OBSERVATION = "NO_FRESH_RUNTIME_OBSERVATION"
DEFAULT_REPLAY_EVIDENCE_PATH = (
    Path("outputs")
    / "reports"
    / "globex_reopen_first_candle_continuation"
    / "globex_reopen_first_candle_continuation_research.json"
)
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "latest_globex_reopen_shadow_candidates.json"
)
DEFAULT_EVENTS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_shadow"
    / "globex_reopen_shadow_candidate_events.jsonl"
)


@dataclass(frozen=True)
class GlobexReopenShadowCandidate:
    candidate_id: str
    candidate_type: str
    strategy_id: str
    lane_id: str
    classification: str
    symbol: str
    side: str
    quantity: int
    anchor_et: str
    window: str
    trigger: str
    confirmation: str
    benchmark_exit: str
    benchmark_hold_minutes: int
    candidate_role: str
    purpose: str
    entry_creation_allowed: bool = False
    shadow_only: bool = True
    no_paper_authority_yet: bool = True
    submit_allowed: bool = False
    broker_authority: bool = False
    broker_mutation_allowed: bool = False
    lifecycle_authority: bool = False
    live_money_eligible: bool = False
    paper_proof_invoked: bool = False


GLOBEX_REOPEN_SHADOW_CANDIDATES: tuple[GlobexReopenShadowCandidate, ...] = (
    GlobexReopenShadowCandidate(
        candidate_id=GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1,
        candidate_type=GLOBEX_REOPEN_ENTRY_CANDIDATE,
        strategy_id=GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1,
        lane_id="globex_reopen_mnq_1m_strong_green_second_candle_confirm_shadow",
        classification="SHADOW_CANDIDATE",
        symbol="MNQ",
        side="LONG",
        quantity=1,
        anchor_et="18:00",
        window="first completed 1m candle after the futures reopen",
        trigger="first 1m candle is strong green",
        confirmation="second candle confirms and no immediate rejection is observed",
        benchmark_exit="60M_TIMEBOX_SHADOW_BENCHMARK",
        benchmark_hold_minutes=60,
        candidate_role="standalone_entry_shadow_candidate",
        purpose=(
            "Observe future live/replay events and compare actual continuation behavior before "
            "deciding whether to promote to a broker-authoritative PAPER evidence lane."
        ),
    ),
)


def build_globex_reopen_shadow_candidate_report(
    *,
    repo_root: Path = REPO_ROOT,
    replay_evidence_path: Path = DEFAULT_REPLAY_EVIDENCE_PATH,
    runtime_observations: Sequence[Mapping[str, Any]] = (),
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    replay_payload = _read_json(_resolve(repo_root, replay_evidence_path))
    replay_evidence = _globex_replay_evidence(replay_payload)
    observations_by_id = {str(row.get("candidate_id") or ""): dict(row) for row in runtime_observations}
    rows = [
        _candidate_payload(
            candidate=candidate,
            replay_evidence=replay_evidence,
            runtime_observation=observations_by_id.get(candidate.candidate_id),
        )
        for candidate in GLOBEX_REOPEN_SHADOW_CANDIDATES
    ]
    gap_rows = [row for row in rows if row["metadata_status"] != "GLOBEX_REOPEN_SHADOW_METADATA_VALID"]
    return {
        "schema_version": "track_b_globex_reopen_shadow_candidates_v1",
        "generated_at": actual_now.isoformat(),
        "classification": GLOBEX_REOPEN_SHADOW_CANDIDATES_GAPS_FOUND if gap_rows else GLOBEX_REOPEN_SHADOW_CANDIDATES_READY,
        "repo_root": str(repo_root),
        "read_only": True,
        "shadow_only": True,
        "paper_authority_changed": False,
        "guarded_paper_runtime_authority_changed": False,
        "active_paper_config_changed": False,
        "submit_allowed": False,
        "broker_authority": False,
        "broker_mutation_allowed": False,
        "lifecycle_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "candidate_count": len(rows),
        "candidates": rows,
        "operator_visibility": {
            "surface": "track_b_globex_reopen_shadow_candidates",
            "candidate_ids": [row["candidate_id"] for row in rows],
            "shadow_candidate_ids": [row["candidate_id"] for row in rows if row["classification"] == "SHADOW_CANDIDATE"],
            "authority_note": "shadow_only_no_broker_or_lifecycle_authority",
            "observe_only_purpose": (
                "Compare fresh Globex reopen first-candle outcomes against the 60m replay benchmark before any "
                "PAPER evidence-lane promotion decision."
            ),
        },
        "future_paper_promotion_recommendation": {
            "candidate_id": GLOBEX_REOPEN_MNQ_1M_STRONG_GREEN_SECOND_CANDLE_CONFIRM_SHADOW_V1,
            "classification": "PAPER_PROMOTION_REVIEW_LATER",
            "paper_authority_promoted": False,
            "required_before_promotion": [
                "fresh shadow-runtime observations after 18:00 ET",
                "broker-backed PAPER evidence design with explicit operator approval",
                "replay/live comparison of 60m continuation behavior",
                "guarded PAPER config review that keeps submit_allowed explicitly false until approved",
            ],
        },
        "artifact_paths": {
            "replay_evidence": str(_resolve(repo_root, replay_evidence_path)),
            "latest_shadow_registry": str(_resolve(repo_root, DEFAULT_OUTPUT_PATH)),
            "event_history": str(_resolve(repo_root, DEFAULT_EVENTS_PATH)),
        },
    }


def write_globex_reopen_shadow_candidate_report(
    payload: Mapping[str, Any],
    *,
    repo_root: Path = REPO_ROOT,
    output_path: Path = DEFAULT_OUTPUT_PATH,
    events_path: Path = DEFAULT_EVENTS_PATH,
) -> tuple[Path, Path]:
    latest_path = _resolve(repo_root, output_path)
    events_full_path = _resolve(repo_root, events_path)
    write_json_atomic(latest_path, payload)
    events_full_path.parent.mkdir(parents=True, exist_ok=True)
    with events_full_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True, default=str) + "\n")
    return latest_path, events_full_path


def _candidate_payload(
    *,
    candidate: GlobexReopenShadowCandidate,
    replay_evidence: Mapping[str, Any],
    runtime_observation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    payload = asdict(candidate)
    metadata_gaps: list[str] = []
    template = APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES.get(candidate.strategy_id)
    if template is None:
        metadata_gaps.append("missing_position_intent")
        intent_payload: dict[str, Any] | None = None
        intent_validation: dict[str, Any] | None = None
    else:
        intent = position_intent_from_template(template)
        intent_payload = asdict(intent)
        intent_validation = validate_position_intent(intent)
        if not intent_validation["valid"]:
            metadata_gaps.append("invalid_position_intent")

    hold_exit_policy = strategy_hold_exit_policy_for(candidate.strategy_id)
    hold_exit_validation = validate_strategy_hold_exit_policy(hold_exit_policy)
    if not hold_exit_validation["valid"]:
        metadata_gaps.append("invalid_or_missing_hold_exit_policy")

    return {
        **payload,
        "metadata_status": "GLOBEX_REOPEN_SHADOW_METADATA_VALID" if not metadata_gaps else "GLOBEX_REOPEN_SHADOW_METADATA_GAPS",
        "metadata_gaps": metadata_gaps,
        "position_intent": intent_payload,
        "position_intent_validation": intent_validation,
        "hold_exit_policy": hold_exit_policy,
        "hold_exit_policy_validation": hold_exit_validation,
        "replay_evidence": dict(replay_evidence),
        "shadow_runtime_visibility": _runtime_visibility(runtime_observation),
        "operator_shadow_visibility": {
            "show_in_operator_shadow_panel": True,
            "display_name": "MNQ Globex reopen 1m strong green plus second-candle confirm",
            "status": candidate.classification,
            "authority_note": "observe_only_no_submit_no_lifecycle_authority",
        },
        "submit_allowed": False,
        "broker_authority": False,
        "broker_mutation_allowed": False,
        "lifecycle_authority": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _runtime_visibility(observation: Mapping[str, Any] | None) -> dict[str, Any]:
    if not observation:
        return {
            "candidate_fired": False,
            "accepted": False,
            "reject_reason": NO_FRESH_RUNTIME_OBSERVATION,
            "shadow_outcome_points": None,
            "actual_continuation_compared_to_replay": "NO_FRESH_RUNTIME_OBSERVATION",
        }
    accepted = bool(observation.get("accepted"))
    return {
        "candidate_fired": bool(observation.get("candidate_fired")),
        "accepted": accepted,
        "reject_reason": None if accepted else str(observation.get("reject_reason") or "GLOBEX_REOPEN_SHADOW_REJECTED"),
        "evidence_used": list(observation.get("evidence_used") or []),
        "evidence_gaps": list(observation.get("evidence_gaps") or []),
        "shadow_outcome_points": observation.get("shadow_outcome_points"),
        "actual_continuation_compared_to_replay": str(
            observation.get("actual_continuation_compared_to_replay") or "PENDING"
        ),
    }


def _globex_replay_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    classification = dict(payload.get("lane_classification") or {})
    best_trigger = dict(classification.get("best_mnq_trigger") or {})
    filter_summaries = payload.get("simple_filter_summaries")
    second_candle_rows = []
    if isinstance(filter_summaries, Mapping):
        second_candle_rows = [
            dict(row)
            for row in filter_summaries.get("second_candle_confirmation", [])
            if isinstance(row, Mapping) and row.get("timeframe") == "1m" and row.get("hold_minutes") == 60
        ]
    return {
        "source_schema_version": payload.get("schema_version"),
        "source_mode": payload.get("mode"),
        "source_artifact_classification": classification.get("classification"),
        "source_authority_status": classification.get("authority_status"),
        "paper_authority_promoted": bool(classification.get("paper_authority_promoted", False)),
        "best_mnq_trigger": best_trigger,
        "second_candle_confirmation_60m_rows": second_candle_rows,
        "artifact_paths": dict(payload.get("artifact_paths") or {}),
        "evidence_linkage_status": "LINKED" if best_trigger else "MISSING_OR_NOT_YET_GENERATED",
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else Path(repo_root) / path
