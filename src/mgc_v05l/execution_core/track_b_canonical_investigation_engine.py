"""Canonical Investigation Engine model for Track B research.

Investigations are durable diagnostic research workspaces. They capture a
hypothesis, references, and a deterministic event timeline without executing
notebooks, invoking AI, touching runtime state, or creating recommendations.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "investigation_engine"

INVESTIGATION_SCHEMA_VERSION = "ie1_canonical_investigation_v1"
SUMMARY_SCHEMA_VERSION = "ie1_canonical_investigation_summary_v1"
TIMELINE_EVENT_SCHEMA_VERSION = "ie1_investigation_timeline_event_v1"

CONTRACT_MD = "ie1_investigation_contract.md"
SCHEMA_JSON = "ie1_investigation_schema.json"
SAMPLE_INVESTIGATION_JSON = "ie1_sample_investigation.json"
SUMMARY_MD = "ie1_investigation_summary.md"
TIMELINE_CONTRACT_MD = "ie1_timeline_contract.md"

VALID_STATUSES = {"OPEN", "ACTIVE", "PAUSED", "COMPLETE", "ARCHIVED"}
VALID_REFERENCE_TYPES = {
    "evidence",
    "saved_query",
    "execution_record",
    "diff_result",
    "insight",
    "morning_brief",
    "research_discovery_candidate",
    "candidate_review",
    "context_snapshot",
    "artifact",
    "bookmark",
}
VALID_EVENT_TYPES = {
    "INVESTIGATION_CREATED",
    "EVIDENCE_ATTACHED",
    "EVIDENCE_SUPERSEDED",
    "EVIDENCE_ARCHIVED",
    "EVIDENCE_INVALIDATED",
    "QUERY_EXECUTED",
    "INSIGHT_ATTACHED",
    "DIFF_ATTACHED",
    "MORNING_BRIEF_REFERENCED",
    "CANDIDATE_BOOKMARKED",
    "ARTIFACT_ATTACHED",
    "INVESTIGATION_COMPLETED",
}


@dataclass(frozen=True)
class CanonicalInvestigationResult:
    investigation: dict[str, Any]
    investigation_path: Path
    summary: dict[str, Any]
    summary_path: Path
    output_dir: Path


def create_investigation(
    *,
    title: str,
    description: str,
    hypothesis: str,
    owner: str = "operator",
    tags: Sequence[str] | None = None,
    investigation_id: str | None = None,
    status: str = "OPEN",
    now: datetime | str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> CanonicalInvestigationResult:
    generated_at = _coerce_now(now)
    status = _validate_status(status)
    tags_list = sorted(set(tags or []))
    inv_id = investigation_id or _investigation_id(title, hypothesis, generated_at)
    investigation = {
        "schema_version": INVESTIGATION_SCHEMA_VERSION,
        "investigation_id": inv_id,
        "title": title,
        "description": description,
        "hypothesis": hypothesis,
        "created_at": generated_at.isoformat(),
        "updated_at": generated_at.isoformat(),
        "status": status,
        "owner": owner,
        "tags": tags_list,
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "references": [],
        "timeline": [
            _timeline_event(
                event_type="INVESTIGATION_CREATED",
                timestamp=generated_at,
                artifact_reference={"reference_type": "artifact", "reference_id": inv_id},
                provenance={"source": "canonical_investigation_engine", "operation": "create"},
            )
        ],
    }
    return write_investigation(investigation, output_dir=output_dir)


def attach_reference(
    investigation: Mapping[str, Any],
    *,
    reference_type: str,
    reference_id: str,
    artifact_path: str | None = None,
    label: str | None = None,
    event_type: str | None = None,
    provenance: Mapping[str, Any] | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    _validate_reference_type(reference_type)
    timestamp = _coerce_now(now)
    updated = dict(investigation)
    references = [dict(item) for item in updated.get("references") or []]
    reference = {
        "reference_type": reference_type,
        "reference_id": reference_id,
        "artifact_path": artifact_path,
        "label": label,
        "attached_at": timestamp.isoformat(),
    }
    references.append(reference)
    updated["references"] = sorted(references, key=lambda row: (str(row.get("reference_type")), str(row.get("reference_id")), str(row.get("attached_at"))))
    updated["updated_at"] = timestamp.isoformat()
    timeline = [dict(item) for item in updated.get("timeline") or []]
    timeline.append(
        _timeline_event(
            event_type=event_type or _event_type_for_reference(reference_type),
            timestamp=timestamp,
            artifact_reference=reference,
            provenance=dict(provenance or {"source": "canonical_investigation_engine", "operation": "attach_reference"}),
        )
    )
    updated["timeline"] = _sort_timeline(timeline)
    return updated


def complete_investigation(
    investigation: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    updated = dict(investigation)
    updated["status"] = "COMPLETE"
    updated["updated_at"] = timestamp.isoformat()
    timeline = [dict(item) for item in updated.get("timeline") or []]
    timeline.append(
        _timeline_event(
            event_type="INVESTIGATION_COMPLETED",
            timestamp=timestamp,
            artifact_reference={"reference_type": "artifact", "reference_id": updated.get("investigation_id")},
            provenance=dict(provenance or {"source": "canonical_investigation_engine", "operation": "complete"}),
        )
    )
    updated["timeline"] = _sort_timeline(timeline)
    return updated


def attach_evidence_reference(
    investigation: Mapping[str, Any],
    *,
    evidence_id: str,
    artifact_path: str | None = None,
    status_event: str = "EVIDENCE_ATTACHED",
    label: str | None = None,
    provenance: Mapping[str, Any] | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    if status_event not in {"EVIDENCE_ATTACHED", "EVIDENCE_SUPERSEDED", "EVIDENCE_ARCHIVED", "EVIDENCE_INVALIDATED"}:
        raise ValueError(f"Unsupported evidence timeline event: {status_event}")
    return attach_reference(
        investigation,
        reference_type="evidence",
        reference_id=evidence_id,
        artifact_path=artifact_path,
        label=label,
        event_type=status_event,
        provenance=provenance or {"source": "canonical_investigation_engine", "operation": "attach_evidence"},
        now=now,
    )


def write_investigation(investigation: Mapping[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> CanonicalInvestigationResult:
    validate_investigation(investigation)
    output_dir.mkdir(parents=True, exist_ok=True)
    inv_id = str(investigation["investigation_id"])
    investigation_path = output_dir / f"{inv_id}.json"
    payload = dict(investigation)
    investigation_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = summarize_investigation(payload)
    summary_path = output_dir / f"{inv_id}.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CanonicalInvestigationResult(
        investigation=payload,
        investigation_path=investigation_path,
        summary=summary,
        summary_path=summary_path,
        output_dir=output_dir,
    )


def load_investigation(investigation_id: str, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    path = output_dir / f"{investigation_id}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def list_investigations(*, output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[dict[str, Any]]:
    rows = []
    if not output_dir.exists():
        return rows
    for path in sorted(output_dir.glob("*.json")):
        if path.name.endswith(".summary.json") or path.name.startswith("ie1_"):
            continue
        try:
            investigation = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if investigation.get("schema_version") == INVESTIGATION_SCHEMA_VERSION:
            rows.append(summarize_investigation(investigation))
    return rows


def validate_investigation(investigation: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if investigation.get("schema_version") != INVESTIGATION_SCHEMA_VERSION:
        errors.append("invalid_schema_version")
    for key in ("investigation_id", "title", "description", "hypothesis", "created_at", "updated_at", "status", "owner"):
        if not investigation.get(key):
            errors.append(f"missing_{key}")
    if investigation.get("status") not in VALID_STATUSES:
        errors.append("invalid_status")
    if investigation.get("diagnostic_only") is not True:
        errors.append("diagnostic_only_not_true")
    if investigation.get("production_recommendation") is not False:
        errors.append("production_recommendation_not_false")
    if investigation.get("trading_gate") is not False:
        errors.append("trading_gate_not_false")
    for ref in investigation.get("references") or []:
        if ref.get("reference_type") not in VALID_REFERENCE_TYPES:
            errors.append(f"invalid_reference_type:{ref.get('reference_type')}")
    timeline = investigation.get("timeline") or []
    for event in timeline:
        if event.get("event_type") not in VALID_EVENT_TYPES:
            errors.append(f"invalid_event_type:{event.get('event_type')}")
    timestamps = [str(event.get("timestamp")) for event in timeline]
    if timestamps != sorted(timestamps):
        errors.append("timeline_not_sorted")
    if errors:
        raise ValueError(";".join(errors))
    return {"valid": True, "errors": []}


def summarize_investigation(investigation: Mapping[str, Any]) -> dict[str, Any]:
    references = list(investigation.get("references") or [])
    timeline = list(investigation.get("timeline") or [])
    by_type: dict[str, list[dict[str, Any]]] = {key: [] for key in VALID_REFERENCE_TYPES}
    for reference in references:
        by_type.setdefault(str(reference.get("reference_type")), []).append(dict(reference))
    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "investigation_id": investigation.get("investigation_id"),
        "title": investigation.get("title"),
        "hypothesis": investigation.get("hypothesis"),
        "current_status": investigation.get("status"),
        "attached_queries": by_type.get("saved_query", []),
        "attached_evidence": by_type.get("evidence", []),
        "attached_insights": by_type.get("insight", []),
        "attached_candidates": by_type.get("research_discovery_candidate", []),
        "attached_artifacts": by_type.get("artifact", []) + by_type.get("context_snapshot", []) + by_type.get("bookmark", []),
        "latest_timeline_event": timeline[-1] if timeline else None,
        "timeline_event_count": len(timeline),
        "reference_count": len(references),
        "diagnostic_only": investigation.get("diagnostic_only") is True,
        "production_recommendation": investigation.get("production_recommendation") is True,
        "trading_gate": investigation.get("trading_gate") is True,
    }
    summary["deterministic_fingerprint"] = investigation_fingerprint(summary)
    return summary


def investigation_fingerprint(payload: Mapping[str, Any]) -> str:
    normalized = _strip_volatile(payload)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def publish_ie1_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    sample = create_investigation(
        title="Sample Investigation",
        description="Example diagnostic investigation workspace.",
        hypothesis="Strategy expectancy may vary by session and VIX context.",
        owner="operator",
        tags=["sample", "diagnostic"],
        investigation_id="sample_investigation",
        now=now or "2026-07-05T12:00:00+00:00",
        output_dir=output_dir,
    ).investigation
    sample = attach_reference(
        sample,
        reference_type="saved_query",
        reference_id="expectancy_by_strategy",
        artifact_path="outputs/track_b_execution_core/research/canonical_analytics_engine/saved_queries/saved_analytics_queries.jsonl",
        label="Expectancy by strategy",
        now=now or "2026-07-05T12:01:00+00:00",
    )
    sample_result = write_investigation(sample, output_dir=output_dir)
    paths = {
        "contract": output_dir / CONTRACT_MD,
        "schema": output_dir / SCHEMA_JSON,
        "sample": output_dir / SAMPLE_INVESTIGATION_JSON,
        "summary": output_dir / SUMMARY_MD,
        "timeline_contract": output_dir / TIMELINE_CONTRACT_MD,
    }
    paths["contract"].write_text(render_investigation_contract(), encoding="utf-8")
    paths["schema"].write_text(json.dumps(investigation_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample"].write_text(json.dumps(sample_result.investigation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["summary"].write_text(render_investigation_summary(sample_result.summary), encoding="utf-8")
    paths["timeline_contract"].write_text(render_timeline_contract(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def render_investigation_contract() -> str:
    return "\n".join([
        "# IE1 Canonical Investigation Contract",
        "",
        f"- Schema version: `{INVESTIGATION_SCHEMA_VERSION}`",
        "- Investigations are durable diagnostic research workspaces.",
        "- They can reference Evidence, saved queries, executions, diffs, insights, Morning Briefs, research candidates, context snapshots, artifacts, and bookmarks.",
        "- IE1 does not implement notebooks, free-form notes, AI, UI, runtime behavior, broker integration, recommendations, or gates.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_investigation_summary(summary: Mapping[str, Any]) -> str:
    return "\n".join([
        "# IE1 Investigation Summary",
        "",
        f"- Investigation: `{summary.get('investigation_id')}`",
        f"- Title: {summary.get('title')}",
        f"- Status: `{summary.get('current_status')}`",
        f"- Timeline events: `{summary.get('timeline_event_count')}`",
        f"- References: `{summary.get('reference_count')}`",
        f"- Fingerprint: `{summary.get('deterministic_fingerprint')}`",
        "",
    ])


def render_timeline_contract() -> str:
    return "\n".join([
        "# IE1 Timeline Contract",
        "",
        f"- Event schema version: `{TIMELINE_EVENT_SCHEMA_VERSION}`",
        "- Timeline events are sorted by timestamp and then event type.",
        "- Supported events: `INVESTIGATION_CREATED`, `EVIDENCE_ATTACHED`, `EVIDENCE_SUPERSEDED`, `EVIDENCE_ARCHIVED`, `EVIDENCE_INVALIDATED`, `QUERY_EXECUTED`, `INSIGHT_ATTACHED`, `DIFF_ATTACHED`, `MORNING_BRIEF_REFERENCED`, `CANDIDATE_BOOKMARKED`, `ARTIFACT_ATTACHED`, `INVESTIGATION_COMPLETED`.",
        "- Each event includes timestamp, event type, artifact reference, provenance, and diagnostic guardrails.",
        "",
    ])


def investigation_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalInvestigation",
        "type": "object",
        "required": [
            "schema_version",
            "investigation_id",
            "title",
            "description",
            "hypothesis",
            "created_at",
            "updated_at",
            "status",
            "owner",
            "diagnostic_only",
            "production_recommendation",
            "trading_gate",
            "references",
            "timeline",
        ],
        "properties": {
            "schema_version": {"const": INVESTIGATION_SCHEMA_VERSION},
            "investigation_id": {"type": "string"},
            "title": {"type": "string"},
            "description": {"type": "string"},
            "hypothesis": {"type": "string"},
            "created_at": {"type": "string"},
            "updated_at": {"type": "string"},
            "status": {"enum": sorted(VALID_STATUSES)},
            "owner": {"type": "string"},
            "tags": {"type": "array", "items": {"type": "string"}},
            "diagnostic_only": {"const": True},
            "production_recommendation": {"const": False},
            "trading_gate": {"const": False},
            "references": {"type": "array"},
            "timeline": {"type": "array"},
        },
    }


def _timeline_event(
    *,
    event_type: str,
    timestamp: datetime,
    artifact_reference: Mapping[str, Any],
    provenance: Mapping[str, Any],
) -> dict[str, Any]:
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(f"Unsupported investigation event type: {event_type}")
    return {
        "schema_version": TIMELINE_EVENT_SCHEMA_VERSION,
        "timestamp": timestamp.isoformat(),
        "event_type": event_type,
        "artifact_reference": dict(artifact_reference),
        "provenance": dict(provenance),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def _event_type_for_reference(reference_type: str) -> str:
    return {
        "saved_query": "QUERY_EXECUTED",
        "evidence": "EVIDENCE_ATTACHED",
        "execution_record": "QUERY_EXECUTED",
        "diff_result": "DIFF_ATTACHED",
        "insight": "INSIGHT_ATTACHED",
        "morning_brief": "MORNING_BRIEF_REFERENCED",
        "research_discovery_candidate": "CANDIDATE_BOOKMARKED",
        "candidate_review": "CANDIDATE_BOOKMARKED",
    }.get(reference_type, "ARTIFACT_ATTACHED")


def _sort_timeline(timeline: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted((dict(item) for item in timeline), key=lambda row: (str(row.get("timestamp")), str(row.get("event_type"))))


def _validate_status(status: str) -> str:
    normalized = status.upper()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"Unsupported investigation status: {status}")
    return normalized


def _validate_reference_type(reference_type: str) -> None:
    if reference_type not in VALID_REFERENCE_TYPES:
        raise ValueError(f"Unsupported investigation reference type: {reference_type}")


def _investigation_id(title: str, hypothesis: str, generated_at: datetime) -> str:
    seed = json.dumps({"title": title, "hypothesis": hypothesis, "created_at": generated_at.isoformat()}, sort_keys=True)
    return f"inv_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}"


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _strip_volatile(item)
            for key, item in sorted(value.items())
            if key not in {"updated_at", "deterministic_fingerprint"}
        }
    if isinstance(value, list):
        return [_strip_volatile(item) for item in value]
    return value


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
