"""Canonical Evidence Engine for deterministic Track B investigations."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "investigation_engine" / "evidence"

EVIDENCE_SCHEMA_VERSION = "ie2_canonical_evidence_v1"
EVIDENCE_SUMMARY_SCHEMA_VERSION = "ie2_canonical_evidence_summary_v1"

CONTRACT_MD = "ie2_evidence_contract.md"
SCHEMA_JSON = "ie2_evidence_schema.json"
SAMPLE_EVIDENCE_JSON = "ie2_sample_evidence.json"
RELATIONSHIP_CONTRACT_MD = "ie2_evidence_relationship_contract.md"
SUMMARY_MD = "ie2_evidence_summary.md"
CLAIM_ARCHITECTURE_MD = "ie3_claim_architecture.md"
CONCLUSION_ARCHITECTURE_MD = "ie4_conclusion_architecture.md"

VALID_CLASSIFICATIONS = {"ANALYTICS", "DIFF", "INSIGHT", "DISCOVERY", "MORNING_BRIEF", "CONTEXT", "CANDIDATE", "OPERATIONAL"}
VALID_STATUSES = {"ACTIVE", "SUPERSEDED", "INVALIDATED", "ARCHIVED"}
VALID_ATTACHMENT_TYPES = {
    "saved_query",
    "execution_record",
    "analytics_result",
    "analytics_diff",
    "insight",
    "morning_brief",
    "research_discovery_candidate",
    "candidate_review",
    "context_snapshot",
    "operational_certification",
    "safe_state",
    "guardian",
    "runtime_health",
    "artifact",
    "bookmark",
}
VALID_RELATIONSHIP_TYPES = {"supports", "derived_from", "references", "supersedes", "duplicates", "related_to"}


@dataclass(frozen=True)
class CanonicalEvidenceResult:
    evidence: dict[str, Any]
    evidence_path: Path
    summary: dict[str, Any]
    summary_path: Path
    output_dir: Path


def create_evidence(
    *,
    investigation_id: str,
    title: str,
    summary: str,
    evidence_classification: str,
    evidence_type: str,
    source_component: str,
    provenance: Mapping[str, Any] | None = None,
    evidence_id: str | None = None,
    status: str = "ACTIVE",
    now: datetime | str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> CanonicalEvidenceResult:
    timestamp = _coerce_now(now)
    classification = _validate_classification(evidence_classification)
    status = _validate_status(status)
    evidence = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "evidence_id": evidence_id or _evidence_id(investigation_id, title, timestamp),
        "investigation_id": investigation_id,
        "created_at": timestamp.isoformat(),
        "updated_at": timestamp.isoformat(),
        "evidence_classification": classification,
        "status": status,
        "title": title,
        "summary": summary,
        "evidence_type": evidence_type,
        "source_component": source_component,
        "provenance": dict(provenance or {"source": "canonical_evidence_engine", "operation": "create"}),
        "attachments": [],
        "relationships": [],
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    evidence["deterministic_fingerprint"] = evidence_fingerprint(evidence)
    return write_evidence(evidence, output_dir=output_dir)


def attach_evidence_reference(
    evidence: Mapping[str, Any],
    *,
    attachment_type: str,
    reference_id: str,
    artifact_path: str | None = None,
    label: str | None = None,
    provenance: Mapping[str, Any] | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    _validate_attachment_type(attachment_type)
    timestamp = _coerce_now(now)
    updated = dict(evidence)
    attachments = [dict(item) for item in updated.get("attachments") or []]
    attachments.append({
        "attachment_type": attachment_type,
        "reference_id": reference_id,
        "artifact_path": artifact_path,
        "label": label,
        "attached_at": timestamp.isoformat(),
        "provenance": dict(provenance or {"source": "canonical_evidence_engine", "operation": "attach_reference"}),
    })
    updated["attachments"] = sorted(attachments, key=lambda row: (str(row.get("attachment_type")), str(row.get("reference_id")), str(row.get("attached_at"))))
    updated["updated_at"] = timestamp.isoformat()
    updated["deterministic_fingerprint"] = evidence_fingerprint(updated)
    return updated


def add_evidence_relationship(
    evidence: Mapping[str, Any],
    *,
    relationship_type: str,
    target_evidence_id: str,
    provenance: Mapping[str, Any] | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    _validate_relationship_type(relationship_type)
    timestamp = _coerce_now(now)
    updated = dict(evidence)
    relationships = [dict(item) for item in updated.get("relationships") or []]
    relationships.append({
        "relationship_type": relationship_type,
        "target_evidence_id": target_evidence_id,
        "created_at": timestamp.isoformat(),
        "provenance": dict(provenance or {"source": "canonical_evidence_engine", "operation": "add_relationship"}),
    })
    updated["relationships"] = sorted(relationships, key=lambda row: (str(row.get("relationship_type")), str(row.get("target_evidence_id")), str(row.get("created_at"))))
    updated["updated_at"] = timestamp.isoformat()
    updated["deterministic_fingerprint"] = evidence_fingerprint(updated)
    return updated


def update_evidence_status(
    evidence: Mapping[str, Any],
    *,
    status: str,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    updated = dict(evidence)
    updated["status"] = _validate_status(status)
    updated["updated_at"] = timestamp.isoformat()
    updated["deterministic_fingerprint"] = evidence_fingerprint(updated)
    return updated


def traverse_evidence_relationships(evidence: Mapping[str, Any], *, relationship_type: str | None = None) -> list[str]:
    rows = []
    for relationship in evidence.get("relationships") or []:
        if relationship_type is None or relationship.get("relationship_type") == relationship_type:
            rows.append(str(relationship.get("target_evidence_id")))
    return rows


def write_evidence(evidence: Mapping[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> CanonicalEvidenceResult:
    validate_evidence(evidence)
    output_dir.mkdir(parents=True, exist_ok=True)
    evidence_id = str(evidence["evidence_id"])
    payload = dict(evidence)
    payload["deterministic_fingerprint"] = evidence_fingerprint(payload)
    evidence_path = output_dir / f"{evidence_id}.json"
    evidence_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = summarize_evidence(payload)
    summary_path = output_dir / f"{evidence_id}.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CanonicalEvidenceResult(evidence=payload, evidence_path=evidence_path, summary=summary, summary_path=summary_path, output_dir=output_dir)


def load_evidence(evidence_id: str, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    return json.loads((output_dir / f"{evidence_id}.json").read_text(encoding="utf-8"))


def list_evidence(*, output_dir: Path = DEFAULT_OUTPUT_DIR, investigation_id: str | None = None) -> list[dict[str, Any]]:
    if not output_dir.exists():
        return []
    rows = []
    for path in sorted(output_dir.glob("*.json")):
        if path.name.endswith(".summary.json") or path.name.startswith("ie2_"):
            continue
        try:
            evidence = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if evidence.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
            continue
        if investigation_id and evidence.get("investigation_id") != investigation_id:
            continue
        rows.append(summarize_evidence(evidence))
    return rows


def validate_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if evidence.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        errors.append("invalid_schema_version")
    for key in ("evidence_id", "investigation_id", "created_at", "updated_at", "title", "summary", "evidence_type", "source_component"):
        if not evidence.get(key):
            errors.append(f"missing_{key}")
    if evidence.get("evidence_classification") not in VALID_CLASSIFICATIONS:
        errors.append("invalid_evidence_classification")
    if evidence.get("status") not in VALID_STATUSES:
        errors.append("invalid_status")
    if evidence.get("diagnostic_only") is not True:
        errors.append("diagnostic_only_not_true")
    if evidence.get("production_recommendation") is not False:
        errors.append("production_recommendation_not_false")
    if evidence.get("trading_gate") is not False:
        errors.append("trading_gate_not_false")
    for attachment in evidence.get("attachments") or []:
        if attachment.get("attachment_type") not in VALID_ATTACHMENT_TYPES:
            errors.append(f"invalid_attachment_type:{attachment.get('attachment_type')}")
    for relationship in evidence.get("relationships") or []:
        if relationship.get("relationship_type") not in VALID_RELATIONSHIP_TYPES:
            errors.append(f"invalid_relationship_type:{relationship.get('relationship_type')}")
    if errors:
        raise ValueError(";".join(errors))
    return {"valid": True, "errors": []}


def summarize_evidence(evidence: Mapping[str, Any]) -> dict[str, Any]:
    summary = {
        "schema_version": EVIDENCE_SUMMARY_SCHEMA_VERSION,
        "evidence_id": evidence.get("evidence_id"),
        "investigation_id": evidence.get("investigation_id"),
        "title": evidence.get("title"),
        "source": evidence.get("source_component"),
        "evidence_classification": evidence.get("evidence_classification"),
        "status": evidence.get("status"),
        "attachment_count": len(evidence.get("attachments") or []),
        "relationship_count": len(evidence.get("relationships") or []),
        "latest_update": evidence.get("updated_at"),
        "diagnostic_only": evidence.get("diagnostic_only") is True,
        "production_recommendation": evidence.get("production_recommendation") is True,
        "trading_gate": evidence.get("trading_gate") is True,
    }
    summary["deterministic_fingerprint"] = evidence_fingerprint(summary)
    return summary


def evidence_fingerprint(payload: Mapping[str, Any]) -> str:
    normalized = _strip_volatile(payload)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def publish_ie2_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result = create_evidence(
        investigation_id="sample_investigation",
        evidence_id="sample_evidence",
        title="Sample analytics evidence",
        summary="Expectancy by strategy query is attached as deterministic evidence.",
        evidence_classification="ANALYTICS",
        evidence_type="saved_query_result",
        source_component="canonical_analytics_engine",
        provenance={"source": "canonical_evidence_engine", "operation": "publish_ie2_artifacts"},
        now=now or "2026-07-05T12:00:00+00:00",
        output_dir=output_dir,
    )
    evidence = attach_evidence_reference(
        result.evidence,
        attachment_type="saved_query",
        reference_id="expectancy_by_strategy",
        artifact_path="outputs/track_b_execution_core/research/canonical_analytics_engine/saved_queries/saved_analytics_queries.jsonl",
        label="Expectancy by strategy",
        now=now or "2026-07-05T12:00:00+00:00",
    )
    result = write_evidence(evidence, output_dir=output_dir)
    paths = {
        "contract": output_dir / CONTRACT_MD,
        "schema": output_dir / SCHEMA_JSON,
        "sample": output_dir / SAMPLE_EVIDENCE_JSON,
        "relationship_contract": output_dir / RELATIONSHIP_CONTRACT_MD,
        "summary": output_dir / SUMMARY_MD,
        "ie3_claim_architecture": output_dir / CLAIM_ARCHITECTURE_MD,
        "ie4_conclusion_architecture": output_dir / CONCLUSION_ARCHITECTURE_MD,
    }
    paths["contract"].write_text(render_evidence_contract(), encoding="utf-8")
    paths["schema"].write_text(json.dumps(evidence_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample"].write_text(json.dumps(result.evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["relationship_contract"].write_text(render_relationship_contract(), encoding="utf-8")
    paths["summary"].write_text(render_evidence_summary(result.summary), encoding="utf-8")
    paths["ie3_claim_architecture"].write_text(render_ie3_claim_architecture(), encoding="utf-8")
    paths["ie4_conclusion_architecture"].write_text(render_ie4_conclusion_architecture(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def render_evidence_contract() -> str:
    return "\n".join([
        "# IE2 Canonical Evidence Contract",
        "",
        f"- Schema version: `{EVIDENCE_SCHEMA_VERSION}`",
        "- Evidence is the canonical deterministic fact model attached to investigations.",
        "- Evidence is never AI-generated and never creates trading recommendations.",
        "- Supported classifications: `ANALYTICS`, `DIFF`, `INSIGHT`, `DISCOVERY`, `MORNING_BRIEF`, `CONTEXT`, `CANDIDATE`, `OPERATIONAL`.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_relationship_contract() -> str:
    return "\n".join([
        "# IE2 Evidence Relationship Contract",
        "",
        "- Relationships are stored as simple references, not a graph database.",
        "- Supported relationship types: `supports`, `derived_from`, `references`, `supersedes`, `duplicates`, `related_to`.",
        "- Relationship traversal returns deterministic target evidence ids for local analysis.",
        "",
    ])


def render_evidence_summary(summary: Mapping[str, Any]) -> str:
    return "\n".join([
        "# IE2 Evidence Summary",
        "",
        f"- Evidence: `{summary.get('evidence_id')}`",
        f"- Investigation: `{summary.get('investigation_id')}`",
        f"- Classification: `{summary.get('evidence_classification')}`",
        f"- Attachments: `{summary.get('attachment_count')}`",
        f"- Relationships: `{summary.get('relationship_count')}`",
        f"- Fingerprint: `{summary.get('deterministic_fingerprint')}`",
        "",
    ])


def render_ie3_claim_architecture() -> str:
    return "\n".join([
        "# IE3 Canonical Claim Architecture",
        "",
        "Design only. Claims are not implemented in IE2A.",
        "",
        "A future CanonicalClaim should include `claim_id`, `investigation_id`, `statement`, `confidence`, `supporting_evidence`, `contradicting_evidence`, `status`, `provenance`, and a deterministic fingerprint.",
        "",
        "Claims consume Evidence by referencing immutable evidence ids and fingerprints. Claim confidence must be deterministic, rule-based, and traceable to supporting and contradicting evidence. Claims should never be generated from free-form AI text or hallucinated sources.",
        "",
    ])


def render_ie4_conclusion_architecture() -> str:
    return "\n".join([
        "# IE4 Canonical Conclusion Architecture",
        "",
        "Design only. Conclusions are not implemented in IE2A.",
        "",
        "A future CanonicalConclusion should include `conclusion_id`, `investigation_id`, `hypothesis`, `conclusion`, `confidence`, `supporting_claims`, `provenance`, and a deterministic fingerprint.",
        "",
        "Conclusions assemble from Canonical Claims, preserving traceability through claim ids, evidence ids, and fingerprints. Conclusions summarize research state only and are distinct from trading recommendations, gates, or production actions.",
        "",
    ])


def evidence_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalEvidence",
        "type": "object",
        "required": [
            "schema_version",
            "evidence_id",
            "investigation_id",
            "created_at",
            "updated_at",
            "evidence_classification",
            "status",
            "title",
            "summary",
            "evidence_type",
            "source_component",
            "provenance",
            "attachments",
            "relationships",
            "diagnostic_only",
            "production_recommendation",
            "trading_gate",
        ],
        "properties": {
            "schema_version": {"const": EVIDENCE_SCHEMA_VERSION},
            "evidence_classification": {"enum": sorted(VALID_CLASSIFICATIONS)},
            "status": {"enum": sorted(VALID_STATUSES)},
            "diagnostic_only": {"const": True},
            "production_recommendation": {"const": False},
            "trading_gate": {"const": False},
        },
    }


def _validate_classification(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_CLASSIFICATIONS:
        raise ValueError(f"Unsupported evidence classification: {value}")
    return normalized


def _validate_status(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"Unsupported evidence status: {value}")
    return normalized


def _validate_attachment_type(value: str) -> None:
    if value not in VALID_ATTACHMENT_TYPES:
        raise ValueError(f"Unsupported evidence attachment type: {value}")


def _validate_relationship_type(value: str) -> None:
    if value not in VALID_RELATIONSHIP_TYPES:
        raise ValueError(f"Unsupported evidence relationship type: {value}")


def _evidence_id(investigation_id: str, title: str, timestamp: datetime) -> str:
    seed = json.dumps({"investigation_id": investigation_id, "title": title, "created_at": timestamp.isoformat()}, sort_keys=True)
    return f"ev_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}"


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
