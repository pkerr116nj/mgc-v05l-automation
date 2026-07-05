"""Shared Canonical Reference and Provenance envelopes for investigations."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "investigation_engine" / "reference_envelope"

REFERENCE_SCHEMA_VERSION = "ie5_canonical_reference_v1"
PROVENANCE_SCHEMA_VERSION = "ie5_canonical_provenance_envelope_v1"

VALID_TARGET_KINDS = {
    "INVESTIGATION",
    "EVIDENCE",
    "CLAIM",
    "CONCLUSION",
    "SAVED_QUERY",
    "EXECUTION_RECORD",
    "ANALYTICS_RESULT",
    "ANALYTICS_DIFF",
    "ANALYTICS_INSIGHT",
    "MORNING_BRIEF",
    "RESEARCH_DISCOVERY_CANDIDATE",
    "CANDIDATE_REVIEW",
    "CONTEXT_SNAPSHOT",
    "OPERATIONAL_CERTIFICATION",
    "SAFE_STATE",
    "GUARDIAN",
    "RUNTIME_HEALTH",
    "ARTIFACT",
    "BOOKMARK",
}
VALID_RELATIONSHIPS = {"references", "supports", "contradicts", "related_to", "derived_from", "supersedes", "duplicates", "contains", "attached_to"}


def build_provenance_envelope(
    *,
    source_component: str,
    source_artifact: str | None = None,
    source_artifact_hash: str | None = None,
    source_generated_at: str | None = None,
    parent_fingerprint: str | None = None,
    parent_schema_version: str | None = None,
    transform_name: str = "direct_reference",
    transform_version: str = "1",
    created_at: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(created_at)
    seed = json.dumps(
        {
            "source_component": source_component,
            "source_artifact": source_artifact,
            "parent_fingerprint": parent_fingerprint,
            "transform_name": transform_name,
            "created_at": timestamp.isoformat(),
        },
        sort_keys=True,
    )
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        "provenance_id": f"prov_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}",
        "source_component": source_component,
        "source_artifact": source_artifact,
        "source_artifact_hash": source_artifact_hash,
        "source_generated_at": source_generated_at,
        "parent_fingerprint": parent_fingerprint,
        "parent_schema_version": parent_schema_version,
        "transform_name": transform_name,
        "transform_version": transform_version,
        "created_at": timestamp.isoformat(),
        "guardrails": {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False},
    }


def build_reference(
    *,
    reference_type: str,
    target_id: str,
    target_kind: str,
    relationship: str = "references",
    target_path: str | None = None,
    target_schema_version: str | None = None,
    target_fingerprint: str | None = None,
    source_component: str = "canonical_reference_envelope",
    provenance: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    created_at: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(created_at)
    target_kind = _validate_target_kind(target_kind)
    relationship = _validate_relationship(relationship)
    envelope = dict(
        provenance
        or build_provenance_envelope(
            source_component=source_component,
            source_artifact=target_path,
            parent_fingerprint=target_fingerprint,
            parent_schema_version=target_schema_version,
            created_at=timestamp,
        )
    )
    reference = {
        "schema_version": REFERENCE_SCHEMA_VERSION,
        "reference_id": _reference_id(reference_type, target_id, relationship, timestamp),
        "reference_type": reference_type,
        "target_id": target_id,
        "target_kind": target_kind,
        "target_path": target_path,
        "target_schema_version": target_schema_version,
        "target_fingerprint": target_fingerprint,
        "relationship": relationship,
        "created_at": timestamp.isoformat(),
        "source_component": source_component,
        "provenance": envelope,
        "metadata": dict(metadata or {}),
    }
    reference["reference_fingerprint"] = reference_fingerprint(reference)
    return reference


def validate_reference(reference: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if reference.get("schema_version") != REFERENCE_SCHEMA_VERSION:
        errors.append("invalid_reference_schema_version")
    if not reference.get("reference_id"):
        errors.append("missing_reference_id")
    if not reference.get("target_id"):
        errors.append("missing_target_id")
    if reference.get("target_kind") not in VALID_TARGET_KINDS:
        errors.append("invalid_target_kind")
    if reference.get("relationship") not in VALID_RELATIONSHIPS:
        errors.append("invalid_relationship")
    provenance = reference.get("provenance") or {}
    if provenance.get("schema_version") != PROVENANCE_SCHEMA_VERSION:
        errors.append("invalid_provenance_schema_version")
    guardrails = provenance.get("guardrails") or {}
    if guardrails.get("diagnostic_only") is not True or guardrails.get("production_recommendation") is not False or guardrails.get("trading_gate") is not False:
        errors.append("invalid_provenance_guardrails")
    return {"valid": not errors, "errors": errors}


def reference_fingerprint(reference: Mapping[str, Any]) -> str:
    payload = {key: _strip_volatile(value) for key, value in sorted(reference.items()) if key != "reference_fingerprint"}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def missing_reference_summary(references: list[Mapping[str, Any]], targets_by_id: Mapping[str, Any]) -> dict[str, Any]:
    missing = [ref for ref in references if ref.get("target_id") not in targets_by_id]
    broken = [ref for ref in references if not validate_reference(ref)["valid"]]
    return {"reference_count": len(references), "missing_target_count": len(missing), "broken_reference_count": len(broken)}


def verify_research_chain(
    conclusion: Mapping[str, Any],
    *,
    claims_by_id: Mapping[str, Mapping[str, Any]],
    evidence_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    if conclusion.get("schema_version") != "ie4_canonical_conclusion_v1":
        return _verification("NOT_APPLICABLE", unsupported_reference_count=1)
    refs = list(conclusion.get("supporting_claims") or []) + list(conclusion.get("contradicting_claims") or []) + list(conclusion.get("related_claims") or [])
    if not refs:
        return _verification("INCOMPLETE")
    counts = {"valid_chain_count": 0, "broken_reference_count": 0, "missing_target_count": 0, "fingerprint_mismatch_count": 0, "unsupported_reference_count": 0}
    for ref in refs:
        if ref.get("target_kind") != "CLAIM":
            counts["unsupported_reference_count"] += 1
            continue
        claim = claims_by_id.get(str(ref.get("target_id")))
        if claim is None:
            counts["missing_target_count"] += 1
            continue
        if ref.get("target_fingerprint") and ref.get("target_fingerprint") != claim.get("deterministic_fingerprint"):
            counts["fingerprint_mismatch_count"] += 1
            continue
        evidence_refs = list(claim.get("supporting_evidence") or []) + list(claim.get("contradicting_evidence") or []) + list(claim.get("related_evidence") or [])
        if not evidence_refs:
            counts["broken_reference_count"] += 1
            continue
        for evidence_ref in evidence_refs:
            if evidence_ref.get("target_kind") != "EVIDENCE":
                counts["unsupported_reference_count"] += 1
                continue
            evidence = evidence_by_id.get(str(evidence_ref.get("target_id")))
            if evidence is None:
                counts["missing_target_count"] += 1
                continue
            if evidence_ref.get("target_fingerprint") and evidence_ref.get("target_fingerprint") != evidence.get("deterministic_fingerprint"):
                counts["fingerprint_mismatch_count"] += 1
                continue
            counts["valid_chain_count"] += 1
    if counts["fingerprint_mismatch_count"] or counts["broken_reference_count"]:
        status = "BROKEN"
    elif counts["missing_target_count"] or counts["unsupported_reference_count"]:
        status = "VERIFIED_WITH_WARNINGS" if counts["valid_chain_count"] else "INCOMPLETE"
    else:
        status = "VERIFIED" if counts["valid_chain_count"] else "INCOMPLETE"
    return {"verification_status": status, **counts}


def publish_ie5_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    sample = build_reference(reference_type="sample", target_id="sample_evidence", target_kind="EVIDENCE", relationship="supports", target_schema_version="ie2_canonical_evidence_v1", target_fingerprint="sample")
    verification = _verification("INCOMPLETE")
    paths = {
        "reference_contract": output_dir / "ie5_canonical_reference_contract.md",
        "provenance_contract": output_dir / "ie5_canonical_provenance_envelope.md",
        "schema": output_dir / "ie5_reference_schema.json",
        "verifier_contract": output_dir / "ie5_chain_verifier_contract.md",
        "sample_verification": output_dir / "ie5_sample_chain_verification.json",
        "migration_notes": output_dir / "ie5_reference_migration_notes.md",
    }
    paths["reference_contract"].write_text(render_reference_contract(), encoding="utf-8")
    paths["provenance_contract"].write_text(render_provenance_contract(), encoding="utf-8")
    paths["schema"].write_text(json.dumps(reference_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["verifier_contract"].write_text(render_chain_verifier_contract(), encoding="utf-8")
    paths["sample_verification"].write_text(json.dumps({"sample_reference": sample, "verification": verification}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["migration_notes"].write_text(render_reference_migration_notes(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def render_reference_contract() -> str:
    return """# IE5 Canonical Reference Contract

CanonicalReference normalizes references across Investigation, Evidence, Claim, and Conclusion layers while preserving legacy semantic aliases.

## Required fields

- schema_version
- reference_id
- reference_type
- target_id
- target_kind
- target_path
- target_schema_version
- target_fingerprint
- relationship
- created_at
- source_component
- provenance
- metadata
- reference_fingerprint

## Target kinds

INVESTIGATION, EVIDENCE, CLAIM, CONCLUSION, SAVED_QUERY, EXECUTION_RECORD, ANALYTICS_RESULT, ANALYTICS_DIFF, ANALYTICS_INSIGHT, MORNING_BRIEF, RESEARCH_DISCOVERY_CANDIDATE, CANDIDATE_REVIEW, CONTEXT_SNAPSHOT, OPERATIONAL_CERTIFICATION, SAFE_STATE, GUARDIAN, RUNTIME_HEALTH, ARTIFACT, BOOKMARK.

## Relationships

references, supports, contradicts, related_to, derived_from, supersedes, duplicates, contains, attached_to.

Canonical reference ids identify the reference envelope. Legacy semantic ids are retained as `target_id` and, where needed, `legacy_reference_id`.
"""


def render_provenance_contract() -> str:
    return """# IE5 Canonical Provenance Envelope

CanonicalProvenanceEnvelope records source component, source artifact, parent fingerprint/schema, transform identity, timestamp, and diagnostic guardrails.

## Fields

- provenance_id
- source_component
- source_artifact
- source_artifact_hash
- source_generated_at
- parent_fingerprint
- parent_schema_version
- transform_name
- transform_version
- created_at
- guardrails

Guardrails are always diagnostic-only:

- diagnostic_only=true
- production_recommendation=false
- trading_gate=false
"""


def render_chain_verifier_contract() -> str:
    return """# IE5 Chain Verifier Contract

The verifier walks Conclusion -> Claim -> Evidence -> source reference and reports valid, broken, missing, fingerprint mismatch, and unsupported counts.

## Status values

- VERIFIED
- VERIFIED_WITH_WARNINGS
- BROKEN
- INCOMPLETE
- NOT_APPLICABLE

## Counts

- valid_chain_count
- broken_reference_count
- missing_target_count
- fingerprint_mismatch_count
- unsupported_reference_count

The verifier is deterministic and informational only. It does not create claims, conclusions, gates, or production recommendations.
"""


def render_reference_migration_notes() -> str:
    return """# IE5 Reference Migration Notes

Existing semantic fields remain for backward compatibility. New references add `target_id`, `target_kind`, `target_schema_version`, `target_fingerprint`, `relationship`, and provenance envelope fields.

## Layer mapping

- Investigation references now emit CanonicalReference and preserve the older target identifier as `target_id` and `legacy_reference_id`.
- Evidence attachments now emit CanonicalReference while preserving attachment labels and artifact paths.
- Claim evidence references now emit CanonicalReference with preserved target fingerprints.
- Conclusion claim references now emit CanonicalReference with preserved target fingerprints.

## Compatibility

The Investigation -> Evidence -> Claim -> Conclusion hierarchy is unchanged. Validation semantics are unchanged except that references now carry normalized provenance and fingerprint metadata.
"""


def reference_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalReference",
        "type": "object",
        "required": ["schema_version", "reference_id", "reference_type", "target_id", "target_kind", "relationship", "created_at", "source_component", "provenance"],
        "properties": {
            "schema_version": {"const": REFERENCE_SCHEMA_VERSION},
            "target_kind": {"enum": sorted(VALID_TARGET_KINDS)},
            "relationship": {"enum": sorted(VALID_RELATIONSHIPS)},
        },
    }


def _verification(status: str, **overrides: int) -> dict[str, Any]:
    base = {
        "valid_chain_count": 0,
        "broken_reference_count": 0,
        "missing_target_count": 0,
        "fingerprint_mismatch_count": 0,
        "unsupported_reference_count": 0,
        "verification_status": status,
    }
    base.update(overrides)
    return base


def _validate_target_kind(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_TARGET_KINDS:
        raise ValueError(f"Unsupported reference target kind: {value}")
    return normalized


def _validate_relationship(value: str) -> str:
    if value not in VALID_RELATIONSHIPS:
        raise ValueError(f"Unsupported reference relationship: {value}")
    return value


def _reference_id(reference_type: str, target_id: str, relationship: str, timestamp: datetime) -> str:
    seed = json.dumps({"reference_type": reference_type, "target_id": target_id, "relationship": relationship, "created_at": timestamp.isoformat()}, sort_keys=True)
    return f"ref_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}"


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _strip_volatile(item) for key, item in sorted(value.items()) if key != "reference_fingerprint"}
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
