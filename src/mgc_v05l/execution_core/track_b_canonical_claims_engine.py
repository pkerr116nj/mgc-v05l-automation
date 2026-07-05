"""Canonical Claims Engine for deterministic investigation research."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "investigation_engine" / "claims"

CLAIM_SCHEMA_VERSION = "ie3_canonical_claim_v1"
CLAIM_SUMMARY_SCHEMA_VERSION = "ie3_canonical_claim_summary_v1"

CONTRACT_MD = "ie3_claim_contract.md"
SCHEMA_JSON = "ie3_claim_schema.json"
SAMPLE_CLAIM_JSON = "ie3_sample_claim.json"
VALIDATION_CONTRACT_MD = "ie3_claim_validation_contract.md"
SUMMARY_MD = "ie3_claim_summary.md"
CONCLUSION_ARCHITECTURE_MD = "ie4_conclusion_architecture.md"

VALID_CLASSIFICATIONS = {"MARKET_STRUCTURE", "ANALYTICS", "DISCOVERY", "CONTEXT", "OPERATIONAL", "CANDIDATE", "HYPOTHESIS", "VALIDATION"}
VALID_STATUSES = {"DRAFT", "ACTIVE", "SUPERSEDED", "INVALIDATED", "ARCHIVED"}
VALID_CONFIDENCE = {"UNKNOWN", "LOW", "MEDIUM", "HIGH"}
VALID_VALIDATION_STATUSES = {"SUPPORTED", "PARTIALLY_SUPPORTED", "CONTRADICTED", "INSUFFICIENT_EVIDENCE", "INVALID"}
EVIDENCE_SCHEMA_VERSION = "ie2_canonical_evidence_v1"


@dataclass(frozen=True)
class CanonicalClaimResult:
    claim: dict[str, Any]
    claim_path: Path
    summary: dict[str, Any]
    summary_path: Path
    output_dir: Path


def create_claim(
    *,
    investigation_id: str,
    title: str,
    statement: str,
    rationale: str,
    claim_classification: str,
    claim_type: str,
    confidence: str = "UNKNOWN",
    provenance: Mapping[str, Any] | None = None,
    claim_id: str | None = None,
    status: str = "DRAFT",
    now: datetime | str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> CanonicalClaimResult:
    timestamp = _coerce_now(now)
    claim = {
        "schema_version": CLAIM_SCHEMA_VERSION,
        "claim_id": claim_id or _claim_id(investigation_id, statement, timestamp),
        "investigation_id": investigation_id,
        "created_at": timestamp.isoformat(),
        "updated_at": timestamp.isoformat(),
        "claim_classification": _validate_classification(claim_classification),
        "status": _validate_status(status),
        "title": title,
        "statement": statement,
        "rationale": rationale,
        "claim_type": claim_type,
        "confidence": _validate_confidence(confidence),
        "provenance": dict(provenance or {"source": "canonical_claims_engine", "operation": "create"}),
        "supporting_evidence": [],
        "contradicting_evidence": [],
        "related_evidence": [],
        "validation": {"status": "INSUFFICIENT_EVIDENCE", "reasons": ["no_supporting_or_contradicting_evidence"]},
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    claim["deterministic_fingerprint"] = claim_fingerprint(claim)
    return write_claim(claim, output_dir=output_dir)


def attach_supporting_evidence(
    claim: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    return _attach_evidence(claim, evidence, bucket="supporting_evidence", now=now)


def attach_contradicting_evidence(
    claim: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    return _attach_evidence(claim, evidence, bucket="contradicting_evidence", now=now)


def attach_related_evidence(
    claim: Mapping[str, Any],
    evidence: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    return _attach_evidence(claim, evidence, bucket="related_evidence", now=now)


def validate_claim_support(claim: Mapping[str, Any]) -> dict[str, Any]:
    supporting = list(claim.get("supporting_evidence") or [])
    contradicting = list(claim.get("contradicting_evidence") or [])
    related = list(claim.get("related_evidence") or [])
    all_refs = supporting + contradicting + related
    reasons: list[str] = []
    invalid_refs = [ref for ref in all_refs if not _canonical_evidence_ref_valid(ref)]
    inactive_refs = [ref for ref in supporting + contradicting if ref.get("evidence_status") != "ACTIVE"]
    missing_fingerprint = [ref for ref in all_refs if not ref.get("evidence_fingerprint")]
    missing_provenance = [ref for ref in all_refs if not ref.get("provenance")]
    if invalid_refs:
        reasons.append("invalid_evidence_reference")
        status = "INVALID"
    elif contradicting:
        reasons.append("contradicting_evidence_present")
        status = "CONTRADICTED"
    elif not supporting:
        reasons.append("no_supporting_evidence")
        status = "INSUFFICIENT_EVIDENCE"
    elif inactive_refs or missing_fingerprint or missing_provenance:
        if inactive_refs:
            reasons.append("supporting_or_contradicting_evidence_not_active")
        if missing_fingerprint:
            reasons.append("missing_evidence_fingerprint")
        if missing_provenance:
            reasons.append("missing_evidence_provenance")
        status = "PARTIALLY_SUPPORTED"
    else:
        reasons.append("supporting_evidence_active_and_fingerprinted")
        status = "SUPPORTED"
    return {
        "status": status,
        "reasons": reasons,
        "supporting_evidence_count": len(supporting),
        "contradicting_evidence_count": len(contradicting),
        "related_evidence_count": len(related),
    }


def refresh_claim_validation(
    claim: Mapping[str, Any],
    *,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    updated = dict(claim)
    updated["validation"] = validate_claim_support(updated)
    updated["updated_at"] = timestamp.isoformat()
    updated["deterministic_fingerprint"] = claim_fingerprint(updated)
    return updated


def write_claim(claim: Mapping[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> CanonicalClaimResult:
    validate_claim(claim)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = dict(claim)
    payload["validation"] = validate_claim_support(payload)
    payload["deterministic_fingerprint"] = claim_fingerprint(payload)
    claim_id = str(payload["claim_id"])
    claim_path = output_dir / f"{claim_id}.json"
    claim_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = summarize_claim(payload)
    summary_path = output_dir / f"{claim_id}.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CanonicalClaimResult(claim=payload, claim_path=claim_path, summary=summary, summary_path=summary_path, output_dir=output_dir)


def load_claim(claim_id: str, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    return json.loads((output_dir / f"{claim_id}.json").read_text(encoding="utf-8"))


def list_claims(*, output_dir: Path = DEFAULT_OUTPUT_DIR, investigation_id: str | None = None) -> list[dict[str, Any]]:
    if not output_dir.exists():
        return []
    rows = []
    for path in sorted(output_dir.glob("*.json")):
        if path.name.endswith(".summary.json") or path.name.startswith("ie3_"):
            continue
        try:
            claim = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if claim.get("schema_version") != CLAIM_SCHEMA_VERSION:
            continue
        if investigation_id and claim.get("investigation_id") != investigation_id:
            continue
        rows.append(summarize_claim(claim))
    return rows


def validate_claim(claim: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if claim.get("schema_version") != CLAIM_SCHEMA_VERSION:
        errors.append("invalid_schema_version")
    for key in ("claim_id", "investigation_id", "created_at", "updated_at", "title", "statement", "rationale", "claim_type"):
        if not claim.get(key):
            errors.append(f"missing_{key}")
    if claim.get("claim_classification") not in VALID_CLASSIFICATIONS:
        errors.append("invalid_claim_classification")
    if claim.get("status") not in VALID_STATUSES:
        errors.append("invalid_status")
    if claim.get("confidence") not in VALID_CONFIDENCE:
        errors.append("invalid_confidence")
    if claim.get("diagnostic_only") is not True:
        errors.append("diagnostic_only_not_true")
    if claim.get("production_recommendation") is not False:
        errors.append("production_recommendation_not_false")
    if claim.get("trading_gate") is not False:
        errors.append("trading_gate_not_false")
    for bucket in ("supporting_evidence", "contradicting_evidence", "related_evidence"):
        for ref in claim.get(bucket) or []:
            if not _canonical_evidence_ref_valid(ref):
                errors.append(f"invalid_{bucket}_reference")
    if errors:
        raise ValueError(";".join(errors))
    return {"valid": True, "errors": []}


def summarize_claim(claim: Mapping[str, Any]) -> dict[str, Any]:
    validation = validate_claim_support(claim)
    summary = {
        "schema_version": CLAIM_SUMMARY_SCHEMA_VERSION,
        "claim_id": claim.get("claim_id"),
        "investigation_id": claim.get("investigation_id"),
        "title": claim.get("title"),
        "statement": claim.get("statement"),
        "confidence": claim.get("confidence"),
        "validation_status": validation.get("status"),
        "supporting_evidence_count": validation.get("supporting_evidence_count"),
        "contradicting_evidence_count": validation.get("contradicting_evidence_count"),
        "related_evidence_count": validation.get("related_evidence_count"),
        "diagnostic_only": claim.get("diagnostic_only") is True,
        "production_recommendation": claim.get("production_recommendation") is True,
        "trading_gate": claim.get("trading_gate") is True,
    }
    summary["deterministic_fingerprint"] = claim_fingerprint(summary)
    return summary


def claim_fingerprint(payload: Mapping[str, Any]) -> str:
    normalized = _strip_volatile(payload)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def publish_ie3_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result = create_claim(
        investigation_id="sample_investigation",
        claim_id="sample_claim",
        title="Sample analytics claim",
        statement="Expectancy by strategy is a candidate relationship worth deterministic review.",
        rationale="The claim is backed by canonical evidence rather than free-form notes.",
        claim_classification="ANALYTICS",
        claim_type="evidence_backed_research_assertion",
        confidence="LOW",
        now=now or "2026-07-05T12:00:00+00:00",
        output_dir=output_dir,
    )
    paths = {
        "contract": output_dir / CONTRACT_MD,
        "schema": output_dir / SCHEMA_JSON,
        "sample": output_dir / SAMPLE_CLAIM_JSON,
        "validation_contract": output_dir / VALIDATION_CONTRACT_MD,
        "summary": output_dir / SUMMARY_MD,
        "ie4_conclusion_architecture": output_dir / CONCLUSION_ARCHITECTURE_MD,
    }
    paths["contract"].write_text(render_claim_contract(), encoding="utf-8")
    paths["schema"].write_text(json.dumps(claim_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample"].write_text(json.dumps(result.claim, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["validation_contract"].write_text(render_claim_validation_contract(), encoding="utf-8")
    paths["summary"].write_text(render_claim_summary(result.summary), encoding="utf-8")
    paths["ie4_conclusion_architecture"].write_text(render_ie4_conclusion_architecture(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def render_claim_contract() -> str:
    return "\n".join([
        "# IE3 Canonical Claim Contract",
        "",
        f"- Schema version: `{CLAIM_SCHEMA_VERSION}`",
        "- Claims are deterministic research assertions backed by Canonical Evidence.",
        "- Supporting and contradicting evidence references must be CanonicalEvidence-derived references with ids and fingerprints.",
        "- Claims do not consume AI and do not infer trading actions.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_claim_validation_contract() -> str:
    return "\n".join([
        "# IE3 Claim Validation Contract",
        "",
        "- `SUPPORTED`: active supporting evidence exists, with no contradiction and complete provenance/fingerprints.",
        "- `PARTIALLY_SUPPORTED`: supporting evidence exists but active status, provenance, or fingerprint integrity is incomplete.",
        "- `CONTRADICTED`: contradicting evidence exists.",
        "- `INSUFFICIENT_EVIDENCE`: no supporting evidence exists.",
        "- `INVALID`: malformed or non-canonical evidence references are present.",
        "",
    ])


def render_claim_summary(summary: Mapping[str, Any]) -> str:
    return "\n".join([
        "# IE3 Claim Summary",
        "",
        f"- Claim: `{summary.get('claim_id')}`",
        f"- Investigation: `{summary.get('investigation_id')}`",
        f"- Confidence: `{summary.get('confidence')}`",
        f"- Validation status: `{summary.get('validation_status')}`",
        f"- Supporting evidence: `{summary.get('supporting_evidence_count')}`",
        f"- Contradicting evidence: `{summary.get('contradicting_evidence_count')}`",
        f"- Fingerprint: `{summary.get('deterministic_fingerprint')}`",
        "",
    ])


def render_ie4_conclusion_architecture() -> str:
    return "\n".join([
        "# IE4 Canonical Conclusion Architecture",
        "",
        "Design only. Conclusions are not implemented in IE3.",
        "",
        "Conclusions consume Canonical Claims, not raw observations. A future Conclusion should reference supporting claims, contradicting claims, claim validation states, claim confidence labels, evidence fingerprints, and investigation provenance.",
        "",
        "Confidence propagation should be deterministic. A conclusion with high-confidence supported claims and no active contradictions may receive stronger research confidence; contradictions should lower or block conclusion confidence until resolved. Unknown or low-confidence claims should keep conclusions exploratory.",
        "",
        "Traceability is preserved through a provenance chain: Conclusion -> Claim ids/fingerprints -> Evidence ids/fingerprints -> source artifacts. The chain must be serializable and auditable without AI interpretation.",
        "",
        "Contradiction handling should keep contradictory claims visible rather than deleting or hiding them. Future investigation completion should require an explicit conclusion status with unresolved contradiction counts.",
        "",
        "Conclusions remain research summaries. They are not trading recommendations, runtime gates, broker instructions, or production actions.",
        "",
    ])


def claim_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalClaim",
        "type": "object",
        "required": [
            "schema_version",
            "claim_id",
            "investigation_id",
            "created_at",
            "updated_at",
            "claim_classification",
            "status",
            "title",
            "statement",
            "rationale",
            "claim_type",
            "confidence",
            "provenance",
            "supporting_evidence",
            "contradicting_evidence",
            "related_evidence",
            "diagnostic_only",
            "production_recommendation",
            "trading_gate",
        ],
        "properties": {
            "schema_version": {"const": CLAIM_SCHEMA_VERSION},
            "claim_classification": {"enum": sorted(VALID_CLASSIFICATIONS)},
            "status": {"enum": sorted(VALID_STATUSES)},
            "confidence": {"enum": sorted(VALID_CONFIDENCE)},
            "diagnostic_only": {"const": True},
            "production_recommendation": {"const": False},
            "trading_gate": {"const": False},
        },
    }


def _attach_evidence(claim: Mapping[str, Any], evidence: Mapping[str, Any], *, bucket: str, now: datetime | str | None) -> dict[str, Any]:
    ref = _evidence_reference(evidence, attached_at=_coerce_now(now))
    updated = dict(claim)
    rows = [dict(item) for item in updated.get(bucket) or []]
    rows.append(ref)
    updated[bucket] = sorted(rows, key=lambda row: (str(row.get("evidence_id")), str(row.get("attached_at"))))
    updated["updated_at"] = ref["attached_at"]
    updated["validation"] = validate_claim_support(updated)
    updated["deterministic_fingerprint"] = claim_fingerprint(updated)
    return updated


def _evidence_reference(evidence: Mapping[str, Any], *, attached_at: datetime) -> dict[str, Any]:
    if evidence.get("schema_version") != EVIDENCE_SCHEMA_VERSION:
        raise ValueError("Claim evidence references must come from CanonicalEvidence")
    evidence_id = evidence.get("evidence_id")
    if not evidence_id:
        raise ValueError("CanonicalEvidence reference missing evidence_id")
    fingerprint = evidence.get("deterministic_fingerprint")
    if not fingerprint:
        raise ValueError("CanonicalEvidence reference missing deterministic_fingerprint")
    return {
        "evidence_schema_version": evidence.get("schema_version"),
        "evidence_id": evidence_id,
        "evidence_status": evidence.get("status"),
        "evidence_fingerprint": fingerprint,
        "evidence_title": evidence.get("title"),
        "source_component": evidence.get("source_component"),
        "provenance": evidence.get("provenance") or {},
        "attached_at": attached_at.isoformat(),
    }


def _canonical_evidence_ref_valid(ref: Mapping[str, Any]) -> bool:
    return bool(
        ref.get("evidence_schema_version") == EVIDENCE_SCHEMA_VERSION
        and ref.get("evidence_id")
        and ref.get("evidence_fingerprint")
    )


def _validate_classification(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_CLASSIFICATIONS:
        raise ValueError(f"Unsupported claim classification: {value}")
    return normalized


def _validate_status(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"Unsupported claim status: {value}")
    return normalized


def _validate_confidence(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_CONFIDENCE:
        raise ValueError(f"Unsupported claim confidence: {value}")
    return normalized


def _claim_id(investigation_id: str, statement: str, timestamp: datetime) -> str:
    seed = json.dumps({"investigation_id": investigation_id, "statement": statement, "created_at": timestamp.isoformat()}, sort_keys=True)
    return f"claim_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}"


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
