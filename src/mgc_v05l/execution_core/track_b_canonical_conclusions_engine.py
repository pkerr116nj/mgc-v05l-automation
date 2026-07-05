"""Canonical Conclusions Engine for deterministic investigation outcomes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_canonical_reference_envelope import build_reference


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "investigation_engine" / "conclusions"

CONCLUSION_SCHEMA_VERSION = "ie4_canonical_conclusion_v1"
CONCLUSION_SUMMARY_SCHEMA_VERSION = "ie4_canonical_conclusion_summary_v1"
CLAIM_SCHEMA_VERSION = "ie3_canonical_claim_v1"

CONTRACT_MD = "ie4_conclusion_contract.md"
SCHEMA_JSON = "ie4_conclusion_schema.json"
SAMPLE_CONCLUSION_JSON = "ie4_sample_conclusion.json"
VALIDATION_CONTRACT_MD = "ie4_conclusion_validation_contract.md"
SUMMARY_MD = "ie4_conclusion_summary.md"
RESEARCH_CHAIN_CONTRACT_MD = "ie4_research_chain_contract.md"

VALID_CLASSIFICATIONS = {"ANALYTICS", "DISCOVERY", "MARKET_STRUCTURE", "CONTEXT", "OPERATIONAL", "CANDIDATE", "VALIDATION", "RESEARCH_SUMMARY"}
VALID_STATUSES = {"DRAFT", "ACTIVE", "SUPERSEDED", "INVALIDATED", "ARCHIVED"}
VALID_OUTCOMES = {"NO_CONCLUSION", "SUPPORTED", "PARTIALLY_SUPPORTED", "CONTRADICTED", "INCONCLUSIVE", "NEEDS_MORE_EVIDENCE"}
VALID_CONFIDENCE = {"UNKNOWN", "LOW", "MEDIUM", "HIGH"}


@dataclass(frozen=True)
class CanonicalConclusionResult:
    conclusion: dict[str, Any]
    conclusion_path: Path
    summary: dict[str, Any]
    summary_path: Path
    output_dir: Path


def create_conclusion(
    *,
    investigation_id: str,
    title: str,
    hypothesis: str,
    conclusion: str,
    rationale: str,
    conclusion_classification: str,
    confidence: str = "UNKNOWN",
    provenance: Mapping[str, Any] | None = None,
    conclusion_id: str | None = None,
    status: str = "DRAFT",
    now: datetime | str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> CanonicalConclusionResult:
    timestamp = _coerce_now(now)
    payload = {
        "schema_version": CONCLUSION_SCHEMA_VERSION,
        "conclusion_id": conclusion_id or _conclusion_id(investigation_id, hypothesis, timestamp),
        "investigation_id": investigation_id,
        "created_at": timestamp.isoformat(),
        "updated_at": timestamp.isoformat(),
        "conclusion_classification": _validate_classification(conclusion_classification),
        "status": _validate_status(status),
        "outcome": "NO_CONCLUSION",
        "confidence": _validate_confidence(confidence),
        "title": title,
        "hypothesis": hypothesis,
        "conclusion": conclusion,
        "rationale": rationale,
        "provenance": dict(provenance or {"source": "canonical_conclusions_engine", "operation": "create"}),
        "supporting_claims": [],
        "contradicting_claims": [],
        "related_claims": [],
        "validation": {"outcome": "NO_CONCLUSION", "reasons": ["no_claims_attached"]},
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    payload["deterministic_fingerprint"] = conclusion_fingerprint(payload)
    return write_conclusion(payload, output_dir=output_dir)


def attach_supporting_claim(conclusion: Mapping[str, Any], claim: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    return _attach_claim(conclusion, claim, bucket="supporting_claims", now=now)


def attach_contradicting_claim(conclusion: Mapping[str, Any], claim: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    return _attach_claim(conclusion, claim, bucket="contradicting_claims", now=now)


def attach_related_claim(conclusion: Mapping[str, Any], claim: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    return _attach_claim(conclusion, claim, bucket="related_claims", now=now)


def validate_conclusion_outcome(conclusion: Mapping[str, Any]) -> dict[str, Any]:
    supporting = list(conclusion.get("supporting_claims") or [])
    contradicting = list(conclusion.get("contradicting_claims") or [])
    related = list(conclusion.get("related_claims") or [])
    all_refs = supporting + contradicting + related
    reasons: list[str] = []
    invalid_refs = [ref for ref in all_refs if not _canonical_claim_ref_valid(ref)]
    if invalid_refs:
        return _validation("NO_CONCLUSION", "UNKNOWN", ["invalid_claim_reference"], supporting, contradicting, related)
    if not supporting and not contradicting:
        return _validation("NO_CONCLUSION", "UNKNOWN", ["no_claims_attached"], supporting, contradicting, related)
    supported_quality = [_claim_quality(ref) for ref in supporting]
    contradicting_quality = [_claim_quality(ref) for ref in contradicting]
    strong_support = sum(1 for quality in supported_quality if quality == "strong")
    weak_support = sum(1 for quality in supported_quality if quality == "weak")
    insufficient_support = sum(1 for quality in supported_quality if quality == "insufficient")
    strong_contradiction = sum(1 for quality in contradicting_quality if quality == "strong")
    weak_contradiction = sum(1 for quality in contradicting_quality if quality == "weak")
    if strong_contradiction > strong_support:
        reasons.append("contradicting_claims_dominate")
        return _validation("CONTRADICTED", "MEDIUM", reasons, supporting, contradicting, related)
    if supporting and contradicting:
        if strong_support > strong_contradiction + weak_contradiction:
            reasons.append("supported_claims_dominate_with_contradictions_present")
            return _validation("PARTIALLY_SUPPORTED", "LOW", reasons, supporting, contradicting, related)
        reasons.append("supporting_and_contradicting_claims_present")
        return _validation("INCONCLUSIVE", "LOW", reasons, supporting, contradicting, related)
    if strong_support:
        reasons.append("supported_claims_without_contradictions")
        return _validation("SUPPORTED", _confidence_from_claims(supporting), reasons, supporting, contradicting, related)
    if weak_support:
        reasons.append("partially_supported_claims_without_contradictions")
        return _validation("PARTIALLY_SUPPORTED", "LOW", reasons, supporting, contradicting, related)
    if insufficient_support:
        reasons.append("supporting_claims_insufficient")
        return _validation("NEEDS_MORE_EVIDENCE", "UNKNOWN", reasons, supporting, contradicting, related)
    reasons.append("claims_do_not_support_conclusion")
    return _validation("NEEDS_MORE_EVIDENCE", "UNKNOWN", reasons, supporting, contradicting, related)


def refresh_conclusion_validation(conclusion: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    updated = dict(conclusion)
    validation = validate_conclusion_outcome(updated)
    updated["validation"] = validation
    updated["outcome"] = validation["outcome"]
    updated["confidence"] = validation["confidence"]
    updated["updated_at"] = timestamp.isoformat()
    updated["deterministic_fingerprint"] = conclusion_fingerprint(updated)
    return updated


def write_conclusion(conclusion: Mapping[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> CanonicalConclusionResult:
    validate_conclusion(conclusion)
    output_dir.mkdir(parents=True, exist_ok=True)
    payload = dict(conclusion)
    validation = validate_conclusion_outcome(payload)
    payload["validation"] = validation
    payload["outcome"] = validation["outcome"]
    payload["confidence"] = validation["confidence"]
    payload["deterministic_fingerprint"] = conclusion_fingerprint(payload)
    conclusion_id = str(payload["conclusion_id"])
    conclusion_path = output_dir / f"{conclusion_id}.json"
    conclusion_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = summarize_conclusion(payload)
    summary_path = output_dir / f"{conclusion_id}.summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CanonicalConclusionResult(conclusion=payload, conclusion_path=conclusion_path, summary=summary, summary_path=summary_path, output_dir=output_dir)


def load_conclusion(conclusion_id: str, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    return json.loads((output_dir / f"{conclusion_id}.json").read_text(encoding="utf-8"))


def list_conclusions(*, output_dir: Path = DEFAULT_OUTPUT_DIR, investigation_id: str | None = None) -> list[dict[str, Any]]:
    if not output_dir.exists():
        return []
    rows = []
    for path in sorted(output_dir.glob("*.json")):
        if path.name.endswith(".summary.json") or path.name.startswith("ie4_"):
            continue
        try:
            conclusion = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if conclusion.get("schema_version") != CONCLUSION_SCHEMA_VERSION:
            continue
        if investigation_id and conclusion.get("investigation_id") != investigation_id:
            continue
        rows.append(summarize_conclusion(conclusion))
    return rows


def validate_conclusion(conclusion: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if conclusion.get("schema_version") != CONCLUSION_SCHEMA_VERSION:
        errors.append("invalid_schema_version")
    for key in ("conclusion_id", "investigation_id", "created_at", "updated_at", "title", "hypothesis", "conclusion", "rationale"):
        if not conclusion.get(key):
            errors.append(f"missing_{key}")
    if conclusion.get("conclusion_classification") not in VALID_CLASSIFICATIONS:
        errors.append("invalid_conclusion_classification")
    if conclusion.get("status") not in VALID_STATUSES:
        errors.append("invalid_status")
    if conclusion.get("outcome") not in VALID_OUTCOMES:
        errors.append("invalid_outcome")
    if conclusion.get("confidence") not in VALID_CONFIDENCE:
        errors.append("invalid_confidence")
    if conclusion.get("diagnostic_only") is not True:
        errors.append("diagnostic_only_not_true")
    if conclusion.get("production_recommendation") is not False:
        errors.append("production_recommendation_not_false")
    if conclusion.get("trading_gate") is not False:
        errors.append("trading_gate_not_false")
    for bucket in ("supporting_claims", "contradicting_claims", "related_claims"):
        for ref in conclusion.get(bucket) or []:
            if not _canonical_claim_ref_valid(ref):
                errors.append(f"invalid_{bucket}_reference")
    if errors:
        raise ValueError(";".join(errors))
    return {"valid": True, "errors": []}


def summarize_conclusion(conclusion: Mapping[str, Any]) -> dict[str, Any]:
    validation = validate_conclusion_outcome(conclusion)
    summary = {
        "schema_version": CONCLUSION_SUMMARY_SCHEMA_VERSION,
        "conclusion_id": conclusion.get("conclusion_id"),
        "investigation_id": conclusion.get("investigation_id"),
        "hypothesis": conclusion.get("hypothesis"),
        "outcome": validation.get("outcome"),
        "confidence": validation.get("confidence"),
        "supporting_claim_count": validation.get("supporting_claim_count"),
        "contradicting_claim_count": validation.get("contradicting_claim_count"),
        "related_claim_count": validation.get("related_claim_count"),
        "diagnostic_only": conclusion.get("diagnostic_only") is True,
        "production_recommendation": conclusion.get("production_recommendation") is True,
        "trading_gate": conclusion.get("trading_gate") is True,
    }
    summary["deterministic_fingerprint"] = conclusion_fingerprint(summary)
    return summary


def conclusion_fingerprint(payload: Mapping[str, Any]) -> str:
    normalized = _strip_volatile(payload)
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def publish_ie4_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    result = create_conclusion(
        investigation_id="sample_investigation",
        conclusion_id="sample_conclusion",
        title="Sample research conclusion",
        hypothesis="Strategy expectancy may vary by context.",
        conclusion="No deterministic conclusion is available until supported claims are attached.",
        rationale="IE4 conclusions consume claims only.",
        conclusion_classification="RESEARCH_SUMMARY",
        now=now or "2026-07-05T12:00:00+00:00",
        output_dir=output_dir,
    )
    paths = {
        "contract": output_dir / CONTRACT_MD,
        "schema": output_dir / SCHEMA_JSON,
        "sample": output_dir / SAMPLE_CONCLUSION_JSON,
        "validation_contract": output_dir / VALIDATION_CONTRACT_MD,
        "summary": output_dir / SUMMARY_MD,
        "research_chain_contract": output_dir / RESEARCH_CHAIN_CONTRACT_MD,
    }
    paths["contract"].write_text(render_conclusion_contract(), encoding="utf-8")
    paths["schema"].write_text(json.dumps(conclusion_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample"].write_text(json.dumps(result.conclusion, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["validation_contract"].write_text(render_conclusion_validation_contract(), encoding="utf-8")
    paths["summary"].write_text(render_conclusion_summary(result.summary), encoding="utf-8")
    paths["research_chain_contract"].write_text(render_research_chain_contract(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def render_conclusion_contract() -> str:
    return "\n".join([
        "# IE4 Canonical Conclusion Contract",
        "",
        f"- Schema version: `{CONCLUSION_SCHEMA_VERSION}`",
        "- Conclusions are deterministic research outcomes assembled from Canonical Claims.",
        "- Conclusions do not bypass Claims and do not infer from raw Evidence directly.",
        "- Conclusions are not trading recommendations, production promotions, or gates.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_conclusion_validation_contract() -> str:
    return "\n".join([
        "# IE4 Conclusion Validation Contract",
        "",
        "- `NO_CONCLUSION`: no claims are attached.",
        "- `SUPPORTED`: supported claims exist without contradictions.",
        "- `PARTIALLY_SUPPORTED`: supporting claims exist but quality is partial or contradictions are dominated.",
        "- `CONTRADICTED`: contradicting claims dominate supported claims.",
        "- `INCONCLUSIVE`: supported and contradicting claims are both present without deterministic dominance.",
        "- `NEEDS_MORE_EVIDENCE`: attached claims are mostly insufficient.",
        "",
    ])


def render_conclusion_summary(summary: Mapping[str, Any]) -> str:
    return "\n".join([
        "# IE4 Conclusion Summary",
        "",
        f"- Conclusion: `{summary.get('conclusion_id')}`",
        f"- Investigation: `{summary.get('investigation_id')}`",
        f"- Outcome: `{summary.get('outcome')}`",
        f"- Confidence: `{summary.get('confidence')}`",
        f"- Supporting claims: `{summary.get('supporting_claim_count')}`",
        f"- Contradicting claims: `{summary.get('contradicting_claim_count')}`",
        f"- Fingerprint: `{summary.get('deterministic_fingerprint')}`",
        "",
    ])


def render_research_chain_contract() -> str:
    return "\n".join([
        "# IE4 Research Chain Contract",
        "",
        "Investigation -> Evidence -> Claims -> Conclusions",
        "",
        "- Evidence captures deterministic facts.",
        "- Claims consume Evidence and record supported or contradicted research assertions.",
        "- Conclusions consume Claims and record deterministic research outcomes.",
        "- No layer in the chain creates trading recommendations, production recommendations, broker instructions, or gates.",
        "",
    ])


def conclusion_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalConclusion",
        "type": "object",
        "required": [
            "schema_version",
            "conclusion_id",
            "investigation_id",
            "created_at",
            "updated_at",
            "conclusion_classification",
            "status",
            "outcome",
            "confidence",
            "title",
            "hypothesis",
            "conclusion",
            "rationale",
            "provenance",
            "supporting_claims",
            "contradicting_claims",
            "related_claims",
            "diagnostic_only",
            "production_recommendation",
            "trading_gate",
        ],
        "properties": {
            "schema_version": {"const": CONCLUSION_SCHEMA_VERSION},
            "conclusion_classification": {"enum": sorted(VALID_CLASSIFICATIONS)},
            "status": {"enum": sorted(VALID_STATUSES)},
            "outcome": {"enum": sorted(VALID_OUTCOMES)},
            "confidence": {"enum": sorted(VALID_CONFIDENCE)},
            "diagnostic_only": {"const": True},
            "production_recommendation": {"const": False},
            "trading_gate": {"const": False},
        },
    }


def _attach_claim(conclusion: Mapping[str, Any], claim: Mapping[str, Any], *, bucket: str, now: datetime | str | None) -> dict[str, Any]:
    ref = _claim_reference(claim, attached_at=_coerce_now(now), bucket=bucket)
    updated = dict(conclusion)
    rows = [dict(item) for item in updated.get(bucket) or []]
    rows.append(ref)
    updated[bucket] = sorted(rows, key=lambda row: (str(row.get("claim_id")), str(row.get("attached_at"))))
    updated["updated_at"] = ref["attached_at"]
    validation = validate_conclusion_outcome(updated)
    updated["validation"] = validation
    updated["outcome"] = validation["outcome"]
    updated["confidence"] = validation["confidence"]
    updated["deterministic_fingerprint"] = conclusion_fingerprint(updated)
    return updated


def _claim_reference(claim: Mapping[str, Any], *, attached_at: datetime, bucket: str) -> dict[str, Any]:
    if claim.get("schema_version") != CLAIM_SCHEMA_VERSION:
        raise ValueError("Conclusion claim references must come from CanonicalClaim")
    if not claim.get("claim_id"):
        raise ValueError("CanonicalClaim reference missing claim_id")
    if not claim.get("deterministic_fingerprint"):
        raise ValueError("CanonicalClaim reference missing deterministic_fingerprint")
    validation = claim.get("validation") or {}
    relationship = {"supporting_claims": "supports", "contradicting_claims": "contradicts", "related_claims": "related_to"}[bucket]
    return {
        **build_reference(
            reference_type="conclusion_claim",
            target_id=str(claim.get("claim_id")),
            target_kind="CLAIM",
            target_schema_version=str(claim.get("schema_version")),
            target_fingerprint=str(claim.get("deterministic_fingerprint")),
            relationship=relationship,
            source_component="canonical_conclusions_engine",
            created_at=attached_at,
        ),
        "claim_schema_version": claim.get("schema_version"),
        "claim_id": claim.get("claim_id"),
        "claim_status": claim.get("status"),
        "claim_validation_status": validation.get("status"),
        "claim_confidence": claim.get("confidence"),
        "claim_fingerprint": claim.get("deterministic_fingerprint"),
        "claim_title": claim.get("title"),
        "source_provenance": claim.get("provenance") or {},
        "attached_at": attached_at.isoformat(),
    }


def _canonical_claim_ref_valid(ref: Mapping[str, Any]) -> bool:
    return bool(
        (ref.get("claim_schema_version") == CLAIM_SCHEMA_VERSION or ref.get("target_schema_version") == CLAIM_SCHEMA_VERSION)
        and (ref.get("claim_id") or ref.get("target_id"))
        and (ref.get("claim_fingerprint") or ref.get("target_fingerprint"))
    )


def _claim_quality(ref: Mapping[str, Any]) -> str:
    if ref.get("claim_status") != "ACTIVE":
        return "weak"
    validation_status = ref.get("claim_validation_status")
    if validation_status == "SUPPORTED" and ref.get("claim_fingerprint") and ref.get("provenance"):
        return "strong"
    if validation_status == "PARTIALLY_SUPPORTED":
        return "weak"
    if validation_status == "CONTRADICTED":
        return "strong"
    return "insufficient"


def _confidence_from_claims(refs: list[Mapping[str, Any]]) -> str:
    rank = {"UNKNOWN": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3}
    best = max((rank.get(str(ref.get("claim_confidence") or "UNKNOWN"), 0) for ref in refs), default=0)
    if best >= 3:
        return "HIGH"
    if best == 2:
        return "MEDIUM"
    if best == 1:
        return "LOW"
    return "UNKNOWN"


def _validation(outcome: str, confidence: str, reasons: list[str], supporting: list[Any], contradicting: list[Any], related: list[Any]) -> dict[str, Any]:
    return {
        "outcome": outcome,
        "confidence": confidence,
        "reasons": reasons,
        "supporting_claim_count": len(supporting),
        "contradicting_claim_count": len(contradicting),
        "related_claim_count": len(related),
    }


def _validate_classification(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_CLASSIFICATIONS:
        raise ValueError(f"Unsupported conclusion classification: {value}")
    return normalized


def _validate_status(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_STATUSES:
        raise ValueError(f"Unsupported conclusion status: {value}")
    return normalized


def _validate_confidence(value: str) -> str:
    normalized = value.upper()
    if normalized not in VALID_CONFIDENCE:
        raise ValueError(f"Unsupported conclusion confidence: {value}")
    return normalized


def _conclusion_id(investigation_id: str, hypothesis: str, timestamp: datetime) -> str:
    seed = json.dumps({"investigation_id": investigation_id, "hypothesis": hypothesis, "created_at": timestamp.isoformat()}, sort_keys=True)
    return f"conclusion_{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}"


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
