from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_claims_engine import attach_contradicting_evidence, attach_supporting_evidence, create_claim
from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import attach_supporting_claim, create_conclusion
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import attach_evidence_reference, create_evidence
from mgc_v05l.execution_core.track_b_canonical_reference_envelope import (
    build_provenance_envelope,
    build_reference,
    publish_ie5_artifacts,
    validate_reference,
    verify_research_chain,
)


NOW = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)


def test_create_canonical_reference() -> None:
    ref = build_reference(reference_type="unit", target_id="ev1", target_kind="EVIDENCE", relationship="supports", created_at=NOW)

    assert ref["schema_version"] == "ie5_canonical_reference_v1"
    assert ref["target_kind"] == "EVIDENCE"
    assert ref["relationship"] == "supports"
    assert validate_reference(ref)["valid"] is True


def test_create_canonical_provenance_envelope() -> None:
    provenance = build_provenance_envelope(source_component="unit", parent_fingerprint="abc", parent_schema_version="schema", created_at=NOW)

    assert provenance["schema_version"] == "ie5_canonical_provenance_envelope_v1"
    assert provenance["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}


def test_evidence_attachment_emits_canonical_reference(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)

    updated = attach_evidence_reference(evidence, attachment_type="saved_query", reference_id="query_1", now=NOW)

    ref = updated["attachments"][0]
    assert ref["target_kind"] == "SAVED_QUERY"
    assert ref["relationship"] == "attached_to"
    assert ref["target_id"] == "query_1"


def test_claim_support_contradiction_references_preserve_target_fingerprint(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    claim = _claim(tmp_path)

    supported = attach_supporting_evidence(claim, evidence, now=NOW)
    contradicted = attach_contradicting_evidence(claim, evidence, now=NOW)

    assert supported["supporting_evidence"][0]["target_fingerprint"] == evidence["deterministic_fingerprint"]
    assert contradicted["contradicting_evidence"][0]["target_fingerprint"] == evidence["deterministic_fingerprint"]
    assert supported["supporting_evidence"][0]["target_kind"] == "EVIDENCE"


def test_conclusion_claim_references_preserve_target_fingerprint(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    claim = attach_supporting_evidence(_claim(tmp_path), evidence, now=NOW)
    conclusion = _conclusion(tmp_path)

    updated = attach_supporting_claim(conclusion, claim, now=NOW)

    assert updated["supporting_claims"][0]["target_fingerprint"] == claim["deterministic_fingerprint"]
    assert updated["supporting_claims"][0]["target_kind"] == "CLAIM"


def test_chain_verifier_detects_valid_chain(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    claim = attach_supporting_evidence(_claim(tmp_path), evidence, now=NOW)
    conclusion = attach_supporting_claim(_conclusion(tmp_path), claim, now=NOW)

    result = verify_research_chain(conclusion, claims_by_id={claim["claim_id"]: claim}, evidence_by_id={evidence["evidence_id"]: evidence})

    assert result["verification_status"] == "VERIFIED"
    assert result["valid_chain_count"] == 1


def test_chain_verifier_detects_missing_target(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    claim = attach_supporting_evidence(_claim(tmp_path), evidence, now=NOW)
    conclusion = attach_supporting_claim(_conclusion(tmp_path), claim, now=NOW)

    result = verify_research_chain(conclusion, claims_by_id={}, evidence_by_id={})

    assert result["verification_status"] == "INCOMPLETE"
    assert result["missing_target_count"] == 1


def test_chain_verifier_detects_fingerprint_mismatch(tmp_path: Path) -> None:
    evidence = _evidence(tmp_path)
    claim = attach_supporting_evidence(_claim(tmp_path), evidence, now=NOW)
    conclusion = attach_supporting_claim(_conclusion(tmp_path), claim, now=NOW)
    claim = dict(claim, deterministic_fingerprint="changed")

    result = verify_research_chain(conclusion, claims_by_id={claim["claim_id"]: claim}, evidence_by_id={evidence["evidence_id"]: evidence})

    assert result["verification_status"] == "BROKEN"
    assert result["fingerprint_mismatch_count"] == 1


def test_guardrails_preserved() -> None:
    ref = build_reference(reference_type="unit", target_id="ev1", target_kind="EVIDENCE", created_at=NOW)

    assert ref["provenance"]["guardrails"]["diagnostic_only"] is True
    assert ref["provenance"]["guardrails"]["production_recommendation"] is False
    assert ref["provenance"]["guardrails"]["trading_gate"] is False


def test_publish_ie5_artifacts(tmp_path: Path) -> None:
    paths = publish_ie5_artifacts(output_dir=tmp_path)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_verification"]).read_text(encoding="utf-8"))


def test_reference_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_reference_envelope.py"),
        Path("src/mgc_v05l/app/track_b_canonical_reference_envelope.py"),
    ]
    forbidden_import_roots = ("mgc_v05l.execution.", "mgc_v05l.strategy", "mgc_v05l.app.ibkr", "ibapi", "ib_insync")
    forbidden_call_names = {"submit", "cancel", "modify", "placeOrder", "create_order_intent", "mutate_lifecycle", "flatten", "global_cancel"}
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(forbidden_import_roots):
                        violations.append(f"{path}:{alias.name}")
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module.startswith(forbidden_import_roots):
                    violations.append(f"{path}:{node.module}")
            elif isinstance(node, ast.Call):
                call_name = node.func.id if isinstance(node.func, ast.Name) else node.func.attr if isinstance(node.func, ast.Attribute) else None
                if call_name in forbidden_call_names:
                    violations.append(f"{path}:{call_name}")
    assert violations == []


def _evidence(tmp_path: Path):
    return create_evidence(
        investigation_id="inv",
        evidence_id="ev1",
        title="Evidence",
        summary="Evidence",
        evidence_classification="ANALYTICS",
        evidence_type="result",
        source_component="unit",
        now=NOW,
        output_dir=tmp_path / "evidence",
    ).evidence


def _claim(tmp_path: Path):
    return create_claim(
        investigation_id="inv",
        claim_id="claim1",
        title="Claim",
        statement="Statement",
        rationale="Rationale",
        claim_classification="ANALYTICS",
        claim_type="assertion",
        confidence="MEDIUM",
        status="ACTIVE",
        now=NOW,
        output_dir=tmp_path / "claims",
    ).claim


def _conclusion(tmp_path: Path):
    return create_conclusion(
        investigation_id="inv",
        conclusion_id="conclusion1",
        title="Conclusion",
        hypothesis="Hypothesis",
        conclusion="Conclusion",
        rationale="Rationale",
        conclusion_classification="RESEARCH_SUMMARY",
        now=NOW,
        output_dir=tmp_path / "conclusions",
    ).conclusion
