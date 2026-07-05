from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_claims_engine import attach_contradicting_evidence, attach_supporting_evidence, create_claim
from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import (
    attach_contradicting_claim,
    attach_supporting_claim,
    conclusion_fingerprint,
    create_conclusion,
    publish_ie4_artifacts,
    summarize_conclusion,
    validate_conclusion_outcome,
)
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import create_evidence, update_evidence_status
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import attach_conclusion_reference, create_investigation


NOW = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)


def test_create_conclusion(tmp_path: Path) -> None:
    result = _create_conclusion(tmp_path)

    assert result.conclusion["schema_version"] == "ie4_canonical_conclusion_v1"
    assert result.conclusion["conclusion_classification"] == "RESEARCH_SUMMARY"
    assert result.conclusion["status"] == "DRAFT"
    assert result.conclusion_path.exists()


def test_attach_supporting_claim(tmp_path: Path) -> None:
    conclusion = _create_conclusion(tmp_path).conclusion
    claim = _supported_claim(tmp_path)

    updated = attach_supporting_claim(conclusion, claim, now=NOW)

    assert updated["supporting_claims"][0]["claim_id"] == "claim_supported"
    assert summarize_conclusion(updated)["supporting_claim_count"] == 1


def test_attach_contradicting_claim(tmp_path: Path) -> None:
    conclusion = _create_conclusion(tmp_path).conclusion
    claim = _supported_claim(tmp_path, claim_id="claim_contra")

    updated = attach_contradicting_claim(conclusion, claim, now=NOW)

    assert updated["contradicting_claims"][0]["claim_id"] == "claim_contra"
    assert summarize_conclusion(updated)["contradicting_claim_count"] == 1


def test_outcome_no_conclusion_needs_more_evidence(tmp_path: Path) -> None:
    conclusion = _create_conclusion(tmp_path).conclusion

    validation = validate_conclusion_outcome(conclusion)

    assert validation["outcome"] == "NO_CONCLUSION"


def test_outcome_supported(tmp_path: Path) -> None:
    conclusion = attach_supporting_claim(_create_conclusion(tmp_path).conclusion, _supported_claim(tmp_path), now=NOW)

    assert validate_conclusion_outcome(conclusion)["outcome"] == "SUPPORTED"


def test_outcome_partially_supported(tmp_path: Path) -> None:
    conclusion = attach_supporting_claim(_create_conclusion(tmp_path).conclusion, _partial_claim(tmp_path), now=NOW)

    assert validate_conclusion_outcome(conclusion)["outcome"] == "PARTIALLY_SUPPORTED"


def test_outcome_contradicted(tmp_path: Path) -> None:
    conclusion = attach_contradicting_claim(_create_conclusion(tmp_path).conclusion, _supported_claim(tmp_path), now=NOW)

    assert validate_conclusion_outcome(conclusion)["outcome"] == "CONTRADICTED"


def test_outcome_inconclusive(tmp_path: Path) -> None:
    conclusion = _create_conclusion(tmp_path).conclusion
    conclusion = attach_supporting_claim(conclusion, _supported_claim(tmp_path), now=NOW)
    conclusion = attach_contradicting_claim(conclusion, _supported_claim(tmp_path, claim_id="claim_other"), now=NOW)

    assert validate_conclusion_outcome(conclusion)["outcome"] == "INCONCLUSIVE"


def test_deterministic_fingerprint_stability() -> None:
    payload = {"conclusion_id": "conclusion", "hypothesis": "hypothesis", "updated_at": "2026-07-05T12:00:00Z", "deterministic_fingerprint": "old"}
    changed_time = dict(payload, updated_at="2026-07-05T13:00:00Z")

    assert conclusion_fingerprint(payload) == conclusion_fingerprint(changed_time)


def test_investigation_attachment(tmp_path: Path) -> None:
    investigation = create_investigation(
        title="Conclusion investigation",
        description="Test",
        hypothesis="Conclusions can be attached.",
        investigation_id="inv_conclusion",
        now=NOW,
        output_dir=tmp_path / "investigations",
    ).investigation

    updated = attach_conclusion_reference(investigation, conclusion_id="conclusion_1", artifact_path="conclusions/conclusion_1.json", now="2026-07-05T12:01:00Z")

    assert updated["references"][0]["reference_type"] == "conclusion"
    assert updated["timeline"][-1]["event_type"] == "CONCLUSION_CREATED"


def test_timeline_ordering(tmp_path: Path) -> None:
    investigation = create_investigation(
        title="Conclusion investigation",
        description="Test",
        hypothesis="Conclusion lifecycle events sort.",
        investigation_id="inv_conclusion_order",
        now=NOW,
        output_dir=tmp_path / "investigations",
    ).investigation

    updated = attach_conclusion_reference(investigation, conclusion_id="late", status_event="CONCLUSION_VALIDATED", now="2026-07-05T12:03:00Z")
    updated = attach_conclusion_reference(updated, conclusion_id="early", status_event="CONCLUSION_SUPERSEDED", now="2026-07-05T12:02:00Z")

    timestamps = [event["timestamp"] for event in updated["timeline"]]
    assert timestamps == sorted(timestamps)


def test_guardrails_preserved(tmp_path: Path) -> None:
    summary = _create_conclusion(tmp_path).summary

    assert summary["diagnostic_only"] is True
    assert summary["production_recommendation"] is False
    assert summary["trading_gate"] is False


def test_publish_ie4_artifacts(tmp_path: Path) -> None:
    paths = publish_ie4_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample"]).read_text(encoding="utf-8"))


def test_conclusion_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_conclusions_engine.py"),
        Path("src/mgc_v05l/app/track_b_canonical_conclusions_engine.py"),
    ]
    forbidden_import_roots = (
        "mgc_v05l.execution.",
        "mgc_v05l.strategy",
        "mgc_v05l.app.ibkr",
        "ibapi",
        "ib_insync",
    )
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


def _create_conclusion(tmp_path: Path):
    return create_conclusion(
        investigation_id="sample_investigation",
        conclusion_id="conclusion_1",
        title="Conclusion",
        hypothesis="A deterministic relationship may be present.",
        conclusion="No conclusion yet.",
        rationale="Conclusion consumes claims only.",
        conclusion_classification="RESEARCH_SUMMARY",
        now=NOW,
        output_dir=tmp_path / "conclusions",
    )


def _supported_claim(tmp_path: Path, claim_id: str = "claim_supported"):
    claim = create_claim(
        investigation_id="sample_investigation",
        claim_id=claim_id,
        title="Supported claim",
        statement="Relationship is supported.",
        rationale="Backed by active evidence.",
        claim_classification="ANALYTICS",
        claim_type="research_assertion",
        confidence="MEDIUM",
        status="ACTIVE",
        now=NOW,
        output_dir=tmp_path / "claims",
    ).claim
    evidence = create_evidence(
        investigation_id="sample_investigation",
        evidence_id=f"evidence_{claim_id}",
        title="Evidence",
        summary="Evidence summary",
        evidence_classification="ANALYTICS",
        evidence_type="saved_query_result",
        source_component="canonical_analytics_engine",
        now=NOW,
        output_dir=tmp_path / "evidence",
    ).evidence
    return attach_supporting_evidence(claim, evidence, now=NOW)


def _partial_claim(tmp_path: Path):
    claim = create_claim(
        investigation_id="sample_investigation",
        claim_id="claim_partial",
        title="Partial claim",
        statement="Relationship is partially supported.",
        rationale="Backed by inactive evidence.",
        claim_classification="ANALYTICS",
        claim_type="research_assertion",
        confidence="LOW",
        status="ACTIVE",
        now=NOW,
        output_dir=tmp_path / "claims",
    ).claim
    evidence = create_evidence(
        investigation_id="sample_investigation",
        evidence_id="evidence_partial",
        title="Evidence",
        summary="Evidence summary",
        evidence_classification="ANALYTICS",
        evidence_type="saved_query_result",
        source_component="canonical_analytics_engine",
        now=NOW,
        output_dir=tmp_path / "evidence",
    ).evidence
    return attach_supporting_evidence(claim, update_evidence_status(evidence, status="ARCHIVED", now=NOW), now=NOW)
