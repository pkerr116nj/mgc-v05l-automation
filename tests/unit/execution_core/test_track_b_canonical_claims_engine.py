from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_claims_engine import (
    attach_contradicting_evidence,
    attach_supporting_evidence,
    claim_fingerprint,
    create_claim,
    publish_ie3_artifacts,
    summarize_claim,
    validate_claim_support,
)
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import create_evidence, update_evidence_status
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import attach_claim_reference, create_investigation


NOW = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)


def test_create_claim(tmp_path: Path) -> None:
    result = _create_claim(tmp_path)

    assert result.claim["schema_version"] == "ie3_canonical_claim_v1"
    assert result.claim["claim_classification"] == "ANALYTICS"
    assert result.claim["status"] == "DRAFT"
    assert result.claim["confidence"] == "LOW"
    assert result.claim_path.exists()


def test_attach_supporting_evidence(tmp_path: Path) -> None:
    claim = _create_claim(tmp_path).claim
    evidence = _create_evidence(tmp_path).evidence

    updated = attach_supporting_evidence(claim, evidence, now="2026-07-05T12:01:00Z")

    assert updated["supporting_evidence"][0]["evidence_id"] == "evidence_1"
    assert summarize_claim(updated)["supporting_evidence_count"] == 1


def test_attach_contradicting_evidence(tmp_path: Path) -> None:
    claim = _create_claim(tmp_path).claim
    evidence = _create_evidence(tmp_path).evidence

    updated = attach_contradicting_evidence(claim, evidence, now="2026-07-05T12:01:00Z")

    assert updated["contradicting_evidence"][0]["evidence_id"] == "evidence_1"
    assert summarize_claim(updated)["contradicting_evidence_count"] == 1


def test_validation_status_supported(tmp_path: Path) -> None:
    claim = attach_supporting_evidence(_create_claim(tmp_path).claim, _create_evidence(tmp_path).evidence, now=NOW)

    assert validate_claim_support(claim)["status"] == "SUPPORTED"


def test_validation_status_partially_supported(tmp_path: Path) -> None:
    inactive = update_evidence_status(_create_evidence(tmp_path).evidence, status="ARCHIVED", now=NOW)
    claim = attach_supporting_evidence(_create_claim(tmp_path).claim, inactive, now=NOW)

    assert validate_claim_support(claim)["status"] == "PARTIALLY_SUPPORTED"


def test_validation_status_contradicted(tmp_path: Path) -> None:
    claim = attach_contradicting_evidence(_create_claim(tmp_path).claim, _create_evidence(tmp_path).evidence, now=NOW)

    assert validate_claim_support(claim)["status"] == "CONTRADICTED"


def test_validation_status_insufficient_evidence(tmp_path: Path) -> None:
    claim = _create_claim(tmp_path).claim

    assert validate_claim_support(claim)["status"] == "INSUFFICIENT_EVIDENCE"


def test_deterministic_fingerprint_stability() -> None:
    payload = {"claim_id": "claim", "statement": "statement", "updated_at": "2026-07-05T12:00:00Z", "deterministic_fingerprint": "old"}
    changed_time = dict(payload, updated_at="2026-07-05T13:00:00Z")

    assert claim_fingerprint(payload) == claim_fingerprint(changed_time)


def test_investigation_attachment(tmp_path: Path) -> None:
    investigation = create_investigation(
        title="Claim investigation",
        description="Test",
        hypothesis="Claims can be attached.",
        investigation_id="inv_claim",
        now=NOW,
        output_dir=tmp_path / "investigations",
    ).investigation

    updated = attach_claim_reference(investigation, claim_id="claim_1", artifact_path="claims/claim_1.json", now="2026-07-05T12:01:00Z")

    assert updated["references"][0]["reference_type"] == "claim"
    assert updated["timeline"][-1]["event_type"] == "CLAIM_CREATED"


def test_timeline_ordering(tmp_path: Path) -> None:
    investigation = create_investigation(
        title="Claim investigation",
        description="Test",
        hypothesis="Claim lifecycle events sort.",
        investigation_id="inv_claim_order",
        now=NOW,
        output_dir=tmp_path / "investigations",
    ).investigation

    updated = attach_claim_reference(investigation, claim_id="late", status_event="CLAIM_VALIDATED", now="2026-07-05T12:03:00Z")
    updated = attach_claim_reference(updated, claim_id="early", status_event="CLAIM_UPDATED", now="2026-07-05T12:02:00Z")

    timestamps = [event["timestamp"] for event in updated["timeline"]]
    assert timestamps == sorted(timestamps)


def test_guardrails_preserved(tmp_path: Path) -> None:
    summary = _create_claim(tmp_path).summary

    assert summary["diagnostic_only"] is True
    assert summary["production_recommendation"] is False
    assert summary["trading_gate"] is False


def test_publish_ie3_artifacts(tmp_path: Path) -> None:
    paths = publish_ie3_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample"]).read_text(encoding="utf-8"))


def test_claim_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_claims_engine.py"),
        Path("src/mgc_v05l/app/track_b_canonical_claims_engine.py"),
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


def _create_claim(tmp_path: Path):
    return create_claim(
        investigation_id="sample_investigation",
        claim_id="claim_1",
        title="Claim",
        statement="A deterministic relationship deserves review.",
        rationale="The statement is test-backed.",
        claim_classification="ANALYTICS",
        claim_type="research_assertion",
        confidence="LOW",
        now=NOW,
        output_dir=tmp_path / "claims",
    )


def _create_evidence(tmp_path: Path):
    return create_evidence(
        investigation_id="sample_investigation",
        evidence_id="evidence_1",
        title="Evidence",
        summary="Evidence summary",
        evidence_classification="ANALYTICS",
        evidence_type="saved_query_result",
        source_component="canonical_analytics_engine",
        now=NOW,
        output_dir=tmp_path / "evidence",
    )
