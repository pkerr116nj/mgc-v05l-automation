from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_evidence_engine import (
    add_evidence_relationship,
    attach_evidence_reference,
    create_evidence,
    evidence_fingerprint,
    publish_ie2_artifacts,
    summarize_evidence,
    traverse_evidence_relationships,
    validate_evidence,
    write_evidence,
)
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import attach_evidence_reference as attach_evidence_to_investigation
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import create_investigation


NOW = datetime(2026, 7, 5, 12, 0, tzinfo=UTC)


def test_create_evidence(tmp_path: Path) -> None:
    result = _create_evidence(tmp_path)

    assert result.evidence["schema_version"] == "ie2_canonical_evidence_v1"
    assert result.evidence["evidence_classification"] == "ANALYTICS"
    assert result.evidence["status"] == "ACTIVE"
    assert result.evidence["diagnostic_only"] is True
    assert result.evidence_path.exists()
    assert result.summary_path.exists()


def test_attach_evidence_reference(tmp_path: Path) -> None:
    result = _create_evidence(tmp_path)

    updated = attach_evidence_reference(
        result.evidence,
        attachment_type="saved_query",
        reference_id="expectancy_by_strategy",
        artifact_path="saved_queries.jsonl",
        now="2026-07-05T12:01:00Z",
    )
    summary = summarize_evidence(updated)

    assert summary["attachment_count"] == 1
    assert updated["attachments"][0]["target_id"] == "expectancy_by_strategy"


def test_relationship_creation_and_traversal(tmp_path: Path) -> None:
    result = _create_evidence(tmp_path)

    updated = add_evidence_relationship(result.evidence, relationship_type="supports", target_evidence_id="evidence_other", now="2026-07-05T12:01:00Z")

    assert summarize_evidence(updated)["relationship_count"] == 1
    assert traverse_evidence_relationships(updated, relationship_type="supports") == ["evidence_other"]


def test_investigation_attachment(tmp_path: Path) -> None:
    investigation = create_investigation(
        title="Investigation",
        description="Test",
        hypothesis="Evidence can be attached.",
        investigation_id="inv_evidence",
        now=NOW,
        output_dir=tmp_path / "investigations",
    ).investigation

    updated = attach_evidence_to_investigation(
        investigation,
        evidence_id="evidence_1",
        artifact_path="evidence/evidence_1.json",
        now="2026-07-05T12:01:00Z",
    )

    assert updated["references"][0]["reference_type"] == "evidence"
    assert updated["timeline"][-1]["event_type"] == "EVIDENCE_ATTACHED"


def test_fingerprint_stability() -> None:
    payload = {
        "evidence_id": "evidence",
        "title": "Title",
        "updated_at": "2026-07-05T12:00:00Z",
        "deterministic_fingerprint": "old",
    }
    changed_time = dict(payload, updated_at="2026-07-05T13:00:00Z")

    assert evidence_fingerprint(payload) == evidence_fingerprint(changed_time)


def test_guardrails_preserved(tmp_path: Path) -> None:
    result = _create_evidence(tmp_path)
    validation = validate_evidence(result.evidence)

    assert validation["valid"] is True
    assert result.summary["diagnostic_only"] is True
    assert result.summary["production_recommendation"] is False
    assert result.summary["trading_gate"] is False


def test_timeline_ordering_with_evidence_status_events(tmp_path: Path) -> None:
    investigation = create_investigation(
        title="Investigation",
        description="Test",
        hypothesis="Evidence lifecycle events sort deterministically.",
        investigation_id="inv_order",
        now=NOW,
        output_dir=tmp_path / "investigations",
    ).investigation

    updated = attach_evidence_to_investigation(investigation, evidence_id="late", status_event="EVIDENCE_ARCHIVED", now="2026-07-05T12:03:00Z")
    updated = attach_evidence_to_investigation(updated, evidence_id="early", status_event="EVIDENCE_SUPERSEDED", now="2026-07-05T12:02:00Z")

    timestamps = [event["timestamp"] for event in updated["timeline"]]
    assert timestamps == sorted(timestamps)


def test_publish_ie2_artifacts(tmp_path: Path) -> None:
    paths = publish_ie2_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample"]).read_text(encoding="utf-8"))


def test_write_evidence_round_trip(tmp_path: Path) -> None:
    result = _create_evidence(tmp_path)
    written = write_evidence(result.evidence, output_dir=tmp_path)

    assert written.summary["deterministic_fingerprint"] == result.summary["deterministic_fingerprint"]


def test_evidence_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_evidence_engine.py"),
        Path("src/mgc_v05l/app/track_b_canonical_evidence_engine.py"),
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


def _create_evidence(tmp_path: Path):
    return create_evidence(
        investigation_id="sample_investigation",
        evidence_id="sample_evidence",
        title="Evidence",
        summary="Deterministic evidence summary.",
        evidence_classification="ANALYTICS",
        evidence_type="saved_query_result",
        source_component="canonical_analytics_engine",
        now=NOW,
        output_dir=tmp_path,
    )
