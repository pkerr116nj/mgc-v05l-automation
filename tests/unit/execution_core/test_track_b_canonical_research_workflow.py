from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_research_workflow import (
    create_or_locate_investigation,
    create_workflow,
    publish_rwf1_artifacts,
    publish_rwf2_artifacts,
    populate_investigation_evidence,
    run_workflow,
    sample_morning_gold_review,
    validate_workflow,
    workflow_run_fingerprint,
)
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import (
    load_investigation,
)


NOW = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)


def test_create_workflow(tmp_path: Path) -> None:
    result = create_workflow(
        workflow_id="wf1",
        title="Workflow",
        description="Diagnostic workflow",
        workflow_type="CUSTOM",
        steps=[{"step_id": "manual", "order": 1, "step_type": "MANUAL_REVIEW"}],
        now=NOW,
        output_dir=tmp_path,
    )

    assert result.workflow["workflow_id"] == "wf1"
    assert result.workflow["diagnostic_only"] is True
    assert result.summary["step_count"] == 1
    assert result.workflow_path.exists()


def test_validate_workflow_step_ordering(tmp_path: Path) -> None:
    workflow = create_workflow(
        workflow_id="wf_order",
        title="Workflow",
        description="Diagnostic workflow",
        workflow_type="CUSTOM",
        steps=[
            {"step_id": "two", "order": 2, "step_type": "MANUAL_REVIEW"},
            {"step_id": "one", "order": 1, "step_type": "EXPORT_SUMMARY"},
        ],
        now=NOW,
        output_dir=tmp_path,
    ).workflow

    assert validate_workflow(workflow)["valid"] is False
    assert "workflow_steps_not_sorted" in validate_workflow(workflow)["errors"]


def test_guardrails_preserved(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    run = run_workflow(workflow, output_dir=tmp_path, saved_query_output_dir=tmp_path / "saved", morning_brief_output_dir=tmp_path / "brief", now=NOW)

    assert workflow["diagnostic_only"] is True
    assert workflow["production_recommendation"] is False
    assert workflow["trading_gate"] is False
    assert run["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}
    assert all(step["guardrails"]["diagnostic_only"] is True for step in run["step_results"])


def test_run_sample_workflow(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    run = run_workflow(workflow, output_dir=tmp_path, saved_query_output_dir=tmp_path / "saved", morning_brief_output_dir=tmp_path / "brief", now=NOW)

    assert run["workflow_id"] == "morning_gold_review"
    assert run["status"] in {"COMPLETE", "COMPLETE_WITH_WARNINGS"}
    assert len(run["step_results"]) == 15
    assert any(step["step_type"] == "RUN_SAVED_QUERY" and step["status"] == "COMPLETE" for step in run["step_results"])
    assert any(step["step_type"] == "CREATE_OR_LOCATE_INVESTIGATION" and step["status"] == "COMPLETE" for step in run["step_results"])
    assert any(step["step_type"] == "EXPORT_INVESTIGATION_SUMMARY" and step["status"] == "COMPLETE" for step in run["step_results"])


def test_unsupported_step_skipped_safely(tmp_path: Path) -> None:
    workflow = create_workflow(
        workflow_id="wf_skip",
        title="Workflow",
        description="Diagnostic workflow",
        workflow_type="CUSTOM",
        steps=[{"step_id": "manual", "order": 1, "step_type": "MANUAL_REVIEW"}],
        now=NOW,
        output_dir=tmp_path,
    ).workflow

    run = run_workflow(workflow, output_dir=tmp_path, now=NOW)

    assert run["status"] == "COMPLETE_WITH_WARNINGS"
    assert run["step_results"][0]["status"] == "SKIPPED"
    assert run["step_results"][0]["reason"] == "unsupported_in_rwf1_minimal_runner"


def test_workflow_run_fingerprint_stable(tmp_path: Path) -> None:
    workflow = create_workflow(
        workflow_id="wf_stable",
        title="Workflow",
        description="Diagnostic workflow",
        workflow_type="CUSTOM",
        steps=[{"step_id": "manual", "order": 1, "step_type": "MANUAL_REVIEW"}],
        now=NOW,
        output_dir=tmp_path,
    ).workflow
    run_a = run_workflow(workflow, output_dir=tmp_path, now=NOW)
    run_b = run_workflow(workflow, output_dir=tmp_path, now=NOW)

    assert workflow_run_fingerprint(run_a) == workflow_run_fingerprint(run_b)


def test_publish_rwf1_artifacts(tmp_path: Path) -> None:
    paths = publish_rwf1_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_workflow"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_run"]).read_text(encoding="utf-8"))


def test_rwf2_creates_investigation_if_missing(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)

    investigation, created = create_or_locate_investigation(
        workflow,
        inputs={"investigation_id": "new_inv", "title": "New Investigation"},
        output_dir=tmp_path / "investigations",
        now=NOW,
    )

    assert created is True
    assert investigation["investigation_id"] == "new_inv"
    assert investigation["diagnostic_only"] is True


def test_rwf2_reuses_existing_investigation(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    create_or_locate_investigation(
        workflow,
        inputs={"investigation_id": "existing_inv", "title": "Existing"},
        output_dir=tmp_path / "investigations",
        now=NOW,
    )

    investigation, created = create_or_locate_investigation(
        workflow,
        inputs={"investigation_id": "existing_inv", "title": "Existing"},
        output_dir=tmp_path / "investigations",
        now=NOW,
    )

    assert created is False
    assert investigation["investigation_id"] == "existing_inv"


def test_rwf2_attaches_all_evidence_and_preserves_references(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    run = run_workflow(workflow, output_dir=tmp_path, saved_query_output_dir=tmp_path / "saved", morning_brief_output_dir=tmp_path / "brief", now=NOW)
    investigation = load_investigation("morning_gold_review_investigation", output_dir=tmp_path / "investigations")
    evidence_refs = [ref for ref in investigation["references"] if ref["reference_type"] == "evidence"]

    assert run["status"] == "COMPLETE"
    assert len(evidence_refs) >= 6
    assert all(ref["target_kind"] == "EVIDENCE" for ref in evidence_refs)
    assert all((ref.get("provenance") or {}).get("guardrails", {}).get("diagnostic_only") is True for ref in evidence_refs)


def test_rwf2_no_duplicate_evidence_on_repeated_population(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    run_workflow(workflow, output_dir=tmp_path, saved_query_output_dir=tmp_path / "saved", morning_brief_output_dir=tmp_path / "brief", now=NOW)
    first = load_investigation("morning_gold_review_investigation", output_dir=tmp_path / "investigations")
    first_count = len([ref for ref in first["references"] if ref["reference_type"] == "evidence"])

    run_workflow(workflow, output_dir=tmp_path, saved_query_output_dir=tmp_path / "saved", morning_brief_output_dir=tmp_path / "brief", now=NOW)
    second = load_investigation("morning_gold_review_investigation", output_dir=tmp_path / "investigations")
    second_count = len([ref for ref in second["references"] if ref["reference_type"] == "evidence"])

    assert second_count == first_count


def test_rwf2_population_result_guardrails_and_skip_missing_artifact(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    investigation, _ = create_or_locate_investigation(
        workflow,
        inputs={"investigation_id": "inv_missing", "title": "Missing Artifact"},
        output_dir=tmp_path / "investigations",
        now=NOW,
    )
    result = populate_investigation_evidence(
        workflow_run_id="run_missing",
        investigation_id=investigation["investigation_id"],
        artifact_refs=[{"target_kind": "ANALYTICS_DIFF", "target_id": "missing", "target_path": str(tmp_path / "missing.json")}],
        investigation_output_dir=tmp_path / "investigations",
        evidence_output_dir=tmp_path / "evidence",
        now=NOW,
    )

    assert result.evidence_attached_count == 0
    assert result.skipped_items[0]["reason"] == "artifact_missing"
    assert result.guardrails == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}


def test_rwf2_timeline_ordering(tmp_path: Path) -> None:
    workflow = sample_morning_gold_review(now=NOW, output_dir=tmp_path)
    run_workflow(workflow, output_dir=tmp_path, saved_query_output_dir=tmp_path / "saved", morning_brief_output_dir=tmp_path / "brief", now=NOW)
    investigation = load_investigation("morning_gold_review_investigation", output_dir=tmp_path / "investigations")
    timestamps = [event["timestamp"] for event in investigation["timeline"]]

    assert timestamps == sorted(timestamps)
    assert any(event["event_type"] == "WORKFLOW_STARTED" for event in investigation["timeline"])
    assert any(event["event_type"] == "WORKFLOW_COMPLETED" for event in investigation["timeline"])


def test_publish_rwf2_artifacts(tmp_path: Path) -> None:
    paths = publish_rwf2_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["population_schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_population"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_updated_investigation"]).read_text(encoding="utf-8"))


def test_research_workflow_import_boundary() -> None:
    paths = [
        Path("src/mgc_v05l/execution_core/track_b_canonical_research_workflow.py"),
        Path("src/mgc_v05l/app/track_b_canonical_research_workflow.py"),
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
