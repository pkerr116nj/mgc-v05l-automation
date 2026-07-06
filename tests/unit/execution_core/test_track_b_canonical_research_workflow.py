from __future__ import annotations

import ast
import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_canonical_research_workflow import (
    accept_claim_draft,
    activate_claim,
    activate_conclusion,
    attach_session_reference,
    complete_session,
    claim_review_readiness,
    conclusion_review_readiness,
    create_sample_morning_gold_session,
    create_or_locate_investigation,
    create_review_record,
    create_session,
    create_workflow,
    export_session_summary,
    export_claim_draft_summary,
    export_claim_review_summary,
    export_review_summary,
    generate_claim_drafts,
    generate_claim_review_queue,
    generate_conclusion_reviews,
    load_session,
    load_claim_drafts,
    load_claim_reviews,
    load_reviews,
    publish_rwf1_artifacts,
    publish_rwf2_artifacts,
    publish_rwf3_artifacts,
    publish_rwf4_artifacts,
    publish_rwf5_artifacts,
    publish_rwf6_artifacts,
    populate_investigation_evidence,
    reject_claim,
    reject_claim_draft,
    reject_conclusion,
    run_workflow,
    sample_morning_gold_review,
    session_fingerprint,
    start_session,
    transition_session,
    validate_workflow,
    workflow_run_fingerprint,
    write_session,
)
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import attach_evidence_reference, create_evidence, write_evidence
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import (
    attach_evidence_reference as attach_investigation_evidence_reference,
    load_investigation,
    write_investigation,
)
from mgc_v05l.execution_core.track_b_canonical_claims_engine import create_claim, list_claims, load_claim
from mgc_v05l.execution_core.track_b_canonical_conclusions_engine import attach_supporting_claim, create_conclusion, load_conclusion, write_conclusion


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
    assert len(run["step_results"]) == 19
    assert any(step["step_type"] == "RUN_SAVED_QUERY" and step["status"] == "COMPLETE" for step in run["step_results"])
    assert any(step["step_type"] == "CREATE_OR_LOCATE_INVESTIGATION" and step["status"] == "COMPLETE" for step in run["step_results"])
    assert any(step["step_type"] == "GENERATE_CLAIM_DRAFTS" and step["status"] == "COMPLETE" for step in run["step_results"])
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


def test_rwf3_generate_draft_from_unchanged_analytics_insight(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_insight", "INSIGHT", "insight", [{"title": "No meaningful analytics changes"}]))

    drafts = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)

    assert len(drafts) == 1
    assert drafts[0]["proposed_claim_type"] == "deterministic_evidence_review"
    assert drafts[0]["validation_preview"] == "SUPPORTED"
    assert drafts[0]["draft_status"] == "NEEDS_REVIEW"


def test_rwf3_generate_draft_from_changed_analytics_diff(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_diff", "DIFF", "analytics_diff", {"classification": "CHANGED"}))

    drafts = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)

    assert len(drafts) == 1
    assert "changed" in drafts[0]["proposed_statement"].lower()


def test_rwf3_generate_draft_from_morning_brief_change(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_brief", "MORNING_BRIEF", "morning_brief_change", {"changes": [{"domain": "PLATFORM"}]}))

    drafts = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)

    assert len(drafts) == 1
    assert drafts[0]["proposed_claim_classification"] == "OPERATIONAL"


def test_rwf3_no_draft_for_unknown_evidence(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_unknown", "ANALYTICS", "unknown", {"ok": True}))

    drafts = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)

    assert drafts == []


def test_rwf3_accept_draft_creates_draft_claim_only(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_insight", "INSIGHT", "insight", [{"title": "No meaningful analytics changes"}]))
    draft = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)[0]

    result = accept_claim_draft(draft["draft_id"], draft_output_dir=tmp_path / "drafts", evidence_output_dir=tmp_path / "evidence", claims_output_dir=tmp_path / "claims", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert result["claim"]["status"] == "DRAFT"
    assert result["claim"]["supporting_evidence"][0]["evidence_id"] == "evidence_insight"
    assert result["draft"]["draft_status"] == "ACCEPTED"


def test_rwf3_reject_draft_does_not_create_claim(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_insight", "INSIGHT", "insight", [{"title": "No meaningful analytics changes"}]))
    draft = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)[0]

    rejected = reject_claim_draft(draft["draft_id"], draft_output_dir=tmp_path / "drafts", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert rejected["draft_status"] == "REJECTED"
    assert list_claims(output_dir=tmp_path / "claims") == []


def test_rwf3_no_duplicate_drafts_on_repeated_run(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_insight", "INSIGHT", "insight", [{"title": "No meaningful analytics changes"}]))
    first = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)
    second = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)

    assert len(first) == len(second) == 1
    assert first[0]["draft_id"] == second[0]["draft_id"]


def test_rwf3_summary_and_guardrails(tmp_path: Path) -> None:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, "evidence_insight", "INSIGHT", "insight", [{"title": "No meaningful analytics changes"}]))
    generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)

    summary = export_claim_draft_summary(investigation_id=inv_id, draft_output_dir=tmp_path / "drafts")

    assert summary["draft_count"] == 1
    assert summary["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}


def test_publish_rwf3_artifacts(tmp_path: Path) -> None:
    paths = publish_rwf3_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["draft_schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_drafts"]).read_text(encoding="utf-8"))


def test_rwf4_draft_claim_appears_in_review_queue(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)

    reviews = generate_claim_review_queue(investigation_id=claim["investigation_id"], claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert len(reviews) == 1
    assert reviews[0]["claim_id"] == claim["claim_id"]
    assert reviews[0]["review_status"] == "PENDING_REVIEW"


def test_rwf4_claim_with_no_supporting_evidence_needs_evidence(tmp_path: Path) -> None:
    create_or_locate_investigation({"workflow_id": "wf", "title": "Review"}, inputs={"investigation_id": "inv_review", "title": "Review"}, output_dir=tmp_path / "investigations", now=NOW)
    claim = create_claim(
        investigation_id="inv_review",
        claim_id="claim_no_support",
        title="No support",
        statement="Needs evidence.",
        rationale="Test.",
        claim_classification="ANALYTICS",
        claim_type="review",
        now=NOW,
        output_dir=tmp_path / "claims",
    ).claim

    assert claim_review_readiness(claim) == "NEEDS_EVIDENCE"


def test_rwf4_claim_with_valid_supporting_evidence_ready(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)

    assert claim_review_readiness(claim) == "READY_FOR_OPERATOR_REVIEW"


def test_rwf4_activation_without_explicit_approval_fails_safely(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)

    try:
        activate_claim(claim["claim_id"], operator_approved=False, claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)
    except PermissionError as exc:
        assert "operator_approval_required" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("activation should require operator approval")
    assert load_claim(claim["claim_id"], output_dir=tmp_path / "claims")["status"] == "DRAFT"


def test_rwf4_activation_with_approval_changes_draft_to_active(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)

    result = activate_claim(claim["claim_id"], operator_approved=True, claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert result["claim"]["status"] == "ACTIVE"
    assert result["review"]["review_status"] == "APPROVED_FOR_ACTIVATION"
    assert not list((tmp_path / "conclusions").glob("*.json")) if (tmp_path / "conclusions").exists() else True


def test_rwf4_rejection_does_not_activate_claim(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)

    record = reject_claim(claim["claim_id"], claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert record["review_status"] == "REJECTED"
    assert load_claim(claim["claim_id"], output_dir=tmp_path / "claims")["status"] == "DRAFT"


def test_rwf4_timeline_events_for_activation_and_rejection(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)
    activate_claim(claim["claim_id"], operator_approved=True, claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)
    investigation = load_investigation(claim["investigation_id"], output_dir=tmp_path / "investigations")

    assert any(event["event_type"] == "CLAIM_ACTIVATED" for event in investigation["timeline"])

    claim_two = _draft_claim_from_accepted_draft(tmp_path, evidence_id="evidence_insight_two")
    reject_claim(claim_two["claim_id"], claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)
    investigation = load_investigation(claim_two["investigation_id"], output_dir=tmp_path / "investigations")
    assert any(event["event_type"] == "CLAIM_REJECTED" for event in investigation["timeline"])


def test_rwf4_review_summary_guardrails(tmp_path: Path) -> None:
    claim = _draft_claim_from_accepted_draft(tmp_path)
    generate_claim_review_queue(investigation_id=claim["investigation_id"], claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    summary = export_claim_review_summary(investigation_id=claim["investigation_id"], review_output_dir=tmp_path / "reviews")

    assert summary["review_count"] == 1
    assert summary["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}


def test_publish_rwf4_artifacts(tmp_path: Path) -> None:
    paths = publish_rwf4_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["review_schema"]).read_text(encoding="utf-8"))
    json.loads(Path(paths["sample_review_queue"]).read_text(encoding="utf-8"))


def test_rwf5_create_reusable_review_record(tmp_path: Path) -> None:
    record = create_review_record(
        review_kind="CONCLUSION",
        target_type="CONCLUSION",
        target_id="conclusion_1",
        investigation_id="inv_1",
        reviewer="operator",
        review_status="PENDING_REVIEW",
        readiness="READY_FOR_REVIEW",
        validation_summary={"outcome": "SUPPORTED", "confidence": "MEDIUM"},
        rationale_template="Deterministic conclusion review.",
        supporting_reference_count=1,
        contradicting_reference_count=0,
        related_reference_count=0,
        now=NOW,
    )

    assert record["schema_version"] == "rwf5_canonical_review_record_v1"
    assert record["review_kind"] == "CONCLUSION"
    assert record["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}
    assert record["deterministic_fingerprint"]


def test_rwf5_conclusion_review_creation_ready(tmp_path: Path) -> None:
    conclusion = _review_ready_conclusion(tmp_path)

    reviews = generate_conclusion_reviews(investigation_id=conclusion["investigation_id"], conclusions_output_dir=tmp_path / "conclusions", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert len(reviews) == 1
    assert reviews[0]["review_kind"] == "CONCLUSION"
    assert reviews[0]["readiness"] == "READY_FOR_REVIEW"
    assert reviews[0]["supporting_reference_count"] == 1


def test_rwf5_activation_without_explicit_approval_fails_safely(tmp_path: Path) -> None:
    conclusion = _review_ready_conclusion(tmp_path)

    try:
        activate_conclusion(conclusion["conclusion_id"], operator_approved=False, conclusions_output_dir=tmp_path / "conclusions", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)
    except PermissionError as exc:
        assert "operator_approval_required" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("activation should require operator approval")
    assert load_conclusion(conclusion["conclusion_id"], output_dir=tmp_path / "conclusions")["status"] == "DRAFT"


def test_rwf5_activation_with_approval_changes_draft_to_active(tmp_path: Path) -> None:
    conclusion = _review_ready_conclusion(tmp_path)

    result = activate_conclusion(conclusion["conclusion_id"], operator_approved=True, conclusions_output_dir=tmp_path / "conclusions", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert result["conclusion"]["status"] == "ACTIVE"
    assert result["review"]["review_status"] == "APPROVED"
    assert result["review"]["readiness"] == "ALREADY_ACTIVE"
    investigation = load_investigation(conclusion["investigation_id"], output_dir=tmp_path / "investigations")
    assert any(event["event_type"] == "CONCLUSION_ACTIVATED" for event in investigation["timeline"])


def test_rwf5_rejection_leaves_conclusion_inactive(tmp_path: Path) -> None:
    conclusion = _review_ready_conclusion(tmp_path)

    review = reject_conclusion(conclusion["conclusion_id"], conclusions_output_dir=tmp_path / "conclusions", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    assert review["review_status"] == "REJECTED"
    assert load_conclusion(conclusion["conclusion_id"], output_dir=tmp_path / "conclusions")["status"] == "DRAFT"
    investigation = load_investigation(conclusion["investigation_id"], output_dir=tmp_path / "investigations")
    assert any(event["event_type"] == "CONCLUSION_REJECTED" for event in investigation["timeline"])


def test_rwf5_review_summary_guardrails(tmp_path: Path) -> None:
    conclusion = _review_ready_conclusion(tmp_path)
    generate_conclusion_reviews(investigation_id=conclusion["investigation_id"], conclusions_output_dir=tmp_path / "conclusions", review_output_dir=tmp_path / "reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)

    summary = export_review_summary(investigation_id=conclusion["investigation_id"], review_output_dir=tmp_path / "reviews")

    assert summary["review_count"] == 1
    assert summary["review_kind_counts"] == {"CONCLUSION": 1}
    assert summary["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}


def test_publish_rwf5_artifacts(tmp_path: Path) -> None:
    paths = publish_rwf5_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["review_schema"]).read_text(encoding="utf-8"))
    sample = json.loads(Path(paths["sample_conclusion_review"]).read_text(encoding="utf-8"))
    assert sample["reviews"][0]["readiness"] == "READY_FOR_REVIEW"


def test_rwf6_create_session(tmp_path: Path) -> None:
    result = create_session(
        session_id="session_test",
        title="Session",
        description="Diagnostic session",
        session_type="CUSTOM",
        now=NOW,
        output_dir=tmp_path / "sessions",
    )

    assert result.session["session_id"] == "session_test"
    assert result.session["status"] == "PLANNED"
    assert result.session["guardrails"] == {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}
    assert result.summary["workflow_count"] == 0


def test_rwf6_lifecycle_transitions(tmp_path: Path) -> None:
    session = create_session(session_id="session_life", title="Session", description="Diagnostic session", session_type="CUSTOM", now=NOW, output_dir=tmp_path / "sessions").session

    running = transition_session(session, target_status="RUNNING", now=NOW)
    complete = transition_session(running, target_status="COMPLETE", now=NOW)
    archived = transition_session(complete, target_status="ARCHIVED", now=NOW)

    assert running["status"] == "RUNNING"
    assert complete["status"] == "COMPLETE"
    assert archived["status"] == "ARCHIVED"


def test_rwf6_invalid_transition_rejected(tmp_path: Path) -> None:
    session = create_session(session_id="session_bad", title="Session", description="Diagnostic session", session_type="CUSTOM", now=NOW, output_dir=tmp_path / "sessions").session

    try:
        transition_session(session, target_status="COMPLETE", now=NOW)
    except ValueError as exc:
        assert "Invalid session transition" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("invalid transition should fail")


def test_rwf6_attach_workflow_and_investigation(tmp_path: Path) -> None:
    session = create_session(session_id="session_attach", title="Session", description="Diagnostic session", session_type="CUSTOM", now=NOW, output_dir=tmp_path / "sessions").session
    session = attach_session_reference(session, reference_type="workflow_run", target_kind="WORKFLOW_RUN", target_id="run_1", target_path=str(tmp_path / "run_1.json"), now=NOW)
    session = attach_session_reference(session, reference_type="investigation", target_kind="INVESTIGATION", target_id="inv_1", target_path=str(tmp_path / "inv_1.json"), now=NOW)

    refs = session["references"]
    assert len(refs) == 2
    assert any((ref.get("metadata") or {}).get("logical_target_kind") == "WORKFLOW_RUN" for ref in refs)
    assert any(ref.get("target_kind") == "INVESTIGATION" for ref in refs)


def test_rwf6_summary_counts(tmp_path: Path) -> None:
    session = create_session(session_id="session_counts", title="Session", description="Diagnostic session", session_type="CUSTOM", now=NOW, output_dir=tmp_path / "sessions").session
    session = attach_session_reference(session, reference_type="evidence", target_kind="EVIDENCE", target_id="evidence_1", now=NOW)
    session = attach_session_reference(session, reference_type="claim_review", target_kind="CLAIM_REVIEW", target_id="review_1", now=NOW)
    session = attach_session_reference(session, reference_type="conclusion_review", target_kind="CONCLUSION_REVIEW", target_id="review_2", now=NOW)
    result = write_session(session, output_dir=tmp_path / "sessions")

    assert result.summary["evidence_count"] == 1
    assert result.summary["review_count"] == 1
    assert result.summary["conclusion_review_count"] == 1


def test_rwf6_fingerprint_stability(tmp_path: Path) -> None:
    a = create_session(session_id="session_fp", title="Session", description="Diagnostic session", session_type="CUSTOM", now=NOW, output_dir=tmp_path / "a").session
    b = create_session(session_id="session_fp", title="Session", description="Diagnostic session", session_type="CUSTOM", now=NOW, output_dir=tmp_path / "b").session

    assert session_fingerprint(a) == session_fingerprint(b)


def test_rwf6_sample_session_records_missing_optional_warnings(tmp_path: Path) -> None:
    session = create_sample_morning_gold_session(output_dir=tmp_path, now=NOW)

    assert session["session_id"] == "morning_gold_research_session"
    assert session["status"] in {"COMPLETE", "COMPLETE_WITH_WARNINGS"}
    assert session["guardrails"]["diagnostic_only"] is True
    assert isinstance(session["warnings"], list)


def test_publish_rwf6_artifacts(tmp_path: Path) -> None:
    paths = publish_rwf6_artifacts(output_dir=tmp_path, now=NOW)

    for path in paths.values():
        assert Path(path).exists()
    json.loads(Path(paths["session_schema"]).read_text(encoding="utf-8"))
    sample = json.loads(Path(paths["sample_session"]).read_text(encoding="utf-8"))
    assert sample["schema_version"] == "rwf6_canonical_research_session_v1"


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


def _evidence_with_source(tmp_path: Path, evidence_id: str, classification: str, evidence_type: str, source_payload):
    source_path = tmp_path / f"{evidence_id}_source.json"
    source_path.write_text(json.dumps(source_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    result = create_evidence(
        investigation_id="inv_rwf3",
        evidence_id=evidence_id,
        title=f"{classification} evidence",
        summary="Synthetic source-backed evidence.",
        evidence_classification=classification,
        evidence_type=evidence_type,
        source_component="test",
        now=NOW,
        output_dir=tmp_path / "evidence",
    )
    evidence = attach_evidence_reference(
        result.evidence,
        attachment_type="artifact" if evidence_type == "unknown" else evidence_type if evidence_type in {"analytics_diff", "insight", "morning_brief"} else "artifact",
        reference_id=source_path.stem,
        artifact_path=str(source_path),
        now=NOW,
    )
    return write_evidence(evidence, output_dir=tmp_path / "evidence").evidence


def _investigation_with_evidence(tmp_path: Path, evidence: dict) -> str:
    investigation = create_or_locate_investigation(
        {"workflow_id": "wf_rwf3", "title": "RWF3 Investigation", "description": "Test", "owner": "operator", "tags": []},
        inputs={"investigation_id": "inv_rwf3", "title": "RWF3 Investigation"},
        output_dir=tmp_path / "investigations",
        now=NOW,
    )[0]
    investigation = attach_investigation_evidence_reference(
        investigation,
        evidence_id=str(evidence["evidence_id"]),
        artifact_path=str(tmp_path / "evidence" / f"{evidence['evidence_id']}.json"),
        now=NOW,
    )
    write_investigation(investigation, output_dir=tmp_path / "investigations")
    return "inv_rwf3"


def _draft_claim_from_accepted_draft(tmp_path: Path, evidence_id: str = "evidence_insight") -> dict:
    inv_id = _investigation_with_evidence(tmp_path, _evidence_with_source(tmp_path, evidence_id, "INSIGHT", "insight", [{"title": "No meaningful analytics changes"}]))
    draft = generate_claim_drafts(investigation_id=inv_id, investigation_output_dir=tmp_path / "investigations", evidence_output_dir=tmp_path / "evidence", draft_output_dir=tmp_path / "drafts", now=NOW)[0]
    result = accept_claim_draft(draft["draft_id"], draft_output_dir=tmp_path / "drafts", evidence_output_dir=tmp_path / "evidence", claims_output_dir=tmp_path / "claims", investigation_output_dir=tmp_path / "investigations", now=NOW)
    return result["claim"]


def _review_ready_conclusion(tmp_path: Path) -> dict:
    claim = _draft_claim_from_accepted_draft(tmp_path)
    active = activate_claim(claim["claim_id"], operator_approved=True, claims_output_dir=tmp_path / "claims", review_output_dir=tmp_path / "claim_reviews", investigation_output_dir=tmp_path / "investigations", now=NOW)["claim"]
    result = create_conclusion(
        investigation_id=active["investigation_id"],
        conclusion_id="conclusion_review_ready",
        title="Review-ready conclusion",
        hypothesis="Supported active claims can prepare a conclusion for review.",
        conclusion="The conclusion is ready for manual review.",
        rationale="Test conclusion.",
        conclusion_classification="ANALYTICS",
        status="DRAFT",
        now=NOW,
        output_dir=tmp_path / "conclusions",
    )
    conclusion = attach_supporting_claim(result.conclusion, active, now=NOW)
    write_conclusion(conclusion, output_dir=tmp_path / "conclusions")
    assert conclusion_review_readiness(load_conclusion("conclusion_review_ready", output_dir=tmp_path / "conclusions")) == "READY_FOR_REVIEW"
    return load_conclusion("conclusion_review_ready", output_dir=tmp_path / "conclusions")
