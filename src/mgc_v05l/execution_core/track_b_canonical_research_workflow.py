"""Canonical Research Workflow model for deterministic research orchestration.

Research workflows coordinate existing diagnostic/reporting components. They do
not schedule jobs, touch runtime state, contact brokers, mutate strategies, or
create trading recommendations.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_analytics_saved_queries import (
    CanonicalAnalyticsInsightEngine,
    DEFAULT_OUTPUT_DIR as DEFAULT_SAVED_QUERY_OUTPUT_DIR,
    EXECUTION_SUMMARY_JSON,
    LATEST_INSIGHTS_JSON,
    RESULT_DIFF_JSON,
    RESULT_SNAPSHOT_DIR,
    compare_latest_execution_for_query,
    load_saved_queries,
    publish_insights,
    publish_result_diff,
    publish_saved_query_artifacts,
    run_saved_query,
)
from mgc_v05l.execution_core.track_b_canonical_evidence_engine import (
    DEFAULT_OUTPUT_DIR as DEFAULT_EVIDENCE_OUTPUT_DIR,
    attach_evidence_reference,
    create_evidence,
    load_evidence,
    write_evidence,
)
from mgc_v05l.execution_core.track_b_canonical_claims_engine import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CLAIMS_OUTPUT_DIR,
    attach_contradicting_evidence,
    attach_related_evidence,
    attach_supporting_evidence,
    create_claim,
    list_claims,
    load_claim,
    refresh_claim_validation,
    write_claim,
)
from mgc_v05l.execution_core.track_b_canonical_investigation_engine import (
    DEFAULT_OUTPUT_DIR as DEFAULT_INVESTIGATION_OUTPUT_DIR,
    append_investigation_event,
    attach_evidence_reference as attach_investigation_evidence_reference,
    create_investigation,
    list_investigations,
    load_investigation,
    summarize_investigation,
    write_investigation,
)
from mgc_v05l.execution_core.track_b_canonical_morning_brief import DEFAULT_OUTPUT_DIR as DEFAULT_MORNING_BRIEF_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_canonical_morning_brief import run_canonical_morning_brief
from mgc_v05l.execution_core.track_b_canonical_reference_envelope import build_provenance_envelope


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "research_workflow_engine"

WORKFLOW_SCHEMA_VERSION = "rwf1_canonical_research_workflow_v1"
WORKFLOW_STEP_SCHEMA_VERSION = "rwf1_workflow_step_v1"
WORKFLOW_RUN_SCHEMA_VERSION = "rwf1_workflow_run_v1"

CONTRACT_MD = "rwf1_workflow_contract.md"
SCHEMA_JSON = "rwf1_workflow_schema.json"
SAMPLE_WORKFLOW_JSON = "rwf1_sample_morning_gold_review.json"
SAMPLE_RUN_JSON = "rwf1_sample_workflow_run.json"
SUMMARY_MD = "rwf1_workflow_summary.md"
RUNNER_CONTRACT_MD = "rwf1_runner_contract.md"
RWF2_POPULATION_CONTRACT_MD = "rwf2_population_contract.md"
RWF2_POPULATION_SCHEMA_JSON = "rwf2_population_schema.json"
RWF2_SAMPLE_POPULATION_JSON = "rwf2_sample_population.json"
RWF2_SAMPLE_INVESTIGATION_JSON = "rwf2_sample_updated_investigation.json"
RWF2_POPULATION_SUMMARY_MD = "rwf2_population_summary.md"
RWF3_DRAFT_CONTRACT_MD = "rwf3_claim_draft_contract.md"
RWF3_DRAFT_SCHEMA_JSON = "rwf3_claim_draft_schema.json"
RWF3_SAMPLE_DRAFTS_JSON = "rwf3_sample_claim_drafts.json"
RWF3_DRAFT_SUMMARY_MD = "rwf3_claim_draft_queue_summary.md"
RWF3_MANUAL_REVIEW_CONTRACT_MD = "rwf3_manual_review_contract.md"
RWF4_REVIEW_CONTRACT_MD = "rwf4_claim_review_contract.md"
RWF4_REVIEW_SCHEMA_JSON = "rwf4_claim_review_schema.json"
RWF4_SAMPLE_REVIEW_QUEUE_JSON = "rwf4_sample_review_queue.json"
RWF4_REVIEW_SUMMARY_MD = "rwf4_claim_review_summary.md"
RWF4_MANUAL_ACTIVATION_CONTRACT_MD = "rwf4_manual_activation_contract.md"

CLAIM_DRAFT_SCHEMA_VERSION = "rwf3_claim_draft_v1"
CLAIM_DRAFT_SUMMARY_SCHEMA_VERSION = "rwf3_claim_draft_queue_summary_v1"
CLAIM_REVIEW_SCHEMA_VERSION = "rwf4_claim_review_record_v1"
CLAIM_REVIEW_SUMMARY_SCHEMA_VERSION = "rwf4_claim_review_summary_v1"

VALID_WORKFLOW_STATUSES = {"DRAFT", "ACTIVE", "PAUSED", "COMPLETE", "ARCHIVED"}
VALID_WORKFLOW_TYPES = {"MORNING_REVIEW", "INVESTIGATION_REFRESH", "CANDIDATE_REVIEW", "STRATEGY_REVIEW", "PLATFORM_REVIEW", "CUSTOM"}
VALID_STEP_TYPES = {
    "RUN_SAVED_QUERY",
    "COMPARE_QUERY_RESULT",
    "GENERATE_INSIGHT",
    "CREATE_INVESTIGATION",
    "CREATE_OR_LOCATE_INVESTIGATION",
    "ATTACH_QUERY_EXECUTION",
    "ATTACH_QUERY_RESULT",
    "ATTACH_ANALYTICS_DIFF",
    "ATTACH_ANALYTICS_INSIGHT",
    "ATTACH_MORNING_BRIEF",
    "ATTACH_BRIEF_CHANGE",
    "ATTACH_EVIDENCE",
    "EXPORT_INVESTIGATION_SUMMARY",
    "GENERATE_CLAIM_DRAFTS",
    "EXPORT_CLAIM_DRAFT_SUMMARY",
    "REVIEW_DRAFT_CLAIMS",
    "ACTIVATE_APPROVED_CLAIM",
    "REJECT_DRAFT_CLAIM",
    "EXPORT_CLAIM_REVIEW_SUMMARY",
    "VALIDATE_CLAIMS",
    "UPDATE_CONCLUSION",
    "GENERATE_MORNING_BRIEF",
    "ARCHIVE_BRIEF",
    "EXPLAIN_BRIEF_CHANGE",
    "VERIFY_CHAIN",
    "EXPORT_SUMMARY",
    "MANUAL_REVIEW",
}
VALID_STEP_STATUSES = {"PENDING", "RUNNING", "COMPLETE", "FAILED", "SKIPPED"}
VALID_RUN_STATUSES = {"PLANNED", "RUNNING", "COMPLETE", "COMPLETE_WITH_WARNINGS", "FAILED"}
VALID_DRAFT_STATUSES = {"PROPOSED", "NEEDS_REVIEW", "ACCEPTED", "REJECTED", "SUPERSEDED", "ARCHIVED"}
VALID_DRAFT_CONFIDENCE = {"UNKNOWN", "LOW", "MEDIUM", "HIGH"}
VALID_VALIDATION_PREVIEW = {"SUPPORTED", "PARTIALLY_SUPPORTED", "CONTRADICTED", "INSUFFICIENT_EVIDENCE", "INVALID"}
VALID_REVIEW_STATUSES = {"PENDING_REVIEW", "APPROVED_FOR_ACTIVATION", "REJECTED", "NEEDS_MORE_EVIDENCE", "SUPERSEDED", "ARCHIVED"}
VALID_REVIEW_READINESS = {"READY_FOR_OPERATOR_REVIEW", "NEEDS_EVIDENCE", "CONTRADICTED", "INVALID_REFERENCES", "ALREADY_ACTIVE", "NOT_APPLICABLE"}


@dataclass(frozen=True)
class CanonicalResearchWorkflowResult:
    workflow: dict[str, Any]
    workflow_path: Path
    summary: dict[str, Any]
    summary_path: Path
    output_dir: Path


@dataclass(frozen=True)
class InvestigationPopulationResult:
    workflow_run_id: str
    investigation_id: str
    created_new_investigation: bool
    evidence_attached_count: int
    references_created: int
    timeline_events_added: int
    skipped_items: list[dict[str, Any]]
    deterministic_fingerprint: str
    provenance: dict[str, Any]
    guardrails: dict[str, bool]


def create_workflow(
    *,
    title: str,
    description: str,
    workflow_type: str,
    owner: str = "operator",
    tags: Sequence[str] | None = None,
    workflow_id: str | None = None,
    status: str = "DRAFT",
    steps: Sequence[Mapping[str, Any]] | None = None,
    now: datetime | str | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> CanonicalResearchWorkflowResult:
    generated_at = _coerce_now(now)
    workflow_type = _validate(workflow_type, VALID_WORKFLOW_TYPES, "workflow_type")
    status = _validate(status, VALID_WORKFLOW_STATUSES, "workflow_status")
    workflow = {
        "schema_version": WORKFLOW_SCHEMA_VERSION,
        "workflow_id": workflow_id or _id("workflow", title, generated_at),
        "title": title,
        "description": description,
        "created_at": generated_at.isoformat(),
        "updated_at": generated_at.isoformat(),
        "status": status,
        "workflow_type": workflow_type,
        "owner": owner,
        "tags": sorted(set(tags or [])),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "steps": [normalize_workflow_step(step, now=generated_at) for step in (steps or [])],
    }
    return write_workflow(workflow, output_dir=output_dir)


def normalize_workflow_step(step: Mapping[str, Any], *, now: datetime | str | None = None) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    step_type = _validate(str(step.get("step_type") or ""), VALID_STEP_TYPES, "step_type")
    status = _validate(str(step.get("status") or "PENDING"), VALID_STEP_STATUSES, "step_status")
    order = int(step.get("order") or 0)
    if order <= 0:
        raise ValueError("Workflow step order must be positive")
    return {
        "schema_version": WORKFLOW_STEP_SCHEMA_VERSION,
        "step_id": str(step.get("step_id") or f"step_{order:03d}_{step_type.lower()}"),
        "order": order,
        "title": str(step.get("title") or step_type.replace("_", " ").title()),
        "description": str(step.get("description") or ""),
        "step_type": step_type,
        "inputs": dict(step.get("inputs") or {}),
        "outputs": dict(step.get("outputs") or {}),
        "status": status,
        "provenance": dict(step.get("provenance") or {"source_component": "canonical_research_workflow"}),
        "guardrails": _guardrails(),
    }


def validate_workflow(workflow: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if workflow.get("schema_version") != WORKFLOW_SCHEMA_VERSION:
        errors.append("invalid_workflow_schema_version")
    if not workflow.get("workflow_id"):
        errors.append("missing_workflow_id")
    if workflow.get("status") not in VALID_WORKFLOW_STATUSES:
        errors.append("invalid_workflow_status")
    if workflow.get("workflow_type") not in VALID_WORKFLOW_TYPES:
        errors.append("invalid_workflow_type")
    if workflow.get("diagnostic_only") is not True or workflow.get("production_recommendation") is not False or workflow.get("trading_gate") is not False:
        errors.append("invalid_workflow_guardrails")
    orders: list[int] = []
    for step in workflow.get("steps") or []:
        step_errors = validate_workflow_step(step).get("errors") or []
        errors.extend([f"{step.get('step_id')}:{error}" for error in step_errors])
        orders.append(int(step.get("order") or 0))
    if orders != sorted(orders):
        errors.append("workflow_steps_not_sorted")
    if len(set(orders)) != len(orders):
        errors.append("duplicate_step_order")
    return {"valid": not errors, "errors": errors}


def validate_workflow_step(step: Mapping[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if step.get("schema_version") != WORKFLOW_STEP_SCHEMA_VERSION:
        errors.append("invalid_step_schema_version")
    if not step.get("step_id"):
        errors.append("missing_step_id")
    if step.get("step_type") not in VALID_STEP_TYPES:
        errors.append("invalid_step_type")
    if step.get("status") not in VALID_STEP_STATUSES:
        errors.append("invalid_step_status")
    if int(step.get("order") or 0) <= 0:
        errors.append("invalid_step_order")
    guardrails = step.get("guardrails") or {}
    if guardrails != _guardrails():
        errors.append("invalid_step_guardrails")
    return {"valid": not errors, "errors": errors}


def run_workflow(
    workflow: Mapping[str, Any],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    saved_query_output_dir: Path = DEFAULT_SAVED_QUERY_OUTPUT_DIR,
    morning_brief_output_dir: Path = DEFAULT_MORNING_BRIEF_OUTPUT_DIR,
    investigation_output_dir: Path | None = None,
    evidence_output_dir: Path | None = None,
    claim_draft_output_dir: Path | None = None,
    claims_output_dir: Path | None = None,
    claim_review_output_dir: Path | None = None,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    started_at = _coerce_now(now)
    validation = validate_workflow(workflow)
    step_results: list[dict[str, Any]] = []
    artifact_refs: list[dict[str, Any]] = []
    status = "RUNNING"
    if not validation["valid"]:
        status = "FAILED"
        step_results.append(_step_result(step_id="validation", order=0, status="FAILED", reason="workflow_validation_failed", details=validation, now=started_at))
    else:
        context: dict[str, Any] = {"artifact_refs": []}
        for step in sorted(workflow.get("steps") or [], key=lambda row: int(row.get("order") or 0)):
            result = _run_step(
                step,
                workflow=workflow,
                output_dir=output_dir,
                saved_query_output_dir=saved_query_output_dir,
                morning_brief_output_dir=morning_brief_output_dir,
                investigation_output_dir=investigation_output_dir or output_dir / "investigations",
                evidence_output_dir=evidence_output_dir or output_dir / "evidence",
                claim_draft_output_dir=claim_draft_output_dir or output_dir / "claim_drafts",
                claims_output_dir=claims_output_dir or output_dir / "claims",
                claim_review_output_dir=claim_review_output_dir or output_dir / "claim_reviews",
                context=context,
                now=started_at,
            )
            step_results.append(result)
            artifact_refs.extend(result.get("artifact_refs") or [])
            context.setdefault("artifact_refs", []).extend(result.get("artifact_refs") or [])
            if result["status"] == "FAILED":
                status = "FAILED"
                break
        if status != "FAILED":
            status = "COMPLETE_WITH_WARNINGS" if any(row["status"] == "SKIPPED" for row in step_results) else "COMPLETE"
    completed_at = _coerce_now(now)
    run = {
        "schema_version": WORKFLOW_RUN_SCHEMA_VERSION,
        "run_id": _run_id(str(workflow.get("workflow_id")), started_at, step_results),
        "workflow_id": workflow.get("workflow_id"),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "status": status,
        "step_results": step_results,
        "artifact_refs": artifact_refs,
        "guardrails": _guardrails(),
    }
    run["deterministic_fingerprint"] = workflow_run_fingerprint(run)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_path = output_dir / f"{run['run_id']}.json"
    run_path.write_text(json.dumps(run, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary_path = output_dir / "latest_workflow_run_summary.json"
    summary_path.write_text(json.dumps(summarize_workflow_run(run), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return run


def _run_step(
    step: Mapping[str, Any],
    *,
    workflow: Mapping[str, Any],
    output_dir: Path,
    saved_query_output_dir: Path,
    morning_brief_output_dir: Path,
    investigation_output_dir: Path,
    evidence_output_dir: Path,
    claim_draft_output_dir: Path,
    claims_output_dir: Path,
    claim_review_output_dir: Path,
    context: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    step_type = str(step.get("step_type"))
    inputs = dict(step.get("inputs") or {})
    try:
        if step_type == "RUN_SAVED_QUERY":
            publish_saved_query_artifacts(output_dir=saved_query_output_dir, now=now)
            query_id = str(inputs.get("saved_query_id") or "expectancy_by_strategy")
            queries = {query.saved_query_id: query for query in load_saved_queries(saved_query_output_dir / "saved_analytics_queries.jsonl")}
            query = queries.get(query_id)
            if query is None:
                return _step_result(step=step, status="FAILED", reason="saved_query_not_found", details={"saved_query_id": query_id}, now=now)
            run_result = run_saved_query(query, output_dir=saved_query_output_dir, write_execution_audit=True, generated_at=now)
            execution_id = (run_result.execution_record or {}).get("execution_id")
            context["saved_query_id"] = query_id
            context["execution_id"] = execution_id
            context["analytics_result_id"] = execution_id
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="saved_query_executed",
                details={"saved_query_id": query_id, "execution_id": execution_id, "validation": run_result.validation.status},
                artifact_refs=[
                    _artifact_ref("EXECUTION_RECORD", execution_id, saved_query_output_dir / EXECUTION_SUMMARY_JSON),
                    _artifact_ref("ANALYTICS_RESULT", execution_id, saved_query_output_dir / RESULT_SNAPSHOT_DIR / f"{execution_id}.json"),
                ],
                now=now,
            )
        if step_type == "COMPARE_QUERY_RESULT":
            query_id = str(inputs.get("saved_query_id") or context.get("saved_query_id") or "expectancy_by_strategy")
            diff = compare_latest_execution_for_query(output_dir=saved_query_output_dir, saved_query_id=query_id, generated_at=now)
            publish_result_diff(output_dir=saved_query_output_dir, diff=diff)
            context["diff_id"] = diff.diff_id
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="query_result_compared",
                details={"saved_query_id": query_id, "diff_id": diff.diff_id, "status": diff.status},
                artifact_refs=[_artifact_ref("ANALYTICS_DIFF", diff.diff_id, saved_query_output_dir / "cae7_latest_result_diff.json")],
                now=now,
            )
        if step_type == "GENERATE_INSIGHT":
            diff_path = saved_query_output_dir / RESULT_DIFF_JSON
            if not diff_path.exists():
                return _step_result(step=step, status="SKIPPED", reason="result_diff_not_available", now=now)
            diff_record = json.loads(diff_path.read_text(encoding="utf-8"))
            insights = CanonicalAnalyticsInsightEngine().evaluate(diff_record, generated_at=now)
            publish_insights(output_dir=saved_query_output_dir, insights=insights)
            context["insight_count"] = len(insights)
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="insights_generated",
                details={"insight_count": len(insights)},
                artifact_refs=[_artifact_ref("ANALYTICS_INSIGHT", "latest", saved_query_output_dir / "cae8_latest_insights.json")],
                now=now,
            )
        if step_type == "GENERATE_MORNING_BRIEF":
            brief = run_canonical_morning_brief(output_dir=morning_brief_output_dir, now=now)
            brief_id = str((brief.brief.get("platform") or {}).get("certification_classification") or "morning_brief")
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="morning_brief_generated",
                details={"brief_json_path": str(brief.json_path)},
                artifact_refs=[_artifact_ref("MORNING_BRIEF", brief_id, brief.json_path)],
                now=now,
            )
        if step_type == "ARCHIVE_BRIEF":
            path = morning_brief_output_dir / "mb2_morning_brief_archive.jsonl"
            return _existing_artifact_step(step, path=path, target_kind="ARTIFACT", reason="brief_archive_recorded", now=now)
        if step_type == "EXPLAIN_BRIEF_CHANGE":
            path = morning_brief_output_dir / "mb3_latest_change_explanation.json"
            return _existing_artifact_step(step, path=path, target_kind="MORNING_BRIEF", reason="brief_change_explained", now=now)
        if step_type == "CREATE_OR_LOCATE_INVESTIGATION":
            investigation, created = create_or_locate_investigation(
                workflow,
                inputs=inputs,
                output_dir=investigation_output_dir,
                now=now,
            )
            investigation = append_investigation_event(
                investigation,
                event_type="WORKFLOW_STARTED",
                artifact_reference=_artifact_ref("ARTIFACT", workflow.get("workflow_id"), output_dir / f"{workflow.get('workflow_id')}.json"),
                provenance={"source": "canonical_research_workflow", "operation": "workflow_population_started", "workflow_id": workflow.get("workflow_id")},
                now=now,
            )
            if not created:
                investigation = append_investigation_event(
                    investigation,
                    event_type="INVESTIGATION_LOCATED",
                    artifact_reference=_artifact_ref("INVESTIGATION", investigation.get("investigation_id"), investigation_output_dir / f"{investigation.get('investigation_id')}.json"),
                    provenance={"source": "canonical_research_workflow", "operation": "investigation_located"},
                    now=now,
                )
            write_investigation(investigation, output_dir=investigation_output_dir)
            context["investigation_id"] = investigation.get("investigation_id")
            context["created_new_investigation"] = created
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="investigation_created" if created else "investigation_located",
                details={"investigation_id": investigation.get("investigation_id"), "created_new_investigation": created},
                artifact_refs=[_artifact_ref("INVESTIGATION", investigation.get("investigation_id"), investigation_output_dir / f"{investigation.get('investigation_id')}.json")],
                now=now,
            )
        if step_type in {
            "ATTACH_QUERY_EXECUTION",
            "ATTACH_QUERY_RESULT",
            "ATTACH_ANALYTICS_DIFF",
            "ATTACH_ANALYTICS_INSIGHT",
            "ATTACH_MORNING_BRIEF",
            "ATTACH_BRIEF_CHANGE",
            "ATTACH_EVIDENCE",
        }:
            result = populate_investigation_evidence(
                workflow_run_id=str(context.get("workflow_run_id") or "active_workflow_run"),
                investigation_id=str(inputs.get("investigation_id") or context.get("investigation_id") or ""),
                artifact_refs=_artifact_refs_for_population_step(
                    step_type,
                    context_refs=list(context.get("artifact_refs") or []),
                    saved_query_output_dir=saved_query_output_dir,
                    morning_brief_output_dir=morning_brief_output_dir,
                    execution_id=str(context.get("execution_id") or ""),
                ),
                investigation_output_dir=investigation_output_dir,
                evidence_output_dir=evidence_output_dir,
                now=now,
            )
            context["last_population_result"] = result.__dict__
            if not result.investigation_id:
                return _step_result(step=step, status="SKIPPED", reason="investigation_not_available", details=result.__dict__, now=now)
            duplicate_only = result.evidence_attached_count == 0 and bool(result.skipped_items) and all(item.get("reason") == "duplicate_evidence" for item in result.skipped_items)
            status_for_step = "COMPLETE" if result.evidence_attached_count > 0 or duplicate_only else "SKIPPED"
            reason = "evidence_already_attached" if duplicate_only else "no_matching_artifacts_for_population_step" if status_for_step == "SKIPPED" else "evidence_attached"
            return _step_result(
                step=step,
                status=status_for_step,
                reason=reason,
                details=result.__dict__,
                artifact_refs=[_artifact_ref("INVESTIGATION", result.investigation_id, investigation_output_dir / f"{result.investigation_id}.json")],
                now=now,
            )
        if step_type == "EXPORT_INVESTIGATION_SUMMARY":
            investigation_id = str(inputs.get("investigation_id") or context.get("investigation_id") or "")
            if not investigation_id:
                return _step_result(step=step, status="SKIPPED", reason="investigation_not_available", now=now)
            investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
            investigation = append_investigation_event(
                investigation,
                event_type="INVESTIGATION_SUMMARY_EXPORTED",
                artifact_reference=_artifact_ref("INVESTIGATION", investigation_id, investigation_output_dir / f"{investigation_id}.summary.json"),
                provenance={"source": "canonical_research_workflow", "operation": "export_investigation_summary"},
                now=now,
            )
            investigation = append_investigation_event(
                investigation,
                event_type="WORKFLOW_COMPLETED",
                artifact_reference=_artifact_ref("ARTIFACT", context.get("workflow_run_id") or "active_workflow_run", output_dir / "latest_workflow_run_summary.json"),
                provenance={"source": "canonical_research_workflow", "operation": "workflow_population_completed", "workflow_id": workflow.get("workflow_id")},
                now=now,
            )
            result = write_investigation(investigation, output_dir=investigation_output_dir)
            context["investigation_summary"] = result.summary
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="investigation_summary_exported",
                details={"investigation_id": investigation_id, "reference_count": result.summary.get("reference_count"), "timeline_event_count": result.summary.get("timeline_event_count")},
                artifact_refs=[_artifact_ref("INVESTIGATION", investigation_id, result.summary_path)],
                now=now,
            )
        if step_type == "GENERATE_CLAIM_DRAFTS":
            investigation_id = str(inputs.get("investigation_id") or context.get("investigation_id") or "")
            if not investigation_id:
                return _step_result(step=step, status="SKIPPED", reason="investigation_not_available", now=now)
            drafts = generate_claim_drafts(
                investigation_id=investigation_id,
                investigation_output_dir=investigation_output_dir,
                evidence_output_dir=evidence_output_dir,
                draft_output_dir=claim_draft_output_dir,
                now=now,
            )
            context["claim_draft_count"] = len(drafts)
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="claim_drafts_generated",
                details={"investigation_id": investigation_id, "draft_count": len(drafts)},
                artifact_refs=[_artifact_ref("ARTIFACT", "claim_draft_queue", claim_draft_output_dir / f"{investigation_id}_claim_drafts.json")],
                now=now,
            )
        if step_type == "EXPORT_CLAIM_DRAFT_SUMMARY":
            investigation_id = str(inputs.get("investigation_id") or context.get("investigation_id") or "")
            if not investigation_id:
                return _step_result(step=step, status="SKIPPED", reason="investigation_not_available", now=now)
            summary = export_claim_draft_summary(investigation_id=investigation_id, draft_output_dir=claim_draft_output_dir)
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="claim_draft_summary_exported",
                details=summary,
                artifact_refs=[_artifact_ref("ARTIFACT", "claim_draft_summary", claim_draft_output_dir / f"{investigation_id}_claim_draft_summary.json")],
                now=now,
            )
        if step_type == "REVIEW_DRAFT_CLAIMS":
            investigation_id = str(inputs.get("investigation_id") or context.get("investigation_id") or "")
            if not investigation_id:
                return _step_result(step=step, status="SKIPPED", reason="investigation_not_available", now=now)
            reviews = generate_claim_review_queue(
                investigation_id=investigation_id,
                claims_output_dir=claims_output_dir,
                review_output_dir=claim_review_output_dir,
                investigation_output_dir=investigation_output_dir,
                now=now,
            )
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="claim_review_queue_generated",
                details={"investigation_id": investigation_id, "review_count": len(reviews)},
                artifact_refs=[_artifact_ref("ARTIFACT", "claim_review_queue", claim_review_output_dir / f"{investigation_id}_claim_reviews.json")],
                now=now,
            )
        if step_type == "ACTIVATE_APPROVED_CLAIM":
            claim_id = str(inputs.get("claim_id") or "")
            if not claim_id:
                return _step_result(step=step, status="SKIPPED", reason="claim_id_not_available", now=now)
            if not inputs.get("operator_approved"):
                return _step_result(step=step, status="SKIPPED", reason="operator_approval_required", now=now)
            result = activate_claim(
                claim_id,
                operator_approved=True,
                reviewer=str(inputs.get("reviewer") or "operator"),
                claims_output_dir=claims_output_dir,
                review_output_dir=claim_review_output_dir,
                investigation_output_dir=investigation_output_dir,
                now=now,
            )
            return _step_result(step=step, status="COMPLETE", reason="claim_activated", details={"claim_id": claim_id, "claim_status": result["claim"]["status"]}, now=now)
        if step_type == "REJECT_DRAFT_CLAIM":
            claim_id = str(inputs.get("claim_id") or "")
            if not claim_id:
                return _step_result(step=step, status="SKIPPED", reason="claim_id_not_available", now=now)
            record = reject_claim(
                claim_id,
                reviewer=str(inputs.get("reviewer") or "operator"),
                claims_output_dir=claims_output_dir,
                review_output_dir=claim_review_output_dir,
                investigation_output_dir=investigation_output_dir,
                now=now,
            )
            return _step_result(step=step, status="COMPLETE", reason="claim_rejected", details={"claim_id": claim_id, "review_status": record["review_status"]}, now=now)
        if step_type == "EXPORT_CLAIM_REVIEW_SUMMARY":
            investigation_id = str(inputs.get("investigation_id") or context.get("investigation_id") or "")
            if not investigation_id:
                return _step_result(step=step, status="SKIPPED", reason="investigation_not_available", now=now)
            summary = export_claim_review_summary(investigation_id=investigation_id, review_output_dir=claim_review_output_dir)
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="claim_review_summary_exported",
                details=summary,
                artifact_refs=[_artifact_ref("ARTIFACT", "claim_review_summary", claim_review_output_dir / f"{investigation_id}_claim_review_summary.json")],
                now=now,
            )
        if step_type == "EXPORT_SUMMARY":
            return _step_result(step=step, status="COMPLETE", reason="workflow_summary_exported", now=now)
        return _step_result(step=step, status="SKIPPED", reason="unsupported_in_rwf1_minimal_runner", now=now)
    except Exception as exc:  # pragma: no cover - defensive fail-closed record
        return _step_result(step=step, status="FAILED", reason="step_exception", details={"error": str(exc)}, now=now)


def _existing_artifact_step(step: Mapping[str, Any], *, path: Path, target_kind: str, reason: str, now: datetime) -> dict[str, Any]:
    if path.exists():
        return _step_result(
            step=step,
            status="COMPLETE",
            reason=reason,
            artifact_refs=[_artifact_ref(target_kind, path.stem, path)],
            now=now,
        )
    return _step_result(step=step, status="SKIPPED", reason=f"{path.name}_not_available", now=now)


def create_or_locate_investigation(
    workflow: Mapping[str, Any],
    *,
    inputs: Mapping[str, Any] | None = None,
    output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> tuple[dict[str, Any], bool]:
    payload = dict(inputs or {})
    investigation_id = str(payload.get("investigation_id") or (workflow.get("metadata") or {}).get("investigation_id") or "")
    if investigation_id:
        try:
            return load_investigation(investigation_id, output_dir=output_dir), False
        except FileNotFoundError:
            pass
    title = str(payload.get("title") or (workflow.get("metadata") or {}).get("investigation_title") or workflow.get("title") or "")
    for row in list_investigations(output_dir=output_dir):
        if investigation_id and row.get("investigation_id") == investigation_id:
            return load_investigation(str(row["investigation_id"]), output_dir=output_dir), False
        if title and row.get("title") == title:
            return load_investigation(str(row["investigation_id"]), output_dir=output_dir), False
    result = create_investigation(
        investigation_id=investigation_id or str(payload.get("new_investigation_id") or _investigation_population_id(str(workflow.get("workflow_id") or "workflow"), title)),
        title=title or "Workflow Investigation",
        description=str(payload.get("description") or workflow.get("description") or "Investigation populated by Canonical Research Workflow."),
        hypothesis=str(payload.get("hypothesis") or "Deterministic workflow artifacts can support repeatable research review."),
        owner=str(payload.get("owner") or workflow.get("owner") or "operator"),
        tags=tuple(payload.get("tags") or workflow.get("tags") or ()),
        now=now,
        output_dir=output_dir,
    )
    return result.investigation, True


def populate_investigation_evidence(
    *,
    workflow_run_id: str,
    investigation_id: str,
    artifact_refs: Sequence[Mapping[str, Any]],
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    evidence_output_dir: Path = DEFAULT_EVIDENCE_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> InvestigationPopulationResult:
    timestamp = _coerce_now(now)
    skipped: list[dict[str, Any]] = []
    if not investigation_id:
        return _population_result(workflow_run_id, "", False, 0, 0, 0, [{"reason": "missing_investigation_id"}], timestamp)
    try:
        investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
        created_new = False
    except FileNotFoundError:
        return _population_result(workflow_run_id, investigation_id, False, 0, 0, 0, [{"reason": "investigation_not_found", "investigation_id": investigation_id}], timestamp)
    evidence_attached = 0
    refs_created = 0
    timeline_before = len(investigation.get("timeline") or [])
    existing_evidence_refs = {str(ref.get("target_id")) for ref in investigation.get("references") or [] if ref.get("reference_type") == "evidence"}
    for artifact_ref in artifact_refs:
        path = Path(str(artifact_ref.get("target_path") or ""))
        if not path.exists():
            skipped.append({"reason": "artifact_missing", "target_kind": artifact_ref.get("target_kind"), "target_path": str(path)})
            continue
        evidence_id = _evidence_id_for_artifact(investigation_id, artifact_ref)
        if evidence_id in existing_evidence_refs:
            skipped.append({"reason": "duplicate_evidence", "evidence_id": evidence_id})
            continue
        evidence = _load_or_create_population_evidence(
            investigation_id=investigation_id,
            evidence_id=evidence_id,
            artifact_ref=artifact_ref,
            evidence_output_dir=evidence_output_dir,
            now=timestamp,
        )
        evidence = attach_evidence_reference(
            evidence,
            attachment_type=_attachment_type_for_target_kind(str(artifact_ref.get("target_kind") or "ARTIFACT")),
            reference_id=str(artifact_ref.get("target_id") or path.stem),
            artifact_path=str(path),
            label=str(artifact_ref.get("target_kind") or "Artifact"),
            provenance=build_provenance_envelope(
                source_component="canonical_research_workflow",
                source_artifact=str(path),
                parent_fingerprint=evidence.get("deterministic_fingerprint"),
                parent_schema_version=evidence.get("schema_version"),
                transform_name="rwf2_attach_artifact_evidence",
                created_at=timestamp,
            ),
            now=timestamp,
        )
        evidence_result = write_evidence(evidence, output_dir=evidence_output_dir)
        investigation = attach_investigation_evidence_reference(
            investigation,
            evidence_id=evidence_id,
            artifact_path=str(evidence_result.evidence_path),
            label=evidence.get("title"),
            provenance={"source": "canonical_research_workflow", "operation": "attach_population_evidence", "workflow_run_id": workflow_run_id},
            now=timestamp,
        )
        evidence_attached += 1
        refs_created += 2
        existing_evidence_refs.add(evidence_id)
    write_investigation(investigation, output_dir=investigation_output_dir)
    return _population_result(
        workflow_run_id,
        investigation_id,
        created_new,
        evidence_attached,
        refs_created,
        len(investigation.get("timeline") or []) - timeline_before,
        skipped,
        timestamp,
    )


def generate_claim_drafts(
    *,
    investigation_id: str,
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    evidence_output_dir: Path = DEFAULT_EVIDENCE_OUTPUT_DIR,
    draft_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_drafts",
    now: datetime | str | None = None,
) -> list[dict[str, Any]]:
    timestamp = _coerce_now(now)
    investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
    evidence_refs = [ref for ref in investigation.get("references") or [] if ref.get("reference_type") == "evidence"]
    drafts = load_claim_drafts(investigation_id=investigation_id, draft_output_dir=draft_output_dir)
    by_id = {str(draft.get("draft_id")): dict(draft) for draft in drafts}
    generated_ids: list[str] = []
    for ref in evidence_refs:
        evidence_id = str(ref.get("target_id") or ref.get("legacy_reference_id") or "")
        if not evidence_id:
            continue
        try:
            evidence = load_evidence(evidence_id, output_dir=evidence_output_dir)
        except FileNotFoundError:
            continue
        draft = _draft_from_evidence(evidence, investigation_id=investigation_id, now=timestamp)
        if draft is None:
            continue
        by_id.setdefault(str(draft["draft_id"]), draft)
        generated_ids.append(str(draft["draft_id"]))
    rows = sorted(by_id.values(), key=lambda row: str(row.get("draft_id")))
    _write_claim_drafts(investigation_id, rows, draft_output_dir=draft_output_dir)
    if generated_ids:
        investigation = append_investigation_event(
            investigation,
            event_type="CLAIM_DRAFT_GENERATED",
            artifact_reference=_artifact_ref("ARTIFACT", "claim_draft_queue", draft_output_dir / f"{investigation_id}_claim_drafts.json"),
            provenance={"source": "canonical_research_workflow", "operation": "generate_claim_drafts", "draft_count": len(generated_ids)},
            now=timestamp,
        )
        write_investigation(investigation, output_dir=investigation_output_dir)
    export_claim_draft_summary(investigation_id=investigation_id, draft_output_dir=draft_output_dir)
    return rows


def accept_claim_draft(
    draft_id: str,
    *,
    draft_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_drafts",
    evidence_output_dir: Path = DEFAULT_EVIDENCE_OUTPUT_DIR,
    claims_output_dir: Path = DEFAULT_CLAIMS_OUTPUT_DIR,
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    draft = load_claim_draft(draft_id, draft_output_dir=draft_output_dir)
    investigation_id = str(draft["investigation_id"])
    claim_id = f"claim_{stable_hash({'draft_id': draft_id, 'investigation_id': investigation_id})[:16]}"
    result = create_claim(
        investigation_id=investigation_id,
        claim_id=claim_id,
        title=str(draft["proposed_claim_title"]),
        statement=str(draft["proposed_statement"]),
        rationale=str(draft["rationale_template"]),
        claim_classification=str(draft["proposed_claim_classification"]),
        claim_type=str(draft["proposed_claim_type"]),
        confidence=str(draft["confidence_suggestion"]),
        status="DRAFT",
        provenance=dict(draft.get("provenance") or {}),
        now=timestamp,
        output_dir=claims_output_dir,
    )
    claim = result.claim
    for bucket, attach_fn in (
        ("supporting_evidence_refs", attach_supporting_evidence),
        ("contradicting_evidence_refs", attach_contradicting_evidence),
        ("related_evidence_refs", attach_related_evidence),
    ):
        for ref in draft.get(bucket) or []:
            evidence_id = str(ref.get("target_id") or ref.get("evidence_id") or "")
            if not evidence_id:
                continue
            claim = attach_fn(claim, load_evidence(evidence_id, output_dir=evidence_output_dir), now=timestamp)
    result = write_claim(claim, output_dir=claims_output_dir)
    draft = _update_draft_status(draft, status="ACCEPTED", now=timestamp)
    _upsert_claim_draft(draft, draft_output_dir=draft_output_dir)
    investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
    investigation = append_investigation_event(
        investigation,
        event_type="CLAIM_DRAFT_ACCEPTED",
        artifact_reference=_artifact_ref("CLAIM", claim_id, result.claim_path),
        provenance={"source": "canonical_research_workflow", "operation": "accept_claim_draft", "draft_id": draft_id},
        now=timestamp,
    )
    write_investigation(investigation, output_dir=investigation_output_dir)
    return {"draft": draft, "claim": result.claim, "claim_path": str(result.claim_path), "summary": result.summary}


def reject_claim_draft(
    draft_id: str,
    *,
    draft_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_drafts",
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    draft = _update_draft_status(load_claim_draft(draft_id, draft_output_dir=draft_output_dir), status="REJECTED", now=timestamp)
    _upsert_claim_draft(draft, draft_output_dir=draft_output_dir)
    investigation_id = str(draft["investigation_id"])
    investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
    investigation = append_investigation_event(
        investigation,
        event_type="CLAIM_DRAFT_REJECTED",
        artifact_reference=_artifact_ref("ARTIFACT", draft_id, draft_output_dir / f"{investigation_id}_claim_drafts.json"),
        provenance={"source": "canonical_research_workflow", "operation": "reject_claim_draft", "draft_id": draft_id},
        now=timestamp,
    )
    write_investigation(investigation, output_dir=investigation_output_dir)
    return draft


def generate_claim_review_queue(
    *,
    investigation_id: str,
    claims_output_dir: Path = DEFAULT_CLAIMS_OUTPUT_DIR,
    review_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_reviews",
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> list[dict[str, Any]]:
    timestamp = _coerce_now(now)
    reviews = load_claim_reviews(investigation_id=investigation_id, review_output_dir=review_output_dir)
    by_claim = {str(review.get("claim_id")): dict(review) for review in reviews}
    claims = [row for row in list_claims(output_dir=claims_output_dir, investigation_id=investigation_id)]
    for claim_summary in claims:
        claim_id = str(claim_summary.get("claim_id") or "")
        if not claim_id:
            continue
        claim = load_claim(claim_id, output_dir=claims_output_dir)
        if claim.get("status") not in {"DRAFT", "ACTIVE"}:
            continue
        by_claim[claim_id] = create_claim_review_record(
            claim,
            reviewer="operator",
            review_status="PENDING_REVIEW",
            now=timestamp,
        )
    rows = sorted(by_claim.values(), key=lambda row: str(row.get("review_id")))
    _write_claim_reviews(investigation_id, rows, review_output_dir=review_output_dir)
    if rows:
        investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
        investigation = append_investigation_event(
            investigation,
            event_type="CLAIM_REVIEW_CREATED",
            artifact_reference=_artifact_ref("ARTIFACT", "claim_review_queue", review_output_dir / f"{investigation_id}_claim_reviews.json"),
            provenance={"source": "canonical_research_workflow", "operation": "generate_claim_review_queue", "review_count": len(rows)},
            now=timestamp,
        )
        write_investigation(investigation, output_dir=investigation_output_dir)
    export_claim_review_summary(investigation_id=investigation_id, review_output_dir=review_output_dir)
    return rows


def create_claim_review_record(
    claim: Mapping[str, Any],
    *,
    reviewer: str = "operator",
    review_status: str = "PENDING_REVIEW",
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    if review_status not in VALID_REVIEW_STATUSES:
        raise ValueError(f"Unsupported review status: {review_status}")
    readiness = claim_review_readiness(claim)
    validation = dict(claim.get("validation") or {})
    record = {
        "schema_version": CLAIM_REVIEW_SCHEMA_VERSION,
        "review_id": _claim_review_id(str(claim.get("investigation_id")), str(claim.get("claim_id"))),
        "investigation_id": claim.get("investigation_id"),
        "claim_id": claim.get("claim_id"),
        "created_at": timestamp.isoformat(),
        "updated_at": timestamp.isoformat(),
        "reviewer": reviewer,
        "review_status": review_status,
        "readiness": readiness,
        "validation_status": validation.get("status") or refresh_claim_validation(claim, now=timestamp).get("validation", {}).get("status"),
        "evidence_summary": {
            "supporting": [ref.get("evidence_id") or ref.get("target_id") for ref in claim.get("supporting_evidence") or []],
            "contradicting": [ref.get("evidence_id") or ref.get("target_id") for ref in claim.get("contradicting_evidence") or []],
            "related": [ref.get("evidence_id") or ref.get("target_id") for ref in claim.get("related_evidence") or []],
        },
        "supporting_evidence_count": len(claim.get("supporting_evidence") or []),
        "contradicting_evidence_count": len(claim.get("contradicting_evidence") or []),
        "related_evidence_count": len(claim.get("related_evidence") or []),
        "review_notes_ref": None,
        "provenance": build_provenance_envelope(
            source_component="canonical_research_workflow",
            source_artifact=f"claim:{claim.get('claim_id')}",
            parent_fingerprint=str(claim.get("deterministic_fingerprint") or ""),
            parent_schema_version=str(claim.get("schema_version") or ""),
            transform_name="rwf4_claim_review_record",
            created_at=timestamp,
        ),
        "guardrails": _guardrails(),
    }
    record["deterministic_fingerprint"] = stable_hash(record)
    return record


def claim_review_readiness(claim: Mapping[str, Any]) -> str:
    if claim.get("schema_version") != "ie3_canonical_claim_v1":
        return "NOT_APPLICABLE"
    if claim.get("status") == "ACTIVE":
        return "ALREADY_ACTIVE"
    if claim.get("status") != "DRAFT":
        return "NOT_APPLICABLE"
    validation = refresh_claim_validation(claim).get("validation") or {}
    status = validation.get("status")
    if status == "INVALID":
        return "INVALID_REFERENCES"
    if status == "CONTRADICTED":
        return "CONTRADICTED"
    if status in {"INSUFFICIENT_EVIDENCE", "PARTIALLY_SUPPORTED"}:
        return "NEEDS_EVIDENCE"
    if status == "SUPPORTED":
        return "READY_FOR_OPERATOR_REVIEW"
    return "NOT_APPLICABLE"


def activate_claim(
    claim_id: str,
    *,
    operator_approved: bool = False,
    reviewer: str = "operator",
    claims_output_dir: Path = DEFAULT_CLAIMS_OUTPUT_DIR,
    review_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_reviews",
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    if not operator_approved:
        raise PermissionError("operator_approval_required")
    claim = load_claim(claim_id, output_dir=claims_output_dir)
    readiness = claim_review_readiness(claim)
    if readiness != "READY_FOR_OPERATOR_REVIEW":
        raise ValueError(f"claim_not_ready_for_activation:{readiness}")
    updated = dict(claim)
    updated["status"] = "ACTIVE"
    updated["updated_at"] = timestamp.isoformat()
    updated["provenance"] = dict(updated.get("provenance") or {})
    updated["provenance"]["activation"] = {"source": "canonical_research_workflow", "reviewer": reviewer, "operator_approved": True, "activated_at": timestamp.isoformat()}
    claim_result = write_claim(updated, output_dir=claims_output_dir)
    record = create_claim_review_record(claim_result.claim, reviewer=reviewer, review_status="APPROVED_FOR_ACTIVATION", now=timestamp)
    _upsert_claim_review(record, review_output_dir=review_output_dir)
    investigation_id = str(claim_result.claim["investigation_id"])
    investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
    investigation = append_investigation_event(
        investigation,
        event_type="CLAIM_ACTIVATED",
        artifact_reference=_artifact_ref("CLAIM", claim_id, claim_result.claim_path),
        provenance={"source": "canonical_research_workflow", "operation": "activate_claim", "review_id": record["review_id"]},
        now=timestamp,
    )
    write_investigation(investigation, output_dir=investigation_output_dir)
    return {"claim": claim_result.claim, "review": record}


def reject_claim(
    claim_id: str,
    *,
    reviewer: str = "operator",
    claims_output_dir: Path = DEFAULT_CLAIMS_OUTPUT_DIR,
    review_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_reviews",
    investigation_output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    claim = load_claim(claim_id, output_dir=claims_output_dir)
    record = create_claim_review_record(claim, reviewer=reviewer, review_status="REJECTED", now=timestamp)
    _upsert_claim_review(record, review_output_dir=review_output_dir)
    investigation_id = str(claim["investigation_id"])
    investigation = load_investigation(investigation_id, output_dir=investigation_output_dir)
    investigation = append_investigation_event(
        investigation,
        event_type="CLAIM_REJECTED",
        artifact_reference=_artifact_ref("CLAIM", claim_id, claims_output_dir / f"{claim_id}.json"),
        provenance={"source": "canonical_research_workflow", "operation": "reject_claim", "review_id": record["review_id"]},
        now=timestamp,
    )
    write_investigation(investigation, output_dir=investigation_output_dir)
    return record


def load_claim_reviews(*, investigation_id: str | None = None, review_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_reviews") -> list[dict[str, Any]]:
    if investigation_id:
        path = review_output_dir / f"{investigation_id}_claim_reviews.json"
        if not path.exists():
            return []
        return list(json.loads(path.read_text(encoding="utf-8")).get("reviews") or [])
    rows: list[dict[str, Any]] = []
    if not review_output_dir.exists():
        return rows
    for path in sorted(review_output_dir.glob("*_claim_reviews.json")):
        rows.extend(json.loads(path.read_text(encoding="utf-8")).get("reviews") or [])
    return rows


def load_claim_review(review_id: str, *, review_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_reviews") -> dict[str, Any]:
    for review in load_claim_reviews(review_output_dir=review_output_dir):
        if review.get("review_id") == review_id:
            return dict(review)
    raise FileNotFoundError(review_id)


def export_claim_review_summary(*, investigation_id: str, review_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_reviews") -> dict[str, Any]:
    reviews = load_claim_reviews(investigation_id=investigation_id, review_output_dir=review_output_dir)
    summary = {
        "schema_version": CLAIM_REVIEW_SUMMARY_SCHEMA_VERSION,
        "investigation_id": investigation_id,
        "review_count": len(reviews),
        "review_status_counts": _counts(review.get("review_status") for review in reviews),
        "readiness_counts": _counts(review.get("readiness") for review in reviews),
        "guardrails": _guardrails(),
        "deterministic_fingerprint": stable_hash({"investigation_id": investigation_id, "reviews": reviews}),
    }
    review_output_dir.mkdir(parents=True, exist_ok=True)
    (review_output_dir / f"{investigation_id}_claim_review_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def load_claim_drafts(*, investigation_id: str | None = None, draft_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_drafts") -> list[dict[str, Any]]:
    if investigation_id:
        path = draft_output_dir / f"{investigation_id}_claim_drafts.json"
        if not path.exists():
            return []
        return list(json.loads(path.read_text(encoding="utf-8")).get("drafts") or [])
    rows: list[dict[str, Any]] = []
    if not draft_output_dir.exists():
        return rows
    for path in sorted(draft_output_dir.glob("*_claim_drafts.json")):
        rows.extend(json.loads(path.read_text(encoding="utf-8")).get("drafts") or [])
    return rows


def load_claim_draft(draft_id: str, *, draft_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_drafts") -> dict[str, Any]:
    for draft in load_claim_drafts(draft_output_dir=draft_output_dir):
        if draft.get("draft_id") == draft_id:
            return dict(draft)
    raise FileNotFoundError(draft_id)


def export_claim_draft_summary(*, investigation_id: str, draft_output_dir: Path = DEFAULT_OUTPUT_DIR / "claim_drafts") -> dict[str, Any]:
    drafts = load_claim_drafts(investigation_id=investigation_id, draft_output_dir=draft_output_dir)
    summary = {
        "schema_version": CLAIM_DRAFT_SUMMARY_SCHEMA_VERSION,
        "investigation_id": investigation_id,
        "draft_count": len(drafts),
        "status_counts": _counts(draft.get("draft_status") for draft in drafts),
        "validation_preview_counts": _counts(draft.get("validation_preview") for draft in drafts),
        "guardrails": _guardrails(),
        "deterministic_fingerprint": stable_hash({"investigation_id": investigation_id, "drafts": drafts}),
    }
    draft_output_dir.mkdir(parents=True, exist_ok=True)
    (draft_output_dir / f"{investigation_id}_claim_draft_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def _load_or_create_population_evidence(
    *,
    investigation_id: str,
    evidence_id: str,
    artifact_ref: Mapping[str, Any],
    evidence_output_dir: Path,
    now: datetime,
) -> dict[str, Any]:
    try:
        return load_evidence(evidence_id, output_dir=evidence_output_dir)
    except FileNotFoundError:
        pass
    target_kind = str(artifact_ref.get("target_kind") or "ARTIFACT")
    result = create_evidence(
        investigation_id=investigation_id,
        evidence_id=evidence_id,
        title=f"{target_kind.replace('_', ' ').title()} Evidence",
        summary=f"Deterministic workflow evidence attached from {artifact_ref.get('target_path')}.",
        evidence_classification=_evidence_classification_for_target_kind(target_kind),
        evidence_type=_attachment_type_for_target_kind(target_kind),
        source_component="canonical_research_workflow",
        provenance=build_provenance_envelope(
            source_component="canonical_research_workflow",
            source_artifact=str(artifact_ref.get("target_path") or ""),
            transform_name="rwf2_create_population_evidence",
            created_at=now,
        ),
        now=now,
        output_dir=evidence_output_dir,
    )
    return result.evidence


def _draft_from_evidence(evidence: Mapping[str, Any], *, investigation_id: str, now: datetime) -> dict[str, Any] | None:
    source = _source_payload_from_evidence(evidence)
    classification = str(evidence.get("evidence_classification") or "")
    evidence_type = str(evidence.get("evidence_type") or "")
    family = ""
    title = ""
    statement = ""
    claim_classification = "ANALYTICS"
    claim_type = "deterministic_evidence_review"
    confidence = "LOW"
    preview = "PARTIALLY_SUPPORTED"
    if classification == "INSIGHT":
        insights = source if isinstance(source, list) else source.get("insights", []) if isinstance(source, Mapping) else []
        first = insights[0] if insights else {}
        insight_title = str(first.get("title") or evidence.get("title") or "")
        if "No meaningful analytics changes" in insight_title:
            family = "analytics_unchanged"
            title = "Analytics unchanged over compared window"
            statement = "Current saved-query analytics are stable over the compared window represented by the attached analytics insight."
            preview = "SUPPORTED"
        else:
            family = "analytics_changed"
            title = "Analytics changed over compared window"
            statement = "A deterministic analytics insight indicates a material analytics change over the compared window."
    elif classification == "DIFF":
        status = str(source.get("classification") or source.get("status") or "") if isinstance(source, Mapping) else ""
        if status and status not in {"UNCHANGED", "NO_PRIOR_EXECUTION"}:
            family = "analytics_changed"
            title = "Analytics result changed"
            statement = "A deterministic analytics diff reports changed metrics or groups for the compared query result."
        elif status == "UNCHANGED":
            family = "analytics_unchanged"
            title = "Analytics result unchanged"
            statement = "A deterministic analytics diff reports no meaningful result change for the compared query result."
            preview = "SUPPORTED"
    elif classification == "MORNING_BRIEF" and "change" in evidence_type:
        change_count = len(source.get("changes") or []) if isinstance(source, Mapping) else 0
        if change_count:
            family = "morning_brief_changed"
            title = "Morning Brief domain changed"
            statement = "The Morning Brief change explanation reports at least one deterministic domain change."
            claim_classification = "OPERATIONAL"
            claim_type = "brief_change_review"
        else:
            family = "morning_brief_no_meaningful_change"
            title = "Morning Brief has no meaningful change"
            statement = "The Morning Brief change explanation reports no meaningful change beyond generated timestamps or fingerprints."
            claim_classification = "OPERATIONAL"
            claim_type = "brief_change_review"
            preview = "SUPPORTED"
    elif classification in {"DISCOVERY", "CANDIDATE"}:
        family = "research_candidate_review"
        title = "Research candidate merits manual review"
        statement = "A deterministic research discovery or candidate-review artifact identifies a candidate for manual research review."
        claim_classification = "DISCOVERY"
        claim_type = "manual_research_candidate"
    elif classification == "OPERATIONAL":
        family = "operational_state_stable"
        title = "Operational evidence is stable for research review"
        statement = "Attached operational evidence indicates platform state can be treated as coherent for research review purposes only."
        claim_classification = "OPERATIONAL"
        claim_type = "operational_research_context"
        preview = "SUPPORTED"
    if not family:
        return None
    evidence_ref = _draft_evidence_ref(evidence, relationship="supports", now=now)
    draft = {
        "schema_version": CLAIM_DRAFT_SCHEMA_VERSION,
        "draft_id": _claim_draft_id(investigation_id, family, [str(evidence.get("evidence_id"))], statement),
        "investigation_id": investigation_id,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "source_evidence_ids": [str(evidence.get("evidence_id"))],
        "proposed_claim_title": title,
        "proposed_statement": statement,
        "proposed_claim_classification": claim_classification,
        "proposed_claim_type": claim_type,
        "rationale_template": f"Deterministic draft generated from `{family}` evidence. Manual review is required before activation.",
        "supporting_evidence_refs": [evidence_ref],
        "contradicting_evidence_refs": [],
        "related_evidence_refs": [],
        "draft_status": "NEEDS_REVIEW",
        "confidence_suggestion": confidence,
        "validation_preview": preview,
        "provenance": build_provenance_envelope(
            source_component="canonical_research_workflow",
            source_artifact=str((evidence.get("attachments") or [{}])[0].get("target_path") or ""),
            parent_fingerprint=str(evidence.get("deterministic_fingerprint") or ""),
            parent_schema_version=str(evidence.get("schema_version") or ""),
            transform_name=f"rwf3_claim_draft_{family}",
            created_at=now,
        ),
        "guardrails": _guardrails(),
    }
    draft["deterministic_fingerprint"] = stable_hash(draft)
    return draft


def _source_payload_from_evidence(evidence: Mapping[str, Any]) -> Any:
    attachments = list(evidence.get("attachments") or [])
    if not attachments:
        return {}
    path = Path(str(attachments[0].get("target_path") or attachments[0].get("artifact_path") or ""))
    if not path.exists() or path.suffix.lower() not in {".json", ".jsonl"}:
        return {}
    try:
        if path.suffix.lower() == ".jsonl":
            return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _draft_evidence_ref(evidence: Mapping[str, Any], *, relationship: str, now: datetime) -> dict[str, Any]:
    return {
        **build_provenance_reference(
            reference_type="claim_draft_evidence",
            target_id=str(evidence.get("evidence_id")),
            target_kind="EVIDENCE",
            target_schema_version=str(evidence.get("schema_version")),
            target_fingerprint=str(evidence.get("deterministic_fingerprint")),
            relationship=relationship,
            source_component="canonical_research_workflow",
            created_at=now,
        ),
        "evidence_id": evidence.get("evidence_id"),
        "evidence_fingerprint": evidence.get("deterministic_fingerprint"),
        "evidence_status": evidence.get("status"),
        "evidence_title": evidence.get("title"),
    }


def build_provenance_reference(**kwargs: Any) -> dict[str, Any]:
    from mgc_v05l.execution_core.track_b_canonical_reference_envelope import build_reference

    return build_reference(**kwargs)


def _write_claim_drafts(investigation_id: str, drafts: Sequence[Mapping[str, Any]], *, draft_output_dir: Path) -> None:
    draft_output_dir.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": "rwf3_claim_draft_queue_v1", "investigation_id": investigation_id, "drafts": [dict(draft) for draft in drafts], "guardrails": _guardrails()}
    (draft_output_dir / f"{investigation_id}_claim_drafts.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _upsert_claim_draft(draft: Mapping[str, Any], *, draft_output_dir: Path) -> None:
    investigation_id = str(draft["investigation_id"])
    rows = {str(item.get("draft_id")): dict(item) for item in load_claim_drafts(investigation_id=investigation_id, draft_output_dir=draft_output_dir)}
    rows[str(draft["draft_id"])] = dict(draft)
    _write_claim_drafts(investigation_id, sorted(rows.values(), key=lambda row: str(row.get("draft_id"))), draft_output_dir=draft_output_dir)
    export_claim_draft_summary(investigation_id=investigation_id, draft_output_dir=draft_output_dir)


def _update_draft_status(draft: Mapping[str, Any], *, status: str, now: datetime) -> dict[str, Any]:
    if status not in VALID_DRAFT_STATUSES:
        raise ValueError(f"Unsupported draft status: {status}")
    updated = dict(draft)
    updated["draft_status"] = status
    updated["updated_at"] = now.isoformat()
    updated["deterministic_fingerprint"] = stable_hash(updated)
    return updated


def _claim_draft_id(investigation_id: str, family: str, evidence_ids: Sequence[str], statement: str) -> str:
    return f"draft_{stable_hash({'investigation_id': investigation_id, 'family': family, 'evidence_ids': sorted(evidence_ids), 'statement': statement})[:18]}"


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _write_claim_reviews(investigation_id: str, reviews: Sequence[Mapping[str, Any]], *, review_output_dir: Path) -> None:
    review_output_dir.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": "rwf4_claim_review_queue_v1", "investigation_id": investigation_id, "reviews": [dict(review) for review in reviews], "guardrails": _guardrails()}
    (review_output_dir / f"{investigation_id}_claim_reviews.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _upsert_claim_review(review: Mapping[str, Any], *, review_output_dir: Path) -> None:
    investigation_id = str(review["investigation_id"])
    rows = {str(item.get("review_id")): dict(item) for item in load_claim_reviews(investigation_id=investigation_id, review_output_dir=review_output_dir)}
    rows[str(review["review_id"])] = dict(review)
    _write_claim_reviews(investigation_id, sorted(rows.values(), key=lambda row: str(row.get("review_id"))), review_output_dir=review_output_dir)
    export_claim_review_summary(investigation_id=investigation_id, review_output_dir=review_output_dir)


def _claim_review_id(investigation_id: str, claim_id: str) -> str:
    return f"review_{stable_hash({'investigation_id': investigation_id, 'claim_id': claim_id})[:18]}"


def _artifact_refs_for_population_step(
    step_type: str,
    *,
    context_refs: Sequence[Mapping[str, Any]],
    saved_query_output_dir: Path,
    morning_brief_output_dir: Path,
    execution_id: str,
) -> list[dict[str, Any]]:
    desired = {
        "ATTACH_QUERY_EXECUTION": {"EXECUTION_RECORD"},
        "ATTACH_QUERY_RESULT": {"ANALYTICS_RESULT"},
        "ATTACH_ANALYTICS_DIFF": {"ANALYTICS_DIFF"},
        "ATTACH_ANALYTICS_INSIGHT": {"ANALYTICS_INSIGHT"},
        "ATTACH_MORNING_BRIEF": {"MORNING_BRIEF"},
        "ATTACH_BRIEF_CHANGE": {"MORNING_BRIEF"},
    }.get(step_type)
    if step_type == "ATTACH_EVIDENCE":
        return _dedupe_artifact_refs([dict(ref) for ref in context_refs])
    refs = [dict(ref) for ref in context_refs if ref.get("target_kind") in (desired or set())]
    if step_type == "ATTACH_MORNING_BRIEF":
        refs = [ref for ref in refs if Path(str(ref.get("target_path") or "")).name == "cae9_morning_brief.json"]
    if step_type == "ATTACH_BRIEF_CHANGE":
        refs = [ref for ref in refs if Path(str(ref.get("target_path") or "")).name == "mb3_latest_change_explanation.json"]
    if step_type == "ATTACH_QUERY_EXECUTION" and not refs:
        refs.append(_artifact_ref("EXECUTION_RECORD", execution_id or "latest", saved_query_output_dir / EXECUTION_SUMMARY_JSON))
    if step_type == "ATTACH_QUERY_RESULT" and not refs and execution_id:
        refs.append(_artifact_ref("ANALYTICS_RESULT", execution_id, saved_query_output_dir / RESULT_SNAPSHOT_DIR / f"{execution_id}.json"))
    if step_type == "ATTACH_ANALYTICS_DIFF" and not refs:
        refs.append(_artifact_ref("ANALYTICS_DIFF", "latest", saved_query_output_dir / RESULT_DIFF_JSON))
    if step_type == "ATTACH_ANALYTICS_INSIGHT" and not refs:
        refs.append(_artifact_ref("ANALYTICS_INSIGHT", "latest", saved_query_output_dir / LATEST_INSIGHTS_JSON))
    if step_type == "ATTACH_MORNING_BRIEF" and not refs:
        refs.append(_artifact_ref("MORNING_BRIEF", "latest", morning_brief_output_dir / "cae9_morning_brief.json"))
    if step_type == "ATTACH_BRIEF_CHANGE" and not refs:
        refs = [_artifact_ref("MORNING_BRIEF", "brief_change_explanation", morning_brief_output_dir / "mb3_latest_change_explanation.json")]
    return _dedupe_artifact_refs(refs)


def _dedupe_artifact_refs(refs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for ref in refs:
        key = (str(ref.get("target_kind")), str(ref.get("target_id")), str(ref.get("target_path")))
        if key in seen:
            continue
        seen.add(key)
        rows.append(dict(ref))
    return rows


def _population_result(
    workflow_run_id: str,
    investigation_id: str,
    created_new: bool,
    evidence_count: int,
    reference_count: int,
    timeline_count: int,
    skipped: list[dict[str, Any]],
    timestamp: datetime,
) -> InvestigationPopulationResult:
    provenance = {
        "schema_version": "rwf2_investigation_population_provenance_v1",
        "source_component": "canonical_research_workflow",
        "workflow_run_id": workflow_run_id,
        "generated_at": timestamp.isoformat(),
    }
    payload = {
        "workflow_run_id": workflow_run_id,
        "investigation_id": investigation_id,
        "created_new_investigation": created_new,
        "evidence_attached_count": evidence_count,
        "references_created": reference_count,
        "timeline_events_added": timeline_count,
        "skipped_items": skipped,
        "provenance": provenance,
        "guardrails": _guardrails(),
    }
    return InvestigationPopulationResult(
        workflow_run_id=workflow_run_id,
        investigation_id=investigation_id,
        created_new_investigation=created_new,
        evidence_attached_count=evidence_count,
        references_created=reference_count,
        timeline_events_added=timeline_count,
        skipped_items=skipped,
        deterministic_fingerprint=stable_hash(payload),
        provenance=provenance,
        guardrails=_guardrails(),
    )


def _evidence_id_for_artifact(investigation_id: str, artifact_ref: Mapping[str, Any]) -> str:
    return f"evidence_{stable_hash({'investigation_id': investigation_id, 'target_kind': artifact_ref.get('target_kind'), 'target_path': artifact_ref.get('target_path')})[:20]}"


def _investigation_population_id(workflow_id: str, title: str) -> str:
    return f"inv_{stable_hash({'workflow_id': workflow_id, 'title': title})[:16]}"


def _attachment_type_for_target_kind(target_kind: str) -> str:
    return {
        "EXECUTION_RECORD": "execution_record",
        "ANALYTICS_RESULT": "analytics_result",
        "ANALYTICS_DIFF": "analytics_diff",
        "ANALYTICS_INSIGHT": "insight",
        "MORNING_BRIEF": "morning_brief",
        "RESEARCH_DISCOVERY_CANDIDATE": "research_discovery_candidate",
        "CANDIDATE_REVIEW": "candidate_review",
        "CONTEXT_SNAPSHOT": "context_snapshot",
        "OPERATIONAL_CERTIFICATION": "operational_certification",
        "SAFE_STATE": "safe_state",
        "GUARDIAN": "guardian",
        "RUNTIME_HEALTH": "runtime_health",
        "BOOKMARK": "bookmark",
    }.get(target_kind, "artifact")


def _evidence_classification_for_target_kind(target_kind: str) -> str:
    return {
        "EXECUTION_RECORD": "ANALYTICS",
        "ANALYTICS_RESULT": "ANALYTICS",
        "ANALYTICS_DIFF": "DIFF",
        "ANALYTICS_INSIGHT": "INSIGHT",
        "MORNING_BRIEF": "MORNING_BRIEF",
        "RESEARCH_DISCOVERY_CANDIDATE": "DISCOVERY",
        "CANDIDATE_REVIEW": "CANDIDATE",
        "CONTEXT_SNAPSHOT": "CONTEXT",
        "OPERATIONAL_CERTIFICATION": "OPERATIONAL",
        "SAFE_STATE": "OPERATIONAL",
        "GUARDIAN": "OPERATIONAL",
        "RUNTIME_HEALTH": "OPERATIONAL",
    }.get(target_kind, "ANALYTICS")


def summarize_workflow(workflow: Mapping[str, Any]) -> dict[str, Any]:
    steps = list(workflow.get("steps") or [])
    return {
        "schema_version": "rwf1_workflow_summary_v1",
        "workflow_id": workflow.get("workflow_id"),
        "title": workflow.get("title"),
        "workflow_type": workflow.get("workflow_type"),
        "status": workflow.get("status"),
        "step_count": len(steps),
        "step_types": [step.get("step_type") for step in steps],
        "guardrails": _guardrails(),
        "deterministic_fingerprint": workflow_fingerprint(workflow),
    }


def summarize_workflow_run(run: Mapping[str, Any]) -> dict[str, Any]:
    step_results = list(run.get("step_results") or [])
    return {
        "schema_version": "rwf1_workflow_run_summary_v1",
        "run_id": run.get("run_id"),
        "workflow_id": run.get("workflow_id"),
        "status": run.get("status"),
        "step_count": len(step_results),
        "complete_steps": sum(1 for row in step_results if row.get("status") == "COMPLETE"),
        "skipped_steps": sum(1 for row in step_results if row.get("status") == "SKIPPED"),
        "failed_steps": sum(1 for row in step_results if row.get("status") == "FAILED"),
        "artifact_ref_count": len(run.get("artifact_refs") or []),
        "guardrails": run.get("guardrails") or _guardrails(),
        "deterministic_fingerprint": run.get("deterministic_fingerprint"),
    }


def write_workflow(workflow: Mapping[str, Any], *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> CanonicalResearchWorkflowResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    workflow_record = dict(workflow)
    workflow_path = output_dir / f"{workflow_record['workflow_id']}.json"
    workflow_path.write_text(json.dumps(workflow_record, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary = summarize_workflow(workflow_record)
    summary_path = output_dir / f"{workflow_record['workflow_id']}_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CanonicalResearchWorkflowResult(workflow=workflow_record, workflow_path=workflow_path, summary=summary, summary_path=summary_path, output_dir=output_dir)


def load_workflow(workflow_id: str, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    return json.loads((output_dir / f"{workflow_id}.json").read_text(encoding="utf-8"))


def list_workflows(*, output_dir: Path = DEFAULT_OUTPUT_DIR) -> list[dict[str, Any]]:
    if not output_dir.exists():
        return []
    rows = []
    for path in sorted(output_dir.glob("*.json")):
        if path.name.startswith("rwf1_") or path.name.endswith("_summary.json") or path.name.startswith("latest_") or path.name.startswith("run_"):
            continue
        try:
            row = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if row.get("schema_version") == WORKFLOW_SCHEMA_VERSION:
            rows.append({"workflow_id": row.get("workflow_id"), "title": row.get("title"), "status": row.get("status"), "workflow_type": row.get("workflow_type")})
    return rows


def sample_morning_gold_review(*, now: datetime | str | None = None, output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, Any]:
    timestamp = _coerce_now(now)
    steps = [
        {"step_id": "run_expectancy_by_strategy", "order": 1, "title": "Run expectancy by strategy", "step_type": "RUN_SAVED_QUERY", "inputs": {"saved_query_id": "expectancy_by_strategy"}},
        {"step_id": "compare_expectancy_by_strategy", "order": 2, "title": "Compare expectancy result", "step_type": "COMPARE_QUERY_RESULT", "inputs": {"saved_query_id": "expectancy_by_strategy"}},
        {"step_id": "generate_analytics_insight", "order": 3, "title": "Generate deterministic insight", "step_type": "GENERATE_INSIGHT"},
        {"step_id": "generate_morning_brief", "order": 4, "title": "Generate Morning Brief", "step_type": "GENERATE_MORNING_BRIEF"},
        {"step_id": "archive_morning_brief", "order": 5, "title": "Archive Morning Brief", "step_type": "ARCHIVE_BRIEF"},
        {"step_id": "explain_brief_change", "order": 6, "title": "Explain Brief change", "step_type": "EXPLAIN_BRIEF_CHANGE"},
        {
            "step_id": "locate_or_create_investigation",
            "order": 7,
            "title": "Locate or create Investigation",
            "step_type": "CREATE_OR_LOCATE_INVESTIGATION",
            "inputs": {
                "investigation_id": "morning_gold_review_investigation",
                "title": "Morning Gold Review",
                "description": "Durable diagnostic workspace populated by the Morning Gold Review workflow.",
                "hypothesis": "Gold strategy research context should be reviewed through deterministic analytics, brief, and evidence artifacts.",
            },
        },
        {"step_id": "attach_query_execution", "order": 8, "title": "Attach query execution Evidence", "step_type": "ATTACH_QUERY_EXECUTION"},
        {"step_id": "attach_query_result", "order": 9, "title": "Attach query result Evidence", "step_type": "ATTACH_QUERY_RESULT"},
        {"step_id": "attach_analytics_diff", "order": 10, "title": "Attach analytics diff Evidence", "step_type": "ATTACH_ANALYTICS_DIFF"},
        {"step_id": "attach_analytics_insight", "order": 11, "title": "Attach analytics insight Evidence", "step_type": "ATTACH_ANALYTICS_INSIGHT"},
        {"step_id": "attach_morning_brief", "order": 12, "title": "Attach Morning Brief Evidence", "step_type": "ATTACH_MORNING_BRIEF"},
        {"step_id": "attach_brief_change", "order": 13, "title": "Attach Brief change Evidence", "step_type": "ATTACH_BRIEF_CHANGE"},
        {"step_id": "generate_claim_drafts", "order": 14, "title": "Generate Claim Drafts", "step_type": "GENERATE_CLAIM_DRAFTS"},
        {"step_id": "export_claim_draft_summary", "order": 15, "title": "Export Claim Draft summary", "step_type": "EXPORT_CLAIM_DRAFT_SUMMARY"},
        {"step_id": "review_draft_claims", "order": 16, "title": "Generate Claim Review queue", "step_type": "REVIEW_DRAFT_CLAIMS"},
        {"step_id": "export_claim_review_summary", "order": 17, "title": "Export Claim Review summary", "step_type": "EXPORT_CLAIM_REVIEW_SUMMARY"},
        {"step_id": "export_investigation_summary", "order": 18, "title": "Export Investigation summary", "step_type": "EXPORT_INVESTIGATION_SUMMARY"},
        {"step_id": "export_workflow_summary", "order": 19, "title": "Export workflow summary", "step_type": "EXPORT_SUMMARY"},
    ]
    return create_workflow(
        workflow_id="morning_gold_review",
        title="Morning Gold Review",
        description="Repeatable diagnostic review of Gold strategy analytics, brief state, and deterministic Investigation evidence.",
        workflow_type="MORNING_REVIEW",
        owner="operator",
        tags=("gold", "morning_review", "diagnostic"),
        status="ACTIVE",
        steps=steps,
        now=timestamp,
        output_dir=output_dir,
    ).workflow


def publish_rwf1_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    timestamp = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    workflow = sample_morning_gold_review(now=timestamp)
    run = run_workflow(workflow, output_dir=output_dir, now=timestamp)
    paths = {
        "contract": output_dir / CONTRACT_MD,
        "schema": output_dir / SCHEMA_JSON,
        "sample_workflow": output_dir / SAMPLE_WORKFLOW_JSON,
        "sample_run": output_dir / SAMPLE_RUN_JSON,
        "summary": output_dir / SUMMARY_MD,
        "runner_contract": output_dir / RUNNER_CONTRACT_MD,
    }
    paths["contract"].write_text(render_workflow_contract(), encoding="utf-8")
    paths["schema"].write_text(json.dumps(workflow_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample_workflow"].write_text(json.dumps(workflow, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample_run"].write_text(json.dumps(run, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["summary"].write_text(render_workflow_summary(workflow, run), encoding="utf-8")
    paths["runner_contract"].write_text(render_runner_contract(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def publish_rwf2_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    timestamp = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    sample_workspace = output_dir / "_rwf2_sample_workspace"
    workflow = sample_morning_gold_review(now=timestamp, output_dir=sample_workspace)
    run = run_workflow(
        workflow,
        output_dir=sample_workspace,
        saved_query_output_dir=sample_workspace / "saved_queries",
        morning_brief_output_dir=sample_workspace / "morning_brief",
        now=timestamp,
    )
    investigation_id = _latest_investigation_id_from_run(run)
    investigation = load_investigation(investigation_id, output_dir=sample_workspace / "investigations") if investigation_id else {}
    population = _aggregate_population_results(run)
    paths = {
        "population_contract": output_dir / RWF2_POPULATION_CONTRACT_MD,
        "population_schema": output_dir / RWF2_POPULATION_SCHEMA_JSON,
        "sample_population": output_dir / RWF2_SAMPLE_POPULATION_JSON,
        "sample_updated_investigation": output_dir / RWF2_SAMPLE_INVESTIGATION_JSON,
        "population_summary": output_dir / RWF2_POPULATION_SUMMARY_MD,
    }
    paths["population_contract"].write_text(render_population_contract(), encoding="utf-8")
    paths["population_schema"].write_text(json.dumps(population_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample_population"].write_text(json.dumps(population, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample_updated_investigation"].write_text(json.dumps(investigation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["population_summary"].write_text(render_population_summary(population, summarize_investigation(investigation) if investigation else {}), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def publish_rwf3_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    timestamp = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    sample_workspace = output_dir / "_rwf3_sample_workspace"
    workflow = sample_morning_gold_review(now=timestamp, output_dir=sample_workspace)
    run_workflow(
        workflow,
        output_dir=sample_workspace,
        saved_query_output_dir=sample_workspace / "saved_queries",
        morning_brief_output_dir=sample_workspace / "morning_brief",
        now=timestamp,
    )
    investigation_id = "morning_gold_review_investigation"
    drafts_payload = {
        "schema_version": "rwf3_sample_claim_drafts_v1",
        "investigation_id": investigation_id,
        "drafts": load_claim_drafts(investigation_id=investigation_id, draft_output_dir=sample_workspace / "claim_drafts"),
        "guardrails": _guardrails(),
    }
    summary = export_claim_draft_summary(investigation_id=investigation_id, draft_output_dir=sample_workspace / "claim_drafts")
    paths = {
        "draft_contract": output_dir / RWF3_DRAFT_CONTRACT_MD,
        "draft_schema": output_dir / RWF3_DRAFT_SCHEMA_JSON,
        "sample_drafts": output_dir / RWF3_SAMPLE_DRAFTS_JSON,
        "draft_summary": output_dir / RWF3_DRAFT_SUMMARY_MD,
        "manual_review_contract": output_dir / RWF3_MANUAL_REVIEW_CONTRACT_MD,
    }
    paths["draft_contract"].write_text(render_claim_draft_contract(), encoding="utf-8")
    paths["draft_schema"].write_text(json.dumps(claim_draft_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample_drafts"].write_text(json.dumps(drafts_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["draft_summary"].write_text(render_claim_draft_summary(summary), encoding="utf-8")
    paths["manual_review_contract"].write_text(render_manual_review_contract(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def publish_rwf4_artifacts(*, output_dir: Path = DEFAULT_OUTPUT_DIR, now: datetime | str | None = None) -> dict[str, str]:
    timestamp = _coerce_now(now)
    output_dir.mkdir(parents=True, exist_ok=True)
    sample_workspace = output_dir / "_rwf4_sample_workspace"
    workflow = sample_morning_gold_review(now=timestamp, output_dir=sample_workspace)
    run_workflow(
        workflow,
        output_dir=sample_workspace,
        saved_query_output_dir=sample_workspace / "saved_queries",
        morning_brief_output_dir=sample_workspace / "morning_brief",
        now=timestamp,
    )
    investigation_id = "morning_gold_review_investigation"
    drafts = load_claim_drafts(investigation_id=investigation_id, draft_output_dir=sample_workspace / "claim_drafts")
    if drafts:
        accept_claim_draft(
            str(drafts[0]["draft_id"]),
            draft_output_dir=sample_workspace / "claim_drafts",
            evidence_output_dir=sample_workspace / "evidence",
            claims_output_dir=sample_workspace / "claims",
            investigation_output_dir=sample_workspace / "investigations",
            now=timestamp,
        )
    reviews = generate_claim_review_queue(
        investigation_id=investigation_id,
        claims_output_dir=sample_workspace / "claims",
        review_output_dir=sample_workspace / "claim_reviews",
        investigation_output_dir=sample_workspace / "investigations",
        now=timestamp,
    )
    summary = export_claim_review_summary(investigation_id=investigation_id, review_output_dir=sample_workspace / "claim_reviews")
    paths = {
        "review_contract": output_dir / RWF4_REVIEW_CONTRACT_MD,
        "review_schema": output_dir / RWF4_REVIEW_SCHEMA_JSON,
        "sample_review_queue": output_dir / RWF4_SAMPLE_REVIEW_QUEUE_JSON,
        "review_summary": output_dir / RWF4_REVIEW_SUMMARY_MD,
        "manual_activation_contract": output_dir / RWF4_MANUAL_ACTIVATION_CONTRACT_MD,
    }
    paths["review_contract"].write_text(render_claim_review_contract(), encoding="utf-8")
    paths["review_schema"].write_text(json.dumps(claim_review_schema(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["sample_review_queue"].write_text(json.dumps({"schema_version": "rwf4_sample_review_queue_v1", "investigation_id": investigation_id, "reviews": reviews, "guardrails": _guardrails()}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    paths["review_summary"].write_text(render_claim_review_summary(summary), encoding="utf-8")
    paths["manual_activation_contract"].write_text(render_manual_activation_contract(), encoding="utf-8")
    return {key: str(path) for key, path in paths.items()}


def workflow_fingerprint(workflow: Mapping[str, Any]) -> str:
    return stable_hash(_strip_volatile(workflow))


def workflow_run_fingerprint(run: Mapping[str, Any]) -> str:
    return stable_hash(_strip_volatile(run))


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def render_workflow_contract() -> str:
    return """# RWF1 Canonical Research Workflow Contract

CanonicalResearchWorkflow records deterministic research recipes. It coordinates existing diagnostic/reporting components and has no runtime, broker, strategy, order, or gate authority.

Guardrails are always `diagnostic_only=true`, `production_recommendation=false`, and `trading_gate=false`.
"""


def render_runner_contract() -> str:
    return """# RWF1 Minimal Runner Contract

The RWF1 runner may execute only safe local/reporting steps that already exist. Unsupported steps are marked `SKIPPED` with a reason. The runner does not schedule itself, call brokers, restart runtime services, modify strategies, download Databento data, or create trading gates.
"""


def render_workflow_summary(workflow: Mapping[str, Any], run: Mapping[str, Any]) -> str:
    summary = summarize_workflow(workflow)
    run_summary = summarize_workflow_run(run)
    return "\n".join(
        [
            "# RWF1 Workflow Summary",
            "",
            f"- Workflow: `{summary['workflow_id']}`",
            f"- Title: {summary['title']}",
            f"- Type: `{summary['workflow_type']}`",
            f"- Steps: `{summary['step_count']}`",
            f"- Sample run status: `{run_summary['status']}`",
            f"- Complete steps: `{run_summary['complete_steps']}`",
            f"- Skipped steps: `{run_summary['skipped_steps']}`",
            f"- Failed steps: `{run_summary['failed_steps']}`",
            "",
            "Research orchestration only. No production recommendations or trading gates are produced.",
            "",
        ]
    )


def render_population_contract() -> str:
    return """# RWF2 Investigation Population Contract

RWF2 lets deterministic research workflows locate or create an Investigation, attach CanonicalEvidence for deterministic artifacts, update the Investigation timeline, and export an Investigation summary.

RWF2 does not create Claims or Conclusions. It does not use AI, notebooks, UI, broker integration, runtime integration, trading recommendations, production recommendations, or gates.

Each attachment is represented as CanonicalEvidence and carries CanonicalReference and CanonicalProvenanceEnvelope metadata.
"""


def render_population_summary(population: Mapping[str, Any], investigation_summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# RWF2 Investigation Population Summary",
            "",
            f"- Investigation: `{population.get('investigation_id')}`",
            f"- Created new Investigation: `{population.get('created_new_investigation')}`",
            f"- Evidence attached: `{population.get('evidence_attached_count')}`",
            f"- References created: `{population.get('references_created')}`",
            f"- Timeline events added: `{population.get('timeline_events_added')}`",
            f"- Investigation references: `{investigation_summary.get('reference_count')}`",
            f"- Investigation timeline events: `{investigation_summary.get('timeline_event_count')}`",
            f"- Fingerprint: `{population.get('deterministic_fingerprint')}`",
            "",
            "Research orchestration only. No Claims, Conclusions, recommendations, or gates are produced.",
            "",
        ]
    )


def workflow_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "CanonicalResearchWorkflow",
        "type": "object",
        "required": ["schema_version", "workflow_id", "title", "status", "workflow_type", "steps", "diagnostic_only", "production_recommendation", "trading_gate"],
        "properties": {
            "schema_version": {"const": WORKFLOW_SCHEMA_VERSION},
            "status": {"enum": sorted(VALID_WORKFLOW_STATUSES)},
            "workflow_type": {"enum": sorted(VALID_WORKFLOW_TYPES)},
            "steps": {"type": "array"},
        },
    }


def population_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "InvestigationPopulationResult",
        "type": "object",
        "required": [
            "workflow_run_id",
            "investigation_id",
            "created_new_investigation",
            "evidence_attached_count",
            "references_created",
            "timeline_events_added",
            "skipped_items",
            "deterministic_fingerprint",
            "provenance",
            "guardrails",
        ],
        "properties": {
            "workflow_run_id": {"type": "string"},
            "investigation_id": {"type": "string"},
            "created_new_investigation": {"type": "boolean"},
            "evidence_attached_count": {"type": "integer"},
            "references_created": {"type": "integer"},
            "timeline_events_added": {"type": "integer"},
            "skipped_items": {"type": "array"},
            "deterministic_fingerprint": {"type": "string"},
            "guardrails": {
                "type": "object",
                "properties": {
                    "diagnostic_only": {"const": True},
                    "production_recommendation": {"const": False},
                    "trading_gate": {"const": False},
                },
            },
        },
    }


def claim_draft_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ClaimDraft",
        "type": "object",
        "required": [
            "schema_version",
            "draft_id",
            "investigation_id",
            "source_evidence_ids",
            "proposed_claim_title",
            "proposed_statement",
            "draft_status",
            "confidence_suggestion",
            "validation_preview",
            "provenance",
            "deterministic_fingerprint",
            "guardrails",
        ],
        "properties": {
            "schema_version": {"const": CLAIM_DRAFT_SCHEMA_VERSION},
            "draft_status": {"enum": sorted(VALID_DRAFT_STATUSES)},
            "confidence_suggestion": {"enum": sorted(VALID_DRAFT_CONFIDENCE)},
            "validation_preview": {"enum": sorted(VALID_VALIDATION_PREVIEW)},
            "guardrails": {
                "type": "object",
                "properties": {
                    "diagnostic_only": {"const": True},
                    "production_recommendation": {"const": False},
                    "trading_gate": {"const": False},
                },
            },
        },
    }


def claim_review_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "ClaimReviewRecord",
        "type": "object",
        "required": [
            "schema_version",
            "review_id",
            "investigation_id",
            "claim_id",
            "reviewer",
            "review_status",
            "readiness",
            "validation_status",
            "evidence_summary",
            "provenance",
            "deterministic_fingerprint",
            "guardrails",
        ],
        "properties": {
            "schema_version": {"const": CLAIM_REVIEW_SCHEMA_VERSION},
            "review_status": {"enum": sorted(VALID_REVIEW_STATUSES)},
            "readiness": {"enum": sorted(VALID_REVIEW_READINESS)},
            "guardrails": {
                "type": "object",
                "properties": {
                    "diagnostic_only": {"const": True},
                    "production_recommendation": {"const": False},
                    "trading_gate": {"const": False},
                },
            },
        },
    }


def render_claim_draft_contract() -> str:
    return """# RWF3 Claim Draft Contract

ClaimDraft records are deterministic proposals derived from CanonicalEvidence. They are queue items for manual research review, not active Claims.

Accepting a draft may create a CanonicalClaim with `status=DRAFT` only. RWF3 never creates ACTIVE Claims, Conclusions, trading recommendations, production recommendations, or gates.
"""


def render_manual_review_contract() -> str:
    return """# RWF3 Manual Review Contract

Claim drafts require manual review before they can become CanonicalClaim records. `accept-draft` creates a DRAFT claim and preserves evidence references/provenance. `reject-draft` records rejection and creates no Claim.

No draft acceptance can activate a Claim or create a Conclusion.
"""


def render_claim_review_contract() -> str:
    return """# RWF4 Claim Review Contract

ClaimReviewRecord captures deterministic review readiness for CanonicalClaim records. It is research orchestration only.

DRAFT Claims may be queued for operator review. ACTIVE Claims can only be produced by explicit operator-approved activation, never by automatic workflow execution.
"""


def render_manual_activation_contract() -> str:
    return """# RWF4 Manual Activation Contract

`activate-claim` requires an explicit `--operator-approved` flag. Without the flag, activation fails safely.

Activation requires a DRAFT Claim with `READY_FOR_OPERATOR_REVIEW` readiness. Activation writes a ClaimReviewRecord and Investigation timeline event. It does not create Conclusions, alter Evidence, touch runtime, broker, strategy, Managed Exit, or create gates.
"""


def render_claim_review_summary(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# RWF4 Claim Review Summary",
            "",
            f"- Investigation: `{summary.get('investigation_id')}`",
            f"- Reviews: `{summary.get('review_count')}`",
            f"- Review status counts: `{summary.get('review_status_counts')}`",
            f"- Readiness counts: `{summary.get('readiness_counts')}`",
            "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
            "",
        ]
    )


def render_claim_draft_summary(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# RWF3 Claim Draft Queue Summary",
            "",
            f"- Investigation: `{summary.get('investigation_id')}`",
            f"- Drafts: `{summary.get('draft_count')}`",
            f"- Status counts: `{summary.get('status_counts')}`",
            f"- Validation preview counts: `{summary.get('validation_preview_counts')}`",
            "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
            "",
        ]
    )


def _aggregate_population_results(run: Mapping[str, Any]) -> dict[str, Any]:
    population_details = [dict((step.get("details") or {})) for step in run.get("step_results") or [] if (step.get("details") or {}).get("investigation_id")]
    investigation_id = _latest_investigation_id_from_run(run)
    payload = {
        "workflow_run_id": run.get("run_id"),
        "investigation_id": investigation_id,
        "created_new_investigation": any(row.get("created_new_investigation") for row in population_details),
        "evidence_attached_count": sum(int(row.get("evidence_attached_count") or 0) for row in population_details),
        "references_created": sum(int(row.get("references_created") or 0) for row in population_details),
        "timeline_events_added": sum(int(row.get("timeline_events_added") or 0) for row in population_details),
        "skipped_items": [item for row in population_details for item in (row.get("skipped_items") or [])],
        "provenance": {"source_component": "canonical_research_workflow", "source_run_id": run.get("run_id")},
        "guardrails": _guardrails(),
    }
    payload["deterministic_fingerprint"] = stable_hash(payload)
    return payload


def _latest_investigation_id_from_run(run: Mapping[str, Any]) -> str:
    for step in reversed(list(run.get("step_results") or [])):
        details = step.get("details") or {}
        if details.get("investigation_id"):
            return str(details["investigation_id"])
    return ""


def _step_result(
    *,
    step: Mapping[str, Any] | None = None,
    step_id: str | None = None,
    order: int | None = None,
    status: str,
    reason: str,
    details: Mapping[str, Any] | None = None,
    artifact_refs: Sequence[Mapping[str, Any]] | None = None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "step_id": step_id or str((step or {}).get("step_id")),
        "order": order if order is not None else int((step or {}).get("order") or 0),
        "step_type": (step or {}).get("step_type"),
        "status": _validate(status, VALID_STEP_STATUSES, "step_status"),
        "reason": reason,
        "details": dict(details or {}),
        "artifact_refs": [dict(ref) for ref in (artifact_refs or [])],
        "completed_at": now.isoformat(),
        "guardrails": _guardrails(),
    }


def _artifact_ref(target_kind: str, target_id: Any, path: Path) -> dict[str, Any]:
    return {"target_kind": target_kind, "target_id": str(target_id or path.stem), "target_path": str(path), "relationship": "references"}


def _guardrails() -> dict[str, bool]:
    return {"diagnostic_only": True, "production_recommendation": False, "trading_gate": False}


def _id(prefix: str, title: str, timestamp: datetime) -> str:
    return f"{prefix}_{stable_hash({'title': title, 'created_at': timestamp.isoformat()})[:16]}"


def _run_id(workflow_id: str, started_at: datetime, step_results: Sequence[Mapping[str, Any]]) -> str:
    return f"run_{stable_hash({'workflow_id': workflow_id, 'started_at': started_at.isoformat(), 'steps': step_results})[:20]}"


def _validate(value: str, valid: set[str], field: str) -> str:
    if value not in valid:
        raise ValueError(f"Unsupported {field}: {value}")
    return value


def _strip_volatile(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _strip_volatile(item) for key, item in sorted(value.items()) if key not in {"deterministic_fingerprint"}}
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
