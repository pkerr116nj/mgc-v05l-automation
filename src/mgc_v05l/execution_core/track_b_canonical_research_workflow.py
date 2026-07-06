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
    RESULT_DIFF_JSON,
    compare_latest_execution_for_query,
    load_saved_queries,
    publish_insights,
    publish_result_diff,
    publish_saved_query_artifacts,
    run_saved_query,
)
from mgc_v05l.execution_core.track_b_canonical_morning_brief import DEFAULT_OUTPUT_DIR as DEFAULT_MORNING_BRIEF_OUTPUT_DIR
from mgc_v05l.execution_core.track_b_canonical_morning_brief import run_canonical_morning_brief


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

VALID_WORKFLOW_STATUSES = {"DRAFT", "ACTIVE", "PAUSED", "COMPLETE", "ARCHIVED"}
VALID_WORKFLOW_TYPES = {"MORNING_REVIEW", "INVESTIGATION_REFRESH", "CANDIDATE_REVIEW", "STRATEGY_REVIEW", "PLATFORM_REVIEW", "CUSTOM"}
VALID_STEP_TYPES = {
    "RUN_SAVED_QUERY",
    "COMPARE_QUERY_RESULT",
    "GENERATE_INSIGHT",
    "CREATE_INVESTIGATION",
    "ATTACH_EVIDENCE",
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


@dataclass(frozen=True)
class CanonicalResearchWorkflowResult:
    workflow: dict[str, Any]
    workflow_path: Path
    summary: dict[str, Any]
    summary_path: Path
    output_dir: Path


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
        context: dict[str, Any] = {}
        for step in sorted(workflow.get("steps") or [], key=lambda row: int(row.get("order") or 0)):
            result = _run_step(
                step,
                saved_query_output_dir=saved_query_output_dir,
                morning_brief_output_dir=morning_brief_output_dir,
                context=context,
                now=started_at,
            )
            step_results.append(result)
            artifact_refs.extend(result.get("artifact_refs") or [])
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
    saved_query_output_dir: Path,
    morning_brief_output_dir: Path,
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
            return _step_result(
                step=step,
                status="COMPLETE",
                reason="saved_query_executed",
                details={"saved_query_id": query_id, "execution_id": execution_id, "validation": run_result.validation.status},
                artifact_refs=[_artifact_ref("EXECUTION_RECORD", execution_id, saved_query_output_dir / "cae6_latest_execution_summary.json")],
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
            return _existing_artifact_step(step, path=path, target_kind="ARTIFACT", reason="brief_change_explained", now=now)
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
        {"step_id": "investigation_placeholder", "order": 7, "title": "Reference Investigation placeholder", "step_type": "CREATE_INVESTIGATION", "inputs": {"mode": "placeholder_reference_only"}},
        {"step_id": "export_workflow_summary", "order": 8, "title": "Export workflow summary", "step_type": "EXPORT_SUMMARY"},
    ]
    return create_workflow(
        workflow_id="morning_gold_review",
        title="Morning Gold Review",
        description="Repeatable diagnostic review of Gold strategy analytics, brief state, and investigation placeholder.",
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
