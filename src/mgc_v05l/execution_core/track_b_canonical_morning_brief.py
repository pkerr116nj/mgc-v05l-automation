"""Canonical Morning Brief aggregation for Track B analytics.

This module reads existing diagnostic artifacts and assembles a deterministic
briefing object. It does not compute new analytics, mutate operational state,
or create trading recommendations.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "morning_brief"

BRIEF_JSON = "cae9_morning_brief.json"
BRIEF_MD = "cae9_morning_brief.md"
CONTRACT_MD = "cae9_brief_contract.md"
COMPONENT_INVENTORY_MD = "cae9_component_inventory.md"
DATA_PROVENANCE_MD = "cae9_data_provenance.md"
ARCHIVE_JSONL = "mb2_morning_brief_archive.jsonl"
ARCHIVE_SUMMARY_JSON = "mb2_latest_brief_archive_summary.json"
DIFF_JSON = "mb2_latest_brief_diff.json"
DIFF_MD = "mb2_latest_brief_diff.md"
ARCHIVE_CONTRACT_MD = "mb2_brief_archive_contract.md"
HISTORY_REPORT_MD = "mb2_brief_history_report.md"
CHANGE_EXPLANATION_JSON = "mb3_latest_change_explanation.json"
CHANGE_EXPLANATION_MD = "mb3_latest_change_explanation.md"
CHANGE_EXPLANATION_CONTRACT_MD = "mb3_change_explanation_contract.md"
CHANGE_RULE_CATALOG_MD = "mb3_change_rule_catalog.md"
OPERATOR_RELEVANCE_MATRIX_MD = "mb3_operator_relevance_matrix.md"

SCHEMA_VERSION = "cae9_canonical_morning_brief_v1"
ARCHIVE_SCHEMA_VERSION = "mb2_canonical_morning_brief_archive_record_v1"
DIFF_SCHEMA_VERSION = "mb2_canonical_morning_brief_diff_v1"
CHANGE_EXPLANATION_SCHEMA_VERSION = "mb3_morning_brief_change_explanation_v1"

DEFAULT_ARTIFACT_PATHS = {
    "operational_certification": DEFAULT_OUTPUT_ROOT / "operations_maintenance" / "operational_certification" / "latest_operational_certification.json",
    "safe_state": DEFAULT_OUTPUT_ROOT / "safe_state" / "latest_runtime_safe_state_envelope.json",
    "guardian": DEFAULT_OUTPUT_ROOT / "broker_position_guardian" / "latest_broker_position_guardian.json",
    "managed_exit": DEFAULT_OUTPUT_ROOT / "managed_exit_service" / "latest_managed_exit_service_status.json",
    "research_discovery_summary": DEFAULT_OUTPUT_ROOT / "research" / "research_discovery_engine" / "latest_research_discovery_summary.json",
    "research_discovery_candidates": DEFAULT_OUTPUT_ROOT / "research" / "research_discovery_engine" / "research_discovery_candidates.jsonl",
    "trade_outcome_enrichment_summary": DEFAULT_OUTPUT_ROOT / "trade_outcome_enrichment" / "latest_trade_outcome_enrichment_summary.json",
    "canonical_market_context_summary": DEFAULT_OUTPUT_ROOT / "research" / "canonical_market_context" / "latest_canonical_market_context_summary.json",
    "cae_catalog_summary": DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "cae4_catalog_summary.json",
    "cae_saved_query_summary": DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "saved_queries" / "cae5_saved_query_summary.json",
    "cae_execution_summary": DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "saved_queries" / "cae6_latest_execution_summary.json",
    "cae_result_diff": DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "saved_queries" / "cae7_latest_result_diff.json",
    "cae_insights": DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "saved_queries" / "cae8_latest_insights.json",
}


@dataclass(frozen=True)
class CanonicalMorningBrief:
    brief: dict[str, Any]
    json_path: Path
    markdown_path: Path
    contract_path: Path
    component_inventory_path: Path
    data_provenance_path: Path
    archive_path: Path
    archive_summary_path: Path
    diff_path: Path
    diff_markdown_path: Path
    archive_contract_path: Path
    history_report_path: Path
    change_explanation_path: Path
    change_explanation_markdown_path: Path
    change_explanation_contract_path: Path
    change_rule_catalog_path: Path
    operator_relevance_matrix_path: Path


def run_canonical_morning_brief(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    artifact_paths: Mapping[str, Path] | None = None,
    now: datetime | str | None = None,
) -> CanonicalMorningBrief:
    generated_at = _coerce_now(now)
    paths = dict(DEFAULT_ARTIFACT_PATHS)
    if artifact_paths:
        paths.update({key: Path(value) for key, value in artifact_paths.items()})
    loaded = {key: _load_artifact(path) for key, path in sorted(paths.items())}
    brief = build_canonical_morning_brief(loaded, generated_at=generated_at)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / BRIEF_JSON
    json_path.write_text(json.dumps(brief, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = output_dir / BRIEF_MD
    markdown_path.write_text(render_morning_brief_markdown(brief), encoding="utf-8")
    contract_path = output_dir / CONTRACT_MD
    contract_path.write_text(render_brief_contract(), encoding="utf-8")
    component_inventory_path = output_dir / COMPONENT_INVENTORY_MD
    component_inventory_path.write_text(render_component_inventory(brief), encoding="utf-8")
    data_provenance_path = output_dir / DATA_PROVENANCE_MD
    data_provenance_path.write_text(render_data_provenance(brief), encoding="utf-8")
    archive_path = output_dir / ARCHIVE_JSONL
    prior_record = _latest_archive_record(archive_path)
    archive_record = create_morning_brief_archive_record(brief)
    diff = compare_morning_brief_archive_records(archive_record, prior_record)
    _append_jsonl(archive_path, archive_record)
    archive_summary_path = output_dir / ARCHIVE_SUMMARY_JSON
    archive_summary_path.write_text(json.dumps(_archive_summary(archive_path, archive_record, diff), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    diff_path = output_dir / DIFF_JSON
    diff_path.write_text(json.dumps(diff, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    diff_markdown_path = output_dir / DIFF_MD
    diff_markdown_path.write_text(render_brief_diff_markdown(diff), encoding="utf-8")
    archive_contract_path = output_dir / ARCHIVE_CONTRACT_MD
    archive_contract_path.write_text(render_brief_archive_contract(), encoding="utf-8")
    history_report_path = output_dir / HISTORY_REPORT_MD
    history_report_path.write_text(render_brief_history_report(archive_record, diff), encoding="utf-8")
    change_explanation = explain_morning_brief_changes(archive_record, prior_record)
    change_explanation_path = output_dir / CHANGE_EXPLANATION_JSON
    change_explanation_path.write_text(json.dumps(change_explanation, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    change_explanation_markdown_path = output_dir / CHANGE_EXPLANATION_MD
    change_explanation_markdown_path.write_text(render_change_explanation_markdown(change_explanation), encoding="utf-8")
    change_explanation_contract_path = output_dir / CHANGE_EXPLANATION_CONTRACT_MD
    change_explanation_contract_path.write_text(render_change_explanation_contract(), encoding="utf-8")
    change_rule_catalog_path = output_dir / CHANGE_RULE_CATALOG_MD
    change_rule_catalog_path.write_text(render_change_rule_catalog(), encoding="utf-8")
    operator_relevance_matrix_path = output_dir / OPERATOR_RELEVANCE_MATRIX_MD
    operator_relevance_matrix_path.write_text(render_operator_relevance_matrix(), encoding="utf-8")
    return CanonicalMorningBrief(
        brief=brief,
        json_path=json_path,
        markdown_path=markdown_path,
        contract_path=contract_path,
        component_inventory_path=component_inventory_path,
        data_provenance_path=data_provenance_path,
        archive_path=archive_path,
        archive_summary_path=archive_summary_path,
        diff_path=diff_path,
        diff_markdown_path=diff_markdown_path,
        archive_contract_path=archive_contract_path,
        history_report_path=history_report_path,
        change_explanation_path=change_explanation_path,
        change_explanation_markdown_path=change_explanation_markdown_path,
        change_explanation_contract_path=change_explanation_contract_path,
        change_rule_catalog_path=change_rule_catalog_path,
        operator_relevance_matrix_path=operator_relevance_matrix_path,
    )


def build_canonical_morning_brief(loaded_artifacts: Mapping[str, Mapping[str, Any]], *, generated_at: datetime) -> dict[str, Any]:
    platform = _build_platform(loaded_artifacts)
    analytics = _build_analytics(loaded_artifacts)
    research = _build_research(loaded_artifacts)
    market_context = _build_market_context(loaded_artifacts)
    sections = {
        "platform": _section("Platform", platform),
        "research": _section("Research", research),
        "analytics": _section("Analytics", analytics),
        "market_context": _section("Market Context", market_context),
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "platform": platform,
        "analytics": analytics,
        "research": research,
        "market_context": market_context,
        "sections": sections,
        "component_inventory": _component_inventory(loaded_artifacts),
        "data_provenance": _data_provenance(loaded_artifacts),
    }


def render_morning_brief_markdown(brief: Mapping[str, Any]) -> str:
    lines = ["# Canonical Morning Brief", "", f"- Generated at: `{brief.get('generated_at')}`", ""]
    for key in ("platform", "research", "analytics", "market_context"):
        section = (brief.get("sections") or {}).get(key) or {}
        lines.extend([
            f"## {section.get('headline')}",
            "",
            f"- Severity: `{section.get('severity')}`",
            f"- Summary: {section.get('summary')}",
        ])
        refs = section.get("supporting_evidence_refs") or []
        if refs:
            lines.append(f"- Evidence: `{', '.join(refs)}`")
        lines.append("")
    lines.append("Diagnostic/read-only aggregation only. No production recommendations or trading gates are produced.")
    lines.append("")
    return "\n".join(lines)


def render_brief_contract() -> str:
    return "\n".join([
        "# CAE9 Morning Brief Contract",
        "",
        f"- Schema version: `{SCHEMA_VERSION}`",
        "- Aggregates existing platform, research, analytics, and market-context artifacts.",
        "- Performs deterministic section headline generation only.",
        "- Does not compute new analytics.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_component_inventory(brief: Mapping[str, Any]) -> str:
    lines = ["# CAE9 Component Inventory", "", "| Component | Path | Present | SHA256 |", "|---|---|---:|---|"]
    for item in brief.get("component_inventory") or []:
        lines.append(f"| `{item.get('component')}` | `{item.get('path')}` | `{item.get('present')}` | `{item.get('sha256')}` |")
    lines.append("")
    return "\n".join(lines)


def render_data_provenance(brief: Mapping[str, Any]) -> str:
    lines = ["# CAE9 Data Provenance", "", "| Component | Loaded | Type | Source |", "|---|---:|---|---|"]
    for item in brief.get("data_provenance") or []:
        lines.append(f"| `{item.get('component')}` | `{item.get('loaded')}` | `{item.get('artifact_type')}` | `{item.get('path')}` |")
    lines.append("")
    return "\n".join(lines)


def create_morning_brief_archive_record(brief: Mapping[str, Any]) -> dict[str, Any]:
    platform = brief.get("platform") or {}
    analytics = brief.get("analytics") or {}
    research = brief.get("research") or {}
    market_context = brief.get("market_context") or {}
    generated_at = str(brief.get("generated_at") or "")
    fingerprint = morning_brief_fingerprint(brief)
    return {
        "schema_version": ARCHIVE_SCHEMA_VERSION,
        "brief_id": f"morning_brief_{fingerprint[:16]}",
        "generated_at": generated_at,
        "session_label": _session_label(generated_at),
        "platform_classification": platform.get("certification_classification"),
        "runtime_status": platform.get("runtime_status"),
        "managed_exit_status": platform.get("managed_exit_status"),
        "safe_state_classification": platform.get("safe_state_classification"),
        "guardian_classification": platform.get("guardian_classification"),
        "insight_count": analytics.get("latest_insight_count") or 0,
        "research_manual_review_count": research.get("manual_review_count") or 0,
        "market_context_status": market_context.get("current_market_context_status"),
        "guardrails": {
            "diagnostic_only": brief.get("diagnostic_only") is True,
            "production_recommendation": brief.get("production_recommendation") is True,
            "trading_gate": brief.get("trading_gate") is True,
        },
        "source_artifact_refs": sorted(_all_source_refs(brief)),
        "brief_fingerprint": fingerprint,
    }


def morning_brief_fingerprint(brief: Mapping[str, Any]) -> str:
    payload = _strip_generated_at(brief)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def compare_morning_brief_archive_records(
    current: Mapping[str, Any],
    prior: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if prior is None:
        return {
            "schema_version": DIFF_SCHEMA_VERSION,
            "classification": "NO_PRIOR_BRIEF",
            "current_brief_id": current.get("brief_id"),
            "prior_brief_id": None,
            "fingerprint_changed": None,
            "changes": [],
            "deltas": {},
            "guardrails": current.get("guardrails") or {},
        }
    changes = []
    for key in (
        "platform_classification",
        "runtime_status",
        "managed_exit_status",
        "safe_state_classification",
        "guardian_classification",
        "market_context_status",
    ):
        if current.get(key) != prior.get(key):
            changes.append({
                "field": key,
                "prior": prior.get(key),
                "current": current.get(key),
                "change_type": "VALUE_CHANGED",
            })
    deltas = {
        "insight_count_delta": int(current.get("insight_count") or 0) - int(prior.get("insight_count") or 0),
        "research_manual_review_count_delta": int(current.get("research_manual_review_count") or 0) - int(prior.get("research_manual_review_count") or 0),
    }
    for field, delta in deltas.items():
        if delta:
            changes.append({
                "field": field.replace("_delta", ""),
                "prior": prior.get(field.replace("_delta", "")),
                "current": current.get(field.replace("_delta", "")),
                "delta": delta,
                "change_type": "COUNT_CHANGED",
            })
    fingerprint_changed = current.get("brief_fingerprint") != prior.get("brief_fingerprint")
    classification = "CHANGED" if changes or fingerprint_changed else "UNCHANGED"
    return {
        "schema_version": DIFF_SCHEMA_VERSION,
        "classification": classification,
        "current_brief_id": current.get("brief_id"),
        "prior_brief_id": prior.get("brief_id"),
        "current_generated_at": current.get("generated_at"),
        "prior_generated_at": prior.get("generated_at"),
        "fingerprint_changed": fingerprint_changed,
        "changes": changes,
        "deltas": deltas,
        "guardrails": current.get("guardrails") or {},
    }


def render_brief_diff_markdown(diff: Mapping[str, Any]) -> str:
    lines = [
        "# MB2 Latest Morning Brief Diff",
        "",
        f"- Classification: `{diff.get('classification')}`",
        f"- Current brief: `{diff.get('current_brief_id')}`",
        f"- Prior brief: `{diff.get('prior_brief_id')}`",
        f"- Fingerprint changed: `{diff.get('fingerprint_changed')}`",
        "",
    ]
    changes = diff.get("changes") or []
    if not changes:
        lines.append("No field-level changes were detected.")
    else:
        lines.extend(["| Field | Prior | Current | Delta |", "|---|---|---|---:|"])
        for change in changes:
            lines.append(f"| `{change.get('field')}` | `{change.get('prior')}` | `{change.get('current')}` | `{change.get('delta', '')}` |")
    lines.extend(["", "Diagnostic archive comparison only. No production recommendations or trading gates are produced.", ""])
    return "\n".join(lines)


def render_brief_archive_contract() -> str:
    return "\n".join([
        "# MB2 Morning Brief Archive Contract",
        "",
        f"- Archive schema version: `{ARCHIVE_SCHEMA_VERSION}`",
        f"- Diff schema version: `{DIFF_SCHEMA_VERSION}`",
        "- Archive storage: append-only JSONL under the Morning Brief output directory.",
        "- Fingerprint excludes `generated_at` recursively so identical content across runs remains stable.",
        "- Comparison is latest brief versus previous archive record.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_brief_history_report(record: Mapping[str, Any], diff: Mapping[str, Any]) -> str:
    return "\n".join([
        "# MB2 Morning Brief History Report",
        "",
        f"- Latest brief id: `{record.get('brief_id')}`",
        f"- Latest generated at: `{record.get('generated_at')}`",
        f"- Session label: `{record.get('session_label')}`",
        f"- Diff classification: `{diff.get('classification')}`",
        f"- Insight count: `{record.get('insight_count')}`",
        f"- Research manual-review count: `{record.get('research_manual_review_count')}`",
        f"- Market context status: `{record.get('market_context_status')}`",
        "",
    ])


def explain_morning_brief_changes(
    current: Mapping[str, Any],
    prior: Mapping[str, Any] | None,
) -> dict[str, Any]:
    guardrails = current.get("guardrails") or {}
    if prior is None:
        return {
            "schema_version": CHANGE_EXPLANATION_SCHEMA_VERSION,
            "classification": "NO_PRIOR_BRIEF",
            "current_brief_id": current.get("brief_id"),
            "prior_brief_id": None,
            "change_count": 0,
            "changes": [],
            "summary": "No prior Morning Brief archive record exists yet, so no change explanation is available.",
            "guardrails": guardrails,
        }
    changes = _explain_field_changes(current, prior)
    guardrail_change = _explain_guardrail_change(current, prior)
    if guardrail_change:
        changes.append(guardrail_change)
    classification = "CHANGED" if changes else "NO_MEANINGFUL_CHANGE"
    summary = (
        f"{len(changes)} meaningful Morning Brief change(s) detected."
        if changes
        else "No meaningful Morning Brief changes were detected beyond archive timing/fingerprint churn."
    )
    return {
        "schema_version": CHANGE_EXPLANATION_SCHEMA_VERSION,
        "classification": classification,
        "current_brief_id": current.get("brief_id"),
        "prior_brief_id": prior.get("brief_id"),
        "current_generated_at": current.get("generated_at"),
        "prior_generated_at": prior.get("generated_at"),
        "change_count": len(changes),
        "changes": changes,
        "summary": summary,
        "guardrails": guardrails,
    }


def render_change_explanation_markdown(explanation: Mapping[str, Any]) -> str:
    lines = [
        "# MB3 Morning Brief Change Explanation",
        "",
        f"- Classification: `{explanation.get('classification')}`",
        f"- Change count: `{explanation.get('change_count')}`",
        f"- Current brief: `{explanation.get('current_brief_id')}`",
        f"- Prior brief: `{explanation.get('prior_brief_id')}`",
        f"- Summary: {explanation.get('summary')}",
        "",
    ]
    changes = explanation.get("changes") or []
    if not changes:
        lines.append("No deterministic domain-level changes were detected.")
    else:
        lines.extend(["| Domain | Severity | Headline | Before | After |", "|---|---|---|---|---|"])
        for change in changes:
            lines.append(
                f"| `{change.get('domain')}` | `{change.get('severity')}` | {change.get('headline')} | `{change.get('before')}` | `{change.get('after')}` |"
            )
    lines.extend(["", "Diagnostic explanation only. No production recommendations or trading gates are produced.", ""])
    return "\n".join(lines)


def render_change_explanation_contract() -> str:
    return "\n".join([
        "# MB3 Change Explanation Contract",
        "",
        f"- Schema version: `{CHANGE_EXPLANATION_SCHEMA_VERSION}`",
        "- Compares the latest Morning Brief archive record to the prior archive record.",
        "- Emits deterministic domain-level explanations only.",
        "- If no domain-level field changes occur, the result is `NO_MEANINGFUL_CHANGE` even if archive timestamps or fingerprints differ.",
        "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
        "",
    ])


def render_change_rule_catalog() -> str:
    return "\n".join([
        "# MB3 Change Rule Catalog",
        "",
        "| Rule | Domain | Severity |",
        "|---|---|---|",
        "| Critical operational downgrade | PLATFORM / SAFE_STATE / GUARDIAN | CRITICAL |",
        "| Operational warning changed | RUNTIME / MANAGED_EXIT | WARNING |",
        "| Certification improved | PLATFORM | INFO |",
        "| Certification deteriorated | PLATFORM | CRITICAL/WARNING |",
        "| Insight count changed | ANALYTICS | INFO |",
        "| Manual-review candidate count changed | RESEARCH_DISCOVERY | INFO |",
        "| Market context status changed | MARKET_CONTEXT | NOTICE |",
        "| Guardrail status changed | GUARDRAILS | HIGH |",
        "| No meaningful change | ALL | INFO |",
        "",
    ])


def render_operator_relevance_matrix() -> str:
    return "\n".join([
        "# MB3 Operator Relevance Matrix",
        "",
        "| Domain | Operator relevance |",
        "|---|---|",
        "| PLATFORM | Certification state changed; review operational readiness context. |",
        "| RUNTIME | Runtime warning/pass/fail label changed; inspect status artifacts if unexpected. |",
        "| MANAGED_EXIT | Managed Exit service status changed; inspect service artifact if unexpected. |",
        "| SAFE_STATE | Safe-State classification changed; this may indicate safety posture drift. |",
        "| GUARDIAN | Guardian classification changed; inspect guardian evidence before further operations. |",
        "| ANALYTICS | Deterministic insight count changed; review CAE8 insights. |",
        "| RESEARCH_DISCOVERY | Manual-review research queue changed. |",
        "| MARKET_CONTEXT | Market-context readiness changed. |",
        "| GUARDRAILS | Diagnostic guardrails changed; treat as high severity. |",
        "",
    ])


def _build_platform(loaded: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    cert = _payload(loaded, "operational_certification")
    safe_state = _payload(loaded, "safe_state")
    guardian = _payload(loaded, "guardian")
    managed_exit = _payload(loaded, "managed_exit")
    cert_summary = cert.get("summary") or {}
    return {
        "certification_classification": cert.get("classification") or cert_summary.get("classification"),
        "certification_warnings": cert_summary.get("warnings") or [],
        "certification_critical_failures": cert_summary.get("critical_failures") or [],
        "runtime_status": _runtime_status(cert),
        "managed_exit_status": managed_exit.get("status") or managed_exit.get("classification") or managed_exit.get("service_status"),
        "managed_exit_generated_at": managed_exit.get("generated_at"),
        "safe_state_classification": safe_state.get("classification") or safe_state.get("safe_state_classification"),
        "guardian_classification": guardian.get("classification") or guardian.get("guardian_status") or guardian.get("status"),
        "source_refs": _refs(loaded, ("operational_certification", "safe_state", "guardian", "managed_exit")),
    }


def _build_analytics(loaded: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    saved = _payload(loaded, "cae_saved_query_summary")
    catalog = _payload(loaded, "cae_catalog_summary")
    execution = _payload(loaded, "cae_execution_summary")
    diff = _payload(loaded, "cae_result_diff")
    insights = _payload(loaded, "cae_insights")
    insight_rows = insights if isinstance(insights, list) else []
    return {
        "latest_insights": insight_rows,
        "latest_insight_count": len(insight_rows),
        "latest_query_diff": diff,
        "latest_execution_summary": execution,
        "saved_query_count": saved.get("saved_query_count"),
        "preset_count": saved.get("preset_count"),
        "catalog_version": catalog.get("schema_version"),
        "catalog_counts": {
            "dimensions": len(catalog.get("dimensions") or {}),
            "metrics": len(catalog.get("metrics") or {}),
            "filters": len(catalog.get("filters") or {}),
        },
        "source_refs": _refs(loaded, ("cae_insights", "cae_result_diff", "cae_execution_summary", "cae_saved_query_summary", "cae_catalog_summary")),
    }


def _build_research(loaded: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    summary = _payload(loaded, "research_discovery_summary")
    candidates = _payload(loaded, "research_discovery_candidates")
    candidate_counts = summary.get("candidate_counts") or {}
    by_recommendation = candidate_counts.get("by_recommendation_level") or {}
    by_sample = candidate_counts.get("by_sample_class") or {}
    research_grade = [row for row in candidates if row.get("confidence_class") == "RESEARCH_GRADE"]
    return {
        "candidate_count": (summary.get("input_counts") or {}).get("candidates"),
        "manual_review_count": by_recommendation.get("MANUAL_REVIEW", 0),
        "classification_counts": by_sample,
        "discovery_family_counts": candidate_counts.get("by_family") or {},
        "top_research_grade_candidates": research_grade[:5],
        "top_candidates": summary.get("top_candidates") or [],
        "source_refs": _refs(loaded, ("research_discovery_summary", "research_discovery_candidates")),
    }


def _build_market_context(loaded: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    cmc = _payload(loaded, "canonical_market_context_summary")
    enrichment = _payload(loaded, "trade_outcome_enrichment_summary")
    context_validity = enrichment.get("context_validity") or {}
    vix = cmc.get("vix") or {}
    return {
        "market_context_provider_count": cmc.get("provider_count"),
        "current_market_context_status": vix.get("join_readiness") or cmc.get("join_readiness"),
        "vix_coverage_summary": {
            "available": vix.get("available"),
            "row_count": vix.get("row_count") or cmc.get("row_count"),
            "coverage_window": vix.get("coverage_window"),
            "freshness": vix.get("freshness"),
        },
        "gre_validity_summary": context_validity.get("gre") or {},
        "context_validity_summary": context_validity,
        "source_refs": _refs(loaded, ("canonical_market_context_summary", "trade_outcome_enrichment_summary")),
    }


def _section(name: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    if name == "Platform":
        classification = str(payload.get("certification_classification") or "UNKNOWN")
        warnings = payload.get("certification_warnings") or []
        severity = "WARNING" if "WARNING" in classification or warnings else "INFO"
        return {
            "headline": f"Platform: {classification}",
            "summary": f"Runtime status {payload.get('runtime_status')}; Managed Exit {payload.get('managed_exit_status')}; Safe-State {payload.get('safe_state_classification')}; Guardian {payload.get('guardian_classification')}.",
            "severity": severity,
            "supporting_evidence_refs": payload.get("source_refs") or [],
        }
    if name == "Research":
        manual = payload.get("manual_review_count") or 0
        research_grade = len(payload.get("top_research_grade_candidates") or [])
        return {
            "headline": f"Research: {manual} manual-review candidates",
            "summary": f"{payload.get('candidate_count')} discovery candidates with {research_grade} research-grade candidates surfaced in the current review slice.",
            "severity": "INFO" if manual else "NOTICE",
            "supporting_evidence_refs": payload.get("source_refs") or [],
        }
    if name == "Analytics":
        insight_count = payload.get("latest_insight_count") or 0
        diff = payload.get("latest_query_diff") or {}
        return {
            "headline": f"Analytics: {insight_count} deterministic insight(s)",
            "summary": f"Latest query diff for {diff.get('query_id')} is {diff.get('status')} with classification {diff.get('comparison_classification')}.",
            "severity": "INFO",
            "supporting_evidence_refs": payload.get("source_refs") or [],
        }
    status = payload.get("current_market_context_status") or "UNKNOWN"
    gre_counts = ((payload.get("gre_validity_summary") or {}).get("classification_counts") or {})
    return {
        "headline": f"Market Context: {status}",
        "summary": f"VIX available={((payload.get('vix_coverage_summary') or {}).get('available'))}; GRE validity counts={gre_counts}.",
        "severity": "INFO" if status in {"READY", "FRESH"} else "NOTICE",
        "supporting_evidence_refs": payload.get("source_refs") or [],
    }


def _component_inventory(loaded: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for key, item in sorted(loaded.items()):
        rows.append({
            "component": key,
            "path": item.get("path"),
            "present": item.get("present"),
            "sha256": item.get("sha256"),
        })
    return rows


def _data_provenance(loaded: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows = []
    for key, item in sorted(loaded.items()):
        rows.append({
            "component": key,
            "path": item.get("path"),
            "loaded": item.get("loaded"),
            "artifact_type": item.get("artifact_type"),
            "error": item.get("error"),
        })
    return rows


def _load_artifact(path: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "path": str(path),
        "present": path.exists(),
        "loaded": False,
        "artifact_type": None,
        "sha256": _file_sha256(path) if path.exists() else None,
        "payload": None,
    }
    if not path.exists():
        return result
    try:
        if path.suffix == ".jsonl":
            rows = _read_jsonl(path)
            result.update({"loaded": True, "artifact_type": "jsonl", "payload": rows})
        else:
            result.update({"loaded": True, "artifact_type": "json", "payload": json.loads(path.read_text(encoding="utf-8"))})
    except Exception as exc:  # pragma: no cover - defensive provenance capture
        result["error"] = str(exc)
    return result


def _payload(loaded: Mapping[str, Mapping[str, Any]], key: str) -> Any:
    payload = (loaded.get(key) or {}).get("payload")
    if payload is None:
        return [] if key.endswith("candidates") or key == "cae_insights" else {}
    return payload


def _refs(loaded: Mapping[str, Mapping[str, Any]], keys: Sequence[str]) -> list[str]:
    return [str((loaded.get(key) or {}).get("path")) for key in keys if (loaded.get(key) or {}).get("present")]


def _runtime_status(certification: Mapping[str, Any]) -> str:
    runtime = ((certification.get("domains") or {}).get("runtime") or {})
    return str(runtime.get("classification") or runtime.get("status") or "SEE_CERTIFICATION")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _latest_archive_record(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    latest: dict[str, Any] | None = None
    for row in _read_jsonl(path):
        latest = row
    return latest


def _explain_field_changes(current: Mapping[str, Any], prior: Mapping[str, Any]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    field_specs = {
        "platform_classification": ("PLATFORM", "Platform certification changed"),
        "runtime_status": ("RUNTIME", "Runtime status changed"),
        "managed_exit_status": ("MANAGED_EXIT", "Managed Exit status changed"),
        "safe_state_classification": ("SAFE_STATE", "Safe-State classification changed"),
        "guardian_classification": ("GUARDIAN", "Guardian classification changed"),
        "market_context_status": ("MARKET_CONTEXT", "Market context status changed"),
        "insight_count": ("ANALYTICS", "Analytics insight count changed"),
        "research_manual_review_count": ("RESEARCH_DISCOVERY", "Research manual-review count changed"),
    }
    for field, (domain, headline) in field_specs.items():
        before = prior.get(field)
        after = current.get(field)
        if before == after:
            continue
        changes.append({
            "change_id": f"mb3_{domain.lower()}_{field}",
            "domain": domain,
            "severity": _change_severity(domain, before, after),
            "headline": headline,
            "before": before,
            "after": after,
            "evidence_refs": current.get("source_artifact_refs") or [],
            "operator_relevance": _operator_relevance(domain, before, after),
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        })
    return changes


def _explain_guardrail_change(current: Mapping[str, Any], prior: Mapping[str, Any]) -> dict[str, Any] | None:
    before = prior.get("guardrails") or {}
    after = current.get("guardrails") or {}
    if before == after:
        return None
    return {
        "change_id": "mb3_guardrails_status",
        "domain": "GUARDRAILS",
        "severity": "HIGH",
        "headline": "Morning Brief diagnostic guardrails changed",
        "before": before,
        "after": after,
        "evidence_refs": current.get("source_artifact_refs") or [],
        "operator_relevance": "Diagnostic guardrail changes affect whether downstream consumers can safely treat the brief as non-authoritative.",
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def _change_severity(domain: str, before: Any, after: Any) -> str:
    after_text = str(after or "").upper()
    before_text = str(before or "").upper()
    if domain in {"SAFE_STATE", "GUARDIAN"} and any(token in after_text for token in ("FAIL", "BLOCK", "HARD_STOP", "NOT_READY")):
        return "CRITICAL"
    if domain == "PLATFORM":
        if "NOT_CERTIFIED" in after_text or "FAIL" in after_text:
            return "CRITICAL"
        if "WARNING" in after_text:
            return "WARNING"
        if "NOT_CERTIFIED" in before_text and "CERTIFIED" in after_text:
            return "INFO"
    if domain in {"RUNTIME", "MANAGED_EXIT"}:
        return "WARNING" if any(token in after_text for token in ("WARN", "FAIL", "STALE", "ERROR")) else "INFO"
    if domain == "MARKET_CONTEXT":
        return "NOTICE"
    return "INFO"


def _operator_relevance(domain: str, before: Any, after: Any) -> str:
    if domain == "PLATFORM":
        return "Certification state changed; review the operational certification artifact before relying on the brief."
    if domain == "RUNTIME":
        return "Runtime status changed; this is operational context only and does not imply a restart."
    if domain == "MANAGED_EXIT":
        return "Managed Exit status changed; this is service context only and does not imply intervention."
    if domain == "SAFE_STATE":
        return "Safe-State classification changed; inspect the Safe-State artifact if the shift is unexpected."
    if domain == "GUARDIAN":
        return "Guardian classification changed; inspect guardian evidence if the shift is unexpected."
    if domain == "ANALYTICS":
        return "Deterministic analytics insight count changed; review CAE8 insight details."
    if domain == "RESEARCH_DISCOVERY":
        return "Manual-review research queue changed; review research discovery candidates."
    if domain == "MARKET_CONTEXT":
        return "Market-context readiness changed; review CMC and enrichment context validity artifacts."
    return f"{domain} changed from {before} to {after}."


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")


def _archive_summary(path: Path, record: Mapping[str, Any], diff: Mapping[str, Any]) -> dict[str, Any]:
    archive_count = len(_read_jsonl(path)) if path.exists() else 0
    return {
        "schema_version": "mb2_morning_brief_archive_summary_v1",
        "archive_path": str(path),
        "archive_record_count": archive_count,
        "latest_brief_id": record.get("brief_id"),
        "latest_generated_at": record.get("generated_at"),
        "latest_fingerprint": record.get("brief_fingerprint"),
        "diff_classification": diff.get("classification"),
        "fingerprint_changed": diff.get("fingerprint_changed"),
        "guardrails": record.get("guardrails") or {},
    }


def _strip_generated_at(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _strip_generated_at(item) for key, item in sorted(value.items()) if key != "generated_at"}
    if isinstance(value, list):
        return [_strip_generated_at(item) for item in value]
    return value


def _all_source_refs(brief: Mapping[str, Any]) -> list[str]:
    refs: list[str] = []
    for key in ("platform", "analytics", "research", "market_context"):
        refs.extend((brief.get(key) or {}).get("source_refs") or [])
    return refs


def _session_label(generated_at: str) -> str | None:
    if not generated_at:
        return None
    try:
        dt = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return None
    local = dt.astimezone(ZoneInfo("America/New_York"))
    hour = local.hour
    if 5 <= hour < 12:
        return "MORNING"
    if 12 <= hour < 17:
        return "AFTERNOON"
    if 17 <= hour < 21:
        return "EVENING"
    return "OVERNIGHT"
