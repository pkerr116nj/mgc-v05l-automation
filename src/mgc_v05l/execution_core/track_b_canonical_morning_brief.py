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


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "canonical_analytics_engine" / "morning_brief"

BRIEF_JSON = "cae9_morning_brief.json"
BRIEF_MD = "cae9_morning_brief.md"
CONTRACT_MD = "cae9_brief_contract.md"
COMPONENT_INVENTORY_MD = "cae9_component_inventory.md"
DATA_PROVENANCE_MD = "cae9_data_provenance.md"

SCHEMA_VERSION = "cae9_canonical_morning_brief_v1"

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
    return CanonicalMorningBrief(
        brief=brief,
        json_path=json_path,
        markdown_path=markdown_path,
        contract_path=contract_path,
        component_inventory_path=component_inventory_path,
        data_provenance_path=data_provenance_path,
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
