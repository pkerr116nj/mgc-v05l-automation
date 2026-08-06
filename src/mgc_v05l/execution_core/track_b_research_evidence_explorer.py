"""Research Evidence Explorer v1.

This module builds a deterministic, read-only product slice over Canonical
Research Record artifacts. It has no broker, runtime, strategy, Managed Exit,
Guardian, Safe-State, readiness, reconciliation, or trading-gate authority.
"""

from __future__ import annotations

import hashlib
import html
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_research_record import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CRR_OUTPUT_DIR,
    CRR_JSONL,
    VALIDATION_JSON,
)
from mgc_v05l.execution_core.track_b_research_eligibility import (
    DEFAULT_OUTPUT_DIR as DEFAULT_ELIGIBILITY_OUTPUT_DIR,
    EXCLUDED_SOURCE_INTEGRITY,
    RECORDS_JSONL as ELIGIBILITY_RECORDS_JSONL,
    SUMMARY_JSON as ELIGIBILITY_SUMMARY_JSON,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CRR_PATH = DEFAULT_CRR_OUTPUT_DIR / CRR_JSONL
DEFAULT_CRR_VALIDATION_PATH = DEFAULT_CRR_OUTPUT_DIR / VALIDATION_JSON
DEFAULT_ELIGIBILITY_RECORDS_PATH = DEFAULT_ELIGIBILITY_OUTPUT_DIR / ELIGIBILITY_RECORDS_JSONL
DEFAULT_ELIGIBILITY_SUMMARY_PATH = DEFAULT_ELIGIBILITY_OUTPUT_DIR / ELIGIBILITY_SUMMARY_JSON
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "research_evidence_explorer"
DEFAULT_INVESTIGATION_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "investigations"
DEFAULT_INVESTIGATION_DOC_DIR = Path("docs") / "research" / "investigations"

ANALYSIS_JSON = "research_evidence_explorer_v1.json"
ANALYSIS_MD = "research_evidence_explorer_v1.md"
POPULATION_JSON = "research_evidence_explorer_population.json"
SCHEMA_JSON = "research_evidence_explorer_schema.json"
VALIDATION_REPORT_JSON = "research_evidence_explorer_validation_report.json"
VALIDATION_REPORT_MD = "research_evidence_explorer_validation_report.md"
PRESENTATION_HTML = "research_evidence_explorer_v1.html"
EXTREME_FORENSIC_JSON = "extreme_trade_pnl_reconciliation.json"
EXTREME_LINEAGE_JSON = "extreme_trade_execution_lineage.json"
EXTREME_CLASSIFICATION_JSON = "extreme_trade_classification.json"
EXTREME_POPULATION_IMPACT_JSON = "extreme_trade_population_impact.json"
EXTREME_FORENSIC_HTML = "extreme_trade_forensic_review.html"
EXTREME_FORENSIC_VALIDATION_JSON = "forensic_validation_report.json"
EXTREME_FORENSIC_VALIDATION_MD = "forensic_validation_report.md"
ANOMALY_SOURCE_TRACE_JSON = "anomaly_source_trace.json"
ANOMALY_ROOT_CAUSE_JSON = "anomaly_root_cause.json"
ANOMALY_REPAIR_PLAN_JSON = "anomaly_repair_plan.json"
ANOMALY_BEFORE_AFTER_JSON = "anomaly_before_after.json"
ANOMALY_ROOT_CAUSE_HTML = "anomaly_root_cause_review.html"
INV_004_PERFORMANCE_SUMMARY_JSON = "performance_summary.json"
INV_004_CONCENTRATION_JSON = "concentration_by_dimension.json"
INV_004_TAIL_SENSITIVITY_JSON = "tail_sensitivity.json"
INV_004_ROLLING_WINDOWS_JSON = "rolling_windows.json"
INV_004_CONTROLLED_COMPARISONS_JSON = "controlled_comparisons.json"
INV_004_TOP_BOTTOM_TRADES_JSON = "top_bottom_trades.json"
INV_004_CONTRADICTORY_EVIDENCE_JSON = "contradictory_evidence.json"
INV_004_HTML = "nq_performance_attribution.html"

SCHEMA_VERSION = "research_evidence_explorer_v1"
VALIDATION_SCHEMA_VERSION = "research_evidence_explorer_validation_v1"

GUARDRAILS = {
    "diagnostic_only": True,
    "production_recommendation": False,
    "trading_gate": False,
}

REQUIRED_LAYERS = ("ctol", "ctoe", "ra7", "ra3")
OPTIONAL_FIELDS = (
    "realized_pnl_proxy",
    "hold_seconds",
    "mfe_points",
    "mae_points",
    "giveback_points",
    "session",
    "regime",
    "exit_reason",
)
MIN_CONTROLLED_INSTRUMENT_SAMPLE = 30
MIN_COMPARISON_CELL_SAMPLE = 20
INVESTIGATION_SCHEMA_VERSION = "research_investigation_record_v1"
INVESTIGATION_VALIDATION_SCHEMA_VERSION = "research_investigation_validation_v1"

EVIDENCE_BACKED_MILESTONES = (
    {
        "milestone_id": "MILESTONE_RA_PATH_LAYER",
        "label": "Trade path research layer introduced",
        "boundary_at": "2026-07-07T07:11:09-04:00",
        "source": "git commit 06af163095f8285b31fbb5132c9df3887c6859f3 add canonical trade path layer",
    },
    {
        "milestone_id": "MILESTONE_RA8_ACCUMULATOR",
        "label": "Live path accumulator introduced",
        "boundary_at": "2026-07-07T08:37:07-04:00",
        "source": "git commit 4cc67bc3d959df0abedb0ddb0daf1a4c6e9086a3 add live trade path accumulator",
    },
    {
        "milestone_id": "MILESTONE_PATH_CAPTURE_REFERENCES",
        "label": "Canonical trade path capture references introduced",
        "boundary_at": "2026-07-27T05:47:43-04:00",
        "source": "git commit dc322d4ba0e2c807866d923794545ecf70e58307 add canonical trade path capture references",
    },
    {
        "milestone_id": "MILESTONE_CRR_FOUNDATION",
        "label": "Canonical Research Record foundation introduced",
        "boundary_at": "2026-08-06T01:32:44-04:00",
        "source": "git commit c903c14ed93bea86933cfd4b710cf24fd8701a46 feat: add exact-join CRR v1 foundation",
    },
)


@dataclass(frozen=True)
class ResearchEvidenceExplorerResult:
    analysis: dict[str, Any]
    population: dict[str, Any]
    validation: dict[str, Any]
    analysis_path: Path
    analysis_markdown_path: Path
    population_path: Path
    schema_path: Path
    validation_path: Path
    validation_markdown_path: Path
    presentation_path: Path


@dataclass(frozen=True)
class ResearchInvestigationRunResult:
    index: dict[str, Any]
    investigations: dict[str, dict[str, Any]]
    index_json_path: Path
    index_markdown_path: Path
    index_html_path: Path
    investigation_paths: dict[str, dict[str, Path]]


def run_research_evidence_explorer(
    *,
    crr_path: Path = DEFAULT_CRR_PATH,
    crr_validation_path: Path = DEFAULT_CRR_VALIDATION_PATH,
    eligibility_records_path: Path = DEFAULT_ELIGIBILITY_RECORDS_PATH,
    eligibility_summary_path: Path = DEFAULT_ELIGIBILITY_SUMMARY_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> ResearchEvidenceExplorerResult:
    generated_at = _coerce_now(now)
    crr_rows = _read_jsonl(crr_path)
    crr_validation = _read_json(crr_validation_path)
    eligibility_records = _read_jsonl(eligibility_records_path)
    eligibility_summary = _read_json(eligibility_summary_path)
    analysis, population, validation = build_research_evidence_explorer(
        crr_rows,
        crr_validation=crr_validation,
        crr_path=crr_path,
        crr_validation_path=crr_validation_path,
        eligibility_records=eligibility_records,
        eligibility_summary=eligibility_summary,
        eligibility_records_path=eligibility_records_path,
        eligibility_summary_path=eligibility_summary_path,
        generated_at=generated_at,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis_path = output_dir / ANALYSIS_JSON
    analysis_markdown_path = output_dir / ANALYSIS_MD
    population_path = output_dir / POPULATION_JSON
    schema_path = output_dir / SCHEMA_JSON
    validation_path = output_dir / VALIDATION_REPORT_JSON
    validation_markdown_path = output_dir / VALIDATION_REPORT_MD
    presentation_path = output_dir / PRESENTATION_HTML
    _write_json(analysis_path, analysis)
    _write_json(population_path, population)
    _write_json(schema_path, research_evidence_explorer_schema())
    _write_json(validation_path, validation)
    analysis_markdown_path.write_text(render_analysis_markdown(analysis), encoding="utf-8")
    validation_markdown_path.write_text(render_validation_markdown(validation), encoding="utf-8")
    presentation_path.write_text(render_presentation_html(analysis), encoding="utf-8")
    return ResearchEvidenceExplorerResult(
        analysis=analysis,
        population=population,
        validation=validation,
        analysis_path=analysis_path,
        analysis_markdown_path=analysis_markdown_path,
        population_path=population_path,
        schema_path=schema_path,
        validation_path=validation_path,
        validation_markdown_path=validation_markdown_path,
        presentation_path=presentation_path,
    )


def run_research_investigations(
    *,
    crr_path: Path = DEFAULT_CRR_PATH,
    crr_validation_path: Path = DEFAULT_CRR_VALIDATION_PATH,
    eligibility_records_path: Path = DEFAULT_ELIGIBILITY_RECORDS_PATH,
    eligibility_summary_path: Path = DEFAULT_ELIGIBILITY_SUMMARY_PATH,
    explorer_output_dir: Path = DEFAULT_OUTPUT_DIR,
    output_dir: Path = DEFAULT_INVESTIGATION_OUTPUT_DIR,
    docs_dir: Path = DEFAULT_INVESTIGATION_DOC_DIR,
    now: datetime | str | None = None,
) -> ResearchInvestigationRunResult:
    generated_at = _coerce_now(now)
    crr_rows = _read_jsonl(crr_path)
    crr_validation = _read_json(crr_validation_path)
    eligibility_records = _read_jsonl(eligibility_records_path)
    eligibility_summary = _read_json(eligibility_summary_path)
    analysis, population, _ = build_research_evidence_explorer(
        crr_rows,
        crr_validation=crr_validation,
        crr_path=crr_path,
        crr_validation_path=crr_validation_path,
        eligibility_records=eligibility_records,
        eligibility_summary=eligibility_summary,
        eligibility_records_path=eligibility_records_path,
        eligibility_summary_path=eligibility_summary_path,
        generated_at=generated_at,
    )
    investigations = build_investigation_records(
        analysis,
        generated_at=generated_at,
        crr_path=crr_path,
        crr_validation_path=crr_validation_path,
        explorer_path=explorer_output_dir / ANALYSIS_JSON,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, dict[str, Path]] = {}
    for investigation_id, investigation in investigations.items():
        investigation_dir = output_dir / investigation_id
        investigation_dir.mkdir(parents=True, exist_ok=True)
        validation = validate_investigation_record(investigation)
        evidence = investigation.get("evidence", {})
        population_artifact = investigation.get("population", {})
        investigation_paths = {
            "investigation_json": investigation_dir / "investigation.json",
            "investigation_md": investigation_dir / "investigation.md",
            "population_json": investigation_dir / "population.json",
            "evidence_json": investigation_dir / "evidence.json",
            "validation_json": investigation_dir / "validation_report.json",
            "validation_md": investigation_dir / "validation_report.md",
        }
        if investigation_id == "INV-001":
            loss_attribution = investigation.get("loss_attribution", {})
            forensic_audit = investigation.get("extreme_trade_forensic_audit", {})
            investigation_paths.update(
                {
                    "extreme_loss_inventory_json": investigation_dir / "extreme_loss_inventory.json",
                    "loss_concentration_json": investigation_dir / "loss_concentration.json",
                    "loss_classification_json": investigation_dir / "loss_classification.json",
                    "population_sensitivity_json": investigation_dir / "population_sensitivity.json",
                    "loss_attribution_html": investigation_dir / "loss_attribution.html",
                    "extreme_trade_pnl_reconciliation_json": investigation_dir / EXTREME_FORENSIC_JSON,
                    "extreme_trade_execution_lineage_json": investigation_dir / EXTREME_LINEAGE_JSON,
                    "extreme_trade_classification_json": investigation_dir / EXTREME_CLASSIFICATION_JSON,
                    "extreme_trade_population_impact_json": investigation_dir / EXTREME_POPULATION_IMPACT_JSON,
                    "extreme_trade_forensic_review_html": investigation_dir / EXTREME_FORENSIC_HTML,
                    "forensic_validation_report_json": investigation_dir / EXTREME_FORENSIC_VALIDATION_JSON,
                    "forensic_validation_report_md": investigation_dir / EXTREME_FORENSIC_VALIDATION_MD,
                    "anomaly_source_trace_json": investigation_dir / ANOMALY_SOURCE_TRACE_JSON,
                    "anomaly_root_cause_json": investigation_dir / ANOMALY_ROOT_CAUSE_JSON,
                    "anomaly_repair_plan_json": investigation_dir / ANOMALY_REPAIR_PLAN_JSON,
                    "anomaly_before_after_json": investigation_dir / ANOMALY_BEFORE_AFTER_JSON,
                    "anomaly_root_cause_review_html": investigation_dir / ANOMALY_ROOT_CAUSE_HTML,
                }
            )
            _write_json(investigation_paths["extreme_loss_inventory_json"], loss_attribution.get("extreme_loss_inventory", {}))
            _write_json(investigation_paths["loss_concentration_json"], loss_attribution.get("loss_concentration", {}))
            _write_json(investigation_paths["loss_classification_json"], loss_attribution.get("loss_classification", {}))
            _write_json(investigation_paths["population_sensitivity_json"], loss_attribution.get("population_sensitivity", {}))
            investigation_paths["loss_attribution_html"].write_text(render_loss_attribution_html(investigation), encoding="utf-8")
            _write_json(investigation_paths["extreme_trade_pnl_reconciliation_json"], forensic_audit.get("pnl_reconciliation", {}))
            _write_json(investigation_paths["extreme_trade_execution_lineage_json"], forensic_audit.get("execution_lineage", {}))
            _write_json(investigation_paths["extreme_trade_classification_json"], forensic_audit.get("classification", {}))
            _write_json(investigation_paths["extreme_trade_population_impact_json"], forensic_audit.get("population_impact", {}))
            investigation_paths["extreme_trade_forensic_review_html"].write_text(render_extreme_trade_forensic_html(investigation), encoding="utf-8")
            forensic_validation = validate_extreme_trade_forensic_audit(forensic_audit)
            _write_json(investigation_paths["forensic_validation_report_json"], forensic_validation)
            investigation_paths["forensic_validation_report_md"].write_text(render_forensic_validation_markdown(forensic_validation), encoding="utf-8")
            _write_json(investigation_paths["anomaly_source_trace_json"], forensic_audit.get("anomaly_source_trace", {}))
            _write_json(investigation_paths["anomaly_root_cause_json"], forensic_audit.get("anomaly_root_cause", {}))
            _write_json(investigation_paths["anomaly_repair_plan_json"], forensic_audit.get("anomaly_repair_plan", {}))
            _write_json(investigation_paths["anomaly_before_after_json"], forensic_audit.get("anomaly_before_after", {}))
            investigation_paths["anomaly_root_cause_review_html"].write_text(render_anomaly_root_cause_html(investigation), encoding="utf-8")
        if investigation_id == "INV-004":
            investigation_paths.update(
                {
                    "performance_summary_json": investigation_dir / INV_004_PERFORMANCE_SUMMARY_JSON,
                    "concentration_by_dimension_json": investigation_dir / INV_004_CONCENTRATION_JSON,
                    "tail_sensitivity_json": investigation_dir / INV_004_TAIL_SENSITIVITY_JSON,
                    "rolling_windows_json": investigation_dir / INV_004_ROLLING_WINDOWS_JSON,
                    "controlled_comparisons_json": investigation_dir / INV_004_CONTROLLED_COMPARISONS_JSON,
                    "top_bottom_trades_json": investigation_dir / INV_004_TOP_BOTTOM_TRADES_JSON,
                    "contradictory_evidence_json": investigation_dir / INV_004_CONTRADICTORY_EVIDENCE_JSON,
                    "nq_performance_attribution_html": investigation_dir / INV_004_HTML,
                }
            )
            _write_json(investigation_paths["performance_summary_json"], investigation.get("performance_summary", {}))
            _write_json(investigation_paths["concentration_by_dimension_json"], investigation.get("concentration_by_dimension", {}))
            _write_json(investigation_paths["tail_sensitivity_json"], investigation.get("tail_sensitivity", {}))
            _write_json(investigation_paths["rolling_windows_json"], investigation.get("rolling_windows", {}))
            _write_json(investigation_paths["controlled_comparisons_json"], investigation.get("controlled_comparisons", {}))
            _write_json(investigation_paths["top_bottom_trades_json"], investigation.get("top_bottom_trades", {}))
            _write_json(investigation_paths["contradictory_evidence_json"], investigation.get("contradictory_evidence_detail", {}))
            investigation_paths["nq_performance_attribution_html"].write_text(render_inv_004_html(investigation), encoding="utf-8")
        _write_json(investigation_paths["investigation_json"], investigation)
        investigation_paths["investigation_md"].write_text(render_investigation_markdown(investigation), encoding="utf-8")
        _write_json(investigation_paths["population_json"], population_artifact)
        _write_json(investigation_paths["evidence_json"], evidence)
        _write_json(investigation_paths["validation_json"], validation)
        investigation_paths["validation_md"].write_text(render_investigation_validation_markdown(validation), encoding="utf-8")
        paths[investigation_id] = investigation_paths
        summary_path = docs_dir / durable_investigation_summary_filename(investigation_id)
        summary_path.write_text(render_durable_investigation_summary(investigation, investigation_paths), encoding="utf-8")
        paths[investigation_id]["durable_summary"] = summary_path

    index = build_investigation_index(investigations, generated_at=generated_at, output_dir=output_dir)
    index_json_path = output_dir / "investigation_index.json"
    index_markdown_path = output_dir / "investigation_index.md"
    index_html_path = output_dir / "investigation_index.html"
    _write_json(index_json_path, index)
    index_markdown_path.write_text(render_investigation_index_markdown(index), encoding="utf-8")
    index_html_path.write_text(render_investigation_index_html(index), encoding="utf-8")
    return ResearchInvestigationRunResult(
        index=index,
        investigations=investigations,
        index_json_path=index_json_path,
        index_markdown_path=index_markdown_path,
        index_html_path=index_html_path,
        investigation_paths=paths,
    )


def build_investigation_records(
    analysis: Mapping[str, Any],
    *,
    generated_at: datetime,
    crr_path: Path,
    crr_validation_path: Path,
    explorer_path: Path,
) -> dict[str, dict[str, Any]]:
    rows = list(analysis.get("trade_drill_down_full_population") or [])
    if not rows:
        rows = full_population_rows_from_analysis(analysis)
    source_artifacts = {
        "crr": str(crr_path),
        "crr_validation": str(crr_validation_path),
        "explorer_analysis": str(explorer_path),
    }
    eligibility_source = analysis.get("source", {}).get("research_eligibility", {})
    if eligibility_source.get("records_path"):
        source_artifacts["research_eligibility_records"] = str(eligibility_source.get("records_path"))
    if eligibility_source.get("summary_path"):
        source_artifacts["research_eligibility_summary"] = str(eligibility_source.get("summary_path"))
    source_fingerprints = {
        "crr": _file_sha256(crr_path),
        "crr_validation": _file_sha256(crr_validation_path),
        "explorer_analysis": _file_sha256(explorer_path),
        "explorer_artifact": analysis.get("deterministic_fingerprint"),
    }
    if eligibility_source.get("records_path"):
        source_fingerprints["research_eligibility_records"] = _file_sha256(Path(str(eligibility_source.get("records_path"))))
    if eligibility_source.get("summary_path"):
        source_fingerprints["research_eligibility_summary"] = _file_sha256(Path(str(eligibility_source.get("summary_path"))))
    common = {
        "schema_version": INVESTIGATION_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "source_artifacts": source_artifacts,
        "source_fingerprints": source_fingerprints,
        "population_definition": analysis.get("population", {}).get("population_definition", {}),
        "active_population_view": analysis.get("population", {}).get("active_population_view"),
        "population_view_summary": analysis.get("population_views", {}),
        "source_confirmed_anomalies": analysis.get("source_confirmed_anomalies", []),
        "review_required_count": analysis.get("population", {}).get("review_required_count", 0),
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    records = {
        "INV-001": build_inv_001(rows, common),
        "INV-002": build_inv_002(rows, common),
        "INV-003": build_inv_003(rows, common),
        "INV-004": build_inv_004(rows, common),
    }
    for record in records.values():
        record["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(record))
    return records


def build_research_evidence_explorer(
    crr_rows: Sequence[Mapping[str, Any]],
    *,
    crr_validation: Mapping[str, Any],
    crr_path: Path,
    crr_validation_path: Path,
    eligibility_records: Sequence[Mapping[str, Any]] = (),
    eligibility_summary: Mapping[str, Any] | None = None,
    eligibility_records_path: Path = DEFAULT_ELIGIBILITY_RECORDS_PATH,
    eligibility_summary_path: Path = DEFAULT_ELIGIBILITY_SUMMARY_PATH,
    generated_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    readiness = list(crr_validation.get("upstream_readiness", []))
    required_ready = all(
        item.get("readiness_classification") == "READY"
        for item in readiness
        if item.get("source_name") in {"canonical_trade_records", *REQUIRED_LAYERS}
    )
    source_validation_status = str(crr_validation.get("status") or "UNKNOWN")
    eligibility_by_id = {str(row.get("research_record_id")): row for row in eligibility_records}
    excluded_source_integrity_ids = {
        record_id
        for record_id, record in eligibility_by_id.items()
        if record.get("classification") == EXCLUDED_SOURCE_INTEGRITY
    }
    population_rows, exclusions = build_explorer_population(
        crr_rows,
        required_sources_ready=required_ready,
        excluded_research_record_ids=excluded_source_integrity_ids,
        exclusion_reason_override="excluded_confirmed_source_integrity_anomaly",
        eligibility_by_id=eligibility_by_id,
    )
    attach_within_instrument_percentiles(population_rows)
    cohorts = build_cohorts(population_rows)
    attach_cohort_memberships(population_rows, cohorts)
    cohort_metrics = {name: cohort_metrics_for(rows, full_population_count=len(population_rows)) for name, rows in cohorts.items()}
    deltas = numeric_metric_deltas(cohort_metrics.get("top_decile", {}), cohort_metrics.get("bottom_decile", {}))
    controlled_comparison = build_within_instrument_comparison(population_rows)
    population_views = build_population_view_comparisons(
        crr_rows,
        required_sources_ready=required_ready,
        eligibility_by_id=eligibility_by_id,
        source_integrity_excluded_ids=excluded_source_integrity_ids,
    )
    anomaly_table = build_anomaly_classification_table(crr_rows, eligibility_by_id=eligibility_by_id)
    review_queue = [
        dict(record)
        for record in sorted(eligibility_records, key=lambda item: str(item.get("research_record_id")))
        if record.get("review_required") is True
    ]
    population = {
        "schema_version": f"{SCHEMA_VERSION}_population",
        "generated_at": generated_at.isoformat(),
        "active_population_view": "SOURCE_INTEGRITY_QUALIFIED" if eligibility_records else "FULL_HISTORICAL",
        "source_crr_path": str(crr_path),
        "source_crr_fingerprint": _file_sha256(crr_path),
        "source_crr_validation_path": str(crr_validation_path),
        "source_crr_validation_status": source_validation_status,
        "source_eligibility_records_path": str(eligibility_records_path) if eligibility_records else None,
        "source_eligibility_summary_path": str(eligibility_summary_path) if eligibility_records else None,
        "source_eligibility_records_fingerprint": _file_sha256(eligibility_records_path) if eligibility_records else None,
        "source_eligibility_summary_fingerprint": _file_sha256(eligibility_summary_path) if eligibility_records else None,
        "required_sources_ready": required_ready,
        "population_definition": {
            "include": "CRR rows with valid required-source readiness, no broken joins, exact CTOL/CTOE/RA7/RA3 joins, numeric realized P&L proxy, and no source-confirmed source-integrity exclusion in the active view.",
            "ra8_required": False,
            "tolerance_joins_allowed": False,
            "source_integrity_qualification": bool(eligibility_records),
        },
        "input_count": len(crr_rows),
        "included_count": len(population_rows),
        "excluded_count": len(exclusions),
        "exclusions": exclusions,
        "population_views": population_views,
        "source_confirmed_anomaly_count": len(anomaly_table),
        "review_required_count": len(review_queue),
        "date_coverage": date_coverage(population_rows),
        "instrument_coverage": _distribution((row.get("instrument") for row in population_rows)),
        "coverage": {
            "ra8_exact_count": sum(1 for row in population_rows if row.get("ra8_coverage_status") == "EXACT"),
            "ra8_missing_count": sum(1 for row in population_rows if row.get("ra8_coverage_status") != "EXACT"),
            "ra8_coverage_rate": _rate(sum(1 for row in population_rows if row.get("ra8_coverage_status") == "EXACT"), len(population_rows)),
        },
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    analysis = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "source": {
            "crr_path": str(crr_path),
            "crr_fingerprint": _file_sha256(crr_path),
            "crr_validation_path": str(crr_validation_path),
            "crr_validation_status": source_validation_status,
            "crr_validation_fingerprint": _file_sha256(crr_validation_path),
            "upstream_readiness": readiness,
            "research_eligibility": {
                "records_path": str(eligibility_records_path) if eligibility_records else None,
                "summary_path": str(eligibility_summary_path) if eligibility_records else None,
                "summary_status": (eligibility_summary or {}).get("schema_version"),
                "classification_counts": (eligibility_summary or {}).get("classification_counts", {}),
            },
        },
        "question": "How do source-qualified completed trades differ across outcome cohorts, and how do those results compare to full history?",
        "comparability_disclosure": {
            "global_comparison_interpretation": "Portfolio-outcome analysis.",
            "limitation": "Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences. It is not automatically a strategy-quality comparison.",
            "controlled_view": "Within-instrument standardized realized P&L percentile comparison.",
            "normalization_policy": "No per-contract or multiplier normalization is invented where contract economics are not present in CRR.",
        },
        "population": population,
        "population_views": population_views,
        "source_confirmed_anomalies": anomaly_table,
        "review_required_queue": review_queue,
        "investigation_highlights": {
            "INV-004": build_inv_004_highlight(population_rows),
        },
        "cohort_definitions": cohort_definitions(len(population_rows)),
        "metric_definitions": metric_definitions(),
        "cohorts": cohort_metrics,
        "deltas": {"top_decile_minus_bottom_decile": deltas},
        "comparability_controlled": controlled_comparison,
        "distributions": build_distributions(population_rows),
        "trade_drill_down": trade_drill_down_rows(population_rows),
        "trade_drill_down_full_population": trade_drill_down_rows(population_rows, limit=None),
        "presentation_contract": {
            "presentation_reads_prepared_artifact_only": True,
            "hidden_recomputation": False,
            "broker_or_runtime_dependency": False,
            "advisory_outputs": False,
        },
        "warnings": data_quality_warnings(population, crr_validation),
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    analysis["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(analysis))
    validation = validate_research_evidence_explorer(analysis)
    return analysis, population, validation


def build_explorer_population(
    rows: Sequence[Mapping[str, Any]],
    *,
    required_sources_ready: bool,
    excluded_research_record_ids: set[str] | None = None,
    exclusion_reason_override: str | None = None,
    eligibility_by_id: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    population: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    excluded_ids = excluded_research_record_ids or set()
    eligibility = eligibility_by_id or {}
    for row in rows:
        record_id = str(row.get("research_record_id") or "")
        reason = exclusion_reason_override if record_id in excluded_ids else exclusion_reason(row, required_sources_ready=required_sources_ready)
        if reason:
            exclusions.append(
                {
                    "research_record_id": row.get("research_record_id"),
                    "source_trade_id": row.get("trade_identity", {}).get("source_trade_id"),
                    "reason": reason,
                    "eligibility_classification": eligibility.get(record_id, {}).get("classification"),
                }
            )
            continue
        normalized = normalize_crr_row(row)
        if record_id in eligibility:
            normalized["research_eligibility"] = {
                "classification": eligibility[record_id].get("classification"),
                "review_required": eligibility[record_id].get("review_required"),
                "limitations": eligibility[record_id].get("limitations", []),
            }
        population.append(normalized)
    population.sort(key=lambda row: (row["exit_time"] or "", row["research_record_id"]))
    return population, exclusions


def exclusion_reason(row: Mapping[str, Any], *, required_sources_ready: bool) -> str | None:
    if not required_sources_ready:
        return "required_upstream_not_ready"
    join_quality = row.get("join_quality", {})
    if join_quality.get("broken"):
        return "broken_join"
    exact_layers = {item.get("layer") for item in join_quality.get("exact", [])}
    missing_required = [layer for layer in REQUIRED_LAYERS if layer not in exact_layers]
    if missing_required:
        return f"missing_required_exact_layers:{','.join(missing_required)}"
    if _number(row.get("outcome_summary", {}).get("realized_pnl_proxy")) is None:
        return "missing_realized_pnl_proxy"
    return None


def normalize_crr_row(row: Mapping[str, Any]) -> dict[str, Any]:
    identity = row.get("trade_identity", {})
    entry = row.get("entry_anchor", {})
    exit_anchor = row.get("exit_anchor", {})
    outcome = row.get("outcome_summary", {})
    enrichment = row.get("enrichment_ref", {}).get("context_validity_summary", {})
    path_ref = row.get("path_ref", {})
    attribution = row.get("attribution_ref", {})
    realized_points = _number(outcome.get("realized_points"))
    mfe = _number(outcome.get("mfe_points"))
    mae = _number(outcome.get("mae_points"))
    giveback = mfe - realized_points if mfe is not None and realized_points is not None else None
    join_quality = row.get("join_quality", {})
    has_ra8 = any(item.get("layer") == "ra8" for item in join_quality.get("exact", []))
    return {
        "research_record_id": row.get("research_record_id"),
        "source_trade_id": identity.get("source_trade_id"),
        "trade_id": identity.get("trade_id"),
        "lifecycle_id": identity.get("lifecycle_id"),
        "con_id": identity.get("con_id"),
        "instrument": identity.get("instrument") or "UNKNOWN",
        "contract": identity.get("contract") or "UNKNOWN",
        "side": str(identity.get("side") or "UNKNOWN").upper(),
        "quantity": identity.get("quantity"),
        "strategy_id": entry.get("strategy_id") or "UNKNOWN",
        "lane_id": entry.get("lane_id") or "UNKNOWN",
        "entry_time": entry.get("entry_time"),
        "exit_time": exit_anchor.get("exit_time"),
        "entry_price": entry.get("entry_price"),
        "entry_exec_id": entry.get("entry_exec_id"),
        "entry_order_id": entry.get("entry_order_id"),
        "entry_perm_id": entry.get("entry_perm_id"),
        "exit_price": exit_anchor.get("exit_price"),
        "exit_exec_id": exit_anchor.get("exit_exec_id"),
        "exit_order_id": exit_anchor.get("exit_order_id"),
        "exit_perm_id": exit_anchor.get("exit_perm_id"),
        "exit_reason": exit_anchor.get("exit_reason") or exit_anchor.get("exit_policy") or "UNKNOWN",
        "exit_policy": exit_anchor.get("exit_policy") or "UNKNOWN",
        "realized_pnl_proxy": _number(outcome.get("realized_pnl_proxy")),
        "realized_points": realized_points,
        "pnl_source_artifact": row.get("outcome_ref", {}).get("path"),
        "pnl_source_record_id": row.get("outcome_ref", {}).get("trade_outcome_id"),
        "path_ref": dict(path_ref),
        "join_quality": dict(join_quality),
        "hold_seconds": _number(outcome.get("hold_seconds")),
        "mfe_points": mfe,
        "mae_points": mae,
        "giveback_points": giveback,
        "data_quality_flags": list(outcome.get("data_quality_flags") or []),
        "session": enrichment.get("session") or "UNKNOWN",
        "regime": enrichment.get("gre_validity_classification") or enrichment.get("market_context_validity_classification") or "UNKNOWN",
        "context_summary": dict(enrichment),
        "entry_attribution_status": attribution.get("entry_attribution_status") or "UNKNOWN",
        "exit_attribution_status": attribution.get("exit_attribution_status") or "UNKNOWN",
        "ra7_path_status": path_ref.get("path_status") or "UNKNOWN",
        "ra8_coverage_status": "EXACT" if has_ra8 else "MISSING",
        "source_provenance": row.get("source_provenance", []),
        "missing_fields": missing_fields_from_row(outcome, enrichment, has_ra8),
    }


def missing_fields_from_row(outcome: Mapping[str, Any], enrichment: Mapping[str, Any], has_ra8: bool) -> list[str]:
    missing: list[str] = []
    checks = {
        "realized_pnl_proxy": outcome.get("realized_pnl_proxy"),
        "hold_seconds": outcome.get("hold_seconds"),
        "mfe_points": outcome.get("mfe_points"),
        "mae_points": outcome.get("mae_points"),
        "session": enrichment.get("session"),
        "regime": enrichment.get("gre_validity_classification") or enrichment.get("market_context_validity_classification"),
    }
    for key, value in checks.items():
        if value in (None, "", [], {}):
            missing.append(key)
    if not has_ra8:
        missing.append("ra8_finalized_capture")
    if "mfe_points" in missing or outcome.get("realized_points") in (None, ""):
        missing.append("giveback_points")
    return sorted(set(missing))


def build_cohorts(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    if not rows:
        return {"top_decile": [], "bottom_decile": [], "winners": [], "losers": [], "long": [], "short": []}
    sorted_rows = sorted(rows, key=lambda row: (float(row["realized_pnl_proxy"]), str(row.get("research_record_id"))))
    boundary = max(1, math.ceil(len(sorted_rows) * 0.10))
    bottom_cutoff = float(sorted_rows[boundary - 1]["realized_pnl_proxy"])
    top_cutoff = float(sorted_rows[-boundary]["realized_pnl_proxy"])
    return {
        "top_decile": [row for row in rows if float(row["realized_pnl_proxy"]) >= top_cutoff],
        "bottom_decile": [row for row in rows if float(row["realized_pnl_proxy"]) <= bottom_cutoff],
        "winners": [row for row in rows if float(row["realized_pnl_proxy"]) > 0.0],
        "losers": [row for row in rows if float(row["realized_pnl_proxy"]) <= 0.0],
        "long": [row for row in rows if str(row.get("side")).upper() == "LONG"],
        "short": [row for row in rows if str(row.get("side")).upper() == "SHORT"],
    }


def attach_within_instrument_percentiles(rows: list[dict[str, Any]]) -> None:
    by_instrument: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_instrument.setdefault(str(row.get("instrument") or "UNKNOWN"), []).append(row)
    for instrument_rows in by_instrument.values():
        for ranked in add_within_instrument_percentiles(instrument_rows):
            for row in instrument_rows:
                if row.get("research_record_id") == ranked.get("research_record_id"):
                    row["within_instrument_pnl_percentile"] = ranked["within_instrument_pnl_percentile"]
                    break


def attach_cohort_memberships(rows: list[dict[str, Any]], cohorts: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
    memberships: dict[str, list[str]] = {str(row.get("research_record_id")): [] for row in rows}
    for name, cohort_rows in cohorts.items():
        for row in cohort_rows:
            memberships.setdefault(str(row.get("research_record_id")), []).append(name)
    for row in rows:
        row["cohort_memberships"] = sorted(memberships.get(str(row.get("research_record_id")), []))


def cohort_metrics_for(rows: Sequence[Mapping[str, Any]], *, full_population_count: int) -> dict[str, Any]:
    pnl = _values(rows, "realized_pnl_proxy")
    hold = _values(rows, "hold_seconds")
    mfe = _values(rows, "mfe_points")
    mae = _values(rows, "mae_points")
    giveback = _values(rows, "giveback_points")
    wins = [value for value in pnl if value > 0.0]
    ra8_available_count = sum(1 for row in rows if ra8_available(row))
    return {
        "trade_count": len(rows),
        "population_percentage": _rate(len(rows), full_population_count),
        "total_realized_pnl_proxy": _round(sum(pnl)) if pnl else None,
        "average_realized_pnl_proxy": _average(pnl),
        "median_realized_pnl_proxy": _median(pnl),
        "win_rate": _rate(len(wins), len(pnl)) if pnl else None,
        "average_duration_seconds": _average(hold),
        "median_duration_seconds": _median(hold),
        "average_mfe": _average(mfe),
        "median_mfe": _median(mfe),
        "average_mae": _average(mae),
        "median_mae": _median(mae),
        "average_giveback": _average(giveback),
        "median_giveback": _median(giveback),
        "long_short_mix": _distribution((row.get("side") for row in rows)),
        "instrument_distribution": _distribution((row.get("instrument") for row in rows)),
        "strategy_distribution": _distribution((row.get("strategy_id") for row in rows), limit=20),
        "session_distribution": _distribution((row.get("session") for row in rows)),
        "regime_distribution": _distribution((row.get("regime") for row in rows)),
        "exit_reason_distribution": _distribution((row.get("exit_reason") for row in rows)),
        "ra8_coverage_count": ra8_available_count,
        "ra8_coverage_percentage": _rate(ra8_available_count, len(rows)),
        "missing_field_counts": _missing_field_counts(rows),
    }


def numeric_metric_deltas(top: Mapping[str, Any], bottom: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "trade_count",
        "total_realized_pnl_proxy",
        "average_realized_pnl_proxy",
        "median_realized_pnl_proxy",
        "win_rate",
        "average_duration_seconds",
        "average_mfe",
        "average_mae",
        "average_giveback",
        "ra8_coverage_percentage",
    )
    deltas: dict[str, Any] = {}
    for key in keys:
        left = _number(top.get(key))
        right = _number(bottom.get(key))
        deltas[key] = _round(left - right) if left is not None and right is not None else None
    return deltas


def build_distributions(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "realized_pnl_proxy": metric_distribution(rows, "realized_pnl_proxy", label="Realized P&L proxy", units="currency_proxy"),
        "duration_seconds": metric_distribution(rows, "hold_seconds", label="Duration", units="seconds"),
        "mfe_points": metric_distribution(rows, "mfe_points", label="MFE", units="points"),
        "mae_points": metric_distribution(rows, "mae_points", label="MAE", units="points"),
        "giveback_points": metric_distribution(rows, "giveback_points", label="Giveback", units="points"),
    }


def metric_distribution(rows: Sequence[Mapping[str, Any]], key: str, *, label: str, units: str) -> dict[str, Any]:
    values = _values(rows, key)
    missing = len(rows) - len(values)
    return {
        "label": label,
        "field": key,
        "units": units,
        "context": "full_population",
        "included_sample_count": len(values),
        "missing_count": missing,
        "missing_rate": _rate(missing, len(rows)),
        "histogram": _histogram(values),
        "sparse_data_warning": len(values) < 30 or _rate(missing, len(rows)) > 0.25,
    }


def build_within_instrument_comparison(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_sample_size: int = MIN_CONTROLLED_INSTRUMENT_SAMPLE,
) -> dict[str, Any]:
    by_instrument: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        by_instrument.setdefault(str(row.get("instrument") or "UNKNOWN"), []).append(row)

    included: dict[str, Any] = {}
    excluded: dict[str, Any] = {}
    all_standardized: list[dict[str, Any]] = []
    for instrument, instrument_rows in sorted(by_instrument.items()):
        if len(instrument_rows) < min_sample_size:
            excluded[instrument] = {
                "sample_size": len(instrument_rows),
                "reason": "below_minimum_sample_size",
                "minimum_sample_size": min_sample_size,
            }
            continue
        standardized = add_within_instrument_percentiles(instrument_rows)
        cohorts = build_percentile_cohorts(standardized)
        included[instrument] = {
            "sample_size": len(instrument_rows),
            "minimum_sample_size": min_sample_size,
            "percentile_method": "Average tie rank within instrument, scaled 0..1.",
            "top_within_instrument": cohort_metrics_for(cohorts["top"], full_population_count=len(instrument_rows)),
            "bottom_within_instrument": cohort_metrics_for(cohorts["bottom"], full_population_count=len(instrument_rows)),
            "delta": numeric_metric_deltas(
                cohort_metrics_for(cohorts["top"], full_population_count=len(instrument_rows)),
                cohort_metrics_for(cohorts["bottom"], full_population_count=len(instrument_rows)),
            ),
            "top_threshold_percentile": cohorts["top_threshold_percentile"],
            "bottom_threshold_percentile": cohorts["bottom_threshold_percentile"],
        }
        all_standardized.extend(standardized)

    standardized_cohorts = build_percentile_cohorts(all_standardized)
    return {
        "method": "standardized_within_instrument_realized_pnl_percentile",
        "minimum_sample_size": min_sample_size,
        "eligible_instrument_count": len(included),
        "excluded_instruments": excluded,
        "included_instruments": included,
        "pooled_standardized_view": {
            "sample_size": len(all_standardized),
            "top_within_instrument_percentile": cohort_metrics_for(
                standardized_cohorts["top"],
                full_population_count=len(all_standardized),
            ),
            "bottom_within_instrument_percentile": cohort_metrics_for(
                standardized_cohorts["bottom"],
                full_population_count=len(all_standardized),
            ),
            "top_threshold_percentile": standardized_cohorts["top_threshold_percentile"],
            "bottom_threshold_percentile": standardized_cohorts["bottom_threshold_percentile"],
        },
    }


def build_population_view_comparisons(
    crr_rows: Sequence[Mapping[str, Any]],
    *,
    required_sources_ready: bool,
    eligibility_by_id: Mapping[str, Mapping[str, Any]],
    source_integrity_excluded_ids: set[str],
) -> dict[str, Any]:
    if not eligibility_by_id:
        rows, exclusions = build_explorer_population(crr_rows, required_sources_ready=required_sources_ready)
        attach_within_instrument_percentiles(rows)
        return {
            "FULL_HISTORICAL": {
                "view_id": "FULL_HISTORICAL",
                "included_count": len(rows),
                "excluded_count": len(exclusions),
                "excluded_count_by_reason": _distribution(item.get("reason") for item in exclusions),
                "metrics": investigation_metrics(rows),
                "long_short_outcomes": {
                    "long": investigation_metrics([row for row in rows if row.get("side") == "LONG"]),
                    "short": investigation_metrics([row for row in rows if row.get("side") == "SHORT"]),
                },
                "instrument_mix": _distribution(row.get("instrument") for row in rows),
                "strategy_mix": _distribution((row.get("strategy_id") for row in rows), limit=30),
                "session_mix": _distribution(row.get("session") for row in rows),
                "regime_mix": _distribution(row.get("regime") for row in rows),
                "missingness": _missing_field_counts(rows),
                "ra8_coverage": {
                    "exact_count": sum(1 for row in rows if row.get("ra8_coverage_status") == "EXACT"),
                    "missing_count": sum(1 for row in rows if row.get("ra8_coverage_status") != "EXACT"),
                    "coverage_rate": _rate(sum(1 for row in rows if row.get("ra8_coverage_status") == "EXACT"), len(rows)),
                },
                "comparability_controlled": build_within_instrument_comparison(rows),
            }
        }
    view_definitions = {
        "FULL_HISTORICAL": set(),
        "SOURCE_INTEGRITY_QUALIFIED": set(source_integrity_excluded_ids),
        "ORDINARY_STRATEGY_EVIDENCE": {
            str(record_id)
            for record_id, record in eligibility_by_id.items()
            if record.get("classification")
            not in {"ELIGIBLE_ORDINARY_STRATEGY_EVIDENCE", "ELIGIBLE_WITH_LIMITATIONS"}
        },
        "REVIEW_REQUIRED": {
            str(row.get("research_record_id"))
            for row in crr_rows
            if eligibility_by_id.get(str(row.get("research_record_id")), {}).get("review_required") is not True
        },
    }
    views: dict[str, Any] = {}
    for view_id, excluded_ids in view_definitions.items():
        rows, exclusions = build_explorer_population(
            crr_rows,
            required_sources_ready=required_sources_ready,
            excluded_research_record_ids=excluded_ids,
            exclusion_reason_override=f"not_in_{view_id.lower()}",
            eligibility_by_id=eligibility_by_id,
        )
        attach_within_instrument_percentiles(rows)
        views[view_id] = {
            "view_id": view_id,
            "included_count": len(rows),
            "excluded_count": len(exclusions),
            "excluded_count_by_reason": _distribution(item.get("eligibility_classification") or item.get("reason") for item in exclusions),
            "metrics": investigation_metrics(rows),
            "long_short_outcomes": {
                "long": investigation_metrics([row for row in rows if row.get("side") == "LONG"]),
                "short": investigation_metrics([row for row in rows if row.get("side") == "SHORT"]),
            },
            "instrument_mix": _distribution(row.get("instrument") for row in rows),
            "strategy_mix": _distribution((row.get("strategy_id") for row in rows), limit=30),
            "session_mix": _distribution(row.get("session") for row in rows),
            "regime_mix": _distribution(row.get("regime") for row in rows),
            "missingness": _missing_field_counts(rows),
            "ra8_coverage": {
                "exact_count": sum(1 for row in rows if row.get("ra8_coverage_status") == "EXACT"),
                "missing_count": sum(1 for row in rows if row.get("ra8_coverage_status") != "EXACT"),
                "coverage_rate": _rate(sum(1 for row in rows if row.get("ra8_coverage_status") == "EXACT"), len(rows)),
            },
            "comparability_controlled": build_within_instrument_comparison(rows),
        }
    return views


def build_anomaly_classification_table(
    crr_rows: Sequence[Mapping[str, Any]],
    *,
    eligibility_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows_by_id = {str(row.get("research_record_id")): row for row in crr_rows}
    result = []
    for record_id, eligibility in sorted(eligibility_by_id.items()):
        if eligibility.get("classification") != EXCLUDED_SOURCE_INTEGRITY:
            continue
        row = rows_by_id.get(record_id, {})
        result.append(
            {
                "research_record_id": record_id,
                "source_trade_id": row.get("trade_identity", {}).get("source_trade_id") or eligibility.get("source_trade_id"),
                "instrument": row.get("trade_identity", {}).get("instrument"),
                "side": row.get("trade_identity", {}).get("side"),
                "realized_pnl_proxy": row.get("outcome_summary", {}).get("realized_pnl_proxy"),
                "classification": eligibility.get("classification"),
                "evidence_basis": eligibility.get("evidence_basis"),
                "confidence": eligibility.get("confidence"),
                "supporting_ids": eligibility.get("supporting_ids"),
                "supporting_artifact_paths": eligibility.get("supporting_artifact_paths"),
            }
        )
    return result


def build_inv_004_highlight(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    nq_rows = [row for row in rows if str(row.get("instrument") or "").upper() == "NQ"]
    if not nq_rows:
        return {
            "investigation_id": "INV-004",
            "title": "NQ Qualified Performance Attribution",
            "qualified_nq_trade_count": 0,
            "status": "NO_NQ_ROWS",
        }
    concentration = nq_concentration_by_dimensions(nq_rows)
    tail = nq_tail_sensitivity(nq_rows)
    top_strategy = next(iter(concentration.get("dimensions", {}).get("strategy", [])), {})
    top_lane = next(iter(concentration.get("dimensions", {}).get("lane", [])), {})
    top_session = next(iter(concentration.get("dimensions", {}).get("session", [])), {})
    contradictory = nq_contradictory_evidence(nq_rows, concentration=concentration, tail=tail, rolling=nq_rolling_windows(nq_rows))
    return {
        "investigation_id": "INV-004",
        "title": "NQ Qualified Performance Attribution",
        "artifact_path": str(DEFAULT_INVESTIGATION_OUTPUT_DIR / "INV-004" / "investigation.json"),
        "html_path": str(DEFAULT_INVESTIGATION_OUTPUT_DIR / "INV-004" / INV_004_HTML),
        "qualified_nq_trade_count": len(nq_rows),
        "total_realized_pnl_proxy": investigation_metrics(nq_rows).get("total_realized_pnl_proxy"),
        "average_realized_pnl_proxy": investigation_metrics(nq_rows).get("average_realized_pnl_proxy"),
        "median_realized_pnl_proxy": investigation_metrics(nq_rows).get("median_realized_pnl_proxy"),
        "win_rate": investigation_metrics(nq_rows).get("win_rate"),
        "tail_sensitivity": {
            "excluding_top_10_percent_total": tail.get("excluding_top_10_percent", {}).get("metrics", {}).get("total_realized_pnl_proxy"),
            "primary_finding": tail.get("primary_finding"),
        },
        "top_contributors": {
            "strategy": top_strategy,
            "lane": top_lane,
            "session": top_session,
        },
        "strongest_contradictory_evidence": contradictory.get("summary", [])[:3],
        "guardrails": dict(GUARDRAILS),
    }


def add_within_instrument_percentiles(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=lambda row: (float(row["realized_pnl_proxy"]), str(row.get("research_record_id"))))
    if len(sorted_rows) == 1:
        only = dict(sorted_rows[0])
        only["within_instrument_pnl_percentile"] = 0.5
        return [only]
    percentile_by_id: dict[str, float] = {}
    index = 0
    while index < len(sorted_rows):
        value = float(sorted_rows[index]["realized_pnl_proxy"])
        end = index
        while end + 1 < len(sorted_rows) and float(sorted_rows[end + 1]["realized_pnl_proxy"]) == value:
            end += 1
        average_rank = (index + end) / 2.0
        percentile = _round(average_rank / (len(sorted_rows) - 1))
        for tied_index in range(index, end + 1):
            percentile_by_id[str(sorted_rows[tied_index].get("research_record_id"))] = float(percentile)
        index = end + 1
    result: list[dict[str, Any]] = []
    for row in rows:
        copied = dict(row)
        copied["within_instrument_pnl_percentile"] = percentile_by_id[str(row.get("research_record_id"))]
        result.append(copied)
    return result


def build_percentile_cohorts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {"top": [], "bottom": [], "top_threshold_percentile": None, "bottom_threshold_percentile": None}
    sorted_rows = sorted(rows, key=lambda row: (float(row["within_instrument_pnl_percentile"]), str(row.get("research_record_id"))))
    boundary = max(1, math.ceil(len(sorted_rows) * 0.10))
    bottom_threshold = float(sorted_rows[boundary - 1]["within_instrument_pnl_percentile"])
    top_threshold = float(sorted_rows[-boundary]["within_instrument_pnl_percentile"])
    return {
        "top": [row for row in rows if float(row["within_instrument_pnl_percentile"]) >= top_threshold],
        "bottom": [row for row in rows if float(row["within_instrument_pnl_percentile"]) <= bottom_threshold],
        "top_threshold_percentile": _round(top_threshold),
        "bottom_threshold_percentile": _round(bottom_threshold),
    }


def date_coverage(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    entry_times = sorted(str(row.get("entry_time")) for row in rows if row.get("entry_time"))
    exit_times = sorted(str(row.get("exit_time")) for row in rows if row.get("exit_time"))
    return {
        "first_entry_time": entry_times[0] if entry_times else None,
        "last_entry_time": entry_times[-1] if entry_times else None,
        "first_exit_time": exit_times[0] if exit_times else None,
        "last_exit_time": exit_times[-1] if exit_times else None,
    }


def trade_drill_down_rows(rows: Sequence[Mapping[str, Any]], *, limit: int | None = 200) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: (abs(float(row.get("realized_pnl_proxy") or 0.0)), str(row.get("research_record_id"))), reverse=True)
    selected = ranked if limit is None else ranked[:limit]
    return [
        {
            "research_record_id": row.get("research_record_id"),
            "source_trade_id": row.get("source_trade_id"),
            "trade_id": row.get("trade_id"),
            "lifecycle_id": row.get("lifecycle_id"),
            "con_id": row.get("con_id"),
            "instrument": row.get("instrument"),
            "contract": row.get("contract"),
            "side": row.get("side"),
            "entry_time": row.get("entry_time"),
            "exit_time": row.get("exit_time"),
            "entry_price": row.get("entry_price"),
            "exit_price": row.get("exit_price"),
            "entry_exec_id": row.get("entry_exec_id"),
            "entry_order_id": row.get("entry_order_id"),
            "entry_perm_id": row.get("entry_perm_id"),
            "exit_exec_id": row.get("exit_exec_id"),
            "exit_order_id": row.get("exit_order_id"),
            "exit_perm_id": row.get("exit_perm_id"),
            "broker_order_fill_refs": {
                "entry_exec_id": row.get("entry_exec_id"),
                "entry_order_id": row.get("entry_order_id"),
                "entry_perm_id": row.get("entry_perm_id"),
                "exit_exec_id": row.get("exit_exec_id"),
                "exit_order_id": row.get("exit_order_id"),
                "exit_perm_id": row.get("exit_perm_id"),
            },
            "quantity": row.get("quantity"),
            "strategy_id": row.get("strategy_id"),
            "lane_id": row.get("lane_id"),
            "session": row.get("session"),
            "regime": row.get("regime"),
            "exit_policy": row.get("exit_policy"),
            "exit_reason": row.get("exit_reason"),
            "cohort_memberships": row.get("cohort_memberships", []),
            "realized_pnl_proxy": row.get("realized_pnl_proxy"),
            "realized_points": row.get("realized_points"),
            "pnl_source_artifact": row.get("pnl_source_artifact"),
            "pnl_source_record_id": row.get("pnl_source_record_id"),
            "within_instrument_pnl_percentile": row.get("within_instrument_pnl_percentile"),
            "hold_seconds": row.get("hold_seconds"),
            "outcome_summary": {
                "mfe_points": row.get("mfe_points"),
                "mae_points": row.get("mae_points"),
                "giveback_points": row.get("giveback_points"),
            },
            "context_summary": row.get("context_summary"),
            "attribution_status": {
                "entry": row.get("entry_attribution_status"),
                "exit": row.get("exit_attribution_status"),
            },
            "path_status": {
                "ra7": row.get("ra7_path_status"),
                "ra8": row.get("ra8_coverage_status"),
            },
            "source_provenance": row.get("source_provenance"),
            "missing_fields": row.get("missing_fields"),
            "data_quality_flags": row.get("data_quality_flags", []),
        }
        for row in selected
    ]


def full_population_rows_from_analysis(analysis: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in analysis.get("trade_drill_down", [])]


def filter_population(rows: Sequence[Mapping[str, Any]], filters: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in rows:
        reason = filter_exclusion_reason(row, filters)
        if reason:
            excluded.append({"research_record_id": row.get("research_record_id"), "reason": reason})
        else:
            included.append(dict(row))
    return included, excluded


def filter_exclusion_reason(row: Mapping[str, Any], filters: Mapping[str, Any]) -> str | None:
    for field in ("instrument", "side", "strategy_id", "lane_id", "session", "regime", "exit_policy", "exit_reason"):
        allowed = filters.get(field)
        if allowed is None:
            continue
        values = {str(item).upper() for item in (allowed if isinstance(allowed, list) else [allowed])}
        if str(row.get(field) or "UNKNOWN").upper() not in values:
            return f"{field}_not_selected"
    if filters.get("winner_loser"):
        pnl = _number(row.get("realized_pnl_proxy"))
        if filters["winner_loser"] == "winner" and (pnl is None or pnl <= 0):
            return "not_winner"
        if filters["winner_loser"] == "loser" and (pnl is None or pnl > 0):
            return "not_loser"
    if filters.get("ra8") == "available" and not ra8_available(row):
        return "ra8_unavailable"
    if filters.get("ra8") == "unavailable" and ra8_available(row):
        return "ra8_available"
    date_range = filters.get("date_range") or {}
    exit_time = str(row.get("exit_time") or "")
    if date_range.get("start") and exit_time < str(date_range["start"]):
        return "before_date_range"
    if date_range.get("end") and exit_time >= str(date_range["end"]):
        return "after_date_range"
    return None


def investigation_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = _values(rows, "realized_pnl_proxy")
    winners = [value for value in pnl if value > 0]
    losers = [value for value in pnl if value <= 0]
    average_winner = _average(winners)
    average_loser = _average(losers)
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    return {
        **cohort_metrics_for(rows, full_population_count=len(rows)),
        "average_winner": average_winner,
        "average_loser": average_loser,
        "payoff_ratio": _round(abs(average_winner / average_loser)) if average_winner is not None and average_loser not in (None, 0) else None,
        "profit_factor_proxy": _round(gross_profit / gross_loss) if gross_loss else None,
        "expectancy": _average(pnl),
        "trimmed_mean_5_percent": trimmed_mean(pnl, trim_fraction=0.05),
        "missingness": _missing_field_counts(rows),
    }


def trimmed_mean(values: Sequence[float], *, trim_fraction: float = 0.05) -> float | None:
    if not values:
        return None
    sorted_values = sorted(float(value) for value in values)
    trim = int(len(sorted_values) * trim_fraction)
    trimmed = sorted_values[trim : len(sorted_values) - trim] if trim and len(sorted_values) > trim * 2 else sorted_values
    return _average(trimmed)


def build_inv_001(rows: Sequence[Mapping[str, Any]], common: Mapping[str, Any]) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0), str(row.get("research_record_id"))))
    losers = [row for row in sorted_rows if (_number(row.get("realized_pnl_proxy")) or 0) <= 0]
    bottom20 = sorted_rows[:20]
    bottom1 = percentile_slice(sorted_rows, 0.01, low=True)
    bottom5 = percentile_slice(sorted_rows, 0.05, low=True)
    bottom10 = percentile_slice(sorted_rows, 0.10, low=True)
    worst1_excluded = sorted_rows[len(bottom1) :]
    worst5_excluded = sorted_rows[len(bottom5) :]
    evidence = {
        "bottom_20_trades": bottom20,
        "bottom_1_percent": cohort_evidence(bottom1),
        "bottom_5_percent": cohort_evidence(bottom5),
        "bottom_10_percent": cohort_evidence(bottom10),
        "median_loser": median_trade(losers),
        "full_losing_population_metrics": investigation_metrics(losers),
        "concentration": {
            field: _distribution((row.get(field) for row in bottom20), limit=20)
            for field in ("instrument", "strategy_id", "lane_id", "session", "regime", "side", "exit_reason")
        },
        "mean_median_trimmed": {
            "full_population": mean_median_trimmed(rows),
            "losers": mean_median_trimmed(losers),
            "bottom_10_percent": mean_median_trimmed(bottom10),
            "excluding_worst_1_percent": mean_median_trimmed(worst1_excluded),
            "excluding_worst_5_percent": mean_median_trimmed(worst5_excluded),
        },
        "classification": classify_extreme_losses(bottom20),
    }
    loss_attribution = build_loss_attribution(rows)
    forensic_audit = build_extreme_trade_forensic_audit(rows)
    forensic_audit = attach_eligibility_excluded_anomalies(
        forensic_audit,
        common.get("source_confirmed_anomalies", []),
    )
    finding = (
        "Aggregate negative results are materially affected by extreme losses: "
        f"bottom 1% total P&L proxy {cohort_evidence(bottom1)['metrics'].get('total_realized_pnl_proxy')}, "
        f"bottom 5% total P&L proxy {cohort_evidence(bottom5)['metrics'].get('total_realized_pnl_proxy')}."
    )
    return {
        **common,
        "investigation_id": "INV-001",
        "title": "Extreme Loss Concentration",
        "status": "DRAFT",
        "question": "What explains the unusually large bottom-decile losses?",
        "rationale": "The global bottom-decile average loss is far below its median, indicating skew that may concentrate aggregate losses.",
        "population": {"count": len(rows), "filters": {}, "exclusions": []},
        "filters_and_exclusions": {"filters": {}, "excluded_count": 0},
        "methodology": [
            "Identify bottom 20 trades and bottom 1%, 5%, and 10% cohorts by realized P&L proxy.",
            "Compare mean, median, trimmed mean, and results excluding worst tails.",
            "Describe concentration by instrument, strategy, lane, session, regime, side, and exit reason.",
            "Do not infer classification without source support.",
        ],
        "metrics": {"full_population": investigation_metrics(rows)},
        "evidence": evidence,
        "loss_attribution": loss_attribution,
        "extreme_trade_forensic_audit": forensic_audit,
        "contradictory_evidence": [
            "The full losing population is larger than the extreme-loss tail, so losses are not solely a one-trade issue.",
            "Sparse MFE/MAE/giveback limits exit-path explanation.",
            "The forensic audit reconciles arithmetic only where source fields are present; missing authoritative contract economics remains a limitation.",
        ],
        "findings": [finding, loss_attribution["summary"]["primary_finding"], forensic_audit["summary"]["primary_finding"]],
        "limitations": ["Raw P&L comparability can reflect instrument, multiplier, and quantity differences.", "RA8 path evidence is partial.", "Extreme-loss classification remains insufficient where source context is absent.", "Contract point values are not independently sourced in CRR v1 and are treated as unresolved when absent."],
        "confidence": "PARTIAL",
        "conclusion_status": "PARTIALLY_SUPPORTED",
        "unresolved_questions": ["Which extreme losses are true strategy outcomes versus development or contract-economics artifacts?"],
        "follow_up_candidates": ["Create a loss attribution view by instrument/session/strategy with source-evidence classifications."],
    }


def build_inv_002(rows: Sequence[Mapping[str, Any]], common: Mapping[str, Any]) -> dict[str, Any]:
    long_rows, _ = filter_population(rows, {"side": "LONG"})
    short_rows, _ = filter_population(rows, {"side": "SHORT"})
    groups = {
        "instrument": side_comparisons_by_field(rows, "instrument"),
        "session": side_comparisons_by_field(rows, "session"),
        "strategy_id": side_comparisons_by_field(rows, "strategy_id"),
        "regime": side_comparisons_by_field(rows, "regime"),
        "calendar_month": side_comparisons_by_period(rows, "month"),
    }
    controlled = side_control_summary(groups)
    return {
        **common,
        "investigation_id": "INV-002",
        "title": "Long Short Underperformance Control",
        "status": "DRAFT",
        "question": "Does long-side underperformance persist after controlling for instrument, session, strategy, and time period?",
        "rationale": "Global long and short averages are both negative, with long worse globally. Composition may explain the observed side gap.",
        "population": {"count": len(rows), "filters": {}, "exclusions": []},
        "filters_and_exclusions": {"minimum_cell_sample": MIN_COMPARISON_CELL_SAMPLE, "excluded_sparse_cells": controlled["excluded_sparse_cells"]},
        "methodology": [
            "Compare long versus short globally.",
            "Compare side metrics within instrument, session, strategy, regime, and month where both sides meet the sample threshold.",
            "Use raw-dollar comparability disclosure and within-instrument control rather than invented multiplier normalization.",
        ],
        "metrics": {"long": investigation_metrics(long_rows), "short": investigation_metrics(short_rows)},
        "evidence": {"controlled_side_comparisons": groups, "summary": controlled},
        "contradictory_evidence": controlled["contradictory_evidence"],
        "findings": [controlled["finding"]],
        "limitations": ["Sparse cells are excluded rather than pooled.", "Raw P&L proxy is not normalized by contract economics.", "Side may be confounded by strategy, instrument, and time period."],
        "confidence": controlled["confidence"],
        "conclusion_status": controlled["conclusion_status"],
        "unresolved_questions": ["Which side gaps persist within specific high-sample instrument/session/strategy combinations?"],
        "follow_up_candidates": ["Build a side-comparison explorer table with minimum-sample controls and confidence labels."],
    }


def build_inv_003(rows: Sequence[Mapping[str, Any]], common: Mapping[str, Any]) -> dict[str, Any]:
    monthly = period_metrics(rows, "month")
    weekly = period_metrics(rows, "week", min_sample=30)
    rolling = rolling_window_metrics(rows, window_size=100)
    milestone = milestone_period_metrics(rows)
    finding = "Performance varies materially across calendar and milestone periods, but population composition also changes; the evidence is descriptive rather than causal."
    return {
        **common,
        "investigation_id": "INV-003",
        "title": "Performance Over Development History",
        "status": "DRAFT",
        "question": "Has performance changed materially across the platform's development history?",
        "rationale": "The CRR population spans multiple development milestones and may obscure changing trade populations over time.",
        "population": {"count": len(rows), "filters": {}, "exclusions": []},
        "filters_and_exclusions": {"weekly_minimum_sample": 30, "milestone_boundaries": list(EVIDENCE_BACKED_MILESTONES)},
        "methodology": [
            "Create neutral monthly, weekly, and rolling completed-trade windows.",
            "Create milestone periods using only committed repository evidence.",
            "Report mix changes and outcome metrics without implying milestone causality.",
        ],
        "metrics": {"full_population": investigation_metrics(rows)},
        "evidence": {"monthly": monthly, "weekly": weekly, "rolling_100_trade_windows": rolling, "milestone_periods": milestone},
        "contradictory_evidence": ["Milestone boundaries are development evidence, not causal market or strategy regime labels.", "Instrument and strategy mix changes can explain apparent performance shifts."],
        "findings": [finding],
        "limitations": ["Milestones are repository-evidence boundaries only.", "Weekly periods below sample threshold are excluded.", "RA8 coverage improves over time and can change path-evidence availability."],
        "confidence": "PARTIAL",
        "conclusion_status": "PARTIALLY_SUPPORTED",
        "unresolved_questions": ["Which period changes remain after controlling for instrument, session, strategy, and side?"],
        "follow_up_candidates": ["Add a period-composition explorer that separates outcome change from population-mix change."],
    }


def build_inv_004(rows: Sequence[Mapping[str, Any]], common: Mapping[str, Any]) -> dict[str, Any]:
    nq_rows = [row for row in rows if str(row.get("instrument") or "").upper() == "NQ"]
    performance_summary = nq_performance_summary(nq_rows)
    concentration = nq_concentration_by_dimensions(nq_rows)
    tail = nq_tail_sensitivity(nq_rows)
    rolling = nq_rolling_windows(nq_rows)
    controlled = nq_controlled_comparisons(nq_rows)
    top_bottom = nq_top_bottom_trades(nq_rows)
    contradictory = nq_contradictory_evidence(nq_rows, concentration=concentration, tail=tail, rolling=rolling)
    classification = nq_breadth_classification(tail, concentration)
    positive_after_tail = tail.get("excluding_top_10_percent", {}).get("metrics", {}).get("total_realized_pnl_proxy")
    status = "PARTIALLY_SUPPORTED" if (_number(positive_after_tail) or 0) > 0 else "INCONCLUSIVE"
    finding = (
        f"NQ contributes {performance_summary.get('metrics', {}).get('total_realized_pnl_proxy')} qualified P&L proxy "
        f"across {performance_summary.get('trade_count')} trades; breadth is classified as {classification.get('classification')}."
    )
    return {
        **common,
        "investigation_id": "INV-004",
        "title": "NQ Qualified Performance Attribution",
        "status": "DRAFT",
        "question": "What explains NQ's approximately +907,653 qualified P&L contribution?",
        "rationale": "The source-integrity-qualified Explorer shows NQ as the dominant positive contributor, requiring a bounded attribution review before forming narrower research questions.",
        "population": {
            "count": len(nq_rows),
            "filters": {"instrument": "NQ", "active_population_view": "SOURCE_INTEGRITY_QUALIFIED"},
            "exclusions": [],
            "date_coverage": date_coverage(nq_rows),
            "contracts": _distribution(row.get("contract") for row in nq_rows),
            "side_mix": _distribution(row.get("side") for row in nq_rows),
            "strategy_mix": _distribution((row.get("strategy_id") for row in nq_rows), limit=30),
            "lane_mix": _distribution((row.get("lane_id") for row in nq_rows), limit=30),
            "session_mix": _distribution(row.get("session") for row in nq_rows),
            "regime_mix": _distribution(row.get("regime") for row in nq_rows),
            "exit_policy_mix": _distribution(row.get("exit_policy") for row in nq_rows),
            "exit_reason_mix": _distribution(row.get("exit_reason") for row in nq_rows),
            "ra8_coverage": cohort_evidence(nq_rows)["ra8_coverage"],
            "missingness": _missing_field_counts(nq_rows),
            "source_fingerprints": common.get("source_fingerprints", {}),
        },
        "filters_and_exclusions": {
            "source_population": "SOURCE_INTEGRITY_QUALIFIED",
            "instrument": "NQ",
            "minimum_controlled_cell_sample": MIN_COMPARISON_CELL_SAMPLE,
            "raw_dollar_disclosure": "Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences; INV-004 stays within NQ but still does not infer contract economics.",
        },
        "methodology": [
            "Filter the prepared source-integrity-qualified Explorer population to NQ only.",
            "Attribute NQ contribution by strategy, lane, session, regime, side, exit policy/reason, calendar period, milestone period, contract, and quantity.",
            "Measure tail sensitivity by removing deterministic top percentile cohorts and the single largest winner.",
            "Use controlled comparisons only where minimum sample thresholds are met; sparse cells are reported rather than pooled.",
            "Treat all results as descriptive and non-causal with no production or trading authority.",
        ],
        "metrics": {"full_nq_qualified_population": performance_summary.get("metrics", {})},
        "performance_summary": performance_summary,
        "concentration_by_dimension": concentration,
        "tail_sensitivity": tail,
        "rolling_windows": rolling,
        "controlled_comparisons": controlled,
        "top_bottom_trades": top_bottom,
        "contradictory_evidence_detail": contradictory,
        "evidence": {
            "performance_summary": performance_summary,
            "concentration_by_dimension": concentration,
            "tail_sensitivity": tail,
            "rolling_windows": rolling,
            "controlled_comparisons": controlled,
            "top_bottom_trades": top_bottom,
            "breadth_classification": classification,
        },
        "contradictory_evidence": contradictory.get("summary", []),
        "findings": [
            finding,
            tail.get("primary_finding"),
            controlled.get("summary", {}).get("finding"),
        ],
        "limitations": [
            "Independent contract point-value provenance is missing in CRR v1.",
            "MFE, MAE, giveback, and full path evidence remain sparse, so exit-quality conclusions are limited.",
            "Concentration and period attribution are descriptive and do not establish causality.",
            "Raw P&L proxy is not a normalized risk or multiplier-adjusted measure.",
        ],
        "confidence": "PARTIAL",
        "conclusion_status": status,
        "unresolved_questions": [
            "Which NQ setup/context fields explain the positive outliers without relying on missing path data?",
            "Does NQ remain positive after future contract-economics provenance is added?",
        ],
        "follow_up_candidates": [
            "Run a narrow NQ strategy/session attribution experiment with fixed source-integrity-qualified membership.",
        ],
    }


def nq_performance_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "inv_004_nq_performance_summary_v1",
        "instrument": "NQ",
        "trade_count": len(rows),
        "date_coverage": date_coverage(rows),
        "contracts": _distribution(row.get("contract") for row in rows),
        "side_mix": _distribution(row.get("side") for row in rows),
        "strategy_mix": _distribution((row.get("strategy_id") for row in rows), limit=30),
        "lane_mix": _distribution((row.get("lane_id") for row in rows), limit=30),
        "session_mix": _distribution(row.get("session") for row in rows),
        "regime_mix": _distribution(row.get("regime") for row in rows),
        "exit_policy_mix": _distribution(row.get("exit_policy") for row in rows),
        "exit_reason_mix": _distribution(row.get("exit_reason") for row in rows),
        "metrics": investigation_metrics(rows),
        "best_1_percent": nq_percentile_contribution(rows, fraction=0.01, low=False),
        "best_5_percent": nq_percentile_contribution(rows, fraction=0.05, low=False),
        "best_10_percent": nq_percentile_contribution(rows, fraction=0.10, low=False),
        "worst_1_percent": nq_percentile_contribution(rows, fraction=0.01, low=True),
        "worst_5_percent": nq_percentile_contribution(rows, fraction=0.05, low=True),
        "worst_10_percent": nq_percentile_contribution(rows, fraction=0.10, low=True),
        "ra8_coverage": cohort_evidence(rows)["ra8_coverage"],
        "missingness": _missing_field_counts(rows),
        "comparability_disclosure": "Within NQ only; no multiplier or contract-economics normalization is inferred.",
        "guardrails": dict(GUARDRAILS),
    }


def nq_percentile_contribution(rows: Sequence[Mapping[str, Any]], *, fraction: float, low: bool) -> dict[str, Any]:
    cohort = percentile_slice(rows, fraction, low=low)
    total = sum(_values(rows, "realized_pnl_proxy"))
    cohort_total = sum(_values(cohort, "realized_pnl_proxy"))
    return {
        "fraction": fraction,
        "direction": "worst" if low else "best",
        "trade_count": len(cohort),
        "total_realized_pnl_proxy": _round(cohort_total),
        "percentage_of_nq_total": _rate(cohort_total, total),
        "metrics": investigation_metrics(cohort),
        "trade_ids": [row.get("research_record_id") for row in sorted(cohort, key=lambda item: (float(item.get("realized_pnl_proxy") or 0), str(item.get("research_record_id"))), reverse=not low)],
    }


def nq_concentration_by_dimensions(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    dimensions = {
        "strategy": "strategy_id",
        "lane": "lane_id",
        "session": "session",
        "regime": "regime",
        "side": "side",
        "exit_policy": "exit_policy",
        "exit_reason": "exit_reason",
        "calendar_week": "calendar_week",
        "calendar_month": "calendar_month",
        "milestone_period": "milestone_period",
        "contract": "contract",
        "quantity": "quantity",
    }
    return {
        "schema_version": "inv_004_nq_concentration_by_dimension_v1",
        "total_realized_pnl_proxy": _round(sum(_values(rows, "realized_pnl_proxy"))),
        "dimensions": {
            name: nq_concentration_by_field(rows, field)
            for name, field in dimensions.items()
        },
        "guardrails": dict(GUARDRAILS),
    }


def nq_concentration_by_field(rows: Sequence[Mapping[str, Any]], field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        if field == "calendar_week":
            key = period_key(row.get("exit_time"), "week")
        elif field == "calendar_month":
            key = period_key(row.get("exit_time"), "month")
        elif field == "milestone_period":
            key = milestone_label_for_row(row)
        else:
            key = str(row.get(field) or "UNKNOWN")
        grouped.setdefault(key, []).append(row)
    total = sum(_values(rows, "realized_pnl_proxy"))
    records = []
    for value, value_rows in sorted(grouped.items()):
        metrics = investigation_metrics(value_rows)
        group_total = _number(metrics.get("total_realized_pnl_proxy")) or 0.0
        records.append(
            {
                "value": value,
                "count": len(value_rows),
                "total_realized_pnl_proxy": _round(group_total),
                "average_realized_pnl_proxy": metrics.get("average_realized_pnl_proxy"),
                "median_realized_pnl_proxy": metrics.get("median_realized_pnl_proxy"),
                "trimmed_mean_5_percent": metrics.get("trimmed_mean_5_percent"),
                "win_rate": metrics.get("win_rate"),
                "percentage_of_nq_total_contribution": _rate(group_total, total),
                "missingness": _missing_field_counts(value_rows),
                "ra8_coverage": cohort_evidence(value_rows)["ra8_coverage"],
            }
        )
    return sorted(records, key=lambda item: abs(_number(item.get("total_realized_pnl_proxy")) or 0.0), reverse=True)


def nq_tail_sensitivity(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    top_1 = set(row.get("research_record_id") for row in percentile_slice(rows, 0.01, low=False))
    top_5 = set(row.get("research_record_id") for row in percentile_slice(rows, 0.05, low=False))
    top_10 = set(row.get("research_record_id") for row in percentile_slice(rows, 0.10, low=False))
    sorted_top = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0), str(row.get("research_record_id"))), reverse=True)
    largest = {sorted_top[0].get("research_record_id")} if sorted_top else set()
    views = {
        "full_qualified_nq": nq_tail_view(rows, excluded_ids=set()),
        "excluding_top_1_percent": nq_tail_view(rows, excluded_ids=top_1),
        "excluding_top_5_percent": nq_tail_view(rows, excluded_ids=top_5),
        "excluding_top_10_percent": nq_tail_view(rows, excluded_ids=top_10),
        "excluding_single_largest_winner": nq_tail_view(rows, excluded_ids=largest),
        "winsorized_top_5_percent": nq_winsorized_view(rows, top_ids=top_5),
    }
    full_total = views["full_qualified_nq"]["metrics"].get("total_realized_pnl_proxy")
    ex10_total = views["excluding_top_10_percent"]["metrics"].get("total_realized_pnl_proxy")
    primary = (
        f"NQ remains positive after excluding top 10% winners: {ex10_total} versus full total {full_total}."
        if (_number(ex10_total) or 0) > 0
        else f"NQ does not remain positive after excluding top 10% winners: {ex10_total} versus full total {full_total}."
    )
    result = {"schema_version": "inv_004_nq_tail_sensitivity_v1", **views, "primary_finding": primary, "guardrails": dict(GUARDRAILS)}
    result["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(result))
    return result


def nq_tail_view(rows: Sequence[Mapping[str, Any]], *, excluded_ids: set[Any]) -> dict[str, Any]:
    included = [row for row in rows if row.get("research_record_id") not in excluded_ids]
    return {
        "included_count": len(included),
        "excluded_count": len(rows) - len(included),
        "excluded_trade_ids": sorted(str(item) for item in excluded_ids if item),
        "metrics": investigation_metrics(included),
    }


def nq_winsorized_view(rows: Sequence[Mapping[str, Any]], *, top_ids: set[Any]) -> dict[str, Any]:
    if not rows or not top_ids:
        return nq_tail_view(rows, excluded_ids=set())
    non_top = [row for row in rows if row.get("research_record_id") not in top_ids]
    cap_values = _values(non_top, "realized_pnl_proxy")
    cap = max(cap_values) if cap_values else None
    adjusted = []
    for row in rows:
        copied = dict(row)
        if cap is not None and row.get("research_record_id") in top_ids:
            copied["realized_pnl_proxy"] = cap
        adjusted.append(copied)
    return {
        "method": "Cap top 5% realized P&L proxy values at the largest non-top-5% value.",
        "cap_value": _round(cap) if cap is not None else None,
        "adjusted_trade_count": len(top_ids),
        "metrics": investigation_metrics(adjusted),
    }


def nq_rolling_windows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    windows = {
        f"{size}_trade_windows": rolling_window_metrics(rows, window_size=size)
        for size in (25, 50, 100)
        if len(rows) >= size
    }
    summary: dict[str, Any] = {}
    for name, window_rows in windows.items():
        if not window_rows:
            continue
        weakest = min(window_rows, key=lambda item: _number(item.get("metrics", {}).get("total_realized_pnl_proxy")) or 0.0)
        strongest = max(window_rows, key=lambda item: _number(item.get("metrics", {}).get("total_realized_pnl_proxy")) or 0.0)
        summary[name] = {
            "window_count": len(window_rows),
            "weakest_window": weakest,
            "strongest_window": strongest,
            "negative_window_count": sum(1 for item in window_rows if (_number(item.get("metrics", {}).get("total_realized_pnl_proxy")) or 0.0) < 0),
        }
    return {"schema_version": "inv_004_nq_rolling_windows_v1", "windows": windows, "summary": summary, "guardrails": dict(GUARDRAILS)}


def nq_controlled_comparisons(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    side_global = nq_side_comparison(rows)
    field_controls = {
        "side_within_session": nq_side_comparisons_by_field(rows, "session"),
        "side_within_strategy": nq_side_comparisons_by_field(rows, "strategy_id"),
        "side_within_regime": nq_side_comparisons_by_field(rows, "regime"),
        "side_within_contract": nq_side_comparisons_by_field(rows, "contract"),
        "strategy_within_session": nq_group_comparisons_within(rows, group_field="session", compare_field="strategy_id"),
        "session_within_strategy": nq_group_comparisons_within(rows, group_field="strategy_id", compare_field="session"),
    }
    cell_count = sum(len(item.get("comparisons", {})) for item in field_controls.values())
    finding = f"Controlled NQ comparisons produced {cell_count} minimum-sample cells; sparse cells were excluded rather than pooled."
    return {
        "schema_version": "inv_004_nq_controlled_comparisons_v1",
        "minimum_sample": MIN_COMPARISON_CELL_SAMPLE,
        "global_long_short": side_global,
        "controls": field_controls,
        "summary": {"controlled_cell_count": cell_count, "finding": finding},
        "guardrails": dict(GUARDRAILS),
    }


def nq_side_comparison(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    long_rows = [row for row in rows if row.get("side") == "LONG"]
    short_rows = [row for row in rows if row.get("side") == "SHORT"]
    return {
        "long_count": len(long_rows),
        "short_count": len(short_rows),
        "long": investigation_metrics(long_rows),
        "short": investigation_metrics(short_rows),
        "delta_long_minus_short_average": _round((_number(investigation_metrics(long_rows).get("average_realized_pnl_proxy")) or 0.0) - (_number(investigation_metrics(short_rows).get("average_realized_pnl_proxy")) or 0.0)),
    }


def nq_side_comparisons_by_field(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(field) or "UNKNOWN"), []).append(row)
    comparisons: dict[str, Any] = {}
    excluded: dict[str, Any] = {}
    for value, value_rows in sorted(grouped.items()):
        long_rows = [row for row in value_rows if row.get("side") == "LONG"]
        short_rows = [row for row in value_rows if row.get("side") == "SHORT"]
        if len(long_rows) < MIN_COMPARISON_CELL_SAMPLE or len(short_rows) < MIN_COMPARISON_CELL_SAMPLE:
            excluded[value] = {"long_count": len(long_rows), "short_count": len(short_rows), "reason": "below_minimum_side_sample", "minimum_sample": MIN_COMPARISON_CELL_SAMPLE}
            continue
        comparisons[value] = {
            "long": investigation_metrics(long_rows),
            "short": investigation_metrics(short_rows),
            "delta_long_minus_short_average": _round((_number(investigation_metrics(long_rows).get("average_realized_pnl_proxy")) or 0.0) - (_number(investigation_metrics(short_rows).get("average_realized_pnl_proxy")) or 0.0)),
            "sample_size": len(value_rows),
        }
    return {"field": field, "comparisons": comparisons, "excluded_cells": excluded}


def nq_group_comparisons_within(rows: Sequence[Mapping[str, Any]], *, group_field: str, compare_field: str) -> dict[str, Any]:
    grouped: dict[str, dict[str, list[Mapping[str, Any]]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(group_field) or "UNKNOWN"), {}).setdefault(str(row.get(compare_field) or "UNKNOWN"), []).append(row)
    comparisons: dict[str, Any] = {}
    excluded: dict[str, Any] = {}
    for group_value, compare_groups in sorted(grouped.items()):
        eligible = {
            value: value_rows
            for value, value_rows in compare_groups.items()
            if len(value_rows) >= MIN_COMPARISON_CELL_SAMPLE
        }
        if len(eligible) < 2:
            excluded[group_value] = {
                "reason": "fewer_than_two_cells_meet_minimum_sample",
                "minimum_sample": MIN_COMPARISON_CELL_SAMPLE,
                "cell_counts": {value: len(value_rows) for value, value_rows in compare_groups.items()},
            }
            continue
        comparisons[group_value] = {
            value: {"sample_size": len(value_rows), "metrics": investigation_metrics(value_rows)}
            for value, value_rows in sorted(eligible.items())
        }
    return {"group_field": group_field, "compare_field": compare_field, "comparisons": comparisons, "excluded_cells": excluded}


def nq_top_bottom_trades(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0.0), str(row.get("research_record_id"))))
    return {
        "schema_version": "inv_004_nq_top_bottom_trades_v1",
        "top_20_winners": [nq_trade_detail(row) for row in reversed(sorted_rows[-20:])],
        "bottom_20_losers": [nq_trade_detail(row) for row in sorted_rows[:20]],
        "guardrails": dict(GUARDRAILS),
    }


def nq_trade_detail(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "research_record_id": row.get("research_record_id"),
        "canonical_trade_identifiers": {
            "source_trade_id": row.get("source_trade_id"),
            "trade_id": row.get("trade_id"),
            "lifecycle_id": row.get("lifecycle_id"),
            "con_id": row.get("con_id"),
        },
        "contract": row.get("contract"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "strategy": row.get("strategy_id"),
        "lane": row.get("lane_id"),
        "session": row.get("session"),
        "regime": row.get("regime"),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "duration": row.get("hold_seconds"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "exit_policy": row.get("exit_policy"),
        "exit_reason": row.get("exit_reason"),
        "ra7_status": row.get("path_status", {}).get("ra7"),
        "ra8_status": row.get("path_status", {}).get("ra8"),
        "attribution_status": row.get("attribution_status"),
        "source_provenance": row.get("source_provenance"),
        "eligibility_classification": row.get("research_eligibility", {}).get("classification"),
        "missing_fields": row.get("missing_fields"),
    }


def nq_breadth_classification(tail: Mapping[str, Any], concentration: Mapping[str, Any]) -> dict[str, Any]:
    total = _number(tail.get("full_qualified_nq", {}).get("metrics", {}).get("total_realized_pnl_proxy"))
    top_view_total = _number(tail.get("excluding_top_10_percent", {}).get("metrics", {}).get("total_realized_pnl_proxy"))
    if total in (None, 0) or top_view_total is None:
        classification = "INCONCLUSIVE"
        ratio = None
    else:
        ratio = _round((total - top_view_total) / total)
        if ratio <= 0.40:
            classification = "BROADLY_DISTRIBUTED"
        elif ratio <= 0.65:
            classification = "MODERATELY_CONCENTRATED"
        else:
            classification = "HIGHLY_CONCENTRATED"
    return {
        "classification": classification,
        "top_10_positive_contribution_share": ratio,
        "thresholds": {
            "BROADLY_DISTRIBUTED": "top 10% winners contribute <= 40% of total positive NQ qualified result",
            "MODERATELY_CONCENTRATED": "top 10% winners contribute > 40% and <= 65%",
            "HIGHLY_CONCENTRATED": "top 10% winners contribute > 65%",
            "INCONCLUSIVE": "insufficient or non-positive total contribution",
        },
    }


def nq_contradictory_evidence(
    rows: Sequence[Mapping[str, Any]],
    *,
    concentration: Mapping[str, Any],
    tail: Mapping[str, Any],
    rolling: Mapping[str, Any],
) -> dict[str, Any]:
    losing_strategies = [
        item
        for item in concentration.get("dimensions", {}).get("strategy", [])
        if (_number(item.get("total_realized_pnl_proxy")) or 0.0) < 0
    ][:10]
    losing_sessions = [
        item
        for item in concentration.get("dimensions", {}).get("session", [])
        if (_number(item.get("total_realized_pnl_proxy")) or 0.0) < 0
    ][:10]
    negative_windows = []
    for name, summary in rolling.get("summary", {}).items():
        if summary.get("negative_window_count", 0):
            negative_windows.append({"window_set": name, "negative_window_count": summary.get("negative_window_count"), "weakest_window": summary.get("weakest_window")})
    missingness = _missing_field_counts(rows)
    summary = []
    if negative_windows:
        summary.append("NQ has negative rolling windows despite positive aggregate qualified contribution.")
    if losing_strategies:
        summary.append("Some NQ strategies have negative total qualified P&L.")
    if losing_sessions:
        summary.append("Some NQ sessions have negative total qualified P&L.")
    if missingness.get("mfe_points") or missingness.get("mae_points") or missingness.get("ra8_finalized_capture"):
        summary.append("Sparse path/excursion evidence limits entry-versus-exit attribution.")
    summary.append("Independent contract point-value provenance is missing, so raw-dollar results are not normalized economics.")
    return {
        "schema_version": "inv_004_nq_contradictory_evidence_v1",
        "summary": summary,
        "negative_rolling_windows": negative_windows,
        "losing_strategies": losing_strategies,
        "losing_sessions": losing_sessions,
        "tail_sensitivity": {
            "excluding_top_10_percent_total": tail.get("excluding_top_10_percent", {}).get("metrics", {}).get("total_realized_pnl_proxy"),
            "excluding_single_largest_winner_total": tail.get("excluding_single_largest_winner", {}).get("metrics", {}).get("total_realized_pnl_proxy"),
        },
        "missingness": missingness,
        "guardrails": dict(GUARDRAILS),
    }


def percentile_slice(rows: Sequence[Mapping[str, Any]], fraction: float, *, low: bool) -> list[Mapping[str, Any]]:
    if not rows:
        return []
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0), str(row.get("research_record_id"))))
    count = max(1, math.ceil(len(sorted_rows) * fraction))
    return sorted_rows[:count] if low else sorted_rows[-count:]


def cohort_evidence(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    ra8_available_count = sum(1 for row in rows if ra8_available(row))
    return {
        "sample_size": len(rows),
        "metrics": investigation_metrics(rows),
        "drill_down_rows": list(rows),
        "distributions": {
            field: _distribution((row.get(field) for row in rows), limit=20)
            for field in ("instrument", "strategy_id", "lane_id", "session", "regime", "side", "exit_reason")
        },
        "ra8_coverage": {
            "available": ra8_available_count,
            "missing": len(rows) - ra8_available_count,
        },
    }


def ra8_available(row: Mapping[str, Any]) -> bool:
    path_status = row.get("path_status")
    if isinstance(path_status, Mapping) and path_status.get("ra8") == "EXACT":
        return True
    return row.get("ra8_coverage_status") == "EXACT"


def median_trade(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0), str(row.get("research_record_id"))))
    return dict(sorted_rows[len(sorted_rows) // 2])


def mean_median_trimmed(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = _values(rows, "realized_pnl_proxy")
    return {
        "sample_size": len(rows),
        "mean": _average(pnl),
        "median": _median(pnl),
        "trimmed_mean_5_percent": trimmed_mean(pnl, trim_fraction=0.05),
        "total": _round(sum(pnl)) if pnl else None,
    }


def classify_extreme_losses(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    classifications: list[dict[str, Any]] = []
    summary: dict[str, int] = {}
    for row in rows:
        quantity = _number(row.get("quantity"))
        missing = set(row.get("missing_fields", []))
        if quantity is not None and quantity > 1:
            classification = "position_size_or_contract_economics_effect"
            evidence = "CRR quantity is greater than one."
        elif {"mfe_points", "mae_points"} & missing:
            classification = "insufficient_evidence"
            evidence = "Path/excursion fields are missing, so exit-path or intra-trade behavior cannot be classified."
        else:
            classification = "ordinary_strategy_outcome"
            evidence = "No operational or sizing anomaly is visible in the CRR row."
        summary[classification] = summary.get(classification, 0) + 1
        classifications.append(
            {
                "research_record_id": row.get("research_record_id"),
                "source_trade_id": row.get("source_trade_id"),
                "classification": classification,
                "evidence": evidence,
            }
        )
    return {"summary": summary, "rows": classifications}


def build_loss_attribution(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0), str(row.get("research_record_id"))))
    cohorts = {
        "worst_20": sorted_rows[:20],
        "bottom_1_percent": percentile_slice(sorted_rows, 0.01, low=True),
        "bottom_5_percent": percentile_slice(sorted_rows, 0.05, low=True),
        "bottom_10_percent": percentile_slice(sorted_rows, 0.10, low=True),
    }
    inventory = {
        name: [extreme_loss_inventory_row(row) for row in cohort_rows]
        for name, cohort_rows in cohorts.items()
    }
    classifications = [classify_loss_trade(row) for row in sorted_rows[: max(20, len(cohorts["bottom_10_percent"]))]]
    classification_counts = _distribution((item["classification"] for item in classifications))
    unresolved_review = [item for item in classifications if item["requires_review"]]
    sensitivity = population_sensitivity_views(rows, classifications)
    total_losses = abs(sum(value for value in _values(rows, "realized_pnl_proxy") if value < 0))
    concentration = {
        name: {
            field: concentration_by_field(cohort_rows, field, total_losses=total_losses)
            for field in ("instrument", "contract", "side", "quantity", "strategy_id", "lane_id", "session", "regime", "exit_policy", "exit_reason", "calendar_month", "milestone_period")
        }
        for name, cohort_rows in cohorts.items()
    }
    supported_excluded_total = sensitivity["excluding_source_confirmed_development_or_leak_test_artifacts"]["metrics"].get("total_realized_pnl_proxy")
    primary_finding = (
        "No source-confirmed development, operational, or data-quality exclusions were sufficient to remove the aggregate negative result."
        if supported_excluded_total is not None and supported_excluded_total < 0
        else "Supported exclusions materially change the aggregate result."
    )
    return {
        "schema_version": "inv_001_loss_attribution_v1",
        "summary": {
            "classification_counts": classification_counts,
            "unresolved_review_count": len(unresolved_review),
            "primary_finding": primary_finding,
            "global_raw_dollar_disclosure": "Raw realized P&L proxy can reflect instrument, multiplier, quantity, and contract-economics differences.",
            "within_instrument_control": "Use standardized within-instrument percentile views before interpreting strategy quality.",
        },
        "classification_contract": loss_classification_contract(),
        "extreme_loss_inventory": inventory,
        "loss_concentration": concentration,
        "loss_classification": {
            "classification_counts": classification_counts,
            "classified_rows": classifications,
            "unresolved_review_queue": unresolved_review,
        },
        "population_sensitivity": sensitivity,
    }


def extreme_loss_inventory_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "research_record_id": row.get("research_record_id"),
        "canonical_trade_identifiers": {
            "source_trade_id": row.get("source_trade_id"),
            "trade_id": row.get("trade_id"),
            "lifecycle_id": row.get("lifecycle_id"),
            "con_id": row.get("con_id"),
        },
        "instrument": row.get("instrument"),
        "contract": row.get("contract"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "entry_time": row.get("entry_time"),
        "exit_time": row.get("exit_time"),
        "duration": row.get("hold_seconds"),
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id"),
        "session": row.get("session"),
        "regime": row.get("regime"),
        "exit_policy": row.get("exit_policy"),
        "exit_reason": row.get("exit_reason"),
        "broker_order_fill_refs": row.get("broker_order_fill_refs", {}),
        "ra7_status": row.get("path_status", {}).get("ra7"),
        "ra8_status": row.get("path_status", {}).get("ra8"),
        "attribution_status": row.get("attribution_status", {}),
        "source_provenance": row.get("source_provenance", []),
        "missing_fields": row.get("missing_fields", []),
        "data_quality_flags": row.get("data_quality_flags", []),
    }


def loss_classification_contract() -> dict[str, Any]:
    categories = {
        "ORDINARY_STRATEGY_OUTCOME": "Source fields support a normal completed trade with no visible sizing, data-quality, or lifecycle concern.",
        "POSITION_SIZE_OR_CONTRACT_SCALE_EFFECT": "CRR quantity or reliable contract fields show scale is a material contributor.",
        "DEVELOPMENT_OR_LEAK_TEST_ARTIFACT": "Source fields explicitly identify development, leak-test, or non-representative test provenance.",
        "OPERATIONAL_OR_LIFECYCLE_ANOMALY": "Source fields explicitly show lifecycle, ownership, duplicate, or operational anomaly evidence.",
        "PNL_PROXY_OR_DATA_QUALITY_CONCERN": "Source fields explicitly show P&L proxy, missing realized proxy, or data-quality invalidity.",
        "INSUFFICIENT_EVIDENCE": "Available source fields do not support a narrower classification.",
        "OTHER_SOURCE_BACKED_CATEGORY": "A different category is used only with explicit source-backed rationale.",
    }
    return {"default": "INSUFFICIENT_EVIDENCE", "categories": categories}


def classify_loss_trade(row: Mapping[str, Any]) -> dict[str, Any]:
    quantity = _number(row.get("quantity"))
    flags = {str(item) for item in row.get("data_quality_flags", [])}
    strategy_lane_text = " ".join(str(row.get(key) or "") for key in ("strategy_id", "lane_id")).lower()
    missing = set(row.get("missing_fields", []))
    if quantity is not None and quantity > 1:
        classification = "POSITION_SIZE_OR_CONTRACT_SCALE_EFFECT"
        reasoning = "CRR quantity is greater than one, so scale may contribute to raw P&L magnitude."
        confidence = "MEDIUM"
        requires_review = False
        supporting = {"quantity": row.get("quantity")}
    elif any(token in strategy_lane_text for token in ("leak", "test", "development", "dev")):
        classification = "DEVELOPMENT_OR_LEAK_TEST_ARTIFACT"
        reasoning = "Strategy or lane identifier explicitly contains development/test wording."
        confidence = "MEDIUM"
        requires_review = False
        supporting = {"strategy_id": row.get("strategy_id"), "lane_id": row.get("lane_id")}
    elif any("lifecycle" in flag.lower() or "duplicate" in flag.lower() or "orphan" in flag.lower() for flag in flags):
        classification = "OPERATIONAL_OR_LIFECYCLE_ANOMALY"
        reasoning = "CRR data-quality flags explicitly reference lifecycle, duplicate, or orphan evidence."
        confidence = "MEDIUM"
        requires_review = False
        supporting = {"data_quality_flags": sorted(flags)}
    elif (
        any("realized_pnl_proxy" in flag.lower() or "pnl_proxy_invalid" in flag.lower() or "invalid_pnl" in flag.lower() for flag in flags)
        or "realized_pnl_proxy" in missing
        or _number(row.get("realized_pnl_proxy")) is None
    ):
        classification = "PNL_PROXY_OR_DATA_QUALITY_CONCERN"
        reasoning = "CRR fields explicitly flag P&L/realized-value data-quality concern."
        confidence = "MEDIUM"
        requires_review = False
        supporting = {"data_quality_flags": sorted(flags), "missing_fields": sorted(missing)}
    else:
        classification = "INSUFFICIENT_EVIDENCE"
        reasoning = "No source field supports a narrower classification; loss size alone is not evidence."
        confidence = "LOW"
        requires_review = True
        supporting = {"available_fields": ["instrument", "contract", "side", "strategy_id", "lane_id", "session", "exit_policy"]}
    return {
        "research_record_id": row.get("research_record_id"),
        "source_trade_id": row.get("source_trade_id"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "classification": classification,
        "supporting_source_fields": supporting,
        "reasoning": reasoning,
        "confidence": confidence,
        "contradictory_evidence": ["No broker/runtime state was queried; classification uses CRR evidence only."],
        "automatic_or_review": "REQUIRES_REVIEW" if requires_review else "AUTOMATIC",
        "requires_review": requires_review,
    }


def concentration_by_field(rows: Sequence[Mapping[str, Any]], field: str, *, total_losses: float) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        value = period_key(row.get("exit_time"), "month") if field == "calendar_month" else milestone_label_for_row(row) if field == "milestone_period" else str(row.get(field) or "UNKNOWN")
        grouped.setdefault(value, []).append(row)
    cohort_loss = abs(sum(value for value in _values(rows, "realized_pnl_proxy") if value < 0))
    result = []
    for value, value_rows in sorted(grouped.items()):
        pnl = _values(value_rows, "realized_pnl_proxy")
        loss = abs(sum(item for item in pnl if item < 0))
        result.append(
            {
                "value": value,
                "count": len(value_rows),
                "total_pnl_proxy": _round(sum(pnl)) if pnl else None,
                "median_pnl_proxy": _median(pnl),
                "percentage_of_total_losses": _rate(loss, total_losses),
                "percentage_of_extreme_loss_cohort": _rate(len(value_rows), len(rows)),
                "missingness": _missing_field_counts(value_rows),
            }
        )
    return sorted(result, key=lambda item: (abs(_number(item.get("total_pnl_proxy")) or 0), item["count"]), reverse=True)


def milestone_label_for_row(row: Mapping[str, Any]) -> str:
    boundaries = sorted(EVIDENCE_BACKED_MILESTONES, key=lambda item: str(item["boundary_at"]))
    label = "Before " + str(boundaries[0]["label"])
    exit_time = _parse_datetime(row.get("exit_time"))
    for boundary in boundaries:
        boundary_time = _parse_datetime(boundary["boundary_at"])
        if exit_time is not None and boundary_time is not None and exit_time >= boundary_time:
            label = "After " + str(boundary["label"])
    return label


def population_sensitivity_views(rows: Sequence[Mapping[str, Any]], classifications: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    class_by_id = {item.get("research_record_id"): item.get("classification") for item in classifications}
    definitions = {
        "full_historical_population": set(),
        "excluding_source_confirmed_development_or_leak_test_artifacts": {"DEVELOPMENT_OR_LEAK_TEST_ARTIFACT"},
        "excluding_source_confirmed_operational_or_lifecycle_anomalies": {"OPERATIONAL_OR_LIFECYCLE_ANOMALY"},
        "excluding_source_confirmed_pnl_or_data_quality_invalid_records": {"PNL_PROXY_OR_DATA_QUALITY_CONCERN"},
        "ordinary_strategy_population_if_sufficient": {"DEVELOPMENT_OR_LEAK_TEST_ARTIFACT", "OPERATIONAL_OR_LIFECYCLE_ANOMALY", "PNL_PROXY_OR_DATA_QUALITY_CONCERN", "POSITION_SIZE_OR_CONTRACT_SCALE_EFFECT"},
    }
    views = {}
    for name, excluded_classes in definitions.items():
        included = []
        excluded_counts: dict[str, int] = {}
        for row in rows:
            classification = class_by_id.get(row.get("research_record_id"))
            if classification in excluded_classes:
                excluded_counts[str(classification)] = excluded_counts.get(str(classification), 0) + 1
            else:
                included.append(row)
        views[name] = {
            "included_count": len(included),
            "excluded_count": len(rows) - len(included),
            "excluded_count_by_classification": excluded_counts,
            "metrics": investigation_metrics(included),
            "long_short_result": {
                "long": investigation_metrics([row for row in included if row.get("side") == "LONG"]),
                "short": investigation_metrics([row for row in included if row.get("side") == "SHORT"]),
            },
            "instrument_mix": _distribution((row.get("instrument") for row in included)),
            "strategy_mix": _distribution((row.get("strategy_id") for row in included), limit=20),
            "session_mix": _distribution((row.get("session") for row in included)),
        }
    return views


def render_loss_attribution_html(investigation: Mapping[str, Any]) -> str:
    attribution = investigation.get("loss_attribution", {})
    inventory = attribution.get("extreme_loss_inventory", {}).get("worst_20", [])
    classification = attribution.get("loss_classification", {})
    sensitivity = attribution.get("population_sensitivity", {})
    concentration = attribution.get("loss_concentration", {}).get("worst_20", {})
    rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('instrument')))}</td>"
        f"<td>{html.escape(str(row.get('contract')))}</td>"
        f"<td>{html.escape(str(row.get('side')))}</td>"
        f"<td>{html.escape(str(row.get('quantity')))}</td>"
        f"<td>{_format_money(row.get('realized_pnl_proxy'))}</td>"
        f"<td>{html.escape(_display_label(row.get('session')))}</td>"
        f"<td>{html.escape(str(row.get('strategy_id')))}</td>"
        f"<td>{html.escape(_display_label(row.get('ra8_status')))}</td>"
        "</tr>"
        for row in inventory
    )
    class_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('classification')))}</td>"
        f"<td>{html.escape(str(row.get('confidence')))}</td>"
        f"<td>{html.escape(str(row.get('automatic_or_review')))}</td>"
        f"<td>{html.escape(str(row.get('reasoning')))}</td>"
        "</tr>"
        for row in classification.get("classified_rows", [])
    )
    sensitivity_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_display_label(name))}</td>"
        f"<td>{view.get('included_count')}</td>"
        f"<td>{view.get('excluded_count')}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('total_realized_pnl_proxy'))}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('average_realized_pnl_proxy'))}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('median_realized_pnl_proxy'))}</td>"
        "</tr>"
        for name, view in sensitivity.items()
    )
    concentration_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_display_label(field))}</td>"
        f"<td>{html.escape(str(items[0].get('value')) if items else 'NONE')}</td>"
        f"<td>{items[0].get('count') if items else 0}</td>"
        f"<td>{_format_money(items[0].get('total_pnl_proxy')) if items else 'MISSING'}</td>"
        "</tr>"
        for field, items in concentration.items()
    )
    unresolved = len(classification.get("unresolved_review_queue", []))
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>INV-001 Loss Attribution</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f6f7f4;color:#1f2933}}main{{max-width:1240px;margin:auto}}section{{background:white;border:1px solid #d9e0df;border-radius:6px;margin:16px 0;padding:16px}}table{{width:100%;border-collapse:collapse;font-size:13px}}th,td{{border-bottom:1px solid #e7ecea;padding:7px;text-align:left;vertical-align:top}}th{{background:#eef2ef}}.warn{{color:#8a5a00;font-weight:700}}</style>
</head><body><main>
<h1>INV-001 Loss Attribution</h1>
<p class="warn">Descriptive, non-causal, no production authority. Unresolved review queue: {unresolved}</p>
<section><h2>Worst 20 Trades</h2><table><thead><tr><th>Research Record</th><th>Instrument</th><th>Contract</th><th>Side</th><th>Qty</th><th>P&L Proxy</th><th>Session</th><th>Strategy</th><th>RA8</th></tr></thead><tbody>{rows}</tbody></table></section>
<section><h2>Top Concentrations In Worst 20</h2><table><thead><tr><th>Field</th><th>Largest Group</th><th>Count</th><th>Total P&L Proxy</th></tr></thead><tbody>{concentration_rows}</tbody></table></section>
<section><h2>Classification</h2><table><thead><tr><th>Research Record</th><th>Classification</th><th>Confidence</th><th>Status</th><th>Reasoning</th></tr></thead><tbody>{class_rows}</tbody></table></section>
<section><h2>Population Sensitivity</h2><table><thead><tr><th>View</th><th>Included</th><th>Excluded</th><th>Total P&L</th><th>Average</th><th>Median</th></tr></thead><tbody>{sensitivity_rows}</tbody></table></section>
</main></body></html>
"""


def build_extreme_trade_forensic_audit(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    sorted_rows = sorted(rows, key=lambda row: (float(row.get("realized_pnl_proxy") or 0), str(row.get("research_record_id"))))
    worst_20 = sorted_rows[:20]
    full_exec_index = execution_evidence_index(rows)
    reconciliations = [reconcile_extreme_trade_pnl(row) for row in worst_20]
    lineage_rows = [execution_lineage_for_row(row, full_exec_index=full_exec_index) for row in worst_20]
    classifications = [
        classify_extreme_trade_forensics(row, reconciliation=reconciliation, lineage=lineage)
        for row, reconciliation, lineage in zip(worst_20, reconciliations, lineage_rows, strict=True)
    ]
    population_impact = extreme_trade_population_impact(rows, classifications)
    classification_counts = _distribution(item["classification"] for item in classifications)
    unresolved = [
        item
        for item in classifications
        if item["classification"] in {"SOURCE_EVIDENCE_INCOMPLETE", "PNL_PROXY_UNRECONCILED"}
    ]
    price_scale = [
        item
        for item in classifications
        if item["classification"] == "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH"
    ]
    source_confirmed = [
        item
        for item in classifications
        if item["classification"] in {"CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH", "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE"}
    ]
    primary_finding = (
        f"Extreme-loss forensic audit found {len(price_scale)} source-backed price-scale anomalies, "
        f"{len(source_confirmed) - len(price_scale)} duplicate/reused execution-evidence anomalies, and "
        f"{len(unresolved)} unresolved records among the worst 20; no records were removed from the population."
    )
    source_trace = anomaly_source_trace(records=source_confirmed, reconciliations=reconciliations, lineage_rows=lineage_rows)
    root_cause = anomaly_root_cause_report(source_trace)
    repair_plan = anomaly_repair_plan(root_cause)
    before_after = anomaly_before_after_report(population_impact)
    audit = {
        "schema_version": "inv_001_extreme_trade_forensic_audit_v1",
        "scope": {
            "description": "Worst 20 CRR trades by realized P&L proxy, covering the extreme GC/NQ losses and one ES comparison control.",
            "trade_count": len(worst_20),
            "instrument_distribution": _distribution(row.get("instrument") for row in worst_20),
            "record_selection": "ascending_realized_pnl_proxy_worst_20",
        },
        "summary": {
            "primary_finding": primary_finding,
            "classification_counts": classification_counts,
            "unresolved_count": len(unresolved),
            "source_supported_exclusion_count": len(
                [
                    item
                    for item in classifications
                    if item["classification"]
                    in {
                        "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH",
                        "QUANTITY_OR_FILL_AGGREGATION_MISMATCH",
                        "TRADE_PAIRING_OR_POSITION_CYCLE_ANOMALY",
                        "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE",
                        "DEVELOPMENT_OR_LEAK_TEST_CONFIRMED",
                        "OPERATIONAL_OR_LIFECYCLE_ANOMALY_CONFIRMED",
                        "PNL_PROXY_UNRECONCILED",
                    }
                ]
            ),
            "guardrails": dict(GUARDRAILS),
        },
        "pnl_reconciliation": {
            "schema_version": "inv_001_extreme_trade_pnl_reconciliation_v1",
            "records": reconciliations,
        },
        "execution_lineage": {
            "schema_version": "inv_001_extreme_trade_execution_lineage_v1",
            "records": lineage_rows,
            "duplicate_execution_summary": duplicate_execution_summary(full_exec_index),
        },
        "classification": {
            "schema_version": "inv_001_extreme_trade_classification_v1",
            "classification_contract": extreme_trade_classification_contract(),
            "classification_counts": classification_counts,
            "records": classifications,
            "unresolved_review_queue": [item for item in classifications if item.get("requires_review")],
        },
        "population_impact": population_impact,
        "anomaly_source_trace": source_trace,
        "anomaly_root_cause": root_cause,
        "anomaly_repair_plan": repair_plan,
        "anomaly_before_after": before_after,
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    audit["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(audit))
    return audit


def attach_eligibility_excluded_anomalies(
    forensic_audit: Mapping[str, Any],
    source_confirmed_anomalies: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    audit = dict(forensic_audit)
    anomalies = [dict(item) for item in source_confirmed_anomalies]
    if not anomalies:
        return audit

    trace_records = [eligibility_anomaly_trace_record(item) for item in anomalies]
    source_trace = dict(audit.get("anomaly_source_trace", {}))
    existing_trace_records = list(source_trace.get("records", []))
    existing_ids = {str(item.get("research_record_id")) for item in existing_trace_records}
    merged_trace_records = existing_trace_records + [
        item for item in trace_records if str(item.get("research_record_id")) not in existing_ids
    ]
    source_trace.update(
        {
            "records": merged_trace_records,
            "eligibility_excluded_record_count": len(anomalies),
            "eligibility_excluded_records": anomalies,
            "active_population_note": "These records are excluded from SOURCE_INTEGRITY_QUALIFIED analysis but retained here as source-confirmed historical anomaly evidence.",
        }
    )
    source_trace["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(source_trace))

    classification = dict(audit.get("classification", {}))
    classification.update(
        {
            "source_confirmed_excluded_record_count": len(anomalies),
            "source_confirmed_excluded_records": anomalies,
        }
    )
    classification["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(classification))

    root_cause = dict(audit.get("anomaly_root_cause", {}))
    root_summary = dict(root_cause.get("summary", {}))
    root_summary["source_confirmed_excluded_record_count"] = len(anomalies)
    root_cause.update(
        {
            "summary": root_summary,
            "source_confirmed_excluded_records": anomalies,
            "active_population_note": "Root-cause evidence is preserved from the eligibility layer because the active investigation population excludes these records.",
        }
    )
    root_cause["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(root_cause))

    audit.update(
        {
            "eligibility_excluded_source_confirmed_anomalies": anomalies,
            "eligibility_excluded_source_confirmed_anomaly_count": len(anomalies),
            "anomaly_source_trace": source_trace,
            "classification": classification,
            "anomaly_root_cause": root_cause,
        }
    )
    summary = dict(audit.get("summary", {}))
    summary["eligibility_excluded_source_confirmed_anomaly_count"] = len(anomalies)
    audit["summary"] = summary
    audit["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(audit))
    return audit


def eligibility_anomaly_trace_record(anomaly: Mapping[str, Any]) -> dict[str, Any]:
    supporting_ids = dict(anomaly.get("supporting_ids") or {})
    evidence_basis = list(anomaly.get("evidence_basis") or [])
    root_classification = next(
        (
            str(item).split(" as ", 1)[1].rstrip(".")
            for item in evidence_basis
            if "INV-001 classified this record as " in str(item)
        ),
        "SOURCE_CONFIRMED_ANOMALY",
    )
    return {
        "research_record_id": anomaly.get("research_record_id"),
        "source_trade_id": anomaly.get("source_trade_id") or supporting_ids.get("source_trade_id"),
        "classification": root_classification,
        "eligibility_classification": anomaly.get("classification"),
        "selection_status": "EXCLUDED_FROM_SOURCE_INTEGRITY_QUALIFIED_POPULATION",
        "first_defective_layer": "entry_fill_persistence",
        "supporting_ids": supporting_ids,
        "supporting_artifact_paths": list(anomaly.get("supporting_artifact_paths") or []),
        "evidence_basis": evidence_basis,
        "confidence": anomaly.get("confidence"),
        "instrument": anomaly.get("instrument"),
        "side": anomaly.get("side"),
        "realized_pnl_proxy": anomaly.get("realized_pnl_proxy"),
    }


def anomaly_source_trace(
    *,
    records: Sequence[Mapping[str, Any]],
    reconciliations: Sequence[Mapping[str, Any]],
    lineage_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    reconciliation_by_id = {item.get("research_record_id"): item for item in reconciliations}
    lineage_by_id = {item.get("research_record_id"): item for item in lineage_rows}
    traces = []
    for record in records:
        record_id = record.get("research_record_id")
        reconciliation = reconciliation_by_id.get(record_id, {})
        lineage = lineage_by_id.get(record_id, {})
        source_paths = record.get("supporting_artifact_paths", [])
        canonical_path = next((path for path in source_paths if "canonical_trade_records.jsonl" in str(path)), None)
        entry_source = "outputs/probationary_pattern_engine/paper_session/filled_bridge_results.jsonl"
        exit_source = "outputs/track_b_execution_core/trade_registry/live_trade_events.jsonl"
        if record.get("classification") == "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH":
            first_defective_layer = "entry_fill_persistence"
            defect = "price_domain_discontinuity_present_before_canonical_trade_construction"
            first_defective_evidence = {
                "entry_price": reconciliation.get("entry_price"),
                "exit_price": reconciliation.get("exit_price"),
                "price_scale_ratio": reconciliation.get("price_scale_ratio"),
                "entry_exec_id": record.get("supporting_ids", {}).get("entry_exec_id"),
            }
            tests = {
                "decimal_scaling_mismatch": "SUPPORTED_BY_PRICE_SCALE_RATIO" if reconciliation.get("price_scale_discontinuity") else "NOT_SUPPORTED",
                "raw_vs_normalized_price_domain_mismatch": "SUPPORTED",
                "contract_series_mapping_mismatch": "NOT_SUPPORTED_BY_CONTRACT_FIELDS",
                "front_month_or_reference_symbol_contamination": "NARROWED_NOT_PROVEN",
                "price_field_substitution": "NARROWED_NOT_PROVEN",
                "string_number_conversion_error": "NOT_SUPPORTED",
                "stale_or_legacy_schema_interpretation": "NARROWED_NOT_PROVEN",
                "cross_instrument_entry_exit_pairing": "NOT_PROVEN_IN_CANONICAL_PAIRING; ENTRY_FILL_PRICE_ALREADY_WRONG_OR_FOREIGN_DOMAIN",
            }
        else:
            first_defective_layer = "entry_fill_persistence"
            defect = "entry_exec_id_reused_across_distinct_lifecycle_ids"
            first_defective_evidence = {
                "entry_exec_id": record.get("supporting_ids", {}).get("entry_exec_id"),
                "duplicate_evidence": lineage.get("duplicate_evidence", {}),
                "lifecycle_id": record.get("supporting_ids", {}).get("lifecycle_id"),
            }
            tests = {
                "one_exec_id_attached_to_multiple_canonical_trades": "SUPPORTED",
                "duplicated_canonical_record_generation": "NOT_PROVEN; DUPLICATE_EXEC_PRESENT_IN_ENTRY_FILL_SOURCE",
                "faulty_position_cycle_boundary": "NARROWED_NOT_PROVEN",
                "scale_in_or_partial_exit_reuse": "SOURCE_EVIDENCE_INCOMPLETE",
                "reversal_mispairing": "NOT_SUPPORTED_BY_ENTRY_EXIT_SIDE_FIELDS",
                "lifecycle_ownership_duplication": "NARROWED_NOT_PROVEN",
                "stale_deduplication_key": "SUPPORTED_AS_MISSING_INVARIANT",
                "source_row_copied_across_records": "NARROWED_NOT_PROVEN",
            }
        traces.append(
            {
                "research_record_id": record_id,
                "source_trade_id": record.get("source_trade_id"),
                "classification": record.get("classification"),
                "instrument": record.get("instrument"),
                "first_defective_layer": first_defective_layer,
                "defect": defect,
                "trace_layers": [
                    {
                        "layer": "original_entry_fill_persistence",
                        "artifact_path": entry_source,
                        "record_id": record.get("supporting_ids", {}).get("source_trade_id"),
                        "exec_id": record.get("supporting_ids", {}).get("entry_exec_id"),
                        "contract_identifier": {
                            "contract": reconciliation.get("contract"),
                            "con_id": reconciliation.get("con_id"),
                            "instrument": reconciliation.get("instrument"),
                        },
                        "price": reconciliation.get("entry_price"),
                    },
                    {
                        "layer": "original_exit_fill_or_close_event",
                        "artifact_path": exit_source,
                        "record_id": record.get("supporting_ids", {}).get("source_trade_id"),
                        "exec_id": record.get("supporting_ids", {}).get("exit_exec_id"),
                        "price": reconciliation.get("exit_price"),
                    },
                    {
                        "layer": "canonical_trade_construction",
                        "artifact_path": canonical_path,
                        "record_id": record.get("supporting_ids", {}).get("canonical_trade_id"),
                        "transformation": "entry fields copied from filled_bridge_results; exit selected by contract-scoped risk-reducing matcher",
                    },
                    {
                        "layer": "ctol_pnl_calculation",
                        "artifact_path": reconciliation.get("pnl_source_artifact"),
                        "record_id": reconciliation.get("pnl_source_record_id"),
                        "transformation": "P&L derived from canonical entry/exit prices, side, quantity, and multiplier.",
                    },
                    {
                        "layer": "crr_materialization",
                        "artifact_path": "outputs/track_b_execution_core/research_analytics/canonical_research_record/canonical_research_records.jsonl",
                        "record_id": record_id,
                        "transformation": "CRR materialized exact-source cache values and provenance references.",
                    },
                    {
                        "layer": "investigation_output",
                        "artifact_path": "outputs/track_b_execution_core/research_analytics/investigations/INV-001/",
                        "record_id": record_id,
                        "transformation": "INV-001 surfaced source-confirmed anomaly without excluding or rewriting the trade.",
                    },
                ],
                "identity_consistency": {
                    "instrument": reconciliation.get("instrument"),
                    "contract": reconciliation.get("contract"),
                    "con_id": reconciliation.get("con_id"),
                    "source_trade_id": record.get("supporting_ids", {}).get("source_trade_id"),
                    "lifecycle_id": record.get("supporting_ids", {}).get("lifecycle_id"),
                    "entry_exec_id": record.get("supporting_ids", {}).get("entry_exec_id"),
                    "exit_exec_id": record.get("supporting_ids", {}).get("exit_exec_id"),
                    "consistent_through_ctol_and_crr": True,
                },
                "first_defective_evidence": first_defective_evidence,
                "hypothesis_tests": tests,
                "source_provenance": record.get("supporting_artifact_paths", []),
            }
        )
    payload = {
        "schema_version": "inv_001_anomaly_source_trace_v1",
        "records": traces,
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def anomaly_root_cause_report(source_trace: Mapping[str, Any]) -> dict[str, Any]:
    records = []
    for trace in source_trace.get("records", []):
        classification = trace.get("classification")
        if classification == "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH":
            root = "ROOT_CAUSE_NARROWED"
            defect_type = "TRANSFORMATION_DEFECT"
            repair_scope = "generalized"
            confidence = "HIGH"
            summary = "Entry-fill persistence accepted a price-domain discontinuity before canonical trade construction."
        else:
            root = "ROOT_CAUSE_NARROWED"
            defect_type = "IDENTITY_OR_DEDUP_DEFECT"
            repair_scope = "generalized"
            confidence = "HIGH"
            summary = "Entry-fill persistence allowed the same broker exec ID to attach to multiple lifecycle/source trade IDs."
        records.append(
            {
                "research_record_id": trace.get("research_record_id"),
                "source_trade_id": trace.get("source_trade_id"),
                "instrument": trace.get("instrument"),
                "root_cause_classification": root,
                "defect_classification": defect_type,
                "first_proven_defective_layer": trace.get("first_defective_layer"),
                "summary": summary,
                "supporting_evidence": trace.get("first_defective_evidence"),
                "contradictory_evidence": [
                    "Canonical trade construction preserves instrument/contract/con_id identity in the affected rows.",
                    "CTOL and CRR propagate the canonical fields rather than introducing a new cross-instrument join.",
                    "The audit did not query broker state or alter source artifacts.",
                ],
                "confidence": confidence,
                "repair_is_local_or_generalized": repair_scope,
            }
        )
    counts = _distribution(item["defect_classification"] for item in records)
    payload = {
        "schema_version": "inv_001_anomaly_root_cause_v1",
        "summary": {
            "record_count": len(records),
            "defect_counts": counts,
            "first_defective_layer": "entry_fill_persistence",
            "conclusion": "The leading cross-instrument hypothesis is narrowed to bad or foreign-domain entry-fill evidence entering durable persistence, not a proven CTOL/CRR pairing defect.",
        },
        "records": records,
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def anomaly_repair_plan(root_cause: Mapping[str, Any]) -> dict[str, Any]:
    payload = {
        "schema_version": "inv_001_anomaly_repair_plan_v1",
        "status": "ROUTINE_TRACK_REPAIR_RECOMMENDED_NOT_IMPLEMENTED",
        "reason_not_implemented": "The repair touches canonical trade construction/source validation semantics and should be done as a separate bounded Routine Track change with regeneration validation.",
        "recommended_generalized_invariants": [
            {
                "invariant": "contract_aware_price_domain_validation",
                "description": "Before accepting or materializing an entry/exit fill, validate price against the instrument/contract domain and quarantine impossible discontinuities.",
                "target_boundary": "filled_bridge_result persistence and canonical trade construction",
            },
            {
                "invariant": "entry_exit_identity_key_must_include_contract",
                "description": "All source lookup and pairing keys must include stable instrument, local_symbol/contract, con_id, lifecycle_id/source_trade_id, and fill role.",
                "target_boundary": "entry/exit pairing and source lookup",
            },
            {
                "invariant": "broker_exec_id_uniqueness_or_explicit_partial_fill_semantics",
                "description": "A broker exec ID may not create multiple canonical trades unless an explicit partial-fill/scale-in record links the rows deterministically.",
                "target_boundary": "entry dedupe and canonical trade record validation",
            },
            {
                "invariant": "impossible_price_discontinuity_quarantine",
                "description": "Trades with source-backed impossible price-domain breaks are preserved but marked research-invalid until repaired from authoritative fill evidence.",
                "target_boundary": "canonical trade validation and CTOL/CRR readiness",
            },
        ],
        "proposed_tests": [
            "GC and NQ entries with adjacent timestamps cannot cross-link exit evidence.",
            "A GC entry cannot accept an NQ-domain price without explicit source evidence.",
            "An NQ entry cannot accept a GC-domain price without explicit source evidence.",
            "Duplicate exec IDs across lifecycle IDs are quarantined unless explicit partial-fill semantics are present.",
            "CTOL/CRR preserve invalid-source flags instead of silently computing ordinary P&L.",
        ],
        "implementation_boundary": "Do not patch the five rows. Add generalized validation/quarantine at source-persistence/canonical-construction boundaries, then regenerate derived artifacts.",
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def anomaly_before_after_report(population_impact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "inv_001_anomaly_before_after_v1",
        "repair_implemented": False,
        "before": population_impact.get("views", {}).get("full_population", {}),
        "source_supported_sensitivity_only": population_impact.get("views", {}).get("excluding_only_source_confirmed_data_anomalies", {}),
        "after": None,
        "note": "No source records were rewritten and no derived artifacts were regenerated under repaired canonical semantics in this slice.",
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }


def reconcile_extreme_trade_pnl(row: Mapping[str, Any]) -> dict[str, Any]:
    entry_price = _number(row.get("entry_price"))
    exit_price = _number(row.get("exit_price"))
    quantity = _number(row.get("quantity"))
    emitted_pnl = _number(row.get("realized_pnl_proxy"))
    emitted_points = _number(row.get("realized_points"))
    direction_sign = 1 if row.get("side") == "LONG" else -1 if row.get("side") == "SHORT" else None
    price_difference = _round(exit_price - entry_price) if entry_price is not None and exit_price is not None else None
    directed_points = _round(price_difference * direction_sign) if price_difference is not None and direction_sign is not None else None
    point_difference = _round((emitted_points or 0) - (directed_points or 0)) if emitted_points is not None and directed_points is not None else None
    point_reconciled = bool(point_difference is not None and abs(point_difference) <= 0.000001)
    point_value_from_emitted_points = (
        _round(emitted_pnl / (emitted_points * quantity))
        if emitted_pnl is not None and emitted_points not in (None, 0) and quantity not in (None, 0)
        else None
    )
    arithmetic_pnl_from_emitted_points = (
        _round(emitted_points * quantity * point_value_from_emitted_points)
        if emitted_points is not None and quantity is not None and point_value_from_emitted_points is not None
        else None
    )
    arithmetic_difference = (
        _round((arithmetic_pnl_from_emitted_points or 0) - emitted_pnl)
        if arithmetic_pnl_from_emitted_points is not None and emitted_pnl is not None
        else None
    )
    pnl_reconciled = bool(arithmetic_difference is not None and abs(arithmetic_difference) <= 0.01)
    ratio = (
        _round(max(abs(entry_price), abs(exit_price)) / min(abs(entry_price), abs(exit_price)))
        if entry_price not in (None, 0) and exit_price not in (None, 0)
        else None
    )
    price_scale_discontinuity = bool(ratio is not None and ratio >= 3.0)
    return {
        "research_record_id": row.get("research_record_id"),
        "canonical_trade_id": row.get("trade_id"),
        "source_trade_id": row.get("source_trade_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "instrument": row.get("instrument"),
        "contract": row.get("contract"),
        "con_id": row.get("con_id"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "entry_timestamp": row.get("entry_time"),
        "exit_timestamp": row.get("exit_time"),
        "entry_price": row.get("entry_price"),
        "exit_price": row.get("exit_price"),
        "price_difference": price_difference,
        "direction_sign": direction_sign,
        "directed_points_from_prices": directed_points,
        "emitted_realized_points": emitted_points,
        "realized_points_reconciliation_difference": point_difference,
        "realized_points_reconciled_to_prices": point_reconciled,
        "contract_multiplier_or_point_value_used": point_value_from_emitted_points,
        "contract_economics_source": "IMPLIED_FROM_EMITTED_PNL_PROXY_AND_REALIZED_POINTS" if point_value_from_emitted_points is not None else "SOURCE_EVIDENCE_ABSENT",
        "commissions_included": "UNKNOWN",
        "expected_arithmetic_pnl_from_available_fields": arithmetic_pnl_from_emitted_points,
        "emitted_pnl_proxy": emitted_pnl,
        "pnl_reconciliation_difference": arithmetic_difference,
        "pnl_proxy_reconciled_to_available_fields": pnl_reconciled,
        "price_scale_ratio": ratio,
        "price_scale_discontinuity": price_scale_discontinuity,
        "pnl_source_artifact": row.get("pnl_source_artifact"),
        "pnl_source_record_id": row.get("pnl_source_record_id"),
        "calculation_inputs": {
            "entry_price": "CRR entry_anchor from canonical_trade_records",
            "exit_price": "CRR exit_anchor from canonical_trade_records",
            "quantity": "CRR trade_identity from canonical_trade_records",
            "realized_points": "CRR outcome_summary from CTOL",
            "realized_pnl_proxy": "CRR outcome_summary from CTOL",
        },
        "source_provenance": row.get("source_provenance", []),
    }


def execution_evidence_index(rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, list[str]]]:
    index: dict[str, dict[str, list[str]]] = {
        "entry_exec_id": {},
        "exit_exec_id": {},
        "entry_order_id": {},
        "exit_order_id": {},
        "source_trade_id": {},
        "lifecycle_id": {},
    }
    for row in rows:
        record_id = str(row.get("research_record_id"))
        for field in index:
            value = row.get(field)
            if value in (None, ""):
                continue
            index[field].setdefault(str(value), []).append(record_id)
    return index


def execution_lineage_for_row(row: Mapping[str, Any], *, full_exec_index: Mapping[str, Mapping[str, Sequence[str]]]) -> dict[str, Any]:
    duplicate_fields: dict[str, list[str]] = {}
    for field in ("entry_exec_id", "exit_exec_id", "source_trade_id", "lifecycle_id"):
        value = row.get(field)
        matches = list(full_exec_index.get(field, {}).get(str(value), [])) if value not in (None, "") else []
        if len(matches) > 1:
            duplicate_fields[field] = matches
    quantity = _number(row.get("quantity"))
    entry_time = _parse_datetime(row.get("entry_time"))
    exit_time = _parse_datetime(row.get("exit_time"))
    pairing_flags = []
    if entry_time is None or exit_time is None:
        pairing_flags.append("missing_entry_or_exit_timestamp")
    elif exit_time <= entry_time:
        pairing_flags.append("exit_not_after_entry")
    if quantity is None or quantity <= 0:
        pairing_flags.append("missing_or_nonpositive_quantity")
    if row.get("entry_exec_id") in (None, "") or row.get("exit_exec_id") in (None, ""):
        pairing_flags.append("missing_entry_or_exit_exec_id")
    return {
        "research_record_id": row.get("research_record_id"),
        "canonical_trade_id": row.get("trade_id"),
        "source_trade_id": row.get("source_trade_id"),
        "lifecycle_id": row.get("lifecycle_id"),
        "instrument": row.get("instrument"),
        "contract": row.get("contract"),
        "con_id": row.get("con_id"),
        "side": row.get("side"),
        "quantity": row.get("quantity"),
        "entry_order_id": row.get("entry_order_id"),
        "entry_perm_id": row.get("entry_perm_id"),
        "entry_exec_id": row.get("entry_exec_id"),
        "exit_order_id": row.get("exit_order_id"),
        "exit_perm_id": row.get("exit_perm_id"),
        "exit_exec_id": row.get("exit_exec_id"),
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id"),
        "session": row.get("session"),
        "exit_reason": row.get("exit_reason"),
        "duplicate_evidence": duplicate_fields,
        "pairing_flags": pairing_flags,
        "source_provenance": row.get("source_provenance", []),
    }


def duplicate_execution_summary(index: Mapping[str, Mapping[str, Sequence[str]]]) -> dict[str, Any]:
    return {
        field: {
            "duplicate_key_count": len([records for records in values.values() if len(records) > 1]),
            "max_records_per_key": max([len(records) for records in values.values()] or [0]),
        }
        for field, values in index.items()
    }


def extreme_trade_classification_contract() -> dict[str, Any]:
    return {
        "PNL_RECONCILED_ECONOMICALLY_PLAUSIBLE": "Prices, direction, quantity, and sourced contract economics reconcile to emitted P&L.",
        "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH": "Source fields show price scale discontinuity or sourced economics cannot explain P&L scale.",
        "QUANTITY_OR_FILL_AGGREGATION_MISMATCH": "Source quantity or partial-fill evidence conflicts with one-trade aggregation.",
        "TRADE_PAIRING_OR_POSITION_CYCLE_ANOMALY": "Entry/exit pairing evidence is missing, reversed, cross-contract, or lifecycle-inconsistent.",
        "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE": "One broker execution is linked to multiple canonical research trades.",
        "DEVELOPMENT_OR_LEAK_TEST_CONFIRMED": "Source provenance explicitly identifies test/leak/development artifact status.",
        "OPERATIONAL_OR_LIFECYCLE_ANOMALY_CONFIRMED": "Source fields explicitly identify operational or lifecycle anomaly evidence.",
        "SOURCE_EVIDENCE_INCOMPLETE": "Available sources do not provide enough evidence for a narrower source-backed classification.",
        "PNL_PROXY_UNRECONCILED": "Available price, point, quantity, and emitted P&L fields do not reconcile.",
    }


def classify_extreme_trade_forensics(
    row: Mapping[str, Any],
    *,
    reconciliation: Mapping[str, Any],
    lineage: Mapping[str, Any],
) -> dict[str, Any]:
    flags = {str(item).lower() for item in row.get("data_quality_flags", [])}
    strategy_lane_text = " ".join(str(row.get(key) or "") for key in ("strategy_id", "lane_id")).lower()
    if not reconciliation.get("pnl_proxy_reconciled_to_available_fields") or not reconciliation.get("realized_points_reconciled_to_prices"):
        classification = "PNL_PROXY_UNRECONCILED"
        confidence = "HIGH"
        reasoning = "Available CRR/CTOL price, point, quantity, and P&L fields do not reconcile arithmetically."
        review = True
    elif reconciliation.get("price_scale_discontinuity"):
        classification = "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH"
        confidence = "HIGH"
        reasoning = "Entry and exit prices for the same contract have a source-backed scale discontinuity."
        review = True
    elif lineage.get("duplicate_evidence"):
        classification = "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE"
        confidence = "HIGH"
        reasoning = "Execution identifiers are reused by more than one CRR row."
        review = True
    elif lineage.get("pairing_flags"):
        classification = "TRADE_PAIRING_OR_POSITION_CYCLE_ANOMALY"
        confidence = "HIGH"
        reasoning = "Entry/exit timestamps, quantity, or execution anchors fail deterministic lineage checks."
        review = True
    elif any(token in strategy_lane_text for token in ("leak", "test", "development", "dev")):
        classification = "DEVELOPMENT_OR_LEAK_TEST_CONFIRMED"
        confidence = "MEDIUM"
        reasoning = "Strategy or lane identifier explicitly contains development/test wording."
        review = True
    elif any(token in " ".join(flags) for token in ("lifecycle", "orphan", "duplicate", "ownership")):
        classification = "OPERATIONAL_OR_LIFECYCLE_ANOMALY_CONFIRMED"
        confidence = "MEDIUM"
        reasoning = "Data-quality flags explicitly identify lifecycle or operational anomaly evidence."
        review = True
    elif reconciliation.get("contract_economics_source") == "IMPLIED_FROM_EMITTED_PNL_PROXY_AND_REALIZED_POINTS":
        classification = "SOURCE_EVIDENCE_INCOMPLETE"
        confidence = "LOW"
        reasoning = "Arithmetic is internally consistent, but no independent contract multiplier/point-value source is present."
        review = True
    else:
        classification = "SOURCE_EVIDENCE_INCOMPLETE"
        confidence = "LOW"
        reasoning = "Available source fields do not support a narrower classification."
        review = True
    result = {
        "research_record_id": row.get("research_record_id"),
        "source_trade_id": row.get("source_trade_id"),
        "instrument": row.get("instrument"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "classification": classification,
        "reasoning": reasoning,
        "confidence": confidence,
        "requires_review": review,
        "supporting_artifact_paths": sorted(
            {
                str(item.get("source_artifact_path"))
                for item in row.get("source_provenance", [])
                if item.get("source_artifact_path")
            }
        ),
        "supporting_ids": {
            "canonical_trade_id": row.get("trade_id"),
            "source_trade_id": row.get("source_trade_id"),
            "lifecycle_id": row.get("lifecycle_id"),
            "entry_exec_id": row.get("entry_exec_id"),
            "exit_exec_id": row.get("exit_exec_id"),
            "pnl_source_record_id": row.get("pnl_source_record_id"),
        },
        "arithmetic": {
            "entry_price": reconciliation.get("entry_price"),
            "exit_price": reconciliation.get("exit_price"),
            "direction_sign": reconciliation.get("direction_sign"),
            "quantity": reconciliation.get("quantity"),
            "directed_points_from_prices": reconciliation.get("directed_points_from_prices"),
            "emitted_realized_points": reconciliation.get("emitted_realized_points"),
            "contract_multiplier_or_point_value_used": reconciliation.get("contract_multiplier_or_point_value_used"),
            "expected_arithmetic_pnl_from_available_fields": reconciliation.get("expected_arithmetic_pnl_from_available_fields"),
            "emitted_pnl_proxy": reconciliation.get("emitted_pnl_proxy"),
            "pnl_reconciliation_difference": reconciliation.get("pnl_reconciliation_difference"),
        },
        "contradictory_evidence": [
            "No live broker state was queried.",
            "No source record was excluded or rewritten.",
        ],
    }
    result["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(result))
    return result


def extreme_trade_population_impact(
    rows: Sequence[Mapping[str, Any]],
    classifications: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    class_by_id = {item.get("research_record_id"): item.get("classification") for item in classifications}
    views = {
        "full_population": set(),
        "excluding_only_pnl_unreconciled_records": {"PNL_PROXY_UNRECONCILED"},
        "excluding_only_source_confirmed_data_anomalies": {
            "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH",
            "QUANTITY_OR_FILL_AGGREGATION_MISMATCH",
            "TRADE_PAIRING_OR_POSITION_CYCLE_ANOMALY",
            "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE",
            "DEVELOPMENT_OR_LEAK_TEST_CONFIRMED",
            "OPERATIONAL_OR_LIFECYCLE_ANOMALY_CONFIRMED",
        },
        "economically_reconciled_trades_only": {"INCLUDE_ONLY:PNL_RECONCILED_ECONOMICALLY_PLAUSIBLE"},
    }
    impact: dict[str, Any] = {"schema_version": "inv_001_extreme_trade_population_impact_v1", "views": {}}
    for name, excluded in views.items():
        if name == "economically_reconciled_trades_only":
            included = [row for row in rows if class_by_id.get(row.get("research_record_id")) == "PNL_RECONCILED_ECONOMICALLY_PLAUSIBLE"]
            excluded_count = len(rows) - len(included)
            excluded_counts = {"not_economically_reconciled_or_not_in_forensic_scope": excluded_count}
        else:
            included = [row for row in rows if class_by_id.get(row.get("research_record_id")) not in excluded]
            excluded_count = len(rows) - len(included)
            excluded_counts: dict[str, int] = {}
            for row in rows:
                classification = class_by_id.get(row.get("research_record_id"))
                if classification in excluded:
                    excluded_counts[str(classification)] = excluded_counts.get(str(classification), 0) + 1
        impact["views"][name] = {
            "included_count": len(included),
            "excluded_count": excluded_count,
            "excluded_count_by_classification": excluded_counts,
            "label_caveat": "This is a source-supported sensitivity view, not a clean population claim.",
            "metrics": investigation_metrics(included),
            "long_short_outcomes": {
                "long": investigation_metrics([row for row in included if row.get("side") == "LONG"]),
                "short": investigation_metrics([row for row in included if row.get("side") == "SHORT"]),
            },
            "instrument_contribution": {
                instrument: investigation_metrics([row for row in included if row.get("instrument") == instrument])
                for instrument in sorted({str(row.get("instrument") or "UNKNOWN") for row in included})
            },
        }
    return impact


def validate_extreme_trade_forensic_audit(audit: Mapping[str, Any]) -> dict[str, Any]:
    blockers = []
    warnings = []
    if audit.get("diagnostic_only") is not True or audit.get("production_recommendation") is not False or audit.get("trading_gate") is not False:
        blockers.append("guardrails_invalid")
    scope_count = audit.get("scope", {}).get("trade_count")
    records = audit.get("classification", {}).get("records", [])
    if scope_count != len(records):
        blockers.append("classification_count_mismatch")
    if audit.get("summary", {}).get("unresolved_count"):
        warnings.append("unresolved_records_require_review")
    if audit.get("summary", {}).get("source_supported_exclusion_count"):
        warnings.append("source_supported_exclusion_sensitivity_present")
    validation = {
        "schema_version": "inv_001_extreme_trade_forensic_validation_v1",
        "status": "INVALID" if blockers else "VALID_WITH_WARNINGS" if warnings else "VALID",
        "blockers": blockers,
        "warnings": warnings,
        "scope_trade_count": scope_count,
        "classification_counts": audit.get("summary", {}).get("classification_counts", {}),
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    validation["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(validation))
    return validation


def render_forensic_validation_markdown(validation: Mapping[str, Any]) -> str:
    return (
        "# INV-001 Forensic Validation\n\n"
        f"- Status: `{validation.get('status')}`\n"
        f"- Scope trades: `{validation.get('scope_trade_count')}`\n"
        f"- Blockers: `{len(validation.get('blockers', []))}`\n"
        f"- Warnings: `{len(validation.get('warnings', []))}`\n"
    )


def render_extreme_trade_forensic_html(investigation: Mapping[str, Any]) -> str:
    audit = investigation.get("extreme_trade_forensic_audit", {})
    reconciliation_rows = audit.get("pnl_reconciliation", {}).get("records", [])
    classification_rows = audit.get("classification", {}).get("records", [])
    impact_views = audit.get("population_impact", {}).get("views", {})
    rec_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('instrument')))}</td>"
        f"<td>{html.escape(str(row.get('contract')))}</td>"
        f"<td>{html.escape(str(row.get('side')))}</td>"
        f"<td>{html.escape(str(row.get('entry_price')))}</td>"
        f"<td>{html.escape(str(row.get('exit_price')))}</td>"
        f"<td>{html.escape(str(row.get('emitted_realized_points')))}</td>"
        f"<td>{html.escape(str(row.get('contract_multiplier_or_point_value_used')))}</td>"
        f"<td>{_format_money(row.get('emitted_pnl_proxy'))}</td>"
        f"<td>{html.escape(str(row.get('price_scale_discontinuity')))}</td>"
        "</tr>"
        for row in reconciliation_rows
    )
    class_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('classification')))}</td>"
        f"<td>{html.escape(str(row.get('confidence')))}</td>"
        f"<td>{html.escape(str(row.get('reasoning')))}</td>"
        "</tr>"
        for row in classification_rows
    )
    impact_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_display_label(name))}</td>"
        f"<td>{view.get('included_count')}</td>"
        f"<td>{view.get('excluded_count')}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('total_realized_pnl_proxy'))}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('average_realized_pnl_proxy'))}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('median_realized_pnl_proxy'))}</td>"
        "</tr>"
        for name, view in impact_views.items()
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>INV-001 Extreme Trade Forensic Review</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f8f8f4;color:#1f2933}}main{{max-width:1280px;margin:auto}}section{{background:white;border:1px solid #d9e0df;border-radius:6px;margin:16px 0;padding:16px}}table{{width:100%;border-collapse:collapse;font-size:13px}}th,td{{border-bottom:1px solid #e7ecea;padding:7px;text-align:left;vertical-align:top}}th{{background:#eef2ef}}.warn{{color:#8a5a00;font-weight:700}}</style>
</head><body><main>
<h1>INV-001 Extreme Trade Forensic Review</h1>
<p class="warn">Descriptive forensic audit only. No exclusions, production guidance, broker access, or trading authority.</p>
<section><h2>P&L Reconciliation</h2><table><thead><tr><th>Research Record</th><th>Instrument</th><th>Contract</th><th>Side</th><th>Entry</th><th>Exit</th><th>Points</th><th>Point Value Used</th><th>P&L Proxy</th><th>Scale Break</th></tr></thead><tbody>{rec_rows}</tbody></table></section>
<section><h2>Classification</h2><table><thead><tr><th>Research Record</th><th>Classification</th><th>Confidence</th><th>Reasoning</th></tr></thead><tbody>{class_rows}</tbody></table></section>
<section><h2>Population Impact</h2><table><thead><tr><th>View</th><th>Included</th><th>Excluded</th><th>Total P&L</th><th>Average</th><th>Median</th></tr></thead><tbody>{impact_rows}</tbody></table></section>
</main></body></html>
"""


def render_anomaly_root_cause_html(investigation: Mapping[str, Any]) -> str:
    audit = investigation.get("extreme_trade_forensic_audit", {})
    source_trace = audit.get("anomaly_source_trace", {})
    root_cause = audit.get("anomaly_root_cause", {})
    repair_plan = audit.get("anomaly_repair_plan", {})
    traces = source_trace.get("records", [])
    causes = {row.get("research_record_id"): row for row in root_cause.get("records", [])}
    trace_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('instrument')))}</td>"
        f"<td>{html.escape(str(row.get('classification')))}</td>"
        f"<td>{html.escape(str(row.get('first_defective_layer')))}</td>"
        f"<td>{html.escape(str(row.get('defect')))}</td>"
        f"<td>{html.escape(str(row.get('identity_consistency', {}).get('contract')))}</td>"
        f"<td>{html.escape(str(row.get('identity_consistency', {}).get('con_id')))}</td>"
        f"<td>{html.escape(str(row.get('identity_consistency', {}).get('entry_exec_id')))}</td>"
        f"<td>{html.escape(str(row.get('identity_consistency', {}).get('exit_exec_id')))}</td>"
        f"<td>{html.escape(str(row.get('hypothesis_tests', {}).get('cross_instrument_entry_exit_pairing')))}</td>"
        "</tr>"
        for row in traces
    )
    cause_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('defect_classification')))}</td>"
        f"<td>{html.escape(str(row.get('root_cause_classification')))}</td>"
        f"<td>{html.escape(str(row.get('confidence')))}</td>"
        f"<td>{html.escape(str(row.get('summary')))}</td>"
        "</tr>"
        for row in causes.values()
    )
    invariant_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(item.get('invariant')))}</td>"
        f"<td>{html.escape(str(item.get('target_boundary')))}</td>"
        f"<td>{html.escape(str(item.get('description')))}</td>"
        "</tr>"
        for item in repair_plan.get("recommended_generalized_invariants", [])
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>INV-001 Anomaly Root Cause Review</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f8f8f4;color:#1f2933}}main{{max-width:1320px;margin:auto}}section{{background:white;border:1px solid #d9e0df;border-radius:6px;margin:16px 0;padding:16px}}table{{width:100%;border-collapse:collapse;font-size:13px}}th,td{{border-bottom:1px solid #e7ecea;padding:7px;text-align:left;vertical-align:top}}th{{background:#eef2ef}}.warn{{color:#8a5a00;font-weight:700}}</style>
</head><body><main>
<h1>INV-001 Anomaly Root Cause Review</h1>
<p class="warn">Research audit only. The report narrows the first defective durable layer without rewriting records or changing production authority.</p>
<section><h2>Source Trace</h2><table><thead><tr><th>Research Record</th><th>Instrument</th><th>Classification</th><th>First Defective Layer</th><th>Defect</th><th>Contract</th><th>Con ID</th><th>Entry Exec</th><th>Exit Exec</th><th>Cross-Instrument Pairing Test</th></tr></thead><tbody>{trace_rows}</tbody></table></section>
<section><h2>Root Cause Classification</h2><table><thead><tr><th>Research Record</th><th>Defect Type</th><th>Root Cause</th><th>Confidence</th><th>Summary</th></tr></thead><tbody>{cause_rows}</tbody></table></section>
<section><h2>Generalized Repair Plan</h2><p>Status: {html.escape(str(repair_plan.get('status')))}</p><table><thead><tr><th>Invariant</th><th>Boundary</th><th>Description</th></tr></thead><tbody>{invariant_rows}</tbody></table></section>
</main></body></html>
"""


def render_inv_004_html(investigation: Mapping[str, Any]) -> str:
    summary = investigation.get("performance_summary", {})
    metrics = summary.get("metrics", {})
    concentration = investigation.get("concentration_by_dimension", {}).get("dimensions", {})
    tail = investigation.get("tail_sensitivity", {})
    controlled = investigation.get("controlled_comparisons", {})
    contradictory = investigation.get("contradictory_evidence_detail", {})
    top_bottom = investigation.get("top_bottom_trades", {})

    def concentration_rows(name: str) -> str:
        return "".join(
            "<tr>"
            f"<td>{html.escape(str(item.get('value')))}</td>"
            f"<td>{item.get('count')}</td>"
            f"<td>{_format_money(item.get('total_realized_pnl_proxy'))}</td>"
            f"<td>{_format_money(item.get('average_realized_pnl_proxy'))}</td>"
            f"<td>{html.escape(str(item.get('win_rate')))}</td>"
            f"<td>{html.escape(str(item.get('percentage_of_nq_total_contribution')))}</td>"
            "</tr>"
            for item in concentration.get(name, [])[:12]
        )

    tail_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_display_label(name))}</td>"
        f"<td>{view.get('included_count')}</td>"
        f"<td>{view.get('excluded_count')}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('total_realized_pnl_proxy'))}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('average_realized_pnl_proxy'))}</td>"
        f"<td>{_format_money(view.get('metrics', {}).get('median_realized_pnl_proxy'))}</td>"
        "</tr>"
        for name, view in tail.items()
        if isinstance(view, Mapping) and "metrics" in view
    )
    trade_rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(row.get('research_record_id')))}</td>"
        f"<td>{html.escape(str(row.get('side')))}</td>"
        f"<td>{_format_money(row.get('realized_pnl_proxy'))}</td>"
        f"<td>{html.escape(str(row.get('strategy')))}</td>"
        f"<td>{html.escape(str(row.get('session')))}</td>"
        f"<td>{html.escape(str(row.get('exit_reason')))}</td>"
        "</tr>"
        for row in top_bottom.get("top_20_winners", [])[:10]
    )
    contrary_rows = "".join(f"<li>{html.escape(str(item))}</li>" for item in contradictory.get("summary", []))
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>INV-004 NQ Qualified Performance Attribution</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f6f7f4;color:#1f2933}}main{{max-width:1240px;margin:auto}}section{{background:white;border:1px solid #d9e0df;border-radius:6px;margin:16px 0;padding:16px}}table{{width:100%;border-collapse:collapse;font-size:13px}}th,td{{border-bottom:1px solid #e7ecea;padding:7px;text-align:left;vertical-align:top}}th{{background:#eef2ef}}.warn{{color:#8a5a00;font-weight:700}}</style>
</head><body><main>
<h1>INV-004 NQ Qualified Performance Attribution</h1>
<p class="warn">Descriptive, non-causal, source-integrity-qualified research only. No production recommendation or trading authority.</p>
<section><h2>Summary</h2><p>Trades: {summary.get('trade_count')} | Total P&L proxy: {_format_money(metrics.get('total_realized_pnl_proxy'))} | Average: {_format_money(metrics.get('average_realized_pnl_proxy'))} | Median: {_format_money(metrics.get('median_realized_pnl_proxy'))} | Win rate: {metrics.get('win_rate')}</p></section>
<section><h2>Tail Sensitivity</h2><p>{html.escape(str(tail.get('primary_finding')))}</p><table><thead><tr><th>View</th><th>Included</th><th>Excluded</th><th>Total P&L</th><th>Average</th><th>Median</th></tr></thead><tbody>{tail_rows}</tbody></table></section>
<section><h2>Top Strategy Contributors</h2><table><thead><tr><th>Strategy</th><th>Count</th><th>Total</th><th>Average</th><th>Win Rate</th><th>% NQ Total</th></tr></thead><tbody>{concentration_rows('strategy')}</tbody></table></section>
<section><h2>Session Contributors</h2><table><thead><tr><th>Session</th><th>Count</th><th>Total</th><th>Average</th><th>Win Rate</th><th>% NQ Total</th></tr></thead><tbody>{concentration_rows('session')}</tbody></table></section>
<section><h2>Controlled Comparisons</h2><p>{html.escape(str(controlled.get('summary', {}).get('finding')))}</p></section>
<section><h2>Strongest Contradictory Evidence</h2><ul>{contrary_rows}</ul></section>
<section><h2>Top 10 Winners</h2><table><thead><tr><th>Research Record</th><th>Side</th><th>P&L</th><th>Strategy</th><th>Session</th><th>Exit Reason</th></tr></thead><tbody>{trade_rows}</tbody></table></section>
</main></body></html>
"""


def side_comparisons_by_field(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(field) or "UNKNOWN"), []).append(row)
    comparisons: dict[str, Any] = {}
    excluded: dict[str, Any] = {}
    for value, value_rows in sorted(grouped.items()):
        long_rows = [row for row in value_rows if row.get("side") == "LONG"]
        short_rows = [row for row in value_rows if row.get("side") == "SHORT"]
        if len(long_rows) < MIN_COMPARISON_CELL_SAMPLE or len(short_rows) < MIN_COMPARISON_CELL_SAMPLE:
            excluded[value] = {
                "long_count": len(long_rows),
                "short_count": len(short_rows),
                "reason": "below_minimum_side_sample",
                "minimum_sample": MIN_COMPARISON_CELL_SAMPLE,
            }
            continue
        long_metrics = investigation_metrics(long_rows)
        short_metrics = investigation_metrics(short_rows)
        comparisons[value] = {
            "long": long_metrics,
            "short": short_metrics,
            "delta_long_minus_short_average": _round((_number(long_metrics.get("average_realized_pnl_proxy")) or 0) - (_number(short_metrics.get("average_realized_pnl_proxy")) or 0)),
            "sample_size": len(value_rows),
        }
    return {"field": field, "comparisons": comparisons, "excluded_cells": excluded}


def side_comparisons_by_period(rows: Sequence[Mapping[str, Any]], period: str) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        key = period_key(row.get("exit_time"), period)
        grouped.setdefault(key, []).append(row)
    comparisons: dict[str, Any] = {}
    excluded: dict[str, Any] = {}
    for value, value_rows in sorted(grouped.items()):
        long_rows = [row for row in value_rows if row.get("side") == "LONG"]
        short_rows = [row for row in value_rows if row.get("side") == "SHORT"]
        if len(long_rows) < MIN_COMPARISON_CELL_SAMPLE or len(short_rows) < MIN_COMPARISON_CELL_SAMPLE:
            excluded[value] = {
                "long_count": len(long_rows),
                "short_count": len(short_rows),
                "reason": "below_minimum_side_sample",
                "minimum_sample": MIN_COMPARISON_CELL_SAMPLE,
            }
            continue
        comparisons[value] = {
            "long": investigation_metrics(long_rows),
            "short": investigation_metrics(short_rows),
            "sample_size": len(value_rows),
        }
    return {"field": f"calendar_{period}", "comparisons": comparisons, "excluded_cells": excluded}


def side_control_summary(groups: Mapping[str, Any]) -> dict[str, Any]:
    cells = []
    excluded: dict[str, Any] = {}
    long_worse = 0
    short_worse = 0
    for group_name, group in groups.items():
        excluded[group_name] = group.get("excluded_cells", {})
        for value, comparison in group.get("comparisons", {}).items():
            long_avg = _number(comparison.get("long", {}).get("average_realized_pnl_proxy"))
            short_avg = _number(comparison.get("short", {}).get("average_realized_pnl_proxy"))
            if long_avg is None or short_avg is None:
                continue
            delta = long_avg - short_avg
            cells.append({"group": group_name, "value": value, "long_minus_short_average": _round(delta)})
            if delta < 0:
                long_worse += 1
            elif delta > 0:
                short_worse += 1
    if not cells:
        status = "INCONCLUSIVE"
        confidence = "LOW"
        finding = "No controlled side-comparison cells met the minimum sample threshold."
    elif long_worse >= short_worse + 3:
        status = "PARTIALLY_SUPPORTED"
        confidence = "PARTIAL"
        finding = f"Long underperformance persists in {long_worse} controlled cells versus {short_worse} cells where short underperforms."
    else:
        status = "INCONCLUSIVE"
        confidence = "PARTIAL"
        finding = f"Controlled cells are mixed: {long_worse} long-worse cells and {short_worse} short-worse cells."
    return {
        "controlled_cell_count": len(cells),
        "long_worse_cell_count": long_worse,
        "short_worse_cell_count": short_worse,
        "cells": cells,
        "excluded_sparse_cells": excluded,
        "contradictory_evidence": [
            "Global long/short averages can be distorted by instrument, strategy, and period composition.",
            "Several controlled cells are sparse and excluded rather than pooled.",
        ],
        "finding": finding,
        "confidence": confidence,
        "conclusion_status": status,
    }


def period_metrics(rows: Sequence[Mapping[str, Any]], period: str, *, min_sample: int = 1) -> dict[str, Any]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(period_key(row.get("exit_time"), period), []).append(row)
    included: dict[str, Any] = {}
    excluded: dict[str, Any] = {}
    for key, period_rows in sorted(grouped.items()):
        if len(period_rows) < min_sample:
            excluded[key] = {"sample_size": len(period_rows), "reason": "below_minimum_sample", "minimum_sample": min_sample}
            continue
        included[key] = {
            "sample_size": len(period_rows),
            "metrics": investigation_metrics(period_rows),
            "instrument_mix": _distribution((row.get("instrument") for row in period_rows)),
            "strategy_mix": _distribution((row.get("strategy_id") for row in period_rows), limit=20),
            "side_mix": _distribution((row.get("side") for row in period_rows)),
            "extreme_loss_concentration": cohort_evidence(percentile_slice(period_rows, 0.05, low=True)).get("metrics"),
        }
    return {"period": period, "included": included, "excluded": excluded}


def rolling_window_metrics(rows: Sequence[Mapping[str, Any]], *, window_size: int = 100) -> list[dict[str, Any]]:
    sorted_rows = sorted(rows, key=lambda row: (str(row.get("exit_time") or ""), str(row.get("research_record_id"))))
    windows: list[dict[str, Any]] = []
    if not sorted_rows:
        return windows
    for start in range(0, len(sorted_rows), window_size):
        window = sorted_rows[start : start + window_size]
        if len(window) < max(20, window_size // 2):
            continue
        windows.append(
            {
                "window_index": len(windows) + 1,
                "start_exit_time": window[0].get("exit_time"),
                "end_exit_time": window[-1].get("exit_time"),
                "sample_size": len(window),
                "metrics": investigation_metrics(window),
                "instrument_mix": _distribution((row.get("instrument") for row in window)),
                "strategy_mix": _distribution((row.get("strategy_id") for row in window), limit=20),
            }
        )
    return windows


def milestone_period_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    boundaries = sorted(EVIDENCE_BACKED_MILESTONES, key=lambda item: str(item["boundary_at"]))
    periods: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        label = "Before " + str(boundaries[0]["label"])
        exit_time = _parse_datetime(row.get("exit_time"))
        for boundary in boundaries:
            boundary_time = _parse_datetime(boundary["boundary_at"])
            if exit_time is not None and boundary_time is not None and exit_time >= boundary_time:
                label = "After " + str(boundary["label"])
        periods.setdefault(label, []).append(row)
    return {
        "boundaries": list(boundaries),
        "periods": {
            label: {
                "sample_size": len(period_rows),
                "metrics": investigation_metrics(period_rows),
                "instrument_mix": _distribution((row.get("instrument") for row in period_rows)),
                "side_mix": _distribution((row.get("side") for row in period_rows)),
                "ra8_coverage": cohort_evidence(period_rows)["ra8_coverage"],
            }
            for label, period_rows in sorted(periods.items())
        },
        "causality_disclosure": "Milestone periods are evidence-backed labels only and do not imply the milestone caused performance changes.",
    }


def period_key(value: Any, period: str) -> str:
    parsed = _parse_datetime(value)
    if parsed is None:
        return "UNKNOWN"
    if period == "month":
        return parsed.strftime("%Y-%m")
    if period == "week":
        year, week, _ = parsed.isocalendar()
        return f"{year}-W{week:02d}"
    return parsed.date().isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def validate_investigation_record(investigation: Mapping[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    if investigation.get("diagnostic_only") is not True or investigation.get("production_recommendation") is not False or investigation.get("trading_gate") is not False:
        blockers.append("guardrails_invalid")
    if investigation.get("conclusion_status") not in {"SUPPORTED", "PARTIALLY_SUPPORTED", "UNSUPPORTED", "INCONCLUSIVE", "SUPERSEDED"}:
        blockers.append("invalid_conclusion_status")
    if not investigation.get("source_fingerprints"):
        blockers.append("missing_source_fingerprints")
    if not investigation.get("evidence"):
        blockers.append("missing_evidence")
    if investigation.get("limitations"):
        warnings.append("limitations_present")
    status = "INVALID" if blockers else "VALID_WITH_WARNINGS" if warnings else "VALID"
    validation = {
        "schema_version": INVESTIGATION_VALIDATION_SCHEMA_VERSION,
        "generated_at": investigation.get("generated_at"),
        "investigation_id": investigation.get("investigation_id"),
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    validation["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(validation))
    return validation


def build_investigation_index(
    investigations: Mapping[str, Mapping[str, Any]],
    *,
    generated_at: datetime,
    output_dir: Path,
) -> dict[str, Any]:
    index = {
        "schema_version": f"{INVESTIGATION_SCHEMA_VERSION}_index",
        "generated_at": generated_at.isoformat(),
        "output_dir": str(output_dir),
        "investigations": [
            {
                "investigation_id": investigation.get("investigation_id"),
                "title": investigation.get("title"),
                "question": investigation.get("question"),
                "conclusion_status": investigation.get("conclusion_status"),
                "confidence": investigation.get("confidence"),
                "active_population_view": investigation.get("active_population_view"),
                "source_confirmed_anomaly_count": len(investigation.get("source_confirmed_anomalies", [])),
                "review_required_count": investigation.get("review_required_count"),
                "fingerprint": investigation.get("deterministic_fingerprint"),
                "primary_artifact": str(output_dir / str(investigation.get("investigation_id")) / "investigation.json"),
            }
            for investigation in investigations.values()
        ],
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    index["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(index))
    return index


def durable_investigation_summary_filename(investigation_id: str) -> str:
    return {
        "INV-001": "INV-001-extreme-loss-concentration.md",
        "INV-002": "INV-002-long-short-underperformance.md",
        "INV-003": "INV-003-performance-over-time.md",
        "INV-004": "INV-004-nq-qualified-performance-attribution.md",
    }.get(investigation_id, f"{investigation_id}.md")


def render_investigation_markdown(investigation: Mapping[str, Any]) -> str:
    lines = [
        f"# {investigation.get('investigation_id')}: {investigation.get('title')}",
        "",
        f"- Status: `{investigation.get('status')}`",
        f"- Conclusion status: `{investigation.get('conclusion_status')}`",
        f"- Confidence: `{investigation.get('confidence')}`",
        f"- Question: {investigation.get('question')}",
        f"- Active population view: `{investigation.get('active_population_view')}`",
        f"- Source-confirmed anomalies: `{len(investigation.get('source_confirmed_anomalies', []))}`",
        f"- Review-required records: `{investigation.get('review_required_count')}`",
        "",
        "## Findings",
        "",
    ]
    lines.extend(f"- {item}" for item in investigation.get("findings", []))
    lines.extend(["", "## Contradictory Evidence", ""])
    lines.extend(f"- {item}" for item in investigation.get("contradictory_evidence", []))
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in investigation.get("limitations", []))
    lines.extend(["", "## Follow-Up Candidates", ""])
    lines.extend(f"- {item}" for item in investigation.get("follow_up_candidates", []))
    lines.extend(["", "## Source Artifacts", ""])
    for name, path in investigation.get("source_artifacts", {}).items():
        lines.append(f"- `{name}`: `{path}`")
    return "\n".join(lines) + "\n"


def render_investigation_validation_markdown(validation: Mapping[str, Any]) -> str:
    return (
        "# Investigation Validation\n\n"
        f"- Investigation: `{validation.get('investigation_id')}`\n"
        f"- Status: `{validation.get('status')}`\n"
        f"- Blockers: `{len(validation.get('blockers', []))}`\n"
        f"- Warnings: `{len(validation.get('warnings', []))}`\n"
    )


def render_durable_investigation_summary(investigation: Mapping[str, Any], paths: Mapping[str, Path]) -> str:
    lines = [
        f"# {investigation.get('investigation_id')}: {investigation.get('title')}",
        "",
        "Status: Draft",
        "",
        "## Purpose",
        "",
        str(investigation.get("question")),
        "",
        "## Current Conclusion",
        "",
        f"`{investigation.get('conclusion_status')}` with `{investigation.get('confidence')}` confidence.",
        "",
        f"Active population view: `{investigation.get('active_population_view')}`.",
        "",
        f"Source-confirmed anomalies surfaced for comparison: `{len(investigation.get('source_confirmed_anomalies', []))}`.",
        "",
        f"Review-required records: `{investigation.get('review_required_count')}`.",
        "",
        "These findings are descriptive, non-causal, and carry no production authority.",
        "",
        "## Generated Evidence",
        "",
    ]
    for name, path in paths.items():
        if name == "durable_summary":
            continue
        lines.append(f"- `{name}`: `{path}`")
    lines.extend(["", "## Findings", ""])
    lines.extend(f"- {item}" for item in investigation.get("findings", []))
    lines.extend(["", "## Limitations", ""])
    lines.extend(f"- {item}" for item in investigation.get("limitations", []))
    lines.extend(["", "## Guardrails", "", "- `diagnostic_only=true`", "- `production_recommendation=false`", "- `trading_gate=false`"])
    return "\n".join(lines) + "\n"


def render_investigation_index_markdown(index: Mapping[str, Any]) -> str:
    lines = [
        "# Research Investigation Index",
        "",
        "|investigation|status|confidence|population view|source-confirmed anomalies|review required|question|",
        "|---|---|---|---|---|---|---|",
    ]
    for item in index.get("investigations", []):
        lines.append(
            f"|{item.get('investigation_id')}|{item.get('conclusion_status')}|{item.get('confidence')}|"
            f"{item.get('active_population_view')}|{item.get('source_confirmed_anomaly_count')}|"
            f"{item.get('review_required_count')}|{item.get('question')}|"
        )
    return "\n".join(lines) + "\n"


def render_investigation_index_html(index: Mapping[str, Any]) -> str:
    rows = "".join(
        "<tr>"
        f"<td>{html.escape(str(item.get('investigation_id')))}</td>"
        f"<td>{html.escape(str(item.get('title')))}</td>"
        f"<td>{html.escape(str(item.get('conclusion_status')))}</td>"
        f"<td>{html.escape(str(item.get('confidence')))}</td>"
        f"<td>{html.escape(str(item.get('active_population_view')))}</td>"
        f"<td>{html.escape(str(item.get('source_confirmed_anomaly_count')))}</td>"
        f"<td>{html.escape(str(item.get('review_required_count')))}</td>"
        f"<td>{html.escape(str(item.get('question')))}</td>"
        "</tr>"
        for item in index.get("investigations", [])
    )
    return f"""<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>Research Investigation Index</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f6f7f4;color:#1f2933}}main{{max-width:1100px;margin:auto}}table{{width:100%;border-collapse:collapse;background:white}}th,td{{border-bottom:1px solid #d9e0df;padding:8px;text-align:left}}th{{background:#eef2ef}}</style>
</head>
<body><main><h1>Research Investigation Index</h1><p>Prepared research records only. No runtime, broker, strategy, or trading authority. Source-confirmed anomaly counts are disclosed separately from the active qualified population.</p><table><thead><tr><th>ID</th><th>Title</th><th>Status</th><th>Confidence</th><th>Population View</th><th>Source-Confirmed Anomalies</th><th>Review Required</th><th>Question</th></tr></thead><tbody>{rows}</tbody></table></main></body>
</html>
"""
def validate_research_evidence_explorer(analysis: Mapping[str, Any]) -> dict[str, Any]:
    warnings = list(analysis.get("warnings", []))
    blockers: list[str] = []
    if analysis.get("diagnostic_only") is not True or analysis.get("production_recommendation") is not False or analysis.get("trading_gate") is not False:
        blockers.append("guardrails_invalid")
    if analysis.get("source", {}).get("crr_validation_status") not in {"VALID", "VALID_WITH_WARNINGS"}:
        blockers.append("crr_validation_not_research_valid")
    if analysis.get("cohorts", {}).get("top_decile", {}).get("trade_count", 0) == 0:
        blockers.append("top_decile_empty")
    if analysis.get("cohorts", {}).get("bottom_decile", {}).get("trade_count", 0) == 0:
        blockers.append("bottom_decile_empty")
    status = "INVALID" if blockers else "VALID_WITH_WARNINGS" if warnings else "VALID"
    validation = {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "generated_at": analysis.get("generated_at"),
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "artifact_fingerprint": analysis.get("deterministic_fingerprint"),
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    validation["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(validation))
    return validation


def data_quality_warnings(population: Mapping[str, Any], crr_validation: Mapping[str, Any]) -> list[str]:
    warnings: list[str] = []
    if crr_validation.get("status") == "VALID_WITH_WARNINGS":
        warnings.append("source_crr_valid_with_warnings")
    coverage = population.get("coverage", {})
    if coverage.get("ra8_missing_count", 0):
        warnings.append("ra8_finalized_path_coverage_partial")
    if population.get("excluded_count", 0):
        warnings.append("population_exclusions_present")
    if population.get("source_confirmed_anomaly_count", 0):
        warnings.append("source_confirmed_anomalies_excluded_from_active_qualified_view")
    if population.get("review_required_count", 0):
        warnings.append("review_required_records_present")
    return warnings


def cohort_definitions(population_count: int) -> dict[str, Any]:
    boundary_count = max(1, math.ceil(population_count * 0.10)) if population_count else 0
    return {
        "top_decile": {"field": "realized_pnl_proxy", "direction": "highest", "boundary_count": boundary_count, "include_ties": True},
        "bottom_decile": {"field": "realized_pnl_proxy", "direction": "lowest", "boundary_count": boundary_count, "include_ties": True},
        "winners": {"field": "realized_pnl_proxy", "condition": "> 0"},
        "losers": {"field": "realized_pnl_proxy", "condition": "<= 0"},
        "long": {"field": "side", "condition": "LONG"},
        "short": {"field": "side", "condition": "SHORT"},
    }


def metric_definitions() -> dict[str, str]:
    return {
        "realized_pnl_proxy": "CTOL-derived realized P&L proxy materialized through CRR outcome_summary.",
        "duration": "CTOL-derived hold_seconds materialized through CRR outcome_summary.",
        "mfe_mae": "MFE/MAE where available through CTOL/RA7 path propagation; missing values remain missing.",
        "giveback": "MFE minus realized points when both values are available.",
        "ra8_coverage": "Exact finalized RA8 capture join status preserved from CRR join quality.",
        "within_instrument_pnl_percentile": "Realized P&L proxy ranked within instrument; used as a comparability-controlled view without inventing multiplier normalization.",
    }


def research_evidence_explorer_schema() -> dict[str, Any]:
    return {
        "title": "ResearchEvidenceExplorerV1",
        "schema_version": SCHEMA_VERSION,
        "source_contract": "Canonical Research Record v1",
        "required_sections": ["source", "population", "cohorts", "comparability_controlled", "deltas", "distributions", "trade_drill_down", "guardrails"],
        "guardrails": dict(GUARDRAILS),
    }


def render_analysis_markdown(analysis: Mapping[str, Any]) -> str:
    population = analysis.get("population", {})
    cohorts = analysis.get("cohorts", {})
    lines = [
        "# Research Evidence Explorer v1",
        "",
        f"- Question: {analysis.get('question')}",
        f"- CRR validation: `{analysis.get('source', {}).get('crr_validation_status')}`",
        f"- Active population view: `{population.get('active_population_view')}`",
        f"- Included trades: `{population.get('included_count')}`",
        f"- Excluded trades: `{population.get('excluded_count')}`",
        f"- Source-confirmed anomalies: `{population.get('source_confirmed_anomaly_count')}`",
        f"- Review-required records: `{population.get('review_required_count')}`",
        f"- Exit coverage: `{population.get('date_coverage', {}).get('first_exit_time')}` to `{population.get('date_coverage', {}).get('last_exit_time')}`",
        f"- RA8 coverage: `{population.get('coverage', {}).get('ra8_exact_count')}/{population.get('included_count')}`",
        f"- Comparability-controlled view: `{analysis.get('comparability_controlled', {}).get('method')}`",
        "",
        "## Comparability Disclosure",
        "",
        "The global top-versus-bottom view is portfolio-outcome analysis. Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences, so it is not automatically a strategy-quality comparison.",
        "",
        "## Population Views",
        "",
        "|view|included|excluded|total pnl|average pnl|median pnl|trimmed mean|win rate|RA8 exact|",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for view_id, view in analysis.get("population_views", {}).items():
        metrics = view.get("metrics", {})
        ra8 = view.get("ra8_coverage", {})
        lines.append(
            f"|{view_id}|{view.get('included_count')}|{view.get('excluded_count')}|{metrics.get('total_realized_pnl_proxy')}|"
            f"{metrics.get('average_realized_pnl_proxy')}|{metrics.get('median_realized_pnl_proxy')}|{metrics.get('trimmed_mean_5_percent')}|"
            f"{metrics.get('win_rate')}|{ra8.get('exact_count')}|"
        )
    lines.extend([
        "",
        "## Source-Confirmed Anomaly Records",
        "",
        "|research record|instrument|side|pnl proxy|classification|confidence|",
        "|---|---|---|---:|---|---|",
    ])
    for item in analysis.get("source_confirmed_anomalies", []):
        lines.append(
            f"|{item.get('research_record_id')}|{item.get('instrument')}|{item.get('side')}|{item.get('realized_pnl_proxy')}|{item.get('classification')}|{item.get('confidence')}|"
        )
    lines.extend([
        "",
        "The active qualified view excludes only source-confirmed source-integrity anomalies. Full Historical remains reported for comparison.",
        "",
        "## Cohort Summary",
        "",
        "|cohort|trades|avg pnl|median pnl|win rate|RA8 coverage|",
        "|---|---:|---:|---:|---:|---:|",
    ])
    for name in ("top_decile", "bottom_decile", "winners", "losers", "long", "short"):
        item = cohorts.get(name, {})
        lines.append(
            f"|{name}|{item.get('trade_count')}|{item.get('average_realized_pnl_proxy')}|{item.get('median_realized_pnl_proxy')}|{item.get('win_rate')}|{item.get('ra8_coverage_percentage')}|"
        )
    lines.extend(
        [
            "",
            "## Within-Instrument Controlled View",
            "",
            "|instrument|sample|top avg pnl|bottom avg pnl|top trades|bottom trades|",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for instrument, item in analysis.get("comparability_controlled", {}).get("included_instruments", {}).items():
        top = item.get("top_within_instrument", {})
        bottom = item.get("bottom_within_instrument", {})
        lines.append(
            f"|{instrument}|{item.get('sample_size')}|{top.get('average_realized_pnl_proxy')}|{bottom.get('average_realized_pnl_proxy')}|{top.get('trade_count')}|{bottom.get('trade_count')}|"
        )
    excluded = analysis.get("comparability_controlled", {}).get("excluded_instruments", {})
    if excluded:
        lines.extend(["", "Excluded instruments below the controlled-view sample threshold:"])
        lines.extend(f"- `{instrument}`: `{item.get('sample_size')}` trades" for instrument, item in excluded.items())
    lines.extend(
        [
            "",
            "## Descriptive Finding",
            "",
            "The top and bottom cohorts differ descriptively by realized P&L proxy and associated distributions. This artifact does not make causal claims or production guidance.",
            "",
            "## Warnings",
            "",
        ]
    )
    for warning in analysis.get("warnings", []):
        lines.append(f"- `{warning}`")
    return "\n".join(lines) + "\n"


def render_validation_markdown(validation: Mapping[str, Any]) -> str:
    lines = [
        "# Research Evidence Explorer Validation",
        "",
        f"- Status: `{validation.get('status')}`",
        f"- Blockers: `{len(validation.get('blockers', []))}`",
        f"- Warnings: `{len(validation.get('warnings', []))}`",
        "",
        "## Warnings",
        "",
    ]
    if validation.get("warnings"):
        lines.extend(f"- `{item}`" for item in validation.get("warnings", []))
    else:
        lines.append("No warnings.")
    return "\n".join(lines) + "\n"


def render_presentation_html(analysis: Mapping[str, Any]) -> str:
    payload = json.dumps(analysis, sort_keys=True)
    cohorts = analysis.get("cohorts", {})
    population = analysis.get("population", {})
    coverage = population.get("coverage", {})
    date_info = population.get("date_coverage", {})
    cohort_rows = []
    for name in ("top_decile", "bottom_decile", "winners", "losers", "long", "short"):
        item = cohorts.get(name, {})
        cohort_rows.append(
            "<tr>"
            f"<td>{html.escape(_display_label(name))}</td>"
            f"<td>{item.get('trade_count')}</td>"
            f"<td>{_format_money(item.get('average_realized_pnl_proxy'))}</td>"
            f"<td>{_format_money(item.get('median_realized_pnl_proxy'))}</td>"
            f"<td>{_format_percent(item.get('win_rate'))}</td>"
            f"<td>{item.get('ra8_coverage_count')} / {item.get('trade_count')} ({_format_percent(item.get('ra8_coverage_percentage'))})</td>"
            "</tr>"
        )
    delta_rows = "".join(
        "<tr>"
        f"<td>{html.escape(_display_label(name))}</td>"
        f"<td>{_format_delta(value, percent=name.endswith('rate') or name.endswith('percentage'))}</td>"
        "</tr>"
        for name, value in analysis.get("deltas", {}).get("top_decile_minus_bottom_decile", {}).items()
        if value is not None
    )
    warnings = "".join(f"<li>{html.escape(_display_label(str(warning)))}</li>" for warning in analysis.get("warnings", []))
    nav = "".join(
        f'<a href="#{anchor}">{label}</a>'
        for anchor, label in (
            ("overview", "Overview"),
            ("population-views", "Population Views"),
            ("inv004", "INV-004 NQ"),
            ("cohorts", "Cohort Comparison"),
            ("controlled", "Within-Instrument View"),
            ("distributions", "Distributions"),
            ("drilldown", "Trade Drill-Down"),
        )
    )
    controlled_rows = render_controlled_rows(analysis)
    controlled_chart = render_controlled_chart(analysis)
    population_view_options = "".join(
        f'<option value="{html.escape(str(view_id))}">{html.escape(_display_label(view_id))}</option>'
        for view_id in analysis.get("population_views", {})
    )
    population_view_rows = render_population_view_rows(analysis)
    anomaly_rows = render_anomaly_rows(analysis)
    inv_004_rows = render_inv_004_highlight_rows(analysis)
    distribution_sections = "".join(
        render_distribution_chart(key, item)
        for key, item in analysis.get("distributions", {}).items()
    )
    cohort_cards = render_cohort_cards(analysis)
    cohort_bars = render_cohort_bars(analysis)
    drill = "".join(
        "<option value='{idx}'>{label}</option>".format(
            idx=index,
            label=html.escape(
                f"{row.get('instrument')} {row.get('side')} {_format_money(row.get('realized_pnl_proxy'))} {row.get('exit_time')}"
            ),
        )
        for index, row in enumerate(analysis.get("trade_drill_down", [])[:200])
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Research Evidence Explorer v1</title>
  <style>
    :root {{ --ink:#1f2933; --muted:#68737d; --line:#d9e0df; --panel:#ffffff; --wash:#f6f7f4; --good:#28665a; --bad:#9a3f35; --mid:#486581; --warn:#8a5a00; }}
    html {{ scroll-behavior: smooth; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: var(--wash); color: var(--ink); }}
    main {{ max-width: 1240px; margin: 0 auto; padding: 24px; }}
    nav {{ position: sticky; top: 0; z-index: 2; display: flex; gap: 10px; flex-wrap: wrap; padding: 10px 24px; background: rgba(246,247,244,.96); border-bottom: 1px solid var(--line); }}
    nav a {{ color: var(--ink); text-decoration: none; padding: 7px 10px; border: 1px solid var(--line); border-radius: 5px; background: #fff; font-size: 13px; }}
    section {{ margin: 18px 0; padding: 18px; background: var(--panel); border: 1px solid var(--line); border-radius: 6px; }}
    h1 {{ margin: 8px 0 4px; font-size: 30px; }}
    h2 {{ margin: 0 0 14px; font-size: 20px; }}
    h3 {{ margin: 16px 0 8px; font-size: 15px; }}
    p {{ line-height: 1.45; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #e7ecea; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #eef2ef; cursor: default; }}
    th.sortable {{ cursor: pointer; }}
    .muted {{ color: var(--muted); }}
    .warning {{ color: var(--warn); font-weight: 700; }}
    .notice {{ border-left: 4px solid var(--warn); padding: 10px 12px; background: #fff8e8; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid var(--line); border-radius: 6px; padding: 12px; background: #fbfcfb; }}
    .label {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: .04em; }}
    .value {{ font-size: 22px; font-weight: 750; margin-top: 4px; }}
    .barrow {{ display: grid; grid-template-columns: 170px 1fr 90px; gap: 10px; align-items: center; margin: 8px 0; }}
    .track {{ height: 16px; background: #e7ecea; border-radius: 4px; overflow: hidden; }}
    .fill {{ height: 100%; background: var(--mid); }}
    .fill.good {{ background: var(--good); }}
    .fill.bad {{ background: var(--bad); }}
    .hist {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(54px, 1fr)); gap: 6px; align-items: end; min-height: 150px; margin-top: 12px; }}
    .histbar {{ background: var(--mid); min-height: 3px; color: white; font-size: 11px; display: flex; align-items: end; justify-content: center; padding: 3px; border-radius: 3px 3px 0 0; }}
    .empty {{ padding: 16px; border: 1px dashed var(--line); color: var(--muted); background: #fafafa; }}
    .pill {{ display: inline-block; padding: 3px 7px; border-radius: 999px; background: #edf2f2; margin: 2px; font-size: 12px; }}
    .details {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 10px; }}
    .detail {{ border-bottom: 1px solid #edf0ef; padding: 7px 0; }}
    .sources {{ max-height: 120px; overflow: auto; font-size: 12px; }}
    select {{ width: 100%; max-width: 760px; padding: 8px; }}
  </style>
</head>
<body>
<nav>{nav}</nav>
<main>
  <h1>Research Evidence Explorer v1</h1>
  <p class="muted">{html.escape(str(analysis.get('question')))}</p>
  <section id="overview">
    <h2>Overview</h2>
    <div class="grid">
      <div class="card"><div class="label">Population</div><div class="value">{population.get('included_count')}</div><div class="muted">{population.get('excluded_count')} excluded</div></div>
      <div class="card"><div class="label">CRR Readiness</div><div class="value">{html.escape(str(analysis.get('source', {}).get('crr_validation_status')))}</div></div>
      <div class="card"><div class="label">RA8 Coverage</div><div class="value">{coverage.get('ra8_exact_count')} / {population.get('included_count')}</div><div class="muted">{_format_percent(coverage.get('ra8_coverage_rate'))}</div></div>
      <div class="card"><div class="label">Exit Window</div><div class="value" style="font-size:15px">{_format_timestamp(date_info.get('first_exit_time'))}</div><div class="muted">to {_format_timestamp(date_info.get('last_exit_time'))}</div></div>
    </div>
    <p class="notice">Findings are descriptive and non-causal. The global comparison is portfolio-outcome analysis; raw P&L can reflect instrument, multiplier, and quantity differences.</p>
    <p class="notice">Active population view: <strong>{html.escape(str(population.get('active_population_view')))}</strong>. Full Historical remains visible for comparison; the active qualified view excludes only source-confirmed source-integrity anomalies.</p>
    <p class="warning">Detailed RA8 path coverage is partial. Missing RA8 is visible and is not treated as invalid CRR evidence.</p>
    <ul>{warnings}</ul>
  </section>
  <section id="population-views">
    <h2>Population Views</h2>
    <p>All views are deterministic prepared artifacts. No source rows are rewritten or silently hidden.</p>
    <select id="populationViewSelect">{population_view_options}</select>
    <div id="populationViewDetails" class="card"></div>
    <h3>Side-by-Side Summary</h3>
    <table><thead><tr><th>View</th><th>Included</th><th>Excluded</th><th>Total P&L</th><th>Average</th><th>Median</th><th>Trimmed Mean</th><th>Win Rate</th><th>RA8</th></tr></thead><tbody>{population_view_rows}</tbody></table>
    <h3>Source-Confirmed Anomaly Records</h3>
    <table><thead><tr><th>Research Record</th><th>Instrument</th><th>Side</th><th>P&L Proxy</th><th>Classification</th><th>Evidence</th></tr></thead><tbody>{anomaly_rows}</tbody></table>
    <p class="muted">Review-required records: {population.get('review_required_count')}.</p>
  </section>
  <section id="inv004">
    <h2>INV-004: NQ Qualified Performance Attribution</h2>
    <p>Prepared investigation highlight over the active source-integrity-qualified population. Descriptive only; no causal or production language.</p>
    <table><thead><tr><th>Metric</th><th>Value</th></tr></thead><tbody>{inv_004_rows}</tbody></table>
  </section>
  <section id="cohorts">
    <h2>Cohort Comparison</h2>
    {cohort_cards}
    <h3>Average P&L by Cohort</h3>
    {cohort_bars}
    <table><thead><tr><th>Cohort</th><th>Trades</th><th>Average P&L</th><th>Median P&L</th><th>Win Rate</th><th>RA8 Coverage</th></tr></thead><tbody>{''.join(cohort_rows)}</tbody></table>
    <h3>Top Minus Bottom Numeric Deltas</h3>
    <table><thead><tr><th>Metric</th><th>Delta</th></tr></thead><tbody>{delta_rows}</tbody></table>
  </section>
  <section id="controlled">
    <h2>Within-Instrument Controlled View</h2>
    <p>Standardized within-instrument realized P&L percentiles avoid inventing unavailable multiplier normalization. Click table headers to sort.</p>
    {controlled_chart}
    <table id="controlledTable"><thead><tr><th class="sortable" data-type="text">Instrument</th><th class="sortable">Sample</th><th>Top Count</th><th>Bottom Count</th><th class="sortable">Top Avg P&L</th><th>Top Median P&L</th><th class="sortable">Bottom Avg P&L</th><th>Bottom Median P&L</th><th class="sortable">Avg Delta</th><th>Long/Short Mix</th><th>RA8 Coverage</th></tr></thead><tbody>{controlled_rows}</tbody></table>
    <p class="muted">Excluded instruments below threshold: {render_excluded_instruments(analysis)}</p>
  </section>
  <section id="distributions">
    <h2>Distributions</h2>
    {distribution_sections}
  </section>
  <section id="drilldown">
    <h2>Trade Drill-Down</h2>
    <select id="tradeSelect">{drill}</select>
    <div id="tradeDetails" class="card"></div>
  </section>
</main>
<script type="application/json" id="prepared-artifact">{html.escape(payload)}</script>
<script>
const data = JSON.parse(document.getElementById('prepared-artifact').textContent);
const select = document.getElementById('tradeSelect');
const details = document.getElementById('tradeDetails');
const missing = '<span class="warning">MISSING</span>';
function label(value) {{
  return String(value || 'UNKNOWN').toLowerCase().split('_').map(x => x ? x[0].toUpperCase() + x.slice(1) : x).join(' ');
}}
function money(value) {{
  if (value === null || value === undefined || value === '') return missing;
  return new Intl.NumberFormat('en-US', {{style:'currency', currency:'USD', maximumFractionDigits: 2}}).format(Number(value));
}}
function pct(value) {{
  if (value === null || value === undefined || value === '') return missing;
  return (Number(value) * 100).toFixed(1) + '%';
}}
function duration(value) {{
  if (value === null || value === undefined || value === '') return missing;
  const seconds = Number(value);
  if (seconds >= 86400) return (seconds / 86400).toFixed(1) + ' days';
  if (seconds >= 3600) return (seconds / 3600).toFixed(1) + ' hrs';
  if (seconds >= 60) return (seconds / 60).toFixed(1) + ' min';
  return seconds.toFixed(0) + ' sec';
}}
function valueOrMissing(value) {{
  if (value === null || value === undefined || value === '') return missing;
  return String(value);
}}
function detail(labelText, value) {{
  return `<div class="detail"><div class="label">${{labelText}}</div><div>${{value}}</div></div>`;
}}
function renderTrade() {{
  const row = data.trade_drill_down[Number(select.value || 0)] || {{}};
  const ra8 = row.path_status?.ra8 === 'EXACT' ? 'RA8 path available' : 'Detailed RA8 path unavailable';
  const sources = (row.source_provenance || []).map(src => `<span class="pill">${{label(src.source_name)}}: ${{valueOrMissing(src.path || src.record_fingerprint)}}</span>`).join(' ');
  details.innerHTML = `
    <div class="details">
      ${{detail('Research Record ID', valueOrMissing(row.research_record_id))}}
      ${{detail('Instrument / Contract', `${{valueOrMissing(row.instrument)}} / ${{valueOrMissing(row.contract)}}`)}}
      ${{detail('Side / Quantity', `${{label(row.side)}} / ${{valueOrMissing(row.quantity)}}`)}}
      ${{detail('Strategy', valueOrMissing(row.strategy_id))}}
      ${{detail('Lane', valueOrMissing(row.lane_id))}}
      ${{detail('Session / Regime', `${{label(row.session)}} / ${{label(row.regime)}}`)}}
      ${{detail('Entry', `${{valueOrMissing(row.entry_time)}} at ${{valueOrMissing(row.entry_price)}}`)}}
      ${{detail('Exit', `${{valueOrMissing(row.exit_time)}} at ${{valueOrMissing(row.exit_price)}}`)}}
      ${{detail('Realized P&L Proxy', money(row.realized_pnl_proxy))}}
      ${{detail('Duration', duration(row.hold_seconds))}}
      ${{detail('MFE / MAE / Giveback', `${{valueOrMissing(row.outcome_summary?.mfe_points)}} / ${{valueOrMissing(row.outcome_summary?.mae_points)}} / ${{valueOrMissing(row.outcome_summary?.giveback_points)}}`)}}
      ${{detail('Exit Policy / Reason', `${{label(row.exit_policy)}} / ${{label(row.exit_reason)}}`)}}
      ${{detail('Path Status', `RA7: ${{label(row.path_status?.ra7)}}<br>${{ra8}}`)}}
      ${{detail('Attribution', `Entry: ${{label(row.attribution_status?.entry)}}<br>Exit: ${{label(row.attribution_status?.exit)}}`)}}
      ${{detail('Cohorts', (row.cohort_memberships || []).map(label).join(', ') || 'None')}}
      ${{detail('Missing Fields', (row.missing_fields || []).map(label).join(', ') || 'None')}}
    </div>
    <h3>Source Provenance</h3>
    <div class="sources">${{sources || 'No provenance references supplied.'}}</div>
  `;
}}
function renderPopulationView() {{
  const viewId = document.getElementById('populationViewSelect').value;
  const view = data.population_views?.[viewId] || {{}};
  const metrics = view.metrics || {{}};
  const ra8 = view.ra8_coverage || {{}};
  document.getElementById('populationViewDetails').innerHTML = `
    <div class="details">
      ${{detail('View', valueOrMissing(viewId))}}
      ${{detail('Included / Excluded', `${{valueOrMissing(view.included_count)}} / ${{valueOrMissing(view.excluded_count)}}`)}}
      ${{detail('Total P&L Proxy', money(metrics.total_realized_pnl_proxy))}}
      ${{detail('Average / Median', `${{money(metrics.average_realized_pnl_proxy)}} / ${{money(metrics.median_realized_pnl_proxy)}}`)}}
      ${{detail('Trimmed Mean / Win Rate', `${{money(metrics.trimmed_mean_5_percent)}} / ${{pct(metrics.win_rate)}}`)}}
      ${{detail('RA8 Coverage', `${{valueOrMissing(ra8.exact_count)}} exact, ${{valueOrMissing(ra8.missing_count)}} missing (${{pct(ra8.coverage_rate)}})`)}}
    </div>
  `;
}}
select.addEventListener('change', renderTrade);
renderTrade();
document.getElementById('populationViewSelect').addEventListener('change', renderPopulationView);
renderPopulationView();
document.querySelectorAll('#controlledTable th.sortable').forEach((th, index) => {{
  th.addEventListener('click', () => {{
    const body = th.closest('table').querySelector('tbody');
    const rows = Array.from(body.querySelectorAll('tr'));
    const numeric = th.dataset.type !== 'text';
    rows.sort((a, b) => {{
      const av = a.children[index].dataset.sort || a.children[index].textContent;
      const bv = b.children[index].dataset.sort || b.children[index].textContent;
      return numeric ? Number(bv) - Number(av) : av.localeCompare(bv);
    }});
    rows.forEach(row => body.appendChild(row));
  }});
}});
</script>
</body>
</html>
"""


def render_cohort_cards(analysis: Mapping[str, Any]) -> str:
    cohorts = analysis.get("cohorts", {})
    cards = []
    for name in ("top_decile", "bottom_decile", "winners", "losers", "long", "short"):
        item = cohorts.get(name, {})
        cards.append(
            '<div class="card">'
            f'<div class="label">{html.escape(_display_label(name))}</div>'
            f'<div class="value">{_format_money(item.get("average_realized_pnl_proxy"))}</div>'
            f'<div class="muted">{item.get("trade_count")} trades, median {_format_money(item.get("median_realized_pnl_proxy"))}, win rate {_format_percent(item.get("win_rate"))}</div>'
            "</div>"
        )
    return f'<div class="grid">{"".join(cards)}</div>'


def render_cohort_bars(analysis: Mapping[str, Any]) -> str:
    cohorts = analysis.get("cohorts", {})
    values = [
        _number(cohorts.get(name, {}).get("average_realized_pnl_proxy")) or 0.0
        for name in ("top_decile", "bottom_decile", "winners", "losers", "long", "short")
    ]
    max_abs = max([abs(value) for value in values] or [1.0]) or 1.0
    rows = []
    for name, value in zip(("top_decile", "bottom_decile", "winners", "losers", "long", "short"), values, strict=True):
        width = max(2.0, abs(value) / max_abs * 100.0)
        css = "good" if value >= 0 else "bad"
        rows.append(
            '<div class="barrow">'
            f'<div>{html.escape(_display_label(name))}</div>'
            f'<div class="track"><div class="fill {css}" style="width:{width:.1f}%"></div></div>'
            f'<div>{_format_money(value)}</div>'
            "</div>"
        )
    return "".join(rows)


def render_controlled_rows(analysis: Mapping[str, Any]) -> str:
    rows = []
    for instrument, item in analysis.get("comparability_controlled", {}).get("included_instruments", {}).items():
        top = item.get("top_within_instrument", {})
        bottom = item.get("bottom_within_instrument", {})
        delta = _number(top.get("average_realized_pnl_proxy")) - _number(bottom.get("average_realized_pnl_proxy")) if _number(top.get("average_realized_pnl_proxy")) is not None and _number(bottom.get("average_realized_pnl_proxy")) is not None else None
        mix = _format_mix(top.get("long_short_mix", {}), bottom.get("long_short_mix", {}))
        ra8 = f'{top.get("ra8_coverage_count")} / {top.get("trade_count")} top; {bottom.get("ra8_coverage_count")} / {bottom.get("trade_count")} bottom'
        rows.append(
            "<tr>"
            f'<td data-sort="{html.escape(str(instrument))}">{html.escape(str(instrument))}</td>'
            f'<td data-sort="{item.get("sample_size")}">{item.get("sample_size")}</td>'
            f'<td>{top.get("trade_count")}</td>'
            f'<td>{bottom.get("trade_count")}</td>'
            f'<td data-sort="{top.get("average_realized_pnl_proxy")}">{_format_money(top.get("average_realized_pnl_proxy"))}</td>'
            f'<td>{_format_money(top.get("median_realized_pnl_proxy"))}</td>'
            f'<td data-sort="{bottom.get("average_realized_pnl_proxy")}">{_format_money(bottom.get("average_realized_pnl_proxy"))}</td>'
            f'<td>{_format_money(bottom.get("median_realized_pnl_proxy"))}</td>'
            f'<td data-sort="{delta}">{_format_money(delta)}</td>'
            f"<td>{mix}</td>"
            f"<td>{ra8}</td>"
            "</tr>"
        )
    return "".join(rows)


def render_controlled_chart(analysis: Mapping[str, Any]) -> str:
    items = analysis.get("comparability_controlled", {}).get("included_instruments", {})
    deltas: list[tuple[str, float]] = []
    for instrument, item in items.items():
        top = _number(item.get("top_within_instrument", {}).get("average_realized_pnl_proxy"))
        bottom = _number(item.get("bottom_within_instrument", {}).get("average_realized_pnl_proxy"))
        if top is not None and bottom is not None:
            deltas.append((str(instrument), top - bottom))
    max_abs = max([abs(delta) for _, delta in deltas] or [1.0]) or 1.0
    rows = []
    for instrument, delta in sorted(deltas, key=lambda item: abs(item[1]), reverse=True):
        width = max(2.0, abs(delta) / max_abs * 100.0)
        rows.append(
            '<div class="barrow">'
            f"<div>{html.escape(instrument)}</div>"
            f'<div class="track"><div class="fill" style="width:{width:.1f}%"></div></div>'
            f"<div>{_format_money(delta)}</div>"
            "</div>"
        )
    return "".join(rows)


def render_excluded_instruments(analysis: Mapping[str, Any]) -> str:
    controlled = analysis.get("comparability_controlled", {})
    excluded = controlled.get("excluded_instruments", {})
    threshold = controlled.get("minimum_sample_size")
    if not excluded:
        return f"None. Minimum sample threshold: {threshold}."
    return "; ".join(
        f"{html.escape(str(instrument))}: {item.get('sample_size')} trades, threshold {threshold}"
        for instrument, item in excluded.items()
    )


def render_population_view_rows(analysis: Mapping[str, Any]) -> str:
    rows = []
    for view_id, view in analysis.get("population_views", {}).items():
        metrics = view.get("metrics", {})
        ra8 = view.get("ra8_coverage", {})
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(view_id))}</td>"
            f"<td>{view.get('included_count')}</td>"
            f"<td>{view.get('excluded_count')}</td>"
            f"<td>{_format_money(metrics.get('total_realized_pnl_proxy'))}</td>"
            f"<td>{_format_money(metrics.get('average_realized_pnl_proxy'))}</td>"
            f"<td>{_format_money(metrics.get('median_realized_pnl_proxy'))}</td>"
            f"<td>{_format_money(metrics.get('trimmed_mean_5_percent'))}</td>"
            f"<td>{_format_percent(metrics.get('win_rate'))}</td>"
            f"<td>{ra8.get('exact_count')} / {view.get('included_count')} ({_format_percent(ra8.get('coverage_rate'))})</td>"
            "</tr>"
        )
    return "".join(rows)


def render_anomaly_rows(analysis: Mapping[str, Any]) -> str:
    rows = []
    for item in analysis.get("source_confirmed_anomalies", []):
        evidence = "; ".join(str(value) for value in item.get("evidence_basis", [])[:2])
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(item.get('research_record_id')))}</td>"
            f"<td>{html.escape(str(item.get('instrument')))}</td>"
            f"<td>{html.escape(str(item.get('side')))}</td>"
            f"<td>{_format_money(item.get('realized_pnl_proxy'))}</td>"
            f"<td>{html.escape(str(item.get('classification')))}</td>"
            f"<td>{html.escape(evidence)}</td>"
            "</tr>"
        )
    if not rows:
        return '<tr><td colspan="6">No source-confirmed anomaly records in the prepared eligibility artifact.</td></tr>'
    return "".join(rows)


def render_inv_004_highlight_rows(analysis: Mapping[str, Any]) -> str:
    highlight = analysis.get("investigation_highlights", {}).get("INV-004", {})
    top = highlight.get("top_contributors", {})
    rows = [
        ("Qualified NQ trades", highlight.get("qualified_nq_trade_count")),
        ("Total P&L proxy", _format_money(highlight.get("total_realized_pnl_proxy"))),
        ("Average P&L proxy", _format_money(highlight.get("average_realized_pnl_proxy"))),
        ("Median P&L proxy", _format_money(highlight.get("median_realized_pnl_proxy"))),
        ("Win rate", _format_percent(highlight.get("win_rate"))),
        ("After excluding top 10%", _format_money(highlight.get("tail_sensitivity", {}).get("excluding_top_10_percent_total"))),
        ("Tail sensitivity", highlight.get("tail_sensitivity", {}).get("primary_finding")),
        ("Top strategy", top.get("strategy", {}).get("value")),
        ("Top lane", top.get("lane", {}).get("value")),
        ("Top session", top.get("session", {}).get("value")),
        ("Strongest contradictory evidence", "; ".join(str(item) for item in highlight.get("strongest_contradictory_evidence", []))),
        ("Investigation artifact", highlight.get("artifact_path")),
    ]
    return "".join(
        "<tr>"
        f"<td>{html.escape(str(label))}</td>"
        f"<td>{html.escape(str(value if value not in (None, '') else 'MISSING'))}</td>"
        "</tr>"
        for label, value in rows
    )


def render_distribution_chart(key: str, item: Mapping[str, Any]) -> str:
    histogram = item.get("histogram", {})
    bins = histogram.get("bins", [])
    label = html.escape(str(item.get("label") or key))
    included = item.get("included_sample_count")
    missing = item.get("missing_count")
    units = html.escape(str(item.get("units") or ""))
    warning = '<p class="warning">Sparse data: this metric is incomplete for the current population.</p>' if item.get("sparse_data_warning") else ""
    if not bins:
        return (
            f"<div class=\"card\"><h3>{label}</h3>"
            f"<p class=\"muted\">Included sample count: {included}; missing: {missing}; units: {units}; context: full population.</p>"
            '<div class="empty">No chartable values are available for this metric.</div>'
            f"{warning}</div>"
        )
    max_count = max((int(bin_item.get("count", 0)) for bin_item in bins), default=1) or 1
    bars = []
    for bin_item in bins:
        count = int(bin_item.get("count", 0))
        height = max(3.0, count / max_count * 140.0)
        bars.append(
            f'<div title="{_format_value(bin_item.get("low"))} to {_format_value(bin_item.get("high"))}: {count}" class="histbar" style="height:{height:.1f}px">{count}</div>'
        )
    return (
        f'<div class="card"><h3>{label}</h3>'
        f'<p class="muted">Included sample count: {included}; missing: {missing}; units: {units}; context: full population.</p>'
        f"{warning}"
        f'<div class="hist">{"".join(bars)}</div></div>'
    )


def _display_label(value: Any) -> str:
    raw = str(value or "UNKNOWN").replace("-", "_")
    words = [word for word in raw.split("_") if word]
    return " ".join(word.capitalize() for word in words) if words else "Unknown"


def _format_money(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "MISSING"
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.2f}"


def _format_percent(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "MISSING"
    return f"{number * 100:.1f}%"


def _format_delta(value: Any, *, percent: bool = False) -> str:
    number = _number(value)
    if number is None:
        return "MISSING"
    sign = "+" if number > 0 else ""
    if percent:
        return f"{sign}{number * 100:.1f} pp"
    return f"{sign}{_format_value(number)}"


def _format_duration(value: Any) -> str:
    seconds = _number(value)
    if seconds is None:
        return "MISSING"
    if seconds >= 86400:
        return f"{seconds / 86400:.1f} days"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} hrs"
    if seconds >= 60:
        return f"{seconds / 60:.1f} min"
    return f"{seconds:.0f} sec"


def _format_timestamp(value: Any) -> str:
    if value in (None, ""):
        return "MISSING"
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    return parsed.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def _format_value(value: Any) -> str:
    number = _number(value)
    if number is None:
        return "MISSING"
    return f"{number:,.2f}"


def _format_mix(top_mix: Mapping[str, Any], bottom_mix: Mapping[str, Any]) -> str:
    keys = sorted(set(top_mix) | set(bottom_mix))
    if not keys:
        return "MISSING"
    return "; ".join(
        f"{html.escape(_display_label(key))}: top {top_mix.get(key, 0)}, bottom {bottom_mix.get(key, 0)}"
        for key in keys
    )


def _histogram(values: Sequence[float], *, bins: int = 8) -> dict[str, Any]:
    vals = [float(value) for value in values]
    if not vals:
        return {"bins": [], "count": 0}
    low = min(vals)
    high = max(vals)
    if low == high:
        return {"bins": [{"low": _round(low), "high": _round(high), "count": len(vals)}], "count": len(vals)}
    width = (high - low) / bins
    result = []
    for index in range(bins):
        start = low + width * index
        end = high if index == bins - 1 else low + width * (index + 1)
        count = sum(1 for value in vals if (start <= value <= end if index == bins - 1 else start <= value < end))
        result.append({"low": _round(start), "high": _round(end), "count": count})
    return {"bins": result, "count": len(vals)}


def _values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = _number(row.get(key))
        if value is not None:
            values.append(value)
    return values


def _distribution(values: Any, *, limit: int | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    if limit:
        items = items[:limit]
    return dict(items)


def _missing_field_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for field in row.get("missing_fields", []):
            counts[str(field)] = counts.get(str(field), 0) + 1
    return dict(sorted(counts.items()))


def _average(values: Sequence[float]) -> float | None:
    return _round(sum(values) / len(values)) if values else None


def _median(values: Sequence[float]) -> float | None:
    return _round(float(median(values))) if values else None


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 6)


def _round(value: float | None) -> float | None:
    return round(float(value), 6) if value is not None else None


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fingerprint_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in {"generated_at", "deterministic_fingerprint"}}


def _fingerprint(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
