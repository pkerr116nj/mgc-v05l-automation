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


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CRR_PATH = DEFAULT_CRR_OUTPUT_DIR / CRR_JSONL
DEFAULT_CRR_VALIDATION_PATH = DEFAULT_CRR_OUTPUT_DIR / VALIDATION_JSON
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "research_evidence_explorer"

ANALYSIS_JSON = "research_evidence_explorer_v1.json"
ANALYSIS_MD = "research_evidence_explorer_v1.md"
POPULATION_JSON = "research_evidence_explorer_population.json"
SCHEMA_JSON = "research_evidence_explorer_schema.json"
VALIDATION_REPORT_JSON = "research_evidence_explorer_validation_report.json"
VALIDATION_REPORT_MD = "research_evidence_explorer_validation_report.md"
PRESENTATION_HTML = "research_evidence_explorer_v1.html"

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


def run_research_evidence_explorer(
    *,
    crr_path: Path = DEFAULT_CRR_PATH,
    crr_validation_path: Path = DEFAULT_CRR_VALIDATION_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> ResearchEvidenceExplorerResult:
    generated_at = _coerce_now(now)
    crr_rows = _read_jsonl(crr_path)
    crr_validation = _read_json(crr_validation_path)
    analysis, population, validation = build_research_evidence_explorer(
        crr_rows,
        crr_validation=crr_validation,
        crr_path=crr_path,
        crr_validation_path=crr_validation_path,
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


def build_research_evidence_explorer(
    crr_rows: Sequence[Mapping[str, Any]],
    *,
    crr_validation: Mapping[str, Any],
    crr_path: Path,
    crr_validation_path: Path,
    generated_at: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    readiness = list(crr_validation.get("upstream_readiness", []))
    required_ready = all(
        item.get("readiness_classification") == "READY"
        for item in readiness
        if item.get("source_name") in {"canonical_trade_records", *REQUIRED_LAYERS}
    )
    source_validation_status = str(crr_validation.get("status") or "UNKNOWN")
    population_rows, exclusions = build_explorer_population(crr_rows, required_sources_ready=required_ready)
    cohorts = build_cohorts(population_rows)
    cohort_metrics = {name: cohort_metrics_for(rows, full_population_count=len(population_rows)) for name, rows in cohorts.items()}
    deltas = numeric_metric_deltas(cohort_metrics.get("top_decile", {}), cohort_metrics.get("bottom_decile", {}))
    controlled_comparison = build_within_instrument_comparison(population_rows)
    population = {
        "schema_version": f"{SCHEMA_VERSION}_population",
        "generated_at": generated_at.isoformat(),
        "source_crr_path": str(crr_path),
        "source_crr_fingerprint": _file_sha256(crr_path),
        "source_crr_validation_path": str(crr_validation_path),
        "source_crr_validation_status": source_validation_status,
        "required_sources_ready": required_ready,
        "population_definition": {
            "include": "CRR rows with valid required-source readiness, no broken joins, exact CTOL/CTOE/RA7/RA3 joins, and numeric realized P&L proxy.",
            "ra8_required": False,
            "tolerance_joins_allowed": False,
        },
        "input_count": len(crr_rows),
        "included_count": len(population_rows),
        "excluded_count": len(exclusions),
        "exclusions": exclusions,
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
        },
        "question": "How do the characteristics of the best completed trades differ from the worst completed trades?",
        "comparability_disclosure": {
            "global_comparison_interpretation": "Portfolio-outcome analysis.",
            "limitation": "Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences. It is not automatically a strategy-quality comparison.",
            "controlled_view": "Within-instrument standardized realized P&L percentile comparison.",
            "normalization_policy": "No per-contract or multiplier normalization is invented where contract economics are not present in CRR.",
        },
        "population": population,
        "cohort_definitions": cohort_definitions(len(population_rows)),
        "metric_definitions": metric_definitions(),
        "cohorts": cohort_metrics,
        "deltas": {"top_decile_minus_bottom_decile": deltas},
        "comparability_controlled": controlled_comparison,
        "distributions": build_distributions(population_rows),
        "trade_drill_down": trade_drill_down_rows(population_rows),
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    population: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    for row in rows:
        reason = exclusion_reason(row, required_sources_ready=required_sources_ready)
        if reason:
            exclusions.append(
                {
                    "research_record_id": row.get("research_record_id"),
                    "source_trade_id": row.get("trade_identity", {}).get("source_trade_id"),
                    "reason": reason,
                }
            )
            continue
        normalized = normalize_crr_row(row)
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
        "instrument": identity.get("instrument") or "UNKNOWN",
        "contract": identity.get("contract") or "UNKNOWN",
        "side": str(identity.get("side") or "UNKNOWN").upper(),
        "quantity": identity.get("quantity"),
        "strategy_id": entry.get("strategy_id") or "UNKNOWN",
        "lane_id": entry.get("lane_id") or "UNKNOWN",
        "entry_time": entry.get("entry_time"),
        "exit_time": exit_anchor.get("exit_time"),
        "entry_price": entry.get("entry_price"),
        "exit_price": exit_anchor.get("exit_price"),
        "exit_reason": exit_anchor.get("exit_reason") or exit_anchor.get("exit_policy") or "UNKNOWN",
        "exit_policy": exit_anchor.get("exit_policy") or "UNKNOWN",
        "realized_pnl_proxy": _number(outcome.get("realized_pnl_proxy")),
        "realized_points": realized_points,
        "hold_seconds": _number(outcome.get("hold_seconds")),
        "mfe_points": mfe,
        "mae_points": mae,
        "giveback_points": giveback,
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


def cohort_metrics_for(rows: Sequence[Mapping[str, Any]], *, full_population_count: int) -> dict[str, Any]:
    pnl = _values(rows, "realized_pnl_proxy")
    hold = _values(rows, "hold_seconds")
    mfe = _values(rows, "mfe_points")
    mae = _values(rows, "mae_points")
    giveback = _values(rows, "giveback_points")
    wins = [value for value in pnl if value > 0.0]
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
        "ra8_coverage_count": sum(1 for row in rows if row.get("ra8_coverage_status") == "EXACT"),
        "ra8_coverage_percentage": _rate(sum(1 for row in rows if row.get("ra8_coverage_status") == "EXACT"), len(rows)),
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
        "realized_pnl_proxy": _histogram(_values(rows, "realized_pnl_proxy")),
        "mfe_points": _histogram(_values(rows, "mfe_points")),
        "mae_points": _histogram(_values(rows, "mae_points")),
        "duration_seconds": _histogram(_values(rows, "hold_seconds")),
        "giveback_points": _histogram(_values(rows, "giveback_points")),
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


def trade_drill_down_rows(rows: Sequence[Mapping[str, Any]], *, limit: int = 200) -> list[dict[str, Any]]:
    ranked = sorted(rows, key=lambda row: (abs(float(row.get("realized_pnl_proxy") or 0.0)), str(row.get("research_record_id"))), reverse=True)
    return [
        {
            "research_record_id": row.get("research_record_id"),
            "source_trade_id": row.get("source_trade_id"),
            "instrument": row.get("instrument"),
            "contract": row.get("contract"),
            "side": row.get("side"),
            "entry_time": row.get("entry_time"),
            "exit_time": row.get("exit_time"),
            "entry_price": row.get("entry_price"),
            "exit_price": row.get("exit_price"),
            "realized_pnl_proxy": row.get("realized_pnl_proxy"),
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
        }
        for row in ranked[:limit]
    ]


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
        f"- Included trades: `{population.get('included_count')}`",
        f"- Excluded trades: `{population.get('excluded_count')}`",
        f"- Exit coverage: `{population.get('date_coverage', {}).get('first_exit_time')}` to `{population.get('date_coverage', {}).get('last_exit_time')}`",
        f"- RA8 coverage: `{population.get('coverage', {}).get('ra8_exact_count')}/{population.get('included_count')}`",
        f"- Comparability-controlled view: `{analysis.get('comparability_controlled', {}).get('method')}`",
        "",
        "## Comparability Disclosure",
        "",
        "The global top-versus-bottom view is portfolio-outcome analysis. Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences, so it is not automatically a strategy-quality comparison.",
        "",
        "## Cohort Summary",
        "",
        "|cohort|trades|avg pnl|median pnl|win rate|RA8 coverage|",
        "|---|---:|---:|---:|---:|---:|",
    ]
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
    rows = []
    for name in ("top_decile", "bottom_decile", "winners", "losers", "long", "short"):
        item = cohorts.get(name, {})
        rows.append(
            "<tr>"
            f"<td>{html.escape(name)}</td>"
            f"<td>{item.get('trade_count')}</td>"
            f"<td>{item.get('average_realized_pnl_proxy')}</td>"
            f"<td>{item.get('median_realized_pnl_proxy')}</td>"
            f"<td>{item.get('win_rate')}</td>"
            f"<td>{item.get('ra8_coverage_percentage')}</td>"
            "</tr>"
        )
    warnings = "".join(f"<li>{html.escape(str(warning))}</li>" for warning in analysis.get("warnings", []))
    instrument_coverage = html.escape(json.dumps(analysis.get("population", {}).get("instrument_coverage", {}), sort_keys=True))
    controlled = html.escape(json.dumps(analysis.get("comparability_controlled", {}), sort_keys=True))
    date_coverage = analysis.get("population", {}).get("date_coverage", {})
    drill = "".join(
        "<option value='{idx}'>{label}</option>".format(
            idx=index,
            label=html.escape(f"{row.get('instrument')} {row.get('side')} {row.get('realized_pnl_proxy')} {row.get('exit_time')}"),
        )
        for index, row in enumerate(analysis.get("trade_drill_down", [])[:200])
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Research Evidence Explorer v1</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 24px; background: #f7f7f4; color: #1d2527; }}
    main {{ max-width: 1180px; margin: 0 auto; }}
    section {{ margin: 22px 0; padding: 18px; background: #fff; border: 1px solid #d8ddd9; border-radius: 6px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ border-bottom: 1px solid #e5e8e5; padding: 8px; text-align: left; }}
    th {{ background: #eef2ef; }}
    .warning {{ color: #7a4b00; font-weight: 600; }}
    .bars {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; align-items: end; min-height: 140px; }}
    .bar {{ background: #587c7a; color: white; padding: 6px; min-height: 20px; display: flex; align-items: end; }}
    pre {{ white-space: pre-wrap; background: #f1f3f2; padding: 12px; border-radius: 6px; max-height: 380px; overflow: auto; }}
  </style>
</head>
<body>
<main>
  <h1>Research Evidence Explorer v1</h1>
  <p>{html.escape(str(analysis.get('question')))}</p>
  <section>
    <h2>Overview</h2>
    <p>Population: <strong>{analysis.get('population', {}).get('included_count')}</strong> trades. CRR readiness: <strong>{html.escape(str(analysis.get('source', {}).get('crr_validation_status')))}</strong>.</p>
    <p>Exit coverage: <strong>{html.escape(str(date_coverage.get('first_exit_time')))}</strong> to <strong>{html.escape(str(date_coverage.get('last_exit_time')))}</strong>.</p>
    <p>Instrument coverage: <code>{instrument_coverage}</code></p>
    <p><strong>Comparability:</strong> Global top/bottom is portfolio-outcome analysis. Raw P&L can reflect instrument, multiplier, and quantity differences.</p>
    <p class="warning">RA8 coverage: {analysis.get('population', {}).get('coverage', {}).get('ra8_exact_count')} / {analysis.get('population', {}).get('included_count')} exact finalized captures. Missing RA8 is visible and not treated as broken.</p>
    <ul>{warnings}</ul>
  </section>
  <section>
    <h2>Cohort Comparison</h2>
    <table><thead><tr><th>Cohort</th><th>Trades</th><th>Average P&L</th><th>Median P&L</th><th>Win Rate</th><th>RA8 Coverage</th></tr></thead><tbody>{''.join(rows)}</tbody></table>
  </section>
  <section>
    <h2>Within-Instrument Controlled View</h2>
    <p>Standardized within-instrument realized P&L percentiles avoid inventing unavailable multiplier normalization. Sample sizes and excluded instruments are included.</p>
    <pre>{controlled}</pre>
  </section>
  <section>
    <h2>Distributions</h2>
    <p>Restrained distribution bins are precomputed in the analytics artifact.</p>
    <pre id="distributions"></pre>
  </section>
  <section>
    <h2>Trade Drill-Down</h2>
    <select id="tradeSelect">{drill}</select>
    <pre id="tradeDetails"></pre>
  </section>
</main>
<script type="application/json" id="prepared-artifact">{html.escape(payload)}</script>
<script>
const data = JSON.parse(document.getElementById('prepared-artifact').textContent);
document.getElementById('distributions').textContent = JSON.stringify(data.distributions, null, 2);
const select = document.getElementById('tradeSelect');
const details = document.getElementById('tradeDetails');
function renderTrade() {{
  const row = data.trade_drill_down[Number(select.value || 0)] || {{}};
  details.textContent = JSON.stringify(row, null, 2);
}}
select.addEventListener('change', renderTrade);
renderTrade();
</script>
</body>
</html>
"""


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
