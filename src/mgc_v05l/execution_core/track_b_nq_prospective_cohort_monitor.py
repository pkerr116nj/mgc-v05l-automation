"""Prospective NQ cohort monitor.

This module freezes INV-005/INV-006 discovery definitions and evaluates later
qualified NQ trades out of sample. It is offline research only and has no
broker, runtime, strategy, Managed Exit, Guardian, Safe-State, readiness,
reconciliation, or trading-gate authority.
"""

from __future__ import annotations

import hashlib
import html
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_research_evidence_explorer import (
    DEFAULT_CRR_PATH,
    DEFAULT_CRR_VALIDATION_PATH,
    DEFAULT_ELIGIBILITY_RECORDS_PATH,
    DEFAULT_INVESTIGATION_OUTPUT_DIR,
    GUARDRAILS as EXPLORER_GUARDRAILS,
    cohort_evidence,
    investigation_metrics,
    nq_percentile_contribution,
    rolling_window_metrics,
    _fingerprint,
    _fingerprint_payload,
    _missing_field_counts,
    _number,
    _read_json,
    _read_jsonl,
)


DEFAULT_OUTPUT_DIR = (
    Path("outputs")
    / "track_b_execution_core"
    / "research_analytics"
    / "prospective_nq_cohort_monitor"
)
DEFAULT_INV_005_DIR = DEFAULT_INVESTIGATION_OUTPUT_DIR / "INV-005"
DEFAULT_INV_006_DIR = DEFAULT_INVESTIGATION_OUTPUT_DIR / "INV-006"
DEFAULT_INV_005_PEER_ASSIGNMENTS_PATH = DEFAULT_INV_005_DIR / "peer_cohort_assignments.json"
DEFAULT_INV_005_PEER_METRICS_PATH = DEFAULT_INV_005_DIR / "peer_cohort_metrics.json"
DEFAULT_INV_006_PERIOD_COMPARISON_PATH = DEFAULT_INV_006_DIR / "period_comparison.json"

DISCOVERY_CUTOFF_UTC = "2026-08-07T04:00:00+00:00"
PROSPECTIVE_START_ET = "2026-08-07 00:00:00 America/New_York"
DISCOVERY_POPULATION_LABEL = "all SOURCE_INTEGRITY_QUALIFIED NQ trades through 2026-08-06 inclusive"
ELIGIBLE_CLASSIFICATIONS = {"ELIGIBLE_ORDINARY_STRATEGY_EVIDENCE", "ELIGIBLE_WITH_LIMITATIONS"}
SCHEMA_VERSION = "nq_prospective_cohort_monitor_v1"
BASELINE_SCHEMA_VERSION = "nq_prospective_discovery_baselines_v1"

GUARDRAILS = {
    **EXPLORER_GUARDRAILS,
    "live_money_eligible": False,
}

DISCOVERY_COHORTS = (
    {
        "cohort_id": "nq_globex_participation_long",
        "source": "INV-006_REQUIRED_BASELINE",
        "definition": {
            "instrument": "NQ",
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_NQ_GLOBEX_PARTICIPATION_LONG_V1",
            "side": "LONG",
            "session": "GLOBEX",
        },
    },
    {
        "cohort_id": "nq_us_participation_long",
        "source": "INV-006_REQUIRED_BASELINE",
        "definition": {
            "instrument": "NQ",
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_NQ_US_PARTICIPATION_LONG_V1",
            "side": "LONG",
            "session": "US",
        },
    },
)

CHECKPOINTS = (10, 20, 50, 100)


@dataclass(frozen=True)
class ProspectiveMonitorResult:
    discovery_baselines: dict[str, Any]
    prospective_population: dict[str, Any]
    prospective_results: dict[str, Any]
    discovery_vs_prospective: dict[str, Any]
    checkpoint_history: list[dict[str, Any]]
    validation_report: dict[str, Any]
    context_coverage_audit: dict[str, Any]
    output_paths: dict[str, Path]


def run_nq_prospective_cohort_monitor(
    *,
    crr_path: Path = DEFAULT_CRR_PATH,
    crr_validation_path: Path = DEFAULT_CRR_VALIDATION_PATH,
    eligibility_records_path: Path = DEFAULT_ELIGIBILITY_RECORDS_PATH,
    inv_005_peer_assignments_path: Path = DEFAULT_INV_005_PEER_ASSIGNMENTS_PATH,
    inv_005_peer_metrics_path: Path = DEFAULT_INV_005_PEER_METRICS_PATH,
    inv_006_period_comparison_path: Path = DEFAULT_INV_006_PERIOD_COMPARISON_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> ProspectiveMonitorResult:
    generated_at = _coerce_now(now).isoformat()
    crr_rows = _read_jsonl(crr_path)
    crr_validation = _read_json(crr_validation_path)
    eligibility_rows = _read_jsonl(eligibility_records_path)
    peer_assignments = _read_json(inv_005_peer_assignments_path)
    peer_metrics = _read_json(inv_005_peer_metrics_path)
    period_comparison = _read_json(inv_006_period_comparison_path)
    source_fingerprints = build_source_fingerprints(
        {
            "crr": crr_path,
            "crr_validation": crr_validation_path,
            "research_eligibility": eligibility_records_path,
            "inv_005_peer_assignments": inv_005_peer_assignments_path,
            "inv_005_peer_metrics": inv_005_peer_metrics_path,
            "inv_006_period_comparison": inv_006_period_comparison_path,
        }
    )
    eligibility_by_id = {row.get("research_record_id"): row for row in eligibility_rows}
    qualified_rows = qualified_nq_rows(crr_rows, eligibility_by_id=eligibility_by_id)
    discovery_rows = [row for row in qualified_rows if row_timestamp(row) < DISCOVERY_CUTOFF_UTC]
    prospective_rows = [row for row in qualified_rows if row_timestamp(row) >= DISCOVERY_CUTOFF_UTC]
    excluded_rows = excluded_prospective_rows(crr_rows, eligibility_by_id=eligibility_by_id)
    cohort_definitions = freeze_cohort_definitions(discovery_rows, peer_assignments)
    discovery_baselines = build_discovery_baselines(
        discovery_rows,
        cohort_definitions=cohort_definitions,
        generated_at=generated_at,
        source_fingerprints=source_fingerprints,
        peer_metrics=peer_metrics,
        period_comparison=period_comparison,
    )
    prospective_population = build_prospective_population(
        prospective_rows,
        excluded_rows=excluded_rows,
        generated_at=generated_at,
        source_fingerprints=source_fingerprints,
    )
    prospective_results = build_prospective_results(prospective_rows, cohort_definitions=cohort_definitions, generated_at=generated_at)
    comparison = compare_discovery_vs_prospective(discovery_baselines, prospective_results, generated_at=generated_at)
    checkpoint_path = output_dir / "checkpoint_history.jsonl"
    checkpoints = merge_checkpoint_history(checkpoint_path, build_checkpoint_history(prospective_results, generated_at=generated_at))
    context_audit = build_context_coverage_audit(discovery_rows, prospective_rows, source_fingerprints=source_fingerprints, generated_at=generated_at)
    validation = validate_monitor_outputs(
        discovery_baselines=discovery_baselines,
        prospective_population=prospective_population,
        prospective_results=prospective_results,
        comparison=comparison,
        checkpoints=checkpoints,
        context_audit=context_audit,
        crr_validation=crr_validation,
        generated_at=generated_at,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "discovery_baselines": output_dir / "discovery_baselines.json",
        "prospective_population": output_dir / "prospective_population.json",
        "prospective_cohort_results": output_dir / "prospective_cohort_results.json",
        "discovery_vs_prospective": output_dir / "discovery_vs_prospective.json",
        "checkpoint_history": output_dir / "checkpoint_history.jsonl",
        "validation_report_json": output_dir / "validation_report.json",
        "validation_report_md": output_dir / "validation_report.md",
        "context_coverage_audit": output_dir / "context_coverage_audit.json",
        "html": output_dir / "prospective_nq_cohort_monitor.html",
    }
    _write_json(paths["discovery_baselines"], discovery_baselines)
    _write_json(paths["prospective_population"], prospective_population)
    _write_json(paths["prospective_cohort_results"], prospective_results)
    _write_json(paths["discovery_vs_prospective"], comparison)
    _write_jsonl(paths["checkpoint_history"], checkpoints)
    _write_json(paths["validation_report_json"], validation)
    paths["validation_report_md"].write_text(render_validation_markdown(validation), encoding="utf-8")
    _write_json(paths["context_coverage_audit"], context_audit)
    paths["html"].write_text(render_monitor_html(discovery_baselines, prospective_population, prospective_results, comparison, checkpoints, context_audit), encoding="utf-8")
    return ProspectiveMonitorResult(
        discovery_baselines=discovery_baselines,
        prospective_population=prospective_population,
        prospective_results=prospective_results,
        discovery_vs_prospective=comparison,
        checkpoint_history=checkpoints,
        validation_report=validation,
        context_coverage_audit=context_audit,
        output_paths=paths,
    )


def qualified_nq_rows(rows: Sequence[Mapping[str, Any]], *, eligibility_by_id: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    qualified: list[dict[str, Any]] = []
    for row in rows:
        if trade_identity(row).get("instrument") != "NQ":
            continue
        eligibility = eligibility_by_id.get(row.get("research_record_id"), {})
        if eligibility.get("classification") not in ELIGIBLE_CLASSIFICATIONS:
            continue
        if row.get("join_quality", {}).get("broken"):
            continue
        if _number(row.get("outcome_summary", {}).get("realized_pnl_proxy")) is None:
            continue
        joined = flatten_crr_row(row)
        joined["research_eligibility"] = dict(eligibility)
        qualified.append(joined)
    return sorted(qualified, key=lambda item: (row_timestamp(item), str(item.get("research_record_id"))))


def excluded_prospective_rows(rows: Sequence[Mapping[str, Any]], *, eligibility_by_id: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    excluded: list[dict[str, Any]] = []
    for row in rows:
        if trade_identity(row).get("instrument") != "NQ":
            continue
        if row_timestamp(row) < DISCOVERY_CUTOFF_UTC:
            continue
        reasons: list[str] = []
        eligibility = eligibility_by_id.get(row.get("research_record_id"), {})
        if eligibility.get("classification") not in ELIGIBLE_CLASSIFICATIONS:
            reasons.append(f"eligibility={eligibility.get('classification', 'MISSING')}")
        if row.get("join_quality", {}).get("broken"):
            reasons.append("broken_join")
        if _number(row.get("outcome_summary", {}).get("realized_pnl_proxy")) is None:
            reasons.append("missing_realized_pnl_proxy")
        if reasons:
            flat = flatten_crr_row(row)
            flat["exclusion_reasons"] = reasons
            excluded.append(flat)
    return sorted(excluded, key=lambda item: (row_timestamp(item), str(item.get("research_record_id"))))


def flatten_crr_row(row: Mapping[str, Any]) -> dict[str, Any]:
    identity = trade_identity(row)
    entry = row.get("entry_anchor", {})
    exit_anchor = row.get("exit_anchor", {})
    outcome = row.get("outcome_summary", {})
    enrichment = row.get("enrichment_ref", {}).get("context_validity_summary", {})
    path = row.get("path_ref", {})
    attribution = row.get("attribution_ref", {})
    return {
        "research_record_id": row.get("research_record_id"),
        "source_trade_id": identity.get("source_trade_id") or identity.get("trade_id"),
        "trade_id": identity.get("trade_id"),
        "lifecycle_id": identity.get("lifecycle_id"),
        "instrument": identity.get("instrument"),
        "contract": identity.get("contract"),
        "con_id": identity.get("con_id"),
        "quantity": identity.get("quantity"),
        "side": identity.get("side"),
        "strategy_id": entry.get("strategy_id"),
        "lane_id": entry.get("lane_id"),
        "entry_time": entry.get("entry_time"),
        "exit_time": exit_anchor.get("exit_time"),
        "exit_policy": exit_anchor.get("exit_policy"),
        "exit_reason": exit_anchor.get("exit_reason"),
        "realized_pnl_proxy": _number(outcome.get("realized_pnl_proxy")),
        "hold_seconds": _number(outcome.get("hold_seconds")),
        "mfe_points": _number(outcome.get("mfe_points")),
        "mae_points": _number(outcome.get("mae_points")),
        "giveback_points": _number(outcome.get("giveback_points")),
        "session": enrichment.get("session"),
        "regime": enrichment.get("market_context_validity_classification"),
        "vix_percentile": _number(enrichment.get("vix_percentile")),
        "gre_validity_classification": enrichment.get("gre_validity_classification"),
        "crfd_validity_classification": enrichment.get("crfd_validity_classification"),
        "vwap_avwap_context": enrichment.get("vwap_avwap_context"),
        "path_status": path.get("path_status"),
        "ra7_join_quality": path.get("ra7_join_quality"),
        "ra8_join_quality": path.get("ra8_join_quality"),
        "trade_decision_attribution_id": attribution.get("trade_decision_attribution_id"),
        "source_provenance": row.get("source_provenance", []),
        "source_fingerprint": row.get("deterministic_fingerprint"),
        "join_quality": row.get("join_quality", {}),
    }


def freeze_cohort_definitions(rows: Sequence[Mapping[str, Any]], peer_assignments: Mapping[str, Any]) -> list[dict[str, Any]]:
    definitions = [dict(item) for item in DISCOVERY_COHORTS]
    seen = {item["cohort_id"] for item in definitions}
    for assignment in peer_assignments.get("assignments", []):
        fields = list(assignment.get("selected_fields") or [])
        focal = assignment.get("focal_trade", {})
        if assignment.get("peer_count", 0) < 10 or not fields:
            continue
        values = {field: focal.get(field) for field in fields}
        values["instrument"] = "NQ"
        cohort_id = "inv_005_peer_" + hashlib.sha256(json.dumps(values, sort_keys=True, default=str).encode("utf-8")).hexdigest()[:16]
        if cohort_id in seen:
            continue
        if cohort_rows(rows, values):
            definitions.append({"cohort_id": cohort_id, "source": "INV-005_MIN_SAMPLE_PEER", "definition": values})
            seen.add(cohort_id)
    return definitions


def build_discovery_baselines(
    rows: Sequence[Mapping[str, Any]],
    *,
    cohort_definitions: Sequence[Mapping[str, Any]],
    generated_at: str,
    source_fingerprints: Mapping[str, Any],
    peer_metrics: Mapping[str, Any],
    period_comparison: Mapping[str, Any],
) -> dict[str, Any]:
    baselines = {
        definition["cohort_id"]: baseline_record(rows, definition=definition)
        for definition in cohort_definitions
    }
    payload = {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "generated_at": generated_at,
        "discovery_population": DISCOVERY_POPULATION_LABEL,
        "prospective_start": PROSPECTIVE_START_ET,
        "discovery_cutoff_utc": DISCOVERY_CUTOFF_UTC,
        "immutable_for_validation_cycle": True,
        "no_future_data_in_baseline": True,
        "baseline_count": len(baselines),
        "baselines": baselines,
        "inv_005_peer_metrics_fingerprint": peer_metrics.get("deterministic_fingerprint"),
        "inv_006_period_comparison_fingerprint": period_comparison.get("deterministic_fingerprint"),
        "source_fingerprints": dict(source_fingerprints),
        "raw_dollar_disclosure": "Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences; this monitor is NQ-only but still does not normalize contract economics.",
        "guardrails": dict(GUARDRAILS),
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def baseline_record(rows: Sequence[Mapping[str, Any]], *, definition: Mapping[str, Any]) -> dict[str, Any]:
    selected = cohort_rows(rows, definition.get("definition", {}))
    record = {
        "cohort_id": definition.get("cohort_id"),
        "source": definition.get("source"),
        "cohort_definition": definition.get("definition", {}),
        "discovery_trade_count": len(selected),
        "date_range": date_range(selected),
        "metrics": monitor_metrics(selected),
        "top_1_sensitivity": nq_percentile_contribution(selected, fraction=0.01, low=False),
        "top_5_sensitivity": nq_percentile_contribution(selected, fraction=0.05, low=False),
        "top_10_sensitivity": nq_percentile_contribution(selected, fraction=0.10, low=False),
        "contribution_concentration": concentration_metrics(selected),
        "rolling_10_trade_windows": rolling_window_metrics(selected, window_size=10) if len(selected) >= 10 else [],
        "rolling_20_trade_windows": rolling_window_metrics(selected, window_size=20) if len(selected) >= 20 else [],
        "rolling_50_trade_windows": rolling_window_metrics(selected, window_size=50) if len(selected) >= 50 else [],
        "ra8_coverage": cohort_evidence(selected)["ra8_coverage"],
        "missingness": _missing_field_counts(selected),
    }
    record["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(record))
    return record


def build_prospective_population(
    rows: Sequence[Mapping[str, Any]],
    *,
    excluded_rows: Sequence[Mapping[str, Any]],
    generated_at: str,
    source_fingerprints: Mapping[str, Any],
) -> dict[str, Any]:
    payload = {
        "schema_version": "nq_prospective_population_v1",
        "generated_at": generated_at,
        "prospective_start": PROSPECTIVE_START_ET,
        "discovery_cutoff_utc": DISCOVERY_CUTOFF_UTC,
        "included_count": len(rows),
        "excluded_count": len(excluded_rows),
        "included_trade_ids": [row.get("research_record_id") for row in rows],
        "excluded_trades": [
            {
                "research_record_id": row.get("research_record_id"),
                "source_trade_id": row.get("source_trade_id"),
                "exit_time": row.get("exit_time"),
                "exclusion_reasons": row.get("exclusion_reasons", []),
            }
            for row in excluded_rows
        ],
        "pending_unclassified_count": 0,
        "active_prospective_cutoff": generated_at,
        "source_fingerprints": dict(source_fingerprints),
        "guardrails": dict(GUARDRAILS),
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def build_prospective_results(rows: Sequence[Mapping[str, Any]], *, cohort_definitions: Sequence[Mapping[str, Any]], generated_at: str) -> dict[str, Any]:
    cohorts = {}
    for definition in cohort_definitions:
        selected = cohort_rows(rows, definition.get("definition", {}))
        cohorts[definition["cohort_id"]] = prospective_cohort_record(selected, definition=definition)
    payload = {
        "schema_version": "nq_prospective_cohort_results_v1",
        "generated_at": generated_at,
        "prospective_start": PROSPECTIVE_START_ET,
        "cohorts": cohorts,
        "overall": prospective_cohort_record(rows, definition={"cohort_id": "all_prospective_qualified_nq", "source": "PROSPECTIVE_POPULATION", "definition": {"instrument": "NQ"}}),
        "guardrails": dict(GUARDRAILS),
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def prospective_cohort_record(rows: Sequence[Mapping[str, Any]], *, definition: Mapping[str, Any]) -> dict[str, Any]:
    record = {
        "cohort_id": definition.get("cohort_id"),
        "source": definition.get("source"),
        "cohort_definition": definition.get("definition", {}),
        "prospective_trade_count": len(rows),
        "date_range": date_range(rows),
        "metrics": monitor_metrics(rows),
        "top_1_sensitivity": nq_percentile_contribution(rows, fraction=0.01, low=False),
        "top_5_sensitivity": nq_percentile_contribution(rows, fraction=0.05, low=False),
        "top_10_sensitivity": nq_percentile_contribution(rows, fraction=0.10, low=False),
        "results_excluding_largest_winner": metrics_excluding_top(rows, fraction=0.01),
        "results_excluding_top_10_percent": metrics_excluding_top(rows, fraction=0.10),
        "rolling_10_trade_windows": rolling_window_metrics(rows, window_size=10) if len(rows) >= 10 else [],
        "rolling_20_trade_windows": rolling_window_metrics(rows, window_size=20) if len(rows) >= 20 else [],
        "rolling_50_trade_windows": rolling_window_metrics(rows, window_size=50) if len(rows) >= 50 else [],
        "ra8_coverage": cohort_evidence(rows)["ra8_coverage"],
        "missingness": _missing_field_counts(rows),
        "validation_state": validation_state_for_count(len(rows), None),
    }
    record["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(record))
    return record


def compare_discovery_vs_prospective(discovery: Mapping[str, Any], prospective: Mapping[str, Any], *, generated_at: str) -> dict[str, Any]:
    comparisons = {}
    for cohort_id, baseline in discovery.get("baselines", {}).items():
        current = prospective.get("cohorts", {}).get(cohort_id, {})
        deltas = metric_deltas(baseline.get("metrics", {}), current.get("metrics", {}))
        comparisons[cohort_id] = {
            "cohort_id": cohort_id,
            "baseline_count": baseline.get("discovery_trade_count"),
            "prospective_count": current.get("prospective_trade_count", 0),
            "deltas": deltas,
            "validation_state": validation_state_for_count(current.get("prospective_trade_count", 0), deltas),
            "raw_dollar_disclosure": "Comparison uses raw realized P&L proxy, not normalized contract economics.",
        }
    payload = {
        "schema_version": "nq_discovery_vs_prospective_v1",
        "generated_at": generated_at,
        "prospective_start": PROSPECTIVE_START_ET,
        "comparisons": comparisons,
        "guardrails": dict(GUARDRAILS),
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def build_checkpoint_history(prospective: Mapping[str, Any], *, generated_at: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for cohort_id, cohort in prospective.get("cohorts", {}).items():
        count = cohort.get("prospective_trade_count", 0)
        for threshold in CHECKPOINTS:
            if count >= threshold:
                payload = {
                    "schema_version": "nq_prospective_checkpoint_v1",
                    "generated_at": generated_at,
                    "cohort_id": cohort_id,
                    "checkpoint_trade_count": threshold,
                    "cohort_population_fingerprint": cohort.get("deterministic_fingerprint"),
                    "metrics": cohort.get("metrics", {}),
                    "contradictory_evidence": contradictory_evidence_for_cohort(cohort),
                    "missingness": cohort.get("missingness", {}),
                    "confidence": confidence_for_count(threshold),
                    "validation_state": validation_state_for_count(count, None),
                    "guardrails": dict(GUARDRAILS),
                }
                payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
                records.append(payload)
    return records


def merge_checkpoint_history(path: Path, new_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    existing = _read_jsonl(path) if path.exists() else []
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for row in [*existing, *new_rows]:
        key = (str(row.get("cohort_id")), int(row.get("checkpoint_trade_count", 0)))
        if key in seen:
            continue
        merged.append(dict(row))
        seen.add(key)
    return sorted(merged, key=lambda item: (str(item.get("cohort_id")), int(item.get("checkpoint_trade_count", 0)), str(item.get("generated_at"))))


def build_context_coverage_audit(
    discovery_rows: Sequence[Mapping[str, Any]],
    prospective_rows: Sequence[Mapping[str, Any]],
    *,
    source_fingerprints: Mapping[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    fields = {
        "vwap_relationship": ("vwap_avwap_context", "outputs/track_b_execution_core/trade_outcome_enrichment/canonical_trade_outcome_enrichment.jsonl"),
        "avwap_relationship": ("vwap_avwap_context", "outputs/track_b_execution_core/trade_outcome_enrichment/canonical_trade_outcome_enrichment.jsonl"),
        "opening_range_position": ("opening_range_position", "not present in CRR v1"),
        "trend_state": ("trend_state", "not present in CRR v1"),
        "participation_state": ("strategy_id", "CRR entry_anchor.strategy_id"),
        "regime": ("regime", "CRR enrichment_ref.context_validity_summary.market_context_validity_classification"),
        "slope": ("slope", "not present in CRR v1"),
        "curvature": ("curvature", "not present in CRR v1"),
        "volatility_state": ("volatility_state", "not present in CRR v1"),
        "crfd": ("crfd_validity_classification", "CRR enrichment_ref.context_validity_summary.crfd_validity_classification"),
        "gre": ("gre_validity_classification", "CRR enrichment_ref.context_validity_summary.gre_validity_classification"),
        "strategy_setup_family": ("strategy_id", "CRR entry_anchor.strategy_id"),
    }
    records = {}
    for name, (field, source) in fields.items():
        discovery_coverage = field_coverage(discovery_rows, field)
        prospective_coverage = field_coverage(prospective_rows, field)
        if source.startswith("not present"):
            status = "ABSENT"
            requirement = "runtime producer change" if name in {"opening_range_position", "trend_state", "slope", "curvature", "volatility_state"} else "research producer change"
            historical = "not defensible from current CRR field set"
        elif discovery_coverage["available_count"] > 0 or prospective_coverage["available_count"] > 0:
            status = "AVAILABLE"
            requirement = "no code change"
            historical = "defensible where source fields are present"
        else:
            status = "AVAILABLE_ONLY_AS_VALIDITY_OR_UNAVAILABLE_STATE"
            requirement = "research producer change"
            historical = "not defensible for raw context values from current CRR field set"
        records[name] = {
            "field": field,
            "status": status,
            "discovery_coverage": discovery_coverage,
            "prospective_coverage": prospective_coverage,
            "source_artifact": source,
            "schema_field_path": field,
            "runtime_safe_to_capture_prospectively": requirement != "Architecture Track decision",
            "implementation_requirement": requirement,
            "historical_reconstruction": historical,
        }
    payload = {
        "schema_version": "prospective_market_context_coverage_audit_v1",
        "generated_at": generated_at,
        "fields": records,
        "source_fingerprints": dict(source_fingerprints),
        "guardrails": dict(GUARDRAILS),
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def validate_monitor_outputs(
    *,
    discovery_baselines: Mapping[str, Any],
    prospective_population: Mapping[str, Any],
    prospective_results: Mapping[str, Any],
    comparison: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
    context_audit: Mapping[str, Any],
    crr_validation: Mapping[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    if not discovery_baselines.get("immutable_for_validation_cycle"):
        errors.append("discovery_baselines_not_immutable")
    if prospective_population.get("included_count", 0) < 10:
        warnings.append("not_enough_prospective_data")
    if crr_validation.get("status") not in {"VALID", "VALID_WITH_WARNINGS"}:
        errors.append("crr_validation_not_usable")
    for name, payload in {
        "discovery_baselines": discovery_baselines,
        "prospective_population": prospective_population,
        "prospective_results": prospective_results,
        "comparison": comparison,
        "context_audit": context_audit,
    }.items():
        guardrails = payload.get("guardrails", {})
        if guardrails.get("diagnostic_only") is not True or guardrails.get("production_recommendation") is not False or guardrails.get("trading_gate") is not False:
            errors.append(f"{name}_guardrails_invalid")
    payload = {
        "schema_version": "nq_prospective_monitor_validation_v1",
        "generated_at": generated_at,
        "status": "INVALID" if errors else ("VALID_WITH_WARNINGS" if warnings else "VALID"),
        "errors": errors,
        "warnings": warnings,
        "prospective_trade_count": prospective_population.get("included_count", 0),
        "checkpoint_count": len(checkpoints),
        "guardrails": dict(GUARDRAILS),
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def monitor_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    metrics = investigation_metrics(rows)
    values = sorted([value for value in (_number(row.get("realized_pnl_proxy")) for row in rows) if value is not None])
    metrics["trimmed_mean"] = trimmed(values, 0.05)
    metrics["expectancy"] = metrics.get("average_realized_pnl_proxy")
    return metrics


def metric_deltas(baseline: Mapping[str, Any], current: Mapping[str, Any]) -> dict[str, Any]:
    fields = ("median_realized_pnl_proxy", "trimmed_mean", "win_rate", "expectancy", "payoff_ratio")
    deltas = {}
    for field in fields:
        before = _number(baseline.get(field))
        after = _number(current.get(field))
        deltas[field] = {
            "discovery": before,
            "prospective": after,
            "absolute_delta": None if before is None or after is None else round(after - before, 6),
            "percentage_delta": None if before in (None, 0) or after is None else round((after - before) / abs(before), 6),
        }
    deltas["concentration"] = {
        "discovery": baseline.get("top_10_sensitivity", {}).get("contribution_to_total_pnl"),
        "prospective": current.get("top_10_sensitivity", {}).get("contribution_to_total_pnl"),
    }
    return deltas


def validation_state_for_count(count: int, deltas: Mapping[str, Any] | None) -> str:
    if count < 10:
        return "NOT_ENOUGH_PROSPECTIVE_DATA"
    if count < 20:
        return "EARLY_MIXED_EVIDENCE"
    if count < 50:
        return "PROSPECTIVE_CONFIRMATION_STRENGTHENING" if average_delta_positive(deltas) else "PROSPECTIVE_CONFIRMATION_WEAKENING"
    return "PROSPECTIVE_CONFIRMATION_STRENGTHENING" if average_delta_positive(deltas) else "INCONCLUSIVE"


def average_delta_positive(deltas: Mapping[str, Any] | None) -> bool:
    if not deltas:
        return False
    average = deltas.get("expectancy", {}).get("absolute_delta")
    return bool(average is not None and average > 0)


def cohort_rows(rows: Sequence[Mapping[str, Any]], definition: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    selected = []
    for row in rows:
        if all(str(row.get(field) or "") == str(value) for field, value in definition.items() if value not in (None, "")):
            selected.append(row)
    return selected


def date_range(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    stamps = sorted(row_timestamp(row) for row in rows if row_timestamp(row))
    return {"start": stamps[0] if stamps else None, "end": stamps[-1] if stamps else None}


def row_timestamp(row: Mapping[str, Any]) -> str:
    return str(row.get("exit_time") or row.get("exit_anchor", {}).get("exit_time") or "")


def trade_identity(row: Mapping[str, Any]) -> Mapping[str, Any]:
    return row.get("trade_identity", {}) if isinstance(row.get("trade_identity"), Mapping) else row


def concentration_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    total = sum(value for value in (_number(row.get("realized_pnl_proxy")) for row in rows) if value is not None)
    top_10 = nq_percentile_contribution(rows, fraction=0.10, low=False)
    return {
        "total_realized_pnl_proxy": round(total, 6),
        "top_10_contribution_to_total_pnl": top_10.get("contribution_to_total_pnl"),
        "top_10_trade_count": top_10.get("trade_count"),
    }


def metrics_excluding_top(rows: Sequence[Mapping[str, Any]], *, fraction: float) -> dict[str, Any]:
    if not rows:
        return monitor_metrics([])
    count = max(1, int(len(rows) * fraction))
    excluded = {row.get("research_record_id") for row in sorted(rows, key=lambda item: (_number(item.get("realized_pnl_proxy")) or 0.0, str(item.get("research_record_id"))), reverse=True)[:count]}
    return monitor_metrics([row for row in rows if row.get("research_record_id") not in excluded])


def field_coverage(rows: Sequence[Mapping[str, Any]], field: str) -> dict[str, Any]:
    available = [row for row in rows if row.get(field) not in (None, "", "UNAVAILABLE", "UNKNOWN")]
    return {
        "total_count": len(rows),
        "available_count": len(available),
        "missing_count": len(rows) - len(available),
        "coverage_rate": None if not rows else round(len(available) / len(rows), 6),
    }


def contradictory_evidence_for_cohort(cohort: Mapping[str, Any]) -> list[str]:
    items = ["Prospective monitoring is descriptive only and does not authorize strategy changes."]
    if cohort.get("ra8_coverage", {}).get("missing", 0):
        items.append("RA8 path coverage remains incomplete for some rows.")
    if cohort.get("prospective_trade_count", 0) < 50:
        items.append("Prospective sample remains below rolling-window validation threshold.")
    return items


def confidence_for_count(count: int) -> str:
    if count >= 50:
        return "DEVELOPING"
    if count >= 20:
        return "PRELIMINARY"
    if count >= 10:
        return "EARLY_DESCRIPTIVE"
    return "INSUFFICIENT_DATA"


def trimmed(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    if len(values) < 3:
        return round(sum(values) / len(values), 6)
    trim_count = int(len(values) * fraction)
    kept = list(values)[trim_count: len(values) - trim_count] if trim_count else list(values)
    if not kept:
        kept = list(values)
    return round(sum(kept) / len(kept), 6)


def build_source_fingerprints(paths: Mapping[str, Path]) -> dict[str, Any]:
    fingerprints = {}
    for name, path in paths.items():
        fingerprints[name] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None,
        }
    return fingerprints


def render_validation_markdown(validation: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# NQ Prospective Cohort Monitor Validation",
            "",
            f"Status: `{validation.get('status')}`",
            "",
            f"Prospective trades: `{validation.get('prospective_trade_count')}`",
            "",
            "## Warnings",
            "",
            *[f"- `{item}`" for item in validation.get("warnings", [])],
            "",
            "## Errors",
            "",
            *[f"- `{item}`" for item in validation.get("errors", [])],
            "",
            "## Guardrails",
            "",
            "- `diagnostic_only=true`",
            "- `production_recommendation=false`",
            "- `trading_gate=false`",
            "- `live_money_eligible=false`",
        ]
    )


def render_monitor_html(
    discovery: Mapping[str, Any],
    population: Mapping[str, Any],
    prospective: Mapping[str, Any],
    comparison: Mapping[str, Any],
    checkpoints: Sequence[Mapping[str, Any]],
    context: Mapping[str, Any],
) -> str:
    baseline_rows = "".join(
        "<tr>"
        f"<td>{html.escape(cohort_id)}</td>"
        f"<td>{baseline.get('discovery_trade_count')}</td>"
        f"<td>{fmt(baseline.get('metrics', {}).get('total_realized_pnl_proxy'))}</td>"
        f"<td>{fmt(baseline.get('metrics', {}).get('average_realized_pnl_proxy'))}</td>"
        f"<td>{fmt(baseline.get('metrics', {}).get('median_realized_pnl_proxy'))}</td>"
        f"<td>{html.escape(str(baseline.get('metrics', {}).get('win_rate')))}</td>"
        "</tr>"
        for cohort_id, baseline in discovery.get("baselines", {}).items()
    )
    comparison_rows = "".join(
        "<tr>"
        f"<td>{html.escape(cohort_id)}</td>"
        f"<td>{item.get('prospective_count')}</td>"
        f"<td>{html.escape(str(item.get('validation_state')))}</td>"
        f"<td>{html.escape(str(item.get('deltas', {}).get('expectancy', {}).get('absolute_delta')))}</td>"
        "</tr>"
        for cohort_id, item in comparison.get("comparisons", {}).items()
    )
    context_rows = "".join(
        "<tr>"
        f"<td>{html.escape(name)}</td>"
        f"<td>{html.escape(str(item.get('status')))}</td>"
        f"<td>{item.get('discovery_coverage', {}).get('available_count')}/{item.get('discovery_coverage', {}).get('total_count')}</td>"
        f"<td>{item.get('prospective_coverage', {}).get('available_count')}/{item.get('prospective_coverage', {}).get('total_count')}</td>"
        f"<td>{html.escape(str(item.get('implementation_requirement')))}</td>"
        "</tr>"
        for name, item in context.get("fields", {}).items()
    )
    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>NQ Prospective Cohort Monitor</title>
<style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:24px;background:#f6f7f4;color:#1f2933}}main{{max-width:1320px;margin:auto}}section{{background:white;border:1px solid #d9e0df;border-radius:6px;margin:16px 0;padding:16px}}table{{width:100%;border-collapse:collapse;font-size:13px}}th,td{{border-bottom:1px solid #e7ecea;padding:7px;text-align:left;vertical-align:top}}th{{background:#eef2ef}}.warn{{color:#8a5a00;font-weight:700}}</style>
</head><body><main>
<h1>NQ Prospective Cohort Monitor</h1>
<p class="warn">Offline descriptive research only. No production recommendation, trading gate, runtime access, or broker authority.</p>
<section><h2>Frozen Discovery Contract</h2><p>{html.escape(DISCOVERY_POPULATION_LABEL)}</p><p>Prospective start: <strong>{html.escape(PROSPECTIVE_START_ET)}</strong></p><p>Current prospective trades: <strong>{population.get('included_count')}</strong></p></section>
<section><h2>Discovery Baselines</h2><table><thead><tr><th>Cohort</th><th>Trades</th><th>Total</th><th>Average</th><th>Median</th><th>Win Rate</th></tr></thead><tbody>{baseline_rows}</tbody></table></section>
<section><h2>Discovery vs Prospective</h2><table><thead><tr><th>Cohort</th><th>Prospective Trades</th><th>Validation State</th><th>Expectancy Delta</th></tr></thead><tbody>{comparison_rows}</tbody></table></section>
<section><h2>Checkpoint Progress</h2><p>{len(checkpoints)} deterministic checkpoint rows currently available.</p></section>
<section><h2>Context Coverage</h2><table><thead><tr><th>Field</th><th>Status</th><th>Discovery</th><th>Prospective</th><th>Requirement</th></tr></thead><tbody>{context_rows}</tbody></table></section>
<section><h2>Comparability Warning</h2><p>Raw realized P&L proxy can reflect contract economics, quantity, and multiplier effects. This monitor is NQ-only but does not infer missing normalization evidence.</p></section>
</main></body></html>
"""


def fmt(value: Any) -> str:
    number = _number(value)
    return "MISSING" if number is None else f"{number:,.2f}"


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
