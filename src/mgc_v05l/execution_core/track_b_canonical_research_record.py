"""Canonical Research Record v1.

This module builds a derived, deterministic research-facing contract over
existing Track B artifacts. It is diagnostic-only and has no broker, runtime,
strategy, Managed Exit, Guardian, Safe-State, readiness, reconciliation, or
trading-gate authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_trade_path_layer import (
    CANONICAL_TRADE_PATHS_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_RA7_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_live_trade_path_accumulator import (
    DEFAULT_OUTPUT_DIR as DEFAULT_RA8_OUTPUT_DIR,
    FINALIZED_CAPTURE_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_decision_attribution import (
    ATTRIBUTION_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_RA3_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOE_OUTPUT_DIR,
    ENRICHMENT_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_CANONICAL_TRADE_RECORDS,
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CANONICAL_RECORDS_PATH = DEFAULT_CANONICAL_TRADE_RECORDS
DEFAULT_OUTCOMES_PATH = DEFAULT_CTOL_OUTPUT_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_CTOE_OUTPUT_DIR / ENRICHMENT_JSONL
DEFAULT_TRADE_PATHS_PATH = DEFAULT_RA7_OUTPUT_DIR / CANONICAL_TRADE_PATHS_JSONL
DEFAULT_ATTRIBUTIONS_PATH = DEFAULT_RA3_OUTPUT_DIR / ATTRIBUTION_JSONL
DEFAULT_FINALIZED_CAPTURES_PATH = DEFAULT_RA8_OUTPUT_DIR / FINALIZED_CAPTURE_JSONL
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "canonical_research_record"

CRR_JSONL = "canonical_research_records.jsonl"
SCHEMA_JSON = "canonical_research_record_schema.json"
VALIDATION_JSON = "canonical_research_record_validation_report.json"
VALIDATION_MD = "canonical_research_record_validation_report.md"
SUMMARY_MD = "canonical_research_record_summary.md"

SCHEMA_VERSION = "canonical_research_record_v1"
VALIDATION_SCHEMA_VERSION = "canonical_research_record_validation_report_v1"
SUMMARY_SCHEMA_VERSION = "canonical_research_record_summary_v1"

GUARDRAILS = {
    "diagnostic_only": True,
    "production_recommendation": False,
    "trading_gate": False,
}


@dataclass(frozen=True)
class CanonicalResearchRecordResult:
    rows: list[dict[str, Any]]
    validation: dict[str, Any]
    summary: dict[str, Any]
    records_path: Path
    schema_path: Path
    validation_json_path: Path
    validation_markdown_path: Path
    summary_markdown_path: Path


def run_canonical_research_record(
    *,
    canonical_records_path: Path = DEFAULT_CANONICAL_RECORDS_PATH,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    trade_paths_path: Path = DEFAULT_TRADE_PATHS_PATH,
    attributions_path: Path = DEFAULT_ATTRIBUTIONS_PATH,
    finalized_captures_path: Path = DEFAULT_FINALIZED_CAPTURES_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> CanonicalResearchRecordResult:
    generated_at = _coerce_now(now)
    source_paths = {
        "canonical_trade_records": canonical_records_path,
        "ctol": outcomes_path,
        "ctoe": enrichments_path,
        "ra7_canonical_trade_paths": trade_paths_path,
        "ra3_trade_decision_attribution": attributions_path,
        "ra8_finalized_capture": finalized_captures_path,
    }
    canonical_records = _read_jsonl(canonical_records_path)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    trade_paths = _read_jsonl(trade_paths_path)
    attributions = _read_jsonl(attributions_path)
    finalized_captures = _read_jsonl(finalized_captures_path)
    rows = build_canonical_research_records(
        canonical_records,
        outcomes=outcomes,
        enrichments=enrichments,
        trade_paths=trade_paths,
        attributions=attributions,
        finalized_captures=finalized_captures,
        generated_at=generated_at,
        source_paths=source_paths,
    )
    validation = validate_canonical_research_records(
        rows,
        canonical_records=canonical_records,
        outcomes=outcomes,
        generated_at=generated_at,
        source_paths=source_paths,
    )
    summary = build_canonical_research_record_summary(rows, validation=validation, generated_at=generated_at, source_paths=source_paths)

    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / CRR_JSONL
    schema_path = output_dir / SCHEMA_JSON
    validation_json_path = output_dir / VALIDATION_JSON
    validation_markdown_path = output_dir / VALIDATION_MD
    summary_markdown_path = output_dir / SUMMARY_MD
    _write_jsonl(records_path, rows)
    _write_json(schema_path, canonical_research_record_schema())
    _write_json(validation_json_path, validation)
    validation_markdown_path.write_text(render_validation_markdown(validation), encoding="utf-8")
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    return CanonicalResearchRecordResult(
        rows=rows,
        validation=validation,
        summary=summary,
        records_path=records_path,
        schema_path=schema_path,
        validation_json_path=validation_json_path,
        validation_markdown_path=validation_markdown_path,
        summary_markdown_path=summary_markdown_path,
    )


def build_canonical_research_records(
    canonical_records: Sequence[Mapping[str, Any]],
    *,
    outcomes: Sequence[Mapping[str, Any]] = (),
    enrichments: Sequence[Mapping[str, Any]] = (),
    trade_paths: Sequence[Mapping[str, Any]] = (),
    attributions: Sequence[Mapping[str, Any]] = (),
    finalized_captures: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    outcome_index = _OutcomeIndex(outcomes)
    enrichment_index = _ByOutcomeIndex(enrichments)
    path_index = _PathIndex(trade_paths)
    attribution_index = _ByOutcomeIndex(attributions)
    capture_index = _CaptureIndex(finalized_captures)
    provenance_templates = _source_provenance_templates(source_paths or {})
    rows: list[dict[str, Any]] = []
    for canonical in canonical_records:
        if not _is_completed_paired_trade(canonical):
            continue
        source_trade_id = _source_trade_id(canonical)
        capture_id = _capture_id(canonical)
        join_quality = _empty_join_quality()
        broken: list[dict[str, Any]] = []

        outcome, outcome_join = outcome_index.find(canonical)
        _record_join(join_quality, "ctol", outcome_join)
        enrichment, enrichment_join = enrichment_index.find(_str_or_none(outcome.get("trade_outcome_id")) if outcome else None)
        _record_join(join_quality, "ctoe", enrichment_join)
        path, path_join = path_index.find(outcome=outcome, canonical=canonical, capture_id=capture_id)
        _record_join(join_quality, "ra7", path_join)
        attribution, attribution_join = attribution_index.find(_str_or_none(outcome.get("trade_outcome_id")) if outcome else None)
        _record_join(join_quality, "ra3", attribution_join)
        finalized_capture, capture_join = capture_index.find(capture_id=capture_id, source_trade_id=source_trade_id)
        _record_join(join_quality, "ra8", capture_join)

        broken.extend(_broken_exact_refs(canonical=canonical, outcome=outcome, path=path, finalized_capture=finalized_capture))
        for item in broken:
            join_quality["broken"].append(item)
        join_quality["overall"] = _overall_join_quality(join_quality)

        row = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "research_record_id": _stable_id("canonical_research_record", source_trade_id, canonical.get("entry_time"), canonical.get("exit_time")),
            "trade_identity": _trade_identity(canonical),
            "entry_anchor": _entry_anchor(canonical),
            "exit_anchor": _exit_anchor(canonical),
            "outcome_ref": _outcome_ref(outcome, outcome_join, source_paths or {}),
            "outcome_summary": _outcome_summary(outcome),
            "enrichment_ref": _enrichment_ref(enrichment, enrichment_join, source_paths or {}),
            "path_ref": _path_ref(path, finalized_capture, capture_id, path_join, capture_join, source_paths or {}),
            "attribution_ref": _attribution_ref(attribution, attribution_join, source_paths or {}),
            "join_quality": join_quality,
            "source_provenance": _row_provenance(
                provenance_templates,
                canonical=canonical,
                outcome=outcome,
                enrichment=enrichment,
                path=path,
                attribution=attribution,
                finalized_capture=finalized_capture,
            ),
            "reconciliation": {
                "status": "NOT_VALIDATED",
                "mismatches": [],
            },
            "guardrails": dict(GUARDRAILS),
            **GUARDRAILS,
        }
        row["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(row))
        rows.append(row)
    rows.sort(key=lambda row: (str(row["exit_anchor"].get("exit_time") or ""), str(row.get("research_record_id") or "")))
    return rows


def validate_canonical_research_records(
    rows: Sequence[Mapping[str, Any]],
    *,
    canonical_records: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    expected = sum(1 for row in canonical_records if _is_completed_paired_trade(row))
    broken = [item for row in rows for item in row.get("join_quality", {}).get("broken", [])]
    missing = [item for row in rows for item in row.get("join_quality", {}).get("missing", [])]
    mismatches: list[dict[str, Any]] = []
    canonical_by_trade_id = {_source_trade_id(row): row for row in canonical_records if _source_trade_id(row)}
    outcome_by_id = {str(row.get("trade_outcome_id")): row for row in outcomes if row.get("trade_outcome_id")}
    for row in rows:
        source_trade_id = row.get("trade_identity", {}).get("source_trade_id")
        canonical = canonical_by_trade_id.get(str(source_trade_id or ""))
        if canonical:
            mismatches.extend(_anchor_mismatches(row, canonical))
        outcome_id = row.get("outcome_ref", {}).get("trade_outcome_id")
        outcome = outcome_by_id.get(str(outcome_id or ""))
        if outcome:
            mismatches.extend(_outcome_mismatches(row, outcome))
    if len(rows) != expected:
        mismatches.append({"layer": "canonical_trade_records", "field": "row_count", "expected": expected, "actual": len(rows)})
    if broken:
        status = "INVALID_BROKEN_JOIN"
    elif mismatches:
        status = "INVALID_RECONCILIATION_MISMATCH"
    elif missing:
        status = "VALID_WITH_WARNINGS"
    else:
        status = "VALID"
    return {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "status": status,
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "counts": {
            "expected_completed_canonical_records": expected,
            "canonical_research_records": len(rows),
            "missing_join_count": len(missing),
            "broken_join_count": len(broken),
            "reconciliation_mismatch_count": len(mismatches),
        },
        "join_quality": _join_quality_counts(rows),
        "missing_by_layer": _items_by_layer(missing),
        "broken_by_layer": _items_by_layer(broken),
        "reconciliation_mismatches": mismatches[:50],
        "valid_for_research": status in {"VALID", "VALID_WITH_WARNINGS"},
    }


def build_canonical_research_record_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    validation: Mapping[str, Any],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "canonical_research_record_count": len(rows),
            "validation_status": validation.get("status"),
            "valid_for_research": validation.get("valid_for_research"),
            "exact_ctol_count": _layer_join_count(rows, "ctol", "exact"),
            "exact_ctoe_count": _layer_join_count(rows, "ctoe", "exact"),
            "exact_ra7_count": _layer_join_count(rows, "ra7", "exact"),
            "exact_ra3_count": _layer_join_count(rows, "ra3", "exact"),
            "exact_ra8_count": _layer_join_count(rows, "ra8", "exact"),
        },
        "join_quality": validation.get("join_quality", {}),
        "missing_by_layer": validation.get("missing_by_layer", {}),
        "broken_by_layer": validation.get("broken_by_layer", {}),
    }


def canonical_research_record_schema() -> dict[str, Any]:
    return {
        "title": "CanonicalResearchRecord",
        "schema_version": SCHEMA_VERSION,
        "required": [
            "schema_version",
            "research_record_id",
            "trade_identity",
            "entry_anchor",
            "exit_anchor",
            "outcome_ref",
            "join_quality",
            "source_provenance",
            "guardrails",
            "deterministic_fingerprint",
        ],
        "guardrails": dict(GUARDRAILS),
    }


def render_validation_markdown(validation: Mapping[str, Any]) -> str:
    counts = validation.get("counts", {})
    lines = [
        "# Canonical Research Record Validation Report",
        "",
        f"- Status: `{validation.get('status')}`",
        f"- Valid for research: `{validation.get('valid_for_research')}`",
        f"- Expected completed canonical records: `{counts.get('expected_completed_canonical_records')}`",
        f"- Canonical research records: `{counts.get('canonical_research_records')}`",
        f"- Missing joins: `{counts.get('missing_join_count')}`",
        f"- Broken joins: `{counts.get('broken_join_count')}`",
        f"- Reconciliation mismatches: `{counts.get('reconciliation_mismatch_count')}`",
        "",
        "## Missing By Layer",
        "",
        _dict_table(validation.get("missing_by_layer", {})),
        "",
        "## Broken By Layer",
        "",
        _dict_table(validation.get("broken_by_layer", {})),
    ]
    return "\n".join(lines) + "\n"


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    lines = [
        "# Canonical Research Record Summary",
        "",
        f"- Records: `{overall.get('canonical_research_record_count')}`",
        f"- Validation status: `{overall.get('validation_status')}`",
        f"- Exact CTOL joins: `{overall.get('exact_ctol_count')}`",
        f"- Exact CTOE joins: `{overall.get('exact_ctoe_count')}`",
        f"- Exact RA7 joins: `{overall.get('exact_ra7_count')}`",
        f"- Exact RA3 joins: `{overall.get('exact_ra3_count')}`",
        f"- Exact RA8 joins: `{overall.get('exact_ra8_count')}`",
        "",
        "CRR is derived, diagnostic-only research infrastructure and has no runtime, broker, Managed Exit, strategy, or trading-gate authority.",
    ]
    return "\n".join(lines) + "\n"


class _OutcomeIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_source_trade_id: dict[str, Mapping[str, Any]] = {}
        self._by_entry_trade_id: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            source_trade_id = _source_ref(row, "source_trade_id")
            if source_trade_id:
                self._by_source_trade_id[source_trade_id] = row
            for key in ("entry_trade_id", "exit_trade_id"):
                value = _str_or_none(row.get(key))
                if value:
                    self._by_entry_trade_id[value] = row

    def find(self, canonical: Mapping[str, Any]) -> tuple[Mapping[str, Any] | None, dict[str, Any]]:
        source_trade_id = _source_trade_id(canonical)
        if source_trade_id and source_trade_id in self._by_source_trade_id:
            return self._by_source_trade_id[source_trade_id], _exact_join("ctol", "source_refs.source_trade_id")
        if source_trade_id and source_trade_id in self._by_entry_trade_id:
            return self._by_entry_trade_id[source_trade_id], _exact_join("ctol", "entry_trade_id/exit_trade_id")
        return None, _missing_join("ctol", "No exact CTOL outcome by source_trade_id or trade id.")


class _ByOutcomeIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_outcome_id = {str(row.get("trade_outcome_id")): row for row in rows if row.get("trade_outcome_id")}

    def find(self, trade_outcome_id: str | None) -> tuple[Mapping[str, Any] | None, dict[str, Any]]:
        if trade_outcome_id and trade_outcome_id in self._by_outcome_id:
            return self._by_outcome_id[trade_outcome_id], _exact_join("", "trade_outcome_id")
        return None, _missing_join("", "No exact row by trade_outcome_id.")


class _PathIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_outcome_id = {str(row.get("trade_outcome_id")): row for row in rows if row.get("trade_outcome_id")}
        self._by_source_trade_id = {str(row.get("source_trade_id")): row for row in rows if row.get("source_trade_id")}
        self._by_capture_id: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            refs = row.get("provenance", {}).get("source_refs", {}) if isinstance(row.get("provenance"), Mapping) else {}
            capture_id = _str_or_none(refs.get("capture_id") or refs.get("retained_path_capture_id"))
            if capture_id:
                self._by_capture_id[capture_id] = row

    def find(
        self,
        *,
        outcome: Mapping[str, Any] | None,
        canonical: Mapping[str, Any],
        capture_id: str | None,
    ) -> tuple[Mapping[str, Any] | None, dict[str, Any]]:
        outcome_id = _str_or_none(outcome.get("trade_outcome_id")) if outcome else None
        if outcome_id and outcome_id in self._by_outcome_id:
            return self._by_outcome_id[outcome_id], _exact_join("ra7", "trade_outcome_id")
        source_trade_id = _source_trade_id(canonical)
        if source_trade_id and source_trade_id in self._by_source_trade_id:
            return self._by_source_trade_id[source_trade_id], _exact_join("ra7", "source_trade_id")
        if capture_id and capture_id in self._by_capture_id:
            return self._by_capture_id[capture_id], _exact_join("ra7", "capture_id")
        return None, _missing_join("ra7", "No exact RA7 canonical path by trade_outcome_id, source_trade_id, or capture_id.")


class _CaptureIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._by_capture_id = {str(row.get("capture_id")): row for row in rows if row.get("capture_id")}
        self._by_source_trade_id = {str(row.get("source_trade_id")): row for row in rows if row.get("source_trade_id")}

    def find(self, *, capture_id: str | None, source_trade_id: str | None) -> tuple[Mapping[str, Any] | None, dict[str, Any]]:
        if capture_id and capture_id in self._by_capture_id:
            return self._by_capture_id[capture_id], _exact_join("ra8", "capture_id")
        if source_trade_id and source_trade_id in self._by_source_trade_id:
            return self._by_source_trade_id[source_trade_id], _exact_join("ra8", "source_trade_id")
        return None, _missing_join("ra8", "No exact RA8 finalized capture by capture_id or source_trade_id.")


def _trade_identity(canonical: Mapping[str, Any]) -> dict[str, Any]:
    path_identity = _path_capture_trade_identity(canonical)
    return {
        "trade_id": canonical.get("trade_id"),
        "source_trade_id": _source_trade_id(canonical),
        "lifecycle_id": canonical.get("lifecycle_id") or path_identity.get("lifecycle_id"),
        "instrument": canonical.get("instrument") or canonical.get("symbol") or path_identity.get("instrument"),
        "contract": canonical.get("contract") or canonical.get("local_symbol") or path_identity.get("contract"),
        "con_id": canonical.get("con_id"),
        "side": canonical.get("side") or canonical.get("entry_side") or path_identity.get("side"),
        "quantity": canonical.get("quantity") or canonical.get("qty") or path_identity.get("quantity"),
    }


def _entry_anchor(canonical: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "entry_time": canonical.get("entry_time"),
        "entry_price": canonical.get("entry_price"),
        "entry_order_id": canonical.get("entry_order_id"),
        "entry_perm_id": canonical.get("entry_perm_id"),
        "entry_exec_id": canonical.get("entry_exec_id"),
        "strategy_id": canonical.get("strategy_id"),
        "lane_id": canonical.get("lane_id"),
        "source": "canonical_trade_records",
        "cache_reconciled": True,
    }


def _exit_anchor(canonical: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "exit_time": canonical.get("exit_time"),
        "exit_price": canonical.get("exit_price"),
        "exit_order_id": canonical.get("exit_order_id"),
        "exit_perm_id": canonical.get("exit_perm_id"),
        "exit_exec_id": canonical.get("exit_exec_id"),
        "exit_policy": canonical.get("exit_policy"),
        "exit_reason": canonical.get("exit_reason"),
        "source": "canonical_trade_records",
        "cache_reconciled": True,
    }


def _outcome_ref(outcome: Mapping[str, Any] | None, join: Mapping[str, Any], source_paths: Mapping[str, Path | str]) -> dict[str, Any]:
    return {
        "trade_outcome_id": outcome.get("trade_outcome_id") if outcome else None,
        "path": str(source_paths.get("ctol", "")) if outcome else None,
        "fingerprint": _record_fingerprint(outcome) if outcome else None,
        "join_quality": join.get("quality"),
        "resolution_method": join.get("method"),
    }


def _outcome_summary(outcome: Mapping[str, Any] | None) -> dict[str, Any]:
    if not outcome:
        return {}
    return {
        "realized_points": outcome.get("realized_points"),
        "realized_pnl_proxy": outcome.get("realized_pnl_proxy"),
        "hold_seconds": outcome.get("hold_seconds"),
        "mfe_points": outcome.get("mfe_points"),
        "mae_points": outcome.get("mae_points"),
        "path_status": outcome.get("path_status"),
        "path_available": outcome.get("path_available"),
        "path_complete": outcome.get("path_complete"),
        "data_quality_flags": outcome.get("data_quality_flags") or [],
    }


def _enrichment_ref(enrichment: Mapping[str, Any] | None, join: Mapping[str, Any], source_paths: Mapping[str, Path | str]) -> dict[str, Any]:
    return {
        "trade_outcome_id": enrichment.get("trade_outcome_id") if enrichment else None,
        "path": str(source_paths.get("ctoe", "")) if enrichment else None,
        "context_validity_summary": _context_validity_summary(enrichment),
        "join_quality": join.get("quality"),
        "resolution_method": join.get("method"),
    }


def _context_validity_summary(enrichment: Mapping[str, Any] | None) -> dict[str, Any]:
    if not enrichment:
        return {}
    keys = (
        "market_context_validity_classification",
        "gre_validity_classification",
        "crfd_validity_classification",
        "vwap_avwap_validity_classification",
        "vix_percentile",
        "session",
    )
    return {key: enrichment.get(key) for key in keys if key in enrichment}


def _path_ref(
    path: Mapping[str, Any] | None,
    finalized_capture: Mapping[str, Any] | None,
    capture_id: str | None,
    path_join: Mapping[str, Any],
    capture_join: Mapping[str, Any],
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    return {
        "capture_id": capture_id or (finalized_capture.get("capture_id") if finalized_capture else None),
        "canonical_trade_path_id": path.get("canonical_trade_path_id") if path else None,
        "path_status": path.get("path_coverage_status") if path else finalized_capture.get("coverage_status") if finalized_capture else None,
        "path_fingerprint": path.get("deterministic_fingerprint") if path else finalized_capture.get("deterministic_fingerprint") if finalized_capture else None,
        "ra7_path": str(source_paths.get("ra7_canonical_trade_paths", "")) if path else None,
        "ra8_finalized_capture_path": str(source_paths.get("ra8_finalized_capture", "")) if finalized_capture else None,
        "ra7_join_quality": path_join.get("quality"),
        "ra8_join_quality": capture_join.get("quality"),
    }


def _attribution_ref(attribution: Mapping[str, Any] | None, join: Mapping[str, Any], source_paths: Mapping[str, Path | str]) -> dict[str, Any]:
    entry = attribution.get("entry") if isinstance(attribution, Mapping) and isinstance(attribution.get("entry"), Mapping) else {}
    exit_attr = attribution.get("exit") if isinstance(attribution, Mapping) and isinstance(attribution.get("exit"), Mapping) else {}
    return {
        "trade_decision_attribution_id": attribution.get("trade_decision_attribution_id") if attribution else None,
        "path": str(source_paths.get("ra3_trade_decision_attribution", "")) if attribution else None,
        "entry_attribution_status": _status_from_unknown(entry),
        "exit_attribution_status": _status_from_unknown(exit_attr),
        "join_quality": join.get("quality"),
        "resolution_method": join.get("method"),
    }


def _row_provenance(
    templates: Mapping[str, Mapping[str, Any]],
    *,
    canonical: Mapping[str, Any],
    outcome: Mapping[str, Any] | None,
    enrichment: Mapping[str, Any] | None,
    path: Mapping[str, Any] | None,
    attribution: Mapping[str, Any] | None,
    finalized_capture: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    records = {
        "canonical_trade_records": canonical,
        "ctol": outcome,
        "ctoe": enrichment,
        "ra7_canonical_trade_paths": path,
        "ra3_trade_decision_attribution": attribution,
        "ra8_finalized_capture": finalized_capture,
    }
    result: list[dict[str, Any]] = []
    for name, template in templates.items():
        record = records.get(name)
        if record is None:
            continue
        item = dict(template)
        item.update(
            {
                "record_identifier": _record_identifier(name, record),
                "record_schema_version": record.get("schema_version"),
                "record_fingerprint": _record_fingerprint(record),
            }
        )
        result.append(item)
    return result


def _source_provenance_templates(source_paths: Mapping[str, Path | str]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name, raw_path in source_paths.items():
        path = Path(raw_path)
        rows = _read_jsonl(path)
        first = rows[0] if rows else {}
        generated_at = first.get("generated_at") if isinstance(first, Mapping) else None
        result[name] = {
            "source_name": name,
            "source_artifact_path": str(path),
            "source_schema_version": first.get("schema_version") if isinstance(first, Mapping) else None,
            "source_generated_at": generated_at,
            "source_hash": _file_sha256(path),
        }
    return result


def _broken_exact_refs(
    *,
    canonical: Mapping[str, Any],
    outcome: Mapping[str, Any] | None,
    path: Mapping[str, Any] | None,
    finalized_capture: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    broken: list[dict[str, Any]] = []
    source_trade_id = _source_trade_id(canonical)
    outcome_id = _str_or_none(outcome.get("trade_outcome_id")) if outcome else None
    if path:
        path_source = _str_or_none(path.get("source_trade_id"))
        path_outcome = _str_or_none(path.get("trade_outcome_id"))
        if source_trade_id and path_source and path_source != source_trade_id:
            broken.append(_broken_join("ra7", f"source_trade_id mismatch: {path_source} != {source_trade_id}"))
        if outcome_id and path_outcome and path_outcome != outcome_id:
            broken.append(_broken_join("ra7", f"trade_outcome_id mismatch: {path_outcome} != {outcome_id}"))
    if finalized_capture:
        capture_source = _str_or_none(finalized_capture.get("source_trade_id"))
        if source_trade_id and capture_source and capture_source != source_trade_id:
            broken.append(_broken_join("ra8", f"source_trade_id mismatch: {capture_source} != {source_trade_id}"))
    return broken


def _anchor_mismatches(row: Mapping[str, Any], canonical: Mapping[str, Any]) -> list[dict[str, Any]]:
    checks = [
        ("entry_anchor.entry_time", row.get("entry_anchor", {}).get("entry_time"), canonical.get("entry_time")),
        ("entry_anchor.entry_price", row.get("entry_anchor", {}).get("entry_price"), canonical.get("entry_price")),
        ("exit_anchor.exit_time", row.get("exit_anchor", {}).get("exit_time"), canonical.get("exit_time")),
        ("exit_anchor.exit_price", row.get("exit_anchor", {}).get("exit_price"), canonical.get("exit_price")),
        ("trade_identity.source_trade_id", row.get("trade_identity", {}).get("source_trade_id"), _source_trade_id(canonical)),
    ]
    return [_mismatch("canonical_trade_records", field, expected, actual) for field, actual, expected in checks if _norm(actual) != _norm(expected)]


def _outcome_mismatches(row: Mapping[str, Any], outcome: Mapping[str, Any]) -> list[dict[str, Any]]:
    summary = row.get("outcome_summary", {})
    checks = [
        ("outcome_summary.realized_pnl_proxy", summary.get("realized_pnl_proxy"), outcome.get("realized_pnl_proxy")),
        ("outcome_summary.realized_points", summary.get("realized_points"), outcome.get("realized_points")),
        ("outcome_summary.hold_seconds", summary.get("hold_seconds"), outcome.get("hold_seconds")),
    ]
    return [_mismatch("ctol", field, expected, actual) for field, actual, expected in checks if _norm(actual) != _norm(expected)]


def _is_completed_paired_trade(row: Mapping[str, Any]) -> bool:
    return row.get("pairing_status") == "PAIRED" and row.get("trade_status") == "CLOSED"


def _empty_join_quality() -> dict[str, Any]:
    return {"overall": "INCOMPLETE", "exact": [], "tolerance": [], "missing": [], "broken": [], "migration_debt": []}


def _record_join(join_quality: dict[str, Any], layer: str, join: Mapping[str, Any]) -> None:
    item = dict(join)
    if layer and not item.get("layer"):
        item["layer"] = layer
    quality = item.get("quality")
    if quality == "EXACT":
        join_quality["exact"].append(item)
    elif quality == "TOLERANCE":
        join_quality["tolerance"].append(item)
        join_quality["migration_debt"].append(item)
    elif quality == "BROKEN":
        join_quality["broken"].append(item)
    else:
        join_quality["missing"].append(item)


def _overall_join_quality(join_quality: Mapping[str, Any]) -> str:
    if join_quality.get("broken"):
        return "BROKEN"
    if join_quality.get("tolerance"):
        return "TOLERANCE_WITH_WARNINGS"
    if join_quality.get("missing"):
        return "INCOMPLETE"
    return "EXACT"


def _exact_join(layer: str, method: str) -> dict[str, Any]:
    return {"layer": layer, "quality": "EXACT", "method": method}


def _missing_join(layer: str, reason: str) -> dict[str, Any]:
    return {"layer": layer, "quality": "MISSING", "reason": reason}


def _broken_join(layer: str, reason: str) -> dict[str, Any]:
    return {"layer": layer, "quality": "BROKEN", "reason": reason}


def _mismatch(layer: str, field: str, expected: Any, actual: Any) -> dict[str, Any]:
    return {"layer": layer, "field": field, "expected": expected, "actual": actual}


def _join_quality_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"EXACT": 0, "TOLERANCE": 0, "MISSING": 0, "BROKEN": 0}
    for row in rows:
        quality = row.get("join_quality", {})
        counts["EXACT"] += len(quality.get("exact", []))
        counts["TOLERANCE"] += len(quality.get("tolerance", []))
        counts["MISSING"] += len(quality.get("missing", []))
        counts["BROKEN"] += len(quality.get("broken", []))
    return counts


def _items_by_layer(items: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        key = str(item.get("layer") or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _layer_join_count(rows: Sequence[Mapping[str, Any]], layer: str, bucket: str) -> int:
    return sum(1 for row in rows for item in row.get("join_quality", {}).get(bucket, []) if item.get("layer") == layer)


def _source_trade_id(row: Mapping[str, Any]) -> str | None:
    return _str_or_none(row.get("trade_id") or _source_ref(row, "source_trade_id"))


def _source_ref(row: Mapping[str, Any], key: str) -> str | None:
    refs = row.get("source_refs") if isinstance(row.get("source_refs"), Mapping) else {}
    return _str_or_none(refs.get(key))


def _capture_id(row: Mapping[str, Any]) -> str | None:
    capture = row.get("path_capture") if isinstance(row.get("path_capture"), Mapping) else {}
    return _str_or_none(capture.get("capture_id"))


def _path_capture_trade_identity(row: Mapping[str, Any]) -> Mapping[str, Any]:
    capture = row.get("path_capture") if isinstance(row.get("path_capture"), Mapping) else {}
    identity = capture.get("trade_identity") if isinstance(capture.get("trade_identity"), Mapping) else {}
    return identity


def _status_from_unknown(payload: Mapping[str, Any]) -> str | None:
    if not payload:
        return None
    values = [value for value in payload.values() if value not in (None, "", [], {})]
    if not values:
        return "MISSING"
    if any(str(value).upper() == "UNKNOWN" for value in values):
        return "PARTIAL"
    return "COMPLETE"


def _record_identifier(name: str, row: Mapping[str, Any]) -> str | None:
    keys = {
        "canonical_trade_records": ("trade_id", "source_trade_id"),
        "ctol": ("trade_outcome_id",),
        "ctoe": ("trade_outcome_id",),
        "ra7_canonical_trade_paths": ("canonical_trade_path_id", "trade_outcome_id", "source_trade_id"),
        "ra3_trade_decision_attribution": ("trade_decision_attribution_id", "trade_outcome_id"),
        "ra8_finalized_capture": ("capture_id", "source_trade_id"),
    }.get(name, ())
    for key in keys:
        value = _str_or_none(row.get(key))
        if value:
            return value
    return None


def _fingerprint_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key not in {"generated_at", "deterministic_fingerprint"}}


def _record_fingerprint(row: Mapping[str, Any] | None) -> str | None:
    if not row:
        return None
    return _fingerprint(_fingerprint_payload(dict(row)))


def _fingerprint(payload: Mapping[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return f"{prefix}_{hashlib.sha256(raw.encode('utf-8')).hexdigest()[:24]}"


def _file_sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _str_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _norm(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _dict_table(data: Mapping[str, Any]) -> str:
    if not data:
        return "No rows."
    lines = ["|layer|count|", "|---|---:|"]
    for key, value in data.items():
        lines.append(f"|{key}|{value}|")
    return "\n".join(lines)
