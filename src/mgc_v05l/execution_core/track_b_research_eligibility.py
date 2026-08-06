"""Historical research eligibility classification.

This module produces deterministic, diagnostic-only research eligibility records
over Canonical Research Record v1 artifacts. It has no broker, runtime,
strategy, Managed Exit, Guardian, Safe-State, readiness, reconciliation, or
trading-gate authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_research_record import (
    CRR_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_CRR_OUTPUT_DIR,
    VALIDATION_JSON as CRR_VALIDATION_JSON,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_CRR_PATH = DEFAULT_CRR_OUTPUT_DIR / CRR_JSONL
DEFAULT_CRR_VALIDATION_PATH = DEFAULT_CRR_OUTPUT_DIR / CRR_VALIDATION_JSON
DEFAULT_INV_001_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "investigations" / "INV-001"
DEFAULT_ANOMALY_ROOT_CAUSE_PATH = DEFAULT_INV_001_DIR / "anomaly_root_cause.json"
DEFAULT_EXTREME_CLASSIFICATION_PATH = DEFAULT_INV_001_DIR / "extreme_trade_classification.json"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "research_eligibility"

RECORDS_JSONL = "research_eligibility_records.jsonl"
SUMMARY_JSON = "research_eligibility_summary.json"
SUMMARY_MD = "research_eligibility_summary.md"
VALIDATION_JSON = "research_eligibility_validation_report.json"
VALIDATION_MD = "research_eligibility_validation_report.md"
SCHEMA_JSON = "research_eligibility_schema.json"

SCHEMA_VERSION = "research_eligibility_record_v1"
SUMMARY_SCHEMA_VERSION = "research_eligibility_summary_v1"
VALIDATION_SCHEMA_VERSION = "research_eligibility_validation_v1"

ELIGIBLE_ORDINARY = "ELIGIBLE_ORDINARY_STRATEGY_EVIDENCE"
ELIGIBLE_LIMITED = "ELIGIBLE_WITH_LIMITATIONS"
EXCLUDED_SOURCE_INTEGRITY = "EXCLUDED_CONFIRMED_SOURCE_INTEGRITY_ANOMALY"
EXCLUDED_DEVELOPMENT = "EXCLUDED_CONFIRMED_DEVELOPMENT_OR_LEAK_TEST_ARTIFACT"
EXCLUDED_OPERATIONAL = "EXCLUDED_CONFIRMED_OPERATIONAL_OR_LIFECYCLE_ANOMALY"
EXCLUDED_PNL_INVALID = "EXCLUDED_CONFIRMED_PNL_OR_DATA_QUALITY_INVALID"
REVIEW_REQUIRED = "REVIEW_REQUIRED_INSUFFICIENT_EVIDENCE"
NOT_APPLICABLE = "NOT_APPLICABLE"

FULL_HISTORICAL = "FULL_HISTORICAL"
SOURCE_INTEGRITY_QUALIFIED = "SOURCE_INTEGRITY_QUALIFIED"
ORDINARY_STRATEGY_EVIDENCE = "ORDINARY_STRATEGY_EVIDENCE"
REVIEW_REQUIRED_VIEW = "REVIEW_REQUIRED"

GUARDRAILS = {
    "diagnostic_only": True,
    "production_recommendation": False,
    "trading_gate": False,
}

SOURCE_INTEGRITY_CLASSIFICATIONS = {
    "CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH",
    "DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE",
}
SOURCE_CONFIRMATION_WORST_TRADE_COUNT = 20
PRICE_DOMAIN_DISCONTINUITY_RATIO = 3.0


@dataclass(frozen=True)
class ResearchEligibilityResult:
    records: list[dict[str, Any]]
    summary: dict[str, Any]
    validation: dict[str, Any]
    records_path: Path
    summary_path: Path
    summary_markdown_path: Path
    validation_path: Path
    validation_markdown_path: Path
    schema_path: Path


def run_research_eligibility(
    *,
    crr_path: Path = DEFAULT_CRR_PATH,
    crr_validation_path: Path = DEFAULT_CRR_VALIDATION_PATH,
    anomaly_root_cause_path: Path = DEFAULT_ANOMALY_ROOT_CAUSE_PATH,
    extreme_classification_path: Path = DEFAULT_EXTREME_CLASSIFICATION_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> ResearchEligibilityResult:
    generated_at = _coerce_now(now)
    crr_rows = _read_jsonl(crr_path)
    crr_validation = _read_json(crr_validation_path)
    anomaly_root_cause = _read_json(anomaly_root_cause_path)
    extreme_classification = _read_json(extreme_classification_path)
    records = build_research_eligibility_records(
        crr_rows,
        crr_validation=crr_validation,
        anomaly_root_cause=anomaly_root_cause,
        extreme_classification=extreme_classification,
        source_paths={
            "crr": crr_path,
            "crr_validation": crr_validation_path,
            "anomaly_root_cause": anomaly_root_cause_path,
            "extreme_trade_classification": extreme_classification_path,
        },
        generated_at=generated_at,
    )
    summary = build_research_eligibility_summary(records, crr_rows=crr_rows, generated_at=generated_at)
    validation = validate_research_eligibility(records, crr_rows=crr_rows, summary=summary, generated_at=generated_at)

    output_dir.mkdir(parents=True, exist_ok=True)
    records_path = output_dir / RECORDS_JSONL
    summary_path = output_dir / SUMMARY_JSON
    summary_markdown_path = output_dir / SUMMARY_MD
    validation_path = output_dir / VALIDATION_JSON
    validation_markdown_path = output_dir / VALIDATION_MD
    schema_path = output_dir / SCHEMA_JSON
    _write_jsonl(records_path, records)
    _write_json(summary_path, summary)
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    _write_json(validation_path, validation)
    validation_markdown_path.write_text(render_validation_markdown(validation), encoding="utf-8")
    _write_json(schema_path, research_eligibility_schema())
    return ResearchEligibilityResult(
        records=records,
        summary=summary,
        validation=validation,
        records_path=records_path,
        summary_path=summary_path,
        summary_markdown_path=summary_markdown_path,
        validation_path=validation_path,
        validation_markdown_path=validation_markdown_path,
        schema_path=schema_path,
    )


def build_research_eligibility_records(
    crr_rows: Sequence[Mapping[str, Any]],
    *,
    crr_validation: Mapping[str, Any],
    anomaly_root_cause: Mapping[str, Any],
    extreme_classification: Mapping[str, Any],
    source_paths: Mapping[str, Path | str],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    anomalies = _source_confirmed_anomaly_index(
        anomaly_root_cause,
        extreme_classification,
        crr_rows=crr_rows,
    )
    base_source_fingerprints = _source_fingerprints_for_names(source_paths, ("crr", "crr_validation"))
    records: list[dict[str, Any]] = []
    for row in crr_rows:
        record_id = str(row.get("research_record_id") or "")
        anomaly = anomalies.get(record_id)
        if anomaly:
            classification = EXCLUDED_SOURCE_INTEGRITY
            confidence = str(anomaly.get("confidence") or "HIGH")
            review_required = False
            evidence_basis = [
                f"INV-001 classified this record as {anomaly.get('source_classification')}.",
                str(anomaly.get("summary") or anomaly.get("reasoning") or "Source-confirmed anomaly evidence exists."),
            ]
            limitations = ["Excluded from qualified research views; canonical source history is preserved unchanged."]
            contradictory = list(anomaly.get("contradictory_evidence") or [])
            artifact_paths = sorted(set(str(path) for path in anomaly.get("supporting_artifact_paths", []) if path))
            supporting_ids = dict(anomaly.get("supporting_ids") or {})
            fingerprint_sources = sorted(set(["crr", "crr_validation", *list(anomaly.get("source_artifact_names") or [])]))
            artifact_paths.extend(str(source_paths[name]) for name in anomaly.get("source_artifact_names", []) if name in source_paths)
        else:
            classification, confidence, review_required, evidence_basis, limitations, contradictory, artifact_paths, supporting_ids = _classify_non_anomaly(
                row,
                crr_validation=crr_validation,
                source_paths=source_paths,
            )
            fingerprint_sources = ["crr", "crr_validation"]
        record = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "research_record_id": record_id,
            "source_trade_id": row.get("trade_identity", {}).get("source_trade_id"),
            "classification": classification,
            "evidence_basis": evidence_basis,
            "supporting_artifact_paths": sorted(set(artifact_paths)),
            "supporting_ids": supporting_ids,
            "confidence": confidence,
            "limitations": sorted(set(limitations)),
            "contradictory_evidence": contradictory,
            "review_required": review_required,
            "source_fingerprint": {
                "crr_record_fingerprint": row.get("deterministic_fingerprint"),
                "source_artifacts": {
                    **base_source_fingerprints,
                    **_source_fingerprints_for_names(source_paths, fingerprint_sources),
                },
            },
            "source_join_quality": row.get("join_quality", {}),
            "guardrails": dict(GUARDRAILS),
            **GUARDRAILS,
        }
        record["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(record))
        records.append(record)
    return sorted(records, key=lambda item: str(item.get("research_record_id")))


def _source_confirmed_anomaly_index(
    anomaly_root_cause: Mapping[str, Any],
    extreme_classification: Mapping[str, Any],
    *,
    crr_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, dict[str, Any]]:
    root_by_id = {str(item.get("research_record_id")): item for item in anomaly_root_cause.get("records", [])}
    classifications = {}
    for item in extreme_classification.get("records", []):
        classification = str(item.get("classification") or "")
        if classification not in SOURCE_INTEGRITY_CLASSIFICATIONS:
            continue
        record_id = str(item.get("research_record_id") or "")
        root = root_by_id.get(record_id, {})
        classifications[record_id] = {
            "source_classification": classification,
            "confidence": item.get("confidence") or root.get("confidence") or "HIGH",
            "summary": root.get("summary") or item.get("reasoning"),
            "reasoning": item.get("reasoning"),
            "supporting_artifact_paths": item.get("supporting_artifact_paths") or [],
            "supporting_ids": item.get("supporting_ids") or {},
            "contradictory_evidence": root.get("contradictory_evidence") or item.get("contradictory_evidence") or [],
            "source_artifact_names": ["anomaly_root_cause", "extreme_trade_classification"],
        }
    if classifications:
        return classifications
    return _source_confirmed_anomaly_index_from_crr(crr_rows)


def _source_confirmed_anomaly_index_from_crr(crr_rows: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Recover source-confirmed anomaly candidates from CRR evidence if INV outputs were regenerated.

    The bounded fallback mirrors the INV-001 source-confirmation scope: it looks
    only at the worst realized-P&L rows and requires durable CRR evidence of
    either same-trade price-domain discontinuity or duplicated entry execution
    evidence across distinct source trades. It does not use instrument-specific
    price ranges and does not rewrite source history.
    """

    worst_rows = sorted(
        crr_rows,
        key=lambda row: (_number(row.get("outcome_summary", {}).get("realized_pnl_proxy")) or 0.0, str(row.get("research_record_id"))),
    )[:SOURCE_CONFIRMATION_WORST_TRADE_COUNT]
    worst_ids = {str(row.get("research_record_id")) for row in worst_rows}
    anomalies: dict[str, dict[str, Any]] = {}

    for row in worst_rows:
        entry_price = _number(row.get("entry_anchor", {}).get("entry_price"))
        exit_price = _number(row.get("exit_anchor", {}).get("exit_price"))
        pnl = _number(row.get("outcome_summary", {}).get("realized_pnl_proxy"))
        if (
            pnl is not None
            and pnl < 0
            and entry_price is not None
            and exit_price is not None
            and min(abs(entry_price), abs(exit_price)) > 0
            and max(abs(entry_price), abs(exit_price)) / min(abs(entry_price), abs(exit_price)) >= PRICE_DOMAIN_DISCONTINUITY_RATIO
        ):
            anomalies[str(row.get("research_record_id"))] = _crr_anomaly(
                row,
                source_classification="CONTRACT_MULTIPLIER_OR_SCALE_MISMATCH",
                summary="CRR entry and exit prices are in materially different price domains for the same completed trade.",
            )

    entry_exec_index: dict[str, list[Mapping[str, Any]]] = {}
    for row in crr_rows:
        exec_id = row.get("entry_anchor", {}).get("entry_exec_id")
        if exec_id:
            entry_exec_index.setdefault(str(exec_id), []).append(row)
    for exec_id, rows in entry_exec_index.items():
        if len(rows) < 2:
            continue
        source_trade_ids = {str(row.get("trade_identity", {}).get("source_trade_id") or "") for row in rows}
        lifecycle_ids = {str(row.get("trade_identity", {}).get("lifecycle_id") or "") for row in rows}
        if len(source_trade_ids) < 2 and len(lifecycle_ids) < 2:
            continue
        for row in rows:
            record_id = str(row.get("research_record_id"))
            if record_id not in worst_ids or record_id in anomalies:
                continue
            anomalies[record_id] = _crr_anomaly(
                row,
                source_classification="DUPLICATE_OR_REUSED_EXECUTION_EVIDENCE",
                summary=f"Entry execution ID {exec_id} is attached to multiple distinct canonical trade identities.",
            )
    return anomalies


def _crr_anomaly(row: Mapping[str, Any], *, source_classification: str, summary: str) -> dict[str, Any]:
    identity = row.get("trade_identity", {})
    entry = row.get("entry_anchor", {})
    exit_anchor = row.get("exit_anchor", {})
    outcome = row.get("outcome_summary", {})
    source_paths = [
        item.get("source_artifact_path")
        for item in row.get("source_provenance", [])
        if item.get("source_artifact_path")
    ]
    return {
        "source_classification": source_classification,
        "confidence": "HIGH",
        "summary": summary,
        "reasoning": summary,
        "supporting_artifact_paths": [path for path in source_paths if path],
        "supporting_ids": {
            "canonical_trade_id": identity.get("trade_id"),
            "source_trade_id": identity.get("source_trade_id"),
            "lifecycle_id": identity.get("lifecycle_id"),
            "entry_exec_id": entry.get("entry_exec_id"),
            "exit_exec_id": exit_anchor.get("exit_exec_id"),
            "pnl_source_record_id": row.get("outcome_ref", {}).get("trade_outcome_id"),
        },
        "contradictory_evidence": [
            "The source CRR row remains preserved and is not rewritten or removed from FULL_HISTORICAL views.",
            f"Observed realized P&L proxy: {outcome.get('realized_pnl_proxy')}.",
        ],
        "source_artifact_names": ["crr"],
    }


def _source_fingerprints_for_names(source_paths: Mapping[str, Path | str], names: Sequence[str]) -> dict[str, str | None]:
    return {
        name: _file_sha256(Path(source_paths[name]))
        for name in sorted(set(names))
        if name in source_paths
    }


def _classify_non_anomaly(
    row: Mapping[str, Any],
    *,
    crr_validation: Mapping[str, Any],
    source_paths: Mapping[str, Path | str],
) -> tuple[str, str, bool, list[str], list[str], list[str], list[str], dict[str, Any]]:
    join_quality = row.get("join_quality", {})
    outcome = row.get("outcome_summary", {})
    limitations = _limitations_for_row(row, crr_validation=crr_validation)
    supporting_ids = {
        "source_trade_id": row.get("trade_identity", {}).get("source_trade_id"),
        "trade_id": row.get("trade_identity", {}).get("trade_id"),
        "lifecycle_id": row.get("trade_identity", {}).get("lifecycle_id"),
        "trade_outcome_id": row.get("outcome_ref", {}).get("trade_outcome_id"),
        "canonical_trade_path_id": row.get("path_ref", {}).get("canonical_trade_path_id"),
        "trade_decision_attribution_id": row.get("attribution_ref", {}).get("trade_decision_attribution_id"),
        "capture_id": row.get("path_ref", {}).get("capture_id"),
    }
    artifact_paths = [
        str(source_paths.get("crr")),
        str(source_paths.get("crr_validation")),
        *[
            str(item.get("source_artifact_path"))
            for item in row.get("source_provenance", [])
            if item.get("source_artifact_path")
        ],
    ]
    if join_quality.get("broken") or _number(outcome.get("realized_pnl_proxy")) is None:
        return (
            REVIEW_REQUIRED,
            "LOW",
            True,
            ["CRR row has broken joins or missing realized P&L evidence."],
            limitations,
            ["No source-confirmed anomaly artifact excludes this row."],
            artifact_paths,
            supporting_ids,
        )
    if limitations:
        return (
            ELIGIBLE_LIMITED,
            "MEDIUM",
            False,
            ["CRR has exact required evidence and no source-confirmed exclusion, with documented limitations."],
            limitations,
            ["No source-confirmed anomaly artifact excludes this row."],
            artifact_paths,
            supporting_ids,
        )
    return (
        ELIGIBLE_ORDINARY,
        "MEDIUM",
        False,
        ["CRR has exact required evidence and no documented research limitation in the eligibility contract."],
        [],
        ["No source-confirmed anomaly artifact excludes this row."],
        artifact_paths,
        supporting_ids,
    )


def _limitations_for_row(row: Mapping[str, Any], *, crr_validation: Mapping[str, Any]) -> list[str]:
    limitations: list[str] = []
    missing = {str(item.get("layer")) for item in row.get("join_quality", {}).get("missing", [])}
    if "ra8" in missing:
        limitations.append("ra8_finalized_capture_missing")
    flags = [str(flag) for flag in row.get("outcome_summary", {}).get("data_quality_flags", [])]
    for flag in flags:
        if flag:
            limitations.append(flag)
    if row.get("outcome_summary", {}).get("mfe_points") in (None, ""):
        limitations.append("mfe_missing")
    if row.get("outcome_summary", {}).get("mae_points") in (None, ""):
        limitations.append("mae_missing")
    if not _has_independent_contract_economics(row):
        limitations.append("independent_contract_point_value_provenance_missing")
    for item in crr_validation.get("refresh_guidance", []):
        if item.get("source_name") == "ra8" and item.get("classification") == "SOURCE_COVERAGE_LIMIT":
            limitations.append("ra8_source_coverage_limit")
    return sorted(set(limitations))


def _has_independent_contract_economics(row: Mapping[str, Any]) -> bool:
    for provenance in row.get("source_provenance", []):
        name = str(provenance.get("source_name") or "").lower()
        if "contract" in name and ("economics" in name or "registry" in name):
            return True
    return False


def build_research_eligibility_summary(
    records: Sequence[Mapping[str, Any]],
    *,
    crr_rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    rows_by_id = {str(row.get("research_record_id")): row for row in crr_rows}
    record_by_id = {str(record.get("research_record_id")): record for record in records}
    views = {
        FULL_HISTORICAL: _population_view(
            FULL_HISTORICAL,
            rows=list(crr_rows),
            eligibility_records=record_by_id,
            excluded_ids=set(),
            description="All CRR rows.",
        ),
        SOURCE_INTEGRITY_QUALIFIED: _population_view(
            SOURCE_INTEGRITY_QUALIFIED,
            rows=[row for row in crr_rows if record_by_id.get(str(row.get("research_record_id")), {}).get("classification") != EXCLUDED_SOURCE_INTEGRITY],
            eligibility_records=record_by_id,
            excluded_ids={
                str(record.get("research_record_id"))
                for record in records
                if record.get("classification") == EXCLUDED_SOURCE_INTEGRITY
            },
            description="Excludes only source-confirmed source-integrity anomalies.",
        ),
        ORDINARY_STRATEGY_EVIDENCE: _population_view(
            ORDINARY_STRATEGY_EVIDENCE,
            rows=[
                row
                for row in crr_rows
                if record_by_id.get(str(row.get("research_record_id")), {}).get("classification")
                in {ELIGIBLE_ORDINARY, ELIGIBLE_LIMITED}
            ],
            eligibility_records=record_by_id,
            excluded_ids={
                str(record.get("research_record_id"))
                for record in records
                if record.get("classification") not in {ELIGIBLE_ORDINARY, ELIGIBLE_LIMITED}
            },
            description="Rows positively classified as ordinary strategy evidence or eligible with documented limitations.",
        ),
        REVIEW_REQUIRED_VIEW: _population_view(
            REVIEW_REQUIRED_VIEW,
            rows=[row for row in crr_rows if record_by_id.get(str(row.get("research_record_id")), {}).get("review_required") is True],
            eligibility_records=record_by_id,
            excluded_ids={
                str(row.get("research_record_id"))
                for row in crr_rows
                if record_by_id.get(str(row.get("research_record_id")), {}).get("review_required") is not True
            },
            description="Rows requiring unresolved research review.",
        ),
    }
    anomaly_records = [
        record
        for record in records
        if record.get("classification") == EXCLUDED_SOURCE_INTEGRITY
    ]
    payload = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "input_crr_count": len(crr_rows),
        "eligibility_record_count": len(records),
        "classification_counts": _distribution(record.get("classification") for record in records),
        "views": views,
        "source_confirmed_anomalies": [_anomaly_summary(record, rows_by_id.get(str(record.get("research_record_id")), {})) for record in anomaly_records],
        "review_queue": [
            {
                "research_record_id": record.get("research_record_id"),
                "source_trade_id": record.get("source_trade_id"),
                "classification": record.get("classification"),
                "evidence_basis": record.get("evidence_basis"),
                "limitations": record.get("limitations"),
            }
            for record in records
            if record.get("review_required") is True
        ],
        "comparability_disclosure": "Raw realized P&L proxy can reflect instrument, multiplier, and quantity differences. Controlled views preserve within-instrument context and do not invent normalization.",
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def _population_view(
    view_id: str,
    *,
    rows: Sequence[Mapping[str, Any]],
    eligibility_records: Mapping[str, Mapping[str, Any]],
    excluded_ids: set[str],
    description: str,
) -> dict[str, Any]:
    normalized = [_normalize_crr_for_metrics(row, eligibility_records.get(str(row.get("research_record_id")), {})) for row in rows]
    excluded_counts: dict[str, int] = {}
    for record_id in sorted(excluded_ids):
        classification = str(eligibility_records.get(record_id, {}).get("classification") or "MISSING_ELIGIBILITY_RECORD")
        excluded_counts[classification] = excluded_counts.get(classification, 0) + 1
    return {
        "view_id": view_id,
        "description": description,
        "included_count": len(rows),
        "excluded_count": len(excluded_ids),
        "excluded_count_by_reason": dict(sorted(excluded_counts.items())),
        "metrics": _metrics(normalized),
        "long_short_outcomes": {
            "long": _metrics([row for row in normalized if row.get("side") == "LONG"]),
            "short": _metrics([row for row in normalized if row.get("side") == "SHORT"]),
        },
        "instrument_mix": _distribution(row.get("instrument") for row in normalized),
        "strategy_mix": _distribution((row.get("strategy_id") for row in normalized), limit=30),
        "session_mix": _distribution(row.get("session") for row in normalized),
        "regime_mix": _distribution(row.get("regime") for row in normalized),
        "missingness": _missing_field_counts(normalized),
        "ra8_coverage": {
            "exact_count": sum(1 for row in normalized if row.get("ra8_coverage_status") == "EXACT"),
            "missing_count": sum(1 for row in normalized if row.get("ra8_coverage_status") != "EXACT"),
            "coverage_rate": _rate(sum(1 for row in normalized if row.get("ra8_coverage_status") == "EXACT"), len(normalized)),
        },
    }


def _normalize_crr_for_metrics(row: Mapping[str, Any], eligibility: Mapping[str, Any]) -> dict[str, Any]:
    identity = row.get("trade_identity", {})
    entry = row.get("entry_anchor", {})
    enrichment = row.get("enrichment_ref", {}).get("context_validity_summary", {})
    outcome = row.get("outcome_summary", {})
    missing_fields = []
    for key, value in {
        "mfe_points": outcome.get("mfe_points"),
        "mae_points": outcome.get("mae_points"),
        "session": enrichment.get("session"),
        "regime": enrichment.get("gre_validity_classification") or enrichment.get("market_context_validity_classification"),
    }.items():
        if value in (None, "", [], {}):
            missing_fields.append(key)
    if eligibility.get("classification") == ELIGIBLE_LIMITED:
        missing_fields.extend(eligibility.get("limitations") or [])
    has_ra8 = any(item.get("layer") == "ra8" for item in row.get("join_quality", {}).get("exact", []))
    if not has_ra8:
        missing_fields.append("ra8_finalized_capture")
    return {
        "research_record_id": row.get("research_record_id"),
        "instrument": identity.get("instrument") or "UNKNOWN",
        "strategy_id": entry.get("strategy_id") or "UNKNOWN",
        "session": enrichment.get("session") or "UNKNOWN",
        "regime": enrichment.get("gre_validity_classification") or enrichment.get("market_context_validity_classification") or "UNKNOWN",
        "side": str(identity.get("side") or "UNKNOWN").upper(),
        "realized_pnl_proxy": _number(outcome.get("realized_pnl_proxy")),
        "mfe_points": _number(outcome.get("mfe_points")),
        "mae_points": _number(outcome.get("mae_points")),
        "ra8_coverage_status": "EXACT" if has_ra8 else "MISSING",
        "missing_fields": sorted(set(missing_fields)),
    }


def validate_research_eligibility(
    records: Sequence[Mapping[str, Any]],
    *,
    crr_rows: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    blockers: list[str] = []
    warnings: list[str] = []
    crr_ids = {str(row.get("research_record_id")) for row in crr_rows}
    record_ids = [str(record.get("research_record_id")) for record in records]
    if len(records) != len(crr_rows):
        blockers.append("eligibility_count_does_not_match_crr_count")
    if set(record_ids) != crr_ids:
        blockers.append("eligibility_ids_do_not_reconcile_to_crr")
    if len(record_ids) != len(set(record_ids)):
        blockers.append("duplicate_eligibility_record_id")
    anomaly_count = summary.get("classification_counts", {}).get(EXCLUDED_SOURCE_INTEGRITY, 0)
    if anomaly_count != 5:
        blockers.append("expected_five_source_integrity_anomalies_not_found")
    if summary.get("views", {}).get(FULL_HISTORICAL, {}).get("included_count") != len(crr_rows):
        blockers.append("full_historical_population_does_not_reconcile")
    if summary.get("views", {}).get(SOURCE_INTEGRITY_QUALIFIED, {}).get("excluded_count") != anomaly_count:
        blockers.append("qualified_population_exclusion_count_mismatch")
    if summary.get("views", {}).get(REVIEW_REQUIRED_VIEW, {}).get("included_count", 0):
        warnings.append("review_required_records_present")
    if summary.get("views", {}).get(FULL_HISTORICAL, {}).get("ra8_coverage", {}).get("missing_count", 0):
        warnings.append("ra8_coverage_partial")
    status = "INVALID" if blockers else "VALID_WITH_WARNINGS" if warnings else "VALID"
    payload = {
        "schema_version": VALIDATION_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
        "counts": {
            "crr_rows": len(crr_rows),
            "eligibility_records": len(records),
            "source_integrity_exclusions": anomaly_count,
            "review_required": summary.get("views", {}).get(REVIEW_REQUIRED_VIEW, {}).get("included_count"),
        },
        "guardrails": dict(GUARDRAILS),
        **GUARDRAILS,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def research_eligibility_schema() -> dict[str, Any]:
    return {
        "title": "ResearchEligibilityRecord",
        "schema_version": SCHEMA_VERSION,
        "required_fields": [
            "research_record_id",
            "classification",
            "evidence_basis",
            "supporting_artifact_paths",
            "supporting_ids",
            "confidence",
            "limitations",
            "contradictory_evidence",
            "review_required",
            "source_fingerprint",
            "generated_at",
            "guardrails",
        ],
        "classifications": [
            ELIGIBLE_ORDINARY,
            ELIGIBLE_LIMITED,
            EXCLUDED_SOURCE_INTEGRITY,
            EXCLUDED_DEVELOPMENT,
            EXCLUDED_OPERATIONAL,
            EXCLUDED_PNL_INVALID,
            REVIEW_REQUIRED,
            NOT_APPLICABLE,
        ],
        "guardrails": dict(GUARDRAILS),
    }


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# Research Eligibility Summary",
        "",
        f"- Input CRR rows: `{summary.get('input_crr_count')}`",
        f"- Eligibility records: `{summary.get('eligibility_record_count')}`",
        f"- Source-integrity exclusions: `{summary.get('classification_counts', {}).get(EXCLUDED_SOURCE_INTEGRITY, 0)}`",
        "",
        "## Population Views",
        "",
        "|view|included|excluded|total pnl|avg pnl|median pnl|win rate|RA8 exact|",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for view_id, view in summary.get("views", {}).items():
        metrics = view.get("metrics", {})
        ra8 = view.get("ra8_coverage", {})
        lines.append(
            f"|{view_id}|{view.get('included_count')}|{view.get('excluded_count')}|{metrics.get('total_realized_pnl_proxy')}|"
            f"{metrics.get('average_realized_pnl_proxy')}|{metrics.get('median_realized_pnl_proxy')}|{metrics.get('win_rate')}|{ra8.get('exact_count')}|"
        )
    lines.extend(["", "## Source-Confirmed Anomalies", ""])
    for item in summary.get("source_confirmed_anomalies", []):
        lines.append(
            f"- `{item.get('research_record_id')}` `{item.get('instrument')}` `{item.get('classification')}` "
            f"P&L `{item.get('realized_pnl_proxy')}`"
        )
    lines.extend(["", "These classifications are diagnostic, non-causal, and carry no production authority.", ""])
    return "\n".join(lines)


def render_validation_markdown(validation: Mapping[str, Any]) -> str:
    return (
        "# Research Eligibility Validation\n\n"
        f"- Status: `{validation.get('status')}`\n"
        f"- CRR rows: `{validation.get('counts', {}).get('crr_rows')}`\n"
        f"- Eligibility records: `{validation.get('counts', {}).get('eligibility_records')}`\n"
        f"- Blockers: `{len(validation.get('blockers', []))}`\n"
        f"- Warnings: `{len(validation.get('warnings', []))}`\n"
    )


def _anomaly_summary(record: Mapping[str, Any], crr_row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "research_record_id": record.get("research_record_id"),
        "source_trade_id": record.get("source_trade_id"),
        "classification": record.get("classification"),
        "evidence_basis": record.get("evidence_basis"),
        "confidence": record.get("confidence"),
        "instrument": crr_row.get("trade_identity", {}).get("instrument"),
        "side": crr_row.get("trade_identity", {}).get("side"),
        "realized_pnl_proxy": crr_row.get("outcome_summary", {}).get("realized_pnl_proxy"),
        "supporting_ids": record.get("supporting_ids"),
        "supporting_artifact_paths": record.get("supporting_artifact_paths"),
    }


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = [value for value in (_number(row.get("realized_pnl_proxy")) for row in rows) if value is not None]
    winners = [value for value in pnl if value > 0]
    losers = [value for value in pnl if value <= 0]
    average_winner = _average(winners)
    average_loser = _average(losers)
    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    return {
        "trade_count": len(rows),
        "total_realized_pnl_proxy": _round(sum(pnl)) if pnl else None,
        "average_realized_pnl_proxy": _average(pnl),
        "median_realized_pnl_proxy": _median(pnl),
        "trimmed_mean_5_percent": _trimmed_mean(pnl, trim_fraction=0.05),
        "win_rate": _rate(len(winners), len(pnl)) if pnl else None,
        "average_winner": average_winner,
        "average_loser": average_loser,
        "payoff_ratio": _round(abs(average_winner / average_loser)) if average_winner is not None and average_loser not in (None, 0) else None,
        "profit_factor_proxy": _round(gross_profit / gross_loss) if gross_loss else None,
        "expectancy": _average(pnl),
        "instrument_mix": _distribution(row.get("instrument") for row in rows),
        "strategy_mix": _distribution((row.get("strategy_id") for row in rows), limit=30),
        "session_mix": _distribution(row.get("session") for row in rows),
        "regime_mix": _distribution(row.get("regime") for row in rows),
    }


def _trimmed_mean(values: Sequence[float], *, trim_fraction: float) -> float | None:
    if not values:
        return None
    sorted_values = sorted(float(value) for value in values)
    trim = int(len(sorted_values) * trim_fraction)
    trimmed = sorted_values[trim : len(sorted_values) - trim] if trim and len(sorted_values) > trim * 2 else sorted_values
    return _average(trimmed)


def _missing_field_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        for field in row.get("missing_fields", []):
            counts[str(field)] = counts.get(str(field), 0) + 1
    return dict(sorted(counts.items()))


def _distribution(values: Any, *, limit: int | None = None) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    if limit:
        items = items[:limit]
    return dict(items)


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: Sequence[float]) -> float | None:
    return _round(sum(values) / len(values)) if values else None


def _median(values: Sequence[float]) -> float | None:
    return _round(float(median(values))) if values else None


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 6) if denominator else 0.0


def _round(value: float | None) -> float | None:
    return round(float(value), 6) if value is not None else None


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


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows), encoding="utf-8")


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
