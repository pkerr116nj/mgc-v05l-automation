"""Saved-query persistence for the Canonical Analytics Engine.

Saved queries are diagnostic/research artifacts only. They serialize
CanonicalAnalyticsQuery payloads for dashboards, APIs, and future AI clients.
They have no runtime, broker, strategy, order, or trading-gate authority.
"""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_analytics_engine import (
    AnalyticsFilter,
    CanonicalAnalyticsEngine,
    CanonicalAnalyticsQuery,
    coerce_datetime,
)
from mgc_v05l.execution_core.track_b_core_expectancy_analytics import (
    DEFAULT_ENRICHMENTS_PATH,
    DEFAULT_OUTCOMES_PATH,
    _confidence_bucket,
    _number,
    _vix_percentile_bucket,
    merge_outcomes_with_enrichment,
)


DEFAULT_OUTPUT_DIR = Path("outputs") / "track_b_execution_core" / "research" / "canonical_analytics_engine" / "saved_queries"
SAVED_QUERIES_JSONL = "saved_analytics_queries.jsonl"
PRESETS_JSON = "saved_analytics_query_presets.json"
VALIDATION_REPORT_MD = "saved_query_validation_report.md"
CONTRACT_MD = "saved_query_contract.md"
SUMMARY_JSON = "cae5_saved_query_summary.json"
EXECUTION_LOG_JSONL = "cae6_execution_log.jsonl"
EXECUTION_SUMMARY_JSON = "cae6_latest_execution_summary.json"
EXECUTION_CONTRACT_MD = "cae6_execution_audit_contract.md"
RESULT_MANIFEST_MD = "cae6_result_manifest.md"
RESULT_PROVENANCE_REPORT_MD = "cae6_result_provenance_report.md"
RESULT_SNAPSHOT_DIR = "result_snapshots"
RESULT_DIFF_CONTRACT_MD = "cae7_result_diff_contract.md"
RESULT_DIFF_JSON = "cae7_latest_result_diff.json"
RESULT_DIFF_MD = "cae7_latest_result_diff.md"
CHANGE_FEED_MD = "cae7_change_feed.md"

SAVED_QUERY_SCHEMA_VERSION = "magic_saved_query_v1"
SUMMARY_SCHEMA_VERSION = "cae5_saved_query_summary_v1"
VALIDATION_SCHEMA_VERSION = "cae5_saved_query_validation_v1"
EXECUTION_RECORD_SCHEMA_VERSION = "cae6_canonical_analytics_execution_record_v1"
RESULT_DIFF_SCHEMA_VERSION = "cae7_canonical_analytics_result_diff_v1"

SUPPORTED_SCOPE_BINDINGS = {"snapshot", "inherit"}
SUPPORTED_FILTER_OPS = {"eq", "ne", "in", "not_in", "exists", "not_null", "missing", "gt", "gte", "lt", "lte", "date_gte", "date_lte"}


@dataclass(frozen=True)
class SavedAnalyticsQuery:
    saved_query_id: str
    name: str
    description: str
    tags: tuple[str, ...]
    created_at: str
    updated_at: str
    catalog_version: str
    query_payload: dict[str, Any]
    scope_binding: str = "inherit"
    diagnostic_only: bool = True
    production_recommendation: bool = False
    trading_gate: bool = False

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": SAVED_QUERY_SCHEMA_VERSION,
            "saved_query_id": self.saved_query_id,
            "name": self.name,
            "description": self.description,
            "tags": list(self.tags),
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "catalog_version": self.catalog_version,
            "query_payload": self.query_payload,
            "scope_binding": self.scope_binding,
            "diagnostic_only": self.diagnostic_only,
            "production_recommendation": self.production_recommendation,
            "trading_gate": self.trading_gate,
        }


@dataclass(frozen=True)
class SavedQueryValidation:
    saved_query_id: str
    status: str
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    stale_identifiers: tuple[str, ...]
    repair_required: bool

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": VALIDATION_SCHEMA_VERSION,
            "saved_query_id": self.saved_query_id,
            "status": self.status,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "stale_identifiers": list(self.stale_identifiers),
            "repair_required": self.repair_required,
        }


@dataclass(frozen=True)
class SavedQueryRunResult:
    saved_query: SavedAnalyticsQuery
    validation: SavedQueryValidation
    result: dict[str, Any] | None
    execution_record: dict[str, Any] | None = None


@dataclass(frozen=True)
class CanonicalAnalyticsExecutionRecord:
    execution_id: str
    generated_at: str
    query_id: str
    saved_query_hash: str
    catalog_version: str
    engine_version: str
    input_artifact_refs: dict[str, str]
    input_artifact_hashes: dict[str, str | None]
    outcome_count_total: int
    outcome_count_matched: int
    group_count: int
    filters_applied: tuple[dict[str, Any], ...]
    dimensions: tuple[str, ...]
    metrics: tuple[str, ...]
    context_validity_rules: tuple[str, ...]
    sample_class_counts: dict[str, int]
    data_quality_flags: tuple[str, ...]
    query_fingerprint: str
    result_fingerprint: str
    diagnostic_only: bool = True
    production_recommendation: bool = False
    trading_gate: bool = False

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": EXECUTION_RECORD_SCHEMA_VERSION,
            "execution_id": self.execution_id,
            "generated_at": self.generated_at,
            "query_id": self.query_id,
            "saved_query_hash": self.saved_query_hash,
            "catalog_version": self.catalog_version,
            "engine_version": self.engine_version,
            "input_artifact_refs": self.input_artifact_refs,
            "input_artifact_hashes": self.input_artifact_hashes,
            "outcome_count_total": self.outcome_count_total,
            "outcome_count_matched": self.outcome_count_matched,
            "group_count": self.group_count,
            "filters_applied": list(self.filters_applied),
            "dimensions": list(self.dimensions),
            "metrics": list(self.metrics),
            "context_validity_rules": list(self.context_validity_rules),
            "sample_class_counts": self.sample_class_counts,
            "data_quality_flags": list(self.data_quality_flags),
            "query_fingerprint": self.query_fingerprint,
            "result_fingerprint": self.result_fingerprint,
            "diagnostic_only": self.diagnostic_only,
            "production_recommendation": self.production_recommendation,
            "trading_gate": self.trading_gate,
        }


@dataclass(frozen=True)
class CanonicalAnalyticsResultDiff:
    diff_id: str
    generated_at: str
    status: str
    comparison_classification: str
    previous_execution_id: str | None
    current_execution_id: str | None
    query_id: str | None
    result_fingerprint_changed: bool | None
    matched_outcome_count_delta: int | None
    group_count_delta: int | None
    new_groups: tuple[str, ...]
    removed_groups: tuple[str, ...]
    changed_metric_values: tuple[dict[str, Any], ...]
    top_positive_movers: tuple[dict[str, Any], ...]
    top_negative_movers: tuple[dict[str, Any], ...]
    sample_class_changes: tuple[dict[str, Any], ...]
    data_quality_flag_changes: dict[str, Any]
    guardrail_notes: tuple[str, ...]
    diagnostic_only: bool = True
    production_recommendation: bool = False
    trading_gate: bool = False

    def to_record(self) -> dict[str, Any]:
        return {
            "schema_version": RESULT_DIFF_SCHEMA_VERSION,
            "diff_id": self.diff_id,
            "generated_at": self.generated_at,
            "status": self.status,
            "comparison_classification": self.comparison_classification,
            "previous_execution_id": self.previous_execution_id,
            "current_execution_id": self.current_execution_id,
            "query_id": self.query_id,
            "result_fingerprint_changed": self.result_fingerprint_changed,
            "matched_outcome_count_delta": self.matched_outcome_count_delta,
            "group_count_delta": self.group_count_delta,
            "new_groups": list(self.new_groups),
            "removed_groups": list(self.removed_groups),
            "changed_metric_values": list(self.changed_metric_values),
            "top_positive_movers": list(self.top_positive_movers),
            "top_negative_movers": list(self.top_negative_movers),
            "sample_class_changes": list(self.sample_class_changes),
            "data_quality_flag_changes": self.data_quality_flag_changes,
            "guardrail_notes": list(self.guardrail_notes),
            "diagnostic_only": self.diagnostic_only,
            "production_recommendation": self.production_recommendation,
            "trading_gate": self.trading_gate,
        }


def build_saved_query(
    *,
    saved_query_id: str,
    name: str,
    description: str,
    query: CanonicalAnalyticsQuery,
    tags: Sequence[str] = (),
    scope_binding: str = "inherit",
    created_at: datetime | str | None = None,
    updated_at: datetime | str | None = None,
    catalog_version: str | None = None,
) -> SavedAnalyticsQuery:
    created = coerce_datetime(created_at).isoformat() if created_at else datetime.now(UTC).isoformat()
    updated = coerce_datetime(updated_at).isoformat() if updated_at else created
    catalog = CanonicalAnalyticsEngine().catalog()
    return SavedAnalyticsQuery(
        saved_query_id=saved_query_id,
        name=name,
        description=description,
        tags=tuple(tags),
        created_at=created,
        updated_at=updated,
        catalog_version=catalog_version or str(catalog.get("schema_version")),
        query_payload=canonical_query_to_payload(query),
        scope_binding=scope_binding,
    )


def canonical_query_to_payload(query: CanonicalAnalyticsQuery) -> dict[str, Any]:
    return {
        "name": query.name,
        "dimensions": list(query.dimensions),
        "metrics": list(query.metrics),
        "filters": [_filter_to_payload(filter_spec) for filter_spec in query.filters],
        "named_filters": list(query.named_filters),
        "validity_requirements": list(query.validity_requirements),
        "order_by": list(query.order_by),
        "order_descending": query.order_descending,
        "min_sample_size": query.min_sample_size,
        "min_sample_class": query.min_sample_class,
        "missing_value": query.missing_value,
    }


def query_from_payload(payload: Mapping[str, Any]) -> CanonicalAnalyticsQuery:
    filters = tuple(_filter_from_payload(item) for item in payload.get("filters") or [])
    return CanonicalAnalyticsQuery(
        name=str(payload.get("name") or "saved_query"),
        dimensions=tuple(str(item) for item in payload.get("dimensions") or ()),
        metrics=tuple(str(item) for item in payload.get("metrics") or ()),
        filters=filters,
        named_filters=tuple(str(item) for item in payload.get("named_filters") or ()),
        validity_requirements=tuple(str(item) for item in payload.get("validity_requirements") or ()),
        order_by=tuple(str(item) for item in payload.get("order_by") or ("sample_rank", "average_pnl_proxy")),
        order_descending=bool(payload.get("order_descending", True)),
        min_sample_size=int(payload.get("min_sample_size") or 0),
        min_sample_class=payload.get("min_sample_class"),
        missing_value=str(payload.get("missing_value") or "UNKNOWN"),
    )


def validate_saved_query(saved_query: SavedAnalyticsQuery | Mapping[str, Any], *, engine: CanonicalAnalyticsEngine | None = None) -> SavedQueryValidation:
    saved = saved_query if isinstance(saved_query, SavedAnalyticsQuery) else saved_query_from_record(saved_query)
    catalog = (engine or CanonicalAnalyticsEngine()).catalog()
    errors: list[str] = []
    warnings: list[str] = []
    stale: list[str] = []
    if saved.diagnostic_only is not True:
        errors.append("diagnostic_only_must_be_true")
    if saved.production_recommendation is not False:
        errors.append("production_recommendation_must_be_false")
    if saved.trading_gate is not False:
        errors.append("trading_gate_must_be_false")
    if saved.scope_binding not in SUPPORTED_SCOPE_BINDINGS:
        errors.append(f"unsupported_scope_binding:{saved.scope_binding}")
    if not saved.saved_query_id:
        errors.append("missing_saved_query_id")
    if saved.catalog_version != catalog.get("schema_version"):
        warnings.append(f"catalog_version_mismatch:{saved.catalog_version}->{catalog.get('schema_version')}")
    payload = saved.query_payload or {}
    dimensions = set(catalog.get("dimensions") or {})
    metrics = set(catalog.get("metrics") or {})
    filters = set(catalog.get("filters") or {})
    for dimension in payload.get("dimensions") or []:
        if dimension not in dimensions:
            stale.append(f"dimension:{dimension}")
    for metric in payload.get("metrics") or []:
        if metric not in metrics:
            stale.append(f"metric:{metric}")
    for named_filter in payload.get("named_filters") or []:
        if named_filter not in filters:
            stale.append(f"filter:{named_filter}")
    for validity in payload.get("validity_requirements") or []:
        if validity not in dimensions and validity not in filters:
            stale.append(f"validity_requirement:{validity}")
    for filter_payload in payload.get("filters") or []:
        op = str(filter_payload.get("op") or "")
        if op not in SUPPORTED_FILTER_OPS:
            stale.append(f"filter_op:{op}")
    status = "VALID"
    if errors:
        status = "ERROR"
    elif stale:
        status = "REPAIR_REQUIRED"
    return SavedQueryValidation(
        saved_query_id=saved.saved_query_id,
        status=status,
        errors=tuple(errors),
        warnings=tuple(warnings),
        stale_identifiers=tuple(stale),
        repair_required=bool(stale),
    )


def save_saved_query(saved_query: SavedAnalyticsQuery, *, output_dir: Path = DEFAULT_OUTPUT_DIR) -> Path:
    validation = validate_saved_query(saved_query)
    if validation.status != "VALID":
        raise ValueError(f"saved query validation failed: {validation.to_record()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / SAVED_QUERIES_JSONL
    existing = {row.saved_query_id: row for row in load_saved_queries(path)}
    existing[saved_query.saved_query_id] = saved_query
    _write_saved_queries(path, existing.values())
    return path


def load_saved_queries(path: Path | None = None) -> list[SavedAnalyticsQuery]:
    resolved = path or DEFAULT_OUTPUT_DIR / SAVED_QUERIES_JSONL
    if not resolved.exists():
        return []
    rows: list[SavedAnalyticsQuery] = []
    for line in resolved.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(saved_query_from_record(json.loads(line)))
    return rows


def saved_query_from_record(record: Mapping[str, Any]) -> SavedAnalyticsQuery:
    return SavedAnalyticsQuery(
        saved_query_id=str(record.get("saved_query_id") or ""),
        name=str(record.get("name") or ""),
        description=str(record.get("description") or ""),
        tags=tuple(str(item) for item in record.get("tags") or ()),
        created_at=str(record.get("created_at") or ""),
        updated_at=str(record.get("updated_at") or ""),
        catalog_version=str(record.get("catalog_version") or ""),
        query_payload=dict(record.get("query_payload") or {}),
        scope_binding=str(record.get("scope_binding") or "inherit"),
        diagnostic_only=bool(record.get("diagnostic_only", True)),
        production_recommendation=bool(record.get("production_recommendation", False)),
        trading_gate=bool(record.get("trading_gate", False)),
    )


def preset_saved_queries(*, now: datetime | str | None = None) -> list[SavedAnalyticsQuery]:
    timestamp = coerce_datetime(now).isoformat() if now else datetime.now(UTC).isoformat()
    presets = [
        ("expectancy_by_strategy", "Expectancy by Strategy", ("strategy",), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
        ("expectancy_by_session", "Expectancy by Session", ("session",), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
        ("expectancy_by_instrument", "Expectancy by Instrument", ("instrument", "contract"), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
        ("expectancy_by_vix_percentile_bucket", "Expectancy by VIX Percentile Bucket", ("vix_percentile_bucket",), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
        ("expectancy_by_valid_gre_label", "Expectancy by Valid GRE Label", ("gre_label",), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
        ("strategy_x_session", "Strategy x Session", ("strategy", "lane", "session"), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
        ("strategy_x_vix_percentile_bucket", "Strategy x VIX Percentile Bucket", ("strategy", "lane", "vix_percentile_bucket"), ("trade_count", "win_rate", "expectancy_proxy", "sample_class")),
    ]
    saved: list[SavedAnalyticsQuery] = []
    for saved_query_id, name, dimensions, metrics in presets:
        query = CanonicalAnalyticsQuery(name=saved_query_id, dimensions=dimensions, metrics=metrics)
        saved.append(
            build_saved_query(
                saved_query_id=saved_query_id,
                name=name,
                description=f"Built-in diagnostic preset: {name}.",
                tags=("preset", "diagnostic", "analytics"),
                query=query,
                scope_binding="inherit",
                created_at=timestamp,
                updated_at=timestamp,
            )
        )
    return saved


def publish_saved_query_artifacts(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_at = coerce_datetime(now).isoformat() if now else datetime.now(UTC).isoformat()
    presets = preset_saved_queries(now=generated_at)
    saved_path = output_dir / SAVED_QUERIES_JSONL
    if not saved_path.exists():
        _write_saved_queries(saved_path, presets)
    preset_path = output_dir / PRESETS_JSON
    preset_path.write_text(json.dumps([preset.to_record() for preset in presets], indent=2, sort_keys=True) + "\n", encoding="utf-8")
    saved_queries = load_saved_queries(saved_path)
    validations = [validate_saved_query(query) for query in saved_queries]
    validation_path = output_dir / VALIDATION_REPORT_MD
    validation_path.write_text(render_validation_report(validations, generated_at=generated_at), encoding="utf-8")
    contract_path = output_dir / CONTRACT_MD
    contract_path.write_text(render_saved_query_contract(), encoding="utf-8")
    summary = build_saved_query_summary(
        saved_queries=saved_queries,
        presets=presets,
        validations=validations,
        generated_at=generated_at,
        output_dir=output_dir,
    )
    summary_path = output_dir / SUMMARY_JSON
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "summary": summary,
        "saved_queries_path": saved_path,
        "presets_path": preset_path,
        "validation_report_path": validation_path,
        "contract_path": contract_path,
        "summary_path": summary_path,
    }


def build_saved_query_summary(
    *,
    saved_queries: Sequence[SavedAnalyticsQuery],
    presets: Sequence[SavedAnalyticsQuery],
    validations: Sequence[SavedQueryValidation],
    generated_at: str,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for validation in validations:
        status_counts[validation.status] = status_counts.get(validation.status, 0) + 1
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at,
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "saved_query_count": len(saved_queries),
        "preset_count": len(presets),
        "validation_status_counts": dict(sorted(status_counts.items())),
        "preset_ids": [preset.saved_query_id for preset in presets],
        "storage": {
            "format": "jsonl",
            "directory": str(output_dir),
            "file": SAVED_QUERIES_JSONL,
        },
        "safety_contract": {
            "broker_actions": False,
            "order_actions": False,
            "position_changes": False,
            "global_cancel": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "production_behavior_changes": False,
            "databento_download": False,
        },
    }


def run_saved_query(
    saved_query: SavedAnalyticsQuery,
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    write_execution_audit: bool = False,
    generated_at: datetime | str | None = None,
) -> SavedQueryRunResult:
    validation = validate_saved_query(saved_query)
    if validation.status != "VALID":
        return SavedQueryRunResult(saved_query=saved_query, validation=validation, result=None)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    rows = [_with_query_buckets(row) for row in merge_outcomes_with_enrichment(outcomes, enrichments)]
    generated = coerce_datetime(generated_at)
    query = query_from_payload(saved_query.query_payload)
    result = CanonicalAnalyticsEngine().run(
        rows,
        query,
        generated_at=generated,
        provenance={"canonical_trade_outcomes": str(outcomes_path), "trade_outcome_enrichment": str(enrichments_path)},
    )
    result_record = result.to_dict()
    execution_record = build_execution_record(
        saved_query=saved_query,
        query=query,
        result=result_record,
        generated_at=generated,
        outcomes_path=outcomes_path,
        enrichments_path=enrichments_path,
        outcome_count_total=len(outcomes),
    )
    if write_execution_audit:
        publish_execution_audit(output_dir=output_dir, execution_record=execution_record)
        publish_execution_result_snapshot(output_dir=output_dir, execution_record=execution_record, result=result_record)
    return SavedQueryRunResult(saved_query=saved_query, validation=validation, result=result_record, execution_record=execution_record.to_record())


def build_execution_record(
    *,
    saved_query: SavedAnalyticsQuery,
    query: CanonicalAnalyticsQuery,
    result: Mapping[str, Any],
    generated_at: datetime | str,
    outcomes_path: Path,
    enrichments_path: Path,
    outcome_count_total: int,
) -> CanonicalAnalyticsExecutionRecord:
    catalog_version = str(CanonicalAnalyticsEngine().catalog().get("schema_version"))
    summary = dict(result.get("summary") or {})
    grouped_rows = list(result.get("grouped_rows") or [])
    validity_metadata = dict(result.get("validity_metadata") or {})
    sample_class_counts = _sample_class_counts(grouped_rows)
    data_quality_flags = _data_quality_flags(grouped_rows)
    query_fingerprint = deterministic_query_fingerprint(saved_query.query_payload, catalog_version=catalog_version)
    result_fingerprint = deterministic_result_fingerprint(result)
    saved_query_hash = stable_hash(saved_query.to_record())
    execution_id = stable_hash(
        {
            "query_id": saved_query.saved_query_id,
            "saved_query_hash": saved_query_hash,
            "query_fingerprint": query_fingerprint,
            "result_fingerprint": result_fingerprint,
            "input_artifact_hashes": {
                "canonical_trade_outcomes": file_sha256(outcomes_path),
                "trade_outcome_enrichment": file_sha256(enrichments_path),
            },
        }
    )[:24]
    return CanonicalAnalyticsExecutionRecord(
        execution_id=execution_id,
        generated_at=coerce_datetime(generated_at).isoformat(),
        query_id=saved_query.saved_query_id,
        saved_query_hash=saved_query_hash,
        catalog_version=catalog_version,
        engine_version="track_b_canonical_analytics_engine_v1",
        input_artifact_refs={"canonical_trade_outcomes": str(outcomes_path), "trade_outcome_enrichment": str(enrichments_path)},
        input_artifact_hashes={"canonical_trade_outcomes": file_sha256(outcomes_path), "trade_outcome_enrichment": file_sha256(enrichments_path)},
        outcome_count_total=outcome_count_total,
        outcome_count_matched=int(summary.get("matched_count") or 0),
        group_count=int(summary.get("group_count") or 0),
        filters_applied=tuple(dict(item) for item in validity_metadata.get("filters") or ()),
        dimensions=tuple(query.dimensions),
        metrics=tuple(query.metrics),
        context_validity_rules=tuple(query.validity_requirements),
        sample_class_counts=sample_class_counts,
        data_quality_flags=data_quality_flags,
        query_fingerprint=query_fingerprint,
        result_fingerprint=result_fingerprint,
    )


def publish_execution_audit(*, output_dir: Path, execution_record: CanonicalAnalyticsExecutionRecord) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / EXECUTION_LOG_JSONL
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(execution_record.to_record(), sort_keys=True) + "\n")
    summary_path = output_dir / EXECUTION_SUMMARY_JSON
    summary_path.write_text(json.dumps(build_execution_summary(execution_record), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    contract_path = output_dir / EXECUTION_CONTRACT_MD
    contract_path.write_text(render_execution_audit_contract(), encoding="utf-8")
    manifest_path = output_dir / RESULT_MANIFEST_MD
    manifest_path.write_text(render_result_manifest(execution_record), encoding="utf-8")
    provenance_path = output_dir / RESULT_PROVENANCE_REPORT_MD
    provenance_path.write_text(render_result_provenance_report(execution_record), encoding="utf-8")
    return {
        "execution_log_path": log_path,
        "execution_summary_path": summary_path,
        "execution_contract_path": contract_path,
        "result_manifest_path": manifest_path,
        "result_provenance_report_path": provenance_path,
    }


def publish_execution_result_snapshot(*, output_dir: Path, execution_record: CanonicalAnalyticsExecutionRecord, result: Mapping[str, Any]) -> Path:
    snapshot_dir = output_dir / RESULT_SNAPSHOT_DIR
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    path = snapshot_dir / f"{execution_record.execution_id}.json"
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def build_execution_summary(execution_record: CanonicalAnalyticsExecutionRecord) -> dict[str, Any]:
    record = execution_record.to_record()
    return {
        "schema_version": "cae6_latest_execution_summary_v1",
        "generated_at": record["generated_at"],
        "latest_execution_id": record["execution_id"],
        "query_id": record["query_id"],
        "outcome_count_total": record["outcome_count_total"],
        "outcome_count_matched": record["outcome_count_matched"],
        "group_count": record["group_count"],
        "query_fingerprint": record["query_fingerprint"],
        "result_fingerprint": record["result_fingerprint"],
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def deterministic_query_fingerprint(query_payload: Mapping[str, Any], *, catalog_version: str | None = None) -> str:
    return stable_hash({"catalog_version": catalog_version, "query_payload": query_payload})


def deterministic_result_fingerprint(result: Mapping[str, Any]) -> str:
    return stable_hash({
        "schema_version": result.get("schema_version"),
        "query": result.get("query"),
        "grouped_rows": result.get("grouped_rows"),
        "summary": result.get("summary"),
        "validity_metadata": result.get("validity_metadata"),
        "diagnostic_only": result.get("diagnostic_only"),
        "production_recommendation": result.get("production_recommendation"),
        "trading_gate": result.get("trading_gate"),
    })


def stable_hash(payload: Mapping[str, Any] | Sequence[Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def file_sha256(path: Path) -> str | None:
    if not path.exists():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def render_execution_audit_contract() -> str:
    return "\n".join(
        [
            "# CAE6 Execution Audit Contract",
            "",
            f"- Schema version: `{EXECUTION_RECORD_SCHEMA_VERSION}`",
            "- Execution logs are diagnostic JSONL records.",
            "- Fingerprints exclude generation timestamps so equivalent query/result payloads are reproducible.",
            "- Execution records preserve input artifact refs, input hashes, dimensions, metrics, filters, validity rules, sample classes, and guardrails.",
            "- Guardrails remain `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`.",
            "",
        ]
    )


def render_result_manifest(execution_record: CanonicalAnalyticsExecutionRecord) -> str:
    record = execution_record.to_record()
    lines = [
        "# CAE6 Result Manifest",
        "",
        f"- Execution id: `{record['execution_id']}`",
        f"- Query id: `{record['query_id']}`",
        f"- Generated at: `{record['generated_at']}`",
        f"- Catalog version: `{record['catalog_version']}`",
        f"- Query fingerprint: `{record['query_fingerprint']}`",
        f"- Result fingerprint: `{record['result_fingerprint']}`",
        f"- Matched outcomes: `{record['outcome_count_matched']}`",
        f"- Groups: `{record['group_count']}`",
        "",
        "Diagnostic/research only. This manifest has no runtime, broker, strategy, or trading-gate authority.",
        "",
    ]
    return "\n".join(lines)


def render_result_provenance_report(execution_record: CanonicalAnalyticsExecutionRecord) -> str:
    record = execution_record.to_record()
    lines = [
        "# CAE6 Result Provenance Report",
        "",
        f"- Query id: `{record['query_id']}`",
        f"- Saved query hash: `{record['saved_query_hash']}`",
        f"- Engine version: `{record['engine_version']}`",
        "",
        "## Inputs",
        "",
    ]
    for key, ref in record["input_artifact_refs"].items():
        lines.append(f"- `{key}`: `{ref}`")
        lines.append(f"  - sha256: `{record['input_artifact_hashes'].get(key)}`")
    lines.extend([
        "",
        "## Query Shape",
        "",
        f"- Dimensions: `{', '.join(record['dimensions'])}`",
        f"- Metrics: `{', '.join(record['metrics'])}`",
        f"- Context validity rules: `{', '.join(record['context_validity_rules'])}`",
        "",
    ])
    return "\n".join(lines)


def load_execution_records(path: Path | None = None) -> list[dict[str, Any]]:
    resolved = path or DEFAULT_OUTPUT_DIR / EXECUTION_LOG_JSONL
    if not resolved.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in resolved.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def load_result_snapshot(output_dir: Path, execution_id: str | None) -> dict[str, Any] | None:
    if not execution_id:
        return None
    path = output_dir / RESULT_SNAPSHOT_DIR / f"{execution_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def compare_latest_execution_for_query(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    saved_query_id: str,
    generated_at: datetime | str | None = None,
) -> CanonicalAnalyticsResultDiff:
    records = [record for record in load_execution_records(output_dir / EXECUTION_LOG_JSONL) if record.get("query_id") == saved_query_id]
    if len(records) < 2:
        current = records[-1] if records else None
        return no_prior_execution_diff(current_execution=current, generated_at=generated_at)
    return compare_execution_records(
        records[-2],
        records[-1],
        previous_result=load_result_snapshot(output_dir, records[-2].get("execution_id")),
        current_result=load_result_snapshot(output_dir, records[-1].get("execution_id")),
        generated_at=generated_at,
    )


def compare_execution_ids(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    previous_execution_id: str,
    current_execution_id: str,
    generated_at: datetime | str | None = None,
) -> CanonicalAnalyticsResultDiff:
    records = {record.get("execution_id"): record for record in load_execution_records(output_dir / EXECUTION_LOG_JSONL)}
    previous = records.get(previous_execution_id)
    current = records.get(current_execution_id)
    if previous is None or current is None:
        return CanonicalAnalyticsResultDiff(
            diff_id=stable_hash({"previous": previous_execution_id, "current": current_execution_id, "status": "EXECUTION_NOT_FOUND"})[:24],
            generated_at=coerce_datetime(generated_at).isoformat(),
            status="EXECUTION_NOT_FOUND",
            comparison_classification="NOT_COMPARABLE",
            previous_execution_id=previous_execution_id,
            current_execution_id=current_execution_id,
            query_id=None,
            result_fingerprint_changed=None,
            matched_outcome_count_delta=None,
            group_count_delta=None,
            new_groups=(),
            removed_groups=(),
            changed_metric_values=(),
            top_positive_movers=(),
            top_negative_movers=(),
            sample_class_changes=(),
            data_quality_flag_changes={},
            guardrail_notes=("execution_id_not_found",),
        )
    return compare_execution_records(
        previous,
        current,
        previous_result=load_result_snapshot(output_dir, previous_execution_id),
        current_result=load_result_snapshot(output_dir, current_execution_id),
        generated_at=generated_at,
    )


def no_prior_execution_diff(*, current_execution: Mapping[str, Any] | None, generated_at: datetime | str | None = None) -> CanonicalAnalyticsResultDiff:
    current_id = str(current_execution.get("execution_id")) if current_execution else None
    query_id = str(current_execution.get("query_id")) if current_execution else None
    return CanonicalAnalyticsResultDiff(
        diff_id=stable_hash({"current_execution_id": current_id, "status": "NO_PRIOR_EXECUTION"})[:24],
        generated_at=coerce_datetime(generated_at).isoformat(),
        status="NO_PRIOR_EXECUTION",
        comparison_classification="NOT_COMPARABLE",
        previous_execution_id=None,
        current_execution_id=current_id,
        query_id=query_id,
        result_fingerprint_changed=None,
        matched_outcome_count_delta=None,
        group_count_delta=None,
        new_groups=(),
        removed_groups=(),
        changed_metric_values=(),
        top_positive_movers=(),
        top_negative_movers=(),
        sample_class_changes=(),
        data_quality_flag_changes={},
        guardrail_notes=("no_prior_execution_available",),
    )


def compare_execution_records(
    previous_execution: Mapping[str, Any],
    current_execution: Mapping[str, Any],
    *,
    previous_result: Mapping[str, Any] | None = None,
    current_result: Mapping[str, Any] | None = None,
    generated_at: datetime | str | None = None,
) -> CanonicalAnalyticsResultDiff:
    guardrails = ["diagnostic_only=true", "production_recommendation=false", "trading_gate=false"]
    if previous_execution.get("query_fingerprint") != current_execution.get("query_fingerprint"):
        return _not_comparable_diff(previous_execution, current_execution, "QUERY_CHANGED", guardrails, generated_at=generated_at)
    if previous_execution.get("catalog_version") != current_execution.get("catalog_version"):
        return _not_comparable_diff(previous_execution, current_execution, "CATALOG_CHANGED", guardrails, generated_at=generated_at)
    result_changed = previous_execution.get("result_fingerprint") != current_execution.get("result_fingerprint")
    matched_delta = int(current_execution.get("outcome_count_matched") or 0) - int(previous_execution.get("outcome_count_matched") or 0)
    group_delta = int(current_execution.get("group_count") or 0) - int(previous_execution.get("group_count") or 0)
    previous_groups = _groups_by_key(previous_result)
    current_groups = _groups_by_key(current_result)
    new_groups = tuple(sorted(set(current_groups) - set(previous_groups)))
    removed_groups = tuple(sorted(set(previous_groups) - set(current_groups)))
    metric_changes = _metric_changes(previous_groups, current_groups)
    sample_class_changes = _sample_class_changes_between(previous_groups, current_groups)
    data_quality_flag_changes = _flag_changes(previous_execution.get("data_quality_flags") or (), current_execution.get("data_quality_flags") or ())
    top_positive = tuple(sorted((item for item in metric_changes if item.get("direction") == "IMPROVED"), key=lambda row: abs(float(row.get("absolute_delta") or 0)), reverse=True)[:10])
    top_negative = tuple(sorted((item for item in metric_changes if item.get("direction") == "DETERIORATED"), key=lambda row: abs(float(row.get("absolute_delta") or 0)), reverse=True)[:10])
    classification = _comparison_classification(result_changed=result_changed, matched_delta=matched_delta, group_delta=group_delta, metric_changes=metric_changes)
    status = "CHANGED" if result_changed else "UNCHANGED"
    diff_id = stable_hash({
        "previous": previous_execution.get("execution_id"),
        "current": current_execution.get("execution_id"),
        "classification": classification,
        "result_changed": result_changed,
    })[:24]
    return CanonicalAnalyticsResultDiff(
        diff_id=diff_id,
        generated_at=coerce_datetime(generated_at).isoformat(),
        status=status,
        comparison_classification=classification,
        previous_execution_id=str(previous_execution.get("execution_id")),
        current_execution_id=str(current_execution.get("execution_id")),
        query_id=str(current_execution.get("query_id") or previous_execution.get("query_id") or ""),
        result_fingerprint_changed=result_changed,
        matched_outcome_count_delta=matched_delta,
        group_count_delta=group_delta,
        new_groups=new_groups,
        removed_groups=removed_groups,
        changed_metric_values=tuple(metric_changes),
        top_positive_movers=top_positive,
        top_negative_movers=top_negative,
        sample_class_changes=tuple(sample_class_changes),
        data_quality_flag_changes=data_quality_flag_changes,
        guardrail_notes=tuple(guardrails),
    )


def publish_result_diff(*, output_dir: Path, diff: CanonicalAnalyticsResultDiff) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / RESULT_DIFF_JSON
    json_path.write_text(json.dumps(diff.to_record(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    contract_path = output_dir / RESULT_DIFF_CONTRACT_MD
    contract_path.write_text(render_result_diff_contract(), encoding="utf-8")
    md_path = output_dir / RESULT_DIFF_MD
    md_path.write_text(render_result_diff_markdown(diff), encoding="utf-8")
    change_feed_path = output_dir / CHANGE_FEED_MD
    change_feed_path.write_text(render_change_feed(diff), encoding="utf-8")
    return {
        "result_diff_json_path": json_path,
        "result_diff_contract_path": contract_path,
        "result_diff_md_path": md_path,
        "change_feed_path": change_feed_path,
    }


def render_result_diff_contract() -> str:
    return "\n".join([
        "# CAE7 Result Diff Contract",
        "",
        f"- Schema version: `{RESULT_DIFF_SCHEMA_VERSION}`",
        "- Compares compatible CAE execution records and result snapshots.",
        "- `QUERY_CHANGED` and `CATALOG_CHANGED` are not comparable guardrail classifications.",
        "- Deltas are diagnostic only and never imply production action, strategy changes, or gates.",
        "",
    ])


def render_result_diff_markdown(diff: CanonicalAnalyticsResultDiff) -> str:
    record = diff.to_record()
    lines = [
        "# CAE7 Latest Result Diff",
        "",
        f"- Status: `{record['status']}`",
        f"- Classification: `{record['comparison_classification']}`",
        f"- Query: `{record['query_id']}`",
        f"- Previous execution: `{record['previous_execution_id']}`",
        f"- Current execution: `{record['current_execution_id']}`",
        f"- Matched outcome delta: `{record['matched_outcome_count_delta']}`",
        f"- Group count delta: `{record['group_count_delta']}`",
        f"- New groups: `{len(record['new_groups'])}`",
        f"- Removed groups: `{len(record['removed_groups'])}`",
        f"- Changed metrics: `{len(record['changed_metric_values'])}`",
        "",
        "Diagnostic/research only. No production recommendation or trading gate is produced.",
        "",
    ]
    return "\n".join(lines)


def render_change_feed(diff: CanonicalAnalyticsResultDiff) -> str:
    record = diff.to_record()
    lines = [
        "# CAE7 Change Feed",
        "",
        f"- `{record['query_id']}`: `{record['status']}` / `{record['comparison_classification']}`",
    ]
    if record["top_positive_movers"]:
        lines.extend(["", "## Top Positive Movers", ""])
        for item in record["top_positive_movers"][:5]:
            lines.append(f"- `{item['group_key']}` `{item['metric']}`: `{item['previous_value']}` -> `{item['current_value']}`")
    if record["top_negative_movers"]:
        lines.extend(["", "## Top Negative Movers", ""])
        for item in record["top_negative_movers"][:5]:
            lines.append(f"- `{item['group_key']}` `{item['metric']}`: `{item['previous_value']}` -> `{item['current_value']}`")
    lines.append("")
    return "\n".join(lines)


def render_validation_report(validations: Sequence[SavedQueryValidation], *, generated_at: str) -> str:
    lines = [
        "# Saved Query Validation Report",
        "",
        f"- Generated at: `{generated_at}`",
        "",
        "| Saved query | Status | Repair required | Errors | Stale identifiers | Warnings |",
        "|---|---:|---:|---|---|---|",
    ]
    for validation in validations:
        lines.append(
            f"| `{validation.saved_query_id}` | `{validation.status}` | `{validation.repair_required}` | "
            f"{', '.join(validation.errors)} | {', '.join(validation.stale_identifiers)} | {', '.join(validation.warnings)} |"
        )
    lines.extend(["", "Diagnostic/research only. Saved queries do not create trading gates or production recommendations.", ""])
    return "\n".join(lines)


def render_saved_query_contract() -> str:
    return "\n".join(
        [
            "# Saved Query Contract",
            "",
            f"- Schema version: `{SAVED_QUERY_SCHEMA_VERSION}`",
            "- Storage: deterministic JSONL",
            "- Scope bindings: `snapshot`, `inherit`",
            "- Guardrails: `diagnostic_only=true`, `production_recommendation=false`, `trading_gate=false`",
            "",
            "Saved query records include `saved_query_id`, `name`, `description`, `tags`, timestamps, `catalog_version`, `query_payload`, and `scope_binding`.",
            "",
            "Validation checks dimensions, metrics, named filters, explicit filter operations, scope binding, and guardrail fields against the live CAE catalog.",
            "",
            "Invalid or stale identifiers are reported as `ERROR` or `REPAIR_REQUIRED`; they are not silently dropped.",
            "",
        ]
    )


def _sample_class_counts(grouped_rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in grouped_rows:
        sample_class = str(row.get("sample_class") or "UNKNOWN")
        counts[sample_class] = counts.get(sample_class, 0) + 1
    return dict(sorted(counts.items()))


def _data_quality_flags(grouped_rows: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    flags: set[str] = set()
    for row in grouped_rows:
        for flag in row.get("data_quality_flags") or ():
            flags.add(str(flag))
    return tuple(sorted(flags))


def _not_comparable_diff(
    previous_execution: Mapping[str, Any],
    current_execution: Mapping[str, Any],
    reason: str,
    guardrails: Sequence[str],
    *,
    generated_at: datetime | str | None = None,
) -> CanonicalAnalyticsResultDiff:
    return CanonicalAnalyticsResultDiff(
        diff_id=stable_hash({"previous": previous_execution.get("execution_id"), "current": current_execution.get("execution_id"), "reason": reason})[:24],
        generated_at=coerce_datetime(generated_at).isoformat(),
        status=reason,
        comparison_classification=reason,
        previous_execution_id=str(previous_execution.get("execution_id")),
        current_execution_id=str(current_execution.get("execution_id")),
        query_id=str(current_execution.get("query_id") or previous_execution.get("query_id") or ""),
        result_fingerprint_changed=None,
        matched_outcome_count_delta=None,
        group_count_delta=None,
        new_groups=(),
        removed_groups=(),
        changed_metric_values=(),
        top_positive_movers=(),
        top_negative_movers=(),
        sample_class_changes=(),
        data_quality_flag_changes={},
        guardrail_notes=tuple(guardrails),
    )


def _groups_by_key(result: Mapping[str, Any] | None) -> dict[str, Mapping[str, Any]]:
    if not result:
        return {}
    groups: dict[str, Mapping[str, Any]] = {}
    for row in result.get("grouped_rows") or ():
        key = str(row.get("key") or json.dumps(row.get("dimensions") or {}, sort_keys=True))
        groups[key] = row
    return groups


def _metric_changes(previous_groups: Mapping[str, Mapping[str, Any]], current_groups: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    comparable_keys = sorted(set(previous_groups) & set(current_groups))
    ignored = {"key", "dimensions", "metric_data_quality_flags", "sample_class"}
    for key in comparable_keys:
        previous = previous_groups[key]
        current = current_groups[key]
        metric_names = sorted((set(previous) | set(current)) - ignored)
        for metric in metric_names:
            previous_value = previous.get(metric)
            current_value = current.get(metric)
            if previous_value == current_value:
                continue
            if not _is_number(previous_value) or not _is_number(current_value):
                continue
            absolute_delta = float(current_value) - float(previous_value)
            changes.append({
                "group_key": key,
                "metric": metric,
                "previous_value": previous_value,
                "current_value": current_value,
                "absolute_delta": round(absolute_delta, 6),
                "percent_delta": _percent_delta(float(previous_value), absolute_delta),
                "direction": _delta_direction(metric, absolute_delta),
            })
    return changes


def _sample_class_changes_between(previous_groups: Mapping[str, Mapping[str, Any]], current_groups: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for key in sorted(set(previous_groups) & set(current_groups)):
        previous_class = previous_groups[key].get("sample_class")
        current_class = current_groups[key].get("sample_class")
        if previous_class != current_class:
            changes.append({"group_key": key, "previous_sample_class": previous_class, "current_sample_class": current_class})
    return changes


def _flag_changes(previous_flags: Sequence[Any], current_flags: Sequence[Any]) -> dict[str, Any]:
    previous = {str(flag) for flag in previous_flags}
    current = {str(flag) for flag in current_flags}
    return {"added": sorted(current - previous), "removed": sorted(previous - current), "unchanged": sorted(previous & current)}


def _comparison_classification(*, result_changed: bool, matched_delta: int, group_delta: int, metric_changes: Sequence[Mapping[str, Any]]) -> str:
    if not result_changed:
        return "UNCHANGED"
    directions = {str(item.get("direction")) for item in metric_changes}
    if matched_delta or group_delta:
        return "MIXED"
    if directions == {"IMPROVED"}:
        return "IMPROVED"
    if directions == {"DETERIORATED"}:
        return "DETERIORATED"
    if directions:
        return "MIXED"
    return "MIXED"


def _delta_direction(metric: str, absolute_delta: float) -> str:
    if absolute_delta == 0:
        return "UNCHANGED"
    lower_is_better = {"loss_rate", "average_loser_pnl_proxy", "median_loser_pnl_proxy", "max_loss_pnl_proxy"}
    if metric in lower_is_better:
        return "IMPROVED" if absolute_delta < 0 else "DETERIORATED"
    return "IMPROVED" if absolute_delta > 0 else "DETERIORATED"


def _percent_delta(previous_value: float, absolute_delta: float) -> float | None:
    if previous_value == 0:
        return None
    return round(absolute_delta / abs(previous_value), 6)


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


def _write_saved_queries(path: Path, saved_queries: Sequence[SavedAnalyticsQuery]) -> None:
    rows = sorted(saved_queries, key=lambda query: query.saved_query_id)
    path.write_text("\n".join(json.dumps(row.to_record(), sort_keys=True) for row in rows) + ("\n" if rows else ""), encoding="utf-8")


def _filter_to_payload(filter_spec: AnalyticsFilter) -> dict[str, Any]:
    return {
        "field": filter_spec.field,
        "op": filter_spec.op,
        "value": filter_spec.value,
        "values": list(filter_spec.values),
    }


def _filter_from_payload(payload: Mapping[str, Any]) -> AnalyticsFilter:
    return AnalyticsFilter(
        field=str(payload.get("field") or ""),
        op=str(payload.get("op") or ""),
        value=payload.get("value"),
        values=tuple(payload.get("values") or ()),
    )


def _with_query_buckets(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["vix_percentile_bucket"] = _vix_percentile_bucket(_number(row.get("vix_percentile")))
    payload["gre_confidence_bucket"] = _confidence_bucket(_number(row.get("gre_confidence")))
    return payload


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows
