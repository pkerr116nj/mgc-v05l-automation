"""Saved-query persistence for the Canonical Analytics Engine.

Saved queries are diagnostic/research artifacts only. They serialize
CanonicalAnalyticsQuery payloads for dashboards, APIs, and future AI clients.
They have no runtime, broker, strategy, order, or trading-gate authority.
"""

from __future__ import annotations

import json
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

SAVED_QUERY_SCHEMA_VERSION = "magic_saved_query_v1"
SUMMARY_SCHEMA_VERSION = "cae5_saved_query_summary_v1"
VALIDATION_SCHEMA_VERSION = "cae5_saved_query_validation_v1"

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
) -> SavedQueryRunResult:
    validation = validate_saved_query(saved_query)
    if validation.status != "VALID":
        return SavedQueryRunResult(saved_query=saved_query, validation=validation, result=None)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    rows = [_with_query_buckets(row) for row in merge_outcomes_with_enrichment(outcomes, enrichments)]
    result = CanonicalAnalyticsEngine().run(
        rows,
        query_from_payload(saved_query.query_payload),
        provenance={"canonical_trade_outcomes": str(outcomes_path), "trade_outcome_enrichment": str(enrichments_path)},
    )
    return SavedQueryRunResult(saved_query=saved_query, validation=validation, result=result.to_dict())


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
