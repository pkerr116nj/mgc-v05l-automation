"""Reusable diagnostic aggregation engine for Track B analytics.

The engine accepts records plus explicit dimensions, metrics, filters, and
context-validity rules. It is analytics/research infrastructure only: it has no
runtime, broker, strategy, order, or gate authority.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import median
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "track_b_canonical_analytics_engine_v1"

SAMPLE_RANK = {"RESEARCH_GRADE": 4, "DEVELOPING": 3, "PRELIMINARY": 2, "EXPLORATORY": 1}


@dataclass(frozen=True)
class AnalyticsFilter:
    field: str
    op: str
    value: Any = None
    values: tuple[Any, ...] = ()


@dataclass(frozen=True)
class ContextValidityRule:
    field: str
    valid_values: tuple[str, ...] = ("VALID",)
    required: bool = True


@dataclass(frozen=True)
class AnalyticsMetric:
    name: str
    kind: str
    source_field: str | None = None
    output_field: str | None = None
    percentiles: tuple[int, ...] = ()


@dataclass(frozen=True)
class CanonicalAnalyticsRequest:
    name: str
    dimensions: tuple[str, ...] = ()
    filters: tuple[AnalyticsFilter, ...] = ()
    validity_rules: tuple[ContextValidityRule, ...] = ()
    metrics: tuple[AnalyticsMetric, ...] = field(default_factory=tuple)
    missing_value: str = "UNKNOWN"
    sort_fields: tuple[str, ...] = ("sample_rank", "average_pnl_proxy")
    sort_descending: bool = True


def default_expectancy_metrics() -> tuple[AnalyticsMetric, ...]:
    return (
        AnalyticsMetric("win_rate", "win_rate", source_field="realized_pnl_proxy"),
        AnalyticsMetric("average_pnl_proxy", "average", source_field="realized_pnl_proxy"),
        AnalyticsMetric("median_pnl_proxy", "median", source_field="realized_pnl_proxy"),
        AnalyticsMetric("average_realized_points", "average", source_field="realized_points"),
        AnalyticsMetric("median_realized_points", "median", source_field="realized_points"),
        AnalyticsMetric("pnl_percentiles", "percentiles", source_field="realized_pnl_proxy", percentiles=(0, 10, 25, 50, 75, 90, 100)),
        AnalyticsMetric("best_trade", "best_ref", source_field="realized_pnl_proxy"),
        AnalyticsMetric("worst_trade", "worst_ref", source_field="realized_pnl_proxy"),
        AnalyticsMetric("average_hold_seconds", "average", source_field="hold_seconds"),
        AnalyticsMetric("median_hold_seconds", "median", source_field="hold_seconds"),
        AnalyticsMetric("data_quality_flags", "counts", source_field="data_quality_flags"),
        AnalyticsMetric("enrichment_data_quality_flags", "counts", source_field="enrichment_data_quality_flags"),
    )


def run_canonical_analytics(
    rows: Sequence[Mapping[str, Any]],
    request: CanonicalAnalyticsRequest,
) -> dict[str, Any]:
    metric_specs = request.metrics or default_expectancy_metrics()
    filtered_rows = [row for row in rows if _matches_request(row, request)]
    groups = aggregate_canonical_groups(filtered_rows, request=request, metrics=metric_specs)
    return {
        "schema_version": SCHEMA_VERSION,
        "request": request_to_dict(request),
        "input_count": len(rows),
        "matched_count": len(filtered_rows),
        "group_count": len(groups),
        "groups": groups,
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }


def aggregate_canonical_groups(
    rows: Sequence[Mapping[str, Any]],
    *,
    request: CanonicalAnalyticsRequest,
    metrics: Sequence[AnalyticsMetric] | None = None,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    dimensions_by_key: dict[str, dict[str, Any]] = {}
    for row in rows:
        key, dimension_values = group_key(row, request.dimensions, missing_value=request.missing_value)
        grouped.setdefault(key, []).append(row)
        dimensions_by_key.setdefault(key, dimension_values)
    aggregated = [
        aggregate_metric_group(key, group_rows, metrics=metrics or request.metrics or default_expectancy_metrics(), dimensions=dimensions_by_key.get(key))
        for key, group_rows in grouped.items()
    ]
    return sorted(
        aggregated,
        key=lambda row: tuple(_sort_value(row.get(field)) for field in request.sort_fields),
        reverse=request.sort_descending,
    )


def aggregate_metric_group(
    key: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    metrics: Sequence[AnalyticsMetric] | None = None,
    dimensions: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sample_class = sample_class_for_count(len(rows))
    payload: dict[str, Any] = {
        "key": key,
        "dimensions": dict(dimensions or {}),
        "count": len(rows),
        "sample_class": sample_class,
        "sample_rank": SAMPLE_RANK.get(sample_class, 0),
    }
    for metric in metrics or default_expectancy_metrics():
        output_field = metric.output_field or metric.name
        if metric.kind == "win_rate":
            payload[output_field] = win_rate(rows, metric.source_field or "realized_pnl_proxy")
        elif metric.kind == "average":
            payload[output_field] = average(numeric_values(rows, metric.source_field or ""))
        elif metric.kind == "median":
            payload[output_field] = median_value(numeric_values(rows, metric.source_field or ""))
        elif metric.kind == "percentiles":
            payload[output_field] = percentile_distribution(numeric_values(rows, metric.source_field or ""), percentiles=metric.percentiles)
        elif metric.kind == "best_ref":
            payload[output_field] = trade_ref(max_by(rows, metric.source_field or ""))
        elif metric.kind == "worst_ref":
            payload[output_field] = trade_ref(min_by(rows, metric.source_field or ""))
        elif metric.kind == "counts":
            payload[output_field] = counts(_flatten(row.get(metric.source_field or "") for row in rows))
        else:
            payload[output_field] = None
    return payload


def request_to_dict(request: CanonicalAnalyticsRequest) -> dict[str, Any]:
    return {
        "name": request.name,
        "dimensions": list(request.dimensions),
        "filters": [filter_spec.__dict__ for filter_spec in request.filters],
        "validity_rules": [rule.__dict__ for rule in request.validity_rules],
        "metrics": [metric.__dict__ for metric in request.metrics],
        "missing_value": request.missing_value,
        "sort_fields": list(request.sort_fields),
        "sort_descending": request.sort_descending,
    }


def group_key(row: Mapping[str, Any], dimensions: Sequence[str], *, missing_value: str = "UNKNOWN") -> tuple[str, dict[str, Any]]:
    if not dimensions:
        return "ALL", {}
    values = {field: row.get(field) if row.get(field) not in (None, "") else missing_value for field in dimensions}
    return " | ".join(str(values[field]) for field in dimensions), values


def sample_class_for_count(count: int) -> str:
    if count >= 100:
        return "RESEARCH_GRADE"
    if count >= 30:
        return "DEVELOPING"
    if count >= 10:
        return "PRELIMINARY"
    return "EXPLORATORY"


def percentile_distribution(values: Sequence[float], *, percentiles: Sequence[int] = (0, 10, 25, 50, 75, 90, 100)) -> dict[str, float | None]:
    if not values:
        return {f"p{pct}": None for pct in percentiles}
    sorted_values = sorted(values)
    return {f"p{pct}": percentile(sorted_values, pct) for pct in percentiles}


def win_rate(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = numeric_values(rows, key)
    if not values:
        return None
    return round(sum(1 for value in values if value > 0) / len(values), 6)


def numeric_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for value in (number(row.get(key)) for row in rows) if value is not None]


def average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def median_value(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def percentile(sorted_values: Sequence[float], pct: int) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return round(sorted_values[0], 6)
    position = (len(sorted_values) - 1) * pct / 100
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return round(sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction, 6)


def max_by(rows: Sequence[Mapping[str, Any]], key: str) -> Mapping[str, Any] | None:
    candidates = [row for row in rows if number(row.get(key)) is not None]
    return max(candidates, key=lambda row: number(row.get(key)) or 0.0) if candidates else None


def min_by(rows: Sequence[Mapping[str, Any]], key: str) -> Mapping[str, Any] | None:
    candidates = [row for row in rows if number(row.get(key)) is not None]
    return min(candidates, key=lambda row: number(row.get(key)) or 0.0) if candidates else None


def trade_ref(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "trade_outcome_id": row.get("trade_outcome_id"),
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id"),
        "instrument": row.get("instrument"),
        "contract": row.get("contract"),
        "side": row.get("side"),
        "entry_time": row.get("entry_time"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "realized_points": row.get("realized_points"),
    }


def counts(values: Iterable[Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        if value in (None, ""):
            continue
        key = str(value)
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _matches_request(row: Mapping[str, Any], request: CanonicalAnalyticsRequest) -> bool:
    return all(_matches_filter(row, filter_spec) for filter_spec in request.filters) and all(
        _matches_validity_rule(row, rule) for rule in request.validity_rules
    )


def _matches_filter(row: Mapping[str, Any], filter_spec: AnalyticsFilter) -> bool:
    value = row.get(filter_spec.field)
    op = filter_spec.op.lower()
    if op == "eq":
        return value == filter_spec.value
    if op == "ne":
        return value != filter_spec.value
    if op == "in":
        return value in filter_spec.values
    if op == "not_in":
        return value not in filter_spec.values
    if op in {"exists", "not_null"}:
        return value not in (None, "")
    if op == "missing":
        return value in (None, "")
    left = number(value)
    right = number(filter_spec.value)
    if left is None or right is None:
        return False
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    if op == "lte":
        return left <= right
    return False


def _matches_validity_rule(row: Mapping[str, Any], rule: ContextValidityRule) -> bool:
    value = row.get(rule.field)
    if value in (None, ""):
        return not rule.required
    return str(value).upper() in {str(item).upper() for item in rule.valid_values}


def _flatten(values: Iterable[Any]) -> Iterable[Any]:
    for value in values:
        if isinstance(value, (list, tuple, set)):
            yield from value
        else:
            yield value


def _sort_value(value: Any) -> float | str:
    numeric = number(value)
    if numeric is not None:
        return numeric
    return str(value or "")
