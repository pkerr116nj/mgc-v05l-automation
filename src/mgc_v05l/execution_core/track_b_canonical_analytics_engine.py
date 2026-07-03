"""Reusable diagnostic aggregation engine for Track B analytics.

The engine accepts records plus explicit dimensions, metrics, filters, and
context-validity rules. It is analytics/research infrastructure only: it has no
runtime, broker, strategy, order, or gate authority.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from statistics import median
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "track_b_canonical_analytics_engine_v1"
QUERY_SCHEMA_VERSION = "track_b_canonical_analytics_query_v1"
RESULT_SCHEMA_VERSION = "track_b_canonical_analytics_result_v1"

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


@dataclass(frozen=True)
class DimensionDefinition:
    name: str
    field: str
    description: str
    validity_rule: ContextValidityRule | None = None


@dataclass(frozen=True)
class FilterDefinition:
    name: str
    field: str | None
    op: str | None
    description: str
    validity_rule: ContextValidityRule | None = None


@dataclass(frozen=True)
class CanonicalAnalyticsQuery:
    name: str
    dimensions: tuple[str, ...] = ()
    metrics: tuple[str, ...] = ()
    filters: tuple[AnalyticsFilter, ...] = ()
    validity_rules: tuple[ContextValidityRule, ...] = ()
    named_filters: tuple[str, ...] = ()
    validity_requirements: tuple[str, ...] = ()
    order_by: tuple[str, ...] = ("sample_rank", "average_pnl_proxy")
    order_descending: bool = True
    min_sample_size: int = 0
    min_sample_class: str | None = None
    missing_value: str = "UNKNOWN"


@dataclass(frozen=True)
class CanonicalAnalyticsResult:
    schema_version: str
    query: dict[str, Any]
    generated_at: str
    grouped_rows: list[dict[str, Any]]
    summary: dict[str, Any]
    validity_metadata: dict[str, Any]
    provenance: dict[str, Any]
    diagnostic_only: bool = True
    production_recommendation: bool = False
    trading_gate: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "query": self.query,
            "generated_at": self.generated_at,
            "grouped_rows": self.grouped_rows,
            "summary": self.summary,
            "validity_metadata": self.validity_metadata,
            "provenance": self.provenance,
            "diagnostic_only": self.diagnostic_only,
            "production_recommendation": self.production_recommendation,
            "trading_gate": self.trading_gate,
        }


class CanonicalAnalyticsEngine:
    def __init__(
        self,
        *,
        dimensions: Mapping[str, DimensionDefinition] | None = None,
        metrics: Mapping[str, AnalyticsMetric] | None = None,
        filters: Mapping[str, FilterDefinition] | None = None,
    ) -> None:
        self.dimensions = dict(dimensions or default_dimension_registry())
        self.metrics = dict(metrics or default_metric_registry())
        self.filters = dict(filters or default_filter_registry())

    def run(
        self,
        rows: Sequence[Mapping[str, Any]],
        query: CanonicalAnalyticsQuery,
        *,
        generated_at: datetime | str | None = None,
        provenance: Mapping[str, Any] | None = None,
    ) -> CanonicalAnalyticsResult:
        generated = coerce_datetime(generated_at)
        request = self.to_request(query)
        raw = run_canonical_analytics(rows, request)
        grouped_rows = [
            row for row in raw["groups"]
            if _group_meets_sample_rules(row, min_sample_size=query.min_sample_size, min_sample_class=query.min_sample_class)
        ]
        summary = {
            "input_count": raw["input_count"],
            "matched_count": raw["matched_count"],
            "group_count": len(grouped_rows),
            "pre_sample_filter_group_count": raw["group_count"],
            "sample_class": sample_class_for_count(raw["matched_count"]),
            "min_sample_size": query.min_sample_size,
            "min_sample_class": query.min_sample_class,
            "dimension_count": len(query.dimensions),
            "metric_count": len(query.metrics or tuple(default_metric_registry().keys())),
        }
        validity_metadata = {
            "validity_requirements": list(query.validity_requirements),
            "resolved_validity_rules": [rule.__dict__ for rule in request.validity_rules],
            "named_filters": list(query.named_filters),
            "filters": [filter_spec.__dict__ for filter_spec in request.filters],
        }
        return CanonicalAnalyticsResult(
            schema_version=RESULT_SCHEMA_VERSION,
            query=query_to_dict(query),
            generated_at=generated.isoformat(),
            grouped_rows=grouped_rows,
            summary=summary,
            validity_metadata=validity_metadata,
            provenance=dict(provenance or {}),
        )

    def to_request(self, query: CanonicalAnalyticsQuery) -> CanonicalAnalyticsRequest:
        dimension_fields = tuple(self._dimension_field(name) for name in query.dimensions)
        metric_specs = tuple(self._metric(name) for name in query.metrics) if query.metrics else tuple(self.metrics.values())
        filters = list(query.filters)
        validity_rules: list[ContextValidityRule] = []
        validity_rules.extend(query.validity_rules)
        for name in query.named_filters:
            definition = self._filter(name)
            if definition.validity_rule is not None:
                validity_rules.append(definition.validity_rule)
            elif definition.field and definition.op:
                filters.append(AnalyticsFilter(definition.field, definition.op))
        for name in query.validity_requirements:
            definition = self._dimension_or_filter_validity(name)
            validity_rules.append(definition)
        for name in query.dimensions:
            definition = self.dimensions.get(name)
            if definition and definition.validity_rule is not None:
                validity_rules.append(definition.validity_rule)
        return CanonicalAnalyticsRequest(
            name=query.name,
            dimensions=dimension_fields,
            filters=tuple(filters),
            validity_rules=tuple(_dedupe_validity_rules(validity_rules)),
            metrics=metric_specs,
            missing_value=query.missing_value,
            sort_fields=query.order_by,
            sort_descending=query.order_descending,
        )

    def _dimension_field(self, name: str) -> str:
        if name in self.dimensions:
            return self.dimensions[name].field
        return name

    def _metric(self, name: str) -> AnalyticsMetric:
        if name not in self.metrics:
            raise ValueError(f"Unsupported canonical analytics metric: {name}")
        return self.metrics[name]

    def _filter(self, name: str) -> FilterDefinition:
        if name not in self.filters:
            raise ValueError(f"Unsupported canonical analytics filter: {name}")
        return self.filters[name]

    def _dimension_or_filter_validity(self, name: str) -> ContextValidityRule:
        if name in self.filters and self.filters[name].validity_rule is not None:
            return self.filters[name].validity_rule  # type: ignore[return-value]
        if name in self.dimensions and self.dimensions[name].validity_rule is not None:
            return self.dimensions[name].validity_rule  # type: ignore[return-value]
        raise ValueError(f"Unsupported canonical analytics validity requirement: {name}")


def default_dimension_registry() -> dict[str, DimensionDefinition]:
    return {
        "strategy": DimensionDefinition("strategy", "strategy_id", "Strategy identifier."),
        "lane": DimensionDefinition("lane", "lane_id", "Lane identifier."),
        "session": DimensionDefinition("session", "session_at_entry", "Session at trade entry."),
        "instrument": DimensionDefinition("instrument", "instrument", "Instrument family."),
        "contract": DimensionDefinition("contract", "contract", "Contract symbol."),
        "side": DimensionDefinition("side", "side", "Trade side."),
        "vix_regime": DimensionDefinition("vix_regime", "vix_regime", "VIX regime.", ContextValidityRule("market_context_validity_classification")),
        "vix_percentile_bucket": DimensionDefinition("vix_percentile_bucket", "vix_percentile_bucket", "VIX percentile bucket.", ContextValidityRule("market_context_validity_classification")),
        "gre_label": DimensionDefinition("gre_label", "gre_label", "Historical GRE label.", ContextValidityRule("gre_validity_classification")),
        "gre_confidence_bucket": DimensionDefinition("gre_confidence_bucket", "gre_confidence_bucket", "Historical GRE confidence bucket.", ContextValidityRule("gre_validity_classification")),
        "crfd_regime": DimensionDefinition("crfd_regime", "crfd_regime", "CRFD/regime label when present.", ContextValidityRule("crfd_validity_classification")),
        "exit_policy": DimensionDefinition("exit_policy", "exit_policy", "Exit policy identifier."),
    }


def default_metric_registry() -> dict[str, AnalyticsMetric]:
    return {
        "trade_count": AnalyticsMetric("trade_count", "count"),
        "win_rate": AnalyticsMetric("win_rate", "win_rate", source_field="realized_pnl_proxy"),
        "average_pnl_proxy": AnalyticsMetric("average_pnl_proxy", "average", source_field="realized_pnl_proxy"),
        "median_pnl_proxy": AnalyticsMetric("median_pnl_proxy", "median", source_field="realized_pnl_proxy"),
        "expectancy_proxy": AnalyticsMetric("expectancy_proxy", "average", source_field="realized_pnl_proxy"),
        "average_duration": AnalyticsMetric("average_duration", "average", source_field="hold_seconds"),
        "sample_class": AnalyticsMetric("sample_class", "sample_class"),
        "average_realized_points": AnalyticsMetric("average_realized_points", "average", source_field="realized_points"),
        "median_realized_points": AnalyticsMetric("median_realized_points", "median", source_field="realized_points"),
        "pnl_percentiles": AnalyticsMetric("pnl_percentiles", "percentiles", source_field="realized_pnl_proxy", percentiles=(0, 10, 25, 50, 75, 90, 100)),
        "best_trade": AnalyticsMetric("best_trade", "best_ref", source_field="realized_pnl_proxy"),
        "worst_trade": AnalyticsMetric("worst_trade", "worst_ref", source_field="realized_pnl_proxy"),
        "average_hold_seconds": AnalyticsMetric("average_hold_seconds", "average", source_field="hold_seconds"),
        "median_hold_seconds": AnalyticsMetric("median_hold_seconds", "median", source_field="hold_seconds"),
        "data_quality_flags": AnalyticsMetric("data_quality_flags", "counts", source_field="data_quality_flags"),
        "enrichment_data_quality_flags": AnalyticsMetric("enrichment_data_quality_flags", "counts", source_field="enrichment_data_quality_flags"),
        "profit_factor_proxy": AnalyticsMetric("profit_factor_proxy", "profit_factor", source_field="realized_pnl_proxy"),
        "average_winner_pnl_proxy": AnalyticsMetric("average_winner_pnl_proxy", "average_winner", source_field="realized_pnl_proxy"),
        "average_loser_pnl_proxy": AnalyticsMetric("average_loser_pnl_proxy", "average_loser", source_field="realized_pnl_proxy"),
        "payoff_ratio_proxy": AnalyticsMetric("payoff_ratio_proxy", "payoff_ratio", source_field="realized_pnl_proxy"),
        "max_win_pnl_proxy": AnalyticsMetric("max_win_pnl_proxy", "max_win", source_field="realized_pnl_proxy"),
        "max_loss_pnl_proxy": AnalyticsMetric("max_loss_pnl_proxy", "max_loss", source_field="realized_pnl_proxy"),
        "median_winner_pnl_proxy": AnalyticsMetric("median_winner_pnl_proxy", "median_winner", source_field="realized_pnl_proxy"),
        "median_loser_pnl_proxy": AnalyticsMetric("median_loser_pnl_proxy", "median_loser", source_field="realized_pnl_proxy"),
        "p10_pnl_proxy": AnalyticsMetric("p10_pnl_proxy", "percentile_value", source_field="realized_pnl_proxy", percentiles=(10,)),
        "p25_pnl_proxy": AnalyticsMetric("p25_pnl_proxy", "percentile_value", source_field="realized_pnl_proxy", percentiles=(25,)),
        "p75_pnl_proxy": AnalyticsMetric("p75_pnl_proxy", "percentile_value", source_field="realized_pnl_proxy", percentiles=(75,)),
        "p90_pnl_proxy": AnalyticsMetric("p90_pnl_proxy", "percentile_value", source_field="realized_pnl_proxy", percentiles=(90,)),
        "downside_tail_mean_proxy": AnalyticsMetric("downside_tail_mean_proxy", "downside_tail_mean", source_field="realized_pnl_proxy"),
        "upside_tail_mean_proxy": AnalyticsMetric("upside_tail_mean_proxy", "upside_tail_mean", source_field="realized_pnl_proxy"),
        "consecutive_win_streak_max": AnalyticsMetric("consecutive_win_streak_max", "win_streak", source_field="realized_pnl_proxy"),
        "consecutive_loss_streak_max": AnalyticsMetric("consecutive_loss_streak_max", "loss_streak", source_field="realized_pnl_proxy"),
        "loss_rate": AnalyticsMetric("loss_rate", "loss_rate", source_field="realized_pnl_proxy"),
        "breakeven_rate": AnalyticsMetric("breakeven_rate", "breakeven_rate", source_field="realized_pnl_proxy"),
        "sample_confidence_label": AnalyticsMetric("sample_confidence_label", "sample_confidence_label"),
    }


def default_filter_registry() -> dict[str, FilterDefinition]:
    return {
        "instrument": FilterDefinition("instrument", "instrument", "eq", "Instrument equality filter template."),
        "strategy": FilterDefinition("strategy", "strategy_id", "eq", "Strategy equality filter template."),
        "session": FilterDefinition("session", "session_at_entry", "eq", "Session equality filter template."),
        "valid_gre_only": FilterDefinition("valid_gre_only", None, None, "Require valid GRE context.", ContextValidityRule("gre_validity_classification")),
        "valid_vix_only": FilterDefinition("valid_vix_only", None, None, "Require valid VIX market context.", ContextValidityRule("market_context_validity_classification")),
        "side": FilterDefinition("side", "side", "eq", "Side equality filter template."),
        "date_window": FilterDefinition("date_window", "entry_time", "gte/lte", "Use explicit entry_time gte/lte filters."),
    }


def query_to_dict(query: CanonicalAnalyticsQuery) -> dict[str, Any]:
    return {
        "schema_version": QUERY_SCHEMA_VERSION,
        "name": query.name,
        "dimensions": list(query.dimensions),
        "metrics": list(query.metrics),
        "filters": [filter_spec.__dict__ for filter_spec in query.filters],
        "validity_rules": [rule.__dict__ for rule in query.validity_rules],
        "named_filters": list(query.named_filters),
        "validity_requirements": list(query.validity_requirements),
        "order_by": list(query.order_by),
        "order_descending": query.order_descending,
        "min_sample_size": query.min_sample_size,
        "min_sample_class": query.min_sample_class,
        "missing_value": query.missing_value,
    }


def run_canonical_query(
    rows: Sequence[Mapping[str, Any]],
    query: CanonicalAnalyticsQuery,
    *,
    generated_at: datetime | str | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> CanonicalAnalyticsResult:
    return CanonicalAnalyticsEngine().run(rows, query, generated_at=generated_at, provenance=provenance)


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
    metric_flags: dict[str, int] = {}
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
        elif metric.kind == "count":
            payload[output_field] = len(rows)
        elif metric.kind == "sample_class":
            payload[output_field] = sample_class
        elif metric.kind == "profit_factor":
            payload[output_field] = profit_factor(rows, metric.source_field or "realized_pnl_proxy", metric_flags)
        elif metric.kind == "average_winner":
            payload[output_field] = average_winner(rows, metric.source_field or "realized_pnl_proxy", metric_flags)
        elif metric.kind == "average_loser":
            payload[output_field] = average_loser(rows, metric.source_field or "realized_pnl_proxy", metric_flags)
        elif metric.kind == "payoff_ratio":
            payload[output_field] = payoff_ratio(rows, metric.source_field or "realized_pnl_proxy", metric_flags)
        elif metric.kind == "max_win":
            payload[output_field] = extrema_winner(rows, metric.source_field or "realized_pnl_proxy", metric_flags, maximum=True)
        elif metric.kind == "max_loss":
            payload[output_field] = extrema_loser(rows, metric.source_field or "realized_pnl_proxy", metric_flags, minimum=True)
        elif metric.kind == "median_winner":
            payload[output_field] = median_winner(rows, metric.source_field or "realized_pnl_proxy", metric_flags)
        elif metric.kind == "median_loser":
            payload[output_field] = median_loser(rows, metric.source_field or "realized_pnl_proxy", metric_flags)
        elif metric.kind == "percentile_value":
            values = numeric_values(rows, metric.source_field or "")
            pct = metric.percentiles[0] if metric.percentiles else 50
            payload[output_field] = percentile(sorted(values), pct) if values else None
            if not values:
                _add_metric_flag(metric_flags, f"missing_{output_field}_pnl_proxy")
        elif metric.kind == "downside_tail_mean":
            payload[output_field] = tail_mean(rows, metric.source_field or "realized_pnl_proxy", metric_flags, side="downside")
        elif metric.kind == "upside_tail_mean":
            payload[output_field] = tail_mean(rows, metric.source_field or "realized_pnl_proxy", metric_flags, side="upside")
        elif metric.kind == "win_streak":
            payload[output_field] = streak_max(rows, metric.source_field or "realized_pnl_proxy", target="win")
        elif metric.kind == "loss_streak":
            payload[output_field] = streak_max(rows, metric.source_field or "realized_pnl_proxy", target="loss")
        elif metric.kind == "loss_rate":
            payload[output_field] = loss_rate(rows, metric.source_field or "realized_pnl_proxy")
        elif metric.kind == "breakeven_rate":
            payload[output_field] = breakeven_rate(rows, metric.source_field or "realized_pnl_proxy")
        elif metric.kind == "sample_confidence_label":
            payload[output_field] = sample_confidence_label(len(rows))
        else:
            payload[output_field] = None
    payload["metric_data_quality_flags"] = metric_flags
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


def loss_rate(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = numeric_values(rows, key)
    if not values:
        return None
    return round(sum(1 for value in values if value < 0) / len(values), 6)


def breakeven_rate(rows: Sequence[Mapping[str, Any]], key: str) -> float | None:
    values = numeric_values(rows, key)
    if not values:
        return None
    return round(sum(1 for value in values if value == 0) / len(values), 6)


def profit_factor(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None) -> float | None:
    values = numeric_values(rows, key)
    winners = [value for value in values if value > 0]
    losers = [value for value in values if value < 0]
    if not values:
        _add_metric_flag(flags, "missing_pnl_proxy_for_profit_factor")
        return None
    if not winners:
        _add_metric_flag(flags, "profit_factor_no_winners")
        return None
    if not losers:
        _add_metric_flag(flags, "profit_factor_no_losses")
        return None
    return round(sum(winners) / abs(sum(losers)), 6)


def average_winner(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None) -> float | None:
    winners = [value for value in numeric_values(rows, key) if value > 0]
    if not winners:
        _add_metric_flag(flags, "average_winner_no_winners")
        return None
    return average(winners)


def average_loser(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None) -> float | None:
    losers = [value for value in numeric_values(rows, key) if value < 0]
    if not losers:
        _add_metric_flag(flags, "average_loser_no_losses")
        return None
    return average(losers)


def payoff_ratio(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None) -> float | None:
    winner_average = average_winner(rows, key, flags)
    loser_average = average_loser(rows, key, flags)
    if winner_average is None or loser_average is None or loser_average == 0:
        _add_metric_flag(flags, "payoff_ratio_insufficient_winners_or_losses")
        return None
    return round(winner_average / abs(loser_average), 6)


def extrema_winner(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None, *, maximum: bool) -> float | None:
    winners = [value for value in numeric_values(rows, key) if value > 0]
    if not winners:
        _add_metric_flag(flags, "winner_extrema_no_winners")
        return None
    return round(max(winners) if maximum else min(winners), 6)


def extrema_loser(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None, *, minimum: bool) -> float | None:
    losers = [value for value in numeric_values(rows, key) if value < 0]
    if not losers:
        _add_metric_flag(flags, "loser_extrema_no_losses")
        return None
    return round(min(losers) if minimum else max(losers), 6)


def median_winner(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None) -> float | None:
    winners = [value for value in numeric_values(rows, key) if value > 0]
    if not winners:
        _add_metric_flag(flags, "median_winner_no_winners")
        return None
    return median_value(winners)


def median_loser(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None) -> float | None:
    losers = [value for value in numeric_values(rows, key) if value < 0]
    if not losers:
        _add_metric_flag(flags, "median_loser_no_losses")
        return None
    return median_value(losers)


def tail_mean(rows: Sequence[Mapping[str, Any]], key: str, flags: dict[str, int] | None = None, *, side: str) -> float | None:
    values = sorted(numeric_values(rows, key))
    if not values:
        _add_metric_flag(flags, f"{side}_tail_mean_missing_pnl_proxy")
        return None
    tail_count = max(1, int(len(values) * 0.1))
    tail_values = values[:tail_count] if side == "downside" else values[-tail_count:]
    return average(tail_values)


def streak_max(rows: Sequence[Mapping[str, Any]], key: str, *, target: str) -> int:
    current = 0
    best = 0
    for row in sorted(rows, key=_row_time_sort_key):
        value = number(row.get(key))
        if value is None:
            current = 0
            continue
        matched = value > 0 if target == "win" else value < 0
        if matched:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def sample_confidence_label(count: int) -> str:
    sample_class = sample_class_for_count(count)
    if sample_class == "RESEARCH_GRADE":
        return "RESEARCH_GRADE_SAMPLE_NOT_STATISTICAL_SIGNIFICANCE"
    if sample_class == "DEVELOPING":
        return "DEVELOPING_SAMPLE_REVIEW_ONLY"
    if sample_class == "PRELIMINARY":
        return "PRELIMINARY_SAMPLE_LOW_CONFIDENCE"
    return "EXPLORATORY_SAMPLE_LOW_CONFIDENCE"


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


def coerce_datetime(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


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
    if op in {"date_gte", "date_lte"}:
        left_dt = _coerce_optional_datetime(value)
        right_dt = _coerce_optional_datetime(filter_spec.value)
        if left_dt is None or right_dt is None:
            return False
        return left_dt >= right_dt if op == "date_gte" else left_dt <= right_dt
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


def _add_metric_flag(flags: dict[str, int] | None, flag: str) -> None:
    if flags is None:
        return
    flags[flag] = flags.get(flag, 0) + 1


def _row_time_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str]:
    exit_time = _coerce_optional_datetime(row.get("exit_time"))
    entry_time = _coerce_optional_datetime(row.get("entry_time"))
    primary = exit_time or entry_time
    return (
        primary.isoformat() if primary else "",
        str(row.get("entry_time") or ""),
        str(row.get("trade_outcome_id") or ""),
    )


def _group_meets_sample_rules(row: Mapping[str, Any], *, min_sample_size: int, min_sample_class: str | None) -> bool:
    if min_sample_size and int(row.get("count") or 0) < min_sample_size:
        return False
    if min_sample_class:
        required = SAMPLE_RANK.get(min_sample_class, 0)
        actual = SAMPLE_RANK.get(str(row.get("sample_class") or ""), 0)
        if actual < required:
            return False
    return True


def _dedupe_validity_rules(rules: Sequence[ContextValidityRule]) -> list[ContextValidityRule]:
    seen: set[tuple[str, tuple[str, ...], bool]] = set()
    deduped: list[ContextValidityRule] = []
    for rule in rules:
        key = (rule.field, tuple(sorted(str(value).upper() for value in rule.valid_values)), rule.required)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rule)
    return deduped


def _coerce_optional_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return coerce_datetime(value if isinstance(value, (datetime, str)) else str(value))
    except ValueError:
        return None
