"""Core expectancy analytics and coverage matrix for Track B.

This module is diagnostic/research only. It reads canonical trade outcome
artifacts and enrichment rows, then publishes descriptive analytics. It has no
runtime, broker, strategy, or gate authority.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_canonical_analytics_engine import (
    AnalyticsFilter,
    CanonicalAnalyticsQuery,
    ContextValidityRule,
    aggregate_metric_group as engine_aggregate_metric_group,
    percentile_distribution as engine_percentile_distribution,
    run_canonical_query,
    sample_class_for_count as engine_sample_class_for_count,
)
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_ENRICHMENT_DIR,
    ENRICHMENT_JSONL,
    SUMMARY_JSON as ENRICHMENT_SUMMARY_JSON,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_OUTCOME_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_OUTCOME_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_JSONL
DEFAULT_ENRICHMENT_SUMMARY_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_SUMMARY_JSON
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "core_expectancy_analytics"

ANALYTICS_COVERAGE_MATRIX_JSON = "analytics_coverage_matrix.json"
ANALYTICS_COVERAGE_MATRIX_MD = "analytics_coverage_matrix.md"
CORE_EXPECTANCY_JSON = "core_expectancy_analytics.json"
CORE_EXPECTANCY_MD = "core_expectancy_analytics.md"
EXPECTANCY_BY_STRATEGY_MD = "expectancy_by_strategy.md"
EXPECTANCY_BY_SESSION_MD = "expectancy_by_session.md"
EXPECTANCY_BY_INSTRUMENT_MD = "expectancy_by_instrument.md"
EXPECTANCY_BY_VIX_CONTEXT_MD = "expectancy_by_vix_context.md"
EXPECTANCY_BY_GRE_CONTEXT_MD = "expectancy_by_valid_gre_context.md"
ANALYTICS_NEXT_STEPS_MD = "analytics_next_steps.md"

SCHEMA_VERSION = "track_b_core_expectancy_analytics_v1"
COVERAGE_SCHEMA_VERSION = "track_b_analytics_coverage_matrix_v1"
CORE_EXPECTANCY_QUERY_METRICS = (
    "win_rate",
    "average_pnl_proxy",
    "median_pnl_proxy",
    "average_realized_points",
    "median_realized_points",
    "pnl_percentiles",
    "best_trade",
    "worst_trade",
    "average_hold_seconds",
    "median_hold_seconds",
    "data_quality_flags",
    "enrichment_data_quality_flags",
)


@dataclass(frozen=True)
class CoreExpectancyAnalyticsResult:
    coverage_matrix: dict[str, Any]
    analytics: dict[str, Any]
    coverage_json_path: Path
    coverage_markdown_path: Path
    analytics_json_path: Path
    analytics_markdown_path: Path
    strategy_path: Path
    session_path: Path
    instrument_path: Path
    vix_path: Path
    gre_path: Path
    next_steps_path: Path


def run_core_expectancy_analytics(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    enrichment_summary_path: Path = DEFAULT_ENRICHMENT_SUMMARY_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
) -> CoreExpectancyAnalyticsResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    enrichment_summary = _read_json_mapping(enrichment_summary_path)
    rows = merge_outcomes_with_enrichment(outcomes, enrichments)
    coverage_matrix = build_analytics_coverage_matrix(
        rows,
        enrichment_summary=enrichment_summary,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "trade_outcome_enrichment": enrichments_path,
            "trade_outcome_enrichment_summary": enrichment_summary_path,
        },
    )
    analytics = build_core_expectancy_analytics(
        rows,
        coverage_matrix=coverage_matrix,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "trade_outcome_enrichment": enrichments_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    coverage_json_path = output_dir / ANALYTICS_COVERAGE_MATRIX_JSON
    analytics_json_path = output_dir / CORE_EXPECTANCY_JSON
    write_bounded_snapshot_json(coverage_json_path, coverage_matrix, config=snapshot_config)
    write_bounded_snapshot_json(analytics_json_path, analytics, config=snapshot_config)
    coverage_markdown_path = output_dir / ANALYTICS_COVERAGE_MATRIX_MD
    analytics_markdown_path = output_dir / CORE_EXPECTANCY_MD
    strategy_path = output_dir / EXPECTANCY_BY_STRATEGY_MD
    session_path = output_dir / EXPECTANCY_BY_SESSION_MD
    instrument_path = output_dir / EXPECTANCY_BY_INSTRUMENT_MD
    vix_path = output_dir / EXPECTANCY_BY_VIX_CONTEXT_MD
    gre_path = output_dir / EXPECTANCY_BY_GRE_CONTEXT_MD
    next_steps_path = output_dir / ANALYTICS_NEXT_STEPS_MD
    coverage_markdown_path.write_text(render_coverage_matrix_markdown(coverage_matrix), encoding="utf-8")
    analytics_markdown_path.write_text(render_core_expectancy_markdown(analytics), encoding="utf-8")
    strategy_path.write_text(render_group_markdown("Expectancy By Strategy/Lane", analytics["groups"]["strategy_lane"]), encoding="utf-8")
    session_path.write_text(render_group_markdown("Expectancy By Session", analytics["groups"]["session"]), encoding="utf-8")
    instrument_path.write_text(render_group_markdown("Expectancy By Instrument", analytics["groups"]["instrument"]), encoding="utf-8")
    vix_path.write_text(render_group_markdown("Expectancy By VIX Context", analytics["groups"]["vix_context"]), encoding="utf-8")
    gre_path.write_text(render_group_markdown("Expectancy By Valid GRE Context", analytics["groups"]["valid_gre_context"]), encoding="utf-8")
    next_steps_path.write_text(render_next_steps_markdown(coverage_matrix), encoding="utf-8")
    return CoreExpectancyAnalyticsResult(
        coverage_matrix=coverage_matrix,
        analytics=analytics,
        coverage_json_path=coverage_json_path,
        coverage_markdown_path=coverage_markdown_path,
        analytics_json_path=analytics_json_path,
        analytics_markdown_path=analytics_markdown_path,
        strategy_path=strategy_path,
        session_path=session_path,
        instrument_path=instrument_path,
        vix_path=vix_path,
        gre_path=gre_path,
        next_steps_path=next_steps_path,
    )


def build_analytics_coverage_matrix(
    rows: Sequence[Mapping[str, Any]],
    *,
    enrichment_summary: Mapping[str, Any] | None = None,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    total = len(rows)
    pnl_count = sum(1 for row in rows if _number(row.get("realized_pnl_proxy")) is not None)
    r_count = sum(1 for row in rows if _number(row.get("realized_r_proxy")) is not None)
    mfe_count = sum(1 for row in rows if _number(row.get("mfe_points")) is not None)
    mae_count = sum(1 for row in rows if _number(row.get("mae_points")) is not None)
    gre_valid = sum(1 for row in rows if _valid_context(row.get("gre_validity_classification")) and row.get("gre_label"))
    crfd_valid = sum(1 for row in rows if _valid_context(row.get("crfd_validity_classification")))
    vix_valid = sum(1 for row in rows if _valid_context(row.get("market_context_validity_classification")) and row.get("vix_regime"))
    hold_count = sum(1 for row in rows if _number(row.get("hold_seconds")) is not None)
    coverage = {
        "total_outcomes": total,
        "pnl_proxy_coverage": _rate(pnl_count, total),
        "r_proxy_coverage": _rate(r_count, total),
        "mfe_coverage": _rate(mfe_count, total),
        "mae_coverage": _rate(mae_count, total),
        "hold_time_coverage": _rate(hold_count, total),
        "vix_valid_coverage": _rate(vix_valid, total),
        "gre_valid_coverage": _rate(gre_valid, total),
        "crfd_valid_coverage": _rate(crfd_valid, total),
    }
    capabilities = [
        _capability("strategy_expectancy", "READY", ["canonical trade outcomes", "P&L proxy"], coverage["pnl_proxy_coverage"], None, "Use core expectancy analytics by strategy/lane."),
        _capability("session_expectancy", "READY", ["canonical trade outcomes", "session_at_entry", "P&L proxy"], coverage["pnl_proxy_coverage"], None, "Use core expectancy analytics by session."),
        _capability("instrument_expectancy", "READY", ["canonical trade outcomes", "instrument/contract", "P&L proxy"], coverage["pnl_proxy_coverage"], None, "Use core expectancy analytics by instrument."),
        _capability("vix_conditioned_performance", _ready_partial_blocked(vix_valid, total, full_threshold=0.9), ["trade outcome enrichment", "VIX context", "P&L proxy"], coverage["vix_valid_coverage"], None if vix_valid else "missing VIX context", "Continue context-aware analytics; expand market context providers next."),
        _capability("gre_conditioned_performance", _ready_partial_blocked(gre_valid, total, full_threshold=0.8), ["historical GRE context", "validity metadata", "P&L proxy"], coverage["gre_valid_coverage"], "GRE coverage is Gold-scoped and partial.", "Expand GRE/CRFD provider coverage before broad conclusions."),
        _capability("gre_vix_interaction", "PARTIAL" if gre_valid and vix_valid else "BLOCKED", ["valid GRE context", "valid VIX context", "P&L proxy"], _rate(min(gre_valid, vix_valid), total), "Limited by valid GRE coverage.", "Run interaction analytics only on valid GRE rows and mark sample limits."),
        _capability("exit_policy_performance", "READY", ["canonical trade outcomes", "exit_policy", "P&L proxy"], coverage["pnl_proxy_coverage"], None, "Use exit-policy expectancy; avoid exit-efficiency claims without MFE/MAE."),
        _capability("hold_time_analytics", _ready_partial_blocked(hold_count, total, full_threshold=0.9), ["hold_seconds"], coverage["hold_time_coverage"], None, "Use hold-time distribution analytics."),
        _capability("drawdown_distribution_analytics", "PARTIAL" if pnl_count else "BLOCKED", ["P&L proxy distribution"], coverage["pnl_proxy_coverage"], "Per-trade distribution exists; portfolio/equity-curve drawdown needs sequencing/capital assumptions.", "Implement equity-curve simulation as research-only later."),
        _capability("r_multiple_analytics", _threshold_status(r_count, total, partial_if_any=False), ["realized_r_proxy"], coverage["r_proxy_coverage"], "R proxy unavailable.", "Capture or derive initial risk per trade."),
        _capability("mfe_mae_exit_efficiency_analytics", _threshold_status(min(mfe_count, mae_count), total, partial_if_any=True), ["MFE", "MAE"], min(coverage["mfe_coverage"] or 0.0, coverage["mae_coverage"] or 0.0), "MFE/MAE is sparse.", "Improve forward-path/MFE-MAE capture before exit-efficiency conclusions."),
        _capability("capital_allocation_analytics", "PARTIAL", ["P&L proxy", "strategy/session/instrument grouping"], coverage["pnl_proxy_coverage"], "No true capital, margin, risk, or R-multiple model yet.", "Design diagnostic allocation model after R proxy is available."),
        _capability("strategy_stability_analytics", "PARTIAL", ["trade sequence", "strategy groups", "P&L proxy"], coverage["pnl_proxy_coverage"], "Needs time-split/rolling-window stability tests.", "Add rolling/holdout stability scorecards."),
        _capability("research_discovery_readiness", "READY", ["enriched trade outcomes", "context validity", "discovery engine"], coverage["pnl_proxy_coverage"], None, "Continue manual-review workflow and rerun after provider refreshes."),
    ]
    # Normalize capability statuses where ternary expressions would produce bools.
    for row in capabilities:
        if row["status"] is True:
            row["status"] = "READY"
        elif row["status"] is False:
            row["status"] = "BLOCKED"
    return {
        "schema_version": COVERAGE_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "coverage": coverage,
        "enrichment_summary_context_validity": (enrichment_summary or {}).get("context_validity", {}),
        "capabilities": capabilities,
        "next_steps": _coverage_next_steps(capabilities),
    }


def build_core_expectancy_analytics(
    rows: Sequence[Mapping[str, Any]],
    *,
    coverage_matrix: Mapping[str, Any],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    bucketed = [_with_context_buckets(row) for row in rows]
    overall = aggregate_expectancy_group("ALL", bucketed)
    groups = {
        "strategy_lane": aggregate_expectancy_groups(bucketed, ("strategy_id", "lane_id")),
        "session": aggregate_expectancy_groups(bucketed, ("session_at_entry",)),
        "instrument": aggregate_expectancy_groups(bucketed, ("instrument", "contract")),
        "side": aggregate_expectancy_groups(bucketed, ("side",)),
        "exit_policy": aggregate_expectancy_groups(bucketed, ("exit_policy",)),
        "vix_context": aggregate_expectancy_groups(bucketed, ("vix_regime", "vix_percentile_bucket")),
        "valid_gre_context": aggregate_expectancy_groups(
            bucketed,
            ("gre_label", "gre_confidence_bucket"),
            validity_rules=(ContextValidityRule("gre_validity_classification"),),
            required_fields=("gre_label",),
        ),
    }
    data_quality = {
        "missing_realized_r_proxy": sum(1 for row in rows if _number(row.get("realized_r_proxy")) is None),
        "missing_mfe": sum(1 for row in rows if _number(row.get("mfe_points")) is None),
        "missing_mae": sum(1 for row in rows if _number(row.get("mae_points")) is None),
        "gre_valid_rows": len([row for row in rows if _valid_context(row.get("gre_validity_classification")) and row.get("gre_label")]),
        "notes": [
            "R proxy is not fabricated.",
            "MFE/MAE and exit-efficiency analytics are limited by sparse coverage.",
            "GRE groups include only valid GRE context rows.",
            "This analytics layer is research/diagnostic only.",
        ],
    }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "coverage_status": {
            row["capability"]: row["status"] for row in coverage_matrix.get("capabilities", [])
        },
        "overall": overall,
        "groups": groups,
        "data_quality": data_quality,
        "safety_contract": {
            "broker_actions": False,
            "runtime_restart": False,
            "managed_exit_restart": False,
            "strategy_changes": False,
            "trading_gates": False,
            "production_behavior_changes": False,
            "databento_download": False,
            "db_mutation": False,
        },
    }


def merge_outcomes_with_enrichment(
    outcomes: Sequence[Mapping[str, Any]],
    enrichments: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enrichment_by_id = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    rows: list[dict[str, Any]] = []
    enrichment_fields = (
        "vix_level",
        "vix_regime",
        "vix_percentile",
        "market_context_validity_classification",
        "gre_label",
        "gre_confidence",
        "gre_validity_classification",
        "crfd_validity_classification",
        "vwap_relation",
        "avwap_relation",
    )
    for outcome in outcomes:
        row = dict(outcome)
        enrichment = enrichment_by_id.get(str(outcome.get("trade_outcome_id") or ""))
        if enrichment:
            for field in enrichment_fields:
                row[field] = enrichment.get(field)
            row["enrichment_data_quality_flags"] = list(enrichment.get("data_quality_flags") or [])
        else:
            row["enrichment_data_quality_flags"] = ["missing_trade_outcome_enrichment"]
        rows.append(row)
    return rows


def aggregate_expectancy_groups(
    rows: Sequence[Mapping[str, Any]],
    key_fields: Sequence[str],
    *,
    validity_rules: Sequence[ContextValidityRule] = (),
    required_fields: Sequence[str] = (),
) -> list[dict[str, Any]]:
    filters = tuple(
        # The canonical engine treats this as a pure record-shape filter, not a
        # trading rule. It keeps partial-context views honest.
        AnalyticsFilter(field, "exists")
        for field in required_fields
    )
    result = run_canonical_query(
        rows,
        CanonicalAnalyticsQuery(
            name="core_expectancy_group",
            dimensions=tuple(key_fields),
            metrics=CORE_EXPECTANCY_QUERY_METRICS,
            filters=filters,
            validity_rules=tuple(validity_rules),
        ),
    )
    return list(result.grouped_rows)


def aggregate_expectancy_group(key: str, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return engine_aggregate_metric_group(key, rows)


def percentile_distribution(values: Sequence[float]) -> dict[str, float | None]:
    return engine_percentile_distribution(values)


def sample_class_for_count(count: int) -> str:
    return engine_sample_class_for_count(count)


def render_coverage_matrix_markdown(matrix: Mapping[str, Any]) -> str:
    lines = [
        "# Analytics Coverage Matrix",
        "",
        f"- Generated at: `{matrix.get('generated_at')}`",
        f"- Diagnostic only: `{matrix.get('diagnostic_only')}`",
        "",
        "| Capability | Status | Coverage | Blocker | Next step |",
        "|---|---:|---:|---|---|",
    ]
    for row in matrix.get("capabilities", []):
        lines.append(
            f"| `{row.get('capability')}` | `{row.get('status')}` | `{row.get('available_coverage')}` | "
            f"{row.get('blocker') or ''} | {row.get('next_step')} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_core_expectancy_markdown(analytics: Mapping[str, Any]) -> str:
    overall = analytics.get("overall", {})
    return "\n".join(
        [
            "# Core Expectancy Analytics",
            "",
            f"- Generated at: `{analytics.get('generated_at')}`",
            f"- Count: `{overall.get('count')}`",
            f"- Win rate: `{overall.get('win_rate')}`",
            f"- Average P&L proxy: `{overall.get('average_pnl_proxy')}`",
            f"- Median P&L proxy: `{overall.get('median_pnl_proxy')}`",
            f"- Average realized points: `{overall.get('average_realized_points')}`",
            f"- Median realized points: `{overall.get('median_realized_points')}`",
            f"- Sample class: `{overall.get('sample_class')}`",
            "",
            "Research/diagnostic only. No production changes, trading gates, broker actions, or strategy changes are recommended.",
            "",
        ]
    )


def render_group_markdown(title: str, groups: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        f"# {title}",
        "",
        "| Key | Count | Sample | Win rate | Avg P&L | Median P&L | Avg points | Median points | Avg hold |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in groups[:100]:
        lines.append(
            f"| {row.get('key')} | {row.get('count')} | {row.get('sample_class')} | {row.get('win_rate')} | "
            f"{row.get('average_pnl_proxy')} | {row.get('median_pnl_proxy')} | "
            f"{row.get('average_realized_points')} | {row.get('median_realized_points')} | {row.get('average_hold_seconds')} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_next_steps_markdown(matrix: Mapping[str, Any]) -> str:
    lines = ["# Analytics Next Steps", ""]
    for item in matrix.get("next_steps", []):
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "## Guardrails",
            "",
            "- Do not recommend production changes from this report.",
            "- Do not recommend trading gates.",
            "- Treat GRE analytics as partial until valid context coverage expands.",
            "- Do not fabricate R proxy, MFE, or MAE.",
            "",
        ]
    )
    return "\n".join(lines)


def _capability(
    name: str,
    status: str,
    inputs: Sequence[str],
    coverage: float | None,
    blocker: str | None,
    next_step: str,
) -> dict[str, Any]:
    return {
        "capability": name,
        "status": status,
        "required_inputs": list(inputs),
        "available_coverage": coverage,
        "blocker": blocker,
        "next_step": next_step,
    }


def _coverage_next_steps(capabilities: Sequence[Mapping[str, Any]]) -> list[str]:
    blocked = [row["capability"] for row in capabilities if row.get("status") == "BLOCKED"]
    partial = [row["capability"] for row in capabilities if row.get("status") == "PARTIAL"]
    steps = [
        "Use READY expectancy views for research/manual review only.",
        "Expand historical GRE/CRFD coverage before broad GRE-conditioned conclusions.",
        "Capture or derive initial risk before R-multiple analytics.",
        "Improve MFE/MAE coverage before exit-efficiency conclusions.",
        "Add rolling/time-split stability analytics before capital allocation research.",
    ]
    if blocked:
        steps.append(f"Blocked capabilities: {', '.join(blocked)}.")
    if partial:
        steps.append(f"Partial capabilities: {', '.join(partial)}.")
    return steps


def _with_context_buckets(row: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    payload["vix_percentile_bucket"] = _vix_percentile_bucket(_number(row.get("vix_percentile")))
    payload["gre_confidence_bucket"] = _confidence_bucket(_number(row.get("gre_confidence")))
    return payload


def _ready_partial_blocked(count: int, total: int, *, full_threshold: float) -> str:
    if total <= 0 or count <= 0:
        return "BLOCKED"
    if count / total >= full_threshold:
        return "READY"
    return "PARTIAL"


def _threshold_status(count: int, total: int, *, partial_if_any: bool) -> str:
    if total <= 0 or count <= 0:
        return "BLOCKED"
    if count / total >= 0.5:
        return "READY"
    return "PARTIAL" if partial_if_any else "BLOCKED"


def _valid_context(value: Any) -> bool:
    return str(value or "").upper() == "VALID"


def _vix_percentile_bucket(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    pct = value * 100 if value <= 1.0 else value
    if pct < 20:
        return "0-20"
    if pct < 40:
        return "20-40"
    if pct < 60:
        return "40-60"
    if pct < 80:
        return "60-80"
    return "80-100"


def _confidence_bucket(value: float | None) -> str:
    if value is None:
        return "UNKNOWN"
    if value < 20:
        return "0-20"
    if value < 40:
        return "20-40"
    if value < 60:
        return "40-60"
    if value < 80:
        return "60-80"
    return "80-100"


def _win_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    values = [_number(row.get("realized_pnl_proxy")) for row in rows]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return round(sum(1 for value in values if value > 0) / len(values), 6)


def _numeric_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for value in (_number(row.get(key)) for row in rows) if value is not None]


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def _percentile(sorted_values: Sequence[float], pct: int) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return round(sorted_values[0], 6)
    position = (len(sorted_values) - 1) * pct / 100
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return round(sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction, 6)


def _max_by(rows: Sequence[Mapping[str, Any]], key: str) -> Mapping[str, Any] | None:
    candidates = [row for row in rows if _number(row.get(key)) is not None]
    return max(candidates, key=lambda row: _number(row.get(key)) or 0.0) if candidates else None


def _min_by(rows: Sequence[Mapping[str, Any]], key: str) -> Mapping[str, Any] | None:
    candidates = [row for row in rows if _number(row.get(key)) is not None]
    return min(candidates, key=lambda row: _number(row.get(key)) or 0.0) if candidates else None


def _trade_ref(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
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


def _counts(values: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if value in (None, ""):
            continue
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _rate(num: int, den: int) -> float | None:
    if den <= 0:
        return None
    return round(num / den, 6)


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _read_json_mapping(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
