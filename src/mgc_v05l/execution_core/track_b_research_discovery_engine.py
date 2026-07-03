"""Diagnostic Research Discovery Engine for enriched Track B outcomes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Callable, Iterable, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import BoundedJsonlConfig, write_bounded_jsonl
from mgc_v05l.execution_core.bounded_snapshot import BoundedSnapshotConfig, write_bounded_snapshot_json
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_ENRICHMENT_DIR,
    ENRICHMENT_JSONL,
    SUMMARY_JSON as ENRICHMENT_SUMMARY_JSON,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_OUTCOME_LAYER_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_OUTCOME_LAYER_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_JSONL
DEFAULT_ENRICHMENT_SUMMARY_PATH = DEFAULT_ENRICHMENT_DIR / ENRICHMENT_SUMMARY_JSON
DEFAULT_T3_SCORECARD_PATH = DEFAULT_OUTPUT_ROOT / "trade_outcome_scorecards" / "latest_trade_outcome_scorecards.json"
DEFAULT_T6_ANALYTICS_PATH = DEFAULT_OUTPUT_ROOT / "context_aware_trade_analytics" / "latest_context_aware_trade_analytics.json"
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research" / "research_discovery_engine"

CANDIDATES_JSONL = "research_discovery_candidates.jsonl"
SUMMARY_JSON = "latest_research_discovery_summary.json"
SUMMARY_MD = "latest_research_discovery_summary.md"
TOP_CANDIDATES_MD = "research_discovery_top_candidates.md"
DATA_QUALITY_MD = "research_discovery_data_quality.md"
GUARDRAIL_MD = "research_discovery_guardrail_report.md"

SCHEMA_VERSION = "track_b_research_discovery_candidate_v1"
SUMMARY_SCHEMA_VERSION = "track_b_research_discovery_summary_v1"


@dataclass(frozen=True)
class ResearchDiscoveryResult:
    candidates: list[dict[str, Any]]
    summary: dict[str, Any]
    candidates_path: Path
    summary_path: Path
    summary_markdown_path: Path
    top_candidates_path: Path
    data_quality_path: Path
    guardrail_path: Path


@dataclass(frozen=True)
class CandidateFamily:
    name: str
    dimensions: tuple[str, ...]
    context_validity_requirements: dict[str, str]
    row_filter: Callable[[Mapping[str, Any]], bool] = lambda row: True


def run_research_discovery_engine(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    enrichment_summary_path: Path = DEFAULT_ENRICHMENT_SUMMARY_PATH,
    t3_scorecard_path: Path = DEFAULT_T3_SCORECARD_PATH,
    t6_analytics_path: Path = DEFAULT_T6_ANALYTICS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
    max_snapshot_bytes: int | None = None,
    jsonl_config: BoundedJsonlConfig | None = None,
) -> ResearchDiscoveryResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    enrichment_summary = _read_json_mapping(enrichment_summary_path)
    t3_scorecard = _read_json_mapping(t3_scorecard_path)
    t6_analytics = _read_json_mapping(t6_analytics_path)
    candidates = build_research_discovery_candidates(
        outcomes,
        enrichments=enrichments,
        generated_at=generated_at,
        source_paths={
            "canonical_trade_outcomes": outcomes_path,
            "trade_outcome_enrichment": enrichments_path,
            "trade_outcome_enrichment_summary": enrichment_summary_path,
            "trade_outcome_scorecards": t3_scorecard_path,
            "context_aware_trade_analytics": t6_analytics_path,
        },
    )
    summary = build_research_discovery_summary(
        candidates,
        outcomes=outcomes,
        enrichments=enrichments,
        enrichment_summary=enrichment_summary,
        t3_scorecard=t3_scorecard,
        t6_analytics=t6_analytics,
        generated_at=generated_at,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    candidates_path = output_dir / CANDIDATES_JSONL
    write_bounded_jsonl(candidates_path, candidates, config=jsonl_config)
    snapshot_config = BoundedSnapshotConfig(max_bytes=max_snapshot_bytes) if max_snapshot_bytes else BoundedSnapshotConfig()
    summary_path = output_dir / SUMMARY_JSON
    write_bounded_snapshot_json(summary_path, summary, config=snapshot_config)
    summary_markdown_path = output_dir / SUMMARY_MD
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    top_candidates_path = output_dir / TOP_CANDIDATES_MD
    top_candidates_path.write_text(render_top_candidates_markdown(candidates), encoding="utf-8")
    data_quality_path = output_dir / DATA_QUALITY_MD
    data_quality_path.write_text(render_data_quality_markdown(summary), encoding="utf-8")
    guardrail_path = output_dir / GUARDRAIL_MD
    guardrail_path.write_text(render_guardrail_markdown(summary), encoding="utf-8")
    return ResearchDiscoveryResult(
        candidates=candidates,
        summary=summary,
        candidates_path=candidates_path,
        summary_path=summary_path,
        summary_markdown_path=summary_markdown_path,
        top_candidates_path=top_candidates_path,
        data_quality_path=data_quality_path,
        guardrail_path=guardrail_path,
    )


def build_research_discovery_candidates(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    rows = _merge_outcomes_with_enrichment(outcomes, enrichments)
    baseline = _metrics(rows)
    candidates: list[dict[str, Any]] = []
    for family in _candidate_families():
        grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for row in rows:
            if not family.row_filter(row):
                continue
            key = tuple(_dimension_value(row, dimension) for dimension in family.dimensions)
            if any(value in (None, "", "UNKNOWN") for value in key):
                continue
            grouped.setdefault(key, []).append(row)
        for key, cohort in grouped.items():
            candidates.append(
                _build_candidate(
                    family=family,
                    key=key,
                    rows=cohort,
                    baseline_rows=rows,
                    baseline=baseline,
                    generated_at=generated_at,
                    source_paths=source_paths or {},
                )
            )
    return sorted(
        candidates,
        key=lambda row: (
            row.get("ranking_score") or 0.0,
            abs(row.get("effect_size_proxy") or 0.0),
            row.get("sample_size") or 0,
        ),
        reverse=True,
    )


def build_research_discovery_summary(
    candidates: Sequence[Mapping[str, Any]],
    *,
    outcomes: Sequence[Mapping[str, Any]],
    enrichments: Sequence[Mapping[str, Any]],
    enrichment_summary: Mapping[str, Any],
    t3_scorecard: Mapping[str, Any],
    t6_analytics: Mapping[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    sample_classes = _counts(row.get("confidence_class") for row in candidates)
    recommendation_levels = _counts(row.get("recommendation_level") for row in candidates)
    family_counts = _counts(row.get("family") for row in candidates)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_effect": False,
        "trading_gate": False,
        "production_recommendation": False,
        "input_counts": {
            "outcomes": len(outcomes),
            "enrichments": len(enrichments),
            "candidates": len(candidates),
        },
        "candidate_counts": {
            "by_sample_class": sample_classes,
            "by_recommendation_level": recommendation_levels,
            "by_family": family_counts,
        },
        "top_candidates": [dict(row) for row in candidates[:10]],
        "context_validity": enrichment_summary.get("context_validity"),
        "source_summaries": {
            "trade_outcome_scorecards_overall": t3_scorecard.get("overall"),
            "context_aware_analytics_overall": t6_analytics.get("overall"),
        },
        "data_quality": {
            "limitations": _summary_limitations(enrichment_summary),
            "low_sample_candidate_count": sum(
                1 for row in candidates if row.get("confidence_class") in {"EXPLORATORY", "PRELIMINARY"}
            ),
            "invalid_context_rule": "Stale or unavailable context is used only for data-quality hypotheses, not edge claims.",
        },
        "guardrails": _guardrails(),
    }


def _candidate_families() -> tuple[CandidateFamily, ...]:
    return (
        CandidateFamily("strategy_x_session", ("strategy_id", "session"), {}),
        CandidateFamily("strategy_x_vix_percentile_bucket", ("strategy_id", "vix_percentile_bucket"), {"market_context": "VALID"}),
        CandidateFamily(
            "strategy_x_valid_gre_label",
            ("strategy_id", "gre_label"),
            {"gre": "VALID"},
            row_filter=lambda row: row.get("gre_validity_classification") == "VALID",
        ),
        CandidateFamily("session_x_vix_percentile_bucket", ("session", "vix_percentile_bucket"), {"market_context": "VALID"}),
        CandidateFamily("instrument_x_session", ("instrument", "session"), {}),
        CandidateFamily("exit_policy_x_vix_percentile_bucket", ("exit_policy", "vix_percentile_bucket"), {"market_context": "VALID"}),
        CandidateFamily(
            "side_x_valid_gre_label",
            ("side", "gre_label"),
            {"gre": "VALID"},
            row_filter=lambda row: row.get("gre_validity_classification") == "VALID",
        ),
        CandidateFamily(
            "vwap_avwap_relation_x_outcome",
            ("vwap_relation", "avwap_relation"),
            {"crfd": "VALID"},
            row_filter=lambda row: row.get("crfd_validity_classification") == "VALID",
        ),
    )


def _build_candidate(
    *,
    family: CandidateFamily,
    key: tuple[Any, ...],
    rows: Sequence[Mapping[str, Any]],
    baseline_rows: Sequence[Mapping[str, Any]],
    baseline: Mapping[str, Any],
    generated_at: datetime,
    source_paths: Mapping[str, Path | str],
) -> dict[str, Any]:
    metrics = _metrics(rows)
    effect = _effect_size(metrics, baseline)
    sample_class = sample_class_for_count(len(rows))
    data_quality_flags = _candidate_quality_flags(rows, sample_class=sample_class)
    candidate_id = _candidate_id(family.name, family.dimensions, key)
    filters = {dimension: value for dimension, value in zip(family.dimensions, key)}
    hypothesis = _hypothesis_text(family.name, filters, effect, sample_class)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "candidate_id": candidate_id,
        "family": family.name,
        "hypothesis_text": hypothesis,
        "dimensions": list(family.dimensions),
        "filters": filters,
        "sample_size": len(rows),
        "comparison_group": {
            "name": "all_completed_outcomes",
            "sample_size": len(baseline_rows),
            "average_pnl_proxy": baseline.get("average_pnl_proxy"),
            "win_rate": baseline.get("win_rate"),
        },
        "metrics": metrics,
        "effect_size_proxy": effect,
        "confidence_class": sample_class,
        "data_quality_flags": data_quality_flags,
        "context_validity_requirements": dict(family.context_validity_requirements),
        "recommendation_level": _recommendation_level(sample_class, effect, data_quality_flags),
        "ranking_score": _ranking_score(sample_class, effect, data_quality_flags),
        "source_refs": {key: str(value) for key, value in source_paths.items()},
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "strategy_change_recommendation": False,
    }


def sample_class_for_count(count: int) -> str:
    if count < 10:
        return "EXPLORATORY"
    if count < 30:
        return "PRELIMINARY"
    if count < 100:
        return "DEVELOPING"
    return "RESEARCH_GRADE"


def _metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnl = _numeric_values(rows, "realized_pnl_proxy")
    points = _numeric_values(rows, "realized_points")
    hold = _numeric_values(rows, "hold_seconds")
    best = max(rows, key=lambda row: _number(row.get("realized_pnl_proxy")) if _number(row.get("realized_pnl_proxy")) is not None else float("-inf")) if rows else {}
    worst = min(rows, key=lambda row: _number(row.get("realized_pnl_proxy")) if _number(row.get("realized_pnl_proxy")) is not None else float("inf")) if rows else {}
    return {
        "trade_count": len(rows),
        "win_rate": _win_rate(rows),
        "average_pnl_proxy": _average(pnl),
        "median_realized_points": _median(points),
        "best_trade": _trade_ref(best),
        "worst_trade": _trade_ref(worst),
        "average_hold_seconds": _average(hold),
        "median_hold_seconds": _median(hold),
        "data_quality_coverage": _data_quality_coverage(rows),
    }


def _merge_outcomes_with_enrichment(
    outcomes: Sequence[Mapping[str, Any]],
    enrichments: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    enrichment_by_id = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        trade_id = str(outcome.get("trade_outcome_id") or "")
        if not trade_id:
            continue
        row = dict(outcome)
        enrichment = enrichment_by_id.get(trade_id, {})
        for key, value in enrichment.items():
            if key not in {"schema_version", "generated_at", "source_refs", "diagnostic_only"}:
                row[key] = value
        row["vix_percentile_bucket"] = _vix_percentile_bucket(_number(row.get("vix_percentile")))
        rows.append(row)
    return rows


def _dimension_value(row: Mapping[str, Any], dimension: str) -> Any:
    if dimension == "session":
        return row.get("session") or row.get("session_at_entry")
    return row.get(dimension)


def _effect_size(metrics: Mapping[str, Any], baseline: Mapping[str, Any]) -> float | None:
    average = _number(metrics.get("average_pnl_proxy"))
    base = _number(baseline.get("average_pnl_proxy"))
    if average is None or base is None:
        return None
    return round(average - base, 6)


def _ranking_score(sample_class: str, effect: float | None, flags: Sequence[str]) -> float:
    if effect is None:
        return 0.0
    sample_weight = {"EXPLORATORY": 0.2, "PRELIMINARY": 0.5, "DEVELOPING": 0.8, "RESEARCH_GRADE": 1.0}.get(sample_class, 0.0)
    quality_weight = 0.65 if flags else 1.0
    return round(abs(effect) * sample_weight * quality_weight, 6)


def _recommendation_level(sample_class: str, effect: float | None, flags: Sequence[str]) -> str:
    if sample_class == "EXPLORATORY":
        return "OBSERVE"
    if flags:
        return "MANUAL_REVIEW"
    if sample_class in {"DEVELOPING", "RESEARCH_GRADE"} and effect is not None and abs(effect) > 100:
        return "SHADOW_RESEARCH"
    if sample_class in {"PRELIMINARY", "DEVELOPING", "RESEARCH_GRADE"}:
        return "MANUAL_REVIEW"
    return "DEFER"


def _candidate_quality_flags(rows: Sequence[Mapping[str, Any]], *, sample_class: str) -> list[str]:
    flags: set[str] = set()
    if sample_class in {"EXPLORATORY", "PRELIMINARY"}:
        flags.add(f"sample_class_{sample_class.lower()}")
    for row in rows:
        for flag in row.get("data_quality_flags") or ():
            if flag in {"missing_realized_r_proxy", "missing_mfe", "missing_mae", "missing_crfd_regime_join"}:
                flags.add(str(flag))
        if row.get("gre_validity_classification") in {"STALE", "OUTSIDE_PROVIDER_WINDOW"}:
            flags.add("non_valid_gre_context_present")
        if row.get("crfd_validity_classification") in {"STALE", "OUTSIDE_PROVIDER_WINDOW"}:
            flags.add("non_valid_crfd_context_present")
    return sorted(flags)


def _hypothesis_text(family: str, filters: Mapping[str, Any], effect: float | None, sample_class: str) -> str:
    direction = "higher" if (effect or 0.0) > 0 else "lower" if (effect or 0.0) < 0 else "similar"
    parts = ", ".join(f"{key}={value}" for key, value in filters.items())
    return f"Research hypothesis: {family} cohort ({parts}) shows {direction} descriptive P&L proxy than the all-outcome baseline; sample class {sample_class}."


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    counts = summary.get("candidate_counts") or {}
    return "\n".join(
        [
            "# Research Discovery Summary",
            "",
            f"- Generated at: {summary.get('generated_at')}",
            f"- Candidates: {(summary.get('input_counts') or {}).get('candidates')}",
            f"- Outcomes: {(summary.get('input_counts') or {}).get('outcomes')}",
            f"- Enrichments: {(summary.get('input_counts') or {}).get('enrichments')}",
            f"- Sample classes: {counts.get('by_sample_class')}",
            f"- Recommendation levels: {counts.get('by_recommendation_level')}",
            "",
            "Diagnostic-only: no production recommendations, no gates, no strategy changes.",
            "",
        ]
    )


def render_top_candidates_markdown(candidates: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# Research Discovery Top Candidates",
        "",
        "| Rank | Family | Sample | Class | Effect | Recommendation | Hypothesis |",
        "| ---: | --- | ---: | --- | ---: | --- | --- |",
    ]
    for idx, row in enumerate(candidates[:20], start=1):
        lines.append(
            f"| {idx} | {row.get('family')} | {row.get('sample_size')} | {row.get('confidence_class')} | "
            f"{row.get('effect_size_proxy')} | {row.get('recommendation_level')} | {row.get('hypothesis_text')} |"
        )
    lines.extend(["", "These are research hypotheses only, not production instructions.", ""])
    return "\n".join(lines)


def render_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    data_quality = summary.get("data_quality") or {}
    lines = ["# Research Discovery Data Quality", ""]
    for limitation in data_quality.get("limitations") or []:
        lines.append(f"- {limitation}")
    lines.append(f"- Low-sample candidates: {data_quality.get('low_sample_candidate_count')}")
    lines.append(f"- Invalid context rule: {data_quality.get('invalid_context_rule')}")
    lines.append("")
    return "\n".join(lines)


def render_guardrail_markdown(summary: Mapping[str, Any]) -> str:
    guardrails = summary.get("guardrails") or {}
    lines = ["# Research Discovery Guardrail Report", ""]
    for key, value in guardrails.items():
        lines.append(f"- {key}: {value}")
    lines.append("")
    return "\n".join(lines)


def _summary_limitations(enrichment_summary: Mapping[str, Any]) -> list[str]:
    missing = enrichment_summary.get("missing_reasons") or {}
    limitations: list[str] = []
    if missing.get("missing_realized_r_proxy"):
        limitations.append("R proxy is unavailable; use P&L proxy and realized points only.")
    if missing.get("missing_mfe") or missing.get("missing_mae"):
        limitations.append("MFE/MAE remains sparse; do not infer exit quality from discovery candidates.")
    if missing.get("missing_gre_context"):
        limitations.append("GRE is only available for Gold-context rows with valid coverage.")
    if missing.get("outside_provider_window_gre_context"):
        limitations.append("Some GRE joins are outside provider window and must not be used as valid GRE evidence.")
    return limitations


def _guardrails() -> dict[str, bool]:
    return {
        "diagnostic_only": True,
        "production_effect": False,
        "production_recommendation": False,
        "trading_gate": False,
        "strategy_change_recommendation": False,
        "broker_actions": False,
        "runtime_integration": False,
        "threshold_optimization": False,
    }


def _candidate_id(family: str, dimensions: Sequence[str], key: Sequence[Any]) -> str:
    payload = json.dumps({"family": family, "dimensions": list(dimensions), "key": list(key)}, sort_keys=True)
    return f"rdx_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:20]}"


def _data_quality_coverage(rows: Sequence[Mapping[str, Any]]) -> float | None:
    if not rows:
        return None
    clean = sum(1 for row in rows if not row.get("data_quality_flags"))
    return _rate(clean, len(rows))


def _trade_ref(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "trade_outcome_id": row.get("trade_outcome_id"),
        "strategy_id": row.get("strategy_id"),
        "lane_id": row.get("lane_id"),
        "instrument": row.get("instrument"),
        "contract": row.get("contract"),
        "session": row.get("session") or row.get("session_at_entry"),
        "side": row.get("side"),
        "realized_points": row.get("realized_points"),
        "realized_pnl_proxy": row.get("realized_pnl_proxy"),
        "hold_seconds": row.get("hold_seconds"),
    }


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


def _numeric_values(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    return [value for value in (_number(row.get(key)) for row in rows) if value is not None]


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return round(float(median(values)), 6)


def _win_rate(rows: Sequence[Mapping[str, Any]]) -> float | None:
    pnl = _numeric_values(rows, "realized_pnl_proxy")
    return _rate(sum(1 for value in pnl if value > 0), len(pnl))


def _rate(part: int, whole: int) -> float | None:
    if whole <= 0:
        return None
    return round(part / whole, 6)


def _counts(values: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return []
    return rows


def _read_json_mapping(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
