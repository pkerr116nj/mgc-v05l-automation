"""REF1 offline research experiment engine.

This module evaluates deterministic research experiments over completed
canonical trade artifacts. It is offline research infrastructure only: it has no
broker, runtime, strategy, Managed Exit, order, or trading-gate authority.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOE_OUTPUT_DIR,
    ENRICHMENT_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    OUTCOMES_JSONL,
)
from mgc_v05l.execution_core.track_b_canonical_trade_path_layer import (
    CANONICAL_TRADE_PATHS_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_TRADE_PATH_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_winner_loser_feature_discovery import (
    DEFAULT_OUTPUT_DIR as DEFAULT_RA9_OUTPUT_DIR,
    HYPOTHESES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_CTOL_OUTPUT_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_CTOE_OUTPUT_DIR / ENRICHMENT_JSONL
DEFAULT_TRADE_PATHS_PATH = DEFAULT_TRADE_PATH_OUTPUT_DIR / CANONICAL_TRADE_PATHS_JSONL
DEFAULT_RA9_HYPOTHESES_PATH = DEFAULT_RA9_OUTPUT_DIR / HYPOTHESES_JSONL
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_experiments" / "offline_experiment_engine"
DEFAULT_ENRICHMENTS_PATH = DEFAULT_CTOE_OUTPUT_DIR / ENRICHMENT_JSONL

EXPERIMENT_CONTRACT_MD = "ref1_experiment_contract.md"
EXPERIMENT_SCHEMA_JSON = "ref1_experiment_schema.json"
SAMPLE_EXPERIMENTS_JSON = "ref1_sample_experiments.json"
SAMPLE_RESULTS_JSON = "ref1_sample_experiment_results.json"
SUMMARY_MD = "ref1_experiment_summary.md"
DATA_QUALITY_MD = "ref1_data_quality_report.md"
BLOCKED_MD = "ref1_blocked_experiments.md"

EXPERIMENT_SCHEMA_VERSION = "canonical_research_experiment_v1"
RESULT_SCHEMA_VERSION = "canonical_research_experiment_result_v1"
SUMMARY_SCHEMA_VERSION = "ref1_offline_research_experiment_summary_v1"

GUARDRAILS = {
    "diagnostic_only": True,
    "production_recommendation": False,
    "trading_gate": False,
}


@dataclass(frozen=True)
class OfflineResearchExperimentRun:
    experiments: list[dict[str, Any]]
    results: list[dict[str, Any]]
    summary: dict[str, Any]
    output_dir: Path
    sample_experiments_path: Path
    sample_results_path: Path
    summary_path: Path
    data_quality_path: Path
    blocked_path: Path


def run_offline_research_experiments(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    trade_paths_path: Path = DEFAULT_TRADE_PATHS_PATH,
    ra9_hypotheses_path: Path = DEFAULT_RA9_HYPOTHESES_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> OfflineResearchExperimentRun:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    trade_paths = _read_jsonl(trade_paths_path)
    hypotheses = _read_jsonl(ra9_hypotheses_path)
    rows = build_experiment_population(outcomes, enrichments=enrichments, trade_paths=trade_paths)
    experiments = sample_experiments(rows=rows, hypotheses=hypotheses, generated_at=generated_at)
    results = [
        run_experiment(
            experiment,
            rows=rows,
            generated_at=generated_at,
            provenance={
                "canonical_trade_outcomes": str(outcomes_path),
                "trade_outcome_enrichment": str(enrichments_path),
                "canonical_trade_paths": str(trade_paths_path),
                "ra9_hypotheses": str(ra9_hypotheses_path),
            },
        )
        for experiment in experiments
    ]
    summary = build_experiment_summary(experiments, results, rows=rows, generated_at=generated_at)
    output_dir.mkdir(parents=True, exist_ok=True)
    contract_path = output_dir / EXPERIMENT_CONTRACT_MD
    schema_path = output_dir / EXPERIMENT_SCHEMA_JSON
    sample_experiments_path = output_dir / SAMPLE_EXPERIMENTS_JSON
    sample_results_path = output_dir / SAMPLE_RESULTS_JSON
    summary_path = output_dir / SUMMARY_MD
    data_quality_path = output_dir / DATA_QUALITY_MD
    blocked_path = output_dir / BLOCKED_MD
    contract_path.write_text(render_contract_markdown(), encoding="utf-8")
    _write_json(schema_path, experiment_schema())
    _write_json(sample_experiments_path, experiments)
    _write_json(sample_results_path, results)
    summary_path.write_text(render_summary_markdown(summary, results), encoding="utf-8")
    data_quality_path.write_text(render_data_quality_markdown(summary), encoding="utf-8")
    blocked_path.write_text(render_blocked_markdown(results), encoding="utf-8")
    return OfflineResearchExperimentRun(
        experiments=experiments,
        results=results,
        summary=summary,
        output_dir=output_dir,
        sample_experiments_path=sample_experiments_path,
        sample_results_path=sample_results_path,
        summary_path=summary_path,
        data_quality_path=data_quality_path,
        blocked_path=blocked_path,
    )


def create_experiment(
    *,
    experiment_id: str,
    title: str,
    hypothesis: str,
    experiment_type: str,
    generated_at: datetime,
    description: str = "",
    population_definition: Mapping[str, Any] | None = None,
    baseline_definition: Mapping[str, Any] | None = None,
    variant_definition: Mapping[str, Any] | None = None,
    metrics: Sequence[str] | None = None,
    data_requirements: Sequence[str] | None = None,
) -> dict[str, Any]:
    experiment = {
        "schema_version": EXPERIMENT_SCHEMA_VERSION,
        "experiment_id": experiment_id,
        "title": title,
        "description": description,
        "hypothesis": hypothesis,
        "created_at": generated_at.isoformat(),
        "updated_at": generated_at.isoformat(),
        "experiment_type": experiment_type,
        "population_definition": dict(population_definition or {}),
        "baseline_definition": dict(baseline_definition or {"filters": []}),
        "variant_definition": dict(variant_definition or {}),
        "metrics": list(metrics or default_metric_names()),
        "data_requirements": list(data_requirements or ()),
        "status": "DRAFT",
        "guardrails": dict(GUARDRAILS),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    experiment["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(experiment))
    return experiment


def build_experiment_population(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]] = (),
    trade_paths: Sequence[Mapping[str, Any]] = (),
) -> list[dict[str, Any]]:
    enrichment_by_id = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    path_by_id = {str(row.get("trade_outcome_id")): row for row in trade_paths if row.get("trade_outcome_id")}
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        trade_outcome_id = str(outcome.get("trade_outcome_id") or "")
        enrichment = enrichment_by_id.get(trade_outcome_id, {})
        path = path_by_id.get(trade_outcome_id, {})
        row = dict(outcome)
        for key, value in enrichment.items():
            row.setdefault(key, value)
        row["path"] = path
        row["canonical_trade_path_id"] = row.get("canonical_trade_path_id") or path.get("canonical_trade_path_id")
        row["path_status"] = row.get("path_status") or path.get("path_coverage_status")
        row["path_complete"] = row.get("path_complete") if row.get("path_complete") is not None else path.get("path_complete_entry_to_exit")
        row["path_sample_count"] = row.get("path_sample_count") if row.get("path_sample_count") is not None else path.get("path_sample_count")
        row["entry_to_exit_path"] = path.get("entry_to_exit_path") if isinstance(path.get("entry_to_exit_path"), list) else []
        rows.append(row)
    return rows


def sample_experiments(
    *,
    rows: Sequence[Mapping[str, Any]],
    hypotheses: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> list[dict[str, Any]]:
    lane = _best_hypothesis_lane(hypotheses) or _most_common(row.get("lane_id") for row in rows)
    session = _most_common(row.get("session_at_entry") for row in rows) or "UNKNOWN"
    return [
        create_experiment(
            experiment_id="ref1_timebox_grid_complete_paths",
            title="Timebox Grid Over Complete-Path Trades",
            description="Tests alternative time exits only where complete path samples exist.",
            hypothesis="Alternative timebox exits may change realized P&L on trades with complete retained paths.",
            experiment_type="EXIT_POLICY",
            population_definition={"filters": [{"field": "path_complete", "op": "eq", "value": True}]},
            variant_definition={"kind": "timebox_grid", "minutes": [15, 30, 45, 60, 90, 120]},
            data_requirements=["complete_entry_to_exit_path", "entry_price", "side"],
            generated_at=generated_at,
        ),
        create_experiment(
            experiment_id="ref1_vix_percentile_filter",
            title="VIX Percentile Median Split",
            description="Compares all rows against a deterministic VIX percentile threshold.",
            hypothesis="VIX percentile context may separate trade outcomes for research review.",
            experiment_type="ENTRY_FILTER",
            population_definition={"filters": []},
            variant_definition={"kind": "field_filter", "filters": [{"field": "vix_percentile", "op": "gte", "value": 0.5}]},
            data_requirements=["vix_percentile"],
            generated_at=generated_at,
        ),
        create_experiment(
            experiment_id="ref1_lane_vix_hypothesis",
            title="Lane-Specific VIX Hypothesis",
            description="Uses the top RA9 lane when available and applies a simple VIX percentile split.",
            hypothesis="The selected RA9 lane may behave differently above a VIX percentile threshold.",
            experiment_type="ENTRY_FILTER",
            population_definition={"filters": [{"field": "lane_id", "op": "eq", "value": lane}] if lane else []},
            variant_definition={"kind": "field_filter", "filters": [{"field": "vix_percentile", "op": "gte", "value": 0.5}]},
            data_requirements=["lane_id", "vix_percentile"],
            generated_at=generated_at,
        ),
        create_experiment(
            experiment_id="ref1_session_filter",
            title="Observed Session Filter",
            description="Compares the most common observed session rows against the broader completed population.",
            hypothesis="Session context may separate trade outcomes for research review.",
            experiment_type="SESSION_FILTER",
            population_definition={"filters": []},
            variant_definition={"kind": "field_filter", "filters": [{"field": "session_at_entry", "op": "eq", "value": session}]},
            data_requirements=["session_at_entry"],
            generated_at=generated_at,
        ),
    ]


def run_experiment(
    experiment: Mapping[str, Any],
    *,
    rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    population_rows, population_exclusions = _apply_filter_definition(rows, experiment.get("population_definition", {}))
    variant_definition = experiment.get("variant_definition") if isinstance(experiment.get("variant_definition"), Mapping) else {}
    baseline_definition = experiment.get("baseline_definition") if isinstance(experiment.get("baseline_definition"), Mapping) else {}
    baseline_rows, baseline_exclusions = _apply_filter_definition(population_rows, baseline_definition)
    experiment_type = str(experiment.get("experiment_type") or "CUSTOM")
    data_quality_flags: list[str] = []
    variant_rows: list[Mapping[str, Any]]
    variant_metrics: dict[str, Any]
    variant_count: int
    status = "COMPLETE"
    exclusion_reasons = _merge_counts(population_exclusions, baseline_exclusions)

    if experiment_type == "EXIT_POLICY" and variant_definition.get("kind") == "timebox_grid":
        variant_metrics, variant_count, blocked_reason = _run_timebox_grid(population_rows, variant_definition)
        variant_rows = []
        if blocked_reason:
            status = "BLOCKED"
            data_quality_flags.append(blocked_reason)
            exclusion_reasons[blocked_reason] = exclusion_reasons.get(blocked_reason, 0) + len(population_rows)
    elif experiment_type == "EXIT_POLICY" and variant_definition.get("kind") == "mfe_giveback":
        variant_metrics, variant_count, blocked_reason = _run_mfe_giveback(population_rows, variant_definition)
        variant_rows = []
        if blocked_reason:
            status = "BLOCKED"
            data_quality_flags.append(blocked_reason)
            exclusion_reasons[blocked_reason] = exclusion_reasons.get(blocked_reason, 0) + len(population_rows)
    else:
        variant_rows, variant_exclusions = _apply_filter_definition(population_rows, variant_definition)
        variant_metrics = calculate_metrics(variant_rows)
        variant_count = len(variant_rows)
        exclusion_reasons = _merge_counts(exclusion_reasons, variant_exclusions)
        if variant_count == 0:
            status = "BLOCKED"
            data_quality_flags.append("variant_population_empty")

    baseline_metrics = calculate_metrics(baseline_rows)
    delta_metrics = _delta_metrics(baseline_metrics, variant_metrics)
    confidence = _confidence_label(variant_count if variant_count else len(baseline_rows))
    if status != "BLOCKED" and data_quality_flags:
        status = "COMPLETE_WITH_WARNINGS"
    payload = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "experiment_id": experiment.get("experiment_id"),
        "run_id": _stable_id("experiment_run", experiment.get("experiment_id"), generated_at.isoformat()),
        "generated_at": generated_at.isoformat(),
        "population_count": len(population_rows),
        "baseline_count": len(baseline_rows),
        "variant_count": variant_count,
        "excluded_count": sum(exclusion_reasons.values()),
        "exclusion_reasons": dict(sorted(exclusion_reasons.items())),
        "baseline_metrics": baseline_metrics,
        "variant_metrics": variant_metrics,
        "delta_metrics": delta_metrics,
        "confidence_label": confidence if status != "BLOCKED" else "INSUFFICIENT_DATA",
        "status": status,
        "data_quality_flags": sorted(set(data_quality_flags)),
        "provenance": dict(provenance or {}),
        "guardrails": dict(GUARDRAILS),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
    }
    payload["deterministic_fingerprint"] = _fingerprint(_fingerprint_payload(payload))
    return payload


def calculate_metrics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pnls = [_number(row.get("realized_pnl_proxy")) for row in rows]
    pnls = [value for value in pnls if value is not None]
    winners = [value for value in pnls if value > 0]
    losers = [value for value in pnls if value < 0]
    count = len(rows)
    total = sum(pnls) if pnls else None
    return {
        "trade_count": count,
        "total_pnl_proxy": _round(total),
        "average_pnl_proxy": _round(sum(pnls) / len(pnls)) if pnls else None,
        "median_pnl_proxy": _round(float(median(pnls))) if pnls else None,
        "win_rate": _round(len(winners) / len(pnls)) if pnls else None,
        "profit_factor_proxy": _profit_factor(winners, losers),
        "average_winner": _round(sum(winners) / len(winners)) if winners else None,
        "average_loser": _round(sum(losers) / len(losers)) if losers else None,
        "payoff_ratio": _round((sum(winners) / len(winners)) / abs(sum(losers) / len(losers))) if winners and losers else None,
        "max_win": _round(max(winners)) if winners else None,
        "max_loss": _round(min(losers)) if losers else None,
        "sample_class": _sample_class(count),
        "data_coverage": {
            "pnl_proxy": _rate(len(pnls), count),
            "valid_vix_percentile": _rate(sum(1 for row in rows if _number(row.get("vix_percentile")) is not None), count),
            "valid_gre": _rate(sum(1 for row in rows if row.get("gre_validity_classification") == "VALID"), count),
            "path_available": _rate(sum(1 for row in rows if row.get("path_available") is True or _path_available(row)), count),
        },
    }


def build_experiment_summary(
    experiments: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    *,
    rows: Sequence[Mapping[str, Any]],
    generated_at: datetime,
) -> dict[str, Any]:
    status_counts = _counts(row.get("status") for row in results)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "experiment_count": len(experiments),
        "result_count": len(results),
        "population_rows": len(rows),
        "status_counts": status_counts,
        "blocked_experiment_count": status_counts.get("BLOCKED", 0),
        "complete_experiment_count": status_counts.get("COMPLETE", 0),
        "data_quality": {
            "rows_missing_path": sum(1 for row in rows if not _path_available(row)),
            "rows_with_path": sum(1 for row in rows if _path_available(row)),
            "rows_with_complete_path_samples": sum(1 for row in rows if _has_complete_path_samples(row)),
            "rows_with_vix_percentile": sum(1 for row in rows if _number(row.get("vix_percentile")) is not None),
            "rows_with_valid_gre": sum(1 for row in rows if row.get("gre_validity_classification") == "VALID"),
        },
        "guardrails": dict(GUARDRAILS),
    }


def _run_timebox_grid(rows: Sequence[Mapping[str, Any]], definition: Mapping[str, Any]) -> tuple[dict[str, Any], int, str | None]:
    eligible = [row for row in rows if _has_complete_path_samples(row)]
    if not eligible:
        return {"grid_results": []}, 0, "blocked_missing_complete_path_samples"
    grid_results = []
    for minutes in definition.get("minutes") or (15, 30, 45, 60, 90, 120):
        simulated = [_simulate_timebox(row, int(minutes)) for row in eligible]
        grid_results.append({"minutes": int(minutes), "metrics": calculate_metrics([row for row in simulated if row])})
    return {"grid_results": grid_results}, len(eligible), None


def _run_mfe_giveback(rows: Sequence[Mapping[str, Any]], definition: Mapping[str, Any]) -> tuple[dict[str, Any], int, str | None]:
    eligible = [row for row in rows if _has_complete_path_samples(row) and _number(row.get("mfe_points")) is not None]
    if not eligible:
        return {"giveback_results": []}, 0, "blocked_missing_mfe_path_samples"
    results = []
    for pct in definition.get("giveback_percentages") or (0.5, 0.7):
        simulated = [_simulate_giveback(row, float(pct)) for row in eligible]
        results.append({"giveback_percentage": float(pct), "metrics": calculate_metrics([row for row in simulated if row])})
    return {"giveback_results": results}, len(eligible), None


def _simulate_timebox(row: Mapping[str, Any], minutes: int) -> dict[str, Any] | None:
    entry_time = _parse_ts(row.get("entry_time") or row.get("entry_timestamp"))
    target = entry_time + timedelta(minutes=minutes) if entry_time else None
    bar = _first_bar_at_or_after(row.get("entry_to_exit_path") or [], target)
    if not bar:
        return None
    close = _number(bar.get("close"))
    entry_price = _number(row.get("entry_price"))
    if close is None or entry_price is None:
        return None
    simulated = dict(row)
    points = _points(side=str(row.get("side") or ""), entry_price=entry_price, exit_price=close)
    simulated["realized_pnl_proxy"] = _pnl_from_points(points, row)
    return simulated


def _simulate_giveback(row: Mapping[str, Any], giveback: float) -> dict[str, Any] | None:
    # REF1 keeps giveback deterministic and deliberately simple: use actual MFE
    # and actual realized P&L proxy to create a conservative simulated proxy when
    # complete path samples exist. Future phases can replace this with a
    # bar-by-bar excursion model once canonical path samples are complete.
    mfe = _number(row.get("mfe_points"))
    if mfe is None:
        return None
    simulated = dict(row)
    current = _number(row.get("realized_points"))
    points = max((current if current is not None else 0.0), mfe * (1.0 - giveback))
    simulated["realized_pnl_proxy"] = _pnl_from_points(points, row)
    return simulated


def _apply_filter_definition(rows: Sequence[Mapping[str, Any]], definition: Mapping[str, Any]) -> tuple[list[Mapping[str, Any]], dict[str, int]]:
    filters = definition.get("filters") if isinstance(definition, Mapping) else []
    if not filters:
        return list(rows), {}
    kept: list[Mapping[str, Any]] = []
    exclusions: dict[str, int] = {}
    for row in rows:
        include = True
        for flt in filters:
            field = flt.get("field")
            if field not in row or row.get(field) in (None, "", "UNKNOWN", "UNAVAILABLE"):
                include = False
                reason = f"missing_{field}"
                exclusions[reason] = exclusions.get(reason, 0) + 1
                break
            if not _matches(row.get(field), flt.get("op"), flt.get("value")):
                include = False
                exclusions["filter_excluded"] = exclusions.get("filter_excluded", 0) + 1
                break
        if include:
            kept.append(row)
    return kept, exclusions


def _matches(value: Any, op: Any, expected: Any) -> bool:
    if op == "eq":
        return str(value) == str(expected)
    if op == "ne":
        return str(value) != str(expected)
    left = _number(value)
    right = _number(expected)
    if left is None or right is None:
        return False
    if op == "gte":
        return left >= right
    if op == "gt":
        return left > right
    if op == "lte":
        return left <= right
    if op == "lt":
        return left < right
    return False


def default_metric_names() -> tuple[str, ...]:
    return (
        "trade_count",
        "total_pnl_proxy",
        "average_pnl_proxy",
        "median_pnl_proxy",
        "win_rate",
        "profit_factor_proxy",
        "average_winner",
        "average_loser",
        "payoff_ratio",
        "max_win",
        "max_loss",
        "sample_class",
        "data_coverage",
    )


def experiment_schema() -> dict[str, Any]:
    return {
        "title": "CanonicalResearchExperiment",
        "type": "object",
        "required": ["experiment_id", "experiment_type", "hypothesis", "population_definition", "variant_definition", "guardrails"],
        "properties": {
            "experiment_type": {"enum": ["EXIT_POLICY", "ENTRY_FILTER", "REGIME_FILTER", "SESSION_FILTER", "LANE_COMPARISON", "CUSTOM"]},
            "status": {"enum": ["DRAFT", "RUNNABLE", "COMPLETE", "COMPLETE_WITH_WARNINGS", "BLOCKED", "ARCHIVED"]},
            "guardrails": {"type": "object"},
        },
    }


def render_contract_markdown() -> str:
    return """# REF1 Offline Research Experiment Contract

REF1 evaluates deterministic offline research experiments over canonical trade
artifacts. Results are research evidence only.

Guardrails:

- diagnostic_only=true
- production_recommendation=false
- trading_gate=false
- no broker actions
- no runtime or Managed Exit restart
- no strategy, entry, or exit changes
- no production recommendations
"""


def render_summary_markdown(summary: Mapping[str, Any], results: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "# REF1 Offline Research Experiment Summary",
        "",
        f"- Generated at: `{summary.get('generated_at')}`",
        f"- Experiments: `{summary.get('experiment_count')}`",
        f"- Results: `{summary.get('result_count')}`",
        f"- Population rows: `{summary.get('population_rows')}`",
        f"- Status counts: `{summary.get('status_counts')}`",
        "",
        "## Results",
        "",
    ]
    for result in results:
        lines.append(
            f"- `{result.get('experiment_id')}`: status `{result.get('status')}`, population `{result.get('population_count')}`, "
            f"variant `{result.get('variant_count')}`, confidence `{result.get('confidence_label')}`"
        )
    lines.append("\nDiagnostic research evidence only. No production actions or gates.")
    return "\n".join(lines) + "\n"


def render_data_quality_markdown(summary: Mapping[str, Any]) -> str:
    dq = summary.get("data_quality") or {}
    lines = ["# REF1 Data Quality Report", ""]
    for key, value in dq.items():
        lines.append(f"- {key}: `{value}`")
    return "\n".join(lines) + "\n"


def render_blocked_markdown(results: Sequence[Mapping[str, Any]]) -> str:
    lines = ["# REF1 Blocked Experiments", ""]
    blocked = [row for row in results if row.get("status") == "BLOCKED"]
    if not blocked:
        lines.append("No blocked sample experiments.")
    for row in blocked:
        lines.append(f"- `{row.get('experiment_id')}`: `{row.get('data_quality_flags')}`")
    return "\n".join(lines) + "\n"


def _path_available(row: Mapping[str, Any]) -> bool:
    return row.get("path_available") is True or (row.get("path") or {}).get("path_coverage_status") not in (None, "MISSING_SOURCE", "UNAVAILABLE")


def _has_complete_path_samples(row: Mapping[str, Any]) -> bool:
    samples = row.get("entry_to_exit_path")
    return (row.get("path_complete") is True or (row.get("path") or {}).get("path_complete_entry_to_exit") is True) and isinstance(samples, list) and bool(samples)


def _first_bar_at_or_after(samples: Sequence[Mapping[str, Any]], target: datetime | None) -> Mapping[str, Any] | None:
    if target is None:
        return None
    for sample in samples:
        ts = _parse_ts(sample.get("bar_end") or sample.get("timestamp"))
        if ts and ts >= target:
            return sample
    return samples[-1] if samples else None


def _points(*, side: str, entry_price: float, exit_price: float) -> float:
    return round(exit_price - entry_price, 10) if side.upper() == "LONG" else round(entry_price - exit_price, 10)


def _pnl_from_points(points: float, row: Mapping[str, Any]) -> float:
    actual_points = _number(row.get("realized_points"))
    actual_pnl = _number(row.get("realized_pnl_proxy"))
    if actual_points not in (None, 0) and actual_pnl is not None:
        return round(points * (actual_pnl / actual_points), 6)
    return round(points, 6)


def _delta_metrics(baseline: Mapping[str, Any], variant: Mapping[str, Any]) -> dict[str, Any]:
    keys = ("trade_count", "total_pnl_proxy", "average_pnl_proxy", "median_pnl_proxy", "win_rate", "profit_factor_proxy")
    return {key: _delta(_number(variant.get(key)), _number(baseline.get(key))) for key in keys}


def _delta(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return _round(left - right)


def _profit_factor(winners: Sequence[float], losers: Sequence[float]) -> float | None:
    if not winners or not losers:
        return None
    return _round(sum(winners) / abs(sum(losers)))


def _sample_class(count: int) -> str:
    if count >= 100:
        return "RESEARCH_GRADE"
    if count >= 40:
        return "DEVELOPING"
    if count >= 15:
        return "PRELIMINARY"
    return "EXPLORATORY" if count > 0 else "INSUFFICIENT_DATA"


def _confidence_label(count: int) -> str:
    return _sample_class(count)


def _rate(part: int, whole: int) -> float | None:
    return round(part / whole, 6) if whole else None


def _round(value: float | None) -> float | None:
    return None if value is None else round(float(value), 6)


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_now(value: datetime | str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _counts(values: Sequence[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value or "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _merge_counts(*items: Mapping[str, int]) -> dict[str, int]:
    merged: dict[str, int] = {}
    for item in items:
        for key, value in item.items():
            merged[key] = merged.get(key, 0) + int(value)
    return merged


def _most_common(values: Sequence[Any]) -> str | None:
    counts = _counts([value for value in values if value])
    return next(iter(counts), None) if counts else None


def _best_hypothesis_lane(hypotheses: Sequence[Mapping[str, Any]]) -> str | None:
    if not hypotheses:
        return None
    return str(sorted(hypotheses, key=lambda row: (-float(row.get("separation_score") or 0.0), str(row.get("lane_id") or "")))[0].get("lane_id") or "")


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _fingerprint_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key not in {"created_at", "updated_at", "generated_at", "deterministic_fingerprint"}}


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
