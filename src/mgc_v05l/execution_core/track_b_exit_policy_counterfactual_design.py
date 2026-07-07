"""RA4 exit policy counterfactual design for completed canonical trades.

This module is research/diagnostic only. It reads completed CTOL/CTOE/RA3
artifacts and reports which exit counterfactuals are testable with current
evidence. It does not touch broker, runtime, strategy, Managed Exit, or gate
paths.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import median
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_trade_decision_attribution import (
    ATTRIBUTION_JSONL,
    DEFAULT_OUTPUT_DIR as DEFAULT_RA3_OUTPUT_DIR,
)
from mgc_v05l.execution_core.track_b_trade_outcome_enrichment import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOE_OUTPUT_DIR,
    ENRICHMENT_JSONL,
)
from mgc_v05l.execution_core.track_b_trade_outcome_layer import (
    DEFAULT_OUTPUT_DIR as DEFAULT_CTOL_OUTPUT_DIR,
    OUTCOMES_JSONL,
)


DEFAULT_OUTPUT_ROOT = Path("outputs") / "track_b_execution_core"
DEFAULT_OUTCOMES_PATH = DEFAULT_CTOL_OUTPUT_DIR / OUTCOMES_JSONL
DEFAULT_ENRICHMENTS_PATH = DEFAULT_CTOE_OUTPUT_DIR / ENRICHMENT_JSONL
DEFAULT_ATTRIBUTIONS_PATH = DEFAULT_RA3_OUTPUT_DIR / ATTRIBUTION_JSONL
DEFAULT_OUTPUT_DIR = DEFAULT_OUTPUT_ROOT / "research_analytics" / "ra4_exit_policy_counterfactual"

OBSERVATIONS_JSONL = "ra4_exit_counterfactual_observations.jsonl"
SUMMARY_JSON = "ra4_exit_policy_counterfactual_design.json"
SUMMARY_MD = "ra4_exit_policy_counterfactual_design.md"
TIMEBOX_MD = "ra4_timebox_counterfactual_report.md"
LANE_MD = "ra4_adaptive_exit_lane_priorities.md"
SIMULATION_MD = "ra4_exit_policy_simulation_plan.md"
MISSING_DATA_MD = "ra4_missing_exit_counterfactual_data.md"

SCHEMA_VERSION = "ra4_exit_policy_counterfactual_observation_v1"
SUMMARY_SCHEMA_VERSION = "ra4_exit_policy_counterfactual_summary_v1"

SHORTER_TIMEBOX_MINUTES = (15, 30, 45, 60)
LONGER_TIMEBOX_MINUTES = (90, 120, 180)


@dataclass(frozen=True)
class ExitCounterfactualResult:
    observations: list[dict[str, Any]]
    summary: dict[str, Any]
    observations_path: Path
    summary_json_path: Path
    summary_markdown_path: Path
    timebox_report_path: Path
    lane_priority_path: Path
    simulation_plan_path: Path
    missing_data_path: Path


def run_exit_policy_counterfactual_design(
    *,
    outcomes_path: Path = DEFAULT_OUTCOMES_PATH,
    enrichments_path: Path = DEFAULT_ENRICHMENTS_PATH,
    attributions_path: Path = DEFAULT_ATTRIBUTIONS_PATH,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    now: datetime | str | None = None,
) -> ExitCounterfactualResult:
    generated_at = _coerce_now(now)
    outcomes = _read_jsonl(outcomes_path)
    enrichments = _read_jsonl(enrichments_path)
    attributions = _read_jsonl(attributions_path)
    observations = build_exit_counterfactual_observations(
        outcomes,
        enrichments=enrichments,
        attributions=attributions,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "ctoe": enrichments_path,
            "ra3_attribution": attributions_path,
        },
    )
    summary = build_exit_counterfactual_summary(
        observations,
        generated_at=generated_at,
        source_paths={
            "ctol": outcomes_path,
            "ctoe": enrichments_path,
            "ra3_attribution": attributions_path,
        },
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    observations_path = output_dir / OBSERVATIONS_JSONL
    summary_json_path = output_dir / SUMMARY_JSON
    summary_markdown_path = output_dir / SUMMARY_MD
    timebox_report_path = output_dir / TIMEBOX_MD
    lane_priority_path = output_dir / LANE_MD
    simulation_plan_path = output_dir / SIMULATION_MD
    missing_data_path = output_dir / MISSING_DATA_MD
    _write_jsonl(observations_path, observations)
    _write_json(summary_json_path, summary)
    summary_markdown_path.write_text(render_summary_markdown(summary), encoding="utf-8")
    timebox_report_path.write_text(render_timebox_markdown(summary), encoding="utf-8")
    lane_priority_path.write_text(render_lane_priority_markdown(summary), encoding="utf-8")
    simulation_plan_path.write_text(render_simulation_plan_markdown(summary), encoding="utf-8")
    missing_data_path.write_text(render_missing_data_markdown(summary), encoding="utf-8")
    return ExitCounterfactualResult(
        observations=observations,
        summary=summary,
        observations_path=observations_path,
        summary_json_path=summary_json_path,
        summary_markdown_path=summary_markdown_path,
        timebox_report_path=timebox_report_path,
        lane_priority_path=lane_priority_path,
        simulation_plan_path=simulation_plan_path,
        missing_data_path=missing_data_path,
    )


def build_exit_counterfactual_observations(
    outcomes: Sequence[Mapping[str, Any]],
    *,
    enrichments: Sequence[Mapping[str, Any]] = (),
    attributions: Sequence[Mapping[str, Any]] = (),
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> list[dict[str, Any]]:
    enrichment_index = {str(row.get("trade_outcome_id")): row for row in enrichments if row.get("trade_outcome_id")}
    attribution_index = {str(row.get("trade_outcome_id")): row for row in attributions if row.get("trade_outcome_id")}
    rows: list[dict[str, Any]] = []
    for outcome in outcomes:
        trade_id = str(outcome.get("trade_outcome_id") or "")
        enrichment = enrichment_index.get(trade_id, {})
        attribution = attribution_index.get(trade_id, {})
        exit_attr = attribution.get("exit") if isinstance(attribution.get("exit"), Mapping) else {}
        pnl = _float_or_none(outcome.get("realized_pnl_proxy"))
        mfe = _float_or_none(outcome.get("mfe_points"))
        mae = _float_or_none(outcome.get("mae_points"))
        hold_seconds = _float_or_none(outcome.get("hold_seconds"))
        timed_exit = _is_time_exit(outcome, exit_attr)
        row = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at.isoformat(),
            "observation_id": _stable_id("exit_counterfactual", trade_id, outcome.get("entry_time"), outcome.get("exit_time")),
            "trade_outcome_id": trade_id,
            "lane_id": outcome.get("lane_id"),
            "strategy_id": outcome.get("strategy_id"),
            "instrument": outcome.get("instrument"),
            "contract": outcome.get("contract"),
            "side": outcome.get("side"),
            "session": outcome.get("session_at_entry"),
            "entry_time": outcome.get("entry_time"),
            "exit_time": outcome.get("exit_time"),
            "hold_seconds": hold_seconds,
            "hold_minutes": _round(hold_seconds / 60.0) if hold_seconds is not None else None,
            "realized_pnl_proxy": pnl,
            "realized_points": _float_or_none(outcome.get("realized_points")),
            "mfe_points": mfe,
            "mae_points": mae,
            "exit_policy": outcome.get("exit_policy"),
            "exit_reason": outcome.get("exit_reason"),
            "canonical_exit_reason": exit_attr.get("canonical_exit_reason"),
            "managed_exit_involved": exit_attr.get("managed_exit_involved"),
            "required_completed_bars": _int_or_none(exit_attr.get("required_completed_bars")),
            "elapsed_completed_bars": _int_or_none(exit_attr.get("elapsed_completed_bars")),
            "timebox_exit": timed_exit,
            "profitable_at_exit": pnl is not None and pnl > 0,
            "profitable_before_timeout": _profitable_before_timeout(mfe),
            "losing_early_never_recovered": _losing_early_never_recovered(pnl=pnl, mfe=mfe, mae=mae),
            "counterfactual_testability": _testability(mfe=mfe, mae=mae),
            "shorter_timebox_counterfactual": _timebox_counterfactual("SHORTER", hold_seconds=hold_seconds, timed_exit=timed_exit, mfe=mfe, mae=mae),
            "longer_timebox_counterfactual": _timebox_counterfactual("LONGER", hold_seconds=hold_seconds, timed_exit=timed_exit, mfe=mfe, mae=mae),
            "context": _context_snapshot(enrichment, attribution),
            "missing_data": _missing_data(outcome, enrichment=enrichment),
            "source_refs": {
                "ctol_trade_outcome_id": trade_id,
                "ctoe_present": bool(enrichment),
                "ra3_attribution_present": bool(attribution),
                "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
            },
            "diagnostic_only": True,
            "production_recommendation": False,
            "trading_gate": False,
        }
        row["deterministic_fingerprint"] = _fingerprint({k: v for k, v in row.items() if k not in {"generated_at", "deterministic_fingerprint"}})
        rows.append(row)
    rows.sort(key=lambda row: (str(row.get("exit_time") or ""), str(row.get("trade_outcome_id") or "")))
    return rows


def build_exit_counterfactual_summary(
    observations: Sequence[Mapping[str, Any]],
    *,
    generated_at: datetime,
    source_paths: Mapping[str, Path | str] | None = None,
) -> dict[str, Any]:
    total = len(observations)
    timed = [row for row in observations if row.get("timebox_exit")]
    profitable_timeout = [row for row in timed if row.get("profitable_at_exit")]
    known_profitable_before = [row for row in timed if row.get("profitable_before_timeout") is True]
    losing_never_recovered = [row for row in timed if row.get("losing_early_never_recovered") is True]
    lane_priorities = _lane_priorities(timed)
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "diagnostic_only": True,
        "production_recommendation": False,
        "trading_gate": False,
        "source_paths": {key: str(value) for key, value in (source_paths or {}).items()},
        "overall": {
            "completed_trades": total,
            "timebox_exit_trades": len(timed),
            "timebox_exit_rate": _rate(len(timed), total),
            "profitable_at_timeout_count": len(profitable_timeout),
            "profitable_at_timeout_rate": _rate(len(profitable_timeout), len(timed)),
            "profitable_before_timeout_known_count": len(known_profitable_before),
            "losing_early_never_recovered_known_count": len(losing_never_recovered),
            "mfe_coverage": _rate(sum(1 for row in observations if row.get("mfe_points") is not None), total),
            "mae_coverage": _rate(sum(1 for row in observations if row.get("mae_points") is not None), total),
            "vwap_entry_coverage": _rate(sum(1 for row in observations if row.get("context", {}).get("vwap_relation_at_entry") not in (None, "UNKNOWN")), total),
            "avwap_entry_coverage": _rate(sum(1 for row in observations if row.get("context", {}).get("avwap_relation_at_entry") not in (None, "UNKNOWN", "unavailable")), total),
            "gre_valid_coverage": _rate(sum(1 for row in observations if row.get("context", {}).get("gre_validity") == "VALID"), total),
            "crfd_valid_coverage": _rate(sum(1 for row in observations if row.get("context", {}).get("crfd_validity") == "VALID"), total),
        },
        "timebox_counterfactuals": {
            "shorter_timebox_minutes": list(SHORTER_TIMEBOX_MINUTES),
            "longer_timebox_minutes": list(LONGER_TIMEBOX_MINUTES),
            "shorter_timebox_testable_now": 0,
            "longer_timebox_testable_now": 0,
            "primary_blocker": "missing_timestamped_in_trade_excursion_and_post_exit_forward_path",
            "profitable_at_actual_timeout_samples": _trade_samples(profitable_timeout),
            "profitable_before_timeout_known_samples": _trade_samples(known_profitable_before),
            "losing_early_never_recovered_known_samples": _trade_samples(losing_never_recovered),
        },
        "distributions": {
            "exit_policy": _counts(row.get("exit_policy") for row in observations),
            "canonical_exit_reason": _counts(row.get("canonical_exit_reason") for row in observations),
            "counterfactual_testability": _counts(row.get("counterfactual_testability") for row in observations),
            "missing_data": _counts(reason for row in observations for reason in row.get("missing_data", [])),
            "instrument": _counts(row.get("instrument") for row in observations),
            "session": _counts(row.get("session") for row in observations),
        },
        "adaptive_exit_lane_priorities": lane_priorities,
        "exit_policies_to_simulate_first": _simulation_plan(lane_priorities),
        "data_missing_to_test_vwap_atr_structure_exits": _data_missing_to_test_structure_exits(observations),
    }


def _lane_priorities(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[Mapping[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("lane_id") or "UNKNOWN"), []).append(row)
    priorities: list[dict[str, Any]] = []
    for lane, lane_rows in groups.items():
        pnls = [_float_or_none(row.get("realized_pnl_proxy")) for row in lane_rows]
        known_pnls = [pnl for pnl in pnls if pnl is not None]
        loss_count = sum(1 for pnl in known_pnls if pnl < 0)
        profit_count = sum(1 for pnl in known_pnls if pnl > 0)
        total_pnl = round(sum(known_pnls), 6) if known_pnls else None
        avg_pnl = _round(sum(known_pnls) / len(known_pnls)) if known_pnls else None
        hold_values = [_float_or_none(row.get("hold_seconds")) for row in lane_rows]
        hold_values = [value for value in hold_values if value is not None]
        missing_mfe = sum(1 for row in lane_rows if row.get("mfe_points") is None)
        missing_mae = sum(1 for row in lane_rows if row.get("mae_points") is None)
        profitable_timeout = sum(1 for row in lane_rows if row.get("profitable_at_exit"))
        priority_score = (
            len(lane_rows) * 2
            + loss_count * 3
            + (1 if total_pnl is not None and total_pnl < 0 else 0) * 10
            + profitable_timeout
            + missing_mfe
            + missing_mae
        )
        priorities.append(
            {
                "lane_id": lane,
                "trade_count": len(lane_rows),
                "instrument": _mode(row.get("instrument") for row in lane_rows),
                "session": _mode(row.get("session") for row in lane_rows),
                "side": _mode(row.get("side") for row in lane_rows),
                "total_pnl_proxy": total_pnl,
                "average_pnl_proxy": avg_pnl,
                "win_rate": _rate(profit_count, len(known_pnls)),
                "loss_count": loss_count,
                "profitable_at_timeout_count": profitable_timeout,
                "average_hold_minutes": _round((sum(hold_values) / len(hold_values)) / 60.0) if hold_values else None,
                "median_hold_minutes": _round(median(hold_values) / 60.0) if hold_values else None,
                "mfe_coverage": _rate(len(lane_rows) - missing_mfe, len(lane_rows)),
                "mae_coverage": _rate(len(lane_rows) - missing_mae, len(lane_rows)),
                "diagnostic_priority_score": priority_score,
                "research_label": _lane_label(len(lane_rows), total_pnl, loss_count, profitable_timeout),
            }
        )
    priorities.sort(key=lambda row: (row["diagnostic_priority_score"], row["trade_count"]), reverse=True)
    return priorities[:50]


def _simulation_plan(lane_priorities: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "simulation_policy": "TIMEBOX_GRID_15_30_45_60_90_120_MINUTES",
            "priority": 1,
            "why_first": "Most completed exits are time-based and can be replayed once timestamped in-trade path data is available.",
            "required_data": ["timestamped_in_trade_price_path", "entry_time", "exit_time", "side", "contract_point_value"],
            "diagnostic_only": True,
        },
        {
            "simulation_policy": "MFE_GIVEBACK_TRAILING_EXIT",
            "priority": 2,
            "why_first": "Directly targets trades that were profitable before a timeout but gave back before exit.",
            "required_data": ["mfe_points", "time_to_mfe_seconds", "timestamped_in_trade_price_path"],
            "diagnostic_only": True,
        },
        {
            "simulation_policy": "EARLY_ADVERSE_EXCURSION_STOP_OR_REVIEW",
            "priority": 3,
            "why_first": "Directly targets trades that went adverse early and did not recover.",
            "required_data": ["mae_points", "time_to_mae_seconds", "timestamped_in_trade_price_path"],
            "diagnostic_only": True,
        },
        {
            "simulation_policy": "VWAP_AVWAP_STRUCTURE_EXIT",
            "priority": 4,
            "why_first": "Requires stronger VWAP/AVWAP state at entry and exit, so it should follow path enrichment.",
            "required_data": ["vwap_relation_at_entry", "vwap_relation_at_exit", "avwap_relation_at_entry", "avwap_relation_at_exit", "distance_to_vwap", "distance_to_avwap"],
            "diagnostic_only": True,
        },
        {
            "simulation_policy": "ATR_VOLATILITY_ADAPTIVE_TIMEBOX",
            "priority": 5,
            "why_first": "Needs ATR/volatility context not yet canonical in CTOL/CTOE.",
            "required_data": ["atr_at_entry", "realized_volatility", "timestamped_in_trade_price_path"],
            "diagnostic_only": True,
        },
    ]


def _data_missing_to_test_structure_exits(observations: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    total = len(observations)
    missing_counts = {
        "timestamped_in_trade_excursion_curve": total,
        "post_exit_forward_path": total,
        "time_to_mfe_seconds": total,
        "time_to_mae_seconds": total,
        "atr_at_entry": total,
        "vwap_distance_at_entry": total,
        "vwap_relation_at_exit": total,
        "avwap_relation_at_exit": total,
        "structure_swing_level_at_entry": total,
    }
    missing_counts["mfe_points"] = sum(1 for row in observations if row.get("mfe_points") is None)
    missing_counts["mae_points"] = sum(1 for row in observations if row.get("mae_points") is None)
    missing_counts["vwap_relation_at_entry"] = sum(1 for row in observations if row.get("context", {}).get("vwap_relation_at_entry") in (None, "UNKNOWN"))
    missing_counts["avwap_relation_at_entry"] = sum(1 for row in observations if row.get("context", {}).get("avwap_relation_at_entry") in (None, "UNKNOWN", "unavailable"))
    return [
        {"field": field, "missing_count": count, "missing_rate": _rate(count, total)}
        for field, count in sorted(missing_counts.items(), key=lambda item: (-item[1], item[0]))
    ]


def render_summary_markdown(summary: Mapping[str, Any]) -> str:
    overall = summary.get("overall", {})
    return "\n".join(
        [
            "# RA4 Exit Policy Counterfactual Design",
            "",
            "Research/diagnostic only. This report designs and bounds exit-policy counterfactuals from completed trades; it does not change live behavior.",
            "",
            f"- Completed trades: `{overall.get('completed_trades')}`",
            f"- Timebox exits: `{overall.get('timebox_exit_trades')}`",
            f"- Profitable at actual timeout: `{overall.get('profitable_at_timeout_count')}`",
            f"- Known profitable before timeout from MFE: `{overall.get('profitable_before_timeout_known_count')}`",
            f"- Known losing early and never recovered: `{overall.get('losing_early_never_recovered_known_count')}`",
            f"- MFE coverage: `{overall.get('mfe_coverage')}`",
            f"- MAE coverage: `{overall.get('mae_coverage')}`",
            "",
            "## Bottom Line",
            "",
            "Shorter and longer timebox exits cannot be truthfully simulated for most trades until timestamped excursion curves and post-exit forward paths are populated. Current data supports lane prioritization and missing-data design, not production exit conclusions.",
        ]
    ) + "\n"


def render_timebox_markdown(summary: Mapping[str, Any]) -> str:
    counter = summary.get("timebox_counterfactuals", {})
    lines = [
        "# RA4 Timebox Counterfactual Report",
        "",
        f"- Shorter timebox grid: `{counter.get('shorter_timebox_minutes')}` minutes",
        f"- Longer timebox grid: `{counter.get('longer_timebox_minutes')}` minutes",
        f"- Shorter timebox testable now: `{counter.get('shorter_timebox_testable_now')}`",
        f"- Longer timebox testable now: `{counter.get('longer_timebox_testable_now')}`",
        f"- Primary blocker: `{counter.get('primary_blocker')}`",
        "",
        "## Profitable At Actual Timeout Samples",
        "",
        _sample_table(counter.get("profitable_at_actual_timeout_samples", [])),
    ]
    return "\n".join(lines) + "\n"


def render_lane_priority_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# RA4 Adaptive Exit Lane Priorities",
        "",
        "These are diagnostic research priorities, not promotion, retirement, or production recommendations.",
        "",
        "|lane|trades|instrument|session|total_pnl_proxy|win_rate|avg_hold_min|label|",
        "|---|---:|---|---|---:|---:|---:|---|",
    ]
    for row in summary.get("adaptive_exit_lane_priorities", [])[:30]:
        lines.append(
            f"|{row.get('lane_id')}|{row.get('trade_count')}|{row.get('instrument')}|{row.get('session')}|{row.get('total_pnl_proxy')}|{row.get('win_rate')}|{row.get('average_hold_minutes')}|{row.get('research_label')}|"
        )
    return "\n".join(lines) + "\n"


def render_simulation_plan_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# RA4 Exit Policy Simulation Plan",
        "",
        "Suggested simulation order is based on available evidence and missing-data dependencies. It is diagnostic only.",
        "",
        "|priority|policy|why first|required data|",
        "|---:|---|---|---|",
    ]
    for row in summary.get("exit_policies_to_simulate_first", []):
        lines.append(
            f"|{row.get('priority')}|{row.get('simulation_policy')}|{row.get('why_first')}|{', '.join(row.get('required_data', []))}|"
        )
    return "\n".join(lines) + "\n"


def render_missing_data_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "# RA4 Missing Data To Test Smarter Exits",
        "",
        "|field|missing_count|missing_rate|",
        "|---|---:|---:|",
    ]
    for row in summary.get("data_missing_to_test_vwap_atr_structure_exits", []):
        lines.append(f"|{row.get('field')}|{row.get('missing_count')}|{row.get('missing_rate')}|")
    return "\n".join(lines) + "\n"


def _timebox_counterfactual(kind: str, *, hold_seconds: float | None, timed_exit: bool, mfe: float | None, mae: float | None) -> dict[str, Any]:
    horizons = SHORTER_TIMEBOX_MINUTES if kind == "SHORTER" else LONGER_TIMEBOX_MINUTES
    if not timed_exit:
        status = "NOT_APPLICABLE_NON_TIMEBOX_EXIT"
    elif kind == "SHORTER":
        status = "NEEDS_INTRATRADE_EXCURSION_CURVE"
    else:
        status = "NEEDS_POST_EXIT_FORWARD_PATH"
    return {
        "status": status,
        "candidate_minutes": list(horizons),
        "actual_hold_minutes": _round(hold_seconds / 60.0) if hold_seconds is not None else None,
        "mfe_mae_bounds_available": mfe is not None and mae is not None,
        "explanation": "Path-dependent simulation is not available without timestamped trade excursion data.",
    }


def _testability(*, mfe: float | None, mae: float | None) -> str:
    if mfe is None and mae is None:
        return "NOT_TESTABLE_MISSING_MFE_MAE_AND_PATH"
    if mfe is not None and mae is not None:
        return "BOUNDS_ONLY_MFE_MAE_PRESENT_PATH_MISSING"
    return "PARTIAL_BOUNDS_ONLY_PATH_MISSING"


def _profitable_before_timeout(mfe: float | None) -> bool | None:
    if mfe is None:
        return None
    return mfe > 0


def _losing_early_never_recovered(*, pnl: float | None, mfe: float | None, mae: float | None) -> bool | None:
    if pnl is None or mfe is None or mae is None:
        return None
    return pnl <= 0 and mae < 0 and mfe <= 0


def _context_snapshot(enrichment: Mapping[str, Any], attribution: Mapping[str, Any]) -> dict[str, Any]:
    entry = attribution.get("entry") if isinstance(attribution.get("entry"), Mapping) else {}
    return {
        "vix_regime": enrichment.get("vix_regime") or _nested(entry, "vix_context", "fields", "vix_regime"),
        "vix_percentile": enrichment.get("vix_percentile") or _nested(entry, "vix_context", "fields", "vix_percentile"),
        "gre_label": enrichment.get("gre_label") or _nested(entry, "gre_context", "fields", "gre_label"),
        "gre_validity": enrichment.get("gre_validity_classification") or _nested(entry, "gre_context", "fields", "validity"),
        "crfd_validity": enrichment.get("crfd_validity_classification") or _nested(entry, "crfd_context", "fields", "validity"),
        "vwap_relation_at_entry": enrichment.get("vwap_relation_at_entry") or enrichment.get("vwap_relation") or _nested(entry, "crfd_context", "fields", "vwap_relation"),
        "avwap_relation_at_entry": enrichment.get("avwap_relation_at_entry") or enrichment.get("avwap_relation") or _nested(entry, "crfd_context", "fields", "avwap_relation"),
    }


def _missing_data(outcome: Mapping[str, Any], *, enrichment: Mapping[str, Any]) -> list[str]:
    missing: list[str] = []
    if outcome.get("mfe_points") is None:
        missing.append("missing_mfe_points")
    if outcome.get("mae_points") is None:
        missing.append("missing_mae_points")
    missing.extend(
        [
            "missing_timestamped_in_trade_excursion_curve",
            "missing_post_exit_forward_path",
            "missing_time_to_mfe_seconds",
            "missing_time_to_mae_seconds",
            "missing_atr_at_entry",
            "missing_vwap_distance_at_entry",
            "missing_vwap_relation_at_exit",
            "missing_avwap_relation_at_exit",
            "missing_structure_context",
        ]
    )
    if not enrichment:
        missing.append("missing_ctoe_join")
    if enrichment and enrichment.get("gre_validity_classification") != "VALID":
        missing.append("missing_valid_gre_context")
    if enrichment and enrichment.get("crfd_validity_classification") != "VALID":
        missing.append("missing_valid_crfd_context")
    return sorted(set(missing))


def _is_time_exit(outcome: Mapping[str, Any], exit_attr: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(value or "")
        for value in (
            outcome.get("exit_policy"),
            outcome.get("exit_reason"),
            exit_attr.get("canonical_exit_reason"),
            exit_attr.get("close_reason"),
        )
    ).upper()
    return "TIME" in text or "TIMEBOX" in text


def _lane_label(count: int, total_pnl: float | None, loss_count: int, profitable_timeout: int) -> str:
    if count < 5:
        return "TOO_THIN_FOR_EXIT_COUNTERFACTUAL"
    if total_pnl is not None and total_pnl < 0 and loss_count >= 3:
        return "ADAPTIVE_EXIT_RESEARCH_PRIORITY"
    if profitable_timeout >= 3:
        return "MFE_GIVEBACK_RESEARCH_PRIORITY"
    return "BASELINE_TIMEBOX_REPLAY_PRIORITY"


def _trade_samples(rows: Sequence[Mapping[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    samples = []
    for row in rows[:limit]:
        samples.append(
            {
                "trade_outcome_id": row.get("trade_outcome_id"),
                "lane_id": row.get("lane_id"),
                "instrument": row.get("instrument"),
                "session": row.get("session"),
                "side": row.get("side"),
                "exit_time": row.get("exit_time"),
                "hold_minutes": row.get("hold_minutes"),
                "realized_pnl_proxy": row.get("realized_pnl_proxy"),
                "mfe_points": row.get("mfe_points"),
                "mae_points": row.get("mae_points"),
            }
        )
    return samples


def _sample_table(samples: Sequence[Mapping[str, Any]]) -> str:
    if not samples:
        return "No samples available."
    lines = ["|trade|lane|instrument|hold_min|pnl_proxy|", "|---|---|---|---:|---:|"]
    for row in samples:
        lines.append(f"|{row.get('trade_outcome_id')}|{row.get('lane_id')}|{row.get('instrument')}|{row.get('hold_minutes')}|{row.get('realized_pnl_proxy')}|")
    return "\n".join(lines)


def _nested(mapping: Mapping[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
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
    text = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, "", "UNKNOWN"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 6)


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 6)


def _counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value if value not in (None, "") else "UNKNOWN")
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _mode(values: Any) -> str | None:
    counts = _counts(value for value in values if value not in (None, ""))
    if not counts:
        return None
    return next(iter(counts))


def _stable_id(prefix: str, *parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _fingerprint(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
