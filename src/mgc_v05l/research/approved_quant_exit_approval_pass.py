"""Exit-only approval pass for approved quant baseline lanes."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..app.approved_quant_lanes.runtime_boundary import build_approved_quant_symbol_store
from ..app.approved_quant_lanes.specs import ApprovedQuantLaneSpec, approved_quant_lane_specs
from .approved_quant_exit_research import (
    ExitVariantSpec,
    _build_exit_variants,
    _collect_entry_candidates,
    _evaluate_exit_variant,
)
from .quant_futures import StrategyResearchArtifacts, _clip, _expectancy


BREAKOUT_CANDIDATE_EXIT_ID = "phase2c.breakout.metals_only.us_unknown.baseline.time_stop_only.h24"
FAILED_CANDIDATE_EXIT_ID = "phase2c.failed.core4_plus_qc.no_us.baseline.structure_invalidation.r045"


def run_approved_quant_exit_approval_pass(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "approved_quant_exit_approval_pass").resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    specs = approved_quant_lane_specs()
    symbols = tuple(sorted({symbol for spec in specs for symbol in spec.symbols}))
    symbol_store = build_approved_quant_symbol_store(
        database_path=resolved_database_path,
        execution_timeframe=execution_timeframe,
        symbols=symbols,
    )

    lane_reports = []
    for spec in specs:
        lane_reports.append(_build_lane_exit_approval_report(spec=spec, symbol_store=symbol_store))

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_of_truth_path": str(Path(specs[0].approval_source).resolve()),
        "execution_timeframe": execution_timeframe,
        "phase": "exit_only_approval_pass",
        "approval_standard": {
            "must_hold": [
                "candidate beats or matches control under tougher cost assumptions",
                "candidate survives leave-one-symbol-out checks inside the approved lane",
                "candidate survives leave-one-session-out checks inside the approved lane",
                "candidate remains credible under local perturbation around the proposed setting",
            ]
        },
        "exit_approval_verdicts": {
            lane["lane_id"]: lane for lane in lane_reports
        },
    }
    json_path = resolved_output_dir / "approved_quant_exit_approval_pass.json"
    markdown_path = resolved_output_dir / "approved_quant_exit_approval_pass.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(json_path=json_path, markdown_path=markdown_path, report=report)


def _build_lane_exit_approval_report(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    entries_by_symbol, entry_diagnostic = _collect_entry_candidates(spec=spec, symbol_store=symbol_store)
    variants = {variant.exit_id: variant for variant in _build_exit_variants(spec)}
    control_variant = next(variant for variant in variants.values() if variant.family == "approved_control")
    candidate_exit_id = BREAKOUT_CANDIDATE_EXIT_ID if spec.direction == "LONG" else FAILED_CANDIDATE_EXIT_ID
    candidate_variant = variants[candidate_exit_id]

    control_row = _evaluate_exit_variant(
        spec=spec,
        variant=control_variant,
        entries_by_symbol=entries_by_symbol,
        symbol_store=symbol_store,
    )
    candidate_row = _evaluate_exit_variant(
        spec=spec,
        variant=candidate_variant,
        entries_by_symbol=entries_by_symbol,
        symbol_store=symbol_store,
    )
    perturbation_variants = _candidate_perturbation_variants(spec=spec, candidate_variant=candidate_variant)
    perturbation_rows = [
        _evaluate_exit_variant(
            spec=spec,
            variant=variant,
            entries_by_symbol=entries_by_symbol,
            symbol_store=symbol_store,
        )
        for variant in perturbation_variants
    ]

    assessment = _exit_approval_assessment(
        spec=spec,
        control_row=control_row,
        candidate_row=candidate_row,
        perturbation_rows=perturbation_rows,
    )
    score = _exit_approval_score(assessment=assessment, candidate_variant=asdict(candidate_variant))
    verdict = _exit_approval_verdict(assessment=assessment)
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.lane_name,
        "entry_diagnostic": entry_diagnostic,
        "control_exit_id": control_variant.exit_id,
        "candidate_exit_id": candidate_variant.exit_id,
        "control_summary": control_row["summary"],
        "candidate_summary": candidate_row["summary"],
        "approval_assessment": assessment,
        "approval_score": score,
        "approval_verdict": verdict,
        "replace_current_approved_exit": verdict["approved"],
        "current_exit_should_remain_unchanged": not verdict["approved"],
        "updated_plain_english_rationale": _updated_rationale(spec=spec, candidate_variant=candidate_variant, approved=verdict["approved"]),
        "session_label_dependency_risk": _session_label_dependency_risk(spec=spec, candidate_variant=candidate_variant),
    }


def _candidate_perturbation_variants(
    *,
    spec: ApprovedQuantLaneSpec,
    candidate_variant: ExitVariantSpec,
) -> list[ExitVariantSpec]:
    if spec.direction == "LONG":
        holds = (20, 22, 24, 26, 28)
        return [
            ExitVariantSpec(
                lane_id=spec.lane_id,
                exit_id=f"{spec.lane_id}.time_stop_only.h{hold}",
                family="time_stop_only",
                description=f"Perturbation around h24 using {hold} bars.",
                hold_bars=hold,
                stop_r=spec.stop_r,
                target_r=None,
                complexity_points=1,
            )
            for hold in holds
        ]
    levels = (0.35, 0.40, 0.45, 0.50, 0.55)
    return [
        ExitVariantSpec(
            lane_id=spec.lane_id,
            exit_id=f"{spec.lane_id}.structure_invalidation.r{str(level).replace('.', '')}",
            family="structure_invalidation",
            description=f"Perturbation around r045 using {level:.2f}R.",
            hold_bars=spec.hold_bars,
            stop_r=spec.stop_r,
            target_r=spec.target_r,
            structural_invalidation_r=level,
            complexity_points=2,
        )
        for level in levels
    ]


def _exit_approval_assessment(
    *,
    spec: ApprovedQuantLaneSpec,
    control_row: dict[str, Any],
    candidate_row: dict[str, Any],
    perturbation_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    costs = (0.25, 0.30, 0.35)
    control_costs = _cost_summary(control_row["trades"], spec=spec, costs=costs)
    candidate_costs = _cost_summary(candidate_row["trades"], spec=spec, costs=costs)
    perturbation_costs = [_cost_summary(row["trades"], spec=spec, costs=costs) for row in perturbation_rows]
    return {
        "control": control_costs,
        "candidate": candidate_costs,
        "improvement_vs_control": {
            "cost_expectancy_r_025": round(candidate_costs["cost_expectancy_r_025"] - control_costs["cost_expectancy_r_025"], 6),
            "cost_expectancy_r_030": round(candidate_costs["cost_expectancy_r_030"] - control_costs["cost_expectancy_r_030"], 6),
            "cost_expectancy_r_035": round(candidate_costs["cost_expectancy_r_035"] - control_costs["cost_expectancy_r_035"], 6),
            "avg_hold_bars": round(candidate_row["summary"]["avg_hold_bars"] - control_row["summary"]["avg_hold_bars"], 6),
            "avg_giveback_from_peak_r": round(candidate_row["summary"]["avg_giveback_from_peak_r"] - control_row["summary"]["avg_giveback_from_peak_r"], 6),
        },
        "leave_one_symbol_out_positive_ratio_030": round(
            sum(1 for row in candidate_costs["leave_one_symbol_out"] if row["cost_expectancy_r_030"] > 0.0) / float(max(len(candidate_costs["leave_one_symbol_out"]), 1)),
            6,
        ),
        "leave_one_session_out_positive_ratio_030": round(
            sum(1 for row in candidate_costs["leave_one_session_out"] if row["cost_expectancy_r_030"] > 0.0) / float(max(len(candidate_costs["leave_one_session_out"]), 1)),
            6,
        ),
        "perturbation": {
            "tested_neighbors": len(perturbation_costs),
            "positive_ratio_cost_030": round(sum(1 for row in perturbation_costs if row["cost_expectancy_r_030"] > 0.0) / float(max(len(perturbation_costs), 1)), 6),
            "median_cost_expectancy_r_030": _median_expectancy(perturbation_costs, key="cost_expectancy_r_030"),
            "worst_cost_expectancy_r_030": min((row["cost_expectancy_r_030"] for row in perturbation_costs), default=0.0),
        },
    }


def _cost_summary(
    trades: list[dict[str, Any]],
    *,
    spec: ApprovedQuantLaneSpec,
    costs: tuple[float, ...],
) -> dict[str, Any]:
    gross_values = [float(trade["gross_r"]) for trade in trades]
    summary = {
        f"cost_expectancy_r_{int(cost * 100):03d}": _expectancy([value - cost for value in gross_values])
        for cost in costs
    }
    summary["trade_count"] = len(trades)
    summary["per_symbol"] = [
        _slice_summary(
            rows=[trade for trade in trades if trade["symbol"] == symbol],
            label_key="symbol",
            label=symbol,
            costs=costs,
        )
        for symbol in spec.symbols
    ]
    summary["per_session"] = [
        _slice_summary(
            rows=[trade for trade in trades if trade["session_label"] == session],
            label_key="session_label",
            label=session,
            costs=costs,
        )
        for session in spec.allowed_sessions
    ]
    summary["leave_one_symbol_out"] = [
        _slice_summary(
            rows=[trade for trade in trades if trade["symbol"] != symbol],
            label_key="excluded_symbol",
            label=symbol,
            costs=costs,
        )
        for symbol in spec.symbols
    ]
    summary["leave_one_session_out"] = [
        _slice_summary(
            rows=[trade for trade in trades if trade["session_label"] != session],
            label_key="excluded_session",
            label=session,
            costs=costs,
        )
        for session in spec.allowed_sessions
    ]
    summary["concentration"] = _concentration_summary(trades)
    return summary


def _slice_summary(
    *,
    rows: list[dict[str, Any]],
    label_key: str,
    label: str,
    costs: tuple[float, ...],
) -> dict[str, Any]:
    gross_values = [float(row["gross_r"]) for row in rows]
    payload = {
        label_key: label,
        "trade_count": len(rows),
    }
    for cost in costs:
        payload[f"cost_expectancy_r_{int(cost * 100):03d}"] = _expectancy([value - cost for value in gross_values])
    return payload


def _concentration_summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
    totals: dict[str, float] = {}
    for trade in trades:
        symbol = str(trade["symbol"])
        totals[symbol] = totals.get(symbol, 0.0) + float(trade["gross_r"])
    total_abs = sum(abs(value) for value in totals.values())
    dominant_symbol = max(totals, key=lambda item: abs(totals[item])) if totals else None
    dominant_share = round(abs(totals.get(dominant_symbol, 0.0)) / total_abs, 6) if dominant_symbol and total_abs else 0.0
    return {
        "dominant_symbol": dominant_symbol,
        "dominant_symbol_share_of_total_abs_r": dominant_share,
    }


def _median_expectancy(rows: list[dict[str, Any]], *, key: str) -> float:
    values = sorted(float(row[key]) for row in rows)
    if not values:
        return 0.0
    mid = len(values) // 2
    if len(values) % 2:
        return round(values[mid], 6)
    return round((values[mid - 1] + values[mid]) / 2.0, 6)


def _exit_approval_score(*, assessment: dict[str, Any], candidate_variant: dict[str, Any]) -> float:
    candidate = assessment["candidate"]
    improvement = assessment["improvement_vs_control"]
    return round(
        100.0
        * (
            0.28 * _clip((candidate["cost_expectancy_r_030"] + 0.03) / 0.18, 0.0, 1.0)
            + 0.20 * _clip((candidate["cost_expectancy_r_035"] + 0.03) / 0.15, 0.0, 1.0)
            + 0.18 * _clip((improvement["cost_expectancy_r_030"] + 0.01) / 0.10, 0.0, 1.0)
            + 0.12 * assessment["leave_one_symbol_out_positive_ratio_030"]
            + 0.10 * assessment["leave_one_session_out_positive_ratio_030"]
            + 0.08 * assessment["perturbation"]["positive_ratio_cost_030"]
            + 0.06 * _clip((0.80 - candidate["concentration"]["dominant_symbol_share_of_total_abs_r"]) / 0.80, 0.0, 1.0)
            - 0.04 * _clip((float(candidate_variant["complexity_points"]) - 1.0) / 4.0, 0.0, 1.0)
        ),
        6,
    )


def _exit_approval_verdict(*, assessment: dict[str, Any]) -> dict[str, Any]:
    candidate = assessment["candidate"]
    improvement = assessment["improvement_vs_control"]
    approved = (
        candidate["cost_expectancy_r_030"] > 0.0
        and candidate["cost_expectancy_r_035"] >= 0.0
        and improvement["cost_expectancy_r_030"] > 0.01
        and assessment["leave_one_symbol_out_positive_ratio_030"] >= 0.75
        and assessment["leave_one_session_out_positive_ratio_030"] >= 0.50
        and assessment["perturbation"]["positive_ratio_cost_030"] >= 0.60
        and assessment["perturbation"]["worst_cost_expectancy_r_030"] > -0.03
        and candidate["concentration"]["dominant_symbol_share_of_total_abs_r"] <= 0.75
    )
    reason = "candidate_exit_replaces_control" if approved else "keep_current_control_exit"
    return {"approved": approved, "reason": reason}


def _updated_rationale(*, spec: ApprovedQuantLaneSpec, candidate_variant: ExitVariantSpec, approved: bool) -> str | None:
    if not approved:
        return None
    if spec.direction == "LONG":
        return (
            "The continuation edge appears to come more from entry quality than the frozen profit target. "
            "A longer hard time stop preserves the metals continuation move more reliably while keeping the lane definition unchanged."
        )
    return (
        "The reversal edge remains intact, but a modest close-based structural invalidation trims weaker follow-through earlier. "
        "This is an exit refinement to the approved no-US reversal lane, not a change to the lane definition."
    )


def _session_label_dependency_risk(*, spec: ApprovedQuantLaneSpec, candidate_variant: ExitVariantSpec) -> dict[str, Any]:
    if candidate_variant.session_boundary_exit:
        return {
            "level": "elevated",
            "note": "This candidate exit depends directly on session labels for exit timing; any label drift would alter realized exits.",
        }
    return {
        "level": "low_indirect",
        "note": (
            "The approved lane still depends on session labels for entry eligibility, but this candidate exit does not use session labels directly for exit timing."
        ),
    }


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Approved Quant Exit Approval Pass",
        "",
        f"Generated at: {report['generated_at']}",
        "",
    ]
    for lane_id, lane in report["exit_approval_verdicts"].items():
        lines.extend(
            [
                f"## {lane['lane_name']}",
                "",
                f"- Control exit: {lane['control_exit_id']}",
                f"- Candidate exit: {lane['candidate_exit_id']}",
                f"- Candidate replaces current approved exit: {lane['replace_current_approved_exit']}",
                f"- Current exit should remain unchanged: {lane['current_exit_should_remain_unchanged']}",
                f"- Approval verdict: {lane['approval_verdict']['approved']} ({lane['approval_verdict']['reason']})",
                f"- Control net 0.30R: {lane['approval_assessment']['control']['cost_expectancy_r_030']}",
                f"- Candidate net 0.30R: {lane['approval_assessment']['candidate']['cost_expectancy_r_030']}",
                f"- Improvement net 0.30R: {lane['approval_assessment']['improvement_vs_control']['cost_expectancy_r_030']}",
                f"- Leave-one-symbol-out positive ratio @0.30R: {lane['approval_assessment']['leave_one_symbol_out_positive_ratio_030']}",
                f"- Leave-one-session-out positive ratio @0.30R: {lane['approval_assessment']['leave_one_session_out_positive_ratio_030']}",
                f"- Perturbation positive ratio @0.30R: {lane['approval_assessment']['perturbation']['positive_ratio_cost_030']}",
                f"- Session-label dependency risk: {lane['session_label_dependency_risk']['level']} | {lane['session_label_dependency_risk']['note']}",
            ]
        )
        if lane["updated_plain_english_rationale"]:
            lines.append(f"- Updated rationale: {lane['updated_plain_english_rationale']}")
        lines.append("")
    return "\n".join(lines)
