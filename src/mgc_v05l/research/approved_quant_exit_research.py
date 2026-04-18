"""Research-only exit analysis for approved quant baseline lanes.

This module intentionally reuses the approved lane entry definitions without
changing the probationary baseline runtime. Any candidate exit found here must
go through a later approval pass before it can affect the approved path.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..app.approved_quant_lanes.evaluator import lane_rejection_reason, lane_rule_snapshot
from ..app.approved_quant_lanes.runtime_boundary import (
    ApprovedQuantFrameSeries,
    build_approved_quant_symbol_store,
    validate_approved_quant_feature_payload,
)
from ..app.approved_quant_lanes.specs import ApprovedQuantLaneSpec, approved_quant_lane_specs
from .quant_futures import (
    StrategyResearchArtifacts,
    _avg,
    _expectancy,
    _max_drawdown,
    _profit_factor,
    _sharpe_proxy,
    _win_rate,
)


@dataclass(frozen=True)
class ExitVariantSpec:
    lane_id: str
    exit_id: str
    family: str
    description: str
    hold_bars: int
    stop_r: float
    target_r: float | None
    structural_invalidation_r: float | None = None
    excursion_arm_r: float | None = None
    giveback_r: float | None = None
    checkpoint_arm_r: float | None = None
    checkpoint_lock_r: float | None = None
    checkpoint_trail_r: float | None = None
    no_traction_abort_bars: int | None = None
    no_traction_min_favorable_r: float | None = None
    session_boundary_exit: bool = False
    complexity_points: int = 1


@dataclass(frozen=True)
class ApprovedLaneEntryCandidate:
    lane_id: str
    lane_name: str
    symbol: str
    session_label: str
    signal_index: int
    entry_index: int
    signal_ts: str
    entry_ts: str
    entry_price: float
    risk: float
    direction: str
    rule_snapshot: dict[str, float | str | bool]


@dataclass(frozen=True)
class ExitResearchTrade:
    lane_id: str
    lane_name: str
    exit_id: str
    exit_family: str
    symbol: str
    session_label: str
    signal_ts: str
    entry_ts: str
    exit_ts: str
    direction: str
    entry_price: float
    exit_price: float
    exit_reason: str
    holding_bars: int
    gross_r: float
    net_r_cost_020: float
    net_r_cost_025: float
    mfe_r: float
    mae_r: float
    giveback_from_peak_r: float


def run_approved_quant_exit_research(
    *,
    database_path: str | Path,
    execution_timeframe: str = "5m",
    output_dir: str | Path | None = None,
) -> StrategyResearchArtifacts:
    resolved_database_path = Path(database_path).resolve()
    resolved_output_dir = Path(output_dir or Path.cwd() / "outputs" / "reports" / "approved_quant_exit_research").resolve()
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
        lane_reports.append(_evaluate_lane_exit_research(spec=spec, symbol_store=symbol_store))

    generated_at = datetime.now(UTC).isoformat()
    report = {
        "generated_at": generated_at,
        "source_of_truth_path": str(Path(specs[0].approval_source).resolve()),
        "execution_timeframe": execution_timeframe,
        "lane_count": len(lane_reports),
        "lane_reports": lane_reports,
        "summary": {
            "lanes_with_exit_redesign_worthwhile": [
                lane["lane_id"]
                for lane in lane_reports
                if lane["recommendation"]["exit_redesign_worthwhile"]
            ],
            "lanes_with_later_approval_candidates": [
                lane["lane_id"]
                for lane in lane_reports
                if lane["recommendation"]["best_candidate_ready_for_later_approval_pass"]
            ],
            "current_exits_should_remain_unchanged": {
                lane["lane_id"]: lane["recommendation"]["current_approved_exit_should_remain_unchanged"]
                for lane in lane_reports
            },
        },
    }
    json_path = resolved_output_dir / "approved_quant_exit_research_report.json"
    markdown_path = resolved_output_dir / "approved_quant_exit_research_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_exit_research_markdown(report).strip() + "\n", encoding="utf-8")
    return StrategyResearchArtifacts(
        json_path=json_path,
        markdown_path=markdown_path,
        report=report,
    )


def _evaluate_lane_exit_research(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    entries_by_symbol, entry_diagnostic = _collect_entry_candidates(spec=spec, symbol_store=symbol_store)
    variants = _build_exit_variants(spec)
    variant_rows = []
    for variant in variants:
        summary = _evaluate_exit_variant(
            spec=spec,
            variant=variant,
            entries_by_symbol=entries_by_symbol,
            symbol_store=symbol_store,
        )
        variant_rows.append(summary)

    family_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in variant_rows:
        family_rows[row["variant"]["family"]].append(row)

    for family, rows in family_rows.items():
        positive_ratio_020 = round(
            sum(1 for row in rows if row["summary"]["expectancy_net_020_r"] > 0.0) / float(len(rows)),
            6,
        )
        positive_ratio_025 = round(
            sum(1 for row in rows if row["summary"]["expectancy_net_025_r"] > 0.0) / float(len(rows)),
            6,
        )
        for row in rows:
            row["parameter_sensitivity"] = {
                "family_variant_count": len(rows),
                "positive_ratio_cost_020": positive_ratio_020,
                "positive_ratio_cost_025": positive_ratio_025,
            }

    control_row = next(row for row in variant_rows if row["variant"]["family"] == "approved_control")
    for row in variant_rows:
        row["summary"]["improvement_vs_control_cost_020"] = round(
            row["summary"]["expectancy_net_020_r"] - control_row["summary"]["expectancy_net_020_r"],
            6,
        )
        row["summary"]["improvement_vs_control_cost_025"] = round(
            row["summary"]["expectancy_net_025_r"] - control_row["summary"]["expectancy_net_025_r"],
            6,
        )
        row["summary"]["score"] = _exit_variant_score(
            summary=row["summary"],
            robustness=row["robustness"],
            parameter_sensitivity=row["parameter_sensitivity"],
            variant=row["variant"],
        )
        row["approval_signal"] = _later_approval_pass_signal(row=row, control_row=control_row)

    ranked_rows = sorted(variant_rows, key=lambda row: row["summary"]["score"], reverse=True)
    best_non_control = next((row for row in ranked_rows if row["variant"]["family"] != "approved_control"), None)

    recommendation = {
        "exit_redesign_worthwhile": bool(
            best_non_control
            and (
                best_non_control["approval_signal"]["ready_for_later_approval_pass"]
                or best_non_control["summary"]["score"] > control_row["summary"]["score"]
                or best_non_control["summary"]["improvement_vs_control_cost_025"] > 0.01
            )
        ),
        "best_candidate_ready_for_later_approval_pass": bool(best_non_control and best_non_control["approval_signal"]["ready_for_later_approval_pass"]),
        "best_candidate_exit_id": best_non_control["variant"]["exit_id"] if best_non_control else None,
        "best_candidate_family": best_non_control["variant"]["family"] if best_non_control else None,
        "current_approved_exit_should_remain_unchanged": True,
        "operator_note": _lane_operator_note(
            control_row=control_row,
            best_non_control=best_non_control,
        ),
    }

    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.lane_name,
        "entry_diagnostic": entry_diagnostic,
        "control_exit_id": control_row["variant"]["exit_id"],
        "control_summary": control_row["summary"],
        "ranked_exit_variants": ranked_rows,
        "recommendation": recommendation,
    }


def _collect_entry_candidates(
    *,
    spec: ApprovedQuantLaneSpec,
    symbol_store: dict[str, dict[str, Any]],
) -> tuple[dict[str, list[ApprovedLaneEntryCandidate]], dict[str, Any]]:
    entries_by_symbol: dict[str, list[ApprovedLaneEntryCandidate]] = {}
    entry_mfe_values = []
    entry_mae_values = []
    reach_half_r = 0
    reach_one_r = 0
    reach_control_target = 0
    for symbol in spec.symbols:
        payload = symbol_store.get(symbol)
        if payload is None:
            continue
        execution: ApprovedQuantFrameSeries = payload["execution"]
        features: list[dict[str, Any]] = payload["features"]
        rows: list[ApprovedLaneEntryCandidate] = []
        for index, feature in enumerate(features):
            if index + 1 >= len(execution.bars):
                continue
            if not feature.get("ready"):
                continue
            validate_approved_quant_feature_payload(lane_id=spec.lane_id, feature=feature)
            session_label = str(feature["session_label"])
            rejection_reason = lane_rejection_reason(spec=spec, session_label=session_label, feature=feature)
            if rejection_reason is not None:
                continue
            entry_index = index + 1
            entry_price = execution.opens[entry_index]
            risk = max(float(feature["risk_unit"]), 1e-6)
            rows.append(
                ApprovedLaneEntryCandidate(
                    lane_id=spec.lane_id,
                    lane_name=spec.lane_name,
                    symbol=symbol,
                    session_label=session_label,
                    signal_index=index,
                    entry_index=entry_index,
                    signal_ts=execution.timestamps[index].isoformat(),
                    entry_ts=execution.timestamps[entry_index].isoformat(),
                    entry_price=round(entry_price, 6),
                    risk=risk,
                    direction=spec.direction,
                    rule_snapshot=lane_rule_snapshot(spec=spec, session_label=session_label, feature=feature),
                )
            )
            mfe_r, mae_r = _entry_quality_excursion(
                execution=execution,
                direction=spec.direction,
                entry_index=entry_index,
                hold_bars=spec.hold_bars,
                entry_price=entry_price,
                risk=risk,
            )
            entry_mfe_values.append(mfe_r)
            entry_mae_values.append(mae_r)
            if mfe_r >= 0.5:
                reach_half_r += 1
            if mfe_r >= 1.0:
                reach_one_r += 1
            if spec.target_r is not None and mfe_r >= spec.target_r:
                reach_control_target += 1
        entries_by_symbol[symbol] = rows

    total_entries = sum(len(rows) for rows in entries_by_symbol.values())
    return entries_by_symbol, {
        "raw_entry_count": total_entries,
        "avg_peak_mfe_control_horizon_r": _avg(entry_mfe_values),
        "avg_worst_mae_control_horizon_r": _avg(entry_mae_values),
        "share_reaching_half_r": round(reach_half_r / float(total_entries), 6) if total_entries else 0.0,
        "share_reaching_one_r": round(reach_one_r / float(total_entries), 6) if total_entries else 0.0,
        "share_reaching_control_target": round(reach_control_target / float(total_entries), 6) if total_entries else 0.0,
    }


def _entry_quality_excursion(
    *,
    execution: ApprovedQuantFrameSeries,
    direction: str,
    entry_index: int,
    hold_bars: int,
    entry_price: float,
    risk: float,
) -> tuple[float, float]:
    last_index = min(entry_index + hold_bars - 1, len(execution.bars) - 1)
    best_mfe = 0.0
    worst_mae = 0.0
    for index in range(entry_index, last_index + 1):
        if direction == "LONG":
            mfe = (execution.highs[index] - entry_price) / risk
            mae = (execution.lows[index] - entry_price) / risk
        else:
            mfe = (entry_price - execution.lows[index]) / risk
            mae = (entry_price - execution.highs[index]) / risk
        best_mfe = max(best_mfe, mfe)
        worst_mae = min(worst_mae, mae)
    return round(best_mfe, 6), round(worst_mae, 6)


def _build_exit_variants(spec: ApprovedQuantLaneSpec) -> list[ExitVariantSpec]:
    variants = [
        ExitVariantSpec(
            lane_id=spec.lane_id,
            exit_id=f"{spec.lane_id}.approved_control",
            family="approved_control",
            description="Frozen approved probation exit used as the control.",
            hold_bars=spec.hold_bars,
            stop_r=spec.stop_r,
            target_r=spec.target_r,
            complexity_points=1,
        )
    ]
    if spec.direction == "LONG":
        variants.extend(
            [
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.time_stop_only.h12", "time_stop_only", "Time exit with hard stop only.", 12, spec.stop_r, None, complexity_points=1),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.time_stop_only.h18", "time_stop_only", "Baseline horizon, no target, hard stop only.", 18, spec.stop_r, None, complexity_points=1),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.time_stop_only.h24", "time_stop_only", "Longer time exit with hard stop only.", 24, spec.stop_r, None, complexity_points=1),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.structure_invalidation.r025", "structure_invalidation", "Exit on close-based structure failure at 0.25R adverse.", spec.hold_bars, spec.stop_r, spec.target_r, structural_invalidation_r=0.25, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.structure_invalidation.r035", "structure_invalidation", "Exit on close-based structure failure at 0.35R adverse.", spec.hold_bars, spec.stop_r, spec.target_r, structural_invalidation_r=0.35, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.structure_invalidation.r045", "structure_invalidation", "Exit on close-based structure failure at 0.45R adverse.", spec.hold_bars, spec.stop_r, spec.target_r, structural_invalidation_r=0.45, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.excursion_trail.a080.g050", "excursion_trail", "Arm a give-back trail after 0.80R MFE.", spec.hold_bars, spec.stop_r, None, excursion_arm_r=0.80, giveback_r=0.50, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.excursion_trail.a100.g050", "excursion_trail", "Arm a give-back trail after 1.00R MFE.", spec.hold_bars, spec.stop_r, None, excursion_arm_r=1.00, giveback_r=0.50, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.excursion_trail.a080.g075", "excursion_trail", "Looser give-back trail after 0.80R MFE.", spec.hold_bars, spec.stop_r, None, excursion_arm_r=0.80, giveback_r=0.75, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.checkpoint_no_traction.a080", "checkpoint_no_traction", "Arm a checkpoint after 0.80R MFE and abort after 2 bars if no traction appears.", spec.hold_bars, spec.stop_r, None, checkpoint_arm_r=0.80, checkpoint_lock_r=0.35, checkpoint_trail_r=0.25, no_traction_abort_bars=2, no_traction_min_favorable_r=0.25, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.session_boundary.h12", "session_boundary", "Exit on first excluded-session bar or after 12 bars.", 12, spec.stop_r, None, session_boundary_exit=True, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.session_boundary.h18", "session_boundary", "Exit on first excluded-session bar or after baseline horizon.", 18, spec.stop_r, None, session_boundary_exit=True, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.hybrid.target_plus_session_boundary", "simple_hybrid", "Keep approved target/stop but clamp at session boundary.", spec.hold_bars, spec.stop_r, spec.target_r, session_boundary_exit=True, complexity_points=3),
            ]
        )
    else:
        variants.extend(
            [
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.time_stop_only.h8", "time_stop_only", "Shorter time exit with hard stop only.", 8, spec.stop_r, None, complexity_points=1),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.time_stop_only.h12", "time_stop_only", "Baseline horizon, no target, hard stop only.", 12, spec.stop_r, None, complexity_points=1),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.time_stop_only.h16", "time_stop_only", "Longer time exit with hard stop only.", 16, spec.stop_r, None, complexity_points=1),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.structure_invalidation.r025", "structure_invalidation", "Exit on close-based structure failure at 0.25R adverse.", spec.hold_bars, spec.stop_r, spec.target_r, structural_invalidation_r=0.25, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.structure_invalidation.r035", "structure_invalidation", "Exit on close-based structure failure at 0.35R adverse.", spec.hold_bars, spec.stop_r, spec.target_r, structural_invalidation_r=0.35, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.structure_invalidation.r045", "structure_invalidation", "Exit on close-based structure failure at 0.45R adverse.", spec.hold_bars, spec.stop_r, spec.target_r, structural_invalidation_r=0.45, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.excursion_trail.a060.g040", "excursion_trail", "Arm a give-back trail after 0.60R MFE.", spec.hold_bars, spec.stop_r, None, excursion_arm_r=0.60, giveback_r=0.40, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.excursion_trail.a080.g040", "excursion_trail", "Arm a give-back trail after 0.80R MFE.", spec.hold_bars, spec.stop_r, None, excursion_arm_r=0.80, giveback_r=0.40, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.excursion_trail.a060.g060", "excursion_trail", "Looser give-back trail after 0.60R MFE.", spec.hold_bars, spec.stop_r, None, excursion_arm_r=0.60, giveback_r=0.60, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.checkpoint_no_traction.a080", "checkpoint_no_traction", "Arm a checkpoint after 0.80R MFE and abort after 2 bars if no traction appears.", spec.hold_bars, spec.stop_r, None, checkpoint_arm_r=0.80, checkpoint_lock_r=0.35, checkpoint_trail_r=0.25, no_traction_abort_bars=2, no_traction_min_favorable_r=0.25, complexity_points=3),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.session_boundary.h8", "session_boundary", "Exit on first excluded-session bar or after 8 bars.", 8, spec.stop_r, None, session_boundary_exit=True, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.session_boundary.h12", "session_boundary", "Exit on first excluded-session bar or after baseline horizon.", 12, spec.stop_r, None, session_boundary_exit=True, complexity_points=2),
                ExitVariantSpec(spec.lane_id, f"{spec.lane_id}.hybrid.target_plus_session_boundary", "simple_hybrid", "Keep approved target/stop but clamp at session boundary.", spec.hold_bars, spec.stop_r, spec.target_r, session_boundary_exit=True, complexity_points=3),
            ]
        )
    return variants


def _evaluate_exit_variant(
    *,
    spec: ApprovedQuantLaneSpec,
    variant: ExitVariantSpec,
    entries_by_symbol: dict[str, list[ApprovedLaneEntryCandidate]],
    symbol_store: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    trades: list[ExitResearchTrade] = []
    for symbol in spec.symbols:
        payload = symbol_store.get(symbol)
        if payload is None:
            continue
        execution: ApprovedQuantFrameSeries = payload["execution"]
        features: list[dict[str, Any]] = payload["features"]
        next_available_index = 0
        for entry in entries_by_symbol.get(symbol, []):
            if entry.signal_index < next_available_index:
                continue
            trade = _simulate_trade(
                spec=spec,
                variant=variant,
                entry=entry,
                execution=execution,
                features=features,
            )
            trades.append(trade)
            next_available_index = _index_for_timestamp(execution.timestamps, trade.exit_ts) + 1

    gross_values = [trade.gross_r for trade in trades]
    net020_values = [trade.net_r_cost_020 for trade in trades]
    net025_values = [trade.net_r_cost_025 for trade in trades]
    hold_values = [float(trade.holding_bars) for trade in trades]
    mfe_values = [trade.mfe_r for trade in trades]
    mae_values = [trade.mae_r for trade in trades]
    giveback_values = [trade.giveback_from_peak_r for trade in trades]
    summary = {
        "trade_count": len(trades),
        "expectancy_gross_r": _expectancy(gross_values),
        "expectancy_net_020_r": _expectancy(net020_values),
        "expectancy_net_025_r": _expectancy(net025_values),
        "win_rate": _win_rate(gross_values),
        "avg_win_r": _avg([value for value in gross_values if value > 0.0]),
        "avg_loss_r": _avg([value for value in gross_values if value < 0.0]),
        "profit_factor": _profit_factor(gross_values),
        "max_drawdown_r": _max_drawdown(gross_values),
        "sharpe_proxy": _sharpe_proxy(gross_values),
        "avg_hold_bars": _avg(hold_values),
        "avg_mfe_r": _avg(mfe_values),
        "avg_mae_r": _avg(mae_values),
        "avg_giveback_from_peak_r": _avg(giveback_values),
        "giveback_capture_ratio": round(_expectancy(gross_values) / max(_avg(mfe_values), 1e-9), 6) if trades else 0.0,
        "walk_forward_positive_ratio": _sequence_walk_forward_positive_ratio(net025_values, folds=5),
        "exit_reason_counts": _count_by(trades, key="exit_reason"),
        "win_loss_distribution": {
            "gt_1r": sum(1 for value in gross_values if value >= 1.0),
            "between_0_and_1r": sum(1 for value in gross_values if 0.0 < value < 1.0),
            "between_0_and_neg1r": sum(1 for value in gross_values if -1.0 < value <= 0.0),
            "lte_neg1r": sum(1 for value in gross_values if value <= -1.0),
        },
    }
    robustness = _robustness_summary(spec=spec, trades=trades)
    return {
        "variant": asdict(variant),
        "summary": summary,
        "robustness": robustness,
        "trades": [asdict(trade) for trade in trades],
    }


def _simulate_trade(
    *,
    spec: ApprovedQuantLaneSpec,
    variant: ExitVariantSpec,
    entry: ApprovedLaneEntryCandidate,
    execution: ApprovedQuantFrameSeries,
    features: list[dict[str, Any]],
) -> ExitResearchTrade:
    last_index = min(entry.entry_index + variant.hold_bars - 1, len(execution.bars) - 1)
    peak_mfe = 0.0
    trough_mae = 0.0
    stop_price = entry.entry_price - variant.stop_r * entry.risk if spec.direction == "LONG" else entry.entry_price + variant.stop_r * entry.risk
    target_price = None
    if variant.target_r is not None:
        target_price = entry.entry_price + variant.target_r * entry.risk if spec.direction == "LONG" else entry.entry_price - variant.target_r * entry.risk
    exit_index = last_index
    exit_price = execution.closes[last_index]
    exit_reason = "time_exit"
    checkpoint_reached = False
    dynamic_stop_price = stop_price
    for index in range(entry.entry_index, last_index + 1):
        high = execution.highs[index]
        low = execution.lows[index]
        close = execution.closes[index]
        current_session = _session_label_at(features=features, index=index, fallback=entry.session_label)
        current_mfe, current_mae = _current_excursion(
            direction=spec.direction,
            high=high,
            low=low,
            entry_price=entry.entry_price,
            risk=entry.risk,
        )
        peak_mfe = max(peak_mfe, current_mfe)
        trough_mae = min(trough_mae, current_mae)
        if (
            not checkpoint_reached
            and variant.checkpoint_arm_r is not None
            and peak_mfe >= float(variant.checkpoint_arm_r)
        ):
            checkpoint_reached = True
        if checkpoint_reached and variant.checkpoint_lock_r is not None and variant.checkpoint_trail_r is not None:
            dynamic_stop_price = _checkpoint_stop_price(
                direction=spec.direction,
                current_stop=dynamic_stop_price,
                entry_price=entry.entry_price,
                risk=entry.risk,
                bar_low=low,
                bar_high=high,
                checkpoint_lock_r=float(variant.checkpoint_lock_r),
                checkpoint_trail_r=float(variant.checkpoint_trail_r),
            )
        if spec.direction == "LONG":
            stop_hit = low <= dynamic_stop_price
            target_hit = target_price is not None and high >= target_price
        else:
            stop_hit = high >= dynamic_stop_price
            target_hit = target_price is not None and low <= target_price
        if stop_hit and target_hit:
            exit_index = index
            exit_price = dynamic_stop_price
            exit_reason = "checkpoint_stop_first_conflict" if checkpoint_reached else "stop_first_conflict"
            break
        if stop_hit:
            exit_index = index
            exit_price = dynamic_stop_price
            exit_reason = "checkpoint_stop" if checkpoint_reached else "stop"
            break
        if target_hit and target_price is not None:
            exit_index = index
            exit_price = target_price
            exit_reason = "target"
            break
        if (
            not checkpoint_reached
            and variant.no_traction_abort_bars is not None
            and variant.no_traction_min_favorable_r is not None
            and (index - entry.entry_index + 1) >= int(variant.no_traction_abort_bars)
            and peak_mfe < float(variant.no_traction_min_favorable_r)
        ):
            exit_index = index
            exit_price = close
            exit_reason = "no_traction_abort"
            break
        if variant.session_boundary_exit and current_session not in spec.allowed_sessions:
            exit_index = index
            exit_price = close
            exit_reason = "session_boundary"
            break
        if variant.structural_invalidation_r is not None:
            if spec.direction == "LONG" and close <= entry.entry_price - variant.structural_invalidation_r * entry.risk:
                exit_index = index
                exit_price = close
                exit_reason = "structural_invalidation"
                break
            if spec.direction == "SHORT" and close >= entry.entry_price + variant.structural_invalidation_r * entry.risk:
                exit_index = index
                exit_price = close
                exit_reason = "structural_invalidation"
                break
        if (
            variant.excursion_arm_r is not None
            and variant.giveback_r is not None
            and peak_mfe >= variant.excursion_arm_r
            and peak_mfe - current_mfe >= variant.giveback_r
        ):
            exit_index = index
            exit_price = close
            exit_reason = "excursion_giveback"
            break

    gross_r = (exit_price - entry.entry_price) / entry.risk if spec.direction == "LONG" else (entry.entry_price - exit_price) / entry.risk
    giveback_from_peak = max(peak_mfe - gross_r, 0.0)
    return ExitResearchTrade(
        lane_id=entry.lane_id,
        lane_name=entry.lane_name,
        exit_id=variant.exit_id,
        exit_family=variant.family,
        symbol=entry.symbol,
        session_label=entry.session_label,
        signal_ts=entry.signal_ts,
        entry_ts=entry.entry_ts,
        exit_ts=execution.timestamps[exit_index].isoformat(),
        direction=entry.direction,
        entry_price=round(entry.entry_price, 6),
        exit_price=round(exit_price, 6),
        exit_reason=exit_reason,
        holding_bars=max(exit_index - entry.entry_index + 1, 1),
        gross_r=round(gross_r, 6),
        net_r_cost_020=round(gross_r - 0.20, 6),
        net_r_cost_025=round(gross_r - 0.25, 6),
        mfe_r=round(peak_mfe, 6),
        mae_r=round(trough_mae, 6),
        giveback_from_peak_r=round(giveback_from_peak, 6),
    )


def _checkpoint_stop_price(
    *,
    direction: str,
    current_stop: float,
    entry_price: float,
    risk: float,
    bar_low: float,
    bar_high: float,
    checkpoint_lock_r: float,
    checkpoint_trail_r: float,
) -> float:
    if direction == "LONG":
        locked_profit_stop = entry_price + risk * checkpoint_lock_r
        structure_stop = bar_low - risk * checkpoint_trail_r
        return max(current_stop, locked_profit_stop, structure_stop)
    locked_profit_stop = entry_price - risk * checkpoint_lock_r
    structure_stop = bar_high + risk * checkpoint_trail_r
    return min(current_stop, locked_profit_stop, structure_stop)


def _current_excursion(
    *,
    direction: str,
    high: float,
    low: float,
    entry_price: float,
    risk: float,
) -> tuple[float, float]:
    if direction == "LONG":
        return (high - entry_price) / risk, (low - entry_price) / risk
    return (entry_price - low) / risk, (entry_price - high) / risk


def _session_label_at(*, features: list[dict[str, Any]], index: int, fallback: str) -> str:
    if index >= len(features):
        return fallback
    return str(features[index].get("session_label", fallback))


def _index_for_timestamp(timestamps: list[datetime], target_iso: str) -> int:
    for index, timestamp in enumerate(timestamps):
        if timestamp.isoformat() == target_iso:
            return index
    raise ValueError(f"Unable to locate timestamp in execution frame: {target_iso}")


def _robustness_summary(*, spec: ApprovedQuantLaneSpec, trades: list[ExitResearchTrade]) -> dict[str, Any]:
    by_symbol = defaultdict(list)
    by_session = defaultdict(list)
    for trade in trades:
        by_symbol[trade.symbol].append(trade.net_r_cost_025)
        by_session[trade.session_label].append(trade.net_r_cost_025)
    leave_one_symbol_out_values = []
    for omitted_symbol in spec.symbols:
        values = [trade.net_r_cost_025 for trade in trades if trade.symbol != omitted_symbol]
        leave_one_symbol_out_values.append({"omitted_symbol": omitted_symbol, "expectancy_net_025_r": _expectancy(values)})
    leave_one_session_out_values = []
    for omitted_session in spec.allowed_sessions:
        values = [trade.net_r_cost_025 for trade in trades if trade.session_label != omitted_session]
        leave_one_session_out_values.append({"omitted_session": omitted_session, "expectancy_net_025_r": _expectancy(values)})
    symbol_rows = []
    for symbol in spec.symbols:
        values020 = [trade.net_r_cost_020 for trade in trades if trade.symbol == symbol]
        values025 = [trade.net_r_cost_025 for trade in trades if trade.symbol == symbol]
        symbol_rows.append(
            {
                "symbol": symbol,
                "trade_count": len(values020),
                "expectancy_net_020_r": _expectancy(values020),
                "expectancy_net_025_r": _expectancy(values025),
            }
        )
    session_rows = []
    for session in spec.allowed_sessions:
        values020 = [trade.net_r_cost_020 for trade in trades if trade.session_label == session]
        values025 = [trade.net_r_cost_025 for trade in trades if trade.session_label == session]
        session_rows.append(
            {
                "session_label": session,
                "trade_count": len(values020),
                "expectancy_net_020_r": _expectancy(values020),
                "expectancy_net_025_r": _expectancy(values025),
            }
        )
    positive_symbol_share_025 = round(
        sum(1 for row in symbol_rows if row["expectancy_net_025_r"] > 0.0) / float(max(len(symbol_rows), 1)),
        6,
    )
    positive_session_share_025 = round(
        sum(1 for row in session_rows if row["expectancy_net_025_r"] > 0.0) / float(max(len(session_rows), 1)),
        6,
    )
    symbol_totals = {
        symbol: sum(trade.net_r_cost_025 for trade in trades if trade.symbol == symbol)
        for symbol in spec.symbols
    }
    total_abs = sum(abs(value) for value in symbol_totals.values())
    dominant_symbol_share = round(max((abs(value) for value in symbol_totals.values()), default=0.0) / total_abs, 6) if total_abs else 0.0
    return {
        "symbol_rows": symbol_rows,
        "session_rows": session_rows,
        "leave_one_symbol_out": leave_one_symbol_out_values,
        "leave_one_symbol_out_positive_ratio_025": round(
            sum(1 for row in leave_one_symbol_out_values if row["expectancy_net_025_r"] > 0.0) / float(max(len(leave_one_symbol_out_values), 1)),
            6,
        ),
        "leave_one_session_out": leave_one_session_out_values,
        "leave_one_session_out_positive_ratio_025": round(
            sum(1 for row in leave_one_session_out_values if row["expectancy_net_025_r"] > 0.0) / float(max(len(leave_one_session_out_values), 1)),
            6,
        ),
        "positive_symbol_share_025": positive_symbol_share_025,
        "positive_session_share_025": positive_session_share_025,
        "concentration": {
            "dominant_symbol_share_of_total_abs_r_025": dominant_symbol_share,
        },
    }


def _exit_variant_score(
    *,
    summary: dict[str, Any],
    robustness: dict[str, Any],
    parameter_sensitivity: dict[str, Any],
    variant: dict[str, Any],
) -> float:
    return round(
        (summary["expectancy_net_020_r"] * 2.5)
        + (summary["expectancy_net_025_r"] * 3.0)
        + (summary["walk_forward_positive_ratio"] * 0.75)
        + (robustness["leave_one_symbol_out_positive_ratio_025"] * 0.75)
        + (robustness["leave_one_session_out_positive_ratio_025"] * 0.50)
        + (parameter_sensitivity["positive_ratio_cost_025"] * 0.35)
        - (summary["avg_giveback_from_peak_r"] * 0.15)
        - (robustness["concentration"]["dominant_symbol_share_of_total_abs_r_025"] * 0.40)
        - (float(variant["complexity_points"]) * 0.08),
        6,
    )


def _later_approval_pass_signal(*, row: dict[str, Any], control_row: dict[str, Any]) -> dict[str, Any]:
    summary = row["summary"]
    robustness = row["robustness"]
    parameter_sensitivity = row["parameter_sensitivity"]
    ready = (
        row["variant"]["family"] != "approved_control"
        and summary["expectancy_net_025_r"] > 0.0
        and summary["improvement_vs_control_cost_025"] > 0.01
        and robustness["leave_one_symbol_out_positive_ratio_025"] >= 0.60
        and robustness["leave_one_session_out_positive_ratio_025"] >= 0.50
        and parameter_sensitivity["positive_ratio_cost_025"] >= 0.40
        and summary["trade_count"] >= 40
        and robustness["concentration"]["dominant_symbol_share_of_total_abs_r_025"] <= 0.75
    )
    return {
        "ready_for_later_approval_pass": ready,
        "reason": (
            "robust_positive_post_cost_exit_candidate"
            if ready
            else (
                "current_control_remains_preferred"
                if summary["score"] <= control_row["summary"]["score"]
                else "insufficient_robustness_for_later_approval"
            )
        ),
    }


def _lane_operator_note(*, control_row: dict[str, Any], best_non_control: dict[str, Any] | None) -> str:
    if best_non_control is None:
        return "No alternate exit improved on the frozen approved control."
    if best_non_control["approval_signal"]["ready_for_later_approval_pass"]:
        return (
            f"Alternate exit {best_non_control['variant']['exit_id']} merits a later exit-only approval pass; "
            "approved probation exit remains frozen until then."
        )
    if best_non_control["summary"]["score"] > control_row["summary"]["score"]:
        return (
            f"Alternate exit {best_non_control['variant']['exit_id']} improved the research score, "
            "but robustness is not yet strong enough to challenge the frozen approved baseline exit."
        )
    return "Current approved control still leads on the research score; keep approved exit unchanged."


def _count_by(trades: list[ExitResearchTrade], *, key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        label = str(getattr(trade, key))
        counts[label] = counts.get(label, 0) + 1
    return dict(sorted(counts.items()))


def _sequence_walk_forward_positive_ratio(values: list[float], *, folds: int = 5) -> float:
    if not values:
        return 0.0
    chunk_size = max(len(values) // folds, 1)
    chunks = [values[index : index + chunk_size] for index in range(0, len(values), chunk_size)]
    positives = sum(1 for chunk in chunks if _expectancy(chunk) > 0.0)
    return round(positives / float(len(chunks)), 6) if chunks else 0.0


def _render_exit_research_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Approved Quant Exit Research",
        "",
        f"Generated at: {report['generated_at']}",
        "",
    ]
    for lane in report["lane_reports"]:
        lines.extend(
            [
                f"## {lane['lane_name']}",
                "",
                f"- Raw entry count: {lane['entry_diagnostic']['raw_entry_count']}",
                f"- Entry diagnostic avg MFE: {lane['entry_diagnostic']['avg_peak_mfe_control_horizon_r']}",
                f"- Entry diagnostic avg MAE: {lane['entry_diagnostic']['avg_worst_mae_control_horizon_r']}",
                f"- Share reaching 1R: {lane['entry_diagnostic']['share_reaching_one_r']}",
                f"- Control exit expectancy net 0.20R: {lane['control_summary']['expectancy_net_020_r']}",
                f"- Control exit expectancy net 0.25R: {lane['control_summary']['expectancy_net_025_r']}",
                f"- Exit redesign worthwhile: {lane['recommendation']['exit_redesign_worthwhile']}",
                f"- Best candidate ready for later approval pass: {lane['recommendation']['best_candidate_ready_for_later_approval_pass']}",
                f"- Current approved exits should remain unchanged: {lane['recommendation']['current_approved_exit_should_remain_unchanged']}",
                "",
                "Top exit variants:",
            ]
        )
        for row in lane["ranked_exit_variants"][:4]:
            lines.append(
                f"- {row['variant']['exit_id']}: score={row['summary']['score']} | "
                f"net020={row['summary']['expectancy_net_020_r']} | "
                f"net025={row['summary']['expectancy_net_025_r']} | "
                f"hold={row['summary']['avg_hold_bars']} | "
                f"giveback={row['summary']['avg_giveback_from_peak_r']} | "
                f"later_approval={row['approval_signal']['ready_for_later_approval_pass']}"
            )
        lines.extend(["", lane["recommendation"]["operator_note"], ""])
    return "\n".join(lines)
