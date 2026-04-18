"""Execution-realism stress for the admitted GC ATP production-track package."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

from .atp_companion_drawdown_limit_governance import DrawdownGovernanceControl, _apply_drawdown_control
from .atp_experiment_registry import register_atp_report_output
from .atp_companion_failure_governance_review import _drawdown_episodes
from .atp_companion_full_history_review import (
    DEFAULT_SOURCE_DB,
    EvaluationTarget,
    _discover_best_sources,
    _shared_1m_coverage,
    build_targets,
)
from .atp_companion_production_shaping_review import _clip_windows_to_rows
from .atp_companion_us_fast_fail_review import _build_candidate_rows, _evaluate_scope_with_context, _materialize_symbol_truth
from .atp_companion_us_late_pocket_refinement import LatePocketControl, _apply_control as _apply_us_late_control
from ..research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from ..research.trend_participation.experiment_configs import ExecutionRealismConfig, config_payload
from ..research.trend_participation.performance_validation import _trade_metrics


DEFAULT_OUTPUT_ROOT = Path("outputs/reports/atp_gc_production_track_execution_realism")
PRIMARY_TARGET_ID = "atp_companion_v1__candidate_gc_asia_us"


@dataclass(frozen=True)
class RealismCase:
    case_id: str
    label: str
    config: ExecutionRealismConfig | None = None
    fee_per_fill: float = 0.0
    slippage_per_fill: float = 0.0
    confirm_halt_next_bar: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-gc-production-track-execution-realism")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    return value


def _target() -> EvaluationTarget:
    return next(target for target in build_targets() if target.target_id == PRIMARY_TARGET_ID)


def _package_rows(*, target: EvaluationTarget, source_db: Path):
    bar_source_index = _discover_best_sources(symbols={target.symbol}, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    coverage_start, coverage_end = _shared_1m_coverage(sqlite_path=source_db, instruments=("MGC", "GC"))
    symbol_truth = _materialize_symbol_truth(
        source_db=source_db,
        symbol=target.symbol,
        bar_source_index=bar_source_index,
        start_timestamp=coverage_start,
        end_timestamp=coverage_end,
    )
    scope = _evaluate_scope_with_context(
        symbol_truth=symbol_truth,
        allowed_sessions=tuple(target.allowed_sessions),
        point_value=float(target.point_value),
    )
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    baseline_rows = _build_candidate_rows(target=target, scope=scope, candidate_defs=candidate_defs)
    us_late_control = LatePocketControl(
        control_id="us_late_tighter_adverse",
        label="US_LATE 2-bar no-traction 0.25R + adverse 0.65R",
        min_favorable_excursion_r=0.25,
        adverse_excursion_abort_r=0.65,
        logic_mode="all",
    )
    us_late_rows, us_late_state = _apply_us_late_control(
        target=target,
        scope=scope,
        baseline_rows_by_trade_id={str(row["trade_id"]): row for row in baseline_rows},
        control=us_late_control,
        candidate_defs=candidate_defs,
    )
    governance = DrawdownGovernanceControl(
        control_id="halt_only_3000",
        label="Halt-only warning at $3,000",
        threshold_cash=3000.0,
        mode="halt_only",
    )
    clipped_windows = _clip_windows_to_rows(rows=us_late_rows, base_trade_windows_by_id=scope.trade_windows_by_id)
    package_rows, governance_state = _apply_drawdown_control(
        rows=us_late_rows,
        trade_windows_by_id=clipped_windows,
        control=governance,
    )
    return scope, baseline_rows, package_rows, us_late_state, governance_state


def _apply_friction(rows: Sequence[dict[str, Any]], *, fee_per_fill: float, slippage_per_fill: float) -> list[dict[str, Any]]:
    adjusted: list[dict[str, Any]] = []
    per_fill_drag = float(fee_per_fill) + float(slippage_per_fill)
    for row in rows:
        next_row = dict(row)
        core_drag = per_fill_drag * 2.0
        add_drag = per_fill_drag * 2.0 if bool(row.get("added")) else 0.0
        next_row["trade_pnl_cash"] = round(float(row.get("trade_pnl_cash") or 0.0) - core_drag, 6)
        next_row["add_pnl_cash"] = round(float(row.get("add_pnl_cash") or 0.0) - add_drag, 6)
        next_row["pnl_cash"] = round(float(next_row["trade_pnl_cash"]) + float(next_row["add_pnl_cash"]), 6)
        next_row["execution_realism"] = {
            "fee_per_fill": fee_per_fill,
            "slippage_per_fill": slippage_per_fill,
            "core_friction_drag_cash": round(core_drag, 6),
            "add_friction_drag_cash": round(add_drag, 6),
        }
        adjusted.append(next_row)
    return adjusted


def _apply_halt_only_with_next_bar_confirmation(
    *,
    rows: Sequence[dict[str, Any]],
    trade_windows_by_id: dict[str, list[Any]],
    threshold_cash: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    halted = False
    session_realized = 0.0
    threshold_breach_count = 0
    halted_day_count = 0
    pnl_saved_after_breach = 0.0
    pnl_lost_after_breach = 0.0
    executed: list[dict[str, Any]] = []
    current_session = None

    for row in sorted(rows, key=lambda item: (str(item.get("entry_ts")), str(item.get("trade_id")))):
        session_key = str((row.get("entry_ts") or row.get("exit_ts")).date().isoformat())
        if session_key != current_session:
            current_session = session_key
            halted = False
            session_realized = 0.0
        if halted:
            pnl_lost_after_breach = round(pnl_lost_after_breach + float(row.get("pnl_cash") or 0.0), 4)
            continue
        bars = list(trade_windows_by_id.get(str(row.get("trade_id"))) or [])
        confirmed = False
        for index, bar in enumerate(bars[:-1]):
            entry_price = float(row.get("entry_price") or 0.0)
            point_value = float(row.get("point_value") or 1.0)
            mtm = (float(bar.close) - entry_price) * point_value
            next_mtm = (float(bars[index + 1].close) - entry_price) * point_value
            if session_realized + mtm <= -abs(threshold_cash) and session_realized + next_mtm <= -abs(threshold_cash):
                confirmed = True
                threshold_breach_count += 1
                halted = True
                halted_day_count += 1
                break
        executed.append(dict(row))
        session_realized = round(session_realized + float(row.get("pnl_cash") or 0.0), 4)
        if confirmed:
            remainder = [
                candidate
                for candidate in rows
                if str((candidate.get("entry_ts") or candidate.get("exit_ts")).date().isoformat()) == session_key
                and str(candidate.get("entry_ts")) > str(row.get("entry_ts"))
            ]
            pnl_saved_after_breach = round(
                pnl_saved_after_breach + sum(float(candidate.get("pnl_cash") or 0.0) for candidate in remainder),
                4,
            )
    return executed, {
        "threshold_breach_count": threshold_breach_count,
        "halted_day_count": halted_day_count,
        "pnl_saved_after_breach_cash": pnl_saved_after_breach,
        "pnl_lost_after_breach_cash": pnl_lost_after_breach,
    }


def _cases() -> list[RealismCase]:
    return [
        RealismCase(case_id="package_exact", label="Exact admitted package", config=ExecutionRealismConfig()),
        RealismCase(
            case_id="package_light_friction",
            label="Package + light friction",
            config=ExecutionRealismConfig(fee_per_fill=1.25, slippage_per_fill=5.0),
            fee_per_fill=1.25,
            slippage_per_fill=5.0,
        ),
        RealismCase(
            case_id="package_stressed_friction",
            label="Package + stressed friction",
            config=ExecutionRealismConfig(fee_per_fill=2.50, slippage_per_fill=10.0),
            fee_per_fill=2.50,
            slippage_per_fill=10.0,
        ),
        RealismCase(
            case_id="package_halt_confirm_next_bar",
            label="Package + halt recognition next bar",
            config=ExecutionRealismConfig(confirm_halt_next_bar=True),
            confirm_halt_next_bar=True,
        ),
        RealismCase(
            case_id="package_light_friction_halt_confirm_next_bar",
            label="Package + light friction + halt recognition next bar",
            config=ExecutionRealismConfig(fee_per_fill=1.25, slippage_per_fill=5.0, confirm_halt_next_bar=True),
            fee_per_fill=1.25,
            slippage_per_fill=5.0,
            confirm_halt_next_bar=True,
        ),
    ]


def run_execution_realism(*, source_db: Path, output_dir: Path) -> dict[str, Path]:
    started = perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    target = _target()
    scope, baseline_rows, package_rows, us_late_state, governance_state = _package_rows(target=target, source_db=source_db)
    baseline_metrics = _trade_metrics(package_rows, bar_count=scope.bar_count)
    baseline_episodes = _drawdown_episodes(rows=package_rows)
    baseline_worst = float(baseline_episodes[0]["peak_to_trough_loss"]) if baseline_episodes else 0.0

    result_rows: list[dict[str, Any]] = []
    for case in _cases():
        case_started = perf_counter()
        rows = [dict(row) for row in package_rows]
        realism_state: dict[str, Any] = {}
        if case.confirm_halt_next_bar:
            rows, realism_state = _apply_halt_only_with_next_bar_confirmation(
                rows=rows,
                trade_windows_by_id=_clip_windows_to_rows(rows=rows, base_trade_windows_by_id=scope.trade_windows_by_id),
                threshold_cash=3000.0,
            )
        if case.fee_per_fill or case.slippage_per_fill:
            rows = _apply_friction(rows, fee_per_fill=case.fee_per_fill, slippage_per_fill=case.slippage_per_fill)
        metrics = _trade_metrics(rows, bar_count=scope.bar_count)
        episodes = _drawdown_episodes(rows=rows)
        worst = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
        result_rows.append(
            {
                "case_id": case.case_id,
                "label": case.label,
                "config": config_payload(case.config or ExecutionRealismConfig()),
                "fee_per_fill": case.fee_per_fill,
                "slippage_per_fill": case.slippage_per_fill,
                "confirm_halt_next_bar": case.confirm_halt_next_bar,
                "metrics": metrics,
                "worst_drawdown_episode_loss": round(worst, 4),
                "delta_vs_exact_package": {
                    "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 4),
                    "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]), 4),
                    "profit_factor_delta": round(float(metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]), 4),
                    "worst_drawdown_episode_delta": round(worst - baseline_worst, 4),
                },
                "realism_state": realism_state,
                "wall_time_seconds": round(perf_counter() - case_started, 6),
            }
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "target": {
            "target_id": target.target_id,
            "label": target.label,
            "symbol": target.symbol,
        },
        "source_db": str(source_db),
        "source_date_span": {
            "start": _shared_1m_coverage(sqlite_path=source_db, instruments=("MGC", "GC"))[0].isoformat(),
            "end": _shared_1m_coverage(sqlite_path=source_db, instruments=("MGC", "GC"))[1].isoformat(),
        },
        "package_definition": {
            "us_late_control": {
                "min_favorable_excursion_r": 0.25,
                "adverse_excursion_abort_r": 0.65,
            },
            "outer_governance": "halt_only_3000",
        },
        "package_baseline_metrics": baseline_metrics,
        "package_baseline_governance": governance_state,
        "package_baseline_us_late_state": us_late_state,
        "rows": result_rows,
        "wall_time_seconds": round(perf_counter() - started, 6),
    }
    json_path = output_dir / "atp_gc_production_track_execution_realism.json"
    md_path = output_dir / "atp_gc_production_track_execution_realism.md"
    csv_path = output_dir / "atp_gc_production_track_execution_realism.csv"
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    md_lines = [
        "# ATP GC Production-Track Execution Realism",
        "",
        f"- Generated: `{payload['generated_at']}`",
        f"- Target: `{target.target_id}`",
        f"- Wall time: `{payload['wall_time_seconds']}`",
        "",
        "## Rows",
    ]
    for row in result_rows:
        md_lines.append(
            f"- `{row['case_id']}` net=`{row['metrics']['net_pnl_cash']}` dd=`{row['metrics']['max_drawdown']}` pf=`{row['metrics']['profit_factor']}` delta_net=`{row['delta_vs_exact_package']['net_pnl_cash_delta']}`"
        )
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "case_id",
                "label",
                "fee_per_fill",
                "slippage_per_fill",
                "confirm_halt_next_bar",
                "net_pnl_cash",
                "profit_factor",
                "max_drawdown",
                "worst_drawdown_episode_loss",
                "net_pnl_cash_delta",
                "max_drawdown_delta",
                "profit_factor_delta",
                "wall_time_seconds",
            ],
        )
        writer.writeheader()
        for row in result_rows:
            writer.writerow(
                {
                    "case_id": row["case_id"],
                    "label": row["label"],
                    "fee_per_fill": row["fee_per_fill"],
                    "slippage_per_fill": row["slippage_per_fill"],
                    "confirm_halt_next_bar": row["confirm_halt_next_bar"],
                    "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                    "profit_factor": row["metrics"]["profit_factor"],
                    "max_drawdown": row["metrics"]["max_drawdown"],
                    "worst_drawdown_episode_loss": row["worst_drawdown_episode_loss"],
                    "net_pnl_cash_delta": row["delta_vs_exact_package"]["net_pnl_cash_delta"],
                    "max_drawdown_delta": row["delta_vs_exact_package"]["max_drawdown_delta"],
                    "profit_factor_delta": row["delta_vs_exact_package"]["profit_factor_delta"],
                    "wall_time_seconds": row["wall_time_seconds"],
                }
            )
    return {"json": json_path, "markdown": md_path, "csv": csv_path}


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db)
    output_dir = Path(args.output_dir) if args.output_dir else (Path.cwd() / DEFAULT_OUTPUT_ROOT / datetime.now(UTC).strftime("%Y%m%d_%H%M%S"))
    artifacts = run_execution_realism(source_db=source_db, output_dir=output_dir)
    registry_result = register_atp_report_output(
        strategy_variant="gc_production_track_execution_realism",
        payload_json_path=Path(artifacts["json"]),
        artifacts=artifacts,
    )
    print(json.dumps({key: str(value) for key, value in artifacts.items()}, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
