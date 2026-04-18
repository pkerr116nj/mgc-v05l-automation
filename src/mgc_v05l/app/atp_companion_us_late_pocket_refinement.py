"""ATP Companion US_LATE invalidation pocket refinement."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Sequence

from .atp_companion_full_history_review import DEFAULT_OUTPUT_ROOT, DEFAULT_SOURCE_DB, EvaluationTarget, _json_ready
from .atp_experiment_registry import register_atp_report_output
from .atp_companion_us_early_invalidation_refinement import (
    _baseline_rows_by_trade_id,
    _control_hash,
    _review_targets,
)
from .atp_companion_us_fast_fail_review import (
    _build_candidate_rows,
    _discover_best_sources,
    _evaluate_scope_with_context,
    _first_window_stats,
    _is_us_core_fast_fail,
    _materialize_symbol_truth,
    _shared_1m_coverage,
    _target_hash,
    _trade_proxy,
)
from .atp_companion_failure_governance_review import _drawdown_episodes
from ..research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.performance_validation import _trade_metrics


@dataclass(frozen=True)
class LatePocketControl:
    control_id: str
    label: str
    min_favorable_excursion_r: float | None = None
    adverse_excursion_abort_r: float | None = None
    logic_mode: str = "all"
    require_adverse_first_bar: bool = False
    require_no_traction_first_bar: bool = False


TARGET_IDS = {
    "atp_companion_v1__candidate_gc_asia_us",
    "atp_companion_v1__benchmark_mgc_asia_us",
    "atp_companion_v1__promotion_1_075r_favorable_only",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-us-late-pocket-refinement")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _targets() -> list[EvaluationTarget]:
    return [target for target in _review_targets() if target.target_id in TARGET_IDS]


def _controls() -> list[LatePocketControl]:
    return [
        LatePocketControl(control_id="none", label="No US_LATE pocket control"),
        LatePocketControl(
            control_id="us_late_safe_anchor",
            label="US_LATE 2-bar no-traction + adverse (safe anchor)",
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
            logic_mode="all",
        ),
        LatePocketControl(
            control_id="us_late_looser_no_traction",
            label="US_LATE 2-bar no-traction 0.20R + adverse 0.75R",
            min_favorable_excursion_r=0.20,
            adverse_excursion_abort_r=0.75,
            logic_mode="all",
        ),
        LatePocketControl(
            control_id="us_late_tighter_no_traction",
            label="US_LATE 2-bar no-traction 0.30R + adverse 0.75R",
            min_favorable_excursion_r=0.30,
            adverse_excursion_abort_r=0.75,
            logic_mode="all",
        ),
        LatePocketControl(
            control_id="us_late_tighter_adverse",
            label="US_LATE 2-bar no-traction 0.25R + adverse 0.65R",
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.65,
            logic_mode="all",
        ),
        LatePocketControl(
            control_id="us_late_looser_adverse",
            label="US_LATE 2-bar no-traction 0.25R + adverse 0.85R",
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.85,
            logic_mode="all",
        ),
        LatePocketControl(
            control_id="us_late_fail_either_anchor",
            label="US_LATE fail either no-traction 0.25R or adverse 0.75R",
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
            logic_mode="any",
        ),
        LatePocketControl(
            control_id="us_late_adverse_first_then_no_traction",
            label="US_LATE adverse first bar then no-traction by 2 bars",
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
            logic_mode="all",
            require_adverse_first_bar=True,
        ),
        LatePocketControl(
            control_id="us_late_no_traction_first_then_adverse",
            label="US_LATE no-traction first bar then adverse by 2 bars",
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
            logic_mode="all",
            require_no_traction_first_bar=True,
        ),
    ]


def _control_hash_local(controls: Sequence[LatePocketControl]) -> str:
    payload = [
        {
            "control_id": control.control_id,
            "label": control.label,
            "min_favorable_excursion_r": control.min_favorable_excursion_r,
            "adverse_excursion_abort_r": control.adverse_excursion_abort_r,
            "logic_mode": control.logic_mode,
            "require_adverse_first_bar": control.require_adverse_first_bar,
            "require_no_traction_first_bar": control.require_no_traction_first_bar,
        }
        for control in controls
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _late_abort_info(
    *,
    trade: Any,
    trade_bars: Sequence[Any],
    timing_state: Any | None,
    control: LatePocketControl,
) -> dict[str, Any] | None:
    if control.control_id == "none" or str(trade.session_segment) != "US" or not trade_bars:
        return None
    current = trade.entry_ts.timetz().replace(tzinfo=None)
    if current < datetime.strptime("14:00", "%H:%M").time():
        return None
    first1 = _first_window_stats(trade=trade, trade_bars=trade_bars, timing_state=timing_state, bars=1)
    first2 = _first_window_stats(trade=trade, trade_bars=trade_bars, timing_state=timing_state, bars=2)
    no_traction = (
        control.min_favorable_excursion_r is not None
        and float(first2["mfe_r"]) < float(control.min_favorable_excursion_r)
    )
    adverse = (
        control.adverse_excursion_abort_r is not None
        and float(first2["mae_r"]) >= float(control.adverse_excursion_abort_r)
    )
    first1_adverse = (
        control.adverse_excursion_abort_r is not None
        and float(first1["mae_r"]) >= float(control.adverse_excursion_abort_r)
    )
    first1_no_traction = (
        control.min_favorable_excursion_r is not None
        and float(first1["mfe_r"]) < min(float(control.min_favorable_excursion_r), 0.10)
    )
    if control.require_adverse_first_bar and not first1_adverse:
        return None
    if control.require_no_traction_first_bar and not first1_no_traction:
        return None
    reasons = {
        "no_traction": bool(no_traction),
        "adverse_excursion": bool(adverse),
    }
    active = [name for name, ok in reasons.items() if ok]
    if control.logic_mode == "all":
        if not active or len(active) != len(reasons):
            return None
    else:
        if not active:
            return None
    trigger_bar = trade_bars[min(1, len(trade_bars) - 1)]
    exit_price = float(trade.stop_price) if float(trigger_bar.low) <= float(trade.stop_price) else float(trigger_bar.close)
    return {
        "reasons": active,
        "used_bars": list(trade_bars[: min(2, len(trade_bars))]),
        "exit_ts": trigger_bar.end_ts,
        "exit_price": exit_price,
    }


def _apply_control(
    *,
    target: EvaluationTarget,
    scope: Any,
    baseline_rows_by_trade_id: dict[str, dict[str, Any]],
    control: LatePocketControl,
    candidate_defs: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidate = candidate_defs.get(str(target.candidate_id)) if target.candidate_id else None
    rows: list[dict[str, Any]] = []
    reason_counts = {"no_traction": 0, "adverse_excursion": 0}
    reason_net_delta = {"no_traction": 0.0, "adverse_excursion": 0.0}
    harmed_count = 0
    harmed_pnl_cost = 0.0
    flipped_count = 0
    flipped_pnl_cost = 0.0

    for trade_row in scope.trade_rows:
        trade_id = str(trade_row["trade_id"])
        trade = trade_row["trade_record"]
        baseline_row = baseline_rows_by_trade_id[trade_id]
        modified_trade = trade
        modified_bars = list(scope.trade_windows_by_id.get(trade_id) or [])
        if control.control_id != "none":
            abort_info = _late_abort_info(
                trade=trade,
                trade_bars=modified_bars,
                timing_state=scope.timing_states_by_trade_id.get(trade_id),
                control=control,
            )
            if abort_info is not None:
                modified_trade = _trade_proxy(
                    trade=trade,
                    exit_ts=abort_info["exit_ts"],
                    exit_price=float(abort_info["exit_price"]),
                    exit_reason=f"overlay_{control.control_id}",
                    point_value=target.point_value,
                    used_bars=abort_info["used_bars"],
                )
                modified_bars = abort_info["used_bars"]
                baseline_pnl = float(baseline_row.get("pnl_cash") or 0.0)
                overlay_pnl = float(modified_trade.pnl_cash)
                delta = round(overlay_pnl - baseline_pnl, 4)
                for reason in abort_info["reasons"]:
                    reason_counts[reason] += 1
                    reason_net_delta[reason] = round(reason_net_delta[reason] + delta, 4)
                if baseline_pnl > 0.0 and overlay_pnl < baseline_pnl:
                    harmed_count += 1
                    harmed_pnl_cost = round(harmed_pnl_cost + (baseline_pnl - overlay_pnl), 4)
                    if overlay_pnl <= 0.0:
                        flipped_count += 1
                        flipped_pnl_cost = round(flipped_pnl_cost + baseline_pnl, 4)

        if candidate is None:
            row = {
                "trade_id": trade_id,
                "entry_ts": modified_trade.entry_ts,
                "exit_ts": modified_trade.exit_ts,
                "decision_ts": modified_trade.decision_ts,
                "entry_price": float(modified_trade.entry_price),
                "exit_price": float(modified_trade.exit_price),
                "stop_price": float(modified_trade.stop_price),
                "pnl_cash": float(modified_trade.pnl_cash),
                "mfe_points": float(modified_trade.mfe_points),
                "mae_points": float(modified_trade.mae_points),
                "hold_minutes": float(modified_trade.hold_minutes),
                "bars_held_1m": int(modified_trade.bars_held_1m),
                "side": modified_trade.side,
                "session_segment": modified_trade.session_segment,
                "family": modified_trade.family,
                "exit_reason": modified_trade.exit_reason,
                "added": False,
                "add_pnl_cash": 0.0,
                "add_reason": None,
                "add_price_quality_state": None,
                "point_value": target.point_value,
                "trade_pnl_cash": float(modified_trade.pnl_cash),
            }
        else:
            row = evaluate_promotion_add_candidate(
                trade=modified_trade,
                minute_bars=modified_bars,
                candidate=candidate,
                point_value=target.point_value,
            )
            row["trade_id"] = trade_id
            row["point_value"] = target.point_value
        row["entry_price"] = float(getattr(modified_trade, "entry_price", row.get("position_entry_price") or 0.0))
        row["stop_price"] = float(getattr(modified_trade, "stop_price", row.get("position_entry_price") or 0.0))
        row["trade_pnl_cash"] = float(getattr(modified_trade, "pnl_cash", row.get("trade_pnl_cash") or row.get("pnl_cash") or 0.0))
        rows.append(row)

    return rows, {
        "reason_counts": reason_counts,
        "reason_net_delta": reason_net_delta,
        "good_winners_harmed_count": harmed_count,
        "good_winners_harmed_pnl_cost": harmed_pnl_cost,
        "flipped_count": flipped_count,
        "flipped_pnl_cost": flipped_pnl_cost,
    }


def _metric_row(
    *,
    target: EvaluationTarget,
    control: LatePocketControl,
    rows: Sequence[dict[str, Any]],
    baseline_rows: Sequence[dict[str, Any]],
    safe_anchor: dict[str, Any] | None,
    blunt_anchor: dict[str, Any] | None,
    bar_count: int,
    wall_seconds: float,
    control_state: dict[str, Any],
) -> dict[str, Any]:
    metrics = _trade_metrics(rows, bar_count=bar_count)
    baseline_metrics = _trade_metrics(baseline_rows, bar_count=bar_count)
    us_net = round(sum(float(r.get("pnl_cash") or 0.0) for r in rows if str(r.get("session_segment")) == "US"), 4)
    baseline_us_net = round(sum(float(r.get("pnl_cash") or 0.0) for r in baseline_rows if str(r.get("session_segment")) == "US"), 4)
    episodes = _drawdown_episodes(rows=rows)
    base_episodes = _drawdown_episodes(rows=baseline_rows)
    worst = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
    base_worst = float(base_episodes[0]["peak_to_trough_loss"]) if base_episodes else 0.0
    us_fast_fail = sum(1 for r in rows if _is_us_core_fast_fail(r) and str(r.get("session_segment")) == "US")
    return {
        "target_id": target.target_id,
        "label": target.label,
        "control_id": control.control_id,
        "control_label": control.label,
        "metrics": metrics,
        "us_net_pnl_cash": us_net,
        "us_fast_fail_loser_count": us_fast_fail,
        "worst_drawdown_episode_loss": round(worst, 4),
        "abort_reason_counts": control_state["reason_counts"],
        "abort_reason_net_delta": control_state["reason_net_delta"],
        "good_winners_harmed_count": int(control_state["good_winners_harmed_count"]),
        "good_winners_harmed_pnl_cost": round(float(control_state["good_winners_harmed_pnl_cost"]), 4),
        "false_negative_cost": {
            "flipped_to_nonpositive_count": int(control_state["flipped_count"]),
            "flipped_to_nonpositive_pnl_cash": round(float(control_state["flipped_pnl_cost"]), 4),
        },
        "delta_vs_baseline": {
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 4),
            "us_net_pnl_cash_delta": round(us_net - baseline_us_net, 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]), 4),
            "worst_drawdown_episode_delta": round(worst - base_worst, 4),
        },
        "delta_vs_safe_anchor": None if safe_anchor is None else {
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(safe_anchor["metrics"]["net_pnl_cash"]), 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(safe_anchor["metrics"]["max_drawdown"]), 4),
            "worst_drawdown_episode_delta": round(worst - float(safe_anchor["worst_drawdown_episode_loss"]), 4),
            "false_negative_pnl_delta": round(
                float(control_state["flipped_pnl_cost"]) - float((safe_anchor["false_negative_cost"] or {}).get("good_us_winners_filtered_out_pnl_cash") or 0.0),
                4,
            ),
        },
        "delta_vs_blunt_anchor": None if blunt_anchor is None else {
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(blunt_anchor["metrics"]["net_pnl_cash"]), 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(blunt_anchor["metrics"]["max_drawdown"]), 4),
            "worst_drawdown_episode_delta": round(worst - float(blunt_anchor["worst_drawdown_episode_loss"]), 4),
            "false_negative_pnl_delta": round(
                float(control_state["flipped_pnl_cost"]) - float((blunt_anchor["false_negative_cost"] or {}).get("good_us_winners_filtered_out_pnl_cash") or 0.0),
                4,
            ),
        },
        "wall_time_seconds": round(wall_seconds, 6),
    }


def run_us_late_pocket_refinement(
    *,
    source_db: Path,
    output_dir: Path,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Path]:
    if not source_db.exists():
        raise FileNotFoundError(f"Source DB not found: {source_db}")
    started_at = perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    targets = _targets()
    controls = _controls()
    candidate_defs = {c.candidate_id: c for c in default_atp_promotion_add_candidates()}
    symbol_set = {t.symbol for t in targets}
    bar_source_index = _discover_best_sources(symbols=symbol_set, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_db, instruments=("MGC", "GC"))
    run_start = max(shared_start, start_timestamp) if start_timestamp is not None else shared_start
    run_end = min(shared_end, end_timestamp) if end_timestamp is not None else shared_end

    symbol_truths = {
        symbol: _materialize_symbol_truth(
            source_db=source_db,
            symbol=symbol,
            bar_source_index=bar_source_index,
            start_timestamp=run_start,
            end_timestamp=run_end,
        )
        for symbol in sorted(symbol_set)
    }
    scopes = {
        (target.symbol, target.allowed_sessions): _evaluate_scope_with_context(
            symbol_truth=symbol_truths[target.symbol],
            allowed_sessions=target.allowed_sessions,
            point_value=target.point_value,
        )
        for target in targets
    }

    prior_path = output_dir.parent / "atp_companion_us_early_invalidation_refinement_20260406" / "atp_us_early_invalidation_review.json"
    prior = json.loads(prior_path.read_text()) if prior_path.exists() else {"us_early_invalidation_matrix": []}
    prior_rows = {(r["target_id"], r["control_id"]): r for r in prior["us_early_invalidation_matrix"]}

    results: list[dict[str, Any]] = []
    for target in targets:
        scope = scopes[(target.symbol, target.allowed_sessions)]
        baseline_rows_by_id = _baseline_rows_by_trade_id(target=target, scope=scope, candidate_defs=candidate_defs)
        baseline_rows = list(baseline_rows_by_id.values())
        safe_anchor = prior_rows.get((target.target_id, "us_late_2bar_no_traction_plus_adverse"))
        blunt_anchor = prior_rows.get((target.target_id, "us_2bar_hold_failure_only"))
        for control in controls:
            started = perf_counter()
            rows, state = _apply_control(
                target=target,
                scope=scope,
                baseline_rows_by_trade_id=baseline_rows_by_id,
                control=control,
                candidate_defs=candidate_defs,
            )
            results.append(
                _metric_row(
                    target=target,
                    control=control,
                    rows=rows,
                    baseline_rows=baseline_rows,
                    safe_anchor=safe_anchor,
                    blunt_anchor=blunt_anchor,
                    bar_count=scope.bar_count,
                    wall_seconds=perf_counter() - started,
                    control_state=state,
                )
            )

    ranking = sorted(
        results,
        key=lambda r: (
            float(r["delta_vs_safe_anchor"]["net_pnl_cash_delta"]) if r["delta_vs_safe_anchor"] else float("-inf"),
            -float(r["delta_vs_safe_anchor"]["false_negative_pnl_delta"]) if r["delta_vs_safe_anchor"] else float("-inf"),
            -float(r["delta_vs_baseline"]["max_drawdown_delta"]),
        ),
        reverse=True,
    )

    manifest = {
        "artifact_version": "atp_us_late_pocket_refinement_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_db": str(source_db.resolve()),
        "source_date_span": {
            "start_timestamp": run_start.isoformat(),
            "end_timestamp": run_end.isoformat(),
        },
        "target_hashes": {t.target_id: _target_hash(t) for t in targets},
        "control_hash": _control_hash_local(controls),
        "prior_anchor_review_path": str(prior_path.resolve()) if prior_path.exists() else None,
        "provenance": {
            "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
            "benchmark_semantics_changed": False,
        },
        "total_wall_seconds": round(perf_counter() - started_at, 6),
    }

    payload = {
        "study": "ATP Companion US_LATE pocket refinement",
        "manifest": manifest,
        "results": results,
        "ranking": ranking,
    }

    manifest_path = output_dir / "atp_us_late_pocket_manifest.json"
    json_path = output_dir / "atp_us_late_pocket_review.json"
    md_path = output_dir / "atp_us_late_pocket_review.md"
    csv_path = output_dir / "atp_us_late_pocket_matrix.csv"
    manifest_path.write_text(json.dumps(_json_ready(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        fieldnames = [
            "target_id","control_id","net_pnl_cash","max_drawdown","worst_drawdown_episode_loss",
            "good_winners_harmed_count","good_winners_harmed_pnl_cost","false_negative_count",
            "false_negative_pnl_cash","delta_vs_baseline_net","delta_vs_baseline_dd","delta_vs_safe_net",
            "delta_vs_safe_false_negative","wall_time_seconds"
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in results:
            writer.writerow({
                "target_id": row["target_id"],
                "control_id": row["control_id"],
                "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                "max_drawdown": row["metrics"]["max_drawdown"],
                "worst_drawdown_episode_loss": row["worst_drawdown_episode_loss"],
                "good_winners_harmed_count": row["good_winners_harmed_count"],
                "good_winners_harmed_pnl_cost": row["good_winners_harmed_pnl_cost"],
                "false_negative_count": row["false_negative_cost"]["flipped_to_nonpositive_count"],
                "false_negative_pnl_cash": row["false_negative_cost"]["flipped_to_nonpositive_pnl_cash"],
                "delta_vs_baseline_net": row["delta_vs_baseline"]["net_pnl_cash_delta"],
                "delta_vs_baseline_dd": row["delta_vs_baseline"]["max_drawdown_delta"],
                "delta_vs_safe_net": None if row["delta_vs_safe_anchor"] is None else row["delta_vs_safe_anchor"]["net_pnl_cash_delta"],
                "delta_vs_safe_false_negative": None if row["delta_vs_safe_anchor"] is None else row["delta_vs_safe_anchor"]["false_negative_pnl_delta"],
                "wall_time_seconds": row["wall_time_seconds"],
            })

    lines = [
        "# ATP Companion US_LATE Pocket Refinement",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Shared date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Total wall seconds: `{manifest['total_wall_seconds']}`",
        "",
    ]
    for row in ranking[:12]:
        lines.extend([
            f"## {row['target_id']} / {row['control_id']}",
            f"- Net P&L: `{row['metrics']['net_pnl_cash']}`",
            f"- Max DD / Worst Episode: `{row['metrics']['max_drawdown']}` / `{row['worst_drawdown_episode_loss']}`",
            f"- Harmed winners / cost: `{row['good_winners_harmed_count']}` / `{row['good_winners_harmed_pnl_cost']}`",
            f"- False-negative cost: `{row['false_negative_cost']}`",
            f"- Delta vs baseline: `{row['delta_vs_baseline']}`",
            f"- Delta vs safe anchor: `{row['delta_vs_safe_anchor']}`",
            "",
        ])
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return {
        "manifest_path": manifest_path,
        "json_path": json_path,
        "markdown_path": md_path,
        "comparison_csv_path": csv_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    source_db = Path(args.source_db).resolve()
    if args.output_dir:
        output_dir = Path(args.output_dir).resolve()
    else:
        output_dir = (
            DEFAULT_OUTPUT_ROOT / f"atp_companion_us_late_pocket_refinement_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_us_late_pocket_refinement(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="us_late_pocket_refinement",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({k: str(v) for k, v in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
