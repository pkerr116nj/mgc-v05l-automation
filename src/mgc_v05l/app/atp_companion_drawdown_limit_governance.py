"""ATP Companion naive drawdown-limit governance review over materialized truth."""

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
from zoneinfo import ZoneInfo

from .atp_companion_failure_governance_review import _drawdown_episodes
from .atp_companion_full_history_review import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SOURCE_DB,
    EvaluationTarget,
    _discover_best_sources,
    _json_ready,
    _materialize_symbol_truth,
    _serialize_datetime,
    _shared_1m_coverage,
    build_targets,
)
from .atp_experiment_registry import register_atp_report_output
from .atp_companion_us_fast_fail_review import (
    _build_candidate_rows,
    _evaluate_scope_with_context,
    _target_hash,
)
from ..research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from ..research.trend_participation.experiment_configs import DrawdownGovernanceConfig as DrawdownGovernanceConfigModel, config_payload
from ..research.trend_participation.performance_validation import _trade_metrics

NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class DrawdownGovernanceControl:
    control_id: str
    label: str
    threshold_cash: float | None = None
    mode: str = "none"


TARGET_IDS = {
    "atp_companion_v1__benchmark_mgc_asia_us",
    "atp_companion_v1__promotion_1_075r_favorable_only",
    "atp_companion_v1__candidate_gc_asia_us",
    "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-drawdown-limit-governance")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _targets() -> list[EvaluationTarget]:
    return [target for target in build_targets() if target.target_id in TARGET_IDS]


def _controls() -> list[DrawdownGovernanceControl]:
    rows = [DrawdownGovernanceControl(control_id="none", label="No drawdown governance")]
    for threshold in (1500.0, 2000.0, 2500.0, 3000.0):
        cash_label = f"${int(threshold):,}"
        rows.append(
            DrawdownGovernanceControl(
                control_id=f"halt_only_{int(threshold)}",
                label=f"Halt-only warning at {cash_label}",
                threshold_cash=threshold,
                mode="halt_only",
            )
        )
        rows.append(
            DrawdownGovernanceControl(
                control_id=f"flatten_and_halt_{int(threshold)}",
                label=f"Flatten-and-halt at {cash_label}",
                threshold_cash=threshold,
                mode="flatten_and_halt",
            )
        )
    return rows


def _control_hash(controls: Sequence[DrawdownGovernanceControl]) -> str:
    payload = [_control_config_payload(control) for control in controls]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _control_config_payload(control: DrawdownGovernanceControl) -> dict[str, Any]:
    threshold_cash = float(control.threshold_cash or 0.0)
    return dict(
        config_payload(
            DrawdownGovernanceConfigModel(
                threshold_cash=threshold_cash,
                mode=str(control.mode),
            )
        )
    )


def _session_date_key(value: datetime | str | None) -> str:
    if value is None:
        return "UNKNOWN"
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value))
    return value.astimezone(NY_TZ).date().isoformat()


def _sign_for_side(side: Any) -> float:
    normalized = str(side or "").upper()
    return -1.0 if normalized in {"SHORT", "SELL"} else 1.0


def _bar_close_price(bar: Any) -> float:
    return float(getattr(bar, "close", None) if hasattr(bar, "close") else bar.close)


def _bar_high_price(bar: Any) -> float:
    return float(getattr(bar, "high", None) if hasattr(bar, "high") else bar.high)


def _bar_low_price(bar: Any) -> float:
    return float(getattr(bar, "low", None) if hasattr(bar, "low") else bar.low)


def _trade_mark_to_market(
    *,
    row: dict[str, Any],
    price: float,
    timestamp: datetime,
) -> dict[str, Any]:
    sign = _sign_for_side(row.get("side"))
    point_value = float(row.get("point_value") or 1.0)
    entry_price = float(row.get("entry_price") or row.get("position_entry_price") or 0.0)
    core_pnl_cash = sign * (float(price) - entry_price) * point_value
    add_active = False
    add_pnl_cash = 0.0
    add_entry_ts = row.get("add_entry_ts")
    add_exit_ts = row.get("add_exit_ts")
    add_entry_price = row.get("add_entry_price")
    if (
        row.get("added")
        and add_entry_ts is not None
        and add_entry_price is not None
        and timestamp >= add_entry_ts
        and (add_exit_ts is None or timestamp <= add_exit_ts)
    ):
        add_active = True
        add_pnl_cash = sign * (float(price) - float(add_entry_price)) * point_value
    return {
        "combined_pnl_cash": round(core_pnl_cash + add_pnl_cash, 6),
        "trade_pnl_cash": round(core_pnl_cash, 6),
        "add_pnl_cash": round(add_pnl_cash, 6),
        "add_active": add_active,
    }


def _build_flattened_row(
    *,
    row: dict[str, Any],
    used_bars: Sequence[Any],
    exit_ts: datetime,
    exit_price: float,
) -> dict[str, Any]:
    mtm = _trade_mark_to_market(row=row, price=exit_price, timestamp=exit_ts)
    sign = _sign_for_side(row.get("side"))
    entry_price = float(row.get("entry_price") or row.get("position_entry_price") or 0.0)
    if sign > 0:
        mfe_points = max((_bar_high_price(bar) - entry_price for bar in used_bars), default=0.0)
        mae_points = max((entry_price - _bar_low_price(bar) for bar in used_bars), default=0.0)
    else:
        mfe_points = max((entry_price - _bar_low_price(bar) for bar in used_bars), default=0.0)
        mae_points = max((_bar_high_price(bar) - entry_price for bar in used_bars), default=0.0)
    next_row = dict(row)
    next_row.update(
        {
            "exit_ts": exit_ts,
            "exit_price": float(exit_price),
            "pnl_cash": round(mtm["combined_pnl_cash"], 6),
            "trade_pnl_cash": round(mtm["trade_pnl_cash"], 6),
            "add_pnl_cash": round(mtm["add_pnl_cash"], 6),
            "added": bool(mtm["add_active"]),
            "add_exit_ts": exit_ts if mtm["add_active"] else None,
            "hold_minutes": float(len(used_bars) + 1),
            "bars_held_1m": int(len(used_bars) + 1),
            "mfe_points": round(mfe_points, 6),
            "mae_points": round(mae_points, 6),
            "exit_reason": f"governance_{next_row.get('exit_reason') or 'flatten_and_halt'}",
        }
    )
    if not mtm["add_active"]:
        next_row["add_entry_ts"] = None
        next_row["add_entry_price"] = None
        next_row["add_hold_minutes"] = 0.0
        next_row["add_reason"] = "not_triggered_before_governance_flatten"
        next_row["add_price_quality_state"] = None
    return next_row


def _rows_by_session(rows: Sequence[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    ordered = sorted(rows, key=lambda row: (str(row.get("entry_ts")), str(row.get("trade_id"))))
    for row in ordered:
        grouped.setdefault(_session_date_key(row.get("entry_ts")), []).append(row)
    return grouped


def _apply_drawdown_control(
    *,
    rows: Sequence[dict[str, Any]],
    trade_windows_by_id: dict[str, list[Any]],
    control: DrawdownGovernanceControl,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if control.mode == "none" or control.threshold_cash is None:
        return [dict(row) for row in rows], {
            "threshold_breach_count": 0,
            "halted_day_count": 0,
            "halted_session_segment_count": 0,
            "forced_flatten_count": 0,
            "pnl_saved_after_breach_cash": 0.0,
            "pnl_lost_after_breach_cash": 0.0,
            "false_positive_recovery_count": 0,
            "false_positive_pnl_cost_cash": 0.0,
            "material_recovery_threshold_cash": 0.0,
        }

    threshold_cash = abs(float(control.threshold_cash))
    material_recovery_threshold_cash = max(250.0, threshold_cash * 0.25)
    executed_rows: list[dict[str, Any]] = []
    threshold_breach_count = 0
    forced_flatten_count = 0
    saved_after_breach = 0.0
    lost_after_breach = 0.0
    false_positive_recovery_count = 0
    false_positive_pnl_cost = 0.0
    halted_days: set[str] = set()
    halted_session_segments: set[tuple[str, str]] = set()

    for session_date, session_rows in _rows_by_session(rows).items():
        session_realized = 0.0
        halted = False
        for index, row in enumerate(session_rows):
            if halted:
                halted_session_segments.add((session_date, str(row.get("session_segment") or "UNKNOWN")))
                continue
            trade_windows = list(trade_windows_by_id.get(str(row.get("trade_id"))) or [])
            breach_bar_index: int | None = None
            breach_bar: Any | None = None
            mark_at_breach: dict[str, Any] | None = None
            for bar_index, bar in enumerate(trade_windows):
                mark = _trade_mark_to_market(row=row, price=_bar_close_price(bar), timestamp=bar.end_ts)
                if (session_realized + float(mark["combined_pnl_cash"])) <= -threshold_cash:
                    breach_bar_index = bar_index
                    breach_bar = bar
                    mark_at_breach = mark
                    break
            if breach_bar_index is None or breach_bar is None or mark_at_breach is None:
                executed_rows.append(dict(row))
                session_realized = round(session_realized + float(row.get("pnl_cash") or 0.0), 6)
                continue

            threshold_breach_count += 1
            halted_days.add(session_date)
            halted_session_segments.add((session_date, str(row.get("session_segment") or "UNKNOWN")))
            future_trade_pnl = sum(float(future.get("pnl_cash") or 0.0) for future in session_rows[index + 1 :])
            baseline_remainder = round(float(row.get("pnl_cash") or 0.0) + future_trade_pnl, 6)
            if control.mode == "halt_only":
                overlay_row = dict(row)
                overlay_remainder = round(float(row.get("pnl_cash") or 0.0), 6)
            else:
                used_bars = trade_windows[: breach_bar_index + 1]
                overlay_row = _build_flattened_row(
                    row=row,
                    used_bars=used_bars,
                    exit_ts=breach_bar.end_ts,
                    exit_price=_bar_close_price(breach_bar),
                )
                overlay_remainder = round(float(overlay_row.get("pnl_cash") or 0.0), 6)
                forced_flatten_count += 1

            remainder_delta = round(overlay_remainder - baseline_remainder, 6)
            saved_after_breach = round(saved_after_breach + max(0.0, remainder_delta), 6)
            lost_after_breach = round(lost_after_breach + max(0.0, -remainder_delta), 6)
            if baseline_remainder > material_recovery_threshold_cash and remainder_delta < 0.0:
                false_positive_recovery_count += 1
                false_positive_pnl_cost = round(false_positive_pnl_cost + abs(remainder_delta), 6)

            executed_rows.append(overlay_row)
            session_realized = round(session_realized + float(overlay_row.get("pnl_cash") or 0.0), 6)
            halted = True

    summary = {
        "threshold_breach_count": threshold_breach_count,
        "halted_day_count": len(halted_days),
        "halted_session_segment_count": len(halted_session_segments),
        "forced_flatten_count": forced_flatten_count,
        "pnl_saved_after_breach_cash": round(saved_after_breach, 4),
        "pnl_lost_after_breach_cash": round(lost_after_breach, 4),
        "false_positive_recovery_count": false_positive_recovery_count,
        "false_positive_pnl_cost_cash": round(false_positive_pnl_cost, 4),
        "material_recovery_threshold_cash": round(material_recovery_threshold_cash, 4),
    }
    ordered_rows = sorted(executed_rows, key=lambda row: (str(row.get("entry_ts")), str(row.get("trade_id"))))
    return ordered_rows, summary


def _review_targets_for_run() -> list[EvaluationTarget]:
    wanted = {
        "atp_companion_v1__candidate_gc_asia_us",
        "atp_companion_v1__benchmark_mgc_asia_us",
        "atp_companion_v1__promotion_1_075r_favorable_only",
        "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
    }
    return [target for target in _targets() if target.target_id in wanted]


def _delta_vs_baseline(metrics: dict[str, Any], baseline_metrics: dict[str, Any], *, worst_episode: float, baseline_worst: float) -> dict[str, Any]:
    return {
        "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 4),
        "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(baseline_metrics["max_drawdown"]), 4),
        "profit_factor_delta": round(float(metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]), 4),
        "win_rate_delta": round(float(metrics["win_rate"]) - float(baseline_metrics["win_rate"]), 4),
        "worst_drawdown_episode_delta": round(float(worst_episode) - float(baseline_worst), 4),
    }


def _ranking_score(row: dict[str, Any]) -> tuple[float, float, float, float]:
    delta = row["delta_vs_baseline"]
    return (
        float(delta["max_drawdown_delta"]) * -1.0,
        float(delta["net_pnl_cash_delta"]),
        float(row["governance"]["pnl_lost_after_breach_cash"]) * -1.0,
        float(row["governance"]["forced_flatten_count"]) * -1.0,
    )


def run_drawdown_limit_governance_review(
    *,
    source_db: Path,
    output_dir: Path,
    start_timestamp: datetime | None = None,
    end_timestamp: datetime | None = None,
) -> dict[str, Any]:
    started = perf_counter()
    targets = _review_targets_for_run()
    controls = _controls()
    target_hashes = {target.target_id: _target_hash(target) for target in targets}
    control_hash = _control_hash(controls)

    discovery_started = perf_counter()
    source_index = _discover_best_sources(
        symbols=sorted({target.symbol for target in targets}),
        timeframes={"1m", "5m"},
        sqlite_paths=[source_db],
    )
    discovery_seconds = perf_counter() - discovery_started

    coverage_started = perf_counter()
    run_start, run_end = _shared_1m_coverage(sqlite_path=source_db, instruments=tuple(sorted({target.symbol for target in targets})))
    coverage_seconds = perf_counter() - coverage_started
    if start_timestamp is not None:
        run_start = max(run_start, start_timestamp)
    if end_timestamp is not None:
        run_end = min(run_end, end_timestamp)

    symbol_truths: dict[str, Any] = {}
    for symbol in sorted({target.symbol for target in targets}):
        symbol_truths[symbol] = _materialize_symbol_truth(
            source_db=source_db,
            symbol=symbol,
            bar_source_index=source_index,
            start_timestamp=run_start,
            end_timestamp=run_end,
        )

    scope_truths: dict[tuple[str, tuple[str, ...]], Any] = {}
    for target in targets:
        key = (target.symbol, target.allowed_sessions)
        if key not in scope_truths:
            scope_truths[key] = _evaluate_scope_with_context(
                symbol_truth=symbol_truths[target.symbol],
                allowed_sessions=target.allowed_sessions,
                point_value=target.point_value,
            )

    build_rows_started = perf_counter()
    baseline_rows_by_target: dict[str, list[dict[str, Any]]] = {}
    baseline_metrics_by_target: dict[str, dict[str, Any]] = {}
    baseline_worst_by_target: dict[str, float] = {}
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
    for target in targets:
        scope = scope_truths[(target.symbol, target.allowed_sessions)]
        rows = _build_candidate_rows(
            target=target,
            scope=scope,
            candidate_defs=candidate_defs,
        )
        baseline_rows_by_target[target.target_id] = rows
        metrics = _trade_metrics(rows, bar_count=scope.bar_count)
        baseline_metrics_by_target[target.target_id] = metrics
        episodes = _drawdown_episodes(rows=rows)
        baseline_worst_by_target[target.target_id] = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
    build_rows_seconds = perf_counter() - build_rows_started

    results: list[dict[str, Any]] = []
    for target in targets:
        scope = scope_truths[(target.symbol, target.allowed_sessions)]
        baseline_rows = baseline_rows_by_target[target.target_id]
        baseline_metrics = baseline_metrics_by_target[target.target_id]
        baseline_worst = baseline_worst_by_target[target.target_id]
        for control in controls:
            control_started = perf_counter()
            governed_rows, governance = _apply_drawdown_control(
                rows=baseline_rows,
                trade_windows_by_id=scope.trade_windows_by_id,
                control=control,
            )
            metrics = _trade_metrics(governed_rows, bar_count=scope.bar_count)
            episodes = _drawdown_episodes(rows=governed_rows)
            worst_episode = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
            results.append(
                {
                    "target_id": target.target_id,
                    "label": target.label,
                    "symbol": target.symbol,
                    "allowed_sessions": list(target.allowed_sessions),
                    "control_id": control.control_id,
                    "control_label": control.label,
                    "control_mode": control.mode,
                    "threshold_cash": control.threshold_cash,
                    "config": _control_config_payload(control),
                    "metrics": metrics,
                    "governance": governance,
                    "worst_drawdown_episode_loss": round(worst_episode, 4),
                    "delta_vs_baseline": _delta_vs_baseline(
                        metrics,
                        baseline_metrics,
                        worst_episode=worst_episode,
                        baseline_worst=baseline_worst,
                    ),
                    "wall_time_seconds": round(perf_counter() - control_started, 6),
                }
            )

    ranking = sorted(
        [row for row in results if row["control_id"] != "none"],
        key=_ranking_score,
        reverse=True,
    )

    payload = {
        "study": "ATP Companion naive drawdown-limit governance review",
        "manifest": {
            "artifact_version": "1",
            "generated_at": datetime.now(UTC).isoformat(),
            "source_db": str(source_db.resolve()),
            "source_date_span": {
                "start_timestamp": _serialize_datetime(run_start),
                "end_timestamp": _serialize_datetime(run_end),
            },
            "target_hashes": target_hashes,
            "config_hash": control_hash,
            "provenance": {
                "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
                "scope_note": "Single-lane replay means desk-level and lane-level realized+unrealized session-loss gates are numerically identical here; this pass compares behavior modes rather than duplicating identical scope rows.",
                "benchmark_semantics_changed": False,
            },
            "timing": {
                "total_wall_seconds": round(perf_counter() - started, 6),
                "source_discovery_seconds": round(discovery_seconds, 6),
                "coverage_seconds": round(coverage_seconds, 6),
                "candidate_row_build_seconds": round(build_rows_seconds, 6),
            },
        },
        "ranking": ranking,
        "results": results,
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "atp_drawdown_limit_governance_review.json"
    md_path = output_dir / "atp_drawdown_limit_governance_review.md"
    csv_path = output_dir / "atp_drawdown_limit_governance_matrix.csv"

    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "target_id",
                "control_id",
                "control_mode",
                "threshold_cash",
                "net_pnl_cash",
                "max_drawdown",
                "worst_drawdown_episode_loss",
                "profit_factor",
                "win_rate",
                "threshold_breach_count",
                "halted_day_count",
                "halted_session_segment_count",
                "forced_flatten_count",
                "pnl_saved_after_breach_cash",
                "pnl_lost_after_breach_cash",
                "false_positive_recovery_count",
                "false_positive_pnl_cost_cash",
                "delta_vs_baseline_net_pnl_cash",
                "delta_vs_baseline_max_drawdown",
                "delta_vs_baseline_worst_episode",
                "wall_time_seconds",
            ],
        )
        writer.writeheader()
        for row in results:
            writer.writerow(
                {
                    "target_id": row["target_id"],
                    "control_id": row["control_id"],
                    "control_mode": row["control_mode"],
                    "threshold_cash": row["threshold_cash"],
                    "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                    "max_drawdown": row["metrics"]["max_drawdown"],
                    "worst_drawdown_episode_loss": row["worst_drawdown_episode_loss"],
                    "profit_factor": row["metrics"]["profit_factor"],
                    "win_rate": row["metrics"]["win_rate"],
                    "threshold_breach_count": row["governance"]["threshold_breach_count"],
                    "halted_day_count": row["governance"]["halted_day_count"],
                    "halted_session_segment_count": row["governance"]["halted_session_segment_count"],
                    "forced_flatten_count": row["governance"]["forced_flatten_count"],
                    "pnl_saved_after_breach_cash": row["governance"]["pnl_saved_after_breach_cash"],
                    "pnl_lost_after_breach_cash": row["governance"]["pnl_lost_after_breach_cash"],
                    "false_positive_recovery_count": row["governance"]["false_positive_recovery_count"],
                    "false_positive_pnl_cost_cash": row["governance"]["false_positive_pnl_cost_cash"],
                    "delta_vs_baseline_net_pnl_cash": row["delta_vs_baseline"]["net_pnl_cash_delta"],
                    "delta_vs_baseline_max_drawdown": row["delta_vs_baseline"]["max_drawdown_delta"],
                    "delta_vs_baseline_worst_episode": row["delta_vs_baseline"]["worst_drawdown_episode_delta"],
                    "wall_time_seconds": row["wall_time_seconds"],
                }
            )

    lines = [
        "# ATP Companion Naive Drawdown-Limit Governance Review",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Config hash: `{control_hash}`",
        "- Scope note: `Single-lane replay means lane-level and desk-level realized+unrealized session-loss gates are equivalent here; behavior mode is the meaningful comparison.`",
        "",
        "## Ranked Rows",
        "",
    ]
    for row in ranking[:12]:
        lines.extend(
            [
                f"### {row['label']} :: {row['control_label']}",
                "",
                f"- Net / Max DD / Worst Episode: `{row['metrics']['net_pnl_cash']}` / `{row['metrics']['max_drawdown']}` / `{row['worst_drawdown_episode_loss']}`",
                f"- Delta vs baseline net / DD / worst episode: `{row['delta_vs_baseline']['net_pnl_cash_delta']}` / `{row['delta_vs_baseline']['max_drawdown_delta']}` / `{row['delta_vs_baseline']['worst_drawdown_episode_delta']}`",
                f"- Breaches / Halted days / Forced flattens: `{row['governance']['threshold_breach_count']}` / `{row['governance']['halted_day_count']}` / `{row['governance']['forced_flatten_count']}`",
                f"- Saved after breach / Lost after breach / False-positive cost: `{row['governance']['pnl_saved_after_breach_cash']}` / `{row['governance']['pnl_lost_after_breach_cash']}` / `{row['governance']['false_positive_pnl_cost_cash']}`",
                "",
            ]
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return {
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
            DEFAULT_OUTPUT_ROOT / f"atp_companion_drawdown_limit_governance_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_drawdown_limit_governance_review(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="drawdown_limit_governance",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
