"""ATP Companion exit/drawdown experiment matrix over materialized ATP trade truth."""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any, Sequence

from .atp_companion_full_history_review import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_SOURCE_DB,
    EvaluationTarget,
    _base_position_rows,
    _comparison_rows,
    _coverage_row,
    _discover_best_sources,
    _evaluate_materialized_scope,
    _json_ready,
    _materialize_symbol_truth,
    _normalize_session_breakdown,
    _result_payload,
    _serialize_datetime,
    _shared_1m_coverage,
    _trade_windows_by_id,
    build_targets,
)
from .atp_experiment_registry import register_atp_report_output
from ..research.trend_participation.atp_promotion_add_review import (
    _candidate_session_breakdown,
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.experiment_configs import ExitDrawdownOverlayConfig, config_payload
from ..research.trend_participation.performance_validation import _trade_metrics

REPO_ROOT = Path.cwd()


@dataclass(frozen=True)
class OverlayControl:
    control_id: str
    label: str
    exit_mode: str = "none"
    daily_loss_halt_multiple: float | None = None
    session_loser_limit: int | None = None
    disable_us_adds: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-exit-drawdown-matrix")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _build_experiment_targets() -> list[EvaluationTarget]:
    wanted = {
        "atp_companion_v1__benchmark_mgc_asia_us",
        "atp_companion_v1__promotion_1_075r_favorable_only",
        "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
        "atp_companion_v1__candidate_gc_asia_us",
    }
    return [target for target in build_targets() if target.target_id in wanted]


def _build_controls() -> list[OverlayControl]:
    return [
        OverlayControl(control_id="none", label="No exit/drawdown overlay"),
        OverlayControl(
            control_id="daily_loss_halt_2x_avg_loser",
            label="Daily loss halt at 2x average loser cash",
            daily_loss_halt_multiple=2.0,
        ),
        OverlayControl(
            control_id="session_loser_limit_2",
            label="Per-session loser limit: 2 losers",
            session_loser_limit=2,
        ),
        OverlayControl(
            control_id="time_stop_120m",
            label="Stale-trade time stop at 120 minutes",
            exit_mode="time_stop_120m",
        ),
        OverlayControl(
            control_id="profit_lock_1r_breakeven",
            label="Profit lock to breakeven after +1R progress",
            exit_mode="profit_lock_1r_breakeven",
        ),
        OverlayControl(
            control_id="trail_after_1r_half_r",
            label="Trail after +1R with 0.5R lock distance",
            exit_mode="trail_after_1r_half_r",
        ),
        OverlayControl(
            control_id="daily_loss_halt_2x_avg_loser__time_stop_120m",
            label="Daily loss halt + stale-trade time stop",
            exit_mode="time_stop_120m",
            daily_loss_halt_multiple=2.0,
        ),
    ]


def _candidate_defs() -> dict[str, Any]:
    return {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}


def _control_config_payload(control: OverlayControl) -> dict[str, Any]:
    return dict(
        config_payload(
            ExitDrawdownOverlayConfig(
                exit_mode=control.exit_mode,
                daily_loss_halt_multiple=control.daily_loss_halt_multiple,
                session_loser_limit=control.session_loser_limit,
                disable_us_adds=control.disable_us_adds,
            )
        )
    )


def _trade_index_by_timestamp(bars_1m: Sequence[Any]) -> dict[Any, int]:
    return {bar.end_ts: index for index, bar in enumerate(bars_1m)}


def _build_trade_proxy(
    *,
    trade,
    exit_ts: Any,
    exit_price: float,
    exit_reason: str,
    point_value: float,
    used_bars: Sequence[Any],
) -> Any:
    pnl_points = float(exit_price) - float(trade.entry_price)
    return SimpleNamespace(
        entry_ts=trade.entry_ts,
        exit_ts=exit_ts,
        decision_ts=trade.decision_ts,
        entry_price=float(trade.entry_price),
        exit_price=float(exit_price),
        stop_price=float(trade.stop_price),
        pnl_cash=round(pnl_points * point_value, 6),
        hold_minutes=float(len(used_bars) + 1),
        bars_held_1m=len(used_bars) + 1,
        side=trade.side,
        session_segment=trade.session_segment,
        mfe_points=max((float(bar.high) - float(trade.entry_price)) for bar in used_bars) if used_bars else 0.0,
        mae_points=max((float(trade.entry_price) - float(bar.low)) for bar in used_bars) if used_bars else 0.0,
        family=trade.family,
        exit_reason=exit_reason,
    )


def _simulate_exit(
    *,
    trade_row: dict[str, Any],
    trade_bars: Sequence[Any],
    control: OverlayControl,
    point_value: float,
) -> tuple[Any, list[Any]]:
    trade = trade_row["trade_record"]
    if control.exit_mode == "none" or not trade_bars:
        return trade, list(trade_bars)
    risk = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)

    if control.exit_mode == "time_stop_120m":
        limit = 120
        if len(trade_bars) > limit:
            capped_bars = list(trade_bars[:limit])
            exit_bar = capped_bars[-1]
            return (
                _build_trade_proxy(
                    trade=trade,
                    exit_ts=exit_bar.end_ts,
                    exit_price=float(exit_bar.close),
                    exit_reason="overlay_time_stop_120m",
                    point_value=point_value,
                    used_bars=capped_bars,
                ),
                capped_bars,
            )
        return trade, list(trade_bars)

    if control.exit_mode == "profit_lock_1r_breakeven":
        activation_price = float(trade.entry_price) + risk
        activation_index = next(
            (index for index, bar in enumerate(trade_bars) if float(bar.high) >= activation_price),
            None,
        )
        if activation_index is None:
            return trade, list(trade_bars)
        for index in range(activation_index, len(trade_bars)):
            bar = trade_bars[index]
            if float(bar.low) <= float(trade.entry_price):
                used_bars = list(trade_bars[: index + 1])
                return (
                    _build_trade_proxy(
                        trade=trade,
                        exit_ts=bar.end_ts,
                        exit_price=float(trade.entry_price),
                        exit_reason="overlay_profit_lock_1r_breakeven",
                        point_value=point_value,
                        used_bars=used_bars,
                    ),
                    used_bars,
                )
        return trade, list(trade_bars)

    if control.exit_mode == "trail_after_1r_half_r":
        activation_price = float(trade.entry_price) + risk
        activation_index = next(
            (index for index, bar in enumerate(trade_bars) if float(bar.high) >= activation_price),
            None,
        )
        if activation_index is None:
            return trade, list(trade_bars)
        max_high = float(trade.entry_price)
        for index in range(activation_index, len(trade_bars)):
            bar = trade_bars[index]
            max_high = max(max_high, float(bar.high))
            trailing_floor = max(float(trade.entry_price), max_high - (0.5 * risk))
            if float(bar.low) <= trailing_floor:
                used_bars = list(trade_bars[: index + 1])
                return (
                    _build_trade_proxy(
                        trade=trade,
                        exit_ts=bar.end_ts,
                        exit_price=trailing_floor,
                        exit_reason="overlay_trailing_1r_half_r",
                        point_value=point_value,
                        used_bars=used_bars,
                    ),
                    used_bars,
                )
        return trade, list(trade_bars)

    return trade, list(trade_bars)


def _base_row_from_trade(*, trade_id: str, trade: Any) -> dict[str, Any]:
    return {
        "trade_id": trade_id,
        "entry_ts": trade.entry_ts,
        "exit_ts": trade.exit_ts,
        "decision_ts": trade.decision_ts,
        "entry_price": float(trade.entry_price),
        "exit_price": float(trade.exit_price),
        "stop_price": float(trade.stop_price),
        "pnl_cash": float(trade.pnl_cash),
        "mfe_points": float(trade.mfe_points),
        "mae_points": float(trade.mae_points),
        "hold_minutes": float(trade.hold_minutes),
        "bars_held_1m": int(trade.bars_held_1m),
        "side": trade.side,
        "session_segment": trade.session_segment,
        "family": trade.family,
        "exit_reason": trade.exit_reason,
        "added": False,
        "add_pnl_cash": 0.0,
        "add_reason": None,
        "add_price_quality_state": None,
    }


def _apples_to_apples_frozen_benchmark(target: EvaluationTarget) -> bool:
    return target.symbol == "MGC" and tuple(target.allowed_sessions) == ("ASIA", "US")


def _target_experiment_rows(
    *,
    target: EvaluationTarget,
    base_scope,
    control: OverlayControl,
    candidate_defs: dict[str, Any],
    daily_loss_threshold_cash: float | None,
    trade_windows_by_id: dict[str, list[Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    daily_realized: dict[str, float] = {}
    session_losers: dict[tuple[str, str], int] = {}
    skipped_by_governance = 0
    for trade_row in sorted(base_scope.trade_rows, key=lambda row: row["trade_record"].entry_ts):
        trade = trade_row["trade_record"]
        day_key = trade.entry_ts.date().isoformat()
        session_key = (day_key, str(trade.session_segment))
        if daily_loss_threshold_cash is not None and float(daily_realized.get(day_key, 0.0)) <= -daily_loss_threshold_cash:
            skipped_by_governance += 1
            continue
        if control.session_loser_limit is not None and int(session_losers.get(session_key, 0)) >= control.session_loser_limit:
            skipped_by_governance += 1
            continue
        modified_trade, modified_trade_bars = _simulate_exit(
            trade_row=trade_row,
            trade_bars=trade_windows_by_id.get(str(trade_row["trade_id"])) or [],
            control=control,
            point_value=target.point_value,
        )
        if target.candidate_id is None:
            row = _base_row_from_trade(trade_id=str(trade_row["trade_id"]), trade=modified_trade)
        else:
            candidate = candidate_defs[str(target.candidate_id)]
            minute_bars = [] if (control.disable_us_adds and str(modified_trade.session_segment) == "US") else modified_trade_bars
            row = evaluate_promotion_add_candidate(
                trade=modified_trade,
                minute_bars=minute_bars,
                candidate=candidate,
                point_value=target.point_value,
            )
            row["trade_id"] = str(trade_row["trade_id"])
        rows.append(row)
        pnl_cash = float(row.get("pnl_cash") or 0.0)
        daily_realized[day_key] = round(float(daily_realized.get(day_key, 0.0)) + pnl_cash, 6)
        if pnl_cash < 0.0:
            session_losers[session_key] = int(session_losers.get(session_key, 0)) + 1
    metadata = {
        "skipped_by_governance": skipped_by_governance,
        "daily_loss_threshold_cash": round(float(daily_loss_threshold_cash), 6) if daily_loss_threshold_cash is not None else None,
    }
    return rows, metadata


def _experiment_result_row(
    *,
    target: EvaluationTarget,
    control: OverlayControl,
    position_rows: Sequence[dict[str, Any]],
    bar_count: int,
    start_timestamp: datetime,
    end_timestamp: datetime,
    no_overlay_metrics: dict[str, Any],
    frozen_benchmark_metrics: dict[str, Any] | None,
    wall_seconds: float,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    metrics = _trade_metrics(position_rows, bar_count=bar_count)
    add_rows = [row for row in position_rows if row.get("added")]
    add_only_rows = [
        {
            "entry_ts": row.get("add_entry_ts"),
            "decision_ts": row.get("decision_ts"),
            "pnl_cash": row.get("add_pnl_cash"),
            "mfe_points": 0.0,
            "mae_points": 0.0,
            "hold_minutes": row.get("add_hold_minutes"),
            "bars_held_1m": row.get("bars_held_1m"),
            "side": row.get("side"),
            "session_segment": row.get("session_segment"),
        }
        for row in add_rows
    ]
    add_only_metrics = _trade_metrics(add_only_rows, bar_count=bar_count)
    session_breakdown = _normalize_session_breakdown(_candidate_session_breakdown(position_rows))
    delta_vs_no_overlay = {
        "trade_count_delta": int(metrics["total_trades"]) - int(no_overlay_metrics["total_trades"]),
        "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(no_overlay_metrics["net_pnl_cash"]), 4),
        "average_trade_pnl_cash_delta": round(
            float(metrics["average_trade_pnl_cash"]) - float(no_overlay_metrics["average_trade_pnl_cash"]),
            4,
        ),
        "profit_factor_delta": round(float(metrics["profit_factor"]) - float(no_overlay_metrics["profit_factor"]), 4),
        "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(no_overlay_metrics["max_drawdown"]), 4),
        "win_rate_delta": round(float(metrics["win_rate"]) - float(no_overlay_metrics["win_rate"]), 4),
    }
    if frozen_benchmark_metrics is None:
        delta_vs_frozen = None
    else:
        delta_vs_frozen = {
            "trade_count_delta": int(metrics["total_trades"]) - int(frozen_benchmark_metrics["total_trades"]),
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(frozen_benchmark_metrics["net_pnl_cash"]), 4),
            "average_trade_pnl_cash_delta": round(
                float(metrics["average_trade_pnl_cash"]) - float(frozen_benchmark_metrics["average_trade_pnl_cash"]),
                4,
            ),
            "profit_factor_delta": round(float(metrics["profit_factor"]) - float(frozen_benchmark_metrics["profit_factor"]), 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(frozen_benchmark_metrics["max_drawdown"]), 4),
            "win_rate_delta": round(float(metrics["win_rate"]) - float(frozen_benchmark_metrics["win_rate"]), 4),
        }
    return {
        "target_id": target.target_id,
        "label": target.label,
        "symbol": target.symbol,
        "sessions": list(target.allowed_sessions),
        "target_kind": target.target_kind,
        "control_id": control.control_id,
        "control_label": control.label,
        "config": _control_config_payload(control),
        "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
        "date_span": {
            "start_timestamp": _serialize_datetime(start_timestamp),
            "end_timestamp": _serialize_datetime(end_timestamp),
        },
        "metrics": metrics,
        "add_count": len(add_rows),
        "add_only_metrics": add_only_metrics,
        "session_breakdown": session_breakdown,
        "delta_vs_target_no_overlay": delta_vs_no_overlay,
        "delta_vs_frozen_benchmark": delta_vs_frozen,
        "rerun_wall_time_seconds": round(wall_seconds, 6),
        "metadata": metadata,
    }


def _comparison_csv_rows(results: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in results:
        metrics = row["metrics"]
        add_only = row["add_only_metrics"]
        no_overlay = row["delta_vs_target_no_overlay"]
        frozen = row["delta_vs_frozen_benchmark"] or {}
        rows.append(
            {
                "target_label": row["label"],
                "target_id": row["target_id"],
                "symbol": row["symbol"],
                "sessions": "/".join(row["sessions"]),
                "control_id": row["control_id"],
                "control_label": row["control_label"],
                "trade_count": metrics["total_trades"],
                "net_pnl_cash": metrics["net_pnl_cash"],
                "average_trade_pnl_cash": metrics["average_trade_pnl_cash"],
                "profit_factor": metrics["profit_factor"],
                "max_drawdown": metrics["max_drawdown"],
                "win_rate": metrics["win_rate"],
                "add_count": row["add_count"],
                "add_only_net_pnl_cash": add_only["net_pnl_cash"],
                "asia_net_pnl_cash": row["session_breakdown"]["ASIA"]["net_pnl_cash"],
                "us_net_pnl_cash": row["session_breakdown"]["US"]["net_pnl_cash"],
                "delta_vs_target_no_overlay_net_pnl_cash": no_overlay["net_pnl_cash_delta"],
                "delta_vs_target_no_overlay_profit_factor": no_overlay["profit_factor_delta"],
                "delta_vs_target_no_overlay_max_drawdown": no_overlay["max_drawdown_delta"],
                "delta_vs_frozen_benchmark_net_pnl_cash": frozen.get("net_pnl_cash_delta"),
                "delta_vs_frozen_benchmark_profit_factor": frozen.get("profit_factor_delta"),
                "rerun_wall_time_seconds": row["rerun_wall_time_seconds"],
                "execution_model": row["execution_model"],
            }
        )
    return rows


def run_exit_drawdown_matrix(
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

    targets = _build_experiment_targets()
    controls = _build_controls()
    candidate_defs = _candidate_defs()
    symbol_set = {target.symbol for target in targets}
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
    scope_truths = {
        (symbol, sessions): _evaluate_materialized_scope(
            symbol_truth=symbol_truths[symbol],
            allowed_sessions=sessions,
            point_value=point_value,
        )
        for symbol, sessions, point_value in {
            ("MGC", ("ASIA", "US"), 10.0),
            ("GC", ("ASIA", "US"), 100.0),
            ("GC", ("ASIA",), 100.0),
        }
    }
    trade_windows_by_scope = {
        key: _trade_windows_by_id(bars_1m=truth.bars_1m, trade_rows=truth.trade_rows)
        for key, truth in scope_truths.items()
    }

    frozen_target = next(target for target in targets if target.target_id == "atp_companion_v1__benchmark_mgc_asia_us")
    frozen_scope = scope_truths[(frozen_target.symbol, frozen_target.allowed_sessions)]
    frozen_position_rows = _base_position_rows(frozen_scope.trade_rows)
    frozen_no_overlay_metrics = _trade_metrics(frozen_position_rows, bar_count=frozen_scope.bar_count)

    baseline_by_target: dict[str, dict[str, Any]] = {}
    results: list[dict[str, Any]] = []
    for target in targets:
        base_scope = scope_truths[(target.symbol, target.allowed_sessions)]
        target_base_rows, _ = _target_experiment_rows(
            target=target,
            base_scope=base_scope,
            control=controls[0],
            candidate_defs=candidate_defs,
            daily_loss_threshold_cash=None,
            trade_windows_by_id=trade_windows_by_scope[(target.symbol, target.allowed_sessions)],
        )
        baseline_by_target[target.target_id] = {
            "metrics": _trade_metrics(target_base_rows, bar_count=base_scope.bar_count),
            "average_loser_pnl_cash": _trade_metrics(target_base_rows, bar_count=base_scope.bar_count)["average_loser_pnl_cash"],
        }
        for control in controls:
            overlay_started = perf_counter()
            daily_loss_threshold_cash = None
            if control.daily_loss_halt_multiple is not None:
                average_loser = abs(float(baseline_by_target[target.target_id]["average_loser_pnl_cash"] or 0.0))
                daily_loss_threshold_cash = average_loser * float(control.daily_loss_halt_multiple)
            position_rows, metadata = _target_experiment_rows(
                target=target,
                base_scope=base_scope,
                control=control,
                candidate_defs=candidate_defs,
                daily_loss_threshold_cash=daily_loss_threshold_cash,
                trade_windows_by_id=trade_windows_by_scope[(target.symbol, target.allowed_sessions)],
            )
            frozen_metrics = frozen_no_overlay_metrics if _apples_to_apples_frozen_benchmark(target) else None
            results.append(
                _experiment_result_row(
                    target=target,
                    control=control,
                    position_rows=position_rows,
                    bar_count=base_scope.bar_count,
                    start_timestamp=run_start,
                    end_timestamp=run_end,
                    no_overlay_metrics=baseline_by_target[target.target_id]["metrics"],
                    frozen_benchmark_metrics=frozen_metrics,
                    wall_seconds=perf_counter() - overlay_started,
                    metadata=metadata,
                )
            )

    comparison_rows = _comparison_csv_rows(results)
    ranking = sorted(
        results,
        key=lambda row: (
            float(row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"]),
            -float(row["delta_vs_target_no_overlay"]["max_drawdown_delta"]),
            float(row["metrics"]["profit_factor"]),
        ),
        reverse=True,
    )
    payload = {
        "study": "ATP Companion exit/drawdown experiment matrix",
        "generated_at": datetime.now(UTC).isoformat(),
        "shared_date_span": {
            "start_timestamp": run_start.isoformat(),
            "end_timestamp": run_end.isoformat(),
        },
        "targets": [
            {
                "target_id": target.target_id,
                "label": target.label,
                "symbol": target.symbol,
                "sessions": list(target.allowed_sessions),
                "target_kind": target.target_kind,
            }
            for target in targets
        ],
        "controls": [
            {
                "control_id": control.control_id,
                "label": control.label,
                "config": _control_config_payload(control),
            }
            for control in controls
        ],
        "results": results,
        "comparison_rows": comparison_rows,
        "ranking": [
            {
                "target_id": row["target_id"],
                "control_id": row["control_id"],
                "label": row["label"],
                "control_label": row["control_label"],
                "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                "max_drawdown": row["metrics"]["max_drawdown"],
                "delta_vs_target_no_overlay": row["delta_vs_target_no_overlay"],
            }
            for row in ranking
        ],
        "timing": {
            "total_wall_seconds": round(perf_counter() - started_at, 6),
        },
    }

    json_path = output_dir / "atp_companion_exit_drawdown_matrix.json"
    markdown_path = output_dir / "atp_companion_exit_drawdown_matrix.md"
    csv_path = output_dir / "atp_companion_exit_drawdown_matrix.csv"
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = [
        "# ATP Companion Exit / Drawdown Experiment Matrix",
        "",
        f"- Shared date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Total wall seconds: `{payload['timing']['total_wall_seconds']}`",
        "",
        "## Top Rows",
    ]
    for row in ranking[:12]:
        lines.extend(
            [
                f"### {row['label']} / {row['control_label']}",
                f"- Net P&L: `{row['metrics']['net_pnl_cash']}`",
                f"- Max drawdown: `{row['metrics']['max_drawdown']}`",
                f"- Win rate: `{row['metrics']['win_rate']}`",
                f"- Delta vs target no-overlay net / PF / DD: `{row['delta_vs_target_no_overlay']['net_pnl_cash_delta']}` / `{row['delta_vs_target_no_overlay']['profit_factor_delta']}` / `{row['delta_vs_target_no_overlay']['max_drawdown_delta']}`",
                "",
            ]
        )
    markdown_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
        writer.writeheader()
        writer.writerows(comparison_rows)
    return {
        "json_path": json_path,
        "markdown_path": markdown_path,
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
            DEFAULT_OUTPUT_ROOT / f"atp_companion_exit_drawdown_matrix_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_exit_drawdown_matrix(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="exit_drawdown_matrix",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
