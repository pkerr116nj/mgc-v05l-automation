"""ATP Companion US early-invalidation refinement review."""

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

from .atp_companion_failure_governance_review import _drawdown_episodes
from .atp_companion_full_history_review import DEFAULT_OUTPUT_ROOT, DEFAULT_SOURCE_DB, EvaluationTarget, _json_ready
from .atp_experiment_registry import register_atp_report_output
from .atp_companion_us_fast_fail_review import (
    ScopeContext,
    _build_candidate_rows,
    _core_fast_fail_bucket,
    _discover_best_sources,
    _evaluate_scope_with_context,
    _first_window_stats,
    _is_us_core_fast_fail,
    _materialize_symbol_truth,
    _review_targets,
    _shared_1m_coverage,
    _target_hash,
    _trade_proxy,
    _us_fast_fail_anatomy,
    _us_subwindow,
)
from ..research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.experiment_configs import EarlyInvalidationConfig as EarlyInvalidationConfigModel, config_payload
from ..research.trend_participation.performance_validation import _trade_metrics


@dataclass(frozen=True)
class EarlyInvalidationControl:
    control_id: str
    label: str
    window_bars: int = 0
    min_favorable_excursion_r: float | None = None
    adverse_excursion_abort_r: float | None = None
    require_hold_failure: bool = False
    apply_subwindows: tuple[str, ...] = ("US_MID", "US_LATE")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-us-early-invalidation-refinement")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _controls_for_target(target: EvaluationTarget) -> list[EarlyInvalidationControl]:
    if tuple(target.allowed_sessions) == ("ASIA",):
        return [EarlyInvalidationControl(control_id="none", label="No US early-invalidation control (Asia-only target)")]
    controls = [
        EarlyInvalidationControl(control_id="none", label="No US early-invalidation control"),
        EarlyInvalidationControl(
            control_id="us_1bar_no_traction",
            label="US 1-bar no-traction abort",
            window_bars=1,
            min_favorable_excursion_r=0.10,
        ),
        EarlyInvalidationControl(
            control_id="us_2bar_no_traction",
            label="US 2-bar no-traction abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
        ),
        EarlyInvalidationControl(
            control_id="us_2bar_adverse_only",
            label="US 2-bar adverse-excursion abort",
            window_bars=2,
            adverse_excursion_abort_r=0.75,
        ),
        EarlyInvalidationControl(
            control_id="us_2bar_hold_failure_only",
            label="US 2-bar reclaim/hold-failure abort",
            window_bars=2,
            require_hold_failure=True,
        ),
        EarlyInvalidationControl(
            control_id="us_2bar_no_traction_plus_adverse",
            label="US 2-bar no-traction + adverse-excursion abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
        ),
        EarlyInvalidationControl(
            control_id="us_2bar_no_traction_plus_hold_failure",
            label="US 2-bar no-traction + reclaim/hold-failure abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
            require_hold_failure=True,
        ),
        EarlyInvalidationControl(
            control_id="us_2bar_adverse_plus_hold_failure",
            label="US 2-bar adverse-excursion + reclaim/hold-failure abort",
            window_bars=2,
            adverse_excursion_abort_r=0.75,
            require_hold_failure=True,
        ),
        EarlyInvalidationControl(
            control_id="us_mid_2bar_no_traction_plus_adverse",
            label="US_MID 2-bar no-traction + adverse-excursion abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
            apply_subwindows=("US_MID",),
        ),
        EarlyInvalidationControl(
            control_id="us_late_2bar_no_traction_plus_adverse",
            label="US_LATE 2-bar no-traction + adverse-excursion abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
            adverse_excursion_abort_r=0.75,
            apply_subwindows=("US_LATE",),
        ),
        EarlyInvalidationControl(
            control_id="us_mid_2bar_no_traction_plus_hold_failure",
            label="US_MID 2-bar no-traction + reclaim/hold-failure abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
            require_hold_failure=True,
            apply_subwindows=("US_MID",),
        ),
        EarlyInvalidationControl(
            control_id="us_late_2bar_no_traction_plus_hold_failure",
            label="US_LATE 2-bar no-traction + reclaim/hold-failure abort",
            window_bars=2,
            min_favorable_excursion_r=0.25,
            require_hold_failure=True,
            apply_subwindows=("US_LATE",),
        ),
    ]
    return controls


def _control_hash(controls: Sequence[EarlyInvalidationControl]) -> str:
    payload = [_control_config_payload(control) for control in controls]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _control_config_payload(control: EarlyInvalidationControl) -> dict[str, Any]:
    return dict(
        config_payload(
            EarlyInvalidationConfigModel(
                session_scope=",".join(control.apply_subwindows),
                window_bars=int(control.window_bars),
                min_favorable_excursion_r=float(control.min_favorable_excursion_r or 0.0),
                adverse_excursion_abort_r=float(control.adverse_excursion_abort_r or 0.0),
                logic_mode="all" if control.require_hold_failure else "partial",
            )
        )
    )


def _baseline_rows_by_trade_id(
    *,
    target: EvaluationTarget,
    scope: ScopeContext,
    candidate_defs: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    rows = _build_candidate_rows(target=target, scope=scope, candidate_defs=candidate_defs)
    return {str(row["trade_id"]): row for row in rows}


def _should_apply_to_trade(trade: Any, control: EarlyInvalidationControl) -> bool:
    if str(trade.session_segment) != "US" or control.window_bars <= 0:
        return False
    return _us_subwindow(trade.entry_ts) in set(control.apply_subwindows)


def _evaluate_abort_logic(
    *,
    trade: Any,
    trade_bars: Sequence[Any],
    timing_state: Any | None,
    control: EarlyInvalidationControl,
) -> dict[str, Any] | None:
    if not trade_bars or not _should_apply_to_trade(trade, control):
        return None
    invalidation_price = float(
        ((timing_state.feature_snapshot if timing_state is not None else {}) or {})
        .get("timing_checks", {})
        .get("invalidation_price")
        or float(trade.stop_price)
    )
    first_window = _first_window_stats(
        trade=trade,
        trade_bars=trade_bars,
        timing_state=timing_state,
        bars=control.window_bars,
    )
    reasons: list[str] = []
    require_no_traction = control.min_favorable_excursion_r is not None
    require_adverse = control.adverse_excursion_abort_r is not None
    if require_no_traction and float(first_window["mfe_r"]) < float(control.min_favorable_excursion_r or 0.0):
        reasons.append("no_traction")
    if require_adverse and float(first_window["mae_r"]) >= float(control.adverse_excursion_abort_r or 0.0):
        reasons.append("adverse_excursion")
    if control.require_hold_failure:
        hold_failure = bool(first_window["close_below_entry"] or first_window["low_below_invalidation"])
        if hold_failure:
            reasons.append("hold_failure")
    requirements = int(require_no_traction) + int(require_adverse) + int(control.require_hold_failure)
    if requirements == 0 or len(reasons) != requirements:
        return None
    trigger_index = min(control.window_bars, len(trade_bars)) - 1
    trigger_bar = trade_bars[trigger_index]
    exit_price = float(trade.stop_price) if float(trigger_bar.low) <= float(trade.stop_price) else float(trigger_bar.close)
    return {
        "reasons": reasons,
        "used_bars": list(trade_bars[: trigger_index + 1]),
        "exit_ts": trigger_bar.end_ts,
        "exit_price": exit_price,
        "invalidation_price": invalidation_price,
        "first_window": first_window,
    }


def _apply_control_to_target(
    *,
    target: EvaluationTarget,
    scope: ScopeContext,
    baseline_rows_by_trade_id: dict[str, dict[str, Any]],
    control: EarlyInvalidationControl,
    candidate_defs: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    candidate = candidate_defs.get(str(target.candidate_id)) if target.candidate_id else None
    rows: list[dict[str, Any]] = []
    abort_reason_counts: dict[str, int] = {"no_traction": 0, "adverse_excursion": 0, "hold_failure": 0}
    abort_reason_net_delta: dict[str, float] = {"no_traction": 0.0, "adverse_excursion": 0.0, "hold_failure": 0.0}
    harmed_winner_count = 0
    harmed_winner_pnl = 0.0
    skipped_us_winner_count = 0
    skipped_us_winner_pnl = 0.0

    for trade_row in scope.trade_rows:
        trade_id = str(trade_row["trade_id"])
        trade = trade_row["trade_record"]
        baseline_row = baseline_rows_by_trade_id[trade_id]
        timing_state = scope.timing_states_by_trade_id.get(trade_id)
        modified_trade = trade
        modified_bars = list(scope.trade_windows_by_id.get(trade_id) or [])
        abort_info = None
        if control.control_id != "none":
            abort_info = _evaluate_abort_logic(
                trade=trade,
                trade_bars=modified_bars,
                timing_state=timing_state,
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
            pnl_delta = round(overlay_pnl - baseline_pnl, 4)
            for reason in abort_info["reasons"]:
                abort_reason_counts[reason] += 1
                abort_reason_net_delta[reason] = round(abort_reason_net_delta[reason] + pnl_delta, 4)
            if str(trade.session_segment) == "US" and baseline_pnl > 0.0 and overlay_pnl < baseline_pnl:
                harmed_winner_count += 1
                harmed_winner_pnl = round(harmed_winner_pnl + (baseline_pnl - overlay_pnl), 4)
                if overlay_pnl <= 0.0:
                    skipped_us_winner_count += 1
                    skipped_us_winner_pnl = round(skipped_us_winner_pnl + baseline_pnl, 4)

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
        "abort_reason_counts": abort_reason_counts,
        "abort_reason_net_delta": abort_reason_net_delta,
        "good_us_winners_harmed_count": harmed_winner_count,
        "good_us_winners_harmed_pnl_cost": harmed_winner_pnl,
        "good_us_winners_flipped_to_nonpositive_count": skipped_us_winner_count,
        "good_us_winners_flipped_to_nonpositive_pnl_cost": skipped_us_winner_pnl,
    }


def _us_fast_fail_loser_count(rows: Sequence[dict[str, Any]]) -> int:
    return sum(1 for row in rows if _is_us_core_fast_fail(row))


def _experiment_row(
    *,
    target: EvaluationTarget,
    control: EarlyInvalidationControl,
    rows: Sequence[dict[str, Any]],
    no_overlay_rows: Sequence[dict[str, Any]],
    bar_count: int,
    wall_seconds: float,
    control_state: dict[str, Any],
) -> dict[str, Any]:
    metrics = _trade_metrics(rows, bar_count=bar_count)
    no_overlay_metrics = _trade_metrics(no_overlay_rows, bar_count=bar_count)
    us_trades = [row for row in rows if str(row.get("session_segment")) == "US"]
    us_net_pnl = round(sum(float(row.get("pnl_cash") or 0.0) for row in us_trades), 4)
    us_fast_fail_loser_count = _us_fast_fail_loser_count(rows)
    baseline_us_fast_fail_loser_count = _us_fast_fail_loser_count(no_overlay_rows)
    episodes = _drawdown_episodes(rows=rows)
    base_episodes = _drawdown_episodes(rows=no_overlay_rows)
    worst_episode_loss = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
    baseline_worst_episode_loss = float(base_episodes[0]["peak_to_trough_loss"]) if base_episodes else 0.0
    baseline_us_net = round(sum(float(row.get("pnl_cash") or 0.0) for row in no_overlay_rows if str(row.get("session_segment")) == "US"), 4)
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
        "metrics": metrics,
        "us_net_pnl_cash": us_net_pnl,
        "us_fast_fail_loser_count": us_fast_fail_loser_count,
        "worst_drawdown_episode_loss": round(worst_episode_loss, 4),
        "abort_reason_counts": control_state["abort_reason_counts"],
        "abort_reason_net_delta": control_state["abort_reason_net_delta"],
        "good_us_winners_harmed_count": int(control_state["good_us_winners_harmed_count"]),
        "good_us_winners_harmed_pnl_cost": round(float(control_state["good_us_winners_harmed_pnl_cost"]), 4),
        "false_negative_cost": {
            "good_us_winners_filtered_out_count": int(control_state["good_us_winners_flipped_to_nonpositive_count"]),
            "good_us_winners_filtered_out_pnl_cash": round(float(control_state["good_us_winners_flipped_to_nonpositive_pnl_cost"]), 4),
        },
        "delta_vs_target_no_overlay": {
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(no_overlay_metrics["net_pnl_cash"]), 4),
            "us_net_pnl_cash_delta": round(us_net_pnl - baseline_us_net, 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(no_overlay_metrics["max_drawdown"]), 4),
            "worst_drawdown_episode_change_delta": round(worst_episode_loss - baseline_worst_episode_loss, 4),
            "profit_factor_delta": round(float(metrics["profit_factor"]) - float(no_overlay_metrics["profit_factor"]), 4),
            "win_rate_delta": round(float(metrics["win_rate"]) - float(no_overlay_metrics["win_rate"]), 4),
            "us_fast_fail_loser_count_delta": int(us_fast_fail_loser_count) - int(baseline_us_fast_fail_loser_count),
        },
        "wall_time_seconds": round(wall_seconds, 6),
    }


def _comparison_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for row in rows:
        payload.append(
            {
                "target_label": row["label"],
                "target_id": row["target_id"],
                "symbol": row["symbol"],
                "sessions": "/".join(row["sessions"]),
                "control_id": row["control_id"],
                "control_label": row["control_label"],
                "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                "us_net_pnl_cash": row["us_net_pnl_cash"],
                "profit_factor": row["metrics"]["profit_factor"],
                "max_drawdown": row["metrics"]["max_drawdown"],
                "worst_drawdown_episode_loss": row["worst_drawdown_episode_loss"],
                "win_rate": row["metrics"]["win_rate"],
                "us_fast_fail_loser_count": row["us_fast_fail_loser_count"],
                "good_us_winners_harmed_count": row["good_us_winners_harmed_count"],
                "good_us_winners_harmed_pnl_cost": row["good_us_winners_harmed_pnl_cost"],
                "false_negative_count": row["false_negative_cost"]["good_us_winners_filtered_out_count"],
                "false_negative_pnl_cash": row["false_negative_cost"]["good_us_winners_filtered_out_pnl_cash"],
                "delta_net_pnl_cash": row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"],
                "delta_us_net_pnl_cash": row["delta_vs_target_no_overlay"]["us_net_pnl_cash_delta"],
                "delta_max_drawdown": row["delta_vs_target_no_overlay"]["max_drawdown_delta"],
                "delta_worst_drawdown_episode": row["delta_vs_target_no_overlay"]["worst_drawdown_episode_change_delta"],
                "delta_us_fast_fail_loser_count": row["delta_vs_target_no_overlay"]["us_fast_fail_loser_count_delta"],
                "wall_time_seconds": row["wall_time_seconds"],
            }
        )
    return payload


def run_us_early_invalidation_refinement(
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

    targets = _review_targets()
    controls_by_target = {target.target_id: _controls_for_target(target) for target in targets}
    candidate_defs = {candidate.candidate_id: candidate for candidate in default_atp_promotion_add_candidates()}
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
    scope_contexts = {
        (target.symbol, target.allowed_sessions): _evaluate_scope_with_context(
            symbol_truth=symbol_truths[target.symbol],
            allowed_sessions=target.allowed_sessions,
            point_value=target.point_value,
        )
        for target in targets
    }

    baseline_rows_by_target: dict[str, dict[str, dict[str, Any]]] = {}
    anatomy_rows: list[dict[str, Any]] = []
    for target in targets:
        scope = scope_contexts[(target.symbol, target.allowed_sessions)]
        baseline_rows = _baseline_rows_by_trade_id(target=target, scope=scope, candidate_defs=candidate_defs)
        baseline_rows_by_target[target.target_id] = baseline_rows
        anatomy_rows.append(
            _us_fast_fail_anatomy(
                target=target,
                scope=scope,
                no_overlay_rows=list(baseline_rows.values()),
            )
        )

    experiment_rows: list[dict[str, Any]] = []
    for target in targets:
        scope = scope_contexts[(target.symbol, target.allowed_sessions)]
        no_overlay_rows = list(baseline_rows_by_target[target.target_id].values())
        for control in controls_by_target[target.target_id]:
            overlay_started = perf_counter()
            rows, control_state = _apply_control_to_target(
                target=target,
                scope=scope,
                baseline_rows_by_trade_id=baseline_rows_by_target[target.target_id],
                control=control,
                candidate_defs=candidate_defs,
            )
            experiment_rows.append(
                _experiment_row(
                    target=target,
                    control=control,
                    rows=rows,
                    no_overlay_rows=no_overlay_rows,
                    bar_count=scope.bar_count,
                    wall_seconds=perf_counter() - overlay_started,
                    control_state=control_state,
                )
            )

    ranking = sorted(
        experiment_rows,
        key=lambda row: (
            float(row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"]),
            -float(row["delta_vs_target_no_overlay"]["max_drawdown_delta"]),
            -float(row["delta_vs_target_no_overlay"]["worst_drawdown_episode_change_delta"]),
            -float(row["false_negative_cost"]["good_us_winners_filtered_out_pnl_cash"]),
        ),
        reverse=True,
    )

    manifest = {
        "artifact_version": "atp_us_early_invalidation_refinement_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "source_db": str(source_db.resolve()),
        "source_date_span": {
            "start_timestamp": run_start.isoformat(),
            "end_timestamp": run_end.isoformat(),
        },
        "target_hashes": {target.target_id: _target_hash(target) for target in targets},
        "control_hashes": {target.target_id: _control_hash(controls_by_target[target.target_id]) for target in targets},
        "provenance": {
            "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
            "benchmark_semantics_changed": False,
        },
        "scope_timing": [
            {
                "symbol": scope.symbol,
                "allowed_sessions": list(scope.allowed_sessions),
                "entry_state_seconds": round(scope.entry_state_seconds, 6),
                "timing_state_seconds": round(scope.timing_state_seconds, 6),
                "trade_rebuild_seconds": round(scope.trade_rebuild_seconds, 6),
                "evaluation_seconds": round(scope.evaluation_seconds, 6),
            }
            for scope in scope_contexts.values()
        ],
        "total_wall_seconds": round(perf_counter() - started_at, 6),
    }

    payload = {
        "study": "ATP Companion US early-invalidation refinement",
        "manifest": manifest,
        "us_fast_fail_anatomy": anatomy_rows,
        "us_early_invalidation_matrix": experiment_rows,
        "ranking": [
            {
                "target_id": row["target_id"],
                "control_id": row["control_id"],
                "label": row["label"],
                "control_label": row["control_label"],
                "metrics": row["metrics"],
                "abort_reason_counts": row["abort_reason_counts"],
                "abort_reason_net_delta": row["abort_reason_net_delta"],
                "delta_vs_target_no_overlay": row["delta_vs_target_no_overlay"],
                "false_negative_cost": row["false_negative_cost"],
            }
            for row in ranking
        ],
    }

    manifest_path = output_dir / "atp_us_early_invalidation_manifest.json"
    json_path = output_dir / "atp_us_early_invalidation_review.json"
    md_path = output_dir / "atp_us_early_invalidation_review.md"
    csv_path = output_dir / "atp_us_early_invalidation_matrix.csv"
    manifest_path.write_text(json.dumps(_json_ready(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    comparison_rows = _comparison_rows(experiment_rows)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
        writer.writeheader()
        writer.writerows(comparison_rows)

    lines = [
        "# ATP Companion US Early Invalidation Refinement",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Shared date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Total wall seconds: `{manifest['total_wall_seconds']}`",
        "",
        "## Top Rows",
        "",
    ]
    for row in ranking[:12]:
        lines.extend(
            [
                f"### {row['label']} / {row['control_label']}",
                f"- Net P&L: `{row['metrics']['net_pnl_cash']}`",
                f"- US net P&L: `{row['us_net_pnl_cash']}`",
                f"- Max DD / worst episode: `{row['metrics']['max_drawdown']}` / `{row['worst_drawdown_episode_loss']}`",
                f"- Delta net / DD / worst episode: `{row['delta_vs_target_no_overlay']['net_pnl_cash_delta']}` / `{row['delta_vs_target_no_overlay']['max_drawdown_delta']}` / `{row['delta_vs_target_no_overlay']['worst_drawdown_episode_change_delta']}`",
                f"- Abort reasons: `{row['abort_reason_counts']}`",
                f"- False-negative cost: `{row['false_negative_cost']}`",
                "",
            ]
        )
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
            DEFAULT_OUTPUT_ROOT / f"atp_companion_us_early_invalidation_refinement_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_us_early_invalidation_refinement(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="us_early_invalidation_refinement",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
