"""ATP Companion US fast-fail anatomy and narrow core-governance review."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, time
from pathlib import Path
from time import perf_counter
from types import SimpleNamespace
from typing import Any, Sequence

from .atp_companion_failure_governance_review import _drawdown_episodes
from .atp_companion_full_history_review import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    DEFAULT_SOURCE_DB,
    EvaluationTarget,
    _base_position_rows,
    _discover_best_sources,
    _json_ready,
    _materialize_symbol_truth,
    _serialize_datetime,
    _shared_1m_coverage,
    _trade_windows_by_id,
    build_targets,
)
from .atp_experiment_registry import register_atp_report_output
from ..research.trend_participation.substrate import ensure_atp_feature_bundle, ensure_atp_scope_bundle
from ..research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.performance_validation import _trade_metrics
from ..research.trend_participation.phase3_timing import VWAP_FAVORABLE


@dataclass(frozen=True)
class UsFastFailControl:
    control_id: str
    label: str
    require_us_favorable_only: bool = False
    early_abort_window_bars: int = 0
    min_favorable_excursion_r: float = 0.0
    early_adverse_excursion_abort_r: float = 0.0
    cluster_fail_count_trigger: int = 0


@dataclass(frozen=True)
class ScopeContext:
    symbol: str
    allowed_sessions: tuple[str, ...]
    point_value: float
    bars_1m: Sequence[Any]
    trade_rows: Sequence[dict[str, Any]]
    trade_windows_by_id: dict[str, list[Any]]
    entry_states_by_trade_id: dict[str, Any]
    timing_states_by_trade_id: dict[str, Any]
    bar_count: int
    evaluation_seconds: float
    entry_state_seconds: float
    timing_state_seconds: float
    trade_rebuild_seconds: float


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-us-fast-fail-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _review_targets() -> list[EvaluationTarget]:
    wanted = {
        "atp_companion_v1__benchmark_mgc_asia_us",
        "atp_companion_v1__promotion_1_075r_favorable_only",
        "atp_companion_v1__candidate_gc_asia_us",
        "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
    }
    return [target for target in build_targets() if target.target_id in wanted]


def _target_hash(target: EvaluationTarget) -> str:
    payload = {
        "target_id": target.target_id,
        "symbol": target.symbol,
        "allowed_sessions": list(target.allowed_sessions),
        "point_value": target.point_value,
        "target_kind": target.target_kind,
        "candidate_id": target.candidate_id,
        "config_path": target.config_path,
        "lane_id": target.lane_id,
        "standalone_strategy_id": target.standalone_strategy_id,
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _control_hash(controls: Sequence[UsFastFailControl]) -> str:
    payload = [
        {
            "control_id": control.control_id,
            "label": control.label,
            "require_us_favorable_only": control.require_us_favorable_only,
            "early_abort_window_bars": control.early_abort_window_bars,
            "min_favorable_excursion_r": control.min_favorable_excursion_r,
            "early_adverse_excursion_abort_r": control.early_adverse_excursion_abort_r,
            "cluster_fail_count_trigger": control.cluster_fail_count_trigger,
        }
        for control in controls
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _controls_for_target(target: EvaluationTarget) -> list[UsFastFailControl]:
    if tuple(target.allowed_sessions) == ("ASIA",):
        return [UsFastFailControl(control_id="none", label="No US-only control (Asia-only target)")]
    return [
        UsFastFailControl(control_id="none", label="No US-only control"),
        UsFastFailControl(
            control_id="us_favorable_only",
            label="US-only require VWAP_FAVORABLE",
            require_us_favorable_only=True,
        ),
        UsFastFailControl(
            control_id="us_early_abort_1bar",
            label="US-only early abort after 1 bar if no traction and early adversity",
            early_abort_window_bars=1,
            min_favorable_excursion_r=0.10,
            early_adverse_excursion_abort_r=0.60,
        ),
        UsFastFailControl(
            control_id="us_early_abort_2bar",
            label="US-only early abort after 2 bars if no traction and early adversity",
            early_abort_window_bars=2,
            min_favorable_excursion_r=0.25,
            early_adverse_excursion_abort_r=0.75,
        ),
        UsFastFailControl(
            control_id="us_cluster_brake_2fastfails",
            label="US-only cluster brake after 2 fast-fail core trades in same US sub-window",
            cluster_fail_count_trigger=2,
        ),
        UsFastFailControl(
            control_id="us_favorable_plus_early_abort_2bar",
            label="US-only VWAP_FAVORABLE + early abort after 2 bars",
            require_us_favorable_only=True,
            early_abort_window_bars=2,
            min_favorable_excursion_r=0.25,
            early_adverse_excursion_abort_r=0.75,
        ),
        UsFastFailControl(
            control_id="us_favorable_plus_cluster_brake",
            label="US-only VWAP_FAVORABLE + cluster brake",
            require_us_favorable_only=True,
            cluster_fail_count_trigger=2,
        ),
        UsFastFailControl(
            control_id="us_favorable_plus_early_abort_2bar_plus_cluster_brake",
            label="US-only VWAP_FAVORABLE + early abort after 2 bars + cluster brake",
            require_us_favorable_only=True,
            early_abort_window_bars=2,
            min_favorable_excursion_r=0.25,
            early_adverse_excursion_abort_r=0.75,
            cluster_fail_count_trigger=2,
        ),
    ]


def _evaluate_scope_with_context(
    *,
    symbol_truth: Any,
    allowed_sessions: tuple[str, ...],
    point_value: float,
    bundle_root: Path = DEFAULT_PLATFORM_SUBSTRATE_ROOT,
) -> ScopeContext:
    scope_started = perf_counter()
    feature_bundle = ensure_atp_feature_bundle(
        bundle_root=bundle_root,
        source_db=symbol_truth.source_db,
        symbol=symbol_truth.symbol,
        selected_sources=symbol_truth.selected_sources,
        start_timestamp=symbol_truth.start_timestamp,
        end_timestamp=symbol_truth.end_timestamp,
        feature_rows=symbol_truth.rolling_scope_feature_rows,
    )
    bundle_started = perf_counter()
    scope_bundle = ensure_atp_scope_bundle(
        bundle_root=bundle_root,
        source_db=symbol_truth.source_db,
        symbol=symbol_truth.symbol,
        selected_sources=symbol_truth.selected_sources,
        start_timestamp=symbol_truth.start_timestamp,
        end_timestamp=symbol_truth.end_timestamp,
        allowed_sessions=allowed_sessions,
        point_value=float(point_value),
        bars_1m=symbol_truth.bars_1m,
        feature_bundle=feature_bundle,
    )
    bundle_seconds = perf_counter() - bundle_started
    entry_states = list(scope_bundle.entry_states)
    timing_states = list(scope_bundle.timing_states)
    trade_rows = list(scope_bundle.trade_rows)
    trade_windows = _trade_windows_by_id(bars_1m=symbol_truth.bars_1m, trade_rows=trade_rows)
    entry_by_trade_id = {
        f"{symbol_truth.symbol}|{state.decision_ts.isoformat()}": state
        for state in entry_states
    }
    timing_by_trade_id = {
        f"{symbol_truth.symbol}|{state.decision_ts.isoformat()}": state
        for state in timing_states
    }
    return ScopeContext(
        symbol=symbol_truth.symbol,
        allowed_sessions=allowed_sessions,
        point_value=point_value,
        bars_1m=symbol_truth.bars_1m,
        trade_rows=trade_rows,
        trade_windows_by_id=trade_windows,
        entry_states_by_trade_id=entry_by_trade_id,
        timing_states_by_trade_id=timing_by_trade_id,
        bar_count=len(symbol_truth.bars_1m),
        evaluation_seconds=perf_counter() - scope_started,
        entry_state_seconds=0.0,
        timing_state_seconds=0.0,
        trade_rebuild_seconds=bundle_seconds,
    )


def _build_candidate_rows(
    *,
    target: EvaluationTarget,
    scope: ScopeContext,
    candidate_defs: dict[str, Any],
) -> list[dict[str, Any]]:
    if target.candidate_id is None:
        rows = _base_position_rows(scope.trade_rows)
        for row in rows:
            row["point_value"] = target.point_value
            row["trade_pnl_cash"] = float(row.get("pnl_cash") or 0.0)
        return rows
    candidate = candidate_defs[str(target.candidate_id)]
    rows: list[dict[str, Any]] = []
    for trade_row in scope.trade_rows:
        trade = trade_row["trade_record"]
        row = evaluate_promotion_add_candidate(
            trade=trade,
            minute_bars=scope.trade_windows_by_id.get(str(trade_row["trade_id"])) or [],
            candidate=candidate,
            point_value=target.point_value,
        )
        row["trade_id"] = str(trade_row["trade_id"])
        row["point_value"] = target.point_value
        row["entry_price"] = float(trade.entry_price)
        row["stop_price"] = float(trade.stop_price)
        row["trade_pnl_cash"] = float(trade.pnl_cash)
        rows.append(row)
    return rows


def _risk_points(row: dict[str, Any]) -> float:
    entry = float(row.get("entry_price") or row.get("position_entry_price") or 0.0)
    stop = float(row.get("stop_price") or entry)
    return max(entry - stop, 1e-9)


def _core_pnl_cash(row: dict[str, Any]) -> float:
    if row.get("trade_pnl_cash") is not None:
        return float(row["trade_pnl_cash"])
    return float(row.get("pnl_cash") or 0.0)


def _core_fast_fail_bucket(row: dict[str, Any]) -> str | None:
    risk = _risk_points(row)
    point_value = abs(float(row.get("point_value") or 1.0))
    core_points = _core_pnl_cash(row) / point_value if point_value else 0.0
    mfe = float(row.get("mfe_points") or 0.0)
    if core_points < 0.0 and mfe < 0.5 * risk:
        return "fail_fast"
    if core_points <= 0.0 and mfe < 1.0 * risk:
        return "never_gets_traction"
    return None


def _is_us_core_fast_fail(row: dict[str, Any]) -> bool:
    return str(row.get("session_segment")) == "US" and _core_fast_fail_bucket(row) is not None


def _us_subwindow(timestamp: datetime | None) -> str:
    if timestamp is None:
        return "UNKNOWN"
    current = timestamp.timetz().replace(tzinfo=None)
    if current < time(11, 0):
        return "US_OPEN"
    if current < time(14, 0):
        return "US_MID"
    return "US_LATE"


def _first_window_stats(
    *,
    trade: Any,
    trade_bars: Sequence[Any],
    timing_state: Any | None,
    bars: int,
) -> dict[str, Any]:
    window = list(trade_bars[:bars])
    risk = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)
    invalidation_price = float(
        ((timing_state.feature_snapshot if timing_state is not None else {}) or {})
        .get("timing_checks", {})
        .get("invalidation_price")
        or float(trade.stop_price)
    )
    if not window:
        return {
            "mfe_r": 0.0,
            "mae_r": 0.0,
            "close_below_entry": False,
            "low_below_invalidation": False,
        }
    return {
        "mfe_r": round(max(float(bar.high) - float(trade.entry_price) for bar in window) / risk, 4),
        "mae_r": round(max(float(trade.entry_price) - float(bar.low) for bar in window) / risk, 4),
        "close_below_entry": any(float(bar.close) < float(trade.entry_price) for bar in window),
        "low_below_invalidation": any(float(bar.low) <= invalidation_price for bar in window),
    }


def _trade_proxy(
    *,
    trade: Any,
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
        hold_minutes=float(len(used_bars)),
        bars_held_1m=len(used_bars),
        side=trade.side,
        session_segment=trade.session_segment,
        mfe_points=max((float(bar.high) - float(trade.entry_price)) for bar in used_bars) if used_bars else 0.0,
        mae_points=max((float(trade.entry_price) - float(bar.low)) for bar in used_bars) if used_bars else 0.0,
        family=trade.family,
        exit_reason=exit_reason,
    )


def _apply_early_abort(
    *,
    trade_row: dict[str, Any],
    trade_bars: Sequence[Any],
    timing_state: Any | None,
    control: UsFastFailControl,
    point_value: float,
) -> tuple[Any, list[Any]]:
    trade = trade_row["trade_record"]
    if str(trade.session_segment) != "US" or control.early_abort_window_bars <= 0 or not trade_bars:
        return trade, list(trade_bars)
    risk = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)
    invalidation_price = float(
        ((timing_state.feature_snapshot if timing_state is not None else {}) or {})
        .get("timing_checks", {})
        .get("invalidation_price")
        or float(trade.stop_price)
    )
    running_mfe = 0.0
    running_mae = 0.0
    max_index = min(control.early_abort_window_bars, len(trade_bars))
    for index in range(max_index):
        bar = trade_bars[index]
        running_mfe = max(running_mfe, float(bar.high) - float(trade.entry_price))
        running_mae = max(running_mae, float(trade.entry_price) - float(bar.low))
        no_traction = running_mfe < (risk * float(control.min_favorable_excursion_r))
        bad_adverse = running_mae >= (risk * float(control.early_adverse_excursion_abort_r))
        hold_break = float(bar.close) < float(trade.entry_price) or float(bar.low) <= invalidation_price
        if no_traction and (bad_adverse or hold_break):
            used_bars = list(trade_bars[: index + 1])
            exit_price = float(trade.stop_price) if float(bar.low) <= float(trade.stop_price) else float(bar.close)
            return (
                _trade_proxy(
                    trade=trade,
                    exit_ts=bar.end_ts,
                    exit_price=exit_price,
                    exit_reason=f"overlay_{control.control_id}",
                    point_value=point_value,
                    used_bars=used_bars,
                ),
                used_bars,
            )
    return trade, list(trade_bars)


def _cluster_key(trade: Any) -> str:
    return f"{trade.entry_ts.date().isoformat()}::{_us_subwindow(trade.entry_ts)}"


def _apply_us_control_to_target(
    *,
    target: EvaluationTarget,
    scope: ScopeContext,
    no_overlay_rows_by_trade_id: dict[str, dict[str, Any]],
    control: UsFastFailControl,
    candidate_defs: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if control.control_id == "none":
        rows = [dict(no_overlay_rows_by_trade_id[str(trade_row["trade_id"])]) for trade_row in scope.trade_rows]
        return rows, {
            "good_us_trades_filtered_count": 0,
            "good_us_trades_filtered_pnl_cash": 0.0,
            "skipped_trade_ids": [],
        }
    candidate = candidate_defs.get(str(target.candidate_id)) if target.candidate_id else None
    rows: list[dict[str, Any]] = []
    cluster_fail_counts: dict[str, int] = {}
    skipped_trade_ids: list[str] = []
    false_negative_count = 0
    false_negative_pnl = 0.0

    for trade_row in scope.trade_rows:
        trade_id = str(trade_row["trade_id"])
        trade = trade_row["trade_record"]
        timing_state = scope.timing_states_by_trade_id.get(trade_id)
        baseline_row = no_overlay_rows_by_trade_id[trade_id]
        if str(trade.session_segment) == "US" and control.cluster_fail_count_trigger > 0:
            key = _cluster_key(trade)
            if cluster_fail_counts.get(key, 0) >= control.cluster_fail_count_trigger:
                skipped_trade_ids.append(trade_id)
                if float(baseline_row.get("pnl_cash") or 0.0) > 0.0:
                    false_negative_count += 1
                    false_negative_pnl = round(false_negative_pnl + float(baseline_row["pnl_cash"]), 4)
                continue
        if (
            str(trade.session_segment) == "US"
            and control.require_us_favorable_only
            and str(getattr(timing_state, "vwap_price_quality_state", "")) != VWAP_FAVORABLE
        ):
            skipped_trade_ids.append(trade_id)
            if float(baseline_row.get("pnl_cash") or 0.0) > 0.0:
                false_negative_count += 1
                false_negative_pnl = round(false_negative_pnl + float(baseline_row["pnl_cash"]), 4)
            continue

        modified_trade, modified_bars = _apply_early_abort(
            trade_row=trade_row,
            trade_bars=scope.trade_windows_by_id.get(trade_id) or [],
            timing_state=timing_state,
            control=control,
            point_value=target.point_value,
        )
        if candidate is None:
            row = {
                "trade_id": trade_id,
                **_base_position_rows([{"trade_id": trade_id, "trade_record": modified_trade}])[0],
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
        if str(modified_trade.session_segment) == "US" and control.cluster_fail_count_trigger > 0 and _is_us_core_fast_fail(row):
            key = _cluster_key(modified_trade)
            cluster_fail_counts[key] = cluster_fail_counts.get(key, 0) + 1
    return rows, {
        "good_us_trades_filtered_count": false_negative_count,
        "good_us_trades_filtered_pnl_cash": false_negative_pnl,
        "skipped_trade_ids": skipped_trade_ids,
    }


def _feature_bucket_counts(rows: Sequence[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "UNKNOWN")
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _mean(rows: Sequence[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _us_fast_fail_anatomy(
    *,
    target: EvaluationTarget,
    scope: ScopeContext,
    no_overlay_rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    rows_by_trade_id = {str(row["trade_id"]): row for row in no_overlay_rows}
    enriched_rows: list[dict[str, Any]] = []
    for trade_row in scope.trade_rows:
        trade_id = str(trade_row["trade_id"])
        row = rows_by_trade_id[trade_id]
        trade = trade_row["trade_record"]
        entry_state = scope.entry_states_by_trade_id.get(trade_id)
        timing_state = scope.timing_states_by_trade_id.get(trade_id)
        first1 = _first_window_stats(
            trade=trade,
            trade_bars=scope.trade_windows_by_id.get(trade_id) or [],
            timing_state=timing_state,
            bars=1,
        )
        first2 = _first_window_stats(
            trade=trade,
            trade_bars=scope.trade_windows_by_id.get(trade_id) or [],
            timing_state=timing_state,
            bars=2,
        )
        snapshot = ((entry_state.feature_snapshot if entry_state is not None else {}) or {})
        enriched_rows.append(
            {
                "trade_id": trade_id,
                "session_segment": str(trade.session_segment),
                "core_pnl_cash": _core_pnl_cash(row),
                "combined_pnl_cash": float(row.get("pnl_cash") or 0.0),
                "point_value": float(target.point_value),
                "entry_ts": trade.entry_ts,
                "vwap_price_quality_state": getattr(timing_state, "vwap_price_quality_state", None),
                "setup_quality_bucket": getattr(entry_state, "setup_quality_bucket", None),
                "pullback_state": getattr(entry_state, "pullback_state", None),
                "volatility_bucket": snapshot.get("volatility_bucket"),
                "regime_bucket": snapshot.get("regime_bucket"),
                "pullback_depth_score": snapshot.get("setup_pullback_depth_score"),
                "pullback_violence_score": snapshot.get("setup_pullback_violence_score"),
                "average_range": snapshot.get("average_range"),
                "first1_mfe_r": first1["mfe_r"],
                "first1_mae_r": first1["mae_r"],
                "first1_close_below_entry": first1["close_below_entry"],
                "first1_low_below_invalidation": first1["low_below_invalidation"],
                "first2_mfe_r": first2["mfe_r"],
                "first2_mae_r": first2["mae_r"],
                "first2_any_close_below_entry": first2["close_below_entry"],
                "first2_any_low_below_invalidation": first2["low_below_invalidation"],
                "subwindow": _us_subwindow(trade.entry_ts),
                "fast_fail_bucket": _core_fast_fail_bucket(row),
            }
        )
    us_rows = [row for row in enriched_rows if row["session_segment"] == "US"]
    if not us_rows:
        return {
            "target_id": target.target_id,
            "label": target.label,
            "symbol": target.symbol,
            "sessions": list(target.allowed_sessions),
            "us_slice_available": False,
            "reason": "Target has no US session scope.",
        }
    us_losers = [row for row in us_rows if float(row["core_pnl_cash"]) < 0.0]
    us_fast_fails = [row for row in us_rows if row["fast_fail_bucket"] is not None]
    good_us_rows = [row for row in us_rows if float(row["core_pnl_cash"]) > 0.0]
    episodes = _drawdown_episodes(rows=no_overlay_rows)
    worst_trade_ids = {trade_id for episode in episodes for trade_id in episode["trade_ids"]}
    worst_damage = abs(
        sum(
            min(float(rows_by_trade_id[trade_id].get("pnl_cash") or 0.0), 0.0)
            for trade_id in worst_trade_ids
            if trade_id in rows_by_trade_id
        )
    )
    fast_fail_damage = abs(
        sum(
            min(float(rows_by_trade_id[row["trade_id"]].get("pnl_cash") or 0.0), 0.0)
            for row in us_fast_fails
            if row["trade_id"] in worst_trade_ids
        )
    )
    vwap_counts = _feature_bucket_counts(us_fast_fails, "vwap_price_quality_state")
    dominant_vwap = next(iter(vwap_counts), "UNKNOWN")
    no_traction_count = sum(1 for row in us_fast_fails if float(row["first2_mfe_r"]) < 0.25)
    hold_break_count = sum(
        1
        for row in us_fast_fails
        if row["first1_close_below_entry"] or row["first2_any_close_below_entry"] or row["first2_any_low_below_invalidation"]
    )
    subwindow_counts = _feature_bucket_counts(us_fast_fails, "subwindow")
    return {
        "target_id": target.target_id,
        "label": target.label,
        "symbol": target.symbol,
        "sessions": list(target.allowed_sessions),
        "us_slice_available": True,
        "us_fast_fail_trade_count": len(us_fast_fails),
        "share_of_total_us_losers_percent": round((len(us_fast_fails) / len(us_losers)) * 100.0, 4) if us_losers else 0.0,
        "share_of_worst_drawdown_damage_percent": round((fast_fail_damage / worst_damage) * 100.0, 4) if worst_damage else 0.0,
        "vwap_quality_counts": vwap_counts,
        "dominant_vwap_quality_state": dominant_vwap,
        "first_window_patterns": {
            "mean_first1_mfe_r": _mean(us_fast_fails, "first1_mfe_r"),
            "mean_first1_mae_r": _mean(us_fast_fails, "first1_mae_r"),
            "mean_first2_mfe_r": _mean(us_fast_fails, "first2_mfe_r"),
            "mean_first2_mae_r": _mean(us_fast_fails, "first2_mae_r"),
            "first1_close_below_entry_percent": round(
                (sum(1 for row in us_fast_fails if row["first1_close_below_entry"]) / len(us_fast_fails)) * 100.0,
                4,
            ) if us_fast_fails else 0.0,
            "first2_any_close_below_entry_percent": round(
                (sum(1 for row in us_fast_fails if row["first2_any_close_below_entry"]) / len(us_fast_fails)) * 100.0,
                4,
            ) if us_fast_fails else 0.0,
            "first2_any_low_below_invalidation_percent": round(
                (sum(1 for row in us_fast_fails if row["first2_any_low_below_invalidation"]) / len(us_fast_fails)) * 100.0,
                4,
            ) if us_fast_fails else 0.0,
        },
        "post_entry_behavior": {
            "no_traction_percent": round((no_traction_count / len(us_fast_fails)) * 100.0, 4) if us_fast_fails else 0.0,
            "hold_break_percent": round((hold_break_count / len(us_fast_fails)) * 100.0, 4) if us_fast_fails else 0.0,
        },
        "subwindow_counts": subwindow_counts,
        "obvious_low_quality_buckets": {
            "setup_quality_bucket_counts": _feature_bucket_counts(us_fast_fails, "setup_quality_bucket"),
            "pullback_state_counts": _feature_bucket_counts(us_fast_fails, "pullback_state"),
            "volatility_bucket_counts": _feature_bucket_counts(us_fast_fails, "volatility_bucket"),
            "regime_bucket_counts": _feature_bucket_counts(us_fast_fails, "regime_bucket"),
        },
        "feature_means_fast_fail": {
            "pullback_depth_score": _mean(us_fast_fails, "pullback_depth_score"),
            "pullback_violence_score": _mean(us_fast_fails, "pullback_violence_score"),
        },
        "feature_means_good_us": {
            "pullback_depth_score": _mean(good_us_rows, "pullback_depth_score"),
            "pullback_violence_score": _mean(good_us_rows, "pullback_violence_score"),
        },
        "explicit_answers": {
            "bad_us_fast_fails_mostly_vwap_neutral": dominant_vwap != VWAP_FAVORABLE,
            "bad_us_fast_fails_fail_because_never_get_traction": no_traction_count >= max(1, len(us_fast_fails) // 2),
            "bad_us_fast_fails_break_continuation_in_first_1_2_bars": hold_break_count >= max(1, len(us_fast_fails) // 2),
            "dominant_us_subwindow": next(iter(subwindow_counts), "UNKNOWN"),
        },
    }


def _experiment_row(
    *,
    target: EvaluationTarget,
    control: UsFastFailControl,
    rows: Sequence[dict[str, Any]],
    no_overlay_rows: Sequence[dict[str, Any]],
    bar_count: int,
    wall_seconds: float,
    false_negative_count: int,
    false_negative_pnl_cash: float,
) -> dict[str, Any]:
    metrics = _trade_metrics(rows, bar_count=bar_count)
    no_overlay_metrics = _trade_metrics(no_overlay_rows, bar_count=bar_count)
    us_trades = [row for row in rows if str(row.get("session_segment")) == "US"]
    us_net_pnl = round(sum(float(row.get("pnl_cash") or 0.0) for row in us_trades), 4)
    us_fast_fail_trade_count = sum(1 for row in rows if _is_us_core_fast_fail(row))
    baseline_us_fast_fail_trade_count = sum(1 for row in no_overlay_rows if _is_us_core_fast_fail(row))
    episodes = _drawdown_episodes(rows=rows)
    base_episodes = _drawdown_episodes(rows=no_overlay_rows)
    worst_episode_loss = float(episodes[0]["peak_to_trough_loss"]) if episodes else 0.0
    baseline_worst_episode_loss = float(base_episodes[0]["peak_to_trough_loss"]) if base_episodes else 0.0
    return {
        "target_id": target.target_id,
        "label": target.label,
        "symbol": target.symbol,
        "sessions": list(target.allowed_sessions),
        "target_kind": target.target_kind,
        "control_id": control.control_id,
        "control_label": control.label,
        "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
        "metrics": metrics,
        "us_trades": len(us_trades),
        "us_fast_fail_trade_count": us_fast_fail_trade_count,
        "us_net_pnl_cash": us_net_pnl,
        "worst_drawdown_episode_loss": round(worst_episode_loss, 4),
        "worst_drawdown_episode_change": round(worst_episode_loss - baseline_worst_episode_loss, 4),
        "false_negative_cost": {
            "good_us_trades_filtered_count": int(false_negative_count),
            "good_us_trades_filtered_pnl_cash": round(false_negative_pnl_cash, 4),
        },
        "delta_vs_target_no_overlay": {
            "trade_count_delta": int(metrics["total_trades"]) - int(no_overlay_metrics["total_trades"]),
            "us_fast_fail_trade_count_delta": int(us_fast_fail_trade_count) - int(baseline_us_fast_fail_trade_count),
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(no_overlay_metrics["net_pnl_cash"]), 4),
            "us_net_pnl_cash_delta": round(us_net_pnl - sum(float(row.get("pnl_cash") or 0.0) for row in no_overlay_rows if str(row.get("session_segment")) == "US"), 4),
            "average_trade_pnl_cash_delta": round(float(metrics["average_trade_pnl_cash"]) - float(no_overlay_metrics["average_trade_pnl_cash"]), 4),
            "profit_factor_delta": round(float(metrics["profit_factor"]) - float(no_overlay_metrics["profit_factor"]), 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(no_overlay_metrics["max_drawdown"]), 4),
            "worst_drawdown_episode_change_delta": round(worst_episode_loss - baseline_worst_episode_loss, 4),
            "win_rate_delta": round(float(metrics["win_rate"]) - float(no_overlay_metrics["win_rate"]), 4),
        },
        "wall_time_seconds": round(wall_seconds, 6),
    }


def _comparison_csv_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
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
                "trade_count": row["metrics"]["total_trades"],
                "us_trades": row["us_trades"],
                "us_fast_fail_trade_count": row["us_fast_fail_trade_count"],
                "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                "us_net_pnl_cash": row["us_net_pnl_cash"],
                "average_trade_pnl_cash": row["metrics"]["average_trade_pnl_cash"],
                "profit_factor": row["metrics"]["profit_factor"],
                "max_drawdown": row["metrics"]["max_drawdown"],
                "worst_drawdown_episode_loss": row["worst_drawdown_episode_loss"],
                "worst_drawdown_episode_change": row["worst_drawdown_episode_change"],
                "win_rate": row["metrics"]["win_rate"],
                "false_negative_count": row["false_negative_cost"]["good_us_trades_filtered_count"],
                "false_negative_pnl_cash": row["false_negative_cost"]["good_us_trades_filtered_pnl_cash"],
                "delta_net_pnl_cash": row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"],
                "delta_us_fast_fail_trade_count": row["delta_vs_target_no_overlay"]["us_fast_fail_trade_count_delta"],
                "delta_max_drawdown": row["delta_vs_target_no_overlay"]["max_drawdown_delta"],
                "wall_time_seconds": row["wall_time_seconds"],
            }
        )
    return payload


def run_us_fast_fail_review(
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

    no_overlay_rows_by_target: dict[str, list[dict[str, Any]]] = {}
    no_overlay_rows_by_trade_id: dict[str, dict[str, dict[str, Any]]] = {}
    anatomy_rows: list[dict[str, Any]] = []
    for target in targets:
        scope = scope_contexts[(target.symbol, target.allowed_sessions)]
        rows = _build_candidate_rows(target=target, scope=scope, candidate_defs=candidate_defs)
        no_overlay_rows_by_target[target.target_id] = rows
        no_overlay_rows_by_trade_id[target.target_id] = {str(row["trade_id"]): row for row in rows}
        anatomy_rows.append(
            _us_fast_fail_anatomy(
                target=target,
                scope=scope,
                no_overlay_rows=rows,
            )
        )

    experiment_rows: list[dict[str, Any]] = []
    for target in targets:
        scope = scope_contexts[(target.symbol, target.allowed_sessions)]
        no_overlay_rows = no_overlay_rows_by_target[target.target_id]
        for control in controls_by_target[target.target_id]:
            overlay_started = perf_counter()
            rows, filter_state = _apply_us_control_to_target(
                target=target,
                scope=scope,
                no_overlay_rows_by_trade_id=no_overlay_rows_by_trade_id[target.target_id],
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
                    false_negative_count=int(filter_state["good_us_trades_filtered_count"]),
                    false_negative_pnl_cash=float(filter_state["good_us_trades_filtered_pnl_cash"]),
                )
            )

    ranking = sorted(
        experiment_rows,
        key=lambda row: (
            -float(row["delta_vs_target_no_overlay"]["us_fast_fail_trade_count_delta"]),
            -float(row["delta_vs_target_no_overlay"]["worst_drawdown_episode_change_delta"]),
            float(row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"]),
            -float(row["false_negative_cost"]["good_us_trades_filtered_pnl_cash"]),
        ),
        reverse=True,
    )

    manifest = {
        "artifact_version": "atp_us_fast_fail_review_v1",
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
        "study": "ATP Companion US fast-fail anatomy and narrow core-governance review",
        "manifest": manifest,
        "us_fast_fail_anatomy": anatomy_rows,
        "targeted_us_core_filter_matrix": experiment_rows,
        "ranking": [
            {
                "target_id": row["target_id"],
                "control_id": row["control_id"],
                "label": row["label"],
                "control_label": row["control_label"],
                "us_fast_fail_trade_count": row["us_fast_fail_trade_count"],
                "false_negative_cost": row["false_negative_cost"],
                "delta_vs_target_no_overlay": row["delta_vs_target_no_overlay"],
                "metrics": row["metrics"],
            }
            for row in ranking
        ],
    }

    manifest_path = output_dir / "atp_us_fast_fail_manifest.json"
    json_path = output_dir / "atp_us_fast_fail_review.json"
    md_path = output_dir / "atp_us_fast_fail_review.md"
    csv_path = output_dir / "atp_us_fast_fail_matrix.csv"
    manifest_path.write_text(json.dumps(_json_ready(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    comparison_rows = _comparison_csv_rows(experiment_rows)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
        writer.writeheader()
        writer.writerows(comparison_rows)

    lines = [
        "# ATP Companion US Fast-Fail Anatomy and Narrow Core-Governance Review",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Shared date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Total wall seconds: `{manifest['total_wall_seconds']}`",
        "",
        "## US Fast-Fail Anatomy",
    ]
    for row in anatomy_rows:
        lines.append(f"### {row['label']}")
        if not row.get("us_slice_available"):
            lines.append(f"- {row['reason']}")
            lines.append("")
            continue
        lines.extend(
            [
                f"- US fast-fail trade count: `{row['us_fast_fail_trade_count']}`",
                f"- Share of total US losers: `{row['share_of_total_us_losers_percent']}`",
                f"- Share of worst drawdown damage: `{row['share_of_worst_drawdown_damage_percent']}`",
                f"- Dominant VWAP quality: `{row['dominant_vwap_quality_state']}`",
                f"- Dominant US subwindow: `{row['explicit_answers']['dominant_us_subwindow']}`",
                "",
            ]
        )
    lines.extend(["## Top Controls", ""])
    for row in ranking[:12]:
        lines.extend(
            [
                f"### {row['label']} / {row['control_label']}",
                f"- US fast-fail count: `{row['us_fast_fail_trade_count']}`",
                f"- Net P&L: `{row['metrics']['net_pnl_cash']}`",
                f"- Delta net / DD / US fast-fails: `{row['delta_vs_target_no_overlay']['net_pnl_cash_delta']}` / `{row['delta_vs_target_no_overlay']['max_drawdown_delta']}` / `{row['delta_vs_target_no_overlay']['us_fast_fail_trade_count_delta']}`",
                f"- False-negative cost count / pnl: `{row['false_negative_cost']['good_us_trades_filtered_count']}` / `{row['false_negative_cost']['good_us_trades_filtered_pnl_cash']}`",
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
            DEFAULT_OUTPUT_ROOT / f"atp_companion_us_fast_fail_review_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_us_fast_fail_review(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="us_fast_fail_review",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
