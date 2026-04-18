"""ATP Companion failure anatomy and targeted governance review."""

from __future__ import annotations

import argparse
import csv
import hashlib
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
    _coverage_row,
    _discover_best_sources,
    _evaluate_materialized_scope,
    _json_ready,
    _materialize_symbol_truth,
    _serialize_datetime,
    _shared_1m_coverage,
    _trade_windows_by_id,
    build_targets,
)
from .atp_experiment_registry import register_atp_report_output
from ..research.trend_participation.atp_promotion_add_review import (
    default_atp_promotion_add_candidates,
    evaluate_promotion_add_candidate,
)
from ..research.trend_participation.performance_validation import _trade_metrics


@dataclass(frozen=True)
class GovernanceControl:
    control_id: str
    label: str
    core_mode: str = "none"
    core_activation_r: float | None = None
    core_giveback_fraction: float | None = None
    core_min_bars: int = 0
    core_us_only: bool = False
    add_mode: str = "none"
    add_activation_r: float | None = None
    add_giveback_fraction: float | None = None
    add_us_only: bool = False
    add_disable_us: bool = False


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-failure-governance-review")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    parser.add_argument("--start", default=None, help="Optional inclusive ISO timestamp override.")
    parser.add_argument("--end", default=None, help="Optional inclusive ISO timestamp override.")
    return parser


def _review_targets() -> list[EvaluationTarget]:
    wanted = {
        "atp_companion_v1__benchmark_mgc_asia_us",
        "atp_companion_v1__promotion_1_075r_favorable_only",
        "atp_companion_v1__gc_asia__promotion_1_075r_favorable_only",
        "atp_companion_v1__candidate_gc_asia_us",
    }
    return [target for target in build_targets() if target.target_id in wanted]


def _controls_for_target(target: EvaluationTarget) -> list[GovernanceControl]:
    controls = [
        GovernanceControl(control_id="none", label="No governance overlay"),
        GovernanceControl(
            control_id="core_giveback_1.0r_50pct",
            label="Core giveback exit after +1.0R peak, 50% giveback",
            core_mode="giveback",
            core_activation_r=1.0,
            core_giveback_fraction=0.50,
        ),
        GovernanceControl(
            control_id="core_giveback_1.25r_33pct",
            label="Core giveback exit after +1.25R peak, 33% giveback",
            core_mode="giveback",
            core_activation_r=1.25,
            core_giveback_fraction=0.33,
        ),
        GovernanceControl(
            control_id="core_trail_after_1.0r_half_r",
            label="Core trailing exit after +1.0R with 0.5R trail",
            core_mode="trail",
            core_activation_r=1.0,
            core_giveback_fraction=0.50,
        ),
    ]
    if tuple(target.allowed_sessions) == ("ASIA", "US"):
        controls.append(
            GovernanceControl(
                control_id="us_only_core_giveback_1.0r_50pct",
                label="US-only core giveback exit after +1.0R peak, 50% giveback",
                core_mode="giveback",
                core_activation_r=1.0,
                core_giveback_fraction=0.50,
                core_us_only=True,
            )
        )
    if target.candidate_id is not None:
        controls.extend(
            [
                GovernanceControl(
                    control_id="promotion_peel_1.0r",
                    label="Promotion peel at +1.0R",
                    add_mode="peel",
                    add_activation_r=1.0,
                ),
                GovernanceControl(
                    control_id="promotion_peel_1.25r",
                    label="Promotion peel at +1.25R",
                    add_mode="peel",
                    add_activation_r=1.25,
                ),
                GovernanceControl(
                    control_id="promotion_giveback_1.0r_50pct",
                    label="Promotion giveback exit after +1.0R peak, 50% giveback",
                    add_mode="giveback",
                    add_activation_r=1.0,
                    add_giveback_fraction=0.50,
                ),
                GovernanceControl(
                    control_id="promotion_trail_1.0r_half_r",
                    label="Promotion trailing exit after +1.0R with 0.5R trail",
                    add_mode="trail",
                    add_activation_r=1.0,
                    add_giveback_fraction=0.50,
                ),
            ]
        )
        if tuple(target.allowed_sessions) == ("ASIA", "US"):
            controls.extend(
                [
                    GovernanceControl(
                        control_id="us_only_promotion_disable",
                        label="US-only promotion disable",
                        add_disable_us=True,
                    ),
                    GovernanceControl(
                        control_id="us_only_promotion_peel_1.0r",
                        label="US-only promotion peel at +1.0R",
                        add_mode="peel",
                        add_activation_r=1.0,
                        add_us_only=True,
                    ),
                ]
            )
    return controls


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


def _control_hash(controls: Sequence[GovernanceControl]) -> str:
    payload = [
        {
            "control_id": control.control_id,
            "label": control.label,
            "core_mode": control.core_mode,
            "core_activation_r": control.core_activation_r,
            "core_giveback_fraction": control.core_giveback_fraction,
            "core_min_bars": control.core_min_bars,
            "core_us_only": control.core_us_only,
            "add_mode": control.add_mode,
            "add_activation_r": control.add_activation_r,
            "add_giveback_fraction": control.add_giveback_fraction,
            "add_us_only": control.add_us_only,
            "add_disable_us": control.add_disable_us,
        }
        for control in controls
    ]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


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
        hold_minutes=float(len(used_bars) + 1),
        bars_held_1m=len(used_bars) + 1,
        side=trade.side,
        session_segment=trade.session_segment,
        mfe_points=max((float(bar.high) - float(trade.entry_price)) for bar in used_bars) if used_bars else 0.0,
        mae_points=max((float(trade.entry_price) - float(bar.low)) for bar in used_bars) if used_bars else 0.0,
        family=trade.family,
        exit_reason=exit_reason,
    )


def _apply_core_control(
    *,
    trade_row: dict[str, Any],
    trade_bars: Sequence[Any],
    target: EvaluationTarget,
    control: GovernanceControl,
) -> tuple[Any, list[Any]]:
    trade = trade_row["trade_record"]
    if not trade_bars or control.core_mode == "none":
        return trade, list(trade_bars)
    if control.core_us_only and str(trade.session_segment) != "US":
        return trade, list(trade_bars)
    risk = max(float(trade.entry_price) - float(trade.stop_price), 1e-9)
    activation_r = float(control.core_activation_r or 0.0)
    activation_price = float(trade.entry_price) + (risk * activation_r)
    activation_index = next(
        (index for index, bar in enumerate(trade_bars) if float(bar.high) >= activation_price),
        None,
    )
    if activation_index is None:
        return trade, list(trade_bars)
    if control.core_mode == "trail":
        peak_high = float(trade.entry_price)
        for index in range(activation_index, len(trade_bars)):
            bar = trade_bars[index]
            peak_high = max(peak_high, float(bar.high))
            trail_floor = max(float(trade.entry_price), peak_high - (risk * float(control.core_giveback_fraction or 0.5)))
            if float(bar.low) <= trail_floor:
                used_bars = list(trade_bars[: index + 1])
                return (
                    _trade_proxy(
                        trade=trade,
                        exit_ts=bar.end_ts,
                        exit_price=trail_floor,
                        exit_reason=f"overlay_{control.control_id}",
                        point_value=target.point_value,
                        used_bars=used_bars,
                    ),
                    used_bars,
                )
        return trade, list(trade_bars)
    if control.core_mode == "giveback":
        peak_high = float(trade.entry_price)
        giveback_fraction = float(control.core_giveback_fraction or 0.5)
        for index in range(activation_index, len(trade_bars)):
            bar = trade_bars[index]
            peak_high = max(peak_high, float(bar.high))
            threshold = float(trade.entry_price) + ((peak_high - float(trade.entry_price)) * (1.0 - giveback_fraction))
            if float(bar.low) <= threshold:
                used_bars = list(trade_bars[: index + 1])
                return (
                    _trade_proxy(
                        trade=trade,
                        exit_ts=bar.end_ts,
                        exit_price=threshold,
                        exit_reason=f"overlay_{control.control_id}",
                        point_value=target.point_value,
                        used_bars=used_bars,
                    ),
                    used_bars,
                )
        return trade, list(trade_bars)
    return trade, list(trade_bars)


def _simulate_add_exit(
    *,
    row: dict[str, Any],
    trade_bars: Sequence[Any],
    control: GovernanceControl,
    point_value: float,
) -> dict[str, Any]:
    if not row.get("added"):
        return dict(row)
    if control.add_disable_us and str(row.get("session_segment")) == "US":
        updated = dict(row)
        updated["added"] = False
        updated["pnl_cash"] = float(updated.get("trade_pnl_cash") or updated.get("pnl_cash") or 0.0)
        updated["add_pnl_cash"] = 0.0
        updated["add_pnl_points"] = 0.0
        updated["add_reason"] = "US_PROMOTION_DISABLED"
        updated["add_exit_ts"] = None
        return updated
    if control.add_mode == "none":
        return dict(row)
    if control.add_us_only and str(row.get("session_segment")) != "US":
        return dict(row)
    add_entry_ts = row.get("add_entry_ts")
    add_entry_price = row.get("add_entry_price")
    if add_entry_ts is None or add_entry_price is None:
        return dict(row)
    trade_entry_price = float(row.get("position_entry_price") or row.get("entry_price") or add_entry_price)
    trade_stop_price = float(row.get("stop_price") or trade_entry_price)
    risk = max(trade_entry_price - trade_stop_price, 1e-9)
    add_bars = [bar for bar in trade_bars if bar.end_ts >= add_entry_ts]
    if not add_bars:
        return dict(row)
    activation_r = float(control.add_activation_r or 0.0)
    activation_price = float(add_entry_price) + (risk * activation_r)
    activation_index = next(
        (index for index, bar in enumerate(add_bars) if float(bar.high) >= activation_price),
        None,
    )
    if activation_index is None:
        return dict(row)
    add_exit_price = float(row.get("position_exit_price") or row.get("exit_price") or add_entry_price)
    add_exit_ts = row.get("add_exit_ts")
    add_reason = row.get("add_reason") or "PROMOTION_1_EARNED"
    if control.add_mode == "peel":
        trigger_bar = add_bars[activation_index]
        add_exit_price = activation_price
        add_exit_ts = trigger_bar.end_ts
        add_reason = f"overlay_{control.control_id}"
    elif control.add_mode == "trail":
        peak_high = float(add_entry_price)
        for index in range(activation_index, len(add_bars)):
            bar = add_bars[index]
            peak_high = max(peak_high, float(bar.high))
            floor = max(float(add_entry_price), peak_high - (risk * float(control.add_giveback_fraction or 0.5)))
            if float(bar.low) <= floor:
                add_exit_price = floor
                add_exit_ts = bar.end_ts
                add_reason = f"overlay_{control.control_id}"
                break
    elif control.add_mode == "giveback":
        peak_high = float(add_entry_price)
        giveback_fraction = float(control.add_giveback_fraction or 0.5)
        for index in range(activation_index, len(add_bars)):
            bar = add_bars[index]
            peak_high = max(peak_high, float(bar.high))
            floor = float(add_entry_price) + ((peak_high - float(add_entry_price)) * (1.0 - giveback_fraction))
            if float(bar.low) <= floor:
                add_exit_price = floor
                add_exit_ts = bar.end_ts
                add_reason = f"overlay_{control.control_id}"
                break
    updated = dict(row)
    add_pnl_points = float(add_exit_price) - float(add_entry_price)
    add_pnl_cash = round(add_pnl_points * point_value - 1.50, 4)
    updated["add_exit_ts"] = add_exit_ts
    updated["add_reason"] = add_reason
    updated["add_pnl_points"] = round(add_pnl_points, 4)
    updated["add_pnl_cash"] = add_pnl_cash
    updated["pnl_cash"] = round(float(updated.get("trade_pnl_cash") or 0.0) + add_pnl_cash, 4)
    return updated


def _build_candidate_row(
    *,
    base_trade: Any,
    candidate: Any,
    trade_bars: Sequence[Any],
    point_value: float,
) -> dict[str, Any]:
    return evaluate_promotion_add_candidate(
        trade=base_trade,
        minute_bars=trade_bars,
        candidate=candidate,
        point_value=point_value,
    )


def _shape_bucket(row: dict[str, Any]) -> str:
    entry_price = float(row.get("entry_price") or row.get("position_entry_price") or 0.0)
    stop_price = float(row.get("stop_price") or entry_price)
    risk = max(entry_price - stop_price, 1e-9)
    mfe = float(row.get("mfe_points") or 0.0)
    pnl_cash = float(row.get("pnl_cash") or 0.0)
    point_value = abs(float(row.get("point_value") or 1.0))
    pnl_points = pnl_cash / point_value if point_value else 0.0
    retained_fraction = pnl_points / mfe if mfe > 1e-9 else (-1.0 if pnl_points < 0 else 0.0)
    if row.get("added"):
        if float(row.get("add_pnl_cash") or 0.0) > 0.0:
            return "promotion_earned_and_held"
        return "promotion_earned_then_given_back"
    if pnl_cash < 0.0 and mfe < 0.5 * risk:
        return "fail_fast"
    if pnl_cash <= 0.0 and mfe < 1.0 * risk:
        return "never_gets_traction"
    if pnl_cash <= 0.0 and mfe >= 1.0 * risk:
        return "works_then_late_reverses"
    if retained_fraction >= 0.7 and mfe >= 1.0 * risk:
        return "works_immediately_and_holds"
    return "works_then_stalls"


def _taxonomy_summary(*, rows: Sequence[dict[str, Any]], worst_episode_trade_ids: set[str]) -> dict[str, Any]:
    buckets = {
        "fail_fast": [],
        "never_gets_traction": [],
        "works_immediately_and_holds": [],
        "works_then_stalls": [],
        "works_then_late_reverses": [],
        "promotion_earned_and_held": [],
        "promotion_earned_then_given_back": [],
    }
    for row in rows:
        buckets[_shape_bucket(row)].append(row)
    payload: dict[str, Any] = {}
    total_worst = len(worst_episode_trade_ids)
    for bucket, items in buckets.items():
        metrics = _trade_metrics(items, bar_count=max(len(items), 1))
        payload[bucket] = {
            "trade_count": len(items),
            "net_pnl_cash": metrics["net_pnl_cash"],
            "average_trade_pnl_cash": metrics["average_trade_pnl_cash"],
            "max_adverse_contribution": round(min((float(row.get("pnl_cash") or 0.0) for row in items), default=0.0), 4),
            "average_mfe_points": round(sum(float(row.get("mfe_points") or 0.0) for row in items) / len(items), 4) if items else 0.0,
            "average_mae_points": round(sum(float(row.get("mae_points") or 0.0) for row in items) / len(items), 4) if items else 0.0,
            "share_of_worst_drawdown_episodes": round(
                (sum(1 for row in items if str(row.get("trade_id")) in worst_episode_trade_ids) / total_worst) * 100.0,
                4,
            ) if total_worst else 0.0,
        }
    return payload


def _drawdown_episodes(*, rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(rows, key=lambda row: (str(row.get("exit_ts") or row.get("entry_ts")), str(row.get("trade_id"))))
    equity = 0.0
    peak = 0.0
    peak_index = -1
    trough_equity = 0.0
    trough_index = -1
    active = False
    episodes: list[dict[str, Any]] = []
    current_start = 0
    for index, row in enumerate(ordered):
        equity = round(equity + float(row.get("pnl_cash") or 0.0), 6)
        if equity >= peak:
            if active:
                trades = ordered[current_start : index + 1]
                peak_to_trough_loss = round(peak - trough_equity, 4)
                episodes.append(
                    _episode_payload(
                        trades=trades,
                        peak_to_trough_loss=peak_to_trough_loss,
                    )
                )
                active = False
            peak = equity
            peak_index = index
            trough_equity = equity
            trough_index = index
            continue
        if not active:
            active = True
            current_start = peak_index if peak_index >= 0 else index
            trough_equity = equity
            trough_index = index
        elif equity < trough_equity:
            trough_equity = equity
            trough_index = index
    if active:
        trades = ordered[current_start:]
        peak_to_trough_loss = round(peak - trough_equity, 4)
        episodes.append(
            _episode_payload(
                trades=trades,
                peak_to_trough_loss=peak_to_trough_loss,
            )
        )
    episodes = [episode for episode in episodes if episode["peak_to_trough_loss"] > 0.0]
    episodes.sort(key=lambda row: row["peak_to_trough_loss"], reverse=True)
    return episodes[:5]


def _episode_payload(*, trades: Sequence[dict[str, Any]], peak_to_trough_loss: float) -> dict[str, Any]:
    asia = round(sum(float(row.get("pnl_cash") or 0.0) for row in trades if str(row.get("session_segment")) == "ASIA"), 4)
    us = round(sum(float(row.get("pnl_cash") or 0.0) for row in trades if str(row.get("session_segment")) == "US"), 4)
    core = round(sum(float(row.get("trade_pnl_cash") if row.get("trade_pnl_cash") is not None else row.get("pnl_cash") or 0.0) for row in trades), 4)
    add = round(sum(float(row.get("add_pnl_cash") or 0.0) for row in trades), 4)
    largest_loser = abs(min((float(row.get("pnl_cash") or 0.0) for row in trades), default=0.0))
    episode_loss = abs(sum(min(float(row.get("pnl_cash") or 0.0), 0.0) for row in trades))
    if episode_loss > 0 and largest_loser / episode_loss >= 0.5:
        damage_shape = "one_oversized_loser"
    else:
        damage_shape = "cluster"
    early_fail = sum(1 for row in trades if _shape_bucket(row) in {"fail_fast", "never_gets_traction"})
    late_reversal = sum(1 for row in trades if _shape_bucket(row) in {"works_then_late_reverses", "promotion_earned_then_given_back"})
    return {
        "start_timestamp": _serialize_datetime(trades[0].get("entry_ts") or trades[0].get("decision_ts")),
        "end_timestamp": _serialize_datetime(trades[-1].get("exit_ts") or trades[-1].get("entry_ts")),
        "peak_to_trough_loss": peak_to_trough_loss,
        "trade_count": len(trades),
        "session_count": len({str(row.get("session_segment") or "UNKNOWN") for row in trades}),
        "damage_shape": damage_shape,
        "asia_contribution": asia,
        "us_contribution": us,
        "core_probe_contribution": core,
        "promotion_add_contribution": add,
        "promotion_add_share_of_damage_percent": round((abs(min(add, 0.0)) / episode_loss) * 100.0, 4) if episode_loss else 0.0,
        "meaningful_positive_excursion_trade_count": sum(
            1
            for row in trades
            if float(row.get("mfe_points") or 0.0) >= max(
                float(row.get("entry_price") or row.get("position_entry_price") or 0.0)
                - float(row.get("stop_price") or row.get("entry_price") or row.get("position_entry_price") or 0.0),
                1e-9,
            )
        ),
        "late_reversal_trade_count": late_reversal,
        "early_failure_trade_count": early_fail,
        "trade_ids": [str(row.get("trade_id")) for row in trades],
    }


def _dominant_pain(*, rows: Sequence[dict[str, Any]], episodes: Sequence[dict[str, Any]]) -> dict[str, Any]:
    core_negative = abs(sum(min(float(row.get("trade_pnl_cash") if row.get("trade_pnl_cash") is not None else row.get("pnl_cash") or 0.0), 0.0) for row in rows))
    add_negative = abs(sum(min(float(row.get("add_pnl_cash") or 0.0), 0.0) for row in rows))
    if add_negative > core_negative:
        source = "promotion_leg"
    else:
        source = "core_probe"
    asia_loss = abs(sum(min(float(row.get("pnl_cash") or 0.0), 0.0) for row in rows if str(row.get("session_segment")) == "ASIA"))
    us_loss = abs(sum(min(float(row.get("pnl_cash") or 0.0), 0.0) for row in rows if str(row.get("session_segment")) == "US"))
    if us_loss > asia_loss:
        session = "US"
    elif asia_loss > us_loss:
        session = "ASIA"
    else:
        session = "BALANCED"
    early = sum(episode["early_failure_trade_count"] for episode in episodes)
    late = sum(episode["late_reversal_trade_count"] for episode in episodes)
    pain_shape = "late_reversal_profit_giveback" if late > early else "fail_fast"
    return {
        "dominant_source": source,
        "dominant_session": session,
        "dominant_shape": pain_shape,
        "core_negative_pnl_abs": round(core_negative, 4),
        "promotion_negative_pnl_abs": round(add_negative, 4),
        "asia_negative_pnl_abs": round(asia_loss, 4),
        "us_negative_pnl_abs": round(us_loss, 4),
    }


def _attribution_payload(
    *,
    target: EvaluationTarget,
    rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    episodes = _drawdown_episodes(rows=rows)
    worst_trade_ids = {trade_id for episode in episodes for trade_id in episode["trade_ids"]}
    taxonomy = _taxonomy_summary(rows=rows, worst_episode_trade_ids=worst_trade_ids)
    core_total = round(sum(float(row.get("trade_pnl_cash") if row.get("trade_pnl_cash") is not None else row.get("pnl_cash") or 0.0) for row in rows), 4)
    add_total = round(sum(float(row.get("add_pnl_cash") or 0.0) for row in rows), 4)
    return {
        "target_id": target.target_id,
        "label": target.label,
        "symbol": target.symbol,
        "sessions": list(target.allowed_sessions),
        "drawdown_episodes": episodes,
        "trade_shape_taxonomy": taxonomy,
        "core_vs_promotion": {
            "core_probe_pnl": core_total,
            "promotion_add_pnl": add_total,
            "combined_pnl": round(sum(float(row.get("pnl_cash") or 0.0) for row in rows), 4),
            "promotion_share_of_total_pnl_percent": round((add_total / (core_total + add_total)) * 100.0, 4) if (core_total + add_total) else 0.0,
            "promotion_share_of_worst_drawdown_damage_percent": round(
                sum(float(episode["promotion_add_share_of_damage_percent"]) for episode in episodes) / len(episodes),
                4,
            ) if episodes else 0.0,
        },
        "dominant_pain": _dominant_pain(rows=rows, episodes=episodes),
    }


def _baseline_and_candidate_rows(
    *,
    target: EvaluationTarget,
    scope_truth: Any,
    trade_windows_by_id: dict[str, list[Any]],
    candidate_defs: dict[str, Any],
) -> list[dict[str, Any]]:
    if target.candidate_id is None:
        rows = _base_position_rows(scope_truth.trade_rows)
        for row in rows:
            row["point_value"] = target.point_value
            row["trade_pnl_cash"] = float(row.get("pnl_cash") or 0.0)
        return rows
    candidate = candidate_defs[str(target.candidate_id)]
    rows: list[dict[str, Any]] = []
    for trade_row in scope_truth.trade_rows:
        trade = trade_row["trade_record"]
        base_row = _build_candidate_row(
            base_trade=trade,
            candidate=candidate,
            trade_bars=trade_windows_by_id.get(str(trade_row["trade_id"])) or [],
            point_value=target.point_value,
        )
        base_row["trade_id"] = str(trade_row["trade_id"])
        base_row["point_value"] = target.point_value
        base_row["stop_price"] = float(trade.stop_price)
        base_row["entry_price"] = float(trade.entry_price)
        base_row["trade_pnl_cash"] = float(trade.pnl_cash)
        rows.append(base_row)
    return rows


def _apply_control_to_target(
    *,
    target: EvaluationTarget,
    base_scope: Any,
    trade_windows_by_id: dict[str, list[Any]],
    control: GovernanceControl,
    candidate_defs: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    candidate = candidate_defs.get(str(target.candidate_id)) if target.candidate_id else None
    for trade_row in base_scope.trade_rows:
        core_trade, modified_trade_bars = _apply_core_control(
            trade_row=trade_row,
            trade_bars=trade_windows_by_id.get(str(trade_row["trade_id"])) or [],
            target=target,
            control=control,
        )
        if candidate is None:
            row = {
                "trade_id": str(trade_row["trade_id"]),
                **_base_position_rows([{"trade_id": trade_row["trade_id"], "trade_record": core_trade}])[0],
                "point_value": target.point_value,
                "trade_pnl_cash": float(core_trade.pnl_cash),
            }
        else:
            row = _build_candidate_row(
                base_trade=core_trade,
                candidate=candidate,
                trade_bars=modified_trade_bars,
                point_value=target.point_value,
            )
            row["trade_id"] = str(trade_row["trade_id"])
        row["point_value"] = target.point_value
        row["entry_price"] = float(getattr(core_trade, "entry_price", row.get("position_entry_price") or 0.0))
        row["stop_price"] = float(getattr(core_trade, "stop_price", row.get("position_entry_price") or 0.0))
        row["trade_pnl_cash"] = float(getattr(core_trade, "pnl_cash", row.get("trade_pnl_cash") or row.get("pnl_cash") or 0.0))
        row = _simulate_add_exit(
            row=row,
            trade_bars=modified_trade_bars,
            control=control,
            point_value=target.point_value,
        )
        rows.append(row)
    return rows


def _experiment_row(
    *,
    target: EvaluationTarget,
    control: GovernanceControl,
    rows: Sequence[dict[str, Any]],
    no_overlay_rows: Sequence[dict[str, Any]],
    baseline_metrics: dict[str, Any],
    bar_count: int,
    start_timestamp: datetime,
    end_timestamp: datetime,
    wall_seconds: float,
) -> dict[str, Any]:
    metrics = _trade_metrics(rows, bar_count=bar_count)
    no_overlay_metrics = _trade_metrics(no_overlay_rows, bar_count=bar_count)
    add_count = sum(1 for row in rows if row.get("added"))
    add_only_net = round(sum(float(row.get("add_pnl_cash") or 0.0) for row in rows if row.get("added")), 4)
    asia_contribution = round(sum(float(row.get("pnl_cash") or 0.0) for row in rows if str(row.get("session_segment")) == "ASIA"), 4)
    us_contribution = round(sum(float(row.get("pnl_cash") or 0.0) for row in rows if str(row.get("session_segment")) == "US"), 4)
    core_pnl = round(sum(float(row.get("trade_pnl_cash") if row.get("trade_pnl_cash") is not None else row.get("pnl_cash") or 0.0) for row in rows), 4)
    add_pnl = round(sum(float(row.get("add_pnl_cash") or 0.0) for row in rows), 4)
    frozen_delta = None
    if target.target_id == "atp_companion_v1__benchmark_mgc_asia_us":
        frozen_delta = {
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(baseline_metrics["net_pnl_cash"]), 4),
            "profit_factor_delta": round(float(metrics["profit_factor"]) - float(baseline_metrics["profit_factor"]), 4),
        }
    return {
        "target_id": target.target_id,
        "label": target.label,
        "symbol": target.symbol,
        "sessions": list(target.allowed_sessions),
        "target_kind": target.target_kind,
        "control_id": control.control_id,
        "control_label": control.label,
        "execution_model": "ATP_5M_CONTEXT_1M_EXECUTABLE_VWAP",
        "date_span": {
            "start_timestamp": _serialize_datetime(start_timestamp),
            "end_timestamp": _serialize_datetime(end_timestamp),
        },
        "metrics": metrics,
        "add_count": add_count,
        "add_only_net_pnl_cash": add_only_net,
        "asia_contribution": asia_contribution,
        "us_contribution": us_contribution,
        "core_pnl": core_pnl,
        "promotion_add_pnl": add_pnl,
        "delta_vs_target_no_overlay": {
            "net_pnl_cash_delta": round(float(metrics["net_pnl_cash"]) - float(no_overlay_metrics["net_pnl_cash"]), 4),
            "average_trade_pnl_cash_delta": round(float(metrics["average_trade_pnl_cash"]) - float(no_overlay_metrics["average_trade_pnl_cash"]), 4),
            "profit_factor_delta": round(float(metrics["profit_factor"]) - float(no_overlay_metrics["profit_factor"]), 4),
            "max_drawdown_delta": round(float(metrics["max_drawdown"]) - float(no_overlay_metrics["max_drawdown"]), 4),
            "win_rate_delta": round(float(metrics["win_rate"]) - float(no_overlay_metrics["win_rate"]), 4),
        },
        "delta_vs_frozen_benchmark": frozen_delta,
        "wall_time_seconds": round(wall_seconds, 6),
    }


def _experiment_csv_rows(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    payload = []
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
                "net_pnl_cash": row["metrics"]["net_pnl_cash"],
                "average_trade_pnl_cash": row["metrics"]["average_trade_pnl_cash"],
                "profit_factor": row["metrics"]["profit_factor"],
                "max_drawdown": row["metrics"]["max_drawdown"],
                "win_rate": row["metrics"]["win_rate"],
                "add_count": row["add_count"],
                "add_only_net_pnl_cash": row["add_only_net_pnl_cash"],
                "asia_contribution": row["asia_contribution"],
                "us_contribution": row["us_contribution"],
                "core_pnl": row["core_pnl"],
                "promotion_add_pnl": row["promotion_add_pnl"],
                "delta_vs_target_no_overlay_net_pnl_cash": row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"],
                "delta_vs_target_no_overlay_profit_factor": row["delta_vs_target_no_overlay"]["profit_factor_delta"],
                "delta_vs_target_no_overlay_max_drawdown": row["delta_vs_target_no_overlay"]["max_drawdown_delta"],
                "delta_vs_frozen_benchmark_net_pnl_cash": (row["delta_vs_frozen_benchmark"] or {}).get("net_pnl_cash_delta"),
                "delta_vs_frozen_benchmark_profit_factor": (row["delta_vs_frozen_benchmark"] or {}).get("profit_factor_delta"),
                "wall_time_seconds": row["wall_time_seconds"],
            }
        )
    return payload


def run_failure_governance_review(
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
    scope_truths = {
        (target.symbol, target.allowed_sessions): _evaluate_materialized_scope(
            symbol_truth=symbol_truths[target.symbol],
            allowed_sessions=target.allowed_sessions,
            point_value=target.point_value,
        )
        for target in targets
    }
    trade_windows_by_scope = {
        key: _trade_windows_by_id(bars_1m=truth.bars_1m, trade_rows=truth.trade_rows)
        for key, truth in scope_truths.items()
    }

    no_overlay_rows_by_target: dict[str, list[dict[str, Any]]] = {}
    attribution_rows: list[dict[str, Any]] = []
    for target in targets:
        no_overlay_rows = _baseline_and_candidate_rows(
            target=target,
            scope_truth=scope_truths[(target.symbol, target.allowed_sessions)],
            trade_windows_by_id=trade_windows_by_scope[(target.symbol, target.allowed_sessions)],
            candidate_defs=candidate_defs,
        )
        no_overlay_rows_by_target[target.target_id] = no_overlay_rows
        attribution_rows.append(
            _attribution_payload(
                target=target,
                rows=no_overlay_rows,
            )
        )

    frozen_baseline_target = next(target for target in targets if target.target_id == "atp_companion_v1__benchmark_mgc_asia_us")
    frozen_baseline_metrics = _trade_metrics(
        no_overlay_rows_by_target[frozen_baseline_target.target_id],
        bar_count=scope_truths[(frozen_baseline_target.symbol, frozen_baseline_target.allowed_sessions)].bar_count,
    )

    experiment_rows: list[dict[str, Any]] = []
    for target in targets:
        base_scope = scope_truths[(target.symbol, target.allowed_sessions)]
        no_overlay_rows = no_overlay_rows_by_target[target.target_id]
        for control in controls_by_target[target.target_id]:
            overlay_started = perf_counter()
            rows = (
                no_overlay_rows
                if control.control_id == "none"
                else _apply_control_to_target(
                    target=target,
                    base_scope=base_scope,
                    trade_windows_by_id=trade_windows_by_scope[(target.symbol, target.allowed_sessions)],
                    control=control,
                    candidate_defs=candidate_defs,
                )
            )
            experiment_rows.append(
                _experiment_row(
                    target=target,
                    control=control,
                    rows=rows,
                    no_overlay_rows=no_overlay_rows,
                    baseline_metrics=frozen_baseline_metrics,
                    bar_count=base_scope.bar_count,
                    start_timestamp=run_start,
                    end_timestamp=run_end,
                    wall_seconds=perf_counter() - overlay_started,
                )
            )

    comparison_rows = _experiment_csv_rows(experiment_rows)
    ranking = sorted(
        experiment_rows,
        key=lambda row: (
            float(row["delta_vs_target_no_overlay"]["net_pnl_cash_delta"]),
            -float(row["delta_vs_target_no_overlay"]["max_drawdown_delta"]),
            float(row["metrics"]["profit_factor"]),
        ),
        reverse=True,
    )
    manifest = {
        "artifact_version": "atp_failure_governance_review_v1",
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
            "materialized_truth_path": str((output_dir / "atp_failure_governance_materialized_truth.json").resolve()),
            "benchmark_semantics_changed": False,
        },
        "total_wall_seconds": round(perf_counter() - started_at, 6),
    }
    materialized_truth = {
        "artifact_version": manifest["artifact_version"],
        "source_date_span": manifest["source_date_span"],
        "targets": [
            {
                "target_id": target.target_id,
                "label": target.label,
                "symbol": target.symbol,
                "sessions": list(target.allowed_sessions),
                "target_kind": target.target_kind,
                "point_value": target.point_value,
                "rows": no_overlay_rows_by_target[target.target_id],
            }
            for target in targets
        ],
    }
    payload = {
        "study": "ATP Companion failure anatomy and targeted governance review",
        "manifest": manifest,
        "drawdown_attribution": attribution_rows,
        "targeted_exit_matrix": experiment_rows,
        "ranking": [
            {
                "target_id": row["target_id"],
                "control_id": row["control_id"],
                "label": row["label"],
                "control_label": row["control_label"],
                "metrics": row["metrics"],
                "delta_vs_target_no_overlay": row["delta_vs_target_no_overlay"],
            }
            for row in ranking
        ],
    }

    manifest_path = output_dir / "atp_failure_governance_manifest.json"
    truth_path = output_dir / "atp_failure_governance_materialized_truth.json"
    json_path = output_dir / "atp_failure_governance_review.json"
    csv_path = output_dir / "atp_failure_governance_matrix.csv"
    md_path = output_dir / "atp_failure_governance_review.md"
    manifest_path.write_text(json.dumps(_json_ready(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    truth_path.write_text(json.dumps(_json_ready(materialized_truth), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    json_path.write_text(json.dumps(_json_ready(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(comparison_rows[0].keys()))
        writer.writeheader()
        writer.writerows(comparison_rows)
    lines = [
        "# ATP Companion Failure Anatomy and Targeted Governance Review",
        "",
        f"- Source DB: `{source_db.resolve()}`",
        f"- Shared date span: `{run_start.isoformat()}` -> `{run_end.isoformat()}`",
        f"- Total wall seconds: `{manifest['total_wall_seconds']}`",
        "",
        "## Dominant Pain by Target",
    ]
    for row in attribution_rows:
        dominant = row["dominant_pain"]
        lines.extend(
            [
                f"### {row['label']}",
                f"- Dominant source: `{dominant['dominant_source']}`",
                f"- Dominant session: `{dominant['dominant_session']}`",
                f"- Dominant shape: `{dominant['dominant_shape']}`",
                "",
            ]
        )
    lines.extend(["## Top Overlay Rows", ""])
    for row in ranking[:12]:
        lines.extend(
            [
                f"### {row['label']} / {row['control_label']}",
                f"- Net P&L: `{row['metrics']['net_pnl_cash']}`",
                f"- PF: `{row['metrics']['profit_factor']}`",
                f"- Max drawdown: `{row['metrics']['max_drawdown']}`",
                f"- Delta vs target no-overlay net / PF / DD: `{row['delta_vs_target_no_overlay']['net_pnl_cash_delta']}` / `{row['delta_vs_target_no_overlay']['profit_factor_delta']}` / `{row['delta_vs_target_no_overlay']['max_drawdown_delta']}`",
                "",
            ]
        )
    md_path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    return {
        "manifest_path": manifest_path,
        "materialized_truth_path": truth_path,
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
            DEFAULT_OUTPUT_ROOT / f"atp_companion_failure_governance_review_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}"
        ).resolve()
    start_timestamp = datetime.fromisoformat(args.start) if args.start else None
    end_timestamp = datetime.fromisoformat(args.end) if args.end else None
    result = run_failure_governance_review(
        source_db=source_db,
        output_dir=output_dir,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    registry_result = register_atp_report_output(
        strategy_variant="failure_governance_review",
        payload_json_path=Path(result["json_path"]),
        artifacts=result,
    )
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, sort_keys=True))
    print(json.dumps({"registry_path": registry_result["manifest_path"]}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
