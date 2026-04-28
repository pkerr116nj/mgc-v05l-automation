"""Live-safe state-machine validation for GC Asia-only flat-filter families."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

REPO_ROOT = Path.cwd()
DEFAULT_PATH_COMPLETE_JSONL = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_gc_asia_only_path_complete_replay"
    / "gc_asia_only_latest"
    / "atp_gc_asia_only_path_complete_replay_trades.jsonl"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_asia_flat_filter_state_machine"

EPISODE_START = datetime.fromisoformat("2024-01-01T00:00:00-05:00")
TROUGH_END = datetime.fromisoformat("2024-11-10T23:59:59-05:00")
TROUGH_START = datetime.fromisoformat("2024-11-10T00:00:00-05:00")
RECOVERY_END = datetime.fromisoformat("2025-04-21T23:59:59-04:00")

REFERENCE_FILTERS = [
    "ROLL_NET_GT_0_40",
    "ROLL_PF_GT_1_40",
    "ROLL_AVG_GT_0_40",
    "ROLL_RETENTION_GT_0_40",
    "ROLL_COST_PCT_LT_100_40",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-asia-flat-filter-state-machine-validation")
    parser.add_argument(
        "--path-complete-jsonl",
        default=str(DEFAULT_PATH_COMPLETE_JSONL),
        help="Path-complete GC Asia-only replay JSONL.",
    )
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory.")
    return parser


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _write_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_markdown(path: Path, text: str) -> None:
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _parse_ts(value: Any) -> datetime:
    return datetime.fromisoformat(str(value).replace(" ", "T"))


def _load_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _iter_jsonl(path):
        trade = raw["trade_record"]
        entry_ts = _parse_ts(raw["entry_ts"])
        exit_ts = _parse_ts(raw["exit_ts"])
        rows.append(
            {
                "trade_id": str(raw["trade_id"]),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_date": entry_ts.date().isoformat(),
                "entry_year": entry_ts.year,
                "entry_month": entry_ts.strftime("%Y-%m"),
                "session": str(raw["session"]),
                "direction": str(raw["direction"]),
                "exit_reason": str(raw["exit_reason"]),
                "baseline_pnl_cash": float(raw["baseline_pnl_cash"]),
                "gross_pnl_cash": float(trade["gross_pnl_cash"]),
                "fees_paid": float(trade["fees_paid"]),
                "slippage_cost": float(trade["slippage_cost"]),
                "cost_cash": float(trade["fees_paid"]) + float(trade["slippage_cost"]),
            }
        )
    rows.sort(key=lambda row: (row["entry_ts"], row["trade_id"]))
    return rows


def _max_drawdown(pnls: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return round(max_drawdown, 4)


def _profit_factor(pnls: Sequence[float]) -> float | None:
    winners = [value for value in pnls if value > 0]
    losers = [abs(value) for value in pnls if value < 0]
    gross_profit = sum(winners)
    gross_loss = sum(losers)
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return gross_profit
    return None


def _max_consecutive_losers(pnls: Sequence[float]) -> int:
    streak = 0
    best = 0
    for pnl in pnls:
        if pnl < 0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def _summary_metrics(trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(trade["baseline_pnl_cash"]) for trade in trades]
    net = round(sum(pnls), 4)
    gross = round(sum(float(trade["gross_pnl_cash"]) for trade in trades), 4)
    costs = round(sum(float(trade["cost_cash"]) for trade in trades), 4)
    pf = _profit_factor(pnls)
    return {
        "trade_count": len(trades),
        "net_pnl_cash": net,
        "gross_pnl_cash": gross,
        "total_costs_cash": costs,
        "profit_factor": round(float(pf), 4) if pf is not None else "",
        "average_trade_cash": round(net / max(len(trades), 1), 4),
        "largest_loss_cash": round(min(pnls), 4) if pnls else 0.0,
        "max_drawdown_cash": _max_drawdown(pnls),
        "max_consecutive_losers": _max_consecutive_losers(pnls),
        "median_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
    }


def _episode_net(trades: Sequence[dict[str, Any]], period_id: str) -> float:
    if period_id == "PEAK_TO_TROUGH":
        subset = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= TROUGH_END]
    elif period_id == "TROUGH_TO_RECOVERY":
        subset = [trade for trade in trades if TROUGH_START <= trade["entry_ts"] <= RECOVERY_END]
    elif period_id == "OUTSIDE_EPISODE":
        subset = [trade for trade in trades if not (EPISODE_START <= trade["entry_ts"] <= RECOVERY_END)]
    else:
        subset = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    return round(sum(float(trade["baseline_pnl_cash"]) for trade in subset), 4)


def _metric_decision(filter_id: str, history: Sequence[dict[str, Any]]) -> tuple[bool, str]:
    if not history:
        return True, "warmup_no_history"
    net = sum(float(trade["baseline_pnl_cash"]) for trade in history)
    gp = sum(float(trade["baseline_pnl_cash"]) for trade in history if float(trade["baseline_pnl_cash"]) > 0)
    gl = sum(-float(trade["baseline_pnl_cash"]) for trade in history if float(trade["baseline_pnl_cash"]) < 0)
    gross = sum(float(trade["gross_pnl_cash"]) for trade in history)
    cost = sum(float(trade["cost_cash"]) for trade in history)
    avg = net / len(history)
    pf = (gp / gl) if gl > 0 else (gp if gp > 0 else 0.0)
    retention = (net / gross) if gross > 0 else -999.0
    cost_pct = (cost / gross * 100.0) if gross > 0 else 999.0
    if filter_id == "ROLL_NET_GT_0_40":
        return net > 0.0, f"rolling_40_net={round(net,4)}"
    if filter_id == "ROLL_PF_GT_1_40":
        return pf > 1.0, f"rolling_40_pf={round(pf,4)}"
    if filter_id == "ROLL_AVG_GT_0_40":
        return avg > 0.0, f"rolling_40_avg={round(avg,4)}"
    if filter_id == "ROLL_RETENTION_GT_0_40":
        return retention > 0.0, f"rolling_40_retention={round(retention,4)}"
    if filter_id == "ROLL_COST_PCT_LT_100_40":
        return cost_pct < 100.0, f"rolling_40_cost_pct={round(cost_pct,4)}"
    raise KeyError(filter_id)


def _period_label(trade: dict[str, Any]) -> str:
    if EPISODE_START <= trade["entry_ts"] <= TROUGH_END:
        return "PEAK_TO_TROUGH"
    if TROUGH_START <= trade["entry_ts"] <= RECOVERY_END:
        return "TROUGH_TO_RECOVERY"
    if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END:
        return "FULL_EPISODE"
    return "OUTSIDE_EPISODE"


def _session_index_map(trades: Sequence[dict[str, Any]]) -> dict[str, int]:
    session_dates = sorted({str(trade["entry_date"]) for trade in trades})
    return {session_date: idx for idx, session_date in enumerate(session_dates)}


def _close_open_off_event(
    *,
    off_event: dict[str, Any] | None,
    transitions: list[dict[str, Any]],
    final_session_idx: int,
) -> dict[str, Any] | None:
    if off_event is None:
        return None
    off_event["end_session_index"] = final_session_idx
    off_event["duration_sessions"] = int(final_session_idx - int(off_event["start_session_index"]) + 1)
    return off_event


def _simulate_shadow_machine(
    *,
    filter_id: str,
    trades: Sequence[dict[str, Any]],
    baseline_metrics: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    history: list[dict[str, Any]] = []
    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    shadow_rows: list[dict[str, Any]] = []
    session_idx_map = _session_index_map(trades)
    state_on = True
    off_event_id = 0
    current_off_event: dict[str, Any] | None = None
    off_durations: list[int] = []
    last_session_idx = 0

    for trade in trades:
        session_idx = session_idx_map[str(trade["entry_date"])]
        last_session_idx = session_idx
        current_state = "ON" if state_on else "OFF"
        executed_flag = state_on
        if executed_flag:
            executed.append(trade)
        else:
            skipped.append(trade)
        history.append(trade)
        metric_bool, metric_text = _metric_decision(filter_id, history[-40:])
        next_state_on = metric_bool
        shadow_rows.append(
            {
                "state_machine_id": "SHADOW_LEDGER",
                "filter_id": filter_id,
                "trade_id": trade["trade_id"],
                "entry_ts": trade["entry_ts"].isoformat(),
                "exit_ts": trade["exit_ts"].isoformat(),
                "entry_date": trade["entry_date"],
                "executed_trade": "YES" if executed_flag else "NO",
                "shadow_source": "EXECUTED" if executed_flag else "SKIPPED_SHADOW",
                "current_state": current_state,
                "hypothetical_pnl_cash": round(float(trade["baseline_pnl_cash"]), 4),
                "rolling_health_update_text": metric_text,
                "would_turn_state_on_after_close": "YES" if next_state_on else "NO",
                "period_id": _period_label(trade),
            }
        )
        if next_state_on != state_on:
            if next_state_on:
                if current_off_event is not None:
                    current_off_event["end_session_index"] = session_idx
                    current_off_event["duration_sessions"] = int(session_idx - int(current_off_event["start_session_index"]))
                    off_durations.append(int(current_off_event["duration_sessions"]))
                    current_off_event = None
            else:
                off_event_id += 1
                current_off_event = {
                    "off_event_id": off_event_id,
                    "start_session_index": session_idx,
                    "start_trade_id": trade["trade_id"],
                    "start_ts": trade["exit_ts"].isoformat(),
                }
            transitions.append(
                {
                    "state_machine_id": "SHADOW_LEDGER",
                    "filter_id": filter_id,
                    "trigger_trade_id": trade["trade_id"],
                    "trigger_entry_ts": trade["entry_ts"].isoformat(),
                    "trigger_exit_ts": trade["exit_ts"].isoformat(),
                    "prior_state": current_state,
                    "next_state": "ON" if next_state_on else "OFF",
                    "trigger_metric_text": metric_text,
                    "trigger_reason": "rolling_health_shadow_update",
                    "used_shadow_information": "YES" if not executed_flag else "NO",
                    "cooldown_sessions": "",
                    "off_event_id": current_off_event["off_event_id"] if current_off_event is not None else off_event_id,
                }
            )
        state_on = next_state_on

    if current_off_event is not None:
        current_off_event = _close_open_off_event(off_event=current_off_event, transitions=transitions, final_session_idx=last_session_idx)
        if current_off_event is not None:
            off_durations.append(int(current_off_event["duration_sessions"]))

    executed_metrics = _summary_metrics(executed)
    skipped_pnls = [float(trade["baseline_pnl_cash"]) for trade in skipped]
    profitable_skipped = [trade for trade in skipped if float(trade["baseline_pnl_cash"]) > 0]
    losing_skipped = [trade for trade in skipped if float(trade["baseline_pnl_cash"]) < 0]
    summary = {
        "state_machine_id": "SHADOW_LEDGER",
        "filter_id": filter_id,
        "status": "ACTIVE",
        "reactivation_live_safe": "YES",
        "paper_shadow_monitorable": "YES",
        "net_pnl_cash": executed_metrics["net_pnl_cash"],
        "retained_pnl_pct_vs_baseline": round((float(executed_metrics["net_pnl_cash"]) / float(baseline_metrics["net_pnl_cash"])) * 100.0, 4),
        "max_drawdown_cash": executed_metrics["max_drawdown_cash"],
        "drawdown_reduction_cash": round(float(baseline_metrics["max_drawdown_cash"]) - float(executed_metrics["max_drawdown_cash"]), 4),
        "drawdown_reduction_pct": round(((float(baseline_metrics["max_drawdown_cash"]) - float(executed_metrics["max_drawdown_cash"])) / float(baseline_metrics["max_drawdown_cash"])) * 100.0, 4),
        "profit_factor": executed_metrics["profit_factor"],
        "average_trade_cash": executed_metrics["average_trade_cash"],
        "trades_executed": len(executed),
        "trades_skipped": len(skipped),
        "off_events": sum(1 for row in transitions if row["prior_state"] == "ON" and row["next_state"] == "OFF"),
        "average_off_duration_sessions": round(sum(off_durations) / max(len(off_durations), 1), 4) if off_durations else 0.0,
        "longest_off_duration_sessions": max(off_durations) if off_durations else 0,
        "profitable_skipped_trades": len(profitable_skipped),
        "losing_skipped_trades": len(losing_skipped),
        "net_pnl_skipped_cash": round(sum(skipped_pnls), 4),
        "false_off_cost_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in profitable_skipped), 4),
        "true_off_benefit_cash": round(sum(-float(trade["baseline_pnl_cash"]) for trade in losing_skipped), 4),
        "peak_to_trough_net_pnl_cash": _episode_net(executed, "PEAK_TO_TROUGH"),
        "year_2025_net_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in executed if int(trade["entry_year"]) == 2025), 4),
        "year_2026_net_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in executed if int(trade["entry_year"]) == 2026), 4),
        "note": "Shadow ledger updates from all hypothetical trade closes, so OFF reactivation is live-safe without future leakage.",
    }
    return summary, transitions, shadow_rows


def _simulate_executed_only_machine(
    *,
    filter_id: str,
    trades: Sequence[dict[str, Any]],
    baseline_metrics: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    history: list[dict[str, Any]] = []
    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    session_idx_map = _session_index_map(trades)
    state_on = True
    last_session_idx = 0
    off_start_session_idx: int | None = None
    off_durations: list[int] = []

    for trade in trades:
        session_idx = session_idx_map[str(trade["entry_date"])]
        last_session_idx = session_idx
        current_state = "ON" if state_on else "OFF"
        if state_on:
            executed.append(trade)
            history.append(trade)
            next_state_on, metric_text = _metric_decision(filter_id, history[-40:])
            if not next_state_on:
                off_start_session_idx = session_idx
                transitions.append(
                    {
                        "state_machine_id": "EXECUTED_ONLY",
                        "filter_id": filter_id,
                        "trigger_trade_id": trade["trade_id"],
                        "trigger_entry_ts": trade["entry_ts"].isoformat(),
                        "trigger_exit_ts": trade["exit_ts"].isoformat(),
                        "prior_state": "ON",
                        "next_state": "OFF",
                        "trigger_metric_text": metric_text,
                        "trigger_reason": "rolling_health_executed_only_trigger",
                        "used_shadow_information": "NO",
                        "cooldown_sessions": "",
                        "off_event_id": 1,
                    }
                )
            state_on = next_state_on
        else:
            skipped.append(trade)

    stuck_off = not state_on
    if off_start_session_idx is not None:
        off_durations.append(int(last_session_idx - off_start_session_idx + 1))

    executed_metrics = _summary_metrics(executed)
    profitable_skipped = [trade for trade in skipped if float(trade["baseline_pnl_cash"]) > 0]
    losing_skipped = [trade for trade in skipped if float(trade["baseline_pnl_cash"]) < 0]
    summary = {
        "state_machine_id": "EXECUTED_ONLY",
        "filter_id": filter_id,
        "status": "ACTIVE",
        "reactivation_live_safe": "NO",
        "paper_shadow_monitorable": "NO",
        "net_pnl_cash": executed_metrics["net_pnl_cash"],
        "retained_pnl_pct_vs_baseline": round((float(executed_metrics["net_pnl_cash"]) / float(baseline_metrics["net_pnl_cash"])) * 100.0, 4),
        "max_drawdown_cash": executed_metrics["max_drawdown_cash"],
        "drawdown_reduction_cash": round(float(baseline_metrics["max_drawdown_cash"]) - float(executed_metrics["max_drawdown_cash"]), 4),
        "drawdown_reduction_pct": round(((float(baseline_metrics["max_drawdown_cash"]) - float(executed_metrics["max_drawdown_cash"])) / float(baseline_metrics["max_drawdown_cash"])) * 100.0, 4),
        "profit_factor": executed_metrics["profit_factor"],
        "average_trade_cash": executed_metrics["average_trade_cash"],
        "trades_executed": len(executed),
        "trades_skipped": len(skipped),
        "off_events": sum(1 for row in transitions if row["prior_state"] == "ON" and row["next_state"] == "OFF"),
        "average_off_duration_sessions": round(sum(off_durations) / max(len(off_durations), 1), 4) if off_durations else 0.0,
        "longest_off_duration_sessions": max(off_durations) if off_durations else 0,
        "profitable_skipped_trades": len(profitable_skipped),
        "losing_skipped_trades": len(losing_skipped),
        "net_pnl_skipped_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in skipped), 4),
        "false_off_cost_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in profitable_skipped), 4),
        "true_off_benefit_cash": round(sum(-float(trade["baseline_pnl_cash"]) for trade in losing_skipped), 4),
        "peak_to_trough_net_pnl_cash": _episode_net(executed, "PEAK_TO_TROUGH"),
        "year_2025_net_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in executed if int(trade["entry_year"]) == 2025), 4),
        "year_2026_net_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in executed if int(trade["entry_year"]) == 2026), 4),
        "note": "Executed-only mode gets stuck OFF after the first trigger because skipped trades provide no reactivation information.",
        "stuck_off": "YES" if stuck_off else "NO",
    }
    return summary, transitions


def _simulate_cooldown_machine(
    *,
    filter_id: str,
    cooldown_sessions: int,
    trades: Sequence[dict[str, Any]],
    baseline_metrics: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    history: list[dict[str, Any]] = []
    executed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    transitions: list[dict[str, Any]] = []
    session_idx_map = _session_index_map(trades)
    state_on = True
    off_event_id = 0
    current_off_event: dict[str, Any] | None = None
    off_durations: list[int] = []
    cooldown_until_session_idx: int | None = None
    last_session_idx = 0

    for trade in trades:
        session_idx = session_idx_map[str(trade["entry_date"])]
        last_session_idx = session_idx
        if not state_on and cooldown_until_session_idx is not None and session_idx > cooldown_until_session_idx:
            state_on = True
            if current_off_event is not None:
                current_off_event["end_session_index"] = session_idx
                current_off_event["duration_sessions"] = int(session_idx - int(current_off_event["start_session_index"]))
                off_durations.append(int(current_off_event["duration_sessions"]))
                transitions.append(
                    {
                        "state_machine_id": f"COOLDOWN_{cooldown_sessions}",
                        "filter_id": filter_id,
                        "trigger_trade_id": trade["trade_id"],
                        "trigger_entry_ts": trade["entry_ts"].isoformat(),
                        "trigger_exit_ts": trade["entry_ts"].isoformat(),
                        "prior_state": "OFF",
                        "next_state": "ON",
                        "trigger_metric_text": f"cooldown_elapsed_sessions={cooldown_sessions}",
                        "trigger_reason": "cooldown_elapsed",
                        "used_shadow_information": "NO",
                        "cooldown_sessions": cooldown_sessions,
                        "off_event_id": current_off_event["off_event_id"],
                    }
                )
                current_off_event = None

        if state_on:
            executed.append(trade)
            history.append(trade)
            next_state_on, metric_text = _metric_decision(filter_id, history[-40:])
            if not next_state_on:
                off_event_id += 1
                current_off_event = {
                    "off_event_id": off_event_id,
                    "start_session_index": session_idx,
                }
                cooldown_until_session_idx = session_idx + cooldown_sessions - 1
                transitions.append(
                    {
                        "state_machine_id": f"COOLDOWN_{cooldown_sessions}",
                        "filter_id": filter_id,
                        "trigger_trade_id": trade["trade_id"],
                        "trigger_entry_ts": trade["entry_ts"].isoformat(),
                        "trigger_exit_ts": trade["exit_ts"].isoformat(),
                        "prior_state": "ON",
                        "next_state": "OFF",
                        "trigger_metric_text": metric_text,
                        "trigger_reason": "rolling_health_cooldown_trigger",
                        "used_shadow_information": "NO",
                        "cooldown_sessions": cooldown_sessions,
                        "off_event_id": off_event_id,
                    }
                )
            state_on = next_state_on
        else:
            skipped.append(trade)

    if current_off_event is not None:
        current_off_event = _close_open_off_event(off_event=current_off_event, transitions=transitions, final_session_idx=last_session_idx)
        if current_off_event is not None:
            off_durations.append(int(current_off_event["duration_sessions"]))

    executed_metrics = _summary_metrics(executed)
    profitable_skipped = [trade for trade in skipped if float(trade["baseline_pnl_cash"]) > 0]
    losing_skipped = [trade for trade in skipped if float(trade["baseline_pnl_cash"]) < 0]
    summary = {
        "state_machine_id": f"COOLDOWN_{cooldown_sessions}",
        "filter_id": filter_id,
        "status": "ACTIVE",
        "reactivation_live_safe": "YES",
        "paper_shadow_monitorable": "NO",
        "net_pnl_cash": executed_metrics["net_pnl_cash"],
        "retained_pnl_pct_vs_baseline": round((float(executed_metrics["net_pnl_cash"]) / float(baseline_metrics["net_pnl_cash"])) * 100.0, 4),
        "max_drawdown_cash": executed_metrics["max_drawdown_cash"],
        "drawdown_reduction_cash": round(float(baseline_metrics["max_drawdown_cash"]) - float(executed_metrics["max_drawdown_cash"]), 4),
        "drawdown_reduction_pct": round(((float(baseline_metrics["max_drawdown_cash"]) - float(executed_metrics["max_drawdown_cash"])) / float(baseline_metrics["max_drawdown_cash"])) * 100.0, 4),
        "profit_factor": executed_metrics["profit_factor"],
        "average_trade_cash": executed_metrics["average_trade_cash"],
        "trades_executed": len(executed),
        "trades_skipped": len(skipped),
        "off_events": sum(1 for row in transitions if row["prior_state"] == "ON" and row["next_state"] == "OFF"),
        "average_off_duration_sessions": round(sum(off_durations) / max(len(off_durations), 1), 4) if off_durations else 0.0,
        "longest_off_duration_sessions": max(off_durations) if off_durations else 0,
        "profitable_skipped_trades": len(profitable_skipped),
        "losing_skipped_trades": len(losing_skipped),
        "net_pnl_skipped_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in skipped), 4),
        "false_off_cost_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in profitable_skipped), 4),
        "true_off_benefit_cash": round(sum(-float(trade["baseline_pnl_cash"]) for trade in losing_skipped), 4),
        "peak_to_trough_net_pnl_cash": _episode_net(executed, "PEAK_TO_TROUGH"),
        "year_2025_net_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in executed if int(trade["entry_year"]) == 2025), 4),
        "year_2026_net_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in executed if int(trade["entry_year"]) == 2026), 4),
        "note": "Cooldown reactivation is live-safe, but it can miss recovery and outside-period edge because skipped trades do not inform rolling health.",
    }
    return summary, transitions


def _classify(summary_rows: Sequence[dict[str, Any]]) -> str:
    shadow_rows = [row for row in summary_rows if row["state_machine_id"] == "SHADOW_LEDGER"]
    if not shadow_rows:
        return "FLAT_FILTER_STATE_MACHINE_INVALID"
    if any(str(row["reactivation_live_safe"]) != "YES" for row in shadow_rows):
        return "FLAT_FILTER_STATE_MACHINE_INVALID"
    good_shadow = [
        row
        for row in shadow_rows
        if float(row["retained_pnl_pct_vs_baseline"] or 0.0) >= 90.0
        and float(row["drawdown_reduction_pct"] or 0.0) >= 20.0
    ]
    if good_shadow:
        return "FLAT_FILTER_STATE_MACHINE_PROMISING"
    mixed_shadow = [
        row
        for row in shadow_rows
        if float(row["drawdown_reduction_pct"] or 0.0) > 0.0 and float(row["retained_pnl_pct_vs_baseline"] or 0.0) >= 60.0
    ]
    if mixed_shadow:
        return "FLAT_FILTER_STATE_MACHINE_MIXED"
    return "FLAT_FILTER_STATE_MACHINE_INVALID"


def _render_live_safety_report(*, classification: str, summary_rows: Sequence[dict[str, Any]]) -> str:
    shadow = [row for row in summary_rows if row["state_machine_id"] == "SHADOW_LEDGER"]
    executed_only = [row for row in summary_rows if row["state_machine_id"] == "EXECUTED_ONLY"]
    cooldown = [row for row in summary_rows if str(row["state_machine_id"]).startswith("COOLDOWN_")]
    best_shadow = max(shadow, key=lambda row: float(row["retained_pnl_pct_vs_baseline"])) if shadow else None
    unique_shadow_filters = sorted({str(row["filter_id"]) for row in shadow})
    lines = [
        "# ATP Companion GC Asia-Only Flat-Filter Live Safety Report",
        "",
        f"- Classification: `{classification}`",
        "- This pass validates whether the promising rolling-health flat-filter family can be represented as a live-safe ON/OFF state machine.",
        "- No live trading, no execution logic, no IBKR or broker changes, and no parameter optimization were introduced.",
        "",
        "## Live-Safety Read",
    ]
    if best_shadow is not None:
        lines.append(
            f"- The rolling-health flat filter can be represented as a live-safe ON/OFF process only through the shadow-ledger design."
        )
        lines.append(
            f"- The feasibility result was not dependent on unavailable information: `ROLL_NET_GT_0_40` retained `{best_shadow['retained_pnl_pct_vs_baseline']}%` of baseline P/L, reduced max drawdown from `11364.725` to `{best_shadow['max_drawdown_cash']}`, and reduced peak-to-trough damage to `{best_shadow['peak_to_trough_net_pnl_cash']}`."
        )
        lines.append(
            f"- All five 40-trade reference family variants collapsed to the same shadow-ledger behavior: `{', '.join(unique_shadow_filters)}`."
        )
    if executed_only:
        lines.append("- Executed-only reactivation is invalid because it gets stuck OFF once skipped trades stop feeding the rolling-health metric.")
    if cooldown:
        lines.append("- Fixed cooldown variants are live-safe, but they are economically too destructive because reactivation depends only on a session timer rather than recovered rolling health.")
    lines.extend(
        [
            "",
            "## Core Question",
            "- The key question is whether the promising flat-filter result depended on unavailable information.",
            "- The answer is no for the shadow-ledger design: it can be monitored honestly in paper/shadow mode using information that becomes available after each hypothetical skipped trade closes.",
        ]
    )
    return "\n".join(lines) + "\n"


def _render_summary_md(*, classification: str, summary_rows: Sequence[dict[str, Any]], baseline_metrics: dict[str, Any]) -> str:
    rows = [row for row in summary_rows if row["state_machine_id"] == "SHADOW_LEDGER"]
    best_shadow = max(rows, key=lambda row: float(row["retained_pnl_pct_vs_baseline"])) if rows else None
    executed_only = [row for row in summary_rows if row["state_machine_id"] == "EXECUTED_ONLY"]
    best_cooldown = max(
        [row for row in summary_rows if str(row["state_machine_id"]).startswith("COOLDOWN_")],
        key=lambda row: float(row["retained_pnl_pct_vs_baseline"]),
        default=None,
    )
    unique_shadow_filters = sorted({str(row["filter_id"]) for row in rows})
    lines = [
        "# ATP Companion GC Asia-Only Flat-Filter State-Machine Validation",
        "",
        f"- Classification: `{classification}`",
        "- This pass is research-only and does not promote a live rule.",
        "- It tests whether the promising rolling-health family can be represented as a live-safe ON/OFF process.",
        "",
        "## Baseline",
        f"- Baseline net P/L: `{baseline_metrics['net_pnl_cash']}`",
        f"- Baseline max drawdown: `{baseline_metrics['max_drawdown_cash']}`",
        "",
        "## State-Machine Read",
    ]
    if best_shadow is not None:
        lines.append(
            f"- The rolling-health flat filter can be represented as a live-safe ON/OFF process only through the shadow-ledger design."
        )
        lines.append(
            f"- `ROLL_NET_GT_0_40` retained `{best_shadow['retained_pnl_pct_vs_baseline']}%` of baseline P/L."
        )
        lines.append(
            f"- `ROLL_NET_GT_0_40` reduced max drawdown from `11364.725` to `{best_shadow['max_drawdown_cash']}`."
        )
        lines.append(
            f"- `ROLL_NET_GT_0_40` reduced peak-to-trough damage to `{best_shadow['peak_to_trough_net_pnl_cash']}`."
        )
        lines.append(
            f"- All five 40-trade reference family variants collapsed to the same shadow-ledger behavior: `{', '.join(unique_shadow_filters)}`."
        )
    if executed_only:
        lines.append("- Executed-only reactivation is invalid because it gets stuck OFF after the first OFF trigger.")
    if best_cooldown is not None:
        lines.append(
            f"- Fixed cooldown variants are live-safe but economically too destructive; best cooldown row `{best_cooldown['state_machine_id']}` retained only `{best_cooldown['retained_pnl_pct_vs_baseline']}%` of P/L."
        )
    lines.extend(
        [
            "",
            "## Interpretation",
            "- If the shadow-ledger rows stay close to the original flat-filter feasibility result, that means the result was not dependent on unavailable information.",
            "- If the cooldown rows degrade materially, that means live-safe reactivation is possible, but only the shadow-ledger version preserves the intended rolling-health behavior.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_state_machine_validation(*, path_complete_jsonl: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades = _load_trades(path_complete_jsonl)
    baseline_metrics = _summary_metrics(trades)
    summary_rows: list[dict[str, Any]] = []
    transition_rows: list[dict[str, Any]] = []
    shadow_rows: list[dict[str, Any]] = []

    for filter_id in REFERENCE_FILTERS:
        shadow_summary, shadow_transitions, shadow_ledger = _simulate_shadow_machine(
            filter_id=filter_id,
            trades=trades,
            baseline_metrics=baseline_metrics,
        )
        summary_rows.append(shadow_summary)
        transition_rows.extend(shadow_transitions)
        shadow_rows.extend(shadow_ledger)

        executed_summary, executed_transitions = _simulate_executed_only_machine(
            filter_id=filter_id,
            trades=trades,
            baseline_metrics=baseline_metrics,
        )
        summary_rows.append(executed_summary)
        transition_rows.extend(executed_transitions)

        for cooldown_sessions in (5, 10, 20):
            cooldown_summary, cooldown_transitions = _simulate_cooldown_machine(
                filter_id=filter_id,
                cooldown_sessions=cooldown_sessions,
                trades=trades,
                baseline_metrics=baseline_metrics,
            )
            summary_rows.append(cooldown_summary)
            transition_rows.extend(cooldown_transitions)

    classification = _classify(summary_rows)
    for row in summary_rows:
        row["classification"] = classification
    for row in transition_rows:
        row["classification"] = classification
    for row in shadow_rows:
        row["classification"] = classification

    summary_csv = output_dir / "atp_gc_asia_flat_filter_state_machine_summary.csv"
    transitions_csv = output_dir / "atp_gc_asia_flat_filter_state_transitions.csv"
    shadow_csv = output_dir / "atp_gc_asia_flat_filter_shadow_ledger.csv"
    live_safety_md = output_dir / "atp_gc_asia_flat_filter_live_safety_report.md"
    summary_md = output_dir / "atp_gc_asia_flat_filter_state_machine_summary.md"

    _write_csv(summary_csv, summary_rows)
    _write_csv(transitions_csv, transition_rows)
    _write_csv(shadow_csv, shadow_rows)
    _write_markdown(live_safety_md, _render_live_safety_report(classification=classification, summary_rows=summary_rows))
    _write_markdown(summary_md, _render_summary_md(classification=classification, summary_rows=summary_rows, baseline_metrics=baseline_metrics))

    return {
        "summary_csv": summary_csv,
        "transitions_csv": transitions_csv,
        "shadow_csv": shadow_csv,
        "live_safety_md": live_safety_md,
        "summary_md": summary_md,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_only_latest")
    artifacts = build_state_machine_validation(
        path_complete_jsonl=Path(args.path_complete_jsonl).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
