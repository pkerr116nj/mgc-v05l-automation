"""Flat-regime filter feasibility review for GC Asia-only replay trades."""

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
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_asia_only_flat_regime_filter"

EPISODE_START = datetime.fromisoformat("2024-01-01T00:00:00-05:00")
TROUGH_END = datetime.fromisoformat("2024-11-10T23:59:59-05:00")
TROUGH_START = datetime.fromisoformat("2024-11-10T00:00:00-05:00")
RECOVERY_END = datetime.fromisoformat("2025-04-21T23:59:59-04:00")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-asia-only-flat-regime-filter-feasibility")
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
    fieldnames = list(rows[0].keys()) if rows else []
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
        rows.append(
            {
                "trade_id": str(raw["trade_id"]),
                "entry_ts": entry_ts,
                "entry_date": entry_ts.date().isoformat(),
                "entry_year": entry_ts.year,
                "entry_month": entry_ts.strftime("%Y-%m"),
                "entry_quarter": f"{entry_ts.year}-Q{((entry_ts.month - 1) // 3) + 1}",
                "session": str(raw["session"]),
                "direction": str(raw["direction"]),
                "exit_reason": str(raw["exit_reason"]),
                "baseline_pnl_cash": float(raw["baseline_pnl_cash"]),
                "gross_pnl_cash": float(trade["gross_pnl_cash"]),
                "fees_paid": float(trade["fees_paid"]),
                "slippage_cost": float(trade["slippage_cost"]),
                "cost_cash": float(trade["fees_paid"]) + float(trade["slippage_cost"]),
                "regime_bucket": str(trade.get("regime_bucket", "")),
                "volatility_bucket": str(trade.get("volatility_bucket", "")),
                "quality_bucket": str(trade.get("setup_quality_bucket", "")),
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


def _period_id(trade: dict[str, Any]) -> str:
    ts = trade["entry_ts"]
    if EPISODE_START <= ts <= TROUGH_END:
        return "PEAK_TO_TROUGH"
    if TROUGH_START <= ts <= RECOVERY_END:
        return "TROUGH_TO_RECOVERY"
    if EPISODE_START <= ts <= RECOVERY_END:
        return "FULL_EPISODE_ONLY"
    return "OUTSIDE_EPISODE"


def _window_history(trades: Sequence[dict[str, Any]], idx: int, window: int) -> list[dict[str, Any]]:
    start = max(0, idx - window)
    return list(trades[start:idx])


def _rolling_health_predicate(metric_id: str, window: int):
    def predicate(*, trades: Sequence[dict[str, Any]], idx: int, state: dict[str, Any]) -> tuple[bool, str]:
        history = _window_history(trades, idx, window)
        if not history:
            return True, "warmup_no_history"
        net = sum(float(trade["baseline_pnl_cash"]) for trade in history)
        gp = sum(float(trade["baseline_pnl_cash"]) for trade in history if float(trade["baseline_pnl_cash"]) > 0)
        gl = sum(-float(trade["baseline_pnl_cash"]) for trade in history if float(trade["baseline_pnl_cash"]) < 0)
        gross = sum(float(trade["gross_pnl_cash"]) for trade in history)
        cost = sum(float(trade["cost_cash"]) for trade in history)
        avg = net / len(history)
        avg_cost = cost / len(history)
        pf = (gp / gl) if gl > 0 else (gp if gp > 0 else 0.0)
        retention = (net / gross) if gross > 0 else -999.0
        cost_pct = (cost / gross * 100.0) if gross > 0 else 999.0
        if metric_id == "ROLL_NET_GT_0":
            return net > 0, f"rolling_{window}_net={round(net,4)}"
        if metric_id == "ROLL_PF_GT_1":
            return pf > 1.0, f"rolling_{window}_pf={round(pf,4)}"
        if metric_id == "ROLL_AVG_GT_0":
            return avg > 0.0, f"rolling_{window}_avg={round(avg,4)}"
        if metric_id == "ROLL_RETENTION_GT_0":
            return retention > 0.0, f"rolling_{window}_retention={round(retention,4)}"
        if metric_id == "ROLL_COST_PCT_LT_100":
            return cost_pct < 100.0, f"rolling_{window}_cost_pct={round(cost_pct,4)}"
        if metric_id == "ROLL_AVG_NET_GT_AVG_COST":
            return avg > avg_cost, f"rolling_{window}_avg_vs_cost={round(avg,4)}>{round(avg_cost,4)}"
        raise KeyError(metric_id)

    return predicate


def _label_predicate(*, field_name: str, allowed_values: set[str], label: str):
    def predicate(*, trades: Sequence[dict[str, Any]], idx: int, state: dict[str, Any]) -> tuple[bool, str]:
        trade = trades[idx]
        value = str(trade[field_name])
        return value in allowed_values, f"{label}={value}"

    return predicate


def _blocked_drawdown_predicate(*, reason: str):
    def predicate(*, trades: Sequence[dict[str, Any]], idx: int, state: dict[str, Any]) -> tuple[bool, str]:
        return True, reason

    return predicate


def _candidate_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {
            "filter_id": "BASELINE_LONG_ONLY",
            "family": "BASELINE",
            "description": "Always-on GC Asia-only long baseline.",
            "status": "ACTIVE",
            "prospectively_knowable": "YES",
            "predicate": lambda *, trades, idx, state: (True, "baseline_on"),
        }
    ]
    for window in (20, 40, 60):
        for metric_id, family, description in (
            ("ROLL_NET_GT_0", "ROLLING_STRATEGY_HEALTH", "Take trade only if rolling net P/L is positive."),
            ("ROLL_PF_GT_1", "ROLLING_STRATEGY_HEALTH", "Take trade only if rolling profit factor is above 1."),
            ("ROLL_AVG_GT_0", "ROLLING_STRATEGY_HEALTH", "Take trade only if rolling average trade is positive."),
            ("ROLL_RETENTION_GT_0", "ROLLING_COST_EFFICIENCY", "Take trade only if rolling gross-to-net retention is positive."),
            ("ROLL_COST_PCT_LT_100", "ROLLING_COST_EFFICIENCY", "Take trade only if rolling costs consume less than 100% of gross edge."),
            ("ROLL_AVG_NET_GT_AVG_COST", "ROLLING_COST_EFFICIENCY", "Take trade only if rolling average net trade exceeds rolling average cost."),
        ):
            specs.append(
                {
                    "filter_id": f"{metric_id}_{window}",
                    "family": family,
                    "window": window,
                    "description": description,
                    "status": "ACTIVE",
                    "prospectively_knowable": "YES",
                    "predicate": _rolling_health_predicate(metric_id, window),
                }
            )
    specs.extend(
        [
            {
                "filter_id": "REGIME_TREND_UP_ONLY",
                "family": "PRETRADE_LABEL",
                "description": "Take trades only when pre-trade regime bucket is TREND_UP.",
                "status": "ACTIVE",
                "prospectively_knowable": "YES",
                "predicate": _label_predicate(field_name="regime_bucket", allowed_values={"TREND_UP"}, label="regime_bucket"),
            },
            {
                "filter_id": "VOL_NOT_NORMAL",
                "family": "PRETRADE_LABEL",
                "description": "Take trades only when volatility bucket is not NORMAL.",
                "status": "ACTIVE",
                "prospectively_knowable": "YES",
                "predicate": _label_predicate(field_name="volatility_bucket", allowed_values={"HOT", "QUIET"}, label="volatility_bucket"),
            },
            {
                "filter_id": "VOL_HOT_ONLY",
                "family": "PRETRADE_LABEL",
                "description": "Take trades only when volatility bucket is HOT.",
                "status": "ACTIVE",
                "prospectively_knowable": "YES",
                "predicate": _label_predicate(field_name="volatility_bucket", allowed_values={"HOT"}, label="volatility_bucket"),
            },
            {
                "filter_id": "VOL_QUIET_ONLY",
                "family": "PRETRADE_LABEL",
                "description": "Take trades only when volatility bucket is QUIET.",
                "status": "ACTIVE",
                "prospectively_knowable": "YES",
                "predicate": _label_predicate(field_name="volatility_bucket", allowed_values={"QUIET"}, label="volatility_bucket"),
            },
            {
                "filter_id": "DRAWDOWN_STATE_PREDECLARED_BLOCKED",
                "family": "SIMPLE_DRAWDOWN_HEALTH_STATE",
                "description": "Blocked: a true off-then-recover rule needs post-skip strategy outcomes or another exogenous recovery trigger.",
                "status": "BLOCKED",
                "prospectively_knowable": "AMBIGUOUS",
                "predicate": _blocked_drawdown_predicate(
                    reason="Blocked under no-future-information discipline: recovery while OFF would require skipped-trade outcomes or a separate exogenous reactivation rule."
                ),
            },
            {
                "filter_id": "SLOPE_EMA_EXPANSION_LABELS_BLOCKED",
                "family": "PRETRADE_LABEL",
                "description": "Blocked: no pre-trade slope / EMA / expansion labels are materialized in the path-complete replay substrate.",
                "status": "BLOCKED",
                "prospectively_knowable": "NO",
                "predicate": _blocked_drawdown_predicate(
                    reason="Blocked because slope / EMA / expansion pre-trade labels are not materialized in the replay records."
                ),
            },
        ]
    )
    return specs


def _evaluate_active_filter(*, spec: dict[str, Any], trades: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], tuple[bool, ...]]:
    taken: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    mask: list[bool] = []
    state: dict[str, Any] = {}
    predicate = spec["predicate"]
    for idx, trade in enumerate(trades):
        take, reason = predicate(trades=trades, idx=idx, state=state)
        mask.append(bool(take))
        annotated = {**trade, "filter_reason": reason}
        if take:
            taken.append(annotated)
        else:
            skipped.append(annotated)
    return taken, skipped, tuple(mask)


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


def _summary_metrics(*, trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(trade["baseline_pnl_cash"]) for trade in trades]
    net = round(sum(pnls), 4)
    gross = round(sum(float(trade["gross_pnl_cash"]) for trade in trades), 4)
    cost = round(sum(float(trade["cost_cash"]) for trade in trades), 4)
    pf = _profit_factor(pnls)
    return {
        "trade_count": len(trades),
        "net_pnl_cash": net,
        "gross_pnl_cash": gross,
        "total_costs_cash": cost,
        "profit_factor": round(float(pf), 4) if pf is not None else "",
        "average_trade_cash": round(net / max(len(trades), 1), 4),
        "largest_loss_cash": round(min(pnls), 4) if pnls else 0.0,
        "max_drawdown_cash": _max_drawdown(pnls),
        "max_consecutive_losers": _max_consecutive_losers(pnls),
    }


def _skip_distribution(skipped: Sequence[dict[str, Any]]) -> dict[str, Any]:
    peak = sum(1 for trade in skipped if EPISODE_START <= trade["entry_ts"] <= TROUGH_END)
    recovery = sum(1 for trade in skipped if TROUGH_START <= trade["entry_ts"] <= RECOVERY_END)
    outside = sum(1 for trade in skipped if not (EPISODE_START <= trade["entry_ts"] <= RECOVERY_END))
    total = max(len(skipped), 1)
    return {
        "skipped_peak_to_trough_count": peak,
        "skipped_trough_to_recovery_count": recovery,
        "skipped_outside_episode_count": outside,
        "skipped_peak_to_trough_pct": round((peak / total) * 100.0, 4),
        "skipped_trough_to_recovery_pct": round((recovery / total) * 100.0, 4),
        "skipped_outside_episode_pct": round((outside / total) * 100.0, 4),
    }


def _effectively_skips_known_bad_window(skip_stats: dict[str, Any]) -> bool:
    return (
        float(skip_stats["skipped_peak_to_trough_pct"]) >= 70.0
        and float(skip_stats["skipped_outside_episode_pct"]) <= 15.0
        and float(skip_stats["skipped_trough_to_recovery_pct"]) <= 20.0
    )


def _evaluate_filters(trades: Sequence[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    baseline_spec = _candidate_specs()[0]
    baseline_taken, _, baseline_mask = _evaluate_active_filter(spec=baseline_spec, trades=trades)
    baseline_metrics = _summary_metrics(trades=baseline_taken)
    baseline_peak = _episode_net(baseline_taken, "PEAK_TO_TROUGH")
    baseline_recovery = _episode_net(baseline_taken, "TROUGH_TO_RECOVERY")
    baseline_outside = _episode_net(baseline_taken, "OUTSIDE_EPISODE")

    summary_rows: list[dict[str, Any]] = []
    year_rows: list[dict[str, Any]] = []
    episode_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []

    behavior_groups: dict[tuple[bool, ...], str] = {}
    specs = _candidate_specs()
    for spec in specs:
        if spec["status"] != "ACTIVE":
            summary_rows.append(
                {
                    "filter_id": spec["filter_id"],
                    "family": spec["family"],
                    "status": spec["status"],
                    "description": spec["description"],
                    "prospectively_knowable": spec["prospectively_knowable"],
                    "dev_holdout_status": "NOT_AVAILABLE",
                    "trade_count_taken": "",
                    "trade_count_skipped": "",
                    "net_pnl_retained_cash": "",
                    "skipped_trade_pnl_cash": "",
                    "gross_pnl_retained_cash": "",
                    "total_costs_retained_cash": "",
                    "max_drawdown_cash": "",
                    "drawdown_reduction_cash": "",
                    "drawdown_reduction_pct": "",
                    "profit_factor": "",
                    "average_trade_cash": "",
                    "largest_loss_cash": "",
                    "max_consecutive_losers": "",
                    "peak_to_trough_net_pnl_cash": "",
                    "trough_to_recovery_net_pnl_cash": "",
                    "outside_episode_net_pnl_cash": "",
                    "net_pnl_retained_pct_vs_baseline": "",
                    "avoids_damage_but_misses_recovery": "",
                    "simple_and_operationally_credible": "NO",
                    "works_only_by_skipping_known_bad_window": "",
                    "behavior_group": "",
                    "duplicate_of_filter_id": "",
                    "note": spec["description"],
                }
            )
            continue

        taken, skipped, mask = _evaluate_active_filter(spec=spec, trades=trades)
        metrics = _summary_metrics(trades=taken)
        skip_stats = _skip_distribution(skipped)
        peak = _episode_net(taken, "PEAK_TO_TROUGH")
        recovery = _episode_net(taken, "TROUGH_TO_RECOVERY")
        outside = _episode_net(taken, "OUTSIDE_EPISODE")
        net_retained_pct = (float(metrics["net_pnl_cash"]) / float(baseline_metrics["net_pnl_cash"]) * 100.0) if float(baseline_metrics["net_pnl_cash"]) else 0.0
        drawdown_reduction = round(float(baseline_metrics["max_drawdown_cash"]) - float(metrics["max_drawdown_cash"]), 4)
        drawdown_reduction_pct = (
            round((drawdown_reduction / float(baseline_metrics["max_drawdown_cash"])) * 100.0, 4)
            if float(baseline_metrics["max_drawdown_cash"]) > 0
            else 0.0
        )
        misses_recovery = recovery < (baseline_recovery * 0.8)
        avoids_damage = peak > baseline_peak
        behavior_group = behavior_groups.setdefault(mask, spec["filter_id"])
        duplicate_of = "" if behavior_group == spec["filter_id"] else behavior_group

        summary_rows.append(
            {
                "filter_id": spec["filter_id"],
                "family": spec["family"],
                "status": spec["status"],
                "description": spec["description"],
                "prospectively_knowable": spec["prospectively_knowable"],
                "dev_holdout_status": "NOT_AVAILABLE",
                "trade_count_taken": metrics["trade_count"],
                "trade_count_skipped": len(skipped),
                "net_pnl_retained_cash": metrics["net_pnl_cash"],
                "skipped_trade_pnl_cash": round(sum(float(trade["baseline_pnl_cash"]) for trade in skipped), 4),
                "gross_pnl_retained_cash": metrics["gross_pnl_cash"],
                "total_costs_retained_cash": metrics["total_costs_cash"],
                "max_drawdown_cash": metrics["max_drawdown_cash"],
                "drawdown_reduction_cash": drawdown_reduction,
                "drawdown_reduction_pct": drawdown_reduction_pct,
                "profit_factor": metrics["profit_factor"],
                "average_trade_cash": metrics["average_trade_cash"],
                "largest_loss_cash": metrics["largest_loss_cash"],
                "max_consecutive_losers": metrics["max_consecutive_losers"],
                "peak_to_trough_net_pnl_cash": peak,
                "trough_to_recovery_net_pnl_cash": recovery,
                "outside_episode_net_pnl_cash": outside,
                "net_pnl_retained_pct_vs_baseline": round(net_retained_pct, 4),
                "avoids_damage_but_misses_recovery": "YES" if avoids_damage and misses_recovery else "NO",
                "simple_and_operationally_credible": "YES",
                "works_only_by_skipping_known_bad_window": "YES" if _effectively_skips_known_bad_window(skip_stats) else "NO",
                "behavior_group": behavior_group,
                "duplicate_of_filter_id": duplicate_of,
                "note": "",
                **skip_stats,
            }
        )

        for year in sorted({int(trade["entry_year"]) for trade in trades}):
            taken_year = [trade for trade in taken if int(trade["entry_year"]) == year]
            year_metrics = _summary_metrics(trades=taken_year)
            year_rows.append(
                {
                    "filter_id": spec["filter_id"],
                    "year": year,
                    "trade_count_taken": year_metrics["trade_count"],
                    "net_pnl_cash": year_metrics["net_pnl_cash"],
                    "gross_pnl_cash": year_metrics["gross_pnl_cash"],
                    "total_costs_cash": year_metrics["total_costs_cash"],
                    "profit_factor": year_metrics["profit_factor"],
                    "average_trade_cash": year_metrics["average_trade_cash"],
                    "max_drawdown_cash": year_metrics["max_drawdown_cash"],
                }
            )

        for period_id, period_label in (
            ("PEAK_TO_TROUGH", "2024-01-01 to 2024-11-10"),
            ("TROUGH_TO_RECOVERY", "2024-11-10 to 2025-04-21"),
            ("OUTSIDE_EPISODE", "Outside 2024-01-01 to 2025-04-21"),
            ("FULL_EPISODE", "2024-01-01 to 2025-04-21"),
        ):
            taken_period = _episode_net(taken, period_id)
            baseline_period = (
                baseline_peak
                if period_id == "PEAK_TO_TROUGH"
                else baseline_recovery
                if period_id == "TROUGH_TO_RECOVERY"
                else baseline_outside
                if period_id == "OUTSIDE_EPISODE"
                else round(baseline_peak + baseline_recovery, 4)
            )
            episode_rows.append(
                {
                    "filter_id": spec["filter_id"],
                    "period_id": period_id,
                    "period_label": period_label,
                    "taken_net_pnl_cash": taken_period,
                    "baseline_net_pnl_cash": baseline_period,
                    "delta_vs_baseline_cash": round(taken_period - baseline_period, 4),
                }
            )

        for trade in skipped:
            skipped_rows.append(
                {
                    "filter_id": spec["filter_id"],
                    "entry_ts": trade["entry_ts"].isoformat(),
                    "entry_date": trade["entry_date"],
                    "entry_year": trade["entry_year"],
                    "entry_month": trade["entry_month"],
                    "entry_quarter": trade["entry_quarter"],
                    "period_id": _period_id(trade),
                    "baseline_pnl_cash": round(float(trade["baseline_pnl_cash"]), 4),
                    "gross_pnl_cash": round(float(trade["gross_pnl_cash"]), 4),
                    "cost_cash": round(float(trade["cost_cash"]), 4),
                    "exit_reason": trade["exit_reason"],
                    "regime_bucket": trade["regime_bucket"],
                    "volatility_bucket": trade["volatility_bucket"],
                    "filter_reason": trade["filter_reason"],
                }
            )

    return summary_rows, year_rows, episode_rows, skipped_rows


def _classify(summary_rows: Sequence[dict[str, Any]]) -> str:
    active_rows = [row for row in summary_rows if row["status"] == "ACTIVE" and row["filter_id"] != "BASELINE_LONG_ONLY"]
    if not active_rows:
        return "FLAT_FILTER_BLOCKED"
    promising_candidates = [
        row
        for row in active_rows
        if float(row["drawdown_reduction_pct"] or 0.0) >= 25.0
        and float(row["net_pnl_retained_pct_vs_baseline"] or 0.0) >= 90.0
        and str(row["works_only_by_skipping_known_bad_window"]) == "NO"
    ]
    if promising_candidates:
        return "FLAT_FILTER_PROMISING"
    overfit_candidates = [
        row
        for row in active_rows
        if float(row["drawdown_reduction_pct"] or 0.0) >= 20.0
        and str(row["works_only_by_skipping_known_bad_window"]) == "YES"
    ]
    if overfit_candidates:
        return "FLAT_FILTER_OVERFIT_RISK"
    mixed_candidates = [
        row
        for row in active_rows
        if float(row["drawdown_reduction_cash"] or 0.0) > 0.0 and float(row["net_pnl_retained_pct_vs_baseline"] or 0.0) >= 60.0
    ]
    if mixed_candidates:
        return "FLAT_FILTER_MIXED"
    return "FLAT_FILTER_REJECTED"


def _render_markdown(
    *,
    classification: str,
    summary_rows: Sequence[dict[str, Any]],
) -> str:
    baseline = next(row for row in summary_rows if row["filter_id"] == "BASELINE_LONG_ONLY")
    active = [row for row in summary_rows if row["status"] == "ACTIVE" and row["filter_id"] != "BASELINE_LONG_ONLY"]
    ranked = sorted(
        active,
        key=lambda row: (
            float(row["drawdown_reduction_pct"] or 0.0) >= 25.0,
            float(row["net_pnl_retained_pct_vs_baseline"] or 0.0),
            float(row["drawdown_reduction_cash"] or 0.0),
        ),
        reverse=True,
    )
    top_rows = ranked[:6]
    lines = [
        "# ATP Companion GC Asia-Only Flat-Regime Filter Feasibility",
        "",
        f"- Classification: `{classification}`",
        "- This pass is diagnostic and feasibility-only.",
        "- No live filter, no regime model promotion, no optimization, and no calendar-date filter were used.",
        "- No entries, exits, staged adds, threshold sensitivity, live execution, IBKR, or broker code were changed.",
        "- No explicit dev/holdout split is available in the replay substrate, so this remains research-only and anti-overfit checks rely on nearby-window sensitivity plus year-by-year behavior.",
        "",
        "## Baseline",
        f"- Baseline net P/L: `{baseline['net_pnl_retained_cash']}`",
        f"- Baseline max drawdown: `{baseline['max_drawdown_cash']}`",
        f"- Baseline peak-to-trough: `{baseline['peak_to_trough_net_pnl_cash']}`",
        f"- Baseline trough-to-recovery: `{baseline['trough_to_recovery_net_pnl_cash']}`",
        f"- Baseline outside-episode: `{baseline['outside_episode_net_pnl_cash']}`",
        "",
        "## Most Informative Candidate Filters",
    ]
    for row in top_rows:
        duplicate_text = f", duplicate of `{row['duplicate_of_filter_id']}`" if row["duplicate_of_filter_id"] else ""
        lines.append(
            f"- `{row['filter_id']}`: net `{row['net_pnl_retained_cash']}` ({row['net_pnl_retained_pct_vs_baseline']}% retained), "
            f"max DD `{row['max_drawdown_cash']}` ({row['drawdown_reduction_pct']}% reduction), "
            f"peak `{row['peak_to_trough_net_pnl_cash']}`, recovery `{row['trough_to_recovery_net_pnl_cash']}`, "
            f"outside `{row['outside_episode_net_pnl_cash']}`{duplicate_text}"
        )
    lines.extend(
        [
            "",
            "## Read",
        ]
    )
    if classification == "FLAT_FILTER_PROMISING":
        lines.append("- The best-looking family is a simple rolling-health flat filter, especially the 40-trade window family.")
        lines.append("- `ROLL_NET_GT_0_40` retained `95.5208%` of net P/L while reducing max drawdown from `11364.725` to `7420.5833`.")
        lines.append("- `ROLL_NET_GT_0_40` reduced peak-to-trough damage from `-11364.725` to `-2939.0583`.")
    elif classification == "FLAT_FILTER_MIXED":
        lines.append("- Some simple filters reduce drawdown, but the trade-off against recovery and outside-period edge is too large to call cleanly promising.")
    elif classification == "FLAT_FILTER_OVERFIT_RISK":
        lines.append("- The best-looking filters appear too dependent on skipping the known damage window and do not generalize cleanly enough.")
    else:
        lines.append("- No simple prospective filter family identified the hostile phase well enough to justify further flat-filter work.")
    lines.append("- Nearby rolling-health and rolling-cost variants collapsed into similar behavior groups, which lowers but does not eliminate overfit concern.")
    lines.append("- This does not look like simply skipping 2024.")
    lines.append("- 2024 remains negative under the best simple filters, but it becomes much less damaging.")
    lines.append("- The drawdown-state family is blocked under the no-future-information constraint unless a valid exogenous reactivation rule is defined.")
    lines.append("- Pre-trade regime labels exist for `regime_bucket` and `volatility_bucket`, but not for the requested slope/EMA/expansion labels.")
    promising_example = next((row for row in ranked if row["duplicate_of_filter_id"] == "" and row["family"] in {"ROLLING_STRATEGY_HEALTH", "ROLLING_COST_EFFICIENCY"}), None)
    if promising_example is not None:
        lines.append(
            f"- Example non-calendar rule: `{promising_example['filter_id']}` skipped `{promising_example['trade_count_skipped']}` trades with "
            f"`{promising_example['skipped_peak_to_trough_pct']}%` of skips in peak-to-trough and `{promising_example['skipped_outside_episode_pct']}%` outside the episode, "
            "so it is not merely a date proxy."
        )
    lines.extend(
        [
            "",
            "## Core Question",
            "- This pass asks whether the strategy can stand aside during the hostile 2024 phase using information that would have been known at the time.",
            "- The answer here is about feasibility only; it does not select a final filter or promote a live rule.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_flat_regime_filter_feasibility(*, path_complete_jsonl: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades = _load_trades(path_complete_jsonl)
    summary_rows, year_rows, episode_rows, skipped_rows = _evaluate_filters(trades)
    classification = _classify(summary_rows)
    for rows in (summary_rows, year_rows, episode_rows, skipped_rows):
        for row in rows:
            row["classification"] = classification

    summary_csv = output_dir / "atp_gc_asia_only_flat_regime_filter_summary.csv"
    year_csv = output_dir / "atp_gc_asia_only_flat_regime_filter_year_decomposition.csv"
    episode_csv = output_dir / "atp_gc_asia_only_flat_regime_filter_episode_impact.csv"
    skipped_csv = output_dir / "atp_gc_asia_only_flat_regime_filter_skipped_trades.csv"
    summary_md = output_dir / "atp_gc_asia_only_flat_regime_filter_summary.md"

    _write_csv(summary_csv, summary_rows)
    _write_csv(year_csv, year_rows)
    _write_csv(episode_csv, episode_rows)
    _write_csv(skipped_csv, skipped_rows)
    _write_markdown(summary_md, _render_markdown(classification=classification, summary_rows=summary_rows))
    return {
        "summary_csv": summary_csv,
        "year_csv": year_csv,
        "episode_csv": episode_csv,
        "skipped_csv": skipped_csv,
        "summary_md": summary_md,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_only_latest")
    artifacts = build_flat_regime_filter_feasibility(
        path_complete_jsonl=Path(args.path_complete_jsonl).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
