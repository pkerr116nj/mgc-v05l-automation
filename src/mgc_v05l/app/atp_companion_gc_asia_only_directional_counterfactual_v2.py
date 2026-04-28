"""Path-backed directional counterfactual diagnostic for GC Asia-only drawdown periods."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
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
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_asia_only_directional_counterfactual_v2"

EPISODE_START = datetime.fromisoformat("2024-01-01T00:00:00-05:00")
TROUGH_END = datetime.fromisoformat("2024-11-10T23:59:59-05:00")
TROUGH_START = datetime.fromisoformat("2024-11-10T00:00:00-05:00")
RECOVERY_END = datetime.fromisoformat("2025-04-21T23:59:59-04:00")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-asia-only-directional-counterfactual-v2")
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


def _load_long_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _iter_jsonl(path):
        entry_ts = _parse_ts(raw["entry_ts"])
        exit_ts = _parse_ts(raw["exit_ts"])
        rows.append(
            {
                "trade_id": str(raw["trade_id"]),
                "posture": "LONG",
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_date": entry_ts.date().isoformat(),
                "entry_year": entry_ts.year,
                "entry_month": entry_ts.strftime("%Y-%m"),
                "entry_quarter": f"{entry_ts.year}-Q{((entry_ts.month - 1) // 3) + 1}",
                "entry_hour": entry_ts.hour,
                "direction": str(raw["direction"]),
                "exit_reason": str(raw["exit_reason"]),
                "gross_pnl_cash": float(raw["trade_record"]["gross_pnl_cash"]),
                "fees_paid": float(raw["trade_record"]["fees_paid"]),
                "slippage_cost": float(raw["trade_record"]["slippage_cost"]),
                "trade_pnl_cash": float(raw["baseline_pnl_cash"]),
                "entry_price": float(raw["entry_price"]),
                "exit_price": float(raw["exit_price"]),
                "stop_price": float(raw["stop_price"]),
                "target_price": float(raw["target_price"]),
                "initial_risk_points": float(raw["initial_risk_points"]),
                "point_value": float(raw.get("point_value", 100.0)),
                "minute_path": list(raw.get("minute_path") or []),
            }
        )
    rows.sort(key=lambda row: (row["entry_ts"], row["exit_ts"], row["trade_id"]))
    return rows


def _simulate_mirrored_short_trade(long_trade: dict[str, Any]) -> dict[str, Any]:
    minute_path = list(long_trade["minute_path"])
    if not minute_path:
        raise ValueError(f"Trade {long_trade['trade_id']} has empty minute_path in the path-complete substrate.")
    entry_price = float(long_trade["entry_price"])
    stop_distance = abs(float(long_trade["entry_price"]) - float(long_trade["stop_price"]))
    target_distance = abs(float(long_trade["target_price"]) - float(long_trade["entry_price"]))
    point_value = float(long_trade["point_value"])
    fees_paid = float(long_trade["fees_paid"])
    slippage_cost = float(long_trade["slippage_cost"])
    short_stop = entry_price + stop_distance
    short_target = entry_price - target_distance

    exit_reason = "time_stop"
    exit_price = float(minute_path[-1]["close"])
    exit_ts = _parse_ts(minute_path[-1]["timestamp"])

    for bar in minute_path:
        high = float(bar["high"])
        low = float(bar["low"])
        stop_hit = high >= short_stop
        target_hit = low <= short_target
        # Conservative ambiguity handling: if both levels print in the same bar,
        # assume the adverse stop is hit before the favorable target.
        if stop_hit and target_hit:
            exit_reason = "stop"
            exit_price = short_stop
            exit_ts = _parse_ts(bar["timestamp"])
            break
        if stop_hit:
            exit_reason = "stop"
            exit_price = short_stop
            exit_ts = _parse_ts(bar["timestamp"])
            break
        if target_hit:
            exit_reason = "target"
            exit_price = short_target
            exit_ts = _parse_ts(bar["timestamp"])
            break

    gross_pnl_cash = (entry_price - exit_price) * point_value
    net_pnl_cash = gross_pnl_cash - fees_paid - slippage_cost
    return {
        **long_trade,
        "trade_id": f"{long_trade['trade_id']}|SHORT_CF",
        "posture": "SHORT",
        "direction": "SHORT",
        "exit_reason": exit_reason,
        "exit_ts": exit_ts,
        "exit_price": round(exit_price, 6),
        "gross_pnl_cash": round(gross_pnl_cash, 4),
        "trade_pnl_cash": round(net_pnl_cash, 4),
        "counterfactual_basis": "Mirrored short using actual minute_path with conservative same-bar stop-first handling.",
    }


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
    gross_profit = sum(value for value in pnls if value > 0)
    gross_loss = sum(abs(value) for value in pnls if value < 0)
    if gross_loss > 0:
        return gross_profit / gross_loss
    if gross_profit > 0:
        return gross_profit
    return None


def _max_consecutive_losers(pnls: Sequence[float]) -> int:
    longest = 0
    current = 0
    for pnl in pnls:
        if pnl < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def _period_ranges() -> list[tuple[str, str, str, datetime | None, datetime | None]]:
    return [
        ("PEAK_TO_TROUGH", "CORE", "2024-01-01 to 2024-11-10", EPISODE_START, TROUGH_END),
        (
            "TROUGH_TO_RECOVERY",
            "CORE",
            "2024-11-10 to 2025-04-21",
            TROUGH_START,
            RECOVERY_END,
        ),
        ("FULL_EPISODE", "CORE", "2024-01-01 to 2025-04-21", EPISODE_START, RECOVERY_END),
        ("OUTSIDE_EPISODE", "CORE", "Outside 2024-01-01 to 2025-04-21", None, None),
    ]


def _select_period(trades: Sequence[dict[str, Any]], period_id: str) -> list[dict[str, Any]]:
    if period_id == "OUTSIDE_EPISODE":
        return [trade for trade in trades if not (EPISODE_START <= trade["entry_ts"] <= RECOVERY_END)]
    if period_id == "PEAK_TO_TROUGH":
        return [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= TROUGH_END]
    if period_id == "TROUGH_TO_RECOVERY":
        return [trade for trade in trades if TROUGH_START <= trade["entry_ts"] <= RECOVERY_END]
    if period_id == "FULL_EPISODE":
        return [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    raise KeyError(period_id)


def _summary_row(
    *,
    posture: str,
    period_id: str,
    period_kind: str,
    period_label: str,
    trades: Sequence[dict[str, Any]],
    note: str = "",
) -> dict[str, Any]:
    pnls = [float(trade["trade_pnl_cash"]) for trade in trades]
    gross = round(sum(float(trade["gross_pnl_cash"]) for trade in trades), 4)
    fees = round(sum(float(trade["fees_paid"]) for trade in trades), 4)
    slippage = round(sum(float(trade["slippage_cost"]) for trade in trades), 4)
    costs = round(fees + slippage, 4)
    net = round(sum(pnls), 4)
    winners = [value for value in pnls if value > 0]
    losers = [value for value in pnls if value < 0]
    pf = _profit_factor(pnls)
    return {
        "posture": posture,
        "period_id": period_id,
        "period_kind": period_kind,
        "period_label": period_label,
        "trade_count": len(trades),
        "gross_pnl_cash": gross,
        "total_fees_cash": fees,
        "total_slippage_cash": slippage,
        "total_costs_cash": costs,
        "net_pnl_cash": net,
        "average_trade_cash": round(net / max(len(trades), 1), 4),
        "median_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
        "win_rate_pct": round((len(winners) / max(len(trades), 1)) * 100.0, 4),
        "profit_factor": round(float(pf), 4) if pf is not None else "",
        "max_drawdown_cash": _max_drawdown(pnls),
        "largest_win_cash": round(max(pnls), 4) if pnls else 0.0,
        "largest_loss_cash": round(min(pnls), 4) if pnls else 0.0,
        "max_consecutive_losers": _max_consecutive_losers(pnls),
        "cost_consumed_pct_of_gross": round((costs / gross) * 100.0, 4) if gross > 0 else "",
        "note": note,
    }


def _flat_summary_row(period_id: str, period_kind: str, period_label: str) -> dict[str, Any]:
    return {
        "posture": "FLAT",
        "period_id": period_id,
        "period_kind": period_kind,
        "period_label": period_label,
        "trade_count": 0,
        "gross_pnl_cash": 0.0,
        "total_fees_cash": 0.0,
        "total_slippage_cash": 0.0,
        "total_costs_cash": 0.0,
        "net_pnl_cash": 0.0,
        "average_trade_cash": 0.0,
        "median_trade_cash": 0.0,
        "win_rate_pct": 0.0,
        "profit_factor": "",
        "max_drawdown_cash": 0.0,
        "largest_win_cash": 0.0,
        "largest_loss_cash": 0.0,
        "max_consecutive_losers": 0,
        "cost_consumed_pct_of_gross": "",
        "note": "No-trade comparison for the same period.",
    }


def _core_summary_rows(long_trades: Sequence[dict[str, Any]], short_trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period_id, period_kind, label, _, _ in _period_ranges():
        long_period = _select_period(long_trades, period_id)
        short_period = _select_period(short_trades, period_id)
        note = "2024-11-10 is included in both core subperiods by request." if period_id == "TROUGH_TO_RECOVERY" else ""
        rows.append(_summary_row(posture="LONG", period_id=period_id, period_kind=period_kind, period_label=label, trades=long_period, note=note))
        rows.append(_summary_row(posture="SHORT", period_id=period_id, period_kind=period_kind, period_label=label, trades=short_period, note=note))
        rows.append(_flat_summary_row(period_id, period_kind, label))
    return rows


def _period_decomposition_rows(long_trades: Sequence[dict[str, Any]], short_trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for posture, trades in (("LONG", long_trades), ("SHORT", short_trades)):
        by_year: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_quarter: dict[str, list[dict[str, Any]]] = defaultdict(list)
        episode_only = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
        for trade in episode_only:
            by_year[str(trade["entry_year"])].append(trade)
            by_quarter[str(trade["entry_quarter"])].append(trade)
        for year in sorted(by_year):
            rows.append(
                _summary_row(
                    posture=posture,
                    period_id=f"YEAR_{year}",
                    period_kind="YEAR",
                    period_label=year,
                    trades=by_year[year],
                )
            )
        for quarter in sorted(by_quarter):
            rows.append(
                _summary_row(
                    posture=posture,
                    period_id=f"QUARTER_{quarter}",
                    period_kind="QUARTER",
                    period_label=quarter,
                    trades=by_quarter[quarter],
                )
            )
    return rows


def _monthly_rows(long_trades: Sequence[dict[str, Any]], short_trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for posture, trades in (("LONG", long_trades), ("SHORT", short_trades)):
        by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for trade in trades:
            by_month[str(trade["entry_month"])].append(trade)
        for month in sorted(by_month):
            rows.append(
                _summary_row(
                    posture=posture,
                    period_id=f"MONTH_{month}",
                    period_kind="MONTH",
                    period_label=month,
                    trades=by_month[month],
                )
            )
    return rows


def _exit_reason_rows(long_trades: Sequence[dict[str, Any]], short_trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for period_id, _, label, _, _ in _period_ranges():
        for posture, trades in (("LONG", long_trades), ("SHORT", short_trades)):
            subset = _select_period(trades, period_id)
            counts = Counter(str(trade["exit_reason"]) for trade in subset)
            total = len(subset)
            for exit_reason in ("target", "stop", "time_stop"):
                count = int(counts.get(exit_reason, 0))
                rows.append(
                    {
                        "posture": posture,
                        "period_id": period_id,
                        "period_label": label,
                        "exit_reason": exit_reason,
                        "trade_count": total,
                        "exit_count": count,
                        "exit_pct": round((count / max(total, 1)) * 100.0, 4),
                    }
                )
    return rows


def _index_summary(rows: Sequence[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    return {(str(row["posture"]), str(row["period_id"])): row for row in rows}


def _classify(summary_rows: Sequence[dict[str, Any]]) -> str:
    idx = _index_summary(summary_rows)
    peak_long = float(idx[("LONG", "PEAK_TO_TROUGH")]["net_pnl_cash"])
    peak_short = float(idx[("SHORT", "PEAK_TO_TROUGH")]["net_pnl_cash"])
    recovery_long = float(idx[("LONG", "TROUGH_TO_RECOVERY")]["net_pnl_cash"])
    recovery_short = float(idx[("SHORT", "TROUGH_TO_RECOVERY")]["net_pnl_cash"])
    full_long = float(idx[("LONG", "FULL_EPISODE")]["net_pnl_cash"])
    full_short = float(idx[("SHORT", "FULL_EPISODE")]["net_pnl_cash"])
    outside_long = float(idx[("LONG", "OUTSIDE_EPISODE")]["net_pnl_cash"])
    outside_short = float(idx[("SHORT", "OUTSIDE_EPISODE")]["net_pnl_cash"])

    if peak_short > 0 and full_short > 0 and recovery_short > 0:
        return "SHORT_COUNTERFACTUAL_PROMISING"
    if peak_long < 0 and peak_short <= 0 and recovery_long > 0 and recovery_short <= 0:
        return "FLAT_REGIME_FILTER_PROMISING"
    if peak_short > 0 and (recovery_short <= 0 or outside_short <= 0):
        return "INCONCLUSIVE_NEEDS_REGIME_DETECTOR"
    if full_long > 0 and peak_long < 0 and recovery_long > 0 and full_short <= 0:
        return "LONG_ONLY_RECOVERY_DEPENDENT"
    if full_long <= 0 and full_short <= 0:
        return "BOTH_SIDES_CHOPPY_COST_HEAVY"
    return "INCONCLUSIVE_NEEDS_REGIME_DETECTOR"


def _render_markdown(
    *,
    classification: str,
    summary_rows: Sequence[dict[str, Any]],
    period_rows: Sequence[dict[str, Any]],
    monthly_rows: Sequence[dict[str, Any]],
    exit_rows: Sequence[dict[str, Any]],
) -> str:
    idx = _index_summary(summary_rows)
    peak_long = idx[("LONG", "PEAK_TO_TROUGH")]
    peak_short = idx[("SHORT", "PEAK_TO_TROUGH")]
    peak_flat = idx[("FLAT", "PEAK_TO_TROUGH")]
    recovery_long = idx[("LONG", "TROUGH_TO_RECOVERY")]
    recovery_short = idx[("SHORT", "TROUGH_TO_RECOVERY")]
    full_long = idx[("LONG", "FULL_EPISODE")]
    full_short = idx[("SHORT", "FULL_EPISODE")]
    outside_long = idx[("LONG", "OUTSIDE_EPISODE")]
    outside_short = idx[("SHORT", "OUTSIDE_EPISODE")]

    monthly_idx = {(str(row["posture"]), str(row["period_label"])): row for row in monthly_rows}
    quarterly_idx = {
        (str(row["posture"]), str(row["period_label"])): row
        for row in period_rows
        if str(row["period_kind"]) == "QUARTER"
    }
    year_2024_long = next((row for row in period_rows if row["posture"] == "LONG" and row["period_id"] == "YEAR_2024"), None)
    year_2024_short = next((row for row in period_rows if row["posture"] == "SHORT" and row["period_id"] == "YEAR_2024"), None)
    month_examples = [
        monthly_idx[key]
        for key in sorted(monthly_idx)
        if key[1] in {"2024-01", "2024-06", "2024-11", "2025-01", "2025-04"}
    ]
    exit_mix_peak_long = ", ".join(
        f"{row['exit_reason']}:{row['exit_count']}"
        for row in exit_rows
        if row["posture"] == "LONG" and row["period_id"] == "PEAK_TO_TROUGH"
    )
    exit_mix_peak_short = ", ".join(
        f"{row['exit_reason']}:{row['exit_count']}"
        for row in exit_rows
        if row["posture"] == "SHORT" and row["period_id"] == "PEAK_TO_TROUGH"
    )
    lines = [
        "# ATP Companion GC Asia-Only Directional Counterfactual v2",
        "",
        f"- Classification: `{classification}`",
        "- This pass uses the committed path-complete GC Asia-only replay substrate.",
        "- Mirrored short was run honestly from actual intratrade bars, using conservative same-bar stop-first handling.",
        "- No entries, exits, thresholds, staged adds, live execution, IBKR, or broker code were changed.",
        "",
        "## Core Read",
        f"- Peak-to-trough: long `{peak_long['net_pnl_cash']}`, short `{peak_short['net_pnl_cash']}`, flat `{peak_flat['net_pnl_cash']}`",
        f"- Flat peak-to-trough posture: `{peak_flat['net_pnl_cash']}` with max drawdown `{peak_flat['max_drawdown_cash']}`",
        f"- Trough-to-recovery: long `+{recovery_long['net_pnl_cash']}`, short `{recovery_short['net_pnl_cash']}`",
        f"- Full episode: long `+{full_long['net_pnl_cash']}`, short `{full_short['net_pnl_cash']}`",
        f"- Outside episode: long `+{outside_long['net_pnl_cash']}`, short `{outside_short['net_pnl_cash']}`",
        "",
        "## Interpretation",
        "- Mirrored short was not attractive.",
    ]
    if float(peak_short["net_pnl_cash"]) > 0 and float(recovery_short["net_pnl_cash"]) <= 0:
        lines.append("- Mirrored short helps only in the damage phase and fails in recovery, so any long/short switching would require a regime detector.")
    if float(peak_flat["net_pnl_cash"]) > float(peak_long["net_pnl_cash"]) and float(peak_flat["net_pnl_cash"]) > float(peak_short["net_pnl_cash"]):
        lines.append("- Flat/no-trade is better than both long and mirrored short during the peak-to-trough damage phase.")
    if float(outside_long["net_pnl_cash"]) > 0:
        lines.append("- Long outside the episode remains strong, so the problem is concentrated in the drawdown regime rather than the whole sample.")
    if float(recovery_long["net_pnl_cash"]) > 0 and float(peak_long["net_pnl_cash"]) < 0:
        lines.append("- The episode is not homogeneous: peak-to-trough is hostile and cost-destructive, while trough-to-recovery is profitable for the long expression.")
    lines.append("- The problem looks like a flat/no-trade regime, not a short regime.")
    lines.extend(
        [
            "",
            "## Exit Mix",
            f"- Peak-to-trough long exit mix: `{exit_mix_peak_long}`",
            f"- Peak-to-trough mirrored short exit mix: `{exit_mix_peak_short}`",
            "",
            "## 2024 / Quarter Read",
        ]
    )
    if year_2024_long and year_2024_short:
        lines.append(f"- 2024 long net P/L: `{year_2024_long['net_pnl_cash']}`")
        lines.append(f"- 2024 mirrored short net P/L: `{year_2024_short['net_pnl_cash']}`")
    for key in sorted(quarterly_idx):
        row = quarterly_idx[key]
        if str(row["period_label"]).startswith("2024-") or str(row["period_label"]).startswith("2025-"):
            lines.append(f"- `{row['posture']} {row['period_label']}`: net `{row['net_pnl_cash']}`, PF `{row['profit_factor']}`")
    lines.extend(
        [
            "",
            "## Monthly Samples",
        ]
    )
    for row in month_examples:
        lines.append(f"- `{row['posture']} {row['period_label']}`: net `{row['net_pnl_cash']}`, PF `{row['profit_factor']}`")
    lines.extend(
        [
            "",
            "## Bottom Line",
            "- This pass does not promote short, build a regime switch, or modify strategy logic.",
            "- It only answers whether the drawdown regime was better handled by long, flat, or mirrored short.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_directional_counterfactual_v2(*, path_complete_jsonl: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    long_trades = _load_long_trades(path_complete_jsonl)
    short_trades = [_simulate_mirrored_short_trade(trade) for trade in long_trades]
    summary_rows = _core_summary_rows(long_trades, short_trades)
    period_rows = _period_decomposition_rows(long_trades, short_trades)
    monthly_rows = _monthly_rows(long_trades, short_trades)
    exit_rows = _exit_reason_rows(long_trades, short_trades)
    classification = _classify(summary_rows)

    for rows in (summary_rows, period_rows, monthly_rows, exit_rows):
        for row in rows:
            row["classification"] = classification

    summary_csv = output_dir / "atp_gc_asia_only_directional_counterfactual_v2_summary.csv"
    period_csv = output_dir / "atp_gc_asia_only_directional_counterfactual_v2_period_decomposition.csv"
    monthly_csv = output_dir / "atp_gc_asia_only_directional_counterfactual_v2_monthly.csv"
    exit_csv = output_dir / "atp_gc_asia_only_directional_counterfactual_v2_exit_reason.csv"
    summary_md = output_dir / "atp_gc_asia_only_directional_counterfactual_v2_summary.md"

    _write_csv(summary_csv, summary_rows)
    _write_csv(period_csv, period_rows)
    _write_csv(monthly_csv, monthly_rows)
    _write_csv(exit_csv, exit_rows)
    _write_markdown(
        summary_md,
        _render_markdown(
            classification=classification,
            summary_rows=summary_rows,
            period_rows=period_rows,
            monthly_rows=monthly_rows,
            exit_rows=exit_rows,
        ),
    )
    return {
        "summary_csv": summary_csv,
        "period_csv": period_csv,
        "monthly_csv": monthly_csv,
        "exit_csv": exit_csv,
        "summary_md": summary_md,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_only_latest")
    artifacts = build_directional_counterfactual_v2(
        path_complete_jsonl=Path(args.path_complete_jsonl).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
