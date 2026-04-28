"""Asia-only feasibility review for the replay-safe GC ATP expression."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

from .atp_companion_gc_drawdown_governance_feasibility import (
    CAP_GRID,
    RESET_POLICIES,
    GovernancePolicy,
    _annotate_summary_rows,
    _baseline_metrics,
    _simulate_policy,
)

REPO_ROOT = Path.cwd()
DEFAULT_GC_TRADES_JSONL = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2_gc_expression"
    / "gc_asia_us_latest"
    / "atp_companion_replay_baseline_v2_gc_trades.jsonl"
)
DEFAULT_MGC_COST_SUMMARY_CSV = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2_cost_sensitivity"
    / "mgc_asia_us_latest"
    / "atp_v2_cost_model_sensitivity_summary.csv"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_asia_only_feasibility"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-asia-only-feasibility")
    parser.add_argument("--gc-trades-jsonl", default=str(DEFAULT_GC_TRADES_JSONL), help="Replay-safe GC trade JSONL.")
    parser.add_argument(
        "--mgc-cost-summary-csv",
        default=str(DEFAULT_MGC_COST_SUMMARY_CSV),
        help="Current replay-safe MGC cost-sensitivity summary CSV for context only.",
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


def _load_gc_trade_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _iter_jsonl(path):
        trade = raw["trade_record"]
        entry_ts = datetime.fromisoformat(str(trade["entry_ts"]).replace(" ", "T"))
        exit_ts = datetime.fromisoformat(str(trade["exit_ts"]).replace(" ", "T"))
        rows.append(
            {
                "trade_id": str(raw["trade_id"]),
                "instrument": str(raw["instrument"]),
                "session": str(raw["session"]),
                "direction": str(raw["direction"]),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_date": entry_ts.date().isoformat(),
                "entry_year": entry_ts.year,
                "entry_month": entry_ts.month,
                "entry_iso_week": entry_ts.isocalendar().week,
                "exit_reason": str(raw["exit_reason"]),
                "gross_pnl_cash": float(trade["gross_pnl_cash"]),
                "fees_paid": float(trade["fees_paid"]),
                "slippage_cost": float(trade["slippage_cost"]),
                "trade_pnl_cash": float(raw["baseline_pnl_cash"]),
            }
        )
    rows.sort(key=lambda row: (row["entry_ts"], row["exit_ts"], row["trade_id"]))
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


def _max_losing_streak(pnls: Sequence[float]) -> int:
    streak = 0
    best = 0
    for pnl in pnls:
        if pnl < 0:
            streak += 1
            best = max(best, streak)
        else:
            streak = 0
    return best


def _drawdown_to_profit_ratio(net_pnl_cash: float, max_drawdown: float) -> float | None:
    if net_pnl_cash <= 0:
        return None
    return max_drawdown / net_pnl_cash


def _summary_for_scope(*, scope_id: str, trades: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(trade["trade_pnl_cash"]) for trade in trades]
    gross_total = round(sum(float(trade["gross_pnl_cash"]) for trade in trades), 4)
    fee_total = round(sum(float(trade["fees_paid"]) for trade in trades), 4)
    slippage_total = round(sum(float(trade["slippage_cost"]) for trade in trades), 4)
    total_costs = round(fee_total + slippage_total, 4)
    net_total = round(sum(pnls), 4)
    pf = _profit_factor(pnls)
    max_dd = _max_drawdown(pnls)
    dd_ratio = _drawdown_to_profit_ratio(net_total, max_dd)
    net_per_dd = (net_total / max_dd) if max_dd > 0 else None
    cost_consumed = ((total_costs / gross_total) * 100.0) if gross_total > 0 else 0.0
    asia_contribution = round(sum(float(trade["trade_pnl_cash"]) for trade in trades if trade["session"] == "ASIA"), 4)
    us_contribution = round(sum(float(trade["trade_pnl_cash"]) for trade in trades if trade["session"] == "US"), 4)
    return {
        "scope_id": scope_id,
        "trade_count": len(trades),
        "gross_pnl_cash": gross_total,
        "total_costs_cash": total_costs,
        "net_pnl_cash": net_total,
        "average_trade_cash": round(net_total / max(len(trades), 1), 4),
        "median_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
        "win_rate": round((sum(1 for pnl in pnls if pnl > 0) / max(len(pnls), 1) * 100.0), 4) if pnls else 0.0,
        "profit_factor": round(float(pf), 4) if pf is not None else "",
        "max_drawdown": max_dd,
        "largest_win": round(max(pnls), 4) if pnls else 0.0,
        "largest_loss": round(min(pnls), 4) if pnls else 0.0,
        "max_consecutive_losers": _max_losing_streak(pnls),
        "drawdown_to_profit_ratio": round(float(dd_ratio), 4) if dd_ratio is not None else "",
        "net_pnl_per_unit_of_drawdown": round(float(net_per_dd), 4) if net_per_dd is not None else "",
        "cost_consumed_pct_of_gross_edge": round(cost_consumed, 4),
        "asia_contribution_cash": asia_contribution,
        "us_contribution_cash": us_contribution,
    }


def _year_rows_for_scope(*, scope_id: str, trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    years = sorted({int(trade["entry_year"]) for trade in trades})
    rows: list[dict[str, Any]] = []
    for year in years:
        year_trades = [trade for trade in trades if int(trade["entry_year"]) == year]
        pnls = [float(trade["trade_pnl_cash"]) for trade in year_trades]
        gross_total = round(sum(float(trade["gross_pnl_cash"]) for trade in year_trades), 4)
        costs = round(sum(float(trade["fees_paid"]) + float(trade["slippage_cost"]) for trade in year_trades), 4)
        net_total = round(sum(pnls), 4)
        rows.append(
            {
                "scope_id": scope_id,
                "year": year,
                "trade_count": len(year_trades),
                "gross_pnl_cash": gross_total,
                "total_costs_cash": costs,
                "net_pnl_cash": net_total,
                "average_trade_cash": round(net_total / max(len(year_trades), 1), 4),
                "win_rate": round((sum(1 for pnl in pnls if pnl > 0) / max(len(pnls), 1) * 100.0), 4) if pnls else 0.0,
                "profit_factor": round(float(_profit_factor(pnls)), 4) if _profit_factor(pnls) is not None else "",
                "max_drawdown": _max_drawdown(pnls),
                "asia_contribution_cash": round(sum(float(trade["trade_pnl_cash"]) for trade in year_trades if trade["session"] == "ASIA"), 4),
                "us_contribution_cash": round(sum(float(trade["trade_pnl_cash"]) for trade in year_trades if trade["session"] == "US"), 4),
            }
        )
    return rows


def _load_mgc_context_row(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    for row in rows:
        if row["scenario_id"] == "CURRENT_V2_COST_MODEL":
            net_per_dd_key = "net_pnl_per_unit_of_drawdown" if "net_pnl_per_unit_of_drawdown" in row else "net_pnl_per_trade"
            cost_pct_key = (
                "cost_consumed_pct_of_gross_edge"
                if "cost_consumed_pct_of_gross_edge" in row
                else "percent_gross_edge_consumed_by_costs"
            )
            return {
                "scope_id": "MGC_CONTEXT_CURRENT_V2",
                "trade_count": int(row["trade_count"]),
                "gross_pnl_cash": float(row["total_gross_pnl_cash"]),
                "total_costs_cash": float(row["total_fees_cash"]) + float(row["total_slippage_cash"]),
                "net_pnl_cash": float(row["total_net_pnl_cash"]),
                "average_trade_cash": float(row["average_net_trade_cash"]),
                "median_trade_cash": float(row["median_net_trade_cash"]),
                "win_rate": float(row["win_rate"]),
                "profit_factor": float(row["profit_factor"]) if row["profit_factor"] else "",
                "max_drawdown": float(row["max_drawdown"]),
                "largest_win": float(row["largest_win"]),
                "largest_loss": float(row["largest_loss"]),
                "max_consecutive_losers": int(row["max_consecutive_losers"]),
                "drawdown_to_profit_ratio": float(row["drawdown_to_profit_ratio"]) if row["drawdown_to_profit_ratio"] else "",
                "net_pnl_per_unit_of_drawdown": float(row[net_per_dd_key]) if row[net_per_dd_key] else "",
                "cost_consumed_pct_of_gross_edge": float(row[cost_pct_key]),
                "asia_contribution_cash": float(row["asia_contribution_cash"]),
                "us_contribution_cash": float(row["us_contribution_cash"]),
            }
    raise ValueError("Could not locate CURRENT_V2_COST_MODEL row in MGC context summary.")


def _comparison_rows(*, asia_only: dict[str, Any], asia_us: dict[str, Any], mgc_context: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in (asia_us, asia_only, mgc_context):
        net_delta_vs_asia_us = float(source["net_pnl_cash"]) - float(asia_us["net_pnl_cash"])
        dd_delta_vs_asia_us = float(source["max_drawdown"]) - float(asia_us["max_drawdown"])
        trade_delta_vs_asia_us = int(source["trade_count"]) - int(asia_us["trade_count"])
        retained_pct_vs_asia_us = (float(source["net_pnl_cash"]) / float(asia_us["net_pnl_cash"]) * 100.0) if float(asia_us["net_pnl_cash"]) else 0.0
        rows.append(
            {
                **source,
                "net_pnl_delta_vs_gc_asia_us": round(net_delta_vs_asia_us, 4),
                "drawdown_delta_vs_gc_asia_us": round(dd_delta_vs_asia_us, 4),
                "trade_count_delta_vs_gc_asia_us": trade_delta_vs_asia_us,
                "net_pnl_pct_vs_gc_asia_us": round(retained_pct_vs_asia_us, 4),
            }
        )
    return rows


def _classify(*, asia_only_summary: dict[str, Any], asia_us_summary: dict[str, Any], governance_rows: Sequence[dict[str, Any]]) -> str:
    net_pnl = float(asia_only_summary["net_pnl_cash"])
    max_dd = float(asia_only_summary["max_drawdown"])
    dd_ratio = float(asia_only_summary["drawdown_to_profit_ratio"]) if asia_only_summary["drawdown_to_profit_ratio"] != "" else None
    full_net = float(asia_us_summary["net_pnl_cash"])
    full_dd = float(asia_us_summary["max_drawdown"])
    retained_pct_vs_full = (net_pnl / full_net * 100.0) if full_net else 0.0
    drawdown_reduction_vs_full = full_dd - max_dd
    fivek_rows = [row for row in governance_rows if float(row["cap_cash"]) == 5000.0]
    tolerable_fivek = [
        row
        for row in fivek_rows
        if float(row["retained_net_pnl_cash"]) > 0.0
        and float(row["retained_net_pnl_vs_ungated_gc_pct"]) >= 45.0
        and float(row["max_drawdown_after_governance"]) <= 10000.0
    ]
    broad_help = [
        row
        for row in governance_rows
        if float(row["cap_cash"]) <= 10000.0
        and float(row["retained_net_pnl_cash"]) > 0.0
        and float(row["retained_net_pnl_vs_ungated_gc_pct"]) >= 60.0
        and float(row["drawdown_reduction_cash"]) >= 5000.0
    ]
    if net_pnl <= 0.0 or max_dd <= 0.0:
        return "GC_ASIA_ONLY_REJECTED"
    if tolerable_fivek:
        return "GC_ASIA_ONLY_PROMISING"
    if retained_pct_vs_full >= 75.0 and drawdown_reduction_vs_full >= 10000.0 and dd_ratio is not None and dd_ratio <= 0.25:
        return "GC_ASIA_ONLY_IMPROVES_RISK_BUT_STILL_TOO_LARGE"
    if dd_ratio is not None and dd_ratio <= 0.2 and broad_help:
        return "GC_ASIA_ONLY_IMPROVES_RISK_BUT_STILL_TOO_LARGE"
    if broad_help:
        return "GC_ASIA_ONLY_IMPROVES_RISK_BUT_STILL_TOO_LARGE"
    if net_pnl > 0.0:
        return "GC_ASIA_ONLY_REDUCES_EDGE_TOO_MUCH"
    return "GC_ASIA_ONLY_REJECTED"


def _render_markdown(
    *,
    classification: str,
    asia_only: dict[str, Any],
    asia_us: dict[str, Any],
    mgc_context: dict[str, Any],
    governance_rows: Sequence[dict[str, Any]],
    year_rows: Sequence[dict[str, Any]],
) -> str:
    year_lookup = {(row["scope_id"], int(row["year"])): row for row in year_rows}
    asia_only_2024 = year_lookup.get(("GC_ASIA_ONLY", 2024))
    asia_us_2024 = year_lookup.get(("GC_ASIA_PLUS_US", 2024))
    fivek_rows = [row for row in governance_rows if float(row["cap_cash"]) == 5000.0]
    strongest_low = sorted(
        [row for row in governance_rows if float(row["cap_cash"]) <= 10000.0],
        key=lambda row: (float(row["retained_net_pnl_vs_ungated_gc_pct"]), float(row["drawdown_reduction_cash"])),
        reverse=True,
    )[0]
    net_share = (float(asia_only["net_pnl_cash"]) / float(asia_us["net_pnl_cash"]) * 100.0) if float(asia_us["net_pnl_cash"]) else 0.0
    dd_improvement = float(asia_us["max_drawdown"]) - float(asia_only["max_drawdown"])
    lines = [
        "# ATP Companion GC Asia-Only Feasibility",
        "",
        f"- Classification: `{classification}`",
        "- This is a session-scope feasibility review only. It does not change GC expression semantics.",
        "- No threshold sensitivity, exit redesign, staged adds, IBKR, broker, or old-v1 comparison work was run.",
        "- London remains diagnostic-only.",
        "",
        "## Asia-Only vs Asia+U.S.",
        f"- GC Asia-only trade count: `{asia_only['trade_count']}`",
        f"- GC Asia-only gross/net: `{asia_only['gross_pnl_cash']}` / `{asia_only['net_pnl_cash']}`",
        f"- GC Asia-only max drawdown: `{asia_only['max_drawdown']}`",
        f"- GC Asia-only drawdown-to-profit ratio: `{asia_only['drawdown_to_profit_ratio']}`",
        f"- GC Asia+U.S. net P/L: `{asia_us['net_pnl_cash']}`",
        f"- GC Asia+U.S. max drawdown: `{asia_us['max_drawdown']}`",
        f"- Removing U.S. retains `{round(net_share, 4)}`% of GC Asia+U.S. net P/L while changing drawdown by `{round(dd_improvement, 4)}`",
        "",
        "## 2024 Behavior",
    ]
    if asia_only_2024 is not None:
        lines.append(
            f"- GC Asia-only 2024 net P/L: `{asia_only_2024['net_pnl_cash']}` with max DD `{asia_only_2024['max_drawdown']}`"
        )
    if asia_us_2024 is not None:
        lines.append(
            f"- GC Asia+U.S. 2024 net P/L: `{asia_us_2024['net_pnl_cash']}` with max DD `{asia_us_2024['max_drawdown']}`"
        )
    lines.extend(
        [
            "",
            "## MGC Context",
            f"- Replay-safe MGC current-v2 net P/L: `{mgc_context['net_pnl_cash']}`",
            f"- Replay-safe MGC current-v2 max drawdown: `{mgc_context['max_drawdown']}`",
            f"- Replay-safe MGC current-v2 drawdown-to-profit ratio: `{mgc_context['drawdown_to_profit_ratio']}`",
            "",
            "## Governance Boundary",
        ]
    )
    for row in fivek_rows:
        lines.append(
            f"- `$5,000` / `{row['reset_policy']}` retained `{row['retained_net_pnl_cash']}` "
            f"(`{row['retained_net_pnl_vs_ungated_gc_pct']}%`) with max DD `{row['max_drawdown_after_governance']}` "
            f"and expectancy effect `{row['expectancy_effect']}`"
        )
    lines.extend(
        [
            "",
            "## Strongest Lower-Cap Governance Row",
            f"- Best sub-`$10,000` row: `{strongest_low['governance_id']}`",
            f"- Retained net P/L: `{strongest_low['retained_net_pnl_cash']}` (`{strongest_low['retained_net_pnl_vs_ungated_gc_pct']}%` of Asia-only GC)",
            f"- Max drawdown after governance: `{strongest_low['max_drawdown_after_governance']}`",
            f"- Drawdown reduction: `{strongest_low['drawdown_reduction_cash']}`",
            f"- Psychological survivability: `{strongest_low['psychological_survivability']}`",
            "",
            "## Practical Read",
            "- Do not treat this as a final sizing or governance recommendation.",
            "- The only question here is whether removing U.S. makes the replay-safe GC expression materially more usable.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_gc_asia_only_feasibility(*, gc_trades_jsonl: Path, mgc_cost_summary_csv: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades = _load_gc_trade_rows(gc_trades_jsonl)
    gc_asia_us_trades = list(trades)
    gc_asia_only_trades = [trade for trade in trades if trade["session"] == "ASIA"]

    asia_us_summary = _summary_for_scope(scope_id="GC_ASIA_PLUS_US", trades=gc_asia_us_trades)
    asia_only_summary = _summary_for_scope(scope_id="GC_ASIA_ONLY", trades=gc_asia_only_trades)
    mgc_context = _load_mgc_context_row(mgc_cost_summary_csv)

    feasibility_summary_rows = [asia_only_summary]
    comparison_rows = _comparison_rows(asia_only=asia_only_summary, asia_us=asia_us_summary, mgc_context=mgc_context)
    year_rows = _year_rows_for_scope(scope_id="GC_ASIA_PLUS_US", trades=gc_asia_us_trades) + _year_rows_for_scope(
        scope_id="GC_ASIA_ONLY",
        trades=gc_asia_only_trades,
    )

    baseline = _baseline_metrics(gc_asia_only_trades)
    governance_rows: list[dict[str, Any]] = []
    for cap in CAP_GRID:
        for reset_policy in RESET_POLICIES:
            policy = GovernancePolicy(cap_cash=cap, reset_policy=reset_policy)
            _trade_ledger, _stop_events, payload = _simulate_policy(gc_asia_only_trades, policy)
            governance_rows.append(payload["summary"])
    _annotate_summary_rows(governance_rows, baseline=baseline)
    classification = _classify(
        asia_only_summary=asia_only_summary,
        asia_us_summary=asia_us_summary,
        governance_rows=governance_rows,
    )
    for row in feasibility_summary_rows:
        row["classification"] = classification
    for row in comparison_rows:
        row["classification"] = classification
    for row in governance_rows:
        row["classification"] = classification
    summary_md = _render_markdown(
        classification=classification,
        asia_only=asia_only_summary,
        asia_us=asia_us_summary,
        mgc_context=mgc_context,
        governance_rows=governance_rows,
        year_rows=year_rows,
    )

    feasibility_summary_csv = output_dir / "atp_gc_asia_only_feasibility_summary.csv"
    comparison_csv = output_dir / "atp_gc_asia_only_vs_asia_us_comparison.csv"
    year_csv = output_dir / "atp_gc_asia_only_year_decomposition.csv"
    governance_csv = output_dir / "atp_gc_asia_only_drawdown_governance_summary.csv"
    summary_md_path = output_dir / "atp_gc_asia_only_feasibility_summary.md"

    _write_csv(feasibility_summary_csv, feasibility_summary_rows)
    _write_csv(comparison_csv, comparison_rows)
    _write_csv(year_csv, year_rows)
    _write_csv(governance_csv, governance_rows)
    _write_markdown(summary_md_path, summary_md)

    return {
        "feasibility_summary_csv": feasibility_summary_csv,
        "comparison_csv": comparison_csv,
        "year_csv": year_csv,
        "governance_csv": governance_csv,
        "summary_md_path": summary_md_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_only_latest")
    artifacts = build_gc_asia_only_feasibility(
        gc_trades_jsonl=Path(args.gc_trades_jsonl).expanduser().resolve(),
        mgc_cost_summary_csv=Path(args.mgc_cost_summary_csv).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
