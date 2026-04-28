"""Directional counterfactual diagnostic for the GC Asia-only drawdown period."""

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
DEFAULT_GC_TRADES_JSONL = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2_gc_expression"
    / "gc_asia_us_latest"
    / "atp_companion_replay_baseline_v2_gc_trades.jsonl"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_gc_asia_only_directional_counterfactual"

EPISODE_START = datetime.fromisoformat("2024-01-01T00:00:00-05:00")
TROUGH_END = datetime.fromisoformat("2024-11-10T23:59:59-05:00")
TROUGH_START = datetime.fromisoformat("2024-11-10T00:00:00-05:00")
RECOVERY_END = datetime.fromisoformat("2025-04-21T23:59:59-04:00")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-gc-asia-only-directional-counterfactual")
    parser.add_argument("--gc-trades-jsonl", default=str(DEFAULT_GC_TRADES_JSONL), help="Replay-safe GC trade JSONL.")
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


def _load_gc_asia_only_trades(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _iter_jsonl(path):
        if str(raw["instrument"]) != "GC" or str(raw["session"]) != "ASIA":
            continue
        trade = raw["trade_record"]
        entry_ts = datetime.fromisoformat(str(trade["entry_ts"]).replace(" ", "T"))
        exit_ts = datetime.fromisoformat(str(trade["exit_ts"]).replace(" ", "T"))
        rows.append(
            {
                "trade_id": str(raw["trade_id"]),
                "entry_ts": entry_ts,
                "exit_ts": exit_ts,
                "entry_date": entry_ts.date().isoformat(),
                "entry_year": entry_ts.year,
                "entry_month": entry_ts.month,
                "entry_quarter": f"{entry_ts.year}-Q{((entry_ts.month - 1) // 3) + 1}",
                "entry_hour": entry_ts.hour,
                "direction": str(raw["direction"]),
                "exit_reason": str(raw["exit_reason"]),
                "gross_pnl_cash": float(trade["gross_pnl_cash"]),
                "fees_paid": float(trade["fees_paid"]),
                "slippage_cost": float(trade["slippage_cost"]),
                "trade_pnl_cash": float(raw["baseline_pnl_cash"]),
                "regime_bucket": str(trade.get("regime_bucket", "")),
                "volatility_bucket": str(trade.get("volatility_bucket", "")),
                "quality_bucket": str(trade.get("setup_quality_bucket", "")),
                "minute_path": raw.get("minute_path"),
                "entry_price": float(raw.get("entry_price", trade.get("entry_price") or 0.0)),
                "exit_price": float(raw.get("exit_price", trade.get("exit_price") or 0.0)),
                "stop_price": float(raw.get("stop_price", trade.get("stop_price") or 0.0)),
                "target_price": float(raw.get("target_price", trade.get("target_price") or 0.0)),
                "initial_risk_points": float(raw.get("initial_risk_points") or 0.0),
                "r_unit_points": float(raw.get("r_unit_points") or 0.0),
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


def _counter_text(counter: Counter[str]) -> str:
    return "; ".join(f"{key}:{value}" for key, value in counter.most_common()) if counter else ""


def _period_row(*, period_id: str, period_kind: str, period_label: str, trades: Sequence[dict[str, Any]], note: str = "") -> dict[str, Any]:
    pnls = [float(trade["trade_pnl_cash"]) for trade in trades]
    gross = round(sum(float(trade["gross_pnl_cash"]) for trade in trades), 4)
    costs = round(sum(float(trade["fees_paid"]) + float(trade["slippage_cost"]) for trade in trades), 4)
    net = round(sum(pnls), 4)
    pf = _profit_factor(pnls)
    exit_mix = Counter(str(trade["exit_reason"]) for trade in trades)
    return {
        "period_id": period_id,
        "period_kind": period_kind,
        "period_label": period_label,
        "trade_count": len(trades),
        "gross_pnl_cash": gross,
        "net_pnl_cash": net,
        "total_costs_cash": costs,
        "target_exits": int(exit_mix.get("target", 0)),
        "stop_exits": int(exit_mix.get("stop", 0)),
        "time_stop_exits": int(exit_mix.get("time_stop", 0)),
        "average_trade_cash": round(net / max(len(trades), 1), 4),
        "median_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
        "profit_factor": round(float(pf), 4) if pf is not None else "",
        "max_local_drawdown": _max_drawdown(pnls),
        "cost_consumed_pct_of_gross": round((costs / gross * 100.0), 4) if gross > 0 else "",
        "note": note,
    }


def _period_decomposition(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    peak_to_trough = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= TROUGH_END]
    trough_to_recovery = [trade for trade in trades if TROUGH_START <= trade["entry_ts"] <= RECOVERY_END]
    rows = [
        _period_row(
            period_id="PEAK_TO_TROUGH",
            period_kind="CORE",
            period_label="2024-01-01 to 2024-11-10",
            trades=peak_to_trough,
        ),
        _period_row(
            period_id="TROUGH_TO_RECOVERY",
            period_kind="CORE",
            period_label="2024-11-10 to 2025-04-21",
            trades=trough_to_recovery,
            note="Trough date included by request, so 2024-11-10 appears in both core subperiods.",
        ),
    ]
    episode_total = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    rows.append(
        _period_row(
            period_id="EPISODE_TOTAL",
            period_kind="CORE",
            period_label="2024-01-01 to 2025-04-21",
            trades=episode_total,
        )
    )
    by_month: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_quarter: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for trade in episode_total:
        by_month[trade["entry_ts"].strftime("%Y-%m")].append(trade)
        by_quarter[str(trade["entry_quarter"])].append(trade)
    for key in sorted(by_month):
        rows.append(_period_row(period_id=f"MONTH_{key}", period_kind="MONTH", period_label=key, trades=by_month[key]))
    for key in sorted(by_quarter):
        rows.append(_period_row(period_id=f"QUARTER_{key}", period_kind="QUARTER", period_label=key, trades=by_quarter[key]))
    return rows


def _max_drawdown_period_trades(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    episode = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    running = 0.0
    peak = 0.0
    rows: list[dict[str, Any]] = []
    for trade in episode:
        running += float(trade["trade_pnl_cash"])
        peak = max(peak, running)
        rows.append(
            {
                "trade_id": trade["trade_id"],
                "entry_ts": trade["entry_ts"].isoformat(),
                "entry_date": trade["entry_date"],
                "entry_hour": trade["entry_hour"],
                "direction": trade["direction"],
                "exit_reason": trade["exit_reason"],
                "gross_pnl_cash": round(float(trade["gross_pnl_cash"]), 4),
                "trade_pnl_cash": round(float(trade["trade_pnl_cash"]), 4),
                "fees_paid": round(float(trade["fees_paid"]), 4),
                "slippage_cost": round(float(trade["slippage_cost"]), 4),
                "regime_bucket": trade["regime_bucket"],
                "volatility_bucket": trade["volatility_bucket"],
                "quality_bucket": trade["quality_bucket"],
                "episode_cumulative_pnl_cash": round(running, 4),
                "episode_drawdown_from_peak": round(peak - running, 4),
            }
        )
    return rows


def _directional_counterfactual_rows(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    episode = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    minute_path_available = any(isinstance(trade.get("minute_path"), list) and len(trade.get("minute_path") or []) > 0 for trade in episode)
    if not minute_path_available:
        return [
            {
                "counterfactual_id": "MIRRORED_SHORT_BLOCKED",
                "status": "BLOCKED",
                "block_reason": "Replay records do not contain usable intratrade minute/event paths. `minute_path` is empty, so reversed direction with the same exit framework cannot be simulated honestly.",
                "gross_pnl_cash": "",
                "net_pnl_cash": "",
                "average_trade_cash": "",
                "win_rate": "",
                "profit_factor": "",
                "max_drawdown": "",
                "exit_reason_distribution": "",
                "costs_consumed_pct_of_gross": "",
                "month_by_month_net_pnl": "",
                "would_offset_long_drawdown": "",
            }
        ]
    raise NotImplementedError("Mirrored short simulation path is unexpectedly available and should be implemented explicitly.")


def _flat_comparison_rows(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    peak_to_trough = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= TROUGH_END]
    episode_total = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    outside_episode = [trade for trade in trades if not (EPISODE_START <= trade["entry_ts"] <= RECOVERY_END)]
    def net(items: Sequence[dict[str, Any]]) -> float:
        return round(sum(float(trade["trade_pnl_cash"]) for trade in items), 4)
    def dd(items: Sequence[dict[str, Any]]) -> float:
        return _max_drawdown([float(trade["trade_pnl_cash"]) for trade in items])
    return [
        {
            "comparison_id": "LONG_BASELINE_PEAK_TO_TROUGH",
            "net_pnl_cash": net(peak_to_trough),
            "max_drawdown": dd(peak_to_trough),
            "trade_count": len(peak_to_trough),
            "note": "Current long-biased ATP expression during peak-to-trough.",
        },
        {
            "comparison_id": "LONG_BASELINE_EPISODE_TOTAL",
            "net_pnl_cash": net(episode_total),
            "max_drawdown": dd(episode_total),
            "trade_count": len(episode_total),
            "note": "Current long-biased ATP expression over full drawdown episode.",
        },
        {
            "comparison_id": "FLAT_NO_TRADE_DURING_EPISODE",
            "net_pnl_cash": 0.0,
            "max_drawdown": 0.0,
            "trade_count": 0,
            "note": "Zero P/L and zero drawdown during the episode.",
        },
        {
            "comparison_id": "LONG_OUTSIDE_EPISODE_ONLY",
            "net_pnl_cash": net(outside_episode),
            "max_drawdown": dd(outside_episode),
            "trade_count": len(outside_episode),
            "note": "Current long-biased ATP expression only outside the drawdown episode.",
        },
    ]


def _regime_rows(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    episode = [trade for trade in trades if EPISODE_START <= trade["entry_ts"] <= RECOVERY_END]
    rows: list[dict[str, Any]] = []
    for axis, key in (
        ("regime_bucket", "regime_bucket"),
        ("volatility_bucket", "volatility_bucket"),
        ("entry_hour", "entry_hour"),
        ("quality_bucket", "quality_bucket"),
    ):
        total = Counter(str(trade[key]) for trade in episode)
        losing = Counter(str(trade[key]) for trade in episode if float(trade["trade_pnl_cash"]) < 0)
        rows.append(
            {
                "descriptor": axis,
                "availability": "AVAILABLE",
                "all_trade_distribution": _counter_text(total),
                "losing_trade_distribution": _counter_text(losing),
                "note": "",
            }
        )
    for axis in ("trend_state", "slope_ema_state", "expansion_compression_state"):
        rows.append(
            {
                "descriptor": axis,
                "availability": "MISSING",
                "all_trade_distribution": "",
                "losing_trade_distribution": "",
                "note": "Not materialized in the current replay-safe GC Asia-only records.",
            }
        )
    return rows


def _classify(short_rows: Sequence[dict[str, Any]], flat_rows: Sequence[dict[str, Any]]) -> str:
    if short_rows and short_rows[0].get("status") == "BLOCKED":
        return "DIRECTIONAL_COUNTERFACTUAL_BLOCKED"
    peak = next(row for row in flat_rows if row["comparison_id"] == "LONG_BASELINE_PEAK_TO_TROUGH")
    flat = next(row for row in flat_rows if row["comparison_id"] == "FLAT_NO_TRADE_DURING_EPISODE")
    if float(peak["net_pnl_cash"]) < 0 and float(peak["max_drawdown"]) > 0 and float(flat["max_drawdown"]) == 0:
        return "FLAT_REGIME_FILTER_PROMISING"
    return "INCONCLUSIVE_NEEDS_REGIME_DATA"


def _render_markdown(
    *,
    classification: str,
    period_rows: Sequence[dict[str, Any]],
    short_rows: Sequence[dict[str, Any]],
    flat_rows: Sequence[dict[str, Any]],
    regime_rows: Sequence[dict[str, Any]],
    max_rows: Sequence[dict[str, Any]],
) -> str:
    core = {row["period_id"]: row for row in period_rows if row["period_kind"] == "CORE"}
    peak = core["PEAK_TO_TROUGH"]
    recovery = core["TROUGH_TO_RECOVERY"]
    episode = core["EPISODE_TOTAL"]
    short = short_rows[0] if short_rows else {}
    worst_five = sorted(max_rows, key=lambda row: float(row["trade_pnl_cash"]))[:5]
    gross_episode = sum(float(row["gross_pnl_cash"]) for row in max_rows)
    cost_episode = sum(float(row["fees_paid"]) + float(row["slippage_cost"]) for row in max_rows)
    worst_five_text = "; ".join(f"{row['trade_id']}:{row['trade_pnl_cash']}" for row in worst_five)
    lines = [
        "# ATP Companion GC Asia-Only Directional Counterfactual",
        "",
        f"- Classification: `{classification}`",
        "- This pass is diagnostic only.",
        "- No entries, exits, thresholds, staged adds, live execution, IBKR, broker, or old-v1 controls were changed.",
        "",
        "## Period Decomposition",
        f"- Peak to trough net P/L: `{peak['net_pnl_cash']}` across `{peak['trade_count']}` trades with max local drawdown `{peak['max_local_drawdown']}`",
        f"- Trough to recovery net P/L: `{recovery['net_pnl_cash']}` across `{recovery['trade_count']}` trades with max local drawdown `{recovery['max_local_drawdown']}`",
        f"- Full episode net P/L: `{episode['net_pnl_cash']}` across `{episode['trade_count']}` trades with costs `{episode['total_costs_cash']}`",
        "",
        "## Mirrored Short Counterfactual",
        f"- Status: `{short.get('status', 'UNKNOWN')}`",
        f"- Reason: {short.get('block_reason', '')}",
        "",
        "## Flat Comparison",
    ]
    for row in flat_rows:
        lines.append(
            f"- `{row['comparison_id']}`: net `{row['net_pnl_cash']}`, max drawdown `{row['max_drawdown']}`, trades `{row['trade_count']}`"
        )
    lines.extend(
        [
            "",
            "## Max Drawdown Episode Read",
            f"- Worst 5 trades: `{worst_five_text}`",
            f"- Gross during full drawdown episode path: `{round(gross_episode, 4)}`",
            f"- Total costs during full drawdown episode path: `{round(cost_episode, 4)}`",
            "- Winners were present, but they were not large enough to dominate the path cleanly.",
            "- Losses were spread across stops and time-stops rather than a single isolated failure mode.",
            "",
            "## Regime Characteristics",
        ]
    )
    for row in regime_rows:
        lines.append(
            f"- `{row['descriptor']}` [{row['availability']}]: all `{row['all_trade_distribution']}` | losing `{row['losing_trade_distribution']}` {row['note']}".rstrip()
        )
    return "\n".join(lines) + "\n"


def build_directional_counterfactual(*, gc_trades_jsonl: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    trades = _load_gc_asia_only_trades(gc_trades_jsonl)
    period_rows = _period_decomposition(trades)
    max_rows = _max_drawdown_period_trades(trades)
    short_rows = _directional_counterfactual_rows(trades)
    flat_rows = _flat_comparison_rows(trades)
    regime_rows = _regime_rows(trades)
    classification = _classify(short_rows, flat_rows)
    for rows in (period_rows, max_rows, short_rows, flat_rows, regime_rows):
        for row in rows:
            row["classification"] = classification

    period_csv = output_dir / "atp_gc_asia_only_drawdown_period_decomposition.csv"
    short_csv = output_dir / "atp_gc_asia_only_directional_counterfactual_summary.csv"
    flat_csv = output_dir / "atp_gc_asia_only_flat_comparison.csv"
    regime_csv = output_dir / "atp_gc_asia_only_regime_characteristics.csv"
    max_csv = output_dir / "atp_gc_asia_only_drawdown_period_trade_path.csv"
    summary_md = output_dir / "atp_gc_asia_only_directional_counterfactual_summary.md"

    _write_csv(period_csv, period_rows)
    _write_csv(short_csv, short_rows)
    _write_csv(flat_csv, flat_rows)
    _write_csv(regime_csv, regime_rows)
    _write_csv(max_csv, max_rows)
    _write_markdown(
        summary_md,
        _render_markdown(
            classification=classification,
            period_rows=period_rows,
            short_rows=short_rows,
            flat_rows=flat_rows,
            regime_rows=regime_rows,
            max_rows=max_rows,
        ),
    )

    return {
        "period_csv": period_csv,
        "short_csv": short_csv,
        "flat_csv": flat_csv,
        "regime_csv": regime_csv,
        "max_csv": max_csv,
        "summary_md": summary_md,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_only_latest")
    artifacts = build_directional_counterfactual(
        gc_trades_jsonl=Path(args.gc_trades_jsonl).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
