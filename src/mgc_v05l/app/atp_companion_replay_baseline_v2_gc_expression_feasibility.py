"""GC expression feasibility pass for ATP Companion Replay Baseline v2."""

from __future__ import annotations

import argparse
import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

from .asia_london_participation_research import (
    POINT_VALUES as REPO_POINT_VALUES,
    ROUND_TURN_COMMISSION_DOLLARS,
    TICK_SIZES,
)
from .atp_companion_full_history_review import (
    DEFAULT_PLATFORM_SUBSTRATE_ROOT,
    DEFAULT_SOURCE_DB,
    _discover_best_sources,
    _materialize_symbol_truth,
    _trade_record_replay_row,
    build_replay_truth_manifest,
    build_targets,
)
from .atp_companion_replay_baseline_v2 import _base_position_rows, _classification as _replay_validation_classification, _summary_from_trade_rows, _validation_observed_from_replay_jsonl, _validation_rows, _write_jsonl
from ..research.platform import stable_hash
from ..research.platform import write_json_manifest
from ..research.trend_participation.atp_promotion_add_review import default_atp_promotion_add_candidates
from ..research.trend_participation.substrate import _read_manifest, ensure_atp_feature_bundle, ensure_atp_scope_bundle

REPO_ROOT = Path.cwd()
DEFAULT_MGC_MANIFEST = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2"
    / "mgc_asia_us_latest"
    / "atp_companion_replay_baseline_v2_manifest.json"
)
DEFAULT_MGC_TRADES_JSONL = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "atp_companion_replay_baseline_v2"
    / "mgc_asia_us_latest"
    / "atp_companion_replay_baseline_v2_trades.jsonl"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "outputs" / "reports" / "atp_companion_replay_baseline_v2_gc_expression"


@dataclass(frozen=True)
class CostScenario:
    scenario_id: str
    label: str
    instrument_expression: str
    fee_multiplier: float | None = None
    slippage_multiplier: float | None = None
    fixed_fee_cash: float | None = None
    fixed_slippage_cash: float | None = None
    note: str | None = None


MGC_SCENARIOS: tuple[CostScenario, ...] = (
    CostScenario("MGC_GROSS_ZERO_COST", "MGC gross / zero cost", "MGC", fee_multiplier=0.0, slippage_multiplier=0.0),
    CostScenario("MGC_CURRENT_V2_COST_MODEL", "MGC current v2 cost model", "MGC", fee_multiplier=1.0, slippage_multiplier=1.0),
    CostScenario("MGC_FEE_ONLY", "MGC fee-only", "MGC", fee_multiplier=1.0, slippage_multiplier=0.0),
    CostScenario("MGC_SLIPPAGE_ONLY", "MGC slippage-only", "MGC", fee_multiplier=0.0, slippage_multiplier=1.0),
    CostScenario("MGC_HALF_SLIPPAGE", "MGC half slippage", "MGC", fee_multiplier=1.0, slippage_multiplier=0.5),
    CostScenario("MGC_DOUBLE_SLIPPAGE", "MGC double slippage", "MGC", fee_multiplier=1.0, slippage_multiplier=2.0),
    CostScenario(
        "MGC_CONSERVATIVE_LIVE_PROXY",
        "MGC conservative live proxy",
        "MGC",
        fee_multiplier=1.0,
        slippage_multiplier=2.0,
        note="No distinct live-specific ATP v2 MGC cost model is materialized on disk; this proxy reuses doubled current slippage with the current fee model.",
    ),
)


GC_FRAMEWORK_SCENARIOS: tuple[CostScenario, ...] = (
    CostScenario("GC_GROSS_ZERO_COST", "GC gross / zero cost", "GC", fee_multiplier=0.0, slippage_multiplier=0.0),
    CostScenario(
        "GC_CURRENT_FRAMEWORK_REPLAY_COST_MODEL",
        "GC current framework replay cost model",
        "GC",
        fee_multiplier=1.0,
        slippage_multiplier=1.0,
        note="This uses the ATP replay engine's current trade-record cost stack as generated for GC under the same v2 framework semantics.",
    ),
)


GC_REPO_PROXY_SCENARIOS: tuple[CostScenario, ...] = (
    CostScenario(
        "GC_CURRENT_REPO_CONTRACT_COST_PROXY",
        "GC current repo contract-cost proxy",
        "GC",
        fixed_fee_cash=ROUND_TURN_COMMISSION_DOLLARS["GC"],
        fixed_slippage_cash=2.0 * 1.0 * TICK_SIZES["GC"] * REPO_POINT_VALUES["GC"],
        note="Repo-grounded proxy using GC commission plus one tick per side slippage.",
    ),
    CostScenario(
        "GC_FEE_ONLY",
        "GC fee-only",
        "GC",
        fixed_fee_cash=ROUND_TURN_COMMISSION_DOLLARS["GC"],
        fixed_slippage_cash=0.0,
    ),
    CostScenario(
        "GC_SLIPPAGE_ONLY",
        "GC slippage-only",
        "GC",
        fixed_fee_cash=0.0,
        fixed_slippage_cash=2.0 * 1.0 * TICK_SIZES["GC"] * REPO_POINT_VALUES["GC"],
    ),
    CostScenario(
        "GC_HALF_SLIPPAGE",
        "GC half slippage",
        "GC",
        fixed_fee_cash=ROUND_TURN_COMMISSION_DOLLARS["GC"],
        fixed_slippage_cash=2.0 * 0.5 * TICK_SIZES["GC"] * REPO_POINT_VALUES["GC"],
    ),
    CostScenario(
        "GC_DOUBLE_SLIPPAGE",
        "GC double slippage",
        "GC",
        fixed_fee_cash=ROUND_TURN_COMMISSION_DOLLARS["GC"],
        fixed_slippage_cash=2.0 * 2.0 * TICK_SIZES["GC"] * REPO_POINT_VALUES["GC"],
    ),
    CostScenario(
        "GC_CONSERVATIVE_LIVE_PROXY",
        "GC conservative live proxy",
        "GC",
        fixed_fee_cash=ROUND_TURN_COMMISSION_DOLLARS["GC"],
        fixed_slippage_cash=2.0 * 2.0 * TICK_SIZES["GC"] * REPO_POINT_VALUES["GC"],
        note="No separate live-specific GC ATP model is materialized on disk; this conservative proxy reuses doubled one-tick-per-side repo slippage.",
    ),
)


NAIVE_SCENARIOS: tuple[CostScenario, ...] = (
    CostScenario(
        "NAIVE_10X_MGC_SCALING_REFERENCE_ONLY",
        "Naive 10x MGC scaling proxy",
        "NAIVE_10X_MGC_PROXY",
        note="Reference only. This multiplies MGC current-v2 economics by 10 and is not a real GC replay result.",
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="atp-companion-replay-baseline-v2-gc-expression-feasibility")
    parser.add_argument("--source-db", default=str(DEFAULT_SOURCE_DB), help="SQLite bars database path.")
    parser.add_argument("--mgc-manifest", default=str(DEFAULT_MGC_MANIFEST), help="Replay Baseline v2 MGC manifest path.")
    parser.add_argument("--mgc-trades-jsonl", default=str(DEFAULT_MGC_TRADES_JSONL), help="Replay Baseline v2 MGC trades JSONL.")
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


def _max_drawdown(pnls: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return abs(max_drawdown)


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


def _drawdown_to_profit_ratio(net_pnl_cash: float, max_drawdown: float) -> float | None:
    if net_pnl_cash <= 0:
        return None
    return max_drawdown / net_pnl_cash


def _normalize_trade_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in _iter_jsonl(path):
        trade = raw["trade_record"]
        entry_ts = str(trade["entry_ts"]).replace(" ", "T")
        rows.append(
            {
                "trade_id": raw["trade_id"],
                "instrument": trade["instrument"],
                "session": trade["session_segment"],
                "direction": trade["side"],
                "year": entry_ts[:4],
                "entry_ts": entry_ts,
                "exit_ts": str(trade["exit_ts"]).replace(" ", "T"),
                "exit_reason": trade["exit_reason"],
                "gross_pnl_cash": float(trade["gross_pnl_cash"]),
                "base_fee_cash": float(trade["fees_paid"]),
                "base_slippage_cash": float(trade["slippage_cost"]),
                "baseline_net_pnl_cash": float(trade["pnl_cash"]),
            }
        )
    return rows


def _scenario_trade_rows(trades: Sequence[dict[str, Any]], scenario: CostScenario) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for trade in trades:
        if scenario.fixed_fee_cash is not None or scenario.fixed_slippage_cash is not None:
            fees = float(scenario.fixed_fee_cash or 0.0)
            slippage = float(scenario.fixed_slippage_cash or 0.0)
        else:
            fees = float(trade["base_fee_cash"]) * float(scenario.fee_multiplier or 0.0)
            slippage = float(trade["base_slippage_cash"]) * float(scenario.slippage_multiplier or 0.0)
        net = float(trade["gross_pnl_cash"]) - fees - slippage
        output.append(
            {
                **trade,
                "scenario_id": scenario.scenario_id,
                "scenario_label": scenario.label,
                "instrument_expression": scenario.instrument_expression,
                "fees_cash": fees,
                "slippage_cash": slippage,
                "net_pnl_cash": net,
            }
        )
    return output


def _scenario_summary_row(scenario: CostScenario, rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    pnls = [float(row["net_pnl_cash"]) for row in rows]
    gross_total = round(sum(float(row["gross_pnl_cash"]) for row in rows), 4)
    fee_total = round(sum(float(row["fees_cash"]) for row in rows), 4)
    slippage_total = round(sum(float(row["slippage_cash"]) for row in rows), 4)
    net_total = round(sum(pnls), 4)
    pf = _profit_factor(pnls)
    max_dd = round(_max_drawdown(pnls), 4)
    dd_ratio = _drawdown_to_profit_ratio(net_total, max_dd)
    avg_trade = net_total / max(len(rows), 1)
    breakeven_cost_per_trade = gross_total / max(len(rows), 1)
    cost_consumed_pct = ((fee_total + slippage_total) / gross_total * 100.0) if gross_total > 0 else 0.0
    net_per_drawdown = (net_total / max_dd) if max_dd > 0 else None
    return {
        "scenario_id": scenario.scenario_id,
        "scenario_label": scenario.label,
        "instrument_expression": scenario.instrument_expression,
        "scenario_note": scenario.note or "",
        "trade_count": len(rows),
        "total_gross_pnl_cash": gross_total,
        "total_fees_cash": fee_total,
        "total_slippage_cash": slippage_total,
        "total_net_pnl_cash": net_total,
        "average_net_trade_cash": round(avg_trade, 4),
        "median_net_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
        "win_rate": round((sum(1 for pnl in pnls if pnl > 0) / len(pnls) * 100.0), 4) if pnls else 0.0,
        "profit_factor": round(float(pf), 4) if pf is not None else "",
        "max_drawdown": max_dd,
        "largest_win": round(max(pnls), 4) if pnls else 0.0,
        "largest_loss": round(min(pnls), 4) if pnls else 0.0,
        "max_consecutive_losers": _max_losing_streak(pnls),
        "asia_contribution_cash": round(sum(float(row["net_pnl_cash"]) for row in rows if row["session"] == "ASIA"), 4),
        "us_contribution_cash": round(sum(float(row["net_pnl_cash"]) for row in rows if row["session"] == "US"), 4),
        "cost_consumed_pct_of_gross_edge": round(cost_consumed_pct, 4),
        "breakeven_cost_per_trade": round(breakeven_cost_per_trade, 4),
        "drawdown_to_profit_ratio": round(float(dd_ratio), 4) if dd_ratio is not None else "",
        "net_pnl_per_unit_of_drawdown": round(float(net_per_drawdown), 4) if net_per_drawdown is not None else "",
        "edge_economically_meaningful": "YES" if net_total > 0 and avg_trade > 0 and (pf or 0.0) > 1.0 else "NO",
    }


def _group_decomposition_rows(
    *,
    scenario: CostScenario,
    rows: Sequence[dict[str, Any]],
    axis: str,
    group_key: str,
) -> list[dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(str(row[group_key]), []).append(dict(row))
    output: list[dict[str, Any]] = []
    for bucket, items in sorted(buckets.items()):
        pnls = [float(item["net_pnl_cash"]) for item in items]
        net_total = round(sum(pnls), 4)
        pf = _profit_factor(pnls)
        output.append(
            {
                "instrument_expression": scenario.instrument_expression,
                "scenario_id": scenario.scenario_id,
                "scenario_label": scenario.label,
                "decomposition_axis": axis,
                "bucket": bucket,
                "trade_count": len(items),
                "total_gross_pnl_cash": round(sum(float(item["gross_pnl_cash"]) for item in items), 4),
                "total_fees_cash": round(sum(float(item["fees_cash"]) for item in items), 4),
                "total_slippage_cash": round(sum(float(item["slippage_cash"]) for item in items), 4),
                "total_net_pnl_cash": net_total,
                "average_net_trade_cash": round(net_total / max(len(items), 1), 4),
                "median_net_trade_cash": round(float(median(pnls)), 4) if pnls else 0.0,
                "win_rate": round((sum(1 for pnl in pnls if pnl > 0) / len(pnls) * 100.0), 4) if pnls else 0.0,
                "profit_factor": round(float(pf), 4) if pf is not None else "",
                "max_drawdown": round(_max_drawdown(pnls), 4),
                "largest_win": round(max(pnls), 4) if pnls else 0.0,
                "largest_loss": round(min(pnls), 4) if pnls else 0.0,
                "max_consecutive_losers": _max_losing_streak(pnls),
            }
        )
    return output


def _naive_10x_row(mgc_current: dict[str, Any]) -> dict[str, Any]:
    scaled = dict(mgc_current)
    scaled.update(
        {
            "scenario_id": "NAIVE_10X_MGC_SCALING_REFERENCE_ONLY",
            "scenario_label": "Naive 10x MGC scaling proxy",
            "instrument_expression": "NAIVE_10X_MGC_PROXY",
            "scenario_note": "Reference only. This scales the current MGC replay baseline by 10 and is not a real GC replay result.",
            "total_gross_pnl_cash": round(float(mgc_current["total_gross_pnl_cash"]) * 10.0, 4),
            "total_fees_cash": round(float(mgc_current["total_fees_cash"]) * 10.0, 4),
            "total_slippage_cash": round(float(mgc_current["total_slippage_cash"]) * 10.0, 4),
            "total_net_pnl_cash": round(float(mgc_current["total_net_pnl_cash"]) * 10.0, 4),
            "average_net_trade_cash": round(float(mgc_current["average_net_trade_cash"]) * 10.0, 4),
            "median_net_trade_cash": round(float(mgc_current["median_net_trade_cash"]) * 10.0, 4),
            "max_drawdown": round(float(mgc_current["max_drawdown"]) * 10.0, 4),
            "largest_win": round(float(mgc_current["largest_win"]) * 10.0, 4),
            "largest_loss": round(float(mgc_current["largest_loss"]) * 10.0, 4),
            "asia_contribution_cash": round(float(mgc_current["asia_contribution_cash"]) * 10.0, 4),
            "us_contribution_cash": round(float(mgc_current["us_contribution_cash"]) * 10.0, 4),
        }
    )
    if scaled.get("drawdown_to_profit_ratio") not in {"", None}:
        scaled["drawdown_to_profit_ratio"] = round(float(scaled["max_drawdown"]) / max(float(scaled["total_net_pnl_cash"]), 1e-9), 4)
    if scaled.get("net_pnl_per_unit_of_drawdown") not in {"", None} and float(scaled["max_drawdown"]) > 0:
        scaled["net_pnl_per_unit_of_drawdown"] = round(float(scaled["total_net_pnl_cash"]) / float(scaled["max_drawdown"]), 4)
    return scaled


def _comparison_rows(summary_rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    rows_by_id = {str(row["scenario_id"]): row for row in summary_rows}
    comparisons = [
        ("GC_GROSS_ZERO_COST", "MGC_GROSS_ZERO_COST", "gross"),
        ("GC_CURRENT_FRAMEWORK_REPLAY_COST_MODEL", "MGC_CURRENT_V2_COST_MODEL", "framework_current_vs_current"),
        ("GC_CURRENT_REPO_CONTRACT_COST_PROXY", "MGC_CURRENT_V2_COST_MODEL", "gc_proxy_current_vs_mgc_current"),
        ("GC_HALF_SLIPPAGE", "MGC_HALF_SLIPPAGE", "half_slippage"),
        ("GC_DOUBLE_SLIPPAGE", "MGC_DOUBLE_SLIPPAGE", "double_slippage"),
        ("GC_FEE_ONLY", "MGC_FEE_ONLY", "fee_only"),
        ("GC_SLIPPAGE_ONLY", "MGC_SLIPPAGE_ONLY", "slippage_only"),
        ("GC_CONSERVATIVE_LIVE_PROXY", "MGC_CONSERVATIVE_LIVE_PROXY", "conservative_live_proxy"),
        ("NAIVE_10X_MGC_SCALING_REFERENCE_ONLY", "MGC_CURRENT_V2_COST_MODEL", "naive_10x_reference"),
    ]
    output: list[dict[str, Any]] = []
    for gc_id, mgc_id, comparison_id in comparisons:
        gc_row = rows_by_id[gc_id]
        mgc_row = rows_by_id[mgc_id]
        output.append(
            {
                "comparison_id": comparison_id,
                "gc_scenario_id": gc_id,
                "mgc_scenario_id": mgc_id,
                "gc_net_pnl_cash": gc_row["total_net_pnl_cash"],
                "mgc_net_pnl_cash": mgc_row["total_net_pnl_cash"],
                "gc_minus_mgc_net_pnl_cash": round(float(gc_row["total_net_pnl_cash"]) - float(mgc_row["total_net_pnl_cash"]), 4),
                "gc_profit_factor": gc_row["profit_factor"],
                "mgc_profit_factor": mgc_row["profit_factor"],
                "gc_cost_consumed_pct": gc_row["cost_consumed_pct_of_gross_edge"],
                "mgc_cost_consumed_pct": mgc_row["cost_consumed_pct_of_gross_edge"],
                "gc_max_drawdown": gc_row["max_drawdown"],
                "mgc_max_drawdown": mgc_row["max_drawdown"],
                "gc_drawdown_to_profit_ratio": gc_row["drawdown_to_profit_ratio"],
                "mgc_drawdown_to_profit_ratio": mgc_row["drawdown_to_profit_ratio"],
                "gc_net_pnl_per_unit_of_drawdown": gc_row["net_pnl_per_unit_of_drawdown"],
                "mgc_net_pnl_per_unit_of_drawdown": mgc_row["net_pnl_per_unit_of_drawdown"],
                "gc_breakeven_cost_per_trade": gc_row["breakeven_cost_per_trade"],
                "mgc_breakeven_cost_per_trade": mgc_row["breakeven_cost_per_trade"],
                "gc_trade_count": gc_row["trade_count"],
                "mgc_trade_count": mgc_row["trade_count"],
            }
        )
    return output


def _classify(summary_rows: Sequence[dict[str, Any]]) -> str:
    rows_by_id = {str(row["scenario_id"]): row for row in summary_rows}
    mgc_current = rows_by_id["MGC_CURRENT_V2_COST_MODEL"]
    gc_framework = rows_by_id["GC_CURRENT_FRAMEWORK_REPLAY_COST_MODEL"]
    gc_current = rows_by_id["GC_CURRENT_REPO_CONTRACT_COST_PROXY"]
    gc_conservative = rows_by_id["GC_CONSERVATIVE_LIVE_PROXY"]

    mgc_net = float(mgc_current["total_net_pnl_cash"])
    mgc_cost = float(mgc_current["cost_consumed_pct_of_gross_edge"])
    mgc_dd = float(mgc_current["max_drawdown"])
    mgc_dd_ratio = float(mgc_current["drawdown_to_profit_ratio"] or 0.0) if mgc_current["drawdown_to_profit_ratio"] != "" else math.inf
    mgc_pf = float(mgc_current["profit_factor"] or 0.0)

    gc_framework_net = float(gc_framework["total_net_pnl_cash"])
    gc_net = float(gc_current["total_net_pnl_cash"])
    gc_cost = float(gc_current["cost_consumed_pct_of_gross_edge"])
    gc_dd = float(gc_current["max_drawdown"])
    gc_dd_ratio = float(gc_current["drawdown_to_profit_ratio"] or 0.0) if gc_current["drawdown_to_profit_ratio"] != "" else math.inf
    gc_pf = float(gc_current["profit_factor"] or 0.0)
    gc_conservative_net = float(gc_conservative["total_net_pnl_cash"])

    if gc_framework_net <= 0 and gc_net <= 0:
        return "GC_EXPRESSION_REJECTED"
    if gc_net <= 0 and gc_conservative_net <= 0:
        return "GC_EXPRESSION_REJECTED"
    cost_improved = gc_cost + 5.0 < mgc_cost
    risk_ratio_too_large = gc_dd > (mgc_dd * 8.0) or gc_dd_ratio > (mgc_dd_ratio * 1.5 if math.isfinite(mgc_dd_ratio) else math.inf)
    profitability_too_weak = gc_pf <= max(1.0, mgc_pf - 0.02)
    if cost_improved and risk_ratio_too_large:
        return "GC_EXPRESSION_COST_IMPROVED_BUT_RISK_TOO_LARGE"
    if cost_improved and gc_net > mgc_net and not profitability_too_weak and gc_conservative_net > 0:
        return "GC_EXPRESSION_PROMISING"
    if gc_net > 0 or gc_framework_net > 0:
        return "GC_EXPRESSION_MIXED"
    return "GC_EXPRESSION_REJECTED"


def _render_summary_markdown(
    *,
    classification: str,
    gc_manifest_payload: dict[str, Any],
    summary_rows: Sequence[dict[str, Any]],
    validation_rows: Sequence[dict[str, Any]],
) -> str:
    rows_by_id = {str(row["scenario_id"]): row for row in summary_rows}
    mgc_current = rows_by_id["MGC_CURRENT_V2_COST_MODEL"]
    gc_framework = rows_by_id["GC_CURRENT_FRAMEWORK_REPLAY_COST_MODEL"]
    gc_current = rows_by_id["GC_CURRENT_REPO_CONTRACT_COST_PROXY"]
    gc_conservative = rows_by_id["GC_CONSERVATIVE_LIVE_PROXY"]
    naive = rows_by_id["NAIVE_10X_MGC_SCALING_REFERENCE_ONLY"]
    lines = [
        "# ATP Companion Replay Baseline v2 GC Expression Feasibility",
        "",
        f"- Classification: `{classification}`",
        "- This is research only. It does not switch execution from MGC to GC.",
        "- Replay Baseline v2 remains the controlling reproducible ATP baseline for this pass.",
        "- Old v1 is materialized-only and not controlling truth here.",
        "- Legacy-intent v2 is diagnostic only and not used as the baseline.",
        "- No staged adds, threshold sensitivity, exit redesign, drawdown governance, live execution, IBKR, broker, or US Open research changes were run.",
        "- London remains diagnostic-only.",
        "",
        "## GC Replay Build",
        f"- GC trade count: `{gc_manifest_payload['summary_metrics']['trade_count']}`",
        f"- GC self-reproduction classification: `{gc_manifest_payload['classification']}`",
        f"- Shared date span: `{gc_manifest_payload['shared_date_span']['start_timestamp']}` -> `{gc_manifest_payload['shared_date_span']['end_timestamp']}`",
        "",
        "## Cost Read",
        f"- MGC current net P/L: `{mgc_current['total_net_pnl_cash']}` with cost consumed `{mgc_current['cost_consumed_pct_of_gross_edge']}%`",
        f"- GC framework-current net P/L: `{gc_framework['total_net_pnl_cash']}` with cost consumed `{gc_framework['cost_consumed_pct_of_gross_edge']}%`",
        f"- GC repo-current proxy net P/L: `{gc_current['total_net_pnl_cash']}` with cost consumed `{gc_current['cost_consumed_pct_of_gross_edge']}%`",
        f"- GC conservative live proxy net P/L: `{gc_conservative['total_net_pnl_cash']}`",
        f"- Naive 10x MGC reference net P/L: `{naive['total_net_pnl_cash']}`",
        "",
        "## Risk Framing",
        "- One GC contract is roughly 10x the notional expression of one MGC contract.",
        f"- MGC current max drawdown: `{mgc_current['max_drawdown']}`",
        f"- GC repo-current proxy max drawdown: `{gc_current['max_drawdown']}`",
        f"- MGC drawdown-to-profit ratio: `{mgc_current['drawdown_to_profit_ratio']}`",
        f"- GC repo-current proxy drawdown-to-profit ratio: `{gc_current['drawdown_to_profit_ratio']}`",
        "",
        "## Replay Validation",
    ]
    for row in validation_rows:
        lines.append(f"- `{row['metric']}` expected=`{row['expected']}` observed=`{row['observed']}` pass=`{row['passed']}`")
    lines.extend(
        [
            "",
            "## Interpretation Discipline",
            "- Do not treat GC as superior unless its net edge improves without unacceptable drawdown expansion.",
            "- Do not treat the naive 10x proxy as a real GC result.",
            "- This pass only asks whether GC expression is more cost-efficient than MGC inside the same replay-safe ATP framework.",
        ]
    )
    return "\n".join(lines) + "\n"


def _load_current_mgc_scope_manifest(mgc_manifest_path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest_payload = json.loads(mgc_manifest_path.read_text(encoding="utf-8"))
    scope_bundle_id = str((((manifest_payload.get("bundle_contract") or {}).get("scope_bundle_ids") or {}).get("MGC:ASIA/US")) or "")
    if not scope_bundle_id:
        raise RuntimeError("Replay Baseline v2 MGC manifest is missing the controlling MGC scope bundle id.")
    scope_manifest = _read_manifest(DEFAULT_PLATFORM_SUBSTRATE_ROOT / "scope_bundles" / scope_bundle_id / "manifest.json")
    return manifest_payload, scope_manifest


def _build_gc_replay_baseline(*, source_db: Path, mgc_manifest_path: Path, output_dir: Path) -> dict[str, Any]:
    mgc_manifest_payload, mgc_scope_manifest = _load_current_mgc_scope_manifest(mgc_manifest_path)
    run_start = datetime.fromisoformat(str((mgc_manifest_payload.get("shared_date_span") or {})["start_timestamp"]))
    run_end = datetime.fromisoformat(str((mgc_manifest_payload.get("shared_date_span") or {})["end_timestamp"]))
    entry_activation_basis = str(mgc_scope_manifest.get("entry_activation_basis") or "rolling_5m_on_1m")
    quality_bucket_policy = mgc_scope_manifest.get("quality_bucket_policy")
    allow_pre_5m = bool(mgc_scope_manifest.get("allow_pre_5m_context_participation") or False)
    exit_policy = str(mgc_scope_manifest.get("exit_policy") or "fixed_target_time_stop")
    variant_overrides = dict(mgc_scope_manifest.get("variant_overrides") or {})
    sides = tuple(mgc_scope_manifest.get("sides") or ["LONG"])

    bar_source_index = _discover_best_sources(symbols={"GC"}, timeframes={"1m", "5m"}, sqlite_paths=[source_db])
    symbol_truth = _materialize_symbol_truth(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        source_db=source_db,
        symbol="GC",
        bar_source_index=bar_source_index,
        start_timestamp=run_start,
        end_timestamp=run_end,
    )
    feature_bundle = ensure_atp_feature_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        source_db=source_db,
        symbol="GC",
        selected_sources=symbol_truth.selected_sources,
        start_timestamp=run_start,
        end_timestamp=run_end,
        feature_rows=symbol_truth.rolling_scope_feature_rows,
    )
    scope_bundle = ensure_atp_scope_bundle(
        bundle_root=DEFAULT_PLATFORM_SUBSTRATE_ROOT,
        source_db=source_db,
        symbol="GC",
        selected_sources=symbol_truth.selected_sources,
        start_timestamp=run_start,
        end_timestamp=run_end,
        allowed_sessions=("ASIA", "US"),
        point_value=100.0,
        bars_1m=symbol_truth.bars_1m,
        feature_bundle=feature_bundle,
        entry_activation_basis=entry_activation_basis,
        quality_bucket_policy=quality_bucket_policy,
        allow_pre_5m_context_participation=allow_pre_5m,
        sides=sides,
        exit_policy=exit_policy,
        variant_overrides=variant_overrides,
    )
    reference_candidate = next(candidate for candidate in default_atp_promotion_add_candidates() if candidate.candidate_id == "promotion_1_075r_favorable_only")
    baseline_target = next(target for target in build_targets() if target.target_id == "atp_companion_v1__candidate_gc_asia_us")

    manifest_payload = build_replay_truth_manifest(
        source_db=source_db,
        run_start=run_start,
        run_end=run_end,
        baseline_target=baseline_target,
        data_substrate={
            "selected_sources": symbol_truth.selected_sources,
            "scope_bundle_id": scope_bundle.bundle_id,
            "feature_bundle_id": feature_bundle.bundle_id,
        },
        review_config={
            "baseline_id": "atp_companion_replay_baseline_v2_gc_expression",
            "scope_bundle_id": scope_bundle.bundle_id,
            "entry_activation_basis": entry_activation_basis,
            "quality_bucket_policy": quality_bucket_policy,
            "allow_pre_5m_context_participation": allow_pre_5m,
            "exit_policy": exit_policy,
            "variant_overrides": variant_overrides,
        },
        scope_bundle_ids={"GC:ASIA/US": scope_bundle.bundle_id},
        feature_bundle_ids={"GC": feature_bundle.bundle_id},
        context_bundle_ids={"GC": symbol_truth.context_bundle_id},
        reference_candidate=reference_candidate,
    )

    replay_manifest_path = output_dir / "atp_companion_replay_baseline_v2_gc_manifest.json"
    replay_trades_path = output_dir / "atp_companion_replay_baseline_v2_gc_trades.jsonl"
    benchmark_rows_path = output_dir / "atp_companion_replay_baseline_v2_gc_benchmark_position_rows.jsonl"
    validation_csv_path = output_dir / "atp_companion_replay_baseline_v2_gc_validation_report.csv"
    validation_json_path = output_dir / "atp_companion_replay_baseline_v2_gc_validation_report.json"

    _write_jsonl(benchmark_rows_path, iter(_base_position_rows(scope_bundle.trade_rows)))
    _write_jsonl(
        replay_trades_path,
        (
            {
                "trade_id": str(row["trade_id"]),
                "instrument": trade.instrument,
                "session": trade.session_segment,
                "direction": trade.side,
                "decision_ts": trade.decision_ts,
                "entry_ts": trade.entry_ts,
                "exit_ts": trade.exit_ts,
                "entry_price": float(trade.entry_price),
                "exit_price": float(trade.exit_price),
                "baseline_pnl_cash": float(trade.pnl_cash),
                "stop_price": float(trade.stop_price),
                "target_price": float(trade.target_price) if trade.target_price is not None else None,
                "initial_risk_points": round(max(abs(float(trade.entry_price) - float(trade.stop_price)), 1e-9), 6),
                "r_unit_points": round(max(abs(float(trade.entry_price) - float(trade.stop_price)), 1e-9), 6),
                "point_value": 100.0,
                "bars_held_1m": int(trade.bars_held_1m),
                "hold_minutes": float(trade.hold_minutes),
                "exit_reason": trade.exit_reason,
                "family": trade.family,
                "trade_record": _trade_record_replay_row(trade),
                "minute_path": [],
                "reference_candidate_075r": {
                    "candidate_id": reference_candidate.candidate_id,
                    "candidate_label": reference_candidate.label,
                    "progress_r_multiple": float(reference_candidate.progress_r_multiple),
                    "allowed_price_quality_states": list(reference_candidate.allowed_price_quality_states),
                    "require_positive_reacceleration": bool(reference_candidate.require_positive_reacceleration),
                    "require_low_above_entry": bool(reference_candidate.require_low_above_entry),
                    "max_adds_per_trade": int(reference_candidate.max_adds_per_trade),
                    "eligibility_timestamps": [],
                    "added": False,
                    "add_reason": "not_modeled_in_gc_expression_feasibility_pass",
                    "add_entry_ts": None,
                    "add_exit_ts": None,
                    "add_trigger_price": None,
                    "add_entry_price": None,
                    "add_pnl_cash": None,
                    "add_pnl_points": None,
                    "modeled_exit_dependency": "none",
                },
            }
            for row in scope_bundle.trade_rows
            for trade in [row["trade_record"]]
        ),
    )

    summary = _summary_from_trade_rows(scope_bundle.trade_rows, bar_count=len(symbol_truth.bars_1m))
    observed = _validation_observed_from_replay_jsonl(replay_trades_path)
    validation_rows = _validation_rows(summary, observed)
    validation_classification = _replay_validation_classification(validation_rows)
    manifest_payload.update(
        {
            "baseline_id": "atp_companion_replay_baseline_v2_gc_expression",
            "classification": validation_classification,
            "generator_module": "src/mgc_v05l/app/atp_companion_replay_baseline_v2_gc_expression_feasibility.py",
            "command": f"PYTHONPATH=src ./.venv/bin/python -m mgc_v05l.app.atp_companion_replay_baseline_v2_gc_expression_feasibility --output-dir {output_dir}",
            "instruments": ["GC"],
            "sessions": ["ASIA", "US"],
            "session_treatment": {
                "ASIA": "EXECUTABLE_RESEARCH_EXPRESSION",
                "US": "EXECUTABLE_RESEARCH_EXPRESSION",
                "LONDON": "DIAGNOSTIC_ONLY_NOT_EXECUTED",
            },
            "entry_exit_semantics": {
                "execution_model": mgc_scope_manifest.get("execution_model"),
                "entry_activation_basis": entry_activation_basis,
                "quality_bucket_policy": quality_bucket_policy,
                "allow_pre_5m_context_participation": allow_pre_5m,
                "exit_policy": exit_policy,
                "variant_overrides": variant_overrides,
                "sides": list(sides),
            },
            "source_hashes": {
                "selected_sources_hash": stable_hash(symbol_truth.selected_sources or {}),
                "variant_overrides_hash": stable_hash(variant_overrides),
                "bundle_id": scope_bundle.bundle_id,
                "feature_bundle_id": feature_bundle.bundle_id,
                "reference_mgc_scope_bundle_id": (((mgc_manifest_payload.get("bundle_contract") or {}).get("scope_bundle_ids") or {}).get("MGC:ASIA/US")),
            },
            "summary_metrics": summary,
            "artifacts": {
                "replay_trades_path": str(replay_trades_path.resolve()),
                "benchmark_position_rows_path": str(benchmark_rows_path.resolve()),
                "validation_csv_path": str(validation_csv_path.resolve()),
            },
            "validation_reference": {
                "expected": summary,
                "observed": observed,
            },
            "source_mode": "generated_scope_bundle_current_v2_semantics",
            "reference_baseline_note": "Old v1 remains materialized-only and not controlling truth. This GC expression is built against the replay-safe ATP Companion Replay Baseline v2 framework.",
        }
    )
    write_json_manifest(replay_manifest_path, manifest_payload)
    _write_csv(validation_csv_path, validation_rows)
    validation_json_path.write_text(json.dumps({"classification": validation_classification, "rows": validation_rows}, indent=2), encoding="utf-8")
    return {
        "manifest_payload": manifest_payload,
        "replay_manifest_path": replay_manifest_path,
        "replay_trades_path": replay_trades_path,
        "validation_rows": validation_rows,
    }


def build_gc_expression_feasibility(*, source_db: Path, mgc_manifest_path: Path, mgc_trades_jsonl: Path, output_dir: Path) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    gc_build = _build_gc_replay_baseline(source_db=source_db, mgc_manifest_path=mgc_manifest_path, output_dir=output_dir)
    gc_manifest_payload = dict(gc_build["manifest_payload"])
    gc_replay_trades_path = Path(gc_build["replay_trades_path"])
    validation_rows = list(gc_build["validation_rows"])

    mgc_trades = _normalize_trade_rows(mgc_trades_jsonl)
    gc_trades = _normalize_trade_rows(gc_replay_trades_path)

    summary_rows: list[dict[str, Any]] = []
    decomposition_rows: list[dict[str, Any]] = []
    for scenario in MGC_SCENARIOS:
        scenario_rows = _scenario_trade_rows(mgc_trades, scenario)
        summary_rows.append(_scenario_summary_row(scenario, scenario_rows))
        for axis, key in (("session", "session"), ("year", "year"), ("direction", "direction"), ("exit_reason", "exit_reason")):
            decomposition_rows.extend(_group_decomposition_rows(scenario=scenario, rows=scenario_rows, axis=axis, group_key=key))
    for scenario in GC_FRAMEWORK_SCENARIOS + GC_REPO_PROXY_SCENARIOS:
        scenario_rows = _scenario_trade_rows(gc_trades, scenario)
        summary_rows.append(_scenario_summary_row(scenario, scenario_rows))
        for axis, key in (("session", "session"), ("year", "year"), ("direction", "direction"), ("exit_reason", "exit_reason")):
            decomposition_rows.extend(_group_decomposition_rows(scenario=scenario, rows=scenario_rows, axis=axis, group_key=key))

    mgc_current = next(row for row in summary_rows if row["scenario_id"] == "MGC_CURRENT_V2_COST_MODEL")
    summary_rows.append(_naive_10x_row(mgc_current))

    classification = _classify(summary_rows)
    for row in summary_rows:
        row["classification"] = classification

    comparison_rows = _comparison_rows(summary_rows)
    summary_md = _render_summary_markdown(
        classification=classification,
        gc_manifest_payload=gc_manifest_payload,
        summary_rows=summary_rows,
        validation_rows=validation_rows,
    )

    expression_summary_csv = output_dir / "atp_v2_gc_expression_summary.csv"
    comparison_csv = output_dir / "atp_v2_gc_vs_mgc_cost_comparison.csv"
    decomposition_csv = output_dir / "atp_v2_gc_session_decomposition.csv"
    cost_sensitivity_csv = output_dir / "atp_v2_gc_cost_sensitivity.csv"
    summary_md_path = output_dir / "atp_v2_gc_expression_summary.md"

    _write_csv(expression_summary_csv, summary_rows)
    _write_csv(comparison_csv, comparison_rows)
    _write_csv(decomposition_csv, decomposition_rows)
    _write_csv(cost_sensitivity_csv, summary_rows)
    _write_markdown(summary_md_path, summary_md)

    return {
        "expression_summary_csv": expression_summary_csv,
        "comparison_csv": comparison_csv,
        "decomposition_csv": decomposition_csv,
        "cost_sensitivity_csv": cost_sensitivity_csv,
        "summary_md_path": summary_md_path,
        "gc_replay_manifest_path": Path(gc_build["replay_manifest_path"]),
        "gc_replay_trades_path": gc_replay_trades_path,
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else (DEFAULT_OUTPUT_ROOT / "gc_asia_us_latest")
    artifacts = build_gc_expression_feasibility(
        source_db=Path(args.source_db).expanduser().resolve(),
        mgc_manifest_path=Path(args.mgc_manifest).expanduser().resolve(),
        mgc_trades_jsonl=Path(args.mgc_trades_jsonl).expanduser().resolve(),
        output_dir=output_dir,
    )
    for label, path in artifacts.items():
        print(f"Wrote {label} -> {path}")


if __name__ == "__main__":
    main()
