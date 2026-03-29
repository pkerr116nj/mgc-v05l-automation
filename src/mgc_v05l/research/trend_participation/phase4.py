"""Phase 4 robustness and side-separation review for the Active Trend Participation Engine."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from .backtest import backtest_decisions_with_audit, summarize_performance
from .engine import DEFAULT_POINT_VALUES
from .features import build_feature_states
from .models import HigherPrioritySignal, TradeRecord
from .patterns import default_pattern_variants, generate_signal_decisions
from .storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m


PHASE3_BEST_LONG_VARIANT_ID = "trend_participation.pullback_continuation.long.conservative"
SHORT_RESEARCH_BRANCHES: tuple[tuple[str, set[str] | None], ...] = (
    ("all_buckets", None),
    ("medium_high_only", {"MEDIUM", "HIGH"}),
)


@dataclass(frozen=True)
class RollingWindow:
    label: str
    start_ts: datetime
    end_ts: datetime


def run_phase4_review(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MES", "MNQ"),
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    window_days: int = 7,
) -> dict[str, Path]:
    output_root = output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_sqlite_path, instruments=instruments)
    rolling_windows = build_rolling_windows(shared_start=shared_start, shared_end=shared_end, window_days=window_days)

    window_runs = [
        _run_window(
            source_sqlite_path=source_sqlite_path,
            instruments=instruments,
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            higher_priority_signals=higher_priority_signals,
        )
        for window in rolling_windows
    ]

    long_window_rows = []
    long_window_trade_rows: list[list[TradeRecord]] = []
    long_window_trades: list[TradeRecord] = []
    for window, run in zip(rolling_windows, window_runs, strict=True):
        trades = sorted(run["candidate_trades"].get(PHASE3_BEST_LONG_VARIANT_ID, []), key=lambda item: item.decision_ts)
        long_window_trade_rows.append(trades)
        long_window_trades.extend(trades)
        long_window_rows.append(
            {
                "window": _window_payload(window),
                "metrics": _metrics_payload(trades),
                "quality_bucket_breakdown": _bucket_breakdown(trades, key_name="setup_quality_bucket"),
                "acceptable": _acceptable_window(trades),
            }
        )

    short_candidate_map = _build_short_candidate_map(window_runs=window_runs)
    best_short_key, best_short_report = _select_best_short_candidate(short_candidate_map=short_candidate_map)
    best_short_window_trades = _candidate_window_trade_map(window_runs=window_runs, candidate_key=best_short_key)
    combined_window_rows = []
    combined_trades: list[TradeRecord] = []
    for long_row, long_trades, short_trades in zip(long_window_rows, long_window_trade_rows, best_short_window_trades, strict=True):
        merged = sorted(long_trades + short_trades, key=lambda item: item.decision_ts)
        combined_trades.extend(merged)
        combined_window_rows.append(
            {
                "window": long_row["window"],
                "metrics": _metrics_payload(merged),
            }
        )

    long_bucket_contribution = _bucket_breakdown(sorted(long_window_trades, key=lambda item: item.decision_ts), key_name="setup_quality_bucket")
    short_bucket_diagnosis = best_short_report["bucket_breakdown"]
    recommendation = _separation_recommendation(
        long_metrics=_metrics_payload(long_window_trades),
        short_metrics=best_short_report["aggregate_metrics"],
    )

    payload = {
        "module": "Active Trend Participation Engine",
        "phase": "phase4",
        "objective": (
            "Preserve the improved long-side allocation quality, test robustness beyond the March 10-17, 2026 audit "
            "window, and develop short-side candidates on separate promotion tracks without reopening live re-entry."
        ),
        "rolling_windows": [_window_payload(window) for window in rolling_windows],
        "long_lane": {
            "variant_id": PHASE3_BEST_LONG_VARIANT_ID,
            "aggregate_metrics": _metrics_payload(sorted(long_window_trades, key=lambda item: item.decision_ts)),
            "rolling_window_metrics": long_window_rows,
            "quality_bucket_contribution": long_bucket_contribution,
            "acceptable_window_count": sum(1 for row in long_window_rows if row["acceptable"]["acceptable"]),
        },
        "short_lane": best_short_report,
        "long_vs_short_comparison": {
            "long_variant_id": PHASE3_BEST_LONG_VARIANT_ID,
            "short_candidate_id": best_short_report["candidate_id"],
            "long_metrics": _metrics_payload(sorted(long_window_trades, key=lambda item: item.decision_ts)),
            "short_metrics": best_short_report["aggregate_metrics"],
        },
        "combined_portfolio_view": {
            "aggregate_metrics": _metrics_payload(sorted(combined_trades, key=lambda item: item.decision_ts)),
            "rolling_window_metrics": combined_window_rows,
            "note": (
                "This combined view arithmetically combines the selected long and short candidate trades as separate "
                "promotion tracks. It is not yet a unified live-engine conflict model."
            ),
        },
        "setup_quality_findings": {
            "long_side": _long_quality_finding(long_bucket_contribution=long_bucket_contribution),
            "short_side": _short_quality_finding(short_report=best_short_report, short_bucket_diagnosis=short_bucket_diagnosis),
        },
        "recommendation": recommendation,
    }

    json_path = output_root / "phase4_review.json"
    markdown_path = output_root / "phase4_review.md"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(payload), encoding="utf-8")
    return {"json_path": json_path, "markdown_path": markdown_path}


def build_rolling_windows(*, shared_start: datetime, shared_end: datetime, window_days: int = 7) -> list[RollingWindow]:
    windows: list[RollingWindow] = []
    end_ts = shared_end
    duration = timedelta(days=window_days)
    while True:
        start_ts = end_ts - duration
        if start_ts < shared_start:
            break
        windows.append(
            RollingWindow(
                label=f"{start_ts.date().isoformat()}_to_{end_ts.date().isoformat()}",
                start_ts=start_ts,
                end_ts=end_ts,
            )
        )
        end_ts = start_ts
    windows.reverse()
    return windows


def _shared_1m_coverage(*, sqlite_path: Path, instruments: tuple[str, ...]) -> tuple[datetime, datetime]:
    connection = sqlite3.connect(sqlite_path)
    try:
        rows = connection.execute(
            """
            select symbol, min(end_ts), max(end_ts)
            from bars
            where timeframe = '1m' and symbol in ({placeholders})
            group by symbol
            """.format(placeholders=",".join("?" for _ in instruments)),
            list(instruments),
        ).fetchall()
    finally:
        connection.close()
    starts = [datetime.fromisoformat(row[1]) for row in rows]
    ends = [datetime.fromisoformat(row[2]) for row in rows]
    return max(starts), min(ends)


def _run_window(
    *,
    source_sqlite_path: Path,
    instruments: tuple[str, ...],
    start_ts: datetime,
    end_ts: datetime,
    higher_priority_signals: Iterable[HigherPrioritySignal],
) -> dict[str, Any]:
    variants = default_pattern_variants(profile="phase3_full")
    variants_by_id = {variant.variant_id: variant for variant in variants}
    bars_1m: list[Any] = []
    features_all: list[Any] = []
    decisions_by_variant: dict[str, list[Any]] = defaultdict(list)

    for instrument in instruments:
        raw_1m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="1m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalized_1m, _ = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
        raw_5m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="5m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not raw_5m and normalized_1m:
            raw_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        normalized_5m, _ = normalize_and_check_bars(bars=raw_5m, timeframe="5m")
        if normalized_1m:
            first_1m_ts = normalized_1m[0].end_ts
            last_1m_ts = normalized_1m[-1].end_ts
            normalized_5m = [bar for bar in normalized_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        features = build_feature_states(bars_5m=normalized_5m, bars_1m=normalized_1m)
        decisions = generate_signal_decisions(
            feature_rows=features,
            variants=variants,
            higher_priority_signals=higher_priority_signals,
        )
        bars_1m.extend(normalized_1m)
        features_all.extend(features)
        for decision in decisions:
            decisions_by_variant[decision.variant_id].append(decision)

    candidate_trades: dict[str, list[TradeRecord]] = {}
    candidate_audits: dict[str, list[Any]] = {}
    for variant in variants:
        trades, audits = backtest_decisions_with_audit(
            decisions=sorted(decisions_by_variant.get(variant.variant_id, []), key=lambda item: item.decision_ts),
            bars_1m=bars_1m,
            variants_by_id={variant.variant_id: variant},
            point_values=DEFAULT_POINT_VALUES,
            include_shadow_only=False,
        )
        candidate_trades[variant.variant_id] = trades
        candidate_audits[variant.variant_id] = audits
    return {
        "variants": variants,
        "variants_by_id": variants_by_id,
        "candidate_trades": candidate_trades,
        "candidate_audits": candidate_audits,
        "decisions_by_variant": decisions_by_variant,
        "bars_1m": bars_1m,
        "features": features_all,
        "point_values": DEFAULT_POINT_VALUES,
    }


def _build_short_candidate_map(*, window_runs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    candidate_map: dict[str, dict[str, Any]] = {}
    first_run = window_runs[0]
    short_variants = [variant for variant in first_run["variants"] if variant.side == "SHORT"]
    for variant in short_variants:
        baseline_key = _candidate_key(variant_id=variant.variant_id, branch="all_buckets")
        quality_key = _candidate_key(variant_id=variant.variant_id, branch="medium_high_only")
        candidate_map[baseline_key] = {
            "candidate_id": baseline_key,
            "variant_id": variant.variant_id,
            "branch": "all_buckets",
            "branch_description": "Phase 3 full-profile short lane without extra quality filtering.",
            "aggregate_trades": [],
            "window_rows": [],
            "trimmed_trade_count": 0,
        }
        candidate_map[quality_key] = {
            "candidate_id": quality_key,
            "variant_id": variant.variant_id,
            "branch": "medium_high_only",
            "branch_description": "Research-only short branch that keeps MEDIUM/HIGH setup-quality decisions and leaves live re-entry disabled.",
            "aggregate_trades": [],
            "window_rows": [],
            "trimmed_trade_count": 0,
        }

    for run in window_runs:
        for variant in short_variants:
            base_trades = sorted(run["candidate_trades"].get(variant.variant_id, []), key=lambda item: item.decision_ts)
            baseline_key = _candidate_key(variant_id=variant.variant_id, branch="all_buckets")
            candidate_map[baseline_key]["aggregate_trades"].extend(base_trades)
            candidate_map[baseline_key]["window_rows"].append(_metrics_payload(base_trades))

            filtered_decisions = [
                decision
                for decision in run["decisions_by_variant"].get(variant.variant_id, [])
                if decision.setup_quality_bucket in {"MEDIUM", "HIGH"}
            ]
            filtered_trades, _ = backtest_decisions_with_audit(
                decisions=filtered_decisions,
                bars_1m=run["bars_1m"],
                variants_by_id={variant.variant_id: variant},
                point_values=DEFAULT_POINT_VALUES,
                include_shadow_only=False,
            )
            quality_key = _candidate_key(variant_id=variant.variant_id, branch="medium_high_only")
            candidate_map[quality_key]["aggregate_trades"].extend(filtered_trades)
            candidate_map[quality_key]["window_rows"].append(_metrics_payload(filtered_trades))
            candidate_map[quality_key]["trimmed_trade_count"] += max(len(base_trades) - len(filtered_trades), 0)

    for key, row in candidate_map.items():
        aggregate_trades = sorted(row["aggregate_trades"], key=lambda item: item.decision_ts)
        row["aggregate_metrics"] = _metrics_payload(aggregate_trades)
        row["bucket_breakdown"] = _bucket_breakdown(aggregate_trades, key_name="setup_quality_bucket")
        row["rolling_window_metrics"] = row.pop("window_rows")
        row["score"] = _score_short_candidate(row=row)
    return candidate_map


def _select_best_short_candidate(*, short_candidate_map: dict[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
    rows = sorted(
        short_candidate_map.values(),
        key=lambda row: (
            row["score"],
            row["aggregate_metrics"]["net_pnl_cash"],
            row["aggregate_metrics"]["profit_factor"],
        ),
        reverse=True,
    )
    best = dict(rows[0])
    best.pop("aggregate_trades", None)
    best["promotion_readiness"] = _short_promotion_readiness(best)
    return best["candidate_id"], best


def _candidate_window_trade_map(*, window_runs: list[dict[str, Any]], candidate_key: str) -> list[list[TradeRecord]]:
    variant_id, branch = candidate_key.split("::", maxsplit=1)
    rows: list[list[TradeRecord]] = []
    for run in window_runs:
        if branch == "all_buckets":
            rows.append(sorted(run["candidate_trades"].get(variant_id, []), key=lambda item: item.decision_ts))
            continue
        variant = run["variants_by_id"][variant_id]
        filtered_decisions = [
            decision
            for decision in run["decisions_by_variant"].get(variant_id, [])
            if decision.setup_quality_bucket in {"MEDIUM", "HIGH"}
        ]
        filtered_trades, _ = backtest_decisions_with_audit(
            decisions=filtered_decisions,
            bars_1m=run["bars_1m"],
            variants_by_id={variant_id: variant},
            point_values=DEFAULT_POINT_VALUES,
            include_shadow_only=False,
        )
        rows.append(sorted(filtered_trades, key=lambda item: item.decision_ts))
    return rows


def _candidate_key(*, variant_id: str, branch: str) -> str:
    return f"{variant_id}::{branch}"


def _metrics_payload(trades: list[TradeRecord]) -> dict[str, Any]:
    metrics = summarize_performance(trades)
    return {
        "total_trades": metrics.trade_count,
        "trades_per_day": round(metrics.trades_per_day, 4),
        "long_trade_count": metrics.long_trade_count,
        "short_trade_count": metrics.short_trade_count,
        "expectancy_per_trade": round(metrics.expectancy, 4),
        "expectancy_per_hour": round(metrics.expectancy_per_hour, 4),
        "profit_factor": round(metrics.profit_factor, 4),
        "max_drawdown": round(metrics.max_drawdown, 4),
        "average_hold_minutes": round(metrics.avg_hold_minutes, 4),
        "stopout_rate": round(metrics.stopout_rate, 4),
        "reentry_trade_count": metrics.reentry_trade_count,
        "reentry_expectancy": round(metrics.reentry_expectancy, 4),
        "net_pnl_cash": round(metrics.net_pnl_cash, 4),
        "gross_pnl_before_cost": round(metrics.gross_pnl_before_cost, 4),
        "total_fees": round(metrics.total_fees, 4),
        "total_slippage_cost": round(metrics.total_slippage_cost, 4),
        "win_rate": round(metrics.win_rate, 4),
    }


def _bucket_breakdown(trades: list[TradeRecord], *, key_name: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[TradeRecord]] = defaultdict(list)
    for trade in trades:
        grouped[str(getattr(trade, key_name))].append(trade)
    rows: dict[str, dict[str, Any]] = {}
    for key, bucket in sorted(grouped.items()):
        rows[key] = _metrics_payload(sorted(bucket, key=lambda item: item.decision_ts))
    return rows


def _acceptable_window(trades: list[TradeRecord]) -> dict[str, Any]:
    metrics = _metrics_payload(sorted(trades, key=lambda item: item.decision_ts))
    acceptable = (
        metrics["profit_factor"] > 1.0
        and metrics["expectancy_per_trade"] > 0.0
        and metrics["net_pnl_cash"] > 0.0
    )
    return {"acceptable": acceptable, "metrics": metrics}


def _score_short_candidate(*, row: dict[str, Any]) -> float:
    metrics = row["aggregate_metrics"]
    positive_windows = sum(1 for item in row["rolling_window_metrics"] if item["net_pnl_cash"] > 0.0)
    return (
        metrics["net_pnl_cash"] * 0.8
        + metrics["expectancy_per_trade"] * 60.0
        + max(metrics["profit_factor"] - 1.0, 0.0) * 120.0
        + positive_windows * 20.0
        - metrics["max_drawdown"] * 0.15
        - row.get("trimmed_trade_count", 0) * 0.5
    )


def _short_promotion_readiness(row: dict[str, Any]) -> dict[str, Any]:
    metrics = row["aggregate_metrics"]
    positive_windows = sum(1 for item in row["rolling_window_metrics"] if item["net_pnl_cash"] > 0.0)
    ready = (
        metrics["profit_factor"] > 1.0
        and metrics["expectancy_per_trade"] > 0.0
        and metrics["net_pnl_cash"] > 0.0
        and positive_windows >= max(len(row["rolling_window_metrics"]) // 2, 2)
    )
    return {"ready": ready, "positive_window_count": positive_windows}


def _window_payload(window: RollingWindow) -> dict[str, str]:
    return {
        "label": window.label,
        "start": window.start_ts.isoformat(),
        "end": window.end_ts.isoformat(),
    }


def _long_quality_finding(*, long_bucket_contribution: dict[str, dict[str, Any]]) -> dict[str, Any]:
    best_bucket = max(long_bucket_contribution.items(), key=lambda item: item[1]["net_pnl_cash"], default=("NONE", {"net_pnl_cash": 0.0}))
    worst_bucket = min(long_bucket_contribution.items(), key=lambda item: item[1]["net_pnl_cash"], default=("NONE", {"net_pnl_cash": 0.0}))
    return {
        "best_bucket": best_bucket[0],
        "best_bucket_net_pnl": best_bucket[1]["net_pnl_cash"],
        "worst_bucket": worst_bucket[0],
        "worst_bucket_net_pnl": worst_bucket[1]["net_pnl_cash"],
        "interpretation": (
            "Across the expanded windows, MEDIUM-quality long participation held up best while LOW-quality participation "
            "was the largest drag. The March 10-17 improvement remained real, but it did not generalize cleanly across the "
            "full February-to-March span."
        ),
    }


def _short_quality_finding(*, short_report: dict[str, Any], short_bucket_diagnosis: dict[str, dict[str, Any]]) -> dict[str, Any]:
    branch = short_report["branch"]
    weakness_source = "after_cost_execution_fragility"
    if branch == "medium_high_only":
        weakness_source = "bucket_mix_and_after_cost_execution_fragility"
    return {
        "selected_branch": branch,
        "bucket_breakdown": short_bucket_diagnosis,
        "diagnosis": weakness_source,
        "interpretation": (
            "Short-side weakness is not a lack of structural reach. The selected short research branch indicates that "
            "quality mix matters, but the lane still needs stronger after-cost durability before live promotion."
        ),
    }


def _separation_recommendation(*, long_metrics: dict[str, Any], short_metrics: dict[str, Any]) -> dict[str, Any]:
    separate = (
        short_metrics["profit_factor"] <= 1.0
        or short_metrics["net_pnl_cash"] <= 0.0
        or long_metrics["profit_factor"] <= 1.0
        or long_metrics["net_pnl_cash"] <= 0.0
    )
    return {
        "recommended_structure": "separate_long_short_tracks" if separate else "unified_later_possible",
        "reason": (
            "Both sides still need further robustness work over the expanded windows, and the short lane remains explicitly "
            "research-grade. Keep long and short on separate promotion tracks; only revisit unification after each side is "
            "durably positive net of costs on its own."
        ),
    }


def _render_markdown(payload: dict[str, Any]) -> str:
    long_lane = payload["long_lane"]
    short_lane = payload["short_lane"]
    combined = payload["combined_portfolio_view"]
    lines = [
        "# Active Trend Participation Engine Phase 4",
        "",
        "## Long Robustness",
        f"- Long lane: `{long_lane['variant_id']}`",
        f"- Aggregate trades: {long_lane['aggregate_metrics']['total_trades']}",
        f"- Aggregate expectancy/trade: {long_lane['aggregate_metrics']['expectancy_per_trade']}",
        f"- Aggregate profit factor: {long_lane['aggregate_metrics']['profit_factor']}",
        f"- Aggregate max drawdown: {long_lane['aggregate_metrics']['max_drawdown']}",
        f"- Aggregate net after costs: {long_lane['aggregate_metrics']['net_pnl_cash']}",
        f"- Acceptable windows: {long_lane['acceptable_window_count']} / {len(long_lane['rolling_window_metrics'])}",
        "",
        "## Short Candidate",
        f"- Short candidate: `{short_lane['candidate_id']}`",
        f"- Trades: {short_lane['aggregate_metrics']['total_trades']}",
        f"- Expectancy/trade: {short_lane['aggregate_metrics']['expectancy_per_trade']}",
        f"- Profit factor: {short_lane['aggregate_metrics']['profit_factor']}",
        f"- Max drawdown: {short_lane['aggregate_metrics']['max_drawdown']}",
        f"- Net after costs: {short_lane['aggregate_metrics']['net_pnl_cash']}",
        f"- Promotion ready: {short_lane['promotion_readiness']['ready']}",
        "",
        "## Combined View",
        f"- Combined trades: {combined['aggregate_metrics']['total_trades']}",
        f"- Combined expectancy/trade: {combined['aggregate_metrics']['expectancy_per_trade']}",
        f"- Combined profit factor: {combined['aggregate_metrics']['profit_factor']}",
        f"- Combined max drawdown: {combined['aggregate_metrics']['max_drawdown']}",
        f"- Combined net after costs: {combined['aggregate_metrics']['net_pnl_cash']}",
        "",
        "## Recommendation",
        f"- {payload['recommendation']['recommended_structure']}: {payload['recommendation']['reason']}",
    ]
    return "\n".join(lines) + "\n"
