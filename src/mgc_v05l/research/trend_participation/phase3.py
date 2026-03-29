"""Phase 3 review runner for the Active Trend Participation Engine."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from .backtest import backtest_decisions_with_audit, summarize_performance
from .engine import DEFAULT_POINT_VALUES, run_trend_participation_engine
from .features import build_feature_states
from .models import HigherPrioritySignal, TradeRecord
from .patterns import (
    default_pattern_variants,
    feature_matches_side_context,
    generate_signal_decisions,
    variant_matches_feature,
)
from .storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m


EXPERIMENT_PROFILES: tuple[tuple[str, str], ...] = (
    ("phase2_baseline", "Accepted Phase 2 baseline"),
    ("phase3_window_extension", "Longer active 1m trigger window"),
    ("phase3_reclaim_band", "Reclaim-band entry logic"),
    ("phase3_reentry_redesign", "Structural-reset-only re-entry"),
    ("phase3_full", "Short-reach repair plus lighter active throttles"),
)


def run_phase3_review(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MES", "MNQ"),
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
) -> dict[str, Path]:
    output_root = output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    dataset = _load_dataset(
        source_sqlite_path=source_sqlite_path,
        instruments=instruments,
        start_ts=start_ts,
        end_ts=end_ts,
    )
    profile_runs = {
        profile: _run_profile(
            profile=profile,
            dataset=dataset,
            higher_priority_signals=higher_priority_signals,
        )
        for profile, _ in EXPERIMENT_PROFILES
    }
    final_profile = "phase3_full"
    final_run = profile_runs[final_profile]
    gating_change_comparison = _build_gating_change_comparison(profile_runs=profile_runs)
    final_best_variant_id = final_run["best_variant_id"]
    final_best_trades = final_run["trades_by_variant"].get(final_best_variant_id, [])
    qualification_funnel = _build_qualification_funnel(
        features=final_run["features"],
        variants=final_run["variants"],
        audits=final_run["audits"],
    )
    bundle = {
        "module": "Active Trend Participation Engine",
        "review_window": {
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "instruments": list(instruments),
            "start": start_ts.isoformat() if start_ts is not None else None,
            "end": end_ts.isoformat() if end_ts is not None else None,
        },
        "profile_snapshots": {
            profile: {
                "label": label,
                "best_candidate": profile_runs[profile]["best_candidate"],
                "side_trade_counts": profile_runs[profile]["side_trade_counts"],
            }
            for profile, label in EXPERIMENT_PROFILES
        },
        "updated_metrics_bundle": {
            "variant_id": final_best_variant_id,
            "metrics": final_run["best_candidate"]["metrics"],
            "instrument_breakdown": final_run["best_candidate"]["instrument_breakdown"],
            "session_breakdown": final_run["best_candidate"]["session_breakdown"],
            "quality_bucket_breakdown": _bucket_breakdown(final_best_trades, key_name="setup_quality_bucket"),
        },
        "first_entry_vs_reentry": _first_entry_vs_reentry_breakdown(final_best_trades),
        "qualification_funnel": qualification_funnel,
        "gating_change_comparison": gating_change_comparison,
        "top_revisions": _top_revisions(gating_change_comparison),
    }

    persisted_artifacts = run_trend_participation_engine(
        source_sqlite_path=source_sqlite_path,
        output_dir=output_root / "persisted_phase3_full",
        instruments=instruments,
        mode="research",
        start_ts=start_ts,
        end_ts=end_ts,
        higher_priority_signals=higher_priority_signals,
        materialize_storage=True,
        variant_profile=final_profile,
    )
    bundle["persisted_phase3_full"] = {
        "root_dir": str(persisted_artifacts.root_dir),
        "report_json_path": str(persisted_artifacts.report_json_path),
        "report_markdown_path": str(persisted_artifacts.report_markdown_path),
        "storage_manifest_path": str(persisted_artifacts.storage_manifest_path),
    }

    json_path = output_root / "phase3_metrics_bundle.json"
    markdown_path = output_root / "phase3_metrics_bundle.md"
    audit_note_path = output_root / "audit_note.md"
    json_path.write_text(json.dumps(bundle, indent=2, sort_keys=True), encoding="utf-8")
    markdown_path.write_text(_render_markdown(bundle), encoding="utf-8")
    audit_note_path.write_text(_render_audit_note(bundle=bundle), encoding="utf-8")
    return {
        "json_path": json_path,
        "markdown_path": markdown_path,
        "audit_note_path": audit_note_path,
        "persisted_root_dir": persisted_artifacts.root_dir,
    }


def _load_dataset(
    *,
    source_sqlite_path: Path,
    instruments: tuple[str, ...],
    start_ts: datetime | None,
    end_ts: datetime | None,
) -> dict[str, dict[str, Any]]:
    dataset: dict[str, dict[str, Any]] = {}
    for instrument in instruments:
        raw_1m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="1m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        normalized_1m, issues_1m = normalize_and_check_bars(bars=raw_1m, timeframe="1m")
        raw_5m = load_sqlite_bars(
            sqlite_path=source_sqlite_path,
            instrument=instrument,
            timeframe="5m",
            start_ts=start_ts,
            end_ts=end_ts,
        )
        if not raw_5m and normalized_1m:
            raw_5m = resample_bars_from_1m(bars_1m=normalized_1m, target_timeframe="5m")
        normalized_5m, issues_5m = normalize_and_check_bars(bars=raw_5m, timeframe="5m")
        if normalized_1m:
            first_1m_ts = normalized_1m[0].end_ts
            last_1m_ts = normalized_1m[-1].end_ts
            normalized_5m = [bar for bar in normalized_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        dataset[instrument] = {
            "bars_1m": normalized_1m,
            "bars_5m": normalized_5m,
            "features": build_feature_states(bars_5m=normalized_5m, bars_1m=normalized_1m),
            "quality_issue_count": len(issues_1m) + len(issues_5m),
        }
    return dataset


def _run_profile(
    *,
    profile: str,
    dataset: dict[str, dict[str, Any]],
    higher_priority_signals: Iterable[HigherPrioritySignal],
) -> dict[str, Any]:
    variants = default_pattern_variants(profile=profile)
    variants_by_id = {variant.variant_id: variant for variant in variants}
    all_features = []
    all_decisions = []
    all_trades = []
    all_audits = []

    for instrument, instrument_data in dataset.items():
        features = instrument_data["features"]
        decisions = generate_signal_decisions(
            feature_rows=features,
            variants=variants,
            higher_priority_signals=higher_priority_signals,
        )
        trades, audits = backtest_decisions_with_audit(
            decisions=decisions,
            bars_1m=instrument_data["bars_1m"],
            variants_by_id=variants_by_id,
            point_values=DEFAULT_POINT_VALUES,
            include_shadow_only=False,
        )
        all_features.extend(features)
        all_decisions.extend(decisions)
        all_trades.extend(trades)
        all_audits.extend(audits)

    trades_by_variant: dict[str, list[TradeRecord]] = defaultdict(list)
    for trade in all_trades:
        trades_by_variant[trade.variant_id].append(trade)

    variant_rows = []
    for variant in variants:
        metrics = _metrics_payload(trades_by_variant.get(variant.variant_id, []))
        variant_rows.append(
            {
                "variant_id": variant.variant_id,
                "family": variant.family,
                "side": variant.side,
                "strictness": variant.strictness,
                "metrics": metrics,
                "instrument_breakdown": _bucket_breakdown(trades_by_variant.get(variant.variant_id, []), key_name="instrument"),
                "session_breakdown": _bucket_breakdown(trades_by_variant.get(variant.variant_id, []), key_name="session_segment"),
                "score": _score_metrics(metrics),
            }
        )
    variant_rows.sort(key=lambda row: (row["score"], row["metrics"]["net_pnl_cash"], row["metrics"]["total_trades"]), reverse=True)
    best_candidate = variant_rows[0]
    return {
        "profile": profile,
        "variants": variants,
        "features": all_features,
        "decisions": all_decisions,
        "trades": all_trades,
        "trades_by_variant": trades_by_variant,
        "audits": all_audits,
        "variant_rows": variant_rows,
        "best_candidate": best_candidate,
        "best_variant_id": best_candidate["variant_id"],
        "variant_metrics": {row["variant_id"]: row["metrics"] for row in variant_rows},
        "side_trade_counts": {
            "LONG": sum(1 for trade in all_trades if trade.side == "LONG"),
            "SHORT": sum(1 for trade in all_trades if trade.side == "SHORT"),
        },
    }


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


def _score_metrics(metrics: dict[str, Any]) -> float:
    if metrics["total_trades"] <= 0:
        return -999999.0
    return (
        metrics["expectancy_per_hour"] * 0.45
        + metrics["expectancy_per_trade"] * 0.75
        + max(metrics["profit_factor"] - 1.0, 0.0) * 1.2
        + min(metrics["trades_per_day"], 10.0) * 0.12
        - min(metrics["max_drawdown"] / 500.0, 1.5)
        - metrics["stopout_rate"] * 0.4
    )


def _bucket_breakdown(trades: list[TradeRecord], *, key_name: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[TradeRecord]] = defaultdict(list)
    for trade in trades:
        grouped[str(getattr(trade, key_name))].append(trade)
    rows: dict[str, dict[str, Any]] = {}
    for key, bucket in sorted(grouped.items()):
        metrics = summarize_performance(bucket)
        rows[key] = {
            "trade_count": metrics.trade_count,
            "expectancy_per_trade": round(metrics.expectancy, 4),
            "net_pnl_cash": round(metrics.net_pnl_cash, 4),
            "profit_factor": round(metrics.profit_factor, 4),
            "stopout_rate": round(metrics.stopout_rate, 4),
            "trades_per_day": round(metrics.trades_per_day, 4),
        }
    return rows


def _first_entry_vs_reentry_breakdown(trades: list[TradeRecord]) -> dict[str, dict[str, Any]]:
    buckets = {
        "first_entry": [trade for trade in trades if not trade.is_reentry],
        "structural_reset_reentry": [trade for trade in trades if trade.reentry_type == "STRUCTURAL_RESET"],
        "local_churn_reentry": [trade for trade in trades if trade.reentry_type == "LOCAL_CHURN"],
    }
    return {name: _metrics_payload(bucket) for name, bucket in buckets.items()}


def _build_qualification_funnel(
    *,
    features: list,
    variants: list,
    audits: list,
) -> dict[str, Any]:
    by_side: dict[str, dict[str, Any]] = {}
    family_rows: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for side in ("LONG", "SHORT"):
        side_variants = [variant for variant in variants if variant.side == side]
        raw_candidates = 0
        structural_candidates = 0
        family_stats: dict[str, dict[str, int]] = defaultdict(lambda: {"raw_candidates": 0, "structural_candidates": 0})
        for feature in features:
            for variant in side_variants:
                if feature_matches_side_context(side=side, feature=feature):
                    raw_candidates += 1
                    family_stats[variant.family]["raw_candidates"] += 1
                if variant_matches_feature(variant=variant, feature=feature):
                    structural_candidates += 1
                    family_stats[variant.family]["structural_candidates"] += 1

        side_audits = [audit for audit in audits if audit.side == side]
        blocked_cooldown = sum(audit.blocked_cooldown for audit in side_audits)
        blocked_reset = sum(audit.blocked_reset for audit in side_audits)
        blocked_reentry_policy = sum(audit.blocked_reentry_policy for audit in side_audits)
        trigger_survived = sum(audit.trigger_survived for audit in side_audits)
        executed = sum(audit.executed for audit in side_audits)
        by_side[side] = {
            "raw_candidates_detected": raw_candidates,
            "surviving_5m_structural_filter": structural_candidates,
            "surviving_1m_trigger_filter": trigger_survived,
            "blocked_by_reset_cooldown_logic": blocked_cooldown + blocked_reset + blocked_reentry_policy,
            "executed": executed,
            "blocked_components": {
                "cooldown": blocked_cooldown,
                "setup_reset_gate": blocked_reset,
                "reentry_policy": blocked_reentry_policy,
            },
            "structural_survival_rate": round(structural_candidates / raw_candidates, 4) if raw_candidates else 0.0,
            "trigger_reachability_rate": round(trigger_survived / structural_candidates, 4) if structural_candidates else 0.0,
            "execution_rate": round(executed / structural_candidates, 4) if structural_candidates else 0.0,
        }
        for family, counts in family_stats.items():
            family_audits = [audit for audit in side_audits if audit.family == family]
            family_rows[side][family] = {
                "raw_candidates": counts["raw_candidates"],
                "structural_candidates": counts["structural_candidates"],
                "trigger_survived": sum(audit.trigger_survived for audit in family_audits),
                "blocked": (
                    sum(audit.blocked_cooldown for audit in family_audits)
                    + sum(audit.blocked_reset for audit in family_audits)
                    + sum(audit.blocked_reentry_policy for audit in family_audits)
                ),
                "executed": sum(audit.executed for audit in family_audits),
            }
    return {"by_side": by_side, "by_family": family_rows}


def _build_gating_change_comparison(*, profile_runs: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    comparisons: list[dict[str, Any]] = []
    for index in range(1, len(EXPERIMENT_PROFILES)):
        before_profile = EXPERIMENT_PROFILES[index - 1][0]
        after_profile = EXPERIMENT_PROFILES[index][0]
        after_variant_id = _focus_variant_id(profile=after_profile, run=profile_runs[after_profile])
        before_metrics = profile_runs[before_profile]["variant_metrics"].get(after_variant_id, _metrics_payload([]))
        after_metrics = profile_runs[after_profile]["variant_metrics"][after_variant_id]
        comparisons.append(
            {
                "change": f"{before_profile} -> {after_profile}",
                "variant_id": after_variant_id,
                "before": before_metrics,
                "after": after_metrics,
                "delta": {
                    "trade_count": after_metrics["total_trades"] - before_metrics["total_trades"],
                    "expectancy_per_trade": round(after_metrics["expectancy_per_trade"] - before_metrics["expectancy_per_trade"], 4),
                    "net_pnl_cash": round(after_metrics["net_pnl_cash"] - before_metrics["net_pnl_cash"], 4),
                    "max_drawdown": round(after_metrics["max_drawdown"] - before_metrics["max_drawdown"], 4),
                },
            }
        )
    return comparisons


def _focus_variant_id(*, profile: str, run: dict[str, Any]) -> str:
    rows = run["variant_rows"]
    if profile == "phase3_window_extension":
        for row in rows:
            if row["strictness"] == "active":
                return row["variant_id"]
    if profile == "phase3_full":
        for row in rows:
            if row["side"] == "SHORT" and row["strictness"] in {"active", "aggressive"}:
                return row["variant_id"]
    return run["best_variant_id"]


def _top_revisions(comparisons: list[dict[str, Any]]) -> list[dict[str, Any]]:
    qualifying = [
        row
        for row in comparisons
        if row["delta"]["net_pnl_cash"] > 0.0
        and row["after"]["total_trades"] >= max(row["before"]["total_trades"] * 0.75, row["before"]["total_trades"] - 15)
    ]
    qualifying.sort(
        key=lambda row: (
            row["delta"]["net_pnl_cash"],
            row["delta"]["expectancy_per_trade"],
            row["delta"]["trade_count"],
        ),
        reverse=True,
    )
    return qualifying[:3]


def _render_markdown(bundle: dict[str, Any]) -> str:
    best = bundle["updated_metrics_bundle"]
    lines = [
        "# Active Trend Participation Engine Phase 3",
        "",
        f"Current best candidate: `{best['variant_id']}`",
        "",
        "## Updated Metrics",
        f"- Total trades: {best['metrics']['total_trades']}",
        f"- Trades/day: {best['metrics']['trades_per_day']}",
        f"- Long vs short: {best['metrics']['long_trade_count']} / {best['metrics']['short_trade_count']}",
        f"- Expectancy/trade: {best['metrics']['expectancy_per_trade']}",
        f"- Profit factor: {best['metrics']['profit_factor']}",
        f"- Max drawdown: {best['metrics']['max_drawdown']}",
        f"- Average hold minutes: {best['metrics']['average_hold_minutes']}",
        f"- Stopout rate: {best['metrics']['stopout_rate']}",
        f"- Net after costs: {best['metrics']['net_pnl_cash']}",
        "",
        "## First Entry Vs Re-entry",
    ]
    for name, metrics in bundle["first_entry_vs_reentry"].items():
        lines.append(
            f"- {name}: trades={metrics['total_trades']}, expectancy={metrics['expectancy_per_trade']}, net={metrics['net_pnl_cash']}"
        )
    lines.extend(
        [
            "",
            "## Qualification Funnel",
        ]
    )
    for side, row in bundle["qualification_funnel"]["by_side"].items():
        lines.append(
            f"- {side}: raw={row['raw_candidates_detected']}, structural={row['surviving_5m_structural_filter']}, "
            f"trigger={row['surviving_1m_trigger_filter']}, blocked={row['blocked_by_reset_cooldown_logic']}, executed={row['executed']}"
        )
    lines.extend(
        [
            "",
            "## Gating Changes",
        ]
    )
    for row in bundle["gating_change_comparison"]:
        lines.append(
            f"- {row['change']} on `{row['variant_id']}`: delta_trades={row['delta']['trade_count']}, "
            f"delta_expectancy={row['delta']['expectancy_per_trade']}, delta_net={row['delta']['net_pnl_cash']}, "
            f"delta_drawdown={row['delta']['max_drawdown']}"
        )
    return "\n".join(lines) + "\n"


def _render_audit_note(*, bundle: dict[str, Any]) -> str:
    funnel = bundle["qualification_funnel"]["by_side"]
    lines = [
        "# Phase 3 Audit Note",
        "",
        "Current Phase 3 implementation changes:",
        "- Active variants use longer 1m trigger windows to reduce slow-valid entry drop-off.",
        "- Reclaim-band trigger logic replaces exactish 5m extreme requirements where appropriate.",
        "- Re-entry is split into first entry, healthy structural reset re-entry, and local churn re-entry.",
        "- Local churn re-entry is suppressed by policy in the full Phase 3 profile.",
        "- Setup quality is bucketed as HIGH, MEDIUM, or LOW and reported after measurement instead of being over-filtered up front.",
        "- Short active and aggressive variants receive extra trigger reachability room in the full profile.",
        "",
        "Interpretation notes:",
        "- Conservative 1m execution still waits for post-decision 1m bars, applies slippage and fees, and resolves stop/target conflicts stop-first.",
        "- Trigger survival and executed counts match in the current engine because reset/cooldown/re-entry gates are applied before entry simulation.",
        "",
        "Long vs short qualification snapshot:",
        f"- LONG trigger reachability: {funnel['LONG']['trigger_reachability_rate']}",
        f"- SHORT trigger reachability: {funnel['SHORT']['trigger_reachability_rate']}",
        f"- LONG structural survival: {funnel['LONG']['structural_survival_rate']}",
        f"- SHORT structural survival: {funnel['SHORT']['structural_survival_rate']}",
    ]
    return "\n".join(lines) + "\n"
