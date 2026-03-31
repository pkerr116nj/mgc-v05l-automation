"""Broader ATP performance sampling and segmentation study."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import Any, Iterable, Sequence

from .engine import DEFAULT_POINT_VALUES
from .features import build_feature_states
from .models import HigherPrioritySignal
from .performance_validation import (
    build_atp_performance_validation_report,
    enrich_atp_trades,
    render_atp_performance_validation_markdown,
    segment_trade_metrics,
    summarize_trade_distribution_diagnostics,
)
from .phase2_continuation import build_phase2_replay_package
from .phase3_timing import build_phase3_replay_package
from .phase4 import RollingWindow, build_rolling_windows
from .state_layers import summarize_atp_state_diagnostics
from .storage import load_sqlite_bars, normalize_and_check_bars, resample_bars_from_1m


def run_broader_sample_performance_validation(
    *,
    source_sqlite_path: Path,
    output_dir: Path,
    instruments: tuple[str, ...] = ("MGC",),
    higher_priority_signals: Iterable[HigherPrioritySignal] = (),
    window_days: int = 1,
    max_windows: int | None = None,
) -> dict[str, Path]:
    output_root = output_dir.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    shared_start, shared_end = _shared_1m_coverage(sqlite_path=source_sqlite_path, instruments=instruments)
    windows = build_rolling_windows(shared_start=shared_start, shared_end=shared_end, window_days=window_days)
    if max_windows is not None:
        windows = windows[-max_windows:]

    sampled_runs = []
    all_entry_states = []
    all_timing_states = []
    all_atp_trades = []
    all_legacy_trades = []
    total_bars = 0

    for window in windows:
        run_payload = _run_window_sample(
            source_sqlite_path=source_sqlite_path,
            instruments=instruments,
            start_ts=window.start_ts,
            end_ts=window.end_ts,
            higher_priority_signals=higher_priority_signals,
        )
        if run_payload["bar_count"] <= 0:
            continue
        sampled_runs.append(run_payload["summary_row"])
        total_bars += run_payload["bar_count"]
        all_entry_states.extend(run_payload["entry_states"])
        all_timing_states.extend(run_payload["timing_states"])
        all_atp_trades.extend(run_payload["atp_trades"])
        all_legacy_trades.extend(run_payload["legacy_trades"])

    combined_validation = build_atp_performance_validation_report(
        bar_count=total_bars,
        entry_states=all_entry_states,
        timing_states=all_timing_states,
        atp_trades=all_atp_trades,
        legacy_proxy_trades=all_legacy_trades,
    )
    combined_enriched_trades = annotate_trade_sequence_features(
        enrich_atp_trades(
            trades=all_atp_trades,
            entry_states=all_entry_states,
            timing_states=all_timing_states,
        )
    )
    cross_run_summary = build_cross_run_summary(
        run_rows=sampled_runs,
        combined_validation=combined_validation,
        combined_enriched_trades=combined_enriched_trades,
        total_bars=total_bars,
    )

    broader_payload = {
        "module": "Active Trend Participation Engine",
        "study": "broader_sample_performance_validation",
        "methodology": {
            "source_sqlite_path": str(source_sqlite_path.resolve()),
            "selection_policy": "systematic_full_coverage_rolling_windows",
            "window_days": window_days,
            "run_count": len(sampled_runs),
            "instruments": list(instruments),
            "window_labels": [row["label"] for row in sampled_runs],
        },
        "sampled_runs": sampled_runs,
        "coverage_summary": {
            "run_count": len(sampled_runs),
            "total_bars_processed": total_bars,
            "tape_direction_tags": _counter_rows(Counter(row["tags"]["tape_direction_tag"] for row in sampled_runs), len(sampled_runs), "tag"),
            "regime_tags": _counter_rows(Counter(row["tags"]["regime_tag"] for row in sampled_runs), len(sampled_runs), "tag"),
            "dominant_session_tags": _counter_rows(Counter(row["tags"]["dominant_session_tag"] for row in sampled_runs), len(sampled_runs), "tag"),
        },
        "aggregated": cross_run_summary,
    }

    broader_json_path = output_root / "broader_sample_performance_validation.json"
    broader_markdown_path = output_root / "broader_sample_performance_validation.md"
    cross_run_json_path = output_root / "cross_run_summary.json"
    cross_run_markdown_path = output_root / "cross_run_summary.md"
    broader_json_path.write_text(json.dumps(broader_payload, indent=2, sort_keys=True), encoding="utf-8")
    broader_markdown_path.write_text(_render_broader_sample_markdown(broader_payload), encoding="utf-8")
    cross_run_json_path.write_text(json.dumps(cross_run_summary, indent=2, sort_keys=True), encoding="utf-8")
    cross_run_markdown_path.write_text(_render_cross_run_markdown(cross_run_summary), encoding="utf-8")
    return {
        "broader_json_path": broader_json_path,
        "broader_markdown_path": broader_markdown_path,
        "cross_run_json_path": cross_run_json_path,
        "cross_run_markdown_path": cross_run_markdown_path,
    }


def build_cross_run_summary(
    *,
    run_rows: Sequence[dict[str, Any]],
    combined_validation: dict[str, Any],
    combined_enriched_trades: Sequence[dict[str, Any]],
    total_bars: int,
) -> dict[str, Any]:
    sequence_rows = annotate_trade_sequence_features(combined_enriched_trades)
    clustering_segments = {
        "by_trade_sequence_position": segment_trade_metrics(
            sequence_rows,
            key_name="trade_sequence_position",
            bar_count=total_bars,
        ),
        "by_session_bucket_density": segment_trade_metrics(
            sequence_rows,
            key_name="session_bucket_density",
            bar_count=total_bars,
        ),
        "by_prior_loss_streak_bucket": segment_trade_metrics(
            sequence_rows,
            key_name="prior_loss_streak_bucket",
            bar_count=total_bars,
        ),
    }
    return {
        "run_count": len(run_rows),
        "run_rows": list(run_rows),
        "aggregated_totals": combined_validation["atp_phase3_performance"],
        "legacy_comparison": combined_validation["same_window_comparison"],
        "run_medians": _run_medians(run_rows),
        "segment_breakdowns": {
            **combined_validation["segment_breakdowns"],
            **clustering_segments,
        },
        "trade_distribution_diagnostics": summarize_trade_distribution_diagnostics(sequence_rows),
        "hypothesis_checks": {
            "vwap_neutral_vs_favorable": _hypothesis_vwap_neutral(combined_validation),
            "session_strength": _hypothesis_sessions(combined_validation),
            "clustering_impact": _hypothesis_clustering(clustering_segments),
        },
    }


def annotate_trade_sequence_features(trades: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted((dict(trade) for trade in trades), key=lambda trade: trade["entry_ts"])
    buckets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for trade in ordered:
        session_key = (trade["decision_ts"].date().isoformat(), str(trade.get("session_segment") or "UNKNOWN"))
        buckets[session_key].append(trade)
    for bucket in buckets.values():
        bucket_size = len(bucket)
        for index, trade in enumerate(bucket, start=1):
            trade["session_bucket_trade_index"] = index
            trade["session_bucket_trade_count"] = bucket_size
            trade["trade_sequence_position"] = "FIRST_IN_SESSION_BUCKET" if index == 1 else "LATER_IN_SESSION_BUCKET"
            trade["session_bucket_density"] = (
                "ISOLATED"
                if bucket_size == 1
                else "PAIR"
                if bucket_size == 2
                else "CLUSTER_3_PLUS"
            )

    loss_streak = 0
    previous_entry_ts = None
    for trade in ordered:
        trade["prior_loss_streak"] = loss_streak
        trade["prior_loss_streak_bucket"] = (
            "AFTER_0_LOSSES"
            if loss_streak == 0
            else "AFTER_1_LOSS"
            if loss_streak == 1
            else "AFTER_2_PLUS_LOSSES"
        )
        if previous_entry_ts is None:
            trade["time_since_prior_trade_minutes"] = None
        else:
            trade["time_since_prior_trade_minutes"] = round(
                (trade["entry_ts"] - previous_entry_ts).total_seconds() / 60.0,
                4,
            )
        previous_entry_ts = trade["entry_ts"]
        loss_streak = loss_streak + 1 if float(trade["pnl_cash"]) < 0.0 else 0
    return ordered


def _run_window_sample(
    *,
    source_sqlite_path: Path,
    instruments: tuple[str, ...],
    start_ts: datetime,
    end_ts: datetime,
    higher_priority_signals: Iterable[HigherPrioritySignal],
) -> dict[str, Any]:
    all_feature_rows = []
    all_entry_states = []
    all_timing_states = []
    all_atp_trades = []
    all_legacy_trades = []

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
        feature_rows = build_feature_states(bars_5m=normalized_5m, bars_1m=normalized_1m)
        phase2 = build_phase2_replay_package(
            feature_rows=feature_rows,
            bars_1m=normalized_1m,
            higher_priority_signals=higher_priority_signals,
            point_value=DEFAULT_POINT_VALUES.get(instrument, 5.0),
        )
        phase3 = build_phase3_replay_package(
            entry_states=phase2["entry_states"],
            bars_1m=normalized_1m,
            point_value=DEFAULT_POINT_VALUES.get(instrument, 5.0),
            old_proxy_trade_count=len(phase2["shadow_trades"]),
        )
        all_feature_rows.extend(feature_rows)
        all_entry_states.extend(phase2["entry_states"])
        all_timing_states.extend(phase3["timing_states"])
        all_atp_trades.extend(phase3["shadow_trades"])
        all_legacy_trades.extend(phase2["shadow_trades"])

    performance_validation = build_atp_performance_validation_report(
        bar_count=len(all_feature_rows),
        entry_states=all_entry_states,
        timing_states=all_timing_states,
        atp_trades=all_atp_trades,
        legacy_proxy_trades=all_legacy_trades,
    )
    phase1_summary = summarize_atp_state_diagnostics(all_feature_rows)
    session_mix = _segment_mix(
        performance_validation["segment_breakdowns"]["by_session_segment"],
        total_key="total_trades",
    )
    vwap_mix = _segment_mix(
        performance_validation["segment_breakdowns"]["by_vwap_price_quality_state"],
        total_key="total_trades",
    )
    tags = _window_tags(feature_rows=all_feature_rows, phase1_summary=phase1_summary, session_mix=session_mix)
    return {
        "bar_count": len(all_feature_rows),
        "entry_states": all_entry_states,
        "timing_states": all_timing_states,
        "atp_trades": all_atp_trades,
        "legacy_trades": all_legacy_trades,
        "summary_row": {
            "label": f"{start_ts.date().isoformat()}_to_{end_ts.date().isoformat()}",
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "bars_processed": len(all_feature_rows),
            "tags": tags,
            "atp_phase3_performance": performance_validation["atp_phase3_performance"],
            "legacy_comparison": performance_validation["same_window_comparison"],
            "average_mfe_points": performance_validation["atp_phase3_performance"]["average_favorable_excursion_points"],
            "average_mae_points": performance_validation["atp_phase3_performance"]["average_adverse_excursion_points"],
            "session_mix": session_mix,
            "vwap_quality_mix": vwap_mix,
        },
    }


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


def _run_medians(run_rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    if not run_rows:
        return {}
    keys = (
        "bars_processed",
        "trade_count",
        "net_pnl_cash",
        "profit_factor",
        "win_rate",
        "average_trade_pnl_cash",
        "max_drawdown",
        "entries_per_100_bars",
        "average_mfe_points",
        "average_mae_points",
    )
    mapped = []
    for row in run_rows:
        metrics = row["atp_phase3_performance"]
        mapped.append(
            {
                "bars_processed": row["bars_processed"],
                "trade_count": metrics["total_trades"],
                "net_pnl_cash": metrics["net_pnl_cash"],
                "profit_factor": metrics["profit_factor"],
                "win_rate": metrics["win_rate"],
                "average_trade_pnl_cash": metrics["average_trade_pnl_cash"],
                "max_drawdown": metrics["max_drawdown"],
                "entries_per_100_bars": metrics["entries_per_100_bars"],
                "average_mfe_points": metrics["average_favorable_excursion_points"],
                "average_mae_points": metrics["average_adverse_excursion_points"],
            }
        )
    return {key: round(median([row[key] for row in mapped]), 4) for key in keys}


def _segment_mix(rows: Sequence[dict[str, Any]], *, total_key: str) -> list[dict[str, Any]]:
    total = sum(int(row.get(total_key) or 0) for row in rows)
    return [
        {
            "segment": row["segment"],
            "count": int(row.get(total_key) or 0),
            "percent": round(((int(row.get(total_key) or 0) / total) * 100.0), 4) if total > 0 else 0.0,
        }
        for row in rows
    ]


def _window_tags(
    *,
    feature_rows: Sequence[Any],
    phase1_summary: dict[str, Any],
    session_mix: Sequence[dict[str, Any]],
) -> dict[str, str]:
    if not feature_rows:
        return {
            "tape_direction_tag": "NO_DATA",
            "regime_tag": "NO_DATA",
            "dominant_session_tag": "NO_DATA",
        }
    first_close = float(feature_rows[0].close)
    last_close = float(feature_rows[-1].close)
    average_range = sum(float(row.average_range) for row in feature_rows) / max(len(feature_rows), 1)
    normalized_move = (last_close - first_close) / max(average_range, 1e-9)
    if normalized_move >= 6.0:
        tape_direction = "UP_TAPE"
    elif normalized_move <= -6.0:
        tape_direction = "DOWN_TAPE"
    else:
        tape_direction = "MIXED_TAPE"
    neutral_pct = float((phase1_summary.get("bias_state_percent") or {}).get("NEUTRAL", 0.0))
    long_pct = float((phase1_summary.get("bias_state_percent") or {}).get("LONG_BIAS", 0.0))
    short_pct = float((phase1_summary.get("bias_state_percent") or {}).get("SHORT_BIAS", 0.0))
    if neutral_pct >= 45.0:
        regime_tag = "CHOP_HEAVY"
    elif max(long_pct, short_pct) >= 45.0:
        regime_tag = "TREND_HEAVY"
    else:
        regime_tag = "MIXED_REGIME"
    dominant_session = max(session_mix, key=lambda row: (row["count"], row["segment"]), default={"segment": "NO_TRADES", "count": 0, "percent": 0.0})
    dominant_session_tag = dominant_session["segment"] if float(dominant_session.get("percent", 0.0)) >= 45.0 else "MIXED_SESSION"
    return {
        "tape_direction_tag": tape_direction,
        "regime_tag": regime_tag,
        "dominant_session_tag": dominant_session_tag,
    }


def _hypothesis_vwap_neutral(combined_validation: dict[str, Any]) -> dict[str, Any]:
    rows = {
        row["segment"]: row
        for row in combined_validation["segment_breakdowns"]["by_vwap_price_quality_state"]
    }
    favorable = rows.get("VWAP_FAVORABLE", {})
    neutral = rows.get("VWAP_NEUTRAL", {})
    return {
        "favorable_trade_count": favorable.get("total_trades", 0),
        "neutral_trade_count": neutral.get("total_trades", 0),
        "favorable_net_pnl_cash": favorable.get("net_pnl_cash", 0.0),
        "neutral_net_pnl_cash": neutral.get("net_pnl_cash", 0.0),
        "favorable_profit_factor": favorable.get("profit_factor", 0.0),
        "neutral_profit_factor": neutral.get("profit_factor", 0.0),
        "favorable_avg_trade": favorable.get("average_trade_pnl_cash", 0.0),
        "neutral_avg_trade": neutral.get("average_trade_pnl_cash", 0.0),
        "neutral_is_quality_leak": (
            favorable.get("net_pnl_cash", 0.0) > 0.0
            and neutral.get("net_pnl_cash", 0.0) < 0.0
            and favorable.get("profit_factor", 0.0) > neutral.get("profit_factor", 0.0)
        ),
    }


def _hypothesis_sessions(combined_validation: dict[str, Any]) -> dict[str, Any]:
    rows = {
        row["segment"]: row
        for row in combined_validation["segment_breakdowns"]["by_session_segment"]
    }
    asia = rows.get("ASIA", {})
    london = rows.get("LONDON", {})
    us = rows.get("US", {})
    london_is_weak = (
        london.get("net_pnl_cash", 0.0) < 0.0
        and london.get("profit_factor", 0.0) < 1.0
        and (
            asia.get("net_pnl_cash", 0.0) > 0.0
            or us.get("net_pnl_cash", 0.0) > 0.0
        )
    )
    return {
        "ASIA": asia,
        "LONDON": london,
        "US": us,
        "london_is_structurally_weaker": london_is_weak,
    }


def _hypothesis_clustering(clustering_segments: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    first_later = {
        row["segment"]: row
        for row in clustering_segments["by_trade_sequence_position"]
    }
    density = {
        row["segment"]: row
        for row in clustering_segments["by_session_bucket_density"]
    }
    first = first_later.get("FIRST_IN_SESSION_BUCKET", {})
    later = first_later.get("LATER_IN_SESSION_BUCKET", {})
    isolated = density.get("ISOLATED", {})
    cluster_3_plus = density.get("CLUSTER_3_PLUS", {})
    return {
        "first_trade_metrics": first,
        "later_trade_metrics": later,
        "isolated_trade_metrics": isolated,
        "cluster_3_plus_metrics": cluster_3_plus,
        "cluster_harm_detected": (
            later.get("average_trade_pnl_cash", 0.0) < first.get("average_trade_pnl_cash", 0.0)
            or cluster_3_plus.get("average_trade_pnl_cash", 0.0) < isolated.get("average_trade_pnl_cash", 0.0)
        ),
    }


def _counter_rows(counter: Counter[str], total: int, key_name: str) -> list[dict[str, Any]]:
    return [
        {
            key_name: key,
            "count": count,
            "percent": round((count / total) * 100.0, 4) if total > 0 else 0.0,
        }
        for key, count in counter.most_common()
    ]


def _render_broader_sample_markdown(payload: dict[str, Any]) -> str:
    methodology = payload["methodology"]
    aggregated = payload["aggregated"]
    lines = [
        "# ATP Broader Sample Performance Validation",
        "",
        f"- Source: `{methodology['source_sqlite_path']}`",
        f"- Selection policy: `{methodology['selection_policy']}`",
        f"- Window days: `{methodology['window_days']}`",
        f"- Run count: `{methodology['run_count']}`",
        "",
        "## Aggregated Totals",
    ]
    lines.extend(render_atp_performance_validation_markdown({"atp_phase3_performance": aggregated["aggregated_totals"], "same_window_comparison": aggregated["legacy_comparison"], "segment_breakdowns": aggregated["segment_breakdowns"], "near_miss_breakdown": {"top_blockers": []}}).splitlines())
    lines.extend(["", "## Coverage Tags"])
    for bucket_name in ("tape_direction_tags", "regime_tags", "dominant_session_tags"):
        lines.append(f"- {bucket_name}:")
        for row in payload["coverage_summary"][bucket_name]:
            lines.append(f"  - {row.get('tag')}: count={row['count']} percent={row['percent']}")
    return "\n".join(lines) + "\n"


def _render_cross_run_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Cross-Run Summary",
        "",
        f"- Run count: `{summary['run_count']}`",
        f"- Median net P/L per run: `{summary['run_medians'].get('net_pnl_cash')}`",
        f"- Median PF per run: `{summary['run_medians'].get('profit_factor')}`",
        f"- Median win rate per run: `{summary['run_medians'].get('win_rate')}`",
        "",
        "## Hypotheses",
        f"- VWAP neutral quality leak: `{summary['hypothesis_checks']['vwap_neutral_vs_favorable']['neutral_is_quality_leak']}`",
        f"- London structurally weaker: `{summary['hypothesis_checks']['session_strength']['london_is_structurally_weaker']}`",
        f"- Clustering harm detected: `{summary['hypothesis_checks']['clustering_impact']['cluster_harm_detected']}`",
        "",
        "## Runs",
    ]
    for row in summary["run_rows"]:
        metrics = row["atp_phase3_performance"]
        lines.append(
            f"- `{row['label']}` bars={row['bars_processed']} trades={metrics['total_trades']} "
            f"net={metrics['net_pnl_cash']} pf={metrics['profit_factor']} win={metrics['win_rate']} "
            f"avg_trade={metrics['average_trade_pnl_cash']} dd={metrics['max_drawdown']} e100={metrics['entries_per_100_bars']}"
        )
    return "\n".join(lines) + "\n"
