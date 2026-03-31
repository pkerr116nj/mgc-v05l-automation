"""Rolling live-paper observation report for ATPE temporary paper lanes."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "atpe_live_observation"
DEFAULT_LANES_ROOT = REPO_ROOT / "outputs" / "probationary_quant_canaries" / "active_trend_participation_engine" / "lanes"
DEFAULT_PAPER_SESSION_STATUS_PATH = REPO_ROOT / "outputs" / "probationary_pattern_engine" / "paper_session" / "operator_status.json"
DEFAULT_STRATEGY_PERFORMANCE_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_strategy_performance_snapshot.json"
DEFAULT_TRADE_LOG_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_strategy_trade_log_snapshot.json"
DEFAULT_TEMP_PAPER_INTEGRITY_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_temporary_paper_runtime_integrity_snapshot.json"
DEFAULT_GC_MGC_OBSERVATION_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "gc_mgc_london_open_acceptance_live_observation"
    / "gc_mgc_london_open_acceptance_live_observation.json"
)
ATPE_LANE_IDS = (
    "atpe_long_medium_high_canary__MES",
    "atpe_long_medium_high_canary__MNQ",
    "atpe_short_high_only_canary__MES",
    "atpe_short_high_only_canary__MNQ",
)


@dataclass(frozen=True)
class ObservationArtifacts:
    json_path: Path
    markdown_path: Path


def run_atpe_live_observation(
    *,
    output_dir: str | Path | None = None,
    lanes_root_path: str | Path | None = None,
    paper_session_status_path: str | Path | None = None,
    strategy_performance_snapshot_path: str | Path | None = None,
    trade_log_snapshot_path: str | Path | None = None,
    temp_paper_integrity_snapshot_path: str | Path | None = None,
    gc_mgc_observation_path: str | Path | None = None,
) -> ObservationArtifacts:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)
    report = build_atpe_live_observation_report(
        lanes_root_path=Path(lanes_root_path or DEFAULT_LANES_ROOT).resolve(),
        paper_session_status_path=Path(paper_session_status_path or DEFAULT_PAPER_SESSION_STATUS_PATH).resolve(),
        strategy_performance_snapshot_path=Path(strategy_performance_snapshot_path or DEFAULT_STRATEGY_PERFORMANCE_PATH).resolve(),
        trade_log_snapshot_path=Path(trade_log_snapshot_path or DEFAULT_TRADE_LOG_PATH).resolve(),
        temp_paper_integrity_snapshot_path=Path(temp_paper_integrity_snapshot_path or DEFAULT_TEMP_PAPER_INTEGRITY_PATH).resolve(),
        gc_mgc_observation_path=Path(gc_mgc_observation_path or DEFAULT_GC_MGC_OBSERVATION_PATH).resolve(),
    )
    archive_snapshot_path = _archive_snapshot(report, resolved_output_dir)
    archived_reports = _load_archived_reports(resolved_output_dir / "snapshots")
    report["archive_snapshot_path"] = str(archive_snapshot_path)
    report["rolling_observation_summary"] = _build_rolling_observation_summary(archived_reports)
    report["promotion_readiness_checklist"] = _build_promotion_readiness_checklist(report)

    json_path = resolved_output_dir / "atpe_live_observation.json"
    markdown_path = resolved_output_dir / "atpe_live_observation.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return ObservationArtifacts(json_path=json_path, markdown_path=markdown_path)


def build_atpe_live_observation_report(
    *,
    lanes_root_path: Path,
    paper_session_status_path: Path,
    strategy_performance_snapshot_path: Path,
    trade_log_snapshot_path: Path,
    temp_paper_integrity_snapshot_path: Path,
    gc_mgc_observation_path: Path,
) -> dict[str, Any]:
    strategy_performance_rows = _rows_by_lane(_read_json(strategy_performance_snapshot_path))
    trade_log_rows = _group_trade_log_rows(_read_json(trade_log_snapshot_path))
    integrity_snapshot = _read_json(temp_paper_integrity_snapshot_path)
    paper_session_status = _read_json(paper_session_status_path)
    gc_mgc_report = _read_json(gc_mgc_observation_path)

    lane_reports: list[dict[str, Any]] = []
    all_trades: list[dict[str, Any]] = []
    total_signals = 0
    total_intents = 0
    total_fills = 0
    total_fill_gaps = 0
    latest_activity_values: list[str] = []
    long_checkpoint_continue_events = 0
    long_checkpoint_exit_trade_count = 0
    long_checkpoint_negative_exit_count = 0
    long_checkpoint_positive_exit_count = 0
    long_checkpoint_exit_pnls: list[float] = []

    for lane_id in ATPE_LANE_IDS:
        lane_dir = lanes_root_path / lane_id
        operator_status = _read_json(lane_dir / "operator_status.json")
        order_intents = _read_jsonl(lane_dir / "order_intents.jsonl")
        fills = _read_jsonl(lane_dir / "fills.jsonl")
        trades = _read_jsonl(lane_dir / "trades.jsonl")
        signals = _read_jsonl(lane_dir / "signals.jsonl")
        events = _read_jsonl(lane_dir / "events.jsonl")
        strategy_row = strategy_performance_rows.get(lane_id, {})
        app_trade_rows = trade_log_rows.get(lane_id, [])

        trade_metrics = _trade_metrics(trades)
        all_trades.extend(trades)
        total_signals += len(signals)
        total_intents += len(order_intents)
        total_fills += len(fills)
        total_fill_gaps += max(len(order_intents) - len(fills), 0)
        checkpoint_continue_events = [
            row for row in events if str(row.get("event_type") or "") == "ATPE_TARGET_CHECKPOINT_CONTINUE"
        ]
        checkpoint_exit_trades = [
            trade
            for trade in trades
            if "long_medium_high" in lane_id
            and str(trade.get("exit_reason") or "") in {"atpe_checkpoint_stop", "atpe_target_momentum_fade", "atpe_time_stop"}
        ]
        long_checkpoint_continue_events += len(checkpoint_continue_events)
        long_checkpoint_exit_trade_count += len(checkpoint_exit_trades)
        checkpoint_exit_pnls = [float(trade.get("realized_pnl") or 0.0) for trade in checkpoint_exit_trades]
        long_checkpoint_exit_pnls.extend(checkpoint_exit_pnls)
        long_checkpoint_negative_exit_count += sum(1 for value in checkpoint_exit_pnls if value < 0)
        long_checkpoint_positive_exit_count += sum(1 for value in checkpoint_exit_pnls if value > 0)
        if strategy_row.get("latest_activity_timestamp"):
            latest_activity_values.append(str(strategy_row["latest_activity_timestamp"]))

        lane_reports.append(
            {
                "lane_id": lane_id,
                "lane_name": operator_status.get("lane_name") or strategy_row.get("display_name") or lane_id,
                "side": operator_status.get("side"),
                "instrument": lane_id.rsplit("__", 1)[-1],
                "quality_bucket_policy": operator_status.get("quality_bucket_policy"),
                "runtime_loaded": bool(operator_status),
                "latest_processed_bar_timestamp": operator_status.get("last_processed_bar_end_ts"),
                "latest_signal_timestamp": _latest_timestamp(signals, "signal_timestamp"),
                "latest_intent_timestamp": _latest_timestamp(order_intents, "created_at"),
                "latest_fill_timestamp": _latest_timestamp(fills, "fill_timestamp"),
                "latest_trade_timestamp": _latest_timestamp(trades, "exit_timestamp"),
                "latest_operator_status_timestamp": operator_status.get("generated_at"),
                "signal_count": len(signals),
                "intent_count": len(order_intents),
                "fill_count": len(fills),
                "fill_gap_count": max(len(order_intents) - len(fills), 0),
                "trade_count": len(trades),
                "fill_reliability": _fill_reliability(intent_count=len(order_intents), fill_count=len(fills)),
                "event_count": len(events),
                "checkpoint_continue_event_count": len(checkpoint_continue_events),
                "checkpoint_exit_trade_count": len(checkpoint_exit_trades),
                "trade_metrics": trade_metrics,
                "exit_reason_counts": dict(sorted(Counter(str(row.get("exit_reason") or "") for row in trades).items())),
                "app_latest_activity_timestamp": strategy_row.get("latest_activity_timestamp"),
                "app_realized_pnl": strategy_row.get("realized_pnl"),
                "app_trade_count": strategy_row.get("trade_count"),
                "app_closed_trade_rows": len(app_trade_rows),
                "capture_consistency": {
                    "strategy_performance_row_present": bool(strategy_row),
                    "closed_trade_log_rows_present": len(app_trade_rows) == len(trades),
                },
            }
        )

    lane_reports.sort(key=lambda row: row["lane_id"])
    long_trades = [trade for trade in all_trades if str(trade.get("direction") or "").upper() == "LONG"]
    short_trades = [trade for trade in all_trades if str(trade.get("direction") or "").upper() == "SHORT"]
    runtime_summary = {
        "paper_session_strategy_status": paper_session_status.get("strategy_status"),
        "paper_session_updated_at": paper_session_status.get("updated_at"),
        "last_processed_bar_end_ts": paper_session_status.get("last_processed_bar_end_ts"),
        "paper_lane_count": paper_session_status.get("paper_lane_count"),
        "temp_paper_enabled_in_app_count": integrity_snapshot.get("enabled_in_app_count"),
        "temp_paper_loaded_in_runtime_count": integrity_snapshot.get("loaded_in_runtime_count"),
        "temp_paper_snapshot_only_count": integrity_snapshot.get("snapshot_only_count"),
        "temp_paper_mismatch_status": integrity_snapshot.get("mismatch_status"),
        "latest_strategy_activity_timestamp": max(latest_activity_values) if latest_activity_values else None,
    }
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "branch_id": "active_trend_participation_engine",
        "status": "observe_more_live_sessions_no_entry_changes",
        "exit_policy_note": (
            "ATPE long lanes now use target-checkpoint continuation; ATPE short lanes remain on the hard target."
        ),
        "live_runtime_summary": {
            **runtime_summary,
            "total_signal_count": total_signals,
            "total_intent_count": total_intents,
            "total_fill_count": total_fills,
            "total_fill_gap_count": total_fill_gaps,
            "total_trade_count": len(all_trades),
            "overall_fill_reliability": _fill_reliability(intent_count=total_intents, fill_count=total_fills),
            "long_checkpoint_continue_event_count": long_checkpoint_continue_events,
            "long_checkpoint_exit_trade_count": long_checkpoint_exit_trade_count,
        },
        "trade_quality_summary": {
            "overall": _trade_metrics(all_trades),
            "long": _trade_metrics(long_trades),
            "short": _trade_metrics(short_trades),
        },
        "long_checkpoint_extension_review": {
            "continue_event_count": long_checkpoint_continue_events,
            "post_checkpoint_exit_trade_count": long_checkpoint_exit_trade_count,
            "positive_exit_count": long_checkpoint_positive_exit_count,
            "negative_exit_count": long_checkpoint_negative_exit_count,
            "average_post_checkpoint_exit_pnl": (
                round(mean(long_checkpoint_exit_pnls), 3) if long_checkpoint_exit_pnls else None
            ),
            "repeated_giveback_pattern": _classify_long_giveback_pattern(
                continue_event_count=long_checkpoint_continue_events,
                exit_trade_count=long_checkpoint_exit_trade_count,
                negative_exit_count=long_checkpoint_negative_exit_count,
                long_trade_metrics=_trade_metrics(long_trades),
            ),
        },
        "per_lane": lane_reports,
        "gc_mgc_temp_paper_watch": {
            "latest_report_path": str(gc_mgc_observation_path),
            "status": gc_mgc_report.get("status"),
            "sample_status": gc_mgc_report.get("sample_status", {}).get("label"),
            "late_entry_assessment": gc_mgc_report.get("late_entry_review", {}).get("assessment"),
            "current_action": gc_mgc_report.get("recommendation", {}).get("current_action"),
        },
        "recommendation": {
            **_atpe_recommendation(
                runtime_summary={
                    **runtime_summary,
                    "overall_fill_reliability": _fill_reliability(intent_count=total_intents, fill_count=total_fills),
                    "total_fill_gap_count": total_fill_gaps,
                },
                overall_metrics=_trade_metrics(all_trades),
                long_metrics=_trade_metrics(long_trades),
                checkpoint_review={
                    "continue_event_count": long_checkpoint_continue_events,
                    "post_checkpoint_exit_trade_count": long_checkpoint_exit_trade_count,
                    "negative_exit_count": long_checkpoint_negative_exit_count,
                },
            ),
        },
    }
    return report


def _trade_metrics(trades: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(trades, key=lambda row: str(row.get("exit_timestamp") or row.get("entry_timestamp") or ""))
    realized = [float(row.get("realized_pnl") or 0.0) for row in ordered]
    winners = [value for value in realized if value > 0]
    losers = [value for value in realized if value < 0]
    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in realized:
        equity += value
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return {
        "trade_count": len(ordered),
        "realized_pnl": round(sum(realized), 3),
        "average_winner": round(mean(winners), 3) if winners else 0.0,
        "average_loser": round(mean(losers), 3) if losers else 0.0,
        "winner_count": len(winners),
        "loser_count": len(losers),
        "max_drawdown": round(max_drawdown, 3),
        "distinct_session_dates": sorted(
            {
                str(row.get("entry_timestamp") or "")[:10]
                for row in ordered
                if str(row.get("entry_timestamp") or "")
            }
        ),
    }


def _build_promotion_readiness_checklist(report: dict[str, Any]) -> list[dict[str, Any]]:
    runtime_summary = report.get("live_runtime_summary", {})
    overall = report.get("trade_quality_summary", {}).get("overall", {})
    long_metrics = report.get("trade_quality_summary", {}).get("long", {})
    session_dates = overall.get("distinct_session_dates", [])
    checks = [
        {
            "name": "Runtime Healthy",
            "status": (
                "pass"
                if str(runtime_summary.get("paper_session_strategy_status") or "").startswith("RUNNING")
                and str(runtime_summary.get("temp_paper_mismatch_status") or "").upper() in {"CLEAR", "MATCHED"}
                else "watch"
            ),
            "detail": (
                f"strategy_status={runtime_summary.get('paper_session_strategy_status')} "
                f"mismatch={runtime_summary.get('temp_paper_mismatch_status')}"
            ),
        },
        {
            "name": "Fill Reliability",
            "status": (
                "pass"
                if runtime_summary.get("overall_fill_reliability") == 1.0
                and int(runtime_summary.get("total_fill_gap_count") or 0) == 0
                else "watch"
            ),
            "detail": (
                f"overall_fill_reliability={runtime_summary.get('overall_fill_reliability')} "
                f"fill_gaps={runtime_summary.get('total_fill_gap_count')}"
            ),
        },
        {
            "name": "Multi-Session Evidence",
            "status": "pass" if len(session_dates) >= 3 else "watch",
            "detail": f"distinct_trade_session_dates={len(session_dates)}",
        },
        {
            "name": "Long Exit Extension Evidence",
            "status": "pass" if int(report.get("long_checkpoint_extension_review", {}).get("continue_event_count") or 0) >= 3 else "watch",
            "detail": (
                f"checkpoint_continue_events={report.get('long_checkpoint_extension_review', {}).get('continue_event_count')} "
                f"post_checkpoint_exits={report.get('long_checkpoint_extension_review', {}).get('post_checkpoint_exit_trade_count')}"
            ),
        },
        {
            "name": "Live Trade Sample Size",
            "status": "pass" if int(overall.get("trade_count") or 0) >= 30 else "watch",
            "detail": f"trade_count={overall.get('trade_count')}",
        },
        {
            "name": "Long Lane Quality",
            "status": "pass" if float(long_metrics.get("realized_pnl") or 0.0) > 0 else "watch",
            "detail": f"long_realized_pnl={long_metrics.get('realized_pnl')}",
        },
    ]
    return checks


def _archive_snapshot(report: dict[str, Any], output_dir: Path) -> Path:
    snapshots_dir = output_dir / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    identity_source = (
        report.get("live_runtime_summary", {}).get("latest_strategy_activity_timestamp")
        or report.get("generated_at")
        or datetime.now(UTC).isoformat()
    )
    safe_name = str(identity_source).replace(":", "-").replace("+", "p").replace("/", "_")
    path = snapshots_dir / f"{safe_name}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_archived_reports(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(item.read_text(encoding="utf-8")) for item in sorted(path.glob("*.json"))]


def _build_rolling_observation_summary(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        return {
            "snapshot_count": 0,
            "distinct_trade_ids_observed": 0,
            "distinct_session_dates_observed": 0,
            "repeated_blocker_patterns": [],
            "continuous_soak_stability": "no_snapshots",
        }
    trade_ids: set[str] = set()
    session_dates: set[str] = set()
    blocker_counter: Counter[str] = Counter()
    stability_labels: list[str] = []
    checkpoint_continue_total = 0
    checkpoint_negative_exit_total = 0
    for report in reports:
        runtime_summary = report.get("live_runtime_summary", {})
        if str(runtime_summary.get("paper_session_strategy_status") or "").startswith("RUNNING"):
            stability_labels.append("running")
        else:
            stability_labels.append("not_running")
        checkpoint_review = report.get("long_checkpoint_extension_review", {})
        checkpoint_continue_total += int(checkpoint_review.get("continue_event_count") or 0)
        checkpoint_negative_exit_total += int(checkpoint_review.get("negative_exit_count") or 0)
        for lane in report.get("per_lane", []):
            for date_value in lane.get("trade_metrics", {}).get("distinct_session_dates", []):
                if date_value:
                    session_dates.add(str(date_value))
            if int(lane.get("signal_count") or 0) > 0 and int(lane.get("trade_count") or 0) == 0:
                blocker_counter[f"{lane['lane_id']}:signals_without_trades"] += 1
            if int(lane.get("intent_count") or 0) > int(lane.get("fill_count") or 0):
                blocker_counter[f"{lane['lane_id']}:fills_lagging_intents"] += 1
        latest_report_trade_log = report.get("trade_quality_summary", {}).get("overall", {}).get("trade_count") or 0
        if latest_report_trade_log:
            for lane in report.get("per_lane", []):
                lane_dir = lane.get("lane_id")
                for index in range(int(lane.get("trade_count") or 0)):
                    trade_ids.add(f"{lane_dir}:{index}")
    stability = "stable_so_far" if all(label == "running" for label in stability_labels) else "interrupted_or_manual_restarts_seen"
    return {
        "snapshot_count": len(reports),
        "distinct_trade_ids_observed": len(trade_ids),
        "distinct_session_dates_observed": len(session_dates),
        "long_checkpoint_continue_events_total": checkpoint_continue_total,
        "long_checkpoint_negative_exit_total": checkpoint_negative_exit_total,
        "repeated_blocker_patterns": [
            {"pattern": pattern, "count": count}
            for pattern, count in blocker_counter.most_common()
            if count >= 2
        ],
        "continuous_soak_stability": stability,
    }


def _rows_by_lane(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("rows", [])
    return {
        str(row.get("lane_id")): dict(row)
        for row in rows
        if isinstance(row, dict) and row.get("lane_id")
    }


def _group_trade_log_rows(payload: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    rows = payload.get("rows", [])
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("lane_id") or row.get("standalone_strategy_id") or "").strip()
        if not lane_id:
            continue
        grouped.setdefault(lane_id, []).append(dict(row))
    return grouped


def _fill_reliability(*, intent_count: int, fill_count: int) -> float | None:
    if intent_count <= 0:
        return None
    if fill_count >= intent_count:
        return 1.0
    return round(fill_count / intent_count, 4)


def _classify_long_giveback_pattern(
    *,
    continue_event_count: int,
    exit_trade_count: int,
    negative_exit_count: int,
    long_trade_metrics: dict[str, Any],
) -> str:
    if continue_event_count <= 0:
        return "insufficient_checkpoint_event_evidence"
    if exit_trade_count <= 0:
        return "checkpoint_events_seen_but_no_post_checkpoint_exits_yet"
    negative_ratio = negative_exit_count / max(exit_trade_count, 1)
    if negative_ratio >= 0.5 and float(long_trade_metrics.get("realized_pnl") or 0.0) <= 0.0:
        return "repeated_negative_giveback_watch"
    if negative_ratio > 0.0:
        return "mixed_giveback_watch"
    return "no_repeated_giveback_pattern_seen"


def _atpe_recommendation(
    *,
    runtime_summary: dict[str, Any],
    overall_metrics: dict[str, Any],
    long_metrics: dict[str, Any],
    checkpoint_review: dict[str, Any],
) -> dict[str, str]:
    if not str(runtime_summary.get("paper_session_strategy_status") or "").startswith("RUNNING"):
        return {
            "current_action": "keep_current_exit_logic_and_fix_runtime_continuity_if_it_recurs",
            "recommendation_label": "keep_current_exit_logic",
            "note": "The current blocker is runtime continuity, not ATPE exit design.",
        }
    if int(runtime_summary.get("total_fill_gap_count") or 0) > 0:
        return {
            "current_action": "keep_current_exit_logic_and_watch_capture_integrity",
            "recommendation_label": "keep_current_exit_logic",
            "note": "Do not retune exits while fill capture integrity is in question.",
        }
    if int(checkpoint_review.get("continue_event_count") or 0) >= 3 and int(checkpoint_review.get("negative_exit_count") or 0) >= 3:
        return {
            "current_action": "tighten_if_negative_post_checkpoint_giveback_repeats_next_sessions",
            "recommendation_label": "tighten",
            "note": "Checkpoint holds are being used enough to judge, and repeated negative giveback is starting to appear.",
        }
    if int(overall_metrics.get("trade_count") or 0) < 30 or len(long_metrics.get("distinct_session_dates") or []) < 3:
        return {
            "current_action": "keep_exit_revision_and_collect_more_live_sessions",
            "recommendation_label": "keep_current_exit_logic",
            "note": (
                "Do not reopen ATPE entries or GC/MGC branch design yet. More live-paper sessions are needed to judge "
                "the revised long exit checkpoint in production-like observation."
            ),
        }
    if float(long_metrics.get("realized_pnl") or 0.0) <= 0.0:
        return {
            "current_action": "keep_exit_revision_but_watch_long_lane_quality_closely",
            "recommendation_label": "keep_current_exit_logic",
            "note": "The long checkpoint logic is active, but long-side realized P&L is not yet positive across the observed sample.",
        }
    return {
        "current_action": "keep_current_exit_logic",
        "recommendation_label": "keep_current_exit_logic",
        "note": "The current long checkpoint revision is behaving acceptably; keep observing before any further tightening or revision.",
    }


def _latest_timestamp(rows: list[dict[str, Any]], key: str) -> str | None:
    values = [str(row.get(key) or "").strip() for row in rows if str(row.get(key) or "").strip()]
    return max(values) if values else None


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped:
            rows.append(json.loads(stripped))
    return rows


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# ATPE Live Observation",
        f"Generated at: {report['generated_at']}",
        f"Status: {report['status']}",
        f"Exit policy note: {report['exit_policy_note']}",
        "",
        "## Runtime Summary",
        f"- Paper session strategy status: {report['live_runtime_summary']['paper_session_strategy_status']}",
        f"- Last processed bar: {report['live_runtime_summary']['last_processed_bar_end_ts']}",
        f"- Total signals: {report['live_runtime_summary']['total_signal_count']}",
        f"- Total intents: {report['live_runtime_summary']['total_intent_count']}",
        f"- Total fills: {report['live_runtime_summary']['total_fill_count']}",
        f"- Total trades: {report['live_runtime_summary']['total_trade_count']}",
        f"- Fill reliability: {report['live_runtime_summary']['overall_fill_reliability']}",
        f"- Long checkpoint continue events: {report['long_checkpoint_extension_review']['continue_event_count']}",
        "",
        "## Trade Quality",
        f"- Overall: trades={report['trade_quality_summary']['overall']['trade_count']}, pnl={report['trade_quality_summary']['overall']['realized_pnl']}, avg_winner={report['trade_quality_summary']['overall']['average_winner']}, avg_loser={report['trade_quality_summary']['overall']['average_loser']}, max_drawdown={report['trade_quality_summary']['overall']['max_drawdown']}",
        f"- Long: trades={report['trade_quality_summary']['long']['trade_count']}, pnl={report['trade_quality_summary']['long']['realized_pnl']}, avg_winner={report['trade_quality_summary']['long']['average_winner']}, avg_loser={report['trade_quality_summary']['long']['average_loser']}, max_drawdown={report['trade_quality_summary']['long']['max_drawdown']}",
        f"- Short: trades={report['trade_quality_summary']['short']['trade_count']}, pnl={report['trade_quality_summary']['short']['realized_pnl']}, avg_winner={report['trade_quality_summary']['short']['average_winner']}, avg_loser={report['trade_quality_summary']['short']['average_loser']}, max_drawdown={report['trade_quality_summary']['short']['max_drawdown']}",
        "",
        "## Long Checkpoint Review",
        f"- Continue events: {report['long_checkpoint_extension_review']['continue_event_count']}",
        f"- Post-checkpoint exits: {report['long_checkpoint_extension_review']['post_checkpoint_exit_trade_count']}",
        f"- Positive post-checkpoint exits: {report['long_checkpoint_extension_review']['positive_exit_count']}",
        f"- Negative post-checkpoint exits: {report['long_checkpoint_extension_review']['negative_exit_count']}",
        f"- Average post-checkpoint exit pnl: {report['long_checkpoint_extension_review']['average_post_checkpoint_exit_pnl']}",
        f"- Giveback pattern: {report['long_checkpoint_extension_review']['repeated_giveback_pattern']}",
        "",
        "## Promotion Readiness",
    ]
    for check in report.get("promotion_readiness_checklist", []):
        lines.append(f"- {check['name']}: {check['status']} ({check['detail']})")
    lines.extend(["", "## Per Lane"])
    for lane in report.get("per_lane", []):
        lines.extend(
            [
                "",
                f"### {lane['lane_name']}",
                f"- Signals={lane['signal_count']} intents={lane['intent_count']} fills={lane['fill_count']} trades={lane['trade_count']}",
                f"- Latest processed={lane['latest_processed_bar_timestamp']} latest trade={lane['latest_trade_timestamp']}",
                f"- Realized pnl={lane['trade_metrics']['realized_pnl']} avg_winner={lane['trade_metrics']['average_winner']} avg_loser={lane['trade_metrics']['average_loser']} max_drawdown={lane['trade_metrics']['max_drawdown']}",
                f"- Fill reliability={lane['fill_reliability']} app trade rows={lane['app_closed_trade_rows']}",
            ]
        )
    lines.extend(
        [
            "",
            "## Recommendation",
            f"- Recommendation: {report['recommendation']['recommendation_label']}",
            f"- Action: {report['recommendation']['current_action']}",
            f"- Note: {report['recommendation']['note']}",
            "",
            "## GC/MGC Watch",
            f"- Status: {report['gc_mgc_temp_paper_watch']['status']}",
            f"- Sample status: {report['gc_mgc_temp_paper_watch']['sample_status']}",
            f"- Late entry assessment: {report['gc_mgc_temp_paper_watch']['late_entry_assessment']}",
        ]
    )
    return "\n".join(lines)
