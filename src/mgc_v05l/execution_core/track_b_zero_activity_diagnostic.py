"""Bounded zero-activity diagnostics for the Track B PAPER monitor.

This read-only diagnostic explains why an autonomous PAPER monitor has not
produced ledger activity. It intentionally reads compact latest summaries and a
bounded set of recent monitor/runtime-cycle reports only.
"""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_JSON = (
    DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT / "latest_track_b_zero_activity_diagnostic.json"
)
DEFAULT_TRACK_B_COMPLETED_DECISION_BAR_AUDIT_JSON = (
    DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT / "latest_track_b_completed_decision_bar_evaluation_audit.json"
)
DEFAULT_TRACK_B_NO_SIGNAL_ATTRIBUTION_ROLLUP_JSON = (
    DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT / "latest_track_b_no_signal_attribution_rollup.json"
)
DEFAULT_RECENT_CYCLE_LIMIT = 120
DEFAULT_DECISION_BAR_AUDIT_WINDOW_MINUTES = 60
DEFAULT_NO_SIGNAL_ATTRIBUTION_DECISION_BAR_LIMIT = 24
MAX_REPORT_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class TrackBZeroActivityDiagnosticResult:
    report_json: Path
    report: dict[str, Any]


def build_track_b_zero_activity_diagnostic(
    *,
    repo_root: Path = Path("."),
    output_root: Path = DEFAULT_TRACK_B_ZERO_ACTIVITY_DIAGNOSTIC_OUTPUT_ROOT,
    recent_cycle_limit: int = DEFAULT_RECENT_CYCLE_LIMIT,
    decision_bar_audit_window_minutes: int = DEFAULT_DECISION_BAR_AUDIT_WINDOW_MINUTES,
    no_signal_attribution_decision_bar_limit: int = DEFAULT_NO_SIGNAL_ATTRIBUTION_DECISION_BAR_LIMIT,
    now: datetime | None = None,
    write: bool = True,
) -> TrackBZeroActivityDiagnosticResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    root = Path(repo_root)
    monitor_root = root / "outputs" / "track_b_execution_core" / "track_b_shadow_monitor"
    runtime_root = root / "outputs" / "track_b_execution_core" / "track_b_multi_strategy_runtime_cycle"
    journal_root = root / "outputs" / "track_b_execution_core" / "track_b_decision_journal"
    ledger_root = root / "outputs" / "track_b_execution_core" / "paper_trade_ledger"
    feed_root = root / "outputs" / "track_b_execution_core" / "databento_live_runtime_feed"
    operator_root = root / "outputs" / "track_b_execution_core" / "operator_status"

    latest_monitor_path = monitor_root / "latest_track_b_shadow_monitor_report.json"
    latest_heartbeat_path = monitor_root / "latest_track_b_shadow_monitor_heartbeat.json"
    latest_live_feed_path = feed_root / "latest_databento_live_runtime_feed_report.json"
    latest_operator_status_path = operator_root / "latest_operator_status_summary.json"
    latest_journal_summary_path = journal_root / "latest_track_b_decision_journal_summary.json"
    latest_trade_summary_path = ledger_root / "latest_track_b_paper_trade_summary.json"
    latest_position_status_path = ledger_root / "latest_track_b_live_position_status.json"
    latest_pnl_summary_path = ledger_root / "latest_track_b_pnl_summary.json"

    latest_monitor = _load_json(latest_monitor_path)
    latest_heartbeat = _load_json(latest_heartbeat_path)
    latest_live_feed = _load_json(latest_live_feed_path)
    latest_operator_status = _load_json(latest_operator_status_path)
    latest_journal_summary = _load_json(latest_journal_summary_path)
    latest_trade_summary = _load_json(latest_trade_summary_path)
    latest_position_status = _load_json(latest_position_status_path)
    latest_pnl_summary = _load_json(latest_pnl_summary_path)

    recent_monitor_reports = _recent_monitor_reports(
        monitor_root=monitor_root,
        latest_monitor_path=latest_monitor_path,
        limit=recent_cycle_limit,
    )
    recent_runtime_reports = _recent_runtime_reports_from_monitor_reports(recent_monitor_reports, runtime_root)
    enabled_by_instrument = _enabled_strategies_by_instrument(latest_monitor, recent_monitor_reports)

    cycle_summary = _summarize_cycles(recent_monitor_reports)
    strategy_summary = _summarize_strategies(
        recent_monitor_reports=recent_monitor_reports,
        recent_runtime_reports=recent_runtime_reports,
        enabled_by_instrument=enabled_by_instrument,
    )
    signals = _summarize_signal_chain(recent_monitor_reports, recent_runtime_reports)
    ledger = _summarize_ledger(
        latest_trade_summary=latest_trade_summary,
        latest_position_status=latest_position_status,
        latest_pnl_summary=latest_pnl_summary,
        latest_trade_summary_path=latest_trade_summary_path,
        latest_position_status_path=latest_position_status_path,
        latest_pnl_summary_path=latest_pnl_summary_path,
        latest_monitor=latest_monitor,
        latest_handoff=signals.get("latest_paper_handoff_attempt"),
    )
    journal = _summarize_journal(latest_journal_summary, recent_monitor_reports)
    live_feed = _summarize_live_feed(latest_monitor, latest_live_feed)
    safety = _summarize_safety(latest_monitor, recent_monitor_reports)
    completed_decision_bar_audit = _build_completed_decision_bar_evaluation_audit_report(
        repo_root=root,
        now=actual_now,
        recent_monitor_reports=recent_monitor_reports,
        enabled_by_instrument=enabled_by_instrument,
        window_minutes=decision_bar_audit_window_minutes,
    )
    no_signal_attribution_rollup = _build_no_signal_attribution_rollup(
        repo_root=root,
        now=actual_now,
        completed_decision_bar_audit=completed_decision_bar_audit,
        decision_bar_limit=no_signal_attribution_decision_bar_limit,
    )
    diagnosis, next_action = _classify_diagnosis(
        latest_monitor=latest_monitor,
        cycle_summary=cycle_summary,
        strategy_summary=strategy_summary,
        signals=signals,
        ledger=ledger,
        live_feed=live_feed,
        completed_decision_bar_audit=completed_decision_bar_audit,
    )

    report_json = Path(output_root) / "latest_track_b_zero_activity_diagnostic.json"
    completed_decision_bar_audit_json = Path(output_root) / "latest_track_b_completed_decision_bar_evaluation_audit.json"
    no_signal_attribution_rollup_json = Path(output_root) / "latest_track_b_no_signal_attribution_rollup.json"
    report = {
        "schema_version": "track_b_zero_activity_diagnostic_v1",
        "generated_at": actual_now.isoformat(),
        "diagnostic_window": {
            "recent_cycle_limit": max(0, int(recent_cycle_limit)),
            "recent_monitor_cycle_count": len(recent_monitor_reports),
            "recent_runtime_cycle_count": len(recent_runtime_reports),
            "bounded_policy": "Reads latest compact summaries and bounded recent monitor/runtime-cycle reports only.",
            "full_paper_trade_ledger_scanned": False,
            "full_decision_journal_scanned": False,
        },
        "monitor_running": latest_heartbeat.get("monitor_running"),
        "monitor_mode": latest_monitor.get("mode") or latest_monitor.get("monitor_mode"),
        "runtime_source": latest_monitor.get("runtime_decision_source") or latest_monitor.get("runtime_data_source"),
        "latest_monitor_verdict": latest_monitor.get("monitor_verdict"),
        "latest_monitor_completed_at": latest_monitor.get("completed_at"),
        "heartbeat_age_seconds": _age_seconds(latest_heartbeat.get("generated_at"), actual_now),
        "heartbeat_path": str(latest_heartbeat_path),
        "latest_monitor_report_path": str(latest_monitor_path),
        "instruments_configured": list(enabled_by_instrument.keys()),
        "enabled_strategies_by_instrument": enabled_by_instrument,
        "live_feed_summary": live_feed,
        "cycle_summary": cycle_summary,
        "completed_decision_bar_audit": completed_decision_bar_audit,
        "no_signal_attribution_rollup": no_signal_attribution_rollup,
        "per_strategy_recent_result_counts": strategy_summary["per_strategy_recent_result_counts"],
        "top_not_ready_reasons": strategy_summary["top_not_ready_reasons"],
        "top_no_signal_predicate_blockers": strategy_summary["top_no_signal_predicate_blockers"],
        "top_suppression_or_arbitration_reasons": signals["top_suppression_or_arbitration_reasons"],
        "latest_signal_candidate": signals["latest_signal_candidate"],
        "latest_paper_handoff_attempt": signals["latest_paper_handoff_attempt"],
        "paper_handoff_attempt_count": signals["paper_handoff_attempt_count"],
        "journal_summary": journal,
        "ledger_summary": ledger,
        "safety_summary": safety,
        "diagnosis_classification": diagnosis,
        "dominant_blocker": _dominant_blocker(cycle_summary, strategy_summary, signals, live_feed),
        "recommended_next_action": next_action,
        "source_artifact_paths": {
            "latest_monitor_report": str(latest_monitor_path),
            "latest_monitor_heartbeat": str(latest_heartbeat_path),
            "latest_live_feed_report": str(latest_live_feed_path),
            "latest_operator_status": str(latest_operator_status_path),
            "latest_decision_journal_summary": str(latest_journal_summary_path),
            "latest_trade_summary": str(latest_trade_summary_path),
            "latest_live_position_status": str(latest_position_status_path),
            "latest_pnl_summary": str(latest_pnl_summary_path),
            "latest_completed_decision_bar_audit": str(completed_decision_bar_audit_json),
            "latest_no_signal_attribution_rollup": str(no_signal_attribution_rollup_json),
        },
        "operator_status_verdict": latest_operator_status.get("status_verdict"),
    }
    if write:
        _write_json(completed_decision_bar_audit_json, completed_decision_bar_audit)
        _write_json(no_signal_attribution_rollup_json, no_signal_attribution_rollup)
        _write_json(report_json, report)
    return TrackBZeroActivityDiagnosticResult(report_json=report_json, report=report)


def _recent_monitor_reports(*, monitor_root: Path, latest_monitor_path: Path, limit: int) -> list[dict[str, Any]]:
    limit = max(0, int(limit))
    paths = sorted(
        monitor_root.glob("track_b_shadow_monitor_*/track_b_shadow_monitor_report.json"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
    )[-limit:]
    if latest_monitor_path.exists() and latest_monitor_path not in paths:
        paths.append(latest_monitor_path)
    loaded: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in paths:
        payload = _load_json(path)
        if not payload:
            continue
        key = str(payload.get("cycle_id") or path)
        if key in seen:
            continue
        seen.add(key)
        payload["_diagnostic_source_path"] = str(path)
        loaded.append(payload)
    return sorted(loaded, key=lambda item: str(item.get("completed_at") or item.get("started_at") or ""))


def _recent_runtime_reports_from_monitor_reports(
    monitor_reports: list[Mapping[str, Any]],
    runtime_root: Path,
) -> list[dict[str, Any]]:
    paths: list[Path] = []
    for report in monitor_reports:
        for instrument in _instrument_reports(report):
            raw_path = instrument.get("multi_strategy_runtime_cycle_report_path")
            if raw_path:
                paths.append(Path(str(raw_path)))
    if not paths:
        paths = sorted(
            runtime_root.glob("*/track_b_multi_strategy_runtime_cycle_report.json"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
        )[-20:]
    loaded: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in paths:
        payload = _load_json(path)
        if not payload:
            continue
        key = str(payload.get("track_b_multi_strategy_runtime_cycle_id") or path)
        if key in seen:
            continue
        seen.add(key)
        payload["_diagnostic_source_path"] = str(path)
        loaded.append(payload)
    return sorted(loaded, key=lambda item: str(item.get("generated_at") or ""))


def _enabled_strategies_by_instrument(
    latest_monitor: Mapping[str, Any],
    recent_monitor_reports: list[Mapping[str, Any]],
) -> dict[str, list[str]]:
    reports = [latest_monitor, *reversed(recent_monitor_reports)]
    result: dict[str, list[str]] = {}
    for report in reports:
        for instrument in _instrument_reports(report):
            family = str(instrument.get("instrument_family") or "")
            if not family:
                continue
            strategies = instrument.get("enabled_strategies")
            if isinstance(strategies, list):
                result.setdefault(family, [str(item) for item in strategies])
    return dict(sorted(result.items()))


def _summarize_cycles(recent_monitor_reports: list[Mapping[str, Any]]) -> dict[str, Any]:
    verdict_counts = Counter(str(item.get("monitor_verdict") or "UNKNOWN") for item in recent_monitor_reports)
    evaluated_cycle_count = sum(1 for item in recent_monitor_reports if int(item.get("evaluated_strategy_count") or 0) > 0)
    stale_cycle_count = sum(1 for item in recent_monitor_reports if _cycle_is_stale(item))
    heartbeat_only_count = sum(
        1 for item in recent_monitor_reports if str(item.get("monitor_verdict") or "").endswith("HEARTBEAT_NO_NEW_COMPLETED_BAR")
    )
    provider_blocked_count = sum(1 for item in recent_monitor_reports if "PROVIDER" in str(item.get("monitor_verdict") or ""))
    total_evaluations = sum(int(item.get("evaluated_strategy_count") or 0) for item in recent_monitor_reports)
    return {
        "recent_monitor_cycle_count": len(recent_monitor_reports),
        "recent_evaluation_cycle_count": evaluated_cycle_count,
        "recent_stale_cycle_count": stale_cycle_count,
        "recent_heartbeat_only_cycle_count": heartbeat_only_count,
        "recent_provider_blocked_cycle_count": provider_blocked_count,
        "recent_strategy_evaluation_count": total_evaluations,
        "recent_candidate_signal_count": sum(len(item.get("candidate_signals") or []) for item in recent_monitor_reports),
        "recent_suppressed_signal_count": sum(len(item.get("suppressed_signals") or []) for item in recent_monitor_reports),
        "monitor_verdict_counts": dict(verdict_counts),
        "latest_cycle": _compact_cycle(recent_monitor_reports[-1]) if recent_monitor_reports else {},
        "recent_cycles": [_compact_cycle(item) for item in recent_monitor_reports[-20:]],
        "top_cycle_blockers": _counter_rows(
            Counter(str(item.get("primary_blocker") or "NONE") for item in recent_monitor_reports if item.get("primary_blocker"))
        ),
    }


def _summarize_strategies(
    *,
    recent_monitor_reports: list[Mapping[str, Any]],
    recent_runtime_reports: list[Mapping[str, Any]],
    enabled_by_instrument: Mapping[str, list[str]],
) -> dict[str, Any]:
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    not_ready_reasons: Counter[str] = Counter()
    no_signal_blockers: Counter[str] = Counter()
    latest_by_strategy: dict[str, dict[str, Any]] = {}

    for instrument, strategies in enabled_by_instrument.items():
        for strategy_id in strategies:
            counts[str(strategy_id)]["configured"] += 1

    for report in recent_monitor_reports:
        if _cycle_is_stale(report) and int(report.get("evaluated_strategy_count") or 0) == 0:
            reason = str(report.get("primary_blocker") or "stale runtime context")
            for instrument in _instrument_reports(report):
                for strategy_id in instrument.get("enabled_strategies") or []:
                    counts[str(strategy_id)]["stale_input"] += 1
                    latest_by_strategy[str(strategy_id)] = {
                        "strategy_id": str(strategy_id),
                        "last_result": "STALE_INPUT",
                        "last_reason": reason,
                        "last_cycle": report.get("cycle_index"),
                        "last_seen_at": report.get("completed_at"),
                    }

    for runtime in recent_runtime_reports:
        suppressed_ids = {str(item.get("strategy_id") or item.get("signal_source") or "") for item in runtime.get("suppressed_signals") or []}
        candidate_ids = {str(item.get("strategy_id") or item.get("signal_source") or "") for item in runtime.get("candidate_signals") or []}
        for strategy in runtime.get("evaluated_strategies") or []:
            if not isinstance(strategy, Mapping):
                continue
            strategy_id = str(strategy.get("strategy_id") or strategy.get("signal_source") or "UNKNOWN")
            result = _strategy_result(strategy, suppressed_ids=suppressed_ids, candidate_ids=candidate_ids)
            counts[strategy_id]["evaluated"] += 1
            counts[strategy_id][result.lower()] += 1
            latest_by_strategy[strategy_id] = {
                "strategy_id": strategy_id,
                "last_result": result,
                "last_verdict": strategy.get("strategy_runtime_verdict") or strategy.get("strategy_rule_runner_verdict"),
                "last_decision": strategy.get("decision"),
                "last_reason": strategy.get("primary_blocker") or strategy.get("decision_reason"),
                "last_runtime_cycle_id": runtime.get("track_b_multi_strategy_runtime_cycle_id"),
                "last_seen_at": runtime.get("generated_at"),
            }
            if result == "NOT_READY":
                not_ready_reasons[str(strategy.get("primary_blocker") or strategy.get("decision_reason") or "NOT_READY")] += 1
            if result == "NO_SIGNAL":
                blockers = strategy.get("rule_blockers")
                if isinstance(blockers, list) and blockers:
                    for blocker in blockers[:6]:
                        no_signal_blockers[str(blocker)] += 1
                else:
                    no_signal_blockers[str(strategy.get("decision_reason") or "conditions_not_met")] += 1

    return {
        "per_strategy_recent_result_counts": {
            key: {name: int(value) for name, value in counter.items()}
            for key, counter in sorted(counts.items())
        },
        "latest_strategy_results": [latest_by_strategy[key] for key in sorted(latest_by_strategy)],
        "top_not_ready_reasons": _counter_rows(not_ready_reasons),
        "top_no_signal_predicate_blockers": _counter_rows(no_signal_blockers, limit=20),
    }


def _summarize_signal_chain(
    recent_monitor_reports: list[Mapping[str, Any]],
    recent_runtime_reports: list[Mapping[str, Any]],
) -> dict[str, Any]:
    latest_candidate: dict[str, Any] | None = None
    latest_handoff: dict[str, Any] | None = None
    suppression_reasons: Counter[str] = Counter()
    handoff_count = 0
    handoff_blockers: Counter[str] = Counter()
    for runtime in recent_runtime_reports:
        for candidate in runtime.get("candidate_signals") or []:
            if isinstance(candidate, Mapping):
                latest_candidate = {
                    "generated_at": runtime.get("generated_at"),
                    "runtime_cycle_id": runtime.get("track_b_multi_strategy_runtime_cycle_id"),
                    **dict(candidate),
                }
        for suppressed in runtime.get("suppressed_signals") or []:
            if isinstance(suppressed, Mapping):
                suppression_reasons[str(suppressed.get("reason") or suppressed.get("primary_blocker") or "suppressed_signal")] += 1
        arbitration = runtime.get("arbitration_result") if isinstance(runtime.get("arbitration_result"), Mapping) else {}
        if arbitration.get("primary_blocker"):
            suppression_reasons[str(arbitration.get("primary_blocker"))] += 1
        if runtime.get("paper_runner_report_path") or runtime.get("paper_proof_invoked") or runtime.get("submit_attempted"):
            handoff_count += 1
            latest_handoff = {
                "generated_at": runtime.get("generated_at"),
                "runtime_cycle_id": runtime.get("track_b_multi_strategy_runtime_cycle_id"),
                "paper_runner_report_path": runtime.get("paper_runner_report_path"),
                "paper_runner_verdict": runtime.get("paper_runner_verdict"),
                "paper_proof_invoked": runtime.get("paper_proof_invoked"),
                "submit_attempted": runtime.get("submit_attempted"),
                "broker_state_mutated": runtime.get("broker_state_mutated"),
                "paper_order_parameter_blocker": runtime.get("paper_order_parameter_blocker"),
            }
        if runtime.get("paper_order_parameter_blocker"):
            handoff_blockers[str(runtime.get("paper_order_parameter_blocker"))] += 1
    for report in recent_monitor_reports:
        if report.get("latest_paper_order_parameter_blocker"):
            handoff_blockers[str(report.get("latest_paper_order_parameter_blocker"))] += 1
    return {
        "latest_signal_candidate": latest_candidate,
        "latest_paper_handoff_attempt": latest_handoff,
        "paper_handoff_attempt_count": handoff_count,
        "top_suppression_or_arbitration_reasons": _counter_rows(suppression_reasons),
        "top_paper_handoff_blockers": _counter_rows(handoff_blockers),
    }


def _summarize_ledger(
    *,
    latest_trade_summary: Mapping[str, Any],
    latest_position_status: Mapping[str, Any],
    latest_pnl_summary: Mapping[str, Any],
    latest_trade_summary_path: Path,
    latest_position_status_path: Path,
    latest_pnl_summary_path: Path,
    latest_monitor: Mapping[str, Any],
    latest_handoff: object,
) -> dict[str, Any]:
    latest_monitor_time = _parse_datetime(latest_monitor.get("completed_at"))
    trade_summary_time = _parse_datetime(latest_trade_summary.get("as_of")) or _mtime_datetime(latest_trade_summary_path)
    latest_handoff_time = _parse_datetime(latest_handoff.get("generated_at")) if isinstance(latest_handoff, Mapping) else None
    return {
        "trade_summary_available": bool(latest_trade_summary),
        "position_status_available": bool(latest_position_status),
        "pnl_summary_available": bool(latest_pnl_summary),
        "paper_trades_attempted_count": latest_trade_summary.get("paper_trades_attempted_count", 0),
        "completed_trade_count": latest_trade_summary.get("completed_trade_count", latest_trade_summary.get("closed_trade_count", 0)),
        "open_position_count": latest_position_status.get("open_position_count", latest_trade_summary.get("open_position_count", 0)),
        "realized_pnl_today": latest_pnl_summary.get("total_realized_pnl_today", "0"),
        "unrealized_pnl": latest_pnl_summary.get("total_unrealized_pnl", "0"),
        "review_required_count": latest_pnl_summary.get("review_required_count", latest_trade_summary.get("review_required_count", 0)),
        "latest_trade_time": latest_trade_summary.get("last_trade_time") or latest_pnl_summary.get("last_trade_time"),
        "latest_ledger_write_time": None if trade_summary_time is None else trade_summary_time.isoformat(),
        "ledger_summary_older_than_latest_monitor_cycle": (
            latest_monitor_time is not None and trade_summary_time is not None and trade_summary_time < latest_monitor_time
        ),
        "ledger_summary_stale_after_handoff": (
            latest_handoff_time is not None and trade_summary_time is not None and trade_summary_time < latest_handoff_time
        ),
        "latest_trade_summary_path": str(latest_trade_summary_path),
        "latest_live_position_status_path": str(latest_position_status_path),
        "latest_pnl_summary_path": str(latest_pnl_summary_path),
        "full_paper_trade_ledger_scanned": False,
    }


def _summarize_journal(latest_journal_summary: Mapping[str, Any], recent_monitor_reports: list[Mapping[str, Any]]) -> dict[str, Any]:
    aggregate_counts: Counter[str] = Counter()
    for report in recent_monitor_reports:
        counts = report.get("decision_journal_tier_counts") if isinstance(report.get("decision_journal_tier_counts"), Mapping) else {}
        for key, value in counts.items():
            aggregate_counts[str(key)] += int(value or 0)
    return {
        "summary_available": bool(latest_journal_summary),
        "latest_summary_generated_at": latest_journal_summary.get("generated_at"),
        "latest_tier_counts": latest_journal_summary.get("latest_tier_counts") or {},
        "recent_monitor_report_tier_counts": dict(aggregate_counts),
        "evaluated_strategy_count": latest_journal_summary.get("evaluated_strategy_count"),
        "decision_journal_path": latest_journal_summary.get("decision_journal_path"),
        "decision_journal_heartbeat_path": latest_journal_summary.get("decision_journal_heartbeat_path"),
        "ordinary_no_setup_aggregate_updates": latest_journal_summary.get("ordinary_no_setup_aggregate_updates"),
        "near_miss_records_written": latest_journal_summary.get("near_miss_records_written"),
        "signal_records_written": latest_journal_summary.get("signal_records_written"),
        "abnormal_records_written": latest_journal_summary.get("abnormal_records_written"),
        "tier3_without_recent_candidate_signal_warning": bool(
            (latest_journal_summary.get("signal_records_written") or 0)
            and not any(report.get("candidate_signals") for report in recent_monitor_reports)
        ),
    }


def _summarize_live_feed(latest_monitor: Mapping[str, Any], latest_live_feed: Mapping[str, Any]) -> dict[str, Any]:
    instrument_rows = []
    for instrument in _instrument_reports(latest_monitor):
        instrument_rows.append(
            {
                "instrument_family": instrument.get("instrument_family"),
                "enabled_strategy_count": len(instrument.get("enabled_strategies") or []),
                "live_feed_connected": instrument.get("live_feed_connected"),
                "live_feed_status": instrument.get("live_feed_status"),
                "live_feed_strategy_ready": instrument.get("live_feed_strategy_ready"),
                "live_feed_warmup_1m_count": instrument.get("live_feed_warmup_1m_count"),
                "live_feed_warmup_completed_5m_count": instrument.get("live_feed_warmup_completed_5m_count"),
                "latest_1m_timestamp": instrument.get("latest_1m_timestamp"),
                "latest_completed_5m_timestamp": instrument.get("latest_completed_5m_timestamp"),
                "fresh_for_execution": instrument.get("fresh_for_execution"),
                "runtime_candle_age_seconds": instrument.get("runtime_candle_age_seconds"),
                "live_feed_blocker": instrument.get("live_feed_blocker"),
            }
        )
    return {
        "latest_live_feed_report_available": bool(latest_live_feed),
        "latest_live_feed_report_status": latest_live_feed.get("databento_live_runtime_feed_verdict")
        or latest_live_feed.get("live_feed_status"),
        "instruments": instrument_rows,
        "live_feed_ready_instruments": [
            item["instrument_family"]
            for item in instrument_rows
            if item.get("live_feed_connected") is True and item.get("live_feed_strategy_ready") is True
        ],
        "strategy_ready_instruments": [
            item["instrument_family"] for item in instrument_rows if item.get("live_feed_strategy_ready") is True
        ],
        "stale_or_not_fresh_instruments": [
            item["instrument_family"]
            for item in instrument_rows
            if item.get("enabled_strategy_count", 0) > 0 and item.get("fresh_for_execution") is False
        ],
    }


def _summarize_safety(latest_monitor: Mapping[str, Any], recent_monitor_reports: list[Mapping[str, Any]]) -> dict[str, Any]:
    keys = ("submit_allowed", "submit_attempted", "paper_proof_invoked", "broker_state_mutated", "live_money_readiness")
    latest = {key: bool(latest_monitor.get(key)) for key in keys}
    recent_true = {
        key: sum(1 for report in recent_monitor_reports if report.get(key) is True)
        for key in keys
    }
    guarded_provenance = bool(latest_monitor.get("latest_paper_lifecycle_report_path") or latest_monitor.get("latest_paper_lifecycle_report_path"))
    return {
        "latest": latest,
        "recent_true_counts": recent_true,
        "guarded_lifecycle_provenance_present": guarded_provenance,
        "critical": bool(latest.get("live_money_readiness"))
        or bool((latest.get("submit_attempted") or latest.get("broker_state_mutated")) and not guarded_provenance),
    }


def _build_completed_decision_bar_evaluation_audit_report(
    *,
    repo_root: Path,
    now: datetime,
    recent_monitor_reports: list[Mapping[str, Any]],
    enabled_by_instrument: Mapping[str, list[str]],
    window_minutes: int,
) -> dict[str, Any]:
    require_aware_datetime(now, "now")
    requested_window_start = now.astimezone(UTC) - timedelta(minutes=max(1, int(window_minutes)))
    requested_window_end = now.astimezone(UTC)
    monitor_times = [
        parsed
        for parsed in (_parse_datetime(item.get("completed_at")) for item in recent_monitor_reports)
        if parsed is not None
    ]
    # Keep the audited decision-bar window inside retained monitor history. If
    # a completed bar is older than the first retained report, its evaluation
    # may already have been pruned and should not be counted as skipped.
    bounded_window_start = (
        max(requested_window_start, min(monitor_times))
        if monitor_times
        else requested_window_start
    )
    instruments: dict[str, Any] = {}
    classifications: Counter[str] = Counter()
    for instrument, strategies in sorted(enabled_by_instrument.items()):
        if not strategies:
            continue
        row = _completed_decision_bar_audit_for_instrument(
            repo_root=repo_root,
            instrument_family=str(instrument),
            enabled_strategies=[str(item) for item in strategies],
            recent_monitor_reports=recent_monitor_reports,
            window_start=bounded_window_start,
            window_end=requested_window_end,
        )
        instruments[str(instrument)] = row
        classifications[str(row.get("classification") or "DIAGNOSTIC_INCONCLUSIVE")] += 1
    classification = _classify_completed_decision_bar_audit(instruments)
    return {
        "schema_version": "track_b_completed_decision_bar_evaluation_audit_v1",
        "generated_at": now.isoformat(),
        "diagnostic_window": {
            "requested_window_minutes": max(1, int(window_minutes)),
            "requested_window_start": requested_window_start.isoformat(),
            "requested_window_end": requested_window_end.isoformat(),
            "bounded_monitor_report_window_start": bounded_window_start.isoformat(),
            "bounded_monitor_report_count": len(recent_monitor_reports),
            "bounded_policy": "Reads latest completed 5m live artifacts and bounded recent monitor/runtime-cycle reports only.",
        },
        "classification": classification,
        "instrument_classification_counts": dict(classifications),
        "instruments": instruments,
        "full_paper_trade_ledger_scanned": False,
        "full_decision_journal_scanned": False,
    }


def _completed_decision_bar_audit_for_instrument(
    *,
    repo_root: Path,
    instrument_family: str,
    enabled_strategies: list[str],
    recent_monitor_reports: list[Mapping[str, Any]],
    window_start: datetime,
    window_end: datetime,
) -> dict[str, Any]:
    completed_5m_path = _latest_live_completed_5m_path(repo_root, instrument_family)
    completed_payload = _load_json(completed_5m_path)
    completed_timestamp_sources: dict[str, set[str]] = defaultdict(set)
    for item in _completed_5m_bars(completed_payload):
        timestamp = str(item.get("candle_timestamp") or item.get("timestamp") or "")
        bar_ts = _parse_datetime(timestamp)
        if timestamp and bar_ts is not None and window_start <= bar_ts <= window_end:
            completed_timestamp_sources[timestamp].add("LIVE_COMPLETED_5M_ARTIFACT")
    rows_by_bar: dict[str, list[tuple[Mapping[str, Any], Mapping[str, Any]]]] = defaultdict(list)
    for report in recent_monitor_reports:
        for row in _instrument_reports(report):
            if str(row.get("instrument_family") or "") != instrument_family:
                continue
            timestamp = str(row.get("latest_completed_5m_timestamp") or "")
            if timestamp:
                rows_by_bar[timestamp].append((report, row))
                bar_ts = _parse_datetime(timestamp)
                if bar_ts is not None and window_start <= bar_ts <= window_end and row.get("runtime_decision_source") == "DATABENTO_LIVE_ARTIFACT":
                    completed_timestamp_sources[timestamp].add("MONITOR_LIVE_DECISION_CONTEXT")
    completed_timestamps = sorted(completed_timestamp_sources)

    decision_bars: list[dict[str, Any]] = []
    eligible_count = 0
    evaluated_count = 0
    skipped_count = 0
    total_no_signal = 0
    total_signal = 0
    total_suppressed = 0
    for timestamp in completed_timestamps:
        rows = rows_by_bar.get(timestamp, [])
        eligible_rows = [row for _, row in rows if row.get("paper_evaluation_allowed") is True]
        evaluated_rows = [
            row
            for _, row in rows
            if int(row.get("evaluated_strategy_count") or 0) > 0 or bool(row.get("multi_strategy_runtime_cycle_report_path"))
        ]
        strategies_evaluated = max((int(row.get("evaluated_strategy_count") or 0) for row in evaluated_rows), default=0)
        strategy_verdicts = []
        for row in evaluated_rows:
            raw = row.get("strategy_verdicts")
            if isinstance(raw, list) and len(raw) > len(strategy_verdicts):
                strategy_verdicts = [item for item in raw if isinstance(item, Mapping)]
        no_signal_count = sum(1 for item in strategy_verdicts if str(item.get("decision") or "").upper() == "NO_SIGNAL")
        signal_count = max(
            sum(1 for item in strategy_verdicts if item.get("signal_emitted") is True or str(item.get("decision") or "").upper() == "SIGNAL"),
            max((len(row.get("candidate_signals") or []) for row in evaluated_rows), default=0),
        )
        suppressed_count = max((len(row.get("suppressed_signals") or []) for row in evaluated_rows), default=0)
        skipped_reason = None
        if not evaluated_rows:
            skipped_count += 1
            skipped_reason = _skipped_decision_bar_reason(rows)
        else:
            evaluated_count += 1
        if eligible_rows or evaluated_rows:
            eligible_count += 1
        total_no_signal += no_signal_count
        total_signal += signal_count
        total_suppressed += suppressed_count
        decision_bars.append(
            {
                "completed_5m_timestamp": timestamp,
                "paper_evaluation_allowed": bool(eligible_rows or evaluated_rows),
                "evaluated": bool(evaluated_rows),
                "skipped": not bool(evaluated_rows),
                "skipped_reason": skipped_reason,
                "strategies_evaluated": strategies_evaluated,
                "no_signal_count": no_signal_count,
                "signal_count": signal_count,
                "suppressed_signal_count": suppressed_count,
                "completed_bar_sources": sorted(completed_timestamp_sources.get(timestamp, set())),
                "monitor_cycle_indices": [report.get("cycle_index") for report, _ in rows],
                "runtime_cycle_report_paths": [
                    str(row.get("multi_strategy_runtime_cycle_report_path"))
                    for row in evaluated_rows
                    if row.get("multi_strategy_runtime_cycle_report_path")
                ],
            }
        )

    latest_completed = completed_timestamps[-1] if completed_timestamps else None
    latest_evaluated = next(
        (item["completed_5m_timestamp"] for item in reversed(decision_bars) if item.get("evaluated")),
        None,
    )
    latest_bar_caught_up = latest_completed is not None and latest_completed == latest_evaluated
    classification = _classify_completed_decision_bar_instrument(
        completed_count=len(completed_timestamps),
        eligible_count=eligible_count,
        skipped_count=skipped_count,
        latest_completed=latest_completed,
        latest_evaluated=latest_evaluated,
    )
    return {
        "instrument_family": instrument_family,
        "classification": classification,
        "completed_live_5m_artifact_path": str(completed_5m_path),
        "completed_live_5m_artifact_available": bool(completed_payload),
        "enabled_strategies": enabled_strategies,
        "enabled_strategy_count": len(enabled_strategies),
        "completed_live_5m_bars_observed": len(completed_timestamps),
        "completed_live_5m_bar_timestamps": completed_timestamps,
        "decision_bars_eligible_for_evaluation": eligible_count,
        "decision_bars_actually_evaluated": evaluated_count,
        "decision_bars_skipped": skipped_count,
        "latest_evaluated_decision_bar_timestamp": latest_evaluated,
        "latest_completed_live_5m_bar_timestamp": latest_completed,
        "monitor_caught_up_to_latest_completed_bar": latest_bar_caught_up,
        "no_signal_count": total_no_signal,
        "signal_count": total_signal,
        "suppressed_signal_count": total_suppressed,
        "decision_bars": decision_bars,
    }


def _latest_live_completed_5m_path(repo_root: Path, instrument_family: str) -> Path:
    return (
        repo_root
        / "outputs"
        / "track_b_execution_core"
        / "databento_live_runtime_feed"
        / f"latest_live_{instrument_family.lower()}_completed_5m_candles.json"
    )


def _completed_5m_bars(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("candles", "bars", "completed_5m_candles"):
        raw = payload.get(key)
        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, Mapping)]
    return []


def _skipped_decision_bar_reason(rows: list[tuple[Mapping[str, Any], Mapping[str, Any]]]) -> str:
    if not rows:
        return "NO_MONITOR_REPORT_FOR_COMPLETED_BAR"
    blockers = [
        str(row.get("primary_blocker") or report.get("primary_blocker") or "")
        for report, row in rows
        if row.get("primary_blocker") or report.get("primary_blocker")
    ]
    if not any(row.get("paper_evaluation_allowed") is True for _, row in rows):
        return f"PAPER_NOT_ALLOWED_DURING_BAR: {blockers[0]}" if blockers else "PAPER_NOT_ALLOWED_DURING_BAR"
    verdicts = [str(row.get("instrument_verdict") or report.get("monitor_verdict") or "") for report, row in rows]
    heartbeat = next((item for item in verdicts if "HEARTBEAT_NO_NEW_COMPLETED_BAR" in item), None)
    if heartbeat:
        return "ELIGIBLE_COMPLETED_BAR_HEARTBEAT_WITHOUT_EVALUATION"
    return "ELIGIBLE_COMPLETED_BAR_NOT_EVALUATED"


def _classify_completed_decision_bar_instrument(
    *,
    completed_count: int,
    eligible_count: int,
    skipped_count: int,
    latest_completed: str | None,
    latest_evaluated: str | None,
) -> str:
    if completed_count <= 0:
        return "NO_COMPLETED_BARS_IN_WINDOW"
    if eligible_count <= 0:
        return "PAPER_NOT_ALLOWED_DURING_BARS"
    if skipped_count > 0 and latest_completed and latest_evaluated and latest_completed > latest_evaluated:
        return "EVALUATION_LAGGING_LIVE_BARS"
    if skipped_count > 0:
        return "COMPLETED_BARS_SKIPPED"
    if latest_completed and latest_completed == latest_evaluated:
        return "EVALUATING_EACH_COMPLETED_BAR"
    return "DIAGNOSTIC_INCONCLUSIVE"


def _classify_completed_decision_bar_audit(instruments: Mapping[str, Any]) -> str:
    if not instruments:
        return "DIAGNOSTIC_INCONCLUSIVE"
    classifications = [str(row.get("classification") or "DIAGNOSTIC_INCONCLUSIVE") for row in instruments.values()]
    if all(item == "NO_COMPLETED_BARS_IN_WINDOW" for item in classifications):
        return "NO_COMPLETED_BARS_IN_WINDOW"
    if any(item == "EVALUATION_LAGGING_LIVE_BARS" for item in classifications):
        return "EVALUATION_LAGGING_LIVE_BARS"
    if any(item == "COMPLETED_BARS_SKIPPED" for item in classifications):
        return "COMPLETED_BARS_SKIPPED"
    if any(item == "PAPER_NOT_ALLOWED_DURING_BARS" for item in classifications):
        return "PAPER_NOT_ALLOWED_DURING_BARS"
    if all(item in {"EVALUATING_EACH_COMPLETED_BAR", "NO_COMPLETED_BARS_IN_WINDOW"} for item in classifications):
        return "EVALUATING_EACH_COMPLETED_BAR"
    return "DIAGNOSTIC_INCONCLUSIVE"


def _build_no_signal_attribution_rollup(
    *,
    repo_root: Path,
    now: datetime,
    completed_decision_bar_audit: Mapping[str, Any],
    decision_bar_limit: int,
) -> dict[str, Any]:
    require_aware_datetime(now, "now")
    limit = max(1, int(decision_bar_limit))
    selected_bars = _evaluated_decision_bars_from_audit(completed_decision_bar_audit)[-limit:]
    instrument_rows: dict[str, dict[str, Any]] = {}
    strategy_rows: dict[tuple[str, str], dict[str, Any]] = {}
    evaluated_entries: list[dict[str, Any]] = []
    total_signals = 0
    total_no_signals = 0
    total_suppressed = 0
    total_handoffs = 0
    attribution_missing = 0
    for bar in selected_bars:
        instrument = str(bar.get("instrument") or "UNKNOWN")
        runtime_reports = [
            _load_json(_resolve_artifact_path(repo_root, path))
            for path in bar.get("runtime_cycle_report_paths") or []
            if path
        ]
        for runtime in runtime_reports:
            suppressed_ids = {
                str(item.get("strategy_id") or item.get("signal_source") or "")
                for item in runtime.get("suppressed_signals") or []
                if isinstance(item, Mapping)
            }
            candidate_ids = {
                str(item.get("strategy_id") or item.get("signal_source") or "")
                for item in runtime.get("candidate_signals") or []
                if isinstance(item, Mapping)
            }
            total_suppressed += len(runtime.get("suppressed_signals") or [])
            if runtime.get("paper_runner_report_path") or runtime.get("paper_proof_invoked") or runtime.get("submit_attempted"):
                total_handoffs += 1
            instrument_row = instrument_rows.setdefault(instrument, _empty_no_signal_instrument_row(instrument, bar))
            instrument_row["evaluated_decision_bars"] = max(
                int(instrument_row.get("evaluated_decision_bars") or 0),
                len({*(instrument_row.get("_decision_bar_timestamps") or []), str(bar.get("decision_bar_timestamp"))}),
            )
            instrument_row.setdefault("_decision_bar_timestamps", set()).add(str(bar.get("decision_bar_timestamp")))
            for strategy in runtime.get("evaluated_strategies") or []:
                if not isinstance(strategy, Mapping):
                    continue
                strategy_id = str(strategy.get("strategy_id") or strategy.get("signal_source") or "UNKNOWN")
                result = _strategy_result(strategy, suppressed_ids=suppressed_ids, candidate_ids=candidate_ids)
                blockers = _strategy_failed_predicates(strategy)
                missing_fields = _strategy_missing_fields(strategy, blockers)
                near_miss = _strategy_near_miss_summary(strategy, blockers)
                if result == "SIGNAL":
                    total_signals += 1
                elif result == "NO_SIGNAL":
                    total_no_signals += 1
                if result == "SUPPRESSED":
                    total_suppressed += 1
                if result == "NO_SIGNAL" and not blockers:
                    attribution_missing += 1
                instrument_row["total_no_signals"] = int(instrument_row.get("total_no_signals") or 0) + (1 if result == "NO_SIGNAL" else 0)
                instrument_row["total_signals"] = int(instrument_row.get("total_signals") or 0) + (1 if result == "SIGNAL" else 0)
                instrument_row["total_suppressed"] = int(instrument_row.get("total_suppressed") or 0) + (1 if result == "SUPPRESSED" else 0)
                instrument_row.setdefault("_strategy_ids", set()).add(strategy_id)
                _update_counter(instrument_row.setdefault("_failed_predicates", Counter()), blockers)
                if near_miss["near_miss_bucket"] in {"ONE_PREDICATE_AWAY", "TWO_PREDICATES_AWAY"}:
                    instrument_row.setdefault("_near_misses", []).append(
                        _compact_near_miss_example(instrument=instrument, strategy_id=strategy_id, bar=bar, near_miss=near_miss)
                    )
                strategy_key = (instrument, strategy_id)
                strategy_row = strategy_rows.setdefault(strategy_key, _empty_no_signal_strategy_row(instrument, strategy_id))
                strategy_row["evaluated_bars"] = int(strategy_row.get("evaluated_bars") or 0) + 1
                strategy_row["signal_count"] = int(strategy_row.get("signal_count") or 0) + (1 if result == "SIGNAL" else 0)
                strategy_row["no_signal_count"] = int(strategy_row.get("no_signal_count") or 0) + (1 if result == "NO_SIGNAL" else 0)
                strategy_row["suppressed_count"] = int(strategy_row.get("suppressed_count") or 0) + (1 if result == "SUPPRESSED" else 0)
                strategy_row["handoff_count"] = int(strategy_row.get("handoff_count") or 0) + (1 if total_handoffs else 0)
                _update_counter(strategy_row.setdefault("_failed_predicates", Counter()), blockers)
                _update_counter(strategy_row.setdefault("_missing_fields", Counter()), missing_fields)
                _update_categorized_blockers(strategy_row, blockers)
                if near_miss["near_miss_bucket"] in {"ONE_PREDICATE_AWAY", "TWO_PREDICATES_AWAY"}:
                    strategy_row["nearest_miss_count"] = int(strategy_row.get("nearest_miss_count") or 0) + 1
                    strategy_row.setdefault("_near_misses", []).append(
                        _compact_near_miss_example(instrument=instrument, strategy_id=strategy_id, bar=bar, near_miss=near_miss)
                    )
                evaluated_entries.append(
                    {
                        "decision_bar_timestamp": bar.get("decision_bar_timestamp"),
                        "instrument": instrument,
                        "strategy_id": strategy_id,
                        "result": result,
                        "failed_predicates": blockers,
                        "passed_predicates_count": near_miss.get("passed_predicates_count"),
                        "failed_predicates_count": near_miss.get("failed_predicates_count"),
                        "missing_fields": missing_fields,
                        "near_miss_score": near_miss.get("near_miss_score"),
                        "near_miss_bucket": near_miss.get("near_miss_bucket"),
                        "nearest_failed_predicate": near_miss.get("nearest_failed_predicate"),
                        "candidate_reason": strategy.get("decision_reason") if result == "SIGNAL" else None,
                        "suppression_reason": _suppression_reason(strategy_id, runtime) if result == "SUPPRESSED" else None,
                    }
                )

    compact_instruments = [_finalize_no_signal_instrument_row(row) for row in instrument_rows.values()]
    compact_strategies = [_finalize_no_signal_strategy_row(row) for row in strategy_rows.values()]
    all_failed = Counter()
    for row in instrument_rows.values():
        all_failed.update(row.get("_failed_predicates") or {})
    classification = _classify_no_signal_attribution_rollup(
        evaluated_bars=len(selected_bars),
        total_evaluations=len(evaluated_entries),
        total_signals=total_signals,
        total_no_signals=total_no_signals,
        attribution_missing=attribution_missing,
    )
    timestamps = [str(item.get("decision_bar_timestamp")) for item in selected_bars if item.get("decision_bar_timestamp")]
    return {
        "schema_version": "track_b_no_signal_attribution_rollup_v1",
        "generated_at": now.isoformat(),
        "window_start": timestamps[0] if timestamps else None,
        "window_end": timestamps[-1] if timestamps else None,
        "decision_bar_limit": limit,
        "completed_decision_bars_observed": len(selected_bars),
        "eligible_decision_bars": sum(1 for item in selected_bars if item.get("paper_evaluation_allowed") is True),
        "evaluated_decision_bars": len(selected_bars),
        "total_strategy_evaluations": len(evaluated_entries),
        "total_signals": total_signals,
        "total_no_signals": total_no_signals,
        "total_suppressed": total_suppressed,
        "total_handoffs": total_handoffs,
        "classification": classification,
        "attribution_complete": bool(evaluated_entries) and attribution_missing == 0,
        "attribution_missing_count": attribution_missing,
        "top_failed_predicates": _counter_rows(all_failed, limit=20),
        "closest_near_misses": _top_near_miss_examples(compact_strategies),
        "instruments": sorted(compact_instruments, key=lambda item: str(item.get("instrument"))),
        "strategies": sorted(compact_strategies, key=lambda item: (str(item.get("instrument")), str(item.get("strategy_id")))),
        "evaluated_decision_bar_records": evaluated_entries[-200:],
        "full_paper_trade_ledger_scanned": False,
        "full_decision_journal_scanned": False,
    }


def _evaluated_decision_bars_from_audit(audit: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    instruments = audit.get("instruments") if isinstance(audit.get("instruments"), Mapping) else {}
    for instrument, row in instruments.items():
        if not isinstance(row, Mapping):
            continue
        for bar in row.get("decision_bars") or []:
            if not isinstance(bar, Mapping) or bar.get("evaluated") is not True:
                continue
            rows.append(
                {
                    "instrument": str(instrument),
                    "decision_bar_timestamp": bar.get("completed_5m_timestamp"),
                    "paper_evaluation_allowed": bar.get("paper_evaluation_allowed"),
                    "runtime_cycle_report_paths": bar.get("runtime_cycle_report_paths") or [],
                    "enabled_strategies": row.get("enabled_strategies") or [],
                }
            )
    return sorted(rows, key=lambda item: (str(item.get("decision_bar_timestamp") or ""), str(item.get("instrument") or "")))


def _resolve_artifact_path(repo_root: Path, raw_path: object) -> Path:
    path = Path(str(raw_path))
    return path if path.is_absolute() else repo_root / path


def _empty_no_signal_instrument_row(instrument: str, bar: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "instrument": instrument,
        "eligible_decision_bars": 0,
        "evaluated_decision_bars": 0,
        "strategies_enabled": bar.get("enabled_strategies") or [],
        "strategies_evaluated": 0,
        "total_no_signals": 0,
        "total_signals": 0,
        "total_suppressed": 0,
        "total_handoffs": 0,
        "window_start": bar.get("decision_bar_timestamp"),
        "window_end": bar.get("decision_bar_timestamp"),
    }


def _empty_no_signal_strategy_row(instrument: str, strategy_id: str) -> dict[str, Any]:
    return {
        "strategy_id": strategy_id,
        "instrument": instrument,
        "evaluated_bars": 0,
        "signal_count": 0,
        "no_signal_count": 0,
        "suppressed_count": 0,
        "handoff_count": 0,
        "nearest_miss_count": 0,
    }


def _strategy_failed_predicates(strategy: Mapping[str, Any]) -> list[str]:
    blockers = [str(item) for item in strategy.get("rule_blockers") or [] if item]
    if blockers:
        return [_predicate_name_from_blocker(item) for item in blockers]
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    return [str(key) for key, value in conditions.items() if value is False]


def _predicate_name_from_blocker(blocker: str) -> str:
    return blocker.split("=", 1)[0].strip() or blocker


def _strategy_missing_fields(strategy: Mapping[str, Any], blockers: list[str]) -> list[str]:
    missing = []
    for item in blockers:
        lowered = item.lower()
        if "missing" in lowered or "required" in lowered:
            missing.append(item)
    reason = str(strategy.get("primary_blocker") or strategy.get("decision_reason") or "")
    if "missing" in reason.lower():
        missing.append(reason)
    return missing


def _strategy_near_miss_summary(strategy: Mapping[str, Any], blockers: list[str]) -> dict[str, Any]:
    conditions = strategy.get("rule_conditions") if isinstance(strategy.get("rule_conditions"), Mapping) else {}
    passed = sum(1 for value in conditions.values() if value is True) if conditions else None
    failed = len(blockers)
    total = (passed or 0) + failed if passed is not None else None
    score = round(float(passed) / float(total), 4) if passed is not None and total else None
    if failed == 1:
        bucket = "ONE_PREDICATE_AWAY"
    elif failed == 2:
        bucket = "TWO_PREDICATES_AWAY"
    elif failed > 2:
        bucket = "MULTI_PREDICATE_FAIL"
    else:
        bucket = "NOT_SCORABLE"
    return {
        "passed_predicates_count": passed,
        "failed_predicates_count": failed,
        "near_miss_score": score,
        "near_miss_bucket": bucket,
        "nearest_failed_predicate": blockers[0] if blockers else None,
    }


def _compact_near_miss_example(
    *,
    instrument: str,
    strategy_id: str,
    bar: Mapping[str, Any],
    near_miss: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "instrument": instrument,
        "strategy_id": strategy_id,
        "decision_bar_timestamp": bar.get("decision_bar_timestamp"),
        "near_miss_bucket": near_miss.get("near_miss_bucket"),
        "near_miss_score": near_miss.get("near_miss_score"),
        "nearest_failed_predicate": near_miss.get("nearest_failed_predicate"),
        "failed_predicates_count": near_miss.get("failed_predicates_count"),
        "passed_predicates_count": near_miss.get("passed_predicates_count"),
    }


def _update_counter(counter: Counter[str], values: list[str]) -> None:
    for value in values:
        if value:
            counter[str(value)] += 1


def _update_categorized_blockers(row: dict[str, Any], blockers: list[str]) -> None:
    for blocker in blockers:
        category = _predicate_category(blocker)
        bucket = row.setdefault(f"_{category}_blockers", Counter())
        bucket[str(blocker)] += 1


def _predicate_category(predicate: str) -> str:
    lowered = predicate.lower()
    if "missing" in lowered or "required" in lowered:
        return "missing_field"
    if "session" in lowered or "phase" in lowered or "window" in lowered:
        return "session"
    if "snap" in lowered or "breakout" in lowered or "retest" in lowered or "pullback" in lowered or "resume" in lowered:
        return "structure"
    if "cooldown" in lowered or "prior_bars" in lowered or "competing" in lowered:
        return "state"
    if "readiness" in lowered or "safety" in lowered or "allowed" in lowered or "eligible" in lowered:
        return "safety_or_readiness"
    return "structure"


def _suppression_reason(strategy_id: str, runtime: Mapping[str, Any]) -> str | None:
    for item in runtime.get("suppressed_signals") or []:
        if not isinstance(item, Mapping):
            continue
        if str(item.get("strategy_id") or item.get("signal_source") or "") == strategy_id:
            return str(item.get("reason") or item.get("primary_blocker") or "suppressed")
    return None


def _finalize_no_signal_instrument_row(row: dict[str, Any]) -> dict[str, Any]:
    strategy_ids = row.get("_strategy_ids") or set()
    return {
        "instrument": row.get("instrument"),
        "eligible_decision_bars": len(row.get("_decision_bar_timestamps") or []),
        "evaluated_decision_bars": int(row.get("evaluated_decision_bars") or 0),
        "strategies_enabled": row.get("strategies_enabled") or [],
        "strategies_evaluated": len(strategy_ids) or None,
        "total_no_signals": row.get("total_no_signals", 0),
        "total_signals": row.get("total_signals", 0),
        "total_suppressed": row.get("total_suppressed", 0),
        "total_handoffs": row.get("total_handoffs", 0),
        "top_failed_predicates": _counter_rows(row.get("_failed_predicates") or Counter(), limit=10),
        "closest_near_misses": (row.get("_near_misses") or [])[:5],
    }


def _finalize_no_signal_strategy_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_id": row.get("strategy_id"),
        "instrument": row.get("instrument"),
        "evaluated_bars": row.get("evaluated_bars", 0),
        "signal_count": row.get("signal_count", 0),
        "no_signal_count": row.get("no_signal_count", 0),
        "suppressed_count": row.get("suppressed_count", 0),
        "handoff_count": row.get("handoff_count", 0),
        "top_failed_predicates": _counter_rows(row.get("_failed_predicates") or Counter(), limit=10),
        "top_missing_fields": _counter_rows(row.get("_missing_field_blockers") or Counter(), limit=10),
        "top_session_blockers": _counter_rows(row.get("_session_blockers") or Counter(), limit=10),
        "top_structure_blockers": _counter_rows(row.get("_structure_blockers") or Counter(), limit=10),
        "top_state_blockers": _counter_rows(row.get("_state_blockers") or Counter(), limit=10),
        "top_safety_or_readiness_blockers": _counter_rows(row.get("_safety_or_readiness_blockers") or Counter(), limit=10),
        "nearest_miss_count": row.get("nearest_miss_count", 0),
        "nearest_miss_examples": (row.get("_near_misses") or [])[:5],
    }


def _top_near_miss_examples(strategy_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for row in strategy_rows:
        examples.extend([item for item in row.get("nearest_miss_examples") or [] if isinstance(item, dict)])
    return sorted(
        examples,
        key=lambda item: (
            int(item.get("failed_predicates_count") or 999),
            -float(item.get("near_miss_score") or 0.0),
            str(item.get("decision_bar_timestamp") or ""),
        ),
    )[:10]


def _classify_no_signal_attribution_rollup(
    *,
    evaluated_bars: int,
    total_evaluations: int,
    total_signals: int,
    total_no_signals: int,
    attribution_missing: int,
) -> str:
    if evaluated_bars <= 0:
        return "INSUFFICIENT_DECISION_BARS"
    if total_evaluations <= 0:
        return "STRATEGIES_NOT_EVALUATING"
    if total_signals > 0:
        return "SIGNALS_OBSERVED"
    if total_no_signals > 0 and attribution_missing > 0:
        return "NO_SIGNAL_BUT_ATTRIBUTION_MISSING"
    if total_no_signals > 0:
        return "NO_SIGNAL_WITH_ATTRIBUTION"
    return "DIAGNOSTIC_INCONCLUSIVE"


def _classify_diagnosis(
    *,
    latest_monitor: Mapping[str, Any],
    cycle_summary: Mapping[str, Any],
    strategy_summary: Mapping[str, Any],
    signals: Mapping[str, Any],
    ledger: Mapping[str, Any],
    live_feed: Mapping[str, Any],
    completed_decision_bar_audit: Mapping[str, Any],
) -> tuple[str, str]:
    cycle_count = int(cycle_summary.get("recent_monitor_cycle_count") or 0)
    eval_cycles = int(cycle_summary.get("recent_evaluation_cycle_count") or 0)
    stale_cycles = int(cycle_summary.get("recent_stale_cycle_count") or 0)
    heartbeat_only_cycles = int(cycle_summary.get("recent_heartbeat_only_cycle_count") or 0)
    provider_blocked_cycles = int(cycle_summary.get("recent_provider_blocked_cycle_count") or 0)
    signal_count = int(cycle_summary.get("recent_candidate_signal_count") or 0)
    signal_seen = signal_count > 0 or bool(signals.get("latest_signal_candidate"))
    handoff_count = int(signals.get("paper_handoff_attempt_count") or 0)
    not_ready_total = sum(
        int(counts.get("not_ready") or 0)
        for counts in strategy_summary.get("per_strategy_recent_result_counts", {}).values()
        if isinstance(counts, Mapping)
    )
    if not cycle_count:
        return (
            "STUCK_BEFORE_EVALUATION",
            "No recent Track B monitor cycle reports were found; inspect launchd process, monitor lock, and heartbeat writer.",
        )
    if provider_blocked_cycles > 0 and eval_cycles == 0:
        return (
            "PROVIDER_OR_LIVE_FEED_BLOCKED",
            "Provider or Live feed readiness blocked all recent evaluation cycles; inspect Live feed diagnostics and provider errors.",
        )
    if "STALE" in str(latest_monitor.get("monitor_verdict") or "") and stale_cycles >= max(1, eval_cycles):
        return (
            "STALE_LIVE_FEED",
            "Live artifacts are connected/warm but execution freshness is frequently failing; inspect Databento Live ohlcv delivery latency and monitor max-latest-1m-age-seconds.",
        )
    if stale_cycles > 0:
        return (
            "LIVE_EXECUTION_INTERMITTENT",
            "Some recent cycles evaluated, but execution freshness also failed in the diagnostic window; inspect exact stale 1m/5m blockers before treating zero trades as normal.",
        )
    if eval_cycles == 0 and not_ready_total > 0:
        return (
            "INPUTS_NOT_READY",
            "Strategies did not evaluate because required envelopes/fields are repeatedly NOT_READY; inspect top_not_ready_reasons.",
        )
    if eval_cycles == 0:
        return (
            "STUCK_BEFORE_EVALUATION",
            "The monitor is cycling but no strategy evaluation occurred in the diagnostic window.",
        )
    if signal_seen and signals.get("top_suppression_or_arbitration_reasons"):
        return (
            "SIGNAL_SUPPRESSED",
            "At least one candidate signal was seen but arbitration suppressed or blocked it.",
        )
    if signal_seen and handoff_count == 0:
        return (
            "PAPER_HANDOFF_BLOCKED",
            "Candidate signals were seen, but no guarded PAPER handoff attempt was recorded.",
        )
    if handoff_count and int(ledger.get("paper_trades_attempted_count") or 0) == 0:
        return (
            "LEDGER_WRITE_BLOCKED",
            "A PAPER handoff attempt appears in recent reports, but compact trade summaries still show zero trades.",
        )
    if not_ready_total > 0 and not_ready_total >= int(cycle_summary.get("recent_strategy_evaluation_count") or 0):
        return (
            "INPUTS_NOT_READY",
            "Strategies are being evaluated but repeatedly return NOT_READY; inspect top_not_ready_reasons.",
        )
    audit_classification = str(completed_decision_bar_audit.get("classification") or "")
    if heartbeat_only_cycles > 0 and eval_cycles > 0 and audit_classification == "EVALUATING_EACH_COMPLETED_BAR":
        return (
            "NORMAL_NO_SIGNAL",
            "Completed decision-bar audit shows every eligible completed bar was evaluated and recent evaluated bars produced no candidate signals.",
        )
    if heartbeat_only_cycles > 0 and eval_cycles > 0 and audit_classification in {
        "COMPLETED_BARS_SKIPPED",
        "EVALUATION_LAGGING_LIVE_BARS",
        "PAPER_NOT_ALLOWED_DURING_BARS",
    }:
        return (
            audit_classification,
            "Completed decision-bar audit found that monitor-cycle heartbeats are hiding missed, lagging, or ineligible completed decision bars.",
        )
    if heartbeat_only_cycles > 0 and eval_cycles > 0:
        return (
            "NO_NEW_COMPLETED_5M_BAR_HEARTBEAT",
            "The monitor is eligible and heartbeating between completed-bar evaluations; wait for the next completed decision bar or inspect latest evaluated strategy blockers.",
        )
    return (
        "NORMAL_NO_SIGNAL",
        "Strategy evaluation is reachable and recent evaluated cycles produced no candidate signals; inspect top predicate blockers and near-miss summaries before changing strategy coverage.",
    )


def _dominant_blocker(
    cycle_summary: Mapping[str, Any],
    strategy_summary: Mapping[str, Any],
    signals: Mapping[str, Any],
    live_feed: Mapping[str, Any],
) -> str | None:
    stale = live_feed.get("stale_or_not_fresh_instruments") or []
    if stale:
        return f"Execution freshness failing for: {', '.join(str(item) for item in stale)}"
    for bucket in (
        cycle_summary.get("top_cycle_blockers"),
        strategy_summary.get("top_not_ready_reasons"),
        strategy_summary.get("top_no_signal_predicate_blockers"),
        signals.get("top_suppression_or_arbitration_reasons"),
    ):
        if isinstance(bucket, list) and bucket:
            first = bucket[0]
            if isinstance(first, Mapping):
                return str(first.get("reason") or first.get("value"))
    return None


def _strategy_result(
    strategy: Mapping[str, Any],
    *,
    suppressed_ids: set[str],
    candidate_ids: set[str],
) -> str:
    strategy_id = str(strategy.get("strategy_id") or strategy.get("signal_source") or "")
    verdict = str(strategy.get("strategy_runtime_verdict") or strategy.get("strategy_rule_runner_verdict") or "")
    if strategy_id in suppressed_ids:
        return "SUPPRESSED"
    if strategy.get("signal_emitted") is True or strategy_id in candidate_ids or "SIGNAL_READY" in verdict:
        return "SIGNAL"
    if "NOT_READY" in verdict or str(strategy.get("decision") or "").upper() == "NOT_READY":
        return "NOT_READY"
    if "STALE" in verdict or "stale" in str(strategy.get("primary_blocker") or "").lower():
        return "STALE_INPUT"
    if "NO_SIGNAL" in verdict or str(strategy.get("decision") or "").upper() == "NO_SIGNAL":
        return "NO_SIGNAL"
    if "BLOCKED" in verdict:
        return "SAFETY_BLOCKED"
    return "NO_SIGNAL"


def _compact_cycle(report: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "cycle_index": report.get("cycle_index"),
        "cycle_id": report.get("cycle_id"),
        "completed_at": report.get("completed_at"),
        "monitor_verdict": report.get("monitor_verdict"),
        "evaluated_strategy_count": report.get("evaluated_strategy_count"),
        "candidate_signal_count": len(report.get("candidate_signals") or []),
        "suppressed_signal_count": len(report.get("suppressed_signals") or []),
        "primary_blocker": report.get("primary_blocker"),
    }


def _cycle_is_stale(report: Mapping[str, Any]) -> bool:
    verdict = str(report.get("monitor_verdict") or "")
    blocker = str(report.get("primary_blocker") or "")
    if "STALE" in verdict or "stale" in blocker.lower():
        return True
    return any(
        item.get("enabled_strategies")
        and item.get("fresh_for_execution") is False
        and item.get("live_feed_connected") is True
        for item in _instrument_reports(report)
    )


def _instrument_reports(report: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = report.get("instrument_reports")
    return [item for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _counter_rows(counter: Counter[str], limit: int = 10) -> list[dict[str, Any]]:
    return [
        {"reason": key, "count": int(value)}
        for key, value in counter.most_common(limit)
        if key and key != "NONE"
    ]


def _load_json(path: Path) -> dict[str, Any]:
    try:
        if not path.exists() or not path.is_file() or path.stat().st_size > MAX_REPORT_BYTES:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _mtime_datetime(path: Path) -> datetime | None:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, UTC)
    except OSError:
        return None


def _age_seconds(value: object, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return round(max(0.0, (now.astimezone(UTC) - parsed).total_seconds()), 3)


def monitor_pid_is_running(pid: object) -> bool | None:
    try:
        actual_pid = int(pid)
    except (TypeError, ValueError):
        return None
    if actual_pid <= 0:
        return False
    try:
        os.kill(actual_pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True
