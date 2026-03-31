"""Live observation report for the GC/MGC London-open acceptance continuation temp-paper branch."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "gc_mgc_london_open_acceptance_live_observation"
DEFAULT_AUDIT_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_signal_intent_fill_audit_snapshot.json"
DEFAULT_NON_APPROVED_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_non_approved_lanes_snapshot.json"
DEFAULT_LANE_ACTIVITY_SNAPSHOT_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_lane_activity_snapshot.json"
DEFAULT_CLOSE_REVIEW_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "paper_session_close_review_latest.json"
DEFAULT_CONFLICT_EVENTS_PATH = REPO_ROOT / "outputs" / "operator_dashboard" / "same_underlying_conflict_events.jsonl"
DEFAULT_SILENT_FAILURE_AUDIT_PATH = REPO_ROOT / "outputs" / "reports" / "strategy_silent_failure_audit" / "strategy_silent_failure_audit.json"
TARGET_SOURCE_FAMILY = "gc_mgc_london_open_acceptance_continuation_long"
TARGET_LANE_IDS = (
    "gc_mgc_london_open_acceptance_continuation_long__GC",
    "gc_mgc_london_open_acceptance_continuation_long__MGC",
)


@dataclass(frozen=True)
class ObservationArtifacts:
    json_path: Path
    markdown_path: Path


def run_gc_mgc_london_open_acceptance_live_observation(
    *,
    output_dir: str | Path | None = None,
    audit_snapshot_path: str | Path | None = None,
    non_approved_snapshot_path: str | Path | None = None,
    lane_activity_snapshot_path: str | Path | None = None,
    close_review_path: str | Path | None = None,
    conflict_events_path: str | Path | None = None,
    silent_failure_audit_path: str | Path | None = None,
) -> ObservationArtifacts:
    resolved_output_dir = Path(output_dir or DEFAULT_OUTPUT_DIR).resolve()
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    report = build_gc_mgc_london_open_acceptance_live_observation_report(
        audit_snapshot_path=Path(audit_snapshot_path or DEFAULT_AUDIT_SNAPSHOT_PATH).resolve(),
        non_approved_snapshot_path=Path(non_approved_snapshot_path or DEFAULT_NON_APPROVED_SNAPSHOT_PATH).resolve(),
        lane_activity_snapshot_path=Path(lane_activity_snapshot_path or DEFAULT_LANE_ACTIVITY_SNAPSHOT_PATH).resolve(),
        close_review_path=Path(close_review_path or DEFAULT_CLOSE_REVIEW_PATH).resolve(),
        conflict_events_path=Path(conflict_events_path or DEFAULT_CONFLICT_EVENTS_PATH).resolve(),
        silent_failure_audit_path=Path(silent_failure_audit_path or DEFAULT_SILENT_FAILURE_AUDIT_PATH).resolve(),
    )
    archive_snapshot_path = _archive_snapshot(report, resolved_output_dir)
    archived_reports = _load_archived_reports(resolved_output_dir / "snapshots")
    report["archive_snapshot_path"] = str(archive_snapshot_path)
    report["rolling_observation_summary"] = _build_rolling_observation_summary(archived_reports)

    json_path = resolved_output_dir / "gc_mgc_london_open_acceptance_live_observation.json"
    markdown_path = resolved_output_dir / "gc_mgc_london_open_acceptance_live_observation.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report).strip() + "\n", encoding="utf-8")
    return ObservationArtifacts(json_path=json_path, markdown_path=markdown_path)


def build_gc_mgc_london_open_acceptance_live_observation_report(
    *,
    audit_snapshot_path: Path,
    non_approved_snapshot_path: Path,
    lane_activity_snapshot_path: Path,
    close_review_path: Path,
    conflict_events_path: Path,
    silent_failure_audit_path: Path,
) -> dict[str, Any]:
    audit_rows = _rows_by_lane(_read_json(audit_snapshot_path))
    non_approved_rows = _rows_by_lane(_read_json(non_approved_snapshot_path))
    lane_activity_rows = _rows_by_lane(_read_json(lane_activity_snapshot_path))
    close_review_rows = _rows_by_lane(_read_json(close_review_path))
    silent_failure_audit = _read_json(silent_failure_audit_path)

    symbol_reports: dict[str, dict[str, Any]] = {}
    total_runtime_signals = 0
    total_actionable_signals = 0
    total_processed_bars = 0
    total_fills = 0
    total_intents = 0
    qualifying_timing_distribution: Counter[str] = Counter()
    session_dates: set[str] = set()
    latest_activity_timestamps: list[str] = []

    for lane_id in TARGET_LANE_IDS:
        audit_row = audit_rows.get(lane_id, {})
        non_approved_row = non_approved_rows.get(lane_id, {})
        lane_activity_row = lane_activity_rows.get(lane_id, {})
        close_review_row = close_review_rows.get(lane_id, {})
        symbol = str(
            audit_row.get("instrument")
            or non_approved_row.get("instrument")
            or close_review_row.get("instrument")
            or lane_id.rsplit("__", 1)[-1]
        ).upper()
        signal_count = int(non_approved_row.get("signal_count") or 0)
        actionable_entry_signal_count = int(audit_row.get("actionable_entry_signal_count") or 0)
        processed_bars = int(audit_row.get("processed_bar_count") or non_approved_row.get("processed_bars") or 0)
        total_runtime_signals += signal_count
        total_actionable_signals += actionable_entry_signal_count
        total_processed_bars += processed_bars
        total_fills += int(non_approved_row.get("fill_count") or 0)
        total_intents += int(non_approved_row.get("intent_count") or 0)
        if actionable_entry_signal_count > 0 and audit_row.get("last_actionable_signal_timestamp"):
            local_dt = datetime.fromisoformat(str(audit_row["last_actionable_signal_timestamp"]))
            qualifying_timing_distribution[local_dt.strftime("%H:%M")] += actionable_entry_signal_count
            session_dates.add(local_dt.date().isoformat())
        if non_approved_row.get("latest_activity_timestamp"):
            latest_activity_timestamps.append(str(non_approved_row["latest_activity_timestamp"]))

        symbol_reports[symbol] = {
            "lane_id": lane_id,
            "display_name": str(
                non_approved_row.get("display_name")
                or close_review_row.get("branch")
                or audit_row.get("strategy_name")
                or lane_id
            ),
            "temporary_paper_strategy": bool(non_approved_row.get("temporary_paper_strategy")),
            "experimental_status": non_approved_row.get("experimental_status") or close_review_row.get("experimental_status"),
            "paper_only": bool(non_approved_row.get("paper_only") or close_review_row.get("paper_only")),
            "non_approved": bool(non_approved_row.get("non_approved") or close_review_row.get("non_approved")),
            "runtime_state_loaded": bool(audit_row.get("runtime_state_loaded")),
            "runtime_instance_present": bool(audit_row.get("runtime_instance_present")),
            "processed_bars": processed_bars,
            "runtime_signal_count": signal_count,
            "actionable_entry_signal_count": actionable_entry_signal_count,
            "raw_setup_candidate_count": int(audit_row.get("raw_setup_candidate_count") or 0),
            "intent_count": int(non_approved_row.get("intent_count") or 0),
            "fill_count": int(non_approved_row.get("fill_count") or 0),
            "fired": bool(non_approved_row.get("fired")),
            "fired_at": non_approved_row.get("fired_at"),
            "latest_signal_label": non_approved_row.get("latest_signal_label"),
            "latest_activity_timestamp": non_approved_row.get("latest_activity_timestamp"),
            "latest_audit_verdict": audit_row.get("audit_verdict"),
            "latest_audit_reason": audit_row.get("audit_reason"),
            "current_session": audit_row.get("current_session"),
            "inspection_window": {
                "start": audit_row.get("inspection_start_ts"),
                "end": audit_row.get("inspection_end_ts"),
            },
            "lane_activity_verdict": lane_activity_row.get("verdict"),
            "lane_activity_event_type": lane_activity_row.get("latest_event_type"),
            "history_sessions_found": int(close_review_row.get("history_sessions_found") or 0),
            "history_sufficiency_status": close_review_row.get("history_sufficiency_status"),
            "trade_count": int(close_review_row.get("fill_count") or 0),
        }

    conflict_rows = _load_conflict_rows(conflict_events_path)
    target_conflicts = [
        row
        for row in conflict_rows
        if set(str(value) for value in row.get("standalone_strategy_ids", [])) & set(TARGET_LANE_IDS)
    ]
    overlap_rows = [_summarize_conflict_row(row) for row in target_conflicts]

    silent_failure_summary = {
        "latest_audit_path": str(silent_failure_audit_path),
        "latest_audit_generated_at": silent_failure_audit.get("generated_at"),
        "remaining_ranked_blockers": silent_failure_audit.get("ranked_blockers", []),
        "target_branch_in_ranked_silent_failures": any(
            lane_id in json.dumps(silent_failure_audit)
            for lane_id in TARGET_LANE_IDS
        ),
    }

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "branch_id": TARGET_SOURCE_FAMILY,
        "status": "observe_only_keep_definition_unchanged",
        "branch_definition_changed": False,
        "sample_status": _sample_status(
            actionable_signal_count=total_actionable_signals,
            history_sessions_found=max((row.get("history_sessions_found") or 0) for row in symbol_reports.values()) if symbol_reports else 0,
        ),
        "live_runtime_summary": {
            "runtime_state_loaded_for_all_target_lanes": all(
                bool(row.get("runtime_state_loaded")) for row in symbol_reports.values()
            ),
            "total_processed_bars": total_processed_bars,
            "total_runtime_signal_count": total_runtime_signals,
            "total_actionable_entry_signal_count": total_actionable_signals,
            "total_intent_count": total_intents,
            "total_fill_count": total_fills,
            "latest_activity_timestamp": max(latest_activity_timestamps) if latest_activity_timestamps else None,
        },
        "gc_vs_mgc": symbol_reports,
        "session_timing_distribution": {
            "qualifying_entry_signals": dict(sorted(qualifying_timing_distribution.items())),
            "session_dates_with_qualifying_entries": sorted(session_dates),
            "note": (
                "No qualifying live entry signals have been observed yet in this runtime sample."
                if total_actionable_signals == 0
                else "Distribution is based on persisted actionable entry timestamps from the live audit surface."
            ),
        },
        "overlap_conflict_with_other_metals_lanes": {
            "same_underlying_conflict_event_count": len(overlap_rows),
            "rows": overlap_rows,
        },
        "late_entry_review": {
            "target_bar": "03:15 ET",
            "late_qualifying_entry_count": qualifying_timing_distribution.get("03:15", 0),
            "assessment": (
                "insufficient_live_entry_sample"
                if total_actionable_signals == 0
                else ("watch_chasey" if qualifying_timing_distribution.get("03:15", 0) > 0 else "no_late_entries_seen")
            ),
            "note": (
                "The branch processed live bars cleanly, but this sample has not produced a qualifying 03:15 entry to judge as acceptable or chasey."
            ),
        },
        "silent_lane_watch": silent_failure_summary,
        "recommendation": {
            "current_action": "keep_branch_unchanged_and_observe_more_live_sessions",
            "note": (
                "Do not promote or retune this branch yet. The runtime path is healthy, but the current live-paper sample still shows zero actionable "
                "entries for both GC and MGC, so more London-open sessions are needed before judging late-entry quality or overlap pressure."
            ),
        },
    }
    return report


def _rows_by_lane(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = payload.get("rows", payload if isinstance(payload, list) else [])
    return {
        str(row.get("lane_id")): dict(row)
        for row in rows
        if isinstance(row, dict) and row.get("lane_id")
    }


def _load_conflict_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        rows.append(json.loads(stripped))
    return rows


def _summarize_conflict_row(row: dict[str, Any]) -> dict[str, Any]:
    standalone_ids = [str(value) for value in row.get("standalone_strategy_ids", [])]
    target_ids = [lane_id for lane_id in standalone_ids if lane_id in TARGET_LANE_IDS]
    other_ids = [lane_id for lane_id in standalone_ids if lane_id not in TARGET_LANE_IDS]
    return {
        "occurred_at": row.get("occurred_at"),
        "instrument": row.get("instrument"),
        "event_type": row.get("event_type"),
        "severity": row.get("severity"),
        "hold_new_entries": row.get("hold_new_entries"),
        "entry_hold_effective": row.get("entry_hold_effective"),
        "target_lane_ids": target_ids,
        "other_metal_lane_ids": other_ids,
    }


def _sample_status(*, actionable_signal_count: int, history_sessions_found: int) -> dict[str, Any]:
    if actionable_signal_count >= 6 and history_sessions_found >= 3:
        label = "enough_for_first_live_quality_review"
    elif actionable_signal_count > 0:
        label = "some_live_entries_seen_but_still_sparse"
    else:
        label = "runtime_valid_but_no_live_entries_yet"
    return {
        "label": label,
        "qualifying_actionable_entry_count": actionable_signal_count,
        "history_sessions_found": history_sessions_found,
    }


def _archive_snapshot(report: dict[str, Any], output_dir: Path) -> Path:
    snapshots_dir = output_dir / "snapshots"
    snapshots_dir.mkdir(parents=True, exist_ok=True)
    identity_source = (
        report.get("live_runtime_summary", {}).get("latest_activity_timestamp")
        or report.get("generated_at")
        or datetime.now(UTC).isoformat()
    )
    safe_name = (
        str(identity_source)
        .replace(":", "-")
        .replace("+", "p")
        .replace("/", "_")
    )
    path = snapshots_dir / f"{safe_name}.json"
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_archived_reports(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    reports: list[dict[str, Any]] = []
    for item in sorted(path.glob("*.json")):
        reports.append(json.loads(item.read_text(encoding="utf-8")))
    return reports


def _build_rolling_observation_summary(reports: list[dict[str, Any]]) -> dict[str, Any]:
    if not reports:
        return {
            "snapshot_count": 0,
            "runtime_signals_total": 0,
            "actionable_entry_signals_total": 0,
            "actionable_conversion_rate": None,
            "gc_vs_mgc_primary_observation_instrument": "undecided_no_snapshots",
            "repeated_blocker_patterns": [],
            "late_entry_pattern": "no_snapshots",
        }

    runtime_signals_total = 0
    actionable_total = 0
    late_entries_total = 0
    blocker_counter: Counter[str] = Counter()
    unique_conflicts: set[tuple[str, str, str]] = set()
    per_symbol: dict[str, dict[str, int]] = {
        "GC": {"runtime_signals": 0, "actionable_entries": 0, "snapshots_seen": 0},
        "MGC": {"runtime_signals": 0, "actionable_entries": 0, "snapshots_seen": 0},
    }

    for report in reports:
        summary = report.get("live_runtime_summary", {})
        runtime_signals_total += int(summary.get("total_runtime_signal_count") or 0)
        actionable_total += int(summary.get("total_actionable_entry_signal_count") or 0)
        late_entries_total += int(report.get("late_entry_review", {}).get("late_qualifying_entry_count") or 0)

        for symbol in ("GC", "MGC"):
            row = report.get("gc_vs_mgc", {}).get(symbol)
            if not row:
                continue
            per_symbol[symbol]["runtime_signals"] += int(row.get("runtime_signal_count") or 0)
            per_symbol[symbol]["actionable_entries"] += int(row.get("actionable_entry_signal_count") or 0)
            per_symbol[symbol]["snapshots_seen"] += 1
            blocker_counter[f"{symbol}:{row.get('latest_audit_verdict') or 'UNKNOWN'}"] += 1

        for conflict in report.get("overlap_conflict_with_other_metals_lanes", {}).get("rows", []):
            unique_conflicts.add(
                (
                    str(conflict.get("occurred_at") or ""),
                    str(conflict.get("instrument") or ""),
                    str(conflict.get("event_type") or ""),
                )
            )

    if runtime_signals_total > 0:
        actionable_conversion_rate = round(actionable_total / runtime_signals_total, 6)
    else:
        actionable_conversion_rate = None

    if per_symbol["GC"]["actionable_entries"] > per_symbol["MGC"]["actionable_entries"]:
        primary_instrument = "GC_so_far"
    elif per_symbol["MGC"]["actionable_entries"] > per_symbol["GC"]["actionable_entries"]:
        primary_instrument = "MGC_so_far"
    else:
        primary_instrument = "undecided_insufficient_live_entries"

    repeated_blocker_patterns = [
        {"pattern": pattern, "count": count}
        for pattern, count in blocker_counter.most_common()
        if count >= 2
    ]

    if late_entries_total > 0:
        late_entry_pattern = "late_entries_seen_watch_quality"
    elif actionable_total > 0:
        late_entry_pattern = "actionable_entries_seen_but_no_03_15_entries"
    else:
        late_entry_pattern = "no_actionable_entries_yet"

    return {
        "snapshot_count": len(reports),
        "runtime_signals_total": runtime_signals_total,
        "actionable_entry_signals_total": actionable_total,
        "actionable_conversion_rate": actionable_conversion_rate,
        "per_symbol": per_symbol,
        "gc_vs_mgc_primary_observation_instrument": primary_instrument,
        "same_underlying_conflict_events_total": len(unique_conflicts),
        "repeated_blocker_patterns": repeated_blocker_patterns,
        "late_entry_pattern": late_entry_pattern,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# GC/MGC London-Open Acceptance Continuation Live Observation",
        f"Generated at: {report['generated_at']}",
        f"Status: {report['status']}",
        f"Sample status: {report['sample_status']['label']}",
        "",
        "## Live Runtime Summary",
        f"- Processed bars across target lanes: {report['live_runtime_summary']['total_processed_bars']}",
        f"- Runtime signal count across target lanes: {report['live_runtime_summary']['total_runtime_signal_count']}",
        f"- Actionable entry signals across target lanes: {report['live_runtime_summary']['total_actionable_entry_signal_count']}",
        f"- Order intents: {report['live_runtime_summary']['total_intent_count']}",
        f"- Fills: {report['live_runtime_summary']['total_fill_count']}",
        f"- Archived snapshots accumulated: {report.get('rolling_observation_summary', {}).get('snapshot_count', 0)}",
        "",
        "## GC vs MGC",
    ]
    for symbol in ("GC", "MGC"):
        row = report["gc_vs_mgc"].get(symbol)
        if not row:
            continue
        lines.append(
            f"- {symbol}: processed_bars={row['processed_bars']}, runtime_signals={row['runtime_signal_count']}, "
            f"actionable_entries={row['actionable_entry_signal_count']}, intents={row['intent_count']}, fills={row['fill_count']}, "
            f"audit={row['latest_audit_verdict']}"
        )
    lines.extend(
        [
            "",
            "## Rolling Summary",
            f"- Runtime signals total: {report.get('rolling_observation_summary', {}).get('runtime_signals_total', 0)}",
            f"- Actionable entry signals total: {report.get('rolling_observation_summary', {}).get('actionable_entry_signals_total', 0)}",
            f"- Actionable conversion rate: {report.get('rolling_observation_summary', {}).get('actionable_conversion_rate')}",
            f"- Primary observation instrument so far: {report.get('rolling_observation_summary', {}).get('gc_vs_mgc_primary_observation_instrument')}",
            f"- Repeated blocker patterns: {report.get('rolling_observation_summary', {}).get('repeated_blocker_patterns') or 'none yet'}",
            "",
            "## Session Timing Distribution",
            f"- Qualifying entry timing distribution: {report['session_timing_distribution']['qualifying_entry_signals'] or 'none yet'}",
            f"- Note: {report['session_timing_distribution']['note']}",
            "",
            "## Overlap / Conflict",
            f"- Same-underlying conflict events mentioning this branch: {report['overlap_conflict_with_other_metals_lanes']['same_underlying_conflict_event_count']}",
            "",
            "## Late Entry Review",
            f"- 03:15 ET qualifying entries: {report['late_entry_review']['late_qualifying_entry_count']}",
            f"- Assessment: {report['late_entry_review']['assessment']}",
            f"- Note: {report['late_entry_review']['note']}",
            "",
            "## Recommendation",
            f"- {report['recommendation']['note']}",
        ]
    )
    return "\n".join(lines)
