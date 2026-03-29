"""Standalone analyst pack generator for probationary paper lanes and replay candidates."""

from __future__ import annotations

import argparse
import csv
import json
import math
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean, pstdev
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = REPO_ROOT / "outputs" / "reports" / "paper_lane_analyst_pack"
ARCHIVE_DIR = OUTPUT_DIR / "archive"
OPERATOR_OUTPUT_DIR = REPO_ROOT / "outputs" / "operator_dashboard"
PAPER_LANE_HISTORY_DIR = OPERATOR_OUTPUT_DIR / "paper_session_lane_history"
REPLAY_OUTPUT_DIR = REPO_ROOT / "outputs" / "replays"
PAPER_DB_PATH = REPO_ROOT / "mgc_v05l.probationary.paper.sqlite3"

LATEST_JSON_PATH = OUTPUT_DIR / "paper_lane_analyst_pack_latest.json"
LATEST_MD_PATH = OUTPUT_DIR / "paper_lane_analyst_pack_latest.md"

ADMITTED_VERDICTS = (
    "STAY_ADMITTED",
    "STAY_ADMITTED_UNDER_WATCH",
    "REVIEW_BEFORE_KEEPING",
)
EVIDENCE_SUFFICIENCY_BUCKETS = (
    "INSUFFICIENT_PAPER_HISTORY",
    "LIGHT_BUT_USABLE",
    "ADEQUATE",
    "STRONG",
)
RETENTION_VERDICTS = (
    "KEEP_RUNNING",
    "KEEP_RUNNING_UNDER_WATCH",
    "REVIEW_FOR_REMOVAL",
    "TOO_EARLY_TO_JUDGE",
)
CANDIDATE_VERDICTS = (
    "NEXT_ADMISSION_CANDIDATE",
    "LATER_REVIEW",
    "DROP_FOR_NOW",
)
CANDIDATE_CONFIDENCE_BUCKETS = ("LOW", "MEDIUM", "HIGH")
CANDIDATE_PRIMARY_BLOCKERS = (
    "THIN_SAMPLE",
    "CONCENTRATED_RETURNS",
    "WEAK_ECONOMICS",
    "POCKET_DEPENDENCE",
    "DRAWDOWN_TOO_HIGH",
    "HOME_CASE_MISMATCH",
    "OTHER",
)
RETENTION_HISTORY_READINESS_BUCKETS = (
    "RETENTION_HISTORY_READY",
    "RETENTION_HISTORY_PARTIAL",
    "RETENTION_HISTORY_NOT_READY",
)

MINIMAL_RETENTION_SCHEMA: tuple[dict[str, str], ...] = (
    {"field": "session_date", "level": "desk", "description": "Paper session date for the archived session bundle."},
    {"field": "lane_id", "level": "lane", "description": "Stable lane identifier."},
    {"field": "instrument", "level": "lane", "description": "Instrument symbol for the lane."},
    {"field": "source_family", "level": "lane", "description": "Approved setup family / branch family."},
    {"field": "session_pocket", "level": "lane", "description": "Session restriction / pocket label for the lane."},
    {"field": "active", "level": "lane", "description": "Whether the lane was active during the paper session."},
    {"field": "blocked", "level": "lane", "description": "Whether the lane was blocked during the paper session."},
    {"field": "signal", "level": "lane", "description": "Whether the lane produced at least one attributed signal."},
    {"field": "intent", "level": "lane", "description": "Whether the lane produced at least one attributed intent."},
    {"field": "fill", "level": "lane", "description": "Whether the lane produced at least one attributed fill."},
    {"field": "open_risk_at_close", "level": "lane", "description": "Whether the lane still had open risk at session close."},
    {"field": "clean_vs_dirty_close", "level": "lane", "description": "Lane/desk close quality at session end."},
    {"field": "halted_by_risk", "level": "lane", "description": "Whether the lane/session was halted by risk."},
    {"field": "attributable_realized_pnl", "level": "lane", "description": "Lane-attributable realized P/L for the paper session, when available."},
    {"field": "attribution_coverage_confidence", "level": "lane", "description": "Coverage/confidence score for lane attribution."},
    {"field": "primary_gap_reason", "level": "lane", "description": "Primary reason the lane cannot be fully attributed or scored."},
)

ARCHIVE_FIELD_CLASSIFICATIONS: dict[str, dict[str, Any]] = {
    "session_date": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_soak_session_snapshot.json", "outputs/operator_dashboard/paper_review_state.json"],
        "note": "Captured now, but not preserved as an archived per-session history family.",
    },
    "lane_id": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Lane identifiers are present in the current approved-models snapshot.",
    },
    "instrument": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Instrument is present in the current approved-models snapshot.",
    },
    "source_family": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Family/branch metadata is present in the current approved-models snapshot.",
    },
    "session_pocket": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Current session restriction can stand in as the session-pocket label.",
    },
    "active": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Current counts are sufficient to tell whether a lane was active.",
    },
    "blocked": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Blocked counts are present now, but not archived per session.",
    },
    "signal": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json", "mgc_v05l.probationary.paper.sqlite3"],
        "note": "Current signal counts exist, but no archived per-session lane bundle exists.",
    },
    "intent": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json", "outputs/operator_dashboard/paper_latest_intents_snapshot.json"],
        "note": "Current intent counts exist, but no archived per-session lane bundle exists.",
    },
    "fill": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json", "outputs/operator_dashboard/paper_latest_fills_snapshot.json"],
        "note": "Current fill counts exist, but no archived per-session lane bundle exists.",
    },
    "open_risk_at_close": {
        "classification": "DERIVABLE_FROM_EXISTING_ARCHIVES",
        "sources": ["outputs/operator_dashboard/paper_readiness_snapshot.json", "outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Could be derived if close-time lane snapshots were archived consistently.",
    },
    "clean_vs_dirty_close": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_review_state.json", "outputs/probationary_pattern_engine/paper_session/reconciliation_events.jsonl"],
        "note": "Desk close quality is present now, but not preserved as archived session history.",
    },
    "halted_by_risk": {
        "classification": "DERIVABLE_FROM_EXISTING_ARCHIVES",
        "sources": ["outputs/probationary_pattern_engine/paper_session/operator_controls.jsonl", "outputs/probationary_pattern_engine/paper_session/alerts.jsonl", "outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Can be inferred from controls/alerts plus lane state when archived together.",
    },
    "attributable_realized_pnl": {
        "classification": "ALREADY_AVAILABLE",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Lane-level realized P/L exists in the latest snapshot, but not in archived session bundles.",
    },
    "attribution_coverage_confidence": {
        "classification": "DERIVABLE_FROM_EXISTING_ARCHIVES",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json"],
        "note": "Coverage/confidence is derivable from signal/fill/block counts plus warning state.",
    },
    "primary_gap_reason": {
        "classification": "DERIVABLE_FROM_EXISTING_ARCHIVES",
        "sources": ["outputs/operator_dashboard/paper_approved_models_snapshot.json", "outputs/operator_dashboard/paper_review_state.json", "outputs/operator_dashboard/paper_soak_session_snapshot.json"],
        "note": "Gap reasons can be derived from warnings, blocked reasons, and session review state if archived together.",
    },
}

PAPER_ARTIFACT_FAMILY_SPECS: tuple[dict[str, Any], ...] = (
    {
        "family": "paper_operator_status_latest",
        "path_pattern": "outputs/probationary_pattern_engine/paper_session/operator_status.json",
        "naming_pattern": "operator_status.json",
        "history_mode": "latest_only",
        "lane_aware": False,
        "retention_sufficient": False,
    },
    {
        "family": "paper_reconciliation_events_log",
        "path_pattern": "outputs/probationary_pattern_engine/paper_session/reconciliation_events.jsonl",
        "naming_pattern": "reconciliation_events.jsonl",
        "history_mode": "append_only_current_session",
        "lane_aware": False,
        "retention_sufficient": False,
    },
    {
        "family": "paper_operator_controls_log",
        "path_pattern": "outputs/probationary_pattern_engine/paper_session/operator_controls.jsonl",
        "naming_pattern": "operator_controls.jsonl",
        "history_mode": "append_only_current_session",
        "lane_aware": False,
        "retention_sufficient": False,
    },
    {
        "family": "paper_alerts_log",
        "path_pattern": "outputs/probationary_pattern_engine/paper_session/alerts.jsonl",
        "naming_pattern": "alerts.jsonl",
        "history_mode": "append_only_current_session",
        "lane_aware": False,
        "retention_sufficient": False,
    },
    {
        "family": "paper_approved_models_snapshot_latest",
        "path_pattern": "outputs/operator_dashboard/paper_approved_models_snapshot.json",
        "naming_pattern": "paper_approved_models_snapshot.json",
        "history_mode": "latest_only",
        "lane_aware": True,
        "retention_sufficient": False,
    },
    {
        "family": "paper_readiness_snapshot_latest",
        "path_pattern": "outputs/operator_dashboard/paper_readiness_snapshot.json",
        "naming_pattern": "paper_readiness_snapshot.json",
        "history_mode": "latest_only",
        "lane_aware": True,
        "retention_sufficient": False,
    },
    {
        "family": "paper_review_state_latest",
        "path_pattern": "outputs/operator_dashboard/paper_review_state.json",
        "naming_pattern": "paper_review_state.json",
        "history_mode": "latest_only",
        "lane_aware": False,
        "retention_sufficient": False,
    },
    {
        "family": "paper_soak_session_snapshot_latest",
        "path_pattern": "outputs/operator_dashboard/paper_soak_session_snapshot.json",
        "naming_pattern": "paper_soak_session_snapshot.json",
        "history_mode": "latest_only",
        "lane_aware": False,
        "retention_sufficient": False,
    },
    {
        "family": "paper_latest_intents_snapshot_latest",
        "path_pattern": "outputs/operator_dashboard/paper_latest_intents_snapshot.json",
        "naming_pattern": "paper_latest_intents_snapshot.json",
        "history_mode": "latest_only",
        "lane_aware": True,
        "retention_sufficient": False,
    },
    {
        "family": "paper_latest_fills_snapshot_latest",
        "path_pattern": "outputs/operator_dashboard/paper_latest_fills_snapshot.json",
        "naming_pattern": "paper_latest_fills_snapshot.json",
        "history_mode": "latest_only",
        "lane_aware": True,
        "retention_sufficient": False,
    },
    {
        "family": "paper_latest_blotter_snapshot_latest",
        "path_pattern": "outputs/operator_dashboard/paper_latest_blotter_snapshot.json",
        "naming_pattern": "paper_latest_blotter_snapshot.json",
        "history_mode": "latest_only",
        "lane_aware": True,
        "retention_sufficient": False,
    },
    {
        "family": "paper_session_lane_history_archive",
        "path_pattern": "outputs/operator_dashboard/paper_session_lane_history/*.json",
        "naming_pattern": "YYYY-MM-DD_<generated_at>.json",
        "history_mode": "archived",
        "lane_aware": True,
        "retention_sufficient": True,
    },
    {
        "family": "paper_daily_summary_archive",
        "path_pattern": "outputs/probationary_pattern_engine/paper_session/daily/*.summary.json",
        "naming_pattern": "*.summary.json",
        "history_mode": "archived",
        "lane_aware": False,
        "retention_sufficient": True,
    },
    {
        "family": "paper_daily_blotter_archive",
        "path_pattern": "outputs/probationary_pattern_engine/paper_session/daily/*.blotter.csv",
        "naming_pattern": "*.blotter.csv",
        "history_mode": "archived",
        "lane_aware": True,
        "retention_sufficient": True,
    },
    {
        "family": "paper_dashboard_archive_snapshots",
        "path_pattern": "outputs/operator_dashboard/archive/paper_*.json",
        "naming_pattern": "paper_*.json",
        "history_mode": "archived",
        "lane_aware": True,
        "retention_sufficient": True,
    },
)

REQUIRED_CANDIDATE_SPECS: tuple[dict[str, str], ...] = (
    {"instrument": "MGC", "branch": "usLatePauseResumeLongTurn", "cohort": "ADMITTED_COMPARATOR"},
    {"instrument": "MGC", "branch": "asiaEarlyNormalBreakoutRetestHoldTurn", "cohort": "ADMITTED_COMPARATOR"},
    {"instrument": "MGC", "branch": "asiaEarlyPauseResumeShortTurn", "cohort": "ADMITTED_COMPARATOR"},
    {"instrument": "PL", "branch": "usLatePauseResumeLongTurn", "cohort": "ADMITTED_COMPARATOR"},
    {"instrument": "GC", "branch": "asiaEarlyNormalBreakoutRetestHoldTurn", "cohort": "ADMITTED_COMPARATOR"},
    {"instrument": "GC", "branch": "usLatePauseResumeLongTurn", "cohort": "NEXT_TIER"},
    {"instrument": "CL", "branch": "usLatePauseResumeLongTurn", "cohort": "NEXT_TIER"},
    {"instrument": "NG", "branch": "usLatePauseResumeLongTurn", "cohort": "NEXT_TIER"},
    {"instrument": "6B", "branch": "asiaEarlyNormalBreakoutRetestHoldTurn", "cohort": "NEXT_TIER"},
    {"instrument": "6E", "branch": "asiaEarlyNormalBreakoutRetestHoldTurn", "cohort": "NEXT_TIER"},
    {"instrument": "NG", "branch": "asiaEarlyNormalBreakoutRetestHoldTurn", "cohort": "NEXT_TIER"},
    {"instrument": "CL", "branch": "asiaEarlyPauseResumeShortTurn", "cohort": "NEXT_TIER"},
    {"instrument": "MBT", "branch": "asiaEarlyPauseResumeShortTurn", "cohort": "NEXT_TIER"},
)


@dataclass(frozen=True)
class CandidateArtifact:
    instrument: str
    branch: str
    path: Path
    summary: dict[str, Any]
    summary_metrics: dict[str, Any]
    trade_rows: list[dict[str, Any]]
    variant: str
    cohort: str


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _iso_now() -> str:
    return datetime.now(tz=UTC).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_decimal(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_div(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return numerator / denominator


def _ratio_percent(numerator: float | None, denominator: float | None) -> float | None:
    result = _safe_div(numerator, denominator)
    if result is None:
        return None
    return result * 100.0


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _positive_concentration(values: list[float], top_n: int) -> float | None:
    positives = sorted((value for value in values if value > 0), reverse=True)
    if not positives:
        return None
    total = sum(positives)
    if total <= 0:
        return None
    return sum(positives[:top_n]) / total


def _max_drawdown_from_pnl(values: list[float]) -> float | None:
    if not values:
        return None
    running = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for value in values:
        running += value
        peak = max(peak, running)
        max_drawdown = max(max_drawdown, peak - running)
    return max_drawdown


def _trade_sharpe_proxy(values: list[float]) -> float | None:
    if len(values) < 5:
        return None
    volatility = pstdev(values)
    if volatility == 0:
        return None
    return mean(values) / volatility * math.sqrt(len(values))


def _discover_archived_paper_artifacts(repo_root: Path = REPO_ROOT) -> list[str]:
    patterns = (
        "outputs/operator_dashboard/paper_session_lane_history/*.json",
        "outputs/probationary_pattern_engine/paper_session/daily/*.summary.json",
        "outputs/probationary_pattern_engine/paper_session/daily/*.summary.md",
        "outputs/probationary_pattern_engine/paper_session/daily/*.blotter.csv",
        "outputs/operator_dashboard/archive/paper_*.json",
    )
    discovered: list[str] = []
    for pattern in patterns:
        for path in sorted(repo_root.glob(pattern)):
            discovered.append(str(path))
    return discovered


def _present_paper_artifact_families(repo_root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    families: list[dict[str, Any]] = []
    for spec in PAPER_ARTIFACT_FAMILY_SPECS:
        matches = sorted(str(path) for path in repo_root.glob(spec["path_pattern"]))
        if not matches:
            continue
        families.append(
            {
                "family": spec["family"],
                "path_pattern": spec["path_pattern"],
                "naming_pattern": spec["naming_pattern"],
                "history_mode": spec["history_mode"],
                "lane_aware": spec["lane_aware"],
                "retention_sufficient": spec["retention_sufficient"],
                "artifact_count": len(matches),
                "artifacts": matches[:20],
            }
        )
    return families


def _load_archived_lane_history_records(repo_root: Path = REPO_ROOT) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted((repo_root / "outputs" / "operator_dashboard" / "paper_session_lane_history").glob("*.json")):
        try:
            payload = _load_json(path)
        except (json.JSONDecodeError, OSError):
            continue
        if not isinstance(payload.get("lanes"), list):
            continue
        payload["_archive_path"] = str(path)
        records.append(payload)
    return records


def _retention_history_readiness_verdict(archived_session_count: int, missing_fields: list[str]) -> str:
    if archived_session_count == 0:
        return "RETENTION_HISTORY_NOT_READY"
    if missing_fields or archived_session_count < 3:
        return "RETENTION_HISTORY_PARTIAL"
    return "RETENTION_HISTORY_READY"


def _lane_history_missing_fields(lane_record: dict[str, Any]) -> list[str]:
    required_fields = [item["field"] for item in MINIMAL_RETENTION_SCHEMA if item["level"] == "lane"]
    return [field for field in required_fields if field not in lane_record]


def _archived_history_desk_missing_fields(record: dict[str, Any]) -> list[str]:
    required_fields = [item["field"] for item in MINIMAL_RETENTION_SCHEMA if item["level"] == "desk"]
    return [field for field in required_fields if field not in record]


def _build_retention_history_substrate(lane_rows: list[dict[str, Any]], repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    present_families = _present_paper_artifact_families(repo_root)
    archived_families = [family for family in present_families if family["history_mode"] == "archived"]
    archived_records = _load_archived_lane_history_records(repo_root)
    archived_session_dates = sorted({str(record.get("session_date") or "") for record in archived_records if record.get("session_date")})
    archived_file_count = len(archived_records)
    archived_session_count = len(archived_session_dates)
    schema = []
    missing_schema_fields = []
    for item in MINIMAL_RETENTION_SCHEMA:
        classification = ARCHIVE_FIELD_CLASSIFICATIONS[item["field"]]
        schema.append(
            {
                **item,
                "classification": classification["classification"],
                "sources": classification["sources"],
                "note": classification["note"],
            }
        )
        if classification["classification"] == "MISSING_FROM_ARCHIVE_SUBSTRATE":
            missing_schema_fields.append(item["field"])

    lane_rows_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    archive_schema_gaps: set[str] = set()
    for record in archived_records:
        archive_schema_gaps.update(_archived_history_desk_missing_fields(record))
        for lane in record.get("lanes") or []:
            lane_id = str(lane.get("lane_id") or "")
            if lane_id:
                lane_rows_by_id[lane_id].append(lane)
    lane_history = []
    for row in lane_rows:
        lane_archives = lane_rows_by_id.get(str(row["lane_id"]), [])
        missing_lane_fields = (
            sorted({field for lane in lane_archives for field in _lane_history_missing_fields(lane)})
            if lane_archives
            else []
        )
        archive_schema_gaps.update(missing_lane_fields)
        lane_history.append(
            {
                "lane_id": row["lane_id"],
                "display_name": row["display_name"],
                "instrument": row["instrument"],
                "branch": row["branch"],
                "lane_history_usable": bool(lane_archives) and not missing_lane_fields,
                "missing_history_fields": missing_lane_fields,
                "history_gap_reason": (
                    "NO_ARCHIVED_LANE_SESSIONS"
                    if not lane_archives
                    else ("MISSING_REQUIRED_FIELDS" if missing_lane_fields else None)
                ),
            }
        )

    exact_sufficient_artifacts = [family["family"] for family in archived_families if family["retention_sufficient"]]
    recommendation_gaps: list[str] = []
    if archived_session_count == 0:
        recommendation_gaps.append("No signed-off paper session lane-history archives exist yet.")
    elif archived_session_count < 3:
        recommendation_gaps.append("Archived lane-history exists, but fewer than 3 completed paper sessions are available for retention scoring.")
    if archive_schema_gaps:
        recommendation_gaps.append(
            "Archived lane-history files exist, but some required per-lane fields are missing: "
            + ", ".join(sorted(archive_schema_gaps))
            + "."
        )
    recommendation = {
        "already_sufficient_artifacts": exact_sufficient_artifacts,
        "missing_before_true_cumulative_scoring": recommendation_gaps,
    }
    return {
        "archived_history_present": archived_file_count > 0,
        "archived_file_count": archived_file_count,
        "archived_session_count": archived_session_count,
        "archived_session_dates": archived_session_dates,
        "artifact_families_found": present_families,
        "minimal_retention_schema": schema,
        "lane_history": lane_history,
        "retention_history_readiness_verdict": _retention_history_readiness_verdict(
            archived_session_count,
            sorted(set(missing_schema_fields).union(archive_schema_gaps)),
        ),
        "recommendation": recommendation,
    }


def _sorted_timestamp_key(path: Path) -> str:
    stem = path.name
    for token in reversed(stem.split("_")):
        if len(token) == 6 and token.isdigit():
            idx = stem.rfind(token)
            if idx >= 9 and stem[idx - 9 : idx - 1].isdigit():
                return stem[idx - 9 : idx - 1] + "_" + token
    return stem


def _load_trade_rows(path: Path, branch: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("setup_family") != branch:
                continue
            rows.append(row)
    rows.sort(key=lambda row: row.get("entry_ts") or "")
    return rows


def _candidate_variant_label(path: Path) -> str:
    name = path.name
    if "second_pass_direct_approved_" in name:
        return "direct"
    marker = "persisted_bar_replay_second_pass_"
    if marker in name and "_approved_" in name:
        middle = name.split(marker, 1)[1].split("_approved_", 1)[0]
        parts = middle.split("_")
        if len(parts) > 2:
            return "_".join(parts[2:])
    if "futures_approved_" in name:
        return "approved_baseline"
    return "artifact"


def _load_replay_candidates() -> list[CandidateArtifact]:
    candidates: list[CandidateArtifact] = []
    for summary_path in REPLAY_OUTPUT_DIR.glob("*.summary.json"):
        summary = _load_json(summary_path)
        trade_ledger_path = summary.get("trade_ledger_path")
        summary_metrics_path = summary.get("summary_metrics_path")
        symbol = str(summary.get("symbol") or "").upper()
        if not trade_ledger_path or not summary_metrics_path or not symbol:
            continue
        trade_ledger = Path(str(trade_ledger_path))
        summary_metrics = Path(str(summary_metrics_path))
        if not trade_ledger.exists() or not summary_metrics.exists():
            continue
        summary_metrics_payload = _load_json(summary_metrics)
        for spec in REQUIRED_CANDIDATE_SPECS:
            if spec["instrument"] != symbol:
                continue
            rows = _load_trade_rows(trade_ledger, spec["branch"])
            if not rows:
                continue
            candidates.append(
                CandidateArtifact(
                    instrument=symbol,
                    branch=spec["branch"],
                    path=summary_path,
                    summary=summary,
                    summary_metrics=summary_metrics_payload,
                    trade_rows=rows,
                    variant=_candidate_variant_label(summary_path),
                    cohort=spec["cohort"],
                )
            )
    return candidates


def _unique_signal_session_dates(bar_ids: list[str]) -> list[str]:
    sessions = set()
    for bar_id in bar_ids:
        parts = str(bar_id).split("|")
        if len(parts) >= 3:
            timestamp = _parse_iso(parts[2])
            if timestamp is not None:
                sessions.add(timestamp.date().isoformat())
    return sorted(sessions)


def _session_pocket_concentration_from_rows(rows: list[dict[str, Any]]) -> float | None:
    pnl_by_day: dict[str, float] = defaultdict(float)
    for row in rows:
        entry_ts = _parse_iso(row.get("entry_ts"))
        pnl = _parse_decimal(row.get("net_pnl"))
        if entry_ts is None or pnl is None:
            continue
        pnl_by_day[entry_ts.date().isoformat()] += pnl
    return _positive_concentration(list(pnl_by_day.values()), 1)


def _profit_factor(values: list[float]) -> float | None:
    gross_win = sum(value for value in values if value > 0)
    gross_loss = abs(sum(value for value in values if value < 0))
    if gross_loss == 0:
        return None if gross_win == 0 else float("inf")
    return gross_win / gross_loss


def _compute_candidate_metrics(candidate: CandidateArtifact) -> dict[str, Any]:
    pnl_values = [_parse_decimal(row.get("net_pnl")) or 0.0 for row in candidate.trade_rows]
    wins = [value for value in pnl_values if value > 0]
    losses = [value for value in pnl_values if value < 0]
    signal_dates = _unique_signal_session_dates(
        list((candidate.summary.get("approved_branch_signal_bars") or {}).get(candidate.branch) or [])
    )
    total_pnl = sum(pnl_values)
    top1_concentration = _positive_concentration(pnl_values, 1)
    top3_concentration = _positive_concentration(pnl_values, 3)
    survives_without_top1 = None
    survives_without_top3 = None
    positive_trades = sorted((value for value in pnl_values if value > 0), reverse=True)
    if positive_trades:
        survives_without_top1 = total_pnl - positive_trades[0] > 0
        survives_without_top3 = total_pnl - sum(positive_trades[:3]) > 0
    max_drawdown = _max_drawdown_from_pnl(pnl_values)
    session_pocket_concentration = _session_pocket_concentration_from_rows(candidate.trade_rows)
    start_ts = min((_parse_iso(row.get("entry_ts")) for row in candidate.trade_rows), default=None)
    end_ts = max((_parse_iso(row.get("exit_ts")) for row in candidate.trade_rows), default=None)
    current_timeframe = str(candidate.summary.get("timeframe") or "")
    return {
        "sample_start": start_ts.isoformat() if start_ts else candidate.summary.get("source_first_bar_ts"),
        "sample_end": end_ts.isoformat() if end_ts else candidate.summary.get("source_last_bar_ts"),
        "sessions_used": len(signal_dates),
        "bars_used": candidate.summary.get("processed_bars"),
        "realized_pnl": total_pnl,
        "profit_factor": _profit_factor(pnl_values),
        "max_drawdown": max_drawdown,
        "trade_sharpe_proxy": _trade_sharpe_proxy(pnl_values),
        "trade_count": len(pnl_values),
        "win_rate": _safe_div(len(wins), len(pnl_values)),
        "avg_realized_pnl_per_trade": _safe_div(total_pnl, len(pnl_values)),
        "top_1_trade_concentration": top1_concentration,
        "top_3_trade_concentration": top3_concentration,
        "survives_without_top_1": survives_without_top1,
        "survives_without_top_3": survives_without_top3,
        "session_pocket_concentration": session_pocket_concentration,
        "exact_vs_home_case_horizon_match": current_timeframe == "5m",
        "signal_sessions": signal_dates,
        "source_timeframe": current_timeframe,
        "gross_win_count": len(wins),
        "gross_loss_count": len(losses),
        "source_summary_path": str(candidate.path),
        "source_trade_ledger_path": str(candidate.summary.get("trade_ledger_path")),
        "source_summary_metrics_path": str(candidate.summary.get("summary_metrics_path")),
    }


def _candidate_warning_flags(metrics: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    trade_count = int(metrics.get("trade_count") or 0)
    realized_pnl = _parse_decimal(metrics.get("realized_pnl")) or 0.0
    top1 = _parse_decimal(metrics.get("top_1_trade_concentration"))
    top3 = _parse_decimal(metrics.get("top_3_trade_concentration"))
    pocket = _parse_decimal(metrics.get("session_pocket_concentration"))
    profit_factor = metrics.get("profit_factor")
    if trade_count < 25:
        warnings.append("THIN_SAMPLE")
    if top1 is not None and top1 >= 0.35:
        warnings.append("CONCENTRATED_TOP_1")
    if top3 is not None and top3 >= 0.75:
        warnings.append("CONCENTRATED_TOP_3")
    if pocket is not None and pocket >= 0.45:
        warnings.append("SESSION_POCKET_DEPENDENCE")
    if realized_pnl <= 0:
        warnings.append("NEGATIVE_ECONOMICS")
    if profit_factor not in (None, float("inf")) and float(profit_factor) < 1.05:
        warnings.append("WEAK_PROFIT_FACTOR")
    max_drawdown = _parse_decimal(metrics.get("max_drawdown"))
    if max_drawdown is not None and realized_pnl > 0 and max_drawdown > realized_pnl:
        warnings.append("DRAWDOWN_TOO_HIGH")
    if metrics.get("exact_vs_home_case_horizon_match") is False:
        warnings.append("HOME_CASE_MISMATCH")
    if metrics.get("survives_without_top_1") is False:
        warnings.append("FAILS_WITHOUT_TOP_1")
    if metrics.get("survives_without_top_3") is False:
        warnings.append("FAILS_WITHOUT_TOP_3")
    return warnings


def _candidate_verdict(metrics: dict[str, Any]) -> str:
    warnings = set(_candidate_warning_flags(metrics))
    realized_pnl = _parse_decimal(metrics.get("realized_pnl")) or 0.0
    trade_count = int(metrics.get("trade_count") or 0)
    profit_factor = metrics.get("profit_factor")
    if realized_pnl <= 0:
        return "DROP_FOR_NOW"
    if profit_factor not in (None, float("inf")) and float(profit_factor) < 1.0:
        return "DROP_FOR_NOW"
    if "DRAWDOWN_TOO_HIGH" in warnings and trade_count < 25:
        return "DROP_FOR_NOW"
    if trade_count >= 30 and (
        profit_factor == float("inf")
        or (profit_factor is not None and float(profit_factor) >= 1.15)
    ) and "CONCENTRATED_TOP_3" not in warnings and metrics.get("survives_without_top_3") is not False and "DRAWDOWN_TOO_HIGH" not in warnings:
        return "NEXT_ADMISSION_CANDIDATE"
    return "LATER_REVIEW"


def _candidate_readiness_confidence(metrics: dict[str, Any], warnings: list[str], verdict: str) -> str:
    warning_set = set(warnings)
    trade_count = int(metrics.get("trade_count") or 0)
    if verdict == "NEXT_ADMISSION_CANDIDATE" and trade_count >= 40 and not warning_set.intersection({"CONCENTRATED_TOP_3", "THIN_SAMPLE", "DRAWDOWN_TOO_HIGH"}):
        return "HIGH"
    if verdict == "LATER_REVIEW" and trade_count >= 25 and "THIN_SAMPLE" not in warning_set:
        return "MEDIUM"
    return "LOW"


def _candidate_primary_blocker(metrics: dict[str, Any], warnings: list[str], verdict: str) -> str:
    warning_set = set(warnings)
    if verdict == "NEXT_ADMISSION_CANDIDATE":
        return "OTHER"
    if "THIN_SAMPLE" in warning_set:
        return "THIN_SAMPLE"
    if warning_set.intersection({"CONCENTRATED_TOP_1", "CONCENTRATED_TOP_3", "FAILS_WITHOUT_TOP_1", "FAILS_WITHOUT_TOP_3"}):
        return "CONCENTRATED_RETURNS"
    if warning_set.intersection({"NEGATIVE_ECONOMICS", "WEAK_PROFIT_FACTOR"}):
        return "WEAK_ECONOMICS"
    if "SESSION_POCKET_DEPENDENCE" in warning_set:
        return "POCKET_DEPENDENCE"
    if "DRAWDOWN_TOO_HIGH" in warning_set:
        return "DRAWDOWN_TOO_HIGH"
    if "HOME_CASE_MISMATCH" in warning_set:
        return "HOME_CASE_MISMATCH"
    return "OTHER"


def _selection_score(metrics: dict[str, Any], path: Path) -> tuple[Any, ...]:
    profit_factor = metrics.get("profit_factor")
    if profit_factor == float("inf"):
        pf_score = 9999.0
    elif profit_factor is None:
        pf_score = -9999.0
    else:
        pf_score = float(profit_factor)
    return (
        1 if (_parse_decimal(metrics.get("realized_pnl")) or 0.0) > 0 else 0,
        1 if metrics.get("survives_without_top_3") else 0,
        pf_score,
        _parse_decimal(metrics.get("realized_pnl")) or 0.0,
        -(_parse_decimal(metrics.get("top_3_trade_concentration")) or 999.0),
        int(metrics.get("trade_count") or 0),
        _sorted_timestamp_key(path),
    )


def _next_tier_ranking_score(row: dict[str, Any]) -> tuple[Any, ...]:
    metrics = row["metrics"]
    warnings = set(row["warnings"])
    verdict_rank = {"NEXT_ADMISSION_CANDIDATE": 2, "LATER_REVIEW": 1, "DROP_FOR_NOW": 0}[row["verdict"]]
    profit_factor = metrics.get("profit_factor")
    if profit_factor == float("inf"):
        pf_score = 9999.0
    elif profit_factor is None:
        pf_score = -9999.0
    else:
        pf_score = float(profit_factor)
    return (
        verdict_rank,
        0 if "THIN_SAMPLE" in warnings else 1,
        0 if metrics.get("survives_without_top_3") is False else 1,
        0 if "CONCENTRATED_TOP_3" in warnings else 1,
        int(metrics.get("trade_count") or 0),
        -(_parse_decimal(metrics.get("top_3_trade_concentration")) or 999.0),
        pf_score,
        _parse_decimal(metrics.get("realized_pnl")) or 0.0,
    )


def _select_best_candidates() -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[tuple[CandidateArtifact, dict[str, Any]]]] = defaultdict(list)
    for candidate in _load_replay_candidates():
        metrics = _compute_candidate_metrics(candidate)
        grouped[(candidate.instrument, candidate.branch)].append((candidate, metrics))

    selected: list[dict[str, Any]] = []
    for spec in REQUIRED_CANDIDATE_SPECS:
        key = (spec["instrument"], spec["branch"])
        options = grouped.get(key, [])
        if not options:
            continue
        best_candidate, best_metrics = max(options, key=lambda item: _selection_score(item[1], item[0].path))
        warnings = _candidate_warning_flags(best_metrics)
        verdict = _candidate_verdict(best_metrics)
        selected.append(
            {
                "instrument": best_candidate.instrument,
                "branch": best_candidate.branch,
                "cohort": spec["cohort"],
                "selected_variant": best_candidate.variant,
                "variant_count_considered": len(options),
                "available_variants": sorted({candidate.variant for candidate, _ in options}),
                "metrics": best_metrics,
                "warnings": warnings,
                "verdict": verdict,
                "admission_readiness_confidence": _candidate_readiness_confidence(best_metrics, warnings, verdict),
                "primary_blocker": _candidate_primary_blocker(best_metrics, warnings, verdict),
            }
        )
    return selected


def _load_paper_db_signal_count() -> dict[str, Any]:
    if not PAPER_DB_PATH.exists():
        return {"signal_rows": 0}
    with sqlite3.connect(PAPER_DB_PATH) as connection:
        signal_rows = connection.execute("select count(*) from signals").fetchone()[0]
    return {"signal_rows": int(signal_rows)}


def _admitted_verdict(row: dict[str, Any], detail: dict[str, Any], coverage_ratio: float | None) -> str:
    risk_state = str(detail.get("risk_state") or "")
    reconciliation_state = str(detail.get("reconciliation_state") or "")
    unresolved = int(detail.get("unresolved_intent_count") or 0)
    blocked_count = int(detail.get("blocked_count") or 0)
    fill_count = int(detail.get("fill_count") or 0)
    signal_count = int(detail.get("signal_count") or 0)
    if (
        risk_state not in ("", "CLEAR", "READY", "NONE", "OK")
        or reconciliation_state not in ("", "CLEAN")
        or unresolved > 0
    ):
        return "REVIEW_BEFORE_KEEPING"
    if fill_count == 0 or signal_count == 0 or (coverage_ratio is not None and coverage_ratio < 0.5):
        return "STAY_ADMITTED_UNDER_WATCH"
    if blocked_count > signal_count and signal_count > 0:
        return "STAY_ADMITTED_UNDER_WATCH"
    return "STAY_ADMITTED"


def _evidence_sufficiency(
    paper_sessions_in_window: int,
    active_sessions_in_window: int,
    filled_sessions_in_window: int,
    attribution_coverage_rate: float | None,
) -> str:
    if paper_sessions_in_window < 3 or active_sessions_in_window < 2 or filled_sessions_in_window < 2:
        return "INSUFFICIENT_PAPER_HISTORY"
    if paper_sessions_in_window < 5 or filled_sessions_in_window < 3 or (attribution_coverage_rate is not None and attribution_coverage_rate < 0.5):
        return "LIGHT_BUT_USABLE"
    if paper_sessions_in_window < 10 or filled_sessions_in_window < 5:
        return "ADEQUATE"
    return "STRONG"


def _retention_warning_flags(
    *,
    paper_sessions_in_window: int,
    active_sessions_in_window: int,
    filled_sessions_in_window: int,
    blocked_sessions_in_window: int,
    open_risk_close_sessions_in_window: int,
    dirty_close_sessions_in_window: int,
    halted_sessions_in_window: int,
    attribution_coverage_rate: float | None,
    session_pocket_concentration: float | None,
) -> list[str]:
    warnings: list[str] = []
    if active_sessions_in_window < 2:
        warnings.append("TOO_FEW_ACTIVE_SESSIONS")
    if filled_sessions_in_window < 2:
        warnings.append("TOO_FEW_FILLED_SESSIONS")
    if paper_sessions_in_window and dirty_close_sessions_in_window >= 2:
        warnings.append("REPEATED_DIRTY_CLOSES")
    if paper_sessions_in_window and halted_sessions_in_window >= 2:
        warnings.append("REPEATED_HALTS")
    if attribution_coverage_rate is not None and attribution_coverage_rate < 0.5:
        warnings.append("LOW_ATTRIBUTION_COVERAGE")
    if paper_sessions_in_window and blocked_sessions_in_window / paper_sessions_in_window >= 0.5:
        warnings.append("BLOCK_HEAVY")
    if paper_sessions_in_window and open_risk_close_sessions_in_window >= 2:
        warnings.append("OPEN_RISK_CLOSE_PATTERN")
    if session_pocket_concentration is not None and session_pocket_concentration >= 0.6:
        warnings.append("SESSION_POCKET_OVERCONCENTRATED")
    return warnings


def _retention_verdict(
    evidence_sufficiency: str,
    warnings: list[str],
    attributable_realized_pnl_total_window: float | None,
) -> str:
    warning_set = set(warnings)
    if evidence_sufficiency == "INSUFFICIENT_PAPER_HISTORY":
        if warning_set.intersection({"REPEATED_DIRTY_CLOSES", "REPEATED_HALTS", "OPEN_RISK_CLOSE_PATTERN"}):
            return "KEEP_RUNNING_UNDER_WATCH"
        return "TOO_EARLY_TO_JUDGE"
    if warning_set.intersection({"REPEATED_DIRTY_CLOSES", "REPEATED_HALTS", "OPEN_RISK_CLOSE_PATTERN"}):
        return "REVIEW_FOR_REMOVAL"
    if attributable_realized_pnl_total_window is not None and attributable_realized_pnl_total_window < 0 and warning_set.intersection({"BLOCK_HEAVY", "LOW_ATTRIBUTION_COVERAGE"}):
        return "REVIEW_FOR_REMOVAL"
    if warning_set:
        return "KEEP_RUNNING_UNDER_WATCH"
    return "KEEP_RUNNING"


def _retention_rank(row: dict[str, Any]) -> tuple[Any, ...]:
    sufficiency_rank = {
        "STRONG": 3,
        "ADEQUATE": 2,
        "LIGHT_BUT_USABLE": 1,
        "INSUFFICIENT_PAPER_HISTORY": 0,
    }[row["evidence_sufficiency_verdict"]]
    retention_rank = {
        "KEEP_RUNNING": 3,
        "KEEP_RUNNING_UNDER_WATCH": 2,
        "TOO_EARLY_TO_JUDGE": 1,
        "REVIEW_FOR_REMOVAL": 0,
    }[row["retention_verdict"]]
    return (
        retention_rank,
        sufficiency_rank,
        row["filled_sessions_in_window"],
        row["active_sessions_in_window"],
        _parse_decimal(row.get("attributable_realized_pnl_total_window")) or 0.0,
        -len(row["retention_warning_flags"]),
        row["instrument"],
        row["branch"],
    )


def _build_admitted_section() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    approved_models = _load_json(OPERATOR_OUTPUT_DIR / "paper_approved_models_snapshot.json")
    readiness = _load_json(OPERATOR_OUTPUT_DIR / "paper_readiness_snapshot.json")
    soak = _load_json(OPERATOR_OUTPUT_DIR / "paper_soak_session_snapshot.json")
    review_state = _load_json(OPERATOR_OUTPUT_DIR / "paper_review_state.json")
    carry_forward = _load_json(OPERATOR_OUTPUT_DIR / "paper_carry_forward_state.json")
    exceptions = _load_json(OPERATOR_OUTPUT_DIR / "paper_exceptions_snapshot.json")
    db_stats = _load_paper_db_signal_count()
    archived_artifacts = _discover_archived_paper_artifacts()

    session_observed = 1 if soak.get("session_date") else 0
    rows: list[dict[str, Any]] = []
    for row in approved_models.get("rows", []):
        detail = approved_models.get("details_by_branch", {}).get(row.get("branch"), {})
        display_name = str(detail.get("branch") or row.get("branch") or "")
        pure_branch = display_name.split(" / ", 1)[-1] if " / " in display_name else display_name
        signal_count = int(detail.get("signal_count") or row.get("signal_count") or 0)
        intent_count = int(detail.get("intent_count") or row.get("intent_count") or 0)
        fill_count = int(detail.get("fill_count") or row.get("fill_count") or 0)
        blocked_count = int(detail.get("blocked_count") or row.get("blocked_count") or 0)
        decision_count = int(detail.get("decision_count") or 0)
        active = any(
            (
                signal_count,
                intent_count,
                fill_count,
                blocked_count,
                decision_count,
                detail.get("latest_activity_timestamp"),
            )
        )
        sessions_active = 1 if active else 0
        sessions_signaled = 1 if signal_count > 0 else 0
        sessions_with_intents = 1 if intent_count > 0 else 0
        sessions_with_fills = 1 if fill_count > 0 else 0
        sessions_blocked_only = 1 if blocked_count > 0 and signal_count == 0 and intent_count == 0 and fill_count == 0 else 0
        risk_state = str(detail.get("risk_state") or "")
        lane_halt_reason = detail.get("lane_halt_reason")
        halted_by_risk_count = 1 if risk_state not in ("", "CLEAR", "READY", "NONE", "OK") or lane_halt_reason else 0
        realized_pnl = _parse_decimal(detail.get("realized_pnl"))
        unresolved_issues = int(detail.get("unresolved_intent_count") or 0)
        coverage_ratio = _safe_div(fill_count, signal_count) if signal_count else (0.0 if active else None)
        paper_sessions_in_window = session_observed
        active_sessions_in_window = sessions_active
        blocked_sessions_in_window = 1 if blocked_count > 0 else 0
        filled_sessions_in_window = sessions_with_fills
        open_risk_close_sessions_in_window = 1 if not readiness.get("flat_state") and (detail.get("open_position") or detail.get("open_qty")) else 0
        dirty_close_sessions_in_window = 1 if detail.get("reconciliation_state") not in ("", "CLEAN") else 0
        halted_sessions_in_window = halted_by_risk_count
        active_session_rate = _safe_div(active_sessions_in_window, paper_sessions_in_window)
        fill_session_rate = _safe_div(filled_sessions_in_window, paper_sessions_in_window)
        dirty_close_rate = _safe_div(dirty_close_sessions_in_window, paper_sessions_in_window)
        halt_session_rate = _safe_div(halted_sessions_in_window, paper_sessions_in_window)
        attributable_realized_values = [realized_pnl] if realized_pnl is not None and filled_sessions_in_window else []
        avg_realized_per_filled_session = _safe_div(sum(attributable_realized_values), filled_sessions_in_window)
        median_realized_per_filled_session = _median(attributable_realized_values)
        attribution_coverage_rate = _safe_div(filled_sessions_in_window + blocked_sessions_in_window, active_sessions_in_window) if active_sessions_in_window else None
        session_pocket_concentration = 1.0 if active_sessions_in_window == 1 else None
        evidence_sufficiency = _evidence_sufficiency(
            paper_sessions_in_window,
            active_sessions_in_window,
            filled_sessions_in_window,
            attribution_coverage_rate,
        )
        retention_warning_flags = _retention_warning_flags(
            paper_sessions_in_window=paper_sessions_in_window,
            active_sessions_in_window=active_sessions_in_window,
            filled_sessions_in_window=filled_sessions_in_window,
            blocked_sessions_in_window=blocked_sessions_in_window,
            open_risk_close_sessions_in_window=open_risk_close_sessions_in_window,
            dirty_close_sessions_in_window=dirty_close_sessions_in_window,
            halted_sessions_in_window=halted_sessions_in_window,
            attribution_coverage_rate=attribution_coverage_rate,
            session_pocket_concentration=session_pocket_concentration,
        )
        warnings: list[str] = []
        if fill_count == 0:
            warnings.append("NO_ATTRIBUTED_FILLS")
        if signal_count == 0:
            warnings.append("NO_ATTRIBUTED_SIGNALS")
        if blocked_count > 0:
            warnings.append("REPEATED_BLOCKS")
        if halted_by_risk_count:
            warnings.append("RISK_HALT_ACTIVITY")
        if unresolved_issues > 0:
            warnings.append("UNRESOLVED_INTENT_OR_FILL_ISSUES")
        if review_state.get("summary_generated") is False:
            warnings.append("SESSION_SUMMARY_MISSING")
        if carry_forward.get("active"):
            warnings.append("DESK_CARRY_FORWARD_ACTIVE")

        rows.append(
            {
                "lane_id": row.get("lane_id"),
                "instrument": row.get("instrument"),
                "display_name": display_name,
                "branch": pure_branch,
                "source_family": detail.get("source_family") or row.get("source_family"),
                "session_restriction": detail.get("session_restriction") or row.get("session_restriction"),
                "side": detail.get("side") or row.get("side"),
                "sessions_observed": session_observed,
                "sessions_active": sessions_active,
                "sessions_blocked_only": sessions_blocked_only,
                "sessions_signaled": sessions_signaled,
                "sessions_with_intents": sessions_with_intents,
                "sessions_with_fills": sessions_with_fills,
                "sessions_with_open_risk_at_close": None,
                "clean_close_count": None,
                "dirty_close_count": None,
                "halted_by_risk_count": halted_by_risk_count,
                "total_attributable_realized_pnl": realized_pnl,
                "avg_attributable_realized_pnl_per_active_session": _safe_div(realized_pnl, sessions_active),
                "win_rate": None,
                "fill_rate": _safe_div(sessions_with_fills, session_observed),
                "block_rate": _safe_div(1 if blocked_count > 0 else 0, session_observed),
                "halt_rate": _safe_div(halted_by_risk_count, session_observed),
                "unresolved_intent_fill_issue_count": unresolved_issues,
                "attribution_coverage_summary": {
                    "coverage_ratio": coverage_ratio,
                    "counts_come_from": "outputs/operator_dashboard/paper_approved_models_snapshot.json",
                    "close_quality": "Lane-level close economics unavailable from current paper artifact set.",
                    "fill_evidence": "No attributed paper fills recorded yet." if fill_count == 0 else "Attributed paper fills present.",
                },
                "session_pocket_concentration": None,
                "top_1_contribution_concentration": None,
                "top_3_contribution_concentration": None,
                "paper_sessions_in_window": paper_sessions_in_window,
                "active_sessions_in_window": active_sessions_in_window,
                "blocked_sessions_in_window": blocked_sessions_in_window,
                "filled_sessions_in_window": filled_sessions_in_window,
                "open_risk_close_sessions_in_window": open_risk_close_sessions_in_window,
                "dirty_close_sessions_in_window": dirty_close_sessions_in_window,
                "halted_sessions_in_window": halted_sessions_in_window,
                "active_session_rate": active_session_rate,
                "fill_session_rate": fill_session_rate,
                "dirty_close_rate": dirty_close_rate,
                "halt_session_rate": halt_session_rate,
                "avg_attributable_realized_pnl_per_filled_session": avg_realized_per_filled_session,
                "median_attributable_realized_pnl_per_filled_session": median_realized_per_filled_session,
                "attributable_realized_pnl_total_window": realized_pnl,
                "attribution_coverage_rate": attribution_coverage_rate,
                "evidence_sufficiency_verdict": evidence_sufficiency,
                "retention_verdict": _retention_verdict(evidence_sufficiency, retention_warning_flags, realized_pnl),
                "retention_warning_flags": retention_warning_flags,
                "latest_activity_timestamp": detail.get("latest_activity_timestamp"),
                "latest_signal_timestamp": detail.get("latest_signal_timestamp"),
                "latest_fill_timestamp": detail.get("latest_fill_timestamp"),
                "latest_blocked_timestamp": detail.get("latest_blocked_timestamp"),
                "risk_state": risk_state,
                "reconciliation_state": detail.get("reconciliation_state"),
                "warnings": warnings,
                "verdict": _admitted_verdict(row, detail, coverage_ratio),
                "artifacts": detail.get("artifacts") or {},
            }
        )
    rows.sort(key=_retention_rank, reverse=True)
    evidence = {
        "session_date": soak.get("session_date"),
        "runtime_running": readiness.get("runtime_running"),
        "entries_enabled": readiness.get("entries_enabled"),
        "desk_flat_state": readiness.get("flat_state"),
        "operator_summary_generated": review_state.get("summary_generated"),
        "carry_forward_active": carry_forward.get("active"),
        "paper_signal_rows_in_db": db_stats["signal_rows"],
        "severe_exception_count": exceptions.get("severe_exception_count", exceptions.get("severity_counts", {}).get("severe", 0)),
        "archived_paper_artifact_sources": archived_artifacts,
        "archived_paper_artifacts_found": len(archived_artifacts),
    }
    return rows, evidence


def _render_metric(value: Any, places: int = 2, percent: bool = False) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "yes" if value else "no"
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value)
    if math.isinf(numeric):
        return "inf"
    if percent:
        return f"{numeric * 100:.1f}%"
    return f"{numeric:.{places}f}"


def _render_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    headers = [label for _, label in columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        rendered = []
        for key, _ in columns:
            value = row.get(key)
            if key.endswith("rate") or key in {"win_rate", "top_3_trade_concentration"}:
                rendered.append(_render_metric(value, percent=True))
            else:
                rendered.append(_render_metric(value))
        lines.append("| " + " | ".join(rendered) + " |")
    return "\n".join(lines)


def _render_markdown(pack: dict[str, Any]) -> str:
    summary = pack["executive_summary"]
    admitted_rows = pack["sections"]["current_admitted_paper_lanes"]["rows"]
    substrate = pack["sections"]["retention_history_substrate"]
    candidate_rows = pack["sections"]["next_tier_candidates"]["rows"]
    keep_list = summary["admitted_keep_recommendations"]
    review_list = summary["admitted_review_recommendations"]
    next_order = summary["next_admission_order"]
    lines = [
        "# Probationary Paper Lane Analyst Pack",
        "",
        f"Generated at: `{pack['generated_at']}`",
        "",
        "## Executive Summary",
        "",
        f"- Current admitted paper lanes reviewed: `{len(admitted_rows)}`",
        f"- Replay candidate rows reviewed: `{len(candidate_rows)}`",
        f"- Recommended keep list: `{', '.join(keep_list) if keep_list else 'none'}`",
        f"- Recommended review list: `{', '.join(review_list) if review_list else 'none'}`",
        f"- Admitted do-nothing-yet list: `{', '.join(summary['admitted_do_nothing_yet']) if summary['admitted_do_nothing_yet'] else 'none'}`",
        f"- Retention-history readiness: `{summary['retention_history_readiness']}`",
        f"- Recommended next-admission order: `{', '.join(next_order) if next_order else 'none'}`",
        f"- Candidate do-nothing-yet list: `{', '.join(summary['candidate_do_nothing_yet']) if summary['candidate_do_nothing_yet'] else 'none'}`",
        "",
        "### Global warnings",
        "",
    ]
    warnings = summary.get("global_warnings") or []
    if warnings:
        lines.extend(f"- {warning}" for warning in warnings)
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "## Section A: Current Admitted Paper Lanes",
            "",
            "Paper-lane judgments are based only on persisted paper artifacts. Replay results are not mixed into these verdicts.",
            "",
            _render_table(
                [
                    {
                        "lane": row["display_name"],
                        "evidence": row["evidence_sufficiency_verdict"],
                        "retention": row["retention_verdict"],
                        "paper_sessions": row["paper_sessions_in_window"],
                        "active_sessions": row["active_sessions_in_window"],
                        "filled_sessions": row["filled_sessions_in_window"],
                        "dirty_rate": row["dirty_close_rate"],
                        "window_realized_pnl": row["attributable_realized_pnl_total_window"],
                    }
                    for row in admitted_rows
                ],
                [
                    ("lane", "Lane"),
                    ("evidence", "Evidence"),
                    ("retention", "Retention"),
                    ("paper_sessions", "Paper sessions"),
                    ("active_sessions", "Active"),
                    ("filled_sessions", "Filled"),
                    ("dirty_rate", "Dirty close rate"),
                    ("window_realized_pnl", "Window realized P/L"),
                ],
            ),
            "",
        ]
    )
    for row in admitted_rows:
        lines.extend(
            [
                f"### {row['instrument']} / {row['branch']}",
                "",
                f"- Evidence sufficiency: `{row['evidence_sufficiency_verdict']}`",
                f"- Retention verdict: `{row['retention_verdict']}`",
                f"- Archived history present: `{row['archived_history_present']}` with `{row['archived_session_count']}` archived session(s); lane history usable: `{row['lane_history_usable']}`",
                f"- Archived file count: `{row['archived_file_count']}`",
                f"- Rolling window: paper `{row['paper_sessions_in_window']}`, active `{row['active_sessions_in_window']}`, filled `{row['filled_sessions_in_window']}`, dirty closes `{row['dirty_close_sessions_in_window']}`, halts `{row['halted_sessions_in_window']}`",
                f"- Missing history fields: `{', '.join(row['missing_history_fields']) if row['missing_history_fields'] else 'none'}`",
                f"- History gap reason: `{row['history_gap_reason'] or 'none'}`",
                f"- Retention warnings: `{', '.join(row['retention_warning_flags']) if row['retention_warning_flags'] else 'none'}`",
                f"- Latest-session warnings: `{', '.join(row['warnings']) if row['warnings'] else 'none'}`",
                f"- Attribution coverage: `{json.dumps(row['attribution_coverage_summary'], sort_keys=True)}`",
                "",
            ]
        )

    lines.extend(
        [
            "## Retention History Substrate",
            "",
            f"- Archived history present: `{substrate['archived_history_present']}`",
            f"- Archived file count: `{substrate['archived_file_count']}`",
            f"- Archived session count: `{substrate['archived_session_count']}`",
            f"- Retention-history readiness: `{substrate['retention_history_readiness_verdict']}`",
            "",
            "### Artifact Families Found",
            "",
        ]
    )
    if substrate["artifact_families_found"]:
        lines.extend(
            f"- `{family['family']}`: pattern `{family['path_pattern']}`, mode `{family['history_mode']}`, lane-aware `{family['lane_aware']}`, retention-sufficient `{family['retention_sufficient']}`"
            for family in substrate["artifact_families_found"]
        )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "### Minimal Retention Schema",
            "",
            _render_table(
                [
                    {
                        "field": item["field"],
                        "level": item["level"],
                        "classification": item["classification"],
                    }
                    for item in substrate["minimal_retention_schema"]
                ],
                [
                    ("field", "Field"),
                    ("level", "Level"),
                    ("classification", "Availability"),
                ],
            ),
            "",
            "### Future-Readiness Recommendation",
            "",
        ]
    )
    sufficient = substrate["recommendation"]["already_sufficient_artifacts"]
    lines.append(f"- Already sufficient artifacts: `{', '.join(sufficient) if sufficient else 'none'}`")
    for item in substrate["recommendation"]["missing_before_true_cumulative_scoring"]:
        lines.append(f"- {item}")

    lines.extend(
        [
            "## Section B: Next-Tier Candidates",
            "",
            "Replay-candidate judgments are based only on replay/research artifacts. These are not directly comparable to paper economics without that label.",
            "",
            _render_table(
                [
                    {
                        "lane": f"{row['instrument']} / {row['branch']}",
                        "variant": row["selected_variant"],
                        "verdict": row["verdict"],
                        "confidence": row["admission_readiness_confidence"],
                        "blocker": row["primary_blocker"],
                        "realized_pnl": row["metrics"]["realized_pnl"],
                        "trade_count": row["metrics"]["trade_count"],
                        "profit_factor": row["metrics"]["profit_factor"],
                    }
                    for row in candidate_rows
                ],
                [
                    ("lane", "Lane"),
                    ("variant", "Selected variant"),
                    ("verdict", "Verdict"),
                    ("confidence", "Confidence"),
                    ("blocker", "Primary blocker"),
                    ("realized_pnl", "Realized P/L"),
                    ("trade_count", "Trades"),
                    ("profit_factor", "Profit factor"),
                ],
            ),
            "",
        ]
    )
    for row in candidate_rows:
        metrics = row["metrics"]
        lines.extend(
            [
                f"### {row['instrument']} / {row['branch']}",
                "",
                f"- Cohort: `{row['cohort']}`",
                f"- Selected variant: `{row['selected_variant']}` from `{row['variant_count_considered']}` artifact(s)",
                f"- Verdict: `{row['verdict']}`",
                f"- Readiness confidence: `{row['admission_readiness_confidence']}`; Primary blocker: `{row['primary_blocker']}`",
                f"- Warnings: `{', '.join(row['warnings']) if row['warnings'] else 'none'}`",
                f"- Sample window: `{metrics['sample_start']}` -> `{metrics['sample_end']}`",
                f"- Trades: `{metrics['trade_count']}`; Profit factor: `{_render_metric(metrics['profit_factor'])}`; Win rate: `{_render_metric(metrics['win_rate'], percent=True)}`",
                f"- Concentration: top-1 `{_render_metric(metrics['top_1_trade_concentration'], percent=True)}`, top-3 `{_render_metric(metrics['top_3_trade_concentration'], percent=True)}`, session-pocket `{_render_metric(metrics['session_pocket_concentration'], percent=True)}`",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def build_pack() -> dict[str, Any]:
    admitted_rows, paper_evidence = _build_admitted_section()
    retention_substrate = _build_retention_history_substrate(admitted_rows)
    lane_history_lookup = {row["lane_id"]: row for row in retention_substrate["lane_history"]}
    for row in admitted_rows:
        history = lane_history_lookup.get(row["lane_id"], {})
        row["archived_history_present"] = retention_substrate["archived_history_present"]
        row["archived_file_count"] = retention_substrate["archived_file_count"]
        row["archived_session_count"] = retention_substrate["archived_session_count"]
        row["lane_history_usable"] = history.get("lane_history_usable", False)
        row["missing_history_fields"] = history.get("missing_history_fields", [])
        row["history_gap_reason"] = history.get("history_gap_reason")
    candidate_rows = _select_best_candidates()

    admitted_keep = [
        row["display_name"]
        for row in admitted_rows
        if row["retention_verdict"] in {"KEEP_RUNNING", "KEEP_RUNNING_UNDER_WATCH"}
    ]
    admitted_review = [
        row["display_name"]
        for row in admitted_rows
        if row["retention_verdict"] == "REVIEW_FOR_REMOVAL"
    ]
    admitted_do_nothing_yet = [
        row["display_name"]
        for row in admitted_rows
        if row["retention_verdict"] == "TOO_EARLY_TO_JUDGE"
    ]
    next_admission_order = [
        f"{row['instrument']} / {row['branch']}"
        for row in sorted(
            [row for row in candidate_rows if row["cohort"] == "NEXT_TIER" and row["verdict"] != "DROP_FOR_NOW"],
            key=_next_tier_ranking_score,
            reverse=True,
        )
    ]
    candidate_do_nothing_yet = [
        f"{row['instrument']} / {row['branch']}"
        for row in candidate_rows
        if row["cohort"] == "NEXT_TIER" and row["admission_readiness_confidence"] == "LOW"
    ]

    global_warnings: list[str] = []
    if all(row["sessions_with_fills"] == 0 for row in admitted_rows):
        global_warnings.append("Current admitted paper lanes have no attributed paper fills yet; keep/review calls are evidence-light.")
    if any("SESSION_SUMMARY_MISSING" in row["warnings"] for row in admitted_rows):
        global_warnings.append("Paper review bundle is incomplete because the latest paper summary is missing.")
    if retention_substrate["archived_session_count"] == 0:
        global_warnings.append("No signed-off archived paper lane history exists yet, so admitted-lane retention calls still fail closed to the current session only.")
    elif retention_substrate["retention_history_readiness_verdict"] == "RETENTION_HISTORY_PARTIAL":
        global_warnings.append("Archived paper lane history now exists, but it is still too thin for true cumulative retention scoring.")
    if any(row["warnings"] for row in candidate_rows if row["verdict"] != "NEXT_ADMISSION_CANDIDATE"):
        global_warnings.append("Several replay candidates remain concentration- or sample-sensitive even when raw P/L is positive.")
    if not any(row["cohort"] == "NEXT_TIER" and row["verdict"] == "NEXT_ADMISSION_CANDIDATE" for row in candidate_rows):
        global_warnings.append("No non-admitted futures lane currently clears the next-admission bar on replay breadth and concentration together.")

    pack = {
        "generated_at": _iso_now(),
        "output_dir": str(OUTPUT_DIR),
        "artifact_sources": {
            "paper": [
                str(OPERATOR_OUTPUT_DIR / "paper_approved_models_snapshot.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_readiness_snapshot.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_soak_session_snapshot.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_review_state.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_carry_forward_state.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_exceptions_snapshot.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_latest_intents_snapshot.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_latest_fills_snapshot.json"),
                str(OPERATOR_OUTPUT_DIR / "paper_latest_blotter_snapshot.json"),
                str(PAPER_LANE_HISTORY_DIR),
                str(PAPER_DB_PATH),
            ],
            "replay": [str(path) for path in sorted(REPLAY_OUTPUT_DIR.glob("*.summary.json")) if "approved" in path.name],
        },
        "metrics_included": {
            "section_a_current_admitted_paper_lanes": [
                "sessions_observed",
                "sessions_active",
                "sessions_blocked_only",
                "sessions_signaled",
                "sessions_with_intents",
                "sessions_with_fills",
                "sessions_with_open_risk_at_close",
                "clean_close_count",
                "dirty_close_count",
                "halted_by_risk_count",
                "total_attributable_realized_pnl",
                "avg_attributable_realized_pnl_per_active_session",
                "win_rate",
                "fill_rate",
                "block_rate",
                "halt_rate",
                "unresolved_intent_fill_issue_count",
                "attribution_coverage_summary",
                "session_pocket_concentration",
                "top_1_contribution_concentration",
                "top_3_contribution_concentration",
                "paper_sessions_in_window",
                "active_sessions_in_window",
                "blocked_sessions_in_window",
                "filled_sessions_in_window",
                "open_risk_close_sessions_in_window",
                "dirty_close_sessions_in_window",
                "halted_sessions_in_window",
                "active_session_rate",
                "fill_session_rate",
                "dirty_close_rate",
                "halt_session_rate",
                "avg_attributable_realized_pnl_per_filled_session",
                "median_attributable_realized_pnl_per_filled_session",
                "attributable_realized_pnl_total_window",
                "attribution_coverage_rate",
                "evidence_sufficiency_verdict",
                "retention_verdict",
                "retention_warning_flags",
                "archived_history_present",
                "archived_file_count",
                "archived_session_count",
                "lane_history_usable",
                "missing_history_fields",
                "history_gap_reason",
            ],
            "section_b_next_tier_candidates": [
                "sample_start",
                "sample_end",
                "sessions_used",
                "bars_used",
                "realized_pnl",
                "profit_factor",
                "max_drawdown",
                "trade_sharpe_proxy",
                "trade_count",
                "top_1_trade_concentration",
                "top_3_trade_concentration",
                "survives_without_top_1",
                "survives_without_top_3",
                "session_pocket_concentration",
                "exact_vs_home_case_horizon_match",
                "admission_readiness_confidence",
                "primary_blocker",
            ],
        },
        "verdict_buckets": {
            "admitted": list(ADMITTED_VERDICTS),
            "admitted_evidence_sufficiency": list(EVIDENCE_SUFFICIENCY_BUCKETS),
            "admitted_retention": list(RETENTION_VERDICTS),
            "candidates": list(CANDIDATE_VERDICTS),
            "candidate_confidence": list(CANDIDATE_CONFIDENCE_BUCKETS),
            "candidate_primary_blocker": list(CANDIDATE_PRIMARY_BLOCKERS),
            "retention_history_readiness": list(RETENTION_HISTORY_READINESS_BUCKETS),
        },
        "executive_summary": {
            "global_warnings": global_warnings,
            "paper_evidence_context": paper_evidence,
            "retention_history_readiness": retention_substrate["retention_history_readiness_verdict"],
            "admitted_keep_recommendations": admitted_keep,
            "admitted_review_recommendations": admitted_review,
            "admitted_do_nothing_yet": admitted_do_nothing_yet,
            "next_admission_order": next_admission_order,
            "candidate_do_nothing_yet": candidate_do_nothing_yet,
        },
        "sections": {
            "current_admitted_paper_lanes": {
                "scope_note": "Paper-lane verdicts are based only on persisted paper artifacts and degrade confidence when lane-level attribution is incomplete.",
                "rows": admitted_rows,
            },
            "retention_history_substrate": retention_substrate,
            "next_tier_candidates": {
                "scope_note": "Replay-candidate verdicts are based only on replay/research artifacts and are not directly comparable to paper economics without that label.",
                "rows": sorted(candidate_rows, key=lambda item: (item["verdict"], item["instrument"], item["branch"])),
            },
        },
    }
    return pack


def write_pack(pack: dict[str, Any]) -> dict[str, str]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
    archive_json = ARCHIVE_DIR / f"paper_lane_analyst_pack_{stamp}.json"
    archive_md = ARCHIVE_DIR / f"paper_lane_analyst_pack_{stamp}.md"
    markdown = _render_markdown(pack)
    _write_json(LATEST_JSON_PATH, pack)
    LATEST_MD_PATH.write_text(markdown, encoding="utf-8")
    _write_json(archive_json, pack)
    archive_md.write_text(markdown, encoding="utf-8")
    return {
        "latest_json": str(LATEST_JSON_PATH),
        "latest_md": str(LATEST_MD_PATH),
        "archive_json": str(archive_json),
        "archive_md": str(archive_md),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="paper-lane-analyst-pack")
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Build the analyst pack and print JSON without writing latest/archive artifacts.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    pack = build_pack()
    outputs = None if args.no_write else write_pack(pack)
    payload = {
        "outputs": outputs,
        "summary": pack["executive_summary"],
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
