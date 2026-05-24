"""Track B weekly maintenance orchestrator v1.

The orchestrator composes maintenance lanes into one PAPER-mode report. It
does not create broker/order/lifecycle authority and never deletes, moves,
compresses, archives, submits, cancels, replaces, closes, flattens, or invokes
paper proof.
"""

from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from mgc_v05l.app.weekly_data_maintenance import (
    WeeklyMaintenanceConfig,
    build_weekly_data_maintenance_report,
    write_weekly_data_maintenance_report,
)
from mgc_v05l.execution_core.track_b_artifact_archive_executor import (
    ARCHIVE_EXECUTOR_APPLY_DISABLED,
    ARCHIVE_EXECUTOR_DRY_RUN_READY,
    TrackBArtifactArchiveExecutorConfig,
    build_track_b_artifact_archive_execution_plan,
    write_track_b_artifact_archive_execution_plan,
)
from mgc_v05l.execution_core.track_b_artifact_archive_planner import (
    ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY,
    ARCHIVE_PLAN_EMPTY,
    ARCHIVE_PLAN_READY,
    TrackBArtifactArchivePlannerConfig,
    build_track_b_artifact_archive_plan,
    write_track_b_artifact_archive_plan,
)
from mgc_v05l.execution_core.track_b_artifact_retention_inventory import (
    ARTIFACT_RETENTION_INVENTORY_READY,
    TrackBArtifactRetentionInventoryConfig,
    build_track_b_artifact_retention_inventory,
    write_track_b_artifact_retention_inventory,
)
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.paths import ARCHIVED_ROOT_FRAGMENTS, PROJECT_ROOT


WEEKLY_MAINTENANCE_READY = "WEEKLY_MAINTENANCE_READY"
WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS = "WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS"
WEEKLY_MAINTENANCE_PROOF_BLOCKED = "WEEKLY_MAINTENANCE_PROOF_BLOCKED"
WEEKLY_MAINTENANCE_INCOMPLETE = "WEEKLY_MAINTENANCE_INCOMPLETE"
WEEKLY_MAINTENANCE_FAILED = "WEEKLY_MAINTENANCE_FAILED"

LANE_READY = "LANE_READY"
LANE_DIAGNOSTIC_WARNING = "LANE_DIAGNOSTIC_WARNING"
LANE_PROOF_BLOCKED = "LANE_PROOF_BLOCKED"
LANE_INCOMPLETE = "LANE_INCOMPLETE"
LANE_FAILED = "LANE_FAILED"

LANE_IDS = (
    "historical_data_maintenance",
    "artifact_hygiene",
    "artifact_retention_inventory",
    "artifact_archive_planner",
    "artifact_archive_executor_boundary",
    "research_offline_labeling_check",
    "old_root_contamination_check",
    "final_control_plane_readiness_context",
)

REPO_ROOT = PROJECT_ROOT
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "weekly_maintenance"
    / "latest_weekly_maintenance_orchestrator.json"
)
DEFAULT_MARKDOWN_REPORT_ROOT = Path("outputs") / "reports" / "weekly_maintenance"
DEFAULT_DATA_MAINTENANCE_REPORT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "track_b_data_maintenance"
    / "latest_track_b_data_maintenance_report.json"
)


@dataclass(frozen=True)
class TrackBWeeklyMaintenanceOrchestratorConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    markdown_report_root: Path = DEFAULT_MARKDOWN_REPORT_ROOT
    data_maintenance_report_path: Path = DEFAULT_DATA_MAINTENANCE_REPORT_PATH
    historical_data_proof_critical: bool = False
    max_research_files: int = 1000
    max_old_root_scan_files: int = 5000
    write_lane_artifacts: bool = True

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_weekly_maintenance_orchestrator(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime | None = None,
    lane_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    overrides = lane_overrides or {}
    lanes: list[dict[str, Any]] = []
    for lane_id in LANE_IDS:
        if lane_id in overrides:
            lanes.append(_normalize_lane_result(lane_id=lane_id, payload=overrides[lane_id]))
            continue
        try:
            lanes.append(_run_lane(lane_id=lane_id, config=config, now=actual_now, lanes=lanes))
        except Exception as exc:  # noqa: BLE001 - maintenance failures must be visible, not hidden.
            lanes.append(
                _lane_result(
                    lane_id=lane_id,
                    classification=LANE_FAILED,
                    status="failed",
                    reason=f"{type(exc).__name__}: {exc}",
                    proof_blocking=False,
                    diagnostic_only=True,
                )
            )

    missing_lanes = sorted(set(LANE_IDS) - {str(lane.get("lane_id") or "") for lane in lanes})
    proof_blockers = _proof_blocking_findings(lanes)
    diagnostics = _diagnostic_findings(lanes)
    failures = [lane for lane in lanes if lane.get("classification") == LANE_FAILED]
    incomplete = [lane for lane in lanes if lane.get("classification") == LANE_INCOMPLETE]
    overall = _overall_classification(
        missing_lanes=missing_lanes,
        failures=failures,
        incomplete=incomplete,
        proof_blockers=proof_blockers,
        diagnostics=diagnostics,
    )
    artifact_archive_lane = _lane_by_id(lanes, "artifact_archive_planner")
    historical_lane = _lane_by_id(lanes, "historical_data_maintenance")
    old_root_lane = _lane_by_id(lanes, "old_root_contamination_check")
    return {
        "schema_version": "track_b_weekly_maintenance_orchestrator_v1",
        "generated_at": actual_now.isoformat(),
        "weekly_maintenance_orchestrator_id": f"track_b_weekly_maintenance_{uuid.uuid4().hex}",
        "mode": "PAPER",
        "overall_classification": overall,
        "lanes_run": [lane.get("lane_id") for lane in lanes],
        "lanes_failed": [lane.get("lane_id") for lane in failures],
        "lanes_incomplete": [lane.get("lane_id") for lane in incomplete],
        "missing_lanes": missing_lanes,
        "proof_blocking_findings": proof_blockers,
        "diagnostic_findings": diagnostics,
        "archive_posture": artifact_archive_lane.get("summary", {}),
        "historical_data_posture": historical_lane.get("summary", {}),
        "old_root_hits": old_root_lane.get("summary", {}).get("old_root_hits", []),
        "recommended_actions": _recommended_actions(overall, lanes),
        "dry_run_only": True,
        "broker_mutation_allowed": False,
        "order_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "submit_allowed": False,
        "cancel_allowed": False,
        "replace_allowed": False,
        "close_allowed": False,
        "flatten_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "archive_apply_enabled": False,
        "archive_delete_enabled": False,
        "archive_compression_enabled": False,
        "maintenance_creates_authority": False,
        "dashboard_projection_consumed": False,
        "lanes": lanes,
        "source_artifact_paths": {
            "weekly_maintenance_orchestrator": str(config.resolve(config.output_path)),
            "data_maintenance_report": str(config.resolve(config.data_maintenance_report_path)),
        },
    }


def write_track_b_weekly_maintenance_orchestrator(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    payload: Mapping[str, Any],
) -> dict[str, Path]:
    json_path = write_json_atomic(config.resolve(config.output_path), dict(payload))
    report_root = config.resolve(config.markdown_report_root)
    report_root.mkdir(parents=True, exist_ok=True)
    stamp = str(payload.get("generated_at") or "unknown").split("T", maxsplit=1)[0]
    latest_markdown = report_root / "latest_weekly_maintenance_orchestrator.md"
    dated_markdown = report_root / f"weekly_maintenance_{stamp}.md"
    markdown = _render_markdown(payload)
    latest_markdown.write_text(markdown, encoding="utf-8")
    dated_markdown.write_text(markdown, encoding="utf-8")
    return {"json": json_path, "markdown": latest_markdown, "dated_markdown": dated_markdown}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write Track B weekly maintenance orchestrator posture.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--markdown-report-root", type=Path, default=DEFAULT_MARKDOWN_REPORT_ROOT)
    parser.add_argument("--historical-data-proof-critical", action="store_true")
    parser.add_argument("--no-write", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBWeeklyMaintenanceOrchestratorConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        markdown_report_root=Path(args.markdown_report_root),
        historical_data_proof_critical=bool(args.historical_data_proof_critical),
    )
    payload = build_track_b_weekly_maintenance_orchestrator(config=config)
    written: dict[str, Path] = {}
    if not bool(args.no_write):
        written = write_track_b_weekly_maintenance_orchestrator(config=config, payload=payload)
    summary = {
        "overall_classification": payload.get("overall_classification"),
        "lanes_run": len(payload.get("lanes_run") or []),
        "lanes_failed": payload.get("lanes_failed"),
        "proof_blocking_findings": payload.get("proof_blocking_findings"),
        "diagnostic_findings_count": len(payload.get("diagnostic_findings") or []),
        "dry_run_only": True,
        "broker_mutation_allowed": False,
        "output_path": str(written.get("json") or config.resolve(config.output_path)),
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    if payload.get("overall_classification") in {
        WEEKLY_MAINTENANCE_READY,
        WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS,
    }:
        return 0
    return 2


def _run_lane(
    *,
    lane_id: str,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime,
    lanes: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if lane_id == "historical_data_maintenance":
        return _historical_data_maintenance_lane(config=config, now=now)
    if lane_id == "artifact_hygiene":
        return _artifact_hygiene_lane(config=config, now=now)
    if lane_id == "artifact_retention_inventory":
        return _artifact_retention_inventory_lane(config=config, now=now)
    if lane_id == "artifact_archive_planner":
        return _artifact_archive_planner_lane(config=config, now=now)
    if lane_id == "artifact_archive_executor_boundary":
        return _artifact_archive_executor_lane(config=config, now=now)
    if lane_id == "research_offline_labeling_check":
        return _research_offline_labeling_lane(config=config)
    if lane_id == "old_root_contamination_check":
        return _old_root_contamination_lane(config=config)
    if lane_id == "final_control_plane_readiness_context":
        return _final_context_lane(lanes=lanes)
    return _lane_result(
        lane_id=lane_id,
        classification=LANE_INCOMPLETE,
        status="missing",
        reason="Unknown weekly maintenance lane.",
        proof_blocking=False,
        diagnostic_only=True,
    )


def _historical_data_maintenance_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime,
) -> dict[str, Any]:
    path = config.resolve(config.data_maintenance_report_path)
    report = _read_json(path)
    cutoff = _prior_friday_close_utc(now)
    if not report:
        classification = LANE_PROOF_BLOCKED if config.historical_data_proof_critical else LANE_DIAGNOSTIC_WARNING
        return _lane_result(
            lane_id="historical_data_maintenance",
            classification=classification,
            status="missing",
            reason="Latest Track B data maintenance report is missing.",
            proof_blocking=config.historical_data_proof_critical,
            diagnostic_only=not config.historical_data_proof_critical,
            summary={
                "expected_command": "python -m mgc_v05l.execution_core.track_b_data_maintenance_cli",
                "expected_complete_through_prior_friday_close": cutoff.isoformat(),
                "report_path": str(path),
            },
        )
    latest_bar = _parse_datetime(report.get("latest_bar_timestamp"))
    complete = report.get("complete_through_cutoff") is True
    ready = report.get("data_maintenance_verdict") == "TRACK_B_DATA_MAINTENANCE_UPDATED_HISTORY_READY"
    cutoff_ready = latest_bar is not None and latest_bar >= cutoff
    ok = ready and complete and cutoff_ready
    classification = LANE_READY
    if not ok:
        classification = LANE_PROOF_BLOCKED if config.historical_data_proof_critical else LANE_DIAGNOSTIC_WARNING
    reason = (
        "MGC 1m historical maintenance is complete through prior Friday close."
        if ok
        else "MGC 1m historical maintenance is stale or incomplete."
    )
    return _lane_result(
        lane_id="historical_data_maintenance",
        classification=classification,
        status="ready" if ok else "attention",
        reason=reason,
        proof_blocking=not ok and config.historical_data_proof_critical,
        diagnostic_only=not config.historical_data_proof_critical,
        summary={
            "expected_command": "python -m mgc_v05l.execution_core.track_b_data_maintenance_cli",
            "data_maintenance_verdict": report.get("data_maintenance_verdict"),
            "latest_bar_timestamp": report.get("latest_bar_timestamp"),
            "complete_through_cutoff": complete,
            "expected_complete_through_prior_friday_close": cutoff.isoformat(),
            "proof_critical": config.historical_data_proof_critical,
            "report_path": str(path),
        },
    )


def _artifact_hygiene_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime,
) -> dict[str, Any]:
    weekly_config = WeeklyMaintenanceConfig(repo_root=config.repo_root, week_ending=now.date(), mode="dry-run")
    report = build_weekly_data_maintenance_report(config=weekly_config)
    written: dict[str, Path] = {}
    if config.write_lane_artifacts:
        written = write_weekly_data_maintenance_report(config=weekly_config, report=report)
    verdict = str(report.get("final_verdict") or "")
    ok = verdict == "DRY_RUN_ONLY_READY"
    classification = LANE_READY if ok else LANE_DIAGNOSTIC_WARNING
    return _lane_result(
        lane_id="artifact_hygiene",
        classification=classification,
        status="ready" if ok else "diagnostic",
        reason="Weekly data maintenance dry-run completed.",
        proof_blocking=False,
        diagnostic_only=True,
        summary={
            "weekly_data_maintenance_verdict": verdict,
            "delete_candidates_count": report.get("delete_candidates_count"),
            "archive_candidates_count": report.get("archive_candidates_count"),
            "old_root_hits": report.get("old_root_hits", []),
            "report_path": str(written.get("json") or ""),
            "dry_run_only": True,
        },
    )


def _artifact_retention_inventory_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime,
) -> dict[str, Any]:
    inventory_config = TrackBArtifactRetentionInventoryConfig(repo_root=config.repo_root)
    payload = build_track_b_artifact_retention_inventory(config=inventory_config, now=now)
    output_path: Path | None = None
    if config.write_lane_artifacts:
        output_path = write_track_b_artifact_retention_inventory(config=inventory_config, payload=payload)
    ok = payload.get("classification") == ARTIFACT_RETENTION_INVENTORY_READY
    return _lane_result(
        lane_id="artifact_retention_inventory",
        classification=LANE_READY if ok else LANE_DIAGNOSTIC_WARNING,
        status="ready" if ok else "diagnostic",
        reason="Artifact retention inventory completed in dry-run mode.",
        proof_blocking=False,
        diagnostic_only=True,
        summary={
            "classification": payload.get("classification"),
            **_mapping(payload.get("summary")),
            "output_path": str(output_path or inventory_config.resolve(inventory_config.output_path)),
            "dry_run_only": True,
        },
    )


def _artifact_archive_planner_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime,
) -> dict[str, Any]:
    planner_config = TrackBArtifactArchivePlannerConfig(repo_root=config.repo_root)
    payload = build_track_b_artifact_archive_plan(config=planner_config, now=now)
    output_path: Path | None = None
    if config.write_lane_artifacts:
        output_path = write_track_b_artifact_archive_plan(config=planner_config, payload=payload)
    classification = str(payload.get("classification") or "")
    active_authority_risk = classification == ARCHIVE_PLAN_BLOCKED_ACTIVE_AUTHORITY
    ok = classification in {ARCHIVE_PLAN_READY, ARCHIVE_PLAN_EMPTY}
    return _lane_result(
        lane_id="artifact_archive_planner",
        classification=LANE_READY if ok else (LANE_PROOF_BLOCKED if active_authority_risk else LANE_DIAGNOSTIC_WARNING),
        status="ready" if ok else "attention",
        reason="Archive planner completed; execution remains disabled.",
        proof_blocking=active_authority_risk,
        diagnostic_only=not active_authority_risk,
        summary={
            "classification": classification,
            "hot_authority_protected_count": payload.get("hot_authority_protected_count"),
            "active_lifecycle_protected_count": payload.get("active_lifecycle_protected_count"),
            "warm_diagnostic_count": payload.get("warm_diagnostic_count"),
            "cold_archive_candidate_count": payload.get("cold_archive_candidate_count"),
            "blocked_candidate_count": payload.get("blocked_candidate_count"),
            "estimated_bytes": payload.get("estimated_bytes"),
            "output_path": str(output_path or planner_config.resolve(planner_config.output_path)),
            "dry_run_only": True,
            "execution_enabled": False,
        },
    )


def _artifact_archive_executor_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime,
) -> dict[str, Any]:
    executor_config = TrackBArtifactArchiveExecutorConfig(repo_root=config.repo_root)
    payload = build_track_b_artifact_archive_execution_plan(config=executor_config, now=now)
    output_path: Path | None = None
    if config.write_lane_artifacts:
        output_path = write_track_b_artifact_archive_execution_plan(config=executor_config, payload=payload)
    ok = payload.get("classification") in {ARCHIVE_EXECUTOR_DRY_RUN_READY, ARCHIVE_EXECUTOR_APPLY_DISABLED}
    return _lane_result(
        lane_id="artifact_archive_executor_boundary",
        classification=LANE_READY if ok else LANE_DIAGNOSTIC_WARNING,
        status="ready" if ok else "blocked",
        reason="Archive execution boundary validated; apply remains disabled.",
        proof_blocking=False,
        diagnostic_only=True,
        summary={
            "classification": payload.get("classification"),
            "candidate_count": payload.get("candidate_count"),
            "blocked_reason": payload.get("blocked_reason"),
            "would_move_files": False,
            "would_delete_files": False,
            "would_compress": False,
            "execution_enabled": False,
            "apply_enabled": False,
            "output_path": str(output_path or executor_config.resolve(executor_config.output_path)),
        },
    )


def _research_offline_labeling_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
) -> dict[str, Any]:
    research_root = config.repo_root / "outputs" / "track_b_research"
    checked = 0
    unlabeled: list[str] = []
    if research_root.exists():
        for path in sorted(research_root.rglob("*.json")):
            if checked >= config.max_research_files:
                break
            checked += 1
            payload = _read_json(path)
            if not _research_offline_labeled(payload):
                unlabeled.append(_relative_text(path, config.repo_root))
    return _lane_result(
        lane_id="research_offline_labeling_check",
        classification=LANE_READY if not unlabeled else LANE_DIAGNOSTIC_WARNING,
        status="ready" if not unlabeled else "diagnostic",
        reason=(
            "Research/offline artifacts are labeled non-runtime authority."
            if not unlabeled
            else "Some research/offline artifacts lack non-runtime authority labels."
        ),
        proof_blocking=False,
        diagnostic_only=True,
        summary={
            "research_root": str(research_root),
            "checked_count": checked,
            "unlabeled_count": len(unlabeled),
            "unlabeled_sample": unlabeled[:10],
            "not_runtime_authority_required": True,
        },
    )


def _old_root_contamination_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
) -> dict[str, Any]:
    hits = _old_root_hits(repo_root=config.repo_root, max_files=config.max_old_root_scan_files)
    return _lane_result(
        lane_id="old_root_contamination_check",
        classification=LANE_READY if not hits else LANE_PROOF_BLOCKED,
        status="ready" if not hits else "blocked",
        reason=(
            "No archived/Documents/iCloud root fragments found."
            if not hits
            else "Archived-root fragments found in active project files."
        ),
        proof_blocking=bool(hits),
        diagnostic_only=False,
        summary={
            "old_root_hits": hits[:20],
            "old_root_hit_count": len(hits),
            "fragments": list(ARCHIVED_ROOT_FRAGMENTS),
        },
    )


def _final_context_lane(*, lanes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    proof_blockers = _proof_blocking_findings(lanes)
    diagnostics = _diagnostic_findings(lanes)
    return _lane_result(
        lane_id="final_control_plane_readiness_context",
        classification=LANE_READY if not proof_blockers else LANE_PROOF_BLOCKED,
        status="ready" if not proof_blockers else "blocked",
        reason=(
            "Weekly maintenance found no proof-blocking maintenance issues."
            if not proof_blockers
            else "Weekly maintenance found proof-blocking issues."
        ),
        proof_blocking=bool(proof_blockers),
        diagnostic_only=False,
        summary={
            "sunday_proof_posture": (
                "PROCEED_AFTER_FRESH_BARS_RETURN" if not proof_blockers else "BLOCKED_BY_MAINTENANCE"
            ),
            "proof_blocking_count": len(proof_blockers),
            "diagnostic_count": len(diagnostics),
            "maintenance_creates_broker_order_lifecycle_authority": False,
        },
    )


def _lane_result(
    *,
    lane_id: str,
    classification: str,
    status: str,
    reason: str,
    proof_blocking: bool,
    diagnostic_only: bool,
    summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "lane_id": lane_id,
        "classification": classification,
        "status": status,
        "reason": reason,
        "proof_blocking": proof_blocking,
        "diagnostic_only": diagnostic_only,
        "dry_run_only": True,
        "broker_mutation_allowed": False,
        "order_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "summary": dict(summary or {}),
    }


def _normalize_lane_result(*, lane_id: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    result = _lane_result(
        lane_id=lane_id,
        classification=str(payload.get("classification") or LANE_READY),
        status=str(payload.get("status") or "ready"),
        reason=str(payload.get("reason") or "Injected lane result."),
        proof_blocking=payload.get("proof_blocking") is True,
        diagnostic_only=payload.get("diagnostic_only") is not False,
        summary=_mapping(payload.get("summary")),
    )
    result.update({key: value for key, value in payload.items() if key not in result})
    result["lane_id"] = lane_id
    return result


def _overall_classification(
    *,
    missing_lanes: Sequence[str],
    failures: Sequence[Mapping[str, Any]],
    incomplete: Sequence[Mapping[str, Any]],
    proof_blockers: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
) -> str:
    if failures:
        return WEEKLY_MAINTENANCE_FAILED
    if missing_lanes or incomplete:
        return WEEKLY_MAINTENANCE_INCOMPLETE
    if proof_blockers:
        return WEEKLY_MAINTENANCE_PROOF_BLOCKED
    if diagnostics:
        return WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS
    return WEEKLY_MAINTENANCE_READY


def _proof_blocking_findings(lanes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "lane_id": str(lane.get("lane_id") or ""),
            "classification": lane.get("classification"),
            "reason": lane.get("reason"),
        }
        for lane in lanes
        if lane.get("proof_blocking") is True
    ]


def _diagnostic_findings(lanes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "lane_id": str(lane.get("lane_id") or ""),
            "classification": lane.get("classification"),
            "reason": lane.get("reason"),
        }
        for lane in lanes
        if lane.get("diagnostic_only") is True and lane.get("classification") != LANE_READY
    ]


def _recommended_actions(overall: str, lanes: Sequence[Mapping[str, Any]]) -> list[str]:
    if overall == WEEKLY_MAINTENANCE_READY:
        return ["No weekly maintenance action required before proof."]
    actions: list[str] = []
    for finding in [*_proof_blocking_findings(lanes), *_diagnostic_findings(lanes)]:
        actions.append(f"{finding['lane_id']}: {finding['reason']}")
    return actions or ["Review incomplete weekly maintenance lanes."]


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Weekly Maintenance",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- overall_classification: {payload.get('overall_classification')}",
        f"- dry_run_only: {payload.get('dry_run_only')}",
        f"- broker_mutation_allowed: {payload.get('broker_mutation_allowed')}",
        f"- proof_blocking_findings: {len(payload.get('proof_blocking_findings') or [])}",
        f"- diagnostic_findings: {len(payload.get('diagnostic_findings') or [])}",
        "",
        "## Lanes",
    ]
    for lane in _list(payload.get("lanes")):
        summary = _mapping(lane).get("summary") or {}
        lines.append(
            f"- {lane.get('lane_id')}: {lane.get('classification')} - {lane.get('reason')}"
        )
        if isinstance(summary, Mapping) and summary.get("classification"):
            lines.append(f"  - lane_summary_classification: {summary.get('classification')}")
    lines.extend(["", "## Recommended Actions"])
    for action in _list(payload.get("recommended_actions")):
        lines.append(f"- {action}")
    lines.append("")
    return "\n".join(lines)


def _lane_by_id(lanes: Sequence[Mapping[str, Any]], lane_id: str) -> Mapping[str, Any]:
    for lane in lanes:
        if lane.get("lane_id") == lane_id:
            return lane
    return {}


def _old_root_hits(*, repo_root: Path, max_files: int) -> list[str]:
    hits: list[str] = []
    scanned = 0
    for root in (repo_root / "config", repo_root / "scripts", repo_root / "src"):
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if scanned >= max_files:
                return hits
            if not path.is_file():
                continue
            scanned += 1
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                if any(fragment in line for fragment in ARCHIVED_ROOT_FRAGMENTS):
                    hits.append(f"{_relative_text(path, repo_root)}:{line_number}")
                    break
    return hits


def _prior_friday_close_utc(now: datetime) -> datetime:
    eastern = ZoneInfo("America/New_York")
    local_now = _ensure_utc(now).astimezone(eastern)
    days_since_friday = (local_now.weekday() - 4) % 7
    friday = local_now.date() - timedelta(days=days_since_friday)
    if days_since_friday == 0 and (local_now.hour, local_now.minute, local_now.second) < (17, 0, 0):
        friday -= timedelta(days=7)
    local_close = datetime.combine(friday, time(17, 0), tzinfo=eastern)
    return local_close.astimezone(UTC)


def _research_offline_labeled(payload: Mapping[str, Any]) -> bool:
    metadata = _mapping(payload.get("research_offline_metadata"))
    combined = {**payload, **metadata}
    return (
        combined.get("research_only") is True
        and combined.get("offline_diagnostic") is True
        and combined.get("not_runtime_authority") is True
        and combined.get("not_broker_truth") is not False
        and combined.get("not_market_data_runtime_truth") is not False
        and combined.get("not_routing_authority") is True
    )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _relative_text(path: Path, repo_root: Path) -> str:
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


if __name__ == "__main__":
    raise SystemExit(main())
