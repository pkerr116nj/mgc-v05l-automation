"""Track B weekly maintenance orchestrator v1.

The orchestrator composes maintenance lanes into one PAPER-mode report and
classifies the Saturday/Sunday retry window. It does not create
broker/order/lifecycle authority and never deletes, moves, compresses,
archives, submits, cancels, replaces, closes, flattens, or invokes paper proof.
"""

from __future__ import annotations

import argparse
import json
import os
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
    ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT,
    ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE,
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
from mgc_v05l.execution_core.generated_artifact_retention import (
    CHECK_OK,
    RetentionConfig,
    check_generated_artifacts,
    maintain_generated_artifacts,
)
from mgc_v05l.paths import ARCHIVED_ROOT_FRAGMENTS, PROJECT_ROOT


WEEKLY_MAINTENANCE_READY = "WEEKLY_MAINTENANCE_READY"
WEEKLY_MAINTENANCE_ALREADY_COMPLETE = "WEEKLY_MAINTENANCE_ALREADY_COMPLETE"
WEEKLY_MAINTENANCE_RETRY_SCHEDULED = "WEEKLY_MAINTENANCE_RETRY_SCHEDULED"
WEEKLY_MAINTENANCE_ALERT_REQUIRED = "WEEKLY_MAINTENANCE_ALERT_REQUIRED"
WEEKLY_MAINTENANCE_WINDOW_EXPIRED = "WEEKLY_MAINTENANCE_WINDOW_EXPIRED"
WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS = "WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS"
WEEKLY_MAINTENANCE_PROOF_BLOCKED = "WEEKLY_MAINTENANCE_PROOF_BLOCKED"
WEEKLY_MAINTENANCE_INCOMPLETE = "WEEKLY_MAINTENANCE_INCOMPLETE"
WEEKLY_MAINTENANCE_FAILED = "WEEKLY_MAINTENANCE_FAILED"

COMPLETION_COMPLETE = "COMPLETE"
COMPLETION_COMPLETE_WITH_DIAGNOSTICS = "COMPLETE_WITH_DIAGNOSTICS"
COMPLETION_INCOMPLETE = "INCOMPLETE"
COMPLETION_PROOF_BLOCKED = "PROOF_BLOCKED"
COMPLETION_WINDOW_EXPIRED = "WINDOW_EXPIRED"

LANE_READY = "LANE_READY"
LANE_DIAGNOSTIC_WARNING = "LANE_DIAGNOSTIC_WARNING"
LANE_PROOF_BLOCKED = "LANE_PROOF_BLOCKED"
LANE_INCOMPLETE = "LANE_INCOMPLETE"
LANE_FAILED = "LANE_FAILED"

ACTIVE_PROOF_PATH_BLOCKER = "ACTIVE_PROOF_PATH_BLOCKER"
ACTIVE_RUNTIME_PATH_BLOCKER = "ACTIVE_RUNTIME_PATH_BLOCKER"
RESEARCH_OR_OFFLINE_DIAGNOSTIC = "RESEARCH_OR_OFFLINE_DIAGNOSTIC"
GENERATED_CACHE_DIAGNOSTIC = "GENERATED_CACHE_DIAGNOSTIC"
DOC_DIAGNOSTIC = "DOC_DIAGNOSTIC"
OTHER_DIAGNOSTIC = "OTHER_DIAGNOSTIC"

LANE_IDS = (
    "historical_data_maintenance",
    "artifact_hygiene",
    "generated_artifact_retention_check",
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
DEFAULT_RETENTION_POLICY_PATH = Path("config") / "generated_artifact_retention.json"


@dataclass(frozen=True)
class TrackBWeeklyMaintenanceOrchestratorConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    markdown_report_root: Path = DEFAULT_MARKDOWN_REPORT_ROOT
    data_maintenance_report_path: Path = DEFAULT_DATA_MAINTENANCE_REPORT_PATH
    historical_data_proof_critical: bool = False
    retention_policy_path: Path = DEFAULT_RETENTION_POLICY_PATH
    run_retention_maintenance_dry_run: bool = False
    max_research_files: int = 1000
    max_old_root_scan_files: int = 5000
    max_disk_usage_scan_files: int = 5000
    write_lane_artifacts: bool = True
    force: bool = False
    maintained_history_runner_enabled: bool = False

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_weekly_maintenance_orchestrator(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
    now: datetime | None = None,
    lane_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    window = _maintenance_window(actual_now)
    previous_state = _read_json(config.resolve(config.output_path))
    if not config.force and _same_completed_window(previous_state, window):
        return _already_complete_payload(now=actual_now, window=window, previous_state=previous_state)

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
    canonical_proof_blockers = _canonical_proof_blocking_findings(lanes)
    optional_strategy_blockers = _optional_strategy_blocking_findings(lanes)
    diagnostics = _diagnostic_findings(lanes)
    maintenance_incomplete = _maintenance_incomplete_findings(lanes)
    failures = [lane for lane in lanes if lane.get("classification") == LANE_FAILED]
    incomplete = [lane for lane in lanes if lane.get("classification") == LANE_INCOMPLETE]
    base_overall = _overall_classification(
        missing_lanes=missing_lanes,
        failures=failures,
        incomplete=incomplete,
        canonical_proof_blockers=canonical_proof_blockers,
        maintenance_incomplete=maintenance_incomplete,
        diagnostics=diagnostics,
    )
    completion_status = _completion_status(
        base_overall=base_overall,
        canonical_proof_blockers=canonical_proof_blockers,
        maintenance_incomplete=maintenance_incomplete,
        diagnostics=diagnostics,
        actual_now=actual_now,
        window=window,
    )
    overall = _scheduled_classification(
        completion_status=completion_status,
        base_overall=base_overall,
        actual_now=actual_now,
        window=window,
    )
    alert_required = _alert_required(actual_now=actual_now, window=window, completion_status=completion_status)
    next_retry_at = _next_retry_at(actual_now=actual_now, window=window, completion_status=completion_status)
    attempts = _attempt_count(previous_state) + 1
    artifact_archive_lane = _lane_by_id(lanes, "artifact_archive_planner")
    historical_lane = _lane_by_id(lanes, "historical_data_maintenance")
    retention_lane = _lane_by_id(lanes, "generated_artifact_retention_check")
    old_root_lane = _lane_by_id(lanes, "old_root_contamination_check")
    disk_usage = _disk_usage_snapshot(repo_root=config.repo_root, max_files=config.max_disk_usage_scan_files)
    backfill_tracking = _historical_backfill_tracking(historical_lane=historical_lane, now=actual_now)
    utility_statuses = _utility_statuses(lanes=lanes, backfill_tracking=backfill_tracking, now=actual_now)
    pass_fail_summary = _pass_fail_summary(
        lanes=lanes,
        missing_lanes=missing_lanes,
        failures=failures,
        incomplete=incomplete,
        maintenance_incomplete=maintenance_incomplete,
        diagnostics=diagnostics,
    )
    return {
        "schema_version": "track_b_weekly_maintenance_orchestrator_v1",
        "generated_at": actual_now.isoformat(),
        "weekly_maintenance_orchestrator_id": f"track_b_weekly_maintenance_{uuid.uuid4().hex}",
        "week_id": window["week_id"],
        "maintenance_window_id": window["maintenance_window_id"],
        "window_start": window["window_start"].isoformat(),
        "alert_start": window["alert_start"].isoformat(),
        "window_end": window["window_end"].isoformat(),
        "completion_status": completion_status,
        "last_attempt_at": actual_now.isoformat(),
        "next_retry_at": next_retry_at.isoformat() if next_retry_at is not None else None,
        "attempts": attempts,
        "alert_required": alert_required,
        "mode": "PAPER",
        "overall_classification": overall,
        "base_lane_classification": base_overall,
        "lanes_run": [lane.get("lane_id") for lane in lanes],
        "lanes_complete": [
            lane.get("lane_id")
            for lane in lanes
            if lane.get("classification") not in {LANE_FAILED, LANE_INCOMPLETE, LANE_PROOF_BLOCKED}
        ],
        "lanes_failed": [lane.get("lane_id") for lane in failures],
        "lanes_failed_or_incomplete": [lane.get("lane_id") for lane in [*failures, *incomplete]],
        "lanes_incomplete": [lane.get("lane_id") for lane in incomplete],
        "missing_lanes": missing_lanes,
        "canonical_proof_blocking_findings": canonical_proof_blockers,
        "optional_strategy_blocking_findings": optional_strategy_blockers,
        "proof_blocking_findings": canonical_proof_blockers,
        "diagnostic_findings": diagnostics,
        "maintenance_incomplete_findings": maintenance_incomplete,
        "archive_posture": artifact_archive_lane.get("summary", {}),
        "historical_data_posture": historical_lane.get("summary", {}),
        "generated_artifact_retention_status": retention_lane.get("summary", {}),
        "utility_statuses": utility_statuses,
        "backfill_tracking": backfill_tracking,
        "disk_usage_before": disk_usage,
        "disk_usage_after": disk_usage,
        "pass_fail_summary": pass_fail_summary,
        "weekly_maintenance_manifest": {
            "schema_version": "weekly_maintenance_manifest_v1",
            "generated_at": actual_now.isoformat(),
            "expected_cadence": "weekly",
            "utility_statuses": utility_statuses,
            "backfill_tracking": backfill_tracking,
            "generated_artifact_retention_status": retention_lane.get("summary", {}),
            "disk_usage_before": disk_usage,
            "disk_usage_after": disk_usage,
            "pass_fail_summary": pass_fail_summary,
        },
        "old_root_hits": old_root_lane.get("summary", {}).get("old_root_hits", []),
        "old_root_active_path_blockers": old_root_lane.get("summary", {}).get("active_path_blockers", []),
        "sunday_proof_blocked": bool(canonical_proof_blockers),
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
            "generated_artifact_retention_policy": str(config.resolve(config.retention_policy_path)),
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
    parser.add_argument("--retention-policy", type=Path, default=DEFAULT_RETENTION_POLICY_PATH)
    parser.add_argument("--historical-data-proof-critical", action="store_true")
    parser.add_argument("--maintained-history-runner-enabled", action="store_true")
    parser.add_argument("--retention-maintenance-dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
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
        retention_policy_path=Path(args.retention_policy),
        run_retention_maintenance_dry_run=bool(args.retention_maintenance_dry_run),
        maintained_history_runner_enabled=bool(args.maintained_history_runner_enabled),
        force=bool(args.force),
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
        "canonical_proof_blocking_findings": payload.get("canonical_proof_blocking_findings"),
        "optional_strategy_blocking_findings": payload.get("optional_strategy_blocking_findings"),
        "maintenance_incomplete_findings": payload.get("maintenance_incomplete_findings"),
        "diagnostic_findings_count": len(payload.get("diagnostic_findings") or []),
        "sunday_proof_blocked": payload.get("sunday_proof_blocked"),
        "dry_run_only": True,
        "broker_mutation_allowed": False,
        "output_path": str(written.get("json") or config.resolve(config.output_path)),
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    if payload.get("overall_classification") not in {
        WEEKLY_MAINTENANCE_PROOF_BLOCKED,
        WEEKLY_MAINTENANCE_WINDOW_EXPIRED,
        WEEKLY_MAINTENANCE_FAILED,
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
    if lane_id == "generated_artifact_retention_check":
        return _generated_artifact_retention_lane(config=config)
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
    latest_success = _latest_successful_historical_data_report(config=config)
    cutoff = _prior_friday_close_utc(now)
    if not report:
        classification = LANE_DIAGNOSTIC_WARNING
        return _lane_result(
            lane_id="historical_data_maintenance",
            classification=classification,
            status="missing",
            reason="Latest Track B data maintenance report is missing.",
            proof_blocking=False,
            diagnostic_only=True,
            optional_strategy_blocking=config.maintained_history_runner_enabled,
            summary={
                "expected_command": "python -m mgc_v05l.execution_core.track_b_data_maintenance_cli",
                "expected_complete_through_prior_friday_close": cutoff.isoformat(),
                **latest_success,
                "optional_mgc_runner_blocking": config.maintained_history_runner_enabled,
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
        classification = LANE_DIAGNOSTIC_WARNING
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
        proof_blocking=False,
        diagnostic_only=True,
        optional_strategy_blocking=not ok and config.maintained_history_runner_enabled,
        summary={
            "expected_command": "python -m mgc_v05l.execution_core.track_b_data_maintenance_cli",
            "data_maintenance_verdict": report.get("data_maintenance_verdict"),
            "latest_bar_timestamp": report.get("latest_bar_timestamp"),
            "complete_through_cutoff": complete,
            "expected_complete_through_prior_friday_close": cutoff.isoformat(),
            **latest_success,
            "proof_critical": config.historical_data_proof_critical,
            "optional_mgc_runner_blocking": not ok and config.maintained_history_runner_enabled,
            "report_path": str(path),
        },
    )


def _generated_artifact_retention_lane(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
) -> dict[str, Any]:
    retention_config = RetentionConfig(
        repo_root=config.repo_root,
        policy_path=config.retention_policy_path,
    )
    check = check_generated_artifacts(config=retention_config)
    maintenance_dry_run: dict[str, Any] = {}
    if config.run_retention_maintenance_dry_run:
        maintenance_dry_run = maintain_generated_artifacts(config=retention_config, apply=False)
    ok = check.get("classification") == CHECK_OK
    return _lane_result(
        lane_id="generated_artifact_retention_check",
        classification=LANE_READY if ok else LANE_DIAGNOSTIC_WARNING,
        status="ready" if ok else "attention",
        reason=(
            "Generated artifact retention check is within configured thresholds."
            if ok
            else "Generated artifact retention check found oversized generated files."
        ),
        proof_blocking=False,
        diagnostic_only=True,
        summary={
            "classification": check.get("classification"),
            "check_command": "generated-artifact-retention check --warn-only",
            "maintenance_dry_run_command": "generated-artifact-retention maintain --dry-run",
            "policy_path": str(retention_config.resolve(retention_config.policy_path)),
            "candidate_count": check.get("candidate_count"),
            "violation_count": check.get("violation_count"),
            "protected_over_threshold_count": check.get("protected_over_threshold_count"),
            "top_large_files": check.get("top_large_files", []),
            "retention_maintenance_dry_run_included": bool(maintenance_dry_run),
            "retention_maintenance_action_count": maintenance_dry_run.get("action_count"),
            "retention_maintenance_rotated_count": maintenance_dry_run.get("rotated_count"),
            "dry_run_only": True,
            "apply_enabled": False,
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
    maintenance_incomplete = classification == ARCHIVE_PLAN_BLOCKED_SCAN_LIMIT
    lifecycle_diagnostic = classification == ARCHIVE_PLAN_BLOCKED_UNRESOLVED_LIFECYCLE
    ok = classification in {ARCHIVE_PLAN_READY, ARCHIVE_PLAN_EMPTY}
    return _lane_result(
        lane_id="artifact_archive_planner",
        classification=LANE_READY if ok else (LANE_PROOF_BLOCKED if active_authority_risk else LANE_DIAGNOSTIC_WARNING),
        status="ready" if ok else "attention",
        reason="Archive planner completed; execution remains disabled.",
        proof_blocking=active_authority_risk,
        diagnostic_only=not active_authority_risk,
        maintenance_incomplete=maintenance_incomplete,
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
            "maintenance_incomplete": maintenance_incomplete,
            "active_lifecycle_diagnostic": lifecycle_diagnostic,
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
    hit_details = _old_root_hit_details(repo_root=config.repo_root, max_files=config.max_old_root_scan_files)
    hits = [str(item.get("hit") or "") for item in hit_details]
    active_path_blockers = [
        item
        for item in hit_details
        if item.get("severity") in {ACTIVE_PROOF_PATH_BLOCKER, ACTIVE_RUNTIME_PATH_BLOCKER}
    ]
    return _lane_result(
        lane_id="old_root_contamination_check",
        classification=LANE_READY if not active_path_blockers else LANE_PROOF_BLOCKED,
        status="ready" if not active_path_blockers else "blocked",
        reason=(
            "No archived/Documents/iCloud root fragments found."
            if not hits
            else "Archived-root fragments found; active proof/runtime path hits are separated from diagnostics."
        ),
        proof_blocking=bool(active_path_blockers),
        diagnostic_only=bool(hits) and not active_path_blockers,
        summary={
            "old_root_hits": hits[:20],
            "old_root_hit_count": len(hits),
            "old_root_hit_details": hit_details[:20],
            "active_path_blockers": active_path_blockers[:20],
            "active_path_blocker_count": len(active_path_blockers),
            "diagnostic_hit_count": max(0, len(hit_details) - len(active_path_blockers)),
            "fragments": list(ARCHIVED_ROOT_FRAGMENTS),
        },
    )


def _final_context_lane(*, lanes: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    proof_blockers = _canonical_proof_blocking_findings(lanes)
    optional_blockers = _optional_strategy_blocking_findings(lanes)
    diagnostics = _diagnostic_findings(lanes)
    maintenance_incomplete = _maintenance_incomplete_findings(lanes)
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
        diagnostic_only=not proof_blockers and bool(diagnostics),
        maintenance_incomplete=bool(maintenance_incomplete),
        summary={
            "sunday_proof_posture": (
                "PROCEED_AFTER_FRESH_BARS_RETURN" if not proof_blockers else "BLOCKED_BY_MAINTENANCE"
            ),
            "canonical_proof_blocking_count": len(proof_blockers),
            "optional_strategy_blocking_count": len(optional_blockers),
            "diagnostic_count": len(diagnostics),
            "maintenance_incomplete_count": len(maintenance_incomplete),
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
    optional_strategy_blocking: bool = False,
    maintenance_incomplete: bool = False,
    summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "lane_id": lane_id,
        "classification": classification,
        "status": status,
        "reason": reason,
        "proof_blocking": proof_blocking,
        "canonical_proof_blocking": proof_blocking,
        "optional_strategy_blocking": optional_strategy_blocking,
        "diagnostic_only": diagnostic_only,
        "maintenance_incomplete": maintenance_incomplete,
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
        proof_blocking=payload.get("canonical_proof_blocking") is True or payload.get("proof_blocking") is True,
        diagnostic_only=payload.get("diagnostic_only") is not False,
        optional_strategy_blocking=payload.get("optional_strategy_blocking") is True,
        maintenance_incomplete=payload.get("maintenance_incomplete") is True,
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
    canonical_proof_blockers: Sequence[Mapping[str, Any]],
    maintenance_incomplete: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
) -> str:
    if failures:
        return WEEKLY_MAINTENANCE_FAILED
    if missing_lanes or incomplete:
        return WEEKLY_MAINTENANCE_INCOMPLETE
    if canonical_proof_blockers:
        return WEEKLY_MAINTENANCE_PROOF_BLOCKED
    if maintenance_incomplete:
        return WEEKLY_MAINTENANCE_INCOMPLETE
    if diagnostics:
        return WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS
    return WEEKLY_MAINTENANCE_READY


def _completion_status(
    *,
    base_overall: str,
    canonical_proof_blockers: Sequence[Mapping[str, Any]],
    maintenance_incomplete: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
    actual_now: datetime,
    window: Mapping[str, datetime | str],
) -> str:
    if canonical_proof_blockers:
        return COMPLETION_PROOF_BLOCKED
    if base_overall in {WEEKLY_MAINTENANCE_READY, WEEKLY_MAINTENANCE_READY_WITH_DIAGNOSTICS}:
        return COMPLETION_COMPLETE_WITH_DIAGNOSTICS if diagnostics else COMPLETION_COMPLETE
    if actual_now >= _window_datetime(window, "window_end"):
        return COMPLETION_WINDOW_EXPIRED
    if maintenance_incomplete:
        return COMPLETION_INCOMPLETE
    return COMPLETION_INCOMPLETE


def _scheduled_classification(
    *,
    completion_status: str,
    base_overall: str,
    actual_now: datetime,
    window: Mapping[str, datetime | str],
) -> str:
    if completion_status in {COMPLETION_COMPLETE, COMPLETION_COMPLETE_WITH_DIAGNOSTICS}:
        return WEEKLY_MAINTENANCE_READY
    if completion_status == COMPLETION_WINDOW_EXPIRED:
        return WEEKLY_MAINTENANCE_WINDOW_EXPIRED
    if base_overall == WEEKLY_MAINTENANCE_PROOF_BLOCKED:
        return WEEKLY_MAINTENANCE_PROOF_BLOCKED
    if _alert_required(actual_now=actual_now, window=window, completion_status=completion_status):
        return WEEKLY_MAINTENANCE_ALERT_REQUIRED
    return WEEKLY_MAINTENANCE_RETRY_SCHEDULED


def _alert_required(
    *,
    actual_now: datetime,
    window: Mapping[str, datetime | str],
    completion_status: str,
) -> bool:
    if completion_status in {COMPLETION_COMPLETE, COMPLETION_COMPLETE_WITH_DIAGNOSTICS}:
        return False
    return _window_datetime(window, "alert_start") <= actual_now <= _window_datetime(window, "window_end")


def _next_retry_at(
    *,
    actual_now: datetime,
    window: Mapping[str, datetime | str],
    completion_status: str,
) -> datetime | None:
    if completion_status in {
        COMPLETION_COMPLETE,
        COMPLETION_COMPLETE_WITH_DIAGNOSTICS,
        COMPLETION_WINDOW_EXPIRED,
    }:
        return None
    window_start = _window_datetime(window, "window_start")
    window_end = _window_datetime(window, "window_end")
    if actual_now < window_start:
        return window_start
    if actual_now >= window_end:
        return None
    next_hour = actual_now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return next_hour if next_hour <= window_end else window_end


def _maintenance_window(now: datetime) -> dict[str, datetime | str]:
    eastern = ZoneInfo("America/New_York")
    local_now = _ensure_utc(now).astimezone(eastern)
    days_until_saturday = (5 - local_now.weekday()) % 7
    if local_now.weekday() == 6:
        days_until_saturday = -1
    saturday = local_now.date() + timedelta(days=days_until_saturday)
    window_start = datetime.combine(saturday, time(0, 0), tzinfo=eastern)
    alert_start = window_start + timedelta(days=1, hours=1)
    window_end = window_start + timedelta(days=1, hours=16)
    week_id = f"{saturday.isoformat()}_saturday_et"
    return {
        "week_id": week_id,
        "maintenance_window_id": f"track_b_weekly_maintenance_{week_id}",
        "window_start": window_start.astimezone(UTC),
        "alert_start": alert_start.astimezone(UTC),
        "window_end": window_end.astimezone(UTC),
    }


def _same_completed_window(
    previous_state: Mapping[str, Any],
    window: Mapping[str, datetime | str],
) -> bool:
    if not previous_state:
        return False
    return (
        previous_state.get("maintenance_window_id") == window.get("maintenance_window_id")
        and previous_state.get("completion_status") in {COMPLETION_COMPLETE, COMPLETION_COMPLETE_WITH_DIAGNOSTICS}
        and previous_state.get("overall_classification")
        in {WEEKLY_MAINTENANCE_READY, WEEKLY_MAINTENANCE_ALREADY_COMPLETE}
    )


def _already_complete_payload(
    *,
    now: datetime,
    window: Mapping[str, datetime | str],
    previous_state: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_weekly_maintenance_orchestrator_v1",
        "generated_at": now.isoformat(),
        "weekly_maintenance_orchestrator_id": f"track_b_weekly_maintenance_{uuid.uuid4().hex}",
        "week_id": window["week_id"],
        "maintenance_window_id": window["maintenance_window_id"],
        "window_start": _window_datetime(window, "window_start").isoformat(),
        "alert_start": _window_datetime(window, "alert_start").isoformat(),
        "window_end": _window_datetime(window, "window_end").isoformat(),
        "completion_status": previous_state.get("completion_status") or COMPLETION_COMPLETE,
        "last_attempt_at": previous_state.get("last_attempt_at"),
        "next_retry_at": None,
        "attempts": _attempt_count(previous_state),
        "alert_required": False,
        "mode": "PAPER",
        "overall_classification": WEEKLY_MAINTENANCE_ALREADY_COMPLETE,
        "base_lane_classification": previous_state.get("base_lane_classification") or WEEKLY_MAINTENANCE_READY,
        "lanes_run": [],
        "lanes_complete": list(_list(previous_state.get("lanes_complete"))),
        "lanes_failed": [],
        "lanes_failed_or_incomplete": [],
        "lanes_incomplete": [],
        "missing_lanes": [],
        "canonical_proof_blocking_findings": [],
        "optional_strategy_blocking_findings": list(_list(previous_state.get("optional_strategy_blocking_findings"))),
        "proof_blocking_findings": [],
        "diagnostic_findings": list(_list(previous_state.get("diagnostic_findings"))),
        "maintenance_incomplete_findings": list(_list(previous_state.get("maintenance_incomplete_findings"))),
        "archive_posture": _mapping(previous_state.get("archive_posture")),
        "historical_data_posture": _mapping(previous_state.get("historical_data_posture")),
        "old_root_hits": list(_list(previous_state.get("old_root_hits"))),
        "old_root_active_path_blockers": list(_list(previous_state.get("old_root_active_path_blockers"))),
        "sunday_proof_blocked": False,
        "recommended_actions": ["Weekly maintenance already completed for this maintenance window."],
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
        "lanes": [],
        "source_artifact_paths": dict(_mapping(previous_state.get("source_artifact_paths"))),
    }


def _window_datetime(window: Mapping[str, datetime | str], key: str) -> datetime:
    value = window[key]
    if isinstance(value, datetime):
        return _ensure_utc(value)
    return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _attempt_count(previous_state: Mapping[str, Any]) -> int:
    try:
        return max(0, int(previous_state.get("attempts") or 0))
    except (TypeError, ValueError):
        return 0


def _canonical_proof_blocking_findings(lanes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "lane_id": str(lane.get("lane_id") or ""),
            "classification": lane.get("classification"),
            "reason": lane.get("reason"),
            "summary": _mapping(lane.get("summary")),
        }
        for lane in lanes
        if lane.get("canonical_proof_blocking") is True or lane.get("proof_blocking") is True
    ]


def _optional_strategy_blocking_findings(lanes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "lane_id": str(lane.get("lane_id") or ""),
            "classification": lane.get("classification"),
            "reason": lane.get("reason"),
            "summary": _mapping(lane.get("summary")),
        }
        for lane in lanes
        if lane.get("optional_strategy_blocking") is True
    ]


def _maintenance_incomplete_findings(lanes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "lane_id": str(lane.get("lane_id") or ""),
            "classification": lane.get("classification"),
            "reason": lane.get("reason"),
            "summary": _mapping(lane.get("summary")),
        }
        for lane in lanes
        if lane.get("maintenance_incomplete") is True or lane.get("classification") == LANE_INCOMPLETE
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


def _latest_successful_historical_data_report(
    *,
    config: TrackBWeeklyMaintenanceOrchestratorConfig,
) -> dict[str, Any]:
    root = config.resolve(DEFAULT_DATA_MAINTENANCE_REPORT_PATH).parent
    candidates = [config.resolve(config.data_maintenance_report_path)]
    if root.exists():
        candidates.extend(sorted(root.glob("track_b_data_maintenance_*/track_b_data_maintenance_report.json")))
    latest: dict[str, Any] = {}
    latest_path: Path | None = None
    latest_generated_at: datetime | None = None
    for path in candidates:
        report = _read_json(path)
        if not _historical_report_successful(report):
            continue
        generated_at = _parse_datetime(report.get("generated_at")) or _parse_datetime(report.get("latest_bar_timestamp"))
        if generated_at is None:
            continue
        if latest_generated_at is None or generated_at > latest_generated_at:
            latest = report
            latest_path = path
            latest_generated_at = generated_at
    return {
        "last_successful_run_at": latest_generated_at.isoformat() if latest_generated_at else None,
        "last_successful_latest_bar_timestamp": latest.get("latest_bar_timestamp"),
        "last_successful_report_path": "" if latest_path is None else str(latest_path),
    }


def _historical_report_successful(report: Mapping[str, Any]) -> bool:
    return (
        report.get("data_maintenance_verdict") == "TRACK_B_DATA_MAINTENANCE_UPDATED_HISTORY_READY"
        and report.get("complete_through_cutoff") is True
        and report.get("history_ready") is not False
    )


def _historical_backfill_tracking(
    *,
    historical_lane: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    summary = _mapping(historical_lane.get("summary"))
    latest_bar = _parse_datetime(summary.get("latest_bar_timestamp"))
    expected_cutoff = _parse_datetime(summary.get("expected_complete_through_prior_friday_close")) or _prior_friday_close_utc(now)
    last_success = _parse_datetime(summary.get("last_successful_run_at"))
    last_success_bar = _parse_datetime(summary.get("last_successful_latest_bar_timestamp"))
    range_start = latest_bar or last_success_bar
    missing_ranges: list[dict[str, str]] = []
    recommended_ranges: list[dict[str, str]] = []
    if range_start is not None and expected_cutoff is not None and range_start < expected_cutoff:
        start = range_start + timedelta(minutes=1)
        missing = {
            "from": start.isoformat(),
            "to": expected_cutoff.isoformat(),
            "reason": "latest_historical_bar_before_expected_prior_friday_close",
        }
        missing_ranges.append(missing)
        recommended_ranges.append(missing)
    elif range_start is None and expected_cutoff is not None:
        recommended_ranges.append(
            {
                "from": "",
                "to": expected_cutoff.isoformat(),
                "reason": "no_successful_historical_maintenance_artifact_found",
            }
        )
    recent_windows = _recent_weekly_windows(now=now, count=2)
    missed_recent_windows = [
        window
        for window in recent_windows
        if last_success is None or last_success < _parse_datetime(window["window_start"])
    ]
    return {
        "utility_id": "historical_data_backfill",
        "expected_cadence": "weekly",
        "last_successful_run_at": last_success.isoformat() if last_success else None,
        "latest_bar_timestamp": summary.get("latest_bar_timestamp"),
        "expected_complete_through": expected_cutoff.isoformat() if expected_cutoff else None,
        "overdue": bool(historical_lane.get("classification") != LANE_READY),
        "last_two_weekly_runs_missed": len(missed_recent_windows) >= 2,
        "missed_week_count": len(missed_recent_windows),
        "missed_weekly_windows": missed_recent_windows,
        "missing_data_ranges": missing_ranges,
        "recommended_backfill_date_ranges": recommended_ranges,
        "backfill_execution_allowed": False,
    }


def _recent_weekly_windows(*, now: datetime, count: int) -> list[dict[str, str]]:
    eastern = ZoneInfo("America/New_York")
    local_now = _ensure_utc(now).astimezone(eastern)
    days_since_saturday = (local_now.weekday() - 5) % 7
    saturday = local_now.date() - timedelta(days=days_since_saturday)
    if local_now.weekday() == 5 and (local_now.hour, local_now.minute, local_now.second) < (0, 0, 0):
        saturday -= timedelta(days=7)
    windows: list[dict[str, str]] = []
    for offset in range(max(0, count)):
        day = saturday - timedelta(days=7 * offset)
        start = datetime.combine(day, time(0, 0), tzinfo=eastern)
        windows.append(
            {
                "week_id": f"{day.isoformat()}_saturday_et",
                "window_start": start.astimezone(UTC).isoformat(),
                "expected_complete_through_prior_friday_close": (start - timedelta(hours=7)).astimezone(UTC).isoformat(),
            }
        )
    return list(reversed(windows))


def _utility_statuses(
    *,
    lanes: Sequence[Mapping[str, Any]],
    backfill_tracking: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    return [
        _utility_status(
            utility_id="historical_data_backfill",
            lane=_lane_by_id(lanes, "historical_data_maintenance"),
            expected_cadence="weekly",
            last_successful_run_at=backfill_tracking.get("last_successful_run_at"),
            missing_data_ranges=_list(backfill_tracking.get("missing_data_ranges")),
            recommended_backfill_date_ranges=_list(backfill_tracking.get("recommended_backfill_date_ranges")),
            now=now,
        ),
        _utility_status(
            utility_id="research_platform_refresh",
            lane=_lane_by_id(lanes, "research_offline_labeling_check"),
            expected_cadence="weekly",
            last_successful_run_at=None,
            now=now,
        ),
        _utility_status(
            utility_id="generated_artifact_retention",
            lane=_lane_by_id(lanes, "generated_artifact_retention_check"),
            expected_cadence="weekly",
            last_successful_run_at=None,
            now=now,
        ),
        _utility_status(
            utility_id="archive_cleanup_planning",
            lane=_lane_by_id(lanes, "artifact_archive_planner"),
            expected_cadence="weekly",
            last_successful_run_at=None,
            now=now,
        ),
        _utility_status(
            utility_id="generated_artifact_hygiene_inventory",
            lane=_lane_by_id(lanes, "artifact_hygiene"),
            expected_cadence="weekly",
            last_successful_run_at=None,
            now=now,
        ),
    ]


def _utility_status(
    *,
    utility_id: str,
    lane: Mapping[str, Any],
    expected_cadence: str,
    last_successful_run_at: Any,
    now: datetime,
    missing_data_ranges: Sequence[Any] = (),
    recommended_backfill_date_ranges: Sequence[Any] = (),
) -> dict[str, Any]:
    lane_ready = lane.get("classification") == LANE_READY
    successful_at = str(last_successful_run_at) if last_successful_run_at else (now.isoformat() if lane_ready else None)
    overdue = not lane_ready
    if successful_at:
        parsed = _parse_datetime(successful_at)
        if parsed is not None and _ensure_utc(now) - parsed > timedelta(days=8):
            overdue = True
    return {
        "utility_id": utility_id,
        "lane_id": lane.get("lane_id"),
        "expected_cadence": expected_cadence,
        "last_successful_run_at": successful_at,
        "overdue": overdue,
        "classification": lane.get("classification"),
        "status": lane.get("status"),
        "missing_data_ranges": list(missing_data_ranges),
        "recommended_backfill_date_ranges": list(recommended_backfill_date_ranges),
    }


def _pass_fail_summary(
    *,
    lanes: Sequence[Mapping[str, Any]],
    missing_lanes: Sequence[str],
    failures: Sequence[Mapping[str, Any]],
    incomplete: Sequence[Mapping[str, Any]],
    maintenance_incomplete: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "pass": not missing_lanes and not failures and not incomplete and not maintenance_incomplete,
        "lane_count": len(lanes),
        "failed_count": len(failures),
        "incomplete_count": len(incomplete),
        "missing_lane_count": len(missing_lanes),
        "maintenance_incomplete_count": len(maintenance_incomplete),
        "diagnostic_count": len(diagnostics),
    }


def _disk_usage_snapshot(*, repo_root: Path, max_files: int) -> dict[str, Any]:
    stat = os.statvfs(repo_root)
    total = int(stat.f_frsize * stat.f_blocks)
    free = int(stat.f_frsize * stat.f_bavail)
    roots = []
    for relative in (Path("outputs"), Path("var"), Path("logs")):
        size, scanned, limited = _apparent_size_bytes(repo_root / relative, max_files=max_files)
        roots.append(
            {
                "path": str(relative),
                "exists": (repo_root / relative).exists(),
                "apparent_size_bytes": size,
                "files_scanned": scanned,
                "scan_limited": limited,
            }
        )
    return {
        "filesystem_total_bytes": total,
        "filesystem_used_bytes": total - free,
        "filesystem_free_bytes": free,
        "generated_roots": roots,
    }


def _apparent_size_bytes(path: Path, *, max_files: int) -> tuple[int, int, bool]:
    if not path.exists():
        return 0, 0, False
    total = 0
    scanned = 0
    stack = [path]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    if scanned >= max_files:
                        return total, scanned, True
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += int(entry.stat(follow_symlinks=False).st_size)
                            scanned += 1
                    except OSError:
                        continue
        except OSError:
            continue
    return total, scanned, False


def _recommended_actions(overall: str, lanes: Sequence[Mapping[str, Any]]) -> list[str]:
    if overall == WEEKLY_MAINTENANCE_READY:
        return ["No weekly maintenance action required before proof."]
    if overall == WEEKLY_MAINTENANCE_ALREADY_COMPLETE:
        return ["Weekly maintenance already completed for this maintenance window."]
    if overall == WEEKLY_MAINTENANCE_RETRY_SCHEDULED:
        return ["Weekly maintenance incomplete; next hourly retry is scheduled."]
    if overall == WEEKLY_MAINTENANCE_ALERT_REQUIRED:
        return ["Weekly maintenance incomplete in the Sunday alert window; alert artifact review is required."]
    if overall == WEEKLY_MAINTENANCE_WINDOW_EXPIRED:
        return ["Weekly maintenance window expired incomplete; review maintenance posture before proof."]
    actions: list[str] = []
    for finding in [
        *_canonical_proof_blocking_findings(lanes),
        *_optional_strategy_blocking_findings(lanes),
        *_maintenance_incomplete_findings(lanes),
        *_diagnostic_findings(lanes),
    ]:
        actions.append(f"{finding['lane_id']}: {finding['reason']}")
    return actions or ["Review incomplete weekly maintenance lanes."]


def _render_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Track B Weekly Maintenance",
        "",
        f"- generated_at: {payload.get('generated_at')}",
        f"- overall_classification: {payload.get('overall_classification')}",
        f"- completion_status: {payload.get('completion_status')}",
        f"- maintenance_window_id: {payload.get('maintenance_window_id')}",
        f"- window_start: {payload.get('window_start')}",
        f"- alert_start: {payload.get('alert_start')}",
        f"- window_end: {payload.get('window_end')}",
        f"- attempts: {payload.get('attempts')}",
        f"- next_retry_at: {payload.get('next_retry_at')}",
        f"- alert_required: {payload.get('alert_required')}",
        f"- sunday_proof_blocked: {payload.get('sunday_proof_blocked')}",
        f"- dry_run_only: {payload.get('dry_run_only')}",
        f"- broker_mutation_allowed: {payload.get('broker_mutation_allowed')}",
        f"- canonical_proof_blocking_findings: {len(payload.get('canonical_proof_blocking_findings') or [])}",
        f"- optional_strategy_blocking_findings: {len(payload.get('optional_strategy_blocking_findings') or [])}",
        f"- maintenance_incomplete_findings: {len(payload.get('maintenance_incomplete_findings') or [])}",
        f"- diagnostic_findings: {len(payload.get('diagnostic_findings') or [])}",
        f"- last_two_weekly_runs_missed: {_mapping(payload.get('backfill_tracking')).get('last_two_weekly_runs_missed')}",
        f"- retention_violation_count: {_mapping(payload.get('generated_artifact_retention_status')).get('violation_count')}",
        "",
        "## Utility Status",
    ]
    for utility in _list(payload.get("utility_statuses")):
        lines.append(
            f"- {utility.get('utility_id')}: {utility.get('classification')} "
            f"(overdue={utility.get('overdue')}, last_successful_run_at={utility.get('last_successful_run_at')})"
        )
    lines.extend([
        "",
        "## Lanes",
    ])
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


def _old_root_hit_details(*, repo_root: Path, max_files: int) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
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
            lines = text.splitlines()
            for line_number, line in enumerate(lines, start=1):
                if any(fragment in line for fragment in ARCHIVED_ROOT_FRAGMENTS):
                    if _old_root_line_is_guard_only(lines=lines, line_number=line_number, line=line):
                        continue
                    relative = _relative_text(path, repo_root)
                    hits.append(
                        {
                            "hit": f"{relative}:{line_number}",
                            "path": relative,
                            "line": str(line_number),
                            "severity": _old_root_hit_severity(relative),
                        }
                    )
                    break
    return hits


def _old_root_hit_severity(relative_path: str) -> str:
    path = Path(relative_path)
    parts = tuple(path.parts)
    lowered = relative_path.lower()
    if "__pycache__" in parts or lowered.endswith((".pyc", ".pyo")):
        return GENERATED_CACHE_DIAGNOSTIC
    if parts and parts[0] == "docs":
        return DOC_DIAGNOSTIC
    if "research" in parts or any(marker in lowered for marker in ("atp_", "us_open", "shadow", "replay")):
        return RESEARCH_OR_OFFLINE_DIAGNOSTIC
    if parts and parts[0] == "scripts":
        name = path.name
        if name in {
            "track_b_paper_preflight.sh",
            "run_probationary_paper_soak.sh",
            "run_headless_supervised_paper_service.sh",
            "show_headless_supervised_paper_status.sh",
            "install_track_b_sunday_preflight_launchd.sh",
        }:
            return ACTIVE_PROOF_PATH_BLOCKER
        if any(token in name for token in ("paper", "headless", "probationary", "operator", "broker_truth")):
            return ACTIVE_RUNTIME_PATH_BLOCKER
        return RESEARCH_OR_OFFLINE_DIAGNOSTIC
    if parts[:3] == ("src", "mgc_v05l", "execution_core"):
        return ACTIVE_PROOF_PATH_BLOCKER
    if parts[:3] == ("src", "mgc_v05l", "app") and any(
        token in lowered for token in ("operator_dashboard", "paper", "broker", "lifecycle", "runtime")
    ):
        return ACTIVE_RUNTIME_PATH_BLOCKER
    return OTHER_DIAGNOSTIC


def _old_root_line_is_guard_only(*, lines: Sequence[str], line_number: int, line: str) -> bool:
    stripped = line.strip()
    context_start = max(0, line_number - 4)
    context = "\n".join(lines[context_start:line_number])
    if "OLD_ROOT_PATTERNS" in context or "ARCHIVED_ROOT_FRAGMENTS" in context:
        return True
    guard_markers = (
        "OLD_ROOT_UNSAFE",
        "deprecated Documents/iCloud root",
        "archived/Documents/iCloud root fragments",
        "Refusing archived Documents/iCloud project root",
        "is_archived_project_root",
        "ALLOW_ARCHIVED_ROOT_FOR_TESTS",
    )
    if any(marker in line for marker in guard_markers):
        return True
    if (
        " in text" in context
        or " in line" in context
        or " in normalized" in context
    ) and ("Documents" in stripped or "Mobile Documents" in stripped or "iCloud" in stripped):
        return True
    return stripped.startswith("*\"/Users/patrick/Documents\"*") or stripped.startswith("*\"Mobile Documents\"*")


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
