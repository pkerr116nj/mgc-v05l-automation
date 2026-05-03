"""Track B no-submit operator status summary boundary.

This module aggregates existing observer reports into one read-only status
artifact. It is intended as a future dashboard read model and never becomes a
source of trading truth or submit authority.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT = Path("outputs/track_b_execution_core/operator_status")
NOT_PROVIDED = "NOT_PROVIDED"


class OperatorStatusVerdict(str, Enum):
    OK_FOR_SHADOW_REVIEW = "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    DEGRADED_SHADOW_FAILURES = "OPERATOR_STATUS_DEGRADED_SHADOW_FAILURES"
    BLOCKED_READINESS = "OPERATOR_STATUS_BLOCKED_READINESS"
    BLOCKED_BROKER_STATE = "OPERATOR_STATUS_BLOCKED_BROKER_STATE"
    MISSING_REPORTS = "OPERATOR_STATUS_MISSING_REPORTS"
    UNKNOWN = "OPERATOR_STATUS_UNKNOWN"


@dataclass(frozen=True)
class OperatorStatusInputs:
    listener_heartbeat_json: Path | None = None
    listener_health_json: Path | None = None
    listener_cycle_json: Path | None = None
    shadow_runner_summary_json: Path | None = None
    attrition_report_json: Path | None = None
    signal_batch_writer_report_json: Path | None = None
    readiness_summary_json: Path | None = None
    recovery_report_json: Path | None = None
    preflight_report_json: Path | None = None
    quote_report_json: Path | None = None
    output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT


@dataclass(frozen=True)
class OperatorStatusResult:
    verdict: OperatorStatusVerdict
    report_json: Path
    report: dict[str, Any]


def create_operator_status_summary(
    *,
    inputs: OperatorStatusInputs,
    status_id: str | None = None,
    now: datetime | None = None,
) -> OperatorStatusResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_status_id = status_id or f"operator_status_{uuid.uuid4().hex}"
    report_json = Path(inputs.output_root) / actual_status_id / "operator_status_summary.json"
    reports = _load_reports(inputs)
    verdict, primary_blocker, required_next_action = _classify(reports)
    report = _report(
        report_json=report_json,
        now=actual_now,
        status_id=actual_status_id,
        verdict=verdict,
        reports=reports,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
    )
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return OperatorStatusResult(verdict=verdict, report_json=report_json, report=report)


def _load_reports(inputs: OperatorStatusInputs) -> dict[str, dict[str, Any] | None]:
    return {
        "listener_heartbeat": _read_json(inputs.listener_heartbeat_json),
        "listener_health": _read_json(inputs.listener_health_json),
        "listener_cycle": _read_json(inputs.listener_cycle_json),
        "shadow_runner": _read_json(inputs.shadow_runner_summary_json),
        "attrition": _read_json(inputs.attrition_report_json),
        "signal_batch_writer": _read_json(inputs.signal_batch_writer_report_json),
        "readiness": _read_json(inputs.readiness_summary_json),
        "recovery": _read_json(inputs.recovery_report_json),
        "preflight": _read_json(inputs.preflight_report_json),
        "quote": _read_json(inputs.quote_report_json),
    }


def _classify(reports: Mapping[str, Mapping[str, Any] | None]) -> tuple[OperatorStatusVerdict, str | None, str]:
    recovery = reports.get("recovery") or {}
    readiness = reports.get("readiness") or {}
    preflight = reports.get("preflight") or {}
    listener_health = reports.get("listener_health") or {}
    listener_heartbeat = reports.get("listener_heartbeat") or {}

    recovery_verdict = _recovery_verdict(recovery)
    if recovery_verdict in {"BLOCKED_UNRESOLVED_BROKER_ORDER", "RECOVERY_BLOCKED_UNRESOLVED_ORDER"}:
        return (
            OperatorStatusVerdict.BLOCKED_BROKER_STATE,
            str(recovery.get("primary_blocker") or "Recovery report shows unresolved broker state."),
            str(recovery.get("required_next_action") or "Resolve broker state before any paper proof submit."),
        )

    readiness_verdict = str(readiness.get("final_readiness_verdict") or "")
    if readiness_verdict and readiness_verdict != "READY_FOR_PAPER_PROOF":
        if readiness_verdict == "BLOCKED_UNRESOLVED_BROKER_ORDER":
            return (
                OperatorStatusVerdict.BLOCKED_BROKER_STATE,
                str(readiness.get("primary_blocker") or "Readiness summary shows unresolved broker state."),
                str(readiness.get("required_next_action") or "Resolve broker state before any paper proof submit."),
            )
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(readiness.get("primary_blocker") or f"Readiness summary is blocked: {readiness_verdict}"),
            str(readiness.get("required_next_action") or "Resolve readiness blocker before paper proof review."),
        )

    preflight_verdict = str(preflight.get("final_readiness_verdict") or "")
    preflight_classification = str(preflight.get("classification") or "")
    if preflight_verdict == "BLOCKED_UNRESOLVED_BROKER_ORDER" or preflight.get("unresolved_broker_order_detected"):
        return (
            OperatorStatusVerdict.BLOCKED_BROKER_STATE,
            str(preflight.get("primary_blocker") or preflight.get("failure_or_ambiguity") or "Preflight shows unresolved broker order."),
            str(preflight.get("required_next_action") or "Resolve broker state before any paper proof submit."),
        )
    if preflight_verdict and preflight_verdict != "READY_FOR_PAPER_PROOF":
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(preflight.get("primary_blocker") or "Preflight readiness is blocked."),
            str(preflight.get("required_next_action") or "Resolve preflight blocker before paper proof review."),
        )
    if preflight_classification and preflight_classification not in {"READY_READ_ONLY"}:
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(preflight.get("primary_blocker") or preflight.get("failure_or_ambiguity") or f"Preflight classification is {preflight_classification}."),
            str(preflight.get("required_next_action") or "Resolve preflight blocker before paper proof review."),
        )

    health_verdict = str(listener_health.get("health_verdict") or listener_heartbeat.get("last_health_verdict") or "")
    if health_verdict == "SHADOW_LISTENER_HEALTH_DEGRADED_FAILURES":
        return (
            OperatorStatusVerdict.DEGRADED_SHADOW_FAILURES,
            str(listener_health.get("last_primary_blocker") or listener_heartbeat.get("primary_blocker") or "Listener health is degraded due to failed files."),
            str(listener_health.get("last_required_next_action") or listener_heartbeat.get("required_next_action") or "Review listener event and replay reports."),
        )
    if health_verdict == "SHADOW_LISTENER_HEALTH_OK":
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            None,
            "Shadow observer reports are OK for no-submit review. This does not authorize paper or live submit.",
        )
    if health_verdict == "SHADOW_LISTENER_HEALTH_NO_FILES":
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            None,
            "Listener is healthy but no files were available. This does not authorize submit.",
        )
    if _all_missing(reports):
        return (
            OperatorStatusVerdict.MISSING_REPORTS,
            "No Track B observer reports were provided.",
            "Provide at least listener health or another Track B observer report.",
        )
    return (
        OperatorStatusVerdict.UNKNOWN,
        "Supplied reports do not contain enough status evidence for a clear verdict.",
        "Provide listener health, readiness, recovery, or preflight reports.",
    )


def _report(
    *,
    report_json: Path,
    now: datetime,
    status_id: str,
    verdict: OperatorStatusVerdict,
    reports: Mapping[str, Mapping[str, Any] | None],
    primary_blocker: str | None,
    required_next_action: str,
) -> dict[str, Any]:
    missing = [name for name, report in reports.items() if report is None]
    considered = {name: report is not None for name, report in reports.items()}
    listener_heartbeat = reports.get("listener_heartbeat") or {}
    listener_health = reports.get("listener_health") or {}
    listener_cycle = reports.get("listener_cycle") or {}
    shadow_runner = reports.get("shadow_runner") or {}
    attrition = reports.get("attrition") or {}
    signal_batch_writer = reports.get("signal_batch_writer") or {}
    readiness = reports.get("readiness") or {}
    recovery = reports.get("recovery") or {}
    preflight = reports.get("preflight") or {}
    quote = reports.get("quote") or {}
    latest_output_paths = {
        "listener_heartbeat": listener_heartbeat.get("heartbeat_json_path"),
        "listener_health": listener_health.get("health_report_path") or listener_health.get("latest_health_report_path"),
        "listener_cycle": listener_cycle.get("report_json_path") or listener_health.get("latest_cycle_summary_path"),
        "shadow_runner": shadow_runner.get("report_json_path"),
        "attrition": attrition.get("report_json_path"),
        "signal_batch_writer": signal_batch_writer.get("report_json_path"),
        "readiness": readiness.get("report_json_path"),
        "recovery": recovery.get("report_json_path"),
        "preflight": preflight.get("report_json_path"),
        "quote": quote.get("report_json_path"),
    }
    return {
        "schema_version": "track_b_operator_status_v1",
        "generated_at": now.isoformat(),
        "operator_status_id": status_id,
        "status_verdict": verdict.value,
        "listener_mode": listener_heartbeat.get("listener_mode") or NOT_PROVIDED,
        "listener_current_cycle_number": listener_heartbeat.get("current_cycle_number") if listener_heartbeat else NOT_PROVIDED,
        "listener_last_cycle_number": listener_heartbeat.get("last_cycle_number") if listener_heartbeat else NOT_PROVIDED,
        "listener_last_cycle_start_at": listener_heartbeat.get("last_cycle_start_at") or NOT_PROVIDED,
        "listener_last_cycle_end_at": listener_heartbeat.get("last_cycle_end_at") or NOT_PROVIDED,
        "listener_processed_cycles": listener_heartbeat.get("processed_cycles") if listener_heartbeat else NOT_PROVIDED,
        "listener_failed_cycles": listener_heartbeat.get("failed_cycles") if listener_heartbeat else NOT_PROVIDED,
        "listener_no_file_cycles": listener_heartbeat.get("no_file_cycles") if listener_heartbeat else NOT_PROVIDED,
        "listener_watch_exited_normally": listener_heartbeat.get("watch_exited_normally") if listener_heartbeat else NOT_PROVIDED,
        "listener_watch_verdict": listener_heartbeat.get("watch_verdict") or NOT_PROVIDED,
        "shadow_listener_health_verdict": listener_health.get("health_verdict") or NOT_PROVIDED,
        "listener_last_health_verdict": listener_heartbeat.get("last_health_verdict") or listener_health.get("health_verdict") or NOT_PROVIDED,
        "latest_listener_cycle_verdict": listener_cycle.get("listener_verdict") or listener_health.get("last_cycle_verdict") or listener_heartbeat.get("last_listener_verdict") or NOT_PROVIDED,
        "shadow_replay_runner_verdict": shadow_runner.get("runner_verdict") or NOT_PROVIDED,
        "attrition_report_verdict": attrition.get("attrition_report_verdict") or NOT_PROVIDED,
        "signal_batch_writer_verdict": signal_batch_writer.get("signal_batch_writer_verdict") or NOT_PROVIDED,
        "signal_batch_writer_batch_file_written": signal_batch_writer.get("batch_file_written") if signal_batch_writer else NOT_PROVIDED,
        "signal_batch_writer_batch_json_path": signal_batch_writer.get("batch_json_path") or NOT_PROVIDED,
        "signal_batch_writer_total_signals": signal_batch_writer.get("total_signals") if signal_batch_writer else NOT_PROVIDED,
        "recent_writer_output_present": bool(signal_batch_writer.get("batch_json_path")) if signal_batch_writer else False,
        "readiness_verdict": readiness.get("final_readiness_verdict") or NOT_PROVIDED,
        "recovery_verdict": _recovery_verdict(recovery) or NOT_PROVIDED,
        "preflight_verdict": preflight.get("final_readiness_verdict") or preflight.get("classification") or NOT_PROVIDED,
        "quote_status": quote.get("quote_status") or quote.get("classification") or NOT_PROVIDED,
        "primary_blocker": primary_blocker,
        "secondary_blockers": _secondary_blockers(reports, missing),
        "required_next_action": required_next_action,
        "reports_considered": considered,
        "reports_missing": missing,
        "latest_output_paths": latest_output_paths,
        "engine_running_inferred": False,
        "engine_running_inference": "UNKNOWN_UNLESS_EXPLICITLY_PROVIDED",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "observer_status_only": True,
        "dashboard_is_not_authority": True,
        "listener_invoked": False,
        "runner_invoked": False,
        "paper_proof_cli_called": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "report_json_path": str(report_json),
    }


def _secondary_blockers(reports: Mapping[str, Mapping[str, Any] | None], missing: list[str]) -> list[str]:
    blockers = [f"missing_report:{name}" for name in missing]
    for report in reports.values():
        if not report:
            continue
        if report.get("primary_blocker"):
            blockers.append(str(report["primary_blocker"]))
        blockers.extend(str(item) for item in report.get("secondary_blockers") or ())
        if report.get("last_primary_blocker"):
            blockers.append(str(report["last_primary_blocker"]))
    return _dedupe(blockers)


def _recovery_verdict(report: Mapping[str, Any]) -> str:
    return str(report.get("final_readiness_verdict") or report.get("classification") or "")


def _all_missing(reports: Mapping[str, Mapping[str, Any] | None]) -> bool:
    return all(report is None for report in reports.values())


def _dedupe(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value
