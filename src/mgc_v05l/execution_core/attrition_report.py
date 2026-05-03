"""Track B no-submit attrition reporting boundary."""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_ATTRITION_REPORT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/attrition_reports")
MISSING_STAGE = "NOT_PROVIDED"


class AttritionReportVerdict(str, Enum):
    CREATED_FOR_REVIEW = "ATTRITION_REPORT_CREATED_FOR_REVIEW"
    CREATED_WITH_MISSING_STAGES = "ATTRITION_REPORT_CREATED_WITH_MISSING_STAGES"
    BLOCKED_SCHEMA_ERROR = "ATTRITION_REPORT_BLOCKED_SCHEMA_ERROR"
    BLOCKED_NO_INPUTS = "ATTRITION_REPORT_BLOCKED_NO_INPUTS"


@dataclass(frozen=True)
class AttritionReportResult:
    verdict: AttritionReportVerdict
    report_json: Path
    report: dict[str, Any]


def create_attrition_report(
    *,
    signal_batch_summary: Mapping[str, Any] | None = None,
    shadow_run_summary: Mapping[str, Any] | None = None,
    readiness_summary: Mapping[str, Any] | None = None,
    output_root: Path = DEFAULT_ATTRITION_REPORT_OUTPUT_ROOT,
    run_id: str | None = None,
    now: datetime | None = None,
) -> AttritionReportResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or _report_id(signal_batch_summary, shadow_run_summary, readiness_summary)
    report_json = Path(output_root) / actual_run_id / "attrition_report.json"
    try:
        if signal_batch_summary is None and shadow_run_summary is None and readiness_summary is None:
            report = _blocked_report(
                report_json=report_json,
                now=actual_now,
                report_id=actual_run_id,
                verdict=AttritionReportVerdict.BLOCKED_NO_INPUTS,
                blocker="No Track B no-submit summaries were supplied.",
                action="Provide at least a signal batch summary or shadow run summary.",
            )
            return _write(report_json, AttritionReportVerdict.BLOCKED_NO_INPUTS, report)
        report = _report(
            report_json=report_json,
            now=actual_now,
            report_id=actual_run_id,
            signal_batch_summary=signal_batch_summary,
            shadow_run_summary=shadow_run_summary,
            readiness_summary=readiness_summary,
        )
        verdict = (
            AttritionReportVerdict.CREATED_WITH_MISSING_STAGES
            if report["missing_stages"]
            else AttritionReportVerdict.CREATED_FOR_REVIEW
        )
        report["attrition_report_verdict"] = verdict.value
        return _write(report_json, verdict, report)
    except (TypeError, ValueError) as exc:
        report = _blocked_report(
            report_json=report_json,
            now=actual_now,
            report_id=actual_run_id,
            verdict=AttritionReportVerdict.BLOCKED_SCHEMA_ERROR,
            blocker=str(exc),
            action="Fix supplied Track B summary JSON before attrition reporting.",
        )
        return _write(report_json, AttritionReportVerdict.BLOCKED_SCHEMA_ERROR, report)


def _report(
    *,
    report_json: Path,
    now: datetime,
    report_id: str,
    signal_batch_summary: Mapping[str, Any] | None,
    shadow_run_summary: Mapping[str, Any] | None,
    readiness_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    missing_stages = []
    if signal_batch_summary is None:
        missing_stages.append("signal_batch")
    if shadow_run_summary is None:
        missing_stages.append("shadow_run")
    if readiness_summary is None:
        missing_stages.append("readiness")

    signal_batch_present = signal_batch_summary is not None
    shadow_run_present = shadow_run_summary is not None
    readiness_present = readiness_summary is not None

    total_signals: int | str = _int_field(signal_batch_summary, "total_signals") if signal_batch_present else MISSING_STAGE
    signals_validated: int | str = _int_field(signal_batch_summary, "signals_validated") if signal_batch_present else MISSING_STAGE
    proposal_attempts: int | str = _int_field(signal_batch_summary, "proposal_attempts") if signal_batch_present else MISSING_STAGE
    proposed_intents_created: int | str = _int_field(signal_batch_summary, "proposed_intents_created") if signal_batch_present else MISSING_STAGE
    proposals_blocked: int | str = _int_field(signal_batch_summary, "blocked_proposals") if signal_batch_present else MISSING_STAGE

    intents_submitted_to_registry: int | str = _int_field(shadow_run_summary, "total_intents") if shadow_run_present else MISSING_STAGE
    intents_authorized_by_registry: int | str = _int_field(shadow_run_summary, "intents_authorized_by_lane_registry") if shadow_run_present else MISSING_STAGE
    lane_registry_blocked: int | str = (
        _safe_subtract(_int_field(shadow_run_summary, "total_intents"), _int_field(shadow_run_summary, "intents_authorized_by_lane_registry"))
        if shadow_run_present
        else MISSING_STAGE
    )
    order_plans_created: int | str = _int_field(shadow_run_summary, "order_plans_created") if shadow_run_present else MISSING_STAGE
    order_plan_blocked: int | str = (
        _safe_subtract(_int_field(shadow_run_summary, "intents_authorized_by_lane_registry"), _int_field(shadow_run_summary, "order_plans_created"))
        if shadow_run_present
        else MISSING_STAGE
    )
    shadow_evaluations_created: int | str = _int_field(shadow_run_summary, "shadow_evaluations_created") if shadow_run_present else MISSING_STAGE

    readiness_verdict = str((readiness_summary or {}).get("final_readiness_verdict") or "")
    readiness_allowed: bool | str = _readiness_allowed(readiness_verdict) if readiness_present else MISSING_STAGE
    readiness_blocked: bool | str = (not _readiness_allowed(readiness_verdict)) if readiness_present else MISSING_STAGE

    blockers_by_stage = _blockers_by_stage(
        proposals_blocked=proposals_blocked,
        lane_registry_blocked=lane_registry_blocked,
        order_plan_blocked=order_plan_blocked,
        shadow_run_summary=shadow_run_summary,
        readiness_blocked=readiness_blocked,
    )
    blockers_by_type = Counter()
    blockers_by_type.update(_mapping_counter((signal_batch_summary or {}).get("blockers_count_by_type")))
    blockers_by_type.update(_mapping_counter((shadow_run_summary or {}).get("primary_blockers_count_by_type")))
    if readiness_blocked is True:
        blockers_by_type.update({str(readiness_summary.get("final_readiness_verdict") or "READINESS_BLOCKED"): 1})

    primary_stage = _primary_attrition_stage(blockers_by_stage)
    output_artifacts = {
        "signal_batch_summary": (signal_batch_summary or {}).get("report_json_path"),
        "signal_batch_proposed_intents": (signal_batch_summary or {}).get("proposed_intent_output_paths") if signal_batch_present else MISSING_STAGE,
        "signal_batch_proposal_reports": (signal_batch_summary or {}).get("proposal_report_output_paths") if signal_batch_present else MISSING_STAGE,
        "shadow_run_summary": (shadow_run_summary or {}).get("report_json_path") or (shadow_run_summary or {}).get("output_paths", {}).get("summary_report_json"),
        "shadow_run_reports": (shadow_run_summary or {}).get("output_paths") if shadow_run_present else MISSING_STAGE,
        "readiness_summary": (readiness_summary or {}).get("report_json_path") if readiness_present else MISSING_STAGE,
    }

    return {
        "schema_version": "track_b_attrition_report_v1",
        "generated_at": now.isoformat(),
        "attrition_report_id": report_id,
        "attrition_report_verdict": AttritionReportVerdict.CREATED_FOR_REVIEW.value,
        "run_id": (shadow_run_summary or {}).get("shadow_run_id") or (signal_batch_summary or {}).get("shadow_run_id"),
        "batch_id": (signal_batch_summary or {}).get("signal_batch_id"),
        "total_signals": total_signals,
        "signals_validated": signals_validated,
        "proposal_attempts": proposal_attempts,
        "proposed_intents_created": proposed_intents_created,
        "proposals_blocked": proposals_blocked,
        "intents_submitted_to_registry": intents_submitted_to_registry,
        "intents_authorized_by_registry": intents_authorized_by_registry,
        "lane_registry_blocked": lane_registry_blocked,
        "order_plans_created": order_plans_created,
        "order_plan_blocked": order_plan_blocked,
        "shadow_evaluations_created": shadow_evaluations_created,
        "readiness_allowed": readiness_allowed,
        "readiness_blocked": readiness_blocked,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "blockers_count_by_stage": blockers_by_stage,
        "blockers_count_by_type": dict(blockers_by_type),
        "primary_attrition_stage": primary_stage,
        "missing_stages": missing_stages,
        "output_artifacts_considered": output_artifacts,
        "report_is_explanatory_only": True,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "report_json_path": str(report_json),
    }


def _blocked_report(
    *,
    report_json: Path,
    now: datetime,
    report_id: str,
    verdict: AttritionReportVerdict,
    blocker: str,
    action: str,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_attrition_report_v1",
        "generated_at": now.isoformat(),
        "attrition_report_id": report_id,
        "attrition_report_verdict": verdict.value,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": blocker,
        "required_next_action": action,
        "report_is_explanatory_only": True,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "report_json_path": str(report_json),
    }


def _write(report_json: Path, verdict: AttritionReportVerdict, report: dict[str, Any]) -> AttritionReportResult:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return AttritionReportResult(verdict=verdict, report_json=report_json, report=report)


def _blockers_by_stage(
    *,
    proposals_blocked: int | str,
    lane_registry_blocked: int | str,
    order_plan_blocked: int | str,
    shadow_run_summary: Mapping[str, Any] | None,
    readiness_blocked: bool | str,
) -> dict[str, int | str]:
    return {
        "proposal": proposals_blocked,
        "lane_registry": lane_registry_blocked,
        "order_plan": order_plan_blocked,
        "shadow_evaluation": _shadow_evaluation_blocked(shadow_run_summary) if shadow_run_summary is not None else MISSING_STAGE,
        "readiness": 1 if readiness_blocked is True else 0 if readiness_blocked is False else MISSING_STAGE,
    }


def _primary_attrition_stage(blockers_by_stage: Mapping[str, int | str]) -> str | None:
    for stage in ("proposal", "lane_registry", "order_plan", "shadow_evaluation", "readiness"):
        count = blockers_by_stage.get(stage)
        if isinstance(count, int) and count > 0:
            return stage
    return None


def _shadow_evaluation_blocked(shadow_run_summary: Mapping[str, Any]) -> int:
    order_plans_created = _int_field(shadow_run_summary, "order_plans_created")
    shadow_evaluations_created = _int_field(shadow_run_summary, "shadow_evaluations_created")
    return max(order_plans_created - shadow_evaluations_created, 0)


def _int_field(payload: Mapping[str, Any] | None, key: str) -> int:
    if payload is None:
        return 0
    value = payload.get(key, 0)
    if value in (None, ""):
        return 0
    return int(value)


def _safe_subtract(left: int, right: int) -> int:
    return max(left - right, 0)


def _mapping_counter(value: object) -> Counter[str]:
    counter: Counter[str] = Counter()
    if isinstance(value, Mapping):
        for key, raw_count in value.items():
            counter[str(key)] += int(raw_count or 0)
    return counter


def _readiness_allowed(verdict: str) -> bool:
    return verdict == "READY_FOR_PAPER_PROOF"


def _report_id(*summaries: Mapping[str, Any] | None) -> str:
    for summary in summaries:
        if summary:
            identifier = summary.get("signal_batch_id") or summary.get("shadow_run_id") or summary.get("run_id")
            if identifier:
                return f"attrition_{identifier}"
    return f"attrition_{uuid.uuid4().hex}"
