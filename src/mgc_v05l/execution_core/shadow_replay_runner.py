"""Track B no-submit end-to-end shadow replay runner.

This runner only orchestrates existing Track B no-submit boundaries. It does
not execute strategy rules, connect to market data or broker APIs, or submit
orders.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .attrition_report import AttritionReportResult, AttritionReportVerdict, create_attrition_report
from .models import require_aware_datetime, to_jsonable
from .shadow_run_assembler import ShadowRunAssemblerResult, ShadowRunAssemblerVerdict, assemble_shadow_run
from .signal_batch import SignalBatchResult, SignalBatchVerdict, process_signal_batch


DEFAULT_SHADOW_REPLAY_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/shadow_replay_runs")


class ShadowReplayRunnerVerdict(str, Enum):
    COMPLETED_FOR_REVIEW = "SHADOW_REPLAY_RUN_COMPLETED_FOR_REVIEW"
    COMPLETED_WITH_BLOCKERS = "SHADOW_REPLAY_RUN_COMPLETED_WITH_BLOCKERS"
    BLOCKED_SIGNAL_BATCH = "SHADOW_REPLAY_RUN_BLOCKED_SIGNAL_BATCH"
    BLOCKED_NO_PROPOSED_INTENTS = "SHADOW_REPLAY_RUN_BLOCKED_NO_PROPOSED_INTENTS"
    BLOCKED_SHADOW_ASSEMBLY = "SHADOW_REPLAY_RUN_BLOCKED_SHADOW_ASSEMBLY"
    BLOCKED_ATTRITION_REPORT = "SHADOW_REPLAY_RUN_BLOCKED_ATTRITION_REPORT"
    BLOCKED_SCHEMA_ERROR = "SHADOW_REPLAY_RUN_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class ShadowReplayRunnerResult:
    verdict: ShadowReplayRunnerVerdict
    report_json: Path
    report: dict[str, Any]


def run_shadow_replay(
    *,
    signal_batch_payload: Mapping[str, Any],
    proposal_policy_payload: Mapping[str, Any],
    manifest_payload: Mapping[str, Any],
    registry_payload: Mapping[str, Any],
    readiness_summary_payload: Mapping[str, Any] | None = None,
    readiness_summary_json: Path | None = None,
    output_root: Path = DEFAULT_SHADOW_REPLAY_RUNNER_OUTPUT_ROOT,
    run_id: str | None = None,
    now: datetime | None = None,
) -> ShadowReplayRunnerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or str(manifest_payload.get("run_id") or signal_batch_payload.get("shadow_run_id") or f"shadow_replay_{uuid.uuid4().hex}")
    run_root = Path(output_root) / actual_run_id
    report_json = run_root / "shadow_replay_runner_summary.json"
    try:
        signal_batch_result = process_signal_batch(
            batch_payload=signal_batch_payload,
            policy_payload=proposal_policy_payload,
            expected_account_id=_expected_account_id(manifest_payload, signal_batch_payload),
            output_root=run_root / "signal_batches",
            run_id=f"{actual_run_id}_signal_batch",
            now=actual_now,
        )
        if signal_batch_result.verdict not in {SignalBatchVerdict.PROCESSED_FOR_REVIEW, SignalBatchVerdict.COMPLETED_WITH_BLOCKERS}:
            attrition = _create_attrition_or_none(
                signal_batch_result=signal_batch_result,
                shadow_run_result=None,
                readiness_summary_payload=readiness_summary_payload,
                output_root=run_root / "attrition_reports",
                run_id=f"{actual_run_id}_attrition",
                now=actual_now,
            )
            report = _summary_report(
                report_json=report_json,
                now=actual_now,
                run_id=actual_run_id,
                verdict=ShadowReplayRunnerVerdict.BLOCKED_SIGNAL_BATCH,
                signal_batch_result=signal_batch_result,
                shadow_run_result=None,
                attrition_result=attrition,
                readiness_summary_json=readiness_summary_json,
                primary_blocker=str(signal_batch_result.report.get("primary_blocker") or "Signal batch blocked shadow replay."),
                secondary_blockers=_secondary_from(signal_batch_result.report),
                required_next_action=str(signal_batch_result.report.get("required_next_action") or "Fix signal batch before replay."),
            )
            return _write(report_json, ShadowReplayRunnerVerdict.BLOCKED_SIGNAL_BATCH, report)

        proposed_intent_paths = [Path(path) for path in signal_batch_result.report.get("proposed_intent_output_paths") or ()]
        if not proposed_intent_paths:
            attrition = _create_attrition_or_none(
                signal_batch_result=signal_batch_result,
                shadow_run_result=None,
                readiness_summary_payload=readiness_summary_payload,
                output_root=run_root / "attrition_reports",
                run_id=f"{actual_run_id}_attrition",
                now=actual_now,
            )
            report = _summary_report(
                report_json=report_json,
                now=actual_now,
                run_id=actual_run_id,
                verdict=ShadowReplayRunnerVerdict.BLOCKED_NO_PROPOSED_INTENTS,
                signal_batch_result=signal_batch_result,
                shadow_run_result=None,
                attrition_result=attrition,
                readiness_summary_json=readiness_summary_json,
                primary_blocker="Signal batch produced no proposed intent artifacts.",
                secondary_blockers=_secondary_from(signal_batch_result.report),
                required_next_action="Review proposal blockers before shadow run assembly.",
            )
            return _write(report_json, ShadowReplayRunnerVerdict.BLOCKED_NO_PROPOSED_INTENTS, report)

        proposed_intents = [json.loads(path.read_text(encoding="utf-8")) for path in proposed_intent_paths]
        readiness_path = _materialize_readiness_summary(
            run_root=run_root,
            readiness_summary_payload=readiness_summary_payload,
            readiness_summary_json=readiness_summary_json,
        )
        shadow_run_result = assemble_shadow_run(
            manifest_payload=manifest_payload,
            registry_payload=registry_payload,
            intent_payloads=proposed_intents,
            readiness_summary_json=readiness_path,
            output_root=run_root / "shadow_runs",
            run_id=f"{actual_run_id}_shadow_run",
            now=actual_now,
        )
        attrition_result = create_attrition_report(
            signal_batch_summary=signal_batch_result.report,
            shadow_run_summary=shadow_run_result.report,
            readiness_summary=readiness_summary_payload,
            output_root=run_root / "attrition_reports",
            run_id=f"{actual_run_id}_attrition",
            now=actual_now,
        )
        if attrition_result.verdict not in {AttritionReportVerdict.CREATED_FOR_REVIEW, AttritionReportVerdict.CREATED_WITH_MISSING_STAGES}:
            report = _summary_report(
                report_json=report_json,
                now=actual_now,
                run_id=actual_run_id,
                verdict=ShadowReplayRunnerVerdict.BLOCKED_ATTRITION_REPORT,
                signal_batch_result=signal_batch_result,
                shadow_run_result=shadow_run_result,
                attrition_result=attrition_result,
                readiness_summary_json=readiness_path,
                primary_blocker=str(attrition_result.report.get("primary_blocker") or "Attrition report blocked shadow replay summary."),
                secondary_blockers=_secondary_from(attrition_result.report),
                required_next_action=str(attrition_result.report.get("required_next_action") or "Fix attrition input summaries."),
            )
            return _write(report_json, ShadowReplayRunnerVerdict.BLOCKED_ATTRITION_REPORT, report)

        if shadow_run_result.verdict not in {ShadowRunAssemblerVerdict.ASSEMBLED_FOR_REVIEW, ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS}:
            verdict = ShadowReplayRunnerVerdict.BLOCKED_SHADOW_ASSEMBLY
            primary = str(shadow_run_result.report.get("primary_blocker") or "Shadow run assembly blocked replay.")
            action = str(shadow_run_result.report.get("required_next_action") or "Fix shadow run assembly blocker.")
        elif (
            signal_batch_result.verdict == SignalBatchVerdict.COMPLETED_WITH_BLOCKERS
            or shadow_run_result.verdict == ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
        ):
            verdict = ShadowReplayRunnerVerdict.COMPLETED_WITH_BLOCKERS
            primary = "One or more no-submit replay stages completed with blockers."
            action = "Review proposal, shadow run, and attrition reports before any further paper review."
        else:
            verdict = ShadowReplayRunnerVerdict.COMPLETED_FOR_REVIEW
            primary = None
            action = "Review generated no-submit replay artifacts. All submit gates remain external and required."

        report = _summary_report(
            report_json=report_json,
            now=actual_now,
            run_id=actual_run_id,
            verdict=verdict,
            signal_batch_result=signal_batch_result,
            shadow_run_result=shadow_run_result,
            attrition_result=attrition_result,
            readiness_summary_json=readiness_path,
            primary_blocker=primary,
            secondary_blockers=_combined_secondary_blockers(signal_batch_result, shadow_run_result, attrition_result),
            required_next_action=action,
        )
        return _write(report_json, verdict, report)
    except (TypeError, ValueError, OSError) as exc:
        report = {
            "schema_version": "track_b_shadow_replay_runner_v1",
            "generated_at": actual_now.isoformat(),
            "shadow_replay_run_id": actual_run_id,
            "runner_verdict": ShadowReplayRunnerVerdict.BLOCKED_SCHEMA_ERROR.value,
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
            "primary_blocker": str(exc),
            "secondary_blockers": [],
            "required_next_action": "Fix shadow replay runner input JSON before no-submit replay.",
            "broker_connection_attempted": False,
            "market_data_connection_attempted": False,
            "strategy_execution_attempted": False,
            "paper_proof_cli_wired": False,
            "report_json_path": str(report_json),
        }
        return _write(report_json, ShadowReplayRunnerVerdict.BLOCKED_SCHEMA_ERROR, report)


def _summary_report(
    *,
    report_json: Path,
    now: datetime,
    run_id: str,
    verdict: ShadowReplayRunnerVerdict,
    signal_batch_result: SignalBatchResult,
    shadow_run_result: ShadowRunAssemblerResult | None,
    attrition_result: AttritionReportResult | None,
    readiness_summary_json: Path | None,
    primary_blocker: str | None,
    secondary_blockers: list[str],
    required_next_action: str,
) -> dict[str, Any]:
    signal_report = signal_batch_result.report
    shadow_report = {} if shadow_run_result is None else shadow_run_result.report
    attrition_report = {} if attrition_result is None else attrition_result.report
    return {
        "schema_version": "track_b_shadow_replay_runner_v1",
        "generated_at": now.isoformat(),
        "shadow_replay_run_id": run_id,
        "signal_batch_id": signal_report.get("signal_batch_id"),
        "manifest_run_id": shadow_report.get("shadow_run_id"),
        "runner_verdict": verdict.value,
        "total_signals": signal_report.get("total_signals", 0),
        "proposed_intents_created": signal_report.get("proposed_intents_created", 0),
        "shadow_run_verdict": shadow_report.get("shadow_run_verdict"),
        "attrition_report_verdict": attrition_report.get("attrition_report_verdict"),
        "primary_blocker": primary_blocker,
        "secondary_blockers": secondary_blockers,
        "required_next_action": required_next_action,
        "signal_batch_summary_path": str(signal_batch_result.report_json),
        "proposal_report_paths": list(signal_report.get("proposal_report_output_paths") or ()),
        "proposed_intent_paths": list(signal_report.get("proposed_intent_output_paths") or ()),
        "shadow_run_summary_path": None if shadow_run_result is None else str(shadow_run_result.report_json),
        "attrition_report_path": None if attrition_result is None else str(attrition_result.report_json),
        "readiness_summary_path": None if readiness_summary_json is None else str(readiness_summary_json),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "strategy_execution_attempted": False,
        "dynamic_scoring_implemented": False,
        "paper_proof_cli_wired": False,
        "missing_downstream_stages_explicit": True,
        "report_json_path": str(report_json),
    }


def _create_attrition_or_none(
    *,
    signal_batch_result: SignalBatchResult,
    shadow_run_result: ShadowRunAssemblerResult | None,
    readiness_summary_payload: Mapping[str, Any] | None,
    output_root: Path,
    run_id: str,
    now: datetime,
) -> AttritionReportResult | None:
    return create_attrition_report(
        signal_batch_summary=signal_batch_result.report,
        shadow_run_summary=None if shadow_run_result is None else shadow_run_result.report,
        readiness_summary=readiness_summary_payload,
        output_root=output_root,
        run_id=run_id,
        now=now,
    )


def _materialize_readiness_summary(
    *,
    run_root: Path,
    readiness_summary_payload: Mapping[str, Any] | None,
    readiness_summary_json: Path | None,
) -> Path | None:
    if readiness_summary_json is not None:
        return readiness_summary_json
    if readiness_summary_payload is None:
        return None
    path = run_root / "inputs" / "readiness_summary.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(readiness_summary_payload)), indent=2, sort_keys=True), encoding="utf-8")
    return path


def _expected_account_id(manifest_payload: Mapping[str, Any], signal_batch_payload: Mapping[str, Any]) -> str | None:
    value = manifest_payload.get("expected_account_id") or signal_batch_payload.get("expected_account_id")
    if value is None:
        return None
    return str(value)


def _combined_secondary_blockers(
    signal_batch_result: SignalBatchResult,
    shadow_run_result: ShadowRunAssemblerResult,
    attrition_result: AttritionReportResult,
) -> list[str]:
    blockers: list[str] = []
    for report in (signal_batch_result.report, shadow_run_result.report, attrition_result.report):
        if report.get("primary_blocker"):
            blockers.append(str(report["primary_blocker"]))
        blockers.extend(str(item) for item in report.get("secondary_blockers") or ())
    return _dedupe(blockers)


def _secondary_from(report: Mapping[str, Any]) -> list[str]:
    blockers = [str(item) for item in report.get("secondary_blockers") or ()]
    if report.get("primary_blocker"):
        blockers.insert(0, str(report["primary_blocker"]))
    return _dedupe(blockers)


def _dedupe(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _write(report_json: Path, verdict: ShadowReplayRunnerVerdict, report: dict[str, Any]) -> ShadowReplayRunnerResult:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return ShadowReplayRunnerResult(verdict=verdict, report_json=report_json, report=report)
