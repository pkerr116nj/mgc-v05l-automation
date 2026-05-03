"""Track B no-submit shadow run assembler.

This module orchestrates existing Track B report boundaries for a batch of
strategy-intent JSON payloads. It deliberately does not execute strategy rules,
connect to market data or broker APIs, or submit orders.
"""

from __future__ import annotations

import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .order_plan import OrderPlanConfig, OrderPlanResult, OrderPlanVerdict, create_order_plan_from_intent
from .shadow_evaluation import ShadowEvaluationConfig, ShadowEvaluationResult, ShadowEvaluationVerdict, run_shadow_evaluation
from .shadow_run_manifest import ShadowRunManifestResult, ShadowRunManifestVerdict, validate_shadow_run_manifest
from .strategy_intent import StrategyIntentValidationConfig, StrategyIntentValidationResult, StrategyIntentValidationVerdict, validate_strategy_intent
from .strategy_lane_registry import LaneValidationResult, LaneValidationVerdict, validate_strategy_lane


DEFAULT_SHADOW_RUN_OUTPUT_ROOT = Path("outputs/track_b_execution_core/shadow_runs")


class ShadowRunAssemblerVerdict(str, Enum):
    ASSEMBLED_FOR_REVIEW = "SHADOW_RUN_ASSEMBLED_FOR_REVIEW"
    BLOCKED_INVALID_MANIFEST = "SHADOW_RUN_BLOCKED_INVALID_MANIFEST"
    BLOCKED_EMPTY_INTENTS = "SHADOW_RUN_BLOCKED_EMPTY_INTENTS"
    COMPLETED_WITH_BLOCKERS = "SHADOW_RUN_COMPLETED_WITH_BLOCKERS"
    BLOCKED_SCHEMA_ERROR = "SHADOW_RUN_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class ShadowRunAssemblerResult:
    verdict: ShadowRunAssemblerVerdict
    report_json: Path
    report: dict[str, Any]


def assemble_shadow_run(
    *,
    manifest_payload: Mapping[str, Any],
    registry_payload: Mapping[str, Any],
    intent_payloads: Sequence[Mapping[str, Any]],
    readiness_summary_payload: Mapping[str, Any] | None = None,
    readiness_summary_json: Path | None = None,
    output_root: Path = DEFAULT_SHADOW_RUN_OUTPUT_ROOT,
    run_id: str | None = None,
    now: datetime | None = None,
) -> ShadowRunAssemblerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or str(manifest_payload.get("run_id") or f"shadow_run_{uuid.uuid4().hex}")
    run_root = Path(output_root) / actual_run_id
    report_json = run_root / "shadow_run_summary_report.json"

    try:
        manifest_result = validate_shadow_run_manifest(
            payload=manifest_payload,
            output_root=run_root / "manifest_validation",
            run_id=f"{actual_run_id}_manifest",
            now=actual_now,
        )
        if manifest_result.verdict != ShadowRunManifestVerdict.VALID:
            return _write_summary(
                report_json=report_json,
                verdict=ShadowRunAssemblerVerdict.BLOCKED_INVALID_MANIFEST,
                now=actual_now,
                manifest_result=manifest_result,
                per_intent_reports=[],
                primary_blocker=str(manifest_result.report.get("primary_blocker") or "Shadow run manifest is invalid."),
                required_next_action=str(manifest_result.report.get("required_next_action") or "Fix manifest before assembling a shadow run."),
            )
        if not intent_payloads:
            return _write_summary(
                report_json=report_json,
                verdict=ShadowRunAssemblerVerdict.BLOCKED_EMPTY_INTENTS,
                now=actual_now,
                manifest_result=manifest_result,
                per_intent_reports=[],
                primary_blocker="No strategy intent payloads were provided.",
                required_next_action="Provide one or more intent JSON files for the no-submit shadow run.",
            )
        readiness_path = _prepare_readiness_summary(
            run_root=run_root,
            readiness_summary_payload=readiness_summary_payload,
            readiness_summary_json=readiness_summary_json,
        )
        per_intent_reports = [
            _process_intent(
                index=index,
                intent_payload=intent_payload,
                registry_payload=registry_payload,
                manifest_result=manifest_result,
                readiness_summary_json=readiness_path,
                run_root=run_root,
                run_id=actual_run_id,
                now=actual_now,
            )
            for index, intent_payload in enumerate(intent_payloads, start=1)
        ]
        blocked_count = sum(1 for item in per_intent_reports if item.get("primary_blocker"))
        verdict = ShadowRunAssemblerVerdict.ASSEMBLED_FOR_REVIEW if blocked_count == 0 else ShadowRunAssemblerVerdict.COMPLETED_WITH_BLOCKERS
        return _write_summary(
            report_json=report_json,
            verdict=verdict,
            now=actual_now,
            manifest_result=manifest_result,
            per_intent_reports=per_intent_reports,
            primary_blocker=None if blocked_count == 0 else "One or more intents were blocked during no-submit shadow assembly.",
            required_next_action=(
                "Review generated shadow evaluation reports. All submit gates remain external and required."
                if blocked_count == 0
                else "Review per-intent blockers before any further Track B paper review."
            ),
        )
    except (TypeError, ValueError, OSError) as exc:
        report = {
            "schema_version": "track_b_shadow_run_assembler_v1",
            "generated_at": actual_now.isoformat(),
            "shadow_run_id": actual_run_id,
            "shadow_run_verdict": ShadowRunAssemblerVerdict.BLOCKED_SCHEMA_ERROR.value,
            "manifest_validation_verdict": None,
            "run_allowed_for_shadow": False,
            "total_intents": len(intent_payloads),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
            "primary_blocker": str(exc),
            "secondary_blockers": [],
            "required_next_action": "Fix shadow run input schema before assembly.",
            "output_paths": {"summary_report_json": str(report_json)},
        }
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        return ShadowRunAssemblerResult(verdict=ShadowRunAssemblerVerdict.BLOCKED_SCHEMA_ERROR, report_json=report_json, report=report)


def _process_intent(
    *,
    index: int,
    intent_payload: Mapping[str, Any],
    registry_payload: Mapping[str, Any],
    manifest_result: ShadowRunManifestResult,
    readiness_summary_json: Path | None,
    run_root: Path,
    run_id: str,
    now: datetime,
) -> dict[str, Any]:
    item_id = f"intent_{index:04d}"
    expected_account_id = str(manifest_result.report.get("expected_account_id") or "")
    contract_allowlist = {key: {} for key in manifest_result.report.get("allowed_local_execution_contract_keys", ())}
    intent_result = validate_strategy_intent(
        payload=intent_payload,
        config=StrategyIntentValidationConfig(
            expected_account_id=expected_account_id,
            contract_allowlist=contract_allowlist,
            output_root=run_root / "intent_validation",
        ),
        run_id=f"{run_id}_{item_id}_intent",
        now=now,
    )
    lane_result = validate_strategy_lane(
        intent_payload=intent_payload,
        registry_payload=registry_payload,
        output_root=run_root / "lane_validation",
        run_id=f"{run_id}_{item_id}_lane",
        now=now,
    )
    order_plan: OrderPlanResult | None = None
    shadow_evaluation: ShadowEvaluationResult | None = None
    primary_blocker = _first_blocker(intent_result, lane_result)
    required_next_action = _first_action(intent_result, lane_result)

    if primary_blocker is None:
        order_plan = create_order_plan_from_intent(
            payload=intent_payload,
            config=OrderPlanConfig(
                expected_account_id=expected_account_id,
                contract_allowlist=contract_allowlist,
                output_root=run_root / "order_plans",
            ),
            run_id=f"{run_id}_{item_id}_order_plan",
            now=now,
        )
        if order_plan.verdict == OrderPlanVerdict.CREATED_FOR_PAPER_REVIEW:
            shadow_evaluation = run_shadow_evaluation(
                payload=intent_payload,
                config=ShadowEvaluationConfig(
                    expected_account_id=expected_account_id,
                    contract_allowlist=contract_allowlist,
                    output_root=run_root / "shadow_evaluations",
                    readiness_summary_json=readiness_summary_json,
                ),
                run_id=f"{run_id}_{item_id}_shadow_eval",
                now=now,
                order_plan_factory=lambda _payload, _config, _run_id, _now: order_plan,  # noqa: ARG005
            )
            if shadow_evaluation.verdict != ShadowEvaluationVerdict.CREATED_FOR_REVIEW:
                primary_blocker = str(shadow_evaluation.report.get("primary_blocker") or "Shadow evaluation was blocked.")
                required_next_action = str(shadow_evaluation.report.get("required_next_action") or "Resolve shadow evaluation blocker.")
        else:
            primary_blocker = str(order_plan.report.get("primary_blocker") or "Order plan was not created.")
            required_next_action = str(order_plan.report.get("required_next_action") or "Fix order plan blocker.")

    item_report_json = run_root / "per_intent" / item_id / "shadow_run_intent_report.json"
    item_report = {
        "schema_version": "track_b_shadow_run_intent_item_v1",
        "generated_at": now.isoformat(),
        "shadow_run_intent_index": index,
        "intent_validation_verdict": intent_result.verdict.value,
        "lane_validation_verdict": lane_result.verdict.value,
        "order_plan_verdict": None if order_plan is None else order_plan.verdict.value,
        "shadow_evaluation_verdict": None if shadow_evaluation is None else shadow_evaluation.verdict.value,
        "intent_allowed_for_review": bool(intent_result.report.get("intent_allowed_for_review")),
        "lane_authorized_for_review": bool(lane_result.report.get("lane_authorized_for_review")),
        "order_plan_created": False if order_plan is None else bool(order_plan.report.get("order_plan_created")),
        "shadow_evaluation_created": False if shadow_evaluation is None else shadow_evaluation.verdict == ShadowEvaluationVerdict.CREATED_FOR_REVIEW,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": _item_secondary_blockers(intent_result, lane_result, order_plan, shadow_evaluation),
        "required_next_action": required_next_action,
        "strategy_id": intent_result.report.get("strategy_id") or lane_result.report.get("strategy_id"),
        "lane_id": intent_result.report.get("lane_id") or lane_result.report.get("lane_id"),
        "account_id": intent_result.report.get("account_id") or lane_result.report.get("account_id"),
        "local_execution_contract_key": intent_result.report.get("local_execution_contract_key") or lane_result.report.get("local_execution_contract_key"),
        "intent_report_json": str(intent_result.report_json),
        "lane_validation_report_json": str(lane_result.report_json),
        "order_plan_report_json": None if order_plan is None else str(order_plan.report_json),
        "shadow_evaluation_report_json": None if shadow_evaluation is None else str(shadow_evaluation.report_json),
        "item_report_json": str(item_report_json),
    }
    item_report_json.parent.mkdir(parents=True, exist_ok=True)
    item_report_json.write_text(json.dumps(to_jsonable(item_report), indent=2, sort_keys=True), encoding="utf-8")
    return item_report


def _write_summary(
    *,
    report_json: Path,
    verdict: ShadowRunAssemblerVerdict,
    now: datetime,
    manifest_result: ShadowRunManifestResult,
    per_intent_reports: Sequence[Mapping[str, Any]],
    primary_blocker: str | None,
    required_next_action: str,
) -> ShadowRunAssemblerResult:
    blockers = Counter(str(item.get("primary_blocker") or "") for item in per_intent_reports if item.get("primary_blocker"))
    output_paths = {
        "summary_report_json": str(report_json),
        "manifest_report_json": str(manifest_result.report_json),
        "per_intent_reports": [str(item.get("item_report_json")) for item in per_intent_reports],
        "intent_validation_reports": [str(item.get("intent_report_json")) for item in per_intent_reports],
        "lane_validation_reports": [str(item.get("lane_validation_report_json")) for item in per_intent_reports],
        "order_plan_reports": [str(item.get("order_plan_report_json")) for item in per_intent_reports if item.get("order_plan_report_json")],
        "shadow_evaluation_reports": [str(item.get("shadow_evaluation_report_json")) for item in per_intent_reports if item.get("shadow_evaluation_report_json")],
    }
    report = {
        "schema_version": "track_b_shadow_run_assembler_v1",
        "generated_at": now.isoformat(),
        "shadow_run_id": manifest_result.report.get("run_id"),
        "shadow_run_verdict": verdict.value,
        "manifest_validation_verdict": manifest_result.verdict.value,
        "run_allowed_for_shadow": verdict == ShadowRunAssemblerVerdict.ASSEMBLED_FOR_REVIEW,
        "total_intents": len(per_intent_reports),
        "intents_validated": sum(1 for item in per_intent_reports if item.get("intent_validation_verdict") == StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW.value),
        "intents_authorized_by_lane_registry": sum(
            1
            for item in per_intent_reports
            if item.get("lane_validation_verdict") in {LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW.value, LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW.value}
        ),
        "order_plans_created": sum(1 for item in per_intent_reports if item.get("order_plan_created")),
        "shadow_evaluations_created": sum(1 for item in per_intent_reports if item.get("shadow_evaluation_created")),
        "blocked_count": sum(1 for item in per_intent_reports if item.get("primary_blocker")),
        "primary_blockers_count_by_type": dict(blockers),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": _summary_secondary_blockers(per_intent_reports),
        "required_next_action": required_next_action,
        "output_paths": output_paths,
        "manifest_governs_run_context_only": True,
        "lane_registry_governs_lane_authorization": True,
        "strategy_intent_governs_proposed_action": True,
        "order_plan_boundary_is_no_submit": True,
        "shadow_evaluation_boundary_is_no_submit": True,
        "paper_proof_cli_remains_only_submit_path": True,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return ShadowRunAssemblerResult(verdict=verdict, report_json=report_json, report=report)


def _item_secondary_blockers(
    intent_result: StrategyIntentValidationResult,
    lane_result: LaneValidationResult,
    order_plan: OrderPlanResult | None,
    shadow_evaluation: ShadowEvaluationResult | None,
) -> list[str]:
    blockers: list[str] = []
    for report in (
        intent_result.report,
        lane_result.report,
        {} if order_plan is None else order_plan.report,
        {} if shadow_evaluation is None else shadow_evaluation.report,
    ):
        blockers.extend(str(item) for item in report.get("secondary_blockers") or ())
    return _dedupe(blockers)


def _summary_secondary_blockers(per_intent_reports: Sequence[Mapping[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for item in per_intent_reports:
        if item.get("primary_blocker"):
            blockers.append(str(item["primary_blocker"]))
        blockers.extend(str(blocker) for blocker in item.get("secondary_blockers") or ())
    return _dedupe(blockers)


def _dedupe(values: Sequence[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _prepare_readiness_summary(
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


def _first_blocker(intent_result: StrategyIntentValidationResult, lane_result: LaneValidationResult) -> str | None:
    if intent_result.verdict != StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW:
        return str(intent_result.report.get("primary_blocker") or "Strategy intent validation blocked review.")
    if lane_result.verdict not in {LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW, LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW}:
        return str(lane_result.report.get("primary_blocker") or "Lane registry blocked review.")
    return None


def _first_action(intent_result: StrategyIntentValidationResult, lane_result: LaneValidationResult) -> str:
    if intent_result.verdict != StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW:
        return str(intent_result.report.get("required_next_action") or "Fix intent before shadow review.")
    if lane_result.verdict not in {LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW, LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW}:
        return str(lane_result.report.get("required_next_action") or "Fix lane registry blocker before shadow review.")
    return "Review generated no-submit Track B shadow evaluation artifacts."
