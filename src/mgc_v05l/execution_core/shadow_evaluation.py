"""Track B no-submit shadow evaluation envelope."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import require_aware_datetime, to_jsonable
from .order_plan import OrderPlanConfig, OrderPlanResult, OrderPlanVerdict, create_order_plan_from_intent
from .strategy_intent import StrategyIntentValidationConfig, StrategyIntentValidationResult, StrategyIntentValidationVerdict, validate_strategy_intent


DEFAULT_SHADOW_EVALUATION_OUTPUT_ROOT = Path("outputs/track_b_execution_core/shadow_evaluations")


class ShadowEvaluationVerdict(str, Enum):
    CREATED_FOR_REVIEW = "SHADOW_EVALUATION_CREATED_FOR_REVIEW"
    BLOCKED_INVALID_INTENT = "SHADOW_EVALUATION_BLOCKED_INVALID_INTENT"
    BLOCKED_ORDER_PLAN = "SHADOW_EVALUATION_BLOCKED_ORDER_PLAN"
    READINESS_BLOCKED = "SHADOW_EVALUATION_READINESS_BLOCKED"


@dataclass(frozen=True)
class ShadowEvaluationConfig:
    expected_account_id: str | None = None
    contract_allowlist: Mapping[str, Mapping[str, Any]] | None = None
    output_root: Path = DEFAULT_SHADOW_EVALUATION_OUTPUT_ROOT
    readiness_summary_json: Path | None = None


@dataclass(frozen=True)
class ShadowEvaluationResult:
    verdict: ShadowEvaluationVerdict
    report_json: Path
    report: dict[str, Any]


OrderPlanFactory = Callable[[Mapping[str, Any], OrderPlanConfig, str, datetime], OrderPlanResult]


def run_shadow_evaluation(
    *,
    payload: Mapping[str, Any],
    config: ShadowEvaluationConfig | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
    order_plan_factory: OrderPlanFactory | None = None,
) -> ShadowEvaluationResult:
    actual_config = config or ShadowEvaluationConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"shadow_eval_{uuid.uuid4().hex}"
    report_json = Path(actual_config.output_root) / actual_run_id / "shadow_evaluation_report.json"
    intent_result = validate_strategy_intent(
        payload=payload,
        config=StrategyIntentValidationConfig(
            expected_account_id=actual_config.expected_account_id,
            contract_allowlist=actual_config.contract_allowlist,
            output_root=Path(actual_config.output_root) / "intent_validation",
        ),
        run_id=f"{actual_run_id}_intent",
        now=actual_now,
    )
    if intent_result.verdict != StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW:
        return _write_report(
            report_json=report_json,
            verdict=ShadowEvaluationVerdict.BLOCKED_INVALID_INTENT,
            now=actual_now,
            intent_result=intent_result,
            order_plan=None,
            readiness_summary=_read_json(actual_config.readiness_summary_json),
            primary_blocker=str(intent_result.report.get("primary_blocker") or "Strategy intent is invalid."),
            required_next_action=str(intent_result.report.get("required_next_action") or "Fix strategy intent before shadow review."),
        )

    plan_factory = order_plan_factory or _default_order_plan_factory
    order_plan = plan_factory(
        payload,
        OrderPlanConfig(
            expected_account_id=actual_config.expected_account_id,
            contract_allowlist=actual_config.contract_allowlist,
            output_root=Path(actual_config.output_root) / "order_plans",
        ),
        f"{actual_run_id}_order_plan",
        actual_now,
    )
    if order_plan.verdict != OrderPlanVerdict.CREATED_FOR_PAPER_REVIEW:
        return _write_report(
            report_json=report_json,
            verdict=ShadowEvaluationVerdict.BLOCKED_ORDER_PLAN,
            now=actual_now,
            intent_result=intent_result,
            order_plan=order_plan,
            readiness_summary=_read_json(actual_config.readiness_summary_json),
            primary_blocker=str(order_plan.report.get("primary_blocker") or "Order plan was not created."),
            required_next_action=str(order_plan.report.get("required_next_action") or "Fix order plan blocker before shadow review."),
        )

    readiness_summary = _read_json(actual_config.readiness_summary_json)
    readiness_verdict = str(readiness_summary.get("final_readiness_verdict") or "")
    if readiness_verdict and readiness_verdict != "READY_FOR_PAPER_PROOF":
        return _write_report(
            report_json=report_json,
            verdict=ShadowEvaluationVerdict.READINESS_BLOCKED,
            now=actual_now,
            intent_result=intent_result,
            order_plan=order_plan,
            readiness_summary=readiness_summary,
            primary_blocker=str(readiness_summary.get("primary_blocker") or "Readiness summary blocks paper proof."),
            required_next_action=str(readiness_summary.get("required_next_action") or "Resolve readiness blocker before any proof submit."),
        )

    return _write_report(
        report_json=report_json,
        verdict=ShadowEvaluationVerdict.CREATED_FOR_REVIEW,
        now=actual_now,
        intent_result=intent_result,
        order_plan=order_plan,
        readiness_summary=readiness_summary,
        primary_blocker=None,
        required_next_action="Shadow evaluation is ready for operator review. All submit gates remain external and required.",
    )


def _write_report(
    *,
    report_json: Path,
    verdict: ShadowEvaluationVerdict,
    now: datetime,
    intent_result: StrategyIntentValidationResult,
    order_plan: OrderPlanResult | None,
    readiness_summary: Mapping[str, Any],
    primary_blocker: str | None,
    required_next_action: str,
) -> ShadowEvaluationResult:
    intent_report = intent_result.report
    plan_report = order_plan.report if order_plan is not None else {}
    shadow_id = _shadow_evaluation_id(intent_report, plan_report)
    report = {
        "schema_version": "track_b_shadow_evaluation_v1",
        "generated_at": now.isoformat(),
        "shadow_evaluation_id": shadow_id,
        "shadow_evaluation_verdict": verdict.value,
        "strategy_id": intent_report.get("strategy_id"),
        "lane_id": intent_report.get("lane_id"),
        "mode": intent_report.get("mode"),
        "account_id": intent_report.get("account_id"),
        "local_execution_contract_key": intent_report.get("local_execution_contract_key"),
        "signal_timestamp": intent_report.get("signal_timestamp"),
        "decision_timestamp": intent_report.get("decision_timestamp"),
        "signal_name": intent_report.get("signal_name"),
        "signal_type": intent_report.get("signal_type"),
        "signal_direction": intent_report.get("side"),
        "signal_reason": intent_report.get("reason"),
        "source": intent_report.get("source"),
        "intent_validation_verdict": intent_result.verdict.value,
        "intent_allowed_for_review": bool(intent_report.get("intent_allowed_for_review")),
        "intent_report_json": str(intent_result.report_json),
        "order_plan_verdict": plan_report.get("order_plan_verdict"),
        "order_plan_created": bool(plan_report.get("order_plan_created", False)),
        "order_plan_id": plan_report.get("order_plan_id"),
        "order_plan_report_json": str(order_plan.report_json) if order_plan is not None else None,
        "readiness_verdict": readiness_summary.get("final_readiness_verdict"),
        "readiness_report_json": readiness_summary.get("report_json_path"),
        "primary_blocker": primary_blocker,
        "secondary_blockers": list(intent_report.get("secondary_blockers") or ()) + list(plan_report.get("secondary_blockers") or ()),
        "required_next_action": required_next_action,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "paper_proof_cli_remains_only_submit_path": True,
        "strategy_intent_boundary_is_no_submit": True,
        "order_plan_boundary_is_no_submit": True,
        "local_execution_contract_key_is_execution_authority": True,
        "databento_symbol_is_execution_authority": False,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return ShadowEvaluationResult(verdict=verdict, report_json=report_json, report=report)


def _default_order_plan_factory(
    payload: Mapping[str, Any],
    config: OrderPlanConfig,
    run_id: str,
    now: datetime,
) -> OrderPlanResult:
    return create_order_plan_from_intent(payload=payload, config=config, run_id=run_id, now=now)


def _shadow_evaluation_id(intent_report: Mapping[str, Any], plan_report: Mapping[str, Any]) -> str:
    raw = "|".join(
        str(value or "")
        for value in (
            intent_report.get("strategy_id"),
            intent_report.get("lane_id"),
            intent_report.get("account_id"),
            intent_report.get("local_execution_contract_key"),
            intent_report.get("decision_timestamp"),
            plan_report.get("order_plan_id"),
        )
    )
    return "shadow_eval_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    return json.loads(Path(path).read_text(encoding="utf-8"))
