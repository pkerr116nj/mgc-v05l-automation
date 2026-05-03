"""Track B no-submit order plan boundary derived from strategy intents."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable
from .strategy_intent import (
    StrategyIntentValidationConfig,
    StrategyIntentValidationVerdict,
    validate_strategy_intent,
)


DEFAULT_ORDER_PLAN_OUTPUT_ROOT = Path("outputs/track_b_execution_core/order_plans")
REQUIRED_GATES_BEFORE_SUBMIT = (
    "strategy_intent_valid_for_paper_review",
    "recovery_status_ready_clean",
    "read_only_preflight_ready",
    "proof_timing_active_session",
    "readiness_summary_ready_for_paper_proof",
    "paper_proof_cli_explicit_submit_enabled",
    "paper_proof_cli_confirm_paper_submit",
)


class OrderPlanVerdict(str, Enum):
    CREATED_FOR_PAPER_REVIEW = "ORDER_PLAN_CREATED_FOR_PAPER_REVIEW"
    BLOCKED_INVALID_INTENT = "ORDER_PLAN_BLOCKED_INVALID_INTENT"
    BLOCKED_LIVE_MODE = "ORDER_PLAN_BLOCKED_LIVE_MODE"
    BLOCKED_UNSUPPORTED_ORDER_TYPE = "ORDER_PLAN_BLOCKED_UNSUPPORTED_ORDER_TYPE"
    BLOCKED_UNSUPPORTED_TIF = "ORDER_PLAN_BLOCKED_UNSUPPORTED_TIF"
    BLOCKED_INVALID_QUANTITY = "ORDER_PLAN_BLOCKED_INVALID_QUANTITY"
    BLOCKED_MISSING_LIMIT_PRICE = "ORDER_PLAN_BLOCKED_MISSING_LIMIT_PRICE"
    BLOCKED_ACCOUNT_MISMATCH = "ORDER_PLAN_BLOCKED_ACCOUNT_MISMATCH"
    BLOCKED_CONTRACT_NOT_ALLOWLISTED = "ORDER_PLAN_BLOCKED_CONTRACT_NOT_ALLOWLISTED"


@dataclass(frozen=True)
class OrderPlanConfig:
    expected_account_id: str | None = None
    contract_allowlist: Mapping[str, Mapping[str, Any]] | None = None
    output_root: Path = DEFAULT_ORDER_PLAN_OUTPUT_ROOT


@dataclass(frozen=True)
class OrderPlanResult:
    verdict: OrderPlanVerdict
    report_json: Path
    report: dict[str, Any]


def create_order_plan_from_intent(
    *,
    payload: Mapping[str, Any],
    config: OrderPlanConfig | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> OrderPlanResult:
    actual_config = config or OrderPlanConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"order_plan_{uuid.uuid4().hex}"
    report_json = Path(actual_config.output_root) / actual_run_id / "order_plan_report.json"
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
    verdict = _order_plan_verdict(intent_result.verdict)
    created = verdict == OrderPlanVerdict.CREATED_FOR_PAPER_REVIEW
    intent_report = intent_result.report
    plan_id = _order_plan_id(intent_report) if created else None
    report = {
        "schema_version": "track_b_order_plan_v1",
        "generated_at": actual_now.isoformat(),
        "order_plan_verdict": verdict.value,
        "order_plan_created": created,
        "order_plan_id": plan_id,
        "source_intent_id": intent_report.get("source_intent_id"),
        "source_intent_validation_verdict": intent_result.verdict.value,
        "source_intent_report_json": str(intent_result.report_json),
        "submit_allowed": False,
        "submit_attempted": False,
        "primary_blocker": None if created else intent_report.get("primary_blocker") or "Strategy intent is not valid for order planning.",
        "secondary_blockers": list(intent_report.get("secondary_blockers") or ()),
        "required_next_action": (
            "Review the order plan, then run recovery/preflight/timing/readiness gates before any paper_proof_cli submit."
            if created
            else intent_report.get("required_next_action") or "Fix intent before creating an order plan."
        ),
        "strategy_id": intent_report.get("strategy_id"),
        "lane_id": intent_report.get("lane_id"),
        "mode": intent_report.get("mode"),
        "account_id": intent_report.get("account_id"),
        "local_execution_contract_key": intent_report.get("local_execution_contract_key"),
        "action": intent_report.get("side"),
        "side": intent_report.get("side"),
        "quantity": intent_report.get("quantity"),
        "order_type": intent_report.get("order_type"),
        "limit_price": intent_report.get("limit_price"),
        "time_in_force": intent_report.get("time_in_force"),
        "signal_timestamp": intent_report.get("signal_timestamp"),
        "decision_timestamp": intent_report.get("decision_timestamp"),
        "required_gates_before_submit": list(REQUIRED_GATES_BEFORE_SUBMIT),
        "live_money_readiness": False,
        "paper_proof_cli_remains_only_submit_path": True,
        "strategy_intent_boundary_is_no_submit": True,
        "local_execution_contract_key_is_execution_authority": True,
        "databento_symbol_is_execution_authority": False,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return OrderPlanResult(verdict=verdict, report_json=report_json, report=report)


def _order_plan_verdict(verdict: StrategyIntentValidationVerdict) -> OrderPlanVerdict:
    mapping = {
        StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW: OrderPlanVerdict.CREATED_FOR_PAPER_REVIEW,
        StrategyIntentValidationVerdict.BLOCKED_LIVE_MODE: OrderPlanVerdict.BLOCKED_LIVE_MODE,
        StrategyIntentValidationVerdict.BLOCKED_UNSUPPORTED_ORDER_TYPE: OrderPlanVerdict.BLOCKED_UNSUPPORTED_ORDER_TYPE,
        StrategyIntentValidationVerdict.BLOCKED_UNSUPPORTED_TIF: OrderPlanVerdict.BLOCKED_UNSUPPORTED_TIF,
        StrategyIntentValidationVerdict.BLOCKED_INVALID_QUANTITY: OrderPlanVerdict.BLOCKED_INVALID_QUANTITY,
        StrategyIntentValidationVerdict.BLOCKED_MISSING_LIMIT_PRICE: OrderPlanVerdict.BLOCKED_MISSING_LIMIT_PRICE,
        StrategyIntentValidationVerdict.BLOCKED_ACCOUNT_MISMATCH: OrderPlanVerdict.BLOCKED_ACCOUNT_MISMATCH,
        StrategyIntentValidationVerdict.BLOCKED_CONTRACT_NOT_ALLOWLISTED: OrderPlanVerdict.BLOCKED_CONTRACT_NOT_ALLOWLISTED,
    }
    return mapping.get(verdict, OrderPlanVerdict.BLOCKED_INVALID_INTENT)


def _order_plan_id(intent_report: Mapping[str, Any]) -> str:
    raw = "|".join(
        str(intent_report.get(key) or "")
        for key in (
            "strategy_id",
            "lane_id",
            "account_id",
            "local_execution_contract_key",
            "side",
            "quantity",
            "order_type",
            "limit_price",
            "time_in_force",
            "decision_timestamp",
        )
    )
    return "order_plan_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]
