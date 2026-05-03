"""Track B-native strategy lane registry validation.

This boundary is intentionally no-submit. It replaces implicit legacy runtime
assumptions with explicit Track B strategy/lane governance.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import TrackBModelError, require_aware_datetime, require_id, to_jsonable
from .order_plan import REQUIRED_GATES_BEFORE_SUBMIT
from .strategy_intent import StrategyIntentSchemaError, StrategyTradeIntent


DEFAULT_LANE_REGISTRY_OUTPUT_ROOT = Path("outputs/track_b_execution_core/lane_registry")


class LaneStatus(str, Enum):
    DISABLED = "DISABLED"
    SHADOW_ONLY = "SHADOW_ONLY"
    PAPER_REVIEW = "PAPER_REVIEW"
    PAPER_SUBMIT_ELIGIBLE = "PAPER_SUBMIT_ELIGIBLE"


class LaneValidationVerdict(str, Enum):
    AUTHORIZED_FOR_SHADOW_REVIEW = "LANE_AUTHORIZED_FOR_SHADOW_REVIEW"
    AUTHORIZED_FOR_PAPER_REVIEW = "LANE_AUTHORIZED_FOR_PAPER_REVIEW"
    BLOCKED_DISABLED = "LANE_BLOCKED_DISABLED"
    BLOCKED_UNKNOWN_STRATEGY = "LANE_BLOCKED_UNKNOWN_STRATEGY"
    BLOCKED_UNKNOWN_LANE = "LANE_BLOCKED_UNKNOWN_LANE"
    BLOCKED_ACCOUNT_MISMATCH = "LANE_BLOCKED_ACCOUNT_MISMATCH"
    BLOCKED_CONTRACT_NOT_ALLOWED = "LANE_BLOCKED_CONTRACT_NOT_ALLOWED"
    BLOCKED_SIDE_NOT_ALLOWED = "LANE_BLOCKED_SIDE_NOT_ALLOWED"
    BLOCKED_ORDER_TYPE_NOT_ALLOWED = "LANE_BLOCKED_ORDER_TYPE_NOT_ALLOWED"
    BLOCKED_TIF_NOT_ALLOWED = "LANE_BLOCKED_TIF_NOT_ALLOWED"
    BLOCKED_QUANTITY_EXCEEDS_MAX = "LANE_BLOCKED_QUANTITY_EXCEEDS_MAX"
    BLOCKED_LIVE_MODE = "LANE_BLOCKED_LIVE_MODE"
    BLOCKED_SCHEMA_ERROR = "LANE_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class StrategyLaneDefinition:
    strategy_id: str
    lane_id: str
    lane_status: str
    mode_allowed: str
    expected_account_id: str
    allowed_local_execution_contract_keys: tuple[str, ...]
    allowed_instrument_family: str | None
    allowed_sides: tuple[str, ...]
    allowed_order_types: tuple[str, ...]
    allowed_time_in_force: tuple[str, ...]
    max_quantity: Decimal
    registry_version: str
    generated_at: datetime
    live_money_readiness: bool = False

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any], *, registry_version: str, generated_at: datetime) -> "StrategyLaneDefinition":
        return cls(
            strategy_id=require_id(str(payload.get("strategy_id") or ""), "strategy_id"),
            lane_id=require_id(str(payload.get("lane_id") or ""), "lane_id"),
            lane_status=_enum_value(LaneStatus, payload.get("lane_status"), "lane_status"),
            mode_allowed=str(payload.get("mode_allowed") or "PAPER").strip().upper(),
            expected_account_id=require_id(str(payload.get("expected_account_id") or ""), "expected_account_id"),
            allowed_local_execution_contract_keys=_required_tuple(payload.get("allowed_local_execution_contract_keys"), "allowed_local_execution_contract_keys"),
            allowed_instrument_family=_optional_str(payload.get("allowed_instrument_family") or payload.get("allowed_symbol")),
            allowed_sides=_upper_tuple(payload.get("allowed_sides"), "allowed_sides"),
            allowed_order_types=_upper_tuple(payload.get("allowed_order_types"), "allowed_order_types"),
            allowed_time_in_force=_upper_tuple(payload.get("allowed_time_in_force"), "allowed_time_in_force"),
            max_quantity=_positive_decimal(payload.get("max_quantity"), "max_quantity"),
            registry_version=registry_version,
            generated_at=generated_at,
            live_money_readiness=False,
        )


@dataclass(frozen=True)
class LaneRegistry:
    registry_version: str
    generated_at: datetime
    lanes: tuple[StrategyLaneDefinition, ...]

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "LaneRegistry":
        generated_at = _parse_timestamp(payload.get("generated_at")) if payload.get("generated_at") else datetime.now(UTC)
        require_aware_datetime(generated_at, "generated_at")
        registry_version = require_id(str(payload.get("registry_version") or ""), "registry_version")
        lanes = tuple(
            StrategyLaneDefinition.from_mapping(row, registry_version=registry_version, generated_at=generated_at)
            for row in payload.get("lanes", ())
            if isinstance(row, Mapping)
        )
        return cls(registry_version=registry_version, generated_at=generated_at, lanes=lanes)

    def strategy_ids(self) -> set[str]:
        return {lane.strategy_id for lane in self.lanes}

    def find_lane(self, *, strategy_id: str, lane_id: str) -> StrategyLaneDefinition | None:
        for lane in self.lanes:
            if lane.strategy_id == strategy_id and lane.lane_id == lane_id:
                return lane
        return None


@dataclass(frozen=True)
class LaneValidationResult:
    verdict: LaneValidationVerdict
    report_json: Path
    report: dict[str, Any]


def validate_strategy_lane(
    *,
    intent_payload: Mapping[str, Any],
    registry_payload: Mapping[str, Any],
    output_root: Path = DEFAULT_LANE_REGISTRY_OUTPUT_ROOT,
    run_id: str | None = None,
    now: datetime | None = None,
) -> LaneValidationResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"lane_validation_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_run_id / "lane_validation_report.json"
    try:
        intent = StrategyTradeIntent.from_mapping(intent_payload)
        registry = LaneRegistry.from_mapping(registry_payload)
        verdict, lane, blocker, action = _classify(intent=intent, registry=registry)
    except (StrategyIntentSchemaError, TrackBModelError, ValueError, TypeError) as exc:
        intent = None
        registry = None
        lane = None
        verdict = LaneValidationVerdict.BLOCKED_SCHEMA_ERROR
        blocker = str(exc)
        action = "Fix lane registry or intent schema before Track B review."
    report = _report(
        verdict=verdict,
        intent=intent,
        registry=registry,
        lane=lane,
        primary_blocker=blocker,
        required_next_action=action,
        generated_at=actual_now,
        report_json=report_json,
    )
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return LaneValidationResult(verdict=verdict, report_json=report_json, report=report)


def _classify(intent: StrategyTradeIntent, registry: LaneRegistry) -> tuple[LaneValidationVerdict, StrategyLaneDefinition | None, str | None, str]:
    if str(intent.mode).upper() != "PAPER":
        return (
            LaneValidationVerdict.BLOCKED_LIVE_MODE,
            None,
            "Strategy lane registry accepts PAPER mode only.",
            "Use PAPER mode; live submit behavior is not supported.",
        )
    if intent.strategy_id not in registry.strategy_ids():
        return (
            LaneValidationVerdict.BLOCKED_UNKNOWN_STRATEGY,
            None,
            "strategy_id is not present in the Track B lane registry.",
            "Add an explicit Track B lane definition before review.",
        )
    lane = registry.find_lane(strategy_id=intent.strategy_id, lane_id=intent.lane_id)
    if lane is None:
        return (
            LaneValidationVerdict.BLOCKED_UNKNOWN_LANE,
            None,
            "lane_id is not defined for the strategy in the Track B lane registry.",
            "Use an explicitly defined strategy/lane pair.",
        )
    if lane.lane_status == LaneStatus.DISABLED:
        return (
            LaneValidationVerdict.BLOCKED_DISABLED,
            lane,
            "Strategy lane is disabled.",
            "Enable the lane explicitly before shadow or paper review.",
        )
    if lane.mode_allowed != "PAPER":
        return (
            LaneValidationVerdict.BLOCKED_LIVE_MODE,
            lane,
            "Lane mode_allowed must be PAPER for Track B v1.",
            "Use a PAPER-only lane definition.",
        )
    if intent.account_id != lane.expected_account_id:
        return (
            LaneValidationVerdict.BLOCKED_ACCOUNT_MISMATCH,
            lane,
            "Intent account_id does not match lane expected_account_id.",
            "Use the explicit paper account configured for this lane.",
        )
    if intent.local_execution_contract_key not in lane.allowed_local_execution_contract_keys:
        return (
            LaneValidationVerdict.BLOCKED_CONTRACT_NOT_ALLOWED,
            lane,
            "Intent local execution contract is not allowed for this lane.",
            "Use a contract key explicitly allowed by the lane registry.",
        )
    if intent.side.upper() not in lane.allowed_sides:
        return (
            LaneValidationVerdict.BLOCKED_SIDE_NOT_ALLOWED,
            lane,
            "Intent side is not allowed for this lane.",
            "Use an allowed side or update lane governance explicitly.",
        )
    if intent.order_type.upper() not in lane.allowed_order_types:
        return (
            LaneValidationVerdict.BLOCKED_ORDER_TYPE_NOT_ALLOWED,
            lane,
            "Intent order_type is not allowed for this lane.",
            "Use an allowed LMT order type.",
        )
    if intent.time_in_force.upper() not in lane.allowed_time_in_force:
        return (
            LaneValidationVerdict.BLOCKED_TIF_NOT_ALLOWED,
            lane,
            "Intent time_in_force is not allowed for this lane.",
            "Use an allowed DAY time_in_force.",
        )
    if _positive_decimal(intent.quantity, "quantity") > lane.max_quantity:
        return (
            LaneValidationVerdict.BLOCKED_QUANTITY_EXCEEDS_MAX,
            lane,
            "Intent quantity exceeds lane max_quantity.",
            "Reduce quantity or update lane governance explicitly.",
        )
    if lane.lane_status == LaneStatus.SHADOW_ONLY:
        return (
            LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW,
            lane,
            None,
            "Lane is authorized for shadow review only. Existing readiness/proof gates still apply before any future paper submit.",
        )
    return (
        LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW,
        lane,
        None,
        "Lane is authorized for paper review. Existing recovery/preflight/timing/readiness/proof gates still apply before submit.",
    )


def _report(
    *,
    verdict: LaneValidationVerdict,
    intent: StrategyTradeIntent | None,
    registry: LaneRegistry | None,
    lane: StrategyLaneDefinition | None,
    primary_blocker: str | None,
    required_next_action: str,
    generated_at: datetime,
    report_json: Path,
) -> dict[str, Any]:
    allowed = verdict in {
        LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW,
        LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW,
    }
    intent_report = intent.to_report_dict() if intent is not None else {}
    return {
        "schema_version": "track_b_strategy_lane_registry_v1",
        "generated_at": generated_at.isoformat(),
        "lane_validation_verdict": verdict.value,
        "lane_authorized_for_review": allowed,
        "lane_status": lane.lane_status if lane is not None else None,
        "submit_allowed": False,
        "submit_attempted": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": [],
        "required_next_action": required_next_action,
        "strategy_id": intent_report.get("strategy_id"),
        "lane_id": intent_report.get("lane_id"),
        "account_id": intent_report.get("account_id"),
        "local_execution_contract_key": intent_report.get("local_execution_contract_key"),
        "side": intent_report.get("side"),
        "quantity": intent_report.get("quantity"),
        "order_type": intent_report.get("order_type"),
        "time_in_force": intent_report.get("time_in_force"),
        "max_quantity": str(lane.max_quantity) if lane is not None else None,
        "registry_version": registry.registry_version if registry is not None else None,
        "required_gates_before_submit": list(REQUIRED_GATES_BEFORE_SUBMIT),
        "live_money_readiness": False,
        "paper_proof_cli_remains_only_submit_path": True,
        "local_execution_contract_key_is_execution_authority": True,
        "databento_symbol_is_execution_authority": False,
        "report_json_path": str(report_json),
    }


def _required_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    items = tuple(str(item).strip() for item in _sequence(value) if str(item).strip())
    if not items:
        raise TrackBModelError(f"{field_name} is required.")
    return items


def _upper_tuple(value: Any, field_name: str) -> tuple[str, ...]:
    items = tuple(str(item).strip().upper() for item in _sequence(value) if str(item).strip())
    if not items:
        raise TrackBModelError(f"{field_name} is required.")
    return items


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (list, tuple)):
        return value
    return ()


def _positive_decimal(value: Any, field_name: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise TrackBModelError(f"{field_name} must be decimal-compatible.") from exc
    if parsed <= 0:
        raise TrackBModelError(f"{field_name} must be positive.")
    return parsed


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_timestamp(value: Any) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return require_aware_datetime(parsed, "generated_at")


def _enum_value(enum_type: type[Enum], value: Any, field_name: str) -> str:
    try:
        return enum_type(str(value or "").strip().upper()).value
    except ValueError as exc:
        raise TrackBModelError(f"{field_name} is invalid.") from exc
