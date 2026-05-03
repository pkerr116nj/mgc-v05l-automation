"""Track B-native no-submit strategy intent boundary."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import Action, TrackBModelError, require_aware_datetime, require_id, to_jsonable
from .preflight import ReadOnlyPreflightConfig


DEFAULT_STRATEGY_INTENT_OUTPUT_ROOT = Path("outputs/track_b_execution_core/strategy_intents")


class StrategyIntentValidationVerdict(str, Enum):
    VALID_FOR_PAPER_REVIEW = "INTENT_VALID_FOR_PAPER_REVIEW"
    BLOCKED_LIVE_MODE = "INTENT_BLOCKED_LIVE_MODE"
    BLOCKED_UNSUPPORTED_ORDER_TYPE = "INTENT_BLOCKED_UNSUPPORTED_ORDER_TYPE"
    BLOCKED_UNSUPPORTED_TIF = "INTENT_BLOCKED_UNSUPPORTED_TIF"
    BLOCKED_INVALID_QUANTITY = "INTENT_BLOCKED_INVALID_QUANTITY"
    BLOCKED_MISSING_LIMIT_PRICE = "INTENT_BLOCKED_MISSING_LIMIT_PRICE"
    BLOCKED_CONTRACT_NOT_ALLOWLISTED = "INTENT_BLOCKED_CONTRACT_NOT_ALLOWLISTED"
    BLOCKED_ACCOUNT_MISMATCH = "INTENT_BLOCKED_ACCOUNT_MISMATCH"
    BLOCKED_SCHEMA_ERROR = "INTENT_BLOCKED_SCHEMA_ERROR"
    BLOCKED_SUBMIT_REQUESTED = "INTENT_BLOCKED_SUBMIT_REQUESTED"


@dataclass(frozen=True)
class StrategyTradeIntent:
    strategy_id: str
    lane_id: str
    mode: str
    account_id: str
    instrument_family: str
    local_execution_contract_key: str
    side: str
    quantity: Any
    order_type: str
    limit_price: Any
    time_in_force: str
    signal_timestamp: datetime
    decision_timestamp: datetime
    reason: str
    source: str
    submit_requested: bool = False
    live_money_readiness: bool = False
    databento_symbol: str | None = None
    databento_continuous_symbol: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "StrategyTradeIntent":
        try:
            return cls(
                strategy_id=require_id(str(payload.get("strategy_id") or ""), "strategy_id"),
                lane_id=require_id(str(payload.get("lane_id") or ""), "lane_id"),
                mode=require_id(str(payload.get("mode") or ""), "mode"),
                account_id=require_id(str(payload.get("account_id") or ""), "account_id"),
                instrument_family=require_id(str(payload.get("instrument_family") or payload.get("symbol") or ""), "instrument_family"),
                local_execution_contract_key=require_id(
                    str(payload.get("local_execution_contract_key") or payload.get("contract_key") or ""),
                    "local_execution_contract_key",
                ),
                side=Action(str(payload.get("side") or "").strip().upper()).value,
                quantity=payload.get("quantity"),
                order_type=require_id(str(payload.get("order_type") or ""), "order_type"),
                limit_price=payload.get("limit_price"),
                time_in_force=require_id(str(payload.get("time_in_force") or ""), "time_in_force"),
                signal_timestamp=_parse_timestamp(payload.get("signal_timestamp"), "signal_timestamp"),
                decision_timestamp=_parse_timestamp(payload.get("decision_timestamp"), "decision_timestamp"),
                reason=require_id(str(payload.get("reason") or ""), "reason"),
                source=require_id(str(payload.get("source") or ""), "source"),
                submit_requested=bool(payload.get("submit_requested", False)),
                live_money_readiness=bool(payload.get("live_money_readiness", False)),
                databento_symbol=_optional_str(payload.get("databento_symbol")),
                databento_continuous_symbol=_optional_str(payload.get("databento_continuous_symbol")),
            )
        except (TrackBModelError, ValueError, TypeError) as exc:
            raise StrategyIntentSchemaError(str(exc)) from exc

    def to_report_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "lane_id": self.lane_id,
            "mode": self.mode,
            "account_id": self.account_id,
            "instrument_family": self.instrument_family,
            "local_execution_contract_key": self.local_execution_contract_key,
            "side": self.side,
            "quantity": self.quantity,
            "order_type": self.order_type,
            "limit_price": self.limit_price,
            "time_in_force": self.time_in_force,
            "signal_timestamp": self.signal_timestamp.isoformat(),
            "decision_timestamp": self.decision_timestamp.isoformat(),
            "reason": self.reason,
            "source": self.source,
            "submit_requested": self.submit_requested,
            "live_money_readiness": False,
            "databento_symbol": self.databento_symbol,
            "databento_continuous_symbol": self.databento_continuous_symbol,
            "databento_symbol_is_execution_authority": False,
            "local_execution_contract_key_is_execution_authority": True,
        }


class StrategyIntentSchemaError(ValueError):
    """Raised when an intent payload cannot be interpreted as Track B intent."""


@dataclass(frozen=True)
class StrategyIntentValidationResult:
    verdict: StrategyIntentValidationVerdict
    report_json: Path
    report: dict[str, Any]
    intent: StrategyTradeIntent | None


@dataclass(frozen=True)
class StrategyIntentValidationConfig:
    expected_account_id: str | None = None
    contract_allowlist: Mapping[str, Mapping[str, Any]] | None = None
    output_root: Path = DEFAULT_STRATEGY_INTENT_OUTPUT_ROOT


def validate_strategy_intent(
    *,
    payload: Mapping[str, Any],
    config: StrategyIntentValidationConfig | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> StrategyIntentValidationResult:
    actual_config = config or StrategyIntentValidationConfig()
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_run_id = run_id or f"strategy_intent_{uuid.uuid4().hex}"
    report_json = Path(actual_config.output_root) / actual_run_id / "strategy_intent_report.json"
    try:
        intent = StrategyTradeIntent.from_mapping(payload)
    except StrategyIntentSchemaError as exc:
        return _write_report(
            report_json=report_json,
            verdict=StrategyIntentValidationVerdict.BLOCKED_SCHEMA_ERROR,
            intent=None,
            primary_blocker=str(exc),
            required_next_action="Fix strategy intent schema before Track B review.",
            now=actual_now,
        )

    verdict, primary_blocker, required_action, secondary = _classify_intent(intent, actual_config)
    return _write_report(
        report_json=report_json,
        verdict=verdict,
        intent=intent,
        primary_blocker=primary_blocker,
        required_next_action=required_action,
        secondary_blockers=secondary,
        now=actual_now,
    )


def _classify_intent(
    intent: StrategyTradeIntent,
    config: StrategyIntentValidationConfig,
) -> tuple[StrategyIntentValidationVerdict, str | None, str, tuple[str, ...]]:
    secondary: list[str] = []
    if str(intent.mode).upper() != "PAPER":
        return (
            StrategyIntentValidationVerdict.BLOCKED_LIVE_MODE,
            "Track B strategy intent v1 accepts PAPER mode only.",
            "Use PAPER mode. Live-money strategy intents are not supported in Track B v1.",
            tuple(secondary),
        )
    if intent.submit_requested:
        return (
            StrategyIntentValidationVerdict.BLOCKED_SUBMIT_REQUESTED,
            "Strategy intent boundary is no-submit; submit_requested must be false.",
            "Clear submit_requested and pass through recovery/preflight/timing/proof gates separately.",
            tuple(secondary),
        )
    if str(intent.order_type).upper() != "LMT":
        return (
            StrategyIntentValidationVerdict.BLOCKED_UNSUPPORTED_ORDER_TYPE,
            "Only LMT order_type is supported for Track B paper review.",
            "Emit an LMT DAY intent with an explicit limit_price.",
            tuple(secondary),
        )
    if str(intent.time_in_force).upper() != "DAY":
        return (
            StrategyIntentValidationVerdict.BLOCKED_UNSUPPORTED_TIF,
            "Only DAY time_in_force is supported for Track B paper review.",
            "Emit an LMT DAY intent.",
            tuple(secondary),
        )
    quantity_error = _quantity_error(intent.quantity)
    if quantity_error is not None:
        return (
            StrategyIntentValidationVerdict.BLOCKED_INVALID_QUANTITY,
            quantity_error,
            "Emit a positive integer quantity. Milestone proof gates may further restrict quantity to 1.",
            tuple(secondary),
        )
    if _limit_price_error(intent.limit_price) is not None:
        return (
            StrategyIntentValidationVerdict.BLOCKED_MISSING_LIMIT_PRICE,
            "LMT strategy intent requires a positive limit_price.",
            "Provide an explicit positive limit_price or block before Track B review.",
            tuple(secondary),
        )
    allowlist = config.contract_allowlist or ReadOnlyPreflightConfig().contract_allowlist
    if intent.local_execution_contract_key not in allowlist:
        return (
            StrategyIntentValidationVerdict.BLOCKED_CONTRACT_NOT_ALLOWLISTED,
            "local_execution_contract_key is not explicitly allowlisted.",
            "Use an exact allowlisted Track B execution contract key.",
            tuple(secondary),
        )
    if config.expected_account_id and intent.account_id != config.expected_account_id:
        return (
            StrategyIntentValidationVerdict.BLOCKED_ACCOUNT_MISMATCH,
            "intent account_id does not match expected Track B paper account.",
            "Use the configured explicit paper account before review.",
            tuple(secondary),
        )
    if intent.live_money_readiness:
        secondary.append("Intent claimed live_money_readiness; Track B v1 report forces this to false.")
    return (
        StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW,
        None,
        "Intent is structurally valid for PAPER review. Recovery, preflight, timing, pricing, and proof CLI gates still apply.",
        tuple(secondary),
    )


def _write_report(
    *,
    report_json: Path,
    verdict: StrategyIntentValidationVerdict,
    intent: StrategyTradeIntent | None,
    primary_blocker: str | None,
    required_next_action: str,
    now: datetime,
    secondary_blockers: tuple[str, ...] = (),
) -> StrategyIntentValidationResult:
    intent_report = intent.to_report_dict() if intent is not None else {}
    allowed = verdict == StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW
    report = {
        "schema_version": "track_b_strategy_intent_validation_v1",
        "generated_at": now.isoformat(),
        "intent_validation_verdict": verdict.value,
        "intent_allowed_for_review": allowed,
        "submit_allowed": False,
        "submit_attempted": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": list(secondary_blockers),
        "required_next_action": required_next_action,
        "live_money_readiness": False,
        **intent_report,
        "report_json_path": str(report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
    return StrategyIntentValidationResult(verdict=verdict, report_json=report_json, report=report, intent=intent)


def _parse_timestamp(value: Any, field_name: str) -> datetime:
    if value is None:
        raise StrategyIntentSchemaError(f"{field_name} is required")
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return require_aware_datetime(parsed, field_name)


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _quantity_error(value: Any) -> str | None:
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return "quantity must be a positive integer"
    if quantity <= 0 or quantity != quantity.to_integral_value():
        return "quantity must be a positive integer"
    return None


def _limit_price_error(value: Any) -> str | None:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return "limit_price must be a positive decimal"
    if price <= 0:
        return "limit_price must be a positive decimal"
    return None
