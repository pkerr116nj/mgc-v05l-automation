"""Guarded Track B strategy-managed PAPER lifecycle v1.

This module is the strategy lifecycle boundary for real Track B PAPER signals.
It is intentionally separate from paper-proof/canary flows: proof verifies
connectivity, while this lifecycle records strategy-owned entry/open/exit
state and refuses to mutate unless an explicit managed broker adapter and exit
policy are supplied.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_paper_trade_ledger import DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT


DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/track_b_strategy_managed_paper_lifecycle"
)


class TrackBManagedPaperLifecycleClassification(str, Enum):
    OPEN_MANAGED = "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
    EXIT_PENDING = "TRACK_B_STRATEGY_PAPER_EXIT_PENDING"
    CLOSED_FLAT = "TRACK_B_STRATEGY_PAPER_CLOSED_FLAT"
    REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_REVIEW_REQUIRED"
    BROKER_MISMATCH = "TRACK_B_STRATEGY_PAPER_BROKER_MISMATCH"
    EXIT_POLICY_MISSING = "TRACK_B_STRATEGY_PAPER_EXIT_POLICY_MISSING"
    LIFECYCLE_NOT_AVAILABLE = "TRACK_B_STRATEGY_PAPER_LIFECYCLE_NOT_AVAILABLE"
    BLOCKED_EXISTING_REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_BLOCKED_EXISTING_REVIEW_REQUIRED"


class TrackBManagedExitPolicy(str, Enum):
    EXIT_NOT_AVAILABLE = "EXIT_NOT_AVAILABLE"
    MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL = "MANAGED_HOLD_REQUIRES_EXTERNAL_EXIT_SIGNAL"
    DIAGNOSTIC_TIME_EXIT_IMMEDIATE = "DIAGNOSTIC_TIME_EXIT_IMMEDIATE"


@dataclass(frozen=True)
class TrackBStrategyManagedPaperLifecycleConfig:
    mode: str
    account_id: str
    expected_account_id: str
    strategy_id: str
    instrument_family: str
    contract_key: str
    local_symbol: str
    con_id: int | None
    side: str
    quantity: int | None
    signal_timestamp: str | None = None
    signal_reason: str | None = None
    decision_bar_timestamp: str | None = None
    latest_decision_bar_source: str | None = None
    pricing_policy: str | None = None
    reference_price_source: str | None = None
    reference_price: str | Decimal | None = None
    entry_limit_price: str | Decimal | None = None
    close_limit_price: str | Decimal | None = None
    managed_exit_policy_id: str | None = None
    source_id: str = "track_b_strategy_managed_paper_lifecycle"
    output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    live_position_status_json: Path | None = None
    live_money_readiness: bool = False
    broker_reconciled: bool = False


@dataclass(frozen=True)
class TrackBStrategyManagedPaperLifecycleStages:
    entry_submitter: Callable[[TrackBStrategyManagedPaperLifecycleConfig, Mapping[str, Any]], Mapping[str, Any]]
    exit_policy: Callable[
        [TrackBStrategyManagedPaperLifecycleConfig, Mapping[str, Any]],
        Mapping[str, Any] | None,
    ]
    close_submitter: Callable[[TrackBStrategyManagedPaperLifecycleConfig, Mapping[str, Any]], Mapping[str, Any]]


@dataclass(frozen=True)
class TrackBStrategyManagedPaperLifecycleResult:
    lifecycle_id: str
    classification: TrackBManagedPaperLifecycleClassification
    report_json: Path
    report: dict[str, Any]


def default_managed_lifecycle_stages() -> TrackBStrategyManagedPaperLifecycleStages:
    return TrackBStrategyManagedPaperLifecycleStages(
        entry_submitter=_default_entry_submitter,
        exit_policy=_default_exit_policy,
        close_submitter=_default_close_submitter,
    )


def run_track_b_strategy_managed_paper_lifecycle(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    stages: TrackBStrategyManagedPaperLifecycleStages | None = None,
    lifecycle_id: str | None = None,
    now: datetime | None = None,
) -> TrackBStrategyManagedPaperLifecycleResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_lifecycle_id = lifecycle_id or f"strategy_managed_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_lifecycle_id / "track_b_strategy_managed_paper_lifecycle_report.json"
    actual_stages = stages or default_managed_lifecycle_stages()

    entry_intent = _entry_intent(config=config, lifecycle_id=actual_lifecycle_id, now=actual_now)
    close_intent: Mapping[str, Any] | None = None
    entry_submit: Mapping[str, Any] | None = None
    close_submit: Mapping[str, Any] | None = None
    entry_fill: Mapping[str, Any] | None = None
    close_fill: Mapping[str, Any] | None = None
    broker_state_mutated = False
    submit_attempted = False
    primary_blocker: str | None = None
    required_next_action = "No broker mutation occurred."

    guard_blocker = _guard_blocker(config)
    existing_review = _existing_review_required_blocker(config)
    exit_policy_id = _normalized_exit_policy(config.managed_exit_policy_id)
    if guard_blocker:
        classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
        primary_blocker = guard_blocker
        required_next_action = "Resolve managed PAPER lifecycle guard before retrying."
    elif existing_review:
        classification = TrackBManagedPaperLifecycleClassification.BLOCKED_EXISTING_REVIEW_REQUIRED
        primary_blocker = existing_review
        required_next_action = "Resolve the existing review-required PAPER position before new managed entries."
    elif exit_policy_id in {"", TrackBManagedExitPolicy.EXIT_NOT_AVAILABLE.value}:
        classification = TrackBManagedPaperLifecycleClassification.EXIT_POLICY_MISSING
        primary_blocker = f"{config.strategy_id} has no managed PAPER exit policy."
        required_next_action = "Add an explicit managed exit policy before allowing strategy-managed PAPER entry."
    else:
        try:
            entry_submit = dict(actual_stages.entry_submitter(config, entry_intent))
            submit_attempted = bool(entry_submit.get("submitted") or entry_submit.get("submit_attempted"))
            broker_state_mutated = bool(entry_submit.get("broker_state_mutated"))
            entry_fill = _mapping(entry_submit.get("entry_fill") or entry_submit.get("fill"))
            if not submit_attempted:
                classification = TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE
                primary_blocker = str(entry_submit.get("primary_blocker") or "Managed broker submit adapter is not configured.")
                required_next_action = "Configure the guarded managed PAPER lifecycle adapter before retrying."
            elif not entry_fill:
                classification = TrackBManagedPaperLifecycleClassification.EXIT_PENDING
                primary_blocker = str(entry_submit.get("primary_blocker") or "Managed entry submit has no fill yet.")
                required_next_action = "Monitor managed entry order state; do not submit another entry for this lifecycle."
            else:
                open_state = _open_state(config=config, lifecycle_id=actual_lifecycle_id, entry_intent=entry_intent, entry_fill=entry_fill)
                close_intent = actual_stages.exit_policy(config, open_state)
                if close_intent is None:
                    classification = TrackBManagedPaperLifecycleClassification.OPEN_MANAGED
                    required_next_action = "Position is open under strategy-managed PAPER state; wait for strategy exit policy."
                else:
                    close_submit = dict(actual_stages.close_submitter(config, close_intent))
                    submit_attempted = submit_attempted or bool(close_submit.get("submitted") or close_submit.get("submit_attempted"))
                    broker_state_mutated = broker_state_mutated or bool(close_submit.get("broker_state_mutated"))
                    close_fill = _mapping(close_submit.get("close_fill") or close_submit.get("fill"))
                    if close_fill:
                        classification = TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
                        required_next_action = "Managed PAPER lifecycle closed flat."
                    else:
                        classification = TrackBManagedPaperLifecycleClassification.EXIT_PENDING
                        primary_blocker = str(close_submit.get("primary_blocker") or "Managed close submit has no fill yet.")
                        required_next_action = "Monitor managed close order state and reconcile before another handoff."
        except Exception as exc:  # noqa: BLE001 - lifecycle errors must become artifacts.
            classification = TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED
            primary_blocker = f"Managed PAPER lifecycle stage error: {exc}"
            required_next_action = "Review managed lifecycle diagnostics before retrying."

    report = _build_report(
        config=config,
        lifecycle_id=actual_lifecycle_id,
        now=actual_now,
        classification=classification,
        entry_intent=entry_intent,
        entry_submit=entry_submit,
        entry_fill=entry_fill,
        close_intent=close_intent,
        close_submit=close_submit,
        close_fill=close_fill,
        submit_attempted=submit_attempted,
        broker_state_mutated=broker_state_mutated,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        report_json=report_json,
    )
    _write_report(report_json, report)
    return TrackBStrategyManagedPaperLifecycleResult(
        lifecycle_id=actual_lifecycle_id,
        classification=classification,
        report_json=report_json,
        report=report,
    )


def _entry_intent(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    lifecycle_id: str,
    now: datetime,
) -> dict[str, Any]:
    action = _order_action(config.side)
    return {
        "intent_schema_version": "track_b_strategy_managed_paper_entry_intent_v1",
        "created_at": now.isoformat(),
        "lifecycle_id": lifecycle_id,
        "trade_id": f"{config.strategy_id}:{lifecycle_id}",
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "side": _normalized_side(config.side),
        "order_action": action,
        "quantity": config.quantity,
        "signal_timestamp": config.signal_timestamp,
        "signal_reason": config.signal_reason,
        "decision_bar_timestamp": config.decision_bar_timestamp,
        "latest_decision_bar_source": config.latest_decision_bar_source,
        "pricing_policy": config.pricing_policy,
        "reference_price_source": config.reference_price_source,
        "reference_price": _decimal_text(config.reference_price),
        "entry_limit_price": _decimal_text(config.entry_limit_price),
        "managed_exit_policy_id": _normalized_exit_policy(config.managed_exit_policy_id),
        "source_id": config.source_id,
    }


def _open_state(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    lifecycle_id: str,
    entry_intent: Mapping[str, Any],
    entry_fill: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "state_schema_version": "track_b_strategy_managed_paper_open_state_v1",
        "lifecycle_id": lifecycle_id,
        "trade_id": entry_intent.get("trade_id"),
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "side": entry_intent.get("side"),
        "quantity": config.quantity,
        "entry_price": _decimal_text(entry_fill.get("price") or entry_fill.get("avg_price")),
        "entry_timestamp": entry_fill.get("filled_at") or entry_fill.get("timestamp"),
        "current_state": TrackBManagedPaperLifecycleClassification.OPEN_MANAGED.value,
        "broker_reconciled": bool(config.broker_reconciled),
        "review_required": False,
    }


def _build_report(
    *,
    config: TrackBStrategyManagedPaperLifecycleConfig,
    lifecycle_id: str,
    now: datetime,
    classification: TrackBManagedPaperLifecycleClassification,
    entry_intent: Mapping[str, Any],
    entry_submit: Mapping[str, Any] | None,
    entry_fill: Mapping[str, Any] | None,
    close_intent: Mapping[str, Any] | None,
    close_submit: Mapping[str, Any] | None,
    close_fill: Mapping[str, Any] | None,
    submit_attempted: bool,
    broker_state_mutated: bool,
    primary_blocker: str | None,
    required_next_action: str,
    report_json: Path,
) -> dict[str, Any]:
    realized = _realized_pnl(entry_intent.get("side"), config.quantity, entry_fill, close_fill)
    review_required = classification in {
        TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED,
        TrackBManagedPaperLifecycleClassification.BROKER_MISMATCH,
        TrackBManagedPaperLifecycleClassification.BLOCKED_EXISTING_REVIEW_REQUIRED,
    }
    return {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "generated_at": now.isoformat(),
        "lifecycle_id": lifecycle_id,
        "trade_id": entry_intent.get("trade_id"),
        "strategy_id": config.strategy_id,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "mode": config.mode,
        "managed_exit_policy_id": _normalized_exit_policy(config.managed_exit_policy_id),
        "strategy_managed_lifecycle_classification": classification.value,
        "paper_lifecycle_classification": classification.value,
        "entry_intent": dict(entry_intent),
        "entry_submit_attempt": dict(entry_submit) if entry_submit else None,
        "entry_fill": dict(entry_fill) if entry_fill else None,
        "close_intent": dict(close_intent) if close_intent else None,
        "close_submit_attempt": dict(close_submit) if close_submit else None,
        "close_fill": dict(close_fill) if close_fill else None,
        "realized_pnl": _decimal_text(realized),
        "pnl_currency": "USD",
        "final_position_status": _final_position_status(classification),
        "final_broker_state_classification": classification.value,
        "review_required": review_required,
        "broker_reconciled": bool(config.broker_reconciled),
        "source": "TRACK_B_STRATEGY_MANAGED_LIFECYCLE",
        "submit_allowed": bool(submit_attempted and not review_required),
        "submit_attempted": bool(submit_attempted),
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "broker_state_mutated": bool(broker_state_mutated),
        "live_money_readiness": False,
        "ui_authority": False,
        "hidden_submit": False,
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_strategy_managed_paper_lifecycle_report.json"),
    }


def _guard_blocker(config: TrackBStrategyManagedPaperLifecycleConfig) -> str | None:
    if str(config.mode).upper() != "PAPER":
        return "Strategy-managed PAPER lifecycle requires mode=PAPER."
    if config.live_money_readiness is True:
        return "Strategy-managed PAPER lifecycle requires live_money_readiness=false."
    if config.account_id != config.expected_account_id:
        return f"Account guard failed: {config.account_id} != expected {config.expected_account_id}."
    if str(config.latest_decision_bar_source or "") != "DATABENTO_LIVE_ARTIFACT":
        return "Strategy-managed PAPER lifecycle requires latest decision bar source DATABENTO_LIVE_ARTIFACT."
    if config.quantity is None or config.quantity <= 0:
        return "Strategy-managed PAPER lifecycle requires positive configured quantity."
    if not config.local_symbol:
        return "Strategy-managed PAPER lifecycle requires an allowlisted local symbol."
    return None


def _existing_review_required_blocker(config: TrackBStrategyManagedPaperLifecycleConfig) -> str | None:
    status_path = Path(config.live_position_status_json) if config.live_position_status_json else Path(config.paper_trade_ledger_output_root) / "latest_track_b_live_position_status.json"
    try:
        value = json.loads(status_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, Mapping):
        return None
    if int(value.get("review_required_count") or 0) > 0:
        return f"Existing review-required PAPER lifecycle in {status_path} blocks new managed entries."
    positions = value.get("positions")
    if isinstance(positions, list):
        for item in positions:
            if isinstance(item, Mapping) and item.get("review_required") is True:
                return f"Existing review-required PAPER position in {status_path} blocks new managed entries."
    return None


def _default_entry_submitter(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    entry_intent: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "submitted": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "entry_intent": dict(entry_intent),
        "primary_blocker": "No managed PAPER broker submit adapter is configured.",
    }


def _default_exit_policy(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    open_state: Mapping[str, Any],
) -> Mapping[str, Any] | None:
    if _normalized_exit_policy(config.managed_exit_policy_id) == TrackBManagedExitPolicy.DIAGNOSTIC_TIME_EXIT_IMMEDIATE.value:
        return {
            "intent_schema_version": "track_b_strategy_managed_paper_close_intent_v1",
            "lifecycle_id": open_state.get("lifecycle_id"),
            "trade_id": open_state.get("trade_id"),
            "strategy_id": config.strategy_id,
            "account_id": config.account_id,
            "contract_key": config.contract_key,
            "local_symbol": config.local_symbol,
            "con_id": config.con_id,
            "side": open_state.get("side"),
            "order_action": "SELL" if open_state.get("side") == "LONG" else "BUY",
            "quantity": config.quantity,
            "close_limit_price": _decimal_text(config.close_limit_price),
            "close_reason": "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
        }
    return None


def _default_close_submitter(
    config: TrackBStrategyManagedPaperLifecycleConfig,
    close_intent: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "submitted": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "close_intent": dict(close_intent),
        "primary_blocker": "No managed PAPER close adapter is configured.",
    }


def _write_report(report_json: Path, report: Mapping[str, Any]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(dict(report)), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest = Path(str(report["latest_report_json_path"]))
    latest.parent.mkdir(parents=True, exist_ok=True)
    latest.write_text(payload, encoding="utf-8")


def _normalized_exit_policy(value: str | None) -> str:
    return str(value or TrackBManagedExitPolicy.EXIT_NOT_AVAILABLE.value).strip().upper()


def _normalized_side(value: str | None) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"BUY", "LONG"}:
        return "LONG"
    if raw in {"SELL", "SHORT"}:
        return "SHORT"
    return raw or "UNKNOWN"


def _order_action(side: str | None) -> str:
    return "SELL" if _normalized_side(side) == "SHORT" else "BUY"


def _mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _decimal_text(value: object) -> str | None:
    parsed = _decimal(value)
    return None if parsed is None else format(parsed.normalize(), "f")


def _realized_pnl(
    side: object,
    quantity: int | None,
    entry_fill: Mapping[str, Any] | None,
    close_fill: Mapping[str, Any] | None,
) -> Decimal | None:
    if not entry_fill or not close_fill or quantity is None:
        return None
    entry = _decimal(entry_fill.get("price") or entry_fill.get("avg_price"))
    close = _decimal(close_fill.get("price") or close_fill.get("avg_price"))
    if entry is None or close is None:
        return None
    direction = Decimal("1") if _normalized_side(str(side)) == "LONG" else Decimal("-1")
    return (close - entry) * Decimal(quantity) * direction


def _final_position_status(classification: TrackBManagedPaperLifecycleClassification) -> str:
    if classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT:
        return "CLOSED_FLAT"
    if classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED:
        return "OPEN_MANAGED"
    if classification == TrackBManagedPaperLifecycleClassification.EXIT_PENDING:
        return "EXIT_PENDING"
    return "REVIEW_REQUIRED" if "REVIEW" in classification.value or "MISMATCH" in classification.value else "NOT_OPENED"
