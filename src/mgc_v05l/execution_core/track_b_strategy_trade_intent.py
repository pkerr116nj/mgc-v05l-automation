"""Durable Track B strategy signal-to-intent bridge.

This module is intentionally pre-lifecycle. It records whether a real,
arbitrated strategy signal became an auditable strategy-managed PAPER intent
before any guarded lifecycle stage can attempt broker mutation.
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
from .track_b_paper_trade_ledger import DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT


DEFAULT_TRACK_B_STRATEGY_TRADE_INTENT_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/strategy_trade_intents"
)


class TrackBStrategyTradeIntentClassification(str, Enum):
    CREATED = "STRATEGY_TRADE_INTENT_CREATED"
    BLOCKED_MISSING_EXIT_POLICY = "INTENT_BLOCKED_MISSING_EXIT_POLICY"
    BLOCKED_NOT_PAPER_ELIGIBLE = "INTENT_BLOCKED_NOT_PAPER_ELIGIBLE"
    BLOCKED_LIVE_MONEY_DISABLED = "INTENT_BLOCKED_LIVE_MONEY_DISABLED"
    BLOCKED_REVIEW_REQUIRED = "INTENT_BLOCKED_REVIEW_REQUIRED"
    BLOCKED_CONTRACT_GUARD = "INTENT_BLOCKED_CONTRACT_GUARD"
    BLOCKED_ACCOUNT_GUARD = "INTENT_BLOCKED_ACCOUNT_GUARD"
    BLOCKED_NOT_LIVE_DECISION_BAR = "INTENT_BLOCKED_NOT_LIVE_DECISION_BAR"
    BLOCKED_STRATEGY_NOT_REGISTERED = "INTENT_BLOCKED_STRATEGY_NOT_REGISTERED"
    BLOCKED_ARBITRATION = "INTENT_BLOCKED_ARBITRATION"
    NOT_CREATED_NO_SIGNAL = "INTENT_NOT_CREATED_NO_SIGNAL"


@dataclass(frozen=True)
class TrackBStrategyTradeIntentConfig:
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
    runtime_source: str | None = "DATABENTO_LIVE_ARTIFACT"
    latest_decision_bar_source: str | None = None
    pricing_policy: str | None = None
    managed_exit_policy_id: str | None = None
    live_money_readiness: bool = False
    lifecycle_mode: str = "STRATEGY_MANAGED"
    output_root: Path = DEFAULT_TRACK_B_STRATEGY_TRADE_INTENT_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    live_position_status_json: Path | None = None
    source_artifact_paths: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class TrackBStrategyTradeIntentResult:
    classification: TrackBStrategyTradeIntentClassification
    intent_created: bool
    intent_id: str | None
    latest_intent_json: Path
    intent_jsonl: Path
    report: dict[str, Any]


def create_track_b_strategy_trade_intent(
    *,
    config: TrackBStrategyTradeIntentConfig,
    strategy_report: Mapping[str, Any],
    intent_id: str | None = None,
    now: datetime | None = None,
) -> TrackBStrategyTradeIntentResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    latest_json = Path(config.output_root) / "latest_track_b_strategy_trade_intent.json"
    jsonl = Path(config.output_root) / "track_b_strategy_trade_intents.jsonl"
    actual_intent_id = intent_id or f"track_b_strategy_trade_intent_{uuid.uuid4().hex}"

    classification, blocker = _classification(config, strategy_report)
    intent_created = classification == TrackBStrategyTradeIntentClassification.CREATED
    report = _build_report(
        config=config,
        strategy_report=strategy_report,
        now=actual_now,
        intent_id=actual_intent_id if intent_created else None,
        classification=classification,
        primary_blocker=blocker,
        latest_json=latest_json,
        jsonl=jsonl,
    )
    _write_intent_artifacts(latest_json=latest_json, jsonl=jsonl, report=report)
    return TrackBStrategyTradeIntentResult(
        classification=classification,
        intent_created=intent_created,
        intent_id=actual_intent_id if intent_created else None,
        latest_intent_json=latest_json,
        intent_jsonl=jsonl,
        report=report,
    )


def _classification(
    config: TrackBStrategyTradeIntentConfig,
    strategy_report: Mapping[str, Any],
) -> tuple[TrackBStrategyTradeIntentClassification, str | None]:
    if strategy_report.get("signal_emitted") is not True or strategy_report.get("real_strategy_signal") is not True:
        return (
            TrackBStrategyTradeIntentClassification.NOT_CREATED_NO_SIGNAL,
            "No real registered strategy signal was emitted.",
        )
    if str(config.mode).upper() != "PAPER":
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_ARBITRATION,
            "Strategy trade intent requires mode=PAPER.",
        )
    registered_id = strategy_report.get("strategy_registry_id") or strategy_report.get("strategy_id")
    if not registered_id or str(registered_id) != str(config.strategy_id):
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_STRATEGY_NOT_REGISTERED,
            f"Strategy signal did not resolve to registered strategy {config.strategy_id}.",
        )
    if strategy_report.get("strategy_registry_paper_eligible") is not True:
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_NOT_PAPER_ELIGIBLE,
            f"{config.strategy_id} is not registered paper_eligible=true.",
        )
    if strategy_report.get("strategy_registry_live_money_eligible") is not False or config.live_money_readiness is True:
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_LIVE_MONEY_DISABLED,
            "Track B PAPER intent requires live_money_eligible=false and live_money_readiness=false.",
        )
    if _review_required(config, strategy_report):
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_REVIEW_REQUIRED,
            "Existing review-required PAPER lifecycle state blocks new strategy trade intent.",
        )
    if config.account_id != config.expected_account_id:
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_ACCOUNT_GUARD,
            f"Account guard failed: {config.account_id} != expected {config.expected_account_id}.",
        )
    if not config.contract_key or not config.local_symbol or config.quantity is None or config.quantity <= 0:
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_CONTRACT_GUARD,
            "Contract guard requires contract_key, local_symbol, and positive quantity.",
        )
    latest_source = str(config.latest_decision_bar_source or strategy_report.get("latest_decision_bar_source") or "")
    if latest_source != "DATABENTO_LIVE_ARTIFACT":
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_NOT_LIVE_DECISION_BAR,
            "Strategy trade intent requires latest decision bar source DATABENTO_LIVE_ARTIFACT.",
        )
    if not _normalized_exit_policy(config.managed_exit_policy_id):
        return (
            TrackBStrategyTradeIntentClassification.BLOCKED_MISSING_EXIT_POLICY,
            f"{config.strategy_id} has no managed PAPER exit policy; paper_proof is not a strategy-management fallback.",
        )
    return (TrackBStrategyTradeIntentClassification.CREATED, None)


def _build_report(
    *,
    config: TrackBStrategyTradeIntentConfig,
    strategy_report: Mapping[str, Any],
    now: datetime,
    intent_id: str | None,
    classification: TrackBStrategyTradeIntentClassification,
    primary_blocker: str | None,
    latest_json: Path,
    jsonl: Path,
) -> dict[str, Any]:
    side = _normalized_side(strategy_report.get("signal_direction") or config.side)
    action = _order_action(side)
    return {
        "schema_version": "track_b_strategy_trade_intent_v1",
        "created_at": now.isoformat(),
        "generated_at": now.isoformat(),
        "classification": classification.value,
        "intent_classification": classification.value,
        "intent_created": classification == TrackBStrategyTradeIntentClassification.CREATED,
        "intent_id": intent_id,
        "strategy_id": config.strategy_id,
        "signal_source": strategy_report.get("signal_source"),
        "instrument": config.instrument_family,
        "instrument_family": config.instrument_family,
        "contract_key": config.contract_key,
        "local_symbol": config.local_symbol,
        "con_id": config.con_id,
        "account": config.account_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "side": side,
        "order_action": action,
        "quantity": config.quantity,
        "decision_bar_timestamp": _first_text(
            strategy_report,
            ("decision_bar_timestamp", "candle_timestamp", "signal_bar_timestamp"),
        ),
        "signal_timestamp": _first_text(
            strategy_report,
            ("signal_timestamp", "decision_bar_timestamp", "candle_timestamp"),
        ),
        "signal_reason": _first_text(strategy_report, ("candidate_reason", "primary_signal_reason", "decision_reason", "decision")),
        "passed_predicates": strategy_report.get("passed_predicates") or strategy_report.get("passed_predicate_labels") or [],
        "failed_predicates": [] if classification == TrackBStrategyTradeIntentClassification.CREATED else strategy_report.get("failed_predicates") or [],
        "latest_decision_bar_source": config.latest_decision_bar_source or strategy_report.get("latest_decision_bar_source"),
        "runtime_source": config.runtime_source,
        "pricing_policy": config.pricing_policy,
        "lifecycle_mode": config.lifecycle_mode,
        "managed_exit_policy_id": config.managed_exit_policy_id,
        "paper_eligible": strategy_report.get("strategy_registry_paper_eligible"),
        "live_money_eligible": strategy_report.get("strategy_registry_live_money_eligible"),
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "required_next_action": _required_next_action(classification),
        "source_artifact_paths": dict(config.source_artifact_paths or {}),
        "latest_intent_json_path": str(latest_json),
        "intent_jsonl_path": str(jsonl),
        "submit_attempted": False,
        "broker_state_mutated": False,
        "paper_proof_invoked": False,
        "ui_authority": False,
    }


def _review_required(config: TrackBStrategyTradeIntentConfig, strategy_report: Mapping[str, Any]) -> bool:
    if strategy_report.get("review_required") is True or strategy_report.get("unresolved_review_required") is True:
        return True
    status_path = (
        Path(config.live_position_status_json)
        if config.live_position_status_json is not None
        else Path(config.paper_trade_ledger_output_root) / "latest_track_b_live_position_status.json"
    )
    try:
        value = json.loads(status_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return False
    if not isinstance(value, Mapping):
        return False
    if int(value.get("review_required_count") or 0) > 0:
        return True
    return any(isinstance(item, Mapping) and item.get("review_required") is True for item in value.get("positions") or [])


def _write_intent_artifacts(*, latest_json: Path, jsonl: Path, report: Mapping[str, Any]) -> None:
    latest_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(dict(report)), indent=2, sort_keys=True)
    latest_json.write_text(payload, encoding="utf-8")
    with jsonl.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(dict(report)), sort_keys=True))
        handle.write("\n")


def _normalized_exit_policy(value: str | None) -> str | None:
    normalized = str(value or "").strip().upper()
    if normalized in {"", "EXIT_NOT_AVAILABLE"}:
        return None
    return normalized


def _normalized_side(value: object) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"BUY", "LONG"}:
        return "LONG"
    if raw in {"SELL", "SHORT"}:
        return "SHORT"
    return raw or "UNKNOWN"


def _order_action(side: str) -> str:
    return "SELL" if _normalized_side(side) == "SHORT" else "BUY"


def _first_text(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = mapping.get(key)
        if value not in {None, ""}:
            return str(value)
    return None


def _required_next_action(classification: TrackBStrategyTradeIntentClassification) -> str:
    if classification == TrackBStrategyTradeIntentClassification.CREATED:
        return "Invoke the guarded strategy-managed PAPER lifecycle with this intent; do not use paper_proof fallback."
    if classification == TrackBStrategyTradeIntentClassification.NOT_CREATED_NO_SIGNAL:
        return "Continue monitoring for a real registered strategy signal."
    return "Do not invoke strategy-managed PAPER lifecycle until the intent blocker is resolved."
