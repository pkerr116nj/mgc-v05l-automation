"""Standard broker-event envelope contract for Track B PAPER lanes.

This module is artifact-only. It does not submit, cancel, modify, flatten, or
call IBKR. Promotion-ready lanes can use it to prove that an accepted entry
intent has all broker-bound identity and lifecycle fields before any later
broker-authoritative activation.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES,
    PositionIntent,
    position_intent_from_template,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "track_b_broker_event_envelope_v1"
DEFAULT_ACCOUNT_ID = "DUM882026"
BROKER_EVENT_ENVELOPE_READY_DRY_RUN = "BROKER_EVENT_ENVELOPE_READY_DRY_RUN"
BROKER_EVENT_ENVELOPE_READY_SUBMIT_CAPABLE = "BROKER_EVENT_ENVELOPE_READY_SUBMIT_CAPABLE"
BROKER_EVENT_ENVELOPE_MAPPED_TO_BRIDGE = "BROKER_EVENT_ENVELOPE_MAPPED_TO_BRIDGE_SUBMIT_ADAPTER"
BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE = "NOT_BROKER_ENVELOPE_ELIGIBLE"
BROKER_EVENT_ENVELOPE_BLOCKED = "BROKER_EVENT_ENVELOPE_BLOCKED"
NO_IBKR_CALL_PATH_INVOKED = "NO_IBKR_CALL_PATH_INVOKED"
SUBMIT_CAPABLE_PENDING_RUNTIME_GATES = "SUBMIT_CAPABLE_PENDING_RUNTIME_GATES"


class BrokerEnvelopeMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    BROKER_AUTHORITATIVE = "BROKER_AUTHORITATIVE"


class BrokerEnvelopeRequirement(str, Enum):
    REQUIRED = "REQUIRED"
    OPTIONAL = "OPTIONAL"
    FORBIDDEN = "FORBIDDEN"
    SATISFIED_BY_BRIDGE = "SATISFIED_BY_BRIDGE"


@dataclass(frozen=True)
class BrokerEventEnvelopeConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = Path("outputs/track_b_execution_core")
    account_id: str = DEFAULT_ACCOUNT_ID

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


@dataclass(frozen=True)
class BrokerEventEnvelopeResult:
    classification: str
    requirement: str
    envelope_mode: str
    envelope: dict[str, Any] | None
    latest_path: Path | None
    event_path: Path | None
    reason_code: str | None = None
    missing_fields: tuple[str, ...] = ()


@dataclass(frozen=True)
class BrokerEventEnvelopeLaneContext:
    lane_id: str
    strategy_id: str
    lane_classification: str
    session: str | None = None
    window: str | None = None
    artifact_family: str | None = None
    anchor_type: str | None = None
    input_artifact_path: str | None = None
    runtime_profile: str | None = None
    runtime_commit: str | None = None
    bridge_submit_adapter_present: bool = False
    promotion_ready: bool = False
    broker_authoritative: bool = False


def build_broker_event_envelope(
    *,
    context: BrokerEventEnvelopeLaneContext,
    order_intent: OrderIntent | Mapping[str, Any] | None,
    rule_report: Mapping[str, Any],
    source_candle_timestamp: datetime | str | None,
    config: BrokerEventEnvelopeConfig | None = None,
    generated_at: datetime | None = None,
) -> BrokerEventEnvelopeResult:
    cfg = config or BrokerEventEnvelopeConfig()
    requirement = envelope_requirement_for_lane(context)
    if requirement == BrokerEnvelopeRequirement.FORBIDDEN:
        return BrokerEventEnvelopeResult(
            classification=BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE,
            requirement=requirement.value,
            envelope_mode=BrokerEnvelopeMode.DRY_RUN.value,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code="LANE_CLASSIFICATION_FORBIDS_BROKER_EVENT_ENVELOPE",
        )
    if order_intent is None:
        return BrokerEventEnvelopeResult(
            classification=BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE,
            requirement=requirement.value,
            envelope_mode=BrokerEnvelopeMode.DRY_RUN.value,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code="NO_ACCEPTED_ENTRY_INTENT",
        )

    report = dict(rule_report)
    if _anchor_required(context) and str(report.get("session_anchor_status") or "").upper() != "READY":
        return BrokerEventEnvelopeResult(
            classification=BROKER_EVENT_ENVELOPE_BLOCKED,
            requirement=requirement.value,
            envelope_mode=BrokerEnvelopeMode.DRY_RUN.value,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code=str(report.get("session_anchor_reason_code") or "SESSION_ANCHOR_NOT_READY"),
        )

    contract = _position_intent_for_strategy(context.strategy_id)
    if contract is None:
        return BrokerEventEnvelopeResult(
            classification=BROKER_EVENT_ENVELOPE_BLOCKED,
            requirement=requirement.value,
            envelope_mode=BrokerEnvelopeMode.DRY_RUN.value,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code="POSITION_INTENT_CONTRACT_NOT_FOUND",
            missing_fields=("position_intent_contract",),
        )

    created_at = _intent_created_at(order_intent) or _parse_time(source_candle_timestamp) or _utc_now(generated_at)
    source_ts = _parse_time(source_candle_timestamp) or created_at
    action = _action_for_contract(contract)
    intent_id = _intent_value(order_intent, "order_intent_id") or f"{source_ts.isoformat()}|{action}_TO_OPEN"
    latest_path, event_path = _artifact_paths(config=cfg, context=context)
    envelope = _envelope_payload(
        cfg=cfg,
        context=context,
        contract=contract,
        order_intent=order_intent,
        rule_report=report,
        action=action,
        intent_id=intent_id,
        source_ts=source_ts,
        latest_path=latest_path,
        event_path=event_path,
        generated_at=_utc_now(generated_at),
        submit_capability=_submit_capability_for_context(context),
    )
    missing = _missing_required_fields(envelope)
    if missing:
        return BrokerEventEnvelopeResult(
            classification=BROKER_EVENT_ENVELOPE_BLOCKED,
            requirement=requirement.value,
            envelope_mode=BrokerEnvelopeMode.DRY_RUN.value,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code="BROKER_EVENT_ENVELOPE_REQUIRED_FIELD_MISSING",
            missing_fields=tuple(missing),
        )
    return BrokerEventEnvelopeResult(
        classification=str(envelope["classification"]),
        requirement=requirement.value,
        envelope_mode=str(envelope["envelope_mode"]),
        envelope=envelope,
        latest_path=latest_path,
        event_path=event_path,
    )


def write_broker_event_envelope(*, result: BrokerEventEnvelopeResult) -> tuple[Path, Path] | None:
    if result.envelope is None or result.latest_path is None or result.event_path is None:
        return None
    write_json_atomic(result.latest_path, result.envelope)
    result.event_path.parent.mkdir(parents=True, exist_ok=True)
    with result.event_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(result.envelope, sort_keys=True) + "\n")
    return result.latest_path, result.event_path


def envelope_requirement_for_lane(context: BrokerEventEnvelopeLaneContext) -> BrokerEnvelopeRequirement:
    classification = str(context.lane_classification or "").upper()
    if context.bridge_submit_adapter_present or context.broker_authoritative:
        return BrokerEnvelopeRequirement.REQUIRED
    if "SHADOW" in classification:
        return BrokerEnvelopeRequirement.FORBIDDEN
    if "RESEARCH" in classification:
        return BrokerEnvelopeRequirement.FORBIDDEN
    if context.promotion_ready:
        return BrokerEnvelopeRequirement.REQUIRED
    if "PROMOTION" in classification or "PAPER_REVIEW" in classification or "PAPER_ONLY" in classification:
        return BrokerEnvelopeRequirement.REQUIRED
    return BrokerEnvelopeRequirement.OPTIONAL


def broker_event_report_fields(result: BrokerEventEnvelopeResult) -> dict[str, Any]:
    mapped_to_bridge = result.classification == BROKER_EVENT_ENVELOPE_MAPPED_TO_BRIDGE
    submit_enabled = mapped_to_bridge or bool(result.envelope and result.envelope.get("broker_submit_enabled") is True)
    submit_blocker = None if submit_enabled else (
        "BROKER_EVENT_ENVELOPE_DRY_RUN_NOT_ACTIVATED" if result.envelope is not None else result.reason_code
    )
    return {
        "broker_event_envelope_classification": result.classification,
        "broker_event_envelope_requirement": result.requirement,
        "broker_event_envelope_mode": result.envelope_mode,
        "broker_event_envelope_path": str(result.latest_path) if result.latest_path else None,
        "broker_event_envelope_event_stream_path": str(result.event_path) if result.event_path else None,
        "broker_event_envelope_reason_code": result.reason_code,
        "broker_event_envelope_missing_fields": list(result.missing_fields),
        "broker_event_envelope_source_candle_timestamp": (
            result.envelope.get("source_candle_timestamp") if result.envelope else None
        ),
        "broker_authoritative_envelope_classification": result.classification,
        "broker_authoritative_envelope_path": str(result.latest_path) if result.latest_path else None,
        "broker_authoritative_envelope_event_stream_path": str(result.event_path) if result.event_path else None,
        "broker_authoritative_envelope_dry_run": result.envelope is not None
        and result.envelope_mode == BrokerEnvelopeMode.DRY_RUN.value,
        "broker_authoritative_submit_enabled": submit_enabled,
        "broker_authoritative_submit_blocker": submit_blocker,
        "ibkr_call_path_invoked": False,
    }


@dataclass(frozen=True)
class _EnvelopeSubmitCapability:
    mode: BrokerEnvelopeMode
    classification: str
    dry_run: bool
    broker_submit_enabled: bool
    submit_allowed: bool
    bridge_activation_status: str
    broker_authoritative_submit_requires_follow_up_activation: bool


def _submit_capability_for_context(context: BrokerEventEnvelopeLaneContext) -> _EnvelopeSubmitCapability:
    if context.broker_authoritative and context.bridge_submit_adapter_present:
        return _EnvelopeSubmitCapability(
            mode=BrokerEnvelopeMode.BROKER_AUTHORITATIVE,
            classification=BROKER_EVENT_ENVELOPE_READY_SUBMIT_CAPABLE,
            dry_run=False,
            broker_submit_enabled=True,
            submit_allowed=True,
            bridge_activation_status=SUBMIT_CAPABLE_PENDING_RUNTIME_GATES,
            broker_authoritative_submit_requires_follow_up_activation=False,
        )
    return _EnvelopeSubmitCapability(
        mode=BrokerEnvelopeMode.DRY_RUN,
        classification=BROKER_EVENT_ENVELOPE_READY_DRY_RUN,
        dry_run=True,
        broker_submit_enabled=False,
        submit_allowed=False,
        bridge_activation_status="ENVELOPE_ONLY_NOT_SUBMIT_CAPABLE",
        broker_authoritative_submit_requires_follow_up_activation=True,
    )


def _envelope_payload(
    *,
    cfg: BrokerEventEnvelopeConfig,
    context: BrokerEventEnvelopeLaneContext,
    contract: PositionIntent,
    order_intent: OrderIntent | Mapping[str, Any],
    rule_report: Mapping[str, Any],
    action: str,
    intent_id: str,
    source_ts: datetime,
    latest_path: Path,
    event_path: Path,
    generated_at: datetime,
    submit_capability: _EnvelopeSubmitCapability,
) -> dict[str, Any]:
    identity_seed = "|".join(
        (
            cfg.account_id,
            contract.local_symbol,
            str(contract.con_id),
            context.lane_id,
            context.strategy_id,
            action,
            source_ts.isoformat(),
            intent_id,
        )
    )
    digest = hashlib.sha256(identity_seed.encode("utf-8")).hexdigest()
    trade_id = f"trade_broker_event_{digest[:16]}"
    lifecycle_id = f"reserved_submit_{context.lane_id}_{source_ts.strftime('%Y%m%dT%H%M%S%fZ')}_{digest[:12]}"
    session = _session_for_context(context)
    runtime_profile = _normalized_context_value(context.runtime_profile, "UNKNOWN_RUNTIME_PROFILE")
    runtime_commit = _normalized_context_value(context.runtime_commit, "UNKNOWN_RUNTIME_COMMIT")
    return {
        "schema_version": SCHEMA_VERSION,
        "classification": submit_capability.classification,
        "generated_at": generated_at.isoformat(),
        "envelope_mode": submit_capability.mode.value,
        "dry_run": submit_capability.dry_run,
        "broker_mutation_allowed": False,
        "broker_submit_enabled": submit_capability.broker_submit_enabled,
        "submit_allowed": submit_capability.submit_allowed,
        "submit_attempted": False,
        "ibkr_call_path_invoked": False,
        "no_ibkr_call_path_reason": NO_IBKR_CALL_PATH_INVOKED,
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_activation_status": submit_capability.bridge_activation_status,
        "bridge_path_reused_if_activated": {
            "exposure_gate": True,
            "anti_flip_lock": True,
            "same_symbol_pending_fill_lock": True,
            "managed_close_exclusivity": True,
            "current_exposure_owner_resolver": True,
            "contract_resolver": True,
            "safe_state_guardian": True,
            "lifecycle_adoption": True,
            "managed_exit_timebox": True,
        },
        "trade_id_generation_rule": "sha256(account/localSymbol/conId/lane_id/strategy_id/action/source_candle_timestamp/intent_id)",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "lane_id": context.lane_id,
        "strategy_id": context.strategy_id,
        "session": session,
        "window": context.window,
        "account": cfg.account_id,
        "account_id": cfg.account_id,
        "symbol": contract.instrument_family,
        "instrument_family": contract.instrument_family,
        "contract_key": contract.contract_key,
        "localSymbol": contract.local_symbol,
        "local_symbol": contract.local_symbol,
        "conId": contract.con_id,
        "con_id": contract.con_id,
        "expiry": contract.expiry,
        "currency": "USD",
        "exchange": "CME",
        "side": contract.side,
        "action": action,
        "quantity": contract.quantity,
        "qty": contract.quantity,
        "entry_reason": _intent_value(order_intent, "reason_code"),
        "predicate_proof": dict(rule_report),
        "anchor_reference": {
            "anchor_type": context.anchor_type,
            "status": rule_report.get("session_anchor_status"),
            "reason_code": rule_report.get("session_anchor_reason_code"),
            "source": rule_report.get("session_anchor_source"),
            "source_artifact_path": rule_report.get("session_anchor_source_artifact_path"),
            "reference_price": rule_report.get("session_open_price"),
        },
        "source_candle_timestamp": source_ts.isoformat(),
        "source_order_intent_id": intent_id,
        "source_order_intent_type": _intent_value(order_intent, "intent_type"),
        "source_reason_code": _intent_value(order_intent, "reason_code"),
        "source_bar_id": _intent_value(order_intent, "bar_id"),
        "exit_policy": asdict(contract.exit_policy),
        "hold_policy": asdict(contract.hold_policy),
        "conflict_group": contract.conflict_group,
        "provenance": {
            "adapter": "track_b_broker_event_envelope",
            "runtime_profile": runtime_profile,
            "runtime_commit": runtime_commit,
            "generated_at": generated_at.isoformat(),
            "input_artifact_path": context.input_artifact_path,
            "source_rule_report": dict(rule_report),
            "paper_lane_order_fill_is_simulated": True,
            "broker_authoritative_submit_requires_follow_up_activation": (
                submit_capability.broker_authoritative_submit_requires_follow_up_activation
            ),
            "latest_artifact_path": str(latest_path),
            "event_stream_path": str(event_path),
        },
    }


def _artifact_paths(*, config: BrokerEventEnvelopeConfig, context: BrokerEventEnvelopeLaneContext) -> tuple[Path, Path]:
    family = str(context.artifact_family or "").strip()
    if family:
        output_dir = config.resolve(config.output_root) / family
        latest_path = output_dir / f"latest_{context.lane_id}_event_envelope.json"
    else:
        output_dir = config.resolve(config.output_root) / "broker_event_envelopes" / context.lane_id
        latest_path = output_dir / "latest_event_envelope.json"
    return latest_path, output_dir / "broker_event_envelope_events.jsonl"


def _anchor_required(context: BrokerEventEnvelopeLaneContext) -> bool:
    return bool(context.anchor_type)


def _position_intent_for_strategy(strategy_id: str) -> PositionIntent | None:
    template = APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES.get(str(strategy_id or ""))
    if template is None:
        return None
    return position_intent_from_template(template)


def _action_for_contract(contract: PositionIntent) -> str:
    return "BUY" if contract.side.upper() == "LONG" else "SELL"


def _missing_required_fields(envelope: Mapping[str, Any]) -> list[str]:
    required = (
        "lane_id",
        "strategy_id",
        "trade_id",
        "lifecycle_id",
        "side",
        "action",
        "qty",
        "symbol",
        "localSymbol",
        "conId",
        "account",
        "account_id",
        "session",
        "source_candle_timestamp",
        "exit_policy",
        "provenance",
    )
    missing = [key for key in required if envelope.get(key) in (None, "", {})]
    provenance = envelope.get("provenance")
    if not isinstance(provenance, Mapping):
        return missing + ["provenance"]
    for key in ("runtime_profile", "runtime_commit", "generated_at"):
        if key == "generated_at":
            continue
        if provenance.get(key) in (None, ""):
            missing.append(f"provenance.{key}")
    return missing


def _session_for_context(context: BrokerEventEnvelopeLaneContext) -> str:
    explicit = str(context.session or "").strip()
    if explicit:
        return explicit
    lane_id = str(context.lane_id or "").lower()
    window = str(context.window or "").lower()
    anchor = str(context.anchor_type or "").upper()
    if "london_late" in lane_id or "london_late" in anchor or "05:30" in window:
        return "LONDON_LATE"
    if "london_open" in lane_id or "london_open" in anchor or "03:00" in window:
        return "LONDON_OPEN"
    if "_us_" in lane_id or "09:30" in window:
        return "US"
    if "globex" in lane_id or "18:00" in window:
        return "GLOBEX"
    return "UNKNOWN_SESSION"


def _normalized_context_value(value: str | None, unknown: str) -> str:
    text = str(value or "").strip()
    if not text or text == unknown:
        return unknown
    return text


def _intent_value(order_intent: OrderIntent | Mapping[str, Any], key: str) -> str | None:
    if isinstance(order_intent, Mapping):
        value = order_intent.get(key)
    else:
        value = getattr(order_intent, key, None)
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _intent_created_at(order_intent: OrderIntent | Mapping[str, Any]) -> datetime | None:
    if isinstance(order_intent, Mapping):
        return _parse_time(order_intent.get("created_at"))
    return _parse_time(getattr(order_intent, "created_at", None))


def _parse_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_now(value: datetime | None = None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    parsed = _parse_time(value)
    return parsed or datetime.now(UTC)
