"""Dry-run broker envelope adapter for London active-evidence PAPER lanes.

The adapter is intentionally envelope-only. It records the broker-authoritative
contract that an accepted London active-evidence intent would need before the
normal Track B bridge could submit it, but it never marks the lane submit-capable
and never calls an IBKR path.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_position_intent_contract import (
    APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES,
    position_intent_from_template,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "track_b_london_active_evidence_broker_envelope_v1"
DEFAULT_ACCOUNT_ID = "DUM882026"
BRIDGE_DRY_RUN_CLASSIFICATION = "BROKER_AUTHORITATIVE_ENVELOPE_READY_DRY_RUN"
NO_ENVELOPE_NO_ACCEPTED_INTENT = "NO_BROKER_ENVELOPE_NO_ACCEPTED_INTENT"
ANCHOR_NOT_READY = "SESSION_ANCHOR_NOT_READY"
UNSUPPORTED_LONDON_LANE = "UNSUPPORTED_LONDON_ACTIVE_EVIDENCE_LANE"
NO_IBKR_CALL_PATH_INVOKED = "NO_IBKR_CALL_PATH_INVOKED"


@dataclass(frozen=True)
class LondonActiveEvidenceBrokerEnvelopeConfig:
    repo_root: Path = REPO_ROOT
    output_root: Path = Path("outputs/track_b_execution_core")
    account_id: str = DEFAULT_ACCOUNT_ID

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


@dataclass(frozen=True)
class LondonActiveEvidenceBrokerEnvelopeResult:
    classification: str
    envelope: dict[str, Any] | None
    latest_path: Path | None
    event_path: Path | None
    reason_code: str | None = None


@dataclass(frozen=True)
class _LondonLaneSpec:
    lane_id: str
    strategy_id: str
    session: str
    anchor_type: str
    output_family: str
    expected_direction: str


_LONDON_LANE_SPECS: dict[str, _LondonLaneSpec] = {
    "mnq_london_open_active_participation_long": _LondonLaneSpec(
        lane_id="mnq_london_open_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
        session="LONDON_OPEN",
        anchor_type="LONDON_0300_OPEN",
        output_family="london_open_active_evidence",
        expected_direction="LONG",
    ),
    "mnq_london_open_active_participation_short": _LondonLaneSpec(
        lane_id="mnq_london_open_active_participation_short",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        session="LONDON_OPEN",
        anchor_type="LONDON_0300_OPEN",
        output_family="london_open_active_evidence",
        expected_direction="SHORT",
    ),
    "mes_london_open_active_participation_long": _LondonLaneSpec(
        lane_id="mes_london_open_active_participation_long",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_LONG_V1",
        session="LONDON_OPEN",
        anchor_type="LONDON_0300_OPEN",
        output_family="london_open_active_evidence",
        expected_direction="LONG",
    ),
    "mes_london_open_active_participation_short": _LondonLaneSpec(
        lane_id="mes_london_open_active_participation_short",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MES_LONDON_OPEN_PARTICIPATION_SHORT_V1",
        session="LONDON_OPEN",
        anchor_type="LONDON_0300_OPEN",
        output_family="london_open_active_evidence",
        expected_direction="SHORT",
    ),
    "mnq_london_late_active_participation_short": _LondonLaneSpec(
        lane_id="mnq_london_late_active_participation_short",
        strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
        session="LONDON_LATE",
        anchor_type="LONDON_LATE_0530_REFERENCE",
        output_family="london_late_active_evidence",
        expected_direction="SHORT",
    ),
}


def london_active_evidence_lane_ids() -> tuple[str, ...]:
    return tuple(_LONDON_LANE_SPECS)


def build_london_active_evidence_broker_envelope(
    *,
    lane_id: str,
    order_intent: OrderIntent | Mapping[str, Any] | None,
    rule_report: Mapping[str, Any],
    source_candle_timestamp: datetime | str | None,
    config: LondonActiveEvidenceBrokerEnvelopeConfig | None = None,
    generated_at: datetime | None = None,
) -> LondonActiveEvidenceBrokerEnvelopeResult:
    cfg = config or LondonActiveEvidenceBrokerEnvelopeConfig()
    spec = _LONDON_LANE_SPECS.get(str(lane_id or "").strip())
    if spec is None:
        return LondonActiveEvidenceBrokerEnvelopeResult(
            classification=UNSUPPORTED_LONDON_LANE,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code=UNSUPPORTED_LONDON_LANE,
        )
    if order_intent is None:
        return LondonActiveEvidenceBrokerEnvelopeResult(
            classification=NO_ENVELOPE_NO_ACCEPTED_INTENT,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code=NO_ENVELOPE_NO_ACCEPTED_INTENT,
        )

    report = dict(rule_report)
    if str(report.get("session_anchor_status") or "").upper() != "READY":
        return LondonActiveEvidenceBrokerEnvelopeResult(
            classification=ANCHOR_NOT_READY,
            envelope=None,
            latest_path=None,
            event_path=None,
            reason_code=str(report.get("session_anchor_reason_code") or ANCHOR_NOT_READY),
        )

    template = APPROVED_TRACK_B_POSITION_INTENT_TEMPLATES[spec.strategy_id]
    contract = position_intent_from_template(template)
    created_at = _intent_created_at(order_intent) or _parse_time(source_candle_timestamp) or _utc_now(generated_at)
    source_ts = _parse_time(source_candle_timestamp) or created_at
    action = "BUY" if contract.side.upper() == "LONG" else "SELL"
    intent_id = _intent_value(order_intent, "order_intent_id") or f"{source_ts.isoformat()}|{action}_TO_OPEN"
    identity_seed = "|".join(
        (
            cfg.account_id,
            contract.local_symbol,
            str(contract.con_id),
            spec.lane_id,
            spec.strategy_id,
            action,
            source_ts.isoformat(),
            intent_id,
        )
    )
    digest = hashlib.sha256(identity_seed.encode("utf-8")).hexdigest()
    trade_id = f"trade_london_active_evidence_{digest[:16]}"
    lifecycle_id = f"reserved_submit_{spec.lane_id}_{source_ts.strftime('%Y%m%dT%H%M%S%fZ')}_{digest[:12]}"
    output_dir = cfg.resolve(cfg.output_root) / spec.output_family
    latest_path = output_dir / f"latest_{spec.lane_id}_event_envelope.json"
    event_path = output_dir / "broker_event_envelope_events.jsonl"
    now = _utc_now(generated_at)
    envelope = {
        "schema_version": SCHEMA_VERSION,
        "classification": BRIDGE_DRY_RUN_CLASSIFICATION,
        "generated_at": now.isoformat(),
        "dry_run": True,
        "broker_mutation_allowed": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "ibkr_call_path_invoked": False,
        "no_ibkr_call_path_reason": NO_IBKR_CALL_PATH_INVOKED,
        "current_order_destination": "ibkr_paper_bridge_submit_capable",
        "bridge_activation_status": "ENVELOPE_ONLY_NOT_SUBMIT_CAPABLE",
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
        "strategy_id": spec.strategy_id,
        "lane_id": spec.lane_id,
        "session": spec.session,
        "exit_policy": asdict(contract.exit_policy),
        "hold_policy": asdict(contract.hold_policy),
        "conflict_group": contract.conflict_group,
        "anchor_reference": {
            "anchor_type": spec.anchor_type,
            "status": report.get("session_anchor_status"),
            "reason_code": report.get("session_anchor_reason_code"),
            "source": report.get("session_anchor_source"),
            "source_artifact_path": report.get("session_anchor_source_artifact_path"),
            "reference_price": report.get("session_open_price"),
        },
        "source_candle_timestamp": source_ts.isoformat(),
        "source_order_intent_id": intent_id,
        "source_order_intent_type": _intent_value(order_intent, "intent_type"),
        "source_reason_code": _intent_value(order_intent, "reason_code"),
        "source_bar_id": _intent_value(order_intent, "bar_id"),
        "provenance": {
            "adapter": "track_b_london_active_evidence_broker_envelope",
            "source_rule_report": dict(report),
            "paper_lane_order_fill_is_simulated": True,
            "broker_authoritative_submit_requires_follow_up_activation": True,
            "latest_artifact_path": str(latest_path),
            "event_stream_path": str(event_path),
        },
    }
    return LondonActiveEvidenceBrokerEnvelopeResult(
        classification=BRIDGE_DRY_RUN_CLASSIFICATION,
        envelope=envelope,
        latest_path=latest_path,
        event_path=event_path,
    )


def write_london_active_evidence_broker_envelope(
    *,
    result: LondonActiveEvidenceBrokerEnvelopeResult,
) -> tuple[Path, Path] | None:
    if result.envelope is None or result.latest_path is None or result.event_path is None:
        return None
    write_json_atomic(result.latest_path, result.envelope)
    result.event_path.parent.mkdir(parents=True, exist_ok=True)
    with result.event_path.open("a", encoding="utf-8") as handle:
        import json

        handle.write(json.dumps(result.envelope, sort_keys=True) + "\n")
    return result.latest_path, result.event_path


def write_london_active_evidence_broker_envelope_for_intent(
    *,
    lane_id: str,
    order_intent: OrderIntent | Mapping[str, Any] | None,
    rule_report: Mapping[str, Any],
    source_candle_timestamp: datetime | str | None,
    config: LondonActiveEvidenceBrokerEnvelopeConfig | None = None,
    generated_at: datetime | None = None,
) -> LondonActiveEvidenceBrokerEnvelopeResult:
    result = build_london_active_evidence_broker_envelope(
        lane_id=lane_id,
        order_intent=order_intent,
        rule_report=rule_report,
        source_candle_timestamp=source_candle_timestamp,
        config=config,
        generated_at=generated_at,
    )
    if result.envelope is not None:
        write_london_active_evidence_broker_envelope(result=result)
    return result


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
