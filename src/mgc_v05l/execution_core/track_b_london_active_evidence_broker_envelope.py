"""Broker envelope adapter for London active-evidence PAPER lanes.

The adapter is artifact-only. It records the broker-authoritative contract that
an accepted London active-evidence intent needs before the normal Track B bridge
can submit it, but it never calls an IBKR path itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from mgc_v05l.execution_core.track_b_broker_event_envelope import (
    BROKER_EVENT_ENVELOPE_BLOCKED,
    BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE,
    BROKER_EVENT_ENVELOPE_READY_DRY_RUN,
    BROKER_EVENT_ENVELOPE_READY_SUBMIT_CAPABLE,
    BrokerEventEnvelopeConfig,
    BrokerEventEnvelopeLaneContext,
    build_broker_event_envelope,
    write_broker_event_envelope,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_VERSION = "track_b_london_active_evidence_broker_envelope_v1"
DEFAULT_ACCOUNT_ID = "DUM882026"
BRIDGE_DRY_RUN_CLASSIFICATION = BROKER_EVENT_ENVELOPE_READY_DRY_RUN
BRIDGE_SUBMIT_CAPABLE_CLASSIFICATION = BROKER_EVENT_ENVELOPE_READY_SUBMIT_CAPABLE
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
    bridge_submit_capable = lane_submit_bridge_adapter(lane_id=spec.lane_id) is not None
    result = build_broker_event_envelope(
        context=BrokerEventEnvelopeLaneContext(
            lane_id=spec.lane_id,
            strategy_id=spec.strategy_id,
            lane_classification="PAPER_ONLY_PROMOTION_READY_ACTIVE_EVIDENCE",
            session=spec.session,
            artifact_family=spec.output_family,
            anchor_type=spec.anchor_type,
            promotion_ready=True,
            bridge_submit_adapter_present=bridge_submit_capable,
            broker_authoritative=bridge_submit_capable,
        ),
        order_intent=order_intent,
        rule_report=rule_report,
        source_candle_timestamp=source_candle_timestamp,
        config=BrokerEventEnvelopeConfig(
            repo_root=cfg.repo_root,
            output_root=cfg.output_root,
            account_id=cfg.account_id,
        ),
        generated_at=generated_at,
    )
    classification = result.classification
    reason_code = result.reason_code
    if classification == BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE and reason_code == "NO_ACCEPTED_ENTRY_INTENT":
        classification = NO_ENVELOPE_NO_ACCEPTED_INTENT
        reason_code = NO_ENVELOPE_NO_ACCEPTED_INTENT
    if classification == BROKER_EVENT_ENVELOPE_BLOCKED and str(reason_code or "").startswith("ANCHOR"):
        classification = ANCHOR_NOT_READY
    return LondonActiveEvidenceBrokerEnvelopeResult(
        classification=classification,
        envelope=result.envelope,
        latest_path=result.latest_path,
        event_path=result.event_path,
        reason_code=reason_code,
    )


def write_london_active_evidence_broker_envelope(
    *,
    result: LondonActiveEvidenceBrokerEnvelopeResult,
) -> tuple[Path, Path] | None:
    if result.envelope is None or result.latest_path is None or result.event_path is None:
        return None
    return write_broker_event_envelope(result=result)  # type: ignore[arg-type]


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
