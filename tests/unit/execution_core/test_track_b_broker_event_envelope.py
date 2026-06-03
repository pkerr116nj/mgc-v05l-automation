from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.execution_core.track_b_broker_event_envelope import (
    BROKER_EVENT_ENVELOPE_BLOCKED,
    BROKER_EVENT_ENVELOPE_MAPPED_TO_BRIDGE,
    BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE,
    BROKER_EVENT_ENVELOPE_READY_DRY_RUN,
    BrokerEventEnvelopeConfig,
    BrokerEventEnvelopeLaneContext,
    BrokerEnvelopeRequirement,
    build_broker_event_envelope,
    envelope_requirement_for_lane,
)


NOW = datetime(2026, 6, 3, 9, 45, tzinfo=UTC)


def test_promotion_ready_accepted_intent_emits_standard_dry_run_envelope(tmp_path) -> None:  # type: ignore[no-untyped-def]
    result = build_broker_event_envelope(
        context=_context(),
        order_intent=_intent(),
        rule_report=_rule_report(),
        source_candle_timestamp=NOW,
        config=BrokerEventEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.classification == BROKER_EVENT_ENVELOPE_READY_DRY_RUN
    assert result.requirement == "REQUIRED"
    assert result.envelope is not None
    assert result.envelope["schema_version"] == "track_b_broker_event_envelope_v1"
    assert result.envelope["lane_id"] == "mnq_london_late_active_participation_short"
    assert result.envelope["strategy_id"] == "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1"
    assert result.envelope["action"] == "SELL"
    assert result.envelope["qty"] == 1
    assert result.envelope["localSymbol"] == "MNQM6"
    assert result.envelope["conId"] == 770561201
    assert result.envelope["account"] == "DUM882026"
    assert result.envelope["account_id"] == "DUM882026"
    assert result.envelope["session"] == "LONDON_LATE"
    assert result.envelope["source_candle_timestamp"] == NOW.isoformat()
    assert result.envelope["exit_policy"]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert result.envelope["anchor_reference"]["anchor_type"] == "LONDON_LATE_0530_REFERENCE"
    assert result.envelope["provenance"]["runtime_profile"] == "mnq_mes_full_session_active_evidence"
    assert result.envelope["provenance"]["runtime_commit"] == "abc123"
    assert result.envelope["broker_submit_enabled"] is False
    assert result.envelope["ibkr_call_path_invoked"] is False


def test_missing_session_is_derived_from_lane_metadata(tmp_path) -> None:  # type: ignore[no-untyped-def]
    result = build_broker_event_envelope(
        context=_context(session=""),
        order_intent=_intent(),
        rule_report=_rule_report(),
        source_candle_timestamp=NOW,
        config=BrokerEventEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.envelope is not None
    assert result.envelope["session"] == "LONDON_LATE"
    assert result.envelope["account"] == result.envelope["account_id"] == "DUM882026"


def test_broker_authoritative_lane_maps_to_existing_bridge_contract_without_dry_run_envelope() -> None:
    result = build_broker_event_envelope(
        context=_context(
            lane_id="mnq_globex_active_participation_short",
            strategy_id="PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1",
            lane_classification="PAPER_SUBMIT_ELIGIBLE",
            artifact_family="globex_active_evidence",
            bridge_submit_adapter_present=True,
            broker_authoritative=True,
        ),
        order_intent=_intent(reason_code="PAPER_ACTIVE_EVIDENCE_MNQ_GLOBEX_PARTICIPATION_SHORT_V1"),
        rule_report=_rule_report(anchor_type="GLOBEX_1800_REOPEN"),
        source_candle_timestamp=NOW,
    )

    assert result.classification == BROKER_EVENT_ENVELOPE_MAPPED_TO_BRIDGE
    assert result.requirement == "SATISFIED_BY_BRIDGE"
    assert result.envelope is None
    assert result.reason_code == "LANE_USES_EXISTING_BRIDGE_SUBMIT_ADAPTER"


def test_shadow_only_lane_is_explicitly_not_envelope_eligible() -> None:
    result = build_broker_event_envelope(
        context=_context(lane_classification="SHADOW_ONLY_RESEARCH"),
        order_intent=_intent(),
        rule_report=_rule_report(),
        source_candle_timestamp=NOW,
    )

    assert result.classification == BROKER_EVENT_ENVELOPE_NOT_ELIGIBLE
    assert result.reason_code == "LANE_CLASSIFICATION_FORBIDS_BROKER_EVENT_ENVELOPE"


def test_missing_contract_blocks_with_explicit_required_field_reason() -> None:
    result = build_broker_event_envelope(
        context=_context(strategy_id="UNKNOWN_PROMOTION_READY_STRATEGY"),
        order_intent=_intent(reason_code="UNKNOWN_PROMOTION_READY_STRATEGY"),
        rule_report=_rule_report(),
        source_candle_timestamp=NOW,
    )

    assert result.classification == BROKER_EVENT_ENVELOPE_BLOCKED
    assert result.reason_code == "POSITION_INTENT_CONTRACT_NOT_FOUND"
    assert result.missing_fields == ("position_intent_contract",)


def test_requirement_policy_by_lane_classification() -> None:
    assert envelope_requirement_for_lane(_context()).value == BrokerEnvelopeRequirement.REQUIRED.value
    assert envelope_requirement_for_lane(_context(lane_classification="SHADOW_ONLY")).value == "FORBIDDEN"
    assert envelope_requirement_for_lane(_context(bridge_submit_adapter_present=True)).value == "SATISFIED_BY_BRIDGE"


def _context(
    *,
    lane_id: str = "mnq_london_late_active_participation_short",
    strategy_id: str = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1",
    lane_classification: str = "PAPER_ONLY_PROMOTION_READY_ACTIVE_EVIDENCE",
    artifact_family: str = "london_late_active_evidence",
    bridge_submit_adapter_present: bool = False,
    broker_authoritative: bool = False,
    session: str = "LONDON_LATE",
) -> BrokerEventEnvelopeLaneContext:
    return BrokerEventEnvelopeLaneContext(
        lane_id=lane_id,
        strategy_id=strategy_id,
        lane_classification=lane_classification,
        session=session,
        window="05:30-08:20_ET",
        artifact_family=artifact_family,
        anchor_type="LONDON_LATE_0530_REFERENCE",
        input_artifact_path="outputs/track_b_execution_core/london_late_active_evidence/latest_mnq_london_late_active_participation_short_event_envelope.json",
        runtime_profile="mnq_mes_full_session_active_evidence",
        runtime_commit="abc123",
        bridge_submit_adapter_present=bridge_submit_adapter_present,
        promotion_ready=True,
        broker_authoritative=broker_authoritative,
    )


def _intent(*, reason_code: str = "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_LATE_PARTICIPATION_SHORT_V1") -> OrderIntent:
    return OrderIntent(
        order_intent_id=f"MNQ|1m|{NOW.isoformat()}|SELL_TO_OPEN",
        bar_id=f"MNQ|1m|{NOW.isoformat()}",
        symbol="MNQ",
        intent_type=OrderIntentType.SELL_TO_OPEN,
        quantity=1,
        created_at=NOW,
        reason_code=reason_code,
        signal_id="signal-1",
    )


def _rule_report(*, anchor_type: str = "LONDON_LATE_0530_REFERENCE") -> dict[str, object]:
    return {
        "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_ACCEPTED",
        "primary_blocker": None,
        "condition": "05:30-08:20_ET_close_below_vwap_or_london_late_reference",
        "session_anchor_status": "READY",
        "session_anchor_reason_code": "ANCHOR_READY_FROM_RUNTIME",
        "session_anchor_source": "LIVE_PHASE1",
        "session_anchor_source_artifact_path": f"outputs/track_b_execution_core/session_anchors/MNQ/2026-06-03/{anchor_type}.json",
        "session_open_price": "30760.5",
    }
