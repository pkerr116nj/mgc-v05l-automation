from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.domain.enums import OrderIntentType
from mgc_v05l.execution.ibkr_paper_strategy_porting import lane_submit_bridge_adapter
from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.execution_core.track_b_london_active_evidence_broker_envelope import (
    ANCHOR_NOT_READY,
    BRIDGE_SUBMIT_CAPABLE_CLASSIFICATION,
    NO_ENVELOPE_NO_ACCEPTED_INTENT,
    LondonActiveEvidenceBrokerEnvelopeConfig,
    build_london_active_evidence_broker_envelope,
    write_london_active_evidence_broker_envelope_for_intent,
)


NOW = datetime(2026, 6, 3, 7, 20, tzinfo=UTC)


def test_london_open_accepted_intent_produces_broker_envelope(tmp_path: Path) -> None:
    result = write_london_active_evidence_broker_envelope_for_intent(
        lane_id="mnq_london_open_active_participation_long",
        order_intent=_intent(OrderIntentType.BUY_TO_OPEN),
        rule_report=_rule_report(anchor_type="LONDON_0300_OPEN"),
        source_candle_timestamp=NOW,
        config=LondonActiveEvidenceBrokerEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.classification == BRIDGE_SUBMIT_CAPABLE_CLASSIFICATION
    assert result.latest_path is not None
    assert result.latest_path.name == "latest_mnq_london_open_active_participation_long_event_envelope.json"
    payload = json.loads(result.latest_path.read_text(encoding="utf-8"))
    assert payload["dry_run"] is False
    assert payload["broker_submit_enabled"] is True
    assert payload["submit_allowed"] is True
    assert payload["bridge_activation_status"] == "SUBMIT_CAPABLE_PENDING_RUNTIME_GATES"
    assert payload["ibkr_call_path_invoked"] is False
    assert payload["strategy_id"] == "PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1"
    assert payload["lane_id"] == "mnq_london_open_active_participation_long"
    assert payload["action"] == "BUY"
    assert payload["localSymbol"] == "MNQM6"
    assert payload["conId"] == 770561201
    assert payload["expiry"] == "20260618"
    assert payload["exit_policy"]["managed_exit_policy_id"] == "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1"
    assert payload["anchor_reference"]["anchor_type"] == "LONDON_0300_OPEN"
    assert payload["bridge_path_reused_if_activated"]["exposure_gate"] is True


def test_london_late_mnq_short_accepted_intent_produces_broker_envelope(tmp_path: Path) -> None:
    result = build_london_active_evidence_broker_envelope(
        lane_id="mnq_london_late_active_participation_short",
        order_intent=_intent(OrderIntentType.SELL_TO_OPEN),
        rule_report=_rule_report(anchor_type="LONDON_LATE_0530_REFERENCE"),
        source_candle_timestamp=NOW,
        config=LondonActiveEvidenceBrokerEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.classification == BRIDGE_SUBMIT_CAPABLE_CLASSIFICATION
    assert result.envelope is not None
    assert result.envelope["broker_submit_enabled"] is True
    assert result.envelope["submit_allowed"] is True
    assert result.envelope["session"] == "LONDON_LATE"
    assert result.envelope["action"] == "SELL"
    assert result.envelope["localSymbol"] == "MNQM6"
    assert result.envelope["conId"] == 770561201
    assert result.envelope["anchor_reference"]["anchor_type"] == "LONDON_LATE_0530_REFERENCE"


def test_london_late_mes_short_accepted_intent_produces_broker_envelope(tmp_path: Path) -> None:
    result = build_london_active_evidence_broker_envelope(
        lane_id="mes_london_late_active_participation_short",
        order_intent=_intent(OrderIntentType.SELL_TO_OPEN),
        rule_report=_rule_report(anchor_type="LONDON_LATE_0530_REFERENCE"),
        source_candle_timestamp=NOW,
        config=LondonActiveEvidenceBrokerEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.classification == BRIDGE_SUBMIT_CAPABLE_CLASSIFICATION
    assert result.envelope is not None
    assert result.envelope["broker_submit_enabled"] is True
    assert result.envelope["submit_allowed"] is True
    assert result.envelope["session"] == "LONDON_LATE"
    assert result.envelope["action"] == "SELL"
    assert result.envelope["localSymbol"] == "MESM6"
    assert result.envelope["conId"] == 770561194
    assert result.envelope["anchor_reference"]["anchor_type"] == "LONDON_LATE_0530_REFERENCE"


def test_no_setup_or_rejected_intent_produces_no_broker_envelope(tmp_path: Path) -> None:
    result = build_london_active_evidence_broker_envelope(
        lane_id="mnq_london_open_active_participation_long",
        order_intent=None,
        rule_report=_rule_report(anchor_type="LONDON_0300_OPEN"),
        source_candle_timestamp=NOW,
        config=LondonActiveEvidenceBrokerEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.classification == NO_ENVELOPE_NO_ACCEPTED_INTENT
    assert result.envelope is None


def test_anchor_not_ready_blocks_envelope_without_ibkr_path(tmp_path: Path) -> None:
    result = build_london_active_evidence_broker_envelope(
        lane_id="mnq_london_open_active_participation_short",
        order_intent=_intent(OrderIntentType.SELL_TO_OPEN),
        rule_report={
            **_rule_report(anchor_type="LONDON_0300_OPEN"),
            "session_anchor_status": "NOT_READY",
            "session_anchor_reason_code": "ANCHOR_BAR_NOT_FOUND",
        },
        source_candle_timestamp=NOW,
        config=LondonActiveEvidenceBrokerEnvelopeConfig(repo_root=tmp_path),
        generated_at=NOW,
    )

    assert result.classification == ANCHOR_NOT_READY
    assert result.reason_code == "ANCHOR_BAR_NOT_FOUND"
    assert result.envelope is None


def test_approved_london_lanes_register_as_submit_capable() -> None:
    for lane_id in (
        "mnq_london_open_active_participation_long",
        "mnq_london_open_active_participation_short",
        "mes_london_open_active_participation_long",
        "mes_london_open_active_participation_short",
        "mnq_london_late_active_participation_short",
        "mes_london_late_active_participation_short",
    ):
        adapter = lane_submit_bridge_adapter(lane_id=lane_id)

        assert adapter is not None
        assert adapter["current_order_destination"] == "ibkr_paper_bridge_submit_capable"
        assert adapter["entry_execution_intent"] == "PARTICIPATE_NOW"
        assert adapter["entry_execution_policy"] == "MARKETABLE_LIMIT_FROM_RUNTIME_TAPE"
        assert adapter["entry_marketable_limit_offset_ticks"] == 4


def _intent(intent_type: OrderIntentType) -> OrderIntent:
    return OrderIntent(
        order_intent_id=f"MNQ|1m|{NOW.isoformat()}|{intent_type.value}",
        bar_id=f"MNQ|1m|{NOW.isoformat()}",
        symbol="MNQ",
        intent_type=intent_type,
        quantity=1,
        created_at=NOW,
        reason_code="PAPER_ACTIVE_EVIDENCE_MNQ_LONDON_OPEN_PARTICIPATION_LONG_V1",
        signal_id="signal-1",
    )


def _rule_report(*, anchor_type: str) -> dict[str, object]:
    return {
        "classification": "TRACK_B_PAPER_ACTIVE_EVIDENCE_ACCEPTED",
        "primary_blocker": None,
        "session_anchor_status": "READY",
        "session_anchor_reason_code": "ANCHOR_READY_FROM_CANONICAL_ARTIFACT",
        "session_anchor_source": "RECOVERED_PHASE1_1M",
        "session_anchor_source_artifact_path": f"outputs/track_b_execution_core/session_anchors/MNQ/2026-06-03/{anchor_type}.json",
        "session_open_price": "21000",
        "current_bar_end_et": NOW.isoformat(),
    }
