from __future__ import annotations

from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    BLOCKED_NO_BROKER_EFFECT,
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    BROKER_BACKED_FILL_EVIDENCE_COMPLETE,
    CLOSED_FLAT,
    OPEN_MANAGED,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    TRACK_B_STRATEGY_PAPER_OPEN_MANAGED,
    broker_backed_fill_evidence_complete,
    classify_managed_position_transition,
    ledger_projection_from_transition,
    normalize_no_broker_effect_result,
    validate_open_managed_evidence,
)


def complete_open_managed_evidence() -> dict[str, object]:
    return {
        "requested_lifecycle_status": OPEN_MANAGED,
        "entry_intent_id": "MGC|1m|2026-05-22T08:10:00Z|BUY_TO_OPEN",
        "lane_id": "track_b_paper_execution_test_mule_v1__mgc",
        "strategy_id": "track_b_paper_execution_test_mule_v1__mgc",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "side": "LONG",
        "quantity": 1,
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "lifecycle_id": "bridge_fill_MGC|1m|2026-05-22T08:10:00Z|BUY_TO_OPEN",
        "broker_order_id": "1",
        "perm_id": 347068086,
        "fill_price": "4528.8",
        "fill_timestamp": "2026-05-22T08:12:07.530547+00:00",
        "runtime_instance_id": "runtime-1",
        "source_commit": "abc123",
        "config_fingerprint": "cfg",
    }


def test_open_managed_requires_complete_broker_fill_evidence() -> None:
    evidence = complete_open_managed_evidence()
    evidence["fill_timestamp"] = None

    result = validate_open_managed_evidence(evidence)

    assert result.classification == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE
    assert result.blockers == ("fill_timestamp",)
    assert result.open_managed_allowed is False


def test_open_managed_requires_management_metadata() -> None:
    evidence = complete_open_managed_evidence()
    evidence["managed_exit_policy_id"] = ""

    result = validate_open_managed_evidence(evidence)

    assert result.classification == OPEN_MANAGED_METADATA_INCOMPLETE
    assert result.blockers == ("managed_exit_policy_id",)


def test_complete_broker_backed_fill_allows_open_managed() -> None:
    result = validate_open_managed_evidence(complete_open_managed_evidence())

    assert result.classification == OPEN_MANAGED
    assert result.open_managed_allowed is True
    assert result.paper_lifecycle_classification == TRACK_B_STRATEGY_PAPER_OPEN_MANAGED
    assert result.final_position_status == OPEN_MANAGED
    assert result.review_required is False


def test_no_broker_effect_becomes_terminal_non_lifecycle_state() -> None:
    result = normalize_no_broker_effect_result(
        {
            "classification": "PRE_SUBMIT_GATE_BLOCKED",
            "broker_effect_classification": "PRE_SUBMIT_BLOCKED_NO_BROKER_EFFECT",
            "submit_sent": False,
            "broker_order_id": None,
            "perm_id": None,
        }
    )

    assert result.classification == BLOCKED_NO_BROKER_EFFECT
    projection = ledger_projection_from_transition(transition=result)
    assert projection["final_position_status"] == "REVIEW_REQUIRED"
    assert projection["review_required"] is True


def test_classification_prefers_no_broker_effect_over_open_managed_request() -> None:
    evidence = complete_open_managed_evidence()
    evidence.update(
        {
            "submit_sent": False,
            "broker_order_id": None,
            "perm_id": None,
            "broker_effect_classification": "PRE_SUBMIT_BLOCKED_NO_BROKER_EFFECT",
        }
    )

    result = classify_managed_position_transition(evidence)

    assert result.classification == BLOCKED_NO_BROKER_EFFECT


def test_broker_fill_identity_helper_preserves_existing_classifications() -> None:
    complete = broker_backed_fill_evidence_complete(complete_open_managed_evidence())
    missing = broker_backed_fill_evidence_complete({"broker_order_id": "1"})

    assert complete.classification == BROKER_BACKED_FILL_EVIDENCE_COMPLETE
    assert missing.classification == BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE
    assert missing.blockers == ("fill_price", "fill_timestamp")


def test_closed_flat_projection_remains_terminal_flat() -> None:
    result = classify_managed_position_transition({"requested_lifecycle_status": CLOSED_FLAT})
    projection = ledger_projection_from_transition(transition=result)

    assert result.classification == CLOSED_FLAT
    assert projection["final_position_status"] == CLOSED_FLAT
    assert projection["review_required"] is False
