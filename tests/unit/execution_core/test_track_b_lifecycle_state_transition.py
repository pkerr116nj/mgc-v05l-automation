from __future__ import annotations

from mgc_v05l.execution_core.track_b_lifecycle_state_transition import (
    BLOCKED_NO_BROKER_EFFECT,
    BROKER_BACKED_FILL_EVIDENCE_INCOMPLETE,
    BROKER_BACKED_FILL_EVIDENCE_COMPLETE,
    CLOSED_FLAT,
    OPEN_MANAGED,
    OPEN_MANAGED_METADATA_INCOMPLETE,
    REVIEW_REQUIRED,
    TRACK_B_STRATEGY_PAPER_OPEN_MANAGED,
    broker_backed_fill_evidence_complete,
    classify_managed_position_transition,
    classify_transition,
    is_clean_trade_stat_eligible,
    is_registry_eligible,
    is_terminal_state,
    ledger_projection_from_transition,
    lifecycle_state_matrix,
    normalize_no_broker_effect_result,
    requires_close_fill_or_broker_flat_proof,
    requires_operator_action,
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
    assert projection["final_position_status"] == BLOCKED_NO_BROKER_EFFECT
    assert projection["review_required"] is False
    assert projection["managed_position_registry_allowed"] is False


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


def test_closed_flat_with_fill_projection_remains_terminal_flat() -> None:
    result = classify_managed_position_transition(
        {
            "requested_lifecycle_status": CLOSED_FLAT,
            "intent_type": "SELL_TO_CLOSE",
            "broker_order_id": "20",
            "perm_id": "1948384228",
            "fill_price": "4543",
            "fill_timestamp": "2026-05-21T20:21:35.305374+00:00",
        }
    )
    projection = ledger_projection_from_transition(transition=result)

    assert result.classification == CLOSED_FLAT
    assert projection["final_position_status"] == CLOSED_FLAT
    assert projection["review_required"] is False


def test_closed_flat_with_broker_flat_proof_is_accepted() -> None:
    result = classify_managed_position_transition(
        {
            "requested_lifecycle_status": CLOSED_FLAT,
            "broker_flat_proof": True,
            "close_order_id": "32",
            "contract": "MGCM6",
        }
    )

    assert result.classification == CLOSED_FLAT
    assert result.reconciliation_clean_eligible is True


def test_closed_flat_without_evidence_is_rejected() -> None:
    result = classify_managed_position_transition({"requested_lifecycle_status": CLOSED_FLAT})

    assert result.classification == REVIEW_REQUIRED
    assert result.blockers == ("close_fill_or_broker_flat_proof",)


def test_review_required_blocks_clean_managed_position_state() -> None:
    result = classify_managed_position_transition(
        {"requested_lifecycle_status": REVIEW_REQUIRED, "review_blockers": ("missing_exec_details",)}
    )
    projection = ledger_projection_from_transition(transition=result)

    assert result.classification == REVIEW_REQUIRED
    assert result.managed_position_registry_allowed is True
    assert result.reconciliation_clean_eligible is False
    assert projection["review_required"] is True


def test_lifecycle_state_matrix_exposes_control_plane_semantics() -> None:
    matrix = lifecycle_state_matrix()

    assert matrix[OPEN_MANAGED]["managed_position_registry_allowed"] is True
    assert matrix[OPEN_MANAGED]["broker_backed"] is True
    assert "managed_exit_policy_id" in matrix[OPEN_MANAGED]["required_evidence"]
    assert matrix[BLOCKED_NO_BROKER_EFFECT]["terminal"] is True
    assert matrix[BLOCKED_NO_BROKER_EFFECT]["managed_position_registry_allowed"] is False
    assert matrix[CLOSED_FLAT]["required_evidence"] == ["close_fill_or_broker_flat_proof"]


def test_lifecycle_state_helpers_expose_matrix_semantics() -> None:
    assert is_registry_eligible(OPEN_MANAGED) is True
    assert is_registry_eligible(BLOCKED_NO_BROKER_EFFECT) is False
    assert is_terminal_state(BLOCKED_NO_BROKER_EFFECT) is True
    assert requires_operator_action(REVIEW_REQUIRED) is True
    assert is_clean_trade_stat_eligible("MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT") is False
    assert requires_close_fill_or_broker_flat_proof(CLOSED_FLAT) is True


def test_classify_transition_rejects_unknown_or_invalid_writer_states() -> None:
    unknown = classify_transition(
        current_state=OPEN_MANAGED,
        target_state="SOME_NEW_LOCAL_STATE",
        evidence={},
    )
    invalid = classify_transition(
        current_state=BLOCKED_NO_BROKER_EFFECT,
        target_state=OPEN_MANAGED,
        evidence=complete_open_managed_evidence(),
    )

    assert unknown.allowed is False
    assert "unknown_target_state" in unknown.blockers
    assert invalid.allowed is False
    assert "transition_not_allowed" in invalid.blockers


def test_malformed_manual_cleanup_is_terminal_but_excluded_from_clean_trade_stats() -> None:
    result = classify_managed_position_transition(
        {"requested_lifecycle_status": "MALFORMED_BROKER_BACKED_MANUALLY_RECONCILED_ARTIFACT"}
    )
    projection = ledger_projection_from_transition(transition=result)

    assert result.terminal is True
    assert result.reconciliation_clean_eligible is True
    assert result.clean_trade_stats_allowed is False
    assert projection["clean_trade_stats_allowed"] is False
