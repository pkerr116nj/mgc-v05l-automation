from __future__ import annotations

from mgc_v05l.execution_core.track_b_exit_safety import classify_exit_attempt_policy


def _cancelled_event(lifecycle_id: str) -> dict[str, object]:
    return {
        "event_type": "delegated_manual_harness_completed",
        "delegated_classification": "PAPER_CLOSE_NOT_FILLED_CANCELLED",
        "caller_metadata": {"intent_type": "SELL_TO_CLOSE", "lifecycle_id": lifecycle_id},
    }


def test_hard_exit_not_filled_cancelled_escalates_execution_policy() -> None:
    policy = classify_exit_attempt_policy(
        history_events=[_cancelled_event("life-1")],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=1,
        broker_reconciled=True,
        open_order_count=0,
    )

    assert policy.not_filled_cancelled_count == 1
    assert policy.execution_policy == "HARD_AGGRESSIVE_LIMIT_4T"
    assert policy.escalation_level == 1
    assert policy.limit_offset_ticks == 4.0
    assert policy.block_submit is False


def test_repeated_cancelled_hard_exits_stop_before_indefinite_loop() -> None:
    policy = classify_exit_attempt_policy(
        history_events=[_cancelled_event("life-1"), _cancelled_event("life-1"), _cancelled_event("life-1")],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=1,
        broker_reconciled=True,
        open_order_count=0,
    )

    assert policy.execution_policy == "OPERATOR_REVIEW_REQUIRED"
    assert policy.block_submit is True
    assert policy.block_reason == "hard_exit_repeated_not_filled_cancelled_operator_review_required"


def test_discretionary_exit_does_not_escalate_to_operator_review() -> None:
    policy = classify_exit_attempt_policy(
        history_events=[_cancelled_event("life-1"), _cancelled_event("life-1"), _cancelled_event("life-1")],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=False,
        discretionary_exit=True,
        broker_position_quantity=1,
        broker_reconciled=True,
        open_order_count=0,
    )

    assert policy.execution_policy == "DISCRETIONARY_MARKETABLE_LIMIT_1T"
    assert policy.escalation_level == 0
    assert policy.block_submit is False


def test_working_order_broker_flat_and_mismatch_fail_closed() -> None:
    working = classify_exit_attempt_policy(
        history_events=[],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=1,
        broker_reconciled=True,
        open_order_count=1,
    )
    flat = classify_exit_attempt_policy(
        history_events=[],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=0,
        broker_reconciled=True,
        open_order_count=0,
    )
    mismatch = classify_exit_attempt_policy(
        history_events=[],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=1,
        broker_reconciled=False,
        open_order_count=0,
    )

    assert working.block_reason == "working_exit_order_present"
    assert flat.block_reason == "broker_truth_exact_contract_flat"
    assert mismatch.block_reason == "broker_lifecycle_reconciliation_not_clean"


def test_final_fill_status_is_retained_for_position_attempt_history() -> None:
    policy = classify_exit_attempt_policy(
        history_events=[
            _cancelled_event("life-1"),
            {
                "event_type": "delegated_manual_harness_completed",
                "delegated_classification": "PAPER_CLOSE_FILLED_FLAT",
                "caller_metadata": {"intent_type": "SELL_TO_CLOSE", "lifecycle_id": "life-1"},
            },
        ],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=1,
        broker_reconciled=True,
        open_order_count=0,
    )

    assert policy.exit_attempt_count == 2
    assert policy.not_filled_cancelled_count == 1
    assert policy.final_close_status == "FILLED_FLAT"


def test_policy_is_paper_only_metadata_and_has_no_live_money_eligibility() -> None:
    policy = classify_exit_attempt_policy(
        history_events=[],
        lifecycle_id="life-1",
        intent_type="SELL_TO_CLOSE",
        action="SELL",
        hard_exit=True,
        broker_position_quantity=1,
        broker_reconciled=True,
        open_order_count=0,
    ).to_json_dict()

    assert "live_money_eligible" not in policy
