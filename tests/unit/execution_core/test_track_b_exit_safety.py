from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mgc_v05l.execution_core.track_b_exit_safety import (
    classify_exit_attempt_policy,
    classify_managed_exit_working_order,
)


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


def test_hard_protective_sell_limit_above_market_after_timeout_requires_reprice() -> None:
    now = datetime(2026, 5, 15, 9, 30, tzinfo=UTC)

    policy = classify_managed_exit_working_order(
        order={
            "action": "SELL",
            "order_type": "LMT",
            "limit_price": "4574.7",
            "submitted_at": (now - timedelta(minutes=10)).isoformat(),
            "exit_reason": "forced_session_initial_stop",
            "status": "Submitted",
        },
        now=now,
        runtime_market_reference=4556.9,
        runtime_market_reference_source="runtime/GC/1m",
    )

    assert policy.classification == "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED"
    assert policy.hard_exit is True
    assert policy.marketable_by_runtime_context is False
    assert policy.stale_by_policy is True
    assert policy.recommended_action == "PREPARE_EXACT_CANCEL_REPLACE_FOR_KNOWN_MANAGED_ORDER"


def test_hard_protective_buy_limit_below_market_after_timeout_requires_reprice() -> None:
    now = datetime(2026, 5, 15, 9, 30, tzinfo=UTC)

    policy = classify_managed_exit_working_order(
        order={
            "action": "BUY",
            "order_type": "LMT",
            "limit_price": "100.0",
            "submitted_at": (now - timedelta(minutes=10)).isoformat(),
            "exit_reason": "SHORT_INTEGRITY_FAIL",
            "status": "Submitted",
        },
        now=now,
        runtime_market_reference=101.0,
    )

    assert policy.classification == "KNOWN_MANAGED_HARD_EXIT_ORDER_REPRICE_REQUIRED"
    assert policy.marketable_by_runtime_context is False


def test_hard_protective_working_order_inside_timeout_is_normal() -> None:
    now = datetime(2026, 5, 15, 9, 30, tzinfo=UTC)

    policy = classify_managed_exit_working_order(
        order={
            "action": "SELL",
            "order_type": "LMT",
            "limit_price": "99.0",
            "submitted_at": (now - timedelta(seconds=30)).isoformat(),
            "exit_reason": "LONG_STOP",
            "status": "Submitted",
        },
        now=now,
        runtime_market_reference=100.0,
    )

    assert policy.classification == "KNOWN_MANAGED_EXIT_ORDER_WORKING_NORMAL"
    assert policy.marketable_by_runtime_context is True
    assert policy.stale_by_policy is False


def test_discretionary_passive_exit_does_not_hard_reprice() -> None:
    now = datetime(2026, 5, 15, 9, 30, tzinfo=UTC)

    policy = classify_managed_exit_working_order(
        order={
            "action": "SELL",
            "order_type": "LMT",
            "limit_price": "105.0",
            "submitted_at": (now - timedelta(minutes=20)).isoformat(),
            "exit_reason": "profit_taking_limit",
            "hard_exit": False,
            "status": "Submitted",
        },
        now=now,
        runtime_market_reference=100.0,
    )

    assert policy.hard_exit is False
    assert policy.classification == "KNOWN_MANAGED_EXIT_ORDER_STALE_REVIEW"
    assert policy.recommended_action == "REVIEW_DISCRETIONARY_WORKING_EXIT_ORDER"
