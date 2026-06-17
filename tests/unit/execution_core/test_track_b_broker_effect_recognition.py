from __future__ import annotations

from mgc_v05l.execution_core.track_b_broker_effect_recognition import (
    BROKER_EFFECT_BLOCKED,
    BROKER_EFFECT_OBSERVED,
    BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT,
    recognize_managed_close_broker_effect,
)


def test_long_close_flat_broker_state_stops_second_sell() -> None:
    payload = recognize_managed_close_broker_effect(
        expected_signed_quantity=1,
        observed_signed_quantity=0,
        close_action="SELL",
        close_quantity=1,
    )

    assert payload["classification"] == BROKER_EFFECT_OBSERVED
    assert payload["effect_state"] == "EXPOSURE_FLAT"
    assert payload["retry_close_allowed"] is False
    assert payload["stop_without_submit"] is True


def test_short_close_flat_broker_state_stops_second_buy() -> None:
    payload = recognize_managed_close_broker_effect(
        expected_signed_quantity=-1,
        observed_signed_quantity=0,
        close_action="BUY",
        close_quantity=1,
    )

    assert payload["classification"] == BROKER_EFFECT_OBSERVED
    assert payload["effect_state"] == "EXPOSURE_FLAT"
    assert payload["retry_close_allowed"] is False


def test_original_exposure_present_allows_normal_exact_close_path() -> None:
    payload = recognize_managed_close_broker_effect(
        expected_signed_quantity=-1,
        observed_signed_quantity=-1,
        close_action="BUY",
        close_quantity=1,
    )

    assert payload["classification"] == BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT
    assert payload["retry_close_allowed"] is True


def test_same_side_excess_exposure_allows_exact_risk_reducing_close() -> None:
    payload = recognize_managed_close_broker_effect(
        expected_signed_quantity=1,
        observed_signed_quantity=2,
        close_action="SELL",
        close_quantity=1,
    )

    assert payload["classification"] == BROKER_EFFECT_ORIGINAL_EXPOSURE_PRESENT
    assert payload["effect_state"] == "SAME_SIDE_EXCESS_EXPOSURE_PRESENT"
    assert payload["retry_close_allowed"] is True


def test_same_side_excess_exposure_blocks_over_lifecycle_close() -> None:
    payload = recognize_managed_close_broker_effect(
        expected_signed_quantity=1,
        observed_signed_quantity=2,
        close_action="SELL",
        close_quantity=2,
    )

    assert payload["classification"] == BROKER_EFFECT_BLOCKED
    assert payload["effect_state"] == "CLOSE_QUANTITY_EXCEEDS_EXPECTED_EXPOSURE"
    assert "close_quantity_exceeds_expected_exposure" in payload["blockers"]


def test_unknown_orders_block_broker_effect_recognition() -> None:
    payload = recognize_managed_close_broker_effect(
        expected_signed_quantity=1,
        observed_signed_quantity=1,
        close_action="SELL",
        close_quantity=1,
        unknown_order_count=1,
    )

    assert payload["classification"] == BROKER_EFFECT_BLOCKED
    assert "unknown_orders_present" in payload["blockers"]
