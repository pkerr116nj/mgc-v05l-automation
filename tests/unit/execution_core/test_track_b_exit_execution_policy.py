from __future__ import annotations

from mgc_v05l.execution_core.track_b_exit_execution_policy import (
    EXIT_CLASS_ALPHA_SEEKING,
    EXIT_CLASS_RISK_REDUCING,
    build_exit_limit_policy,
    classify_exit_execution,
)


def test_timebox_exit_classifies_as_risk_reducing() -> None:
    assert (
        classify_exit_execution(policy_id="GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1")
        == EXIT_CLASS_RISK_REDUCING
    )


def test_managed_position_maintenance_classifies_as_risk_reducing() -> None:
    assert classify_exit_execution(reason="managed_position_maintenance") == EXIT_CLASS_RISK_REDUCING


def test_risk_reducing_buy_to_close_prefers_fresh_ask() -> None:
    policy = build_exit_limit_policy(
        reference={
            "reference_price": "30000",
            "ask_price": "30001.25",
            "last_price": "30000",
            "reference_age_seconds": 1.0,
            "pricing_source": "DATABENTO_RUNTIME",
        },
        close_action="BUY",
        tick_size="0.25",
        stale_reference_seconds=60,
        execution_class=EXIT_CLASS_RISK_REDUCING,
    )

    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["execution_class"] == EXIT_CLASS_RISK_REDUCING
    assert policy["marketable_execution_required"] is True
    assert policy["passive_execution_allowed"] is False
    assert policy["reference_price_kind"] == "ask_price"
    assert policy["limit_price"] == "30001.25"


def test_risk_reducing_sell_to_close_without_bid_uses_fresh_last_minus_bounded_offset() -> None:
    policy = build_exit_limit_policy(
        reference={
            "reference_price": "30000",
            "last_price": "30000",
            "reference_age_seconds": 1.0,
            "pricing_source": "DATABENTO_RUNTIME",
        },
        close_action="SELL",
        tick_size="0.25",
        stale_reference_seconds=60,
        execution_class=EXIT_CLASS_RISK_REDUCING,
    )

    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["execution_class"] == EXIT_CLASS_RISK_REDUCING
    assert policy["marketable_execution_required"] is True
    assert policy["reference_price_kind"] == "last_price"
    assert policy["aggressive_paper_fallback"] is True
    assert policy["marketable_limit_offset_ticks"] == 8.0
    assert policy["limit_price"] == "29998"
    assert policy["price_deviation_from_reference"] == "2"


def test_risk_reducing_met_close_only_reference_prices_near_current_market() -> None:
    policy = build_exit_limit_policy(
        reference={
            "reference_price": "1660",
            "close": "1660",
            "reference_age_seconds": 1.0,
            "pricing_source": "DATABENTO_RUNTIME",
        },
        close_action="SELL",
        tick_size="0.5",
        stale_reference_seconds=60,
        execution_class=EXIT_CLASS_RISK_REDUCING,
    )

    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["limit_price"] == "1656"
    assert policy["marketable_limit_offset_ticks"] == 8.0


def test_risk_reducing_far_away_close_limit_fails_sanity() -> None:
    policy = build_exit_limit_policy(
        reference={
            "reference_price": "1660",
            "close": "1660",
            "reference_age_seconds": 1.0,
            "pricing_source": "DATABENTO_RUNTIME",
        },
        close_action="SELL",
        tick_size="0.5",
        stale_reference_seconds=60,
        execution_class=EXIT_CLASS_RISK_REDUCING,
        base_offset_ticks=400,
        max_slippage_ticks=None,
    )

    assert policy["classification"] == "MANAGED_CLOSE_PRICING_BLOCKED"
    assert policy["stale_reference_blocker"] == "MANAGED_CLOSE_PRICE_SANITY_DEVIATION"


def test_risk_reducing_exit_blocks_without_fresh_price() -> None:
    policy = build_exit_limit_policy(
        reference={"reference_price": "30000", "last_price": "30000", "reference_age_seconds": 120.0},
        close_action="BUY",
        tick_size="0.25",
        stale_reference_seconds=60,
        execution_class=EXIT_CLASS_RISK_REDUCING,
    )

    assert policy["classification"] == "MANAGED_CLOSE_PRICING_BLOCKED"
    assert policy["marketable_execution_required"] is True
    assert policy["passive_execution_allowed"] is False
    assert policy["stale_reference_blocker"] == "MANAGED_CLOSE_REFERENCE_STALE"


def test_alpha_seeking_exit_remains_passive() -> None:
    policy = build_exit_limit_policy(
        reference={"reference_price": "30000", "reference_age_seconds": 1.0},
        close_action="SELL",
        tick_size="0.25",
        stale_reference_seconds=60,
        execution_class=EXIT_CLASS_ALPHA_SEEKING,
        base_offset_ticks=2,
        max_slippage_ticks=16,
    )

    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["execution_class"] == EXIT_CLASS_ALPHA_SEEKING
    assert policy["marketable_execution_required"] is False
    assert policy["passive_execution_allowed"] is True
    assert policy["limit_price"] == "29999.5"
