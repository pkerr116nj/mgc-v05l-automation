from __future__ import annotations

from datetime import datetime, timezone

from mgc_v05l.execution_core.track_b_futures_contract_resolver import (
    CONTRACT_AMBIGUOUS,
    CONTRACT_ALLOWED,
    CONTRACT_DETAILS_STALE,
    CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED,
    CONTRACT_NEAR_EXPIRY,
    CONTRACT_ROLL_BLOCKED,
    FuturesContractResolverInput,
    evaluate_futures_contract_pre_submit,
    recommended_index_contract_month,
)

NOW = datetime(2026, 5, 29, 12, 0, tzinfo=timezone.utc)


def _report(
    *,
    symbol: str,
    expiry: str,
    con_id: int,
    local_symbol: str,
    updated_at: str = "2026-05-29T11:59:00+00:00",
    extra_details: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    detail = {
        "symbol": symbol,
        "expiry": expiry,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "exchange": "COMEX" if symbol in {"GC", "MGC"} else "CBOT" if symbol in {"ZT", "ZF", "ZN", "ZB"} else "CME",
        "currency": "USD",
        "multiplier": (
            "10"
            if symbol == "MGC"
            else "100"
            if symbol == "GC"
            else "2000"
            if symbol == "ZT"
            else "1000"
            if symbol in {"ZF", "ZN", "ZB"}
            else "2"
            if symbol == "MNQ"
            else "5"
        ),
        "updated_at": updated_at,
    }
    return {
        "ok": True,
        "qualified_contract": dict(detail),
        "qualified_contract_identifier": con_id,
        "api_contract_details": [detail, *list(extra_details or [])],
    }


def _target(
    *,
    symbol: str,
    contract_month: str,
    expiry: str | None,
    con_id: int | None,
    local_symbol: str | None,
) -> dict[str, object]:
    return {
        "symbol": symbol,
        "contract_month": contract_month,
        "expiry": expiry,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "exchange": "COMEX" if symbol in {"GC", "MGC"} else "CBOT" if symbol in {"ZT", "ZF", "ZN", "ZB"} else "CME",
        "currency": "USD",
    }


def test_mgc_june_blocks_with_ibkr_warning_and_recommends_confirmed_august() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mgc_lane",
            symbol="MGC",
            contract_month="202606",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(
                symbol="MGC",
                contract_month="202606",
                expiry="20260626",
                con_id=712565978,
                local_symbol="MGCM6",
            ),
            qualified_contract_report=_report(symbol="MGC", expiry="20260626", con_id=712565978, local_symbol="MGCM6"),
            recommendation_contract_report=_report(symbol="MGC", expiry="20260826", con_id=800000001, local_symbol="MGCQ6"),
            ibkr_warnings=("IBKR warning: June contract is near expiry; consider August MGCQ6",),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_ROLL_BLOCKED
    assert result["submit_allowed"] is False
    assert result["recommended_contract"]["confirmed"] is True
    assert result["recommended_contract"]["contract_month"] == "202608"
    assert result["recommended_contract"]["local_symbol"] == "MGCQ6"


def test_gc_june_blocks_at_gold_roll_threshold_and_recommends_gcq6() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="gc_lane",
            symbol="GC",
            contract_month="202606",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(
                symbol="GC",
                contract_month="202606",
                expiry=None,
                con_id=None,
                local_symbol=None,
            ),
            qualified_contract_report=_report(symbol="GC", expiry="20260626", con_id=430360630, local_symbol="GCM6"),
            recommendation_contract_report=_report(symbol="GC", expiry="20260826", con_id=800000002, local_symbol="GCQ6"),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_NEAR_EXPIRY
    assert result["days_to_expiry"] == 28
    assert result["recommended_contract"]["confirmed"] is True
    assert result["recommended_contract"]["local_symbol"] == "GCQ6"


def test_mnq_and_mes_june_allowed_with_warning_only_roll_watch() -> None:
    cases = [
        ("MNQ", 770561201, "MNQM6"),
        ("MES", 770561194, "MESM6"),
    ]
    for symbol, con_id, local_symbol in cases:
        result = evaluate_futures_contract_pre_submit(
            FuturesContractResolverInput(
                strategy_id=f"{symbol.lower()}_lane",
                symbol=symbol,
                contract_month="202606",
                action="BUY",
                intent_type="BUY_TO_OPEN",
                selected_target=_target(
                    symbol=symbol,
                    contract_month="202606",
                    expiry=None,
                    con_id=None,
                    local_symbol=None,
                ),
                qualified_contract_report=_report(symbol=symbol, expiry="20260618", con_id=con_id, local_symbol=local_symbol),
                now=NOW,
            )
        )

        assert result["classification"] == CONTRACT_ALLOWED
        assert result["submit_allowed"] is True
        assert result["warning_only"] is True
        assert result["roll_status"] == "ROLL_WARNING_ONLY"
        assert result["days_to_expiry"] == 20


def test_index_near_expiry_uses_confirmed_next_contract_for_new_entry() -> None:
    now = datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc)
    cases = [
        ("MNQ", 770561201, "MNQM6", 880000201, "MNQU6"),
        ("MES", 770561194, "MESM6", 880000194, "MESU6"),
    ]
    for symbol, front_con_id, front_local, next_con_id, next_local in cases:
        result = evaluate_futures_contract_pre_submit(
            FuturesContractResolverInput(
                strategy_id=f"{symbol.lower()}_lane",
                symbol=symbol,
                contract_month="202606",
                action="BUY",
                intent_type="BUY_TO_OPEN",
                selected_target=_target(
                    symbol=symbol,
                    contract_month="202606",
                    expiry=None,
                    con_id=None,
                    local_symbol=None,
                ),
                qualified_contract_report=_report(
                    symbol=symbol,
                    expiry="20260618",
                    con_id=front_con_id,
                    local_symbol=front_local,
                    updated_at="2026-06-11T11:59:00+00:00",
                ),
                recommendation_contract_report=_report(
                    symbol=symbol,
                    expiry="20260918",
                    con_id=next_con_id,
                    local_symbol=next_local,
                    updated_at="2026-06-11T11:59:00+00:00",
                ),
                now=now,
            )
        )

        assert result["classification"] == CONTRACT_ALLOWED
        assert result["submit_allowed"] is True
        assert result["replacement_applied"] is True
        assert result["roll_status"] == "ROLL_REPLACED_NEAR_EXPIRY"
        assert result["original_selected_contract"]["local_symbol"] == front_local
        assert result["selected_contract"]["contract_month"] == "202609"
        assert result["selected_contract"]["local_symbol"] == next_local
        assert result["selected_contract"]["con_id"] == next_con_id


def test_index_near_expiry_blocks_when_no_replacement_confirmed() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mnq_lane",
            symbol="MNQ",
            contract_month="202606",
            action="SELL",
            intent_type="SELL_TO_OPEN",
            selected_target=_target(
                symbol="MNQ",
                contract_month="202606",
                expiry=None,
                con_id=None,
                local_symbol=None,
            ),
            qualified_contract_report=_report(
                symbol="MNQ",
                expiry="20260618",
                con_id=770561201,
                local_symbol="MNQM6",
                updated_at="2026-06-11T11:59:00+00:00",
            ),
            recommendation_contract_report={},
            now=datetime(2026, 6, 11, 12, 0, tzinfo=timezone.utc),
        )
    )

    assert result["classification"] == CONTRACT_NEAR_EXPIRY
    assert result["submit_allowed"] is False
    assert result["roll_status"] == "ROLL_BLOCKED_NEAR_EXPIRY"


def test_index_recommendation_rolls_to_next_quarter() -> None:
    assert recommended_index_contract_month("202606") == "202609"
    assert recommended_index_contract_month("202609") == "202612"
    assert recommended_index_contract_month("202612") == "202703"


def test_stale_contract_details_fail_closed() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mnq_lane",
            symbol="MNQ",
            contract_month="202606",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(symbol="MNQ", contract_month="202606", expiry=None, con_id=None, local_symbol=None),
            qualified_contract_report=_report(
                symbol="MNQ",
                expiry="20260618",
                con_id=770561201,
                local_symbol="MNQM6",
                updated_at="2026-05-27T11:59:00+00:00",
            ),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_DETAILS_STALE
    assert result["submit_allowed"] is False


def test_stale_hardcoded_con_id_or_local_symbol_fails_closed_as_ambiguous() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mgc_lane",
            symbol="MGC",
            contract_month="202606",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(
                symbol="MGC",
                contract_month="202606",
                expiry="20260626",
                con_id=999,
                local_symbol="STALEM6",
            ),
            qualified_contract_report=_report(symbol="MGC", expiry="20260626", con_id=712565978, local_symbol="MGCM6"),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_AMBIGUOUS
    assert result["submit_allowed"] is False


def test_ambiguous_contract_details_fail_closed() -> None:
    duplicate = {
        "symbol": "MNQ",
        "expiry": "20260618",
        "con_id": 770561201,
        "local_symbol": "MNQM6",
        "updated_at": "2026-05-29T11:59:00+00:00",
    }
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mnq_lane",
            symbol="MNQ",
            contract_month="202606",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(symbol="MNQ", contract_month="202606", expiry=None, con_id=None, local_symbol=None),
            qualified_contract_report=_report(
                symbol="MNQ",
                expiry="20260618",
                con_id=770561201,
                local_symbol="MNQM6",
                extra_details=[duplicate],
            ),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_AMBIGUOUS
    assert result["submit_allowed"] is False


def test_existing_lifecycle_exit_uses_original_contract_even_if_details_missing() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mgc_lane",
            symbol="MGC",
            contract_month="202606",
            action="SELL",
            intent_type="SELL_TO_CLOSE",
            selected_target=_target(
                symbol="MGC",
                contract_month="202606",
                expiry="20260626",
                con_id=712565978,
                local_symbol="MGCM6",
            ),
            qualified_contract_report={},
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED
    assert result["submit_allowed"] is True
    assert result["lifecycle_mutation_allowed"] is False


def test_rates_new_entry_allows_fresh_exact_contract_details() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="zb_us_active_participation_long",
            symbol="ZB",
            contract_month="202609",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(
                symbol="ZB",
                contract_month="202609",
                expiry="20260921",
                con_id=840227357,
                local_symbol="ZBU6",
            ),
            qualified_contract_report=_report(
                symbol="ZB",
                expiry="20260921",
                con_id=840227357,
                local_symbol="ZBU6",
                updated_at="2026-05-29T11:59:00+00:00",
            ),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_ALLOWED
    assert result["submit_allowed"] is True
    assert result["selected_contract"]["local_symbol"] == "ZBU6"
    assert result["selected_contract"]["con_id"] == 840227357
    assert result["product_family"] == "RATES"


def test_crypto_new_entry_allows_fresh_exact_contract_details() -> None:
    cases = [
        ("MBT", "202609", "20260925", 772435596, "MBTU6"),
        ("MET", "202609", "20260925", 772435602, "METU6"),
    ]
    for symbol, contract_month, expiry, con_id, local_symbol in cases:
        result = evaluate_futures_contract_pre_submit(
            FuturesContractResolverInput(
                strategy_id=f"{symbol.lower()}_us_active_participation_long",
                symbol=symbol,
                contract_month=contract_month,
                action="BUY",
                intent_type="BUY_TO_OPEN",
                selected_target=_target(
                    symbol=symbol,
                    contract_month=contract_month,
                    expiry=expiry,
                    con_id=con_id,
                    local_symbol=local_symbol,
                ),
                qualified_contract_report=_report(
                    symbol=symbol,
                    expiry=expiry,
                    con_id=con_id,
                    local_symbol=local_symbol,
                    updated_at="2026-05-29T11:59:00+00:00",
                ),
                now=NOW,
            )
        )

        assert result["classification"] == CONTRACT_ALLOWED
        assert result["submit_allowed"] is True
        assert result["selected_contract"]["local_symbol"] == local_symbol
        assert result["selected_contract"]["con_id"] == con_id
        assert result["product_family"] == "CRYPTO"


def test_unsupported_symbol_still_fails_closed() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="unsupported_us_active_participation_long",
            symbol="ABC",
            contract_month="202609",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target=_target(
                symbol="ABC",
                contract_month="202609",
                expiry="20260925",
                con_id=123,
                local_symbol="ABCU6",
            ),
            qualified_contract_report=_report(
                symbol="ABC",
                expiry="20260925",
                con_id=123,
                local_symbol="ABCU6",
                updated_at="2026-05-29T11:59:00+00:00",
            ),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_AMBIGUOUS
    assert result["submit_allowed"] is False


def test_new_short_entry_resolves_independently_from_exit_policy() -> None:
    result = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mes_short_lane",
            symbol="MES",
            contract_month="202606",
            action="SELL",
            intent_type="SELL_TO_OPEN",
            selected_target=_target(symbol="MES", contract_month="202606", expiry=None, con_id=None, local_symbol=None),
            qualified_contract_report=_report(symbol="MES", expiry="20260618", con_id=770561194, local_symbol="MESM6"),
            now=NOW,
        )
    )

    assert result["classification"] == CONTRACT_ALLOWED
    assert result["submit_allowed"] is True
    assert result["broker_mutation_allowed"] is False
