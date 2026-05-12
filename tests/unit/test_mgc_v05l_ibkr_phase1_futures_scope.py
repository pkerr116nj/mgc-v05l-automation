from __future__ import annotations

from datetime import date

import pytest

from mgc_v05l.execution.ibkr_phase1_futures_scope import (
    active_index_contract_month,
    active_platinum_contract_month,
    phase1_execution_symbol_for_source,
    phase1_execution_target_for_symbol,
    phase1_execution_target_for_source,
    supported_phase1_source_instruments,
)


def test_active_index_contract_month_uses_current_quarter() -> None:
    assert active_index_contract_month(date(2026, 4, 29)) == "202606"
    assert active_index_contract_month(date(2026, 6, 1)) == "202606"
    assert active_index_contract_month(date(2026, 12, 20)) == "202612"
    assert active_index_contract_month(date(2026, 12, 31)) == "202612"


def test_active_platinum_contract_month_uses_pl_cycle() -> None:
    assert active_platinum_contract_month(date(2026, 5, 12)) == "202607"
    assert active_platinum_contract_month(date(2026, 10, 1)) == "202610"
    assert active_platinum_contract_month(date(2026, 11, 1)) == "202701"


def test_phase1_execution_symbols_cover_gold_and_index_pairs() -> None:
    assert phase1_execution_symbol_for_source("GC") == "GC"
    assert phase1_execution_symbol_for_source("MGC") == "MGC"
    assert phase1_execution_symbol_for_source("NQ") == "NQ"
    assert phase1_execution_symbol_for_source("MNQ") == "MNQ"
    assert phase1_execution_symbol_for_source("ES") == "ES"
    assert phase1_execution_symbol_for_source("MES") == "MES"
    assert phase1_execution_symbol_for_source("ZT") == "ZT"
    assert phase1_execution_symbol_for_source("ZF") == "ZF"
    assert phase1_execution_symbol_for_source("ZN") == "ZN"
    assert phase1_execution_symbol_for_source("ZB") == "ZB"
    assert phase1_execution_symbol_for_source("PL") == "PL"


def test_phase1_full_size_targets_are_direct_execution_targets() -> None:
    gc = phase1_execution_target_for_source("GC", now=date(2026, 4, 29))
    nq = phase1_execution_target_for_source("NQ", now=date(2026, 4, 29))
    es = phase1_execution_target_for_source("ES", now=date(2026, 4, 29))

    assert gc["symbol"] == "GC"
    assert gc["contract_month"] == "202606"
    assert gc["exchange"] == "COMEX"
    assert gc["multiplier"] == "100"
    assert gc["phase1_proxy_mode"] == "DIRECT"
    assert nq["symbol"] == "NQ"
    assert nq["contract_month"] == "202606"
    assert nq["exchange"] == "CME"
    assert nq["multiplier"] == "20"
    assert nq["phase1_proxy_mode"] == "DIRECT"
    assert es["symbol"] == "ES"
    assert es["contract_month"] == "202606"
    assert es["exchange"] == "CME"
    assert es["multiplier"] == "50"
    assert es["phase1_proxy_mode"] == "DIRECT"


def test_phase1_micro_targets_remain_direct_execution_targets() -> None:
    target = phase1_execution_target_for_source("MNQ", now=date(2026, 4, 29))

    assert target["symbol"] == "MNQ"
    assert target["contract_month"] == "202606"
    assert target["exchange"] == "CME"
    assert target["multiplier"] == "2"

    mnq = phase1_execution_target_for_source("MNQ", now=date(2026, 4, 29))
    mes = phase1_execution_target_for_source("MES", now=date(2026, 4, 29))
    assert mnq["symbol"] == "MNQ"
    assert mnq["multiplier"] == "2"
    assert mes["symbol"] == "MES"
    assert mes["multiplier"] == "5"


def test_phase1_rates_targets_are_direct_cbot_execution_targets() -> None:
    expected = {
        "ZT": ("2000", "2-Year Treasury Note"),
        "ZF": ("1000", "5-Year Treasury Note"),
        "ZN": ("1000", "10-Year Treasury Note"),
        "ZB": ("1000", "30-Year Treasury Bond"),
    }
    assert {"ZT", "ZF", "ZN", "ZB"}.issubset(supported_phase1_source_instruments())
    for symbol, (multiplier, contract_family) in expected.items():
        target = phase1_execution_target_for_source(symbol, now=date(2026, 4, 29))
        assert target["symbol"] == symbol
        assert target["contract_month"] == "202606"
        assert target["exchange"] == "CBOT"
        assert target["currency"] == "USD"
        assert target["multiplier"] == multiplier
        assert target["trading_class"] == symbol
        assert target["phase1_proxy_mode"] == "DIRECT"
        assert target["contract_family"] == contract_family


def test_phase1_platinum_target_is_direct_nymex_execution_target() -> None:
    assert "PL" in supported_phase1_source_instruments()

    target = phase1_execution_target_for_source("PL", now=date(2026, 5, 12))

    assert target["symbol"] == "PL"
    assert target["contract_month"] == "202607"
    assert target["exchange"] == "NYMEX"
    assert target["currency"] == "USD"
    assert target["multiplier"] == "50"
    assert target["trading_class"] == "PL"
    assert target["phase1_proxy_mode"] == "DIRECT"
    assert target["contract_family"] == "Platinum"


def test_equities_unsupported_rates_and_unapproved_futures_fail_closed() -> None:
    assert phase1_execution_symbol_for_source("AAPL") is None
    assert phase1_execution_symbol_for_source("UB") is None
    assert phase1_execution_symbol_for_source("ZQ") is None
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("AAPL")
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("UB")
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("ZQ")
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("RTY")
