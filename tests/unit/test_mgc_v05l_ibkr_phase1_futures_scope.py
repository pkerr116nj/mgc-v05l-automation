from __future__ import annotations

from datetime import date

import pytest

from mgc_v05l.execution.ibkr_phase1_futures_scope import (
    active_index_contract_month,
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


def test_phase1_execution_symbols_cover_gold_and_index_pairs() -> None:
    assert phase1_execution_symbol_for_source("GC") == "GC"
    assert phase1_execution_symbol_for_source("MGC") == "MGC"
    assert phase1_execution_symbol_for_source("NQ") == "NQ"
    assert phase1_execution_symbol_for_source("MNQ") == "MNQ"
    assert phase1_execution_symbol_for_source("ES") == "ES"
    assert phase1_execution_symbol_for_source("MES") == "MES"


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


def test_rates_equities_and_unapproved_futures_fail_closed() -> None:
    assert "ZN" not in supported_phase1_source_instruments()
    assert phase1_execution_symbol_for_source("AAPL") is None
    assert phase1_execution_symbol_for_source("ZN") is None
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("AAPL")
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("ZN")
    with pytest.raises(KeyError):
        phase1_execution_target_for_symbol("RTY")
