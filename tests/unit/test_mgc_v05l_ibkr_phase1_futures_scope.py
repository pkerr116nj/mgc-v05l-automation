from __future__ import annotations

from datetime import date

from mgc_v05l.execution.ibkr_phase1_futures_scope import (
    active_index_contract_month,
    phase1_execution_symbol_for_source,
    phase1_execution_target_for_source,
)


def test_active_index_contract_month_uses_current_quarter() -> None:
    assert active_index_contract_month(date(2026, 4, 29)) == "202606"
    assert active_index_contract_month(date(2026, 6, 1)) == "202606"
    assert active_index_contract_month(date(2026, 12, 20)) == "202612"
    assert active_index_contract_month(date(2026, 12, 31)) == "202612"


def test_phase1_proxy_symbols_cover_gold_and_index_pairs() -> None:
    assert phase1_execution_symbol_for_source("GC") == "MGC"
    assert phase1_execution_symbol_for_source("MGC") == "MGC"
    assert phase1_execution_symbol_for_source("NQ") == "MNQ"
    assert phase1_execution_symbol_for_source("MNQ") == "MNQ"


def test_phase1_target_for_nq_source_uses_mnq_proxy() -> None:
    target = phase1_execution_target_for_source("NQ", now=date(2026, 4, 29))

    assert target["symbol"] == "MNQ"
    assert target["contract_month"] == "202606"
    assert target["exchange"] == "CME"
    assert target["multiplier"] == "2"
