from __future__ import annotations

from mgc_v05l.execution_core.phase1_runtime_ticker_registry import (
    PHASE1_RUNTIME_TICKER_ORDER,
    PHASE1_RUNTIME_TIMEFRAMES,
    phase1_runtime_ticker_registry,
    phase1_runtime_ticker_rows,
)


def test_registry_contains_exact_phase1_ticker_universe() -> None:
    registry = phase1_runtime_ticker_registry()

    assert tuple(registry) == PHASE1_RUNTIME_TICKER_ORDER
    assert set(registry) == {"GC", "NQ", "ES", "MGC", "MNQ", "MES", "ZT", "ZF", "ZN", "ZB", "PL"}
    assert "RTY" not in registry
    assert "AAPL" not in registry


def test_registry_classifies_full_size_micro_and_rates() -> None:
    registry = phase1_runtime_ticker_registry()

    assert {symbol for symbol, item in registry.items() if item.contract_type == "full_size"} == {"GC", "NQ", "ES", "PL"}
    assert {symbol for symbol, item in registry.items() if item.contract_type == "micro"} == {"MGC", "MNQ", "MES"}
    assert {symbol for symbol, item in registry.items() if item.contract_type == "rates"} == {"ZT", "ZF", "ZN", "ZB"}


def test_registry_defines_runtime_timeframes_and_retention_policy() -> None:
    for item in phase1_runtime_ticker_registry().values():
        assert item.intended_timeframes == PHASE1_RUNTIME_TIMEFRAMES
        assert item.rolling_retention_policy["1m"] == "5_trading_days"
        assert item.rolling_retention_policy["3m"] == "5_trading_days"
        assert item.rolling_retention_policy["5m"] == "10_trading_days"
        assert item.completed_candles_only is True


def test_registry_has_source_and_execution_support_without_strategy_approval() -> None:
    for item in phase1_runtime_ticker_registry().values():
        assert item.source_symbol_supported is True
        assert item.reference_symbol_supported is True
        assert item.executable_target_supported is True
        assert item.executable_target_symbol == item.symbol
        assert item.live_money_eligible is False
        assert item.strategy_approved is False


def test_registry_rows_are_serializable() -> None:
    rows = phase1_runtime_ticker_rows()

    assert len(rows) == 11
    assert rows[0]["symbol"] == "GC"
    assert rows[-1]["symbol"] == "PL"
    assert all(row["live_money_eligible"] is False for row in rows)
