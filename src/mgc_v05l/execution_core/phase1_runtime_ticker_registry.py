"""Phase-1 HOT runtime ticker registry.

This module defines runtime market-data expectations only. It does not approve
strategy lanes, submit authority, or live-money eligibility.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

from mgc_v05l.phase1_futures_scope import (
    phase1_execution_symbol_for_source,
    phase1_execution_target_for_source,
    supported_phase1_source_instruments,
)
from mgc_v05l.execution_core.track_b_live_market_data_symbols import active_phase1_runtime_symbols

PHASE1_RUNTIME_TICKER_ORDER = active_phase1_runtime_symbols()
PHASE1_RUNTIME_TIMEFRAMES = ("1m", "3m", "5m")
PHASE1_RUNTIME_DERIVED_FEATURES = (
    "close_to_close_pressure",
    "candle_body",
    "candle_range",
    "close_location_in_range",
    "slope",
    "curvature",
    "second_derivative",
    "micro_trend_participation",
)
ROLLING_RETENTION_BY_TIMEFRAME = {
    "1m": "5_trading_days",
    "3m": "5_trading_days",
    "5m": "10_trading_days",
    "latest_state": "current_latest_plus_short_backups",
}


ContractType = Literal["full_size", "micro", "rates"]


@dataclass(frozen=True)
class Phase1RuntimeTicker:
    symbol: str
    contract_type: ContractType
    source_symbol_supported: bool
    reference_symbol_supported: bool
    executable_target_supported: bool
    executable_target_symbol: str
    intended_timeframes: tuple[str, ...]
    rolling_retention_policy: dict[str, str]
    completed_candles_only: bool
    derived_feature_names: tuple[str, ...]
    live_money_eligible: bool
    strategy_approved: bool

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def phase1_runtime_ticker_registry() -> dict[str, Phase1RuntimeTicker]:
    supported_symbols = supported_phase1_source_instruments()
    registry: dict[str, Phase1RuntimeTicker] = {}
    for symbol in PHASE1_RUNTIME_TICKER_ORDER:
        target = phase1_execution_target_for_source(symbol)
        registry[symbol] = Phase1RuntimeTicker(
            symbol=symbol,
            contract_type=_contract_type(symbol),
            source_symbol_supported=symbol in supported_symbols,
            reference_symbol_supported=symbol in supported_symbols,
            executable_target_supported=target is not None,
            executable_target_symbol=phase1_execution_symbol_for_source(symbol) or "",
            intended_timeframes=PHASE1_RUNTIME_TIMEFRAMES,
            rolling_retention_policy=dict(ROLLING_RETENTION_BY_TIMEFRAME),
            completed_candles_only=True,
            derived_feature_names=PHASE1_RUNTIME_DERIVED_FEATURES,
            live_money_eligible=False,
            strategy_approved=False,
        )
    return registry


def phase1_runtime_ticker_rows() -> list[dict[str, Any]]:
    registry = phase1_runtime_ticker_registry()
    return [registry[symbol].as_dict() for symbol in PHASE1_RUNTIME_TICKER_ORDER]


def _contract_type(symbol: str) -> ContractType:
    if symbol in {"MGC", "MNQ", "MES", "MBT", "MET", "MSL"}:
        return "micro"
    if symbol in {"ZT", "ZF", "ZN", "ZB"}:
        return "rates"
    return "full_size"
