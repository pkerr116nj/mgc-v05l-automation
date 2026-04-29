"""Shared phase-1 futures execution targets for IBKR paper routing."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

_SUPPORTED_SOURCE_INSTRUMENTS = {"GC", "MGC", "NQ", "MNQ", "ES", "MES"}
_SOURCE_TO_PHASE1_EXECUTION_SYMBOL = {
    "GC": "MGC",
    "MGC": "MGC",
    "NQ": "MNQ",
    "MNQ": "MNQ",
    "ES": "MES",
    "MES": "MES",
}
_FIXED_GOLD_CONTRACT_MONTH = "202606"


def active_index_contract_month(now: date | datetime | None = None) -> str:
    anchor = now.date() if isinstance(now, datetime) else (now or date.today())
    quarter_months = (3, 6, 9, 12)
    for month in quarter_months:
        if anchor.month <= month:
            return f"{anchor.year:04d}{month:02d}"
    return f"{anchor.year + 1:04d}03"


def supported_phase1_source_instruments() -> set[str]:
    return set(_SUPPORTED_SOURCE_INSTRUMENTS)


def phase1_execution_symbol_for_source(instrument: str) -> str | None:
    normalized = str(instrument or "").strip().upper()
    return _SOURCE_TO_PHASE1_EXECUTION_SYMBOL.get(normalized)


def phase1_execution_target_for_symbol(
    symbol: str,
    *,
    contract_month: str | None = None,
    now: date | datetime | None = None,
) -> dict[str, Any]:
    normalized = str(symbol or "").strip().upper()
    if normalized == "MGC":
        return {
            "symbol": "MGC",
            "contract_month": contract_month or _FIXED_GOLD_CONTRACT_MONTH,
            "expiry": "20260626",
            "con_id": 712565978,
            "local_symbol": "MGCM6",
            "friendly_label": "MGC 202606",
            "exchange": "COMEX",
            "currency": "USD",
            "multiplier": "10",
            "trading_class": "MGC",
            "phase1_proxy_mode": "DIRECT",
        }
    if normalized == "MNQ":
        resolved_month = contract_month or active_index_contract_month(now=now)
        return {
            "symbol": "MNQ",
            "contract_month": resolved_month,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"MNQ {resolved_month}",
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "2",
            "trading_class": "MNQ",
            "phase1_proxy_mode": "DIRECT",
        }
    if normalized == "MES":
        resolved_month = contract_month or active_index_contract_month(now=now)
        return {
            "symbol": "MES",
            "contract_month": resolved_month,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"MES {resolved_month}",
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "5",
            "trading_class": "MES",
            "phase1_proxy_mode": "DIRECT",
        }
    raise KeyError(f"Unsupported phase-1 execution symbol: {symbol}")


def phase1_execution_target_for_source(
    instrument: str,
    *,
    now: date | datetime | None = None,
) -> dict[str, Any] | None:
    execution_symbol = phase1_execution_symbol_for_source(instrument)
    if execution_symbol is None:
        return None
    return phase1_execution_target_for_symbol(execution_symbol, now=now)
