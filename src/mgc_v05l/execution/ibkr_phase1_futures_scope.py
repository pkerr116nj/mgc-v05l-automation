"""Shared phase-1 futures execution targets for IBKR paper routing."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

_SUPPORTED_SOURCE_INSTRUMENTS = {"GC", "MGC", "NQ", "MNQ", "ES", "MES", "ZT", "ZF", "ZN", "ZB"}
_SOURCE_TO_PHASE1_EXECUTION_SYMBOL = {
    "GC": "GC",
    "MGC": "MGC",
    "NQ": "NQ",
    "MNQ": "MNQ",
    "ES": "ES",
    "MES": "MES",
    "ZT": "ZT",
    "ZF": "ZF",
    "ZN": "ZN",
    "ZB": "ZB",
}
_FIXED_GOLD_CONTRACT_MONTH = "202606"
_RATES_TARGETS = {
    "ZT": {"multiplier": "2000", "friendly_name": "2-Year Treasury Note"},
    "ZF": {"multiplier": "1000", "friendly_name": "5-Year Treasury Note"},
    "ZN": {"multiplier": "1000", "friendly_name": "10-Year Treasury Note"},
    "ZB": {"multiplier": "1000", "friendly_name": "30-Year Treasury Bond"},
}


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
    if normalized == "GC":
        return {
            "symbol": "GC",
            "contract_month": contract_month or _FIXED_GOLD_CONTRACT_MONTH,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"GC {contract_month or _FIXED_GOLD_CONTRACT_MONTH}",
            "exchange": "COMEX",
            "currency": "USD",
            "multiplier": "100",
            "trading_class": "GC",
            "phase1_proxy_mode": "DIRECT",
        }
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
    if normalized == "NQ":
        resolved_month = contract_month or active_index_contract_month(now=now)
        return {
            "symbol": "NQ",
            "contract_month": resolved_month,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"NQ {resolved_month}",
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "20",
            "trading_class": "NQ",
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
    if normalized == "ES":
        resolved_month = contract_month or active_index_contract_month(now=now)
        return {
            "symbol": "ES",
            "contract_month": resolved_month,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"ES {resolved_month}",
            "exchange": "CME",
            "currency": "USD",
            "multiplier": "50",
            "trading_class": "ES",
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
    if normalized in _RATES_TARGETS:
        resolved_month = contract_month or active_index_contract_month(now=now)
        metadata = _RATES_TARGETS[normalized]
        return {
            "symbol": normalized,
            "contract_month": resolved_month,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"{normalized} {resolved_month}",
            "exchange": "CBOT",
            "currency": "USD",
            "multiplier": metadata["multiplier"],
            "trading_class": normalized,
            "phase1_proxy_mode": "DIRECT",
            "contract_family": str(metadata["friendly_name"]),
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
