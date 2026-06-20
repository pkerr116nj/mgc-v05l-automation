"""Shared phase-1 futures execution targets."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

_SUPPORTED_SOURCE_INSTRUMENTS = {"GC", "MGC", "NQ", "MNQ", "ES", "MES", "ZT", "ZF", "ZN", "ZB", "PL", "BTC", "MBT"}
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
    "PL": "PL",
    "BTC": "BTC",
    "MBT": "MBT",
}
_FIXED_GOLD_CONTRACT_MONTH = "202606"
_PLATINUM_CONTRACT_MONTHS = (1, 4, 7, 10)
_RATES_TARGETS = {
    "ZT": {
        "multiplier": "2000",
        "friendly_name": "2-Year Treasury Note",
        "expiry": "20260930",
        "con_id": 842590391,
        "local_symbol": "ZTU6",
    },
    "ZF": {
        "multiplier": "1000",
        "friendly_name": "5-Year Treasury Note",
        "expiry": "20260930",
        "con_id": 842590380,
        "local_symbol": "ZFU6",
    },
    "ZN": {
        "multiplier": "1000",
        "friendly_name": "10-Year Treasury Note",
        "expiry": "20260921",
        "con_id": 840227361,
        "local_symbol": "ZNU6",
    },
    "ZB": {
        "multiplier": "1000",
        "friendly_name": "30-Year Treasury Bond",
        "expiry": "20260921",
        "con_id": 840227357,
        "local_symbol": "ZBU6",
    },
}
_CRYPTO_TARGETS = {
    "BTC": {
        "broker_symbol": "BRR",
        "multiplier": "5",
        "friendly_name": "Bitcoin",
        "expiry": "20260925",
        "con_id": 772435574,
        "local_symbol": "BTCU6",
        "trading_class": "BTC",
    },
    "MBT": {
        "broker_symbol": "MBT",
        "multiplier": "0.1",
        "friendly_name": "Micro Bitcoin",
        "expiry": "20260925",
        "con_id": 772435596,
        "local_symbol": "MBTU6",
        "trading_class": "MBT",
    },
}


def active_index_contract_month(now: date | datetime | None = None) -> str:
    anchor = now.date() if isinstance(now, datetime) else (now or date.today())
    quarter_months = (3, 6, 9, 12)
    for month in quarter_months:
        if anchor.month <= month:
            return f"{anchor.year:04d}{month:02d}"
    return f"{anchor.year + 1:04d}03"


def active_rates_contract_month(now: date | datetime | None = None) -> str:
    anchor = now.date() if isinstance(now, datetime) else (now or date.today())
    quarter_months = (3, 6, 9, 12)
    for month in quarter_months:
        if anchor.month < month:
            return f"{anchor.year:04d}{month:02d}"
        if anchor.month == month:
            next_index = quarter_months.index(month) + 1
            if next_index < len(quarter_months):
                return f"{anchor.year:04d}{quarter_months[next_index]:02d}"
            return f"{anchor.year + 1:04d}03"
    return f"{anchor.year + 1:04d}03"


def active_platinum_contract_month(now: date | datetime | None = None) -> str:
    anchor = now.date() if isinstance(now, datetime) else (now or date.today())
    for month in _PLATINUM_CONTRACT_MONTHS:
        if anchor.month <= month:
            return f"{anchor.year:04d}{month:02d}"
    return f"{anchor.year + 1:04d}{_PLATINUM_CONTRACT_MONTHS[0]:02d}"


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
        resolved_month = contract_month or active_rates_contract_month(now=now)
        metadata = _RATES_TARGETS[normalized]
        return {
            "symbol": normalized,
            "contract_month": resolved_month,
            "expiry": metadata["expiry"] if resolved_month == "202609" else None,
            "con_id": metadata["con_id"] if resolved_month == "202609" else None,
            "local_symbol": metadata["local_symbol"] if resolved_month == "202609" else None,
            "friendly_label": f"{normalized} {resolved_month}",
            "exchange": "CBOT",
            "currency": "USD",
            "multiplier": metadata["multiplier"],
            "trading_class": normalized,
            "phase1_proxy_mode": "DIRECT",
            "contract_family": str(metadata["friendly_name"]),
        }
    if normalized == "PL":
        resolved_month = contract_month or active_platinum_contract_month(now=now)
        return {
            "symbol": "PL",
            "contract_month": resolved_month,
            "expiry": None,
            "con_id": None,
            "local_symbol": None,
            "friendly_label": f"PL {resolved_month}",
            "exchange": "NYMEX",
            "currency": "USD",
            "multiplier": "50",
            "trading_class": "PL",
            "phase1_proxy_mode": "DIRECT",
            "contract_family": "Platinum",
        }
    if normalized in _CRYPTO_TARGETS:
        resolved_month = contract_month or active_rates_contract_month(now=now)
        metadata = _CRYPTO_TARGETS[normalized]
        return {
            "symbol": normalized,
            "broker_symbol": metadata["broker_symbol"],
            "contract_month": resolved_month,
            "expiry": metadata["expiry"] if resolved_month == "202609" else None,
            "con_id": metadata["con_id"] if resolved_month == "202609" else None,
            "local_symbol": metadata["local_symbol"] if resolved_month == "202609" else None,
            "friendly_label": f"{normalized} {resolved_month}",
            "exchange": "CME",
            "currency": "USD",
            "multiplier": metadata["multiplier"],
            "trading_class": metadata["trading_class"],
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
