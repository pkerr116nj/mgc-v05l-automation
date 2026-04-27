"""Validation helpers for provisional TradeStation futures contract metadata."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REQUIRED_SYMBOLS = ("ES", "MES", "NQ", "MNQ")
REQUIRED_FIELDS = (
    "internal_symbol",
    "broker_symbol",
    "root",
    "asset_class",
    "exchange",
    "expiry",
    "multiplier",
    "tick_size",
    "tick_value",
    "currency",
    "notes",
    "verification_status",
)
NUMERIC_FIELDS = ("multiplier", "tick_size", "tick_value")


class TradeStationSymbolMapValidationError(ValueError):
    """Raised when a TradeStation futures symbol map is malformed."""


@dataclass(frozen=True)
class TradeStationFuturesSymbolMapValidationResult:
    payload: dict[str, dict[str, Any]]
    validated_symbols: tuple[str, ...]


def load_tradestation_futures_symbol_map(path: Path) -> TradeStationFuturesSymbolMapValidationResult:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return validate_tradestation_futures_symbol_map_payload(payload)


def validate_tradestation_futures_symbol_map_payload(
    payload: dict[str, Any],
) -> TradeStationFuturesSymbolMapValidationResult:
    if not isinstance(payload, dict):
        raise TradeStationSymbolMapValidationError("TradeStation futures symbol map must be a JSON object.")

    normalized: dict[str, dict[str, Any]] = {}
    missing_symbols = [symbol for symbol in REQUIRED_SYMBOLS if symbol not in payload]
    if missing_symbols:
        raise TradeStationSymbolMapValidationError(
            f"TradeStation futures symbol map is missing required symbols: {', '.join(missing_symbols)}."
        )

    for symbol in REQUIRED_SYMBOLS:
        row = payload.get(symbol)
        if not isinstance(row, dict):
            raise TradeStationSymbolMapValidationError(f"TradeStation symbol row for {symbol} must be an object.")
        missing_fields = [field for field in REQUIRED_FIELDS if field not in row]
        if missing_fields:
            raise TradeStationSymbolMapValidationError(
                f"TradeStation symbol row for {symbol} is missing required fields: {', '.join(missing_fields)}."
            )
        if str(row["internal_symbol"]).strip().upper() != symbol:
            raise TradeStationSymbolMapValidationError(
                f"TradeStation symbol row for {symbol} must declare internal_symbol={symbol}."
            )
        if str(row["asset_class"]).strip().upper() != "FUTURE":
            raise TradeStationSymbolMapValidationError(
                f"TradeStation symbol row for {symbol} must declare asset_class=FUTURE."
            )
        if not str(row["verification_status"]).strip():
            raise TradeStationSymbolMapValidationError(
                f"TradeStation symbol row for {symbol} must include a non-empty verification_status."
            )
        for field in NUMERIC_FIELDS:
            value = row[field]
            if isinstance(value, bool):
                raise TradeStationSymbolMapValidationError(
                    f"TradeStation symbol row for {symbol} field {field} must be numeric, not boolean."
                )
            if not isinstance(value, int | float):
                raise TradeStationSymbolMapValidationError(
                    f"TradeStation symbol row for {symbol} field {field} must be numeric."
                )
        normalized[symbol] = dict(row)

    return TradeStationFuturesSymbolMapValidationResult(
        payload=normalized,
        validated_symbols=REQUIRED_SYMBOLS,
    )
