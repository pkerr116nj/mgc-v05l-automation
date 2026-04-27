from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.execution.tradestation_symbol_map import (
    REQUIRED_SYMBOLS,
    TradeStationSymbolMapValidationError,
    load_tradestation_futures_symbol_map,
    validate_tradestation_futures_symbol_map_payload,
)


def _valid_payload() -> dict:
    return {
        "ES": {
            "internal_symbol": "ES",
            "broker_symbol": "ESM26",
            "root": "ES",
            "asset_class": "FUTURE",
            "exchange": "CME",
            "expiry": "202606",
            "multiplier": 50,
            "tick_size": 0.25,
            "tick_value": 12.5,
            "currency": "USD",
            "notes": "provisional",
            "verification_status": "provisional",
        },
        "MES": {
            "internal_symbol": "MES",
            "broker_symbol": "MESM26",
            "root": "MES",
            "asset_class": "FUTURE",
            "exchange": "CME",
            "expiry": "202606",
            "multiplier": 5,
            "tick_size": 0.25,
            "tick_value": 1.25,
            "currency": "USD",
            "notes": "provisional",
            "verification_status": "provisional",
        },
        "NQ": {
            "internal_symbol": "NQ",
            "broker_symbol": "NQM26",
            "root": "NQ",
            "asset_class": "FUTURE",
            "exchange": "CME",
            "expiry": "202606",
            "multiplier": 20,
            "tick_size": 0.25,
            "tick_value": 5.0,
            "currency": "USD",
            "notes": "provisional",
            "verification_status": "provisional",
        },
        "MNQ": {
            "internal_symbol": "MNQ",
            "broker_symbol": "MNQM26",
            "root": "MNQ",
            "asset_class": "FUTURE",
            "exchange": "CME",
            "expiry": "202606",
            "multiplier": 2,
            "tick_size": 0.25,
            "tick_value": 0.5,
            "currency": "USD",
            "notes": "provisional",
            "verification_status": "provisional",
        },
    }


def test_config_file_loads_and_validates() -> None:
    result = load_tradestation_futures_symbol_map(Path("config/tradestation_futures_symbols.json"))
    assert result.validated_symbols == REQUIRED_SYMBOLS
    assert result.payload["ES"]["multiplier"] == 50
    assert result.payload["MNQ"]["tick_value"] == 0.5


def test_validation_requires_all_four_symbols() -> None:
    payload = _valid_payload()
    payload.pop("MNQ")
    with pytest.raises(TradeStationSymbolMapValidationError, match="missing required symbols: MNQ"):
        validate_tradestation_futures_symbol_map_payload(payload)


def test_validation_requires_required_fields() -> None:
    payload = _valid_payload()
    payload["NQ"].pop("verification_status")
    with pytest.raises(TradeStationSymbolMapValidationError, match="missing required fields: verification_status"):
        validate_tradestation_futures_symbol_map_payload(payload)


def test_validation_requires_numeric_contract_fields() -> None:
    payload = _valid_payload()
    payload["MES"]["tick_size"] = "0.25"
    with pytest.raises(TradeStationSymbolMapValidationError, match="field tick_size must be numeric"):
        validate_tradestation_futures_symbol_map_payload(payload)


def test_validation_requires_futures_asset_class() -> None:
    payload = _valid_payload()
    payload["ES"]["asset_class"] = "STOCK"
    with pytest.raises(TradeStationSymbolMapValidationError, match="asset_class=FUTURE"):
        validate_tradestation_futures_symbol_map_payload(payload)


def test_validation_result_is_json_round_trip_safe(tmp_path: Path) -> None:
    path = tmp_path / "symbols.json"
    path.write_text(json.dumps(_valid_payload()), encoding="utf-8")
    result = load_tradestation_futures_symbol_map(path)
    reloaded = json.loads(json.dumps(result.payload))
    assert reloaded["NQ"]["verification_status"] == "provisional"
