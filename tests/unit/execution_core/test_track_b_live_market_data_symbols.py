from __future__ import annotations

from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
    MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS,
    MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED,
    SESSION_CALENDAR_CME_CRYPTO_FUTURES,
    SESSION_CALENDAR_GLOBEX_FUTURES,
    TrackBLiveMarketDataSymbolConfigError,
    load_track_b_live_market_data_symbols,
    parse_track_b_live_market_data_symbols,
)


def valid_payload() -> dict[str, object]:
    return {
        "version": 1,
        "symbols": [
            symbol_row("GC", execution_symbol="MGC", reference_symbol="GC", databento_symbol="GC.v.0"),
            symbol_row("MGC", execution_symbol="MGC", reference_symbol="GC", databento_symbol="MGC.v.0"),
            symbol_row("ES", execution_symbol="MES", reference_symbol="ES", databento_symbol="ES.v.0"),
            symbol_row("MES", execution_symbol="MES", reference_symbol="ES", databento_symbol="MES.v.0"),
            symbol_row("NQ", execution_symbol="MNQ", reference_symbol="NQ", databento_symbol="NQ.v.0"),
            symbol_row("MNQ", execution_symbol="MNQ", reference_symbol="NQ", databento_symbol="MNQ.v.0"),
            symbol_row(
                "ZT",
                execution_symbol="ZT",
                reference_symbol="ZT",
                databento_symbol="ZT.v.0",
                required_for_readiness=False,
            ),
        ],
    }


def symbol_row(symbol: str, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "symbol": symbol,
        "enabled": True,
        "required_for_readiness": True,
        "asset_class": "futures",
        "execution_symbol": symbol,
        "reference_symbol": symbol,
        "databento_symbol": f"{symbol}.v.0",
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "venue": "CME Globex",
        "timezone": "America/New_York",
        "min_confirmed_bars": 8,
        "freshness_threshold_seconds": 180,
        "notes": "test row",
    }
    row.update(overrides)
    return row


def test_loads_default_track_b_live_market_data_symbols_config() -> None:
    namelist = load_track_b_live_market_data_symbols()

    assert namelist.version == 1
    assert len(namelist.all_symbols()) == 17
    assert [row.symbol for row in namelist.required_for_readiness_symbols()] == ["GC", "MGC", "ES", "MES", "NQ", "MNQ"]
    assert [row.symbol for row in namelist.optional_symbols()] == [
        "ZT",
        "ZF",
        "ZN",
        "ZB",
        "PL",
        "BTC",
        "MBT",
        "ETH",
        "MET",
        "SOL",
        "MSL",
    ]
    assert "MNQ.v.0" in namelist.enabled_databento_symbols()
    assert namelist.by_symbol()["MGC"].execution_symbol == "MGC"
    assert namelist.by_symbol()["MGC"].reference_symbol == "GC"
    assert namelist.by_symbol()["MES"].reference_symbol == "ES"
    assert namelist.by_symbol()["MNQ"].reference_symbol == "NQ"
    assert namelist.by_symbol()["MNQ"].session_calendar == SESSION_CALENDAR_GLOBEX_FUTURES
    assert namelist.by_symbol()["MNQ"].market_freshness_policy == MARKET_FRESHNESS_POLICY_LIQUID_TRADE_BARS
    for symbol in ("MBT", "MET", "MSL"):
        row = namelist.by_symbol()[symbol]
        assert row.session_calendar == SESSION_CALENDAR_CME_CRYPTO_FUTURES
        assert row.market_freshness_policy == MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED
        assert row.latest_bar_freshness_seconds == 3600
    assert namelist.by_symbol()["PL"].session_calendar == SESSION_CALENDAR_GLOBEX_FUTURES
    assert namelist.by_symbol()["PL"].market_freshness_policy == MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED
    assert namelist.by_symbol()["PL"].latest_bar_freshness_seconds == 3600


def test_yaml_loader_reports_disabled_symbols_but_ignores_missing_disabled_databento_mapping(tmp_path: Path) -> None:
    config_path = tmp_path / "symbols.yaml"
    config_path.write_text(
        "\n".join(
            [
                "version: 1",
                "symbols:",
                "  - symbol: GC",
                "    enabled: true",
                "    required_for_readiness: true",
                "    asset_class: futures",
                "    execution_symbol: MGC",
                "    reference_symbol: GC",
                "    databento_symbol: GC.v.0",
                "    dataset: GLBX.MDP3",
                "    schema: ohlcv-1m",
                "    venue: CME Globex",
                "    timezone: America/New_York",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 180",
                "    notes: reference row",
                "  - symbol: MGC",
                "    enabled: true",
                "    required_for_readiness: true",
                "    asset_class: futures",
                "    execution_symbol: MGC",
                "    reference_symbol: GC",
                "    databento_symbol: MGC.v.0",
                "    dataset: GLBX.MDP3",
                "    schema: ohlcv-1m",
                "    venue: CME Globex",
                "    timezone: America/New_York",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 180",
                "    notes: execution row",
                "  - symbol: ES",
                "    enabled: true",
                "    required_for_readiness: true",
                "    asset_class: futures",
                "    execution_symbol: MES",
                "    reference_symbol: ES",
                "    databento_symbol: ES.v.0",
                "    dataset: GLBX.MDP3",
                "    schema: ohlcv-1m",
                "    venue: CME Globex",
                "    timezone: America/New_York",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 180",
                "    notes: reference row",
                "  - symbol: MES",
                "    enabled: true",
                "    required_for_readiness: true",
                "    asset_class: futures",
                "    execution_symbol: MES",
                "    reference_symbol: ES",
                "    databento_symbol: MES.v.0",
                "    dataset: GLBX.MDP3",
                "    schema: ohlcv-1m",
                "    venue: CME Globex",
                "    timezone: America/New_York",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 180",
                "    notes: execution row",
                "  - symbol: NQ",
                "    enabled: true",
                "    required_for_readiness: true",
                "    asset_class: futures",
                "    execution_symbol: MNQ",
                "    reference_symbol: NQ",
                "    databento_symbol: NQ.v.0",
                "    dataset: GLBX.MDP3",
                "    schema: ohlcv-1m",
                "    venue: CME Globex",
                "    timezone: America/New_York",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 180",
                "    notes: reference row",
                "  - symbol: MNQ",
                "    enabled: true",
                "    required_for_readiness: true",
                "    asset_class: futures",
                "    execution_symbol: MNQ",
                "    reference_symbol: NQ",
                "    databento_symbol: MNQ.v.0",
                "    dataset: GLBX.MDP3",
                "    schema: ohlcv-1m",
                "    venue: CME Globex",
                "    timezone: America/New_York",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 180",
                "    notes: execution row",
                "  - symbol: PL",
                "    enabled: false",
                "    required_for_readiness: false",
                "    asset_class: futures",
                "    execution_symbol: PL",
                "    reference_symbol: PL",
                "    databento_symbol: ''",
                "    dataset: ''",
                "    schema: ''",
                "    venue: ''",
                "    timezone: ''",
                "    min_confirmed_bars: 8",
                "    freshness_threshold_seconds: 300",
                "    notes: disabled row still reported",
            ]
        ),
        encoding="utf-8",
    )

    namelist = load_track_b_live_market_data_symbols(config_path)

    assert [row.symbol for row in namelist.enabled_symbols()] == ["GC", "MGC", "ES", "MES", "NQ", "MNQ"]
    assert [row.symbol for row in namelist.disabled_symbols()] == ["PL"]
    assert namelist.disabled_symbols()[0].databento_symbol == ""


def test_enabled_symbol_missing_databento_mapping_fails_validation() -> None:
    payload = valid_payload()
    rows = payload["symbols"]
    assert isinstance(rows, list)
    rows[1] = {**rows[1], "databento_symbol": ""}

    with pytest.raises(TrackBLiveMarketDataSymbolConfigError, match="Enabled symbol MGC.*databento_symbol"):
        parse_track_b_live_market_data_symbols(payload)


def test_enabled_thin_symbol_requires_explicit_latest_bar_tolerance() -> None:
    payload = valid_payload()
    rows = payload["symbols"]
    assert isinstance(rows, list)
    rows.append(
        symbol_row(
            "PL",
            execution_symbol="PL",
            reference_symbol="PL",
            databento_symbol="PL.v.0",
            required_for_readiness=False,
            market_freshness_policy=MARKET_FRESHNESS_POLICY_THIN_QUOTE_FEED,
        )
    )

    with pytest.raises(TrackBLiveMarketDataSymbolConfigError, match="Thin market symbol PL"):
        parse_track_b_live_market_data_symbols(payload)


def test_duplicate_databento_mapping_fails_validation() -> None:
    payload = valid_payload()
    rows = payload["symbols"]
    assert isinstance(rows, list)
    rows[1] = {**rows[1], "databento_symbol": "GC.v.0"}

    with pytest.raises(TrackBLiveMarketDataSymbolConfigError, match="Duplicate enabled Databento mappings"):
        parse_track_b_live_market_data_symbols(payload)


def test_duplicate_symbol_fails_validation() -> None:
    payload = valid_payload()
    rows = payload["symbols"]
    assert isinstance(rows, list)
    rows.append(symbol_row("MNQ", execution_symbol="MNQ", reference_symbol="NQ", databento_symbol="MNQ_DUP.v.0"))

    with pytest.raises(TrackBLiveMarketDataSymbolConfigError, match="Duplicate Track B live market-data symbols: MNQ"):
        parse_track_b_live_market_data_symbols(payload)


def test_invalid_required_execution_reference_pair_fails_validation() -> None:
    payload = valid_payload()
    rows = payload["symbols"]
    assert isinstance(rows, list)
    rows[1] = {**rows[1], "reference_symbol": "MGC"}

    with pytest.raises(TrackBLiveMarketDataSymbolConfigError, match="Expected MGC/GC, MES/ES, MNQ/NQ"):
        parse_track_b_live_market_data_symbols(payload)


def test_required_for_readiness_filters_enabled_required_symbols_only() -> None:
    payload = valid_payload()
    rows = payload["symbols"]
    assert isinstance(rows, list)
    rows.append(
        symbol_row(
            "PL",
            enabled=False,
            required_for_readiness=False,
            execution_symbol="PL",
            reference_symbol="PL",
            databento_symbol="",
            dataset="",
            schema="",
            venue="",
            timezone="",
        )
    )

    namelist = parse_track_b_live_market_data_symbols(payload)

    assert [row.symbol for row in namelist.required_for_readiness_symbols()] == ["GC", "MGC", "ES", "MES", "NQ", "MNQ"]
    assert [row.symbol for row in namelist.disabled_symbols()] == ["PL"]
