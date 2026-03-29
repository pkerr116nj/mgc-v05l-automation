from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

from mgc_v05l.app.market_data_status_report import build_market_data_status_report


def test_market_data_status_report_flags_base_and_derived_surfaces(tmp_path: Path) -> None:
    db_path = tmp_path / "status.sqlite3"
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            create table bars (
              bar_id text primary key,
              ticker text not null,
              data_source text not null,
              timeframe text not null,
              end_ts text not null
            );
            """
        )
        base = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
        rows = [
            ("a", "MGC", "schwab_history", "1m", base.isoformat()),
            ("b", "MGC", "resampled_1m_to_3m", "3m", (base + timedelta(minutes=3)).isoformat()),
            ("c", "MGC", "internal", "5m", (base + timedelta(minutes=5)).isoformat()),
        ]
        connection.executemany("insert into bars (bar_id, ticker, data_source, timeframe, end_ts) values (?, ?, ?, ?, ?)", rows)
        connection.commit()
    finally:
        connection.close()

    report = build_market_data_status_report(db_path=db_path)
    symbol = report["symbols"][0]

    assert report["mode_aware_assumptions"]["baseline_parity_mode"]["structural_signal_timeframe"] == "5m"
    assert report["mode_aware_assumptions"]["research_execution_mode"]["minimum_execution_base_layer"] == "1m_schwab_history"
    assert symbol["ticker"] == "MGC"
    assert symbol["base_1m_available"] is True
    assert symbol["native_timeframes"] == ["1m"]
    assert symbol["derived_timeframes"] == ["3m"]
    assert symbol["coverage_modes"]["baseline_parity_mode"] is True
    assert symbol["coverage_modes"]["research_execution_mode"] is True


def test_market_data_status_report_labels_missing_5m_as_baseline_parity_gap(tmp_path: Path) -> None:
    db_path = tmp_path / "status.sqlite3"
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            create table bars (
              bar_id text primary key,
              ticker text not null,
              data_source text not null,
              timeframe text not null,
              end_ts text not null
            );
            """
        )
        base = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
        rows = [
            ("a", "MGC", "schwab_history", "1m", base.isoformat()),
            ("b", "MGC", "resampled_1m_to_3m", "3m", (base + timedelta(minutes=3)).isoformat()),
        ]
        connection.executemany("insert into bars (bar_id, ticker, data_source, timeframe, end_ts) values (?, ?, ?, ?, ?)", rows)
        connection.commit()
    finally:
        connection.close()

    report = build_market_data_status_report(db_path=db_path)
    symbol = report["symbols"][0]

    assert symbol["coverage_modes"]["research_execution_mode"] is True
    assert symbol["coverage_modes"]["baseline_parity_mode"] is False
    assert "legacy benchmark 5m surface not available" in symbol["notes"]


def test_market_data_status_report_includes_symbol_role_metadata(tmp_path: Path) -> None:
    db_path = tmp_path / "status.sqlite3"
    symbol_config_path = tmp_path / "schwab.local.json"
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            """
            create table bars (
              bar_id text primary key,
              ticker text not null,
              data_source text not null,
              timeframe text not null,
              end_ts text not null
            );
            """
        )
        base = datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc)
        rows = [
            ("a", "MGC", "schwab_history", "1m", base.isoformat()),
            ("b", "GC", "schwab_history", "1m", base.isoformat()),
        ]
        connection.executemany("insert into bars (bar_id, ticker, data_source, timeframe, end_ts) values (?, ?, ?, ?, ?)", rows)
        connection.commit()
    finally:
        connection.close()

    symbol_config_path.write_text(
        json.dumps(
            {
                "symbol_metadata": {
                    "MGC": {"symbol_role": "execution", "reference_symbol": "GC", "contract_family": "gold"},
                    "GC": {"symbol_role": "reference", "execution_symbol": "MGC", "contract_family": "gold"},
                }
            }
        ),
        encoding="utf-8",
    )

    report = build_market_data_status_report(db_path=db_path, symbol_config_path=symbol_config_path)
    symbols = {item["ticker"]: item for item in report["symbols"]}

    assert symbols["MGC"]["symbol_role"] == "execution"
    assert symbols["MGC"]["reference_symbol"] == "GC"
    assert symbols["GC"]["symbol_role"] == "reference"
    assert symbols["GC"]["execution_symbol"] == "MGC"
