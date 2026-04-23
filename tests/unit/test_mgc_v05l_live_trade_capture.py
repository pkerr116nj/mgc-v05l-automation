from __future__ import annotations

import json
from pathlib import Path
from sqlite3 import connect

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.app.live_trade_capture import run_live_trade_capture


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "tick_engine.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def test_run_live_trade_capture_replays_jsonl_into_canonical_store(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    input_path = tmp_path / "ticks.jsonl"
    rows = [
        {"symbol": "MGC", "timestamp": "2026-04-17T09:30:05-04:00", "price": "100.0", "size": 1},
        {"symbol": "MES", "timestamp": "2026-04-17T09:30:10-04:00", "price": "5000.0", "size": 2},
        {"symbol": "MGC", "timestamp": "2026-04-17T09:31:05-04:00", "price": "101.0", "size": 3},
        {"symbol": "MES", "timestamp": "2026-04-17T09:31:10-04:00", "price": "5001.0", "size": 4},
        {"symbol": "MGC", "timestamp": "2026-04-17T09:32:05-04:00", "price": "102.0", "size": 5},
        {"symbol": "MES", "timestamp": "2026-04-17T09:32:10-04:00", "price": "5002.0", "size": 6},
        {"symbol": "MGC", "timestamp": "2026-04-17T09:33:05-04:00", "price": "103.0", "size": 7},
        {"symbol": "MES", "timestamp": "2026-04-17T09:33:10-04:00", "price": "5003.0", "size": 8},
        {"symbol": "MGC", "timestamp": "2026-04-17T09:34:05-04:00", "price": "104.0", "size": 9},
        {"symbol": "MES", "timestamp": "2026-04-17T09:34:10-04:00", "price": "5004.0", "size": 10},
        {"symbol": "MGC", "timestamp": "2026-04-17T09:35:05-04:00", "price": "105.0", "size": 11},
        {"symbol": "MES", "timestamp": "2026-04-17T09:35:10-04:00", "price": "5005.0", "size": 12},
    ]
    input_path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    result = run_live_trade_capture(
        settings=settings,
        symbols=["MGC", "MES"],
        input_jsonl=input_path,
        derive_timeframes=("5m",),
        raw_data_source="jsonl_tick_replay",
    )

    assert result.mode == "jsonl_replay"
    assert result.trade_count == 12
    assert result.finalized_bar_count == 12
    assert result.per_symbol_trade_count == {"MES": 6, "MGC": 6}
    assert result.per_symbol_finalized_bars == {"MES": 6, "MGC": 6}
    assert result.derived_timeframes == ("5m",)

    db_path = tmp_path / "tick_engine.sqlite3"
    connection = connect(db_path)
    try:
        counts = connection.execute(
            """
            select ticker, timeframe, data_source, count(*)
            from bars
            where ticker in ('MGC', 'MES')
            group by ticker, timeframe, data_source
            order by ticker, timeframe, data_source
            """
        ).fetchall()
    finally:
        connection.close()

    assert counts == [
        ("MES", "1m", "historical_1m_canonical", 6),
        ("MES", "1m", "jsonl_tick_replay", 6),
        ("MES", "5m", "historical_5m_canonical", 1),
        ("MGC", "1m", "historical_1m_canonical", 6),
        ("MGC", "1m", "jsonl_tick_replay", 6),
        ("MGC", "5m", "historical_5m_canonical", 1),
    ]
