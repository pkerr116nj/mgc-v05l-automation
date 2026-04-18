from __future__ import annotations

import sqlite3
from pathlib import Path

from mgc_v05l.research.platform import discover_best_sources, ensure_source_inventory, last_source_discovery_metadata
from mgc_v05l.research.platform import source_context as source_context_module


def _build_source_db(path: Path, *, row_count: int) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            create table if not exists bars (
              symbol text not null,
              timeframe text not null,
              data_source text not null,
              end_ts text not null
            );
            delete from bars;
            """
        )
        rows = [
            ("GC", "1m", "historical_1m_canonical", f"2026-03-13T18:{minute:02d}:00+00:00")
            for minute in range(1, row_count + 1)
        ]
        connection.executemany(
            "insert into bars (symbol, timeframe, data_source, end_ts) values (?, ?, ?, ?)",
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def _append_source_rows(path: Path, rows: list[tuple[str, str, str, str]]) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executemany(
            "insert into bars (symbol, timeframe, data_source, end_ts) values (?, ?, ?, ?)",
            rows,
        )
        connection.commit()
    finally:
        connection.close()


def test_source_inventory_reuses_and_refreshes_by_sqlite_signature(tmp_path: Path) -> None:
    database_path = tmp_path / "sources.sqlite3"
    inventory_root = tmp_path / "inventory"
    _build_source_db(database_path, row_count=5)

    first = ensure_source_inventory(inventory_root=inventory_root, sqlite_paths=[database_path])
    assert first["timing"]["inventory_cache_hit"] is False
    assert first["timing"]["scanned_files"] == 1
    assert first["timing"]["reused_files"] == 0
    assert len(first["rows"]) == 1
    assert int(first["rows"][0]["row_count"]) == 5

    second = ensure_source_inventory(inventory_root=inventory_root, sqlite_paths=[database_path])
    assert second["timing"]["inventory_cache_hit"] is True
    assert second["timing"]["scanned_files"] == 0
    assert second["timing"]["reused_files"] == 1
    assert len(second["rows"]) == 1
    assert int(second["rows"][0]["row_count"]) == 5

    _build_source_db(database_path, row_count=7)
    refreshed = ensure_source_inventory(inventory_root=inventory_root, sqlite_paths=[database_path])
    assert refreshed["timing"]["inventory_cache_hit"] is False
    assert refreshed["timing"]["scanned_files"] == 1
    assert len(refreshed["rows"]) == 1
    assert int(refreshed["rows"][0]["row_count"]) == 7


def test_source_inventory_filtered_scope_scans_only_requested_symbol_timeframes(tmp_path: Path) -> None:
    database_path = tmp_path / "sources.sqlite3"
    inventory_root = tmp_path / "inventory"
    _build_source_db(database_path, row_count=4)
    _append_source_rows(
        database_path,
        [
            ("MGC", "1m", "historical_1m_canonical", "2026-03-13T18:01:00+00:00"),
            ("MGC", "5m", "historical_5m_canonical", "2026-03-13T18:05:00+00:00"),
            ("CL", "1m", "historical_1m_canonical", "2026-03-13T18:01:00+00:00"),
        ],
    )

    filtered = ensure_source_inventory(
        inventory_root=inventory_root,
        sqlite_paths=[database_path],
        symbols={"GC"},
        timeframes={"1m"},
    )

    assert len(filtered["rows"]) == 1
    assert filtered["rows"][0]["symbol"] == "GC"
    assert filtered["rows"][0]["timeframe"] == "1m"
    assert filtered["inventory_scope"] == {"symbols": ["GC"], "timeframes": ["1m"]}


def test_discover_best_sources_uses_persisted_inventory_rows(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "sources.sqlite3"
    inventory_root = tmp_path / "inventory"
    _build_source_db(database_path, row_count=4)
    monkeypatch.setattr(source_context_module, "_default_source_inventory_root", lambda: inventory_root)
    ensure_source_inventory(
        inventory_root=inventory_root,
        sqlite_paths=[database_path],
        symbols={"GC"},
        timeframes={"1m"},
    )

    selections = discover_best_sources(symbols={"GC"}, timeframes={"1m"}, sqlite_paths=[database_path])
    metadata = last_source_discovery_metadata()

    assert selections["GC"]["1m"].data_source == "historical_1m_canonical"
    assert metadata["inventory_timing"]["inventory_cache_hit"] is True
    assert metadata["selection_count"] == 1
