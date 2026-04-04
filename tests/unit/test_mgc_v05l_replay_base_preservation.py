from __future__ import annotations

import sqlite3
from pathlib import Path

from mgc_v05l.app.replay_base_preservation import preserve_replay_base


def _create_current_schema_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            create table bars (
                bar_id text primary key,
                instrument_id integer,
                ticker text not null,
                cusip text,
                asset_class text,
                data_source text not null,
                timestamp text not null,
                symbol text not null,
                timeframe text not null,
                start_ts text not null,
                end_ts text not null,
                open numeric not null,
                high numeric not null,
                low numeric not null,
                close numeric not null,
                volume integer not null,
                is_final boolean not null,
                session_asia boolean not null,
                session_london boolean not null,
                session_us boolean not null,
                session_allowed boolean not null,
                created_at text not null
            )
            """
        )
        connection.commit()
    finally:
        connection.close()


def _insert_bar(path: Path, *, bar_id: str, end_ts: str) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            insert into bars (
                bar_id, instrument_id, ticker, cusip, asset_class, data_source, timestamp, symbol,
                timeframe, start_ts, end_ts, open, high, low, close, volume, is_final,
                session_asia, session_london, session_us, session_allowed, created_at
            ) values (?, null, 'MGC', null, null, 'schwab_history', ?, 'MGC', '1m', ?, ?, 1, 1, 1, 1, 1, 1, 0, 0, 1, 1, ?)
            """,
            (
                bar_id,
                end_ts,
                end_ts,
                end_ts,
                end_ts,
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _count_bars(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("select count(*) from bars").fetchone()
        return int(row[0] or 0)
    finally:
        connection.close()


def test_preserve_replay_base_bootstraps_archive_and_restores_primary(tmp_path: Path) -> None:
    primary = tmp_path / "replay.sqlite3"
    archive = tmp_path / "replay.archive.sqlite3"
    _create_current_schema_db(primary)
    _insert_bar(primary, bar_id="MGC|1m|2026-02-15T18:00:00-05:00", end_ts="2026-02-15T18:00:00-05:00")

    first = preserve_replay_base(primary_db_path=primary, archive_db_path=archive)
    assert archive.exists()
    assert _count_bars(archive) == 1
    assert first["gap_window"]["exists_in_primary_after_sync"] is False

    primary.unlink()
    second = preserve_replay_base(primary_db_path=primary, archive_db_path=archive)
    assert primary.exists()
    assert _count_bars(primary) == 1
    assert any(action["action"] == "restore_primary_from_archive" for action in second["actions"])


def test_preserve_replay_base_merges_deeper_archive_into_primary(tmp_path: Path) -> None:
    primary = tmp_path / "replay.sqlite3"
    archive = tmp_path / "replay.archive.sqlite3"
    _create_current_schema_db(primary)
    _create_current_schema_db(archive)
    _insert_bar(primary, bar_id="MGC|1m|2026-02-15T18:00:00-05:00", end_ts="2026-02-15T18:00:00-05:00")
    _insert_bar(archive, bar_id="MGC|1m|2026-02-03T18:00:00-05:00", end_ts="2026-02-03T18:00:00-05:00")
    _insert_bar(archive, bar_id="MGC|1m|2026-02-15T18:00:00-05:00", end_ts="2026-02-15T18:00:00-05:00")

    payload = preserve_replay_base(primary_db_path=primary, archive_db_path=archive)

    assert _count_bars(primary) == 2
    assert payload["regression_detected_before_sync"] is True
    assert payload["status"] == "warning"
    assert payload["after"][str(primary)]["earliest_1m"] == "2026-02-03T18:00:00-05:00"
    assert any(action["action"] == "merge_archive_into_primary" for action in payload["actions"])
