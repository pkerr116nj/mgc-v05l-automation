import sqlite3
from pathlib import Path

import pytest
from sqlalchemy.exc import OperationalError

from mgc_v05l.persistence.db import (
    SQLITE_BUSY_TIMEOUT_MS,
    SQLITE_WAL_AUTOCHECKPOINT_PAGES,
    build_engine,
)
from mgc_v05l.persistence.repositories import _run_sqlite_write_with_retry


def test_build_engine_applies_sqlite_busy_timeout_and_wal(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite:///{tmp_path / 'runtime.sqlite3'}")

    with engine.begin() as connection:
        journal_mode = connection.exec_driver_sql("PRAGMA journal_mode").scalar()
        busy_timeout = connection.exec_driver_sql("PRAGMA busy_timeout").scalar()
        wal_autocheckpoint = connection.exec_driver_sql("PRAGMA wal_autocheckpoint").scalar()

    assert str(journal_mode).lower() == "wal"
    assert int(busy_timeout) == SQLITE_BUSY_TIMEOUT_MS
    assert int(wal_autocheckpoint) == SQLITE_WAL_AUTOCHECKPOINT_PAGES


def test_run_sqlite_write_with_retry_retries_locked_errors_then_succeeds(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite:///{tmp_path / 'runtime.sqlite3'}")
    attempts = {"count": 0}

    def _operation(_connection) -> str:
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise OperationalError(
                "insert into example values (?)",
                {},
                sqlite3.OperationalError("database is locked"),
            )
        return "ok"

    assert _run_sqlite_write_with_retry(engine, _operation) == "ok"
    assert attempts["count"] == 3


def test_run_sqlite_write_with_retry_does_not_swallow_non_lock_errors(tmp_path: Path) -> None:
    engine = build_engine(f"sqlite:///{tmp_path / 'runtime.sqlite3'}")

    def _operation(_connection) -> None:
        raise OperationalError(
            "insert into example values (?)",
            {},
            sqlite3.OperationalError("disk I/O error"),
        )

    with pytest.raises(OperationalError):
        _run_sqlite_write_with_retry(engine, _operation)
