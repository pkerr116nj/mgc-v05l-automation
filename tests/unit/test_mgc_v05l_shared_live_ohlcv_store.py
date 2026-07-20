from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import sqlite3

import mgc_v05l.market_data.shared_live_ohlcv_store as shared_store_module
from mgc_v05l.market_data.shared_live_ohlcv_store import SharedLiveOhlcvStore, shared_live_ohlcv_chart_payload


def test_shared_live_ohlcv_store_upserts_and_retains_latest_bounded_rows(tmp_path: Path) -> None:
    store = SharedLiveOhlcvStore(tmp_path / "shared.sqlite3", max_bars_per_symbol_timeframe=3)
    start = datetime(2026, 7, 20, 6, 0, tzinfo=timezone.utc)
    rows = [
        {
            "bar_start": (start + timedelta(minutes=index)).isoformat(),
            "bar_end": (start + timedelta(minutes=index + 1)).isoformat(),
            "open": 100 + index,
            "high": 101 + index,
            "low": 99 + index,
            "close": 100.5 + index,
            "volume": 10 + index,
            "completed": True,
        }
        for index in range(5)
    ]

    assert store.upsert_mapping_bars(symbol="MNQ", timeframe="1m", bars=rows, source="DATABENTO_REALTIME_PHASE1") == 5
    assert store.upsert_mapping_bars(symbol="MNQ", timeframe="1m", bars=rows[-1:], source="DATABENTO_REALTIME_PHASE1") == 1

    bars = store.load_recent_bars(symbol="MNQ", timeframe="1m", limit=10)
    assert [bar.bar_end for bar in bars] == [start + timedelta(minutes=index) for index in (3, 4, 5)]

    chart = shared_live_ohlcv_chart_payload(path=tmp_path / "shared.sqlite3", symbol="MNQ", timeframe="1m", limit=2)
    assert chart is not None
    assert chart["source"] == "track_b_shared_live_ohlcv_store"
    assert chart["bar_count"] == 2
    assert [row["time"] for row in chart["bars"]] == [
        (start + timedelta(minutes=4)).isoformat(),
        (start + timedelta(minutes=5)).isoformat(),
    ]


def test_shared_live_ohlcv_store_closes_sqlite_connections(monkeypatch: Any, tmp_path: Path) -> None:
    real_connect = sqlite3.connect
    close_count = 0

    class ClosingConnection:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._conn = real_connect(*args, **kwargs)

        def __enter__(self) -> sqlite3.Connection:
            return self._conn.__enter__()

        def __exit__(self, *args: Any) -> bool | None:
            return self._conn.__exit__(*args)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._conn, name)

        def close(self) -> None:
            nonlocal close_count
            close_count += 1
            self._conn.close()

    monkeypatch.setattr(shared_store_module.sqlite3, "connect", ClosingConnection)
    store = SharedLiveOhlcvStore(tmp_path / "shared.sqlite3", max_bars_per_symbol_timeframe=3)
    start = datetime(2026, 7, 20, 6, 0, tzinfo=timezone.utc)
    row = {
        "bar_start": start.isoformat(),
        "bar_end": (start + timedelta(minutes=1)).isoformat(),
        "open": 100,
        "high": 101,
        "low": 99,
        "close": 100.5,
        "volume": 10,
        "completed": True,
    }

    store.upsert_mapping_bars(symbol="MNQ", timeframe="1m", bars=[row], source="DATABENTO_REALTIME_PHASE1")
    store.load_recent_bars(symbol="MNQ", timeframe="1m", limit=1)

    assert close_count == 2
