"""Bounded local OHLCV store for short-window live market-data consumers."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from ..domain.models import Bar
from .bar_models import build_bar_id
from .timeframes import normalize_timeframe_label, timeframe_minutes

DEFAULT_SHARED_LIVE_OHLCV_DB_PATH = Path("var") / "track_b_shared_live_ohlcv.sqlite3"
DEFAULT_SHARED_LIVE_OHLCV_MAX_BARS_PER_KEY = 1440
SHARED_LIVE_OHLCV_SOURCE = "track_b_shared_live_ohlcv_store"


@dataclass(frozen=True)
class SharedLiveOhlcvBar:
    symbol: str
    timeframe: str
    bar_start: datetime
    bar_end: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    source: str
    dataset: str | None = None
    schema: str | None = None
    request_symbol: str | None = None
    source_id: str | None = None
    raw_dbn_path: str | None = None

    def to_domain_bar(self) -> Bar:
        return Bar(
            bar_id=build_bar_id(self.symbol, self.timeframe, self.bar_end),
            symbol=self.symbol,
            timeframe=self.timeframe,
            start_ts=self.bar_start,
            end_ts=self.bar_end,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            is_final=True,
        )

    def to_chart_payload(self) -> dict[str, Any]:
        return {
            "time": self.bar_end.isoformat(),
            "start": self.bar_start.isoformat(),
            "open": float(self.open),
            "high": float(self.high),
            "low": float(self.low),
            "close": float(self.close),
            "volume": self.volume,
            "completed": True,
            "source_bar_count": 1,
        }


class SharedLiveOhlcvStore:
    """SQLite-backed store for bounded, local, recently completed OHLCV bars."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_bars_per_symbol_timeframe: int = DEFAULT_SHARED_LIVE_OHLCV_MAX_BARS_PER_KEY,
    ) -> None:
        self.path = Path(path)
        self.max_bars_per_symbol_timeframe = max(1, int(max_bars_per_symbol_timeframe))

    def upsert_mapping_bars(
        self,
        *,
        symbol: str,
        timeframe: str,
        bars: Sequence[Mapping[str, Any]],
        source: str,
        generated_at: datetime | None = None,
        dataset: str | None = None,
        schema: str | None = None,
        request_symbol: str | None = None,
        source_id: str | None = None,
        raw_dbn_path: str | Path | None = None,
    ) -> int:
        rows = [
            self._row_from_mapping(
                symbol=symbol,
                timeframe=timeframe,
                raw=row,
                source=source,
                generated_at=generated_at,
                dataset=dataset,
                schema=schema,
                request_symbol=request_symbol,
                source_id=source_id,
                raw_dbn_path=raw_dbn_path,
            )
            for row in bars
        ]
        return self.upsert_bars(rows)

    def upsert_bars(self, bars: Sequence[SharedLiveOhlcvBar]) -> int:
        if not bars:
            return 0
        self.path.parent.mkdir(parents=True, exist_ok=True)
        now_text = _utc_text(datetime.now(UTC))
        with closing(sqlite3.connect(self.path)) as conn:
            with conn:
                _ensure_schema(conn)
                rows = [
                    (
                        bar.symbol,
                        bar.timeframe,
                        _utc_text(bar.bar_start),
                        _utc_text(bar.bar_end),
                        str(bar.open),
                        str(bar.high),
                        str(bar.low),
                        str(bar.close),
                        int(bar.volume),
                        bar.source,
                        bar.dataset,
                        bar.schema,
                        bar.request_symbol,
                        bar.source_id,
                        bar.raw_dbn_path,
                        now_text,
                        now_text,
                    )
                    for bar in bars
                ]
                conn.executemany(
                    """
                    INSERT INTO live_ohlcv_bars (
                        symbol, timeframe, bar_start, bar_end, open, high, low, close, volume,
                        source, dataset, schema, request_symbol, source_id, raw_dbn_path,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, timeframe, bar_end) DO UPDATE SET
                        bar_start = excluded.bar_start,
                        open = excluded.open,
                        high = excluded.high,
                        low = excluded.low,
                        close = excluded.close,
                        volume = excluded.volume,
                        source = excluded.source,
                        dataset = excluded.dataset,
                        schema = excluded.schema,
                        request_symbol = excluded.request_symbol,
                        source_id = excluded.source_id,
                        raw_dbn_path = excluded.raw_dbn_path,
                        updated_at = excluded.updated_at
                    """,
                    rows,
                )
                for symbol, timeframe in {(bar.symbol, bar.timeframe) for bar in bars}:
                    _trim(conn, symbol=symbol, timeframe=timeframe, limit=self.max_bars_per_symbol_timeframe)
        return len(rows)

    def load_recent_bars(self, *, symbol: str, timeframe: str, limit: int) -> list[SharedLiveOhlcvBar]:
        symbol = str(symbol or "").strip().upper()
        timeframe = normalize_timeframe_label(timeframe)
        if not symbol or not self.path.exists():
            return []
        with closing(sqlite3.connect(self.path)) as conn:
            _ensure_schema(conn)
            rows = conn.execute(
                """
                SELECT symbol, timeframe, bar_start, bar_end, open, high, low, close, volume,
                       source, dataset, schema, request_symbol, source_id, raw_dbn_path
                FROM live_ohlcv_bars
                WHERE symbol = ? AND timeframe = ?
                ORDER BY bar_end DESC
                LIMIT ?
                """,
                (symbol, timeframe, max(1, int(limit))),
            ).fetchall()
        return [_bar_from_row(row) for row in reversed(rows)]

    def _row_from_mapping(
        self,
        *,
        symbol: str,
        timeframe: str,
        raw: Mapping[str, Any],
        source: str,
        generated_at: datetime | None,
        dataset: str | None,
        schema: str | None,
        request_symbol: str | None,
        source_id: str | None,
        raw_dbn_path: str | Path | None,
    ) -> SharedLiveOhlcvBar:
        normalized_timeframe = normalize_timeframe_label(timeframe)
        bar_end = _parse_datetime(raw.get("bar_end") or raw.get("end_ts") or raw.get("timestamp"))
        bar_start = _parse_datetime(raw.get("bar_start") or raw.get("start_ts")) if raw.get("bar_start") or raw.get("start_ts") else None
        if bar_start is None:
            bar_start = bar_end - timedelta(minutes=timeframe_minutes(normalized_timeframe))
        return SharedLiveOhlcvBar(
            symbol=str(raw.get("symbol") or symbol).strip().upper(),
            timeframe=normalized_timeframe,
            bar_start=bar_start,
            bar_end=bar_end,
            open=Decimal(str(raw["open"])),
            high=Decimal(str(raw["high"])),
            low=Decimal(str(raw["low"])),
            close=Decimal(str(raw["close"])),
            volume=max(0, int(Decimal(str(raw.get("volume") or 0)))),
            source=source,
            dataset=dataset,
            schema=schema,
            request_symbol=request_symbol,
            source_id=source_id,
            raw_dbn_path=None if raw_dbn_path is None else str(raw_dbn_path),
        )


def shared_live_ohlcv_chart_payload(
    *,
    path: str | Path | None,
    symbol: str,
    timeframe: str = "5m",
    limit: int = 72,
) -> dict[str, Any] | None:
    if path is None:
        return None
    bars = SharedLiveOhlcvStore(path).load_recent_bars(symbol=symbol, timeframe=timeframe, limit=limit)
    if not bars:
        return None
    return {
        "schema_version": "shared_live_ohlcv_chart_v1",
        "source": SHARED_LIVE_OHLCV_SOURCE,
        "source_detail": "bounded local SQLite store populated by Phase-1 Databento Live OHLCV listener",
        "symbol": str(symbol).strip().upper(),
        "timeframe": normalize_timeframe_label(timeframe),
        "bar_limit": max(1, int(limit)),
        "bar_count": len(bars),
        "latest_bar_ts": bars[-1].bar_end.isoformat(),
        "bars": [bar.to_chart_payload() for bar in bars],
    }


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS live_ohlcv_bars (
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            bar_start TEXT NOT NULL,
            bar_end TEXT NOT NULL,
            open TEXT NOT NULL,
            high TEXT NOT NULL,
            low TEXT NOT NULL,
            close TEXT NOT NULL,
            volume INTEGER NOT NULL,
            source TEXT NOT NULL,
            dataset TEXT,
            schema TEXT,
            request_symbol TEXT,
            source_id TEXT,
            raw_dbn_path TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (symbol, timeframe, bar_end)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_live_ohlcv_bars_lookup ON live_ohlcv_bars(symbol, timeframe, bar_end)"
    )


def _trim(conn: sqlite3.Connection, *, symbol: str, timeframe: str, limit: int) -> None:
    conn.execute(
        """
        DELETE FROM live_ohlcv_bars
        WHERE symbol = ? AND timeframe = ?
          AND bar_end NOT IN (
              SELECT bar_end
              FROM live_ohlcv_bars
              WHERE symbol = ? AND timeframe = ?
              ORDER BY bar_end DESC
              LIMIT ?
          )
        """,
        (symbol, timeframe, symbol, timeframe, max(1, int(limit))),
    )


def _bar_from_row(row: tuple[Any, ...]) -> SharedLiveOhlcvBar:
    return SharedLiveOhlcvBar(
        symbol=str(row[0]),
        timeframe=str(row[1]),
        bar_start=_parse_datetime(row[2]),
        bar_end=_parse_datetime(row[3]),
        open=Decimal(str(row[4])),
        high=Decimal(str(row[5])),
        low=Decimal(str(row[6])),
        close=Decimal(str(row[7])),
        volume=int(row[8]),
        source=str(row[9]),
        dataset=row[10],
        schema=row[11],
        request_symbol=row[12],
        source_id=row[13],
        raw_dbn_path=row[14],
    )


def _parse_datetime(value: Any) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("OHLCV bar timestamp is required.")
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


__all__ = [
    "DEFAULT_SHARED_LIVE_OHLCV_DB_PATH",
    "DEFAULT_SHARED_LIVE_OHLCV_MAX_BARS_PER_KEY",
    "SHARED_LIVE_OHLCV_SOURCE",
    "SharedLiveOhlcvBar",
    "SharedLiveOhlcvStore",
    "shared_live_ohlcv_chart_payload",
]
