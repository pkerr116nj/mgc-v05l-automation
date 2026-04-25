from __future__ import annotations

from datetime import datetime

from mgc_v05l.research.warehouse_historical_evaluator.derived_materializer import _derive_timeframe_rows


def _raw_row(*, ts: str, open_: float, high: float, low: float, close: float, volume: int) -> dict[str, object]:
    parsed = datetime.fromisoformat(ts)
    return {
        "symbol": "GC",
        "bar_ts": parsed,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "data_source": "historical_1m_canonical",
    }


def test_derive_15m_rows_builds_complete_bucket() -> None:
    raw_rows = [
        _raw_row(
            ts=f"2026-04-01T18:{minute:02d}:00-04:00",
            open_=100.0 + minute,
            high=101.0 + minute,
            low=99.0 + minute,
            close=100.5 + minute,
            volume=10 + minute,
        )
        for minute in range(1, 16)
    ]

    derived = _derive_timeframe_rows(
        raw_rows=raw_rows,
        timeframe="15m",
        minutes=15,
        raw_version="rawv1",
        materialized_ts=datetime.fromisoformat("2026-04-02T00:00:00+00:00"),
    )

    assert len(derived) == 1
    row = derived[0]
    assert row["timeframe"] == "15m"
    assert row["bar_ts"] == datetime.fromisoformat("2026-04-01T22:15:00+00:00")
    assert row["open"] == 101.0
    assert row["close"] == 115.5
    assert row["high"] == 116.0
    assert row["low"] == 100.0
    assert row["volume"] == sum(10 + minute for minute in range(1, 16))


def test_derive_daily_rows_groups_by_futures_session_date() -> None:
    raw_rows = [
        _raw_row(
            ts="2026-04-05T18:00:00-04:00",
            open_=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10,
        ),
        _raw_row(
            ts="2026-04-05T23:59:00-04:00",
            open_=100.5,
            high=102.0,
            low=100.0,
            close=101.0,
            volume=11,
        ),
        _raw_row(
            ts="2026-04-06T17:00:00-04:00",
            open_=101.0,
            high=103.0,
            low=98.0,
            close=102.5,
            volume=12,
        ),
        _raw_row(
            ts="2026-04-06T18:00:00-04:00",
            open_=103.0,
            high=104.0,
            low=102.0,
            close=103.5,
            volume=13,
        ),
    ]

    derived = _derive_timeframe_rows(
        raw_rows=raw_rows,
        timeframe="daily",
        minutes=0,
        raw_version="rawv1",
        materialized_ts=datetime.fromisoformat("2026-04-07T00:00:00+00:00"),
    )

    assert len(derived) == 2
    first, second = derived
    assert first["timeframe"] == "daily"
    assert first["bar_ts"] == datetime.fromisoformat("2026-04-06T21:00:00+00:00")
    assert first["open"] == 100.0
    assert first["close"] == 102.5
    assert first["high"] == 103.0
    assert first["low"] == 98.0
    assert first["volume"] == 33
    assert second["bar_ts"] == datetime.fromisoformat("2026-04-06T22:00:00+00:00")
