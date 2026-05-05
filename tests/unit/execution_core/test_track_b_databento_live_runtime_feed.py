from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from mgc_v05l.execution_core.track_b_databento_live_runtime_feed import (
    TrackBDatabentoLiveFeedConfig,
    TrackBDatabentoLiveFeedVerdict,
    run_track_b_databento_live_runtime_feed,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 12, 10, 30, tzinfo=UTC)


def live_records() -> list[dict[str, object]]:
    start = datetime(2026, 5, 5, 12, 1, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    for index in range(9):
        ts = start + timedelta(minutes=index)
        rows.append(
            {
                "ts_event": ts.isoformat(),
                "ts_recv": (ts + timedelta(seconds=1)).isoformat(),
                "open": str(3400 + index / 10),
                "high": str(3400.2 + index / 10),
                "low": str(3399.8 + index / 10),
                "close": str(3400.1 + index / 10),
                "volume": "2",
            }
        )
    return rows


class FakeLiveClient:
    def __init__(self, records: list[dict[str, object]], *, error: Exception | None = None) -> None:
        self.records = records
        self.error = error
        self.callback: Callable[[Any], None] | None = None
        self.exception_callback: Callable[[Exception], None] | None = None
        self.subscribe_kwargs: dict[str, Any] = {}
        self.terminated = False

    def add_callback(self, callback, exception_callback):  # type: ignore[no-untyped-def]
        self.callback = callback
        self.exception_callback = exception_callback

    def subscribe(self, **kwargs: Any) -> None:
        self.subscribe_kwargs = dict(kwargs)
        if self.error is not None:
            raise self.error

    def start(self) -> None:
        assert self.callback is not None
        for record in self.records:
            self.callback(record)

    def terminate(self) -> None:
        self.terminated = True


def config(tmp_path: Path, **overrides: object) -> TrackBDatabentoLiveFeedConfig:
    values = {
        "output_root": tmp_path / "live",
        "max_records": 9,
        "max_bars": 9,
        "min_bars": 8,
        "max_seconds": 0.01,
        "max_latest_1m_age_seconds": 120,
        "max_completed_5m_age_seconds": 360,
    }
    values.update(overrides)
    return TrackBDatabentoLiveFeedConfig(**values)


def test_live_runtime_feed_writes_fresh_bounded_candles(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    client = FakeLiveClient(live_records())
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    result = run_track_b_databento_live_runtime_feed(
        config=config(tmp_path),
        live_client_factory=lambda _key: client,
        now_func=aware_now,
        run_id="live-test",
    )

    assert result.verdict == TrackBDatabentoLiveFeedVerdict.DATA_WRITTEN_EXECUTION_FRESH
    assert result.report["live_feed_connected"] is True
    assert result.report["subscription_status"] == "SUBSCRIBED_RECORDS_RECEIVED"
    assert result.report["dataset"] == "GLBX.MDP3"
    assert result.report["schema"] == "ohlcv-1m"
    assert result.report["latest_1m_timestamp"] == "2026-05-05T12:09:00+00:00"
    assert result.report["latest_completed_5m_timestamp"] == "2026-05-05T12:05:00+00:00"
    assert result.report["fresh_for_execution"] is True
    assert result.live_1m_candles_json is not None
    assert (tmp_path / "live" / "latest_live_mgc_1m_candles.json").exists()
    assert (tmp_path / "live" / "latest_databento_live_runtime_feed_heartbeat.json").exists()
    assert client.subscribe_kwargs == {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": ["MGC.v.0"],
        "stype_in": "continuous",
        "stype_out": "instrument_id",
    }
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_live_runtime_feed_provider_error_never_logs_secret(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DATABENTO_API_KEY", "SECRET_VALUE")

    result = run_track_b_databento_live_runtime_feed(
        config=config(tmp_path),
        live_client_factory=lambda _key: FakeLiveClient([], error=RuntimeError("bad SECRET_VALUE entitlement")),
        now_func=aware_now,
        run_id="live-error",
    )

    assert result.verdict == TrackBDatabentoLiveFeedVerdict.PROVIDER_LIVE_UNAVAILABLE
    assert result.report["live_feed_connected"] is False
    assert "SECRET_VALUE" not in str(result.report)
    assert "<redacted>" in str(result.report)
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_live_runtime_feed_blocks_when_no_records(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    result = run_track_b_databento_live_runtime_feed(
        config=config(tmp_path),
        live_client_factory=lambda _key: FakeLiveClient([]),
        now_func=aware_now,
        run_id="live-none",
    )

    assert result.verdict == TrackBDatabentoLiveFeedVerdict.BLOCKED_NO_RECORDS
    assert result.report["live_feed_connected"] is False
    assert result.live_1m_candles_json is None
    assert "no records" in str(result.report["primary_blocker"]).lower()

