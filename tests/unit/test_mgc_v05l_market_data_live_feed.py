from __future__ import annotations

import json
from decimal import Decimal
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import mgc_v05l.market_data.live_feed as live_feed_module
from mgc_v05l.domain.models import Bar
from mgc_v05l.market_data.live_feed import (
    DatabentoHistoricalPollingClient,
    _DATABENTO_LIVE_REPLAY_LOOKBACK_CAP_MINUTES,
    databento_live_auth_response,
    HistoricalPollingLiveClient,
    LivePollingService,
    Phase1RuntimeArtifactMarketClosedError,
    Phase1RuntimeArtifactPollingClient,
    databento_live_effective_end,
    databento_live_format_timestamp,
    _DatabentoRawLiveSession,
)
from mgc_v05l.market_data.databento_provider import DatabentoHttpError
from mgc_v05l.market_data.schwab_models import SchwabLivePollRequest
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet


class _RecordingHistoricalClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, object | None]] = []

    def fetch_price_history(self, external_symbol, request, default_frequency):
        self.calls.append((external_symbol, request, default_frequency))
        return {"candles": []}


class _RecordingDatabentoProvider:
    def __init__(self, bars: list[Bar] | None = None) -> None:
        self.bars = list(bars or [])
        self.requests: list[object] = []

    def fetch_historical_bars(self, request):
        self.requests.append(request)
        return SimpleNamespace(bars=list(self.bars))


class _RetryingDatabentoProvider:
    def __init__(self, *, error_text: str, bars: list[Bar] | None = None) -> None:
        self.error_text = error_text
        self.bars = list(bars or [])
        self.requests: list[object] = []
        self.call_count = 0

    def fetch_historical_bars(self, request):
        self.requests.append(request)
        self.call_count += 1
        if self.call_count == 1:
            raise DatabentoHttpError(self.error_text)
        return SimpleNamespace(bars=list(self.bars))


class _RecoveryClient:
    def __init__(self, poll_results: list[list[Bar]], *, recover_ok: bool = True) -> None:
        self.poll_results = list(poll_results)
        self.recover_ok = recover_ok
        self.poll_calls = 0
        self.recover_calls: list[dict[str, str]] = []

    def poll_live_bars(self, _external_symbol, _external_timeframe, _request):
        index = min(self.poll_calls, len(self.poll_results) - 1)
        self.poll_calls += 1
        return list(self.poll_results[index])

    def recover_live_bars(self, *, internal_symbol: str, internal_timeframe: str, reason: str) -> dict[str, object]:
        self.recover_calls.append(
            {
                "internal_symbol": internal_symbol,
                "internal_timeframe": internal_timeframe,
                "reason": reason,
            }
        )
        return {
            "action": "provider_resubscribe",
            "ok": self.recover_ok,
            "detail": "test recovery",
        }


def _databento_bar() -> Bar:
    end_ts = datetime.fromisoformat("2026-04-30T12:00:00+00:00")
    return Bar(
        bar_id=f"MGC|1m|{end_ts.isoformat()}",
        symbol="MGC",
        timeframe="1m",
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def _bar_at(end_ts_iso: str, *, symbol: str = "MGC") -> Bar:
    end_ts = datetime.fromisoformat(end_ts_iso)
    return Bar(
        bar_id=f"{symbol}|1m|{end_ts.isoformat()}",
        symbol=symbol,
        timeframe="1m",
        start_ts=end_ts - timedelta(minutes=1),
        end_ts=end_ts,
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


class _FixedDateTime(datetime):
    fixed_now = datetime.fromisoformat("2026-05-01T00:00:00+00:00")

    @classmethod
    def now(cls, tz=None):
        value = cls.fixed_now
        return value if tz is None else value.astimezone(tz)


def _write_phase1_runtime_artifact(
    root: Path,
    *,
    symbol: str = "MNQ",
    timeframe: str = "1m",
    generated_at: str = "2026-05-18T12:00:10+00:00",
    bars: list[dict] | None = None,
    **overrides,
) -> Path:
    path = root / symbol / timeframe / "latest_runtime_candles.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": generated_at,
        "source": "DATABENTO_REALTIME_PHASE1",
        "symbol": symbol,
        "instrument": symbol,
        "timeframe": timeframe,
        "freshness_seconds": 180,
        "historical_seed_ready": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "databento_live_api_replay": False,
        "bars": bars
        if bars is not None
        else [
            {
                "bar_start": "2026-05-18T11:59:00+00:00",
                "bar_end": "2026-05-18T12:00:00+00:00",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100.5,
                "volume": 10,
                "completed": True,
            }
        ],
    }
    payload.update(overrides)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def test_phase1_runtime_artifact_polling_client_reads_fresh_completed_bars_and_filters_since(
    tmp_path: Path,
) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_phase1_runtime_artifact(
        root,
        bars=[
            {
                "bar_start": "2026-05-18T11:58:00+00:00",
                "bar_end": "2026-05-18T11:59:00+00:00",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
                "completed": True,
            },
            {
                "bar_start": "2026-05-18T11:59:00+00:00",
                "bar_end": "2026-05-18T12:00:00+00:00",
                "open": 100,
                "high": 102,
                "low": 99,
                "close": 101,
                "volume": 11,
                "completed": True,
            },
            {
                "bar_start": "2026-05-18T11:59:00+00:00",
                "bar_end": "2026-05-18T12:00:00+00:00",
                "open": 100,
                "high": 102,
                "low": 99,
                "close": 101,
                "volume": 11,
                "completed": True,
            },
            {
                "bar_start": "2026-05-18T12:00:00+00:00",
                "bar_end": "2026-05-18T12:01:00+00:00",
                "open": 101,
                "high": 103,
                "low": 100,
                "close": 102,
                "volume": 12,
                "completed": False,
            },
        ],
    )
    client = Phase1RuntimeArtifactPollingClient(
        artifact_root=root,
        now_fn=lambda: datetime.fromisoformat("2026-05-18T12:00:20+00:00"),
    )

    bars = client.poll_live_bars(
        None,
        "1m",
        SchwabLivePollRequest(
            internal_symbol="MNQ",
            since=datetime.fromisoformat("2026-05-18T11:59:00+00:00"),
        ),
    )

    assert [bar.end_ts.isoformat() for bar in bars] == ["2026-05-18T12:00:00+00:00"]
    assert bars[0].symbol == "MNQ"
    assert bars[0].bar_id.startswith("MNQ|1m|")


def test_phase1_runtime_artifact_polling_client_rejects_wrong_symbol_or_timeframe(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_phase1_runtime_artifact(root, symbol="MNQ", timeframe="1m")
    artifact_path = root / "MNQ" / "1m" / "latest_runtime_candles.json"
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload["symbol"] = "MES"
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    client = Phase1RuntimeArtifactPollingClient(
        artifact_root=root,
        now_fn=lambda: datetime.fromisoformat("2026-05-18T12:00:20+00:00"),
    )

    with pytest.raises(RuntimeError, match="symbol mismatch"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))

    _write_phase1_runtime_artifact(root, symbol="MNQ", timeframe="1m")
    payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    payload["timeframe"] = "3m"
    artifact_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(RuntimeError, match="timeframe mismatch"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))


def test_phase1_runtime_artifact_polling_client_rejects_forbidden_provenance(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_phase1_runtime_artifact(root, source="DATABENTO_HISTORICAL_SEED")
    client = Phase1RuntimeArtifactPollingClient(
        artifact_root=root,
        now_fn=lambda: datetime.fromisoformat("2026-05-18T12:00:20+00:00"),
    )

    with pytest.raises(RuntimeError, match="invalid provenance"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))

    _write_phase1_runtime_artifact(root, research_artifact_used=True)
    with pytest.raises(RuntimeError, match="historical/replay/research/archive"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))


def test_phase1_runtime_artifact_polling_client_rejects_stale_or_empty_artifact(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_phase1_runtime_artifact(root, generated_at="2026-05-18T11:00:00+00:00")
    client = Phase1RuntimeArtifactPollingClient(
        artifact_root=root,
        now_fn=lambda: datetime.fromisoformat("2026-05-18T12:00:20+00:00"),
    )

    with pytest.raises(RuntimeError, match="stale"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))

    _write_phase1_runtime_artifact(root, bars=[])
    with pytest.raises(RuntimeError, match="no completed bars"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))


def test_phase1_runtime_artifact_polling_client_classifies_weekend_halt_stale_bars(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_phase1_runtime_artifact(
        root,
        generated_at="2026-05-22T21:00:00+00:00",
        bars=[
            {
                "bar_start": "2026-05-22T20:58:00+00:00",
                "bar_end": "2026-05-22T20:59:00+00:00",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 10,
                "completed": True,
            }
        ],
    )
    client = Phase1RuntimeArtifactPollingClient(
        artifact_root=root,
        now_fn=lambda: datetime.fromisoformat("2026-05-23T07:15:00+00:00"),
    )

    with pytest.raises(Phase1RuntimeArtifactMarketClosedError, match="MARKET_CLOSED_NO_FRESH_BARS"):
        client.poll_live_bars(None, "1m", SchwabLivePollRequest(internal_symbol="MNQ"))


def test_live_polling_service_persists_phase1_artifact_bars_without_duplicate_insertions(tmp_path: Path) -> None:
    root = tmp_path / "phase1_runtime_market_data"
    _write_phase1_runtime_artifact(root)
    repositories = RepositorySet(build_engine(f"sqlite:///{tmp_path / 'lane.sqlite3'}"))
    service = LivePollingService(
        adapter=None,
        client=Phase1RuntimeArtifactPollingClient(
            artifact_root=root,
            now_fn=lambda: datetime.fromisoformat("2026-05-18T12:00:20+00:00"),
        ),
        repositories=repositories,
        data_source="phase1_runtime_artifact",
        provider="databento_phase1_runtime_artifact",
        provenance_tag="DATABENTO_REALTIME_PHASE1",
    )

    service.poll_bars(SchwabLivePollRequest(internal_symbol="MNQ"), internal_timeframe="1m")
    service.poll_bars(SchwabLivePollRequest(internal_symbol="MNQ"), internal_timeframe="1m")

    assert repositories.bars.count() == 1


def test_historical_polling_live_client_caps_stale_since_window() -> None:
    now = datetime.fromisoformat("2026-04-30T12:00:00-04:00")
    fixed_datetime = SimpleNamespace(now=lambda _tz=None: now)
    settings = SimpleNamespace(timezone_info=now.tzinfo)
    adapter = SimpleNamespace(
        _settings=settings,
        map_timeframe=lambda _tf: SimpleNamespace(frequency_type="minute", frequency=1),
    )
    historical_client = _RecordingHistoricalClient()
    client = HistoricalPollingLiveClient(adapter=adapter, historical_client=historical_client, lookback_minutes=180)
    original_datetime = live_feed_module.datetime
    live_feed_module.datetime = fixed_datetime
    try:
        stale_since = now - timedelta(hours=20)
        client.poll_live_bars(
            "/MGC",
            "1m",
            SchwabLivePollRequest(internal_symbol="MGC", since=stale_since),
        )
    finally:
        live_feed_module.datetime = original_datetime

    _, request, _ = historical_client.calls[-1]
    expected_floor_ms = int((now - timedelta(minutes=180)).timestamp() * 1000)
    assert request.start_date_ms == expected_floor_ms


def test_historical_polling_live_client_preserves_recent_since_window() -> None:
    now = datetime.fromisoformat("2026-04-30T12:00:00-04:00")
    fixed_datetime = SimpleNamespace(now=lambda _tz=None: now)
    settings = SimpleNamespace(timezone_info=now.tzinfo)
    adapter = SimpleNamespace(
        _settings=settings,
        map_timeframe=lambda _tf: SimpleNamespace(frequency_type="minute", frequency=1),
    )
    historical_client = _RecordingHistoricalClient()
    client = HistoricalPollingLiveClient(adapter=adapter, historical_client=historical_client, lookback_minutes=180)
    original_datetime = live_feed_module.datetime
    live_feed_module.datetime = fixed_datetime
    try:
        recent_since = now - timedelta(minutes=10)
        client.poll_live_bars(
            "/MGC",
            "1m",
            SchwabLivePollRequest(internal_symbol="MGC", since=recent_since),
        )
    finally:
        live_feed_module.datetime = original_datetime

    _, request, _ = historical_client.calls[-1]
    expected_start_ms = int((recent_since - timedelta(minutes=1)).timestamp() * 1000)
    assert request.start_date_ms == expected_start_ms


def test_databento_historical_polling_live_client_caps_stale_since_window() -> None:
    now = datetime.fromisoformat("2026-04-30T12:00:00-04:00")
    fixed_datetime = SimpleNamespace(now=lambda _tz=None: now)
    provider = _RecordingDatabentoProvider(bars=[_databento_bar()])
    client = DatabentoHistoricalPollingClient(
        provider=provider,
        timezone_info=now.tzinfo,
        lookback_minutes=180,
    )
    original_datetime = live_feed_module.datetime
    live_feed_module.datetime = fixed_datetime
    try:
        stale_since = now - timedelta(hours=20)
        bars = client.poll_live_bars(
            None,
            "1m",
            SchwabLivePollRequest(internal_symbol="MGC", since=stale_since),
        )
    finally:
        live_feed_module.datetime = original_datetime

    request = provider.requests[-1]
    expected_end = databento_live_effective_end(now, "1m").astimezone(request.end.tzinfo)
    expected_floor = (expected_end - timedelta(minutes=180)).astimezone(request.start.tzinfo)
    assert request.start == expected_floor
    assert request.end == expected_end
    assert bars == [_databento_bar()]


def test_databento_historical_polling_live_client_clamps_end_behind_wall_clock() -> None:
    now = datetime.fromisoformat("2026-04-30T13:12:04-04:00")
    fixed_datetime = SimpleNamespace(now=lambda _tz=None: now)
    provider = _RecordingDatabentoProvider(bars=[_databento_bar()])
    client = DatabentoHistoricalPollingClient(
        provider=provider,
        timezone_info=now.tzinfo,
        lookback_minutes=180,
    )
    original_datetime = live_feed_module.datetime
    live_feed_module.datetime = fixed_datetime
    try:
        client.poll_live_bars(
            None,
            "1m",
            SchwabLivePollRequest(internal_symbol="MGC", since=now - timedelta(minutes=2)),
        )
    finally:
        live_feed_module.datetime = original_datetime

    request = provider.requests[-1]
    assert request.end == databento_live_effective_end(now, "1m").astimezone(request.end.tzinfo)


def test_databento_historical_polling_live_client_retries_with_available_end_when_end_ahead() -> None:
    now = datetime.fromisoformat("2026-05-01T00:11:15+00:00")
    provider = _RetryingDatabentoProvider(
        error_text=(
            "Databento HTTP error 422: "
            '{"detail":{"case":"data_end_after_available_end","payload":{"available_end":"2026-05-01T00:10:00+00:00"}}}'
        ),
        bars=[_bar_at("2026-05-01T00:10:00+00:00")],
    )
    client = DatabentoHistoricalPollingClient(
        provider=provider,
        timezone_info=now.tzinfo,
        lookback_minutes=180,
    )
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = now
    live_feed_module.datetime = _FixedDateTime
    try:
        bars = client.poll_live_bars(
            None,
            "1m",
            SchwabLivePollRequest(
                internal_symbol="MGC",
                since=datetime.fromisoformat("2026-05-01T00:09:00+00:00"),
            ),
        )
    finally:
        live_feed_module.datetime = original_datetime

    assert provider.call_count == 2
    assert provider.requests[0].end == datetime.fromisoformat("2026-05-01T00:11:00+00:00")
    assert provider.requests[1].end == datetime.fromisoformat("2026-05-01T00:10:00+00:00")
    assert provider.requests[1].start == datetime.fromisoformat("2026-05-01T00:08:00+00:00")
    assert bars == [_bar_at("2026-05-01T00:10:00+00:00")]


def test_databento_historical_polling_live_client_retries_latest_window_when_start_ahead() -> None:
    now = datetime.fromisoformat("2026-05-01T00:02:15+00:00")
    provider = _RetryingDatabentoProvider(
        error_text=(
            "Databento HTTP error 422: "
            '{"detail":{"case":"data_start_after_available_end","payload":{"available_end":"2026-05-01T00:00:00+00:00"}}}'
        ),
        bars=[_bar_at("2026-05-01T00:00:00+00:00")],
    )
    client = DatabentoHistoricalPollingClient(
        provider=provider,
        timezone_info=now.tzinfo,
        lookback_minutes=180,
    )
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = now
    live_feed_module.datetime = _FixedDateTime
    try:
        bars = client.poll_live_bars(
            None,
            "1m",
            SchwabLivePollRequest(
                internal_symbol="MGC",
                since=datetime.fromisoformat("2026-05-01T00:02:00+00:00"),
            ),
        )
    finally:
        live_feed_module.datetime = original_datetime

    assert provider.call_count == 2
    assert provider.requests[1].start == datetime.fromisoformat("2026-04-30T23:59:00+00:00")
    assert provider.requests[1].end == datetime.fromisoformat("2026-05-01T00:00:00+00:00")
    assert bars == [_bar_at("2026-05-01T00:00:00+00:00")]


def test_databento_live_effective_end_stays_on_latest_completed_minute() -> None:
    now = datetime.fromisoformat("2026-04-30T13:12:04-04:00")

    assert databento_live_effective_end(now, "1m") == datetime.fromisoformat("2026-04-30T13:11:00-04:00")


def test_databento_live_auth_response_suffixes_api_key_bucket() -> None:
    response = databento_live_auth_response(cram="server-challenge", api_key="db-live-test-abcde")

    assert response.endswith("-abcde")


def test_databento_live_format_timestamp_emits_utc_without_suffix() -> None:
    value = datetime.fromisoformat("2026-04-30T16:10:56-04:00")

    assert databento_live_format_timestamp(value) == "2026-04-30T20:10:56"


def test_databento_raw_live_session_accepts_nested_header_timestamp() -> None:
    session = _DatabentoRawLiveSession(
        api_key="test-key",
        dataset="GLBX.MDP3",
        request_symbol="MGC.v.0",
        stype_in="continuous",
        schema_name="ohlcv-1m",
        internal_symbol="MGC",
        internal_timeframe="1m",
        lookback_minutes=30,
    )

    bar = session._bar_from_live_record(  # noqa: SLF001
        {
            "hd": {
                "ts_event": "2026-04-30T20:36:00.000000000Z",
                "rtype": 33,
            },
            "open": "4630.900000000",
            "high": "4631.300000000",
            "low": "4630.500000000",
            "close": "4630.500000000",
            "volume": "38",
        }
    )

    assert bar is not None
    assert bar.start_ts == datetime.fromisoformat("2026-04-30T20:36:00+00:00")
    assert bar.end_ts == datetime.fromisoformat("2026-04-30T20:37:00+00:00")
    assert bar.close == Decimal("4630.500000000")


def test_databento_raw_live_session_caps_initial_replay_start_for_reopen() -> None:
    now = datetime.fromisoformat("2026-05-17T22:04:07+00:00")
    session = _DatabentoRawLiveSession(
        api_key="test-key",
        dataset="GLBX.MDP3",
        request_symbol="MGC.v.0",
        stype_in="continuous",
        schema_name="ohlcv-1m",
        internal_symbol="MGC",
        internal_timeframe="1m",
        lookback_minutes=180,
    )
    replay_start = session._next_replay_start(now_utc=now, timeframe_duration=timedelta(minutes=1))  # noqa: SLF001

    assert replay_start == now - timedelta(minutes=_DATABENTO_LIVE_REPLAY_LOOKBACK_CAP_MINUTES)


def test_live_polling_service_triggers_stale_recovery_attempt() -> None:
    client = _RecoveryClient(
        [
            [_bar_at("2026-05-01T00:02:00+00:00")],
            [_bar_at("2026-05-01T00:02:00+00:00")],
        ]
    )
    service = LivePollingService(adapter=None, client=client, data_source="databento_live", provider="databento_live")
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:30+00:00")
    live_feed_module.datetime = _FixedDateTime
    try:
        service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
        snapshot = service.market_data_recovery_snapshot(internal_symbol="MGC", internal_timeframe="1m")
    finally:
        live_feed_module.datetime = original_datetime

    assert len(client.recover_calls) == 1
    assert snapshot["recovery_root_cause"] == "SUBSCRIPTION_DROPPED"
    assert snapshot["recovery_action"] == "provider_resubscribe"


def test_live_polling_service_does_not_recover_inside_normal_publication_grace() -> None:
    client = _RecoveryClient([[_bar_at("2026-05-01T00:02:00+00:00")]])
    service = LivePollingService(adapter=None, client=client, data_source="databento_live", provider="databento_live")
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:05+00:00")
    live_feed_module.datetime = _FixedDateTime
    try:
        service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
        snapshot = service.market_data_recovery_snapshot(internal_symbol="MGC", internal_timeframe="1m")
    finally:
        live_feed_module.datetime = original_datetime

    assert client.recover_calls == []
    assert snapshot["market_data_recovery_state"] == "GRACE"
    assert snapshot["recovery_root_cause"] == "NORMAL_PUBLICATION_DELAY_WITHIN_GRACE"
    assert snapshot["stale"] is False


def test_live_polling_service_clears_stale_only_after_fresh_bar_observed() -> None:
    client = _RecoveryClient(
        [
            [_bar_at("2026-05-01T00:02:00+00:00")],
            [_bar_at("2026-05-01T00:03:00+00:00")],
        ]
    )
    service = LivePollingService(adapter=None, client=client, data_source="databento_live", provider="databento_live")
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:30+00:00")
    live_feed_module.datetime = _FixedDateTime
    try:
        bars = service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
        snapshot = service.market_data_recovery_snapshot(internal_symbol="MGC", internal_timeframe="1m")
    finally:
        live_feed_module.datetime = original_datetime

    assert len(client.recover_calls) == 1
    assert [bar.end_ts.isoformat() for bar in bars] == [
        "2026-05-01T00:02:00+00:00",
        "2026-05-01T00:03:00+00:00",
    ]
    assert snapshot["market_data_recovery_state"] == "RECOVERED"
    assert snapshot["recovery_result"] == "FRESH_BAR_OBSERVED"
    assert snapshot["latest_observed_bar_after_recovery"] == "2026-05-01T00:03:00+00:00"
    assert snapshot["recovered"] is True
    assert snapshot["stale"] is False


def test_live_polling_service_keeps_hard_blocker_when_recovery_fails() -> None:
    client = _RecoveryClient(
        [
            [_bar_at("2026-05-01T00:02:00+00:00")],
            [_bar_at("2026-05-01T00:02:00+00:00")],
        ],
        recover_ok=True,
    )
    service = LivePollingService(adapter=None, client=client, data_source="databento_live", provider="databento_live")
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:30+00:00")
    live_feed_module.datetime = _FixedDateTime
    try:
        service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
        snapshot = service.market_data_recovery_snapshot(internal_symbol="MGC", internal_timeframe="1m")
    finally:
        live_feed_module.datetime = original_datetime

    assert len(client.recover_calls) == 1
    assert snapshot["market_data_recovery_state"] == "FAILED"
    assert snapshot["recovery_result"] == "NO_FRESH_BAR_AFTER_RECOVERY"
    assert snapshot["stale"] is True
    assert snapshot["recovered"] is False


def test_live_polling_service_rate_limits_recovery_attempts() -> None:
    client = _RecoveryClient(
        [
            [_bar_at("2026-05-01T00:02:00+00:00")],
            [_bar_at("2026-05-01T00:02:00+00:00")],
            [_bar_at("2026-05-01T00:02:00+00:00")],
        ]
    )
    service = LivePollingService(adapter=None, client=client, data_source="databento_live", provider="databento_live")
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:30+00:00")
    live_feed_module.datetime = _FixedDateTime
    try:
        service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
        _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:35+00:00")
        service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
        snapshot = service.market_data_recovery_snapshot(internal_symbol="MGC", internal_timeframe="1m")
    finally:
        live_feed_module.datetime = original_datetime

    assert len(client.recover_calls) == 1
    assert snapshot["market_data_recovery_state"] == "COOLDOWN_ACTIVE"
    assert snapshot["recovery_result"] == "RATE_LIMITED"


def test_live_polling_service_scopes_recovery_to_the_affected_symbol() -> None:
    client = _RecoveryClient(
        [
            [_bar_at("2026-05-01T00:02:00+00:00", symbol="MGC")],
            [_bar_at("2026-05-01T00:02:00+00:00", symbol="MGC")],
        ]
    )
    service = LivePollingService(adapter=None, client=client, data_source="databento_live", provider="databento_live")
    original_datetime = live_feed_module.datetime
    _FixedDateTime.fixed_now = datetime.fromisoformat("2026-05-01T00:03:30+00:00")
    live_feed_module.datetime = _FixedDateTime
    try:
        service.poll_bars(SchwabLivePollRequest(internal_symbol="MGC"), internal_timeframe="1m")
    finally:
        live_feed_module.datetime = original_datetime

    assert client.recover_calls == [
        {
            "internal_symbol": "MGC",
            "internal_timeframe": "1m",
            "reason": "SUBSCRIPTION_DROPPED",
        }
    ]
