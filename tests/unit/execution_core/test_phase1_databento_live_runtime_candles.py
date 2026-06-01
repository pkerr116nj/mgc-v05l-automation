from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.phase1_databento_live_runtime_candles import (
    Phase1DatabentoLiveListenerConfig,
    Phase1DatabentoLiveRuntimeCandlesConfig,
    build_phase1_databento_live_runtime_candles,
    run_phase1_databento_live_listener,
)
from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    Phase1RuntimeDataReadinessConfig,
    build_phase1_runtime_data_readiness,
)
from mgc_v05l.execution_core.track_b_session_anchor_resolver import (
    SessionAnchorStatus,
    TrackBSessionAnchorConfig,
    resolve_session_anchor,
)
from mgc_v05l.execution_core.track_b_databento_live_runtime_feed import (
    TrackBDatabentoLiveFeedConfig,
    TrackBDatabentoLiveFeedResult,
    TrackBDatabentoLiveFeedVerdict,
)
from mgc_v05l.execution_core.track_b_live_market_data_symbols import load_track_b_live_market_data_symbols

NOW = datetime(2026, 5, 11, 0, 13, tzinfo=timezone.utc)


def _configured_live_symbols() -> tuple[str, ...]:
    return tuple(row.symbol for row in load_track_b_live_market_data_symbols().enabled_symbols())


def _configured_databento_symbols() -> tuple[str, ...]:
    return tuple(row.databento_symbol for row in load_track_b_live_market_data_symbols().enabled_symbols())


@dataclass
class RecordingRunner:
    candles_by_symbol: dict[str, list[dict[str, Any]]]

    def __post_init__(self) -> None:
        self.configs: list[TrackBDatabentoLiveFeedConfig] = []

    def __call__(self, config: TrackBDatabentoLiveFeedConfig) -> TrackBDatabentoLiveFeedResult:
        self.configs.append(config)
        symbol = config.instrument_family
        candles = self.candles_by_symbol.get(symbol, [])
        report_json = Path(config.output_root) / f"{symbol.lower()}_report.json"
        report_json.parent.mkdir(parents=True, exist_ok=True)
        connected = bool(candles)
        report = {
            "generated_at": NOW.isoformat(),
            "live_feed_connected": connected,
            "fresh_for_execution": connected,
            "live_runtime_feed_verdict": TrackBDatabentoLiveFeedVerdict.DATA_WRITTEN_EXECUTION_FRESH.value
            if connected
            else TrackBDatabentoLiveFeedVerdict.BLOCKED_NO_RECORDS.value,
            "primary_blocker": None if connected else "Databento Live produced no records within bounded wait.",
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        event = None if not connected else {"generated_at": NOW.isoformat(), "candles": candles}
        report_json.write_text(json.dumps(report), encoding="utf-8")
        return TrackBDatabentoLiveFeedResult(
            verdict=TrackBDatabentoLiveFeedVerdict.DATA_WRITTEN_EXECUTION_FRESH
            if connected
            else TrackBDatabentoLiveFeedVerdict.BLOCKED_NO_RECORDS,
            report_json=report_json,
            report=report,
            live_1m_candles_json=None,
            live_1m_candles_event=event,
        )


@dataclass
class SequentialRunner:
    candles_by_symbol: dict[str, list[list[dict[str, Any]]]]

    def __post_init__(self) -> None:
        self.call_counts: dict[str, int] = {}

    def __call__(self, config: TrackBDatabentoLiveFeedConfig) -> TrackBDatabentoLiveFeedResult:
        symbol = config.instrument_family
        count = self.call_counts.get(symbol, 0)
        self.call_counts[symbol] = count + 1
        sequence = self.candles_by_symbol.get(symbol, [])
        candles = sequence[min(count, len(sequence) - 1)] if sequence else []
        return RecordingRunner({symbol: candles})(config)


@dataclass
class FakeOhlcvRecord:
    symbol: str
    ts_event: datetime
    open: int = 100_000_000_000
    high: int = 101_000_000_000
    low: int = 99_000_000_000
    close: int = 100_500_000_000
    volume: int = 10


@dataclass
class FakeMappingRecord:
    instrument_id: int
    stype_in_symbol: str


class FakeLiveClient:
    def __init__(self, records: list[Any]) -> None:
        self.records = records
        self.callback: Any = None
        self.exception_callback: Any = None
        self.stream: Any = None
        self.subscribe_kwargs: dict[str, Any] | None = None
        self.events: list[str] = []

    def add_callback(self, callback: Any, exception_callback: Any) -> None:
        self.events.append("add_callback")
        self.callback = callback
        self.exception_callback = exception_callback

    def add_stream(self, stream: Any, exception_callback: Any | None = None) -> None:
        self.events.append("add_stream")
        self.stream = stream

    def subscribe(self, **kwargs: Any) -> None:
        self.events.append("subscribe")
        self.subscribe_kwargs = kwargs

    def start(self) -> None:
        self.events.append("start")
        for record in self.records:
            self.callback(record)

    def block_for_close(self, timeout: float | None = None) -> None:
        self.events.append("block_for_close")
        raise TimeoutError()

    def stop(self) -> None:
        self.events.append("stop")

    def terminate(self) -> None:
        self.events.append("terminate")


def _config(root: Path, **overrides: object) -> Phase1DatabentoLiveRuntimeCandlesConfig:
    values = {
        "repo_root": root,
        "runtime_candle_root": Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data",
        "report_dir": Path("outputs") / "reports" / "phase1_databento_live_runtime_candles",
        "legacy_live_output_root": Path("outputs") / "track_b_execution_core" / "databento_live_runtime_feed",
        "now": NOW,
        "max_seconds_per_symbol": 0.01,
    }
    values.update(overrides)
    return Phase1DatabentoLiveRuntimeCandlesConfig(**values)


def _live_candles(count: int = 10, *, end: datetime = NOW - timedelta(minutes=1)) -> list[dict[str, Any]]:
    start = end - timedelta(minutes=count - 1)
    rows: list[dict[str, Any]] = []
    for index in range(count):
        ts = start + timedelta(minutes=index)
        rows.append(
            {
                "candle_timestamp": ts.isoformat(),
                "open": str(100 + index),
                "high": str(101 + index),
                "low": str(99 + index),
                "close": str(100.5 + index),
                "volume": str(10 + index),
            }
        )
    return rows


def _listener_config(root: Path, **overrides: object) -> Phase1DatabentoLiveListenerConfig:
    env_file = root / ".env.local"
    env_file.write_text("DATABENTO_API_KEY=test-key\n", encoding="utf-8")
    values = {
        "repo_root": root,
        "runtime_candle_root": Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data",
        "report_dir": Path("outputs") / "reports" / "phase1_databento_live_runtime_candles",
        "raw_dbn_root": Path("outputs") / "track_b_execution_core" / "phase1_databento_live_raw_dbn",
        "symbols": ("GC",),
        "env_file": env_file,
        "run_seconds": 0.01,
        "now": NOW,
    }
    values.update(overrides)
    return Phase1DatabentoLiveListenerConfig(**values)


def _live_records(count: int = 10, *, symbol: str = "GC", end: datetime = NOW - timedelta(minutes=1)) -> list[Any]:
    start = end - timedelta(minutes=count - 1)
    return [FakeOhlcvRecord(symbol=symbol, ts_event=start + timedelta(minutes=index)) for index in range(count)]


def _write_existing_phase1_payloads(root: Path, symbol: str, *, generated_at: datetime = NOW) -> None:
    for timeframe, count in (("1m", 10), ("3m", 3), ("5m", 2)):
        path = (
            root
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / symbol
            / timeframe
            / "latest_runtime_candles.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        bars = [
            {
                "bar_start": (generated_at - timedelta(minutes=index + 1)).isoformat(),
                "bar_end": (generated_at - timedelta(minutes=index)).isoformat(),
                "open": 1,
                "high": 1,
                "low": 1,
                "close": 1,
                "volume": 1,
                "completed": True,
            }
            for index in range(count, 0, -1)
        ]
        path.write_text(
            json.dumps(
                {
                    "source": "DATABENTO_REALTIME_PHASE1",
                    "source_id": f"DATABENTO_REALTIME_PHASE1_{symbol.lower()}",
                    "generated_at": generated_at.isoformat(),
                    "symbol": symbol,
                    "instrument": symbol,
                    "root": symbol,
                    "timeframe": timeframe,
                    "bar_count": count,
                    "first_bar_ts": bars[0]["bar_end"],
                    "last_completed_bar_ts": bars[-1]["bar_end"],
                    "historical_seed_ready": False,
                    "realtime_feed_confirmed": True,
                    "research_artifact_used": False,
                    "archive_artifact_used": False,
                    "completed_candles_only": True,
                    "can_submit": False,
                    "live_money_eligible": False,
                    "bars": bars,
                }
            ),
            encoding="utf-8",
        )


def test_phase1_live_writer_writes_1m_3m_5m_artifacts_and_readiness_passes(tmp_path: Path) -> None:
    runner = RecordingRunner({"GC": _live_candles(10)})

    result = build_phase1_databento_live_runtime_candles(
        config=_config(tmp_path, symbols=("GC",)),
        live_runner=runner,
    )

    assert result.report["realtime_feed_confirmed_count"] == 1
    assert result.report["can_submit"] is False
    assert result.report["live_money_eligible"] is False
    assert runner.configs[0].databento_continuous_symbol == "GC.v.0"
    assert runner.configs[0].max_seconds == 0.01

    for timeframe in ("1m", "3m", "5m"):
        path = (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / "GC"
            / timeframe
            / "latest_runtime_candles.json"
        )
        assert path.exists()
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["source"] == "DATABENTO_REALTIME_PHASE1"
        assert payload["symbol"] == "GC"
        assert payload["timeframe"] == timeframe
        assert payload["realtime_feed_confirmed"] is True
        assert payload["historical_seed_ready"] is False
        assert payload["research_artifact_used"] is False
        assert payload["archive_artifact_used"] is False
        assert payload["completed_candles_only"] is True
        assert payload["can_submit"] is False
        assert payload["live_money_eligible"] is False

    readiness = build_phase1_runtime_data_readiness(config=Phase1RuntimeDataReadinessConfig(repo_root=tmp_path, now=NOW))
    gc = next(row for row in readiness.rows if row["symbol"] == "GC")
    assert gc["realtime_feed_confirmed"] is True
    assert gc["runtime_candles_ready"] is True


def test_symbol_accumulation_retries_until_live_bars_are_complete(tmp_path: Path) -> None:
    runner = SequentialRunner({"GC": [_live_candles(1), _live_candles(10)]})

    result = build_phase1_databento_live_runtime_candles(
        config=_config(tmp_path, symbols=("GC",), max_accumulation_attempts=3),
        live_runner=runner,
    )

    assert runner.call_counts["GC"] == 2
    assert result.report["realtime_feed_confirmed_count"] == 1
    assert result.report["rows"][0]["realtime_feed_confirmed"] is True
    assert result.report["rows"][0]["attempt_count"] == 2


def test_partial_refresh_does_not_overwrite_existing_fresh_confirmed_phase1_artifacts(tmp_path: Path) -> None:
    _write_existing_phase1_payloads(tmp_path, "GC")
    original = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "GC"
        / "1m"
        / "latest_runtime_candles.json"
    ).read_text(encoding="utf-8")
    runner = RecordingRunner({"GC": _live_candles(1)})

    result = build_phase1_databento_live_runtime_candles(
        config=_config(tmp_path, symbols=("GC",), max_accumulation_attempts=2),
        live_runner=runner,
    )

    assert result.report["rows"][0]["realtime_feed_confirmed"] is True
    assert result.report["rows"][0]["block_reason"] == "PRESERVED_EXISTING_CONFIRMED_RUNTIME_CANDLES"
    assert result.artifacts_written == []
    assert (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "GC"
        / "1m"
        / "latest_runtime_candles.json"
    ).read_text(encoding="utf-8") == original


def test_gc_can_confirm_even_when_other_phase1_symbols_are_thin(tmp_path: Path) -> None:
    runner = RecordingRunner({"GC": _live_candles(10), "NQ": _live_candles(1)})

    result = build_phase1_databento_live_runtime_candles(
        config=_config(tmp_path, symbols=("GC", "NQ"), max_accumulation_attempts=2),
        live_runner=runner,
    )

    rows = {row["symbol"]: row for row in result.report["rows"]}
    assert rows["GC"]["realtime_feed_confirmed"] is True
    assert rows["NQ"]["realtime_feed_confirmed"] is False
    assert result.report["realtime_feed_confirmed_count"] == 1
    assert result.report["final_classification"] == "PHASE1_REALTIME_MARKET_DATA_PARTIAL_OR_BLOCKED"


def test_realtime_confirmation_requires_fresh_complete_live_bars(tmp_path: Path) -> None:
    stale_runner = RecordingRunner({"GC": _live_candles(10, end=NOW - timedelta(minutes=30))})
    stale = build_phase1_databento_live_runtime_candles(config=_config(tmp_path, symbols=("GC",)), live_runner=stale_runner)
    assert stale.report["rows"][0]["realtime_feed_confirmed"] is False
    one_minute = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "GC"
        / "1m"
        / "latest_runtime_candles.json"
    )
    assert json.loads(one_minute.read_text(encoding="utf-8"))["realtime_feed_block_reason"] == "LIVE_BARS_STALE"

    sparse_runner = RecordingRunner({"NQ": _live_candles(2)})
    sparse = build_phase1_databento_live_runtime_candles(config=_config(tmp_path, symbols=("NQ",)), live_runner=sparse_runner)
    assert sparse.report["rows"][0]["realtime_feed_confirmed"] is False
    nq_one_minute = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "NQ"
        / "1m"
        / "latest_runtime_candles.json"
    )
    assert json.loads(nq_one_minute.read_text(encoding="utf-8"))["realtime_feed_block_reason"] == "INSUFFICIENT_LIVE_BARS"


def test_fresh_merged_legacy_live_artifact_can_satisfy_phase1_contract(tmp_path: Path) -> None:
    legacy = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "databento_live_runtime_feed"
        / "latest_live_gc_1m_candles.json"
    )
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(
        json.dumps(
            {
                "source_id": "DATABENTO_REALTIME_PHASE1_gc",
                "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
                "fresh_for_execution": True,
                "generated_at": NOW.isoformat(),
                "candles": _live_candles(10),
            }
        ),
        encoding="utf-8",
    )
    runner = RecordingRunner({"GC": []})

    result = build_phase1_databento_live_runtime_candles(
        config=_config(tmp_path, symbols=("GC",)),
        live_runner=runner,
    )

    assert result.report["rows"][0]["realtime_feed_confirmed"] is True
    payload = json.loads(
        (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / "GC"
            / "1m"
            / "latest_runtime_candles.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["realtime_feed_confirmed"] is True
    assert payload["historical_seed_ready"] is False


def test_all_phase1_symbols_are_requested_and_fail_closed_without_records(tmp_path: Path) -> None:
    runner = RecordingRunner({})
    configured_symbols = _configured_live_symbols()

    result = build_phase1_databento_live_runtime_candles(config=_config(tmp_path), live_runner=runner)

    assert {config.instrument_family for config in runner.configs} == set(configured_symbols)
    assert {config.databento_continuous_symbol for config in runner.configs} == set(_configured_databento_symbols())
    assert result.report["symbols"] == list(configured_symbols)
    assert result.report["requested_symbols"] == list(_configured_databento_symbols())
    assert result.report["phase1_symbol_count"] == len(configured_symbols)
    assert result.report["realtime_feed_confirmed_count"] == 0
    assert all(row["realtime_feed_confirmed"] is False for row in result.report["rows"])
    assert all(row["can_submit"] is False for row in result.report["rows"])
    assert all(row["live_money_eligible"] is False for row in result.report["rows"])


def test_multi_symbol_subscriptions_run_concurrently(tmp_path: Path) -> None:
    configured_symbols = _configured_live_symbols()
    barrier = threading.Barrier(len(configured_symbols))
    started: set[str] = set()
    lock = threading.Lock()

    def runner(config: TrackBDatabentoLiveFeedConfig) -> TrackBDatabentoLiveFeedResult:
        with lock:
            started.add(config.instrument_family)
        barrier.wait(timeout=2.0)
        report_json = Path(config.output_root) / f"{config.instrument_family.lower()}_report.json"
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report = {
            "generated_at": NOW.isoformat(),
            "live_feed_connected": False,
            "live_runtime_feed_verdict": TrackBDatabentoLiveFeedVerdict.BLOCKED_NO_RECORDS.value,
            "primary_blocker": "Databento Live produced no records within bounded wait.",
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        report_json.write_text(json.dumps(report), encoding="utf-8")
        return TrackBDatabentoLiveFeedResult(
            verdict=TrackBDatabentoLiveFeedVerdict.BLOCKED_NO_RECORDS,
            report_json=report_json,
            report=report,
            live_1m_candles_json=None,
            live_1m_candles_event=None,
        )

    result = build_phase1_databento_live_runtime_candles(config=_config(tmp_path, max_workers=1), live_runner=runner)

    assert started == set(configured_symbols)
    assert result.report["phase1_symbol_count"] == len(configured_symbols)


def test_sunday_evening_globex_timestamp_is_accepted_as_asia_session(tmp_path: Path) -> None:
    sunday_evening = datetime(2026, 5, 10, 22, 13, tzinfo=timezone.utc)
    runner = RecordingRunner({"GC": _live_candles(10, end=sunday_evening - timedelta(minutes=1))})

    build_phase1_databento_live_runtime_candles(
        config=_config(tmp_path, symbols=("GC",), now=sunday_evening),
        live_runner=runner,
    )

    payload = json.loads(
        (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / "GC"
            / "1m"
            / "latest_runtime_candles.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["sunday_session_label"] == "ASIA_EARLY"
    assert payload["sunday_globex_session_supported"] is True


def test_live_listener_follows_databento_session_pattern_and_writes_raw_stream_path(tmp_path: Path) -> None:
    client = FakeLiveClient(_live_records(10))

    result = run_phase1_databento_live_listener(
        config=_listener_config(tmp_path),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    assert client.events[:4] == ["add_callback", "add_stream", "subscribe", "start"]
    assert "block_for_close" in client.events
    assert client.subscribe_kwargs == {
        "dataset": "GLBX.MDP3",
        "schema": "ohlcv-1m",
        "symbols": ["GC.v.0"],
        "stype_in": "continuous",
    }
    assert str(result.raw_dbn_path).endswith(".dbn")
    assert "phase1_databento_live_raw_dbn" in str(result.raw_dbn_path)
    assert result.status["databento_pattern"] == "db.Live + add_stream(raw DBN) + subscribe before start + start + block_for_close"
    assert result.status["can_submit"] is False
    assert result.status["live_money_eligible"] is False


def test_live_listener_default_symbols_come_from_enabled_namelist_rows(tmp_path: Path) -> None:
    client = FakeLiveClient([])
    namelist = load_track_b_live_market_data_symbols()

    result = run_phase1_databento_live_listener(
        config=_listener_config(tmp_path, symbols=None),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    assert result.status["symbols"] == [row.symbol for row in namelist.enabled_symbols()]
    assert result.status["requested_symbols"] == [row.databento_symbol for row in namelist.enabled_symbols()]
    assert result.status["required_for_readiness_symbols"] == [
        row.symbol for row in namelist.required_for_readiness_symbols()
    ]
    assert result.status["optional_symbols"] == [row.symbol for row in namelist.optional_symbols()]
    assert result.status["disabled_symbols"] == [row.symbol for row in namelist.disabled_symbols()]
    assert client.subscribe_kwargs is not None
    assert client.subscribe_kwargs["symbols"] == [row.databento_symbol for row in namelist.enabled_symbols()]
    assert result.status["can_submit"] is False
    assert result.status["live_money_eligible"] is False


def test_live_listener_rolls_1m_3m_5m_artifacts_from_live_records(tmp_path: Path) -> None:
    client = FakeLiveClient(_live_records(10))

    result = run_phase1_databento_live_listener(
        config=_listener_config(tmp_path),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    assert result.status["realtime_feed_confirmed_count"] == 1
    for timeframe in ("1m", "3m", "5m"):
        path = (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / "GC"
            / timeframe
            / "latest_runtime_candles.json"
        )
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["source"] == "DATABENTO_REALTIME_PHASE1"
        assert payload["raw_dbn_evidence_path"].endswith(".dbn")
        assert payload["realtime_feed_confirmed"] is True
        assert payload["historical_seed_ready"] is False
        assert payload["research_artifact_used"] is False
        assert payload["archive_artifact_used"] is False
        assert payload["can_submit"] is False
        assert payload["live_money_eligible"] is False


def test_live_listener_stale_sparse_data_fails_closed(tmp_path: Path) -> None:
    client = FakeLiveClient(_live_records(1))

    result = run_phase1_databento_live_listener(
        config=_listener_config(tmp_path),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    assert result.status["realtime_feed_confirmed_count"] == 0
    payload = json.loads(
        (
            tmp_path
            / "outputs"
            / "track_b_execution_core"
            / "phase1_runtime_market_data"
            / "GC"
            / "1m"
            / "latest_runtime_candles.json"
        ).read_text(encoding="utf-8")
    )
    assert payload["realtime_feed_confirmed"] is False
    assert payload["realtime_feed_block_reason"] == "INSUFFICIENT_LIVE_BARS"


def test_live_listener_preserves_existing_confirmed_artifact_on_partial_update(tmp_path: Path) -> None:
    _write_existing_phase1_payloads(tmp_path, "GC")
    one_minute = (
        tmp_path
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / "GC"
        / "1m"
        / "latest_runtime_candles.json"
    )
    original = one_minute.read_text(encoding="utf-8")
    client = FakeLiveClient(_live_records(1))

    run_phase1_databento_live_listener(
        config=_listener_config(tmp_path),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    assert one_minute.read_text(encoding="utf-8") == original


def test_live_listener_uses_symbol_mapping_messages_for_multi_symbol_session(tmp_path: Path) -> None:
    records = [
        FakeMappingRecord(instrument_id=1, stype_in_symbol="GC.v.0"),
        FakeMappingRecord(instrument_id=2, stype_in_symbol="NQ.v.0"),
    ]
    records.extend([FakeOhlcvRecord(symbol="", ts_event=row.ts_event, open=row.open, high=row.high, low=row.low, close=row.close, volume=row.volume) for row in _live_records(10)])
    for record in records[2:]:
        record.instrument_id = 1  # type: ignore[attr-defined]
    client = FakeLiveClient(records)

    result = run_phase1_databento_live_listener(
        config=_listener_config(tmp_path, symbols=("GC", "NQ")),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    assert client.subscribe_kwargs["symbols"] == ["GC.v.0", "NQ.v.0"]
    rows = {row["symbol"]: row for row in result.status["rows"]}
    assert rows["GC"]["realtime_feed_confirmed"] is True
    assert rows["NQ"]["realtime_feed_confirmed"] is False


def test_live_listener_symbol_mapping_distinguishes_micros_from_full_size_roots(tmp_path: Path) -> None:
    records = [
        FakeMappingRecord(instrument_id=1, stype_in_symbol="GC.v.0"),
        FakeMappingRecord(instrument_id=2, stype_in_symbol="MGC.v.0"),
        FakeMappingRecord(instrument_id=3, stype_in_symbol="NQ.v.0"),
        FakeMappingRecord(instrument_id=4, stype_in_symbol="MNQ.v.0"),
        FakeMappingRecord(instrument_id=5, stype_in_symbol="ES.v.0"),
        FakeMappingRecord(instrument_id=6, stype_in_symbol="MES.v.0"),
    ]
    for instrument_id in range(1, 7):
        for row in _live_records(10, symbol=""):
            row.instrument_id = instrument_id  # type: ignore[attr-defined]
            records.append(row)
    client = FakeLiveClient(records)

    result = run_phase1_databento_live_listener(
        config=_listener_config(tmp_path, symbols=("GC", "MGC", "NQ", "MNQ", "ES", "MES")),
        live_client_factory=lambda _key: client,
        now_func=lambda: NOW,
    )

    rows = {row["symbol"]: row for row in result.status["rows"]}
    assert set(rows) == {"GC", "MGC", "NQ", "MNQ", "ES", "MES"}
    assert all(row["bar_count"] == 10 for row in rows.values())
    assert all(row["realtime_feed_confirmed"] is True for row in rows.values())
    for symbol in ("MGC", "MNQ", "MES"):
        payload = json.loads(
            (
                tmp_path
                / "outputs"
                / "track_b_execution_core"
                / "phase1_runtime_market_data"
                / symbol
                / "1m"
                / "latest_runtime_candles.json"
            ).read_text(encoding="utf-8")
        )
        assert payload["symbol"] == symbol
        assert payload["realtime_feed_confirmed"] is True


def test_live_listener_retains_current_session_intraday_backfill_for_session_anchors(tmp_path: Path) -> None:
    now = datetime(2026, 6, 1, 18, 42, tzinfo=timezone.utc)
    records = _live_records(312, symbol="MNQ", end=now - timedelta(minutes=1))
    records.extend(_live_records(312, symbol="MES", end=now - timedelta(minutes=1)))
    client = FakeLiveClient(records)

    run_phase1_databento_live_listener(
        config=_listener_config(tmp_path, symbols=("MNQ", "MES"), now=now, max_bars=90),
        live_client_factory=lambda _key: client,
        now_func=lambda: now,
    )

    for symbol in ("MNQ", "MES"):
        hot_payload = json.loads(
            (
                tmp_path
                / f"outputs/track_b_execution_core/phase1_runtime_market_data/{symbol}/1m/latest_runtime_candles.json"
            ).read_text(encoding="utf-8")
        )
        assert hot_payload["bar_count"] == 90
        assert all(str(bar["bar_end"]) != "2026-06-01T13:30:00+00:00" for bar in hot_payload["bars"])

        backfill_path = (
            tmp_path
            / f"outputs/track_b_execution_core/phase1_runtime_market_data_intraday_backfill/{symbol}/1m/latest_runtime_candles.json"
        )
        backfill_payload = json.loads(backfill_path.read_text(encoding="utf-8"))
        assert backfill_payload["source"] == "RECOVERED_PHASE1_1M"
        assert backfill_payload["anchor_recovery_backfill"] is True
        assert any(str(bar["bar_end"]) == "2026-06-01T13:30:00+00:00" for bar in backfill_payload["bars"])

        anchor = resolve_session_anchor(
            symbol,
            "US_0930_OPEN",
            now,
            config=TrackBSessionAnchorConfig(repo_root=tmp_path),
        )
        assert anchor.status == SessionAnchorStatus.READY
        assert anchor.source == "RECOVERED_PHASE1_1M"


def test_no_broker_or_paper_proof_terms_in_phase1_live_module() -> None:
    text = Path("src/mgc_v05l/execution_core/phase1_databento_live_runtime_candles.py").read_text(encoding="utf-8")

    assert "placeOrder" not in text
    assert "cancelOrder" not in text
    assert "paper_proof" not in text
