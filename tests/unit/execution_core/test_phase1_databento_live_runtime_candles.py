from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from mgc_v05l.execution_core.phase1_databento_live_runtime_candles import (
    Phase1DatabentoLiveRuntimeCandlesConfig,
    build_phase1_databento_live_runtime_candles,
)
from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    Phase1RuntimeDataReadinessConfig,
    build_phase1_runtime_data_readiness,
)
from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER
from mgc_v05l.execution_core.track_b_databento_live_runtime_feed import (
    TrackBDatabentoLiveFeedConfig,
    TrackBDatabentoLiveFeedResult,
    TrackBDatabentoLiveFeedVerdict,
)

NOW = datetime(2026, 5, 11, 0, 13, tzinfo=timezone.utc)


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


def test_all_10_phase1_symbols_are_requested_and_fail_closed_without_records(tmp_path: Path) -> None:
    runner = RecordingRunner({})

    result = build_phase1_databento_live_runtime_candles(config=_config(tmp_path), live_runner=runner)

    assert {config.instrument_family for config in runner.configs} == set(PHASE1_RUNTIME_TICKER_ORDER)
    assert {config.databento_continuous_symbol for config in runner.configs} == {
        f"{symbol}.v.0" for symbol in PHASE1_RUNTIME_TICKER_ORDER
    }
    assert result.report["phase1_symbol_count"] == 10
    assert result.report["realtime_feed_confirmed_count"] == 0
    assert all(row["realtime_feed_confirmed"] is False for row in result.report["rows"])
    assert all(row["can_submit"] is False for row in result.report["rows"])
    assert all(row["live_money_eligible"] is False for row in result.report["rows"])


def test_multi_symbol_subscriptions_run_concurrently(tmp_path: Path) -> None:
    barrier = threading.Barrier(len(PHASE1_RUNTIME_TICKER_ORDER))
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

    assert started == set(PHASE1_RUNTIME_TICKER_ORDER)
    assert result.report["phase1_symbol_count"] == 10


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


def test_no_broker_or_paper_proof_terms_in_phase1_live_module() -> None:
    text = Path("src/mgc_v05l/execution_core/phase1_databento_live_runtime_candles.py").read_text(encoding="utf-8")

    assert "placeOrder" not in text
    assert "cancelOrder" not in text
    assert "paper_proof" not in text
