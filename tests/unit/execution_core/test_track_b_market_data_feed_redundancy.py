from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core import track_b_market_data_feed_redundancy as redundancy
from mgc_v05l.execution_core.track_b_market_data_feed_redundancy import (
    BACKUP_FEED_HEALTHY,
    FEEDS_ALIGNED,
    FEEDS_DIVERGENT,
    IBKR_FEED_NOT_CONFIGURED,
    NO_TRUSTED_FEED,
    PRIMARY_FEED_HEALTHY,
    PRIMARY_STALE_BACKUP_HEALTHY,
    TrackBMarketDataFeedRedundancyConfig,
    build_track_b_market_data_feed_redundancy_audit,
    write_track_b_market_data_feed_redundancy_artifacts,
)


NOW = datetime(2026, 5, 22, 21, 1, tzinfo=UTC)


def test_databento_healthy_ibkr_missing_reports_not_configured(tmp_path: Path) -> None:
    _write_databento_bar(tmp_path, "MGC", "1m", NOW - timedelta(seconds=60), close=4511.0)
    _write_databento_bar(tmp_path, "MGC", "5m", NOW - timedelta(seconds=60), close=4511.0)

    artifacts = build_track_b_market_data_feed_redundancy_audit(config=_config(tmp_path, symbols=("MGC",)))

    assert PRIMARY_FEED_HEALTHY in artifacts.feed_comparison["classifications"]
    assert IBKR_FEED_NOT_CONFIGURED in artifacts.feed_comparison["classifications"]
    ibkr = _feed(artifacts.feed_health, "IBKR")
    assert ibkr["status"] == "NOT_CONFIGURED"
    assert ibkr["connected"] is False
    assert ibkr["timeframe_available"] == []


def test_both_feeds_healthy_aligned(tmp_path: Path) -> None:
    ts = NOW - timedelta(seconds=60)
    _write_databento_bar(tmp_path, "MGC", "1m", ts, close=4511.0)
    _write_ibkr_bar(tmp_path, "MGC", "1m", ts, close=4511.1)

    artifacts = build_track_b_market_data_feed_redundancy_audit(
        config=_config(tmp_path, symbols=("MGC",), timeframes=("1m",), price_tolerance=0.25)
    )

    assert PRIMARY_FEED_HEALTHY in artifacts.feed_comparison["classifications"]
    assert BACKUP_FEED_HEALTHY in artifacts.feed_comparison["classifications"]
    assert FEEDS_ALIGNED in artifacts.feed_comparison["classifications"]
    assert artifacts.feed_comparison["comparisons"][0]["classification"] == FEEDS_ALIGNED


def test_primary_stale_backup_healthy(tmp_path: Path) -> None:
    _write_databento_bar(tmp_path, "MGC", "1m", NOW - timedelta(minutes=30), close=4511.0)
    _write_ibkr_bar(tmp_path, "MGC", "1m", NOW - timedelta(seconds=60), close=4511.0)

    artifacts = build_track_b_market_data_feed_redundancy_audit(
        config=_config(tmp_path, symbols=("MGC",), timeframes=("1m",))
    )

    assert PRIMARY_STALE_BACKUP_HEALTHY in artifacts.feed_comparison["classifications"]
    assert artifacts.feed_comparison["comparisons"][0]["classification"] == PRIMARY_STALE_BACKUP_HEALTHY


def test_divergent_prices(tmp_path: Path) -> None:
    ts = NOW - timedelta(seconds=60)
    _write_databento_bar(tmp_path, "MGC", "1m", ts, close=4511.0)
    _write_ibkr_bar(tmp_path, "MGC", "1m", ts, close=4515.0)

    artifacts = build_track_b_market_data_feed_redundancy_audit(
        config=_config(tmp_path, symbols=("MGC",), timeframes=("1m",), price_tolerance=0.25)
    )

    row = artifacts.feed_comparison["comparisons"][0]
    assert FEEDS_DIVERGENT in artifacts.feed_comparison["classifications"]
    assert row["classification"] == FEEDS_DIVERGENT
    assert row["ohlc_price_difference"]["close"] == 4.0


def test_no_trusted_feed(tmp_path: Path) -> None:
    _write_databento_bar(
        tmp_path,
        "MGC",
        "1m",
        NOW - timedelta(seconds=60),
        close=4511.0,
        source="RESEARCH_ARCHIVE",
    )
    _write_ibkr_bar(tmp_path, "MGC", "1m", NOW - timedelta(minutes=30), close=4511.0)

    artifacts = build_track_b_market_data_feed_redundancy_audit(
        config=_config(tmp_path, symbols=("MGC",), timeframes=("1m",))
    )

    assert NO_TRUSTED_FEED in artifacts.feed_comparison["classifications"]
    assert artifacts.feed_comparison["comparisons"][0]["classification"] == NO_TRUSTED_FEED


def test_artifacts_write_expected_paths(tmp_path: Path) -> None:
    _write_databento_bar(tmp_path, "MGC", "1m", NOW - timedelta(seconds=60), close=4511.0)
    artifacts = build_track_b_market_data_feed_redundancy_audit(
        config=_config(tmp_path, symbols=("MGC",), timeframes=("1m",))
    )

    written = write_track_b_market_data_feed_redundancy_artifacts(config=_config(tmp_path), artifacts=artifacts)

    assert written["feed_health"].name == "latest_feed_health.json"
    assert written["feed_comparison"].name == "latest_feed_comparison.json"
    assert json.loads(written["feed_health"].read_text(encoding="utf-8"))["read_only"] is True


def test_no_broker_order_runtime_switching_or_paper_proof_calls() -> None:
    source = inspect.getsource(redundancy)

    assert "reqMktData" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
    assert "submit_order" not in source
    assert "close_position" not in source
    assert "flatten(" not in source
    assert "paper_proof" not in source.lower().replace("paper_proof_invoked", "")
    assert "runtime_feed_switching_allowed\": True" not in source
    assert "strategy_input_switching_allowed\": True" not in source
    assert "live_money_eligible\": True" not in source


def _config(
    root: Path,
    *,
    symbols: tuple[str, ...] = ("MGC", "MNQ"),
    timeframes: tuple[str, ...] = ("1m", "5m"),
    price_tolerance: float = 0.25,
) -> TrackBMarketDataFeedRedundancyConfig:
    return TrackBMarketDataFeedRedundancyConfig(
        repo_root=root,
        symbols=symbols,
        timeframes=timeframes,
        now=NOW,
        price_tolerance=price_tolerance,
    )


def _write_databento_bar(
    root: Path,
    symbol: str,
    timeframe: str,
    timestamp: datetime,
    *,
    close: float,
    source: str = "DATABENTO_REALTIME_PHASE1",
) -> None:
    _write_bar(
        root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / timeframe
        / "latest_runtime_candles.json",
        symbol=symbol,
        timeframe=timeframe,
        timestamp=timestamp,
        close=close,
        source=source,
    )


def _write_ibkr_bar(root: Path, symbol: str, timeframe: str, timestamp: datetime, *, close: float) -> None:
    _write_bar(
        root
        / "outputs"
        / "track_b_execution_core"
        / "ibkr_runtime_market_data"
        / symbol
        / timeframe
        / "latest_runtime_candles.json",
        symbol=symbol,
        timeframe=timeframe,
        timestamp=timestamp,
        close=close,
        source="IBKR_READONLY_RUNTIME_CANDLES",
    )


def _write_bar(
    path: Path,
    *,
    symbol: str,
    timeframe: str,
    timestamp: datetime,
    close: float,
    source: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "test_runtime_candles_v1",
        "generated_at": timestamp.isoformat(),
        "source": source,
        "source_id": source,
        "symbol": symbol,
        "timeframe": timeframe,
        "completed_candles_only": True,
        "realtime_feed_confirmed": True,
        "historical_seed_ready": False,
        "research_artifact_used": source == "RESEARCH_ARCHIVE",
        "archive_artifact_used": False,
        "databento_live_api_replay": False,
        "bars": [
            {
                "bar_end": timestamp.isoformat(),
                "open": close - 0.5,
                "high": close + 0.5,
                "low": close - 1.0,
                "close": close,
                "volume": 10,
                "completed": True,
            }
        ],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _feed(payload: dict, provider: str) -> dict:
    return next(row for row in payload["feeds"] if row["provider"] == provider)
