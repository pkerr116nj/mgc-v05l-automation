from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.phase1_runtime_data_readiness import (
    Phase1RuntimeDataReadinessConfig,
    _artifact_check,
    build_phase1_runtime_data_readiness,
    write_phase1_runtime_data_readiness_artifacts,
)
from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TICKER_ORDER


NOW = datetime(2026, 5, 9, 14, 0, tzinfo=timezone.utc)


def _config(root: Path, **overrides: object) -> Phase1RuntimeDataReadinessConfig:
    values = {"repo_root": root, "now": NOW}
    values.update(overrides)
    return Phase1RuntimeDataReadinessConfig(**values)


def _write_artifact(
    root: Path,
    *,
    symbol: str,
    timeframe: str,
    kind: str = "candles",
    generated_at: datetime | None = NOW,
    source_id: str | None = "databento_live:test",
    completed_candles_only: bool | None = True,
    payload_symbol: str | None = None,
    payload_timeframe: str | None = None,
    historical_seed_ready: bool = False,
    realtime_feed_confirmed: bool = True,
) -> Path:
    base = "phase1_runtime_market_data" if kind == "candles" else "phase1_runtime_features"
    filename = "latest_runtime_candles.json" if kind == "candles" else "latest_runtime_features.json"
    path = root / "outputs" / "track_b_execution_core" / base / symbol / timeframe / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": generated_at.isoformat() if generated_at else None,
        "source_id": source_id,
        "symbol": payload_symbol or symbol,
        "timeframe": payload_timeframe or timeframe,
        "completed_candles_only": completed_candles_only,
        "historical_seed_ready": historical_seed_ready,
        "realtime_feed_confirmed": realtime_feed_confirmed,
        "bars" if kind == "candles" else "features": [{"bar_end": NOW.isoformat(), "close": 100.0}],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_valid_fixture_candles_and_features_are_ready(tmp_path: Path) -> None:
    for timeframe in ("1m", "3m", "5m"):
        _write_artifact(tmp_path, symbol="GC", timeframe=timeframe, kind="candles")
        _write_artifact(tmp_path, symbol="GC", timeframe=timeframe, kind="features")

    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path))
    gc = next(row for row in artifacts.rows if row["symbol"] == "GC")

    assert gc["runtime_candles_ready"] is True
    assert gc["derived_features_ready"] is True
    assert gc["runtime_candles_block_reason"] == "READY"
    assert gc["derived_features_block_reason"] == "READY"


def test_stale_fixture_candles_fail_closed(tmp_path: Path) -> None:
    open_session_now = datetime(2026, 5, 11, 14, 0, tzinfo=timezone.utc)
    _write_artifact(
        tmp_path,
        symbol="GC",
        timeframe="1m",
        generated_at=open_session_now - timedelta(minutes=10),
    )

    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path, now=open_session_now))
    gc = next(row for row in artifacts.rows if row["symbol"] == "GC")

    assert gc["runtime_candles_ready"] is False
    assert gc["candle_checks"]["1m"]["reason"] == "RUNTIME_CANDLES_STALE"


def test_weekend_halt_stale_candles_are_classified_market_closed(tmp_path: Path) -> None:
    friday_close = datetime(2026, 5, 22, 21, 0, tzinfo=timezone.utc)
    saturday = datetime(2026, 5, 23, 7, 15, tzinfo=timezone.utc)
    _write_artifact(
        tmp_path,
        symbol="GC",
        timeframe="1m",
        generated_at=friday_close,
    )

    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path, now=saturday))
    gc = next(row for row in artifacts.rows if row["symbol"] == "GC")

    assert gc["runtime_candles_ready"] is False
    assert gc["candle_checks"]["1m"]["reason"] == "MARKET_CLOSED_NO_FRESH_BARS"
    assert gc["candle_checks"]["1m"]["market_session"]["market_closed"] is True
    assert artifacts.report["market_session"]["classification"] == "MARKET_CLOSED_NO_FRESH_BARS"


def test_crypto_weekend_stale_candles_are_not_classified_market_closed(tmp_path: Path) -> None:
    friday_close = datetime(2026, 5, 22, 21, 0, tzinfo=timezone.utc)
    saturday = datetime(2026, 5, 23, 7, 15, tzinfo=timezone.utc)
    path = _write_artifact(
        tmp_path,
        symbol="MET",
        timeframe="1m",
        generated_at=friday_close,
    )

    check = _artifact_check(
        path=path,
        symbol="MET",
        timeframe="1m",
        now=saturday,
        kind="candles",
    )

    assert check["ready"] is False
    assert check["reason"] == "RUNTIME_CANDLES_STALE"
    assert check["market_session"] == {}


def test_historical_seed_is_visible_but_does_not_confirm_realtime_readiness(tmp_path: Path) -> None:
    for timeframe in ("1m", "3m", "5m"):
        _write_artifact(
            tmp_path,
            symbol="GC",
            timeframe=timeframe,
            historical_seed_ready=True,
            realtime_feed_confirmed=False,
        )

    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path))
    gc = next(row for row in artifacts.rows if row["symbol"] == "GC")

    assert gc["historical_seed_ready"] is True
    assert gc["realtime_feed_confirmed"] is False
    assert gc["runtime_candles_ready"] is False
    assert gc["runtime_candles_block_reason"] == "REALTIME_FEED_NOT_CONFIRMED"
    assert all(check["historical_seed_ready"] is True for check in gc["candle_checks"].values())
    assert all(check["realtime_feed_confirmed"] is False for check in gc["candle_checks"].values())


def test_wrong_symbol_and_timeframe_fail_closed(tmp_path: Path) -> None:
    _write_artifact(tmp_path, symbol="GC", timeframe="1m", payload_symbol="NQ")
    _write_artifact(tmp_path, symbol="NQ", timeframe="1m", payload_timeframe="5m")

    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path))
    rows = {row["symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["candle_checks"]["1m"]["reason"] == "RUNTIME_SYMBOL_MISMATCH"
    assert rows["NQ"]["candle_checks"]["1m"]["reason"] == "RUNTIME_TIMEFRAME_MISMATCH"


def test_missing_generated_at_source_id_or_completed_semantics_fail_closed(tmp_path: Path) -> None:
    _write_artifact(tmp_path, symbol="GC", timeframe="1m", generated_at=None)
    _write_artifact(tmp_path, symbol="NQ", timeframe="1m", source_id=None)
    _write_artifact(tmp_path, symbol="ES", timeframe="1m", completed_candles_only=None)

    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path))
    rows = {row["symbol"]: row for row in artifacts.rows}

    assert rows["GC"]["candle_checks"]["1m"]["reason"] == "RUNTIME_GENERATED_AT_MISSING"
    assert rows["NQ"]["candle_checks"]["1m"]["reason"] == "RUNTIME_SOURCE_ID_MISSING"
    assert rows["ES"]["candle_checks"]["1m"]["reason"] == "COMPLETED_CANDLE_SEMANTICS_MISSING"


def test_research_artifact_path_is_rejected_as_runtime_truth(tmp_path: Path) -> None:
    research_root = Path("outputs") / "track_b_research" / "snapshots"
    config = _config(tmp_path, runtime_candle_root=research_root)

    artifacts = build_phase1_runtime_data_readiness(config=config)
    gc = next(row for row in artifacts.rows if row["symbol"] == "GC")

    assert gc["candle_checks"]["1m"]["reason"] == "RESEARCH_ONLY_UNSAFE"


def test_all_phase1_tickers_appear_and_missing_features_are_explicit(tmp_path: Path) -> None:
    artifacts = build_phase1_runtime_data_readiness(config=_config(tmp_path))

    assert [row["symbol"] for row in artifacts.rows] == list(PHASE1_RUNTIME_TICKER_ORDER)
    assert all(row["runtime_candles_ready"] is False for row in artifacts.rows)
    assert all(row["runtime_candles_block_reason"] == "RUNTIME_CANDLES_MISSING" for row in artifacts.rows)
    assert all(row["derived_features_ready"] is False for row in artifacts.rows)
    assert all(row["derived_features_block_reason"] == "FEATURES_NOT_IMPLEMENTED" for row in artifacts.rows)


def test_writes_latest_runtime_data_readiness_artifact(tmp_path: Path) -> None:
    config = _config(tmp_path, output_dir=Path("outputs") / "reports" / "phase1_runtime_data_readiness")
    artifacts = build_phase1_runtime_data_readiness(config=config)

    write_phase1_runtime_data_readiness_artifacts(config=config, artifacts=artifacts)

    path = tmp_path / "outputs" / "reports" / "phase1_runtime_data_readiness" / "latest_phase1_runtime_data_readiness.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["row_count"] == len(PHASE1_RUNTIME_TICKER_ORDER)
    assert payload["archive_artifact_used"] is False
    assert payload["research_artifact_used"] is False
