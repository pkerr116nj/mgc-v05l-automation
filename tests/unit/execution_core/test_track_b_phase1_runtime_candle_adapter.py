from __future__ import annotations

from pathlib import Path

from mgc_v05l.execution_core.track_b_phase1_runtime_candle_adapter import (
    PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY,
    is_legacy_p0_runtime_candle_path,
    normalize_phase1_runtime_candle_payload,
)


def test_phase1_bars_schema_normalizes_to_candles_and_preserves_provenance(tmp_path: Path) -> None:
    source = tmp_path / "outputs/track_b_execution_core/phase1_runtime_market_data/MGC/5m/latest_runtime_candles.json"
    payload = {
        "symbol": "MGC",
        "timeframe": "5m",
        "dataset": "GLBX.MDP3",
        "source_id": "DATABENTO_REALTIME_PHASE1_mgc",
        "schema": "ohlcv-1m",
        "realtime_feed_confirmed": True,
        "realtime_feed_block_reason": "READY",
        "last_completed_bar_ts": "2026-05-24T22:20:00+00:00",
        "bars": [
            {
                "bar_start": "2026-05-24T22:15:00+00:00",
                "bar_end": "2026-05-24T22:20:00+00:00",
                "open": 4556.9,
                "high": 4558.2,
                "low": 4551.0,
                "close": 4556.2,
                "volume": 1919,
                "completed": True,
            }
        ],
    }

    normalized = normalize_phase1_runtime_candle_payload(payload, source_path=source)

    assert normalized["source_category"] == PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY
    assert normalized["input_source_category"] == PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY
    assert normalized["source_authority_path"] == str(source)
    assert normalized["latest_bar_timestamp"] == "2026-05-24T22:20:00+00:00"
    assert normalized["freshness_status"] == "READY"
    assert normalized["realtime_quote_received"] is True
    assert normalized["current_quote_available"] is True
    assert normalized["p0_observe_only_quote_mapping"] is True
    assert normalized["candles"] == normalized["candle_history"]
    assert normalized["candles"][0]["candle_timestamp"] == "2026-05-24T22:20:00+00:00"
    assert normalized["candles"][0]["source_category"] == PHASE1_RUNTIME_MARKET_DATA_SOURCE_CATEGORY


def test_legacy_p0_runtime_candle_path_detects_diagnostic_only_paths() -> None:
    assert is_legacy_p0_runtime_candle_path(
        "outputs/track_b_execution_core/databento_live_runtime_feed/latest_live_mgc_1m_candles.json"
    )
    assert is_legacy_p0_runtime_candle_path(
        "outputs/track_b_execution_core/track_b_runtime_candle_capture/latest_runtime_mgc_1m_candles.json"
    )
    assert not is_legacy_p0_runtime_candle_path(
        "outputs/track_b_execution_core/phase1_runtime_market_data/MGC/1m/latest_runtime_candles.json"
    )
