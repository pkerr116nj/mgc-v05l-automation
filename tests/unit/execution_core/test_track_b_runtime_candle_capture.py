from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import mgc_v05l.execution_core.track_b_runtime_candle_capture_cli as runtime_cli
from mgc_v05l.execution_core.track_b_feature_builder import TrackBFeatureBuilderVerdict, build_track_b_mgc_feature_event
from mgc_v05l.execution_core.track_b_runtime_candle_capture import (
    TrackBRuntimeCandleCaptureVerdict,
    capture_track_b_runtime_mgc_1m_candles,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 14, 30, tzinfo=timezone.utc)


def runtime_payload(*, candle_count: int = 5) -> dict[str, object]:
    candles = []
    for index in range(candle_count):
        minute = 26 + index
        candles.append(
            {
                "candle_timestamp": f"2026-05-04T14:{minute:02d}:00+00:00",
                "open": str(4574 + index / 10),
                "high": str(4574.2 + index / 10),
                "low": str(4573.9 + index / 10),
                "close": str(4574.1 + index / 10),
                "volume": "1",
            }
        )
    return {
        "source_id": "runtime_test",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "symbol": "MGCM6",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "timeframe": "1m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "candles": candles,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def test_runtime_capture_writes_bounded_latest_artifacts(tmp_path: Path) -> None:
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=runtime_payload(candle_count=5),
        output_root=tmp_path,
        max_bars=3,
        min_bars=3,
        capture_id="track_b_runtime_candle_capture_test",
        now=aware_now(),
    )

    assert result.verdict == TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES
    assert result.runtime_candles_json is not None
    assert result.report["runtime_candle_context_ready"] is True
    assert result.report["bars_available"] == 3
    assert result.report["first_candle_timestamp"] == "2026-05-04T14:28:00+00:00"
    assert result.report["last_candle_timestamp"] == "2026-05-04T14:30:00+00:00"
    assert (tmp_path / "latest_runtime_mgc_1m_candles.json").exists()
    assert (tmp_path / "latest_runtime_candle_capture_report.json").exists()
    latest = json.loads((tmp_path / "latest_runtime_mgc_1m_candles.json").read_text(encoding="utf-8"))
    assert len(latest["candles"]) == 3
    assert latest["submit_allowed"] is False
    assert latest["submit_attempted"] is False
    assert latest["live_money_readiness"] is False


def test_runtime_capture_insufficient_blocks(tmp_path: Path) -> None:
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=runtime_payload(candle_count=1),
        output_root=tmp_path,
        min_bars=3,
        now=aware_now(),
    )

    assert result.verdict == TrackBRuntimeCandleCaptureVerdict.BLOCKED_INSUFFICIENT_RUNTIME_CANDLES
    assert result.runtime_candles_json is None
    assert result.report["runtime_candle_context_ready"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_runtime_capture_prunes_old_run_folders(tmp_path: Path) -> None:
    for index in range(4):
        capture_track_b_runtime_mgc_1m_candles(
            runtime_candle_payload=runtime_payload(candle_count=3),
            output_root=tmp_path,
            capture_id=f"track_b_runtime_candle_capture_{index}",
            retention_runs=2,
            now=aware_now(),
        )

    run_dirs = [item for item in tmp_path.iterdir() if item.is_dir() and item.name.startswith("track_b_runtime_candle_capture_")]
    assert len(run_dirs) <= 2
    assert (tmp_path / "latest_runtime_mgc_1m_candles.json").exists()
    assert (tmp_path / "latest_runtime_candle_capture_report.json").exists()


def test_runtime_capture_output_feeds_feature_builder(tmp_path: Path) -> None:
    capture = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=runtime_payload(candle_count=4),
        output_root=tmp_path / "capture",
        now=aware_now(),
    )
    assert capture.runtime_candles_event is not None

    feature = build_track_b_mgc_feature_event(
        source_event_payload=capture.runtime_candles_event,
        source_event_path=capture.runtime_candles_json,
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature",
        now=aware_now(),
    )

    assert feature.verdict == TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT
    assert feature.feature_event is not None
    assert feature.report["submit_attempted"] is False
    assert feature.report["live_money_readiness"] is False


def test_runtime_capture_blocks_stale_candles_when_freshness_required(tmp_path: Path) -> None:
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=runtime_payload(candle_count=5),
        output_root=tmp_path,
        max_latest_1m_age_seconds=900,
        max_completed_5m_age_seconds=900,
        now=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
    )

    assert result.verdict == TrackBRuntimeCandleCaptureVerdict.BLOCKED_STALE_RUNTIME_CANDLES
    assert result.runtime_candles_json is None
    assert result.report["latest_1m_candle_timestamp"] == "2026-05-04T14:30:00+00:00"
    assert result.report["latest_completed_5m_candle_timestamp"] == "2026-05-04T14:30:00+00:00"
    assert result.report["runtime_candle_context_stale"] is True
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_runtime_capture_cli_fetches_bounded_databento_history(monkeypatch, tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    quote_path = tmp_path / "quote.json"
    quote_path.write_text(
        json.dumps(
            {
                "quote_provider_mode": "REALTIME",
                "realtime_quote_received": True,
                "current_quote_available": True,
                "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
                "report_json_path": str(quote_path),
            }
        ),
        encoding="utf-8",
    )
    records = runtime_payload(candle_count=5)["candles"]

    def fake_fetch(**kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["symbol"] == "MGC.v.0"
        assert kwargs["schema"] == "ohlcv-1m"
        assert kwargs["max_candles"] == 5
        return records

    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    monkeypatch.setattr(runtime_cli, "fetch_databento_ohlcv_1m_records", fake_fetch)

    exit_code = runtime_cli.main(
        [
            "--fetch-databento-history",
            "--current-quote-report-json",
            str(quote_path),
            "--history-end",
            "2026-05-04T14:31:00+00:00",
            "--lookback-minutes",
            "5",
            "--max-bars",
            "5",
            "--max-latest-1m-age-seconds",
            "999999",
            "--max-completed-5m-age-seconds",
            "999999",
            "--output-root",
            str(tmp_path / "capture"),
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["runtime_candle_capture_verdict"] == TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES
    assert output["candle_source_mode"] == "DATABENTO_HISTORICAL_RECENT"
    assert output["latest_1m_candle_timestamp"] == "2026-05-04T14:30:00+00:00"
    assert output["runtime_candle_context_stale"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
    assert (tmp_path / "capture" / "latest_runtime_mgc_1m_candles.json").exists()


def test_runtime_capture_does_not_reference_broker_or_proof_paths() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_runtime_candle_capture.py").read_text(encoding="utf-8")
    forbidden = ["run_paper_proof", "placeOrder", "cancelOrder", "IbkrReadOnlyTwsTransport"]
    assert not any(token in source for token in forbidden)
