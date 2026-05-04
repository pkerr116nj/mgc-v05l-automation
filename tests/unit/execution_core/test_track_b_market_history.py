from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import mgc_v05l.execution_core.track_b_market_history as market_history_module
from mgc_v05l.execution_core.track_b_feature_builder import (
    TrackBFeatureBuilderVerdict,
    build_track_b_mgc_feature_event,
)
from mgc_v05l.execution_core.track_b_market_history import (
    TrackBMarketHistoryVerdict,
    collect_track_b_mgc_market_history,
)
from mgc_v05l.execution_core.track_b_market_history_cli import main as market_history_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 15, 45, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def quote_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_databento_current_quote_v1",
        "classification": "CURRENT_QUOTE_AVAILABLE",
        "quote_provider_mode": "REALTIME",
        "market_data_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "timestamp": aware_now().isoformat(),
        "quote_age_seconds": "0",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    path = tmp_path / "current_quote_report.json"
    write_json(path, payload)
    return path


def market_history_payload(tmp_path: Path, *, candle_count: int = 4, realtime: bool = True, **overrides: object) -> dict[str, object]:
    quote_path = quote_report(
        tmp_path,
        quote_provider_mode="REALTIME" if realtime else "HISTORICAL_AVAILABLE_END",
        market_data_mode="REALTIME" if realtime else "HISTORICAL_AVAILABLE_END",
        realtime_quote_received=realtime,
        current_quote_available=realtime,
    )
    base_closes = ["4574.6", "4574.2", "4574.9", "4575.4", "4575.8"]
    candles = [
        {
            "candle_timestamp": f"2026-05-04T15:{41 + index:02d}:00+00:00",
            "open": base_closes[index],
            "high": base_closes[index],
            "low": base_closes[index],
            "close": base_closes[index],
            "volume": "1",
            "provider_symbol": "MGC.v.0",
        }
        for index in range(candle_count)
    ]
    payload: dict[str, object] = {
        "source_id": "unit_test_market_history",
        "batch_id": "market_history_batch_001",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "1m",
        "quote_provider_mode": "REALTIME" if realtime else "HISTORICAL_AVAILABLE_END",
        "realtime_quote_received": realtime,
        "current_quote_available": realtime,
        "metadata": {
            "source_report_path": str(quote_path),
            "quote_provider_mode": "REALTIME" if realtime else "HISTORICAL_AVAILABLE_END",
            "realtime_quote_received": realtime,
            "current_quote_available": realtime,
            "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        },
        "candles": candles,
    }
    payload.update(overrides)
    return payload


def test_market_history_event_with_sufficient_candles_feeds_feature_builder(tmp_path: Path) -> None:
    result = collect_track_b_mgc_market_history(
        market_history_payload=market_history_payload(tmp_path, candle_count=4),
        source_payload_path=tmp_path / "raw_market_history.json",
        expected_account_id="DUM882026",
        output_root=tmp_path / "market_history",
        collector_id="market-history-pass",
        now=aware_now(),
    )

    assert result.verdict == TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT
    assert result.history_event_json is not None
    assert result.history_event_json.exists()
    assert result.history_event is not None
    assert len(result.history_event["candles"]) == 4
    assert result.report["candles_collected"] == 4
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert (tmp_path / "market_history" / "latest_track_b_market_history_event.json").exists()
    assert (tmp_path / "market_history" / "latest_track_b_market_history_report.json").exists()

    feature_result = build_track_b_mgc_feature_event(
        source_event_payload=result.history_event,
        source_event_path=result.history_event_json,
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature_builder",
        builder_id="feature-from-market-history",
        now=aware_now(),
    )

    assert feature_result.verdict == TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT
    assert feature_result.feature_event is not None
    assert feature_result.report["signal_ready"] is True


def test_single_candle_market_history_blocks_without_faking_features(tmp_path: Path) -> None:
    result = collect_track_b_mgc_market_history(
        market_history_payload=market_history_payload(tmp_path, candle_count=1),
        source_payload_path=None,
        expected_account_id="DUM882026",
        output_root=tmp_path / "market_history",
        collector_id="market-history-short",
        min_candles=3,
        now=aware_now(),
    )

    assert result.verdict == TrackBMarketHistoryVerdict.BLOCKED_INSUFFICIENT_HISTORY
    assert result.history_event_json is None
    assert "At least 3 candles" in str(result.report["primary_blocker"])
    assert result.report["submit_attempted"] is False
    assert not (tmp_path / "market_history" / "latest_track_b_market_history_event.json").exists()


def test_stale_or_non_realtime_market_history_blocks_as_diagnostic(tmp_path: Path) -> None:
    result = collect_track_b_mgc_market_history(
        market_history_payload=market_history_payload(tmp_path, realtime=False),
        source_payload_path=None,
        expected_account_id="DUM882026",
        output_root=tmp_path / "market_history",
        collector_id="market-history-historical",
        now=aware_now(),
    )

    assert result.verdict == TrackBMarketHistoryVerdict.BLOCKED_NON_REALTIME_INPUT
    assert result.history_event_json is None
    assert "REALTIME" in str(result.report["primary_blocker"])
    assert result.report["current_quote_available"] is False
    assert result.report["submit_allowed"] is False


def test_market_history_cli_writes_latest_artifacts(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    source_json = tmp_path / "market_history_input.json"
    write_json(source_json, market_history_payload(tmp_path, candle_count=4))

    exit_code = market_history_cli_main(
        [
            "--market-history-json",
            str(source_json),
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_example_gold_shadow_v1",
            "--lane-id",
            "mgc_example_long_lmt_day",
            "--output-root",
            str(tmp_path / "market_history"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["market_history_verdict"] == "TRACK_B_MARKET_HISTORY_WROTE_HISTORY_EVENT"
    assert output["candles_collected"] == 4
    assert Path(output["latest_history_event_path"]).exists()
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False


def test_market_history_collector_does_not_define_broker_or_proof_calls() -> None:
    source = inspect.getsource(market_history_module)
    assert "paper_proof_cli import" not in source
    assert "run_paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "Ibkr" not in source
