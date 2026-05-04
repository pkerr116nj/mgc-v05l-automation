from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import mgc_v05l.execution_core.track_b_mgc_candle_history_producer as producer_module
from mgc_v05l.execution_core.track_b_market_history import (
    TrackBMarketHistoryVerdict,
    collect_track_b_mgc_market_history,
)
from mgc_v05l.execution_core.track_b_mgc_candle_history_producer import (
    TrackBMgcCandleHistoryProducerVerdict,
    fetch_databento_ohlcv_1m_records,
    produce_track_b_mgc_candle_history_input,
)
from mgc_v05l.execution_core.track_b_mgc_candle_history_producer_cli import main as producer_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 16, 5, tzinfo=timezone.utc)


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def current_quote_report(**overrides: object) -> dict[str, object]:
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
        "report_json_path": "current_quote_report.json",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def candle_records(count: int = 4) -> list[dict[str, object]]:
    closes = ["4574.6", "4574.2", "4574.9", "4575.4", "4575.8"]
    return [
        {
            "ts_event": f"2026-05-04T16:{index:02d}:00+00:00",
            "open": closes[index],
            "high": closes[index],
            "low": closes[index],
            "close": closes[index],
            "volume": 1,
            "symbol": "MGC.v.0",
        }
        for index in range(count)
    ]


def history_payload(count: int = 4, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "source_id": "unit_test_history_producer",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "dataset": "GLBX.MDP3",
        "databento_continuous_symbol": "MGC.v.0",
        "timeframe": "1m",
        "candles": candle_records(count),
    }
    payload.update(overrides)
    return payload


class FakeHistoryTransport:
    def __init__(self, records: Sequence[Mapping[str, Any]]) -> None:
        self.records = tuple(records)
        self.requests: list[dict[str, Any]] = []

    def request_records(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]:
        self.requests.append(dict(kwargs))
        return self.records


def test_sufficient_generated_history_feeds_market_history_collector(tmp_path: Path) -> None:
    quote_path = tmp_path / "current_quote_report.json"
    quote = current_quote_report(report_json_path=str(quote_path))
    write_json(quote_path, quote)
    result = produce_track_b_mgc_candle_history_input(
        history_payload=history_payload(4),
        current_quote_report_payload=quote,
        history_payload_path=tmp_path / "raw_history.json",
        current_quote_report_path=quote_path,
        expected_account_id="DUM882026",
        strategy_id="track_b_example_gold_shadow_v1",
        lane_id="mgc_example_long_lmt_day",
        output_root=tmp_path / "producer",
        producer_id="producer-pass",
        now=aware_now(),
    )

    assert result.verdict == TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT
    assert result.history_input_json is not None
    assert result.history_input is not None
    assert len(result.history_input["candles"]) == 4
    assert result.history_input["history_provider_mode"] == "DATABENTO_HISTORICAL_BOUNDED_WITH_REALTIME_CURRENT"
    assert result.history_input["quote_provider_mode"] == "REALTIME"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert (tmp_path / "producer" / "latest_track_b_mgc_candle_history_input.json").exists()

    market_result = collect_track_b_mgc_market_history(
        market_history_payload=result.history_input,
        source_payload_path=result.history_input_json,
        expected_account_id="DUM882026",
        output_root=tmp_path / "market_history",
        collector_id="collector-from-producer",
        now=aware_now(),
    )
    assert market_result.verdict == TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT
    assert market_result.history_event is not None
    assert len(market_result.history_event["candles"]) == 4


def test_insufficient_candles_block_without_faking_history(tmp_path: Path) -> None:
    result = produce_track_b_mgc_candle_history_input(
        history_payload=history_payload(1),
        current_quote_report_payload=current_quote_report(),
        history_payload_path=None,
        current_quote_report_path=None,
        expected_account_id="DUM882026",
        strategy_id="track_b_example_gold_shadow_v1",
        lane_id="mgc_example_long_lmt_day",
        output_root=tmp_path / "producer",
        producer_id="producer-short",
        min_candles=3,
        now=aware_now(),
    )

    assert result.verdict == TrackBMgcCandleHistoryProducerVerdict.BLOCKED_INSUFFICIENT_CANDLES
    assert result.history_input_json is None
    assert "At least 3 candles" in str(result.report["primary_blocker"])
    assert result.report["submit_attempted"] is False


def test_stale_non_realtime_current_quote_blocks_as_diagnostic(tmp_path: Path) -> None:
    result = produce_track_b_mgc_candle_history_input(
        history_payload=history_payload(4),
        current_quote_report_payload=current_quote_report(
            quote_provider_mode="HISTORICAL_AVAILABLE_END",
            realtime_quote_received=False,
            current_quote_available=False,
        ),
        history_payload_path=None,
        current_quote_report_path=None,
        expected_account_id="DUM882026",
        strategy_id="track_b_example_gold_shadow_v1",
        lane_id="mgc_example_long_lmt_day",
        output_root=tmp_path / "producer",
        producer_id="producer-stale-current",
        now=aware_now(),
    )

    assert result.verdict == TrackBMgcCandleHistoryProducerVerdict.BLOCKED_NON_REALTIME_CURRENT_EVIDENCE
    assert result.history_input_json is None
    assert "REALTIME" in str(result.report["primary_blocker"])
    assert result.report["current_quote_available"] is False
    assert result.report["submit_allowed"] is False


def test_history_payload_marked_stale_blocks_before_collector(tmp_path: Path) -> None:
    result = produce_track_b_mgc_candle_history_input(
        history_payload=history_payload(4, provider_mode="HISTORICAL_AVAILABLE_END"),
        current_quote_report_payload=current_quote_report(),
        history_payload_path=None,
        current_quote_report_path=None,
        expected_account_id="DUM882026",
        strategy_id="track_b_example_gold_shadow_v1",
        lane_id="mgc_example_long_lmt_day",
        output_root=tmp_path / "producer",
        producer_id="producer-stale-history",
        now=aware_now(),
    )

    assert result.verdict == TrackBMgcCandleHistoryProducerVerdict.BLOCKED_INVALID_INPUT
    assert "diagnostic-only" in str(result.report["primary_blocker"])
    assert result.report["submit_attempted"] is False


def test_fetch_databento_ohlcv_1m_records_uses_bounded_transport() -> None:
    transport = FakeHistoryTransport(candle_records(4))
    records = fetch_databento_ohlcv_1m_records(
        transport=transport,
        api_key="test-key",
        dataset="GLBX.MDP3",
        symbol="MGC.v.0",
        stype_in="continuous",
        schema="ohlcv-1m",
        start=datetime(2026, 5, 4, 16, 0, tzinfo=timezone.utc),
        end=datetime(2026, 5, 4, 16, 5, tzinfo=timezone.utc),
        max_candles=4,
    )

    assert len(records) == 4
    assert transport.requests[0]["schema"] == "ohlcv-1m"
    assert transport.requests[0]["limit"] == 4
    assert transport.requests[0]["symbol"] == "MGC.v.0"


def test_cli_fixture_history_writes_latest_artifacts(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    quote_path = tmp_path / "current_quote_report.json"
    history_path = tmp_path / "history.json"
    write_json(quote_path, current_quote_report(report_json_path=str(quote_path)))
    write_json(history_path, history_payload(4))

    exit_code = producer_cli_main(
        [
            "--history-json",
            str(history_path),
            "--current-quote-report-json",
            str(quote_path),
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_example_gold_shadow_v1",
            "--lane-id",
            "mgc_example_long_lmt_day",
            "--output-root",
            str(tmp_path / "producer"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["candle_history_producer_verdict"] == "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_WROTE_HISTORY_INPUT"
    assert output["candles_produced"] == 4
    assert Path(output["latest_history_input_path"]).exists()
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False


def test_candle_history_producer_does_not_define_broker_or_proof_calls() -> None:
    source = inspect.getsource(producer_module)
    assert "paper_proof_cli import" not in source
    assert "run_paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "Ibkr" not in source
