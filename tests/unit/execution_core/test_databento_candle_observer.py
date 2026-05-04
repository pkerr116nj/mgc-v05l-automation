from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.databento_candle_observer import DatabentoCandleObserverVerdict, observe_databento_candle_event, watch_databento_candle_observer
from mgc_v05l.execution_core.databento_candle_observer_cli import main as databento_candle_observer_cli_main
import mgc_v05l.execution_core.databento_candle_observer_cli as observer_cli_module
from mgc_v05l.execution_core.strategy_signal_adapter import StrategySignalAdapterVerdict, adapt_demo_candle_direction_signal
from mgc_v05l.execution_core.strategy_signal_adapter_cli import main as strategy_signal_adapter_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc)


def quote_report(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": "track_b_databento_current_quote_v1",
        "classification": "CURRENT_QUOTE_AVAILABLE",
        "quote_observed": True,
        "contract_key": "MGC-202606",
        "local_execution_contract_key": "MGC-202606",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "bid": "4623.1",
        "ask": "4623.3",
        "last": "4623.0",
        "timestamp": aware_now().isoformat(),
        "report_json_path": "outputs/track_b_execution_core/quotes/example_quote_report.json",
    }
    payload.update(overrides)
    return payload


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def available_end_lag_error() -> RuntimeError:
    error = RuntimeError("requested quote window is after Databento available_end")
    error.diagnostics = {  # type: ignore[attr-defined]
        "requested_quote_start": "2026-05-03T03:55:00+00:00",
        "requested_quote_end": "2026-05-03T04:00:00+00:00",
        "actual_quote_start": "2026-05-03T03:55:00+00:00",
        "actual_quote_end": "2026-05-03T04:00:00+00:00",
        "provider_available_end": "2026-05-03T03:50:00+00:00",
        "provider_available_end_final": "2026-05-03T03:50:00+00:00",
        "available_end_fallback_used": False,
        "allow_available_end_fallback_requested": False,
        "available_end_retry_reason": "fallback_not_enabled",
        "native_databento_error_code": "data_start_after_available_end",
    }
    return error


class FakeCurrentQuoteTransport:
    def __init__(self, quotes: list[dict[str, object] | Exception] | None = None, *, error: Exception | None = None) -> None:
        self.quotes = quotes or [quote_report()]
        self.error = error
        self.calls = 0

    def get_current_quote(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        self.calls += 1
        if self.error is not None:
            raise self.error
        quote = self.quotes[min(self.calls - 1, len(self.quotes) - 1)]
        if isinstance(quote, Exception):
            raise quote
        return quote


class FakeCurrentQuoteTransportFactory:
    def __init__(self, quote_transport: FakeCurrentQuoteTransport) -> None:
        self.quote_transport = quote_transport

    def __call__(self, **kwargs: object) -> FakeCurrentQuoteTransport:
        del kwargs
        return self.quote_transport


def observe(tmp_path: Path, payload: dict[str, object], **overrides: object):
    kwargs = {
        "market_data_payload": payload,
        "contract_key": "MGC-202606",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "expected_account_id": "DUM882026",
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_review_lane",
        "timeframe": "quote_snapshot",
        "output_root": tmp_path / "observer_reports",
        "source_id": "unit_test_databento_observer",
        "observer_id": "observer-001",
        "now": aware_now(),
    }
    kwargs.update(overrides)
    return observe_databento_candle_event(**kwargs)


def test_valid_databento_quote_writes_candle_event_and_latest_report(tmp_path: Path) -> None:
    result = observe(tmp_path, quote_report(), signal_direction="LONG")

    assert result.verdict == DatabentoCandleObserverVerdict.WROTE_EVENT
    assert result.candle_event_json is not None
    assert result.candle_event_json.exists()
    event = json.loads(result.candle_event_json.read_text(encoding="utf-8"))
    assert event["account_id"] == "DUM882026"
    assert event["contract_key"] == "MGC-202606"
    assert event["strategy_id"] == "track_b_test_strategy"
    assert event["lane_id"] == "paper_review_lane"
    assert event["open"] == "4623.0"
    assert event["high"] == "4623.0"
    assert event["low"] == "4623.0"
    assert event["close"] == "4623.0"
    assert event["signal_direction"] == "LONG"
    assert event["decision_style"] == "BINARY"
    assert event["metadata"]["market_data_provider"] == "DATABENTO"
    assert event["metadata"]["market_data_role"] == "EVIDENCE_ONLY"
    assert event["metadata"]["databento_is_execution_authority"] is False
    assert result.report["observer_mode"] == "one_shot"
    assert result.report["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["output_candle_event_path"] == str(result.candle_event_json)
    assert result.report["source_schema_version"] == "track_b_databento_current_quote_v1"
    assert result.report["source_report_path"] == "outputs/track_b_execution_core/quotes/example_quote_report.json"
    assert result.report["current_quote_report_json"] == "outputs/track_b_execution_core/quotes/example_quote_report.json"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False
    assert result.report["operator_status_invoked"] is False
    assert result.report["databento_connection_attempted"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["paper_proof_cli_wired"] is False
    latest_event = tmp_path / "observer_reports" / "latest_databento_candle_event.json"
    latest_report = tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json"
    assert latest_event.exists()
    assert latest_report.exists()
    assert json.loads(latest_report.read_text(encoding="utf-8"))["databento_candle_observer_id"] == "observer-001"


def test_valid_databento_candle_input_preserves_ohlcv(tmp_path: Path) -> None:
    result = observe(
        tmp_path,
        quote_report(
            open="4622.8",
            high="4623.4",
            low="4622.7",
            close="4623.2",
            volume=42,
            candle_timestamp=aware_now().isoformat(),
            last=None,
        ),
    )

    assert result.verdict == DatabentoCandleObserverVerdict.WROTE_EVENT
    assert result.candle_event is not None
    assert result.candle_event["open"] == "4622.8"
    assert result.candle_event["high"] == "4623.4"
    assert result.candle_event["low"] == "4622.7"
    assert result.candle_event["close"] == "4623.2"
    assert result.candle_event["volume"] == 42
    assert result.report["quote_candle_source_metadata"]["ohlc_source"] == "explicit_ohlc"


def test_no_record_payload_blocks_safely(tmp_path: Path) -> None:
    result = observe(tmp_path, quote_report(quote_observed=False, classification="CURRENT_QUOTE_MARKET_CLOSED_OR_NO_RECORDS"))

    assert result.verdict == DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA
    assert result.candle_event_json is None
    assert result.report["primary_blocker"] == (
        "Databento market-data payload did not contain an observed quote/candle: CURRENT_QUOTE_MARKET_CLOSED_OR_NO_RECORDS"
    )
    assert result.report["output_candle_event_path"] is None
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False


def test_missing_price_fails_without_invoking_downstream_paths(tmp_path: Path) -> None:
    result = observe(tmp_path, quote_report(last=None, close=None))

    assert result.verdict == DatabentoCandleObserverVerdict.BLOCKED_SCHEMA_ERROR
    assert result.candle_event_json is None
    assert "close or last is required" in str(result.report["primary_blocker"])
    assert result.report["strategy_adapter_invoked"] is False
    assert result.report["candle_signal_producer_invoked"] is False
    assert result.report["signal_batch_writer_invoked"] is False
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_observer_event_can_feed_strategy_adapter_explicitly(tmp_path: Path) -> None:
    observed = observe(tmp_path, quote_report(), signal_direction="LONG")

    assert observed.candle_event is not None
    adapter = adapt_demo_candle_direction_signal(
        strategy_event_payload=observed.candle_event,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="databento_observer_adapter_test",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-from-observer",
        now=aware_now(),
    )

    assert adapter.verdict == StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH
    assert adapter.batch_json is not None
    batch = json.loads(adapter.batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["signal_direction"] == "LONG"
    assert signal["local_execution_contract_key"] == "MGC-202606"
    assert signal["metadata"]["input_metadata"]["databento_candle_observer_boundary"] == "databento_candle_observer"
    assert observed.report["strategy_adapter_invoked"] is False
    assert adapter.report["submit_allowed"] is False
    assert adapter.report["live_money_readiness"] is False


def test_observer_event_without_direction_feeds_strategy_adapter_as_human_review(tmp_path: Path) -> None:
    observed = observe(tmp_path, quote_report())

    assert observed.candle_event is not None
    assert "signal_direction" not in observed.candle_event
    adapter = adapt_demo_candle_direction_signal(
        strategy_event_payload=observed.candle_event,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="databento_observer_review_only_test",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-from-observer-review-only",
        now=aware_now(),
    )

    assert adapter.verdict == StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH
    assert adapter.batch_json is not None
    batch = json.loads(adapter.batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["signal_direction"] == "NONE"
    assert signal["decision_style"] == "HUMAN_REVIEW"
    assert "Direction was missing" in signal["reason"]
    assert adapter.report["submit_allowed"] is False
    assert adapter.report["submit_attempted"] is False
    assert adapter.report["live_money_readiness"] is False


def test_databento_cli_latest_event_can_flow_through_strategy_adapter_cli_to_inbox(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    quote_json = tmp_path / "quote_report.json"
    write_json(quote_json, quote_report())

    observer_exit = databento_candle_observer_cli_main(
        [
            "--quote-report-json",
            str(quote_json),
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--timeframe",
            "quote_snapshot",
            "--source-id",
            "cli_databento_observer",
            "--signal-direction",
            "LONG",
            "--output-root",
            str(tmp_path / "observer_reports"),
        ]
    )
    observer_output = json.loads(capsys.readouterr().out)

    adapter_exit = strategy_signal_adapter_cli_main(
        [
            "--strategy-event-json",
            str(tmp_path / "observer_reports" / "latest_databento_candle_event.json"),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--expected-account-id",
            "DUM882026",
            "--source-id",
            "cli_databento_strategy_adapter",
            "--output-root",
            str(tmp_path / "adapter_reports"),
            "--candle-producer-output-root",
            str(tmp_path / "candle_reports"),
            "--writer-output-root",
            str(tmp_path / "writer_reports"),
        ]
    )
    adapter_output = json.loads(capsys.readouterr().out)

    assert observer_exit == 0
    assert adapter_exit == 0
    assert observer_output["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert adapter_output["adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert Path(adapter_output["output_batch_path"]).exists()
    assert list((tmp_path / "inbox").glob("*.json"))
    assert adapter_output["listener_invoked"] is False
    assert adapter_output["runner_invoked"] is False
    assert adapter_output["submit_allowed"] is False
    assert adapter_output["submit_attempted"] is False
    assert adapter_output["live_money_readiness"] is False


def test_databento_candle_observer_cli_writes_event(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    quote_json = tmp_path / "quote_report.json"
    write_json(quote_json, quote_report())

    exit_code = databento_candle_observer_cli_main(
        [
            "--quote-report-json",
            str(quote_json),
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--timeframe",
            "quote_snapshot",
            "--source-id",
            "cli_databento_observer",
            "--output-root",
            str(tmp_path / "observer_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert output["source_id"] == "cli_databento_observer"
    assert Path(output["output_candle_event_path"]).exists()
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
    assert output["listener_invoked"] is False
    assert output["runner_invoked"] is False


def test_databento_candle_observer_cli_live_current_quote_writes_event(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport([quote_report()])
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--allowlisted-local-symbol",
            "MGCM6",
            "--tick-size",
            "0.1",
            "--exchange",
            "COMEX",
            "--currency",
            "USD",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--timeframe",
            "quote_snapshot",
            "--source-id",
            "cli_live_databento_observer",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
            "--max-age-seconds",
            "999999",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    report = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert quote_transport.calls == 1
    assert output["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert Path(output["output_candle_event_path"]).exists()
    assert report["market_data_connection_attempted"] is True
    assert report["databento_connection_attempted"] is True
    assert report["listener_invoked"] is False
    assert report["runner_invoked"] is False
    assert report["operator_status_invoked"] is False
    assert report["submit_allowed"] is False
    assert report["submit_attempted"] is False
    assert report["live_money_readiness"] is False


def test_databento_candle_observer_cli_live_current_quote_provider_error_blocks_safely(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport(error=RuntimeError("Databento provider unavailable"))
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    report = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json").read_text(encoding="utf-8"))

    assert exit_code == 2
    assert output["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA"
    assert "Databento market-data provider error" in report["primary_blocker"]
    assert report["market_data_connection_attempted"] is True
    assert report["submit_allowed"] is False
    assert report["submit_attempted"] is False
    assert report["live_money_readiness"] is False
    assert report["listener_invoked"] is False
    assert report["runner_invoked"] is False


def test_current_quote_after_available_end_blocks_with_window_diagnostics(tmp_path: Path) -> None:
    result = observe(
        tmp_path,
        quote_report(
            classification="CURRENT_QUOTE_PROVIDER_ERROR",
            quote_observed=False,
            current_quote_available=False,
            provider_error="requested quote window is after Databento available_end",
            requested_quote_end="2026-05-03T04:00:00+00:00",
            provider_available_end="2026-05-03T03:50:00+00:00",
            available_end_fallback_used=False,
            allow_available_end_fallback_requested=False,
        ),
    )

    assert result.verdict == DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA
    assert result.candle_event is None
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert "requested_quote_end=2026-05-03T04:00:00+00:00" in result.report["primary_blocker"]
    assert "provider_available_end=2026-05-03T03:50:00+00:00" in result.report["primary_blocker"]
    assert "allow-available-end-fallback" in result.report["required_next_action"]
    assert "do not treat the fallback as readiness" in result.report["required_next_action"]


def test_current_quote_available_end_fallback_remains_no_data_for_current_readiness(tmp_path: Path) -> None:
    result = observe(
        tmp_path,
        quote_report(
            classification="CURRENT_QUOTE_STALE",
            quote_observed=True,
            current_quote_available=False,
            requested_quote_end="2026-05-03T04:00:00+00:00",
            actual_quote_end="2026-05-03T03:45:00+00:00",
            provider_available_end="2026-05-03T03:50:00+00:00",
            available_end_fallback_used=True,
            allow_available_end_fallback_requested=True,
            quote_usable_for_paper_pricing=False,
            quote_usable_for_live_money_readiness=False,
        ),
    )

    assert result.verdict == DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA
    assert result.candle_event is None
    assert result.report["primary_blocker"].startswith("Databento current quote is not currently available")
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False


def test_databento_candle_observer_cli_live_current_quote_missing_api_key_fails_safely(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    report = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json").read_text(encoding="utf-8"))

    assert exit_code == 2
    assert output["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_SCHEMA_ERROR"
    assert "DATABENTO_API_KEY is required" in report["primary_blocker"]
    assert "not-printed" not in json.dumps(report)
    assert report["market_data_connection_attempted"] is False
    assert report["databento_connection_attempted"] is False
    assert report["submit_allowed"] is False
    assert report["submit_attempted"] is False
    assert report["live_money_readiness"] is False


def test_databento_candle_observer_cli_live_current_quote_watch_is_bounded(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport(
        [
            quote_report(last="4623.0", timestamp=aware_now().isoformat()),
            quote_report(last="4624.0", timestamp=datetime(2026, 5, 1, 20, 1, tzinfo=timezone.utc).isoformat()),
        ]
    )
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
            "--max-age-seconds",
            "999999",
            "--watch",
            "--max-cycles",
            "2",
            "--poll-seconds",
            "0",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    latest_event = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_event.json").read_text(encoding="utf-8"))
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert quote_transport.calls == 2
    assert output["observer_mode"] == "watch"
    assert output["current_cycle_number"] == 2
    assert output["processed_cycles"] == 2
    assert output["watch_exited_normally"] is True
    assert latest_event["close"] == "4624.0"
    assert latest_heartbeat["market_data_connection_attempted"] is True
    assert latest_heartbeat["databento_connection_attempted"] is True
    assert latest_heartbeat["submit_allowed"] is False
    assert latest_heartbeat["submit_attempted"] is False
    assert latest_heartbeat["live_money_readiness"] is False


def test_databento_candle_observer_cli_wait_for_current_quote_succeeds_after_available_end_catches_up(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport(
        [
            available_end_lag_error(),
            quote_report(last="4625.0", timestamp=aware_now().isoformat()),
        ]
    )
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--wait-for-current-quote",
            "--max-wait-cycles",
            "3",
            "--wait-poll-seconds",
            "0",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
            "--max-age-seconds",
            "999999",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    latest_event = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_event.json").read_text(encoding="utf-8"))
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert quote_transport.calls == 2
    assert output["observer_mode"] == "wait_for_current_quote"
    assert output["wait_succeeded"] is True
    assert output["successful_current_quote_cycles"] == 1
    assert output["available_end_lag_cycles"] == 1
    assert output["current_quote_available"] is True
    assert latest_event["close"] == "4625.0"
    assert latest_heartbeat["wait_exited_normally"] is True
    assert latest_heartbeat["submit_allowed"] is False
    assert latest_heartbeat["submit_attempted"] is False
    assert latest_heartbeat["live_money_readiness"] is False
    assert latest_heartbeat["listener_invoked"] is False
    assert latest_heartbeat["runner_invoked"] is False


def test_databento_candle_observer_cli_wait_for_current_quote_exhausts_available_end_lag(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport([available_end_lag_error(), available_end_lag_error()])
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--wait-for-current-quote",
            "--max-wait-cycles",
            "2",
            "--wait-poll-seconds",
            "0",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))
    latest_report = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json").read_text(encoding="utf-8"))

    assert exit_code == 2
    assert quote_transport.calls == 2
    assert output["wait_succeeded"] is False
    assert output["available_end_lag_cycles"] == 2
    assert output["no_data_cycles"] == 2
    assert output["last_requested_quote_end"] == "2026-05-03T04:00:00+00:00"
    assert output["last_provider_available_end"] == "2026-05-03T03:50:00+00:00"
    assert latest_heartbeat["wait_exited_normally"] is True
    assert latest_heartbeat["required_next_action"].startswith("Databento available_end is still behind")
    assert latest_report["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA"
    assert latest_report["submit_allowed"] is False
    assert latest_report["submit_attempted"] is False
    assert latest_report["live_money_readiness"] is False


def test_databento_candle_observer_cli_wait_for_current_quote_provider_error_fails_safely(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport(error=RuntimeError("Databento provider unavailable"))
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--wait-for-current-quote",
            "--max-wait-cycles",
            "1",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))

    assert exit_code == 2
    assert output["wait_succeeded"] is False
    assert output["available_end_lag_cycles"] == 0
    assert output["no_data_cycles"] == 1
    assert output["error_cycles"] == 0
    assert quote_transport.calls == 1
    assert latest_heartbeat["wait_exited_normally"] is False
    assert "Databento provider unavailable" in latest_heartbeat["primary_blocker"]
    assert latest_heartbeat["submit_allowed"] is False
    assert latest_heartbeat["submit_attempted"] is False
    assert latest_heartbeat["live_money_readiness"] is False


def test_databento_candle_observer_cli_wait_for_current_quote_fallback_does_not_count_as_ready(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport(
        [
            quote_report(
                last="4625.0",
                timestamp=(aware_now() - timedelta(minutes=5)).isoformat(),
                raw={
                    "requested_quote_end": "2026-05-03T04:00:00+00:00",
                    "actual_quote_end": "2026-05-03T03:45:00+00:00",
                    "provider_available_end": "2026-05-03T03:50:00+00:00",
                    "available_end_fallback_used": True,
                    "allow_available_end_fallback_requested": True,
                    "quote_temporal_scope": "CURRENT_AVAILABLE_END",
                    "active_session_quote": False,
                },
            )
        ]
    )
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--wait-for-current-quote",
            "--max-wait-cycles",
            "1",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
        ]
    )
    output = json.loads(capsys.readouterr().out)
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))

    assert exit_code == 2
    assert output["wait_succeeded"] is False
    assert output["current_quote_available"] is False
    assert output["available_end_lag_cycles"] == 1
    assert latest_heartbeat["last_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA"
    assert latest_heartbeat["submit_allowed"] is False
    assert latest_heartbeat["submit_attempted"] is False
    assert latest_heartbeat["live_money_readiness"] is False


def test_databento_candle_observer_cli_wait_for_current_quote_accepts_explicit_freshness_tolerance(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    quote_transport = FakeCurrentQuoteTransport(
        [
            quote_report(
                last="4625.0",
                timestamp=(aware_now() - timedelta(minutes=5)).isoformat(),
                raw={
                    "requested_quote_end": "2026-05-01T20:00:00+00:00",
                    "actual_quote_end": "2026-05-01T19:56:00+00:00",
                    "provider_available_end": "2026-05-01T19:56:00+00:00",
                    "provider_available_end_final": "2026-05-01T19:56:00+00:00",
                    "available_end_fallback_used": True,
                    "allow_available_end_fallback_requested": True,
                    "quote_temporal_scope": "CURRENT_AVAILABLE_END",
                    "active_session_quote": False,
                },
            )
        ]
    )
    monkeypatch.setenv("DATABENTO_API_KEY", "not-printed")
    monkeypatch.setattr(observer_cli_module, "DatabentoQuoteProviderCurrentQuoteTransport", FakeCurrentQuoteTransportFactory(quote_transport))

    exit_code = databento_candle_observer_cli_main(
        [
            "--live-current-quote",
            "--wait-for-current-quote",
            "--max-wait-cycles",
            "1",
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--current-quote-output-root",
            str(tmp_path / "current_quotes"),
            "--max-current-quote-age-seconds",
            "300",
        ]
    )
    output = json.loads(capsys.readouterr().out)
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))
    latest_report = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert output["wait_succeeded"] is True
    assert output["current_quote_available"] is True
    assert output["quote_age_seconds"] == "240.0"
    assert output["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_ACCEPTED_AVAILABLE_END_WITHIN_TOLERANCE"
    assert latest_heartbeat["max_current_quote_age_seconds"] == 300
    assert latest_report["current_quote_available"] is True
    assert latest_report["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_ACCEPTED_AVAILABLE_END_WITHIN_TOLERANCE"
    assert latest_report["submit_allowed"] is False
    assert latest_report["submit_attempted"] is False
    assert latest_report["live_money_readiness"] is False


def test_watch_mode_runs_bounded_cycles_and_updates_latest_artifacts(tmp_path: Path) -> None:
    payloads = [
        quote_report(last="4623.0", timestamp=aware_now().isoformat()),
        quote_report(last="4624.0", timestamp=datetime(2026, 5, 1, 20, 1, tzinfo=timezone.utc).isoformat()),
    ]
    calls = {"count": 0}

    def reader() -> dict[str, object]:
        index = min(calls["count"], len(payloads) - 1)
        calls["count"] += 1
        return payloads[index]

    result = watch_databento_candle_observer(
        market_data_payload_reader=reader,
        contract_key="MGC-202606",
        databento_continuous_symbol="MGC.v.0",
        dataset="GLBX.MDP3",
        expected_account_id="DUM882026",
        strategy_id="track_b_test_strategy",
        lane_id="paper_review_lane",
        timeframe="quote_snapshot",
        output_root=tmp_path / "observer_reports",
        source_id="watch_test",
        signal_direction="LONG",
        max_cycles=2,
        poll_seconds=0,
        watch_id="watch-001",
        now_func=aware_now,
    )

    assert len(result.cycle_results) == 2
    assert result.heartbeat["observer_mode"] == "watch"
    assert result.heartbeat["current_cycle_number"] == 2
    assert result.heartbeat["processed_cycles"] == 2
    assert result.heartbeat["no_data_cycles"] == 0
    assert result.heartbeat["error_cycles"] == 0
    assert result.heartbeat["last_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.heartbeat["watch_exited_normally"] is True
    assert result.heartbeat["submit_allowed"] is False
    assert result.heartbeat["submit_attempted"] is False
    assert result.heartbeat["live_money_readiness"] is False
    assert result.heartbeat["listener_invoked"] is False
    assert result.heartbeat["runner_invoked"] is False
    latest_event = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_event.json").read_text(encoding="utf-8"))
    latest_report = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_report.json").read_text(encoding="utf-8"))
    latest_heartbeat = json.loads((tmp_path / "observer_reports" / "latest_databento_candle_observer_heartbeat.json").read_text(encoding="utf-8"))
    assert latest_event["close"] == "4624.0"
    assert latest_report["databento_candle_observer_id"] == "watch-001_cycle_2"
    assert latest_heartbeat["current_cycle_number"] == 2


def test_watch_mode_reports_no_data_cycles_without_failing_safety(tmp_path: Path) -> None:
    result = watch_databento_candle_observer(
        market_data_payload_reader=lambda: quote_report(quote_observed=False, classification="CURRENT_QUOTE_MARKET_CLOSED_OR_NO_RECORDS"),
        contract_key="MGC-202606",
        databento_continuous_symbol="MGC.v.0",
        dataset="GLBX.MDP3",
        expected_account_id="DUM882026",
        strategy_id="track_b_test_strategy",
        lane_id="paper_review_lane",
        timeframe="quote_snapshot",
        output_root=tmp_path / "observer_reports",
        source_id="watch_no_data_test",
        max_cycles=2,
        poll_seconds=0,
        watch_id="watch-no-data",
        now_func=aware_now,
    )

    assert result.heartbeat["processed_cycles"] == 0
    assert result.heartbeat["no_data_cycles"] == 2
    assert result.heartbeat["error_cycles"] == 0
    assert result.heartbeat["last_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA"
    assert result.heartbeat["output_event_path"] is None
    assert result.heartbeat["submit_allowed"] is False
    assert result.heartbeat["submit_attempted"] is False
    assert result.heartbeat["live_money_readiness"] is False
    assert result.heartbeat["watch_exited_normally"] is True


def test_watch_mode_catches_invalid_payload_reader_safely(tmp_path: Path) -> None:
    result = watch_databento_candle_observer(
        market_data_payload_reader=lambda: {"quote_observed": True, "timestamp": aware_now().isoformat()},
        contract_key="MGC-202606",
        databento_continuous_symbol="MGC.v.0",
        dataset="GLBX.MDP3",
        expected_account_id="DUM882026",
        strategy_id="track_b_test_strategy",
        lane_id="paper_review_lane",
        timeframe="quote_snapshot",
        output_root=tmp_path / "observer_reports",
        source_id="watch_invalid_test",
        max_cycles=1,
        poll_seconds=0,
        watch_id="watch-invalid",
        now_func=aware_now,
    )

    assert result.heartbeat["processed_cycles"] == 0
    assert result.heartbeat["no_data_cycles"] == 0
    assert result.heartbeat["error_cycles"] == 1
    assert result.heartbeat["last_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_SCHEMA_ERROR"
    assert "close or last is required" in result.cycle_results[0].report["primary_blocker"]
    assert result.heartbeat["strategy_adapter_invoked"] is False
    assert result.heartbeat["listener_invoked"] is False
    assert result.heartbeat["runner_invoked"] is False
    assert result.heartbeat["submit_allowed"] is False
    assert result.heartbeat["live_money_readiness"] is False


def test_databento_candle_observer_cli_watch_mode_writes_heartbeat(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    quote_json = tmp_path / "quote_report.json"
    write_json(quote_json, quote_report())

    exit_code = databento_candle_observer_cli_main(
        [
            "--quote-report-json",
            str(quote_json),
            "--contract-key",
            "MGC-202606",
            "--databento-continuous-symbol",
            "MGC.v.0",
            "--dataset",
            "GLBX.MDP3",
            "--expected-account-id",
            "DUM882026",
            "--strategy-id",
            "track_b_test_strategy",
            "--lane-id",
            "paper_review_lane",
            "--timeframe",
            "quote_snapshot",
            "--source-id",
            "cli_watch_databento_observer",
            "--output-root",
            str(tmp_path / "observer_reports"),
            "--watch",
            "--max-cycles",
            "2",
            "--poll-seconds",
            "0",
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["observer_mode"] == "watch"
    assert output["current_cycle_number"] == 2
    assert output["processed_cycles"] == 2
    assert output["watch_exited_normally"] is True
    assert Path(output["heartbeat_json"]).exists()
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
    assert output["listener_invoked"] is False
    assert output["runner_invoked"] is False
