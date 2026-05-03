from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.databento_candle_observer import DatabentoCandleObserverVerdict, observe_databento_candle_event
from mgc_v05l.execution_core.databento_candle_observer_cli import main as databento_candle_observer_cli_main
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
    assert result.report["observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["output_candle_event_path"] == str(result.candle_event_json)
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
    assert result.report["primary_blocker"] == "Databento market-data payload did not contain an observed quote/candle."
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
