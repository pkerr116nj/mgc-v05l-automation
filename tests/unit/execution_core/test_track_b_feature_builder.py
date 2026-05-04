from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import mgc_v05l.execution_core.track_b_feature_builder as feature_builder_module
from mgc_v05l.execution_core.track_b_feature_builder import (
    TrackBFeatureBuilderVerdict,
    build_track_b_mgc_feature_event,
)
from mgc_v05l.execution_core.track_b_feature_builder_cli import main as feature_builder_cli_main
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 15, 30, tzinfo=timezone.utc)


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
        "report_json_path": str(tmp_path / "current_quote_report.json"),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    path = tmp_path / "current_quote_report.json"
    write_json(path, payload)
    return path


def candle_history_payload(tmp_path: Path, **overrides: object) -> dict[str, object]:
    quote_path = quote_report(tmp_path)
    payload: dict[str, object] = {
        "source_id": "unit_test_databento_history",
        "batch_id": "feature_builder_batch_001",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "quote_snapshot",
        "metadata": {
            "source_report_path": str(quote_path),
            "quote_provider_mode": "REALTIME",
            "realtime_quote_received": True,
            "current_quote_available": True,
        },
        "candle_items": [
            {
                "candle_timestamp": "2026-05-04T15:28:00+00:00",
                "open": "4575.0",
                "high": "4575.0",
                "low": "4575.0",
                "close": "4575.0",
                "volume": "1",
            },
            {
                "candle_timestamp": "2026-05-04T15:29:00+00:00",
                "open": "4573.0",
                "high": "4573.0",
                "low": "4573.0",
                "close": "4573.0",
                "volume": "1",
            },
            {
                "candle_timestamp": "2026-05-04T15:30:00+00:00",
                "open": "4575.3",
                "high": "4575.3",
                "low": "4575.3",
                "close": "4575.3",
                "volume": "1",
            },
        ],
    }
    payload.update(overrides)
    return payload


def test_sufficient_fixture_candles_produce_required_feature_event(tmp_path: Path) -> None:
    result = build_track_b_mgc_feature_event(
        source_event_payload=candle_history_payload(tmp_path),
        source_event_path=tmp_path / "databento_candle_history.json",
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature_builder",
        builder_id="feature-builder-pass",
        now=aware_now(),
    )

    assert result.verdict == TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT
    assert result.feature_event_json is not None
    assert result.feature_event_json.exists()
    assert result.feature_event is not None
    features = result.feature_event["metadata"]["ema_momentum_features"]
    assert features["close"] == "4575.3"
    assert features["prior_close"] == "4573"
    assert features["momentum_turning_positive"] is True
    assert features["close_reclaimed_vwap"] is True
    assert features["prior_close_below_vwap"] is True
    assert result.report["signal_ready"] is True
    assert result.report["ema_fields"]["ema"]
    assert result.report["vwap_fields"]["vwap"]
    assert result.report["momentum_reclaim_fields"]["momentum_norm"]
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    latest_event = tmp_path / "feature_builder" / "latest_track_b_feature_event.json"
    latest_report = tmp_path / "feature_builder" / "latest_track_b_feature_builder_report.json"
    assert latest_event.exists()
    assert latest_report.exists()


def test_insufficient_history_blocks_without_feature_event(tmp_path: Path) -> None:
    payload = candle_history_payload(tmp_path)
    payload["candle_items"] = payload["candle_items"][:2]  # type: ignore[index]

    result = build_track_b_mgc_feature_event(
        source_event_payload=payload,
        source_event_path=None,
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature_builder",
        builder_id="feature-builder-short-history",
        now=aware_now(),
    )

    assert result.verdict == TrackBFeatureBuilderVerdict.BLOCKED_INSUFFICIENT_FEATURE_HISTORY
    assert result.feature_event_json is None
    assert result.report["signal_ready"] is False
    assert "At least 3 candles" in str(result.report["primary_blocker"])
    assert not (tmp_path / "feature_builder" / "latest_track_b_feature_event.json").exists()
    assert result.report["submit_attempted"] is False


def test_non_realtime_input_blocks_as_diagnostic(tmp_path: Path) -> None:
    quote_path = quote_report(
        tmp_path / "historical",
        quote_provider_mode="HISTORICAL_AVAILABLE_END",
        market_data_mode="HISTORICAL_AVAILABLE_END",
        realtime_quote_received=False,
        current_quote_available=False,
    )
    payload = candle_history_payload(tmp_path)
    payload["metadata"] = {
        "source_report_path": str(quote_path),
        "quote_provider_mode": "HISTORICAL_AVAILABLE_END",
        "realtime_quote_received": False,
        "current_quote_available": False,
    }

    result = build_track_b_mgc_feature_event(
        source_event_payload=payload,
        source_event_path=None,
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature_builder",
        builder_id="feature-builder-historical",
        now=aware_now(),
    )

    assert result.verdict == TrackBFeatureBuilderVerdict.BLOCKED_NON_REALTIME_INPUT
    assert result.report["signal_ready"] is False
    assert "REALTIME" in str(result.report["primary_blocker"])
    assert result.report["submit_allowed"] is False


def test_generated_feature_event_can_be_consumed_by_mgc_strategy_rule(tmp_path: Path) -> None:
    feature_result = build_track_b_mgc_feature_event(
        source_event_payload=candle_history_payload(tmp_path),
        source_event_path=tmp_path / "databento_candle_history.json",
        expected_account_id="DUM882026",
        output_root=tmp_path / "feature_builder",
        builder_id="feature-builder-rule-input",
        now=aware_now(),
    )
    assert feature_result.feature_event is not None

    rule_result = run_track_b_strategy_rule(
        input_event_payload=feature_result.feature_event,
        input_event_path=feature_result.feature_event_json,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="mgc_ema_momentum_reclaim_long_v1",
        rule_mode="MGC_EMA_MOMENTUM_RECLAIM_LONG",
        emit_signal=True,
        output_root=tmp_path / "rule_runner",
        strategy_adapter_output_root=tmp_path / "adapter",
        candle_producer_output_root=tmp_path / "candle",
        writer_output_root=tmp_path / "writer",
        runner_id="feature-to-rule",
        now=aware_now(),
    )

    assert rule_result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert rule_result.report["decision"] == "LONG"
    assert rule_result.report["signal_emitted"] is True
    assert rule_result.output_batch_json is not None
    assert rule_result.report["paper_proof_cli_called"] is False
    assert rule_result.report["submit_attempted"] is False
    assert rule_result.report["live_money_readiness"] is False


def test_feature_builder_cli_writes_latest_artifacts(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    source_json = tmp_path / "source_event.json"
    write_json(source_json, candle_history_payload(tmp_path))

    exit_code = feature_builder_cli_main(
        [
            "--source-event-json",
            str(source_json),
            "--expected-account-id",
            "DUM882026",
            "--output-root",
            str(tmp_path / "feature_builder"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["feature_builder_verdict"] == "TRACK_B_FEATURE_BUILDER_WROTE_FEATURE_EVENT"
    assert output["signal_ready"] is True
    assert Path(output["latest_feature_event_path"]).exists()
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False


def test_feature_builder_does_not_define_broker_or_proof_calls() -> None:
    source = inspect.getsource(feature_builder_module)
    assert "paper_proof_cli import" not in source
    assert "run_paper_proof" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "ibapi" not in source
