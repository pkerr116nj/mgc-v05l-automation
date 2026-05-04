from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_observation_runner import (
    TrackBObservationRunnerVerdict,
    run_track_b_observation_once,
    run_track_b_observation_watch,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 3, 22, 0, tzinfo=timezone.utc)


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
        "last": "4623.2",
        "timestamp": aware_now().isoformat(),
        "report_json_path": "outputs/track_b_execution_core/current_quotes/example/current_quote_report.json",
    }
    payload.update(overrides)
    return payload


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def policy() -> dict[str, object]:
    return {
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "4623.1",
        "limit_price_source": "unit_test",
        "time_in_force": "DAY",
        "source": "unit_test_track_b_observation_runner",
    }


def manifest() -> dict[str, object]:
    return {
        "run_id": "track_b_observation_runner_manifest",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_observation_runner_registry_v1",
        "input_source_type": "SHADOW_SIGNAL_FILE",
        "output_root": "outputs/track_b_execution_core/shadow_runs/track_b_observation_runner_manifest",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }


def registry() -> dict[str, object]:
    return {
        "registry_version": "track_b_observation_runner_registry_v1",
        "generated_at": aware_now().isoformat(),
        "lanes": [
            {
                "strategy_id": "track_b_example_gold_shadow_v1",
                "lane_id": "mgc_example_long_lmt_day",
                "lane_status": "PAPER_REVIEW",
                "mode_allowed": "PAPER",
                "expected_account_id": "DUM882026",
                "allowed_local_execution_contract_keys": ["MGC-202606"],
                "allowed_instrument_family": "MGC",
                "allowed_sides": ["BUY", "SELL"],
                "allowed_order_types": ["LMT"],
                "allowed_time_in_force": ["DAY"],
                "max_quantity": 1,
            }
        ],
    }


def listener_config(root: Path) -> dict[str, object]:
    write_json(root / "policy.json", policy())
    write_json(root / "manifest.json", manifest())
    write_json(root / "registry.json", registry())
    return {
        "listener_id": "track_b_observation_runner_listener",
        "mode": "SHADOW",
        "inbox_dir": str(root / "inbox"),
        "processing_dir": str(root / "processing"),
        "processed_dir": str(root / "processed"),
        "failed_dir": str(root / "failed"),
        "output_root": str(root / "listener_outputs"),
        "proposal_policy_json": str(root / "policy.json"),
        "manifest_json": str(root / "manifest.json"),
        "registry_json": str(root / "registry.json"),
        "poll_once": True,
        "file_glob": "*.json",
        "submit_enabled": False,
        "live_money_readiness": False,
    }


def runner_kwargs(tmp_path: Path, **overrides: object) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "listener_config_payload": listener_config(tmp_path),
        "contract_key": "MGC-202606",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "expected_account_id": "DUM882026",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "quote_snapshot",
        "source_id": "unit_test_observation_runner",
        "output_root": tmp_path / "runner_reports",
        "databento_observer_output_root": tmp_path / "databento_observer",
        "strategy_adapter_output_root": tmp_path / "strategy_adapter",
        "candle_producer_output_root": tmp_path / "candle_producer",
        "writer_output_root": tmp_path / "writer",
        "listener_output_root": tmp_path / "listener_outputs",
        "operator_status_output_root": tmp_path / "operator_status",
        "now": aware_now(),
    }
    kwargs.update(overrides)
    return kwargs


def test_one_shot_fixture_observation_flow_with_explicit_direction(tmp_path: Path) -> None:
    result = run_track_b_observation_once(
        market_data_payload=quote_report(),
        signal_direction="LONG",
        runner_id="runner-one-shot-long",
        **runner_kwargs(tmp_path),
    )

    assert result.verdict == TrackBObservationRunnerVerdict.COMPLETED_FOR_REVIEW
    assert result.report["runner_verdict"] == "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW"
    assert result.report["databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["strategy_adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert result.report["candle_producer_verdict"] == "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"
    assert result.report["signal_batch_writer_verdict"] == "SIGNAL_BATCH_WRITER_WROTE_BATCH"
    assert result.report["listener_verdict"] == "SHADOW_LISTENER_CYCLE_COMPLETED"
    assert result.report["listener_health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert result.report["operator_status_verdict"] == "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    assert Path(str(result.report["latest_operator_status_path"])).exists()
    assert (tmp_path / "runner_reports" / "latest_track_b_observation_runner_report.json").exists()
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["place_order_called"] is False
    assert result.report["cancel_called"] is False


def test_no_direction_produces_human_review_and_no_unsafe_submit(tmp_path: Path) -> None:
    result = run_track_b_observation_once(
        market_data_payload=quote_report(),
        runner_id="runner-human-review",
        **runner_kwargs(tmp_path),
    )
    adapter_batch = json.loads(Path(str(result.report["strategy_adapter_report_path"])).read_text(encoding="utf-8"))
    listener_report = json.loads(Path(str(result.report["listener_cycle_summary_path"])).read_text(encoding="utf-8"))
    final_batch_path = Path((listener_report["processed_paths"] or listener_report["failed_paths"])[0])
    batch = json.loads(final_batch_path.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]

    assert signal["decision_style"] == "HUMAN_REVIEW"
    assert signal["signal_direction"] == "NONE"
    assert adapter_batch["submit_allowed"] is False
    assert result.verdict == TrackBObservationRunnerVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_bounded_watch_exits_normally_and_updates_latest_report(tmp_path: Path) -> None:
    payloads = [
        quote_report(last="4623.2", timestamp=aware_now().isoformat()),
        quote_report(last="4624.0", timestamp=datetime(2026, 5, 3, 22, 1, tzinfo=timezone.utc).isoformat()),
    ]
    calls = {"count": 0}

    def reader() -> dict[str, object]:
        index = min(calls["count"], len(payloads) - 1)
        calls["count"] += 1
        return payloads[index]

    result = run_track_b_observation_watch(
        market_data_payload_reader=reader,
        max_cycles=2,
        poll_seconds=0,
        runner_id="runner-watch",
        now_func=aware_now,
        sleep_func=lambda seconds: None,
        signal_direction="LONG",
        **{key: value for key, value in runner_kwargs(tmp_path).items() if key != "now"},
    )

    assert len(result.cycle_results) == 2
    assert calls["count"] == 2
    assert result.report["mode"] == "watch"
    assert result.report["current_cycle"] == 2
    assert result.report["watch_exited_normally"] is True
    assert len(result.report["cycle_report_paths"]) == 2
    assert Path(str(result.report["latest_operator_status_path"])).exists()
    assert (tmp_path / "runner_reports" / "latest_track_b_observation_runner_report.json").exists()
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_databento_observer_failure_stops_before_adapter_listener(tmp_path: Path) -> None:
    result = run_track_b_observation_once(
        market_data_payload=quote_report(quote_observed=False, classification="CURRENT_QUOTE_MARKET_CLOSED_OR_NO_RECORDS"),
        signal_direction="LONG",
        runner_id="runner-db-blocked",
        **runner_kwargs(tmp_path),
    )

    assert result.verdict == TrackBObservationRunnerVerdict.BLOCKED_DATABENTO_OBSERVER
    assert result.report["databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA"
    assert result.report["strategy_adapter_verdict"] is None
    assert result.report["listener_verdict"] is None
    assert result.report["operator_status_verdict"] == "OPERATOR_STATUS_UNKNOWN"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["paper_proof_cli_called"] is False
