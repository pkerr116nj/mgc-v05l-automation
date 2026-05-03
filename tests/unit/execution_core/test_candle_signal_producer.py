from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.candle_signal_producer import CandleSignalProducerVerdict, produce_candle_signal_batch
from mgc_v05l.execution_core.candle_signal_producer_cli import main as candle_signal_producer_cli_main
from mgc_v05l.execution_core.shadow_listener import ShadowListenerVerdict, run_shadow_listener_cycle


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc)


def candle_event(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "source_id": "unit_test_candle_producer",
        "batch_id": "candle_batch_001",
        "shadow_run_id": "candle_shadow_run_001",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "signal_type": "candle_breakout_review",
        "candle_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "timeframe": "1m",
        "open": "4622.8",
        "high": "4623.4",
        "low": "4622.7",
        "close": "4623.2",
        "volume": 42,
        "signal_direction": "LONG",
        "decision_style": "BINARY",
        "reason": "fixture candle event with explicit long direction",
        "metadata": {"fixture": True},
    }
    payload.update(overrides)
    return payload


def policy() -> dict[str, object]:
    return {
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "4623.1",
        "limit_price_source": "fixture",
        "time_in_force": "DAY",
        "source": "unit_test_candle_producer",
        "min_signal_score": "0.70",
    }


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": "candle_manifest_001",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_lane_registry_candle_test_v1",
        "registry_path": "config/track_b_lane_registry.json",
        "input_source_type": "SHADOW_SIGNAL_FILE",
        "output_root": "outputs/track_b_execution_core/shadow_runs/candle_manifest_001",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def registry() -> dict[str, object]:
    return {
        "registry_version": "track_b_lane_registry_candle_test_v1",
        "generated_at": aware_now().isoformat(),
        "lanes": [
            {
                "strategy_id": "track_b_test_strategy",
                "lane_id": "paper_proof_lane",
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


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def listener_config(root: Path) -> dict[str, object]:
    write_json(root / "policy.json", policy())
    write_json(root / "manifest.json", manifest())
    write_json(root / "registry.json", registry())
    return {
        "listener_id": "candle_signal_producer_listener_test",
        "mode": "SHADOW",
        "inbox_dir": str(root / "inbox"),
        "processing_dir": str(root / "processing"),
        "processed_dir": str(root / "processed"),
        "failed_dir": str(root / "failed"),
        "output_root": str(root / "outputs"),
        "proposal_policy_json": str(root / "policy.json"),
        "manifest_json": str(root / "manifest.json"),
        "registry_json": str(root / "registry.json"),
        "poll_once": True,
        "file_glob": "*.json",
        "submit_enabled": False,
        "live_money_readiness": False,
    }


def test_valid_candle_event_writes_signal_batch_to_listener_inbox(tmp_path: Path) -> None:
    result = produce_candle_signal_batch(
        candle_event_payload=candle_event(),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_candle",
        output_root=tmp_path / "producer_reports",
        writer_output_root=tmp_path / "writer_reports",
        producer_id="producer-valid",
        now=aware_now(),
    )

    assert result.verdict == CandleSignalProducerVerdict.PRODUCED_SIGNAL_BATCH
    assert result.batch_json is not None
    assert result.batch_json.exists()
    batch = json.loads(result.batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["decision_style"] == "BINARY"
    assert signal["signal_direction"] == "LONG"
    assert signal["local_execution_contract_key"] == "MGC-202606"
    assert signal["metadata"]["candle_signal_producer_boundary"] == "candle_signal_producer"
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False
    assert result.report["operator_status_invoked"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    latest_report = tmp_path / "producer_reports" / "latest_candle_signal_producer_report.json"
    assert latest_report.exists()
    latest = json.loads(latest_report.read_text(encoding="utf-8"))
    assert latest["candle_signal_producer_id"] == "producer-valid"
    assert latest["output_batch_path"] == str(result.batch_json)
    assert result.writer_report_json is not None
    assert result.writer_report_json.exists()


def test_missing_direction_becomes_human_review_signal_without_proposal_authority(tmp_path: Path) -> None:
    result = produce_candle_signal_batch(
        candle_event_payload=candle_event(signal_direction="", decision_style="BINARY"),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "producer_reports",
        writer_output_root=tmp_path / "writer_reports",
        producer_id="producer-review-only",
        now=aware_now(),
    )

    assert result.verdict == CandleSignalProducerVerdict.PRODUCED_SIGNAL_BATCH
    assert result.batch_json is not None
    batch = json.loads(result.batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["decision_style"] == "HUMAN_REVIEW"
    assert signal["signal_direction"] == "NONE"
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_invalid_candle_input_fails_safely(tmp_path: Path) -> None:
    result = produce_candle_signal_batch(
        candle_event_payload={"batch_id": "bad_candle_batch", "events": []},
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "producer_reports",
        writer_output_root=tmp_path / "writer_reports",
        producer_id="producer-invalid",
        now=aware_now(),
    )

    assert result.verdict == CandleSignalProducerVerdict.BLOCKED_INVALID_INPUT
    assert result.batch_json is None
    assert not list((tmp_path / "inbox").glob("*.json"))
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert result.report["paper_proof_cli_wired"] is False
    assert result.report["place_order_called"] is False


def test_latest_producer_report_updates_to_newest_run(tmp_path: Path) -> None:
    first = produce_candle_signal_batch(
        candle_event_payload=candle_event(batch_id="first_candle_batch"),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "producer_reports",
        writer_output_root=tmp_path / "writer_reports",
        producer_id="producer-first",
        now=datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc),
    )
    second = produce_candle_signal_batch(
        candle_event_payload=candle_event(batch_id="second_candle_batch"),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "producer_reports",
        writer_output_root=tmp_path / "writer_reports",
        producer_id="producer-second",
        now=datetime(2026, 5, 1, 21, 1, tzinfo=timezone.utc),
    )

    latest = json.loads((tmp_path / "producer_reports" / "latest_candle_signal_producer_report.json").read_text(encoding="utf-8"))
    assert latest["candle_signal_producer_id"] == "producer-second"
    assert latest["batch_id"] == "second_candle_batch"
    assert latest["report_json_path"] == str(second.report_json)
    assert first.report_json.exists()
    assert second.report_json.exists()


def test_listener_can_process_batch_produced_by_candle_producer(tmp_path: Path) -> None:
    config = listener_config(tmp_path)
    producer = produce_candle_signal_batch(
        candle_event_payload=candle_event(),
        inbox_dir=Path(str(config["inbox_dir"])),
        expected_account_id="DUM882026",
        output_root=tmp_path / "producer_reports",
        writer_output_root=tmp_path / "writer_reports",
        producer_id="producer-to-listener",
        now=aware_now(),
    )

    listener = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="candle-producer-listener-cycle",
        now=aware_now(),
    )

    assert producer.batch_json is not None
    assert listener.verdict == ShadowListenerVerdict.COMPLETED
    assert listener.report["files_succeeded"] == 1
    assert listener.report["submit_allowed"] is False
    assert listener.report["submit_attempted"] is False
    assert listener.report["live_money_readiness"] is False
    assert not producer.batch_json.exists()
    assert Path(listener.report["processed_paths"][0]).exists()


def test_candle_signal_producer_cli_writes_batch(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    candle_json = tmp_path / "candle_event.json"
    write_json(candle_json, candle_event())

    exit_code = candle_signal_producer_cli_main(
        [
            "--candle-event-json",
            str(candle_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--expected-account-id",
            "DUM882026",
            "--source-id",
            "cli_candle_demo",
            "--output-root",
            str(tmp_path / "producer_reports"),
            "--writer-output-root",
            str(tmp_path / "writer_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["producer_verdict"] == "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"
    assert output["source_id"] == "cli_candle_demo"
    assert Path(output["output_batch_path"]).exists()
    assert Path(output["writer_report_path"]).exists()
    assert output["listener_invoked"] is False
    assert output["runner_invoked"] is False
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
