from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_listener import ShadowListenerVerdict, run_shadow_listener_cycle
from mgc_v05l.execution_core.strategy_signal_adapter import StrategySignalAdapterVerdict, adapt_demo_candle_direction_signal
from mgc_v05l.execution_core.strategy_signal_adapter_cli import main as strategy_signal_adapter_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 21, 15, tzinfo=timezone.utc)


def strategy_event(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "adapter_name": "demo_candle_direction_signal",
        "source_id": "unit_test_strategy_adapter",
        "batch_id": "strategy_adapter_batch_001",
        "shadow_run_id": "strategy_adapter_shadow_run_001",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_test_strategy",
        "signal_family": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "signal_type": "demo_candle_direction_signal",
        "candle_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "timeframe": "1m",
        "open": "4623.0",
        "high": "4623.5",
        "low": "4622.9",
        "close": "4623.4",
        "volume": 57,
        "signal_direction": "LONG",
        "reason": "fixture strategy-like candle direction event",
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
        "source": "unit_test_strategy_adapter",
        "min_signal_score": "0.70",
    }


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": "strategy_adapter_manifest_001",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_lane_registry_strategy_adapter_test_v1",
        "registry_path": "config/track_b_lane_registry.json",
        "input_source_type": "SHADOW_SIGNAL_FILE",
        "output_root": "outputs/track_b_execution_core/shadow_runs/strategy_adapter_manifest_001",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def registry() -> dict[str, object]:
    return {
        "registry_version": "track_b_lane_registry_strategy_adapter_test_v1",
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
        "listener_id": "strategy_signal_adapter_listener_test",
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


def test_valid_explicit_long_event_emits_no_submit_signal_batch(tmp_path: Path) -> None:
    result = adapt_demo_candle_direction_signal(
        strategy_event_payload=strategy_event(),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_adapter",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-valid",
        now=aware_now(),
    )

    assert result.verdict == StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH
    assert result.batch_json is not None
    assert result.batch_json.exists()
    batch = json.loads(result.batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["decision_style"] == "BINARY"
    assert signal["signal_direction"] == "LONG"
    assert signal["metadata"]["input_metadata"]["strategy_signal_adapter_boundary"] == "demo_candle_direction_signal"
    assert result.report["strategy_id"] == "track_b_test_strategy"
    assert result.report["signal_count"] == 1
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False
    assert result.report["operator_status_invoked"] is False
    assert result.report["lane_registry_invoked"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.candle_producer_report_json is not None
    assert result.candle_producer_report_json.exists()
    assert result.writer_report_json is not None
    assert result.writer_report_json.exists()


def test_missing_side_emits_human_review_only(tmp_path: Path) -> None:
    result = adapt_demo_candle_direction_signal(
        strategy_event_payload=strategy_event(signal_direction=""),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-human-review",
        now=aware_now(),
    )

    assert result.verdict == StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH
    assert result.batch_json is not None
    batch = json.loads(result.batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["decision_style"] == "HUMAN_REVIEW"
    assert signal["signal_direction"] == "NONE"
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_invalid_event_fails_safely(tmp_path: Path) -> None:
    result = adapt_demo_candle_direction_signal(
        strategy_event_payload={"batch_id": "bad_strategy_event", "account_id": "DUM882026"},
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-invalid",
        now=aware_now(),
    )

    assert result.verdict == StrategySignalAdapterVerdict.BLOCKED_SCHEMA_ERROR
    assert result.batch_json is None
    assert not list((tmp_path / "inbox").glob("*.json"))
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert result.report["paper_proof_cli_wired"] is False
    assert result.report["place_order_called"] is False


def test_latest_adapter_report_updates_to_newest_run(tmp_path: Path) -> None:
    first = adapt_demo_candle_direction_signal(
        strategy_event_payload=strategy_event(batch_id="first_adapter_batch"),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-first",
        now=datetime(2026, 5, 1, 21, 15, tzinfo=timezone.utc),
    )
    second = adapt_demo_candle_direction_signal(
        strategy_event_payload=strategy_event(batch_id="second_adapter_batch"),
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-second",
        now=datetime(2026, 5, 1, 21, 16, tzinfo=timezone.utc),
    )

    latest = json.loads((tmp_path / "adapter_reports" / "latest_strategy_signal_adapter_report.json").read_text(encoding="utf-8"))
    assert latest["strategy_signal_adapter_id"] == "adapter-second"
    assert latest["batch_id"] == "second_adapter_batch"
    assert latest["report_json_path"] == str(second.report_json)
    assert first.report_json.exists()
    assert second.report_json.exists()


def test_listener_can_process_batch_produced_by_adapter(tmp_path: Path) -> None:
    config = listener_config(tmp_path)
    adapter = adapt_demo_candle_direction_signal(
        strategy_event_payload=strategy_event(),
        inbox_dir=Path(str(config["inbox_dir"])),
        expected_account_id="DUM882026",
        output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        adapter_id="adapter-to-listener",
        now=aware_now(),
    )

    listener = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="strategy-adapter-listener-cycle",
        now=aware_now(),
    )

    assert adapter.batch_json is not None
    assert listener.verdict == ShadowListenerVerdict.COMPLETED
    assert listener.report["files_succeeded"] == 1
    assert listener.report["submit_allowed"] is False
    assert listener.report["submit_attempted"] is False
    assert listener.report["live_money_readiness"] is False
    assert not adapter.batch_json.exists()
    assert Path(listener.report["processed_paths"][0]).exists()


def test_strategy_signal_adapter_cli_writes_batch(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    event_json = tmp_path / "strategy_event.json"
    write_json(event_json, strategy_event())

    exit_code = strategy_signal_adapter_cli_main(
        [
            "--strategy-event-json",
            str(event_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--expected-account-id",
            "DUM882026",
            "--source-id",
            "cli_strategy_demo",
            "--output-root",
            str(tmp_path / "adapter_reports"),
            "--candle-producer-output-root",
            str(tmp_path / "candle_reports"),
            "--writer-output-root",
            str(tmp_path / "writer_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert output["source_id"] == "cli_strategy_demo"
    assert Path(output["output_batch_path"]).exists()
    assert Path(output["candle_producer_report_path"]).exists()
    assert Path(output["downstream_writer_report_path"]).exists()
    assert output["listener_invoked"] is False
    assert output["runner_invoked"] is False
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
