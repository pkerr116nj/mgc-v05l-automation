from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_listener import ShadowListenerVerdict, run_shadow_listener_cycle
from mgc_v05l.execution_core.signal_batch_writer import SignalBatchWriterVerdict, write_signal_batch_to_inbox
from mgc_v05l.execution_core.signal_batch_writer_cli import main as signal_batch_writer_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 30, tzinfo=timezone.utc)


def signal(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_id": "writer-signal-001",
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "signal_type": "breakout_retest",
        "signal_direction": "LONG",
        "decision_style": "BINARY",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "instrument_family": "MGC",
        "local_execution_contract_key": "MGC-202606",
        "signal_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "source": "unit_test_signal_batch_writer",
        "reason": "synthetic writer signal",
        "submit_requested": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def batch(*signals: dict[str, object], **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "batch_id": "writer_batch_001",
        "shadow_run_id": "writer_shadow_run_001",
        "mode": "PAPER",
        "expected_account_id": "DUM882026",
        "signal_items": [{"signal": row} for row in signals],
        "submit_enabled": False,
        "live_money_readiness": False,
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
        "source": "unit_test_signal_batch_writer",
        "min_signal_score": "0.70",
    }


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": "writer_manifest_001",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_lane_registry_writer_test_v1",
        "registry_path": "config/track_b_lane_registry.json",
        "input_source_type": "SHADOW_SIGNAL_FILE",
        "output_root": "outputs/track_b_execution_core/shadow_runs/writer_manifest_001",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def registry() -> dict[str, object]:
    return {
        "registry_version": "track_b_lane_registry_writer_test_v1",
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
        "listener_id": "signal_batch_writer_listener_test",
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


def test_writer_writes_valid_batch_file_to_inbox(tmp_path: Path) -> None:
    result = write_signal_batch_to_inbox(
        inbox_dir=tmp_path / "inbox",
        signal_payloads=[signal()],
        batch_id="writer_batch_valid",
        shadow_run_id="writer_shadow_run_valid",
        source_id="unit_test",
        expected_account_id="DUM882026",
        output_root=tmp_path / "writer_reports",
        writer_id="writer-valid",
        now=aware_now(),
    )

    assert result.verdict == SignalBatchWriterVerdict.WROTE_BATCH
    assert result.batch_json is not None
    assert result.batch_json.exists()
    payload = json.loads(result.batch_json.read_text(encoding="utf-8"))
    assert payload["batch_id"] == "writer_batch_valid"
    assert payload["writer_metadata"]["source_id"] == "unit_test"
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_writer_uses_unique_filenames_without_overwriting(tmp_path: Path) -> None:
    kwargs = {
        "inbox_dir": tmp_path / "inbox",
        "signal_payloads": [signal()],
        "batch_id": "same_batch",
        "source_id": "unit_test",
        "expected_account_id": "DUM882026",
        "output_root": tmp_path / "writer_reports",
        "now": aware_now(),
    }

    first = write_signal_batch_to_inbox(writer_id="writer-one", **kwargs)
    second = write_signal_batch_to_inbox(writer_id="writer-two", **kwargs)

    assert first.batch_json is not None
    assert second.batch_json is not None
    assert first.batch_json != second.batch_json
    assert first.batch_json.exists()
    assert second.batch_json.exists()


def test_writer_rejects_invalid_signal_safely(tmp_path: Path) -> None:
    result = write_signal_batch_to_inbox(
        inbox_dir=tmp_path / "inbox",
        signal_payloads=[signal(strategy_id="")],
        batch_id="writer_invalid_signal",
        expected_account_id="DUM882026",
        output_root=tmp_path / "writer_reports",
        writer_id="writer-invalid-signal",
        now=aware_now(),
    )

    assert result.verdict == SignalBatchWriterVerdict.BLOCKED_INVALID_SIGNAL
    assert result.batch_json is None
    assert not list((tmp_path / "inbox").glob("*.json"))
    assert result.report["submit_attempted"] is False
    assert result.report["primary_blocker"]


def test_writer_rejects_invalid_batch_safely(tmp_path: Path) -> None:
    result = write_signal_batch_to_inbox(
        inbox_dir=tmp_path / "inbox",
        batch_payload=batch(signal(), mode="LIVE"),
        source_id="unit_test",
        expected_account_id="DUM882026",
        output_root=tmp_path / "writer_reports",
        writer_id="writer-invalid-batch",
        now=aware_now(),
    )

    assert result.verdict == SignalBatchWriterVerdict.BLOCKED_INVALID_BATCH
    assert result.batch_json is None
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert result.report["paper_proof_cli_wired"] is False


def test_listener_can_process_file_produced_by_writer(tmp_path: Path) -> None:
    config = listener_config(tmp_path)
    writer = write_signal_batch_to_inbox(
        inbox_dir=Path(str(config["inbox_dir"])),
        signal_payloads=[signal()],
        batch_id="writer_to_listener_batch",
        shadow_run_id="writer_to_listener_run",
        source_id="unit_test",
        expected_account_id="DUM882026",
        output_root=tmp_path / "writer_reports",
        writer_id="writer-to-listener",
        now=aware_now(),
    )

    result = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="writer-listener-cycle",
        now=aware_now(),
    )

    assert writer.batch_json is not None
    assert result.verdict == ShadowListenerVerdict.COMPLETED
    assert result.report["files_succeeded"] == 1
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert not writer.batch_json.exists()
    assert len(result.report["processed_paths"]) == 1
    assert Path(result.report["processed_paths"][0]).exists()


def test_signal_batch_writer_cli_writes_batch(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    signal_json = tmp_path / "signal.json"
    write_json(signal_json, signal())

    exit_code = signal_batch_writer_cli_main(
        [
            "--signal-json",
            str(signal_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--source-id",
            "cli_test",
            "--batch-id",
            "cli_batch",
            "--expected-account-id",
            "DUM882026",
            "--output-root",
            str(tmp_path / "writer_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["signal_batch_writer_verdict"] == "SIGNAL_BATCH_WRITER_WROTE_BATCH"
    assert output["batch_file_written"] is True
    assert output["batch_id"] == "cli_batch"
    assert Path(output["batch_json_path"]).exists()
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
