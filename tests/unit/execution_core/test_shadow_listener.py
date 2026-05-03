from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.shadow_listener import ShadowListenerVerdict, run_shadow_listener_cycle
from mgc_v05l.execution_core.shadow_listener_cli import main as shadow_listener_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 45, tzinfo=timezone.utc)


def signal(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_id": "shadow-listener-signal-001",
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
        "source": "unit_test_shadow_listener",
        "reason": "synthetic shadow listener signal",
        "submit_requested": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def signal_batch(*signals: dict[str, object], **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "batch_id": "shadow_listener_batch_001",
        "shadow_run_id": "shadow_listener_run_001",
        "mode": "PAPER",
        "expected_account_id": "DUM882026",
        "default_policy_id": "shadow_listener_policy_001",
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
        "source": "unit_test_shadow_listener",
        "min_signal_score": "0.70",
    }


def manifest(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "run_id": "shadow_listener_manifest_001",
        "run_mode": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "registry_version": "track_b_lane_registry_test_v1",
        "registry_path": "config/track_b_lane_registry.json",
        "input_source_type": "SHADOW_SIGNAL_FILE",
        "output_root": "outputs/track_b_execution_core/shadow_runs/shadow_listener_manifest_001",
        "required_artifacts": ["intent_validation", "lane_validation", "order_plan", "shadow_evaluation"],
        "submit_enabled": False,
        "live_money_readiness": False,
        "generated_at": aware_now().isoformat(),
    }
    payload.update(overrides)
    return payload


def registry() -> dict[str, object]:
    return {
        "registry_version": "track_b_lane_registry_test_v1",
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


def listener_config(root: Path, **overrides: object) -> dict[str, object]:
    write_json(root / "policy.json", policy())
    write_json(root / "manifest.json", manifest())
    write_json(root / "registry.json", registry())
    payload: dict[str, object] = {
        "listener_id": "shadow_listener_test",
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
    payload.update(overrides)
    return payload


def test_valid_listener_config_processes_one_signal_batch(tmp_path: Path) -> None:
    config = listener_config(tmp_path)
    inbox_file = Path(str(config["inbox_dir"])) / "batch.json"
    write_json(inbox_file, signal_batch(signal()))

    result = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="cycle-valid",
        now=aware_now(),
    )

    assert result.verdict == ShadowListenerVerdict.COMPLETED
    assert result.report["listener_verdict"] == "SHADOW_LISTENER_CYCLE_COMPLETED"
    assert result.report["files_discovered"] == 1
    assert result.report["files_succeeded"] == 1
    assert result.report["files_failed"] == 0
    assert len(result.report["processed_paths"]) == 1
    assert Path(result.report["processed_paths"][0]).exists()
    assert not inbox_file.exists()
    assert len(result.report["runner_summary_paths"]) == 1
    assert Path(result.report["runner_summary_paths"][0]).exists()
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert result.report["paper_proof_cli_wired"] is False
    health = json.loads(Path(result.report["health_report_path"]).read_text(encoding="utf-8"))
    latest_health = json.loads(Path(result.report["latest_health_report_path"]).read_text(encoding="utf-8"))
    assert health["health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert latest_health["health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert health["latest_cycle_summary_path"] == str(result.report_json)
    assert health["latest_runner_summary_paths"] == result.report["runner_summary_paths"]
    assert health["submit_allowed"] is False
    assert health["submit_attempted"] is False
    assert health["live_money_readiness"] is False


def test_empty_inbox_produces_no_files_verdict(tmp_path: Path) -> None:
    config = listener_config(tmp_path)

    result = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="cycle-empty",
        now=aware_now(),
    )

    assert result.verdict == ShadowListenerVerdict.NO_FILES
    assert result.report["files_discovered"] == 0
    assert result.report["files_succeeded"] == 0
    assert result.report["submit_allowed"] is False
    health = json.loads(Path(result.report["health_report_path"]).read_text(encoding="utf-8"))
    assert health["health_verdict"] == "SHADOW_LISTENER_HEALTH_NO_FILES"
    assert health["last_primary_blocker"] is None


def test_invalid_config_blocks_clearly(tmp_path: Path) -> None:
    config = listener_config(tmp_path, mode="LIVE")

    result = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="cycle-invalid-config",
        now=aware_now(),
    )

    assert result.verdict == ShadowListenerVerdict.BLOCKED_INVALID_CONFIG
    assert result.report["primary_blocker"] == "Shadow listener accepts SHADOW or PAPER_REVIEW no-submit mode only."
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    health = json.loads(Path(result.report["health_report_path"]).read_text(encoding="utf-8"))
    assert health["health_verdict"] == "SHADOW_LISTENER_HEALTH_BLOCKED_INVALID_CONFIG"
    assert health["last_primary_blocker"] == "Shadow listener accepts SHADOW or PAPER_REVIEW no-submit mode only."


def test_invalid_signal_batch_moves_to_failed_and_appears_in_summary(tmp_path: Path) -> None:
    config = listener_config(tmp_path)
    inbox_file = Path(str(config["inbox_dir"])) / "bad_batch.json"
    write_json(inbox_file, signal_batch(signal(), mode="LIVE"))

    result = run_shadow_listener_cycle(
        config_payload=config,
        output_root=tmp_path / "listener_outputs",
        cycle_id="cycle-bad-file",
        now=aware_now(),
    )

    assert result.verdict == ShadowListenerVerdict.COMPLETED_WITH_FAILURES
    assert result.report["files_failed"] == 1
    assert len(result.report["failed_paths"]) == 1
    assert Path(result.report["failed_paths"][0]).exists()
    assert result.report["runner_summary_paths"]
    event = json.loads(Path(result.report["event_report_paths"][0]).read_text(encoding="utf-8"))
    assert event["listener_file_verdict"] == "SHADOW_LISTENER_FILE_FAILED"
    assert event["runner_verdict"] == "SHADOW_REPLAY_RUN_BLOCKED_SIGNAL_BATCH"
    assert event["submit_allowed"] is False
    health = json.loads(Path(result.report["health_report_path"]).read_text(encoding="utf-8"))
    assert health["health_verdict"] == "SHADOW_LISTENER_HEALTH_DEGRADED_FAILURES"
    assert health["files_failed"] == 1
    assert health["latest_runner_summary_paths"] == result.report["runner_summary_paths"]


def test_listener_cli_reads_config_and_writes_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    config = listener_config(tmp_path)
    inbox_file = Path(str(config["inbox_dir"])) / "batch.json"
    write_json(inbox_file, signal_batch(signal()))
    config_path = tmp_path / "listener_config.json"
    write_json(config_path, config)

    exit_code = shadow_listener_cli_main(
        [
            "--listener-config-json",
            str(config_path),
            "--output-root",
            str(tmp_path / "cli_outputs"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["listener_verdict"] == "SHADOW_LISTENER_CYCLE_COMPLETED"
    assert output["files_succeeded"] == 1
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
    assert Path(output["health_report"]).exists()
    assert Path(output["latest_health_report"]).exists()
