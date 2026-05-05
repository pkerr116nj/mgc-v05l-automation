from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.operator_status import OperatorStatusInputs, OperatorStatusVerdict, create_operator_status_summary
from mgc_v05l.execution_core.operator_status_cli import main as operator_status_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc)


def write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def listener_health(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_shadow_listener_health_v1",
        "generated_at": aware_now().isoformat(),
        "listener_id": "shadow_listener_test",
        "listener_cycle_id": "cycle-ok",
        "health_verdict": "SHADOW_LISTENER_HEALTH_OK",
        "last_cycle_verdict": "SHADOW_LISTENER_CYCLE_COMPLETED",
        "last_cycle_generated_at": aware_now().isoformat(),
        "inbox_dir": str(tmp_path / "inbox"),
        "processing_dir": str(tmp_path / "processing"),
        "processed_dir": str(tmp_path / "processed"),
        "failed_dir": str(tmp_path / "failed"),
        "files_discovered": 1,
        "files_processed": 1,
        "files_succeeded": 1,
        "files_failed": 0,
        "last_success_at": aware_now().isoformat(),
        "last_failure_at": None,
        "last_primary_blocker": None,
        "last_required_next_action": "Review generated no-submit artifacts.",
        "latest_cycle_summary_path": "cycle_summary.json",
        "latest_runner_summary_paths": ["runner_summary.json"],
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "health_report_path": "health.json",
        "latest_health_report_path": "latest_health.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "listener_health.json", payload)


def listener_heartbeat(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_shadow_listener_watch_heartbeat_v1",
        "generated_at": aware_now().isoformat(),
        "listener_id": "shadow_listener_test",
        "listener_watch_id": "watch-001",
        "listener_mode": "watch",
        "watch_verdict": "SHADOW_LISTENER_WATCH_COMPLETED",
        "watch_started_at": aware_now().isoformat(),
        "watch_ended_at": aware_now().isoformat(),
        "watch_exited_normally": True,
        "current_cycle_number": 3,
        "last_cycle_number": 3,
        "last_cycle_start_at": aware_now().isoformat(),
        "last_cycle_end_at": aware_now().isoformat(),
        "last_listener_verdict": "SHADOW_LISTENER_CYCLE_NO_FILES",
        "last_health_verdict": "SHADOW_LISTENER_HEALTH_NO_FILES",
        "processed_cycles": 1,
        "failed_cycles": 0,
        "no_file_cycles": 2,
        "runner_summary_paths": ["runner_summary.json"],
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Watch mode completed the bounded cycle count. Submit gates remain external and required.",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "paper_proof_cli_wired": False,
        "heartbeat_json_path": "latest_shadow_listener_heartbeat.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "listener_heartbeat.json", payload)


def signal_batch_writer_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_signal_batch_writer_v1",
        "generated_at": aware_now().isoformat(),
        "signal_batch_writer_id": "writer-001",
        "signal_batch_writer_verdict": "SIGNAL_BATCH_WRITER_WROTE_BATCH",
        "batch_file_written": True,
        "batch_id": "writer_batch_001",
        "shadow_run_id": "writer_shadow_run_001",
        "source_id": "unit_test_writer",
        "inbox_dir": str(tmp_path / "inbox"),
        "batch_json_path": str(tmp_path / "inbox" / "writer_batch_001.json"),
        "total_signals": 2,
        "listener_invoked": False,
        "runner_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "paper_proof_cli_wired": False,
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Let shadow_listener process the written no-submit signal batch file.",
        "report_json_path": "writer_report.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "signal_batch_writer_report.json", payload)


def backend_health_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "status": "ok",
        "ready": True,
        "generated_at": aware_now().isoformat(),
        "url": "http://127.0.0.1:8790/",
        "host": "127.0.0.1",
        "port": 8790,
        "pid": 12345,
        "checks": {
            "operator_surface_loadable": {"ok": True, "detail": "Operator surface loaded."},
            "api_dashboard_responding": {"ok": True, "detail": "/api/dashboard responded."},
            "startup_convergence_stable": {"ok": True, "detail": "Dashboard startup is stable."},
        },
        "error": None,
        "info_file": "outputs/operator_dashboard/runtime/operator_dashboard.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "backend_health.json", payload)


def multi_strategy_runtime_cycle_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_multi_strategy_runtime_cycle_v1",
        "generated_at": aware_now().isoformat(),
        "track_b_multi_strategy_runtime_cycle_id": "multi-cycle-001",
        "multi_strategy_runtime_cycle_verdict": "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT",
        "mode": "PAPER",
        "source_id": "unit_test_multi_strategy_cycle",
        "evaluated_strategies": [
            {
                "strategy_id": "ASIAN_DRIFT_V1",
                "strategy_runtime_verdict": "NO_SIGNAL_NO_MUTATION",
                "registry_metadata": {"strategy_registry_live_money_eligible": False},
            },
            {
                "strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
                "strategy_runtime_verdict": "SIGNAL_READY_NO_SUBMIT",
                "registry_metadata": {"strategy_registry_live_money_eligible": False},
            },
        ],
        "candidate_signals": [
            {
                "strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
                "signal_source": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
                "real_strategy_signal": True,
                "signal_direction": "SHORT",
                "paper_eligible": True,
                "live_money_eligible": False,
            }
        ],
        "suppressed_signals": [],
        "arbitration_result": {"strategy_arbitration_verdict": "TRACK_B_STRATEGY_REGISTRY_READY"},
        "chosen_signal": {"strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1", "signal_direction": "SHORT"},
        "chosen_strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        "reason_no_signal_chosen": "One signal was chosen, but explicit PAPER submit flags were not supplied.",
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "decision_journal_invoked": True,
        "decision_journal_summary_path": "latest_track_b_decision_journal_summary.json",
        "decision_journal_active_path": "track_b_decision_journal.jsonl",
        "decision_journal_heartbeat_path": "track_b_runtime_heartbeat.jsonl",
        "decision_journal_full_records_written": 1,
        "decision_journal_tier_counts": {"TIER_3_SIGNAL_TRADE_DECISION": 1},
        "decision_journal_error": None,
        "primary_blocker": None,
        "required_next_action": "Exactly one real strategy signal is ready, but explicit PAPER submit flags were not supplied.",
        "report_json_path": "track_b_multi_strategy_runtime_cycle_report.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "track_b_multi_strategy_runtime_cycle_report.json", payload)


def strategy_signal_adapter_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_strategy_signal_adapter_v1",
        "generated_at": aware_now().isoformat(),
        "strategy_signal_adapter_id": "adapter-001",
        "adapter_name": "demo_candle_direction_signal",
        "adapter_verdict": "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH",
        "strategy_id": "track_b_test_strategy",
        "signal_family": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "source_id": "unit_test_strategy_adapter",
        "batch_id": "strategy_adapter_batch_001",
        "signal_count": 1,
        "output_batch_path": str(tmp_path / "inbox" / "strategy_adapter_batch_001.json"),
        "candle_producer_report_path": str(tmp_path / "candle_report.json"),
        "downstream_writer_report_path": str(tmp_path / "writer_report.json"),
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "lane_registry_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Let shadow_listener process the adapter-produced no-submit signal batch file.",
        "report_json_path": "strategy_adapter_report.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "strategy_signal_adapter_report.json", payload)


def candle_signal_producer_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_candle_signal_producer_v1",
        "generated_at": aware_now().isoformat(),
        "candle_signal_producer_id": "candle-producer-001",
        "producer_verdict": "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH",
        "source_id": "unit_test_candle",
        "batch_id": "candle_batch_001",
        "signal_count": 1,
        "output_batch_path": str(tmp_path / "inbox" / "candle_batch_001.json"),
        "writer_report_path": str(tmp_path / "writer_report.json"),
        "signal_batch_writer_verdict": "SIGNAL_BATCH_WRITER_WROTE_BATCH",
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Let shadow_listener process the produced no-submit signal batch file.",
        "report_json_path": "candle_signal_producer_report.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "candle_signal_producer_report.json", payload)


def databento_candle_observer_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_databento_candle_observer_v1",
        "generated_at": aware_now().isoformat(),
        "databento_candle_observer_id": "databento-observer-001",
        "observer_mode": "one_shot",
        "observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        "source_id": "unit_test_databento_observer",
        "contract_key": "MGC-202606",
        "local_execution_contract_key": "MGC-202606",
        "databento_continuous_symbol": "MGC.v.0",
        "databento_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "timeframe": "quote_snapshot",
        "event_timestamp": aware_now().isoformat(),
        "candle_timestamp": aware_now().isoformat(),
        "output_candle_event_path": str(tmp_path / "databento_candle_event.json"),
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Run strategy_signal_adapter_cli explicitly if needed.",
        "report_json_path": "databento_candle_observer_report.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "databento_candle_observer_report.json", payload)


def databento_candle_observer_heartbeat(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_databento_candle_observer_heartbeat_v1",
        "generated_at": aware_now().isoformat(),
        "observer_mode": "watch",
        "watch_id": "databento-watch-001",
        "current_cycle_number": 3,
        "max_cycles": 5,
        "processed_cycles": 2,
        "no_data_cycles": 1,
        "error_cycles": 0,
        "last_observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        "last_event_timestamp": aware_now().isoformat(),
        "contract_key": "MGC-202606",
        "local_execution_contract_key": "MGC-202606",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "output_event_path": str(tmp_path / "latest_databento_candle_event.json"),
        "watch_exited_normally": False,
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Continue explicit no-submit downstream steps only when operator review requires them.",
        "heartbeat_json_path": "databento_candle_observer_heartbeat.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "databento_candle_observer_heartbeat.json", payload)


def observation_runner_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_observation_runner_v1",
        "generated_at": aware_now().isoformat(),
        "track_b_observation_runner_id": "observation-runner-001",
        "source_id": "unit_test_observation_runner",
        "runner_verdict": "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW",
        "mode": "watch",
        "current_cycle": 2,
        "watch_exited_normally": True,
        "databento_observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        "strategy_adapter_verdict": "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH",
        "candle_producer_verdict": "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH",
        "signal_batch_writer_verdict": "SIGNAL_BATCH_WRITER_WROTE_BATCH",
        "listener_verdict": "SHADOW_LISTENER_CYCLE_COMPLETED",
        "listener_health_verdict": "SHADOW_LISTENER_HEALTH_OK",
        "operator_status_verdict": "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW",
        "latest_operator_status_path": str(tmp_path / "operator_status" / "latest_operator_status_summary.json"),
        "required_next_action": "Review Track B Status UI and latest no-submit artifacts.",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "paper_proof_cli_called": False,
        "place_order_called": False,
        "cancel_called": False,
        "report_json_path": "track_b_observation_runner_report.json",
        "latest_report_json_path": str(tmp_path / "track_b_observation_runner" / "latest_track_b_observation_runner_report.json"),
        "primary_blocker": None,
        "secondary_blockers": [],
    }
    payload.update(overrides)
    return write_json(tmp_path / "track_b_observation_runner_report.json", payload)


def strategy_rule_runner_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_strategy_rule_runner_v1",
        "generated_at": aware_now().isoformat(),
        "track_b_strategy_rule_runner_id": "strategy-rule-runner-001",
        "strategy_rule_runner_verdict": "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL",
        "strategy_rule_id": "mgc_realtime_quote_demo_long_v1",
        "rule_name": "mgc_realtime_quote_momentum_reclaim_demo",
        "rule_mode": "DEMO_LONG_ONLY",
        "source_id": "unit_test_strategy_rule",
        "input_event_path": "latest_databento_candle_event.json",
        "input_quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "decision": "LONG",
        "decision_reason": "DEMO_LONG_ONLY emitted explicit LONG from valid realtime Databento MGC quote evidence with --emit-signal.",
        "signal_emitted": True,
        "signal_direction": "LONG",
        "downstream_strategy_adapter_report_path": "strategy_adapter_report.json",
        "downstream_candle_producer_report_path": "candle_signal_producer_report.json",
        "downstream_signal_batch_writer_report_path": "signal_batch_writer_report.json",
        "output_batch_path": str(tmp_path / "inbox" / "strategy_rule_batch.json"),
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "track_b_strategy_rule_runner_report.json",
        "latest_report_json_path": str(tmp_path / "track_b_strategy_rule_runner" / "latest_track_b_strategy_rule_runner_report.json"),
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "Let shadow_listener process the no-submit strategy-rule signal batch.",
    }
    payload.update(overrides)
    return write_json(tmp_path / "track_b_strategy_rule_runner_report.json", payload)


def strategy_paper_runner_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_strategy_paper_runner_v1",
        "generated_at": aware_now().isoformat(),
        "track_b_strategy_paper_runner_id": "strategy-paper-runner-001",
        "strategy_paper_runner_verdict": "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED",
        "mode": "PAPER",
        "source_id": "unit_test_strategy_paper",
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "rule_id": "mgc_realtime_quote_demo_long_v1",
        "rule_mode": "DEMO_LONG_ONLY",
        "rule_decision": "LONG",
        "signal_emitted": True,
        "signal_direction": "LONG",
        "readiness_runner_verdict": "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW",
        "readiness_verdict": "READY_FOR_PAPER_PROOF",
        "paper_submit_requested": True,
        "paper_proof_invoked": True,
        "paper_proof_classification": "TRACK_B_PAPER_PROOF_PASSED",
        "paper_proof_report_path": "proof_report.json",
        "final_flat": True,
        "submit_allowed": True,
        "submit_attempted": True,
        "live_money_readiness": False,
        "primary_blocker": None,
        "secondary_blockers": [],
        "required_next_action": "PAPER strategy proof lifecycle passed and final broker state is flat.",
        "report_json_path": "track_b_strategy_paper_runner_report.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "track_b_strategy_paper_runner_report.json", payload)


def readiness_check_runner_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "schema_version": "track_b_readiness_check_runner_v1",
        "generated_at": aware_now().isoformat(),
        "track_b_readiness_check_runner_id": "readiness-check-001",
        "runner_verdict": "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW",
        "recovery_verdict": "RECOVERY_READY_CLEAN",
        "preflight_verdict": "READY_READ_ONLY",
        "databento_observer_verdict": "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT",
        "current_quote_available": True,
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "wait_succeeded": True,
        "readiness_verdict": "READY_FOR_PAPER_PROOF",
        "required_next_action": "paper_proof_cli remains a separate explicit operator decision and was not called.",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "paper_proof_cli_called": False,
        "place_order_called": False,
        "cancel_called": False,
        "report_json_path": "track_b_readiness_check_runner_report.json",
        "latest_report_json_path": str(tmp_path / "track_b_readiness_check_runner" / "latest_track_b_readiness_check_runner_report.json"),
        "primary_blocker": None,
        "secondary_blockers": [],
    }
    payload.update(overrides)
    return write_json(tmp_path / "track_b_readiness_check_runner_report.json", payload)


def recovery_report(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "classification": "RECOVERY_READY_CLEAN",
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": "Broker state is clean.",
        "submit_allowed": True,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "recovery.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "recovery.json", payload)


def readiness_summary(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": "Readiness inputs are clean.",
        "submit_allowed": True,
        "submit_attempted": False,
        "live_money_readiness": False,
        "report_json_path": "readiness.json",
    }
    payload.update(overrides)
    return write_json(tmp_path / "readiness.json", payload)


def test_listener_health_ok_produces_ok_for_shadow_review(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-ok",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW
    assert result.report["status_verdict"] == "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    assert result.report["shadow_listener_health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert result.report["primary_blocker"] is None
    assert "recovery" in result.report["reports_missing"]
    assert result.report["secondary_blockers"]
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    latest_report = Path(result.report["latest_report_json_path"])
    assert latest_report == tmp_path / "operator_status" / "latest_operator_status_summary.json"
    assert latest_report.exists()
    latest_payload = json.loads(latest_report.read_text(encoding="utf-8"))
    assert latest_payload["operator_status_id"] == "status-ok"
    assert latest_payload["report_json_path"] == str(result.report_json)


def test_listener_heartbeat_is_summarized_for_watch_mode(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-heartbeat",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW
    assert result.report["listener_mode"] == "watch"
    assert result.report["listener_current_cycle_number"] == 3
    assert result.report["listener_last_cycle_number"] == 3
    assert result.report["listener_processed_cycles"] == 1
    assert result.report["listener_failed_cycles"] == 0
    assert result.report["listener_no_file_cycles"] == 2
    assert result.report["listener_watch_exited_normally"] is True
    assert result.report["listener_last_health_verdict"] == "SHADOW_LISTENER_HEALTH_NO_FILES"
    assert result.report["latest_listener_cycle_verdict"] == "SHADOW_LISTENER_CYCLE_NO_FILES"
    assert "listener_health" in result.report["reports_missing"]
    assert result.report["submit_allowed"] is False


def test_missing_listener_heartbeat_is_explicit_when_health_is_supplied(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-missing-heartbeat",
        now=aware_now(),
    )

    assert result.report["listener_mode"] == "NOT_PROVIDED"
    assert result.report["listener_current_cycle_number"] == "NOT_PROVIDED"
    assert "listener_heartbeat" in result.report["reports_missing"]
    assert result.report["reports_considered"]["listener_heartbeat"] is False


def test_signal_batch_writer_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            signal_batch_writer_report_json=signal_batch_writer_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-writer",
        now=aware_now(),
    )

    assert result.report["signal_batch_writer_verdict"] == "SIGNAL_BATCH_WRITER_WROTE_BATCH"
    assert result.report["signal_batch_writer_batch_file_written"] is True
    assert result.report["signal_batch_writer_batch_json_path"].endswith("writer_batch_001.json")
    assert result.report["signal_batch_writer_total_signals"] == 2
    assert result.report["recent_writer_output_present"] is True
    assert result.report["listener_invoked"] is False
    assert result.report["runner_invoked"] is False
    assert result.report["submit_attempted"] is False


def test_backend_health_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            backend_health_json=backend_health_report(tmp_path),
            listener_health_json=listener_health(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-backend-health",
        now=aware_now(),
    )

    assert result.report["backend_health_status"] == "ok"
    assert result.report["backend_health_ready"] is True
    assert result.report["backend_health_url"] == "http://127.0.0.1:8790/"
    assert result.report["backend_health_api_dashboard_ok"] is True
    assert result.report["backend_health_operator_surface_ok"] is True
    assert result.report["latest_output_paths"]["backend_health"] == "outputs/operator_dashboard/runtime/operator_dashboard.json"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_backend_readiness_contract_is_summarized(tmp_path: Path) -> None:
    backend_readiness = write_json(
        tmp_path / "operator_dashboard_readiness.json",
        {
            "contract_version": "dashboard_readiness_contract.v1",
            "readiness_state": "READY",
            "reason_detail": "Dashboard ownership, health, payload validity, and identity remained stable.",
            "launch_allowed": True,
            "configured_url": "http://127.0.0.1:8790/",
            "listener": {
                "reachable": True,
                "health_url": "http://127.0.0.1:8790/health",
                "dashboard_api_url": "http://127.0.0.1:8790/api/dashboard",
            },
            "health": {"status": "ok", "ready": True, "pid": 12345},
            "payload": {
                "reachable": True,
                "json_valid": True,
                "startup_control_plane_present": True,
            },
            "control_plane": {"convergence_stable_ready": True},
        },
    )

    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            backend_health_json=backend_readiness,
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-backend-readiness-contract",
        now=aware_now(),
    )

    assert result.report["backend_health_status"] == "ok"
    assert result.report["backend_health_ready"] is True
    assert result.report["backend_health_url"] == "http://127.0.0.1:8790/"
    assert result.report["backend_health_api_dashboard_ok"] is True
    assert result.report["backend_health_operator_surface_ok"] is True
    assert result.report["backend_health_startup_stable"] is True
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_backend_down_state_is_reported_clearly_without_readiness(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            backend_health_json=backend_health_report(
                tmp_path,
                status="degraded",
                ready=False,
                error="dashboard_snapshot_failed",
                checks={
                    "operator_surface_loadable": {"ok": False, "detail": "No snapshot."},
                    "api_dashboard_responding": {"ok": False, "detail": "/api/dashboard unreachable."},
                    "startup_convergence_stable": {"ok": False, "detail": "Backend down."},
                },
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-backend-down",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.UNKNOWN
    assert result.report["backend_health_status"] == "degraded"
    assert result.report["backend_health_ready"] is False
    assert result.report["backend_health_api_dashboard_ok"] is False
    assert result.report["backend_health_operator_surface_ok"] is False
    assert result.report["primary_blocker"] == "dashboard_snapshot_failed"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_multi_strategy_runtime_cycle_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            backend_health_json=backend_health_report(tmp_path),
            track_b_multi_strategy_runtime_cycle_report_json=multi_strategy_runtime_cycle_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-multi-strategy-cycle",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW
    assert result.report["multi_strategy_runtime_cycle_verdict"] == "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT"
    assert result.report["multi_strategy_chosen_strategy_id"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["multi_strategy_candidate_signals"][0]["signal_direction"] == "SHORT"
    assert result.report["multi_strategy_suppressed_signals"] == []
    assert result.report["multi_strategy_readiness_invoked"] is False
    assert result.report["multi_strategy_paper_proof_invoked"] is False
    assert result.report["multi_strategy_submit_attempted"] is False
    assert result.report["multi_strategy_broker_state_mutated"] is False
    assert result.report["multi_strategy_live_money_readiness"] is False
    assert result.report["track_b_decision_journal_invoked"] is True
    assert result.report["track_b_decision_journal_summary_path"] == "latest_track_b_decision_journal_summary.json"
    assert result.report["track_b_decision_journal_active_path"] == "track_b_decision_journal.jsonl"
    assert result.report["track_b_decision_journal_full_records_written"] == 1
    assert result.report["track_b_decision_journal_tier_counts"] == {"TIER_3_SIGNAL_TRADE_DECISION": 1}
    assert result.report["latest_output_paths"]["track_b_multi_strategy_runtime_cycle"] == "track_b_multi_strategy_runtime_cycle_report.json"
    assert result.report["latest_output_paths"]["track_b_decision_journal_summary"] == "latest_track_b_decision_journal_summary.json"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_blocked_multi_strategy_runtime_cycle_degrades_operator_status(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_multi_strategy_runtime_cycle_report_json=multi_strategy_runtime_cycle_report(
                tmp_path,
                multi_strategy_runtime_cycle_verdict="TRACK_B_MULTI_STRATEGY_RUNTIME_ARBITRATION_BLOCKED",
                primary_blocker="Conflicting LONG and SHORT strategy signals require explicit arbitration.",
                required_next_action="Do not submit; review suppressed candidates.",
                candidate_signals=[
                    {"strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1", "signal_direction": "SHORT"},
                    {"strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1", "signal_direction": "LONG"},
                ],
                suppressed_signals=[
                    {"strategy_id": "ASIA_EARLY_PAUSE_RESUME_SHORT_V1", "signal_direction": "SHORT"},
                    {"strategy_id": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1", "signal_direction": "LONG"},
                ],
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-multi-strategy-blocked",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.BLOCKED_READINESS
    assert result.report["primary_blocker"] == "Conflicting LONG and SHORT strategy signals require explicit arbitration."
    assert result.report["multi_strategy_runtime_cycle_verdict"] == "TRACK_B_MULTI_STRATEGY_RUNTIME_ARBITRATION_BLOCKED"
    assert len(result.report["multi_strategy_suppressed_signals"]) == 2
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_upstream_strategy_and_candle_reports_are_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            strategy_signal_adapter_report_json=strategy_signal_adapter_report(tmp_path),
            candle_signal_producer_report_json=candle_signal_producer_report(tmp_path),
            signal_batch_writer_report_json=signal_batch_writer_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-upstream-chain",
        now=aware_now(),
    )

    assert result.report["strategy_adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert result.report["strategy_id"] == "track_b_test_strategy"
    assert result.report["signal_family"] == "track_b_test_strategy"
    assert result.report["strategy_source_id"] == "unit_test_strategy_adapter"
    assert result.report["strategy_batch_id"] == "strategy_adapter_batch_001"
    assert result.report["strategy_signal_count"] == 1
    assert result.report["strategy_output_batch_path"].endswith("strategy_adapter_batch_001.json")
    assert result.report["strategy_downstream_candle_producer_report_path"].endswith("candle_report.json")
    assert result.report["strategy_downstream_writer_report_path"].endswith("writer_report.json")
    assert result.report["candle_producer_verdict"] == "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"
    assert result.report["candle_source_id"] == "unit_test_candle"
    assert result.report["candle_batch_id"] == "candle_batch_001"
    assert result.report["candle_signal_count"] == 1
    assert result.report["candle_output_batch_path"].endswith("candle_batch_001.json")
    assert result.report["candle_downstream_writer_report_path"].endswith("writer_report.json")
    assert result.report["latest_output_paths"]["strategy_signal_adapter"] == "strategy_adapter_report.json"
    assert result.report["latest_output_paths"]["candle_signal_producer"] == "candle_signal_producer_report.json"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    latest = json.loads((tmp_path / "operator_status" / "latest_operator_status_summary.json").read_text(encoding="utf-8"))
    assert latest["strategy_adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert latest["candle_producer_verdict"] == "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"


def test_databento_observer_report_and_heartbeat_are_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            databento_candle_observer_report_json=databento_candle_observer_report(tmp_path),
            databento_candle_observer_heartbeat_json=databento_candle_observer_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-databento-observer",
        now=aware_now(),
    )

    assert result.report["databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["databento_contract_key"] == "MGC-202606"
    assert result.report["databento_symbol"] == "MGC.v.0"
    assert result.report["databento_dataset"] == "GLBX.MDP3"
    assert result.report["databento_timeframe"] == "quote_snapshot"
    assert result.report["databento_source_id"] == "unit_test_databento_observer"
    assert result.report["databento_event_timestamp"] == aware_now().isoformat()
    assert result.report["databento_output_event_path"].endswith("databento_candle_event.json")
    assert result.report["databento_observer_submit_allowed"] is False
    assert result.report["databento_observer_submit_attempted"] is False
    assert result.report["databento_observer_live_money_readiness"] is False
    assert result.report["databento_observer_mode"] == "watch"
    assert result.report["databento_observer_current_cycle"] == 3
    assert result.report["databento_observer_processed_cycles"] == 2
    assert result.report["databento_observer_no_data_cycles"] == 1
    assert result.report["databento_observer_error_cycles"] == 0
    assert result.report["databento_observer_last_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["databento_observer_watch_exited_normally"] is False
    assert result.report["latest_output_paths"]["databento_candle_observer"] == "databento_candle_observer_report.json"
    assert result.report["latest_output_paths"]["databento_candle_observer_heartbeat"] == "databento_candle_observer_heartbeat.json"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    latest = json.loads((tmp_path / "operator_status" / "latest_operator_status_summary.json").read_text(encoding="utf-8"))
    assert latest["databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"


def test_observation_runner_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            track_b_observation_runner_report_json=observation_runner_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-observation-runner",
        now=aware_now(),
    )

    assert result.report["observation_runner_verdict"] == "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW"
    assert result.report["observation_runner_mode"] == "watch"
    assert result.report["observation_runner_source_id"] == "unit_test_observation_runner"
    assert result.report["observation_runner_current_cycle"] == 2
    assert result.report["observation_runner_watch_exited_normally"] is True
    assert result.report["observation_runner_required_next_action"] == "Review Track B Status UI and latest no-submit artifacts."
    assert result.report["observation_runner_latest_report_path"].endswith("latest_track_b_observation_runner_report.json")
    assert result.report["observation_runner_latest_operator_status_path"].endswith("latest_operator_status_summary.json")
    assert result.report["observation_runner_databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["observation_runner_strategy_adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert result.report["observation_runner_candle_producer_verdict"] == "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"
    assert result.report["observation_runner_signal_batch_writer_verdict"] == "SIGNAL_BATCH_WRITER_WROTE_BATCH"
    assert result.report["observation_runner_listener_verdict"] == "SHADOW_LISTENER_CYCLE_COMPLETED"
    assert result.report["observation_runner_listener_health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert result.report["observation_runner_submit_allowed"] is False
    assert result.report["observation_runner_submit_attempted"] is False
    assert result.report["observation_runner_live_money_readiness"] is False
    assert result.report["latest_output_paths"]["track_b_observation_runner"] == "track_b_observation_runner_report.json"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    latest = json.loads((tmp_path / "operator_status" / "latest_operator_status_summary.json").read_text(encoding="utf-8"))
    assert latest["observation_runner_verdict"] == "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW"


def test_strategy_rule_runner_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            track_b_strategy_rule_runner_report_json=strategy_rule_runner_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-strategy-rule-runner",
        now=aware_now(),
    )

    assert result.report["strategy_rule_runner_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"
    assert result.report["strategy_rule_id"] == "mgc_realtime_quote_demo_long_v1"
    assert result.report["strategy_rule_name"] == "mgc_realtime_quote_momentum_reclaim_demo"
    assert result.report["strategy_rule_mode"] == "DEMO_LONG_ONLY"
    assert result.report["strategy_rule_decision"] == "LONG"
    assert result.report["strategy_rule_signal_emitted"] is True
    assert result.report["strategy_rule_signal_direction"] == "LONG"
    assert result.report["strategy_rule_input_quote_provider_mode"] == "REALTIME"
    assert result.report["strategy_rule_realtime_quote_received"] is True
    assert result.report["strategy_rule_current_quote_available"] is True
    assert result.report["strategy_rule_downstream_strategy_adapter_report_path"] == "strategy_adapter_report.json"
    assert result.report["strategy_rule_downstream_candle_producer_report_path"] == "candle_signal_producer_report.json"
    assert result.report["strategy_rule_downstream_signal_batch_writer_report_path"] == "signal_batch_writer_report.json"
    assert result.report["strategy_rule_output_batch_path"].endswith("strategy_rule_batch.json")
    assert result.report["strategy_rule_paper_proof_cli_called"] is False
    assert result.report["strategy_rule_submit_allowed"] is False
    assert result.report["strategy_rule_submit_attempted"] is False
    assert result.report["strategy_rule_live_money_readiness"] is False
    assert result.report["latest_output_paths"]["track_b_strategy_rule_runner"] == "track_b_strategy_rule_runner_report.json"
    latest = json.loads((tmp_path / "operator_status" / "latest_operator_status_summary.json").read_text(encoding="utf-8"))
    assert latest["strategy_rule_runner_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"


def test_strategy_paper_runner_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_strategy_paper_runner_report_json=strategy_paper_runner_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-strategy-paper-runner",
        now=aware_now(),
    )

    assert result.report["strategy_paper_runner_verdict"] == "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED"
    assert result.report["strategy_paper_rule_decision"] == "LONG"
    assert result.report["strategy_paper_signal_emitted"] is True
    assert result.report["strategy_paper_signal_direction"] == "LONG"
    assert result.report["strategy_paper_readiness_runner_verdict"] == "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW"
    assert result.report["strategy_paper_readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert result.report["strategy_paper_submit_requested"] is True
    assert result.report["strategy_paper_proof_invoked"] is True
    assert result.report["strategy_paper_proof_classification"] == "TRACK_B_PAPER_PROOF_PASSED"
    assert result.report["strategy_paper_proof_report_path"] == "proof_report.json"
    assert result.report["strategy_paper_final_flat"] is True
    assert result.report["strategy_paper_submit_allowed"] is True
    assert result.report["strategy_paper_submit_attempted"] is True
    assert result.report["strategy_paper_live_money_readiness"] is False
    assert result.report["latest_output_paths"]["track_b_strategy_paper_runner"] == "track_b_strategy_paper_runner_report.json"
    latest = json.loads((tmp_path / "operator_status" / "latest_operator_status_summary.json").read_text(encoding="utf-8"))
    assert latest["strategy_paper_runner_verdict"] == "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED"


def test_readiness_check_runner_report_is_summarized(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_readiness_check_runner_report_json=readiness_check_runner_report(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-readiness-check-runner",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.READY_FOR_PAPER_PROOF_REVIEW
    assert result.report["status_verdict"] == "OPERATOR_STATUS_READY_FOR_PAPER_PROOF_REVIEW"
    assert result.report["readiness_check_runner_verdict"] == "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW"
    assert result.report["readiness_check_runner_recovery_verdict"] == "RECOVERY_READY_CLEAN"
    assert result.report["readiness_check_runner_preflight_verdict"] == "READY_READ_ONLY"
    assert result.report["readiness_check_runner_databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert result.report["readiness_check_runner_current_quote_available"] is True
    assert result.report["readiness_check_runner_quote_provider_mode"] == "REALTIME"
    assert result.report["readiness_check_runner_realtime_quote_received"] is True
    assert result.report["readiness_check_runner_quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE"
    assert result.report["readiness_check_runner_wait_succeeded"] is True
    assert result.report["readiness_check_runner_readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert result.report["readiness_check_runner_required_next_action"].startswith("paper_proof_cli remains")
    assert result.report["readiness_check_runner_latest_report_path"].endswith("latest_track_b_readiness_check_runner_report.json")
    assert result.report["readiness_check_runner_submit_allowed"] is False
    assert result.report["readiness_check_runner_submit_attempted"] is False
    assert result.report["readiness_check_runner_paper_proof_cli_called"] is False
    assert result.report["readiness_check_runner_live_money_readiness"] is False
    assert result.report["latest_output_paths"]["track_b_readiness_check_runner"] == "track_b_readiness_check_runner_report.json"
    assert "track_b_readiness_check_runner" not in result.report["reports_missing"]
    assert "listener_health" in result.report["reports_missing"]
    assert result.report["primary_blocker"] is None
    assert result.report["required_next_action"].startswith("paper_proof_cli remains")
    assert result.report["submit_attempted"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["live_money_readiness"] is False
    latest = json.loads((tmp_path / "operator_status" / "latest_operator_status_summary.json").read_text(encoding="utf-8"))
    assert latest["status_verdict"] == "OPERATOR_STATUS_READY_FOR_PAPER_PROOF_REVIEW"
    assert latest["readiness_check_runner_verdict"] == "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW"


def test_blocked_readiness_check_runner_degrades_operator_status(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_readiness_check_runner_report_json=readiness_check_runner_report(
                tmp_path,
                runner_verdict="TRACK_B_READINESS_CHECK_BLOCKED_CURRENT_QUOTE",
                current_quote_available=False,
                wait_succeeded=False,
                primary_blocker="Current quote unavailable after bounded wait.",
                required_next_action="Wait for a current quote before paper proof review.",
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-readiness-check-blocked",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.BLOCKED_READINESS
    assert result.report["status_verdict"] == "OPERATOR_STATUS_BLOCKED_READINESS"
    assert result.report["readiness_check_runner_verdict"] == "TRACK_B_READINESS_CHECK_BLOCKED_CURRENT_QUOTE"
    assert result.report["primary_blocker"] == "Current quote unavailable after bounded wait."
    assert result.report["required_next_action"] == "Wait for a current quote before paper proof review."
    assert result.report["submit_allowed"] is False


def test_missing_readiness_check_runner_report_remains_explicit(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-missing-readiness-check-runner",
        now=aware_now(),
    )

    assert result.report["status_verdict"] == "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    assert result.report["readiness_check_runner_verdict"] == "NOT_PROVIDED"
    assert result.report["readiness_check_runner_current_quote_available"] == "NOT_PROVIDED"
    assert result.report["readiness_check_runner_realtime_quote_received"] == "NOT_PROVIDED"
    assert "track_b_readiness_check_runner" in result.report["reports_missing"]
    assert result.report["reports_considered"]["track_b_readiness_check_runner"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["paper_proof_cli_called"] is False


def test_missing_databento_observer_reports_are_explicit(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-missing-databento-observer",
        now=aware_now(),
    )

    assert result.report["databento_observer_verdict"] == "NOT_PROVIDED"
    assert result.report["databento_observer_mode"] == "NOT_PROVIDED"
    assert result.report["databento_observer_current_cycle"] == "NOT_PROVIDED"
    assert result.report["databento_output_event_path"] == "NOT_PROVIDED"
    assert "databento_candle_observer" in result.report["reports_missing"]
    assert "databento_candle_observer_heartbeat" in result.report["reports_missing"]
    assert result.report["reports_considered"]["databento_candle_observer"] is False
    assert result.report["reports_considered"]["databento_candle_observer_heartbeat"] is False


def test_missing_observation_runner_report_is_explicit(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-missing-observation-runner",
        now=aware_now(),
    )

    assert result.report["observation_runner_verdict"] == "NOT_PROVIDED"
    assert result.report["observation_runner_mode"] == "NOT_PROVIDED"
    assert result.report["observation_runner_current_cycle"] == "NOT_PROVIDED"
    assert result.report["observation_runner_latest_operator_status_path"] == "NOT_PROVIDED"
    assert "track_b_observation_runner" in result.report["reports_missing"]
    assert result.report["reports_considered"]["track_b_observation_runner"] is False


def test_operator_status_latest_pointer_updates_without_overwriting_canonical_reports(tmp_path: Path) -> None:
    first = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-first",
        now=datetime(2026, 5, 1, 21, 0, tzinfo=timezone.utc),
    )
    second = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-second",
        now=datetime(2026, 5, 1, 21, 1, tzinfo=timezone.utc),
    )

    latest_report = tmp_path / "operator_status" / "latest_operator_status_summary.json"
    latest_payload = json.loads(latest_report.read_text(encoding="utf-8"))
    assert latest_payload["operator_status_id"] == "status-second"
    assert latest_payload["report_json_path"] == str(second.report_json)
    assert first.report_json.exists()
    assert second.report_json.exists()
    assert first.report_json != latest_report
    assert second.report_json != latest_report


def test_missing_signal_batch_writer_report_is_explicit(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-missing-writer",
        now=aware_now(),
    )

    assert result.report["signal_batch_writer_verdict"] == "NOT_PROVIDED"
    assert result.report["signal_batch_writer_batch_json_path"] == "NOT_PROVIDED"
    assert result.report["recent_writer_output_present"] is False
    assert "signal_batch_writer" in result.report["reports_missing"]


def test_missing_upstream_reports_are_explicit(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_heartbeat_json=listener_heartbeat(tmp_path),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-missing-upstream",
        now=aware_now(),
    )

    assert result.report["strategy_adapter_verdict"] == "NOT_PROVIDED"
    assert result.report["strategy_rule_runner_verdict"] == "NOT_PROVIDED"
    assert result.report["candle_producer_verdict"] == "NOT_PROVIDED"
    assert result.report["strategy_rule_output_batch_path"] == "NOT_PROVIDED"
    assert result.report["strategy_output_batch_path"] == "NOT_PROVIDED"
    assert result.report["candle_output_batch_path"] == "NOT_PROVIDED"
    assert "track_b_strategy_rule_runner" in result.report["reports_missing"]
    assert "strategy_signal_adapter" in result.report["reports_missing"]
    assert "candle_signal_producer" in result.report["reports_missing"]
    assert result.report["reports_considered"]["track_b_strategy_rule_runner"] is False
    assert result.report["reports_considered"]["strategy_signal_adapter"] is False
    assert result.report["reports_considered"]["candle_signal_producer"] is False


def test_listener_degraded_produces_degraded_status(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(
                tmp_path,
                health_verdict="SHADOW_LISTENER_HEALTH_DEGRADED_FAILURES",
                files_failed=1,
                last_primary_blocker="One file failed.",
                last_required_next_action="Review failed event report.",
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-degraded",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.DEGRADED_SHADOW_FAILURES
    assert result.report["primary_blocker"] == "One file failed."
    assert result.report["submit_allowed"] is False


def test_recovery_unresolved_broker_state_is_distinct_blocker(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            recovery_report_json=recovery_report(
                tmp_path,
                classification="RECOVERY_BLOCKED_UNRESOLVED_ORDER",
                final_readiness_verdict="BLOCKED_UNRESOLVED_BROKER_ORDER",
                primary_blocker="unresolved broker order blocks same account/contract submit",
                required_next_action="Wait for terminal broker order state.",
                submit_allowed=False,
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-broker-block",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.BLOCKED_BROKER_STATE
    assert result.report["status_verdict"] == "OPERATOR_STATUS_BLOCKED_BROKER_STATE"
    assert result.report["recovery_verdict"] == "BLOCKED_UNRESOLVED_BROKER_ORDER"
    assert result.report["primary_blocker"] == "unresolved broker order blocks same account/contract submit"
    assert result.report["submit_allowed"] is False


def test_readiness_blocked_is_surfaced_distinctly(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=listener_health(tmp_path),
            readiness_summary_json=readiness_summary(
                tmp_path,
                final_readiness_verdict="BLOCKED_UNKNOWN_PROOF_TIMING",
                primary_blocker="Proof timing is unknown.",
                required_next_action="Provide active-session timing evidence.",
                submit_allowed=False,
            ),
            output_root=tmp_path / "operator_status",
        ),
        status_id="status-readiness-block",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.BLOCKED_READINESS
    assert result.report["status_verdict"] == "OPERATOR_STATUS_BLOCKED_READINESS"
    assert result.report["readiness_verdict"] == "BLOCKED_UNKNOWN_PROOF_TIMING"
    assert result.report["primary_blocker"] == "Proof timing is unknown."
    assert result.report["submit_attempted"] is False


def test_no_inputs_reports_missing_not_ok(tmp_path: Path) -> None:
    result = create_operator_status_summary(
        inputs=OperatorStatusInputs(output_root=tmp_path / "operator_status"),
        status_id="status-missing",
        now=aware_now(),
    )

    assert result.verdict == OperatorStatusVerdict.MISSING_REPORTS
    assert result.report["status_verdict"] == "OPERATOR_STATUS_MISSING_REPORTS"
    assert result.report["reports_missing"]
    assert result.report["primary_blocker"] == "No Track B observer reports were provided."
    assert result.report["submit_allowed"] is False
    assert result.report["paper_proof_cli_called"] is False


def test_operator_status_cli_reads_reports_and_writes_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    exit_code = operator_status_cli_main(
        [
            "--backend-health-json",
            str(backend_health_report(tmp_path)),
            "--listener-heartbeat-json",
            str(listener_heartbeat(tmp_path)),
            "--listener-health-json",
            str(listener_health(tmp_path)),
            "--track-b-observation-runner-report-json",
            str(observation_runner_report(tmp_path)),
            "--track-b-strategy-rule-runner-report-json",
            str(strategy_rule_runner_report(tmp_path)),
            "--track-b-strategy-paper-runner-report-json",
            str(strategy_paper_runner_report(tmp_path)),
            "--track-b-multi-strategy-runtime-cycle-report-json",
            str(multi_strategy_runtime_cycle_report(tmp_path)),
            "--databento-candle-observer-report-json",
            str(databento_candle_observer_report(tmp_path)),
            "--databento-candle-observer-heartbeat-json",
            str(databento_candle_observer_heartbeat(tmp_path)),
            "--signal-batch-writer-report-json",
            str(signal_batch_writer_report(tmp_path)),
            "--strategy-signal-adapter-report-json",
            str(strategy_signal_adapter_report(tmp_path)),
            "--candle-signal-producer-report-json",
            str(candle_signal_producer_report(tmp_path)),
            "--output-root",
            str(tmp_path / "operator_status_cli"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["status_verdict"] == "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    assert output["backend_health_status"] == "ok"
    assert output["backend_health_ready"] is True
    assert output["backend_health_url"] == "http://127.0.0.1:8790/"
    assert output["listener_mode"] == "watch"
    assert output["listener_current_cycle_number"] == 3
    assert output["observation_runner_verdict"] == "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW"
    assert output["observation_runner_mode"] == "watch"
    assert output["observation_runner_current_cycle"] == 2
    assert output["observation_runner_watch_exited_normally"] is True
    assert output["strategy_rule_runner_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"
    assert output["strategy_rule_id"] == "mgc_realtime_quote_demo_long_v1"
    assert output["strategy_rule_decision"] == "LONG"
    assert output["strategy_rule_signal_emitted"] is True
    assert output["strategy_rule_signal_direction"] == "LONG"
    assert output["strategy_paper_runner_verdict"] == "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED"
    assert output["strategy_paper_rule_decision"] == "LONG"
    assert output["strategy_paper_readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert output["strategy_paper_proof_invoked"] is True
    assert output["strategy_paper_proof_classification"] == "TRACK_B_PAPER_PROOF_PASSED"
    assert output["strategy_paper_final_flat"] is True
    assert output["multi_strategy_runtime_cycle_verdict"] == "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT"
    assert output["multi_strategy_chosen_strategy_id"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert output["multi_strategy_candidate_signals"][0]["signal_direction"] == "SHORT"
    assert output["multi_strategy_suppressed_signals"] == []
    assert output["multi_strategy_readiness_invoked"] is False
    assert output["multi_strategy_paper_proof_invoked"] is False
    assert output["multi_strategy_submit_attempted"] is False
    assert output["multi_strategy_broker_state_mutated"] is False
    assert output["databento_observer_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert output["databento_observer_mode"] == "watch"
    assert output["databento_observer_current_cycle"] == 3
    assert output["databento_observer_last_verdict"] == "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    assert output["strategy_adapter_verdict"] == "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    assert output["candle_producer_verdict"] == "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"
    assert output["signal_batch_writer_verdict"] == "SIGNAL_BATCH_WRITER_WROTE_BATCH"
    assert output["shadow_listener_health_verdict"] == "SHADOW_LISTENER_HEALTH_OK"
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
    assert Path(output["report_json"]).exists()
