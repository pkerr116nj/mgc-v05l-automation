from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import mgc_v05l.execution_core.track_b_strategy_rule_runner as rule_runner_module
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)
from mgc_v05l.execution_core.track_b_strategy_rule_runner_cli import main as strategy_rule_runner_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 13, 40, tzinfo=timezone.utc)


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
        "quote_status": "CURRENT_QUOTE_AVAILABLE",
        "report_json_path": str(tmp_path / "current_quote_report.json"),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    path = tmp_path / "current_quote_report.json"
    write_json(path, payload)
    return path


def realtime_event(tmp_path: Path, **overrides: object) -> dict[str, object]:
    quote_path = quote_report(tmp_path)
    payload: dict[str, object] = {
        "account_id": "DUM882026",
        "batch_id": "strategy_rule_databento_batch_001",
        "candle_timestamp": aware_now().isoformat(),
        "close": "4575.3",
        "contract_key": "MGC-202606",
        "high": "4575.3",
        "instrument_family": "MGC",
        "lane_id": "mgc_example_long_lmt_day",
        "low": "4575.3",
        "observed_at": aware_now().isoformat(),
        "open": "4575.3",
        "reason": "fixture realtime Databento candle event",
        "signal_type": "databento_market_data_observation",
        "source_id": "unit_test_databento_observer",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "timeframe": "quote_snapshot",
        "volume": None,
        "metadata": {
            "databento_candle_observer_boundary": "databento_candle_observer",
            "market_data_provider": "DATABENTO",
            "market_data_role": "EVIDENCE_ONLY",
            "source_report_path": str(quote_path),
            "source_schema_version": "track_b_databento_current_quote_v1",
        },
    }
    payload.update(overrides)
    return payload


def ema_reclaim_event(tmp_path: Path, **overrides: object) -> dict[str, object]:
    event = realtime_event(tmp_path)
    metadata = dict(event["metadata"])
    metadata["ema_momentum_features"] = {
        "vwap": "4575.0",
        "prior_close": "4574.8",
        "momentum_norm": "0.18",
        "momentum_acceleration": "0.03",
        "momentum_turning_positive": True,
    }
    event["metadata"] = metadata
    event.update(overrides)
    return event


def asian_drift_event(tmp_path: Path, **overrides: object) -> dict[str, object]:
    event = realtime_event(
        tmp_path,
        strategy_id="asian_drift_v1",
        lane_id="mgc_example_long_lmt_day",
        timeframe="5m",
    )
    event.update(
        {
            "signal_family": "asian_drift_v1",
            "asia_drift_state": "NO_TRADE",
            "asia_drift_regime": "NO_TRADE",
            "direction": None,
            "entry_window_open": True,
            "in_scope": True,
            "session_timeout": False,
            "hypothetical_entry_ready": False,
            "feature_version": "asia_drift_v1_phase1",
            "calibration_profile": "recovery_confirmed",
            "close": "4575.3",
        }
    )
    event.update(overrides)
    return event


def asia_early_pause_resume_short_event(tmp_path: Path, **overrides: object) -> dict[str, object]:
    event = realtime_event(
        tmp_path,
        strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_asia_early_pause_resume_short",
        timeframe="5m",
        open="4576.0",
        high="4576.4",
        low="4574.7",
        close="4575.0",
    )
    metadata = dict(event["metadata"])
    metadata["asia_early_pause_resume_short_state"] = {
        "derivative_phase": "ASIA_EARLY",
        "session_asia": True,
        "allow_asia": True,
        "timeframe": "5m",
    }
    metadata["asia_early_pause_resume_short_features"] = {
        "feature_version": "asia_early_pause_resume_short_v1_phase1",
        "calibration_profile": "probationary_baseline_v1",
        "normalized_curvature": "-0.20",
        "max_normalized_curvature": "-0.15",
        "signal_range_expansion_ratio": "1.05",
        "max_range_expansion_ratio": "1.25",
        "setup_bar_curvature_is_flat": True,
        "one_bar_rebound_before_signal": True,
        "signal_breaks_prior_1_low": True,
        "close_below_fast_ema": True,
        "derivative_bear_close_weak": True,
        "derivative_bear_range_ok": True,
        "derivative_bear_body_ok": True,
        "derivative_bear_stretch_ok": True,
        "derivative_bear_cooldown_ok": True,
        "no_competing_bear_short_candidate": True,
        "previous_close": "4575.8",
        "open": "4576.0",
        "close": "4575.0",
    }
    event["metadata"] = metadata
    event.update(overrides)
    return event


def asia_early_normal_breakout_retest_hold_long_event(tmp_path: Path, **overrides: object) -> dict[str, object]:
    event = realtime_event(
        tmp_path,
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
        timeframe="5m",
        open="4575.0",
        high="4576.8",
        low="4575.0",
        close="4576.2",
    )
    metadata = dict(event["metadata"])
    metadata["asia_early_normal_breakout_retest_hold_long_state"] = {
        "derivative_phase": "ASIA_EARLY",
        "session_asia": True,
        "allow_asia": True,
        "asia_early_or_gc_mgc_london_open": True,
        "no_first_bull_snap_turn": True,
        "prior_bars_since_long_setup_gt_anti_churn": True,
        "timeframe": "5m",
    }
    metadata["asia_early_normal_breakout_retest_hold_long_features"] = {
        "feature_version": "asia_early_normal_breakout_retest_hold_long_v1_phase1",
        "calibration_profile": "probationary_baseline_v1",
        "breakout_bar_slope_is_flat": True,
        "breakout_bar_expansion_is_normal": True,
        "breakout_breaks_prior_1_high": True,
        "signal_retests_and_holds_breakout_level": True,
        "breakout_normalized_slope": "0.05",
        "breakout_abs_slope_max": "0.20",
        "breakout_range_expansion_ratio": "1.00",
        "breakout_min_range_expansion_ratio": "0.85",
        "breakout_max_range_expansion_ratio": "1.25",
    }
    event["metadata"] = metadata
    event.update(overrides)
    return event


def test_valid_realtime_quote_demo_long_emit_writes_no_submit_signal_batch(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(tmp_path),
        input_event_path=tmp_path / "latest_databento_candle_event.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_strategy_rule",
        rule_id="mgc_realtime_quote_demo_long_v1",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-valid",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.output_batch_json is not None
    assert result.output_batch_json.exists()
    batch = json.loads(result.output_batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["signal_direction"] == "LONG"
    assert signal["decision_style"] == "BINARY"
    assert signal["metadata"]["input_metadata"]["track_b_strategy_rule_runner_boundary"] == "track_b_strategy_rule_runner"
    assert result.report["decision"] == "LONG"
    assert result.report["signal_emitted"] is True
    assert result.report["input_quote_provider_mode"] == "REALTIME"
    assert result.report["realtime_quote_received"] is True
    assert result.report["current_quote_available"] is True
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.downstream_strategy_adapter_report_json is not None
    assert result.downstream_candle_producer_report_json is not None
    assert result.downstream_signal_batch_writer_report_json is not None
    latest = json.loads((tmp_path / "rule_reports" / "latest_track_b_strategy_rule_runner_report.json").read_text(encoding="utf-8"))
    assert latest["track_b_strategy_rule_runner_id"] == "rule-runner-valid"


def test_valid_realtime_quote_without_emit_flag_stays_no_signal(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(tmp_path),
        input_event_path=tmp_path / "latest_databento_candle_event.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=False,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-no-emit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert result.report["decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert result.output_batch_json is None
    assert not list((tmp_path / "inbox").glob("*.json"))
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_mgc_ema_momentum_reclaim_long_emits_no_submit_signal_when_conditions_pass(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=ema_reclaim_event(tmp_path),
        input_event_path=tmp_path / "latest_databento_candle_event.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_ema_rule",
        rule_id="mgc_ema_momentum_reclaim_long_v1",
        rule_mode="MGC_EMA_MOMENTUM_RECLAIM_LONG",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-ema-pass",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.report["rule_name"] == "mgc_ema_momentum_reclaim_long"
    assert result.report["decision"] == "LONG"
    assert result.report["signal_emitted"] is True
    assert result.report["signal_direction"] == "LONG"
    assert result.report["rule_inputs"]["close"] == "4575.3"
    assert result.report["rule_conditions"]["close_reclaimed_vwap"] is True
    assert result.report["rule_conditions"]["prior_close_below_vwap"] is True
    assert result.report["rule_conditions"]["momentum_turning_positive"] is True
    assert result.report["rule_blockers"] == []
    assert "research/ema_momentum.py" in result.report["research_lineage"]
    assert result.output_batch_json is not None
    batch = json.loads(result.output_batch_json.read_text(encoding="utf-8"))
    signal = batch["signal_items"][0]["signal"]
    assert signal["signal_direction"] == "LONG"
    assert signal["decision_style"] == "BINARY"
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_mgc_ema_momentum_reclaim_long_no_signal_when_conditions_fail(tmp_path: Path) -> None:
    event = ema_reclaim_event(tmp_path)
    event["close"] = "4574.9"

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="mgc_ema_momentum_reclaim_long_v1",
        rule_mode="MGC_EMA_MOMENTUM_RECLAIM_LONG",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-ema-fail",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert result.report["decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert result.report["rule_conditions"]["close_reclaimed_vwap"] is False
    assert "close_reclaimed_vwap" in result.report["decision_reason"]
    assert result.output_batch_json is None
    assert not list((tmp_path / "inbox").glob("*.json"))
    assert result.report["submit_attempted"] is False


def test_mgc_ema_momentum_reclaim_long_missing_features_blocks_not_ready(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(tmp_path),
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="mgc_ema_momentum_reclaim_long_v1",
        rule_mode="MGC_EMA_MOMENTUM_RECLAIM_LONG",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-ema-missing",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT
    assert result.report["signal_emitted"] is False
    assert "NOT_READY" in result.report["decision_reason"]
    assert "metadata.ema_momentum_features" in result.report["decision_reason"]
    assert result.report["strategy_registry_verdict"] == "TRACK_B_STRATEGY_REGISTRY_NOT_READY"
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_unregistered_strategy_rule_is_rejected(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(tmp_path, strategy_id="unregistered_strategy"),
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="unregistered_rule",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-unregistered",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT
    assert result.report["strategy_registry_id"] == "NOT_REGISTERED"
    assert "not registered" in str(result.report["primary_blocker"])
    assert result.report["signal_emitted"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_v1_no_signal_on_non_setup_snapshot(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=asian_drift_event(tmp_path),
        input_event_path=tmp_path / "asian_drift_state_snapshot.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_asian_drift",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-asian-drift-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    assert result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_v1_emits_signal_on_explicit_entry_armed_snapshot(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=asian_drift_event(
            tmp_path,
            asia_drift_state="ENTRY_ARMED",
            asia_drift_regime="ASIA_DRIFT_LONG",
            direction="LONG",
            hypothetical_entry_ready=True,
        ),
        input_event_path=tmp_path / "asian_drift_state_snapshot.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_asian_drift",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-asian-drift-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    assert result.report["rule_name"] == "asian_drift_v1_state_snapshot"
    assert result.report["decision"] == "LONG"
    assert result.report["signal_emitted"] is True
    assert result.report["signal_direction"] == "LONG"
    assert result.report["real_strategy_signal"] is True
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.output_batch_json is not None
    batch = json.loads(result.output_batch_json.read_text(encoding="utf-8"))
    assert batch["signal_items"][0]["signal"]["signal_direction"] == "LONG"


def test_asian_drift_v1_missing_snapshot_fields_not_ready_for_tonight(tmp_path: Path) -> None:
    event = realtime_event(tmp_path, strategy_id="asian_drift_v1", timeframe="5m")

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-asian-drift-not-ready",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert "NOT_READY" in str(result.report["primary_blocker"])
    assert "asia_drift_state" in str(result.report["primary_blocker"])
    assert result.report["signal_emitted"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_asia_early_pause_resume_short_missing_required_features_not_ready(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(
            tmp_path,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            lane_id="mgc_asia_early_pause_resume_short",
            timeframe="5m",
        ),
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-pause-resume-missing",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT
    assert result.report["asia_early_pause_resume_short_watch_verdict"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    assert "NOT_READY" in str(result.report["primary_blocker"])
    assert "asia_early_pause_resume_short_features.normalized_curvature" in str(result.report["primary_blocker"])
    assert result.report["signal_emitted"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asia_early_pause_resume_short_no_signal_no_mutation(tmp_path: Path) -> None:
    event = asia_early_pause_resume_short_event(tmp_path)
    metadata = dict(event["metadata"])
    features = dict(metadata["asia_early_pause_resume_short_features"])
    features["signal_breaks_prior_1_low"] = False
    metadata["asia_early_pause_resume_short_features"] = features
    event["metadata"] = metadata

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=tmp_path / "pause_resume_short_state.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_pause_resume_short",
        rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-pause-resume-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert result.report["strategy_registry_id"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["asia_early_pause_resume_short_watch_verdict"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION"
    assert result.report["signal_source"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    assert result.output_batch_json is None


def test_asia_early_pause_resume_short_signal_ready_no_submit(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=asia_early_pause_resume_short_event(tmp_path),
        input_event_path=tmp_path / "pause_resume_short_state.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_pause_resume_short",
        rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-pause-resume-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.report["rule_name"] == "asia_early_pause_resume_short_v1"
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["asia_early_pause_resume_short_watch_verdict"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT"
    assert result.report["signal_source"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["decision"] == "SHORT"
    assert result.report["signal_emitted"] is True
    assert result.report["signal_direction"] == "SHORT"
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    assert result.output_batch_json is not None
    batch = json.loads(result.output_batch_json.read_text(encoding="utf-8"))
    assert batch["signal_items"][0]["signal"]["signal_direction"] == "SHORT"


def test_asia_early_normal_breakout_retest_hold_long_missing_required_features_not_ready(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(
            tmp_path,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
            timeframe="5m",
        ),
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-breakout-retest-missing",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT
    assert result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] == (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    )
    assert "NOT_READY" in str(result.report["primary_blocker"])
    assert "asia_early_normal_breakout_retest_hold_long_features.breakout_bar_slope_is_flat" in str(result.report["primary_blocker"])
    assert result.report["signal_emitted"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asia_early_normal_breakout_retest_hold_long_no_signal_no_mutation(tmp_path: Path) -> None:
    event = asia_early_normal_breakout_retest_hold_long_event(tmp_path)
    metadata = dict(event["metadata"])
    features = dict(metadata["asia_early_normal_breakout_retest_hold_long_features"])
    features["signal_retests_and_holds_breakout_level"] = False
    metadata["asia_early_normal_breakout_retest_hold_long_features"] = features
    event["metadata"] = metadata

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=tmp_path / "breakout_retest_hold_long_state.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_breakout_retest_hold_long",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-breakout-retest-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert result.report["strategy_registry_id"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] == (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION"
    )
    assert result.report["signal_source"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    assert result.output_batch_json is None


def test_asia_early_normal_breakout_retest_hold_long_signal_ready_no_submit(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=asia_early_normal_breakout_retest_hold_long_event(tmp_path),
        input_event_path=tmp_path / "breakout_retest_hold_long_state.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_breakout_retest_hold_long",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-breakout-retest-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.report["rule_name"] == "asia_early_normal_breakout_retest_hold_long_v1"
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] == (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT"
    )
    assert result.report["signal_source"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["decision"] == "LONG"
    assert result.report["signal_emitted"] is True
    assert result.report["signal_direction"] == "LONG"
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    assert result.output_batch_json is not None
    batch = json.loads(result.output_batch_json.read_text(encoding="utf-8"))
    assert batch["signal_items"][0]["signal"]["signal_direction"] == "LONG"


def test_human_review_only_mode_does_not_emit_signal(tmp_path: Path) -> None:
    result = run_track_b_strategy_rule(
        input_event_payload=realtime_event(tmp_path),
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_mode="HUMAN_REVIEW_ONLY",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-human-review",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL
    assert result.report["decision"] == "HUMAN_REVIEW"
    assert result.report["signal_emitted"] is False
    assert result.output_batch_json is None
    assert result.report["paper_proof_cli_called"] is False


def test_missing_or_invalid_quote_blocks_without_signal(tmp_path: Path) -> None:
    event = realtime_event(tmp_path)
    event["metadata"] = {"source_report_path": str(tmp_path / "missing_quote_report.json")}

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        runner_id="rule-runner-missing-quote",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT
    assert result.report["signal_emitted"] is False
    assert result.report["primary_blocker"]
    assert result.output_batch_json is None
    assert not list((tmp_path / "inbox").glob("*.json"))
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_historical_or_fixture_input_cannot_pretend_to_be_realtime(tmp_path: Path) -> None:
    event = realtime_event(tmp_path)
    quote_path = quote_report(
        tmp_path / "historical",
        quote_provider_mode="HISTORICAL_AVAILABLE_END",
        market_data_mode="HISTORICAL_AVAILABLE_END",
        realtime_quote_received=False,
        current_quote_available=False,
    )
    event["metadata"] = {"source_report_path": str(quote_path)}

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=None,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-historical",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT
    assert "REALTIME" in str(result.report["primary_blocker"])
    assert result.report["signal_emitted"] is False
    assert result.report["paper_proof_cli_called"] is False
    assert result.report["submit_attempted"] is False


def test_fixture_input_requires_explicit_allow_fixture_flag(tmp_path: Path) -> None:
    event = realtime_event(tmp_path)
    event["metadata"] = {"fixture": True}

    blocked = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=None,
        inbox_dir=tmp_path / "blocked_inbox",
        expected_account_id="DUM882026",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=True,
        output_root=tmp_path / "blocked_reports",
        runner_id="rule-runner-fixture-blocked",
        now=aware_now(),
    )
    allowed = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=None,
        inbox_dir=tmp_path / "allowed_inbox",
        expected_account_id="DUM882026",
        rule_mode="DEMO_LONG_ONLY",
        emit_signal=True,
        allow_fixture_input=True,
        output_root=tmp_path / "allowed_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="rule-runner-fixture-allowed",
        now=aware_now(),
    )

    assert blocked.verdict == TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT
    assert allowed.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert allowed.report["signal_emitted"] is True
    assert allowed.report["submit_allowed"] is False


def test_cli_emits_demo_long_signal_from_realtime_event(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    event_json = tmp_path / "latest_databento_candle_event.json"
    write_json(event_json, realtime_event(tmp_path))

    exit_code = strategy_rule_runner_cli_main(
        [
            "--input-event-json",
            str(event_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--expected-account-id",
            "DUM882026",
            "--source-id",
            "cli_strategy_rule_demo",
            "--rule-id",
            "mgc_realtime_quote_demo_long_v1",
            "--rule-mode",
            "DEMO_LONG_ONLY",
            "--emit-signal",
            "--output-root",
            str(tmp_path / "rule_reports"),
            "--strategy-adapter-output-root",
            str(tmp_path / "adapter_reports"),
            "--candle-producer-output-root",
            str(tmp_path / "candle_reports"),
            "--writer-output-root",
            str(tmp_path / "writer_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["strategy_rule_runner_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"
    assert output["decision"] == "LONG"
    assert output["signal_emitted"] is True
    assert Path(output["output_batch_path"]).exists()
    assert output["paper_proof_cli_called"] is False
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False


def test_cli_default_rule_evaluates_real_mgc_rule_without_emitting(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    event_json = tmp_path / "latest_databento_candle_event.json"
    write_json(event_json, ema_reclaim_event(tmp_path))

    exit_code = strategy_rule_runner_cli_main(
        [
            "--input-event-json",
            str(event_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--expected-account-id",
            "DUM882026",
            "--source-id",
            "cli_strategy_rule_default",
            "--output-root",
            str(tmp_path / "rule_reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["strategy_rule_id"] == "mgc_ema_momentum_reclaim_long_v1"
    assert output["rule_name"] == "mgc_ema_momentum_reclaim_long"
    assert output["rule_mode"] == "MGC_EMA_MOMENTUM_RECLAIM_LONG"
    assert output["strategy_rule_runner_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_NO_SIGNAL"
    assert output["decision"] == "NO_SIGNAL"
    assert output["signal_emitted"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False


def test_strategy_rule_runner_does_not_define_broker_or_proof_calls() -> None:
    source = inspect.getsource(rule_runner_module)
    assert "paper_proof_cli.main" not in source
    assert "paper_proof_cli import" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "ibapi" not in source
