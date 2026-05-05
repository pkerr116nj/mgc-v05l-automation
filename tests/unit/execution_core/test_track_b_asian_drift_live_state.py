from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_asian_drift_live_state import (
    TrackBAsianDriftLiveStateVerdict,
    produce_track_b_asian_drift_live_state,
)
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 1, 15, tzinfo=timezone.utc)


def feature_row(index: int, **overrides: object) -> dict[str, object]:
    ts = aware_now() + timedelta(minutes=5 * index)
    row: dict[str, object] = {
        "calibration_profile": "recovery_confirmed",
        "instrument": "MGC",
        "instrument_family": "MGC",
        "timeframe": "5m",
        "decision_ts": ts.isoformat(),
        "candle_timestamp": ts.isoformat(),
        "asia_drift_session_id": "MGC_2026-05-04_ASIA",
        "session_bar_index": index + 1,
        "in_scope": True,
        "entry_window_open": True,
        "session_timeout": False,
        "anchor_observed": True,
        "open": "4575.0",
        "high": "4575.8",
        "low": "4574.8",
        "close": "4575.4",
        "regime": "NO_TRADE",
        "dominant_direction": "NONE",
        "long_drift_strength": "NONE",
        "short_drift_strength": "NONE",
        "regime_persistence_score": "0.8",
        "pullback_state": "NO_PULLBACK",
        "pullback_structure_break": False,
        "pullback_warning_flag": False,
        "pullback_vwap_interaction": "NONE",
        "thesis_invalidated_flag": False,
        "recovery_score": "0.0",
        "hypothetical_entry_ready": False,
        "feature_version": "asia_drift_v1_phase1",
    }
    row.update(overrides)
    return row


def runtime_payload(rows: list[dict[str, object]], **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "asian_drift_feature_rows": rows,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def test_live_state_blocks_on_insufficient_5m_rows(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_live_state(
        runtime_payload=runtime_payload([feature_row(0)]),
        output_root=tmp_path / "asian_drift_state",
        producer_id="asian-drift-insufficient",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftLiveStateVerdict.BLOCKED_INSUFFICIENT_5M_CANDLES
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert result.report["asian_drift_state_ready"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    latest_builder_report = tmp_path / "asian_drift_state" / "latest_asian_drift_state_builder_report.json"
    assert latest_builder_report.exists()
    latest_payload = json.loads(latest_builder_report.read_text(encoding="utf-8"))
    assert (
        latest_payload["asian_drift_live_state_verdict"]
        == "TRACK_B_ASIAN_DRIFT_LIVE_STATE_BLOCKED_INSUFFICIENT_5M_CANDLES"
    )


def test_live_state_blocks_on_missing_realtime_quote_evidence(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_live_state(
        runtime_payload=runtime_payload([feature_row(index) for index in range(8)], realtime_quote_received=False),
        output_root=tmp_path / "asian_drift_state",
        producer_id="asian-drift-missing-quote",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftLiveStateVerdict.BLOCKED_MISSING_REALTIME_QUOTE
    assert "realtime_quote_received=true" in str(result.report["primary_blocker"])
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False


def test_live_state_emits_valid_no_signal_snapshot(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_live_state(
        runtime_payload=runtime_payload([feature_row(index) for index in range(8)]),
        output_root=tmp_path / "asian_drift_state",
        producer_id="asian-drift-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT
    assert result.snapshot_json is not None
    assert result.snapshot_json.exists()
    assert result.snapshot is not None
    assert result.snapshot["asia_drift_state"] == "NO_TRADE"
    assert result.snapshot["asia_drift_regime"] == "NO_TRADE"
    assert result.snapshot["asian_drift_state_ready"] is True
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_live_state_emits_valid_signal_snapshot_and_rule_consumes_it(tmp_path: Path) -> None:
    rows = [feature_row(index) for index in range(7)]
    rows.append(
        feature_row(
            7,
            regime="ASIA_DRIFT_LONG",
            dominant_direction="LONG",
            long_drift_strength="MEDIUM",
            pullback_state="NORMAL_PULLBACK",
            hypothetical_entry_ready=True,
        )
    )
    result = produce_track_b_asian_drift_live_state(
        runtime_payload=runtime_payload(rows),
        output_root=tmp_path / "asian_drift_state",
        producer_id="asian-drift-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT
    assert result.snapshot_json is not None
    snapshot = json.loads(result.snapshot_json.read_text(encoding="utf-8"))
    assert snapshot["asia_drift_state"] == "ENTRY_ARMED"
    assert snapshot["asia_drift_regime"] == "ASIA_DRIFT_LONG"

    rule_result = run_track_b_strategy_rule(
        input_event_payload=snapshot,
        input_event_path=result.snapshot_json,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_test_asian_drift_live_state",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="asian-drift-rule-consumes-live-state",
        now=aware_now(),
    )

    assert rule_result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert rule_result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    assert rule_result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert rule_result.report["real_strategy_signal"] is True
    assert rule_result.report["signal_direction"] == "LONG"
    assert rule_result.report["paper_proof_invoked"] is False
    assert rule_result.report["submit_attempted"] is False
    assert rule_result.report["broker_state_mutated"] is False


def test_live_state_blocks_raw_candles_without_asian_drift_features(tmp_path: Path) -> None:
    candles = [
        {
            "candle_timestamp": (aware_now() + timedelta(minutes=5 * index)).isoformat(),
            "open": "4575.0",
            "high": "4575.5",
            "low": "4574.5",
            "close": "4575.2",
        }
        for index in range(8)
    ]
    result = produce_track_b_asian_drift_live_state(
        runtime_payload=runtime_payload(candles),
        output_root=tmp_path / "asian_drift_state",
        producer_id="asian-drift-raw-candles",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftLiveStateVerdict.BLOCKED_MISSING_FEATURE_FIELDS
    assert "research feature fields" in str(result.report["primary_blocker"])
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
