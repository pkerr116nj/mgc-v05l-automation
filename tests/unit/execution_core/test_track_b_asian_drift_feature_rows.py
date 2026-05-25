from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from mgc_v05l.execution_core.track_b_asian_drift_feature_rows import (
    TrackBAsianDriftFeatureRowsVerdict,
    produce_track_b_asian_drift_feature_rows,
)
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


def candle_payload(
    closes: list[float],
    *,
    completed: bool = True,
    start: datetime | None = None,
) -> dict[str, object]:
    start = start or datetime(2026, 5, 4, 18, 5, tzinfo=ZoneInfo("America/New_York"))
    candles: list[dict[str, object]] = []
    previous_close = closes[0] if closes else 4575.0
    for index, close in enumerate(closes):
        ts = start + timedelta(minutes=5 * index)
        open_price = previous_close if index else close
        high = max(open_price, close) + 0.4
        low = min(open_price, close) - 0.4
        candles.append(
            {
                "candle_timestamp": ts.isoformat(),
                "timeframe": "5m",
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": 20,
                "completed": completed,
            }
        )
        previous_close = close
    return {
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "local_symbol": "MGCM6",
        "dataset": "GLBX.MDP3",
        "timeframe": "5m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "candles": candles,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def flat_closes(count: int) -> list[float]:
    return [4575.0 + (0.05 if index % 2 == 0 else -0.05) for index in range(count)]


def long_signal_closes() -> list[float]:
    return [4575.0, 4576.2, 4577.5, 4578.8, 4580.0, 4581.3, 4582.4, 4583.0, 4583.5, 4582.9]


def late_join_strong_drift_closes() -> list[float]:
    return [4562.9, 4565.0, 4564.6, 4569.9, 4571.2, 4575.3, 4577.4, 4578.7, 4576.2, 4580.3]


def test_feature_rows_block_with_zero_completed_rows(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload([]),
        output_root=tmp_path / "asian_drift_state",
        producer_id="zero-rows",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftFeatureRowsVerdict.BLOCKED_NO_5M_CANDLES
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert result.report["feature_rows_ready"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_feature_rows_block_with_fewer_than_eight_completed_rows(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(flat_closes(7)),
        output_root=tmp_path / "asian_drift_state",
        producer_id="seven-rows",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftFeatureRowsVerdict.BLOCKED_INSUFFICIENT_COMPLETED_5M_CANDLES
    assert result.feature_rows_payload is not None
    assert result.feature_rows_payload["rows_available"] == 7
    assert result.report["live_state_invoked"] is False
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert result.report["submit_attempted"] is False


def test_feature_rows_block_on_incomplete_5m_bar(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(flat_closes(8), completed=False),
        output_root=tmp_path / "asian_drift_state",
        producer_id="incomplete-rows",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftFeatureRowsVerdict.BLOCKED_INCOMPLETE_5M_CANDLE
    assert result.feature_rows_payload is None
    assert result.report["submit_attempted"] is False


def test_feature_rows_emit_with_eight_completed_rows_and_feed_live_state(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(flat_closes(8)),
        output_root=tmp_path / "asian_drift_state",
        producer_id="eight-rows",
        now=aware_now(),
    )

    assert result.verdict == TrackBAsianDriftFeatureRowsVerdict.WROTE_ROWS
    assert result.feature_rows_payload is not None
    assert result.feature_rows_json is not None
    assert result.feature_rows_json.exists()
    latest_rows = tmp_path / "asian_drift_state" / "latest_asian_drift_5m_feature_rows.json"
    assert latest_rows.exists()
    row_payload = json.loads(latest_rows.read_text(encoding="utf-8"))
    latest_row = row_payload["asian_drift_feature_rows"][-1]
    assert latest_row["calibration_profile"] == "recovery_confirmed"
    assert latest_row["timeframe"] == "5m"
    assert latest_row["feature_version"] == "asia_drift_v1_phase1:recovery_confirmed"
    assert latest_row["regime"] == "NO_TRADE"
    assert result.live_state_result is not None
    assert result.live_state_result.verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False


def test_produced_feature_rows_are_accepted_by_live_state(tmp_path: Path) -> None:
    result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(flat_closes(8)),
        output_root=tmp_path / "feature_rows",
        producer_id="accepted-rows",
        invoke_live_state=False,
        now=aware_now(),
    )
    assert result.feature_rows_payload is not None

    live = produce_track_b_asian_drift_live_state(
        runtime_payload=result.feature_rows_payload,
        output_root=tmp_path / "live_state",
        producer_id="accepted-live-state",
        now=aware_now(),
    )

    assert live.verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT
    assert live.snapshot is not None
    assert live.snapshot["asia_drift_state"] == "NO_TRADE"
    assert live.snapshot["submit_attempted"] is False


def test_valid_no_signal_fixture_reaches_asian_drift_no_signal_no_mutation(tmp_path: Path) -> None:
    feature_result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(flat_closes(8)),
        output_root=tmp_path / "asian_drift_state",
        producer_id="no-signal-rows",
        now=aware_now(),
    )
    assert feature_result.live_state_result is not None
    assert feature_result.live_state_result.snapshot is not None
    assert feature_result.live_state_result.snapshot_json is not None

    rule_result = run_track_b_strategy_rule(
        input_event_payload=feature_result.live_state_result.snapshot,
        input_event_path=feature_result.live_state_result.snapshot_json,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="asian_drift_feature_rows_no_signal",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="asian-drift-no-signal",
        now=aware_now(),
    )

    assert rule_result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert rule_result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    assert rule_result.report["signal_emitted"] is False
    assert rule_result.report["readiness_invoked"] is False
    assert rule_result.report["paper_proof_invoked"] is False
    assert rule_result.report["submit_attempted"] is False
    assert rule_result.report["broker_state_mutated"] is False
    assert rule_result.report["live_money_readiness"] is False


def test_valid_signal_fixture_reaches_asian_drift_signal_ready_no_submit(tmp_path: Path) -> None:
    feature_result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(long_signal_closes()),
        output_root=tmp_path / "asian_drift_state",
        producer_id="signal-rows",
        now=aware_now(),
    )
    assert feature_result.live_state_result is not None
    assert feature_result.live_state_result.snapshot is not None
    assert feature_result.live_state_result.snapshot_json is not None
    assert feature_result.live_state_result.snapshot["asia_drift_state"] == "ENTRY_ARMED"

    rule_result = run_track_b_strategy_rule(
        input_event_payload=feature_result.live_state_result.snapshot,
        input_event_path=feature_result.live_state_result.snapshot_json,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="asian_drift_feature_rows_signal",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="asian-drift-signal",
        now=aware_now(),
    )

    assert rule_result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert rule_result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    assert rule_result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert rule_result.report["real_strategy_signal"] is True
    assert rule_result.report["signal_direction"] == "LONG"
    assert rule_result.report["readiness_invoked"] is False
    assert rule_result.report["paper_proof_invoked"] is False
    assert rule_result.report["submit_attempted"] is False
    assert rule_result.report["broker_state_mutated"] is False
    assert rule_result.report["live_money_readiness"] is False


def test_strong_late_join_drift_missing_anchor_is_diagnostic_only(tmp_path: Path) -> None:
    feature_result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(
            late_join_strong_drift_closes(),
            start=datetime(2026, 5, 4, 18, 45, tzinfo=ZoneInfo("America/New_York")),
        ),
        output_root=tmp_path / "asian_drift_state",
        producer_id="late-join-missing-anchor",
        now=aware_now(),
    )

    assert feature_result.live_state_result is not None
    assert feature_result.live_state_result.snapshot is not None
    snapshot = feature_result.live_state_result.snapshot
    assert feature_result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_BLOCKED_MISSING_SESSION_ANCHOR_CONTEXT"
    assert feature_result.report["late_join_diagnostic"] is True
    assert snapshot["asian_drift_diagnostic_classification"] == "ASIAN_DRIFT_BLOCKED_MISSING_SESSION_ANCHOR_CONTEXT"
    assert snapshot["late_join_classification"] == "ASIAN_DRIFT_LATE_JOIN_STRONG_DRIFT_OBSERVED"
    assert snapshot["anchor_required"] is True
    assert snapshot["anchor_observed"] is False
    assert snapshot["late_join_policy"] == "DIAGNOSTIC_ONLY"
    assert snapshot["hypothetical_entry_ready"] is False
    assert snapshot["submit_allowed"] is False
    assert snapshot["submit_attempted"] is False
    assert snapshot["no_mutation"] is True
    assert "18:00 ET session anchor context is missing" in snapshot["operator_explanation"]


def test_weak_late_join_missing_anchor_remains_plain_no_signal(tmp_path: Path) -> None:
    feature_result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(
            flat_closes(10),
            start=datetime(2026, 5, 4, 18, 45, tzinfo=ZoneInfo("America/New_York")),
        ),
        output_root=tmp_path / "asian_drift_state",
        producer_id="late-join-weak-missing-anchor",
        now=aware_now(),
    )

    assert feature_result.live_state_result is not None
    assert feature_result.live_state_result.snapshot is not None
    assert feature_result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    assert feature_result.report["late_join_diagnostic"] is False
    assert feature_result.live_state_result.snapshot.get("asian_drift_diagnostic_classification") is None


def test_late_join_diagnostic_rule_runner_preserves_no_submit_authority(tmp_path: Path) -> None:
    feature_result = produce_track_b_asian_drift_feature_rows(
        runtime_5m_payload=candle_payload(
            late_join_strong_drift_closes(),
            start=datetime(2026, 5, 4, 18, 45, tzinfo=ZoneInfo("America/New_York")),
        ),
        output_root=tmp_path / "asian_drift_state",
        producer_id="late-join-rule",
        now=aware_now(),
    )
    assert feature_result.live_state_result is not None
    assert feature_result.live_state_result.snapshot is not None
    assert feature_result.live_state_result.snapshot_json is not None

    rule_result = run_track_b_strategy_rule(
        input_event_payload=feature_result.live_state_result.snapshot,
        input_event_path=feature_result.live_state_result.snapshot_json,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="asian_drift_late_join_rule",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_reports",
        strategy_adapter_output_root=tmp_path / "adapter_reports",
        candle_producer_output_root=tmp_path / "candle_reports",
        writer_output_root=tmp_path / "writer_reports",
        runner_id="asian-drift-late-join-rule",
        now=aware_now(),
    )

    assert rule_result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert rule_result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_BLOCKED_MISSING_SESSION_ANCHOR_CONTEXT"
    assert rule_result.report["late_join_diagnostic"] is True
    assert rule_result.report["signal_emitted"] is False
    assert rule_result.report["submit_allowed"] is False
    assert rule_result.report["submit_attempted"] is False
    assert rule_result.report["broker_state_mutated"] is False
    assert "18:00 ET session anchor context is missing" in rule_result.report["decision_reason"]
