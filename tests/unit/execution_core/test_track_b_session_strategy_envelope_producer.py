from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_multi_strategy_runtime_cycle import (
    TrackBMultiStrategyRuntimeCycleConfig,
    TrackBMultiStrategyRuntimeCycleVerdict,
    run_track_b_multi_strategy_runtime_cycle,
)
from mgc_v05l.execution_core.track_b_session_strategy_envelope_producer import (
    TrackBSessionStrategyEnvelopeProducerVerdict,
    produce_track_b_session_strategy_envelopes,
)
from mgc_v05l.execution_core.track_b_strategy_registry import validate_strategy_event_against_registry
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 11, 30, tzinfo=timezone.utc)


def runtime_5m_payload(*, bars: int = 9, completed: bool = True) -> dict[str, object]:
    start = datetime(2026, 5, 5, 9, 45, tzinfo=timezone.utc)
    candles: list[dict[str, object]] = []
    close = 4525.0
    for index in range(bars):
        ts = start + timedelta(minutes=5 * index)
        open_price = close
        close = close + (0.3 if index % 2 == 0 else -0.2)
        high = max(open_price, close) + 0.5
        low = min(open_price, close) - 0.5
        candles.append(
            {
                "candle_timestamp": ts.isoformat(),
                "observed_at": ts.isoformat(),
                "timeframe": "5m",
                "open": round(open_price, 2),
                "high": round(high, 2),
                "low": round(low, 2),
                "close": round(close, 2),
                "volume": 100 + index,
                "completed": completed,
            }
        )
    return {
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "local_symbol": "MGCM6",
        "dataset": "GLBX.MDP3",
        "timeframe": "5m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "CURRENT_QUOTE_FRESHNESS_ACCEPTED_STRICT_MAX_AGE",
        "source_id": "unit_test_runtime_5m",
        "generated_at": aware_now().isoformat(),
        "candles": candles,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def mnq_runtime_5m_payload(*, bars: int = 9, completed: bool = True) -> dict[str, object]:
    payload = runtime_5m_payload(bars=bars, completed=completed)
    payload.update(
        {
            "contract_key": "MNQ-202606",
            "instrument_family": "MNQ",
            "local_symbol": "MNQM6",
            "source_id": "unit_test_mnq_runtime_5m",
        }
    )
    return payload


def test_producer_blocks_when_insufficient_completed_5m_bars(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=3),
        runtime_5m_payload_path=tmp_path / "runtime_5m.json",
        output_root=tmp_path / "session",
        now=aware_now(),
    )

    assert result.verdict == TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INSUFFICIENT_5M_CANDLES
    assert result.london_late_pause_resume_short_event_json is None
    assert result.asia_late_flat_pullback_pause_resume_long_event_json is None
    assert result.asia_early_pause_resume_short_event_json is None
    assert result.asia_early_normal_breakout_retest_hold_long_event_json is None
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_producer_blocks_when_input_contains_incomplete_5m_bars(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=8, completed=False),
        output_root=tmp_path / "session",
        now=aware_now(),
    )

    assert result.verdict == TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INCOMPLETE_5M_CANDLE
    assert "incomplete" in str(result.report["primary_blocker"])


def test_producer_blocks_stale_runtime_5m_context_when_freshness_required(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
        max_completed_5m_age_seconds=900,
    )

    assert result.verdict == TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_STALE_RUNTIME_CONTEXT
    assert result.london_late_pause_resume_short_event_json is None
    assert result.asia_late_flat_pullback_pause_resume_long_event_json is None
    assert result.report["runtime_candle_context_stale"] is True
    assert result.report["latest_completed_5m_candle_age_seconds"] > 900
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_producer_emits_valid_session_strategy_envelopes(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
        producer_id="unit-producer",
    )

    assert result.verdict == TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES
    assert result.london_late_pause_resume_short_event_json == (
        tmp_path / "session" / "latest_london_late_pause_resume_short_event_envelope.json"
    )
    assert result.asia_late_flat_pullback_pause_resume_long_event_json == (
        tmp_path / "session" / "latest_asia_late_flat_pullback_pause_resume_long_event_envelope.json"
    )
    assert result.asia_early_pause_resume_short_event_json == (
        tmp_path / "session" / "latest_asia_early_pause_resume_short_event_envelope.json"
    )
    assert result.asia_early_normal_breakout_retest_hold_long_event_json == (
        tmp_path / "session" / "latest_asia_early_normal_breakout_retest_hold_long_event_envelope.json"
    )
    assert result.us_derivative_bear_turn_event_json == (
        tmp_path / "session" / "latest_us_derivative_bear_turn_event_envelope.json"
    )
    assert result.us_late_pause_resume_long_event_json == (
        tmp_path / "session" / "latest_us_late_pause_resume_long_event_envelope.json"
    )
    assert result.london_late_pause_resume_short_event is not None
    assert result.asia_late_flat_pullback_pause_resume_long_event is not None
    assert result.asia_early_pause_resume_short_event is not None
    assert result.asia_early_normal_breakout_retest_hold_long_event is not None
    assert result.us_derivative_bear_turn_event is not None
    assert result.us_late_pause_resume_long_event is not None
    assert "london_late_pause_resume_short_state" in result.london_late_pause_resume_short_event["metadata"]
    assert "asia_late_flat_pullback_pause_resume_long_features" in result.asia_late_flat_pullback_pause_resume_long_event["metadata"]
    assert "asia_early_pause_resume_short_state" in result.asia_early_pause_resume_short_event["metadata"]
    assert (
        "asia_early_normal_breakout_retest_hold_long_features"
        in result.asia_early_normal_breakout_retest_hold_long_event["metadata"]
    )
    assert "us_derivative_bear_turn_features" in result.us_derivative_bear_turn_event["metadata"]
    assert "us_late_pause_resume_long_state" in result.us_late_pause_resume_long_event["metadata"]
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_producer_emits_valid_mnq_us_derivative_bear_envelope(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=mnq_runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
        producer_id="unit-mnq-producer",
    )

    assert result.verdict == TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES
    assert result.mnq_us_derivative_bear_turn_event_json == (
        tmp_path / "session" / "latest_mnq_us_derivative_bear_turn_event_envelope.json"
    )
    assert result.mnq_us_derivative_bear_turn_event is not None
    event = result.mnq_us_derivative_bear_turn_event
    assert event["strategy_id"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert event["contract_key"] == "MNQ-202606"
    assert event["instrument_family"] == "MNQ"
    assert "mnq_us_derivative_bear_turn_state" in event["metadata"]
    assert "mnq_us_derivative_bear_turn_features" in event["metadata"]
    entry, blocker = validate_strategy_event_against_registry(
        event=event,
        rule_mode="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        rule_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        strategy_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
    )
    assert blocker is None
    assert entry is not None
    assert entry.instrument_family == "MNQ"
    assert entry.paper_eligible is True
    assert entry.live_money_eligible is False


def test_mnq_us_derivative_bear_rule_runner_consumes_envelope_without_mutation(tmp_path: Path) -> None:
    producer = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=mnq_runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert producer.mnq_us_derivative_bear_turn_event is not None

    result = run_track_b_strategy_rule(
        input_event_payload=producer.mnq_us_derivative_bear_turn_event,
        input_event_path=tmp_path / "mnq_us_derivative_bear.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        strategy_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        lane_id="mnq_us_derivative_bear_turn",
        rule_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        rule_mode="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
        emit_signal=False,
        output_root=tmp_path / "rule",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.NO_SIGNAL
    assert result.report["strategy_registry_id"] == "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    assert result.report["strategy_registry_instrument_family"] == "MNQ"
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_runtime_cycle_filters_to_mnq_derivative_bear_strategy(tmp_path: Path) -> None:
    producer = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=mnq_runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert producer.mnq_us_derivative_bear_turn_event_json is not None

    result = run_track_b_multi_strategy_runtime_cycle(
        config=TrackBMultiStrategyRuntimeCycleConfig(
            enabled_strategy_ids=("MNQ_US_DERIVATIVE_BEAR_TURN_V1",),
            mnq_us_derivative_bear_turn_event_json=producer.mnq_us_derivative_bear_turn_event_json,
            inbox_dir=tmp_path / "inbox",
            expected_account_id="DUM882026",
            source_id="unit_test_mnq_cycle",
            allow_fixture_input=False,
            submit_paper=False,
            confirm_paper_submit=False,
            output_root=tmp_path / "cycle",
            strategy_rule_output_root=tmp_path / "rule",
            decision_journal_enabled=True,
        ),
        now=aware_now(),
    )

    assert result.verdict == TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION
    assert [item["strategy_id"] for item in result.report["evaluated_strategies"]] == [
        "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    ]
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_produced_envelopes_satisfy_registered_schema(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert result.london_late_pause_resume_short_event is not None
    assert result.asia_late_flat_pullback_pause_resume_long_event is not None
    assert result.asia_early_pause_resume_short_event is not None
    assert result.asia_early_normal_breakout_retest_hold_long_event is not None
    assert result.us_derivative_bear_turn_event is not None
    assert result.us_late_pause_resume_long_event is not None

    london_entry, london_blocker = validate_strategy_event_against_registry(
        event=result.london_late_pause_resume_short_event,
        rule_mode="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        rule_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
    )
    asia_entry, asia_blocker = validate_strategy_event_against_registry(
        event=result.asia_late_flat_pullback_pause_resume_long_event,
        rule_mode="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        rule_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
    )
    asia_early_short_entry, asia_early_short_blocker = validate_strategy_event_against_registry(
        event=result.asia_early_pause_resume_short_event,
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
    )
    asia_early_long_entry, asia_early_long_blocker = validate_strategy_event_against_registry(
        event=result.asia_early_normal_breakout_retest_hold_long_event,
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
    )
    derivative_entry, derivative_blocker = validate_strategy_event_against_registry(
        event=result.us_derivative_bear_turn_event,
        rule_mode="US_DERIVATIVE_BEAR_TURN_V1",
        rule_id="US_DERIVATIVE_BEAR_TURN_V1",
        strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
    )
    us_late_entry, us_late_blocker = validate_strategy_event_against_registry(
        event=result.us_late_pause_resume_long_event,
        rule_mode="US_LATE_PAUSE_RESUME_LONG_V1",
        rule_id="US_LATE_PAUSE_RESUME_LONG_V1",
        strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
    )

    assert london_entry is not None
    assert asia_entry is not None
    assert asia_early_short_entry is not None
    assert asia_early_long_entry is not None
    assert derivative_entry is not None
    assert us_late_entry is not None
    assert london_blocker is None
    assert asia_blocker is None
    assert asia_early_short_blocker is None
    assert asia_early_long_blocker is None
    assert derivative_blocker is None
    assert us_late_blocker is None


def test_rule_runner_consumes_session_strategy_envelopes(tmp_path: Path) -> None:
    producer = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert producer.london_late_pause_resume_short_event is not None
    assert producer.asia_late_flat_pullback_pause_resume_long_event is not None
    assert producer.asia_early_pause_resume_short_event is not None
    assert producer.asia_early_normal_breakout_retest_hold_long_event is not None
    assert producer.us_derivative_bear_turn_event is not None
    assert producer.us_late_pause_resume_long_event is not None

    london = run_track_b_strategy_rule(
        input_event_payload=producer.london_late_pause_resume_short_event,
        input_event_path=tmp_path / "london.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy",
        strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_london_late_pause_resume_short",
        rule_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        rule_mode="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
    )
    asia = run_track_b_strategy_rule(
        input_event_payload=producer.asia_late_flat_pullback_pause_resume_long_event,
        input_event_path=tmp_path / "asia.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy",
        strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_asia_late_flat_pullback_pause_resume_long",
        rule_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        rule_mode="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
        strategy_adapter_output_root=tmp_path / "strategy_adapter",
        candle_producer_output_root=tmp_path / "candle_producer",
        writer_output_root=tmp_path / "signal_batch_writer",
    )
    asia_early_short = run_track_b_strategy_rule(
        input_event_payload=producer.asia_early_pause_resume_short_event,
        input_event_path=tmp_path / "asia_early_short.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy",
        strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        lane_id="mgc_asia_early_pause_resume_short",
        rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
        strategy_adapter_output_root=tmp_path / "strategy_adapter",
        candle_producer_output_root=tmp_path / "candle_producer",
        writer_output_root=tmp_path / "signal_batch_writer",
    )
    asia_early_long = run_track_b_strategy_rule(
        input_event_payload=producer.asia_early_normal_breakout_retest_hold_long_event,
        input_event_path=tmp_path / "asia_early_long.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy",
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
        rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
        strategy_adapter_output_root=tmp_path / "strategy_adapter",
        candle_producer_output_root=tmp_path / "candle_producer",
        writer_output_root=tmp_path / "signal_batch_writer",
    )
    derivative = run_track_b_strategy_rule(
        input_event_payload=producer.us_derivative_bear_turn_event,
        input_event_path=tmp_path / "us_derivative_bear.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy",
        strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
        lane_id="mgc_us_derivative_bear_turn",
        rule_id="US_DERIVATIVE_BEAR_TURN_V1",
        rule_mode="US_DERIVATIVE_BEAR_TURN_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
        strategy_adapter_output_root=tmp_path / "strategy_adapter",
        candle_producer_output_root=tmp_path / "candle_producer",
        writer_output_root=tmp_path / "signal_batch_writer",
    )
    us_late = run_track_b_strategy_rule(
        input_event_payload=producer.us_late_pause_resume_long_event,
        input_event_path=tmp_path / "us_late_long.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy",
        strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_us_late_pause_resume_long",
        rule_id="US_LATE_PAUSE_RESUME_LONG_V1",
        rule_mode="US_LATE_PAUSE_RESUME_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
        strategy_adapter_output_root=tmp_path / "strategy_adapter",
        candle_producer_output_root=tmp_path / "candle_producer",
        writer_output_root=tmp_path / "signal_batch_writer",
    )

    assert london.verdict in {TrackBStrategyRuleRunnerVerdict.NO_SIGNAL, TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL}
    assert asia.verdict in {TrackBStrategyRuleRunnerVerdict.NO_SIGNAL, TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL}
    assert asia_early_short.verdict in {
        TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
        TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL,
    }
    assert asia_early_long.verdict in {
        TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
        TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL,
    }
    assert derivative.verdict in {TrackBStrategyRuleRunnerVerdict.NO_SIGNAL, TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL}
    assert us_late.verdict in {TrackBStrategyRuleRunnerVerdict.NO_SIGNAL, TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL}
    assert london.report["london_late_pause_resume_short_watch_verdict"] != "LONDON_LATE_PAUSE_RESUME_SHORT_NOT_READY"
    assert asia.report["asia_late_flat_pullback_pause_resume_long_watch_verdict"] != (
        "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_NOT_READY"
    )
    assert asia_early_short.report["asia_early_pause_resume_short_watch_verdict"] != (
        "ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    )
    assert asia_early_long.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] != (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    )
    assert derivative.report["us_derivative_bear_turn_watch_verdict"] != "US_DERIVATIVE_BEAR_TURN_NOT_READY"
    assert us_late.report["us_late_pause_resume_long_watch_verdict"] != "US_LATE_PAUSE_RESUME_LONG_NOT_READY"
    assert london.report["paper_proof_cli_called"] is False
    assert asia.report["paper_proof_cli_called"] is False
    assert asia_early_short.report["paper_proof_cli_called"] is False
    assert asia_early_long.report["paper_proof_cli_called"] is False
    assert derivative.report["paper_proof_cli_called"] is False
    assert us_late.report["paper_proof_cli_called"] is False


def test_signal_fixture_without_paper_flags_reports_signal_ready_no_submit(tmp_path: Path) -> None:
    producer = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert producer.asia_late_flat_pullback_pause_resume_long_event is not None
    event = copy.deepcopy(producer.asia_late_flat_pullback_pause_resume_long_event)
    state = event["metadata"]["asia_late_flat_pullback_pause_resume_long_state"]
    features = event["metadata"]["asia_late_flat_pullback_pause_resume_long_features"]
    state.update({"derivative_phase": "ASIA_LATE", "session_asia": True, "allow_asia": True, "no_first_bull_snap_turn": True})
    event.update({"open": "4525.0", "close": "4526.0", "last": "4526.0"})
    features.update(
        {
            "open": "4525.0",
            "close": "4526.0",
            "previous_close": "4525.5",
            "bull_snap_close_strong": True,
            "one_bar_pullback_before_signal": True,
            "signal_breaks_prior_1_high": True,
            "pullback_range_expansion_ratio": "0.5",
            "signal_range_expansion_ratio": "1.0",
            "pullback_normalized_curvature": "0.02",
            "prior_bars_since_long_setup_gt_anti_churn": True,
        }
    )

    result = run_track_b_strategy_rule(
        input_event_payload=event,
        input_event_path=tmp_path / "asia_signal.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_session_strategy_signal",
        strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        lane_id="mgc_asia_late_flat_pullback_pause_resume_long",
        rule_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        rule_mode="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
        strategy_adapter_output_root=tmp_path / "strategy_adapter",
        candle_producer_output_root=tmp_path / "candle_producer",
        writer_output_root=tmp_path / "signal_batch_writer",
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.report["signal_direction"] == "LONG"
    assert result.report["real_strategy_signal"] is True
    assert result.report["submit_attempted"] is False


def test_runtime_cycle_consumes_session_strategy_envelopes(tmp_path: Path) -> None:
    producer = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert producer.london_late_pause_resume_short_event_json is not None
    assert producer.asia_late_flat_pullback_pause_resume_long_event_json is not None
    assert producer.asia_early_pause_resume_short_event_json is not None
    assert producer.asia_early_normal_breakout_retest_hold_long_event_json is not None
    assert producer.us_derivative_bear_turn_event_json is not None
    assert producer.us_late_pause_resume_long_event_json is not None

    result = run_track_b_multi_strategy_runtime_cycle(
        config=TrackBMultiStrategyRuntimeCycleConfig(
            pause_resume_short_event_json=producer.asia_early_pause_resume_short_event_json,
            breakout_retest_hold_long_event_json=producer.asia_early_normal_breakout_retest_hold_long_event_json,
            london_late_pause_resume_short_event_json=producer.london_late_pause_resume_short_event_json,
            asia_late_flat_pullback_pause_resume_long_event_json=producer.asia_late_flat_pullback_pause_resume_long_event_json,
            us_derivative_bear_turn_event_json=producer.us_derivative_bear_turn_event_json,
            us_late_pause_resume_long_event_json=producer.us_late_pause_resume_long_event_json,
            inbox_dir=tmp_path / "inbox",
            output_root=tmp_path / "cycle",
            strategy_rule_output_root=tmp_path / "rules",
            strategy_paper_runner_output_root=tmp_path / "paper",
        ),
        now=aware_now(),
        cycle_id="unit-cycle",
    )

    assert result.verdict in {
        TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION,
        TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT,
        TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED,
    }
    summaries = {item["strategy_id"]: item for item in result.report["evaluated_strategies"]}
    assert summaries["ASIA_EARLY_PAUSE_RESUME_SHORT_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert summaries["ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert summaries["LONDON_LATE_PAUSE_RESUME_SHORT_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert summaries["ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert summaries["US_DERIVATIVE_BEAR_TURN_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert summaries["US_LATE_PAUSE_RESUME_LONG_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_latest_session_envelope_files_are_written_as_json(tmp_path: Path) -> None:
    result = produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "session",
        now=aware_now(),
    )
    assert result.london_late_pause_resume_short_event_json is not None
    assert result.asia_early_pause_resume_short_event_json is not None
    assert result.asia_early_normal_breakout_retest_hold_long_event_json is not None
    assert result.us_derivative_bear_turn_event_json is not None
    assert result.us_late_pause_resume_long_event_json is not None
    payload = json.loads(result.london_late_pause_resume_short_event_json.read_text(encoding="utf-8"))
    early_short_payload = json.loads(result.asia_early_pause_resume_short_event_json.read_text(encoding="utf-8"))
    early_long_payload = json.loads(result.asia_early_normal_breakout_retest_hold_long_event_json.read_text(encoding="utf-8"))
    derivative_payload = json.loads(result.us_derivative_bear_turn_event_json.read_text(encoding="utf-8"))
    us_late_payload = json.loads(result.us_late_pause_resume_long_event_json.read_text(encoding="utf-8"))
    assert payload["strategy_id"] == "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
    assert early_short_payload["strategy_id"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert early_long_payload["strategy_id"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert derivative_payload["strategy_id"] == "US_DERIVATIVE_BEAR_TURN_V1"
    assert us_late_payload["strategy_id"] == "US_LATE_PAUSE_RESUME_LONG_V1"
    assert (
        payload["metadata"]["london_late_pause_resume_short_features"]["feature_version"]
        == "london_late_pause_resume_short_v1_phase1"
    )
    assert (
        early_short_payload["metadata"]["asia_early_pause_resume_short_features"]["feature_version"]
        == "asia_early_pause_resume_short_v1_phase1"
    )
    assert (
        early_long_payload["metadata"]["asia_early_normal_breakout_retest_hold_long_features"]["feature_version"]
        == "asia_early_normal_breakout_retest_hold_long_v1_phase1"
    )
    assert (
        derivative_payload["metadata"]["us_derivative_bear_turn_features"]["feature_version"]
        == "us_derivative_bear_turn_v1_phase1"
    )
    assert (
        us_late_payload["metadata"]["us_late_pause_resume_long_features"]["feature_version"]
        == "us_late_pause_resume_long_v1_phase1"
    )
