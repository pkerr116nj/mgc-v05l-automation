from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_multi_strategy_runtime_cycle import (
    TrackBMultiStrategyRuntimeCycleConfig,
    TrackBMultiStrategyRuntimeCycleVerdict,
    run_track_b_multi_strategy_runtime_cycle,
)
from mgc_v05l.execution_core.track_b_snap_turn_envelope_producer import (
    TrackBSnapTurnEnvelopeProducerVerdict,
    produce_track_b_snap_turn_envelopes,
)
from mgc_v05l.execution_core.track_b_strategy_registry import validate_strategy_event_against_registry
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 5, 7, 30, tzinfo=timezone.utc)


def runtime_5m_payload(*, bars: int = 9, completed: bool = True) -> dict[str, object]:
    start = datetime(2026, 5, 5, 6, 45, tzinfo=timezone.utc)
    candles: list[dict[str, object]] = []
    close = 4525.0
    for index in range(bars):
        ts = start + timedelta(minutes=5 * index)
        open_price = close
        close = close + (0.4 if index % 2 == 0 else -0.2)
        high = max(open_price, close) + 0.6
        low = min(open_price, close) - 0.6
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


def test_producer_blocks_when_insufficient_completed_5m_bars(tmp_path: Path) -> None:
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=3),
        runtime_5m_payload_path=tmp_path / "runtime_5m.json",
        output_root=tmp_path / "snap",
        now=aware_now(),
    )

    assert result.verdict == TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INSUFFICIENT_5M_CANDLES
    assert result.first_bull_snap_turn_event_json is None
    assert result.first_bear_snap_turn_event_json is None
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_producer_blocks_when_input_contains_incomplete_5m_bars(tmp_path: Path) -> None:
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=8, completed=False),
        runtime_5m_payload_path=tmp_path / "runtime_5m.json",
        output_root=tmp_path / "snap",
        now=aware_now(),
    )

    assert result.verdict == TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INCOMPLETE_5M_CANDLE
    assert "incomplete" in str(result.report["primary_blocker"])


def test_producer_blocks_stale_runtime_5m_context_when_freshness_required(tmp_path: Path) -> None:
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        runtime_5m_payload_path=tmp_path / "runtime_5m.json",
        output_root=tmp_path / "snap",
        now=datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc),
        max_completed_5m_age_seconds=900,
    )

    assert result.verdict == TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_STALE_RUNTIME_CONTEXT
    assert result.first_bull_snap_turn_event_json is None
    assert result.first_bear_snap_turn_event_json is None
    assert result.report["runtime_candle_context_stale"] is True
    assert result.report["latest_completed_5m_candle_age_seconds"] > 900
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_producer_emits_valid_bull_and_bear_envelopes(tmp_path: Path) -> None:
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        runtime_5m_payload_path=tmp_path / "runtime_5m.json",
        output_root=tmp_path / "snap",
        now=aware_now(),
        producer_id="unit-producer",
    )

    assert result.verdict == TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES
    assert result.first_bull_snap_turn_event_json == tmp_path / "snap" / "latest_first_bull_snap_turn_event_envelope.json"
    assert result.first_bear_snap_turn_event_json == tmp_path / "snap" / "latest_first_bear_snap_turn_event_envelope.json"
    assert result.first_bull_snap_turn_event is not None
    assert result.first_bear_snap_turn_event is not None
    bull_metadata = result.first_bull_snap_turn_event["metadata"]
    bear_metadata = result.first_bear_snap_turn_event["metadata"]
    assert "first_bull_snap_turn_state" in bull_metadata
    assert "first_bull_snap_turn_features" in bull_metadata
    assert "first_bear_snap_turn_state" in bear_metadata
    assert "first_bear_snap_turn_features" in bear_metadata
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_produced_envelopes_satisfy_registered_schema(tmp_path: Path) -> None:
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "snap",
        now=aware_now(),
    )
    assert result.first_bull_snap_turn_event is not None
    assert result.first_bear_snap_turn_event is not None

    bull_entry, bull_blocker = validate_strategy_event_against_registry(
        event=result.first_bull_snap_turn_event,
        rule_mode="FIRST_BULL_SNAP_TURN_V1",
        rule_id="FIRST_BULL_SNAP_TURN_V1",
        strategy_id="FIRST_BULL_SNAP_TURN_V1",
    )
    bear_entry, bear_blocker = validate_strategy_event_against_registry(
        event=result.first_bear_snap_turn_event,
        rule_mode="FIRST_BEAR_SNAP_TURN_V1",
        rule_id="FIRST_BEAR_SNAP_TURN_V1",
        strategy_id="FIRST_BEAR_SNAP_TURN_V1",
    )

    assert bull_entry is not None
    assert bear_entry is not None
    assert bull_blocker is None
    assert bear_blocker is None


def test_rule_runner_consumes_produced_snap_turn_envelopes(tmp_path: Path) -> None:
    producer = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "snap",
        now=aware_now(),
    )
    assert producer.first_bull_snap_turn_event is not None
    assert producer.first_bear_snap_turn_event is not None

    bull = run_track_b_strategy_rule(
        input_event_payload=producer.first_bull_snap_turn_event,
        input_event_path=tmp_path / "bull.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_snap_turn",
        strategy_id="FIRST_BULL_SNAP_TURN_V1",
        lane_id="mgc_first_bull_snap_turn",
        rule_id="FIRST_BULL_SNAP_TURN_V1",
        rule_mode="FIRST_BULL_SNAP_TURN_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
    )
    bear = run_track_b_strategy_rule(
        input_event_payload=producer.first_bear_snap_turn_event,
        input_event_path=tmp_path / "bear.json",
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="unit_snap_turn",
        strategy_id="FIRST_BEAR_SNAP_TURN_V1",
        lane_id="mgc_first_bear_snap_turn",
        rule_id="FIRST_BEAR_SNAP_TURN_V1",
        rule_mode="FIRST_BEAR_SNAP_TURN_V1",
        emit_signal=True,
        output_root=tmp_path / "rules",
    )

    assert bull.verdict in {TrackBStrategyRuleRunnerVerdict.NO_SIGNAL, TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL}
    assert bear.verdict in {TrackBStrategyRuleRunnerVerdict.NO_SIGNAL, TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL}
    assert bull.report["first_bull_snap_turn_watch_verdict"] != "FIRST_BULL_SNAP_TURN_NOT_READY"
    assert bear.report["first_bear_snap_turn_watch_verdict"] != "FIRST_BEAR_SNAP_TURN_NOT_READY"
    assert bull.report["paper_proof_cli_called"] is False
    assert bear.report["paper_proof_cli_called"] is False


def test_runtime_cycle_consumes_produced_snap_turn_envelopes(tmp_path: Path) -> None:
    producer = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "snap",
        now=aware_now(),
    )
    assert producer.first_bull_snap_turn_event_json is not None
    assert producer.first_bear_snap_turn_event_json is not None

    result = run_track_b_multi_strategy_runtime_cycle(
        config=TrackBMultiStrategyRuntimeCycleConfig(
            first_bull_snap_turn_event_json=producer.first_bull_snap_turn_event_json,
            first_bear_snap_turn_event_json=producer.first_bear_snap_turn_event_json,
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
    assert summaries["FIRST_BULL_SNAP_TURN_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert summaries["FIRST_BEAR_SNAP_TURN_V1"]["strategy_runtime_verdict"] != "NOT_READY"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_latest_envelope_files_are_written_as_json(tmp_path: Path) -> None:
    result = produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=runtime_5m_payload(bars=9),
        output_root=tmp_path / "snap",
        now=aware_now(),
    )
    assert result.first_bull_snap_turn_event_json is not None
    payload = json.loads(result.first_bull_snap_turn_event_json.read_text(encoding="utf-8"))
    assert payload["strategy_id"] == "FIRST_BULL_SNAP_TURN_V1"
    assert payload["metadata"]["first_bull_snap_turn_features"]["feature_version"] == "first_bull_snap_turn_v1_phase1"
