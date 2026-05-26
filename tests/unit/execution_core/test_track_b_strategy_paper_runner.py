from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.models import TerminalClassification
from mgc_v05l.execution_core.paper_proof import PaperProofConfig, PaperProofResult
import mgc_v05l.execution_core.track_b_strategy_paper_runner as paper_runner_module
from mgc_v05l.execution_core.track_b_readiness_check_runner import (
    TrackBReadinessCheckRunnerResult,
    TrackBReadinessCheckRunnerVerdict,
)
from mgc_v05l.execution_core.track_b_feature_builder import (
    TrackBFeatureBuilderResult,
    TrackBFeatureBuilderVerdict,
)
from mgc_v05l.execution_core.track_b_market_history import (
    TrackBMarketHistoryResult,
    TrackBMarketHistoryVerdict,
)
from mgc_v05l.execution_core.track_b_mgc_candle_history_producer import (
    TrackBMgcCandleHistoryProducerResult,
    TrackBMgcCandleHistoryProducerVerdict,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner import (
    LEGACY_PAPER_PROOF_DISABLED_REASON,
    TrackBStrategyPaperRunnerConfig,
    TrackBStrategyPaperRunnerStages,
    TrackBStrategyPaperRunnerVerdict,
    _continuation_exit_evidence_from_strategy_report,
    run_track_b_strategy_paper,
)
from mgc_v05l.execution_core.track_b_strategy_managed_paper_lifecycle import (
    TrackBManagedPaperLifecycleClassification,
    TrackBStrategyManagedPaperLifecycleResult,
)
from mgc_v05l.execution_core.track_b_strategy_trade_intent import (
    TrackBStrategyTradeIntentConfig,
    TrackBStrategyTradeIntentResult,
    create_track_b_strategy_trade_intent,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner_cli import main as strategy_paper_runner_cli_main
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerResult,
    TrackBStrategyRuleRunnerVerdict,
    run_track_b_strategy_rule,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 14, 30, tzinfo=timezone.utc)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def asian_drift_entry_fixture(name: str) -> Path:
    return repo_root() / "examples" / "track_b_signal_amplification" / name


class Calls:
    def __init__(self) -> None:
        self.candle_history = 0
        self.market_history = 0
        self.feature = 0
        self.strategy = 0
        self.readiness = 0
        self.intent = 0
        self.managed_lifecycle = 0
        self.proof = 0
        self.operator_status = 0


def base_config(tmp_path: Path, **overrides: object) -> TrackBStrategyPaperRunnerConfig:
    payload = {
        "mode": "PAPER",
        "input_event_payload": {
            "account_id": "DUM882026",
            "contract_key": "MGC-202606",
            "strategy_id": "track_b_example_gold_shadow_v1",
            "lane_id": "mgc_example_long_lmt_day",
            "candle_timestamp": aware_now().isoformat(),
            "observed_at": aware_now().isoformat(),
            "close": "4575.3",
            "metadata": {"fixture": True},
        },
        "inbox_dir": tmp_path / "inbox",
        "strategy_id": "mgc_ema_momentum_reclaim_long_v1",
        "allow_fixture_input": True,
        "output_root": tmp_path / "paper_runner",
        "strategy_rule_output_root": tmp_path / "rule_runner",
        "strategy_adapter_output_root": tmp_path / "adapter",
        "candle_producer_output_root": tmp_path / "candle",
        "writer_output_root": tmp_path / "writer",
        "readiness_output_root": tmp_path / "readiness",
        "paper_proof_output_root": tmp_path / "proof",
        "strategy_trade_intent_output_root": tmp_path / "intents",
        "operator_status_output_root": tmp_path / "operator_status",
    }
    payload.update(overrides)
    return TrackBStrategyPaperRunnerConfig(**payload)


def _write_strategy_input_event_with_runtime_candles(
    tmp_path: Path,
    *,
    strategy_id: str,
    symbol: str,
) -> Path:
    runtime_payload_path = tmp_path / "runtime" / f"{strategy_id}_runtime_5m.json"
    runtime_payload_path.parent.mkdir(parents=True, exist_ok=True)
    runtime_payload_path.write_text(
        json.dumps(
            {
                "instrument_family": symbol,
                "timeframe": "5m",
                "candles": [
                    {
                        "timestamp": "2026-05-04T14:15:00+00:00",
                        "timeframe": "5m",
                        "open": "100.0",
                        "high": "101.0",
                        "low": "99.8",
                        "close": "100.7",
                        "completed": True,
                    },
                    {
                        "timestamp": "2026-05-04T14:20:00+00:00",
                        "timeframe": "5m",
                        "open": "100.7",
                        "high": "101.6",
                        "low": "100.5",
                        "close": "101.4",
                        "completed": True,
                    },
                    {
                        "timestamp": "2026-05-04T14:25:00+00:00",
                        "timeframe": "5m",
                        "open": "101.4",
                        "high": "102.2",
                        "low": "101.1",
                        "close": "102.0",
                        "completed": True,
                    },
                ],
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    input_event_path = tmp_path / "events" / f"{strategy_id}.json"
    input_event_path.parent.mkdir(parents=True, exist_ok=True)
    input_event_path.write_text(
        json.dumps(
            {
                "strategy_id": strategy_id,
                "instrument_family": symbol,
                "metadata": {
                    "source_payload_path": str(runtime_payload_path),
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return input_event_path


def assert_legacy_paper_proof_disabled(result, calls: Calls) -> None:
    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert calls.proof == 0
    assert result.report["primary_blocker"] == LEGACY_PAPER_PROOF_DISABLED_REASON
    assert result.report["required_next_action"] == "Use the strategy-managed lifecycle path; paper_proof is not a runtime submit boundary."
    assert result.report["paper_submit_requested"] is True
    assert result.report["paper_proof_invoked"] is False
    assert result.report["paper_proof_classification"] is None
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_envelope_auto_populates_continuation_exit_evidence(tmp_path: Path) -> None:
    evidence = _continuation_exit_evidence_from_strategy_report(
        config=base_config(tmp_path, strategy_id="asian_drift_v1"),
        strategy_report={
            "report_json_path": str(tmp_path / "rule" / "asian_drift.json"),
            "completed_5m_candles": [
                {"open": "100.0", "high": "100.4", "low": "99.9", "close": "100.3"},
                {"open": "100.3", "high": "100.8", "low": "100.2", "close": "100.7"},
            ],
            "position_age_minutes": 65,
            "mfe": "1.4",
            "mae": "-0.2",
            "unrealized_pnl": "1.0",
            "rule_inputs": {
                "asia_drift_state": "ENTRY_ARMED",
                "asia_drift_regime": "ASIA_DRIFT_LONG",
                "direction": "LONG",
            },
            "rule_conditions": {"entry_window_open": True, "in_scope": True},
            "safe_state_classification": "SAFE_STATE_NORMAL",
            "reconciliation_classification": "TRACK_B_PAPER_BROKER_RECONCILED",
        },
    )

    assert evidence["continuation_exit_profile_id"] == "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1"
    assert len(evidence["continuation_completed_5m_candles"]) == 2
    assert evidence["continuation_position_age_minutes"] == 65
    assert evidence["continuation_mfe"] == "1.4"
    assert evidence["continuation_mae"] == "-0.2"
    assert evidence["continuation_unrealized_pnl"] == "1.0"
    assert evidence["continuation_microtrend_state"]["microtrend"] == "ALIGNED_LOW_VOL_DRIFT"
    assert evidence["continuation_participation_state"]["participation"] == "STRONG_PARTICIPATING"
    assert evidence["continuation_lifecycle_reconciliation_classification"] == "TRACK_B_PAPER_BROKER_RECONCILED"
    assert evidence["continuation_source_strategy_report_path"].endswith("asian_drift.json")


def test_pause_resume_short_envelope_auto_populates_continuation_exit_evidence(tmp_path: Path) -> None:
    evidence = _continuation_exit_evidence_from_strategy_report(
        config=base_config(tmp_path, strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1"),
        strategy_report={
            "strategy_rule_report_json": str(tmp_path / "rule" / "pause_resume_short.json"),
            "input_candle_window": [
                {"open": "100.0", "high": "100.1", "low": "99.4", "close": "99.5"},
                {"open": "99.5", "high": "99.6", "low": "98.9", "close": "99.0"},
            ],
            "open_position_age_minutes": 18,
            "open_position_mfe": "1.0",
            "open_position_mae": "-0.3",
            "open_position_unrealized_pnl": "0.4",
            "rule_inputs": {"derivative_phase": "RESUME_SHORT"},
            "rule_conditions": {"breakdown_confirmed": True, "failed_resume": False},
        },
    )

    assert evidence["continuation_exit_profile_id"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1"
    assert len(evidence["continuation_completed_5m_candles"]) == 2
    assert evidence["continuation_position_age_minutes"] == 18
    assert evidence["continuation_mfe"] == "1.0"
    assert evidence["continuation_mae"] == "-0.3"
    assert evidence["continuation_unrealized_pnl"] == "0.4"
    assert evidence["continuation_microtrend_state"]["strategy_family"] == "pause_resume"
    assert evidence["continuation_participation_state"]["failed_continuation_sensitive"] is True
    assert evidence["continuation_safe_state_classification"] == "SAFE_STATE_NORMAL"


def test_remaining_p0_envelopes_have_profile_mapping_without_exit_behavior_change(tmp_path: Path) -> None:
    cases = {
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": (
            "BREAKOUT_RETEST_CONTINUATION_HOLD_V1",
            "breakout_retest",
        ),
        "MNQ_FIRST_BEAR_SNAP_TURN_V1": ("SNAP_TURN_FAST_DECAY_V1", "snap_turn"),
        "MNQ_FIRST_BULL_SNAP_TURN_V1": ("SNAP_TURN_FAST_DECAY_V1", "snap_turn"),
    }

    for strategy_id, (expected_profile, expected_family) in cases.items():
        evidence = _continuation_exit_evidence_from_strategy_report(
            config=base_config(tmp_path, strategy_id=strategy_id),
            strategy_report={},
        )

        assert evidence["continuation_exit_profile_id"] == expected_profile
        assert evidence["continuation_completed_5m_candles"] == ()
        assert evidence["continuation_microtrend_state"]["strategy_family"] == expected_family
        assert evidence["continuation_microtrend_state"]["runtime_behavior_changed"] is False


def test_breakout_retest_long_envelope_auto_populates_continuation_exit_evidence(tmp_path: Path) -> None:
    input_event_path = _write_strategy_input_event_with_runtime_candles(
        tmp_path,
        strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        symbol="MGC",
    )

    evidence = _continuation_exit_evidence_from_strategy_report(
        config=base_config(tmp_path, strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"),
        strategy_report={
            "report_json_path": str(tmp_path / "rule" / "breakout_retest_long.json"),
            "input_event_path": str(input_event_path),
            "position_age_minutes": 45,
            "mfe": "1.8",
            "mae": "-0.4",
            "unrealized_pnl": "1.1",
            "rule_inputs": {
                "derivative_phase": "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD",
                "breakout_normalized_slope": "0.08",
                "breakout_range_expansion_ratio": "1.05",
            },
            "rule_conditions": {
                "breakout_breaks_prior_1_high": True,
                "signal_retests_and_holds_breakout_level": True,
                "breakout_bar_expansion_is_normal": True,
            },
        },
    )

    assert evidence["continuation_exit_profile_id"] == "BREAKOUT_RETEST_CONTINUATION_HOLD_V1"
    assert len(evidence["continuation_completed_5m_candles"]) == 3
    assert evidence["continuation_position_age_minutes"] == 45
    assert evidence["continuation_mfe"] == "1.8"
    assert evidence["continuation_mae"] == "-0.4"
    assert evidence["continuation_unrealized_pnl"] == "1.1"
    assert evidence["continuation_microtrend_state"]["strategy_family"] == "breakout_retest"
    assert evidence["continuation_microtrend_state"]["microtrend"] == "HEALTHY_BREAKOUT_RETEST_CONTINUATION"
    assert evidence["continuation_participation_state"]["healthy_pullback_tolerant"] is True


def test_mnq_bear_snap_turn_envelope_auto_populates_continuation_exit_evidence(tmp_path: Path) -> None:
    input_event_path = _write_strategy_input_event_with_runtime_candles(
        tmp_path,
        strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        symbol="MNQ",
    )

    evidence = _continuation_exit_evidence_from_strategy_report(
        config=base_config(tmp_path, strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1"),
        strategy_report={
            "strategy_rule_report_json": str(tmp_path / "rule" / "mnq_bear_snap.json"),
            "input_event_path": str(input_event_path),
            "open_position_age_minutes": 12,
            "open_position_mfe": "4.0",
            "open_position_mae": "-0.8",
            "open_position_unrealized_pnl": "2.1",
            "rule_inputs": {"derivative_phase": "FIRST_BEAR_SNAP_TURN", "session_allowed": True},
            "rule_conditions": {
                "session_allowed": True,
                "bear_snap_close_weak": True,
                "bear_snap_reversal_bar": True,
                "first_bear_snap_turn": True,
            },
        },
    )

    assert evidence["continuation_exit_profile_id"] == "SNAP_TURN_FAST_DECAY_V1"
    assert len(evidence["continuation_completed_5m_candles"]) == 3
    assert evidence["continuation_position_age_minutes"] == 12
    assert evidence["continuation_mfe"] == "4.0"
    assert evidence["continuation_mae"] == "-0.8"
    assert evidence["continuation_unrealized_pnl"] == "2.1"
    assert evidence["continuation_microtrend_state"]["direction"] == "SHORT"
    assert evidence["continuation_microtrend_state"]["microtrend"] == "FAST_SNAP_TURN_CONTINUATION"
    assert evidence["continuation_participation_state"]["fast_decay_sensitive"] is True


def test_mnq_bull_snap_turn_envelope_auto_populates_continuation_exit_evidence(tmp_path: Path) -> None:
    input_event_path = _write_strategy_input_event_with_runtime_candles(
        tmp_path,
        strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
        symbol="MNQ",
    )

    evidence = _continuation_exit_evidence_from_strategy_report(
        config=base_config(tmp_path, strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1"),
        strategy_report={
            "source_strategy_report_path": str(tmp_path / "rule" / "mnq_bull_snap.json"),
            "input_event_path": str(input_event_path),
            "managed_position_age_minutes": 14,
            "max_favorable_excursion": "3.2",
            "max_adverse_excursion": "-0.5",
            "unrealized_pnl": "1.7",
            "rule_inputs": {"derivative_phase": "FIRST_BULL_SNAP_TURN", "session_allowed": True},
            "rule_conditions": {
                "session_allowed": True,
                "bull_snap_close_strong": True,
                "bull_snap_reversal_bar": True,
                "first_bull_snap_turn": True,
            },
        },
    )

    assert evidence["continuation_exit_profile_id"] == "SNAP_TURN_FAST_DECAY_V1"
    assert len(evidence["continuation_completed_5m_candles"]) == 3
    assert evidence["continuation_position_age_minutes"] == 14
    assert evidence["continuation_mfe"] == "3.2"
    assert evidence["continuation_mae"] == "-0.5"
    assert evidence["continuation_unrealized_pnl"] == "1.7"
    assert evidence["continuation_microtrend_state"]["direction"] == "LONG"
    assert evidence["continuation_microtrend_state"]["microtrend"] == "FAST_SNAP_TURN_CONTINUATION"
    assert evidence["continuation_participation_state"]["fast_decay_sensitive"] is True


def test_remaining_p0_missing_evidence_falls_back_without_behavior_change(tmp_path: Path) -> None:
    for strategy_id in (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "MNQ_FIRST_BEAR_SNAP_TURN_V1",
        "MNQ_FIRST_BULL_SNAP_TURN_V1",
    ):
        evidence = _continuation_exit_evidence_from_strategy_report(
            config=base_config(tmp_path, strategy_id=strategy_id),
            strategy_report={"rule_conditions": {}},
        )

        assert evidence["continuation_completed_5m_candles"] == ()
        assert evidence["continuation_microtrend_state"]["runtime_behavior_changed"] is False


def test_managed_lifecycle_receives_auto_populated_continuation_evidence(tmp_path: Path, monkeypatch) -> None:
    captured = {}

    def fake_managed_lifecycle(*, config):
        captured["config"] = config
        return TrackBStrategyManagedPaperLifecycleResult(
            lifecycle_id="captured",
            classification=TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
            report_json=tmp_path / "captured.json",
            report={"continuation_aware_exit_preview": None},
        )

    monkeypatch.setattr(paper_runner_module, "run_track_b_strategy_managed_paper_lifecycle", fake_managed_lifecycle)

    paper_runner_module._run_managed_lifecycle(
        base_config(
            tmp_path,
            strategy_id="asian_drift_v1",
            managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        ),
        {
            "strategy_registry_instrument_family": "MGC",
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
            "completed_5m_candles": [
                {"open": "100.0", "high": "100.5", "low": "99.9", "close": "100.4"},
                {"open": "100.4", "high": "100.9", "low": "100.3", "close": "100.8"},
            ],
            "position_age_minutes": 70,
            "mfe": "1.6",
            "mae": "-0.2",
            "unrealized_pnl": "1.1",
            "rule_inputs": {
                "asia_drift_state": "ENTRY_ARMED",
                "asia_drift_regime": "ASIA_DRIFT_LONG",
                "direction": "LONG",
            },
            "rule_conditions": {"entry_window_open": True, "in_scope": True},
        },
    )

    managed_config = captured["config"]
    assert managed_config.continuation_exit_profile_id == "ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1"
    assert len(managed_config.continuation_completed_5m_candles) == 2
    assert managed_config.continuation_position_age_minutes == 70
    assert managed_config.continuation_mfe == "1.6"
    assert managed_config.continuation_mae == "-0.2"
    assert managed_config.continuation_unrealized_pnl == "1.1"
    assert managed_config.continuation_microtrend_state["microtrend"] == "ALIGNED_LOW_VOL_DRIFT"
    assert managed_config.continuation_participation_state["participation"] == "STRONG_PARTICIPATING"


def candle_history_producer_result(tmp_path: Path, *, ready: bool = True) -> TrackBMgcCandleHistoryProducerResult:
    report_json = tmp_path / "candle_history_producer_report.json"
    input_json = tmp_path / "candle_history_input.json"
    history_input = {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "databento_continuous_symbol": "MGC.v.0",
        "dataset": "GLBX.MDP3",
        "timeframe": "1m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "candles": [
            {
                "candle_timestamp": "2026-05-04T14:28:00+00:00",
                "open": "4574.6",
                "high": "4574.6",
                "low": "4574.6",
                "close": "4574.6",
                "volume": "1",
            },
            {
                "candle_timestamp": "2026-05-04T14:29:00+00:00",
                "open": "4574.8",
                "high": "4574.8",
                "low": "4574.8",
                "close": "4574.8",
                "volume": "1",
            },
            {
                "candle_timestamp": "2026-05-04T14:30:00+00:00",
                "open": "4575.3",
                "high": "4575.3",
                "low": "4575.3",
                "close": "4575.3",
                "volume": "1",
            },
        ],
    }
    report = {
        "candle_history_producer_verdict": (
            TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT.value
            if ready
            else TrackBMgcCandleHistoryProducerVerdict.BLOCKED_INSUFFICIENT_CANDLES.value
        ),
        "output_history_input_path": str(input_json) if ready else None,
        "primary_blocker": None if ready else "At least 3 candles are required; received 1.",
        "required_next_action": "history next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    if ready:
        input_json.write_text(json.dumps(history_input), encoding="utf-8")
    return TrackBMgcCandleHistoryProducerResult(
        verdict=TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT
        if ready
        else TrackBMgcCandleHistoryProducerVerdict.BLOCKED_INSUFFICIENT_CANDLES,
        report_json=report_json,
        report=report,
        history_input_json=input_json if ready else None,
        history_input=history_input if ready else None,
    )


def market_history_result(tmp_path: Path, *, ready: bool = True) -> TrackBMarketHistoryResult:
    report_json = tmp_path / "market_history_report.json"
    event_json = tmp_path / "market_history_event.json"
    history_event = {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "1m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "candles": [],
    }
    report = {
        "market_history_verdict": (
            TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT.value
            if ready
            else TrackBMarketHistoryVerdict.BLOCKED_INSUFFICIENT_HISTORY.value
        ),
        "output_history_event_path": str(event_json) if ready else None,
        "primary_blocker": None if ready else "At least 3 candles are required; received 1.",
        "required_next_action": "collector next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    if ready:
        event_json.write_text(json.dumps(history_event), encoding="utf-8")
    return TrackBMarketHistoryResult(
        verdict=TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT if ready else TrackBMarketHistoryVerdict.BLOCKED_INSUFFICIENT_HISTORY,
        report_json=report_json,
        report=report,
        history_event_json=event_json if ready else None,
        history_event=history_event if ready else None,
    )


def maintained_history_payload(*, ready: bool = True, latest_timestamp: str | None = None) -> dict[str, object]:
    latest = latest_timestamp or aware_now().isoformat()
    return {
        "schema_version": "track_b_latest_good_mgc_1m_history_v1",
        "source_id": "track_b_data_maintenance",
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "1m",
        "history_provider_mode": "DATABENTO_MAINTAINED_LOCAL_1M",
        "history_ready": ready,
        "history_freshness_seconds": 0,
        "gap_count": 0,
        "primary_blocker": None if ready else "Maintained MGC 1m history is stale.",
        "candles": [
            {
                "candle_timestamp": "2026-05-04T14:28:00+00:00",
                "open": "4574.6",
                "high": "4574.6",
                "low": "4574.6",
                "close": "4574.6",
                "volume": "1",
            },
            {
                "candle_timestamp": "2026-05-04T14:29:00+00:00",
                "open": "4574.8",
                "high": "4574.8",
                "low": "4574.8",
                "close": "4574.8",
                "volume": "1",
            },
            {
                "candle_timestamp": latest,
                "open": "4575.3",
                "high": "4575.3",
                "low": "4575.3",
                "close": "4575.3",
                "volume": "1",
            },
        ],
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def runtime_candle_context_payload() -> dict[str, object]:
    payload = maintained_history_payload()
    payload.update(
        {
            "schema_version": "track_b_runtime_mgc_1m_candles_v1",
            "source_id": "track_b_runtime_candle_capture",
            "candle_source_mode": "SUPPLIED_RUNTIME_CANDLES",
            "runtime_candle_context_ready": True,
            "quote_provider_mode": "REALTIME",
            "realtime_quote_received": True,
            "current_quote_available": True,
            "gap_count": 0,
        }
    )
    return payload


def feature_result(tmp_path: Path, *, ready: bool = True) -> TrackBFeatureBuilderResult:
    report_json = tmp_path / "feature_builder_report.json"
    event_json = tmp_path / "feature_event.json"
    feature_event = {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "strategy_id": "track_b_example_gold_shadow_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "candle_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "close": "4575.3",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "metadata": {
            "ema_momentum_features": {
                "close": "4575.3",
                "vwap": "4575.0",
                "prior_close": "4574.8",
                "momentum_norm": "0.18",
                "momentum_acceleration": "0.03",
                "momentum_turning_positive": True,
            }
        },
    }
    report = {
        "feature_builder_verdict": (
            TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT.value
            if ready
            else TrackBFeatureBuilderVerdict.BLOCKED_INSUFFICIENT_FEATURE_HISTORY.value
        ),
        "signal_ready": ready,
        "output_feature_event_path": str(event_json) if ready else None,
        "primary_blocker": None if ready else "At least 3 candles are required; received 1.",
        "required_next_action": "feature next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    if ready:
        event_json.write_text(json.dumps(feature_event), encoding="utf-8")
    return TrackBFeatureBuilderResult(
        verdict=TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT if ready else TrackBFeatureBuilderVerdict.BLOCKED_INSUFFICIENT_FEATURE_HISTORY,
        report_json=report_json,
        report=report,
        feature_event_json=event_json if ready else None,
        feature_event=feature_event if ready else None,
    )


def strategy_result(tmp_path: Path, *, verdict: str = "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL", decision: str = "LONG", emitted: bool = True) -> TrackBStrategyRuleRunnerResult:
    report_json = tmp_path / "strategy_rule_report.json"
    report = {
        "strategy_rule_runner_verdict": verdict,
        "signal_source": "REAL_STRATEGY_RULE",
        "real_strategy_signal": True,
        "decision": decision,
        "signal_emitted": emitted,
        "signal_direction": "LONG" if emitted else None,
        "strategy_registry_id": "mgc_ema_momentum_reclaim_long_v1",
        "strategy_registry_rule_id": "mgc_ema_momentum_reclaim_long_v1",
        "strategy_registry_rule_mode": "MGC_EMA_MOMENTUM_RECLAIM_LONG",
        "strategy_registry_instrument_family": "MGC",
        "strategy_registry_timeframe": "1m",
        "strategy_registry_paper_eligible": True,
        "strategy_registry_live_money_eligible": False,
        "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
        "runtime_decision_source": "DATABENTO_LIVE_ARTIFACT",
        "decision_bar_timestamp": aware_now().isoformat(),
        "output_batch_path": str(tmp_path / "inbox" / "signal_batch.json") if emitted else None,
        "primary_blocker": None,
        "required_next_action": "strategy next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBStrategyRuleRunnerResult(
        verdict=TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL if emitted else TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
        report_json=report_json,
        report=report,
        downstream_strategy_adapter_report_json=None,
        downstream_candle_producer_report_json=None,
        downstream_signal_batch_writer_report_json=None,
        output_batch_json=Path(str(report["output_batch_path"])) if emitted else None,
    )


def demo_strategy_result(tmp_path: Path, *, emitted: bool = True) -> TrackBStrategyRuleRunnerResult:
    result = strategy_result(tmp_path, emitted=emitted)
    result.report["signal_source"] = "DEMO_WIRING_PROOF"
    result.report["real_strategy_signal"] = False
    result.report["rule_mode"] = "DEMO_LONG_ONLY"
    result.report_json.write_text(json.dumps(result.report), encoding="utf-8")
    return result


def asian_drift_state_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": "DUM882026",
        "contract_key": "MGC-202606",
        "instrument_family": "MGC",
        "strategy_id": "asian_drift_v1",
        "lane_id": "mgc_example_long_lmt_day",
        "timeframe": "5m",
        "candle_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "close": "4575.3",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "asia_drift_state": "NO_TRADE",
        "asia_drift_regime": "NO_TRADE",
        "entry_window_open": True,
        "in_scope": True,
        "session_timeout": False,
        "hypothetical_entry_ready": False,
        "feature_version": "asia_drift_v1_phase1",
        "calibration_profile": "recovery_confirmed",
        "asian_drift_state_ready": True,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def asian_strategy_result(
    tmp_path: Path,
    *,
    decision: str = "NO_SIGNAL",
    emitted: bool = False,
    signal_direction: str | None = None,
) -> TrackBStrategyRuleRunnerResult:
    result = strategy_result(tmp_path, decision=decision, emitted=emitted)
    result.report["signal_source"] = "ASIAN_DRIFT_V1"
    result.report["real_strategy_signal"] = True
    result.report["rule_mode"] = "ASIAN_DRIFT_V1"
    result.report["rule_name"] = "asian_drift_v1_state_snapshot"
    result.report["signal_direction"] = signal_direction or ("LONG" if emitted else None)
    result.report["strategy_registry_id"] = "asian_drift_v1"
    result.report["strategy_registry_rule_id"] = "asian_drift_v1"
    result.report["strategy_registry_rule_mode"] = "ASIAN_DRIFT_V1"
    result.report["strategy_registry_instrument_family"] = "MGC"
    result.report["strategy_registry_timeframe"] = "5m"
    result.report["strategy_registry_paper_eligible"] = True
    result.report["strategy_registry_live_money_eligible"] = False
    result.report["strategy_registry_managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    result.report["strategy_registry_exit_not_available"] = False
    result.report["asian_drift_watch_verdict"] = (
        "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT" if emitted else "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    )
    result.report_json.write_text(json.dumps(result.report), encoding="utf-8")
    return result


def pause_resume_short_strategy_result(
    tmp_path: Path,
    *,
    decision: str = "NO_SIGNAL",
    emitted: bool = False,
    signal_direction: str | None = None,
    paper_eligible: bool = True,
) -> TrackBStrategyRuleRunnerResult:
    result = strategy_result(tmp_path, decision=decision, emitted=emitted)
    result.report["signal_source"] = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    result.report["real_strategy_signal"] = True
    result.report["rule_mode"] = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    result.report["rule_name"] = "asiaEarlyPauseResumeShortTurn"
    result.report["signal_direction"] = signal_direction or ("SHORT" if emitted else None)
    result.report["strategy_registry_id"] = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    result.report["strategy_registry_rule_id"] = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    result.report["strategy_registry_rule_mode"] = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    result.report["strategy_registry_instrument_family"] = "MGC"
    result.report["strategy_registry_timeframe"] = "5m"
    result.report["strategy_registry_paper_eligible"] = paper_eligible
    result.report["strategy_registry_live_money_eligible"] = False
    result.report["strategy_registry_managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    result.report["strategy_registry_exit_not_available"] = False
    result.report["asia_early_pause_resume_short_watch_verdict"] = (
        "ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT"
        if emitted
        else "ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION"
    )
    result.report_json.write_text(json.dumps(result.report), encoding="utf-8")
    return result


def breakout_retest_hold_long_strategy_result(
    tmp_path: Path,
    *,
    decision: str = "NO_SIGNAL",
    emitted: bool = False,
    signal_direction: str | None = None,
    paper_eligible: bool = True,
) -> TrackBStrategyRuleRunnerResult:
    result = strategy_result(tmp_path, decision=decision, emitted=emitted)
    result.report["signal_source"] = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    result.report["real_strategy_signal"] = True
    result.report["rule_mode"] = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    result.report["rule_name"] = "asiaEarlyNormalBreakoutRetestHoldTurn"
    result.report["signal_direction"] = signal_direction or ("LONG" if emitted else None)
    result.report["strategy_registry_id"] = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    result.report["strategy_registry_rule_id"] = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    result.report["strategy_registry_rule_mode"] = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    result.report["strategy_registry_instrument_family"] = "MGC"
    result.report["strategy_registry_timeframe"] = "5m"
    result.report["strategy_registry_paper_eligible"] = paper_eligible
    result.report["strategy_registry_live_money_eligible"] = False
    result.report["strategy_registry_managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    result.report["strategy_registry_exit_not_available"] = False
    result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] = (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT"
        if emitted
        else "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION"
    )
    result.report_json.write_text(json.dumps(result.report), encoding="utf-8")
    return result


def first_snap_turn_strategy_result(
    tmp_path: Path,
    *,
    strategy_id: str,
    decision: str = "NO_SIGNAL",
    emitted: bool = False,
    signal_direction: str | None = None,
    paper_eligible: bool = True,
) -> TrackBStrategyRuleRunnerResult:
    result = strategy_result(tmp_path, decision=decision, emitted=emitted)
    result.report["signal_source"] = strategy_id
    result.report["real_strategy_signal"] = True
    result.report["rule_mode"] = strategy_id
    result.report["rule_name"] = strategy_id.lower()
    result.report["signal_direction"] = signal_direction or (decision if emitted else None)
    result.report["strategy_registry_id"] = strategy_id
    result.report["strategy_registry_rule_id"] = strategy_id
    result.report["strategy_registry_rule_mode"] = strategy_id
    result.report["strategy_registry_instrument_family"] = "MGC"
    result.report["strategy_registry_timeframe"] = "5m"
    result.report["strategy_registry_paper_eligible"] = paper_eligible
    result.report["strategy_registry_live_money_eligible"] = False
    result.report["strategy_registry_managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    result.report["strategy_registry_exit_not_available"] = False
    watch_key = "first_bull_snap_turn_watch_verdict" if strategy_id == "FIRST_BULL_SNAP_TURN_V1" else "first_bear_snap_turn_watch_verdict"
    prefix = "FIRST_BULL_SNAP_TURN" if strategy_id == "FIRST_BULL_SNAP_TURN_V1" else "FIRST_BEAR_SNAP_TURN"
    result.report[watch_key] = f"{prefix}_SIGNAL_READY_NO_SUBMIT" if emitted else f"{prefix}_NO_SIGNAL_NO_MUTATION"
    result.report_json.write_text(json.dumps(result.report), encoding="utf-8")
    return result


def managed_ready_strategy_result(
    tmp_path: Path,
    *,
    strategy_id: str,
    rule_mode: str | None = None,
    rule_id: str | None = None,
    signal_direction: str,
    instrument_family: str = "MGC",
) -> TrackBStrategyRuleRunnerResult:
    result = strategy_result(tmp_path, decision=signal_direction, emitted=True)
    result.report["signal_source"] = strategy_id
    result.report["real_strategy_signal"] = True
    result.report["rule_mode"] = rule_mode or strategy_id
    result.report["rule_name"] = strategy_id.lower()
    result.report["signal_direction"] = signal_direction
    result.report["strategy_registry_id"] = strategy_id
    result.report["strategy_registry_rule_id"] = rule_id or strategy_id
    result.report["strategy_registry_rule_mode"] = rule_mode or strategy_id
    result.report["strategy_registry_instrument_family"] = instrument_family
    result.report["strategy_registry_timeframe"] = "5m"
    result.report["strategy_registry_paper_eligible"] = True
    result.report["strategy_registry_live_money_eligible"] = False
    result.report["strategy_registry_managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    result.report["strategy_registry_exit_not_available"] = False
    result.report_json.write_text(json.dumps(result.report), encoding="utf-8")
    return result


def readiness_result(tmp_path: Path, *, ready: bool = True) -> TrackBReadinessCheckRunnerResult:
    report_json = tmp_path / "readiness_report.json"
    verdict = (
        TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW
        if ready
        else TrackBReadinessCheckRunnerVerdict.BLOCKED_CURRENT_QUOTE
    )
    report = {
        "runner_verdict": verdict.value,
        "readiness_verdict": "READY_FOR_PAPER_PROOF" if ready else "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE",
        "realtime_quote_received": ready,
        "current_quote_available": ready,
        "quote_provider_mode": "REALTIME",
        "primary_blocker": None if ready else "Current quote unavailable.",
        "required_next_action": "readiness next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBReadinessCheckRunnerResult(verdict=verdict, report_json=report_json, report=report)


def proof_result(tmp_path: Path, classification: TerminalClassification) -> PaperProofResult:
    report_json = tmp_path / "paper_proof_report.json"
    report_md = tmp_path / "paper_proof_report.md"
    lifecycle_status = "AMBIGUOUS_MANUAL_REVIEW_REQUIRED"
    final_reconciliation = None
    failure_or_ambiguity = "callback gap"
    required_manual_action = "Manual review required."
    if classification == TerminalClassification.PASSED:
        lifecycle_status = "PROOF_COMPLETE_FLAT"
        final_reconciliation = {"status": "CLEAN"}
        failure_or_ambiguity = None
        required_manual_action = None
    elif classification == TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE:
        lifecycle_status = "PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
        failure_or_ambiguity = "flat but close provenance incomplete"
        required_manual_action = "Verify broker activity."
    proof_payload = {
        "classification": classification.value,
        "proof_lifecycle_status": lifecycle_status,
        "open_intent": {"side": "BUY", "quantity": 1},
        "open_submit_attempt": {"submitted": True},
        "open_fill": {"filled_quantity": 1},
        "close_intent": {"side": "SELL", "quantity": 1},
        "close_submit_attempt": {"submitted": classification == TerminalClassification.PASSED},
        "close_fill": {"filled_quantity": 1} if classification == TerminalClassification.PASSED else None,
        "final_reconciliation": final_reconciliation,
        "flat_after_close_guard_reports": [
            {
                "flat_clean": classification == TerminalClassification.PASSED,
                "position": {"account_id": "DUM882026", "contract_key": "MGC-202606", "signed_quantity": 0},
                "working_orders": [],
            }
        ]
        if classification == TerminalClassification.PASSED
        else [],
        "failure_or_ambiguity": failure_or_ambiguity,
        "required_manual_action": required_manual_action,
    }
    report = {
        "classification": classification.value,
        "proof_payload": proof_payload,
        "failure_or_ambiguity": proof_payload["failure_or_ambiguity"],
        "required_manual_action": proof_payload["required_manual_action"],
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "submit_allowed": classification == TerminalClassification.PASSED,
        "submit_attempted": True,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    report_md.write_text("# proof", encoding="utf-8")
    return PaperProofResult(
        run_id="proof-test",
        classification=classification,
        report_json=report_json,
        report_md=report_md,
        report=report,
        proof_result=None,
    )


def managed_lifecycle_result(
    tmp_path: Path,
    classification: TrackBManagedPaperLifecycleClassification,
    *,
    managed_exit_policy_id: str = "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
) -> TrackBStrategyManagedPaperLifecycleResult:
    report_json = tmp_path / "managed_lifecycle_report.json"
    report = {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": "managed-test",
        "trade_id": "mgc_ema_momentum_reclaim_long_v1:managed-test",
        "strategy_id": "mgc_ema_momentum_reclaim_long_v1",
        "instrument_family": "MGC",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "account_id": "DUM882026",
        "mode": "PAPER",
        "managed_exit_policy_id": managed_exit_policy_id,
        "strategy_managed_lifecycle_classification": classification.value,
        "paper_lifecycle_classification": classification.value,
        "entry_intent": {
            "strategy_id": "mgc_ema_momentum_reclaim_long_v1",
            "contract_key": "MGC-202606",
            "local_symbol": "MGCM6",
            "side": "LONG",
            "order_action": "BUY",
            "quantity": 1,
            "entry_limit_price": "4575.3",
            "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
        },
        "entry_submit_attempt": {"submitted": True, "broker_state_mutated": True, "broker_order_id": "101"},
        "entry_fill": {"price": "4575.3", "quantity": 1, "filled_at": aware_now().isoformat()},
        "close_intent": {"order_action": "SELL", "quantity": 1, "close_limit_price": "4575.6"},
        "close_submit_attempt": {"submitted": classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT},
        "close_fill": (
            {"price": "4575.6", "quantity": 1, "filled_at": aware_now().isoformat()}
            if classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
            else None
        ),
        "final_position_status": "CLOSED_FLAT"
        if classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT
        else "OPEN_MANAGED",
        "final_broker_state_classification": classification.value,
        "review_required": classification == TrackBManagedPaperLifecycleClassification.REVIEW_REQUIRED,
        "broker_reconciled": False,
        "submit_allowed": True,
        "submit_attempted": True,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "broker_state_mutated": True,
        "live_money_readiness": False,
        "required_next_action": "managed next",
        "primary_blocker": None,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(tmp_path / "latest_managed_lifecycle_report.json"),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBStrategyManagedPaperLifecycleResult(
        lifecycle_id="managed-test",
        classification=classification,
        report_json=report_json,
        report=report,
    )


def managed_lifecycle_no_submit_result(
    tmp_path: Path,
    *,
    managed_exit_policy_id: str = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
) -> TrackBStrategyManagedPaperLifecycleResult:
    report_json = tmp_path / "managed_lifecycle_no_submit_report.json"
    report = {
        "schema_version": "track_b_strategy_managed_paper_lifecycle_v1",
        "lifecycle_id": "strategy_managed_fixture_no_submit",
        "trade_id": None,
        "strategy_id": "asian_drift_v1",
        "instrument_family": "MGC",
        "contract_key": "MGC-202606",
        "local_symbol": "MGCM6",
        "con_id": 712565978,
        "account_id": "DUM882026",
        "mode": "PAPER",
        "managed_exit_policy_id": managed_exit_policy_id,
        "strategy_managed_lifecycle_classification": TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE.value,
        "paper_lifecycle_classification": TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE.value,
        "entry_intent": None,
        "entry_submit_attempt": None,
        "entry_fill": None,
        "close_intent": None,
        "close_submit_attempt": None,
        "close_fill": None,
        "final_position_status": "NO_BROKER_SUBMIT_TEST_FIXTURE",
        "final_broker_state_classification": TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE.value,
        "review_required": False,
        "broker_reconciled": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "primary_blocker": "Test fixture stopped after strategy trade intent creation; no broker adapter invoked.",
        "required_next_action": "Fixture proof complete; do not submit.",
        "report_json_path": str(report_json),
        "latest_report_json_path": str(tmp_path / "latest_managed_lifecycle_no_submit_report.json"),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBStrategyManagedPaperLifecycleResult(
        lifecycle_id="strategy_managed_fixture_no_submit",
        classification=TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE,
        report_json=report_json,
        report=report,
    )


def stages(
    *,
    calls: Calls,
    strategy: TrackBStrategyRuleRunnerResult,
    candle_history: TrackBMgcCandleHistoryProducerResult | None = None,
    market_history: TrackBMarketHistoryResult | None = None,
    feature: TrackBFeatureBuilderResult | None = None,
    readiness: TrackBReadinessCheckRunnerResult | None = None,
    intent: TrackBStrategyTradeIntentResult | None = None,
    managed_lifecycle: TrackBStrategyManagedPaperLifecycleResult | None = None,
    proof: PaperProofResult | None = None,
) -> TrackBStrategyPaperRunnerStages:
    def candle_history_stage(config: TrackBStrategyPaperRunnerConfig) -> TrackBMgcCandleHistoryProducerResult:
        calls.candle_history += 1
        assert candle_history is not None
        return candle_history

    def market_history_stage(
        config: TrackBStrategyPaperRunnerConfig,
        candle_history_result: TrackBMgcCandleHistoryProducerResult,
    ) -> TrackBMarketHistoryResult:
        calls.market_history += 1
        assert market_history is not None
        if candle_history is not None:
            assert candle_history_result == candle_history
        return market_history

    def market_history_from_payload_stage(
        config: TrackBStrategyPaperRunnerConfig,
        payload: dict[str, object],
        source_payload_path: Path | None,
    ) -> TrackBMarketHistoryResult:
        calls.market_history += 1
        assert market_history is not None
        assert payload["quote_provider_mode"] == "REALTIME"
        assert payload["realtime_quote_received"] is True
        assert payload["current_quote_available"] is True
        assert source_payload_path in {config.maintained_history_json, config.runtime_candle_context_json}
        return market_history

    def feature_stage(config: TrackBStrategyPaperRunnerConfig) -> TrackBFeatureBuilderResult:
        calls.feature += 1
        assert feature is not None
        if market_history is not None and market_history.history_event is not None:
            assert config.build_features_from_payload == market_history.history_event
        return feature

    def strategy_stage(config: TrackBStrategyPaperRunnerConfig) -> TrackBStrategyRuleRunnerResult:
        calls.strategy += 1
        if feature is not None and feature.feature_event is not None:
            assert config.input_event_payload == feature.feature_event
        return strategy

    def readiness_stage(config: TrackBStrategyPaperRunnerConfig) -> TrackBReadinessCheckRunnerResult:
        calls.readiness += 1
        assert readiness is not None
        return readiness

    def proof_stage(config: TrackBStrategyPaperRunnerConfig) -> PaperProofResult:
        calls.proof += 1
        assert proof is not None
        return proof

    def intent_stage(
        config: TrackBStrategyPaperRunnerConfig,
        strategy_report: dict[str, object],
    ) -> TrackBStrategyTradeIntentResult:
        calls.intent += 1
        if intent is not None:
            return intent
        return create_track_b_strategy_trade_intent(
            config=TrackBStrategyTradeIntentConfig(
                mode=config.mode,
                account_id=config.account_id,
                expected_account_id=config.expected_account_id,
                strategy_id=config.strategy_id,
                instrument_family=str(strategy_report.get("strategy_registry_instrument_family") or "MGC"),
                contract_key=config.contract_key,
                local_symbol=config.allowlisted_local_symbol,
                con_id=config.con_id,
                side=config.side,
                quantity=config.quantity,
                runtime_source=config.runtime_decision_source,
                latest_decision_bar_source=str(strategy_report.get("latest_decision_bar_source") or "DATABENTO_LIVE_ARTIFACT"),
                pricing_policy=config.paper_order_pricing_policy,
                managed_exit_policy_id=config.managed_exit_policy_id,
                output_root=config.strategy_trade_intent_output_root,
                paper_trade_ledger_output_root=config.paper_trade_ledger_output_root
                or Path(config.strategy_trade_intent_output_root).parent / "paper_trade_ledger",
            ),
            strategy_report=strategy_report,
            intent_id="intent-test",
            now=aware_now(),
        )

    def managed_lifecycle_stage(
        config: TrackBStrategyPaperRunnerConfig,
        strategy_report: dict[str, object],
    ) -> TrackBStrategyManagedPaperLifecycleResult:
        calls.managed_lifecycle += 1
        assert managed_lifecycle is not None
        assert config.managed_exit_policy_id is not None
        assert strategy_report["real_strategy_signal"] is True
        return managed_lifecycle

    def operator_status_stage(config: TrackBStrategyPaperRunnerConfig, runner_report_json: Path) -> None:
        calls.operator_status += 1

    return TrackBStrategyPaperRunnerStages(
        candle_history_producer=candle_history_stage,
        market_history_collector=market_history_stage,
        market_history_from_payload=market_history_from_payload_stage,
        feature_builder=feature_stage,
        strategy_rule=strategy_stage,
        readiness=readiness_stage,
        strategy_trade_intent=intent_stage,
        managed_lifecycle=managed_lifecycle_stage,
        paper_proof=proof_stage,
        operator_status=operator_status_stage,
    )


def test_no_signal_stops_without_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path, decision="NO_SIGNAL", emitted=False)),
        runner_id="paper-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.NO_SIGNAL
    assert calls.feature == 0
    assert calls.strategy == 1
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_human_review_stops_without_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path, decision="HUMAN_REVIEW", emitted=False)),
        runner_id="paper-human-review",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL
    assert calls.feature == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False


def test_signal_with_blocked_readiness_does_not_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, emit_signal=True),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path), readiness=readiness_result(tmp_path, ready=False)),
        runner_id="paper-readiness-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_READINESS
    assert calls.feature == 0
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["readiness_verdict"] == "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE"
    assert result.report["submit_attempted"] is False


def test_strategy_managed_phase1_pricing_evidence_skips_legacy_quote_readiness(tmp_path: Path) -> None:
    calls = Calls()
    now = aware_now()
    phase1_payload = {
        "schema": "ohlcv-1m",
        "source": "DATABENTO_REALTIME_PHASE1",
        "source_category": "PHASE1_RUNTIME_MARKET_DATA",
        "symbol": "MGC",
        "timeframe": "1m",
        "generated_at": now.isoformat(),
        "last_completed_bar_ts": now.isoformat(),
        "realtime_feed_confirmed": True,
        "realtime_feed_block_reason": "READY",
        "freshness_seconds": 180.0,
        "bars": [{"bar_end": now.isoformat(), "close": "4575.3", "completed": True}],
    }
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.6",
            paper_order_pricing_policy="LIMIT_AT_LAST",
            current_quote_report_payload=phase1_payload,
            managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path, ready=False),
            managed_lifecycle=managed_lifecycle_result(
                tmp_path,
                TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
                managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
            ),
        ),
        runner_id="paper-phase1-managed-submit",
        now=now,
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_OPEN_MANAGED
    assert calls.readiness == 0
    assert calls.managed_lifecycle == 1
    assert result.report["strategy_managed_phase1_runtime_pricing_ready"] is True
    assert result.report["readiness_verdict"] == "READY_FOR_PAPER_PROOF"
    assert result.report["paper_proof_invoked"] is False


def test_blocked_strategy_rule_does_not_run_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    blocked_strategy = strategy_result(
        tmp_path,
        verdict="TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_NON_REALTIME_INPUT",
        decision="NO_SIGNAL",
        emitted=False,
    )
    blocked_strategy.report["primary_blocker"] = "Input is not explicitly REALTIME."

    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, emit_signal=True),
        stages=stages(calls=calls, strategy=blocked_strategy),
        runner_id="paper-strategy-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_STRATEGY_RULE
    assert calls.feature == 0
    assert calls.strategy == 1
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["primary_blocker"] == "Input is not explicitly REALTIME."
    assert result.report["submit_attempted"] is False


def test_signal_and_green_readiness_without_submit_flags_stops_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, emit_signal=True),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path), readiness=readiness_result(tmp_path)),
        runner_id="paper-ready-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert calls.feature == 0
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["paper_submit_requested"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_real_strategy_signal_does_not_route_to_paper_proof_by_default(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-managed-missing-exit-policy",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_EXIT_POLICY_MISSING
    assert calls.proof == 0
    assert calls.intent == 1
    assert calls.managed_lifecycle == 0
    assert result.report["paper_execution_path"] == "STRATEGY_MANAGED"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["managed_lifecycle_invoked"] is False
    assert result.report["strategy_trade_intent_created"] is False
    assert result.report["strategy_trade_intent_classification"] == "INTENT_BLOCKED_MISSING_EXIT_POLICY"
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert "paper_proof is not a strategy-management fallback" in result.report["primary_blocker"]


def test_real_strategy_signal_with_exit_policy_routes_to_managed_lifecycle(tmp_path: Path) -> None:
    calls = Calls()
    managed = managed_lifecycle_result(
        tmp_path,
        TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    )
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
            managed_exit_policy_id="DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            managed_lifecycle=managed,
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-managed-open",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_OPEN_MANAGED
    assert calls.intent == 1
    assert calls.managed_lifecycle == 1
    assert calls.proof == 0
    assert result.report["strategy_trade_intent_created"] is True
    assert result.report["strategy_trade_intent_classification"] == "STRATEGY_TRADE_INTENT_CREATED"
    assert result.report["strategy_trade_intent_report_path"]
    assert result.report["paper_proof_invoked"] is False
    assert result.report["managed_lifecycle_invoked"] is True
    assert result.report["managed_lifecycle_classification"] == "TRACK_B_STRATEGY_PAPER_OPEN_MANAGED"
    assert result.report["managed_entry_intent"] is not None
    assert result.report["submit_attempted"] is True
    assert result.report["broker_state_mutated"] is True
    assert result.report["live_money_readiness"] is False


def test_registry_managed_exit_policy_enables_selected_breakout_strategy(tmp_path: Path) -> None:
    calls = Calls()
    managed = managed_lifecycle_result(
        tmp_path,
        TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    )
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=breakout_retest_hold_long_strategy_result(tmp_path, emitted=True),
            readiness=readiness_result(tmp_path),
            managed_lifecycle=managed,
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-managed-breakout-registry-policy",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_OPEN_MANAGED
    assert calls.intent == 1
    assert calls.managed_lifecycle == 1
    assert calls.proof == 0
    assert result.report["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.report["strategy_registry_managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.report["strategy_trade_intent_created"] is True
    assert result.report["paper_proof_invoked"] is False


def test_registry_managed_exit_policy_enables_mnq_first_bear_snap_turn(tmp_path: Path) -> None:
    calls = Calls()
    managed = managed_lifecycle_result(
        tmp_path,
        TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
        managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
    )
    strategy = first_snap_turn_strategy_result(
        tmp_path,
        strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
        decision="SHORT",
        emitted=True,
        signal_direction="SHORT",
    )
    strategy.report["strategy_registry_instrument_family"] = "MNQ"
    strategy.report["strategy_registry_managed_exit_policy_id"] = "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    strategy.report["strategy_registry_exit_not_available"] = False
    strategy.report_json.write_text(json.dumps(strategy.report), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            rule_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            rule_mode="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            contract_key="MNQ-202606",
            allowlisted_local_symbol="MNQM6",
            con_id=770561201,
            side="SELL",
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="28775.0",
            manual_close_limit_price="28776.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy,
            readiness=readiness_result(tmp_path),
            managed_lifecycle=managed,
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-managed-mnq-bear-registry-policy",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_OPEN_MANAGED
    assert calls.intent == 1
    assert calls.managed_lifecycle == 1
    assert calls.proof == 0
    assert result.report["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.report["strategy_registry_managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.report["strategy_trade_intent_created"] is True
    assert result.report["paper_proof_invoked"] is False


def test_remaining_ported_strategy_signals_route_to_managed_lifecycle(tmp_path: Path) -> None:
    cases = (
        ("asian_drift_v1", "ASIAN_DRIFT_V1", "asian_drift_v1", "LONG"),
        ("ASIA_EARLY_PAUSE_RESUME_SHORT_V1", None, None, "SHORT"),
        ("FIRST_BULL_SNAP_TURN_V1", None, None, "LONG"),
        ("FIRST_BEAR_SNAP_TURN_V1", None, None, "SHORT"),
        ("ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1", None, None, "LONG"),
    )

    for strategy_id, rule_mode, rule_id, direction in cases:
        case_dir = tmp_path / strategy_id.lower()
        calls = Calls()
        managed = managed_lifecycle_result(
            case_dir,
            TrackBManagedPaperLifecycleClassification.OPEN_MANAGED,
            managed_exit_policy_id="PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        )
        strategy = (
            asian_strategy_result(case_dir, decision=direction, emitted=True, signal_direction=direction)
            if strategy_id == "asian_drift_v1"
            else managed_ready_strategy_result(
                case_dir,
                strategy_id=strategy_id,
                rule_mode=rule_mode,
                rule_id=rule_id,
                signal_direction=direction,
            )
        )

        result = run_track_b_strategy_paper(
            config=base_config(
                case_dir,
                input_event_payload=(
                    asian_drift_state_payload(
                        asia_drift_state="ENTRY_ARMED",
                        asia_drift_regime=f"ASIA_DRIFT_{direction}",
                        direction=direction,
                        hypothetical_entry_ready=True,
                    )
                    if strategy_id == "asian_drift_v1"
                    else base_config(case_dir).input_event_payload
                ),
                strategy_id=strategy_id,
                rule_id=rule_id or strategy_id,
                rule_mode=rule_mode or strategy_id,
                side="SELL" if direction == "SHORT" else "BUY",
                emit_signal=True,
                submit_paper=True,
                confirm_paper_submit=True,
                quantity=1,
                manual_open_limit_price="4575.3",
                manual_close_limit_price="4575.0",
            ),
            stages=stages(
                calls=calls,
                strategy=strategy,
                readiness=readiness_result(case_dir),
                managed_lifecycle=managed,
                proof=proof_result(case_dir, TerminalClassification.PASSED),
            ),
            runner_id=f"paper-managed-{strategy_id.lower()}",
            now=aware_now(),
        )

        assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_OPEN_MANAGED
        assert calls.intent == 1
        assert calls.managed_lifecycle == 1
        assert calls.proof == 0
        assert result.report["paper_execution_path"] == "STRATEGY_MANAGED"
        assert result.report["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
        assert result.report["strategy_trade_intent_created"] is True
        assert result.report["strategy_trade_intent_classification"] == "STRATEGY_TRADE_INTENT_CREATED"
        assert result.report["paper_proof_invoked"] is False
        assert result.report["live_money_readiness"] is False


def test_signal_readiness_green_and_explicit_submit_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert calls.feature == 0
    assert result.report["signal_source"] == "REAL_STRATEGY_RULE"
    assert result.report["real_strategy_signal"] is True


def test_feature_builder_signal_readiness_green_and_explicit_submit_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            build_features_from_payload={"candle_items": []},
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-feature-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert result.report["feature_builder_invoked"] is True
    assert result.report["feature_builder_verdict"] == "TRACK_B_FEATURE_BUILDER_WROTE_FEATURE_EVENT"
    assert result.report["feature_event_path"]
    assert result.report["strategy_rule_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"
    assert result.report["readiness_invoked"] is True
    assert result.report["readiness_verdict"] == "READY_FOR_PAPER_PROOF"


def test_full_history_feature_rule_readiness_green_and_explicit_submit_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            candle_history_payload={"candles": []},
            current_quote_report_payload={"current_quote_available": True},
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            candle_history=candle_history_producer_result(tmp_path),
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-full-real-rule-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert calls.candle_history == 1
    assert calls.market_history == 1
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert result.report["candle_history_producer_invoked"] is True
    assert result.report["candle_history_producer_verdict"] == "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_WROTE_HISTORY_INPUT"
    assert result.report["market_history_collector_invoked"] is True
    assert result.report["market_history_collector_verdict"] == "TRACK_B_MARKET_HISTORY_WROTE_HISTORY_EVENT"
    assert result.report["feature_builder_verdict"] == "TRACK_B_FEATURE_BUILDER_WROTE_FEATURE_EVENT"
    assert result.report["strategy_rule_verdict"] == "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"
    assert result.report["readiness_verdict"] == "READY_FOR_PAPER_PROOF"


def test_maintained_history_feature_rule_submit_uses_maintenance_without_history_fetch(tmp_path: Path) -> None:
    calls = Calls()
    maintained_history_json = tmp_path / "latest_good_mgc_1m_history.json"
    maintained_history_json.write_text(json.dumps(maintained_history_payload()), encoding="utf-8")
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=maintained_history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            candle_history=candle_history_producer_result(tmp_path),
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-maintained-history-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert calls.candle_history == 0
    assert calls.market_history == 1
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert result.report["data_maintenance_history_requested"] is True
    assert result.report["maintained_history_path"] == str(maintained_history_json)
    assert result.report["candle_history_producer_invoked"] is False


def test_stale_maintained_history_blocks_only_when_intraday_freshness_required(tmp_path: Path) -> None:
    calls = Calls()
    stale_history_json = tmp_path / "stale_latest_good_mgc_1m_history.json"
    stale_history_json.write_text(
        json.dumps(maintained_history_payload(latest_timestamp="2026-05-04T13:00:00+00:00")),
        encoding="utf-8",
    )
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=stale_history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
            runtime_intraday_freshness_policy="REQUIRE_MAX_AGE",
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-stale-maintained-history",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE
    assert calls.candle_history == 0
    assert calls.market_history == 0
    assert calls.feature == 0
    assert calls.strategy == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert "stale" in str(result.report["primary_blocker"])
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_maintained_history_age_961s_passes_as_historical_context_by_default(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_961s.json"
    history_json.write_text(
        json.dumps(maintained_history_payload(latest_timestamp="2026-05-04T14:13:59+00:00")),
        encoding="utf-8",
    )

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-maintained-history-961s-default-historical-context",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert result.report["maintained_history_age_seconds"] == 961
    assert result.report["max_maintained_history_age_seconds"] == 900
    assert result.report["runtime_intraday_freshness_policy"] == "NOT_REQUESTED"
    assert result.report["historical_context_ready"] is True
    assert result.report["maintained_history_ready"] is True
    assert calls.market_history == 1


def test_maintained_history_age_961s_blocks_when_intraday_policy_explicit(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_961s_intraday.json"
    history_json.write_text(
        json.dumps(maintained_history_payload(latest_timestamp="2026-05-04T14:13:59+00:00")),
        encoding="utf-8",
    )

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            runtime_intraday_freshness_policy="REQUIRE_MAX_AGE",
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-maintained-history-961s-intraday-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE
    assert result.report["maintained_history_age_seconds"] == 961
    assert result.report["runtime_intraday_freshness_policy"] == "REQUIRE_MAX_AGE"
    assert result.report["maintained_history_ready"] is False
    assert calls.market_history == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False


def test_maintained_history_age_961s_passes_with_explicit_1200s_threshold(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_961s_wide.json"
    history_json.write_text(
        json.dumps(maintained_history_payload(latest_timestamp="2026-05-04T14:13:59+00:00")),
        encoding="utf-8",
    )

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            max_maintained_history_age_seconds=1200,
            runtime_intraday_freshness_policy="REQUIRE_MAX_AGE",
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-maintained-history-961s-explicit-pass",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert result.report["maintained_history_age_seconds"] == 961
    assert result.report["max_maintained_history_age_seconds"] == 1200
    assert result.report["maintained_history_ready"] is True
    assert calls.market_history == 1
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False


def test_stale_maintained_history_passes_for_paper_diagnostic_override_without_submit(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_stale_override.json"
    history_json.write_text(
        json.dumps(maintained_history_payload(latest_timestamp="2026-05-04T13:00:00+00:00")),
        encoding="utf-8",
    )

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            runtime_intraday_freshness_policy="REQUIRE_MAX_AGE",
            allow_stale_maintained_history_paper=True,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-maintained-history-stale-override-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert result.report["maintained_history_age_seconds"] == 5400
    assert result.report["max_maintained_history_age_seconds"] == 900
    assert result.report["maintained_history_ready"] is False
    assert result.report["maintained_history_effective_ready"] is True
    assert result.report["maintained_history_stale_override_requested"] is True
    assert result.report["maintained_history_stale_override_used"] is True
    assert result.report["live_money_readiness"] is False
    assert "PAPER diagnostics only" in str(result.report["required_next_action"])
    assert calls.market_history == 1
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False


def test_stale_maintained_history_override_refuses_non_paper_mode(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_stale_nonpaper.json"
    history_json.write_text(
        json.dumps(maintained_history_payload(latest_timestamp="2026-05-04T13:00:00+00:00")),
        encoding="utf-8",
    )

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            mode="LIVE",
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            allow_stale_maintained_history_paper=True,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-maintained-history-stale-override-nonpaper",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_NON_PAPER_MODE
    assert result.report["maintained_history_stale_override_requested"] is True
    assert result.report["maintained_history_stale_override_used"] is False
    assert result.report["live_money_readiness"] is False
    assert calls.market_history == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False


def test_maintained_history_insufficient_bars_blocks_even_if_reported_ready(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_short.json"
    short_payload = maintained_history_payload()
    short_payload["candles"] = short_payload["candles"][-1:]  # type: ignore[index]
    short_payload["candle_history"] = short_payload["candles"]
    history_json.write_text(json.dumps(short_payload), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            max_maintained_history_age_seconds=1200,
            allow_stale_maintained_history_paper=True,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-maintained-history-short-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE
    assert result.report["maintained_history_ready"] is False
    assert result.report["maintained_history_stale_override_used"] is False
    assert result.report["maintained_history_bar_count"] == 1
    assert "requires at least" in str(result.report["primary_blocker"])
    assert calls.market_history == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False


def test_maintained_history_missing_realtime_quote_blocks(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_missing_quote.json"
    history_json.write_text(json.dumps(maintained_history_payload()), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            max_maintained_history_age_seconds=1200,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-maintained-history-missing-quote-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE
    assert result.report["maintained_history_ready"] is False
    assert "current quote report" in str(result.report["primary_blocker"])
    assert calls.market_history == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False


def test_runtime_candle_context_required_blocks_maintained_history_only(tmp_path: Path) -> None:
    calls = Calls()
    history_json = tmp_path / "latest_good_mgc_1m_history_runtime_required.json"
    history_json.write_text(json.dumps(maintained_history_payload()), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            maintained_history_json=history_json,
            current_quote_report_payload={"quote_provider_mode": "REALTIME", "realtime_quote_received": True, "current_quote_available": True},
            runtime_candle_context_required=True,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-maintained-history-runtime-context-required",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE
    assert result.report["historical_context_ready"] is True
    assert result.report["runtime_candle_context_required"] is True
    assert result.report["runtime_candle_context_supplied"] is False
    assert "Runtime candle context is required" in str(result.report["primary_blocker"])
    assert calls.market_history == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False


def test_runtime_candle_context_can_feed_feature_builder_and_rule(tmp_path: Path) -> None:
    calls = Calls()
    runtime_context_json = tmp_path / "latest_runtime_mgc_1m_candles.json"
    runtime_context_json.write_text(json.dumps(runtime_candle_context_payload()), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            runtime_candle_context_json=runtime_context_json,
            runtime_candle_context_required=True,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-runtime-candle-context-ready-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert calls.candle_history == 0
    assert calls.market_history == 1
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["runtime_candle_context_required"] is True
    assert result.report["runtime_candle_context_requested"] is True
    assert result.report["runtime_candle_context_supplied"] is True
    assert result.report["runtime_candle_context_ready"] is True
    assert result.report["runtime_candle_context_bars_available"] == 3
    assert result.report["runtime_candle_context_path"] == str(runtime_context_json)
    assert result.report["market_history_collector_invoked"] is True
    assert result.report["feature_builder_invoked"] is True
    assert result.report["strategy_rule_evaluated"] is True
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_runtime_candle_context_no_signal_does_not_invoke_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    runtime_context_json = tmp_path / "latest_runtime_mgc_1m_candles_no_signal.json"
    runtime_context_json.write_text(json.dumps(runtime_candle_context_payload()), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            runtime_candle_context_json=runtime_context_json,
            runtime_candle_context_required=True,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path, decision="NO_SIGNAL", emitted=False),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-runtime-candle-context-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.NO_SIGNAL
    assert result.report["runtime_candle_context_supplied"] is True
    assert result.report["runtime_candle_context_required"] is True
    assert result.report["runtime_candle_context_ready"] is True
    assert result.report["feature_builder_invoked"] is True
    assert result.report["strategy_rule_evaluated"] is True
    assert result.report["rule_decision"] == "NO_SIGNAL"
    assert result.report["signal_source"] == "REAL_STRATEGY_RULE"
    assert result.report["real_strategy_signal"] is True
    assert result.report["signal_emitted"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_demo_wiring_signal_is_explicitly_labeled_and_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_mode="DEMO_LONG_ONLY",
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=demo_strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-demo-wiring-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert result.report["rule_mode"] == "DEMO_LONG_ONLY"
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False


def test_demo_wiring_signal_stays_no_submit_without_explicit_flags(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, rule_mode="DEMO_LONG_ONLY", emit_signal=True),
        stages=stages(
            calls=calls,
            strategy=demo_strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-demo-wiring-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_missing_state_reports_not_ready_and_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    blocked_strategy = strategy_result(
        tmp_path,
        verdict="TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_INVALID_INPUT",
        decision="NO_SIGNAL",
        emitted=False,
    )
    blocked_strategy.report["primary_blocker"] = "Asian Drift v1 requires explicit research state/feature snapshot fields."
    blocked_strategy.report["asian_drift_watch_verdict"] = "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    blocked_strategy.report_json.write_text(json.dumps(blocked_strategy.report), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload={"account_id": "DUM882026", "contract_key": "MGC-202606", "close": "4575.3"},
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
        ),
        stages=stages(calls=calls, strategy=blocked_strategy),
        runner_id="paper-asian-drift-not-ready",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NOT_READY_FOR_TONIGHT
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    assert result.report["asian_drift_state_ready"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_valid_no_signal_state_reports_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=asian_drift_state_payload(),
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
            strategy_id="asian_drift_v1",
        ),
        stages=stages(calls=calls, strategy=asian_strategy_result(tmp_path, decision="NO_SIGNAL", emitted=False)),
        runner_id="paper-asian-drift-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    assert result.report["asian_drift_state_ready"] is True
    assert result.report["rule_decision"] == "NO_SIGNAL"
    assert result.report["signal_emitted"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_signal_without_paper_flags_reports_signal_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=asian_drift_state_payload(
                asia_drift_state="ENTRY_ARMED",
                asia_drift_regime="ASIA_DRIFT_LONG",
                direction="LONG",
                hypothetical_entry_ready=True,
            ),
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
            strategy_id="asian_drift_v1",
        ),
        stages=stages(calls=calls, strategy=asian_strategy_result(tmp_path, decision="LONG", emitted=True)),
        runner_id="paper-asian-drift-signal-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    assert result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["signal_emitted"] is True
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_entry_capable_fixture_creates_managed_trade_intent_without_broker_submit(
    tmp_path: Path,
) -> None:
    fixture_path = asian_drift_entry_fixture("asian_drift_entry_capable_long_state.json")
    fixture_payload = json.loads(fixture_path.read_text(encoding="utf-8"))
    strategy_result = run_track_b_strategy_rule(
        input_event_payload=fixture_payload,
        input_event_path=fixture_path,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="asian_drift_entry_capable_fixture",
        strategy_id="asian_drift_v1",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_runner",
        strategy_adapter_output_root=tmp_path / "adapter",
        candle_producer_output_root=tmp_path / "candle",
        writer_output_root=tmp_path / "writer",
        runner_id="asian-drift-entry-capable-rule",
        now=aware_now(),
    )

    assert strategy_result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert strategy_result.report["decision"] == "LONG"
    assert strategy_result.report["signal_direction"] == "LONG"
    assert strategy_result.report["rule_inputs"]["asia_drift_regime"] == "ASIA_DRIFT_LONG"
    assert strategy_result.report["rule_inputs"]["direction_required"] is True
    assert strategy_result.report["rule_blockers"] == []

    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_json=fixture_path,
            input_event_payload=fixture_payload,
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
            strategy_id="asian_drift_v1",
            side="BUY",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="STRATEGY_MANAGED",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result,
            readiness=readiness_result(tmp_path),
            managed_lifecycle=managed_lifecycle_no_submit_result(tmp_path),
        ),
        runner_id="paper-asian-drift-entry-capable-intent",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_LIFECYCLE_NOT_AVAILABLE
    assert calls.readiness == 1
    assert calls.intent == 1
    assert calls.managed_lifecycle == 1
    assert calls.proof == 0
    assert result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert result.report["signal_direction"] == "LONG"
    assert result.report["strategy_trade_intent_created"] is True
    assert result.report["strategy_trade_intent_classification"] == "STRATEGY_TRADE_INTENT_CREATED"
    assert result.report["strategy_trade_intent_id"] == "intent-test"
    assert result.report["lifecycle_mode"] == "STRATEGY_MANAGED"
    assert result.report["paper_execution_path"] == "STRATEGY_MANAGED"
    assert result.report["managed_exit_policy_id"] == "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False
    intent_path = Path(str(result.report["strategy_trade_intent_report_path"]))
    assert intent_path.exists()
    intent_payload = json.loads(intent_path.read_text(encoding="utf-8"))
    assert intent_payload["strategy_id"] == "asian_drift_v1"
    assert intent_payload["side"] == "LONG"
    assert intent_payload["order_action"] == "BUY"
    assert intent_payload["lifecycle_mode"] == "STRATEGY_MANAGED"
    assert intent_payload["latest_decision_bar_source"] == "DATABENTO_LIVE_ARTIFACT"
    assert intent_payload["live_money_readiness"] is False


def test_asian_drift_entry_capable_short_fixture_maps_to_short_signal(tmp_path: Path) -> None:
    fixture_path = asian_drift_entry_fixture("asian_drift_entry_capable_short_state.json")
    fixture_payload = json.loads(fixture_path.read_text(encoding="utf-8"))

    result = run_track_b_strategy_rule(
        input_event_payload=fixture_payload,
        input_event_path=fixture_path,
        inbox_dir=tmp_path / "inbox",
        expected_account_id="DUM882026",
        source_id="asian_drift_entry_capable_short_fixture",
        strategy_id="asian_drift_v1",
        rule_id="asian_drift_v1",
        rule_mode="ASIAN_DRIFT_V1",
        emit_signal=True,
        output_root=tmp_path / "rule_runner",
        strategy_adapter_output_root=tmp_path / "adapter",
        candle_producer_output_root=tmp_path / "candle",
        writer_output_root=tmp_path / "writer",
        runner_id="asian-drift-entry-capable-short-rule",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL
    assert result.report["decision"] == "SHORT"
    assert result.report["signal_direction"] == "SHORT"
    assert result.report["rule_inputs"]["asia_drift_regime"] == "ASIA_DRIFT_SHORT"
    assert result.report["rule_inputs"]["direction_required"] is True
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_asian_drift_signal_with_paper_flags_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=asian_drift_state_payload(
                asia_drift_state="ENTRY_ARMED",
                asia_drift_regime="ASIA_DRIFT_LONG",
                direction="LONG",
                hypothetical_entry_ready=True,
            ),
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
            strategy_id="asian_drift_v1",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=asian_strategy_result(tmp_path, decision="LONG", emitted=True),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-asian-drift-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert result.report["asian_drift_watch_verdict"] == "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    assert result.report["signal_source"] == "ASIAN_DRIFT_V1"
    assert result.report["real_strategy_signal"] is True
    assert calls.readiness == 1
    assert result.report["readiness_invoked"] is True


def test_demo_wiring_signal_cannot_drive_asian_drift_paper_path(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=asian_drift_state_payload(
                asia_drift_state="ENTRY_ARMED",
                asia_drift_regime="ASIA_DRIFT_LONG",
                direction="LONG",
                hypothetical_entry_ready=True,
            ),
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
            strategy_id="asian_drift_v1",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=demo_strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-asian-drift-reject-demo",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NOT_READY_FOR_TONIGHT
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False
    assert "DEMO/proof signals cannot drive this path" in result.report["required_next_action"]
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_first_bull_snap_turn_signal_without_paper_flags_reports_signal_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="FIRST_BULL_SNAP_TURN_V1",
            rule_mode="FIRST_BULL_SNAP_TURN_V1",
            emit_signal=True,
            strategy_id="FIRST_BULL_SNAP_TURN_V1",
        ),
        stages=stages(
            calls=calls,
            strategy=first_snap_turn_strategy_result(
                tmp_path,
                strategy_id="FIRST_BULL_SNAP_TURN_V1",
                decision="LONG",
                emitted=True,
                signal_direction="LONG",
            ),
        ),
        runner_id="paper-first-bull-signal-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT
    assert result.report["first_bull_snap_turn_watch_verdict"] == "FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT"
    assert result.report["signal_source"] == "FIRST_BULL_SNAP_TURN_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_first_bear_snap_turn_signal_with_paper_flags_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="FIRST_BEAR_SNAP_TURN_V1",
            rule_mode="FIRST_BEAR_SNAP_TURN_V1",
            emit_signal=True,
            strategy_id="FIRST_BEAR_SNAP_TURN_V1",
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=stages(
            calls=calls,
            strategy=first_snap_turn_strategy_result(
                tmp_path,
                strategy_id="FIRST_BEAR_SNAP_TURN_V1",
                decision="SHORT",
                emitted=True,
                signal_direction="SHORT",
            ),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-first-bear-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert result.report["first_bear_snap_turn_watch_verdict"] == "FIRST_BEAR_SNAP_TURN_SIGNAL_READY_NO_SUBMIT"
    assert result.report["signal_source"] == "FIRST_BEAR_SNAP_TURN_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert calls.readiness == 1
    assert result.report["readiness_invoked"] is True


def test_demo_wiring_signal_cannot_drive_first_bull_snap_turn_paper_path(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="FIRST_BULL_SNAP_TURN_V1",
            rule_mode="FIRST_BULL_SNAP_TURN_V1",
            emit_signal=True,
            strategy_id="FIRST_BULL_SNAP_TURN_V1",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=demo_strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-first-bull-reject-demo",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_NOT_READY
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False
    assert "DEMO/proof signals cannot drive this path" in result.report["required_next_action"]
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_first_bull_snap_turn_long_signal_requires_buy_side_for_paper_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="FIRST_BULL_SNAP_TURN_V1",
            rule_mode="FIRST_BULL_SNAP_TURN_V1",
            emit_signal=True,
            strategy_id="FIRST_BULL_SNAP_TURN_V1",
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=first_snap_turn_strategy_result(
                tmp_path,
                strategy_id="FIRST_BULL_SNAP_TURN_V1",
                decision="LONG",
                emitted=True,
                signal_direction="LONG",
            ),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-first-bull-side-mismatch",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "requires --side BUY" in str(result.report["primary_blocker"])
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_asian_drift_short_signal_requires_sell_side_for_paper_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=asian_drift_state_payload(
                asia_drift_state="ENTRY_ARMED",
                asia_drift_regime="ASIA_DRIFT_SHORT",
                direction="SHORT",
                hypothetical_entry_ready=True,
            ),
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            emit_signal=True,
            strategy_id="asian_drift_v1",
            side="BUY",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=asian_strategy_result(tmp_path, decision="SHORT", emitted=True, signal_direction="SHORT"),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-asian-drift-short-side-mismatch",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "requires --side SELL" in str(result.report["primary_blocker"])
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_pause_resume_short_missing_fields_reports_not_ready_and_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    blocked_strategy = strategy_result(
        tmp_path,
        verdict="TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_INVALID_INPUT",
        decision="NO_SIGNAL",
        emitted=False,
    )
    blocked_strategy.report["primary_blocker"] = "Asia Early pause-resume short v1 requires explicit state and feature envelopes."
    blocked_strategy.report["asia_early_pause_resume_short_watch_verdict"] = "ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    blocked_strategy.report_json.write_text(json.dumps(blocked_strategy.report), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload={"account_id": "DUM882026", "contract_key": "MGC-202606", "close": "4575.3"},
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        ),
        stages=stages(calls=calls, strategy=blocked_strategy),
        runner_id="paper-pause-resume-not-ready",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY
    assert result.report["asia_early_pause_resume_short_watch_verdict"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_pause_resume_short_no_signal_reports_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        ),
        stages=stages(calls=calls, strategy=pause_resume_short_strategy_result(tmp_path, decision="NO_SIGNAL", emitted=False)),
        runner_id="paper-pause-resume-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION
    assert result.report["asia_early_pause_resume_short_watch_verdict"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION"
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["signal_emitted"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_pause_resume_short_signal_without_paper_flags_reports_signal_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        ),
        stages=stages(calls=calls, strategy=pause_resume_short_strategy_result(tmp_path, decision="SHORT", emitted=True)),
        runner_id="paper-pause-resume-signal-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT
    assert result.report["asia_early_pause_resume_short_watch_verdict"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT"
    assert result.report["signal_source"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_pause_resume_short_signal_with_paper_flags_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=stages(
            calls=calls,
            strategy=pause_resume_short_strategy_result(tmp_path, decision="SHORT", emitted=True),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-pause-resume-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert result.report["signal_source"] == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert calls.readiness == 1
    assert result.report["readiness_invoked"] is True


def test_demo_wiring_signal_cannot_drive_pause_resume_short_paper_path(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=stages(
            calls=calls,
            strategy=demo_strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-pause-resume-reject-demo",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False
    assert "DEMO/proof signals cannot drive this path" in result.report["required_next_action"]
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_pause_resume_short_signal_requires_registry_paper_eligible(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=stages(
            calls=calls,
            strategy=pause_resume_short_strategy_result(tmp_path, decision="SHORT", emitted=True, paper_eligible=False),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-pause-resume-reject-paper-ineligible",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY
    assert "paper_eligible=true" in str(result.report["primary_blocker"])
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_pause_resume_short_signal_requires_sell_side_for_paper_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            side="BUY",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.0",
            manual_close_limit_price="4575.3",
        ),
        stages=stages(
            calls=calls,
            strategy=pause_resume_short_strategy_result(tmp_path, decision="SHORT", emitted=True),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-pause-resume-short-side-mismatch",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "requires --side SELL" in str(result.report["primary_blocker"])
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_breakout_retest_hold_long_missing_fields_reports_not_ready_and_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    blocked_strategy = strategy_result(
        tmp_path,
        verdict="TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_INVALID_INPUT",
        decision="NO_SIGNAL",
        emitted=False,
    )
    blocked_strategy.report["primary_blocker"] = "Asia Early normal breakout-retest-hold long v1 requires explicit state and feature envelopes."
    blocked_strategy.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] = (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    )
    blocked_strategy.report_json.write_text(json.dumps(blocked_strategy.report), encoding="utf-8")

    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload={"account_id": "DUM882026", "contract_key": "MGC-202606", "close": "4575.3"},
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        ),
        stages=stages(calls=calls, strategy=blocked_strategy),
        runner_id="paper-breakout-retest-not-ready",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY
    assert result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] == (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    )
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_breakout_retest_hold_long_no_signal_reports_no_mutation(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        ),
        stages=stages(calls=calls, strategy=breakout_retest_hold_long_strategy_result(tmp_path, decision="NO_SIGNAL", emitted=False)),
        runner_id="paper-breakout-retest-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION
    assert result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] == (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION"
    )
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert result.report["signal_emitted"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_breakout_retest_hold_long_signal_without_paper_flags_reports_signal_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        ),
        stages=stages(calls=calls, strategy=breakout_retest_hold_long_strategy_result(tmp_path, decision="LONG", emitted=True)),
        runner_id="paper-breakout-retest-signal-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT
    assert result.report["asia_early_normal_breakout_retest_hold_long_watch_verdict"] == (
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT"
    )
    assert result.report["signal_source"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_breakout_retest_hold_long_signal_with_paper_flags_blocks_legacy_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            side="BUY",
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=breakout_retest_hold_long_strategy_result(tmp_path, decision="LONG", emitted=True),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-breakout-retest-proof-passed",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)
    assert result.report["signal_source"] == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    assert result.report["real_strategy_signal"] is True
    assert result.report["strategy_registry_paper_eligible"] is True
    assert result.report["strategy_registry_live_money_eligible"] is False
    assert calls.readiness == 1


def test_demo_wiring_signal_cannot_drive_breakout_retest_hold_long_paper_path(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            side="BUY",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=demo_strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-breakout-retest-reject-demo",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False
    assert "DEMO/proof signals cannot drive this path" in result.report["required_next_action"]
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_breakout_retest_hold_long_signal_requires_registry_paper_eligible(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            side="BUY",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=breakout_retest_hold_long_strategy_result(tmp_path, decision="LONG", emitted=True, paper_eligible=False),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-breakout-retest-reject-paper-ineligible",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY
    assert "paper_eligible=true" in str(result.report["primary_blocker"])
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False


def test_breakout_retest_hold_long_signal_requires_buy_side_for_paper_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            emit_signal=True,
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            side="SELL",
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=breakout_retest_hold_long_strategy_result(tmp_path, decision="LONG", emitted=True),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-breakout-retest-long-side-mismatch",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "requires --side BUY" in str(result.report["primary_blocker"])
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_full_history_feature_rule_ready_without_submit_flags_stays_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            candle_history_payload={"candles": []},
            current_quote_report_payload={"current_quote_available": True},
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            candle_history=candle_history_producer_result(tmp_path),
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-full-real-rule-ready-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert calls.candle_history == 1
    assert calls.market_history == 1
    assert calls.feature == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["paper_submit_requested"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_history_producer_blocked_stops_before_collector_feature_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            candle_history_payload={"candles": []},
            current_quote_report_payload={"current_quote_available": True},
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            candle_history=candle_history_producer_result(tmp_path, ready=False),
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-history-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER
    assert calls.candle_history == 1
    assert calls.market_history == 0
    assert calls.feature == 0
    assert calls.strategy == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["candle_history_producer_verdict"] == "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_BLOCKED_INSUFFICIENT_CANDLES"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_market_history_blocked_stops_before_feature_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            candle_history_payload={"candles": []},
            current_quote_report_payload={"current_quote_available": True},
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            candle_history=candle_history_producer_result(tmp_path),
            market_history=market_history_result(tmp_path, ready=False),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-market-history-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER
    assert calls.candle_history == 1
    assert calls.market_history == 1
    assert calls.feature == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["market_history_collector_verdict"] == "TRACK_B_MARKET_HISTORY_BLOCKED_INSUFFICIENT_HISTORY"
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_history_producer_request_requires_current_quote_report_before_any_stage(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            candle_history_payload={"candles": []},
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            candle_history=candle_history_producer_result(tmp_path),
            market_history=market_history_result(tmp_path),
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-history-missing-current-quote",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER
    assert calls.candle_history == 0
    assert calls.market_history == 0
    assert calls.feature == 0
    assert calls.strategy == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert "current quote report" in str(result.report["primary_blocker"])
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_feature_builder_signal_readiness_green_without_submit_flags_stays_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            build_features_from_payload={"candle_items": []},
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            feature=feature_result(tmp_path),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-feature-ready-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert calls.feature == 1
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["feature_builder_invoked"] is True
    assert result.report["paper_submit_requested"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_feature_builder_blocked_stops_before_strategy_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            build_features_from_payload={"candle_items": []},
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            feature=feature_result(tmp_path, ready=False),
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-feature-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER
    assert calls.feature == 1
    assert calls.strategy == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["feature_builder_verdict"] == "TRACK_B_FEATURE_BUILDER_BLOCKED_INSUFFICIENT_FEATURE_HISTORY"
    assert result.report["signal_emitted"] is False
    assert result.report["readiness_invoked"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_missing_feature_event_path_blocks_cleanly_before_strategy_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    missing_feature_event = tmp_path / "missing_feature_event.json"
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            input_event_payload=None,
            feature_event_json=missing_feature_event,
            emit_signal=True,
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
        ),
        runner_id="paper-missing-feature-event",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER
    assert calls.feature == 0
    assert calls.strategy == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["feature_event_path"] == str(missing_feature_event)
    assert "Feature event JSON does not exist" in str(result.report["primary_blocker"])
    assert "track_b_feature_builder_cli" in str(result.report["required_next_action"])
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_ambiguous_paper_proof_path_is_disabled_before_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED),
        ),
        runner_id="paper-proof-ambiguous",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)


def test_flat_but_close_provenance_incomplete_paper_proof_path_is_disabled_before_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            paper_execution_path="PAPER_PROOF_DEBUG",
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE),
        ),
        runner_id="paper-proof-flat-provenance",
        now=aware_now(),
    )

    assert_legacy_paper_proof_disabled(result, calls)


def test_non_paper_mode_refuses_before_strategy(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, mode="LIVE"),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path)),
        runner_id="paper-non-paper-mode",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_NON_PAPER_MODE
    assert calls.feature == 0
    assert calls.strategy == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_submit_requested_missing_manual_prices_or_quantity_refuses_before_strategy(tmp_path: Path) -> None:
    calls = Calls()
    missing_quantity = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            submit_paper=True,
            confirm_paper_submit=True,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path)),
        runner_id="paper-missing-quantity",
        now=aware_now(),
    )
    missing_price = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
        ),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path)),
        runner_id="paper-missing-price",
        now=aware_now(),
    )

    assert missing_quantity.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "--quantity" in str(missing_quantity.report["primary_blocker"])
    assert missing_price.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "--manual-close-limit-price" in str(missing_price.report["primary_blocker"])
    assert calls.feature == 0
    assert calls.strategy == 0
    assert calls.proof == 0
    assert missing_price.report["submit_attempted"] is False


def test_explicit_submit_requires_paper_account_contract_guards(tmp_path: Path) -> None:
    calls = Calls()
    wrong_account = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
            account_id="NOT_DUM882026",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-submit-wrong-account-blocked",
        now=aware_now(),
    )
    wrong_contract = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
            contract_key="MES-202606",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-submit-wrong-contract-blocked",
        now=aware_now(),
    )

    assert wrong_account.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "DUM882026" in str(wrong_account.report["primary_blocker"])
    assert wrong_contract.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "MGC-202606" in str(wrong_contract.report["primary_blocker"])
    assert calls.strategy == 0
    assert calls.readiness == 0
    assert calls.proof == 0
    assert wrong_account.report["paper_proof_invoked"] is False
    assert wrong_contract.report["paper_proof_invoked"] is False
    assert wrong_account.report["submit_attempted"] is False
    assert wrong_contract.report["submit_attempted"] is False
    assert wrong_account.report["live_money_readiness"] is False
    assert wrong_contract.report["live_money_readiness"] is False


def test_cli_dry_run_no_signal_does_not_submit(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    event_json = tmp_path / "event.json"
    event_json.write_text(
        json.dumps(
            {
                "account_id": "DUM882026",
                "contract_key": "MGC-202606",
                "strategy_id": "track_b_example_gold_shadow_v1",
                "lane_id": "mgc_example_long_lmt_day",
                "candle_timestamp": aware_now().isoformat(),
                "observed_at": aware_now().isoformat(),
                "close": "4575.3",
                "metadata": {
                    "fixture": True,
                    "ema_momentum_features": {
                        "vwap": "4576.0",
                        "prior_close": "4575.8",
                        "momentum_norm": "0.01",
                        "momentum_acceleration": "0.0",
                        "momentum_turning_positive": True,
                    },
                },
            }
        ),
        encoding="utf-8",
    )

    exit_code = strategy_paper_runner_cli_main(
        [
            "--mode",
            "PAPER",
            "--input-event-json",
            str(event_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--allow-fixture-input",
            "--output-root",
            str(tmp_path / "paper_runner"),
            "--strategy-rule-output-root",
            str(tmp_path / "rule_runner"),
            "--strategy-adapter-output-root",
            str(tmp_path / "adapter"),
            "--candle-producer-output-root",
            str(tmp_path / "candle"),
            "--writer-output-root",
            str(tmp_path / "writer"),
            "--operator-status-output-root",
            str(tmp_path / "operator_status"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["strategy_paper_runner_verdict"] in {
        "TRACK_B_STRATEGY_PAPER_RUNNER_NO_SIGNAL",
        "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_READY_NO_SUBMIT_REQUESTED",
    }
    assert output["paper_proof_invoked"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
