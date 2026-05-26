"""Track B controlled PAPER strategy execution runner.

This Phase 2 boundary is the explicit handoff from a strategy signal to
readiness and Track B PAPER lifecycle management. It defaults to dry-run/no
submit. Real strategy signals route to the strategy-managed lifecycle; legacy
paper-proof debug/canary routes are accepted only to fail closed.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .candle_signal_producer import DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT
from .harness import HarnessResult
from .models import TerminalClassification, require_aware_datetime, to_jsonable
from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, create_operator_status_summary
from .paper_proof import DEFAULT_PAPER_PROOF_OUTPUT_ROOT, PaperProofResult, ProofRunner
from .track_b_continuation_aware_exit_decision import (
    ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1,
    ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1,
    BREAKOUT_RETEST_CONTINUATION_HOLD_V1,
    SNAP_TURN_FAST_DECAY_V1,
)
from .track_b_strategy_managed_paper_lifecycle import (
    DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT,
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
    DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT,
    DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT,
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
    REPO_ROOT,
    TrackBManagedPaperLifecycleClassification,
    TrackBStrategyManagedPaperLifecycleConfig,
    TrackBStrategyManagedPaperLifecycleResult,
    run_track_b_strategy_managed_paper_lifecycle,
)
from .track_b_strategy_trade_intent import (
    DEFAULT_TRACK_B_STRATEGY_TRADE_INTENT_OUTPUT_ROOT,
    TrackBStrategyTradeIntentClassification,
    TrackBStrategyTradeIntentConfig,
    TrackBStrategyTradeIntentResult,
    create_track_b_strategy_trade_intent,
)
from .track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    update_track_b_paper_trade_ledger_from_runner_report,
)
from .preflight import ReadOnlyPreflightConfig
from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT
from .strategy_signal_adapter import DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT
from .track_b_feature_builder import (
    DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT,
    TrackBFeatureBuilderResult,
    TrackBFeatureBuilderVerdict,
    build_track_b_mgc_feature_event,
)
from .track_b_data_maintenance import DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT
from .track_b_market_history import (
    DEFAULT_TRACK_B_MARKET_HISTORY_OUTPUT_ROOT,
    TrackBMarketHistoryResult,
    TrackBMarketHistoryVerdict,
    collect_track_b_mgc_market_history,
)
from .track_b_mgc_candle_history_producer import (
    DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT,
    TrackBMgcCandleHistoryProducerResult,
    TrackBMgcCandleHistoryProducerVerdict,
    produce_track_b_mgc_candle_history_input,
)
from .track_b_readiness_check_runner import (
    DEFAULT_READINESS_CHECK_RUNNER_OUTPUT_ROOT,
    TrackBReadinessCheckRunnerConfig,
    TrackBReadinessCheckRunnerResult,
    TrackBReadinessCheckRunnerStages,
    TrackBReadinessCheckRunnerVerdict,
    run_track_b_readiness_check,
)
from .track_b_strategy_rule_runner import (
    DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
    DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT,
    TrackBStrategyRuleRunnerResult,
    run_track_b_strategy_rule,
)


DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_strategy_paper_runner")
LEGACY_PAPER_PROOF_DISABLED_REASON = (
    "Legacy paper_proof execution paths are disabled for Track B strategy runtime. "
    "Use STRATEGY_MANAGED with Control Plane Snapshot / Safe-State / generation-scoped authorization."
)


class TrackBStrategyPaperRunnerVerdict(str, Enum):
    NO_SIGNAL = "TRACK_B_STRATEGY_PAPER_RUNNER_NO_SIGNAL"
    HUMAN_REVIEW_NO_SIGNAL = "TRACK_B_STRATEGY_PAPER_RUNNER_HUMAN_REVIEW_NO_SIGNAL"
    ASIAN_DRIFT_NOT_READY_FOR_TONIGHT = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION"
    ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT"
    ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION"
    ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT = "TRACK_B_STRATEGY_PAPER_RUNNER_ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT"
    FIRST_BULL_SNAP_TURN_NOT_READY = "TRACK_B_STRATEGY_PAPER_RUNNER_FIRST_BULL_SNAP_TURN_NOT_READY"
    FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION = "TRACK_B_STRATEGY_PAPER_RUNNER_FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION"
    FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT = "TRACK_B_STRATEGY_PAPER_RUNNER_FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT"
    FIRST_BEAR_SNAP_TURN_NOT_READY = "TRACK_B_STRATEGY_PAPER_RUNNER_FIRST_BEAR_SNAP_TURN_NOT_READY"
    FIRST_BEAR_SNAP_TURN_NO_SIGNAL_NO_MUTATION = "TRACK_B_STRATEGY_PAPER_RUNNER_FIRST_BEAR_SNAP_TURN_NO_SIGNAL_NO_MUTATION"
    FIRST_BEAR_SNAP_TURN_SIGNAL_READY_NO_SUBMIT = "TRACK_B_STRATEGY_PAPER_RUNNER_FIRST_BEAR_SNAP_TURN_SIGNAL_READY_NO_SUBMIT"
    BLOCKED_NON_PAPER_MODE = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_NON_PAPER_MODE"
    BLOCKED_INVALID_SUBMIT_REQUEST = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_INVALID_SUBMIT_REQUEST"
    BLOCKED_DATA_MAINTENANCE = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_DATA_MAINTENANCE_STALE_OR_INSUFFICIENT"
    BLOCKED_FEATURE_BUILDER = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_FEATURE_BUILDER"
    BLOCKED_STRATEGY_RULE = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_STRATEGY_RULE"
    BLOCKED_READINESS = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_READINESS"
    PAPER_READY_NO_SUBMIT_REQUESTED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_READY_NO_SUBMIT_REQUESTED"
    PAPER_PROOF_PASSED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED"
    PAPER_PROOF_BLOCKED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_BLOCKED"
    PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
    PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED"
    STRATEGY_MANAGED_LIFECYCLE_NOT_AVAILABLE = "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_LIFECYCLE_NOT_AVAILABLE"
    STRATEGY_MANAGED_EXIT_POLICY_MISSING = "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_EXIT_POLICY_MISSING"
    STRATEGY_MANAGED_OPEN_MANAGED = "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_OPEN_MANAGED"
    STRATEGY_MANAGED_EXIT_PENDING = "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_EXIT_PENDING"
    STRATEGY_MANAGED_CLOSED_FLAT = "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_CLOSED_FLAT"
    STRATEGY_MANAGED_REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_REVIEW_REQUIRED"
    BLOCKED_STAGE_ERROR = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_STAGE_ERROR"


@dataclass(frozen=True)
class TrackBStrategyPaperRunnerConfig:
    mode: str = "PAPER"
    input_event_json: Path | None = None
    input_event_payload: Mapping[str, object] | None = None
    candle_history_json: Path | None = None
    candle_history_payload: Mapping[str, object] | None = None
    current_quote_report_json: Path | None = None
    current_quote_report_payload: Mapping[str, object] | None = None
    runtime_candle_context_json: Path | None = None
    runtime_candle_context_payload: Mapping[str, object] | None = None
    maintained_history_json: Path | None = None
    maintained_history_payload: Mapping[str, object] | None = None
    build_features_from_json: Path | None = None
    build_features_from_payload: Mapping[str, object] | None = None
    feature_event_json: Path | None = None
    inbox_dir: Path = Path("examples/track_b_shadow_listener/inbox")
    source_id: str = "track_b_strategy_paper_runner"
    strategy_id: str = "track_b_example_gold_shadow_v1"
    lane_id: str = "mgc_example_long_lmt_day"
    rule_id: str = DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID
    rule_mode: str = "MGC_EMA_MOMENTUM_RECLAIM_LONG"
    emit_signal: bool = False
    allow_fixture_input: bool = False
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17086
    account_id: str = "DUM882026"
    expected_account_id: str = "DUM882026"
    contract_key: str = "MGC-202606"
    side: str = "BUY"
    quantity: int | None = None
    order_type: str = "LMT"
    time_in_force: str = "DAY"
    submit_paper: bool = False
    confirm_paper_submit: bool = False
    manual_open_limit_price: str | Decimal | None = None
    manual_close_limit_price: str | Decimal | None = None
    paper_execution_path: str = "STRATEGY_MANAGED"
    managed_exit_policy_id: str | None = None
    runtime_decision_source: str = "DATABENTO_LIVE_ARTIFACT"
    paper_order_pricing_policy: str | None = None
    strategy_trade_intent_output_root: Path = DEFAULT_TRACK_B_STRATEGY_TRADE_INTENT_OUTPUT_ROOT
    managed_lifecycle_output_root: Path = DEFAULT_TRACK_B_STRATEGY_MANAGED_PAPER_LIFECYCLE_OUTPUT_ROOT
    broker_order_id: str | None = None
    perm_id: str | None = None
    market_data_mode: str = "DELAYED"
    databento_continuous_symbol: str = "MGC.v.0"
    dataset: str = "GLBX.MDP3"
    allowlisted_local_symbol: str = "MGCM6"
    con_id: int | None = 712565978
    tick_size: str = "0.1"
    exchange: str = "COMEX"
    currency: str = "USD"
    timeframe: str = "quote_snapshot"
    proof_timing_status: str = "ACTIVE_SESSION"
    proof_timing_source: str = "track_b_strategy_paper_runner"
    proof_timing_detail: str | None = None
    max_wait_cycles: int = 1
    wait_poll_seconds: float = 0.0
    max_current_quote_age_seconds: int | None = None
    quote_provider_mode: str = "REALTIME"
    realtime_receive_timeout_seconds: float = 10.0
    request_timeout_seconds: float = 10.0
    quote_timeout_seconds: float = 3.0
    output_root: Path = DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT
    data_maintenance_output_root: Path = DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT
    max_maintained_history_age_seconds: int = 900
    allow_stale_maintained_history_paper: bool = False
    runtime_intraday_freshness_policy: str = "NOT_REQUESTED"
    runtime_candle_context_required: bool = False
    candle_history_producer_output_root: Path = DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT
    candle_history_max_candles: int = 50
    candle_history_min_candles: int = 3
    market_history_output_root: Path = DEFAULT_TRACK_B_MARKET_HISTORY_OUTPUT_ROOT
    market_history_max_candles: int = 50
    market_history_min_candles: int = 3
    feature_builder_output_root: Path = DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT
    feature_builder_min_history_candles: int = 3
    feature_builder_ema_span: int = 3
    strategy_rule_output_root: Path = DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT
    strategy_adapter_output_root: Path = DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT
    candle_producer_output_root: Path = DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT
    writer_output_root: Path = DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT
    readiness_output_root: Path = DEFAULT_READINESS_CHECK_RUNNER_OUTPUT_ROOT
    recovery_output_root: Path = Path("outputs/track_b_execution_core/recovery_status")
    preflight_output_root: Path = Path("outputs/track_b_execution_core/preflight")
    databento_observer_output_root: Path = Path("outputs/track_b_execution_core/databento_candle_observer")
    current_quote_output_root: Path = Path("outputs/track_b_execution_core/current_quotes")
    readiness_summary_output_root: Path = Path("outputs/track_b_execution_core/readiness_summary")
    paper_proof_output_root: Path = DEFAULT_PAPER_PROOF_OUTPUT_ROOT
    paper_trade_ledger_output_root: Path | None = None
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT
    repo_root: Path = REPO_ROOT
    runtime_generation_id: str | None = None
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    runtime_safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    expected_control_plane_snapshot_id: str | None = None
    expected_shared_truth_generation_id: str | None = None
    managed_lifecycle_id: str | None = None


@dataclass(frozen=True)
class TrackBStrategyPaperRunnerStages:
    candle_history_producer: Callable[[TrackBStrategyPaperRunnerConfig], TrackBMgcCandleHistoryProducerResult]
    market_history_collector: Callable[[TrackBStrategyPaperRunnerConfig, TrackBMgcCandleHistoryProducerResult], TrackBMarketHistoryResult]
    market_history_from_payload: Callable[[TrackBStrategyPaperRunnerConfig, Mapping[str, object], Path | None], TrackBMarketHistoryResult]
    feature_builder: Callable[[TrackBStrategyPaperRunnerConfig], TrackBFeatureBuilderResult]
    strategy_rule: Callable[[TrackBStrategyPaperRunnerConfig], TrackBStrategyRuleRunnerResult]
    readiness: Callable[[TrackBStrategyPaperRunnerConfig], TrackBReadinessCheckRunnerResult]
    strategy_trade_intent: Callable[[TrackBStrategyPaperRunnerConfig, Mapping[str, object]], TrackBStrategyTradeIntentResult]
    managed_lifecycle: Callable[[TrackBStrategyPaperRunnerConfig, Mapping[str, object]], TrackBStrategyManagedPaperLifecycleResult]
    paper_proof: Callable[[TrackBStrategyPaperRunnerConfig], PaperProofResult]
    operator_status: Callable[[TrackBStrategyPaperRunnerConfig, Path], None]


@dataclass(frozen=True)
class TrackBStrategyPaperRunnerResult:
    verdict: TrackBStrategyPaperRunnerVerdict
    report_json: Path
    report: dict[str, object]
    candle_history_producer_result: TrackBMgcCandleHistoryProducerResult | None = None
    market_history_result: TrackBMarketHistoryResult | None = None
    feature_builder_result: TrackBFeatureBuilderResult | None = None
    strategy_rule_result: TrackBStrategyRuleRunnerResult | None = None
    readiness_result: TrackBReadinessCheckRunnerResult | None = None
    managed_lifecycle_result: TrackBStrategyManagedPaperLifecycleResult | None = None
    strategy_trade_intent_result: TrackBStrategyTradeIntentResult | None = None
    paper_proof_result: PaperProofResult | None = None


def default_stages(
    *,
    readiness_stages: TrackBReadinessCheckRunnerStages | None = None,
    proof_runner: ProofRunner | None = None,
) -> TrackBStrategyPaperRunnerStages:
    return TrackBStrategyPaperRunnerStages(
        candle_history_producer=_run_candle_history_producer,
        market_history_collector=_run_market_history_collector,
        market_history_from_payload=_run_market_history_from_payload,
        feature_builder=_run_feature_builder,
        strategy_rule=_run_strategy_rule,
        readiness=lambda config: _run_readiness(config, readiness_stages=readiness_stages),
        strategy_trade_intent=_run_strategy_trade_intent,
        managed_lifecycle=_run_managed_lifecycle,
        paper_proof=_disabled_legacy_paper_proof,
        operator_status=_run_operator_status,
    )


def run_track_b_strategy_paper(
    *,
    config: TrackBStrategyPaperRunnerConfig,
    stages: TrackBStrategyPaperRunnerStages | None = None,
    runner_id: str | None = None,
    now: datetime | None = None,
) -> TrackBStrategyPaperRunnerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_runner_id = runner_id or f"track_b_strategy_paper_runner_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_runner_id / "track_b_strategy_paper_runner_report.json"
    actual_stages = stages or default_stages()
    candle_history_producer: TrackBMgcCandleHistoryProducerResult | None = None
    market_history: TrackBMarketHistoryResult | None = None
    feature_builder: TrackBFeatureBuilderResult | None = None
    strategy_rule: TrackBStrategyRuleRunnerResult | None = None
    readiness: TrackBReadinessCheckRunnerResult | None = None
    strategy_trade_intent: TrackBStrategyTradeIntentResult | None = None
    managed_lifecycle: TrackBStrategyManagedPaperLifecycleResult | None = None
    proof: PaperProofResult | None = None

    try:
        mode_error = _mode_error(config)
        if mode_error:
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_NON_PAPER_MODE,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                strategy_trade_intent=strategy_trade_intent,
                proof=proof,
                primary_blocker=mode_error,
                required_next_action="Set --mode PAPER. Live-money execution is not implemented.",
                operator_status_stage=actual_stages.operator_status,
            )

        submit_error = _submit_request_error(config)
        if submit_error:
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                strategy_trade_intent=strategy_trade_intent,
                proof=proof,
                primary_blocker=submit_error,
                required_next_action="Provide explicit PAPER submit flags, quantity, and manual open/close limit prices before retrying.",
                operator_status_stage=actual_stages.operator_status,
            )

        candle_history_request_error = None if _maintained_history_requested(config) or _runtime_candle_context_requested(config) else _candle_history_producer_request_error(config)
        if candle_history_request_error:
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                strategy_trade_intent=strategy_trade_intent,
                proof=proof,
                primary_blocker=candle_history_request_error,
                required_next_action="Provide both bounded candle history and a current quote report before running the history producer.",
                operator_status_stage=actual_stages.operator_status,
            )

        strategy_config = config
        feature_config = config
        if _runtime_candle_context_requested(config):
            runtime_payload_error = _runtime_candle_context_request_error(config)
            if runtime_payload_error:
                return _finalize(
                    config=config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=runtime_payload_error,
                    required_next_action="Provide a valid bounded runtime candle context JSON before feature building.",
                    operator_status_stage=actual_stages.operator_status,
                )
            runtime_payload = _runtime_candle_context_payload(config)
            market_history = actual_stages.market_history_from_payload(config, runtime_payload, config.runtime_candle_context_json)
            if market_history.verdict != TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT or market_history.history_event is None:
                return _finalize(
                    config=config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=market_history.report.get("primary_blocker") or "Runtime candle context did not produce a feature-builder event.",
                    required_next_action=str(market_history.report.get("required_next_action") or "Resolve runtime candle context blocker before feature building."),
                    operator_status_stage=actual_stages.operator_status,
                )
            feature_config = replace(
                config,
                build_features_from_payload=market_history.history_event,
                build_features_from_json=market_history.history_event_json,
            )
        elif _maintained_history_requested(config):
            maintained_history_error = _maintained_history_request_error(config, now=actual_now)
            if maintained_history_error:
                return _finalize(
                    config=config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=maintained_history_error,
                    required_next_action="Run track_b_data_maintenance_cli until latest_good_mgc_1m_history.json is ready, and provide a separate realtime current quote report.",
                    operator_status_stage=actual_stages.operator_status,
                )
            maintained_payload = _maintained_history_market_payload(config, now=actual_now)
            market_history = actual_stages.market_history_from_payload(config, maintained_payload, config.maintained_history_json)
            if market_history.verdict != TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT or market_history.history_event is None:
                return _finalize(
                    config=config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_DATA_MAINTENANCE,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=market_history.report.get("primary_blocker") or "Maintained market history did not produce a feature-builder event.",
                    required_next_action=str(market_history.report.get("required_next_action") or "Resolve maintained history blocker before feature building."),
                    operator_status_stage=actual_stages.operator_status,
                )
            feature_config = replace(
                config,
                build_features_from_payload=market_history.history_event,
                build_features_from_json=market_history.history_event_json,
            )
        elif _candle_history_producer_requested(config):
            candle_history_producer = actual_stages.candle_history_producer(config)
            if (
                candle_history_producer.verdict != TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT
                or candle_history_producer.history_input is None
            ):
                return _finalize(
                    config=config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=candle_history_producer.report.get("primary_blocker") or "Candle-history producer did not produce bounded strategy input.",
                    required_next_action=str(candle_history_producer.report.get("required_next_action") or "Resolve candle-history producer blocker before feature building."),
                    operator_status_stage=actual_stages.operator_status,
                )
            market_history = actual_stages.market_history_collector(config, candle_history_producer)
            if market_history.verdict != TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT or market_history.history_event is None:
                return _finalize(
                    config=config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=market_history.report.get("primary_blocker") or "Market-history collector did not produce a feature-builder event.",
                    required_next_action=str(market_history.report.get("required_next_action") or "Resolve market-history collector blocker before feature building."),
                    operator_status_stage=actual_stages.operator_status,
                )
            feature_config = replace(
                config,
                build_features_from_payload=market_history.history_event,
                build_features_from_json=market_history.history_event_json,
            )

        if _feature_builder_requested(feature_config):
            feature_builder = actual_stages.feature_builder(feature_config)
            if feature_builder.verdict != TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT or feature_builder.feature_event is None:
                return _finalize(
                    config=feature_config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=feature_builder.report.get("primary_blocker") or "Feature builder did not produce a signal-ready event.",
                    required_next_action=str(feature_builder.report.get("required_next_action") or "Resolve feature builder blocker before strategy evaluation."),
                    operator_status_stage=actual_stages.operator_status,
                )
            strategy_config = replace(feature_config, input_event_payload=feature_builder.feature_event, input_event_json=feature_builder.feature_event_json)
        elif feature_config.feature_event_json is not None:
            if not Path(feature_config.feature_event_json).exists():
                return _finalize(
                    config=feature_config,
                    report_json=report_json,
                    now=actual_now,
                    runner_id=actual_runner_id,
                    verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_FEATURE_BUILDER,
                    candle_history_producer=candle_history_producer,
                    market_history=market_history,
                    feature_builder=feature_builder,
                    strategy_rule=strategy_rule,
                    readiness=readiness,
                    proof=proof,
                    primary_blocker=f"Feature event JSON does not exist: {feature_config.feature_event_json}",
                    required_next_action="Run track_b_feature_builder_cli or provide a valid --feature-event-json before strategy evaluation.",
                    operator_status_stage=actual_stages.operator_status,
                )
            strategy_config = replace(feature_config, input_event_payload=None, input_event_json=feature_config.feature_event_json)

        strategy_rule = actual_stages.strategy_rule(strategy_config)
        strategy_verdict = str(strategy_rule.report.get("strategy_rule_runner_verdict") or "")
        if strategy_verdict.startswith("TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED"):
            blocked_verdict = _strategy_specific_not_ready_verdict(config) or TrackBStrategyPaperRunnerVerdict.BLOCKED_STRATEGY_RULE
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=blocked_verdict,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=strategy_rule.report.get("primary_blocker") or "Strategy rule runner is blocked.",
                required_next_action=str(strategy_rule.report.get("required_next_action") or "Resolve strategy rule blocker before PAPER handoff."),
                operator_status_stage=actual_stages.operator_status,
            )
        if strategy_rule.report.get("signal_emitted") is not True:
            decision = str(strategy_rule.report.get("decision") or "")
            if _is_asian_drift_rule(config):
                verdict = TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION
            elif _is_pause_resume_short_rule(config):
                verdict = TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION
            elif _is_breakout_retest_hold_long_rule(config):
                verdict = TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION
            elif _is_first_bull_snap_turn_rule(config):
                verdict = TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION
            elif _is_first_bear_snap_turn_rule(config):
                verdict = TrackBStrategyPaperRunnerVerdict.FIRST_BEAR_SNAP_TURN_NO_SIGNAL_NO_MUTATION
            else:
                verdict = (
                    TrackBStrategyPaperRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL
                    if decision == "HUMAN_REVIEW"
                    else TrackBStrategyPaperRunnerVerdict.NO_SIGNAL
                )
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=verdict,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=strategy_rule.report.get("primary_blocker"),
                required_next_action=str(strategy_rule.report.get("required_next_action") or "Review strategy rule report; no paper submit was requested."),
                operator_status_stage=actual_stages.operator_status,
            )
        if not strategy_verdict.endswith("EMITTED_SIGNAL"):
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_STRATEGY_RULE,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=strategy_rule.report.get("primary_blocker") or "Strategy rule did not produce a clean emitted signal.",
                required_next_action=str(strategy_rule.report.get("required_next_action") or "Resolve strategy rule blocker before PAPER handoff."),
                operator_status_stage=actual_stages.operator_status,
            )

        strategy_guard_blocker = _registered_strategy_signal_blocker(config, strategy_rule.report)
        if strategy_guard_blocker is not None:
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=_strategy_specific_not_ready_verdict(config) or TrackBStrategyPaperRunnerVerdict.BLOCKED_STRATEGY_RULE,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=strategy_guard_blocker,
                required_next_action=_strategy_specific_real_signal_next_action(config),
                operator_status_stage=actual_stages.operator_status,
            )

        if _is_watch_only_until_submit_rule(config) and not _paper_submit_requested(config):
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=_strategy_specific_signal_ready_no_submit_verdict(config)
                or TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=None,
                required_next_action=_strategy_specific_signal_ready_next_action(config),
                operator_status_stage=actual_stages.operator_status,
            )

        strategy_side_blocker = _registered_strategy_submit_side_blocker(config, strategy_rule.report)
        if strategy_side_blocker is not None:
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=strategy_side_blocker,
                required_next_action=f"Align --side with the explicit { _expected_signal_source(config) } signal direction before PAPER submit.",
                operator_status_stage=actual_stages.operator_status,
            )

        readiness = _strategy_managed_phase1_readiness(config=config, runner_id=actual_runner_id, now=actual_now)
        if readiness is None:
            readiness = actual_stages.readiness(config)
        if readiness.report.get("runner_verdict") != "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW" or readiness.report.get("readiness_verdict") != "READY_FOR_PAPER_PROOF":
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_READINESS,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=readiness.report.get("primary_blocker") or "Readiness check runner is not ready for PAPER proof.",
                required_next_action=str(readiness.report.get("required_next_action") or "Resolve readiness blocker before PAPER submit."),
                operator_status_stage=actual_stages.operator_status,
            )

        if not _paper_submit_requested(config):
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=None,
                required_next_action="Strategy signal and readiness are green, but PAPER submit flags were not supplied. No submit was attempted.",
                operator_status_stage=actual_stages.operator_status,
            )

        legacy_paper_proof_error = _legacy_paper_proof_path_error(config)
        if legacy_paper_proof_error:
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST,
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                proof=proof,
                primary_blocker=legacy_paper_proof_error,
                required_next_action="Use the strategy-managed lifecycle path; paper_proof is not a runtime submit boundary.",
                operator_status_stage=actual_stages.operator_status,
            )

        managed_exit_policy_id = _managed_exit_policy_id(config, strategy_rule.report if strategy_rule else {})
        managed_config = replace(config, managed_exit_policy_id=managed_exit_policy_id)
        strategy_trade_intent = actual_stages.strategy_trade_intent(managed_config, strategy_rule.report if strategy_rule else {})
        if strategy_trade_intent.classification != TrackBStrategyTradeIntentClassification.CREATED:
            is_missing_exit_policy = (
                strategy_trade_intent.classification
                == TrackBStrategyTradeIntentClassification.BLOCKED_MISSING_EXIT_POLICY
            )
            return _finalize(
                config=config,
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                verdict=(
                    TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_EXIT_POLICY_MISSING
                    if is_missing_exit_policy
                    else TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_LIFECYCLE_NOT_AVAILABLE
                ),
                candle_history_producer=candle_history_producer,
                market_history=market_history,
                feature_builder=feature_builder,
                strategy_rule=strategy_rule,
                readiness=readiness,
                strategy_trade_intent=strategy_trade_intent,
                managed_lifecycle=managed_lifecycle,
                proof=proof,
                primary_blocker=strategy_trade_intent.report.get("primary_blocker")
                or f"{config.strategy_id} strategy trade intent was not created.",
                required_next_action=str(
                    strategy_trade_intent.report.get("required_next_action")
                    or "Resolve strategy trade intent blocker before invoking managed lifecycle."
                ),
                operator_status_stage=actual_stages.operator_status,
            )

        managed_lifecycle = actual_stages.managed_lifecycle(
            managed_config,
            {
                **(strategy_rule.report if strategy_rule else {}),
                "strategy_trade_intent": strategy_trade_intent.report,
                "strategy_trade_intent_path": str(strategy_trade_intent.latest_intent_json),
            },
        )
        verdict = _managed_lifecycle_runner_verdict(managed_lifecycle.classification)
        primary_blocker = managed_lifecycle.report.get("primary_blocker")
        required_next_action = str(
            managed_lifecycle.report.get("required_next_action")
            or "Review strategy-managed PAPER lifecycle report."
        )
        return _finalize(
            config=config,
            report_json=report_json,
            now=actual_now,
            runner_id=actual_runner_id,
            verdict=verdict,
            candle_history_producer=candle_history_producer,
            market_history=market_history,
            feature_builder=feature_builder,
            strategy_rule=strategy_rule,
            readiness=readiness,
            strategy_trade_intent=strategy_trade_intent,
            managed_lifecycle=managed_lifecycle,
            proof=proof,
            primary_blocker=primary_blocker,
            required_next_action=required_next_action,
            operator_status_stage=actual_stages.operator_status,
        )
    except Exception as exc:  # noqa: BLE001 - runner errors must become artifacts.
        return _finalize(
            config=config,
            report_json=report_json,
            now=actual_now,
            runner_id=actual_runner_id,
            verdict=TrackBStrategyPaperRunnerVerdict.BLOCKED_STAGE_ERROR,
            candle_history_producer=candle_history_producer,
            market_history=market_history,
            feature_builder=feature_builder,
            strategy_rule=strategy_rule,
            readiness=readiness,
            proof=proof,
            primary_blocker=f"Strategy PAPER runner stage error: {exc}",
            required_next_action="Review strategy PAPER runner diagnostics before retrying.",
            operator_status_stage=actual_stages.operator_status,
        )


def _run_feature_builder(config: TrackBStrategyPaperRunnerConfig) -> TrackBFeatureBuilderResult:
    payload = _feature_builder_source_payload(config)
    return build_track_b_mgc_feature_event(
        source_event_payload=payload,
        source_event_path=config.build_features_from_json,
        expected_account_id=config.expected_account_id,
        rule_id=config.rule_id,
        output_root=config.feature_builder_output_root,
        source_id=config.source_id,
        min_history_candles=config.feature_builder_min_history_candles,
        ema_span=config.feature_builder_ema_span,
    )


def _run_candle_history_producer(config: TrackBStrategyPaperRunnerConfig) -> TrackBMgcCandleHistoryProducerResult:
    history_payload = _candle_history_payload(config)
    current_quote_payload = _current_quote_report_payload(config)
    return produce_track_b_mgc_candle_history_input(
        history_payload=history_payload,
        current_quote_report_payload=current_quote_payload,
        history_payload_path=config.candle_history_json,
        current_quote_report_path=config.current_quote_report_json,
        expected_account_id=config.expected_account_id,
        strategy_id=config.strategy_id,
        lane_id=config.lane_id,
        contract_key=config.contract_key,
        databento_continuous_symbol=config.databento_continuous_symbol,
        allowlisted_local_symbol=config.allowlisted_local_symbol,
        dataset=config.dataset,
        timeframe="1m" if config.timeframe == "quote_snapshot" else config.timeframe,
        max_candles=config.candle_history_max_candles,
        min_candles=config.candle_history_min_candles,
        source_id=config.source_id,
        output_root=config.candle_history_producer_output_root,
    )


def _run_market_history_collector(
    config: TrackBStrategyPaperRunnerConfig,
    candle_history_producer: TrackBMgcCandleHistoryProducerResult,
) -> TrackBMarketHistoryResult:
    if candle_history_producer.history_input is None:
        raise ValueError("candle history producer did not produce history input.")
    return collect_track_b_mgc_market_history(
        market_history_payload=candle_history_producer.history_input,
        source_payload_path=candle_history_producer.history_input_json,
        expected_account_id=config.expected_account_id,
        contract_key=config.contract_key,
        databento_continuous_symbol=config.databento_continuous_symbol,
        dataset=config.dataset,
        allowlisted_local_symbol=config.allowlisted_local_symbol,
        timeframe="1m" if config.timeframe == "quote_snapshot" else config.timeframe,
        max_candles=config.market_history_max_candles,
        min_candles=config.market_history_min_candles,
        output_root=config.market_history_output_root,
        source_id=config.source_id,
        strategy_id=config.strategy_id,
        lane_id=config.lane_id,
    )


def _run_market_history_from_payload(
    config: TrackBStrategyPaperRunnerConfig,
    payload: Mapping[str, object],
    source_payload_path: Path | None,
) -> TrackBMarketHistoryResult:
    return collect_track_b_mgc_market_history(
        market_history_payload=payload,
        source_payload_path=source_payload_path,
        expected_account_id=config.expected_account_id,
        contract_key=config.contract_key,
        databento_continuous_symbol=config.databento_continuous_symbol,
        dataset=config.dataset,
        allowlisted_local_symbol=config.allowlisted_local_symbol,
        timeframe="1m" if config.timeframe == "quote_snapshot" else config.timeframe,
        max_candles=config.market_history_max_candles,
        min_candles=config.market_history_min_candles,
        output_root=config.market_history_output_root,
        source_id=config.source_id,
        strategy_id=config.strategy_id,
        lane_id=config.lane_id,
    )


def _run_strategy_rule(config: TrackBStrategyPaperRunnerConfig) -> TrackBStrategyRuleRunnerResult:
    payload = _input_event_payload(config)
    return run_track_b_strategy_rule(
        input_event_payload=payload,
        input_event_path=config.input_event_json,
        inbox_dir=config.inbox_dir,
        expected_account_id=config.expected_account_id,
        source_id=config.source_id,
        strategy_id=config.strategy_id,
        lane_id=config.lane_id,
        rule_id=config.rule_id,
        rule_mode=config.rule_mode,
        emit_signal=config.emit_signal,
        allow_fixture_input=config.allow_fixture_input,
        output_root=config.strategy_rule_output_root,
        strategy_adapter_output_root=config.strategy_adapter_output_root,
        candle_producer_output_root=config.candle_producer_output_root,
        writer_output_root=config.writer_output_root,
    )


def _run_readiness(
    config: TrackBStrategyPaperRunnerConfig,
    *,
    readiness_stages: TrackBReadinessCheckRunnerStages | None,
) -> TrackBReadinessCheckRunnerResult:
    readiness_config = TrackBReadinessCheckRunnerConfig(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_key=config.contract_key,
        broker_order_id=config.broker_order_id,
        perm_id=config.perm_id,
        market_data_mode=config.market_data_mode,
        databento_continuous_symbol=config.databento_continuous_symbol,
        dataset=config.dataset,
        allowlisted_local_symbol=config.allowlisted_local_symbol,
        tick_size=config.tick_size,
        exchange=config.exchange,
        currency=config.currency,
        expected_account_id=config.expected_account_id,
        strategy_id=config.strategy_id,
        lane_id=config.lane_id,
        timeframe=config.timeframe,
        source_id=config.source_id,
        proof_timing_status=config.proof_timing_status,
        proof_timing_source=config.proof_timing_source,
        proof_timing_detail=config.proof_timing_detail,
        max_wait_cycles=config.max_wait_cycles,
        wait_poll_seconds=config.wait_poll_seconds,
        max_current_quote_age_seconds=config.max_current_quote_age_seconds,
        quote_provider_mode=config.quote_provider_mode,
        realtime_receive_timeout_seconds=config.realtime_receive_timeout_seconds,
        request_timeout_seconds=config.request_timeout_seconds,
        quote_timeout_seconds=config.quote_timeout_seconds,
        output_root=config.readiness_output_root,
        recovery_output_root=config.recovery_output_root,
        preflight_output_root=config.preflight_output_root,
        databento_observer_output_root=config.databento_observer_output_root,
        current_quote_output_root=config.current_quote_output_root,
        readiness_summary_output_root=config.readiness_summary_output_root,
        operator_status_output_root=config.operator_status_output_root,
    )
    return run_track_b_readiness_check(config=readiness_config, stages=readiness_stages)


def _strategy_managed_phase1_readiness(
    *,
    config: TrackBStrategyPaperRunnerConfig,
    runner_id: str,
    now: datetime,
) -> TrackBReadinessCheckRunnerResult | None:
    if _paper_execution_path(config) != "STRATEGY_MANAGED":
        return None
    if str(config.paper_order_pricing_policy or "").strip().upper() not in {
        "LIMIT_AT_LAST",
        "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT",
        "LIMIT_AT_SIGNAL_PRICE",
    }:
        return None

    quote_payload = _optional_current_quote_payload(config)
    if not quote_payload:
        return None
    blocker = _phase1_runtime_pricing_blocker(config=config, payload=quote_payload, now=now)
    if blocker is not None:
        return None

    report_json = (
        Path(config.readiness_output_root)
        / f"track_b_readiness_check_{runner_id}_phase1_runtime_pricing"
        / "track_b_readiness_check_runner_report.json"
    )
    source_path = quote_payload.get("source_authority_path") or quote_payload.get("source_path")
    if source_path is None and config.current_quote_report_json is not None:
        source_path = str(config.current_quote_report_json)
    report = {
        "runner_id": f"track_b_readiness_check_{runner_id}_phase1_runtime_pricing",
        "generated_at": now.isoformat(),
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_readiness_check_runner_report.json"),
        "runner_verdict": TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW.value,
        "readiness_verdict": "READY_FOR_PAPER_PROOF",
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": (
            "Strategy-managed PAPER submit is using fresh Phase-1 runtime candle pricing evidence; "
            "the final mutation boundary remains Control Plane Snapshot / Safe-State / pre-action validation."
        ),
        "strategy_managed_phase1_runtime_pricing_ready": True,
        "readiness_source": "PHASE1_RUNTIME_MARKET_DATA_STRATEGY_MANAGED_PRICING",
        "current_quote_available": True,
        "realtime_quote_received": True,
        "quote_provider_mode": quote_payload.get("quote_provider_mode") or "REALTIME",
        "quote_freshness_verdict": quote_payload.get("quote_freshness_verdict")
        or quote_payload.get("realtime_feed_block_reason")
        or "PHASE1_RUNTIME_MARKET_DATA_READY",
        "phase1_runtime_market_data_authority": True,
        "source_category": quote_payload.get("source_category") or "PHASE1_RUNTIME_MARKET_DATA",
        "source_authority_path": source_path,
        "symbol": quote_payload.get("symbol"),
        "timeframe": quote_payload.get("timeframe"),
        "latest_bar_timestamp": _phase1_latest_bar_timestamp(quote_payload),
        "live_money_readiness": False,
        "paper_proof_invoked": False,
    }
    _write_report(report_json, report)
    return TrackBReadinessCheckRunnerResult(
        verdict=TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW,
        report_json=report_json,
        report=report,
    )


def _optional_current_quote_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object] | None:
    try:
        return _current_quote_report_payload(config)
    except (TypeError, ValueError, OSError, json.JSONDecodeError):
        return None


def _phase1_runtime_pricing_blocker(
    *,
    config: TrackBStrategyPaperRunnerConfig,
    payload: Mapping[str, object],
    now: datetime,
) -> str | None:
    source = str(payload.get("source") or payload.get("source_category") or payload.get("input_source_category") or "")
    if "PHASE1" not in source and payload.get("phase1_runtime_market_data_authority") is not True:
        return "Strategy-managed readiness bypass only accepts Phase-1 runtime market-data pricing evidence."
    if payload.get("realtime_feed_confirmed") is not True and payload.get("current_quote_available") is not True:
        return "Phase-1 runtime pricing evidence is not realtime/current."
    if payload.get("realtime_feed_block_reason") not in {None, "READY"}:
        return f"Phase-1 runtime pricing evidence is not ready: {payload.get('realtime_feed_block_reason')}."
    if _phase1_latest_bar_timestamp(payload) is None:
        return "Phase-1 runtime pricing evidence is missing latest completed bar timestamp."

    freshness_seconds = _optional_positive_float(payload.get("freshness_seconds"))
    generated_at = _parse_optional_datetime(payload.get("generated_at"))
    if generated_at is not None and freshness_seconds is not None:
        age_seconds = max((now - generated_at).total_seconds(), 0.0)
        if age_seconds > freshness_seconds:
            return f"Phase-1 runtime pricing evidence is stale: age_seconds={age_seconds:.1f}."

    if config.expected_account_id != "DUM882026" or config.account_id != "DUM882026":
        return "Strategy-managed Phase-1 readiness bypass is PAPER-account-only."
    return None


def _phase1_latest_bar_timestamp(payload: Mapping[str, object]) -> str | None:
    for key in ("last_completed_bar_ts", "latest_bar_timestamp", "latest_completed_bar_at"):
        value = payload.get(key)
        if value:
            return str(value)
    bars = payload.get("bars")
    if isinstance(bars, list) and bars:
        last = bars[-1]
        if isinstance(last, Mapping):
            value = last.get("bar_end") or last.get("timestamp") or last.get("candle_timestamp")
            return None if value is None else str(value)
    return None


def _optional_positive_float(value: object) -> float | None:
    try:
        parsed = float(str(value))
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _parse_optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _continuation_exit_evidence_from_strategy_report(
    *,
    config: TrackBStrategyPaperRunnerConfig,
    strategy_report: Mapping[str, object],
) -> dict[str, object]:
    rule_inputs = _mapping_or_empty(strategy_report.get("rule_inputs"))
    rule_conditions = _mapping_or_empty(strategy_report.get("rule_conditions"))
    return {
        "continuation_exit_profile_id": _continuation_exit_profile_id(config.strategy_id, strategy_report),
        "continuation_completed_5m_candles": _completed_5m_candles_from_report(strategy_report),
        "continuation_microtrend_state": _continuation_microtrend_state(config.strategy_id, strategy_report, rule_inputs),
        "continuation_participation_state": _continuation_participation_state(
            config.strategy_id,
            strategy_report,
            rule_conditions,
        ),
        "continuation_position_age_minutes": _first_present(
            strategy_report,
            (
                "continuation_position_age_minutes",
                "position_age_minutes",
                "open_position_age_minutes",
                "managed_position_age_minutes",
            ),
        ),
        "continuation_mfe": _first_present(
            strategy_report,
            ("continuation_mfe", "mfe", "max_favorable_excursion", "open_position_mfe"),
        ),
        "continuation_mae": _first_present(
            strategy_report,
            ("continuation_mae", "mae", "max_adverse_excursion", "open_position_mae"),
        ),
        "continuation_unrealized_pnl": _first_present(
            strategy_report,
            ("continuation_unrealized_pnl", "unrealized_pnl", "open_position_unrealized_pnl"),
        ),
        "continuation_safe_state_classification": _string_or_none(
            _first_present(
                strategy_report,
                ("continuation_safe_state_classification", "safe_state_classification", "safe_state"),
            )
        )
        or "SAFE_STATE_NORMAL",
        "continuation_lifecycle_reconciliation_classification": _string_or_none(
            _first_present(
                strategy_report,
                (
                    "continuation_lifecycle_reconciliation_classification",
                    "lifecycle_reconciliation_classification",
                    "reconciliation_classification",
                    "broker_reconciliation_classification",
                ),
            )
        )
        or "CLEAN",
        "continuation_source_strategy_report_path": _strategy_report_path(strategy_report),
    }


def _continuation_exit_profile_id(strategy_id: str, strategy_report: Mapping[str, object]) -> str | None:
    explicit = _string_or_none(strategy_report.get("continuation_exit_profile_id") or strategy_report.get("exit_profile_id"))
    if explicit:
        return explicit
    return {
        "asian_drift_v1": ASIAN_DRIFT_CONTINUATION_LONG_LEASH_V1,
        "ASIA_EARLY_PAUSE_RESUME_SHORT_V1": ASIA_EARLY_PAUSE_RESUME_SHORT_MEDIUM_LEASH_V1,
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1": BREAKOUT_RETEST_CONTINUATION_HOLD_V1,
        "MNQ_FIRST_BEAR_SNAP_TURN_V1": SNAP_TURN_FAST_DECAY_V1,
        "MNQ_FIRST_BULL_SNAP_TURN_V1": SNAP_TURN_FAST_DECAY_V1,
    }.get(strategy_id)


def _completed_5m_candles_from_report(strategy_report: Mapping[str, object]) -> tuple[Mapping[str, Any], ...]:
    raw = _first_present(
        strategy_report,
        (
            "completed_5m_candles",
            "continuation_completed_5m_candles",
            "input_candle_window",
            "recent_completed_5m_candles",
        ),
    )
    if raw is None:
        input_event = _input_event_from_strategy_report(strategy_report)
        raw = _first_present(
            input_event,
            (
                "completed_5m_candles",
                "continuation_completed_5m_candles",
                "input_candle_window",
                "recent_completed_5m_candles",
            ),
        )
        if raw is None:
            source_payload = _source_payload_from_event(input_event)
            raw = _first_present(
                source_payload,
                (
                    "completed_5m_candles",
                    "candles",
                    "candle_history",
                    "input_candle_window",
                    "recent_completed_5m_candles",
                ),
            )
    if isinstance(raw, Mapping):
        nested = raw.get("completed_5m_candles") or raw.get("candles") or raw.get("items")
        raw = nested if nested is not None else raw
    if isinstance(raw, Mapping):
        return (dict(raw),)
    if isinstance(raw, (str, bytes)) or raw is None:
        return ()
    try:
        return tuple(dict(item) for item in raw if isinstance(item, Mapping))  # type: ignore[union-attr]
    except TypeError:
        return ()


def _continuation_microtrend_state(
    strategy_id: str,
    strategy_report: Mapping[str, object],
    rule_inputs: Mapping[str, object],
) -> Mapping[str, object] | str | None:
    explicit = strategy_report.get("continuation_microtrend_state") or strategy_report.get("microtrend_state")
    if explicit:
        return explicit  # type: ignore[return-value]
    if strategy_id == "asian_drift_v1":
        return {
            "source": "strategy_report.rule_inputs",
            "strategy_family": "asia_drift",
            "asia_drift_state": rule_inputs.get("asia_drift_state"),
            "asia_drift_regime": rule_inputs.get("asia_drift_regime"),
            "direction": rule_inputs.get("direction"),
            "microtrend": _asian_drift_microtrend_label(rule_inputs),
        }
    if strategy_id == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1":
        return {
            "source": "strategy_report.rule_inputs",
            "strategy_family": "pause_resume",
            "direction": "SHORT",
            "derivative_phase": rule_inputs.get("derivative_phase"),
            "microtrend": rule_inputs.get("microtrend_state") or rule_inputs.get("trend_state"),
        }
    if strategy_id == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1":
        return {
            "source": "strategy_report.rule_inputs",
            "strategy_family": "breakout_retest",
            "direction": "LONG",
            "derivative_phase": rule_inputs.get("derivative_phase"),
            "breakout_normalized_slope": rule_inputs.get("breakout_normalized_slope"),
            "breakout_range_expansion_ratio": rule_inputs.get("breakout_range_expansion_ratio"),
            "microtrend": _breakout_retest_microtrend_label(rule_inputs, _mapping_or_empty(strategy_report.get("rule_conditions"))),
            "healthy_pullback_tolerant": True,
            "runtime_behavior_changed": False,
        }
    if strategy_id in {"MNQ_FIRST_BEAR_SNAP_TURN_V1", "MNQ_FIRST_BULL_SNAP_TURN_V1"}:
        return {
            "source": "strategy_report.rule_inputs",
            "strategy_family": "snap_turn",
            "direction": "SHORT" if strategy_id == "MNQ_FIRST_BEAR_SNAP_TURN_V1" else "LONG",
            "derivative_phase": rule_inputs.get("derivative_phase"),
            "session_allowed": rule_inputs.get("session_allowed"),
            "microtrend": _snap_turn_microtrend_label(strategy_id, _mapping_or_empty(strategy_report.get("rule_conditions"))),
            "fast_decay_sensitive": True,
            "runtime_behavior_changed": False,
        }
    return None


def _continuation_participation_state(
    strategy_id: str,
    strategy_report: Mapping[str, object],
    rule_conditions: Mapping[str, object],
) -> Mapping[str, object] | str | None:
    explicit = strategy_report.get("continuation_participation_state") or strategy_report.get("participation_state")
    if explicit:
        return explicit  # type: ignore[return-value]
    if strategy_id == "asian_drift_v1":
        return {
            "source": "strategy_report.rule_conditions",
            "strategy_family": "asia_drift",
            "participation": _participation_label(rule_conditions),
            "conditions": dict(rule_conditions),
        }
    if strategy_id == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1":
        return {
            "source": "strategy_report.rule_conditions",
            "strategy_family": "pause_resume",
            "participation": _participation_label(rule_conditions),
            "failed_continuation_sensitive": True,
            "conditions": dict(rule_conditions),
        }
    if strategy_id == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1":
        return {
            "source": "strategy_report.rule_conditions",
            "strategy_family": "breakout_retest",
            "participation": _participation_label(rule_conditions),
            "healthy_pullback_tolerant": True,
            "conditions": dict(rule_conditions),
        }
    if strategy_id in {"MNQ_FIRST_BEAR_SNAP_TURN_V1", "MNQ_FIRST_BULL_SNAP_TURN_V1"}:
        return {
            "source": "strategy_report.rule_conditions",
            "strategy_family": "snap_turn",
            "participation": _participation_label(rule_conditions),
            "fast_decay_sensitive": True,
            "conditions": dict(rule_conditions),
        }
    return None


def _asian_drift_microtrend_label(rule_inputs: Mapping[str, object]) -> str:
    joined = " ".join(str(value).upper() for value in rule_inputs.values())
    if "LONG" in joined or "SHORT" in joined:
        return "ALIGNED_LOW_VOL_DRIFT"
    return "UNKNOWN"


def _participation_label(rule_conditions: Mapping[str, object]) -> str:
    if rule_conditions and all(value is True for value in rule_conditions.values() if isinstance(value, bool)):
        return "STRONG_PARTICIPATING"
    if any(value is False for value in rule_conditions.values() if isinstance(value, bool)):
        return "FAILED_OR_MIXED_PARTICIPATION"
    return "UNKNOWN_PARTICIPATION"


def _breakout_retest_microtrend_label(
    rule_inputs: Mapping[str, object],
    rule_conditions: Mapping[str, object],
) -> str:
    if rule_conditions and all(value is True for value in rule_conditions.values() if isinstance(value, bool)):
        return "HEALTHY_BREAKOUT_RETEST_CONTINUATION"
    if any(value is False for value in rule_conditions.values() if isinstance(value, bool)):
        return "FAILED_BREAKOUT_RETEST_CONTINUATION"
    joined = " ".join(str(value).upper() for value in rule_inputs.values())
    if "BREAKOUT" in joined or "RETEST" in joined:
        return "BREAKOUT_RETEST_CONTEXT_AVAILABLE"
    return "UNKNOWN_BREAKOUT_RETEST_CONTEXT"


def _snap_turn_microtrend_label(strategy_id: str, rule_conditions: Mapping[str, object]) -> str:
    if rule_conditions and all(value is True for value in rule_conditions.values() if isinstance(value, bool)):
        return "FAST_SNAP_TURN_CONTINUATION"
    if any(value is False for value in rule_conditions.values() if isinstance(value, bool)):
        return "FAILED_SNAP_TURN_CONTINUATION"
    return "SNAP_TURN_CONTEXT_AVAILABLE" if strategy_id.startswith("MNQ_FIRST_") else "UNKNOWN_SNAP_TURN_CONTEXT"


def _strategy_report_path(strategy_report: Mapping[str, object]) -> str | None:
    return _string_or_none(
        strategy_report.get("report_json_path")
        or strategy_report.get("strategy_rule_report_json")
        or strategy_report.get("source_strategy_report_path")
    )


def _first_present(payload: Mapping[str, object], keys: tuple[str, ...]) -> object | None:
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return None


def _input_event_from_strategy_report(strategy_report: Mapping[str, object]) -> Mapping[str, object]:
    embedded = strategy_report.get("input_event") or strategy_report.get("source_event")
    if isinstance(embedded, Mapping):
        return embedded
    input_path = _string_or_none(strategy_report.get("input_event_path") or strategy_report.get("input_event_json"))
    if not input_path:
        return {}
    return _read_json_mapping(Path(input_path))


def _source_payload_from_event(input_event: Mapping[str, object]) -> Mapping[str, object]:
    metadata = input_event.get("metadata")
    metadata_mapping = metadata if isinstance(metadata, Mapping) else {}
    source_payload_path = _string_or_none(
        metadata_mapping.get("source_payload_path")
        or input_event.get("source_payload_path")
        or input_event.get("input_runtime_5m_candles_json")
    )
    if not source_payload_path:
        return {}
    return _read_json_mapping(Path(source_payload_path))


def _read_json_mapping(path: Path) -> Mapping[str, object]:
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def _mapping_or_empty(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    return {}


def _run_managed_lifecycle(
    config: TrackBStrategyPaperRunnerConfig,
    strategy_report: Mapping[str, object],
) -> TrackBStrategyManagedPaperLifecycleResult:
    instrument_family = str(
        strategy_report.get("strategy_registry_instrument_family")
        or _instrument_family_from_contract(config.contract_key)
        or "UNKNOWN"
    )
    signal_reason = strategy_report.get("candidate_reason") or strategy_report.get("primary_signal_reason") or strategy_report.get("decision")
    latest_decision_bar_source = (
        strategy_report.get("latest_decision_bar_source")
        or strategy_report.get("runtime_decision_source")
        or strategy_report.get("runtime_data_source")
        or "DATABENTO_LIVE_ARTIFACT"
    )
    continuation_evidence = _continuation_exit_evidence_from_strategy_report(config=config, strategy_report=strategy_report)
    managed_config = TrackBStrategyManagedPaperLifecycleConfig(
        mode=config.mode,
        account_id=config.account_id,
        expected_account_id=config.expected_account_id,
        strategy_id=config.strategy_id,
        instrument_family=instrument_family,
        contract_key=config.contract_key,
        local_symbol=config.allowlisted_local_symbol,
        con_id=config.con_id,
        side=config.side,
        quantity=config.quantity,
        signal_timestamp=_string_or_none(strategy_report.get("signal_timestamp") or strategy_report.get("decision_bar_timestamp")),
        signal_reason=_string_or_none(signal_reason),
        decision_bar_timestamp=_string_or_none(strategy_report.get("decision_bar_timestamp") or strategy_report.get("candle_timestamp")),
        latest_decision_bar_source=_string_or_none(latest_decision_bar_source),
        pricing_policy=_string_or_none(strategy_report.get("pricing_policy") or "TRACK_B_RUNNER_CONFIG"),
        reference_price_source=_string_or_none(strategy_report.get("reference_price_source")),
        reference_price=strategy_report.get("reference_price"),
        entry_limit_price=config.manual_open_limit_price,
        close_limit_price=config.manual_close_limit_price,
        managed_exit_policy_id=config.managed_exit_policy_id,
        submit_enabled=_paper_submit_requested(config),
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        order_type=config.order_type,
        time_in_force=config.time_in_force,
        exchange=config.exchange,
        currency=config.currency,
        tick_size=config.tick_size,
        source_id=config.source_id,
        output_root=config.managed_lifecycle_output_root,
        paper_trade_ledger_output_root=_paper_trade_ledger_output_root(config),
        live_money_readiness=False,
        broker_reconciled=False,
        repo_root=config.repo_root,
        lane_id=config.lane_id,
        runtime_generation_id=config.runtime_generation_id,
        control_plane_snapshot_path=config.control_plane_snapshot_path,
        autonomous_recovery_plan_path=config.autonomous_recovery_plan_path,
        runtime_supervisor_authority_path=config.runtime_supervisor_authority_path,
        runtime_safe_state_envelope_path=config.runtime_safe_state_envelope_path,
        expected_control_plane_snapshot_id=config.expected_control_plane_snapshot_id,
        expected_shared_truth_generation_id=config.expected_shared_truth_generation_id,
        continuation_exit_profile_id=_string_or_none(continuation_evidence.get("continuation_exit_profile_id")),
        continuation_completed_5m_candles=tuple(continuation_evidence.get("continuation_completed_5m_candles") or ()),
        continuation_microtrend_state=continuation_evidence.get("continuation_microtrend_state"),
        continuation_participation_state=continuation_evidence.get("continuation_participation_state"),
        continuation_position_age_minutes=continuation_evidence.get("continuation_position_age_minutes"),
        continuation_mfe=continuation_evidence.get("continuation_mfe"),
        continuation_mae=continuation_evidence.get("continuation_mae"),
        continuation_unrealized_pnl=continuation_evidence.get("continuation_unrealized_pnl"),
        continuation_safe_state_classification=_string_or_none(
            continuation_evidence.get("continuation_safe_state_classification")
        ),
        continuation_lifecycle_reconciliation_classification=_string_or_none(
            continuation_evidence.get("continuation_lifecycle_reconciliation_classification")
        ),
        continuation_source_strategy_report_path=continuation_evidence.get("continuation_source_strategy_report_path"),
    )
    if config.managed_lifecycle_id is not None:
        return run_track_b_strategy_managed_paper_lifecycle(
            config=managed_config,
            lifecycle_id=config.managed_lifecycle_id,
        )
    return run_track_b_strategy_managed_paper_lifecycle(config=managed_config)


def _run_strategy_trade_intent(
    config: TrackBStrategyPaperRunnerConfig,
    strategy_report: Mapping[str, object],
) -> TrackBStrategyTradeIntentResult:
    instrument_family = str(
        strategy_report.get("strategy_registry_instrument_family")
        or _instrument_family_from_contract(config.contract_key)
        or "UNKNOWN"
    )
    latest_decision_bar_source = (
        strategy_report.get("latest_decision_bar_source")
        or strategy_report.get("runtime_decision_source")
        or strategy_report.get("runtime_data_source")
        or config.runtime_decision_source
    )
    intent_config = TrackBStrategyTradeIntentConfig(
        mode=config.mode,
        account_id=config.account_id,
        expected_account_id=config.expected_account_id,
        strategy_id=config.strategy_id,
        instrument_family=instrument_family,
        contract_key=config.contract_key,
        local_symbol=config.allowlisted_local_symbol,
        con_id=config.con_id,
        side=config.side,
        quantity=config.quantity,
        runtime_source=config.runtime_decision_source,
        latest_decision_bar_source=_string_or_none(latest_decision_bar_source),
        pricing_policy=config.paper_order_pricing_policy,
        managed_exit_policy_id=config.managed_exit_policy_id,
        live_money_readiness=False,
        output_root=config.strategy_trade_intent_output_root,
        paper_trade_ledger_output_root=_paper_trade_ledger_output_root(config),
        source_artifact_paths={
            "strategy_rule_report_json": strategy_report.get("report_json_path"),
            "runtime_candle_context_json": str(config.runtime_candle_context_json) if config.runtime_candle_context_json else None,
            "input_event_json": str(config.input_event_json) if config.input_event_json else None,
        },
    )
    return create_track_b_strategy_trade_intent(config=intent_config, strategy_report=strategy_report)


def _disabled_legacy_paper_proof(_config: TrackBStrategyPaperRunnerConfig) -> PaperProofResult:
    raise RuntimeError(LEGACY_PAPER_PROOF_DISABLED_REASON)


def _run_operator_status(config: TrackBStrategyPaperRunnerConfig, runner_report_json: Path) -> None:
    create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_strategy_paper_runner_report_json=runner_report_json,
            output_root=config.operator_status_output_root,
        )
    )


def _paper_trade_ledger_output_root(config: TrackBStrategyPaperRunnerConfig) -> Path:
    if config.paper_trade_ledger_output_root is not None:
        return config.paper_trade_ledger_output_root
    if Path(config.output_root) == DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT:
        return DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
    return Path(config.output_root).parent / "paper_trade_ledger"


def _paper_execution_path(config: TrackBStrategyPaperRunnerConfig) -> str:
    return str(config.paper_execution_path or "STRATEGY_MANAGED").strip().upper()


def _legacy_paper_proof_path_error(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    if _paper_execution_path(config) in {"PAPER_PROOF_DEBUG", "PAPER_PROOF_CANARY"}:
        return LEGACY_PAPER_PROOF_DISABLED_REASON
    return None


def _managed_exit_policy_id(
    config: TrackBStrategyPaperRunnerConfig,
    strategy_report: Mapping[str, object],
) -> str:
    configured = config.managed_exit_policy_id or strategy_report.get("strategy_registry_managed_exit_policy_id")
    if configured:
        return str(configured).strip().upper()
    if strategy_report.get("strategy_registry_exit_not_available") is False:
        return ""
    return "EXIT_NOT_AVAILABLE"


def _managed_lifecycle_runner_verdict(
    classification: TrackBManagedPaperLifecycleClassification,
) -> TrackBStrategyPaperRunnerVerdict:
    if classification == TrackBManagedPaperLifecycleClassification.EXIT_POLICY_MISSING:
        return TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_EXIT_POLICY_MISSING
    if classification == TrackBManagedPaperLifecycleClassification.LIFECYCLE_NOT_AVAILABLE:
        return TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_LIFECYCLE_NOT_AVAILABLE
    if classification == TrackBManagedPaperLifecycleClassification.OPEN_MANAGED:
        return TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_OPEN_MANAGED
    if classification == TrackBManagedPaperLifecycleClassification.EXIT_PENDING:
        return TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_EXIT_PENDING
    if classification == TrackBManagedPaperLifecycleClassification.CLOSED_FLAT:
        return TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_CLOSED_FLAT
    return TrackBStrategyPaperRunnerVerdict.STRATEGY_MANAGED_REVIEW_REQUIRED


def _instrument_family_from_contract(contract_key: object) -> str | None:
    raw = str(contract_key or "")
    if "-" in raw:
        return raw.split("-", 1)[0]
    return None


def _string_or_none(value: object) -> str | None:
    if value in {None, ""}:
        return None
    return str(value)


def _finalize(
    *,
    config: TrackBStrategyPaperRunnerConfig,
    report_json: Path,
    now: datetime,
    runner_id: str,
    verdict: TrackBStrategyPaperRunnerVerdict,
    candle_history_producer: TrackBMgcCandleHistoryProducerResult | None,
    market_history: TrackBMarketHistoryResult | None,
    feature_builder: TrackBFeatureBuilderResult | None,
    strategy_rule: TrackBStrategyRuleRunnerResult | None,
    readiness: TrackBReadinessCheckRunnerResult | None,
    proof: PaperProofResult | None,
    primary_blocker: object | None,
    required_next_action: str,
    operator_status_stage: Callable[[TrackBStrategyPaperRunnerConfig, Path], None],
    proof_classification: str | None = None,
    managed_lifecycle: TrackBStrategyManagedPaperLifecycleResult | None = None,
    strategy_trade_intent: TrackBStrategyTradeIntentResult | None = None,
) -> TrackBStrategyPaperRunnerResult:
    report = _build_report(
        config=config,
        report_json=report_json,
        now=now,
        runner_id=runner_id,
        verdict=verdict,
        candle_history_producer=candle_history_producer,
        market_history=market_history,
        feature_builder=feature_builder,
        strategy_rule=strategy_rule,
        readiness=readiness,
        strategy_trade_intent=strategy_trade_intent,
        managed_lifecycle=managed_lifecycle,
        proof=proof,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        proof_classification=proof_classification,
    )
    _write_report(report_json, report)
    try:
        ledger = update_track_b_paper_trade_ledger_from_runner_report(
            runner_report=report,
            runner_report_json=report_json.parent.parent / "latest_track_b_strategy_paper_runner_report.json",
            output_root=_paper_trade_ledger_output_root(config),
            now=now,
        )
        report.update(
            {
                "paper_trade_ledger_invoked": True,
                "paper_trade_record_written": ledger.trade_record_written,
                "latest_paper_trade_ledger_path": str(ledger.ledger_jsonl),
                "latest_paper_trade_summary_path": str(ledger.trade_summary_json),
                "latest_live_position_status_path": str(ledger.live_position_status_json),
                "latest_pnl_summary_path": str(ledger.pnl_summary_json),
            }
        )
        _write_report(report_json, report)
    except Exception as exc:  # noqa: BLE001 - ledger is a compact read model, not submit authority.
        report["paper_trade_ledger_error"] = str(exc)
        _write_report(report_json, report)
    try:
        operator_status_stage(config, report_json.parent.parent / "latest_track_b_strategy_paper_runner_report.json")
    except Exception as exc:  # noqa: BLE001 - operator-status is read-model convenience only.
        report["operator_status_update_error"] = str(exc)
        _write_report(report_json, report)
    return TrackBStrategyPaperRunnerResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        candle_history_producer_result=candle_history_producer,
        market_history_result=market_history,
        feature_builder_result=feature_builder,
        strategy_rule_result=strategy_rule,
        readiness_result=readiness,
        managed_lifecycle_result=managed_lifecycle,
        strategy_trade_intent_result=strategy_trade_intent,
        paper_proof_result=proof,
    )


def _build_report(
    *,
    config: TrackBStrategyPaperRunnerConfig,
    report_json: Path,
    now: datetime,
    runner_id: str,
    verdict: TrackBStrategyPaperRunnerVerdict,
    candle_history_producer: TrackBMgcCandleHistoryProducerResult | None,
    market_history: TrackBMarketHistoryResult | None,
    feature_builder: TrackBFeatureBuilderResult | None,
    strategy_rule: TrackBStrategyRuleRunnerResult | None,
    readiness: TrackBReadinessCheckRunnerResult | None,
    strategy_trade_intent: TrackBStrategyTradeIntentResult | None,
    managed_lifecycle: TrackBStrategyManagedPaperLifecycleResult | None,
    proof: PaperProofResult | None,
    primary_blocker: object | None,
    required_next_action: str,
    proof_classification: str | None,
) -> dict[str, object]:
    candle_history_report = candle_history_producer.report if candle_history_producer else {}
    market_history_report = market_history.report if market_history else {}
    feature_report = feature_builder.report if feature_builder else {}
    strategy_report = strategy_rule.report if strategy_rule else {}
    readiness_report = readiness.report if readiness else {}
    strategy_trade_intent_report = strategy_trade_intent.report if strategy_trade_intent else {}
    managed_report = managed_lifecycle.report if managed_lifecycle else {}
    proof_report = proof.report if proof else {}
    proof_payload = proof_report.get("proof_payload") if isinstance(proof_report.get("proof_payload"), Mapping) else proof_report
    paper_proof_invoked = proof is not None
    strategy_trade_intent_invoked = strategy_trade_intent is not None
    managed_lifecycle_invoked = managed_lifecycle is not None
    managed_submit_attempted = bool(managed_report.get("submit_attempted")) if managed_report else False
    managed_broker_state_mutated = bool(managed_report.get("broker_state_mutated")) if managed_report else False
    submit_allowed = _paper_submit_requested(config) and (
        proof is not None or bool(managed_report.get("submit_allowed"))
    )
    maintained_history_status = _maintained_history_status(config, now=now)
    runtime_candle_status = _runtime_candle_context_status(config)
    required_next_action_text = (
        f"{required_next_action} Maintained-history stale-age override was used for PAPER diagnostics only; "
        "do not treat this as real strategy freshness or live-money readiness."
        if maintained_history_status.get("maintained_history_stale_override_used") is True
        else required_next_action
    )
    return {
        "schema_version": "track_b_strategy_paper_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_strategy_paper_runner_id": runner_id,
        "strategy_paper_runner_verdict": verdict.value,
        "mode": config.mode,
        "source_id": config.source_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "contract_key": config.contract_key,
        "local_symbol": config.allowlisted_local_symbol,
        "con_id": config.con_id,
        "strategy_id": config.strategy_id,
        "lane_id": config.lane_id,
        "rule_id": config.rule_id,
        "rule_mode": config.rule_mode,
        "asian_drift_watch_verdict": _asian_drift_runner_verdict(verdict, strategy_report),
        "asian_drift_state_snapshot_path": _asian_drift_state_snapshot_path(config),
        "asian_drift_state_ready": _asian_drift_state_ready(config),
        "asia_early_pause_resume_short_watch_verdict": _pause_resume_short_runner_verdict(verdict, strategy_report),
        "asia_early_normal_breakout_retest_hold_long_watch_verdict": _breakout_retest_hold_long_runner_verdict(
            verdict,
            strategy_report,
        ),
        "first_bull_snap_turn_watch_verdict": _first_bull_snap_turn_runner_verdict(verdict, strategy_report),
        "first_bear_snap_turn_watch_verdict": _first_bear_snap_turn_runner_verdict(verdict, strategy_report),
        "strategy_registry_id": strategy_report.get("strategy_registry_id"),
        "strategy_registry_rule_id": strategy_report.get("strategy_registry_rule_id"),
        "strategy_registry_rule_mode": strategy_report.get("strategy_registry_rule_mode"),
        "strategy_registry_instrument_family": strategy_report.get("strategy_registry_instrument_family"),
        "strategy_registry_timeframe": strategy_report.get("strategy_registry_timeframe"),
        "strategy_registry_required_feature_schema": strategy_report.get("strategy_registry_required_feature_schema"),
        "strategy_registry_required_state_schema": strategy_report.get("strategy_registry_required_state_schema"),
        "strategy_registry_feature_version": strategy_report.get("strategy_registry_feature_version"),
        "strategy_registry_calibration_profile": strategy_report.get("strategy_registry_calibration_profile"),
        "strategy_registry_paper_eligible": strategy_report.get("strategy_registry_paper_eligible"),
        "strategy_registry_live_money_eligible": strategy_report.get("strategy_registry_live_money_eligible"),
        "strategy_registry_managed_exit_policy_id": strategy_report.get("strategy_registry_managed_exit_policy_id"),
        "strategy_registry_exit_not_available": strategy_report.get("strategy_registry_exit_not_available"),
        "paper_execution_path": _paper_execution_path(config),
        "managed_exit_policy_id": _managed_exit_policy_id(config, strategy_report),
        "signal_source": strategy_report.get("signal_source") or _signal_source_from_rule_mode(config.rule_mode),
        "real_strategy_signal": (
            bool(strategy_report.get("real_strategy_signal"))
            if strategy_report
            else _real_strategy_signal_from_rule_mode(config.rule_mode)
        ),
        "maintained_history_path": str(config.maintained_history_json) if config.maintained_history_json is not None else None,
        "runtime_candle_context_path": str(config.runtime_candle_context_json) if config.runtime_candle_context_json is not None else None,
        "runtime_candle_context_requested": _runtime_candle_context_requested(config),
        "runtime_candle_context_ready": runtime_candle_status.get("runtime_candle_context_ready"),
        "runtime_candle_context_bars_available": runtime_candle_status.get("runtime_candle_context_bars_available"),
        "runtime_candle_context_gap_count": runtime_candle_status.get("runtime_candle_context_gap_count"),
        "runtime_candle_context_source_mode": runtime_candle_status.get("runtime_candle_context_source_mode"),
        "data_maintenance_history_requested": _maintained_history_requested(config),
        "maintained_history_age_seconds": maintained_history_status.get("maintained_history_age_seconds"),
        "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        "runtime_intraday_freshness_policy": config.runtime_intraday_freshness_policy,
        "runtime_candle_context_required": config.runtime_candle_context_required,
        "runtime_candle_context_supplied": _runtime_candle_context_supplied(config),
        "historical_context_ready": maintained_history_status.get("historical_context_ready"),
        "maintained_history_ready": maintained_history_status.get("maintained_history_ready"),
        "maintained_history_effective_ready": maintained_history_status.get("maintained_history_effective_ready"),
        "maintained_history_stale_override_requested": config.allow_stale_maintained_history_paper,
        "maintained_history_stale_override_used": maintained_history_status.get("maintained_history_stale_override_used", False),
        "maintained_history_bar_count": maintained_history_status.get("maintained_history_bar_count"),
        "maintained_history_gap_count": maintained_history_status.get("maintained_history_gap_count"),
        "maintained_history_provider_mode": maintained_history_status.get("history_provider_mode"),
        "maintained_history_reported_ready": maintained_history_status.get("maintained_history_reported_ready"),
        "maintained_history_reported_freshness_seconds": maintained_history_status.get("maintained_history_reported_freshness_seconds"),
        "candle_history_producer_invoked": candle_history_producer is not None,
        "candle_history_producer_verdict": candle_history_report.get("candle_history_producer_verdict"),
        "candle_history_producer_report_path": str(candle_history_producer.report_json) if candle_history_producer else None,
        "candle_history_input_path": (
            str(candle_history_producer.history_input_json)
            if candle_history_producer and candle_history_producer.history_input_json is not None
            else str(config.candle_history_json) if config.candle_history_json is not None else None
        ),
        "market_history_collector_invoked": market_history is not None,
        "market_history_collector_verdict": market_history_report.get("market_history_verdict"),
        "market_history_collector_report_path": str(market_history.report_json) if market_history else None,
        "market_history_event_path": str(market_history.history_event_json) if market_history and market_history.history_event_json else None,
        "feature_builder_invoked": feature_builder is not None,
        "feature_builder_verdict": feature_report.get("feature_builder_verdict"),
        "feature_builder_report_path": str(feature_builder.report_json) if feature_builder else None,
        "feature_event_path": (
            str(feature_builder.feature_event_json)
            if feature_builder and feature_builder.feature_event_json is not None
            else str(config.feature_event_json) if config.feature_event_json is not None else None
        ),
        "feature_builder_signal_ready": feature_report.get("signal_ready") if feature_report else None,
        "rule_decision": strategy_report.get("decision") or "NOT_PROVIDED",
        "strategy_rule_evaluated": strategy_rule is not None,
        "signal_emitted": strategy_report.get("signal_emitted") if strategy_report else False,
        "signal_direction": strategy_report.get("signal_direction"),
        "strategy_rule_runner_verdict": strategy_report.get("strategy_rule_runner_verdict"),
        "strategy_rule_verdict": strategy_report.get("strategy_rule_runner_verdict"),
        "strategy_rule_report_path": str(strategy_rule.report_json) if strategy_rule else None,
        "strategy_rule_output_batch_path": strategy_report.get("output_batch_path"),
        "readiness_runner_verdict": readiness_report.get("runner_verdict"),
        "readiness_verdict": readiness_report.get("readiness_verdict"),
        "readiness_invoked": readiness is not None,
        "readiness_runner_report_path": str(readiness.report_json) if readiness else None,
        "realtime_quote_received": readiness_report.get("realtime_quote_received"),
        "current_quote_available": readiness_report.get("current_quote_available"),
        "quote_provider_mode": readiness_report.get("quote_provider_mode"),
        "strategy_managed_phase1_runtime_pricing_ready": readiness_report.get(
            "strategy_managed_phase1_runtime_pricing_ready"
        ),
        "phase1_runtime_pricing_source_authority_path": readiness_report.get("source_authority_path"),
        "phase1_runtime_pricing_latest_bar_timestamp": readiness_report.get("latest_bar_timestamp"),
        "paper_submit_requested": _paper_submit_requested(config),
        "paper_submit_flags_present": bool(config.submit_paper and config.confirm_paper_submit),
        "strategy_trade_intent_invoked": strategy_trade_intent_invoked,
        "strategy_trade_intent_created": bool(strategy_trade_intent_report.get("intent_created")) if strategy_trade_intent_report else False,
        "strategy_trade_intent_classification": strategy_trade_intent_report.get("intent_classification")
        or strategy_trade_intent_report.get("classification"),
        "strategy_trade_intent_id": strategy_trade_intent_report.get("intent_id"),
        "strategy_trade_intent_report_path": str(strategy_trade_intent.latest_intent_json) if strategy_trade_intent else None,
        "strategy_trade_intent_jsonl_path": str(strategy_trade_intent.intent_jsonl) if strategy_trade_intent else None,
        "latest_signal_strategy_id": strategy_trade_intent_report.get("strategy_id")
        or strategy_report.get("strategy_registry_id"),
        "latest_signal_side": strategy_trade_intent_report.get("side") or strategy_report.get("signal_direction"),
        "intent_blocked_reason": strategy_trade_intent_report.get("primary_blocker"),
        "lifecycle_mode": strategy_trade_intent_report.get("lifecycle_mode"),
        "paper_proof_invoked": paper_proof_invoked,
        "paper_proof_classification": proof_classification or (proof.classification.value if proof else None),
        "paper_proof_report_path": str(proof.report_json) if proof else None,
        "paper_proof_lifecycle_status": proof_payload.get("proof_lifecycle_status"),
        "strategy_managed_lifecycle_invoked": managed_lifecycle_invoked,
        "managed_lifecycle_invoked": managed_lifecycle_invoked,
        "managed_lifecycle_classification": managed_report.get("strategy_managed_lifecycle_classification"),
        "managed_lifecycle_report_path": str(managed_lifecycle.report_json) if managed_lifecycle else None,
        "managed_lifecycle_id": managed_report.get("lifecycle_id"),
        "managed_trade_id": managed_report.get("trade_id"),
        "managed_exit_policy_id": managed_report.get("managed_exit_policy_id") or _managed_exit_policy_id(config, strategy_report),
        "managed_exit_policy_max_completed_5m_bars": managed_report.get("managed_exit_policy_max_completed_5m_bars"),
        "managed_open_position_age_completed_5m_bars": managed_report.get("open_position_age_completed_5m_bars"),
        "managed_expected_exit_condition": managed_report.get("expected_exit_condition"),
        "managed_close_intent_status": managed_report.get("close_intent_status"),
        "open_intent": proof_payload.get("open_intent"),
        "open_submit_attempt": proof_payload.get("open_submit_attempt"),
        "open_broker_order": proof_payload.get("open_broker_order"),
        "open_fill": proof_payload.get("open_fill"),
        "close_intent": proof_payload.get("close_intent"),
        "close_submit_attempt": proof_payload.get("close_submit_attempt"),
        "close_broker_order": proof_payload.get("close_broker_order"),
        "close_fill": proof_payload.get("close_fill"),
        "managed_entry_intent": managed_report.get("entry_intent"),
        "managed_entry_submit_attempt": managed_report.get("entry_submit_attempt"),
        "managed_entry_fill": managed_report.get("entry_fill"),
        "managed_close_intent": managed_report.get("close_intent"),
        "managed_close_submit_attempt": managed_report.get("close_submit_attempt"),
        "managed_close_fill": managed_report.get("close_fill"),
        "close_only_guard_reports": proof_payload.get("close_only_guard_reports"),
        "flat_after_close_guard_reports": proof_payload.get("flat_after_close_guard_reports"),
        "final_reconciliation": proof_payload.get("final_reconciliation"),
        "final_position_snapshot": _final_position_snapshot(proof_payload),
        "final_open_orders_snapshot": _final_open_orders_snapshot(proof_payload),
        "final_broker_state_classification": (
            managed_report.get("final_broker_state_classification")
            or proof_payload.get("proof_lifecycle_status")
            or proof_classification
        ),
        "final_position_status": managed_report.get("final_position_status") or _final_position_status(proof_payload),
        "final_flat": _final_flat(proof_payload, proof),
        "quantity": config.quantity,
        "order_type": config.order_type,
        "time_in_force": config.time_in_force,
        "manual_open_limit_price": str(config.manual_open_limit_price) if config.manual_open_limit_price is not None else None,
        "manual_close_limit_price": str(config.manual_close_limit_price) if config.manual_close_limit_price is not None else None,
        "primary_blocker": None if primary_blocker is None else str(primary_blocker),
        "secondary_blockers": _secondary_blockers(strategy_report, readiness_report, proof_report),
        "required_next_action": required_next_action_text,
        "submit_allowed": submit_allowed,
        "submit_attempted": paper_proof_invoked or managed_submit_attempted,
        "live_money_readiness": False,
        "paper_proof_cli_called": paper_proof_invoked,
        "broker_state_mutated": paper_proof_invoked or managed_broker_state_mutated,
        "ui_authority": False,
        "hidden_submit": False,
        "live_money_submit_allowed": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_strategy_paper_runner_report.json"),
        "artifact_paths": {
            "maintained_history_json": str(config.maintained_history_json) if config.maintained_history_json else None,
            "runtime_candle_context_json": str(config.runtime_candle_context_json) if config.runtime_candle_context_json else None,
            "candle_history_producer_report_json": str(candle_history_producer.report_json) if candle_history_producer else None,
            "candle_history_input_json": (
                str(candle_history_producer.history_input_json)
                if candle_history_producer and candle_history_producer.history_input_json is not None
                else str(config.candle_history_json) if config.candle_history_json is not None else None
            ),
            "market_history_collector_report_json": str(market_history.report_json) if market_history else None,
            "market_history_event_json": str(market_history.history_event_json) if market_history and market_history.history_event_json else None,
            "feature_builder_report_json": str(feature_builder.report_json) if feature_builder else None,
            "feature_event_json": (
                str(feature_builder.feature_event_json)
                if feature_builder and feature_builder.feature_event_json is not None
                else str(config.feature_event_json) if config.feature_event_json is not None else None
            ),
            "strategy_rule_report_json": str(strategy_rule.report_json) if strategy_rule else None,
            "readiness_runner_report_json": str(readiness.report_json) if readiness else None,
            "strategy_trade_intent_json": str(strategy_trade_intent.latest_intent_json) if strategy_trade_intent else None,
            "paper_proof_report_json": str(proof.report_json) if proof else None,
            "managed_lifecycle_report_json": str(managed_lifecycle.report_json) if managed_lifecycle else None,
            "runner_report_json": str(report_json),
            "latest_runner_report_json": str(report_json.parent.parent / "latest_track_b_strategy_paper_runner_report.json"),
        },
    }


def _write_report(report_json: Path, report: dict[str, object]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")


def _input_event_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object]:
    if config.input_event_payload is not None:
        return config.input_event_payload
    if config.input_event_json is None:
        raise ValueError("input_event_json or input_event_payload is required.")
    value = json.loads(Path(config.input_event_json).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("input event JSON must contain an object.")
    return value


def _feature_builder_source_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object]:
    if config.build_features_from_payload is not None:
        return config.build_features_from_payload
    if config.build_features_from_json is None:
        raise ValueError("build_features_from_json or build_features_from_payload is required when feature building is requested.")
    value = json.loads(Path(config.build_features_from_json).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("feature builder source JSON must contain an object.")
    return value


def _candle_history_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object]:
    if config.candle_history_payload is not None:
        return config.candle_history_payload
    if config.candle_history_json is None:
        raise ValueError("candle_history_json or candle_history_payload is required when candle-history production is requested.")
    value = json.loads(Path(config.candle_history_json).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("candle history JSON must contain an object.")
    return value


def _current_quote_report_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object]:
    if config.current_quote_report_payload is not None:
        return config.current_quote_report_payload
    if config.current_quote_report_json is None:
        raise ValueError("current_quote_report_json or current_quote_report_payload is required when candle-history production is requested.")
    value = json.loads(Path(config.current_quote_report_json).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("current quote report JSON must contain an object.")
    return value


def _maintained_history_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object]:
    if config.maintained_history_payload is not None:
        return config.maintained_history_payload
    if config.maintained_history_json is None:
        raise ValueError("maintained_history_json or maintained_history_payload is required.")
    value = json.loads(Path(config.maintained_history_json).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("maintained history JSON must contain an object.")
    return value


def _runtime_candle_context_payload(config: TrackBStrategyPaperRunnerConfig) -> Mapping[str, object]:
    if config.runtime_candle_context_payload is not None:
        return config.runtime_candle_context_payload
    if config.runtime_candle_context_json is None:
        raise ValueError("runtime_candle_context_json or runtime_candle_context_payload is required.")
    value = json.loads(Path(config.runtime_candle_context_json).read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("runtime candle context JSON must contain an object.")
    return value


def _maintained_history_requested(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return config.maintained_history_json is not None or config.maintained_history_payload is not None


def _runtime_candle_context_requested(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return config.runtime_candle_context_json is not None or config.runtime_candle_context_payload is not None


def _runtime_candle_context_request_error(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    try:
        payload = _runtime_candle_context_payload(config)
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return f"Runtime candle context JSON could not be read: {exc}"
    if payload.get("runtime_candle_context_ready") is not True:
        return str(payload.get("primary_blocker") or "Runtime candle context is not runtime_candle_context_ready=true.")
    candles = payload.get("candles") or payload.get("candle_history")
    if not isinstance(candles, list) or len(candles) < max(config.market_history_min_candles, config.feature_builder_min_history_candles):
        return (
            "Runtime candle context has insufficient candles for market-history collection and feature building: "
            f"received {len(candles) if isinstance(candles, list) else 0}."
        )
    if payload.get("gap_count") not in {None, 0}:
        return f"Runtime candle context has detected gaps: {payload.get('gap_count')}."
    return None


def _runtime_candle_context_status(config: TrackBStrategyPaperRunnerConfig) -> dict[str, object]:
    if not _runtime_candle_context_requested(config):
        return {
            "runtime_candle_context_requested": False,
            "runtime_candle_context_ready": None,
            "runtime_candle_context_bars_available": None,
            "runtime_candle_context_gap_count": None,
            "runtime_candle_context_source_mode": None,
        }
    try:
        payload = _runtime_candle_context_payload(config)
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return {
            "runtime_candle_context_requested": True,
            "runtime_candle_context_ready": False,
            "runtime_candle_context_bars_available": None,
            "runtime_candle_context_gap_count": None,
            "runtime_candle_context_source_mode": None,
            "primary_blocker": f"Runtime candle context JSON could not be read: {exc}",
        }
    candles = payload.get("candles") or payload.get("candle_history")
    bars_available = len(candles) if isinstance(candles, list) else payload.get("bars_available")
    return {
        "runtime_candle_context_requested": True,
        "runtime_candle_context_ready": payload.get("runtime_candle_context_ready") is True,
        "runtime_candle_context_bars_available": bars_available,
        "runtime_candle_context_gap_count": payload.get("gap_count"),
        "runtime_candle_context_source_mode": payload.get("candle_source_mode"),
        "primary_blocker": payload.get("primary_blocker"),
    }


def _maintained_history_request_error(config: TrackBStrategyPaperRunnerConfig, *, now: datetime) -> str | None:
    status = _maintained_history_status(config, now=now)
    blocker = status.get("primary_blocker")
    return str(blocker) if blocker else None


def _maintained_history_status(config: TrackBStrategyPaperRunnerConfig, *, now: datetime) -> dict[str, object]:
    if not _maintained_history_requested(config):
        return {
            "maintained_history_requested": False,
            "historical_context_ready": None,
            "maintained_history_ready": None,
            "primary_blocker": None,
        }
    has_current_quote = config.current_quote_report_json is not None or config.current_quote_report_payload is not None
    if not has_current_quote:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "primary_blocker": "Maintained-history PAPER path requires a separate current quote report JSON/payload.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    try:
        payload = _maintained_history_payload(config)
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "primary_blocker": f"Maintained history JSON could not be read: {exc}",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    reported_ready = payload.get("history_ready")
    reported_freshness = payload.get("history_freshness_seconds")
    reported_blocker = str(payload.get("primary_blocker") or "")
    history_provider_mode = payload.get("history_provider_mode")
    runtime_policy = str(config.runtime_intraday_freshness_policy or "NOT_REQUESTED").upper()
    candles = payload.get("candles") or payload.get("candle_history")
    if not isinstance(candles, list) or not candles:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": 0,
            "history_provider_mode": history_provider_mode,
            "primary_blocker": "Maintained MGC 1m history contains no candle array.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    min_bars = max(int(config.market_history_min_candles), int(config.feature_builder_min_history_candles))
    if len(candles) < min_bars:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": len(candles),
            "history_provider_mode": history_provider_mode,
            "primary_blocker": f"Maintained MGC 1m history has {len(candles)} bars; requires at least {min_bars}.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    gap_count = _optional_int(payload.get("gap_count"))
    if gap_count is not None and gap_count > 0:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": len(candles),
            "maintained_history_gap_count": gap_count,
            "history_provider_mode": history_provider_mode,
            "primary_blocker": f"Maintained MGC 1m history has {gap_count} detected 1m gaps.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    latest_timestamp = _latest_history_timestamp(payload)
    if latest_timestamp is None:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": len(candles),
            "maintained_history_gap_count": gap_count,
            "history_provider_mode": history_provider_mode,
            "primary_blocker": "Maintained MGC 1m history has no latest candle timestamp.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    history_age_seconds = max(int((now.astimezone(UTC) - latest_timestamp).total_seconds()), 0)
    if config.runtime_candle_context_required:
        return {
            "maintained_history_requested": True,
            "historical_context_ready": True,
            "maintained_history_ready": False,
            "maintained_history_effective_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": len(candles),
            "maintained_history_gap_count": gap_count,
            "maintained_history_age_seconds": history_age_seconds,
            "history_provider_mode": history_provider_mode,
            "primary_blocker": "Runtime candle context is required; maintained historical context is not a live runtime candle capture.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    if runtime_policy == "REQUIRE_MAX_AGE" and history_age_seconds > config.max_maintained_history_age_seconds:
        if _allow_stale_maintained_history_paper(config):
            return {
                "maintained_history_requested": True,
                "historical_context_ready": True,
                "maintained_history_ready": False,
                "maintained_history_effective_ready": True,
                "maintained_history_stale_override_used": True,
                "maintained_history_reported_ready": reported_ready,
                "maintained_history_reported_freshness_seconds": reported_freshness,
                "maintained_history_bar_count": len(candles),
                "maintained_history_gap_count": gap_count,
                "maintained_history_age_seconds": history_age_seconds,
                "history_provider_mode": history_provider_mode,
                "primary_blocker": None,
                "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
            }
        return {
            "maintained_history_requested": True,
            "historical_context_ready": True,
            "maintained_history_ready": False,
            "maintained_history_effective_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": len(candles),
            "maintained_history_gap_count": gap_count,
            "maintained_history_age_seconds": history_age_seconds,
            "history_provider_mode": history_provider_mode,
            "primary_blocker": (
                f"Maintained MGC 1m history is stale: {history_age_seconds}s old, "
                f"max allowed {config.max_maintained_history_age_seconds}s."
            ),
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    if payload.get("history_ready") is not True and not _reported_blocker_is_age_only(reported_blocker):
        return {
            "maintained_history_requested": True,
            "historical_context_ready": False,
            "maintained_history_ready": False,
            "maintained_history_effective_ready": False,
            "maintained_history_reported_ready": reported_ready,
            "maintained_history_reported_freshness_seconds": reported_freshness,
            "maintained_history_bar_count": len(candles),
            "maintained_history_gap_count": gap_count,
            "maintained_history_age_seconds": history_age_seconds,
            "history_provider_mode": history_provider_mode,
            "primary_blocker": reported_blocker or "Maintained MGC 1m history is not history_ready=true.",
            "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
        }
    return {
        "maintained_history_requested": True,
        "historical_context_ready": True,
        "maintained_history_ready": True,
        "maintained_history_effective_ready": True,
        "maintained_history_stale_override_used": False,
        "maintained_history_reported_ready": reported_ready,
        "maintained_history_reported_freshness_seconds": reported_freshness,
        "maintained_history_bar_count": len(candles),
        "maintained_history_gap_count": gap_count,
        "maintained_history_age_seconds": history_age_seconds,
        "history_provider_mode": history_provider_mode,
        "primary_blocker": None,
        "max_maintained_history_age_seconds": config.max_maintained_history_age_seconds,
    }


def _allow_stale_maintained_history_paper(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return bool(config.allow_stale_maintained_history_paper and str(config.mode).upper() == "PAPER")


def _reported_blocker_is_age_only(blocker: str) -> bool:
    lowered = blocker.lower()
    return "stale" in lowered or "old" in lowered or "max allowed" in lowered


def _runtime_candle_context_supplied(config: TrackBStrategyPaperRunnerConfig) -> bool:
    if _maintained_history_requested(config):
        return False
    return bool(
        _runtime_candle_context_requested(config)
        or config.candle_history_json is not None
        or config.candle_history_payload is not None
        or config.build_features_from_json is not None
        or config.build_features_from_payload is not None
        or config.feature_event_json is not None
        or config.input_event_json is not None
        or config.input_event_payload is not None
    )


def _maintained_history_market_payload(config: TrackBStrategyPaperRunnerConfig, *, now: datetime) -> Mapping[str, object]:
    del now
    history = dict(_maintained_history_payload(config))
    current_quote = _current_quote_report_payload(config)
    metadata = history.get("metadata") if isinstance(history.get("metadata"), Mapping) else {}
    quote_report_path = current_quote.get("report_json_path") or (None if config.current_quote_report_json is None else str(config.current_quote_report_json))
    history.update(
        {
            "quote_provider_mode": current_quote.get("quote_provider_mode"),
            "realtime_quote_received": current_quote.get("realtime_quote_received"),
            "current_quote_available": current_quote.get("current_quote_available"),
            "quote_freshness_verdict": current_quote.get("quote_freshness_verdict"),
            "quote_report_path": quote_report_path,
            "source_report_path": quote_report_path,
            "metadata": {
                **dict(metadata),
                "source_report_path": quote_report_path,
                "current_quote_report_path": quote_report_path,
                "quote_provider_mode": current_quote.get("quote_provider_mode"),
                "realtime_quote_received": current_quote.get("realtime_quote_received"),
                "current_quote_available": current_quote.get("current_quote_available"),
                "quote_freshness_verdict": current_quote.get("quote_freshness_verdict"),
                "realtime_current_quote_is_separate": True,
            },
        }
    )
    return history


def _latest_history_timestamp(payload: Mapping[str, object]) -> datetime | None:
    timestamp = payload.get("candle_timestamp")
    candles = payload.get("candles") or payload.get("candle_history")
    if isinstance(candles, list) and candles:
        latest = candles[-1]
        if isinstance(latest, Mapping):
            timestamp = latest.get("candle_timestamp") or latest.get("timestamp") or latest.get("observed_at")
    if timestamp is None:
        return None
    parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_int(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _candle_history_producer_requested(config: TrackBStrategyPaperRunnerConfig) -> bool:
    if _current_quote_is_phase1_strategy_managed_pricing(config):
        return config.candle_history_json is not None or config.candle_history_payload is not None
    return (
        config.candle_history_json is not None
        or config.candle_history_payload is not None
        or config.current_quote_report_json is not None
        or config.current_quote_report_payload is not None
    )


def _candle_history_producer_request_error(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    has_history = config.candle_history_json is not None or config.candle_history_payload is not None
    has_current_quote = (
        config.current_quote_report_json is not None or config.current_quote_report_payload is not None
    ) and not _current_quote_is_phase1_strategy_managed_pricing(config)
    if has_history and not has_current_quote:
        return "Candle-history PAPER path requires a current quote report JSON/payload."
    if has_current_quote and not has_history:
        return "Candle-history PAPER path requires bounded candle history JSON/payload."
    return None


def _current_quote_is_phase1_strategy_managed_pricing(config: TrackBStrategyPaperRunnerConfig) -> bool:
    if _paper_execution_path(config) != "STRATEGY_MANAGED":
        return False
    if str(config.paper_order_pricing_policy or "").strip().upper() not in {
        "LIMIT_AT_LAST",
        "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT",
        "LIMIT_AT_SIGNAL_PRICE",
    }:
        return False
    payload = _optional_current_quote_payload(config)
    if not payload:
        return False
    source = str(payload.get("source") or payload.get("source_category") or payload.get("input_source_category") or "")
    return "PHASE1" in source or payload.get("phase1_runtime_market_data_authority") is True


def _feature_builder_requested(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return config.build_features_from_json is not None or config.build_features_from_payload is not None


def _mode_error(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    if str(config.mode or "").strip().upper() != "PAPER":
        return "Track B strategy PAPER runner only supports --mode PAPER."
    return None


def _submit_request_error(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    if not config.submit_paper and not config.confirm_paper_submit:
        return None
    if not (config.submit_paper and config.confirm_paper_submit):
        return "--submit-paper and --confirm-paper-submit are both required for PAPER submit."
    if config.account_id != config.expected_account_id:
        return f"--account-id {config.account_id} must match --expected-account-id {config.expected_account_id} for PAPER submit."
    if config.account_id != "DUM882026" or config.expected_account_id != "DUM882026":
        return "Track B strategy PAPER submit is currently restricted to PAPER account DUM882026."
    allowlist_entry = ReadOnlyPreflightConfig().contract_allowlist.get(config.contract_key)
    if allowlist_entry is None:
        allowed = ", ".join(sorted(ReadOnlyPreflightConfig().contract_allowlist))
        return (
            "Track B strategy PAPER submit requires an explicitly allowlisted contract_key "
            f"({allowed}); observed {config.contract_key}."
        )
    expected_local_symbol = str(allowlist_entry.get("local_symbol") or "")
    if config.allowlisted_local_symbol != expected_local_symbol:
        return (
            "Track B strategy PAPER submit local symbol guard failed: "
            f"expected {expected_local_symbol}, observed {config.allowlisted_local_symbol}."
        )
    expected_con_id = allowlist_entry.get("con_id")
    if config.con_id is not None and expected_con_id is not None and int(config.con_id) != int(expected_con_id):
        return f"Track B strategy PAPER submit conId guard failed: expected {expected_con_id}, observed {config.con_id}."
    if config.quantity is None:
        return "--quantity is required for PAPER submit."
    if int(config.quantity) != 1:
        return "--quantity must be exactly 1 for the current Track B paper proof lifecycle."
    if config.manual_open_limit_price is None:
        return "--manual-open-limit-price is required for PAPER submit."
    if config.manual_close_limit_price is None:
        return "--manual-close-limit-price is required for PAPER submit."
    open_error = _manual_price_error(config.manual_open_limit_price, "--manual-open-limit-price")
    if open_error:
        return open_error
    return _manual_price_error(config.manual_close_limit_price, "--manual-close-limit-price")


def _manual_price_error(value: str | Decimal, label: str) -> str | None:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return f"{label} must be a positive decimal."
    if not price.is_finite() or price <= 0:
        return f"{label} must be positive."
    return None


def _paper_submit_requested(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return bool(config.submit_paper and config.confirm_paper_submit)


def _proof_blocker(report: Mapping[str, object]) -> str:
    return str(report.get("failure_or_ambiguity") or report.get("primary_blocker") or "Paper proof did not pass.")


def _proof_required_action(report: Mapping[str, object], fallback: str) -> str:
    return str(report.get("required_manual_action") or report.get("required_next_action") or fallback)


def _final_position_status(proof_payload: Mapping[str, object]) -> str | None:
    final = proof_payload.get("final_reconciliation")
    if isinstance(final, Mapping):
        return str(final.get("status") or "") or None
    return None


def _final_open_orders_snapshot(proof_payload: Mapping[str, object]) -> object | None:
    final = proof_payload.get("final_reconciliation")
    if isinstance(final, Mapping):
        if "working_orders" in final:
            return final.get("working_orders")
        if "open_orders" in final:
            return final.get("open_orders")
        if "working_order_count" in final:
            return final.get("working_order_count")
    guard_reports = proof_payload.get("flat_after_close_guard_reports")
    if isinstance(guard_reports, list) and guard_reports:
        latest = guard_reports[-1]
        if isinstance(latest, Mapping):
            if "working_orders" in latest:
                return latest.get("working_orders")
            if "open_orders" in latest:
                return latest.get("open_orders")
            if "working_order_count" in latest:
                return latest.get("working_order_count")
    return None


def _final_position_snapshot(proof_payload: Mapping[str, object]) -> object | None:
    guard_reports = proof_payload.get("flat_after_close_guard_reports")
    if isinstance(guard_reports, list) and guard_reports:
        latest = guard_reports[-1]
        if isinstance(latest, Mapping) and latest.get("position") is not None:
            return latest.get("position")
    return proof_payload.get("final_reconciliation")


def _final_flat(proof_payload: Mapping[str, object], proof: PaperProofResult | None) -> bool | None:
    if proof is None:
        return None
    if proof.classification == TerminalClassification.PASSED and proof_payload.get("proof_lifecycle_status") == "PROOF_COMPLETE_FLAT":
        return True
    return (
        proof.classification == TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE
        and proof_payload.get("proof_lifecycle_status") == "PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
    )


def _signal_source_from_rule_mode(rule_mode: str) -> str:
    if str(rule_mode or "").upper() == "DEMO_LONG_ONLY":
        return "DEMO_WIRING_PROOF"
    if str(rule_mode or "").upper() == "HUMAN_REVIEW_ONLY":
        return "HUMAN_REVIEW_ONLY"
    if str(rule_mode or "").upper() == "ASIAN_DRIFT_V1":
        return "ASIAN_DRIFT_V1"
    if str(rule_mode or "").upper() == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1":
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    if str(rule_mode or "").upper() == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1":
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    if str(rule_mode or "").upper() == "FIRST_BULL_SNAP_TURN_V1":
        return "FIRST_BULL_SNAP_TURN_V1"
    if str(rule_mode or "").upper() == "FIRST_BEAR_SNAP_TURN_V1":
        return "FIRST_BEAR_SNAP_TURN_V1"
    return "REAL_STRATEGY_RULE"


def _real_strategy_signal_from_rule_mode(rule_mode: str) -> bool:
    return str(rule_mode or "").upper() in {
        "MGC_EMA_MOMENTUM_RECLAIM_LONG",
        "ASIAN_DRIFT_V1",
        "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "FIRST_BULL_SNAP_TURN_V1",
        "FIRST_BEAR_SNAP_TURN_V1",
    }


def _is_asian_drift_rule(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return str(config.rule_mode or "").upper() == "ASIAN_DRIFT_V1" or str(config.rule_id or "") == "asian_drift_v1"


def _is_pause_resume_short_rule(config: TrackBStrategyPaperRunnerConfig) -> bool:
    value = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    return str(config.rule_mode or "").upper() == value or str(config.rule_id or "").upper() == value


def _is_breakout_retest_hold_long_rule(config: TrackBStrategyPaperRunnerConfig) -> bool:
    value = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    return str(config.rule_mode or "").upper() == value or str(config.rule_id or "").upper() == value


def _is_first_bull_snap_turn_rule(config: TrackBStrategyPaperRunnerConfig) -> bool:
    value = "FIRST_BULL_SNAP_TURN_V1"
    return str(config.rule_mode or "").upper() == value or str(config.rule_id or "").upper() == value


def _is_first_bear_snap_turn_rule(config: TrackBStrategyPaperRunnerConfig) -> bool:
    value = "FIRST_BEAR_SNAP_TURN_V1"
    return str(config.rule_mode or "").upper() == value or str(config.rule_id or "").upper() == value


def _is_watch_only_until_submit_rule(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return (
        _is_asian_drift_rule(config)
        or _is_pause_resume_short_rule(config)
        or _is_breakout_retest_hold_long_rule(config)
        or _is_first_bull_snap_turn_rule(config)
        or _is_first_bear_snap_turn_rule(config)
    )


def _strategy_specific_not_ready_verdict(config: TrackBStrategyPaperRunnerConfig) -> TrackBStrategyPaperRunnerVerdict | None:
    if _is_asian_drift_rule(config):
        return TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NOT_READY_FOR_TONIGHT
    if _is_pause_resume_short_rule(config):
        return TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY
    if _is_breakout_retest_hold_long_rule(config):
        return TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY
    if _is_first_bull_snap_turn_rule(config):
        return TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_NOT_READY
    if _is_first_bear_snap_turn_rule(config):
        return TrackBStrategyPaperRunnerVerdict.FIRST_BEAR_SNAP_TURN_NOT_READY
    return None


def _strategy_specific_signal_ready_no_submit_verdict(config: TrackBStrategyPaperRunnerConfig) -> TrackBStrategyPaperRunnerVerdict | None:
    if _is_asian_drift_rule(config):
        return TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT
    if _is_pause_resume_short_rule(config):
        return TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT
    if _is_breakout_retest_hold_long_rule(config):
        return TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT
    if _is_first_bull_snap_turn_rule(config):
        return TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT
    if _is_first_bear_snap_turn_rule(config):
        return TrackBStrategyPaperRunnerVerdict.FIRST_BEAR_SNAP_TURN_SIGNAL_READY_NO_SUBMIT
    return None


def _expected_signal_source(config: TrackBStrategyPaperRunnerConfig) -> str:
    if _is_asian_drift_rule(config):
        return "ASIAN_DRIFT_V1"
    if _is_pause_resume_short_rule(config):
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    if _is_breakout_retest_hold_long_rule(config):
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    if _is_first_bull_snap_turn_rule(config):
        return "FIRST_BULL_SNAP_TURN_V1"
    if _is_first_bear_snap_turn_rule(config):
        return "FIRST_BEAR_SNAP_TURN_V1"
    return _signal_source_from_rule_mode(config.rule_mode)


def _strategy_specific_real_signal_next_action(config: TrackBStrategyPaperRunnerConfig) -> str:
    if _is_asian_drift_rule(config):
        return "Use an actual ASIAN_DRIFT_V1 state snapshot; DEMO/proof signals cannot drive this path."
    if _is_pause_resume_short_rule(config):
        return "Use an actual ASIA_EARLY_PAUSE_RESUME_SHORT_V1 feature/state envelope; DEMO/proof signals cannot drive this path."
    if _is_breakout_retest_hold_long_rule(config):
        return "Use an actual ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1 feature/state envelope; DEMO/proof signals cannot drive this path."
    if _is_first_bull_snap_turn_rule(config):
        return "Use an actual FIRST_BULL_SNAP_TURN_V1 feature/state envelope; DEMO/proof signals cannot drive this path."
    if _is_first_bear_snap_turn_rule(config):
        return "Use an actual FIRST_BEAR_SNAP_TURN_V1 feature/state envelope; DEMO/proof signals cannot drive this path."
    return "Use a registered real strategy signal before PAPER handoff."


def _strategy_specific_signal_ready_next_action(config: TrackBStrategyPaperRunnerConfig) -> str:
    if _is_asian_drift_rule(config):
        return "Asian Drift real state signal is ready, but explicit PAPER submit flags were not supplied. No readiness or broker mutation was attempted."
    if _is_pause_resume_short_rule(config):
        return "Pause-resume short real strategy signal is ready, but explicit PAPER submit flags were not supplied. No readiness or broker mutation was attempted."
    if _is_breakout_retest_hold_long_rule(config):
        return "Breakout-retest-hold long real strategy signal is ready, but explicit PAPER submit flags were not supplied. No readiness or broker mutation was attempted."
    if _is_first_bull_snap_turn_rule(config):
        return "First bull snap-turn real strategy signal is ready, but explicit PAPER submit flags were not supplied. No readiness or broker mutation was attempted."
    if _is_first_bear_snap_turn_rule(config):
        return "First bear snap-turn real strategy signal is ready, but explicit PAPER submit flags were not supplied. No readiness or broker mutation was attempted."
    return "Strategy signal is ready, but explicit PAPER submit flags were not supplied. No readiness or broker mutation was attempted."


def _registered_strategy_signal_blocker(
    config: TrackBStrategyPaperRunnerConfig,
    strategy_report: Mapping[str, object],
) -> str | None:
    if not _is_watch_only_until_submit_rule(config):
        return None
    expected_signal_source = _expected_signal_source(config)
    if strategy_report.get("real_strategy_signal") is not True:
        return f"{expected_signal_source} PAPER path requires real_strategy_signal=true."
    if strategy_report.get("signal_source") != expected_signal_source:
        return f"{expected_signal_source} PAPER path requires signal_source={expected_signal_source}."
    if strategy_report.get("strategy_registry_paper_eligible") is not True:
        return f"{expected_signal_source} PAPER path requires registry paper_eligible=true."
    if strategy_report.get("strategy_registry_live_money_eligible") is not False:
        return f"{expected_signal_source} PAPER path requires registry live_money_eligible=false."
    return None


def _asian_drift_runner_verdict(
    verdict: TrackBStrategyPaperRunnerVerdict,
    strategy_report: Mapping[str, object],
) -> str | None:
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NOT_READY_FOR_TONIGHT:
        return "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION:
        return "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT:
        return "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    return strategy_report.get("asian_drift_watch_verdict") if strategy_report else None


def _pause_resume_short_runner_verdict(
    verdict: TrackBStrategyPaperRunnerVerdict,
    strategy_report: Mapping[str, object],
) -> str | None:
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY:
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION:
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION"
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT:
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT"
    return strategy_report.get("asia_early_pause_resume_short_watch_verdict") if strategy_report else None


def _breakout_retest_hold_long_runner_verdict(
    verdict: TrackBStrategyPaperRunnerVerdict,
    strategy_report: Mapping[str, object],
) -> str | None:
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY:
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION:
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION"
    if verdict == TrackBStrategyPaperRunnerVerdict.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT:
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT"
    return strategy_report.get("asia_early_normal_breakout_retest_hold_long_watch_verdict") if strategy_report else None


def _first_bull_snap_turn_runner_verdict(
    verdict: TrackBStrategyPaperRunnerVerdict,
    strategy_report: Mapping[str, object],
) -> str | None:
    if verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_NOT_READY:
        return "FIRST_BULL_SNAP_TURN_NOT_READY"
    if verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION:
        return "FIRST_BULL_SNAP_TURN_NO_SIGNAL_NO_MUTATION"
    if verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT:
        return "FIRST_BULL_SNAP_TURN_SIGNAL_READY_NO_SUBMIT"
    return strategy_report.get("first_bull_snap_turn_watch_verdict") if strategy_report else None


def _first_bear_snap_turn_runner_verdict(
    verdict: TrackBStrategyPaperRunnerVerdict,
    strategy_report: Mapping[str, object],
) -> str | None:
    if verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BEAR_SNAP_TURN_NOT_READY:
        return "FIRST_BEAR_SNAP_TURN_NOT_READY"
    if verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BEAR_SNAP_TURN_NO_SIGNAL_NO_MUTATION:
        return "FIRST_BEAR_SNAP_TURN_NO_SIGNAL_NO_MUTATION"
    if verdict == TrackBStrategyPaperRunnerVerdict.FIRST_BEAR_SNAP_TURN_SIGNAL_READY_NO_SUBMIT:
        return "FIRST_BEAR_SNAP_TURN_SIGNAL_READY_NO_SUBMIT"
    return strategy_report.get("first_bear_snap_turn_watch_verdict") if strategy_report else None


def _asian_drift_state_snapshot_path(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    if not _is_asian_drift_rule(config):
        return None
    if config.input_event_json is not None:
        return str(config.input_event_json)
    return None


def _asian_drift_state_ready(config: TrackBStrategyPaperRunnerConfig) -> bool | None:
    if not _is_asian_drift_rule(config):
        return None
    try:
        payload = _input_event_payload(config)
    except (TypeError, ValueError, OSError, json.JSONDecodeError):
        return False
    return payload.get("asian_drift_state_ready") is True or (
        payload.get("asia_drift_state") is not None
        and payload.get("asia_drift_regime") is not None
        and payload.get("hypothetical_entry_ready") is not None
    )


def _registered_strategy_submit_side_blocker(
    config: TrackBStrategyPaperRunnerConfig,
    strategy_report: Mapping[str, object],
) -> str | None:
    if not _is_watch_only_until_submit_rule(config) or not _paper_submit_requested(config):
        return None
    direction = str(strategy_report.get("signal_direction") or strategy_report.get("decision") or "").upper()
    requested_side = str(config.side or "").upper()
    expected_side_by_direction = {"LONG": "BUY", "SHORT": "SELL"}
    expected_side = expected_side_by_direction.get(direction)
    signal_source = _expected_signal_source(config)
    if expected_side is None:
        return f"{signal_source} PAPER submit requires explicit signal_direction LONG or SHORT."
    if requested_side != expected_side:
        return f"{signal_source} {direction} signal requires --side {expected_side}; received {requested_side or 'NOT_PROVIDED'}."
    return None


def _secondary_blockers(*reports: Mapping[str, object]) -> list[str]:
    blockers: list[str] = []
    for report in reports:
        if report.get("primary_blocker"):
            blockers.append(str(report["primary_blocker"]))
        blockers.extend(str(item) for item in report.get("secondary_blockers") or ())
        if report.get("failure_or_ambiguity"):
            blockers.append(str(report["failure_or_ambiguity"]))
    return _dedupe(blockers)


def _dedupe(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            unique.append(value)
    return unique
