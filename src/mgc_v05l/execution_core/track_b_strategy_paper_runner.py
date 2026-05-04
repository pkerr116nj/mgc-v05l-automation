"""Track B controlled PAPER strategy execution runner.

This Phase 2 boundary is the explicit handoff from a strategy signal to
readiness and the Track B paper-proof lifecycle. It defaults to dry-run/no
submit. Broker mutation is possible only through ``run_paper_proof`` when the
operator supplies PAPER mode, submit/confirm flags, quantity, and manual open
and close limit prices.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from .candle_signal_producer import DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT
from .harness import HarnessResult
from .ibkr_readonly_transport import IbkrReadOnlyTransportConfig, IbkrReadOnlyTwsTransport
from .models import TerminalClassification, require_aware_datetime, to_jsonable
from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, create_operator_status_summary
from .paper_proof import DEFAULT_PAPER_PROOF_OUTPUT_ROOT, PaperProofConfig, PaperProofResult, ProofRunner, run_paper_proof
from .preflight import ReadOnlyPreflightConfig, run_read_only_preflight
from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT
from .strategy_signal_adapter import DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT
from .track_b_feature_builder import (
    DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT,
    TrackBFeatureBuilderResult,
    TrackBFeatureBuilderVerdict,
    build_track_b_mgc_feature_event,
)
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
    run_track_b_readiness_check,
)
from .track_b_strategy_rule_runner import (
    DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
    DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT,
    TrackBStrategyRuleRunnerResult,
    run_track_b_strategy_rule,
)


DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_strategy_paper_runner")


class TrackBStrategyPaperRunnerVerdict(str, Enum):
    NO_SIGNAL = "TRACK_B_STRATEGY_PAPER_RUNNER_NO_SIGNAL"
    HUMAN_REVIEW_NO_SIGNAL = "TRACK_B_STRATEGY_PAPER_RUNNER_HUMAN_REVIEW_NO_SIGNAL"
    BLOCKED_NON_PAPER_MODE = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_NON_PAPER_MODE"
    BLOCKED_INVALID_SUBMIT_REQUEST = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_INVALID_SUBMIT_REQUEST"
    BLOCKED_FEATURE_BUILDER = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_FEATURE_BUILDER"
    BLOCKED_STRATEGY_RULE = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_STRATEGY_RULE"
    BLOCKED_READINESS = "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_READINESS"
    PAPER_READY_NO_SUBMIT_REQUESTED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_READY_NO_SUBMIT_REQUESTED"
    PAPER_PROOF_PASSED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED"
    PAPER_PROOF_BLOCKED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_BLOCKED"
    PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
    PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED = "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED"
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
    broker_order_id: str | None = None
    perm_id: str | None = None
    market_data_mode: str = "DELAYED"
    databento_continuous_symbol: str = "MGC.v.0"
    dataset: str = "GLBX.MDP3"
    allowlisted_local_symbol: str = "MGCM6"
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
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT


@dataclass(frozen=True)
class TrackBStrategyPaperRunnerStages:
    candle_history_producer: Callable[[TrackBStrategyPaperRunnerConfig], TrackBMgcCandleHistoryProducerResult]
    market_history_collector: Callable[[TrackBStrategyPaperRunnerConfig, TrackBMgcCandleHistoryProducerResult], TrackBMarketHistoryResult]
    feature_builder: Callable[[TrackBStrategyPaperRunnerConfig], TrackBFeatureBuilderResult]
    strategy_rule: Callable[[TrackBStrategyPaperRunnerConfig], TrackBStrategyRuleRunnerResult]
    readiness: Callable[[TrackBStrategyPaperRunnerConfig], TrackBReadinessCheckRunnerResult]
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
    paper_proof_result: PaperProofResult | None = None


def default_stages(
    *,
    readiness_stages: TrackBReadinessCheckRunnerStages | None = None,
    proof_runner: ProofRunner | None = None,
) -> TrackBStrategyPaperRunnerStages:
    return TrackBStrategyPaperRunnerStages(
        candle_history_producer=_run_candle_history_producer,
        market_history_collector=_run_market_history_collector,
        feature_builder=_run_feature_builder,
        strategy_rule=_run_strategy_rule,
        readiness=lambda config: _run_readiness(config, readiness_stages=readiness_stages),
        paper_proof=lambda config: _run_paper_proof(config, proof_runner=proof_runner),
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
                proof=proof,
                primary_blocker=submit_error,
                required_next_action="Provide explicit PAPER submit flags, quantity, and manual open/close limit prices before retrying.",
                operator_status_stage=actual_stages.operator_status,
            )

        candle_history_request_error = _candle_history_producer_request_error(config)
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
                proof=proof,
                primary_blocker=candle_history_request_error,
                required_next_action="Provide both bounded candle history and a current quote report before running the history producer.",
                operator_status_stage=actual_stages.operator_status,
            )

        strategy_config = config
        feature_config = config
        if _candle_history_producer_requested(config):
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
                primary_blocker=strategy_rule.report.get("primary_blocker") or "Strategy rule runner is blocked.",
                required_next_action=str(strategy_rule.report.get("required_next_action") or "Resolve strategy rule blocker before PAPER handoff."),
                operator_status_stage=actual_stages.operator_status,
            )
        if strategy_rule.report.get("signal_emitted") is not True:
            decision = str(strategy_rule.report.get("decision") or "")
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

        proof = actual_stages.paper_proof(config)
        proof_classification = proof.classification.value
        if proof.classification == TerminalClassification.PASSED:
            verdict = TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_PASSED
            primary_blocker = None
            required_next_action = "PAPER strategy proof lifecycle passed and final broker state is flat."
        elif proof.classification == TerminalClassification.BLOCKED:
            verdict = TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_BLOCKED
            primary_blocker = _proof_blocker(proof.report)
            required_next_action = _proof_required_action(proof.report, "Resolve paper proof blocker before retrying.")
        elif proof.classification == TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE:
            verdict = TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE
            primary_blocker = _proof_blocker(proof.report)
            required_next_action = _proof_required_action(
                proof.report,
                "Verify broker activity and rerun read-only recovery before any further PAPER submit.",
            )
        else:
            verdict = TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED
            primary_blocker = _proof_blocker(proof.report)
            required_next_action = _proof_required_action(proof.report, "Manual review required; do not run another open proof until broker state is reconciled.")
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
            primary_blocker=primary_blocker,
            required_next_action=required_next_action,
            operator_status_stage=actual_stages.operator_status,
            proof_classification=proof_classification,
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


def _run_paper_proof(config: TrackBStrategyPaperRunnerConfig, *, proof_runner: ProofRunner | None) -> PaperProofResult:
    paper_config = PaperProofConfig(
        mode=config.mode,
        host=config.host,
        port=config.port,
        client_id=config.client_id,
        account_id=config.account_id,
        contract_key=config.contract_key,
        side=config.side,
        quantity=int(config.quantity or 0),
        order_type=config.order_type,
        time_in_force=config.time_in_force,
        output_root=config.paper_proof_output_root,
        submit_enabled=True,
        confirm_paper_submit=True,
        allow_delayed_data_for_paper_proof=True,
        manual_open_limit_price=config.manual_open_limit_price,
        manual_close_limit_price=config.manual_close_limit_price,
        proof_timing_status=config.proof_timing_status,
        proof_timing_source=config.proof_timing_source,
        proof_timing_detail=config.proof_timing_detail,
    )
    transport_config = IbkrReadOnlyTransportConfig(
        request_timeout_seconds=config.request_timeout_seconds,
        quote_timeout_seconds=config.quote_timeout_seconds,
    )

    def preflight_runner(preflight_config: ReadOnlyPreflightConfig, run_id: str):
        return run_read_only_preflight(
            config=preflight_config,
            transport=IbkrReadOnlyTwsTransport(config=transport_config),
            run_id=run_id,
        )

    return run_paper_proof(config=paper_config, preflight_runner=preflight_runner, proof_runner=proof_runner)


def _run_operator_status(config: TrackBStrategyPaperRunnerConfig, runner_report_json: Path) -> None:
    create_operator_status_summary(
        inputs=OperatorStatusInputs(
            track_b_strategy_paper_runner_report_json=runner_report_json,
            output_root=config.operator_status_output_root,
        )
    )


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
        proof=proof,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        proof_classification=proof_classification,
    )
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
    proof_report = proof.report if proof else {}
    proof_payload = proof_report.get("proof_payload") if isinstance(proof_report.get("proof_payload"), Mapping) else {}
    paper_proof_invoked = proof is not None
    submit_allowed = _paper_submit_requested(config) and proof is not None
    return {
        "schema_version": "track_b_strategy_paper_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_strategy_paper_runner_id": runner_id,
        "strategy_paper_runner_verdict": verdict.value,
        "mode": config.mode,
        "source_id": config.source_id,
        "strategy_id": config.strategy_id,
        "lane_id": config.lane_id,
        "rule_id": config.rule_id,
        "rule_mode": config.rule_mode,
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
        "paper_submit_requested": _paper_submit_requested(config),
        "paper_submit_flags_present": bool(config.submit_paper and config.confirm_paper_submit),
        "paper_proof_invoked": paper_proof_invoked,
        "paper_proof_classification": proof_classification or (proof.classification.value if proof else None),
        "paper_proof_report_path": str(proof.report_json) if proof else None,
        "paper_proof_lifecycle_status": proof_payload.get("proof_lifecycle_status"),
        "final_position_status": _final_position_status(proof_payload),
        "final_flat": _final_flat(proof_payload, proof),
        "quantity": config.quantity,
        "order_type": config.order_type,
        "time_in_force": config.time_in_force,
        "manual_open_limit_price": str(config.manual_open_limit_price) if config.manual_open_limit_price is not None else None,
        "manual_close_limit_price": str(config.manual_close_limit_price) if config.manual_close_limit_price is not None else None,
        "primary_blocker": None if primary_blocker is None else str(primary_blocker),
        "secondary_blockers": _secondary_blockers(strategy_report, readiness_report, proof_report),
        "required_next_action": required_next_action,
        "submit_allowed": submit_allowed,
        "submit_attempted": paper_proof_invoked,
        "live_money_readiness": False,
        "paper_proof_cli_called": paper_proof_invoked,
        "broker_state_mutated": paper_proof_invoked,
        "ui_authority": False,
        "hidden_submit": False,
        "live_money_submit_allowed": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_strategy_paper_runner_report.json"),
        "artifact_paths": {
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
            "paper_proof_report_json": str(proof.report_json) if proof else None,
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


def _candle_history_producer_requested(config: TrackBStrategyPaperRunnerConfig) -> bool:
    return (
        config.candle_history_json is not None
        or config.candle_history_payload is not None
        or config.current_quote_report_json is not None
        or config.current_quote_report_payload is not None
    )


def _candle_history_producer_request_error(config: TrackBStrategyPaperRunnerConfig) -> str | None:
    has_history = config.candle_history_json is not None or config.candle_history_payload is not None
    has_current_quote = config.current_quote_report_json is not None or config.current_quote_report_payload is not None
    if has_history and not has_current_quote:
        return "Candle-history PAPER path requires a current quote report JSON/payload."
    if has_current_quote and not has_history:
        return "Candle-history PAPER path requires bounded candle history JSON/payload."
    return None


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
    if config.quantity is None:
        return "--quantity is required for PAPER submit."
    if int(config.quantity) != 1:
        return "--quantity must be exactly 1 for the current MGC paper proof lifecycle."
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


def _final_flat(proof_payload: Mapping[str, object], proof: PaperProofResult | None) -> bool | None:
    if proof is None:
        return None
    if proof.classification == TerminalClassification.PASSED and proof_payload.get("proof_lifecycle_status") == "PROOF_COMPLETE_FLAT":
        return True
    return (
        proof.classification == TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE
        and proof_payload.get("proof_lifecycle_status") == "PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
    )


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
