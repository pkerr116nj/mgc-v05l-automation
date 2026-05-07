"""Track B no-submit strategy rule runner.

This Phase 2 boundary is intentionally narrow. It evaluates one configured
MGC-only rule against existing Databento quote/candle evidence, then delegates
signal production to the established strategy signal adapter path. It does not
submit, create order plans, run paper proof, or infer execution authority.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .candle_signal_producer import DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT
from .models import require_aware_datetime, to_jsonable
from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT
from .strategy_signal_adapter import (
    DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT,
    StrategySignalAdapterResult,
    StrategySignalAdapterVerdict,
    adapt_demo_candle_direction_signal,
)
from .track_b_strategy_registry import (
    TrackBStrategyRegistryEntry,
    validate_strategy_event_against_registry,
)


DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_strategy_rule_runner")
MGC_CONTRACT_KEY = "MGC-202606"
MGC_INSTRUMENT_FAMILY = "MGC"
MNQ_CONTRACT_KEY = "MNQ-202606"
MNQ_INSTRUMENT_FAMILY = "MNQ"
DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID = "mgc_ema_momentum_reclaim_long_v1"
DEFAULT_ASIAN_DRIFT_RULE_ID = "asian_drift_v1"
DEFAULT_ASIA_EARLY_PAUSE_RESUME_SHORT_RULE_ID = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
DEFAULT_ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_RULE_ID = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
DEFAULT_FIRST_BULL_SNAP_TURN_RULE_ID = "FIRST_BULL_SNAP_TURN_V1"
DEFAULT_FIRST_BEAR_SNAP_TURN_RULE_ID = "FIRST_BEAR_SNAP_TURN_V1"
DEFAULT_LONDON_LATE_PAUSE_RESUME_SHORT_RULE_ID = "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
DEFAULT_ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_RULE_ID = "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1"
DEFAULT_US_DERIVATIVE_BEAR_TURN_RULE_ID = "US_DERIVATIVE_BEAR_TURN_V1"
DEFAULT_MNQ_US_DERIVATIVE_BEAR_TURN_RULE_ID = "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
DEFAULT_MNQ_FIRST_BEAR_SNAP_TURN_RULE_ID = "MNQ_FIRST_BEAR_SNAP_TURN_V1"
DEFAULT_MNQ_FIRST_BULL_SNAP_TURN_RULE_ID = "MNQ_FIRST_BULL_SNAP_TURN_V1"
DEFAULT_US_LATE_PAUSE_RESUME_LONG_RULE_ID = "US_LATE_PAUSE_RESUME_LONG_V1"


class TrackBStrategyRuleRunnerVerdict(str, Enum):
    EMITTED_SIGNAL = "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL"
    HUMAN_REVIEW_NO_SIGNAL = "TRACK_B_STRATEGY_RULE_RUNNER_HUMAN_REVIEW_NO_SIGNAL"
    NO_SIGNAL = "TRACK_B_STRATEGY_RULE_RUNNER_NO_SIGNAL"
    BLOCKED_INVALID_INPUT = "TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_INVALID_INPUT"
    BLOCKED_NON_REALTIME_INPUT = "TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_NON_REALTIME_INPUT"
    BLOCKED_DOWNSTREAM_REJECTED = "TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_DOWNSTREAM_REJECTED"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_SCHEMA_ERROR"


class TrackBStrategyRuleMode(str, Enum):
    DEMO_LONG_ONLY = "DEMO_LONG_ONLY"
    MGC_EMA_MOMENTUM_RECLAIM_LONG = "MGC_EMA_MOMENTUM_RECLAIM_LONG"
    ASIAN_DRIFT_V1 = "ASIAN_DRIFT_V1"
    ASIA_EARLY_PAUSE_RESUME_SHORT_V1 = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1 = "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    FIRST_BULL_SNAP_TURN_V1 = "FIRST_BULL_SNAP_TURN_V1"
    FIRST_BEAR_SNAP_TURN_V1 = "FIRST_BEAR_SNAP_TURN_V1"
    LONDON_LATE_PAUSE_RESUME_SHORT_V1 = "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
    ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1 = "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1"
    US_DERIVATIVE_BEAR_TURN_V1 = "US_DERIVATIVE_BEAR_TURN_V1"
    MNQ_US_DERIVATIVE_BEAR_TURN_V1 = "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    MNQ_FIRST_BEAR_SNAP_TURN_V1 = "MNQ_FIRST_BEAR_SNAP_TURN_V1"
    MNQ_FIRST_BULL_SNAP_TURN_V1 = "MNQ_FIRST_BULL_SNAP_TURN_V1"
    US_LATE_PAUSE_RESUME_LONG_V1 = "US_LATE_PAUSE_RESUME_LONG_V1"
    HUMAN_REVIEW_ONLY = "HUMAN_REVIEW_ONLY"


class TrackBStrategyRuleDecision(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_SIGNAL = "NO_SIGNAL"
    HUMAN_REVIEW = "HUMAN_REVIEW"


@dataclass(frozen=True)
class TrackBStrategyRuleRunnerResult:
    verdict: TrackBStrategyRuleRunnerVerdict
    report_json: Path
    report: dict[str, Any]
    downstream_strategy_adapter_report_json: Path | None
    downstream_candle_producer_report_json: Path | None
    downstream_signal_batch_writer_report_json: Path | None
    output_batch_json: Path | None


def run_track_b_strategy_rule(
    *,
    input_event_payload: Mapping[str, Any],
    input_event_path: Path | None,
    inbox_dir: Path,
    expected_account_id: str | None = None,
    source_id: str | None = None,
    strategy_id: str | None = None,
    lane_id: str | None = None,
    rule_id: str = DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
    rule_mode: str | TrackBStrategyRuleMode = TrackBStrategyRuleMode.MGC_EMA_MOMENTUM_RECLAIM_LONG,
    emit_signal: bool = False,
    allow_fixture_input: bool = False,
    output_root: Path = DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT,
    strategy_adapter_output_root: Path = DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT,
    candle_producer_output_root: Path = DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT,
    writer_output_root: Path = DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT,
    runner_id: str | None = None,
    now: datetime | None = None,
) -> TrackBStrategyRuleRunnerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_runner_id = runner_id or f"track_b_strategy_rule_runner_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_runner_id / "track_b_strategy_rule_runner_report.json"
    actual_source_id = source_id or _optional_text(input_event_payload.get("source_id")) or "track_b_strategy_rule_runner"
    registry_entry: TrackBStrategyRegistryEntry | None = None

    try:
        actual_rule_mode = _rule_mode(rule_mode)
        quote_evidence = _quote_evidence_from_event(input_event_payload)
        registry_entry, registry_blocker = validate_strategy_event_against_registry(
            event=input_event_payload,
            rule_mode=actual_rule_mode.value,
            rule_id=rule_id,
            strategy_id=strategy_id,
        )
        if registry_blocker:
            return _write_report(
                report_json=report_json,
                verdict=TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT,
                now=actual_now,
                runner_id=actual_runner_id,
                source_id=actual_source_id,
                rule_id=rule_id,
                rule_mode=actual_rule_mode,
                registry_entry=registry_entry,
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                rule_evaluation={},
                decision=TrackBStrategyRuleDecision.NO_SIGNAL,
                decision_reason=registry_blocker,
                signal_emitted=False,
                signal_direction=None,
                downstream_adapter=None,
                primary_blocker=registry_blocker,
                required_next_action="Provide an explicitly registered Track B strategy payload with all required feature/state fields before evaluation.",
            )
        validation_blocker = _validate_input(
            event=input_event_payload,
            quote_evidence=quote_evidence,
            expected_account_id=expected_account_id,
            allow_fixture_input=allow_fixture_input,
            rule_mode=actual_rule_mode,
            registry_entry=registry_entry,
        )
        if validation_blocker:
            verdict = (
                TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT
                if "realtime" in validation_blocker.lower() or "fixture" in validation_blocker.lower()
                else TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT
            )
            return _write_report(
                report_json=report_json,
                verdict=verdict,
                now=actual_now,
                runner_id=actual_runner_id,
                source_id=actual_source_id,
                rule_id=rule_id,
                rule_mode=actual_rule_mode,
                registry_entry=registry_entry,
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                rule_evaluation={},
                decision=TrackBStrategyRuleDecision.NO_SIGNAL,
                decision_reason=validation_blocker,
                signal_emitted=False,
                signal_direction=None,
                downstream_adapter=None,
                primary_blocker=validation_blocker,
                required_next_action="Provide a valid realtime MGC Databento quote/candle event before evaluating this rule.",
            )

        if actual_rule_mode == TrackBStrategyRuleMode.HUMAN_REVIEW_ONLY:
            return _write_report(
                report_json=report_json,
                verdict=TrackBStrategyRuleRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL,
                now=actual_now,
                runner_id=actual_runner_id,
                source_id=actual_source_id,
                rule_id=rule_id,
                rule_mode=actual_rule_mode,
                registry_entry=registry_entry,
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                rule_evaluation={},
                decision=TrackBStrategyRuleDecision.HUMAN_REVIEW,
                decision_reason="Rule mode is HUMAN_REVIEW_ONLY; no Track B signal batch was emitted.",
                signal_emitted=False,
                signal_direction=None,
                downstream_adapter=None,
                primary_blocker=None,
                required_next_action="Review the rule report. Use --rule-mode DEMO_LONG_ONLY --emit-signal for the explicit no-submit wiring proof.",
            )
        rule_decision = _evaluate_rule_decision(
            event=input_event_payload,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            rule_mode=actual_rule_mode,
        )
        if rule_decision["decision"] not in {TrackBStrategyRuleDecision.LONG, TrackBStrategyRuleDecision.SHORT}:
            return _write_report(
                report_json=report_json,
                verdict=TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
                now=actual_now,
                runner_id=actual_runner_id,
                source_id=actual_source_id,
                rule_id=rule_id,
                rule_mode=actual_rule_mode,
                registry_entry=registry_entry,
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                rule_evaluation=rule_decision,
                decision=TrackBStrategyRuleDecision.NO_SIGNAL,
                decision_reason=str(rule_decision["decision_reason"]),
                signal_emitted=False,
                signal_direction=None,
                downstream_adapter=None,
                primary_blocker=None,
                required_next_action="No Track B signal was emitted; continue observation until rule conditions pass.",
            )

        if not emit_signal:
            return _write_report(
                report_json=report_json,
                verdict=TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
                now=actual_now,
                runner_id=actual_runner_id,
                source_id=actual_source_id,
                rule_id=rule_id,
                rule_mode=actual_rule_mode,
                registry_entry=registry_entry,
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                rule_evaluation=rule_decision,
                decision=TrackBStrategyRuleDecision.NO_SIGNAL,
                decision_reason="Rule conditions passed, but --emit-signal was not supplied.",
                signal_emitted=False,
                signal_direction=None,
                downstream_adapter=None,
                primary_blocker=None,
                required_next_action="Re-run with --emit-signal only when intentionally producing no-submit listener inbox work.",
            )

        strategy_event = _strategy_event_for_adapter(
            event=input_event_payload,
            source_id=actual_source_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            expected_account_id=expected_account_id,
            rule_id=rule_id,
            rule_mode=actual_rule_mode,
            signal_direction=str(rule_decision["decision"].value),
        )
        adapter = adapt_demo_candle_direction_signal(
            strategy_event_payload=strategy_event,
            inbox_dir=Path(inbox_dir),
            expected_account_id=expected_account_id,
            source_id=actual_source_id,
            output_root=strategy_adapter_output_root,
            candle_producer_output_root=candle_producer_output_root,
            writer_output_root=writer_output_root,
            adapter_id=f"{actual_runner_id}_strategy_signal_adapter",
            now=actual_now,
        )
        if adapter.verdict != StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH:
            return _write_report(
                report_json=report_json,
                verdict=TrackBStrategyRuleRunnerVerdict.BLOCKED_DOWNSTREAM_REJECTED,
                now=actual_now,
                runner_id=actual_runner_id,
                source_id=actual_source_id,
                rule_id=rule_id,
                rule_mode=actual_rule_mode,
                registry_entry=registry_entry,
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                rule_evaluation=rule_decision,
                decision=rule_decision["decision"],
                decision_reason=f"{actual_rule_mode.value} produced explicit {rule_decision['decision'].value}, but downstream adapter rejected the event.",
                signal_emitted=False,
                signal_direction=str(rule_decision["decision"].value),
                downstream_adapter=adapter,
                primary_blocker=str(adapter.report.get("primary_blocker") or "Strategy signal adapter rejected rule output."),
                required_next_action=str(adapter.report.get("required_next_action") or "Review strategy adapter report before retrying."),
            )
        return _write_report(
            report_json=report_json,
            verdict=TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL,
            now=actual_now,
            runner_id=actual_runner_id,
            source_id=actual_source_id,
            rule_id=rule_id,
            rule_mode=actual_rule_mode,
            registry_entry=registry_entry,
            input_event_path=input_event_path,
            input_event=input_event_payload,
            quote_evidence=quote_evidence,
            rule_evaluation=rule_decision,
            decision=rule_decision["decision"],
            decision_reason=str(rule_decision["decision_reason"]),
            signal_emitted=True,
            signal_direction=str(rule_decision["decision"].value),
            downstream_adapter=adapter,
            primary_blocker=None,
            required_next_action="Let shadow_listener process the no-submit strategy-rule signal batch; paper proof remains a separate explicit operator decision.",
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_report(
            report_json=report_json,
            verdict=TrackBStrategyRuleRunnerVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            runner_id=actual_runner_id,
            source_id=actual_source_id,
            rule_id=rule_id,
            rule_mode=_rule_mode_or_default(rule_mode),
            registry_entry=registry_entry,
            input_event_path=input_event_path,
            input_event=input_event_payload,
            quote_evidence={},
            rule_evaluation={},
            decision=TrackBStrategyRuleDecision.NO_SIGNAL,
            decision_reason=str(exc),
            signal_emitted=False,
            signal_direction=None,
            downstream_adapter=None,
            primary_blocker=str(exc),
            required_next_action="Fix Track B strategy rule runner input schema before retrying.",
        )


def _quote_evidence_from_event(event: Mapping[str, Any]) -> dict[str, Any]:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    evidence: dict[str, Any] = {
        "input_quote_provider_mode": metadata.get("quote_provider_mode") or event.get("quote_provider_mode"),
        "realtime_quote_received": metadata.get("realtime_quote_received") if "realtime_quote_received" in metadata else event.get("realtime_quote_received"),
        "current_quote_available": metadata.get("current_quote_available") if "current_quote_available" in metadata else event.get("current_quote_available"),
        "quote_freshness_verdict": metadata.get("quote_freshness_verdict") or event.get("quote_freshness_verdict"),
        "quote_report_path": metadata.get("source_report_path") or event.get("source_report_path"),
    }
    quote_report_path = _optional_text(evidence.get("quote_report_path"))
    quote_report = _read_optional_json(quote_report_path)
    if quote_report:
        evidence.update(
            {
                "input_quote_provider_mode": evidence.get("input_quote_provider_mode") or quote_report.get("quote_provider_mode") or quote_report.get("market_data_mode"),
                "realtime_quote_received": _coalesce_bool(evidence.get("realtime_quote_received"), quote_report.get("realtime_quote_received")),
                "current_quote_available": _coalesce_bool(evidence.get("current_quote_available"), quote_report.get("current_quote_available")),
                "quote_freshness_verdict": evidence.get("quote_freshness_verdict") or quote_report.get("quote_freshness_verdict"),
                "quote_timestamp": quote_report.get("timestamp") or event.get("candle_timestamp") or event.get("observed_at"),
                "quote_age_seconds": quote_report.get("quote_age_seconds"),
                "quote_status": quote_report.get("quote_status") or quote_report.get("classification"),
                "quote_provider_report_json_path": quote_report.get("report_json_path") or quote_report_path,
            }
        )
    else:
        evidence["quote_timestamp"] = event.get("candle_timestamp") or event.get("observed_at")
        evidence["quote_provider_report_json_path"] = quote_report_path
    return evidence


def _validate_input(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    expected_account_id: str | None,
    allow_fixture_input: bool,
    rule_mode: TrackBStrategyRuleMode,
    registry_entry: TrackBStrategyRegistryEntry | None = None,
) -> str | None:
    account_id = _optional_text(event.get("account_id") or event.get("expected_account_id"))
    if expected_account_id and account_id and account_id != expected_account_id:
        return f"Event account_id {account_id} does not match expected_account_id {expected_account_id}."
    contract_key = _optional_text(event.get("local_execution_contract_key") or event.get("contract_key"))
    expected_family = registry_entry.instrument_family if registry_entry is not None else MGC_INSTRUMENT_FAMILY
    expected_contract_prefix = _contract_prefix_for_instrument_family(expected_family)
    if expected_contract_prefix and (contract_key is None or not contract_key.startswith(expected_contract_prefix)):
        return (
            f"Track B strategy rule runner expected a {expected_family} contract_key "
            f"with prefix {expected_contract_prefix}; observed {contract_key}."
        )
    instrument_family = _optional_text(event.get("instrument_family") or event.get("symbol"))
    if instrument_family and instrument_family != expected_family:
        return f"Track B strategy rule runner expected instrument_family={expected_family}; observed {instrument_family}."
    if _optional_text(event.get("close")) is None and _optional_text(event.get("last")) is None:
        return "Realtime quote/candle event must include close or last price evidence."

    provider_mode = _optional_text(quote_evidence.get("input_quote_provider_mode"))
    realtime_received = quote_evidence.get("realtime_quote_received") is True
    current_available = quote_evidence.get("current_quote_available") is True
    if allow_fixture_input:
        return None
    if provider_mode != "REALTIME":
        return "Input is not explicitly REALTIME; fixture/historical/current-available-end evidence cannot produce a strategy signal."
    if not realtime_received:
        return "Input realtime_quote_received is not true."
    if not current_available:
        return "Input current_quote_available is not true."
    if rule_mode == TrackBStrategyRuleMode.ASIAN_DRIFT_V1:
        return _validate_asian_drift_snapshot(event)
    if rule_mode == TrackBStrategyRuleMode.ASIA_EARLY_PAUSE_RESUME_SHORT_V1:
        return _validate_asia_early_pause_resume_short_snapshot(event)
    if rule_mode == TrackBStrategyRuleMode.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1:
        return _validate_asia_early_normal_breakout_retest_hold_long_snapshot(event)
    if rule_mode == TrackBStrategyRuleMode.FIRST_BULL_SNAP_TURN_V1:
        return _validate_first_snap_turn_snapshot(
            event,
            state_key="first_bull_snap_turn_state",
            features_key="first_bull_snap_turn_features",
            feature_version="first_bull_snap_turn_v1_phase1",
            label="First Bull Snap Turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.FIRST_BEAR_SNAP_TURN_V1:
        return _validate_first_snap_turn_snapshot(
            event,
            state_key="first_bear_snap_turn_state",
            features_key="first_bear_snap_turn_features",
            feature_version="first_bear_snap_turn_v1_phase1",
            label="First Bear Snap Turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.LONDON_LATE_PAUSE_RESUME_SHORT_V1:
        return _validate_session_strategy_snapshot(
            event,
            state_key="london_late_pause_resume_short_state",
            features_key="london_late_pause_resume_short_features",
            feature_version="london_late_pause_resume_short_v1_phase1",
            label="London Late pause-resume short v1",
        )
    if rule_mode == TrackBStrategyRuleMode.ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1:
        return _validate_session_strategy_snapshot(
            event,
            state_key="asia_late_flat_pullback_pause_resume_long_state",
            features_key="asia_late_flat_pullback_pause_resume_long_features",
            feature_version="asia_late_flat_pullback_pause_resume_long_v1_phase1",
            label="Asia Late flat-pullback pause-resume long v1",
        )
    if rule_mode == TrackBStrategyRuleMode.US_DERIVATIVE_BEAR_TURN_V1:
        return _validate_session_strategy_snapshot(
            event,
            state_key="us_derivative_bear_turn_state",
            features_key="us_derivative_bear_turn_features",
            feature_version="us_derivative_bear_turn_v1_phase1",
            label="US derivative bear turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.MNQ_US_DERIVATIVE_BEAR_TURN_V1:
        return _validate_session_strategy_snapshot(
            event,
            state_key="mnq_us_derivative_bear_turn_state",
            features_key="mnq_us_derivative_bear_turn_features",
            feature_version="mnq_us_derivative_bear_turn_v1_phase1",
            label="MNQ US derivative bear turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.MNQ_FIRST_BEAR_SNAP_TURN_V1:
        return _validate_first_snap_turn_snapshot(
            event,
            state_key="mnq_first_bear_snap_turn_state",
            features_key="mnq_first_bear_snap_turn_features",
            feature_version="mnq_first_bear_snap_turn_v1_phase1",
            label="MNQ First Bear Snap Turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.MNQ_FIRST_BULL_SNAP_TURN_V1:
        return _validate_first_snap_turn_snapshot(
            event,
            state_key="mnq_first_bull_snap_turn_state",
            features_key="mnq_first_bull_snap_turn_features",
            feature_version="mnq_first_bull_snap_turn_v1_phase1",
            label="MNQ First Bull Snap Turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.US_LATE_PAUSE_RESUME_LONG_V1:
        return _validate_session_strategy_snapshot(
            event,
            state_key="us_late_pause_resume_long_state",
            features_key="us_late_pause_resume_long_features",
            feature_version="us_late_pause_resume_long_v1_phase1",
            label="US Late pause-resume long v1",
        )
    return None


def _contract_prefix_for_instrument_family(instrument_family: str | None) -> str | None:
    if instrument_family == MNQ_INSTRUMENT_FAMILY:
        return "MNQ-"
    if instrument_family == MGC_INSTRUMENT_FAMILY:
        return "MGC-"
    return None


def _validate_asian_drift_snapshot(event: Mapping[str, Any]) -> str | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    required = {
        "asia_drift_state": _first_text(event, metadata, "asia_drift_state", "state"),
        "asia_drift_regime": _first_text(event, metadata, "asia_drift_regime", "regime"),
        "entry_window_open": _first_bool(event, metadata, "entry_window_open"),
        "in_scope": _first_bool(event, metadata, "in_scope"),
        "hypothetical_entry_ready": _first_bool(event, metadata, "hypothetical_entry_ready"),
        "timeframe": _optional_text(event.get("timeframe") or metadata.get("timeframe")),
        "feature_version": _first_text(event, metadata, "feature_version"),
        "calibration_profile": _first_text(event, metadata, "calibration_profile"),
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        return "Asian Drift v1 requires explicit research state/feature snapshot fields: " + ", ".join(missing)
    if required["timeframe"] != "5m":
        return "Asian Drift v1 Track B watch requires completed 5m decision-bar state."
    return None


def _validate_asia_early_pause_resume_short_snapshot(event: Mapping[str, Any]) -> str | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = metadata.get("asia_early_pause_resume_short_state")
    features = metadata.get("asia_early_pause_resume_short_features")
    if not isinstance(state, Mapping) or not isinstance(features, Mapping):
        return "Asia Early pause-resume short v1 requires explicit state and feature envelopes."
    timeframe = _optional_text(event.get("timeframe") or state.get("timeframe") or metadata.get("timeframe"))
    if timeframe != "5m":
        return "Asia Early pause-resume short v1 Track B watch requires completed 5m decision-bar state."
    if _optional_text(features.get("feature_version")) != "asia_early_pause_resume_short_v1_phase1":
        return "Asia Early pause-resume short v1 requires feature_version=asia_early_pause_resume_short_v1_phase1."
    if _optional_text(features.get("calibration_profile")) != "probationary_baseline_v1":
        return "Asia Early pause-resume short v1 requires calibration_profile=probationary_baseline_v1."
    return None


def _validate_asia_early_normal_breakout_retest_hold_long_snapshot(event: Mapping[str, Any]) -> str | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = metadata.get("asia_early_normal_breakout_retest_hold_long_state")
    features = metadata.get("asia_early_normal_breakout_retest_hold_long_features")
    if not isinstance(state, Mapping) or not isinstance(features, Mapping):
        return "Asia Early normal breakout-retest-hold long v1 requires explicit state and feature envelopes."
    timeframe = _optional_text(event.get("timeframe") or state.get("timeframe") or metadata.get("timeframe"))
    if timeframe != "5m":
        return "Asia Early normal breakout-retest-hold long v1 Track B watch requires completed 5m decision-bar state."
    if _optional_text(features.get("feature_version")) != "asia_early_normal_breakout_retest_hold_long_v1_phase1":
        return "Asia Early normal breakout-retest-hold long v1 requires feature_version=asia_early_normal_breakout_retest_hold_long_v1_phase1."
    if _optional_text(features.get("calibration_profile")) != "probationary_baseline_v1":
        return "Asia Early normal breakout-retest-hold long v1 requires calibration_profile=probationary_baseline_v1."
    return None


def _validate_first_snap_turn_snapshot(
    event: Mapping[str, Any],
    *,
    state_key: str,
    features_key: str,
    feature_version: str,
    label: str,
) -> str | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = metadata.get(state_key)
    features = metadata.get(features_key)
    if not isinstance(state, Mapping) or not isinstance(features, Mapping):
        return f"{label} requires explicit state and feature envelopes."
    timeframe = _optional_text(event.get("timeframe") or state.get("timeframe") or metadata.get("timeframe"))
    if timeframe != "5m":
        return f"{label} Track B watch requires completed 5m decision-bar state."
    if _optional_text(features.get("feature_version")) != feature_version:
        return f"{label} requires feature_version={feature_version}."
    if _optional_text(features.get("calibration_profile")) != "probationary_baseline_v1":
        return f"{label} requires calibration_profile=probationary_baseline_v1."
    return None


def _validate_session_strategy_snapshot(
    event: Mapping[str, Any],
    *,
    state_key: str,
    features_key: str,
    feature_version: str,
    label: str,
) -> str | None:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = metadata.get(state_key)
    features = metadata.get(features_key)
    if not isinstance(state, Mapping) or not isinstance(features, Mapping):
        return f"{label} requires explicit state and feature envelopes."
    timeframe = _optional_text(event.get("timeframe") or state.get("timeframe") or metadata.get("timeframe"))
    if timeframe != "5m":
        return f"{label} Track B watch requires completed 5m decision-bar state."
    if _optional_text(features.get("feature_version")) != feature_version:
        return f"{label} requires feature_version={feature_version}."
    if _optional_text(features.get("calibration_profile")) != "probationary_baseline_v1":
        return f"{label} requires calibration_profile=probationary_baseline_v1."
    return None


def _evaluate_rule_decision(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
    rule_mode: TrackBStrategyRuleMode,
) -> dict[str, Any]:
    if rule_mode == TrackBStrategyRuleMode.DEMO_LONG_ONLY:
        return {
            "rule_name": "mgc_realtime_quote_demo_long",
            "decision": TrackBStrategyRuleDecision.LONG,
            "decision_reason": "DEMO_LONG_ONLY emitted explicit LONG from valid realtime Databento MGC quote evidence.",
            "rule_inputs": {},
            "rule_conditions": {"demo_long_only": True},
            "rule_blockers": [],
            "research_lineage": "Track B wiring proof only; not a production strategy rule.",
        }
    if rule_mode == TrackBStrategyRuleMode.MGC_EMA_MOMENTUM_RECLAIM_LONG:
        return _evaluate_mgc_ema_momentum_reclaim_long(event=event, quote_evidence=quote_evidence, rule_id=rule_id)
    if rule_mode == TrackBStrategyRuleMode.ASIAN_DRIFT_V1:
        return _evaluate_asian_drift_v1(event=event, quote_evidence=quote_evidence, rule_id=rule_id)
    if rule_mode == TrackBStrategyRuleMode.ASIA_EARLY_PAUSE_RESUME_SHORT_V1:
        return _evaluate_asia_early_pause_resume_short_v1(event=event, quote_evidence=quote_evidence, rule_id=rule_id)
    if rule_mode == TrackBStrategyRuleMode.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1:
        return _evaluate_asia_early_normal_breakout_retest_hold_long_v1(event=event, quote_evidence=quote_evidence, rule_id=rule_id)
    if rule_mode == TrackBStrategyRuleMode.FIRST_BULL_SNAP_TURN_V1:
        return _evaluate_first_snap_turn_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            direction=TrackBStrategyRuleDecision.LONG,
            state_key="first_bull_snap_turn_state",
            features_key="first_bull_snap_turn_features",
            predicate_prefix="bull",
            rule_name="first_bull_snap_turn_v1",
        )
    if rule_mode == TrackBStrategyRuleMode.FIRST_BEAR_SNAP_TURN_V1:
        return _evaluate_first_snap_turn_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            direction=TrackBStrategyRuleDecision.SHORT,
            state_key="first_bear_snap_turn_state",
            features_key="first_bear_snap_turn_features",
            predicate_prefix="bear",
            rule_name="first_bear_snap_turn_v1",
        )
    if rule_mode == TrackBStrategyRuleMode.LONDON_LATE_PAUSE_RESUME_SHORT_V1:
        return _evaluate_london_late_pause_resume_short_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
        )
    if rule_mode == TrackBStrategyRuleMode.ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1:
        return _evaluate_asia_late_flat_pullback_pause_resume_long_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
        )
    if rule_mode == TrackBStrategyRuleMode.US_DERIVATIVE_BEAR_TURN_V1:
        return _evaluate_us_derivative_bear_turn_v1(event=event, quote_evidence=quote_evidence, rule_id=rule_id)
    if rule_mode == TrackBStrategyRuleMode.MNQ_US_DERIVATIVE_BEAR_TURN_V1:
        return _evaluate_us_derivative_bear_turn_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            state_key="mnq_us_derivative_bear_turn_state",
            features_key="mnq_us_derivative_bear_turn_features",
            rule_name="mnq_us_derivative_bear_turn_v1",
            label="MNQ US derivative bear turn v1",
        )
    if rule_mode == TrackBStrategyRuleMode.MNQ_FIRST_BEAR_SNAP_TURN_V1:
        return _evaluate_first_snap_turn_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            direction=TrackBStrategyRuleDecision.SHORT,
            state_key="mnq_first_bear_snap_turn_state",
            features_key="mnq_first_bear_snap_turn_features",
            predicate_prefix="bear",
            rule_name="mnq_first_bear_snap_turn_v1",
        )
    if rule_mode == TrackBStrategyRuleMode.MNQ_FIRST_BULL_SNAP_TURN_V1:
        return _evaluate_first_snap_turn_v1(
            event=event,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            direction=TrackBStrategyRuleDecision.LONG,
            state_key="mnq_first_bull_snap_turn_state",
            features_key="mnq_first_bull_snap_turn_features",
            predicate_prefix="bull",
            rule_name="mnq_first_bull_snap_turn_v1",
        )
    if rule_mode == TrackBStrategyRuleMode.US_LATE_PAUSE_RESUME_LONG_V1:
        return _evaluate_us_late_pause_resume_long_v1(event=event, quote_evidence=quote_evidence, rule_id=rule_id)
    raise ValueError(f"Unsupported rule mode: {rule_mode.value}")


def _evaluate_asian_drift_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = event.get("metadata") if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = _first_text(event, metadata, "asia_drift_state", "state")
    regime = _first_text(event, metadata, "asia_drift_regime", "regime")
    direction = _asian_drift_direction(event, metadata, regime)
    entry_ready = _first_bool(event, metadata, "hypothetical_entry_ready")
    entry_window_open = _first_bool(event, metadata, "entry_window_open")
    in_scope = _first_bool(event, metadata, "in_scope")
    session_timeout = _first_bool(event, metadata, "session_timeout")
    feature_version = _first_text(event, metadata, "feature_version")
    calibration_profile = _first_text(event, metadata, "calibration_profile")
    signal_states = {"ENTRY_ARMED", "REQUALIFIED_CANDIDATE"}
    direction_required = state in signal_states or entry_ready is True
    conditions = {
        "state_is_entry_eligible": state in signal_states,
        "direction_is_explicit": direction in {"LONG", "SHORT"} if direction_required else True,
        "entry_ready": entry_ready is True,
        "entry_window_open": entry_window_open is True,
        "in_scope": in_scope is True,
        "session_not_timeout": session_timeout is not True,
        "feature_version_present": feature_version is not None,
        "calibration_profile_present": calibration_profile is not None,
    }
    failed = [name for name, passed in conditions.items() if passed is False]
    blocker_labels = {
        "state_is_entry_eligible": "Asian Drift state is not entry eligible.",
        "direction_is_explicit": "Asian Drift direction is not explicit LONG/SHORT.",
        "entry_ready": "Asian Drift hypothetical entry readiness is false.",
        "entry_window_open": "Asian Drift entry window is closed.",
        "in_scope": "Asian Drift is outside execution session scope.",
        "session_not_timeout": "Asian Drift session timed out.",
        "feature_version_present": "Asian Drift feature version is missing.",
        "calibration_profile_present": "Asian Drift calibration profile is missing.",
    }
    blockers = [blocker_labels.get(name, name) for name in failed]
    if direction_required and direction not in {"LONG", "SHORT"} and blocker_labels["direction_is_explicit"] not in blockers:
        blockers.append("Asian Drift direction is not explicit LONG/SHORT.")
    if failed:
        decision_reason = "Asian Drift v1 conditions did not pass: " + ", ".join(failed)
    else:
        decision_reason = f"Asian Drift v1 explicit state snapshot is entry-ready for {direction}."
    return {
        "rule_name": "asian_drift_v1_state_snapshot",
        "decision": TrackBStrategyRuleDecision(direction) if direction in {"LONG", "SHORT"} and not failed else TrackBStrategyRuleDecision.NO_SIGNAL,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "asia_drift_state": state,
            "asia_drift_regime": regime,
            "direction": direction,
            "direction_required": direction_required,
            "entry_window_open": entry_window_open,
            "in_scope": in_scope,
            "session_timeout": session_timeout,
            "hypothetical_entry_ready": entry_ready,
            "feature_version": feature_version,
            "calibration_profile": calibration_profile,
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Based on docs/specs/ASIA_DRIFT_V1_RESEARCH_SPEC.md and the research/asia_drift "
            "feature/state snapshot contract. Track B does not infer these fields from raw candles."
        ),
        "rule_id": rule_id,
    }


def _evaluate_mgc_ema_momentum_reclaim_long(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    features = metadata.get("ema_momentum_features") if isinstance(metadata.get("ema_momentum_features") or {}, Mapping) else {}
    rule_config = metadata.get("strategy_rule_config") if isinstance(metadata.get("strategy_rule_config") or {}, Mapping) else {}
    blockers: list[str] = []

    close = _decimal_field(event, metadata, features, "close")
    vwap = _decimal_field(event, metadata, features, "vwap", "reference_vwap")
    prior_close = _decimal_field(event, metadata, features, "prior_close", "previous_close")
    momentum_norm = _decimal_field(event, metadata, features, "momentum_norm")
    momentum_acceleration = _decimal_field(event, metadata, features, "momentum_acceleration")
    momentum_turning_positive = _bool_field(event, metadata, features, "momentum_turning_positive")
    min_momentum_norm = _decimal_field(rule_config, metadata, features, "min_momentum_norm") or Decimal("0")
    min_momentum_acceleration = _decimal_field(rule_config, metadata, features, "min_momentum_acceleration") or Decimal("0")

    required_fields = {
        "close": close,
        "vwap": vwap,
        "prior_close": prior_close,
        "momentum_norm": momentum_norm,
        "momentum_acceleration": momentum_acceleration,
        "momentum_turning_positive": momentum_turning_positive,
    }
    for field_name, value in required_fields.items():
        if value is None:
            blockers.append(f"missing required EMA momentum rule field: {field_name}")

    conditions: dict[str, bool | None] = {
        "close_reclaimed_vwap": None,
        "prior_close_below_vwap": None,
        "momentum_turning_positive": momentum_turning_positive,
        "momentum_norm_at_or_above_threshold": None,
        "momentum_acceleration_at_or_above_threshold": None,
    }
    if close is not None and vwap is not None:
        conditions["close_reclaimed_vwap"] = close >= vwap
    if prior_close is not None and vwap is not None:
        conditions["prior_close_below_vwap"] = prior_close < vwap
    if momentum_norm is not None:
        conditions["momentum_norm_at_or_above_threshold"] = momentum_norm >= min_momentum_norm
    if momentum_acceleration is not None:
        conditions["momentum_acceleration_at_or_above_threshold"] = momentum_acceleration >= min_momentum_acceleration

    failed_conditions = [name for name, passed in conditions.items() if passed is False]
    if blockers:
        decision_reason = "MGC EMA momentum reclaim long rule could not evaluate: " + "; ".join(blockers)
    elif failed_conditions:
        decision_reason = "MGC EMA momentum reclaim long conditions did not pass: " + ", ".join(failed_conditions)
    else:
        decision_reason = (
            "MGC EMA momentum reclaim long conditions passed: close reclaimed VWAP, prior close was below VWAP, "
            "and EMA momentum turned positive with nonnegative acceleration."
        )

    return {
        "rule_name": "mgc_ema_momentum_reclaim_long",
        "decision": TrackBStrategyRuleDecision.LONG if not blockers and not failed_conditions else TrackBStrategyRuleDecision.NO_SIGNAL,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "close": None if close is None else str(close),
            "vwap": None if vwap is None else str(vwap),
            "prior_close": None if prior_close is None else str(prior_close),
            "momentum_norm": None if momentum_norm is None else str(momentum_norm),
            "momentum_acceleration": None if momentum_acceleration is None else str(momentum_acceleration),
            "momentum_turning_positive": momentum_turning_positive,
            "min_momentum_norm": str(min_momentum_norm),
            "min_momentum_acceleration": str(min_momentum_acceleration),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": "Based on research/ema_momentum.py causal EMA momentum and interpreted momentum-turning flags.",
        "rule_id": rule_id,
    }


def _evaluate_asia_early_pause_resume_short_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = metadata.get("asia_early_pause_resume_short_state") if isinstance(metadata.get("asia_early_pause_resume_short_state") or {}, Mapping) else {}
    features = metadata.get("asia_early_pause_resume_short_features") if isinstance(metadata.get("asia_early_pause_resume_short_features") or {}, Mapping) else {}

    normalized_curvature = _decimal_field(features, "normalized_curvature")
    max_normalized_curvature = _decimal_field(features, "max_normalized_curvature") or Decimal("-0.15")
    signal_range_expansion_ratio = _decimal_field(features, "signal_range_expansion_ratio")
    max_range_expansion_ratio = _decimal_field(features, "max_range_expansion_ratio") or Decimal("1.25")
    close = _decimal_field(event, features, "close")
    open_price = _decimal_field(event, features, "open")
    previous_close = _decimal_field(features, "previous_close", "prior_close")

    conditions: dict[str, bool | None] = {
        "rule_is_watch_only": True,
        "allow_asia": _bool_field(state, "allow_asia") is True,
        "session_asia": _bool_field(state, "session_asia") is True,
        "derivative_phase_asia_early": _optional_text(state.get("derivative_phase")) == "ASIA_EARLY",
        "close_below_open": close < open_price if close is not None and open_price is not None else None,
        "close_below_previous_close": close < previous_close if close is not None and previous_close is not None else None,
        "derivative_bear_close_weak": _bool_field(features, "derivative_bear_close_weak") is True,
        "derivative_bear_range_ok": _bool_field(features, "derivative_bear_range_ok") is True,
        "derivative_bear_body_ok": _bool_field(features, "derivative_bear_body_ok") is True,
        "derivative_bear_stretch_ok": _bool_field(features, "derivative_bear_stretch_ok") is True,
        "normalized_curvature_at_or_below_threshold": (
            normalized_curvature <= max_normalized_curvature
            if normalized_curvature is not None and max_normalized_curvature is not None
            else None
        ),
        "setup_bar_curvature_is_flat": _bool_field(features, "setup_bar_curvature_is_flat") is True,
        "signal_range_expansion_below_threshold": (
            signal_range_expansion_ratio < max_range_expansion_ratio
            if signal_range_expansion_ratio is not None and max_range_expansion_ratio is not None
            else None
        ),
        "one_bar_rebound_before_signal": _bool_field(features, "one_bar_rebound_before_signal") is True,
        "signal_breaks_prior_1_low": _bool_field(features, "signal_breaks_prior_1_low") is True,
        "close_below_fast_ema": _bool_field(features, "close_below_fast_ema") is True,
        "derivative_bear_cooldown_ok": _bool_field(features, "derivative_bear_cooldown_ok") is True,
        "no_competing_bear_short_candidate": _bool_field(features, "no_competing_bear_short_candidate") is True,
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    if failed:
        decision = TrackBStrategyRuleDecision.NO_SIGNAL
        decision_reason = "Asia Early pause-resume short v1 conditions did not pass: " + ", ".join(failed)
    else:
        decision = TrackBStrategyRuleDecision.SHORT
        decision_reason = "Asia Early pause-resume short v1 explicit feature/state snapshot is entry-ready for SHORT."
    return {
        "rule_name": "asia_early_pause_resume_short_v1",
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_asia": state.get("session_asia"),
            "allow_asia": state.get("allow_asia"),
            "close": None if close is None else str(close),
            "open": None if open_price is None else str(open_price),
            "previous_close": None if previous_close is None else str(previous_close),
            "normalized_curvature": None if normalized_curvature is None else str(normalized_curvature),
            "max_normalized_curvature": str(max_normalized_curvature),
            "signal_range_expansion_ratio": None if signal_range_expansion_ratio is None else str(signal_range_expansion_ratio),
            "max_range_expansion_ratio": str(max_range_expansion_ratio),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Mirrors the explicit asiaEarlyPauseResumeShortTurn predicates in "
            "src/mgc_v05l/signals/bear_snap.py and config/replay.asia_early_pause_resume_short_pattern_v1.yaml. "
            "Track B consumes a precomputed state/feature envelope and does not infer these fields from raw candles."
        ),
        "rule_id": rule_id,
    }


def _evaluate_asia_early_normal_breakout_retest_hold_long_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = (
        metadata.get("asia_early_normal_breakout_retest_hold_long_state")
        if isinstance(metadata.get("asia_early_normal_breakout_retest_hold_long_state") or {}, Mapping)
        else {}
    )
    features = (
        metadata.get("asia_early_normal_breakout_retest_hold_long_features")
        if isinstance(metadata.get("asia_early_normal_breakout_retest_hold_long_features") or {}, Mapping)
        else {}
    )

    breakout_normalized_slope = _decimal_field(features, "breakout_normalized_slope")
    breakout_abs_slope_max = _decimal_field(features, "breakout_abs_slope_max") or Decimal("0.20")
    breakout_range_expansion_ratio = _decimal_field(features, "breakout_range_expansion_ratio")
    breakout_min_range_expansion_ratio = _decimal_field(features, "breakout_min_range_expansion_ratio") or Decimal("0.85")
    breakout_max_range_expansion_ratio = _decimal_field(features, "breakout_max_range_expansion_ratio") or Decimal("1.25")

    conditions: dict[str, bool | None] = {
        "allow_asia": _bool_field(state, "allow_asia") is True,
        "asia_early_or_gc_mgc_london_open": _bool_field(state, "asia_early_or_gc_mgc_london_open") is True,
        "no_first_bull_snap_turn": _bool_field(state, "no_first_bull_snap_turn") is True,
        "prior_bars_since_long_setup_gt_anti_churn": _bool_field(state, "prior_bars_since_long_setup_gt_anti_churn") is True,
        "breakout_bar_slope_is_flat": _bool_field(features, "breakout_bar_slope_is_flat") is True,
        "breakout_bar_expansion_is_normal": _bool_field(features, "breakout_bar_expansion_is_normal") is True,
        "breakout_breaks_prior_1_high": _bool_field(features, "breakout_breaks_prior_1_high") is True,
        "signal_retests_and_holds_breakout_level": _bool_field(features, "signal_retests_and_holds_breakout_level") is True,
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    if failed:
        decision = TrackBStrategyRuleDecision.NO_SIGNAL
        decision_reason = "Asia Early normal breakout-retest-hold long v1 conditions did not pass: " + ", ".join(failed)
    else:
        decision = TrackBStrategyRuleDecision.LONG
        decision_reason = "Asia Early normal breakout-retest-hold long v1 explicit feature/state snapshot is entry-ready for LONG."
    return {
        "rule_name": "asia_early_normal_breakout_retest_hold_long_v1",
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_asia": state.get("session_asia"),
            "allow_asia": state.get("allow_asia"),
            "asia_early_or_gc_mgc_london_open": state.get("asia_early_or_gc_mgc_london_open"),
            "no_first_bull_snap_turn": state.get("no_first_bull_snap_turn"),
            "prior_bars_since_long_setup_gt_anti_churn": state.get("prior_bars_since_long_setup_gt_anti_churn"),
            "breakout_normalized_slope": None if breakout_normalized_slope is None else str(breakout_normalized_slope),
            "breakout_abs_slope_max": str(breakout_abs_slope_max),
            "breakout_range_expansion_ratio": (
                None if breakout_range_expansion_ratio is None else str(breakout_range_expansion_ratio)
            ),
            "breakout_min_range_expansion_ratio": str(breakout_min_range_expansion_ratio),
            "breakout_max_range_expansion_ratio": str(breakout_max_range_expansion_ratio),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Mirrors the explicit asiaEarlyNormalBreakoutRetestHoldTurn predicates in "
            "src/mgc_v05l/signals/bull_snap.py and config/replay.asia_early_breakout_retest_hold_pattern_v1_normal.yaml. "
            "Track B consumes a precomputed state/feature envelope and does not infer these fields from raw candles."
        ),
        "rule_id": rule_id,
    }


def _evaluate_first_snap_turn_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
    direction: TrackBStrategyRuleDecision,
    state_key: str,
    features_key: str,
    predicate_prefix: str,
    rule_name: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = metadata.get(state_key) if isinstance(metadata.get(state_key) or {}, Mapping) else {}
    features = metadata.get(features_key) if isinstance(metadata.get(features_key) or {}, Mapping) else {}
    is_bull = predicate_prefix == "bull"
    stretch_key = "bull_snap_downside_stretch_ok" if is_bull else "bear_snap_up_stretch_ok"
    close_key = "bull_snap_close_strong" if is_bull else "bear_snap_close_weak"
    cooldown_key = "prior_bars_since_bull_snap_gt_cooldown" if is_bull else "prior_bars_since_bear_snap_gt_cooldown"
    candidate_key = f"{predicate_prefix}_snap_turn_candidate"
    first_key = f"first_{predicate_prefix}_snap_turn"

    conditions: dict[str, bool | None] = {
        "session_allowed": _bool_field(state, "session_allowed") is True,
        cooldown_key: _bool_field(state, cooldown_key) is True,
        stretch_key: _bool_field(features, stretch_key) is True,
        f"{predicate_prefix}_snap_range_ok": _bool_field(features, f"{predicate_prefix}_snap_range_ok") is True,
        f"{predicate_prefix}_snap_body_ok": _bool_field(features, f"{predicate_prefix}_snap_body_ok") is True,
        close_key: _bool_field(features, close_key) is True,
        f"{predicate_prefix}_snap_velocity_ok": _bool_field(features, f"{predicate_prefix}_snap_velocity_ok") is True,
        f"{predicate_prefix}_snap_reversal_bar": _bool_field(features, f"{predicate_prefix}_snap_reversal_bar") is True,
        f"{predicate_prefix}_snap_location_ok": _bool_field(features, f"{predicate_prefix}_snap_location_ok") is True,
        f"{predicate_prefix}_snap_raw": _bool_field(features, f"{predicate_prefix}_snap_raw") is True,
        candidate_key: _bool_field(features, candidate_key) is True,
        first_key: _bool_field(features, first_key) is True,
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    if failed:
        decision = TrackBStrategyRuleDecision.NO_SIGNAL
        decision_reason = f"{rule_name} conditions did not pass: " + ", ".join(failed)
    else:
        decision = direction
        decision_reason = f"{rule_name} explicit feature/state snapshot is entry-ready for {direction.value}."
    return {
        "rule_name": rule_name,
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_allowed": state.get("session_allowed"),
            cooldown_key: state.get(cooldown_key),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            f"Mirrors the explicit {rule_name} predicates from src/mgc_v05l/signals/"
            f"{'bull_snap.py' if is_bull else 'bear_snap.py'} and the core SignalPacket first snap turn fields. "
            "Track B consumes a precomputed state/feature envelope and does not infer these fields from raw candles."
        ),
        "rule_id": rule_id,
    }


def _evaluate_london_late_pause_resume_short_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = (
        metadata.get("london_late_pause_resume_short_state")
        if isinstance(metadata.get("london_late_pause_resume_short_state") or {}, Mapping)
        else {}
    )
    features = (
        metadata.get("london_late_pause_resume_short_features")
        if isinstance(metadata.get("london_late_pause_resume_short_features") or {}, Mapping)
        else {}
    )
    close = _decimal_field(event, features, "close")
    open_price = _decimal_field(event, features, "open")
    previous_close = _decimal_field(features, "previous_close", "prior_close")
    normalized_slope = _decimal_field(features, "normalized_slope")
    min_slope = _decimal_field(features, "min_normalized_slope") or Decimal("-0.10")
    max_slope = _decimal_field(features, "max_normalized_slope") or Decimal("0.10")
    normalized_curvature = _decimal_field(features, "normalized_curvature")
    min_curvature = _decimal_field(features, "min_normalized_curvature") or Decimal("-0.50")
    max_curvature = _decimal_field(features, "max_normalized_curvature") or Decimal("-0.10")
    signal_range_expansion_ratio = _decimal_field(features, "signal_range_expansion_ratio")
    max_range_expansion_ratio = _decimal_field(features, "max_range_expansion_ratio") or Decimal("1.25")

    conditions: dict[str, bool | None] = {
        "allow_london": _bool_field(state, "allow_london") is True,
        "session_london": _bool_field(state, "session_london") is True,
        "derivative_phase_london_late": _optional_text(state.get("derivative_phase")) == "LONDON_LATE",
        "no_first_bear_snap_turn": _bool_field(state, "no_first_bear_snap_turn") is True,
        "close_below_open": close < open_price if close is not None and open_price is not None else None,
        "close_below_previous_close": close < previous_close if close is not None and previous_close is not None else None,
        "derivative_bear_close_weak": _bool_field(features, "derivative_bear_close_weak") is True,
        "derivative_bear_range_ok": _bool_field(features, "derivative_bear_range_ok") is True,
        "derivative_bear_body_ok": _bool_field(features, "derivative_bear_body_ok") is True,
        "derivative_bear_stretch_ok": _bool_field(features, "derivative_bear_stretch_ok") is True,
        "normalized_slope_in_range": (
            min_slope <= normalized_slope <= max_slope
            if normalized_slope is not None and min_slope is not None and max_slope is not None
            else None
        ),
        "normalized_curvature_in_range": (
            min_curvature <= normalized_curvature <= max_curvature
            if normalized_curvature is not None and min_curvature is not None and max_curvature is not None
            else None
        ),
        "signal_range_expansion_below_threshold": (
            signal_range_expansion_ratio < max_range_expansion_ratio
            if signal_range_expansion_ratio is not None and max_range_expansion_ratio is not None
            else None
        ),
        "slow_ema_ok": _bool_field(features, "slow_ema_ok") is True,
        "one_bar_rebound_before_signal": _bool_field(features, "one_bar_rebound_before_signal") is True,
        "prior_3_any_positive_curvature": _bool_field(features, "prior_3_any_positive_curvature") is True,
        "signal_breaks_prior_1_low": _bool_field(features, "signal_breaks_prior_1_low") is True,
        "derivative_bear_cooldown_ok": _bool_field(features, "derivative_bear_cooldown_ok") is True,
        "no_competing_bear_short_candidate": _bool_field(features, "no_competing_bear_short_candidate") is True,
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    decision = TrackBStrategyRuleDecision.NO_SIGNAL if failed else TrackBStrategyRuleDecision.SHORT
    decision_reason = (
        "London Late pause-resume short v1 conditions did not pass: " + ", ".join(failed)
        if failed
        else "London Late pause-resume short v1 explicit feature/state snapshot is entry-ready for SHORT."
    )
    return {
        "rule_name": "london_late_pause_resume_short_v1",
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_london": state.get("session_london"),
            "allow_london": state.get("allow_london"),
            "close": None if close is None else str(close),
            "open": None if open_price is None else str(open_price),
            "previous_close": None if previous_close is None else str(previous_close),
            "normalized_slope": None if normalized_slope is None else str(normalized_slope),
            "normalized_curvature": None if normalized_curvature is None else str(normalized_curvature),
            "signal_range_expansion_ratio": (
                None if signal_range_expansion_ratio is None else str(signal_range_expansion_ratio)
            ),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Mirrors the explicit londonLatePauseResumeShortTurn predicates in "
            "src/mgc_v05l/signals/bear_snap.py and config/replay.london_late_pause_resume_short_family.yaml. "
            "Track B consumes a precomputed state/feature envelope and does not infer these fields from raw candles."
        ),
        "rule_id": rule_id,
    }


def _evaluate_asia_late_flat_pullback_pause_resume_long_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = (
        metadata.get("asia_late_flat_pullback_pause_resume_long_state")
        if isinstance(metadata.get("asia_late_flat_pullback_pause_resume_long_state") or {}, Mapping)
        else {}
    )
    features = (
        metadata.get("asia_late_flat_pullback_pause_resume_long_features")
        if isinstance(metadata.get("asia_late_flat_pullback_pause_resume_long_features") or {}, Mapping)
        else {}
    )
    close = _decimal_field(event, features, "close")
    open_price = _decimal_field(event, features, "open")
    previous_close = _decimal_field(features, "previous_close", "prior_close")
    pullback_ratio = _decimal_field(features, "pullback_range_expansion_ratio")
    pullback_max = _decimal_field(features, "pullback_max_range_expansion_ratio") or Decimal("0.85")
    signal_ratio = _decimal_field(features, "signal_range_expansion_ratio")
    signal_min = _decimal_field(features, "signal_min_range_expansion_ratio") or Decimal("0.85")
    signal_max = _decimal_field(features, "signal_max_range_expansion_ratio") or Decimal("1.25")
    pullback_curvature = _decimal_field(features, "pullback_normalized_curvature")
    pullback_curvature_threshold = _decimal_field(features, "pullback_curvature_flat_threshold") or Decimal("0.15")

    conditions: dict[str, bool | None] = {
        "allow_asia": _bool_field(state, "allow_asia") is True,
        "session_asia": _bool_field(state, "session_asia") is True,
        "derivative_phase_asia_late": _optional_text(state.get("derivative_phase")) == "ASIA_LATE",
        "no_first_bull_snap_turn": _bool_field(state, "no_first_bull_snap_turn") is True,
        "close_above_open": close > open_price if close is not None and open_price is not None else None,
        "close_above_previous_close": close > previous_close if close is not None and previous_close is not None else None,
        "bull_snap_close_strong": _bool_field(features, "bull_snap_close_strong") is True,
        "one_bar_pullback_before_signal": _bool_field(features, "one_bar_pullback_before_signal") is True,
        "signal_breaks_prior_1_high": _bool_field(features, "signal_breaks_prior_1_high") is True,
        "pullback_range_expansion_below_threshold": (
            pullback_ratio < pullback_max if pullback_ratio is not None and pullback_max is not None else None
        ),
        "signal_range_expansion_above_threshold": (
            signal_ratio > signal_min if signal_ratio is not None and signal_min is not None else None
        ),
        "signal_range_expansion_below_threshold": (
            signal_ratio < signal_max if signal_ratio is not None and signal_max is not None else None
        ),
        "pullback_curvature_flat": (
            abs(pullback_curvature) <= pullback_curvature_threshold
            if pullback_curvature is not None and pullback_curvature_threshold is not None
            else None
        ),
        "prior_bars_since_long_setup_gt_anti_churn": (
            _bool_field(features, "prior_bars_since_long_setup_gt_anti_churn") is True
        ),
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    decision = TrackBStrategyRuleDecision.NO_SIGNAL if failed else TrackBStrategyRuleDecision.LONG
    decision_reason = (
        "Asia Late flat-pullback pause-resume long v1 conditions did not pass: " + ", ".join(failed)
        if failed
        else "Asia Late flat-pullback pause-resume long v1 explicit feature/state snapshot is entry-ready for LONG."
    )
    return {
        "rule_name": "asia_late_flat_pullback_pause_resume_long_v1",
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_asia": state.get("session_asia"),
            "allow_asia": state.get("allow_asia"),
            "close": None if close is None else str(close),
            "open": None if open_price is None else str(open_price),
            "previous_close": None if previous_close is None else str(previous_close),
            "pullback_range_expansion_ratio": None if pullback_ratio is None else str(pullback_ratio),
            "signal_range_expansion_ratio": None if signal_ratio is None else str(signal_ratio),
            "pullback_normalized_curvature": None if pullback_curvature is None else str(pullback_curvature),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Mirrors the explicit asiaLateFlatPullbackPauseResumeLongTurn predicates in "
            "src/mgc_v05l/signals/bull_snap.py and config/replay.asia_late_pause_resume_long_pattern_v1_flat_pullback.yaml. "
            "Track B consumes a precomputed state/feature envelope and does not infer these fields from raw candles."
        ),
        "rule_id": rule_id,
    }


def _evaluate_us_derivative_bear_turn_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
    state_key: str = "us_derivative_bear_turn_state",
    features_key: str = "us_derivative_bear_turn_features",
    rule_name: str = "us_derivative_bear_turn_v1",
    label: str = "US derivative bear turn v1",
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = (
        metadata.get(state_key)
        if isinstance(metadata.get(state_key) or {}, Mapping)
        else {}
    )
    features = (
        metadata.get(features_key)
        if isinstance(metadata.get(features_key) or {}, Mapping)
        else {}
    )
    normalized_slope = _decimal_field(features, "normalized_slope")
    min_slope = _decimal_field(features, "min_normalized_slope") or Decimal("-0.80")
    max_slope = _decimal_field(features, "max_normalized_slope") or Decimal("-0.15")
    normalized_curvature = _decimal_field(features, "normalized_curvature")
    max_curvature = _decimal_field(features, "max_normalized_curvature") or Decimal("-0.35")

    conditions: dict[str, bool | None] = {
        "allow_us": _bool_field(state, "allow_us") is True,
        "session_us": _bool_field(state, "session_us") is True,
        "derivative_bear_window_ok": _bool_field(state, "derivative_bear_window_ok") is True,
        "derivative_bear_phase_ok": _bool_field(state, "derivative_bear_phase_ok") is True,
        "normalized_slope_in_range": (
            min_slope <= normalized_slope <= max_slope
            if normalized_slope is not None and min_slope is not None and max_slope is not None
            else None
        ),
        "normalized_curvature_below_threshold": (
            normalized_curvature <= max_curvature
            if normalized_curvature is not None and max_curvature is not None
            else None
        ),
        "close_below_open": _bool_field(features, "close_below_open") is True,
        "close_below_previous_close": _bool_field(features, "close_below_previous_close") is True,
        "derivative_bear_close_weak": _bool_field(features, "derivative_bear_close_weak") is True,
        "derivative_bear_range_ok": _bool_field(features, "derivative_bear_range_ok") is True,
        "derivative_bear_body_ok": _bool_field(features, "derivative_bear_body_ok") is True,
        "derivative_bear_stretch_ok": _bool_field(features, "derivative_bear_stretch_ok") is True,
        "derivative_bear_fast_ema_ok": _bool_field(features, "derivative_bear_fast_ema_ok") is True,
        "derivative_bear_vwap_ok": _bool_field(features, "derivative_bear_vwap_ok") is True,
        "derivative_bear_vwap_extension_ok": _bool_field(features, "derivative_bear_vwap_extension_ok") is True,
        "derivative_bear_open_late_extension_floor_ok": (
            _bool_field(features, "derivative_bear_open_late_extension_floor_ok") is True
        ),
        "derivative_bear_open_late_body_ok": _bool_field(features, "derivative_bear_open_late_body_ok") is True,
        "derivative_bear_open_late_close_ok": _bool_field(features, "derivative_bear_open_late_close_ok") is True,
        "derivative_bear_open_late_fast_ema_extension_ok": (
            _bool_field(features, "derivative_bear_open_late_fast_ema_extension_ok") is True
        ),
        "derivative_bear_slow_ema_ok": _bool_field(features, "derivative_bear_slow_ema_ok") is True,
        "derivative_bear_structure_ok": _bool_field(features, "derivative_bear_structure_ok") is True,
        "derivative_bear_cooldown_ok": _bool_field(features, "derivative_bear_cooldown_ok") is True,
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    decision = TrackBStrategyRuleDecision.NO_SIGNAL if failed else TrackBStrategyRuleDecision.SHORT
    decision_reason = (
        f"{label} conditions did not pass: " + ", ".join(failed)
        if failed
        else f"{label} explicit feature/state snapshot is entry-ready for SHORT."
    )
    return {
        "rule_name": rule_name,
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_us": state.get("session_us"),
            "allow_us": state.get("allow_us"),
            "normalized_slope": None if normalized_slope is None else str(normalized_slope),
            "normalized_curvature": None if normalized_curvature is None else str(normalized_curvature),
            "vwap": features.get("vwap"),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Mirrors the explicit usDerivativeBearTurn predicates in src/mgc_v05l/signals/bear_snap.py "
            "and the MGC/MNQ usDerivativeBearTurn validation packets. Track B consumes a precomputed state/feature "
            "envelope and does not infer these fields from raw candles inside the adapter."
        ),
        "rule_id": rule_id,
    }


def _evaluate_us_late_pause_resume_long_v1(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    state = (
        metadata.get("us_late_pause_resume_long_state")
        if isinstance(metadata.get("us_late_pause_resume_long_state") or {}, Mapping)
        else {}
    )
    features = (
        metadata.get("us_late_pause_resume_long_features")
        if isinstance(metadata.get("us_late_pause_resume_long_features") or {}, Mapping)
        else {}
    )
    close = _decimal_field(event, features, "close")
    open_price = _decimal_field(event, features, "open")
    previous_close = _decimal_field(features, "previous_close", "prior_close")
    signal_ratio = _decimal_field(features, "signal_range_expansion_ratio")
    max_signal_ratio = _decimal_field(features, "max_range_expansion_ratio") or Decimal("1.25")

    conditions: dict[str, bool | None] = {
        "allow_us": _bool_field(state, "allow_us") is True,
        "session_us_late": _bool_field(state, "session_us_late") is True,
        "derivative_phase_us_late": _optional_text(state.get("derivative_phase")) == "US_LATE",
        "no_first_bull_snap_turn": _bool_field(state, "no_first_bull_snap_turn") is True,
        "close_above_open": close > open_price if close is not None and open_price is not None else None,
        "close_above_previous_close": close > previous_close if close is not None and previous_close is not None else None,
        "bull_snap_close_strong": _bool_field(features, "bull_snap_close_strong") is True,
        "signal_range_expansion_below_threshold": (
            signal_ratio < max_signal_ratio if signal_ratio is not None and max_signal_ratio is not None else None
        ),
        "one_bar_pullback_before_signal": _bool_field(features, "one_bar_pullback_before_signal") is True,
        "signal_breaks_prior_1_high": _bool_field(features, "signal_breaks_prior_1_high") is True,
        "signal_ema_location_ok": _bool_field(features, "signal_ema_location_ok") is True,
        "setup_bar_curvature_is_positive": _bool_field(features, "setup_bar_curvature_is_positive") is True,
        "prior_bars_since_long_setup_gt_anti_churn": (
            _bool_field(features, "prior_bars_since_long_setup_gt_anti_churn") is True
        ),
        "not_1755_carryover": _bool_field(features, "not_1755_carryover") is True,
    }
    failed = [name for name, passed in conditions.items() if passed is not True]
    blockers = [f"{name}=false_or_missing" for name in failed]
    decision = TrackBStrategyRuleDecision.NO_SIGNAL if failed else TrackBStrategyRuleDecision.LONG
    decision_reason = (
        "US Late pause-resume long v1 conditions did not pass: " + ", ".join(failed)
        if failed
        else "US Late pause-resume long v1 explicit feature/state snapshot is entry-ready for LONG."
    )
    return {
        "rule_name": "us_late_pause_resume_long_v1",
        "decision": decision,
        "decision_reason": decision_reason,
        "rule_inputs": {
            "strategy_id": event.get("strategy_id"),
            "derivative_phase": state.get("derivative_phase"),
            "session_us_late": state.get("session_us_late"),
            "allow_us": state.get("allow_us"),
            "close": None if close is None else str(close),
            "open": None if open_price is None else str(open_price),
            "previous_close": None if previous_close is None else str(previous_close),
            "signal_range_expansion_ratio": None if signal_ratio is None else str(signal_ratio),
            "feature_version": features.get("feature_version"),
            "calibration_profile": features.get("calibration_profile"),
            "quote_provider_mode": quote_evidence.get("input_quote_provider_mode"),
        },
        "rule_conditions": conditions,
        "rule_blockers": blockers,
        "research_lineage": (
            "Mirrors the explicit usLatePauseResumeLongTurn predicates in src/mgc_v05l/signals/bull_snap.py "
            "and config/replay.us_late_pause_resume_long_pattern_v1.yaml. Track B consumes a precomputed "
            "state/feature envelope and does not infer these fields from raw candles inside the adapter."
        ),
        "rule_id": rule_id,
    }


def _strategy_event_for_adapter(
    *,
    event: Mapping[str, Any],
    source_id: str,
    strategy_id: str | None,
    lane_id: str | None,
    expected_account_id: str | None,
    rule_id: str,
    rule_mode: TrackBStrategyRuleMode,
    signal_direction: str,
) -> dict[str, Any]:
    metadata = dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {}
    metadata.update(
        {
            "track_b_strategy_rule_runner_boundary": "track_b_strategy_rule_runner",
            "strategy_rule_id": rule_id,
            "strategy_rule_mode": rule_mode.value,
            "strategy_rule_infers_execution_authority": False,
            "strategy_rule_calls_paper_proof_cli": False,
            "strategy_rule_direction_is_explicit_output": True,
        }
    )
    actual_strategy_id = _required_text(strategy_id or event.get("strategy_id") or event.get("signal_family"), "strategy_id")
    actual_lane_id = _required_text(lane_id or event.get("lane_id"), "lane_id")
    account_id = _required_text(event.get("account_id") or event.get("expected_account_id") or expected_account_id, "account_id")
    timestamp = _required_text(event.get("signal_timestamp") or event.get("observed_at") or event.get("candle_timestamp"), "signal_timestamp")
    return {
        "adapter_name": "demo_candle_direction_signal",
        "source_id": source_id,
        "batch_id": _optional_text(event.get("batch_id")) or f"track_b_strategy_rule_batch_{uuid.uuid4().hex}",
        "account_id": account_id,
        "contract_key": _required_text(event.get("local_execution_contract_key") or event.get("contract_key"), "contract_key"),
        "instrument_family": _optional_text(event.get("instrument_family") or event.get("symbol")) or MGC_INSTRUMENT_FAMILY,
        "strategy_id": actual_strategy_id,
        "signal_family": _optional_text(event.get("signal_family")) or actual_strategy_id,
        "lane_id": actual_lane_id,
        "signal_type": "track_b_strategy_rule_signal",
        "signal_direction": signal_direction,
        "decision_style": "BINARY",
        "candle_timestamp": timestamp,
        "observed_at": _optional_text(event.get("observed_at")) or timestamp,
        "timeframe": event.get("timeframe"),
        "open": event.get("open") or event.get("last") or event.get("close"),
        "high": event.get("high") or event.get("last") or event.get("close"),
        "low": event.get("low") or event.get("last") or event.get("close"),
        "close": event.get("close") or event.get("last"),
        "volume": event.get("volume"),
        "reason": f"{rule_id} emitted explicit {signal_direction} no-submit Track B signal from realtime rule evidence.",
        "metadata": metadata,
    }


def _write_report(
    *,
    report_json: Path,
    verdict: TrackBStrategyRuleRunnerVerdict,
    now: datetime,
    runner_id: str,
    source_id: str,
    rule_id: str,
    rule_mode: TrackBStrategyRuleMode,
    registry_entry: TrackBStrategyRegistryEntry | None,
    input_event_path: Path | None,
    input_event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_evaluation: Mapping[str, Any],
    decision: TrackBStrategyRuleDecision,
    decision_reason: str,
    signal_emitted: bool,
    signal_direction: str | None,
    downstream_adapter: StrategySignalAdapterResult | None,
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBStrategyRuleRunnerResult:
    raw_rule_blockers = rule_evaluation.get("rule_blockers")
    rule_blockers = raw_rule_blockers if isinstance(raw_rule_blockers, list) else []
    registry_metadata = registry_entry.report_metadata() if registry_entry is not None else _missing_registry_metadata()
    report = {
        "schema_version": "track_b_strategy_rule_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_strategy_rule_runner_id": runner_id,
        "strategy_rule_runner_verdict": verdict.value,
        "strategy_rule_id": rule_id,
        "strategy_registry_verdict": _strategy_registry_report_verdict(registry_entry, primary_blocker),
        **registry_metadata,
        "rule_name": rule_evaluation.get("rule_name") or "NOT_PROVIDED",
        "rule_mode": rule_mode.value,
        "signal_source": _signal_source(rule_mode),
        "real_strategy_signal": _real_strategy_signal(rule_mode),
        "asian_drift_watch_verdict": _asian_drift_watch_verdict(rule_mode, verdict, signal_emitted, primary_blocker),
        "asia_early_pause_resume_short_watch_verdict": _asia_early_pause_resume_short_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
        ),
        "asia_early_normal_breakout_retest_hold_long_watch_verdict": (
            _asia_early_normal_breakout_retest_hold_long_watch_verdict(
                rule_mode,
                verdict,
                signal_emitted,
                primary_blocker,
            )
        ),
        "first_bull_snap_turn_watch_verdict": _first_snap_turn_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.FIRST_BULL_SNAP_TURN_V1,
            prefix="FIRST_BULL_SNAP_TURN",
        ),
        "first_bear_snap_turn_watch_verdict": _first_snap_turn_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.FIRST_BEAR_SNAP_TURN_V1,
            prefix="FIRST_BEAR_SNAP_TURN",
        ),
        "london_late_pause_resume_short_watch_verdict": _session_strategy_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.LONDON_LATE_PAUSE_RESUME_SHORT_V1,
            prefix="LONDON_LATE_PAUSE_RESUME_SHORT",
        ),
        "asia_late_flat_pullback_pause_resume_long_watch_verdict": _session_strategy_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1,
            prefix="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG",
        ),
        "us_derivative_bear_turn_watch_verdict": _session_strategy_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.US_DERIVATIVE_BEAR_TURN_V1,
            prefix="US_DERIVATIVE_BEAR_TURN",
        ),
        "mnq_us_derivative_bear_turn_watch_verdict": _session_strategy_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.MNQ_US_DERIVATIVE_BEAR_TURN_V1,
            prefix="MNQ_US_DERIVATIVE_BEAR_TURN",
        ),
        "mnq_first_bear_snap_turn_watch_verdict": _first_snap_turn_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.MNQ_FIRST_BEAR_SNAP_TURN_V1,
            prefix="MNQ_FIRST_BEAR_SNAP_TURN",
        ),
        "mnq_first_bull_snap_turn_watch_verdict": _first_snap_turn_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.MNQ_FIRST_BULL_SNAP_TURN_V1,
            prefix="MNQ_FIRST_BULL_SNAP_TURN",
        ),
        "us_late_pause_resume_long_watch_verdict": _session_strategy_watch_verdict(
            rule_mode,
            verdict,
            signal_emitted,
            primary_blocker,
            expected_mode=TrackBStrategyRuleMode.US_LATE_PAUSE_RESUME_LONG_V1,
            prefix="US_LATE_PAUSE_RESUME_LONG",
        ),
        "rule_inputs": rule_evaluation.get("rule_inputs") or {},
        "rule_conditions": rule_evaluation.get("rule_conditions") or {},
        "rule_blockers": rule_blockers,
        "research_lineage": rule_evaluation.get("research_lineage") or "NOT_PROVIDED",
        "source_id": source_id,
        "input_event_path": None if input_event_path is None else str(input_event_path),
        "input_quote_provider_mode": quote_evidence.get("input_quote_provider_mode") or "NOT_PROVIDED",
        "realtime_quote_received": quote_evidence.get("realtime_quote_received") if quote_evidence else False,
        "current_quote_available": quote_evidence.get("current_quote_available") if quote_evidence else False,
        "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict") or "NOT_PROVIDED",
        "quote_timestamp": quote_evidence.get("quote_timestamp") or input_event.get("candle_timestamp") or input_event.get("observed_at"),
        "quote_age_seconds": quote_evidence.get("quote_age_seconds"),
        "quote_report_path": quote_evidence.get("quote_provider_report_json_path") or quote_evidence.get("quote_report_path"),
        "strategy_id": input_event.get("strategy_id"),
        "lane_id": input_event.get("lane_id"),
        "account_id": input_event.get("account_id") or input_event.get("expected_account_id"),
        "local_execution_contract_key": input_event.get("local_execution_contract_key") or input_event.get("contract_key"),
        "decision": decision.value,
        "decision_reason": decision_reason,
        "signal_emitted": signal_emitted,
        "signal_direction": signal_direction,
        "downstream_strategy_adapter_report_path": None if downstream_adapter is None else str(downstream_adapter.report_json),
        "downstream_candle_producer_report_path": None if downstream_adapter is None or downstream_adapter.candle_producer_report_json is None else str(downstream_adapter.candle_producer_report_json),
        "downstream_signal_batch_writer_report_path": None if downstream_adapter is None or downstream_adapter.writer_report_json is None else str(downstream_adapter.writer_report_json),
        "output_batch_path": None if downstream_adapter is None or downstream_adapter.batch_json is None else str(downstream_adapter.batch_json),
        "strategy_adapter_verdict": None if downstream_adapter is None else downstream_adapter.report.get("adapter_verdict"),
        "candle_producer_verdict": None if downstream_adapter is None else downstream_adapter.report.get("candle_signal_producer_verdict"),
        "signal_batch_writer_verdict": _writer_verdict(downstream_adapter),
        "listener_invoked": False,
        "runner_invoked": False,
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "operator_status_invoked": False,
        "paper_proof_cli_called": False,
        "lane_registry_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "market_data_connection_attempted": False,
        "databento_connection_attempted": False,
        "place_order_called": False,
        "cancel_called": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": list(rule_blockers),
        "required_next_action": required_next_action,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_strategy_rule_runner_report.json"),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return TrackBStrategyRuleRunnerResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        downstream_strategy_adapter_report_json=downstream_adapter.report_json if downstream_adapter is not None else None,
        downstream_candle_producer_report_json=downstream_adapter.candle_producer_report_json if downstream_adapter is not None else None,
        downstream_signal_batch_writer_report_json=downstream_adapter.writer_report_json if downstream_adapter is not None else None,
        output_batch_json=downstream_adapter.batch_json if downstream_adapter is not None else None,
    )


def _writer_verdict(adapter: StrategySignalAdapterResult | None) -> str | None:
    if adapter is None or adapter.writer_report_json is None:
        return None
    try:
        writer = json.loads(adapter.writer_report_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return writer.get("signal_batch_writer_verdict")


def _missing_registry_metadata() -> dict[str, Any]:
    return {
        "strategy_registry_id": "NOT_REGISTERED",
        "strategy_registry_rule_id": "NOT_REGISTERED",
        "strategy_registry_rule_mode": "NOT_REGISTERED",
        "strategy_registry_instrument_family": "NOT_REGISTERED",
        "strategy_registry_timeframe": "NOT_REGISTERED",
        "strategy_registry_required_feature_schema": [],
        "strategy_registry_required_state_schema": [],
        "strategy_registry_feature_version": "NOT_REGISTERED",
        "strategy_registry_calibration_profile": "NOT_REGISTERED",
        "strategy_registry_paper_eligible": False,
        "strategy_registry_live_money_eligible": False,
        "strategy_registry_evaluation_mode": "NOT_REGISTERED",
        "strategy_registry_required_1m_context_bars": None,
        "strategy_registry_required_5m_context_bars": None,
    }


def _strategy_registry_report_verdict(
    registry_entry: TrackBStrategyRegistryEntry | None,
    primary_blocker: str | None,
) -> str:
    if registry_entry is None:
        return "TRACK_B_STRATEGY_REGISTRY_NOT_READY"
    blocker = str(primary_blocker or "")
    if "Track B strategy" in blocker and ("NOT_READY" in blocker or "not registered" in blocker):
        return "TRACK_B_STRATEGY_REGISTRY_NOT_READY"
    if "strategy registry" in blocker.lower():
        return "TRACK_B_STRATEGY_REGISTRY_NOT_READY"
    return "TRACK_B_STRATEGY_REGISTRY_READY"


def _signal_source(rule_mode: TrackBStrategyRuleMode) -> str:
    if rule_mode == TrackBStrategyRuleMode.DEMO_LONG_ONLY:
        return "DEMO_WIRING_PROOF"
    if rule_mode == TrackBStrategyRuleMode.HUMAN_REVIEW_ONLY:
        return "HUMAN_REVIEW_ONLY"
    if rule_mode == TrackBStrategyRuleMode.ASIAN_DRIFT_V1:
        return "ASIAN_DRIFT_V1"
    if rule_mode == TrackBStrategyRuleMode.ASIA_EARLY_PAUSE_RESUME_SHORT_V1:
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    if rule_mode == TrackBStrategyRuleMode.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1:
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    if rule_mode == TrackBStrategyRuleMode.FIRST_BULL_SNAP_TURN_V1:
        return "FIRST_BULL_SNAP_TURN_V1"
    if rule_mode == TrackBStrategyRuleMode.FIRST_BEAR_SNAP_TURN_V1:
        return "FIRST_BEAR_SNAP_TURN_V1"
    if rule_mode == TrackBStrategyRuleMode.LONDON_LATE_PAUSE_RESUME_SHORT_V1:
        return "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
    if rule_mode == TrackBStrategyRuleMode.ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1:
        return "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1"
    if rule_mode == TrackBStrategyRuleMode.US_DERIVATIVE_BEAR_TURN_V1:
        return "US_DERIVATIVE_BEAR_TURN_V1"
    if rule_mode == TrackBStrategyRuleMode.MNQ_US_DERIVATIVE_BEAR_TURN_V1:
        return "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    if rule_mode == TrackBStrategyRuleMode.MNQ_FIRST_BEAR_SNAP_TURN_V1:
        return "MNQ_FIRST_BEAR_SNAP_TURN_V1"
    if rule_mode == TrackBStrategyRuleMode.MNQ_FIRST_BULL_SNAP_TURN_V1:
        return "MNQ_FIRST_BULL_SNAP_TURN_V1"
    if rule_mode == TrackBStrategyRuleMode.US_LATE_PAUSE_RESUME_LONG_V1:
        return "US_LATE_PAUSE_RESUME_LONG_V1"
    return "REAL_STRATEGY_RULE"


def _real_strategy_signal(rule_mode: TrackBStrategyRuleMode) -> bool:
    return rule_mode in {
        TrackBStrategyRuleMode.MGC_EMA_MOMENTUM_RECLAIM_LONG,
        TrackBStrategyRuleMode.ASIAN_DRIFT_V1,
        TrackBStrategyRuleMode.ASIA_EARLY_PAUSE_RESUME_SHORT_V1,
        TrackBStrategyRuleMode.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1,
        TrackBStrategyRuleMode.FIRST_BULL_SNAP_TURN_V1,
        TrackBStrategyRuleMode.FIRST_BEAR_SNAP_TURN_V1,
        TrackBStrategyRuleMode.LONDON_LATE_PAUSE_RESUME_SHORT_V1,
        TrackBStrategyRuleMode.ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1,
        TrackBStrategyRuleMode.US_DERIVATIVE_BEAR_TURN_V1,
        TrackBStrategyRuleMode.MNQ_US_DERIVATIVE_BEAR_TURN_V1,
        TrackBStrategyRuleMode.MNQ_FIRST_BEAR_SNAP_TURN_V1,
        TrackBStrategyRuleMode.MNQ_FIRST_BULL_SNAP_TURN_V1,
        TrackBStrategyRuleMode.US_LATE_PAUSE_RESUME_LONG_V1,
    }


def _asian_drift_watch_verdict(
    rule_mode: TrackBStrategyRuleMode,
    verdict: TrackBStrategyRuleRunnerVerdict,
    signal_emitted: bool,
    primary_blocker: str | None,
) -> str | None:
    if rule_mode != TrackBStrategyRuleMode.ASIAN_DRIFT_V1:
        return None
    if primary_blocker or verdict in {
        TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_SCHEMA_ERROR,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_DOWNSTREAM_REJECTED,
    }:
        return "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    if signal_emitted:
        return "ASIAN_DRIFT_SIGNAL_READY_NO_SUBMIT"
    return "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION"


def _asia_early_pause_resume_short_watch_verdict(
    rule_mode: TrackBStrategyRuleMode,
    verdict: TrackBStrategyRuleRunnerVerdict,
    signal_emitted: bool,
    primary_blocker: str | None,
) -> str | None:
    if rule_mode != TrackBStrategyRuleMode.ASIA_EARLY_PAUSE_RESUME_SHORT_V1:
        return None
    if primary_blocker or verdict in {
        TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_SCHEMA_ERROR,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_DOWNSTREAM_REJECTED,
    }:
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_NOT_READY"
    if signal_emitted:
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_SIGNAL_READY_NO_SUBMIT"
    return "ASIA_EARLY_PAUSE_RESUME_SHORT_NO_SIGNAL_NO_MUTATION"


def _asia_early_normal_breakout_retest_hold_long_watch_verdict(
    rule_mode: TrackBStrategyRuleMode,
    verdict: TrackBStrategyRuleRunnerVerdict,
    signal_emitted: bool,
    primary_blocker: str | None,
) -> str | None:
    if rule_mode != TrackBStrategyRuleMode.ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1:
        return None
    if primary_blocker or verdict in {
        TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_SCHEMA_ERROR,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_DOWNSTREAM_REJECTED,
    }:
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NOT_READY"
    if signal_emitted:
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_SIGNAL_READY_NO_SUBMIT"
    return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_NO_SIGNAL_NO_MUTATION"


def _first_snap_turn_watch_verdict(
    rule_mode: TrackBStrategyRuleMode,
    verdict: TrackBStrategyRuleRunnerVerdict,
    signal_emitted: bool,
    primary_blocker: str | None,
    *,
    expected_mode: TrackBStrategyRuleMode,
    prefix: str,
) -> str | None:
    if rule_mode != expected_mode:
        return None
    if primary_blocker or verdict in {
        TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_SCHEMA_ERROR,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_DOWNSTREAM_REJECTED,
    }:
        return f"{prefix}_NOT_READY"
    if signal_emitted:
        return f"{prefix}_SIGNAL_READY_NO_SUBMIT"
    return f"{prefix}_NO_SIGNAL_NO_MUTATION"


def _session_strategy_watch_verdict(
    rule_mode: TrackBStrategyRuleMode,
    verdict: TrackBStrategyRuleRunnerVerdict,
    signal_emitted: bool,
    primary_blocker: str | None,
    *,
    expected_mode: TrackBStrategyRuleMode,
    prefix: str,
) -> str | None:
    if rule_mode != expected_mode:
        return None
    if primary_blocker or verdict in {
        TrackBStrategyRuleRunnerVerdict.BLOCKED_INVALID_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_NON_REALTIME_INPUT,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_SCHEMA_ERROR,
        TrackBStrategyRuleRunnerVerdict.BLOCKED_DOWNSTREAM_REJECTED,
    }:
        return f"{prefix}_NOT_READY"
    if signal_emitted:
        return f"{prefix}_SIGNAL_READY_NO_SUBMIT"
    return f"{prefix}_NO_SIGNAL_NO_MUTATION"


def _rule_mode(value: str | TrackBStrategyRuleMode) -> TrackBStrategyRuleMode:
    if isinstance(value, TrackBStrategyRuleMode):
        return value
    try:
        return TrackBStrategyRuleMode(str(value or "").strip().upper())
    except ValueError as exc:
        allowed = ", ".join(item.value for item in TrackBStrategyRuleMode)
        raise ValueError(f"rule_mode must be one of: {allowed}.") from exc


def _rule_mode_or_default(value: str | TrackBStrategyRuleMode) -> TrackBStrategyRuleMode:
    try:
        return _rule_mode(value)
    except ValueError:
        return TrackBStrategyRuleMode.HUMAN_REVIEW_ONLY


def _read_optional_json(path_value: object) -> dict[str, Any] | None:
    path_text = _optional_text(path_value)
    if not path_text:
        return None
    path = Path(path_text)
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


def _coalesce_bool(first: object, second: object) -> bool | None:
    if isinstance(first, bool):
        return first
    if isinstance(second, bool):
        return second
    return None


def _decimal_field(*sources_and_names: object) -> Decimal | None:
    sources: list[Mapping[str, Any]] = [item for item in sources_and_names if isinstance(item, Mapping)]
    names = [str(item) for item in sources_and_names if not isinstance(item, Mapping)]
    for source in sources:
        for name in names:
            if name not in source or source.get(name) is None:
                continue
            try:
                return Decimal(str(source.get(name)))
            except (InvalidOperation, ValueError):
                return None
    return None


def _bool_field(*sources_and_names: object) -> bool | None:
    sources: list[Mapping[str, Any]] = [item for item in sources_and_names if isinstance(item, Mapping)]
    names = [str(item) for item in sources_and_names if not isinstance(item, Mapping)]
    for source in sources:
        for name in names:
            if name not in source or source.get(name) is None:
                continue
            value = source.get(name)
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y"}:
                return True
            if text in {"false", "0", "no", "n"}:
                return False
            return None
    return None


def _first_text(event: Mapping[str, Any], metadata: Mapping[str, Any], *names: str) -> str | None:
    for name in names:
        value = event.get(name)
        if value is None:
            value = metadata.get(name)
        text = _optional_text(value)
        if text is not None:
            return text
    return None


def _first_bool(event: Mapping[str, Any], metadata: Mapping[str, Any], *names: str) -> bool | None:
    for name in names:
        value = event.get(name)
        if value is None:
            value = metadata.get(name)
        if isinstance(value, bool):
            return value
        text = _optional_text(value)
        if text is None:
            continue
        lowered = text.lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _asian_drift_direction(event: Mapping[str, Any], metadata: Mapping[str, Any], regime: str | None) -> str | None:
    explicit = _first_text(event, metadata, "signal_side", "signal_direction", "direction", "side")
    if explicit is not None:
        normalized = explicit.upper()
        if normalized in {"BUY", "LONG"}:
            return "LONG"
        if normalized in {"SELL", "SHORT"}:
            return "SHORT"
    if regime == "ASIA_DRIFT_LONG":
        return "LONG"
    if regime == "ASIA_DRIFT_SHORT":
        return "SHORT"
    return None


def _required_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
