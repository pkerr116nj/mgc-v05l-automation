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


DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_strategy_rule_runner")
MGC_CONTRACT_KEY = "MGC-202606"
MGC_INSTRUMENT_FAMILY = "MGC"


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
    rule_id: str = "mgc_realtime_quote_demo_long_v1",
    rule_mode: str | TrackBStrategyRuleMode = TrackBStrategyRuleMode.HUMAN_REVIEW_ONLY,
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

    try:
        actual_rule_mode = _rule_mode(rule_mode)
        quote_evidence = _quote_evidence_from_event(input_event_payload)
        validation_blocker = _validate_mgc_realtime_input(
            event=input_event_payload,
            quote_evidence=quote_evidence,
            expected_account_id=expected_account_id,
            allow_fixture_input=allow_fixture_input,
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
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
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
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                decision=TrackBStrategyRuleDecision.HUMAN_REVIEW,
                decision_reason="Rule mode is HUMAN_REVIEW_ONLY; no Track B signal batch was emitted.",
                signal_emitted=False,
                signal_direction=None,
                downstream_adapter=None,
                primary_blocker=None,
                required_next_action="Review the rule report. Use --rule-mode DEMO_LONG_ONLY --emit-signal for the explicit no-submit wiring proof.",
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
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                decision=TrackBStrategyRuleDecision.NO_SIGNAL,
                decision_reason="Rule conditions were reviewable, but --emit-signal was not supplied.",
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
                input_event_path=input_event_path,
                input_event=input_event_payload,
                quote_evidence=quote_evidence,
                decision=TrackBStrategyRuleDecision.LONG,
                decision_reason="DEMO_LONG_ONLY produced explicit LONG, but downstream adapter rejected the event.",
                signal_emitted=False,
                signal_direction="LONG",
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
            input_event_path=input_event_path,
            input_event=input_event_payload,
            quote_evidence=quote_evidence,
            decision=TrackBStrategyRuleDecision.LONG,
            decision_reason="DEMO_LONG_ONLY emitted explicit LONG from valid realtime Databento MGC quote evidence with --emit-signal.",
            signal_emitted=True,
            signal_direction="LONG",
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
            input_event_path=input_event_path,
            input_event=input_event_payload,
            quote_evidence={},
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


def _validate_mgc_realtime_input(
    *,
    event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    expected_account_id: str | None,
    allow_fixture_input: bool,
) -> str | None:
    account_id = _optional_text(event.get("account_id") or event.get("expected_account_id"))
    if expected_account_id and account_id and account_id != expected_account_id:
        return f"Event account_id {account_id} does not match expected_account_id {expected_account_id}."
    contract_key = _optional_text(event.get("local_execution_contract_key") or event.get("contract_key"))
    if contract_key != MGC_CONTRACT_KEY:
        return f"Only {MGC_CONTRACT_KEY} is supported by this first Track B strategy rule runner."
    instrument_family = _optional_text(event.get("instrument_family") or event.get("symbol"))
    if instrument_family and instrument_family != MGC_INSTRUMENT_FAMILY:
        return f"Only instrument_family={MGC_INSTRUMENT_FAMILY} is supported by this first Track B strategy rule runner."
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
    return None


def _strategy_event_for_adapter(
    *,
    event: Mapping[str, Any],
    source_id: str,
    strategy_id: str | None,
    lane_id: str | None,
    expected_account_id: str | None,
    rule_id: str,
    rule_mode: TrackBStrategyRuleMode,
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
        "signal_type": "track_b_demo_realtime_quote_rule",
        "signal_direction": "LONG",
        "decision_style": "BINARY",
        "candle_timestamp": timestamp,
        "observed_at": _optional_text(event.get("observed_at")) or timestamp,
        "timeframe": event.get("timeframe"),
        "open": event.get("open") or event.get("last") or event.get("close"),
        "high": event.get("high") or event.get("last") or event.get("close"),
        "low": event.get("low") or event.get("last") or event.get("close"),
        "close": event.get("close") or event.get("last"),
        "volume": event.get("volume"),
        "reason": f"{rule_id} emitted explicit LONG no-submit Track B signal from realtime MGC quote evidence.",
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
    input_event_path: Path | None,
    input_event: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    decision: TrackBStrategyRuleDecision,
    decision_reason: str,
    signal_emitted: bool,
    signal_direction: str | None,
    downstream_adapter: StrategySignalAdapterResult | None,
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBStrategyRuleRunnerResult:
    report = {
        "schema_version": "track_b_strategy_rule_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_strategy_rule_runner_id": runner_id,
        "strategy_rule_runner_verdict": verdict.value,
        "strategy_rule_id": rule_id,
        "rule_name": "mgc_realtime_quote_momentum_reclaim_demo",
        "rule_mode": rule_mode.value,
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
        "secondary_blockers": [],
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


def _required_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
