"""Track B no-submit candle/event to shadow signal batch producer.

This boundary translates explicit candle-style observations into Track B
shadow signal observations, then delegates the inbox write to
signal_batch_writer. It does not infer trade direction from prices, run the
listener, create order plans, or touch broker/data/submit paths.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .shadow_signal import SignalDecisionStyle, SignalDirection
from .signal_batch_writer import (
    DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT,
    SignalBatchWriterVerdict,
    write_signal_batch_to_inbox,
)


DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/candle_signal_producer")


class CandleSignalProducerVerdict(str, Enum):
    PRODUCED_SIGNAL_BATCH = "CANDLE_SIGNAL_PRODUCER_PRODUCED_SIGNAL_BATCH"
    BLOCKED_INVALID_INPUT = "CANDLE_SIGNAL_PRODUCER_BLOCKED_INVALID_INPUT"
    BLOCKED_WRITER_REJECTED_BATCH = "CANDLE_SIGNAL_PRODUCER_BLOCKED_WRITER_REJECTED_BATCH"
    BLOCKED_SCHEMA_ERROR = "CANDLE_SIGNAL_PRODUCER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class CandleSignalProducerResult:
    verdict: CandleSignalProducerVerdict
    report_json: Path
    report: dict[str, Any]
    batch_json: Path | None
    writer_report_json: Path | None


def produce_candle_signal_batch(
    *,
    candle_event_payload: Mapping[str, Any],
    inbox_dir: Path,
    expected_account_id: str | None = None,
    source_id: str | None = None,
    output_root: Path = DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT,
    writer_output_root: Path = DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT,
    producer_id: str | None = None,
    now: datetime | None = None,
) -> CandleSignalProducerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"candle_signal_producer_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_producer_id / "candle_signal_producer_report.json"

    try:
        batch_id = _batch_id(candle_event_payload)
        actual_source_id = source_id or _optional_text(candle_event_payload.get("source_id")) or "candle_signal_producer"
        events = _event_items(candle_event_payload)
        if not events:
            return _write_report(
                report_json=report_json,
                verdict=CandleSignalProducerVerdict.BLOCKED_INVALID_INPUT,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=actual_source_id,
                batch_id=batch_id,
                signal_count=0,
                output_batch_path=None,
                writer_report_path=None,
                primary_blocker="Candle signal producer input contains no candle/events.",
                required_next_action="Provide one or more candle/event observations before producing listener inbox work.",
                writer_verdict=None,
                translated_signals=[],
            )

        translated_signals = [
            _translate_event(
                event=event,
                root_payload=candle_event_payload,
                source_id=actual_source_id,
                expected_account_id=expected_account_id,
                now=actual_now,
                index=index,
            )
            for index, event in enumerate(events, start=1)
        ]
        writer = write_signal_batch_to_inbox(
            inbox_dir=Path(inbox_dir),
            signal_payloads=translated_signals,
            batch_id=batch_id,
            shadow_run_id=_optional_text(candle_event_payload.get("shadow_run_id") or candle_event_payload.get("run_id")),
            source_id=actual_source_id,
            expected_account_id=expected_account_id or _optional_text(candle_event_payload.get("account_id") or candle_event_payload.get("expected_account_id")),
            output_root=writer_output_root,
            writer_id=f"{actual_producer_id}_writer",
            now=actual_now,
        )
        if writer.verdict != SignalBatchWriterVerdict.WROTE_BATCH:
            return _write_report(
                report_json=report_json,
                verdict=CandleSignalProducerVerdict.BLOCKED_WRITER_REJECTED_BATCH,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=actual_source_id,
                batch_id=batch_id,
                signal_count=len(translated_signals),
                output_batch_path=None,
                writer_report_path=writer.report_json,
                primary_blocker=str(writer.report.get("primary_blocker") or "Signal batch writer rejected translated candle signals."),
                required_next_action=str(writer.report.get("required_next_action") or "Review signal batch writer report before retrying."),
                writer_verdict=writer.verdict.value,
                translated_signals=translated_signals,
            )
        return _write_report(
            report_json=report_json,
            verdict=CandleSignalProducerVerdict.PRODUCED_SIGNAL_BATCH,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=actual_source_id,
            batch_id=batch_id,
            signal_count=len(translated_signals),
            output_batch_path=writer.batch_json,
            writer_report_path=writer.report_json,
            primary_blocker=None,
            required_next_action="Let shadow_listener process the produced no-submit signal batch file.",
            writer_verdict=writer.verdict.value,
            translated_signals=translated_signals,
        )
    except (TypeError, ValueError, OSError) as exc:
        return _write_report(
            report_json=report_json,
            verdict=CandleSignalProducerVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id or _optional_text(candle_event_payload.get("source_id")) or "candle_signal_producer",
            batch_id=_optional_text(candle_event_payload.get("batch_id")) or f"candle_signal_batch_{actual_producer_id}",
            signal_count=0,
            output_batch_path=None,
            writer_report_path=None,
            primary_blocker=str(exc),
            required_next_action="Fix candle signal producer input schema before retrying.",
            writer_verdict=None,
            translated_signals=[],
        )


def _translate_event(
    *,
    event: Mapping[str, Any],
    root_payload: Mapping[str, Any],
    source_id: str,
    expected_account_id: str | None,
    now: datetime,
    index: int,
) -> dict[str, Any]:
    account_id = _required_text(
        event.get("account_id") or root_payload.get("account_id") or root_payload.get("expected_account_id") or expected_account_id,
        "account_id",
    )
    contract_key = _required_text(
        event.get("local_execution_contract_key") or event.get("contract_key") or root_payload.get("local_execution_contract_key") or root_payload.get("contract_key"),
        "contract_key",
    )
    strategy_id = _required_text(event.get("strategy_id") or root_payload.get("strategy_id") or root_payload.get("signal_family"), "strategy_id")
    lane_id = _required_text(event.get("lane_id") or root_payload.get("lane_id"), "lane_id")
    signal_timestamp = _required_text(
        event.get("signal_timestamp") or event.get("observed_at") or event.get("candle_timestamp") or root_payload.get("observed_at") or root_payload.get("candle_timestamp"),
        "signal_timestamp",
    )
    raw_direction = event.get("signal_direction") or event.get("side")
    direction = _direction(raw_direction)
    explicit_style = _optional_text(event.get("decision_style") or root_payload.get("decision_style"))
    decision_style = _decision_style(explicit_style=explicit_style, direction=direction)
    signal_type = _optional_text(event.get("signal_type") or root_payload.get("signal_type")) or "candle_event"
    reason = _optional_text(event.get("reason") or root_payload.get("reason"))
    if reason is None:
        reason = "Explicit candle/event input translated into no-submit Track B shadow signal."
    if direction == SignalDirection.NONE.value and decision_style == SignalDecisionStyle.HUMAN_REVIEW.value:
        reason = f"{reason} Direction was missing or NONE, so this is review-only and should not auto-propose an intent."

    metadata = {
        "candle_signal_producer_boundary": "candle_signal_producer",
        "candle_event_index": index,
        "timeframe": event.get("timeframe") or root_payload.get("timeframe"),
        "open": event.get("open"),
        "high": event.get("high"),
        "low": event.get("low"),
        "close": event.get("close"),
        "volume": event.get("volume"),
        "confidence": event.get("confidence"),
        "score": event.get("score"),
        "input_metadata": dict(event.get("metadata") or {}) if isinstance(event.get("metadata") or {}, Mapping) else {},
        "source_batch_id": root_payload.get("batch_id"),
    }
    return {
        "signal_id": _optional_text(event.get("signal_id")) or f"candle_signal_{uuid.uuid4().hex}",
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "signal_type": signal_type,
        "signal_direction": direction,
        "decision_style": decision_style,
        "mode": "PAPER",
        "account_id": account_id,
        "instrument_family": _optional_text(event.get("instrument_family") or root_payload.get("instrument_family") or event.get("symbol") or root_payload.get("symbol")),
        "local_execution_contract_key": contract_key,
        "signal_timestamp": signal_timestamp,
        "observed_at": _optional_text(event.get("observed_at") or root_payload.get("observed_at")) or signal_timestamp,
        "source": source_id,
        "reason": reason,
        "metadata": metadata,
        "submit_requested": False,
        "live_money_readiness": False,
    }


def _event_items(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw_events = payload.get("events")
    if raw_events is None:
        raw_events = payload.get("candle_events")
    if raw_events is None:
        raw_events = payload.get("candles")
    if raw_events is None:
        raw_events = payload.get("signal_events")
    if raw_events is None:
        return [payload]
    if not isinstance(raw_events, Sequence) or isinstance(raw_events, (str, bytes)):
        raise ValueError("events/candle_events/candles must be a list when provided.")
    events: list[Mapping[str, Any]] = []
    for event in raw_events:
        if not isinstance(event, Mapping):
            raise ValueError("Each candle/event item must be a JSON object.")
        events.append(event)
    return events


def _batch_id(payload: Mapping[str, Any]) -> str:
    return _optional_text(payload.get("batch_id")) or f"candle_signal_batch_{uuid.uuid4().hex}"


def _decision_style(*, explicit_style: str | None, direction: str) -> str:
    if explicit_style:
        try:
            style = SignalDecisionStyle(explicit_style.strip().upper()).value
        except ValueError as exc:
            allowed = ", ".join(item.value for item in SignalDecisionStyle)
            raise ValueError(f"decision_style must be one of: {allowed}.") from exc
        if direction == SignalDirection.NONE.value and style == SignalDecisionStyle.BINARY.value:
            return SignalDecisionStyle.HUMAN_REVIEW.value
        return style
    if direction == SignalDirection.NONE.value:
        return SignalDecisionStyle.HUMAN_REVIEW.value
    return SignalDecisionStyle.BINARY.value


def _direction(value: object) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"", "NONE", "NO_SIGNAL", "REVIEW"}:
        return SignalDirection.NONE.value
    if raw in {"BUY", "LONG"}:
        return SignalDirection.LONG.value
    if raw in {"SELL", "SHORT"}:
        return SignalDirection.SHORT.value
    if raw in {"FLAT"}:
        return SignalDirection.FLAT.value
    allowed = "LONG, SHORT, FLAT, NONE, BUY, SELL"
    raise ValueError(f"side/signal_direction must be one of: {allowed}.")


def _required_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _write_report(
    *,
    report_json: Path,
    verdict: CandleSignalProducerVerdict,
    now: datetime,
    producer_id: str,
    source_id: str,
    batch_id: str,
    signal_count: int,
    output_batch_path: Path | None,
    writer_report_path: Path | None,
    primary_blocker: str | None,
    required_next_action: str,
    writer_verdict: str | None,
    translated_signals: Sequence[Mapping[str, Any]],
) -> CandleSignalProducerResult:
    report = {
        "schema_version": "track_b_candle_signal_producer_v1",
        "generated_at": now.isoformat(),
        "candle_signal_producer_id": producer_id,
        "producer_verdict": verdict.value,
        "source_id": source_id,
        "batch_id": batch_id,
        "signal_count": signal_count,
        "output_batch_path": None if output_batch_path is None else str(output_batch_path),
        "writer_report_path": None if writer_report_path is None else str(writer_report_path),
        "signal_batch_writer_verdict": writer_verdict,
        "translated_signal_ids": [signal.get("signal_id") for signal in translated_signals],
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "databento_connection_attempted": False,
        "paper_proof_cli_wired": False,
        "place_order_called": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": [],
        "required_next_action": required_next_action,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_candle_signal_producer_report.json"),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return CandleSignalProducerResult(verdict=verdict, report_json=report_json, report=report, batch_json=output_batch_path, writer_report_json=writer_report_path)
