"""Track B no-submit strategy-event to signal producer adapter.

This module is intentionally narrow: the demo candle direction adapter accepts
explicit strategy-like event fields and delegates no-submit inbox production to
the candle signal producer. It does not implement strategy rules, infer market
direction, run the listener, authorize lanes, or touch broker/data/submit paths.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .candle_signal_producer import (
    DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT,
    CandleSignalProducerVerdict,
    produce_candle_signal_batch,
)
from .models import require_aware_datetime, to_jsonable
from .signal_batch_writer import DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT


DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/strategy_signal_adapter")


class StrategySignalAdapterVerdict(str, Enum):
    EMITTED_SIGNAL_BATCH = "STRATEGY_SIGNAL_ADAPTER_EMITTED_SIGNAL_BATCH"
    BLOCKED_INVALID_EVENT = "STRATEGY_SIGNAL_ADAPTER_BLOCKED_INVALID_EVENT"
    BLOCKED_PRODUCER_REJECTED_EVENT = "STRATEGY_SIGNAL_ADAPTER_BLOCKED_PRODUCER_REJECTED_EVENT"
    BLOCKED_SCHEMA_ERROR = "STRATEGY_SIGNAL_ADAPTER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class StrategySignalAdapterResult:
    verdict: StrategySignalAdapterVerdict
    report_json: Path
    report: dict[str, Any]
    batch_json: Path | None
    candle_producer_report_json: Path | None
    writer_report_json: Path | None


def adapt_demo_candle_direction_signal(
    *,
    strategy_event_payload: Mapping[str, Any],
    inbox_dir: Path,
    expected_account_id: str | None = None,
    source_id: str | None = None,
    output_root: Path = DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT,
    candle_producer_output_root: Path = DEFAULT_CANDLE_SIGNAL_PRODUCER_OUTPUT_ROOT,
    writer_output_root: Path = DEFAULT_SIGNAL_BATCH_WRITER_OUTPUT_ROOT,
    adapter_id: str | None = None,
    now: datetime | None = None,
) -> StrategySignalAdapterResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_adapter_id = adapter_id or f"strategy_signal_adapter_{uuid.uuid4().hex}"
    actual_source_id = source_id or _optional_text(strategy_event_payload.get("source_id")) or "demo_candle_direction_signal"
    report_json = Path(output_root) / actual_adapter_id / "strategy_signal_adapter_report.json"

    try:
        candle_payload = _normalize_demo_candle_event(
            payload=strategy_event_payload,
            source_id=actual_source_id,
            expected_account_id=expected_account_id,
        )
        producer = produce_candle_signal_batch(
            candle_event_payload=candle_payload,
            inbox_dir=Path(inbox_dir),
            expected_account_id=expected_account_id,
            source_id=actual_source_id,
            output_root=candle_producer_output_root,
            writer_output_root=writer_output_root,
            producer_id=f"{actual_adapter_id}_candle_producer",
            now=actual_now,
        )
        if producer.verdict != CandleSignalProducerVerdict.PRODUCED_SIGNAL_BATCH:
            return _write_report(
                report_json=report_json,
                verdict=StrategySignalAdapterVerdict.BLOCKED_PRODUCER_REJECTED_EVENT,
                now=actual_now,
                adapter_id=actual_adapter_id,
                source_id=actual_source_id,
                strategy_event=strategy_event_payload,
                normalized_payload=candle_payload,
                signal_count=0,
                output_batch_path=None,
                candle_producer_report_path=producer.report_json,
                writer_report_path=producer.writer_report_json,
                primary_blocker=str(producer.report.get("primary_blocker") or "Candle signal producer rejected strategy adapter event."),
                required_next_action=str(producer.report.get("required_next_action") or "Review candle signal producer report before retrying."),
                candle_producer_verdict=producer.verdict.value,
            )
        return _write_report(
            report_json=report_json,
            verdict=StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH,
            now=actual_now,
            adapter_id=actual_adapter_id,
            source_id=actual_source_id,
            strategy_event=strategy_event_payload,
            normalized_payload=candle_payload,
            signal_count=int(producer.report.get("signal_count") or 0),
            output_batch_path=producer.batch_json,
            candle_producer_report_path=producer.report_json,
            writer_report_path=producer.writer_report_json,
            primary_blocker=None,
            required_next_action="Let shadow_listener process the adapter-produced no-submit signal batch file.",
            candle_producer_verdict=producer.verdict.value,
        )
    except (TypeError, ValueError, OSError) as exc:
        return _write_report(
            report_json=report_json,
            verdict=StrategySignalAdapterVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            adapter_id=actual_adapter_id,
            source_id=actual_source_id,
            strategy_event=strategy_event_payload,
            normalized_payload={},
            signal_count=0,
            output_batch_path=None,
            candle_producer_report_path=None,
            writer_report_path=None,
            primary_blocker=str(exc),
            required_next_action="Fix strategy signal adapter event schema before retrying.",
            candle_producer_verdict=None,
        )


def _normalize_demo_candle_event(
    *,
    payload: Mapping[str, Any],
    source_id: str,
    expected_account_id: str | None,
) -> dict[str, Any]:
    if str(payload.get("adapter_name") or "demo_candle_direction_signal").strip() not in {"", "demo_candle_direction_signal"}:
        raise ValueError("Only adapter_name=demo_candle_direction_signal is supported in this scaffold.")
    strategy_id = _required_text(payload.get("strategy_id") or payload.get("signal_family"), "strategy_id")
    lane_id = _required_text(payload.get("lane_id"), "lane_id")
    account_id = _required_text(payload.get("account_id") or payload.get("expected_account_id") or expected_account_id, "account_id")
    contract_key = _required_text(payload.get("local_execution_contract_key") or payload.get("contract_key"), "contract_key")
    timestamp = _required_text(payload.get("signal_timestamp") or payload.get("observed_at") or payload.get("candle_timestamp"), "signal_timestamp")
    side = _optional_text(payload.get("signal_direction") or payload.get("side"))
    decision_style = _optional_text(payload.get("decision_style"))
    if side is None:
        decision_style = "HUMAN_REVIEW"

    metadata = dict(payload.get("metadata") or {}) if isinstance(payload.get("metadata") or {}, Mapping) else {}
    metadata.update(
        {
            "strategy_signal_adapter_boundary": "demo_candle_direction_signal",
            "adapter_infers_direction": False,
            "adapter_authorizes_lane": False,
            "adapter_creates_order_plan": False,
        }
    )
    reason = _optional_text(payload.get("reason")) or "Demo candle direction adapter emitted explicit no-submit Track B signal input."
    if side is None:
        reason = f"{reason} Direction was missing, so this adapter emits review-only HUMAN_REVIEW input."
    return {
        "source_id": source_id,
        "batch_id": _optional_text(payload.get("batch_id")) or f"strategy_adapter_batch_{uuid.uuid4().hex}",
        "shadow_run_id": _optional_text(payload.get("shadow_run_id") or payload.get("run_id")),
        "account_id": account_id,
        "contract_key": contract_key,
        "instrument_family": _optional_text(payload.get("instrument_family") or payload.get("symbol")),
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "signal_type": _optional_text(payload.get("signal_type")) or "demo_candle_direction_signal",
        "signal_direction": side or "NONE",
        "decision_style": decision_style or "BINARY",
        "candle_timestamp": timestamp,
        "observed_at": _optional_text(payload.get("observed_at")) or timestamp,
        "timeframe": payload.get("timeframe"),
        "open": payload.get("open"),
        "high": payload.get("high"),
        "low": payload.get("low"),
        "close": payload.get("close"),
        "volume": payload.get("volume"),
        "confidence": payload.get("confidence"),
        "score": payload.get("score"),
        "reason": reason,
        "metadata": metadata,
    }


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
    verdict: StrategySignalAdapterVerdict,
    now: datetime,
    adapter_id: str,
    source_id: str,
    strategy_event: Mapping[str, Any],
    normalized_payload: Mapping[str, Any],
    signal_count: int,
    output_batch_path: Path | None,
    candle_producer_report_path: Path | None,
    writer_report_path: Path | None,
    primary_blocker: str | None,
    required_next_action: str,
    candle_producer_verdict: str | None,
) -> StrategySignalAdapterResult:
    report = {
        "schema_version": "track_b_strategy_signal_adapter_v1",
        "generated_at": now.isoformat(),
        "strategy_signal_adapter_id": adapter_id,
        "adapter_name": "demo_candle_direction_signal",
        "adapter_verdict": verdict.value,
        "strategy_id": strategy_event.get("strategy_id") or strategy_event.get("signal_family"),
        "signal_family": strategy_event.get("signal_family"),
        "lane_id": strategy_event.get("lane_id"),
        "source_id": source_id,
        "batch_id": normalized_payload.get("batch_id") or strategy_event.get("batch_id"),
        "signal_count": signal_count,
        "output_batch_path": None if output_batch_path is None else str(output_batch_path),
        "candle_producer_report_path": None if candle_producer_report_path is None else str(candle_producer_report_path),
        "downstream_writer_report_path": None if writer_report_path is None else str(writer_report_path),
        "candle_signal_producer_verdict": candle_producer_verdict,
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "lane_registry_invoked": False,
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
        "latest_report_json_path": str(report_json.parent.parent / "latest_strategy_signal_adapter_report.json"),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return StrategySignalAdapterResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        batch_json=output_batch_path,
        candle_producer_report_json=candle_producer_report_path,
        writer_report_json=writer_report_path,
    )
