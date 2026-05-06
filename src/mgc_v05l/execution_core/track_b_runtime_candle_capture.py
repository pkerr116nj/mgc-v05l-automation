"""Track B bounded runtime candle capture for MGC.

This boundary maintains a small execution-time candle context artifact for the
MGC strategy path. It is intentionally bounded and artifact-only: no broker
access, no paper proof, no Databento stream orchestration, and no submit path.
The first supported input mode is a supplied runtime candle JSON payload.
"""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/track_b_runtime_candle_capture"
)
MGC_CONTRACT_KEY = "MGC-202606"
MGC_LOCAL_SYMBOL = "MGCM6"
MGC_CONTINUOUS_SYMBOL = "MGC.v.0"
MGC_DATASET = "GLBX.MDP3"


class TrackBRuntimeCandleCaptureVerdict(str, Enum):
    WROTE_RUNTIME_CANDLES = "TRACK_B_RUNTIME_CANDLE_CAPTURE_WROTE_RUNTIME_CANDLES"
    DATA_WRITTEN_EXECUTION_FRESH = "TRACK_B_RUNTIME_CANDLE_CAPTURE_DATA_WRITTEN_EXECUTION_FRESH"
    DATA_WRITTEN_NOT_EXECUTION_FRESH = "TRACK_B_RUNTIME_CANDLE_CAPTURE_DATA_WRITTEN_NOT_EXECUTION_FRESH"
    FETCH_FAILED = "TRACK_B_RUNTIME_CANDLE_CAPTURE_FETCH_FAILED"
    PROVIDER_ERROR = "TRACK_B_RUNTIME_CANDLE_CAPTURE_PROVIDER_ERROR"
    BLOCKED_INSUFFICIENT_RUNTIME_CANDLES = "TRACK_B_RUNTIME_CANDLE_CAPTURE_BLOCKED_INSUFFICIENT_RUNTIME_CANDLES"
    BLOCKED_INVALID_INPUT = "TRACK_B_RUNTIME_CANDLE_CAPTURE_BLOCKED_INVALID_INPUT"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_RUNTIME_CANDLE_CAPTURE_BLOCKED_SCHEMA_ERROR"
    BLOCKED_STALE_RUNTIME_CANDLES = "TRACK_B_RUNTIME_CANDLE_CAPTURE_BLOCKED_STALE_RUNTIME_CANDLES"
    BLOCKED_PROVIDER_ERROR = "TRACK_B_RUNTIME_CANDLE_CAPTURE_BLOCKED_PROVIDER_ERROR"


@dataclass(frozen=True)
class TrackBRuntimeCandleCaptureResult:
    verdict: TrackBRuntimeCandleCaptureVerdict
    report_json: Path
    report: dict[str, Any]
    runtime_candles_json: Path | None
    runtime_candles_event: dict[str, Any] | None


_DATA_WRITTEN_VERDICTS = frozenset(
    {
        TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES,
        TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_EXECUTION_FRESH,
        TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_NOT_EXECUTION_FRESH,
    }
)


def _artifact_symbol_from_contract(contract_key: str | None, local_symbol: str | None) -> str:
    source = str(contract_key or local_symbol or "MGC").strip().upper()
    if source.startswith("MNQ"):
        return "mnq"
    if source.startswith("MGC"):
        return "mgc"
    return source.split("-", 1)[0].lower() or "mgc"


def capture_track_b_runtime_mgc_1m_candles(
    *,
    runtime_candle_payload: Mapping[str, Any],
    source_payload_path: Path | None = None,
    expected_account_id: str | None = "DUM882026",
    account_id: str = "DUM882026",
    contract_key: str = MGC_CONTRACT_KEY,
    local_symbol: str = MGC_LOCAL_SYMBOL,
    databento_continuous_symbol: str = MGC_CONTINUOUS_SYMBOL,
    dataset: str = MGC_DATASET,
    timeframe: str = "1m",
    max_bars: int = 250,
    min_bars: int = 3,
    candle_source_mode: str = "SUPPLIED_RUNTIME_CANDLES",
    requested_window_start: datetime | None = None,
    requested_window_end: datetime | None = None,
    provider_available_end: datetime | None = None,
    history_end_used: datetime | None = None,
    available_end_lag_seconds: int | None = None,
    provider_transport: str | None = None,
    provider_request_symbol: str | None = None,
    provider_request_stype_in: str | None = None,
    provider_request_stype_out: str | None = None,
    max_latest_1m_age_seconds: int | None = None,
    max_completed_5m_age_seconds: int | None = None,
    provider_credential_status: str | None = None,
    provider_credential_source: str | None = None,
    source_id: str | None = None,
    strategy_id: str = "track_b_example_gold_shadow_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    output_root: Path = DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT,
    capture_id: str | None = None,
    retention_runs: int = 5,
    now: datetime | None = None,
) -> TrackBRuntimeCandleCaptureResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_capture_id = capture_id or f"track_b_runtime_candle_capture_{uuid.uuid4().hex}"
    actual_source_id = source_id or _optional_text(runtime_candle_payload.get("source_id")) or "track_b_runtime_candle_capture"
    report_json = Path(output_root) / actual_capture_id / "track_b_runtime_candle_capture_report.json"
    artifact_symbol = _artifact_symbol_from_contract(contract_key, local_symbol)
    event_json = Path(output_root) / actual_capture_id / f"runtime_{artifact_symbol}_1m_candles.json"

    try:
        if max_bars <= 0:
            raise ValueError("max_bars must be positive.")
        if min_bars <= 0:
            raise ValueError("min_bars must be positive.")
        raw_candles = _raw_candles(runtime_candle_payload)
        normalized = [_normalize_candle(item, index=index) for index, item in enumerate(raw_candles, start=1)]
        deduped, duplicate_count = _dedupe_and_sort(normalized)
        bounded = deduped[-max_bars:]
        gap_count = _gap_count(bounded, timeframe=timeframe)
        quote_evidence = _quote_evidence(runtime_candle_payload)
        validation_blocker = _validation_blocker(
            payload=runtime_candle_payload,
            candles=bounded,
            expected_account_id=expected_account_id,
            expected_contract_key=contract_key,
        )
        if validation_blocker:
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=TrackBRuntimeCandleCaptureVerdict.BLOCKED_INVALID_INPUT,
                now=actual_now,
                capture_id=actual_capture_id,
                source_id=actual_source_id,
                source_payload_path=source_payload_path,
                account_id=account_id,
                contract_key=contract_key,
                local_symbol=local_symbol,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                candle_source_mode=candle_source_mode,
                max_bars=max_bars,
                min_bars=min_bars,
                candles=bounded,
                duplicate_count=duplicate_count,
                gap_count=gap_count,
                quote_evidence=quote_evidence,
                runtime_event=None,
                primary_blocker=validation_blocker,
                required_next_action="Provide valid bounded MGC runtime candle input before feature building.",
                retention_runs=retention_runs,
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
                provider_available_end=provider_available_end,
                history_end_used=history_end_used,
                available_end_lag_seconds=available_end_lag_seconds,
                provider_transport=provider_transport,
                provider_request_symbol=provider_request_symbol,
                provider_request_stype_in=provider_request_stype_in,
                provider_request_stype_out=provider_request_stype_out,
                max_latest_1m_age_seconds=max_latest_1m_age_seconds,
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
                provider_credential_status=provider_credential_status,
                provider_credential_source=provider_credential_source,
            )
        if len(bounded) < min_bars:
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=TrackBRuntimeCandleCaptureVerdict.BLOCKED_INSUFFICIENT_RUNTIME_CANDLES,
                now=actual_now,
                capture_id=actual_capture_id,
                source_id=actual_source_id,
                source_payload_path=source_payload_path,
                account_id=account_id,
                contract_key=contract_key,
                local_symbol=local_symbol,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                candle_source_mode=candle_source_mode,
                max_bars=max_bars,
                min_bars=min_bars,
                candles=bounded,
                duplicate_count=duplicate_count,
                gap_count=gap_count,
                quote_evidence=quote_evidence,
                runtime_event=None,
                primary_blocker=f"At least {min_bars} runtime candles are required; received {len(bounded)}.",
                required_next_action="Collect more bounded MGC runtime candles before feature building.",
                retention_runs=retention_runs,
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
                provider_available_end=provider_available_end,
                history_end_used=history_end_used,
                available_end_lag_seconds=available_end_lag_seconds,
                provider_transport=provider_transport,
                provider_request_symbol=provider_request_symbol,
                provider_request_stype_in=provider_request_stype_in,
                provider_request_stype_out=provider_request_stype_out,
                max_latest_1m_age_seconds=max_latest_1m_age_seconds,
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
                provider_credential_status=provider_credential_status,
                provider_credential_source=provider_credential_source,
            )
        if gap_count > 0:
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=TrackBRuntimeCandleCaptureVerdict.BLOCKED_INVALID_INPUT,
                now=actual_now,
                capture_id=actual_capture_id,
                source_id=actual_source_id,
                source_payload_path=source_payload_path,
                account_id=account_id,
                contract_key=contract_key,
                local_symbol=local_symbol,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                candle_source_mode=candle_source_mode,
                max_bars=max_bars,
                min_bars=min_bars,
                candles=bounded,
                duplicate_count=duplicate_count,
                gap_count=gap_count,
                quote_evidence=quote_evidence,
                runtime_event=None,
                primary_blocker=f"Runtime MGC 1m candle context has {gap_count} detected gaps.",
                required_next_action="Repair or continue runtime candle capture until the bounded window is contiguous.",
                retention_runs=retention_runs,
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
                provider_available_end=provider_available_end,
                history_end_used=history_end_used,
                available_end_lag_seconds=available_end_lag_seconds,
                provider_transport=provider_transport,
                provider_request_symbol=provider_request_symbol,
                provider_request_stype_in=provider_request_stype_in,
                provider_request_stype_out=provider_request_stype_out,
                max_latest_1m_age_seconds=max_latest_1m_age_seconds,
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
                provider_credential_status=provider_credential_status,
                provider_credential_source=provider_credential_source,
            )
        freshness = _runtime_freshness(
            candles=bounded,
            now=actual_now,
            max_latest_1m_age_seconds=max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            provider_available_end=provider_available_end,
        )
        fresh_for_execution = freshness["runtime_candle_context_stale"] is not True
        execution_freshness_blocker = None if fresh_for_execution else _stale_blocker(freshness)
        runtime_event = _runtime_event(
            payload=runtime_candle_payload,
            candles=bounded,
            quote_evidence=quote_evidence,
            capture_id=actual_capture_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            account_id=account_id,
            contract_key=contract_key,
            local_symbol=local_symbol,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            candle_source_mode=candle_source_mode,
            max_bars=max_bars,
            strategy_id=strategy_id,
            lane_id=lane_id,
            now=actual_now,
            fresh_for_execution=fresh_for_execution,
            execution_freshness_blocker=execution_freshness_blocker,
        )
        verdict = (
            TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_EXECUTION_FRESH
            if fresh_for_execution
            else TrackBRuntimeCandleCaptureVerdict.DATA_WRITTEN_NOT_EXECUTION_FRESH
        )
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=verdict,
            now=actual_now,
            capture_id=actual_capture_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            account_id=account_id,
            contract_key=contract_key,
            local_symbol=local_symbol,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            candle_source_mode=candle_source_mode,
            max_bars=max_bars,
            min_bars=min_bars,
            candles=bounded,
            duplicate_count=duplicate_count,
            gap_count=gap_count,
            quote_evidence=quote_evidence,
            runtime_event=runtime_event,
            primary_blocker=execution_freshness_blocker,
            required_next_action=(
                "Runtime candle context is fresh for live Track B strategy evaluation."
                if fresh_for_execution
                else "Bounded candle data was written for backfill/context only; do not run live strategy evaluation until fresh_for_execution=true or an explicit research/shadow replay override is supplied."
            ),
            retention_runs=retention_runs,
            requested_window_start=requested_window_start,
            requested_window_end=requested_window_end,
            provider_available_end=provider_available_end,
            history_end_used=history_end_used,
            available_end_lag_seconds=available_end_lag_seconds,
            provider_transport=provider_transport,
            provider_request_symbol=provider_request_symbol,
            provider_request_stype_in=provider_request_stype_in,
            provider_request_stype_out=provider_request_stype_out,
            max_latest_1m_age_seconds=max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            provider_credential_status=provider_credential_status,
            provider_credential_source=provider_credential_source,
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=TrackBRuntimeCandleCaptureVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            capture_id=actual_capture_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            account_id=account_id,
            contract_key=contract_key,
            local_symbol=local_symbol,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            candle_source_mode=candle_source_mode,
            max_bars=max_bars,
            min_bars=min_bars,
            candles=[],
            duplicate_count=0,
            gap_count=0,
            quote_evidence={},
            runtime_event=None,
            primary_blocker=str(exc),
            required_next_action="Fix Track B runtime candle capture input schema before retrying.",
            retention_runs=retention_runs,
            requested_window_start=requested_window_start,
            requested_window_end=requested_window_end,
            provider_available_end=provider_available_end,
            history_end_used=history_end_used,
            available_end_lag_seconds=available_end_lag_seconds,
            provider_transport=provider_transport,
            provider_request_symbol=provider_request_symbol,
            provider_request_stype_in=provider_request_stype_in,
            provider_request_stype_out=provider_request_stype_out,
            max_latest_1m_age_seconds=max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            provider_credential_status=provider_credential_status,
            provider_credential_source=provider_credential_source,
        )


def write_runtime_candle_capture_provider_error(
    *,
    primary_blocker: str,
    required_next_action: str,
    verdict: TrackBRuntimeCandleCaptureVerdict = TrackBRuntimeCandleCaptureVerdict.PROVIDER_ERROR,
    source_id: str = "track_b_runtime_candle_capture",
    account_id: str = "DUM882026",
    contract_key: str = MGC_CONTRACT_KEY,
    local_symbol: str = MGC_LOCAL_SYMBOL,
    databento_continuous_symbol: str = MGC_CONTINUOUS_SYMBOL,
    dataset: str = MGC_DATASET,
    timeframe: str = "1m",
    candle_source_mode: str = "DATABENTO_HISTORICAL_RECENT",
    requested_window_start: datetime | None = None,
    requested_window_end: datetime | None = None,
    provider_available_end: datetime | None = None,
    history_end_used: datetime | None = None,
    available_end_lag_seconds: int | None = None,
    provider_transport: str | None = None,
    provider_request_symbol: str | None = None,
    provider_request_stype_in: str | None = None,
    provider_request_stype_out: str | None = None,
    max_bars: int = 250,
    min_bars: int = 3,
    max_latest_1m_age_seconds: int | None = None,
    max_completed_5m_age_seconds: int | None = None,
    provider_credential_status: str | None = None,
    provider_credential_source: str | None = None,
    output_root: Path = DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT,
    capture_id: str | None = None,
    now: datetime | None = None,
) -> TrackBRuntimeCandleCaptureResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_capture_id = capture_id or f"track_b_runtime_candle_capture_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_capture_id / "track_b_runtime_candle_capture_report.json"
    event_json = Path(output_root) / actual_capture_id / "runtime_mgc_1m_candles.json"
    return _write_report(
        report_json=report_json,
        event_json=event_json,
        verdict=verdict,
        now=actual_now,
        capture_id=actual_capture_id,
        source_id=source_id,
        source_payload_path=None,
        account_id=account_id,
        contract_key=contract_key,
        local_symbol=local_symbol,
        databento_continuous_symbol=databento_continuous_symbol,
        dataset=dataset,
        timeframe=timeframe,
        candle_source_mode=candle_source_mode,
        max_bars=max_bars,
        min_bars=min_bars,
        candles=[],
        duplicate_count=0,
        gap_count=0,
        quote_evidence={},
        runtime_event=None,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        retention_runs=0,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        provider_available_end=provider_available_end,
        history_end_used=history_end_used,
        available_end_lag_seconds=available_end_lag_seconds,
        provider_transport=provider_transport,
        provider_request_symbol=provider_request_symbol,
        provider_request_stype_in=provider_request_stype_in,
        provider_request_stype_out=provider_request_stype_out,
        max_latest_1m_age_seconds=max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        provider_credential_status=provider_credential_status,
        provider_credential_source=provider_credential_source,
    )


def _raw_candles(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    raw = payload.get("candles") or payload.get("candle_history") or payload.get("runtime_candles") or payload.get("events")
    if raw is None:
        raw = [payload] if payload.get("close") is not None or payload.get("last") is not None else []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("candles/runtime_candles must be a list of objects.")
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"candles[{index}] must be an object.")
    return raw


def _normalize_candle(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    close = _decimal(item.get("close") if item.get("close") is not None else item.get("last"), f"candles[{index}].close")
    open_price = _decimal(item.get("open") if item.get("open") is not None else close, f"candles[{index}].open")
    high = _decimal(item.get("high") if item.get("high") is not None else close, f"candles[{index}].high")
    low = _decimal(item.get("low") if item.get("low") is not None else close, f"candles[{index}].low")
    header = item.get("hd") if isinstance(item.get("hd"), Mapping) else {}
    timestamp = _required_text(
        item.get("candle_timestamp") or item.get("timestamp") or item.get("observed_at") or item.get("ts_event") or header.get("ts_event"),
        f"candles[{index}].timestamp",
    )
    return {
        "candle_timestamp": _parse_timestamp(timestamp).isoformat(),
        "observed_at": _optional_text(item.get("observed_at")) or _parse_timestamp(timestamp).isoformat(),
        "open": _decimal_text(open_price),
        "high": _decimal_text(high),
        "low": _decimal_text(low),
        "close": _decimal_text(close),
        "volume": None if item.get("volume") in {None, ""} else _decimal_text(_decimal(item.get("volume"), f"candles[{index}].volume")),
        "raw_symbol": item.get("raw_symbol"),
        "provider_symbol": item.get("provider_symbol") or item.get("symbol"),
        "source_tag": item.get("source_tag"),
        "source_role": item.get("source_role"),
    }


def _dedupe_and_sort(candles: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    by_timestamp: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for candle in candles:
        timestamp = str(candle["candle_timestamp"])
        if timestamp in by_timestamp:
            duplicate_count += 1
        by_timestamp[timestamp] = dict(candle)
    return [by_timestamp[key] for key in sorted(by_timestamp, key=lambda value: _parse_timestamp(value))], duplicate_count


def _gap_count(candles: Sequence[Mapping[str, Any]], *, timeframe: str) -> int:
    if len(candles) < 2 or timeframe != "1m":
        return 0
    gaps = 0
    previous = _parse_timestamp(str(candles[0]["candle_timestamp"]))
    for candle in candles[1:]:
        current = _parse_timestamp(str(candle["candle_timestamp"]))
        if current - previous > timedelta(minutes=1):
            gaps += 1
        previous = current
    return gaps


def _quote_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    return {
        "quote_provider_mode": _first_text(metadata.get("quote_provider_mode"), payload.get("quote_provider_mode")),
        "realtime_quote_received": _first_bool(metadata.get("realtime_quote_received"), payload.get("realtime_quote_received")),
        "current_quote_available": _first_bool(metadata.get("current_quote_available"), payload.get("current_quote_available")),
        "quote_freshness_verdict": _first_text(metadata.get("quote_freshness_verdict"), payload.get("quote_freshness_verdict")),
        "quote_report_path": _first_text(metadata.get("source_report_path"), payload.get("quote_report_path"), payload.get("source_report_path")),
    }


def _validation_blocker(
    *,
    payload: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    expected_account_id: str | None,
    expected_contract_key: str,
) -> str | None:
    if not candles:
        return "Runtime candle input contains no candles."
    account_id = _optional_text(payload.get("account_id") or payload.get("expected_account_id"))
    if expected_account_id and account_id and account_id != expected_account_id:
        return f"Runtime candle account_id {account_id} does not match expected_account_id {expected_account_id}."
    contract_key = _optional_text(payload.get("contract_key") or payload.get("local_execution_contract_key"))
    if contract_key != expected_contract_key:
        return f"Only {expected_contract_key} is supported by this Track B runtime candle capture."
    return None


def _runtime_event(
    *,
    payload: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    quote_evidence: Mapping[str, Any],
    capture_id: str,
    source_id: str,
    source_payload_path: Path | None,
    account_id: str,
    contract_key: str,
    local_symbol: str,
    databento_continuous_symbol: str,
    dataset: str,
    timeframe: str,
    candle_source_mode: str,
    max_bars: int,
    strategy_id: str,
    lane_id: str,
    now: datetime,
    fresh_for_execution: bool,
    execution_freshness_blocker: str | None,
) -> dict[str, Any]:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    latest = candles[-1]
    artifact_symbol = _artifact_symbol_from_contract(contract_key, local_symbol)
    instrument_family = str(contract_key or artifact_symbol).split("-", 1)[0].upper()
    return {
        "schema_version": f"track_b_runtime_{artifact_symbol}_1m_candles_v1",
        "source_id": source_id,
        "capture_id": capture_id,
        "batch_id": _optional_text(payload.get("batch_id")) or f"track_b_runtime_candle_batch_{uuid.uuid4().hex}",
        "account_id": _optional_text(payload.get("account_id")) or account_id,
        "contract_key": contract_key,
        "instrument_family": instrument_family,
        "symbol": local_symbol,
        "local_symbol": local_symbol,
        "allowlisted_local_symbol": local_symbol,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "candle_source_mode": candle_source_mode,
        "generated_at": now.isoformat(),
        "strategy_id": _optional_text(payload.get("strategy_id")) or strategy_id,
        "lane_id": _optional_text(payload.get("lane_id")) or lane_id,
        "max_bars": max_bars,
        "bars_available": len(candles),
        "first_candle_timestamp": candles[0].get("candle_timestamp"),
        "last_candle_timestamp": latest.get("candle_timestamp"),
        "candle_timestamp": latest.get("candle_timestamp"),
        "observed_at": latest.get("observed_at"),
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "candles": list(candles),
        "candle_history": list(candles),
        "data_written": True,
        "fresh_for_execution": fresh_for_execution,
        "execution_freshness_blocker": execution_freshness_blocker,
        "runtime_candle_context_ready": fresh_for_execution,
        "quote_provider_mode": quote_evidence.get("quote_provider_mode"),
        "realtime_quote_received": quote_evidence.get("realtime_quote_received"),
        "current_quote_available": quote_evidence.get("current_quote_available"),
        "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict"),
        "metadata": {
            **dict(metadata),
            "track_b_runtime_candle_capture_boundary": "track_b_runtime_candle_capture",
            "market_data_role": "RUNTIME_CONTEXT_EVIDENCE_ONLY",
            "source_payload_path": None if source_payload_path is None else str(source_payload_path),
            "source_report_path": quote_evidence.get("quote_report_path") or metadata.get("source_report_path"),
            "bounded_max_bars": max_bars,
            "research_archive": False,
            "databento_is_execution_authority": False,
            "local_execution_contract_key_remains_authority": True,
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _write_report(
    *,
    report_json: Path,
    event_json: Path,
    verdict: TrackBRuntimeCandleCaptureVerdict,
    now: datetime,
    capture_id: str,
    source_id: str,
    source_payload_path: Path | None,
    account_id: str,
    contract_key: str,
    local_symbol: str,
    databento_continuous_symbol: str,
    dataset: str,
    timeframe: str,
    candle_source_mode: str,
    max_bars: int,
    min_bars: int,
    candles: Sequence[Mapping[str, Any]],
    duplicate_count: int,
    gap_count: int,
    quote_evidence: Mapping[str, Any],
    runtime_event: dict[str, Any] | None,
    primary_blocker: str | None,
    required_next_action: str,
    retention_runs: int,
    requested_window_start: datetime | None = None,
    requested_window_end: datetime | None = None,
    provider_available_end: datetime | None = None,
    history_end_used: datetime | None = None,
    available_end_lag_seconds: int | None = None,
    provider_transport: str | None = None,
    provider_request_symbol: str | None = None,
    provider_request_stype_in: str | None = None,
    provider_request_stype_out: str | None = None,
    max_latest_1m_age_seconds: int | None = None,
    max_completed_5m_age_seconds: int | None = None,
    provider_credential_status: str | None = None,
    provider_credential_source: str | None = None,
) -> TrackBRuntimeCandleCaptureResult:
    output_root = report_json.parent.parent
    artifact_symbol = _artifact_symbol_from_contract(contract_key, local_symbol)
    latest_report_json = output_root / f"latest_runtime_candle_capture_{artifact_symbol}_report.json"
    latest_event_json = output_root / f"latest_runtime_{artifact_symbol}_1m_candles.json"
    data_written = runtime_event is not None and verdict in _DATA_WRITTEN_VERDICTS
    freshness = _runtime_freshness(
        candles=candles,
        now=now,
        max_latest_1m_age_seconds=max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        provider_available_end=provider_available_end,
    )
    fresh_for_execution = data_written and freshness["runtime_candle_context_stale"] is not True
    execution_freshness_blocker = None if fresh_for_execution else (_stale_blocker(freshness) if data_written else None)
    report = {
        "schema_version": "track_b_runtime_candle_capture_report_v1",
        "generated_at": now.isoformat(),
        "track_b_runtime_candle_capture_id": capture_id,
        "runtime_candle_capture_verdict": verdict.value,
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "account_id": account_id,
        "contract_key": contract_key,
        "symbol": local_symbol,
        "local_symbol": local_symbol,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "candle_source_mode": candle_source_mode,
        "requested_window_start": None if requested_window_start is None else requested_window_start.astimezone(UTC).isoformat(),
        "requested_window_end": None if requested_window_end is None else requested_window_end.astimezone(UTC).isoformat(),
        "provider_available_end": None if provider_available_end is None else provider_available_end.astimezone(UTC).isoformat(),
        "history_end_used": None if history_end_used is None else history_end_used.astimezone(UTC).isoformat(),
        "available_end_lag_seconds": available_end_lag_seconds,
        "provider_transport": provider_transport,
        "provider_request_symbol": provider_request_symbol,
        "provider_request_stype_in": provider_request_stype_in,
        "provider_request_stype_out": provider_request_stype_out,
        "provider_credential_status": provider_credential_status or "NOT_APPLICABLE",
        "provider_credential_source": provider_credential_source,
        "max_bars": max_bars,
        "min_bars": min_bars,
        "bars_available": len(candles),
        "first_candle_timestamp": candles[0].get("candle_timestamp") if candles else None,
        "last_candle_timestamp": candles[-1].get("candle_timestamp") if candles else None,
        **freshness,
        "data_written": data_written,
        "fresh_for_execution": fresh_for_execution,
        "execution_freshness_blocker": execution_freshness_blocker,
        "runtime_candle_context_ready": fresh_for_execution,
        "duplicate_count": duplicate_count,
        "gap_count": gap_count,
        "quote_provider_mode": quote_evidence.get("quote_provider_mode") or "NOT_PROVIDED",
        "realtime_quote_received": quote_evidence.get("realtime_quote_received") if quote_evidence else False,
        "current_quote_available": quote_evidence.get("current_quote_available") if quote_evidence else False,
        "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict") or "NOT_PROVIDED",
        "output_runtime_candles_path": str(event_json) if data_written else None,
        "latest_runtime_candles_path": str(latest_event_json) if data_written else None,
        "stored_run_count": _prune_old_runs(output_root=output_root, keep=max(int(retention_runs), 0), current_run_dir=report_json.parent),
        "retention_runs": retention_runs,
        "primary_blocker": primary_blocker,
        "secondary_blockers": [],
        "required_next_action": required_next_action,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "paper_proof_cli_called": False,
        "listener_invoked": False,
        "runner_invoked": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "place_order_called": False,
        "cancel_called": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    if artifact_symbol == "mgc":
        (output_root / "latest_runtime_candle_capture_report.json").write_text(payload, encoding="utf-8")
    if data_written:
        event_payload = json.dumps(to_jsonable(runtime_event), indent=2, sort_keys=True)
        event_json.write_text(event_payload, encoding="utf-8")
        latest_event_json.write_text(event_payload, encoding="utf-8")
        if artifact_symbol == "mgc":
            (output_root / "latest_runtime_mgc_1m_candles.json").write_text(event_payload, encoding="utf-8")
    return TrackBRuntimeCandleCaptureResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        runtime_candles_json=event_json if data_written else None,
        runtime_candles_event=runtime_event,
    )


def _runtime_freshness(
    *,
    candles: Sequence[Mapping[str, Any]],
    now: datetime,
    max_latest_1m_age_seconds: int | None,
    max_completed_5m_age_seconds: int | None,
    provider_available_end: datetime | None = None,
) -> dict[str, Any]:
    latest_1m_timestamp = _latest_candle_timestamp(candles)
    latest_completed_5m_timestamp = _latest_completed_5m_timestamp(candles)
    latest_1m_age = None if latest_1m_timestamp is None else max(0.0, (now - latest_1m_timestamp).total_seconds())
    latest_completed_5m_age = (
        None if latest_completed_5m_timestamp is None else max(0.0, (now - latest_completed_5m_timestamp).total_seconds())
    )
    latest_1m_stale = (
        max_latest_1m_age_seconds is not None
        and (latest_1m_age is None or latest_1m_age > max_latest_1m_age_seconds)
    )
    completed_5m_stale = (
        max_completed_5m_age_seconds is not None
        and (latest_completed_5m_age is None or latest_completed_5m_age > max_completed_5m_age_seconds)
    )
    stale = latest_1m_stale or completed_5m_stale
    provider_lag = None
    completed_5m_lag_vs_provider = None
    if provider_available_end is not None:
        provider_lag = (now - provider_available_end.astimezone(UTC)).total_seconds()
        if latest_completed_5m_timestamp is not None:
            completed_5m_lag_vs_provider = (
                provider_available_end.astimezone(UTC) - latest_completed_5m_timestamp
            ).total_seconds()
    return {
        "latest_1m_timestamp": None if latest_1m_timestamp is None else latest_1m_timestamp.isoformat(),
        "latest_1m_candle_timestamp": None if latest_1m_timestamp is None else latest_1m_timestamp.isoformat(),
        "latest_1m_candle_age_seconds": None if latest_1m_age is None else round(latest_1m_age, 3),
        "latest_1m_candle_age_minutes": None if latest_1m_age is None else round(latest_1m_age / 60.0, 3),
        "max_latest_1m_candle_age_seconds": max_latest_1m_age_seconds,
        "latest_completed_5m_timestamp": None if latest_completed_5m_timestamp is None else latest_completed_5m_timestamp.isoformat(),
        "latest_completed_5m_candle_timestamp": None if latest_completed_5m_timestamp is None else latest_completed_5m_timestamp.isoformat(),
        "latest_completed_5m_candle_age_seconds": None if latest_completed_5m_age is None else round(latest_completed_5m_age, 3),
        "latest_completed_5m_candle_age_minutes": None if latest_completed_5m_age is None else round(latest_completed_5m_age / 60.0, 3),
        "max_completed_5m_candle_age_seconds": max_completed_5m_age_seconds,
        "provider_lag_seconds_vs_wall_clock": None if provider_lag is None else round(provider_lag, 3),
        "completed_5m_lag_vs_provider_seconds": (
            None if completed_5m_lag_vs_provider is None else round(completed_5m_lag_vs_provider, 3)
        ),
        "completed_5m_lag_vs_wall_clock_seconds": None if latest_completed_5m_age is None else round(latest_completed_5m_age, 3),
        "latest_1m_candle_stale": latest_1m_stale,
        "completed_5m_candle_stale": completed_5m_stale,
        "runtime_candle_context_stale": stale,
        "runtime_candle_context_fresh": None if max_latest_1m_age_seconds is None and max_completed_5m_age_seconds is None else not stale,
    }


def _latest_candle_timestamp(candles: Sequence[Mapping[str, Any]]) -> datetime | None:
    if not candles:
        return None
    return _parse_timestamp(str(candles[-1]["candle_timestamp"]))


def _latest_completed_5m_timestamp(candles: Sequence[Mapping[str, Any]]) -> datetime | None:
    candidates: list[datetime] = []
    for candle in candles:
        timestamp = _parse_timestamp(str(candle["candle_timestamp"]))
        if timestamp.minute % 5 == 0:
            candidates.append(timestamp)
    return max(candidates) if candidates else None


def _stale_blocker(freshness: Mapping[str, Any]) -> str:
    parts: list[str] = []
    if freshness.get("latest_1m_candle_stale") is True:
        parts.append(
            "latest 1m candle age "
            f"{freshness.get('latest_1m_candle_age_seconds')}s exceeds max "
            f"{freshness.get('max_latest_1m_candle_age_seconds')}s"
        )
    if freshness.get("completed_5m_candle_stale") is True:
        parts.append(
            "latest completed 5m candle age "
            f"{freshness.get('latest_completed_5m_candle_age_seconds')}s exceeds max "
            f"{freshness.get('max_completed_5m_candle_age_seconds')}s"
        )
    detail = "; ".join(parts) if parts else "runtime candle freshness threshold was not met"
    return f"Track B runtime candle context is stale: {detail}."


def _prune_old_runs(*, output_root: Path, keep: int, current_run_dir: Path) -> int:
    if keep <= 0:
        return 0
    output_root.mkdir(parents=True, exist_ok=True)
    existing_to_keep = max(keep - 1, 0) if not current_run_dir.exists() else keep
    run_dirs = [
        item
        for item in output_root.iterdir()
        if item.is_dir() and item.name.startswith("track_b_runtime_candle_capture_")
    ]
    run_dirs.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    retained = 0
    for item in run_dirs:
        if item == current_run_dir or retained < existing_to_keep:
            retained += 1
            continue
        shutil.rmtree(item)
    return min(retained + (0 if current_run_dir.exists() else 1), keep)


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible.") from exc


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _required_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _first_text(*values: object) -> str | None:
    for value in values:
        text = _optional_text(value)
        if text is not None:
            return text
    return None


def _first_bool(*values: object) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None
