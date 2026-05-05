"""Track B Databento Live runtime feed writer.

This boundary owns live market-data artifacts only. It has no broker access and
does not provide execution authority. Historical/HTTP data remains a separate
backfill/recovery path; runtime execution decisions should use artifacts written
from this Databento Live stream.
"""

from __future__ import annotations

import contextlib
import importlib.metadata
import json
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from .models import require_aware_datetime, to_jsonable
from .track_b_runtime_candle_capture import MGC_CONTINUOUS_SYMBOL, MGC_DATASET, MGC_LOCAL_SYMBOL
from .track_b_runtime_candle_capture_cli import _load_databento_api_key


DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/databento_live_runtime_feed"
)


class TrackBDatabentoLiveFeedVerdict(str, Enum):
    DATA_WRITTEN_EXECUTION_FRESH = "TRACK_B_DATABENTO_LIVE_FEED_DATA_WRITTEN_EXECUTION_FRESH"
    DATA_WRITTEN_NOT_EXECUTION_FRESH = "TRACK_B_DATABENTO_LIVE_FEED_DATA_WRITTEN_NOT_EXECUTION_FRESH"
    LIVE_FEED_NOT_READY = "TRACK_B_DATABENTO_LIVE_FEED_NOT_READY"
    PROVIDER_LIVE_UNAVAILABLE = "TRACK_B_DATABENTO_LIVE_FEED_PROVIDER_LIVE_UNAVAILABLE"
    BLOCKED_NO_RECORDS = "TRACK_B_DATABENTO_LIVE_FEED_BLOCKED_NO_RECORDS"
    BLOCKED_INVALID_RECORDS = "TRACK_B_DATABENTO_LIVE_FEED_BLOCKED_INVALID_RECORDS"


class DatabentoLiveRuntimeFeedError(RuntimeError):
    def __init__(self, message: str, *, diagnostics: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.diagnostics = dict(diagnostics or {})


class DatabentoLiveClient(Protocol):
    def add_callback(self, callback: Callable[[Any], None], exception_callback: Callable[[Exception], None]) -> None: ...

    def subscribe(self, **kwargs: Any) -> None: ...

    def start(self) -> None: ...

    def terminate(self) -> None: ...


@dataclass(frozen=True)
class TrackBDatabentoLiveFeedConfig:
    expected_account_id: str = "DUM882026"
    account_id: str = "DUM882026"
    contract_key: str = "MGC-202606"
    instrument_family: str = "MGC"
    local_symbol: str = MGC_LOCAL_SYMBOL
    databento_continuous_symbol: str = MGC_CONTINUOUS_SYMBOL
    dataset: str = MGC_DATASET
    schema: str = "ohlcv-1m"
    stype_in: str = "continuous"
    stype_out: str | None = "instrument_id"
    timeframe: str = "1m"
    max_bars: int = 90
    min_bars: int = 8
    max_records: int = 90
    max_seconds: float = 75.0
    max_latest_1m_age_seconds: int = 90
    max_completed_5m_age_seconds: int = 360
    env_file: Path | None = None
    output_root: Path = DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT
    source_id: str = "track_b_databento_live_runtime_feed"

    def selector_symbol(self) -> str:
        return self.databento_continuous_symbol


@dataclass(frozen=True)
class TrackBDatabentoLiveFeedResult:
    verdict: TrackBDatabentoLiveFeedVerdict
    report_json: Path
    report: dict[str, Any]
    live_1m_candles_json: Path | None
    live_1m_candles_event: dict[str, Any] | None


def _artifact_symbol(instrument_family: str | None) -> str:
    return str(instrument_family or "MGC").strip().lower() or "mgc"


def _latest_live_1m_path(root: Path, instrument_family: str | None) -> Path:
    return Path(root) / f"latest_live_{_artifact_symbol(instrument_family)}_1m_candles.json"


def _latest_live_completed_5m_path(root: Path, instrument_family: str | None) -> Path:
    return Path(root) / f"latest_live_{_artifact_symbol(instrument_family)}_completed_5m_candles.json"


def _latest_live_report_path(root: Path, instrument_family: str | None) -> Path:
    return Path(root) / f"latest_databento_live_runtime_feed_{_artifact_symbol(instrument_family)}_report.json"


def _latest_live_heartbeat_path(root: Path, instrument_family: str | None) -> Path:
    return Path(root) / f"latest_databento_live_runtime_feed_{_artifact_symbol(instrument_family)}_heartbeat.json"


def _latest_live_quote_status_path(root: Path, instrument_family: str | None) -> Path:
    return Path(root) / f"latest_live_{_artifact_symbol(instrument_family)}_quote_status_report.json"


def run_track_b_databento_live_runtime_feed(
    *,
    config: TrackBDatabentoLiveFeedConfig,
    live_client_factory: Callable[[str], DatabentoLiveClient] | None = None,
    now_func: Callable[[], datetime] | None = None,
    run_id: str | None = None,
) -> TrackBDatabentoLiveFeedResult:
    clock = now_func or (lambda: datetime.now(UTC))
    started_at = clock()
    require_aware_datetime(started_at, "started_at")
    actual_run_id = run_id or f"track_b_databento_live_runtime_feed_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_run_id / "databento_live_runtime_feed_report.json"
    live_1m_json = Path(config.output_root) / actual_run_id / f"live_{_artifact_symbol(config.instrument_family)}_1m_candles.json"
    if config.max_records <= 0:
        return _write_provider_error(
            config=config,
            report_json=report_json,
            live_1m_json=live_1m_json,
            run_id=actual_run_id,
            started_at=started_at,
            completed_at=clock(),
            verdict=TrackBDatabentoLiveFeedVerdict.LIVE_FEED_NOT_READY,
            primary_blocker="max_records must be positive.",
            provider_diagnostics={},
            credential_status="NOT_APPLICABLE",
            credential_source=None,
        )
    api_key, credential_status, credential_source = _load_databento_api_key(config.env_file)
    if not api_key:
        return _write_provider_error(
            config=config,
            report_json=report_json,
            live_1m_json=live_1m_json,
            run_id=actual_run_id,
            started_at=started_at,
            completed_at=clock(),
            verdict=TrackBDatabentoLiveFeedVerdict.PROVIDER_LIVE_UNAVAILABLE,
            primary_blocker="DATABENTO_API_KEY is missing for Databento Live runtime feed.",
            provider_diagnostics={"provider_error_category": "DATABENTO_LIVE_CREDENTIAL_MISSING"},
            credential_status=credential_status,
            credential_source=credential_source,
        )

    raw_records: list[Any] = []
    errors: list[Exception] = []
    diagnostics = _base_diagnostics(config=config, credential_status=credential_status, credential_source=credential_source)
    records_ready = threading.Event()
    valid_live_ohlcv_count = {"count": 0}
    valid_live_candles: list[dict[str, Any]] = []

    def on_record(record: Any) -> None:
        raw_records.append(record)
        diagnostics["latest_record_type"] = type(record).__name__
        diagnostics["records_received"] = len(raw_records)
        try:
            candle, ts_event, ts_recv = _record_to_candle(record, received_at=clock())
        except ValueError:
            diagnostics["non_ohlcv_live_message_count"] = int(diagnostics.get("non_ohlcv_live_message_count") or 0) + 1
        else:
            valid_live_ohlcv_count["count"] += 1
            valid_live_candles.append(candle)
            diagnostics["valid_live_ohlcv_record_count"] = valid_live_ohlcv_count["count"]
            _write_hot_live_artifacts(
                config=config,
                run_id=actual_run_id,
                generated_at=clock(),
                candles=valid_live_candles[-config.max_bars :],
                latest_ts_event=ts_event,
                latest_ts_recv=ts_recv,
            )
        if valid_live_ohlcv_count["count"] >= config.max_records:
            records_ready.set()

    def on_error(exc: Exception) -> None:
        errors.append(exc)
        records_ready.set()

    client: DatabentoLiveClient | None = None
    try:
        client = _create_live_client(api_key=api_key, live_client_factory=live_client_factory)
        diagnostics["databento_live_api_available"] = True
        diagnostics["subscription_status"] = "SUBSCRIPTION_ATTEMPTED"
        client.add_callback(on_record, on_error)
        subscribe_kwargs = {
            "dataset": config.dataset,
            "schema": config.schema,
            "symbols": [config.selector_symbol()],
            "stype_in": config.stype_in,
        }
        if config.stype_out:
            subscribe_kwargs["stype_out"] = config.stype_out
        try:
            client.subscribe(**subscribe_kwargs)
        except TypeError:
            if "stype_out" not in subscribe_kwargs:
                raise
            diagnostics["stype_out_omitted_after_subscribe_typeerror"] = True
            subscribe_kwargs.pop("stype_out", None)
            client.subscribe(**subscribe_kwargs)
        diagnostics["subscription_status"] = "SUBSCRIBED"
        client.start()
        records_ready.wait(max(float(config.max_seconds), 0.0))
    except Exception as exc:  # noqa: BLE001 - provider errors must become artifacts.
        diagnostics.update(_diagnostics_from_exception(exc, category="DATABENTO_LIVE_SUBSCRIPTION_ERROR"))
        return _write_provider_error(
            config=config,
            report_json=report_json,
            live_1m_json=live_1m_json,
            run_id=actual_run_id,
            started_at=started_at,
            completed_at=clock(),
            verdict=TrackBDatabentoLiveFeedVerdict.PROVIDER_LIVE_UNAVAILABLE,
            primary_blocker=f"Databento Live subscription failed: {_sanitize_exception(exc)}",
            provider_diagnostics=diagnostics,
            credential_status=credential_status,
            credential_source=credential_source,
        )
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.terminate()

    if errors:
        exc = errors[0]
        diagnostics.update(_diagnostics_from_exception(exc, category="DATABENTO_LIVE_CALLBACK_ERROR"))
        return _write_provider_error(
            config=config,
            report_json=report_json,
            live_1m_json=live_1m_json,
            run_id=actual_run_id,
            started_at=started_at,
            completed_at=clock(),
            verdict=TrackBDatabentoLiveFeedVerdict.PROVIDER_LIVE_UNAVAILABLE,
            primary_blocker=f"Databento Live callback failed: {_sanitize_exception(exc)}",
            provider_diagnostics=diagnostics,
            credential_status=credential_status,
            credential_source=credential_source,
        )
    completed_at = clock()
    require_aware_datetime(completed_at, "completed_at")
    if not raw_records:
        diagnostics["subscription_status"] = "NO_RECORDS_WITHIN_BOUNDED_WAIT"
        return _write_provider_error(
            config=config,
            report_json=report_json,
            live_1m_json=live_1m_json,
            run_id=actual_run_id,
            started_at=started_at,
            completed_at=completed_at,
            verdict=TrackBDatabentoLiveFeedVerdict.BLOCKED_NO_RECORDS,
            primary_blocker="Databento Live produced no records within bounded wait.",
            provider_diagnostics=diagnostics,
            credential_status=credential_status,
            credential_source=credential_source,
        )
    normalized: list[dict[str, Any]] = []
    invalid_count = 0
    latest_ts_recv: datetime | None = None
    latest_ts_event: datetime | None = None
    for record in raw_records:
        try:
            candle, ts_event, ts_recv = _record_to_candle(record, received_at=completed_at)
        except ValueError:
            invalid_count += 1
            continue
        normalized.append(candle)
        latest_ts_event = ts_event if latest_ts_event is None or ts_event > latest_ts_event else latest_ts_event
        if ts_recv is not None:
            latest_ts_recv = ts_recv if latest_ts_recv is None or ts_recv > latest_ts_recv else latest_ts_recv
    candles, duplicate_count = _dedupe_and_sort(normalized)
    candles = candles[-config.max_bars :]
    if len(candles) < config.min_bars:
        diagnostics["invalid_record_count"] = invalid_count
        diagnostics["valid_ohlcv_1m_record_count"] = len(candles)
        return _write_provider_error(
            config=config,
            report_json=report_json,
            live_1m_json=live_1m_json,
            run_id=actual_run_id,
            started_at=started_at,
            completed_at=completed_at,
            verdict=TrackBDatabentoLiveFeedVerdict.BLOCKED_INVALID_RECORDS,
            primary_blocker=f"Databento Live did not provide enough valid ohlcv-1m records; received {len(candles)} valid records.",
            provider_diagnostics=diagnostics,
            credential_status=credential_status,
            credential_source=credential_source,
        )
    return _write_success(
        config=config,
        report_json=report_json,
        live_1m_json=live_1m_json,
        run_id=actual_run_id,
        started_at=started_at,
        completed_at=completed_at,
        candles=candles,
        duplicate_count=duplicate_count,
        invalid_count=invalid_count,
        raw_record_count=len(raw_records),
        latest_ts_event=latest_ts_event,
        latest_ts_recv=latest_ts_recv,
        provider_diagnostics=diagnostics,
        credential_status=credential_status,
        credential_source=credential_source,
    )


def _write_success(
    *,
    config: TrackBDatabentoLiveFeedConfig,
    report_json: Path,
    live_1m_json: Path,
    run_id: str,
    started_at: datetime,
    completed_at: datetime,
    candles: Sequence[Mapping[str, Any]],
    duplicate_count: int,
    invalid_count: int,
    raw_record_count: int,
    latest_ts_event: datetime | None,
    latest_ts_recv: datetime | None,
    provider_diagnostics: Mapping[str, Any],
    credential_status: str,
    credential_source: str | None,
) -> TrackBDatabentoLiveFeedResult:
    latest_1m = _parse_time(str(candles[-1]["candle_timestamp"])) if candles else None
    latest_completed_5m = _latest_completed_5m_timestamp(candles)
    latency_anchor = latest_ts_recv or latest_ts_event or latest_1m
    latency_ms = None if latency_anchor is None else max(0.0, (completed_at - latency_anchor).total_seconds() * 1000.0)
    latest_1m_age = None if latest_1m is None else max(0.0, (completed_at - latest_1m).total_seconds())
    completed_5m_age = None if latest_completed_5m is None else max(0.0, (completed_at - latest_completed_5m).total_seconds())
    fresh_for_execution = (
        latest_1m_age is not None
        and latest_1m_age <= config.max_latest_1m_age_seconds
        and completed_5m_age is not None
        and completed_5m_age <= config.max_completed_5m_age_seconds
    )
    verdict = (
        TrackBDatabentoLiveFeedVerdict.DATA_WRITTEN_EXECUTION_FRESH
        if fresh_for_execution
        else TrackBDatabentoLiveFeedVerdict.DATA_WRITTEN_NOT_EXECUTION_FRESH
    )
    event = _live_1m_event(
        config=config,
        run_id=run_id,
        completed_at=completed_at,
        candles=candles,
        fresh_for_execution=fresh_for_execution,
        latest_1m_age=latest_1m_age,
        completed_5m_age=completed_5m_age,
    )
    completed_5m_candles = _completed_5m_candles(candles, instrument_family=config.instrument_family)
    report = _base_report(
        config=config,
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        verdict=verdict.value,
        credential_status=credential_status,
        credential_source=credential_source,
        provider_diagnostics=provider_diagnostics,
    )
    report.update(
        {
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "raw_record_count": raw_record_count,
            "valid_ohlcv_1m_record_count": len(candles),
            "invalid_record_count": invalid_count,
            "duplicate_count": duplicate_count,
            "bars_available": len(candles),
            "latest_record_ts_event": None if latest_ts_event is None else latest_ts_event.isoformat(),
            "latest_record_ts_recv": None if latest_ts_recv is None else latest_ts_recv.isoformat(),
            "latest_record_latency_ms": None if latency_ms is None else round(latency_ms, 3),
            "latency_ms": None if latency_ms is None else round(latency_ms, 3),
            "latest_1m_timestamp": None if latest_1m is None else latest_1m.isoformat(),
            "latest_completed_5m_timestamp": None if latest_completed_5m is None else latest_completed_5m.isoformat(),
            "latest_1m_age_seconds": None if latest_1m_age is None else round(latest_1m_age, 3),
            "latest_completed_5m_age_seconds": None if completed_5m_age is None else round(completed_5m_age, 3),
            "fresh_for_execution": fresh_for_execution,
            "runtime_candle_context_ready": fresh_for_execution,
            "primary_blocker": None
            if fresh_for_execution
            else "Databento Live records were written, but latest candles are not fresh_for_execution.",
            "required_next_action": "Track B Live runtime feed is fresh for SHADOW strategy evaluation."
            if fresh_for_execution
            else "Keep Live feed running; do not evaluate execution-live strategies until fresh_for_execution=true.",
            "latest_live_1m_candles_path": str(_latest_live_1m_path(report_json.parent.parent, config.instrument_family)),
            "latest_live_completed_5m_candles_path": str(_latest_live_completed_5m_path(report_json.parent.parent, config.instrument_family)),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
    )
    report["report_json_path"] = str(report_json)
    report["latest_report_json_path"] = str(_latest_live_report_path(report_json.parent.parent, config.instrument_family))
    _write_json(report_json, report)
    _write_json(_latest_live_report_path(report_json.parent.parent, config.instrument_family), report)
    _write_json(live_1m_json, event)
    _write_json(_latest_live_1m_path(report_json.parent.parent, config.instrument_family), event)
    _write_json(_latest_live_completed_5m_path(report_json.parent.parent, config.instrument_family), completed_5m_candles)
    if config.instrument_family == "MGC":
        _write_json(report_json.parent.parent / "latest_databento_live_runtime_feed_report.json", report)
        _write_json(report_json.parent.parent / "latest_live_mgc_1m_candles.json", event)
        _write_json(report_json.parent.parent / "latest_live_mgc_completed_5m_candles.json", completed_5m_candles)
    _write_json(
        _latest_live_heartbeat_path(report_json.parent.parent, config.instrument_family),
        {
            "schema_version": "track_b_databento_live_runtime_feed_heartbeat_v1",
            "generated_at": completed_at.isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "latest_1m_timestamp": report["latest_1m_timestamp"],
            "latest_completed_5m_timestamp": report["latest_completed_5m_timestamp"],
            "fresh_for_execution": fresh_for_execution,
            "report_json_path": str(report_json),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )
    if config.instrument_family == "MGC":
        _write_json(
            report_json.parent.parent / "latest_databento_live_runtime_feed_heartbeat.json",
            {
                "schema_version": "track_b_databento_live_runtime_feed_heartbeat_v1",
                "generated_at": completed_at.isoformat(),
                "live_feed_connected": True,
                "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
                "latest_1m_timestamp": report["latest_1m_timestamp"],
                "latest_completed_5m_timestamp": report["latest_completed_5m_timestamp"],
                "fresh_for_execution": fresh_for_execution,
                "report_json_path": str(report_json),
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
            },
        )
    _write_json(
        _latest_live_quote_status_path(report_json.parent.parent, config.instrument_family),
        {
            "schema_version": "track_b_databento_live_quote_status_v1",
            "generated_at": completed_at.isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "dataset": config.dataset,
            "symbol": config.selector_symbol(),
            "schema": config.schema,
            "quote_status": "LIVE_OHLCV_FEED_STATUS_ONLY",
            "current_quote_available": False,
            "realtime_quote_received": config.schema != "ohlcv-1m",
            "latest_record_ts_event": report["latest_record_ts_event"],
            "latest_record_ts_recv": report["latest_record_ts_recv"],
            "latency_ms": report["latency_ms"],
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )
    if config.instrument_family == "MGC":
        _write_json(
            report_json.parent.parent / "latest_live_quote_status_report.json",
            {
                "schema_version": "track_b_databento_live_quote_status_v1",
                "generated_at": completed_at.isoformat(),
                "live_feed_connected": True,
                "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
                "dataset": config.dataset,
                "symbol": config.selector_symbol(),
                "schema": config.schema,
                "quote_status": "LIVE_OHLCV_FEED_STATUS_ONLY",
                "current_quote_available": False,
                "realtime_quote_received": config.schema != "ohlcv-1m",
                "latest_record_ts_event": report["latest_record_ts_event"],
                "latest_record_ts_recv": report["latest_record_ts_recv"],
                "latency_ms": report["latency_ms"],
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
            },
        )
    return TrackBDatabentoLiveFeedResult(verdict=verdict, report_json=report_json, report=report, live_1m_candles_json=live_1m_json, live_1m_candles_event=event)


def _write_hot_live_artifacts(
    *,
    config: TrackBDatabentoLiveFeedConfig,
    run_id: str,
    generated_at: datetime,
    candles: Sequence[Mapping[str, Any]],
    latest_ts_event: datetime | None,
    latest_ts_recv: datetime | None,
) -> None:
    if not candles:
        return
    generated_at = generated_at.astimezone(UTC)
    latest_1m = _parse_time(str(candles[-1]["candle_timestamp"]))
    latest_completed_5m = _latest_completed_5m_timestamp(candles)
    latest_1m_age = max(0.0, (generated_at - latest_1m).total_seconds())
    completed_5m_age = None if latest_completed_5m is None else max(0.0, (generated_at - latest_completed_5m).total_seconds())
    fresh_for_execution = (
        latest_1m_age <= config.max_latest_1m_age_seconds
        and completed_5m_age is not None
        and completed_5m_age <= config.max_completed_5m_age_seconds
        and len(candles) >= config.min_bars
    )
    event = _live_1m_event(
        config=config,
        run_id=run_id,
        completed_at=generated_at,
        candles=candles,
        fresh_for_execution=fresh_for_execution,
        latest_1m_age=latest_1m_age,
        completed_5m_age=completed_5m_age,
    )
    latency_anchor = latest_ts_recv or latest_ts_event or latest_1m
    latency_ms = max(0.0, (generated_at - latency_anchor).total_seconds() * 1000.0)
    output_root = Path(config.output_root)
    completed_payload = _completed_5m_candles(candles, instrument_family=config.instrument_family)
    _write_json(_latest_live_1m_path(output_root, config.instrument_family), event)
    _write_json(_latest_live_completed_5m_path(output_root, config.instrument_family), completed_payload)
    if config.instrument_family == "MGC":
        _write_json(output_root / "latest_live_mgc_1m_candles.json", event)
        _write_json(output_root / "latest_live_mgc_completed_5m_candles.json", completed_payload)
    _write_json(
        _latest_live_heartbeat_path(output_root, config.instrument_family),
        {
            "schema_version": "track_b_databento_live_runtime_feed_heartbeat_v1",
            "generated_at": generated_at.isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "latest_1m_timestamp": latest_1m.isoformat(),
            "latest_completed_5m_timestamp": None if latest_completed_5m is None else latest_completed_5m.isoformat(),
            "latest_record_ts_event": None if latest_ts_event is None else latest_ts_event.isoformat(),
            "latest_record_ts_recv": None if latest_ts_recv is None else latest_ts_recv.isoformat(),
            "latency_ms": round(latency_ms, 3),
            "fresh_for_execution": fresh_for_execution,
            "bars_available": len(candles),
            "report_json_path": None,
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )
    if config.instrument_family == "MGC":
        _write_json(
            output_root / "latest_databento_live_runtime_feed_heartbeat.json",
            {
                "schema_version": "track_b_databento_live_runtime_feed_heartbeat_v1",
                "generated_at": generated_at.isoformat(),
                "live_feed_connected": True,
                "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
                "latest_1m_timestamp": latest_1m.isoformat(),
                "latest_completed_5m_timestamp": None if latest_completed_5m is None else latest_completed_5m.isoformat(),
                "latest_record_ts_event": None if latest_ts_event is None else latest_ts_event.isoformat(),
                "latest_record_ts_recv": None if latest_ts_recv is None else latest_ts_recv.isoformat(),
                "latency_ms": round(latency_ms, 3),
                "fresh_for_execution": fresh_for_execution,
                "bars_available": len(candles),
                "report_json_path": None,
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
            },
        )
    _write_json(
        _latest_live_quote_status_path(output_root, config.instrument_family),
        {
            "schema_version": "track_b_databento_live_quote_status_v1",
            "generated_at": generated_at.isoformat(),
            "live_feed_connected": True,
            "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
            "dataset": config.dataset,
            "symbol": config.selector_symbol(),
            "schema": config.schema,
            "stype_in": config.stype_in,
            "stype_out": config.stype_out,
            "quote_status": "LIVE_OHLCV_FEED_STATUS_ONLY",
            "current_quote_available": False,
            "realtime_quote_received": config.schema != "ohlcv-1m",
            "latest_record_ts_event": None if latest_ts_event is None else latest_ts_event.isoformat(),
            "latest_record_ts_recv": None if latest_ts_recv is None else latest_ts_recv.isoformat(),
            "latency_ms": round(latency_ms, 3),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )
    if config.instrument_family == "MGC":
        _write_json(
            output_root / "latest_live_quote_status_report.json",
            {
                "schema_version": "track_b_databento_live_quote_status_v1",
                "generated_at": generated_at.isoformat(),
                "live_feed_connected": True,
                "subscription_status": "SUBSCRIBED_RECORDS_RECEIVED",
                "dataset": config.dataset,
                "symbol": config.selector_symbol(),
                "schema": config.schema,
                "stype_in": config.stype_in,
                "stype_out": config.stype_out,
                "quote_status": "LIVE_OHLCV_FEED_STATUS_ONLY",
                "current_quote_available": False,
                "realtime_quote_received": config.schema != "ohlcv-1m",
                "latest_record_ts_event": None if latest_ts_event is None else latest_ts_event.isoformat(),
                "latest_record_ts_recv": None if latest_ts_recv is None else latest_ts_recv.isoformat(),
                "latency_ms": round(latency_ms, 3),
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
            },
        )


def _write_provider_error(
    *,
    config: TrackBDatabentoLiveFeedConfig,
    report_json: Path,
    live_1m_json: Path,
    run_id: str,
    started_at: datetime,
    completed_at: datetime,
    verdict: TrackBDatabentoLiveFeedVerdict,
    primary_blocker: str,
    provider_diagnostics: Mapping[str, Any],
    credential_status: str,
    credential_source: str | None,
) -> TrackBDatabentoLiveFeedResult:
    report = _base_report(
        config=config,
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        verdict=verdict.value,
        credential_status=credential_status,
        credential_source=credential_source,
        provider_diagnostics=provider_diagnostics,
    )
    report.update(
        {
            "live_feed_connected": False,
            "subscription_status": provider_diagnostics.get("subscription_status") or "NOT_READY",
            "fresh_for_execution": False,
            "runtime_candle_context_ready": False,
            "primary_blocker": primary_blocker,
            "required_next_action": "Start or repair Databento Live runtime feed; do not fall back to delayed HTTP as live.",
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
    )
    report["report_json_path"] = str(report_json)
    report["latest_report_json_path"] = str(_latest_live_report_path(report_json.parent.parent, config.instrument_family))
    _write_json(report_json, report)
    _write_json(_latest_live_report_path(report_json.parent.parent, config.instrument_family), report)
    if config.instrument_family == "MGC":
        _write_json(report_json.parent.parent / "latest_databento_live_runtime_feed_report.json", report)
    _write_json(
        _latest_live_heartbeat_path(report_json.parent.parent, config.instrument_family),
        {
            "schema_version": "track_b_databento_live_runtime_feed_heartbeat_v1",
            "generated_at": completed_at.isoformat(),
            "live_feed_connected": False,
            "subscription_status": report["subscription_status"],
            "fresh_for_execution": False,
            "primary_blocker": primary_blocker,
            "report_json_path": str(report_json),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        },
    )
    if config.instrument_family == "MGC":
        _write_json(
            report_json.parent.parent / "latest_databento_live_runtime_feed_heartbeat.json",
            {
                "schema_version": "track_b_databento_live_runtime_feed_heartbeat_v1",
                "generated_at": completed_at.isoformat(),
                "live_feed_connected": False,
                "subscription_status": report["subscription_status"],
                "fresh_for_execution": False,
                "primary_blocker": primary_blocker,
                "report_json_path": str(report_json),
                "submit_allowed": False,
                "submit_attempted": False,
                "live_money_readiness": False,
            },
        )
    return TrackBDatabentoLiveFeedResult(verdict=verdict, report_json=report_json, report=report, live_1m_candles_json=None, live_1m_candles_event=None)


def _base_report(
    *,
    config: TrackBDatabentoLiveFeedConfig,
    run_id: str,
    started_at: datetime,
    completed_at: datetime,
    verdict: str,
    credential_status: str,
    credential_source: str | None,
    provider_diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_databento_live_runtime_feed_report_v1",
        "track_b_databento_live_runtime_feed_id": run_id,
        "generated_at": completed_at.isoformat(),
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "wall_clock_time": completed_at.isoformat(),
        "live_runtime_feed_verdict": verdict,
        "source_id": config.source_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "contract_key": config.contract_key,
        "instrument_family": config.instrument_family,
        "symbol": config.local_symbol,
        "local_symbol": config.local_symbol,
        "databento_continuous_symbol": config.databento_continuous_symbol,
        "dataset": config.dataset,
        "schema": config.schema,
        "stype_in": config.stype_in,
        "stype_out": config.stype_out,
        "timeframe": config.timeframe,
        "max_bars": config.max_bars,
        "min_bars": config.min_bars,
        "max_records": config.max_records,
        "max_seconds": config.max_seconds,
        "max_latest_1m_age_seconds": config.max_latest_1m_age_seconds,
        "max_completed_5m_age_seconds": config.max_completed_5m_age_seconds,
        "provider_credential_status": credential_status,
        "provider_credential_source": credential_source,
        "databento_package_version": _installed_package_version("databento"),
        "provider_diagnostics": dict(provider_diagnostics),
        "report_json_path": None,
        "latest_report_json_path": None,
        "paper_proof_cli_called": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "place_order_called": False,
        "cancel_called": False,
    }


def _live_1m_event(
    *,
    config: TrackBDatabentoLiveFeedConfig,
    run_id: str,
    completed_at: datetime,
    candles: Sequence[Mapping[str, Any]],
    fresh_for_execution: bool,
    latest_1m_age: float | None,
    completed_5m_age: float | None,
) -> dict[str, Any]:
    latest = candles[-1]
    return {
        "schema_version": f"track_b_databento_live_{_artifact_symbol(config.instrument_family)}_1m_candles_v1",
        "source_id": config.source_id,
        "capture_id": run_id,
        "account_id": config.account_id,
        "expected_account_id": config.expected_account_id,
        "contract_key": config.contract_key,
        "instrument_family": config.instrument_family,
        "symbol": config.local_symbol,
        "local_symbol": config.local_symbol,
        "allowlisted_local_symbol": config.local_symbol,
        "databento_continuous_symbol": config.databento_continuous_symbol,
        "dataset": config.dataset,
        "timeframe": config.timeframe,
        "candle_source_mode": "DATABENTO_LIVE_RUNTIME_FEED",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": "DATABENTO_LIVE_RUNTIME_FEED_CONNECTED",
        "generated_at": completed_at.isoformat(),
        "max_bars": config.max_bars,
        "bars_available": len(candles),
        "first_candle_timestamp": candles[0].get("candle_timestamp"),
        "last_candle_timestamp": latest.get("candle_timestamp"),
        "candle_timestamp": latest.get("candle_timestamp"),
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "candles": list(candles),
        "candle_history": list(candles),
        "data_written": True,
        "fresh_for_execution": fresh_for_execution,
        "runtime_candle_context_ready": fresh_for_execution,
        "latest_1m_age_seconds": None if latest_1m_age is None else round(latest_1m_age, 3),
        "latest_completed_5m_age_seconds": None if completed_5m_age is None else round(completed_5m_age, 3),
        "metadata": {
            "track_b_live_runtime_feed_boundary": "track_b_databento_live_runtime_feed",
            "market_data_role": "PRIMARY_LIVE_RUNTIME_CONTEXT",
            "research_archive": False,
            "http_historical_backfill": False,
            "databento_is_execution_authority": False,
            "local_execution_contract_key_remains_authority": True,
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _completed_5m_candles(candles: Sequence[Mapping[str, Any]], *, instrument_family: str = "MGC") -> dict[str, Any]:
    grouped: dict[datetime, list[Mapping[str, Any]]] = {}
    for candle in candles:
        ts = _parse_time(str(candle["candle_timestamp"]))
        end_minute = ts.minute - (ts.minute % 5)
        key = ts.replace(minute=end_minute, second=0, microsecond=0)
        grouped.setdefault(key, []).append(candle)
    completed = []
    for key in sorted(grouped):
        rows = sorted(grouped[key], key=lambda item: str(item.get("candle_timestamp")))
        if len(rows) < 5:
            continue
        completed.append(
            {
                "candle_timestamp": key.isoformat(),
                "open": rows[0].get("open"),
                "high": _decimal_text(max(_decimal(row.get("high")) for row in rows)),
                "low": _decimal_text(min(_decimal(row.get("low")) for row in rows)),
                "close": rows[-1].get("close"),
                "volume": _decimal_text(sum((_decimal(row.get("volume") or "0") for row in rows), Decimal("0"))),
            }
        )
    return {
        "schema_version": f"track_b_databento_live_{_artifact_symbol(instrument_family)}_completed_5m_candles_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "candles": completed,
        "bars_available": len(completed),
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _record_to_candle(record: Any, *, received_at: datetime) -> tuple[dict[str, Any], datetime, datetime | None]:
    ts_event = _timestamp_from_record(record, "ts_event") or _timestamp_from_record(record, "pretty_ts_event")
    if ts_event is None:
        raise ValueError("record missing ts_event")
    ts_recv = _timestamp_from_record(record, "ts_recv") or _timestamp_from_record(record, "pretty_ts_recv")
    open_px = _price_from_record(record, "open")
    high_px = _price_from_record(record, "high")
    low_px = _price_from_record(record, "low")
    close_px = _price_from_record(record, "close")
    if open_px is None or high_px is None or low_px is None or close_px is None:
        raise ValueError("record missing OHLC values")
    return (
        {
            "candle_timestamp": ts_event.isoformat(),
            "observed_at": received_at.isoformat(),
            "open": _decimal_text(open_px),
            "high": _decimal_text(high_px),
            "low": _decimal_text(low_px),
            "close": _decimal_text(close_px),
            "volume": _decimal_text(_price_from_record(record, "volume") or Decimal("0")),
            "provider_record_type": type(record).__name__,
        },
        ts_event,
        ts_recv,
    )


def _timestamp_from_record(record: Any, name: str) -> datetime | None:
    value = _field(record, name)
    if callable(value):
        value = value()
    if isinstance(value, datetime):
        return require_aware_datetime(value, name).astimezone(UTC)
    if value is None:
        header = _field(record, "hd")
        if isinstance(header, Mapping):
            value = header.get(name)
    if value in {None, ""}:
        return None
    if isinstance(value, str):
        return _parse_time(value)
    try:
        nanoseconds = int(value)
    except (TypeError, ValueError):
        return None
    if nanoseconds <= 0:
        return None
    seconds, remainder = divmod(nanoseconds, 1_000_000_000)
    return datetime.fromtimestamp(seconds, tz=UTC).replace(microsecond=remainder // 1000)


def _price_from_record(record: Any, name: str) -> Decimal | None:
    value = _field(record, name)
    if value in {None, ""}:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if isinstance(value, int) and abs(value) > 1_000_000:
        parsed = parsed / Decimal("1000000000")
    return parsed


def _field(record: Any, name: str) -> Any:
    if isinstance(record, Mapping):
        if name in record:
            return record[name]
        header = record.get("hd")
        if isinstance(header, Mapping) and name in header:
            return header[name]
        return None
    return getattr(record, name, None)


def _dedupe_and_sort(candles: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    by_timestamp: dict[str, dict[str, Any]] = {}
    duplicates = 0
    for candle in candles:
        key = str(candle["candle_timestamp"])
        if key in by_timestamp:
            duplicates += 1
        by_timestamp[key] = dict(candle)
    return [by_timestamp[key] for key in sorted(by_timestamp, key=_parse_time)], duplicates


def _latest_completed_5m_timestamp(candles: Sequence[Mapping[str, Any]]) -> datetime | None:
    candidates: list[datetime] = []
    for candle in candles:
        ts = _parse_time(str(candle["candle_timestamp"]))
        if ts.minute % 5 == 0:
            candidates.append(ts)
    return max(candidates) if candidates else None


def _create_live_client(
    *, api_key: str, live_client_factory: Callable[[str], DatabentoLiveClient] | None
) -> DatabentoLiveClient:
    if live_client_factory is not None:
        return live_client_factory(api_key)
    try:
        import databento as db  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001
        raise DatabentoLiveRuntimeFeedError(
            "Databento package is not available for Live runtime feed.",
            diagnostics=_diagnostics_from_exception(exc, category="DATABENTO_PACKAGE_IMPORT_ERROR"),
        ) from exc
    if not hasattr(db, "Live"):
        raise DatabentoLiveRuntimeFeedError(
            "Installed databento package does not expose databento.Live.",
            diagnostics={"provider_error_category": "DATABENTO_LIVE_API_MISSING"},
        )
    try:
        return db.Live(key=api_key, ts_out=True)
    except TypeError:
        return db.Live(api_key, ts_out=True)


def _base_diagnostics(
    *, config: TrackBDatabentoLiveFeedConfig, credential_status: str, credential_source: str | None
) -> dict[str, Any]:
    return {
        "live_feed_connected": False,
        "subscription_status": "NOT_STARTED",
        "dataset": config.dataset,
        "symbol": config.selector_symbol(),
        "schema": config.schema,
        "stype_in": config.stype_in,
        "stype_out": config.stype_out,
        "provider_credential_status": credential_status,
        "provider_credential_source": credential_source,
        "records_received": 0,
        "databento_live_api_available": None,
    }


def _diagnostics_from_exception(exc: Exception, *, category: str) -> dict[str, Any]:
    diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
    diagnostics.update({"provider_error_category": category, "native_databento_error_message": _sanitize_exception(exc)})
    return diagnostics


def _sanitize_exception(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    return text.replace(api_key, "<redacted>") if api_key else text


def _installed_package_version(package_name: str) -> str | None:
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any) -> Decimal:
    parsed = Decimal(str(value))
    return parsed


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")
