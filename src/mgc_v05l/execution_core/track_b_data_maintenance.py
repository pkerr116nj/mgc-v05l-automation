"""Track B MGC 1m data maintenance foundation.

This boundary owns the local rolling MGC 1m history used by Track B strategy
features. It is market-data maintenance only: no broker, proof, listener,
strategy, or submit path is invoked here.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_data_maintenance")
MGC_CONTRACT_KEY = "MGC-202606"
MGC_LOCAL_SYMBOL = "MGCM6"
MGC_DATABENTO_CONTINUOUS_SYMBOL = "MGC.v.0"
MGC_DATASET = "GLBX.MDP3"


class TrackBDataMaintenanceVerdict(str, Enum):
    UPDATED_HISTORY_READY = "TRACK_B_DATA_MAINTENANCE_UPDATED_HISTORY_READY"
    UPDATED_HISTORY_STALE_OR_INSUFFICIENT = "TRACK_B_DATA_MAINTENANCE_STALE_OR_INSUFFICIENT"
    BLOCKED_PROVIDER_ERROR = "TRACK_B_DATA_MAINTENANCE_BLOCKED_PROVIDER_ERROR"
    BLOCKED_INVALID_INPUT = "TRACK_B_DATA_MAINTENANCE_BLOCKED_INVALID_INPUT"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_DATA_MAINTENANCE_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBDataMaintenanceResult:
    verdict: TrackBDataMaintenanceVerdict
    report_json: Path
    report: dict[str, Any]
    latest_good_history_json: Path | None
    latest_good_history: dict[str, Any] | None
    store_json: Path


def maintain_track_b_mgc_1m_history(
    *,
    incoming_history_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    output_root: Path = DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT,
    source_payload_path: Path | None = None,
    expected_account_id: str = "DUM882026",
    strategy_id: str = "track_b_example_gold_shadow_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    contract_key: str = MGC_CONTRACT_KEY,
    symbol: str = MGC_LOCAL_SYMBOL,
    databento_continuous_symbol: str = MGC_DATABENTO_CONTINUOUS_SYMBOL,
    dataset: str = MGC_DATASET,
    timeframe: str = "1m",
    max_bars: int = 5000,
    export_bars: int = 50,
    min_bars: int = 20,
    max_history_age_seconds: int = 900,
    maintenance_id: str | None = None,
    source_id: str | None = None,
    now: datetime | None = None,
) -> TrackBDataMaintenanceResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_maintenance_id = maintenance_id or f"track_b_data_maintenance_{uuid.uuid4().hex}"
    actual_output_root = Path(output_root)
    report_json = actual_output_root / actual_maintenance_id / "track_b_data_maintenance_report.json"
    latest_report_json = actual_output_root / "latest_track_b_data_maintenance_report.json"
    store_json = actual_output_root / "store" / "mgc_1m_history.json"
    latest_good_history_json = actual_output_root / "latest_good_mgc_1m_history.json"
    actual_source_id = source_id or _source_id(incoming_history_payload) or "track_b_data_maintenance"
    history_provider_mode = _history_provider_mode(incoming_history_payload)
    requested_history_end = _optional_timestamp_from_payload(incoming_history_payload, "requested_history_end")
    provider_available_end = _optional_timestamp_from_payload(incoming_history_payload, "provider_available_end")
    history_end_used = _optional_timestamp_from_payload(incoming_history_payload, "history_end_used")
    available_end_lag_seconds = _optional_int_from_payload(incoming_history_payload, "available_end_lag_seconds")

    try:
        if max_bars <= 0:
            raise ValueError("max_bars must be positive.")
        if export_bars <= 0:
            raise ValueError("export_bars must be positive.")
        if min_bars <= 0:
            raise ValueError("min_bars must be positive.")
        if max_history_age_seconds < 0:
            raise ValueError("max_history_age_seconds must be non-negative.")
        input_blocker = _input_blocker(
            payload=incoming_history_payload,
            expected_account_id=expected_account_id,
            contract_key=contract_key,
        )
        if input_blocker:
            return _write_result(
                report_json=report_json,
                latest_report_json=latest_report_json,
                store_json=store_json,
                latest_good_history_json=latest_good_history_json,
                verdict=TrackBDataMaintenanceVerdict.BLOCKED_INVALID_INPUT,
                now=actual_now,
                maintenance_id=actual_maintenance_id,
                source_id=actual_source_id,
                source_payload_path=source_payload_path,
                expected_account_id=expected_account_id,
                strategy_id=strategy_id,
                lane_id=lane_id,
                contract_key=contract_key,
                symbol=symbol,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                max_bars=max_bars,
                export_bars=export_bars,
                min_bars=min_bars,
                max_history_age_seconds=max_history_age_seconds,
                store_candles=[],
                export_candles=[],
                duplicate_count=0,
                gap_count=0,
                history_freshness_seconds=None,
                requested_history_end=requested_history_end,
                provider_available_end=provider_available_end,
                history_end_used=history_end_used,
                available_end_lag_seconds=available_end_lag_seconds,
                history_provider_mode=history_provider_mode,
                latest_good_history=None,
                primary_blocker=input_blocker,
                required_next_action="Fix the incoming MGC 1m history input before updating Track B data maintenance.",
            )
        incoming = [_normalize_bar(item, index=index) for index, item in enumerate(_raw_bars(incoming_history_payload), start=1)]
        existing = _store_bars(store_json)
        merged, duplicate_count = _merge_bars([*existing, *incoming])
        bounded_store = merged[-max_bars:]
        export_candles = bounded_store[-export_bars:]
        gap_count = _gap_count(bounded_store)
        history_freshness_seconds = _history_freshness_seconds(export_candles, actual_now)
        blocker = _readiness_blocker(
            export_candles=export_candles,
            min_bars=min_bars,
            gap_count=gap_count,
            history_freshness_seconds=history_freshness_seconds,
            max_history_age_seconds=max_history_age_seconds,
        )
        latest_good_history = _history_export(
            generated_at=actual_now,
            source_id=actual_source_id,
            maintenance_id=actual_maintenance_id,
            source_payload_path=source_payload_path,
            expected_account_id=expected_account_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            contract_key=contract_key,
            symbol=symbol,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            candles=export_candles,
            gap_count=gap_count,
            duplicate_count=duplicate_count,
            history_freshness_seconds=history_freshness_seconds,
            requested_history_end=requested_history_end,
            provider_available_end=provider_available_end,
            history_end_used=history_end_used,
            available_end_lag_seconds=available_end_lag_seconds,
            history_provider_mode=history_provider_mode,
            history_ready=blocker is None,
            primary_blocker=blocker,
        )
        return _write_result(
            report_json=report_json,
            latest_report_json=latest_report_json,
            store_json=store_json,
            latest_good_history_json=latest_good_history_json,
            verdict=TrackBDataMaintenanceVerdict.UPDATED_HISTORY_READY
            if blocker is None
            else TrackBDataMaintenanceVerdict.UPDATED_HISTORY_STALE_OR_INSUFFICIENT,
            now=actual_now,
            maintenance_id=actual_maintenance_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            expected_account_id=expected_account_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            contract_key=contract_key,
            symbol=symbol,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            max_bars=max_bars,
            export_bars=export_bars,
            min_bars=min_bars,
            max_history_age_seconds=max_history_age_seconds,
            store_candles=bounded_store,
            export_candles=export_candles,
            duplicate_count=duplicate_count,
            gap_count=gap_count,
            history_freshness_seconds=history_freshness_seconds,
            requested_history_end=requested_history_end,
            provider_available_end=provider_available_end,
            history_end_used=history_end_used,
            available_end_lag_seconds=available_end_lag_seconds,
            history_provider_mode=history_provider_mode,
            latest_good_history=latest_good_history,
            primary_blocker=blocker,
            required_next_action="Latest-good maintained history is ready; run the Track B feature builder with separate realtime quote evidence."
            if blocker is None
            else "Wait and rerun Track B data maintenance, or widen the diagnostic-only history age threshold before evaluating strategy rules.",
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_result(
            report_json=report_json,
            latest_report_json=latest_report_json,
            store_json=store_json,
            latest_good_history_json=latest_good_history_json,
            verdict=TrackBDataMaintenanceVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            maintenance_id=actual_maintenance_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            expected_account_id=expected_account_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            contract_key=contract_key,
            symbol=symbol,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            max_bars=max_bars,
            export_bars=export_bars,
            min_bars=min_bars,
            max_history_age_seconds=max_history_age_seconds,
            store_candles=[],
            export_candles=[],
            duplicate_count=0,
            gap_count=0,
            history_freshness_seconds=None,
            requested_history_end=requested_history_end,
            provider_available_end=provider_available_end,
            history_end_used=history_end_used,
            available_end_lag_seconds=available_end_lag_seconds,
            history_provider_mode=history_provider_mode,
            latest_good_history=None,
            primary_blocker=str(exc),
            required_next_action="Fix Track B data maintenance input schema before retrying.",
        )


def write_data_maintenance_provider_error(
    *,
    primary_blocker: str,
    required_next_action: str,
    output_root: Path = DEFAULT_TRACK_B_DATA_MAINTENANCE_OUTPUT_ROOT,
    expected_account_id: str = "DUM882026",
    strategy_id: str = "track_b_example_gold_shadow_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    contract_key: str = MGC_CONTRACT_KEY,
    symbol: str = MGC_LOCAL_SYMBOL,
    databento_continuous_symbol: str = MGC_DATABENTO_CONTINUOUS_SYMBOL,
    dataset: str = MGC_DATASET,
    timeframe: str = "1m",
    source_id: str | None = None,
    maintenance_id: str | None = None,
    requested_history_end: datetime | None = None,
    provider_available_end: datetime | None = None,
    history_end_used: datetime | None = None,
    available_end_lag_seconds: int | None = None,
    history_provider_mode: str | None = None,
    now: datetime | None = None,
) -> TrackBDataMaintenanceResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_output_root = Path(output_root)
    actual_maintenance_id = maintenance_id or f"track_b_data_maintenance_{uuid.uuid4().hex}"
    report_json = actual_output_root / actual_maintenance_id / "track_b_data_maintenance_report.json"
    latest_report_json = actual_output_root / "latest_track_b_data_maintenance_report.json"
    store_json = actual_output_root / "store" / "mgc_1m_history.json"
    latest_good_history_json = actual_output_root / "latest_good_mgc_1m_history.json"
    return _write_result(
        report_json=report_json,
        latest_report_json=latest_report_json,
        store_json=store_json,
        latest_good_history_json=latest_good_history_json,
        verdict=TrackBDataMaintenanceVerdict.BLOCKED_PROVIDER_ERROR,
        now=actual_now,
        maintenance_id=actual_maintenance_id,
        source_id=source_id or "track_b_data_maintenance",
        source_payload_path=None,
        expected_account_id=expected_account_id,
        strategy_id=strategy_id,
        lane_id=lane_id,
        contract_key=contract_key,
        symbol=symbol,
        databento_continuous_symbol=databento_continuous_symbol,
        dataset=dataset,
        timeframe=timeframe,
        max_bars=0,
        export_bars=0,
        min_bars=0,
        max_history_age_seconds=0,
        store_candles=[],
        export_candles=[],
        duplicate_count=0,
        gap_count=0,
        history_freshness_seconds=None,
        requested_history_end=requested_history_end,
        provider_available_end=provider_available_end,
        history_end_used=history_end_used,
        available_end_lag_seconds=available_end_lag_seconds,
        history_provider_mode=history_provider_mode or "NOT_PROVIDED",
        latest_good_history=None,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
    )


def _raw_bars(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, Mapping)):
        raw = payload
    elif isinstance(payload, Mapping):
        raw = payload.get("candles") or payload.get("candle_history") or payload.get("bars") or payload.get("ohlcv") or payload.get("records")
        if raw is None:
            raw = [payload] if payload.get("close") is not None or payload.get("last") is not None else []
    else:
        raise ValueError("incoming_history_payload must be a JSON object or list of bar objects.")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("candles/candle_history/bars/ohlcv/records must be a list of objects.")
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"bars[{index}] must be an object.")
    return raw


def _history_provider_mode(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> str:
    if isinstance(payload, Mapping):
        return _optional_text(payload.get("history_provider_mode")) or "DATABENTO_MAINTAINED_LOCAL_1M"
    return "DATABENTO_MAINTAINED_LOCAL_1M"


def _optional_timestamp_from_payload(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]], key: str) -> datetime | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get(key)
    if value in {None, ""}:
        return None
    return _parse_timestamp(str(value))


def _optional_int_from_payload(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]], key: str) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get(key)
    if value in {None, ""}:
        return None
    return int(value)


def _normalize_bar(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    close = _decimal(_first_value(item, "close", "last", "price"), f"bars[{index}].close")
    open_price = _decimal(_first_value(item, "open", "open_price", default=close), f"bars[{index}].open")
    high = _decimal(_first_value(item, "high", "high_price", default=close), f"bars[{index}].high")
    low = _decimal(_first_value(item, "low", "low_price", default=close), f"bars[{index}].low")
    timestamp = _required_text(_first_value(item, "candle_timestamp", "timestamp", "ts_event", "bar_timestamp", "observed_at"), f"bars[{index}].timestamp")
    parsed_timestamp = _parse_timestamp(timestamp)
    volume_value = _first_value(item, "volume", "size", default=None)
    return {
        "candle_timestamp": parsed_timestamp.isoformat(),
        "timestamp": parsed_timestamp.isoformat(),
        "observed_at": _optional_text(item.get("observed_at")) or parsed_timestamp.isoformat(),
        "open": _decimal_text(open_price),
        "high": _decimal_text(high),
        "low": _decimal_text(low),
        "close": _decimal_text(close),
        "volume": None if volume_value in {None, ""} else _decimal_text(_decimal(volume_value, f"bars[{index}].volume")),
        "provider_symbol": item.get("provider_symbol") or item.get("symbol") or item.get("raw_symbol"),
        "record_type": item.get("record_type") or "ohlcv-1m",
    }


def _store_bars(store_json: Path) -> list[dict[str, Any]]:
    if not store_json.exists():
        return []
    value = json.loads(store_json.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError("rolling store JSON must contain an object.")
    return [_normalize_bar(item, index=index) for index, item in enumerate(_raw_bars(value), start=1)]


def _merge_bars(bars: Sequence[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    keyed: dict[str, dict[str, Any]] = {}
    duplicate_count = 0
    for index, item in enumerate(bars, start=1):
        normalized = _normalize_bar(item, index=index)
        key = str(normalized["candle_timestamp"])
        if key in keyed:
            duplicate_count += 1
        keyed[key] = normalized
    ordered_keys = sorted(keyed, key=lambda value: _parse_timestamp(value))
    return [keyed[key] for key in ordered_keys], duplicate_count


def _gap_count(candles: Sequence[Mapping[str, Any]]) -> int:
    if len(candles) < 2:
        return 0
    gaps = 0
    previous = _parse_timestamp(str(candles[0]["candle_timestamp"]))
    for item in candles[1:]:
        current = _parse_timestamp(str(item["candle_timestamp"]))
        delta = current - previous
        if delta > timedelta(seconds=60):
            gaps += max(int(delta.total_seconds() // 60) - 1, 1)
        previous = current
    return gaps


def _history_freshness_seconds(candles: Sequence[Mapping[str, Any]], now: datetime) -> int | None:
    if not candles:
        return None
    latest = _parse_timestamp(str(candles[-1]["candle_timestamp"]))
    return max(int((now.astimezone(UTC) - latest).total_seconds()), 0)


def _readiness_blocker(
    *,
    export_candles: Sequence[Mapping[str, Any]],
    min_bars: int,
    gap_count: int,
    history_freshness_seconds: int | None,
    max_history_age_seconds: int,
) -> str | None:
    if len(export_candles) < min_bars:
        return f"Maintained MGC 1m history has {len(export_candles)} bars; requires at least {min_bars}."
    if gap_count > 0:
        return f"Maintained MGC 1m history has {gap_count} detected 1m gaps."
    if history_freshness_seconds is None:
        return "Maintained MGC 1m history has no latest bar timestamp."
    if history_freshness_seconds > max_history_age_seconds:
        return (
            f"Maintained MGC 1m history is stale: {history_freshness_seconds}s old, "
            f"max allowed {max_history_age_seconds}s."
        )
    return None


def _history_export(
    *,
    generated_at: datetime,
    source_id: str,
    maintenance_id: str,
    source_payload_path: Path | None,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    contract_key: str,
    symbol: str,
    databento_continuous_symbol: str,
    dataset: str,
    timeframe: str,
    candles: Sequence[Mapping[str, Any]],
    gap_count: int,
    duplicate_count: int,
    history_freshness_seconds: int | None,
    requested_history_end: datetime | None,
    provider_available_end: datetime | None,
    history_end_used: datetime | None,
    available_end_lag_seconds: int | None,
    history_provider_mode: str,
    history_ready: bool,
    primary_blocker: str | None,
) -> dict[str, Any]:
    latest = candles[-1] if candles else {}
    return {
        "schema_version": "track_b_latest_good_mgc_1m_history_v1",
        "generated_at": generated_at.isoformat(),
        "source_id": source_id,
        "batch_id": f"track_b_data_maintenance_batch_{uuid.uuid4().hex}",
        "track_b_data_maintenance_id": maintenance_id,
        "account_id": expected_account_id,
        "contract_key": contract_key,
        "local_execution_contract_key": contract_key,
        "instrument_family": "MGC",
        "symbol": symbol,
        "allowlisted_local_symbol": symbol,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "candle_timestamp": latest.get("candle_timestamp"),
        "observed_at": latest.get("observed_at"),
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "candles": list(candles),
        "candle_history": list(candles),
        "history_provider_mode": history_provider_mode,
        "source_provider": "DATABENTO",
        "requested_history_end": None if requested_history_end is None else requested_history_end.isoformat(),
        "provider_available_end": None if provider_available_end is None else provider_available_end.isoformat(),
        "history_end_used": None if history_end_used is None else history_end_used.isoformat(),
        "available_end_lag_seconds": available_end_lag_seconds,
        "history_ready": history_ready,
        "history_freshness_seconds": history_freshness_seconds,
        "gap_count": gap_count,
        "duplicate_count": duplicate_count,
        "primary_blocker": primary_blocker,
        "metadata": {
            "track_b_data_maintenance_boundary": "track_b_data_maintenance",
            "track_b_data_maintenance_id": maintenance_id,
            "market_data_role": "MAINTAINED_HISTORY_ONLY",
            "source_provider": "DATABENTO",
            "history_provider_mode": history_provider_mode,
            "source_payload_path": None if source_payload_path is None else str(source_payload_path),
            "realtime_current_quote_is_separate": True,
            "databento_is_execution_authority": False,
            "local_execution_contract_key_remains_authority": True,
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _write_result(
    *,
    report_json: Path,
    latest_report_json: Path,
    store_json: Path,
    latest_good_history_json: Path,
    verdict: TrackBDataMaintenanceVerdict,
    now: datetime,
    maintenance_id: str,
    source_id: str,
    source_payload_path: Path | None,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    contract_key: str,
    symbol: str,
    databento_continuous_symbol: str,
    dataset: str,
    timeframe: str,
    max_bars: int,
    export_bars: int,
    min_bars: int,
    max_history_age_seconds: int,
    store_candles: Sequence[Mapping[str, Any]],
    export_candles: Sequence[Mapping[str, Any]],
    duplicate_count: int,
    gap_count: int,
    history_freshness_seconds: int | None,
    requested_history_end: datetime | None,
    provider_available_end: datetime | None,
    history_end_used: datetime | None,
    available_end_lag_seconds: int | None,
    history_provider_mode: str,
    latest_good_history: dict[str, Any] | None,
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBDataMaintenanceResult:
    history_ready = latest_good_history is not None and latest_good_history.get("history_ready") is True
    first_bar_timestamp = export_candles[0].get("candle_timestamp") if export_candles else None
    last_bar_timestamp = export_candles[-1].get("candle_timestamp") if export_candles else None
    report = {
        "schema_version": "track_b_data_maintenance_report_v1",
        "generated_at": now.isoformat(),
        "track_b_data_maintenance_id": maintenance_id,
        "data_maintenance_verdict": verdict.value,
        "source_id": source_id,
        "source_provider": "DATABENTO",
        "contract_key": contract_key,
        "symbol": symbol,
        "allowlisted_local_symbol": symbol,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "store_path": str(store_json),
        "max_bars": max_bars,
        "export_bars": export_bars,
        "min_bars": min_bars,
        "max_history_age_seconds": max_history_age_seconds,
        "requested_history_end": None if requested_history_end is None else requested_history_end.isoformat(),
        "provider_available_end": None if provider_available_end is None else provider_available_end.isoformat(),
        "history_end_used": None if history_end_used is None else history_end_used.isoformat(),
        "available_end_lag_seconds": available_end_lag_seconds,
        "history_provider_mode": history_provider_mode,
        "bars_available": len(export_candles),
        "store_bars_available": len(store_candles),
        "first_bar_timestamp": first_bar_timestamp,
        "last_bar_timestamp": last_bar_timestamp,
        "gap_count": gap_count,
        "duplicate_count": duplicate_count,
        "latest_good_history_path": str(latest_good_history_json) if latest_good_history is not None else None,
        "history_freshness_seconds": history_freshness_seconds,
        "history_ready": history_ready,
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
    report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json.write_text(payload, encoding="utf-8")
    if store_candles:
        store_payload = {
            "schema_version": "track_b_mgc_1m_rolling_store_v1",
            "generated_at": now.isoformat(),
            "account_id": expected_account_id,
            "strategy_id": strategy_id,
            "lane_id": lane_id,
            "contract_key": contract_key,
            "symbol": symbol,
            "databento_continuous_symbol": databento_continuous_symbol,
            "dataset": dataset,
            "timeframe": timeframe,
            "candles": list(store_candles),
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        store_json.parent.mkdir(parents=True, exist_ok=True)
        store_json.write_text(json.dumps(to_jsonable(store_payload), indent=2, sort_keys=True), encoding="utf-8")
    if latest_good_history is not None:
        latest_good_history_json.parent.mkdir(parents=True, exist_ok=True)
        latest_good_history_json.write_text(json.dumps(to_jsonable(latest_good_history), indent=2, sort_keys=True), encoding="utf-8")
    return TrackBDataMaintenanceResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        latest_good_history_json=latest_good_history_json if latest_good_history is not None else None,
        latest_good_history=latest_good_history,
        store_json=store_json,
    )


def _input_blocker(
    *,
    payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    expected_account_id: str,
    contract_key: str,
) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    account_id = _optional_text(payload.get("account_id") or payload.get("expected_account_id"))
    if account_id and account_id != expected_account_id:
        return f"History payload account_id {account_id} does not match expected_account_id {expected_account_id}."
    payload_contract_key = _optional_text(payload.get("contract_key") or payload.get("local_execution_contract_key"))
    if payload_contract_key and payload_contract_key != contract_key:
        return f"History payload contract_key {payload_contract_key} does not match {contract_key}."
    return None


def _source_id(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> str | None:
    if isinstance(payload, Mapping):
        return _optional_text(payload.get("source_id"))
    return None


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


def _first_value(item: Mapping[str, Any], *keys: str, default: object = None) -> object:
    for key in keys:
        if item.get(key) is not None:
            return item[key]
    return default


def _required_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
