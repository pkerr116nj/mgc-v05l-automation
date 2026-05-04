"""Track B no-submit MGC feature builder.

This boundary enriches explicit MGC quote/candle evidence with the narrow
EMA/VWAP momentum fields consumed by the first Track B MGC rule. It writes
artifacts only. It does not emit signals, call paper proof, connect to broker
paths, or infer execution authority.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .track_b_strategy_rule_runner import DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID


DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_feature_builder")
MGC_CONTRACT_KEY = "MGC-202606"
MGC_INSTRUMENT_FAMILY = "MGC"


class TrackBFeatureBuilderVerdict(str, Enum):
    WROTE_FEATURE_EVENT = "TRACK_B_FEATURE_BUILDER_WROTE_FEATURE_EVENT"
    BLOCKED_INSUFFICIENT_FEATURE_HISTORY = "TRACK_B_FEATURE_BUILDER_BLOCKED_INSUFFICIENT_FEATURE_HISTORY"
    BLOCKED_NON_REALTIME_INPUT = "TRACK_B_FEATURE_BUILDER_BLOCKED_NON_REALTIME_INPUT"
    BLOCKED_INVALID_INPUT = "TRACK_B_FEATURE_BUILDER_BLOCKED_INVALID_INPUT"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_FEATURE_BUILDER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBFeatureBuilderResult:
    verdict: TrackBFeatureBuilderVerdict
    report_json: Path
    report: dict[str, Any]
    feature_event_json: Path | None
    feature_event: dict[str, Any] | None


@dataclass(frozen=True)
class CandlePoint:
    timestamp: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    raw: Mapping[str, Any]


def build_track_b_mgc_feature_event(
    *,
    source_event_payload: Mapping[str, Any],
    source_event_path: Path | None,
    expected_account_id: str | None = None,
    rule_id: str = DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
    output_root: Path = DEFAULT_TRACK_B_FEATURE_BUILDER_OUTPUT_ROOT,
    builder_id: str | None = None,
    source_id: str | None = None,
    min_history_candles: int = 3,
    ema_span: int = 3,
    now: datetime | None = None,
) -> TrackBFeatureBuilderResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_builder_id = builder_id or f"track_b_feature_builder_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_builder_id / "track_b_feature_builder_report.json"
    event_json = Path(output_root) / actual_builder_id / "track_b_feature_event.json"
    actual_source_id = source_id or _optional_text(source_event_payload.get("source_id")) or "track_b_feature_builder"

    try:
        candles = _candle_points(source_event_payload)
        quote_evidence = _quote_evidence(source_event_payload, candles[-1].raw if candles else {})
        validation_blocker = _validation_blocker(
            source_event_payload=source_event_payload,
            latest_raw=candles[-1].raw if candles else {},
            quote_evidence=quote_evidence,
            expected_account_id=expected_account_id,
        )
        if validation_blocker:
            verdict = (
                TrackBFeatureBuilderVerdict.BLOCKED_NON_REALTIME_INPUT
                if "REALTIME" in validation_blocker or "realtime" in validation_blocker or "current_quote_available" in validation_blocker
                else TrackBFeatureBuilderVerdict.BLOCKED_INVALID_INPUT
            )
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=verdict,
                now=actual_now,
                builder_id=actual_builder_id,
                source_id=actual_source_id,
                source_event_path=source_event_path,
                rule_id=rule_id,
                candle_count=len(candles),
                min_history_candles=min_history_candles,
                ema_span=ema_span,
                quote_evidence=quote_evidence,
                feature_event=None,
                feature_fields={},
                primary_blocker=validation_blocker,
                required_next_action="Provide current realtime MGC quote/candle evidence before building Track B strategy features.",
            )
        if len(candles) < min_history_candles:
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=TrackBFeatureBuilderVerdict.BLOCKED_INSUFFICIENT_FEATURE_HISTORY,
                now=actual_now,
                builder_id=actual_builder_id,
                source_id=actual_source_id,
                source_event_path=source_event_path,
                rule_id=rule_id,
                candle_count=len(candles),
                min_history_candles=min_history_candles,
                ema_span=ema_span,
                quote_evidence=quote_evidence,
                feature_event=None,
                feature_fields={},
                primary_blocker=f"At least {min_history_candles} candles are required; received {len(candles)}.",
                required_next_action="Collect more realtime MGC candle history before evaluating the strategy rule.",
            )
        features = _feature_fields(candles=candles, ema_span=ema_span)
        feature_event = _feature_event(
            source_event_payload=source_event_payload,
            latest=candles[-1],
            features=features,
            quote_evidence=quote_evidence,
            rule_id=rule_id,
            builder_id=actual_builder_id,
            source_id=actual_source_id,
            source_event_path=source_event_path,
        )
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT,
            now=actual_now,
            builder_id=actual_builder_id,
            source_id=actual_source_id,
            source_event_path=source_event_path,
            rule_id=rule_id,
            candle_count=len(candles),
            min_history_candles=min_history_candles,
            ema_span=ema_span,
            quote_evidence=quote_evidence,
            feature_event=feature_event,
            feature_fields=features,
            primary_blocker=None,
            required_next_action="Run track_b_strategy_rule_runner_cli on the feature event when explicitly evaluating the no-submit strategy rule.",
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=TrackBFeatureBuilderVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            builder_id=actual_builder_id,
            source_id=actual_source_id,
            source_event_path=source_event_path,
            rule_id=rule_id,
            candle_count=0,
            min_history_candles=min_history_candles,
            ema_span=ema_span,
            quote_evidence={},
            feature_event=None,
            feature_fields={},
            primary_blocker=str(exc),
            required_next_action="Fix Track B feature builder input schema before retrying.",
        )


def _candle_points(payload: Mapping[str, Any]) -> list[CandlePoint]:
    raw_items = payload.get("candle_items") or payload.get("candles") or payload.get("events")
    if raw_items is None:
        raw_items = [payload] if payload.get("close") is not None or payload.get("last") is not None else []
    if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
        raise ValueError("candle_items must be a list of candle/event objects.")
    candles: list[CandlePoint] = []
    for index, item in enumerate(raw_items, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"candle_items[{index}] must be an object.")
        close = _decimal(item.get("close") if item.get("close") is not None else item.get("last"), f"candle_items[{index}].close")
        open_price = _decimal(item.get("open") if item.get("open") is not None else close, f"candle_items[{index}].open")
        high = _decimal(item.get("high") if item.get("high") is not None else close, f"candle_items[{index}].high")
        low = _decimal(item.get("low") if item.get("low") is not None else close, f"candle_items[{index}].low")
        volume = _decimal(item.get("volume") if item.get("volume") not in {None, ""} else "1", f"candle_items[{index}].volume")
        if volume <= 0:
            volume = Decimal("1")
        timestamp = _required_text(item.get("candle_timestamp") or item.get("observed_at") or item.get("timestamp"), f"candle_items[{index}].timestamp")
        candles.append(CandlePoint(timestamp=timestamp, open=open_price, high=high, low=low, close=close, volume=volume, raw=item))
    return candles


def _quote_evidence(root: Mapping[str, Any], latest: Mapping[str, Any]) -> dict[str, Any]:
    raw_root_metadata = root.get("metadata")
    raw_latest_metadata = latest.get("metadata")
    root_metadata = raw_root_metadata if isinstance(raw_root_metadata, Mapping) else {}
    latest_metadata = raw_latest_metadata if isinstance(raw_latest_metadata, Mapping) else {}
    quote_report_path = (
        latest_metadata.get("source_report_path")
        or root_metadata.get("source_report_path")
        or latest.get("source_report_path")
        or root.get("source_report_path")
    )
    quote_report = _read_optional_json(quote_report_path)
    return {
        "quote_provider_mode": _first_text(
            latest_metadata.get("quote_provider_mode"),
            root_metadata.get("quote_provider_mode"),
            latest.get("quote_provider_mode"),
            root.get("quote_provider_mode"),
            None if quote_report is None else quote_report.get("quote_provider_mode"),
            None if quote_report is None else quote_report.get("market_data_mode"),
        ),
        "realtime_quote_received": _first_bool(
            latest_metadata.get("realtime_quote_received"),
            root_metadata.get("realtime_quote_received"),
            latest.get("realtime_quote_received"),
            root.get("realtime_quote_received"),
            None if quote_report is None else quote_report.get("realtime_quote_received"),
        ),
        "current_quote_available": _first_bool(
            latest_metadata.get("current_quote_available"),
            root_metadata.get("current_quote_available"),
            latest.get("current_quote_available"),
            root.get("current_quote_available"),
            None if quote_report is None else quote_report.get("current_quote_available"),
        ),
        "quote_freshness_verdict": _first_text(
            latest_metadata.get("quote_freshness_verdict"),
            root_metadata.get("quote_freshness_verdict"),
            latest.get("quote_freshness_verdict"),
            root.get("quote_freshness_verdict"),
            None if quote_report is None else quote_report.get("quote_freshness_verdict"),
        ),
        "quote_report_path": _optional_text(quote_report_path),
        "quote_timestamp": _first_text(latest.get("candle_timestamp"), latest.get("observed_at"), latest.get("timestamp")),
    }


def _validation_blocker(
    *,
    source_event_payload: Mapping[str, Any],
    latest_raw: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    expected_account_id: str | None,
) -> str | None:
    if not latest_raw:
        return "Feature builder input contains no candle history."
    account_id = _optional_text(latest_raw.get("account_id") or source_event_payload.get("account_id") or source_event_payload.get("expected_account_id"))
    if expected_account_id and account_id and account_id != expected_account_id:
        return f"Feature input account_id {account_id} does not match expected_account_id {expected_account_id}."
    contract_key = _optional_text(
        latest_raw.get("local_execution_contract_key")
        or latest_raw.get("contract_key")
        or source_event_payload.get("local_execution_contract_key")
        or source_event_payload.get("contract_key")
    )
    if contract_key != MGC_CONTRACT_KEY:
        return f"Only {MGC_CONTRACT_KEY} is supported by this Track B feature builder."
    instrument_family = _optional_text(latest_raw.get("instrument_family") or latest_raw.get("symbol") or source_event_payload.get("instrument_family"))
    if instrument_family and instrument_family != MGC_INSTRUMENT_FAMILY:
        return f"Only instrument_family={MGC_INSTRUMENT_FAMILY} is supported by this Track B feature builder."
    if quote_evidence.get("quote_provider_mode") != "REALTIME":
        return "Feature builder input is not explicitly REALTIME."
    if quote_evidence.get("realtime_quote_received") is not True:
        return "Feature builder input realtime_quote_received is not true."
    if quote_evidence.get("current_quote_available") is not True:
        return "Feature builder input current_quote_available is not true."
    return None


def _feature_fields(*, candles: Sequence[CandlePoint], ema_span: int) -> dict[str, Any]:
    if ema_span < 2:
        raise ValueError("ema_span must be at least 2.")
    closes = [item.close for item in candles]
    ema_values = _ema_values(closes, span=ema_span)
    momentum_raw = [close - ema for close, ema in zip(closes, ema_values, strict=True)]
    normalizer = _momentum_normalizer(closes)
    momentum_norm = [value / normalizer for value in momentum_raw]
    vwap = _vwap(candles)
    current = candles[-1]
    prior = candles[-2]
    current_momentum_norm = momentum_norm[-1]
    prior_momentum_norm = momentum_norm[-2]
    momentum_acceleration = current_momentum_norm - prior_momentum_norm
    momentum_turning_positive = prior_momentum_norm <= 0 < current_momentum_norm
    close_reclaimed_vwap = current.close >= vwap
    prior_close_below_vwap = prior.close < vwap
    return {
        "close": _decimal_text(current.close),
        "vwap": _decimal_text(vwap),
        "reference_vwap": _decimal_text(vwap),
        "prior_close": _decimal_text(prior.close),
        "previous_close": _decimal_text(prior.close),
        "ema": _decimal_text(ema_values[-1]),
        "prior_ema": _decimal_text(ema_values[-2]),
        "ema_span": ema_span,
        "momentum_raw": _decimal_text(momentum_raw[-1]),
        "prior_momentum_raw": _decimal_text(momentum_raw[-2]),
        "momentum_norm": _decimal_text(current_momentum_norm),
        "prior_momentum_norm": _decimal_text(prior_momentum_norm),
        "momentum_acceleration": _decimal_text(momentum_acceleration),
        "momentum_turning_positive": momentum_turning_positive,
        "close_reclaimed_vwap": close_reclaimed_vwap,
        "prior_close_below_vwap": prior_close_below_vwap,
        "history_candle_count": len(candles),
        "feature_timestamp": current.timestamp,
        "vwap_volume": _decimal_text(sum((item.volume for item in candles), Decimal("0"))),
        "momentum_normalizer": _decimal_text(normalizer),
    }


def _feature_event(
    *,
    source_event_payload: Mapping[str, Any],
    latest: CandlePoint,
    features: Mapping[str, Any],
    quote_evidence: Mapping[str, Any],
    rule_id: str,
    builder_id: str,
    source_id: str,
    source_event_path: Path | None,
) -> dict[str, Any]:
    raw_root_metadata = source_event_payload.get("metadata")
    raw_latest_metadata = latest.raw.get("metadata")
    root_metadata = raw_root_metadata if isinstance(raw_root_metadata, Mapping) else {}
    latest_metadata = raw_latest_metadata if isinstance(raw_latest_metadata, Mapping) else {}
    metadata = dict(root_metadata)
    metadata.update(latest_metadata)
    metadata.update(
        {
            "track_b_feature_builder_boundary": "track_b_feature_builder",
            "track_b_feature_builder_id": builder_id,
            "strategy_rule_id": rule_id,
            "feature_role": "EVIDENCE_ONLY",
            "source_event_path": None if source_event_path is None else str(source_event_path),
            "quote_provider_mode": quote_evidence.get("quote_provider_mode"),
            "realtime_quote_received": quote_evidence.get("realtime_quote_received"),
            "current_quote_available": quote_evidence.get("current_quote_available"),
            "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict"),
            "source_report_path": quote_evidence.get("quote_report_path") or metadata.get("source_report_path"),
            "ema_momentum_features": dict(features),
        }
    )
    return {
        "schema_version": "track_b_feature_event_v1",
        "source_id": source_id,
        "batch_id": _optional_text(source_event_payload.get("batch_id")) or f"track_b_feature_batch_{uuid.uuid4().hex}",
        "account_id": _required_text(latest.raw.get("account_id") or source_event_payload.get("account_id") or source_event_payload.get("expected_account_id"), "account_id"),
        "contract_key": _required_text(latest.raw.get("contract_key") or source_event_payload.get("contract_key"), "contract_key"),
        "instrument_family": _optional_text(latest.raw.get("instrument_family") or source_event_payload.get("instrument_family") or latest.raw.get("symbol")) or MGC_INSTRUMENT_FAMILY,
        "strategy_id": _required_text(latest.raw.get("strategy_id") or source_event_payload.get("strategy_id"), "strategy_id"),
        "lane_id": _required_text(latest.raw.get("lane_id") or source_event_payload.get("lane_id"), "lane_id"),
        "signal_type": "track_b_mgc_feature_event",
        "candle_timestamp": latest.timestamp,
        "observed_at": _optional_text(latest.raw.get("observed_at") or source_event_payload.get("observed_at")) or latest.timestamp,
        "timeframe": latest.raw.get("timeframe") or source_event_payload.get("timeframe"),
        "open": _decimal_text(latest.open),
        "high": _decimal_text(latest.high),
        "low": _decimal_text(latest.low),
        "close": _decimal_text(latest.close),
        "volume": _decimal_text(latest.volume),
        "quote_provider_mode": quote_evidence.get("quote_provider_mode"),
        "realtime_quote_received": quote_evidence.get("realtime_quote_received"),
        "current_quote_available": quote_evidence.get("current_quote_available"),
        "reason": "Track B feature builder produced explicit EMA/VWAP momentum fields for no-submit strategy-rule evaluation.",
        "metadata": metadata,
        "submit_requested": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _write_report(
    *,
    report_json: Path,
    event_json: Path,
    verdict: TrackBFeatureBuilderVerdict,
    now: datetime,
    builder_id: str,
    source_id: str,
    source_event_path: Path | None,
    rule_id: str,
    candle_count: int,
    min_history_candles: int,
    ema_span: int,
    quote_evidence: Mapping[str, Any],
    feature_event: dict[str, Any] | None,
    feature_fields: Mapping[str, Any],
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBFeatureBuilderResult:
    latest_report_json = report_json.parent.parent / "latest_track_b_feature_builder_report.json"
    latest_event_json = report_json.parent.parent / "latest_track_b_feature_event.json"
    signal_ready = verdict == TrackBFeatureBuilderVerdict.WROTE_FEATURE_EVENT and feature_event is not None
    report = {
        "schema_version": "track_b_feature_builder_report_v1",
        "generated_at": now.isoformat(),
        "track_b_feature_builder_id": builder_id,
        "feature_builder_verdict": verdict.value,
        "source_id": source_id,
        "source_quote_candle_event_path": None if source_event_path is None else str(source_event_path),
        "rule_id": rule_id,
        "candle_count": candle_count,
        "min_history_candles": min_history_candles,
        "feature_timestamp": feature_fields.get("feature_timestamp"),
        "quote_provider_mode": quote_evidence.get("quote_provider_mode") or "NOT_PROVIDED",
        "realtime_quote_received": quote_evidence.get("realtime_quote_received") if quote_evidence else False,
        "current_quote_available": quote_evidence.get("current_quote_available") if quote_evidence else False,
        "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict") or "NOT_PROVIDED",
        "quote_report_path": quote_evidence.get("quote_report_path"),
        "ema_fields": {
            "ema": feature_fields.get("ema"),
            "prior_ema": feature_fields.get("prior_ema"),
            "ema_span": ema_span,
        },
        "vwap_fields": {
            "vwap": feature_fields.get("vwap"),
            "reference_vwap": feature_fields.get("reference_vwap"),
            "vwap_volume": feature_fields.get("vwap_volume"),
        },
        "momentum_reclaim_fields": {
            "close": feature_fields.get("close"),
            "prior_close": feature_fields.get("prior_close"),
            "momentum_raw": feature_fields.get("momentum_raw"),
            "prior_momentum_raw": feature_fields.get("prior_momentum_raw"),
            "momentum_norm": feature_fields.get("momentum_norm"),
            "prior_momentum_norm": feature_fields.get("prior_momentum_norm"),
            "momentum_acceleration": feature_fields.get("momentum_acceleration"),
            "momentum_turning_positive": feature_fields.get("momentum_turning_positive"),
            "close_reclaimed_vwap": feature_fields.get("close_reclaimed_vwap"),
            "prior_close_below_vwap": feature_fields.get("prior_close_below_vwap"),
        },
        "signal_ready": signal_ready,
        "output_feature_event_path": str(event_json) if signal_ready else None,
        "latest_feature_event_path": str(latest_event_json) if signal_ready else None,
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
    if feature_event is not None:
        event_payload = json.dumps(to_jsonable(feature_event), indent=2, sort_keys=True)
        event_json.write_text(event_payload, encoding="utf-8")
        latest_event_json.write_text(event_payload, encoding="utf-8")
    return TrackBFeatureBuilderResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        feature_event_json=event_json if feature_event is not None else None,
        feature_event=feature_event,
    )


def _ema_values(values: Sequence[Decimal], *, span: int) -> list[Decimal]:
    alpha = Decimal("2") / Decimal(span + 1)
    ema = values[0]
    output = [ema]
    for value in values[1:]:
        ema = (value * alpha) + (ema * (Decimal("1") - alpha))
        output.append(ema)
    return output


def _vwap(candles: Sequence[CandlePoint]) -> Decimal:
    notional = Decimal("0")
    volume = Decimal("0")
    for candle in candles:
        typical = (candle.high + candle.low + candle.close) / Decimal("3")
        notional += typical * candle.volume
        volume += candle.volume
    if volume <= 0:
        raise ValueError("VWAP volume must be positive.")
    return notional / volume


def _momentum_normalizer(closes: Sequence[Decimal]) -> Decimal:
    diffs = [abs(current - prior) for prior, current in zip(closes, closes[1:], strict=False)]
    nonzero = [value for value in diffs if value > 0]
    if not nonzero:
        return Decimal("1")
    return sum(nonzero, Decimal("0")) / Decimal(len(nonzero))


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
