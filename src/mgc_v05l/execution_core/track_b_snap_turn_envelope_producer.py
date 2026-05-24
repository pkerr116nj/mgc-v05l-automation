"""Track B snap-turn feature/state envelope producer.

This module is a narrow execution-core boundary for FIRST_BULL_SNAP_TURN_V1
and FIRST_BEAR_SNAP_TURN_V1. It mirrors the stable first snap-turn predicate
fields from the Track A signal modules, but it does not import the broader app,
strategy, research, or broker stacks. The strategy adapters remain envelope-only
consumers; raw candle interpretation lives here and produces explicit artifacts.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .models import require_aware_datetime, to_jsonable
from .track_b_phase1_runtime_candle_adapter import (
    legacy_p0_runtime_candle_path_blocker,
    normalize_phase1_runtime_candle_payload,
)


DEFAULT_TRACK_B_SNAP_TURN_ENVELOPE_OUTPUT_ROOT = Path("outputs/track_b_execution_core/snap_turn_state")

MGC_CONTRACT_KEY = "MGC-202606"
MGC_INSTRUMENT_FAMILY = "MGC"
MGC_LOCAL_SYMBOL = "MGCM6"
MGC_DATASET = "GLBX.MDP3"
MNQ_CONTRACT_KEY = "MNQ-202606"
MNQ_INSTRUMENT_FAMILY = "MNQ"
MNQ_LOCAL_SYMBOL = "MNQM6"
MNQ_DATASET = "GLBX.MDP3"
DEFAULT_EXPECTED_ACCOUNT_ID = "DUM882026"
DEFAULT_CALIBRATION_PROFILE = "probationary_baseline_v1"

FIRST_BULL_SNAP_TURN_STRATEGY_ID = "FIRST_BULL_SNAP_TURN_V1"
FIRST_BEAR_SNAP_TURN_STRATEGY_ID = "FIRST_BEAR_SNAP_TURN_V1"
MNQ_FIRST_BEAR_SNAP_TURN_STRATEGY_ID = "MNQ_FIRST_BEAR_SNAP_TURN_V1"
MNQ_FIRST_BULL_SNAP_TURN_STRATEGY_ID = "MNQ_FIRST_BULL_SNAP_TURN_V1"
FIRST_BULL_SNAP_TURN_FEATURE_VERSION = "first_bull_snap_turn_v1_phase1"
FIRST_BEAR_SNAP_TURN_FEATURE_VERSION = "first_bear_snap_turn_v1_phase1"
MNQ_FIRST_BEAR_SNAP_TURN_FEATURE_VERSION = "mnq_first_bear_snap_turn_v1_phase1"
MNQ_FIRST_BULL_SNAP_TURN_FEATURE_VERSION = "mnq_first_bull_snap_turn_v1_phase1"

NY = ZoneInfo("America/New_York")

ATR_LEN = 14
TURN_FAST_LEN = 3
TURN_SLOW_LEN = 6
TURN_STRETCH_LOOKBACK = 8
MIN_COMPLETED_5M_BARS = 8

MIN_SNAP_DOWN_STRETCH_ATR = Decimal("1.20")
MIN_SNAP_BAR_RANGE_ATR = Decimal("1.00")
MIN_SNAP_BODY_ATR = Decimal("0.45")
MIN_SNAP_CLOSE_LOCATION = Decimal("0.72")
MIN_SNAP_VELOCITY_DELTA_ATR = Decimal("0.18")
SNAP_COOLDOWN_BARS = 5
ASIA_MIN_SNAP_BAR_RANGE_ATR = Decimal("0.80")
ASIA_MIN_SNAP_BODY_ATR = Decimal("0.35")
ASIA_MIN_SNAP_VELOCITY_DELTA_ATR = Decimal("0.12")
USE_ASIA_BULL_SNAP_THRESHOLDS = True
USE_BULL_SNAP_LOCATION_FILTER = True
BULL_SNAP_MAX_CLOSE_VS_SLOW_EMA_ATR = Decimal("0.15")
BULL_SNAP_REQUIRE_CLOSE_BELOW_SLOW_EMA = True

MIN_BEAR_SNAP_UP_STRETCH_ATR = Decimal("1.00")
MIN_BEAR_SNAP_BAR_RANGE_ATR = Decimal("0.90")
MIN_BEAR_SNAP_BODY_ATR = Decimal("0.40")
MAX_BEAR_SNAP_CLOSE_LOCATION = Decimal("0.28")
MIN_BEAR_SNAP_VELOCITY_DELTA_ATR = Decimal("0.16")
BEAR_SNAP_COOLDOWN_BARS = 5
USE_BEAR_SNAP_LOCATION_FILTER = True
BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR = Decimal("0.15")
BEAR_SNAP_REQUIRE_CLOSE_ABOVE_SLOW_EMA = True


class TrackBSnapTurnEnvelopeProducerVerdict(str, Enum):
    WROTE_ENVELOPES = "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_WROTE_ENVELOPES"
    BLOCKED_INVALID_INPUT = "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_BLOCKED_INVALID_INPUT"
    BLOCKED_NO_5M_CANDLES = "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_BLOCKED_NO_5M_CANDLES"
    BLOCKED_INCOMPLETE_5M_CANDLE = "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_BLOCKED_INCOMPLETE_5M_CANDLE"
    BLOCKED_INSUFFICIENT_5M_CANDLES = "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_BLOCKED_INSUFFICIENT_5M_CANDLES"
    BLOCKED_STALE_RUNTIME_CONTEXT = "TRACK_B_SNAP_TURN_ENVELOPE_PRODUCER_BLOCKED_STALE_RUNTIME_CONTEXT"


@dataclass(frozen=True)
class TrackBSnapTurnEnvelopeProducerResult:
    verdict: TrackBSnapTurnEnvelopeProducerVerdict
    report_json: Path
    report: dict[str, Any]
    first_bull_snap_turn_event_json: Path | None
    first_bear_snap_turn_event_json: Path | None
    first_bull_snap_turn_event: dict[str, Any] | None
    first_bear_snap_turn_event: dict[str, Any] | None


@dataclass(frozen=True)
class _RuntimeCandle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal | None
    completed: bool


@dataclass(frozen=True)
class _FeaturePacket:
    atr: Decimal
    bar_range: Decimal
    body_size: Decimal
    turn_ema_fast: Decimal
    turn_ema_slow: Decimal
    velocity: Decimal
    velocity_delta: Decimal
    vwap: Decimal
    downside_stretch: Decimal
    upside_stretch: Decimal
    close_location: Decimal | None


def produce_track_b_snap_turn_envelopes(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None = None,
    expected_account_id: str = DEFAULT_EXPECTED_ACCOUNT_ID,
    source_id: str = "track_b_snap_turn_envelope_producer",
    output_root: Path = DEFAULT_TRACK_B_SNAP_TURN_ENVELOPE_OUTPUT_ROOT,
    min_completed_bars: int = MIN_COMPLETED_5M_BARS,
    prior_bars_since_bull_snap: int | None = None,
    prior_bars_since_bear_snap: int | None = None,
    max_completed_5m_age_seconds: int | None = None,
    allow_legacy_runtime_candles: bool = False,
    now: datetime | None = None,
    producer_id: str | None = None,
) -> TrackBSnapTurnEnvelopeProducerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"track_b_snap_turn_envelope_producer_{uuid.uuid4().hex}"
    output_root = Path(output_root)
    report_json = output_root / actual_producer_id / "snap_turn_envelope_producer_report.json"
    normalized_payload = normalize_phase1_runtime_candle_payload(
        runtime_5m_payload,
        source_path=runtime_5m_payload_path,
    )
    legacy_blocker = None if allow_legacy_runtime_candles else legacy_p0_runtime_candle_path_blocker(runtime_5m_payload_path)

    try:
        candles = _completed_5m_candles(normalized_payload)
        blocker = legacy_blocker or _input_blocker(normalized_payload, candles, min_completed_bars)
        if blocker:
            return _write_blocked_result(
                verdict=_verdict_for_blocker(blocker),
                report_json=report_json,
                output_root=output_root,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                input_payload=normalized_payload,
                input_payload_path=runtime_5m_payload_path,
                candles=candles,
                primary_blocker=blocker,
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            )

        freshness = _runtime_candle_freshness(
            candles=candles,
            now=actual_now,
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )
        if freshness["runtime_candle_context_stale"] is True:
            return _write_blocked_result(
                verdict=TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_STALE_RUNTIME_CONTEXT,
                report_json=report_json,
                output_root=output_root,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                input_payload=normalized_payload,
                input_payload_path=runtime_5m_payload_path,
                candles=candles,
                primary_blocker=(
                    "Track B snap-turn runtime candle context is stale: latest completed 5m candle age "
                    f"{freshness.get('latest_completed_5m_candle_age_seconds')}s exceeds "
                    f"max {freshness.get('max_completed_5m_candle_age_seconds')}s."
                ),
                required_next_action="Refresh bounded Track B runtime 5m candles before producing snap-turn envelopes.",
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            )

        features = _compute_features(candles)
        bull_features = _bull_snap_features(
            candles=candles,
            features=features,
            prior_bars_since_snap=prior_bars_since_bull_snap,
        )
        bear_features = _bear_snap_features(
            candles=candles,
            features=features,
            prior_bars_since_snap=prior_bars_since_bear_snap,
        )
        last = candles[-1]
        derivative_phase = _derivative_phase(last.timestamp)
        session_allowed = _session_allowed(last.timestamp)
        instrument_family = _payload_instrument_family(normalized_payload)

        if instrument_family == MNQ_INSTRUMENT_FAMILY:
            bull_event = _event_envelope(
                runtime_5m_payload=normalized_payload,
                runtime_5m_payload_path=runtime_5m_payload_path,
                expected_account_id=expected_account_id,
                source_id=source_id,
                now=actual_now,
                candle=last,
                strategy_id=MNQ_FIRST_BULL_SNAP_TURN_STRATEGY_ID,
                lane_id="mnq_first_bull_snap_turn",
                signal_side="LONG",
                state_key="mnq_first_bull_snap_turn_state",
                features_key="mnq_first_bull_snap_turn_features",
                feature_version=MNQ_FIRST_BULL_SNAP_TURN_FEATURE_VERSION,
                state={
                    "derivative_phase": derivative_phase,
                    "session_allowed": session_allowed,
                    "prior_bars_since_bull_snap": bull_features.pop("prior_bars_since_bull_snap"),
                    "prior_bars_since_bull_snap_gt_cooldown": bull_features.pop(
                        "prior_bars_since_bull_snap_gt_cooldown"
                    ),
                    "timeframe": "5m",
                },
                features={**bull_features, "feature_version": MNQ_FIRST_BULL_SNAP_TURN_FEATURE_VERSION},
                feature_packet=features,
                input_bar_count=len(candles),
            )
            bear_event = _event_envelope(
                runtime_5m_payload=normalized_payload,
                runtime_5m_payload_path=runtime_5m_payload_path,
                expected_account_id=expected_account_id,
                source_id=source_id,
                now=actual_now,
                candle=last,
                strategy_id=MNQ_FIRST_BEAR_SNAP_TURN_STRATEGY_ID,
                lane_id="mnq_first_bear_snap_turn",
                signal_side="SHORT",
                state_key="mnq_first_bear_snap_turn_state",
                features_key="mnq_first_bear_snap_turn_features",
                feature_version=MNQ_FIRST_BEAR_SNAP_TURN_FEATURE_VERSION,
                state={
                    "derivative_phase": derivative_phase,
                    "session_allowed": session_allowed,
                    "prior_bars_since_bear_snap": bear_features.pop("prior_bars_since_bear_snap"),
                    "prior_bars_since_bear_snap_gt_cooldown": bear_features.pop(
                        "prior_bars_since_bear_snap_gt_cooldown"
                    ),
                    "timeframe": "5m",
                },
                features={**bear_features, "feature_version": MNQ_FIRST_BEAR_SNAP_TURN_FEATURE_VERSION},
                feature_packet=features,
                input_bar_count=len(candles),
            )
            bull_json = output_root / actual_producer_id / "mnq_first_bull_snap_turn_event_envelope.json"
            bear_json = output_root / actual_producer_id / "mnq_first_bear_snap_turn_event_envelope.json"
            latest_bull = output_root / "latest_mnq_first_bull_snap_turn_event_envelope.json"
            latest_bear = output_root / "latest_mnq_first_bear_snap_turn_event_envelope.json"
            _write_json(bull_json, bull_event)
            _write_json(bear_json, bear_event)
            _write_json(latest_bull, bull_event)
            _write_json(latest_bear, bear_event)

            report = _base_report(
                verdict=TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES,
                now=actual_now,
                producer_id=actual_producer_id,
                report_json=report_json,
                source_id=source_id,
                input_payload=normalized_payload,
                input_payload_path=runtime_5m_payload_path,
                candles=candles,
                primary_blocker=None,
                required_next_action="Run the multi-strategy runtime cycle with the produced MNQ snap-turn envelope.",
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            )
            report.update(
                {
                    "mnq_first_bull_snap_turn_event_json": str(bull_json),
                    "mnq_first_bear_snap_turn_event_json": str(bear_json),
                    "latest_mnq_first_bull_snap_turn_event_json": str(latest_bull),
                    "latest_mnq_first_bear_snap_turn_event_json": str(latest_bear),
                    "mnq_first_bull_snap_turn_envelope_ready": True,
                    "mnq_first_bear_snap_turn_envelope_ready": True,
                    "mnq_first_bull_snap_turn": bull_event["metadata"]["mnq_first_bull_snap_turn_features"][
                        "first_bull_snap_turn"
                    ],
                    "mnq_first_bear_snap_turn": bear_event["metadata"]["mnq_first_bear_snap_turn_features"][
                        "first_bear_snap_turn"
                    ],
                    "feature_diagnostics": _feature_diagnostics(features),
                }
            )
            _write_json(report_json, report)
            latest_report = output_root / "latest_snap_turn_envelope_producer_report.json"
            _write_json(latest_report, report)
            return TrackBSnapTurnEnvelopeProducerResult(
                verdict=TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES,
                report_json=report_json,
                report=report,
                first_bull_snap_turn_event_json=latest_bull,
                first_bear_snap_turn_event_json=latest_bear,
                first_bull_snap_turn_event=bull_event,
                first_bear_snap_turn_event=bear_event,
            )

        bull_event = _event_envelope(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candle=last,
            strategy_id=FIRST_BULL_SNAP_TURN_STRATEGY_ID,
            lane_id="mgc_first_bull_snap_turn",
            signal_side="LONG",
            state_key="first_bull_snap_turn_state",
            features_key="first_bull_snap_turn_features",
            feature_version=FIRST_BULL_SNAP_TURN_FEATURE_VERSION,
            state={
                "derivative_phase": derivative_phase,
                "session_allowed": session_allowed,
                "prior_bars_since_bull_snap": bull_features.pop("prior_bars_since_bull_snap"),
                "prior_bars_since_bull_snap_gt_cooldown": bull_features.pop(
                    "prior_bars_since_bull_snap_gt_cooldown"
                ),
                "timeframe": "5m",
            },
            features=bull_features,
            feature_packet=features,
            input_bar_count=len(candles),
        )
        bear_event = _event_envelope(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candle=last,
            strategy_id=FIRST_BEAR_SNAP_TURN_STRATEGY_ID,
            lane_id="mgc_first_bear_snap_turn",
            signal_side="SHORT",
            state_key="first_bear_snap_turn_state",
            features_key="first_bear_snap_turn_features",
            feature_version=FIRST_BEAR_SNAP_TURN_FEATURE_VERSION,
            state={
                "derivative_phase": derivative_phase,
                "session_allowed": session_allowed,
                "prior_bars_since_bear_snap": bear_features.pop("prior_bars_since_bear_snap"),
                "prior_bars_since_bear_snap_gt_cooldown": bear_features.pop(
                    "prior_bars_since_bear_snap_gt_cooldown"
                ),
                "timeframe": "5m",
            },
            features=bear_features,
            feature_packet=features,
            input_bar_count=len(candles),
        )

        bull_json = output_root / actual_producer_id / "first_bull_snap_turn_event_envelope.json"
        bear_json = output_root / actual_producer_id / "first_bear_snap_turn_event_envelope.json"
        _write_json(bull_json, bull_event)
        _write_json(bear_json, bear_event)
        latest_bull = output_root / "latest_first_bull_snap_turn_event_envelope.json"
        latest_bear = output_root / "latest_first_bear_snap_turn_event_envelope.json"
        _write_json(latest_bull, bull_event)
        _write_json(latest_bear, bear_event)

        report = _base_report(
            verdict=TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES,
            now=actual_now,
            producer_id=actual_producer_id,
            report_json=report_json,
            source_id=source_id,
            input_payload=normalized_payload,
            input_payload_path=runtime_5m_payload_path,
            candles=candles,
            primary_blocker=None,
            required_next_action="Run the multi-strategy runtime cycle with the produced snap-turn envelopes.",
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )
        report.update(
            {
                "first_bull_snap_turn_event_json": str(bull_json),
                "first_bear_snap_turn_event_json": str(bear_json),
                "latest_first_bull_snap_turn_event_json": str(latest_bull),
                "latest_first_bear_snap_turn_event_json": str(latest_bear),
                "first_bull_snap_turn_envelope_ready": True,
                "first_bear_snap_turn_envelope_ready": True,
                "first_bull_snap_turn": bull_event["metadata"]["first_bull_snap_turn_features"][
                    "first_bull_snap_turn"
                ],
                "first_bear_snap_turn": bear_event["metadata"]["first_bear_snap_turn_features"][
                    "first_bear_snap_turn"
                ],
                "feature_diagnostics": _feature_diagnostics(features),
            }
        )
        _write_json(report_json, report)
        latest_report = output_root / "latest_snap_turn_envelope_producer_report.json"
        _write_json(latest_report, report)
        return TrackBSnapTurnEnvelopeProducerResult(
            verdict=TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES,
            report_json=report_json,
            report=report,
            first_bull_snap_turn_event_json=latest_bull,
            first_bear_snap_turn_event_json=latest_bear,
            first_bull_snap_turn_event=bull_event,
            first_bear_snap_turn_event=bear_event,
        )
    except Exception as exc:  # noqa: BLE001 - producer failures must become artifacts.
        return _write_blocked_result(
            verdict=TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INVALID_INPUT,
            report_json=report_json,
            output_root=output_root,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id,
            input_payload=runtime_5m_payload,
            input_payload_path=runtime_5m_payload_path,
            candles=[],
            primary_blocker=f"Track B snap-turn envelope producer invalid input: {exc}",
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )


def _input_blocker(payload: Mapping[str, Any], candles: Sequence[_RuntimeCandle], min_completed_bars: int) -> str | None:
    contract_key = _optional_text(payload.get("contract_key"))
    instrument_family = _payload_instrument_family(payload)
    expected_prefix = "MNQ-" if instrument_family == MNQ_INSTRUMENT_FAMILY else "MGC-"
    if contract_key and not contract_key.startswith(expected_prefix):
        return (
            "Track B snap-turn envelope producer received mismatched contract/instrument metadata: "
            f"instrument_family={instrument_family}, contract_key={contract_key}."
        )
    timeframe = _optional_text(payload.get("timeframe"))
    if timeframe and timeframe != "5m":
        return "Track B snap-turn envelope producer requires bounded completed 5m candles."
    raw_candles = payload.get("candles") or payload.get("candle_history")
    if not isinstance(raw_candles, Sequence) or isinstance(raw_candles, (str, bytes)):
        return "Runtime 5m candle payload must include a candles array."
    if not raw_candles:
        return "Runtime 5m candle payload did not include any candles."
    incomplete_count = sum(1 for item in raw_candles if isinstance(item, Mapping) and item.get("completed") is not True)
    if incomplete_count:
        return f"Runtime 5m candle payload included {incomplete_count} incomplete candle(s)."
    if len(candles) < min_completed_bars:
        return f"Track B snap-turn envelope producer requires at least {min_completed_bars} completed 5m candles; observed {len(candles)}."
    if payload.get("realtime_quote_received") is not True:
        return "Snap-turn envelope producer requires realtime_quote_received=true on the runtime context payload."
    if payload.get("current_quote_available") is not True:
        return "Snap-turn envelope producer requires current_quote_available=true on the runtime context payload."
    if _optional_text(payload.get("quote_provider_mode")) != "REALTIME":
        return "Snap-turn envelope producer requires quote_provider_mode=REALTIME."
    return None


def _payload_instrument_family(payload: Mapping[str, Any]) -> str:
    return _optional_text(payload.get("instrument_family") or payload.get("symbol")) or MGC_INSTRUMENT_FAMILY


def _verdict_for_blocker(blocker: str) -> TrackBSnapTurnEnvelopeProducerVerdict:
    if "incomplete" in blocker:
        return TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INCOMPLETE_5M_CANDLE
    if "at least" in blocker:
        return TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INSUFFICIENT_5M_CANDLES
    if "any candles" in blocker or "candles array" in blocker:
        return TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_NO_5M_CANDLES
    return TrackBSnapTurnEnvelopeProducerVerdict.BLOCKED_INVALID_INPUT


def _completed_5m_candles(payload: Mapping[str, Any]) -> list[_RuntimeCandle]:
    raw = payload.get("candles") or payload.get("candle_history") or []
    candles: list[_RuntimeCandle] = []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return candles
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        timeframe = _optional_text(item.get("timeframe") or payload.get("timeframe"))
        if timeframe and timeframe != "5m":
            continue
        completed = item.get("completed")
        if completed is not True:
            continue
        candles.append(
            _RuntimeCandle(
                timestamp=_parse_timestamp(
                    item.get("candle_timestamp")
                    or item.get("timestamp")
                    or item.get("observed_at")
                    or item.get("source_end_timestamp")
                ),
                open=_decimal(item.get("open"), "open"),
                high=_decimal(item.get("high"), "high"),
                low=_decimal(item.get("low"), "low"),
                close=_decimal(item.get("close"), "close"),
                volume=_optional_decimal(item.get("volume")),
                completed=completed is not False,
            )
        )
    return sorted(candles, key=lambda candle: candle.timestamp)


def _compute_features(candles: Sequence[_RuntimeCandle]) -> _FeaturePacket:
    current = candles[-1]
    closes = [bar.close for bar in candles]
    tr_values = _true_range_series(candles)
    atr = _wilders_average(tr_values, ATR_LEN)
    bar_range = current.high - current.low
    body_size = abs(current.close - current.open)
    fast = _exp_average(closes, TURN_FAST_LEN)
    slow = _exp_average(closes, TURN_SLOW_LEN)
    velocity = fast - slow
    previous_velocity = _previous_velocity(closes, TURN_FAST_LEN, TURN_SLOW_LEN)
    velocity_delta = velocity - previous_velocity
    close_location = None if bar_range <= 0 else (current.close - current.low) / bar_range
    return _FeaturePacket(
        atr=atr,
        bar_range=bar_range,
        body_size=body_size,
        turn_ema_fast=fast,
        turn_ema_slow=slow,
        velocity=velocity,
        velocity_delta=velocity_delta,
        vwap=_session_vwap(candles),
        downside_stretch=_downside_stretch(candles, TURN_STRETCH_LOOKBACK, current.close),
        upside_stretch=_upside_stretch(candles, TURN_STRETCH_LOOKBACK, current.close),
        close_location=close_location,
    )


def _bull_snap_features(
    *,
    candles: Sequence[_RuntimeCandle],
    features: _FeaturePacket,
    prior_bars_since_snap: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous_close = candles[-2].close if len(candles) >= 2 else current.close
    is_asia = _session_phase(current.timestamp).startswith("ASIA")
    range_threshold = ASIA_MIN_SNAP_BAR_RANGE_ATR if is_asia and USE_ASIA_BULL_SNAP_THRESHOLDS else MIN_SNAP_BAR_RANGE_ATR
    body_threshold = ASIA_MIN_SNAP_BODY_ATR if is_asia and USE_ASIA_BULL_SNAP_THRESHOLDS else MIN_SNAP_BODY_ATR
    velocity_threshold = (
        ASIA_MIN_SNAP_VELOCITY_DELTA_ATR if is_asia and USE_ASIA_BULL_SNAP_THRESHOLDS else MIN_SNAP_VELOCITY_DELTA_ATR
    )
    prior = prior_bars_since_snap if prior_bars_since_snap is not None else 1000
    cooldown_ok = prior > SNAP_COOLDOWN_BARS
    downside_stretch_ok = features.downside_stretch >= MIN_SNAP_DOWN_STRETCH_ATR * features.atr
    range_ok = features.bar_range >= range_threshold * features.atr
    body_ok = features.body_size >= body_threshold * features.atr
    close_strong = _close_location_above_threshold(current.low, current.close, features.bar_range, MIN_SNAP_CLOSE_LOCATION)
    velocity_ok = features.velocity_delta >= velocity_threshold * features.atr
    reversal_bar = current.close > current.open and range_ok and body_ok and close_strong
    location_ok = (
        True
        if not USE_BULL_SNAP_LOCATION_FILTER
        else (
            current.close <= features.turn_ema_slow + BULL_SNAP_MAX_CLOSE_VS_SLOW_EMA_ATR * features.atr
            and (current.close <= features.turn_ema_slow if BULL_SNAP_REQUIRE_CLOSE_BELOW_SLOW_EMA else True)
        )
    )
    raw = downside_stretch_ok and reversal_bar and velocity_ok and current.close > previous_close
    candidate = _session_allowed(current.timestamp) and raw and location_ok
    first = candidate and cooldown_ok
    return {
        "feature_version": FIRST_BULL_SNAP_TURN_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "bull_snap_downside_stretch_ok": downside_stretch_ok,
        "bull_snap_range_ok": range_ok,
        "bull_snap_body_ok": body_ok,
        "bull_snap_close_strong": close_strong,
        "bull_snap_velocity_ok": velocity_ok,
        "bull_snap_reversal_bar": reversal_bar,
        "bull_snap_location_ok": location_ok,
        "bull_snap_raw": raw,
        "bull_snap_turn_candidate": candidate,
        "first_bull_snap_turn": first,
        "prior_bars_since_bull_snap": prior,
        "prior_bars_since_bull_snap_gt_cooldown": cooldown_ok,
        "bull_snap_range_threshold_atr": range_threshold,
        "bull_snap_body_threshold_atr": body_threshold,
        "bull_snap_velocity_threshold_atr": velocity_threshold,
        "bull_snap_min_downside_stretch_atr": MIN_SNAP_DOWN_STRETCH_ATR,
    }


def _bear_snap_features(
    *,
    candles: Sequence[_RuntimeCandle],
    features: _FeaturePacket,
    prior_bars_since_snap: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous_close = candles[-2].close if len(candles) >= 2 else current.close
    prior = prior_bars_since_snap if prior_bars_since_snap is not None else 1000
    cooldown_ok = prior > BEAR_SNAP_COOLDOWN_BARS
    up_stretch_ok = features.upside_stretch >= MIN_BEAR_SNAP_UP_STRETCH_ATR * features.atr
    range_ok = features.bar_range >= MIN_BEAR_SNAP_BAR_RANGE_ATR * features.atr
    body_ok = features.body_size >= MIN_BEAR_SNAP_BODY_ATR * features.atr
    close_weak = _close_location_below_threshold(current.low, current.close, features.bar_range, MAX_BEAR_SNAP_CLOSE_LOCATION)
    velocity_ok = features.velocity_delta <= -MIN_BEAR_SNAP_VELOCITY_DELTA_ATR * features.atr
    reversal_bar = current.close < current.open and range_ok and body_ok and close_weak
    if not USE_BEAR_SNAP_LOCATION_FILTER:
        location_ok = True
    elif BEAR_SNAP_REQUIRE_CLOSE_ABOVE_SLOW_EMA:
        location_ok = current.close >= features.turn_ema_slow + BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR * features.atr
    elif BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR > 0:
        location_ok = current.close >= features.turn_ema_slow - BEAR_SNAP_MIN_CLOSE_VS_SLOW_EMA_ATR * features.atr
    else:
        location_ok = True
    raw = up_stretch_ok and reversal_bar and velocity_ok and current.close < previous_close
    candidate = _session_allowed(current.timestamp) and raw and location_ok
    first = candidate and cooldown_ok
    return {
        "feature_version": FIRST_BEAR_SNAP_TURN_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "bear_snap_up_stretch_ok": up_stretch_ok,
        "bear_snap_range_ok": range_ok,
        "bear_snap_body_ok": body_ok,
        "bear_snap_close_weak": close_weak,
        "bear_snap_velocity_ok": velocity_ok,
        "bear_snap_reversal_bar": reversal_bar,
        "bear_snap_location_ok": location_ok,
        "bear_snap_raw": raw,
        "bear_snap_turn_candidate": candidate,
        "first_bear_snap_turn": first,
        "prior_bars_since_bear_snap": prior,
        "prior_bars_since_bear_snap_gt_cooldown": cooldown_ok,
        "bear_snap_min_upside_stretch_atr": MIN_BEAR_SNAP_UP_STRETCH_ATR,
        "bear_snap_range_threshold_atr": MIN_BEAR_SNAP_BAR_RANGE_ATR,
        "bear_snap_body_threshold_atr": MIN_BEAR_SNAP_BODY_ATR,
        "bear_snap_velocity_threshold_atr": MIN_BEAR_SNAP_VELOCITY_DELTA_ATR,
    }


def _event_envelope(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candle: _RuntimeCandle,
    strategy_id: str,
    lane_id: str,
    signal_side: str,
    state_key: str,
    features_key: str,
    feature_version: str,
    state: Mapping[str, Any],
    features: Mapping[str, Any],
    feature_packet: _FeaturePacket,
    input_bar_count: int,
) -> dict[str, Any]:
    metadata = dict(runtime_5m_payload.get("metadata") or {}) if isinstance(runtime_5m_payload.get("metadata") or {}, Mapping) else {}
    metadata.update(
        {
            "track_b_snap_turn_envelope_producer_boundary": "track_b_snap_turn_envelope_producer",
            "track_b_strategy_adapter_consumes_envelope_only": True,
            "track_b_no_submit_market_state": True,
            "source_payload_path": None if runtime_5m_payload_path is None else str(runtime_5m_payload_path),
            "source_category": runtime_5m_payload.get("source_category"),
            "input_source_category": runtime_5m_payload.get("input_source_category"),
            "source_authority": runtime_5m_payload.get("source_authority"),
            "source_authority_path": runtime_5m_payload.get("source_authority_path"),
            "latest_bar_timestamp": runtime_5m_payload.get("latest_bar_timestamp"),
            "freshness_status": runtime_5m_payload.get("freshness_status"),
            "phase1_runtime_market_data_authority": runtime_5m_payload.get("phase1_runtime_market_data_authority")
            is True,
            "source_bar_count": input_bar_count,
            "signal_side_if_ready": signal_side,
            "feature_version": feature_version,
            "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
            "feature_diagnostics": _feature_diagnostics(feature_packet),
            state_key: dict(state),
            features_key: dict(features),
        }
    )
    account_id = _optional_text(runtime_5m_payload.get("account_id") or runtime_5m_payload.get("expected_account_id")) or expected_account_id
    return {
        "account_id": account_id,
        "expected_account_id": expected_account_id,
        "contract_key": _optional_text(runtime_5m_payload.get("contract_key")) or MGC_CONTRACT_KEY,
        "local_execution_contract_key": _optional_text(runtime_5m_payload.get("contract_key")) or MGC_CONTRACT_KEY,
        "instrument_family": _optional_text(runtime_5m_payload.get("instrument_family")) or MGC_INSTRUMENT_FAMILY,
        "local_symbol": _optional_text(runtime_5m_payload.get("local_symbol")) or MGC_LOCAL_SYMBOL,
        "dataset": _optional_text(runtime_5m_payload.get("dataset")) or MGC_DATASET,
        "source_category": runtime_5m_payload.get("source_category"),
        "input_source_category": runtime_5m_payload.get("input_source_category"),
        "source_authority": runtime_5m_payload.get("source_authority"),
        "source_authority_path": runtime_5m_payload.get("source_authority_path"),
        "latest_bar_timestamp": runtime_5m_payload.get("latest_bar_timestamp"),
        "freshness_status": runtime_5m_payload.get("freshness_status"),
        "phase1_runtime_market_data_authority": runtime_5m_payload.get("phase1_runtime_market_data_authority")
        is True,
        "strategy_id": strategy_id,
        "signal_family": strategy_id,
        "lane_id": lane_id,
        "rule_mode": strategy_id,
        "source_id": source_id,
        "timeframe": "5m",
        "candle_timestamp": candle.timestamp.isoformat(),
        "observed_at": now.isoformat(),
        "generated_at": now.isoformat(),
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "last": candle.close,
        "volume": candle.volume,
        "quote_provider_mode": _optional_text(runtime_5m_payload.get("quote_provider_mode")) or "REALTIME",
        "input_quote_provider_mode": _optional_text(runtime_5m_payload.get("quote_provider_mode")) or "REALTIME",
        "realtime_quote_received": runtime_5m_payload.get("realtime_quote_received") is True,
        "current_quote_available": runtime_5m_payload.get("current_quote_available") is True,
        "quote_freshness_verdict": runtime_5m_payload.get("quote_freshness_verdict"),
        "feature_version": feature_version,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "metadata": metadata,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }


def _base_report(
    *,
    verdict: TrackBSnapTurnEnvelopeProducerVerdict,
    now: datetime,
    producer_id: str,
    report_json: Path,
    source_id: str,
    input_payload: Mapping[str, Any],
    input_payload_path: Path | None,
    candles: Sequence[_RuntimeCandle],
    primary_blocker: str | None,
    required_next_action: str,
    max_completed_5m_age_seconds: int | None = None,
) -> dict[str, Any]:
    report = {
        "snap_turn_envelope_producer_verdict": verdict.value,
        "producer_id": producer_id,
        "source_id": source_id,
        "generated_at": now.isoformat(),
        "report_json_path": str(report_json),
        "input_runtime_5m_candles_json": None if input_payload_path is None else str(input_payload_path),
        "contract_key": _optional_text(input_payload.get("contract_key")) or MGC_CONTRACT_KEY,
        "instrument_family": _optional_text(input_payload.get("instrument_family")) or MGC_INSTRUMENT_FAMILY,
        "local_symbol": _optional_text(input_payload.get("local_symbol")) or MGC_LOCAL_SYMBOL,
        "dataset": _optional_text(input_payload.get("dataset")) or MGC_DATASET,
        "source_category": input_payload.get("source_category"),
        "input_source_category": input_payload.get("input_source_category"),
        "source_authority": input_payload.get("source_authority"),
        "source_authority_path": input_payload.get("source_authority_path"),
        "latest_bar_timestamp": input_payload.get("latest_bar_timestamp"),
        "freshness_status": input_payload.get("freshness_status"),
        "phase1_runtime_market_data_authority": input_payload.get("phase1_runtime_market_data_authority") is True,
        "timeframe": _optional_text(input_payload.get("timeframe")) or "5m",
        "quote_provider_mode": _optional_text(input_payload.get("quote_provider_mode")),
        "realtime_quote_received": input_payload.get("realtime_quote_received") is True,
        "current_quote_available": input_payload.get("current_quote_available") is True,
        "input_bar_count": len(candles),
        "first_bar_timestamp": None if not candles else candles[0].timestamp.isoformat(),
        "last_bar_timestamp": None if not candles else candles[-1].timestamp.isoformat(),
        "first_bull_snap_turn_envelope_ready": False,
        "first_bear_snap_turn_envelope_ready": False,
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }
    report.update(
        _runtime_candle_freshness(
            candles=candles,
            now=now,
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )
    )
    return report


def _write_blocked_result(
    *,
    verdict: TrackBSnapTurnEnvelopeProducerVerdict,
    report_json: Path,
    output_root: Path,
    now: datetime,
    producer_id: str,
    source_id: str,
    input_payload: Mapping[str, Any],
    input_payload_path: Path | None,
    candles: Sequence[_RuntimeCandle],
    primary_blocker: str,
    required_next_action: str = "Provide bounded completed realtime MGC 5m candles before producing snap-turn envelopes.",
    max_completed_5m_age_seconds: int | None = None,
) -> TrackBSnapTurnEnvelopeProducerResult:
    report = _base_report(
        verdict=verdict,
        now=now,
        producer_id=producer_id,
        report_json=report_json,
        source_id=source_id,
        input_payload=input_payload,
        input_payload_path=input_payload_path,
        candles=candles,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        max_completed_5m_age_seconds=max_completed_5m_age_seconds,
    )
    _write_json(report_json, report)
    _write_json(output_root / "latest_snap_turn_envelope_producer_report.json", report)
    return TrackBSnapTurnEnvelopeProducerResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        first_bull_snap_turn_event_json=None,
        first_bear_snap_turn_event_json=None,
        first_bull_snap_turn_event=None,
        first_bear_snap_turn_event=None,
    )


def _runtime_candle_freshness(
    *,
    candles: Sequence[_RuntimeCandle],
    now: datetime,
    max_completed_5m_age_seconds: int | None,
) -> dict[str, Any]:
    latest_5m_timestamp = candles[-1].timestamp if candles else None
    latest_5m_age = None if latest_5m_timestamp is None else max(0.0, (now - latest_5m_timestamp).total_seconds())
    stale = (
        max_completed_5m_age_seconds is not None
        and (latest_5m_age is None or latest_5m_age > max_completed_5m_age_seconds)
    )
    return {
        "latest_completed_5m_candle_timestamp": None if latest_5m_timestamp is None else latest_5m_timestamp.isoformat(),
        "latest_completed_5m_candle_age_seconds": None if latest_5m_age is None else round(latest_5m_age, 3),
        "latest_completed_5m_candle_age_minutes": None if latest_5m_age is None else round(latest_5m_age / 60.0, 3),
        "max_completed_5m_candle_age_seconds": max_completed_5m_age_seconds,
        "runtime_candle_context_stale": stale,
        "runtime_candle_context_fresh": None if max_completed_5m_age_seconds is None else not stale,
    }


def _feature_diagnostics(features: _FeaturePacket) -> dict[str, Any]:
    return {
        "atr": features.atr,
        "bar_range": features.bar_range,
        "body_size": features.body_size,
        "turn_ema_fast": features.turn_ema_fast,
        "turn_ema_slow": features.turn_ema_slow,
        "velocity": features.velocity,
        "velocity_delta": features.velocity_delta,
        "vwap": features.vwap,
        "downside_stretch": features.downside_stretch,
        "upside_stretch": features.upside_stretch,
        "close_location": features.close_location,
    }


def _session_vwap(candles: Sequence[_RuntimeCandle]) -> Decimal:
    if not candles:
        return Decimal("0")
    latest_session_date = candles[-1].timestamp.astimezone(NY).date()
    total_volume = Decimal("0")
    total_price_volume = Decimal("0")
    for candle in candles:
        if candle.timestamp.astimezone(NY).date() != latest_session_date:
            continue
        volume = candle.volume if candle.volume is not None else Decimal("0")
        typical_price = (candle.high + candle.low + candle.close) / Decimal("3")
        total_volume += volume
        total_price_volume += typical_price * volume
    return candles[-1].close if total_volume == 0 else total_price_volume / total_volume


def _true_range_series(candles: Sequence[_RuntimeCandle]) -> list[Decimal]:
    values: list[Decimal] = []
    previous_close: Decimal | None = None
    for candle in candles:
        if previous_close is None:
            tr = candle.high - candle.low
        else:
            tr = max(candle.high - candle.low, abs(candle.high - previous_close), abs(candle.low - previous_close))
        values.append(tr)
        previous_close = candle.close
    return values


def _wilders_average(values: Sequence[Decimal], length: int) -> Decimal:
    relevant = list(values[-max(length, 1) :])
    if not relevant:
        return Decimal("0")
    average = relevant[0]
    for value in relevant[1:]:
        average = average + (value - average) / Decimal(length)
    return average


def _exp_average(values: Sequence[Decimal], length: int) -> Decimal:
    relevant = list(values[-max(length, 1) :])
    if not relevant:
        return Decimal("0")
    multiplier = Decimal("2") / Decimal(length + 1)
    ema = relevant[0]
    for value in relevant[1:]:
        ema = (value - ema) * multiplier + ema
    return ema


def _previous_velocity(values: Sequence[Decimal], fast_len: int, slow_len: int) -> Decimal:
    if len(values) < 2:
        return _exp_average(values, fast_len) - _exp_average(values, slow_len)
    prior = values[:-1]
    return _exp_average(prior, fast_len) - _exp_average(prior, slow_len)


def _downside_stretch(candles: Sequence[_RuntimeCandle], lookback: int, current_close: Decimal) -> Decimal:
    prior_highs = [bar.high for bar in candles[-(lookback + 1) : -1]]
    return Decimal("0") if not prior_highs else max(prior_highs) - current_close


def _upside_stretch(candles: Sequence[_RuntimeCandle], lookback: int, current_close: Decimal) -> Decimal:
    prior_lows = [bar.low for bar in candles[-(lookback + 1) : -1]]
    return Decimal("0") if not prior_lows else current_close - min(prior_lows)


def _close_location_above_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    return bar_range > 0 and close > low + threshold * bar_range


def _close_location_below_threshold(low: Decimal, close: Decimal, bar_range: Decimal, threshold: Decimal) -> bool:
    return bar_range > 0 and close < low + threshold * bar_range


def _session_allowed(timestamp: datetime) -> bool:
    phase = _session_phase(timestamp)
    return phase.startswith("ASIA") or phase.startswith("LONDON") or phase.startswith("US")


def _derivative_phase(timestamp: datetime) -> str:
    return _session_phase(timestamp)


def _session_phase(timestamp: datetime) -> str:
    local_time = timestamp.astimezone(NY).time()
    if time(18, 0) <= local_time < time(23, 0):
        return "ASIA_EARLY"
    if time(23, 0) <= local_time or local_time < time(3, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(4, 0):
        return "LONDON_OPEN"
    if time(4, 0) <= local_time < time(8, 30):
        return "LONDON_MID"
    if time(8, 30) <= local_time < time(10, 30):
        return "US_CASH_OPEN_IMPULSE"
    if time(10, 30) <= local_time < time(17, 0):
        return "US_OPEN_LATE"
    return "OUT_OF_SCOPE"


def _parse_timestamp(value: Any) -> datetime:
    text = _optional_text(value)
    if text is None:
        raise ValueError("candle timestamp is required")
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _decimal(value: Any, field_name: str) -> Decimal:
    parsed = _optional_decimal(value)
    if parsed is None:
        raise ValueError(f"{field_name} is required")
    return parsed


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
