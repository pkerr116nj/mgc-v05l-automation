"""Track B session-strategy feature/state envelope producer.

This boundary migrates selected single-entry Track A session strategies into
Track B's envelope contract. Raw 5m candle interpretation happens here, and the
strategy adapters remain envelope-only consumers. The module intentionally does
not import Track A strategy/app/research stacks and does not touch broker state.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .models import require_aware_datetime, to_jsonable
from .track_b_phase1_runtime_candle_adapter import (
    legacy_p0_runtime_candle_path_blocker,
    normalize_phase1_runtime_candle_payload,
)
from .track_b_snap_turn_envelope_producer import (
    DEFAULT_CALIBRATION_PROFILE,
    DEFAULT_EXPECTED_ACCOUNT_ID,
    MAX_BEAR_SNAP_CLOSE_LOCATION,
    MGC_CONTRACT_KEY,
    MGC_DATASET,
    MGC_INSTRUMENT_FAMILY,
    MGC_LOCAL_SYMBOL,
    MIN_BEAR_SNAP_BAR_RANGE_ATR,
    MIN_BEAR_SNAP_BODY_ATR,
    MIN_BEAR_SNAP_UP_STRETCH_ATR,
    MIN_SNAP_CLOSE_LOCATION,
    _FeaturePacket,
    _RuntimeCandle,
    _bear_snap_features,
    _bull_snap_features,
    _close_location_above_threshold,
    _close_location_below_threshold,
    _completed_5m_candles,
    _compute_features,
    _feature_diagnostics,
    _optional_text,
    _runtime_candle_freshness,
)


DEFAULT_TRACK_B_SESSION_STRATEGY_ENVELOPE_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/session_strategy_state"
)

LONDON_LATE_PAUSE_RESUME_SHORT_STRATEGY_ID = "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_STRATEGY_ID = "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1"
ASIA_EARLY_PAUSE_RESUME_SHORT_STRATEGY_ID = "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_STRATEGY_ID = (
    "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
)
US_DERIVATIVE_BEAR_TURN_STRATEGY_ID = "US_DERIVATIVE_BEAR_TURN_V1"
MNQ_US_DERIVATIVE_BEAR_TURN_STRATEGY_ID = "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
US_LATE_PAUSE_RESUME_LONG_STRATEGY_ID = "US_LATE_PAUSE_RESUME_LONG_V1"
LONDON_LATE_PAUSE_RESUME_SHORT_FEATURE_VERSION = "london_late_pause_resume_short_v1_phase1"
ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_FEATURE_VERSION = (
    "asia_late_flat_pullback_pause_resume_long_v1_phase1"
)
ASIA_EARLY_PAUSE_RESUME_SHORT_FEATURE_VERSION = "asia_early_pause_resume_short_v1_phase1"
ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURE_VERSION = (
    "asia_early_normal_breakout_retest_hold_long_v1_phase1"
)
US_DERIVATIVE_BEAR_TURN_FEATURE_VERSION = "us_derivative_bear_turn_v1_phase1"
MNQ_US_DERIVATIVE_BEAR_TURN_FEATURE_VERSION = "mnq_us_derivative_bear_turn_v1_phase1"
US_LATE_PAUSE_RESUME_LONG_FEATURE_VERSION = "us_late_pause_resume_long_v1_phase1"

MNQ_CONTRACT_KEY = "MNQ-202606"
MNQ_INSTRUMENT_FAMILY = "MNQ"
MNQ_LOCAL_SYMBOL = "MNQM6"
MNQ_DATASET = "GLBX.MDP3"

NY = ZoneInfo("America/New_York")
MIN_COMPLETED_5M_BARS = 8
RISK_FLOOR = Decimal("0.01")

LONDON_LATE_MIN_NORMALIZED_SLOPE = Decimal("-0.10")
LONDON_LATE_MAX_NORMALIZED_SLOPE = Decimal("0.10")
LONDON_LATE_MIN_NORMALIZED_CURVATURE = Decimal("-0.50")
LONDON_LATE_MAX_NORMALIZED_CURVATURE = Decimal("-0.10")
LONDON_LATE_MAX_RANGE_EXPANSION_RATIO = Decimal("1.25")

ASIA_LATE_PULLBACK_MAX_RANGE_EXPANSION_RATIO = Decimal("0.85")
ASIA_LATE_SIGNAL_MIN_RANGE_EXPANSION_RATIO = Decimal("0.85")
ASIA_LATE_SIGNAL_MAX_RANGE_EXPANSION_RATIO = Decimal("1.25")
ASIA_LATE_PULLBACK_CURVATURE_FLAT_THRESHOLD = Decimal("0.15")
ASIA_EARLY_PAUSE_RESUME_SHORT_MAX_NORMALIZED_CURVATURE = Decimal("-0.15")
ASIA_EARLY_PAUSE_RESUME_SHORT_SETUP_CURVATURE_FLAT_THRESHOLD = Decimal("0.15")
ASIA_EARLY_PAUSE_RESUME_SHORT_MAX_RANGE_EXPANSION_RATIO = Decimal("1.25")
ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_ABS_SLOPE_MAX = Decimal("0.20")
ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_MIN_RANGE_EXPANSION_RATIO = Decimal("0.85")
ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_MAX_RANGE_EXPANSION_RATIO = Decimal("1.25")
ANTI_CHURN_BARS = 5

US_DERIVATIVE_BEAR_MIN_NORMALIZED_SLOPE = Decimal("-0.80")
US_DERIVATIVE_BEAR_MAX_NORMALIZED_SLOPE = Decimal("-0.15")
US_DERIVATIVE_BEAR_MAX_NORMALIZED_CURVATURE = Decimal("-0.35")
US_DERIVATIVE_BEAR_MIN_BAR_RANGE_ATR = Decimal("1.00")
US_DERIVATIVE_BEAR_MIN_BODY_ATR = Decimal("0.45")
US_DERIVATIVE_BEAR_MAX_CLOSE_LOCATION = Decimal("0.28")
US_DERIVATIVE_BEAR_MIN_UP_STRETCH_ATR = Decimal("1.00")
US_DERIVATIVE_BEAR_MAX_DISTANCE_BELOW_VWAP_ATR = Decimal("1.80")
US_DERIVATIVE_BEAR_OPEN_LATE_MIN_DISTANCE_BELOW_VWAP_ATR = Decimal("0.00")
US_DERIVATIVE_BEAR_OPEN_LATE_MIN_BODY_ATR = Decimal("0.00")
US_DERIVATIVE_BEAR_OPEN_LATE_MAX_CLOSE_LOCATION = Decimal("1.00")
US_DERIVATIVE_BEAR_OPEN_LATE_MAX_DISTANCE_BELOW_FAST_EMA_ATR = Decimal("999")
US_DERIVATIVE_BEAR_COOLDOWN_BARS = 20

US_LATE_PAUSE_RESUME_LONG_SETUP_CURVATURE_MIN = Decimal("0.15")
US_LATE_PAUSE_RESUME_LONG_MAX_RANGE_EXPANSION_RATIO = Decimal("1.25")


class TrackBSessionStrategyEnvelopeProducerVerdict(str, Enum):
    WROTE_ENVELOPES = "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_WROTE_ENVELOPES"
    BLOCKED_INVALID_INPUT = "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_BLOCKED_INVALID_INPUT"
    BLOCKED_NO_5M_CANDLES = "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_BLOCKED_NO_5M_CANDLES"
    BLOCKED_INCOMPLETE_5M_CANDLE = "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_BLOCKED_INCOMPLETE_5M_CANDLE"
    BLOCKED_INSUFFICIENT_5M_CANDLES = "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_BLOCKED_INSUFFICIENT_5M_CANDLES"
    BLOCKED_STALE_RUNTIME_CONTEXT = "TRACK_B_SESSION_STRATEGY_ENVELOPE_PRODUCER_BLOCKED_STALE_RUNTIME_CONTEXT"


@dataclass(frozen=True)
class TrackBSessionStrategyEnvelopeProducerResult:
    verdict: TrackBSessionStrategyEnvelopeProducerVerdict
    report_json: Path
    report: dict[str, Any]
    london_late_pause_resume_short_event_json: Path | None
    asia_late_flat_pullback_pause_resume_long_event_json: Path | None
    asia_early_pause_resume_short_event_json: Path | None
    asia_early_normal_breakout_retest_hold_long_event_json: Path | None
    us_derivative_bear_turn_event_json: Path | None
    mnq_us_derivative_bear_turn_event_json: Path | None
    us_late_pause_resume_long_event_json: Path | None
    london_late_pause_resume_short_event: dict[str, Any] | None
    asia_late_flat_pullback_pause_resume_long_event: dict[str, Any] | None
    asia_early_pause_resume_short_event: dict[str, Any] | None
    asia_early_normal_breakout_retest_hold_long_event: dict[str, Any] | None
    us_derivative_bear_turn_event: dict[str, Any] | None
    mnq_us_derivative_bear_turn_event: dict[str, Any] | None
    us_late_pause_resume_long_event: dict[str, Any] | None


def produce_track_b_session_strategy_envelopes(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None = None,
    expected_account_id: str = DEFAULT_EXPECTED_ACCOUNT_ID,
    source_id: str = "track_b_session_strategy_envelope_producer",
    output_root: Path = DEFAULT_TRACK_B_SESSION_STRATEGY_ENVELOPE_OUTPUT_ROOT,
    min_completed_bars: int = MIN_COMPLETED_5M_BARS,
    prior_bars_since_long_setup: int | None = None,
    prior_bars_since_short_setup: int | None = None,
    prior_bars_since_bull_snap: int | None = None,
    prior_bars_since_bear_snap: int | None = None,
    max_completed_5m_age_seconds: int | None = None,
    allow_legacy_runtime_candles: bool = False,
    now: datetime | None = None,
    producer_id: str | None = None,
) -> TrackBSessionStrategyEnvelopeProducerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"track_b_session_strategy_envelope_producer_{uuid.uuid4().hex}"
    output_root = Path(output_root)
    report_json = output_root / actual_producer_id / "session_strategy_envelope_producer_report.json"
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
                verdict=TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_STALE_RUNTIME_CONTEXT,
                report_json=report_json,
                output_root=output_root,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                input_payload=normalized_payload,
                input_payload_path=runtime_5m_payload_path,
                candles=candles,
                primary_blocker=(
                    "Track B session-strategy runtime candle context is stale: latest completed 5m candle age "
                    f"{freshness.get('latest_completed_5m_candle_age_seconds')}s exceeds "
                    f"max {freshness.get('max_completed_5m_candle_age_seconds')}s."
                ),
                required_next_action="Refresh bounded Track B runtime 5m candles before producing session-strategy envelopes.",
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            )

        feature_history = [_compute_features(candles[: index + 1]) for index in range(len(candles))]
        current_features = feature_history[-1]
        bull_snap = _bull_snap_features(
            candles=candles,
            features=current_features,
            prior_bars_since_snap=prior_bars_since_bull_snap,
        )
        bear_snap = _bear_snap_features(
            candles=candles,
            features=current_features,
            prior_bars_since_snap=prior_bars_since_bear_snap,
        )
        instrument_family = _payload_instrument_family(normalized_payload)
        if instrument_family == MNQ_INSTRUMENT_FAMILY:
            mnq_derivative_bear_event = _mnq_us_derivative_bear_turn_event(
                runtime_5m_payload=normalized_payload,
                runtime_5m_payload_path=runtime_5m_payload_path,
                expected_account_id=expected_account_id,
                source_id=source_id,
                now=actual_now,
                candles=candles,
                feature_history=feature_history,
                prior_bars_since_short_setup=prior_bars_since_short_setup,
            )
            mnq_derivative_bear_json = (
                output_root / actual_producer_id / "mnq_us_derivative_bear_turn_event_envelope.json"
            )
            latest_mnq_derivative_bear = output_root / "latest_mnq_us_derivative_bear_turn_event_envelope.json"
            _write_json(mnq_derivative_bear_json, mnq_derivative_bear_event)
            _write_json(latest_mnq_derivative_bear, mnq_derivative_bear_event)

            report = _base_report(
                verdict=TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES,
                now=actual_now,
                producer_id=actual_producer_id,
                report_json=report_json,
                source_id=source_id,
                input_payload=normalized_payload,
                input_payload_path=runtime_5m_payload_path,
                candles=candles,
                primary_blocker=None,
                required_next_action="Run the multi-strategy runtime cycle with the produced MNQ session strategy envelope.",
                max_completed_5m_age_seconds=max_completed_5m_age_seconds,
            )
            report.update(
                {
                    "mnq_us_derivative_bear_turn_event_json": str(mnq_derivative_bear_json),
                    "latest_mnq_us_derivative_bear_turn_event_json": str(latest_mnq_derivative_bear),
                    "mnq_us_derivative_bear_turn_envelope_ready": True,
                    "feature_diagnostics": _feature_diagnostics(current_features),
                }
            )
            _write_json(report_json, report)
            _write_json(output_root / "latest_session_strategy_envelope_producer_report.json", report)
            return TrackBSessionStrategyEnvelopeProducerResult(
                verdict=TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES,
                report_json=report_json,
                report=report,
                london_late_pause_resume_short_event_json=None,
                asia_late_flat_pullback_pause_resume_long_event_json=None,
                asia_early_pause_resume_short_event_json=None,
                asia_early_normal_breakout_retest_hold_long_event_json=None,
                us_derivative_bear_turn_event_json=None,
                mnq_us_derivative_bear_turn_event_json=latest_mnq_derivative_bear,
                us_late_pause_resume_long_event_json=None,
                london_late_pause_resume_short_event=None,
                asia_late_flat_pullback_pause_resume_long_event=None,
                asia_early_pause_resume_short_event=None,
                asia_early_normal_breakout_retest_hold_long_event=None,
                us_derivative_bear_turn_event=None,
                mnq_us_derivative_bear_turn_event=mnq_derivative_bear_event,
                us_late_pause_resume_long_event=None,
            )

        london_event = _london_late_pause_resume_short_event(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candles=candles,
            feature_history=feature_history,
            bear_snap=dict(bear_snap),
            prior_bars_since_short_setup=prior_bars_since_short_setup,
        )
        asia_event = _asia_late_flat_pullback_pause_resume_long_event(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candles=candles,
            feature_history=feature_history,
            bull_snap=dict(bull_snap),
            prior_bars_since_long_setup=prior_bars_since_long_setup,
        )
        asia_early_short_event = _asia_early_pause_resume_short_event(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candles=candles,
            feature_history=feature_history,
            bear_snap=dict(bear_snap),
            prior_bars_since_short_setup=prior_bars_since_short_setup,
        )
        asia_early_long_event = _asia_early_normal_breakout_retest_hold_long_event(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candles=candles,
            feature_history=feature_history,
            bull_snap=dict(bull_snap),
            prior_bars_since_long_setup=prior_bars_since_long_setup,
        )
        derivative_bear_event = _us_derivative_bear_turn_event(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candles=candles,
            feature_history=feature_history,
            prior_bars_since_short_setup=prior_bars_since_short_setup,
        )
        us_late_long_event = _us_late_pause_resume_long_event(
            runtime_5m_payload=normalized_payload,
            runtime_5m_payload_path=runtime_5m_payload_path,
            expected_account_id=expected_account_id,
            source_id=source_id,
            now=actual_now,
            candles=candles,
            feature_history=feature_history,
            bull_snap=dict(bull_snap),
            prior_bars_since_long_setup=prior_bars_since_long_setup,
        )

        london_json = output_root / actual_producer_id / "london_late_pause_resume_short_event_envelope.json"
        asia_json = output_root / actual_producer_id / "asia_late_flat_pullback_pause_resume_long_event_envelope.json"
        asia_early_short_json = output_root / actual_producer_id / "asia_early_pause_resume_short_event_envelope.json"
        asia_early_long_json = (
            output_root / actual_producer_id / "asia_early_normal_breakout_retest_hold_long_event_envelope.json"
        )
        latest_london = output_root / "latest_london_late_pause_resume_short_event_envelope.json"
        latest_asia = output_root / "latest_asia_late_flat_pullback_pause_resume_long_event_envelope.json"
        latest_asia_early_short = output_root / "latest_asia_early_pause_resume_short_event_envelope.json"
        latest_asia_early_long = (
            output_root / "latest_asia_early_normal_breakout_retest_hold_long_event_envelope.json"
        )
        derivative_bear_json = output_root / actual_producer_id / "us_derivative_bear_turn_event_envelope.json"
        us_late_long_json = output_root / actual_producer_id / "us_late_pause_resume_long_event_envelope.json"
        latest_derivative_bear = output_root / "latest_us_derivative_bear_turn_event_envelope.json"
        latest_us_late_long = output_root / "latest_us_late_pause_resume_long_event_envelope.json"
        _write_json(london_json, london_event)
        _write_json(asia_json, asia_event)
        _write_json(asia_early_short_json, asia_early_short_event)
        _write_json(asia_early_long_json, asia_early_long_event)
        _write_json(derivative_bear_json, derivative_bear_event)
        _write_json(us_late_long_json, us_late_long_event)
        _write_json(latest_london, london_event)
        _write_json(latest_asia, asia_event)
        _write_json(latest_asia_early_short, asia_early_short_event)
        _write_json(latest_asia_early_long, asia_early_long_event)
        _write_json(latest_derivative_bear, derivative_bear_event)
        _write_json(latest_us_late_long, us_late_long_event)

        report = _base_report(
            verdict=TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES,
            now=actual_now,
            producer_id=actual_producer_id,
            report_json=report_json,
            source_id=source_id,
            input_payload=normalized_payload,
            input_payload_path=runtime_5m_payload_path,
            candles=candles,
            primary_blocker=None,
            required_next_action="Run the multi-strategy runtime cycle with the produced session strategy envelopes.",
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )
        report.update(
            {
                "london_late_pause_resume_short_event_json": str(london_json),
                "asia_late_flat_pullback_pause_resume_long_event_json": str(asia_json),
                "asia_early_pause_resume_short_event_json": str(asia_early_short_json),
                "asia_early_normal_breakout_retest_hold_long_event_json": str(asia_early_long_json),
                "us_derivative_bear_turn_event_json": str(derivative_bear_json),
                "us_late_pause_resume_long_event_json": str(us_late_long_json),
                "latest_london_late_pause_resume_short_event_json": str(latest_london),
                "latest_asia_late_flat_pullback_pause_resume_long_event_json": str(latest_asia),
                "latest_asia_early_pause_resume_short_event_json": str(latest_asia_early_short),
                "latest_asia_early_normal_breakout_retest_hold_long_event_json": str(latest_asia_early_long),
                "latest_us_derivative_bear_turn_event_json": str(latest_derivative_bear),
                "latest_us_late_pause_resume_long_event_json": str(latest_us_late_long),
                "london_late_pause_resume_short_envelope_ready": True,
                "asia_late_flat_pullback_pause_resume_long_envelope_ready": True,
                "asia_early_pause_resume_short_envelope_ready": True,
                "asia_early_normal_breakout_retest_hold_long_envelope_ready": True,
                "us_derivative_bear_turn_envelope_ready": True,
                "us_late_pause_resume_long_envelope_ready": True,
                "feature_diagnostics": _feature_diagnostics(current_features),
            }
        )
        _write_json(report_json, report)
        _write_json(output_root / "latest_session_strategy_envelope_producer_report.json", report)
        return TrackBSessionStrategyEnvelopeProducerResult(
            verdict=TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES,
            report_json=report_json,
            report=report,
            london_late_pause_resume_short_event_json=latest_london,
            asia_late_flat_pullback_pause_resume_long_event_json=latest_asia,
            asia_early_pause_resume_short_event_json=latest_asia_early_short,
            asia_early_normal_breakout_retest_hold_long_event_json=latest_asia_early_long,
            us_derivative_bear_turn_event_json=latest_derivative_bear,
            mnq_us_derivative_bear_turn_event_json=None,
            us_late_pause_resume_long_event_json=latest_us_late_long,
            london_late_pause_resume_short_event=london_event,
            asia_late_flat_pullback_pause_resume_long_event=asia_event,
            asia_early_pause_resume_short_event=asia_early_short_event,
            asia_early_normal_breakout_retest_hold_long_event=asia_early_long_event,
            us_derivative_bear_turn_event=derivative_bear_event,
            mnq_us_derivative_bear_turn_event=None,
            us_late_pause_resume_long_event=us_late_long_event,
        )
    except Exception as exc:  # noqa: BLE001 - producer failures must become artifacts.
        return _write_blocked_result(
            verdict=TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INVALID_INPUT,
            report_json=report_json,
            output_root=output_root,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id,
            input_payload=normalized_payload,
            input_payload_path=runtime_5m_payload_path,
            candles=[],
            primary_blocker=f"Track B session-strategy envelope producer invalid input: {exc}",
            max_completed_5m_age_seconds=max_completed_5m_age_seconds,
        )


def _london_late_pause_resume_short_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    bear_snap: Mapping[str, Any],
    prior_bars_since_short_setup: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous = candles[-2]
    features = feature_history[-1]
    normalized_slope = _normalized(features.velocity, features.atr)
    normalized_curvature = _normalized(features.velocity_delta, features.atr)
    recent = _bear_recent_context(candles, feature_history)
    prior_short = prior_bars_since_short_setup if prior_bars_since_short_setup is not None else 1000
    state = {
        "derivative_phase": _research_session_phase(current.timestamp),
        "session_london": _research_session_phase(current.timestamp).startswith("LONDON"),
        "allow_london": True,
        "no_first_bear_snap_turn": bear_snap.get("first_bear_snap_turn") is not True,
        "timeframe": "5m",
    }
    features_payload = {
        "feature_version": LONDON_LATE_PAUSE_RESUME_SHORT_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "close": current.close,
        "open": current.open,
        "previous_close": previous.close,
        "normalized_slope": normalized_slope,
        "min_normalized_slope": LONDON_LATE_MIN_NORMALIZED_SLOPE,
        "max_normalized_slope": LONDON_LATE_MAX_NORMALIZED_SLOPE,
        "normalized_curvature": normalized_curvature,
        "min_normalized_curvature": LONDON_LATE_MIN_NORMALIZED_CURVATURE,
        "max_normalized_curvature": LONDON_LATE_MAX_NORMALIZED_CURVATURE,
        "signal_range_expansion_ratio": recent["signal_range_expansion_ratio"],
        "max_range_expansion_ratio": LONDON_LATE_MAX_RANGE_EXPANSION_RATIO,
        "derivative_bear_close_weak": _close_location_below_threshold(
            current.low, current.close, features.bar_range, MAX_BEAR_SNAP_CLOSE_LOCATION
        ),
        "derivative_bear_range_ok": features.bar_range >= MIN_BEAR_SNAP_BAR_RANGE_ATR * features.atr,
        "derivative_bear_body_ok": features.body_size >= MIN_BEAR_SNAP_BODY_ATR * features.atr,
        "derivative_bear_stretch_ok": features.upside_stretch >= MIN_BEAR_SNAP_UP_STRETCH_ATR * features.atr,
        "slow_ema_ok": current.close >= features.turn_ema_slow,
        "one_bar_rebound_before_signal": recent["one_bar_rebound_before_signal"],
        "prior_3_any_positive_curvature": recent["prior_3_any_positive_curvature"],
        "signal_breaks_prior_1_low": recent["signal_breaks_prior_1_low"],
        "derivative_bear_cooldown_ok": prior_short > 5,
        "prior_bars_since_short_setup": prior_short,
        "no_competing_bear_short_candidate": bear_snap.get("first_bear_snap_turn") is not True,
    }
    return _event_envelope(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candle=current,
        strategy_id=LONDON_LATE_PAUSE_RESUME_SHORT_STRATEGY_ID,
        lane_id="mgc_london_late_pause_resume_short",
        signal_side="SHORT",
        state_key="london_late_pause_resume_short_state",
        features_key="london_late_pause_resume_short_features",
        feature_version=LONDON_LATE_PAUSE_RESUME_SHORT_FEATURE_VERSION,
        state=state,
        features=features_payload,
        feature_packet=features,
        input_bar_count=len(candles),
    )


def _asia_late_flat_pullback_pause_resume_long_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    bull_snap: Mapping[str, Any],
    prior_bars_since_long_setup: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous = candles[-2]
    features = feature_history[-1]
    recent = _asia_late_long_recent_context(candles, feature_history)
    prior_long = prior_bars_since_long_setup if prior_bars_since_long_setup is not None else 1000
    state = {
        "derivative_phase": _research_session_phase(current.timestamp),
        "session_asia": _research_session_phase(current.timestamp).startswith("ASIA"),
        "allow_asia": True,
        "no_first_bull_snap_turn": bull_snap.get("first_bull_snap_turn") is not True,
        "timeframe": "5m",
    }
    features_payload = {
        "feature_version": ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "close": current.close,
        "open": current.open,
        "previous_close": previous.close,
        "bull_snap_close_strong": _close_location_above_threshold(
            current.low, current.close, features.bar_range, MIN_SNAP_CLOSE_LOCATION
        ),
        "one_bar_pullback_before_signal": recent["one_bar_pullback_before_signal"],
        "signal_breaks_prior_1_high": recent["signal_breaks_prior_1_high"],
        "pullback_range_expansion_ratio": recent["pullback_range_expansion_ratio"],
        "pullback_max_range_expansion_ratio": ASIA_LATE_PULLBACK_MAX_RANGE_EXPANSION_RATIO,
        "signal_range_expansion_ratio": recent["signal_range_expansion_ratio"],
        "signal_min_range_expansion_ratio": ASIA_LATE_SIGNAL_MIN_RANGE_EXPANSION_RATIO,
        "signal_max_range_expansion_ratio": ASIA_LATE_SIGNAL_MAX_RANGE_EXPANSION_RATIO,
        "pullback_normalized_curvature": recent["pullback_normalized_curvature"],
        "pullback_curvature_flat_threshold": ASIA_LATE_PULLBACK_CURVATURE_FLAT_THRESHOLD,
        "prior_bars_since_long_setup": prior_long,
        "prior_bars_since_long_setup_gt_anti_churn": prior_long > ANTI_CHURN_BARS,
    }
    return _event_envelope(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candle=current,
        strategy_id=ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_STRATEGY_ID,
        lane_id="mgc_asia_late_flat_pullback_pause_resume_long",
        signal_side="LONG",
        state_key="asia_late_flat_pullback_pause_resume_long_state",
        features_key="asia_late_flat_pullback_pause_resume_long_features",
        feature_version=ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_FEATURE_VERSION,
        state=state,
        features=features_payload,
        feature_packet=features,
        input_bar_count=len(candles),
    )


def _asia_early_pause_resume_short_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    bear_snap: Mapping[str, Any],
    prior_bars_since_short_setup: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous = candles[-2]
    features = feature_history[-1]
    recent = _asia_early_short_recent_context(candles, feature_history)
    prior_short = prior_bars_since_short_setup if prior_bars_since_short_setup is not None else 1000
    phase = _research_session_phase(current.timestamp)
    normalized_curvature = _normalized(features.velocity_delta, features.atr)
    state = {
        "derivative_phase": phase,
        "session_asia": phase.startswith("ASIA"),
        "allow_asia": True,
        "timeframe": "5m",
    }
    features_payload = {
        "feature_version": ASIA_EARLY_PAUSE_RESUME_SHORT_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "close": current.close,
        "open": current.open,
        "previous_close": previous.close,
        "normalized_curvature": normalized_curvature,
        "max_normalized_curvature": ASIA_EARLY_PAUSE_RESUME_SHORT_MAX_NORMALIZED_CURVATURE,
        "signal_range_expansion_ratio": recent["signal_range_expansion_ratio"],
        "max_range_expansion_ratio": ASIA_EARLY_PAUSE_RESUME_SHORT_MAX_RANGE_EXPANSION_RATIO,
        "setup_bar_normalized_curvature": recent["setup_bar_normalized_curvature"],
        "setup_curvature_flat_threshold": ASIA_EARLY_PAUSE_RESUME_SHORT_SETUP_CURVATURE_FLAT_THRESHOLD,
        "setup_bar_curvature_is_flat": recent["setup_bar_curvature_is_flat"],
        "one_bar_rebound_before_signal": recent["one_bar_rebound_before_signal"],
        "signal_breaks_prior_1_low": recent["signal_breaks_prior_1_low"],
        "close_below_fast_ema": current.close <= features.turn_ema_fast,
        "derivative_bear_close_weak": _close_location_below_threshold(
            current.low, current.close, features.bar_range, MAX_BEAR_SNAP_CLOSE_LOCATION
        ),
        "derivative_bear_range_ok": features.bar_range >= MIN_BEAR_SNAP_BAR_RANGE_ATR * features.atr,
        "derivative_bear_body_ok": features.body_size >= MIN_BEAR_SNAP_BODY_ATR * features.atr,
        "derivative_bear_stretch_ok": features.upside_stretch >= MIN_BEAR_SNAP_UP_STRETCH_ATR * features.atr,
        "derivative_bear_cooldown_ok": prior_short > ANTI_CHURN_BARS,
        "prior_bars_since_short_setup": prior_short,
        "no_competing_bear_short_candidate": bear_snap.get("first_bear_snap_turn") is not True,
    }
    return _event_envelope(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candle=current,
        strategy_id=ASIA_EARLY_PAUSE_RESUME_SHORT_STRATEGY_ID,
        lane_id="mgc_asia_early_pause_resume_short",
        signal_side="SHORT",
        state_key="asia_early_pause_resume_short_state",
        features_key="asia_early_pause_resume_short_features",
        feature_version=ASIA_EARLY_PAUSE_RESUME_SHORT_FEATURE_VERSION,
        state=state,
        features=features_payload,
        feature_packet=features,
        input_bar_count=len(candles),
    )


def _asia_early_normal_breakout_retest_hold_long_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    bull_snap: Mapping[str, Any],
    prior_bars_since_long_setup: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    features = feature_history[-1]
    breakout = _asia_early_breakout_retest_context(candles, feature_history)
    prior_long = prior_bars_since_long_setup if prior_bars_since_long_setup is not None else 1000
    phase = _research_session_phase(current.timestamp)
    state = {
        "derivative_phase": phase,
        "session_asia": phase.startswith("ASIA"),
        "allow_asia": True,
        "asia_early_or_gc_mgc_london_open": _asia_early_or_gc_mgc_london_open(current.timestamp),
        "no_first_bull_snap_turn": bull_snap.get("first_bull_snap_turn") is not True,
        "prior_bars_since_long_setup": prior_long,
        "prior_bars_since_long_setup_gt_anti_churn": prior_long > ANTI_CHURN_BARS,
        "timeframe": "5m",
    }
    features_payload = {
        "feature_version": ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "close": current.close,
        "open": current.open,
        "previous_close": candles[-2].close,
        "breakout_normalized_slope": breakout["breakout_normalized_slope"],
        "breakout_abs_slope_max": ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_ABS_SLOPE_MAX,
        "breakout_range_expansion_ratio": breakout["breakout_range_expansion_ratio"],
        "breakout_min_range_expansion_ratio": ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_MIN_RANGE_EXPANSION_RATIO,
        "breakout_max_range_expansion_ratio": ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_MAX_RANGE_EXPANSION_RATIO,
        "breakout_level": breakout["breakout_level"],
        "retest_depth_ticks_or_points": breakout["retest_depth_ticks_or_points"],
        "retest_depth_normalized": breakout["retest_depth_normalized"],
        "hold_margin_ticks_or_points": breakout["hold_margin_ticks_or_points"],
        "hold_margin_normalized": breakout["hold_margin_normalized"],
        "bars_since_breakout": breakout["bars_since_breakout"],
        "bars_since_retest": breakout["bars_since_retest"],
        "range_expansion_ratio": breakout["range_expansion_ratio"],
        "close_location": breakout["close_location"],
        "body_to_range_ratio": breakout["body_to_range_ratio"],
        "prior_bars_since_long_setup": prior_long,
        "anti_churn_bars": ANTI_CHURN_BARS,
        "anti_churn_margin_bars": prior_long - ANTI_CHURN_BARS,
        "churn_score": _churn_score(prior_long),
        "snap_turn_conflict_strength": Decimal("0") if bull_snap.get("first_bull_snap_turn") is not True else Decimal("1"),
        "breakout_bar_slope_is_flat": breakout["breakout_bar_slope_is_flat"],
        "breakout_bar_expansion_is_normal": breakout["breakout_bar_expansion_is_normal"],
        "breakout_breaks_prior_1_high": breakout["breakout_breaks_prior_1_high"],
        "signal_retests_and_holds_breakout_level": breakout["signal_retests_and_holds_breakout_level"],
    }
    return _event_envelope(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candle=current,
        strategy_id=ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_STRATEGY_ID,
        lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
        signal_side="LONG",
        state_key="asia_early_normal_breakout_retest_hold_long_state",
        features_key="asia_early_normal_breakout_retest_hold_long_features",
        feature_version=ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_FEATURE_VERSION,
        state=state,
        features=features_payload,
        feature_packet=features,
        input_bar_count=len(candles),
    )


def _us_derivative_bear_turn_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    prior_bars_since_short_setup: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous = candles[-2]
    features = feature_history[-1]
    phase = _research_session_phase(current.timestamp)
    local_time = current.timestamp.astimezone(NY).time()
    prior_short = prior_bars_since_short_setup if prior_bars_since_short_setup is not None else 1000
    normalized_slope = _normalized(features.velocity, features.atr)
    normalized_curvature = _normalized(features.velocity_delta, features.atr)
    close_below_fast_floor = current.close >= features.turn_ema_fast - US_DERIVATIVE_BEAR_OPEN_LATE_MAX_DISTANCE_BELOW_FAST_EMA_ATR * features.atr
    state = {
        "derivative_phase": phase,
        "session_us": phase.startswith("US_"),
        "allow_us": True,
        "derivative_bear_window_ok": time(9, 0) <= local_time < time(10, 30),
        "derivative_bear_phase_ok": phase in {"US_PREOPEN_OPENING", "US_CASH_OPEN_IMPULSE", "US_OPEN_LATE"},
        "timeframe": "5m",
    }
    features_payload = {
        "feature_version": US_DERIVATIVE_BEAR_TURN_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "close": current.close,
        "open": current.open,
        "previous_close": previous.close,
        "vwap": features.vwap,
        "turn_ema_fast": features.turn_ema_fast,
        "turn_ema_slow": features.turn_ema_slow,
        "normalized_slope": normalized_slope,
        "min_normalized_slope": US_DERIVATIVE_BEAR_MIN_NORMALIZED_SLOPE,
        "max_normalized_slope": US_DERIVATIVE_BEAR_MAX_NORMALIZED_SLOPE,
        "normalized_curvature": normalized_curvature,
        "max_normalized_curvature": US_DERIVATIVE_BEAR_MAX_NORMALIZED_CURVATURE,
        "close_below_open": current.close < current.open,
        "close_below_previous_close": current.close < previous.close,
        "derivative_bear_close_weak": _close_location_below_threshold(
            current.low, current.close, features.bar_range, US_DERIVATIVE_BEAR_MAX_CLOSE_LOCATION
        ),
        "derivative_bear_range_ok": features.bar_range >= US_DERIVATIVE_BEAR_MIN_BAR_RANGE_ATR * features.atr,
        "derivative_bear_body_ok": features.body_size >= US_DERIVATIVE_BEAR_MIN_BODY_ATR * features.atr,
        "derivative_bear_stretch_ok": features.upside_stretch >= US_DERIVATIVE_BEAR_MIN_UP_STRETCH_ATR * features.atr,
        "derivative_bear_fast_ema_ok": current.close <= features.turn_ema_fast,
        "derivative_bear_vwap_ok": current.close <= features.vwap,
        "derivative_bear_vwap_extension_ok": (
            current.close >= features.vwap - US_DERIVATIVE_BEAR_MAX_DISTANCE_BELOW_VWAP_ATR * features.atr
        ),
        "derivative_bear_open_late_extension_floor_ok": (
            True
            if phase != "US_OPEN_LATE"
            else current.close <= features.vwap - US_DERIVATIVE_BEAR_OPEN_LATE_MIN_DISTANCE_BELOW_VWAP_ATR * features.atr
        ),
        "derivative_bear_open_late_body_ok": (
            True if phase != "US_OPEN_LATE" else features.body_size >= US_DERIVATIVE_BEAR_OPEN_LATE_MIN_BODY_ATR * features.atr
        ),
        "derivative_bear_open_late_close_ok": (
            True
            if phase != "US_OPEN_LATE"
            else _close_location_below_threshold(
                current.low,
                current.close,
                features.bar_range,
                US_DERIVATIVE_BEAR_OPEN_LATE_MAX_CLOSE_LOCATION,
            )
        ),
        "derivative_bear_open_late_fast_ema_extension_ok": True if phase != "US_OPEN_LATE" else close_below_fast_floor,
        "derivative_bear_slow_ema_ok": True,
        "derivative_bear_structure_ok": True,
        "derivative_bear_cooldown_ok": prior_short > US_DERIVATIVE_BEAR_COOLDOWN_BARS,
        "prior_bars_since_short_setup": prior_short,
    }
    return _event_envelope(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candle=current,
        strategy_id=US_DERIVATIVE_BEAR_TURN_STRATEGY_ID,
        lane_id="mgc_us_derivative_bear_turn",
        signal_side="SHORT",
        state_key="us_derivative_bear_turn_state",
        features_key="us_derivative_bear_turn_features",
        feature_version=US_DERIVATIVE_BEAR_TURN_FEATURE_VERSION,
        state=state,
        features=features_payload,
        feature_packet=features,
        input_bar_count=len(candles),
    )


def _mnq_us_derivative_bear_turn_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    prior_bars_since_short_setup: int | None,
) -> dict[str, Any]:
    event = _us_derivative_bear_turn_event(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candles=candles,
        feature_history=feature_history,
        prior_bars_since_short_setup=prior_bars_since_short_setup,
    )
    metadata = dict(event.get("metadata") or {})
    state = dict(metadata.pop("us_derivative_bear_turn_state", {}) or {})
    features = dict(metadata.pop("us_derivative_bear_turn_features", {}) or {})
    features["feature_version"] = MNQ_US_DERIVATIVE_BEAR_TURN_FEATURE_VERSION
    metadata.update(
        {
            "track_b_mnq_us_derivative_bear_turn_lineage": (
                "Mirrors the explicit usDerivativeBearTurn predicates from the MNQ validation path. "
                "Track B computes a bounded 5m feature/state envelope and adapters remain envelope-only."
            ),
            "mnq_us_derivative_bear_turn_state": state,
            "mnq_us_derivative_bear_turn_features": features,
        }
    )
    event.update(
        {
            "strategy_id": MNQ_US_DERIVATIVE_BEAR_TURN_STRATEGY_ID,
            "signal_family": MNQ_US_DERIVATIVE_BEAR_TURN_STRATEGY_ID,
            "lane_id": "mnq_us_derivative_bear_turn",
            "rule_mode": MNQ_US_DERIVATIVE_BEAR_TURN_STRATEGY_ID,
            "contract_key": _optional_text(runtime_5m_payload.get("contract_key")) or MNQ_CONTRACT_KEY,
            "local_execution_contract_key": _optional_text(runtime_5m_payload.get("contract_key")) or MNQ_CONTRACT_KEY,
            "instrument_family": _optional_text(runtime_5m_payload.get("instrument_family")) or MNQ_INSTRUMENT_FAMILY,
            "local_symbol": _optional_text(runtime_5m_payload.get("local_symbol")) or MNQ_LOCAL_SYMBOL,
            "dataset": _optional_text(runtime_5m_payload.get("dataset")) or MNQ_DATASET,
            "metadata": metadata,
        }
    )
    return event


def _us_late_pause_resume_long_event(
    *,
    runtime_5m_payload: Mapping[str, Any],
    runtime_5m_payload_path: Path | None,
    expected_account_id: str,
    source_id: str,
    now: datetime,
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
    bull_snap: Mapping[str, Any],
    prior_bars_since_long_setup: int | None,
) -> dict[str, Any]:
    current = candles[-1]
    previous = candles[-2]
    features = feature_history[-1]
    recent = _us_late_long_recent_context(candles, feature_history)
    prior_long = prior_bars_since_long_setup if prior_bars_since_long_setup is not None else 1000
    phase = _research_session_phase(current.timestamp)
    local_time = current.timestamp.astimezone(NY).time()
    state = {
        "derivative_phase": phase,
        "session_us_late": phase == "US_LATE",
        "allow_us": True,
        "no_first_bull_snap_turn": bull_snap.get("first_bull_snap_turn") is not True,
        "timeframe": "5m",
    }
    features_payload = {
        "feature_version": US_LATE_PAUSE_RESUME_LONG_FEATURE_VERSION,
        "calibration_profile": DEFAULT_CALIBRATION_PROFILE,
        "close": current.close,
        "open": current.open,
        "previous_close": previous.close,
        "bull_snap_close_strong": _close_location_above_threshold(
            current.low, current.close, features.bar_range, MIN_SNAP_CLOSE_LOCATION
        ),
        "signal_range_expansion_ratio": recent["signal_range_expansion_ratio"],
        "max_range_expansion_ratio": US_LATE_PAUSE_RESUME_LONG_MAX_RANGE_EXPANSION_RATIO,
        "one_bar_pullback_before_signal": recent["one_bar_pullback_before_signal"],
        "signal_breaks_prior_1_high": recent["signal_breaks_prior_1_high"],
        "signal_ema_location_ok": recent["signal_ema_location_ok"],
        "setup_bar_normalized_curvature": recent["setup_bar_normalized_curvature"],
        "setup_curvature_min": US_LATE_PAUSE_RESUME_LONG_SETUP_CURVATURE_MIN,
        "setup_bar_curvature_is_positive": recent["setup_bar_curvature_is_positive"],
        "prior_bars_since_long_setup": prior_long,
        "prior_bars_since_long_setup_gt_anti_churn": prior_long > ANTI_CHURN_BARS,
        "not_1755_carryover": local_time != time(16, 55),
    }
    return _event_envelope(
        runtime_5m_payload=runtime_5m_payload,
        runtime_5m_payload_path=runtime_5m_payload_path,
        expected_account_id=expected_account_id,
        source_id=source_id,
        now=now,
        candle=current,
        strategy_id=US_LATE_PAUSE_RESUME_LONG_STRATEGY_ID,
        lane_id="mgc_us_late_pause_resume_long",
        signal_side="LONG",
        state_key="us_late_pause_resume_long_state",
        features_key="us_late_pause_resume_long_features",
        feature_version=US_LATE_PAUSE_RESUME_LONG_FEATURE_VERSION,
        state=state,
        features=features_payload,
        feature_packet=features,
        input_bar_count=len(candles),
    )


def _bear_recent_context(
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
) -> dict[str, Any]:
    current = candles[-1]
    previous = candles[-2]
    prior_curvatures = [
        _normalized(feature.velocity_delta, feature.atr) for feature in feature_history[max(0, len(feature_history) - 4) : -1]
    ]
    return {
        "prior_3_any_positive_curvature": any(value > 0 for value in prior_curvatures),
        "one_bar_rebound_before_signal": len(candles) >= 3 and candles[-2].close > candles[-3].close,
        "signal_range_expansion_ratio": _range_over_atr(current, feature_history[-1]),
        "signal_breaks_prior_1_low": current.low < previous.low,
    }


def _asia_early_short_recent_context(
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
) -> dict[str, Any]:
    if len(candles) < 3 or len(feature_history) < 3:
        return {
            "setup_bar_normalized_curvature": Decimal("0"),
            "setup_bar_curvature_is_flat": False,
            "one_bar_rebound_before_signal": False,
            "signal_breaks_prior_1_low": False,
            "signal_range_expansion_ratio": Decimal("0"),
        }
    setup_features = feature_history[-3]
    setup_curvature = _normalized(setup_features.velocity_delta, setup_features.atr)
    return {
        "setup_bar_normalized_curvature": setup_curvature,
        "setup_bar_curvature_is_flat": abs(setup_curvature) <= ASIA_EARLY_PAUSE_RESUME_SHORT_SETUP_CURVATURE_FLAT_THRESHOLD,
        "one_bar_rebound_before_signal": candles[-2].close > candles[-3].close,
        "signal_breaks_prior_1_low": candles[-1].low < candles[-2].low,
        "signal_range_expansion_ratio": _range_over_atr(candles[-1], feature_history[-1]),
    }


def _asia_early_breakout_retest_context(
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
) -> dict[str, Any]:
    if len(candles) < 3 or len(feature_history) < 2:
        return {
            "breakout_normalized_slope": Decimal("0"),
            "breakout_range_expansion_ratio": Decimal("0"),
            "breakout_level": None,
            "retest_depth_ticks_or_points": None,
            "retest_depth_normalized": None,
            "hold_margin_ticks_or_points": None,
            "hold_margin_normalized": None,
            "bars_since_breakout": None,
            "bars_since_retest": None,
            "range_expansion_ratio": Decimal("0"),
            "close_location": None,
            "body_to_range_ratio": None,
            "breakout_bar_slope_is_flat": False,
            "breakout_bar_expansion_is_normal": False,
            "breakout_breaks_prior_1_high": False,
            "signal_retests_and_holds_breakout_level": False,
        }
    prior_bar = candles[-3]
    breakout_bar = candles[-2]
    signal_bar = candles[-1]
    breakout_features = feature_history[-2]
    breakout_normalized_slope = _normalized(breakout_features.velocity, breakout_features.atr)
    breakout_range_expansion_ratio = _range_over_atr(breakout_bar, breakout_features)
    breakout_level = breakout_bar.high
    signal_retests = signal_bar.low <= breakout_level
    signal_holds = signal_bar.close >= breakout_level
    retest_depth = max(Decimal("0"), breakout_level - signal_bar.low)
    hold_margin = signal_bar.close - breakout_level
    return {
        "breakout_normalized_slope": breakout_normalized_slope,
        "breakout_range_expansion_ratio": breakout_range_expansion_ratio,
        "breakout_level": breakout_level,
        "retest_depth_ticks_or_points": retest_depth,
        "retest_depth_normalized": _normalized(retest_depth, feature_history[-1].atr),
        "hold_margin_ticks_or_points": hold_margin,
        "hold_margin_normalized": _normalized(hold_margin, feature_history[-1].atr),
        "bars_since_breakout": 1,
        "bars_since_retest": 0 if signal_retests else None,
        "range_expansion_ratio": breakout_range_expansion_ratio,
        "close_location": _bar_close_location(signal_bar),
        "body_to_range_ratio": _bar_body_to_range_ratio(signal_bar),
        "breakout_bar_slope_is_flat": (
            abs(breakout_normalized_slope) <= ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_ABS_SLOPE_MAX
        ),
        "breakout_bar_expansion_is_normal": (
            breakout_range_expansion_ratio > ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_MIN_RANGE_EXPANSION_RATIO
            and breakout_range_expansion_ratio < ASIA_EARLY_BREAKOUT_RETEST_HOLD_BREAKOUT_MAX_RANGE_EXPANSION_RATIO
        ),
        "breakout_breaks_prior_1_high": breakout_bar.high > prior_bar.high and breakout_bar.close >= prior_bar.close,
        "signal_retests_and_holds_breakout_level": signal_retests and signal_holds,
    }


def _asia_late_long_recent_context(
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
) -> dict[str, Any]:
    if len(candles) < 3:
        return {
            "pullback_range_expansion_ratio": Decimal("0"),
            "signal_range_expansion_ratio": Decimal("0"),
            "pullback_normalized_curvature": Decimal("0"),
            "one_bar_pullback_before_signal": False,
            "signal_breaks_prior_1_high": False,
        }
    pullback = candles[-2]
    signal = candles[-1]
    pullback_features = feature_history[-2]
    signal_features = feature_history[-1]
    return {
        "pullback_range_expansion_ratio": _range_over_atr(pullback, pullback_features),
        "signal_range_expansion_ratio": _range_over_atr(signal, signal_features),
        "pullback_normalized_curvature": _normalized(pullback_features.velocity_delta, pullback_features.atr),
        "one_bar_pullback_before_signal": candles[-2].close < candles[-3].close,
        "signal_breaks_prior_1_high": candles[-1].high > candles[-2].high,
    }


def _us_late_long_recent_context(
    candles: Sequence[_RuntimeCandle],
    feature_history: Sequence[_FeaturePacket],
) -> dict[str, Any]:
    if len(candles) < 3 or len(feature_history) < 3:
        return {
            "signal_range_expansion_ratio": Decimal("0"),
            "one_bar_pullback_before_signal": False,
            "signal_breaks_prior_1_high": False,
            "signal_ema_location_ok": False,
            "setup_bar_normalized_curvature": Decimal("0"),
            "setup_bar_curvature_is_positive": False,
        }
    current = candles[-1]
    current_features = feature_history[-1]
    setup_features = feature_history[-3]
    setup_curvature = _normalized(setup_features.velocity_delta, setup_features.atr)
    fast = current_features.turn_ema_fast
    slow = current_features.turn_ema_slow
    return {
        "signal_range_expansion_ratio": _range_over_atr(current, current_features),
        "one_bar_pullback_before_signal": candles[-2].close < candles[-3].close,
        "signal_breaks_prior_1_high": current.high > candles[-2].high,
        "signal_ema_location_ok": (
            (fast < slow and current.close > fast and current.close <= slow)
            or (fast > slow and current.close >= fast and current.close >= slow)
        ),
        "setup_bar_normalized_curvature": setup_curvature,
        "setup_bar_curvature_is_positive": setup_curvature >= US_LATE_PAUSE_RESUME_LONG_SETUP_CURVATURE_MIN,
    }


def _range_over_atr(candle: _RuntimeCandle, features: _FeaturePacket) -> Decimal:
    return Decimal("0") if features.atr <= 0 else (candle.high - candle.low) / features.atr


def _bar_close_location(candle: _RuntimeCandle) -> Decimal | None:
    candle_range = candle.high - candle.low
    if candle_range <= 0:
        return None
    return (candle.close - candle.low) / candle_range


def _bar_body_to_range_ratio(candle: _RuntimeCandle) -> Decimal | None:
    candle_range = candle.high - candle.low
    if candle_range <= 0:
        return None
    return abs(candle.close - candle.open) / candle_range


def _churn_score(prior_bars_since_setup: int) -> Decimal:
    if prior_bars_since_setup > ANTI_CHURN_BARS:
        return Decimal("0")
    return Decimal(ANTI_CHURN_BARS + 1 - prior_bars_since_setup) / Decimal(ANTI_CHURN_BARS + 1)


def _normalized(value: Decimal, atr: Decimal) -> Decimal:
    return value / max(atr, RISK_FLOOR)


def _input_blocker(payload: Mapping[str, Any], candles: Sequence[_RuntimeCandle], min_completed_bars: int) -> str | None:
    contract_key = _optional_text(payload.get("contract_key"))
    instrument_family = _payload_instrument_family(payload)
    expected_prefix = "MNQ-" if instrument_family == MNQ_INSTRUMENT_FAMILY else "MGC-"
    if contract_key and not contract_key.startswith(expected_prefix):
        return (
            "Track B session-strategy envelope producer received mismatched contract/instrument metadata: "
            f"instrument_family={instrument_family}, contract_key={contract_key}."
        )
    timeframe = _optional_text(payload.get("timeframe"))
    if timeframe and timeframe != "5m":
        return "Track B session-strategy envelope producer requires bounded completed 5m candles."
    raw_candles = payload.get("candles") or payload.get("candle_history")
    if not isinstance(raw_candles, Sequence) or isinstance(raw_candles, (str, bytes)):
        return "Runtime 5m candle payload must include a candles array."
    if not raw_candles:
        return "Runtime 5m candle payload did not include any candles."
    incomplete_count = sum(1 for item in raw_candles if isinstance(item, Mapping) and item.get("completed") is not True)
    if incomplete_count:
        return f"Runtime 5m candle payload included {incomplete_count} incomplete candle(s)."
    if len(candles) < min_completed_bars:
        return f"Track B session-strategy envelope producer requires at least {min_completed_bars} completed 5m candles; observed {len(candles)}."
    if payload.get("realtime_quote_received") is not True:
        return "Session-strategy envelope producer requires realtime_quote_received=true on the runtime context payload."
    if payload.get("current_quote_available") is not True:
        return "Session-strategy envelope producer requires current_quote_available=true on the runtime context payload."
    if _optional_text(payload.get("quote_provider_mode")) != "REALTIME":
        return "Session-strategy envelope producer requires quote_provider_mode=REALTIME."
    return None


def _payload_instrument_family(payload: Mapping[str, Any]) -> str:
    return _optional_text(payload.get("instrument_family") or payload.get("symbol")) or MGC_INSTRUMENT_FAMILY


def _verdict_for_blocker(blocker: str) -> TrackBSessionStrategyEnvelopeProducerVerdict:
    if "incomplete" in blocker:
        return TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INCOMPLETE_5M_CANDLE
    if "at least" in blocker:
        return TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INSUFFICIENT_5M_CANDLES
    if "any candles" in blocker or "candles array" in blocker:
        return TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_NO_5M_CANDLES
    return TrackBSessionStrategyEnvelopeProducerVerdict.BLOCKED_INVALID_INPUT


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
            "track_b_session_strategy_envelope_producer_boundary": "track_b_session_strategy_envelope_producer",
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
    verdict: TrackBSessionStrategyEnvelopeProducerVerdict,
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
        "session_strategy_envelope_producer_verdict": verdict.value,
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
        "london_late_pause_resume_short_envelope_ready": False,
        "asia_late_flat_pullback_pause_resume_long_envelope_ready": False,
        "asia_early_pause_resume_short_envelope_ready": False,
        "asia_early_normal_breakout_retest_hold_long_envelope_ready": False,
        "us_derivative_bear_turn_envelope_ready": False,
        "mnq_us_derivative_bear_turn_envelope_ready": False,
        "us_late_pause_resume_long_envelope_ready": False,
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
    verdict: TrackBSessionStrategyEnvelopeProducerVerdict,
    report_json: Path,
    output_root: Path,
    now: datetime,
    producer_id: str,
    source_id: str,
    input_payload: Mapping[str, Any],
    input_payload_path: Path | None,
    candles: Sequence[_RuntimeCandle],
    primary_blocker: str,
    required_next_action: str = "Provide bounded completed realtime MGC 5m candles before producing session-strategy envelopes.",
    max_completed_5m_age_seconds: int | None = None,
) -> TrackBSessionStrategyEnvelopeProducerResult:
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
    _write_json(output_root / "latest_session_strategy_envelope_producer_report.json", report)
    return TrackBSessionStrategyEnvelopeProducerResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        london_late_pause_resume_short_event_json=None,
        asia_late_flat_pullback_pause_resume_long_event_json=None,
        asia_early_pause_resume_short_event_json=None,
        asia_early_normal_breakout_retest_hold_long_event_json=None,
        us_derivative_bear_turn_event_json=None,
        mnq_us_derivative_bear_turn_event_json=None,
        us_late_pause_resume_long_event_json=None,
        london_late_pause_resume_short_event=None,
        asia_late_flat_pullback_pause_resume_long_event=None,
        asia_early_pause_resume_short_event=None,
        asia_early_normal_breakout_retest_hold_long_event=None,
        us_derivative_bear_turn_event=None,
        mnq_us_derivative_bear_turn_event=None,
        us_late_pause_resume_long_event=None,
    )


def _research_session_phase(timestamp: datetime) -> str:
    local_time = timestamp.astimezone(NY).time()
    if time(18, 0) <= local_time < time(20, 30):
        return "ASIA_EARLY"
    if time(20, 30) <= local_time < time(23, 0):
        return "ASIA_LATE"
    if time(3, 0) <= local_time < time(5, 30):
        return "LONDON_OPEN"
    if time(5, 30) <= local_time < time(8, 30):
        return "LONDON_LATE"
    if time(9, 0) <= local_time < time(9, 30):
        return "US_PREOPEN_OPENING"
    if time(9, 30) <= local_time < time(10, 0):
        return "US_CASH_OPEN_IMPULSE"
    if time(10, 0) <= local_time < time(10, 30):
        return "US_OPEN_LATE"
    if time(11, 0) <= local_time < time(13, 30):
        return "US_MIDDAY"
    if time(13, 30) <= local_time < time(16, 0):
        return "US_LATE"
    return "OUT_OF_SCOPE"


def _asia_early_or_gc_mgc_london_open(timestamp: datetime) -> bool:
    phase = _research_session_phase(timestamp)
    if phase == "ASIA_EARLY":
        return True
    local_time = timestamp.astimezone(NY).time()
    return phase == "LONDON_OPEN" and local_time in {time(3, 5), time(3, 10), time(3, 15)}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True) + "\n", encoding="utf-8")
