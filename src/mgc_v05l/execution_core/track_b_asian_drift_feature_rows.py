"""Track B Asian Drift 5m feature-row producer.

This is a narrow execution-core mirror of the research-defined Asia Drift v1
feature row semantics. It deliberately avoids importing the research package,
does not emit broker actions, and only writes bounded no-submit artifacts that
can feed ``track_b_asian_drift_live_state``.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import Enum
from pathlib import Path
from statistics import fmean, median
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from .models import require_aware_datetime, to_jsonable
from .track_b_asian_drift_live_state import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_LIVE_STATE_OUTPUT_ROOT,
    TrackBAsianDriftLiveStateResult,
    produce_track_b_asian_drift_live_state,
)


DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT = DEFAULT_TRACK_B_ASIAN_DRIFT_LIVE_STATE_OUTPUT_ROOT

NEW_YORK = ZoneInfo("America/New_York")
EPS = 1e-9
FAST_EMA_SPAN = 4
SLOW_EMA_SPAN = 9
ATR_WINDOW = 8
WARMUP_BARS = 8

NO_TRADE = "NO_TRADE"
ASIA_DRIFT_LONG = "ASIA_DRIFT_LONG"
ASIA_DRIFT_SHORT = "ASIA_DRIFT_SHORT"

DRIFT_NONE = "NONE"
DRIFT_WEAK = "WEAK"
DRIFT_MEDIUM = "MEDIUM"
DRIFT_STRONG = "STRONG"

NO_PULLBACK = "NO_PULLBACK"
TOO_EXTENDED = "TOO_EXTENDED"
NORMAL_PULLBACK = "NORMAL_PULLBACK"
STRETCHED_BUT_VALID = "STRETCHED_BUT_VALID"
DISQUALIFYING_PULLBACK = "DISQUALIFYING_PULLBACK"

SLOW_OR_STANDARD = "SLOW_OR_STANDARD"
FAST_SHALLOW_VALID = "FAST_SHALLOW_VALID"
FAST_STRETCHED_WARNING = "FAST_STRETCHED_WARNING"
FAST_DEEP_DISQUALIFYING = "FAST_DEEP_DISQUALIFYING"

RECOVERY_CONFIRMED = "recovery_confirmed"


@dataclass(frozen=True)
class _CalibrationProfile:
    name: str
    pullback_speed_warning: float
    pullback_speed_disqualify: float
    pullback_depth_fraction_warning: float
    pullback_depth_fraction_disqualify: float
    pullback_depth_atr_warning: float
    pullback_depth_atr_disqualify: float
    pullback_expansion_warning: float
    pullback_expansion_disqualify: float
    normal_depth_fraction_max: float
    normal_depth_atr_max: float
    normal_speed_max: float
    disqualifying_confirmation_bars: int
    regime_loss_confirmation_bars: int
    at_risk_persistence_threshold: float
    invalidation_persistence_threshold: float
    drift_collapse_confirmation_bars: int
    vwap_reclaim_confirmation_bars: int
    ema_failure_confirmation_bars: int
    structure_break_confirmation_bars: int
    recovery_score_threshold: float
    requalification_score_threshold: float
    max_recovery_bars: int


CALIBRATION_PROFILES: dict[str, _CalibrationProfile] = {
    RECOVERY_CONFIRMED: _CalibrationProfile(
        name=RECOVERY_CONFIRMED,
        pullback_speed_warning=0.58,
        pullback_speed_disqualify=0.82,
        pullback_depth_fraction_warning=0.60,
        pullback_depth_fraction_disqualify=0.74,
        pullback_depth_atr_warning=1.15,
        pullback_depth_atr_disqualify=1.48,
        pullback_expansion_warning=1.32,
        pullback_expansion_disqualify=1.58,
        normal_depth_fraction_max=0.50,
        normal_depth_atr_max=1.00,
        normal_speed_max=0.42,
        disqualifying_confirmation_bars=2,
        regime_loss_confirmation_bars=3,
        at_risk_persistence_threshold=0.50,
        invalidation_persistence_threshold=0.29,
        drift_collapse_confirmation_bars=4,
        vwap_reclaim_confirmation_bars=2,
        ema_failure_confirmation_bars=2,
        structure_break_confirmation_bars=2,
        recovery_score_threshold=0.62,
        requalification_score_threshold=0.72,
        max_recovery_bars=4,
    )
}


@dataclass(frozen=True)
class _RuntimeBar:
    instrument: str
    timeframe: str
    start_ts: datetime
    end_ts: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    source: str

    @property
    def range_points(self) -> float:
        return max(self.high - self.low, EPS)


@dataclass(frozen=True)
class _PullbackAssessment:
    direction: str
    state: str
    reason: str | None
    veto_category: str | None
    depth_points: float
    depth_atr: float
    depth_fraction: float
    duration_bars: int
    speed: float
    severity: float
    expansion_ratio: float
    fast_pullback_class: str
    vwap_interaction: str
    structure_preserved: bool
    structure_break: bool
    warning_flag: bool
    warning_reason: str | None
    warning_category: str | None
    hard_invalidation_candidate: bool
    too_extended: bool
    drift_leg_start: float
    drift_leg_extreme: float
    protected_swing_price: float
    retracement_38: float
    retracement_50: float
    retracement_62: float
    envelope_low: float
    envelope_high: float


class TrackBAsianDriftFeatureRowsVerdict(str, Enum):
    WROTE_ROWS = "TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_WROTE_ROWS"
    BLOCKED_NO_5M_CANDLES = "TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_BLOCKED_NO_5M_CANDLES"
    BLOCKED_INCOMPLETE_5M_CANDLE = "TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_BLOCKED_INCOMPLETE_5M_CANDLE"
    BLOCKED_INSUFFICIENT_COMPLETED_5M_CANDLES = (
        "TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_BLOCKED_INSUFFICIENT_COMPLETED_5M_CANDLES"
    )
    BLOCKED_UNSUPPORTED_CALIBRATION = "TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_BLOCKED_UNSUPPORTED_CALIBRATION"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBAsianDriftFeatureRowsResult:
    verdict: TrackBAsianDriftFeatureRowsVerdict
    feature_rows_json: Path | None
    report_json: Path
    report: dict[str, Any]
    feature_rows_payload: dict[str, Any] | None
    live_state_result: TrackBAsianDriftLiveStateResult | None


def produce_track_b_asian_drift_feature_rows(
    *,
    runtime_5m_payload: Mapping[str, Any],
    source_payload_path: Path | None = None,
    current_quote_report_payload: Mapping[str, Any] | None = None,
    current_quote_report_json: Path | None = None,
    expected_account_id: str | None = "DUM882026",
    account_id: str = "DUM882026",
    contract_key: str = "MGC-202606",
    instrument_family: str = "MGC",
    local_symbol: str = "MGCM6",
    dataset: str = "GLBX.MDP3",
    source_id: str = "track_b_asian_drift_feature_rows",
    strategy_id: str = "asian_drift_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    calibration_profile: str = RECOVERY_CONFIRMED,
    output_root: Path = DEFAULT_TRACK_B_ASIAN_DRIFT_FEATURE_ROWS_OUTPUT_ROOT,
    producer_id: str | None = None,
    invoke_live_state: bool = True,
    now: datetime | None = None,
) -> TrackBAsianDriftFeatureRowsResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"track_b_asian_drift_feature_rows_{uuid.uuid4().hex}"
    output_root = Path(output_root)
    report_json = output_root / actual_producer_id / "asian_drift_feature_rows_report.json"
    feature_rows_json = output_root / actual_producer_id / "asian_drift_5m_feature_rows.json"
    latest_report_json = output_root / "latest_asian_drift_feature_rows_report.json"
    latest_feature_rows_json = output_root / "latest_asian_drift_5m_feature_rows.json"
    try:
        if calibration_profile not in CALIBRATION_PROFILES:
            return _write_result(
                verdict=TrackBAsianDriftFeatureRowsVerdict.BLOCKED_UNSUPPORTED_CALIBRATION,
                report_json=report_json,
                latest_report_json=latest_report_json,
                feature_rows_json=None,
                latest_feature_rows_json=None,
                feature_rows_payload=None,
                live_state_result=None,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                contract_key=contract_key,
                instrument_family=instrument_family,
                local_symbol=local_symbol,
                dataset=dataset,
                calibration_profile=calibration_profile,
                completed_bar_count=0,
                primary_blocker=f"Unsupported Asia Drift calibration profile: {calibration_profile}.",
                required_next_action="Use an explicitly mirrored Track B calibration profile.",
            )
        incomplete = [
            bar
            for bar in _raw_candles(runtime_5m_payload)
            if _completed_value(bar) is False
        ]
        if incomplete:
            return _write_result(
                verdict=TrackBAsianDriftFeatureRowsVerdict.BLOCKED_INCOMPLETE_5M_CANDLE,
                report_json=report_json,
                latest_report_json=latest_report_json,
                feature_rows_json=None,
                latest_feature_rows_json=None,
                feature_rows_payload=None,
                live_state_result=None,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                contract_key=contract_key,
                instrument_family=instrument_family,
                local_symbol=local_symbol,
                dataset=dataset,
                calibration_profile=calibration_profile,
                completed_bar_count=0,
                primary_blocker="Asian Drift feature rows require completed 5m bars; at least one input bar was incomplete.",
                required_next_action="Wait for the 5m bar to complete before feature-row production.",
            )
        bars = _parse_runtime_bars(
            runtime_5m_payload=runtime_5m_payload,
            instrument_family=instrument_family,
            local_symbol=local_symbol,
            default_source=source_id,
        )
        if not bars:
            return _write_result(
                verdict=TrackBAsianDriftFeatureRowsVerdict.BLOCKED_NO_5M_CANDLES,
                report_json=report_json,
                latest_report_json=latest_report_json,
                feature_rows_json=None,
                latest_feature_rows_json=None,
                feature_rows_payload=None,
                live_state_result=None,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                contract_key=contract_key,
                instrument_family=instrument_family,
                local_symbol=local_symbol,
                dataset=dataset,
                calibration_profile=calibration_profile,
                completed_bar_count=0,
                primary_blocker="Asian Drift feature rows require bounded completed 5m MGC candles; received 0.",
                required_next_action="Provide completed 5m MGC runtime candles before Asian Drift feature-row production.",
            )
        feature_rows = _build_feature_rows(bars=bars, profile=CALIBRATION_PROFILES[calibration_profile])
        quote_evidence = _quote_evidence(runtime_5m_payload, current_quote_report_payload, current_quote_report_json)
        payload = {
            "schema_version": "track_b_asian_drift_5m_feature_rows_v1",
            "generated_at": actual_now.isoformat(),
            "source_id": source_id,
            "account_id": account_id,
            "expected_account_id": expected_account_id,
            "contract_key": contract_key,
            "instrument_family": instrument_family,
            "local_symbol": local_symbol,
            "dataset": dataset,
            "timeframe": "5m",
            "calibration_profile": calibration_profile,
            "feature_version": f"asia_drift_v1_phase1:{calibration_profile}",
            "rows_available": len(feature_rows),
            "first_row_timestamp": None if not feature_rows else feature_rows[0]["decision_ts"],
            "last_row_timestamp": None if not feature_rows else feature_rows[-1]["decision_ts"],
            "asian_drift_feature_rows": feature_rows,
            **quote_evidence,
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        live_state_result = None
        if invoke_live_state and len(feature_rows) >= WARMUP_BARS:
            live_state_result = produce_track_b_asian_drift_live_state(
                runtime_payload=payload,
                source_payload_path=latest_feature_rows_json,
                current_quote_report_payload=current_quote_report_payload,
                current_quote_report_json=current_quote_report_json,
                expected_account_id=expected_account_id,
                account_id=account_id,
                contract_key=contract_key,
                instrument_family=instrument_family,
                source_id=source_id,
                strategy_id=strategy_id,
                lane_id=lane_id,
                output_root=output_root,
                producer_id=f"{actual_producer_id}_live_state",
                now=actual_now,
            )
        verdict = (
            TrackBAsianDriftFeatureRowsVerdict.WROTE_ROWS
            if len(feature_rows) >= WARMUP_BARS
            else TrackBAsianDriftFeatureRowsVerdict.BLOCKED_INSUFFICIENT_COMPLETED_5M_CANDLES
        )
        return _write_result(
            verdict=verdict,
            report_json=report_json,
            latest_report_json=latest_report_json,
            feature_rows_json=feature_rows_json,
            latest_feature_rows_json=latest_feature_rows_json,
            feature_rows_payload=payload,
            live_state_result=live_state_result,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            contract_key=contract_key,
            instrument_family=instrument_family,
            local_symbol=local_symbol,
            dataset=dataset,
            calibration_profile=calibration_profile,
            completed_bar_count=len(bars),
            primary_blocker=(
                None
                if verdict == TrackBAsianDriftFeatureRowsVerdict.WROTE_ROWS
                else f"Asian Drift live state requires at least 8 completed 5m feature rows; produced {len(feature_rows)}."
            ),
            required_next_action=(
                "Run ASIAN_DRIFT_V1 no-submit watch against latest_asian_drift_5m_state_snapshot.json."
                if verdict == TrackBAsianDriftFeatureRowsVerdict.WROTE_ROWS and live_state_result is not None
                else "Provide at least 8 completed 5m MGC candles before Asian Drift watch."
            ),
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_result(
            verdict=TrackBAsianDriftFeatureRowsVerdict.BLOCKED_SCHEMA_ERROR,
            report_json=report_json,
            latest_report_json=latest_report_json,
            feature_rows_json=None,
            latest_feature_rows_json=None,
            feature_rows_payload=None,
            live_state_result=None,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            contract_key=contract_key,
            instrument_family=instrument_family,
            local_symbol=local_symbol,
            dataset=dataset,
            calibration_profile=calibration_profile,
            completed_bar_count=0,
            primary_blocker=str(exc),
            required_next_action="Fix Asian Drift 5m candle input schema before retrying.",
        )


def _parse_runtime_bars(
    *,
    runtime_5m_payload: Mapping[str, Any],
    instrument_family: str,
    local_symbol: str,
    default_source: str,
) -> list[_RuntimeBar]:
    rows = _raw_candles(runtime_5m_payload)
    bars: list[_RuntimeBar] = []
    top_timeframe = _text(runtime_5m_payload.get("timeframe")) or "5m"
    for row in rows:
        completed_value = _completed_value(row)
        if completed_value is False:
            continue
        timeframe = _text(row.get("timeframe") or top_timeframe)
        if timeframe != "5m":
            raise ValueError(f"Asian Drift feature rows require timeframe=5m; received {timeframe}.")
        end_ts = _timestamp(row.get("end_ts") or row.get("bar_end_ts") or row.get("candle_timestamp") or row.get("timestamp"))
        start_value = row.get("start_ts") or row.get("bar_start_ts")
        start_ts = _timestamp(start_value) if start_value is not None else end_ts - timedelta(minutes=5)
        instrument = _text(
            row.get("instrument")
            or row.get("instrument_family")
            or row.get("symbol")
            or runtime_5m_payload.get("instrument_family")
            or runtime_5m_payload.get("symbol")
            or instrument_family
        )
        if instrument not in {instrument_family, local_symbol, "MGC"}:
            raise ValueError(f"Asian Drift feature rows are MGC-only; received instrument={instrument}.")
        bars.append(
            _RuntimeBar(
                instrument=instrument_family,
                timeframe="5m",
                start_ts=start_ts,
                end_ts=end_ts,
                open=_float_required(row.get("open"), "open"),
                high=_float_required(row.get("high"), "high"),
                low=_float_required(row.get("low"), "low"),
                close=_float_required(row.get("close") or row.get("last"), "close"),
                volume=max(_int(row.get("volume")), 0),
                source=_text(row.get("source") or runtime_5m_payload.get("source_id") or default_source) or default_source,
            )
        )
    return sorted(bars, key=lambda bar: bar.end_ts)


def _completed_value(row: Mapping[str, Any]) -> bool | None:
    if "completed" in row:
        return _bool(row.get("completed"))
    if "is_complete" in row:
        return _bool(row.get("is_complete"))
    return None


def _raw_candles(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("candles", "candle_history", "runtime_candles", "bars", "ohlcv"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _build_feature_rows(*, bars: Sequence[_RuntimeBar], profile: _CalibrationProfile) -> list[dict[str, Any]]:
    close_values = [bar.close for bar in bars]
    fast_ema_values = _rolling_ema(close_values, span=FAST_EMA_SPAN)
    slow_ema_values = _rolling_ema(close_values, span=SLOW_EMA_SPAN)
    atr_values = _rolling_atr(bars, window=ATR_WINDOW)

    feature_rows: list[dict[str, Any]] = []
    session_bars: dict[str, list[_RuntimeBar]] = {}
    session_ranges: dict[str, list[float]] = {}
    session_vwap_state: dict[str, tuple[float, float]] = {}
    anchor_observed_by_session: dict[str, bool] = {}
    last_impulse_by_session_direction: dict[tuple[str, str], int] = {}

    for index, bar in enumerate(bars):
        local_time = bar.end_ts.astimezone(NEW_YORK).timetz().replace(tzinfo=None)
        session_date = _session_date(bar.end_ts.astimezone(NEW_YORK))
        provisional_session_id = f"{bar.instrument.lower()}__{session_date.isoformat()}__asia_drift_v1"
        if provisional_session_id not in anchor_observed_by_session:
            anchor_observed_by_session[provisional_session_id] = _anchor_observed_for_bar(local_time=local_time)
        scope = _derive_session_scope(
            instrument=bar.instrument,
            timeframe=bar.timeframe,
            bar_end_ts=bar.end_ts,
            anchor_observed=anchor_observed_by_session[provisional_session_id],
        )
        anchor_observed_by_session[scope["asia_drift_session_id"]] = (
            anchor_observed_by_session.get(scope["asia_drift_session_id"], False)
            or _anchor_observed_for_bar(local_time=local_time)
        )
        scope = _derive_session_scope(
            instrument=bar.instrument,
            timeframe=bar.timeframe,
            bar_end_ts=bar.end_ts,
            anchor_observed=anchor_observed_by_session[scope["asia_drift_session_id"]],
        )
        session_id = str(scope["asia_drift_session_id"])
        session_bars.setdefault(session_id, []).append(bar)
        bars_in_session = session_bars[session_id]
        session_ranges.setdefault(session_id, []).append(bar.range_points)
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        price_volume, total_volume = session_vwap_state.get(session_id, (0.0, 0.0))
        price_volume += typical_price * max(float(bar.volume), 1.0)
        total_volume += max(float(bar.volume), 1.0)
        session_vwap_state[session_id] = (price_volume, total_volume)
        session_vwap = price_volume / max(total_volume, 1.0)
        atr = max(float(atr_values[index]), EPS)
        fast_ema = float(fast_ema_values[index])
        slow_ema = float(slow_ema_values[index])
        previous_slow = float(slow_ema_values[index - 1]) if index > 0 else slow_ema
        slow_slope = (slow_ema - previous_slow) / atr

        session_open = bars_in_session[0].open
        close_location = (bar.close - bar.low) / max(bar.high - bar.low, EPS)
        signed_session_disp_long = (bar.close - session_open) / atr
        signed_session_disp_short = (session_open - bar.close) / atr
        signed_vwap_disp_long = (bar.close - session_vwap) / atr
        signed_vwap_disp_short = (session_vwap - bar.close) / atr
        slope_3_long = _directional_slope(bars_in_session, direction="LONG", window=3, atr=atr)
        slope_6_long = _directional_slope(bars_in_session, direction="LONG", window=6, atr=atr)
        slope_12_long = _directional_slope(bars_in_session, direction="LONG", window=12, atr=atr)
        slope_3_short = _directional_slope(bars_in_session, direction="SHORT", window=3, atr=atr)
        slope_6_short = _directional_slope(bars_in_session, direction="SHORT", window=6, atr=atr)
        slope_12_short = _directional_slope(bars_in_session, direction="SHORT", window=12, atr=atr)
        slope_combo_long = 0.5 * slope_3_long + 0.3 * slope_6_long + 0.2 * slope_12_long
        slope_combo_short = 0.5 * slope_3_short + 0.3 * slope_6_short + 0.2 * slope_12_short
        efficiency_ratio_12 = _efficiency_ratio(bars_in_session, window=12)
        close_location_persistence_long = _close_location_persistence(bars_in_session, direction="LONG", window=8)
        close_location_persistence_short = _close_location_persistence(bars_in_session, direction="SHORT", window=8)
        bar_overlap_ratio_8 = _bar_overlap_ratio(bars_in_session, window=8)
        reversal_frequency_12 = _reversal_frequency(bars_in_session, window=12)
        directional_persistence_8 = _directional_persistence(bars_in_session, window=8)
        local_realized_volatility = _realized_volatility(bars_in_session, window=6)
        realized_volatility_ratio = _realized_volatility_ratio(bars_in_session, window=6, baseline_window=18)
        upside_extension_atr = max(bar.close - max(fast_ema, session_vwap), 0.0) / atr
        downside_extension_atr = max(min(fast_ema, session_vwap) - bar.close, 0.0) / atr
        chop_veto = (
            abs(signed_session_disp_long) < 0.6
            or efficiency_ratio_12 < 0.30
            or (bar_overlap_ratio_8 > 0.65 and reversal_frequency_12 > 0.30)
            or reversal_frequency_12 > 0.45
        )
        post_spike_instability = _post_spike_instability(
            bars_in_session=bars_in_session,
            atr=atr,
            realized_volatility_ratio=realized_volatility_ratio,
            efficiency_ratio=efficiency_ratio_12,
        )
        long_score, long_reasons = _drift_score(
            direction="LONG",
            signed_session_disp=signed_session_disp_long,
            signed_vwap_disp=signed_vwap_disp_long,
            slope_combo=slope_combo_long,
            efficiency_ratio=efficiency_ratio_12,
            close_location_persistence=close_location_persistence_long,
            bar_overlap_ratio=bar_overlap_ratio_8,
            reversal_frequency=reversal_frequency_12,
            extension_atr=upside_extension_atr,
            realized_volatility_ratio=realized_volatility_ratio,
            post_spike_instability=post_spike_instability,
            directional_persistence=directional_persistence_8,
        )
        short_score, short_reasons = _drift_score(
            direction="SHORT",
            signed_session_disp=signed_session_disp_short,
            signed_vwap_disp=signed_vwap_disp_short,
            slope_combo=slope_combo_short,
            efficiency_ratio=efficiency_ratio_12,
            close_location_persistence=close_location_persistence_short,
            bar_overlap_ratio=bar_overlap_ratio_8,
            reversal_frequency=reversal_frequency_12,
            extension_atr=downside_extension_atr,
            realized_volatility_ratio=realized_volatility_ratio,
            post_spike_instability=post_spike_instability,
            directional_persistence=directional_persistence_8,
        )
        long_strength = _drift_strength(long_score)
        short_strength = _drift_strength(short_score)
        score_gap = abs(long_score - short_score)
        dominant_direction = "LONG" if long_score >= short_score else "SHORT"
        warmup_complete = len(bars_in_session) >= WARMUP_BARS and bool(scope["anchor_observed"])
        regime = NO_TRADE
        if warmup_complete and not chop_veto and not post_spike_instability and long_score >= 3.1 and long_score - short_score >= 0.75:
            regime = ASIA_DRIFT_LONG
        elif warmup_complete and not chop_veto and not post_spike_instability and short_score >= 3.1 and short_score - long_score >= 0.75:
            regime = ASIA_DRIFT_SHORT

        pullback = _pullback_assessment(
            bars_in_session=bars_in_session,
            profile=profile,
            regime=regime,
            close=bar.close,
            session_open=session_open,
            session_vwap=session_vwap,
            fast_ema=fast_ema,
            slow_ema=slow_ema,
            atr=atr,
            session_ranges=session_ranges[session_id],
        )
        relevant_direction = _relevant_direction(regime=regime, dominant_direction=dominant_direction)
        if _is_drift_impulse(
            direction=relevant_direction,
            close=bar.close,
            session_vwap=session_vwap,
            fast_ema=fast_ema,
            signed_vwap_disp_long=signed_vwap_disp_long,
            signed_vwap_disp_short=signed_vwap_disp_short,
            slope_combo_long=slope_combo_long,
            slope_combo_short=slope_combo_short,
            close_location=close_location,
        ):
            last_impulse_by_session_direction[(session_id, relevant_direction)] = len(bars_in_session)
        last_impulse_index = last_impulse_by_session_direction.get((session_id, relevant_direction), len(bars_in_session))
        bars_since_last_drift_impulse = max(len(bars_in_session) - last_impulse_index, 0)
        regime_persistence_score = _regime_persistence_score(
            direction=relevant_direction,
            drift_score=long_score if relevant_direction == "LONG" else short_score,
            signed_vwap_disp=signed_vwap_disp_long if relevant_direction == "LONG" else signed_vwap_disp_short,
            slope_combo=slope_combo_long if relevant_direction == "LONG" else slope_combo_short,
            close_location_persistence=close_location_persistence_long if relevant_direction == "LONG" else close_location_persistence_short,
            pullback=pullback,
            bars_since_last_drift_impulse=bars_since_last_drift_impulse,
            realized_volatility_ratio=realized_volatility_ratio,
            regime=regime,
        )
        renewed_signed_vwap_displacement = signed_vwap_disp_long if relevant_direction == "LONG" else signed_vwap_disp_short
        slope_recovery_score = slope_combo_long if relevant_direction == "LONG" else slope_combo_short
        close_location_recovery_score = (
            close_location_persistence_long if relevant_direction == "LONG" else close_location_persistence_short
        )
        fresh_drift_impulse = bars_since_last_drift_impulse == 0
        countertrend_extension_failed = _countertrend_extension_failed(
            direction=relevant_direction,
            signed_vwap_disp=renewed_signed_vwap_displacement,
            pullback=pullback,
            realized_volatility_ratio=realized_volatility_ratio,
        )
        compression_followed_by_drift_expansion = _compression_followed_by_drift_expansion(
            signed_vwap_disp=renewed_signed_vwap_displacement,
            slope_combo=slope_recovery_score,
            pullback=pullback,
        )
        recovery_score = _recovery_score(
            renewed_signed_vwap_displacement=renewed_signed_vwap_displacement,
            slope_recovery_score=slope_recovery_score,
            close_location_recovery_score=close_location_recovery_score,
            fresh_drift_impulse=fresh_drift_impulse,
            countertrend_extension_failed=countertrend_extension_failed,
            compression_followed_by_drift_expansion=compression_followed_by_drift_expansion,
            regime_persistence_score=regime_persistence_score,
        )
        hypothetical_entry_ready = (
            regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT}
            and pullback.state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID}
            and bool(scope["entry_window_open"])
        )
        thesis_invalidated_flag = regime in {ASIA_DRIFT_LONG, ASIA_DRIFT_SHORT} and pullback.hard_invalidation_candidate
        feature_version = f"asia_drift_v1_phase1:{profile.name}"
        feature_rows.append(
            {
                "calibration_profile": profile.name,
                "instrument": bar.instrument,
                "instrument_family": bar.instrument,
                "timeframe": bar.timeframe,
                "decision_ts": bar.end_ts.isoformat(),
                "candle_timestamp": bar.end_ts.isoformat(),
                "local_session_date": scope["local_session_date"].isoformat(),
                "asia_drift_session_id": session_id,
                "session_bar_index": len(bars_in_session),
                "session_time_label": scope["session_time_label"],
                "subphase": scope["subphase"],
                "in_scope": scope["in_scope"],
                "entry_window_open": scope["entry_window_open"],
                "session_timeout": scope["session_timeout"],
                "scope_provenance": scope["scope_provenance"],
                "anchor_observed": scope["anchor_observed"],
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "range_points": bar.range_points,
                "session_open": session_open,
                "session_vwap": session_vwap,
                "atr": atr,
                "fast_ema": fast_ema,
                "slow_ema": slow_ema,
                "slow_ema_slope": slow_slope,
                "session_displacement_atr": (bar.close - session_open) / atr,
                "signed_session_displacement_long": signed_session_disp_long,
                "signed_session_displacement_short": signed_session_disp_short,
                "signed_vwap_displacement_long": signed_vwap_disp_long,
                "signed_vwap_displacement_short": signed_vwap_disp_short,
                "bars_since_last_drift_impulse": bars_since_last_drift_impulse,
                "regime_persistence_score": regime_persistence_score,
                "regime_persistence_label": _regime_persistence_label(score=regime_persistence_score, profile=profile),
                "recovery_score": recovery_score,
                "recovery_label": _recovery_label(score=recovery_score, profile=profile),
                "renewed_signed_vwap_displacement": renewed_signed_vwap_displacement,
                "slope_recovery_score": slope_recovery_score,
                "close_location_recovery_score": close_location_recovery_score,
                "fresh_drift_impulse": fresh_drift_impulse,
                "countertrend_extension_failed": countertrend_extension_failed,
                "compression_followed_by_drift_expansion": compression_followed_by_drift_expansion,
                "slope_3_long": slope_3_long,
                "slope_6_long": slope_6_long,
                "slope_12_long": slope_12_long,
                "slope_3_short": slope_3_short,
                "slope_6_short": slope_6_short,
                "slope_12_short": slope_12_short,
                "slope_combo_long": slope_combo_long,
                "slope_combo_short": slope_combo_short,
                "efficiency_ratio_12": efficiency_ratio_12,
                "close_location": close_location,
                "close_location_persistence_long": close_location_persistence_long,
                "close_location_persistence_short": close_location_persistence_short,
                "bar_overlap_ratio_8": bar_overlap_ratio_8,
                "reversal_frequency_12": reversal_frequency_12,
                "directional_persistence_8": directional_persistence_8,
                "local_realized_volatility": local_realized_volatility,
                "realized_volatility_ratio": realized_volatility_ratio,
                "upside_extension_atr": upside_extension_atr,
                "downside_extension_atr": downside_extension_atr,
                "long_drift_score": long_score,
                "short_drift_score": short_score,
                "long_drift_strength": long_strength,
                "short_drift_strength": short_strength,
                "dominant_direction": dominant_direction,
                "regime": regime,
                "score_gap": score_gap,
                "chop_veto": chop_veto,
                "post_spike_instability": post_spike_instability,
                "extension_elevated": max(upside_extension_atr, downside_extension_atr) >= 2.2,
                "drift_long_reasons": list(long_reasons),
                "drift_short_reasons": list(short_reasons),
                "pullback_direction": pullback.direction,
                "pullback_state": pullback.state,
                "pullback_reason": pullback.reason,
                "pullback_veto_category": pullback.veto_category,
                "pullback_depth_points": pullback.depth_points,
                "pullback_depth_atr": pullback.depth_atr,
                "pullback_depth_fraction": pullback.depth_fraction,
                "pullback_duration_bars": pullback.duration_bars,
                "pullback_speed": pullback.speed,
                "pullback_severity": pullback.severity,
                "pullback_expansion_ratio": pullback.expansion_ratio,
                "fast_pullback_class": pullback.fast_pullback_class,
                "pullback_vwap_interaction": pullback.vwap_interaction,
                "pullback_structure_preserved": pullback.structure_preserved,
                "pullback_structure_break": pullback.structure_break,
                "pullback_warning_flag": pullback.warning_flag,
                "pullback_warning_reason": pullback.warning_reason,
                "pullback_warning_category": pullback.warning_category,
                "pullback_hard_invalidation_candidate": pullback.hard_invalidation_candidate,
                "pullback_too_extended": pullback.too_extended,
                "drift_leg_start": pullback.drift_leg_start,
                "drift_leg_extreme": pullback.drift_leg_extreme,
                "protected_swing_price": pullback.protected_swing_price,
                "retracement_38": pullback.retracement_38,
                "retracement_50": pullback.retracement_50,
                "retracement_62": pullback.retracement_62,
                "envelope_low": pullback.envelope_low,
                "envelope_high": pullback.envelope_high,
                "hypothetical_entry_ready": hypothetical_entry_ready,
                "thesis_invalidated_flag": thesis_invalidated_flag,
                "feature_version": feature_version,
            }
        )
    return feature_rows


def _derive_session_scope(*, instrument: str, timeframe: str, bar_end_ts: datetime, anchor_observed: bool) -> dict[str, Any]:
    local_ts = bar_end_ts.astimezone(NEW_YORK)
    session_date = _session_date(local_ts)
    anchor_ts = datetime.combine(session_date, time(18, 0), tzinfo=NEW_YORK)
    latest_entry_ts = datetime.combine(session_date + timedelta(days=1), time(1, 30), tzinfo=NEW_YORK)
    mandatory_exit_ts = datetime.combine(session_date + timedelta(days=1), time(2, 55), tzinfo=NEW_YORK)
    in_scope = anchor_ts <= local_ts <= mandatory_exit_ts
    session_timeout = local_ts > mandatory_exit_ts
    entry_window_open = anchor_ts <= local_ts <= latest_entry_ts
    return {
        "instrument": instrument,
        "timeframe": timeframe,
        "bar_end_ts": bar_end_ts,
        "local_session_date": session_date,
        "asia_drift_session_id": f"{instrument.lower()}__{session_date.isoformat()}__asia_drift_v1",
        "session_time_label": local_ts.strftime("%H:%M"),
        "subphase": _subphase(local_ts.timetz().replace(tzinfo=None)) if in_scope else "OUT_OF_SCOPE",
        "in_scope": in_scope,
        "entry_window_open": in_scope and entry_window_open,
        "session_timeout": session_timeout,
        "anchor_observed": anchor_observed,
        "scope_provenance": "asia_drift_v1_phase1.track_b_derived_scope",
    }


def _session_date(local_ts: datetime) -> date:
    if local_ts.timetz().replace(tzinfo=None) >= time(18, 0):
        return local_ts.date()
    return (local_ts - timedelta(days=1)).date()


def _subphase(local_time: time) -> str:
    if time(18, 0) <= local_time < time(20, 30):
        return "ASIA_DRIFT_BUILD"
    if local_time >= time(20, 30) or local_time < time(0, 30):
        return "ASIA_DRIFT_MATURE"
    if time(0, 30) <= local_time <= time(2, 55):
        return "ASIA_DRIFT_PRE_HANDOFF"
    return "OUT_OF_SCOPE"


def _anchor_observed_for_bar(*, local_time: time) -> bool:
    anchor_dt = datetime.combine(date(2000, 1, 1), time(18, 0))
    local_dt = datetime.combine(date(2000, 1, 1), local_time)
    return anchor_dt <= local_dt <= anchor_dt + timedelta(minutes=5)


def _rolling_ema(values: Sequence[float], *, span: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (float(span) + 1.0)
    ema_values = [float(values[0])]
    for value in values[1:]:
        ema_values.append(alpha * float(value) + (1.0 - alpha) * ema_values[-1])
    return ema_values


def _rolling_atr(bars: Sequence[_RuntimeBar], *, window: int = ATR_WINDOW) -> list[float]:
    true_ranges: list[float] = []
    for index, bar in enumerate(bars):
        if index == 0:
            true_ranges.append(bar.range_points)
            continue
        previous_close = bars[index - 1].close
        true_ranges.append(max(bar.high - bar.low, abs(bar.high - previous_close), abs(bar.low - previous_close)))
    atr_values: list[float] = []
    for index in range(len(true_ranges)):
        window_values = true_ranges[max(0, index - window + 1) : index + 1]
        atr_values.append(float(median(window_values)))
    return atr_values


def _directional_slope(bars: Sequence[_RuntimeBar], *, direction: str, window: int, atr: float) -> float:
    if len(bars) <= window:
        return 0.0
    raw = (bars[-1].close - bars[-1 - window].close) / max(window * atr, EPS)
    return raw if direction == "LONG" else -raw


def _efficiency_ratio(bars: Sequence[_RuntimeBar], *, window: int) -> float:
    if len(bars) < 3:
        return 0.0
    effective_window = min(window, len(bars) - 1)
    recent = bars[-(effective_window + 1) :]
    net = abs(recent[-1].close - recent[0].close)
    travel = sum(abs(current.close - previous.close) for previous, current in zip(recent, recent[1:], strict=False))
    return net / max(travel, EPS)


def _close_location_persistence(bars: Sequence[_RuntimeBar], *, direction: str, window: int) -> float:
    recent = list(bars[-window:])
    if not recent:
        return 0.0
    if direction == "LONG":
        hits = [1.0 for bar in recent if ((bar.close - bar.low) / max(bar.high - bar.low, EPS)) >= 0.60]
    else:
        hits = [1.0 for bar in recent if ((bar.close - bar.low) / max(bar.high - bar.low, EPS)) <= 0.40]
    return sum(hits) / max(len(recent), 1)


def _bar_overlap_ratio(bars: Sequence[_RuntimeBar], *, window: int) -> float:
    recent = list(bars[-window:])
    if len(recent) < 2:
        return 0.0
    overlaps = [
        max(0.0, min(previous.high, current.high) - max(previous.low, current.low))
        / max(previous.high - previous.low, current.high - current.low, EPS)
        for previous, current in zip(recent, recent[1:], strict=False)
    ]
    return fmean(overlaps) if overlaps else 0.0


def _reversal_frequency(bars: Sequence[_RuntimeBar], *, window: int) -> float:
    recent = list(bars[-(window + 1) :])
    if len(recent) < 3:
        return 0.0
    signs: list[int] = []
    for previous, current in zip(recent, recent[1:], strict=False):
        delta = current.close - previous.close
        signs.append(1 if delta > 0 else -1 if delta < 0 else 0)
    reversals = 0
    previous_sign = 0
    for sign in signs:
        if sign == 0:
            continue
        if previous_sign != 0 and sign == -previous_sign:
            reversals += 1
        previous_sign = sign
    return reversals / max(len(signs) - 1, 1)


def _directional_persistence(bars: Sequence[_RuntimeBar], *, window: int) -> float:
    recent = list(bars[-(window + 1) :])
    if len(recent) < 2:
        return 0.0
    signed_steps = [current.close - previous.close for previous, current in zip(recent, recent[1:], strict=False)]
    return abs(sum(signed_steps)) / max(sum(abs(step) for step in signed_steps), EPS)


def _realized_volatility(bars: Sequence[_RuntimeBar], *, window: int) -> float:
    recent = list(bars[-(window + 1) :])
    if len(recent) < 2:
        return 0.0
    returns = [current.close - previous.close for previous, current in zip(recent, recent[1:], strict=False)]
    return (sum(value * value for value in returns) / max(len(returns), 1)) ** 0.5


def _realized_volatility_ratio(bars: Sequence[_RuntimeBar], *, window: int, baseline_window: int) -> float:
    recent = list(bars[-(baseline_window + 1) :])
    if len(recent) < 3:
        return 1.0
    absolute_returns = [abs(current.close - previous.close) for previous, current in zip(recent, recent[1:], strict=False)]
    local = fmean(absolute_returns[-window:]) if absolute_returns[-window:] else 0.0
    baseline = median(absolute_returns) if absolute_returns else 0.0
    return local / max(baseline, EPS)


def _post_spike_instability(
    *,
    bars_in_session: Sequence[_RuntimeBar],
    atr: float,
    realized_volatility_ratio: float,
    efficiency_ratio: float,
) -> bool:
    recent = list(bars_in_session[-3:])
    if not recent:
        return False
    extreme_range = max((bar.high - bar.low) / max(atr, EPS) for bar in recent)
    return extreme_range >= 2.0 and (realized_volatility_ratio >= 1.8 or efficiency_ratio < 0.45)


def _drift_score(
    *,
    direction: str,
    signed_session_disp: float,
    signed_vwap_disp: float,
    slope_combo: float,
    efficiency_ratio: float,
    close_location_persistence: float,
    bar_overlap_ratio: float,
    reversal_frequency: float,
    extension_atr: float,
    realized_volatility_ratio: float,
    post_spike_instability: bool,
    directional_persistence: float,
) -> tuple[float, tuple[str, ...]]:
    x1 = _clip(signed_session_disp / 2.0, 0.0, 1.5)
    x2 = _clip(signed_vwap_disp / 1.0, 0.0, 1.0)
    x3 = _clip(slope_combo / 0.35, 0.0, 1.25)
    x4 = _clip((efficiency_ratio - 0.30) / 0.40, 0.0, 1.0)
    x5 = _clip((close_location_persistence - 0.50) / 0.40, 0.0, 1.0)
    x6 = _clip((0.60 - bar_overlap_ratio) / 0.35, 0.0, 1.0)
    x7 = _clip((0.55 - reversal_frequency) / 0.35, 0.0, 1.0)
    penalties = 0.0
    reasons: list[str] = []
    if signed_session_disp >= 1.0:
        reasons.append("session_displacement_material")
    if signed_vwap_disp >= 0.15:
        reasons.append("vwap_displacement_positive")
    if slope_combo >= 0.20:
        reasons.append("multi_window_slope_aligned")
    if efficiency_ratio >= 0.38:
        reasons.append("efficiency_supportive")
    if close_location_persistence >= 0.55:
        reasons.append("close_location_persistent")
    if bar_overlap_ratio <= 0.55:
        reasons.append("overlap_not_choppy")
    if reversal_frequency <= 0.35:
        reasons.append("reversal_frequency_controlled")
    if directional_persistence >= 0.40:
        reasons.append("directional_persistence_supportive")
    if extension_atr > 2.2:
        penalties += 0.6
        reasons.append("extension_elevated_penalty")
    if post_spike_instability:
        penalties += 0.8
        reasons.append("post_spike_instability_penalty")
    if realized_volatility_ratio > 1.8 and signed_session_disp < 1.2:
        penalties += 0.4
        reasons.append("volatility_surge_penalty")
    if reversal_frequency > 0.35 and bar_overlap_ratio > 0.50:
        penalties += 0.5
        reasons.append("countertrend_quality_penalty")
    score = 1.25 * x1 + 1.0 * x2 + 1.0 * x3 + 0.75 * x4 + 0.50 * x5 + 0.50 * x6 + 0.50 * x7 - penalties
    return score, tuple(reasons)


def _drift_strength(score: float) -> str:
    if score >= 3.9:
        return DRIFT_STRONG
    if score >= 3.1:
        return DRIFT_MEDIUM
    if score >= 2.4:
        return DRIFT_WEAK
    return DRIFT_NONE


def _pullback_assessment(
    *,
    bars_in_session: Sequence[_RuntimeBar],
    profile: _CalibrationProfile,
    regime: str,
    close: float,
    session_open: float,
    session_vwap: float,
    fast_ema: float,
    slow_ema: float,
    atr: float,
    session_ranges: Sequence[float],
) -> _PullbackAssessment:
    if regime == ASIA_DRIFT_SHORT:
        direction = "SHORT"
        session_extreme = min(bar.low for bar in bars_in_session)
        extreme_index = max(index for index, bar in enumerate(bars_in_session) if bar.low == session_extreme)
        leg_size = max(session_open - session_extreme, EPS)
        depth_points = max(close - session_extreme, 0.0)
        protected_swing = max(bar.high for bar in bars_in_session[max(0, len(bars_in_session) - 4) :])
    else:
        direction = "LONG"
        session_extreme = max(bar.high for bar in bars_in_session)
        extreme_index = max(index for index, bar in enumerate(bars_in_session) if bar.high == session_extreme)
        leg_size = max(session_extreme - session_open, EPS)
        depth_points = max(session_extreme - close, 0.0)
        protected_swing = min(bar.low for bar in bars_in_session[max(0, len(bars_in_session) - 4) :])
    vwap_interaction = _pullback_vwap_interaction(bars_in_session, direction=direction, session_vwap=session_vwap, slow_ema=slow_ema)
    structure_break_reason = _structure_break_reason(direction=direction, close=close, protected_swing=protected_swing, vwap_interaction=vwap_interaction)
    structure_break = structure_break_reason is not None
    depth_atr = depth_points / max(atr, EPS)
    depth_fraction = depth_points / max(leg_size, EPS)
    duration_bars = max(len(bars_in_session) - 1 - extreme_index, 0)
    speed = depth_atr / max(duration_bars, 1)
    pullback_ranges = list(session_ranges[extreme_index + 1 :]) if duration_bars > 0 else []
    baseline_ranges = list(session_ranges[max(0, extreme_index - 12) : extreme_index + 1])
    expansion_ratio = fmean(pullback_ranges) / max(median(baseline_ranges), EPS) if pullback_ranges and baseline_ranges else 1.0
    too_extended = duration_bars == 0 and (
        (close - max(fast_ema, session_vwap)) / max(atr, EPS) > 2.2
        if direction == "LONG"
        else (min(fast_ema, session_vwap) - close) / max(atr, EPS) > 2.2
    )
    fast_pullback_class = _fast_pullback_class(
        profile=profile,
        depth_fraction=depth_fraction,
        depth_atr=depth_atr,
        speed=speed,
        expansion_ratio=expansion_ratio,
        vwap_interaction=vwap_interaction,
        structure_break=structure_break,
    )
    retracement_38 = session_extreme - 0.382 * leg_size if direction == "LONG" else session_extreme + 0.382 * leg_size
    retracement_50 = session_extreme - 0.500 * leg_size if direction == "LONG" else session_extreme + 0.500 * leg_size
    retracement_62 = session_extreme - 0.618 * leg_size if direction == "LONG" else session_extreme + 0.618 * leg_size
    envelope_low = min(retracement_62, min(session_vwap, fast_ema) - 0.10 * atr)
    envelope_high = max(retracement_38, max(session_vwap, fast_ema) + 0.10 * atr)
    veto_category = None
    warning_flag = False
    warning_reason = None
    warning_category = None
    hard_invalidation_candidate = False
    if too_extended:
        state = TOO_EXTENDED
        reason = "too_extended_without_reset"
    elif structure_break:
        state = DISQUALIFYING_PULLBACK
        reason = structure_break_reason
        veto_category = _reason_category(reason)
        hard_invalidation_candidate = True
    elif depth_fraction > profile.pullback_depth_fraction_disqualify or depth_atr > profile.pullback_depth_atr_disqualify:
        state = DISQUALIFYING_PULLBACK
        reason = "depth_exceeds_limit"
        veto_category = "DEPTH"
    elif speed > profile.pullback_speed_disqualify:
        if fast_pullback_class == FAST_SHALLOW_VALID:
            state = NORMAL_PULLBACK
            reason = "fast_shallow_valid"
        elif fast_pullback_class == FAST_STRETCHED_WARNING:
            state = STRETCHED_BUT_VALID
            reason = "fast_stretched_warning"
        else:
            state = DISQUALIFYING_PULLBACK
            reason = "fast_deep_disqualifying"
            veto_category = "SPEED"
    elif expansion_ratio > profile.pullback_expansion_disqualify:
        state = DISQUALIFYING_PULLBACK
        reason = "violent_countertrend_expansion"
        veto_category = "EXPANSION"
    elif depth_fraction < 0.18 and depth_atr < 0.30:
        state = NO_PULLBACK
        reason = "too_shallow"
    elif depth_fraction <= profile.normal_depth_fraction_max and depth_atr <= profile.normal_depth_atr_max and speed <= profile.normal_speed_max:
        state = NORMAL_PULLBACK
        reason = None
    else:
        state = STRETCHED_BUT_VALID
        reason = "deep_but_structure_intact"
    if state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID}:
        warning_reason, warning_category = _pullback_warning_reason(
            profile=profile,
            depth_fraction=depth_fraction,
            depth_atr=depth_atr,
            speed=speed,
            expansion_ratio=expansion_ratio,
            vwap_interaction=vwap_interaction,
            fast_pullback_class=fast_pullback_class,
        )
        warning_flag = warning_reason is not None
    return _PullbackAssessment(
        direction=direction,
        state=state,
        reason=reason,
        veto_category=veto_category,
        depth_points=depth_points,
        depth_atr=depth_atr,
        depth_fraction=depth_fraction,
        duration_bars=duration_bars,
        speed=speed,
        severity=max(depth_atr, speed, max(expansion_ratio - 1.0, 0.0)),
        expansion_ratio=expansion_ratio,
        fast_pullback_class=fast_pullback_class,
        vwap_interaction=vwap_interaction,
        structure_preserved=not structure_break,
        structure_break=structure_break,
        warning_flag=warning_flag,
        warning_reason=warning_reason,
        warning_category=warning_category,
        hard_invalidation_candidate=hard_invalidation_candidate,
        too_extended=too_extended,
        drift_leg_start=session_open,
        drift_leg_extreme=session_extreme,
        protected_swing_price=protected_swing,
        retracement_38=retracement_38,
        retracement_50=retracement_50,
        retracement_62=retracement_62,
        envelope_low=envelope_low,
        envelope_high=envelope_high,
    )


def _pullback_vwap_interaction(
    bars_in_session: Sequence[_RuntimeBar],
    *,
    direction: str,
    session_vwap: float,
    slow_ema: float,
) -> str:
    recent = list(bars_in_session[-2:])
    if direction == "LONG":
        closes_through = [bar.close < session_vwap for bar in recent]
        if len(closes_through) >= 2 and all(closes_through):
            return "CONFIRMED_CLOSES_THROUGH_VWAP"
        if recent and recent[-1].close < session_vwap and recent[-1].close < slow_ema:
            return "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"
        if recent and recent[-1].close < session_vwap:
            return "SINGLE_CLOSE_THROUGH_VWAP"
        if recent and recent[-1].low <= session_vwap <= recent[-1].high:
            return "VWAP_TAG"
        return "FAVORABLE_SIDE_OF_VWAP"
    closes_through = [bar.close > session_vwap for bar in recent]
    if len(closes_through) >= 2 and all(closes_through):
        return "CONFIRMED_CLOSES_THROUGH_VWAP"
    if recent and recent[-1].close > session_vwap and recent[-1].close > slow_ema:
        return "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"
    if recent and recent[-1].close > session_vwap:
        return "SINGLE_CLOSE_THROUGH_VWAP"
    if recent and recent[-1].low <= session_vwap <= recent[-1].high:
        return "VWAP_TAG"
    return "FAVORABLE_SIDE_OF_VWAP"


def _structure_break_reason(*, direction: str, close: float, protected_swing: float, vwap_interaction: str) -> str | None:
    if direction == "LONG" and close < protected_swing:
        return "protected_swing_break"
    if direction == "SHORT" and close > protected_swing:
        return "protected_swing_break"
    if vwap_interaction == "CONFIRMED_CLOSES_THROUGH_VWAP":
        return "confirmed_vwap_reclaim"
    if vwap_interaction == "CLOSE_THROUGH_VWAP_AND_SLOW_EMA":
        return "vwap_and_slow_ema_failure"
    return None


def _pullback_warning_reason(
    *,
    profile: _CalibrationProfile,
    depth_fraction: float,
    depth_atr: float,
    speed: float,
    expansion_ratio: float,
    vwap_interaction: str,
    fast_pullback_class: str,
) -> tuple[str | None, str | None]:
    if fast_pullback_class == FAST_STRETCHED_WARNING:
        return "fast_stretched_warning", "SPEED"
    if fast_pullback_class == FAST_SHALLOW_VALID:
        return None, None
    if vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP":
        return "single_close_through_vwap", "VWAP_INTERACTION"
    if depth_fraction > profile.pullback_depth_fraction_warning or depth_atr > profile.pullback_depth_atr_warning:
        return "depth_warning", "DEPTH"
    if speed > profile.pullback_speed_warning:
        return "speed_warning", "SPEED"
    if expansion_ratio > profile.pullback_expansion_warning:
        return "expansion_warning", "EXPANSION"
    return None, None


def _fast_pullback_class(
    *,
    profile: _CalibrationProfile,
    depth_fraction: float,
    depth_atr: float,
    speed: float,
    expansion_ratio: float,
    vwap_interaction: str,
    structure_break: bool,
) -> str:
    if speed <= profile.pullback_speed_warning:
        return SLOW_OR_STANDARD
    if structure_break:
        return FAST_DEEP_DISQUALIFYING
    if vwap_interaction in {"CONFIRMED_CLOSES_THROUGH_VWAP", "CLOSE_THROUGH_VWAP_AND_SLOW_EMA"}:
        return FAST_DEEP_DISQUALIFYING
    if depth_fraction > profile.pullback_depth_fraction_disqualify or depth_atr > profile.pullback_depth_atr_disqualify:
        return FAST_DEEP_DISQUALIFYING
    if expansion_ratio > profile.pullback_expansion_disqualify:
        return FAST_DEEP_DISQUALIFYING
    shallow_depth = depth_fraction <= profile.normal_depth_fraction_max and depth_atr <= profile.normal_depth_atr_max
    warning_depth = depth_fraction <= profile.pullback_depth_fraction_warning and depth_atr <= profile.pullback_depth_atr_warning
    favorable_vwap = vwap_interaction in {"FAVORABLE_SIDE_OF_VWAP", "VWAP_TAG"}
    recoverable_vwap = favorable_vwap or vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP"
    compressed = expansion_ratio <= profile.pullback_expansion_warning
    if shallow_depth and favorable_vwap and compressed:
        return FAST_SHALLOW_VALID
    if warning_depth and recoverable_vwap and compressed:
        return FAST_STRETCHED_WARNING
    return FAST_DEEP_DISQUALIFYING


def _relevant_direction(*, regime: str, dominant_direction: str) -> str:
    if regime == ASIA_DRIFT_SHORT:
        return "SHORT"
    return "LONG" if regime == ASIA_DRIFT_LONG else dominant_direction


def _is_drift_impulse(
    *,
    direction: str,
    close: float,
    session_vwap: float,
    fast_ema: float,
    signed_vwap_disp_long: float,
    signed_vwap_disp_short: float,
    slope_combo_long: float,
    slope_combo_short: float,
    close_location: float,
) -> bool:
    if direction == "LONG":
        return close >= max(session_vwap, fast_ema) and signed_vwap_disp_long >= 0.05 and slope_combo_long >= 0.12 and close_location >= 0.55
    return close <= min(session_vwap, fast_ema) and signed_vwap_disp_short >= 0.05 and slope_combo_short >= 0.12 and close_location <= 0.45


def _regime_persistence_score(
    *,
    direction: str,
    drift_score: float,
    signed_vwap_disp: float,
    slope_combo: float,
    close_location_persistence: float,
    pullback: _PullbackAssessment,
    bars_since_last_drift_impulse: int,
    realized_volatility_ratio: float,
    regime: str,
) -> float:
    drift_component = _clip(drift_score / 4.1, 0.0, 1.0)
    vwap_component = _clip((signed_vwap_disp + 0.10) / 0.70, 0.0, 1.0)
    slope_component = _clip((slope_combo + 0.05) / 0.35, 0.0, 1.0)
    close_component = _clip((close_location_persistence - 0.40) / 0.40, 0.0, 1.0)
    compression_component = _clip((1.55 - pullback.expansion_ratio) / 0.70, 0.0, 1.0)
    freshness_component = _clip((6.0 - float(bars_since_last_drift_impulse)) / 6.0, 0.0, 1.0)
    score = (
        0.28 * drift_component
        + 0.18 * vwap_component
        + 0.18 * slope_component
        + 0.12 * close_component
        + 0.12 * compression_component
        + 0.12 * freshness_component
    )
    penalties = 0.0
    if regime == NO_TRADE:
        penalties += 0.10
    if pullback.vwap_interaction == "SINGLE_CLOSE_THROUGH_VWAP":
        penalties += 0.06
    if pullback.vwap_interaction == "CONFIRMED_CLOSES_THROUGH_VWAP":
        penalties += 0.15
    if pullback.vwap_interaction == "CLOSE_THROUGH_VWAP_AND_SLOW_EMA":
        penalties += 0.22
    if pullback.structure_break:
        penalties += 0.28
    if pullback.reason == "violent_countertrend_expansion":
        penalties += 0.16
    if realized_volatility_ratio > 1.8 and pullback.expansion_ratio > 1.2:
        penalties += 0.08
    return _clip(score - penalties, 0.0, 1.0)


def _regime_persistence_label(*, score: float, profile: _CalibrationProfile) -> str:
    if score >= profile.at_risk_persistence_threshold + 0.10:
        return "PERSISTENT"
    if score >= profile.invalidation_persistence_threshold:
        return "AT_RISK"
    return "COLLAPSING"


def _countertrend_extension_failed(
    *,
    direction: str,
    signed_vwap_disp: float,
    pullback: _PullbackAssessment,
    realized_volatility_ratio: float,
) -> bool:
    return (
        signed_vwap_disp >= 0.05
        and pullback.expansion_ratio <= 1.15
        and pullback.depth_atr <= 1.10
        and not pullback.structure_break
        and realized_volatility_ratio <= 1.75
        and direction in {"LONG", "SHORT"}
    )


def _compression_followed_by_drift_expansion(
    *,
    signed_vwap_disp: float,
    slope_combo: float,
    pullback: _PullbackAssessment,
) -> bool:
    return signed_vwap_disp >= 0.08 and slope_combo >= 0.14 and pullback.expansion_ratio <= 1.05 and pullback.speed <= 0.42


def _recovery_score(
    *,
    renewed_signed_vwap_displacement: float,
    slope_recovery_score: float,
    close_location_recovery_score: float,
    fresh_drift_impulse: bool,
    countertrend_extension_failed: bool,
    compression_followed_by_drift_expansion: bool,
    regime_persistence_score: float,
) -> float:
    score = (
        0.22 * _clip((renewed_signed_vwap_displacement + 0.02) / 0.45, 0.0, 1.0)
        + 0.20 * _clip((slope_recovery_score + 0.02) / 0.28, 0.0, 1.0)
        + 0.16 * _clip((close_location_recovery_score - 0.42) / 0.40, 0.0, 1.0)
        + 0.18 * regime_persistence_score
        + (0.12 if fresh_drift_impulse else 0.0)
        + (0.07 if countertrend_extension_failed else 0.0)
        + (0.05 if compression_followed_by_drift_expansion else 0.0)
    )
    return _clip(score, 0.0, 1.0)


def _recovery_label(*, score: float, profile: _CalibrationProfile) -> str:
    if score >= profile.requalification_score_threshold:
        return "REQUALIFY"
    if score >= profile.recovery_score_threshold:
        return "RECOVERING"
    return "UNRECOVERED"


def _reason_category(reason: str | None) -> str | None:
    mapping = {
        "protected_swing_break": "STRUCTURE_DAMAGE",
        "confirmed_vwap_reclaim": "VWAP_RECLAIM",
        "vwap_and_slow_ema_failure": "EMA_FAILURE",
        "depth_exceeds_limit": "DEPTH",
        "pullback_too_fast": "SPEED",
        "violent_countertrend_expansion": "EXPANSION",
    }
    if reason is None:
        return None
    return mapping.get(reason, "OTHER")


def _write_result(
    *,
    verdict: TrackBAsianDriftFeatureRowsVerdict,
    report_json: Path,
    latest_report_json: Path,
    feature_rows_json: Path | None,
    latest_feature_rows_json: Path | None,
    feature_rows_payload: dict[str, Any] | None,
    live_state_result: TrackBAsianDriftLiveStateResult | None,
    now: datetime,
    producer_id: str,
    source_id: str,
    source_payload_path: Path | None,
    contract_key: str,
    instrument_family: str,
    local_symbol: str,
    dataset: str,
    calibration_profile: str,
    completed_bar_count: int,
    primary_blocker: object | None,
    required_next_action: str,
) -> TrackBAsianDriftFeatureRowsResult:
    rows_available = 0 if feature_rows_payload is None else int(feature_rows_payload.get("rows_available") or 0)
    report = {
        "schema_version": "track_b_asian_drift_feature_rows_report_v1",
        "generated_at": now.isoformat(),
        "asian_drift_feature_rows_id": producer_id,
        "asian_drift_feature_rows_verdict": verdict.value,
        "asian_drift_watch_verdict": (
            "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
            if live_state_result is None
            else live_state_result.report.get("asian_drift_watch_verdict")
        ),
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "contract_key": contract_key,
        "instrument_family": instrument_family,
        "local_symbol": local_symbol,
        "dataset": dataset,
        "timeframe": "5m",
        "calibration_profile": calibration_profile,
        "completed_5m_bar_count": completed_bar_count,
        "feature_row_count": rows_available,
        "feature_rows_ready": verdict == TrackBAsianDriftFeatureRowsVerdict.WROTE_ROWS,
        "feature_rows_json_path": None if feature_rows_json is None else str(feature_rows_json),
        "latest_feature_rows_json_path": None if latest_feature_rows_json is None else str(latest_feature_rows_json),
        "live_state_invoked": live_state_result is not None,
        "live_state_verdict": None if live_state_result is None else live_state_result.report.get("asian_drift_live_state_verdict"),
        "asian_drift_state_ready": False if live_state_result is None else live_state_result.report.get("asian_drift_state_ready"),
        "latest_asian_drift_state_snapshot_path": None
        if live_state_result is None
        else live_state_result.report.get("latest_asian_drift_state_snapshot_path"),
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "paper_proof_cli_called": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    if feature_rows_payload is not None and feature_rows_json is not None and latest_feature_rows_json is not None:
        payload_text = json.dumps(to_jsonable(feature_rows_payload), indent=2, sort_keys=True)
        feature_rows_json.parent.mkdir(parents=True, exist_ok=True)
        feature_rows_json.write_text(payload_text, encoding="utf-8")
        latest_feature_rows_json.write_text(payload_text, encoding="utf-8")
    report_text = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(report_text, encoding="utf-8")
    latest_report_json.write_text(report_text, encoding="utf-8")
    return TrackBAsianDriftFeatureRowsResult(
        verdict=verdict,
        feature_rows_json=feature_rows_json,
        report_json=report_json,
        report=report,
        feature_rows_payload=feature_rows_payload,
        live_state_result=live_state_result,
    )


def _quote_evidence(
    payload: Mapping[str, Any],
    quote_report: Mapping[str, Any] | None,
    quote_report_json: Path | None,
) -> dict[str, Any]:
    quote = quote_report or {}
    return {
        "quote_provider_mode": _text(payload.get("quote_provider_mode") or quote.get("quote_provider_mode")),
        "realtime_quote_received": _bool(payload.get("realtime_quote_received"))
        if _bool(payload.get("realtime_quote_received")) is not None
        else _bool(quote.get("realtime_quote_received")),
        "current_quote_available": _bool(payload.get("current_quote_available"))
        if _bool(payload.get("current_quote_available")) is not None
        else _bool(quote.get("current_quote_available")),
        "quote_freshness_verdict": payload.get("quote_freshness_verdict") or quote.get("quote_freshness_verdict"),
        "current_quote_report_json": None if quote_report_json is None else str(quote_report_json),
    }


def _clip(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _timestamp(value: object) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Asian Drift 5m candle timestamp must be timezone-aware.")
    return dt


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = _text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"true", "1", "yes", "y"}:
        return True
    if lowered in {"false", "0", "no", "n"}:
        return False
    return None


def _int(value: object) -> int:
    try:
        if value is None:
            return 0
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _float_required(value: object, field_name: str) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Asian Drift 5m candle field {field_name} must be numeric.") from exc
