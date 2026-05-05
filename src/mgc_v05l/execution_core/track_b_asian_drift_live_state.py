"""Track B Asian Drift live state producer.

This module is intentionally narrow. It does not import the research package and
it does not infer Asia Drift state from raw candles. It accepts completed 5m
rows that already carry the research-defined Asia Drift feature fields, applies
the stable state-machine transition rules, and writes the explicit Track B state
snapshot consumed by ``ASIAN_DRIFT_V1``.
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
from .track_b_asian_drift_state import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_STATE_OUTPUT_ROOT,
    TrackBAsianDriftStateResult,
    TrackBAsianDriftStateVerdict,
    write_track_b_asian_drift_state_snapshot,
)


DEFAULT_TRACK_B_ASIAN_DRIFT_LIVE_STATE_OUTPUT_ROOT = DEFAULT_TRACK_B_ASIAN_DRIFT_STATE_OUTPUT_ROOT

NO_TRADE = "NO_TRADE"
ASIA_DRIFT_LONG = "ASIA_DRIFT_LONG"
ASIA_DRIFT_SHORT = "ASIA_DRIFT_SHORT"
NORMAL_PULLBACK = "NORMAL_PULLBACK"
STRETCHED_BUT_VALID = "STRETCHED_BUT_VALID"
TOO_EXTENDED = "TOO_EXTENDED"
NO_PULLBACK = "NO_PULLBACK"
DISQUALIFYING_PULLBACK = "DISQUALIFYING_PULLBACK"

STATE_NO_TRADE = "NO_TRADE"
STATE_DRIFT_LONG_CANDIDATE = "DRIFT_LONG_CANDIDATE"
STATE_DRIFT_SHORT_CANDIDATE = "DRIFT_SHORT_CANDIDATE"
STATE_PULLBACK_PENDING = "PULLBACK_PENDING"
STATE_ENTRY_ARMED = "ENTRY_ARMED"
STATE_DRIFT_AT_RISK = "DRIFT_AT_RISK"
STATE_RECOVERED_DRIFT = "RECOVERED_DRIFT"
STATE_REQUALIFIED_CANDIDATE = "REQUALIFIED_CANDIDATE"
STATE_CONFIRMED_INVALIDATION = "CONFIRMED_INVALIDATION"
STATE_SESSION_TIMEOUT = "SESSION_TIMEOUT"

VALID_DRIFT_STATES = {
    STATE_DRIFT_LONG_CANDIDATE,
    STATE_DRIFT_SHORT_CANDIDATE,
    STATE_PULLBACK_PENDING,
    STATE_ENTRY_ARMED,
    STATE_RECOVERED_DRIFT,
    STATE_REQUALIFIED_CANDIDATE,
}

REQUIRED_FEATURE_FIELDS = (
    "calibration_profile",
    "timeframe",
    "session_bar_index",
    "asia_drift_session_id",
    "in_scope",
    "entry_window_open",
    "session_timeout",
    "anchor_observed",
    "close",
    "regime",
    "pullback_state",
    "hypothetical_entry_ready",
    "feature_version",
)

PROFILE_DEFAULTS = {
    "recovery_confirmed": {
        "disqualifying_confirmation_bars": 2,
        "regime_loss_confirmation_bars": 3,
        "at_risk_persistence_threshold": 0.50,
        "drift_collapse_confirmation_bars": 4,
        "vwap_reclaim_confirmation_bars": 2,
        "ema_failure_confirmation_bars": 2,
        "structure_break_confirmation_bars": 2,
        "recovery_score_threshold": 0.62,
        "requalification_score_threshold": 0.72,
        "max_recovery_bars": 4,
    }
}


class TrackBAsianDriftLiveStateVerdict(str, Enum):
    WROTE_SNAPSHOT = "TRACK_B_ASIAN_DRIFT_LIVE_STATE_WROTE_SNAPSHOT"
    BLOCKED_INSUFFICIENT_5M_CANDLES = "TRACK_B_ASIAN_DRIFT_LIVE_STATE_BLOCKED_INSUFFICIENT_5M_CANDLES"
    BLOCKED_MISSING_REALTIME_QUOTE = "TRACK_B_ASIAN_DRIFT_LIVE_STATE_BLOCKED_MISSING_REALTIME_QUOTE"
    BLOCKED_MISSING_FEATURE_FIELDS = "TRACK_B_ASIAN_DRIFT_LIVE_STATE_BLOCKED_MISSING_FEATURE_FIELDS"
    NOT_IMPLEMENTABLE_YET = "TRACK_B_ASIAN_DRIFT_LIVE_STATE_NOT_IMPLEMENTABLE_YET"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_ASIAN_DRIFT_LIVE_STATE_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBAsianDriftLiveStateResult:
    verdict: TrackBAsianDriftLiveStateVerdict
    report_json: Path
    report: dict[str, Any]
    snapshot_json: Path | None
    snapshot: dict[str, Any] | None
    state_writer: TrackBAsianDriftStateResult | None


def produce_track_b_asian_drift_live_state(
    *,
    runtime_payload: Mapping[str, Any],
    source_payload_path: Path | None = None,
    current_quote_report_payload: Mapping[str, Any] | None = None,
    current_quote_report_json: Path | None = None,
    expected_account_id: str | None = "DUM882026",
    account_id: str = "DUM882026",
    contract_key: str = "MGC-202606",
    instrument_family: str = "MGC",
    source_id: str = "track_b_asian_drift_live_state",
    strategy_id: str = "asian_drift_v1",
    lane_id: str = "mgc_example_long_lmt_day",
    output_root: Path = DEFAULT_TRACK_B_ASIAN_DRIFT_LIVE_STATE_OUTPUT_ROOT,
    producer_id: str | None = None,
    now: datetime | None = None,
) -> TrackBAsianDriftLiveStateResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"track_b_asian_drift_live_state_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_producer_id / "asian_drift_live_state_report.json"
    latest_report_json = Path(output_root) / "latest_asian_drift_live_state_report.json"
    try:
        rows = _feature_rows(runtime_payload)
        if len(rows) < 8:
            return _write_live_report(
                report_json=report_json,
                latest_report_json=latest_report_json,
                verdict=TrackBAsianDriftLiveStateVerdict.BLOCKED_INSUFFICIENT_5M_CANDLES,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                snapshot_writer=None,
                rows_available=len(rows),
                latest_state=None,
                primary_blocker=f"Asian Drift live state requires at least 8 completed 5m feature rows; received {len(rows)}.",
                required_next_action="Provide bounded completed 5m Asia Drift feature rows before state production.",
            )
        quote_blocker = _quote_blocker(runtime_payload, current_quote_report_payload)
        if quote_blocker is not None:
            return _write_live_report(
                report_json=report_json,
                latest_report_json=latest_report_json,
                verdict=TrackBAsianDriftLiveStateVerdict.BLOCKED_MISSING_REALTIME_QUOTE,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                snapshot_writer=None,
                rows_available=len(rows),
                latest_state=None,
                primary_blocker=quote_blocker,
                required_next_action="Provide realtime/current quote evidence before Asian Drift watch.",
            )
        missing = _missing_feature_fields(rows)
        if missing:
            return _write_live_report(
                report_json=report_json,
                latest_report_json=latest_report_json,
                verdict=TrackBAsianDriftLiveStateVerdict.BLOCKED_MISSING_FEATURE_FIELDS,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=source_id,
                source_payload_path=source_payload_path,
                snapshot_writer=None,
                rows_available=len(rows),
                latest_state=None,
                primary_blocker="Asian Drift live state input is missing research feature fields: " + ", ".join(missing),
                required_next_action="Run or supply the Asia Drift feature-row producer; raw 5m candles are not enough.",
            )
        sorted_rows = sorted(rows, key=lambda row: (_text(row.get("asia_drift_session_id")), _timestamp(row)))
        state_rows = _evaluate_states(sorted_rows)
        latest_state = state_rows[-1]
        latest_row = latest_state["row"]
        quote_evidence = _quote_evidence(runtime_payload, current_quote_report_payload, current_quote_report_json)
        state_payload = {
            **latest_row,
            **quote_evidence,
            "account_id": account_id,
            "expected_account_id": expected_account_id,
            "contract_key": contract_key,
            "instrument_family": instrument_family,
            "strategy_id": strategy_id,
            "signal_family": strategy_id,
            "lane_id": lane_id,
            "timeframe": "5m",
            "candle_timestamp": _timestamp(latest_row).isoformat(),
            "observed_at": actual_now.isoformat(),
            "asia_drift_state": latest_state["state"],
            "asia_drift_regime": _text(latest_row.get("regime")),
            "hypothetical_entry_ready": _bool(latest_row.get("hypothetical_entry_ready")),
            "entry_window_open": _bool(latest_row.get("entry_window_open")),
            "in_scope": _bool(latest_row.get("in_scope")),
            "session_timeout": _bool(latest_row.get("session_timeout")) or False,
            "feature_version": _text(latest_row.get("feature_version")),
            "calibration_profile": _text(latest_row.get("calibration_profile")),
            "close": latest_row.get("close"),
            "metadata": {
                "track_b_asian_drift_live_state_boundary": "track_b_asian_drift_live_state",
                "source_payload_path": None if source_payload_path is None else str(source_payload_path),
                "current_quote_report_json": None if current_quote_report_json is None else str(current_quote_report_json),
                "transition_reason": latest_state["transition_reason"],
                "previous_state": latest_state["previous_state"],
                "pullback_state": latest_row.get("pullback_state"),
                "dominant_direction": latest_row.get("dominant_direction"),
                "track_b_does_not_infer_asian_drift_from_raw_candles": True,
            },
            "submit_allowed": False,
            "submit_attempted": False,
            "live_money_readiness": False,
        }
        writer = write_track_b_asian_drift_state_snapshot(
            state_payload=state_payload,
            source_payload_path=source_payload_path,
            expected_account_id=expected_account_id,
            account_id=account_id,
            contract_key=contract_key,
            instrument_family=instrument_family,
            source_id=source_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            output_root=output_root,
            builder_id=actual_producer_id,
            now=actual_now,
        )
        verdict = (
            TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT
            if writer.verdict == TrackBAsianDriftStateVerdict.WROTE_SNAPSHOT
            else TrackBAsianDriftLiveStateVerdict.BLOCKED_SCHEMA_ERROR
        )
        return _write_live_report(
            report_json=report_json,
            latest_report_json=latest_report_json,
            verdict=verdict,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            snapshot_writer=writer,
            rows_available=len(rows),
            latest_state=latest_state,
            primary_blocker=None if verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT else writer.report.get("primary_blocker"),
            required_next_action=(
                "Run ASIAN_DRIFT_V1 no-submit watch against latest_asian_drift_5m_state_snapshot.json."
                if verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT
                else "Fix Asian Drift live state snapshot schema before retrying."
            ),
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_live_report(
            report_json=report_json,
            latest_report_json=latest_report_json,
            verdict=TrackBAsianDriftLiveStateVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=source_id,
            source_payload_path=source_payload_path,
            snapshot_writer=None,
            rows_available=None,
            latest_state=None,
            primary_blocker=str(exc),
            required_next_action="Fix Asian Drift live state input schema before retrying.",
        )


def _feature_rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    for key in ("asian_drift_feature_rows", "feature_rows", "candles"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, Mapping)]
    return []


def _missing_feature_fields(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    missing: set[str] = set()
    for row in rows:
        for field in REQUIRED_FEATURE_FIELDS:
            if row.get(field) is None:
                missing.add(field)
        if row.get("decision_ts") is None and row.get("candle_timestamp") is None:
            missing.add("decision_ts")
        instrument = _text(row.get("instrument") or row.get("instrument_family") or row.get("symbol"))
        if instrument != "MGC":
            missing.add("instrument=MGC")
    return sorted(missing)


def _quote_blocker(payload: Mapping[str, Any], quote_report: Mapping[str, Any] | None) -> str | None:
    provider_mode = _text(payload.get("quote_provider_mode") or (quote_report or {}).get("quote_provider_mode"))
    realtime_received = _bool(payload.get("realtime_quote_received"))
    if realtime_received is None and quote_report is not None:
        realtime_received = _bool(quote_report.get("realtime_quote_received"))
    current_available = _bool(payload.get("current_quote_available"))
    if current_available is None and quote_report is not None:
        current_available = _bool(quote_report.get("current_quote_available"))
    if provider_mode != "REALTIME":
        return "Asian Drift live state requires quote_provider_mode=REALTIME."
    if realtime_received is not True:
        return "Asian Drift live state requires realtime_quote_received=true."
    if current_available is not True:
        return "Asian Drift live state requires current_quote_available=true."
    return None


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
        "source_report_path": str(quote_report_json) if quote_report_json is not None else payload.get("source_report_path"),
    }


def _evaluate_states(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    previous_by_session: dict[str, dict[str, Any]] = {}
    context_by_session: dict[str, dict[str, Any]] = {}
    states: list[dict[str, Any]] = []
    for row in rows:
        session_id = _text(row.get("asia_drift_session_id")) or "UNKNOWN_SESSION"
        previous = previous_by_session.get(session_id)
        context = context_by_session.get(session_id, {})
        state, reason, at_risk_reason, bars_in_at_risk, resolution_tag, next_context = _derive_state(
            row=row,
            previous=previous,
            context=context,
        )
        state_row = {
            "row": dict(row),
            "state": state,
            "previous_state": None if previous is None else previous.get("state"),
            "transition_reason": reason,
            "at_risk_reason": at_risk_reason,
            "bars_in_at_risk_state": bars_in_at_risk,
            "resolution_tag": resolution_tag,
        }
        previous_by_session[session_id] = state_row
        context_by_session[session_id] = next_context
        states.append(state_row)
    return states


def _derive_state(
    *,
    row: Mapping[str, Any],
    previous: Mapping[str, Any] | None,
    context: Mapping[str, Any],
) -> tuple[str, str, str | None, int, str | None, dict[str, Any]]:
    profile = PROFILE_DEFAULTS.get(_text(row.get("calibration_profile")) or "recovery_confirmed", PROFILE_DEFAULTS["recovery_confirmed"])
    if _bool(row.get("in_scope")) is not True:
        if previous is not None and previous.get("state") not in {STATE_NO_TRADE, STATE_SESSION_TIMEOUT}:
            return STATE_SESSION_TIMEOUT, "left_derived_session_scope", None, 0, None, {}
        return STATE_NO_TRADE, "out_of_scope", None, 0, None, {}
    if _bool(row.get("session_timeout")) is True:
        return STATE_SESSION_TIMEOUT, "mandatory_handoff_exit_boundary", None, 0, None, {}
    if _bool(row.get("anchor_observed")) is not True:
        return STATE_NO_TRADE, "session_anchor_not_observed", None, 0, None, {}
    if _int(row.get("session_bar_index")) < 8:
        return STATE_NO_TRADE, "warmup_incomplete", None, 0, None, {}
    base_state, base_reason = _base_state(row)
    previous_state = _text(previous.get("state")) if previous is not None else None
    had_live_drift = previous_state in VALID_DRIFT_STATES | {STATE_DRIFT_AT_RISK}
    failure_reason, failure_category = _failure_signal(row=row, had_live_drift=had_live_drift, profile=profile)
    if previous_state == STATE_DRIFT_AT_RISK:
        recovery_state = _recovery_resolution(row=row, base_state=base_state, previous=previous, profile=profile, failure_reason=failure_reason)
        if recovery_state is not None:
            state, reason, resolution_tag = recovery_state
            return state, reason, None, 0, resolution_tag, {}
        if failure_reason is None and _int(previous.get("bars_in_at_risk_state")) < int(profile["max_recovery_bars"]):
            reason = _text(previous.get("at_risk_reason")) or "awaiting_recovery_confirmation"
            return STATE_DRIFT_AT_RISK, reason, reason, _int(previous.get("bars_in_at_risk_state")) + 1, None, dict(context)
    if failure_reason is None:
        return base_state, base_reason, None, 0, None, {}
    streak = 1
    if context.get("failure_category") == failure_category:
        streak = _int(context.get("failure_streak")) + 1
    confirmation_bars = _confirmation_bars(profile=profile, category=failure_category)
    next_context = {"failure_reason": failure_reason, "failure_category": failure_category, "failure_streak": streak}
    if streak >= confirmation_bars:
        return STATE_CONFIRMED_INVALIDATION, failure_reason, failure_reason, streak, "INVALIDATED", next_context
    return STATE_DRIFT_AT_RISK, failure_reason, failure_reason, streak, None, next_context


def _base_state(row: Mapping[str, Any]) -> tuple[str, str]:
    regime = _text(row.get("regime"))
    pullback_state = _text(row.get("pullback_state"))
    entry_window_open = _bool(row.get("entry_window_open")) is True
    if regime == NO_TRADE:
        return STATE_NO_TRADE, "session_filter_not_tradable"
    if regime == ASIA_DRIFT_LONG:
        if pullback_state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID} and entry_window_open:
            return STATE_ENTRY_ARMED, "valid_long_pullback_inside_entry_window"
        if pullback_state == TOO_EXTENDED or not entry_window_open:
            return STATE_PULLBACK_PENDING, "long_drift_waiting_for_better_reset"
        if pullback_state == NO_PULLBACK:
            return STATE_DRIFT_LONG_CANDIDATE, "long_drift_detected_no_pullback_yet"
        return STATE_DRIFT_LONG_CANDIDATE, "long_drift_detected"
    if regime == ASIA_DRIFT_SHORT:
        if pullback_state in {NORMAL_PULLBACK, STRETCHED_BUT_VALID} and entry_window_open:
            return STATE_ENTRY_ARMED, "valid_short_pullback_inside_entry_window"
        if pullback_state == TOO_EXTENDED or not entry_window_open:
            return STATE_PULLBACK_PENDING, "short_drift_waiting_for_better_reset"
        if pullback_state == NO_PULLBACK:
            return STATE_DRIFT_SHORT_CANDIDATE, "short_drift_detected_no_pullback_yet"
        return STATE_DRIFT_SHORT_CANDIDATE, "short_drift_detected"
    return STATE_NO_TRADE, "unclassified"


def _failure_signal(
    *,
    row: Mapping[str, Any],
    had_live_drift: bool,
    profile: Mapping[str, float | int],
) -> tuple[str | None, str | None]:
    if not had_live_drift:
        return None, None
    if _text(row.get("regime")) == NO_TRADE:
        if int(profile["regime_loss_confirmation_bars"]) <= 1 or _float(row.get("regime_persistence_score")) < float(profile["at_risk_persistence_threshold"]):
            return "regime_lost_after_candidate", "DRIFT_COLLAPSE"
        return "regime_loss_warning", "DRIFT_COLLAPSE"
    if _bool(row.get("pullback_structure_break")) is True:
        reason = _text(row.get("pullback_reason"))
        if reason == "confirmed_vwap_reclaim":
            return "confirmed_vwap_reclaim", "VWAP_RECLAIM"
        if reason == "vwap_and_slow_ema_failure":
            return "vwap_and_slow_ema_failure", "EMA_FAILURE"
        if reason == "protected_swing_break":
            return "protected_swing_break", "STRUCTURE_DAMAGE"
    if _text(row.get("pullback_state")) == DISQUALIFYING_PULLBACK or _bool(row.get("thesis_invalidated_flag")) is True:
        return _text(row.get("pullback_reason")) or "pullback_disqualified", _text(row.get("pullback_veto_category")) or "OTHER"
    if _text(row.get("pullback_vwap_interaction")) == "VWAP_TAG" and int(profile["vwap_reclaim_confirmation_bars"]) > 1:
        return "vwap_tag_warning", "VWAP_TAG"
    if _text(row.get("pullback_vwap_interaction")) == "SINGLE_CLOSE_THROUGH_VWAP":
        return "single_close_through_vwap", "VWAP_RECLAIM"
    if _bool(row.get("pullback_warning_flag")) is True and int(profile["disqualifying_confirmation_bars"]) > 1:
        return _text(row.get("pullback_warning_reason")) or "soft_pullback_warning", _text(row.get("pullback_warning_category")) or "OTHER"
    if _float(row.get("regime_persistence_score")) < float(profile["at_risk_persistence_threshold"]) and int(profile["regime_loss_confirmation_bars"]) > 1:
        return "drift_persistence_degraded", "DRIFT_COLLAPSE"
    return None, None


def _recovery_resolution(
    *,
    row: Mapping[str, Any],
    base_state: str,
    previous: Mapping[str, Any],
    profile: Mapping[str, float | int],
    failure_reason: str | None,
) -> tuple[str, str, str] | None:
    if failure_reason is not None:
        return None
    if _int(previous.get("bars_in_at_risk_state")) > int(profile["max_recovery_bars"]):
        return STATE_CONFIRMED_INVALIDATION, "recovery_window_expired", "INVALIDATED"
    if _float(row.get("recovery_score")) < float(profile["recovery_score_threshold"]):
        return None
    if base_state == STATE_ENTRY_ARMED and _float(row.get("recovery_score")) >= float(profile["requalification_score_threshold"]):
        return STATE_REQUALIFIED_CANDIDATE, "requalified_after_at_risk_recovery", "REQUALIFIED"
    if base_state in VALID_DRIFT_STATES:
        return STATE_RECOVERED_DRIFT, "drift_recovered_after_at_risk", "RECOVERED"
    return None


def _confirmation_bars(*, profile: Mapping[str, float | int], category: str | None) -> int:
    if category == "VWAP_RECLAIM":
        return int(profile["vwap_reclaim_confirmation_bars"])
    if category == "EMA_FAILURE":
        return int(profile["ema_failure_confirmation_bars"])
    if category == "STRUCTURE_DAMAGE":
        return int(profile["structure_break_confirmation_bars"])
    if category == "DRIFT_COLLAPSE":
        return max(int(profile["regime_loss_confirmation_bars"]), int(profile["drift_collapse_confirmation_bars"]))
    return int(profile["disqualifying_confirmation_bars"])


def _write_live_report(
    *,
    report_json: Path,
    latest_report_json: Path,
    verdict: TrackBAsianDriftLiveStateVerdict,
    now: datetime,
    producer_id: str,
    source_id: str,
    source_payload_path: Path | None,
    snapshot_writer: TrackBAsianDriftStateResult | None,
    rows_available: int | None,
    latest_state: Mapping[str, Any] | None,
    primary_blocker: object | None,
    required_next_action: str,
) -> TrackBAsianDriftLiveStateResult:
    row = latest_state.get("row") if latest_state else {}
    report = {
        "schema_version": "track_b_asian_drift_live_state_report_v1",
        "generated_at": now.isoformat(),
        "asian_drift_live_state_id": producer_id,
        "asian_drift_live_state_verdict": verdict.value,
        "asian_drift_state_builder_verdict": (
            verdict.value if snapshot_writer is None else snapshot_writer.report.get("asian_drift_state_builder_verdict")
        ),
        "asian_drift_watch_verdict": _watch_verdict(verdict, snapshot_writer),
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "rows_available": rows_available,
        "latest_state": None if latest_state is None else latest_state.get("state"),
        "asia_drift_state": None if latest_state is None else latest_state.get("state"),
        "asia_drift_regime": row.get("regime") if isinstance(row, Mapping) else None,
        "hypothetical_entry_ready": row.get("hypothetical_entry_ready") if isinstance(row, Mapping) else None,
        "entry_window_open": row.get("entry_window_open") if isinstance(row, Mapping) else None,
        "in_scope": row.get("in_scope") if isinstance(row, Mapping) else None,
        "transition_reason": None if latest_state is None else latest_state.get("transition_reason"),
        "asian_drift_state_ready": verdict == TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT,
        "asian_drift_state_snapshot_path": None if snapshot_writer is None else snapshot_writer.report.get("asian_drift_state_snapshot_path"),
        "latest_asian_drift_state_snapshot_path": None if snapshot_writer is None else snapshot_writer.report.get("latest_asian_drift_state_snapshot_path"),
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
        "latest_state_builder_report_json_path": str(report_json.parent.parent / "latest_asian_drift_state_builder_report.json"),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    latest_state_builder_report_json = report_json.parent.parent / "latest_asian_drift_state_builder_report.json"
    report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json.write_text(payload, encoding="utf-8")
    latest_state_builder_report_json.write_text(payload, encoding="utf-8")
    return TrackBAsianDriftLiveStateResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        snapshot_json=None if snapshot_writer is None else snapshot_writer.snapshot_json,
        snapshot=None if snapshot_writer is None else snapshot_writer.snapshot,
        state_writer=snapshot_writer,
    )


def _watch_verdict(verdict: TrackBAsianDriftLiveStateVerdict, snapshot_writer: TrackBAsianDriftStateResult | None) -> str:
    if verdict != TrackBAsianDriftLiveStateVerdict.WROTE_SNAPSHOT:
        return "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    if snapshot_writer is None:
        return "ASIAN_DRIFT_NOT_READY_FOR_TONIGHT"
    return str(snapshot_writer.report.get("asian_drift_watch_verdict") or "ASIAN_DRIFT_NO_SIGNAL_NO_MUTATION")


def _timestamp(row: Mapping[str, Any]) -> datetime:
    value = row.get("decision_ts") or row.get("candle_timestamp")
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("Asian Drift 5m feature row timestamp must be timezone-aware.")
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
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0
