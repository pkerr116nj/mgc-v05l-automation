"""Per-lane Track B PAPER no-trade diagnostics.

The helpers in this module are pure/read-only except for the explicit artifact
writer. They do not evaluate strategy thresholds or call broker/order APIs.
"""

from __future__ import annotations

import json
import os
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from .bounded_jsonl import append_bounded_jsonl
from .models import to_jsonable
from .track_b_strategy_attrition_funnel import (
    events_from_no_trade_diagnostic,
    try_record_strategy_funnel_events,
)


NO_TRADE_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/no_trade_diagnostics")
LATEST_NO_TRADE_DIAGNOSTICS_JSON = NO_TRADE_DIAGNOSTICS_ROOT / "latest_no_trade_diagnostics.json"
NO_TRADE_DIAGNOSTICS_JSONL = NO_TRADE_DIAGNOSTICS_ROOT / "no_trade_diagnostics.jsonl"
NO_CANDIDATE_SUMMARY_JSON = NO_TRADE_DIAGNOSTICS_ROOT / "latest_no_candidate_summary.json"
NO_CANDIDATE_SUMMARY_JSONL = NO_TRADE_DIAGNOSTICS_ROOT / "no_candidate_summary.jsonl"
DEFAULT_NO_CANDIDATE_WINDOW_SECONDS = 300


class NoTradeFinalDecision(str, Enum):
    NO_SETUP = "NO_SETUP"
    FILTER_REJECTED = "FILTER_REJECTED"
    GOVERNANCE_BLOCKED = "GOVERNANCE_BLOCKED"
    EXPOSURE_BLOCKED = "EXPOSURE_BLOCKED"
    STARTUP_CATCHUP_DIAGNOSTIC_ONLY = "STARTUP_CATCHUP_DIAGNOSTIC_ONLY"
    STARTUP_CATCHUP_NOT_ROUTABLE = "STARTUP_CATCHUP_NOT_ROUTABLE"
    ROUTE_HELD_UNTIL_READINESS_CONVERGED = "ROUTE_HELD_UNTIL_READINESS_CONVERGED"
    WOULD_ROUTE = "WOULD_ROUTE"
    ORDER_INTENT_CREATED = "ORDER_INTENT_CREATED"


_PREDICATE_SUFFIXES = (
    "_ok",
    "_raw",
    "_candidate",
    "_signal",
    "_entry",
    "_setup",
    "_bar",
    "_hold",
    "_reversal_bar",
)

_EXPOSURE_BLOCKER_TOKENS = (
    "exposure",
    "position",
    "same_underlying",
    "opposite",
    "pending",
    "stack",
    "pyramid",
)


class NoCandidateReasonCode(str, Enum):
    NO_SETUP = "NO_SETUP"
    SESSION_DISALLOWED = "SESSION_DISALLOWED"
    WARMUP_INCOMPLETE = "WARMUP_INCOMPLETE"
    MIN_EVIDENCE_NOT_MET = "MIN_EVIDENCE_NOT_MET"
    SIDE_DISABLED = "SIDE_DISABLED"
    COOLDOWN_ACTIVE = "COOLDOWN_ACTIVE"
    SAME_UNDERLYING_HOLD = "SAME_UNDERLYING_HOLD"
    MISSING_CONTEXT = "MISSING_CONTEXT"
    SUBMIT_BLOCKED = "SUBMIT_BLOCKED"
    UNKNOWN = "UNKNOWN"


_TERMINAL_CANDIDATE_DECISIONS = {
    NoTradeFinalDecision.ORDER_INTENT_CREATED.value,
    NoTradeFinalDecision.WOULD_ROUTE.value,
}


class NoCandidateWindowAggregator:
    """Aggregate no-candidate reasons without emitting one artifact per bar."""

    def __init__(
        self,
        *,
        diagnostics_root: Path | str,
        window_seconds: int = DEFAULT_NO_CANDIDATE_WINDOW_SECONDS,
    ) -> None:
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        self._diagnostics_root = Path(diagnostics_root)
        self._window = timedelta(seconds=window_seconds)
        self._window_seconds = window_seconds
        self._state: dict[str, Any] | None = None

    def observe(self, payload: Mapping[str, Any]) -> dict[str, Any] | None:
        """Record one no-candidate observation and emit only at window boundaries."""

        if not is_no_candidate_observation(payload):
            return None
        observed_at = _parse_timestamp(payload.get("bar_timestamp") or payload.get("generated_at"))
        emitted: dict[str, Any] | None = None
        if self._state is None:
            self._state = self._new_window(payload, observed_at)
        elif observed_at >= self._state["window_started_at"] + self._window:
            emitted = self.flush(generated_at=observed_at)
            self._state = self._new_window(payload, observed_at)
        self._add(payload, observed_at)
        return emitted

    def flush(self, *, generated_at: datetime | None = None) -> dict[str, Any] | None:
        if self._state is None or self._state["bars_evaluated"] <= 0:
            self._state = None
            return None
        generated = generated_at or datetime.now(timezone.utc)
        summary = _build_no_candidate_summary(self._state, generated_at=generated, window_seconds=self._window_seconds)
        write_no_candidate_summary(summary, diagnostics_root=self._diagnostics_root)
        self._state = None
        return summary

    def _new_window(self, payload: Mapping[str, Any], observed_at: datetime) -> dict[str, Any]:
        return {
            "lane_id": str(payload.get("lane_id") or ""),
            "symbol": str(payload.get("symbol") or "").upper(),
            "session": str(payload.get("session") or "UNKNOWN"),
            "window_started_at": observed_at,
            "window_ended_at": observed_at,
            "bars_evaluated": 0,
            "reason_counts": Counter(),
            "latest_evaluation_timestamp": observed_at,
            "latest_bar_id": payload.get("bar_id"),
            "session_state": {},
            "warmup_min_evidence_state": {},
            "cooldown_hold_state": {},
        }

    def _add(self, payload: Mapping[str, Any], observed_at: datetime) -> None:
        if self._state is None:
            self._state = self._new_window(payload, observed_at)
        reason = classify_no_candidate_reason(payload)
        extra = payload.get("extra") if isinstance(payload.get("extra"), Mapping) else {}
        self._state["bars_evaluated"] += 1
        self._state["reason_counts"][reason] += 1
        self._state["window_ended_at"] = max(self._state["window_ended_at"], observed_at)
        self._state["latest_evaluation_timestamp"] = observed_at
        self._state["latest_bar_id"] = payload.get("bar_id")
        self._state["session"] = str(payload.get("session") or self._state["session"] or "UNKNOWN")
        self._state["session_state"] = {
            "session": self._state["session"],
            "session_allowed": payload.get("session_allowed"),
        }
        self._state["warmup_min_evidence_state"] = {
            "warmup_complete": extra.get("warmup_complete"),
            "warmup_bars_observed": extra.get("warmup_bars_observed"),
            "warmup_bars_required": extra.get("warmup_bars_required"),
            "min_evidence_met": extra.get("min_evidence_met"),
        }
        self._state["cooldown_hold_state"] = {
            "cooldown_active": reason == NoCandidateReasonCode.COOLDOWN_ACTIVE.value,
            "entries_enabled": extra.get("entries_enabled"),
            "exits_enabled": extra.get("exits_enabled"),
            "operator_halt": extra.get("operator_halt"),
            "same_underlying_entry_hold": extra.get("same_underlying_entry_hold"),
        }


def classify_no_candidate_reason(payload: Mapping[str, Any]) -> str:
    extra = payload.get("extra") if isinstance(payload.get("extra"), Mapping) else {}
    reason_text = " ".join(
        str(value or "")
        for value in (
            payload.get("blocker_reason"),
            payload.get("final_decision"),
            extra.get("same_underlying_hold_reason"),
        )
    ).lower()
    if payload.get("session_allowed") is False or "session_not_allowed" in reason_text:
        return NoCandidateReasonCode.SESSION_DISALLOWED.value
    if payload.get("strategy_evaluated") is False or "context_feature_history_not_ready" in reason_text:
        return NoCandidateReasonCode.MISSING_CONTEXT.value
    if "warmup" in reason_text or extra.get("warmup_complete") is False:
        return NoCandidateReasonCode.WARMUP_INCOMPLETE.value
    if "same_underlying" in reason_text or extra.get("same_underlying_entry_hold") is True:
        return NoCandidateReasonCode.SAME_UNDERLYING_HOLD.value
    if "side_not_allowed" in reason_text or "side_disabled" in reason_text:
        return NoCandidateReasonCode.SIDE_DISABLED.value
    if (
        "cooldown" in reason_text
        or "entries_disabled" in reason_text
        or "operator_halt" in reason_text
        or extra.get("operator_halt") is True
    ):
        return NoCandidateReasonCode.COOLDOWN_ACTIVE.value
    if "entry_signal_filtered" in reason_text or "controls_not_satisfied" in reason_text:
        return NoCandidateReasonCode.MIN_EVIDENCE_NOT_MET.value
    if str(payload.get("final_decision") or "").upper() in {
        NoTradeFinalDecision.GOVERNANCE_BLOCKED.value,
        NoTradeFinalDecision.EXPOSURE_BLOCKED.value,
        NoTradeFinalDecision.ROUTE_HELD_UNTIL_READINESS_CONVERGED.value,
    }:
        return NoCandidateReasonCode.SUBMIT_BLOCKED.value
    if "no_setup" in reason_text or payload.get("setup_detected") is False:
        return NoCandidateReasonCode.NO_SETUP.value
    if str(payload.get("final_decision") or "").upper() == NoTradeFinalDecision.FILTER_REJECTED.value:
        return NoCandidateReasonCode.MIN_EVIDENCE_NOT_MET.value
    return NoCandidateReasonCode.UNKNOWN.value


def is_no_candidate_observation(payload: Mapping[str, Any]) -> bool:
    decision = str(payload.get("final_decision") or "").upper()
    if decision in _TERMINAL_CANDIDATE_DECISIONS:
        return False
    if payload.get("order_intent_id"):
        return False
    return True


def write_no_candidate_summary(
    payload: Mapping[str, Any],
    *,
    repo_root: Path | str = Path("."),
    diagnostics_root: Path | str | None = None,
) -> dict[str, Path]:
    root = Path(diagnostics_root) if diagnostics_root is not None else Path(repo_root) / NO_TRADE_DIAGNOSTICS_ROOT
    root.mkdir(parents=True, exist_ok=True)
    latest_path = root / "latest_no_candidate_summary.json"
    jsonl_path = root / "no_candidate_summary.jsonl"
    record = to_jsonable(dict(payload))
    tmp_path = latest_path.with_name(f".{latest_path.name}.{os.getpid()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2, sort_keys=True)
            handle.write("\n")
        tmp_path.replace(latest_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    append_bounded_jsonl(jsonl_path, record)
    return {"latest": latest_path, "jsonl": jsonl_path}


def _build_no_candidate_summary(
    state: Mapping[str, Any],
    *,
    generated_at: datetime,
    window_seconds: int,
) -> dict[str, Any]:
    reason_counts = dict(sorted(state["reason_counts"].items()))
    return {
        "schema_version": "track_b_no_candidate_observability_v1",
        "generated_at": generated_at.isoformat(),
        "lane_id": state["lane_id"],
        "symbol": state["symbol"],
        "window": {
            "started_at": state["window_started_at"].isoformat(),
            "ended_at": state["window_ended_at"].isoformat(),
            "duration_seconds": window_seconds,
        },
        "bars_evaluated": state["bars_evaluated"],
        "primary_no_candidate_reason_counts": reason_counts,
        "latest_evaluation_timestamp": state["latest_evaluation_timestamp"].isoformat(),
        "latest_bar_id": state["latest_bar_id"],
        "session_state": state["session_state"],
        "warmup_min_evidence_state": state["warmup_min_evidence_state"],
        "cooldown_hold_state": state["cooldown_hold_state"],
        "live_money_eligible": False,
    }


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text) if text else datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_no_trade_diagnostic(
    *,
    lane_id: str,
    symbol: str,
    session: str,
    bar_timestamp: datetime | str,
    bar_id: str | None = None,
    session_allowed: bool | None = None,
    market_data_fresh: bool | None = None,
    strategy_evaluated: bool = False,
    signal_packet: Any | None = None,
    setup_detected: bool | None = None,
    blocker_reason: str | None = None,
    submit_blocker: str | None = None,
    final_decision: str | NoTradeFinalDecision | None = None,
    order_intent_created: bool = False,
    would_route: bool = False,
    order_intent_id: str | None = None,
    runtime_identity: Mapping[str, Any] | None = None,
    extra: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build one diagnostic record for a lane/bar decision.

    ``signal_packet`` is treated as observed evidence only. The predicate lists
    expose existing booleans without recomputing thresholds.
    """

    generated = generated_at or datetime.now(timezone.utc)
    predicates_passed, predicates_failed = extract_signal_predicates(signal_packet)
    inferred_setup = bool(predicates_passed) if setup_detected is None else bool(setup_detected)
    normalized_blocker = _normalize_blocker(blocker_reason, submit_blocker)
    decision = _classify_final_decision(
        final_decision=final_decision,
        blocker_reason=normalized_blocker,
        setup_detected=inferred_setup,
        order_intent_created=order_intent_created,
        would_route=would_route,
    )
    near_miss_score = _near_miss_score(predicates_passed, predicates_failed)
    relaxation = _safe_relaxation_hint(
        decision=decision,
        blocker_reason=normalized_blocker,
        predicates_passed=predicates_passed,
        predicates_failed=predicates_failed,
    )

    payload: dict[str, Any] = {
        "schema_version": "track_b_no_trade_diagnostic_v1",
        "generated_at": generated.isoformat(),
        "lane_id": str(lane_id or ""),
        "symbol": str(symbol or "").upper(),
        "session": str(session or "UNKNOWN"),
        "bar_timestamp": _isoformat(bar_timestamp),
        "bar_id": bar_id,
        "session_allowed": session_allowed,
        "market_data_fresh": market_data_fresh,
        "strategy_evaluated": bool(strategy_evaluated),
        "setup_detected": inferred_setup,
        "predicates_passed": predicates_passed,
        "predicates_failed": predicates_failed,
        "blocker_reason": normalized_blocker,
        "near_miss_score": near_miss_score,
        "near_miss_score_source": "signal_predicate_ratio" if near_miss_score is not None else None,
        "would_trade_if_relaxed": relaxation,
        "final_decision": decision.value,
        "order_intent_id": order_intent_id,
        "live_money_eligible": False,
    }
    if runtime_identity:
        payload["runtime_identity"] = {str(key): to_jsonable(value) for key, value in runtime_identity.items()}
    if extra:
        payload["extra"] = to_jsonable(dict(extra))
    return payload


def extract_signal_predicates(signal_packet: Any | None) -> tuple[list[str], list[str]]:
    if signal_packet is None:
        return [], []
    if is_dataclass(signal_packet):
        raw = asdict(signal_packet)
    elif isinstance(signal_packet, Mapping):
        raw = dict(signal_packet)
    else:
        raw = {
            key: getattr(signal_packet, key)
            for key in dir(signal_packet)
            if not key.startswith("_") and not callable(getattr(signal_packet, key))
        }
    passed: list[str] = []
    failed: list[str] = []
    for key, value in sorted(raw.items()):
        if not isinstance(value, bool) or not _is_signal_predicate_name(str(key)):
            continue
        if value:
            passed.append(str(key))
        else:
            failed.append(str(key))
    return passed, failed


def write_no_trade_diagnostic(
    payload: Mapping[str, Any],
    *,
    repo_root: Path | str = Path("."),
    diagnostics_root: Path | str | None = None,
) -> dict[str, Path]:
    root = Path(diagnostics_root) if diagnostics_root is not None else Path(repo_root) / NO_TRADE_DIAGNOSTICS_ROOT
    root.mkdir(parents=True, exist_ok=True)
    latest_path = root / "latest_no_trade_diagnostics.json"
    jsonl_path = root / "no_trade_diagnostics.jsonl"
    record = to_jsonable(dict(payload))
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True))
        handle.write("\n")
    tmp_path = latest_path.with_name(f".{latest_path.name}.{os.getpid()}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2, sort_keys=True)
            handle.write("\n")
        tmp_path.replace(latest_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
    try_record_strategy_funnel_events(events_from_no_trade_diagnostic(record), repo_root=repo_root)
    return {"latest": latest_path, "jsonl": jsonl_path}


def diagnostics_root_from_artifact_dir(artifact_dir: Path | str | None) -> Path:
    if artifact_dir is None:
        return Path.cwd() / NO_TRADE_DIAGNOSTICS_ROOT
    artifact_path = Path(artifact_dir)
    for candidate in (artifact_path, *artifact_path.parents):
        if candidate.name == "outputs":
            return candidate / "track_b_execution_core" / "no_trade_diagnostics"
    return Path.cwd() / NO_TRADE_DIAGNOSTICS_ROOT


def _classify_final_decision(
    *,
    final_decision: str | NoTradeFinalDecision | None,
    blocker_reason: str | None,
    setup_detected: bool,
    order_intent_created: bool,
    would_route: bool,
) -> NoTradeFinalDecision:
    if final_decision is not None:
        return (
            final_decision
            if isinstance(final_decision, NoTradeFinalDecision)
            else NoTradeFinalDecision(str(final_decision).strip().upper())
        )
    if order_intent_created:
        return NoTradeFinalDecision.ORDER_INTENT_CREATED
    if would_route:
        return NoTradeFinalDecision.WOULD_ROUTE
    if blocker_reason:
        if not setup_detected and "no_setup" in blocker_reason.lower():
            return NoTradeFinalDecision.NO_SETUP
        if any(token in blocker_reason.lower() for token in _EXPOSURE_BLOCKER_TOKENS):
            return NoTradeFinalDecision.EXPOSURE_BLOCKED
        if setup_detected and _looks_like_governance_blocker(blocker_reason):
            return NoTradeFinalDecision.GOVERNANCE_BLOCKED
        return NoTradeFinalDecision.FILTER_REJECTED
    return NoTradeFinalDecision.FILTER_REJECTED if setup_detected else NoTradeFinalDecision.NO_SETUP


def _looks_like_governance_blocker(reason: str) -> bool:
    lowered = reason.lower()
    return any(
        token in lowered
        for token in (
            "authority",
            "bridge",
            "broker",
            "gate",
            "governance",
            "monitor",
            "reconciliation",
            "submit",
        )
    )


def _is_signal_predicate_name(name: str) -> bool:
    if name in {"long_entry", "short_entry", "recent_long_setup", "recent_short_setup"}:
        return True
    return any(name.endswith(suffix) for suffix in _PREDICATE_SUFFIXES)


def _near_miss_score(predicates_passed: list[str], predicates_failed: list[str]) -> float | None:
    total = len(predicates_passed) + len(predicates_failed)
    if total <= 0:
        return None
    return round(len(predicates_passed) / total, 4)


def _safe_relaxation_hint(
    *,
    decision: NoTradeFinalDecision,
    blocker_reason: str | None,
    predicates_passed: list[str],
    predicates_failed: list[str],
) -> dict[str, Any]:
    if decision in {NoTradeFinalDecision.ORDER_INTENT_CREATED, NoTradeFinalDecision.WOULD_ROUTE}:
        return {"safe_to_infer": False, "reason": "order_path_already_reached"}
    if not predicates_passed:
        return {"safe_to_infer": False, "reason": "no_setup_predicates_passed"}
    return {
        "safe_to_infer": True,
        "blocked_by": blocker_reason,
        "failed_predicate_count": len(predicates_failed),
        "passed_predicate_count": len(predicates_passed),
        "relaxation_required": "predicate_or_gate_relaxation_not_applied",
    }


def _normalize_blocker(blocker_reason: str | None, submit_blocker: str | None) -> str | None:
    for value in (submit_blocker, blocker_reason):
        text = str(value or "").strip()
        if text:
            return text
    return None


def _isoformat(value: datetime | str) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)
