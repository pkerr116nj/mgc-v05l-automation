"""Per-lane Track B PAPER no-trade diagnostics.

The helpers in this module are pure/read-only except for the explicit artifact
writer. They do not evaluate strategy thresholds or call broker/order APIs.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from .models import to_jsonable


NO_TRADE_DIAGNOSTICS_ROOT = Path("outputs/track_b_execution_core/no_trade_diagnostics")
LATEST_NO_TRADE_DIAGNOSTICS_JSON = NO_TRADE_DIAGNOSTICS_ROOT / "latest_no_trade_diagnostics.json"
NO_TRADE_DIAGNOSTICS_JSONL = NO_TRADE_DIAGNOSTICS_ROOT / "no_trade_diagnostics.jsonl"


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
