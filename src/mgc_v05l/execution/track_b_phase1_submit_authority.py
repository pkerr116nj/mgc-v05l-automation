"""Fail-closed Track B Phase-1 PAPER submit authority checks."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_RECONCILIATION_PATH = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_MAX_AGE_SECONDS = 120.0


def evaluate_phase1_broker_reconciliation_submit_gate(
    *,
    repo_root: Path,
    reconciliation_path: Path = DEFAULT_RECONCILIATION_PATH,
    max_age_seconds: float = DEFAULT_MAX_AGE_SECONDS,
    candidate_symbol: str | None = None,
) -> dict[str, Any]:
    """Return whether current Phase-1 broker truth is safe enough to consider submit.

    This gate never grants submit authority by itself. It only answers whether
    current broker/lifecycle/open-order/review-required state is fresh and clean.
    Route, strategy, exposure, and bridge checks must still pass separately.
    """

    path = repo_root / reconciliation_path if not reconciliation_path.is_absolute() else reconciliation_path
    payload = _load_json(path)
    failures: list[str] = []
    if not payload:
        failures.append("phase1_broker_reconciliation_missing")
    generated_at_value = (
        payload.get("generated_at")
        or payload.get("snapshot_time")
        or payload.get("broker_truth_snapshot_time")
        or payload.get("source_snapshot_time")
    )
    generated_at = _parse_datetime(generated_at_value)
    age_seconds: float | None = None
    if generated_at is None:
        failures.append("phase1_broker_reconciliation_timestamp_missing")
    else:
        age_seconds = max((datetime.now(timezone.utc) - generated_at).total_seconds(), 0.0)
        if age_seconds > float(max_age_seconds):
            failures.append("phase1_broker_reconciliation_stale")
    if str(payload.get("classification") or "") != "TRACK_B_PAPER_BROKER_RECONCILED":
        failures.append("phase1_broker_reconciliation_not_reconciled")
    if payload.get("broker_reconciled") is not True:
        failures.append("phase1_broker_reconciled_false")
    if _int_value(payload.get("review_required_count")) != 0:
        failures.append("phase1_review_required_present")
    if _int_value(payload.get("track_b_broker_open_order_count") or payload.get("open_order_count")) != 0:
        failures.append("phase1_open_orders_present")
    if bool(payload.get("live_money_eligible")):
        failures.append("phase1_live_money_eligible_true")
    if list(payload.get("blockers") or []):
        failures.append("phase1_broker_reconciliation_blockers_present")
    for reason in list(payload.get("block_reasons") or []):
        normalized = str(reason or "").strip()
        if normalized:
            failures.append(normalized)

    failures = list(dict.fromkeys(failures))
    scoped = _symbol_scoped_reconciliation_status(payload=payload, candidate_symbol=candidate_symbol)
    blocking_failures = list(failures)
    if scoped["candidate_symbol_allowed"]:
        blocking_failures = [
            reason
            for reason in blocking_failures
            if reason
            not in {
                "phase1_broker_reconciliation_not_reconciled",
                "phase1_broker_reconciled_false",
                "phase1_review_required_present",
                "phase1_broker_reconciliation_blockers_present",
            }
        ]
    ready = not blocking_failures
    return {
        "classification": (
            "TRACK_B_PHASE1_BROKER_RECONCILIATION_SUBMIT_GATE_READY"
            if ready
            else "TRACK_B_PHASE1_BROKER_RECONCILIATION_SUBMIT_GATE_BLOCKED"
        ),
        "ready": ready,
        "block_reasons": blocking_failures,
        "diagnostic_block_reasons": failures if ready and failures else [],
        "detail": (
            "Current Phase-1 broker reconciliation is fresh and clean; route/governance/exposure gates still apply."
            if ready
            else f"Current Phase-1 broker reconciliation blocks submit: {', '.join(blocking_failures)}"
        ),
        "blocker_scope": "SYMBOL" if scoped["candidate_symbol_allowed"] and failures else ("GLOBAL" if failures else "NONE"),
        "candidate_symbol": scoped["candidate_symbol"],
        "blocked_entry_symbols": scoped["blocked_entry_symbols"],
        "symbol_scoped_blockers": scoped["symbol_scoped_blockers"],
        "path": str(path),
        "generated_at": generated_at_value,
        "age_seconds": age_seconds,
        "max_age_seconds": float(max_age_seconds),
        "broker_reconciled": payload.get("broker_reconciled"),
        "review_required_count": _int_value(payload.get("review_required_count")),
        "track_b_broker_open_order_count": _int_value(
            payload.get("track_b_broker_open_order_count") or payload.get("open_order_count")
        ),
        "track_b_broker_position_count": payload.get("track_b_broker_position_count"),
        "track_b_broker_positions": list(payload.get("track_b_broker_positions") or []),
        "track_b_lifecycle_positions": list(payload.get("track_b_lifecycle_positions") or []),
        "live_money_eligible": bool(payload.get("live_money_eligible")),
        "source": "TRACK_B_PHASE1_BROKER_RECONCILIATION",
    }


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _symbol_scoped_reconciliation_status(*, payload: dict[str, Any], candidate_symbol: str | None) -> dict[str, Any]:
    candidate = _canonical_symbol(candidate_symbol)
    blocked_symbols = _blocked_symbols_from_current_broker_positions(payload)
    global_reasons: list[str] = []
    if _int_value(payload.get("unknown_broker_open_order_count")):
        global_reasons.append("unknown_open_orders")
    if _int_value(payload.get("track_b_broker_open_order_count") or payload.get("open_order_count")):
        global_reasons.append("open_orders_present")
    if bool(payload.get("live_money_eligible")):
        global_reasons.append("live_money_eligible")
    for blocker in list(payload.get("blockers") or []):
        if not isinstance(blocker, dict):
            global_reasons.append("unclassified_blocker")
            continue
        code = str(blocker.get("code") or "").upper()
        if "OPEN_ORDER" in code or "UNKNOWN_ORDER" in code or "LIVE_MONEY" in code:
            global_reasons.append(code.lower())
            continue
        symbols = _symbols_from_nested(blocker)
        if symbols and blocked_symbols and not symbols.issubset(blocked_symbols):
            continue
        if not symbols and not any(token in code for token in ("REGISTRY", "POSITION", "LIFECYCLE")):
            global_reasons.append(code.lower() or "unclassified_blocker")
    candidate_allowed = bool(candidate and blocked_symbols and candidate not in blocked_symbols and not global_reasons)
    return {
        "candidate_symbol": candidate,
        "candidate_symbol_allowed": candidate_allowed,
        "blocked_entry_symbols": sorted(blocked_symbols),
        "symbol_scoped_blockers": [
            {
                "scope": "SYMBOL",
                "symbol": symbol,
                "code": "phase1_reconciliation_symbol_position_blocker",
                "detail": "Current reconciliation has unresolved broker/lifecycle ownership for this symbol only.",
            }
            for symbol in sorted(blocked_symbols)
        ],
        "global_reasons": global_reasons,
    }


def _blocked_symbols_from_current_broker_positions(payload: dict[str, Any]) -> set[str]:
    rows = payload.get("track_b_broker_positions") or []
    symbols: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            qty = float(row.get("quantity") or row.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0.0
        if abs(qty) <= 1e-9:
            continue
        symbol = _canonical_symbol(row.get("symbol") or row.get("track_b_root") or row.get("instrument_family") or row.get("local_symbol"))
        if symbol:
            symbols.add(symbol)
    return symbols


def _symbols_from_nested(value: Any) -> set[str]:
    symbols: set[str] = set()
    if isinstance(value, dict):
        if any(key in value for key in ("symbol", "track_b_root", "instrument_family", "local_symbol")):
            symbol = _canonical_symbol(value.get("symbol") or value.get("track_b_root") or value.get("instrument_family") or value.get("local_symbol"))
            if symbol:
                symbols.add(symbol)
        for nested in value.values():
            symbols.update(_symbols_from_nested(nested))
    elif isinstance(value, list):
        for item in value:
            symbols.update(_symbols_from_nested(item))
    return symbols


def _canonical_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    letters = "".join(ch for ch in text if ch.isalpha())
    if text in {"MES", "MNQ", "MGC", "MET", "MSL", "MBT"}:
        return text
    for root in ("MNQ", "MES", "MGC", "MET", "MSL", "MBT", "NQ", "ES", "GC", "ZN", "ZB", "ZF", "ZT", "PL"):
        if letters.startswith(root):
            return root
    return letters[:3]
