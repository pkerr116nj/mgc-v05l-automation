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
    ready = not failures
    return {
        "classification": (
            "TRACK_B_PHASE1_BROKER_RECONCILIATION_SUBMIT_GATE_READY"
            if ready
            else "TRACK_B_PHASE1_BROKER_RECONCILIATION_SUBMIT_GATE_BLOCKED"
        ),
        "ready": ready,
        "block_reasons": failures,
        "detail": (
            "Current Phase-1 broker reconciliation is fresh and clean; route/governance/exposure gates still apply."
            if ready
            else f"Current Phase-1 broker reconciliation blocks submit: {', '.join(failures)}"
        ),
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
