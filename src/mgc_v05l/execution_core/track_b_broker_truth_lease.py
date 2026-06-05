"""Pure Track B PAPER broker-truth lease evaluator.

The evaluator consumes already-produced broker truth, reconciliation, lifecycle,
and order-intent summaries. It does not connect to brokers, execute repairs,
restart services, mutate lifecycle, or transmit orders.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic

LEASE_STATES = {
    "ACTIVE",
    "ACTIVE_DEGRADED_REFRESH_FAILING",
    "EXPIRED_BLOCK_NEW_ENTRIES",
    "EXPIRED_EXITS_ONLY",
    "INVALIDATED_CONTRADICTION",
    "INVALIDATED_UNKNOWN_OPEN_ORDERS",
    "INVALIDATED_MANUAL_BROKER_ACTION",
    "OPERATOR_REQUIRED",
}

CONNECTION_MODES = {
    "IBKR_CONNECTION_DOWN",
    "POSITION_TRUTH_ONLY",
    "ORDER_STATUS_UNRELIABLE",
    "SUBMIT_CAPABLE",
    "FILL_CALLBACK_CAPABLE",
    "DEGRADED_RECOVERED",
}

AUTHORITY_USE_NEW_ENTRY = "NEW_ENTRY_AUTHORITY"
AUTHORITY_USE_RISK_REDUCING_CLOSE = "RISK_REDUCING_CLOSE_AUTHORITY"
AUTHORITY_USE_BROKER_OBSERVED_ADOPTION = "BROKER_OBSERVED_ADOPTION_DIAGNOSIS"
AUTHORITY_USE_FILL_CALLBACK_ADOPTION = "FILL_CALLBACK_ADOPTION"
AUTHORITY_USE_STATUS_DIAGNOSTIC = "STATUS_DIAGNOSTIC"

DEFAULT_LEASE_ARTIFACT = Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
DEFAULT_LEASE_HISTORY = Path("outputs") / "operator_dashboard" / "runtime" / "broker_truth_lease_history.jsonl"


def classify_broker_truth_lease(inputs: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a broker-truth account safety lease from supplied artifacts."""

    current_time = _parse_time(inputs.get("current_time") or inputs.get("generated_at")) or datetime.now(timezone.utc)
    generated_at = current_time.isoformat()
    account_id = str(inputs.get("account_id") or "").strip()
    policy = _mapping(inputs.get("policy"))
    max_entry_age_seconds = _float(policy.get("max_entry_age_seconds"), 0.0)
    max_exit_age_seconds = _float(policy.get("max_exit_age_seconds"), max_entry_age_seconds)
    degraded_refresh_grace_seconds = _float(policy.get("degraded_refresh_grace_seconds"), max_entry_age_seconds)
    max_position_lease_age_seconds = _float(policy.get("max_position_lease_age_seconds"), max_exit_age_seconds)
    max_open_order_lease_age_seconds = _float(policy.get("max_open_order_lease_age_seconds"), max_exit_age_seconds)
    max_fill_evidence_lease_age_seconds = _float(policy.get("max_fill_evidence_lease_age_seconds"), max_exit_age_seconds)
    allowed_instruments = _string_set(inputs.get("allowed_instruments") or inputs.get("allowed_scope"))

    broker_truth = _mapping(inputs.get("last_successful_broker_truth") or inputs.get("broker_truth"))
    latest_attempt = _mapping(inputs.get("latest_attempt_status"))
    fill_evidence = _mapping(inputs.get("fill_evidence") or inputs.get("execution_fill_evidence"))
    reconciliation = _mapping(inputs.get("reconciliation") or inputs.get("phase1_reconciliation"))
    lifecycle = _mapping(inputs.get("lifecycle") or inputs.get("lifecycle_state"))
    order_state = _mapping(inputs.get("order_state") or inputs.get("order_intent_state") or inputs.get("open_order_state"))
    source_paths = _mapping(inputs.get("source_artifact_paths") or inputs.get("source_artifacts"))
    source_timestamps = dict(_mapping(inputs.get("source_artifact_timestamps") or {}))

    broker_truth_time = _parse_time(
        broker_truth.get("generated_at")
        or broker_truth.get("last_success_at")
        or broker_truth.get("latest_refresh_time")
        or broker_truth.get("completed_at")
    )
    entry_valid_until = _add_seconds(broker_truth_time, max_entry_age_seconds)
    exit_valid_until = _add_seconds(broker_truth_time, max_exit_age_seconds)
    valid_until = entry_valid_until
    broker_session_owner = _broker_session_owner(
        inputs=inputs,
        broker_truth=broker_truth,
        latest_attempt=latest_attempt,
        order_state=order_state,
        fill_evidence=fill_evidence,
        current_time=current_time,
    )
    connection_health = _connection_health(
        inputs=inputs,
        broker_truth=broker_truth,
        latest_attempt=latest_attempt,
        fill_evidence=fill_evidence,
        order_state=order_state,
        broker_session_owner=broker_session_owner,
        current_time=current_time,
        max_open_order_lease_age_seconds=max_open_order_lease_age_seconds,
        max_fill_evidence_lease_age_seconds=max_fill_evidence_lease_age_seconds,
    )
    connection_mode = str(connection_health["connection_mode"])

    builder = _LeaseBuilder(
        generated_at=generated_at,
        account_id=account_id,
        broker_truth=broker_truth,
        latest_attempt=latest_attempt,
        reconciliation=reconciliation,
        lifecycle=lifecycle,
        order_state=order_state,
        allowed_instruments=allowed_instruments,
        source_paths=source_paths,
        source_timestamps=source_timestamps,
    )

    live_money_sources = {
        "inputs": inputs,
        "broker_truth": broker_truth,
        "latest_attempt_status": latest_attempt,
        "reconciliation": reconciliation,
        "lifecycle": lifecycle,
        "order_state": order_state,
    }
    live_money_flag_present = any(_bool(_mapping(value).get("live_money_eligible")) for value in live_money_sources.values())
    if live_money_flag_present:
        builder.invalidate(
            "INVALIDATED_CONTRADICTION",
            "live_money_eligible_enabled",
            "Live-money eligibility is forbidden for Track B PAPER leases.",
            operator_action_required=True,
        )
    elif not broker_truth:
        builder.invalidate(
            "OPERATOR_REQUIRED",
            "broker_truth_missing",
            "Last successful broker truth is missing.",
            operator_action_required=True,
        )
    elif account_id and str(broker_truth.get("account") or broker_truth.get("account_id") or "").strip() not in {"", account_id}:
        builder.invalidate(
            "INVALIDATED_CONTRADICTION",
            "wrong_account",
            "Broker truth account does not match the configured PAPER account.",
            operator_action_required=True,
        )
    elif not _bool(broker_truth.get("positions_complete")):
        builder.invalidate(
            "OPERATOR_REQUIRED",
            "positions_incomplete",
            "Broker position truth is incomplete.",
            operator_action_required=True,
        )
    elif _unknown_open_orders(
        broker_truth=broker_truth,
        latest_attempt=latest_attempt,
        reconciliation=reconciliation,
        order_state=order_state,
        allowed_instruments=allowed_instruments,
    ):
        builder.invalidate(
            "INVALIDATED_UNKNOWN_OPEN_ORDERS",
            "unknown_open_orders",
            "Broker open-order truth is incomplete or unknown open orders are present.",
            operator_action_required=True,
        )
    elif _manual_broker_action_detected(lifecycle=lifecycle, reconciliation=reconciliation, order_state=order_state):
        builder.invalidate(
            "INVALIDATED_MANUAL_BROKER_ACTION",
            "manual_broker_action",
            "Manual broker action or stale lifecycle after manual broker action is unresolved.",
            operator_action_required=True,
        )
    else:
        latest_contradiction = _latest_success_contradiction(
            latest_attempt=latest_attempt,
            account_id=account_id,
            allowed_instruments=allowed_instruments,
            lifecycle=lifecycle,
            order_state=order_state,
        )
        broker_position_contradiction = _broker_position_contradiction(
            broker_truth=broker_truth,
            reconciliation=reconciliation,
            lifecycle=lifecycle,
            allowed_instruments=allowed_instruments,
        )
        if latest_contradiction:
            builder.invalidate(
                "INVALIDATED_CONTRADICTION",
                latest_contradiction["code"],
                latest_contradiction["detail"],
                operator_action_required=True,
            )
        elif broker_position_contradiction:
            builder.invalidate(
                "INVALIDATED_CONTRADICTION",
                broker_position_contradiction["code"],
                broker_position_contradiction["detail"],
                operator_action_required=True,
            )
        elif not _reconciliation_clean(reconciliation):
            builder.invalidate(
                "OPERATOR_REQUIRED",
                "reconciliation_not_clean",
                "Phase-1 reconciliation is not clean.",
                operator_action_required=True,
            )
        elif broker_truth_time is None or entry_valid_until is None or exit_valid_until is None:
            builder.invalidate(
                "OPERATOR_REQUIRED",
                "broker_truth_time_missing",
                "Broker truth generated_at/last_success_at timestamp is missing or invalid.",
                operator_action_required=True,
            )
        elif current_time <= entry_valid_until:
            if _latest_attempt_failed_after_success(latest_attempt=latest_attempt, broker_truth_time=broker_truth_time):
                builder.state = "ACTIVE_DEGRADED_REFRESH_FAILING"
                builder.warn(
                    "broker_truth_refresh_failing",
                    "Latest broker-truth refresh failed; active lease is preserved until entry validity expires.",
                )
                latest_time = _parse_time(latest_attempt.get("generated_at"))
                if latest_time is not None and current_time > latest_time + timedelta(seconds=degraded_refresh_grace_seconds):
                    builder.warn(
                        "broker_truth_refresh_failure_grace_exceeded",
                        "Latest broker-truth refresh failure is older than degraded refresh grace.",
                    )
            else:
                builder.state = "ACTIVE"
        elif _lifecycle_has_owned_position(lifecycle) and current_time <= exit_valid_until:
            builder.state = "EXPIRED_EXITS_ONLY"
            builder.block("entry_lease_expired", "Entry lease expired; new entries are blocked.")
        else:
            builder.state = "EXPIRED_BLOCK_NEW_ENTRIES"
            builder.block("entry_lease_expired", "Entry lease expired; new entries are blocked.")

    broker_position_lease = _evidence_lease(
        lease_type="broker_position_lease",
        generated_at=broker_truth_time,
        current_time=current_time,
        max_age_seconds=max_position_lease_age_seconds,
        source=broker_truth.get("positions_snapshot_path") or source_paths.get("positions") or source_paths.get("broker_truth"),
        complete=_bool(broker_truth.get("positions_complete")),
        base_authority_uses=(
            AUTHORITY_USE_NEW_ENTRY,
            AUTHORITY_USE_RISK_REDUCING_CLOSE,
            AUTHORITY_USE_BROKER_OBSERVED_ADOPTION,
        ),
    )
    broker_open_order_lease = _evidence_lease(
        lease_type="broker_open_order_lease",
        generated_at=_parse_time(
            broker_truth.get("open_orders_generated_at")
            or broker_truth.get("orders_generated_at")
            or broker_truth.get("generated_at")
            or broker_truth.get("last_success_at")
        ),
        current_time=current_time,
        max_age_seconds=max_open_order_lease_age_seconds,
        source=broker_truth.get("open_orders_snapshot_path") or source_paths.get("open_orders") or source_paths.get("broker_truth"),
        complete=_bool(broker_truth.get("open_orders_complete")),
        base_authority_uses=(AUTHORITY_USE_NEW_ENTRY, AUTHORITY_USE_RISK_REDUCING_CLOSE),
    )
    fill_evidence_time = _parse_time(
        fill_evidence.get("generated_at")
        or fill_evidence.get("last_callback_at")
        or fill_evidence.get("latest_fill_at")
        or fill_evidence.get("latest_execution_at")
    )
    execution_fill_evidence_lease = _evidence_lease(
        lease_type="execution_fill_evidence_lease",
        generated_at=fill_evidence_time,
        current_time=current_time,
        max_age_seconds=max_fill_evidence_lease_age_seconds,
        source=fill_evidence.get("source_path") or source_paths.get("fill_evidence") or source_paths.get("executions"),
        complete=_fill_evidence_complete(fill_evidence),
        base_authority_uses=(AUTHORITY_USE_FILL_CALLBACK_ADOPTION,),
    )
    submit_ownership_available = _submit_ownership_available(
        inputs=inputs,
        reconciliation=reconciliation,
        order_state=order_state,
    )
    current_scope_clean = _current_scope_review_required_count(reconciliation) == 0

    submit_entry_allowed = builder.state in {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"} and valid_until is not None and current_time <= valid_until
    submit_exit_allowed = (
        builder.state in {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
        and exit_valid_until is not None
        and current_time <= exit_valid_until
    ) or (builder.state == "EXPIRED_EXITS_ONLY" and exit_valid_until is not None and current_time <= exit_valid_until)

    if builder.state.startswith("INVALIDATED") or builder.state == "OPERATOR_REQUIRED":
        submit_entry_allowed = False
        submit_exit_allowed = False
    if builder.state == "EXPIRED_BLOCK_NEW_ENTRIES":
        submit_entry_allowed = False
        submit_exit_allowed = False
    if builder.state == "EXPIRED_EXITS_ONLY":
        submit_entry_allowed = False

    authority_use_blockers = _authority_use_blockers(
        connection_mode=connection_mode,
        broker_position_lease=broker_position_lease,
        broker_open_order_lease=broker_open_order_lease,
        reconciliation_clean=_reconciliation_clean(reconciliation),
    )
    submit_connection_capable = _connection_allows_submit(connection_mode)
    submit_entry_allowed = bool(
        submit_entry_allowed
        and submit_connection_capable
        and _lease_allows(broker_position_lease, AUTHORITY_USE_NEW_ENTRY)
        and _lease_allows(broker_open_order_lease, AUTHORITY_USE_NEW_ENTRY)
        and _reconciliation_clean(reconciliation)
    )
    submit_exit_allowed = bool(
        submit_exit_allowed
        and submit_connection_capable
        and _lease_allows(broker_position_lease, AUTHORITY_USE_RISK_REDUCING_CLOSE)
        and _lease_allows(broker_open_order_lease, AUTHORITY_USE_RISK_REDUCING_CLOSE)
    )
    broker_observed_adoption_allowed = bool(
        _lease_allows(broker_position_lease, AUTHORITY_USE_BROKER_OBSERVED_ADOPTION)
        and submit_ownership_available
        and current_scope_clean
        and not live_money_flag_present
        and not _bool(inputs.get("paper_proof_invoked"))
        and not any(_bool(_mapping(value).get("paper_proof_invoked")) for value in live_money_sources.values())
    )
    allowed_uses = {
        "new_entry": submit_entry_allowed,
        "managed_risk_reducing_close": submit_exit_allowed,
        "broker_observed_adoption_diagnosis": broker_observed_adoption_allowed,
        "fill_callback_adoption": _lease_allows(execution_fill_evidence_lease, AUTHORITY_USE_FILL_CALLBACK_ADOPTION),
        "status_diagnostic": True,
    }

    payload = {
        "schema_version": "track_b_broker_truth_lease_v1",
        "lease_id": _lease_id(account_id=account_id, broker_truth=broker_truth, reconciliation=reconciliation),
        "account_id": account_id,
        "mode": "PAPER",
        "paper_only": True,
        "live_money_eligible": False,
        "generated_at": generated_at,
        "broker_truth_generated_at": _iso_or_none(broker_truth_time),
        "reconciliation_generated_at": reconciliation.get("generated_at"),
        "valid_until": _iso_or_none(valid_until),
        "entry_valid_until": _iso_or_none(entry_valid_until),
        "exit_valid_until": _iso_or_none(exit_valid_until),
        "max_entry_age_seconds": max_entry_age_seconds,
        "max_exit_age_seconds": max_exit_age_seconds,
        "degraded_refresh_grace_seconds": degraded_refresh_grace_seconds,
        "lease_state": builder.state,
        "connection_health": connection_health,
        "connection_mode": connection_mode,
        "broker_session_owner": broker_session_owner,
        "broker_position_lease": broker_position_lease,
        "broker_open_order_lease": broker_open_order_lease,
        "execution_fill_evidence_lease": execution_fill_evidence_lease,
        "allowed_uses": allowed_uses,
        "authority_use_blockers": authority_use_blockers,
        "positions_snapshot_path": broker_truth.get("positions_snapshot_path"),
        "open_orders_snapshot_path": broker_truth.get("open_orders_snapshot_path"),
        "broker_truth_status_path": source_paths.get("broker_truth_status") or source_paths.get("broker_truth"),
        "reconciliation_path": source_paths.get("reconciliation"),
        "positions": _scoped_positions(broker_truth, allowed_instruments),
        "open_orders": _scoped_open_orders(broker_truth, allowed_instruments),
        "track_b_broker_position_count": int(reconciliation.get("track_b_broker_position_count") or 0),
        "track_b_broker_open_order_count": int(reconciliation.get("track_b_broker_open_order_count") or 0),
        "unknown_broker_open_order_count": int(
            reconciliation.get("unknown_broker_open_order_count")
            or order_state.get("unknown_open_order_count")
            or 0
        ),
        "lifecycle_open_position_count": int(
            reconciliation.get("lifecycle_open_position_count")
            or lifecycle.get("open_position_count")
            or len(lifecycle.get("open_positions") or [])
            or 0
        ),
        "lifecycle_open_order_count": int(
            reconciliation.get("lifecycle_open_order_count")
            or lifecycle.get("open_order_count")
            or order_state.get("lifecycle_open_order_count")
            or 0
        ),
        "review_required_count": _current_scope_review_required_count(reconciliation),
        "current_scope_review_required_count": _current_scope_review_required_count(reconciliation),
        "historical_review_required_count": int(reconciliation.get("historical_review_required_count") or 0),
        "broker_reconciled": _bool(reconciliation.get("broker_reconciled")),
        "lifecycle_match_status": lifecycle.get("match_status") or reconciliation.get("position_match_report", {}).get("state"),
        "order_intent_match_status": order_state.get("match_status")
        or reconciliation.get("submit_intent_ownership_reconciliation", {}).get("classification"),
        "allowed_instruments": sorted(allowed_instruments),
        "allowed_contracts": list(inputs.get("allowed_contracts") or []),
        "allowed_lane_ids": list(inputs.get("allowed_lane_ids") or []),
        "submit_entry_allowed": bool(submit_entry_allowed),
        "submit_exit_allowed": bool(submit_exit_allowed),
        "warnings": list(builder.warnings),
        "blockers": list(builder.blockers),
        "contradiction_details": list(builder.contradiction_details),
        "operator_action_required": bool(builder.operator_action_required),
        "source_artifact_paths": dict(source_paths),
        "source_artifact_timestamps": _source_timestamps(
            explicit=source_timestamps,
            broker_truth=broker_truth,
            latest_attempt=latest_attempt,
            reconciliation=reconciliation,
            lifecycle=lifecycle,
            order_state=order_state,
        ),
        "latest_attempt_classification": latest_attempt.get("classification"),
        "latest_attempt_generated_at": latest_attempt.get("generated_at"),
        "latest_attempt_error": latest_attempt.get("last_error") or latest_attempt.get("error"),
    }
    return payload


def write_broker_truth_lease(
    *,
    output_path: Path,
    lease: Mapping[str, Any],
    history_path: Path | None = None,
) -> None:
    """Write the lease artifact and optional JSONL history; no actions are executed."""

    path = Path(output_path)
    write_json_atomic(path, lease)

    if history_path is not None:
        history = Path(history_path)
        history.parent.mkdir(parents=True, exist_ok=True)
        with history.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(dict(lease), sort_keys=True) + "\n")


class _LeaseBuilder:
    def __init__(
        self,
        *,
        generated_at: str,
        account_id: str,
        broker_truth: Mapping[str, Any],
        latest_attempt: Mapping[str, Any],
        reconciliation: Mapping[str, Any],
        lifecycle: Mapping[str, Any],
        order_state: Mapping[str, Any],
        allowed_instruments: set[str],
        source_paths: Mapping[str, Any],
        source_timestamps: Mapping[str, Any],
    ) -> None:
        self.generated_at = generated_at
        self.account_id = account_id
        self.broker_truth = broker_truth
        self.latest_attempt = latest_attempt
        self.reconciliation = reconciliation
        self.lifecycle = lifecycle
        self.order_state = order_state
        self.allowed_instruments = allowed_instruments
        self.source_paths = source_paths
        self.source_timestamps = source_timestamps
        self.state = "OPERATOR_REQUIRED"
        self.warnings: list[dict[str, str]] = []
        self.blockers: list[dict[str, str]] = []
        self.contradiction_details: list[dict[str, str]] = []
        self.operator_action_required = False

    def warn(self, code: str, detail: str) -> None:
        if not any(row.get("code") == code for row in self.warnings):
            self.warnings.append({"code": code, "detail": detail})

    def block(self, code: str, detail: str) -> None:
        if not any(row.get("code") == code for row in self.blockers):
            self.blockers.append({"code": code, "detail": detail})

    def invalidate(self, state: str, code: str, detail: str, *, operator_action_required: bool) -> None:
        self.state = state if state in LEASE_STATES else "OPERATOR_REQUIRED"
        self.block(code, detail)
        self.contradiction_details.append({"code": code, "detail": detail})
        self.operator_action_required = operator_action_required


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return False


def _float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _first_present(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _parse_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _add_seconds(value: datetime | None, seconds: float) -> datetime | None:
    if value is None:
        return None
    return value + timedelta(seconds=max(0.0, seconds))


def _iso_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _iso_text(value: Any) -> str | None:
    parsed = _parse_time(value)
    if parsed is not None:
        return parsed.isoformat()
    return str(value) if value not in (None, "") else None


def _string_set(value: Any) -> set[str]:
    if isinstance(value, str):
        return {value.strip().upper()} if value.strip() else set()
    if isinstance(value, Sequence):
        return {str(item).strip().upper() for item in value if str(item).strip()}
    return set()


def _symbol(row: Mapping[str, Any]) -> str:
    explicit = row.get("symbol") or row.get("internal_symbol") or row.get("broker_symbol") or row.get("track_b_root") or row.get("instrument_family")
    if explicit:
        return str(explicit).strip().upper()
    local_symbol = str(row.get("local_symbol") or "").strip().upper()
    return "".join(ch for ch in local_symbol if ch.isalpha())[:3]


def _quantity(row: Mapping[str, Any]) -> float:
    return _float(row.get("quantity", row.get("position", row.get("qty", 0))), 0.0)


def _in_scope(row: Mapping[str, Any], allowed_instruments: set[str]) -> bool:
    symbol = _symbol(row)
    return not allowed_instruments or symbol in allowed_instruments


def _scoped_positions(broker_truth: Mapping[str, Any], allowed_instruments: set[str]) -> list[dict[str, Any]]:
    positions = broker_truth.get("positions") or broker_truth.get("track_b_broker_positions") or []
    return [dict(row) for row in positions if isinstance(row, Mapping) and _in_scope(row, allowed_instruments)]


def _scoped_open_orders(broker_truth: Mapping[str, Any], allowed_instruments: set[str]) -> list[dict[str, Any]]:
    open_orders = broker_truth.get("open_orders") or broker_truth.get("track_b_broker_open_orders") or []
    return [dict(row) for row in open_orders if isinstance(row, Mapping) and _in_scope(row, allowed_instruments)]


def _unknown_open_orders(
    *,
    broker_truth: Mapping[str, Any],
    latest_attempt: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    order_state: Mapping[str, Any],
    allowed_instruments: set[str],
) -> bool:
    if not _bool(broker_truth.get("open_orders_complete")):
        return True
    if int(reconciliation.get("unknown_broker_open_order_count") or order_state.get("unknown_open_order_count") or 0) > 0:
        return True
    for row in _scoped_open_orders(broker_truth, allowed_instruments):
        if not _bool(row.get("known") if "known" in row else row.get("owned", True)):
            return True
    if _latest_attempt_success(latest_attempt) and not _bool(latest_attempt.get("open_orders_complete")):
        return True
    for row in _scoped_open_orders(latest_attempt, allowed_instruments):
        if not _bool(row.get("known") if "known" in row else row.get("owned", True)):
            return True
    return False


def _manual_broker_action_detected(
    *,
    lifecycle: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    order_state: Mapping[str, Any],
) -> bool:
    flags = (
        lifecycle.get("manual_broker_action_detected"),
        lifecycle.get("manual_close_detected"),
        lifecycle.get("stale_after_manual_close"),
        reconciliation.get("manual_broker_action_detected"),
        reconciliation.get("manual_close_detected"),
        order_state.get("manual_broker_action_detected"),
    )
    return any(_bool(flag) for flag in flags)


def _latest_attempt_success(latest_attempt: Mapping[str, Any]) -> bool:
    classification = str(latest_attempt.get("classification") or latest_attempt.get("verifier_classification") or "").upper()
    if not classification:
        return _bool(latest_attempt.get("last_success")) and not _bool(latest_attempt.get("last_failure"))
    if any(token in classification for token in ("FAILED", "BLOCKED", "TIMEOUT", "INCOMPLETE")):
        return False
    return any(token in classification for token in ("READY", "CONNECTED", "RECONCILED")) or _bool(latest_attempt.get("last_success"))


def _latest_attempt_failed_after_success(*, latest_attempt: Mapping[str, Any], broker_truth_time: datetime) -> bool:
    classification = str(latest_attempt.get("classification") or latest_attempt.get("verifier_classification") or "").upper()
    failed = _bool(latest_attempt.get("last_failure")) or any(
        token in classification for token in ("FAILED", "BLOCKED", "TIMEOUT", "INCOMPLETE")
    )
    latest_time = _parse_time(latest_attempt.get("generated_at"))
    return bool(failed and latest_time is not None and latest_time >= broker_truth_time)


def _latest_success_contradiction(
    *,
    latest_attempt: Mapping[str, Any],
    account_id: str,
    allowed_instruments: set[str],
    lifecycle: Mapping[str, Any],
    order_state: Mapping[str, Any],
) -> dict[str, str] | None:
    if not _latest_attempt_success(latest_attempt):
        return None
    latest_account = str(latest_attempt.get("account") or latest_attempt.get("account_id") or "").strip()
    if account_id and latest_account and latest_account != account_id:
        return {"code": "wrong_account", "detail": "Latest successful broker truth uses a different account."}
    if int(latest_attempt.get("unknown_broker_open_order_count") or order_state.get("unknown_open_order_count") or 0) > 0:
        return {"code": "unknown_open_orders", "detail": "Latest successful broker truth reports unknown open orders."}
    positions = [row for row in _scoped_positions(latest_attempt, allowed_instruments) if abs(_quantity(row)) > 1e-9]
    if positions and not _lifecycle_positions_match(positions=positions, lifecycle=lifecycle):
        return {"code": "unexpected_broker_position", "detail": "Latest successful broker truth reports unexpected position."}
    return None


def _broker_position_contradiction(
    *,
    broker_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    allowed_instruments: set[str],
) -> dict[str, str] | None:
    nonzero_positions = [row for row in _scoped_positions(broker_truth, allowed_instruments) if abs(_quantity(row)) > 1e-9]
    if not nonzero_positions:
        if _lifecycle_has_owned_position(lifecycle) and not _reconciliation_clean(reconciliation):
            return {
                "code": "lifecycle_broker_position_mismatch",
                "detail": "Lifecycle reports an owned position but broker truth is flat and reconciliation is not clean.",
            }
        return None
    if not _lifecycle_positions_match(positions=nonzero_positions, lifecycle=lifecycle):
        return {"code": "unexpected_broker_position", "detail": "Broker truth reports an unexpected in-scope position."}
    return None


def _lifecycle_has_owned_position(lifecycle: Mapping[str, Any]) -> bool:
    if int(lifecycle.get("owned_open_position_count") or lifecycle.get("open_position_count") or 0) > 0:
        return True
    return any(isinstance(row, Mapping) and _bool(row.get("owned", True)) for row in lifecycle.get("open_positions") or [])


def _lifecycle_positions_match(*, positions: Sequence[Mapping[str, Any]], lifecycle: Mapping[str, Any]) -> bool:
    lifecycle_positions = [row for row in lifecycle.get("open_positions") or [] if isinstance(row, Mapping)]
    if not lifecycle_positions:
        return False
    for broker_row in positions:
        symbol = _symbol(broker_row)
        quantity = _quantity(broker_row)
        matched = False
        for lifecycle_row in lifecycle_positions:
            if _symbol(lifecycle_row) == symbol and abs(_quantity(lifecycle_row) - quantity) <= 1e-9:
                matched = True
                break
        if not matched:
            return False
    return True


def _reconciliation_clean(reconciliation: Mapping[str, Any]) -> bool:
    clean_classifications = {
        "BROKER_LIFECYCLE_RECONCILED",
        "TRACK_B_PAPER_BROKER_RECONCILED",
        "TRACK_B_PAPER_BROKER_RECONCILED_WITH_KNOWN_MANAGED_EXIT_ORDER",
    }
    return (
        str(reconciliation.get("classification") or "") in clean_classifications
        and _bool(reconciliation.get("broker_reconciled"))
        and _current_scope_review_required_count(reconciliation) == 0
    )


def _current_scope_review_required_count(reconciliation: Mapping[str, Any]) -> int:
    if "current_scope_review_required_count" in reconciliation:
        return int(reconciliation.get("current_scope_review_required_count") or 0)
    return int(reconciliation.get("review_required_count") or 0)


def _broker_session_owner(
    *,
    inputs: Mapping[str, Any],
    broker_truth: Mapping[str, Any],
    latest_attempt: Mapping[str, Any],
    order_state: Mapping[str, Any],
    fill_evidence: Mapping[str, Any],
    current_time: datetime,
) -> dict[str, Any]:
    explicit = _mapping(inputs.get("broker_session_owner") or inputs.get("session_owner"))
    connection = _mapping(inputs.get("connection") or inputs.get("connection_health"))
    connection_check = _mapping(
        latest_attempt.get("connection_check")
        or broker_truth.get("connection_check")
        or connection.get("connection_check")
        or inputs.get("connection_check")
    )

    client_id = _first_present(
        explicit.get("client_id"),
        broker_truth.get("client_id"),
        latest_attempt.get("client_id"),
        connection_check.get("client_id"),
        inputs.get("client_id"),
    )
    connection_started_at = _first_present(
        explicit.get("connection_started_at"),
        explicit.get("connected_at"),
        connection_check.get("connection_timestamp"),
        broker_truth.get("started_at"),
        latest_attempt.get("started_at"),
    )
    last_position_at = _first_present(
        explicit.get("last_position_at"),
        broker_truth.get("positions_generated_at"),
        broker_truth.get("generated_at"),
        latest_attempt.get("positions_generated_at"),
        latest_attempt.get("generated_at"),
    )
    last_open_order_at = _first_present(
        explicit.get("last_open_order_at"),
        broker_truth.get("open_orders_generated_at"),
        broker_truth.get("orders_generated_at"),
        broker_truth.get("generated_at"),
        latest_attempt.get("open_orders_generated_at"),
        latest_attempt.get("orders_generated_at"),
        latest_attempt.get("generated_at"),
    )
    last_order_status_at = _first_present(
        explicit.get("last_order_status_at"),
        order_state.get("last_order_status_at"),
        order_state.get("latest_order_status_at"),
        _mapping(order_state.get("latest_order_status")).get("updated_at"),
        broker_truth.get("last_order_status_at"),
        latest_attempt.get("last_order_status_at"),
    )
    last_exec_at = _first_present(
        explicit.get("last_exec_at"),
        fill_evidence.get("last_exec_at"),
        fill_evidence.get("latest_execution_at"),
        fill_evidence.get("latest_fill_at"),
        fill_evidence.get("generated_at") if _fill_evidence_complete(fill_evidence) else None,
    )
    last_completed_order_at = _first_present(
        explicit.get("last_completed_order_at"),
        order_state.get("last_completed_order_at"),
        order_state.get("latest_completed_order_at"),
        fill_evidence.get("last_completed_order_at"),
    )
    pid = _first_present(explicit.get("pid"), broker_truth.get("pid"), latest_attempt.get("pid"), inputs.get("pid"))
    source_connection_id = _first_present(
        explicit.get("source_connection_id"),
        explicit.get("session_id"),
        broker_truth.get("source_connection_id"),
        broker_truth.get("session_id"),
        latest_attempt.get("source_connection_id"),
        latest_attempt.get("session_id"),
    )
    if not source_connection_id and client_id not in {None, ""}:
        source_connection_id = f"ibkr-client-{client_id}"

    return {
        "schema_version": "track_b_broker_session_owner_v1",
        "pid": _int_or_none(pid),
        "client_id": _int_or_none(client_id),
        "connection_started_at": _iso_text(connection_started_at),
        "server_version": _int_or_none(_first_present(explicit.get("server_version"), connection_check.get("server_version"))),
        "last_position_at": _iso_text(last_position_at),
        "last_open_order_at": _iso_text(last_open_order_at),
        "last_order_status_at": _iso_text(last_order_status_at),
        "last_exec_at": _iso_text(last_exec_at),
        "last_completed_order_at": _iso_text(last_completed_order_at),
        "source_connection_id": str(source_connection_id) if source_connection_id not in {None, ""} else None,
        "generated_at": current_time.isoformat(),
    }


def _connection_health(
    *,
    inputs: Mapping[str, Any],
    broker_truth: Mapping[str, Any],
    latest_attempt: Mapping[str, Any],
    fill_evidence: Mapping[str, Any],
    order_state: Mapping[str, Any],
    broker_session_owner: Mapping[str, Any],
    current_time: datetime,
    max_open_order_lease_age_seconds: float,
    max_fill_evidence_lease_age_seconds: float,
) -> dict[str, Any]:
    explicit = str(
        inputs.get("connection_mode")
        or _mapping(inputs.get("connection")).get("mode")
        or _mapping(inputs.get("connection_health")).get("connection_mode")
        or ""
    ).strip().upper()
    if explicit:
        mode = explicit if explicit in CONNECTION_MODES else "IBKR_CONNECTION_DOWN"
    elif not broker_truth:
        mode = "IBKR_CONNECTION_DOWN"
    elif not _bool(broker_truth.get("positions_complete")):
        mode = "IBKR_CONNECTION_DOWN"
    elif not _bool(broker_truth.get("open_orders_complete")):
        mode = "POSITION_TRUTH_ONLY"
    elif _latest_attempt_failed_after_success(
        latest_attempt=latest_attempt,
        broker_truth_time=_parse_time(
            broker_truth.get("generated_at")
            or broker_truth.get("last_success_at")
            or broker_truth.get("latest_refresh_time")
            or broker_truth.get("completed_at")
        )
        or datetime.min.replace(tzinfo=timezone.utc),
    ):
        mode = "DEGRADED_RECOVERED"
    elif _open_order_status_unreliable(
        broker_truth=broker_truth,
        order_state=order_state,
        broker_session_owner=broker_session_owner,
        current_time=current_time,
        max_age_seconds=max_open_order_lease_age_seconds,
    ):
        mode = "ORDER_STATUS_UNRELIABLE"
    elif _fill_evidence_complete(fill_evidence) and _callback_fresh(
        broker_session_owner.get("last_exec_at"),
        current_time=current_time,
        max_age_seconds=max_fill_evidence_lease_age_seconds,
    ):
        mode = "FILL_CALLBACK_CAPABLE"
    else:
        mode = "SUBMIT_CAPABLE"

    blockers: list[dict[str, str]] = []
    if mode == "IBKR_CONNECTION_DOWN":
        blockers.append({"code": "ibkr_connection_down", "detail": "IBKR broker truth connection is unavailable."})
    elif mode == "POSITION_TRUTH_ONLY":
        blockers.append(
            {
                "code": "position_truth_only",
                "detail": "Position truth is available, but open-order/submit status is not reliable enough for submits.",
            }
        )
    elif mode == "ORDER_STATUS_UNRELIABLE":
        blockers.append(
            {
                "code": "order_status_unreliable",
                "detail": "Order status is unreliable; new entries and managed closes fail closed.",
            }
        )
    elif mode == "DEGRADED_RECOVERED":
        blockers.append(
            {
                "code": "degraded_recovered_not_submit_capable",
                "detail": "Cached broker truth recovered diagnostics, but connection is not classified submit-capable.",
            }
        )

    return {
        "schema_version": "track_b_ibkr_connection_health_v1",
        "connection_mode": mode,
        "submit_capable": _connection_allows_submit(mode),
        "fill_callback_capable": mode == "FILL_CALLBACK_CAPABLE",
        "position_truth_available": mode in {"POSITION_TRUTH_ONLY", "ORDER_STATUS_UNRELIABLE", "SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE", "DEGRADED_RECOVERED"},
        "order_status_reliable": mode in {"SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE"},
        "broker_observed_adoption_diagnosis_allowed": mode
        in {"POSITION_TRUTH_ONLY", "ORDER_STATUS_UNRELIABLE", "SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE", "DEGRADED_RECOVERED"},
        "callback_freshness": {
            "last_position_at": broker_session_owner.get("last_position_at"),
            "last_open_order_at": broker_session_owner.get("last_open_order_at"),
            "last_order_status_at": broker_session_owner.get("last_order_status_at"),
            "last_exec_at": broker_session_owner.get("last_exec_at"),
            "last_completed_order_at": broker_session_owner.get("last_completed_order_at"),
        },
        "blockers": blockers,
    }


def _connection_allows_submit(connection_mode: str) -> bool:
    return str(connection_mode).strip().upper() in {"SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE"}


def _open_order_status_unreliable(
    *,
    broker_truth: Mapping[str, Any],
    order_state: Mapping[str, Any],
    broker_session_owner: Mapping[str, Any],
    current_time: datetime,
    max_age_seconds: float,
) -> bool:
    if not _bool(broker_truth.get("open_orders_complete")):
        return True
    explicit_reliable = order_state.get("order_status_reliable")
    if explicit_reliable is not None and not _bool(explicit_reliable):
        return True
    if _bool(order_state.get("order_status_callbacks_complete")) or _bool(order_state.get("order_status_complete")):
        return not _callback_fresh(
            broker_session_owner.get("last_order_status_at") or order_state.get("generated_at"),
            current_time=current_time,
            max_age_seconds=max_age_seconds,
        )
    if int(broker_truth.get("open_order_count") or broker_truth.get("track_b_broker_open_order_count") or 0) > 0:
        return not _callback_fresh(
            broker_session_owner.get("last_order_status_at"),
            current_time=current_time,
            max_age_seconds=max_age_seconds,
        )
    status = str(order_state.get("classification") or "").upper()
    if any(token in status for token in ("UNKNOWN", "STALE", "UNRELIABLE", "REVIEW_REQUIRED")):
        return True
    if order_state or broker_session_owner.get("last_order_status_at"):
        return not _callback_fresh(
            broker_session_owner.get("last_order_status_at") or order_state.get("generated_at"),
            current_time=current_time,
            max_age_seconds=max_age_seconds,
        )
    return True


def _callback_fresh(value: Any, *, current_time: datetime, max_age_seconds: float) -> bool:
    timestamp = _parse_time(value)
    if timestamp is None:
        return False
    return current_time <= timestamp + timedelta(seconds=max(0.0, max_age_seconds))


def _evidence_lease(
    *,
    lease_type: str,
    generated_at: datetime | None,
    current_time: datetime,
    max_age_seconds: float,
    source: Any,
    complete: bool,
    base_authority_uses: Sequence[str],
) -> dict[str, Any]:
    expires_at = _add_seconds(generated_at, max_age_seconds)
    age_seconds = None if generated_at is None else max(0.0, (current_time - generated_at).total_seconds())
    if generated_at is None:
        state = "MISSING"
        confidence = "NONE"
    elif not complete:
        state = "INCOMPLETE"
        confidence = "LOW"
    elif expires_at is not None and current_time > expires_at:
        state = "STALE"
        confidence = "LOW"
    else:
        state = "FRESH"
        confidence = "HIGH"

    allowed_uses = [AUTHORITY_USE_STATUS_DIAGNOSTIC]
    if state == "FRESH":
        allowed_uses.extend(str(use) for use in base_authority_uses)

    return {
        "schema_version": "track_b_broker_truth_evidence_lease_v1",
        "lease_type": lease_type,
        "state": state,
        "fresh": state == "FRESH",
        "complete": bool(complete),
        "generated_at": _iso_or_none(generated_at),
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "expires_at": _iso_or_none(expires_at),
        "source": str(source) if source not in {None, ""} else None,
        "confidence": confidence,
        "allowed_uses": _dedupe(allowed_uses),
    }


def _lease_allows(lease: Mapping[str, Any], authority_use: str) -> bool:
    return str(authority_use) in {str(item) for item in lease.get("allowed_uses") or []}


def _fill_evidence_complete(fill_evidence: Mapping[str, Any]) -> bool:
    if not fill_evidence:
        return False
    if "complete" in fill_evidence:
        return _bool(fill_evidence.get("complete"))
    if "fill_callbacks_complete" in fill_evidence:
        return _bool(fill_evidence.get("fill_callbacks_complete"))
    classification = str(fill_evidence.get("classification") or "").upper()
    if any(token in classification for token in {"MISSING", "INCOMPLETE", "UNCERTAIN", "STALE"}):
        return False
    return bool(
        fill_evidence.get("latest_fill_at")
        or fill_evidence.get("latest_execution_at")
        or fill_evidence.get("last_callback_at")
        or fill_evidence.get("exec_id")
        or fill_evidence.get("execution_id")
    )


def _submit_ownership_available(
    *,
    inputs: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    order_state: Mapping[str, Any],
) -> bool:
    submit_ownership = _mapping(inputs.get("submit_ownership") or inputs.get("submit_intent_ownership"))
    if submit_ownership:
        if submit_ownership.get("available") is False:
            return False
        return True
    if reconciliation.get("broker_backed_entry_adoption"):
        return True
    ownership = _mapping(reconciliation.get("submit_intent_ownership_reconciliation"))
    if ownership.get("matching_submit_intent") or ownership.get("matching_submit_intents"):
        return True
    if int(ownership.get("unresolved_count") or reconciliation.get("unresolved_submit_intent_ownership_count") or 0) > 0:
        return True
    if order_state.get("matching_submit_intent") or order_state.get("submit_ownership_record"):
        return True
    return False


def _authority_use_blockers(
    *,
    connection_mode: str,
    broker_position_lease: Mapping[str, Any],
    broker_open_order_lease: Mapping[str, Any],
    reconciliation_clean: bool,
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    if not _connection_allows_submit(connection_mode):
        blockers.append(
            {
                "code": "connection_not_submit_capable",
                "detail": f"Connection mode {connection_mode} cannot grant submit authority.",
            }
        )
    if not _lease_allows(broker_position_lease, AUTHORITY_USE_NEW_ENTRY):
        blockers.append(
            {
                "code": "broker_position_lease_not_fresh",
                "detail": "Fresh broker position lease is required for new entry and close authority.",
            }
        )
    if not _lease_allows(broker_open_order_lease, AUTHORITY_USE_NEW_ENTRY):
        blockers.append(
            {
                "code": "broker_open_order_lease_not_fresh",
                "detail": "Fresh broker open-order lease is required for new entry and close authority.",
            }
        )
    if not reconciliation_clean:
        blockers.append(
            {
                "code": "reconciliation_not_clean",
                "detail": "Clean broker/lifecycle reconciliation is required for new entry authority.",
            }
        )
    return blockers


def _dedupe(values: Sequence[str]) -> list[str]:
    result: list[str] = []
    for value in values:
        if value and value not in result:
            result.append(value)
    return result


def _source_timestamps(
    *,
    explicit: Mapping[str, Any],
    broker_truth: Mapping[str, Any],
    latest_attempt: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    lifecycle: Mapping[str, Any],
    order_state: Mapping[str, Any],
) -> dict[str, Any]:
    result = dict(explicit)
    defaults = {
        "broker_truth": broker_truth.get("generated_at") or broker_truth.get("last_success_at"),
        "latest_attempt": latest_attempt.get("generated_at"),
        "reconciliation": reconciliation.get("generated_at"),
        "lifecycle": lifecycle.get("generated_at"),
        "order_state": order_state.get("generated_at"),
    }
    for key, value in defaults.items():
        if value is not None and key not in result:
            result[key] = value
    return result


def _lease_id(*, account_id: str, broker_truth: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> str:
    material = json.dumps(
        {
            "account_id": account_id,
            "broker_truth_generated_at": broker_truth.get("generated_at") or broker_truth.get("last_success_at"),
            "reconciliation_generated_at": reconciliation.get("generated_at"),
            "broker_truth_classification": broker_truth.get("classification"),
            "reconciliation_classification": reconciliation.get("classification"),
        },
        sort_keys=True,
    )
    return f"btlease-{hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]}"


__all__ = [
    "AUTHORITY_USE_BROKER_OBSERVED_ADOPTION",
    "AUTHORITY_USE_FILL_CALLBACK_ADOPTION",
    "AUTHORITY_USE_NEW_ENTRY",
    "AUTHORITY_USE_RISK_REDUCING_CLOSE",
    "AUTHORITY_USE_STATUS_DIAGNOSTIC",
    "CONNECTION_MODES",
    "DEFAULT_LEASE_ARTIFACT",
    "DEFAULT_LEASE_HISTORY",
    "LEASE_STATES",
    "classify_broker_truth_lease",
    "write_broker_truth_lease",
]
