"""Read-only broker session authority publisher for Track B PAPER.

This module consumes broker-truth leases that have already been produced by the
read-only broker truth refresher. It publishes a session-authority view for
status and downstream authority layers without opening broker connections or
executing broker actions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic

DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_session_authority.json"
)
DEFAULT_BROKER_SESSION_AUTHORITY_HISTORY = (
    Path("outputs") / "operator_dashboard" / "runtime" / "broker_session_authority_history.jsonl"
)

SESSION_AUTHORITY_CLASSIFICATIONS = {
    "BROKER_SESSION_AUTHORITY_CONNECTION_DOWN",
    "BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY",
    "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
    "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
    "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
    "BROKER_SESSION_AUTHORITY_FILL_CALLBACK_CAPABLE",
    "BROKER_SESSION_AUTHORITY_DEGRADED_RECOVERED",
    "BROKER_SESSION_AUTHORITY_OPERATOR_REQUIRED",
}

DIAGNOSTIC_USE_PUBLISHED_AUTHORITY_ACTIVE_EXPOSURE = (
    "DIAGNOSTIC_USE_PUBLISHED_BROKER_SESSION_AUTHORITY_ACTIVE_EXPOSURE"
)
DIAGNOSTIC_OPERATOR_PROBE_ALLOWED_NO_EXPOSURE = "DIAGNOSTIC_OPERATOR_PROBE_ALLOWED_NO_ACTIVE_EXPOSURE"


def build_broker_session_authority(
    *,
    lease: Mapping[str, Any],
    generated_at: str | None = None,
    source_lease_path: Path | str | None = None,
    active_track_b_exposure: bool = False,
    observed_submit_client_ids: list[int] | None = None,
) -> dict[str, Any]:
    """Build the read-only broker-session authority publisher artifact."""

    now_text = generated_at or datetime.now(timezone.utc).isoformat()
    owner = _mapping(lease.get("broker_session_owner"))
    source_connection_mode = str(lease.get("connection_mode") or "").strip().upper()
    connection_health = _mapping(lease.get("connection_health"))
    connection_mode = _effective_connection_mode(lease=lease, source_connection_mode=source_connection_mode)
    degraded_exact_close_context = _mapping(lease.get("degraded_exact_risk_reducing_close_context"))
    callback_attribution = _mapping(
        lease.get("callback_ownership_attribution") or connection_health.get("callback_ownership_attribution")
    )
    lease_state = str(lease.get("lease_state") or "").strip().upper()
    allowed_uses = _published_allowed_uses(lease=lease, connection_mode=connection_mode, lease_state=lease_state)
    authority_blockers = _authority_blockers(
        lease=lease,
        connection_mode=connection_mode,
        lease_state=lease_state,
        callback_attribution=callback_attribution,
    )
    diagnostics_policy = diagnostic_probe_policy(active_track_b_exposure=active_track_b_exposure)
    split_ownership = _split_session_ownership(owner=owner, observed_submit_client_ids=observed_submit_client_ids or [])
    authority_source_timestamp = (
        lease.get("authority_source_timestamp") or lease.get("broker_truth_generated_at") or lease.get("generated_at")
    )
    position_snapshot_timestamp = lease.get("position_snapshot_timestamp") or owner.get("last_position_at")
    open_order_snapshot_timestamp = lease.get("open_order_snapshot_timestamp") or owner.get("last_open_order_at")
    callback_timestamps = _mapping(lease.get("callback_timestamps")) or {
        "last_position_at": owner.get("last_position_at"),
        "last_open_order_at": owner.get("last_open_order_at"),
        "last_order_status_at": owner.get("last_order_status_at"),
        "last_exec_at": owner.get("last_exec_at"),
        "last_completed_order_at": owner.get("last_completed_order_at"),
    }

    return {
        "schema_version": "track_b_broker_session_authority_v1",
        "generated_at": now_text,
        "authority_generation_id": lease.get("authority_generation_id"),
        "authority_writer": lease.get("authority_writer"),
        "authority_source_timestamp": authority_source_timestamp,
        "mode": "PAPER",
        "paper_only": True,
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_attempted": False,
        "close_attempted": False,
        "cancel_attempted": False,
        "global_cancel_allowed": False,
        "broad_flatten_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": _classification_for(connection_mode=connection_mode, lease_state=lease_state),
        "lease_state": lease_state or "OPERATOR_REQUIRED",
        "connection_mode": connection_mode or "IBKR_CONNECTION_DOWN",
        "connection_health": connection_health,
        "broker_session_owner": owner,
        "pid": _int_or_none(owner.get("pid")),
        "client_id": _int_or_none(owner.get("client_id")),
        "connection_started_at": owner.get("connection_started_at"),
        "server_version": _int_or_none(owner.get("server_version")),
        "last_position_at": owner.get("last_position_at"),
        "last_open_order_at": owner.get("last_open_order_at"),
        "last_order_status_at": owner.get("last_order_status_at"),
        "last_exec_at": owner.get("last_exec_at"),
        "last_completed_order_at": owner.get("last_completed_order_at"),
        "position_snapshot_timestamp": position_snapshot_timestamp,
        "open_order_snapshot_timestamp": open_order_snapshot_timestamp,
        "callback_timestamps": callback_timestamps,
        "observation_basis": {
            "position_snapshot_timestamp": position_snapshot_timestamp,
            "open_order_snapshot_timestamp": open_order_snapshot_timestamp,
            "last_order_status_at": callback_timestamps.get("last_order_status_at"),
            "last_exec_at": callback_timestamps.get("last_exec_at"),
            "last_completed_order_at": callback_timestamps.get("last_completed_order_at"),
            "source_connection_id": owner.get("source_connection_id"),
        },
        "source_connection_id": owner.get("source_connection_id"),
        "position_truth_client_id": callback_attribution.get("position_truth_client_id"),
        "open_order_truth_client_id": callback_attribution.get("open_order_truth_client_id"),
        "last_order_status_client_id": callback_attribution.get("last_order_status_client_id"),
        "last_exec_details_client_id": callback_attribution.get("last_exec_details_client_id"),
        "last_completed_order_client_id": callback_attribution.get("last_completed_order_client_id"),
        "submit_client_id": callback_attribution.get("submit_client_id"),
        "session_match": callback_attribution.get("session_match"),
        "callback_age_seconds": callback_attribution.get("callback_age_seconds"),
        "callback_missing_reason": callback_attribution.get("callback_missing_reason"),
        "callback_ownership_attribution": callback_attribution,
        "broker_position_lease": _mapping(lease.get("broker_position_lease")),
        "broker_open_order_lease": _mapping(lease.get("broker_open_order_lease")),
        "execution_fill_evidence_lease": _mapping(lease.get("execution_fill_evidence_lease")),
        "degraded_exact_risk_reducing_close_context": degraded_exact_close_context,
        "risk_reducing_close_connection_mode": (
            "RISK_REDUCING_CLOSE_CAPABLE_ORDER_STATUS_DEGRADED"
            if degraded_exact_close_context.get("ready") is True
            and allowed_uses.get("managed_risk_reducing_close") is True
            else connection_mode
        ),
        "allowed_uses": allowed_uses,
        "connection_allowed_uses": _connection_allowed_uses(connection_health=connection_health, connection_mode=connection_mode),
        "authority_blockers": authority_blockers,
        "callback_health": {
            "classification": callback_attribution.get("classification"),
            "last_order_status_at": owner.get("last_order_status_at"),
            "last_exec_at": owner.get("last_exec_at"),
            "last_completed_order_at": owner.get("last_completed_order_at"),
            "last_order_status_client_id": callback_attribution.get("last_order_status_client_id"),
            "last_exec_details_client_id": callback_attribution.get("last_exec_details_client_id"),
            "last_completed_order_client_id": callback_attribution.get("last_completed_order_client_id"),
            "order_status_reliable": bool(connection_health.get("order_status_reliable")),
            "fill_callback_capable": bool(connection_health.get("fill_callback_capable")),
            "missing_execution_callbacks_visible": not bool(
                _mapping(lease.get("execution_fill_evidence_lease")).get("state") == "FRESH"
            ),
        },
        "split_session_ownership": split_ownership,
        "diagnostics_policy": diagnostics_policy,
        "source_lease_path": str(source_lease_path) if source_lease_path is not None else None,
        "source_lease_id": lease.get("lease_id"),
        "source_lease_generated_at": lease.get("generated_at"),
    }


def write_broker_session_authority(
    *,
    output_path: Path,
    authority: Mapping[str, Any],
    history_path: Path | None = None,
) -> None:
    """Write the publisher artifact and optional JSONL history."""

    write_json_atomic(Path(output_path), authority)
    if history_path is None:
        return
    append_bounded_jsonl(history_path, dict(authority))


def load_broker_session_authority(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def diagnostic_probe_policy(*, active_track_b_exposure: bool) -> dict[str, Any]:
    classification = (
        DIAGNOSTIC_USE_PUBLISHED_AUTHORITY_ACTIVE_EXPOSURE
        if active_track_b_exposure
        else DIAGNOSTIC_OPERATOR_PROBE_ALLOWED_NO_EXPOSURE
    )
    return {
        "classification": classification,
        "active_track_b_exposure": bool(active_track_b_exposure),
        "preferred_source": "broker_session_authority"
        if active_track_b_exposure
        else "operator_requested_read_only_probe",
        "independent_ibkr_probe_hot_path_authority": False,
        "reconnect_probe_allowed": not bool(active_track_b_exposure),
        "operator_diagnostic_only": True,
    }


def should_use_published_authority_for_diagnostic(authority: Mapping[str, Any]) -> bool:
    policy = _mapping(authority.get("diagnostics_policy"))
    return (
        policy.get("classification") == DIAGNOSTIC_USE_PUBLISHED_AUTHORITY_ACTIVE_EXPOSURE
        and bool(policy.get("active_track_b_exposure"))
    )


def _published_allowed_uses(*, lease: Mapping[str, Any], connection_mode: str, lease_state: str) -> dict[str, bool]:
    lease_allowed = _mapping(lease.get("allowed_uses"))
    submit_capable = connection_mode in {
        "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
        "SUBMIT_CAPABLE",
        "FILL_CALLBACK_CAPABLE",
    } and lease_state in {
        "ACTIVE",
        "ACTIVE_DEGRADED_REFRESH_FAILING",
    }
    open_order_reliable = connection_mode in {"SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE"}
    degraded_exact_close = _lease_allows_degraded_exact_risk_reducing_close(lease=lease)
    return {
        "new_entry": bool(submit_capable and lease_allowed.get("new_entry") is True),
        "managed_risk_reducing_close": bool(
            lease_allowed.get("managed_risk_reducing_close") is True
            and ((submit_capable and open_order_reliable) or degraded_exact_close)
        ),
        "broker_observed_adoption_diagnosis": bool(lease_allowed.get("broker_observed_adoption_diagnosis") is True),
        "fill_callback_adoption": bool(
            connection_mode == "FILL_CALLBACK_CAPABLE" and lease_allowed.get("fill_callback_adoption") is True
        ),
        "status_diagnostic": True,
    }


def _effective_connection_mode(*, lease: Mapping[str, Any], source_connection_mode: str) -> str:
    mode = str(source_connection_mode or "").strip().upper()
    if _lease_allows_flat_no_order_new_entry(lease=lease):
        return "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
    return mode


def _lease_allows_flat_no_order_new_entry(*, lease: Mapping[str, Any]) -> bool:
    lease_allowed = _mapping(lease.get("allowed_uses"))
    connection_health = _mapping(lease.get("connection_health"))
    flat_context = _mapping(connection_health.get("flat_no_order_submit_capable_context"))
    connection_mode = str(lease.get("connection_mode") or connection_health.get("connection_mode") or "").strip().upper()
    classification = str(lease.get("classification") or lease.get("lease_classification") or "").strip().upper()
    flat_no_order_classified = (
        connection_mode == "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS"
        or classification in {
            "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
            "BROKER_TRUTH_LEASE_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
        }
        or bool(flat_context.get("ready"))
    )
    new_entry_allowed = lease_allowed.get("new_entry") is True or lease.get("submit_entry_allowed") is True
    return bool(flat_no_order_classified and new_entry_allowed)


def _lease_allows_degraded_exact_risk_reducing_close(*, lease: Mapping[str, Any]) -> bool:
    lease_allowed = _mapping(lease.get("allowed_uses"))
    context = _mapping(lease.get("degraded_exact_risk_reducing_close_context"))
    connection_mode = str(lease.get("connection_mode") or "").strip().upper()
    return bool(
        connection_mode == "ORDER_STATUS_UNRELIABLE"
        and context.get("ready") is True
        and lease_allowed.get("managed_risk_reducing_close") is True
    )


def _connection_allowed_uses(*, connection_health: Mapping[str, Any], connection_mode: str) -> dict[str, bool]:
    return {
        "position_truth": bool(connection_health.get("position_truth_available")),
        "open_order_truth": bool(connection_health.get("order_status_reliable")),
        "new_entry_connection": connection_mode
        in {"SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS", "SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE"},
        "managed_risk_reducing_close_connection": connection_mode in {"SUBMIT_CAPABLE", "FILL_CALLBACK_CAPABLE"},
        "broker_observed_adoption_diagnosis_connection": bool(
            connection_health.get("broker_observed_adoption_diagnosis_allowed")
        ),
        "fill_callback_evidence": bool(connection_health.get("fill_callback_capable")),
    }


def _classification_for(*, connection_mode: str, lease_state: str) -> str:
    if lease_state in {"", "OPERATOR_REQUIRED"} or lease_state.startswith("INVALIDATED"):
        if connection_mode == "ORDER_STATUS_UNRELIABLE":
            return "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE"
        if connection_mode == "POSITION_TRUTH_ONLY":
            return "BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY"
        if connection_mode == "IBKR_CONNECTION_DOWN":
            return "BROKER_SESSION_AUTHORITY_CONNECTION_DOWN"
        return "BROKER_SESSION_AUTHORITY_OPERATOR_REQUIRED"
    mapping = {
        "IBKR_CONNECTION_DOWN": "BROKER_SESSION_AUTHORITY_CONNECTION_DOWN",
        "POSITION_TRUTH_ONLY": "BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY",
        "ORDER_STATUS_UNRELIABLE": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        "SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE_NO_RECENT_ORDER_EVENTS",
        "SUBMIT_CAPABLE": "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
        "FILL_CALLBACK_CAPABLE": "BROKER_SESSION_AUTHORITY_FILL_CALLBACK_CAPABLE",
        "DEGRADED_RECOVERED": "BROKER_SESSION_AUTHORITY_DEGRADED_RECOVERED",
    }
    return mapping.get(connection_mode, "BROKER_SESSION_AUTHORITY_OPERATOR_REQUIRED")


def _authority_blockers(
    *,
    lease: Mapping[str, Any],
    connection_mode: str,
    lease_state: str,
    callback_attribution: Mapping[str, Any],
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    for source_name in ("authority_use_blockers", "blockers"):
        for row in lease.get(source_name) or []:
            if isinstance(row, Mapping):
                code = str(row.get("code") or "").strip()
                detail = str(row.get("detail") or "").strip()
                if code:
                    blockers.append({"code": code, "detail": detail})
    if connection_mode == "ORDER_STATUS_UNRELIABLE":
        attribution_classification = str(callback_attribution.get("classification") or "").strip()
        attribution_reason = str(callback_attribution.get("callback_missing_reason") or "").strip()
        detail = "Order status callback truth is stale or unknown; entry and close authority fail closed."
        if attribution_classification in {"SPLIT_CALLBACK_OWNERSHIP", "CALLBACK_ATTRIBUTION_GAP"}:
            detail = (
                "Order status is unreliable because broker truth and callback ownership are not aligned"
                f" ({attribution_reason or attribution_classification})."
            )
        blockers.append(
            {
                "code": "order_status_unreliable_blocks_submit_and_close",
                "detail": detail,
            }
        )
    elif connection_mode == "POSITION_TRUTH_ONLY":
        blockers.append(
            {
                "code": "position_truth_only_diagnostic_use_only",
                "detail": "Position truth may diagnose broker-observed adoption, but submit and close authority are blocked.",
            }
        )
    elif connection_mode == "DEGRADED_RECOVERED":
        blockers.append(
            {
                "code": "degraded_recovered_not_authority_clean",
                "detail": "A later refresh failed after last-good truth; authority requires a clean session classification.",
            }
        )
    if lease_state.startswith("INVALIDATED") or lease_state in {"", "OPERATOR_REQUIRED"}:
        blockers.append(
            {
                "code": "lease_not_authority_clean",
                "detail": "Broker truth lease is not clean enough to grant submit or close authority.",
            }
        )
    return _dedupe_blockers(blockers)


def _split_session_ownership(*, owner: Mapping[str, Any], observed_submit_client_ids: list[int]) -> dict[str, Any]:
    owner_client_id = _int_or_none(owner.get("client_id"))
    submit_client_ids = sorted({int(value) for value in observed_submit_client_ids if _int_or_none(value) is not None})
    split = bool(owner_client_id is not None and any(client_id != owner_client_id for client_id in submit_client_ids))
    return {
        "detected": split,
        "broker_truth_client_id": owner_client_id,
        "observed_submit_client_ids": submit_client_ids,
        "classification": "SPLIT_BROKER_SESSION_OWNERSHIP" if split else "BROKER_SESSION_OWNER_ALIGNED_OR_UNKNOWN",
        "authority_note": "Split ownership is diagnostic-only in Slice 1; authority remains governed by lease allowed_uses.",
    }


def _dedupe_blockers(blockers: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for row in blockers:
        code = str(row.get("code") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        result.append(row)
    return result


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
