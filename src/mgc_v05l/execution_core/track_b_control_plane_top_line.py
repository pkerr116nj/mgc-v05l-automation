"""Operator-facing top-line formatter for Track B control-plane snapshots.

The formatter is display-only. It consumes the execution_core Control Plane
Snapshot payload and returns the same compact summary packet for launch,
status, and dashboard surfaces.
"""

from __future__ import annotations

from typing import Any, Mapping


TOP_LINE_MARKET_CLOSED_WAIT = "MARKET_CLOSED_WAIT"
TOP_LINE_READY_FOR_OPERATOR_START = "READY_FOR_OPERATOR_START"
TOP_LINE_HARD_UNSAFE_DUPLICATE_WRITER = "HARD_UNSAFE_DUPLICATE_WRITER"
TOP_LINE_MISSING_AUTHORITY_ARTIFACT = "MISSING_AUTHORITY_ARTIFACT"
TOP_LINE_CONTROL_PLANE_BLOCKED = "CONTROL_PLANE_BLOCKED"
TOP_LINE_CONTROL_PLANE_STATUS = "CONTROL_PLANE_STATUS"


def build_track_b_control_plane_top_line(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    """Build the shared operator top-line fields from one Control Plane Snapshot."""

    proof_window_status = _text(snapshot.get("proof_window_status"))
    supervisor_mode = _text(snapshot.get("supervisor_mode"))
    supervisor_classification = _text(snapshot.get("runtime_supervisor_classification"))
    primary_blocking_agent_id = _text(snapshot.get("primary_blocking_agent_id"))
    operator_explanation = _text(snapshot.get("operator_explanation"))
    recommended_observation_step = _text(snapshot.get("recommended_observation_step"))
    safe_to_start_runtime = snapshot.get("safe_to_start_runtime") is True

    if _duplicate_writer_detected(snapshot):
        classification = TOP_LINE_HARD_UNSAFE_DUPLICATE_WRITER
        status = _join_status(
            "Hard unsafe: duplicate Track B PAPER runtime writer detected.",
            operator_explanation,
            recommended_observation_step,
        )
    elif _missing_authority_artifact_detected(snapshot):
        classification = TOP_LINE_MISSING_AUTHORITY_ARTIFACT
        status = _join_status(
            _missing_authority_status(snapshot),
            operator_explanation,
            recommended_observation_step,
        )
    elif proof_window_status == "market_closed" or supervisor_mode == "MARKET_CLOSED_WAIT":
        classification = TOP_LINE_MARKET_CLOSED_WAIT
        status = _join_status(
            "Market closed/no fresh bars expected; wait and refresh after reopen.",
            operator_explanation,
            recommended_observation_step,
        )
    elif (
        safe_to_start_runtime
        and supervisor_classification == "SUPERVISOR_RUNTIME_START_ALLOWED"
        and supervisor_mode == "READY_FOR_OPERATOR_START"
    ):
        classification = TOP_LINE_READY_FOR_OPERATOR_START
        status = _join_status(
            "Ready for supervised Track B PAPER runtime start.",
            operator_explanation,
            recommended_observation_step,
        )
    elif snapshot.get("blockers"):
        classification = TOP_LINE_CONTROL_PLANE_BLOCKED
        status = _join_status(
            "Control Plane Snapshot blocks runtime start.",
            operator_explanation,
            recommended_observation_step,
        )
    else:
        classification = TOP_LINE_CONTROL_PLANE_STATUS
        status = _join_status(
            "Control Plane Snapshot status is available.",
            operator_explanation,
            recommended_observation_step,
        )

    return {
        "top_line_classification": classification,
        "top_line_status": status,
        "primary_blocking_agent_id": primary_blocking_agent_id,
        "operator_explanation": operator_explanation,
        "recommended_observation_step": recommended_observation_step,
        "safe_to_start_runtime": safe_to_start_runtime,
        "proof_window_status": proof_window_status,
    }


def _duplicate_writer_detected(snapshot: Mapping[str, Any]) -> bool:
    if snapshot.get("agent_health_has_duplicate_writer") is True:
        return True
    for blocker in _blocker_rows(snapshot):
        if _text(blocker.get("status")) == "DUPLICATE_PROCESS":
            return True
        if "duplicate" in _text(blocker.get("reason")).lower() and "runtime" in _text(blocker.get("reason")).lower():
            return True
    combined = " ".join(
        [
            _text(snapshot.get("primary_blocking_agent_id")),
            _text(snapshot.get("primary_blocking_reason")),
            _text(snapshot.get("operator_explanation")),
        ]
    ).lower()
    return "duplicate" in combined and ("writer" in combined or "runtime" in combined)


def _missing_authority_artifact_detected(snapshot: Mapping[str, Any]) -> bool:
    if _as_int(snapshot.get("missing_artifact_count")) > 0:
        return True
    for blocker in _blocker_rows(snapshot):
        if _text(blocker.get("status")) == "MISSING_ARTIFACT":
            return True
        if "missing" in _text(blocker.get("reason")).lower() and "artifact" in _text(blocker.get("reason")).lower():
            return True
    combined = " ".join(
        [
            _text(snapshot.get("primary_blocking_agent_id")),
            _text(snapshot.get("primary_blocking_reason")),
            _text(snapshot.get("operator_explanation")),
        ]
    ).lower()
    return "missing" in combined and "artifact" in combined


def _missing_authority_status(snapshot: Mapping[str, Any]) -> str:
    agent_id = _text(snapshot.get("primary_blocking_agent_id"))
    for blocker in _blocker_rows(snapshot):
        if _text(blocker.get("status")) == "MISSING_ARTIFACT":
            display_name = _text(blocker.get("display_name")) or _text(blocker.get("agent_id"))
            reason = _text(blocker.get("reason"))
            if display_name and reason:
                return f"Missing authority artifact: {display_name} reports {reason}."
            if display_name:
                return f"Missing authority artifact: {display_name}."
    if agent_id:
        return f"Missing authority artifact: {agent_id}."
    return "Missing required execution_core authority artifact."


def _blocker_rows(snapshot: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = []
    for key in ("prioritized_blockers", "agent_health_top_blockers"):
        value = snapshot.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, Mapping):
                rows.append(item)
    return rows


def _join_status(*parts: str) -> str:
    seen: set[str] = set()
    clean_parts: list[str] = []
    for part in parts:
        clean = _text(part)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        clean_parts.append(clean.rstrip("."))
    if not clean_parts:
        return "Control Plane Snapshot status is available."
    return ". ".join(clean_parts) + "."


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _as_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
