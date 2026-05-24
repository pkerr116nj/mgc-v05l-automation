from __future__ import annotations

from mgc_v05l.execution_core.track_b_control_plane_top_line import (
    TOP_LINE_HARD_UNSAFE_DUPLICATE_WRITER,
    TOP_LINE_MARKET_CLOSED_WAIT,
    TOP_LINE_MISSING_AUTHORITY_ARTIFACT,
    TOP_LINE_READY_FOR_OPERATOR_START,
    build_track_b_control_plane_top_line,
)


def test_market_closed_top_line_is_calm_and_wait_oriented() -> None:
    top_line = build_track_b_control_plane_top_line(
        {
            "supervisor_mode": "MARKET_CLOSED_WAIT",
            "proof_window_status": "market_closed",
            "safe_to_start_runtime": False,
            "primary_blocking_agent_id": "market_session",
            "operator_explanation": (
                "Market/session is closed; no fresh Phase-1 bars are expected, "
                "and PAPER should wait without treating this as a process failure."
            ),
            "recommended_observation_step": (
                "Wait for Globex/session reopen, then rebuild the Control Plane Snapshot."
            ),
        }
    )

    assert top_line["top_line_classification"] == TOP_LINE_MARKET_CLOSED_WAIT
    assert "Market closed/no fresh bars expected" in top_line["top_line_status"]
    assert "wait" in top_line["top_line_status"].lower()
    assert top_line["primary_blocking_agent_id"] == "market_session"
    assert top_line["safe_to_start_runtime"] is False
    assert top_line["proof_window_status"] == "market_closed"


def test_ready_for_start_top_line_is_clear() -> None:
    top_line = build_track_b_control_plane_top_line(
        {
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_mode": "READY_FOR_OPERATOR_START",
            "proof_window_status": "ready",
            "safe_to_start_runtime": True,
        }
    )

    assert top_line["top_line_classification"] == TOP_LINE_READY_FOR_OPERATOR_START
    assert "Ready for supervised Track B PAPER runtime start" in top_line["top_line_status"]
    assert top_line["safe_to_start_runtime"] is True


def test_duplicate_writer_top_line_is_loud() -> None:
    top_line = build_track_b_control_plane_top_line(
        {
            "agent_health_has_duplicate_writer": True,
            "primary_blocking_agent_id": "track_b_paper_runtime",
            "operator_explanation": (
                "Hard PAPER invariant blocked recovery: Track B PAPER runtime reports duplicate runtime writer detected."
            ),
            "recommended_observation_step": (
                "Preserve evidence and do not run autonomous recovery until the hard invariant clears."
            ),
            "prioritized_blockers": [
                {
                    "agent_id": "track_b_paper_runtime",
                    "display_name": "Track B PAPER runtime",
                    "status": "DUPLICATE_PROCESS",
                    "reason": "duplicate runtime writer detected",
                }
            ],
        }
    )

    assert top_line["top_line_classification"] == TOP_LINE_HARD_UNSAFE_DUPLICATE_WRITER
    assert "Hard unsafe" in top_line["top_line_status"]
    assert "duplicate" in top_line["top_line_status"].lower()
    assert top_line["primary_blocking_agent_id"] == "track_b_paper_runtime"


def test_safe_state_hard_hold_top_line_is_loud() -> None:
    top_line = build_track_b_control_plane_top_line(
        {
            "safe_state_classification": "SAFE_STATE_HARD_HOLD",
            "safe_state_operator_explanation": "Hard safe-state hold: live-money route is prohibited.",
            "safe_state_recommended_next_step": "hold runtime/start/submit actions",
            "safe_to_start_runtime": False,
            "proof_window_status": "blocked",
        }
    )

    assert top_line["top_line_classification"] == "HARD_UNSAFE_SAFE_STATE"
    assert "Runtime Safe-State Envelope" in top_line["top_line_status"]
    assert "live-money route is prohibited" in top_line["top_line_status"]


def test_missing_authority_artifact_top_line_names_artifact() -> None:
    top_line = build_track_b_control_plane_top_line(
        {
            "primary_blocking_agent_id": "open_order_truth",
            "primary_blocking_reason": "authority artifact missing",
            "operator_explanation": (
                "Refresh evidence before recovery planning: Open Order Truth reports authority artifact missing."
            ),
            "recommended_observation_step": "Run the Control Plane Snapshot refresh path.",
            "missing_artifact_count": 1,
            "prioritized_blockers": [
                {
                    "agent_id": "open_order_truth",
                    "display_name": "Open Order Truth",
                    "status": "MISSING_ARTIFACT",
                    "reason": "authority artifact missing",
                }
            ],
        }
    )

    assert top_line["top_line_classification"] == TOP_LINE_MISSING_AUTHORITY_ARTIFACT
    assert "Open Order Truth" in top_line["top_line_status"]
    assert "authority artifact missing" in top_line["top_line_status"]
    assert top_line["primary_blocking_agent_id"] == "open_order_truth"
