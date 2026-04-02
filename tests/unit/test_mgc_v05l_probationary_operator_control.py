"""Focused tests for shared probationary paper operator control."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from mgc_v05l.app.probationary_runtime import (
    ProbationaryPaperRiskRuntimeState,
    _apply_probationary_supervisor_operator_control,
)


class _FakeStrategyEngine:
    def __init__(self, *, operator_halt: bool = True, fault_code: str | None = None) -> None:
        self.state = SimpleNamespace(operator_halt=operator_halt, fault_code=fault_code)
        self.operator_halt_updates: list[bool] = []

    def set_operator_halt(self, _occurred_at, enabled: bool) -> None:
        self.state.operator_halt = enabled
        self.operator_halt_updates.append(enabled)


def _lane(*, lane_id: str, operator_halt: bool = True, fault_code: str | None = None):
    return SimpleNamespace(
        spec=SimpleNamespace(lane_id=lane_id, display_name=lane_id, symbol="MGC"),
        strategy_engine=_FakeStrategyEngine(operator_halt=operator_halt, fault_code=fault_code),
        execution_engine=SimpleNamespace(),
    )


def test_resume_entries_targets_only_requested_lane(tmp_path: Path) -> None:
    control_path = tmp_path / "operator_control.json"
    control_path.write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "lane_id": "mgc_us_late_pause_resume_long",
                "status": "pending",
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    target_lane = _lane(lane_id="mgc_us_late_pause_resume_long", operator_halt=True)
    other_lane = _lane(lane_id="mgc_asia_breakout_long", operator_halt=True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-04-01",
        lane_states={
            "mgc_us_late_pause_resume_long": {"risk_state": "OK"},
            "mgc_asia_breakout_long": {"risk_state": "OK"},
        },
    )

    result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[target_lane, other_lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )

    assert result is not None
    assert result["status"] == "applied"
    assert result["lane_id"] == "mgc_us_late_pause_resume_long"
    assert result["resumed_lanes"] == ["mgc_us_late_pause_resume_long"]
    assert result["blocked_lanes"] == []
    assert target_lane.strategy_engine.state.operator_halt is False
    assert other_lane.strategy_engine.state.operator_halt is True
    persisted = json.loads(control_path.read_text(encoding="utf-8"))
    assert persisted["resumed_lanes"] == ["mgc_us_late_pause_resume_long"]


def test_resume_entries_reports_exact_fault_blocker_for_target_lane(tmp_path: Path) -> None:
    control_path = tmp_path / "operator_control.json"
    control_path.write_text(
        json.dumps(
            {
                "action": "resume_entries",
                "lane_id": "mgc_us_late_pause_resume_long",
                "status": "pending",
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    target_lane = _lane(
        lane_id="mgc_us_late_pause_resume_long",
        operator_halt=True,
        fault_code="RESTORE_REVIEW_REQUIRED",
    )
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-04-01",
        lane_states={"mgc_us_late_pause_resume_long": {"risk_state": "OK"}},
    )

    result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[target_lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )

    assert result is not None
    assert result["status"] == "rejected"
    assert result["lane_id"] == "mgc_us_late_pause_resume_long"
    assert result["resumed_lanes"] == []
    assert result["blocked_lanes"] == [
        {
            "lane_id": "mgc_us_late_pause_resume_long",
            "reason": "fault",
            "detail": "RESTORE_REVIEW_REQUIRED",
        }
    ]
    assert "fault" in result["message"]
    assert target_lane.strategy_engine.state.operator_halt is True
