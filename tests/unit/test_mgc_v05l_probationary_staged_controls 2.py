from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from mgc_v05l.app import probationary_runtime as runtime_module
from mgc_v05l.app.probationary_runtime import (
    ProbationaryPaperRiskRuntimeState,
    _apply_probationary_supervisor_operator_control,
    _stop_after_cycle_is_safe_for_supervisor,
)
from mgc_v05l.domain.enums import OrderIntentType, PositionSide
from mgc_v05l.execution.order_models import OrderIntent


class _FakePendingExecution:
    def __init__(self, intent: OrderIntent, broker_order_id: str) -> None:
        self.intent = intent
        self.broker_order_id = broker_order_id
        self.submitted_at = intent.created_at
        self.acknowledged_at = intent.created_at
        self.broker_order_status = "ACKNOWLEDGED"
        self.last_status_checked_at = intent.created_at
        self.retry_count = 0
        self.signal_bar_id = None
        self.long_entry_family = None
        self.short_entry_family = None
        self.short_entry_source = None


class _FakeExecutionEngine:
    def __init__(self) -> None:
        self._pending: dict[str, _FakePendingExecution] = {}

    def submit_intent(self, intent: OrderIntent) -> _FakePendingExecution:
        pending = _FakePendingExecution(intent, f"paper-{intent.order_intent_id}")
        self._pending[intent.order_intent_id] = pending
        return pending

    def pending_executions(self) -> list[_FakePendingExecution]:
        return list(self._pending.values())


class _FakeStrategyEngine:
    def __init__(
        self,
        *,
        lane_id: str,
        operator_halt: bool = True,
        fault_code: str | None = None,
        position_side: PositionSide = PositionSide.LONG,
        internal_position_qty: int = 2,
        broker_position_qty: int = 2,
        open_broker_order_id: str | None = None,
    ) -> None:
        self.state = SimpleNamespace(
            operator_halt=operator_halt,
            fault_code=fault_code,
            position_side=position_side,
            internal_position_qty=internal_position_qty,
            broker_position_qty=broker_position_qty,
            open_broker_order_id=open_broker_order_id,
            reconcile_required=False,
            entries_enabled=not operator_halt,
        )
        self.operator_halt_updates: list[bool] = []
        self.flatten_intents: list[OrderIntent] = []
        self.clear_fault_calls = 0
        self._lane_id = lane_id

    def set_operator_halt(self, _occurred_at, enabled: bool) -> None:
        self.state.operator_halt = enabled
        self.state.entries_enabled = not enabled
        self.operator_halt_updates.append(enabled)

    def clear_fault(self, _occurred_at) -> None:
        self.clear_fault_calls += 1
        self.state.fault_code = None

    def submit_operator_flatten_intent(self, occurred_at: datetime, reason_code: str = "operator_flatten_and_halt") -> OrderIntent | None:
        if self.state.position_side == PositionSide.FLAT or self.state.internal_position_qty <= 0:
            return None
        intent_type = OrderIntentType.SELL_TO_CLOSE if self.state.position_side == PositionSide.LONG else OrderIntentType.BUY_TO_CLOSE
        intent = OrderIntent(
            order_intent_id=f"{self._lane_id}|{int(occurred_at.timestamp())}|{intent_type.value}",
            bar_id=f"{self._lane_id}|operator-control",
            symbol="MGC",
            intent_type=intent_type,
            quantity=self.state.internal_position_qty,
            created_at=occurred_at,
            reason_code=reason_code,
        )
        self.state.open_broker_order_id = f"paper-{intent.order_intent_id}"
        self.flatten_intents.append(intent)
        return intent


def _lane(
    *,
    lane_id: str,
    operator_halt: bool = True,
    fault_code: str | None = None,
    position_side: PositionSide = PositionSide.LONG,
    internal_position_qty: int = 2,
    broker_position_qty: int = 2,
) -> SimpleNamespace:
    strategy_engine = _FakeStrategyEngine(
        lane_id=lane_id,
        operator_halt=operator_halt,
        fault_code=fault_code,
        position_side=position_side,
        internal_position_qty=internal_position_qty,
        broker_position_qty=broker_position_qty,
    )
    return SimpleNamespace(
        spec=SimpleNamespace(lane_id=lane_id, display_name=lane_id, symbol="MGC"),
        strategy_engine=strategy_engine,
        execution_engine=_FakeExecutionEngine(),
        repositories=SimpleNamespace(),
    )


@pytest.fixture(autouse=True)
def _clean_reconcile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        runtime_module,
        "_reconcile_paper_runtime",
        lambda **kwargs: {
            "clean": True,
            "classification": "clean",
            "recommended_action": "No operator action required.",
        },
    )


def test_halt_and_resume_entries_are_safe_while_staged_exposure_is_open(tmp_path: Path) -> None:
    control_path = tmp_path / "operator_control.json"
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    lane = _lane(lane_id="staged_lane", operator_halt=False)
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-04-01", lane_states={"staged_lane": {"risk_state": "OK"}})

    control_path.write_text(json.dumps({"action": "halt_entries", "status": "pending"}, indent=2) + "\n", encoding="utf-8")
    halt_result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )
    assert halt_result is not None
    assert halt_result["status"] == "applied"
    assert lane.strategy_engine.state.operator_halt is True
    assert lane.strategy_engine.state.internal_position_qty == 2

    control_path.write_text(json.dumps({"action": "resume_entries", "lane_id": "staged_lane", "status": "pending"}, indent=2) + "\n", encoding="utf-8")
    resume_result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )
    assert resume_result is not None
    assert resume_result["status"] == "applied"
    assert resume_result["resumed_lanes"] == ["staged_lane"]
    assert lane.strategy_engine.state.operator_halt is False
    assert lane.strategy_engine.state.internal_position_qty == 2


def test_flatten_and_halt_submits_full_exit_for_staged_exposure(tmp_path: Path) -> None:
    control_path = tmp_path / "operator_control.json"
    control_path.write_text(json.dumps({"action": "flatten_and_halt", "status": "pending"}, indent=2) + "\n", encoding="utf-8")
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    lane = _lane(lane_id="staged_lane", operator_halt=False, internal_position_qty=3, broker_position_qty=3)
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-04-01", lane_states={"staged_lane": {"risk_state": "OK"}})

    result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )

    assert result is not None
    assert result["status"] == "flatten_pending"
    assert result["lane_flatten_states"] == {"staged_lane": "pending_fill"}
    assert lane.strategy_engine.state.operator_halt is True
    assert len(lane.strategy_engine.flatten_intents) == 1
    assert lane.strategy_engine.flatten_intents[0].quantity == 3


def test_clear_risk_halts_rejects_open_staged_exposure(tmp_path: Path) -> None:
    control_path = tmp_path / "operator_control.json"
    control_path.write_text(json.dumps({"action": "clear_risk_halts", "status": "pending"}, indent=2) + "\n", encoding="utf-8")
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    lane = _lane(lane_id="staged_lane", operator_halt=True)
    risk_state = ProbationaryPaperRiskRuntimeState(
        session_date="2026-04-01",
        desk_halt_new_entries_triggered=True,
        lane_states={"staged_lane": {"risk_state": "HALTED_DEGRADATION", "degradation_triggered": True}},
    )

    result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )

    assert result is not None
    assert result["status"] == "rejected"
    assert result["blocked_lanes"] == [{"lane_id": "staged_lane", "reason": "lane_not_flat_reconciled_and_clear"}]


def test_clear_fault_rejects_staged_open_exposure(tmp_path: Path) -> None:
    control_path = tmp_path / "operator_control.json"
    control_path.write_text(json.dumps({"action": "clear_fault", "status": "pending"}, indent=2) + "\n", encoding="utf-8")
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    lane = _lane(lane_id="staged_lane", operator_halt=True, fault_code="RESTORE_REVIEW_REQUIRED")
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-04-01", lane_states={"staged_lane": {"risk_state": "OK"}})

    result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )

    assert result is not None
    assert result["status"] == "rejected"
    assert result["uncleared_lanes"] == ["staged_lane"]
    assert lane.strategy_engine.clear_fault_calls == 0


def test_stop_after_cycle_waits_for_staged_exposure_to_flatten() -> None:
    lane = _lane(lane_id="staged_lane", operator_halt=False, internal_position_qty=2, broker_position_qty=2)
    result = {"action": "stop_after_cycle", "status": "applied"}

    assert _stop_after_cycle_is_safe_for_supervisor(result, [lane]) is False


def test_force_reconcile_reports_staged_lane_review_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    control_path = tmp_path / "operator_control.json"
    control_path.write_text(json.dumps({"action": "force_reconcile", "status": "pending"}, indent=2) + "\n", encoding="utf-8")
    settings = SimpleNamespace(resolved_probationary_operator_control_path=control_path)
    lane = _lane(lane_id="staged_lane", operator_halt=False, internal_position_qty=2, broker_position_qty=2)
    risk_state = ProbationaryPaperRiskRuntimeState(session_date="2026-04-01", lane_states={"staged_lane": {"risk_state": "OK"}})
    monkeypatch.setattr(
        runtime_module,
        "_reconcile_paper_runtime",
        lambda **kwargs: {
            "clean": False,
            "classification": "fill_ack_uncertainty",
            "recommended_action": "Freeze new entries and inspect missing fill acknowledgement before resuming.",
        },
    )
    lane.strategy_engine.force_reconcile = lambda **kwargs: {  # type: ignore[attr-defined]
        "clean": False,
        "classification": "fill_ack_uncertainty",
        "recommended_action": "Freeze new entries and inspect missing fill acknowledgement before resuming.",
    }

    result = _apply_probationary_supervisor_operator_control(
        settings=settings,
        lanes=[lane],
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
        risk_state=risk_state,
    )

    assert result is not None
    assert result["status"] == "applied"
    assert result["reconciliation"]["unresolved_lanes"] == ["staged_lane"]
    assert result["reconciliation"]["lane_results"][0]["classification"] == "fill_ack_uncertainty"
