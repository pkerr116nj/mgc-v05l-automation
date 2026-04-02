from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import mgc_v05l.app.probationary_runtime as probationary_runtime_module
from mgc_v05l.app.operator_dashboard import (
    OperatorDashboardService,
    _annotate_same_underlying_strategy_ambiguity,
    _build_same_underlying_conflicts,
)
from mgc_v05l.app.probationary_runtime import _apply_probationary_same_underlying_entry_holds


def _paper_payload(
    *,
    runtime_rows: list[dict[str, object]] | None = None,
    strategy_rows: list[dict[str, object]] | None = None,
    audit_rows: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "runtime_registry": {"rows": runtime_rows or []},
        "strategy_performance": {"rows": strategy_rows or []},
        "signal_intent_fill_audit": {"rows": audit_rows or []},
    }


def _production_link_payload(
    *,
    positions: list[dict[str, object]] | None = None,
    open_orders: list[dict[str, object]] | None = None,
    reconciliation_label: str = "CLEAR",
) -> dict[str, object]:
    return {
        "portfolio": {"positions": positions or []},
        "orders": {"open_rows": open_orders or []},
        "reconciliation": {"label": reconciliation_label},
    }


def _runtime_row(strategy_id: str, *, instrument: str = "MGC", runtime_loaded: bool = True) -> dict[str, object]:
    return {
        "standalone_strategy_id": strategy_id,
        "instrument": instrument,
        "display_name": strategy_id,
        "runtime_instance_present": True,
        "runtime_state_loaded": runtime_loaded,
        "can_process_bars": True,
    }


def _strategy_row(strategy_id: str, *, instrument: str = "MGC", position_side: str = "FLAT") -> dict[str, object]:
    return {
        "standalone_strategy_id": strategy_id,
        "instrument": instrument,
        "strategy_name": strategy_id,
        "status": "READY",
        "position_side": position_side,
        "entries_enabled": True,
    }


def _audit_row(
    strategy_id: str,
    *,
    instrument: str = "MGC",
    position_side: str = "FLAT",
    eligible_now: bool | None = None,
    open_broker_order_id: str | None = None,
    last_intent_type: str | None = None,
) -> dict[str, object]:
    return {
        "standalone_strategy_id": strategy_id,
        "instrument": instrument,
        "strategy_name": strategy_id,
        "runtime_state_loaded": True,
        "current_strategy_status": "READY",
        "position_side": position_side,
        "eligible_now": eligible_now,
        "open_broker_order_id": open_broker_order_id,
        "last_intent_type": last_intent_type,
    }


def _single_conflict(**kwargs: object) -> dict[str, object]:
    payload = _build_same_underlying_conflicts(
        paper=kwargs["paper"],
        production_link=kwargs["production_link"],
        generated_at="2026-04-01T12:00:00+00:00",
    )
    rows = payload["rows"]
    assert len(rows) == 1
    return rows[0]


def test_same_underlying_runtime_coexistence_is_informational_by_default() -> None:
    row = _single_conflict(
        paper=_paper_payload(
            runtime_rows=[
                _runtime_row("lane_a"),
                _runtime_row("lane_b"),
            ],
        ),
        production_link=_production_link_payload(),
    )

    assert row["conflict_kind"] == "multiple_runtime_instances_same_instrument"
    assert row["severity"] == "INFO"
    assert row["observational_only"] is True
    assert row["operator_action_required"] is False


def test_same_side_live_overlap_is_observational_by_default() -> None:
    row = _single_conflict(
        paper=_paper_payload(
            runtime_rows=[_runtime_row("lane_a"), _runtime_row("lane_b")],
            strategy_rows=[
                _strategy_row("lane_a", position_side="LONG"),
                _strategy_row("lane_b", position_side="LONG"),
            ],
            audit_rows=[
                _audit_row("lane_a", position_side="LONG"),
                _audit_row("lane_b", position_side="LONG"),
            ],
        ),
        production_link=_production_link_payload(),
    )

    assert row["conflict_kind"] == "same_side_in_position_overlap"
    assert row["severity"] == "INFO"
    assert row["observational_only"] is True
    assert row["operator_action_required"] is False


def test_pending_order_ambiguity_still_blocks() -> None:
    row = _single_conflict(
        paper=_paper_payload(
            runtime_rows=[_runtime_row("lane_a"), _runtime_row("lane_b")],
            audit_rows=[
                _audit_row("lane_a", open_broker_order_id="ord-1", last_intent_type="BUY_TO_OPEN"),
                _audit_row("lane_b", open_broker_order_id="ord-2", last_intent_type="SELL_TO_OPEN"),
            ],
        ),
        production_link=_production_link_payload(),
    )

    assert row["conflict_kind"] == "multiple_pending_orders_same_instrument"
    assert row["severity"] == "BLOCKING"
    assert row["operator_action_required"] is True


def test_broker_overlap_mismatch_still_blocks() -> None:
    row = _single_conflict(
        paper=_paper_payload(
            runtime_rows=[_runtime_row("lane_a"), _runtime_row("lane_b")],
        ),
        production_link=_production_link_payload(
            positions=[{"symbol": "MGC"}],
            reconciliation_label="CLEAR",
        ),
    )

    assert row["conflict_kind"] == "broker_vs_strategy_overlap_mismatch"
    assert row["severity"] == "BLOCKING"
    assert row["operator_action_required"] is True


def test_reconciliation_required_same_underlying_overlap_blocks() -> None:
    row = _single_conflict(
        paper=_paper_payload(
            runtime_rows=[_runtime_row("lane_a"), _runtime_row("lane_b")],
            strategy_rows=[
                _strategy_row("lane_a", position_side="LONG"),
                _strategy_row("lane_b", position_side="LONG"),
            ],
            audit_rows=[
                _audit_row("lane_a", position_side="LONG"),
                _audit_row("lane_b", position_side="LONG"),
            ],
        ),
        production_link=_production_link_payload(reconciliation_label="RECONCILIATION_REQUIRED"),
    )

    assert row["conflict_kind"] == "reconciliation_required_same_instrument_overlap"
    assert row["severity"] == "BLOCKING"
    assert row["operator_action_required"] is True


def test_observational_same_underlying_review_state_is_default(tmp_path: Path) -> None:
    service = OperatorDashboardService(tmp_path)
    conflicts = _build_same_underlying_conflicts(
        paper=_paper_payload(
            runtime_rows=[_runtime_row("lane_a"), _runtime_row("lane_b")],
            strategy_rows=[
                _strategy_row("lane_a", position_side="LONG"),
                _strategy_row("lane_b", position_side="LONG"),
            ],
            audit_rows=[
                _audit_row("lane_a", position_side="LONG"),
                _audit_row("lane_b", position_side="LONG"),
            ],
        ),
        production_link=_production_link_payload(),
        generated_at="2026-04-01T12:00:00+00:00",
    )

    enriched = service._apply_same_underlying_conflict_review_state(  # noqa: SLF001
        conflicts,
        generated_at="2026-04-01T12:00:00+00:00",
    )

    row = enriched["rows"][0]
    assert row["observational_only"] is True
    assert row["review_state_status"] == "OBSERVATIONAL"
    assert row["entry_hold_effective"] is False


def test_same_underlying_ambiguity_note_says_coexistence_allowed_by_default() -> None:
    rows = _annotate_same_underlying_strategy_ambiguity(
        [
            {"instrument": "MGC", "lane_id": "lane_a"},
            {"instrument": "MGC", "lane_id": "lane_b"},
        ]
    )

    assert rows[0]["same_underlying_ambiguity"] is True
    assert "allowed by default" in rows[0]["same_underlying_ambiguity_note"]


class _FakeStrategyEngine:
    def __init__(self, *, held: bool = False, reason: str | None = None) -> None:
        self.state = SimpleNamespace(
            same_underlying_entry_hold=held,
            same_underlying_hold_reason=reason,
        )
        self.calls: list[tuple[bool, str | None]] = []

    def set_same_underlying_entry_hold(self, _occurred_at: datetime, held: bool, *, reason: str | None = None) -> None:
        self.state.same_underlying_entry_hold = held
        self.state.same_underlying_hold_reason = reason
        self.calls.append((held, reason))


def _lane_with_symbol(lane_id: str, symbol: str, *, held: bool = False):
    return SimpleNamespace(
        spec=SimpleNamespace(lane_id=lane_id, symbol=symbol),
        strategy_engine=_FakeStrategyEngine(held=held, reason="legacy hold" if held else None),
    )


def test_runtime_does_not_auto_preserve_same_underlying_hold_for_plain_coexistence(monkeypatch) -> None:
    monkeypatch.setattr(
        probationary_runtime_module,
        "_load_probationary_same_underlying_entry_holds",
        lambda _settings: {},
    )
    lanes = [
        _lane_with_symbol("lane_a", "MGC", held=True),
        _lane_with_symbol("lane_b", "MGC", held=False),
    ]

    _apply_probationary_same_underlying_entry_holds(
        settings=SimpleNamespace(),
        lanes=lanes,
        structured_logger=Mock(),
        alert_dispatcher=Mock(),
    )

    assert lanes[0].strategy_engine.state.same_underlying_entry_hold is False
    assert lanes[1].strategy_engine.state.same_underlying_entry_hold is False
    assert lanes[0].strategy_engine.state.same_underlying_hold_reason is None
    assert lanes[1].strategy_engine.state.same_underlying_hold_reason is None
