"""Tests for reconciliation workflow behavior."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from sqlalchemy import select

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.app.probationary_runtime import _run_order_timeout_watchdog, _run_reconciliation_heartbeat
from mgc_v05l.domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from mgc_v05l.execution.execution_engine import ExecutionEngine, PendingExecution
from mgc_v05l.execution.order_models import FillEvent, OrderIntent
from mgc_v05l.execution.paper_broker import PaperBroker, PaperPosition
from mgc_v05l.monitoring.alerts import AlertDispatcher
from mgc_v05l.monitoring.logger import StructuredLogger
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import fault_events_table, reconciliation_events_table
from mgc_v05l.strategy.strategy_engine import StrategyEngine
from mgc_v05l.strategy.trade_state import build_initial_state


def _build_settings(tmp_path: Path):
    override_path = tmp_path / "override.yaml"
    override_path.write_text(
        f'database_url: "sqlite:///{tmp_path / "reconciliation.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), override_path])


def _build_runtime(tmp_path: Path):
    settings = _build_settings(tmp_path)
    runtime_identity = {
        "standalone_strategy_id": "test_strategy__MGC",
        "strategy_family": "TEST_RUNTIME",
        "instrument": settings.symbol,
        "lane_id": "lane-1",
    }
    repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    execution_engine = ExecutionEngine(PaperBroker())
    state = replace(build_initial_state(datetime.now(timezone.utc)), strategy_status=StrategyStatus.READY)
    strategy_engine = StrategyEngine(
        settings=settings,
        initial_state=state,
        repositories=repositories,
        execution_engine=execution_engine,
        runtime_identity=runtime_identity,
    )
    return settings, repositories, strategy_engine, execution_engine


def _build_watchdog_runtime(tmp_path: Path):
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    logger = StructuredLogger(tmp_path / "artifacts")
    dispatcher = AlertDispatcher(logger, repositories.alerts, source_subsystem="test_runtime")
    return settings, repositories, logger, dispatcher, strategy_engine, execution_engine


def _make_intent(created_at: datetime, order_intent_id: str = "intent-1") -> OrderIntent:
    return OrderIntent(
        order_intent_id=order_intent_id,
        bar_id=f"bar-{order_intent_id}",
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=created_at,
        reason_code="test_reason",
    )


def _read_alert_rows(logger: StructuredLogger) -> list[dict]:
    path = logger.artifact_dir / "alerts.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_startup_reconciliation_clean_when_internal_and_broker_are_aligned(tmp_path: Path) -> None:
    _, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id=None,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="startup",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "clean"
    assert strategy_engine.state.strategy_status is StrategyStatus.READY
    assert strategy_engine.state.reconcile_required is False
    with repositories.engine.begin() as connection:
        rows = connection.execute(select(reconciliation_events_table)).mappings().all()
    assert len(rows) == 1


def test_startup_reconciliation_safely_clears_stale_internal_pending_order_marker(tmp_path: Path) -> None:
    _, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    intent = _make_intent(now, "intent-stale")
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-intent-stale")
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id="paper-intent-stale",
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="startup",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "safe_repair"
    assert "clear_stale_open_order_markers" in payload["repair_actions"]
    assert strategy_engine.state.open_broker_order_id is None
    assert strategy_engine.state.strategy_status is StrategyStatus.READY


def test_internal_broker_quantity_mismatch_moves_to_reconciling_and_freezes_entries(tmp_path: Path) -> None:
    _, _, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="startup",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "unsafe_ambiguity"
    assert strategy_engine.state.strategy_status is StrategyStatus.RECONCILING
    assert strategy_engine.state.entries_enabled is False
    assert strategy_engine.state.reconcile_required is True


def test_missing_fill_acknowledgement_triggers_reconciling(tmp_path: Path) -> None:
    _, _, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=2, average_price=Decimal("101")),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=now,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="missing_fill_ack",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "fill_ack_uncertainty"
    assert strategy_engine.state.strategy_status is StrategyStatus.RECONCILING
    assert strategy_engine.state.entries_enabled is False


def test_unresolved_open_order_ambiguity_stays_reconciling(tmp_path: Path) -> None:
    _, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    intent = _make_intent(now, "intent-open")
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-intent-open")
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id="paper-intent-open",
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=["paper-broker-other"],
        order_status={"paper-broker-other": OrderStatus.ACKNOWLEDGED},
        last_fill_timestamp=None,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="scheduled_heartbeat",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "open_order_uncertainty"
    assert strategy_engine.state.strategy_status is StrategyStatus.RECONCILING
    assert strategy_engine.state.entries_enabled is False


def test_safe_flat_repair_returns_to_ready(tmp_path: Path) -> None:
    _, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    entry_intent = _make_intent(now, "intent-entry")
    repositories.order_intents.save(entry_intent, order_status=OrderStatus.FILLED, broker_order_id="paper-intent-entry")
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=now,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="scheduled_heartbeat",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "safe_repair"
    assert "confirm_flat_from_broker_fill" in payload["repair_actions"]
    assert strategy_engine.state.strategy_status is StrategyStatus.READY
    assert strategy_engine.state.position_side is PositionSide.FLAT
    assert strategy_engine.state.internal_position_qty == 0


def test_unsafe_opposite_side_ambiguity_enters_fault_and_persists_fault_event(tmp_path: Path) -> None:
    _, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=-1, average_price=Decimal("99")),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    payload = strategy_engine.apply_reconciliation(
        occurred_at=now,
        trigger="position_mismatch",
        execution_engine=execution_engine,
    )

    assert payload["classification"] == "unsafe_ambiguity"
    assert strategy_engine.state.strategy_status is StrategyStatus.FAULT
    assert strategy_engine.state.fault_code == "reconciliation_unsafe_opposite_side_exposure"
    with repositories.engine.begin() as connection:
        fault_rows = connection.execute(select(fault_events_table)).mappings().all()
    assert len(fault_rows) == 1


def test_heartbeat_reconcile_clean_is_quiet_when_state_is_already_aligned(tmp_path: Path) -> None:
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)

    heartbeat_status, payload, ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=None,
        occurred_at=now,
    )

    assert ran is True
    assert payload is not None
    assert payload["classification"] == "clean"
    assert heartbeat_status["status"] == "CLEAN"
    assert heartbeat_status["reconciliation_applied"] is False
    with repositories.engine.begin() as connection:
        recon_rows = connection.execute(select(reconciliation_events_table)).mappings().all()
    assert recon_rows == []


def test_heartbeat_reconcile_safe_repair_applies_and_persists_event(tmp_path: Path) -> None:
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    intent = _make_intent(now, "intent-heartbeat-stale")
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-intent-heartbeat-stale")
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id="paper-intent-heartbeat-stale",
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    heartbeat_status, payload, ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=None,
        occurred_at=now,
    )

    assert ran is True
    assert payload is not None
    assert payload["classification"] == "safe_repair"
    assert heartbeat_status["status"] == "SAFE_REPAIR"
    assert heartbeat_status["reconciliation_applied"] is True
    assert strategy_engine.state.open_broker_order_id is None
    recon_rows = repositories.reconciliation_events.list_all()
    assert len(recon_rows) == 1
    assert recon_rows[0]["trigger"] == "heartbeat"


def test_heartbeat_reconcile_unresolved_mismatch_moves_to_reconciling(tmp_path: Path) -> None:
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    heartbeat_status, payload, ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=None,
        occurred_at=now,
    )

    assert ran is True
    assert payload is not None
    assert payload["classification"] == "unsafe_ambiguity"
    assert heartbeat_status["status"] == "RECONCILING"
    assert strategy_engine.state.strategy_status is StrategyStatus.RECONCILING
    assert strategy_engine.state.entries_enabled is False
    with repositories.engine.begin() as connection:
        recon_rows = connection.execute(select(reconciliation_events_table)).mappings().all()
    assert len(recon_rows) == 1


def test_heartbeat_reconcile_unsafe_ambiguity_enters_fault(tmp_path: Path) -> None:
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=-1, average_price=Decimal("99")),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    heartbeat_status, payload, ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=None,
        occurred_at=now,
    )

    assert ran is True
    assert payload is not None
    assert payload["classification"] == "unsafe_ambiguity"
    assert heartbeat_status["status"] == "FAULT"
    assert strategy_engine.state.strategy_status is StrategyStatus.FAULT
    with repositories.engine.begin() as connection:
        fault_rows = connection.execute(select(fault_events_table)).mappings().all()
    assert len(fault_rows) == 1


def test_heartbeat_reconcile_degrades_cleanly_when_broker_truth_is_unavailable(tmp_path: Path) -> None:
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    execution_engine.broker.disconnect()

    heartbeat_status, payload, ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=None,
        occurred_at=now,
    )

    assert ran is True
    assert payload is not None
    assert payload["classification"] == "broker_unavailable_incomplete_truth"
    assert heartbeat_status["status"] == "BROKER_UNAVAILABLE"
    assert heartbeat_status["manual_action_required"] is False
    assert strategy_engine.state.strategy_status is StrategyStatus.RECONCILING
    with repositories.engine.begin() as connection:
        recon_rows = connection.execute(select(reconciliation_events_table)).mappings().all()
    assert len(recon_rows) == 1


def test_repeated_identical_heartbeat_mismatch_does_not_persist_duplicate_events(tmp_path: Path) -> None:
    settings, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    heartbeat_status, first_payload, first_ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=None,
        occurred_at=now,
    )
    assert first_ran is True
    assert first_payload is not None
    second_status, second_payload, second_ran = _run_reconciliation_heartbeat(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        heartbeat_status=heartbeat_status,
        occurred_at=now + timedelta(seconds=settings.reconciliation_heartbeat_interval_seconds),
    )

    assert second_ran is True
    assert second_payload is not None
    assert second_payload["classification"] == "unsafe_ambiguity"
    assert second_status["status"] == "RECONCILING"
    assert second_status["reconciliation_applied"] is False
    with repositories.engine.begin() as connection:
        recon_rows = connection.execute(select(reconciliation_events_table)).mappings().all()
    assert len(recon_rows) == 1


def test_manual_force_reconcile_path_records_audit_event(tmp_path: Path) -> None:
    _, repositories, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        strategy_status=StrategyStatus.RECONCILING,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id=None,
    )

    payload = strategy_engine.force_reconcile(
        occurred_at=now,
        execution_engine=execution_engine,
    )

    assert payload["trigger"] == "manual_force_reconcile"
    assert payload["classification"] == "clean"
    with repositories.engine.begin() as connection:
        recon_rows = connection.execute(select(reconciliation_events_table)).mappings().all()
    assert len(recon_rows) == 1


def test_order_timeout_watchdog_stays_quiet_for_normal_ack_fill_lifecycle(tmp_path: Path) -> None:
    settings, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_watchdog_runtime(tmp_path)
    now = datetime.now(timezone.utc)
    intent = _make_intent(now, "intent-normal")
    pending = execution_engine.submit_intent(intent)
    assert pending is not None
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at,
        broker_order_status=pending.broker_order_status,
        last_status_checked_at=pending.last_status_checked_at,
        retry_count=pending.retry_count,
    )
    fill = execution_engine.broker.fill_order(intent, Decimal("100.0"), now + timedelta(seconds=5))
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.FILLED,
        broker_order_id=fill.broker_order_id,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at,
        broker_order_status=OrderStatus.FILLED.value,
        last_status_checked_at=fill.fill_timestamp,
        retry_count=pending.retry_count,
    )
    repositories.fills.save(fill)
    execution_engine.clear_intent(intent.order_intent_id)

    status, event, ran = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=None,
        occurred_at=now + timedelta(minutes=1),
    )

    assert ran is True
    assert event is None
    assert status["status"] == "HEALTHY"
    assert status["overdue_ack_count"] == 0
    assert status["overdue_fill_count"] == 0
    assert _read_alert_rows(logger) == []


def test_order_timeout_watchdog_ack_overdue_but_broker_open_order_is_safely_acknowledged(tmp_path: Path) -> None:
    settings, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_watchdog_runtime(tmp_path)
    now = datetime.now(timezone.utc) - timedelta(seconds=settings.order_ack_timeout_seconds + 5)
    intent = _make_intent(now, "intent-ack-open")
    pending = PendingExecution(
        intent=intent,
        broker_order_id="paper-intent-ack-open",
        submitted_at=now,
        acknowledged_at=None,
        broker_order_status=None,
        last_status_checked_at=None,
        retry_count=0,
        signal_bar_id=intent.bar_id,
        long_entry_family=LongEntryFamily.K,
        short_entry_family=LongEntryFamily.NONE,
        short_entry_source=None,
    )
    execution_engine.restore_pending_execution(pending)
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.PENDING,
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        retry_count=0,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[pending.broker_order_id],
        order_status={pending.broker_order_id: OrderStatus.ACKNOWLEDGED},
        last_fill_timestamp=None,
    )

    status, event, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=None,
        occurred_at=now + timedelta(seconds=settings.order_ack_timeout_seconds + 10),
    )

    assert status["status"] == "SAFE_REPAIR"
    assert status["safe_repair_count"] == 1
    assert event is not None
    assert event["repair_action"] == "record_broker_acknowledgement"
    saved_row = repositories.order_intents.list_all()[0]
    assert saved_row["order_status"] == OrderStatus.ACKNOWLEDGED.value
    assert saved_row["acknowledged_at"] is not None


def test_order_timeout_watchdog_stale_pending_flat_no_open_order_safely_cleans_up(tmp_path: Path) -> None:
    settings, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_watchdog_runtime(tmp_path)
    now = datetime.now(timezone.utc) - timedelta(seconds=settings.order_ack_timeout_seconds + 5)
    intent = _make_intent(now, "intent-stale-flat")
    pending = PendingExecution(
        intent=intent,
        broker_order_id="paper-intent-stale-flat",
        submitted_at=now,
        acknowledged_at=None,
        broker_order_status=None,
        last_status_checked_at=None,
        retry_count=0,
        signal_bar_id=intent.bar_id,
        long_entry_family=LongEntryFamily.K,
        short_entry_family=LongEntryFamily.NONE,
        short_entry_source=None,
    )
    execution_engine.restore_pending_execution(pending)
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.PENDING,
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        retry_count=0,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id=pending.broker_order_id,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    status, event, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=None,
        occurred_at=now + timedelta(seconds=settings.order_ack_timeout_seconds + 10),
    )

    assert status["status"] == "SAFE_REPAIR"
    assert event is not None
    assert event["repair_action"] == "clear_stale_pending_intent"
    assert execution_engine.pending_executions() == []
    saved_row = repositories.order_intents.list_all()[0]
    assert saved_row["order_status"] == OrderStatus.CANCELLED.value
    assert strategy_engine.state.open_broker_order_id is None


def test_order_timeout_watchdog_fill_timeout_escalates_to_reconciling_when_unresolved(tmp_path: Path) -> None:
    settings, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_watchdog_runtime(tmp_path)
    now = datetime.now(timezone.utc) - timedelta(seconds=settings.order_fill_timeout_seconds + settings.order_timeout_reconcile_grace_seconds + 5)
    intent = _make_intent(now, "intent-fill-timeout")
    pending = PendingExecution(
        intent=intent,
        broker_order_id="paper-intent-fill-timeout",
        submitted_at=now,
        acknowledged_at=now + timedelta(seconds=1),
        broker_order_status=OrderStatus.ACKNOWLEDGED.value,
        last_status_checked_at=now + timedelta(seconds=1),
        retry_count=0,
        signal_bar_id=intent.bar_id,
        long_entry_family=LongEntryFamily.K,
        short_entry_family=LongEntryFamily.NONE,
        short_entry_source=None,
    )
    execution_engine.restore_pending_execution(pending)
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at,
        broker_order_status=pending.broker_order_status,
        last_status_checked_at=pending.last_status_checked_at,
        retry_count=0,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        open_broker_order_id=pending.broker_order_id,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=1, average_price=Decimal("100")),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )

    status, event, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=None,
        occurred_at=now + timedelta(seconds=settings.order_fill_timeout_seconds + settings.order_timeout_reconcile_grace_seconds + 20),
    )

    assert status["status"] == "RECONCILING"
    assert event is not None
    assert event["reconciliation_trigger"] == "fill_timeout"
    assert strategy_engine.state.strategy_status is StrategyStatus.RECONCILING
    assert strategy_engine.state.entries_enabled is False


def test_order_timeout_watchdog_unsafe_ambiguity_enters_fault(tmp_path: Path) -> None:
    settings, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_watchdog_runtime(tmp_path)
    now = datetime.now(timezone.utc) - timedelta(seconds=settings.order_fill_timeout_seconds + settings.order_timeout_reconcile_grace_seconds + 5)
    intent = OrderIntent(
        order_intent_id="intent-exit-timeout",
        bar_id="bar-exit-timeout",
        symbol="MGC",
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        quantity=1,
        created_at=now,
        reason_code="test_exit_timeout",
    )
    pending = PendingExecution(
        intent=intent,
        broker_order_id="paper-intent-exit-timeout",
        submitted_at=now,
        acknowledged_at=now + timedelta(seconds=1),
        broker_order_status=OrderStatus.ACKNOWLEDGED.value,
        last_status_checked_at=now + timedelta(seconds=1),
        retry_count=0,
        signal_bar_id=None,
        long_entry_family=LongEntryFamily.NONE,
        short_entry_family=LongEntryFamily.NONE,
        short_entry_source=None,
    )
    execution_engine.restore_pending_execution(pending)
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at,
        broker_order_status=pending.broker_order_status,
        last_status_checked_at=pending.last_status_checked_at,
        retry_count=0,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        open_broker_order_id=pending.broker_order_id,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=-1, average_price=Decimal("99")),
        open_order_ids=[pending.broker_order_id],
        order_status={pending.broker_order_id: OrderStatus.ACKNOWLEDGED},
        last_fill_timestamp=None,
    )

    status, event, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=None,
        occurred_at=now + timedelta(seconds=settings.order_fill_timeout_seconds + settings.order_timeout_reconcile_grace_seconds + 20),
    )

    assert status["status"] == "FAULT"
    assert event is not None
    assert event["resulting_state"] == "FAULT"
    assert strategy_engine.state.strategy_status is StrategyStatus.FAULT


def test_repeated_identical_order_timeout_condition_does_not_spam_alerts(tmp_path: Path) -> None:
    settings, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_watchdog_runtime(tmp_path)
    now = datetime.now(timezone.utc) - timedelta(seconds=settings.order_fill_timeout_seconds + 1)
    intent = _make_intent(now, "intent-open-fill-delay")
    pending = PendingExecution(
        intent=intent,
        broker_order_id="paper-intent-open-fill-delay",
        submitted_at=now,
        acknowledged_at=now + timedelta(seconds=1),
        broker_order_status=OrderStatus.ACKNOWLEDGED.value,
        last_status_checked_at=now + timedelta(seconds=1),
        retry_count=0,
        signal_bar_id=intent.bar_id,
        long_entry_family=LongEntryFamily.K,
        short_entry_family=LongEntryFamily.NONE,
        short_entry_source=None,
    )
    execution_engine.restore_pending_execution(pending)
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at,
        broker_order_status=pending.broker_order_status,
        last_status_checked_at=pending.last_status_checked_at,
        retry_count=0,
    )
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[pending.broker_order_id],
        order_status={pending.broker_order_id: OrderStatus.ACKNOWLEDGED},
        last_fill_timestamp=None,
    )

    first_status, _, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=None,
        occurred_at=now + timedelta(seconds=settings.order_fill_timeout_seconds + 5),
    )
    second_status, _, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        watchdog_status=first_status,
        occurred_at=now + timedelta(seconds=settings.order_fill_timeout_seconds + 15),
    )

    rows = [row for row in _read_alert_rows(logger) if row.get("category") == "fill_timeout"]
    assert first_status["status"] == "ACTIVE_TIMEOUTS"
    assert second_status["status"] == "ACTIVE_TIMEOUTS"
    assert len(rows) == 1
