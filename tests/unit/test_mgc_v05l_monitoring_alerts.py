"""Tests for operator alert dispatching and runtime alert generation."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mgc_v05l.app.probationary_runtime import _sync_runtime_health_alerts
from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.domain.enums import HealthStatus, LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from mgc_v05l.domain.models import Bar, HealthSnapshot
from mgc_v05l.execution.execution_engine import ExecutionEngine
from mgc_v05l.execution.order_models import FillEvent, OrderIntent
from mgc_v05l.execution.paper_broker import PaperBroker
from mgc_v05l.monitoring.alerts import AlertDispatcher
from mgc_v05l.monitoring.logger import StructuredLogger
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.strategy.strategy_engine import StrategyEngine
from mgc_v05l.strategy.trade_state import build_initial_state


def _build_settings(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    override_path = tmp_path / "override.yaml"
    override_path.write_text(
        f'database_url: "sqlite:///{tmp_path / "alerts.sqlite3"}"\n',
        encoding="utf-8",
    )
    settings = load_settings_from_files([Path("config/base.yaml"), override_path])
    return settings.model_copy(update={"probationary_artifacts_path": tmp_path / "artifacts"})


def _build_runtime(tmp_path: Path):
    settings = _build_settings(tmp_path)
    runtime_identity = {
        "standalone_strategy_id": "test_strategy__MGC",
        "strategy_family": "TEST_RUNTIME",
        "instrument": settings.symbol,
        "lane_id": "lane-1",
    }
    repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    logger = StructuredLogger(settings.probationary_artifacts_path)
    dispatcher = AlertDispatcher(logger, repositories.alerts, source_subsystem="test_runtime")
    execution_engine = ExecutionEngine(PaperBroker())
    state = replace(
        build_initial_state(datetime.now(timezone.utc)),
        strategy_status=StrategyStatus.READY,
        entries_enabled=True,
        exits_enabled=True,
        operator_halt=False,
        reconcile_required=False,
        fault_code=None,
    )
    strategy_engine = StrategyEngine(
        settings=settings,
        initial_state=state,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=dispatcher,
        runtime_identity=runtime_identity,
    )
    return settings, repositories, logger, dispatcher, strategy_engine, execution_engine


def _bar(ts: datetime, *, bar_id: str) -> Bar:
    return Bar(
        bar_id=bar_id,
        symbol="MGC",
        timeframe="5m",
        start_ts=ts,
        end_ts=ts + timedelta(minutes=5),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def _read_alert_rows(logger: StructuredLogger) -> list[dict]:
    path = logger.artifact_dir / "alerts.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _read_alert_state(logger: StructuredLogger) -> dict:
    path = logger.artifact_dir / "alerts_state.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def test_strategy_engine_emits_entry_and_exit_lifecycle_alerts(tmp_path: Path) -> None:
    _, _, logger, _, strategy_engine, _ = _build_runtime(tmp_path)
    opened_bar = _bar(datetime(2026, 3, 26, 14, 0, tzinfo=timezone.utc), bar_id="bar-open")
    entry_intent = strategy_engine.submit_runtime_entry_intent(
        opened_bar,
        side="LONG",
        signal_source="test_entry_signal",
        reason_code="test_entry_reason",
        long_entry_family=LongEntryFamily.K,
    )
    assert entry_intent is not None

    strategy_engine.apply_fill(
        FillEvent(
            order_intent_id=entry_intent.order_intent_id,
            intent_type=OrderIntentType.BUY_TO_OPEN,
            order_status=OrderStatus.FILLED,
            fill_timestamp=opened_bar.end_ts,
            fill_price=Decimal("100.5"),
            broker_order_id=f"paper-{entry_intent.order_intent_id}",
        ),
        signal_bar_id=opened_bar.bar_id,
        long_entry_family=LongEntryFamily.K,
    )

    strategy_engine._state = replace(strategy_engine.state, strategy_status=StrategyStatus.IN_LONG_K)  # noqa: SLF001
    flatten_intent = strategy_engine.submit_operator_flatten_intent(
        occurred_at=opened_bar.end_ts + timedelta(minutes=5),
        reason_code="test_exit_reason",
    )
    assert flatten_intent.intent_type is OrderIntentType.SELL_TO_CLOSE
    strategy_engine.apply_fill(
        FillEvent(
            order_intent_id=flatten_intent.order_intent_id,
            intent_type=OrderIntentType.SELL_TO_CLOSE,
            order_status=OrderStatus.FILLED,
            fill_timestamp=opened_bar.end_ts + timedelta(minutes=5),
            fill_price=Decimal("101.0"),
            broker_order_id=f"paper-{flatten_intent.order_intent_id}",
        ),
    )

    codes = [row["code"] for row in _read_alert_rows(logger)]
    assert "entry_created" in codes
    assert "entry_submitted" in codes
    assert "entry_filled" in codes
    assert "exit_created" in codes
    assert "exit_submitted" in codes
    assert "exit_filled" in codes


def test_strategy_engine_emits_order_rejection_alert(tmp_path: Path) -> None:
    _, _, logger, _, strategy_engine, _ = _build_runtime(tmp_path)
    first_bar = _bar(datetime(2026, 3, 26, 14, 0, tzinfo=timezone.utc), bar_id="bar-1")
    second_bar = _bar(datetime(2026, 3, 26, 14, 5, tzinfo=timezone.utc), bar_id="bar-2")
    assert strategy_engine.submit_runtime_entry_intent(first_bar, side="LONG", signal_source="sig-a", reason_code="entry-a") is not None
    strategy_engine._state = replace(strategy_engine.state, open_broker_order_id=None)  # noqa: SLF001
    rejected = strategy_engine.submit_runtime_entry_intent(second_bar, side="LONG", signal_source="sig-b", reason_code="entry-b")
    assert rejected is None

    rows = _read_alert_rows(logger)
    rejection_rows = [row for row in rows if row["code"] == "order_rejection"]
    assert rejection_rows
    assert rejection_rows[-1]["category"] == "order_rejection"
    assert "rejected before broker submission" in rejection_rows[-1]["message"]


def test_alert_dispatcher_deduplicates_and_resolves_stateful_alerts(tmp_path: Path) -> None:
    logger = StructuredLogger(tmp_path / "artifacts")
    dispatcher = AlertDispatcher(logger, source_subsystem="test")
    occurred_at = datetime(2026, 3, 26, 15, 0, tzinfo=timezone.utc)

    dispatcher.sync_condition(
        code="market_data_stale",
        active=True,
        severity="ACTION",
        category="market_data",
        title="Market Data Stale",
        message="Completed bars stopped advancing.",
        dedup_key="market-data",
        occurred_at=occurred_at,
    )
    dispatcher.sync_condition(
        code="market_data_stale",
        active=True,
        severity="ACTION",
        category="market_data",
        title="Market Data Stale",
        message="Completed bars stopped advancing.",
        dedup_key="market-data",
        occurred_at=occurred_at + timedelta(seconds=30),
    )
    dispatcher.sync_condition(
        code="market_data_stale",
        active=False,
        severity="RECOVERY",
        category="market_data",
        title="Market Data Recovered",
        message="Completed bars are advancing again.",
        dedup_key="market-data",
        occurred_at=occurred_at + timedelta(minutes=6),
    )

    rows = _read_alert_rows(logger)
    assert [row["state_transition"] for row in rows] == ["opened", "resolved"]
    state = _read_alert_state(logger)
    assert state["active_alerts"] == []
    assert state["by_key"]["market-data"]["occurrence_count"] == 3


def test_sync_runtime_health_alerts_emits_market_data_and_broker_disconnect_alerts(tmp_path: Path) -> None:
    logger = StructuredLogger(tmp_path / "artifacts")
    dispatcher = AlertDispatcher(logger, source_subsystem="test_runtime")
    occurred_at = datetime(2026, 3, 26, 16, 0, tzinfo=timezone.utc)
    unhealthy = HealthSnapshot(
        market_data_ok=False,
        broker_ok=False,
        persistence_ok=True,
        reconciliation_clean=True,
        invariants_ok=True,
        health_status=HealthStatus.DEGRADED,
    )
    healthy = HealthSnapshot(
        market_data_ok=True,
        broker_ok=True,
        persistence_ok=True,
        reconciliation_clean=True,
        invariants_ok=True,
        health_status=HealthStatus.HEALTHY,
    )

    _sync_runtime_health_alerts(
        alert_dispatcher=dispatcher,
        snapshot=unhealthy,
        runtime_name="paper_runtime",
        occurred_at=occurred_at,
        operator_status_path="operator_status.json",
    )
    _sync_runtime_health_alerts(
        alert_dispatcher=dispatcher,
        snapshot=healthy,
        runtime_name="paper_runtime",
        occurred_at=occurred_at + timedelta(minutes=10),
        operator_status_path="operator_status.json",
    )

    rows = _read_alert_rows(logger)
    categories = [row["category"] for row in rows]
    assert "market_data" in categories
    assert "broker_connectivity" in categories
    assert any(row["state_transition"] == "resolved" for row in rows)


def test_reconciliation_alerts_cover_safe_repair_and_active_mismatch(tmp_path: Path) -> None:
    _, repositories, logger, dispatcher, strategy_engine, execution_engine = _build_runtime(tmp_path)
    now = datetime.now(timezone.utc)

    intent = OrderIntent(
        order_intent_id="intent-stale",
        bar_id="bar-safe",
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=now,
        reason_code="reason",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-stale")
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
        open_broker_order_id="paper-stale",
    )
    execution_engine.broker.restore_state(position=execution_engine.broker.get_position(), open_order_ids=[], order_status={}, last_fill_timestamp=None)
    payload = strategy_engine.apply_reconciliation(occurred_at=now, trigger="startup", execution_engine=execution_engine)
    assert payload["classification"] == "safe_repair"

    _, _, mismatch_logger, _, mismatch_engine, mismatch_execution_engine = _build_runtime(tmp_path / "mismatch")
    mismatch_engine._state = replace(  # noqa: SLF001
        mismatch_engine.state,
        position_side=PositionSide.LONG,
        internal_position_qty=1,
        broker_position_qty=1,
        entry_price=Decimal("100"),
        strategy_status=StrategyStatus.IN_LONG_K,
    )
    mismatch_execution_engine.broker.restore_state(
        position=mismatch_execution_engine.broker.get_position().__class__(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )
    payload = mismatch_engine.apply_reconciliation(
        occurred_at=now + timedelta(minutes=5),
        trigger="scheduled_heartbeat",
        execution_engine=mismatch_execution_engine,
    )
    assert payload["classification"] == "unsafe_ambiguity"

    rows = _read_alert_rows(logger) + _read_alert_rows(mismatch_logger)
    codes = [row["code"] for row in rows]
    assert "safe_repair_performed" in codes
    assert "strategy_reconciliation_mismatch" in codes
