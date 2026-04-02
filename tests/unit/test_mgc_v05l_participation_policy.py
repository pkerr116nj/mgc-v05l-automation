from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mgc_v05l.config_models import ParticipationPolicy, RuntimeMode, StrategySettings, load_settings_from_files
from mgc_v05l.domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from mgc_v05l.domain.models import Bar
from mgc_v05l.execution.reconciliation import (
    BrokerReconciliationSnapshot,
    InternalReconciliationSnapshot,
    RECONCILIATION_CLASS_FILL_ACK_UNCERTAINTY,
    ReconciliationCoordinator,
)
from mgc_v05l.app.strategy_runtime_registry import build_runtime_settings, build_standalone_strategy_definitions
from mgc_v05l.execution.order_models import FillEvent
from mgc_v05l.persistence import build_engine
from mgc_v05l.persistence.repositories import RepositorySet, decode_fill
from mgc_v05l.strategy.reconcile import StrategyReconciler
from mgc_v05l.strategy.strategy_engine import StrategyEngine


def _settings(tmp_path: Path, **updates: object) -> StrategySettings:
    base = load_settings_from_files([Path("config/base.yaml")])
    default_updates = {
        "mode": RuntimeMode.PAPER,
        "database_url": f"sqlite:///{tmp_path / 'participation.sqlite3'}",
        "probationary_artifacts_dir": str(tmp_path / "outputs"),
        "participation_policy": ParticipationPolicy.STAGED_SAME_DIRECTION,
        "max_concurrent_entries": 2,
        "max_adds_after_entry": 1,
    }
    default_updates.update(updates)
    return base.model_copy(update=default_updates)


def _bar(index: int, *, close: str = "100") -> Bar:
    end_ts = datetime(2026, 4, 1, 10, 0, tzinfo=timezone.utc) + timedelta(minutes=5 * index)
    start_ts = end_ts - timedelta(minutes=5)
    return Bar(
        bar_id=f"MGC|5m|{index}",
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal(close),
        high=Decimal(close),
        low=Decimal(close),
        close=Decimal(close),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )


def test_staged_same_direction_lane_can_add_without_flat_reset(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    engine = StrategyEngine(settings=settings, repositories=repositories)

    bar_1 = _bar(1, close="100")
    intent_1 = engine.submit_runtime_entry_intent(
        bar_1,
        side="LONG",
        signal_source="paperTestLong",
        reason_code="paperTestLong",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent_1 is not None
    engine.apply_fill(
        FillEvent(
            order_intent_id=intent_1.order_intent_id,
            intent_type=intent_1.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=bar_1.end_ts,
            fill_price=Decimal("100"),
            broker_order_id="paper-1",
            quantity=intent_1.quantity,
        ),
        signal_bar_id=bar_1.bar_id,
        long_entry_family=LongEntryFamily.K,
    )
    engine._execution_engine.clear_intent(intent_1.order_intent_id)  # noqa: SLF001

    bar_2 = _bar(2, close="102")
    intent_2 = engine.submit_runtime_entry_intent(
        bar_2,
        side="LONG",
        signal_source="paperTestLongAdd",
        reason_code="paperTestLongAdd",
        long_entry_family=LongEntryFamily.K,
    )

    assert intent_2 is not None
    engine.apply_fill(
        FillEvent(
            order_intent_id=intent_2.order_intent_id,
            intent_type=intent_2.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=bar_2.end_ts,
            fill_price=Decimal("102"),
            broker_order_id="paper-2",
            quantity=intent_2.quantity,
        ),
        signal_bar_id=bar_2.bar_id,
        long_entry_family=LongEntryFamily.K,
    )

    assert engine.state.position_side is PositionSide.LONG
    assert engine.state.strategy_status is StrategyStatus.IN_LONG_K
    assert engine.state.internal_position_qty == 2
    assert len(engine.state.open_entry_legs) == 2
    assert engine.state.entry_price == Decimal("101")


def test_single_entry_policy_keeps_benchmark_style_lane_flat_only(tmp_path: Path) -> None:
    settings = _settings(
        tmp_path,
        participation_policy=ParticipationPolicy.SINGLE_ENTRY_ONLY,
        max_concurrent_entries=1,
        max_adds_after_entry=0,
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    engine = StrategyEngine(settings=settings, repositories=repositories)

    bar_1 = _bar(1)
    intent_1 = engine.submit_runtime_entry_intent(
        bar_1,
        side="LONG",
        signal_source="paperTestLong",
        reason_code="paperTestLong",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent_1 is not None
    engine.apply_fill(
        FillEvent(
            order_intent_id=intent_1.order_intent_id,
            intent_type=intent_1.intent_type,
            order_status=OrderStatus.FILLED,
            fill_timestamp=bar_1.end_ts,
            fill_price=Decimal("100"),
            broker_order_id="paper-1",
            quantity=intent_1.quantity,
        ),
        signal_bar_id=bar_1.bar_id,
        long_entry_family=LongEntryFamily.K,
    )
    engine._execution_engine.clear_intent(intent_1.order_intent_id)  # noqa: SLF001

    assert (
        engine.submit_runtime_entry_intent(
            _bar(2, close="101"),
            side="LONG",
            signal_source="paperTestLongAdd",
            reason_code="paperTestLongAdd",
            long_entry_family=LongEntryFamily.K,
        )
        is None
    )


def test_staged_state_round_trips_through_persistence_and_restore(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    runtime_identity = {"standalone_strategy_id": "test_lane__mgc", "instrument": "MGC", "lane_id": "test_lane"}
    repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    engine = StrategyEngine(settings=settings, repositories=repositories, runtime_identity=runtime_identity)

    for index, price in enumerate((Decimal("100"), Decimal("101")), start=1):
        bar = _bar(index, close=str(price))
        intent = engine.submit_runtime_entry_intent(
            bar,
            side="LONG",
            signal_source=f"paperTestLong{index}",
            reason_code=f"paperTestLong{index}",
            long_entry_family=LongEntryFamily.K,
        )
        assert intent is not None
        engine.apply_fill(
            FillEvent(
                order_intent_id=intent.order_intent_id,
                intent_type=intent.intent_type,
                order_status=OrderStatus.FILLED,
                fill_timestamp=bar.end_ts,
                fill_price=price,
                broker_order_id=f"paper-{index}",
                quantity=intent.quantity,
            ),
            signal_bar_id=bar.bar_id,
            long_entry_family=LongEntryFamily.K,
        )
        engine._execution_engine.clear_intent(intent.order_intent_id)  # noqa: SLF001

    restored_repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    restored = StrategyEngine(settings=settings, repositories=restored_repositories, runtime_identity=runtime_identity)

    assert restored.state.position_side is PositionSide.LONG
    assert restored.state.internal_position_qty == 2
    assert len(restored.state.open_entry_legs) == 2
    assert restored.state.open_entry_legs[0].entry_price == Decimal("100")
    assert restored.state.open_entry_legs[1].entry_price == Decimal("101")


def test_lane_level_participation_settings_flow_through_runtime_definition_builder(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    definitions = build_standalone_strategy_definitions(
        settings,
        runtime_lanes=[
            {
                "lane_id": "staged_mgc_long",
                "display_name": "Staged MGC Long",
                "symbol": "MGC",
                "runtime_kind": "strategy_engine",
                "long_sources": ["usLatePauseResumeLongTurn"],
                "short_sources": [],
                "session_restriction": "US",
                "allowed_sessions": ["US"],
                "trade_size": 1,
                "participation_policy": "STAGED_SAME_DIRECTION",
                "max_concurrent_entries": 3,
                "max_position_quantity": 3,
                "max_adds_after_entry": 2,
                "add_direction_policy": "SAME_DIRECTION_ONLY",
            }
        ],
    )

    definition = definitions[0]
    lane_settings = build_runtime_settings(settings, definition)

    assert definition.participation_policy is ParticipationPolicy.STAGED_SAME_DIRECTION
    assert definition.max_concurrent_entries == 3
    assert definition.max_position_quantity == 3
    assert definition.max_adds_after_entry == 2
    assert lane_settings.participation_policy is ParticipationPolicy.STAGED_SAME_DIRECTION
    assert lane_settings.max_concurrent_entries == 3
    assert lane_settings.max_position_quantity == 3
    assert lane_settings.max_adds_after_entry == 2


def test_fill_repository_round_trips_fill_quantity_without_mutation(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    fill = FillEvent(
        order_intent_id="fill-1",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        order_status=OrderStatus.FILLED,
        fill_timestamp=datetime(2026, 4, 1, 10, 5, tzinfo=timezone.utc),
        fill_price=Decimal("100"),
        broker_order_id="paper-fill-1",
        quantity=2,
    )

    repositories.fills.save(fill)
    persisted = repositories.fills.list_all()[0]
    decoded = decode_fill(persisted)

    assert int(persisted["quantity"]) == 2
    assert decoded.quantity == 2


def test_missing_fill_ack_on_one_add_leg_still_escalates_as_fill_ack_uncertainty() -> None:
    coordinator = ReconciliationCoordinator()

    outcome = coordinator.evaluate(
        trigger="heartbeat",
        internal=InternalReconciliationSnapshot(
            strategy_status=StrategyStatus.IN_LONG_K.value,
            position_side=PositionSide.LONG.value,
            expected_signed_quantity=1,
            internal_position_qty=1,
            broker_position_qty=1,
            average_price="100",
            open_broker_order_id=None,
            persisted_open_order_ids=(),
            pending_execution_open_order_ids=(),
            last_fill_timestamp="2026-04-01T10:05:00+00:00",
            last_order_intent_id="intent-1",
            entries_enabled=True,
            exits_enabled=True,
            operator_halt=False,
            reconcile_required=False,
            fault_code=None,
        ),
        broker=BrokerReconciliationSnapshot(
            connected=True,
            truth_complete=True,
            position_quantity=2,
            side=PositionSide.LONG.value,
            average_price="101",
            open_order_ids=(),
            order_status={},
            last_fill_timestamp="2026-04-01T10:10:00+00:00",
        ),
    )

    assert outcome.classification == RECONCILIATION_CLASS_FILL_ACK_UNCERTAINTY
    assert "broker_position_quantity_mismatch" in outcome.mismatches


def test_staged_reconciliation_payload_exposes_leg_structure(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    engine = StrategyEngine(settings=settings, repositories=repositories)

    for index, price in enumerate((Decimal("100"), Decimal("101")), start=1):
        bar = _bar(index, close=str(price))
        intent = engine.submit_runtime_entry_intent(
            bar,
            side="LONG",
            signal_source=f"paperTestLong{index}",
            reason_code=f"paperTestLong{index}",
            long_entry_family=LongEntryFamily.K,
        )
        assert intent is not None
        engine.apply_fill(
            FillEvent(
                order_intent_id=intent.order_intent_id,
                intent_type=intent.intent_type,
                order_status=OrderStatus.FILLED,
                fill_timestamp=bar.end_ts,
                fill_price=price,
                broker_order_id=f"paper-{index}",
                quantity=intent.quantity,
            ),
            signal_bar_id=bar.bar_id,
            long_entry_family=LongEntryFamily.K,
        )
        engine._execution_engine.clear_intent(intent.order_intent_id)  # noqa: SLF001

    payload = StrategyReconciler().inspect(
        state=engine.state,
        occurred_at=datetime(2026, 4, 1, 10, 15, tzinfo=timezone.utc),
        trigger="heartbeat",
        execution_engine=engine._execution_engine,  # noqa: SLF001
    )

    assert payload["internal_snapshot"]["open_entry_leg_count"] == 2
    assert payload["internal_snapshot"]["open_entry_leg_quantities"] == [1, 1]
