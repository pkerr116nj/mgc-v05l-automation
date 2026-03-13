"""State-machine and invariant tests for the Phase 3A skeleton."""

from datetime import datetime, timezone
from decimal import Decimal

from mgc_v05l.domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from mgc_v05l.execution.order_models import FillEvent
from mgc_v05l.strategy.invariants import validate_state
from mgc_v05l.strategy.state_machine import transition_on_entry_fill, transition_on_exit_fill
from mgc_v05l.strategy.trade_state import build_initial_state


def test_long_entry_fill_transitions_state_to_in_long_k() -> None:
    now = datetime.now(timezone.utc)
    state = build_initial_state(now)
    fill = FillEvent(
        order_intent_id="intent-1",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        order_status=OrderStatus.FILLED,
        fill_timestamp=now,
        fill_price=Decimal("3000.50"),
        broker_order_id="paper-intent-1",
    )

    next_state = transition_on_entry_fill(
        state=state,
        fill_event=fill,
        trade_size=1,
        signal_bar_id="bar-1",
        long_entry_family=LongEntryFamily.K,
    )

    assert next_state.strategy_status is StrategyStatus.IN_LONG_K
    assert next_state.position_side is PositionSide.LONG
    assert next_state.bars_in_trade == 1
    assert next_state.broker_position_qty == 1
    assert next_state.long_be_armed is False


def test_exit_fill_transitions_state_back_to_ready_flat() -> None:
    now = datetime.now(timezone.utc)
    state = build_initial_state(now)
    entry_fill = FillEvent(
        order_intent_id="intent-1",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        order_status=OrderStatus.FILLED,
        fill_timestamp=now,
        fill_price=Decimal("3000.50"),
        broker_order_id="paper-intent-1",
    )
    in_position = transition_on_entry_fill(
        state=state,
        fill_event=entry_fill,
        trade_size=1,
        signal_bar_id="bar-1",
        long_entry_family=LongEntryFamily.VWAP,
    )
    exit_fill = FillEvent(
        order_intent_id="intent-2",
        intent_type=OrderIntentType.SELL_TO_CLOSE,
        order_status=OrderStatus.FILLED,
        fill_timestamp=now,
        fill_price=Decimal("3001.00"),
        broker_order_id="paper-intent-2",
    )

    flat_state = transition_on_exit_fill(in_position, exit_fill)

    assert flat_state.strategy_status is StrategyStatus.READY
    assert flat_state.position_side is PositionSide.FLAT
    assert flat_state.bars_in_trade == 0
    assert flat_state.broker_position_qty == 0
    assert flat_state.entry_price is None


def test_invariants_detect_family_while_flat() -> None:
    state = build_initial_state(datetime.now(timezone.utc))
    broken_state = state.__class__(**{**state.__dict__, "long_entry_family": LongEntryFamily.K})

    violations = validate_state(broken_state)

    assert "long_entry_family must be NONE while flat" in violations


def test_invariants_detect_negative_cooldown_counter() -> None:
    state = build_initial_state(datetime.now(timezone.utc))
    broken_state = state.__class__(**{**state.__dict__, "bars_since_bull_snap": -1})

    violations = validate_state(broken_state)

    assert "bars_since_bull_snap must be >= 0" in violations


def test_invariants_detect_flat_with_nonzero_qty() -> None:
    state = build_initial_state(datetime.now(timezone.utc))
    broken_state = state.__class__(**{**state.__dict__, "internal_position_qty": 1})

    violations = validate_state(broken_state)

    assert "internal_position_qty must be 0 while flat" in violations
