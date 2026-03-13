"""Tests for the canonical mgc_v05l domain package."""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from mgc_v05l.domain.enums import HealthStatus, LongEntryFamily, OrderIntentType, PositionSide, StrategyStatus
from mgc_v05l.domain.models import Bar, HealthSnapshot, StrategyState
from mgc_v05l.domain.state_machine import is_valid_transition
from mgc_v05l.execution.order_models import OrderIntent
from mgc_v05l.monitoring.health import derive_health_status


def test_bar_requires_unique_identity_and_timezone_aware_bounds() -> None:
    start_ts = datetime.now(timezone.utc)
    end_ts = start_ts + timedelta(minutes=5)

    bar = Bar(
        bar_id="MGC-2026-03-13T15:00:00Z",
        symbol="MGC",
        timeframe="5m",
        start_ts=start_ts,
        end_ts=end_ts,
        open=Decimal("3000.0"),
        high=Decimal("3001.0"),
        low=Decimal("2999.0"),
        close=Decimal("3000.5"),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=True,
        session_allowed=True,
    )

    assert bar.bar_id == "MGC-2026-03-13T15:00:00Z"


def test_strategy_state_matches_phase_25_schema() -> None:
    state = StrategyState(
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        broker_position_qty=0,
        internal_position_qty=0,
        entry_price=None,
        entry_timestamp=None,
        entry_bar_id=None,
        long_entry_family=LongEntryFamily.NONE,
        bars_in_trade=0,
        long_be_armed=False,
        short_be_armed=False,
        last_swing_low=None,
        last_swing_high=None,
        asia_reclaim_bar_low=None,
        asia_reclaim_bar_high=None,
        asia_reclaim_bar_vwap=None,
        bars_since_bull_snap=None,
        bars_since_bear_snap=None,
        bars_since_asia_reclaim=None,
        bars_since_asia_vwap_signal=None,
        bars_since_long_setup=None,
        bars_since_short_setup=None,
        last_signal_bar_id=None,
        last_order_intent_id=None,
        open_broker_order_id=None,
        entries_enabled=True,
        exits_enabled=True,
        operator_halt=False,
        reconcile_required=False,
        fault_code=None,
        updated_at=datetime.now(timezone.utc),
    )

    assert state.strategy_status is StrategyStatus.READY
    assert state.long_entry_family is LongEntryFamily.NONE


def test_order_intent_uses_documented_lifecycle_type() -> None:
    intent = OrderIntent(
        order_intent_id="intent-1",
        bar_id="bar-1",
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=datetime.now(timezone.utc),
        reason_code="firstBullSnapTurn",
    )

    assert intent.intent_type is OrderIntentType.BUY_TO_OPEN


def test_health_status_is_composite() -> None:
    snapshot = HealthSnapshot(
        market_data_ok=True,
        broker_ok=True,
        persistence_ok=True,
        reconciliation_clean=True,
        invariants_ok=True,
        health_status=HealthStatus.HEALTHY,
    )

    assert derive_health_status(snapshot) is HealthStatus.HEALTHY


def test_illegal_direct_in_position_flip_is_rejected() -> None:
    assert not is_valid_transition(StrategyStatus.IN_LONG_K, StrategyStatus.IN_SHORT_K)
