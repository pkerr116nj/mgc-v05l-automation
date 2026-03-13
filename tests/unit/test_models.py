"""Smoke tests for typed models."""

from datetime import datetime, timezone
from decimal import Decimal

from mgc_automation.models import (
    BrokerConnectionStatus,
    DataHealthStatus,
    DeploymentEnvironment,
    ExecutionIntent,
    LongTradeFamily,
    OperatingState,
    OrderIntentType,
    RuntimeStatus,
    StrategySide,
    StrategyState,
)


def test_execution_intent_requires_spec_defined_intent_and_symbol() -> None:
    intent = ExecutionIntent(
        created_at=datetime.now(timezone.utc),
        bar_timestamp=datetime.now(timezone.utc),
        intent=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        symbol="MGC",
        source_signal="firstBullSnapTurn",
        reason="spec smoke test",
    )

    assert intent.intent is OrderIntentType.BUY_TO_OPEN
    assert intent.symbol == "MGC"


def test_strategy_state_tracks_trade_family_and_side() -> None:
    state = StrategyState(
        in_position=True,
        strategy_side=StrategySide.LONG,
        long_trade_family=LongTradeFamily.K_LONG,
        position_quantity=1,
        entry_price=Decimal("3000.10"),
        bars_in_trade=1,
        long_break_even_armed=False,
        short_break_even_armed=False,
        active_long_stop_reference=None,
        active_short_stop_reference=None,
        asia_reclaim_bar_low=None,
        asia_reclaim_bar_high=None,
        asia_reclaim_bar_vwap=None,
        bull_snap_cooldown=0,
        bear_snap_cooldown=0,
        last_long_signal_timestamp=None,
        last_short_signal_timestamp=None,
        last_execution_timestamp=None,
    )

    assert state.strategy_side is StrategySide.LONG
    assert state.long_trade_family is LongTradeFamily.K_LONG


def test_runtime_status_supports_phase_2_operating_states() -> None:
    runtime = RuntimeStatus(
        environment=DeploymentEnvironment.PAPER,
        operating_state=OperatingState.READY,
        broker_connection_status=BrokerConnectionStatus.CONNECTED,
        data_health_status=DataHealthStatus.HEALTHY,
        strategy_enabled=True,
        new_entries_allowed=True,
        warmup_complete=True,
        state_loaded=True,
    )

    assert runtime.environment is DeploymentEnvironment.PAPER
    assert runtime.operating_state is OperatingState.READY
