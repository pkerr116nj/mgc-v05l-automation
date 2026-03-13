"""Replay runner for the current v0.5l build."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ..domain.enums import OrderIntentType, PositionSide, StrategyStatus
from ..domain.events import FillReceivedEvent, OrderIntentCreatedEvent, ServiceStartupEvent
from .container import ApplicationContainer


@dataclass(frozen=True)
class ReplayRunSummary:
    processed_bars: int
    order_intents: int
    fills: int
    long_entries: int
    short_entries: int
    exits: int
    final_position_side: PositionSide
    final_strategy_status: StrategyStatus


class StrategyServiceRunner:
    """Replay-first strategy runner for CSV-based research and validation."""

    def __init__(self, container: ApplicationContainer) -> None:
        self._container = container

    def bootstrap(self) -> ServiceStartupEvent:
        return ServiceStartupEvent(source="app.runner")

    def run_replay(self, csv_path: str | Path) -> ReplayRunSummary:
        events = []
        for bar in self._container.replay_feed.iter_csv(csv_path):
            events.extend(self._container.strategy_engine.process_bar(bar))

        long_entries = 0
        short_entries = 0
        exits = 0
        fills = 0
        order_intents = 0
        for event in events:
            if isinstance(event, OrderIntentCreatedEvent):
                order_intents += 1
                if event.intent_type == OrderIntentType.BUY_TO_OPEN:
                    long_entries += 1
                elif event.intent_type == OrderIntentType.SELL_TO_OPEN:
                    short_entries += 1
                else:
                    exits += 1
            elif isinstance(event, FillReceivedEvent):
                fills += 1

        final_state = self._container.strategy_engine.state
        return ReplayRunSummary(
            processed_bars=self._container.repositories.processed_bars.count(),
            order_intents=order_intents,
            fills=fills,
            long_entries=long_entries,
            short_entries=short_entries,
            exits=exits,
            final_position_side=final_state.position_side,
            final_strategy_status=final_state.strategy_status,
        )
