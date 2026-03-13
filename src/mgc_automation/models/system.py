"""System-level identifiers for persistence derived from the Phase 2 architecture."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PersistenceTableSet:
    bars: str = "bars"
    indicator_values: str = "indicator_values"
    signal_events: str = "signal_events"
    strategy_state: str = "strategy_state"
    order_intents: str = "order_intents"
    broker_orders: str = "broker_orders"
    fills: str = "fills"
    reconciliation_events: str = "reconciliation_events"
    fault_events: str = "fault_events"
