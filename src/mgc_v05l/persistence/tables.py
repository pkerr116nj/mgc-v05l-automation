"""SQLite schema for replay-first persistence."""

from sqlalchemy import Boolean, Column, Integer, Table, Text

from .db import metadata

PERSISTENCE_TABLES = (
    "bars",
    "features",
    "signals",
    "strategy_state_snapshots",
    "order_intents",
    "fills",
    "reconciliation_events",
    "fault_events",
    "processed_bars",
)

bars_table = Table(
    "bars",
    metadata,
    Column("bar_id", Text, primary_key=True),
    Column("symbol", Text, nullable=False),
    Column("timeframe", Text, nullable=False),
    Column("start_ts", Text, nullable=False),
    Column("end_ts", Text, nullable=False, index=True),
    Column("open", Text, nullable=False),
    Column("high", Text, nullable=False),
    Column("low", Text, nullable=False),
    Column("close", Text, nullable=False),
    Column("volume", Integer, nullable=False),
    Column("is_final", Boolean, nullable=False),
    Column("session_asia", Boolean, nullable=False),
    Column("session_london", Boolean, nullable=False),
    Column("session_us", Boolean, nullable=False),
    Column("session_allowed", Boolean, nullable=False),
)

features_table = Table(
    "features",
    metadata,
    Column("bar_id", Text, primary_key=True),
    Column("payload_json", Text, nullable=False),
    Column("created_at", Text, nullable=False),
)

signals_table = Table(
    "signals",
    metadata,
    Column("bar_id", Text, primary_key=True),
    Column("payload_json", Text, nullable=False),
    Column("created_at", Text, nullable=False),
)

strategy_state_snapshots_table = Table(
    "strategy_state_snapshots",
    metadata,
    Column("snapshot_id", Integer, primary_key=True, autoincrement=True),
    Column("updated_at", Text, nullable=False, index=True),
    Column("strategy_status", Text, nullable=False),
    Column("position_side", Text, nullable=False),
    Column("long_entry_family", Text, nullable=False),
    Column("transition_label", Text, nullable=True),
    Column("payload_json", Text, nullable=False),
)

order_intents_table = Table(
    "order_intents",
    metadata,
    Column("order_intent_id", Text, primary_key=True),
    Column("bar_id", Text, nullable=False, index=True),
    Column("symbol", Text, nullable=False),
    Column("intent_type", Text, nullable=False),
    Column("quantity", Integer, nullable=False),
    Column("created_at", Text, nullable=False),
    Column("reason_code", Text, nullable=False),
    Column("broker_order_id", Text, nullable=True),
    Column("order_status", Text, nullable=False),
)

fills_table = Table(
    "fills",
    metadata,
    Column("fill_id", Integer, primary_key=True, autoincrement=True),
    Column("order_intent_id", Text, nullable=False, index=True),
    Column("intent_type", Text, nullable=False),
    Column("order_status", Text, nullable=False),
    Column("fill_timestamp", Text, nullable=False, index=True),
    Column("fill_price", Text, nullable=True),
    Column("broker_order_id", Text, nullable=True),
)

reconciliation_events_table = Table(
    "reconciliation_events",
    metadata,
    Column("event_id", Integer, primary_key=True, autoincrement=True),
    Column("created_at", Text, nullable=False),
    Column("payload_json", Text, nullable=False),
)

fault_events_table = Table(
    "fault_events",
    metadata,
    Column("event_id", Integer, primary_key=True, autoincrement=True),
    Column("created_at", Text, nullable=False),
    Column("bar_id", Text, nullable=True),
    Column("fault_code", Text, nullable=False),
    Column("payload_json", Text, nullable=False),
)

processed_bars_table = Table(
    "processed_bars",
    metadata,
    Column("bar_id", Text, primary_key=True),
    Column("end_ts", Text, nullable=False, index=True),
)
