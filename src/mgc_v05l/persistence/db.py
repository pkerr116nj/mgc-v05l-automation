"""Database helpers."""

from sqlalchemy import MetaData, create_engine, event
from sqlalchemy.engine import Engine

metadata = MetaData()


def build_engine(database_url: str) -> Engine:
    """Create the SQLAlchemy engine for the configured SQLite database."""
    connect_args = {}
    if database_url.startswith("sqlite"):
        connect_args["timeout"] = 30
    engine = create_engine(database_url, future=True, connect_args=connect_args)
    if engine.dialect.name == "sqlite":
        _configure_sqlite_engine(engine)
    return engine


def _configure_sqlite_engine(engine: Engine) -> None:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # type: ignore[no-untyped-def]
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
        finally:
            cursor.close()


def create_schema(engine: Engine) -> None:
    """Create the configured persistence schema."""
    metadata.create_all(engine)
    _ensure_runtime_identity_columns(engine)


def _ensure_runtime_identity_columns(engine: Engine) -> None:
    table_columns = {
        "strategy_state_snapshots": (
            ("standalone_strategy_id", "TEXT"),
            ("strategy_family", "TEXT"),
            ("instrument", "TEXT"),
            ("lane_id", "TEXT"),
        ),
        "order_intents": (
            ("standalone_strategy_id", "TEXT"),
            ("strategy_family", "TEXT"),
            ("instrument", "TEXT"),
            ("lane_id", "TEXT"),
            ("submitted_at", "TEXT"),
            ("acknowledged_at", "TEXT"),
            ("broker_order_status", "TEXT"),
            ("last_status_checked_at", "TEXT"),
            ("timeout_classification", "TEXT"),
            ("timeout_status_updated_at", "TEXT"),
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
        ),
        "fills": (
            ("standalone_strategy_id", "TEXT"),
            ("strategy_family", "TEXT"),
            ("instrument", "TEXT"),
            ("lane_id", "TEXT"),
            ("quantity", "INTEGER"),
        ),
        "processed_bars": (
            ("standalone_strategy_id", "TEXT"),
            ("instrument", "TEXT"),
            ("lane_id", "TEXT"),
        ),
    }
    with engine.begin() as connection:
        for table_name, columns in table_columns.items():
            existing = {
                str(row[1])
                for row in connection.exec_driver_sql(f"PRAGMA table_info({table_name})").fetchall()
            }
            for column_name, column_type in columns:
                if column_name in existing:
                    continue
                connection.exec_driver_sql(
                    f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                )
