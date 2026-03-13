"""Database helpers."""

from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine import Engine

metadata = MetaData()


def build_engine(database_url: str) -> Engine:
    """Create the SQLAlchemy engine for the configured SQLite database."""
    return create_engine(database_url, future=True)


def create_schema(engine: Engine) -> None:
    """Create the configured persistence schema."""
    metadata.create_all(engine)
