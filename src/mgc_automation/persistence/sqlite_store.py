"""SQLite persistence placeholder."""

from ..exceptions import SpecificationRequiredError


class SQLiteStateStore:
    """Placeholder SQLite state store."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("SQLiteStateStore requires the formal specification.")
