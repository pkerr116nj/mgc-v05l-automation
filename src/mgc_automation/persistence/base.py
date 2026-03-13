"""Persistence interfaces."""

from typing import Protocol
from typing import Optional

from ..models.persistence import PersistedStateEnvelope


class StateStore(Protocol):
    """Interface for explicit persisted state."""

    def load_latest(self) -> Optional[PersistedStateEnvelope]:
        """Load the latest persisted state snapshot."""

    def save(self, snapshot: PersistedStateEnvelope) -> None:
        """Persist a state snapshot."""
