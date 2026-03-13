"""State-store wrapper for restart-safe strategy state."""

from __future__ import annotations

from typing import Optional

from ..domain.models import StrategyState
from .state_repository import StateRepository


class StateStore:
    """Loads and saves the explicit restart-safe strategy state."""

    def __init__(self, state_repository: StateRepository) -> None:
        self._state_repository = state_repository

    def load(self) -> Optional[StrategyState]:
        return self._state_repository.load_latest()

    def save(self, state: StrategyState, transition_label: Optional[str] = None) -> None:
        self._state_repository.save_snapshot(state, transition_label=transition_label)
