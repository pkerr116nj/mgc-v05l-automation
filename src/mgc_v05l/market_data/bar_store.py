"""Processed-bar deduplication store."""

from datetime import datetime
from typing import Optional

from ..domain.exceptions import DeterminismError
from ..domain.models import Bar
from ..persistence.repositories import ProcessedBarRepository

class BarStore:
    """Tracks processed bars to enforce one evaluation per completed bar."""

    def __init__(self, processed_bar_repository: Optional[ProcessedBarRepository] = None) -> None:
        self._processed_bar_repository = processed_bar_repository
        self._processed_bar_ids: set[str] = set()
        self._latest_bar_end_ts: Optional[datetime] = (
            processed_bar_repository.latest_end_ts() if processed_bar_repository is not None else None
        )

    def has_processed(self, bar_id: str) -> bool:
        """Return whether the given bar identifier has already been processed."""
        if bar_id in self._processed_bar_ids:
            return True
        if self._processed_bar_repository is None:
            return False
        return self._processed_bar_repository.has_processed(bar_id)

    def validate_next_bar(self, bar: Bar) -> bool:
        """Return whether the bar should be processed.

        Duplicate bars are ignored. Out-of-order bars raise.
        """
        if self.has_processed(bar.bar_id):
            return False
        if self._latest_bar_end_ts is not None and bar.end_ts < self._latest_bar_end_ts:
            raise DeterminismError(f"Out-of-order bar rejected: {bar.bar_id}")
        return True

    def mark_processed(self, bar: Bar) -> None:
        """Persist the processed bar in the in-memory registry."""
        if not self.validate_next_bar(bar):
            return
        self._processed_bar_ids.add(bar.bar_id)
        self._latest_bar_end_ts = bar.end_ts
        if self._processed_bar_repository is not None:
            self._processed_bar_repository.mark_processed(bar)
