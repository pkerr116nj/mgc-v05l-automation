"""Bar normalization and validation helpers for shared market-data ingestion."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

from ..domain.models import Bar
from .session_clock import classify_sessions

if TYPE_CHECKING:
    from ..config_models import StrategySettings


class BarBuilder:
    """Validates and classifies bars for historical, live, and replay ingestion paths."""

    def __init__(self, settings: StrategySettings) -> None:
        self._settings = settings

    def normalize(self, bar: Bar) -> Bar:
        """Return a session-classified bar using the shared internal model."""
        return classify_sessions(bar, self._settings)

    def require_finalized(self, bar: Bar) -> Bar:
        """Require a completed bar for the strategy evaluation path."""
        if not bar.is_final:
            raise ValueError("Bar must be finalized before entering the completed-bar strategy path.")
        return bar

    def mark_partial(self, bar: Bar) -> Bar:
        """Return a partial bar without changing its market values."""
        return replace(bar, is_final=False)
