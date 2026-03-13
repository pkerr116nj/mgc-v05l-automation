"""Broker interfaces."""

from collections.abc import Sequence
from typing import Protocol

from ..models.execution import ExecutionIntent


class Broker(Protocol):
    """Interface for execution targets."""

    def submit(self, intents: Sequence[ExecutionIntent]) -> None:
        """Submit execution intents."""
