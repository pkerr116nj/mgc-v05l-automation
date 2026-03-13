"""Signal module interfaces."""

from collections.abc import Sequence
from typing import Protocol

from ..models.market import Bar
from ..models.signals import SignalDecision


class SignalModule(Protocol):
    """Interface for pure signal generation on completed bars."""

    name: str

    def evaluate(self, bar: Bar) -> Sequence[SignalDecision]:
        """Return signal decisions for one completed bar."""
