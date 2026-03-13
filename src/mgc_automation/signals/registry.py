"""Signal registration placeholder."""

from collections.abc import Sequence
from typing import Optional

from .base import SignalModule


class SignalRegistry:
    """Placeholder registry for signal modules."""

    def __init__(self, modules: Optional[Sequence[SignalModule]] = None) -> None:
        self.modules = tuple(modules or ())
