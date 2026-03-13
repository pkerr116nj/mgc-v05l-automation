"""Paper broker placeholder."""

from collections.abc import Sequence

from ..models.execution import ExecutionIntent


class PaperBroker:
    """Placeholder paper broker.

    Execution behavior remains deferred until the formal specification is loaded.
    """

    def submit(self, intents: Sequence[ExecutionIntent]) -> None:
        del intents
        raise NotImplementedError("PaperBroker behavior requires the formal specification.")
