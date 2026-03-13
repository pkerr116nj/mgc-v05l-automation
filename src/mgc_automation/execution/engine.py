"""Execution engine placeholder."""

from ..exceptions import SpecificationRequiredError


class ExecutionEngine:
    """Placeholder execution engine."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("ExecutionEngine requires broker and sequencing implementation details.")
