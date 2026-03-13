"""State engine placeholder."""

from ..exceptions import SpecificationRequiredError


class StateEngine:
    """Build-stage placeholder for explicit persisted state transitions."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("StateEngine requires the formal specification.")
