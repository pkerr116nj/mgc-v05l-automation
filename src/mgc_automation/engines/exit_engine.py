"""Exit engine placeholder."""

from ..exceptions import SpecificationRequiredError


class ExitEngine:
    """Build-stage placeholder for family-specific exit behavior."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("ExitEngine requires the formal specification.")
