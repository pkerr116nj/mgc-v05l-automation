"""Feature engine placeholder."""

from ..exceptions import SpecificationRequiredError


class FeatureEngine:
    """Build-stage placeholder for derived feature computation."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("FeatureEngine requires the formal specification.")
