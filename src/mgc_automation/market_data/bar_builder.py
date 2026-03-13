"""Bar builder and time normalizer placeholder."""

from ..exceptions import SpecificationRequiredError


class BarBuilder:
    """Placeholder for exact one-bar-close event generation."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("BarBuilder requires exact session and bar boundary rules.")
