"""Operator console placeholder."""

from ..exceptions import SpecificationRequiredError


class OperatorConsole:
    """Placeholder operator control layer."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("OperatorConsole requires the control-surface implementation.")
