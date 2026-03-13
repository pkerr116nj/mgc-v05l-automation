"""Audit logger placeholder."""

from ..exceptions import SpecificationRequiredError


class AuditLogger:
    """Placeholder audit logger."""

    def __init__(self) -> None:
        raise SpecificationRequiredError("AuditLogger requires structured log schema implementation.")
