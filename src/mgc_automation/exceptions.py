"""Project-specific exceptions."""


class SpecificationRequiredError(RuntimeError):
    """Raised when a module cannot be implemented safely without the provided spec."""
