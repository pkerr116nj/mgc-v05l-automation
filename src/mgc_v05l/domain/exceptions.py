"""Domain-level exceptions."""


class StrategyError(Exception):
    """Base strategy exception."""


class DeterminismError(StrategyError):
    """Raised when deterministic processing guarantees are violated."""


class InvariantViolationError(StrategyError):
    """Raised when strategy invariants are violated."""


class SpecificationBlockedError(StrategyError):
    """Raised when an ambiguous release-candidate detail blocks safe implementation."""
