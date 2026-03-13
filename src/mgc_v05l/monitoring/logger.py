"""Structured logger placeholder."""

import logging


def get_logger(name: str) -> logging.Logger:
    """Return a standard logger pending structured logging configuration."""
    return logging.getLogger(name)


class StructuredLogger:
    """Produces bar, order, reconciliation, and fault audit records."""
