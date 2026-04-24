"""JSON logging helpers for the validation layer."""

from __future__ import annotations

import json
import logging
from typing import Any


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def log_event(logger: logging.Logger, event: str, **payload: Any) -> None:
    record = {"event": event, **payload}
    logger.info(json.dumps(record, sort_keys=True, default=str))
