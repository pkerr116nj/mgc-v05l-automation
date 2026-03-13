"""Persistence envelope models."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .enums import RunMode


@dataclass(frozen=True)
class PersistedStateEnvelope:
    snapshot_id: str
    created_at: datetime
    run_mode: RunMode
    symbol: str
    last_completed_bar_timestamp: datetime
    state_payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not self.snapshot_id:
            raise ValueError("PersistedStateEnvelope.snapshot_id is required.")
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("PersistedStateEnvelope.created_at must be timezone-aware.")
        if self.last_completed_bar_timestamp.tzinfo is None or self.last_completed_bar_timestamp.utcoffset() is None:
            raise ValueError(
                "PersistedStateEnvelope.last_completed_bar_timestamp must be timezone-aware."
            )
        if not self.symbol:
            raise ValueError("PersistedStateEnvelope.symbol is required.")
