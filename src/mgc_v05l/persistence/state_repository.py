"""Strategy-state snapshot repository."""

from __future__ import annotations

from typing import Optional

from sqlalchemy import desc, select
from sqlalchemy.engine import Engine

from ..domain.models import StrategyState
from .repositories import decode_strategy_state, encode_strategy_state
from .tables import strategy_state_snapshots_table


class StateRepository:
    """Loads and saves explicit strategy state snapshots."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save_snapshot(self, state: StrategyState, transition_label: Optional[str] = None) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                strategy_state_snapshots_table.insert(),
                {
                    "updated_at": state.updated_at.isoformat(),
                    "strategy_status": state.strategy_status.value,
                    "position_side": state.position_side.value,
                    "long_entry_family": state.long_entry_family.value,
                    "transition_label": transition_label,
                    "payload_json": encode_strategy_state(state),
                },
            )

    def load_latest(self) -> Optional[StrategyState]:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(strategy_state_snapshots_table.c.payload_json)
                .order_by(desc(strategy_state_snapshots_table.c.snapshot_id))
                .limit(1)
            ).first()
        return decode_strategy_state(row.payload_json) if row is not None else None
