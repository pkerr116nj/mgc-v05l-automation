"""Strategy-state snapshot repository."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import desc, select
from sqlalchemy.engine import Engine

from ..domain.models import StrategyState
from .repositories import decode_strategy_state, encode_strategy_state
from .tables import strategy_state_snapshots_table


class StateRepository:
    """Loads and saves explicit strategy state snapshots."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save_snapshot(self, state: StrategyState, transition_label: Optional[str] = None) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                strategy_state_snapshots_table.insert(),
                {
                    "standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
                    "strategy_family": self._runtime_identity.get("strategy_family"),
                    "instrument": self._runtime_identity.get("instrument"),
                    "lane_id": self._runtime_identity.get("lane_id"),
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
            statement = (
                select(strategy_state_snapshots_table.c.payload_json)
                .order_by(desc(strategy_state_snapshots_table.c.snapshot_id))
                .limit(1)
            )
            standalone_strategy_id = str(self._runtime_identity.get("standalone_strategy_id") or "").strip()
            if standalone_strategy_id:
                statement = statement.where(
                    (strategy_state_snapshots_table.c.standalone_strategy_id == standalone_strategy_id)
                    | (strategy_state_snapshots_table.c.standalone_strategy_id.is_(None))
                )
            row = connection.execute(statement).first()
        return decode_strategy_state(row.payload_json) if row is not None else None
