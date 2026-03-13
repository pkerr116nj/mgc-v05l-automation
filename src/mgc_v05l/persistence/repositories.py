"""Persistence repositories for replay-first execution."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import desc, select
from sqlalchemy.engine import Engine

from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, StrategyStatus
from ..domain.models import Bar, FeaturePacket, SignalPacket, StrategyState
from ..execution.order_models import FillEvent, OrderIntent
from .db import create_schema
from .tables import (
    bars_table,
    features_table,
    fills_table,
    order_intents_table,
    processed_bars_table,
    signals_table,
    strategy_state_snapshots_table,
)


def _to_iso(value: datetime) -> str:
    return value.isoformat()


def _from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return {"__type__": "datetime", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__type__": "decimal", "value": str(value)}
    if hasattr(value, "value"):
        return {"__type__": "enum", "value": value.value}
    return value


def _deserialize_value(value: Any) -> Any:
    if not isinstance(value, dict) or "__type__" not in value:
        return value
    value_type = value["__type__"]
    if value_type == "datetime":
        return datetime.fromisoformat(value["value"])
    if value_type == "decimal":
        return Decimal(value["value"])
    if value_type == "enum":
        return value["value"]
    return value


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, default=_serialize_value, sort_keys=True)


def _json_loads(payload_json: str) -> dict[str, Any]:
    raw = json.loads(payload_json)
    return {key: _deserialize_value(value) for key, value in raw.items()}


class BarRepository:
    """Persists finalized bars."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, bar: Bar) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                bars_table.insert().prefix_with("OR REPLACE"),
                {
                    "bar_id": bar.bar_id,
                    "symbol": bar.symbol,
                    "timeframe": bar.timeframe,
                    "start_ts": _to_iso(bar.start_ts),
                    "end_ts": _to_iso(bar.end_ts),
                    "open": str(bar.open),
                    "high": str(bar.high),
                    "low": str(bar.low),
                    "close": str(bar.close),
                    "volume": bar.volume,
                    "is_final": bar.is_final,
                    "session_asia": bar.session_asia,
                    "session_london": bar.session_london,
                    "session_us": bar.session_us,
                    "session_allowed": bar.session_allowed,
                },
            )


class FeatureRepository:
    """Persists computed feature packets."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, features: FeaturePacket, created_at: datetime) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                features_table.insert().prefix_with("OR REPLACE"),
                {
                    "bar_id": features.bar_id,
                    "payload_json": _json_dumps(asdict(features)),
                    "created_at": _to_iso(created_at),
                },
            )


class SignalRepository:
    """Persists signal packets."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, signals: SignalPacket, created_at: datetime) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                signals_table.insert().prefix_with("OR REPLACE"),
                {
                    "bar_id": signals.bar_id,
                    "payload_json": _json_dumps(asdict(signals)),
                    "created_at": _to_iso(created_at),
                },
            )


class ProcessedBarRepository:
    """Durably tracks processed bar IDs."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def has_processed(self, bar_id: str) -> bool:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(processed_bars_table.c.bar_id).where(processed_bars_table.c.bar_id == bar_id)
            ).first()
        return row is not None

    def latest_end_ts(self) -> Optional[datetime]:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(processed_bars_table.c.end_ts).order_by(desc(processed_bars_table.c.end_ts)).limit(1)
            ).first()
        return _from_iso(row.end_ts) if row is not None else None

    def mark_processed(self, bar: Bar) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                processed_bars_table.insert().prefix_with("OR IGNORE"),
                {"bar_id": bar.bar_id, "end_ts": _to_iso(bar.end_ts)},
            )

    def count(self) -> int:
        with self._engine.begin() as connection:
            return len(connection.execute(select(processed_bars_table.c.bar_id)).all())


class OrderIntentRepository:
    """Persists order intents and broker submission details."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(
        self,
        intent: OrderIntent,
        order_status: OrderStatus,
        broker_order_id: Optional[str] = None,
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                order_intents_table.insert().prefix_with("OR REPLACE"),
                {
                    "order_intent_id": intent.order_intent_id,
                    "bar_id": intent.bar_id,
                    "symbol": intent.symbol,
                    "intent_type": intent.intent_type.value,
                    "quantity": intent.quantity,
                    "created_at": _to_iso(intent.created_at),
                    "reason_code": intent.reason_code,
                    "broker_order_id": broker_order_id,
                    "order_status": order_status.value,
                },
            )

    def list_all(self) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            rows = connection.execute(select(order_intents_table)).mappings().all()
        return [dict(row) for row in rows]


class FillRepository:
    """Persists confirmed fill events."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, fill: FillEvent) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                fills_table.insert(),
                {
                    "order_intent_id": fill.order_intent_id,
                    "intent_type": fill.intent_type.value,
                    "order_status": fill.order_status.value,
                    "fill_timestamp": _to_iso(fill.fill_timestamp),
                    "fill_price": str(fill.fill_price) if fill.fill_price is not None else None,
                    "broker_order_id": fill.broker_order_id,
                },
            )

    def list_all(self) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            rows = connection.execute(select(fills_table)).mappings().all()
        return [dict(row) for row in rows]


class RepositorySet:
    """Coordinates repositories for bars, features, signals, state, orders, fills, and processed bars."""

    def __init__(self, engine: Engine) -> None:
        create_schema(engine)
        self.engine = engine
        self.bars = BarRepository(engine)
        self.features = FeatureRepository(engine)
        self.signals = SignalRepository(engine)
        self.processed_bars = ProcessedBarRepository(engine)
        self.order_intents = OrderIntentRepository(engine)
        self.fills = FillRepository(engine)


def decode_strategy_state(payload_json: str) -> StrategyState:
    """Decode a persisted state snapshot payload."""
    payload = _json_loads(payload_json)
    payload["strategy_status"] = StrategyStatus(payload["strategy_status"])
    payload["position_side"] = PositionSide(payload["position_side"])
    payload["long_entry_family"] = LongEntryFamily(payload["long_entry_family"])
    return StrategyState(**payload)


def encode_strategy_state(state: StrategyState) -> str:
    """Encode a strategy state snapshot."""
    return _json_dumps(asdict(state))


def decode_order_intent(row: dict[str, Any]) -> OrderIntent:
    """Decode a persisted order-intent row."""
    return OrderIntent(
        order_intent_id=row["order_intent_id"],
        bar_id=row["bar_id"],
        symbol=row["symbol"],
        intent_type=OrderIntentType(row["intent_type"]),
        quantity=row["quantity"],
        created_at=_from_iso(row["created_at"]),
        reason_code=row["reason_code"],
    )


def decode_fill(row: dict[str, Any]) -> FillEvent:
    """Decode a persisted fill row."""
    return FillEvent(
        order_intent_id=row["order_intent_id"],
        intent_type=OrderIntentType(row["intent_type"]),
        order_status=OrderStatus(row["order_status"]),
        fill_timestamp=_from_iso(row["fill_timestamp"]),
        fill_price=Decimal(row["fill_price"]) if row["fill_price"] is not None else None,
        broker_order_id=row["broker_order_id"],
    )
