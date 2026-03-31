"""Persistence repositories for replay-first execution plus additive research storage."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional, Sequence

from sqlalchemy import desc, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

from ..domain.enums import LongEntryFamily, OrderIntentType, OrderStatus, PositionSide, ShortEntryFamily, StrategyStatus
from ..domain.models import Bar, FeaturePacket, SignalPacket, StrategyState
from ..execution.order_models import FillEvent, OrderIntent
from .db import create_schema
from .research_models import (
    DerivedFeatureRecord,
    ExperimentRunRecord,
    InstrumentRecord,
    SignalEvaluationRecord,
    TradeOutcomeRecord,
)
from .tables import (
    alert_events_table,
    bars_table,
    derived_features_table,
    experiment_runs_table,
    features_table,
    fault_events_table,
    fills_table,
    instruments_table,
    execution_watchdog_events_table,
    order_intents_table,
    processed_bars_table,
    reconciliation_events_table,
    signal_evaluations_table,
    signals_table,
    strategy_state_snapshots_table,
    trade_outcomes_table,
)


def _to_iso(value: datetime) -> str:
    return value.isoformat()


def _from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _to_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _to_numeric(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _to_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    return Decimal(str(value))


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


class InstrumentRepository:
    """Upserts instruments for raw-market and experiment linkage."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def upsert(self, instrument: InstrumentRecord) -> InstrumentRecord:
        values = {
            "ticker": instrument.ticker,
            "cusip": instrument.cusip,
            "asset_class": instrument.asset_class,
            "description": instrument.description,
            "exchange": instrument.exchange,
            "multiplier": _to_numeric(instrument.multiplier),
            "is_active": instrument.is_active,
        }
        statement = sqlite_insert(instruments_table).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=["ticker", "asset_class"],
            set_={
                "cusip": statement.excluded.cusip,
                "description": statement.excluded.description,
                "exchange": statement.excluded.exchange,
                "multiplier": statement.excluded.multiplier,
                "is_active": statement.excluded.is_active,
            },
        )
        with self._engine.begin() as connection:
            connection.execute(statement)
            row = connection.execute(
                select(instruments_table).where(
                    instruments_table.c.ticker == instrument.ticker,
                    instruments_table.c.asset_class == instrument.asset_class,
                )
            ).mappings().one()
        return decode_instrument(dict(row))

    def get_by_ticker(self, ticker: str, asset_class: str) -> Optional[InstrumentRecord]:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(instruments_table).where(
                    instruments_table.c.ticker == ticker,
                    instruments_table.c.asset_class == asset_class,
                )
            ).mappings().first()
        return decode_instrument(dict(row)) if row is not None else None


class BarRepository:
    """Persists finalized bars and additive research metadata."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(
        self,
        bar: Bar,
        instrument_id: Optional[int] = None,
        cusip: Optional[str] = None,
        asset_class: Optional[str] = None,
        data_source: str = "internal",
        created_at: Optional[datetime] = None,
    ) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                bars_table.insert().prefix_with("OR REPLACE"),
                {
                    "bar_id": bar.bar_id,
                    "instrument_id": instrument_id,
                    "ticker": bar.symbol,
                    "cusip": cusip,
                    "asset_class": asset_class,
                    "data_source": data_source,
                    "timestamp": _to_iso(bar.end_ts),
                    "symbol": bar.symbol,
                    "timeframe": bar.timeframe,
                    "start_ts": _to_iso(bar.start_ts),
                    "end_ts": _to_iso(bar.end_ts),
                    "open": _to_numeric(bar.open),
                    "high": _to_numeric(bar.high),
                    "low": _to_numeric(bar.low),
                    "close": _to_numeric(bar.close),
                    "volume": bar.volume,
                    "is_final": bar.is_final,
                    "session_asia": bar.session_asia,
                    "session_london": bar.session_london,
                    "session_us": bar.session_us,
                    "session_allowed": bar.session_allowed,
                    "created_at": _to_iso(created_at or datetime.now(bar.end_ts.tzinfo)),
                },
            )

    def count(self) -> int:
        with self._engine.begin() as connection:
            return len(connection.execute(select(bars_table.c.bar_id)).all())

    def get_row(self, bar_id: str) -> Optional[dict[str, Any]]:
        with self._engine.begin() as connection:
            row = connection.execute(select(bars_table).where(bars_table.c.bar_id == bar_id)).mappings().first()
        return dict(row) if row is not None else None

    def list_recent_processed(self, *, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        with self._engine.begin() as connection:
            rows = connection.execute(
                select(bars_table)
                .join(processed_bars_table, processed_bars_table.c.bar_id == bars_table.c.bar_id)
                .where(bars_table.c.symbol == symbol)
                .where(bars_table.c.timeframe == timeframe)
                .order_by(desc(bars_table.c.end_ts))
                .limit(limit)
            ).mappings().all()
        return [decode_bar(dict(row)) for row in reversed(rows)]

    def list_recent(self, *, symbol: str, timeframe: str, limit: int) -> list[Bar]:
        with self._engine.begin() as connection:
            rows = connection.execute(
                select(bars_table)
                .where(bars_table.c.symbol == symbol)
                .where(bars_table.c.timeframe == timeframe)
                .order_by(desc(bars_table.c.end_ts))
                .limit(limit)
            ).mappings().all()
        return [decode_bar(dict(row)) for row in reversed(rows)]


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

    def load_by_bar_ids(self, bar_ids: Sequence[str]) -> list[FeaturePacket]:
        if not bar_ids:
            return []
        with self._engine.begin() as connection:
            rows = connection.execute(
                select(features_table).where(features_table.c.bar_id.in_(list(bar_ids)))
            ).mappings().all()
        payload_by_bar_id = {
            str(row["bar_id"]): decode_feature_packet(str(row["payload_json"]))
            for row in rows
        }
        return [payload_by_bar_id[bar_id] for bar_id in bar_ids if bar_id in payload_by_bar_id]


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


class DerivedFeatureRepository:
    """Stores continuous research features for a bar and optional experiment run."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, record: DerivedFeatureRecord) -> DerivedFeatureRecord:
        values = {
            "bar_id": record.bar_id,
            "experiment_run_id": record.experiment_run_id,
            "atr": _to_numeric(record.atr),
            "vwap": _to_numeric(record.vwap),
            "ema_fast": _to_numeric(record.ema_fast),
            "ema_slow": _to_numeric(record.ema_slow),
            "velocity": _to_numeric(record.velocity),
            "velocity_delta": _to_numeric(record.velocity_delta),
            "stretch_down": _to_numeric(record.stretch_down),
            "stretch_up": _to_numeric(record.stretch_up),
            "smoothed_close": _to_numeric(record.smoothed_close),
            "momentum_raw": _to_numeric(record.momentum_raw),
            "momentum_norm": _to_numeric(record.momentum_norm),
            "momentum_delta": _to_numeric(record.momentum_delta),
            "momentum_acceleration": _to_numeric(record.momentum_acceleration),
            "volume_ratio": _to_numeric(record.volume_ratio),
            "signed_impulse": _to_numeric(record.signed_impulse),
            "smoothed_signed_impulse": _to_numeric(record.smoothed_signed_impulse),
            "impulse_delta": _to_numeric(record.impulse_delta),
            "created_at": _to_iso(record.created_at),
        }
        statement = sqlite_insert(derived_features_table).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=["bar_id", "experiment_run_id"],
            set_={key: statement.excluded[key] for key in values if key not in {"bar_id", "experiment_run_id"}},
        )
        with self._engine.begin() as connection:
            result = connection.execute(statement)
            if result.lastrowid is not None:
                row = connection.execute(
                    select(derived_features_table).where(derived_features_table.c.feature_id == result.lastrowid)
                ).mappings().first()
            else:
                row = connection.execute(
                    select(derived_features_table).where(
                        derived_features_table.c.bar_id == record.bar_id,
                        derived_features_table.c.experiment_run_id == record.experiment_run_id,
                    )
                ).mappings().one()
        return decode_derived_feature(dict(row))

    def get_by_bar_id(self, bar_id: str, experiment_run_id: Optional[int]) -> Optional[DerivedFeatureRecord]:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(derived_features_table).where(
                    derived_features_table.c.bar_id == bar_id,
                    derived_features_table.c.experiment_run_id == experiment_run_id,
                )
            ).mappings().first()
        return decode_derived_feature(dict(row)) if row is not None else None


class SignalEvaluationRepository:
    """Stores per-bar signal-evaluation rows for experiments."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, record: SignalEvaluationRecord) -> SignalEvaluationRecord:
        values = {
            "bar_id": record.bar_id,
            "experiment_run_id": record.experiment_run_id,
            "bull_snap_raw": record.bull_snap_raw,
            "bear_snap_raw": record.bear_snap_raw,
            "asia_vwap_reclaim_raw": record.asia_vwap_reclaim_raw,
            "momentum_compressing_up": record.momentum_compressing_up,
            "momentum_turning_positive": record.momentum_turning_positive,
            "momentum_compressing_down": record.momentum_compressing_down,
            "momentum_turning_negative": record.momentum_turning_negative,
            "filter_pass_long": record.filter_pass_long,
            "filter_pass_short": record.filter_pass_short,
            "trigger_long_math": record.trigger_long_math,
            "trigger_short_math": record.trigger_short_math,
            "warmup_complete": record.warmup_complete,
            "compression_long": record.compression_long,
            "reclaim_long": record.reclaim_long,
            "separation_long": record.separation_long,
            "structure_long_candidate": record.structure_long_candidate,
            "compression_short": record.compression_short,
            "failure_short": record.failure_short,
            "separation_short": record.separation_short,
            "structure_short_candidate": record.structure_short_candidate,
            "quality_score_long": _to_numeric(record.quality_score_long),
            "quality_score_short": _to_numeric(record.quality_score_short),
            "size_recommendation_long": _to_numeric(record.size_recommendation_long),
            "size_recommendation_short": _to_numeric(record.size_recommendation_short),
            "created_at": _to_iso(record.created_at),
        }
        statement = sqlite_insert(signal_evaluations_table).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=["bar_id", "experiment_run_id"],
            set_={key: statement.excluded[key] for key in values if key not in {"bar_id", "experiment_run_id"}},
        )
        with self._engine.begin() as connection:
            result = connection.execute(statement)
            if result.lastrowid is not None:
                row = connection.execute(
                    select(signal_evaluations_table).where(
                        signal_evaluations_table.c.signal_eval_id == result.lastrowid
                    )
                ).mappings().first()
            else:
                row = connection.execute(
                    select(signal_evaluations_table).where(
                        signal_evaluations_table.c.bar_id == record.bar_id,
                        signal_evaluations_table.c.experiment_run_id == record.experiment_run_id,
                    )
                ).mappings().one()
        return decode_signal_evaluation(dict(row))

    def get_by_bar_id(self, bar_id: str, experiment_run_id: int) -> Optional[SignalEvaluationRecord]:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(signal_evaluations_table).where(
                    signal_evaluations_table.c.bar_id == bar_id,
                    signal_evaluations_table.c.experiment_run_id == experiment_run_id,
                )
            ).mappings().first()
        return decode_signal_evaluation(dict(row)) if row is not None else None


class TradeOutcomeRepository:
    """Stores experiment-linked trade outcomes."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def save(self, record: TradeOutcomeRecord) -> TradeOutcomeRecord:
        values = {
            "experiment_run_id": record.experiment_run_id,
            "entry_bar_id": record.entry_bar_id,
            "exit_bar_id": record.exit_bar_id,
            "ticker": record.ticker,
            "timeframe": record.timeframe,
            "side": record.side,
            "entry_family": record.entry_family,
            "entry_reason": record.entry_reason,
            "entry_price": _to_numeric(record.entry_price),
            "exit_price": _to_numeric(record.exit_price),
            "size": _to_numeric(record.size),
            "bars_held": record.bars_held,
            "pnl": _to_numeric(record.pnl),
            "mae": _to_numeric(record.mae),
            "mfe": _to_numeric(record.mfe),
            "exit_reason": record.exit_reason,
            "quality_score_at_entry": _to_numeric(record.quality_score_at_entry),
            "size_recommendation_at_entry": _to_numeric(record.size_recommendation_at_entry),
            "created_at": _to_iso(record.created_at),
        }
        with self._engine.begin() as connection:
            result = connection.execute(trade_outcomes_table.insert(), values)
            row = connection.execute(
                select(trade_outcomes_table).where(trade_outcomes_table.c.trade_id == result.lastrowid)
            ).mappings().one()
        return decode_trade_outcome(dict(row))

    def list_by_experiment_run(self, experiment_run_id: int) -> list[TradeOutcomeRecord]:
        with self._engine.begin() as connection:
            rows = connection.execute(
                select(trade_outcomes_table).where(trade_outcomes_table.c.experiment_run_id == experiment_run_id)
            ).mappings().all()
        return [decode_trade_outcome(dict(row)) for row in rows]


class ExperimentRunRepository:
    """Creates and queries experiment-run metadata."""

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def create(self, record: ExperimentRunRecord) -> ExperimentRunRecord:
        values = {
            "name": record.name,
            "description": record.description,
            "market_universe": record.market_universe,
            "timeframe": record.timeframe,
            "feature_version": record.feature_version,
            "signal_version": record.signal_version,
            "sizing_version": record.sizing_version,
            "config_json": record.config_json,
            "started_at": _to_iso(record.started_at),
            "completed_at": _to_iso(record.completed_at) if record.completed_at is not None else None,
        }
        with self._engine.begin() as connection:
            result = connection.execute(experiment_runs_table.insert(), values)
            row = connection.execute(
                select(experiment_runs_table).where(
                    experiment_runs_table.c.experiment_run_id == result.lastrowid
                )
            ).mappings().one()
        return decode_experiment_run(dict(row))

    def get(self, experiment_run_id: int) -> Optional[ExperimentRunRecord]:
        with self._engine.begin() as connection:
            row = connection.execute(
                select(experiment_runs_table).where(
                    experiment_runs_table.c.experiment_run_id == experiment_run_id
                )
            ).mappings().first()
        return decode_experiment_run(dict(row)) if row is not None else None


class ProcessedBarRepository:
    """Durably tracks processed bar IDs."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def has_processed(self, bar_id: str) -> bool:
        with self._engine.begin() as connection:
            statement = select(processed_bars_table.c.bar_id).where(processed_bars_table.c.bar_id == bar_id)
            statement = self._apply_runtime_identity_filters(statement)
            row = connection.execute(statement).first()
        return row is not None

    def latest_end_ts(self) -> Optional[datetime]:
        with self._engine.begin() as connection:
            statement = select(processed_bars_table.c.end_ts).order_by(desc(processed_bars_table.c.end_ts)).limit(1)
            statement = self._apply_runtime_identity_filters(statement)
            row = connection.execute(statement).first()
        return _from_iso(row.end_ts) if row is not None else None

    def mark_processed(self, bar: Bar) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                processed_bars_table.insert().prefix_with("OR IGNORE"),
                {
                    "bar_id": bar.bar_id,
                    "standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
                    "instrument": self._runtime_identity.get("instrument") or bar.symbol,
                    "lane_id": self._runtime_identity.get("lane_id"),
                    "end_ts": _to_iso(bar.end_ts),
                },
            )

    def count(self) -> int:
        with self._engine.begin() as connection:
            statement = select(processed_bars_table.c.bar_id)
            statement = self._apply_runtime_identity_filters(statement)
            return len(connection.execute(statement).all())

    def _apply_runtime_identity_filters(self, statement):
        standalone_strategy_id = str(self._runtime_identity.get("standalone_strategy_id") or "").strip()
        if standalone_strategy_id:
            return statement.where(
                (processed_bars_table.c.standalone_strategy_id == standalone_strategy_id)
                | (processed_bars_table.c.standalone_strategy_id.is_(None))
            )
        return statement


class OrderIntentRepository:
    """Persists order intents and broker submission details."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save(
        self,
        intent: OrderIntent,
        order_status: OrderStatus,
        broker_order_id: Optional[str] = None,
        *,
        submitted_at: datetime | None = None,
        acknowledged_at: datetime | None = None,
        broker_order_status: str | None = None,
        last_status_checked_at: datetime | None = None,
        timeout_classification: str | None = None,
        timeout_status_updated_at: datetime | None = None,
        retry_count: int | None = None,
    ) -> None:
        with self._engine.begin() as connection:
            existing = connection.execute(
                select(order_intents_table).where(order_intents_table.c.order_intent_id == intent.order_intent_id)
            ).mappings().first()
            connection.execute(
                order_intents_table.insert().prefix_with("OR REPLACE"),
                {
                    "order_intent_id": intent.order_intent_id,
                    "standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
                    "strategy_family": self._runtime_identity.get("strategy_family"),
                    "instrument": self._runtime_identity.get("instrument") or intent.symbol,
                    "lane_id": self._runtime_identity.get("lane_id"),
                    "bar_id": intent.bar_id,
                    "symbol": intent.symbol,
                    "intent_type": intent.intent_type.value,
                    "quantity": intent.quantity,
                    "created_at": _to_iso(intent.created_at),
                    "submitted_at": _to_text(
                        _to_iso(submitted_at)
                        if submitted_at is not None
                        else existing.get("submitted_at") if existing is not None else None
                    ),
                    "acknowledged_at": _to_text(
                        _to_iso(acknowledged_at)
                        if acknowledged_at is not None
                        else existing.get("acknowledged_at") if existing is not None else None
                    ),
                    "reason_code": intent.reason_code,
                    "broker_order_id": broker_order_id if broker_order_id is not None else (existing.get("broker_order_id") if existing is not None else None),
                    "order_status": order_status.value,
                    "broker_order_status": _to_text(
                        broker_order_status if broker_order_status is not None else (existing.get("broker_order_status") if existing is not None else None)
                    ),
                    "last_status_checked_at": _to_text(
                        _to_iso(last_status_checked_at)
                        if last_status_checked_at is not None
                        else existing.get("last_status_checked_at") if existing is not None else None
                    ),
                    "timeout_classification": _to_text(
                        timeout_classification if timeout_classification is not None else (existing.get("timeout_classification") if existing is not None else None)
                    ),
                    "timeout_status_updated_at": _to_text(
                        _to_iso(timeout_status_updated_at)
                        if timeout_status_updated_at is not None
                        else existing.get("timeout_status_updated_at") if existing is not None else None
                    ),
                    "retry_count": int(retry_count if retry_count is not None else (existing.get("retry_count") if existing is not None else 0)),
                },
            )

    def list_all(self) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            statement = select(order_intents_table)
            statement = self._apply_runtime_identity_filters(statement)
            rows = connection.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def _apply_runtime_identity_filters(self, statement):
        standalone_strategy_id = str(self._runtime_identity.get("standalone_strategy_id") or "").strip()
        if standalone_strategy_id:
            return statement.where(
                (order_intents_table.c.standalone_strategy_id == standalone_strategy_id)
                | (order_intents_table.c.standalone_strategy_id.is_(None))
            )
        return statement


class FillRepository:
    """Persists confirmed fill events."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save(self, fill: FillEvent) -> None:
        with self._engine.begin() as connection:
            connection.execute(
                fills_table.insert(),
                {
                    "standalone_strategy_id": self._runtime_identity.get("standalone_strategy_id"),
                    "strategy_family": self._runtime_identity.get("strategy_family"),
                    "instrument": self._runtime_identity.get("instrument"),
                    "lane_id": self._runtime_identity.get("lane_id"),
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
            statement = select(fills_table)
            statement = self._apply_runtime_identity_filters(statement)
            rows = connection.execute(statement).mappings().all()
        return [dict(row) for row in rows]

    def _apply_runtime_identity_filters(self, statement):
        standalone_strategy_id = str(self._runtime_identity.get("standalone_strategy_id") or "").strip()
        if standalone_strategy_id:
            return statement.where(
                (fills_table.c.standalone_strategy_id == standalone_strategy_id)
                | (fills_table.c.standalone_strategy_id.is_(None))
            )
        return statement


class AlertEventRepository:
    """Persists operator-visible alert audit events."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save(self, payload: dict[str, Any], *, occurred_at: datetime) -> None:
        record = {**self._runtime_identity, **payload}
        with self._engine.begin() as connection:
            connection.execute(
                alert_events_table.insert(),
                {
                    "occurred_at": _to_iso(occurred_at),
                    "category": str(record.get("category") or "runtime"),
                    "severity": str(record.get("severity") or "INFO"),
                    "title": str(record.get("title") or "Alert"),
                    "message": str(record.get("message") or record.get("title") or "Alert"),
                    "source_subsystem": str(record.get("source_subsystem") or "runtime"),
                    "dedup_key": _to_text(record.get("dedup_key")),
                    "active": bool(record.get("active", False)),
                    "acknowledged": bool(record.get("acknowledged", False)),
                    "payload_json": _json_dumps(record),
                },
            )

    def list_recent(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            rows = connection.execute(
                select(alert_events_table).order_by(desc(alert_events_table.c.occurred_at)).limit(limit)
            ).mappings().all()
        return [_json_loads(str(row["payload_json"])) for row in rows]


class ReconciliationEventRepository:
    """Persists explicit reconciliation audit events."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save(self, payload: dict[str, Any], *, created_at: datetime) -> None:
        record = {**self._runtime_identity, **payload}
        with self._engine.begin() as connection:
            connection.execute(
                reconciliation_events_table.insert(),
                {
                    "created_at": _to_iso(created_at),
                    "payload_json": _json_dumps(record),
                },
            )

    def list_all(self) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            rows = connection.execute(select(reconciliation_events_table)).mappings().all()
        return [_json_loads(str(row["payload_json"])) for row in rows]


class ExecutionWatchdogEventRepository:
    """Persists explicit pending-order watchdog audit events."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save(self, payload: dict[str, Any], *, created_at: datetime) -> None:
        record = {**self._runtime_identity, **payload}
        with self._engine.begin() as connection:
            connection.execute(
                execution_watchdog_events_table.insert(),
                {
                    "created_at": _to_iso(created_at),
                    "payload_json": _json_dumps(record),
                },
            )

    def list_all(self) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            rows = connection.execute(select(execution_watchdog_events_table)).mappings().all()
        return [_json_loads(str(row["payload_json"])) for row in rows]


class FaultEventRepository:
    """Persists explicit fault audit events."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        self._engine = engine
        self._runtime_identity = dict(runtime_identity or {})

    def save(
        self,
        *,
        fault_code: str,
        payload: dict[str, Any],
        created_at: datetime,
        bar_id: str | None,
    ) -> None:
        record = {**self._runtime_identity, **payload}
        with self._engine.begin() as connection:
            connection.execute(
                fault_events_table.insert(),
                {
                    "created_at": _to_iso(created_at),
                    "bar_id": bar_id,
                    "fault_code": fault_code,
                    "payload_json": _json_dumps(record),
                },
            )

    def list_all(self) -> list[dict[str, Any]]:
        with self._engine.begin() as connection:
            rows = connection.execute(select(fault_events_table)).mappings().all()
        return [_json_loads(str(row["payload_json"])) for row in rows]


class RepositorySet:
    """Coordinates runtime and additive research repositories."""

    def __init__(self, engine: Engine, *, runtime_identity: dict[str, Any] | None = None) -> None:
        create_schema(engine)
        self.engine = engine
        self.runtime_identity = dict(runtime_identity or {})
        self.instruments = InstrumentRepository(engine)
        self.bars = BarRepository(engine)
        self.features = FeatureRepository(engine)
        self.signals = SignalRepository(engine)
        self.derived_features = DerivedFeatureRepository(engine)
        self.signal_evaluations = SignalEvaluationRepository(engine)
        self.trade_outcomes = TradeOutcomeRepository(engine)
        self.experiment_runs = ExperimentRunRepository(engine)
        self.processed_bars = ProcessedBarRepository(engine, runtime_identity=self.runtime_identity)
        self.order_intents = OrderIntentRepository(engine, runtime_identity=self.runtime_identity)
        self.fills = FillRepository(engine, runtime_identity=self.runtime_identity)
        self.alerts = AlertEventRepository(engine, runtime_identity=self.runtime_identity)
        self.reconciliation_events = ReconciliationEventRepository(engine, runtime_identity=self.runtime_identity)
        self.execution_watchdog_events = ExecutionWatchdogEventRepository(engine, runtime_identity=self.runtime_identity)
        self.fault_events = FaultEventRepository(engine, runtime_identity=self.runtime_identity)


def decode_strategy_state(payload_json: str) -> StrategyState:
    """Decode a persisted state snapshot payload."""
    payload = _json_loads(payload_json)
    payload["strategy_status"] = StrategyStatus(payload["strategy_status"])
    payload["position_side"] = PositionSide(payload["position_side"])
    payload["long_entry_family"] = LongEntryFamily(payload["long_entry_family"])
    payload.setdefault("same_underlying_entry_hold", False)
    payload.setdefault("same_underlying_hold_reason", None)
    if "short_entry_family" in payload:
        payload["short_entry_family"] = ShortEntryFamily(payload["short_entry_family"])
    return StrategyState(**payload)


def decode_bar(row: dict[str, Any]) -> Bar:
    """Decode a persisted bar row."""
    return Bar(
        bar_id=str(row["bar_id"]),
        symbol=str(row["symbol"]),
        timeframe=str(row["timeframe"]),
        start_ts=_from_iso(str(row["start_ts"])),
        end_ts=_from_iso(str(row["end_ts"])),
        open=_to_decimal(row["open"]) or Decimal("0"),
        high=_to_decimal(row["high"]) or Decimal("0"),
        low=_to_decimal(row["low"]) or Decimal("0"),
        close=_to_decimal(row["close"]) or Decimal("0"),
        volume=int(row["volume"]),
        is_final=bool(row["is_final"]),
        session_asia=bool(row["session_asia"]),
        session_london=bool(row["session_london"]),
        session_us=bool(row["session_us"]),
        session_allowed=bool(row["session_allowed"]),
    )


def decode_feature_packet(payload_json: str) -> FeaturePacket:
    """Decode a persisted feature packet payload."""
    return FeaturePacket(**_json_loads(payload_json))


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


def decode_instrument(row: dict[str, Any]) -> InstrumentRecord:
    return InstrumentRecord(
        instrument_id=row["instrument_id"],
        ticker=row["ticker"],
        cusip=row["cusip"],
        asset_class=row["asset_class"],
        description=row["description"],
        exchange=row["exchange"],
        multiplier=_to_decimal(row["multiplier"]),
        is_active=bool(row["is_active"]),
    )


def decode_experiment_run(row: dict[str, Any]) -> ExperimentRunRecord:
    return ExperimentRunRecord(
        experiment_run_id=row["experiment_run_id"],
        name=row["name"],
        description=row["description"],
        market_universe=row["market_universe"],
        timeframe=row["timeframe"],
        feature_version=row["feature_version"],
        signal_version=row["signal_version"],
        sizing_version=row["sizing_version"],
        config_json=row["config_json"],
        started_at=_from_iso(row["started_at"]),
        completed_at=_from_iso(row["completed_at"]) if row["completed_at"] is not None else None,
    )


def decode_derived_feature(row: dict[str, Any]) -> DerivedFeatureRecord:
    return DerivedFeatureRecord(
        feature_id=row["feature_id"],
        bar_id=row["bar_id"],
        experiment_run_id=row["experiment_run_id"],
        atr=_to_decimal(row["atr"]),
        vwap=_to_decimal(row["vwap"]),
        ema_fast=_to_decimal(row["ema_fast"]),
        ema_slow=_to_decimal(row["ema_slow"]),
        velocity=_to_decimal(row["velocity"]),
        velocity_delta=_to_decimal(row["velocity_delta"]),
        stretch_down=_to_decimal(row["stretch_down"]),
        stretch_up=_to_decimal(row["stretch_up"]),
        smoothed_close=_to_decimal(row["smoothed_close"]),
        momentum_raw=_to_decimal(row["momentum_raw"]),
        momentum_norm=_to_decimal(row["momentum_norm"]),
        momentum_delta=_to_decimal(row["momentum_delta"]),
        momentum_acceleration=_to_decimal(row["momentum_acceleration"]),
        volume_ratio=_to_decimal(row["volume_ratio"]),
        signed_impulse=_to_decimal(row["signed_impulse"]),
        smoothed_signed_impulse=_to_decimal(row["smoothed_signed_impulse"]),
        impulse_delta=_to_decimal(row["impulse_delta"]),
        created_at=_from_iso(row["created_at"]),
    )


def decode_signal_evaluation(row: dict[str, Any]) -> SignalEvaluationRecord:
    return SignalEvaluationRecord(
        signal_eval_id=row["signal_eval_id"],
        bar_id=row["bar_id"],
        experiment_run_id=row["experiment_run_id"],
        bull_snap_raw=bool(row["bull_snap_raw"]),
        bear_snap_raw=bool(row["bear_snap_raw"]),
        asia_vwap_reclaim_raw=bool(row["asia_vwap_reclaim_raw"]),
        momentum_compressing_up=bool(row["momentum_compressing_up"]),
        momentum_turning_positive=bool(row["momentum_turning_positive"]),
        momentum_compressing_down=bool(row["momentum_compressing_down"]),
        momentum_turning_negative=bool(row["momentum_turning_negative"]),
        filter_pass_long=bool(row["filter_pass_long"]),
        filter_pass_short=bool(row["filter_pass_short"]),
        trigger_long_math=bool(row["trigger_long_math"]),
        trigger_short_math=bool(row["trigger_short_math"]),
        warmup_complete=bool(row["warmup_complete"]),
        compression_long=bool(row["compression_long"]),
        reclaim_long=bool(row["reclaim_long"]),
        separation_long=bool(row["separation_long"]),
        structure_long_candidate=bool(row["structure_long_candidate"]),
        compression_short=bool(row["compression_short"]),
        failure_short=bool(row["failure_short"]),
        separation_short=bool(row["separation_short"]),
        structure_short_candidate=bool(row["structure_short_candidate"]),
        quality_score_long=_to_decimal(row["quality_score_long"]),
        quality_score_short=_to_decimal(row["quality_score_short"]),
        size_recommendation_long=_to_decimal(row["size_recommendation_long"]),
        size_recommendation_short=_to_decimal(row["size_recommendation_short"]),
        created_at=_from_iso(row["created_at"]),
    )


def decode_trade_outcome(row: dict[str, Any]) -> TradeOutcomeRecord:
    return TradeOutcomeRecord(
        trade_id=row["trade_id"],
        experiment_run_id=row["experiment_run_id"],
        entry_bar_id=row["entry_bar_id"],
        exit_bar_id=row["exit_bar_id"],
        ticker=row["ticker"],
        timeframe=row["timeframe"],
        side=row["side"],
        entry_family=row["entry_family"],
        entry_reason=row["entry_reason"],
        entry_price=Decimal(row["entry_price"]),
        exit_price=_to_decimal(row["exit_price"]),
        size=Decimal(row["size"]),
        bars_held=row["bars_held"],
        pnl=_to_decimal(row["pnl"]),
        mae=_to_decimal(row["mae"]),
        mfe=_to_decimal(row["mfe"]),
        exit_reason=row["exit_reason"],
        quality_score_at_entry=_to_decimal(row["quality_score_at_entry"]),
        size_recommendation_at_entry=_to_decimal(row["size_recommendation_at_entry"]),
        created_at=_from_iso(row["created_at"]),
    )
