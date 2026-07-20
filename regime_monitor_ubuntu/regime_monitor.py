#!/usr/bin/env python3
"""Low-power Flask dashboard and Databento regime monitor.

One process owns Databento ingestion, the current regime state, the /data JSON
endpoint, and the dashboard served at /. The browser-side update boundary is
intentionally isolated so it can later be fed by Server-Sent Events without
changing the rendering code.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify


LOGGER = logging.getLogger("regime_monitor")
VALID_REGIMES = {"LONG", "SHORT", "NO_TRADE"}
REGIME_COLORS = {
    "LONG": "#00e676",
    "SHORT": "#ff3333",
    "NO_TRADE": "#9e9e9e",
    "NO DATA": "#ffcc33",
}
EASTERN_TZ = ZoneInfo("America/New_York")
DEFAULT_CHART_BAR_LIMIT = 72
DEFAULT_STATE_DIR = Path("/var/lib/regime-monitor")
DEFAULT_STATE_FILE_NAME = "candle_state.json"
MULTI_CANDLE_STATE_SCHEMA_VERSION = "regime_monitor_multi_candle_state_v2"
ROUTING_RECORD_DIAGNOSTIC_LIMIT = 20
DEFAULT_SHARED_OHLCV_DB_PATH = Path("var") / "track_b_shared_live_ohlcv.sqlite3"
DEFAULT_TRACK_B_MONITOR_INSTRUMENT_KEYS = ("MNQ", "MES", "MGC")
MONITOR_ONLY_TRACK_B_INSTRUMENT_KEYS = ("MBT",)
DEFAULT_MONITOR_INSTRUMENT_KEYS = DEFAULT_TRACK_B_MONITOR_INSTRUMENT_KEYS + MONITOR_ONLY_TRACK_B_INSTRUMENT_KEYS


@dataclass(frozen=True)
class RegimeSnapshot:
    regime: str
    confidence: float | None
    timestamp: str
    connection_status: str
    error: str | None = None
    received_at: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["colors"] = REGIME_COLORS
        return payload


@dataclass(frozen=True)
class InstrumentConfig:
    key: str
    name: str
    symbol: str


def load_default_instruments_from_catalog(
    *,
    catalog_path: Path | str | None = None,
    instrument_keys: tuple[str, ...] = DEFAULT_MONITOR_INSTRUMENT_KEYS,
) -> tuple[InstrumentConfig, ...]:
    namelist = _load_track_b_live_market_data_symbols(catalog_path=catalog_path)
    if namelist is None:
        return ()
    by_symbol = namelist.by_symbol()
    instruments: list[InstrumentConfig] = []
    missing: list[str] = []
    missing_labels: list[str] = []
    for key in instrument_keys:
        row = by_symbol.get(key)
        if row is None or not row.enabled or not row.databento_symbol:
            missing.append(key)
            continue
        display_label = str(getattr(row, "display_label", "") or "").strip()
        if not display_label:
            missing_labels.append(key)
            continue
        instruments.append(InstrumentConfig(key=key, name=display_label, symbol=row.databento_symbol))
    if missing:
        raise ValueError(
            "Regime monitor default instruments are missing enabled Track B catalog rows: "
            + ", ".join(missing)
            + "."
        )
    if missing_labels:
        raise ValueError(
            "Regime monitor default instruments are missing Track B display labels: "
            + ", ".join(missing_labels)
            + "."
        )
    return tuple(instruments)


def _load_track_b_live_market_data_symbols(*, catalog_path: Path | str | None = None) -> Any | None:
    try:
        from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
            DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
            load_track_b_live_market_data_symbols,
        )
    except ModuleNotFoundError as exc:
        if exc.name != "mgc_v05l":
            raise
        repo_source_root = Path(__file__).resolve().parents[1] / "src"
        if not repo_source_root.exists():
            return None
        sys.path.insert(0, str(repo_source_root))
        from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
            DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
            load_track_b_live_market_data_symbols,
        )

    return load_track_b_live_market_data_symbols(catalog_path or DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH)


def _shared_ohlcv_chart_payload(
    *,
    path: Path | None,
    symbol: str,
    timeframe: str,
    limit: int,
) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        from mgc_v05l.market_data.shared_live_ohlcv_store import shared_live_ohlcv_chart_payload
    except ModuleNotFoundError as exc:
        if exc.name != "mgc_v05l":
            raise
        repo_source_root = Path(__file__).resolve().parents[1] / "src"
        if not repo_source_root.exists():
            return None
        if str(repo_source_root) not in sys.path:
            sys.path.insert(0, str(repo_source_root))
        from mgc_v05l.market_data.shared_live_ohlcv_store import shared_live_ohlcv_chart_payload

    try:
        return shared_live_ohlcv_chart_payload(path=path, symbol=symbol, timeframe=timeframe, limit=limit)
    except Exception as exc:  # noqa: BLE001 - monitor dashboard should degrade to its in-process chart.
        LOGGER.warning("shared_ohlcv_chart_payload_unavailable symbol=%s path=%s error=%s", symbol, path, exc)
        return None


DEFAULT_INSTRUMENTS: tuple[InstrumentConfig, ...] = load_default_instruments_from_catalog()
DEFAULT_DATABENTO_SYMBOLS: tuple[str, ...] = tuple(config.symbol for config in DEFAULT_INSTRUMENTS)


@dataclass(frozen=True)
class DatabentoFeedConfig:
    api_key: str | None
    dataset: str = "GLBX.MDP3"
    schema: str = "trades"
    symbols: tuple[str, ...] = ()
    stype_in: str = "continuous"
    reconnect_interval: float = 5.0

    def __post_init__(self) -> None:
        if not self.symbols:
            object.__setattr__(self, "symbols", DEFAULT_DATABENTO_SYMBOLS)


@dataclass(frozen=True)
class CandleBar:
    time: str
    start: str
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
    completed: bool = False
    source_bar_count: int = 0

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)


class RollingCandleState:
    def __init__(
        self,
        *,
        state_dir: Path,
        symbol: str,
        bar_limit: int = DEFAULT_CHART_BAR_LIMIT,
        persist_interval: float = 1.0,
    ) -> None:
        self.state_dir = state_dir
        self.state_path = state_dir / DEFAULT_STATE_FILE_NAME
        self.symbol = symbol
        self.bar_limit = max(1, int(bar_limit))
        self.persist_interval = max(0.0, float(persist_interval))
        self._lock = threading.Lock()
        self._completed_5m: list[CandleBar] = []
        self._current_5m: CandleBar | None = None
        self._current_1m: CandleBar | None = None
        self._last_persist_monotonic = 0.0
        self._last_error: str | None = None
        self.load()

    @classmethod
    def from_payload(
        cls,
        *,
        state_dir: Path,
        symbol: str,
        payload: object,
        bar_limit: int = DEFAULT_CHART_BAR_LIMIT,
        persist_interval: float = 1.0,
    ) -> RollingCandleState:
        state = cls(
            state_dir=state_dir,
            symbol=symbol,
            bar_limit=bar_limit,
            persist_interval=persist_interval,
        )
        state.load_payload(payload)
        return state

    def load(self) -> None:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            self._last_error = f"state_load_failed: {exc}"
            return
        if not isinstance(payload, dict):
            self._last_error = "state_load_failed: payload_not_object"
            return

        completed = _normalize_payload_candles(payload.get("completed_5m"), completed=True)
        current_5m = _normalize_payload_candle(payload.get("current_5m"), default_completed=False)
        current_1m = _normalize_payload_candle(payload.get("current_1m"), default_completed=False)
        with self._lock:
            self._completed_5m = _dedupe_sorted_candles(completed)[-self.bar_limit :]
            self._current_5m = current_5m
            self._current_1m = current_1m
            self._last_error = None

    def load_payload(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        completed = _normalize_payload_candles(payload.get("completed_5m"), completed=True)
        current_5m = _normalize_payload_candle(payload.get("current_5m"), default_completed=False)
        current_1m = _normalize_payload_candle(payload.get("current_1m"), default_completed=False)
        with self._lock:
            self._completed_5m = _dedupe_sorted_candles(completed)[-self.bar_limit :]
            self._current_5m = current_5m
            self._current_1m = current_1m
            self._last_error = None

    def record_trade(self, *, price: float, event_time: datetime | None = None, persist: bool = True) -> None:
        event_dt = event_time or datetime.now(timezone.utc)
        event_dt = _ensure_utc(event_dt)
        one_minute_start = event_dt.replace(second=0, microsecond=0)
        one_minute_end = one_minute_start + timedelta(minutes=1)
        five_minute_start = _five_minute_bucket_start(event_dt)
        five_minute_end = five_minute_start + timedelta(minutes=5)

        with self._lock:
            current_5m_end = _parse_datetime_or_none(None if self._current_5m is None else self._current_5m.time)
            if current_5m_end is not None and five_minute_end < current_5m_end:
                self._last_error = f"ignored_out_of_order_trade: {event_dt.isoformat()}"
                return
            self._current_1m = _update_bar(
                self._current_1m,
                start=one_minute_start,
                end=one_minute_end,
                price=price,
            )
            if self._current_5m is not None and self._current_5m.time != five_minute_end.isoformat():
                self._completed_5m.append(_complete_bar(self._current_5m))
                self._completed_5m = _dedupe_sorted_candles(self._completed_5m)[-self.bar_limit :]
                self._current_5m = None
            self._current_5m = _update_bar(
                self._current_5m,
                start=five_minute_start,
                end=five_minute_end,
                price=price,
            )
            self._last_error = None
            should_persist = (
                self.persist_interval == 0
                or time.monotonic() - self._last_persist_monotonic >= self.persist_interval
            )
        if persist and should_persist:
            self.persist()

    def payload(self) -> dict[str, Any]:
        with self._lock:
            bars = [bar.to_payload() for bar in self._completed_5m]
            if self._current_5m is not None:
                bars.append(self._current_5m.to_payload())
            bars = sorted(bars, key=lambda bar: _parse_datetime_for_sort(bar["time"]))[-self.bar_limit :]
            error = self._last_error
        return {
            "schema_version": "regime_monitor_in_process_5m_chart_v1",
            "source": "regime_monitor_databento_live",
            "source_detail": "in-process Databento trade stream aggregated to current 1m and rolling 5m candles",
            "symbol": self.symbol,
            "timeframe": "5m",
            "bar_limit": self.bar_limit,
            "bar_count": len(bars),
            "generated_at": utc_now_text(),
            "state_path": str(self.state_path),
            "bars": bars,
            "error": error,
        }

    def persist(self) -> None:
        payload = self.export_state()
        self.state_dir.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{self.state_path.name}.",
            suffix=".tmp",
            dir=str(self.state_dir),
            text=True,
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.state_path)
            self._last_persist_monotonic = time.monotonic()
        finally:
            try:
                if Path(tmp_name).exists():
                    Path(tmp_name).unlink()
            except OSError:
                pass

    def export_state(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema_version": "regime_monitor_candle_state_v1",
                "generated_at": utc_now_text(),
                "symbol": self.symbol,
                "bar_limit": self.bar_limit,
                "completed_5m": [bar.to_payload() for bar in self._completed_5m[-self.bar_limit :]],
                "current_5m": None if self._current_5m is None else self._current_5m.to_payload(),
                "current_1m": None if self._current_1m is None else self._current_1m.to_payload(),
            }


class PriceRegimeState:
    def __init__(self, *, max_prices: int = 20, candle_state: RollingCandleState | None = None) -> None:
        self._lock = threading.Lock()
        self._latest_price: float | None = None
        self._prices: list[float] = []
        self._max_prices = max_prices
        self._connection_status = "STARTING"
        self._error: str | None = None
        self._candle_state = candle_state

    def mark_status(self, status: str, error: str | None = None) -> None:
        with self._lock:
            self._connection_status = status
            self._error = error

    def record_message(self, msg: object, *, persist_candle: bool = True) -> None:
        price_value = getattr(msg, "px", None)
        if price_value is None:
            price_value = getattr(msg, "price", None)
        if price_value is None:
            return

        price = float(price_value) / 1e9
        event_time = _message_event_time(msg)
        with self._lock:
            self._latest_price = price
            self._prices.append(price)
            if len(self._prices) > self._max_prices:
                self._prices.pop(0)
            self._connection_status = "CONNECTED"
            self._error = None
        if self._candle_state is not None:
            self._candle_state.record_trade(price=price, event_time=event_time, persist=persist_candle)

    def snapshot(self) -> RegimeSnapshot:
        now = datetime.now(EASTERN_TZ)
        with self._lock:
            latest_price = self._latest_price
            prices = list(self._prices)
            connection_status = self._connection_status
            error = self._error

        timestamp = now.strftime("%H:%M:%S")
        if not latest_price:
            return RegimeSnapshot(
                regime="NO_TRADE",
                confidence=0,
                timestamp=timestamp,
                connection_status=connection_status,
                error=error,
                received_at=utc_now_text(),
            )

        ma = sum(prices) / len(prices) if prices else latest_price
        regime = "LONG" if latest_price > ma else "SHORT"
        confidence = abs(latest_price - ma) / ma if ma != 0 else 0
        return RegimeSnapshot(
            regime=regime,
            confidence=round(confidence, 4),
            timestamp=timestamp,
            connection_status=connection_status,
            error=error,
            received_at=utc_now_text(),
        )


class InstrumentMonitorState:
    def __init__(
        self,
        *,
        config: InstrumentConfig,
        state_dir: Path,
        bar_limit: int,
        persisted_candles: object = None,
    ) -> None:
        self.config = config
        self.candles = RollingCandleState.from_payload(
            state_dir=state_dir,
            symbol=config.key,
            payload=persisted_candles,
            bar_limit=bar_limit,
            persist_interval=9999,
        )
        self.regime = PriceRegimeState(candle_state=self.candles)
        self._lock = threading.Lock()
        self._resolved_instrument_id: int | None = None
        self._last_source_symbol: str | None = None
        self._last_routed_at: str | None = None
        self._routing_status = "AWAITING_MAPPING"
        self._routing_error: str | None = None

    def mark_status(self, status: str, error: str | None = None) -> None:
        self.regime.mark_status(status, error)

    def bind_route(self, *, instrument_id: int | None, source_symbol: str | None) -> None:
        with self._lock:
            if instrument_id is not None:
                self._resolved_instrument_id = instrument_id
            if source_symbol:
                self._last_source_symbol = source_symbol
            self._routing_status = "MAPPED"
            self._routing_error = None

    def mark_routing_error(self, status: str, error: str) -> None:
        with self._lock:
            self._routing_status = status
            self._routing_error = error

    def record_message(self, msg: object, *, instrument_id: int | None, source_symbol: str | None) -> None:
        self.bind_route(instrument_id=instrument_id, source_symbol=source_symbol)
        self.regime.record_message(msg, persist_candle=False)
        with self._lock:
            self._last_routed_at = utc_now_text()

    def payload(self) -> dict[str, Any]:
        snapshot = self.regime.snapshot().to_payload()
        with self._lock:
            resolved_instrument_id = self._resolved_instrument_id
            last_source_symbol = self._last_source_symbol
            last_routed_at = self._last_routed_at
            routing_status = self._routing_status
            routing_error = self._routing_error
        snapshot["instrument"] = self.config.key
        snapshot["name"] = self.config.name
        snapshot["symbol"] = self.config.symbol
        snapshot["resolved_instrument_id"] = resolved_instrument_id
        snapshot["last_source_symbol"] = last_source_symbol
        snapshot["routing_status"] = routing_status
        snapshot["routing_error"] = routing_error
        snapshot["last_routed_at"] = last_routed_at
        snapshot["chart"] = self.candles.payload()
        return snapshot

    def export_state(self) -> dict[str, Any]:
        return {
            "name": self.config.name,
            "symbol": self.config.symbol,
            "candles": self.candles.export_state(),
        }


class RoutingDiagnostics:
    def __init__(self, *, record_limit: int = ROUTING_RECORD_DIAGNOSTIC_LIMIT) -> None:
        self.record_limit = record_limit
        self._lock = threading.Lock()
        self.subscriptions: list[dict[str, Any]] = []
        self.unknown_instrument_ids: list[int] = []
        self.records_seen = 0
        self.records_routed = 0
        self.records_rejected = 0
        self.first_records: list[dict[str, Any]] = []

    def record_subscription(self, *, dataset: str, schema: str, symbols: list[str], stype_in: str) -> None:
        entry = {
            "requested_at": utc_now_text(),
            "dataset": dataset,
            "schema": schema,
            "symbols": symbols,
            "stype_in": stype_in,
        }
        with self._lock:
            self.subscriptions.append(entry)
        LOGGER.info("databento_subscription_requested %s", json.dumps(entry, sort_keys=True))

    def log_symbol_mapping_message(self, msg: object, *, instrument_id: int, candidates: list[str]) -> None:
        entry = {
            "received_at": utc_now_text(),
            "record_type": _message_record_type(msg),
            "instrument_id": instrument_id,
            "symbol_candidates": candidates,
        }
        LOGGER.info("databento_symbol_mapping_message %s", json.dumps(entry, sort_keys=True))

    def log_mapping_created(self, *, instrument_id: int, key: str, source_symbol: str) -> None:
        entry = {
            "mapped_at": utc_now_text(),
            "instrument_id": instrument_id,
            "instrument": key,
            "source_symbol": source_symbol,
        }
        LOGGER.info("databento_instrument_mapping_created %s", json.dumps(entry, sort_keys=True))

    def record_price_decision(
        self,
        *,
        msg: object,
        instrument_id: int | None,
        resolved_symbol: str | None,
        accepted: bool,
        rejection_reason: str | None,
    ) -> None:
        entry = {
            "received_at": utc_now_text(),
            "record_type": _message_record_type(msg),
            "instrument_id": instrument_id,
            "resolved_symbol": resolved_symbol,
            "accepted": accepted,
            "rejection_reason": rejection_reason,
        }
        with self._lock:
            self.records_seen += 1
            if accepted:
                self.records_routed += 1
            else:
                self.records_rejected += 1
                if instrument_id is not None and instrument_id not in self.unknown_instrument_ids:
                    self.unknown_instrument_ids.append(instrument_id)
            should_log_record = len(self.first_records) < self.record_limit
            if should_log_record:
                self.first_records.append(entry)
        if should_log_record:
            LOGGER.info("databento_price_record_routing_decision %s", json.dumps(entry, sort_keys=True))

    def payload(self, *, instrument_map: dict[int, str]) -> dict[str, Any]:
        with self._lock:
            return {
                "subscriptions": list(self.subscriptions),
                "instrument_map": {
                    str(instrument_id): key
                    for instrument_id, key in sorted(instrument_map.items())
                },
                "unknown_instrument_ids": list(self.unknown_instrument_ids),
                "records_seen": self.records_seen,
                "records_routed": self.records_routed,
                "records_rejected": self.records_rejected,
                "first_records": list(self.first_records),
            }


class MultiInstrumentMonitorState:
    def __init__(
        self,
        *,
        instruments: tuple[InstrumentConfig, ...],
        state_dir: Path,
        bar_limit: int = DEFAULT_CHART_BAR_LIMIT,
        persist_interval: float = 1.0,
        shared_ohlcv_db_path: Path | None = None,
    ) -> None:
        self.instruments = instruments
        self.state_dir = state_dir
        self.state_path = state_dir / DEFAULT_STATE_FILE_NAME
        self.bar_limit = bar_limit
        self.shared_ohlcv_db_path = shared_ohlcv_db_path
        self.persist_interval = max(0.0, float(persist_interval))
        self._last_persist_monotonic = 0.0
        self._lock = threading.Lock()
        self._instrument_by_key: dict[str, InstrumentMonitorState] = {}
        self._instrument_id_to_key: dict[int, str] = {}
        self._unmapped_record_count = 0
        self._last_unmapped_instrument_id: int | None = None
        self._last_unmapped_at: str | None = None
        self._routing_diagnostics = RoutingDiagnostics()
        persisted = self._load_persisted_state()
        persisted_instruments = persisted.get("instruments") if isinstance(persisted, dict) else {}
        for config in instruments:
            persisted_entry = persisted_instruments.get(config.key, {}) if isinstance(persisted_instruments, dict) else {}
            persisted_candles = persisted_entry.get("candles") if isinstance(persisted_entry, dict) else None
            self._instrument_by_key[config.key] = InstrumentMonitorState(
                config=config,
                state_dir=state_dir,
                bar_limit=bar_limit,
                persisted_candles=persisted_candles,
            )
        self._symbol_to_key = _build_symbol_lookup(instruments)

    def mark_all(self, status: str, error: str | None = None) -> None:
        for state in self._instrument_by_key.values():
            state.mark_status(status, error)

    def mark_instrument(self, key: str, status: str, error: str | None = None) -> None:
        instrument = self._instrument_by_key.get(key.upper())
        if instrument is not None:
            instrument.mark_status(status, error)

    def record_subscription(self, config: DatabentoFeedConfig) -> None:
        self._routing_diagnostics.record_subscription(
            dataset=config.dataset,
            schema=config.schema,
            symbols=list(config.symbols),
            stype_in=config.stype_in,
        )

    def record_message(self, msg: object) -> str | None:
        mapping_key = self.record_symbol_mapping(msg)
        if mapping_key is not None and not _message_has_price(msg):
            return None
        key = self.resolve_message_key(msg)
        if key is None:
            self._record_unmapped(msg)
            self._record_price_routing_decision(msg, key=None, accepted=False, rejection_reason=_unresolved_record_reason(msg))
            return None
        instrument = self._instrument_by_key.get(key)
        if instrument is None:
            self._record_unmapped(msg)
            self._record_price_routing_decision(
                msg,
                key=key,
                accepted=False,
                rejection_reason="RESOLVED_INSTRUMENT_NOT_CONFIGURED",
            )
            return None
        instrument_id = _message_instrument_id(msg)
        source_symbol = _message_primary_source_symbol(msg)
        instrument.record_message(msg, instrument_id=instrument_id, source_symbol=source_symbol)
        self._record_price_routing_decision(msg, key=key, accepted=True, rejection_reason=None)
        if self.persist_interval == 0 or time.monotonic() - self._last_persist_monotonic >= self.persist_interval:
            self.persist()
        return key

    def record_symbol_mapping(self, msg: object) -> str | None:
        instrument_id = _message_instrument_id(msg)
        if instrument_id is None:
            return None
        candidates = _message_symbol_candidates(msg)
        if not _message_has_price(msg):
            self._routing_diagnostics.log_symbol_mapping_message(
                msg,
                instrument_id=instrument_id,
                candidates=candidates,
            )
        for candidate in candidates:
            key = _lookup_symbol_key(self._symbol_to_key, candidate)
            if key is not None:
                previous_key = self._instrument_id_to_key.get(instrument_id)
                self._instrument_id_to_key[instrument_id] = key
                if previous_key != key:
                    self._routing_diagnostics.log_mapping_created(
                        instrument_id=instrument_id,
                        key=key,
                        source_symbol=candidate,
                    )
                instrument = self._instrument_by_key.get(key)
                if instrument is not None:
                    instrument.bind_route(instrument_id=instrument_id, source_symbol=candidate)
                return key
        return None

    def resolve_message_key(self, msg: object) -> str | None:
        instrument_id = _message_instrument_id(msg)
        if instrument_id is not None:
            key = self._instrument_id_to_key.get(instrument_id)
            if key is not None:
                return key
            mapped_key = self.record_symbol_mapping(msg)
            if mapped_key is not None:
                return mapped_key
            return None
        for candidate in _message_symbol_candidates(msg):
            key = _lookup_symbol_key(self._symbol_to_key, candidate)
            if key is not None:
                return key
        return None

    def payload(self) -> dict[str, Any]:
        instruments_payload: dict[str, Any] = {}
        for config in self.instruments:
            item = self._instrument_by_key[config.key].payload()
            shared_chart = _shared_ohlcv_chart_payload(
                path=self.shared_ohlcv_db_path,
                symbol=config.key,
                timeframe="5m",
                limit=self.bar_limit,
            )
            if shared_chart is not None:
                item["chart"] = shared_chart
            instruments_payload[config.key] = item
        return {
            "schema_version": "regime_monitor_multi_instrument_v1",
            "generated_at": utc_now_text(),
            "chart_source": {
                "preferred_source": "shared_live_ohlcv_store",
                "shared_ohlcv_db_path": None if self.shared_ohlcv_db_path is None else str(self.shared_ohlcv_db_path),
                "timeframe": "5m",
                "bar_limit": self.bar_limit,
            },
            "routing": {
                "schema_version": "regime_monitor_databento_routing_v1",
                "instrument_id_map": {
                    str(instrument_id): key
                    for instrument_id, key in sorted(self._instrument_id_to_key.items())
                },
                "unmapped_record_count": self._unmapped_record_count,
                "last_unmapped_instrument_id": self._last_unmapped_instrument_id,
                "last_unmapped_at": self._last_unmapped_at,
                **self._routing_diagnostics.payload(instrument_map=self._instrument_id_to_key),
            },
            "instruments": instruments_payload,
        }

    def persist(self) -> None:
        with self._lock:
            payload = {
                "schema_version": MULTI_CANDLE_STATE_SCHEMA_VERSION,
                "generated_at": utc_now_text(),
                "bar_limit": self.bar_limit,
                "configured_symbols": {
                    config.key: config.symbol
                    for config in self.instruments
                },
                "instruments": {
                    key: state.export_state()
                    for key, state in self._instrument_by_key.items()
                },
            }
            _atomic_write_json(self.state_path, payload)
            self._last_persist_monotonic = time.monotonic()

    def _load_persisted_state(self) -> dict[str, Any]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (FileNotFoundError, OSError, json.JSONDecodeError):
            return {}
        if not isinstance(payload, dict):
            return {}
        if payload.get("schema_version") != MULTI_CANDLE_STATE_SCHEMA_VERSION:
            return {}
        configured_symbols = payload.get("configured_symbols")
        expected_symbols = {config.key: config.symbol for config in self.instruments}
        if configured_symbols != expected_symbols:
            return {}
        return payload

    def _record_unmapped(self, msg: object) -> None:
        instrument_id = _message_instrument_id(msg)
        with self._lock:
            self._unmapped_record_count += 1
            self._last_unmapped_instrument_id = instrument_id
            self._last_unmapped_at = utc_now_text()

    def _record_price_routing_decision(
        self,
        msg: object,
        *,
        key: str | None,
        accepted: bool,
        rejection_reason: str | None,
    ) -> None:
        if not _message_has_price(msg):
            return
        self._routing_diagnostics.record_price_decision(
            msg=msg,
            instrument_id=_message_instrument_id(msg),
            resolved_symbol=key,
            accepted=accepted,
            rejection_reason=rejection_reason,
        )


def utc_now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_config(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        text=True,
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, path)
    finally:
        try:
            if Path(tmp_name).exists():
                Path(tmp_name).unlink()
        except OSError:
            pass


def _build_symbol_lookup(instruments: tuple[InstrumentConfig, ...]) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for config in instruments:
        root = config.symbol.split(".", 1)[0].upper()
        aliases = {
            config.key.upper(),
            config.symbol.upper(),
            root,
            f"{root}.FUT",
            f"{root}.V.0",
        }
        for alias in aliases:
            lookup[alias] = config.key
    return lookup


def _lookup_symbol_key(lookup: dict[str, str], symbol: object) -> str | None:
    text = str(symbol or "").strip().upper()
    if not text:
        return None
    direct = lookup.get(text)
    if direct is not None:
        return direct
    root = _symbol_root_candidate(text)
    return lookup.get(root)


def _symbol_root_candidate(symbol: str) -> str:
    text = symbol.strip().upper()
    if "." in text:
        return text.split(".", 1)[0]
    for index, char in enumerate(text):
        if index > 0 and char in "FGHJKMNQUVXZ" and index + 1 < len(text) and text[index + 1].isdigit():
            return text[:index]
    return text


def _message_has_price(msg: object) -> bool:
    return getattr(msg, "px", None) is not None or getattr(msg, "price", None) is not None


def _message_instrument_id(msg: object) -> int | None:
    for value in (
        getattr(msg, "instrument_id", None),
        getattr(getattr(msg, "hd", None), "instrument_id", None),
        getattr(getattr(msg, "header", None), "instrument_id", None),
    ):
        parsed = _int_or_none(value)
        if parsed is not None:
            return parsed
    text = str(msg)
    for marker in ("instrument_id=", "instrument_id:"):
        if marker in text:
            tail = text.split(marker, 1)[1]
            token = tail.split(",", 1)[0].split(")", 1)[0].strip().strip("'\"")
            parsed = _int_or_none(token)
            if parsed is not None:
                return parsed
    return None


def _message_primary_source_symbol(msg: object) -> str | None:
    candidates = _message_symbol_candidates(msg)
    return candidates[0] if candidates else None


def _message_record_type(msg: object) -> str:
    return type(msg).__name__


def _unresolved_record_reason(msg: object) -> str:
    if _message_instrument_id(msg) is not None:
        return "UNMAPPED_INSTRUMENT_ID"
    if _message_symbol_candidates(msg):
        return "UNRESOLVED_SYMBOL"
    return "MISSING_INSTRUMENT_ID_OR_SYMBOL"


def _message_symbol_candidates(msg: object) -> list[str]:
    candidates: list[str] = []
    for attr in (
        "symbol",
        "raw_symbol",
        "stype_in_symbol",
        "stype_out_symbol",
        "continuous_symbol",
        "parent_symbol",
        "instrument_symbol",
        "local_symbol",
        "localSymbol",
        "native_symbol",
    ):
        value = getattr(msg, attr, None)
        if value not in (None, ""):
            candidates.append(str(value).strip())
    text = str(msg)
    for marker in (
        "symbol=",
        "raw_symbol=",
        "stype_in_symbol=",
        "stype_out_symbol=",
        "local_symbol=",
        "localSymbol=",
    ):
        if marker in text:
            tail = text.split(marker, 1)[1]
            token = tail.split(",", 1)[0].split(")", 1)[0].strip().strip("'\"")
            if token:
                candidates.append(token)
    return candidates


def _normalize_payload_candles(raw_bars: object, *, completed: bool) -> list[CandleBar]:
    if not isinstance(raw_bars, list):
        return []
    bars: list[CandleBar] = []
    for raw in raw_bars:
        bar = _normalize_payload_candle(raw, default_completed=completed)
        if bar is not None:
            bars.append(bar)
    return sorted(bars, key=lambda bar: _parse_datetime_for_sort(bar.time))


def _normalize_payload_candle(raw: object, *, default_completed: bool) -> CandleBar | None:
    if not isinstance(raw, dict):
        return None
    end_text = raw.get("time") or raw.get("bar_end") or raw.get("timestamp")
    start_text = raw.get("start") or raw.get("bar_start")
    end = _parse_datetime_or_none(end_text)
    start = _parse_datetime_or_none(start_text)
    open_value = _float_or_none(raw.get("open"))
    high_value = _float_or_none(raw.get("high"))
    low_value = _float_or_none(raw.get("low"))
    close_value = _float_or_none(raw.get("close"))
    if end is None or start is None or None in (open_value, high_value, low_value, close_value):
        return None
    return CandleBar(
        time=end.isoformat(),
        start=start.isoformat(),
        open=open_value,
        high=high_value,
        low=low_value,
        close=close_value,
        volume=_float_or_none(raw.get("volume")) or 0.0,
        completed=bool(raw.get("completed", default_completed)),
        source_bar_count=max(0, int(_float_or_none(raw.get("source_bar_count")) or 0)),
    )


def _dedupe_sorted_candles(bars: list[CandleBar]) -> list[CandleBar]:
    by_time = {bar.time: bar for bar in bars}
    return [by_time[key] for key in sorted(by_time, key=_parse_datetime_for_sort)]


def _complete_bar(bar: CandleBar) -> CandleBar:
    return CandleBar(
        time=bar.time,
        start=bar.start,
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        completed=True,
        source_bar_count=bar.source_bar_count,
    )


def _update_bar(current: CandleBar | None, *, start: datetime, end: datetime, price: float) -> CandleBar:
    start_text = start.isoformat()
    end_text = end.isoformat()
    if current is None or current.time != end_text:
        return CandleBar(
            time=end_text,
            start=start_text,
            open=price,
            high=price,
            low=price,
            close=price,
            volume=1.0,
            completed=False,
            source_bar_count=1,
        )
    return CandleBar(
        time=current.time,
        start=current.start,
        open=current.open,
        high=max(current.high, price),
        low=min(current.low, price),
        close=price,
        volume=current.volume + 1.0,
        completed=False,
        source_bar_count=current.source_bar_count + 1,
    )


def _five_minute_bucket_start(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    epoch_minutes = int(utc_value.timestamp() // 60)
    bucket_epoch_minutes = (epoch_minutes // 5) * 5
    return datetime.fromtimestamp(bucket_epoch_minutes * 60, tz=timezone.utc)


def _message_event_time(msg: object) -> datetime | None:
    for attr in ("ts_event", "ts_recv", "ts_out", "timestamp"):
        raw = getattr(msg, attr, None)
        parsed = _databento_time_or_none(raw)
        if parsed is not None:
            return parsed
    return None


def _databento_time_or_none(value: object) -> datetime | None:
    parsed = _parse_datetime_or_none(value)
    if parsed is not None:
        return parsed
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1_000_000_000_000_000:
            return datetime.fromtimestamp(numeric / 1_000_000_000, tz=timezone.utc)
        if numeric > 1_000_000_000:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
    return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime_for_sort(value: object) -> datetime:
    parsed = _parse_datetime_or_none(value)
    return parsed or datetime.min.replace(tzinfo=timezone.utc)


def _parse_datetime_or_none(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def run_databento_feed(
    *,
    config: DatabentoFeedConfig,
    state: PriceRegimeState | MultiInstrumentMonitorState,
    stop: threading.Event,
    live_factory: Any | None = None,
) -> None:
    if not config.api_key:
        _mark_feed_state(state, "NO_API_KEY", "DATABENTO_API_KEY is not configured")
        return

    if live_factory is None:
        try:
            import databento as db  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:
            if exc.name == "databento":
                _mark_feed_state(state, "DATABENTO_PACKAGE_MISSING", "Install the databento Python package")
                return
            raise
        live_factory = db.Live

    while not stop.is_set():
        client: Any | None = None
        try:
            _mark_feed_state(state, "CONNECTING")
            client = live_factory(key=config.api_key)
            _mark_feed_state(state, "CONNECTING")
            if isinstance(state, MultiInstrumentMonitorState):
                state.record_subscription(config)
            client.subscribe(
                dataset=config.dataset,
                schema=config.schema,
                symbols=list(config.symbols),
                stype_in=config.stype_in,
            )
            _mark_feed_state(state, "CONNECTED")
            for msg in client:
                if stop.is_set():
                    break
                print(msg, flush=True)
                state.record_message(msg)
            if not stop.is_set():
                _mark_feed_state(state, "DISCONNECTED", "Databento live feed ended")
        except Exception as exc:
            _mark_feed_state(state, "DISCONNECTED", str(exc))
        finally:
            close = getattr(client, "close", None)
            stop_client = getattr(client, "stop", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            elif callable(stop_client):
                try:
                    stop_client()
                except Exception:
                    pass

        stop.wait(max(1.0, config.reconnect_interval))


def _mark_feed_state(
    state: PriceRegimeState | MultiInstrumentMonitorState,
    status: str,
    error: str | None = None,
) -> None:
    if isinstance(state, MultiInstrumentMonitorState):
        state.mark_all(status, error)
    else:
        state.mark_status(status, error)


def create_app(
    *,
    state: PriceRegimeState | MultiInstrumentMonitorState,
    candle_source: RollingCandleState | None = None,
) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def dashboard() -> Response:
        return Response(DASHBOARD_HTML, mimetype="text/html")

    @app.get("/data")
    def data() -> Response:
        if isinstance(state, MultiInstrumentMonitorState):
            payload = state.payload()
        else:
            payload = state.snapshot().to_payload()
        if candle_source is not None and "instruments" not in payload:
            payload["chart"] = candle_source.payload()
        response = jsonify(payload)
        response.headers["Cache-Control"] = "no-store"
        return response

    return app


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Regime Monitor</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #000;
      --panel: #030303;
      --panel-border: #171717;
      --text: #f4f4f4;
      --muted: #8c8c8c;
      --long: #00e676;
      --short: #ff3333;
      --no-trade: #9e9e9e;
      --no-data: #ffcc33;
    }
    * { box-sizing: border-box; }
    html, body {
      width: 100%;
      height: 100%;
      margin: 0;
      overflow: hidden;
      background: var(--bg);
      color: var(--text);
      cursor: none;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      width: 100vw;
      height: 100vh;
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      grid-template-rows: repeat(2, minmax(0, 1fr));
      gap: 8px;
      padding: 8px;
      background: #000;
    }
    .panel {
      min-width: 0;
      min-height: 0;
      display: grid;
      grid-template-rows: auto auto minmax(0, 1fr) auto;
      gap: 4px;
      padding: 10px 12px 8px;
      background: var(--panel);
      border: 1px solid var(--panel-border);
      overflow: hidden;
    }
    .instrument {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 1rem;
      min-width: 0;
    }
    .name {
      margin: 0;
      font-size: clamp(1.4rem, 2.1vw, 2.3rem);
      line-height: 1;
      font-weight: 700;
      letter-spacing: 0;
    }
    .symbol {
      color: var(--muted);
      font-size: clamp(1rem, 1.35vw, 1.45rem);
      font-weight: 600;
      white-space: nowrap;
    }
    .regime {
      margin: 0;
      font-size: clamp(3.2rem, 6.2vw, 7.2rem);
      line-height: 0.92;
      letter-spacing: 0;
      font-weight: 800;
      color: var(--no-data);
      text-align: center;
      overflow-wrap: anywhere;
    }
    .confidence {
      margin: 0;
      color: #ddd;
      font-size: clamp(1.2rem, 2vw, 2.4rem);
      line-height: 1;
      font-weight: 550;
      text-align: center;
    }
    .chart {
      display: block;
      width: 100%;
      height: 100%;
      min-height: 190px;
      background: #020202;
    }
    .status {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 0.4rem 1rem;
      color: var(--muted);
      font-size: clamp(0.9rem, 1.1vw, 1.2rem);
      line-height: 1.1;
      font-weight: 500;
      min-width: 0;
    }
    .connection[data-state="CONNECTED"] { color: var(--long); }
    .connection[data-state="CONNECTING"] { color: #ddd; }
    .connection[data-state="DISCONNECTED"],
    .connection[data-state="NO_API_KEY"],
    .connection[data-state="DATABENTO_PACKAGE_MISSING"],
    .connection[data-state="STARTING"],
    .connection[data-state="DASHBOARD_DISCONNECTED"] { color: var(--no-data); }
    .error {
      grid-column: 1 / -1;
      min-height: 1.1em;
      color: var(--no-data);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .route {
      grid-column: 1 / -1;
      min-height: 1.1em;
      color: #5f5f5f;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .hidden { visibility: hidden; }
  </style>
</head>
<body>
  <main id="dashboard" aria-live="polite" aria-atomic="false"></main>
  <template id="panel-template">
    <article class="panel">
      <header class="instrument">
        <h2 class="name"></h2>
        <div class="symbol"></div>
      </header>
      <section>
        <h1 class="regime">NO DATA</h1>
        <p class="confidence">Confidence: --</p>
      </section>
      <canvas class="chart"></canvas>
      <footer class="status">
        <div class="connection" data-state="STARTING">STARTING</div>
        <div class="timestamp">--:--:--</div>
        <div class="route">id: -- source: --</div>
        <div class="error hidden"></div>
      </footer>
    </article>
  </template>
  <script>
    const DATA_ENDPOINT = "/data";
    const POLL_INTERVAL_MS = 250;
    const PANEL_ORDER = ["MNQ", "MES", "MGC", "MBT"];
    const colors = {
      LONG: "var(--long)",
      SHORT: "var(--short)",
      NO_TRADE: "var(--no-trade)",
      "NO DATA": "var(--no-data)",
    };
    const chartAxis = {
      yLabelFont: "500 22px system-ui, sans-serif",
      xLabelFont: "500 20px system-ui, sans-serif",
      yLabelWidth: 94,
      rightPadding: 72,
      topPadding: 14,
      bottomPadding: 44,
      yLabelGap: 12,
      xLabelBottomGap: 11,
      minXLabelGap: 132,
      fallbackLabelFont: "500 20px system-ui, sans-serif",
    };
    const dashboard = document.getElementById("dashboard");
    const template = document.getElementById("panel-template");
    const panels = new Map();

    function ensurePanel(key, payload) {
      if (panels.has(key)) return panels.get(key);
      const fragment = template.content.cloneNode(true);
      const panel = fragment.querySelector(".panel");
      const nodes = {
        panel,
        name: fragment.querySelector(".name"),
        symbol: fragment.querySelector(".symbol"),
        regime: fragment.querySelector(".regime"),
        confidence: fragment.querySelector(".confidence"),
        canvas: fragment.querySelector(".chart"),
        connection: fragment.querySelector(".connection"),
        timestamp: fragment.querySelector(".timestamp"),
        route: fragment.querySelector(".route"),
        error: fragment.querySelector(".error"),
        current: {},
      };
      panel.dataset.instrument = key;
      nodes.name.textContent = payload.name || key;
      nodes.symbol.textContent = payload.symbol || key;
      dashboard.appendChild(fragment);
      panels.set(key, nodes);
      return nodes;
    }

    function updateText(nodes, field, value) {
      const next = value == null || value === "" ? "" : String(value);
      if (nodes.current[field] === next) return;
      nodes.current[field] = next;
      nodes[field].textContent = next;
    }

    function updatePanel(key, payload) {
      const nodes = ensurePanel(key, payload);
      updateText(nodes, "name", payload.name || key);
      updateText(nodes, "symbol", payload.symbol || key);
      const regime = payload.regime || "NO DATA";
      updateText(nodes, "regime", regime);
      const color = colors[regime] || colors["NO DATA"];
      if (nodes.current.regimeColor !== color) {
        nodes.current.regimeColor = color;
        nodes.regime.style.color = color;
      }
      const confidence = typeof payload.confidence === "number"
        ? `Confidence: ${(payload.confidence * 100).toFixed(1)}%`
        : "Confidence: --";
      updateText(nodes, "confidence", confidence);
      const status = payload.connection_status || "UNKNOWN";
      if (nodes.current.connectionStatus !== status) {
        nodes.current.connectionStatus = status;
        nodes.connection.dataset.state = status;
        nodes.connection.textContent = status;
      }
      updateText(nodes, "timestamp", payload.timestamp || "--:--:--");
      const route = `id: ${payload.resolved_instrument_id ?? "--"} source: ${payload.last_source_symbol || "--"}`;
      updateText(nodes, "route", route);
      const error = payload.error || "";
      updateText(nodes, "error", error);
      nodes.error.classList.toggle("hidden", error === "");
      updateChart(nodes, payload.chart || null);
    }

    function updateDashboard(payload) {
      const instruments = payload && payload.instruments ? payload.instruments : { MBT: payload || {} };
      const orderedKeys = PANEL_ORDER.filter((key) => instruments[key]).concat(
        Object.keys(instruments).filter((key) => !PANEL_ORDER.includes(key)).sort()
      );
      for (const key of orderedKeys) {
        updatePanel(key, instruments[key] || {});
      }
    }

    // Future SSE migration point: EventSource messages can call this function
    // directly without changing the render/update logic.
    function handleSnapshotMessage(data) {
      updateDashboard(data);
    }

    async function pollOnce() {
      try {
        const response = await fetch(DATA_ENDPOINT, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        handleSnapshotMessage(await response.json());
      } catch (error) {
        const fallback = {};
        for (const key of PANEL_ORDER) {
          fallback[key] = {
            name: key,
            symbol: key,
            regime: "NO DATA",
            confidence: null,
            timestamp: new Date().toISOString(),
            connection_status: "DASHBOARD_DISCONNECTED",
            error: error.message,
            chart: { bars: [] },
          };
        }
        handleSnapshotMessage({ instruments: fallback });
      }
    }

    function updateChart(nodes, chart) {
      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const key = JSON.stringify(bars);
      if (nodes.current.chartKey === key) return;
      nodes.current.chartKey = key;
      nodes.current.lastChartPayload = chart || { bars: [] };
      drawChart(nodes.canvas, nodes.current.lastChartPayload);
    }

    function resizeChartCanvas(canvas) {
      const rect = canvas.getBoundingClientRect();
      const ratio = window.devicePixelRatio || 1;
      const width = Math.max(1, Math.floor(rect.width * ratio));
      const height = Math.max(1, Math.floor(rect.height * ratio));
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }
      const context = canvas.getContext("2d");
      context.setTransform(ratio, 0, 0, ratio, 0, 0);
      return { context, width: rect.width, height: rect.height };
    }

    function drawChart(canvas, chart) {
      const { context, width, height } = resizeChartCanvas(canvas);
      context.clearRect(0, 0, width, height);
      context.fillStyle = "#020202";
      context.fillRect(0, 0, width, height);

      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const valid = bars.filter((bar) =>
        Number.isFinite(bar.open) &&
        Number.isFinite(bar.high) &&
        Number.isFinite(bar.low) &&
        Number.isFinite(bar.close)
      );
      const left = chartAxis.yLabelWidth;
      const right = chartAxis.rightPadding;
      const top = chartAxis.topPadding;
      const bottom = chartAxis.bottomPadding;
      const plotWidth = Math.max(1, width - left - right);
      const plotHeight = Math.max(1, height - top - bottom);

      context.strokeStyle = "#151515";
      context.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = top + (plotHeight * i / 4);
        context.beginPath();
        context.moveTo(left, y);
        context.lineTo(width - right, y);
        context.stroke();
      }

      if (valid.length === 0) {
        context.fillStyle = "#555";
        context.font = chartAxis.fallbackLabelFont;
        context.textAlign = "center";
        context.fillText("Waiting for live 5m candles", width / 2, height / 2);
        return;
      }

      const highs = valid.map((bar) => bar.high);
      const lows = valid.map((bar) => bar.low);
      let minPrice = Math.min(...lows);
      let maxPrice = Math.max(...highs);
      const span = Math.max(maxPrice - minPrice, Math.abs(maxPrice) * 0.0005, 1);
      const padding = span * 0.08;
      minPrice -= padding;
      maxPrice += padding;
      const priceToY = (price) => top + ((maxPrice - price) / (maxPrice - minPrice)) * plotHeight;
      const candleStep = plotWidth / Math.max(valid.length, 1);
      const bodyWidth = Math.max(2, Math.min(12, candleStep * 0.58));

      context.font = chartAxis.yLabelFont;
      context.textAlign = "right";
      context.fillStyle = "#777";
      for (let i = 0; i <= 4; i += 1) {
        const price = maxPrice - ((maxPrice - minPrice) * i / 4);
        context.fillText(formatPrice(price), left - chartAxis.yLabelGap, top + (plotHeight * i / 4) + 7);
      }

      const timeLabelIndices = calculateTimeLabelIndices(valid.length, plotWidth);
      valid.forEach((bar, index) => {
        const x = left + candleStep * index + candleStep / 2;
        const openY = priceToY(bar.open);
        const closeY = priceToY(bar.close);
        const highY = priceToY(bar.high);
        const lowY = priceToY(bar.low);
        const rising = bar.close >= bar.open;
        const color = rising ? "#00e676" : "#ff3333";
        context.strokeStyle = color;
        context.fillStyle = color;
        context.globalAlpha = bar.completed === false ? 0.55 : 1;
        context.beginPath();
        context.moveTo(x, highY);
        context.lineTo(x, lowY);
        context.stroke();
        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(2, Math.abs(closeY - openY));
        context.fillRect(x - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
        context.globalAlpha = 1;

        if (timeLabelIndices.has(index)) {
          context.fillStyle = "#777";
          context.font = chartAxis.xLabelFont;
          context.textAlign = index === valid.length - 1 ? "right" : "center";
          context.fillText(formatTimeLabel(bar.time), x, height - chartAxis.xLabelBottomGap);
        }
      });
    }

    function calculateTimeLabelIndices(barCount, plotWidth) {
      if (barCount <= 0) return new Set();
      if (barCount === 1) return new Set([0]);
      const candleStep = plotWidth / barCount;
      const maxLabels = Math.max(2, Math.floor(plotWidth / chartAxis.minXLabelGap));
      const tickEvery = Math.max(1, Math.ceil(barCount / maxLabels));
      const indices = [];
      for (let index = 0; index < barCount; index += tickEvery) {
        const distanceToFinal = (barCount - 1 - index) * candleStep;
        if (index === 0 || distanceToFinal >= chartAxis.minXLabelGap) {
          indices.push(index);
        }
      }
      const finalIndex = barCount - 1;
      const previousIndex = indices[indices.length - 1];
      if (previousIndex == null) {
        indices.push(finalIndex);
      } else if ((finalIndex - previousIndex) * candleStep >= chartAxis.minXLabelGap) {
        indices.push(finalIndex);
      } else {
        indices[indices.length - 1] = finalIndex;
      }
      return new Set(indices);
    }

    function formatPrice(value) {
      return Math.abs(value) >= 1000 ? value.toFixed(0) : value.toFixed(2);
    }

    function formatTimeLabel(value) {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "";
      return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }

    window.addEventListener("resize", () => {
      for (const nodes of panels.values()) {
        drawChart(nodes.canvas, nodes.current.lastChartPayload || { bars: [] });
      }
    });
    setInterval(pollOnce, POLL_INTERVAL_MS);
    pollOnce();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flask-served regime monitor dashboard")
    parser.add_argument("--config", default="~/.config/regime-monitor/config.json")
    parser.add_argument("--api-key-env", help="Environment variable containing the Databento API key.")
    parser.add_argument("--dataset", help="Databento dataset.")
    parser.add_argument("--schema", help="Databento schema.")
    parser.add_argument("--symbols", help="Comma-separated Databento symbols.")
    parser.add_argument("--stype-in", help="Databento input symbol type.")
    parser.add_argument("--reconnect-interval", type=float, help="Databento reconnect interval in seconds.")
    parser.add_argument("--chart-bar-limit", type=int, help="Maximum five-minute candles to display.")
    parser.add_argument("--shared-ohlcv-db-path", type=Path, help="Bounded local OHLCV SQLite source for charts.")
    parser.add_argument("--state-dir", type=Path, help="Directory for bounded persisted runtime state.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    return parser.parse_args()


def _symbols_from_config(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        symbols = [part.strip() for part in value.split(",")]
    elif isinstance(value, list):
        symbols = [str(part).strip() for part in value]
    else:
        symbols = []
    return tuple(symbol for symbol in symbols if symbol) or tuple(config.symbol for config in DEFAULT_INSTRUMENTS)


def _instruments_from_config(value: object) -> tuple[InstrumentConfig, ...]:
    if not isinstance(value, list):
        return DEFAULT_INSTRUMENTS
    instruments: list[InstrumentConfig] = []
    for raw in value:
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("key") or "").strip().upper()
        name = str(raw.get("name") or key).strip()
        symbol = str(raw.get("symbol") or "").strip()
        if key and name and symbol:
            instruments.append(InstrumentConfig(key=key, name=name, symbol=symbol))
    return tuple(instruments) or DEFAULT_INSTRUMENTS


def resolve_state_dir(*, config: dict[str, object], state_dir_override: object = None) -> Path:
    state_dir_env = str(config.get("state_dir_env") or "REGIME_MONITOR_STATE_DIR").strip()
    state_dir_value = state_dir_override or os.environ.get(state_dir_env) or config.get("state_dir")
    return Path(str(state_dir_value or DEFAULT_STATE_DIR)).expanduser()


def resolve_shared_ohlcv_db_path(
    *,
    config: dict[str, object],
    shared_ohlcv_db_path_override: object = None,
) -> Path | None:
    if config.get("shared_ohlcv_db_enabled") is False:
        return None
    env_name = str(config.get("shared_ohlcv_db_path_env") or "REGIME_MONITOR_SHARED_OHLCV_DB_PATH").strip()
    value = shared_ohlcv_db_path_override or os.environ.get(env_name) or config.get("shared_ohlcv_db_path")
    path = Path(str(value or DEFAULT_SHARED_OHLCV_DB_PATH)).expanduser()
    return path


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = parse_args()
    config = load_config(Path(args.config).expanduser())
    api_key_env = str(args.api_key_env or config.get("api_key_env") or "DATABENTO_API_KEY").strip()
    api_key = os.environ.get(api_key_env) or str(config.get("api_key") or "").strip() or None
    dataset = str(args.dataset or config.get("dataset") or "GLBX.MDP3").strip()
    schema = str(args.schema or config.get("schema") or "trades").strip()
    instruments = _instruments_from_config(config.get("instruments"))
    symbol_value = args.symbols if args.symbols is not None else config.get("symbols")
    symbols = _symbols_from_config(symbol_value) if symbol_value is not None else tuple(item.symbol for item in instruments)
    stype_in = str(args.stype_in or config.get("stype_in") or "continuous").strip()
    reconnect_interval = float(args.reconnect_interval or config.get("reconnect_interval", 5.0))
    chart_bar_limit = int(args.chart_bar_limit or config.get("chart_bar_limit", DEFAULT_CHART_BAR_LIMIT))
    state_dir = resolve_state_dir(config=config, state_dir_override=args.state_dir)
    shared_ohlcv_db_path = resolve_shared_ohlcv_db_path(
        config=config,
        shared_ohlcv_db_path_override=args.shared_ohlcv_db_path,
    )
    host = str(config.get("host") or args.host)
    port = int(config.get("port") or args.port)
    state = MultiInstrumentMonitorState(
        instruments=instruments,
        state_dir=state_dir,
        bar_limit=chart_bar_limit,
        shared_ohlcv_db_path=shared_ohlcv_db_path,
    )
    stop_event = threading.Event()
    worker = threading.Thread(
        target=run_databento_feed,
        kwargs={
            "config": DatabentoFeedConfig(
                api_key=api_key,
                dataset=dataset,
                schema=schema,
                symbols=symbols,
                stype_in=stype_in,
                reconnect_interval=reconnect_interval,
            ),
            "state": state,
            "stop": stop_event,
        },
        daemon=True,
    )
    worker.start()
    app = create_app(state=state)
    try:
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    finally:
        stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
