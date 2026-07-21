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
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify, request


LOGGER = logging.getLogger("regime_monitor")
VALID_REGIMES = {"LONG", "SHORT", "NO_TRADE", "UNAVAILABLE", "STALE", "CALCULATION_ERROR"}
REGIME_COLORS = {
    "LONG": "#00e676",
    "SHORT": "#ff3333",
    "NO_TRADE": "#9e9e9e",
    "UNAVAILABLE": "#ffcc33",
    "STALE": "#ffcc33",
    "CALCULATION_ERROR": "#ffcc33",
    "NO DATA": "#ffcc33",
}
EASTERN_TZ = ZoneInfo("America/New_York")
DEFAULT_CHART_BAR_LIMIT = 72
DEFAULT_REGIME_PRICE_WINDOW = 20
DEFAULT_SHARED_CHART_STALE_AFTER_SECONDS = 900.0
DEFAULT_STATE_DIR = Path("/var/lib/regime-monitor")
DEFAULT_STATE_FILE_NAME = "candle_state.json"
MULTI_CANDLE_STATE_SCHEMA_VERSION = "regime_monitor_multi_candle_state_v2"
ROUTING_RECORD_DIAGNOSTIC_LIMIT = 20
CLIENT_DIAGNOSTIC_LIMIT = 300
DEFAULT_SHARED_OHLCV_DB_PATH = Path("var") / "track_b_shared_live_ohlcv.sqlite3"
DEFAULT_TRACK_B_MONITOR_INSTRUMENT_KEYS = ("MNQ", "MES", "MGC")
MONITOR_ONLY_TRACK_B_INSTRUMENT_KEYS = ("MBT",)
DEFAULT_MONITOR_INSTRUMENT_KEYS = DEFAULT_TRACK_B_MONITOR_INSTRUMENT_KEYS + MONITOR_ONLY_TRACK_B_INSTRUMENT_KEYS
DIRECTIONAL_AGREEMENT_BANDS: tuple[tuple[int, int, str], ...] = (
    (0, 24, "WEAK"),
    (25, 44, "DEVELOPING"),
    (45, 64, "MODERATE"),
    (65, 79, "STRONG"),
    (80, 100, "VERY STRONG"),
)
CLIENT_DIAGNOSTICS: deque[dict[str, Any]] = deque(maxlen=CLIENT_DIAGNOSTIC_LIMIT)


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
class RegimeCalculation:
    decision: str
    confidence: float | None
    reason: str
    calculated_at: str
    source_bar_timestamp: str | None
    stale_reason: str | None = None
    error_reason: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["regime"] = self.decision
        return payload


@dataclass(frozen=True)
class DirectionalAgreementComponent:
    name: str
    category: str
    points: float
    max_points: float
    reason: str
    value: float | int | str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "points": round(self.points, 4),
            "max_points": round(self.max_points, 4),
            "reason": self.reason,
            "value": self.value,
        }


@dataclass(frozen=True)
class DirectionalAgreementScore:
    score: int | None
    band: str
    points_awarded: float
    points_available: float
    scoring_rule: str
    components: tuple[DirectionalAgreementComponent, ...]
    unavailable_reason: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "regime_monitor_directional_agreement_score_v1",
            "score": self.score,
            "band": self.band,
            "points_awarded": round(self.points_awarded, 4),
            "points_available": round(self.points_available, 4),
            "scoring_rule": self.scoring_rule,
            "components": [component.to_payload() for component in self.components],
            "unavailable_reason": self.unavailable_reason,
        }


@dataclass(frozen=True)
class TradeQualityScore:
    score: int | None
    components: tuple[DirectionalAgreementComponent, ...]
    freshness_state: str
    unavailable_reason: str | None = None

    def to_payload(self) -> dict[str, Any]:
        points_awarded = sum(component.points for component in self.components)
        points_available = sum(component.max_points for component in self.components)
        return {
            "schema_version": "regime_monitor_trade_quality_score_v1",
            "score": self.score,
            "points_awarded": round(points_awarded, 4),
            "points_available": round(points_available, 4),
            "scoring_rule": "confidence_adx_vwap_distance_freshness_v1",
            "freshness_state": self.freshness_state,
            "components": [component.to_payload() for component in self.components],
            "unavailable_reason": self.unavailable_reason,
        }


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

        regime, confidence, _reason = calculate_price_window_regime(prices, latest_price=latest_price)
        return RegimeSnapshot(
            regime=regime,
            confidence=confidence,
            timestamp=timestamp,
            connection_status=connection_status,
            error=error,
            received_at=utc_now_text(),
        )


def calculate_price_window_regime(
    prices: list[float],
    *,
    latest_price: float | None = None,
) -> tuple[str, float, str]:
    clean_prices = [float(price) for price in prices if _float_or_none(price) is not None]
    if not clean_prices and latest_price is None:
        return "UNAVAILABLE", 0.0, "PRICE_WINDOW_EMPTY"
    latest = float(latest_price if latest_price is not None else clean_prices[-1])
    window = clean_prices or [latest]
    average_price = sum(window) / len(window)
    if average_price == 0:
        return "UNAVAILABLE", 0.0, "PRICE_WINDOW_AVERAGE_ZERO"
    if latest > average_price:
        decision = "LONG"
    elif latest < average_price:
        decision = "SHORT"
    else:
        decision = "NO_TRADE"
    confidence = round(abs(latest - average_price) / average_price, 4)
    return decision, confidence, f"latest_close_vs_{len(window)}_value_average"


def calculate_regime_from_chart_payload(
    chart: dict[str, Any] | None,
    *,
    now: datetime | None = None,
    price_window: int = DEFAULT_REGIME_PRICE_WINDOW,
    stale_after_seconds: float = DEFAULT_SHARED_CHART_STALE_AFTER_SECONDS,
) -> RegimeCalculation:
    calculated_at_dt = now or datetime.now(timezone.utc)
    calculated_at = calculated_at_dt.isoformat()
    if not isinstance(chart, dict):
        return RegimeCalculation(
            decision="UNAVAILABLE",
            confidence=None,
            reason="shared_chart_unavailable",
            calculated_at=calculated_at,
            source_bar_timestamp=None,
            error_reason="MISSING_SHARED_CHART",
        )
    bars = chart.get("bars")
    if not isinstance(bars, list) or not bars:
        return RegimeCalculation(
            decision="UNAVAILABLE",
            confidence=None,
            reason="shared_chart_has_no_bars",
            calculated_at=calculated_at,
            source_bar_timestamp=None,
            error_reason="NO_SHARED_CHART_BARS",
        )
    source_bar_timestamp = str(chart.get("latest_bar_ts") or bars[-1].get("time") or "")
    latest_bar_dt = _parse_datetime_or_none(source_bar_timestamp)
    if latest_bar_dt is None:
        return RegimeCalculation(
            decision="CALCULATION_ERROR",
            confidence=None,
            reason="shared_chart_latest_timestamp_invalid",
            calculated_at=calculated_at,
            source_bar_timestamp=source_bar_timestamp or None,
            error_reason="INVALID_SOURCE_BAR_TIMESTAMP",
        )
    age_seconds = max(0.0, (calculated_at_dt.astimezone(timezone.utc) - latest_bar_dt).total_seconds())
    if age_seconds > stale_after_seconds:
        return RegimeCalculation(
            decision="STALE",
            confidence=None,
            reason="shared_chart_latest_bar_stale",
            calculated_at=calculated_at,
            source_bar_timestamp=latest_bar_dt.isoformat(),
            stale_reason=f"latest_bar_age_seconds={round(age_seconds, 3)} exceeds {stale_after_seconds}",
        )
    prices: list[float] = []
    try:
        for bar in bars[-max(1, int(price_window)) :]:
            close = _float_or_none(bar.get("close") if isinstance(bar, dict) else None)
            if close is None:
                raise ValueError("bar close is missing or invalid")
            prices.append(close)
        decision, confidence, reason = calculate_price_window_regime(prices)
    except Exception as exc:  # noqa: BLE001 - expose monitor calculation failure without killing /data.
        return RegimeCalculation(
            decision="CALCULATION_ERROR",
            confidence=None,
            reason="shared_chart_calculation_failed",
            calculated_at=calculated_at,
            source_bar_timestamp=latest_bar_dt.isoformat(),
            error_reason=str(exc),
        )
    return RegimeCalculation(
        decision=decision,
        confidence=confidence,
        reason=reason,
        calculated_at=calculated_at,
        source_bar_timestamp=latest_bar_dt.isoformat(),
    )


def calculate_directional_agreement_score(
    chart: dict[str, Any] | None,
    *,
    trend: str,
) -> DirectionalAgreementScore:
    scoring_rule = "weighted_indicator_agreement_v1_renormalized_available_components"
    direction = 1 if trend == "LONG" else -1 if trend == "SHORT" else 0
    if direction == 0:
        return DirectionalAgreementScore(
            score=None,
            band="UNAVAILABLE",
            points_awarded=0.0,
            points_available=0.0,
            scoring_rule=scoring_rule,
            components=(),
            unavailable_reason="directional_agreement_requires_LONG_or_SHORT_trend",
        )
    bars = _clean_chart_bars(chart)
    if not bars:
        return DirectionalAgreementScore(
            score=None,
            band="UNAVAILABLE",
            points_awarded=0.0,
            points_available=0.0,
            scoring_rule=scoring_rule,
            components=(),
            unavailable_reason="chart_bars_unavailable",
        )

    closes = [bar["close"] for bar in bars]
    latest = bars[-1]
    ma20_series = _moving_average_series(closes, 20)
    vwap_series_values = _chart_vwap_series(bars)
    rsi_series_values = _rsi_series(closes, 14)
    atr_series_values = _atr_series(bars, 14)
    adx_series_values = _adx_series(bars, 14)
    ma20 = _last_not_none(ma20_series)
    ma20_previous = _previous_not_none(ma20_series)
    vwap = _last_not_none(vwap_series_values)
    vwap_previous = _previous_not_none(vwap_series_values)
    rsi = _last_not_none(rsi_series_values)
    rsi_previous = _previous_not_none(rsi_series_values)
    atr = _last_not_none(atr_series_values)
    adx = _last_not_none(adx_series_values)
    adx_previous = _previous_not_none(adx_series_values)
    momentum_reference = bars[-11]["close"] if len(bars) >= 11 else None
    momentum = (latest["close"] - momentum_reference) / momentum_reference if momentum_reference else None

    components: list[DirectionalAgreementComponent] = []

    _append_signed_component(
        components,
        name="price_vs_vwap",
        category="trend_alignment",
        max_points=10.0,
        signed_value=None if vwap is None else latest["close"] - vwap,
        direction=direction,
        value=vwap,
        strength=None if atr is None or atr <= 0 or vwap is None else abs(latest["close"] - vwap) / atr / 0.25,
    )
    _append_signed_component(
        components,
        name="price_vs_ma20",
        category="trend_alignment",
        max_points=10.0,
        signed_value=None if ma20 is None else latest["close"] - ma20,
        direction=direction,
        value=ma20,
        strength=None if atr is None or atr <= 0 or ma20 is None else abs(latest["close"] - ma20) / atr / 0.25,
    )
    _append_signed_component(
        components,
        name="ma20_slope",
        category="trend_alignment",
        max_points=7.5,
        signed_value=None if ma20 is None or ma20_previous is None else ma20 - ma20_previous,
        direction=direction,
        value=None if ma20 is None or ma20_previous is None else ma20 - ma20_previous,
        strength=None if atr is None or atr <= 0 or ma20 is None or ma20_previous is None else abs(ma20 - ma20_previous) / atr / 0.10,
    )
    _append_signed_component(
        components,
        name="vwap_slope",
        category="trend_alignment",
        max_points=7.5,
        signed_value=None if vwap is None or vwap_previous is None else vwap - vwap_previous,
        direction=direction,
        value=None if vwap is None or vwap_previous is None else vwap - vwap_previous,
        strength=None if atr is None or atr <= 0 or vwap is None or vwap_previous is None else abs(vwap - vwap_previous) / atr / 0.10,
    )
    _append_rsi_component(
        components,
        rsi=rsi,
        rsi_previous=rsi_previous,
        direction=direction,
    )
    _append_momentum_component(
        components,
        momentum=momentum,
        atr=atr,
        close=latest["close"],
        direction=direction,
    )
    _append_adx_level_component(components, adx=adx)
    _append_adx_direction_component(components, adx=adx, adx_previous=adx_previous)
    _append_separation_component(
        components,
        close=latest["close"],
        vwap=vwap,
        ma20=ma20,
        atr=atr,
        direction=direction,
    )
    _append_persistence_component(
        components,
        bars=bars,
        ma20_series=ma20_series,
        vwap_series_values=vwap_series_values,
        atr=atr,
        direction=direction,
    )

    points_available = sum(component.max_points for component in components)
    points_awarded = sum(component.points for component in components)
    if points_available <= 0:
        return DirectionalAgreementScore(
            score=None,
            band="UNAVAILABLE",
            points_awarded=0.0,
            points_available=0.0,
            scoring_rule=scoring_rule,
            components=tuple(components),
            unavailable_reason="no_scored_components_available",
        )
    score = max(0, min(100, int(round(points_awarded / points_available * 100))))
    return DirectionalAgreementScore(
        score=score,
        band=directional_agreement_band(score),
        points_awarded=points_awarded,
        points_available=points_available,
        scoring_rule=scoring_rule,
        components=tuple(components),
    )


def directional_agreement_band(score: int | None) -> str:
    if score is None:
        return "UNAVAILABLE"
    bounded = max(0, min(100, int(score)))
    for lower, upper, label in DIRECTIONAL_AGREEMENT_BANDS:
        if lower <= bounded <= upper:
            return label
    return "UNAVAILABLE"


def calculate_trade_quality_score(
    chart: dict[str, Any] | None,
    *,
    directional_agreement: DirectionalAgreementScore,
    now: datetime | None = None,
) -> TradeQualityScore:
    if directional_agreement.score is None:
        return TradeQualityScore(
            score=None,
            components=(),
            freshness_state=_chart_freshness_state(chart, now=now),
            unavailable_reason="directional_agreement_unavailable",
        )
    bars = _clean_chart_bars(chart)
    if not bars:
        return TradeQualityScore(
            score=None,
            components=(),
            freshness_state=_chart_freshness_state(chart, now=now),
            unavailable_reason="chart_bars_unavailable",
        )

    atr = _last_not_none(_atr_series(bars, 14))
    adx = _last_not_none(_adx_series(bars, 14))
    vwap = _last_not_none(_chart_vwap_series(bars))
    latest_close = bars[-1]["close"]
    freshness_state = _chart_freshness_state(chart, now=now)
    freshness_points = {"fresh": 5.0, "degraded": 2.0, "stale": 0.0}.get(freshness_state, 0.0)
    confidence_points = directional_agreement.score / 100.0 * 60.0
    adx_points = 0.0 if adx is None else _scale_positive((adx - 15.0) / 25.0) * 20.0
    vwap_distance_atr = None
    if atr is not None and atr > 0 and vwap is not None:
        vwap_distance_atr = abs(latest_close - vwap) / atr
    movement_points = 0.0 if vwap_distance_atr is None else _scale_positive(vwap_distance_atr / 0.50) * 15.0
    components = (
        DirectionalAgreementComponent(
            name="directional_agreement",
            category="trade_quality",
            points=confidence_points,
            max_points=60.0,
            reason="confidence_score_normalized_to_60",
            value=directional_agreement.score,
        ),
        DirectionalAgreementComponent(
            name="adx_strength",
            category="trade_quality",
            points=adx_points,
            max_points=20.0,
            reason="adx_scaled_15_to_40",
            value=None if adx is None else round(adx, 4),
        ),
        DirectionalAgreementComponent(
            name="vwap_distance_atr",
            category="trade_quality",
            points=movement_points,
            max_points=15.0,
            reason="absolute_vwap_distance_scaled_to_half_atr",
            value=None if vwap_distance_atr is None else round(vwap_distance_atr, 6),
        ),
        DirectionalAgreementComponent(
            name="data_freshness",
            category="trade_quality",
            points=freshness_points,
            max_points=5.0,
            reason=freshness_state,
            value=freshness_state,
        ),
    )
    score = max(0, min(100, int(round(sum(component.points for component in components)))))
    return TradeQualityScore(score=score, components=components, freshness_state=freshness_state)


def _chart_freshness_state(chart: dict[str, Any] | None, *, now: datetime | None = None) -> str:
    if not isinstance(chart, dict):
        return "stale"
    source_time = _parse_datetime_or_none(chart.get("generated_at") or chart.get("received_at"))
    if source_time is None:
        return "stale"
    reference = _ensure_utc(now or datetime.now(timezone.utc))
    age_seconds = max(0.0, (reference - source_time).total_seconds())
    if age_seconds <= 2.0:
        return "fresh"
    if age_seconds <= 10.0:
        return "degraded"
    return "stale"


def _clean_chart_bars(chart: dict[str, Any] | None) -> list[dict[str, float]]:
    if not isinstance(chart, dict):
        return []
    bars = chart.get("bars")
    if not isinstance(bars, list):
        return []
    clean: list[dict[str, float]] = []
    for bar in bars:
        if not isinstance(bar, dict):
            continue
        open_ = _float_or_none(bar.get("open"))
        high = _float_or_none(bar.get("high"))
        low = _float_or_none(bar.get("low"))
        close = _float_or_none(bar.get("close"))
        volume = _float_or_none(bar.get("volume"))
        if open_ is None or high is None or low is None or close is None:
            continue
        clean.append(
            {
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 0.0 if volume is None else max(0.0, volume),
            }
        )
    return clean


def _append_signed_component(
    components: list[DirectionalAgreementComponent],
    *,
    name: str,
    category: str,
    max_points: float,
    signed_value: float | None,
    direction: int,
    value: float | int | str | None,
    strength: float | None = None,
) -> None:
    if signed_value is None:
        return
    alignment = _sign(signed_value) * direction
    strength_scale = 1.0 if strength is None else _scale_positive(strength)
    points = max_points * strength_scale if alignment > 0 else 0.0
    if alignment > 0:
        reason = "aligned_scaled_by_magnitude" if strength is not None else "aligned"
    else:
        reason = "contradictory" if alignment < 0 else "neutral"
    components.append(
        DirectionalAgreementComponent(
            name=name,
            category=category,
            points=points,
            max_points=max_points,
            reason=reason,
            value=None if value is None else round(float(value), 6),
        )
    )


def _append_rsi_component(
    components: list[DirectionalAgreementComponent],
    *,
    rsi: float | None,
    rsi_previous: float | None,
    direction: int,
) -> None:
    if rsi is None:
        return
    level_points = _scale_positive((rsi - 50.0) / 20.0) * 8.0 if direction > 0 else _scale_positive((50.0 - rsi) / 20.0) * 8.0
    if rsi_previous is None:
        direction_points = 0.0
    else:
        rsi_change = rsi - rsi_previous
        already_confirmed = (direction > 0 and rsi >= 70.0) or (direction < 0 and rsi <= 30.0)
        direction_points = 4.5 if rsi_change * direction > 0 or (rsi_change == 0 and already_confirmed) else 0.0
    components.append(
        DirectionalAgreementComponent(
            name="rsi_level_direction",
            category="momentum_alignment",
            points=level_points + direction_points,
            max_points=12.5,
            reason="rsi_level_and_direction_alignment",
            value=round(rsi, 4),
        )
    )


def _append_momentum_component(
    components: list[DirectionalAgreementComponent],
    *,
    momentum: float | None,
    atr: float | None,
    close: float,
    direction: int,
) -> None:
    if momentum is None:
        return
    magnitude = abs(momentum)
    if atr is not None and close:
        magnitude = abs(momentum) / max(atr / abs(close), 0.000001)
    aligned = momentum * direction > 0
    points = 0.0 if not aligned else min(12.5, 12.5 * magnitude)
    components.append(
        DirectionalAgreementComponent(
            name="mom_sign_magnitude",
            category="momentum_alignment",
            points=points,
            max_points=12.5,
            reason="aligned_normalized_magnitude" if aligned else "contradictory",
            value=round(momentum, 6),
        )
    )


def _append_adx_level_component(components: list[DirectionalAgreementComponent], *, adx: float | None) -> None:
    if adx is None:
        return
    points = _scale_positive((adx - 10.0) / 20.0) * 17.0
    components.append(
        DirectionalAgreementComponent(
            name="adx_level",
            category="trend_strength",
            points=points,
            max_points=17.0,
            reason="trend_strength_level",
            value=round(adx, 4),
        )
    )


def _append_adx_direction_component(
    components: list[DirectionalAgreementComponent],
    *,
    adx: float | None,
    adx_previous: float | None,
) -> None:
    if adx is None or adx_previous is None:
        return
    change = adx - adx_previous
    points = 8.0 if change > 0 or (change == 0 and adx >= 30.0) else 0.0
    components.append(
        DirectionalAgreementComponent(
            name="adx_direction",
            category="trend_strength",
            points=points,
            max_points=8.0,
            reason="rising_or_already_strong" if points else "falling_or_weak",
            value=round(change, 6),
        )
    )


def _append_separation_component(
    components: list[DirectionalAgreementComponent],
    *,
    close: float,
    vwap: float | None,
    ma20: float | None,
    atr: float | None,
    direction: int,
) -> None:
    if atr is None or atr <= 0:
        return
    distances = []
    for reference in (vwap, ma20):
        if reference is not None and (close - reference) * direction > 0:
            distances.append(abs(close - reference) / atr)
    normalized = min(1.0, sum(distances) / 2.0) if distances else 0.0
    components.append(
        DirectionalAgreementComponent(
            name="atr_normalized_separation",
            category="persistence_separation",
            points=normalized * 8.0,
            max_points=8.0,
            reason="distance_from_vwap_ma20_normalized_by_atr",
            value=round(normalized, 6),
        )
    )


def _append_persistence_component(
    components: list[DirectionalAgreementComponent],
    *,
    bars: list[dict[str, float]],
    ma20_series: list[float | None],
    vwap_series_values: list[float | None],
    atr: float | None,
    direction: int,
) -> None:
    checks = 0
    aligned = 0
    strengths: list[float] = []
    start = max(0, len(bars) - 5)
    for index in range(start, len(bars)):
        references = [ma20_series[index], vwap_series_values[index]]
        available = [reference for reference in references if reference is not None]
        if not available:
            continue
        checks += 1
        if all((bars[index]["close"] - reference) * direction > 0 for reference in available):
            aligned += 1
            if atr is not None and atr > 0:
                strengths.append(min(abs(bars[index]["close"] - reference) / atr for reference in available) / 0.25)
    if checks == 0:
        return
    ratio = aligned / checks
    strength_scale = 1.0 if not strengths else _scale_positive(sum(strengths) / len(strengths))
    components.append(
        DirectionalAgreementComponent(
            name="recent_side_persistence",
            category="persistence_separation",
            points=ratio * strength_scale * 7.0,
            max_points=7.0,
            reason="recent_bars_remaining_on_trend_side_scaled_by_separation",
            value=round(ratio, 4),
        )
    )


def _moving_average_series(values: list[float], period: int) -> list[float | None]:
    series: list[float | None] = []
    for index in range(len(values)):
        if index + 1 < period:
            series.append(None)
            continue
        window = values[index + 1 - period : index + 1]
        series.append(sum(window) / period)
    return series


def _chart_vwap_series(bars: list[dict[str, float]]) -> list[float | None]:
    value_volume = 0.0
    volume = 0.0
    series: list[float | None] = []
    for bar in bars:
        bar_volume = max(0.0, bar["volume"])
        if bar_volume <= 0:
            series.append(value_volume / volume if volume > 0 else None)
            continue
        typical = (bar["high"] + bar["low"] + bar["close"]) / 3
        value_volume += typical * bar_volume
        volume += bar_volume
        series.append(value_volume / volume)
    return series


def _rsi_series(values: list[float], period: int) -> list[float | None]:
    series: list[float | None] = [None] * len(values)
    for index in range(period, len(values)):
        gains = 0.0
        losses = 0.0
        for inner in range(index + 1 - period, index + 1):
            change = values[inner] - values[inner - 1]
            gains += max(change, 0.0)
            losses += max(-change, 0.0)
        avg_gain = gains / period
        avg_loss = losses / period
        series[index] = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))
    return series


def _atr_series(bars: list[dict[str, float]], period: int) -> list[float | None]:
    series: list[float | None] = [None] * len(bars)
    ranges: list[float] = []
    for index in range(1, len(bars)):
        current = bars[index]
        prior = bars[index - 1]
        ranges.append(
            max(
                current["high"] - current["low"],
                abs(current["high"] - prior["close"]),
                abs(current["low"] - prior["close"]),
            )
        )
        if len(ranges) >= period:
            series[index] = sum(ranges[-period:]) / period
    return series


def _adx_series(bars: list[dict[str, float]], period: int) -> list[float | None]:
    series: list[float | None] = [None] * len(bars)
    ranges: list[float] = []
    plus_moves: list[float] = []
    minus_moves: list[float] = []
    for index in range(1, len(bars)):
        current = bars[index]
        prior = bars[index - 1]
        up_move = current["high"] - prior["high"]
        down_move = prior["low"] - current["low"]
        ranges.append(
            max(
                current["high"] - current["low"],
                abs(current["high"] - prior["close"]),
                abs(current["low"] - prior["close"]),
            )
        )
        plus_moves.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_moves.append(down_move if down_move > up_move and down_move > 0 else 0.0)
    if len(ranges) < period * 2:
        return series
    smoothed_range = sum(ranges[:period])
    smoothed_plus = sum(plus_moves[:period])
    smoothed_minus = sum(minus_moves[:period])
    dx_values: list[tuple[int, float]] = []
    for index in range(period, len(ranges)):
        smoothed_range = smoothed_range - (smoothed_range / period) + ranges[index]
        smoothed_plus = smoothed_plus - (smoothed_plus / period) + plus_moves[index]
        smoothed_minus = smoothed_minus - (smoothed_minus / period) + minus_moves[index]
        if smoothed_range == 0:
            dx_values.append((index + 1, 0.0))
            continue
        plus_di = 100.0 * (smoothed_plus / smoothed_range)
        minus_di = 100.0 * (smoothed_minus / smoothed_range)
        di_sum = plus_di + minus_di
        dx_values.append((index + 1, 0.0 if di_sum == 0 else 100.0 * abs(plus_di - minus_di) / di_sum))
    if len(dx_values) < period:
        return series
    adx = sum(value for _, value in dx_values[:period]) / period
    first_bar_index = dx_values[period - 1][0]
    if first_bar_index < len(series):
        series[first_bar_index] = adx
    for bar_index, dx in dx_values[period:]:
        adx = ((adx * (period - 1)) + dx) / period
        if bar_index < len(series):
            series[bar_index] = adx
    return series


def _last_not_none(values: list[float | None]) -> float | None:
    for value in reversed(values):
        if value is not None:
            return value
    return None


def _previous_not_none(values: list[float | None]) -> float | None:
    seen_latest = False
    for value in reversed(values):
        if value is None:
            continue
        if not seen_latest:
            seen_latest = True
            continue
        return value
    return None


def _sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _scale_positive(value: float) -> float:
    return max(0.0, min(1.0, value))


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
        now = datetime.now(timezone.utc)
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
            if self.shared_ohlcv_db_path is not None:
                calculation = calculate_regime_from_chart_payload(shared_chart)
                item["regime"] = calculation.decision
                item["confidence"] = calculation.confidence
                item["regime_calculation"] = calculation.to_payload()
                item["regime_reason"] = calculation.reason
                item["regime_calculated_at"] = calculation.calculated_at
                item["regime_source_bar_timestamp"] = calculation.source_bar_timestamp
                item["regime_stale_reason"] = calculation.stale_reason
                item["regime_error_reason"] = calculation.error_reason
            agreement = calculate_directional_agreement_score(item.get("chart"), trend=str(item.get("regime") or ""))
            item["directional_agreement_score"] = agreement.to_payload()
            item["trade_quality_score"] = calculate_trade_quality_score(
                item.get("chart"),
                directional_agreement=agreement,
                now=now,
            ).to_payload()
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
        response = Response(DASHBOARD_HTML, mimetype="text/html")
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

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
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response

    @app.post("/client-diagnostics")
    def client_diagnostics_post() -> tuple[str, int]:
        raw = request.get_data(cache=False, as_text=True, parse_form_data=False)
        if len(raw) > 4096:
            raw = raw[:4096]
        try:
            payload = json.loads(raw) if raw else {}
            if not isinstance(payload, dict):
                payload = {"value": payload}
        except json.JSONDecodeError:
            payload = {"malformed": raw[:512]}
        CLIENT_DIAGNOSTICS.append(
            {
                "received_at": utc_now_text(),
                "remote_addr": request.remote_addr,
                "payload": payload,
            }
        )
        return "", 204

    @app.get("/client-diagnostics")
    def client_diagnostics_get() -> Response:
        response = jsonify({"diagnostics": list(CLIENT_DIAGNOSTICS)})
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
      --panel: #020809;
      --panel-border: #183033;
      --grid: rgba(83, 113, 118, 0.18);
      --text: #f7f7f7;
      --muted: #a5abad;
      --dim: #697174;
      --cyan: #00e6c3;
      --long: #38d430;
      --short: #ff3434;
      --no-trade: #ffb000;
      --no-data: #ffb000;
      --warn: #ffb000;
      --orange: #ff8a20;
      --lime: #9af36c;
      --blue: #2f83ff;
      --magenta: #d948ff;
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
      height: calc(100vh - 32px);
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
      grid-template-rows: auto minmax(0, 1fr) auto;
      gap: 8px;
      padding: 10px 12px 8px;
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 4px;
      box-shadow: inset 0 0 18px rgba(0, 230, 195, 0.04);
      overflow: hidden;
    }
    .top {
      display: grid;
      grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.7fr);
      gap: 14px;
      min-width: 0;
      padding-bottom: 5px;
      border-bottom: 1px solid rgba(125, 153, 158, 0.18);
    }
    .identity {
      min-width: 0;
      display: grid;
      gap: 5px;
      border-right: 1px solid rgba(125, 153, 158, 0.22);
      padding-right: 14px;
    }
    .identity-head {
      min-width: 0;
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      align-items: start;
      gap: 8px;
    }
    .name {
      margin: 0;
      font-size: clamp(1.55rem, 2.55vw, 3.1rem);
      line-height: 1;
      font-weight: 820;
      letter-spacing: 0;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .rank-marker {
      align-self: start;
      padding: 4px 7px;
      border: 1px solid rgba(247, 247, 247, 0.24);
      border-radius: 3px;
      color: var(--text);
      background: rgba(247, 247, 247, 0.06);
      font-size: clamp(0.78rem, 1.05vw, 1.16rem);
      line-height: 1;
      font-weight: 860;
      font-variant-numeric: tabular-nums;
    }
    .rank-marker[data-rank="1"] {
      border-color: rgba(0, 230, 195, 0.65);
      color: var(--cyan);
      background: rgba(0, 230, 195, 0.10);
    }
    .symbol {
      color: var(--muted);
      font-size: clamp(0.92rem, 1.35vw, 1.45rem);
      line-height: 1;
      font-weight: 720;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .last-price {
      font-size: clamp(1.55rem, 2.55vw, 3.05rem);
      line-height: 0.95;
      font-weight: 780;
      color: var(--long);
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .last-price[data-tone="short"] { color: var(--short); }
    .last-price[data-tone="flat"] { color: var(--muted); }
    .change {
      color: var(--long);
      font-size: clamp(0.95rem, 1.45vw, 1.55rem);
      line-height: 1;
      font-weight: 760;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .change[data-tone="short"] { color: var(--short); }
    .change[data-tone="flat"] { color: var(--muted); }
    .quality {
      color: var(--muted);
      font-size: clamp(0.72rem, 1vw, 1.02rem);
      line-height: 1;
      font-weight: 800;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .quality[data-tone="weak"] { color: var(--dim); }
    .quality[data-tone="developing"] { color: var(--warn); }
    .quality[data-tone="moderate"] { color: var(--orange); }
    .quality[data-tone="strong"] { color: var(--lime); }
    .quality[data-tone="very-strong"] { color: var(--long); }
    .signal-table {
      min-width: 0;
      display: grid;
      grid-template-columns: minmax(7rem, 0.55fr) minmax(0, 1fr);
      align-content: start;
      color: var(--text);
    }
    .metric-label,
    .metric-value {
      min-width: 0;
      padding: 6px 0;
      border-bottom: 1px solid rgba(125, 153, 158, 0.13);
      font-size: clamp(0.82rem, 1.05vw, 1.18rem);
      line-height: 1;
      font-weight: 760;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .metric-label {
      color: var(--muted);
      text-transform: uppercase;
    }
    .metric-label.primary { color: var(--cyan); }
    .metric-value {
      color: var(--long);
      font-size: clamp(0.9rem, 1.22vw, 1.32rem);
      font-weight: 820;
      font-variant-numeric: tabular-nums;
    }
    .metric-value.trend {
      color: var(--text);
      font-weight: 760;
    }
    .metric-value.confidence {
      font-size: clamp(1.08rem, 1.55vw, 1.72rem);
      font-weight: 900;
    }
    .metric-value[data-tone="short"] { color: var(--short); }
    .metric-value[data-tone="flat"] { color: var(--warn); }
    .metric-value[data-tone="neutral"] { color: var(--text); }
    .metric-value[data-tone="weak"] { color: var(--dim); }
    .metric-value[data-tone="developing"] { color: var(--warn); }
    .metric-value[data-tone="moderate"] { color: var(--orange); }
    .metric-value[data-tone="strong"] { color: var(--lime); }
    .metric-value[data-tone="very-strong"] { color: var(--long); }
    .chart-wrap {
      min-width: 0;
      min-height: 0;
      position: relative;
      display: block;
      border-top: 1px solid rgba(125, 153, 158, 0.12);
      border-bottom: 1px solid rgba(125, 153, 158, 0.12);
      background: #010606;
    }
    .overlay {
      position: absolute;
      left: 14px;
      top: 12px;
      display: grid;
      gap: 4px;
      font-size: clamp(0.72rem, 1vw, 1.05rem);
      line-height: 1;
      font-weight: 650;
      pointer-events: none;
      z-index: 2;
    }
    .overlay .vwap { color: var(--magenta); }
    .overlay .ma { color: var(--blue); }
    .volume-label {
      position: absolute;
      left: 14px;
      bottom: 42px;
      color: var(--long);
      font-size: clamp(0.72rem, 0.95vw, 1rem);
      line-height: 1;
      font-weight: 700;
      pointer-events: none;
      z-index: 2;
    }
    .price-badge {
      position: absolute;
      right: 6px;
      padding: 4px 6px;
      border-radius: 2px;
      color: #fff;
      background: var(--long);
      font-size: clamp(0.72rem, 0.95vw, 1rem);
      line-height: 1;
      font-weight: 760;
      font-variant-numeric: tabular-nums;
      pointer-events: none;
      z-index: 3;
    }
    .price-badge[data-kind="vwap"] { background: #8f33d8; }
    .price-badge[data-tone="short"] { background: var(--short); }
    .price-badge.hidden { display: none; }
    .indicators {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      min-width: 0;
    }
    .indicator {
      min-width: 0;
      display: grid;
      gap: 4px;
      padding: 7px 8px;
      border: 1px solid rgba(125, 153, 158, 0.16);
      background: rgba(8, 18, 20, 0.68);
      text-align: center;
      overflow: hidden;
    }
    .indicator-label {
      color: var(--text);
      font-size: clamp(0.72rem, 1vw, 1rem);
      line-height: 1;
      white-space: nowrap;
    }
    .indicator-value {
      color: var(--warn);
      font-size: clamp(1rem, 1.72vw, 2rem);
      line-height: 1;
      font-weight: 820;
      font-variant-numeric: tabular-nums;
      white-space: nowrap;
    }
    .indicator-value[data-tone="long"] { color: var(--long); }
    .indicator-value[data-tone="short"] { color: var(--short); }
    .indicator-value[data-tone="orange"] { color: var(--orange); }
    .indicator-value[data-tone="yellow"] { color: var(--warn); }
    .indicator-value[data-tone="lime"] { color: var(--lime); }
    .indicator-value[data-tone="neutral"] { color: var(--muted); }
    .chart {
      display: block;
      width: 100%;
      height: 100%;
      min-height: 0;
      background: #010606;
    }
    .status {
      display: none;
    }
    .connection[data-state="CONNECTED"] { color: var(--long); }
    .connection[data-state="CONNECTING"] { color: #ddd; }
    .connection[data-state="DISCONNECTED"],
    .connection[data-state="NO_API_KEY"],
    .connection[data-state="DATABENTO_PACKAGE_MISSING"],
    .connection[data-state="SHARED_OHLCV_DISPLAY_ONLY"],
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
      color: var(--dim);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .hidden { visibility: hidden; }
    .dashboard-footer {
      height: 32px;
      padding: 0 16px;
      display: grid;
      grid-template-columns: auto auto auto auto minmax(0, 1fr) auto;
      gap: 22px;
      align-items: center;
      color: var(--muted);
      background: #010606;
      border-top: 1px solid rgba(125, 153, 158, 0.18);
      font-size: clamp(0.8rem, 1.15vw, 1.2rem);
      font-weight: 720;
      line-height: 1;
      white-space: nowrap;
    }
    .dashboard-footer .live { color: var(--long); }
    .dashboard-footer .stale { color: var(--warn); }
    .dashboard-footer .offline { color: var(--short); }
    .data-heartbeat {
      width: 0.62em;
      height: 0.62em;
      margin-right: 7px;
      display: inline-block;
      border-radius: 999px;
      background: var(--dim);
      vertical-align: 0.02em;
      opacity: 0.78;
      transform: scale(0.86);
      transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease, opacity 120ms ease;
    }
    .data-heartbeat.live { background: var(--long); color: var(--long); }
    .data-heartbeat.stale { background: var(--warn); color: var(--warn); }
    .data-heartbeat.offline { background: var(--short); color: var(--short); }
    .data-heartbeat.pulse {
      opacity: 1;
      transform: scale(1.34);
      box-shadow: 0 0 12px currentColor;
    }
  </style>
</head>
<body>
  <main id="dashboard" aria-live="polite" aria-atomic="false"></main>
  <template id="panel-template">
    <article class="panel">
      <header class="top">
        <div class="identity">
          <div class="identity-head">
            <h2 class="name"></h2>
            <div class="rank-marker">#--</div>
          </div>
          <div class="symbol"></div>
          <div class="last-price">--</div>
          <div class="change" data-tone="flat">--</div>
          <div class="quality">QUALITY --</div>
        </div>
        <div class="signal-table">
          <div class="metric-label primary">Trend</div>
          <div class="metric-value trend">--</div>
          <div class="metric-label">Confidence</div>
          <div class="metric-value confidence">--</div>
          <div class="metric-label">Regime</div>
          <div class="metric-value bias">--</div>
        </div>
      </header>
      <section class="chart-wrap">
        <div class="overlay">
          <div class="vwap">VWAP&nbsp;&nbsp;<span class="vwap-value">--</span></div>
          <div class="ma">MA20&nbsp;&nbsp;<span class="ma-value">--</span></div>
        </div>
        <canvas class="chart"></canvas>
        <div class="volume-label">Vol --</div>
        <div class="price-badge latest-badge">--</div>
        <div class="price-badge vwap-badge" data-kind="vwap">--</div>
      </section>
      <section class="indicators">
        <div class="indicator"><div class="indicator-label">RSI(14)</div><div class="indicator-value rsi">--</div></div>
        <div class="indicator"><div class="indicator-label">ADX(14)</div><div class="indicator-value adx">--</div></div>
        <div class="indicator"><div class="indicator-label">MOM(10)</div><div class="indicator-value mom">--</div></div>
        <div class="indicator"><div class="indicator-label">VWAP Δ</div><div class="indicator-value vwap-delta">--</div></div>
      </section>
      <footer class="status">
        <div class="connection" data-state="STARTING">STARTING</div>
        <div class="timestamp">--:--:--</div>
        <div class="route">id: -- source: --</div>
        <div class="error hidden"></div>
      </footer>
    </article>
  </template>
  <footer class="dashboard-footer">
    <span>Market: <span id="footer-market">--</span></span>
    <span>Session: <span id="footer-session">--</span></span>
    <span>TIME: <span id="footer-time">--:--:-- ET</span></span>
    <span>AGE: <span id="footer-age">--</span></span>
    <span>DATA: <span id="footer-data-heartbeat" class="data-heartbeat offline" title="Browser payload receipt/render heartbeat"></span><span id="footer-data">--</span></span>
    <span>ALL TIMES EASTERN</span>
  </footer>
  <script>
    const DATA_ENDPOINT = "/data";
    const POLL_INTERVAL_MS = 250;
    const DASHBOARD_TIME_ZONE = "America/New_York";
    const PANEL_ORDER = ["MNQ", "MES", "MGC", "MBT"];
    const colors = {
      LONG: "var(--long)",
      SHORT: "var(--short)",
      NO_TRADE: "var(--no-trade)",
      UNAVAILABLE: "var(--no-data)",
      STALE: "var(--no-data)",
      CALCULATION_ERROR: "var(--no-data)",
      "NO DATA": "var(--no-data)",
    };
    const chartAxis = {
      yLabelFont: "560 17px system-ui, sans-serif",
      xLabelFont: "560 15px system-ui, sans-serif",
      yLabelWidth: 76,
      leftPadding: 14,
      rightPadding: 108,
      topPadding: 34,
      bottomPadding: 62,
      yLabelGap: 10,
      xLabelBottomGap: 14,
      minXLabelGap: 110,
      priceRangePaddingRatio: 0.16,
      minPriceRangePixels: 20,
      fallbackLabelFont: "560 17px system-ui, sans-serif",
    };
    const dashboard = document.getElementById("dashboard");
    const template = document.getElementById("panel-template");
    const panels = new Map();
    const clientDiagnostics = {
      pollIntervalMs: POLL_INTERVAL_MS,
      pollSequence: 0,
      requestInFlight: false,
      skippedOverlapCount: 0,
      successfulFetchCount: 0,
      acceptedPayloadCount: 0,
      consecutiveFailureCount: 0,
      lastFetchStartedAt: null,
      lastSuccessfulFetchAt: null,
      lastAcceptedPayloadTimestamp: null,
      lastPayloadReceivedAt: null,
      lastClientRenderError: null,
      lastFetchError: null,
      lastStatus: "STARTING",
      lastPayload: null,
      forceNextFetchFailure: false,
      forceNextMalformedInstrument: null,
      history: [],
    };
    window.__REGIME_MONITOR_CLIENT_DIAGNOSTICS = clientDiagnostics;

    function ensurePanel(key, payload) {
      if (panels.has(key)) return panels.get(key);
      const fragment = template.content.cloneNode(true);
      const panel = fragment.querySelector(".panel");
      const nodes = {
        panel,
        name: fragment.querySelector(".name"),
        rankMarker: fragment.querySelector(".rank-marker"),
        symbol: fragment.querySelector(".symbol"),
        lastPrice: fragment.querySelector(".last-price"),
        change: fragment.querySelector(".change"),
        quality: fragment.querySelector(".quality"),
        trend: fragment.querySelector(".trend"),
        confidence: fragment.querySelector(".confidence"),
        bias: fragment.querySelector(".bias"),
        vwapValue: fragment.querySelector(".vwap-value"),
        maValue: fragment.querySelector(".ma-value"),
        volumeLabel: fragment.querySelector(".volume-label"),
        latestBadge: fragment.querySelector(".latest-badge"),
        vwapBadge: fragment.querySelector(".vwap-badge"),
        rsi: fragment.querySelector(".rsi"),
        adx: fragment.querySelector(".adx"),
        mom: fragment.querySelector(".mom"),
        vwapDelta: fragment.querySelector(".vwap-delta"),
        canvas: fragment.querySelector(".chart"),
        connection: fragment.querySelector(".connection"),
        timestamp: fragment.querySelector(".timestamp"),
        route: fragment.querySelector(".route"),
        error: fragment.querySelector(".error"),
        current: {},
      };
      panel.dataset.instrument = key;
      nodes.name.textContent = instrumentCode(payload.symbol || key, key);
      nodes.symbol.textContent = payload.name || payload.symbol || key;
      dashboard.appendChild(fragment);
      panels.set(key, nodes);
      return nodes;
    }

    function updateText(nodes, field, value) {
      const next = value == null || value === "" ? "" : String(value);
      if (nodes.current[field] === next) return;
      nodes.current[field] = next;
      if (!nodes[field]) {
        recordRenderError(field, new Error(`missing DOM node ${field}`));
        return;
      }
      nodes[field].textContent = next;
    }

    function updatePanel(key, payload, rankInfo) {
      if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
        payload = {
          name: key,
          symbol: key,
          regime: "UNAVAILABLE",
          connection_status: "MALFORMED_INSTRUMENT_PAYLOAD",
          error: "Instrument payload is not an object",
          chart: { bars: [] },
        };
      }
      const nodes = ensurePanel(key, payload);
      updateText(nodes, "name", instrumentCode(payload.symbol || key, key));
      updateText(nodes, "symbol", payload.name || payload.symbol || key);
      const quality = tradeQualityDisplay(payload.trade_quality_score);
      updateText(nodes, "rankMarker", rankInfo && rankInfo.rank ? `#${rankInfo.rank}` : "#--");
      updateText(nodes, "quality", quality);
      nodes.rankMarker.dataset.rank = rankInfo && rankInfo.rank ? String(rankInfo.rank) : "";
      nodes.quality.dataset.tone = tradeQualityTone(payload.trade_quality_score && payload.trade_quality_score.score);
      const calculation = payload.regime_calculation || null;
      const regime = payload.regime || (calculation && calculation.decision) || "UNAVAILABLE";
      const view = directionView(regime);
      updateText(nodes, "trend", view.trend);
      updateText(nodes, "bias", view.bias);
      nodes.trend.dataset.tone = "neutral";
      nodes.bias.dataset.tone = view.tone;
      const color = colors[regime] || colors["NO DATA"];
      if (nodes.current.regimeColor !== color) {
        nodes.current.regimeColor = color;
      }
      const confidence = directionalAgreementDisplay(payload.directional_agreement_score);
      updateText(nodes, "confidence", confidence);
      nodes.confidence.dataset.tone = confidenceTone(payload.directional_agreement_score && payload.directional_agreement_score.score);
      const metrics = chartMetrics(payload.chart || null);
      updateText(nodes, "lastPrice", metrics.lastPriceText);
      updateText(nodes, "change", metrics.changeText);
      nodes.change.dataset.tone = metrics.tone;
      nodes.lastPrice.dataset.tone = metrics.tone;
      updateText(nodes, "vwapValue", metrics.vwapText);
      updateText(nodes, "maValue", metrics.ma20Text);
      updateText(nodes, "volumeLabel", metrics.volumeText);
      updateText(nodes, "latestBadge", metrics.lastPriceText);
      updateText(nodes, "vwapBadge", metrics.vwapText);
      nodes.latestBadge.dataset.tone = metrics.tone;
      nodes.vwapBadge.classList.toggle("hidden", metrics.vwap == null);
      updateText(nodes, "rsi", metrics.rsiText);
      updateText(nodes, "adx", metrics.adxText);
      updateText(nodes, "mom", metrics.momText);
      updateText(nodes, "vwapDelta", metrics.vwapDeltaText);
      nodes.rsi.dataset.tone = metrics.rsiTone;
      nodes.adx.dataset.tone = metrics.adxTone;
      nodes.mom.dataset.tone = metrics.momTone;
      nodes.vwapDelta.dataset.tone = metrics.vwapDeltaTone;
      const status = payload.connection_status || "UNKNOWN";
      if (nodes.current.connectionStatus !== status) {
        nodes.current.connectionStatus = status;
        nodes.connection.dataset.state = status;
        nodes.connection.textContent = status;
      }
      updateText(nodes, "timestamp", formatSourceTime(payload.regime_source_bar_timestamp || metrics.latestTime));
      const source = payload.chart && payload.chart.source === "track_b_shared_live_ohlcv_store" ? "SQLite 5m" : "local 5m";
      const route = `${source} - ${status}`;
      updateText(nodes, "route", route);
      const error = payload.error || payload.regime_error_reason || payload.regime_stale_reason || "";
      updateText(nodes, "error", error);
      nodes.error.classList.toggle("hidden", error === "");
      updateChart(nodes, payload.chart || null, metrics);
    }

    function directionView(regime) {
      if (regime === "LONG") return { trend: "LONG \\u2191", bias: "BULLISH", tone: "long" };
      if (regime === "SHORT") return { trend: "SHORT \\u2193", bias: "BEARISH", tone: "short" };
      if (regime === "NO_TRADE") return { trend: "FLAT", bias: "NEUTRAL", tone: "flat" };
      if (regime === "STALE") return { trend: "STALE", bias: "STALE", tone: "flat" };
      if (regime === "CALCULATION_ERROR") return { trend: "ERROR", bias: "ERROR", tone: "flat" };
      return { trend: "UNAVAILABLE", bias: "UNAVAILABLE", tone: "flat" };
    }

    function directionalAgreementDisplay(score) {
      if (!score || !Number.isInteger(score.score) || !score.band) return "--";
      return `${score.score} \\u2014 ${score.band}`;
    }

    function confidenceTone(score) {
      return scoreBandTone(score);
    }

    function tradeQualityDisplay(score) {
      if (!score || !Number.isInteger(score.score)) return "QUALITY --";
      return `QUALITY ${score.score}`;
    }

    function tradeQualityTone(score) {
      return scoreBandTone(score);
    }

    function scoreBandTone(score) {
      if (!Number.isFinite(score)) return "weak";
      if (score <= 24) return "weak";
      if (score <= 44) return "developing";
      if (score <= 64) return "moderate";
      if (score <= 79) return "strong";
      return "very-strong";
    }

    function chartMetrics(chart) {
      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const valid = bars.filter((bar) => Number.isFinite(bar.close));
      if (valid.length === 0) {
        return {
          lastPriceText: "--",
          changeText: "--",
          tone: "flat",
          lastPrice: null,
          latestTime: null,
          vwap: null,
          vwapText: "--",
          ma20: null,
          ma20Text: "--",
          volumeText: "Vol --",
          rsiText: "--",
          adxText: "--",
          momText: "--",
          vwapDeltaText: "N/A",
          rsiTone: "flat",
          adxTone: "flat",
          momTone: "flat",
          vwapDeltaTone: "neutral",
        };
      }
      const latest = valid[valid.length - 1];
      const previous = valid.length > 1 ? valid[valid.length - 2] : null;
      const latestClose = latest.close;
      const delta = previous ? latestClose - previous.close : 0;
      const pct = previous && previous.close !== 0 ? delta / previous.close : 0;
      const tone = delta > 0 ? "long" : delta < 0 ? "short" : "flat";
      const sign = delta > 0 ? "+" : delta < 0 ? "-" : "";
      const ma20 = movingAverage(valid, 20);
      const vwap = currentVwap(valid);
      const technicals = calculateTechnicalMetrics(valid);
      const vwapDeltaAtr = technicals.atr != null && technicals.atr > 0 && vwap != null ? (latestClose - vwap) / technicals.atr : null;
      return {
        lastPriceText: formatPrice(latestClose),
        lastPrice: latestClose,
        changeText: `${sign}${formatDelta(delta)} / ${sign}${(pct * 100).toFixed(2)}%`,
        tone,
        latestTime: latest.time || latest.bar_end || chart.latest_bar_ts || null,
        vwap,
        vwapText: vwap == null ? "--" : formatPrice(vwap),
        ma20,
        ma20Text: ma20 == null ? "--" : formatPrice(ma20),
        volumeText: `Vol ${formatVolume(latest.volume)}`,
        rsiText: technicals.rsi == null ? "--" : technicals.rsi.toFixed(1),
        adxText: technicals.adx == null ? "--" : technicals.adx.toFixed(1),
        momText: technicals.mom == null ? "--" : `${technicals.mom >= 0 ? "+" : ""}${(technicals.mom * 100).toFixed(2)}%`,
        vwapDeltaText: vwapDeltaAtr == null ? "N/A" : `${vwapDeltaAtr >= 0 ? "+" : ""}${vwapDeltaAtr.toFixed(2)} ATR`,
        rsiTone: rsiTone(technicals.rsi),
        adxTone: technicals.adx == null ? "flat" : technicals.adx >= 25 ? "long" : "flat",
        momTone: technicals.mom == null ? "flat" : technicals.mom > 0 ? "long" : technicals.mom < 0 ? "short" : "flat",
        vwapDeltaTone: vwapDeltaAtr == null ? "neutral" : Math.abs(vwapDeltaAtr) < 0.10 ? "yellow" : vwapDeltaAtr > 0 ? "long" : "short",
      };
    }

    function movingAverage(bars, period) {
      if (bars.length < period) return null;
      const window = bars.slice(-period);
      return window.reduce((sum, bar) => sum + bar.close, 0) / period;
    }

    function movingAverageSeries(bars, period) {
      return bars.map((bar, index) => {
        if (index + 1 < period) return null;
        const window = bars.slice(index + 1 - period, index + 1);
        return window.reduce((sum, item) => sum + item.close, 0) / period;
      });
    }

    function currentVwap(bars) {
      const series = vwapSeries(bars);
      return series.length ? series[series.length - 1] : null;
    }

    function vwapSeries(bars) {
      let valueVolume = 0;
      let volume = 0;
      return bars.map((bar) => {
        const barVolume = Number.isFinite(bar.volume) && bar.volume > 0 ? bar.volume : 0;
        if (barVolume === 0) return volume > 0 ? valueVolume / volume : null;
        const typical = (bar.high + bar.low + bar.close) / 3;
        valueVolume += typical * barVolume;
        volume += barVolume;
        return valueVolume / volume;
      });
    }

    function calculateTechnicalMetrics(bars) {
      const period = 14;
      const latest = bars[bars.length - 1];
      const previous = bars.length > 10 ? bars[bars.length - 11] : null;
      const mom = previous && previous.close !== 0 ? (latest.close - previous.close) / previous.close : null;
      if (bars.length < period + 1) {
        return { rsi: null, adx: null, mom, atr: null };
      }
      let gains = 0;
      let losses = 0;
      let trueRange = 0;
      for (let index = bars.length - period; index < bars.length; index += 1) {
        const current = bars[index];
        const prior = bars[index - 1];
        const change = current.close - prior.close;
        gains += Math.max(change, 0);
        losses += Math.max(-change, 0);
        trueRange += Math.max(
          current.high - current.low,
          Math.abs(current.high - prior.close),
          Math.abs(current.low - prior.close)
        );
      }
      const avgGain = gains / period;
      const avgLoss = losses / period;
      const rsi = avgLoss === 0 ? 100 : 100 - (100 / (1 + avgGain / avgLoss));
      const atr = trueRange / period;
      const adx = calculateAdx(bars, period);
      return { rsi, adx, mom, atr };
    }

    function calculateAdx(bars, period) {
      const ranges = [];
      const plusMoves = [];
      const minusMoves = [];
      for (let index = 1; index < bars.length; index += 1) {
        const current = bars[index];
        const prior = bars[index - 1];
        const upMove = current.high - prior.high;
        const downMove = prior.low - current.low;
        ranges.push(Math.max(
          current.high - current.low,
          Math.abs(current.high - prior.close),
          Math.abs(current.low - prior.close)
        ));
        plusMoves.push(upMove > downMove && upMove > 0 ? upMove : 0);
        minusMoves.push(downMove > upMove && downMove > 0 ? downMove : 0);
      }
      if (ranges.length < period * 2 - 1) return null;
      let smoothedRange = ranges.slice(0, period).reduce((sum, value) => sum + value, 0);
      let smoothedPlus = plusMoves.slice(0, period).reduce((sum, value) => sum + value, 0);
      let smoothedMinus = minusMoves.slice(0, period).reduce((sum, value) => sum + value, 0);
      const dxValues = [];
      for (let index = period; index < ranges.length; index += 1) {
        smoothedRange = smoothedRange - (smoothedRange / period) + ranges[index];
        smoothedPlus = smoothedPlus - (smoothedPlus / period) + plusMoves[index];
        smoothedMinus = smoothedMinus - (smoothedMinus / period) + minusMoves[index];
        if (smoothedRange === 0) {
          dxValues.push(0);
          continue;
        }
        const plusDi = 100 * (smoothedPlus / smoothedRange);
        const minusDi = 100 * (smoothedMinus / smoothedRange);
        const diSum = plusDi + minusDi;
        dxValues.push(diSum === 0 ? 0 : 100 * Math.abs(plusDi - minusDi) / diSum);
      }
      if (dxValues.length < period) return null;
      let adx = dxValues.slice(0, period).reduce((sum, value) => sum + value, 0) / period;
      for (let index = period; index < dxValues.length; index += 1) {
        adx = ((adx * (period - 1)) + dxValues[index]) / period;
      }
      return Math.min(100, Math.max(0, adx));
    }

    function formatSourceTime(value) {
      if (!value) return "--:--";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "--:--";
      return date.toLocaleTimeString("en-US", { timeZone: DASHBOARD_TIME_ZONE, hour: "2-digit", minute: "2-digit" });
    }

    function rsiTone(value) {
      if (!Number.isFinite(value)) return "flat";
      if (value < 20) return "short";
      if (value < 40) return "orange";
      if (value < 60) return "yellow";
      if (value <= 80) return "lime";
      return "long";
    }

    function instrumentCode(symbol, fallback) {
      const root = String(symbol || fallback || "").split(".")[0].replace(/[^A-Za-z]/g, "");
      return root ? `/${root}` : `/${fallback}`;
    }

    function updateDashboard(payload, meta) {
      meta = meta || {};
      const safePayload = payload && typeof payload === "object" ? payload : {};
      const instruments = safePayload.instruments && typeof safePayload.instruments === "object"
        ? safePayload.instruments
        : { MBT: safePayload || {} };
      updateFooterStatus(safePayload, instruments);
      let ranks = new Map();
      try {
        ranks = rankInstruments(instruments);
      } catch (error) {
        recordRenderError("rankInstruments", error);
      }
      const orderedKeys = PANEL_ORDER.filter((key) => instruments[key]).concat(
        Object.keys(instruments).filter((key) => !PANEL_ORDER.includes(key)).sort()
      );
      let renderedInstrumentCount = 0;
      const renderErrors = [];
      for (const key of orderedKeys) {
        try {
          updatePanel(key, instruments[key] || {}, ranks.get(key) || null);
          renderedInstrumentCount += 1;
        } catch (error) {
          renderErrors.push({ instrument: key, message: error.message });
          recordRenderError(key, error);
          renderPanelError(key, instruments[key] || {}, error);
        }
      }
      const acceptedTimestamp = latestSourceTimestamp(safePayload, instruments);
      clientDiagnostics.acceptedPayloadCount += 1;
      clientDiagnostics.lastAcceptedPayloadTimestamp = acceptedTimestamp;
      clientDiagnostics.lastPayloadReceivedAt = meta.receivedAt || new Date().toISOString();
      clientDiagnostics.lastPayload = safePayload;
      const entry = {
        sequence: clientDiagnostics.pollSequence,
        requestUrl: meta.requestUrl || null,
        receivedAt: clientDiagnostics.lastPayloadReceivedAt,
        payloadTimestamp: acceptedTimestamp,
        renderedInstrumentCount,
        renderErrors,
      };
      appendDiagnosticHistory(entry);
      if (entry.sequence <= 10 || entry.sequence % 20 === 0 || renderErrors.length) {
        sendClientDiagnostic({ event: "render", ...entry });
      }
      if (renderErrors.length) {
        console.warn("[regime-monitor] payload rendered with panel errors", entry);
      } else {
        console.debug("[regime-monitor] payload rendered", entry);
      }
      pulseHeartbeat(clientDiagnostics.lastStatus || "LIVE");
      return entry;
    }

    function rankInstruments(instruments) {
      const rows = Object.entries(instruments || {}).map(([key, payload]) => ({
        key,
        quality: numericScore(payload && payload.trade_quality_score),
        confidence: numericScore(payload && payload.directional_agreement_score),
        adx: tradeQualityComponentValue(payload && payload.trade_quality_score, "adx_strength"),
      }));
      rows.sort((left, right) => {
        if (right.quality !== left.quality) return right.quality - left.quality;
        if (right.confidence !== left.confidence) return right.confidence - left.confidence;
        if (right.adx !== left.adx) return right.adx - left.adx;
        return left.key.localeCompare(right.key);
      });
      return new Map(rows.map((row, index) => [row.key, { rank: index + 1, quality: row.quality }]));
    }

    function numericScore(score) {
      return score && Number.isInteger(score.score) ? score.score : -1;
    }

    function tradeQualityComponentValue(score, name) {
      if (!score || !Array.isArray(score.components)) return -1;
      const component = score.components.find((item) => item.name === name);
      return component && Number.isFinite(component.value) ? component.value : -1;
    }

    function renderPanelError(key, payload, error) {
      const nodes = ensurePanel(key, payload && typeof payload === "object" ? payload : { name: key, symbol: key });
      updateText(nodes, "connection", "RENDER_ERROR");
      nodes.connection.dataset.state = "DASHBOARD_DISCONNECTED";
      updateText(nodes, "error", error.message || String(error));
      nodes.error.classList.remove("hidden");
    }

    function recordRenderError(scope, error) {
      const message = error && error.message ? error.message : String(error);
      clientDiagnostics.lastClientRenderError = {
        scope,
        message,
        at: new Date().toISOString(),
      };
    }

    function appendDiagnosticHistory(entry) {
      clientDiagnostics.history.push(entry);
      if (clientDiagnostics.history.length > 40) clientDiagnostics.history.shift();
    }

    function dataRequestUrl() {
      const url = new URL(DATA_ENDPOINT, window.location.href);
      url.searchParams.set("_", String(Date.now()));
      url.searchParams.set("seq", String(clientDiagnostics.pollSequence));
      return `${url.pathname}${url.search}`;
    }

    function debugParams() {
      try {
        return new URLSearchParams(window.location.search);
      } catch (_error) {
        return new URLSearchParams();
      }
    }

    function applyDebugPayloadMutation(payload) {
      const params = debugParams();
      if (params.has("debug_malformed_once") && clientDiagnostics.forceNextMalformedInstrument == null) {
        clientDiagnostics.forceNextMalformedInstrument = params.get("debug_malformed_once") || PANEL_ORDER[0];
      }
      const key = clientDiagnostics.forceNextMalformedInstrument;
      if (!key) return payload;
      clientDiagnostics.forceNextMalformedInstrument = null;
      if (!payload || !payload.instruments || !payload.instruments[key]) return payload;
      payload.instruments[key] = "SIMULATED_MALFORMED_INSTRUMENT_PAYLOAD";
      return payload;
    }

    // Future SSE migration point: EventSource messages can call this function
    // directly without changing the render/update logic.
    function handleSnapshotMessage(data, meta) {
      meta = meta || {};
      return updateDashboard(data, meta);
    }

    async function pollOnce() {
      updateFooterTime();
      if (clientDiagnostics.requestInFlight) {
        clientDiagnostics.skippedOverlapCount += 1;
        return;
      }
      clientDiagnostics.requestInFlight = true;
      clientDiagnostics.pollSequence += 1;
      clientDiagnostics.lastFetchStartedAt = new Date().toISOString();
      const requestUrl = dataRequestUrl();
      try {
        const params = debugParams();
        if ((params.has("debug_fail_once") && !clientDiagnostics.debugFailConsumed) || clientDiagnostics.forceNextFetchFailure) {
          clientDiagnostics.debugFailConsumed = true;
          clientDiagnostics.forceNextFetchFailure = false;
          throw new Error("simulated_fetch_failure");
        }
        const response = await fetch(requestUrl, {
          cache: "no-store",
          headers: { "Cache-Control": "no-store", "Pragma": "no-cache" },
        });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        const data = applyDebugPayloadMutation(await response.json());
        clientDiagnostics.successfulFetchCount += 1;
        clientDiagnostics.consecutiveFailureCount = 0;
        clientDiagnostics.lastSuccessfulFetchAt = new Date().toISOString();
        clientDiagnostics.lastFetchError = null;
        handleSnapshotMessage(data, { requestUrl, receivedAt: clientDiagnostics.lastSuccessfulFetchAt });
      } catch (error) {
        const message = error && error.message ? error.message : String(error);
        clientDiagnostics.consecutiveFailureCount += 1;
        clientDiagnostics.lastFetchError = { message, at: new Date().toISOString(), requestUrl };
        console.warn("[regime-monitor] data poll failed", clientDiagnostics.lastFetchError);
        sendClientDiagnostic({
          event: "fetch_error",
          sequence: clientDiagnostics.pollSequence,
          requestUrl,
          message,
          consecutiveFailureCount: clientDiagnostics.consecutiveFailureCount,
        });
        updateFooterStatus({ generated_at: null }, {});
        pulseHeartbeat("STALE");
      } finally {
        clientDiagnostics.requestInFlight = false;
        scheduleNextPoll();
      }
    }

    function scheduleNextPoll() {
      window.setTimeout(pollOnce, POLL_INTERVAL_MS);
    }

    function updateChart(nodes, chart, metrics) {
      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const key = JSON.stringify(bars);
      if (nodes.current.chartKey === key) return;
      nodes.current.chartKey = key;
      nodes.current.lastChartPayload = chart || { bars: [] };
      nodes.current.lastChartMetrics = metrics || null;
      drawChart(nodes.canvas, nodes.current.lastChartPayload);
      const scale = nodes.canvas._lastPriceScale || null;
      positionPriceBadge(nodes.latestBadge, scale, metrics ? metrics.lastPrice : null);
      positionPriceBadge(nodes.vwapBadge, scale, metrics ? metrics.vwap : null);
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
      context.fillStyle = "#010606";
      context.fillRect(0, 0, width, height);

      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const valid = bars.filter((bar) =>
        Number.isFinite(bar.open) &&
        Number.isFinite(bar.high) &&
        Number.isFinite(bar.low) &&
        Number.isFinite(bar.close)
      );
      const left = chartAxis.leftPadding;
      const right = chartAxis.rightPadding;
      const top = chartAxis.topPadding;
      const bottom = chartAxis.bottomPadding;
      const plotWidth = Math.max(1, width - left - right);
      const plotHeight = Math.max(1, height - top - bottom);
      const pricePlotHeight = Math.max(1, Math.floor(plotHeight * 0.76));
      const volumeTop = top + pricePlotHeight + 10;
      const volumeHeight = Math.max(14, height - bottom - volumeTop);

      context.strokeStyle = "rgba(83, 113, 118, 0.18)";
      context.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = top + (pricePlotHeight * i / 4);
        context.beginPath();
        context.moveTo(left, y);
        context.lineTo(width - right, y);
        context.stroke();
      }
      if (valid.length === 0) {
        canvas._lastPriceScale = null;
        context.fillStyle = "#697174";
        context.font = chartAxis.fallbackLabelFont;
        context.textAlign = "center";
        context.fillText("Waiting for live 5m candles", width / 2, height / 2);
        return;
      }

      const ma20 = movingAverageSeries(valid, 20);
      const vwap = vwapSeries(valid);
      const highs = valid.map((bar) => bar.high);
      const lows = valid.map((bar) => bar.low);
      ma20.forEach((value) => {
        if (value != null) {
          highs.push(value);
          lows.push(value);
        }
      });
      vwap.forEach((value) => {
        if (value != null) {
          highs.push(value);
          lows.push(value);
        }
      });
      let minPrice = Math.min(...lows);
      let maxPrice = Math.max(...highs);
      const span = Math.max(maxPrice - minPrice, Math.abs(maxPrice) * 0.0005, 1);
      const padding = Math.max(span * chartAxis.priceRangePaddingRatio, span / Math.max(pricePlotHeight, 1) * chartAxis.minPriceRangePixels);
      minPrice -= padding;
      maxPrice += padding;
      const priceToY = (price) => top + ((maxPrice - price) / (maxPrice - minPrice)) * pricePlotHeight;
      canvas._lastPriceScale = { minPrice, maxPrice, top, plotHeight: pricePlotHeight };
      const candleStep = plotWidth / Math.max(valid.length, 1);
      const bodyWidth = Math.max(2, Math.min(12, candleStep * 0.58));
      const timeLabelIndices = calculateTimeLabelIndices(valid, plotWidth);

      context.strokeStyle = "rgba(83, 113, 118, 0.18)";
      context.lineWidth = 1;
      timeLabelIndices.forEach((index) => {
        const x = left + candleStep * index + candleStep / 2;
        context.beginPath();
        context.moveTo(x, top);
        context.lineTo(x, height - bottom + 4);
        context.stroke();
      });

      context.font = chartAxis.yLabelFont;
      context.textAlign = "right";
      context.fillStyle = "#f0f0f0";
      for (let i = 0; i <= 4; i += 1) {
        const price = maxPrice - ((maxPrice - minPrice) * i / 4);
        context.fillText(formatPrice(price), width - 12, top + (pricePlotHeight * i / 4) + 7);
      }

      const maxVolume = Math.max(...valid.map((bar) => Number.isFinite(bar.volume) ? bar.volume : 0), 1);
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
        const volume = Number.isFinite(bar.volume) ? bar.volume : 0;
        const volumeBarHeight = Math.max(1, (volume / maxVolume) * volumeHeight);
        context.globalAlpha = bar.completed === false ? 0.25 : 0.45;
        context.fillRect(
          x - bodyWidth / 2,
          volumeTop + volumeHeight - volumeBarHeight,
          bodyWidth,
          volumeBarHeight
        );
        context.globalAlpha = 1;

        if (timeLabelIndices.has(index)) {
          context.fillStyle = "#f0f0f0";
          context.font = chartAxis.xLabelFont;
          const first = index === 0;
          const last = index === valid.length - 1;
          context.textAlign = first ? "left" : last ? "right" : "center";
          context.fillText(formatTimeLabel(bar.time), first ? left : x, height - chartAxis.xLabelBottomGap);
        }
      });
      drawLine(context, valid, ma20, priceToY, left, candleStep, "#2f83ff", 1.5);
      drawLine(context, valid, vwap, priceToY, left, candleStep, "#d948ff", 1.5);
    }

    function drawLine(context, bars, values, priceToY, left, candleStep, color, width) {
      context.strokeStyle = color;
      context.lineWidth = width;
      context.beginPath();
      let started = false;
      values.forEach((value, index) => {
        if (value == null || !Number.isFinite(value)) return;
        const x = left + candleStep * index + candleStep / 2;
        const y = priceToY(value);
        if (!started) {
          context.moveTo(x, y);
          started = true;
        } else {
          context.lineTo(x, y);
        }
      });
      if (started) context.stroke();
    }

    function positionPriceBadge(element, scale, price) {
      if (!element) return;
      if (!scale || price == null || !Number.isFinite(price)) {
        element.classList.add("hidden");
        return;
      }
      const rawY = scale.top + ((scale.maxPrice - price) / (scale.maxPrice - scale.minPrice)) * scale.plotHeight;
      const clamped = Math.max(scale.top + 4, Math.min(scale.top + scale.plotHeight - 20, rawY - 10));
      element.style.top = `${clamped}px`;
      element.classList.remove("hidden");
    }

    function calculateTimeLabelIndices(bars, plotWidth) {
      const barCount = Array.isArray(bars) ? bars.length : 0;
      if (barCount <= 0) return new Set();
      if (barCount === 1) return new Set([0]);
      const candleStep = plotWidth / barCount;
      const candidates = [];
      let previousHourKey = null;
      const seenHourKeys = new Set();
      bars.forEach((bar, index) => {
        const parts = zonedTimeParts(bar.time || bar.bar_end || bar.start);
        if (!parts) return;
        if (parts.minute === 0) {
          candidates.push({ index, hourKey: parts.hourKey, exact: true });
          seenHourKeys.add(parts.hourKey);
        } else if (previousHourKey != null && parts.hourKey !== previousHourKey && !seenHourKeys.has(parts.hourKey)) {
          candidates.push({ index, hourKey: parts.hourKey, exact: false });
          seenHourKeys.add(parts.hourKey);
        }
        previousHourKey = parts.hourKey;
      });
      const indices = [];
      candidates.forEach((candidate) => {
        const previousIndex = indices[indices.length - 1];
        if (previousIndex == null || (candidate.index - previousIndex) * candleStep >= chartAxis.minXLabelGap) {
          indices.push(candidate.index);
        }
      });
      return new Set(indices);
    }

    function formatPrice(value) {
      if (!Number.isFinite(value)) return "--";
      return value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function formatDelta(value) {
      if (!Number.isFinite(value)) return "--";
      return Math.abs(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    }

    function formatVolume(value) {
      if (!Number.isFinite(value)) return "--";
      if (Math.abs(value) >= 1000000) return `${(value / 1000000).toFixed(1)}M`;
      if (Math.abs(value) >= 1000) return `${(value / 1000).toFixed(1)}K`;
      return value.toFixed(0);
    }

    function formatTimeLabel(value) {
      const parts = zonedTimeParts(value);
      if (!parts) return "";
      const labelHour = parts.hour % 12 || 12;
      const suffix = parts.hour >= 12 ? "PM" : "AM";
      return `${String(labelHour).padStart(2, "0")}:00 ${suffix}`;
    }

    function zonedTimeParts(value) {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return null;
      const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: DASHBOARD_TIME_ZONE,
        weekday: "short",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        hourCycle: "h23",
      }).formatToParts(date);
      const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
      const hour = Number(byType.hour);
      const minute = Number(byType.minute);
      if (!Number.isFinite(hour) || !Number.isFinite(minute)) return null;
      return {
        weekday: byType.weekday,
        weekdayIndex: { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 }[byType.weekday],
        year: Number(byType.year),
        month: Number(byType.month),
        day: Number(byType.day),
        hour,
        minute,
        hourKey: `${byType.year}-${byType.month}-${byType.day}-${String(hour).padStart(2, "0")}`,
      };
    }

    function currentDashboardDate() {
      return window.__REGIME_MONITOR_NOW_OVERRIDE ? new Date(window.__REGIME_MONITOR_NOW_OVERRIDE) : new Date();
    }

    window.addEventListener("resize", () => {
      for (const nodes of panels.values()) {
        drawChart(nodes.canvas, nodes.current.lastChartPayload || { bars: [] });
        const scale = nodes.canvas._lastPriceScale || null;
        const metrics = nodes.current.lastChartMetrics || null;
        positionPriceBadge(nodes.latestBadge, scale, metrics ? metrics.lastPrice : null);
        positionPriceBadge(nodes.vwapBadge, scale, metrics ? metrics.vwap : null);
      }
    });
    function updateFooterTime() {
      const node = document.getElementById("footer-time");
      if (!node) return;
      const now = currentDashboardDate();
      node.textContent = `${now.toLocaleTimeString("en-US", {
        timeZone: DASHBOARD_TIME_ZONE,
        hour12: false,
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
      })} ET`;
      updateMarketSessionFooter(now);
    }

    function updateMarketSessionFooter(now) {
      const marketNode = document.getElementById("footer-market");
      const sessionNode = document.getElementById("footer-session");
      if (!marketNode || !sessionNode) return;
      const state = marketSessionState(now);
      marketNode.textContent = state.market;
      sessionNode.textContent = state.session;
      marketNode.className = state.market === "OPEN" ? "live" : state.market === "MAINTENANCE" ? "stale" : "offline";
      sessionNode.className = state.session === "MAINTENANCE" ? "stale" : state.session === "CLOSED" ? "offline" : "live";
    }

    function marketSessionState(now) {
      const parts = zonedTimeParts(now);
      if (!parts || !Number.isFinite(parts.weekdayIndex)) return { market: "CLOSED", session: "CLOSED" };
      const minuteOfDay = parts.hour * 60 + parts.minute;
      const maintenanceStart = 17 * 60;
      const maintenanceEnd = 18 * 60;
      let market = "OPEN";
      if (parts.weekdayIndex === 6 || (parts.weekdayIndex === 0 && minuteOfDay < maintenanceEnd) || (parts.weekdayIndex === 5 && minuteOfDay >= maintenanceStart)) {
        market = "CLOSED";
      } else if (minuteOfDay >= maintenanceStart && minuteOfDay < maintenanceEnd) {
        market = "MAINTENANCE";
      }
      let session = "CLOSED";
      if (market === "MAINTENANCE") {
        session = "MAINTENANCE";
      } else if (market === "OPEN") {
        if (minuteOfDay >= 18 * 60 || minuteOfDay < 3 * 60) session = "ASIA";
        else if (minuteOfDay < 8 * 60) session = "EUROPE";
        else if (minuteOfDay < 9 * 60 + 30) session = "US PREMARKET";
        else if (minuteOfDay < 16 * 60) session = "US RTH";
        else if (minuteOfDay < 17 * 60) session = "US AFTER HOURS";
      }
      return { market, session };
    }

    function updateFooterStatus(payload, instruments) {
      const values = Object.values(instruments || {});
      const dataNode = document.getElementById("footer-data");
      const ageNode = document.getElementById("footer-age");
      if (!dataNode) return;
      const hasBars = values.some((item) => item.chart && Array.isArray(item.chart.bars) && item.chart.bars.length > 0);
      const hasError = values.some((item) =>
        item.error || item.regime_error_reason || item.regime_stale_reason || item.connection_status === "DASHBOARD_DISCONNECTED"
      );
      const age = latestSourceAgeSeconds(payload, instruments);
      let label = "--";
      let dataClass = "";
      if (hasError || (age != null && age > 10)) {
        label = "STALE";
        dataClass = "offline";
      } else if (age != null && age > 2) {
        label = "DEGRADED";
        dataClass = "stale";
      } else if (hasBars) {
        label = "LIVE";
        dataClass = "live";
      }
      dataNode.textContent = label;
      dataNode.className = dataClass;
      clientDiagnostics.lastStatus = label;
      if (ageNode) {
        if (age == null) {
          ageNode.textContent = "--";
          ageNode.className = "offline";
        } else {
          ageNode.textContent = `${age.toFixed(age < 10 ? 1 : 0)}s`;
          ageNode.className = age <= 2 ? "live" : age <= 10 ? "stale" : "offline";
        }
      }
    }

    function pulseHeartbeat(status) {
      const node = document.getElementById("footer-data-heartbeat");
      if (!node) return;
      const state = status === "LIVE" ? "live" : status === "DEGRADED" ? "stale" : "offline";
      node.className = `data-heartbeat ${state}`;
      node.classList.remove("pulse");
      void node.offsetWidth;
      node.classList.add("pulse");
      window.setTimeout(() => node.classList.remove("pulse"), 150);
    }

    function sendClientDiagnostic(entry) {
      const payload = JSON.stringify({
        ...entry,
        clientSentAt: new Date().toISOString(),
        location: window.location.href,
        status: clientDiagnostics.lastStatus,
        successfulFetchCount: clientDiagnostics.successfulFetchCount,
        acceptedPayloadCount: clientDiagnostics.acceptedPayloadCount,
        consecutiveFailureCount: clientDiagnostics.consecutiveFailureCount,
        lastAcceptedPayloadTimestamp: clientDiagnostics.lastAcceptedPayloadTimestamp,
        lastClientRenderError: clientDiagnostics.lastClientRenderError,
      });
      try {
        if (navigator.sendBeacon) {
          navigator.sendBeacon("/client-diagnostics", new Blob([payload], { type: "application/json" }));
          return;
        }
        fetch("/client-diagnostics", {
          method: "POST",
          body: payload,
          headers: { "Content-Type": "application/json" },
          cache: "no-store",
          keepalive: true,
        }).catch(() => {});
      } catch (_error) {
        // Diagnostics must never affect dashboard rendering.
      }
    }

    function latestSourceTimestamp(payload, instruments) {
      const candidates = [];
      for (const item of Object.values(instruments || {})) {
        candidates.push(item && item.chart && item.chart.generated_at);
        candidates.push(item && item.received_at);
        candidates.push(item && item.regime_calculated_at);
      }
      candidates.push(payload && payload.generated_at);
      const timestamps = candidates
        .map((value) => ({ value, millis: Date.parse(value) }))
        .filter((item) => Number.isFinite(item.millis));
      if (!timestamps.length) return null;
      timestamps.sort((left, right) => right.millis - left.millis);
      return timestamps[0].value;
    }

    function latestSourceAgeSeconds(payload, instruments) {
      const candidates = [];
      for (const item of Object.values(instruments || {})) {
        candidates.push(item && item.chart && item.chart.generated_at);
        candidates.push(item && item.received_at);
        candidates.push(item && item.regime_calculated_at);
      }
      candidates.push(payload && payload.generated_at);
      const timestamps = candidates
        .map((value) => Date.parse(value))
        .filter((value) => Number.isFinite(value));
      if (!timestamps.length) return null;
      const newest = Math.max(...timestamps);
      return Math.max(0, (currentDashboardDate().getTime() - newest) / 1000);
    }
    window.__REGIME_MONITOR_DEBUG = {
      diagnostics: clientDiagnostics,
      pollOnce,
      handleSnapshotMessage,
      simulateFetchFailureOnce() {
        clientDiagnostics.forceNextFetchFailure = true;
      },
      simulateMalformedInstrumentOnce(key = PANEL_ORDER[0]) {
        clientDiagnostics.forceNextMalformedInstrument = key;
      },
    };
    setInterval(updateFooterTime, 1000);
    updateFooterTime();
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


def resolve_databento_feed_enabled(*, config: dict[str, object]) -> bool:
    value = config.get("databento_feed_enabled", True)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() not in {"0", "false", "no", "off", "disabled"}
    return bool(value)


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
    if resolve_databento_feed_enabled(config=config):
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
    else:
        state.mark_all("SHARED_OHLCV_DISPLAY_ONLY")
    app = create_app(state=state)
    try:
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    finally:
        stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
