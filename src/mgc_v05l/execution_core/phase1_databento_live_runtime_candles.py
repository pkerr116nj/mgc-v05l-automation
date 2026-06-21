"""Phase-1 Databento Live runtime candle producer.

The service path in this module follows Databento's documented Live API pattern:
create one ``db.Live`` client for one Raw API session, add a raw DBN output stream
for provider evidence, subscribe to ``GLBX.MDP3`` ``ohlcv-1m`` with explicit
``stype_in``, call ``start()``, and keep the session open with
``block_for_close()`` or an equivalent bounded wait for tests/smoke runs.

It has no broker access and never grants submit authority. Runtime artifacts are
HOT market-data truth only when they are generated from Databento Live records or
Databento Live intraday replay records.
"""

from __future__ import annotations

import argparse
import contextlib
import concurrent.futures
import json
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

from mgc_v05l.execution_core.phase1_databento_historical_seed import _aggregate_bars
from mgc_v05l.execution_core.phase1_runtime_ticker_registry import PHASE1_RUNTIME_TIMEFRAMES
from mgc_v05l.execution_core.track_b_databento_live_runtime_feed import (
    DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT,
    TrackBDatabentoLiveFeedConfig,
    TrackBDatabentoLiveFeedResult,
    run_track_b_databento_live_runtime_feed,
    _record_to_candle,
)
from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
    DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
    TrackBLiveMarketDataSymbol,
    load_track_b_live_market_data_symbols,
)
from mgc_v05l.execution_core.track_b_runtime_candle_capture_cli import _load_databento_api_key
from mgc_v05l.market_data.phase1_market_session import (
    phase1_latest_bar_freshness_seconds,
    phase1_symbol_allows_stale_trade_bars,
    phase1_symbol_market_freshness_policy,
)
from mgc_v05l.session_phase_labels import NEW_YORK, label_session_phase, session_restriction_matches_timestamp

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RUNTIME_CANDLE_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
DEFAULT_INTRADAY_BACKFILL_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data_intraday_backfill"
DEFAULT_REPORT_DIR = Path("outputs") / "reports" / "phase1_databento_live_runtime_candles"
DEFAULT_RAW_DBN_ROOT = Path("outputs") / "track_b_execution_core" / "phase1_databento_live_raw_dbn"
DEFAULT_DATABENTO_DATASET = "GLBX.MDP3"
SOURCE_ID = "DATABENTO_REALTIME_PHASE1"
FRESHNESS_SECONDS_BY_TIMEFRAME = {
    "1m": 180.0,
    "3m": 360.0,
    "5m": 600.0,
}


@dataclass(frozen=True)
class Phase1DatabentoLiveRuntimeCandlesConfig:
    repo_root: Path = REPO_ROOT
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    intraday_backfill_root: Path = DEFAULT_INTRADAY_BACKFILL_ROOT
    report_dir: Path = DEFAULT_REPORT_DIR
    legacy_live_output_root: Path = DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT
    live_market_data_symbols_path: Path = DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH
    symbols: tuple[str, ...] | None = None
    dataset: str = DEFAULT_DATABENTO_DATASET
    schema: str = "ohlcv-1m"
    stype_in: str = "continuous"
    stype_out: str = "instrument_id"
    env_file: Path | None = None
    source_id: str = SOURCE_ID
    max_bars: int = 90
    min_bars: int = 8
    max_records: int = 90
    max_seconds_per_symbol: float = 75.0
    max_accumulation_attempts: int = 8
    max_accumulation_seconds_per_symbol: float = 600.0
    max_workers: int = 10
    max_latest_1m_age_seconds: int = 180
    max_completed_5m_age_seconds: int = 600
    now: datetime | None = None


@dataclass(frozen=True)
class Phase1DatabentoLiveRuntimeCandlesResult:
    report: dict[str, Any]
    artifacts_written: list[Path]


LiveRunner = Callable[[TrackBDatabentoLiveFeedConfig], TrackBDatabentoLiveFeedResult]


class DatabentoLiveSession(Protocol):
    def add_callback(self, callback: Callable[[Any], None], exception_callback: Callable[[Exception], None]) -> None: ...

    def add_stream(self, stream: Any, exception_callback: Callable[[Exception], None] | None = None) -> None: ...

    def subscribe(self, **kwargs: Any) -> Any: ...

    def start(self) -> None: ...

    def block_for_close(self, timeout: float | None = None) -> None: ...

    def stop(self) -> None: ...

    def terminate(self) -> None: ...


LiveClientFactory = Callable[[str], DatabentoLiveSession]


@dataclass(frozen=True)
class _Phase1LiveSymbolSelection:
    config_path: Path
    config_version: int
    rows: tuple[TrackBLiveMarketDataSymbol, ...]
    disabled_rows: tuple[TrackBLiveMarketDataSymbol, ...]

    @property
    def symbols(self) -> tuple[str, ...]:
        return tuple(row.symbol for row in self.rows)

    @property
    def requested_symbols(self) -> tuple[str, ...]:
        return tuple(row.databento_symbol for row in self.rows)

    @property
    def required_symbols(self) -> tuple[str, ...]:
        return tuple(row.symbol for row in self.rows if row.required_for_readiness)

    @property
    def optional_symbols(self) -> tuple[str, ...]:
        return tuple(row.symbol for row in self.rows if not row.required_for_readiness)

    @property
    def disabled_symbols(self) -> tuple[str, ...]:
        return tuple(row.symbol for row in self.disabled_rows)

    def by_symbol(self) -> dict[str, TrackBLiveMarketDataSymbol]:
        return {row.symbol: row for row in self.rows}

    def row_for(self, symbol: str) -> TrackBLiveMarketDataSymbol:
        return self.by_symbol()[symbol]


@dataclass(frozen=True)
class Phase1DatabentoLiveListenerConfig:
    repo_root: Path = REPO_ROOT
    runtime_candle_root: Path = DEFAULT_RUNTIME_CANDLE_ROOT
    intraday_backfill_root: Path = DEFAULT_INTRADAY_BACKFILL_ROOT
    report_dir: Path = DEFAULT_REPORT_DIR
    raw_dbn_root: Path = DEFAULT_RAW_DBN_ROOT
    live_market_data_symbols_path: Path = DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH
    symbols: tuple[str, ...] | None = None
    dataset: str = DEFAULT_DATABENTO_DATASET
    schema: str = "ohlcv-1m"
    stype_in: str = "continuous"
    env_file: Path | None = None
    source_id: str = SOURCE_ID
    max_bars: int = 90
    min_bars: int = 8
    max_latest_1m_age_seconds: int = 180
    run_seconds: float | None = None
    intraday_replay_start: str | None = None
    run_id: str | None = None
    now: datetime | None = None


@dataclass(frozen=True)
class Phase1DatabentoLiveListenerResult:
    status: dict[str, Any]
    raw_dbn_path: Path
    artifacts_written: list[Path]


def build_phase1_databento_live_runtime_candles(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    live_runner: LiveRunner | None = None,
    write_artifacts: bool = True,
) -> Phase1DatabentoLiveRuntimeCandlesResult:
    now = _coerce_now(config.now)
    runner = live_runner or _default_live_runner
    selection = _phase1_live_symbol_selection(
        repo_root=config.repo_root,
        path=config.live_market_data_symbols_path,
        requested_symbols=config.symbols,
    )
    symbols = selection.symbols
    rows_by_symbol: dict[str, dict[str, Any]] = {}
    artifacts_written: list[Path] = []

    def produce_symbol(symbol: str) -> tuple[str, dict[str, Any], list[Path]]:
        return _produce_symbol_with_accumulation(
            config=config,
            live_symbol=selection.row_for(symbol),
            symbol=symbol,
            now=now,
            runner=runner,
            write_artifacts=write_artifacts,
        )

    max_workers = max(max(len(symbols), 1), int(config.max_workers))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(produce_symbol, symbol): symbol for symbol in symbols}
        for future in concurrent.futures.as_completed(futures):
            symbol, row, written = future.result()
            rows_by_symbol[symbol] = row
            artifacts_written.extend(written)

    rows = [rows_by_symbol[symbol] for symbol in symbols if symbol in rows_by_symbol]
    confirmed_count = sum(1 for row in rows if row.get("realtime_feed_confirmed") is True)
    required_confirmed_count = sum(
        1 for row in rows if row.get("required_for_readiness") is True and row.get("realtime_feed_confirmed") is True
    )
    report = {
        "schema_version": "phase1_databento_live_runtime_candles_v1",
        "generated_at": now.isoformat(),
        "repo_root": str(Path(config.repo_root)),
        "live_market_data_symbols_path": str(selection.config_path),
        "live_market_data_symbols_version": selection.config_version,
        "source": SOURCE_ID,
        "source_id": config.source_id,
        "dataset": _single_dataset(selection=selection, fallback=config.dataset),
        "schema": _single_schema(selection=selection, fallback=config.schema),
        "symbols": list(symbols),
        "requested_symbols": list(selection.requested_symbols),
        "required_for_readiness_symbols": list(selection.required_symbols),
        "optional_symbols": list(selection.optional_symbols),
        "disabled_symbols": list(selection.disabled_symbols),
        "phase1_symbol_count": len(symbols),
        "realtime_feed_confirmed_count": confirmed_count,
        "readiness_required_confirmed_count": required_confirmed_count,
        "realtime_feed_confirmed_symbols": [
            row["symbol"] for row in rows if row.get("realtime_feed_confirmed") is True
        ],
        "realtime_feed_blocked_symbols": [
            row["symbol"] for row in rows if row.get("realtime_feed_confirmed") is not True
        ],
        "required_for_readiness_blocked_symbols": [
            row["symbol"]
            for row in rows
            if row.get("required_for_readiness") is True and row.get("realtime_feed_confirmed") is not True
        ],
        "optional_degraded_symbols": [
            row["symbol"]
            for row in rows
            if row.get("required_for_readiness") is not True and row.get("realtime_feed_confirmed") is not True
        ],
        "historical_seed_ready": False,
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "can_submit": False,
        "live_money_eligible": False,
        "final_classification": "PHASE1_REALTIME_MARKET_DATA_CONFIRMED"
        if rows and _readiness_confirmed(selection=selection, confirmed_count=confirmed_count, required_confirmed_count=required_confirmed_count)
        else "PHASE1_REALTIME_MARKET_DATA_PARTIAL_OR_BLOCKED",
        "rows": rows,
    }
    if write_artifacts:
        _write_report(config=config, report=report)
    return Phase1DatabentoLiveRuntimeCandlesResult(report=report, artifacts_written=artifacts_written)


def _default_live_runner(config: TrackBDatabentoLiveFeedConfig) -> TrackBDatabentoLiveFeedResult:
    return run_track_b_databento_live_runtime_feed(config=config)


def run_phase1_databento_live_listener(
    *,
    config: Phase1DatabentoLiveListenerConfig,
    live_client_factory: LiveClientFactory | None = None,
    now_func: Callable[[], datetime] | None = None,
) -> Phase1DatabentoLiveListenerResult:
    """Run the Databento-doc-aligned Phase-1 live listener service.

    Default CLI usage keeps the Databento session open. Tests and operator smoke
    checks may set ``run_seconds`` to bound the wait without changing the live
    artifact contract.
    """

    clock = now_func or (lambda: datetime.now(timezone.utc))
    started_at = _coerce_now(config.now or clock())
    selection = _phase1_live_symbol_selection(
        repo_root=config.repo_root,
        path=config.live_market_data_symbols_path,
        requested_symbols=config.symbols,
    )
    symbols = selection.symbols
    requested_symbols = list(selection.requested_symbols)
    run_id = config.run_id or f"phase1_databento_live_listener_{uuid.uuid4().hex}"
    raw_dbn_path = _resolve_path(config.repo_root, config.raw_dbn_root) / f"{run_id}.dbn"
    raw_dbn_path.parent.mkdir(parents=True, exist_ok=True)
    state = _LiveListenerState(
        config=config,
        selection=selection,
        raw_dbn_path=raw_dbn_path,
        started_at=started_at,
        clock=clock,
    )

    api_key, credential_status, credential_source = _load_databento_api_key(config.env_file)
    if not api_key:
        status = state.status(
            provider_status="BLOCKED_MISSING_CREDENTIALS",
            credential_status=credential_status,
            credential_source=credential_source,
            final_classification="PHASE1_DATABENTO_LIVE_LISTENER_BLOCKED",
        )
        _write_listener_status(config=config, status=status)
        return Phase1DatabentoLiveListenerResult(status=status, raw_dbn_path=raw_dbn_path, artifacts_written=[])

    client: DatabentoLiveSession | None = None
    try:
        client = _create_phase1_live_client(api_key=api_key, live_client_factory=live_client_factory)
        client.add_callback(state.on_record, state.on_error)
        client.add_stream(raw_dbn_path, exception_callback=state.on_error)
        intraday_replay_start = config.intraday_replay_start or _current_futures_session_replay_start(started_at)
        subscribe_kwargs: dict[str, Any] = {
            "dataset": _single_dataset(selection=selection, fallback=config.dataset),
            "schema": _single_schema(selection=selection, fallback=config.schema),
            "symbols": requested_symbols,
            "stype_in": config.stype_in,
            "start": intraday_replay_start,
        }
        client.subscribe(**subscribe_kwargs)
        state.subscription_status = "SUBSCRIBED"
        state.subscribe_kwargs = dict(subscribe_kwargs)
        client.start()
        state.listener_alive = True
        state.subscription_status = "STARTED"
        _write_listener_status(
            config=config,
            status=state.status(
                provider_status="RUNNING",
                credential_status=credential_status,
                credential_source=credential_source,
                final_classification="PHASE1_DATABENTO_LIVE_LISTENER_RUNNING",
            ),
        )
        if config.run_seconds is None:
            client.block_for_close()
        else:
            _block_for_bounded_smoke(client=client, timeout=float(config.run_seconds))
    except Exception as exc:  # noqa: BLE001 - provider failures must become status artifacts.
        state.on_error(exc)
    finally:
        state.listener_alive = False
        if client is not None:
            with contextlib.suppress(Exception):
                client.stop()
            with contextlib.suppress(Exception):
                client.terminate()

    final_status = state.status(
        provider_status="STOPPED_WITH_ERRORS" if state.provider_errors else "STOPPED",
        credential_status=credential_status,
        credential_source=credential_source,
        final_classification="PHASE1_REALTIME_MARKET_DATA_CONFIRMED"
        if _readiness_confirmed(
            selection=selection,
            confirmed_count=state.confirmed_symbol_count(),
            required_confirmed_count=state.required_confirmed_symbol_count(),
        )
        else "PHASE1_REALTIME_MARKET_DATA_PARTIAL_OR_BLOCKED",
    )
    _write_listener_status(config=config, status=final_status)
    return Phase1DatabentoLiveListenerResult(
        status=final_status,
        raw_dbn_path=raw_dbn_path,
        artifacts_written=list(state.artifacts_written),
    )


class _LiveListenerState:
    def __init__(
        self,
        *,
        config: Phase1DatabentoLiveListenerConfig,
        selection: _Phase1LiveSymbolSelection,
        raw_dbn_path: Path,
        started_at: datetime,
        clock: Callable[[], datetime],
    ) -> None:
        self.config = config
        self.selection = selection
        self.symbols = selection.symbols
        self.live_symbol_by_symbol = selection.by_symbol()
        self.raw_dbn_path = raw_dbn_path
        self.started_at = started_at
        self.clock = clock
        self.lock = threading.Lock()
        self.bars_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in self.symbols}
        self.instrument_id_to_symbol: dict[str, str] = {}
        self.provider_errors: list[str] = []
        self.system_messages: list[dict[str, Any]] = []
        self.intraday_bars_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in self.symbols}
        self.latest_record_at: datetime | None = None
        self.latest_completed_bar_by_symbol: dict[str, str | None] = {symbol: None for symbol in self.symbols}
        self.records_received = 0
        self.ohlcv_records_received = 0
        self.artifacts_written: list[Path] = []
        self.subscription_status = "NOT_STARTED"
        self.subscribe_kwargs: dict[str, Any] = {}
        self.listener_alive = False

    def on_record(self, record: Any) -> None:
        now = _coerce_now(self.clock())
        with self.lock:
            self.records_received += 1
            self.latest_record_at = now
            mapping = _symbol_mapping_from_record(record=record, symbols=self.symbols)
            if mapping is not None:
                instrument_id, symbol = mapping
                self.instrument_id_to_symbol[instrument_id] = symbol
                self.system_messages.append({"record_type": type(record).__name__, "instrument_id": instrument_id, "symbol": symbol})
                self._write_status_locked(provider_status="RUNNING")
                return
            symbol = _symbol_from_record(record=record, symbols=self.symbols, mapping=self.instrument_id_to_symbol)
            if symbol is None:
                self.system_messages.append({"record_type": type(record).__name__, "message": "unmapped non-OHLCV record"})
                self._write_status_locked(provider_status="RUNNING")
                return
            try:
                candle, _ts_event, _ts_recv = _record_to_candle(record, received_at=now)
            except ValueError:
                self.system_messages.append({"record_type": type(record).__name__, "symbol": symbol, "message": "non-OHLCV record"})
                self._write_status_locked(provider_status="RUNNING")
                return
            self.ohlcv_records_received += 1
            bars = self.bars_by_symbol.setdefault(symbol, [])
            bars.extend(_normalize_live_1m_candles({"candles": [candle]}))
            self.bars_by_symbol[symbol] = _dedupe_phase1_bars(bars)[-int(self.config.max_bars) :]
            intraday_bars = self.intraday_bars_by_symbol.setdefault(symbol, [])
            intraday_bars.extend(_normalize_live_1m_candles({"candles": [candle]}))
            self.intraday_bars_by_symbol[symbol] = _current_session_anchor_bars(
                bars=_dedupe_phase1_bars(intraday_bars),
                generated_at=now,
            )
            written = _write_symbol_runtime_artifacts_from_bars(
                config=self.config,
                live_symbol=self.live_symbol_by_symbol[symbol],
                symbol=symbol,
                bars=self.bars_by_symbol[symbol],
                intraday_bars=self.intraday_bars_by_symbol[symbol],
                generated_at=now,
                raw_dbn_path=self.raw_dbn_path,
            )
            self.artifacts_written.extend(written)
            if self.bars_by_symbol[symbol]:
                self.latest_completed_bar_by_symbol[symbol] = self.bars_by_symbol[symbol][-1]["bar_end"]
            self._write_status_locked(provider_status="RUNNING")

    def on_error(self, exc: Exception) -> None:
        with self.lock:
            self.provider_errors.append(_sanitize_provider_error(exc))
            self._write_status_locked(provider_status="ERROR")

    def confirmed_symbol_count(self) -> int:
        return sum(
            1
            for symbol in self.symbols
            if _all_timeframes_confirmed(
                config=self.config,
                live_symbol=self.live_symbol_by_symbol[symbol],
                symbol=symbol,
                now=_coerce_now(self.clock()),
            )
        )

    def required_confirmed_symbol_count(self) -> int:
        return sum(
            1
            for symbol in self.selection.required_symbols
            if _all_timeframes_confirmed(
                config=self.config,
                live_symbol=self.live_symbol_by_symbol[symbol],
                symbol=symbol,
                now=_coerce_now(self.clock()),
            )
        )

    def status(
        self,
        *,
        provider_status: str,
        credential_status: str,
        credential_source: str | None,
        final_classification: str,
    ) -> dict[str, Any]:
        with self.lock:
            return self._status_locked(
                provider_status=provider_status,
                credential_status=credential_status,
                credential_source=credential_source,
                final_classification=final_classification,
            )

    def _write_status_locked(self, *, provider_status: str) -> None:
        status = self._status_locked(
            provider_status=provider_status,
            credential_status="AVAILABLE",
            credential_source=None,
            final_classification="PHASE1_DATABENTO_LIVE_LISTENER_RUNNING",
        )
        _write_listener_status(config=self.config, status=status)

    def _status_locked(
        self,
        *,
        provider_status: str,
        credential_status: str,
        credential_source: str | None,
        final_classification: str,
    ) -> dict[str, Any]:
        generated_at = _coerce_now(self.clock())
        rows = []
        for symbol in self.symbols:
            live_symbol = self.live_symbol_by_symbol[symbol]
            rows.append(
                {
                    "symbol": symbol,
                    "required_for_readiness": live_symbol.required_for_readiness,
                    "databento_symbol": live_symbol.databento_symbol,
                    "dataset": live_symbol.dataset,
                    "schema": live_symbol.schema,
                    "bar_count": len(self.bars_by_symbol.get(symbol, [])),
                    "intraday_backfill_bar_count": len(self.intraday_bars_by_symbol.get(symbol, [])),
                    "intraday_backfill_path": str(
                        _intraday_backfill_path_for_listener(config=self.config, symbol=symbol, timeframe="1m")
                    ),
                    "latest_completed_bar_ts": self.latest_completed_bar_by_symbol.get(symbol),
                    "realtime_feed_confirmed": _all_timeframes_confirmed(
                        config=self.config,
                        live_symbol=live_symbol,
                        symbol=symbol,
                        now=generated_at,
                    ),
                }
            )
        required_confirmed_count = sum(
            1 for row in rows if row["required_for_readiness"] is True and row["realtime_feed_confirmed"]
        )
        return {
            "schema_version": "phase1_databento_live_listener_status_v1",
            "generated_at": generated_at.isoformat(),
            "repo_root": str(Path(self.config.repo_root)),
            "live_market_data_symbols_path": str(self.selection.config_path),
            "live_market_data_symbols_version": self.selection.config_version,
            "source": SOURCE_ID,
            "source_id": self.config.source_id,
            "dataset": _single_dataset(selection=self.selection, fallback=self.config.dataset),
            "schema": _single_schema(selection=self.selection, fallback=self.config.schema),
            "stype_in": self.config.stype_in,
            "symbols": list(self.symbols),
            "requested_symbols": list(self.selection.requested_symbols),
            "required_for_readiness_symbols": list(self.selection.required_symbols),
            "optional_symbols": list(self.selection.optional_symbols),
            "disabled_symbols": list(self.selection.disabled_symbols),
            "subscribe_kwargs": self.subscribe_kwargs,
            "provider_status": provider_status,
            "subscription_status": self.subscription_status,
            "listener_alive": self.listener_alive,
            "raw_dbn_path": str(self.raw_dbn_path),
            "latest_record_at": None if self.latest_record_at is None else self.latest_record_at.isoformat(),
            "records_received": self.records_received,
            "ohlcv_records_received": self.ohlcv_records_received,
            "provider_errors": list(self.provider_errors[-10:]),
            "system_messages": list(self.system_messages[-20:]),
            "credential_status": credential_status,
            "credential_source": credential_source,
            "realtime_feed_confirmed_count": sum(1 for row in rows if row["realtime_feed_confirmed"]),
            "readiness_required_confirmed_count": required_confirmed_count,
            "required_for_readiness_blocked_symbols": [
                row["symbol"]
                for row in rows
                if row["required_for_readiness"] is True and row["realtime_feed_confirmed"] is not True
            ],
            "optional_degraded_symbols": [
                row["symbol"]
                for row in rows
                if row["required_for_readiness"] is not True and row["realtime_feed_confirmed"] is not True
            ],
            "rows": rows,
            "historical_seed_ready": False,
            "research_artifact_used": False,
            "archive_artifact_used": False,
            "can_submit": False,
            "live_money_eligible": False,
            "final_classification": final_classification,
            "databento_pattern": "db.Live + add_stream(raw DBN) + subscribe before start + start + block_for_close",
        }


def _create_phase1_live_client(*, api_key: str, live_client_factory: LiveClientFactory | None) -> DatabentoLiveSession:
    if live_client_factory is not None:
        return live_client_factory(api_key)
    try:
        import databento as db  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("Databento package is not available for Phase-1 live listener.") from exc
    if not hasattr(db, "Live"):
        raise RuntimeError("Installed databento package does not expose databento.Live.")
    try:
        return db.Live(key=api_key, ts_out=True)
    except TypeError:
        return db.Live(api_key, ts_out=True)


def _block_for_bounded_smoke(*, client: DatabentoLiveSession, timeout: float) -> None:
    try:
        client.block_for_close(timeout=timeout)
    except TimeoutError:
        return
    except Exception as exc:  # noqa: BLE001
        if "timeout" not in str(exc).lower():
            raise


def _phase1_live_symbol_selection(
    *,
    repo_root: Path,
    path: Path,
    requested_symbols: Sequence[str] | None,
) -> _Phase1LiveSymbolSelection:
    config_path = _resolve_namelist_path(repo_root=repo_root, path=path)
    namelist = load_track_b_live_market_data_symbols(config_path)
    enabled_by_symbol = {row.symbol: row for row in namelist.enabled_symbols()}
    if requested_symbols is None:
        rows = tuple(enabled_by_symbol.values())
    else:
        parsed = tuple(str(symbol).strip().upper() for symbol in requested_symbols if str(symbol).strip())
        unknown = [symbol for symbol in parsed if symbol not in namelist.by_symbol()]
        if unknown:
            raise ValueError(f"Unsupported Phase-1 Databento live symbols: {', '.join(unknown)}")
        rows = tuple(enabled_by_symbol[symbol] for symbol in parsed if symbol in enabled_by_symbol)
    if not rows:
        raise ValueError("No enabled Phase-1 Databento live symbols selected from Track B live market-data namelist.")
    return _Phase1LiveSymbolSelection(
        config_path=config_path,
        config_version=namelist.version,
        rows=rows,
        disabled_rows=namelist.disabled_symbols(),
    )


def _resolve_namelist_path(*, repo_root: Path, path: Path) -> Path:
    if path.is_absolute():
        return path
    repo_relative = Path(repo_root) / path
    return repo_relative if repo_relative.exists() else path


def _single_dataset(*, selection: _Phase1LiveSymbolSelection, fallback: str) -> str:
    datasets = {row.dataset for row in selection.rows if row.dataset}
    if len(datasets) > 1:
        raise ValueError(f"Phase-1 Databento live listener requires one dataset per session; got {sorted(datasets)}.")
    return next(iter(datasets), fallback)


def _single_schema(*, selection: _Phase1LiveSymbolSelection, fallback: str) -> str:
    schemas = {row.schema for row in selection.rows if row.schema}
    if len(schemas) > 1:
        raise ValueError(f"Phase-1 Databento live listener requires one schema per session; got {sorted(schemas)}.")
    return next(iter(schemas), fallback)


def _readiness_confirmed(
    *,
    selection: _Phase1LiveSymbolSelection,
    confirmed_count: int,
    required_confirmed_count: int,
) -> bool:
    if selection.required_symbols:
        return required_confirmed_count == len(selection.required_symbols)
    return confirmed_count == len(selection.symbols)


def _symbol_mapping_from_record(*, record: Any, symbols: Sequence[str]) -> tuple[str, str] | None:
    record_type = type(record).__name__
    if (
        "SymbolMapping" not in record_type
        and "SymbolMap" not in record_type
        and _record_field(record, "stype_in_symbol") in {None, ""}
    ):
        return None
    text_fields = [
        _record_field(record, "stype_in_symbol"),
        _record_field(record, "stype_out_symbol"),
        _record_field(record, "raw_symbol"),
        _record_field(record, "symbol"),
    ]
    symbol = _match_phase1_symbol(text_fields=text_fields, symbols=symbols)
    instrument_id = _record_field(record, "instrument_id") or _record_field(record, "hd.instrument_id")
    if symbol is None or instrument_id in {None, ""}:
        return None
    return str(instrument_id), symbol


def _symbol_from_record(
    *,
    record: Any,
    symbols: Sequence[str],
    mapping: Mapping[str, str],
) -> str | None:
    for field_name in ("symbol", "raw_symbol", "stype_in_symbol"):
        symbol = _match_phase1_symbol(text_fields=(_record_field(record, field_name),), symbols=symbols)
        if symbol is not None:
            return symbol
    instrument_id = _record_field(record, "instrument_id") or _record_field(record, "hd.instrument_id")
    if instrument_id not in {None, ""} and str(instrument_id) in mapping:
        return mapping[str(instrument_id)]
    return symbols[0] if len(symbols) == 1 else None


def _match_phase1_symbol(*, text_fields: Sequence[Any], symbols: Sequence[str]) -> str | None:
    candidates = sorted((str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()), key=len, reverse=True)
    normalized_fields = [str(value or "").strip().upper() for value in text_fields]
    for value in normalized_fields:
        if not value:
            continue
        for symbol in candidates:
            if value == symbol or value.startswith(f"{symbol}.") or value in {f"{symbol}.V.0", f"{symbol}.C.0"}:
                return symbol
    joined = " ".join(normalized_fields)
    for symbol in candidates:
        if f"{symbol}.V." in joined or f"{symbol}.C." in joined or f" {symbol} " in f" {joined} ":
            return symbol
    return None


def _record_field(record: Any, name: str) -> Any:
    current: Any = record
    for part in name.split("."):
        if isinstance(current, Mapping):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
        if current is None:
            return None
    value = current() if callable(current) else current
    return value


def _dedupe_phase1_bars(bars: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_end: dict[str, dict[str, Any]] = {}
    for bar in bars:
        key = str(bar.get("bar_end") or "")
        if key:
            by_end[key] = dict(bar)
    return [by_end[key] for key in sorted(by_end, key=lambda value: _parse_datetime(value) or datetime.min.replace(tzinfo=timezone.utc))]


def _current_session_anchor_bars(*, bars: Sequence[Mapping[str, Any]], generated_at: datetime) -> list[dict[str, Any]]:
    """Retain the current futures session for anchor recovery, independent of the hot rolling window."""

    generated_at = _coerce_now(generated_at)
    session_start_utc = _current_futures_session_start_utc(generated_at)
    retained = []
    for bar in bars:
        end = _parse_datetime(bar.get("bar_end"))
        if end is not None and session_start_utc <= end <= generated_at + timedelta(minutes=1):
            retained.append(dict(bar))
    return _dedupe_phase1_bars(retained)


def _current_futures_session_replay_start(value: datetime) -> str:
    return _current_futures_session_start_utc(value).isoformat()


def _current_futures_session_start_utc(value: datetime) -> datetime:
    value = _coerce_now(value)
    generated_et = value.astimezone(NEW_YORK)
    session_start_date = generated_et.date()
    if generated_et.hour < 18:
        session_start_date -= timedelta(days=1)
    session_start_et = datetime.combine(
        session_start_date,
        datetime.min.time().replace(hour=18),
        tzinfo=NEW_YORK,
    )
    return session_start_et.astimezone(timezone.utc)


def _write_symbol_runtime_artifacts_from_bars(
    *,
    config: Phase1DatabentoLiveListenerConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    bars: Sequence[Mapping[str, Any]],
    intraday_bars: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    raw_dbn_path: Path,
) -> list[Path]:
    source_one_minute = [dict(bar) for bar in bars]
    one_minute = _merge_existing_runtime_1m_bars(
        config=config,
        symbol=symbol,
        bars=source_one_minute,
    )
    timeframe_bars = _timeframe_bars(one_minute, live_symbol=live_symbol)
    written: list[Path] = []
    for timeframe in PHASE1_RUNTIME_TIMEFRAMES:
        payload = _runtime_payload_for_service(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            generated_at=generated_at,
            bars=timeframe_bars.get(timeframe, []),
            raw_dbn_path=raw_dbn_path,
        )
        path = _runtime_candle_path_for_listener(config=config, symbol=symbol, timeframe=timeframe)
        existing = _read_json(path)
        existing_confirmed = isinstance(existing, dict) and _phase1_payload_confirmed_fresh(
            payload=existing,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            now=generated_at,
        )
        source_payload = _runtime_payload_for_service(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            generated_at=generated_at,
            bars=_timeframe_bars(source_one_minute, live_symbol=live_symbol).get(timeframe, []),
            raw_dbn_path=raw_dbn_path,
        )
        if existing_confirmed:
            if payload["realtime_feed_confirmed"] is not True or source_payload["realtime_feed_confirmed"] is not True:
                continue
            if _parse_datetime(existing.get("last_completed_bar_ts")) == _parse_datetime(
                payload.get("last_completed_bar_ts")
            ):
                continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        written.append(path)
    if intraday_bars:
        backfill_written = _write_intraday_backfill_artifacts_from_bars(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            bars=intraday_bars,
            generated_at=generated_at,
            raw_dbn_path=raw_dbn_path,
        )
        written.extend(backfill_written)
    return written


def _merge_existing_runtime_1m_bars(
    *,
    config: Phase1DatabentoLiveListenerConfig,
    symbol: str,
    bars: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    existing_payload = _read_json(_runtime_candle_path_for_listener(config=config, symbol=symbol, timeframe="1m"))
    existing_bars: list[dict[str, Any]] = []
    if isinstance(existing_payload, Mapping):
        existing_bars = _normalize_live_1m_candles(existing_payload)
    merged = _dedupe_phase1_bars([*existing_bars, *[dict(bar) for bar in bars]])
    return merged[-int(config.max_bars) :]


def _write_intraday_backfill_artifacts_from_bars(
    *,
    config: Phase1DatabentoLiveListenerConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    bars: Sequence[Mapping[str, Any]],
    generated_at: datetime,
    raw_dbn_path: Path,
) -> list[Path]:
    one_minute = _current_session_anchor_bars(
        bars=[dict(bar) for bar in bars],
        generated_at=generated_at,
    )
    timeframe_bars = _timeframe_bars(one_minute, live_symbol=live_symbol)
    written: list[Path] = []
    for timeframe in PHASE1_RUNTIME_TIMEFRAMES:
        payload = _runtime_payload_for_service(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            generated_at=generated_at,
            bars=timeframe_bars.get(timeframe, []),
            raw_dbn_path=raw_dbn_path,
        )
        payload.update(
            {
                "source": "RECOVERED_PHASE1_1M",
                "source_id": f"RECOVERED_PHASE1_1M_{symbol.lower()}",
                "anchor_recovery_backfill": True,
                "phase1_current_day_backfill": True,
                "retention_policy": "CURRENT_FUTURES_SESSION_FROM_1800_ET",
                "source_live_runtime_root": str(_resolve_path(config.repo_root, config.runtime_candle_root)),
                "can_submit": False,
                "paper_trade_allowed": False,
                "live_money_eligible": False,
            }
        )
        path = _intraday_backfill_path_for_listener(config=config, symbol=symbol, timeframe=timeframe)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        written.append(path)
    return written


def _runtime_payload_for_service(
    *,
    config: Phase1DatabentoLiveListenerConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    timeframe: str,
    generated_at: datetime,
    bars: Sequence[Mapping[str, Any]],
    raw_dbn_path: Path,
) -> dict[str, Any]:
    generated_at = _coerce_now(generated_at)
    normalized_bars = [dict(bar) for bar in bars]
    last_completed = _parse_datetime(normalized_bars[-1].get("bar_end")) if normalized_bars else None
    freshness_seconds = _freshness_seconds_for_timeframe(live_symbol=live_symbol, timeframe=timeframe)
    latest_bar_freshness_seconds = _latest_bar_freshness_seconds_for_timeframe(live_symbol=live_symbol, timeframe=timeframe)
    age_seconds = None if last_completed is None else max(0.0, (generated_at - last_completed).total_seconds())
    min_bars = _min_bars_for_timeframe_value(min_bars=live_symbol.min_confirmed_bars, timeframe=timeframe)
    fresh = age_seconds is not None and age_seconds <= latest_bar_freshness_seconds
    complete = len(normalized_bars) >= min_bars
    realtime_confirmed = fresh and complete
    return {
        "source": SOURCE_ID,
        "source_id": f"{config.source_id}_{symbol.lower()}",
        "generated_at": generated_at.isoformat(),
        "symbol": symbol,
        "instrument": symbol,
        "root": symbol,
        "timeframe": timeframe,
        "dataset": live_symbol.dataset,
        "request_symbol": live_symbol.databento_symbol,
        "schema": live_symbol.schema,
        "stype_in": config.stype_in,
        "required_for_readiness": live_symbol.required_for_readiness,
        "execution_symbol": live_symbol.execution_symbol,
        "reference_symbol": live_symbol.reference_symbol,
        "bar_count": len(normalized_bars),
        "first_bar_ts": normalized_bars[0].get("bar_end") if normalized_bars else None,
        "last_completed_bar_ts": None if last_completed is None else last_completed.isoformat(),
        "freshness_seconds": freshness_seconds,
        "latest_bar_freshness_seconds": latest_bar_freshness_seconds,
        "latest_bar_age_seconds": age_seconds,
        "minimum_bar_count": min_bars,
        "historical_seed_ready": False,
        "realtime_feed_confirmed": realtime_confirmed,
        "realtime_feed_block_reason": "READY" if realtime_confirmed else _realtime_block_reason(fresh=fresh, complete=complete, connected=True),
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "completed_candles_only": True,
        "sunday_session_label": label_session_phase(generated_at),
        "sunday_globex_session_supported": session_restriction_matches_timestamp(generated_at, "ASIA"),
        "raw_dbn_evidence_path": str(raw_dbn_path),
        "databento_live_api_replay": bool(config.intraday_replay_start),
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "bars": normalized_bars,
    }


def _all_timeframes_confirmed(
    *,
    config: Phase1DatabentoLiveListenerConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    now: datetime,
) -> bool:
    for timeframe in PHASE1_RUNTIME_TIMEFRAMES:
        payload = _read_json(_runtime_candle_path_for_listener(config=config, symbol=symbol, timeframe=timeframe))
        if not isinstance(payload, dict):
            return False
        if not _phase1_payload_confirmed_fresh(
            payload=payload,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            now=now,
        ):
            return False
    return True


def _write_listener_status(*, config: Phase1DatabentoLiveListenerConfig, status: Mapping[str, Any]) -> None:
    output_dir = _resolve_path(config.repo_root, config.report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_databento_live_listener_status.json").write_text(
        json.dumps(dict(status), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _runtime_candle_path_for_listener(
    *, config: Phase1DatabentoLiveListenerConfig, symbol: str, timeframe: str
) -> Path:
    return _resolve_path(config.repo_root, config.runtime_candle_root) / symbol / timeframe / "latest_runtime_candles.json"


def _intraday_backfill_path_for_listener(
    *, config: Phase1DatabentoLiveListenerConfig, symbol: str, timeframe: str
) -> Path:
    return _resolve_path(config.repo_root, config.intraday_backfill_root) / symbol / timeframe / "latest_runtime_candles.json"


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _sanitize_provider_error(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    api_key = str(os.environ.get("DATABENTO_API_KEY") or "").strip()
    return text.replace(api_key, "<redacted>") if api_key else text


def _produce_symbol_with_accumulation(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    now: datetime,
    runner: LiveRunner,
    write_artifacts: bool,
) -> tuple[str, dict[str, Any], list[Path]]:
    attempts = _accumulation_attempts(config)
    last_row: dict[str, Any] | None = None
    last_result: TrackBDatabentoLiveFeedResult | None = None
    errors: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            live_result = runner(_live_config_for_symbol(config=config, live_symbol=live_symbol, symbol=symbol))
        except Exception as exc:  # noqa: BLE001 - provider/runtime errors become fail-closed rows.
            errors.append(str(exc))
            last_row = _blocked_row(
                live_symbol=live_symbol,
                symbol=symbol,
                reason="DATABENTO_LIVE_PRODUCER_ERROR",
                detail=str(exc),
                attempt_count=attempt,
            )
            continue
        row, _written = _row_and_artifacts_for_live_result(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            now=now,
            live_result=live_result,
            write_artifacts=False,
            attempt_count=attempt,
        )
        last_row = row
        last_result = live_result
        if row.get("realtime_feed_confirmed") is True:
            final_row, written = _row_and_artifacts_for_live_result(
                config=config,
                live_symbol=live_symbol,
                symbol=symbol,
                now=now,
                live_result=live_result,
                write_artifacts=write_artifacts,
                attempt_count=attempt,
            )
            return symbol, final_row, written

    preserved_row = _preserved_existing_confirmed_row(config=config, live_symbol=live_symbol, symbol=symbol, now=now)
    if preserved_row is not None:
        preserved_row["attempt_count"] = attempts
        preserved_row["latest_attempt_block_reason"] = None if last_row is None else last_row.get("block_reason")
        preserved_row["latest_attempt_detail"] = None if last_row is None else last_row.get("detail")
        return symbol, preserved_row, []

    if last_result is not None:
        final_row, written = _row_and_artifacts_for_live_result(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            now=now,
            live_result=last_result,
            write_artifacts=write_artifacts,
            attempt_count=attempts,
        )
        return symbol, final_row, written
    return symbol, _blocked_row(
        live_symbol=live_symbol,
        symbol=symbol,
        reason="DATABENTO_LIVE_PRODUCER_ERROR",
        detail="; ".join(errors[-3:]),
        attempt_count=attempts,
    ), []


def _accumulation_attempts(config: Phase1DatabentoLiveRuntimeCandlesConfig) -> int:
    max_attempts = max(int(config.max_accumulation_attempts), 1)
    per_attempt_seconds = max(float(config.max_seconds_per_symbol), 0.001)
    by_window = int(float(config.max_accumulation_seconds_per_symbol) // per_attempt_seconds)
    if by_window * per_attempt_seconds < float(config.max_accumulation_seconds_per_symbol):
        by_window += 1
    return max(1, min(max_attempts, by_window))


def _row_and_artifacts_for_live_result(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    now: datetime,
    live_result: TrackBDatabentoLiveFeedResult,
    write_artifacts: bool,
    attempt_count: int = 1,
) -> tuple[dict[str, Any], list[Path]]:
    live_event = live_result.live_1m_candles_event or _fallback_legacy_live_event(config=config, symbol=symbol)
    live_connected = live_result.report.get("live_feed_connected") is True or live_event is not None
    if not live_connected:
        return (
            _blocked_row(
                live_symbol=live_symbol,
                symbol=symbol,
                reason=str(live_result.report.get("primary_blocker") or "DATABENTO_LIVE_NOT_CONNECTED"),
                detail=str(live_result.report.get("live_runtime_feed_verdict") or ""),
                live_report_path=str(live_result.report_json),
                attempt_count=attempt_count,
            ),
            [],
        )
    candles_1m = _normalize_live_1m_candles(live_event or {})
    timeframe_bars = _timeframe_bars(candles_1m, live_symbol=live_symbol)
    payloads = {
        timeframe: _runtime_payload(
            config=config,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            generated_at=_parse_datetime(live_result.report.get("generated_at")) or now,
            bars=bars,
            live_result=live_result,
            live_connected=live_connected,
        )
        for timeframe, bars in timeframe_bars.items()
        if timeframe in PHASE1_RUNTIME_TIMEFRAMES
    }
    incomplete = [timeframe for timeframe in PHASE1_RUNTIME_TIMEFRAMES if not payloads.get(timeframe)]
    not_confirmed = [
        timeframe
        for timeframe, payload in payloads.items()
        if payload.get("realtime_feed_confirmed") is not True
    ]
    written: list[Path] = []
    if write_artifacts:
        for timeframe, payload in payloads.items():
            path = _runtime_candle_path(config=config, symbol=symbol, timeframe=timeframe)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            written.append(path)
        intraday_one_minute = _current_session_anchor_bars(
            bars=candles_1m,
            generated_at=_parse_datetime(live_result.report.get("generated_at")) or now,
        )
        intraday_timeframe_bars = _timeframe_bars(intraday_one_minute, live_symbol=live_symbol)
        for timeframe in PHASE1_RUNTIME_TIMEFRAMES:
            payload = _runtime_payload(
                config=config,
                live_symbol=live_symbol,
                symbol=symbol,
                timeframe=timeframe,
                generated_at=_parse_datetime(live_result.report.get("generated_at")) or now,
                bars=intraday_timeframe_bars.get(timeframe, []),
                live_result=live_result,
                live_connected=live_connected,
            )
            intraday_payload = {
                **payload,
                "source": "RECOVERED_PHASE1_1M",
                "source_id": f"RECOVERED_PHASE1_1M_{symbol.lower()}",
                "anchor_recovery_backfill": True,
                "phase1_current_day_backfill": True,
                "retention_policy": "CURRENT_FUTURES_SESSION_FROM_1800_ET",
                "source_live_runtime_root": str(_resolve_path(config.repo_root, config.runtime_candle_root)),
                "can_submit": False,
                "paper_trade_allowed": False,
                "live_money_eligible": False,
            }
            intraday_path = _intraday_backfill_path(config=config, symbol=symbol, timeframe=timeframe)
            intraday_path.parent.mkdir(parents=True, exist_ok=True)
            intraday_path.write_text(json.dumps(intraday_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            written.append(intraday_path)
    return (
        {
            "symbol": symbol,
            "requested_symbol": live_symbol.databento_symbol,
            "required_for_readiness": live_symbol.required_for_readiness,
            "execution_symbol": live_symbol.execution_symbol,
            "reference_symbol": live_symbol.reference_symbol,
            "live_feed_connected": True,
            "runtime_candles_written": sorted(payloads),
            "bar_counts": {timeframe: payload["bar_count"] for timeframe, payload in payloads.items()},
            "latest_completed_bar_ts": {
                timeframe: payload.get("last_completed_bar_ts") for timeframe, payload in payloads.items()
            },
            "realtime_feed_confirmed": bool(payloads) and not incomplete and not not_confirmed,
            "block_reason": "READY" if bool(payloads) and not incomplete and not not_confirmed else "LIVE_CANDLES_NOT_READY",
            "incomplete_timeframes": incomplete,
            "not_confirmed_timeframes": not_confirmed,
            "live_report_path": str(live_result.report_json),
            "attempt_count": attempt_count,
            "can_submit": False,
            "live_money_eligible": False,
        },
        written,
    )


def _preserved_existing_confirmed_row(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    now: datetime,
) -> dict[str, Any] | None:
    payloads: dict[str, dict[str, Any]] = {}
    for timeframe in PHASE1_RUNTIME_TIMEFRAMES:
        path = _runtime_candle_path(config=config, symbol=symbol, timeframe=timeframe)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not _phase1_payload_confirmed_fresh(
            payload=payload,
            live_symbol=live_symbol,
            symbol=symbol,
            timeframe=timeframe,
            now=now,
        ):
            return None
        payloads[timeframe] = payload
    return {
        "symbol": symbol,
        "requested_symbol": live_symbol.databento_symbol,
        "required_for_readiness": live_symbol.required_for_readiness,
        "execution_symbol": live_symbol.execution_symbol,
        "reference_symbol": live_symbol.reference_symbol,
        "live_feed_connected": True,
        "runtime_candles_written": [],
        "bar_counts": {timeframe: payload.get("bar_count") for timeframe, payload in payloads.items()},
        "latest_completed_bar_ts": {
            timeframe: payload.get("last_completed_bar_ts") for timeframe, payload in payloads.items()
        },
        "realtime_feed_confirmed": True,
        "block_reason": "PRESERVED_EXISTING_CONFIRMED_RUNTIME_CANDLES",
        "incomplete_timeframes": [],
        "not_confirmed_timeframes": [],
        "preserved_existing_confirmed_artifacts": True,
        "can_submit": False,
        "live_money_eligible": False,
    }


def _phase1_payload_confirmed_fresh(
    *,
    payload: Mapping[str, Any],
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    timeframe: str,
    now: datetime,
) -> bool:
    if str(payload.get("symbol") or "").strip().upper() != symbol:
        return False
    if str(payload.get("timeframe") or "").strip() != timeframe:
        return False
    if payload.get("realtime_feed_confirmed") is not True:
        return False
    if payload.get("historical_seed_ready") is True:
        return False
    if payload.get("research_artifact_used") is True or payload.get("archive_artifact_used") is True:
        return False
    generated_at = _parse_datetime(payload.get("generated_at"))
    if generated_at is None:
        return False
    return max(0.0, (now - generated_at).total_seconds()) <= _freshness_seconds_for_timeframe(
        live_symbol=live_symbol,
        timeframe=timeframe,
    )


def _live_config_for_symbol(
    *, config: Phase1DatabentoLiveRuntimeCandlesConfig, live_symbol: TrackBLiveMarketDataSymbol, symbol: str
) -> TrackBDatabentoLiveFeedConfig:
    return TrackBDatabentoLiveFeedConfig(
        contract_key=f"{symbol}-PHASE1",
        instrument_family=symbol,
        local_symbol=symbol,
        databento_continuous_symbol=live_symbol.databento_symbol,
        dataset=live_symbol.dataset,
        schema=live_symbol.schema,
        stype_in=config.stype_in,
        stype_out=config.stype_out,
        max_bars=config.max_bars,
        min_bars=live_symbol.min_confirmed_bars,
        max_records=config.max_records,
        max_seconds=config.max_seconds_per_symbol,
        max_latest_1m_age_seconds=live_symbol.freshness_threshold_seconds,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        env_file=config.env_file,
        output_root=_resolve_path(config.repo_root, config.legacy_live_output_root),
        source_id=f"{config.source_id}_{symbol.lower()}",
    )


def _normalize_live_1m_candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("candles") or payload.get("candle_history") or payload.get("bars") or []
    bars: list[dict[str, Any]] = []
    if not isinstance(raw, list):
        return bars
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        end = _parse_datetime(row.get("candle_timestamp") or row.get("bar_end"))
        if end is None:
            continue
        try:
            open_px = float(row.get("open"))
            high_px = float(row.get("high"))
            low_px = float(row.get("low"))
            close_px = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        try:
            volume = float(row.get("volume") or 0.0)
        except (TypeError, ValueError):
            volume = 0.0
        bars.append(
            {
                "bar_start": (end - timedelta(minutes=1)).isoformat(),
                "bar_end": end.isoformat(),
                "open": open_px,
                "high": high_px,
                "low": low_px,
                "close": close_px,
                "volume": volume,
                "completed": True,
            }
        )
    return sorted(bars, key=lambda item: str(item["bar_end"]))


def _dense_no_trade_1m_candles(
    one_minute: Sequence[Mapping[str, Any]],
    *,
    max_gap_minutes: int = 10,
) -> list[dict[str, Any]]:
    """Fill sparse Databento OHLCV no-trade minutes for derived runtime bars."""

    source = [dict(bar) for bar in one_minute]
    if len(source) < 2:
        return source
    dense: list[dict[str, Any]] = []
    previous_end: datetime | None = None
    previous_close: float | None = None
    max_gap = max(int(max_gap_minutes), 1)
    for bar in source:
        current_end = _parse_datetime(bar.get("bar_end"))
        if current_end is None:
            continue
        if previous_end is not None and previous_close is not None:
            missing_minutes = int((current_end - previous_end).total_seconds() // 60) - 1
            if 0 < missing_minutes <= max_gap:
                for offset in range(1, missing_minutes + 1):
                    end = previous_end + timedelta(minutes=offset)
                    dense.append(
                        {
                            "bar_start": (end - timedelta(minutes=1)).isoformat(),
                            "bar_end": end.isoformat(),
                            "open": previous_close,
                            "high": previous_close,
                            "low": previous_close,
                            "close": previous_close,
                            "volume": 0.0,
                            "completed": True,
                            "synthetic_no_trade": True,
                            "source": "DATABENTO_OHLCV_1M_NO_TRADE_GAP_FILL",
                        }
                    )
        dense.append(bar)
        previous_end = current_end
        try:
            previous_close = float(bar.get("close"))
        except (TypeError, ValueError):
            previous_close = None
    return dense


def _derived_candle_sparse_fill_max_gap_minutes(live_symbol: TrackBLiveMarketDataSymbol | None) -> int:
    if live_symbol is None or not phase1_symbol_allows_stale_trade_bars(live_symbol.symbol):
        return 10
    tolerated_seconds = phase1_latest_bar_freshness_seconds(live_symbol.symbol, 10 * 60)
    return max(10, int((float(tolerated_seconds) + 59.0) // 60) * 2)


def _fallback_legacy_live_event(
    *, config: Phase1DatabentoLiveRuntimeCandlesConfig, symbol: str
) -> dict[str, Any] | None:
    path = _resolve_path(config.repo_root, config.legacy_live_output_root) / f"latest_live_{symbol.lower()}_1m_candles.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if str(payload.get("source_id") or "").strip() != f"{config.source_id}_{symbol.lower()}":
        return None
    if str(payload.get("candle_source_mode") or "") != "DATABENTO_LIVE_RUNTIME_FEED":
        return None
    if payload.get("fresh_for_execution") is not True:
        return None
    return payload


def _timeframe_bars(
    one_minute: list[dict[str, Any]],
    *,
    live_symbol: TrackBLiveMarketDataSymbol | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if not one_minute:
        return {}
    dense_one_minute = _dense_no_trade_1m_candles(
        one_minute,
        max_gap_minutes=_derived_candle_sparse_fill_max_gap_minutes(live_symbol),
    )
    return {
        "1m": one_minute,
        "3m": _aggregate_bars(dense_one_minute, minutes=3),
        "5m": _aggregate_bars(dense_one_minute, minutes=5),
    }


def _runtime_payload(
    *,
    config: Phase1DatabentoLiveRuntimeCandlesConfig,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    timeframe: str,
    generated_at: datetime,
    bars: list[dict[str, Any]],
    live_result: TrackBDatabentoLiveFeedResult,
    live_connected: bool,
) -> dict[str, Any]:
    generated_at = _coerce_now(generated_at)
    last_completed = _parse_datetime(bars[-1].get("bar_end")) if bars else None
    freshness_seconds = _freshness_seconds_for_timeframe(live_symbol=live_symbol, timeframe=timeframe)
    latest_bar_freshness_seconds = _latest_bar_freshness_seconds_for_timeframe(live_symbol=live_symbol, timeframe=timeframe)
    age_seconds = None if last_completed is None else max(0.0, (generated_at - last_completed).total_seconds())
    min_bars = _min_bars_for_timeframe_value(min_bars=live_symbol.min_confirmed_bars, timeframe=timeframe)
    fresh = age_seconds is not None and age_seconds <= latest_bar_freshness_seconds
    complete = len(bars) >= min_bars
    stale_trade_bars_allowed = phase1_symbol_allows_stale_trade_bars(live_symbol.symbol)
    realtime_confirmed = live_connected and complete and (fresh or stale_trade_bars_allowed)
    return {
        "source": SOURCE_ID,
        "source_id": f"{config.source_id}_{symbol.lower()}",
        "generated_at": generated_at.isoformat(),
        "symbol": symbol,
        "instrument": symbol,
        "root": symbol,
        "timeframe": timeframe,
        "dataset": live_symbol.dataset,
        "request_symbol": live_symbol.databento_symbol,
        "schema": live_symbol.schema,
        "required_for_readiness": live_symbol.required_for_readiness,
        "execution_symbol": live_symbol.execution_symbol,
        "reference_symbol": live_symbol.reference_symbol,
        "bar_count": len(bars),
        "first_bar_ts": bars[0].get("bar_end") if bars else None,
        "last_completed_bar_ts": None if last_completed is None else last_completed.isoformat(),
        "freshness_seconds": freshness_seconds,
        "latest_bar_freshness_seconds": latest_bar_freshness_seconds,
        "latest_bar_age_seconds": age_seconds,
        "market_freshness_policy": phase1_symbol_market_freshness_policy(live_symbol.symbol),
        "stale_trade_bars_allowed": stale_trade_bars_allowed,
        "minimum_bar_count": min_bars,
        "historical_seed_ready": False,
        "realtime_feed_confirmed": realtime_confirmed,
        "realtime_feed_block_reason": "READY"
        if realtime_confirmed
        else _realtime_block_reason(fresh=fresh, complete=complete, connected=live_connected),
        "research_artifact_used": False,
        "archive_artifact_used": False,
        "completed_candles_only": True,
        "sunday_session_label": label_session_phase(generated_at),
        "sunday_globex_session_supported": session_restriction_matches_timestamp(generated_at, "ASIA"),
        "source_live_report_path": str(live_result.report_json),
        "can_submit": False,
        "paper_trade_allowed": False,
        "live_money_eligible": False,
        "bars": bars,
    }


def _realtime_block_reason(*, fresh: bool, complete: bool, connected: bool) -> str:
    if not connected:
        return "LIVE_FEED_NOT_CONNECTED"
    if not complete:
        return "INSUFFICIENT_LIVE_BARS"
    if not fresh:
        return "LIVE_BARS_STALE"
    return "REALTIME_FEED_NOT_CONFIRMED"


def _min_bars_for_timeframe_value(*, min_bars: int, timeframe: str) -> int:
    if timeframe == "1m":
        return max(int(min_bars), 1)
    if timeframe == "3m":
        return max(int(min_bars) // 3, 1)
    if timeframe == "5m":
        return max(int(min_bars) // 5, 1)
    return 1


def _freshness_seconds_for_timeframe(*, live_symbol: TrackBLiveMarketDataSymbol, timeframe: str) -> float:
    if timeframe == "1m":
        return float(live_symbol.freshness_threshold_seconds)
    return max(float(live_symbol.freshness_threshold_seconds), FRESHNESS_SECONDS_BY_TIMEFRAME[timeframe])


def _latest_bar_freshness_seconds_for_timeframe(*, live_symbol: TrackBLiveMarketDataSymbol, timeframe: str) -> float:
    return phase1_latest_bar_freshness_seconds(
        live_symbol.symbol,
        _freshness_seconds_for_timeframe(live_symbol=live_symbol, timeframe=timeframe),
    )


def _blocked_row(
    *,
    live_symbol: TrackBLiveMarketDataSymbol,
    symbol: str,
    reason: str,
    detail: str = "",
    live_report_path: str | None = None,
    attempt_count: int = 1,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "requested_symbol": live_symbol.databento_symbol,
        "required_for_readiness": live_symbol.required_for_readiness,
        "execution_symbol": live_symbol.execution_symbol,
        "reference_symbol": live_symbol.reference_symbol,
        "live_feed_connected": False,
        "runtime_candles_written": [],
        "realtime_feed_confirmed": False,
        "block_reason": reason,
        "detail": detail,
        "live_report_path": live_report_path,
        "attempt_count": attempt_count,
        "can_submit": False,
        "live_money_eligible": False,
    }


def _write_report(*, config: Phase1DatabentoLiveRuntimeCandlesConfig, report: dict[str, Any]) -> None:
    output_dir = _resolve_path(config.repo_root, config.report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "latest_phase1_databento_live_runtime_candles_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _runtime_candle_path(*, config: Phase1DatabentoLiveRuntimeCandlesConfig, symbol: str, timeframe: str) -> Path:
    return _resolve_path(config.repo_root, config.runtime_candle_root) / symbol / timeframe / "latest_runtime_candles.json"


def _intraday_backfill_path(*, config: Phase1DatabentoLiveRuntimeCandlesConfig, symbol: str, timeframe: str) -> Path:
    return _resolve_path(config.repo_root, config.intraday_backfill_root) / symbol / timeframe / "latest_runtime_candles.json"


def _resolve_path(repo_root: Path, value: Path) -> Path:
    return value if value.is_absolute() else Path(repo_root) / value


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_now(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(timezone.utc)
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Phase-1 Databento Live listener. No broker or submit paths are invoked."
    )
    parser.add_argument("--mode", choices=("service", "legacy-sample"), default="service")
    parser.add_argument("--symbols", help="Optional comma-separated enabled namelist symbols. Omit to use all enabled rows.")
    parser.add_argument("--live-market-data-symbols-path", type=Path, default=DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH)
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--runtime-candle-root", default=str(DEFAULT_RUNTIME_CANDLE_ROOT))
    parser.add_argument("--intraday-backfill-root", default=str(DEFAULT_INTRADAY_BACKFILL_ROOT))
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--raw-dbn-root", default=str(DEFAULT_RAW_DBN_ROOT))
    parser.add_argument("--legacy-live-output-root", default=str(DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT))
    parser.add_argument("--dataset", default=DEFAULT_DATABENTO_DATASET)
    parser.add_argument("--schema", default="ohlcv-1m")
    parser.add_argument("--stype-in", default="continuous")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--run-seconds", type=float, help="Bound the live listener for smoke tests. Omit for service mode.")
    parser.add_argument("--intraday-replay-start", help="Optional Databento Live API replay start passed to subscribe(start=...).")
    parser.add_argument("--max-bars", type=int, default=90)
    parser.add_argument("--min-bars", type=int, default=8)
    parser.add_argument("--max-records", type=int, default=90)
    parser.add_argument("--max-seconds-per-symbol", type=float, default=75.0)
    parser.add_argument("--max-accumulation-attempts", type=int, default=8)
    parser.add_argument("--max-accumulation-seconds-per-symbol", type=float, default=600.0)
    parser.add_argument("--max-workers", type=int, default=10)
    parser.add_argument("--no-write", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    symbols = None
    if args.symbols:
        symbols = tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip())
    if args.mode == "service":
        result = run_phase1_databento_live_listener(
            config=Phase1DatabentoLiveListenerConfig(
                repo_root=Path(args.repo_root),
                runtime_candle_root=Path(args.runtime_candle_root),
                intraday_backfill_root=Path(args.intraday_backfill_root),
                report_dir=Path(args.report_dir),
                raw_dbn_root=Path(args.raw_dbn_root),
                live_market_data_symbols_path=Path(args.live_market_data_symbols_path),
                symbols=symbols,
                dataset=args.dataset,
                schema=args.schema,
                stype_in=args.stype_in,
                env_file=args.env_file,
                max_bars=args.max_bars,
                min_bars=args.min_bars,
                run_seconds=args.run_seconds,
                intraday_replay_start=args.intraday_replay_start,
            )
        )
        print(
            json.dumps(
                {
                    "final_classification": result.status["final_classification"],
                    "phase1_symbol_count": len(result.status["symbols"]),
                    "realtime_feed_confirmed_count": result.status["realtime_feed_confirmed_count"],
                    "listener_status_path": str(_resolve_path(Path(args.repo_root), Path(args.report_dir)) / "latest_phase1_databento_live_listener_status.json"),
                    "raw_dbn_path": str(result.raw_dbn_path),
                    "can_submit": result.status["can_submit"],
                    "live_money_eligible": result.status["live_money_eligible"],
                },
                sort_keys=True,
            )
        )
        return 0 if result.status["realtime_feed_confirmed_count"] > 0 else 2

    result = build_phase1_databento_live_runtime_candles(
        config=Phase1DatabentoLiveRuntimeCandlesConfig(
            repo_root=Path(args.repo_root),
            runtime_candle_root=Path(args.runtime_candle_root),
            intraday_backfill_root=Path(args.intraday_backfill_root),
            report_dir=Path(args.report_dir),
            legacy_live_output_root=Path(args.legacy_live_output_root),
            live_market_data_symbols_path=Path(args.live_market_data_symbols_path),
            symbols=symbols,
            dataset=args.dataset,
            schema=args.schema,
            stype_in=args.stype_in,
            env_file=args.env_file,
            max_bars=args.max_bars,
            min_bars=args.min_bars,
            max_records=args.max_records,
            max_seconds_per_symbol=args.max_seconds_per_symbol,
            max_accumulation_attempts=args.max_accumulation_attempts,
            max_accumulation_seconds_per_symbol=args.max_accumulation_seconds_per_symbol,
            max_workers=args.max_workers,
        ),
        write_artifacts=not args.no_write,
    )
    print(
        json.dumps(
            {
                "final_classification": result.report["final_classification"],
                "phase1_symbol_count": result.report["phase1_symbol_count"],
                "realtime_feed_confirmed_count": result.report["realtime_feed_confirmed_count"],
                "report_path": str(_resolve_path(Path(args.repo_root), Path(args.report_dir)) / "latest_phase1_databento_live_runtime_candles_report.json"),
                "can_submit": result.report["can_submit"],
                "live_money_eligible": result.report["live_money_eligible"],
            },
            sort_keys=True,
        )
    )
    return 0 if result.report["final_classification"] == "PHASE1_REALTIME_MARKET_DATA_CONFIRMED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
