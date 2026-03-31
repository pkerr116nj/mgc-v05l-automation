"""Daily operational capture for permanently retained research-history bars."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from sqlalchemy import func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..config_models import DataStoragePolicy, load_data_storage_policy, load_settings_from_files
from ..domain.models import Bar
from ..market_data import (
    HistoricalBackfillService,
    SchwabHistoricalHttpClient,
    SchwabHistoricalRequest,
    SchwabMarketDataAdapter,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
    normalize_timeframe_label,
    timeframe_minutes,
    UrllibJsonTransport,
)
from ..persistence import build_engine
from ..persistence.db import create_schema
from ..persistence.repositories import RepositorySet
from ..persistence.tables import bars_table, research_capture_runs_table, research_capture_status_table
from ..market_data.schwab_auth import SchwabOAuthClient, SchwabTokenStore

_TERMINAL_BROKER_ORDER_STATUSES = frozenset({"FILLED", "CANCELED", "CANCELLED", "REJECTED", "EXPIRED"})
_TRANSIENT_OPTION_CONTRACT_SYMBOL_PATTERN = re.compile(r"^[A-Z0-9.$/_-]{1,8}\s*\d{6}[CP]\d{8}$")


@dataclass(frozen=True)
class ResearchCaptureTarget:
    capture_class: str
    symbol: str
    timeframe: str
    bootstrap_lookback_days: int


@dataclass(frozen=True)
class ResearchCaptureResult:
    capture_class: str
    symbol: str
    timeframe: str
    status: str
    started_at: str
    completed_at: str
    previous_last_bar_end_ts: str | None
    fetched_bar_count: int
    fetched_first_bar_end_ts: str | None
    fetched_last_bar_end_ts: str | None
    persisted_last_bar_end_ts: str | None
    failure_code: str | None = None
    failure_detail: str | None = None


class DailyResearchHistoryCaptureService:
    """Appends the next missing research-history slice for policy-tracked symbols."""

    def __init__(
        self,
        repo_root: Path,
        *,
        config_paths: list[str | Path] | None = None,
        data_policy: DataStoragePolicy | None = None,
        token_file: str | Path | None = None,
        schwab_config_path: str | Path | None = None,
        historical_client=None,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._repo_root = repo_root.resolve(strict=False)
        self._policy = data_policy or load_data_storage_policy(self._repo_root)
        self._config_paths = [Path(path) for path in (config_paths or [self._repo_root / "config" / "base.yaml", self._repo_root / "config" / "replay.yaml"])]
        self._settings = load_settings_from_files(self._config_paths)
        self._research_db_path = self._policy.resolve_path(self._policy.storage_layout.runtime_replay_database_path)
        self._engine = build_engine(f"sqlite:///{self._research_db_path}")
        create_schema(self._engine)
        self._repositories = RepositorySet(self._engine)
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))
        self._schwab_config = _load_capture_schwab_config(
            repo_root=self._repo_root,
            token_file=token_file,
            schwab_config_path=schwab_config_path,
        )
        self._adapter = SchwabMarketDataAdapter(self._settings, self._schwab_config)
        self._history = HistoricalBackfillService(
            adapter=self._adapter,
            client=historical_client
            or SchwabHistoricalHttpClient(
                oauth_client=SchwabOAuthClient(
                    config=self._schwab_config.auth,
                    transport=UrllibJsonTransport(),
                    token_store=SchwabTokenStore(self._schwab_config.auth.token_store_path),
                ),
                market_data_config=self._schwab_config,
                transport=UrllibJsonTransport(),
            ),
            repositories=self._repositories,
        )
        self._manifest_root = self._policy.resolve_path(self._policy.storage_layout.research_root) / "daily_capture"
        self._manifest_root.mkdir(parents=True, exist_ok=True)
        self._data_source = "schwab_history"

    def capture(self) -> dict[str, Any]:
        targets = self._resolve_targets()
        if not targets:
            summary = {
                "status": "no_targets",
                "target_count": 0,
                "attempted_symbols": [],
                "succeeded_symbols": [],
                "failed_symbols": [],
                "target_rows": [],
                "research_database_path": str(self._research_db_path),
                "results": [],
            }
            self._write_manifests(summary)
            return summary

        results: list[ResearchCaptureResult] = []
        for target in targets:
            results.append(self._capture_target(target))

        target_rows = [
            {
                "capture_class": item.capture_class,
                "symbol": item.symbol,
                "timeframe": item.timeframe,
                "status": item.status,
                "last_captured_bar_end_ts": item.persisted_last_bar_end_ts,
                "failure_code": item.failure_code,
                "failure_detail": item.failure_detail,
            }
            for item in results
        ]
        attempted_symbols = sorted({item.symbol for item in results})
        succeeded_symbols = sorted({item.symbol for item in results if item.status == "success"})
        failed_symbols = [
            {
                "capture_class": item.capture_class,
                "symbol": item.symbol,
                "timeframe": item.timeframe,
                "failure_code": item.failure_code,
                "failure_detail": item.failure_detail,
            }
            for item in results
            if item.status != "success"
        ]
        summary = {
            "status": "success" if all(item.status == "success" for item in results) else "partial_failure",
            "target_count": len(targets),
            "success_count": sum(1 for item in results if item.status == "success"),
            "failure_count": sum(1 for item in results if item.status != "success"),
            "attempted_symbols": attempted_symbols,
            "succeeded_symbols": succeeded_symbols,
            "failed_symbols": failed_symbols,
            "capture_started_at": min(item.started_at for item in results),
            "capture_completed_at": max(item.completed_at for item in results),
            "research_database_path": str(self._research_db_path),
            "capture_classes": sorted({item.capture_class for item in results}),
            "target_rows": target_rows,
            "results": [asdict(item) for item in results],
            "policy_config_path": str(self._policy.config_path) if self._policy.config_path is not None else None,
            "config_paths": [str(path) for path in self._config_paths],
        }
        self._write_manifests(summary)
        return summary

    def _resolve_targets(self) -> list[ResearchCaptureTarget]:
        targets: dict[tuple[str, str, str], ResearchCaptureTarget] = {}
        for capture_class, category in self._policy.tracked_symbols.items():
            if not category.include_in_daily_research_capture:
                continue
            for symbol in self._resolve_category_symbols(capture_class=capture_class):
                normalized_symbol = str(symbol).strip().upper()
                if not normalized_symbol:
                    continue
                raw_timeframes = category.research_capture_timeframes or (self._settings.timeframe,)
                for raw_timeframe in raw_timeframes:
                    timeframe = normalize_timeframe_label(raw_timeframe)
                    target = ResearchCaptureTarget(
                        capture_class=capture_class,
                        symbol=normalized_symbol,
                        timeframe=timeframe,
                        bootstrap_lookback_days=category.bootstrap_lookback_days or 30,
                    )
                    targets[(target.capture_class, target.symbol, target.timeframe)] = target
        return sorted(targets.values(), key=lambda item: (item.capture_class, item.symbol, item.timeframe))

    def _resolve_category_symbols(self, *, capture_class: str) -> tuple[str, ...]:
        category = self._policy.tracked_symbols[capture_class]
        if category.symbol_discovery == "explicit_symbols":
            return tuple(category.symbols)
        if category.symbol_discovery == "broker_monitor_activity":
            return self._discover_broker_monitor_symbols()
        if category.symbol_discovery == "runtime_paper_activity":
            return self._discover_runtime_paper_symbols()
        raise ValueError(f"Unsupported symbol discovery mode '{category.symbol_discovery}' for {capture_class}.")

    def _discover_broker_monitor_symbols(self) -> tuple[str, ...]:
        database_path = self._policy.broker_monitor_database_path
        if not database_path.exists():
            return ()
        try:
            with sqlite3.connect(database_path) as connection:
                symbols = set(self._query_symbols(connection, "select distinct symbol from broker_positions where trim(coalesce(symbol, '')) <> ''"))
                open_order_query = """
                    select distinct symbol
                    from broker_orders
                    where trim(coalesce(symbol, '')) <> ''
                      and upper(coalesce(status, '')) not in ({placeholders})
                """.format(placeholders=", ".join("?" for _ in _TERMINAL_BROKER_ORDER_STATUSES))
                symbols.update(self._query_symbols(connection, open_order_query, tuple(sorted(_TERMINAL_BROKER_ORDER_STATUSES))))
        except sqlite3.Error:
            return ()
        return self._filter_default_permanent_symbols(symbols)

    def _discover_runtime_paper_symbols(self) -> tuple[str, ...]:
        database_path = self._policy.resolve_path(self._policy.storage_layout.runtime_paper_database_path)
        if not database_path.exists():
            return ()
        queries = (
            "select distinct instrument from strategy_state_snapshots where trim(coalesce(instrument, '')) <> ''",
            "select distinct symbol from order_intents where trim(coalesce(symbol, '')) <> ''",
            "select distinct instrument from fills where trim(coalesce(instrument, '')) <> ''",
            "select distinct instrument from processed_bars where trim(coalesce(instrument, '')) <> ''",
        )
        try:
            with sqlite3.connect(database_path) as connection:
                symbols: set[str] = set()
                for query in queries:
                    symbols.update(self._query_symbols(connection, query))
        except sqlite3.Error:
            return ()
        return self._filter_default_permanent_symbols(symbols)

    @staticmethod
    def _filter_default_permanent_symbols(symbols: set[str]) -> tuple[str, ...]:
        return tuple(sorted(symbol for symbol in symbols if not _is_transient_option_contract_symbol(symbol)))

    @staticmethod
    def _query_symbols(connection: sqlite3.Connection, query: str, parameters: tuple[Any, ...] = ()) -> tuple[str, ...]:
        rows = connection.execute(query, parameters).fetchall()
        return tuple(
            sorted(
                {
                    str(row[0]).strip().upper()
                    for row in rows
                    if row and str(row[0] or "").strip()
                }
            )
        )

    def _capture_target(self, target: ResearchCaptureTarget) -> ResearchCaptureResult:
        started_at = self._now_fn().astimezone(timezone.utc)
        previous_last_bar_end_ts = self._latest_persisted_bar_end_ts(target.symbol, target.timeframe)
        run_id = self._insert_capture_run(target=target, started_at=started_at, previous_last_bar_end_ts=previous_last_bar_end_ts)
        try:
            request = self._build_history_request(target=target, previous_last_bar_end_ts=previous_last_bar_end_ts)
            bars = self._history.fetch_bars(request, internal_timeframe=target.timeframe)
            fetched_bars = sorted(bars, key=lambda item: item.end_ts)
            persisted_last_bar_end_ts = self._latest_persisted_bar_end_ts(target.symbol, target.timeframe)
            result = ResearchCaptureResult(
                capture_class=target.capture_class,
                symbol=target.symbol,
                timeframe=target.timeframe,
                status="success",
                started_at=started_at.isoformat(),
                completed_at=self._now_fn().astimezone(timezone.utc).isoformat(),
                previous_last_bar_end_ts=previous_last_bar_end_ts.isoformat() if previous_last_bar_end_ts else None,
                fetched_bar_count=len(fetched_bars),
                fetched_first_bar_end_ts=fetched_bars[0].end_ts.astimezone(timezone.utc).isoformat() if fetched_bars else None,
                fetched_last_bar_end_ts=fetched_bars[-1].end_ts.astimezone(timezone.utc).isoformat() if fetched_bars else None,
                persisted_last_bar_end_ts=persisted_last_bar_end_ts.astimezone(timezone.utc).isoformat() if persisted_last_bar_end_ts else None,
            )
            self._finalize_capture_run(run_id=run_id, result=result)
            self._upsert_capture_status(run_id=run_id, result=result)
            return result
        except Exception as exc:
            completed_at = self._now_fn().astimezone(timezone.utc)
            result = ResearchCaptureResult(
                capture_class=target.capture_class,
                symbol=target.symbol,
                timeframe=target.timeframe,
                status="failure",
                started_at=started_at.isoformat(),
                completed_at=completed_at.isoformat(),
                previous_last_bar_end_ts=previous_last_bar_end_ts.isoformat() if previous_last_bar_end_ts else None,
                fetched_bar_count=0,
                fetched_first_bar_end_ts=None,
                fetched_last_bar_end_ts=None,
                persisted_last_bar_end_ts=previous_last_bar_end_ts.isoformat() if previous_last_bar_end_ts else None,
                failure_code=type(exc).__name__,
                failure_detail=str(exc),
            )
            self._finalize_capture_run(run_id=run_id, result=result)
            self._upsert_capture_status(run_id=run_id, result=result)
            return result

    def _build_history_request(
        self,
        *,
        target: ResearchCaptureTarget,
        previous_last_bar_end_ts: datetime | None,
    ) -> SchwabHistoricalRequest:
        now = self._now_fn().astimezone(timezone.utc)
        if previous_last_bar_end_ts is None:
            start_at = now - timedelta(days=target.bootstrap_lookback_days)
        else:
            overlap_minutes = timeframe_minutes(target.timeframe)
            start_at = previous_last_bar_end_ts.astimezone(timezone.utc) - timedelta(minutes=overlap_minutes)
        return SchwabHistoricalRequest(
            internal_symbol=target.symbol,
            period_type="day",
            start_date_ms=int(start_at.timestamp() * 1000),
            end_date_ms=int(now.timestamp() * 1000),
            need_extended_hours_data=False,
            need_previous_close=False,
        )

    def _latest_persisted_bar_end_ts(self, symbol: str, timeframe: str) -> datetime | None:
        statement = (
            select(func.max(bars_table.c.end_ts))
            .where(bars_table.c.ticker == symbol)
            .where(bars_table.c.timeframe == timeframe)
            .where(bars_table.c.data_source == self._data_source)
        )
        with self._engine.begin() as connection:
            value = connection.execute(statement).scalar_one_or_none()
        return datetime.fromisoformat(str(value)) if value not in (None, "") else None

    def _insert_capture_run(
        self,
        *,
        target: ResearchCaptureTarget,
        started_at: datetime,
        previous_last_bar_end_ts: datetime | None,
    ) -> int:
        values = {
            "symbol": target.symbol,
            "timeframe": target.timeframe,
            "capture_class": target.capture_class,
            "data_source": self._data_source,
            "started_at": started_at.isoformat(),
            "status": "running",
            "previous_last_bar_end_ts": previous_last_bar_end_ts.isoformat() if previous_last_bar_end_ts else None,
            "fetched_bar_count": 0,
        }
        with self._engine.begin() as connection:
            result = connection.execute(research_capture_runs_table.insert(), values)
        return int(result.inserted_primary_key[0])

    def _finalize_capture_run(self, *, run_id: int, result: ResearchCaptureResult) -> None:
        values = {
            "completed_at": result.completed_at,
            "status": result.status,
            "fetched_bar_count": result.fetched_bar_count,
            "fetched_first_bar_end_ts": result.fetched_first_bar_end_ts,
            "fetched_last_bar_end_ts": result.fetched_last_bar_end_ts,
            "persisted_last_bar_end_ts": result.persisted_last_bar_end_ts,
            "failure_code": result.failure_code,
            "failure_detail": result.failure_detail,
        }
        with self._engine.begin() as connection:
            connection.execute(
                research_capture_runs_table.update()
                .where(research_capture_runs_table.c.capture_run_id == run_id)
                .values(**values)
            )

    def _upsert_capture_status(self, *, run_id: int, result: ResearchCaptureResult) -> None:
        values = {
            "symbol": result.symbol,
            "timeframe": result.timeframe,
            "capture_class": result.capture_class,
            "data_source": self._data_source,
            "last_attempted_at": result.completed_at,
            "last_succeeded_at": result.completed_at if result.status == "success" else None,
            "last_bar_end_ts": result.persisted_last_bar_end_ts,
            "last_status": result.status,
            "last_failure_code": result.failure_code,
            "last_failure_detail": result.failure_detail,
            "last_capture_run_id": run_id,
        }
        statement = sqlite_insert(research_capture_status_table).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=["symbol", "timeframe", "capture_class", "data_source"],
            set_={
                "last_attempted_at": statement.excluded.last_attempted_at,
                "last_succeeded_at": (
                    statement.excluded.last_succeeded_at
                    if result.status == "success"
                    else research_capture_status_table.c.last_succeeded_at
                ),
                "last_bar_end_ts": statement.excluded.last_bar_end_ts,
                "last_status": statement.excluded.last_status,
                "last_failure_code": statement.excluded.last_failure_code,
                "last_failure_detail": statement.excluded.last_failure_detail,
                "last_capture_run_id": statement.excluded.last_capture_run_id,
            },
        )
        with self._engine.begin() as connection:
            connection.execute(statement)

    def _write_manifests(self, summary: dict[str, Any]) -> None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        runs_dir = self._manifest_root / "runs"
        runs_dir.mkdir(parents=True, exist_ok=True)
        latest_path = self._manifest_root / "latest.json"
        run_path = runs_dir / f"research_daily_capture__{timestamp}.json"
        latest_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
        run_path.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")


def _load_capture_schwab_config(
    *,
    repo_root: Path,
    token_file: str | Path | None,
    schwab_config_path: str | Path | None,
):
    config_path = Path(schwab_config_path or repo_root / "config" / "schwab.local.json")
    config_path = config_path.expanduser()
    if not config_path.is_absolute():
        config_path = (repo_root / config_path).resolve(strict=False)
    schwab_config = load_schwab_market_data_config(config_path)
    if token_file is None:
        return schwab_config
    auth_config = load_schwab_auth_config_from_env(str(token_file))
    return type(schwab_config)(
        auth=auth_config,
        historical_symbol_map=schwab_config.historical_symbol_map,
        quote_symbol_map=schwab_config.quote_symbol_map,
        timeframe_map=schwab_config.timeframe_map,
        field_map=schwab_config.field_map,
        market_context_quote_symbols=schwab_config.market_context_quote_symbols,
        treasury_context_quote_symbols=schwab_config.treasury_context_quote_symbols,
        market_data_base_url=schwab_config.market_data_base_url,
        quotes_symbol_query_param=schwab_config.quotes_symbol_query_param,
    )


def _is_transient_option_contract_symbol(symbol: str) -> bool:
    normalized = str(symbol).strip().upper()
    if not normalized:
        return False
    return bool(_TRANSIENT_OPTION_CONTRACT_SYMBOL_PATTERN.fullmatch(normalized))
