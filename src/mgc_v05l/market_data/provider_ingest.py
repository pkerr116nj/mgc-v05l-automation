"""Canonical historical-ingest service with provenance and coverage auditing."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from sqlalchemy import select

from ..persistence import build_engine
from ..persistence.db import create_schema
from ..persistence.repositories import RepositorySet
from ..persistence.research_models import InstrumentRecord
from ..persistence.tables import bars_table, market_data_bar_provenance_table, market_data_ingest_runs_table
from .provider_config import load_market_data_providers_config
from .provider_interfaces import MarketDataProvider
from .provider_models import CoverageChange, CoverageSnapshot, HistoricalBarsRequest, HistoricalIngestAudit

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "market_data_ingest"


class HistoricalMarketDataIngestionService:
    """Merge-based ingest into the canonical replay base with provenance sidecars."""

    def __init__(
        self,
        *,
        database_url: str,
        provider_config_path: str | Path | None = None,
        report_dir: Path = DEFAULT_REPORT_DIR,
    ) -> None:
        self._engine = build_engine(database_url)
        create_schema(self._engine)
        self._repositories = RepositorySet(self._engine)
        self._provider_config = load_market_data_providers_config(provider_config_path)
        self._report_dir = report_dir
        self._report_dir.mkdir(parents=True, exist_ok=True)

    def ingest(
        self,
        *,
        provider: MarketDataProvider,
        request: HistoricalBarsRequest,
        allow_canonical_overwrite: bool = False,
    ) -> HistoricalIngestAudit:
        result = provider.fetch_historical_bars(request)
        ingest_run_id = str(uuid4())
        before = self._coverage_snapshot(symbol=request.internal_symbol, timeframe=request.timeframe, data_source=result.data_source)
        inserted = 0
        skipped = 0
        instrument = self._upsert_instrument(provider=provider, internal_symbol=request.internal_symbol)
        with self._engine.begin() as connection:
            connection.execute(
                market_data_ingest_runs_table.insert().prefix_with("OR REPLACE"),
                {
                    "ingest_run_id": ingest_run_id,
                    "provider": result.provider,
                    "dataset": result.dataset,
                    "schema_name": result.schema_name,
                    "request_symbol": result.request_symbol,
                    "internal_symbol": result.internal_symbol,
                    "timeframe": result.timeframe,
                    "data_source": result.data_source,
                    "coverage_start": result.coverage_start.isoformat() if result.coverage_start is not None else None,
                    "coverage_end": result.coverage_end.isoformat() if result.coverage_end is not None else None,
                    "ingest_started_at": result.ingest_time.isoformat(),
                    "ingest_completed_at": datetime.now(UTC).isoformat(),
                    "status": "running",
                    "payload_json": json.dumps(result.metadata, sort_keys=True),
                },
            )
            for bar in result.bars:
                stored_bar_id = _storage_bar_id(result.data_source, bar.bar_id)
                existing = connection.execute(
                    select(bars_table)
                    .where(bars_table.c.bar_id == stored_bar_id)
                    .where(bars_table.c.data_source == result.data_source)
                ).mappings().first()
                if existing is not None and not allow_canonical_overwrite:
                    skipped += 1
                else:
                    connection.execute(
                        bars_table.insert().prefix_with("OR REPLACE"),
                        {
                            "bar_id": stored_bar_id,
                            "instrument_id": instrument.instrument_id,
                            "ticker": bar.symbol,
                            "cusip": instrument.cusip,
                            "asset_class": instrument.asset_class,
                            "data_source": result.data_source,
                            "timestamp": bar.end_ts.isoformat(),
                            "symbol": bar.symbol,
                            "timeframe": bar.timeframe,
                            "start_ts": bar.start_ts.isoformat(),
                            "end_ts": bar.end_ts.isoformat(),
                            "open": bar.open,
                            "high": bar.high,
                            "low": bar.low,
                            "close": bar.close,
                            "volume": bar.volume,
                            "is_final": bar.is_final,
                            "session_asia": bar.session_asia,
                            "session_london": bar.session_london,
                            "session_us": bar.session_us,
                            "session_allowed": bar.session_allowed,
                            "created_at": result.ingest_time.isoformat(),
                        },
                    )
                    inserted += 1
                provenance = result.bar_provenance.get(bar.bar_id)
                if provenance is None:
                    continue
                connection.execute(
                    market_data_bar_provenance_table.insert().prefix_with("OR REPLACE"),
                    {
                        "provenance_id": f"{ingest_run_id}:{stored_bar_id}",
                        "ingest_run_id": ingest_run_id,
                        "bar_id": stored_bar_id,
                        "data_source": result.data_source,
                        "provider": provenance.provider,
                        "dataset": provenance.dataset,
                        "schema_name": provenance.schema_name,
                        "internal_symbol": result.internal_symbol,
                        "raw_symbol": provenance.raw_symbol,
                        "request_symbol": provenance.request_symbol,
                        "stype_in": provenance.stype_in,
                        "stype_out": provenance.stype_out,
                        "interval": provenance.interval,
                        "source_timestamp": bar.end_ts.isoformat(),
                        "ingest_time": provenance.ingest_time.isoformat(),
                        "coverage_start": provenance.coverage_start.isoformat() if provenance.coverage_start is not None else None,
                        "coverage_end": provenance.coverage_end.isoformat() if provenance.coverage_end is not None else None,
                        "provenance_tag": provenance.provenance_tag,
                        "provider_metadata_json": json.dumps(provenance.provider_metadata, sort_keys=True),
                    },
                )
            connection.execute(
                market_data_ingest_runs_table.update()
                .where(market_data_ingest_runs_table.c.ingest_run_id == ingest_run_id),
                {
                    "status": "completed",
                    "payload_json": json.dumps(
                        {
                            **result.metadata,
                            "inserted_bar_count": inserted,
                            "skipped_existing_count": skipped,
                        },
                        sort_keys=True,
                    ),
                },
            )
        after = self._coverage_snapshot(symbol=request.internal_symbol, timeframe=request.timeframe, data_source=result.data_source)
        change = _coverage_change(before=before, after=after)
        if before.earliest and after.earliest and after.earliest > before.earliest:
            raise RuntimeError(
                f"Coverage regression detected for {request.internal_symbol} {request.timeframe}: {before.earliest} -> {after.earliest}"
            )
        payload = HistoricalIngestAudit(
            provider=result.provider,
            internal_symbol=request.internal_symbol,
            timeframe=request.timeframe,
            data_source=result.data_source,
            before=before,
            after=after,
            change=change,
            inserted_bar_count=inserted,
            skipped_existing_count=skipped,
            ingest_run_id=ingest_run_id,
        )
        report_path = self._report_dir / f"historical_ingest_{request.internal_symbol.lower()}_{request.timeframe}_{ingest_run_id}.json"
        report_path.write_text(json.dumps(asdict(payload), indent=2, sort_keys=True), encoding="utf-8")
        return HistoricalIngestAudit(
            provider=payload.provider,
            internal_symbol=payload.internal_symbol,
            timeframe=payload.timeframe,
            data_source=payload.data_source,
            before=payload.before,
            after=payload.after,
            change=payload.change,
            inserted_bar_count=payload.inserted_bar_count,
            skipped_existing_count=payload.skipped_existing_count,
            ingest_run_id=payload.ingest_run_id,
            report_path=str(report_path),
        )

    def _coverage_snapshot(self, *, symbol: str, timeframe: str, data_source: str) -> CoverageSnapshot:
        with self._engine.begin() as connection:
            row = connection.exec_driver_sql(
                """
                select count(*) as bar_count, min(end_ts) as earliest, max(end_ts) as latest
                from bars
                where ticker = ? and timeframe = ? and data_source = ?
                """,
                (symbol, timeframe, data_source),
            ).mappings().one()
        return CoverageSnapshot(
            symbol=symbol,
            timeframe=timeframe,
            data_source=data_source,
            bar_count=int(row["bar_count"] or 0),
            earliest=str(row["earliest"]) if row["earliest"] else None,
            latest=str(row["latest"]) if row["latest"] else None,
        )

    def _upsert_instrument(self, *, provider: MarketDataProvider, internal_symbol: str):
        metadata = provider.describe_symbol(internal_symbol)
        return self._repositories.instruments.upsert(
            InstrumentRecord(
                ticker=internal_symbol,
                asset_class=str(metadata.get("asset_class") or "future"),
                description=str(metadata.get("description") or "") or None,
                exchange=str(metadata.get("exchange") or "") or None,
                is_active=True,
            )
        )


def _storage_bar_id(data_source: str, bar_id: str) -> str:
    normalized_source = str(data_source or "").strip()
    if normalized_source in {"", "internal"}:
        return bar_id
    prefix = f"{normalized_source}::"
    if bar_id.startswith(prefix):
        return bar_id
    return f"{prefix}{bar_id}"


def _coverage_change(*, before: CoverageSnapshot, after: CoverageSnapshot) -> CoverageChange:
    if before.bar_count == 0 and after.bar_count > 0:
        return CoverageChange.INITIAL
    if before.earliest is None or after.earliest is None:
        return CoverageChange.MATCHED
    if after.earliest < before.earliest:
        return CoverageChange.WIDENED
    if after.latest and before.latest and after.latest > before.latest:
        return CoverageChange.APPENDED
    if after.earliest > before.earliest:
        return CoverageChange.NARROWED
    return CoverageChange.MATCHED
