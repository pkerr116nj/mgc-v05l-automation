"""Canonical historical-ingest service with provenance and coverage auditing."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from sqlalchemy import select

from ..persistence import build_engine
from ..persistence.db import create_schema
from ..persistence.repositories import RepositorySet
from ..persistence.research_models import InstrumentRecord
from ..persistence.tables import bars_table, market_data_bar_provenance_table, market_data_ingest_runs_table
from .provider_config import load_market_data_providers_config
from .provider_interfaces import MarketDataProvider
from .provider_models import (
    CoverageChange,
    CoverageSnapshot,
    HistoricalBarsBatch,
    HistoricalBarsRequest,
    HistoricalBarsResult,
    HistoricalBarsStage,
    HistoricalIngestAudit,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "market_data_ingest"
DEFAULT_MANIFEST_DIR = DEFAULT_REPORT_DIR / "manifests"


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
        self._manifest_dir = DEFAULT_MANIFEST_DIR
        self._manifest_dir.mkdir(parents=True, exist_ok=True)

    def ingest(
        self,
        *,
        provider: MarketDataProvider,
        request: HistoricalBarsRequest,
        allow_canonical_overwrite: bool = False,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> HistoricalIngestAudit:
        if hasattr(provider, "stage_historical_bars") and hasattr(provider, "iter_staged_historical_bar_batches"):
            return self._ingest_staged(
                provider=provider,
                request=request,
                allow_canonical_overwrite=allow_canonical_overwrite,
                progress_callback=progress_callback,
            )
        result = provider.fetch_historical_bars(request)
        ingest_run_id = str(uuid4())
        before = self._coverage_snapshot(symbol=request.internal_symbol, timeframe=request.timeframe, data_source=result.data_source)
        inserted = 0
        skipped = 0
        instrument = self._upsert_instrument(provider=provider, internal_symbol=request.internal_symbol)
        with self._engine.begin() as connection:
            self._insert_ingest_run(connection=connection, ingest_run_id=ingest_run_id, result=result)
            inserted, skipped = self._persist_batch(
                connection=connection,
                ingest_run_id=ingest_run_id,
                instrument=instrument,
                result=result,
                batch=HistoricalBarsBatch(bars=result.bars, bar_provenance=result.bar_provenance),
                allow_canonical_overwrite=allow_canonical_overwrite,
                inserted=inserted,
                skipped=skipped,
            )
            self._complete_ingest_run(
                connection=connection,
                ingest_run_id=ingest_run_id,
                metadata={
                    **result.metadata,
                    "inserted_bar_count": inserted,
                    "skipped_existing_count": skipped,
                },
            )
        after = self._coverage_snapshot(symbol=request.internal_symbol, timeframe=request.timeframe, data_source=result.data_source)
        return self._finalize_ingest_audit(
            provider=result.provider,
            request=request,
            data_source=result.data_source,
            before=before,
            after=after,
            fetched_bar_count=len(result.bars),
            inserted_bar_count=inserted,
            skipped_existing_count=skipped,
            ingest_run_id=ingest_run_id,
        )

    def _ingest_staged(
        self,
        *,
        provider: Any,
        request: HistoricalBarsRequest,
        allow_canonical_overwrite: bool,
        progress_callback: Callable[[dict[str, Any]], None] | None,
    ) -> HistoricalIngestAudit:
        if progress_callback is not None:
            progress_callback({"label": "download_started", "status": "running", "detail": {}})
        stage: HistoricalBarsStage = provider.stage_historical_bars(request)
        manifest_path = self._manifest_dir / f"historical_backfill_{request.internal_symbol.lower()}_{request.timeframe}_{uuid4()}.json"
        manifest = self._initial_backfill_manifest(stage=stage, request=request)
        self._write_backfill_manifest(manifest_path, manifest)
        if progress_callback is not None:
            progress_callback(
                {
                    "label": "download_completed",
                    "status": "completed",
                    "detail": {
                        "staged_path": stage.staged_path,
                        "byte_count": stage.metadata.get("byte_count"),
                        "route": stage.route,
                        "fallback_route": stage.fallback_route,
                        "request_size_estimate_bytes": stage.estimated_billable_bytes,
                        "artifact_count": len(stage.staged_artifact_paths),
                        "manifest_path": str(manifest_path),
                    },
                }
            )
            progress_callback({"label": "parse_started", "status": "running", "detail": {}})
        ingest_run_id = str(uuid4())
        before = self._coverage_snapshot(symbol=request.internal_symbol, timeframe=request.timeframe, data_source=stage.data_source)
        instrument = self._upsert_instrument(provider=provider, internal_symbol=request.internal_symbol)
        inserted = 0
        skipped = 0
        fetched = 0
        coverage_start: datetime | None = None
        coverage_end: datetime | None = None
        manifest_status = "completed"
        manifest_reason: str | None = None
        artifact_stats: dict[str, dict[str, Any]] = {
            str(row["path"]): {
                "parse_status": "pending",
                "persist_status": "pending",
                "fetched_bar_count": 0,
                "inserted_bar_count": 0,
                "skipped_existing_count": 0,
            }
            for row in manifest.get("artifact_rows", [])
        }
        try:
            with self._engine.begin() as connection:
                self._insert_ingest_run(
                    connection=connection,
                    ingest_run_id=ingest_run_id,
                    result=HistoricalBarsResult(
                        provider=stage.provider,
                        data_source=stage.data_source,
                        internal_symbol=stage.internal_symbol,
                        timeframe=stage.timeframe,
                        bars=[],
                        coverage_start=None,
                        coverage_end=None,
                        ingest_time=stage.ingest_time,
                        dataset=stage.dataset,
                        schema_name=stage.schema_name,
                        stype_in=stage.stype_in,
                        stype_out=stage.stype_out,
                        request_symbol=stage.request_symbol,
                        provenance_tag=stage.provenance_tag,
                        metadata=stage.metadata,
                        bar_provenance={},
                    ),
                )
                for batch in provider.iter_staged_historical_bar_batches(stage, request=request):
                    artifact_key = str(batch.artifact_path or "")
                    artifact_row = artifact_stats.get(artifact_key)
                    if artifact_row is not None:
                        artifact_row["parse_status"] = "running"
                    fetched += len(batch.bars)
                    if batch.bars:
                        batch_start = batch.bars[0].start_ts
                        batch_end = batch.bars[-1].end_ts
                        coverage_start = batch_start if coverage_start is None or batch_start < coverage_start else coverage_start
                        coverage_end = batch_end if coverage_end is None or batch_end > coverage_end else coverage_end
                    previous_inserted = inserted
                    previous_skipped = skipped
                    inserted, skipped = self._persist_batch(
                        connection=connection,
                        ingest_run_id=ingest_run_id,
                        instrument=instrument,
                        result=HistoricalBarsResult(
                            provider=stage.provider,
                            data_source=stage.data_source,
                            internal_symbol=stage.internal_symbol,
                            timeframe=stage.timeframe,
                            bars=[],
                            coverage_start=coverage_start,
                            coverage_end=coverage_end,
                            ingest_time=stage.ingest_time,
                            dataset=stage.dataset,
                            schema_name=stage.schema_name,
                            stype_in=stage.stype_in,
                            stype_out=stage.stype_out,
                            request_symbol=stage.request_symbol,
                            provenance_tag=stage.provenance_tag,
                            metadata=stage.metadata,
                            bar_provenance={},
                        ),
                        batch=batch,
                        allow_canonical_overwrite=allow_canonical_overwrite,
                        inserted=inserted,
                        skipped=skipped,
                    )
                    if artifact_row is not None:
                        artifact_row["persist_status"] = "running"
                        artifact_row["fetched_bar_count"] += len(batch.bars)
                        artifact_row["inserted_bar_count"] += inserted - previous_inserted
                        artifact_row["skipped_existing_count"] += skipped - previous_skipped
                    if progress_callback is not None:
                        progress_callback(
                            {
                                "label": "rows_persisted",
                                "status": "running",
                                "detail": {
                                    "rows_in_batch": len(batch.bars),
                                    "fetched_bar_count": fetched,
                                    "inserted_bar_count": inserted,
                                    "skipped_existing_count": skipped,
                                    "artifact_path": batch.artifact_path,
                                    "route": stage.route,
                                },
                            }
                        )
                for artifact_row in artifact_stats.values():
                    if artifact_row["parse_status"] == "running":
                        artifact_row["parse_status"] = "completed"
                    if artifact_row["persist_status"] == "running":
                        artifact_row["persist_status"] = "completed"
                    if artifact_row["parse_status"] == "pending":
                        artifact_row["parse_status"] = "zero_records_no_data"
                    if artifact_row["persist_status"] == "pending":
                        artifact_row["persist_status"] = "zero_records_no_data"
                if coverage_start is not None or coverage_end is not None:
                    connection.execute(
                        market_data_bar_provenance_table.update()
                        .where(market_data_bar_provenance_table.c.ingest_run_id == ingest_run_id),
                        {
                            "coverage_start": coverage_start.isoformat() if coverage_start is not None else None,
                            "coverage_end": coverage_end.isoformat() if coverage_end is not None else None,
                        },
                    )
                    connection.execute(
                        market_data_ingest_runs_table.update()
                        .where(market_data_ingest_runs_table.c.ingest_run_id == ingest_run_id),
                        {
                            "coverage_start": coverage_start.isoformat() if coverage_start is not None else None,
                            "coverage_end": coverage_end.isoformat() if coverage_end is not None else None,
                        },
                    )
                self._complete_ingest_run(
                    connection=connection,
                    ingest_run_id=ingest_run_id,
                    metadata={
                        **stage.metadata,
                        "inserted_bar_count": inserted,
                        "skipped_existing_count": skipped,
                        "fetched_bar_count": fetched,
                        "route": stage.route,
                        "fallback_route": stage.fallback_route,
                        "request_size_estimate_bytes": stage.estimated_billable_bytes,
                        "manifest_path": str(manifest_path),
                    },
                )
        except Exception as exc:
            manifest_status = "failed"
            manifest_reason = type(exc).__name__
            raise
        finally:
            manifest["ingest_run_id"] = ingest_run_id
            manifest["parse_status"] = manifest_status
            manifest["persist_status"] = manifest_status
            manifest["fetched_bar_count"] = fetched
            manifest["inserted_bar_count"] = inserted
            manifest["skipped_existing_count"] = skipped
            manifest["status"] = manifest_status
            manifest["reason"] = manifest_reason
            manifest["artifact_rows"] = [
                {
                    **row,
                    **artifact_stats.get(str(row["path"]), {}),
                }
                for row in manifest.get("artifact_rows", [])
            ]
            self._write_backfill_manifest(manifest_path, manifest)
            cleanup = bool(stage.metadata.get("cleanup_staged_files_on_success"))
            if cleanup:
                for artifact_path in stage.staged_artifact_paths:
                    Path(artifact_path).unlink(missing_ok=True)
                stage_root = Path(stage.staged_path)
                manifest_stage_path = Path(stage.manifest_path) if stage.manifest_path else None
                if manifest_stage_path is not None:
                    manifest_stage_path.unlink(missing_ok=True)
                if stage_root.exists() and not any(stage_root.iterdir()):
                    stage_root.rmdir()
        if progress_callback is not None:
            progress_callback(
                {
                    "label": "parse_completed",
                    "status": "completed",
                    "detail": {
                        "fetched_bar_count": fetched,
                        "inserted_bar_count": inserted,
                        "skipped_existing_count": skipped,
                        "route": stage.route,
                        "manifest_path": str(manifest_path),
                    },
                }
            )
        after = self._coverage_snapshot(symbol=request.internal_symbol, timeframe=request.timeframe, data_source=stage.data_source)
        return self._finalize_ingest_audit(
            provider=stage.provider,
            request=request,
            data_source=stage.data_source,
            before=before,
            after=after,
            fetched_bar_count=fetched,
            inserted_bar_count=inserted,
            skipped_existing_count=skipped,
            ingest_run_id=ingest_run_id,
            metadata={
                "route": stage.route,
                "fallback_route": stage.fallback_route,
                "request_size_estimate_bytes": stage.estimated_billable_bytes,
                "manifest_path": str(manifest_path),
                "staged_path": stage.staged_path,
                "artifact_paths": list(stage.staged_artifact_paths),
            },
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

    def _insert_ingest_run(self, *, connection: Any, ingest_run_id: str, result: HistoricalBarsResult) -> None:
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

    def _complete_ingest_run(self, *, connection: Any, ingest_run_id: str, metadata: dict[str, Any]) -> None:
        connection.execute(
            market_data_ingest_runs_table.update().where(market_data_ingest_runs_table.c.ingest_run_id == ingest_run_id),
            {
                "status": "completed",
                "payload_json": json.dumps(metadata, sort_keys=True),
            },
        )

    def _persist_batch(
        self,
        *,
        connection: Any,
        ingest_run_id: str,
        instrument: Any,
        result: HistoricalBarsResult,
        batch: HistoricalBarsBatch,
        allow_canonical_overwrite: bool,
        inserted: int,
        skipped: int,
    ) -> tuple[int, int]:
        for bar in batch.bars:
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
            provenance = batch.bar_provenance.get(bar.bar_id)
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
        return inserted, skipped

    def _finalize_ingest_audit(
        self,
        *,
        provider: str,
        request: HistoricalBarsRequest,
        data_source: str,
        before: CoverageSnapshot,
        after: CoverageSnapshot,
        fetched_bar_count: int,
        inserted_bar_count: int,
        skipped_existing_count: int,
        ingest_run_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> HistoricalIngestAudit:
        change = _coverage_change(before=before, after=after)
        if before.earliest and after.earliest and after.earliest > before.earliest:
            raise RuntimeError(
                f"Coverage regression detected for {request.internal_symbol} {request.timeframe}: {before.earliest} -> {after.earliest}"
            )
        payload = HistoricalIngestAudit(
            provider=provider,
            internal_symbol=request.internal_symbol,
            timeframe=request.timeframe,
            data_source=data_source,
            before=before,
            after=after,
            change=change,
            fetched_bar_count=fetched_bar_count,
            inserted_bar_count=inserted_bar_count,
            skipped_existing_count=skipped_existing_count,
            ingest_run_id=ingest_run_id,
            metadata=dict(metadata or {}),
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
            fetched_bar_count=payload.fetched_bar_count,
            inserted_bar_count=payload.inserted_bar_count,
            skipped_existing_count=payload.skipped_existing_count,
            ingest_run_id=payload.ingest_run_id,
            report_path=str(report_path),
            metadata=dict(payload.metadata),
        )

    def _initial_backfill_manifest(self, *, stage: HistoricalBarsStage, request: HistoricalBarsRequest) -> dict[str, Any]:
        return {
            "provider": stage.provider,
            "internal_symbol": stage.internal_symbol,
            "resolved_provider_symbol": stage.request_symbol,
            "timeframe": stage.timeframe,
            "request_start": request.start.isoformat(),
            "request_end": request.end.isoformat() if request.end is not None else None,
            "request_size_estimate_bytes": stage.estimated_billable_bytes,
            "route": stage.route,
            "fallback_route": stage.fallback_route,
            "staged_path": stage.staged_path,
            "artifact_rows": list(stage.metadata.get("artifact_rows") or []),
            "status": "running",
            "parse_status": "running",
            "persist_status": "pending",
            "fetched_bar_count": 0,
            "inserted_bar_count": 0,
            "skipped_existing_count": 0,
            "ingest_run_id": None,
        }

    def _write_backfill_manifest(self, manifest_path: Path, payload: dict[str, Any]) -> None:
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


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
