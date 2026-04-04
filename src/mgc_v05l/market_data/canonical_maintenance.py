"""Canonical 1m maintenance, gap audit, and derived timeframe persistence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Sequence
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import select

from ..domain.models import Bar
from ..persistence import build_engine
from ..persistence.db import create_schema
from ..persistence.tables import bars_table, market_data_bar_provenance_table, market_data_ingest_runs_table
from .provider_ingest import HistoricalMarketDataIngestionService, _storage_bar_id
from .provider_interfaces import MarketDataProvider
from .provider_models import HistoricalBarProvenance, HistoricalBarsRequest
from .timeframes import normalize_timeframe_label, timeframe_minutes
from .bar_models import build_bar_id

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REPORT_DIR = REPO_ROOT / "outputs" / "reports" / "canonical_market_data"
NEW_YORK = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class GapRange:
    start: str
    end: str
    missing_minutes: int
    expected_maintenance_gap: bool


@dataclass(frozen=True)
class CanonicalCoverageAudit:
    symbol: str
    timeframe: str
    data_source: str
    bar_count: int
    earliest: str | None
    latest: str | None
    gap_count: int
    gaps: list[GapRange]
    report_path: str | None = None


@dataclass(frozen=True)
class CanonicalMergeAudit:
    symbol: str
    timeframe: str
    raw_data_source: str
    canonical_data_source: str
    raw_saved_count: int
    canonical_saved_count: int
    canonical_skipped_existing_count: int
    coverage_start: str | None
    coverage_end: str | None
    ingest_run_id: str
    report_path: str | None = None


@dataclass(frozen=True)
class DerivedTimeframeAudit:
    symbol: str
    source_timeframe: str
    target_timeframe: str
    source_data_source: str
    target_data_source: str
    source_bar_count: int
    derived_bar_count: int
    skipped_incomplete_buckets: int
    coverage_start: str | None
    coverage_end: str | None
    ingest_run_id: str
    report_path: str | None = None


class CanonicalMarketDataMaintenanceService:
    """Maintains the preserved canonical 1m base and its derived whole-minute surfaces."""

    def __init__(
        self,
        *,
        database_url: str,
        report_dir: Path = DEFAULT_REPORT_DIR,
        provider_config_path: str | Path | None = None,
    ) -> None:
        self._engine = build_engine(database_url)
        create_schema(self._engine)
        self._report_dir = report_dir
        self._report_dir.mkdir(parents=True, exist_ok=True)
        self._provider_config_path = provider_config_path

    def persist_completed_1m_bars(
        self,
        *,
        bars: Sequence[Bar],
        raw_data_source: str,
        provider: str,
        provenance_tag: str,
        dataset: str | None = None,
        schema_name: str | None = None,
        request_symbol: str | None = None,
        provider_metadata: dict[str, Any] | None = None,
    ) -> CanonicalMergeAudit | None:
        finalized_1m_bars = [
            bar
            for bar in bars
            if bar.is_final and str(bar.timeframe).lower() == "1m"
        ]
        if not finalized_1m_bars:
            return None

        ordered_bars = sorted(finalized_1m_bars, key=lambda item: item.end_ts)
        symbol = ordered_bars[0].symbol
        ingest_run_id = str(uuid4())
        ingest_time = datetime.now(UTC)
        canonical_data_source = "historical_1m_canonical"
        raw_saved_count = 0
        canonical_saved_count = 0
        canonical_skipped_existing_count = 0
        coverage_start = ordered_bars[0].start_ts.isoformat()
        coverage_end = ordered_bars[-1].end_ts.isoformat()

        with self._engine.begin() as connection:
            connection.execute(
                market_data_ingest_runs_table.insert().prefix_with("OR REPLACE"),
                {
                    "ingest_run_id": ingest_run_id,
                    "provider": provider,
                    "dataset": dataset,
                    "schema_name": schema_name,
                    "request_symbol": request_symbol or symbol,
                    "internal_symbol": symbol,
                    "timeframe": "1m",
                    "data_source": canonical_data_source,
                    "coverage_start": coverage_start,
                    "coverage_end": coverage_end,
                    "ingest_started_at": ingest_time.isoformat(),
                    "ingest_completed_at": ingest_time.isoformat(),
                    "status": "running",
                    "payload_json": json.dumps(
                        {
                            "raw_data_source": raw_data_source,
                            "provider_metadata": provider_metadata or {},
                        },
                        sort_keys=True,
                    ),
                },
            )
            for bar in ordered_bars:
                raw_bar = replace(bar, bar_id=_storage_bar_id(raw_data_source, bar.bar_id))
                connection.execute(
                    bars_table.insert().prefix_with("OR REPLACE"),
                    _bar_insert_values(raw_bar, data_source=raw_data_source, created_at=ingest_time),
                )
                raw_saved_count += 1

                canonical_bar_id = _storage_bar_id(canonical_data_source, bar.bar_id)
                existing = connection.execute(
                    select(bars_table.c.bar_id).where(
                        bars_table.c.bar_id == canonical_bar_id,
                        bars_table.c.data_source == canonical_data_source,
                    )
                ).first()
                if existing is not None:
                    canonical_skipped_existing_count += 1
                else:
                    canonical_bar = replace(bar, bar_id=canonical_bar_id)
                    connection.execute(
                        bars_table.insert().prefix_with("OR REPLACE"),
                        _bar_insert_values(canonical_bar, data_source=canonical_data_source, created_at=ingest_time),
                    )
                    canonical_saved_count += 1
                provenance = HistoricalBarProvenance(
                    provider=provider,
                    dataset=dataset,
                    schema_name=schema_name,
                    raw_symbol=bar.symbol,
                    request_symbol=request_symbol or symbol,
                    stype_in=None,
                    stype_out=None,
                    interval="1m",
                    ingest_time=ingest_time,
                    coverage_start=ordered_bars[0].start_ts,
                    coverage_end=ordered_bars[-1].end_ts,
                    provenance_tag=provenance_tag,
                    provider_metadata=dict(provider_metadata or {"raw_data_source": raw_data_source}),
                )
                connection.execute(
                    market_data_bar_provenance_table.insert().prefix_with("OR REPLACE"),
                    {
                        "provenance_id": f"{ingest_run_id}:{canonical_bar_id}",
                        "ingest_run_id": ingest_run_id,
                        "bar_id": canonical_bar_id,
                        "data_source": canonical_data_source,
                        "provider": provenance.provider,
                        "dataset": provenance.dataset,
                        "schema_name": provenance.schema_name,
                        "internal_symbol": symbol,
                        "raw_symbol": provenance.raw_symbol,
                        "request_symbol": provenance.request_symbol,
                        "stype_in": provenance.stype_in,
                        "stype_out": provenance.stype_out,
                        "interval": provenance.interval,
                        "source_timestamp": bar.end_ts.isoformat(),
                        "ingest_time": provenance.ingest_time.isoformat(),
                        "coverage_start": provenance.coverage_start.isoformat() if provenance.coverage_start else None,
                        "coverage_end": provenance.coverage_end.isoformat() if provenance.coverage_end else None,
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
                            "raw_data_source": raw_data_source,
                            "raw_saved_count": raw_saved_count,
                            "canonical_saved_count": canonical_saved_count,
                            "canonical_skipped_existing_count": canonical_skipped_existing_count,
                            "provider_metadata": provider_metadata or {},
                        },
                        sort_keys=True,
                    ),
                },
            )

        audit = CanonicalMergeAudit(
            symbol=symbol,
            timeframe="1m",
            raw_data_source=raw_data_source,
            canonical_data_source=canonical_data_source,
            raw_saved_count=raw_saved_count,
            canonical_saved_count=canonical_saved_count,
            canonical_skipped_existing_count=canonical_skipped_existing_count,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            ingest_run_id=ingest_run_id,
        )
        report_path = self._report_dir / f"canonical_merge_{symbol.lower()}_{ingest_run_id}.json"
        report_path.write_text(json.dumps(asdict(audit), indent=2, sort_keys=True), encoding="utf-8")
        return replace(audit, report_path=str(report_path))

    def derive_timeframe(
        self,
        *,
        symbol: str,
        target_timeframe: str,
        start: datetime | None = None,
        end: datetime | None = None,
        source_data_source: str = "historical_1m_canonical",
    ) -> DerivedTimeframeAudit:
        source_bars = self._load_bars(
            symbol=symbol,
            timeframe="1m",
            data_source=source_data_source,
            start=start,
            end=end,
        )
        resampled_bars, skipped_bucket_count = _resample_finalized_whole_minute_bars(
            source_bars,
            target_timeframe=target_timeframe,
        )
        target_data_source = f"historical_{target_timeframe}_canonical"
        ingest_run_id = str(uuid4())
        ingest_time = datetime.now(UTC)
        ordered_bars = list(resampled_bars)
        with self._engine.begin() as connection:
            connection.execute(
                market_data_ingest_runs_table.insert().prefix_with("OR REPLACE"),
                {
                    "ingest_run_id": ingest_run_id,
                    "provider": "derived_from_canonical_1m",
                    "dataset": None,
                    "schema_name": f"derived_{target_timeframe}",
                    "request_symbol": symbol,
                    "internal_symbol": symbol,
                    "timeframe": target_timeframe,
                    "data_source": target_data_source,
                    "coverage_start": ordered_bars[0].start_ts.isoformat() if ordered_bars else None,
                    "coverage_end": ordered_bars[-1].end_ts.isoformat() if ordered_bars else None,
                    "ingest_started_at": ingest_time.isoformat(),
                    "ingest_completed_at": ingest_time.isoformat(),
                    "status": "running",
                    "payload_json": json.dumps(
                        {
                            "source_data_source": source_data_source,
                            "source_bar_count": len(source_bars),
                            "skipped_incomplete_buckets": skipped_bucket_count,
                        },
                        sort_keys=True,
                    ),
                },
            )
            for bar in ordered_bars:
                derived_bar = replace(bar, bar_id=_storage_bar_id(target_data_source, bar.bar_id))
                connection.execute(
                    bars_table.insert().prefix_with("OR REPLACE"),
                    _bar_insert_values(derived_bar, data_source=target_data_source, created_at=ingest_time),
                )
                connection.execute(
                    market_data_bar_provenance_table.insert().prefix_with("OR REPLACE"),
                    {
                        "provenance_id": f"{ingest_run_id}:{derived_bar.bar_id}",
                        "ingest_run_id": ingest_run_id,
                        "bar_id": derived_bar.bar_id,
                        "data_source": target_data_source,
                        "provider": "derived_from_canonical_1m",
                        "dataset": None,
                        "schema_name": f"derived_{target_timeframe}",
                        "internal_symbol": symbol,
                        "raw_symbol": symbol,
                        "request_symbol": symbol,
                        "stype_in": "internal",
                        "stype_out": "internal",
                        "interval": target_timeframe,
                        "source_timestamp": bar.end_ts.isoformat(),
                        "ingest_time": ingest_time.isoformat(),
                        "coverage_start": ordered_bars[0].start_ts.isoformat() if ordered_bars else None,
                        "coverage_end": ordered_bars[-1].end_ts.isoformat() if ordered_bars else None,
                        "provenance_tag": "derived_from_historical_1m_canonical",
                        "provider_metadata_json": json.dumps(
                            {
                                "source_data_source": source_data_source,
                                "source_bar_count": len(source_bars),
                                "skipped_incomplete_buckets": skipped_bucket_count,
                            },
                            sort_keys=True,
                        ),
                    },
                )
            connection.execute(
                market_data_ingest_runs_table.update()
                .where(market_data_ingest_runs_table.c.ingest_run_id == ingest_run_id),
                {
                    "status": "completed",
                },
            )

        audit = DerivedTimeframeAudit(
            symbol=symbol,
            source_timeframe="1m",
            target_timeframe=target_timeframe,
            source_data_source=source_data_source,
            target_data_source=target_data_source,
            source_bar_count=len(source_bars),
            derived_bar_count=len(ordered_bars),
            skipped_incomplete_buckets=skipped_bucket_count,
            coverage_start=ordered_bars[0].start_ts.isoformat() if ordered_bars else None,
            coverage_end=ordered_bars[-1].end_ts.isoformat() if ordered_bars else None,
            ingest_run_id=ingest_run_id,
        )
        report_path = self._report_dir / f"derived_{symbol.lower()}_{target_timeframe}_{ingest_run_id}.json"
        report_path.write_text(json.dumps(asdict(audit), indent=2, sort_keys=True), encoding="utf-8")
        return replace(audit, report_path=str(report_path))

    def audit_coverage(
        self,
        *,
        symbol: str,
        timeframe: str = "1m",
        data_source: str = "historical_1m_canonical",
    ) -> CanonicalCoverageAudit:
        with self._engine.begin() as connection:
            rows = connection.execute(
                select(bars_table.c.end_ts)
                .where(
                    bars_table.c.ticker == symbol,
                    bars_table.c.timeframe == timeframe,
                    bars_table.c.data_source == data_source,
                    bars_table.c.is_final.is_(True),
                )
                .order_by(bars_table.c.end_ts.asc())
            ).all()
        timestamps = [datetime.fromisoformat(str(row[0])) for row in rows]
        gap_ranges: list[GapRange] = []
        for left, right in zip(timestamps, timestamps[1:], strict=False):
            gap_minutes = int((right - left).total_seconds() // 60)
            if gap_minutes <= 1:
                continue
            expected_maintenance = _is_expected_maintenance_gap(left, right)
            if expected_maintenance:
                continue
            gap_ranges.append(
                GapRange(
                    start=(left + timedelta(minutes=1)).isoformat(),
                    end=right.isoformat(),
                    missing_minutes=gap_minutes - 1,
                    expected_maintenance_gap=False,
                )
            )
        audit = CanonicalCoverageAudit(
            symbol=symbol,
            timeframe=timeframe,
            data_source=data_source,
            bar_count=len(timestamps),
            earliest=timestamps[0].isoformat() if timestamps else None,
            latest=timestamps[-1].isoformat() if timestamps else None,
            gap_count=len(gap_ranges),
            gaps=gap_ranges,
        )
        report_path = self._report_dir / f"coverage_audit_{symbol.lower()}_{timeframe}_{data_source}.json"
        report_path.write_text(json.dumps(asdict(audit), indent=2, sort_keys=True), encoding="utf-8")
        return replace(audit, report_path=str(report_path))

    def backfill_detected_gaps(
        self,
        *,
        provider: MarketDataProvider,
        symbol: str,
        timeframe: str = "1m",
        data_source: str = "historical_1m_canonical",
    ) -> dict[str, Any]:
        audit = self.audit_coverage(symbol=symbol, timeframe=timeframe, data_source=data_source)
        ingestion = HistoricalMarketDataIngestionService(
            database_url=str(self._engine.url),
            provider_config_path=self._provider_config_path,
        )
        repairs = []
        for gap in audit.gaps:
            repairs.append(
                asdict(
                    ingestion.ingest(
                        provider=provider,
                        request=HistoricalBarsRequest(
                            internal_symbol=symbol,
                            timeframe=timeframe,
                            start=datetime.fromisoformat(gap.start),
                            end=datetime.fromisoformat(gap.end),
                        ),
                    )
                )
            )
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "data_source": data_source,
            "gap_count": audit.gap_count,
            "repairs": repairs,
            "coverage_report_path": audit.report_path,
        }

    def _load_bars(
        self,
        *,
        symbol: str,
        timeframe: str,
        data_source: str,
        start: datetime | None,
        end: datetime | None,
    ) -> list[Bar]:
        statement = (
            select(bars_table)
            .where(
                bars_table.c.ticker == symbol,
                bars_table.c.timeframe == timeframe,
                bars_table.c.data_source == data_source,
                bars_table.c.is_final.is_(True),
            )
            .order_by(bars_table.c.end_ts.asc(), bars_table.c.bar_id.asc())
        )
        if start is not None:
            statement = statement.where(bars_table.c.end_ts >= start.isoformat())
        if end is not None:
            statement = statement.where(bars_table.c.end_ts <= end.isoformat())
        with self._engine.begin() as connection:
            rows = connection.execute(statement).mappings().all()
        return [
            Bar(
                bar_id=str(row["bar_id"]),
                symbol=str(row["symbol"]),
                timeframe=str(row["timeframe"]),
                start_ts=datetime.fromisoformat(str(row["start_ts"])),
                end_ts=datetime.fromisoformat(str(row["end_ts"])),
                open=row["open"],
                high=row["high"],
                low=row["low"],
                close=row["close"],
                volume=int(row["volume"]),
                is_final=bool(row["is_final"]),
                session_asia=bool(row["session_asia"]),
                session_london=bool(row["session_london"]),
                session_us=bool(row["session_us"]),
                session_allowed=bool(row["session_allowed"]),
            )
            for row in rows
        ]


def _is_expected_maintenance_gap(left: datetime, right: datetime) -> bool:
    left_ny = left.astimezone(NEW_YORK)
    right_ny = right.astimezone(NEW_YORK)
    delta = right_ny - left_ny
    if delta >= timedelta(hours=40):
        return True
    if left_ny.time().hour == 17 and left_ny.time().minute == 0 and right_ny.time().hour == 18 and right_ny.time().minute == 0:
        return True
    return False


def _bar_insert_values(bar: Bar, *, data_source: str, created_at: datetime) -> dict[str, Any]:
    return {
        "bar_id": bar.bar_id,
        "instrument_id": None,
        "ticker": bar.symbol,
        "cusip": None,
        "asset_class": None,
        "data_source": data_source,
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
        "created_at": created_at.isoformat(),
    }


def _resample_finalized_whole_minute_bars(source_bars: Sequence[Bar], *, target_timeframe: str) -> tuple[list[Bar], int]:
    if not source_bars:
        return [], 0

    canonical_target = normalize_timeframe_label(target_timeframe)
    canonical_source = normalize_timeframe_label(source_bars[0].timeframe)
    source_minutes = timeframe_minutes(canonical_source)
    target_minutes = timeframe_minutes(canonical_target)
    if target_minutes <= source_minutes or target_minutes % source_minutes != 0:
        raise ValueError("target_timeframe must be a larger whole-minute multiple of the source timeframe.")

    ratio = target_minutes // source_minutes
    buckets: dict[int, list[Bar]] = {}
    for bar in source_bars:
        epoch_minutes = int(bar.end_ts.astimezone(UTC).timestamp() // 60)
        bucket_key = (epoch_minutes - 1) // target_minutes
        buckets.setdefault(bucket_key, []).append(bar)

    resampled: list[Bar] = []
    skipped_bucket_count = 0
    for key in sorted(buckets):
        bucket = sorted(buckets[key], key=lambda item: item.end_ts)
        if len(bucket) != ratio:
            skipped_bucket_count += 1
            continue
        expected_gap = timedelta(minutes=source_minutes)
        if any(right.end_ts - left.end_ts != expected_gap for left, right in zip(bucket, bucket[1:], strict=False)):
            skipped_bucket_count += 1
            continue
        first = bucket[0]
        last = bucket[-1]
        resampled.append(
            Bar(
                bar_id=build_bar_id(first.symbol, canonical_target, last.end_ts),
                symbol=first.symbol,
                timeframe=canonical_target,
                start_ts=first.start_ts,
                end_ts=last.end_ts,
                open=first.open,
                high=max(item.high for item in bucket),
                low=min(item.low for item in bucket),
                close=last.close,
                volume=sum(int(item.volume) for item in bucket),
                is_final=True,
                session_asia=all(bool(item.session_asia) for item in bucket),
                session_london=all(bool(item.session_london) for item in bucket),
                session_us=all(bool(item.session_us) for item in bucket),
                session_allowed=all(bool(item.session_allowed) for item in bucket),
            )
        )
    return resampled, skipped_bucket_count
