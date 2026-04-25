"""Databento historical provider for deep replay/research backfills."""

from __future__ import annotations

import base64
import json
import math
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from ..config_models import StrategySettings
from ..domain.models import Bar
from .bar_builder import BarBuilder
from .bar_models import build_bar_id
from .provider_config import DatabentoProviderConfig, load_market_data_providers_config
from .provider_interfaces import MarketDataProvider
from .provider_models import (
    HistoricalBarProvenance,
    HistoricalBarsBatch,
    HistoricalBarsRequest,
    HistoricalBarsResult,
    HistoricalBarsStage,
    QuoteSnapshot,
    TradePrint,
)
from .timeframes import normalize_timeframe_label, timeframe_minutes


class DatabentoHttpError(RuntimeError):
    """Raised when the Databento HTTP layer fails."""


class DatabentoTransport(Protocol):
    def request_text(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> str:
        """Execute a Databento request and return the decoded response body."""

    def request_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        """Execute a Databento request and decode the JSON response body."""

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, Any]) -> list[str]:
        """Execute a Databento request and return decoded lines."""

    def download_to_file(self, *, url: str, headers: dict[str, str], form: dict[str, Any], destination: Path) -> dict[str, Any]:
        """Execute a Databento request and stream the decoded response into a local file."""

    def download_url_to_file(self, *, url: str, headers: dict[str, str], destination: Path) -> dict[str, Any]:
        """Download a Databento-hosted artifact directly to a local file."""


class UrllibDatabentoTransport:
    """Small stdlib-only transport for Databento historical requests."""

    def __init__(self, timeout_seconds: int = 60) -> None:
        self._timeout_seconds = timeout_seconds

    def request_text(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> str:
        request_url = _build_request_url(url=url, query=query)
        body = None
        if form is not None:
            body = urlencode({key: _encode_form_value(value) for key, value in form.items()}).encode("utf-8")
        request = Request(url=request_url, method=method.upper(), headers=headers, data=body)
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response:
                return response.read().decode("utf-8")
        except HTTPError as exc:  # pragma: no cover - exercised in integration only
            detail = exc.read().decode("utf-8", errors="replace")
            raise DatabentoHttpError(f"Databento HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised in integration only
            raise DatabentoHttpError(f"Databento transport error: {exc}") from exc

    def request_json(
        self,
        *,
        url: str,
        headers: dict[str, str],
        method: str = "POST",
        form: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        return json.loads(
            self.request_text(
                url=url,
                headers=headers,
                method=method,
                form=form,
                query=query,
            )
        )

    def request_lines(self, *, url: str, headers: dict[str, str], form: dict[str, Any]) -> list[str]:
        payload = self.request_text(url=url, headers=headers, method="POST", form=form)
        return [line for line in payload.splitlines() if line.strip()]

    def download_to_file(self, *, url: str, headers: dict[str, str], form: dict[str, Any], destination: Path) -> dict[str, Any]:
        request = Request(
            url=url,
            method="POST",
            headers=headers,
            data=urlencode({key: _encode_form_value(value) for key, value in form.items()}).encode("utf-8"),
        )
        byte_count = 0
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response, destination.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 128)
                    if not chunk:
                        break
                    handle.write(chunk)
                    byte_count += len(chunk)
        except HTTPError as exc:  # pragma: no cover - exercised in integration only
            detail = exc.read().decode("utf-8", errors="replace")
            raise DatabentoHttpError(f"Databento HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised in integration only
            raise DatabentoHttpError(f"Databento transport error: {exc}") from exc
        return {"byte_count": byte_count}

    def download_url_to_file(self, *, url: str, headers: dict[str, str], destination: Path) -> dict[str, Any]:
        request = Request(url=url, method="GET", headers=headers)
        byte_count = 0
        try:
            with urlopen(request, timeout=self._timeout_seconds) as response, destination.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 128)
                    if not chunk:
                        break
                    handle.write(chunk)
                    byte_count += len(chunk)
        except HTTPError as exc:  # pragma: no cover - exercised in integration only
            detail = exc.read().decode("utf-8", errors="replace")
            raise DatabentoHttpError(f"Databento HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised in integration only
            raise DatabentoHttpError(f"Databento transport error: {exc}") from exc
        return {"byte_count": byte_count}


@dataclass(frozen=True)
class DatabentoHistoricalHttpClient:
    api_key: str
    base_url: str
    transport: DatabentoTransport

    def get_range_json_lines(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
        stype_out: str,
        encoding: str,
        compression: str,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        limit: int | None,
    ) -> list[dict[str, Any]]:
        headers = {
            "Accept": "application/x-ndjson",
            "Authorization": _basic_auth_header(self.api_key),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form: dict[str, Any] = {
            "dataset": dataset,
            "symbols": request_symbol,
            "schema": schema_name,
            "start": start.astimezone(UTC).isoformat(),
            "stype_in": stype_in,
            "stype_out": stype_out,
            "encoding": encoding,
            "compression": compression,
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
        }
        if end is not None:
            form["end"] = end.astimezone(UTC).isoformat()
        if limit is not None:
            form["limit"] = int(limit)
        lines = self.transport.request_lines(
            url=f"{self.base_url.rstrip('/')}/timeseries.get_range",
            headers=headers,
            form=form,
        )
        return [json.loads(line) for line in lines]

    def download_range_to_file(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
        stype_out: str,
        encoding: str,
        compression: str,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        limit: int | None,
        destination: Path,
    ) -> dict[str, Any]:
        headers = {
            "Accept": "application/x-ndjson",
            "Authorization": _basic_auth_header(self.api_key),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form: dict[str, Any] = {
            "dataset": dataset,
            "symbols": request_symbol,
            "schema": schema_name,
            "start": start.astimezone(UTC).isoformat(),
            "stype_in": stype_in,
            "stype_out": stype_out,
            "encoding": encoding,
            "compression": compression,
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
        }
        if end is not None:
            form["end"] = end.astimezone(UTC).isoformat()
        if limit is not None:
            form["limit"] = int(limit)
        if hasattr(self.transport, "download_to_file"):
            return self.transport.download_to_file(
                url=f"{self.base_url.rstrip('/')}/timeseries.get_range",
                headers=headers,
                form=form,
                destination=destination,
            )
        lines = self.transport.request_lines(
            url=f"{self.base_url.rstrip('/')}/timeseries.get_range",
            headers=headers,
            form=form,
        )
        payload = "\n".join(lines)
        destination.write_text(f"{payload}\n" if payload else "", encoding="utf-8")
        return {"byte_count": destination.stat().st_size}

    def get_billable_size(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
    ) -> int | None:
        headers = {
            "Authorization": _basic_auth_header(self.api_key),
        }
        query: dict[str, Any] = {
            "dataset": dataset,
            "symbols": request_symbol,
            "schema": schema_name,
            "start": start.astimezone(UTC).isoformat(),
            "stype_in": stype_in,
        }
        if end is not None:
            query["end"] = end.astimezone(UTC).isoformat()
        text = self._request_text(
            method="GET",
            url=f"{self.base_url.rstrip('/')}/metadata.get_billable_size",
            headers=headers,
            query=query,
        ).strip()
        if not text:
            return None
        try:
            return int(text)
        except ValueError:
            payload = json.loads(text)
            if isinstance(payload, dict):
                value = payload.get("size") or payload.get("billable_size") or payload.get("result")
                if value is not None:
                    return int(value)
            raise DatabentoHttpError(f"Unexpected get_billable_size response: {text[:200]}")

    def submit_batch_job(
        self,
        *,
        dataset: str,
        request_symbol: str,
        schema_name: str,
        start: datetime,
        end: datetime | None,
        stype_in: str,
        stype_out: str,
        encoding: str,
        compression: str,
        pretty_px: bool,
        pretty_ts: bool,
        map_symbols: bool,
        split_duration: str | None,
        limit: int | None,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": _basic_auth_header(self.api_key),
            "Content-Type": "application/x-www-form-urlencoded",
        }
        form: dict[str, Any] = {
            "dataset": dataset,
            "symbols": request_symbol,
            "schema": schema_name,
            "start": start.astimezone(UTC).isoformat(),
            "stype_in": stype_in,
            "stype_out": stype_out,
            "encoding": encoding,
            "compression": compression,
            "pretty_px": pretty_px,
            "pretty_ts": pretty_ts,
            "map_symbols": map_symbols,
        }
        if end is not None:
            form["end"] = end.astimezone(UTC).isoformat()
        if split_duration:
            form["split_duration"] = split_duration
        if limit is not None:
            form["limit"] = int(limit)
        payload = self._request_json(
            method="POST",
            url=f"{self.base_url.rstrip('/')}/batch.submit_job",
            headers=headers,
            form=form,
        )
        if not isinstance(payload, dict):
            raise DatabentoHttpError("Unexpected batch.submit_job response payload.")
        return payload

    def list_batch_jobs(self, *, states: Sequence[str] | None = None) -> list[dict[str, Any]]:
        headers = {
            "Authorization": _basic_auth_header(self.api_key),
        }
        query: dict[str, Any] = {}
        if states:
            query["states"] = ",".join(str(state) for state in states)
        payload = self._request_json(
            method="GET",
            url=f"{self.base_url.rstrip('/')}/batch.list_jobs",
            headers=headers,
            query=query or None,
        )
        if not isinstance(payload, list):
            raise DatabentoHttpError("Unexpected batch.list_jobs response payload.")
        return [dict(item) for item in payload]

    def list_batch_files(self, *, job_id: str) -> list[dict[str, Any]]:
        headers = {
            "Authorization": _basic_auth_header(self.api_key),
        }
        payload = self._request_json(
            method="GET",
            url=f"{self.base_url.rstrip('/')}/batch.list_files",
            headers=headers,
            query={"job_id": job_id},
        )
        if not isinstance(payload, list):
            raise DatabentoHttpError("Unexpected batch.list_files response payload.")
        return [dict(item) for item in payload]

    def download_batch_file_to_file(
        self,
        *,
        url: str,
        destination: Path,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": _basic_auth_header(self.api_key),
        }
        if hasattr(self.transport, "download_url_to_file"):
            return self.transport.download_url_to_file(
                url=url,
                headers=headers,
                destination=destination,
            )
        payload = self._request_text(method="GET", url=url, headers=headers)
        destination.write_text(payload, encoding="utf-8")
        return {"byte_count": destination.stat().st_size}

    def _request_text(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        form: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> str:
        if hasattr(self.transport, "request_text"):
            return self.transport.request_text(
                url=url,
                headers=headers,
                method=method,
                form=form,
                query=query,
            )
        if method.upper() == "POST" and form is not None:
            return "\n".join(self.transport.request_lines(url=url, headers=headers, form=form))
        raise DatabentoHttpError(f"Configured Databento transport does not support method={method} url={url}.")

    def _request_json(
        self,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        form: dict[str, Any] | None = None,
        query: dict[str, Any] | None = None,
    ) -> Any:
        if hasattr(self.transport, "request_json"):
            return self.transport.request_json(
                url=url,
                headers=headers,
                method=method,
                form=form,
                query=query,
            )
        return json.loads(
            self._request_text(
                method=method,
                url=url,
                headers=headers,
                form=form,
                query=query,
            )
        )


class DatabentoMarketDataProvider(MarketDataProvider):
    """Provider implementation for Databento historical bars."""

    provider_id = "databento"
    _staged_parse_batch_size = 5_000
    _small_request_max_bytes = 100 * 1024 * 1024
    _large_request_min_bytes = 5 * 1024 * 1024 * 1024
    _medium_chunk_target_bytes = 512 * 1024 * 1024
    _batch_poll_interval_seconds = 2.0
    _batch_poll_attempts = 120

    def __init__(
        self,
        settings: StrategySettings,
        *,
        repo_root: Path | None = None,
        config_path: str | Path | None = None,
        api_key: str | None = None,
        client: DatabentoHistoricalHttpClient | None = None,
    ) -> None:
        self._settings = settings
        self._repo_root = (repo_root or Path.cwd()).resolve(strict=False)
        self._providers_config = load_market_data_providers_config(config_path)
        self._config: DatabentoProviderConfig = self._providers_config.databento
        self._bar_builder = BarBuilder(settings)
        resolved_api_key = api_key or __import__("os").environ.get(self._config.api_key_env)
        self._api_key = str(resolved_api_key or "").strip()
        self._client = client or DatabentoHistoricalHttpClient(
            api_key=self._api_key,
            base_url=self._config.historical_base_url,
            transport=UrllibDatabentoTransport(),
        )

    def fetch_historical_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsResult:
        if not self._api_key:
            raise RuntimeError(
                f"Databento historical access requires {self._config.api_key_env} to be set in the environment."
            )
        normalized_timeframe = normalize_timeframe_label(request.timeframe)
        symbol_config = self._config.pilot_symbols.get(request.internal_symbol)
        if symbol_config is None:
            raise ValueError(f"No Databento pilot symbol mapping configured for {request.internal_symbol!r}.")
        schema_name = symbol_config.schema_by_timeframe.get(normalized_timeframe)
        if schema_name is None:
            raise ValueError(
                f"No Databento schema is configured for {request.internal_symbol!r} {normalized_timeframe!r}."
            )
        records = self._client.get_range_json_lines(
            dataset=symbol_config.dataset,
            request_symbol=symbol_config.request_symbol,
            schema_name=schema_name,
            start=request.start,
            end=request.end,
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            encoding=self._config.encoding,
            compression=self._config.compression,
            pretty_px=self._config.pretty_px,
            pretty_ts=self._config.pretty_ts,
            map_symbols=self._config.map_symbols,
            limit=request.limit,
        )
        ingest_time = datetime.now(UTC)
        bars: list[Bar] = []
        raw_symbols_by_bar_id: dict[str, str | None] = {}
        provider_metadata_by_bar_id: dict[str, dict[str, Any]] = {}
        interval = normalize_timeframe_label(request.timeframe)
        for record in records:
            if not _looks_like_ohlcv_record(record):
                continue
            header = _record_header(record)
            start_ts = _parse_timestamp(_record_timestamp(record), settings=self._settings)
            end_ts = start_ts + timedelta(minutes=timeframe_minutes(interval))
            bar = self._bar_builder.require_finalized(
                self._bar_builder.normalize(
                    Bar(
                        bar_id=build_bar_id(request.internal_symbol, interval, end_ts),
                        symbol=request.internal_symbol,
                        timeframe=interval,
                        start_ts=start_ts,
                        end_ts=end_ts,
                        open=Decimal(str(record["open"])),
                        high=Decimal(str(record["high"])),
                        low=Decimal(str(record["low"])),
                        close=Decimal(str(record["close"])),
                        volume=int(record.get("volume") or 0),
                        is_final=True,
                        session_asia=False,
                        session_london=False,
                        session_us=False,
                        session_allowed=False,
                    )
                )
            )
            bars.append(bar)
            raw_symbols_by_bar_id[bar.bar_id] = _record_raw_symbol(
                record,
                stype_out=symbol_config.stype_out,
                request_symbol=symbol_config.request_symbol,
            )
            provider_metadata_by_bar_id[bar.bar_id] = {
                "instrument_id": header.get("instrument_id"),
                "publisher_id": header.get("publisher_id"),
                "response_symbol": str(record.get("symbol") or "").strip() or None,
            }
        bars.sort(key=lambda item: item.end_ts)
        coverage_start = bars[0].start_ts if bars else None
        coverage_end = bars[-1].end_ts if bars else None
        data_source = self._config.canonical_data_source_by_timeframe.get(interval, f"databento_{interval}_canonical")
        provenance = {
            bar.bar_id: HistoricalBarProvenance(
                provider=self.provider_id,
                dataset=symbol_config.dataset,
                schema_name=schema_name,
                raw_symbol=raw_symbols_by_bar_id.get(bar.bar_id),
                stype_in=symbol_config.stype_in,
                stype_out=symbol_config.stype_out,
                interval=interval,
                ingest_time=ingest_time,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                provenance_tag=self._config.provenance_tag,
                request_symbol=symbol_config.request_symbol,
                provider_metadata=provider_metadata_by_bar_id.get(bar.bar_id) or {},
            )
            for bar in bars
        }
        return HistoricalBarsResult(
            provider=self.provider_id,
            data_source=data_source,
            internal_symbol=request.internal_symbol,
            timeframe=interval,
            bars=bars,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            ingest_time=ingest_time,
            dataset=symbol_config.dataset,
            schema_name=schema_name,
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            request_symbol=symbol_config.request_symbol,
            provenance_tag=self._config.provenance_tag,
            metadata={
                "record_count": len(bars),
                "description": symbol_config.description,
                "exchange": symbol_config.exchange,
                "api_base_url": self._config.historical_base_url,
            },
            bar_provenance=provenance,
        )

    def stage_historical_bars(self, request: HistoricalBarsRequest) -> HistoricalBarsStage:
        symbol_config, interval, schema_name = self._resolve_request_metadata(request)
        route_plan = self._plan_route(
            request=request,
            symbol_config=symbol_config,
            schema_name=schema_name,
        )
        staging_parent = (self._repo_root / "outputs" / "reports" / "market_data_ingest" / "staging").resolve(strict=False)
        staging_parent.mkdir(parents=True, exist_ok=True)
        staging_root = Path(
            mkdtemp(
                prefix=f"databento_stage_{request.internal_symbol.lower()}_{interval}_",
                dir=str(staging_parent),
            )
        )
        staged_paths: list[Path] = []
        artifact_rows: list[dict[str, Any]] = []
        try:
            if route_plan["route"] == "batch_submit_job":
                batch_rows = self._stage_batch_job(
                    staging_root=staging_root,
                    request=request,
                    symbol_config=symbol_config,
                    schema_name=schema_name,
                )
                for row in batch_rows:
                    staged_paths.append(Path(str(row["path"])))
                artifact_rows.extend(batch_rows)
            elif route_plan["route"] == "staged_get_range_chunked":
                chunk_rows = self._stage_chunked_range_downloads(
                    staging_root=staging_root,
                    request=request,
                    symbol_config=symbol_config,
                    schema_name=schema_name,
                    estimated_billable_bytes=route_plan["estimated_billable_bytes"],
                )
                for row in chunk_rows:
                    staged_paths.append(Path(str(row["path"])))
                artifact_rows.extend(chunk_rows)
            else:
                staged_path = staging_root / f"part_000_{request.internal_symbol.lower()}_{interval}.ndjson"
                download_metadata = self._client.download_range_to_file(
                    dataset=symbol_config.dataset,
                    request_symbol=symbol_config.request_symbol,
                    schema_name=schema_name,
                    start=request.start,
                    end=request.end,
                    stype_in=symbol_config.stype_in,
                    stype_out=symbol_config.stype_out,
                    encoding=self._config.encoding,
                    compression=self._config.compression,
                    pretty_px=self._config.pretty_px,
                    pretty_ts=self._config.pretty_ts,
                    map_symbols=self._config.map_symbols,
                    limit=request.limit,
                    destination=staged_path,
                )
                staged_paths.append(staged_path)
                artifact_rows.append(
                    {
                        "artifact_index": 0,
                        "path": str(staged_path),
                        "byte_count": int(download_metadata.get("byte_count") or 0),
                        "request_start": request.start.isoformat(),
                        "request_end": request.end.isoformat() if request.end is not None else None,
                        "route": route_plan["route"],
                        "status": "downloaded",
                    }
                )
        except Exception:
            for path in staged_paths:
                path.unlink(missing_ok=True)
            if staging_root.exists() and not any(staging_root.iterdir()):
                staging_root.rmdir()
            raise
        manifest_payload = {
            "requested_symbol": request.internal_symbol,
            "resolved_provider_symbol": symbol_config.request_symbol,
            "timeframe": interval,
            "request_start": request.start.isoformat(),
            "request_end": request.end.isoformat() if request.end is not None else None,
            "request_size_estimate_bytes": route_plan["estimated_billable_bytes"],
            "route": route_plan["route"],
            "fallback_route": route_plan["fallback_route"],
            "artifact_rows": artifact_rows,
        }
        manifest_path = staging_root / "stage_manifest.json"
        manifest_path.write_text(json.dumps(manifest_payload, indent=2, sort_keys=True), encoding="utf-8")
        return HistoricalBarsStage(
            provider=self.provider_id,
            data_source=self._config.canonical_data_source_by_timeframe.get(interval, f"databento_{interval}_canonical"),
            internal_symbol=request.internal_symbol,
            timeframe=interval,
            ingest_time=datetime.now(UTC),
            staged_path=str(staging_root),
            staged_artifact_paths=tuple(str(path) for path in staged_paths),
            dataset=symbol_config.dataset,
            schema_name=schema_name,
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            request_symbol=symbol_config.request_symbol,
            provenance_tag=self._config.provenance_tag,
            route=route_plan["route"],
            fallback_route=route_plan["fallback_route"],
            estimated_billable_bytes=route_plan["estimated_billable_bytes"],
            manifest_path=str(manifest_path),
            metadata={
                "description": symbol_config.description,
                "exchange": symbol_config.exchange,
                "api_base_url": self._config.historical_base_url,
                "artifact_rows": artifact_rows,
                "request_size_estimate_bytes": route_plan["estimated_billable_bytes"],
                "route": route_plan["route"],
                "fallback_route": route_plan["fallback_route"],
            },
        )

    def iter_staged_historical_bar_batches(
        self,
        stage: HistoricalBarsStage,
        *,
        request: HistoricalBarsRequest,
        batch_size: int | None = None,
    ):
        symbol_config, interval, _schema_name = self._resolve_request_metadata(request)
        resolved_batch_size = max(1, int(batch_size or self._staged_parse_batch_size))
        bars: list[Bar] = []
        provenance: dict[str, HistoricalBarProvenance] = {}
        staged_artifacts = list(stage.staged_artifact_paths or ((stage.staged_path,) if stage.staged_path else ()))
        for artifact_index, artifact_path_str in enumerate(staged_artifacts):
            staged_path = Path(artifact_path_str)
            with staged_path.open("r", encoding="utf-8") as handle:
                for raw_line in handle:
                    line = raw_line.strip()
                    if not line:
                        continue
                    record = json.loads(line)
                    if not _looks_like_ohlcv_record(record):
                        continue
                    bar, item_provenance = self._normalize_record(
                        request=request,
                        interval=interval,
                        record=record,
                        symbol_config=symbol_config,
                        schema_name=stage.schema_name or "",
                        ingest_time=stage.ingest_time,
                    )
                    bars.append(bar)
                    provenance[bar.bar_id] = item_provenance
                    if len(bars) >= resolved_batch_size:
                        yield HistoricalBarsBatch(
                            bars=list(bars),
                            bar_provenance=dict(provenance),
                            artifact_path=str(staged_path),
                            artifact_index=artifact_index,
                        )
                        bars.clear()
                        provenance.clear()
        if bars:
            yield HistoricalBarsBatch(
                bars=list(bars),
                bar_provenance=dict(provenance),
                artifact_path=staged_artifacts[-1] if staged_artifacts else None,
                artifact_index=(len(staged_artifacts) - 1) if staged_artifacts else None,
            )

    def fetch_quotes(self, internal_symbols: list[str] | tuple[str, ...]) -> list[QuoteSnapshot]:
        raise NotImplementedError("Databento live quotes are not wired in this pass.")

    def describe_symbol(self, internal_symbol: str) -> dict[str, Any]:
        symbol_config = self._config.pilot_symbols.get(internal_symbol)
        if symbol_config is None:
            raise ValueError(f"No Databento pilot symbol mapping configured for {internal_symbol!r}.")
        return symbol_config.model_dump(mode="json")

    def subscribe_live_quotes(self, internal_symbols: list[str] | tuple[str, ...]):
        raise NotImplementedError("Databento live streaming is reserved for a later pass.")

    def subscribe_live_trades(self, internal_symbols: list[str] | tuple[str, ...]) -> list[TradePrint]:
        raise NotImplementedError(
            "Databento live trade streaming is not wired in this repo yet. "
            "Use market-data-live-trade-capture with --input-jsonl for offline tick capture tests, "
            "or add the provider-specific live trade adapter first."
        )

    def _resolve_request_metadata(
        self, request: HistoricalBarsRequest
    ) -> tuple[Any, str, str]:
        if not self._api_key:
            raise RuntimeError(
                f"Databento historical access requires {self._config.api_key_env} to be set in the environment."
            )
        normalized_timeframe = normalize_timeframe_label(request.timeframe)
        symbol_config = self._config.pilot_symbols.get(request.internal_symbol)
        if symbol_config is None:
            raise ValueError(f"No Databento pilot symbol mapping configured for {request.internal_symbol!r}.")
        schema_name = symbol_config.schema_by_timeframe.get(normalized_timeframe)
        if schema_name is None:
            raise ValueError(
                f"No Databento schema is configured for {request.internal_symbol!r} {normalized_timeframe!r}."
            )
        return symbol_config, normalized_timeframe, schema_name

    def _normalize_record(
        self,
        *,
        request: HistoricalBarsRequest,
        interval: str,
        record: dict[str, Any],
        symbol_config: Any,
        schema_name: str,
        ingest_time: datetime,
    ) -> tuple[Bar, HistoricalBarProvenance]:
        header = _record_header(record)
        start_ts = _parse_timestamp(_record_timestamp(record), settings=self._settings)
        end_ts = start_ts + timedelta(minutes=timeframe_minutes(interval))
        bar = self._bar_builder.require_finalized(
            self._bar_builder.normalize(
                Bar(
                    bar_id=build_bar_id(request.internal_symbol, interval, end_ts),
                    symbol=request.internal_symbol,
                    timeframe=interval,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    open=Decimal(str(record["open"])),
                    high=Decimal(str(record["high"])),
                    low=Decimal(str(record["low"])),
                    close=Decimal(str(record["close"])),
                    volume=int(record.get("volume") or 0),
                    is_final=True,
                    session_asia=False,
                    session_london=False,
                    session_us=False,
                    session_allowed=False,
                )
            )
        )
        provenance = HistoricalBarProvenance(
            provider=self.provider_id,
            dataset=symbol_config.dataset,
            schema_name=schema_name,
            raw_symbol=_record_raw_symbol(
                record,
                stype_out=symbol_config.stype_out,
                request_symbol=symbol_config.request_symbol,
            ),
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            interval=interval,
            ingest_time=ingest_time,
            coverage_start=None,
            coverage_end=None,
            provenance_tag=self._config.provenance_tag,
            request_symbol=symbol_config.request_symbol,
            provider_metadata={
                "instrument_id": header.get("instrument_id"),
                "publisher_id": header.get("publisher_id"),
                "response_symbol": str(record.get("symbol") or "").strip() or None,
            },
        )
        return bar, provenance

    def _plan_route(
        self,
        *,
        request: HistoricalBarsRequest,
        symbol_config: Any,
        schema_name: str,
    ) -> dict[str, Any]:
        estimated_billable_bytes = self._client.get_billable_size(
            dataset=symbol_config.dataset,
            request_symbol=symbol_config.request_symbol,
            schema_name=schema_name,
            start=request.start,
            end=request.end,
            stype_in=symbol_config.stype_in,
        )
        if estimated_billable_bytes is None:
            return {
                "route": "staged_get_range",
                "fallback_route": None,
                "estimated_billable_bytes": None,
            }
        if estimated_billable_bytes <= self._small_request_max_bytes:
            return {
                "route": "staged_get_range",
                "fallback_route": None,
                "estimated_billable_bytes": estimated_billable_bytes,
            }
        if estimated_billable_bytes < self._large_request_min_bytes:
            return {
                "route": "staged_get_range_chunked",
                "fallback_route": "staged_get_range",
                "estimated_billable_bytes": estimated_billable_bytes,
            }
        return {
            "route": "batch_submit_job",
            "fallback_route": "staged_get_range_chunked",
            "estimated_billable_bytes": estimated_billable_bytes,
        }

    def _stage_chunked_range_downloads(
        self,
        *,
        staging_root: Path,
        request: HistoricalBarsRequest,
        symbol_config: Any,
        schema_name: str,
        estimated_billable_bytes: int | None,
    ) -> list[dict[str, Any]]:
        chunk_ranges = _split_request_into_chunks(
            start=request.start,
            end=request.end,
            estimated_billable_bytes=estimated_billable_bytes,
            target_chunk_bytes=self._medium_chunk_target_bytes,
        )
        artifact_rows: list[dict[str, Any]] = []
        for artifact_index, (chunk_start, chunk_end) in enumerate(chunk_ranges):
            artifact_path = staging_root / f"part_{artifact_index:03d}_{request.internal_symbol.lower()}_{request.timeframe}.ndjson"
            download_metadata = self._client.download_range_to_file(
                dataset=symbol_config.dataset,
                request_symbol=symbol_config.request_symbol,
                schema_name=schema_name,
                start=chunk_start,
                end=chunk_end,
                stype_in=symbol_config.stype_in,
                stype_out=symbol_config.stype_out,
                encoding=self._config.encoding,
                compression=self._config.compression,
                pretty_px=self._config.pretty_px,
                pretty_ts=self._config.pretty_ts,
                map_symbols=self._config.map_symbols,
                limit=request.limit,
                destination=artifact_path,
            )
            artifact_rows.append(
                {
                    "artifact_index": artifact_index,
                    "path": str(artifact_path),
                    "byte_count": int(download_metadata.get("byte_count") or 0),
                    "request_start": chunk_start.isoformat(),
                    "request_end": chunk_end.isoformat() if chunk_end is not None else None,
                    "route": "staged_get_range_chunked",
                    "status": "downloaded",
                }
            )
        return artifact_rows

    def _stage_batch_job(
        self,
        *,
        staging_root: Path,
        request: HistoricalBarsRequest,
        symbol_config: Any,
        schema_name: str,
    ) -> list[dict[str, Any]]:
        submitted = self._client.submit_batch_job(
            dataset=symbol_config.dataset,
            request_symbol=symbol_config.request_symbol,
            schema_name=schema_name,
            start=request.start,
            end=request.end,
            stype_in=symbol_config.stype_in,
            stype_out=symbol_config.stype_out,
            encoding=self._config.encoding,
            compression=self._config.compression,
            pretty_px=self._config.pretty_px,
            pretty_ts=self._config.pretty_ts,
            map_symbols=self._config.map_symbols,
            split_duration="month",
            limit=request.limit,
        )
        job_id = str(submitted.get("id") or "").strip()
        if not job_id:
            raise DatabentoHttpError("Databento batch.submit_job response did not include a job id.")
        for _attempt in range(self._batch_poll_attempts):
            job_rows = self._client.list_batch_jobs(states=("queued", "processing", "done", "expired"))
            job_row = next((row for row in job_rows if str(row.get("id") or "").strip() == job_id), None)
            if job_row is None:
                time.sleep(self._batch_poll_interval_seconds)
                continue
            state = str(job_row.get("state") or "").strip().lower()
            if state == "done":
                break
            if state == "expired":
                raise DatabentoHttpError(f"Databento batch job expired before download (job_id={job_id}).")
            time.sleep(self._batch_poll_interval_seconds)
        else:
            raise DatabentoHttpError(f"Databento batch job did not complete in time (job_id={job_id}).")

        file_rows = self._client.list_batch_files(job_id=job_id)
        artifact_rows: list[dict[str, Any]] = []
        artifact_index = 0
        for file_row in file_rows:
            filename = str(file_row.get("filename") or "").strip()
            if not _is_batch_data_file(filename):
                continue
            download_url = str(((file_row.get("urls") or {}) or {}).get("https") or "").strip()
            if not download_url:
                continue
            artifact_path = staging_root / filename
            download_metadata = self._client.download_batch_file_to_file(
                url=download_url,
                destination=artifact_path,
            )
            artifact_rows.append(
                {
                    "artifact_index": artifact_index,
                    "path": str(artifact_path),
                    "filename": filename,
                    "byte_count": int(download_metadata.get("byte_count") or file_row.get("size") or 0),
                    "request_start": request.start.isoformat(),
                    "request_end": request.end.isoformat() if request.end is not None else None,
                    "route": "batch_submit_job",
                    "status": "downloaded",
                    "job_id": job_id,
                }
            )
            artifact_index += 1
        if not artifact_rows:
            # Preserve an empty data artifact marker so the ingest manifest can explain the result.
            artifact_path = staging_root / "batch_empty.ndjson"
            artifact_path.write_text("", encoding="utf-8")
            artifact_rows.append(
                {
                    "artifact_index": 0,
                    "path": str(artifact_path),
                    "filename": "batch_empty.ndjson",
                    "byte_count": 0,
                    "request_start": request.start.isoformat(),
                    "request_end": request.end.isoformat() if request.end is not None else None,
                    "route": "batch_submit_job",
                    "status": "downloaded",
                    "job_id": job_id,
                }
            )
        return artifact_rows


def _basic_auth_header(api_key: str) -> str:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _encode_form_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _build_request_url(*, url: str, query: dict[str, Any] | None = None) -> str:
    if not query:
        return url
    encoded = urlencode({key: _encode_form_value(value) for key, value in query.items()})
    separator = "&" if urlparse(url).query else "?"
    return f"{url}{separator}{encoded}"


def _split_request_into_chunks(
    *,
    start: datetime,
    end: datetime | None,
    estimated_billable_bytes: int | None,
    target_chunk_bytes: int,
) -> list[tuple[datetime, datetime | None]]:
    if end is None or estimated_billable_bytes is None or estimated_billable_bytes <= target_chunk_bytes:
        return [(start, end)]
    total_seconds = max(1.0, (end - start).total_seconds())
    chunk_count = max(2, math.ceil(estimated_billable_bytes / target_chunk_bytes))
    chunk_seconds = max(24 * 60 * 60, math.ceil(total_seconds / chunk_count))
    ranges: list[tuple[datetime, datetime | None]] = []
    current_start = start
    while current_start < end:
        current_end = min(end, current_start + timedelta(seconds=chunk_seconds))
        ranges.append((current_start, current_end))
        if current_end <= current_start:
            break
        current_start = current_end
    return ranges or [(start, end)]


def _is_batch_data_file(filename: str) -> bool:
    normalized = filename.strip().lower()
    if not normalized:
        return False
    support_files = {"metadata.json", "manifest.json", "condition.json"}
    if normalized in support_files:
        return False
    if normalized.endswith(".symbology.json") or normalized.endswith(".symbology.csv"):
        return False
    return True


def _parse_timestamp(value: Any, *, settings: StrategySettings) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=settings.timezone_info)
    return parsed.astimezone(settings.timezone_info)


def _looks_like_ohlcv_record(record: dict[str, Any]) -> bool:
    required = {"ts_event", "open", "high", "low", "close", "volume"}
    if required.issubset(set(record)):
        return True
    header = record.get("hd")
    return isinstance(header, dict) and required.difference({"ts_event"}).issubset(set(record)) and "ts_event" in header


def _record_header(record: dict[str, Any]) -> dict[str, Any]:
    header = record.get("hd")
    if isinstance(header, dict):
        return header
    return record


def _record_timestamp(record: dict[str, Any]) -> Any:
    if "ts_event" in record:
        return record["ts_event"]
    header = _record_header(record)
    return header["ts_event"]


def _record_raw_symbol(record: dict[str, Any], *, stype_out: str, request_symbol: str) -> str | None:
    symbol = str(record.get("symbol") or "").strip() or None
    if symbol is None:
        return None
    if stype_out == "raw_symbol":
        return symbol
    if symbol != request_symbol:
        return symbol
    return None
