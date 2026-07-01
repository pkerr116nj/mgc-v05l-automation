"""Canonical candle providers for Track B research datasets.

Providers are research-only candle sources. They do not know about CRFD
features, GRE, VWAP, strategies, broker state, runtime, or Managed Exit.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


@dataclass(frozen=True)
class CanonicalResearchCandle:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float | None
    symbol: str
    timeframe: str
    source_ref: str


@dataclass(frozen=True)
class ResearchDataCoverage:
    symbol: str
    timeframe: str
    candle_count: int
    first_timestamp: datetime | None
    latest_timestamp: datetime | None


class CanonicalResearchDataProvider(Protocol):
    provider_id: str
    provider_kind: str

    def provider_metadata(self) -> dict[str, Any]:
        """Return descriptive provider metadata."""

    def available_symbols(self) -> tuple[str, ...]:
        """Return symbols that appear available from this provider."""

    def available_timeframes(self) -> tuple[str, ...]:
        """Return timeframes that appear available from this provider."""

    def load_candles(self, *, symbols: Sequence[str], timeframe: str) -> dict[str, tuple[CanonicalResearchCandle, ...]]:
        """Return canonical candles keyed by upper-case symbol."""

    def coverage(self, *, symbols: Sequence[str], timeframe: str) -> dict[str, ResearchDataCoverage]:
        """Return candle coverage keyed by upper-case symbol."""


@dataclass(frozen=True)
class RetainedPhase1ResearchDataProvider:
    output_root: Path
    provider_id: str = "retained"
    provider_kind: str = "retained_phase1_candles"

    def provider_metadata(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "provider_kind": self.provider_kind,
            "root": str(self.output_root / "phase1_runtime_market_data"),
            "read_only": True,
            "diagnostic_only": True,
        }

    def available_symbols(self) -> tuple[str, ...]:
        root = self.output_root / "phase1_runtime_market_data"
        if not root.exists():
            return ()
        return tuple(sorted(path.name.upper() for path in root.iterdir() if path.is_dir()))

    def available_timeframes(self) -> tuple[str, ...]:
        root = self.output_root / "phase1_runtime_market_data"
        values: set[str] = set()
        if root.exists():
            for symbol_dir in root.iterdir():
                if symbol_dir.is_dir():
                    values.update(path.name for path in symbol_dir.iterdir() if path.is_dir())
        return tuple(sorted(values))

    def load_candles(self, *, symbols: Sequence[str], timeframe: str) -> dict[str, tuple[CanonicalResearchCandle, ...]]:
        result: dict[str, tuple[CanonicalResearchCandle, ...]] = {}
        for symbol in _normalize_symbols(symbols):
            path = self.output_root / "phase1_runtime_market_data" / symbol / timeframe / "latest_runtime_candles.json"
            payload = _read_json(path)
            result[symbol] = _extract_phase1_bars(payload, symbol=symbol, timeframe=timeframe, source_ref=str(path))
        return result

    def coverage(self, *, symbols: Sequence[str], timeframe: str) -> dict[str, ResearchDataCoverage]:
        return _coverage_from_candles(self.load_candles(symbols=symbols, timeframe=timeframe), timeframe=timeframe)


@dataclass(frozen=True)
class HistoricalParquetResearchDataProvider:
    research_store_root: Path
    provider_id: str = "parquet"
    provider_kind: str = "historical_databento_parquet"

    @property
    def raw_bars_root(self) -> Path:
        return self.research_store_root / "raw_bars" / "databento_minute_backfill"

    def provider_metadata(self) -> dict[str, Any]:
        return {
            "provider_id": self.provider_id,
            "provider_kind": self.provider_kind,
            "root": str(self.raw_bars_root),
            "read_only": True,
            "diagnostic_only": True,
        }

    def available_symbols(self) -> tuple[str, ...]:
        if not self.raw_bars_root.exists():
            return ()
        prefix = "symbol="
        return tuple(
            sorted(path.name[len(prefix) :].upper() for path in self.raw_bars_root.iterdir() if path.is_dir() and path.name.startswith(prefix))
        )

    def available_timeframes(self) -> tuple[str, ...]:
        return ("1m",) if self.raw_bars_root.exists() else ()

    def load_candles(self, *, symbols: Sequence[str], timeframe: str) -> dict[str, tuple[CanonicalResearchCandle, ...]]:
        if timeframe != "1m":
            return {symbol: () for symbol in _normalize_symbols(symbols)}
        result: dict[str, tuple[CanonicalResearchCandle, ...]] = {}
        for symbol in _normalize_symbols(symbols):
            paths = sorted((self.raw_bars_root / f"symbol={symbol}").glob("year=*/month=*/bars.parquet"))
            candles: list[CanonicalResearchCandle] = []
            for path in paths:
                candles.extend(_read_parquet_candles(path, symbol=symbol, timeframe=timeframe))
            candles.sort(key=lambda row: row.timestamp)
            deduped = {row.timestamp.isoformat(): row for row in candles}
            result[symbol] = tuple(deduped[key] for key in sorted(deduped))
        return result

    def coverage(self, *, symbols: Sequence[str], timeframe: str) -> dict[str, ResearchDataCoverage]:
        return _coverage_from_candles(self.load_candles(symbols=symbols, timeframe=timeframe), timeframe=timeframe)


def build_research_data_provider(
    provider: str,
    *,
    output_root: Path,
    research_store_root: Path | None = None,
) -> CanonicalResearchDataProvider:
    provider_id = str(provider).strip().lower()
    if provider_id == "retained":
        return RetainedPhase1ResearchDataProvider(output_root=output_root)
    if provider_id == "parquet":
        return HistoricalParquetResearchDataProvider(research_store_root=research_store_root or Path("outputs/reports/trend_participation_engine"))
    raise ValueError(f"Unsupported research data provider: {provider!r}; expected one of ['retained', 'parquet']")


def _read_parquet_candles(path: Path, *, symbol: str, timeframe: str) -> list[CanonicalResearchCandle]:
    try:
        import pyarrow.parquet as pq  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError("Historical Parquet provider requires pyarrow in the repo environment.") from exc
    table = pq.ParquetFile(path).read()
    rows = table.to_pylist()
    candles: list[CanonicalResearchCandle] = []
    for row in rows:
        candle = _canonical_from_mapping(row, symbol=symbol, timeframe=timeframe, source_ref=str(path))
        if candle is not None:
            candles.append(candle)
    return candles


def _extract_phase1_bars(payload: Mapping[str, Any], *, symbol: str, timeframe: str, source_ref: str) -> tuple[CanonicalResearchCandle, ...]:
    rows = payload.get("bars") or payload.get("candles") or []
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return ()
    candles = [_canonical_from_mapping(row, symbol=symbol, timeframe=timeframe, source_ref=source_ref) for row in rows if isinstance(row, Mapping)]
    return tuple(row for row in candles if row is not None)


def _canonical_from_mapping(row: Mapping[str, Any], *, symbol: str, timeframe: str, source_ref: str) -> CanonicalResearchCandle | None:
    timestamp = _parse_datetime(row.get("bar_end") or row.get("timestamp") or row.get("ts") or row.get("bar_start") or row.get("bar_ts"))
    if timestamp is None:
        return None
    try:
        return CanonicalResearchCandle(
            timestamp=timestamp,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=_optional_float(row.get("volume")),
            symbol=symbol,
            timeframe=timeframe,
            source_ref=source_ref,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _coverage_from_candles(
    candles_by_symbol: Mapping[str, Sequence[CanonicalResearchCandle]],
    *,
    timeframe: str,
) -> dict[str, ResearchDataCoverage]:
    result: dict[str, ResearchDataCoverage] = {}
    for symbol, candles in candles_by_symbol.items():
        timestamps = [row.timestamp for row in candles]
        result[symbol] = ResearchDataCoverage(
            symbol=symbol,
            timeframe=timeframe,
            candle_count=len(timestamps),
            first_timestamp=min(timestamps) if timestamps else None,
            latest_timestamp=max(timestamps) if timestamps else None,
        )
    return result


def canonical_candles_as_mappings(
    candles_by_symbol: Mapping[str, Sequence[CanonicalResearchCandle]],
) -> dict[str, tuple[dict[str, Any], ...]]:
    return {
        symbol: tuple(
            {
                "timestamp": row.timestamp,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "volume": row.volume,
                "source_ref": row.source_ref,
            }
            for row in rows
        )
        for symbol, rows in candles_by_symbol.items()
    }


def _normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()))


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json_loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def json_loads(text: str) -> dict[str, Any]:
    import json

    payload = json.loads(text)
    return payload if isinstance(payload, dict) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    else:
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
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
