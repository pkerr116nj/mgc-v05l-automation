"""Compact operator-facing market-data coverage and freshness report."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..market_data.timeframes import normalize_timeframe_label, timeframe_aliases, timeframe_minutes


@dataclass(frozen=True)
class MarketDataStatusRow:
    ticker: str
    symbol_role: str
    execution_symbol: str | None
    reference_symbol: str | None
    contract_family: str | None
    timeframe: str
    canonical_timeframe: str
    timeframe_aliases: list[str]
    data_source: str
    bar_count: int
    first_bar_ts: str | None
    last_bar_ts: str | None
    freshness_minutes: int | None
    expected_interval_minutes: int | None
    gap_count_over_2x_interval: int
    max_gap_minutes: int | None
    is_base_1m: bool
    is_native_surface: bool
    is_derived_surface: bool
    derived_from: str | None


def build_market_data_status_report(*, db_path: Path, symbol_config_path: Path | None = None) -> dict[str, Any]:
    symbol_metadata = _load_symbol_metadata(symbol_config_path)
    rows = _load_rows(db_path, symbol_metadata=symbol_metadata)
    by_symbol: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol_metadata_row = symbol_metadata.get(row.ticker, {})
        symbol = by_symbol.setdefault(
            row.ticker,
            {
                "ticker": row.ticker,
                "symbol_role": symbol_metadata_row.get("symbol_role", "unclassified"),
                "execution_symbol": symbol_metadata_row.get("execution_symbol"),
                "reference_symbol": symbol_metadata_row.get("reference_symbol"),
                "contract_family": symbol_metadata_row.get("contract_family"),
                "base_1m_available": False,
                "native_timeframes": [],
                "derived_timeframes": [],
                "timeframes": [],
                "coverage_modes": {
                    "baseline_parity_mode": False,
                    "research_execution_mode": False,
                },
                "notes": [],
            },
        )
        symbol["timeframes"].append(
            {
                "timeframe": row.timeframe,
                "symbol_role": row.symbol_role,
                "execution_symbol": row.execution_symbol,
                "reference_symbol": row.reference_symbol,
                "contract_family": row.contract_family,
                "canonical_timeframe": row.canonical_timeframe,
                "timeframe_aliases": row.timeframe_aliases,
                "data_source": row.data_source,
                "bar_count": row.bar_count,
                "first_bar_ts": row.first_bar_ts,
                "last_bar_ts": row.last_bar_ts,
                "freshness_minutes": row.freshness_minutes,
                "gap_count_over_2x_interval": row.gap_count_over_2x_interval,
                "max_gap_minutes": row.max_gap_minutes,
                "is_native_surface": row.is_native_surface,
                "is_derived_surface": row.is_derived_surface,
            }
        )
        if row.timeframe == "1m" and row.data_source in {"schwab_history", "historical_1m_canonical"}:
            symbol["base_1m_available"] = True
        if row.is_native_surface and row.canonical_timeframe not in symbol["native_timeframes"]:
            symbol["native_timeframes"].append(row.canonical_timeframe)
        if row.is_derived_surface and row.canonical_timeframe not in symbol["derived_timeframes"]:
            symbol["derived_timeframes"].append(row.canonical_timeframe)
        if row.gap_count_over_2x_interval > 0:
            symbol["notes"].append(
                f"{row.timeframe}/{row.data_source} has {row.gap_count_over_2x_interval} gaps over 2x expected interval"
            )
    for symbol in by_symbol.values():
        if not symbol["base_1m_available"]:
            symbol["notes"].append("research execution base layer missing: stored canonical 1m history unavailable")
        if "3m" not in symbol["derived_timeframes"]:
            symbol["notes"].append("3m derived surface not available")
        if "5m" not in symbol["native_timeframes"] and "5m" not in symbol["derived_timeframes"]:
            symbol["notes"].append("legacy benchmark 5m surface not available")
        if "10m" not in symbol["derived_timeframes"] and "10m" not in symbol["native_timeframes"]:
            symbol["notes"].append("10m derived surface not available")
        symbol["coverage_modes"]["research_execution_mode"] = bool(symbol["base_1m_available"])
        available_timeframes = {
            str(item.get("canonical_timeframe") or item.get("timeframe") or "")
            for item in symbol["timeframes"]
        }
        symbol["coverage_modes"]["baseline_parity_mode"] = bool(
            "5m" in symbol["native_timeframes"] or "5m" in symbol["derived_timeframes"] or "5m" in available_timeframes
        )
        symbol["native_timeframes"].sort(key=timeframe_minutes)
        symbol["derived_timeframes"].sort(key=timeframe_minutes)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_path": str(db_path),
        "mode_aware_assumptions": {
            "baseline_parity_mode": {
                "structural_signal_timeframe": "5m",
                "execution_timeframe": "5m",
                "artifact_timeframe": "5m",
            },
            "research_execution_mode": {
                "structural_signal_timeframe": "strategy-specific",
                "execution_timeframe": "1m_or_strategy_specific",
                "artifact_timeframe": "study-specific",
                "minimum_execution_base_layer": "1m_schwab_history",
                "preferred_preserved_base_layer": "historical_1m_canonical",
            },
        },
        "rows": [row.__dict__ for row in rows],
        "symbols": list(by_symbol.values()),
    }


def write_market_data_status_json(report: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def write_market_data_status_csv(report: dict[str, Any], output_path: Path) -> Path:
    rows = report["rows"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "ticker",
                "symbol_role",
                "execution_symbol",
                "reference_symbol",
                "contract_family",
                "timeframe",
                "canonical_timeframe",
                "timeframe_aliases",
                "data_source",
                "bar_count",
                "first_bar_ts",
                "last_bar_ts",
                "freshness_minutes",
                "expected_interval_minutes",
                "gap_count_over_2x_interval",
                "max_gap_minutes",
                "is_base_1m",
                "is_native_surface",
                "is_derived_surface",
                "derived_from",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    return output_path


def _load_rows(db_path: Path, *, symbol_metadata: dict[str, dict[str, Any]]) -> list[MarketDataStatusRow]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        identity_rows = connection.execute(
            """
            select ticker, timeframe, data_source, count(*) as bar_count, min(end_ts) as first_bar_ts, max(end_ts) as last_bar_ts
            from bars
            group by ticker, timeframe, data_source
            order by ticker, timeframe, data_source
            """
        ).fetchall()
        results: list[MarketDataStatusRow] = []
        for identity in identity_rows:
            ticker = identity["ticker"]
            metadata = symbol_metadata.get(ticker, {})
            timeframe = identity["timeframe"]
            canonical_timeframe = normalize_timeframe_label(timeframe)
            data_source = identity["data_source"]
            timestamps = [
                datetime.fromisoformat(row[0])
                for row in connection.execute(
                    """
                    select end_ts
                    from bars
                    where ticker = ? and timeframe = ? and data_source = ?
                    order by end_ts asc
                    """,
                    (ticker, timeframe, data_source),
                ).fetchall()
            ]
            expected = _expected_interval_minutes(canonical_timeframe)
            gap_count = 0
            max_gap_minutes = None
            if expected is not None and len(timestamps) > 1:
                gaps = [
                    int((right - left).total_seconds() // 60)
                    for left, right in zip(timestamps, timestamps[1:])
                ]
                over = [gap for gap in gaps if gap > expected * 2]
                gap_count = len(over)
                max_gap_minutes = max(gaps) if gaps else None
            last_bar_ts = identity["last_bar_ts"]
            freshness_minutes = None
            if last_bar_ts is not None:
                freshness_delta = datetime.now(timezone.utc) - datetime.fromisoformat(last_bar_ts).astimezone(timezone.utc)
                freshness_minutes = int(freshness_delta.total_seconds() // 60)
            results.append(
                MarketDataStatusRow(
                    ticker=ticker,
                    symbol_role=str(metadata.get("symbol_role", "unclassified")),
                    execution_symbol=_optional_string(metadata.get("execution_symbol")),
                    reference_symbol=_optional_string(metadata.get("reference_symbol")),
                    contract_family=_optional_string(metadata.get("contract_family")),
                    timeframe=timeframe,
                    canonical_timeframe=canonical_timeframe,
                    timeframe_aliases=timeframe_aliases(canonical_timeframe),
                    data_source=data_source,
                    bar_count=int(identity["bar_count"]),
                    first_bar_ts=identity["first_bar_ts"],
                    last_bar_ts=last_bar_ts,
                    freshness_minutes=freshness_minutes,
                    expected_interval_minutes=expected,
                    gap_count_over_2x_interval=gap_count,
                    max_gap_minutes=max_gap_minutes,
                    is_base_1m=(canonical_timeframe == "1m" and data_source in {"schwab_history", "historical_1m_canonical"}),
                    is_native_surface=(data_source in {"schwab_history", "historical_1m_canonical"}),
                    is_derived_surface=data_source.startswith("resampled_"),
                    derived_from=_derived_from(data_source),
                )
            )
        return results
    finally:
        connection.close()


def _expected_interval_minutes(timeframe: str) -> int | None:
    try:
        return timeframe_minutes(timeframe)
    except ValueError:
        return None


def _derived_from(data_source: str) -> str | None:
    if data_source.startswith("resampled_") and "_to_" in data_source:
        raw = data_source.removeprefix("resampled_").split("_to_", 1)[0]
        try:
            return normalize_timeframe_label(raw)
        except ValueError:
            return raw
    return None


def _load_symbol_metadata(symbol_config_path: Path | None) -> dict[str, dict[str, Any]]:
    if symbol_config_path is None or not symbol_config_path.exists():
        return {}
    payload = json.loads(symbol_config_path.read_text(encoding="utf-8"))
    raw_metadata = payload.get("symbol_metadata", {})
    return {
        str(symbol): dict(metadata)
        for symbol, metadata in raw_metadata.items()
        if isinstance(metadata, dict)
    }


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", required=True, type=Path)
    parser.add_argument("--symbol-config", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, default=None)
    parser.add_argument("--output-csv", type=Path, default=None)
    args = parser.parse_args()

    report = build_market_data_status_report(db_path=args.db_path, symbol_config_path=args.symbol_config)
    if args.output_json is not None:
        write_market_data_status_json(report, args.output_json)
    if args.output_csv is not None:
        write_market_data_status_csv(report, args.output_csv)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
