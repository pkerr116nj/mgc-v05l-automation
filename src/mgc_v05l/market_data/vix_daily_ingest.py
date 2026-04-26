"""Ingest official Cboe daily VIX history into the research warehouse."""

from __future__ import annotations

import csv
import io
import json
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from ..research.trend_participation.storage import materialize_parquet_dataset, write_storage_manifest
from ..research.warehouse_historical_evaluator.catalog import refresh_query_views
from ..research.warehouse_historical_evaluator.layout import build_layout


NEW_YORK = ZoneInfo("America/New_York")
DEFAULT_SOURCE = "cboe_official_daily_history"


def ingest_vix_daily(
    *,
    output_root: Path,
    config_path: Path,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    output_root = output_root.resolve()
    config_path = config_path.resolve()
    config = _load_vix_config(config_path)
    loaded_at = datetime.now(UTC).isoformat()
    csv_text = fetch_vix_history_csv(source_url=str(config["source_url"]))
    rows = parse_cboe_vix_history_csv(
        csv_text,
        start_date=start_date,
        end_date=end_date,
        asof_time_et=str(config["asof_time_et"]),
        source=DEFAULT_SOURCE,
        loaded_at=loaded_at,
    )

    layout = build_layout(output_root)
    artifact_path = layout["vix_daily"] / "vix_daily.parquet"
    materialize_parquet_dataset(artifact_path, rows)
    manifest_path = layout["manifests"] / "vix_daily_ingest_manifest.json"
    write_storage_manifest(
        manifest_path,
        {
            "module": "vix_daily_ingest",
            "source_url": str(config["source_url"]),
            "source": DEFAULT_SOURCE,
            "bucket_config_path": str(config_path),
            "row_count": len(rows),
            "artifact_path": str(artifact_path),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "loaded_at": loaded_at,
        },
    )
    _refresh_duckdb_views_if_available(layout)
    return {
        "artifact_path": str(artifact_path),
        "manifest_path": str(manifest_path),
        "row_count": len(rows),
        "source_url": str(config["source_url"]),
    }


def fetch_vix_history_csv(*, source_url: str, timeout_seconds: float = 30.0) -> str:
    request = Request(
        source_url,
        headers={
            "User-Agent": "MGC-v05l-automation/1.0",
            "Accept": "text/csv,text/plain,*/*",
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8")


def parse_cboe_vix_history_csv(
    csv_text: str,
    *,
    start_date: date,
    end_date: date,
    asof_time_et: str,
    source: str,
    loaded_at: str,
) -> list[dict[str, Any]]:
    reader = csv.DictReader(io.StringIO(csv_text))
    asof_parts = [int(part) for part in asof_time_et.split(":")]
    asof_clock = time(asof_parts[0], asof_parts[1], asof_parts[2] if len(asof_parts) > 2 else 0)

    date_key = None
    for candidate in ("DATE", "Date", "date", "Trade Date", "trade_date"):
        if candidate in (reader.fieldnames or []):
            date_key = candidate
            break
    if date_key is None:
        raise RuntimeError("Cboe VIX CSV is missing a recognizable date column.")

    open_key = _pick_key(reader.fieldnames, ("OPEN", "Open", "open"))
    high_key = _pick_key(reader.fieldnames, ("HIGH", "High", "high"))
    low_key = _pick_key(reader.fieldnames, ("LOW", "Low", "low"))
    close_key = _pick_key(reader.fieldnames, ("CLOSE", "Close", "close"))
    if close_key is None:
        raise RuntimeError("Cboe VIX CSV is missing a recognizable close column.")

    payload: list[dict[str, Any]] = []
    for raw_row in reader:
        trade_date = _parse_trade_date(str(raw_row[date_key]))
        if trade_date < start_date or trade_date > end_date:
            continue
        asof_ts = datetime.combine(trade_date, asof_clock, tzinfo=NEW_YORK).astimezone(UTC)
        payload.append(
            {
                "vix_trade_date": trade_date.isoformat(),
                "vix_asof_ts": asof_ts.isoformat(),
                "vix_open": _parse_optional_float(raw_row.get(open_key)) if open_key else None,
                "vix_high": _parse_optional_float(raw_row.get(high_key)) if high_key else None,
                "vix_low": _parse_optional_float(raw_row.get(low_key)) if low_key else None,
                "vix_close": float(raw_row[close_key]),
                "source": source,
                "loaded_at": loaded_at,
            }
        )
    payload.sort(key=lambda row: row["vix_trade_date"])
    return payload


def _load_vix_config(config_path: Path) -> dict[str, Any]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    return dict(payload["vix_daily"])


def _pick_key(fieldnames: list[str] | None, candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in (fieldnames or []):
            return candidate
    return None


def _parse_trade_date(value: str) -> date:
    normalized = value.strip()
    for pattern in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(normalized, pattern).date()
        except ValueError:
            continue
    raise RuntimeError(f"Unsupported VIX trade date format: {value!r}")


def _parse_optional_float(value: Any) -> float | None:
    if value in {None, "", "NA", "N/A"}:
        return None
    return float(value)


def _refresh_duckdb_views_if_available(layout: dict[str, Path]) -> None:
    if not layout["duckdb"].exists():
        return
    try:
        import duckdb  # type: ignore
    except ModuleNotFoundError:
        return
    connection = duckdb.connect(str(layout["duckdb"]))
    try:
        try:
            refresh_query_views(connection=connection, dataset_root=layout["root"])
        except Exception:
            # The VIX ingest can land before the derived vol_regime_daily dataset exists.
            pass
    finally:
        connection.close()
