"""Acquire narrowly scoped inputs for the naive NDXP baseline.

Cost estimation is a separate command.  Quote downloads require an explicit
maximum accepted Databento cost and abort before downloading when exceeded.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import urllib.request
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo


NEW_YORK = ZoneInfo("America/New_York")
DATASET = "OPRA.PILLAR"
SCHEMA = "cbbo-1s"
DEFAULT_PARENT = "NDXP.OPT"


def weekday_sessions(start: date, end: date) -> list[date]:
    sessions = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    return sessions


def opening_window(session: date) -> tuple[datetime, datetime]:
    start = datetime.combine(session, time(9, 30), NEW_YORK)
    return start, start + timedelta(minutes=1)


def historical_client() -> Any:
    key = os.environ.get("DATABENTO_API_KEY", "").strip()
    if not key:
        raise RuntimeError("DATABENTO_API_KEY is not set; load the existing Mars environment file")
    try:
        import databento as db
    except ImportError as exc:
        raise RuntimeError("install the project's databento extra before using this command") from exc
    return db.Historical(key)


def estimate_cost(client: Any, sessions: Iterable[date], *, parent: str = DEFAULT_PARENT) -> tuple[float, list[dict[str, object]]]:
    total = 0.0
    details = []
    for session in sessions:
        start, end = opening_window(session)
        cost = float(
            client.metadata.get_cost(
                dataset=DATASET,
                schema=SCHEMA,
                stype_in="parent",
                symbols=[parent],
                start=start,
                end=end,
            )
        )
        total += cost
        details.append({"date": session.isoformat(), "cost": cost})
    return total, details


def parse_osi_symbol(symbol: str) -> tuple[str, date, str, float]:
    value = str(symbol)
    if len(value) < 15:
        raise ValueError(f"not an OSI option symbol: {symbol!r}")
    root = value[:-15].strip()
    expiry_text = value[-15:-9]
    option_type = value[-9]
    strike_text = value[-8:]
    if option_type not in {"C", "P"} or not expiry_text.isdigit() or not strike_text.isdigit():
        raise ValueError(f"not an OSI option symbol: {symbol!r}")
    expiration = datetime.strptime(expiry_text, "%y%m%d").date()
    return root, expiration, option_type, int(strike_text) / 1000.0


def _normalized_rows(frame: Any, session: date) -> list[dict[str, object]]:
    if frame.empty:
        return []
    indexed = frame.reset_index()
    timestamp_column = "ts_recv" if "ts_recv" in indexed.columns else indexed.columns[0]
    rows = []
    for record in indexed.to_dict(orient="records"):
        try:
            root, expiration, option_type, strike = parse_osi_symbol(record["symbol"])
            bid = float(record["bid_px_00"])
            ask = float(record["ask_px_00"])
            quote_time = record[timestamp_column]
            if hasattr(quote_time, "to_pydatetime"):
                quote_time = quote_time.to_pydatetime()
            quote_time = quote_time.astimezone(NEW_YORK)
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if expiration != session or option_type not in {"C", "P"}:
            continue
        if not math.isfinite(bid) or not math.isfinite(ask) or bid < 0 or ask <= 0 or bid > ask:
            continue
        rows.append(
            {
                "quote_time": quote_time.isoformat(),
                "expiration": expiration.isoformat(),
                "option_type": option_type,
                "strike": strike,
                "bid": bid,
                "ask": ask,
                "symbol": str(record["symbol"]),
                "spot": "",
                "root": root,
            }
        )
    return rows


def download_quotes(
    client: Any,
    sessions: Sequence[date],
    output: Path,
    cache_dir: Path,
    *,
    parent: str = DEFAULT_PARENT,
) -> int:
    try:
        import databento as db
    except ImportError as exc:
        raise RuntimeError("install the project's databento extra before using this command") from exc
    cache_dir.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    fields = ["quote_time", "expiration", "option_type", "strike", "bid", "ask", "symbol", "spot", "root"]
    count = 0
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for session in sessions:
            raw_path = cache_dir / f"{session.isoformat()}-{parent.replace('.', '_')}-{SCHEMA}.dbn.zst"
            if raw_path.exists():
                store = db.DBNStore.from_file(raw_path)
            else:
                start, end = opening_window(session)
                store = client.timeseries.get_range(
                    dataset=DATASET,
                    schema=SCHEMA,
                    stype_in="parent",
                    symbols=[parent],
                    start=start,
                    end=end,
                    path=raw_path,
                )
            rows = _normalized_rows(store.to_df(), session)
            writer.writerows(rows)
            count += len(rows)
    temporary.replace(output)
    return count


def download_settlements(start: date, end: date, output: Path) -> int:
    """Download the FRED distribution of Nasdaq's NASDAQXQC series."""

    url = (
        "https://fred.stlouisfed.org/graph/fredgraph.csv"
        f"?id=NASDAQXQC&cosd={start.isoformat()}&coed={end.isoformat()}"
    )
    request = urllib.request.Request(url, headers={"User-Agent": "mgc-v05l-ndxp-research/1.0"})
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - fixed HTTPS host
        text = response.read().decode("utf-8-sig")
    source_rows = list(csv.DictReader(text.splitlines()))
    rows = []
    for row in source_rows:
        value = row.get("NASDAQXQC")
        observed = row.get("observation_date") or row.get("DATE")
        if observed and value not in (None, "", "."):
            rows.append({"date": observed, "settlement": float(value), "source": "FRED NASDAQXQC / Nasdaq XQC"})
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "settlement", "source"])
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(output)
    return len(rows)


def _dates(args: argparse.Namespace) -> list[date]:
    return weekday_sessions(date.fromisoformat(args.start), date.fromisoformat(args.end))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("estimate", "download-quotes"):
        child = subparsers.add_parser(command)
        child.add_argument("--start", required=True)
        child.add_argument("--end", required=True)
        child.add_argument("--parent", default=DEFAULT_PARENT)
    quote_parser = subparsers.choices["download-quotes"]
    quote_parser.add_argument("--output", type=Path, required=True)
    quote_parser.add_argument("--cache-dir", type=Path, required=True)
    quote_parser.add_argument("--max-cost", type=float, required=True)
    settlement_parser = subparsers.add_parser("download-settlements")
    settlement_parser.add_argument("--start", required=True)
    settlement_parser.add_argument("--end", required=True)
    settlement_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)

    if args.command == "download-settlements":
        count = download_settlements(date.fromisoformat(args.start), date.fromisoformat(args.end), args.output)
        print(f"wrote {count} XQC settlements to {args.output}")
        return 0

    client = historical_client()
    sessions = _dates(args)
    cost, details = estimate_cost(client, sessions, parent=args.parent)
    print(f"estimated Databento cost: ${cost:.4f} across {len(sessions)} weekdays")
    if args.command == "estimate":
        for row in details:
            print(f"{row['date']},{row['cost']:.6f}")
        return 0
    if cost > args.max_cost:
        raise SystemExit(f"aborted: estimated ${cost:.4f} exceeds --max-cost ${args.max_cost:.4f}")
    count = download_quotes(client, sessions, args.output, args.cache_dir, parent=args.parent)
    print(f"wrote {count} normalized 0DTE quote rows to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
