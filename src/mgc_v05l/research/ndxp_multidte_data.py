"""Targeted Databento acquisition for NDXP 2DTE/3DTE vertical research.

Two-stage design keeps OPRA cost/runtime bounded:
1. Discover the opening NDXP option chain through 09:32 ET with CBBO-1m.
2. Select a small set of 10-point put spreads, then download CBBO-1m only
   for those exact option symbols through expiration.

The module never prints the Databento API key and reuses local DBN caches.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

NEW_YORK = ZoneInfo("America/New_York")
DATASET = "OPRA.PILLAR"
PARENT = "NDXP.OPT"
DISCOVERY_SCHEMA = "cbbo-1m"
PATH_SCHEMA = "cbbo-1m"


@dataclass(frozen=True)
class Candidate:
    session_date: date
    expiration: date
    dte_sessions: int
    target_credit: float
    entry_time: datetime
    short_strike: float
    long_strike: float
    short_symbol: str
    long_symbol: str
    short_bid: float
    short_ask: float
    long_bid: float
    long_ask: float
    mid_credit: float
    natural_credit: float


def historical_client() -> Any:
    key = os.environ.get("DATABENTO_API_KEY", "").strip()
    if not key:
        raise RuntimeError("DATABENTO_API_KEY is not set; load the existing Mars environment")
    try:
        import databento as db
    except ImportError as exc:
        raise RuntimeError("install the project databento dependency") from exc
    return db.Historical(key)


def weekdays_back(end: date, count: int) -> list[date]:
    out: list[date] = []
    cur = end
    while len(out) < count:
        if cur.weekday() < 5:
            out.append(cur)
        cur -= timedelta(days=1)
    return sorted(out)



def weekdays_between(start: date, end: date) -> list[date]:
    if start > end:
        raise ValueError("start is after end")
    out: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def parse_osi(symbol: str) -> tuple[str, date, str, float]:
    value = str(symbol)
    if len(value) < 15:
        raise ValueError(symbol)
    root = value[:-15].strip()
    exp = datetime.strptime(value[-15:-9], "%y%m%d").date()
    cp = value[-9]
    strike = int(value[-8:]) / 1000.0
    if cp not in {"C", "P"}:
        raise ValueError(symbol)
    return root, exp, cp, strike


def _frame_rows(frame: Any) -> list[dict[str, object]]:
    if frame.empty:
        return []
    df = frame.reset_index()
    tcol = "ts_recv" if "ts_recv" in df.columns else df.columns[0]
    rows: list[dict[str, object]] = []
    for rec in df.to_dict(orient="records"):
        try:
            _, exp, cp, strike = parse_osi(rec["symbol"])
            bid = float(rec["bid_px_00"])
            ask = float(rec["ask_px_00"])
            ts = rec[tcol]
            if hasattr(ts, "to_pydatetime"):
                ts = ts.to_pydatetime()
            ts = ts.astimezone(NEW_YORK)
        except Exception:
            continue
        if cp != "P" or not math.isfinite(bid) or not math.isfinite(ask) or bid < 0 or ask <= 0 or bid > ask:
            continue
        rows.append({
            "ts": ts,
            "expiration": exp,
            "strike": strike,
            "bid": bid,
            "ask": ask,
            "symbol": str(rec["symbol"]),
        })
    return rows


def _latest_first_minute(rows: Iterable[dict[str, object]], session: date, expirations: set[date]) -> dict[tuple[date, float], dict[str, object]]:
    start = datetime.combine(session, time(9, 31), NEW_YORK)
    end = start + timedelta(minutes=1)
    latest: dict[tuple[date, float], dict[str, object]] = {}
    for row in rows:
        ts = row["ts"]
        exp = row["expiration"]
        if not (start <= ts < end) or exp not in expirations:
            continue
        key = (exp, float(row["strike"]))
        if key not in latest or ts > latest[key]["ts"]:
            latest[key] = row
    return latest


def discover_candidates(frame: Any, session: date, *, dtes: Sequence[int], targets: Sequence[float], width: float = 10.0) -> list[Candidate]:
    rows = _frame_rows(frame)
    listed_expirations = sorted({row["expiration"] for row in rows if row["expiration"] >= session})
    requested = {}
    for dte in dtes:
        index = int(dte)
        if 0 <= index < len(listed_expirations):
            requested[int(dte)] = listed_expirations[index]
    latest = _latest_first_minute(rows, session, set(requested.values()))
    result: list[Candidate] = []
    for dte in dtes:
        exp = requested.get(int(dte))
        if exp is None:
            continue
        spreads: list[Candidate] = []
        strikes = sorted(strike for e, strike in latest if e == exp)
        for short_strike in strikes:
            long_strike = short_strike - width
            s = latest.get((exp, short_strike))
            l = latest.get((exp, long_strike))
            if not s or not l:
                continue
            mid = ((float(s["bid"]) + float(s["ask"])) / 2.0) - ((float(l["bid"]) + float(l["ask"])) / 2.0)
            natural = float(s["bid"]) - float(l["ask"])
            if not (0 < mid < width):
                continue
            spreads.append(Candidate(
                session_date=session,
                expiration=exp,
                dte_sessions=int(dte),
                target_credit=0.0,
                entry_time=max(s["ts"], l["ts"]),
                short_strike=short_strike,
                long_strike=long_strike,
                short_symbol=str(s["symbol"]),
                long_symbol=str(l["symbol"]),
                short_bid=float(s["bid"]), short_ask=float(s["ask"]),
                long_bid=float(l["bid"]), long_ask=float(l["ask"]),
                mid_credit=mid,
                natural_credit=natural,
            ))
        for target in targets:
            if not spreads:
                continue
            chosen = min(spreads, key=lambda x: (abs(x.mid_credit - target), x.entry_time, x.short_strike))
            result.append(Candidate(**{**asdict(chosen), "target_credit": float(target)}))
    return result


def estimate_discovery(client: Any, sessions: Sequence[date]) -> tuple[float, list[dict[str, object]]]:
    total = 0.0
    details: list[dict[str, object]] = []
    for session in sessions:
        start = datetime.combine(session, time(9, 30), NEW_YORK)
        end = start + timedelta(minutes=2)
        try:
            cost = float(client.metadata.get_cost(dataset=DATASET, schema=DISCOVERY_SCHEMA, stype_in="parent", symbols=[PARENT], start=start, end=end))
        except Exception as exc:
            details.append({"session": session.isoformat(), "status": "unresolved", "error": str(exc)})
            continue
        total += cost
        details.append({"session": session.isoformat(), "status": "ok", "cost": cost})
    return total, details


def estimate_discovery_range(client: Any, start_date: date, end_date: date) -> float:
    start = datetime.combine(start_date, time(9, 30), NEW_YORK)
    end = datetime.combine(end_date, time(9, 32), NEW_YORK)
    return float(client.metadata.get_cost(
        dataset=DATASET,
        schema=DISCOVERY_SCHEMA,
        stype_in="parent",
        symbols=[PARENT],
        start=start,
        end=end,
    ))


def download_discovery(client: Any, sessions: Sequence[date], cache_dir: Path, out_csv: Path, *, dtes: Sequence[int], targets: Sequence[float], workers: int = 8) -> list[Candidate]:
    import databento as db
    cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_one(session: date) -> tuple[date, list[Candidate], str | None]:
        cache = cache_dir / f"{session.isoformat()}-opening-{DISCOVERY_SCHEMA}.dbn.zst"
        try:
            if cache.exists():
                store = db.DBNStore.from_file(cache)
            else:
                start = datetime.combine(session, time(9, 30), NEW_YORK)
                end = start + timedelta(minutes=2)
                local_client = historical_client()
                store = local_client.timeseries.get_range(
                    dataset=DATASET,
                    schema=DISCOVERY_SCHEMA,
                    stype_in="parent",
                    symbols=[PARENT],
                    start=start,
                    end=end,
                    path=cache,
                )
            return session, discover_candidates(store.to_df(), session, dtes=dtes, targets=targets), None
        except Exception as exc:
            return session, [], str(exc)

    results: dict[date, list[Candidate]] = {}
    failures: list[dict[str, str]] = []
    max_workers = max(1, min(int(workers), 12))
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(fetch_one, session): session for session in sessions}
        for future in as_completed(futures):
            session, rows, error = future.result()
            if error:
                failures.append({"session": session.isoformat(), "error": error})
            else:
                results[session] = rows

    all_candidates: list[Candidate] = []
    for session in sorted(results):
        all_candidates.extend(results[session])

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        fields = list(asdict(all_candidates[0]).keys()) if all_candidates else ["session_date"]
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for row in all_candidates:
            rec = asdict(row)
            for k in ("session_date", "expiration", "entry_time"):
                rec[k] = rec[k].isoformat()
            w.writerow(rec)

    failure_path = out_csv.with_suffix(".failures.json")
    failure_path.write_text(json.dumps(sorted(failures, key=lambda x: x["session"]), indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "download_sessions_requested": len(sessions),
        "download_sessions_completed": len(results),
        "download_sessions_failed": len(failures),
        "candidate_rows": len(all_candidates),
        "failure_log": str(failure_path),
    }, indent=2))
    return all_candidates

def load_candidates(path: Path) -> list[Candidate]:
    out: list[Candidate] = []
    with path.open(newline="", encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out.append(Candidate(
                session_date=date.fromisoformat(r["session_date"]), expiration=date.fromisoformat(r["expiration"]), dte_sessions=int(r["dte_sessions"]),
                target_credit=float(r["target_credit"]), entry_time=datetime.fromisoformat(r["entry_time"]), short_strike=float(r["short_strike"]), long_strike=float(r["long_strike"]),
                short_symbol=r["short_symbol"], long_symbol=r["long_symbol"], short_bid=float(r["short_bid"]), short_ask=float(r["short_ask"]), long_bid=float(r["long_bid"]), long_ask=float(r["long_ask"]),
                mid_credit=float(r["mid_credit"]), natural_credit=float(r["natural_credit"])))
    return out


def estimate_paths(client: Any, candidates: Sequence[Candidate]) -> float:
    by_session: dict[date, list[Candidate]] = {}
    for c in candidates:
        by_session.setdefault(c.session_date, []).append(c)
    total = 0.0
    for session, rows in sorted(by_session.items()):
        symbols = sorted({s for c in rows for s in (c.short_symbol, c.long_symbol)})
        end_date = max(c.expiration for c in rows)
        start = datetime.combine(session, time(9, 30), NEW_YORK)
        end = datetime.combine(end_date, time(16, 1), NEW_YORK)
        total += float(client.metadata.get_cost(dataset=DATASET, schema=PATH_SCHEMA, stype_in="raw_symbol", symbols=symbols, start=start, end=end))
    return total


def download_paths(client: Any, candidates: Sequence[Candidate], cache_dir: Path, out_csv: Path) -> int:
    import databento as db
    by_session: dict[date, list[Candidate]] = {}
    for c in candidates:
        by_session.setdefault(c.session_date, []).append(c)
    cache_dir.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_csv.open("w", newline="", encoding="utf-8") as fh:
        fields = ["session_date", "quote_time", "symbol", "expiration", "strike", "bid", "ask"]
        w = csv.DictWriter(fh, fieldnames=fields); w.writeheader()
        for session, rows in sorted(by_session.items()):
            symbols = sorted({s for c in rows for s in (c.short_symbol, c.long_symbol)})
            end_date = max(c.expiration for c in rows)
            cache = cache_dir / f"{session.isoformat()}-selected-{PATH_SCHEMA}.dbn.zst"
            if cache.exists():
                store = db.DBNStore.from_file(cache)
            else:
                start = datetime.combine(session, time(9, 30), NEW_YORK)
                end = datetime.combine(end_date, time(16, 1), NEW_YORK)
                store = client.timeseries.get_range(dataset=DATASET, schema=PATH_SCHEMA, stype_in="raw_symbol", symbols=symbols, start=start, end=end, path=cache)
            for row in _frame_rows(store.to_df()):
                w.writerow({"session_date": session.isoformat(), "quote_time": row["ts"].isoformat(), "symbol": row["symbol"], "expiration": row["expiration"].isoformat(), "strike": row["strike"], "bid": row["bid"], "ask": row["ask"]})
                count += 1
    return count


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--end", required=True)
    common.add_argument("--start")
    common.add_argument("--sessions", type=int, default=60)
    common.add_argument("--workers", type=int, default=8)
    common.add_argument("--dtes", default="2,3")
    common.add_argument("--targets", default="5,6,7")
    sub.add_parser("estimate-discovery", parents=[common])
    eh = sub.add_parser("estimate-history")
    eh.add_argument("--end", required=True)
    eh.add_argument("--starts", default="2013-04-01,2016-09-24,2021-09-24,2023-09-24,2025-09-24")
    d = sub.add_parser("download-discovery", parents=[common]); d.add_argument("--cache-dir", type=Path, required=True); d.add_argument("--output", type=Path, required=True); d.add_argument("--max-cost", type=float, required=True)
    ep = sub.add_parser("estimate-paths"); ep.add_argument("--candidates", type=Path, required=True)
    dp = sub.add_parser("download-paths"); dp.add_argument("--candidates", type=Path, required=True); dp.add_argument("--cache-dir", type=Path, required=True); dp.add_argument("--output", type=Path, required=True); dp.add_argument("--max-cost", type=float, required=True)
    ic = sub.add_parser("inspect-cache"); ic.add_argument("--file", type=Path, required=True)
    a = p.parse_args(argv)
    if a.cmd == "inspect-cache":
        import databento as db
        store = db.DBNStore.from_file(a.file)
        print("metadata:")
        print(store.metadata)
        for map_symbols in (False, True):
            try:
                df = store.to_df(map_symbols=map_symbols).reset_index()
            except TypeError:
                df = store.to_df().reset_index()
                map_symbols = "unsupported"
            print(json.dumps({
                "map_symbols": map_symbols,
                "rows": int(len(df)),
                "columns": [str(c) for c in df.columns],
            }, indent=2))
            print(df.head(8).to_string(index=False))
            if "symbol" in df.columns:
                print("sample_symbols:", df["symbol"].dropna().astype(str).head(12).tolist())
            print("---")
        return 0
    client = historical_client()
    if a.cmd == "estimate-history":
        end_date = date.fromisoformat(a.end)
        results = []
        for start_text in a.starts.split(","):
            start_date = date.fromisoformat(start_text.strip())
            if start_date > end_date:
                raise SystemExit(f"start {start_date} is after end {end_date}")
            start_ts = datetime.combine(start_date, time(9, 30), NEW_YORK)
            end_ts = datetime.combine(end_date, time(9, 32), NEW_YORK)
            cost = float(client.metadata.get_cost(
                dataset=DATASET,
                schema=DISCOVERY_SCHEMA,
                stype_in="parent",
                symbols=[PARENT],
                start=start_ts,
                end=end_ts,
            ))
            results.append({
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "estimated_cost": cost,
            })
        print(json.dumps({
            "stage": "history_discovery",
            "parent": PARENT,
            "schema": DISCOVERY_SCHEMA,
            "note": "Each range is priced with one Databento metadata request over the continuous interval.",
            "ranges": results,
        }, indent=2))
        return 0
    if a.cmd in {"estimate-discovery", "download-discovery"}:
        end_date = date.fromisoformat(a.end)
        sessions = weekdays_between(date.fromisoformat(a.start), end_date) if a.start else weekdays_back(end_date, a.sessions)
        dtes = [int(x) for x in a.dtes.split(",")]
        targets = [float(x) for x in a.targets.split(",")]
        if a.start:
            cost = estimate_discovery_range(client, date.fromisoformat(a.start), end_date)
            details = []
            unresolved = []
            print(json.dumps({"stage":"discovery","parent":PARENT,"schema":DISCOVERY_SCHEMA,"range_start":a.start,"range_end":a.end,"sessions_requested":len(sessions),"estimated_cost":cost,"estimator":"single_range_request"}, indent=2))
        else:
            cost, details = estimate_discovery(client, sessions)
            unresolved = [row for row in details if row["status"] != "ok"]
            print(json.dumps({"stage":"discovery","parent":PARENT,"sessions_requested":len(sessions),"sessions_priced":len(details)-len(unresolved),"sessions_unresolved":len(unresolved),"estimated_cost":cost,"unresolved":unresolved}, indent=2))
        if a.cmd == "download-discovery":
            if cost > a.max_cost: raise SystemExit(f"aborted: ${cost:.4f} exceeds max ${a.max_cost:.4f}")
            rows = download_discovery(client, sessions, a.cache_dir, a.output, dtes=dtes, targets=targets, workers=a.workers)
            print(f"wrote {len(rows)} candidate rows to {a.output}")
        return 0
    candidates = load_candidates(a.candidates)
    cost = estimate_paths(client, candidates)
    print(json.dumps({"stage":"paths","candidates":len(candidates),"estimated_cost":cost}, indent=2))
    if a.cmd == "download-paths":
        if cost > a.max_cost: raise SystemExit(f"aborted: ${cost:.4f} exceeds max ${a.max_cost:.4f}")
        count = download_paths(client, candidates, a.cache_dir, a.output)
        print(f"wrote {count} path rows to {a.output}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
