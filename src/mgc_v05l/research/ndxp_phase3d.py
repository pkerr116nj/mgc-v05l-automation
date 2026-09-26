"""Phase 3D coarse direct acquisition with durable per-request spend reservations.

No cost-estimation, batch, broker or live-trading endpoints are used. A single
unit-rate lookup plus the server's record limit bounds each request's exposure.
Successful files are immutable; uncertain attempts are never automatically retried.
"""
from __future__ import annotations

import argparse
import fcntl
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import shutil
import time
import warnings
from collections import Counter

import numpy as np

import databento as db
import exchange_calendars as xc
import pandas as pd
import zstandard

from .index_options_metadata_estimator import load_key
from .ndxp_multidte_data import parse_osi

OUT = Path('output/ndxp_multidte/program_v1/phase3d_expanded_event_panel')
CEILING = 100.0
RECORD_BYTES = 80
MAX_RECORDS = 100_000_000
HEADER_ALLOWANCE = 16_000_000
DISK_RESERVE = 5_000_000_000


def save(path, value):
    with path.open('x') as f:
        json.dump(value, f, indent=2, sort_keys=True, allow_nan=False)
        f.write('\n')


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(8*1024*1024), b''):
            h.update(chunk)
    return h.hexdigest()


def unit_rate(root):
    evidence = json.loads((root/'unit_rate.json').read_text())
    age = (datetime.now(timezone.utc) - datetime.fromisoformat(evidence['checked_at_utc'])).total_seconds()
    if not 0 <= age <= 86400:
        raise ValueError('Unit rate is stale; refresh the single unit-rate lookup before new paid requests')
    rate = next(r['unit_prices']['cmbp-1'] for r in evidence['rates']
                if r['mode'] == 'historical-streaming')
    if not math.isfinite(rate) or rate < 0 or evidence['cmbp1_record_bytes'] != RECORD_BYTES:
        raise ValueError('Invalid unit rate or record size')
    return rate


def requests(start='2023-03-28', end='2026-09-24', mode='full'):
    if mode not in ('full', 'opening'):
        raise ValueError('Unknown acquisition mode')
    cal = xc.get_calendar('XNYS')
    result = []
    for day in cal.sessions_in_range(start, end):
        a, b = cal.session_open(day), cal.session_close(day)
        if mode == 'opening':
            b = min(b, a + pd.Timedelta(minutes=30))
        result.append(dict(session=str(day.date()), mode=mode, params=dict(
            dataset='OPRA.PILLAR', schema='cmbp-1', stype_in='parent',
            symbols=['NDXP.OPT'], start=a.isoformat(), end=b.isoformat(),
            limit=MAX_RECORDS)))
    return result


def reserved_cost(root):
    total = 0.0
    for path in (root/'intents').glob('*.json'):
        intent = json.loads(path.read_text())
        receipt = root/'receipts'/path.name
        if receipt.exists():
            total += json.loads(receipt.read_text())['metered_upper_cost_usd']
        else:
            total += intent['reserved_cost_usd']
    return total


def admit(rate, spent, record_limit, free_bytes):
    if not 0 < record_limit <= MAX_RECORDS or not math.isfinite(spent) or spent < 0:
        raise ValueError('Invalid reservation')
    byte_bound = record_limit*RECORD_BYTES + HEADER_ALLOWANCE
    cost = byte_bound/1_000_000_000*rate
    if spent + cost > CEILING:
        raise RuntimeError('Hard $100 ceiling: insufficient unreserved budget')
    # Zstd incompressible overhead is covered by the disk reserve.
    if free_bytes < byte_bound + DISK_RESERVE:
        raise RuntimeError('Insufficient storage for bounded stream plus safety reserve')
    return cost


def validate_request(req):
    p = req['params']
    if (p['dataset'], p['schema'], p['stype_in'], p['symbols']) != (
            'OPRA.PILLAR', 'cmbp-1', 'parent', ['NDXP.OPT']):
        raise ValueError('Wrong acquisition universe')
    cal = xc.get_calendar('XNYS')
    a, b = pd.Timestamp(p['start']), pd.Timestamp(p['end'])
    if a != cal.session_open(req['session']) or b > cal.session_close(req['session']) or b <= a:
        raise ValueError('Incorrect exchange-session bounds')
    if p['limit'] != MAX_RECORDS:
        raise ValueError('Unapproved record bound')


def validate_artifact(raw, req):
    p = req['params']
    store = db.DBNStore.from_file(raw)
    h = store.metadata
    if str(h.dataset) != p['dataset'] or str(h.schema) != p['schema']:
        raise ValueError('Unexpected DBN dataset/schema')
    if int(h.start) != pd.Timestamp(p['start']).value or int(h.end) > pd.Timestamp(p['end']).value:
        raise ValueError('Unexpected DBN bounds')
    records = 0
    first = last = None
    symbols = set()
    eligible = set()
    session = pd.Timestamp(req['session']).date()
    for frame in store.to_df(price_type='fixed', pretty_ts=False, map_symbols=True, count=250000):
        frame = frame.reset_index()
        if not ((frame.ts_recv >= pd.Timestamp(p['start']).value) &
                (frame.ts_recv < pd.Timestamp(p['end']).value)).all():
            raise ValueError('Record outside requested time bounds')
        for symbol in set(frame.symbol) - symbols:
            root, expiry, side, strike = parse_osi(symbol)
            if root != 'NDXP':
                raise ValueError('Non-NDXP record in parent capture')
            if 0 <= (expiry-session).days <= 5:
                eligible.add(symbol)
        symbols.update(frame.symbol)
        records += len(frame)
        if len(frame):
            low, high = int(frame.ts_recv.min()), int(frame.ts_recv.max())
            first = low if first is None else min(first, low)
            last = high if last is None else max(last, high)
    with Path(raw).open('rb') as source, zstandard.ZstdDecompressor().stream_reader(source) as decoded:
        uncompressed = 0
        while chunk := decoded.read(8*1024*1024):
            uncompressed += len(chunk)
    if records == 0:
        raise ValueError('Empty parent response; do not claim source coverage')
    if records > p['limit'] or uncompressed > p['limit']*RECORD_BYTES + HEADER_ALLOWANCE:
        raise ValueError('Provider record/byte limit exceeded')
    return dict(records=records, uncompressed_bytes_including_metadata=uncompressed,
                first_recv_ns=first, last_recv_ns=last, symbols=len(symbols),
                eligible_0_5dte_symbols=sorted(eligible),
                source_interval_complete=records < p['limit'],
                truncated_by_record_limit=records == p['limit'])


def retrieve(req, root, client, rate, validator=validate_artifact):
    validate_request(req)
    key = req['session'] + '_' + req['mode']
    raw = root/'raw'/(key+'.dbn.zst')
    receipt = root/'receipts'/(key+'.json')
    intent = root/'intents'/(key+'.json')
    transfer = root/'transfers'/(key+'.json')
    if receipt.exists():
        r = json.loads(receipt.read_text())
        if r['request'] != req or digest(raw) != r['sha256']:
            raise ValueError('Immutable completed artifact changed')
        return r
    if transfer.exists():
        completed = json.loads(transfer.read_text())
        if completed['request'] != req or digest(raw) != completed['sha256']:
            raise ValueError('Completed transfer changed')
    else:
        if intent.exists() or raw.exists():
            raise RuntimeError('Uncertain previous attempt; no automatic duplicate purchase')
        reservation = admit(rate, reserved_cost(root), req['params']['limit'], shutil.disk_usage(root).free)
        save(intent, dict(request=req, reserved_cost_usd=reservation,
                          rate_usd_per_decimal_gb=rate, started_utc=datetime.now(timezone.utc).isoformat()))
        start = time.monotonic()
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always')
            try:
                client.timeseries.get_range(**req['params'], path=raw)
            except Exception as exc:
                failure = root/'failures'/(key+'.json')
                failure.parent.mkdir(exist_ok=True)
                save(failure, dict(request=req, error_type=type(exc).__name__,
                                   http_status=getattr(exc, 'http_status', None),
                                   elapsed_seconds=time.monotonic()-start,
                                   compressed_bytes=raw.stat().st_size if raw.exists() else 0,
                                   warnings=[str(w.message) for w in captured],
                                   automatic_retry=False))
                raise
        completed = dict(request=req, sha256=digest(raw), compressed_bytes=raw.stat().st_size,
                         download_seconds=time.monotonic()-start,
                         warnings=[str(w.message) for w in captured])
        save(transfer, completed)
    check = validator(raw, req)
    result = dict(**completed, **check,
                  metered_upper_cost_usd=check['uncompressed_bytes_including_metadata']/1e9*rate,
                  actual_provider_charge_usd=None,
                  accounting_note='Observed uncompressed bytes including metadata times unit rate; conservative metering, not invoice. Uncertain attempts retain full reservation.')
    save(receipt, result)
    return result



def profile_owned_opening(root=OUT):
    """Count actual owned event records by DTE/side; no pricing or download calls."""
    raw = root/'raw'/'2023-03-28_opening.dbn.zst'
    receipt = json.loads((root/'receipts'/'2023-03-28_opening.json').read_text())
    if digest(raw) != receipt['sha256']:
        raise ValueError('Owned source hash changed')
    store = db.DBNStore.from_file(raw)
    session = pd.Timestamp('2023-03-28').date()
    ids = {}
    for symbol, intervals in store.metadata.mappings.items():
        name, expiry, side, strike = parse_osi(symbol)
        dte = (expiry-session).days
        if name == 'NDXP' and 0 <= dte <= 5:
            for interval in intervals:
                if interval['start_date'] <= session < interval['end_date']:
                    ids[int(interval['symbol'])] = (dte, side)
    counts = Counter()
    total = 0
    for a in store.to_ndarray(count=1_000_000):
        keys, sizes = np.unique(a['instrument_id'], return_counts=True)
        total += len(a)
        for key, size in zip(keys, sizes):
            if int(key) in ids:
                counts[ids[int(key)]] += int(size)
    result = dict(total_records=total, near_expiry_records=sum(counts.values()),
                  near_expiry_record_share=sum(counts.values())/total,
                  near_expiry_symbols=len(ids),
                  by_dte_side=[dict(calendar_dte=d, side=s, records=n)
                               for (d,s),n in sorted(counts.items())], network_calls=0)
    path = root/'owned_opening_record_profile.json'
    if path.exists():
        if json.loads(path.read_text()) != result:
            raise ValueError('Owned record profile changed')
    else:
        save(path, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile-owned', action='store_true')
    parser.add_argument('--mode', choices=['full','opening'], default='full')
    parser.add_argument('--max-sessions', type=int, default=1)
    args = parser.parse_args()
    if args.profile_owned:
        print(json.dumps(profile_owned_opening(), indent=2))
        return
    OUT.mkdir(parents=True, exist_ok=True)
    for name in ('raw','receipts','intents','transfers'):
        (OUT/name).mkdir(exist_ok=True)
    manifest = OUT/f'{args.mode}_session_manifest.json'
    plan = requests(mode=args.mode)
    if not manifest.exists():
        save(manifest, dict(requests=plan, sessions=len(plan), hard_ceiling_usd=CEILING,
                            role='Broad operational capture; locally restrict to NDXP 0-5 calendar DTE',
                            frozen_benchmark_policy='Preserve all prior benchmark code and artifacts unchanged',
                            primary_proxy='Midpoint with modest adverse slippage; naturals, size and persistence are diagnostics only',
                            optimization=False))
    elif json.loads(manifest.read_text())['requests'] != plan:
        raise ValueError('Immutable manifest mismatch')
    selected = plan[:args.max_sessions]
    if (OUT/'budget_stop.json').exists() and any(
            not (OUT/'receipts'/(r['session']+'_'+r['mode']+'.json')).exists()
            for r in selected):
        raise RuntimeError('Observed budget-risk stop is active; see budget_stop.json before any new paid request')
    client = db.Historical(load_key('.env.local'))
    rate = unit_rate(OUT)
    started = time.monotonic()
    for req in selected:
        r = retrieve(req, OUT, client, rate)
        print(json.dumps(dict(session=req['session'], total_sessions=len(plan),
                              completed_receipts=len(list((OUT/'receipts').glob('*.json'))),
                              gb_downloaded=sum(p.stat().st_size for p in (OUT/'raw').glob('*'))/1e9,
                              elapsed_seconds=time.monotonic()-started, failures=0,
                              cumulative_metered_upper_or_reserved_usd=reserved_cost(OUT),
                              source_complete=r['source_interval_complete'])), flush=True)
        if not r['source_interval_complete']:
            raise RuntimeError('Coarse request reached record cap; retain partial evidence and reassess coverage before more acquisition')


if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=True)
    # One writer owns the spend ledger; concurrent CLI invocations cannot
    # reserve the same remaining budget or duplicate an in-flight unit.
    with (OUT/'.acquisition.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        main()
