"""Broad 0–5 calendar-DTE openings. Only expiry/root filter, no strike selection.

Daily parent symbology comes from already-owned DBN headers. Requests are packed
up to the provider's 2,000-symbol limit; every unit spans the whole opening.
"""
from __future__ import annotations
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
import fcntl
import json
import math
import os
import uuid
from pathlib import Path
import shutil
import threading
import time
import warnings

import databento as db
import numpy as np
import pandas as pd
import zstandard

from . import ndxp_phase3d as base
from .index_options_metadata_estimator import load_key

ROOT = base.OUT/'stage1'
RAW = Path('/Volumes/Personal-Drive/MGC-research/ndxp-phase3d/stage1-raw')
CEILING = 500.0
LIMIT = 100_000_000
LOCK = threading.Lock()
ACCOUNTED = {}


def save(path,value):
    """Publish complete immutable JSON atomically so concurrent readers never see a prefix."""
    tmp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        tmp.write_text(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n')
        os.link(tmp,path)
    finally:
        tmp.unlink(missing_ok=True)


def option(symbol):
    if len(symbol)<15 or symbol[:-15].strip()!='NDXP' or symbol[-9] not in ('C','P'):
        raise ValueError('Unexpected option root/side')
    ymd=symbol[-15:-9]
    return date(2000+int(ymd[:2]),int(ymd[2:4]),int(ymd[4:])),symbol[-9],int(symbol[-8:])/1000


def universe(header, day):
    if str(header.dataset)!='OPRA.PILLAR' or str(header.schema) not in ('cbbo-1m','cmbp-1') or list(header.symbols)!=['NDXP.OPT']:
        raise ValueError('Owned chain is not an NDXP parent capture')
    session=date.fromisoformat(day);symbols=[]
    for symbol,intervals in header.mappings.items():
        expiry,side,strike=option(symbol)
        if 0<=(expiry-session).days<=5 and any(i['start_date']<=session<i['end_date'] for i in intervals):
            symbols.append(symbol)
    if not symbols or {option(s)[1] for s in symbols}!={'P','C'}:
        raise ValueError('Missing both-side near-expiry universe')
    return sorted(symbols)


def build_manifest(root=ROOT):
    path=root/'session_manifest.json.zst'
    if path.exists():
        return json.loads(zstandard.ZstdDecompressor().decompress(path.read_bytes()))
    sessions=[]
    for r in base.requests(mode='opening'):
        day=r['session'];source=Path('output/ndxp_multidte/discovery_cache/ndxp-opt/opening-0930-0932/cbbo-1m')/(day+'.dbn.zst')
        if not source.exists() and day=='2026-09-24':
            source=root/'inventory_2026-09-24.dbn.zst'
            proof=json.loads((root/'inventory_receipt.json').read_text())
            if base.digest(source)!=proof['sha256']:raise ValueError('Inventory integrity failure')
        h=db.DBNStore.from_file(source).metadata;symbols=universe(h,day)
        # Parent resolution in DBN metadata is date-based, not quote-observation based.
        # Include every date-valid symbol, including symbols with no first-two-minute quote.
        if int(h.start)>pd.Timestamp(r['params']['start']).value or int(h.end)<pd.Timestamp(r['params']['start']).value:
            raise ValueError('Owned chain metadata is not from this session opening')
        sessions.append(dict(session=day,symbols=symbols,start=r['params']['start'],end=r['params']['end'],
                             source=str(source),source_sha256=base.digest(source),
                             reuse_parent=(day=='2023-03-28')))
    result=dict(version=2,hard_ceiling_usd=CEILING,sessions=sessions,total_sessions=len(sessions),
                symbol_filter='NDXP root, both sides, expiration minus session date 0 through 5 inclusive; no strike/credit/outcome filter',
                source_policy='All date-valid mappings in existing parent DBN headers, not symbols observed in minute quotes',
                per_request_symbol_limit=2000,max_workers=8,raw_directory=str(RAW),
                benchmark_unchanged=True,prior_100_and_200_budget_stops_superseded=True)
    with path.open('xb') as f:f.write(zstandard.ZstdCompressor(level=9).compress(json.dumps(result,sort_keys=True).encode()))
    return result


def units(session):
    if session['reuse_parent']:return []
    return [dict(key=f"{session['session']}_part{i//2000:02d}",session=session['session'],params=dict(
        dataset='OPRA.PILLAR',schema='cmbp-1',stype_in='raw_symbol',symbols=session['symbols'][i:i+2000],
        start=session['start'],end=session['end'],limit=LIMIT)) for i in range(0,len(session['symbols']),2000)]


def confirmed_rejection(failure,raw):
    # SDK check_http_error executes before opening a data file. A returned 5xx
    # without any file is a confirmed rejected request, not an interrupted stream.
    return (failure.get('error_type')=='BentoServerError'
            and isinstance(failure.get('http_status'),int) and 500<=failure['http_status']<600
            and failure.get('received_bytes')==0 and not Path(raw).exists())


def accounting(root=ROOT, prior_root=None):
    """Metered estimates and reservations are distinct; invoice spend is unknown."""
    prior_root=base.OUT if prior_root is None else prior_root
    completed=failed=active=0.0
    for directory in (prior_root,root):
        for path in (directory/'intents').glob('*.json'):
            intent=json.loads(path.read_text());receipt=directory/'receipts'/path.name
            if receipt.exists():
                result=json.loads(receipt.read_text())
                if not result.get('accounting_alias_for'):completed+=result['metered_upper_cost_usd']
            elif (directory/'failures'/path.name).exists():
                failed+=intent['reserved_cost_usd']
            else:
                # A stale unresolved intent is uncertain, not a currently active stream.
                active+=intent['reserved_cost_usd']
    inventory=root/'inventory_receipt.json'
    if inventory.exists():completed+=json.loads(inventory.read_text())['metered_upper_cost_usd']
    elif (root/'inventory_intent.json').exists():failed+=json.loads((root/'inventory_intent.json').read_text())['reserved_cost_usd']
    return dict(completed_estimated_metered_cost_usd=completed,
                uncertain_failed_request_reservation_usd=failed,active_reservation_usd=active,
                actual_provider_charge_usd=None)


def exposure(root=ROOT,refresh=False):
    if str(root) in ACCOUNTED and not refresh:return ACCOUNTED[str(root)]
    values=accounting(root)
    total=sum(values[k] for k in ('completed_estimated_metered_cost_usd',
              'uncertain_failed_request_reservation_usd','active_reservation_usd'))
    ACCOUNTED[str(root)]=total
    return total


def request_count_audit(plan,root=ROOT):
    """Offline inventory union and exact request-count evidence, before paid dispatch."""
    expected={r['key']:r for session in plan['sessions'] for r in units(session)}
    for session in plan['sessions']:
        if session['reuse_parent']:continue
        packed=[symbol for req in units(session) for symbol in req['params']['symbols']]
        if packed!=session['symbols']:raise ValueError('Incomplete inventory packing')
    for path in (root/'intents').glob('*.json'):
        req=json.loads(path.read_text())['request'];validate_unit(req)
        canonical=req.get('retry_of',req['key'])
        if req['params']!=expected[canonical]['params']:raise ValueError('Saved request differs from full inventory')
    completed=sum((root/'receipts'/(key+'.json')).exists() for key in expected)
    attempted=sum((root/'intents'/(key+'.json')).exists() for key in expected)
    return dict(sessions=len(plan['sessions']),requests_before_reuse=sum(math.ceil(len(s['symbols'])/2000) for s in plan['sessions']),
        requests_avoided_by_reuse=sum(math.ceil(len(s['symbols'])/2000) for s in plan['sessions'] if s['reuse_parent']),
        canonical_requests=len(expected),completed_requests=completed,previously_attempted_requests=attempted,
        unattempted_requests=len(expected)-attempted,max_symbols_per_request=2000,
        stype_in='raw_symbol',schema='cmbp-1',all_strikes=True,calendar_dte=[0,5],
        opening_window_et=['09:30','10:00'],cost_metadata_calls=0)


def verify(raw,req):
    params=req['params'];store=db.DBNStore.from_file(raw);h=store.metadata
    lo,hi=pd.Timestamp(params['start']).value,pd.Timestamp(params['end']).value
    if str(h.dataset)!='OPRA.PILLAR' or str(h.schema)!='cmbp-1' or int(h.start)!=lo or int(h.end)!=hi:
        raise ValueError('DBN header bounds/dataset/schema mismatch')
    wanted=set(params['symbols']);session=date.fromisoformat(req['session']);mapping={}
    if not set(h.mappings)<=wanted:raise ValueError('Unexpected mapped symbol')
    for symbol,intervals in h.mappings.items():
        expiry,side,strike=option(symbol);dte=(expiry-session).days
        if not 0<=dte<=5:raise ValueError('Wrong calendar DTE')
        for interval in intervals:
            if interval['start_date']<=session<interval['end_date']:
                iid=int(interval['symbol'])
                if iid in mapping and mapping[iid][0]!=symbol:raise ValueError('Ambiguous daily instrument mapping')
                mapping[iid]=(symbol,dte,side)
    counts=Counter();seen=set();records=0;first=last=None
    for a in store.to_ndarray(count=1_000_000):
        if len(a)==0:continue
        if not ((a['ts_recv']>=lo)&(a['ts_recv']<hi)).all():raise ValueError('Wrong event timestamps')
        ids,n=np.unique(a['instrument_id'],return_counts=True)
        for iid,count in zip(ids,n):
            if int(iid) not in mapping:raise ValueError('Unmapped/wrong-universe instrument')
            symbol,dte,side=mapping[int(iid)];seen.add(symbol);counts[(dte,side)]+=int(count)
        records+=len(a);a0,a1=int(a['ts_recv'].min()),int(a['ts_recv'].max())
        first=a0 if first is None else min(first,a0);last=a1 if last is None else max(last,a1)
    with Path(raw).open('rb') as source,zstandard.ZstdDecompressor().stream_reader(source) as stream:
        nbytes=0
        while chunk:=stream.read(8*1024*1024):nbytes+=len(chunk)
    if records>=params['limit']:raise ValueError('Record limit reached; cannot claim full opening')
    if nbytes>params['limit']*80+base.HEADER_ALLOWANCE:raise ValueError('Record byte bound exceeded')
    # Empty exact-symbol units are disclosed, not treated as proof each symbol quoted.
    return dict(records=records,uncompressed_bytes=nbytes,first_recv_ns=first,last_recv_ns=last,
                observed_symbols=len(seen),requested_symbols=len(wanted),symbols_without_events=sorted(wanted-seen),
                unmapped_requested_symbols=sorted(wanted-set(h.mappings)),source_interval_complete=True,
                by_dte_side=[dict(calendar_dte=d,side=s,records=n) for (d,s),n in sorted(counts.items())])



def validate_unit(req):
    p=req['params'];session=date.fromisoformat(req['session'])
    if p['dataset']!='OPRA.PILLAR' or p['schema']!='cmbp-1' or p['stype_in']!='raw_symbol':
        raise ValueError('Wrong direct retrieval universe')
    if not 1<=len(p['symbols'])<=2000 or len(set(p['symbols']))!=len(p['symbols']):
        raise ValueError('Invalid symbol batch')
    if any(not 0<=(option(s)[0]-session).days<=5 for s in p['symbols']):
        raise ValueError('Wrong calendar DTE')
    import exchange_calendars as xc
    opening=xc.get_calendar('XNYS').session_open(req['session'])
    if pd.Timestamp(p['start'])!=opening or pd.Timestamp(p['end'])!=opening+pd.Timedelta(minutes=30):
        raise ValueError('Wrong opening interval')
    if p['limit']!=LIMIT:raise ValueError('Unapproved record cap')


def retrieve(req,root,raw_root,rate,client,validator=verify):
    validate_unit(req)
    key=req['key'];raw=raw_root/(key+'.dbn.zst');receipt=root/'receipts'/(key+'.json');intent=root/'intents'/(key+'.json');transfer=root/'transfers'/(key+'.json')
    if receipt.exists():
        result=json.loads(receipt.read_text())
        if result['request']!=req or base.digest(Path(result['path']))!=result['sha256']:raise ValueError('Completed cache integrity failure')
        return result
    if transfer.exists():
        result=json.loads(transfer.read_text())
        if result['request']!=req or base.digest(raw)!=result['sha256']:raise ValueError('Transferred cache integrity failure')
    else:
        with LOCK:
            if intent.exists() or raw.exists():raise RuntimeError('Uncertain prior paid attempt; no duplicate retry')
            reservation=(req['params']['limit']*80+base.HEADER_ALLOWANCE)/1e9*rate
            if exposure(root)+reservation>CEILING:raise RuntimeError('Next coarse retrieval would exceed $500 reserved ceiling')
            if shutil.disk_usage(raw_root).free<req['params']['limit']*80+base.HEADER_ALLOWANCE+base.DISK_RESERVE:
                raise RuntimeError('Insufficient storage for bounded stream')
            before=exposure(root)
            save(intent,dict(request=req,reserved_cost_usd=reservation,started_utc=datetime.now(timezone.utc).isoformat()))
            ACCOUNTED[str(root)]=before+reservation
        started=time.monotonic()
        try:
            with warnings.catch_warnings(record=True) as captured:
                warnings.simplefilter('always');client.timeseries.get_range(**req['params'],path=raw)
            result=dict(request=req,path=str(raw),sha256=base.digest(raw),compressed_bytes=raw.stat().st_size,
                        download_seconds=time.monotonic()-started,warnings=[str(w.message) for w in captured])
            save(transfer,result)
        except Exception as exc:
            failure=dict(request=req,error_type=type(exc).__name__,
                http_status=getattr(exc,'http_status',None),elapsed_seconds=time.monotonic()-started,
                received_bytes=raw.stat().st_size if raw.exists() else 0,automatic_retry=False)
            failure['confirmed_pre_stream_rejection']=confirmed_rejection(failure,raw)
            save(root/'failures'/(key+'.json'),failure)
            raise
    checked=validator(raw,req)
    result=dict(**result,**checked,metered_upper_cost_usd=checked['uncompressed_bytes']/1e9*rate,actual_provider_charge_usd=None)
    with LOCK:
        before=exposure(root)
        reservation=json.loads(intent.read_text())['reserved_cost_usd']
        save(receipt,result)
        ACCOUNTED[str(root)]=before-reservation+result['metered_upper_cost_usd']
    return result


def progress(plan,started,root=ROOT):
    receipts=[json.loads(p.read_text()) for p in (root/'receipts').glob('*.json')]
    receipts=[r for r in receipts if not r['request'].get('retry_of')]
    keys={r['request']['key'] for r in receipts}
    done=[s['session'] for s in plan['sessions'] if s['reuse_parent'] or all(r['key'] in keys for r in units(s))]
    prior=json.loads((base.OUT/'receipts'/'2023-03-28_opening.json').read_text())
    failures=[json.loads(p.read_text()) for p in (root/'failures').glob('*.json')]
    result=dict(sessions_completed=len(done),sessions_total=len(plan['sessions']),completed_sessions=done,
                units_completed=len(receipts),gb_acquired=(sum(p.stat().st_size for p in RAW.glob('*.dbn.zst'))+prior['compressed_bytes'])/1e9,
                gb_validated=(sum(r['compressed_bytes'] for r in receipts)+prior['compressed_bytes'])/1e9,
                elapsed_seconds=time.monotonic()-started,failures=len(failures),prior_phase3d_failures=1,
                **accounting(root),
                hard_ceiling_usd=CEILING)
    tmp=root/'progress.tmp';tmp.write_text(json.dumps(result,indent=2)+'\n');tmp.replace(root/'progress.json')
    print(f"{len(done)}/{len(plan['sessions'])} sessions | {result['gb_acquired']:.3f} GB | {result['elapsed_seconds']:.0f}s | {len(failures)} new failures | ${result['completed_estimated_metered_cost_usd']:.3f} completed estimated metered / ${result['uncertain_failed_request_reservation_usd']:.3f} uncertain failed reservation / ${result['active_reservation_usd']:.3f} active reservation",flush=True)
    return result



def run(workers=4):
    if not 1<=workers<=8:raise ValueError('One to eight workers required')
    for p in ('intents','receipts','transfers','failures'):(ROOT/p).mkdir(parents=True,exist_ok=True)
    RAW.mkdir(parents=True,exist_ok=True)
    plan=build_manifest();rate=base.unit_rate(base.OUT)
    original=json.loads((base.OUT/'receipts'/'2023-03-28_opening.json').read_text())
    oldraw=base.OUT/'raw'/'2023-03-28_opening.dbn.zst'
    if base.digest(oldraw)!=original['sha256']:raise ValueError('Reused March 28 file changed')
    if set(plan['sessions'][0]['symbols'])!=set(original['eligible_0_5dte_symbols']):raise ValueError('Owned chain differs from completed broad capture')
    audit=request_count_audit(plan)
    save(ROOT/f'request_count_audit_{time.time_ns()}.json',audit)
    print(json.dumps(audit,sort_keys=True),flush=True)
    client=db.Historical(load_key('.env.local'));started=time.monotonic()
    pending=[r for s in plan['sessions'] for r in units(s)]
    # Isolate uncertain units instead of repurchasing them or blocking unrelated sessions.
    pending=[r for r in pending if not ((ROOT/'intents'/(r['key']+'.json')).exists()
             and not (ROOT/'receipts'/(r['key']+'.json')).exists()
             and not (ROOT/'transfers'/(r['key']+'.json')).exists())]
    errors=[];fatal=False;progress(plan,started)
    # Rolling bounded workers. Isolate provider 5xx failures; repair confirmed rejections afterward.
    with ThreadPoolExecutor(workers) as pool:
        iterator=iter(pending);active={}
        for req in list(next(iterator,None) for _ in range(workers)):
            if req:active[pool.submit(retrieve,req,ROOT,RAW,rate,client)]=req
        while active:
            from concurrent.futures import wait,FIRST_COMPLETED
            ready,_=wait(active,timeout=45,return_when=FIRST_COMPLETED)
            for future in ready:
                req=active.pop(future)
                try:future.result()
                except Exception as exc:
                    errors.append(dict(key=req['key'],error_type=type(exc).__name__))
                    # A failed remote unit remains reserved and is never retried.
                    # Continue independent sessions for server-side 5xx errors only.
                    if not isinstance(getattr(exc,'http_status',None),int) or not 500<=exc.http_status<600:fatal=True
                if (ROOT/'drain.flag').exists():fatal=True
                if not fatal:
                    following=next(iterator,None)
                    if following:active[pool.submit(retrieve,following,ROOT,RAW,rate,client)]=following
            progress(plan,started)
    # Failed attempts retain reservations and are not repurchased automatically.
    unresolved=[r['key'] for session in plan['sessions'] for r in units(session)
                if not (ROOT/'receipts'/(r['key']+'.json')).exists()]
    if fatal or unresolved:
        save(ROOT/f'stop_{time.time_ns()}.json',dict(errors=errors,unresolved_units=unresolved,fatal=fatal))
        raise RuntimeError('Acquisition stopped; preserve uncertain attempts and inspect evidence')
    return progress(plan,started)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--workers',type=int,default=8);parser.add_argument('--manifest-only',action='store_true');args=parser.parse_args()
    ROOT.mkdir(parents=True,exist_ok=True)
    with (ROOT/'.acquisition.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if args.manifest_only:
            m=build_manifest();print('Validated session universes:',m['total_sessions'],flush=True)
        else:run(args.workers)
