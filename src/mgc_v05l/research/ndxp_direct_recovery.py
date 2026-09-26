"""Bounded direct recovery of the frozen Phase 3C panel; no batch API calls."""
from __future__ import annotations
import json,time,warnings,argparse
from pathlib import Path
from collections import defaultdict,Counter
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
import pandas as pd
import exchange_calendars as xc
import databento as db
from .ndxp_complete_panel import OUT as PRIOR,OLD,save,digest,merge,load_key,http_metadata,cache_key
from .ndxp_owned_pilot import subtract_intervals

OUT=PRIOR.parent/'phase3c_direct_recovery'
CEILING=10.

def planning_guardrail(plan,justification=None,approved_ceiling=CEILING):
    required=('projected_cost','projected_bytes','metadata_call_count','provider_request_count','consolidation_alternative','expected_operational_complexity')
    if any(k not in plan for k in required):raise ValueError('Incomplete operational acquisition plan')
    if plan['metadata_call_count']>12:raise ValueError('Metadata budget exceeded')
    if plan['projected_cost'] is None or not 0<=plan['projected_cost']<=approved_ceiling:raise ValueError('New-spend ceiling exceeded or estimate unavailable')
    alternative=plan['consolidation_alternative']['provider_request_count']
    if plan['projected_cost']<=approved_ceiling*.5 and plan['provider_request_count']>max(2*alternative,alternative+20) and not justification:
        raise ValueError('Highly fragmented low-cost acquisition needs explicit justification')
    return True


def consolidate(fragments,panel,calendar):
    groups=defaultdict(list)
    for c in panel['candidates']:groups[c['session_date']].append(c)
    result=[]
    for session,cs in sorted(groups.items()):
        symbols=sorted({c[k] for c in cs for k in ('short_symbol','long_symbol')})
        start=min(pd.Timestamp(c['entry_time']).value for c in cs)
        end=max(calendar.session_close(c['expiration']).value for c in cs)
        days=calendar.sessions_in_range(session,max(c['expiration'] for c in cs))
        windows=[(max(start,calendar.session_open(d).value),min(end,calendar.session_close(d).value)) for d in days]
        params=dict(dataset='OPRA.PILLAR',schema='cmbp-1',stype_in='raw_symbol',symbols=symbols,start=pd.Timestamp(start,unit='ns',tz='UTC').isoformat(),end=pd.Timestamp(end,unit='ns',tz='UTC').isoformat())
        result.append(dict(pilot_session=session,params=params,regular_windows_ns=windows,symbol_minutes=len(symbols)*sum(b-a for a,b in windows)/60e9))
    covered=defaultdict(list)
    for r in result:
        for s in r['params']['symbols']:covered[s].append((pd.Timestamp(r['params']['start']).value,pd.Timestamp(r['params']['end']).value))
    for r in fragments:
        for s in r['symbols']:
            if subtract_intervals(pd.Timestamp(r['start']).value,pd.Timestamp(r['end']).value,covered[s]):raise ValueError('Consolidation omitted a frozen missing interval')
    return result


def manifest():
    p=OUT/'session_request_manifest.json'
    if p.exists():return json.loads(p.read_text())
    original=json.loads((PRIOR/'final_request_manifest.json').read_text());panel=json.loads((OLD/'pilot_manifest.json').read_text())
    requests=consolidate(original['requests'],panel,xc.get_calendar('XNYS'))
    assert len(requests)<=60 and len(requests)==len(panel['sessions'])
    surgical=defaultdict(list);coarse=defaultdict(list)
    for r in original['requests']:
        for s in r['symbols']:surgical[s].append((pd.Timestamp(r['start']).value,pd.Timestamp(r['end']).value))
    for r in requests:
        for s in r['params']['symbols']:coarse[s].extend(r['regular_windows_ns'])
    surgical_minutes=sum(b-a for rr in surgical.values() for a,b in merge(rr))/60e9
    coarse_unique=sum(b-a for rr in coarse.values() for a,b in merge(rr))/60e9
    requested=sum(r['symbol_minutes'] for r in requests)
    result=dict(version=1,requests=requests,original_fragment_count=len(original['requests']),consolidated_request_count=len(requests),frozen_candidate_ids=panel['candidate_ids'],sessions=panel['sessions'],symbol_count_distribution=dict(Counter(len(r['params']['symbols']) for r in requests)),requested_regular_session_minutes=sum(sum(b-a for a,b in r['regular_windows_ns']) for r in requests)/60e9,requested_symbol_minutes=requested,surgical_unique_symbol_minutes=surgical_minutes,extra_regular_symbol_minutes=coarse_unique-surgical_minutes,cross_request_duplicate_symbol_minutes=requested-coarse_unique,envelope_symbol_minutes=sum(len(r['params']['symbols'])*(pd.Timestamp(r['params']['end']).value-pd.Timestamp(r['params']['start']).value)/60e9 for r in requests),boundary_policy='One continuous entry-to-latest-expiry request per pilot entry session. First/last bounds are frozen exchange-session bounds; intermediate overnight/extended-session padding and duplicate bytes are tolerated. Replay filters each cash-session boundary, including early closes.',source_sha256={str(p):digest(p) for p in (PRIOR/'final_request_manifest.json',PRIOR/'submitted_jobs.json',PRIOR/'preflight_quote.json',OLD/'pilot_manifest.json')})
    save(p,result);return result


def quote():
    m=manifest();p=OUT/'preflight_quote.json'
    if p.exists():return json.loads(p.read_text())
    old=json.loads((PRIOR/'preflight_quote.json').read_text())
    activity=defaultdict(float)
    for q in old['results'].values():
        if q['request']['method']=='get_cost' and q['status']=='ok':
            r=q['request']['params']
            for s in r['symbols']:activity[s]+=q['value']/len(r['symbols'])
    ranked=sorted(m['requests'],key=lambda r:(sum(activity[s] for s in r['params']['symbols']),r['symbol_minutes'],r['pilot_session']))
    # Six activity strata; choose the largest exposure in each stratum.
    strata=[ranked[i*len(ranked)//6:(i+1)*len(ranked)//6] for i in range(6)]
    sample=[max(group,key=lambda r:(r['symbol_minutes'],sum(activity[s] for s in r['params']['symbols']),r['pilot_session'])) for group in strata]
    jobs=[(r,method) for r in sample for method in ('get_cost','get_billable_size')]
    assert len(jobs)<=12
    cache=OUT/'quote_cache';cache.mkdir(exist_ok=True);call=http_metadata(load_key('.env.local'))
    def one(r,method):
        job=dict(method=method,params=r['params']);key=cache_key(job);dest=cache/f'{key}.json';intent=cache/f'{key}.intent.json'
        if dest.exists():return json.loads(dest.read_text())
        if intent.exists():raise RuntimeError('Metadata attempt already used; no automatic retry beyond 12 calls')
        save(intent,job)
        try:value=call(method,r['params']);result=dict(pilot_session=r['pilot_session'],method=method,value=value,status='ok',symbol_minutes=r['symbol_minutes'])
        except Exception as exc:result=dict(pilot_session=r['pilot_session'],method=method,status='unavailable',error_type=type(exc).__name__,symbol_minutes=r['symbol_minutes'])
        result['quoted_at_utc']=datetime.now(timezone.utc).isoformat();save(dest,result);return result
    with ThreadPoolExecutor(4) as pool:results=list(pool.map(lambda pair:one(*pair),jobs))
    valid=all(r['status']=='ok' and isinstance(r['value'],(int,float)) and 0<=r['value']<float('inf') for r in results)
    totals={}
    for method in ('get_cost','get_billable_size'):
        totals[method]=2*max((r['value']/r['symbol_minutes'] for r in results if r['method']==method and r['status']=='ok'),default=float('inf'))*m['requested_symbol_minutes'] if valid else None
    plan=dict(projected_cost=totals['get_cost'],projected_bytes=totals['get_billable_size'],metadata_call_count=len(jobs),provider_request_count=len(m['requests']),consolidation_alternative=dict(provider_request_count=len(m['requests']),original_fragment_count=m['original_fragment_count']),expected_operational_complexity='60 immutable session streams, at most four workers, one network attempt per session; no batch dependency')
    try:planning_guardrail(plan);allowed=True;reason='conservative estimate within new $10 ceiling'
    except ValueError as exc:allowed=False;reason=str(exc)
    result=dict(manifest_sha256=digest(OUT/'session_request_manifest.json'),purchase_allowed=allowed,gate_reason=reason,hard_new_spend_ceiling_usd=CEILING,estimate_method='Six activity strata from prior fragment-price evidence; largest symbol-minute exposure per stratum. Twice the highest sampled dollars (or bytes) per regular symbol-minute applied to all requests, including cross-request duplication. Conservative stress estimate, not a provider guarantee or exact full quote.',uncertainty='Unobserved activity can exceed sampled rates; overnight/extended-session padding is included in sampled provider prices. No metadata retries.',sample_sessions=[r['pilot_session'] for r in sample],results=results,planning=plan)
    save(p,result);return result


def retrieve_once(r,client,root):
    session=r['pilot_session'];params=r['params'];raw=root/'raw'/f'{session}.dbn.zst';receipt=root/'receipts'/f'{session}.json';intent=root/'intents'/f'{session}.json';transfer=root/'receipts'/f'{session}.transfer.json'
    if receipt.exists():
        saved=json.loads(receipt.read_text())
        if saved['request']!=params or digest(raw)!=saved['files'][0]['sha256']:raise ValueError('Immutable session cache integrity failure')
        return saved
    if transfer.exists():
        completed=json.loads(transfer.read_text())
        if completed['request']!=params or digest(raw)!=completed['sha256']:raise ValueError('Completed stream integrity failure')
    else:
        if intent.exists() or raw.exists():raise RuntimeError('Prior direct attempt is uncertain; preserve partial data and stop, never re-download automatically')
        save(intent,dict(request=params,started_utc=datetime.now(timezone.utc).isoformat()))
        started=time.monotonic()
        with warnings.catch_warnings(record=True) as ww:
            warnings.simplefilter('always');client.timeseries.get_range(**params,path=raw)
        completed=dict(request=params,sha256=digest(raw),bytes=raw.stat().st_size,elapsed_seconds=time.monotonic()-started,quality_warnings=[str(w.message) for w in ww])
        save(transfer,completed)
    from .ndxp_panel_analysis import validate_header
    header=db.DBNStore.from_file(raw).metadata;validate_header(header,params)
    result=dict(pilot_session=session,request=params,files=[dict(path=str(raw),bytes=completed['bytes'],sha256=completed['sha256'])],elapsed_seconds=completed['elapsed_seconds'],quality_warnings=completed['quality_warnings'],actual_provider_cost_usd=None,cost_note='SDK streaming response does not expose an actual billed charge',header_verified=True)
    save(receipt,result);return result


def acquire():
    m=manifest();q=quote();planning_guardrail(q['planning'])
    if not q['purchase_allowed'] or q['manifest_sha256']!=digest(OUT/'session_request_manifest.json'):raise ValueError('Direct purchase gate rejected')
    for path,expected in m['source_sha256'].items():
        if digest(path)!=expected:raise ValueError('Frozen source changed')
    for sub in ('raw','receipts','intents'):(OUT/sub).mkdir(exist_ok=True)
    pending=[r for r in m['requests'] if not (OUT/'receipts'/f"{r['pilot_session']}.json").exists()]
    if pending:
        for result in q['results']:
            if not 0<=(datetime.now(timezone.utc)-datetime.fromisoformat(result['quoted_at_utc'])).total_seconds()<=86400:raise ValueError('Estimate expired; operator review required')
    client=db.Historical(load_key('.env.local'));started=time.monotonic();done=[];errors=[]
    # Windows of four bound the additional exposure after any failed stream.
    for begin in range(0,len(m['requests']),4):
        with ThreadPoolExecutor(4) as pool:
            futures={pool.submit(retrieve_once,r,client,OUT):r for r in m['requests'][begin:begin+4]}
            for f in as_completed(futures):
                try:done.append(f.result())
                except Exception as exc:errors.append(dict(pilot_session=futures[f]['pilot_session'],error_type=type(exc).__name__))
        print('direct completed',len(done),'/',len(m['requests']),'errors',len(errors),flush=True)
        if errors:break
    evidence=dict(completed_requests=len(done),planned_requests=len(m['requests']),new_direct_requests_with_intents=len(list((OUT/'intents').glob('*.json'))),actual_provider_cost_usd=None,actual_cost_complete=False,conservative_estimated_cost_usd=q['planning']['projected_cost'],downloaded_bytes=sum(f['bytes'] for r in done for f in r['files']),wall_clock_seconds=time.monotonic()-started,receipts=sorted(done,key=lambda r:r['pilot_session']),errors=errors,batch_calls=0)
    p=OUT/'acquisition_receipts.json'
    if p.exists():p.rename(OUT/f'acquisition_receipts_previous_{time.time_ns()}.json')
    save(p,evidence)
    if errors or len(done)!=len(m['requests']):raise RuntimeError('Direct recovery incomplete; no automatic duplicate paid attempts')
    return evidence

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['manifest','quote','acquire']);args=parser.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    result=manifest() if args.action=='manifest' else quote() if args.action=='quote' else acquire()
    print(json.dumps({k:v for k,v in result.items() if k not in ('requests','results','receipts')},indent=2),flush=True)
