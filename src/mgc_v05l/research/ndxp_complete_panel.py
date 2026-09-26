"""Phase 3C immutable manifest and bounded, metadata-only purchase preflight."""
from __future__ import annotations
import json,hashlib,argparse,math
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
import pandas as pd
import exchange_calendars as xc
from .index_options_metadata_estimator import canonical,cache_key,http_metadata,load_key,validate_job
from .ndxp_owned_pilot import subtract_intervals

ROOT=Path('output/ndxp_multidte/program_v1')
OLD=ROOT/'phase3b_owned_execution_pilot'
OUT=ROOT/'phase3c_complete_event_panel'
CEILING=10.0

def digest(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,value):
    p=Path(p);data=json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n'
    with p.open('x') as f:f.write(data)

def merge(intervals):
    out=[]
    for a,b in sorted(intervals):
        if a>=b:continue
        if out and a<=out[-1][1]:out[-1]=(out[-1][0],max(b,out[-1][1]))
        else:out.append((a,b))
    return out

def final_requests(planned,inventory,calendar):
    owned=defaultdict(list)
    for h in inventory:
        if h.get('error') or h.get('quarantine'):continue
        for symbol in h.get('candidate_symbols',[]):owned[symbol].append((h['start_ns'],h['end_ns']))
    unique=defaultdict(list);corrections=[]
    for i,r in enumerate(planned):
        assert r['dataset']=='OPRA.PILLAR' and r['schema']=='cmbp-1' and r['stype_in']=='raw_symbol'
        d=r['date'];assert '2023-03-28'<=d<='2024-12-31'
        if not calendar.is_session(d):raise ValueError('Non-session request')
        a=pd.Timestamp(r['start']).value;b=pd.Timestamp(r['end']).value
        op=calendar.session_open(d).value;cl=calendar.session_close(d).value
        lo=max(a,op);hi=min(b,cl)
        if (a,b)!=(lo,hi):corrections.append(dict(original_request=i,date=d,old_start_ns=a,old_end_ns=b,new_start_ns=lo,new_end_ns=hi))
        for s in r['symbols']:
            if not s.startswith('NDXP  ') or '.OPT' in s:raise ValueError('Exact NDXP legs only')
            if lo<hi:unique[(d,s)].append((lo,hi))
    groups=defaultdict(set)
    for (d,s),intervals in sorted(unique.items()):
        for a,b in merge(intervals):
            for lo,hi in subtract_intervals(a,b,owned[s]):groups[(d,lo,hi)].add(s)
    result=[]
    for (d,a,b),symbols in sorted(groups.items()):
        symbols=sorted(symbols)
        for i in range(0,len(symbols),24):
            result.append(dict(dataset='OPRA.PILLAR',schema='cmbp-1',stype_in='raw_symbol',symbols=symbols[i:i+24],start=pd.Timestamp(a,unit='ns',tz='UTC').isoformat(),end=pd.Timestamp(b,unit='ns',tz='UTC').isoformat()))
    return result,corrections

def gate(results,expected_keys):
    if set(results)!=set(expected_keys):return False,None,'incomplete quote'
    if any(r.get('status')!='ok' or not isinstance(r.get('value'),(int,float)) or not math.isfinite(r['value']) or r['value']<0 for r in results.values()):return False,None,'invalid or unavailable quote'
    total=sum(r['value'] for r in results.values() if r['request']['method']=='get_cost')
    return total<=CEILING,total,'authorized' if total<=CEILING else 'hard ceiling exceeded'

def finalize():
    if (OUT/'final_request_manifest.json').exists():return json.loads((OUT/'final_request_manifest.json').read_text())
    planned=json.loads((OLD/'minimum_acquisition_manifest.json').read_text());inventory=json.loads((OLD/'store_inventory.json').read_text())
    for h in inventory:
        if 'mtime_ns' in h:
            st=Path(h['path']).stat()
            if (st.st_size,st.st_mtime_ns)!=(h['bytes'],h['mtime_ns']):raise ValueError('Owned source changed; reindex required')
    calendar=xc.get_calendar('XNYS')
    requests,changes=final_requests(planned,inventory,calendar)
    panel=json.loads((OLD/'pilot_manifest.json').read_text());symbols={c[k] for c in panel['candidates'] for k in ('short_symbol','long_symbol')}
    assert all(set(r['symbols'])<=symbols for r in requests)
    obj=dict(version=1,requests=requests,original_requests=len(planned),final_requests=len(requests),calendar=dict(library='exchange_calendars',version=xc.__version__,name='XNYS',scope='Frozen Phase 3B regular cash-session panel through 16:00; corrected early cash closes to 13:00; no extended-close expansion',sources=['https://www.nasdaqtrader.com/content/technicalsupport/2023tradingcalendar.pdf','https://www.nasdaqtrader.com/content/technicalsupport/2024tradingcalendar.pdf']),calendar_corrections=changes,frozen_candidate_ids=panel['candidate_ids'],sessions=panel['sessions'],source_sha256={str(p):digest(p) for p in (OLD/'minimum_acquisition_manifest.json',OLD/'pilot_manifest.json',OLD/'store_inventory.json')})
    save(OUT/'final_request_manifest.json',obj);save(OUT/'manifest_digest.json',dict(sha256=digest(OUT/'final_request_manifest.json')));return obj

def quote():
    manifest=finalize();cache=OUT/'quote_cache';cache.mkdir(exist_ok=True)
    jobs=[dict(method=m,params=r) for r in manifest['requests'] for m in ('get_cost','get_billable_size')]
    for j in jobs:validate_job(j)
    unique={cache_key(j):j for j in jobs}
    if len(unique)>1024:raise ValueError('Hard metadata budget 1024 exceeded')
    results={};pending=[]
    for k,j in sorted(unique.items()):
        p=cache/(k+'.json')
        if p.exists():results[k]=json.loads(p.read_text())
        else:pending.append((k,j))
    call=http_metadata(load_key('.env.local'))
    print('metadata calls planned',len(pending),'workers',4,'downloads',0,flush=True)
    def one(k,j):
        try:r=dict(request=j,status='ok',value=call(j['method'],j['params']))
        except Exception as e:r=dict(request=j,status='unavailable',error_type=type(e).__name__)
        r['quoted_at_utc']=datetime.now(timezone.utc).isoformat();save(cache/(k+'.json'),r);return k,r
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures=[ex.submit(one,k,j) for k,j in pending]
        for i,f in enumerate(as_completed(futures),1):
            k,r=f.result();results[k]=r
            if i%25==0:print('quoted',i,'/',len(pending),'errors',sum(r['status']!='ok' for r in results.values()),flush=True)
    allowed,cost,reason=gate(results,unique)
    result=dict(manifest_sha256=digest(OUT/'final_request_manifest.json'),hard_ceiling_usd=CEILING,estimated_cost_usd=cost,billable_bytes=sum(r['value'] for r in results.values() if r['status']=='ok' and r['request']['method']=='get_billable_size'),purchase_allowed=allowed,gate_reason=reason,metadata_calls_this_run=len(pending),metadata_calls_total=len(results),concurrency=4,download_calls=0,results=results)
    save(OUT/'preflight_quote.json',result);print({k:v for k,v in result.items() if k!='results'},flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['manifest','quote']);a=p.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    if a.action=='manifest':print(finalize()['final_requests'])
    else:quote()


def resolve_quote_timeouts():
    """One retry per failed metadata call, total budget 1024 including attempts."""
    p=OUT/'preflight_quote.json';q=json.loads(p.read_text())
    bad=[(k,r) for k,r in q['results'].items() if r['status']!='ok']
    if not bad:return q
    if q['metadata_calls_total']+len(bad)>1024:raise ValueError('Retry budget exceeded; no purchase')
    call=http_metadata(load_key('.env.local'))
    def one(item):
        k,r=item;j=r['request'];record=OUT/'quote_cache'/(k+'.retry1.json')
        if record.exists():return k,json.loads(record.read_text())
        try:v=dict(request=j,status='ok',value=call(j['method'],j['params']))
        except Exception as e:v=dict(request=j,status='unavailable',error_type=type(e).__name__)
        v['quoted_at_utc']=datetime.now(timezone.utc).isoformat();save(record,v);return k,v
    with ThreadPoolExecutor(4) as pool:
        for k,r in pool.map(one,bad):q['results'][k]=r
    allowed,cost,reason=gate(q['results'],q['results'])
    q.update(purchase_allowed=allowed,estimated_cost_usd=cost,gate_reason=reason,metadata_calls_total=q['metadata_calls_total']+len(bad),retry_calls=len(bad),billable_bytes=sum(r['value'] for r in q['results'].values() if r['status']=='ok' and r['request']['method']=='get_billable_size'))
    backup=OUT/'preflight_quote_first_attempt.json'
    if backup.exists():raise ValueError('Retry already recorded')
    p.rename(backup);save(p,q);print({k:v for k,v in q.items() if k!='results'},flush=True);return q
