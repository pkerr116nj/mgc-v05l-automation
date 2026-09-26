"""Authorized Phase 3C batch acquisition with immutable intents and safe resume.

Batch submission is paid once; retrieving the same provider job is not a new
purchase. Unknown submission outcomes are reconciled, never blindly retried.
"""
from __future__ import annotations
import json,time,hashlib,warnings,threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
from urllib.parse import urlparse
import requests
import pandas as pd
import databento as db
from .ndxp_complete_panel import OUT,CEILING,save,digest,gate,finalize,cache_key,load_key


def request_key(r):return cache_key(r)
def safe_job(job):return {k:v for k,v in job.items() if k not in ('api_key','user_id')}
def matches(job,r):
    symbols=job.get('symbols',[])
    if isinstance(symbols,str):symbols=symbols.split(',')
    return all(job.get(k)==r[k] for k in ('dataset','schema','stype_in')) and sorted(symbols)==sorted(r['symbols']) and all(pd.Timestamp(job[k]).value==pd.Timestamp(r[k]).value for k in ('start','end'))

def authorize(manifest,quote,manifest_hash,require_fresh=True):
    keys=[cache_key(dict(method=m,params=r)) for r in manifest['requests'] for m in ('get_cost','get_billable_size')]
    ok,cost,reason=gate(quote['results'],keys)
    if not ok or quote['manifest_sha256']!=manifest_hash:raise ValueError('Purchase gate rejected: '+reason)
    for q in quote['results'].values():
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(q['quoted_at_utc'])).total_seconds()
        if require_fresh and not 0<=age<=86400:raise ValueError('Fresh quote expired')
    return cost

_submit_lock=threading.Lock()
_last_submit=0.0

def paced_submit(batch,r):
    global _last_submit
    with _submit_lock:
        delay=max(0,3.2-(time.monotonic()-_last_submit))
        if delay:time.sleep(delay)
        _last_submit=time.monotonic()
    return batch.submit_job(**r,encoding='dbn',compression='zstd')

def submit_once(r,batch,root,known):
    key=request_key(r);intent=root/'intents'/f'{key}.json';receipt=root/'jobs'/f'{key}.json'
    if receipt.exists():return json.loads(receipt.read_text())
    existing=[j for j in known if matches(j,r) and j.get('state')!='expired']
    if existing:
        j=sorted(existing,key=lambda x:(x['state']!='done',x['id']))[0]
        result=dict(request=r,job=safe_job(j),reused_prior_job=not intent.exists());save(receipt,result);return result
    if intent.exists():
        proof=root/'reconciled_absent'/f'{key}.json';retry=root/'retry_intents'/f'{key}.json'
        if not proof.exists() or retry.exists():raise RuntimeError('Uncertain prior submission; reconcile provider job, never repurchase')
        save(retry,dict(request=r,started_utc=datetime.now(timezone.utc).isoformat(),reconciliation=json.loads(proof.read_text())))
    else:save(intent,dict(request=r,started_utc=datetime.now(timezone.utc).isoformat()))
    with warnings.catch_warnings(record=True) as ww:
        warnings.simplefilter('always');j=paced_submit(batch,r)
    result=dict(request=r,job=safe_job(j),reused_prior_job=False,quality_warnings=[str(w.message) for w in ww])
    save(receipt,result);return result

def download_file(detail,destination,key,get=requests.get):
    detail=dict(detail);detail['hash']=detail['hash'].removeprefix('sha256:')
    p=Path(destination)/detail['filename']
    if p.name!=detail['filename']:raise ValueError('Unsafe provider filename')
    if p.exists():
        if p.stat().st_size!=detail['size'] or digest(p)!=detail['hash']:raise ValueError('Existing immutable raw file failed integrity')
        return dict(path=str(p),bytes=p.stat().st_size,sha256=detail['hash'],reused=True)
    url=detail['urls']['https'];host=urlparse(url).hostname
    if urlparse(url).scheme!='https' or not (host=='databento.com' or host.endswith('.databento.com')):raise ValueError('Unexpected download host')
    p.parent.mkdir(parents=True,exist_ok=True)
    for attempt in range(3):
        tmp=p.with_name(p.name+f'.attempt{attempt}.part')
        if tmp.exists():
            if tmp.stat().st_size==detail['size'] and digest(tmp)==detail['hash']:
                tmp.rename(p);return dict(path=str(p),bytes=p.stat().st_size,sha256=detail['hash'],reused=True)
            continue
        try:
            with get(url,auth=(key,''),stream=True,timeout=(20,60)) as response:
                response.raise_for_status()
                with tmp.open('xb') as f:
                    for chunk in response.iter_content(1024*1024):f.write(chunk)
            if tmp.stat().st_size!=detail['size'] or digest(tmp)!=detail['hash']:raise ValueError('Provider file checksum mismatch')
            tmp.rename(p)
            return dict(path=str(p),bytes=p.stat().st_size,sha256=detail['hash'],reused=False)
        except (requests.RequestException,ValueError):
            if attempt==2:raise
    raise RuntimeError('Transfer attempts exhausted; raw partial files preserved')

def acquire(max_polls=240):
    if not isinstance(max_polls,int) or not 1<=max_polls<=240:raise ValueError('Status poll budget must be 1..240')
    manifest=finalize();quote=json.loads((OUT/'preflight_quote.json').read_text())
    already_submitted=all((OUT/'jobs'/f'{request_key(r)}.json').exists() and json.loads((OUT/'jobs'/f'{request_key(r)}.json').read_text())['request']==r for r in manifest['requests'])
    cost=authorize(manifest,quote,digest(OUT/'final_request_manifest.json'),require_fresh=not already_submitted)
    expected=json.loads((OUT/'manifest_digest.json').read_text())['sha256']
    if digest(OUT/'final_request_manifest.json')!=expected:raise ValueError('Immutable manifest changed')
    key=load_key('.env.local');client=db.Historical(key)
    for sub in ('intents','jobs','raw','downloads','reconciled_absent','retry_intents'): (OUT/sub).mkdir(exist_ok=True)
    known=client.batch.list_jobs()
    print('authorized quoted USD',cost,'requests',len(manifest['requests']),'workers 4',flush=True)
    results=[];errors=[]
    with ThreadPoolExecutor(4) as pool:
        futures={pool.submit(submit_once,r,client.batch,OUT,known):r for r in manifest['requests']}
        for i,f in enumerate(as_completed(futures),1):
            try:results.append(f.result())
            except Exception as e:errors.append(dict(request_key=request_key(futures[f]),error_type=type(e).__name__))
            if i%25==0:print('submitted/reused',i,'errors',len(errors),flush=True)
    # One reconciliation pass can recover a response lost after successful submission.
    if errors:
        known=client.batch.list_jobs();errors=[];results=[]
        for r in manifest['requests']:
            try:results.append(submit_once(r,client.batch,OUT,known))
            except Exception as e:errors.append(dict(request_key=request_key(r),error_type=type(e).__name__))
    completed={};last_jobs={r['job']['id']:r['job'] for r in results}
    def fetch(r,job):
        k=request_key(r['request']);record=OUT/'downloads'/f'{k}.json'
        if record.exists():
            result=json.loads(record.read_text())
            for f in result['files']:
                if digest(f['path'])!=f['sha256']:raise ValueError('Immutable raw integrity failed')
            return k,result
        details=client.batch.list_files(job['id']);files=[]
        for d in details:files.append(download_file(d,OUT/'raw'/k,key))
        result=dict(request=r['request'],job=safe_job(job),reused_prior_job=r['reused_prior_job'],files=files,quality_warnings=r.get('quality_warnings',[]))
        save(record,result);return k,result
    # At most 240 status polls, 15s apart. No unbounded loops or paid retries.
    polls=0
    for turn in range(max_polls):
        known=client.batch.list_jobs();polls+=1;last_jobs.update({j['id']:j for j in known if j['id'] in last_jobs})
        ready=[r for r in results if request_key(r['request']) not in completed and last_jobs[r['job']['id']]['state']=='done']
        with ThreadPoolExecutor(4) as pool:
            for k,result in pool.map(lambda r:fetch(r,last_jobs[r['job']['id']]),ready):completed[k]=result
        print('downloaded',len(completed),'/',len(results),'status polls',polls,flush=True)
        if len(completed)==len(results):break
        if turn+1<max_polls:time.sleep(15)
    actual=sum(float(r['job']['cost_usd']) for r in completed.values() if not r['reused_prior_job'] and r['job'].get('cost_usd') is not None)
    cost_complete=len(completed)==len(manifest['requests']) and all(r['job'].get('cost_usd') is not None for r in completed.values())
    evidence=dict(quoted_cost_usd=cost,actual_provider_cost_usd=actual if cost_complete else None,known_completed_cost_usd=actual,actual_cost_complete=cost_complete,completed_requests=len(completed),planned_requests=len(manifest['requests']),status_polls=polls,errors=errors,receipts=list(completed.values()),remaining_jobs=[safe_job(j) for j in last_jobs.values() if j['state']!='done'])
    aggregate=OUT/'acquisition_receipts.json'
    if aggregate.exists():aggregate.rename(OUT/f'acquisition_receipts_previous_{time.time_ns()}.json')
    save(aggregate,evidence);print('actual cost',evidence['actual_provider_cost_usd'],'completed',len(completed),flush=True)
    if errors or len(completed)!=len(manifest['requests']):raise RuntimeError('Panel acquisition incomplete; reuse existing jobs on resume')

def reconcile_absent():
    manifest=finalize();quote=json.loads((OUT/'preflight_quote.json').read_text())
    authorize(manifest,quote,digest(OUT/'final_request_manifest.json'))
    client=db.Historical(load_key('.env.local'))
    first=client.batch.list_jobs();time.sleep(5);second=client.batch.list_jobs()
    directory=OUT/'reconciled_absent';directory.mkdir(exist_ok=True);(OUT/'retry_intents').mkdir(exist_ok=True)
    recovered=absent=0
    for r in manifest['requests']:
        k=request_key(r);intent=OUT/'intents'/f'{k}.json';receipt=OUT/'jobs'/f'{k}.json'
        if receipt.exists() or not intent.exists():continue
        matches_now=[j for j in first+second if matches(j,r)]
        if matches_now:
            save(receipt,dict(request=r,job=safe_job(matches_now[-1]),reused_prior_job=False,recovered_lost_response=True));recovered+=1
        else:
            started=datetime.fromisoformat(json.loads(intent.read_text())['started_utc'])
            if (datetime.now(timezone.utc)-started).total_seconds()<60:continue
            proof=directory/f'{k}.json'
            if not proof.exists():save(proof,dict(request=r,reconciled_utc=datetime.now(timezone.utc).isoformat(),provider_snapshots=2,separation_seconds=5,matching_jobs=0))
            absent+=1
    print('recovered receipts',recovered,'reconciled absent',absent,flush=True)

if __name__=='__main__':
    import sys
    if '--reconcile' in sys.argv:reconcile_absent()
    else:
        import argparse
        parser=argparse.ArgumentParser();parser.add_argument('--max-polls',type=int,default=240)
        acquire(parser.parse_args().max_polls)
