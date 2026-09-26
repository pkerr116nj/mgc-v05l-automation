"""Bounded metadata-only pricing. This module has no market-data download API.

The default CLI is offline. --online permits at most 64 metadata requests with
four workers and no application retries. Provider quotes are exact only for
those sample requests; any multiplication to a research program is estimated.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
from urllib.request import Request, urlopen
import base64
from urllib.parse import urlencode

MAX_CALLS=64
MAX_WORKERS=4
ALLOWED={'get_cost','get_billable_size','list_unit_prices','get_dataset_range','list_schemas'}

def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)

def cache_key(job):
    return hashlib.sha256(canonical(job).encode()).hexdigest()

def validate_job(job):
    if set(job)!={'method','params'} or job['method'] not in ALLOWED: raise ValueError('Metadata allowlist only')
    p=job['params']
    if p.get('dataset')!='OPRA.PILLAR': raise ValueError('Only planned OPRA metadata')
    if job['method'] in ('get_cost','get_billable_size'):
        start,end=(datetime.fromisoformat(p[k]) for k in ('start','end'))
        if start.tzinfo is None or end.tzinfo is None: raise ValueError('Explicit timezone required')
        seconds=(end-start).total_seconds()
        if not 0<seconds<=86400: raise ValueError('Sample must be at most one day')
        if p.get('schema') not in {'cbbo-1m','cbbo-1s','cmbp-1','tcbbo','definition'}: raise ValueError('Unsupported schema')
        if not isinstance(p.get('symbols'),list) or not 1<=len(p['symbols'])<=24: raise ValueError('Bound symbols')
        if p.get('stype_in') not in {'parent','raw_symbol'}: raise ValueError('Explicit stype required')
        if 'ALL_SYMBOLS' in p['symbols']: raise ValueError('No full-market request')
        if p['stype_in']=='parent' and seconds>120: raise ValueError('Parent samples limited to two minutes')

def price_samples(jobs,call,cache=None,workers=4,max_calls=64):
    """Validate the entire workload before any call. Cache is returned, not mutated."""
    if not 1<=workers<=MAX_WORKERS or not 0<=max_calls<=MAX_CALLS: raise ValueError('Bounds exceeded')
    for job in jobs: validate_job(job)
    unique={cache_key(j):j for j in jobs}
    prior=dict(cache or {})
    pending=[(k,j) for k,j in sorted(unique.items()) if k not in prior]
    if len(pending)>max_calls: raise ValueError('Metadata call budget exceeded')
    def one(item):
        key,job=item
        try:
            value=call(job['method'],job['params'])
            row={'request':job,'status':'ok','value':value,'cost_label':'provider_sample_quote' if job['method']=='get_cost' else 'metadata'}
        except Exception as exc:
            # Do not persist response text: it could contain authentication details.
            row={'request':job,'status':'unavailable','error_type':type(exc).__name__}
        return key,row
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for key,row in pool.map(one,pending): prior[key]=row
    return {'call_count':len(pending),'cache_hits':len(unique)-len(pending),'max_concurrency':workers,
            'download_calls':0,'results':{k:prior[k] for k in sorted(unique)}}

def http_metadata(key):
    def call(method,params):
        encoded=dict(params)
        if isinstance(encoded.get('symbols'),list): encoded['symbols']=','.join(encoded['symbols'])
        url='https://hist.databento.com/v0/metadata.'+method+'?'+urlencode(encoded)
        auth=base64.b64encode((key+':').encode()).decode()
        request=Request(url,headers={'Authorization':'Basic '+auth,'Accept':'application/json'})
        # One bounded HTTP request; urllib has no automatic application retries.
        with urlopen(request,timeout=20) as response: return json.load(response)
    return call

def load_key(env_file=None):
    key=os.environ.get('DATABENTO_API_KEY','').strip()
    if not key and env_file:
        for line in Path(env_file).read_text().splitlines():
            line=line.removeprefix('export ').strip()
            if line.startswith('DATABENTO_API_KEY='): key=line.split('=',1)[1].strip().strip('\"\'')
    if not key: raise ValueError('DATABENTO_API_KEY unavailable; use labeled offline estimates')
    return key

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--jobs',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--online',action='store_true');p.add_argument('--env-file',type=Path);p.add_argument('--cache',type=Path)
    a=p.parse_args()
    if a.output.exists(): raise FileExistsError('Choose a new evidence output; no calls made')
    jobs=json.loads(a.jobs.read_text())
    if not a.online:
        for j in jobs:validate_job(j)
        if len({cache_key(j) for j in jobs})>MAX_CALLS:raise ValueError('Budget exceeded')
        result={'mode':'offline_preview','expected_metadata_calls':len({cache_key(j) for j in jobs}),'actual_calls':0,'download_calls':0}
    else:
        cache=json.loads(a.cache.read_text())['results'] if a.cache else None
        result=price_samples(jobs,http_metadata(load_key(a.env_file)),cache)
    a.output.parent.mkdir(parents=True,exist_ok=True)
    # No overwrite, including metadata evidence; choose a new output for a new run.
    with a.output.open('x') as f:json.dump(result,f,indent=2,sort_keys=True);f.write('\n')
    print({k:v for k,v in result.items() if k!='results'})

if __name__=='__main__':main()
