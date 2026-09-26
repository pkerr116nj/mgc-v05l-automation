"""Read-only DBN index and bounded-memory exact-leg extraction for Phase 3B."""
from __future__ import annotations
import csv,json,hashlib
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import numpy as np
import pandas as pd
import databento as db


def header(path):
    p=Path(path);stat=p.stat()
    try:
        m=db.DBNStore.from_file(p).metadata
        if str(m.schema)!='cmbp-1':return None
        return dict(path=str(p),bytes=stat.st_size,mtime_ns=stat.st_mtime_ns,start=m.start,end=m.end,mappings=m.mappings,quarantine='quarantine' in str(p))
    except Exception as e:return dict(path=str(p),error=type(e).__name__)


def index_headers(roots,cache):
    paths=sorted({p for root in roots for p in Path(root).rglob('*.dbn.zst')})
    with ThreadPoolExecutor(4) as ex:rows=[r for r in ex.map(header,paths) if r]
    Path(cache).write_text(json.dumps(rows,default=str));return rows


def extract(job):
    h,ids,bounds,cache=job
    key=hashlib.sha256(h['path'].encode()).hexdigest()[:24];p=Path(cache)/(key+'.parquet');sp=Path(cache)/(key+'.json')
    signature=[h['bytes'],h['mtime_ns'],sorted(ids.items()),sorted(bounds.items())]
    if sp.exists():
        old=json.loads(sp.read_text())
        if old['signature']==json.loads(json.dumps(signature)):return old['stats']
    # Chunk decode never materializes a full-chain session in RAM.
    chunks=[];wanted=np.array(list(ids),dtype=np.uint32)
    for a in db.DBNStore.from_file(h['path']).to_ndarray(count=250000):
        a=a[np.isin(a['instrument_id'],wanted)]
        if not len(a):continue
        df=pd.DataFrame({k:a[k] for k in ('instrument_id','ts_recv','ts_event','bid_px_00','ask_px_00','bid_sz_00','ask_sz_00','action','price','size')})
        df['symbol']=df.instrument_id.map(ids)
        df=df[(df.ts_recv>=df.symbol.map({s:b[0] for s,b in bounds.items()})) & (df.ts_recv<df.symbol.map({s:b[1] for s,b in bounds.items()}))]
        if len(df):chunks.append(df)
    stats=[]
    if chunks:
        df=pd.concat(chunks,ignore_index=True).sort_values(['symbol','ts_recv','ts_event'],kind='stable');df.to_parquet(p,index=False)
        for symbol,g in df.groupby('symbol',sort=True):
            ts=g.ts_recv.to_numpy();gaps=np.diff(ts.astype(np.int64));big=gaps>1_000_000_000
            stats.append(dict(store=key,path=h['path'],symbol=symbol,header_start_ns=h['start'],header_end_ns=h['end'],first_ns=int(ts[0]),last_ns=int(ts[-1]),events=len(g),gaps_over_1s=int(big.sum()),max_gap_ns=int(gaps.max()) if len(gaps) else 0,parquet=str(p)))
    sp.write_text(json.dumps(dict(signature=signature,stats=stats)));return stats


def run_index(headers,features,cache):
    bounds={}
    for c in features:
        start=pd.Timestamp(c['entry_time']).value-60_000_000_000
        end=pd.Timestamp(c['expiration']+' 16:00',tz='America/New_York').value
        for k in ('short_symbol','long_symbol'):
            s=c[k];old=bounds.get(s,(start,end));bounds[s]=(min(start,old[0]),max(end,old[1]))
    jobs=[];inventory=[]
    for h in headers:
        if h.get('error') or h.get('quarantine'):
            inventory.append({k:v for k,v in h.items() if k!='mappings'});continue
        ids={};match=[]
        for symbol in h['mappings'].keys() & bounds.keys():
            start,end=bounds[symbol]
            if h['start']>=end or h['end']<=start:continue
            for m in h['mappings'][symbol]:
                a=pd.Timestamp(m['start_date'],tz='UTC').value;b=pd.Timestamp(m['end_date'],tz='UTC').value
                if max(a,h['start'],start)<min(b,h['end'],end):ids[int(m['symbol'])]=symbol
            if symbol in ids.values():match.append(symbol)
        inventory.append(dict(path=h['path'],bytes=h['bytes'],mtime_ns=h['mtime_ns'],start_ns=h['start'],end_ns=h['end'],mapped_symbols=len(h['mappings']),candidate_symbols=sorted(match)))
        if ids:jobs.append((h,ids,{s:bounds[s] for s in set(ids.values())},cache))
    Path(cache).mkdir(parents=True,exist_ok=True)
    Path(cache,'store_inventory.json').write_text(json.dumps(inventory,indent=2))
    print('decode_jobs',len(jobs),'compressed_GB',sum(x[0]['bytes'] for x in jobs)/1e9,flush=True)
    stats=[]
    with ThreadPoolExecutor(4) as ex:
        for i,r in enumerate(ex.map(extract,jobs)):
            stats.extend(r)
            if i%50==0:print('decoded',i,'leg_store_pairs',len(stats),flush=True)
    Path(cache,'leg_store_stats.json').write_text(json.dumps(stats));return stats

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser();p.add_argument('--headers',required=True);p.add_argument('--scan-root',nargs='+');p.add_argument('--cache',required=True);p.add_argument('--features',default='output/ndxp_multidte/deep_dive_v1/path_features.csv');args=p.parse_args()
    if args.scan_root:index_headers(args.scan_root,args.headers)
    run_index(json.loads(Path(args.headers).read_text()),list(csv.DictReader(open(args.features))),args.cache)
