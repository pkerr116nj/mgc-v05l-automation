"""Read-only Phase 3A file/header inventory; no data acquisition or rule analysis."""
from __future__ import annotations
import argparse,csv,hashlib,json,os,re
from collections import defaultdict,Counter
from datetime import datetime,timezone
from pathlib import Path
from .ndxp_multidte_validation import digest

SKIP={'.git','.venv','node_modules','desktop','__pycache__','program_v1'}
RELEVANT=re.compile(r'ndx|qqq|spx|rut|vix|symbol=(?:NQ|MNQ|ES|MES|RTY)(?:/|$)',re.I)

def walk(root):
    for base,dirs,files in os.walk(root,followlinks=False):
        dirs[:]=sorted(d for d in dirs if d not in SKIP)
        for f in sorted(files):
            p=Path(base)/f
            if p.is_file():yield p

def header(path):
    import databento as db
    m=db.DBNStore.from_file(path).metadata
    def stamp(value):return datetime.fromtimestamp(value/1e9,timezone.utc).isoformat() if value else None
    raw=list(m.mappings) if isinstance(m.mappings,dict) else [x.raw_symbol for x in m.mappings]
    with path.open('rb') as f:prefix=hashlib.sha256(f.read(65536)).hexdigest()
    return {'file':str(path),'dataset':str(m.dataset),'schema':str(m.schema),'start':stamp(m.start),'end_exclusive':stamp(m.end),
            'stype_in':str(m.stype_in),'request_symbols':list(m.symbols) if len(m.symbols)<=24 else list(m.symbols)[:4],
            'request_symbol_count':len(m.symbols),'mapped_symbol_count':len(raw),'mapped_roots':sorted({s[:-15].strip() if len(s)>=15 else s for s in raw}),
            'mapped_expirations_sample':sorted({s[-15:-9] for s in raw if len(s)>=15})[:8],
            'partial_count':len(m.partial),'not_found_count':len(m.not_found),'prefix_64KiB_sha256':prefix}

def inventory(roots,repo):
    groups=defaultdict(list);files=[];observed=[]
    for root in roots:
        for p in walk(root):
            if p.name.endswith(('.dbn.zst','.parquet','.csv','.jsonl','.sqlite3','.duckdb')):
                s=p.stat();files.append({'path':str(p),'bytes':s.st_size,'mtime_ns':s.st_mtime_ns})
                if p.name.endswith('.dbn.zst'): groups[('dbn',str(p.parent))].append(p)
                elif p.suffix=='.parquet' and RELEVANT.search(str(p)) and any(x in str(p) for x in ['raw_bars','vix_daily']): groups[('parquet',str(p.parent))].append(p)
                elif p.name in {'xqc_settlements.csv','qqq_2024_2025.dbn.zst','qqq_2026.dbn.zst'}:groups[('csv',str(p))].append(p)
    for (kind,location),paths in sorted(groups.items()):
        paths=sorted(paths);dates=[d for p in paths for d in re.findall(r'20\d\d-\d\d-\d\d',p.name)]
        item={'location':location,'kind':kind,'file_count':len(paths),'bytes':sum(p.stat().st_size for p in paths),
              'filename_date_range':[min(dates),max(dates)] if dates else None,'row_count':None,
              'coverage_basis':'File counts exact; filename ranges are not continuous coverage; metadata sampled first/middle/last.',
              'quality':['No semantic completeness or fills inferred from cache presence.']}
        if kind=='dbn':
            samples=[paths[i] for i in sorted({0,len(paths)//2,len(paths)-1})]
            item['header_samples']=[]
            for p in samples:
                try:item['header_samples'].append(header(p))
                except Exception as e:item['quality'].append(f'{p.name}: {type(e).__name__}')
            item['instruments']=sorted({s for h in item['header_samples'] for s in h['mapped_roots']})
            item['schemas']=sorted({h['schema'] for h in item['header_samples']})
            item['source']=sorted({h['dataset'] for h in item['header_samples']})
            schema=' '.join(item['schemas'])
            item['sufficiency']={'structural':'partial; inspect exact contract/date overlap','path':'partial event windows; can derive second bars' if 'cmbp' in schema else 'sampled bars only','execution':'quote-event context only, not complex-book fill proof' if 'cmbp' in schema else 'insufficient'}
            if 'quarantine' in location:item['quality'].append('QUARANTINED: exclude until independently repaired; no reuse credit.')
        elif kind=='csv':
            with paths[0].open(newline='') as f: rows=list(csv.DictReader(f))
            days=[r['date'] for r in rows];item.update(row_count=len(rows),date_range=[min(days),max(days)],instruments=['NDXP/XQC'],schemas=['daily PM settlement'],source=sorted({r.get('source','unknown') for r in rows}),sha256=digest(paths[0]),sufficiency={'structural':'terminal payoff reference pending source/holiday validation','path':'no','execution':'no'})
        else:
            import pyarrow.parquet as pq
            item['instruments']=re.findall(r'symbol=([^/]+)',location) or ['VIX']
            item['schemas']=['parquet: '+('daily VIX' if 'vix_daily' in location else 'raw_bars_1m')]
            item['row_count']=sum(pq.ParquetFile(p).metadata.num_rows for p in paths)
            ranges=[]
            for p in paths:
                pf=pq.ParquetFile(p)
                for rg in range(pf.metadata.num_row_groups):
                    group=pf.metadata.row_group(rg)
                    for k in range(group.num_columns):
                        col=group.column(k)
                        if col.path_in_schema in ('timestamp','end_ts','date','ts_event','ts','vix_trade_date','bar_end') and col.statistics and col.statistics.has_min_max:
                            ranges.append([str(col.statistics.min),str(col.statistics.max)])
            item['date_range']=[min(x[0] for x in ranges),max(x[1] for x in ranges)] if ranges else None
            item['source']=['local research warehouse; upstream provenance must be checked']
            item['sufficiency']={'structural':'underlying/regime context only' if item['row_count'] else 'empty schema only','path':'not second-level options','execution':'no'}
        observed.append(item)
    # Inventory prior authoritative coverage reports without treating them as raw data.
    references=[]
    for symbol in ['nq','mnq','es','mes','rty']:
        p=repo/f'outputs/reports/canonical_market_data/coverage_audit_{symbol}_1m_historical_1m_canonical.json'
        if p.exists():
            r=json.loads(p.read_text());references.append({'path':str(p),'symbol':symbol.upper(),'sha256':digest(p),'evidence':'prior coverage audit; physical backing database not re-counted',**{k:v for k,v in r.items() if k not in ('gaps',)}})
    for p in [repo/'output/ndxp_multidte/deep_dive_v1/data_quality_report.json',repo/'output/ndxp_multidte/deep_dive_v1/report.json']:
        r=json.loads(p.read_text());references.append({'path':str(p),'sha256':digest(p),'evidence':'committed Phase 1/2 integrity audit','counts':r.get('counts',{k:r[k] for k in ['candidate_rows','candidate_sessions','path_rows','path_sessions'] if k in r})})
    changed=[x['path'] for x in files if Path(x['path']).stat().st_size!=x['bytes'] or Path(x['path']).stat().st_mtime_ns!=x['mtime_ns']]
    unchanged=not changed
    raw_unchanged=not any(p.endswith(('.dbn.zst','.parquet','.csv')) for p in changed)
    return {'schema_version':1,'scan_roots':[str(r) for r in roots],'market_file_count':len(files),'logical_bytes':sum(x['bytes'] for x in files),
            'mutation_check_size_mtime':unchanged,'raw_research_size_mtime_unchanged':raw_unchanged,'externally_changed_files':changed,'groups':observed,'coverage_references':references,
            'scope_caveats':['Logical sizes may double-count copies/hardlinks and overlapping datasets.','Only named local research roots scanned; no remote Mars mount assumed.','DBN metadata sampled, not every record decoded.','Operational SQLite/JSONL files counted but not read as authoritative historical option feeds.','No SPX/SPXW, QQQ-option or RUT/RUTW cache identified unless explicitly present in header samples. QQQ equity is distinct from QQQ options.'],
            'file_inventory':files}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,action='append',required=True);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();r=inventory([x.resolve() for x in a.root],a.repo.resolve())
    with a.output.open('x') as f:json.dump(r,f,indent=2,sort_keys=True);f.write('\n')
    print({'groups':len(r['groups']),'files':r['market_file_count'],'logical_GB':r['logical_bytes']/1e9,'raw_unchanged':r['raw_research_size_mtime_unchanged']})
if __name__=='__main__':main()
