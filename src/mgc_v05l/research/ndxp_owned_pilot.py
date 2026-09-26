"""Reproducible, local-only owned-data pilot. Run after ndxp_owned_index.

The extracted event cache is disposable and never committed. All analyses are
restricted to the development cohort; later dates are indexed for coverage only.
"""
from __future__ import annotations
import argparse,csv,json,hashlib,math
from pathlib import Path
from collections import defaultdict,Counter
from datetime import datetime
from bisect import bisect_left,bisect_right
import numpy as np
import pandas as pd
from .ndxp_owned_execution import NS,RULES,development,select_pilot,synchronize,valid,classify,crossings,missed_crossings,settlement_join,fast_sequences
from .ndxp_multidte_data import load_candidates
from .ndxp_multidte_path_features import session_paths,paired_path,settlement_payoff

BASE=Path('output/ndxp_multidte'); OUT=BASE/'program_v1/phase3b_owned_execution_pilot'
WINDOW='2021-09-24_2026-09-24'

def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()

def writejson(p,obj):p.write_text(json.dumps(obj,indent=2,allow_nan=False,default=str)+'\n')
def writecsv(p,rows,columns=None):
    keys=columns or list(dict.fromkeys(k for r in rows for k in r))
    with p.open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=keys,lineterminator='\n');w.writeheader();w.writerows(rows)

def ts(s):return pd.Timestamp(s).value

def bounds(c):return ts(c['entry_time'])-60*NS,pd.Timestamp(c['expiration']+' 16:00',tz='America/New_York').value

def clip(a,start,end):return a[np.searchsorted(a,start):np.searchsorted(a,end)]

def time_coverage(a,b,start,end,age=NS):
    """Timestamp-only intersection of fresh leg intervals, independent of prices."""
    aa=clip(a,start-age,end);bb=clip(b,start-age,end)
    if not len(aa) or not len(bb):return 0.0
    points=np.unique(np.concatenate((aa,bb,[start,end])));points=points[(points>=start)&(points<=end)]
    left=points[:-1];right=points[1:];i=np.searchsorted(aa,left,side='right')-1;j=np.searchsorted(bb,left,side='right')-1
    usable=(i>=0)&(j>=0)
    ends=np.minimum(np.minimum(aa[np.maximum(i,0)]+age,bb[np.maximum(j,0)]+age),right)
    return float(np.maximum(0,ends-left)[usable].sum()/NS)


def build_coverage(cache,features):
    stats=json.loads((cache/'leg_store_stats.json').read_text());inventory=json.loads((cache/'store_inventory.json').read_text())
    bysym=defaultdict(list)
    for c in features:
        for leg in ('short','long'):bysym[c[leg+'_symbol']].append((c,leg))
    stamps=defaultdict(list);store_rows=[];raw_counts=Counter();filegroups=defaultdict(list)
    for r in stats:filegroups[r['parquet']].append(r)
    for i,(p,ss) in enumerate(sorted(filegroups.items())):
        df=pd.read_parquet(p,columns=['symbol','ts_recv'])
        for symbol,g in df.groupby('symbol'):
            a=np.sort(g.ts_recv.to_numpy(dtype=np.int64));stamps[symbol].append(a)
            for c,leg in bysym[symbol]:
                start,end=bounds(c);x=clip(a,start,end)
                if not len(x):continue
                raw_counts[(c['candidate_id'],leg)]+=len(x)
                store_rows.append(dict(candidate_id=int(c['candidate_id']),leg=leg,store=ss[0]['store'],events=len(x),first_ns=int(x[0]),last_ns=int(x[-1]),gaps_over_1s=int((np.diff(x)>NS).sum()),max_gap_ns=int(np.diff(x).max()) if len(x)>1 else 0))
        if i%1000==0:print('coverage files',i,flush=True)
    stamps={s:np.unique(np.concatenate(v)) for s,v in stamps.items()}
    storeids=defaultdict(list)
    for r in store_rows:storeids[(str(r['candidate_id']),r['leg'])].append(r['store'])
    rows=[]
    for c in features:
        start,end=bounds(c);entry=ts(c['entry_time']);legs=[]
        r={k:c[k] for k in ('candidate_id','session_date','expiration','calendar_dte','target_credit','entry_time','short_symbol','long_symbol','partition','degraded','path_crosses_degraded_date')}
        r.update(window_start_ns=start,window_end_exclusive_ns=end)
        for leg in ('short','long'):
            a=clip(stamps.get(c[leg+'_symbol'],np.array([],dtype=np.int64)),start,end);legs.append(a)
            r.update({leg+'_raw_events':raw_counts[(c['candidate_id'],leg)],leg+'_unique_timestamps':len(a),leg+'_first_ns':int(a[0]) if len(a) else None,leg+'_last_ns':int(a[-1]) if len(a) else None,leg+'_gaps_over_1s':int((np.diff(a)>NS).sum()),leg+'_max_gap_ns':int(np.diff(a).max()) if len(a)>1 else None,leg+'_stores':json.dumps(sorted(storeids[(c['candidate_id'],leg)]),separators=(',',':'))})
        r['has_owned_pair']=bool(len(legs[0]) and len(legs[1]))
        r['entry_fresh_pair_seconds']=time_coverage(*legs,entry,entry+60*NS)
        r['preentry_fresh_pair_seconds']=time_coverage(*legs,entry-60*NS,entry)
        r['entry_supported']=r['entry_fresh_pair_seconds']>=57
        rows.append(r)
    pd.DataFrame(store_rows).to_parquet(OUT/'candidate_store_coverage.parquet',index=False)
    # Header symbol membership is retained even when no matching events exist.
    for h in inventory:
        if 'path' in h:h['store']=hashlib.sha256(h['path'].encode()).hexdigest()[:24]
    writejson(OUT/'store_inventory.json',inventory)
    return rows,stamps,stats


def minute_windows(features,candidates,coverage,stamps,pilot_ids):
    sessions=defaultdict(list)
    for i,c in enumerate(candidates):sessions[str(c.session_date)].append((i,c))
    out=[];selected={};covmap={int(r['candidate_id']):r for r in coverage}
    for count,(session,paths) in enumerate(session_paths(BASE/f'paths_{WINDOW}.csv')):
        for cid,c in sessions[session]:
            series,_=paired_path(c,paths)
            if cid in pilot_ids:selected[cid]=[(int(pd.Timestamp(t).value),m) for t,m in series]
            aa=stamps.get(c.short_symbol,np.array([],dtype=np.int64));bb=stamps.get(c.long_symbol,np.array([],dtype=np.int64))
            levels=[('target_mid_3',3,False),('baseline_target_2.95',2.95,False),('adverse_entry_plus_0.5',c.mid_credit+.5,True),('adverse_7',7,True),('adverse_8',8,True),('adverse_9',9,True)]
            r=covmap[cid]
            for name,level,above in levels:
                prev=False;n=covered=0
                for t,m in series:
                    yes=m>=level if above else m<=level
                    if yes and not prev:
                        n+=1;t=ts(t);sec=time_coverage(aa,bb,t-30*NS,t+60*NS);covered+=sec>0
                        out.append(dict(candidate_id=cid,kind=name,minute_ns=t,start_ns=t-30*NS,end_ns=t+60*NS,fresh_pair_seconds=sec))
                    prev=yes
                r[name+'_crossing_windows']=n;r[name+'_windows_any_owned']=covered
        if count%150==0:print('minute coverage sessions',count,flush=True)
    pd.DataFrame(out).to_parquet(OUT/'minute_window_coverage.parquet',index=False)
    return selected


def load_pilot_events(stats,features):
    symbols={c[k] for c in features for k in ('short_symbol','long_symbol')}
    paths=sorted({r['parquet'] for r in stats if r['symbol'] in symbols});groups=defaultdict(list)
    for p in paths:
        df=pd.read_parquet(p);df=df[df.symbol.isin(symbols)]
        for s,g in df.groupby('symbol'):groups[s].append(g)
    result={}
    for s,gg in groups.items():
        g=pd.concat(gg).drop_duplicates().sort_values(['ts_recv','ts_event'],kind='stable')
        result[s]=[(int(r.ts_recv),float(r.bid_px_00)/1e9,float(r.ask_px_00)/1e9,int(r.bid_sz_00),int(r.ask_sz_00),r.action.decode(),float(r.price)/1e9,int(r.size),int(r.ts_event)) for r in g.itertuples()]
    return result


def snapshot(rows,t,times=None):
    if times is None:times=[r['t'] for r in rows]
    i=bisect_right(times,t)-1
    if i<0 or not valid(rows[i]) or rows[i]['fresh_until']<t:return None
    return rows[i]['mid']


def measurements(rows,start,end):
    rr=[r for r in rows if start<=r['t']<end];good=[r for r in rr if valid(r)];dur=sum(min(r['duration_s'],(end-r['t'])/NS) for r in good)
    d=dict(events=len(rr),valid_events=len(good),fresh_seconds=dur,stale_events=sum(r['stale'] for r in rr),unmatched_events=sum(r['unmatched'] for r in rr),invalid_events=sum(r['invalid'] for r in rr),locked_events=sum(r['locked'] for r in rr),trade_events=sum(r['trade_events'] for r in rr),first_mid=good[0]['mid'] if good else None,last_mid=good[-1]['mid'] if good else None,median_mid=float(np.median([r['mid'] for r in good])) if good else None,median_width=float(np.median([r['width'] for r in good])) if good else None,updates_per_second=len(rr)/((end-start)/NS))
    for side in ('bid','ask'):
        d[side+'_min']=min((r[side] for r in good),default=None);d[side+'_max']=max((r[side] for r in good),default=None)
    for side in ('bid','mid','ask'):
        d[side+'_target_seconds']=sum(min(r['duration_s'],(end-r['t'])/NS) for r in good if r[side]<=3)
        d[side+'_target_crossings']=len(crossings(rr,side,3))
    return d


def persistence_at(series,i,n):
    # Same frozen forward consecutive-minute criterion applied to this touch.
    return i+n<=len(series) and all(series[j][1]+.05<=3 and (j==i or series[j][0]-series[j-1][0]==60*NS) for j in range(i,i+n))


def analyze(features,events,minute):
    entries=[];targets=[];false=[];taxonomy=[];touch_rows=[];second_rows=[];fast_rows=[];evidence_rows=[]
    for ix,c in enumerate(features):
        if not development(c['session_date']) or c['expiration']>'2024-12-31':raise ValueError('Execution analysis is development only')
        cid=int(c['candidate_id']);start,end=bounds(c);entry=ts(c['entry_time']);credit=float(c['target_credit'])
        rows=synchronize(events.get(c['short_symbol'],[]),events.get(c['long_symbol'],[]),start,end)
        times=[r['t'] for r in rows]
        rr=[r for r in rows if entry<=r['t']<entry+60*NS];fill=next((r['t'] for r in rr if valid(r) and r['mid']>=credit),None)
        e=dict(candidate_id=cid,session_date=c['session_date'],calendar_dte=c['calendar_dte'],target_credit=credit,modeled_fill_ns=fill,entry_model='hypothetical_midpoint',frozen_baseline_fill_ns=entry,frozen_baseline_credit=float(c['entry_mid'])-.05,baseline_entry_verified=False,**measurements(rows,entry,entry+60*NS))
        e['marketable_seconds']=sum(r['duration_s'] for r in rr if valid(r) and r['bid']>=credit);e['plausible_seconds']=sum(r['duration_s'] for r in rr if valid(r) and r['mid']>=credit)
        e['marketable_fraction']=e['marketable_seconds']/60;e['plausible_fraction']=e['plausible_seconds']/60;e['entry_credit_crossings']=len(crossings(rr,'mid',credit,'above'))
        context=measurements(rows,start,entry+300*NS)
        for k in ('first_mid','last_mid','median_mid'):e['around_entry_'+k]=context[k]
        for seconds in (5,15,30,60,120,300):e[f'plus_{seconds}s_mid']=snapshot(rows,entry+seconds*NS,times)
        entries.append(e)
        post=[r for r in rows if fill is not None and r['t']>fill];series=minute.get(cid,[]);touches=[i for i,(_,m) in enumerate(series) if m+.05<=3]
        times=[r['t'] for r in rows];classes=Counter();conditional_classes=Counter();persist_false=Counter();conditional_persist=Counter();supported=0
        for i in touches:
            t=series[i][0];lo=t-30*NS;hi=t+60*NS;sl=rows[bisect_left(times,lo):bisect_left(times,hi)]
            cl=classify(sl,fill,max(lo,(fill+1) if fill is not None else lo),hi);classes[cl]+=1
            conditional=classify(sl,entry,max(lo,entry+1),hi);conditional_classes[conditional]+=1
            p2=persistence_at(series,i,2);p3=persistence_at(series,i,3)
            strong=cl=='strong fill evidence';plausible=cl in ('strong fill evidence','plausible fill evidence')
            if plausible and not p2:persist_false['p2_rejected_supported']+=1
            if plausible and not p3:persist_false['p3_rejected_supported']+=1
            if p2 and not strong:persist_false['p2_approved_not_strong']+=1
            if p3 and not strong:persist_false['p3_approved_not_strong']+=1
            cs=conditional=='strong fill evidence';cp=conditional in ('strong fill evidence','plausible fill evidence')
            if cp and not p2:conditional_persist['p2_rejected_supported']+=1
            if cp and not p3:conditional_persist['p3_rejected_supported']+=1
            if p2 and not cs:conditional_persist['p2_approved_not_strong']+=1
            if p3 and not cs:conditional_persist['p3_approved_not_strong']+=1
            metrics=measurements(sl,lo,hi);supported+=metrics['fresh_seconds']>0
            tr=dict(candidate_id=cid,touch_ns=t,classification=cl,conditional_classification=conditional,persist_2=p2,persist_3=p3,**metrics)
            nexts=[r for r in sl if r['t']>t and valid(r)]
            tr['next_event_mid']=nexts[0]['mid'] if nexts else None
            for h in (1,5,15,30,60):tr[f'plus_{h}s_mid']=snapshot(rows,t+h*NS,times)
            good=[r for r in sl if valid(r) and r['mid']<=3]
            tr['minimum_target_leg_size']=min((min(r[k] for k in ('short_bid_size','short_ask_size','long_bid_size','long_ask_size')) for r in good),default=None)
            touch_rows.append(tr)
            er={r['t']:r for field in ('bid','mid','ask') for r in crossings(sl,field,3,require_transition=False)}
            for r in er.values():evidence_rows.append(dict(candidate_id=cid,**r))
        missed=missed_crossings(rows,[series[i][0] for i in touches],fill)
        conditional_missed=missed_crossings(rows,[series[i][0] for i in touches],entry)
        raw_touch_times=[t for t,m in series if m<=3]
        raw_missed=missed_crossings(rows,raw_touch_times,entry)
        raw_touch_bins={t//(60*NS) for t in raw_touch_times}
        raw_ask_missed=[r for r in crossings([r for r in rows if r['t']>entry],'ask',3) if r['t']//(60*NS) not in raw_touch_bins]
        # Ask and midpoint evidence are separate; bid alone never implies a fill.
        f=dict(candidate_id=cid,session_date=c['session_date'],calendar_dte=c['calendar_dte'],target_credit=credit,modeled_entry=fill is not None,minute_touches=len(touches),minute_touches_any_fresh_events=supported,missed_minute_mid_crossings=len(missed),missed_minute_ask_crossings=sum(r['ask']<=3 for r in missed),**{k:persist_false[k] for k in ('p2_rejected_supported','p3_rejected_supported','p2_approved_not_strong','p3_approved_not_strong')})
        actual_fast=fast_sequences(post)
        conditional_fast=fast_sequences([r for r in rows if r['t']>entry])
        for scenario,found in [('supported_entry',actual_fast),('conditional_baseline_entry',conditional_fast)]:
            fast_rows.extend(dict(candidate_id=cid,scenario=scenario,**r) for r in found)
        f['fast_6_5_4_3_rebound_sequences']=len(actual_fast)
        f['conditional_fast_6_5_4_3_rebound_sequences']=len(conditional_fast)
        f['conditional_missed_minute_mid_crossings']=len(conditional_missed)
        f['conditional_missed_raw_minute_mid_crossings']=len(raw_missed)
        f['conditional_missed_raw_minute_ask_crossings']=len(raw_ask_missed)
        f.update({'conditional_'+k:conditional_persist[k] for k in ('p2_rejected_supported','p3_rejected_supported','p2_approved_not_strong','p3_approved_not_strong')})
        false.append(f)
        targets.append(dict(candidate_id=cid,session_date=c['session_date'],calendar_dte=c['calendar_dte'],target_credit=credit,resting_activation_ns=fill,minute_touches=len(touches),minute_touches_any_fresh_events=supported,**{k:classes[k] for k in ('strong fill evidence','plausible fill evidence','ambiguous','unlikely fill')},**{'conditional_'+k:conditional_classes[k] for k in ('strong fill evidence','plausible fill evidence','ambiguous','unlikely fill')},**measurements([r for r in rows if r['t']>=entry],entry,end)))
        # Regular-session denominator from observed minute calendar, conservatively 390m/day.
        trading_dates={pd.Timestamp(t,unit='ns',tz='UTC').tz_convert('America/New_York').date() for t,_ in series}
        observed_post=[r for r in rows if r['t']>=entry]
        expected=len(trading_dates)*390*60;covered=sum(r['duration_s'] for r in observed_post if valid(r));hit=next((r for r in observed_post if valid(r) and r['mid']<=3),None)
        before=[r for r in observed_post if valid(r) and (hit is None or r['t']<=hit['t'])];maximum=max((r['mid'] for r in before),default=None)
        cat='censored/insufficient coverage'
        if fill is not None and expected and covered/expected>=.95:
            cat=('severe adverse ($9+) then success' if maximum>=9 else 'adverse-then-success' if maximum>credit+.5 else 'clean compression') if hit else 'failure/no target'
        taxonomy.append(dict(candidate_id=cid,session_date=c['session_date'],calendar_dte=c['calendar_dte'],target_credit=credit,category=cat,observed_fresh_seconds=covered,expected_regular_seconds=expected,coverage_fraction=covered/expected if expected else 0,first_target_ns=hit['t'] if hit else None,max_before_observed_target=maximum,reason='missing entry or incomplete path' if cat.startswith('censored') else 'covered path'))
        groups=defaultdict(list);second_duration=Counter();held_states=defaultdict(list)
        for r in rows:
            groups[r['t']//NS].append(r)
            if valid(r):
                stop=r['t']+int(round(r['duration_s']*NS));a=r['t']
                while a<stop:
                    b=min(stop,(a//NS+1)*NS);second_duration[a//NS]+=(b-a)/NS;held_states[a//NS].append(r);groups.setdefault(a//NS,[]);a=b
        for sec,rs in groups.items():
            good=sorted([r for r in rs if valid(r)]+held_states[sec],key=lambda r:r['t'])
            second_rows.append(dict(candidate_id=cid,second_ns=sec*NS,events=len(rs),fresh_seconds=second_duration[sec],mid_min=min((r['mid'] for r in good),default=None),mid_max=max((r['mid'] for r in good),default=None),last_mid=good[-1]['mid'] if good else None,stale_events=sum(r['stale'] for r in rs),invalid_events=sum(r['invalid'] for r in rs),trade_events=sum(r['trade_events'] for r in rs)))
        if ix%30==0:print('analyzed candidates',ix,flush=True)
    for name,rr in [('entry_microstructure',entries),('target_microstructure',targets),('false_negative_analysis',false),('path_taxonomy',taxonomy)]:writecsv(OUT/(name+'.csv'),rr)
    pd.DataFrame(evidence_rows).drop_duplicates(subset=['candidate_id','t']).to_parquet(OUT/'event_crossing_evidence.parquet',index=False) if evidence_rows else pd.DataFrame(columns=['candidate_id','t']).to_parquet(OUT/'event_crossing_evidence.parquet',index=False)
    for name,rr in [('touch_window_evidence',touch_rows),('second_aggregates',second_rows),('fast_sequences',fast_rows)]:pd.DataFrame(rr).to_parquet(OUT/(name+'.parquet'),index=False)
    return entries,targets,false,taxonomy


def subtract_intervals(start,end,intervals):
    cursor=start;gaps=[]
    for a,b in sorted(intervals):
        if b<=cursor or a>=end:continue
        if a>cursor:gaps.append((cursor,min(a,end)))
        cursor=max(cursor,min(b,end))
    if cursor<end:gaps.append((cursor,end))
    return gaps


def main():
    p=argparse.ArgumentParser();p.add_argument('--cache',default='/tmp/phase3b_events');p.add_argument('--reuse-coverage',action='store_true');args=p.parse_args();cache=Path(args.cache);OUT.mkdir(parents=True,exist_ok=True)
    features=list(csv.DictReader((BASE/'deep_dive_v1/path_features.csv').open()));candidates=load_candidates(BASE/f'candidates_{WINDOW}.csv')
    if args.reuse_coverage:
        manifest=json.loads((OUT/'pilot_manifest.json').read_text())
        for path,digest in manifest['frozen_inputs_sha256'].items():assert sha(path)==digest
        assert json.loads((OUT/'execution_classification_rules.json').read_text())==RULES
        for h in json.loads((cache/'store_inventory.json').read_text()):
            if 'mtime_ns' in h:
                st=Path(h['path']).stat();assert (st.st_size,st.st_mtime_ns)==(h['bytes'],h['mtime_ns'])
        coverage=list(csv.DictReader((OUT/'coverage_matrix.csv').open()))
        for r in coverage:
            for key in ('has_owned_pair','entry_supported','settlement_available'):r[key]=r[key]=='True'
        stats=json.loads((cache/'leg_store_stats.json').read_text())
        dates=manifest['sessions'];ids=set(manifest['candidate_ids']);pilot=[c for c in features if int(c['candidate_id']) in ids]
        minute={int(k):v for k,v in json.loads((cache/'pilot_minutes.json').read_text()).items()}
        frozen={name:sha(OUT/name) for name in ('pilot_manifest.json','execution_classification_rules.json')}
    else:
        coverage,stamps,stats=build_coverage(cache,features)
        dates=select_pilot(coverage);ids={int(r['candidate_id']) for r in coverage if r['session_date'] in dates and r['expiration']<='2024-12-31'};pilot=[c for c in features if int(c['candidate_id']) in ids]
        manifest=dict(selection='month round-robin with farthest-date spacing; only dates and actual timestamp coverage used',requested_sessions=60,diagnostic_sessions=len(dates),supported_entry_sessions=len({r['session_date'] for r in coverage if r['session_date'] in dates and r['entry_supported']}),sessions=dates,candidate_ids=sorted(ids),candidates=[{k:c[k] for k in ('candidate_id','session_date','calendar_dte','target_credit','short_symbol','long_symbol','entry_time','expiration')} for c in pilot],frozen_inputs_sha256={str(x):sha(x) for x in (BASE/f'candidates_{WINDOW}.csv',BASE/'deep_dive_v1/path_features.csv')},outcomes_examined_for_selection=False)
        writejson(OUT/'pilot_manifest.json',manifest);writejson(OUT/'execution_classification_rules.json',RULES)
        frozen={name:sha(OUT/name) for name in ('pilot_manifest.json','execution_classification_rules.json')}
        print('MANIFEST AND RULES SAVED BEFORE OUTCOMES',frozen,flush=True)
        minute=minute_windows(features,candidates,coverage,stamps,ids)
        writejson(cache/'pilot_minutes.json',minute)
    settlements=[]
    owned=Path('/Users/patrick/Dev/MGC-v05l-ndxp-research/outputs')
    sources=sorted(owned.rglob('xqc_settlements.csv'))
    for path in sources:
        for r in csv.DictReader(path.open()):settlements.append(r)
    audit=[]
    for c,co,ca in zip(features,coverage,candidates):
        value,status=settlement_join(c['expiration'],settlements);co['settlement_status']=status;co['settlement_available']=value is not None
        # No later-period payoff analysis, even though date/source coverage is indexed.
        if int(c['candidate_id']) not in ids:continue
        intrinsic=settlement_payoff(value,ca.short_strike,ca.long_strike) if value is not None else None
        last=float(c['terminal_mid']) if c['terminal_mid'] else None
        audit.append(dict(candidate_id=c['candidate_id'],expiration=c['expiration'],xqc=value,source='FRED NASDAQXQC / Nasdaq XQC' if value else None,status=status,independent_official_verification=False,intrinsic=intrinsic,last_quote_mid=last,last_quote_time=c['last_quote_time'],last_quote_age_minutes=c['terminal_quote_age_minutes'],intrinsic_minus_last_quote=intrinsic-last if intrinsic is not None and last is not None else None))
    writecsv(OUT/'coverage_matrix.csv',coverage);writecsv(OUT/'settlement_audit.csv',audit)
    events=load_pilot_events(stats,pilot);entries,targets,false,taxonomy=analyze(pilot,events,minute)
    assert frozen=={name:sha(OUT/name) for name in frozen}
    summary=dict(requested_sessions=60,diagnostic_sessions=len(dates),pilot_candidates=len(ids),entry_supported_sessions=manifest['supported_entry_sessions'],modeled_entry_candidates=sum(r['modeled_fill_ns'] is not None for r in entries),complete_path_candidates=sum(not r['category'].startswith('censored') for r in taxonomy),coverage_candidates=len(coverage),owned_pair_candidates=sum(r['has_owned_pair'] for r in coverage),entry_supported_candidates=sum(r['entry_supported'] for r in coverage),indexed_files=len(json.loads((cache/'store_inventory.json').read_text())),decoded_files=len({r['store'] for r in stats}),minute_touches=sum(r['minute_touches'] for r in targets),minute_touches_any_fresh_events=sum(r['minute_touches_any_fresh_events'] for r in targets),classes=dict(Counter({k:sum(r[k] for r in targets) for k in ('strong fill evidence','plausible fill evidence','ambiguous','unlikely fill')})),false_negatives={k:sum(r[k] for r in false) for k in ('missed_minute_mid_crossings','missed_minute_ask_crossings','p2_rejected_supported','p3_rejected_supported','p2_approved_not_strong','p3_approved_not_strong','fast_6_5_4_3_rebound_sequences')},taxonomy=dict(Counter(r['category'] for r in taxonomy)),settlements=dict(covered=sum(r['intrinsic'] is not None for r in audit),missing_expirations=sorted({r['expiration'] for r in audit if r['intrinsic'] is None}),nonzero_quote_intrinsic_differences=sum(r['intrinsic_minus_last_quote'] is not None and abs(r['intrinsic_minus_last_quote'])>1e-9 for r in audit),max_abs_difference=max((abs(r['intrinsic_minus_last_quote']) for r in audit if r['intrinsic_minus_last_quote'] is not None),default=None)),pf_assessment='unresolved',network_calls=0,download_calls=0,frozen_before_outcomes_sha256=frozen)
    summary['by_dte_credit']=[dict(calendar_dte=d,target_credit=k,candidates=sum(int(r['calendar_dte'])==d and r['target_credit']==k for r in targets),minute_touches=sum(r['minute_touches'] for r in targets if int(r['calendar_dte'])==d and r['target_credit']==k),**{cl:sum(r[cl] for r in targets if int(r['calendar_dte'])==d and r['target_credit']==k) for cl in summary['classes']}) for d in (2,3) for k in (5.,6.,7.)]
    # Price only the exact selected contracts and regular trading dates through expiry.
    # Phase 3A provider quote: 6 symbols x 30 min = 7,465,120 billable bytes / $0.001112389565.
    requests={}
    for c in pilot:
        for day in sorted({pd.Timestamp(t,unit='ns',tz='UTC').tz_convert('America/New_York').strftime('%Y-%m-%d') for t,_ in minute.get(int(c['candidate_id']),[])}):
            key=day;requests.setdefault(key,set()).update([c['short_symbol'],c['long_symbol']])
    owned_intervals=defaultdict(list)
    for h in json.loads((cache/'store_inventory.json').read_text()):
        if h.get('error') or h.get('quarantine'):continue
        for symbol in h.get('candidate_symbols',[]):owned_intervals[symbol].append((h['start_ns'],h['end_ns']))
    gaps=defaultdict(set);requested_minutes=0
    for d,symbols in sorted(requests.items()):
        start=pd.Timestamp(d+' 09:30',tz='America/New_York').value
        end=pd.Timestamp(d+' 16:00',tz='America/New_York').value
        for symbol in sorted(symbols):
            requested_minutes+=390
            for a,b in subtract_intervals(start,end,owned_intervals[symbol]):gaps[(d,a,b)].add(symbol)
    acquisition=[]
    for (d,a,b),symbols in sorted(gaps.items()):
        symbols=sorted(symbols)
        for i in range(0,len(symbols),24):
            acquisition.append(dict(date=d,symbols=symbols[i:i+24],start=pd.Timestamp(a,unit='ns',tz='UTC').tz_convert('America/New_York').isoformat(),end=pd.Timestamp(b,unit='ns',tz='UTC').tz_convert('America/New_York').isoformat(),schema='cmbp-1',dataset='OPRA.PILLAR',stype_in='raw_symbol'))
    minutes=sum(len(r['symbols'])*(ts(r['end'])-ts(r['start']))/(60*NS) for r in acquisition);central=minutes/180*.001112389565;gb=minutes/180*7465120/1024**3
    summary['next_acquisition']=dict(purpose='complete observation panel for unbiased intraminute missed-touch detection; exact selected legs, development regular sessions, owned header intervals subtracted',requests=len(acquisition),symbol_minutes=minutes,owned_symbol_minutes_reused=requested_minutes-minutes,estimated_usd_low=central*.25,estimated_usd_central=central,estimated_usd_high=central*10,estimated_billable_gib_central=gb,basis='linear scaling of saved 2024-06-12 provider 6-leg 30-minute CMBP sample; 0.25x..10x planning envelope, not guaranteed price cap; no paid calls',metadata_calls_needed_for_this_estimate=0,purchase_authorized=False)
    summary['conditional_classes']={k:sum(r['conditional_'+k] for r in targets) for k in summary['classes']}
    summary['conditional_false_negatives']={k:sum(r[k] for r in false) for k in false[0] if k.startswith('conditional_')}
    summary['conditional_by_dte_credit']=[dict(calendar_dte=d,target_credit=k,candidates=sum(int(r['calendar_dte'])==d and r['target_credit']==k for r in targets),minute_touches=sum(r['minute_touches'] for r in targets if int(r['calendar_dte'])==d and r['target_credit']==k),**{cl:sum(r['conditional_'+cl] for r in targets if int(r['calendar_dte'])==d and r['target_credit']==k) for cl in summary['classes']}) for d in (2,3) for k in (5.,6.,7.)]
    writejson(OUT/'minimum_acquisition_manifest.json',acquisition);writejson(OUT/'summary.json',summary)
    source_manifest=[dict(path=str(x),sha256=sha(x)) for x in sources]
    writejson(OUT/'settlement_sources.json',source_manifest)
    print(json.dumps(summary,indent=2),flush=True)

if __name__=='__main__':main()
