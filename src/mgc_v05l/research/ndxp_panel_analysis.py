"""Bounded-memory Phase 3C event-panel replay; frozen descriptive models only."""
from __future__ import annotations
import json,csv,math
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd
import exchange_calendars as xc
import databento as db
from .ndxp_complete_panel import OUT,OLD,save,digest,merge
from .ndxp_owned_execution import NS
from .ndxp_owned_pilot import writecsv,subtract_intervals
from .ndxp_multidte_data import load_candidates
from .ndxp_multidte_path_features import target_exit,settlement_payoff

RULES=dict(version=1,max_leg_age_ns=NS,entry='Phase 3B first fresh valid midpoint >= nominal cohort credit in first 60s; hypothetical complex midpoint support, not proven fill',entry_price='nominal credit minus 0.05; conservative nominal minus 0.25',resting='active strictly after modeled entry timestamp, first qualifying event closes the modeled position',event_strong='spread ask <=3 with positive short ask/long bid sizes',event_plausible='spread midpoint <=3 and positive sizes on all four leg sides',conservative='spread ask +0.25 <=3, entry slippage 0.25; exit limit debit 3',fee_per_contract_per_side=1.324,contracts=20,terminal='owned validated XQC intrinsic; no evidence fill means hypothetical hold-to-settlement, not proof of no real fill',complete='full exact-symbol requested regular-session source intervals AND >=95% fresh valid paired duration AND >=57 of first60 fresh seconds',models=['minute_touch_original_quote_terminal','minute_touch_settlement','persist_2_settlement','persist_3_settlement','event_strong','event_strong_plausible','event_conservative'],no_tuning=True)


def sync_arrays(short,long,start,end):
    """Equivalent as-of state to Phase 3B, using arrays instead of Python row objects."""
    legs=[]
    for df in (short,long):
        df=df[(df.ts_recv>=start-NS)&(df.ts_recv<end)].sort_values(['ts_recv','ts_event'],kind='stable').drop_duplicates('ts_recv',keep='last')
        legs.append(df)
    if any(len(d)==0 for d in legs):return {}
    s,l=legs;t=np.union1d(s.ts_recv.to_numpy(dtype=np.int64),l.ts_recv.to_numpy(dtype=np.int64));t=t[(t>=start)&(t<end)]
    ii=[np.searchsorted(d.ts_recv.to_numpy(dtype=np.int64),t,side='right')-1 for d in legs]
    unmatched=(ii[0]<0)|(ii[1]<0);ii=[np.maximum(x,0) for x in ii]
    values=[]
    for d,i in zip(legs,ii):
        values.append({k:d[k].to_numpy(dtype=np.int64)[i] for k in ('ts_recv','bid_px_00','ask_px_00','bid_sz_00','ask_sz_00')})
    a,b=values;sb=a['bid_px_00']/1e9;sa=a['ask_px_00']/1e9;lb=b['bid_px_00']/1e9;la=b['ask_px_00']/1e9
    bid=sb-la;ask=sa-lb;mid=(bid+ask)/2
    stale=unmatched|(t-a['ts_recv']>NS)|(t-b['ts_recv']>NS)
    invalid=unmatched|(sb<0)|(lb<0)|(sa<=0)|(la<=0)|(sb>sa)|(lb>la)|(sa>=1e9)|(la>=1e9)|(mid<0)|(mid>10)|~np.isfinite(mid)
    good=~(stale|invalid)
    until=np.minimum(np.minimum(a['ts_recv']+NS,b['ts_recv']+NS),np.r_[t[1:],end]);duration=np.where(good,np.maximum(0,until-t)/NS,0)
    sizes=(a['bid_sz_00']>0)&(a['ask_sz_00']>0)&(b['bid_sz_00']>0)&(b['ask_sz_00']>0)
    strong=good&(ask<=3)&(a['ask_sz_00']>0)&(b['bid_sz_00']>0)
    plausible=good&(mid<=3)&sizes
    conservative=good&(ask+.25<=3)&(a['ask_sz_00']>0)&(b['bid_sz_00']>0)
    continuous=np.r_[False,good[:-1]&(until[:-1]>=t[1:])]&good
    cross_mid=continuous&(mid<=3)&np.r_[False,mid[:-1]>3]
    cross_ask=continuous&(ask<=3)&np.r_[False,ask[:-1]>3]
    trade=np.zeros(len(t),dtype=np.int64)
    for d in legs:
        z=d[d.action==b'T'].ts_recv.to_numpy(dtype=np.int64);z=z[(z>=start)&(z<end)];i=np.searchsorted(t,z);i=i[i<len(t)];np.add.at(trade,i,1)
    return dict(t=t,bid=bid,ask=ask,mid=mid,width=ask-bid,good=good,stale=stale,unmatched=unmatched,invalid=invalid,locked=(sb==sa)|(lb==la),duration=duration,until=until,strong=strong,plausible=plausible,conservative=conservative,cross_mid=cross_mid,cross_ask=cross_ask,trade_events=trade,positive_sizes=sizes,short_bid_size=a['bid_sz_00'],short_ask_size=a['ask_sz_00'],long_bid_size=b['bid_sz_00'],long_ask_size=b['ask_sz_00'])


def write_derived(path,value):
    temporary=path.with_suffix('.json.tmp')
    temporary.write_text(json.dumps(value,indent=2,sort_keys=True,allow_nan=False)+'\n')
    temporary.replace(path)


def first_index(mask):
    ii=np.flatnonzero(mask);return int(ii[0]) if len(ii) else None

def pnl(entry,debit):return (entry-debit)*2000-1.324*40

def summarize(rows):
    values=[r['pnl'] for r in rows if r['pnl'] is not None]
    wins=sum(v for v in values if v>0);loss=-sum(v for v in values if v<0)
    return dict(trades=len(values),excluded=sum(r['pnl'] is None for r in rows),mean_pnl=float(np.mean(values)) if values else None,win_rate=sum(v>0 for v in values)/len(values) if values else None,profit_factor=wins/loss if loss else None,gross_profit=wins,gross_loss=loss)


def fast_count(t,mid,good,until):
    """Run-compressed equivalent of Phase 3B fast_sequences, including timeouts."""
    if not len(t):return 0
    band=np.select([mid>=6,mid>5,mid>4,mid>3],[4,3,2,1],default=0)
    breaks=~good|np.r_[False,until[:-1]<t[1:]]
    starts=np.flatnonzero(np.r_[True,band[1:]!=band[:-1]]|breaks|np.r_[False,~good[:-1]])
    ends=np.r_[starts[1:],len(t)];seq=[];count=0
    for j,k in zip(starts,ends):
        if breaks[j]:seq=[]
        if not good[j]:continue
        if seq and t[j]-seq[0]>60*NS:seq=[]
        if not seq and mid[j]>=6:seq=[t[j]]
        elif seq:
            while len(seq)<4 and mid[j]<=(5,4,3)[len(seq)-1]:seq.append(t[j])
            if len(seq)==4 and mid[j]>3:count+=1;seq=[]
        if band[j]==4:
            if not seq and j+1<k:seq=[t[j+1]]
            while seq and t[k-1]-seq[0]>60*NS:
                z=np.searchsorted(t,seq[0]+60*NS,side='right')
                if z>=k:break
                seq=[t[z]]
    return count


def cache_new():
    cache=Path('/tmp/phase3c_events');cache.mkdir(exist_ok=True)
    receipts=json.loads((OUT/'acquisition_receipts.json').read_text());records=[]
    if receipts['completed_requests']!=receipts['planned_requests']:raise ValueError('Finish acquisition before building the complete-panel cache')
    for receipt in receipts['receipts']:
        for f in receipt['files']:
            p=Path(f['path'])
            if not p.name.endswith('.dbn.zst'):continue
            key=f['sha256'];dest=cache/(key+'.parquet');h=db.DBNStore.from_file(p).metadata
            if str(h.schema)!='cmbp-1':raise ValueError('Unexpected schema')
            if not dest.exists():
                import pyarrow as pa,pyarrow.parquet as pq
                writer=None;temporary=dest.with_suffix('.parquet.part')
                if temporary.exists():raise ValueError('Incomplete derived cache; preserve and move aside before rebuilding')
                for df in db.DBNStore.from_file(p).to_df(price_type='fixed',pretty_ts=False,map_symbols=True,count=250000):
                    df=df.reset_index();cols=['symbol','ts_recv','ts_event','bid_px_00','ask_px_00','bid_sz_00','ask_sz_00','action','price','size'];df=df[cols]
                    # DBN dataframe action uses categorical strings; normalize to bytes.
                    df['action']=df.action.astype(str).map(lambda s:s.encode())
                    table=pa.Table.from_pandas(df,preserve_index=False)
                    if writer is None:writer=pq.ParquetWriter(temporary,table.schema)
                    writer.write_table(table)
                if writer:
                    writer.close();temporary.rename(dest)
                else:pd.DataFrame(columns=['symbol','ts_recv','ts_event','bid_px_00','ask_px_00','bid_sz_00','ask_sz_00','action','price','size']).to_parquet(dest,index=False)
            for symbol in h.mappings:records.append(dict(symbol=symbol,parquet=str(dest),start=int(h.start),end=int(h.end),path=str(p)))
    save(OUT/'new_raw_index.json',records)
    return records


def load_legs(c,index):
    groups=defaultdict(list);start=pd.Timestamp(c.entry_time).value-NS;end=pd.Timestamp(str(c.expiration)+' 16:00',tz='America/New_York').value
    symbols=[c.short_symbol,c.long_symbol]
    paths=sorted({r['parquet'] for s in symbols for r in index.get(s,[]) if r['start']<end and r['end']>start})
    columns=['symbol','ts_recv','ts_event','bid_px_00','ask_px_00','bid_sz_00','ask_sz_00','action','price','size']
    for path in paths:
        df=pd.read_parquet(path,columns=columns,filters=[('symbol','in',symbols),('ts_recv','>=',start),('ts_recv','<',end)])
        for s,g in df.groupby('symbol'):groups[s].append(g)
    return [pd.concat(groups[s],ignore_index=True).drop_duplicates() if groups[s] else pd.DataFrame(columns=columns) for s in symbols]


def analyze(cache=Path('/tmp/phase3b_events')):
    import gc
    if not (OUT/'execution_rules.json').exists():save(OUT/'execution_rules.json',RULES)
    assert json.loads((OUT/'execution_rules.json').read_text())==RULES
    freeze=digest(OUT/'execution_rules.json')
    panel=json.loads((OLD/'pilot_manifest.json').read_text());ids=panel['candidate_ids']
    new=json.loads((OUT/'new_raw_index.json').read_text()) if (OUT/'new_raw_index.json').exists() else cache_new()
    old=json.loads((cache/'leg_store_stats.json').read_text());index=defaultdict(list)
    for r in old:index[r['symbol']].append(dict(parquet=r['parquet'],start=r['header_start_ns'],end=r['header_end_ns']))
    for r in new:index[r['symbol']].append(r)
    # Header/source intervals are distinct from actual quote freshness.
    owned=defaultdict(list)
    for h in json.loads((OLD/'store_inventory.json').read_text()):
        if h.get('error') or h.get('quarantine'):continue
        for s in h.get('candidate_symbols',[]):owned[s].append((h['start_ns'],h['end_ns']))
    for r in new:owned[r['symbol']].append((r['start'],r['end']))
    candidates=load_candidates(Path('output/ndxp_multidte/candidates_2021-09-24_2026-09-24.csv'))
    minute={int(k):v for k,v in json.loads((cache/'pilot_minutes.json').read_text()).items()}
    sources=OLD/'settlement_sources.json'
    if sources.exists():
        for source in json.loads(sources.read_text()):
            if digest(source['path'])!=source['sha256']:raise ValueError('Settlement source changed; re-audit required')
    settlements={int(r['candidate_id']):r for r in csv.DictReader((OLD/'settlement_audit.csv').open())}
    cal=xc.get_calendar('XNYS');coverage=[];entries=[];targets=[];false=[];taxonomy=[];outcomes=[];settlement_rows=[];windows=[]
    for ordinal,cid in enumerate(ids):
        c=candidates[cid];assert str(c.expiration)<='2024-12-31' and str(c.session_date)>='2023-03-28'
        entry=pd.Timestamp(c.entry_time).value;end=cal.session_close(str(c.expiration)).value
        sessions=cal.sessions_in_range(str(c.session_date),str(c.expiration));intervals=[(max(entry,cal.session_open(d).value),cal.session_close(d).value) for d in sessions]
        expected=sum(b-a for a,b in intervals)/NS;gap_seconds=0
        for symbol in (c.short_symbol,c.long_symbol):
            gap_seconds+=sum(y-x for a,b in intervals for x,y in subtract_intervals(a,b,owned[symbol]))/NS
        s,l=load_legs(c,index);a=sync_arrays(s,l,entry,end);del s,l
        if not a:raise ValueError(f'No events for candidate {cid}')
        t=a['t'];good=a['good'];mid=a['mid'];duration=a['duration']
        in_session=np.zeros(len(t),bool)
        for lo,hi in intervals:
            inside=(t>=lo)&(t<hi);in_session|=inside
            a['until'][inside]=np.minimum(a['until'][inside],hi)
            duration[inside]=np.minimum(duration[inside],(hi-t[inside])/NS)
        good&=in_session;duration=np.where(good,duration,0)
        entry_mask=(t<entry+60*NS)&good;fill_i=first_index(entry_mask&(mid>=c.target_credit))
        fill=int(t[fill_i]) if fill_i is not None else None;after=(t>fill) if fill is not None else np.zeros(len(t),bool)
        fresh=float(duration.sum());entry_seconds=float(duration[entry_mask].sum());source_complete=gap_seconds==0;complete=source_complete and fresh/expected>=.95 and entry_seconds>=57
        base=dict(candidate_id=cid,session_date=str(c.session_date),expiration=str(c.expiration),calendar_dte=c.calendar_dte,target_credit=c.target_credit)
        coverage.append(dict(**base,source_complete=source_complete,missing_source_leg_seconds=gap_seconds,events=len(t),fresh_valid_seconds=fresh,required_seconds=expected,fresh_fraction=fresh/expected,entry_fresh_seconds=entry_seconds,phase3b_complete=complete,stale_events=int(a['stale'].sum()),invalid_events=int(a['invalid'].sum())))
        valid_entry_mid=mid[entry_mask]
        e=dict(**base,modeled_entry_supported=fill is not None,modeled_fill_ns=fill,modeled_credit=c.target_credit-.05,first_mid=float(valid_entry_mid[0]) if len(valid_entry_mid) else None,last_mid=float(valid_entry_mid[-1]) if len(valid_entry_mid) else None,median_mid=float(np.median(valid_entry_mid)) if len(valid_entry_mid) else None,entry_fresh_seconds=entry_seconds,marketable_seconds=float(duration[entry_mask&(a['bid']>=c.target_credit)].sum()),plausible_seconds=float(duration[entry_mask&(mid>=c.target_credit)].sum()),median_width=float(np.median(a['width'][entry_mask])) if entry_mask.any() else None)
        for h in (5,15,30,60,120,300):
            j=np.searchsorted(t,entry+h*NS,side='right')-1;e[f'plus_{h}s_mid']=float(mid[j]) if j>=0 and good[j] and a['until'][j]>=entry+h*NS else None
        e.update(entry_updates=int((t<entry+60*NS).sum()),updates_per_second=float((t<entry+60*NS).sum()/60),positive_sizes_at_fill=bool(a['positive_sizes'][fill_i]) if fill_i is not None else None,marketable_fraction=e['marketable_seconds']/60,plausible_fraction=e['plausible_seconds']/60)
        for side in ('bid','ask'):
            e[side+'_min']=float(a[side][entry_mask].min()) if entry_mask.any() else None
            e[side+'_max']=float(a[side][entry_mask].max()) if entry_mask.any() else None
        e['entry_credit_crossings']=int(np.sum(entry_mask & np.r_[False,good[:-1]&(a['until'][:-1]>=t[1:])&(mid[:-1]<c.target_credit)] & (mid>=c.target_credit)))
        entries.append(e)
        masks={'event_strong':after&good&a['strong'],'event_strong_plausible':after&good&(a['strong']|a['plausible']),'event_conservative':after&good&a['conservative']}
        hits={name:first_index(mask) for name,mask in masks.items()}
        target=dict(**base,entry_supported=fill is not None,**{name+'_first_ns':int(t[i]) if i is not None else None for name,i in hits.items()},**{name+'_events':int(mask.sum()) for name,mask in masks.items()})
        series=minute[cid];tt=np.array([x[0] for x in series],dtype=np.int64);mm=np.array([x[1] for x in series]);touch=mm+.05<=3;bins=set((tt[mm<=3]//(60*NS)).tolist())
        fc={};classes=defaultdict(int);p2reject=p3reject=p2nostrong=p3nostrong=0
        prefixes={name:np.r_[0,np.cumsum(mask.astype(np.int64))] for name,mask in masks.items()};durprefix=np.r_[0,np.cumsum(duration)]
        for i in np.flatnonzero(touch):
            lo=max(int(tt[i])-30*NS,(fill+1) if fill is not None else entry);hi=int(tt[i])+60*NS;j=np.searchsorted(t,lo);k=np.searchsorted(t,hi)
            strong=prefixes['event_strong'][k]>prefixes['event_strong'][j];plausible=prefixes['event_strong_plausible'][k]>prefixes['event_strong_plausible'][j]
            observed=max(0,float(durprefix[k]-durprefix[j]));classification='strong' if strong else 'plausible' if plausible else 'ambiguous'
            if fill is not None and observed>=.95*(hi-lo)/NS and not np.any(a['bid'][j:k]<=3):classification='unlikely'
            classes[classification]+=1
            p2=i+1<len(tt) and touch[i+1] and tt[i+1]-tt[i]==60*NS
            p3=p2 and i+2<len(tt) and touch[i+2] and tt[i+2]-tt[i+1]==60*NS
            p2reject+=plausible and not p2;p3reject+=plausible and not p3;p2nostrong+=p2 and not strong;p3nostrong+=p3 and not strong
            windows.append(dict(candidate_id=cid,touch_ns=int(tt[i]),classification=classification,fresh_seconds=observed,persist_2=bool(p2),persist_3=bool(p3)))
        for field in ('cross_mid','cross_ask'):
            crossed=t[after&good&a[field]];fc['missed_minute_'+field]=sum(int(x//(60*NS)) not in bins for x in crossed)
        fast=fast_count(t[after],mid[after],good[after],a['until'][after])
        false.append(dict(**base,entry_supported=fill is not None,minute_touches=int(touch.sum()),p2_rejected_supported=int(p2reject),p3_rejected_supported=int(p3reject),p2_approved_not_strong=int(p2nostrong),p3_approved_not_strong=int(p3nostrong),fast_sequences=fast,**fc))
        target.update(valid_post_entry_seconds=float(duration[after].sum()),trade_events_after_fill=int(a['trade_events'][after].sum()))
        for side in ('bid','mid','ask'):target[side+'_target_seconds']=float(duration[after&good&(a[side]<=3)].sum())
        for model,i in hits.items():
            if i is not None:
                target[model+'_minimum_exit_leg_size']=int(min(a['short_ask_size'][i],a['long_bid_size'][i]))
                for h in (1,5,15,30,60):
                    j=np.searchsorted(t,t[i]+h*NS,side='right')-1
                    target[model+f'_plus_{h}s_mid']=float(mid[j]) if j>=0 and good[j] and a['until'][j]>=t[i]+h*NS else None
        targets.append(dict(**target,**{k+'_windows':classes[k] for k in ('strong','plausible','ambiguous','unlikely')}))
        hit=hits['event_strong_plausible'];before=good&after&((t<=t[hit]) if hit is not None else np.ones(len(t),bool));maximum=float(mid[before].max()) if before.any() else None
        category='censored/insufficient coverage'
        if complete and fill is not None:
            category=('severe adverse ($9+) then success' if maximum>=9 else 'adverse-then-success' if maximum>c.target_credit+.5 else 'clean compression') if hit is not None else 'failure/no target'
        taxonomy.append(dict(**base,category=category,max_before_observed_target=maximum,first_target_ns=int(t[hit]) if hit is not None else None,fresh_fraction=fresh/expected,source_complete=source_complete))
        sr=settlements[cid];intrinsic=float(sr['intrinsic']) if sr['intrinsic'] else None
        if intrinsic is not None:
            assert 0<=intrinsic<=10
            assert abs(settlement_payoff(float(sr['xqc']),c.short_strike,c.long_strike)-intrinsic)<1e-8
        settlement_rows.append(dict(**base,xqc=sr['xqc'],status=sr['status'],intrinsic=intrinsic,last_quote_mid=sr['last_quote_mid'],intrinsic_minus_last_quote=sr['intrinsic_minus_last_quote'],frozen_entry_hold_to_settlement_pnl=pnl(c.mid_credit-.05,intrinsic) if intrinsic is not None else None))
        dtseries=[(pd.Timestamp(x,unit='ns',tz='UTC').to_pydatetime(),m) for x,m in series]
        for model in RULES['models']:
            event=model.startswith('event_');eligible=(fill is not None and source_complete) if event else True
            if event:
                i=hits[model];debit=3 if i is not None else intrinsic;entry_px=c.target_credit-(.25 if model=='event_conservative' else .05);exit_ns=int(t[i]) if i is not None else None
            else:
                mode='persist_2' if model.startswith('persist_2') else 'persist_3' if model.startswith('persist_3') else 'touch'
                i=target_exit(dtseries,3,.05,mode);debit=mm[i]+.05 if i is not None else min(10.,float(sr['last_quote_mid'])+.05) if model=='minute_touch_original_quote_terminal' else intrinsic
                entry_px=c.mid_credit-.05;exit_ns=int(tt[i]) if i is not None else None
            outcomes.append(dict(**base,model=model,entry_supported=fill is not None,source_complete=source_complete,phase3b_complete=complete,eligible=eligible,modeled_entry_price=entry_px,modeled_exit_debit=debit,exit_ns=exit_ns,terminal_fallback=i is None,pnl=pnl(entry_px,debit) if eligible and debit is not None else None))
        print('analyzed',ordinal+1,'/',len(ids),'candidate',cid,'events',len(t),'fresh',round(fresh/expected,3),flush=True)
        del a,t,mid,duration,good;gc.collect()
    for name,rows in [('coverage_after_acquisition',coverage),('entry_execution_results',entries),('target_execution_results',targets),('false_negative_analysis',false),('path_taxonomy',taxonomy),('settlement_results',settlement_rows),('candidate_model_outcomes',outcomes)]:writecsv(OUT/(name+'.csv'),rows)
    pd.DataFrame(windows).to_parquet(OUT/'touch_window_evidence.parquet',index=False)
    comparison=[]
    for d in (2,3):
        for credit in (5.,6.,7.):
            for cohort in ('all_candidates','supported_entry_matched','strict_complete_matched'):
                for model in RULES['models']:
                    rr=[r for r in outcomes if r['calendar_dte']==d and r['target_credit']==credit and r['model']==model and (cohort=='all_candidates' or r['entry_supported']) and (cohort!='strict_complete_matched' or r['phase3b_complete'])]
                    comparison.append(dict(calendar_dte=d,target_credit=credit,cohort=cohort,model=model,**summarize(rr)))
    writecsv(OUT/'execution_model_comparison.csv',comparison)
    assert digest(OUT/'execution_rules.json')==freeze
    write_derived(OUT/'analysis_summary.json',dict(candidates=len(ids),sessions=len(panel['sessions']),source_complete_candidates=sum(r['source_complete'] for r in coverage),fresh_complete_candidates=sum(r['phase3b_complete'] for r in coverage),supported_entries=sum(r['modeled_entry_supported'] for r in entries),rules_sha256=freeze,source_complete_sessions=len({r['session_date'] for r in coverage if all(x['source_complete'] for x in coverage if x['session_date']==r['session_date'])}),complete_sessions=len({r['session_date'] for r in coverage if all(x['phase3b_complete'] for x in coverage if x['session_date']==r['session_date'])})))

if __name__=='__main__':analyze()
