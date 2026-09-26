"""Session-scoped, observed-minute path labels (never entry features)."""
from __future__ import annotations
import csv
import math
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime, timedelta, time
from itertools import groupby
from .ndxp_multidte_data import NEW_YORK
from .ndxp_multidte_validation import partition, quality_flags, DEGRADED

HORIZONS = (1,2,5,10,15,30,60,120)
TARGETS = (3.,2.,1.,.5)
SLIPPAGES = (0.,.05,.10,.15,.25)


def session_paths(path):
    """Bound memory to one acquisition session; reject noncontiguous sessions."""
    seen=set()
    with path.open(newline='') as f:
        for session, group in groupby(csv.DictReader(f),key=lambda r:r['session_date']):
            if session in seen: raise ValueError('Path CSV must be grouped by session')
            seen.add(session); symbols=defaultdict(list)
            for r in group:
                b,a=float(r['bid']),float(r['ask'])
                if math.isfinite(b) and math.isfinite(a) and 0<=b<=a and a>0:
                    t=datetime.fromisoformat(r['quote_time']).astimezone(NEW_YORK)
                    symbols[r['symbol']].append((t,b,a))
            for rows in symbols.values(): rows.sort()
            yield session,symbols


def paired_path(c, paths):
    """Latest quote per leg/minute, bounded to entry and expiration 16:00 ET.

    Timestamp is the later actual leg timestamp, never rounded before entry.
    No interpolation or forward fill; duplicates are diagnosed explicitly.
    """
    legs=[]; diagnostics={}
    for name,symbol in [('short',c.short_symbol),('long',c.long_symbol)]:
        rows=paths.get(symbol,[]); minutes={}
        for t,b,a in rows:
            key=t.replace(second=0,microsecond=0)
            if key not in minutes or t>=minutes[key][0]: minutes[key]=(t,b,a)
        diagnostics[name+'_duplicate_minutes']=len(rows)-len(minutes)
        legs.append(minutes)
    out=[]; invalid=0; before=0
    end=datetime.combine(c.expiration,time(16),NEW_YORK)
    for minute in sorted(legs[0].keys() & legs[1].keys()):
        s,l=legs[0][minute],legs[1][minute]; t=max(s[0],l[0])
        if min(s[0],l[0])<c.entry_time or t>end:
            before+=1; continue
        mid=(s[1]+s[2])/2-(l[1]+l[2])/2
        if 0<=mid<=10: out.append((t,mid))
        else: invalid+=1
    diagnostics.update(unpaired_minutes=len(legs[0].keys() ^ legs[1].keys()), invalid_spread_minutes=invalid, outside_trade_minutes=before)
    return out,diagnostics


def first_passage(series, level, adverse=False):
    return next((i for i,(_,m) in enumerate(series) if (m>=level if adverse else m<=level)),None)


def target_exit(series,target,slippage,mode='touch'):
    """Execution at confirmation minute; next-minute requires exactly +60s."""
    if mode not in ('touch','persist_2','persist_3','next_minute'): raise ValueError('Unknown persistence mode')
    run=0
    for i,(t,m) in enumerate(series):
        if m+slippage<=target:
            run=run+1 if i and (t-series[i-1][0]).total_seconds()==60 else 1
            if mode=='touch': return i
            if mode in ('persist_2','persist_3') and run>=int(mode[-1]): return i
            if mode=='next_minute':
                return i+1 if i+1<len(series) and (series[i+1][0]-t).total_seconds()==60 else None
        else: run=0
    return None


def prices(entry_mid,exit_mid,slippage):
    return max(.01,min(9.99,entry_mid-slippage)),max(0.,min(10.,exit_mid+slippage))


def economics(entry_mid,exit_mid,slippage):
    entry,debit=prices(entry_mid,exit_mid,slippage)
    fees=1.324*20*2; pnl=(entry-debit)*2000-fees
    risk=(10-entry)*2000+fees
    return pnl,risk


def settlement_payoff(underlying,short_strike,long_strike):
    if short_strike-long_strike != 10: raise ValueError('Expected 10-point put vertical')
    return max(0,short_strike-underlying)-max(0,long_strike-underlying)


def elapsed(c,t): return (t-c.entry_time).total_seconds()/60


def observed_underwater(series,entry):
    # Count only adjacent observed minute intervals. No overnight/gap carry.
    return sum(m>entry and (u-t).total_seconds()==60 for (t,m),(u,_) in zip(series,series[1:]))


def sign_changes(series,entry):
    count=0; previous=0; previous_t=None
    for t,m in series:
        if previous_t is not None and (t-previous_t).total_seconds()!=60: previous=0
        sign=(m>entry)-(m<entry)
        if sign and previous and sign!=previous: count+=1
        if sign: previous=sign
        previous_t=t
    return count


def path_features(c,series,diagnostics):
    row={'session_date':str(c.session_date),'expiration':str(c.expiration),'calendar_dte':c.calendar_dte,'target_credit':c.target_credit,
         'entry_time':c.entry_time.isoformat(),'entry_mid':c.mid_credit,'short_symbol':c.short_symbol,'long_symbol':c.long_symbol,
         'partition':partition(c.session_date),**quality_flags(c.session_date),**diagnostics,
         'path_rows':len(series),'path_status':'available' if series else 'missing',
         'path_crosses_degraded_date': any(str(t.date()) in DEGRADED for t,_ in series)}
    # Initialize all fields also for missing candidates, preserving one stable schema.
    row.update(first_quote_time=None,last_quote_time=None,terminal_mid=None,terminal_pnl=None,terminal_quote_age_minutes=None,
               terminal_basis='last_observed_quote_not_settlement',max_gap_minutes=None,underwater_observed_minutes=None,sign_changes=None)
    for h in HORIZONS:
        for field in ('mid','pnl','quote_time','quote_age_seconds'): row[f'plus_{h}m_{field}']=None
    for d in range(4):
        for field in ('mid','pnl','quote_time','quote_age_seconds'): row[f'day_{d}_cash_close_{field}']=None
    levels=[(f'favorable_{x:g}',x,False) for x in (5.5,5,4,3,2,1,.5)] + [(f'adverse_{n}',x,True) for n,x in [('entry_plus_0.5',c.mid_credit+.5),('entry_plus_1',c.mid_credit+1),('7',7),('8',8),('9',9),('9.5',9.5)]]
    for name,_,_ in levels: row[name+'_minutes']=None; row[name+'_status']='missing_path' if not series else 'never_hit'
    for target in TARGETS:
        for name in ('max_before_hit','underwater_before_hit','hit_7_first','hit_8_first','hit_9_first','deep_adverse_then_success','best_improvement_before_failure'):
            row[f'target_{target:g}_{name}']=None
    for target in TARGETS:
        for mode in ('touch','persist_2','persist_3','next_minute'):
            for field in ('minutes','time','mid','pnl'): row[f'target_{target:g}_{mode}_{field}']=None
    if not series: return row
    row.update(first_quote_time=series[0][0].isoformat(),last_quote_time=series[-1][0].isoformat(),terminal_mid=series[-1][1],terminal_pnl=economics(c.mid_credit,series[-1][1],.05)[0],
               terminal_quote_age_minutes=(datetime.combine(c.expiration,time(16),NEW_YORK)-series[-1][0]).total_seconds()/60,
               max_gap_minutes=max(((b[0]-a[0]).total_seconds()/60 for a,b in zip(series,series[1:])),default=0),
               underwater_observed_minutes=observed_underwater(series,c.mid_credit),sign_changes=sign_changes(series,c.mid_credit))
    times=[t for t,_ in series]
    def snapshot(prefix,t):
        idx=bisect_right(times,t)-1
        if idx<0: return
        actual,mid=series[idx]; age=(t-actual).total_seconds()
        row[prefix+'_quote_time']=actual.isoformat(); row[prefix+'_quote_age_seconds']=age
        # Only accept exact/minute-recent observations; disclose age even when rejected.
        if age<60:
            row[prefix+'_mid']=mid; row[prefix+'_pnl']=economics(c.mid_credit,mid,.05)[0]
    for h in HORIZONS: snapshot(f'plus_{h}m',c.entry_time+timedelta(minutes=h))
    for d in range(c.calendar_dte+1): snapshot(f'day_{d}_cash_close',datetime.combine(c.session_date+timedelta(days=d),time(16),NEW_YORK))
    for name,level,adverse in levels:
        i=first_passage(series,level,adverse)
        if i is not None: row[name+'_minutes']=elapsed(c,series[i][0]); row[name+'_status']='hit'
    for target in TARGETS:
        i=first_passage(series,target); prefix=f'target_{target:g}_'
        before=series[:i+1] if i is not None else series
        row[prefix+'max_before_hit']=max(m for _,m in before) if i is not None else None
        row[prefix+'underwater_before_hit']=observed_underwater(before,c.mid_credit) if i is not None else None
        for level in (7,8,9):
            j=first_passage(series,level,True)
            row[prefix+f'hit_{level}_first']= j is not None and (i is None or j<i)
        row[prefix+'deep_adverse_then_success']=i is not None and row[prefix+'hit_9_first']
        row[prefix+'best_improvement_before_failure']=c.mid_credit-min(m for _,m in series) if i is None else None
    for target in TARGETS:
        for mode in ('touch','persist_2','persist_3','next_minute'):
            idx=target_exit(series,target,.05,mode)
            if idx is not None:
                t,mid=series[idx]; prefix=f'target_{target:g}_{mode}_'
                row[prefix+'minutes']=elapsed(c,t); row[prefix+'time']=t.isoformat()
                row[prefix+'mid']=mid; row[prefix+'pnl']=economics(c.mid_credit,mid,.05)[0]
    return row
