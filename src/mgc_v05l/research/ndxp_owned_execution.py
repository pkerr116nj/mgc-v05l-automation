"""Local-only Phase 3B primitives. No acquisition or order submission paths."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime
import math
import numpy as np
import pandas as pd

NS=1_000_000_000
RULES={
 'version':1,
 'expiry_cutoff':'2024-12-31; candidates expiring later are excluded from pilot execution analysis',
 'conditional_exit_diagnostic':'separate hypothetical frozen baseline STO fill at entry_time, credit=entry_mid-0.05; activate BTC immediately; does not validate that entry or imply an actual fill','development_end':'2024-12-31','max_leg_age_seconds':1,
 'entry_window_seconds':[0,60], 'context_window_seconds':[-60,300],
 'target':3.0,'baseline_touch_mid':2.95,'quote_window_seconds':[-30,60],
 'modeled_entry':'first valid synchronized midpoint >= cohort credit in [entry,entry+60s); hypothetical complex midpoint fill, not proof',
 'resting_activation':'immediately after modeled fill; strictly later event timestamps only',
 'strong':'valid nonstale spread ask <= 3 with positive executable-side leg sizes; diagnostic sufficient legged-price evidence, not complex queue proof',
 'plausible':'valid nonstale spread midpoint <= 3 and positive quoted sizes on all sides; hypothetical resting midpoint evidence',
 'ambiguous':'missing modeled entry, incomplete window, or only bid crosses; missing coverage never means unlikely',
 'unlikely':'modeled entry exists, >=95% fresh valid window duration, and no bid/mid/ask target crossing',
 'duration':'piecewise constant, capped at earliest leg timestamp+1s and next event; no extension beyond query boundary',
 'invalid':'nonfinite/negative leg prices, bid>ask, zero/undefined prices, spread midpoint outside [0,10]; locked flagged separately',
 'false_negative':'event ask/mid crossing during post-fill observed interval with no minute mid<=2.95 in the same floored UTC minute; descriptive sampling mismatch',
 'fast_sequence':'mid >=6 then <=5 then <=4 then <=3 then >3 within 60s, no stale/invalid break',
 'taxonomy':'censor unless entry and >=95% regular-session path coverage; success midpoint<=3, severe prehit>=9, adverse prehit>credit+0.5, otherwise clean',
 'limitations':['No complex-order queue position','No guaranteed 20-lot fill','Single-leg trade context is not a spread execution','Thresholds descriptive, frozen before aggregate analysis']}


def development(date):
    return '2023-03-28' <= str(date)[:10] <= '2024-12-31'


def select_pilot(rows,n=60):
    """Month round-robin quantiles, date/coverage only; retain all frozen cohorts."""
    months=defaultdict(list)
    for r in rows:
        if development(r['session_date']) and r.get('expiration',r['session_date'])<='2024-12-31' and r['has_owned_pair']:
            months[r['session_date'][:7]].append(r['session_date'])
    months={m:sorted(set(ds)) for m,ds in sorted(months.items())}
    chosen=[];rank=0
    while len(chosen)<n:
        added=False
        for dates in months.values():
            # Alternating median, first quartile, third quartile via farthest date index.
            remaining=[d for d in dates if d not in chosen]
            if not remaining:continue
            used=[dates.index(d) for d in dates if d in chosen]
            d=min(remaining,key=lambda d:(-min((abs(dates.index(d)-i) for i in used),default=0),abs(dates.index(d)-(len(dates)-1)/2),d))
            chosen.append(d);added=True
            if len(chosen)==n:break
        if not added:break
        rank+=1
    return sorted(chosen)


def mapping_ids(header,symbol,start,end):
    """Header date-scoped mappings; timestamps are nanoseconds, end exclusive."""
    if header.get('quarantine') or header.get('error') or header['start']>=end or header['end']<=start:return []
    return [int(v['symbol']) for v in header['mappings'].get(symbol,[]) if v['symbol'].isdigit() and v['start_date']<pd.Timestamp(end,unit='ns',tz='UTC').strftime('%Y-%m-%dT%H:%M:%S')[:10]+'~' and v['end_date']>pd.Timestamp(start,unit='ns',tz='UTC').strftime('%Y-%m-%d')]


def synchronize(short,long,start,end,max_age=NS):
    """Use receive-time order, collapse ties to final leg state, reject stale state.

    Raw event tuples: ts_recv, bid, ask, bid_size, ask_size, action, trade_price,
    trade_size, ts_event. Same-time ties use final source-sorted record per leg.
    """
    events={}
    for leg,rows in enumerate((short,long)):
        for r in rows:
            if start-max_age<=int(r[0])<end:events.setdefault(int(r[0]),{})[leg]=r
    state=[None,None];out=[]
    for t,updates in sorted(events.items()):
        for leg,r in updates.items():state[leg]=r
        if t<start:continue
        missing=any(r is None for r in state)
        stale=missing or any(t-int(r[0])>max_age for r in state if r is not None)
        invalid=missing or any(not all(math.isfinite(float(x)) for x in r[1:3]) or not 0<=r[1]<=r[2]<1e9 or r[2]<=0 for r in state if r is not None)
        s,l=state
        bid=ask=mid=width=float('nan')
        if not missing:
            bid=s[1]-l[2];ask=s[2]-l[1];mid=(bid+ask)/2; width=ask-bid
            invalid=invalid or not 0<=mid<=10
        out.append(dict(t=t,bid=bid,ask=ask,mid=mid,width=width,stale=stale,unmatched=missing,invalid=invalid,crossed=not missing and (s[1]>s[2] or l[1]>l[2]),locked=not missing and (s[1]==s[2] or l[1]==l[2]),
            short_bid=None if missing else s[1],short_ask=None if missing else s[2],long_bid=None if missing else l[1],long_ask=None if missing else l[2],
            short_bid_size=0 if missing else s[3],short_ask_size=0 if missing else s[4],long_bid_size=0 if missing else l[3],long_ask_size=0 if missing else l[4],
            trade_events=sum(r[5]=='T' for r in updates.values()),trade_context=[{'leg':leg,'price':r[6],'size':r[7],'ts_event':r[8]} for leg,r in updates.items() if r[5]=='T'],
            fresh_until=t if stale or invalid else min(end,int(s[0])+max_age,int(l[0])+max_age)))
    for i,r in enumerate(out):r['duration_s']=max(0,min(r['fresh_until'],out[i+1]['t'] if i+1<len(out) else end)-r['t'])/NS
    return out


def valid(r):return not (r['stale'] or r['invalid'])


def classify(rows,fill_time,start,end):
    rr=[r for r in rows if start<=r['t']<end and fill_time is not None and r['t']>fill_time and valid(r)]
    if fill_time is None:return 'ambiguous'
    if any(r['ask']<=3 and r['short_ask_size']>0 and r['long_bid_size']>0 for r in rr):return 'strong fill evidence'
    if any(r['mid']<=3 and min(r[k] for k in ('short_bid_size','short_ask_size','long_bid_size','long_ask_size'))>0 for r in rr):return 'plausible fill evidence'
    covered=sum(min(r['duration_s'],(end-r['t'])/NS) for r in rr)
    if covered>=.95*(end-start)/NS and not any(r['bid']<=3 for r in rr):return 'unlikely fill'
    return 'ambiguous'


def crossings(rows,field,level,direction='below',require_transition=True):
    hits=[];prior=None
    for r in rows:
        if not valid(r):prior=None;continue
        yes=r[field]<=level if direction=='below' else r[field]>=level
        continuous=prior is not None and prior['fresh_until']>=r['t']
        was_through=continuous and (prior[field]<=level if direction=='below' else prior[field]>=level)
        if yes and not was_through and (continuous or not require_transition):hits.append(r)
        prior=r
    return hits


def missed_crossings(rows,minute_touch_ns,fill_time):
    if fill_time is None:return []
    minutes={t//(60*NS) for t in minute_touch_ns}
    return [r for r in crossings([r for r in rows if r['t']>fill_time],'mid',3) if r['t']//(60*NS) not in minutes]


def settlement_join(expiration,records):
    rr=[r for r in records if r['date']==str(expiration)]
    if not rr:return None,'missing'
    try:
        values={float(r['settlement']) for r in rr}
    except (ValueError,TypeError):return None,'conflict_or_unrecognized_source'
    if len(values)!=1 or any(not math.isfinite(v) or v<=0 for v in values) or any(r['source']!='FRED NASDAQXQC / Nasdaq XQC' for r in rr):return None,'conflict_or_unrecognized_source'
    return values.pop(),'source_tag_and_date_validated_offline'


def fast_sequences(rows):
    """Descriptive 60s compression/rebound, allowing a single update to sweep levels."""
    sequence=[];previous=None;found=[]
    for r in rows:
        if not valid(r) or (previous is not None and previous['fresh_until']<r['t']):sequence=[]
        previous=r
        if not valid(r):continue
        if sequence and r['t']-sequence[0]>60*NS:sequence=[]
        if not sequence and r['mid']>=6:sequence=[r['t']]
        elif sequence:
            while len(sequence)<4 and r['mid']<=(5,4,3)[len(sequence)-1]:sequence.append(r['t'])
            if len(sequence)==4 and r['mid']>3:
                found.append(dict(start_ns=sequence[0],level_5_ns=sequence[1],level_4_ns=sequence[2],level_3_ns=sequence[3],rebound_ns=r['t']));sequence=[]
    return found
