import copy
import numpy as np
import pytest
from mgc_v05l.research.ndxp_owned_execution import (
 NS, RULES, development, select_pilot, mapping_ids, synchronize, classify,
 crossings, missed_crossings, settlement_join,
)
from mgc_v05l.research.ndxp_owned_pilot import time_coverage,persistence_at,snapshot


def quote(t,b,a,bs=20,az=20,action='A'):
 return (int(t*NS),b,a,bs,az,action,(b+a)/2,1,int(t*NS))

def spread(mid,t=1):
 return synchronize([quote(t,10+mid-.1,10+mid+.1)],[quote(t,9.9,10.1)],0,int((t+1)*NS))


def test_selection_deterministic_and_coverage_only():
 rows=[dict(session_date=f'2024-{m:02}-{d:02}',has_owned_pair=True,pnl=m*d) for m in range(1,13) for d in range(1,11)]
 a=select_pilot(rows);assert len(a)==60
 assert a==select_pilot(list(reversed(rows)))
 for r in rows:r['pnl']=-10000
 assert a==select_pilot(rows)
 assert len({d[:7] for d in a})==12


def test_shortfall_and_development_gate():
 rows=[dict(session_date=d,has_owned_pair=c) for d,c in [('2023-03-27',True),('2023-03-28',True),('2024-12-31',True),('2025-01-01',True),('2024-05-01',False)]]
 assert select_pilot(rows)==['2023-03-28','2024-12-31']
 assert not development('2026-01-01')


def test_header_coverage_exact_symbols_and_end_exclusive():
 import pandas as pd
 a=pd.Timestamp('2024-01-02',tz='UTC').value;b=a+60*NS
 h=dict(start=a,end=b,mappings={'EXACT':[dict(start_date='2024-01-02',end_date='2024-01-03',symbol='12')]})
 assert mapping_ids(h,'EXACT',a,b)==[12]
 assert mapping_ids(h,'OTHER',a,b)==[]
 assert mapping_ids(h,'EXACT',b,b+NS)==[]
 h['quarantine']=True;assert mapping_ids(h,'EXACT',a,b)==[]


def test_synchronized_spread_sizes_and_trade_context():
 r=synchronize([quote(1,15,16,7,8,'T')],[quote(1,10,11,9,10)],0,2*NS)[0]
 assert (r['bid'],r['ask'],r['mid'],r['width'])==(4,6,5,2)
 assert r['short_bid_size']==7 and r['long_ask_size']==10
 assert r['trade_events']==1 and r['trade_context'][0]['leg']==0
 assert not r['stale'] and not r['invalid']


def test_no_long_gap_forward_fill():
 rr=synchronize([quote(1,15,16),quote(100,15,16)],[quote(1,10,11)],0,101*NS)
 assert rr[-1]['stale'];assert rr[-1]['duration_s']==0
 assert rr[0]['duration_s']==1
 assert snapshot(rr,3*NS) is None


def test_unmatched_and_crossed_invalid():
 rr=synchronize([quote(1,15,14)],[quote(2,10,11)],0,3*NS)
 assert rr[0]['unmatched'];assert rr[-1]['invalid']


def test_same_timestamp_last_update():
 rr=synchronize([quote(1,15,16),quote(1,16,17)],[quote(1,10,11)],0,2*NS)
 assert len(rr)==1 and rr[0]['mid']==6


def test_resting_order_not_activated_by_target():
 rr=spread(2.5)
 assert classify(rr,None,0,2*NS)=='ambiguous'
 assert classify(rr,NS,0,2*NS)=='ambiguous' # same-time target excluded
 assert classify(rr,NS-1,0,2*NS)=='strong fill evidence'

@pytest.mark.parametrize('mid,expected',[(2.5,'strong fill evidence'),(2.95,'plausible fill evidence'),(3.1,'ambiguous')])
def test_deterministic_classification(mid,expected):
 rr=spread(mid)
 assert classify(rr,0,0,2*NS)==expected
 assert classify(copy.deepcopy(rr),0,0,2*NS)==expected


def test_unlikely_requires_complete_observation():
 rr=spread(5)
 assert classify(rr,0,NS,2*NS)=='unlikely fill'
 assert classify(rr,0,NS,3*NS)=='ambiguous'


def test_zero_size_not_fill_evidence():
 rr=spread(2.5);rr[0]['short_ask_size']=0
 assert classify(rr,0,0,2*NS)=='ambiguous'


def test_false_negative_detects_resting_crossing_only():
 rr=spread(4,60.5)+spread(2.5,61)
 assert len(missed_crossings(rr,[],0))==1
 assert missed_crossings(rr,[60*NS],0)==[]
 assert missed_crossings(rr,[],None)==[]
 assert missed_crossings(rr,[],62*NS)==[]


def test_persistence_failure_does_not_mean_nonfill():
 ss=[(60*NS,2.9),(120*NS,4)]
 assert not persistence_at(ss,0,2)
 assert classify(spread(2.5,61),0,60*NS,62*NS)=='strong fill evidence'


def test_persistence_gap_rejected():
 assert persistence_at([(0,2.9),(60*NS,2.9)],0,2)
 assert not persistence_at([(0,2.9),(120*NS,2.9)],0,2)


def test_timestamp_coverage_no_overcount_or_gap_fill():
 a=np.array([0,NS//2,100*NS],dtype=np.int64)
 b=np.array([0,NS//2],dtype=np.int64)
 assert time_coverage(a,b,0,101*NS)==1.5
 assert time_coverage(a,b,10*NS,11*NS)==0
 assert time_coverage(a,b,NS,2*NS)==.5


def test_settlement_join_duplicates_conflicts_and_missing():
 rr=[dict(date='2024-01-02',settlement='15000',source='FRED NASDAQXQC / Nasdaq XQC')]
 assert settlement_join('2024-01-02',rr*2)[0]==15000
 assert settlement_join('2024-01-03',rr)==(None,'missing')
 assert settlement_join('2024-01-02',rr+[dict(rr[0],settlement='15001')])[0] is None
 assert settlement_join('2024-01-02',[dict(rr[0],source='unknown')])[0] is None


def test_rules_separate_evidence_from_queue_claims():
 assert RULES['max_leg_age_seconds']==1
 assert 'No guaranteed 20-lot fill' in RULES['limitations']


def test_expiry_crossing_holdout_excluded():
 rows=[dict(session_date='2024-12-30',expiration='2025-01-02',has_owned_pair=True)]
 assert select_pilot(rows)==[]


def test_undefined_dbn_prices_rejected_even_if_cancel_in_spread():
 rr=synchronize([quote(1,9223372036,9223372036)],[quote(1,9223372036,9223372036)],0,2*NS)
 assert rr[0]['invalid']


def test_minimum_request_subtracts_owned_and_merges_overlaps():
 from mgc_v05l.research.ndxp_owned_pilot import subtract_intervals
 assert subtract_intervals(0,100,[(0,10),(5,20),(30,40),(90,110)])==[(20,30),(40,90)]
 assert subtract_intervals(0,100,[(-1,101)])==[]
 assert subtract_intervals(0,100,[])==[(0,100)]


def test_fast_sequence_single_update_sweeps_and_stale_breaks():
 from mgc_v05l.research.ndxp_owned_execution import fast_sequences
 rr=[spread(m,t)[0] for t,m in [(1,6),(1.5,2.5),(2,4)]]
 assert len(fast_sequences(rr))==1
 rr[1]['stale']=True;assert fast_sequences(rr)==[]


def test_event_extraction_counts_and_clips(tmp_path,monkeypatch):
 from mgc_v05l.research import ndxp_owned_index as mod
 import pandas as pd
 dtype=[('instrument_id','u4'),('ts_recv','u8'),('ts_event','u8'),('bid_px_00','i8'),('ask_px_00','i8'),('bid_sz_00','u4'),('ask_sz_00','u4'),('action','S1'),('price','i8'),('size','u4')]
 a=np.zeros(4,dtype=dtype);a['instrument_id']=[1,2,1,1];a['ts_recv']=[10,11,20,30];a['action']=b'A'
 class Fake:
  def to_ndarray(self,count):return iter([a])
 monkeypatch.setattr(mod.db.DBNStore,'from_file',lambda p:Fake())
 job=({'path':'fake.dbn','bytes':1,'mtime_ns':1,'start':0,'end':100},{1:'EXACT'},{'EXACT':(10,30)},str(tmp_path))
 r=mod.extract(job);assert r[0]['events']==2
 assert (r[0]['first_ns'],r[0]['last_ns'])==(10,20)
 assert set(pd.read_parquet(r[0]['parquet']).symbol)=={'EXACT'}
 assert mod.extract(job)==r


def test_left_censored_quote_not_a_confirmed_crossing():
 rr=spread(2.5,61)
 assert missed_crossings(rr,[],0)==[]
 rr=spread(4,1)+spread(2.5,61)
 assert missed_crossings(rr,[],0)==[]


def test_censored_taxonomy_preserves_observed_fragments(tmp_path,monkeypatch):
 from mgc_v05l.research import ndxp_owned_pilot as mod
 import pandas as pd
 monkeypatch.setattr(mod,'OUT',tmp_path)
 entry=pd.Timestamp('2024-01-02T09:31:00-05:00').value
 c=dict(candidate_id='1',session_date='2024-01-02',expiration='2024-01-04',entry_time='2024-01-02T09:31:00-05:00',target_credit='6',entry_mid='6',calendar_dte='2',short_symbol='S',long_symbol='L')
 t=entry//NS+120
 events={'S':[quote(t,12.4,12.6)],'L':[quote(t,9.9,10.1)]}
 entries,targets,negative,taxonomy=mod.analyze([c],events,{1:[(t*NS,2.5)]})
 assert entries[0]['modeled_fill_ns'] is None
 assert taxonomy[0]['category']=='censored/insufficient coverage'
 assert taxonomy[0]['observed_fresh_seconds']==1
 assert taxonomy[0]['first_target_ns']==t*NS
 assert targets[0]['conditional_strong fill evidence']==1
 assert targets[0]['ambiguous']==1
