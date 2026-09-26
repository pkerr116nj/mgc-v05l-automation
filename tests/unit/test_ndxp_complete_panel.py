import math
import pandas as pd
import exchange_calendars as xc
import pytest
from mgc_v05l.research.ndxp_complete_panel import merge,final_requests,gate


def request(start='2024-07-03T09:30:00-04:00',end='2024-07-03T16:00:00-04:00'):
 return dict(date='2024-07-03',dataset='OPRA.PILLAR',schema='cmbp-1',stype_in='raw_symbol',symbols=['NDXP  240705P19000000'],start=start,end=end)


def test_merge_overlap_and_adjacent():
 assert merge([(5,10),(0,7),(10,12),(20,30)])==[(0,12),(20,30)]


def test_calendar_half_day_cache_subtraction_deterministic():
 r=request();h=dict(start_ns=pd.Timestamp(r['start']).value,end_ns=pd.Timestamp('2024-07-03T10:00:00-04:00').value,candidate_symbols=r['symbols'])
 a,c=final_requests([r,r],[h],xc.get_calendar('XNYS'))
 assert len(a)==1 and len(c)==2
 assert pd.Timestamp(a[0]['start']).hour==14 and pd.Timestamp(a[0]['end']).hour==17
 assert (a,c)==final_requests([r,r],[h],xc.get_calendar('XNYS'))

@pytest.mark.parametrize('amount,allowed',[(0,True),(9.99,True),(10,True),(10.000001,False)])
def test_hard_ceiling(amount,allowed):
 r={'a':dict(status='ok',value=amount,request={'method':'get_cost'}),'b':dict(status='ok',value=100,request={'method':'get_billable_size'})}
 assert gate(r,['a','b'])[0] is allowed

@pytest.mark.parametrize('value',[-1,float('inf'),float('nan'),'1.00',None])
def test_bad_quotes_never_authorize(value):
 assert not gate({'a':dict(status='ok',value=value,request={'method':'get_cost'})},['a'])[0]


def test_incomplete_or_failed_quotes_never_authorize():
 assert not gate({},['a'])[0]
 assert not gate({'a':{'status':'unavailable'}},['a'])[0]


def test_full_chain_rejected_before_pricing():
 r=request();r['symbols']=['NDXP.OPT']
 with pytest.raises(ValueError):final_requests([r],[],xc.get_calendar('XNYS'))


def test_batch_resume_does_not_repurchase(tmp_path):
 from mgc_v05l.research.ndxp_panel_acquire import submit_once
 for s in ('intents','jobs'):(tmp_path/s).mkdir()
 class Batch:
  calls=0
  def submit_job(self,**kwargs):
   self.calls+=1;return dict(id='one',state='queued',api_key='DO_NOT_PERSIST',user_id='private')
 b=Batch();r=request();r.pop('date')
 a=submit_once(r,b,tmp_path,[]);c=submit_once(r,b,tmp_path,[])
 assert a==c and b.calls==1 and 'api_key' not in a['job']


def test_unknown_paid_submission_never_blindly_retried(tmp_path):
 from mgc_v05l.research.ndxp_panel_acquire import submit_once
 for s in ('intents','jobs'):(tmp_path/s).mkdir()
 class Batch:
  calls=0
  def submit_job(self,**kwargs):self.calls+=1;raise TimeoutError()
 b=Batch();r=request();r.pop('date')
 with pytest.raises(TimeoutError):submit_once(r,b,tmp_path,[])
 with pytest.raises(RuntimeError):submit_once(r,b,tmp_path,[])
 assert b.calls==1


def test_existing_provider_job_reused(tmp_path):
 from mgc_v05l.research.ndxp_panel_acquire import submit_once
 for s in ('intents','jobs'):(tmp_path/s).mkdir()
 r=request();r.pop('date');j=dict(r,id='existing',state='done')
 a=submit_once(r,None,tmp_path,[j]);assert a['reused_prior_job']


def test_valid_local_file_has_no_download(tmp_path):
 from mgc_v05l.research.ndxp_panel_acquire import download_file
 import hashlib
 p=tmp_path/'data.dbn.zst';p.write_bytes(b'abc')
 detail=dict(filename=p.name,size=3,hash=hashlib.sha256(b'abc').hexdigest())
 def no_call(*a,**k):raise AssertionError('Must not download')
 assert download_file(detail,tmp_path,'unused',no_call)['reused']


def test_partial_transfer_is_preserved_and_not_a_repurchase(tmp_path):
 from mgc_v05l.research.ndxp_panel_acquire import download_file
 import hashlib,requests
 (tmp_path/'data.attempt0.part').write_bytes(b'partial')
 class Response:
  def __enter__(self):return self
  def __exit__(self,*a):pass
  def raise_for_status(self):pass
  def iter_content(self,*a):yield b'abc'
 d=dict(filename='data',size=3,hash=hashlib.sha256(b'abc').hexdigest(),urls={'https':'https://hist.databento.com/file'})
 result=download_file(d,tmp_path,'unused',lambda *a,**k:Response())
 assert (tmp_path/'data.attempt0.part').read_bytes()==b'partial'
 assert not result['reused'] and (tmp_path/'data').read_bytes()==b'abc'


def test_array_sync_matches_phase3b_freshness():
 import numpy as np
 from mgc_v05l.research.ndxp_panel_analysis import sync_arrays
 from mgc_v05l.research.ndxp_owned_execution import synchronize,NS
 cols=['ts_recv','bid_px_00','ask_px_00','bid_sz_00','ask_sz_00','ts_event','action']
 short=pd.DataFrame([[NS,15*NS,16*NS,20,20,NS,b'A'],[3*NS,12*NS,13*NS,20,20,3*NS,b'A']],columns=cols)
 long=pd.DataFrame([[NS,10*NS,11*NS,20,20,NS,b'A']],columns=cols)
 a=sync_arrays(short,long,0,4*NS)
 assert list(a['mid'])==[5,2]
 assert list(a['good'])==[True,False]
 assert list(a['duration'])==[1,0]


def test_fast_count_equivalent_to_frozen_phase3b():
 import numpy as np
 from mgc_v05l.research.ndxp_panel_analysis import fast_count
 from mgc_v05l.research.ndxp_owned_execution import fast_sequences,NS
 rng=np.random.default_rng(17)
 for case in range(30):
  t=np.cumsum(rng.integers(1,10,500,dtype=np.int64)*NS//10)
  mid=rng.choice([2.,3.,3.1,4.,5.,5.9,6.,7.],500);good=rng.random(500)>.02;until=t+NS
  rows=[dict(t=int(x),mid=float(m),stale=not g,invalid=False,fresh_until=int(u)) for x,m,g,u in zip(t,mid,good,until)]
  assert fast_count(t,mid,good,until)==len(fast_sequences(rows))
 t=np.arange(1000,dtype=np.int64)*NS//10;mid=np.r_[np.full(990,6.),5.,4.,3.,4.,np.full(6,6.)];good=np.ones(1000,bool);until=t+NS
 rows=[dict(t=int(x),mid=float(m),stale=False,invalid=False,fresh_until=int(u)) for x,m,u in zip(t,mid,until)]
 assert fast_count(t,mid,good,until)==len(fast_sequences(rows))


def test_execution_economics_and_summary():
 from mgc_v05l.research.ndxp_panel_analysis import pnl,summarize
 assert pnl(5.95,3)==pytest.approx(5847.04)
 r=summarize([{'pnl':100},{'pnl':-50},{'pnl':None}])
 assert r['profit_factor']==2 and r['win_rate']==.5 and r['excluded']==1


def test_nanosecond_synchronization_does_not_cast_timestamps_to_float():
 import numpy as np
 from mgc_v05l.research.ndxp_panel_analysis import sync_arrays
 from mgc_v05l.research.ndxp_owned_execution import NS
 base=1700000000000000000
 def leg(t,b,a):return pd.DataFrame(dict(ts_recv=np.array([t],dtype='uint64'),ts_event=np.array([t],dtype='uint64'),bid_px_00=[b*NS],ask_px_00=[a*NS],bid_sz_00=[1],ask_sz_00=[1],action=[b'A']))
 x=sync_arrays(leg(base+1,15,16),leg(base+5,10,11),base,base+NS+10)
 assert x['until'].dtype==np.dtype('int64')
 assert x['t'][-1]==base+5
 assert x['until'][-1]==base+1+NS
 assert x['duration'][-1]==pytest.approx((NS-4)/NS,abs=1e-12)


def test_purchase_gate_rejects_manifest_mismatch_and_stale_quote():
 from mgc_v05l.research.ndxp_panel_acquire import authorize
 from mgc_v05l.research.ndxp_complete_panel import cache_key
 from datetime import datetime,timezone,timedelta
 r=request();r.pop('date');manifest={'requests':[r]}
 results={cache_key(dict(method=m,params=r)):dict(status='ok',value=1,request=dict(method=m,params=r),quoted_at_utc=datetime.now(timezone.utc).isoformat()) for m in ('get_cost','get_billable_size')}
 q=dict(manifest_sha256='expected',results=results)
 assert authorize(manifest,q,'expected')==1
 with pytest.raises(ValueError):authorize(manifest,q,'wrong')
 for v in results.values():v['quoted_at_utc']=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
 with pytest.raises(ValueError):authorize(manifest,q,'expected')


def test_tiny_panel_replay_preserves_censoring_and_settlement(tmp_path,monkeypatch):
 from mgc_v05l.research import ndxp_panel_analysis as mod
 from types import SimpleNamespace
 from datetime import date
 import json,csv
 from mgc_v05l.research.ndxp_owned_execution import NS
 out=tmp_path/'out';old=tmp_path/'old';cache=tmp_path/'cache'
 for p in (out,old,cache):p.mkdir()
 monkeypatch.setattr(mod,'OUT',out);monkeypatch.setattr(mod,'OLD',old)
 entry=pd.Timestamp('2024-01-02T09:31:00-05:00');end=pd.Timestamp('2024-01-04T16:00:00-05:00').value
 c=SimpleNamespace(session_date=date(2024,1,2),expiration=date(2024,1,4),entry_time=entry,calendar_dte=2,target_credit=6.,mid_credit=6.,short_symbol='S',long_symbol='L',short_strike=16990.,long_strike=16980.)
 monkeypatch.setattr(mod,'load_candidates',lambda p:[c])
 (old/'pilot_manifest.json').write_text(json.dumps(dict(candidate_ids=[0],sessions=['2024-01-02'])))
 (out/'new_raw_index.json').write_text('[]')
 rows=[]
 for symbol,mids in [('S',[16.5,12.5]),('L',[10,10])]:
  for delta,mid in zip([1,2],mids):rows.append(dict(symbol=symbol,ts_recv=entry.value+delta*NS,ts_event=entry.value+delta*NS,bid_px_00=int((mid-.1)*NS),ask_px_00=int((mid+.1)*NS),bid_sz_00=20,ask_sz_00=20,action=b'A',price=0,size=0))
 raw=cache/'raw.parquet';pd.DataFrame(rows).to_parquet(raw,index=False)
 (cache/'leg_store_stats.json').write_text(json.dumps([dict(symbol=s,parquet=str(raw),header_start_ns=entry.value-60*NS,header_end_ns=end) for s in ('S','L')]))
 (cache/'pilot_minutes.json').write_text(json.dumps({'0':[(entry.value,6.),(entry.value+60*NS,2.5)]}))
 (old/'store_inventory.json').write_text(json.dumps([dict(candidate_symbols=['S','L'],start_ns=entry.value-60*NS,end_ns=end)]))
 with (old/'settlement_audit.csv').open('w') as f:
  w=csv.DictWriter(f,fieldnames=['candidate_id','xqc','status','intrinsic','last_quote_mid','intrinsic_minus_last_quote']);w.writeheader();w.writerow(dict(candidate_id=0,xqc=17000,status='source_tag_and_date_validated_offline',intrinsic=0,last_quote_mid=1,intrinsic_minus_last_quote=-1))
 mod.analyze(cache)
 result=json.loads((out/'analysis_summary.json').read_text())
 assert result['source_complete_candidates']==1 and result['fresh_complete_candidates']==0
 assert result['supported_entries']==1
 df=pd.read_csv(out/'candidate_model_outcomes.csv')
 assert len(df)==7
 strong=df[df.model=='event_strong'].iloc[0]
 assert strong.pnl==pytest.approx(5847.04) and not strong.terminal_fallback
 assert pd.read_csv(out/'path_taxonomy.csv').iloc[0]['category']=='censored/insufficient coverage'
 before={p.name:p.read_bytes() for p in out.iterdir() if p.suffix in ('.csv','.parquet') or p.name=='analysis_summary.json'}
 mod.analyze(cache)
 assert before=={p.name:p.read_bytes() for p in out.iterdir() if p.name in before}


def test_provider_prefixed_checksum_and_complete_partial_resume(tmp_path):
 from mgc_v05l.research.ndxp_panel_acquire import download_file
 import hashlib
 (tmp_path/'data.attempt0.part').write_bytes(b'abc')
 d=dict(filename='data',size=3,hash='sha256:'+hashlib.sha256(b'abc').hexdigest(),urls={'https':'https://hist.databento.com/file'})
 def forbidden(*a,**k):raise AssertionError('Must reuse complete partial')
 r=download_file(d,tmp_path,'unused',forbidden)
 assert r['reused'] and (tmp_path/'data').read_bytes()==b'abc'


def test_pending_jobs_have_unknown_cost_and_resume_without_purchase(tmp_path,monkeypatch):
 from mgc_v05l.research import ndxp_panel_acquire as mod
 from mgc_v05l.research.ndxp_complete_panel import cache_key,digest
 from datetime import datetime,timezone,timedelta
 from types import SimpleNamespace
 import json
 r=request();r.pop('date');manifest={'requests':[r]}
 (tmp_path/'final_request_manifest.json').write_text(json.dumps(manifest))
 checksum=digest(tmp_path/'final_request_manifest.json')
 (tmp_path/'manifest_digest.json').write_text(json.dumps({'sha256':checksum}))
 # Old pricing cannot authorize a new purchase, but free retrieval is permitted.
 results={cache_key(dict(method=m,params=r)):dict(status='ok',value=1,request=dict(method=m,params=r),quoted_at_utc=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()) for m in ('get_cost','get_billable_size')}
 (tmp_path/'preflight_quote.json').write_text(json.dumps(dict(manifest_sha256=checksum,results=results)))
 (tmp_path/'jobs').mkdir();job=dict(r,id='already-paid-intent',state='queued',cost_usd=None)
 (tmp_path/'jobs'/f'{mod.request_key(r)}.json').write_text(json.dumps(dict(request=r,job=job,reused_prior_job=False)))
 class Batch:
  def list_jobs(self):return [job]
  def submit_job(self,**kwargs):raise AssertionError('No duplicate purchase')
 monkeypatch.setattr(mod,'OUT',tmp_path);monkeypatch.setattr(mod,'finalize',lambda:manifest)
 monkeypatch.setattr(mod,'load_key',lambda p:'unused')
 monkeypatch.setattr(mod.db,'Historical',lambda key:SimpleNamespace(batch=Batch()))
 for attempt in range(2):
  with pytest.raises(RuntimeError,match='incomplete'):mod.acquire(max_polls=1)
  evidence=json.loads((tmp_path/'acquisition_receipts.json').read_text())
  assert evidence['actual_provider_cost_usd'] is None
  assert not evidence['actual_cost_complete'] and evidence['completed_requests']==0
  assert evidence['known_completed_cost_usd']==0 and len(evidence['remaining_jobs'])==1
 assert len(list(tmp_path.glob('acquisition_receipts_previous_*.json')))==1
