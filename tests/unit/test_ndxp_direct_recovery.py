import json
from types import SimpleNamespace
import pandas as pd
import pytest
import exchange_calendars as xc
from mgc_v05l.research import ndxp_direct_recovery as mod


def params():
 return dict(dataset='OPRA.PILLAR',schema='cmbp-1',stype_in='raw_symbol',symbols=['NDXP  240705P19000000','NDXP  240705P18990000'],start='2024-07-03T13:31:00+00:00',end='2024-07-05T20:00:00+00:00')


def test_consolidation_covers_fragments_and_preserves_half_day():
 p=params();cs=[dict(session_date='2024-07-03',entry_time=p['start'],expiration='2024-07-05',short_symbol=p['symbols'][0],long_symbol=p['symbols'][1])]
 fragments=[dict(p,end='2024-07-03T17:00:00+00:00'),dict(p,start='2024-07-05T13:30:00+00:00')]
 result=mod.consolidate(fragments,dict(candidates=cs),xc.get_calendar('XNYS'))
 assert len(result)==1 and result[0]['params']==dict(p,symbols=sorted(p['symbols']))
 assert len(result[0]['regular_windows_ns'])==2
 assert pd.Timestamp(result[0]['regular_windows_ns'][0][1],tz='UTC').hour==17
 assert result==mod.consolidate(fragments,dict(candidates=cs),xc.get_calendar('XNYS'))


def plan(cost=1,count=60):
 return dict(projected_cost=cost,projected_bytes=100,metadata_call_count=12,provider_request_count=count,consolidation_alternative={'provider_request_count':60},expected_operational_complexity='bounded')

@pytest.mark.parametrize('value',[10.0001,None,float('nan'),float('inf'),-1])
def test_new_spend_ceiling(value):
 with pytest.raises(ValueError):mod.planning_guardrail(plan(value))
 assert mod.planning_guardrail(plan(10))


def test_fragmented_low_cost_plan_rejected():
 with pytest.raises(ValueError,match='fragmented'):mod.planning_guardrail(plan(count=501))
 assert mod.planning_guardrail(plan(count=501),justification='Provider requires separate windows')
 p=plan();p['metadata_call_count']=13
 with pytest.raises(ValueError,match='budget'):mod.planning_guardrail(p)


def test_direct_resume_never_retrieves_twice(tmp_path,monkeypatch):
 for name in ('raw','intents','receipts'):(tmp_path/name).mkdir()
 p=params();r=dict(pilot_session='2024-07-03',params=p)
 class Stream:
  calls=0
  def get_range(self,**kwargs):self.calls+=1;kwargs['path'].write_bytes(b'complete fixture')
 stream=Stream();client=SimpleNamespace(timeseries=stream)
 h=SimpleNamespace(dataset=p['dataset'],schema=p['schema'],stype_in=p['stype_in'],symbols=p['symbols'],mappings={s:[] for s in p['symbols']},start=pd.Timestamp(p['start']).value,end=pd.Timestamp(p['end']).value)
 monkeypatch.setattr(mod.db.DBNStore,'from_file',lambda path:SimpleNamespace(metadata=h))
 first=mod.retrieve_once(r,client,tmp_path);second=mod.retrieve_once(r,client,tmp_path)
 assert first==second and stream.calls==1
 (tmp_path/'receipts'/'2024-07-03.json').unlink()
 assert mod.retrieve_once(r,client,tmp_path)==first and stream.calls==1
 (tmp_path/'raw'/'2024-07-03.dbn.zst').write_bytes(b'corrupted')
 with pytest.raises(ValueError):mod.retrieve_once(r,client,tmp_path)
 assert stream.calls==1


def test_uncertain_stream_attempt_blocks_duplicate(tmp_path):
 for name in ('raw','intents','receipts'):(tmp_path/name).mkdir()
 (tmp_path/'intents'/'2024-07-03.json').write_text('{}')
 with pytest.raises(RuntimeError,match='uncertain'):mod.retrieve_once(dict(pilot_session='2024-07-03',params=params()),None,tmp_path)


def test_sampling_uses_at_most_twelve_calls_and_reuses_quote(tmp_path,monkeypatch):
 out=tmp_path/'out';prior=tmp_path/'prior';out.mkdir();prior.mkdir()
 requests=[dict(pilot_session=f'session-{i:02}',params=params(),symbol_minutes=i+1.) for i in range(60)]
 # Make the canonical calls unique, as real session requests are.
 for i,r in enumerate(requests):r['params']=dict(r['params'],symbols=[f'NDXP  {i:06}P19000000'])
 manifest=dict(requests=requests,requested_symbol_minutes=sum(r['symbol_minutes'] for r in requests),original_fragment_count=501)
 (out/'session_request_manifest.json').write_text(json.dumps(manifest));(prior/'preflight_quote.json').write_text('{"results":{}}')
 monkeypatch.setattr(mod,'OUT',out);monkeypatch.setattr(mod,'PRIOR',prior);monkeypatch.setattr(mod,'manifest',lambda:manifest);monkeypatch.setattr(mod,'load_key',lambda path:'unused')
 calls=[]
 def call(method,params):calls.append(method);return .001 if method=='get_cost' else 100
 monkeypatch.setattr(mod,'http_metadata',lambda key:call)
 quote=mod.quote();assert quote['purchase_allowed'] and len(calls)==12
 assert mod.quote()==quote and len(calls)==12


@pytest.mark.parametrize('mutation',['valid','symbol','before_start','at_end'])
def test_record_body_verifies_exact_symbols_and_exclusive_end(mutation):
 from mgc_v05l.research.ndxp_panel_analysis import validate_body
 p=params();start=pd.Timestamp(p['start']).value;end=pd.Timestamp(p['end']).value
 frame=pd.DataFrame({'symbol':[p['symbols'][0],p['symbols'][1]],'ts_recv':[start,end-1]})
 if mutation=='symbol':frame.loc[0,'symbol']='SPXW  240705P01900000'
 elif mutation=='before_start':frame.loc[0,'ts_recv']=start-1
 elif mutation=='at_end':frame.loc[1,'ts_recv']=end
 if mutation=='valid':validate_body(frame,p)
 else:
  with pytest.raises(ValueError):validate_body(frame,p)
