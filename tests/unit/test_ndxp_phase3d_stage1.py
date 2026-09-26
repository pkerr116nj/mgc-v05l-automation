from datetime import date
import json
from pathlib import Path
from types import SimpleNamespace
import pytest
from mgc_v05l.research import ndxp_phase3d_stage1 as m


def symbol(day='240705',side='C',strike=19000000):return f'NDXP  {day}{side}{strike:08d}'


def session():return dict(session='2024-07-03',symbols=[symbol(),symbol(side='P')],start='2024-07-03T13:30:00+00:00',end='2024-07-03T14:00:00+00:00',reuse_parent=False)


def test_no_strike_filter_and_inclusive_calendar_dte():
    symbols=[symbol(day='240703'),symbol(day='240708'),symbol(day='240709'),symbol(side='P',strike=1000)]
    h=SimpleNamespace(dataset='OPRA.PILLAR',schema='cbbo-1m',symbols=['NDXP.OPT'],mappings={s:[dict(start_date=date(2024,7,3),end_date=date(2024,7,4))] for s in symbols})
    result=m.universe(h,'2024-07-03')
    assert result==sorted([symbols[0],symbols[1],symbols[3]])


def test_coarse_maximum_symbol_chunks_cover_union():
    s=session();s['symbols']=[symbol(strike=i) for i in range(4360)]
    r=m.units(s);assert [len(x['params']['symbols']) for x in r]==[2000,2000,360]
    assert [q for x in r for q in x['params']['symbols']]==s['symbols']
    for x in r:m.validate_unit(x)
    s['reuse_parent']=True;assert m.units(s)==[]


@pytest.mark.parametrize('field,value',[('symbols',['SPXW  240705C19000000']),('symbols',[symbol(day='240709')]),('end','2024-07-03T20:00:00+00:00'),('limit',None)])
def test_wrong_universe_rejected_before_network(field,value):
    r=m.units(session())[0];r['params'][field]=value
    with pytest.raises(ValueError):m.validate_unit(r)


def setup(root):
    for n in ('intents','receipts','transfers','failures','raw'):(root/n).mkdir()


def test_exact_resume_and_uncertain_protection(tmp_path,monkeypatch):
    setup(tmp_path);calls=[]
    monkeypatch.setattr(m,'exposure',lambda root:1.84)
    monkeypatch.setattr(m.shutil,'disk_usage',lambda p:SimpleNamespace(free=100_000_000_000))
    def stream(**kwargs):calls.append(kwargs);kwargs['path'].write_bytes(b'fixture')
    client=SimpleNamespace(timeseries=SimpleNamespace(get_range=stream));req=m.units(session())[0]
    validator=lambda p,r:dict(uncompressed_bytes=1000,source_interval_complete=True)
    a=m.retrieve(req,tmp_path,tmp_path/'raw',.16,client,validator)
    assert m.retrieve(req,tmp_path,tmp_path/'raw',.16,client,validator)==a and len(calls)==1
    (tmp_path/'receipts'/(req['key']+'.json')).unlink()
    assert m.retrieve(req,tmp_path,tmp_path/'raw',.16,client,validator)==a and len(calls)==1
    (tmp_path/'receipts'/(req['key']+'.json')).unlink();(tmp_path/'transfers'/(req['key']+'.json')).unlink()
    with pytest.raises(RuntimeError,match='Uncertain'):m.retrieve(req,tmp_path,tmp_path/'raw',.16,client,validator)
    assert len(calls)==1


def test_revised_total_ceiling_is_500_including_prior_exposure(tmp_path,monkeypatch):
    setup(tmp_path);monkeypatch.setattr(m,'exposure',lambda root:499.)
    with pytest.raises(RuntimeError,match='500'):m.retrieve(m.units(session())[0],tmp_path,tmp_path/'raw',.16,None)
    assert not list((tmp_path/'intents').glob('*'))


@pytest.mark.parametrize('mutation',['valid','wrong_id','wrong_time','capped'])
def test_full_record_integrity_check(tmp_path,monkeypatch,mutation):
    import numpy as np
    import pandas as pd
    import zstandard
    req=m.units(session())[0];start=pd.Timestamp(req['params']['start']).value
    a=np.array([(123,start)],dtype=[('instrument_id','u4'),('ts_recv','u8')])
    if mutation=='wrong_id':a['instrument_id']=999
    if mutation=='wrong_time':a['ts_recv']=start-1
    if mutation=='capped':req['params']['limit']=1
    h=SimpleNamespace(dataset='OPRA.PILLAR',schema='cmbp-1',start=start,end=pd.Timestamp(req['params']['end']).value,
                      mappings={req['params']['symbols'][0]:[dict(start_date=date(2024,7,3),end_date=date(2024,7,4),symbol='123')]})
    monkeypatch.setattr(m.db.DBNStore,'from_file',lambda p:SimpleNamespace(metadata=h,to_ndarray=lambda **kwargs:iter([a])))
    raw=tmp_path/'fixture.dbn.zst';raw.write_bytes(zstandard.ZstdCompressor().compress(b'fixture'))
    if mutation=='valid':
        result=m.verify(raw,req);assert result['records']==1 and result['source_interval_complete']
        assert result['symbols_without_events']==[req['params']['symbols'][1]]
    else:
        with pytest.raises(ValueError):m.verify(raw,req)


def test_atomic_evidence_write_never_overwrites(tmp_path):
    p=tmp_path/'receipt.json';m.save(p,{'complete':True})
    with pytest.raises(FileExistsError):m.save(p,{'complete':False})
    assert json.loads(p.read_text())=={'complete':True}
    assert not list(tmp_path.glob('*.tmp'))


def test_accounting_keeps_failed_and_active_reservations_separate(tmp_path):
    root=tmp_path/'current';prior=tmp_path/'prior';root.mkdir();prior.mkdir()
    setup(root);setup(prior)
    for directory,key,value in [(prior,'owned',3),(prior,'failed',4),(root,'active',5)]:
        m.save(directory/'intents'/(key+'.json'),dict(reserved_cost_usd=value))
    m.save(prior/'receipts'/'owned.json',dict(metered_upper_cost_usd=.2))
    m.save(prior/'failures'/'failed.json',dict(error_type='BentoServerError',http_status=504,received_bytes=0))
    result=m.accounting(root,prior)
    assert result==dict(completed_estimated_metered_cost_usd=.2,
        uncertain_failed_request_reservation_usd=4,active_reservation_usd=5,actual_provider_charge_usd=None)


def test_offline_count_audit_rejects_saved_subset(tmp_path):
    setup(tmp_path);s=session();plan=dict(sessions=[s]);req=m.units(s)[0]
    assert m.request_count_audit(plan,tmp_path)['canonical_requests']==1
    req['params']['symbols']=req['params']['symbols'][:1]
    m.save(tmp_path/'intents'/(req['key']+'.json'),dict(request=req))
    with pytest.raises(ValueError,match='full inventory'):m.request_count_audit(plan,tmp_path)
