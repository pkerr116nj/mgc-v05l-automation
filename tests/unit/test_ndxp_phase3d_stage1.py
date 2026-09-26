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
    monkeypatch.setattr(m,'native_capacity',lambda p:dict(available_bytes=100_000_000_000))
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


def test_native_df_avoids_overflow_and_python_capacity(tmp_path,monkeypatch):
    import os,shutil
    def forbidden(*args):raise AssertionError('Python capacity APIs must not be used')
    monkeypatch.setattr(os,'statvfs',forbidden);monkeypatch.setattr(shutil,'disk_usage',forbidden)
    def native(args,**kwargs):
        assert args==['/bin/df','-kP','/Volumes/Personal-Drive/data']
        return SimpleNamespace(stdout='Filesystem 1024-blocks Used Available Capacity Mounted on\n//server/Personal-Drive 4882812500 12231168 4870581332 1% /Volumes/Personal-Drive\n')
    monkeypatch.setattr(m.subprocess,'run',native)
    result=m.native_capacity('/Volumes/Personal-Drive/data')
    assert result['total_bytes']==5_000_000_000_000
    assert result['available_bytes']==4_987_475_283_968


@pytest.mark.parametrize('line',[
    '/dev/disk3 4882812500 0 4882812500 0% /',
    '//server/share 10 0 -1 0% /Volumes/Personal-Drive',
    'broken output'])
def test_native_df_rejects_missing_mount_or_invalid_capacity(monkeypatch,line):
    monkeypatch.setattr(m.subprocess,'run',lambda *a,**k:SimpleNamespace(stdout='header\n'+line+'\n'))
    with pytest.raises(RuntimeError):m.native_capacity('/Volumes/Personal-Drive/data')


def test_low_native_capacity_stops_before_paid_intent(tmp_path,monkeypatch):
    setup(tmp_path);monkeypatch.setattr(m,'exposure',lambda root:0)
    monkeypatch.setattr(m,'native_capacity',lambda p:dict(available_bytes=m.NAS_FREE_FLOOR))
    with pytest.raises(RuntimeError,match='Insufficient storage'):
        m.retrieve(m.units(session())[0],tmp_path,tmp_path/'raw',.16,None)
    assert not list((tmp_path/'intents').glob('*'))


def test_checkpoint_quarantines_failed_and_orphan_attempts(tmp_path):
    setup(tmp_path);sessions=[];reqs=[]
    # Use distinct keys via symbol groups; no network or validator required for queue selection.
    s=session();s['symbols']=[symbol(strike=i) for i in range(10000)]
    reqs=m.units(s);plan=dict(sessions=[s])
    m.save(tmp_path/'receipts'/(reqs[0]['key']+'.json'),{})
    m.save(tmp_path/'failures'/(reqs[1]['key']+'.json'),{})
    m.save(tmp_path/'transfers'/(reqs[1]['key']+'.json'),{})  # Failed overrides transfer.
    m.save(tmp_path/'intents'/(reqs[2]['key']+'.json'),dict(reserved_cost_usd=1))
    (tmp_path/'raw'/(reqs[3]['key']+'.dbn.zst')).write_bytes(b'partial')
    pending=m.checkpoint_queue(plan,tmp_path,tmp_path/'raw')
    assert [r['key'] for r in pending]==[reqs[0]['key'],reqs[4]['key']]
    assert len(list((tmp_path/'quarantine').glob('*.json')))==3
    assert m.checkpoint_queue(plan,tmp_path,tmp_path/'raw')==pending
    assert (tmp_path/'raw'/(reqs[3]['key']+'.dbn.zst')).read_bytes()==b'partial'


def test_quarantined_transfer_never_retried_or_validated(tmp_path):
    setup(tmp_path);req=m.units(session())[0]
    m.save(tmp_path/'transfers'/(req['key']+'.json'),{})
    m.quarantine(req,tmp_path,'uncertain')
    with pytest.raises(RuntimeError,match='quarantined'):
        m.retrieve(req,tmp_path,tmp_path/'raw',.16,None)


@pytest.mark.parametrize('exception',[m.BentoError('Error streaming response: connection reset'),m.requests.exceptions.ReadTimeout('timed out')])
def test_isolated_stream_failure_does_not_terminate_queue(exception):
    calls=[];quarantined=[]
    def work(req):
        calls.append(req['key'])
        if req['key']=='1':raise exception
    errors,fatal=m.dispatch([dict(key=str(i)) for i in range(6)],work,1,
        lambda req,exc:quarantined.append(req['key']),lambda n:None)
    assert calls==list(map(str,range(6))) and quarantined==['1'] and not fatal and len(errors)==1


def test_isolated_504_continues_but_systemic_outage_stops():
    class ServerFailure(m.BentoError):http_status=504
    calls=[]
    def work(req):
        calls.append(req['key'])
        if req['key']!='1':raise ServerFailure('gateway timeout')
    errors,fatal=m.dispatch([dict(key=str(i)) for i in range(9)],work,1,lambda *a:None,lambda n:None,systemic_failure_streak=3)
    assert calls==['0','1','2','3','4'] and fatal


@pytest.mark.parametrize('exception',[PermissionError('storage permission'),OSError('disk failure'),RuntimeError('$500 ceiling'),ValueError('integrity'),m.BentoError('Error streaming response: No space left on device')])
def test_systemic_error_stops_new_dispatch(exception):
    calls=[]
    def work(req):calls.append(req['key']);raise exception
    _,fatal=m.dispatch([dict(key=str(i)) for i in range(8)],work,1,lambda *a:None,lambda n:None)
    assert fatal and calls==['0']


def test_auth_failure_is_systemic():
    class AuthFailure(m.BentoError):http_status=401
    assert not m.isolated_request_failure(AuthFailure('invalid credentials'))


def test_four_workers_drain_on_systemic_failure():
    import threading
    gate=threading.Barrier(4);calls=[]
    def work(req):
        calls.append(req['key']);gate.wait(timeout=5)
        if req['key']=='0':raise PermissionError('disk unavailable')
    _,fatal=m.dispatch([dict(key=str(i)) for i in range(4)],work,4,lambda *a:None,lambda n:None)
    assert fatal and sorted(calls)==['0','1','2','3']
