import json
from types import SimpleNamespace
import pytest
from mgc_v05l.research import ndxp_phase3d as m


def test_sessions_full_parent_and_half_day():
    r=m.requests('2024-07-03','2024-07-05')
    assert len(r)==2
    assert r[0]['params']['end']=='2024-07-03T17:00:00+00:00'
    assert all(x['params']['symbols']==['NDXP.OPT'] for x in r)
    for x in r:m.validate_request(x)
    assert m.requests('2024-07-03','2024-07-03','opening')[0]['params']['end']=='2024-07-03T14:00:00+00:00'


def test_ceiling_reserves_inflight_and_disk():
    assert m.admit(.16,98,100_000_000,20_000_000_000)==pytest.approx(1.28256)
    with pytest.raises(RuntimeError,match='ceiling'):m.admit(.16,99,100_000_000,20_000_000_000)
    with pytest.raises(RuntimeError,match='storage'):m.admit(.16,0,100_000_000,1_000_000_000)


def test_wrong_universe_rejected():
    r=m.requests('2024-07-03','2024-07-03')[0]
    r['params']['symbols']=['SPX.OPT']
    with pytest.raises(ValueError):m.validate_request(r)


def setup(root):
    for name in ('raw','receipts','intents','transfers'):(root/name).mkdir()


def test_no_duplicate_download_and_transfer_resume(tmp_path,monkeypatch):
    setup(tmp_path)
    monkeypatch.setattr(m.shutil,'disk_usage',lambda p:SimpleNamespace(free=20_000_000_000))
    calls=[]
    def stream(**kwargs):calls.append(kwargs);kwargs['path'].write_bytes(b'fixture')
    client=SimpleNamespace(timeseries=SimpleNamespace(get_range=stream))
    req=m.requests('2024-07-03','2024-07-03')[0]
    validate=lambda p,r:dict(uncompressed_bytes_including_metadata=1000,source_interval_complete=True)
    a=m.retrieve(req,tmp_path,client,.16,validate)
    assert m.retrieve(req,tmp_path,client,.16,validate)==a and len(calls)==1
    (tmp_path/'receipts'/'2024-07-03_full.json').unlink()
    assert m.reserved_cost(tmp_path)==pytest.approx(1.28256)
    assert m.retrieve(req,tmp_path,client,.16,validate)==a and len(calls)==1
    assert m.reserved_cost(tmp_path)==pytest.approx(.00000016)


def test_uncertain_attempt_not_retried(tmp_path):
    setup(tmp_path)
    (tmp_path/'intents'/'2024-07-03_full.json').write_text('{}')
    with pytest.raises(RuntimeError,match='Uncertain'):
        m.retrieve(m.requests('2024-07-03','2024-07-03')[0],tmp_path,None,.16)


def test_provider_failure_is_durable_and_never_automatically_retried(tmp_path,monkeypatch):
    from databento.common.error import BentoServerError
    setup(tmp_path)
    monkeypatch.setattr(m.shutil,'disk_usage',lambda p:SimpleNamespace(free=20_000_000_000))
    calls=[]
    def fail(**kwargs):
        calls.append(kwargs)
        raise BentoServerError(http_status=504,message='gateway timeout')
    client=SimpleNamespace(timeseries=SimpleNamespace(get_range=fail))
    req=m.requests('2024-07-03','2024-07-03')[0]
    with pytest.raises(BentoServerError):m.retrieve(req,tmp_path,client,.16)
    evidence=json.loads((tmp_path/'failures'/'2024-07-03_full.json').read_text())
    assert evidence['http_status']==504 and evidence['compressed_bytes']==0
    assert m.reserved_cost(tmp_path)==pytest.approx(1.28256)
    with pytest.raises(RuntimeError,match='Uncertain'):m.retrieve(req,tmp_path,client,.16)
    assert len(calls)==1


def test_validated_cache_corruption_blocks_network(tmp_path,monkeypatch):
    setup(tmp_path)
    req=m.requests('2024-07-03','2024-07-03')[0]
    (tmp_path/'receipts'/'2024-07-03_full.json').write_text(json.dumps(dict(request=req,sha256='bad')))
    (tmp_path/'raw'/'2024-07-03_full.dbn.zst').write_bytes(b'corrupt')
    with pytest.raises(ValueError,match='changed'):m.retrieve(req,tmp_path,None,.16)


@pytest.mark.parametrize('case',['valid','wrong_symbol','wrong_time','empty','record_cap'])
def test_record_validation_and_truncation(tmp_path,monkeypatch,case):
    import pandas as pd
    import zstandard
    req=m.requests('2024-07-03','2024-07-03')[0]
    start=pd.Timestamp(req['params']['start']).value
    frame=pd.DataFrame({'symbol':['NDXP  240705P19000000'],'ts_recv':[start]})
    if case=='wrong_symbol':frame['symbol']='SPXW  240705P01900000'
    if case=='wrong_time':frame['ts_recv']=start-1
    if case=='empty':frame=frame.iloc[:0]
    if case=='record_cap':req['params']['limit']=1
    header=SimpleNamespace(dataset='OPRA.PILLAR',schema='cmbp-1',start=start,end=pd.Timestamp(req['params']['end']).value)
    monkeypatch.setattr(m.db.DBNStore,'from_file',lambda p:SimpleNamespace(metadata=header,to_df=lambda **kw:iter([frame])))
    raw=tmp_path/'fixture.dbn.zst';raw.write_bytes(zstandard.ZstdCompressor().compress(b'fixture'))
    if case in ('wrong_symbol','wrong_time','empty'):
        with pytest.raises(ValueError):m.validate_artifact(raw,req)
    else:
        result=m.validate_artifact(raw,req)
        assert result['source_interval_complete']==(case=='valid')
        assert result['eligible_0_5dte_symbols']==['NDXP  240705P19000000']


def test_stale_or_invalid_unit_rate_blocks_paid_resume(tmp_path):
    from datetime import datetime,timezone,timedelta
    evidence=dict(checked_at_utc=datetime.now(timezone.utc).isoformat(),cmbp1_record_bytes=80,
                  rates=[dict(mode='historical-streaming',unit_prices={'cmbp-1':.16})])
    p=tmp_path/'unit_rate.json';p.write_text(json.dumps(evidence))
    assert m.unit_rate(tmp_path)==.16
    evidence['checked_at_utc']=(datetime.now(timezone.utc)-timedelta(days=2)).isoformat()
    p.write_text(json.dumps(evidence))
    with pytest.raises(ValueError,match='stale'):m.unit_rate(tmp_path)


def test_observed_budget_stop_prevents_new_client_or_paid_request(tmp_path,monkeypatch):
    import sys
    (tmp_path/'budget_stop.json').write_text('{"status":"blocked"}')
    monkeypatch.setattr(m,'OUT',tmp_path)
    monkeypatch.setattr(sys,'argv',['phase3d','--mode','opening','--max-sessions','2'])
    def forbidden(*args,**kwargs):raise AssertionError('Must stop before client construction')
    monkeypatch.setattr(m.db,'Historical',forbidden)
    with pytest.raises(RuntimeError,match='budget-risk stop'):m.main()
