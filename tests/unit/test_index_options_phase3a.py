import copy,json,threading,time
from pathlib import Path
import pytest
from mgc_v05l.research.index_options_metadata_estimator import (price_samples,validate_job,cache_key,MAX_CALLS,http_metadata)
from mgc_v05l.research.index_options_acquisition_plan import (INSTRUMENTS,build_manifest,request_design,project_structural,render)
from mgc_v05l.research.index_options_local_inventory import header


def job(schema='cbbo-1m',start='2024-06-12T09:30:00-04:00',end='2024-06-12T09:32:00-04:00',symbols=None):
    return {'method':'get_cost','params':{'dataset':'OPRA.PILLAR','schema':schema,'stype_in':'parent','symbols':symbols or ['SPXW.OPT'],'start':start,'end':end}}


def test_bounded_calls_and_concurrency():
    lock=threading.Lock();active=peak=calls=0
    def call(method,params):
        nonlocal active,peak,calls
        with lock:active+=1;calls+=1;peak=max(peak,active)
        time.sleep(.005)
        with lock:active-=1
        return .2
    jobs=[job(symbols=[f'ROOT{i}.OPT']) for i in range(10)]
    r=price_samples(jobs,call,workers=3)
    assert calls==r['call_count']==10 and 1<=peak<=3 and r['download_calls']==0


def test_call_budget_rejected_before_any_network():
    calls=[]
    with pytest.raises(ValueError):price_samples([job(symbols=[f'ROOT{i}.OPT']) for i in range(MAX_CALLS+1)],lambda *a:calls.append(a))
    assert calls==[]

@pytest.mark.parametrize('method',['get_range','submit_job','download','timeseries.get_range','list_jobs'])
def test_no_download_path(method):
    j=job();j['method']=method
    with pytest.raises(ValueError):price_samples([j],lambda *a:pytest.fail('Network invoked'))

@pytest.mark.parametrize('mutation',[{'symbols':['ALL_SYMBOLS']},{'end':'2024-06-13T09:32:00-04:00'},{'symbols':['x']*25},{'start':'2024-06-12T09:30:00'},{'dataset':'GLBX.MDP3'},{'schema':'mbo'}])
def test_invalid_request_guards(mutation):
    j=job();j['params'].update(mutation)
    with pytest.raises(ValueError):validate_job(j)


def test_cache_dedup_and_input_immutability():
    j=job();before=copy.deepcopy(j)
    r=price_samples([j,j],lambda *a:1.5)
    assert r['call_count']==1 and j==before
    cached=price_samples([j],lambda *a:pytest.fail('Unneeded call'),cache=r['results'])
    assert cached['call_count']==0 and cached['cache_hits']==1


def test_errors_not_zero_cost_and_no_secret_output():
    def fail(*a):raise RuntimeError('secret authentication material')
    r=price_samples([job()],fail)
    row=next(iter(r['results'].values()))
    assert row['status']=='unavailable' and 'value' not in row
    assert 'secret' not in json.dumps(r)


def test_all_validation_before_first_call():
    bad=job();bad['method']='get_range'
    with pytest.raises(ValueError):price_samples([job(),bad],lambda *a:pytest.fail('Network invoked'))

@pytest.mark.parametrize('workers',[0,5])
def test_worker_cap(workers):
    with pytest.raises(ValueError):price_samples([],lambda *a:None,workers=workers)


def test_provider_quote_is_labeled():
    r=price_samples([job()],lambda *a:.01)
    assert next(iter(r['results'].values()))['cost_label']=='provider_sample_quote'


def test_instruments_explicit_and_mechanically_distinct():
    assert set(INSTRUMENTS)=={'NDXP','SPX','QQQ','RUT'}
    assert INSTRUMENTS['SPX']['preferred_root']=='SPXW'
    assert INSTRUMENTS['RUT']['parents']==['RUT.OPT','RUTW.OPT']
    assert INSTRUMENTS['QQQ']['exercise']=='American'
    assert INSTRUMENTS['NDXP']['exercise']=='European'
    assert all(c['multiplier']==100 for c in INSTRUMENTS.values())


def test_coverage_and_no_purchase_authority():
    for instrument in INSTRUMENTS:
        for layer in ['structural','path','execution']:
            r=request_design(instrument,layer)
            assert r['download_authorized'] is False and r['timezone']=='America/New_York'
            assert r['dataset']=='OPRA.PILLAR'
        assert '2025-02-20' in request_design(instrument,'path')['schema_gate']
        assert request_design(instrument,'path')['date_range'][1]=='2024-12-31'


def test_structural_estimate_math_and_labels():
    x=project_structural([1,3],[1000,3000],10)
    assert x['components']['opening_usd']==20
    assert x['download_requests_upper']==30
    assert x['cost_usd']['low']<x['cost_usd']['central']<x['cost_usd']['high']
    assert x['estimate_label']=='sample_scaled_plus_explicit_record_model'


def evidence_fixture():
    results={};i=0
    for root in ['NDXP','SPXW','QQQ','RUTW']:
        for method,value in [('get_cost',.01),('get_billable_size',1000000)]:
            j=job(symbols=[root+'.OPT']);j['method']=method
            results[str(i)]={'request':j,'status':'ok','value':value};i+=1
    j=job('cmbp-1',end='2024-06-12T10:00:00-04:00',symbols=['NDXP  240614P19460000','NDXP  240614P19450000'])
    j['method']='get_billable_size';j['params']['stype_in']='raw_symbol'
    results[str(i)]={'request':j,'status':'ok','value':1000000}
    samples={'call_count':9,'results':results}
    catalog={'call_count':3,'results':{m:{'request':{'method':m},'status':'ok','value':{}} for m in ['list_schemas','list_unit_prices','get_dataset_range']}}
    inv={'groups':[],'market_file_count':0,'logical_bytes':0}
    return inv,catalog,samples


def test_manifest_deterministic_and_separated_categories():
    args=evidence_fixture();before=copy.deepcopy(args)
    a=build_manifest(*args);b=build_manifest(*copy.deepcopy(args))
    assert json.dumps(a,sort_keys=True)==json.dumps(b,sort_keys=True)
    assert args==before and a['schema_version']==1
    assert all(k in a for k in ['already_owned_data','recommended_new_data','optional_later_data','unnecessary_data'])
    entries=a['recommended_new_data']+a['optional_later_data']
    assert len(entries)==12 and len({e['id'] for e in entries})==12
    assert all(e['estimate']['estimate_label'] for e in entries)
    assert render(a,args[0])==render(b,args[0])


def test_http_allowlist_function_contains_no_download_api():
    import inspect
    text=inspect.getsource(http_metadata)
    assert '/v0/metadata.' in text and 'timeseries' not in text and 'batch.' not in text


def test_real_dbn_header_when_local_fixture_present():
    files=list(Path('output/ndxp_multidte/path_cache').rglob('*.dbn.zst'))
    if not files:pytest.skip('Local optional integration cache not present')
    p=files[0];before=p.stat()
    h=header(p)
    assert h['schema']=='cbbo-1m' and h['dataset']=='OPRA.PILLAR'
    assert h['mapped_roots']==['NDXP'] and len(h['prefix_64KiB_sha256'])==64
    assert p.stat().st_size==before.st_size and p.stat().st_mtime_ns==before.st_mtime_ns
