import importlib.util
import json
from pathlib import Path
import pytest

spec=importlib.util.spec_from_file_location('monitor',Path(__file__).parents[2]/'tools/phase3d_observability/monitor.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def heartbeat(**updates):
    h=dict(heartbeat_epoch=1000,last_progress_epoch=950,stall_threshold_seconds=1200,
           worker_exception=None,legitimately_waiting=False)
    h.update(updates);return h


@pytest.mark.parametrize('h,now,alive,expected',[
    (heartbeat(),1001,True,'RUNNING'),
    (heartbeat(legitimately_waiting=True),1001,True,'IDLE'),
    (heartbeat(),1601,True,'STALLED'),
    (heartbeat(heartbeat_epoch=2300),2300,True,'STALLED'),
    (heartbeat(),1001,False,'EXITED'),
    (heartbeat(worker_exception='ValueError: broken'),1001,True,'ERROR'),
    (heartbeat(monitor_error='read failed'),1001,True,'ERROR'),
    (heartbeat(legitimately_waiting=True),1601,True,'STALLED')])
def test_status_rules(h,now,alive,expected):
    assert m.classify(h,now,alive)[0]==expected


def test_atomic_heartbeat_replaces_complete_json(tmp_path):
    p=tmp_path/'worker-a.json';m.atomic_text(p,'{"old":true}\n')
    m.atomic_text(p,'{"new":true}\n')
    assert json.loads(p.read_text())=={'new':True}
    assert list(tmp_path.iterdir())==[p]


def test_delta_throughput_and_stall_clock():
    cfg=dict(pid=7,process_identity='identity',stall_threshold_seconds=1200)
    sample=dict(progress_vector=[2,100],throughput_counters=dict(units=2,bytes=100),last_evidence_epoch=900)
    a=m.build_heartbeat('worker-a',cfg,sample,None,1000,True)
    b=m.build_heartbeat('worker-a',cfg,sample,a,1120,True)
    assert b['last_progress_epoch']==900 and b['throughput']['bytes_per_second']==0
    sample=dict(sample,progress_vector=[3,340],throughput_counters=dict(units=3,bytes=340))
    c=m.build_heartbeat('worker-a',cfg,sample,b,1240,True)
    assert c['last_progress_epoch']==1240 and c['throughput']['bytes_per_second']==2
    assert c['throughput']['units_per_minute']==.5


def test_watchdog_detects_stale_without_changing_workers(tmp_path,monkeypatch):
    cfg=dict(workers={'worker-a':dict(pid=7,stall_threshold_seconds=1200)})
    h=dict(heartbeat(),worker='worker-a',pid=7,heartbeat_timestamp='old',progress={})
    m.atomic_text(tmp_path/'worker-a.json',json.dumps(h))
    monkeypatch.setattr(m.time,'time',lambda:2000)
    monkeypatch.setattr(m,'process_alive',lambda c:True)
    m.write_overview(cfg,tmp_path)
    assert 'STALLED' in (tmp_path/'overview.txt').read_text()
    assert json.loads((tmp_path/'worker-a.json').read_text())==h


def test_pid_identity_prevents_reuse(monkeypatch):
    monkeypatch.setattr(m,'process_identity',lambda pid:'different launch')
    assert not m.process_alive(dict(pid=7,process_identity='original launch'))


def test_quarantined_request_errors_are_not_worker_errors(tmp_path):
    p=tmp_path/'log';p.write_text('{"event":"request_failure","disposition":"quarantine_and_continue"}\n')
    assert m.log_exception(p) is None
    p.write_text('Traceback (most recent call last):\nValueError: broken\n')
    assert m.log_exception(p)=='ValueError: broken'


def test_worker_b_only_idle_when_no_backlog(tmp_path):
    out=tmp_path/'out';out.mkdir();stage=tmp_path/'stage.json'
    stage.write_text('{"sessions_completed":0}')
    cfg=dict(out=str(out),derived=str(tmp_path/'derived'),stage1_progress=str(stage),log=str(tmp_path/'missing.log'),watch_mode=True)
    assert m.observe_b(cfg)['legitimately_waiting']
    stage.write_text('{"sessions_completed":1}')
    result=m.observe_b(cfg)
    assert not result['legitimately_waiting'] and result['active_work_count']==1
