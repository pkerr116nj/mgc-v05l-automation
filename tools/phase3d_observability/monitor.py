"""Read-only worker observation. Writes only configured status/log/lock files.

No worker signals, imports, API requests, retries, or activation hooks.
"""
import argparse
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid


def utc(timestamp=None):
    return datetime.fromtimestamp(time.time() if timestamp is None else timestamp, timezone.utc).isoformat()


def atomic_text(path, text):
    path=Path(path);temporary=path.with_name('.'+path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temporary.open('x') as f:
            f.write(text);f.flush();os.fsync(f.fileno())
        os.replace(temporary,path)
    finally:
        temporary.unlink(missing_ok=True)


def read_json(path, default=None):
    try:return json.loads(Path(path).read_text())
    except FileNotFoundError:return default


def process_identity(pid):
    result=subprocess.run(['/bin/ps','-p',str(pid),'-o','lstart=','-o','command='],
                          capture_output=True,text=True,timeout=10)
    return result.stdout.strip() if result.returncode==0 else None


def process_alive(config):
    # Comparing start time and command also protects against PID reuse.
    return process_identity(config['pid'])==config['process_identity']


def stamp(path):
    try:
        stat=Path(path).stat();return dict(bytes=stat.st_size,mtime=stat.st_mtime)
    except FileNotFoundError:return None


def log_exception(path):
    try:
        with Path(path).open('rb') as f:
            f.seek(max(0,os.fstat(f.fileno()).st_size-16384));text=f.read().decode(errors='replace')
    except FileNotFoundError:return None
    marker=text.rfind('Traceback (most recent call last):')
    if marker<0:return None
    tail=text[marker:]
    # Handled per-request errors are not fatal worker exceptions. A later successful
    # progress line or processing restart supersedes a historical traceback.
    if re.search(r'\n(?:\d+/877 sessions|Validating completed session|\d+ \| \d+ \|)',tail):return None
    errors=re.findall(r'^\w*(?:Error|Exception):.*$',tail,re.M)
    return errors[-1][:500] if errors else 'Traceback reported in worker log'


def files_json(directory):
    return {p.stem:(read_json(p),p.stat().st_mtime) for p in Path(directory).glob('*.json')}


def observe_a(config):
    root=Path(config['root']);raw=Path(config['raw']);prior=Path(config['prior'])
    receipts=files_json(root/'receipts');failures=files_json(root/'failures')
    quarantine=files_json(root/'quarantine');intents=files_json(root/'intents')
    source=read_json(root/'progress.json',{})
    completed_bytes=sum(r['compressed_bytes'] for r,_ in receipts.values() if not r.get('accounting_alias_for'))
    old=read_json(prior/'receipts/2023-03-28_opening.json',{})
    completed_bytes+=old.get('compressed_bytes',0)  # Exactly once, even if raw is now a NAS symlink.
    active=[];partial_bytes=0;latest_change=0
    for key,(intent,_) in intents.items():
        if key in receipts:continue
        if key in failures:
            partial_bytes+=failures[key][0].get('received_bytes',0);continue
        info=stamp(raw/(key+'.dbn.zst'))
        if info:partial_bytes+=info['bytes'];latest_change=max(latest_change,info['mtime'])
        if key not in quarantine:active.append(dict(key=key,bytes=info['bytes'] if info else 0))
    completed_cost=sum(r.get('metered_upper_cost_usd',0) for r,_ in receipts.values() if not r.get('accounting_alias_for'))
    completed_cost+=old.get('metered_upper_cost_usd',0)
    completed_cost+=read_json(root/'inventory_receipt.json',{}).get('metered_upper_cost_usd',0)
    uncertain=sum(i['reserved_cost_usd'] for k,(i,_) in intents.items() if k not in receipts and (k in failures or k in quarantine))
    active_reservation=sum(i['reserved_cost_usd'] for k,(i,_) in intents.items() if k not in receipts and k not in failures and k not in quarantine)
    for p in (prior/'intents').glob('*.json'):
        if not (prior/'receipts'/p.name).exists():uncertain+=read_json(p)['reserved_cost_usd']
    last_success=max([t for _,t in receipts.values()]+[0])
    latest_change=max(latest_change,last_success)
    return dict(progress=dict(sessions_complete=source.get('sessions_completed'),sessions_total=877,
            completed_units=len(receipts),bytes_acquired=completed_bytes+partial_bytes,
            gb_acquired=(completed_bytes+partial_bytes)/1e9),
        active_work_count=max(len(active),source.get('active_workers') or 0),active_request_count=len(active),active_work=active,
        reported_worker_slots=source.get('active_workers'),
        failures=dict(request_failures=len(failures),quarantined=len(quarantine)),
        accounting=dict(completed_estimated_cost=completed_cost,uncertain_reservation=uncertain,
                        active_reservation=active_reservation,actual_billed_cost=None),
        last_success_timestamp=utc(last_success) if last_success else None,
        last_evidence_epoch=latest_change,legitimately_waiting=False,
        progress_vector=[len(receipts),completed_bytes+partial_bytes],
        throughput_counters=dict(units=len(receipts),bytes=completed_bytes+partial_bytes),
        worker_exception=log_exception(config['log']))


def observe_b(config):
    out=Path(config['out']);complete=list((out/'sessions').glob('*/complete.json'))
    processed=len(complete);last_success=max((p.stat().st_mtime for p in complete),default=0)
    stage1=read_json(config['stage1_progress'],{})
    available=stage1.get('sessions_completed');backlog=max(0,available-processed) if available is not None else None
    building=[]
    for directory in Path(config['derived']).glob('*.building'):
        for p in directory.glob('*.parquet'):
            value=stamp(p)
            if value:building.append(dict(path=str(p),**value))
    # Timing summaries are local, small, immutable evidence; no raw data scan.
    timings=files_json(out/'timings')
    processed_raw_bytes=sum(r.get('raw_bytes',0) for r,_ in timings.values())
    building_bytes=sum(v['bytes'] for v in building)
    latest=max([last_success]+[v['mtime'] for v in building])
    progress_files=list(out.glob('progress_*.json'))
    summary=read_json(max(progress_files,key=lambda p:p.stat().st_mtime),{}) if progress_files else {}
    waiting=(backlog==0 and not building and config.get('watch_mode',False))
    return dict(progress=dict(sessions_processed=processed,available_stage1_sessions=available,
            pending_sessions=backlog,candidates=summary.get('candidate_spreads_constructed'),
            processed_raw_bytes_with_timing=processed_raw_bytes,in_progress_output_bytes=building_bytes),
        active_work_count=0 if waiting else (1 if backlog or building else 0),active_work=building,
        failures=dict(worker_exception_count=int(log_exception(config['log']) is not None)),
        last_success_timestamp=utc(last_success) if last_success else None,last_evidence_epoch=latest,
        legitimately_waiting=waiting,waiting_reason='No unprocessed completed Stage 1 sessions' if waiting else None,
        progress_vector=[processed,building_bytes],
        throughput_counters=dict(units=processed,bytes=processed_raw_bytes),
        worker_exception=log_exception(config['log']))


def classify(heartbeat,now,alive,stale_seconds=600):
    if not alive:return 'EXITED','Worker PID no longer exists or identity changed'
    if now-heartbeat.get('heartbeat_epoch',0)>stale_seconds:return 'STALLED','Heartbeat older than 10 minutes'
    if heartbeat.get('worker_exception'):return 'ERROR',heartbeat['worker_exception']
    if heartbeat.get('monitor_error'):return 'ERROR','Observability error: '+heartbeat['monitor_error']
    if heartbeat.get('legitimately_waiting'):return 'IDLE',heartbeat.get('waiting_reason','Waiting for input')
    if now-heartbeat['last_progress_epoch']>heartbeat['stall_threshold_seconds']:
        return 'STALLED','No observed progress beyond configured threshold'
    return 'RUNNING','Recent completion/file progress (initial sample uses existing evidence)'


def build_heartbeat(name,config,sample,previous,now,alive):
    previous=previous if previous and previous.get('worker_identity')==config['process_identity'] else None
    changed=previous is not None and sample['progress_vector']!=previous.get('progress_vector')
    last_progress=now if changed else (previous['last_progress_epoch'] if previous else sample.get('last_evidence_epoch',now))
    if not last_progress:last_progress=now
    # A legitimate idle period is not charged against the next unit's processing time.
    if previous and previous.get('legitimately_waiting') and not sample.get('legitimately_waiting'):last_progress=now
    elapsed=now-previous['heartbeat_epoch'] if previous else None
    throughput={'interval_seconds':elapsed,'units_per_minute':None,'bytes_per_second':None}
    if elapsed and elapsed>0:
        before=previous.get('throughput_counters',{});after=sample['throughput_counters']
        throughput.update(units_per_minute=max(0,after['units']-before.get('units',after['units']))*60/elapsed,
                          bytes_per_second=max(0,after['bytes']-before.get('bytes',after['bytes']))/elapsed)
    value=dict(sample,worker=name,pid=config['pid'],worker_identity=config['process_identity'],
        monitor_pid=os.getpid(),heartbeat_epoch=now,heartbeat_timestamp=utc(now),last_progress_epoch=last_progress,
        progress_advanced_since_previous=changed if previous else None,throughput=throughput,
        stall_threshold_seconds=config['stall_threshold_seconds'],heartbeat_interval_seconds=120,
        detection_only=True,pid_alive=alive)
    value['status'],value['status_reason']=classify(value,now,alive)
    return value


def concise(value):
    p=value.get('progress',{});count=p.get('sessions_complete',p.get('sessions_processed','?'))
    return (f"{value['heartbeat_timestamp']} {value['worker']} {value['status']} pid={value['pid']} "
            f"sessions={count} active={value.get('active_work_count','?')} failures={json.dumps(value.get('failures',{}),separators=(',',':'))} "
            f"throughput={json.dumps(value.get('throughput',{}),separators=(',',':'))}")


def write_worker(name,config,status):
    path=status/(name+'.json');previous=read_json(path)
    try:sample=(observe_a if name=='worker-a' else observe_b)(config)
    except Exception as exc:
        sample=dict(progress={},progress_vector=previous.get('progress_vector',[]) if previous else [],
                    throughput_counters=previous.get('throughput_counters',dict(units=0,bytes=0)) if previous else dict(units=0,bytes=0),
                    active_work_count=None,failures={},last_success_timestamp=previous.get('last_success_timestamp') if previous else None,
                    monitor_error=f'{type(exc).__name__}: {exc}',worker_exception=None,legitimately_waiting=False)
    now=time.time();value=build_heartbeat(name,config,sample,previous,now,process_alive(config))
    atomic_text(path,json.dumps(value,indent=2,allow_nan=False)+'\n')
    with (status/(name+'.log')).open('a') as f:f.write(concise(value)+'\n')
    return value


def write_overview(config,status):
    now=time.time();lines=[f'Phase 3D detection-only watchdog — {utc(now)}',
        'Checks every 300s; heartbeats every 120s; stale threshold 600s. No automatic restarts.','']
    for name,cfg in config['workers'].items():
        try:
            h=read_json(status/(name+'.json'))
            if h is None:
                state,reason=('STALLED','Heartbeat missing') if process_alive(cfg) else ('EXITED','Worker PID no longer exists')
                lines.append(f'{name}: {state} pid={cfg["pid"]} — {reason}');continue
            state,reason=classify(h,now,process_alive(cfg));h['status']=state
            lines.append(concise(h)+f' heartbeat_age={max(0,now-h["heartbeat_epoch"]):.0f}s no_progress_threshold={cfg["stall_threshold_seconds"]}s — {reason}')
        except Exception as exc:lines.append(f'{name}: ERROR — heartbeat read failed: {type(exc).__name__}: {exc}')
    atomic_text(status/'overview.txt','\n'.join(lines)+'\n')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--role',choices=['worker-a','worker-b','watchdog'],required=True)
    parser.add_argument('--once',action='store_true');args=parser.parse_args()
    config=read_json(args.config);status=Path(config['status_directory'])
    if not Path('/Volumes/Personal-Drive').is_mount():raise RuntimeError('NAS is not mounted; refusing local fallback')
    status.mkdir(parents=True,exist_ok=True)
    # Separate local monitor locks; never acquire or modify a worker lock.
    lockpath=args.config.parent/(args.role+'.observer.lock')
    with lockpath.open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        period=300 if args.role=='watchdog' else 120
        while True:
            started=time.monotonic()
            try:
                if args.role=='watchdog':write_overview(config,status)
                else:write_worker(args.role,config['workers'][args.role],status)
            except Exception as exc:print(f'{utc()} observer error: {type(exc).__name__}: {exc}',flush=True)
            if args.once:return
            time.sleep(max(1,period-(time.monotonic()-started)))

if __name__=='__main__':main()
