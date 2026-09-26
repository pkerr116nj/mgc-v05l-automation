from datetime import date,datetime,timedelta
from dataclasses import replace,asdict
import csv
import json
from pathlib import Path
import pytest
from mgc_v05l.research.ndxp_multidte_data import Candidate,NEW_YORK,discover_candidates
from mgc_v05l.research.ndxp_multidte_backtest import evaluate,summarize,write_csv
from mgc_v05l.research.ndxp_multidte_path_features import (paired_path,path_features,first_passage,target_exit,prices,economics,settlement_payoff,observed_underwater,sign_changes,session_paths)
from mgc_v05l.research.ndxp_multidte_robustness import summarize_trades
from mgc_v05l.research.ndxp_multidte_validation import WINDOW,manifest

T=datetime(2024,6,3,9,31,tzinfo=NEW_YORK)
C=Candidate(T.date(),date(2024,6,5),2,2,6,T,110,100,'NDXP  240605P00110000','NDXP  240605P00100000',10,12,4,6,6,4)

def series(values,minutes=None):
    return [(T+timedelta(minutes=m),v) for m,v in zip(minutes if minutes is not None else range(len(values)),values)]

def test_pairing_midpoint_and_boundaries():
    paths={C.short_symbol:[(T-timedelta(seconds=1),10,12),(T,10,12),(T+timedelta(minutes=1),11,13)], C.long_symbol:[(T,4,6),(T+timedelta(minutes=1),4,6)]}
    actual,d=paired_path(C,paths)
    assert actual==series([6,7])
    assert d['outside_trade_minutes']==0

def test_pairing_never_rounds_preentry_into_path():
    c=replace(C,entry_time=T+timedelta(seconds=30))
    actual,_=paired_path(c,{C.short_symbol:[(T,10,12)],C.long_symbol:[(T,4,6)]})
    assert actual==[]

def test_pairing_drops_unmatched_invalid_and_post_expiration():
    late=T+timedelta(days=3)
    paths={C.short_symbol:[(T,20,22),(late,10,12)], C.long_symbol:[(T,4,6),(T+timedelta(minutes=1),4,6),(late,4,6)]}
    actual,d=paired_path(C,paths)
    assert actual==[] and d['invalid_spread_minutes']==1 and d['unpaired_minutes']==1 and d['outside_trade_minutes']==1

def test_duplicate_minute_uses_latest():
    p={C.short_symbol:[(T,10,12),(T+timedelta(seconds=20),11,13)],C.long_symbol:[(T,4,6)]}
    actual,d=paired_path(C,p)
    assert actual==[(T+timedelta(seconds=20),7)] and d['short_duplicate_minutes']==1

@pytest.mark.parametrize('slip',[0,.05,.1,.15,.25])
def test_adverse_slippage(slip):
    entry,exit=prices(6,3,slip)
    assert entry==6-slip and exit==3+slip
    pnl,risk=economics(6,3,slip)
    assert pnl==pytest.approx((3-2*slip)*2000-52.96)
    assert risk==pytest.approx((4+slip)*2000+52.96)

def test_first_passage():
    s=series([6,9,4,3,2])
    assert first_passage(s,3)==3
    assert first_passage(s,8,True)==1
    assert first_passage(s,.5) is None

@pytest.mark.parametrize('mode,expected',[('touch',1),('persist_2',4),('persist_3',5),('next_minute',2)])
def test_persistence(mode,expected):
    assert target_exit(series([6,2,4,2,2,2]),3,.05,mode)==expected

def test_persistence_gap_and_overnight():
    assert target_exit(series([2,2,2],[0,2,1440]),3,0,'persist_2') is None
    assert target_exit(series([2,2,2],[0,2,3]),3,0,'persist_2')==2

def test_next_minute_not_next_available_or_later_touch():
    assert target_exit(series([2,2,2],[0,2,3]),3,0,'next_minute') is None
    assert target_exit(series([2]),3,0,'next_minute') is None

def test_touch_includes_exit_friction():
    assert target_exit(series([3,2.95]),3,.05)==1

def test_underwater_does_not_count_gaps():
    assert observed_underwater(series([8,9,8,3],[0,1,1440,1441]),6)==2
    assert sign_changes(series([5,7,5,7],[0,1,2,1440]),6)==2

def test_path_schema_missing_preserved():
    missing=path_features(C,[],{})
    filled=path_features(C,series([6,9,3]),{})
    assert set(missing)==set(filled)
    assert missing['path_status']=='missing' and missing['favorable_3_status']=='missing_path'
    assert filled['favorable_3_minutes']==2 and filled['target_3_deep_adverse_then_success']
    assert filled['target_3_max_before_hit']==9

def test_fixed_horizon_no_future_or_stale_quote():
    row=path_features(C,series([6,4,3],[0,2,120]),{})
    assert row['plus_1m_mid'] is None
    assert row['plus_1m_quote_age_seconds']==60
    assert row['plus_2m_mid']==4
    assert row['plus_5m_mid'] is None
    assert row['plus_120m_mid']==3
    assert row['degraded'] and row['path_crosses_degraded_date']

def test_adverse_after_success_not_before():
    row=path_features(C,series([6,3,9]),{})
    assert not row['target_3_hit_9_first']
    assert not row['target_3_deep_adverse_then_success']

def test_failure_best_improvement_and_never_hit():
    row=path_features(C,series([6,4,9]),{})
    assert row['target_3_best_improvement_before_failure']==2
    assert row['favorable_3_status']=='never_hit'
    assert row['target_3_max_before_hit'] is None

@pytest.mark.parametrize('underlying,expected',[(0,10),(99,10),(100,10),(105,5),(110,0),(200,0)])
def test_settlement_bounded(underlying,expected):
    assert settlement_payoff(underlying,110,100)==expected

def test_literal_calendar_dte_weekend():
    # Reuse discovery implementation's literal date selection, not trading days.
    import pandas as pd
    friday=date(2024,6,7); ts=datetime(2024,6,7,9,31,tzinfo=NEW_YORK)
    frame=pd.DataFrame({'ts_recv':[ts,ts], 'symbol':['NDXP  240610P00110000','NDXP  240610P00100000'],'bid_px_00':[10.,4.],'ask_px_00':[12.,6.]}).set_index('ts_recv')
    rows=discover_candidates(frame,friday,dtes=(2,3),targets=(6,))
    assert len(rows)==1 and rows[0].calendar_dte==3
    assert (rows[0].expiration-rows[0].session_date).days==3
    assert rows[0].entry_time<=ts
    assert not any('future' in k or 'path' in k for k in asdict(rows[0]))

def test_drawdown_streak_uses_chronological_order():
    rows=[dict(calendar_dte=2,target_credit=6,exit_target=3,slippage=.05,mode='touch',session_date=str(i),entry_time='',candidate_id=i,net_pnl=p,max_risk=10,hit_target=p>0) for i,p in enumerate([10,-5,-6,4,-1])]
    r=summarize_trades(list(reversed(rows)))[0]
    assert r['max_drawdown']==11 and r['longest_losing_streak']==2
    assert r['profit_factor']==pytest.approx(14/12)

def test_stream_session_isolation_and_invalid_quotes(tmp_path):
    p=tmp_path/'paths.csv'
    write_csv(p,[dict(session_date=d,quote_time=T.isoformat(),symbol='same',bid=b,ask=a) for d,b,a in [('2024-06-03',1,2),('2024-06-03',float('nan'),2),('2024-06-04',3,4)]])
    rows=list(session_paths(p))
    assert len(rows)==2 and rows[0][1]['same']==[(T,1,2)] and rows[1][1]['same']==[(T,3,4)]

def test_stream_rejects_noncontiguous_sessions(tmp_path):
    p=tmp_path/'paths.csv'
    write_csv(p,[dict(session_date=d,quote_time=T.isoformat(),symbol='same',bid=1,ask=2) for d in ['a','b','a']])
    with pytest.raises(ValueError): list(session_paths(p))

def test_cash_close_snapshot_and_terminal_pnl():
    close=datetime(2024,6,3,16,tzinfo=NEW_YORK)
    row=path_features(C,[(T,6),(close,4)],{})
    assert row['day_0_cash_close_mid']==4
    assert row['terminal_pnl']==pytest.approx(3747.04)
    assert row['day_1_cash_close_mid'] is None
    assert row['day_1_cash_close_quote_age_seconds']==86400

def test_persistence_features_execute_at_confirmation():
    row=path_features(C,series([6,2,2,2]),{})
    assert row['target_3_touch_minutes']==1
    assert row['target_3_persist_2_minutes']==2
    assert row['target_3_persist_3_minutes']==3
    assert row['target_3_next_minute_minutes']==2


def test_full_local_pipeline_preserves_raw_and_missing_candidates(tmp_path,monkeypatch):
    from mgc_v05l.research.ndxp_multidte_deep_dive import run
    from mgc_v05l.research import ndxp_multidte_data
    def forbidden(): raise AssertionError('No remote client allowed')
    monkeypatch.setattr(ndxp_multidte_data,'historical_client',forbidden)
    absent=replace(C,session_date=date(2026,9,22),expiration=date(2026,9,24),entry_time=datetime(2026,9,22,9,31,tzinfo=NEW_YORK))
    def serialized(c): return {k:v.isoformat() if hasattr(v,'isoformat') else v for k,v in asdict(c).items()}
    write_csv(tmp_path/f'candidates_{WINDOW}.csv',[serialized(C),serialized(absent)])
    paths={C.short_symbol:[(t,m+5,m+5) for t,m in series([6,2,2,2])],C.long_symbol:[(t,5,5) for t,_ in series([6,2,2,2])]}
    records=[dict(session_date=str(C.session_date),quote_time=t.isoformat(),symbol=s,bid=b,ask=a) for s,rows in paths.items() for t,b,a in rows]
    write_csv(tmp_path/f'paths_{WINDOW}.csv',records)
    base=tmp_path/'backtest_mid_5c'; base.mkdir(); write_csv(base/'summary.csv',summarize(evaluate([C],paths)))
    before=manifest(tmp_path)
    report=run(tmp_path)
    assert before==manifest(tmp_path)
    assert report['baseline_reproduction']['matches']
    assert report['counts']['feature_rows']==2 and report['counts']['missing_paths']==1
    rows=list(csv.DictReader((tmp_path/'deep_dive_v1'/'path_features.csv').open()))
    assert rows[1]['path_status']=='missing'
    assert report['counts']['robustness_groups']==80
    assert all(r['partition']=='development' for r in report['development_anatomy'])
    assert not (tmp_path/'deep_dive_v1'/'entry_features.csv').exists()
