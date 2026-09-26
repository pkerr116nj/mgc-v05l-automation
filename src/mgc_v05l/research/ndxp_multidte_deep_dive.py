"""Run NDXP Phase 2 locally from immutable caches; zero remote requests.

CLI: python -m mgc_v05l.research.ndxp_multidte_deep_dive
"""
from __future__ import annotations
import argparse
import csv
from collections import defaultdict
from pathlib import Path
from statistics import median
from .ndxp_multidte_data import load_candidates
from .ndxp_multidte_backtest import evaluate, summarize, write_csv
from .ndxp_multidte_validation import WINDOW, manifest, dump, development_only
from .ndxp_multidte_path_features import (session_paths,paired_path,path_features,TARGETS,SLIPPAGES,target_exit,economics)
from .ndxp_multidte_robustness import summarize_trades


def run(root):
    out=root/'deep_dive_v1'; out.mkdir(exist_ok=True)
    before=manifest(root)
    candidates=load_candidates(root/f'candidates_{WINDOW}.csv')
    by_session=defaultdict(list)
    for i,c in enumerate(candidates): by_session[str(c.session_date)].append((i,c))
    features=[]; trades=[]; legacy=[]; seen=set()
    for session,paths in session_paths(root/f'paths_{WINDOW}.csv'):
        selected=by_session.get(session,[])
        # Existing helper is deliberately retained for exact legacy reproduction.
        legacy.extend(evaluate([c for _,c in selected],paths))
        for i,c in selected:
            seen.add(i); series,diagnostics=paired_path(c,paths)
            row=path_features(c,series,diagnostics); row['candidate_id']=i; features.append(row)
            if not series: continue
            for slip in SLIPPAGES:
                for target in TARGETS:
                    for mode in ('touch','persist_2','persist_3','next_minute'):
                        idx=target_exit(series,target,slip,mode)
                        chosen=series[idx] if idx is not None else series[-1]
                        pnl,risk=economics(c.mid_credit,chosen[1],slip)
                        trades.append({'candidate_id':i,'session_date':str(c.session_date),'entry_time':c.entry_time.isoformat(),
                            'calendar_dte':c.calendar_dte,'target_credit':c.target_credit,'exit_target':target,'slippage':slip,'mode':mode,
                            'net_pnl':pnl,'max_risk':risk,'hit_target':idx is not None,
                            'missing_next_minute':mode=='next_minute' and idx is None and target_exit(series,target,slip,'touch') is not None})
    for i,c in enumerate(candidates):
        if i not in seen:
            series,diagnostics=paired_path(c,{})
            row=path_features(c,series,diagnostics); row['candidate_id']=i; features.append(row)
    features.sort(key=lambda r:r['candidate_id'])
    write_csv(out/'path_features.csv',features)
    legacy_summary=summarize(legacy); write_csv(out/'baseline_reproduction.csv',legacy_summary)
    prior=list(csv.DictReader((root/'backtest_mid_5c'/'summary.csv').open()))
    keyed={(int(r['calendar_dte']),float(r['target_credit']),r['exit_rule']):r for r in prior}
    differences=[]
    for r in legacy_summary:
        old=keyed[(r['calendar_dte'],r['target_credit'],r['exit_rule'])]
        for k,v in r.items():
            if k in ('calendar_dte','target_credit','exit_rule'): continue
            if v is None and not old[k]: continue
            if abs(v-float(old[k]))>1e-6: differences.append({'group':[r['calendar_dte'],r['target_credit'],r['exit_rule']],'metric':k,'old':old[k],'new':v})
    robustness=summarize_trades(trades); write_csv(out/'execution_robustness.csv',robustness)
    # Conditional interpretation is strictly development-only; no holdout rule selection.
    anatomy=[]
    for dte in (2,3):
        group=[r for r in development_only(features) if r['calendar_dte']==dte and r['target_credit']==6 and r['path_status']=='available']
        for label in ('clean_success','adverse_then_success','failure'):
            subset=[r for r in group if ('failure' if r['favorable_3_status']!='hit' else 'adverse_then_success' if r['target_3_max_before_hit'] > r['entry_mid']+.5 else 'clean_success')==label]
            def med(key):
                values=[r[key] for r in subset if r[key] is not None]
                return median(values) if values else None
            anatomy.append({'partition':'development','calendar_dte':dte,'target_credit':6,'target':3,'category':label,'count':len(subset),
                            'group_count':len(group),'severe_9_before_success_count':sum(r['target_3_deep_adverse_then_success'] for r in subset),'median_minutes_to_3':med('favorable_3_minutes'),
                            'median_max_before_3':med('target_3_max_before_hit'),'median_underwater_before_3':med('target_3_underwater_before_hit'),
                            'median_best_improvement_before_failure':med('target_3_best_improvement_before_failure')})
    write_csv(out/'failure_anatomy.csv',anatomy)
    unchanged=before==manifest(root)
    if not unchanged: raise ValueError('Raw inputs changed')
    report={'phase':2,'remote_requests':0,'raw_inputs_unchanged':unchanged,
        'counts':{'candidates':len(candidates),'feature_rows':len(features),'available_paths':sum(r['path_status']=='available' for r in features),'missing_paths':sum(r['path_status']=='missing' for r in features),'baseline_outcomes':len(legacy),'robustness_trade_evaluations':len(trades),'robustness_groups':len(robustness)},
        'baseline_reproduction':{'matches':not differences,'differences':differences,'groups':len(legacy_summary)},
        'path_quality':{'invalid_spread_minutes':sum(r['invalid_spread_minutes'] for r in features),'unpaired_minutes':sum(r['unpaired_minutes'] for r in features),'terminal_more_than_1m_before_expiry_close':sum(r['terminal_quote_age_minutes'] is not None and r['terminal_quote_age_minutes']>1 for r in features)},
        'assumptions':{'quantity':20,'fee_per_contract_side':1.324,'slippages':SLIPPAGES,'targets':TARGETS,'timezone':'America/New_York',
            'target_condition':'spread_mid + exit_slippage <= target; persistence executes at final confirmation minute',
            'next_minute':'Exactly 60 seconds after first executable touch; absent next minute falls back to terminal proxy and is flagged.',
            'drawdown':'Realized trade P/L in entry chronological order within each separate family, not a portfolio mark-to-market drawdown.',
            'terminal':'Last synchronized quote through expiration 16:00 ET; NOT settlement; no settlement data verified.',
            'underwater':'Observed consecutive 60-second intervals above entry midpoint; overnight and gaps excluded.',
            'anatomy':'Development only, midpoint $3 touch; clean means max debit <= entry midpoint + $0.50 before success. Adverse-then-success exceeds that level. Severe excursion ($9 before $3) is also counted.',
            'horizons':'As-of requested time, quote age <60 seconds; older observations have null value and explicit age.',
            'legacy':'Existing evaluate helper, session-scoped input; preserves original last-quote behavior solely for reproduction.',
            'holdout':'Unconditional prespecified execution sensitivity only; no conditional 2026 interpretation, rule selection, or validation claim.'},
        'development_anatomy':anatomy,
        'phase3_gaps':['Phase 3 local NDX/NQ/MNQ/VIX inventory and causal timestamp audit not performed (out of scope).','No verified PM settlement source; terminal quote outcomes remain provisional.','Two missing path sessions cannot be replenished under this no-acquisition assignment.'],
        'deferred_outputs':['entry_features.csv','candidate_timeline.csv','yearly_stability.csv','regime_breakdown.csv','matched_pairs.csv','partition_results.csv'],
        'baseline_summary':legacy_summary,'execution_robustness':robustness}
    dump(out/'report.json',report)
    lines=['# NDXP rich-credit research: Phases 1 and 2','',
           'Research only. Fixed rules; no acquisition, live changes, feature fitting, or holdout tuning. All dollar P/L uses 20 contracts and $1.324 per contract per side.','',
           f"Path store: {len(features):,} candidate rows, {report['counts']['available_paths']:,} usable paths, {report['counts']['missing_paths']} explicitly missing. Legacy baseline match: {not differences} across {len(legacy_summary)} groups.",'',
           '## Development-only $6 → $3 path anatomy','',
           '| DTE | Category | n / family n | Median elapsed minutes to $3 | Median max debit before $3 | Median observed underwater minutes |',
           '|---|---|---|---|---|---|']
    def fmt(value): return '—' if value is None else f'{value:,.2f}'
    for r in anatomy: lines.append(f"| {r['calendar_dte']} | {r['category']} | {r['count']} / {r['group_count']} | {fmt(r['median_minutes_to_3'])} | {fmt(r['median_max_before_3'])} | {fmt(r['median_underwater_before_3'])} |")
    lines += ['', 'Success here means a midpoint touch, not a guaranteed fill or positive net P/L. Clean success never exceeds entry midpoint + $0.50 before $3. Adverse-then-success exceeds that level; severe $9-before-$3 counts are separately recorded in failure_anatomy.csv. Elapsed timing includes nights/weekends; underwater minutes exclude unobserved intervals. Failures are right-censored at their last quote, not necessarily settlement failures.', '',
              '## Prespecified $6 → $3 execution sensitivity (all available dates)','',
              '| DTE | Adverse cents/side | Exit rule | n | Win rate | Target rate | Mean P/L | PF | Drawdown | Losing streak |',
              '|---|---|---|---|---|---|---|---|---|---|']
    for r in robustness:
        if r['target_credit']==6 and r['exit_target']==3:
            lines.append(f"| {r['calendar_dte']} | {r['slippage']*100:g} | {r['mode']} | {r['trades']} | {r['win_rate']:.1%} | {r['target_hit_rate']:.1%} | ${r['avg_pnl']:,.2f} | {fmt(r['profit_factor'])} | ${r['max_drawdown']:,.2f} | {r['longest_losing_streak']} |")
    for dte in (2,3):
        pick=lambda mode,slip: next(r for r in robustness if r['calendar_dte']==dte and r['target_credit']==6 and r['exit_target']==3 and r['mode']==mode and r['slippage']==slip)
        if any(r['calendar_dte']==dte and r['target_credit']==6 for r in robustness):
            touch,persist,nxt=pick('touch',.05),pick('persist_3',.05),pick('next_minute',.25)
            lines.append(f"\n{dte}DTE: mean P/L falls from ${touch['avg_pnl']:,.0f} on a five-cent touch to ${persist['avg_pnl']:,.0f} with three-minute persistence. At 25 cents and next-minute execution it is ${nxt['avg_pnl']:,.0f}. The apparent edge is highly sensitive to transient marks; midpoint-touch results alone are insufficient evidence of executable profit.")
    lines += ['', 'The full $5/$6/$7 × 2/3DTE × four targets × five slippages × four persistence modes is in execution_robustness.csv. These unconditional fixed sensitivities do not select a rule or constitute final-holdout validation.', '', '## Caveats and phase boundary','',
              'The two missing acquisition sessions are 2026-09-22 and 2026-09-23. Known degraded dates 2024-06-03 and 2025-10-22 are flagged both at entry and when crossed by a path; source is the governing specification because original warning logs were not found. Prices outside [0,10] and unmatched leg minutes are excluded and counted. No long-gap forward fills are used.', '',
              f"Path diagnostics count {report['path_quality']['invalid_spread_minutes']:,} out-of-bounds spread observations and {report['path_quality']['unpaired_minutes']:,} unmatched minutes across candidate paths (overlapping candidates can repeat observations); {report['path_quality']['terminal_more_than_1m_before_expiry_close']} candidates end more than one minute before expiration close.", '', 'Terminal prices are quote proxies, not PM settlement. Next-minute missing quotes fall back to that proxy and are counted. Drawdown is entry-ordered realized P/L per family, not portfolio mark-to-market risk. Repeated credits and overlapping trades are not independent observations.', '',
              'Phase 3 must inventory local underlying/volatility histories and audit causal availability. No acquisition is authorized. Settlement-source verification and the two missing paths remain data gaps. Entry, regime, matched-pair, validation and holdout-rule work are deferred.', '',
              '## Reproduce','', '```sh', 'PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_multidte_validation', 'PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_multidte_deep_dive', 'PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_ndxp_multidte*.py tests/unit/test_mgc_v05l_ndxp_naive*.py', '```', '', 'Both commands use local files only and verify raw SHA-256 manifests. Derived outputs are written exclusively under output/ndxp_multidte/deep_dive_v1/.']
    (out/'research_report.md').write_text('\n'.join(lines)+'\n')
    print(report['counts']); print(report['baseline_reproduction']); return report


def main():
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('--root',type=Path,default=Path('output/ndxp_multidte'))
    run(p.parse_args().root)

if __name__=='__main__': main()
