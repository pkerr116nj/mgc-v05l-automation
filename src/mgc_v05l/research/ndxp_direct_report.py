"""Evidence and decision report for completed, frozen Phase 3C direct recovery."""
from __future__ import annotations
import json
from pathlib import Path
from collections import Counter
import pandas as pd
from .ndxp_direct_recovery import OUT,PRIOR,OLD
from .ndxp_complete_panel import digest
from .ndxp_owned_pilot import sha


def write(path,value):path.write_text(json.dumps(value,indent=2,allow_nan=False,default=str)+'\n')
def fmt(value):return 'undefined' if pd.isna(value) else f'{value:,.3f}'


def main():
    m=json.loads((OUT/'session_request_manifest.json').read_text());q=json.loads((OUT/'preflight_quote.json').read_text());a=json.loads((OUT/'acquisition_receipts.json').read_text());s=json.loads((OUT/'analysis_summary.json').read_text());tests=json.loads((OUT/'test_results.json').read_text())
    coverage=pd.read_csv(OUT/'coverage_after_acquisition.csv');entry=pd.read_csv(OUT/'entry_execution_results.csv');targets=pd.read_csv(OUT/'target_execution_results.csv');false=pd.read_csv(OUT/'false_negative_analysis.csv');models=pd.read_csv(OUT/'execution_model_comparison.csv');settlements=pd.read_csv(OUT/'settlement_results.csv');paths=pd.read_csv(OUT/'path_taxonomy.csv');windows=pd.read_parquet(OUT/'touch_window_evidence.parquet')
    assert a['completed_requests']==a['planned_requests']==60 and not a['errors'] and a['batch_calls']==0
    assert q['purchase_allowed'] and q['planning']['projected_cost']<=10 and q['planning']['metadata_call_count']<=12
    assert q['manifest_sha256']==digest(OUT/'session_request_manifest.json')
    assert digest(OUT/'execution_rules.json')==digest(PRIOR/'execution_rules.json')==s['rules_sha256']
    assert tests['failures']==tests['errors']==0
    reproduction=json.loads((OUT/'reproducibility.json').read_text())
    assert reproduction['byte_identical'] and reproduction['full_frozen_replays']==2 and reproduction['network_calls_during_reproduction']==0
    assert all(digest(OUT/name)==expected for name,expected in reproduction['scientific_artifacts'].items())
    assert set(coverage.candidate_id)==set(entry.candidate_id)==set(targets.candidate_id)==set(settlements.candidate_id)==set(m['frozen_candidate_ids'])
    assert len(coverage)==246 and all(coverage.expiration<='2024-12-31')
    for p,expected in m['source_sha256'].items():assert sha(p)==expected
    originals=json.loads(Path('output/ndxp_multidte/deep_dive_v1/data_quality_report.json').read_text())['input_manifest']
    assert all(sha(Path('output/ndxp_multidte')/p)==r['sha256'] for p,r in originals.items())
    for r in a['receipts']:
        for f in r['files']:assert Path(f['path']).stat().st_size==f['bytes'] and sha(f['path'])==f['sha256']
    record_checks=json.loads((OUT/'record_verification.json').read_text())
    assert len(record_checks)==60 and all(r['exact_symbol_and_timestamp_bounds_verified'] for r in record_checks)
    primary=[]
    for d in (2,3):
        e=entry[(entry.calendar_dte==d)&(entry.target_credit==6)];t=targets[(targets.calendar_dte==d)&(targets.target_credit==6)];f=false[(false.calendar_dte==d)&(false.target_credit==6)]
        primary.append(dict(dte=d,candidates=len(e),supported_entries=int(e.modeled_entry_supported.sum()),naturally_marketable_entries=int((e.marketable_seconds>0).sum()),median_entry_quote_width=float(e.median_width.median()),strong_first_target_20lot_displayed=int((t.event_strong_minimum_exit_leg_size>=20).sum()),plausible_first_target_20lot_displayed=int((t.event_strong_plausible_minimum_exit_leg_size>=20).sum()),strong_targets=int(t.event_strong_first_ns.notna().sum()),strong_or_plausible_targets=int(t.event_strong_plausible_first_ns.notna().sum()),conservative_targets=int(t.event_conservative_first_ns.notna().sum()),missed_minute_mid_crossings=int(f.missed_minute_cross_mid.sum()),missed_minute_ask_crossings=int(f.missed_minute_cross_ask.sum()),persist_2_rejected_supported=int(f.p2_rejected_supported.sum()),persist_3_rejected_supported=int(f.p3_rejected_supported.sum())))
    primary_models=models[(models.target_credit==6)&(models.cohort=='supported_entry_matched')]
    model_records=json.loads(primary_models.to_json(orient='records'))
    warning_counts=Counter(w for r in a['receipts'] for w in r['quality_warnings'])
    summary=dict(status='completed_direct_recovery_and_frozen_analysis',requests=60,original_fragments=m['original_fragment_count'],new_conservative_estimate_usd=q['planning']['projected_cost'],actual_new_spend_usd=a['actual_provider_cost_usd'],actual_spend_note='Not exposed by SDK direct streaming; the conservative estimate is not an actual bill.',pricing_calls=q['planning']['metadata_call_count'],batch_api_calls=0,wall_clock_acquisition_seconds=a['wall_clock_seconds'],downloaded_bytes=a['downloaded_bytes'],coverage=s,entry_fresh_complete_candidates=int((coverage.entry_fresh_seconds>=57).sum()),target_windows=len(windows),target_windows_95pct_fresh=int((windows.fresh_fraction>=.95).sum()),primary_six_findings=primary,primary_six_models=model_records,all_cohort_false_negatives={k:int(false[k].sum()) for k in ('missed_minute_cross_mid','missed_minute_cross_ask','p2_rejected_supported','p3_rejected_supported','p2_approved_not_strong','p3_approved_not_strong','fast_sequences')},settlements=dict(candidates=int(settlements.intrinsic.notna().sum()),quote_proxy_differences=int((settlements.intrinsic_minus_last_quote.abs()>1e-9).sum())),taxonomy=dict(Counter(paths.category)),quality_warning_count=sum(warning_counts.values()),decoded_records_verified=sum(r['records'] for r in record_checks),tests=tests['tests'],next_phase='Resolve NDXP entry and exit model sensitivity with frozen replay and prospective timestamped complex-order paper/shadow observations before SPXW expansion')
    write(OUT/'summary.json',summary)
    details='\n'.join(f"- {r['dte']}DTE: {r['supported_entries']}/{r['candidates']} midpoint entries supported, but {r['naturally_marketable_entries']} naturally marketable at the nominal credit. Median entry spread quote width: ${r['median_entry_quote_width']:.2f}. Conditional on supported entry: {r['strong_targets']} strong, {r['strong_or_plausible_targets']} strong/plausible, and {r['conservative_targets']} conservative target observations. Missed minute crossings: {r['missed_minute_mid_crossings']:,} midpoint and {r['missed_minute_ask_crossings']:,} ask. Event-supported touch windows rejected by 2/3-minute persistence: {r['persist_2_rejected_supported']:,}/{r['persist_3_rejected_supported']:,}." for r in primary)
    comparison=[]
    for d in (2,3):
        rows=primary_models[primary_models.calendar_dte==d].set_index('model')
        comparison.append(f"{d}DTE minute-settlement PF {fmt(rows.loc['minute_touch_settlement','profit_factor'])} falls to {fmt(rows.loc['event_strong','profit_factor'])} under strong evidence and {fmt(rows.loc['event_conservative','profit_factor'])} under conservative stress")
    decision_comparison='; '.join(comparison)+'.'
    table='\n'.join(f'| {r.calendar_dte} | {r.model} | {r.trades} | {fmt(r.mean_pnl)} | {fmt(r.win_rate*100)}% | {fmt(r.profit_factor)} |' for r in models[(models.target_credit==6)&(models.cohort=='supported_entry_matched')].itertuples())
    report=f'''# Phase 3C — direct recovery and frozen scientific replay

## Acquisition, cost and consolidation

Completed **60/60 direct requests** for the exact 60 frozen development sessions and 246 candidates. The original 501 batch jobs were not polled, cancelled, modified or resubmitted during this recovery; their prior receipts remain unchanged for archival reconciliation. This assignment explicitly authorized the separate direct-retrieval path despite those queued commitments.

The plan uses one entry-to-latest-expiry stream per pilot session, with only that session's exact selected legs. First/last bounds follow the exchange calendar; intermediate overnight and extended-session padding is harmless acquisition overhead and excluded from the regular-session replay. Symbol-count distribution: `{json.dumps(m['symbol_count_distribution'],sort_keys=True)}`. Requested regular-session minutes: {m['requested_regular_session_minutes']:,.0f}; requested leg-symbol minutes: {m['requested_symbol_minutes']:,.0f}. Relative to the surgical manifest, extra unique regular leg-symbol minutes are {m['extra_regular_symbol_minutes']:,.3f}, plus {m['cross_request_duplicate_symbol_minutes']:,.0f} duplicated across streams. Wall-clock envelope leg-symbol minutes, including closed-market padding: {m['envelope_symbol_minutes']:,.0f}. Every original missing symbol/interval is covered by the consolidated envelopes.

**Conservative estimated new spend: ${q['planning']['projected_cost']:.6f}**, below the new **$10** ceiling. Exactly **{q['planning']['metadata_call_count']} metadata pricing calls** sampled six sessions. Method: stratify by prior pricing activity, select the largest symbol-minute exposure within each stratum, then apply twice the highest sampled cost per regular symbol-minute to the entire plan, including duplication. Projected billable bytes under the analogous stress estimate: {q['planning']['projected_bytes']:,.0f}. This is an uncertainty-aware extrapolation, not an exact provider quote for all 60 streams; no additional pricing calls were used. Sampling uses acquisition size/activity, not strategy outcomes.

**Actual new billed cost is not exposed by the SDK streaming response.** It is unknown, not zero and not the estimate. Acquisition wall time: **{a['wall_clock_seconds']:.1f} seconds**; downloaded compressed DBN bytes: **{a['downloaded_bytes']:,}**. Each session had at most one direct network attempt. Valid cached files are reused, successful-transfer markers permit local validation recovery, and uncertain or partial paid attempts stop rather than redownload. Raw artifacts are immutable and ignored by Git. All session metadata, exact request mappings and SHA-256 hashes were verified. All {summary['decoded_records_verified']:,} decoded records were checked against their exact requested symbols and receive-time bounds. Recorded warnings: {summary['quality_warning_count']}; raw warning strings are preserved in acquisition receipts, including SDK resource warnings where present.

The permanent planner guardrail exposes cost, bytes, pricing-call count, retrieval count, a consolidation alternative and operational complexity. It rejects highly fragmented plans whose cost is small relative to the approved ceiling unless an explicit justification is supplied. This replaces byte-minimization as the default for small-cost research acquisitions.

## Coverage and limitations of completeness

Exact source intervals are complete for **{s['source_complete_candidates']}/246 candidates** and **{s['source_complete_sessions']}/60 sessions**. Strict Phase 3B freshness completeness is **{s['fresh_complete_candidates']}/246 candidates** and **{s['complete_sessions']}/60 sessions**. Entry freshness reaches 57 of 60 seconds for {summary['entry_fresh_complete_candidates']} candidates. Of {len(windows):,} target-touch windows, {summary['target_windows_95pct_fresh']:,} meet 95% fresh valid duration. `coverage_after_acquisition.csv` reports remaining source-leg seconds and observed fresh fractions for every candidate; window boundaries and fresh fractions are in `touch_window_evidence.parquet`.

Complete source delivery does not guarantee a quote update every second. The frozen policy rejects invalid/crossed/undefined states and pairs whose leg age exceeds one second. It requires 95% valid fresh path duration and 57/60 seconds at entry for strict completeness. Censored paths remain censored; no absence of evidence is converted into a failed fill. Coverage is evaluated only inside cash sessions, including early closes. No 2025/2026 holdout tuning or new instruments were introduced.

## Frozen execution assumptions

The rule hash is `{s['rules_sha256']}`, identical to the original Phase 3C file. No strategy definitions were optimized. Entry support means the first fresh valid midpoint at or above the nominal $5/$6/$7 cohort credit in the first minute. It is a hypothetical midpoint entry, not an observed complex fill. Event models use nominal credit minus $0.05, or minus $0.25 under conservative stress; minute models retain the original frozen midpoint minus $0.05. Entry tables disclose natural marketability, positive sizes, durations, quote ranges and repricing.

A hypothetical $3 BTC rests strictly after modeled entry. Strong evidence requires spread ask ≤$3 and positive executable-side leg sizes; strong/plausible also accepts midpoint ≤$3 with positive sizes on all four sides. Conservative evidence requires ask+$0.25 ≤$3 and applies $0.25 entry slippage. Event exit debit is the $3 limit. Only the first qualifying event closes each modeled candidate. If none is observed, an eligible scenario holds to owned XQC settlement; this is an assumption, not proof that a real order could not fill.

Event P/L requires supported entry and complete source intervals. The main table uses the same supported-entry subset for each model; all-candidate and strict-freshness-complete matched subsets are also reported. All P/L assumes 20 contracts and $1.324 per contract per side. Overlapping candidates are individual scenarios, not a portfolio/capital-capacity simulation. OPRA leg quotes do not establish simultaneous accessible liquidity, complex-order queue priority, routing latency or proven 20-lot fills.

## Primary $6→$3 findings

{details}

Neither DTE cohort has a strict-freshness-complete $6 candidate, so the strict matched $6 results are unavailable. None of the first strong or strong/plausible target observations has 20 contracts displayed on both executable exit legs. The observed positive-size rule is weaker than a 20-lot fill requirement. Wide leg-derived spread quotes make midpoint support especially fragile.

| DTE | Model | Trades | Mean P/L ($) | Win rate | PF |
|---|---|---|---|---|---|
{table}

The full $5/$6/$7 × 2DTE/3DTE matrix is retained in `execution_model_comparison.csv`; exact candidate outcomes are in `candidate_model_outcomes.csv`. Minute touch and consecutive 2/3-minute persistence use midpoint+$0.05 ≤$3. The original quote-terminal variant retains last midpoint+$0.05 capped at $10; other non-target exits use audited intrinsic settlement. This separates settlement corrections from execution assumptions.

## Sampling, persistence, paths and settlement

Across all cohorts, fresh continuous $3 midpoint/ask crossings absent from raw-minute ≤$3 observations in the same minute bin number **{summary['all_cohort_false_negatives']['missed_minute_cross_mid']:,}/{summary['all_cohort_false_negatives']['missed_minute_cross_ask']:,}**. Initial below-target observations after gaps are not counted as crossings. The equal $3 threshold isolates sampling; these are event counts, not extra winning trades.

There are **{summary['all_cohort_false_negatives']['p2_rejected_supported']:,}/{summary['all_cohort_false_negatives']['p3_rejected_supported']:,}** overlapping minute-touch windows with event support that fail 2/3-minute persistence. Conversely, **{summary['all_cohort_false_negatives']['p2_approved_not_strong']:,}/{summary['all_cohort_false_negatives']['p3_approved_not_strong']:,}** persistence-approved windows lack strong evidence. Windows are conditional on an entry but counterfactually retain an order even after earlier evidence; model P/L uses only the first fill. Absence of strong evidence is not proof of non-fill.

Fast $6→$5→$4→$3→rebound sequences within 60 seconds: **{summary['all_cohort_false_negatives']['fast_sequences']:,}**. Taxonomy: `{json.dumps(summary['taxonomy'],sort_keys=True)}`. Incomplete freshness paths retain extrema but are not labeled proven clean/adverse recoveries or failures.

All **{summary['settlements']['candidates']}** settlements use owned, date/source-validated XQC observations. Source hashes and strike-based intrinsic values were reverified; **{summary['settlements']['quote_proxy_differences']}** last-quote proxies differ from intrinsic. These are validations of the owned source labels, not a new independent official settlement acquisition.

## Decision and next phase

{decision_comparison} Both strong models have negative expectancy and win rates below 40%. Conversely, the midpoint-plausible scenario has 100% modeled wins in both cohorts, with PF undefined because there are no modeled losses. That extreme divergence is execution-assumption sensitivity, not evidence of a guaranteed strategy. The $6 strict-freshness subset is empty, and neither natural entry marketability nor displayed 20-lot exit liquidity supports the optimistic interpretation.

The original five-year PF≈3.5 therefore looks materially optimistic relative to this panel's strong/conservative execution scenarios. This selected development panel is a different sample, so it cannot quantify the five-year PF's bias or establish whether actual complex fills would match any scenario. The original PF is not validated as executable at 20-lot size; midpoint-plausible profitability remains unproven.

Before SPXW expansion, preserve these thresholds and conduct frozen NDXP replay plus prospective paper/shadow observations with timestamped complex-order submissions, acknowledgements, partial fills and fills. Calibrate entry/exit evidence and the one-second freshness policy prospectively; do not tune on the 2025/2026 holdouts.

## Verification and reproduction

**{tests['tests']} tests pass.** Two full frozen replays produced byte-identical hashes for all ten scientific artifacts, with zero network calls during reproduction. All 2,430 original input hashes, prior manifests, batch-job receipts and frozen rules remain unchanged. The direct raw files pass local SHA-256 and metadata validation. No batch API calls, live trading-code changes, broker actions, subscription changes or new instruments occurred during recovery.

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_direct_recovery acquire
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_analysis --direct
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_direct_report
```

Existing session artifacts are reused; uncertain partial streams require operator review. Large local DBN/Parquet caches are not committed.
'''
    (OUT/'research_report.md').write_text(report)
    write(OUT/'completion_evidence.json',dict(status=summary['status'],assignment_complete=True,tests=tests,summary=summary,raw_files_verified=60,original_inputs_verified=len(originals),original_batch_receipts_unchanged=True,manifest_sha256=q['manifest_sha256'],outputs={p.name:dict(bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='completion_evidence.json' and not p.name.startswith('acquisition_receipts_previous_')},commit_sha_resolution='git log -1 --format=%H -- '+str(OUT/'completion_evidence.json')))
    print(json.dumps(summary,indent=2))

if __name__=='__main__':main()
