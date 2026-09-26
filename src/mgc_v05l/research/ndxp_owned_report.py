"""Render Phase 3B decisions and independently check compact output invariants."""
from __future__ import annotations
import argparse,csv,json
from pathlib import Path
from collections import Counter
import pandas as pd
from .ndxp_owned_pilot import OUT,BASE,sha,writejson


def main():
    p=argparse.ArgumentParser();p.add_argument('--test-count',type=int,required=True);p.add_argument('--integrity',default='/tmp/phase3b_integrity.json');args=p.parse_args()
    s=json.loads((OUT/'summary.json').read_text());manifest=json.loads((OUT/'pilot_manifest.json').read_text())
    coverage=list(csv.DictReader((OUT/'coverage_matrix.csv').open()));inventory=json.loads((OUT/'store_inventory.json').read_text())
    settlement=list(csv.DictReader((OUT/'settlement_audit.csv').open()));entries=pd.read_csv(OUT/'entry_microstructure.csv')
    target=pd.read_csv(OUT/'target_microstructure.csv');fn=pd.read_csv(OUT/'false_negative_analysis.csv')
    touches=pd.read_parquet(OUT/'touch_window_evidence.parquet');seconds=pd.read_parquet(OUT/'second_aggregates.parquet')
    test_results=json.loads((OUT/'test_results.json').read_text())
    assert test_results['tests']==args.test_count and test_results['failures']==test_results['errors']==0
    checks={
      'all_frozen_candidate_ids_once':sorted(int(r['candidate_id']) for r in coverage)==list(range(3738)),
      'sixty_diagnostic_sessions':len(manifest['sessions'])==60,
      'both_dte_all_credit_cohorts':{(int(r['calendar_dte']),float(r['target_credit'])) for r in manifest['candidates']}=={(d,k) for d in (2,3) for k in (5.,6.,7.)},
      'development_entries_and_expirations_only':all('2023-03-28'<=r['session_date']<=r['expiration']<='2024-12-31' for r in manifest['candidates']),
      'outputs_preserve_selected_ids':set(entries.candidate_id)==set(manifest['candidate_ids'])==set(target.candidate_id)==set(fn.candidate_id),
      'class_counts_reconcile':sum(s['classes'].values())==sum(s['conditional_classes'].values())==len(touches)==s['minute_touches'],
      'second_duration_bounded':bool(((seconds.fresh_seconds>=0)&(seconds.fresh_seconds<=1+1e-8)).all()),
      'frozen_manifest_and_rules_unchanged':all(sha(OUT/k)==v for k,v in s['frozen_before_outcomes_sha256'].items()),
      'intrinsic_bounded':all(0<=float(r['intrinsic'])<=10 for r in settlement if r['intrinsic']),
      'no_network_calls':s['network_calls']==s['download_calls']==0,
    }
    unchanged=[]
    for h in inventory:
        if 'mtime_ns' not in h:continue
        st=Path(h['path']).stat();unchanged.append(st.st_size==h['bytes'] and st.st_mtime_ns==h['mtime_ns'])
    checks['owned_dbn_sizes_mtimes_unchanged']=all(unchanged)
    original=json.loads((BASE/'deep_dive_v1/data_quality_report.json').read_text())['input_manifest']
    failures=[name for name,r in original.items() if (BASE/name).stat().st_size!=r['bytes'] or sha(BASE/name)!=r['sha256']]
    integrity=dict(files_checked=len(original),failures=failures,full_sha256_unchanged=not failures)
    checks['original_raw_full_sha256_unchanged']=integrity['full_sha256_unchanged']
    assert all(checks.values()),checks
    six=target[target.target_credit==6];six_fn=fn[fn.target_credit==6]
    conditional=s['conditional_classes'];cost=s['next_acquisition']
    table='\n'.join(f"| {r['calendar_dte']} | ${r['target_credit']:g} | {r['candidates']} | {r['minute_touches']} | {r['strong fill evidence']} | {r['plausible fill evidence']} | {r['ambiguous']} | {r['unlikely fill']} |" for r in s['conditional_by_dte_credit'])
    percentages={k:round(v/s['minute_touches']*100,3) for k,v in conditional.items()}
    intersecting_dates=sorted({r['session_date'] for r in coverage if r['entry_supported']=='True'})
    overlap_candidates=int((target.minute_touches_any_fresh_events>0).sum())
    sources=json.loads((OUT/'settlement_sources.json').read_text())
    primary_false={k:int(six_fn[k].sum()) for k in six_fn.columns if k.startswith('conditional_')}
    confirmed_missing=s['conditional_false_negatives']['conditional_missed_raw_minute_mid_crossings']
    report=f'''# Phase 3B — owned-data execution pilot

## Decision

**PF ≈3.5 remains unresolved.** The maximum defensible fully covered execution cohort is **0 of 60 requested sessions**, a shortfall of 60. A separate, deterministic **60-session diagnostic cohort / {s['pilot_candidates']} frozen candidates** uses the partial owned evidence. None of these candidates has a supported modeled entry or a complete replay. Missing data are not evidence of non-fill, and persistence failure is not evidence of non-fill.

Conditional exit quotes do support brief compression in some observed fragments. They do not establish whether the original entry filled, whether a resting complex order had queue priority, or whether 20 contracts could execute. This is Level 2/partial Level 3 evidence, not a validated strategy or realized trade count.

## Coverage and selection

Indexed {s['indexed_files']:,} CMBP-related files by DBN headers, date-scoped instrument mappings and exact OSI symbols. The scan attempted 6,195 relevant decodes; {s['decoded_files']:,} files yielded exact-contract events inside candidate windows. Quarantined files and decoding errors are listed in `store_inventory.json` and excluded. The coverage matrix retains all {s['coverage_candidates']:,} original IDs, including 2025/2026 for coverage only; {s['owned_pair_candidates']:,} have some observations of both legs. No candidate has 57 of 60 seconds of fresh paired entry coverage.

Full-chain opening acquisitions generally stop **at 09:31 exclusive**, the frozen entry time. Many later stores contain same-day-expiring contracts, which can overlap our candidates on expiry day, but do not cover their earlier entry. A symbol appearing in a header is never treated as proof of actual observations. `candidate_store_coverage.parquet` gives per-candidate, per-leg, per-store event counts and timestamps; absence means zero actual matching events. Header memberships, including stores with no observations, remain in `store_inventory.json`. Repeated timestamps across overlapping stores are disclosed separately from raw event counts.

The pilot is month-stratified from March 2023 through December 2024, with farthest-date spacing, and retains available $5/$6/$7 and 2DTE/3DTE candidates. Selection uses only dates and timestamp coverage, not prices or P/L. Candidates expiring after 2024-12-31 are excluded. The manifest and machine-readable classification rules were written and hashed before the final outcome pass; hashes were checked afterward. All {s['pilot_candidates']} paths remain censored. No selection or threshold was optimized from these results.

{overlap_candidates} candidates have some event overlap with a target-touch window. Across all cohorts, **{s['minute_touches_any_fresh_events']:,}/{s['minute_touches']:,} ({100*s['minute_touches_any_fresh_events']/s['minute_touches']:.2f}%)** baseline minute-touch windows contain fresh valid synchronized event observations. This is fragment coverage, not continuous path coverage. `minute_window_coverage.parquet` also records each crossing into raw midpoint $3, baseline $2.95, and adverse entry+$0.50/$7/$8/$9, including candidates outside the pilot without computing their execution outcomes.

## Entry and exit evidence

The frozen baseline STO remains entry midpoint minus $0.05 at the original timestamp. It is a hypothetical fill assumption. The separate entry-support diagnostic searches the next 60 seconds for a fresh midpoint at the nominal cohort credit; it found zero modeled fills, so it changes no baseline trades. Entry tables retain first/last/median midpoint, bid/ask range, marketable/plausible durations, crossings, quote width, updates, and +5/15/30/60/120/300-second observations. Blank repricing values mean unavailable, never zero.

Receive-time synchronization uses the last observed state of each exact leg, requires both legs to be at most one second old, and caps duration at the next update or either leg's freshness deadline. Missing, stale, invalid, locked and crossed conditions are retained. Raw selected-leg events remain in the disposable local cache; compact crossing witnesses retain prices, sizes and trade context, and `second_aggregates.parquet` retains observed extrema and valid durations. Quotes do not persist across long gaps. Windows are conservative about a state first observed before their left boundary, so a boundary fragment can be undercounted by up to one second. Same-timestamp updates use the last receive/event-sorted update per leg. Single-leg prints do not prove a spread fill.

Under the **supported-entry replay**, all {s['minute_touches']:,} minute touches are ambiguous: strong 0%, plausible 0%, ambiguous 100%, unlikely 0%.

Under the **separate conditional exit diagnostic**, assume the frozen baseline entry filled and the BTC order rested immediately afterward. Every touch window is evaluated as a potential resting-order opportunity, conditional on the order still being unfilled. Multiple windows can belong to the same candidate and must not be counted as separate executed trades. Classes are:

- Strong: {conditional['strong fill evidence']:,} ({percentages['strong fill evidence']}% of all minute touches).
- Plausible: {conditional['plausible fill evidence']:,} ({percentages['plausible fill evidence']}%).
- Ambiguous: {conditional['ambiguous']:,} ({percentages['ambiguous']}%), predominantly uncovered.
- Unlikely: {conditional['unlikely fill']:,} ({percentages['unlikely fill']}%).

Observed windows are heavily selected by the coverage of the earlier same-day-expiry studies and overlap in time. Their class frequencies must not be extrapolated to uncovered entries, paths, or the whole baseline.

The strong class is a sufficient legged-quote diagnostic (spread ask ≤$3 and positive executable-side sizes), not the primary complex-order price model. The plausible class uses midpoint ≤$3 and positive quoted sizes. Neither proves queue priority or a 20-lot fill. Unlikely requires ≥95% fresh window coverage and no bid/mid/ask crossing. No coverage cannot qualify as unlikely.

| DTE | Credit | Candidates | Minute touches | Conditional strong | Conditional plausible | Ambiguous | Unlikely |
|---|---|---|---|---|---|---|---|
{table}

For the primary **$6→$3** family: {len(six)} candidates, {int(six.minute_touches.sum()):,} minute touches; conditional strong {int(six['conditional_strong fill evidence'].sum()):,}, plausible {int(six['conditional_plausible fill evidence'].sum()):,}, and ambiguous {int(six['conditional_ambiguous'].sum()):,}. There are zero complete entry-to-exit observations.

## False negatives and persistence

Conditional on the baseline entry, **{confirmed_missing:,} confirmed event-level midpoint crossings** occur in minute bins with no raw midpoint ≤$3 observation. A confirmed crossing requires a continuous fresh previous state above $3; a quote first appearing below $3 after a gap is not counted. This measures sampling mismatches in covered fragments, not incremental winning trades or an unbiased miss rate. A separate baseline comparator uses midpoint+$0.05 ≤$3 and is labeled `conditional_missed_minute_mid_crossings`; it mixes sampling and the five-cent threshold difference and must not be interpreted as a pure sampling effect.

{ s['conditional_false_negatives']['conditional_p2_rejected_supported']:,} minute touches rejected by the forward 2-minute persistence test nevertheless have strong/plausible conditional evidence; the 3-minute count is {s['conditional_false_negatives']['conditional_p3_rejected_supported']:,}. These tests are per-touch forward runs, not the candidate's eventual first persistence exit. Conversely, {s['conditional_false_negatives']['conditional_p2_approved_not_strong']:,} / {s['conditional_false_negatives']['conditional_p3_approved_not_strong']:,} persistence-approved touches lack strong evidence, chiefly because windows are missing; those counts are not failures to fill. For $6 only, the corresponding detailed counts are `{json.dumps(primary_false,sort_keys=True)}`.

Fast $6→$5→$4→$3→rebound sequences use a 60-second limit, allow a single update to sweep several levels, and reset at stale/invalid gaps. Conditional count: {s['conditional_false_negatives'].get('conditional_fast_6_5_4_3_rebound_sequences',0):,}. They are listed in `fast_sequences.parquet`. The supported-entry replay cannot test these quantities because no entry is supported; zeros in its columns mean untestable here, not rejection of the hypothesis.

The path taxonomy is **{s['pilot_candidates']} censored/insufficient coverage**. Available event timing and extrema are retained, but none is promoted to clean compression, adverse recovery, severe recovery, or failure on an incomplete path. The completeness denominator conservatively uses 390 minutes per observed trading date, including shortened sessions; it cannot create false complete paths in this sample.

## Settlement audit

All {s['settlements']['covered']} pilot expirations join owned XQC values with recognized source tags, finite positive values and no conflicting duplicates. Uncovered pilot expiry dates: {s['settlements']['missing_expirations']}. The owned downloader's implementation explicitly requests FRED's `NASDAQXQC` series and writes the observed date and value without forward-fill; its source path is recorded in completion evidence. The {len(sources)} owned settlement files are hashed. This verifies local provenance and joins, not independent re-fetching of official daily values.

Put-vertical intrinsic is max(short strike − XQC, 0) − max(long strike − XQC, 0), bounded to $0–$10. **{s['settlements']['nonzero_quote_intrinsic_differences']}** candidates differ from their last-quote proxy; maximum absolute difference is **${s['settlements']['max_abs_difference']:g}**. Quote timestamp and age remain visible. No last quote silently replaces missing settlement. These differences concern hypothetical expiration outcomes, not a recalculated target-exit PF.

## Remaining gaps and minimum next acquisition

The missing foundation is post-09:31 entry coverage and the exact selected contracts' intervening trading sessions through expiry. Target-only snippets cannot resolve excursions missed entirely by minute sampling. The minimum proposed complete panel therefore uses only the pilot's exact symbols and regular-session intervals, removing already-owned header intervals. This is minimal for this fixed 60-session panel and coverage rule, not a claim that 60 sessions is a statistically sufficient experiment.

`minimum_acquisition_manifest.json` lists **{cost['requests']} explicit requests**, each at most 24 symbols, with timezone-aware bounds; no request was submitted. Total missing symbol-minutes: {cost['symbol_minutes']:,.2f}; owned symbol-minutes reused: {cost['owned_symbol_minutes_reused']:,.2f}. Existing owned intervals are not repurchased. No full-chain or full-market acquisition is proposed. Minute-calendar session bounds conservatively extend to 16:00 on half-days and should be shortened against the exchange calendar before authorization.

The saved Phase 3A provider quote was $0.001112389565 / 7,465,120 billable bytes for six exact NDXP legs over 30 minutes on 2024-06-12. Linear scaling gives **${cost['estimated_usd_central']:.2f} central**, with a deliberately broad **${cost['estimated_usd_low']:.2f}–${cost['estimated_usd_high']:.2f} planning envelope** (0.25×–10×), and {cost['estimated_billable_gib_central']:.2f} billable GiB central. Billable bytes are not compressed disk bytes. This is a sample-based estimate, not a binding price cap; differing quote intensity and provider rounding can change it. A small bounded metadata re-quote is required before any separately authorized purchase. No subscription upgrade is justified by this pilot.

No purchase, subscription change, broker action or live-code modification occurred. The operator decision is whether to authorize a separately reviewed acquisition; until then, the execution/PF question remains blocked by coverage, not by a demonstrated absence of opportunity.

## Reproduction and verification

Python commands run from the repository root, using the existing environment:

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_owned_index --scan-root /Users/patrick/Dev/MGC-v05l-ndxp-research/outputs --headers /tmp/phase3b_headers.json --cache /tmp/phase3b_events
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_owned_pilot --cache /tmp/phase3b_events
PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_ndxp_owned_execution.py tests/unit/test_ndxp_multidte*.py tests/unit/test_mgc_v05l_ndxp_naive*.py tests/unit/test_index_options_phase3a.py
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_owned_report --test-count {args.test_count}
```

`--reuse-coverage` reruns the pilot outcome tables only after validating frozen input hashes, rules and owned-source size/mtime metadata. No command uses a data API. The extracted ~4.1 GiB event cache and full header cache are temporary, not committed. Compact outputs, research code and tests are committed.

**{args.test_count} tests passed.** All output invariants passed. Full SHA-256 checks confirm {integrity['files_checked']:,} original input files unchanged; size/mtime checks confirm {len(unchanged):,} indexed valid DBN sources unchanged. Evidence includes the manifest/rule hashes, output hashes and source checks. No 2025/2026 execution or conditional results were computed.
'''
    (OUT/'research_report.md').write_text(report)
    # Runtime code provenance, without invoking the acquisition function.
    downloader=Path('/Users/patrick/Dev/MGC-v05l-ndxp-research/src/mgc_v05l/research/ndxp_naive_data.py')
    evidence=dict(status='owned-data assignment completed; full execution inference censored by source coverage',base_commit='b3703c6abd',commit_sha_resolution='git log -1 --format=%H -- '+str(OUT/'completion_evidence.json'),tests=dict(passed=args.test_count,failed=0,command='PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_ndxp_owned_execution.py tests/unit/test_ndxp_multidte*.py tests/unit/test_mgc_v05l_ndxp_naive*.py tests/unit/test_index_options_phase3a.py'),checks=checks,original_input_integrity=integrity,owned_dbn_metadata_checked=len(unchanged),settlement_downloader=dict(path=str(downloader),sha256=sha(downloader),function='download_settlements',invoked=False),frozen_sha256=s['frozen_before_outcomes_sha256'],primary_six_conditional_counts=primary_false,summary=s,outputs={p.name:dict(bytes=p.stat().st_size,sha256=sha(p)) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='completion_evidence.json'},hard_constraints=dict(purchases=0,metadata_calls=0,subscription_changes=0,broker_actions=0,live_code_changes=0,holdout_execution_analysis=False),limitations=['No supported entry-to-exit cohort','Conditional quote fragments do not validate actual fills or profit factor','Settlement provenance verified locally; no independent official re-fetch','Acquisition estimate is not a price guarantee'])
    writejson(OUT/'completion_evidence.json',evidence);print(json.dumps(checks,indent=2))

if __name__=='__main__':main()
