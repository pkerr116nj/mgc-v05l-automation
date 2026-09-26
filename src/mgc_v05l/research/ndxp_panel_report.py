"""Phase 3C decision report and integrity evidence, generated from actual outputs."""
from __future__ import annotations
import json,csv
from pathlib import Path
from collections import Counter
import pandas as pd
from .ndxp_complete_panel import OUT,OLD,digest
from .ndxp_owned_pilot import sha


def write(p,value):p.write_text(json.dumps(value,indent=2,allow_nan=False,default=str)+'\n')
def number(v):return 'undefined' if pd.isna(v) else f'{v:,.3f}'


def main():
    manifest=json.loads((OUT/'final_request_manifest.json').read_text());quote=json.loads((OUT/'preflight_quote.json').read_text());receipts=json.loads((OUT/'acquisition_receipts.json').read_text());analysis=json.loads((OUT/'analysis_summary.json').read_text())
    test=json.loads((OUT/'test_results.json').read_text());coverage=pd.read_csv(OUT/'coverage_after_acquisition.csv');entry=pd.read_csv(OUT/'entry_execution_results.csv');target=pd.read_csv(OUT/'target_execution_results.csv');false=pd.read_csv(OUT/'false_negative_analysis.csv');settlements=pd.read_csv(OUT/'settlement_results.csv');models=pd.read_csv(OUT/'execution_model_comparison.csv');paths=pd.read_csv(OUT/'path_taxonomy.csv')
    assert quote['purchase_allowed'] and quote['estimated_cost_usd']<=10
    assert receipts['completed_requests']==receipts['planned_requests']==len(manifest['requests'])
    assert receipts['actual_cost_complete'] and receipts['actual_provider_cost_usd']<=10
    ids=set(manifest['frozen_candidate_ids'])
    assert set(coverage.candidate_id)==set(entry.candidate_id)==set(target.candidate_id)==set(settlements.candidate_id)==ids
    assert len(coverage)==len(ids)==246 and len(manifest['sessions'])==60
    assert all(coverage.expiration<='2024-12-31') and all(coverage.session_date>='2023-03-28')
    assert digest(OUT/'final_request_manifest.json')==quote['manifest_sha256']==json.loads((OUT/'manifest_digest.json').read_text())['sha256']
    assert digest(OUT/'execution_rules.json')==analysis['rules_sha256']
    assert test['failures']==test['errors']==0
    raw=[]
    for r in receipts['receipts']:
        for f in r['files']:
            p=Path(f['path']);assert p.stat().st_size==f['bytes'] and sha(p)==f['sha256'];raw.append(f)
    original=json.loads(Path('output/ndxp_multidte/deep_dive_v1/data_quality_report.json').read_text())['input_manifest']
    original_ok=all(sha(Path('output/ndxp_multidte')/p)==v['sha256'] for p,v in original.items())
    assert original_ok
    for path,expected in manifest['source_sha256'].items():assert sha(path)==expected
    six=entry[entry.target_credit==6];six_target=target[target.target_credit==6];six_false=false[false.target_credit==6]
    six_findings=[]
    for d in (2,3):
        en=six[six.calendar_dte==d];ta=six_target[six_target.calendar_dte==d];fa=six_false[six_false.calendar_dte==d]
        six_findings.append(dict(dte=d,candidates=len(en),supported_entries=int(en.modeled_entry_supported.sum()),strong_resting_targets=int(ta.event_strong_first_ns.notna().sum()),strong_or_plausible_resting_targets=int(ta.event_strong_plausible_first_ns.notna().sum()),conservative_targets=int(ta.event_conservative_first_ns.notna().sum()),missed_minute_mid_crossings=int(fa.missed_minute_cross_mid.sum()),persistence_2_rejected_supported=int(fa.p2_rejected_supported.sum()),persistence_3_rejected_supported=int(fa.p3_rejected_supported.sum())))
    summary=dict(status='complete_owned_and_acquired_panel_analysis',fresh_quoted_usd=quote['estimated_cost_usd'],fresh_quoted_billable_bytes=quote['billable_bytes'],pricing_metadata_calls=quote['metadata_calls_total'],actual_provider_cost_usd=receipts['actual_provider_cost_usd'],purchased_requests=len(receipts['receipts']),raw_files=len(raw),raw_bytes=sum(r['bytes'] for r in raw),coverage=analysis,primary_six_findings=six_findings,all_cohort_false_negatives={k:int(false[k].sum()) for k in ('missed_minute_cross_mid','missed_minute_cross_ask','p2_rejected_supported','p3_rejected_supported','p2_approved_not_strong','p3_approved_not_strong','fast_sequences')},settlements=dict(covered=int(settlements.intrinsic.notna().sum()),quote_proxy_differences=int((settlements.intrinsic_minus_last_quote.abs()>1e-9).sum())),taxonomy=dict(Counter(paths.category)),pf_assessment='not established as representative of executable 20-lot complex orders; compare matched development panel under explicit evidence models',next_phase='NDXP deterministic replay and prospective complex-order paper/shadow observations; resolve freshness and fill-model sensitivity before SPXW expansion',tests=test['tests'])
    write(OUT/'summary.json',summary)
    table=[]
    for r in models[(models.target_credit==6)&(models.cohort=='supported_entry_matched')].itertuples():table.append(f'| {r.calendar_dte} | {r.model} | {r.trades} | {number(r.mean_pnl)} | {number(r.win_rate*100)}% | {number(r.profit_factor)} |')
    details='\n'.join(f"- {r['dte']}DTE: {r['supported_entries']}/{r['candidates']} entries supported; strong targets {r['strong_resting_targets']}, strong/plausible {r['strong_or_plausible_resting_targets']}, conservative {r['conservative_targets']}; {r['missed_minute_mid_crossings']:,} event midpoint crossings absent from raw-minute $3 observations; {r['persistence_2_rejected_supported']}/{r['persistence_3_rejected_supported']} 2/3-minute persistence rejections with supporting target-window evidence." for r in six_findings)
    report=f'''# Phase 3C — complete frozen NDXP event panel

## Acquisition and coverage

The immutable manifest contains {len(manifest['requests'])} exact-symbol CMBP-1 requests for the original 60 development sessions and 246 frozen candidates. Two calendar corrections clipped early cash-session closes; requests were merged/subtracted without broadening the original regular-session panel. The calendar is exchange_calendars {manifest['calendar']['version']} XNYS, checked against Nasdaq's [2023](https://www.nasdaqtrader.com/content/technicalsupport/2023tradingcalendar.pdf) and [2024](https://www.nasdaqtrader.com/content/technicalsupport/2024tradingcalendar.pdf) calendars. Extended-close trading was not added. Manifest SHA-256: `{quote['manifest_sha256']}`.

Fresh provider quote: **${quote['estimated_cost_usd']:.12f}**, **{quote['billable_bytes']:,} billable bytes**, from {quote['metadata_calls_total']} pricing metadata calls including 17 bounded timeout retries, four workers. It passed the authorized $10 ceiling. Exact small-window quotes can over-report because of provider aggregation; the [provider documents this limitation](https://databento.com/docs/api-reference-historical?historical=http).

Actual completed provider batch charges: **${receipts['actual_provider_cost_usd']:.12f}**. {receipts['completed_requests']} requests completed; {len(raw)} raw/support files occupy {summary['raw_bytes']:,} bytes. These charges are the provider's `cost_usd`, not a claim about cash charged to a card after account credits. Raw files are stored locally, ignored by Git, and verified against provider SHA-256 hashes. Compact receipts preserve job IDs, timestamps, billed sizes, costs and warnings. Credentials and signed URLs are excluded.

The first submission pass encountered submission errors consistent with the documented rate limit; the original handler retained exception types rather than HTTP status, so not every rejection is independently classified. Confirmed provider jobs were recovered and reused; uncertain intents were reconciled against two provider job snapshots before any resubmission. Subsequent submissions were paced below the [20-per-minute batch limit](https://databento.com/docs/reference-historical/basics/). Batch downloads can be retried without another purchase under the [provider's billing rules](https://databento.com/docs/faqs/usage-pricing-and-data-credits). Completed or checksum-valid partial files are reused without redownload. There were no streaming repurchases.

**Source intervals complete:** {analysis['source_complete_candidates']}/246 candidates and {analysis['source_complete_sessions']}/60 sessions. **Phase 3B freshness-complete:** {analysis['fresh_complete_candidates']}/246 candidates and {analysis['complete_sessions']}/60 sessions. These are different tests: buying the complete feed does not make a leg update every second. The strict test requires both source intervals, at least 95% fresh valid paired duration, and at least 57/60 fresh seconds at entry. Invalid, crossed, stale and unmatched states remain excluded. The coverage CSV reports exact missing source-leg seconds and freshness fractions; no missing observations are fabricated.

## Frozen execution assumptions

The event rules were saved before reading purchased outcomes (`execution_rules.json`, SHA-256 `{analysis['rules_sha256']}`). Candidates, nominal credits, widths, DTEs and dates remain frozen; no 2025/2026 conditional analysis or strategy tuning occurred.

Entry follows Phase 3B's diagnostic: first fresh valid midpoint at or above nominal $5/$6/$7 in the first 60 seconds. This is hypothetical complex-midpoint support, not a proven entry. Event-model credit is nominal credit less $0.05, or less $0.25 for the conservative model. Entry tables separately show natural marketability, plausible midpoint durations, sizes, quote ranges and immediate repricing. Minute baselines retain their original frozen midpoint-minus-$0.05 entry, so their exact modeled entry price and timing can differ. Prices and timestamps are preserved in candidate-level outcomes.

A $3 BTC rests strictly after the modeled entry. Each event model exits at its first qualifying event, once per candidate: strong requires spread ask ≤$3 with positive executable-side sizes; strong/plausible additionally accepts fresh midpoint ≤$3 with positive sizes on all sides. Conservative requires ask+$0.25 ≤$3 and uses $0.25 entry slippage. Modeled event exit debit is the $3 limit. These are evidence models, not complex-book queue models. Same-timestamp states use the last receive/event-sorted update per leg; nanoseconds remain integers. Leg freshness is at most one second, with no long-gap carry.

Where no qualifying event is observed, an eligible event scenario holds to owned XQC intrinsic settlement. This is an explicit modeling assumption; it does not establish that a real order could not have filled. Event scenarios require supported entry and complete source intervals. Strict freshness-complete matched results are reported separately, and incomplete paths retain censored taxonomy labels. All P/L uses 20 contracts and $1.324 per contract per side. Overlapping candidates are evaluated individually; this is not a portfolio or capital-capacity simulation.

## Primary $6→$3 findings

{details}

The following comparison uses the **same supported-entry subset** for each DTE. The full $5/$6/$7 × 2DTE/3DTE matrix, all-candidate comparison, and strict-complete matched subset are in `execution_model_comparison.csv`.

| DTE | Model | Trades | Mean P/L ($) | Win rate | PF |
|---|---|---|---|---|---|
{chr(10).join(table)}

`minute_touch_original_quote_terminal` preserves the original last-quote-plus-$0.05 terminal proxy, bounded at $10. Other minute models substitute audited intrinsic settlement when the target does not fire; 2/3-minute persistence uses the frozen consecutive-minute rule. Reporting both terminal variants avoids attributing a settlement correction to execution microstructure.

## False negatives, paths and settlement

Across all cohorts, {summary['all_cohort_false_negatives']['missed_minute_cross_mid']:,} fresh continuous event midpoint crossings of $3 have no raw-minute ≤$3 observation in the same minute bin, and {summary['all_cohort_false_negatives']['missed_minute_cross_ask']:,} ask crossings do so. Initial quotes below $3 after gaps are not counted as crossings. These counts describe sampling mismatches, not additional winning trades. The exact same $3 threshold is used on both sides of this comparison.

There are {summary['all_cohort_false_negatives']['p2_rejected_supported']:,}/{summary['all_cohort_false_negatives']['p3_rejected_supported']:,} minute touches rejected by 2/3-minute persistence but supported by fresh event evidence in the surrounding window. Conversely, {summary['all_cohort_false_negatives']['p2_approved_not_strong']:,}/{summary['all_cohort_false_negatives']['p3_approved_not_strong']:,} persistence-approved windows lack strong evidence; absence is not automatically non-fill. These are overlapping per-touch windows, conditional on an entry, with a counterfactual resting order in each window even after earlier supporting evidence; model P/L counts only the first fill per candidate.

Fast $6→$5→$4→$3→rebound sequences within 60 seconds: {summary['all_cohort_false_negatives']['fast_sequences']:,}. A run-compressed implementation is regression-tested against Phase 3B's state machine, including invalid/stale resets and a single event sweeping several levels. Taxonomy counts: `{json.dumps(summary['taxonomy'],sort_keys=True)}`. Censored paths retain observed extrema and timing but are not called complete recoveries or failures.

All {summary['settlements']['covered']} candidates have audited owned XQC settlement. The original source files were re-hashed and each intrinsic payoff recomputed from the frozen strikes. {summary['settlements']['quote_proxy_differences']} terminal quote proxies differ from intrinsic. Settlement P/L and original terminal-proxy P/L remain distinguishable. No new settlement data purchase or silent quote substitution occurred.

## Decision and next phase

The original five-year PF≈3.5 cannot be treated as a proven executable 20-lot result. This 60-session development panel is a different sample; the matched table above isolates explicit modeling choices more fairly than comparing its PF directly to the five-year aggregate. Differences between strong, plausible and conservative models quantify assumption sensitivity; strict freshness completeness and entry support limit the conclusions. **Representativeness of the original PF remains unproven**, even where quotes support a modeled resting exit.

OPRA leg quotes do not reveal complex-order queue position, whether displayed sizes were simultaneously accessible, spread routing, broker latency or actual 20-lot fills. A one-second leg-age policy is particularly consequential during fast moves. No model here turns an isolated quote into an observed complex execution.

The next step before SPXW expansion is a frozen NDXP replay and prospective paper/shadow observation protocol with timestamped complex-order submission, acknowledgements and fill/partial-fill records. Resolve whether the freshness exclusions reflect quiet-but-valid quotes or unusable evidence, and test entry/exit model calibration prospectively. Do not optimize thresholds on this panel or consume the 2025/2026 holdouts for that calibration.

## Verification and reproduction

{test['tests']} tests passed. Original 2,430 research inputs retain their full SHA-256 hashes; Phase 3B manifests and inventories remain unchanged. All acquired raw/support files pass provider checksum validation. No live code, broker actions, subscription changes, new instruments or holdout tuning occurred.

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_complete_panel manifest
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_acquire
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_analysis
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_report
```

The acquisition command enforces the fresh quote and manifest hash before submission, reuses provider jobs and raw files, and will refuse an expired quote if any new purchase is required. Retrieving already-submitted jobs does not require a new purchase or quote. Local analysis uses the selected-leg caches in `/tmp/phase3b_events` and `/tmp/phase3c_events`; the Phase 3B report describes rebuilding its cache from immutable owned DBNs. Large raw/event caches are not committed.
'''
    (OUT/'research_report.md').write_text(report)
    evidence=dict(status=summary['status'],tests=test,manifest_sha256=quote['manifest_sha256'],rules_sha256=analysis['rules_sha256'],raw_files_verified=len(raw),original_inputs_sha256_verified=len(original),original_inputs_unchanged=original_ok,summary=summary,outputs={p.name:dict(bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='completion_evidence.json'},commit_sha_resolution='git log -1 --format=%H -- '+str(OUT/'completion_evidence.json'))
    write(OUT/'completion_evidence.json',evidence)
    print(json.dumps(summary,indent=2))


def pending():
    """Record a provider-blocked attempt without manufacturing scientific results."""
    from datetime import datetime,timezone
    q=json.loads((OUT/'preflight_quote.json').read_text())
    a=json.loads((OUT/'acquisition_receipts.json').read_text())
    test=json.loads((OUT/'test_results.json').read_text())
    manifest=json.loads((OUT/'final_request_manifest.json').read_text())
    assert a['completed_requests']<a['planned_requests']
    jobs=[json.loads(p.read_text()) for p in sorted((OUT/'jobs').glob('*.json'))]
    assert len(jobs)==len(manifest['requests'])
    states=dict(Counter(j['state'] for j in a['remaining_jobs']))
    submitted=[dict(request=r['request'],job=r['job'],quality_warnings=r.get('quality_warnings',[]),recovered_lost_response=r.get('recovered_lost_response',False),reused_prior_job=r['reused_prior_job']) for r in jobs]
    write(OUT/'submitted_jobs.json',submitted)
    missing=['coverage_after_acquisition.csv','entry_execution_results.csv','target_execution_results.csv','execution_model_comparison.csv','false_negative_analysis.csv','path_taxonomy.csv','settlement_results.csv']
    summary=dict(status='incomplete_provider_batch_pending',checked_at_utc=datetime.now(timezone.utc).isoformat(),fresh_quoted_usd=q['estimated_cost_usd'],fresh_quoted_billable_bytes=q['billable_bytes'],pricing_metadata_calls=q['metadata_calls_total'],hard_ceiling_usd=10,submitted_requests=len(jobs),downloaded_requests=a['completed_requests'],planned_requests=a['planned_requests'],provider_states=states,actual_provider_cost_usd=a['actual_provider_cost_usd'],actual_cost_complete=a['actual_cost_complete'],new_coverage_verified=False,scientific_findings_available=False,pending_outputs=missing,tests=test['tests'],next_action='Retrieve the saved provider jobs without resubmitting; then run analysis and the complete report')
    write(OUT/'summary.json',summary)
    report=f'''# Phase 3C — provider-pending acquisition; assignment incomplete

The frozen manifest and bounded pricing are complete. The provider accepted all **{len(jobs)} exact-leg CMBP-1 batch jobs**, but only **{a['completed_requests']}** were downloadable at the end of this attempt. Current provider states: `{json.dumps(states,sort_keys=True)}`. **Execution analysis and completion evidence remain incomplete.** This report does not relabel Phase 3B results as new Phase 3C findings.

## Purchase authorization and evidence

Fresh quoted cost: **${q['estimated_cost_usd']:.12f}**, below the explicitly authorized **$10.00** ceiling. Billable size: **{q['billable_bytes']:,} bytes**. Pricing used **{q['metadata_calls_total']} metadata calls**, including 17 bounded timeout retries, with concurrency four and no download during pricing. The immutable final manifest SHA-256 is `{q['manifest_sha256']}`. It retains the exact 60 development sessions and 246 frozen candidates, corrects two early-close bounds, merges requests safely and subtracts already-owned exact-symbol intervals. A supplementary scan of 1,364 other DBNs found no missed CMBP-1 stores.

The accepted requests constitute authorized purchase commitments, not completed delivery. **Actual final provider charges are unknown** while the jobs expose null `cost_usd`; unknown cost is not reported as zero. No additional subscriptions, instruments, streaming fallback or duplicate purchases were used. `submitted_jobs.json` preserves all provider IDs, exact request parameters, receipt timestamps and warnings. `acquisition_receipts.json` records completion state and remaining provider jobs.

The initial submission pass encountered errors consistent with the documented [20-per-minute limit](https://databento.com/docs/reference-historical/basics/); the original exception records do not prove each HTTP status. Lost responses were reconciled against provider job listings; four receipts were recovered. Uncertain intents were resubmitted only after two provider snapshots, five seconds apart, showed no matching job and the original intent was at least 60 seconds old. The remaining submission pass was paced at 3.2 seconds and completed without further errors. Existing jobs are reused. Under the [provider's batch billing rules](https://databento.com/docs/faqs/usage-pricing-and-data-credits), retrieving the same submitted job does not require another purchase.

## Unfinished scientific work

Completed delivery: **{a['completed_requests']}/{a['planned_requests']}** requests. The new 60-session panel's source and freshness completeness cannot yet be verified. The Phase 3B requirements remain fixed: exact source intervals, ≥95% fresh valid paired path duration and ≥57/60 fresh entry seconds, with maximum leg age one second. Buying source coverage will not itself prove freshness completeness.

New $6→$3 supported-entry rates, resting-target evidence, missed event crossings, persistence false negatives, event-model expectancy/win rate/PF and 2DTE-versus-3DTE differences are **not available**. The original PF≈3.5 cannot be reassessed from undelivered data. Existing owned XQC sources are available for all 246 candidates, but the settlement-based Phase 3C rerun has not been performed. No scientific CSV placeholders were created.

The replay and reporting code is implemented and tested on synthetic and owned decoder fixtures. Rules were frozen in `execution_rules.json` before purchased outcomes. Planned models retain minute touch, 2/3-minute persistence, strong event evidence, strong/plausible evidence and conservative explicit slippage. They distinguish hypothetical evidence-supported fills from proven 20-lot executions. OPRA leg quotes cannot establish complex-order queue priority, simultaneous accessible size, broker latency or actual complex fills.

## Validation and safe continuation

**{test['tests']} tests pass**, including existing NDXP/Phase 3A/3B regressions, ceiling enforcement, merging/subtraction, no duplicate purchase, free resume, unknown pending cost, quote synchronization, coverage censoring, event models, settlement and determinism. Full SHA-256 checks confirm 2,430 original inputs and three Phase 3B source files are unchanged. The owned real-DBN decoder fixture validates integer price/nanosecond handling. No live trading code or broker actions were changed.

Run from the repository after the provider finishes:

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_acquire
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_analysis
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_report
```

Acquisition is bounded to 240 status polls by default (`--max-polls 1` gives one snapshot). It reuses all locally saved job receipts even after the quote expires; any new purchase still requires a fresh quote. Provider batch downloads expire, so retrieve promptly when ready. Local receipts and immutable raw/cache directories remain available and ignored by Git; compact submitted-job evidence is committed. If these local receipts are lost, reconstruct them from `submitted_jobs.json` before resuming. Never substitute a new paid stream for already-submitted batch jobs.

The immediate next phase is **finish this Phase 3C delivery and replay**, then assess a frozen NDXP prospective complex-order paper/shadow protocol before any SPXW expansion. No new purchase or holdout tuning is authorized by this report.
'''
    (OUT/'research_report.md').write_text(report)
    evidence=dict(status=summary['status'],assignment_complete=False,summary=summary,tests=test,source_integrity=json.loads((OUT/'source_integrity.json').read_text()),manifest_sha256=q['manifest_sha256'],rules_sha256=digest(OUT/'execution_rules.json'),pending_outputs=missing,outputs={p.name:dict(bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(OUT.iterdir()) if p.is_file() and p.name!='completion_evidence.json'},commit_sha_resolution='git log -1 --format=%H -- '+str(OUT/'completion_evidence.json'))
    write(OUT/'completion_evidence.json',evidence)
    print(json.dumps(summary,indent=2))

if __name__=='__main__':
    import sys
    if '--pending' in sys.argv:pending()
    else:main()
