"""Deterministic Phase 3A acquisition design. Reads evidence; never downloads data."""
from __future__ import annotations
import argparse,json,math
from pathlib import Path
from statistics import median
from .ndxp_multidte_validation import digest

GIB=1024**3
SOURCES={
 'opra':'https://databento.com/docs/venues-and-datasets/opra-pillar',
 'catalog':'https://databento.com/datasets/OPRA.PILLAR',
 'cost':'https://databento.com/docs/api-reference-historical?historical=http',
 'pricing':'https://databento.com/pricing',
 'opra_plans':'https://databento.com/blog/introducing-new-opra-pricing-plans',
 'symbology':'https://databento.com/docs/examples/options/equity-options-introduction/using-parent-symbology-to-fetch-an-option-chain',
 'ndxp':'https://www.nasdaq.com/NDX_NDXP_Factsheet',
 'ndxp_history':'https://nasdaqtrader.com/MicroNews.aspx?id=2018-04',
 'qqq':'https://www.nasdaq.com/products/north-american-markets/nasdaq-100-options-xnd-ndx',
 'spx':'https://www.cboe.com/tradable_products/sp_500/spx_weekly_options/specifications',
 'rut':'https://www.cboe.com/tradable_products/ftse_russell/russell_2000_index_options/rut_specifications',
 'rut_daily':'https://ir.cboe.com/news/news-details/2023/Cboe-TO-OFFER-DAILY-EXPIRIES-FOR-RUSSELL-2000-INDEX-OPTIONS-SUITE-BEGINNING-JANUARY-8-2024/default.aspx',
 'settlement':'https://www.cboe.com/index_settlement_values/weeklys_settlement_values/',
}
INSTRUMENTS={
 'NDXP':{'roots':['NDXP'],'parents':['NDXP.OPT'],'preferred_root':'NDXP','exercise':'European','settlement':'cash, PM; official XQC','multiplier':100,
 'expirations':'Currently Mon–Fri including PM monthly series; use actual historical listings and holidays. Keep NDX AM/XQO separate.',
 'historical_start_design':'2018-02-02','mechanics_sources':['ndxp','ndxp_history'],'comparability':'Frozen 10-point put vertical, literal 2/3 calendar DTE, credits 5/6/7. Earlier root-history extension needs listing audit; 2016 sample failed and is not zero-cost evidence.'},
 'SPX':{'roots':['SPX','SPXW'],'parents':['SPX.OPT','SPXW.OPT'],'preferred_root':'SPXW','exercise':'European','settlement':'SPX traditional AM (SET); SPXW PM cash','multiplier':100,
 'expirations':'SPX traditional third-Friday monthly; SPXW daily/weekly, PM third-Friday and end-of-month where listed. AM last trading day generally precedes settlement.',
 'historical_start_design':'2013-04-01','mechanics_sources':['spx','settlement'],'comparability':'Use SPXW for PM comparison. SPX AM is a separately budgeted optional cohort; never silently merge roots.'},
 'QQQ':{'roots':['QQQ'],'parents':['QQQ.OPT'],'preferred_root':'QQQ','exercise':'American','settlement':'physical delivery of normally 100 ETF shares; early exercise/assignment possible','multiplier':100,
 'expirations':'Monthly and weekday short-term expirations where historically listed; verify holidays, adjusted contracts and deliverables.',
 'historical_start_design':'2013-04-01','mechanics_sources':['qqq'],'comparability':'QQQ equity OHLCV is not option history. Dividends, early assignment, pin/after-hours risk, corporate actions and physical delivery break index-settlement equivalence; a 10-point width is not equal underlying-normalized risk.'},
 'RUT':{'roots':['RUT','RUTW'],'parents':['RUT.OPT','RUTW.OPT'],'preferred_root':'RUTW','exercise':'European','settlement':'RUT AM (RLS); RUTW PM cash','multiplier':100,
 'expirations':'RUT monthly third Friday; RUTW weekly/EOM and Mon–Fri daily suite since January 8, 2024. Older specification page lists Friday only; dated daily-launch notice resolves that omission.',
 'historical_start_design':'2013-04-01','mechanics_sources':['rut','rut_daily','settlement'],'comparability':'Historical daily availability differs; keep AM RUT separate. Smaller underlying level changes relative width and risk.'},
}


def bounds(base,low=.5,high=2.):
    return {'low':round(base*low,4),'central':round(base,4),'high':round(base*high,4)}


def project_structural(sample_usd,sample_bytes,sessions,legs=12):
    """Opening quotes: empirical two-minute quotes; paths: explicit dense-minute cap.

    Definition overhead assumes 1 MiB per root/session, range 0.25–4 MiB.
    12 legs x 3 cash sessions x 390 minutes x 80 bytes is a planning cap,
    not a claim about observed activity or available expirations.
    """
    if not sample_usd or not sample_bytes or sessions<=0:raise ValueError('Need nonempty samples and positive sessions')
    opening_cost=median(sample_usd)*sessions
    opening_bytes=median(sample_bytes)*sessions
    path_bytes=sessions*legs*3*390*80
    definition_bytes=sessions*1024**2
    total_bytes=opening_bytes+path_bytes+definition_bytes
    total_cost=opening_cost+path_bytes/GIB*2+definition_bytes/GIB*5
    return {'cost_usd':bounds(total_cost),'billable_GiB':bounds(total_bytes/GIB),
            'compressed_GiB':bounds(total_bytes/GIB*.25,.4,2.),
            'components':{'opening_usd':opening_cost,'selected_paths_usd':path_bytes/GIB*2,'definition_usd':definition_bytes/GIB*5},
            'download_requests_upper':sessions*3,'sessions_assumed':sessions,'estimate_label':'sample_scaled_plus_explicit_record_model',
            'methodology':'Median successful 2-minute parent cost/size samples × session count + 12 legs × 3 sessions × 390 minutes × 80 bytes + 1 MiB/day definitions. Half-to-double cost/byte range; 10–50% DBN compression range; actual listing days can reduce totals. Provider sub-10-minute quotes may overestimate.'}


def request_design(instrument,layer):
    config=INSTRUMENTS[instrument]
    common={'dataset':'OPRA.PILLAR','instrument':instrument,'layer':layer,'timezone':'America/New_York',
      'download_authorized':False,'cache':'output/ndxp_multidte/program_v1/raw/{dataset}/{schema}/{root}/{session}/{sha256_canonical_request}.dbn.zst',
      'cache_rules':'Read existing index first; merge overlapping symbol/time windows; never overwrite originals. Save request, SHA-256, byte size, source schema/version, timestamp semantics, symbol mapping, quality notices, and receipt. Derive second bars locally from owned CMBP-1 when possible.'}
    if layer=='structural':
        return {**common,'schemas':['cbbo-1m','definition'],'symbols':[config['preferred_root']+'.OPT','selected raw OSI legs after local discovery'],
          'stype':['parent','raw_symbol'],'date_range':['2021-09-24','2026-09-24'],'long_extension':[config['historical_start_design'],'2021-09-23'],
          'strategy':'Full preferred-root chain 09:30–09:32 ET once per trading day; select 2/3-calendar-DTE put legs near credits 5/6/7 causally; selected legs only 09:30 entry through expiration close. Definitions once/day/root for listing and deliverables. No whole-market or continuous multiyear query.',
          'full_chain':'Opening discovery and daily definitions only; paths selected-contract',
          'expected_calls':'At most 3 × session_count before reuse: discovery + definitions + batched selected-leg paths. Calendar/expiry prefilter; concurrency 4. These are FUTURE acquisition calls, not estimator calls.',
          'partition':['development 2021-09-24..2024-12-31','validation 2025','holdout 2026-01-01..2026-09-24'],
          'root_policy':'AM root excluded from initial PM baseline; same-name chain is not root equivalence.'}
    if layer=='path':
        return {**common,'schemas':['cmbp-1','cbbo-1s'],'symbols':'Selected raw OSI legs only; union at most 12 per sampled session','stype':['raw_symbol'],
          'date_range':['2023-03-28','2024-12-31'],'optional_later_date_range':['2025-02-20','2026-09-24'],
          'strategy':'60 development candidate sessions chosen deterministically across calendar months before path outcomes: first qualifying session then fixed spacing. 30-minute opening window plus three non-overlapping 10-minute windows around first $3/$7/$9 crossings or time-matched controls. Maximum 60 minutes/session across at most 12 legs. Outcome-centered windows are measurement labels, never causal entry selection.',
          'schema_gate':'Direct CBBO-1s starts 2025-02-20 per captured catalog; development seconds must be reconstructed from CMBP-1 (available 2023-03-28). No seconds before CMBP coverage; no synthetic interpolation through gaps.',
          'full_chain':False,'expected_calls':'Up to 240 selected-contract window requests for 60 sessions; local reuse first; four workers; cache/deduplicate.'}
    if layer=='execution':
        return {**common,'schemas':['cmbp-1','tcbbo'],'symbols':'Same selected raw OSI legs as path pilot','stype':['raw_symbol'],
          'date_range':['2023-03-28','2024-12-31'],'strategy':'60 development sessions × four 2-minute windows around entry/first target/adverse crossing/control. Preserve nanosecond timestamps, venue, NBBO size, conditions and gaps; CMBP from the path pilot is reused, TCBBO only where incremental trade context is needed.',
          'full_chain':False,'expected_calls':'Up to 240 CMBP + 240 TCBBO windows standalone; CMBP adds zero downloads if path windows cover all events.',
          'limitation':'OPRA top-of-book and prints cannot identify complex-order queue position, routing, auction participation or guarantee a 20-lot spread fill. Proprietary complex-book/trade-linkage data or prospective paper/broker observations require a separate feasibility/permission gate.'}
    raise ValueError(layer)


def build_manifest(inventory,catalog,samples):
    results=list(samples['results'].values())
    def values(root,schema,method,stype='parent'):
        return [x['value'] for x in results if x['status']=='ok' and x['request']['method']==method and x['request']['params']['schema']==schema and x['request']['params']['stype_in']==stype and x['request']['params']['symbols'][0].startswith(root)]
    cat={x['request']['method']:x.get('value') for x in catalog['results'].values() if x['status']=='ok'}
    if not {'get_dataset_range','list_unit_prices','list_schemas'}<=cat.keys():raise ValueError('Catalog evidence incomplete')
    entries=[]
    for instrument,config in INSTRUMENTS.items():
        root=config['preferred_root'];costs=values(root,'cbbo-1m','get_cost');sizes=values(root,'cbbo-1m','get_billable_size')
        for layer in ['structural','path','execution']:
            design=request_design(instrument,layer)
            if layer=='structural':
                estimate=project_structural(costs,sizes,1254)
                if instrument=='NDXP':
                    # Already-owned baseline: only two failed session requests need repair.
                    estimate=project_structural(costs,sizes,2)
                    estimate['methodology']+=' NDXP costs are only a conservative two-session gap budget, not repurchase of the owned five-year store.'
                extension_sessions=900 if instrument=='NDXP' else 2138
                design['optional_extension_estimate']=project_structural(costs,sizes,extension_sessions)
                category='recommended_new_data' if instrument!='NDXP' else 'optional_later_data'
            else:
                # Observed NDXP selected-leg 30m CMBP quote is a proxy, not other instruments' prices.
                raw=[x for x in results if x['status']=='ok' and x['request']['method']=='get_billable_size' and x['request']['params']['schema']=='cmbp-1' and x['request']['params']['stype_in']=='raw_symbol' and x['request']['params']['end'].endswith('10:00:00-04:00')]
                if not raw:raise ValueError('Selected-contract size anchor unavailable')
                anchor=raw[0];legs=len(anchor['request']['params']['symbols'])
                factor={'NDXP':1.,'SPX':5.,'QQQ':5.,'RUT':2.}[instrument]
                minutes=60 if layer=='path' else 8
                nbytes=anchor['value']*(12/legs)*(minutes/30)*60*factor
                # Event print frequency is unknown: explicit 0.05 trades/leg/sec central assumption.
                trade_bytes=60*12*8*60*.05*80 if layer=='execution' else 0
                dollars=nbytes/GIB*.16+trade_bytes/GIB*210
                estimate={'estimate_label':'engineering_scenario_NOT_instrument_quote','cost_usd':bounds(dollars,.25,10),
                    'billable_GiB':bounds((nbytes+trade_bytes)/GIB,.25,10),
                    'compressed_GiB':{'low':round(nbytes/GIB*.025,4),'central':round(nbytes/GIB*.25,4),'high':round((nbytes+trade_bytes)/GIB*5,4)},
                    'download_requests_upper':240 if layer=='path' else 480,'sessions_assumed':60,
                    'methodology':f'NDXP selected-leg CMBP 30m billable-size anchor {anchor["value"]} bytes / {legs} legs, scaled to 12 legs × {minutes} minutes × 60 sessions. Explicit unmeasured activity multiplier {factor} for {instrument}; low/high 0.25–10×. Execution adds TCBBO at 0.05 prints/leg/sec × 80 bytes; zero-trade sample is not generalized.',
                    'overlap':'Execution CMBP is contained in the path pilot: do not add its bytes/cost twice. TCBBO incremental central cost '+str(round(trade_bytes/GIB*210,4)) if layer=='execution' else 'Derive seconds from acquired/owned events; no separate CBBO-1s download for the same window.'}
                category='recommended_new_data' if instrument=='NDXP' else 'optional_later_data'
            entries.append({'id':instrument+'_'+layer,'category':category,'request_design':design,'estimate':estimate})
    own=[{k:g[k] for k in ['location','file_count','bytes','row_count','schemas','source','sufficiency','quality'] if k in g} for g in inventory['groups']]
    errors=[{'request':x['request'],'error_type':x['error_type']} for x in results if x['status']!='ok']
    return {'schema_version':1,'as_of':'2026-09-26','scope':'Phase 3A planning and inventory only','instruments':INSTRUMENTS,'sources':SOURCES,
       'catalog_evidence':cat,'metadata_calls':{'catalog':catalog['call_count'],'samples':samples['call_count'],'total':catalog['call_count']+samples['call_count'],'maximum_workers':4,'downloads':0,'purchases':0,'failed_samples':errors},
       'data_gaps_by_instrument':{
        'NDXP':['Two multi-DTE minute path sessions missing (2026-09-22/23).','Owned event windows require exact 2/3DTE symbol/time intersection; sampled old stop paths are 0DTE and openings often stop before 09:31.','XQC CSV covers 2023-03-28..2026-09-18; frozen baseline settlement dates outside this span remain gaps.','No verified cash NDX intraday store found; reuse NQ/MNQ and QQQ proxies with timestamp/roll checks.'],
        'SPX':['No SPX/SPXW option history or official settlement series identified.','SPXW PM first; AM SPX optional separately. ES/MES minute proxies exist, not cash SPX second-level or complex-book data.'],
        'QQQ':['No QQQ option history identified; owned QQQ equity is venue-specific context only.','Equity history before 2024 and after 2026-09-18 not verified in scoped caches.','Dividend, OCC adjustment and assignment/deliverable records need validation; no index cash-settlement shortcut.'],
        'RUT':['No RUT/RUTW option history or official settlement series identified.','No verified cash RUT/RTY/IWM tape in scoped inventory; do not assume NQ or ES is interchangeable.','Daily-expiry coverage changes in 2024 and historical AM/PM series must remain separate.']},
       'supporting_data_design':[
        {'instrument':'NDXP','source':'Existing XQC CSV, then Nasdaq XQC official history or FRED NASDAQXQC mirror with source validation','dataset_schema':'Daily official settlement values, not option last quotes','range':'2021-09-24..2026-09-25 only missing expiration dates','requests':'Reuse 873 local rows first; one bounded public-history range request if separately authorized, not executed','cost_usd':None,'cost_label':'unquoted; no paid provider assumed','storage':'under 1 MiB for daily research window, engineering estimate'},
        {'instrument':'SPX/RUT','source':SOURCES['settlement'],'dataset_schema':'Official daily settlement series, PM SPX/RUT separate from AM SET/RLS','range':'2021-09-24..2026-09-25','requests':'Stage per-index monthly history queries or supported bulk export; estimate/permission gate before paid DataShop products','cost_usd':None,'cost_label':'unquoted; verify access and terms','storage':'under 1 MiB per daily index series, engineering estimate'},
        {'instrument':'QQQ','source':'Existing XNAS.ITCH equity DBN plus supported historical equity schema if gaps remain; OCC adjustments and dividend calendar','dataset_schema':'XNAS.ITCH ohlcv-1m/ohlcv-1s; raw_symbol QQQ; venue-only scope explicitly retained','range':'2021-09-24..2026-09-24; only uncovered windows','requests':'Monthly 1m chunks; selected opening 1s windows; no underlying tick-wide download','cost_usd':None,'cost_label':'unquoted until exact gaps and current equity entitlements known','storage':'roughly 0.05–0.2 GiB raw for five years of QQQ 1m, excluding options; scenario'},
        {'instrument':'All','source':'Local futures parquet/canonical coverage reports and Cboe-source VIX daily parquet','dataset_schema':'NQ/MNQ/ES/MES GLBX.MDP3 ohlcv-1m context; VIX daily','range':'Align causal availability to each candidate, not contemporaneous future daily close','requests':'No new data until physical backing/coverage audit; proposed gap pricing bounded separately','cost_usd':0,'cost_label':'exact incremental acquisition cost for local reuse only','storage':'Existing stores; small joined feature tables, size deferred to feature schema'}],
       'already_owned_data':own,'recommended_new_data':[e for e in entries if e['category']=='recommended_new_data'],
       'optional_later_data':[e for e in entries if e['category']=='optional_later_data'],
       'unnecessary_data':['Repurchasing existing NDXP minute/event windows or QQQ equity/VIX context before coverage intersection.','Full-market OPRA or full-chain multi-year event downloads.','Duplicate CBBO-1s for event windows already owned; compute second samples locally.','Independent leg-natural fill envelopes as complex-order execution truth.','AM SPX/RUT/NDX substituted into PM cohorts; pre-2018 NDX history treated as homogeneous NDXP.'],
       'sequence':[
        '0. Index existing NDXP headers/contracts/time windows and XQC settlement dates; audit exact 2/3DTE overlap, repair only approved gaps. No acquisition cost to reuse.',
        '1. NDXP development-only 60-session second/event pilot: disprove transient-mark fills before expanding history. Reuse first; new selected windows only after approval.',
        '2. SPXW structural baseline 2021-09-24..2026-09-24, PM only; best like-for-like cash-settled comparator.',
        '3. QQQ structural baseline with exercise/dividend/physical-delivery model frozen first; cheaper Nasdaq-linked control but mechanically different.',
        '4. RUTW structural baseline; preserve pre/post daily-expiry listing differences.',
        '5. Only instruments retaining evidence receive 60-session path/execution pilots; earlier structural history is optional, not an automatic purchase.'],
       'upgrade_analysis':{'recommendation':'No upgrade justified yet; reuse and bounded usage-based windows first.','standard_advertised_usd_month':199,
         'price_source':SOURCES['opra_plans'],'price_caveat':'OPRA-specific $199 advertised June 2025; confirm current account offer. The generic pricing page currently renders CME tiers; do not apply its Plus/Unlimited dollar amounts to OPRA.',
         'plus_unlimited':'Current OPRA quotes, annual commitment, included schemas/date ranges, professional status and fees not independently verified. Request a written offer before economic comparison; no subscription change made.',
         'current_entitlement':'Unknown; metadata.get_cost respects account discounts, so sample charges may already include existing entitlements. list_unit_prices is the marginal rate reference, not an account invoice.',
         'break_even':'Upgrade only if incremental committed subscription+license cost is below remaining unique payable bytes × applicable rates AFTER reuse and included-schema coverage, over the same commitment period.',
         'reuse':'Keep immutable compressed DBN + small partitioned Parquet derivatives. Local replay incurs no new provider charge; avoid re-downloading or broad CSV expansion. Existing research footprint is about 442 GB logical, dominated by duplicate/normalized event history.'},
       'operator_decisions':['Approve a concrete selected-contract/date manifest and spending cap before any purchase.','Confirm current OPRA plan, fees, older-history entitlements and Plus/Unlimited quotes; no upgrade now.','Approve 60-session development pilot definition and prioritization; no strategy selection from 2026.','Confirm portable cache location/disk headroom and source licensing; do not move/delete existing caches.','Choose whether/when to fund optional AM cohorts, older structural history, other-instrument event pilots or proprietary complex-book data.'],
       'quality_controls':['Known NDXP degraded dates 2024-06-03 and 2025-10-22 retained; missing 2026-09-22/23 acquisition sessions retained.','No conditional holdout analysis: inventory headers/counts only; development sample dates frozen before pilot outcomes.','Sample timeout and missing-symbol requests are disclosed in failed_samples; failures are not zero cost.','Datetime windows explicit ET with DST; metadata/DBN UTC retained. Historical listing calendars govern literal DTE; no assumed weekday expirations.','Sub-10-minute provider quote/size estimates can over-report; definitions need full-day estimates. Pilot projections are scenarios, not exact bills.','Use bar availability/end timestamps for causal features; daily VIX close is not available at that morning entry.','The inventory detects external live diagnostic-log growth; read-only scanner does not alter those files. Raw caches must remain unchanged.']}


def render(m,inventory):
    lines=['# Phase 3A: index-options data acquisition plan','',
      'Planning only. No downloads, purchases, subscription changes, broker actions, live-code changes or holdout tuning. All proposed purchases remain subject to operator approval.','',
      '## Decision','',
      'Reuse the existing NDXP event and settlement stores first, then run a small development-only execution pilot. The prior midpoint edge deteriorated under persistence, so additional broad history has less immediate value than resolving whether transient marks can support fills. SPXW is the first new structural comparator, then QQQ, then RUTW. Do not upgrade Databento yet.','',
      '## Available locally','',
      f"Scanned {inventory['market_file_count']:,} market/research files across four named roots: {inventory['logical_bytes']/GIB:,.1f} GiB logical footprint. Copies, derived CSVs and overlaps are included, not unique purchased bytes. DBN headers were sampled first/middle/last by directory; file counts and sizes are exact at scan time, coverage is not inferred from filenames alone. Full provenance is in local_inventory.json.",'',
      '| Store | Files/rows | Dates | Use and limitation |','|---|---|---|---|',
      '| Frozen NDXP multi-DTE | 3,738 candidates; 8,979,954 minute option rows; 913/915 path sessions | 2021-09-24–2026-09-24 requested | Structural baseline owned; nine candidates missing paths, two failed sessions |',
      '| NDXP event openings | 873 CMBP-1 files, 40.95 GiB | 2023-03-28–2026-09-18 | Full-parent 09:30–09:31 samples; header end is exclusive, so do not assume coverage at 09:31 entry |',
      '| NDXP stop paths | 665 CMBP-1 files, 44.06 GiB | 2023-03-28–2026-01-08 | Selected-contract intraday paths, sampled symbols predominantly same-day expiry; do not assume 2/3DTE coverage |',
      '| Other NDXP studies | Definitions, later-entry snapshots and selected event paths | Mainly 2024–2026 | Metadata counts in inventory; quarantine excluded; exact contract/time intersection still required |',
      '| XQC settlement | 873 daily rows | 2023-03-28–2026-09-18 | Local source-tagged settlement CSV exists; verify holidays/source and join to expirations before replacing terminal proxies |',
      '| QQQ equity | 681 one-second DBN windows plus two minute-history files | 2024-01-02–2026-09-18 | XNAS.ITCH equity, not QQQ options; one-second windows roughly first 90 cash minutes |',
      '| VIX | 1,663 daily rows, 50,455 bytes | 2020-01-02–2026-07-01 | Official-Cboe-source-tagged parquet; use prior available close for morning features |',
      '| NQ/MNQ/ES/MES | Local 1m backfill parquet plus earlier coverage audits | Physical backfill mainly 2026-04–07; MNQ/MES also 2019 | Earlier audits report ~2.22m bars each, 2020–2026-04, but backing store not re-counted; roll/gaps need audit |',
      '| SPX/SPXW, QQQ options, RUT/RUTW | No historical option cache identified in scoped search | Unknown | All three option layers missing; absence claim is scoped to named roots |','',
      'One running terminal diagnostic JSONL may grow externally during inventory. The scanner opens raw caches read-only, records size/mtime changes, and hashes key evidence and sampled cache prefixes. It neither stops live processes nor treats their logs as a historical option feed.','',
      '## Instrument mechanics','',
      '| Instrument | Parent/root | Exercise and settlement | Expirations/comparability |','|---|---|---|---|']
    for name,c in m['instruments'].items():
        links=' '.join(f"[{k}]({SOURCES[k]})" for k in c['mechanics_sources'])
        lines.append(f"| {name} | {', '.join(c['parents'])}; preferred {c['preferred_root']} | {c['exercise']}; {c['settlement']}; multiplier {c['multiplier']} | {c['expirations']} {c['comparability']} {links} |")
    lines += ['', '### Gaps and supporting sources','']
    for instrument,gaps in m['data_gaps_by_instrument'].items():
        lines.append('**'+instrument+':** '+' '.join(gaps)+'\n')
    lines.append('The manifest also specifies settlement-history and underlying-context gap requests. These ancillary external requests are unquoted, not priced as zero or included in the option-layer totals; local reuse costs zero acquisition dollars. Daily official settlement series should be under 1 MiB each. No ancillary retrieval was performed.')
    lines += ['', 'Parent requests use stype=parent; selected options use exact historical OSI raw symbols and stype=raw_symbol. Preserve padded symbols, daily mappings and adjusted deliverables; parent names do not make settlement conventions interchangeable. [Databento symbology]('+SOURCES['symbology']+')','',
      '## Date and resolution boundaries','',
      'Catalog metadata captured for this assignment reports CBBO-1m/definitions from 2013-04-01, CMBP-1/TCBBO from 2023-03-28, and direct CBBO-1s only from 2025-02-20. Published general history is not a per-schema guarantee. Before 2023-03-28 OPRA history is subsampled, with different timestamp/venue limitations. Development-era seconds must come from CMBP-1, not a nonexistent 2024 CBBO-1s request. [OPRA specification]('+SOURCES['opra']+')','',
      'Initial structural comparators use 2021-09-24–2026-09-24. Optional extensions use 2013-04-01 onward for SPXW/QQQ/RUTW, and 2018-02-02 onward for a conservative NDXP weekly-root design pending listing audit. The failed 2016 NDXP sample is not treated as free data. Earlier AM NDX is not a substitute. Freeze development through 2024, validation in 2025 and holdout in 2026 before new conditional work.','',
      '## Cost, storage and request budget','',
      '43 actual metadata calls (3 catalog + 40 sample), maximum four workers, 20-second HTTP timeout, no retries and zero download calls. The reusable estimator rejects workloads above 64 calls before sending anything; cache hits do not call the API. Failures are listed in the manifest, never replaced with zero.','',
      'Costs below are USD low / central / high **estimates**, not purchase quotes. Structural estimates scale three era-spaced parent opening samples (two for NDXP) plus a dense-minute selected-leg and definition budget. High-resolution estimates use one measured NDXP selected-leg size anchor with explicit activity scenarios for other instruments: those cross-instrument multipliers are assumptions, not measured quotes. Small windows can be overestimated by the provider; use full 10-minute bins for final approval estimates. [Metadata pricing semantics]('+SOURCES['cost']+')','',
      '| Instrument/layer | Scope | USD low / central / high | Billable GiB central | Compressed GiB low / central / high | Future request cap |','|---|---|---|---|---|---|']
    entries=sorted(m['recommended_new_data']+m['optional_later_data'],key=lambda e:(list(INSTRUMENTS).index(e['request_design']['instrument']),['structural','path','execution'].index(e['request_design']['layer'])))
    def triple(v):return ' / '.join(f'{v[k]:,.3f}' for k in ['low','central','high'])
    for e in entries:
        d=e['request_design'];x=e['estimate'];scope='2-session gap only' if e['id']=='NDXP_structural' else '1,254-session structural' if d['layer']=='structural' else '60-session pilot'
        lines.append(f"| {e['id']} | {scope} | {triple(x['cost_usd'])} | {x['billable_GiB']['central']:.3f} | {triple(x['compressed_GiB'])} | {x['download_requests_upper']} |")
    lines += ['', 'Path and execution budgets overlap: execution CMBP windows are a subset of the path pilot and must not be added twice; only TCBBO trade-context costs are incremental. Raw compressed storage is not the billable size; billing is based on uncompressed binary bytes. Reserve another 2–4× compressed capacity for derived Parquet, indexes and temporary decoding, and avoid broad CSV expansion. Network lower bound at 100 MB/s is 10 seconds per GB; symbol selection, request latency and decode work can dominate.','',
      'The unit-price metadata returned 2 for CBBO-1m/1s, 0.16 for CMBP-1, 210 for TCBBO, and 5 for definitions; cost/size sample pairs reconcile using 2^30 bytes per quoted unit. TCBBO is expensive per byte and one NDXP two-minute sample had zero trades: never extrapolate that zero across the pilot.','',
      '### Request design by layer','',
      'Structural: one preferred-root parent chain at 09:30–09:32 ET per listed trading day, then only selected put legs through expiry; root-filter daily definitions for listing/deliverables. At most three requests/session before reuse, with four workers. Full chains are confined to opening discovery and definitions; no continuous multi-year event query.','',
      'Path: 60 development sessions, deterministic month-spaced dates fixed before outcomes. Up to 12 legs; 30 minutes around opening plus up to three 10-minute crossing/control windows, 60 minutes/session total. Up to 240 requests. Reconstruct second-level NBBO from local/acquired CMBP for development dates; use direct CBBO-1s only where its coverage permits and no owned event window already suffices.','',
      'Execution: four two-minute entry/target/adverse/control windows per session, preserving quote/trade conditions and sizes. Up to 240 CMBP and 240 TCBBO requests standalone; reuse covered CMBP from the path pilot. The same windows must include time-matched failures/controls to avoid survivorship bias. OPRA cannot establish complex-order queue or 20-contract fills; proprietary complex-book data or prospective observations are a separate later gate. [OPRA feed scope]('+SOURCES['opra']+')','',
      'Cache all approved new requests by canonical dataset/schema/root/date/symbol/time hash in immutable DBN; save request manifests, hashes, receipts and quality warnings. Index exact contract/time coverage before any request, merge overlapping windows and retain originals. No acquisition function is implemented in this assignment.','',
      '### Optional longer structural history','',
      '| Instrument | Additional session planning assumption | Estimated USD low / central / high | Compressed GiB low / central / high |','|---|---|---|---|']
    for e in entries:
        if e['request_design']['layer']=='structural':
            x=e['request_design']['optional_extension_estimate'];lines.append(f"| {e['request_design']['instrument']} | {x['sessions_assumed']} | {triple(x['cost_usd'])} | {triple(x['compressed_GiB'])} |")
    lines += ['', 'Extension session counts are coarse planning assumptions, not verified exchange calendars; actual listings, holidays, gaps and prior ownership determine the final manifest. AM SPX/RUT cohorts and proprietary complex-book sources are optional later and require separate instrument-specific quotes; no invented vendor prices are assigned.','',
      '## Acquisition order','']+[f'{i+1}. {s}' for i,s in enumerate(m['sequence'])]
    core=[e for e in entries if e['id'] in ('SPX_structural','QQQ_structural','RUT_structural','NDXP_structural','NDXP_path')]
    central=sum(e['estimate']['cost_usd']['central'] for e in core)+60*12*8*60*.05*80/GIB*210
    lines += ['', '## Upgrade economics','', f'The initial option-layer sequence has a central scenario of approximately ${central:,.2f}, after avoiding duplicate NDXP execution CMBP. This excludes optional historical extensions, ancillary unquoted gaps and proprietary complex data. A $199 subscription would need to remove more payable cost than this central scenario; it is not justified merely for this staged acquisition.', '',
      'Keep usage-based access for now. The OPRA-specific announcement advertises Standard at $199/month; confirm today’s account offer and which historical schemas/dates it includes. The generic page renders CME pricing, so its Plus/Unlimited figures are not used for OPRA. Current OPRA Plus/Unlimited commitment and license quotes remain unverified. [OPRA plan announcement]('+SOURCES['opra_plans']+')','',
      'Compare incremental subscription/fees over its full commitment against only unique payable data after reuse. Metadata get_cost respects existing plan discounts, so do not mistake a zero sample for universally free data or buy a plan for access already included. Repeated local replay has no provider charge; another subscription does not solve timestamp, settlement, physical-delivery or complex-fill evidence gaps.','',
      '## Operator decisions before any acquisition','']+['- '+x for x in m['operator_decisions']]
    lines += ['', '## Reproduce and verification','',
      'The checked-in JSON evidence makes plan regeneration entirely offline. The inventory CLI reads named roots only. The estimator CLI is offline unless --online is explicit, and accepts only a fixed metadata endpoint allowlist. See completion_evidence.json for tests and exact changed files.','',
      '```sh','PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.index_options_acquisition_plan --evidence-dir output/ndxp_multidte/program_v1 --output-dir /tmp/index-options-phase3a-rebuild','PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_index_options_phase3a.py','```']
    return '\n'.join(lines)+'\n'


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--evidence-dir',type=Path,required=True);p.add_argument('--output-dir',type=Path,required=True)
    a=p.parse_args();read=lambda name:json.loads((a.evidence_dir/name).read_text())
    inv,cat,samples=read('local_inventory.json'),read('catalog_quotes.json'),read('sample_quotes.json')
    m=build_manifest(inv,cat,samples)
    m['evidence_sha256']={name:digest(a.evidence_dir/name) for name in ['local_inventory.json','catalog_quotes.json','sample_quotes.json']}
    a.output_dir.mkdir(parents=True,exist_ok=True)
    for name,value in [('data_acquisition_manifest.json',json.dumps(m,indent=2,sort_keys=True)+'\n'),('data_acquisition_plan.md',render(m,inv))]:
        with (a.output_dir/name).open('x') as f:f.write(value)
    print('Wrote deterministic planning artifacts; no remote calls.')
if __name__=='__main__':main()
