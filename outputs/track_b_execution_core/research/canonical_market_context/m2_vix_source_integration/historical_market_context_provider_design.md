# Historical Market Context Provider Design

## Purpose

Historical market context providers supply research-grade context for trade outcome enrichment, regime research, shadow research, and backtests.

## Current Historical VIX Sources

Existing repo support:

- `src/mgc_v05l/market_data/vix_daily_ingest.py`
- `src/mgc_v05l/research/regime/vix_regime_builder.py`
- `src/mgc_v05l/research/regime/vix_join.py`
- `config/research_regime_buckets.json`

The current config points to the official Cboe VIX daily history CSV and uses `16:15:00 ET` as the as-of time.

## Historical Provider V1

Name: `historical_vix_market_context_provider`

Primary source:

- Cboe official daily VIX history materialized into the research warehouse.

Expected paths:

- `outputs/research_platform/warehouse/historical_evaluator/datasets/vix_daily`
- `outputs/research_platform/warehouse/historical_evaluator/datasets/vol_regime_daily`

Join method:

- nearest prior `vix_asof_ts <= target_timestamp`
- never future observations

## Databento Historical VIX

Databento historical VIX should be treated as a candidate source, not assumed available. M3 should add metadata inspection/preflight only if there is an existing approved Databento reference endpoint or config. Do not download data in M3 unless separately approved.

If Databento historical VIX becomes available, preserve Cboe as the canonical daily fallback unless a later validation proves Databento provides better timestamped trade-time history.

## Historical Staleness

Historical rows should expose as-of staleness relative to the joined trade/research timestamp:

- same trading day after as-of: fresh
- prior trading day/weekend carry: acceptable with `carry_forward=true`
- older than seven calendar days: `VIX_HISTORICAL_STALE_WARN`
- no prior row: `NO_PRIOR_VIX_OBSERVATION`

## Derived Fields

Historical provider may derive:

- `vix_daily_change`
- `vix_regime`
- `vix_percentile`
- `vix_ma_20`
- `vix_ma_50`

Derived fields must include provenance and must not be fabricated when history is insufficient.
