# VIX Source Integration Plan

Generated for Canonical Market Context Phase M2.

## Current State

M1 added the Canonical Market Context framework and a VIX provider. The provider currently publishes clean diagnostic output but reports `vix_available=false` because the default warehouse source has no populated VIX rows at:

- `outputs/research_platform/warehouse/historical_evaluator/datasets/vol_regime_daily`
- `outputs/research_platform/warehouse/historical_evaluator/datasets/vix_daily`

The repo already contains two relevant VIX paths:

- Historical Cboe daily ingest: `src/mgc_v05l/market_data/vix_daily_ingest.py`
- Historical VIX regime builder/as-of join: `src/mgc_v05l/research/regime/vix_regime_builder.py` and `src/mgc_v05l/research/regime/vix_join.py`

The repo also contains Databento live infrastructure:

- Phase-1 live symbol universe: `config/track_b_live_market_data_symbols.yaml`
- Phase-1 runtime ticker registry: `src/mgc_v05l/execution_core/phase1_runtime_ticker_registry.py`
- Databento live completed-bar client: `src/mgc_v05l/market_data/live_feed.py`
- Databento current quote boundary: `src/mgc_v05l/execution_core/databento_current_quote.py`

## Source Audit Findings

The existing Phase-1 live universe is futures-focused and currently lists GC/MGC, ES/MES, NQ/MNQ, rates, PL, and crypto futures. It does not include VIX. Its config validator is strict and expects fields oriented around trading/reference futures, readiness, and completed-bar freshness.

VIX should not be added to that trading/readiness universe directly. It should be modeled as context-only market data.

The existing Databento current quote boundary can fetch a current quote with configurable dataset, symbol, stype, and schema, but generic Databento live quotes are not wired as a shared streaming provider. The implementation notes in `DatabentoMarketDataProvider` explicitly reserve live quotes/trades for a later pass.

The historical VIX path is currently Cboe daily CSV based, not Databento based. This is acceptable for research because VIX is a daily index close for many historical regimes, but it is not sufficient for trade-time intraday snapshots.

## Recommended Architecture

Split VIX integration into two providers under the CMC framework:

1. `LiveVixMarketContextProvider`
   - Purpose: trade-time context snapshots.
   - Source: Databento current quote or a Databento-backed context-only live artifact.
   - Output cadence: on demand at trade/intent/enrichment timestamps, not a strategy gate.
   - Freshness: seconds to minutes depending on confirmed Databento symbol/schema.

2. `HistoricalVixMarketContextProvider`
   - Purpose: research/backtest/enrichment joins.
   - Source: existing Cboe daily VIX warehouse path first; Databento historical VIX only if entitlement/schema is confirmed.
   - Join: nearest prior as-of timestamp, never future observations.
   - Freshness: daily close availability; stale is acceptable for historical rows if provenance is explicit.

## Databento Symbol and Schema Work

The exact Databento VIX symbol/dataset/schema is not established in repo config. M3 should not assume `GLBX.MDP3` because VIX is not a CME Globex futures symbol. Operator clarification says Databento can provide live VIX levels when trades occur, so M3 should add a small context-source config and preflight:

- `context_key`: `vix`
- `provider`: `databento`
- `dataset`: to be confirmed from Databento entitlement/reference metadata
- `symbol`: to be confirmed, likely a VIX index symbol rather than futures continuous symbol
- `stype_in`: to be confirmed
- `schema`: prefer a quote/last-value schema if available; otherwise bars/trades with explicit limitations
- `max_age_seconds`: default 120 for trade snapshots until empirical cadence is known

M3 must fail diagnostic-only when symbol/schema is unavailable, not create a runtime blocker.

## Provenance Contract

Every VIX context row should expose:

- `vix_source`: e.g. `databento_live_context`, `cboe_official_daily_history`, `databento_historical_context`
- `source_dataset`
- `source_symbol`
- `source_schema`
- `source_stype_in`
- `source_event_time`
- `source_received_at`
- `source_artifact_path`
- `source_provider_mode`
- `vix_unavailable_reason`
- `data_quality_flags`

## Fallback Behavior

If VIX is unavailable:

- publish `vix_available=false`
- set `vix_level=null`
- set `vix_unavailable_reason`
- preserve source refs and attempted configuration
- do not block runtime
- do not block strategy submits
- do not alter GRE or trade enrichment
- allow consumers to treat VIX as optional context until a later explicit integration phase

## Recommended M3 Slice

M3 should implement source plumbing only:

- Add a context-source config for VIX.
- Add a read-only Databento live VIX snapshot command using the existing current-quote boundary where possible.
- Add historical VIX provider support for the existing Cboe warehouse path.
- Add capability/preflight reporting for unresolved Databento symbol/schema.
- Regenerate CMC artifacts.

M3 should not integrate CMC into T4 enrichment or GRE.
