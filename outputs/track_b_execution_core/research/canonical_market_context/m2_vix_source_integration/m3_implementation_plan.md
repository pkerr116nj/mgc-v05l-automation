# M3 Implementation Plan

## Goal

Implement source plumbing for VIX without integrating CMC into trade enrichment, GRE, runtime, strategy behavior, or gates.

## Step 1: Context Source Config

Add a context-only config, separate from Phase-1 trading/readiness symbols.

Suggested path:

- `config/canonical_market_context_sources.json`

Initial fields:

- `context_key`
- `enabled`
- `provider`
- `mode`
- `dataset`
- `symbol`
- `stype_in`
- `schema`
- `max_age_seconds`
- `required_for_runtime`
- `diagnostic_only`

For M3, `required_for_runtime=false`.

## Step 2: Databento VIX Preflight

Add a read-only preflight that reports:

- configured dataset/symbol/schema
- whether config is complete
- whether credentials are present
- whether provider path can be initialized
- no download by default
- no subscription by default

If the Databento symbol/schema remains unknown, M3 should publish a diagnostic failure, not guess.

## Step 3: Live VIX Snapshot Command

If symbol/schema are confirmed, implement a one-shot read-only snapshot command using the Databento current quote boundary or a small transport-injected equivalent.

Output:

- `outputs/track_b_execution_core/research/canonical_market_context/live_vix/latest_live_vix_context.json`
- optional bounded JSONL history

No runtime hook.

## Step 4: Historical Provider Wiring

Update CMC VIX provider source selection to prefer:

1. explicit source path
2. `vol_regime_daily`
3. `vix_daily`
4. live VIX latest snapshot for live summary only

No Cboe download in M3 unless separately approved.

## Step 5: Tests

Focused tests:

- config parses context-only VIX source
- unknown symbol/schema reports diagnostic unavailable
- historical Cboe rows load into CMC
- live snapshot row loads into CMC
- nearest-prior join does not use future rows
- stale live VIX marks warning/stale
- missing VIX remains diagnostic-only

## Step 6: Validation

- focused tests pass
- compileall
- `git diff --check`
- AST safety scan clean
- regenerate CMC summary

## Explicit Non-Goals

- no T4 enrichment integration
- no GRE integration
- no runtime hook
- no strategy gate
- no broker interaction
- no Databento historical bulk download
