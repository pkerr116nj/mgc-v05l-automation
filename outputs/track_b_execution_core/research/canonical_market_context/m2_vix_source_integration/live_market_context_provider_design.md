# Live Market Context Provider Design

## Purpose

Live market context providers supply diagnostic snapshots at or near live PAPER trade/intent timestamps. They are observation-only and never become trade gates.

## Live VIX Provider

Name: `live_vix_market_context_provider`

Initial role:

- Fetch or read current VIX level.
- Publish VIX context rows through CMC.
- Support later trade-time joins by timestamp.

## Preferred Source

Use a Databento-backed context-only source after confirming:

- dataset
- symbol
- stype
- schema
- entitlement
- expected update cadence

The existing current quote boundary at `src/mgc_v05l/execution_core/databento_current_quote.py` is the lowest-risk pattern because it is transport-injected, report-producing, and no-submit.

## Provider Output

Each live VIX observation should include:

- `context_key=vix`
- `provider_name=databento_live_vix`
- `vix_available`
- `vix_level`
- `vix_observation_time`
- `vix_staleness_seconds`
- `vix_source=databento_live_context`
- `source_dataset`
- `source_symbol`
- `source_schema`
- `source_stype_in`
- `source_received_at`
- `source_artifact_path`
- `data_quality_flags`
- `diagnostic_only=true`
- `production_effect=false`

## Staleness Rules

Initial conservative defaults:

- Fresh: `<= 120 seconds`
- Warning: `> 120 seconds and <= 900 seconds`
- Stale/unavailable: `> 900 seconds`

For trade-time snapshots, CMC should attach the latest observation whose event time is less than or equal to the trade/intent timestamp. It must never use a future VIX observation.

## Readiness and Runtime Independence

Live VIX context must not be added to:

- Phase-1 required readiness symbols
- strategy gate checks
- broker startup authority
- Safe-State
- Managed Exit

Missing VIX is diagnostic only until a future explicit integration phase changes a consumer contract.

## Failure Modes

- Symbol/schema unresolved: `VIX_DATABENTO_SYMBOL_SCHEMA_UNCONFIRMED`
- No entitlement: `VIX_DATABENTO_ENTITLEMENT_UNAVAILABLE`
- No records: `VIX_LIVE_NO_RECORDS`
- Provider stale: `VIX_LIVE_STALE`
- Provider error: `VIX_LIVE_PROVIDER_ERROR`
- Unsupported data type: `VIX_LIVE_UNSUPPORTED_SCHEMA`
