# VIX Trade Snapshot Contract

## Scope

This contract defines a diagnostic VIX context snapshot for a trade, strategy intent, blocked intent, or research observation timestamp.

It is not a trading gate.

## Join Rule

For a target timestamp `T`, select the latest VIX observation with:

`vix_observation_time <= T`

Never use a future observation.

## Required Fields

- `schema_version`
- `generated_at`
- `context_key`
- `provider_name`
- `target_timestamp`
- `target_type`
- `target_id`
- `vix_available`
- `vix_level`
- `vix_observation_time`
- `vix_staleness_seconds`
- `vix_source`
- `vix_unavailable_reason`
- `source_refs`
- `data_quality_flags`
- `diagnostic_only=true`
- `production_effect=false`

## Optional Derived Fields

- `vix_daily_change`
- `vix_regime`
- `vix_percentile`
- `vix_ma_20`
- `vix_ma_50`

## Source Provenance Fields

- `source_provider`
- `source_dataset`
- `source_symbol`
- `source_schema`
- `source_stype_in`
- `source_event_time`
- `source_received_at`
- `source_artifact_path`
- `source_provider_mode`

## Unavailable Behavior

Unavailable VIX is represented explicitly:

- `vix_available=false`
- numeric fields are `null`
- `vix_unavailable_reason` is populated
- data-quality flags include the reason

Consumers must treat unavailable VIX as missing optional context unless a future consumer-specific contract states otherwise.

## Initial Staleness Bands

Live:

- fresh: `<= 120s`
- warning: `120s-900s`
- stale: `> 900s`

Historical:

- fresh/carry-forward: latest prior daily as-of row
- warning: older than seven calendar days
- unavailable: no prior row
